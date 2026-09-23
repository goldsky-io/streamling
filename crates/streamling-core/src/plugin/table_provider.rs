use crate::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointEpoch, CheckpointMessage,
    enrich_batch_metadata_with_checkpoints, extract_checkpoint_messages, now_ms,
    report_marker_at_sink_stream,
};
use crate::operators::parallel_sink::{ParallelSinkExec, ParallelSinks};
use crate::plugin::partitioned::{PluginInstance, PluginInstances};
use crate::utils::batch::enrich_batch_with_metadata;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::common::internal_err;
use datafusion::common::{DataFusionError, not_impl_err, project_schema};
use datafusion::datasource::sink::DataSink;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::{Debug, Formatter};
use tokio::sync::watch;

use crate::checkpoints::channels::{send, subscribe_with_id, unsubscribe};
use crate::operators::wrapping::WrappingDataSink;
use crate::plugin::telemetry::process_plugin_metrics;
use crate::telemetry::provider::get_reference_name_from_metric_key;
use crate::telemetry::recorder::get_metrics_recorder;
use crate::topology::Telemetry;
use crate::utils::metrics::metric_metadata_id_to_reference_name;
use abi_stable::nonexhaustive_enum::NonExhaustive;
use crossbeam::channel::TryRecvError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use std::sync::Arc;
use streamling_plugin::{PluginCheckpointEpoch, PluginMsg};
use tracing::log::trace;
use tracing::{debug, error, info, warn};

/// Flush buffered checkpoint messages downstream on a synthetic empty batch,
/// clearing the buffer. Used when there is no data batch for them to ride —
/// at end of stream, and whenever the plugin's output goes idle (an exhausted
/// bounded source stops emitting, yet its final epoch can only finalize once
/// the sinks see the marker). Callers must only invoke this when the plugin
/// output channel has been drained: that is what guarantees every row the
/// buffered markers cover has already been sent downstream, keeping the
/// marker-after-covered-data ordering that at-least-once relies on.
async fn flush_checkpoint_buffer_on_empty_batch(
    tx: &tokio::sync::mpsc::Sender<Result<RecordBatch>>,
    schema: &SchemaRef,
    checkpoint_buffer: &mut Vec<CheckpointMessage>,
) {
    let empty = RecordBatch::new_empty(schema.clone());
    let mut metadata = schema.metadata().clone();
    enrich_batch_metadata_with_checkpoints(&mut metadata, checkpoint_buffer);
    match enrich_batch_with_metadata(empty, metadata) {
        Ok(batch) => {
            if tx.send(Ok(batch)).await.is_err() {
                warn!(
                    "PluginSourceExec: downstream closed before the synthetic checkpoint batch could be sent"
                );
            }
            checkpoint_buffer.clear();
        }
        Err(e) => warn!(
            "PluginSourceExec: failed to build synthetic checkpoint batch: {:?}",
            e
        ),
    }
}

/// Emits partition `i` from plugin instance `i`.
#[derive(Debug)]
struct PluginSourceExec {
    schema: SchemaRef,
    /// Column indices into the plugin's full schema, pushed down from the scan.
    /// The plugin always emits full rows; the projection is applied here so the
    /// emitted batches match `schema` (DataFusion 54 resolves downstream column
    /// indices against the projected scan schema).
    projection: Option<Vec<usize>>,
    instances: Vec<PluginInstance>,
    cached_properties: Arc<PlanProperties>,
    internal_buffer_size: u32,
    metric_metadata_id: String,
    scope: Arc<crate::shutdown::ComponentScope>,
}

impl PluginSourceExec {
    pub fn new(
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        instances: Vec<PluginInstance>,
        internal_buffer_size: u32,
        metric_metadata_id: String,
        scope: Arc<crate::shutdown::ComponentScope>,
    ) -> Self {
        let cached_properties = Self::compute_properties(schema.clone(), instances.len());
        Self {
            schema,
            projection,
            instances,
            cached_properties: Arc::new(cached_properties),
            internal_buffer_size,
            metric_metadata_id,
            scope,
        }
    }

    fn compute_properties(schema: SchemaRef, partitions: usize) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        )
    }
}

impl DisplayAs for PluginSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "PluginSourceExec: partitions={}",
            self.properties().output_partitioning().partition_count()
        )
    }
}

impl ExecutionPlan for PluginSourceExec {
    fn name(&self) -> &'static str {
        "PluginSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cached_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(instance) = self.instances.get(partition) else {
            return internal_err!(
                "PluginSourceExec has {} partitions, got request for partition {partition}",
                self.instances.len()
            );
        };
        crate::plugin::send_to_plugin_blocking(
            &instance.channels.input.sender,
            NonExhaustive::new(PluginMsg::Init),
            &instance.key,
        )?;

        // Every partition subscribes on its own and forwards the coordinator's
        // markers to its own instance, so each stream carries one copy of an
        // epoch's marker, as downstream alignment expects.
        let (checkpoint_receiver, checkpoint_subscriber_id) =
            subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);

        let plugin_input_sender = instance.channels.input.sender.clone();
        let plugin_output_receiver = instance.channels.output.receiver.clone();

        let mut builder = RecordBatchReceiverStreamBuilder::new(
            self.schema.clone(),
            self.internal_buffer_size as usize,
        );

        let tx = builder.tx();
        let metrics_receiver = instance.channels.metrics.receiver.clone();
        let metrics_recorder = get_metrics_recorder();
        let metric_metadata_id = self.metric_metadata_id.clone();
        let plugin_label = instance.key.clone();
        let forwarder_scope = self.scope.clone();
        let projection = self.projection.clone();
        let schema_for_flush = self.schema.clone();

        builder.spawn(async move {
            // PostPlugin-stage scope: exits on channel disconnect at plugin
            // teardown; the stage placement means the drain ladder awaits it
            // AFTER the dispatcher flush it serves.
            forwarder_scope.spawn(process_plugin_metrics(
                metrics_receiver,
                metrics_recorder,
                metric_metadata_id,
                plugin_label.clone(),
                forwarder_scope.stage_token().clone(),
            ));

            let mut checkpoint_buffer: Vec<CheckpointMessage> = Vec::new();
            // Track created_at_ms for epochs so we can preserve timing through plugin round-trip
            let mut epoch_created_at: std::collections::HashMap<u64, u64> =
                std::collections::HashMap::new();
            let mut batch_count: u64 = 0;
            let mut batches_with_markers: u64 = 0;
            let mut batches_without_markers: u64 = 0;

            // Observe the process-wide shutdown signal so a plugin source stops
            // producing and ends its stream (front-to-back drain), instead of
            // emitting until the watchdog hard-exits. The plugin process itself
            // keeps running until the run loop sends Terminate AFTER the sinks
            // drain — here we only stop forwarding: drain the messages the
            // plugin had already emitted at signal time (a snapshot, so a
            // still-producing plugin cannot pin the drain), then end the
            // stream so downstream sinks see stream-end and flush.
            let shutdown_rx = crate::shutdown::subscribe();
            // Some(n) once shutdown was observed: at most n more messages are
            // forwarded before the stream ends.
            let mut drain_remaining: Option<usize> = None;

            'outer: loop {
                loop {
                    if drain_remaining.is_none() && *shutdown_rx.borrow() {
                        let in_flight = plugin_output_receiver.len();
                        info!(
                            "PluginSourceExec: shutdown requested; draining {} in-flight plugin message(s), then ending stream",
                            in_flight
                        );
                        drain_remaining = Some(in_flight);
                    }
                    if drain_remaining == Some(0) {
                        break 'outer;
                    }
                    if !plugin_output_receiver.is_empty() {
                        if let Some(n) = drain_remaining.as_mut() {
                            *n -= 1;
                        }
                        if let Ok(message) = plugin_output_receiver.recv() {
                            match message.into_enum() {
                                Ok(PluginMsg::NextBatch { data }) => {
                                    let mut record_batch: RecordBatch = data.into();
                                    // The plugin emits full rows; apply the scan's column
                                    // projection so batches match the declared (projected)
                                    // schema. `RecordBatch::project` preserves schema
                                    // metadata, so checkpoint signals survive.
                                    if let Some(indices) = &projection {
                                        record_batch = match record_batch.project(indices) {
                                            Ok(projected) => projected,
                                            Err(e) => {
                                                let _ = tx
                                                    .send(Err(DataFusionError::from(e)
                                                        .context("projecting plugin source batch")))
                                                    .await;
                                                unsubscribe(
                                                    CHECKPOINT_COORDINATOR_CHANNEL,
                                                    checkpoint_subscriber_id,
                                                );
                                                return Ok(());
                                            }
                                        };
                                    }
                                    batch_count += 1;

                                    if !checkpoint_buffer.is_empty() {
                                        batches_with_markers += 1;
                                        debug!(
                                            "PluginSourceExec: Attaching {} buffered checkpoint messages to batch #{} (with_markers={}, without_markers={})",
                                            checkpoint_buffer.len(), batch_count, batches_with_markers, batches_without_markers
                                        );

                                        let mut metadata = record_batch.schema().metadata().clone();
                                        enrich_batch_metadata_with_checkpoints(
                                            &mut metadata,
                                            &checkpoint_buffer,
                                        );
                                        record_batch = enrich_batch_with_metadata(
                                            record_batch,
                                            metadata,
                                        )
                                        .expect("Failed to enrich batch with checkpoint metadata");

                                        checkpoint_buffer.clear();
                                    } else {
                                        batches_without_markers += 1;
                                        if batches_without_markers % 100 == 1 {
                                            debug!(
                                                "PluginSourceExec: Sending batch #{} WITHOUT checkpoint markers (with_markers={}, without_markers={})",
                                                batch_count, batches_with_markers, batches_without_markers
                                            );
                                        }
                                    }

                                    match tx.send(Ok(record_batch)).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            // this could simply mean shutdown
                                            warn!("Error sending record batch: {:?}", e);
                                        }
                                    }
                                }
                                Ok(PluginMsg::CheckpointMarker { epoch }) => {
                                    debug!(
                                        "Buffering checkpoint marker with epoch {} from plugin",
                                        epoch.0,
                                    );
                                    // Use stored created_at_ms or current time if not found
                                    // Remove entry to prevent unbounded HashMap growth
                                    let created_at_ms =
                                        epoch_created_at.remove(&epoch.0).unwrap_or_else(now_ms);
                                    checkpoint_buffer.push(CheckpointMessage::Marker {
                                        epoch: CheckpointEpoch(epoch.0),
                                        created_at_ms,
                                    });
                                }
                                Ok(PluginMsg::CheckpointFinalizer { epoch }) => {
                                    debug!(
                                        "Buffering checkpoint finalizer with epoch {} from plugin",
                                        epoch.0
                                    );
                                    checkpoint_buffer.push(CheckpointMessage::Finalizer(
                                        CheckpointEpoch(epoch.0),
                                    ));
                                }
                                Ok(PluginMsg::Terminate) => {
                                    // Output-direction Terminate: the plugin
                                    // source stopped on its own (bounded work
                                    // complete) — see the variant's doc on
                                    // `PluginMsg` for the reuse rationale. End
                                    // the stream so downstream sinks see
                                    // end-of-stream and a job-mode pipeline
                                    // can finish; the flush after the loop
                                    // carries any still-buffered markers to
                                    // the sinks. Ends this partition only.
                                    info!(
                                        "PluginSourceExec: source {} reported completion; ending its record-batch stream",
                                        plugin_label
                                    );
                                    break 'outer;
                                }
                                Ok(PluginMsg::Error { message }) => {
                                    // Buffered markers are dropped with the
                                    // stream: an epoch covering rows the
                                    // failed plugin lost must never finalize.
                                    let _ = tx
                                        .send(Err(crate::plugin::plugin_failure(
                                            &plugin_label,
                                            &message,
                                        )))
                                        .await;
                                    unsubscribe(
                                        CHECKPOINT_COORDINATOR_CHANNEL,
                                        checkpoint_subscriber_id,
                                    );
                                    return Ok(());
                                }
                                _ => {}
                            }
                        }

                        if checkpoint_receiver.is_empty() {
                            continue;
                        } else {
                            break;
                        }
                    } else {
                        // Plugin output idle. Two things must not starve
                        // behind a quiet plugin (an exhausted bounded source,
                        // a long fetch):
                        //
                        // 1. Round-tripped markers already sitting in
                        //    `checkpoint_buffer`. There may never be another
                        //    data batch to ride, and a bounded job's final
                        //    epoch cannot finalize — so the job cannot end —
                        //    until the sinks see its marker. Flush them on a
                        //    synthetic empty batch now. Ordering stays
                        //    correct: the plugin's output channel carries
                        //    markers and batches in emission order and it is
                        //    empty here, so every row the marker covers has
                        //    already been forwarded downstream.
                        //
                        // 2. Coordinator messages waiting to be forwarded
                        //    INTO the plugin — the round-trip cannot even
                        //    start if servicing them requires plugin output
                        //    first (this loop used to spin right here without
                        //    ever reaching the coordinator arm below).
                        if !checkpoint_buffer.is_empty() {
                            info!(
                                "PluginSourceExec: flushing {} pending checkpoint message(s) on a synthetic batch (plugin output idle)",
                                checkpoint_buffer.len()
                            );
                            flush_checkpoint_buffer_on_empty_batch(
                                &tx,
                                &schema_for_flush,
                                &mut checkpoint_buffer,
                            )
                            .await;
                        }
                        if !checkpoint_receiver.is_empty() {
                            break;
                        }
                        // Sleep, don't yield: yield_now in this idle arm
                        // busy-spun a worker at 100% CPU when the plugin
                        // channels were empty (main's f356711).
                        tokio::time::sleep(super::IDLE_POLL_INTERVAL).await;
                    }
                }

                if !checkpoint_receiver.is_empty() {
                    match checkpoint_receiver.recv() {
                        Ok(CheckpointMessage::Marker {
                            epoch,
                            created_at_ms,
                        }) => {
                            // Store created_at_ms for this epoch
                            epoch_created_at.insert(epoch.0, created_at_ms);
                            debug!(
                                "PluginSourceExec: Received checkpoint Marker epoch {} from coordinator, forwarding to plugin (batch_count={}, pending_buffer={})",
                                epoch.0, batch_count, checkpoint_buffer.len()
                            );
                            crate::plugin::send_to_plugin(
                                &plugin_input_sender,
                                NonExhaustive::new(PluginMsg::CheckpointMarker {
                                    epoch: PluginCheckpointEpoch(epoch.0),
                                }),
                                &plugin_label,
                            )
                            .await?;
                        }
                        Ok(CheckpointMessage::Finalizer(epoch)) => {
                            debug!(
                                "Propagating checkpoint Finalizer with epoch {} to plugin",
                                epoch.0
                            );
                            crate::plugin::send_to_plugin(
                                &plugin_input_sender,
                                NonExhaustive::new(PluginMsg::CheckpointFinalizer {
                                    epoch: PluginCheckpointEpoch(epoch.0),
                                }),
                                &plugin_label,
                            )
                            .await?;
                        }
                        Ok(_) => {
                            // ignore other messages
                        }
                        Err(e) => {
                            error!("Error receiving message from checkpoint channel: {:?}", e);
                        }
                    }
                }
            }

            // Shutdown drain complete. Coordinator messages still queued on
            // our subscription never made the plugin round-trip (e.g. the
            // terminal epoch's Marker, broadcast during the shutdown window).
            // The plugin is about to be terminated, so forwarding them to it
            // is pointless — but the SINKS still need to see the Marker for
            // the epoch to collect acks and finalize. Fold them into the
            // synthetic-final-batch flush below.
            while let Ok(message) = checkpoint_receiver.try_recv() {
                if let m @ (CheckpointMessage::Marker { .. } | CheckpointMessage::Finalizer(_)) =
                    message
                {
                    debug!(
                        "PluginSourceExec: attaching still-queued coordinator message to final flush: {:?}",
                        m
                    );
                    checkpoint_buffer.push(m);
                }
            }

            // Flush any checkpoint markers the plugin
            // emitted that never got a data batch to ride on, on a synthetic
            // empty batch, so the sinks can still ack their epochs before the
            // stream ends (the same shape as the hybrid source's pending-marker
            // flush).
            if !checkpoint_buffer.is_empty() {
                info!(
                    "PluginSourceExec: flushing {} pending checkpoint message(s) on a synthetic final batch",
                    checkpoint_buffer.len()
                );
                flush_checkpoint_buffer_on_empty_batch(&tx, &schema_for_flush, &mut checkpoint_buffer)
                    .await;
            }
            // Drop our coordinator subscription cleanly so later broadcasts
            // don't hit a dead sender.
            unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, checkpoint_subscriber_id);
            Ok(())
        });

        Ok(builder.build())
    }
}

// TODO: this could be combined with PluginSinkProvider
#[derive(Clone, Debug)]
pub struct PluginSourceProvider {
    schema: SchemaRef,
    instances: PluginInstances,
    /// How many streams the source runs: 1 for a single-stream plugin.
    partitions: usize,
    internal_buffer_size: u32,
    metric_metadata_id: String,
    /// PostPlugin-stage scope: the metrics forwarder must outlive the plugin
    /// dispatcher drain (it serves the dispatcher's flush), so it drains
    /// between the dispatcher drain and coordinator stop.
    scope: Arc<crate::shutdown::ComponentScope>,
}

impl PluginSourceProvider {
    pub fn new(
        schema: SchemaRef,
        instances: PluginInstances,
        partitions: usize,
        internal_buffer_size: u32,
        metric_metadata_id: String,
        scope: Arc<crate::shutdown::ComponentScope>,
    ) -> Self {
        Self {
            schema,
            instances,
            partitions,
            internal_buffer_size,
            metric_metadata_id,
            scope,
        }
    }

    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema_projected = project_schema(&self.schema, projections)?;
        // An identity projection (all columns in order) needs no per-batch work.
        let projection = projections
            .filter(|indices| !indices.iter().copied().eq(0..self.schema.fields().len()))
            .cloned();
        let instances = self.instances.resolve(self.partitions, None).await?;
        Ok(Arc::new(PluginSourceExec::new(
            schema_projected,
            projection,
            instances,
            self.internal_buffer_size,
            self.metric_metadata_id.clone(),
            self.scope.clone(),
        )))
    }
}

#[async_trait]
impl TableProvider for PluginSourceProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(projection).await
    }
}

/// An instance's checkpoint progress, as its ack forwarder observes it.
#[derive(Debug, Default)]
struct AckState {
    /// Epochs whose marker was sent to the instance and not yet acked.
    unacked: BTreeSet<u64>,
    /// Set once the instance reports, through `PluginMsg::Error`, that it
    /// failed.
    failure: Option<String>,
}

/// Resolves once the instance reported a failure.
async fn reported_failure(ack_state: &mut watch::Receiver<AckState>) -> String {
    let failure = ack_state
        .wait_for(|state| state.failure.is_some())
        .await
        .map(|state| state.failure.clone().unwrap_or_default());
    match failure {
        Ok(failure) => failure,
        // The sender lives as long as the sink writing through this receiver.
        Err(_) => std::future::pending().await,
    }
}

/// The write path into one plugin instance.
struct PluginSink {
    schema: SchemaRef,
    instance: PluginInstance,
    /// The write stream this sink is bound to, which it reports its
    /// instance's acks for.
    stream: usize,
    num_records_before_stop: Option<u64>, // for integration tests only!
    metric_metadata_id: String,
    /// PostPlugin-stage scope for the metrics and ack forwarders: both serve
    /// the plugin dispatcher's flush, so they drain after it.
    scope: Arc<crate::shutdown::ComponentScope>,
    /// Set for a partition instance: its stream is one of several gated
    /// write streams, and a finished stream releases its share of every
    /// pending epoch (see `ParallelSinkExec`). Plugin acks arrive after
    /// `write_all` has sent the marker, so `write_all` must not finish before
    /// the instance acked every marker it was sent.
    awaits_acks: bool,
    ack_state: Arc<watch::Sender<AckState>>,
    /// One `PluginMsg::Init` and one metrics/ack-forwarder set for the
    /// instance, however many times `write_all` is called.
    init: std::sync::Once,
}

impl PluginSink {
    fn new(
        schema: SchemaRef,
        instance: PluginInstance,
        stream: usize,
        num_records_before_stop: Option<u64>,
        metric_metadata_id: String,
        scope: Arc<crate::shutdown::ComponentScope>,
        awaits_acks: bool,
    ) -> Self {
        Self {
            schema,
            instance,
            stream,
            num_records_before_stop,
            metric_metadata_id,
            scope,
            awaits_acks,
            ack_state: Arc::new(watch::Sender::new(AckState::default())),
            init: std::sync::Once::new(),
        }
    }

    fn sink_id(&self) -> String {
        metric_metadata_id_to_reference_name(&self.metric_metadata_id)
            .unwrap_or_else(|| self.metric_metadata_id.clone())
    }

    /// Forwards the instance's checkpoint acks and failure report,
    /// independently of batch arrival. An ack lands on the plugin output
    /// channel only after the plugin's durable flush completes, and the
    /// terminal marker rides the LAST batch — so an ack drained only from
    /// inside the batch loop is never picked up: the loop is parked on a
    /// stream that ends only once the coordinator finalizes the terminal
    /// epoch, which needs this very ack. A dedicated task breaks that cycle.
    /// Same polling pattern as process_plugin_metrics (the channel is a sync
    /// crossbeam channel; a blocking recv() here would pin an executor
    /// thread).
    fn spawn_ack_forwarder(&self) {
        let ack_receiver = self.instance.channels.output.receiver.clone();
        let ack_state = self.ack_state.clone();
        let sink_id = self.sink_id();
        let stream = self.stream;
        let instance_key = self.instance.key.clone();
        // PostPlugin-stage scope: the ack forwarder MUST outlive the plugin
        // dispatcher drain (acks still flow while the plugin flushes after
        // Terminate); the stage placement guarantees it. Exits on scope
        // cancellation once the queue is drained — the channel itself never
        // disconnects (host and plugin each hold both ends for the process's
        // life), so without watching the token this task can only ever blow
        // its drain slice. By PostPlugin-cancel time the dispatcher's flush
        // has run, so every ack it emitted has been forwarded.
        let ack_cancel = self.scope.stage_token().clone();
        self.scope.spawn(async move {
            // Cancellation is checked on the BUSY path too, not only when
            // the queue goes idle: a plugin acking faster than the 10ms
            // idle poll would otherwise never let the Empty arm run,
            // leaving the teardown-ordering invariant as the only
            // protection. Once cancellation is observed, the drain is
            // bounded to the acks queued at that moment — complete by
            // PostPlugin-cancel, so none are lost — and a plugin
            // misbehaving past Terminate cannot pin this task.
            let mut remaining_after_cancel: Option<usize> = None;
            loop {
                if remaining_after_cancel.is_none() && ack_cancel.is_cancelled() {
                    remaining_after_cancel = Some(ack_receiver.len());
                }
                if remaining_after_cancel == Some(0) {
                    debug!("Scope cancelled and queued acks forwarded; stopping ack forwarder");
                    break;
                }
                match ack_receiver.try_recv() {
                    Ok(message) => {
                        if let Some(n) = remaining_after_cancel.as_mut() {
                            *n -= 1;
                        }
                        match message.into_enum() {
                            Ok(PluginMsg::CheckpointAck { epoch }) => {
                                debug!(
                                    "Propagating checkpoint Ack with epoch {} from plugin {}",
                                    epoch.0, instance_key
                                );
                                // The sink acks once every write stream's
                                // instance has flushed the epoch.
                                let release = report_marker_at_sink_stream(
                                    &sink_id,
                                    stream,
                                    CheckpointEpoch(epoch.0),
                                );
                                // Counted by the gate before `write_all` may
                                // see the epoch acked and finish its stream.
                                ack_state.send_modify(|state| {
                                    state.unacked.remove(&epoch.0);
                                });
                                if release
                                    && let Err(e) = send(
                                        CHECKPOINT_COORDINATOR_CHANNEL,
                                        CheckpointMessage::Ack {
                                            epoch: CheckpointEpoch(epoch.0),
                                            sink_id: sink_id.clone(),
                                        },
                                    )
                                {
                                    warn!(
                                        "Stopping plugin ack forwarder: coordinator channel rejected ack for epoch {}: {}",
                                        epoch.0, e
                                    );
                                    ack_state.send_modify(|state| {
                                        state.failure.get_or_insert_with(|| {
                                            format!("coordinator rejected its ack: {e}")
                                        });
                                    });
                                    break;
                                }
                            }
                            Ok(PluginMsg::Error { message }) => {
                                error!("Plugin {} reported a failure: {}", instance_key, message);
                                ack_state.send_modify(|state| {
                                    state.failure.get_or_insert_with(|| message.to_string());
                                });
                            }
                            _ => {
                                warn!("Received unexpected message from plugin channel");
                            }
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        if ack_cancel.is_cancelled() {
                            debug!("Scope cancelled and queue drained; stopping ack forwarder");
                            break;
                        }
                        // Acks arrive at checkpoint cadence (seconds
                        // apart); ~10ms keeps the idle poll cheap while
                        // still negligible against checkpoint latency.
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    Err(TryRecvError::Disconnected) => {
                        debug!("Plugin output channel disconnected, stopping ack forwarder");
                        break;
                    }
                }
            }
        });
    }
}

#[async_trait]
impl DataSink for PluginSink {
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let metrics_recorder = get_metrics_recorder();
        let mut ack_state = self.ack_state.subscribe();

        // One Init and ONE forwarder set per instance: a second Init would be
        // a protocol error, and duplicate ack forwarders would double-ack
        // epochs.
        let mut init_send: crate::error::Result<()> = Ok(());
        self.init.call_once(|| {
            // PostPlugin-stage scope: exits on channel disconnect at plugin
            // teardown; drained after the dispatcher flush it serves.
            self.scope.spawn(process_plugin_metrics(
                self.instance.channels.metrics.receiver.clone(),
                metrics_recorder.clone(),
                self.metric_metadata_id.clone(),
                self.instance.key.clone(),
                self.scope.stage_token().clone(),
            ));
            self.spawn_ack_forwarder();

            // Sent through the shutdown-aware blocking facade (we are inside
            // a sync Once closure): bounded, and a disconnected channel
            // errors instead of panicking the writer.
            init_send = crate::plugin::send_to_plugin_blocking(
                &self.instance.channels.input.sender,
                NonExhaustive::new(PluginMsg::Init),
                &self.instance.key,
            );
        });
        init_send?;

        let mut row_count = 0;

        loop {
            // A failed plugin keeps draining its input until teardown, so its
            // report is the only way this write learns of the failure early.
            let next = tokio::select! {
                biased;
                failure = reported_failure(&mut ack_state) => {
                    return Err(crate::plugin::plugin_failure(&self.instance.key, &failure));
                }
                next = data.next() => next,
            };
            let Some(batch) = next.transpose()? else {
                break;
            };
            row_count += batch.num_rows();

            let checkpoint_messages = extract_checkpoint_messages(batch.schema().metadata());
            trace!(
                "Extracted checkpoint messages from batch metadata: {:?}",
                checkpoint_messages
            );

            crate::plugin::send_to_plugin(
                &self.instance.channels.input.sender,
                NonExhaustive::new(PluginMsg::NextBatch { data: batch.into() }),
                &self.instance.key,
            )
            .await?;

            // Send extracted checkpoint messages to plugin
            for message in checkpoint_messages {
                match message {
                    CheckpointMessage::Marker { epoch, .. } => {
                        debug!(
                            "Sending extracted checkpoint Marker with epoch {} to plugin",
                            epoch.0
                        );
                        self.ack_state.send_modify(|state| {
                            state.unacked.insert(epoch.0);
                        });
                        crate::plugin::send_to_plugin(
                            &self.instance.channels.input.sender,
                            NonExhaustive::new(PluginMsg::CheckpointMarker {
                                epoch: PluginCheckpointEpoch(epoch.0),
                            }),
                            &self.instance.key,
                        )
                        .await?;
                    }
                    CheckpointMessage::Finalizer(epoch) => {
                        debug!(
                            "Sending extracted checkpoint Finalizer with epoch {} to plugin",
                            epoch.0
                        );
                        // External plugins are bound by the Finalizer consumer
                        // contract on `CheckpointMessage::Finalizer`: idempotent,
                        // non-blocking, never gate on a specific epoch (during a
                        // terminal checkpoint, in-flight timer epochs are dropped
                        // without their Finalizers ever broadcasting). The host
                        // cannot verify an out-of-repo plugin honors this; a
                        // violating plugin stalls only its own dispatcher, which
                        // the run loop awaits under the shutdown budget before
                        // the watchdog hard-exits — it cannot wedge the process
                        // past the grace period.
                        crate::plugin::send_to_plugin(
                            &self.instance.channels.input.sender,
                            NonExhaustive::new(PluginMsg::CheckpointFinalizer {
                                epoch: PluginCheckpointEpoch(epoch.0),
                            }),
                            &self.instance.key,
                        )
                        .await?;
                    }
                    _ => {
                        // ignore other messages
                    }
                }
            }

            // Checkpoint acks and metrics are handled by the dedicated tasks
            // spawned above — nothing to drain per-batch here.

            if let Some(num_records_before_stop) = self.num_records_before_stop
                && row_count >= num_records_before_stop as usize
            {
                break;
            }
        }

        if self.awaits_acks {
            // An instance can also exit without reporting a failure (a hook
            // that panicked); its unacked epochs are then never flushed.
            let mut exit = self.instance.exit.clone();
            tokio::select! {
                biased;
                _ = ack_state.wait_for(|state| state.failure.is_some() || state.unacked.is_empty()) => {}
                () = exit.exited() => {}
            }
            let failure = {
                let state = ack_state.borrow();
                state.failure.clone().or_else(|| {
                    (!state.unacked.is_empty())
                        .then(|| format!("it exited with epochs {:?} unacked", state.unacked))
                })
            };
            if let Some(failure) = failure {
                return Err(crate::plugin::plugin_failure(&self.instance.key, &failure));
            }
        }

        Ok(row_count as u64)
    }
}

impl Debug for PluginSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginSink").finish()
    }
}

impl DisplayAs for PluginSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "PluginSink")
            }
        }
    }
}

#[derive(Debug)]
pub struct PluginSinkProvider {
    schema: SchemaRef,
    instances: PluginInstances,
    num_records_before_stop: Option<u64>,
    metric_metadata_id: String,
    telemetry: Option<Telemetry>,
    /// See [`PluginSink::scope`].
    scope: Arc<crate::shutdown::ComponentScope>,
}

impl PluginSinkProvider {
    pub fn new(
        schema: SchemaRef,
        instances: PluginInstances,
        num_records_before_stop: Option<u64>,
        metric_metadata_id: String,
        telemetry: Option<Telemetry>,
        scope: Arc<crate::shutdown::ComponentScope>,
    ) -> Self {
        Self {
            schema,
            instances,
            num_records_before_stop,
            metric_metadata_id,
            telemetry,
            scope,
        }
    }
}

#[async_trait]
impl TableProvider for PluginSinkProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Reading is not implemented for PluginSinkProvider")
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // A single-stream plugin's input is narrowed to one stream on both the
        // single-sink and the fan-out path. A partitioned one runs an instance
        // per write stream, each bound to its stream.
        let partitions = input.output_partitioning().partition_count();
        let awaits_acks = matches!(self.instances, PluginInstances::Partitioned(_));
        if let PluginInstances::Partitioned(plugin) = &self.instances {
            plugin.check_width(
                partitions,
                &format!(
                    "its input is {partitions} streams wide; set `parallelism` on the sink \
                     to run it at a supported width"
                ),
            )?;
        }
        let instances = self
            .instances
            .resolve(partitions, Some(self.schema.clone()))
            .await?;
        let sinks = instances
            .into_iter()
            .enumerate()
            .map(|(stream, instance)| {
                let sink = Arc::new(PluginSink::new(
                    self.schema.clone(),
                    instance,
                    stream,
                    self.num_records_before_stop,
                    self.metric_metadata_id.clone(),
                    self.scope.clone(),
                    awaits_acks,
                ));
                Arc::new(WrappingDataSink::new(
                    sink,
                    self.metric_metadata_id.clone(),
                    None,
                    self.telemetry.as_ref(),
                )) as Arc<dyn DataSink>
            })
            .collect();
        Ok(Arc::new(ParallelSinkExec::new(
            input,
            ParallelSinks::PerPartition(sinks),
            get_reference_name_from_metric_key(&self.metric_metadata_id),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::checkpoint_management::now_ms;
    use abi_stable::external_types::crossbeam_channel as ffi_channel;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::arrow::array::Int64Array;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::time::{Duration, Instant};
    use streamling_plugin::PluginChannels;
    use streamling_plugin::ffi::{PluginChannel, PluginMetricsChannel};

    fn test_channels() -> Arc<PluginChannels> {
        Arc::new(PluginChannels {
            input: PluginChannel::new(ffi_channel::bounded(64)),
            output: PluginChannel::new(ffi_channel::bounded(64)),
            metrics: PluginMetricsChannel::new(ffi_channel::bounded(64)),
        })
    }

    /// Regression: in job mode the terminal checkpoint marker rides the LAST
    /// batch, so the plugin's `CheckpointAck` lands on the output channel only
    /// after `write_all`'s batch loop has already parked on the exhausted
    /// stream. Ack propagation must therefore not be coupled to batch arrival:
    /// the coordinator has to receive the ack even though no further batch
    /// ever shows up (the upstream source is itself waiting on epoch
    /// finalization before ending its stream).
    ///
    /// `#[serial]`: the coordinator channel is a process-wide global.
    #[tokio::test]
    #[serial]
    async fn plugin_sink_forwards_ack_that_arrives_after_the_last_batch() {
        let channels = test_channels();
        let (coordinator_rx, coordinator_sub_id) =
            subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);

        // Fake plugin: consume the input channel; when the checkpoint marker
        // arrives, ack it only after a delay — strictly after the sink's
        // batch loop has seen stream end. Exits when the input channel closes.
        let plugin_input_rx = channels.input.receiver.clone();
        let plugin_output_tx = channels.output.sender.clone();
        let fake_plugin = std::thread::spawn(move || {
            while let Ok(msg) = plugin_input_rx.recv() {
                if let Ok(PluginMsg::CheckpointMarker { epoch }) = msg.into_enum() {
                    std::thread::sleep(Duration::from_millis(200));
                    // Plugin-SIDE send on a dedicated test thread — the lint
                    // guards HOST-side async contexts, which this is not.
                    #[allow(clippy::disallowed_methods)]
                    plugin_output_tx
                        .send(NonExhaustive::new(PluginMsg::CheckpointAck { epoch }))
                        .expect("test plugin failed to send ack");
                    break;
                }
            }
        });

        // A single (final) batch carrying the terminal marker in its metadata.
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                created_at_ms: now_ms(),
            }],
        );
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            metadata,
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("failed to build test batch");
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch)]),
        ));

        let sink = PluginSink::new(
            schema,
            PluginInstance {
                key: "ack_after_last_batch_sink".to_string(),
                channels: channels.clone(),
                exit: crate::plugin::track_exit(Box::pin(std::future::pending())).1,
            },
            0,
            None,
            "plugin::ack_after_last_batch_sink".to_string(),
            crate::shutdown::ComponentScope::detached("test"),
            false,
        );
        let task_ctx = Arc::new(TaskContext::default());
        let rows = tokio::time::timeout(Duration::from_secs(10), sink.write_all(stream, &task_ctx))
            .await
            .expect("write_all must complete once its input stream ends")
            .expect("write_all failed");
        assert_eq!(rows, 1);

        // The ack must reach the coordinator even though no further batch
        // arrives after the marker.
        let deadline = Instant::now() + Duration::from_secs(5);
        let (epoch, sink_id) = loop {
            match coordinator_rx.try_recv() {
                Ok(CheckpointMessage::Ack { epoch, sink_id }) => break (epoch, sink_id),
                Ok(_) => {} // unrelated coordinator traffic
                Err(_) => {
                    assert!(
                        Instant::now() < deadline,
                        "coordinator never received the plugin's checkpoint ack — \
                         ack propagation is coupled to batch arrival again"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        };
        assert_eq!(epoch, CheckpointEpoch(1));
        assert_eq!(sink_id, "ack_after_last_batch_sink");

        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, coordinator_sub_id);
        fake_plugin.join().expect("test plugin thread panicked");
    }
}

#[cfg(test)]
mod partitioned_tests {
    use super::*;
    use crate::app_config::AppConfig;
    use crate::plugin::partitioned::{PartitionedPlugin, PluginInstances, PluginKind};
    use crate::plugin::test_plugins::{
        self, ACK_DELAY_MS, FAIL_BATCHES_AT, FAIL_MARKERS, ID_STRIDE, NODE, ROWS, SINK,
        SLOW_ACK_AT, SOURCE, batch, ids, marker, shut_down, source_schema,
    };
    use datafusion::prelude::SessionContext;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    const EXIT_BOUND: Duration = Duration::from_secs(5);

    fn options(node: &str, extra: &[(&str, &str)]) -> HashMap<String, String> {
        extra
            .iter()
            .chain(&[(NODE, node)])
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn describe(
        node: &str,
        plugin_type: &str,
        kind: PluginKind,
        extra: &[(&str, &str)],
    ) -> Arc<PartitionedPlugin> {
        test_plugins::install();
        let input_schema = (kind != PluginKind::Source).then(source_schema);
        PartitionedPlugin::describe(
            &AppConfig::load().unwrap(),
            node,
            plugin_type,
            kind,
            input_schema,
            options(node, extra),
        )
        .unwrap()
        .unwrap()
    }

    async fn scan_source(
        node: &str,
        source: &Arc<PartitionedPlugin>,
        width: usize,
    ) -> Arc<dyn ExecutionPlan> {
        PluginSourceProvider::new(
            source_schema(),
            PluginInstances::Partitioned(source.clone()),
            width,
            10,
            format!("app::{node}"),
            crate::shutdown::ComponentScope::detached("test"),
        )
        .scan(&SessionContext::new().state(), None, &[], None)
        .await
        .unwrap()
    }

    async fn sink_plan(
        node: &str,
        sink: &Arc<PartitionedPlugin>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        PluginSinkProvider::new(
            source_schema(),
            PluginInstances::Partitioned(sink.clone()),
            None,
            format!("app::{node}"),
            None,
            crate::shutdown::ComponentScope::detached("test"),
        )
        .insert_into(&SessionContext::new().state(), input, InsertOp::Append)
        .await
        .unwrap()
    }

    fn marker_epochs(batch: &RecordBatch) -> Vec<u64> {
        extract_checkpoint_messages(batch.schema().metadata())
            .into_iter()
            .filter_map(|m| match m {
                CheckpointMessage::Marker { epoch, .. } => Some(epoch.0),
                _ => None,
            })
            .collect()
    }

    /// Waits for the coordinator to receive an ack for `epoch` from `sink_id`.
    async fn next_ack(
        coordinator: &crossbeam::channel::Receiver<CheckpointMessage>,
        sink_id: &str,
        epoch: u64,
    ) -> Option<()> {
        let deadline = Instant::now() + EXIT_BOUND;
        while Instant::now() < deadline {
            match coordinator.try_recv() {
                Ok(CheckpointMessage::Ack {
                    epoch: acked,
                    sink_id: acker,
                }) if acked.0 == epoch && acker == sink_id => {
                    return Some(());
                }
                Ok(_) => {}
                Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
        None
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn partitioned_source_routes_each_partition_to_its_own_instance() {
        let source = describe("routed_source", SOURCE, PluginKind::Source, &[(ROWS, "25")]);
        let exec = scan_source("routed_source", &source, 3).await;
        assert_eq!(exec.output_partitioning().partition_count(), 3);

        let streams = (0..3).map(|partition| {
            let stream = exec
                .execute(partition, Arc::new(TaskContext::default()))
                .unwrap();
            stream.map(|b| ids(&b.unwrap())).concat()
        });
        let per_partition = tokio::time::timeout(EXIT_BOUND, futures::future::join_all(streams))
            .await
            .expect("every partition ends once its instance completes");

        for (partition, mut received) in per_partition.into_iter().enumerate() {
            received.sort();
            let expected: Vec<i64> = (0..25).map(|n| partition as i64 * ID_STRIDE + n).collect();
            assert_eq!(received, expected, "partition {partition}");
        }
        for (key, exit) in shut_down(&source).await {
            assert!(exit.is_ok(), "{key}: {exit:?}");
        }
    }

    /// Each partition subscribes to the coordinator itself and forwards the
    /// marker to its own instance, so every stream carries exactly one copy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn each_source_partition_carries_one_copy_of_a_marker() {
        const EPOCH: u64 = 90_001;
        let source = describe("marked_source", SOURCE, PluginKind::Source, &[]);
        let exec = scan_source("marked_source", &source, 2).await;
        let mut streams: Vec<_> = (0..2)
            .map(|p| exec.execute(p, Arc::new(TaskContext::default())).unwrap())
            .collect();
        let deadline = Instant::now() + EXIT_BOUND;
        while !(0..2).all(|p| test_plugins::log(&format!("marked_source[{p}]")).initialized) {
            assert!(Instant::now() < deadline, "instances never initialized");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        send(CHECKPOINT_COORDINATOR_CHANNEL, marker(EPOCH)).unwrap();

        for (partition, stream) in streams.iter_mut().enumerate() {
            let mut copies = 0;
            while copies == 0 {
                let batch = tokio::time::timeout(EXIT_BOUND, stream.next())
                    .await
                    .expect("the marker must come back out")
                    .unwrap()
                    .unwrap();
                copies += marker_epochs(&batch)
                    .iter()
                    .filter(|e| **e == EPOCH)
                    .count();
            }
            assert_eq!(copies, 1, "partition {partition}");
            assert_eq!(
                test_plugins::log(&format!("marked_source[{partition}]")).markers,
                [EPOCH],
                "partition {partition} must forward the marker to its own instance only"
            );
        }
        shut_down(&source).await;
        for stream in streams {
            let rest: Vec<RecordBatch> = stream.map(|b| b.unwrap()).collect().await;
            assert!(rest.iter().all(|b| !marker_epochs(b).contains(&EPOCH)));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn source_failure_ends_its_stream_with_an_error() {
        let source = describe(
            "failing_source",
            SOURCE,
            PluginKind::Source,
            &[(FAIL_MARKERS, "true")],
        );
        let exec = scan_source("failing_source", &source, 1).await;
        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let deadline = Instant::now() + EXIT_BOUND;
        while !test_plugins::log("failing_source[0]").initialized {
            assert!(Instant::now() < deadline, "instance never initialized");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        send(CHECKPOINT_COORDINATOR_CHANNEL, marker(90_002)).unwrap();

        let failure = tokio::time::timeout(EXIT_BOUND, async {
            loop {
                match stream.next().await {
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => break e.to_string(),
                    None => panic!("the stream ended without reporting the failure"),
                }
            }
        })
        .await
        .expect("the failure must reach the stream without waiting for teardown");
        assert!(failure.contains("marker failed on purpose"), "{failure}");
        shut_down(&source).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn partitioned_sink_writes_each_stream_through_its_own_instance() {
        let sink = describe("per_stream_sink", SINK, PluginKind::Sink, &[]);
        // Each write ends only once its instance acked the stream's marker,
        // which it does after processing the rows before it.
        let plan = sink_plan(
            "per_stream_sink",
            &sink,
            test_plugins::input(vec![
                vec![batch(&[1, 2], &[marker(1)])],
                vec![batch(&[3], &[marker(1)])],
                vec![batch(&[4, 5, 6], &[marker(1)])],
            ]),
        )
        .await;

        let written: Vec<RecordBatch> = plan
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .map(|b| b.unwrap())
            .collect()
            .await;
        assert_eq!(written.len(), 1);

        let expected: [&[i64]; 3] = [&[1, 2], &[3], &[4, 5, 6]];
        for (partition, expected) in expected.iter().enumerate() {
            assert_eq!(
                test_plugins::log(&format!("per_stream_sink[{partition}]")).ids,
                *expected
            );
        }
        shut_down(&sink).await;
    }

    /// Each instance's own state is keyed by its partition; the node-wide
    /// state is one namespace they all opt into.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn partition_instances_keep_their_own_state_and_share_the_node_state() {
        use streamling_config::app_config::{
            SqliteStateBackendConfig, StateBackendConfig, StateBackendType,
        };
        use streamling_plugin::PluginInstanceContext;
        use streamling_plugin::api::PluginStateBackendFactory;

        let database = std::env::temp_dir().join(format!(
            "partitioned_plugin_state_{}.db",
            uuid::Uuid::new_v4()
        ));
        let mut app_config = AppConfig::load().unwrap();
        app_config.state_backend = StateBackendConfig {
            backend_type: StateBackendType::Sqlite,
            postgres: None,
            sqlite: Some(SqliteStateBackendConfig {
                database_path: database.to_string_lossy().to_string(),
                max_connections: None,
                state_table_name: None,
            }),
        };
        test_plugins::install();
        let sink = PartitionedPlugin::describe(
            &app_config,
            "stateful_sink",
            SINK,
            PluginKind::Sink,
            Some(source_schema()),
            options("stateful_sink", &[(test_plugins::WRITE_STATE, "true")]),
        )
        .unwrap()
        .unwrap();
        let plan = sink_plan(
            "stateful_sink",
            &sink,
            test_plugins::input(vec![
                vec![batch(&[1], &[marker(1)])],
                vec![batch(&[2], &[marker(1)])],
            ]),
        )
        .await;
        plan.execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .for_each(|b| async move {
                b.unwrap();
            })
            .await;
        shut_down(&sink).await;

        let state_config =
            || crate::plugin::create_plugin_state_backend_config(&app_config, "stateful_sink");
        for partition in 0..2u32 {
            let own = PluginStateBackendFactory::for_partition(
                state_config(),
                &PluginInstanceContext {
                    reference_name: "stateful_sink".into(),
                    partition_index: partition,
                    partition_count: 2,
                },
            );
            assert_eq!(own.create::<u32>().get().await.unwrap(), Some(partition));
        }
        let shared = PluginStateBackendFactory::new(state_config()).create_shared::<u32>();
        assert_eq!(
            shared.get().await.unwrap(),
            None,
            "no instance wrote the shared default key"
        );
        for partition in 0..2u32 {
            assert_eq!(
                shared
                    .get_kv(&format!("partition_{partition}"))
                    .await
                    .unwrap(),
                Some(partition)
            );
        }
        let _ = std::fs::remove_file(database);
    }

    /// Stream 0 ends right after its marker while its instance is still
    /// flushing. Finishing the stream must not release the epoch: the sink
    /// acks it once, after every instance flushed it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    // The spawned write is joined below; a unit test has no drain ladder to
    // track it.
    #[allow(clippy::disallowed_methods)]
    async fn an_epoch_is_acked_once_after_every_instance_flushed_it() {
        const EPOCH: u64 = 90_003;
        let (coordinator, subscriber) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);
        let sink = describe(
            "gated_sink",
            SINK,
            PluginKind::Sink,
            &[(SLOW_ACK_AT, "0"), (ACK_DELAY_MS, "300")],
        );
        let plan = sink_plan(
            "gated_sink",
            &sink,
            test_plugins::input(vec![
                vec![batch(&[1], &[marker(EPOCH)])],
                vec![batch(&[2], &[marker(EPOCH)])],
            ]),
        )
        .await;

        let running = tokio::spawn(async move {
            plan.execute(0, Arc::new(TaskContext::default()))
                .unwrap()
                .map(|b| b.unwrap())
                .collect::<Vec<_>>()
                .await
        });
        next_ack(&coordinator, "gated_sink", EPOCH)
            .await
            .expect("the epoch must be acked");
        assert_eq!(
            test_plugins::log("gated_sink[0]").markers,
            [EPOCH],
            "the ack must wait for the slow instance's flush"
        );

        running.await.unwrap();
        assert!(
            next_ack(&coordinator, "gated_sink", EPOCH).await.is_none(),
            "the epoch must be acked exactly once"
        );
        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, subscriber);
        shut_down(&sink).await;
    }

    /// Instance 1 fails and loses its rows; instance 0 flushes the epoch
    /// afterwards. The epoch covers the lost rows, so it must never be acked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn a_failed_instance_blocks_its_siblings_acks() {
        const EPOCH: u64 = 90_005;
        let (coordinator, subscriber) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);
        let sink = describe(
            "poisoned_sink",
            SINK,
            PluginKind::Sink,
            &[
                (FAIL_BATCHES_AT, "1"),
                (SLOW_ACK_AT, "0"),
                (ACK_DELAY_MS, "300"),
            ],
        );
        let plan = sink_plan(
            "poisoned_sink",
            &sink,
            test_plugins::input(vec![
                vec![batch(&[1], &[marker(EPOCH)])],
                vec![batch(&[2], &[marker(EPOCH)])],
            ]),
        )
        .await;

        let result = plan
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .next()
            .await
            .unwrap();
        assert!(result.is_err(), "the failed instance fails the write");

        let deadline = Instant::now() + EXIT_BOUND;
        while !test_plugins::log("poisoned_sink[0]")
            .markers
            .contains(&EPOCH)
        {
            assert!(
                Instant::now() < deadline,
                "instance 0 never flushed the epoch"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !coordinator.try_iter().any(|m| matches!(
                m,
                CheckpointMessage::Ack { epoch, sink_id } if epoch.0 == EPOCH && sink_id == "poisoned_sink"
            )),
            "an epoch covering the failed instance's rows must not be acked"
        );
        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, subscriber);
        shut_down(&sink).await;
    }

    /// A panicking hook ends the instance without a failure report, so the
    /// write must notice the instance exited rather than wait for its ack.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_write_stops_waiting_for_acks_once_its_instance_exits() {
        let sink = describe(
            "panicking_sink",
            SINK,
            PluginKind::Sink,
            &[(test_plugins::PANIC_MARKERS, "true")],
        );
        let plan = sink_plan(
            "panicking_sink",
            &sink,
            test_plugins::input(vec![vec![batch(&[1], &[marker(90_006)])]]),
        )
        .await;

        // The run loop polls every instance's execution future; so does this.
        let executions = futures::future::join_all(
            sink.take_execution_futures()
                .into_iter()
                .map(|(_, execution)| execution),
        );
        let write = async {
            plan.execute(0, Arc::new(TaskContext::default()))
                .unwrap()
                .next()
                .await
                .unwrap()
        };
        let (result, exits) =
            tokio::time::timeout(EXIT_BOUND, async { tokio::join!(write, executions) })
                .await
                .expect("the write must not wait on an instance that exited");

        assert!(exits.iter().all(|exit| exit.is_err()), "{exits:?}");
        let err = result
            .expect_err("unacked epochs must fail the write")
            .to_string();
        assert!(err.contains("exited"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn failed_sink_instance_fails_its_write_and_never_acks() {
        const EPOCH: u64 = 90_004;
        let (coordinator, subscriber) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);
        let sink = describe(
            "failing_sink",
            SINK,
            PluginKind::Sink,
            &[(FAIL_BATCHES_AT, "0")],
        );
        let plan = sink_plan(
            "failing_sink",
            &sink,
            test_plugins::input(vec![vec![batch(&[1], &[marker(EPOCH)])]]),
        )
        .await;

        let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let result = tokio::time::timeout(EXIT_BOUND, stream.next())
            .await
            .expect("the failure must reach the write without waiting for teardown")
            .unwrap();

        let err = result.expect_err("the write must fail").to_string();
        assert!(err.contains("batch failed on purpose"), "{err}");
        assert!(
            next_ack(&coordinator, "failing_sink", EPOCH)
                .await
                .is_none()
        );
        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, subscriber);
        shut_down(&sink).await;
    }
}
