use crate::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointEpoch, CheckpointMessage,
    enrich_batch_metadata_with_checkpoints, extract_checkpoint_messages, now_ms,
};
use crate::operators::parallel_sink::ParallelSinkExec;
use crate::utils::batch::enrich_batch_with_metadata;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::common::{DataFusionError, not_impl_err, project_schema};
use datafusion::datasource::sink::DataSink;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::fmt;
use std::fmt::{Debug, Formatter};

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
use streamling_plugin::{PluginChannels, PluginCheckpointEpoch, PluginMsg};
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

#[derive(Debug)]
struct PluginSourceExec {
    schema: SchemaRef,
    /// Column indices into the plugin's full schema, pushed down from the scan.
    /// The plugin always emits full rows; the projection is applied here so the
    /// emitted batches match `schema` (DataFusion 54 resolves downstream column
    /// indices against the projected scan schema).
    projection: Option<Vec<usize>>,
    plugin_channels: Arc<PluginChannels>,
    cached_properties: Arc<PlanProperties>,
    internal_buffer_size: u32,
    metric_metadata_id: String,
    scope: Arc<crate::shutdown::ComponentScope>,
}

impl PluginSourceExec {
    pub fn new(
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        plugin_channels: Arc<PluginChannels>,
        internal_buffer_size: u32,
        metric_metadata_id: String,
        scope: Arc<crate::shutdown::ComponentScope>,
    ) -> Self {
        let cached_properties = Self::compute_properties(schema.clone());
        Self {
            schema,
            projection,
            plugin_channels,
            cached_properties: Arc::new(cached_properties),
            internal_buffer_size,
            metric_metadata_id,
            scope,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1), // TODO
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        crate::plugin::send_to_plugin_blocking(
            &self.plugin_channels.input.sender,
            NonExhaustive::new(PluginMsg::Init),
            &self.metric_metadata_id,
        )?;

        let (checkpoint_receiver, checkpoint_subscriber_id) =
            subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);

        let plugin_input_sender = self.plugin_channels.input.sender.clone();
        let plugin_output_receiver = self.plugin_channels.output.receiver.clone();

        let mut builder = RecordBatchReceiverStreamBuilder::new(
            self.schema.clone(),
            self.internal_buffer_size as usize,
        );

        let tx = builder.tx();
        let metrics_receiver = self.plugin_channels.metrics.receiver.clone();
        let metrics_recorder = get_metrics_recorder();
        let metric_metadata_id = self.metric_metadata_id.clone();
        let plugin_label = self.metric_metadata_id.clone();
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
                                    // the sinks.
                                    info!(
                                        "PluginSourceExec: source reported completion; ending its record-batch stream"
                                    );
                                    break 'outer;
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
    plugin_channels: Arc<PluginChannels>,
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
        plugin_channels: Arc<PluginChannels>,
        internal_buffer_size: u32,
        metric_metadata_id: String,
        scope: Arc<crate::shutdown::ComponentScope>,
    ) -> Self {
        Self {
            schema,
            plugin_channels,
            internal_buffer_size,
            metric_metadata_id,
            scope,
        }
    }

    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema_ref: SchemaRef,
        plugin_channels: Arc<PluginChannels>,
        internal_buffer_size: u32,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema_projected = project_schema(&schema_ref, projections)?;
        // An identity projection (all columns in order) needs no per-batch work.
        let projection = projections
            .filter(|indices| !indices.iter().copied().eq(0..schema_ref.fields().len()))
            .cloned();
        Ok(Arc::new(PluginSourceExec::new(
            schema_projected,
            projection,
            plugin_channels,
            internal_buffer_size,
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
        self.create_physical_plan(
            projection,
            self.schema.clone(),
            self.plugin_channels.clone(),
            self.internal_buffer_size,
        )
        .await
    }
}

struct PluginSink {
    schema: SchemaRef,
    plugin_channels: Arc<PluginChannels>,
    num_records_before_stop: Option<u64>, // for integration tests only!
    metric_metadata_id: String,
    /// PostPlugin-stage scope for the metrics and ack forwarders: both serve
    /// the plugin dispatcher's flush, so they drain after it.
    scope: Arc<crate::shutdown::ComponentScope>,
    /// One `PluginMsg::Init` and one metrics/ack-forwarder set regardless of
    /// how many partition streams `ParallelSinkExec` writes concurrently.
    init: std::sync::Once,
}

impl PluginSink {
    fn new(
        schema: SchemaRef,
        plugin_channels: Arc<PluginChannels>,
        num_records_before_stop: Option<u64>,
        metric_metadata_id: String,
        scope: Arc<crate::shutdown::ComponentScope>,
    ) -> Self {
        Self {
            schema,
            plugin_channels,
            num_records_before_stop,
            metric_metadata_id,
            scope,
            init: std::sync::Once::new(),
        }
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

        // One Init and ONE forwarder set for the whole sink, no matter how
        // many partition streams ParallelSinkExec writes concurrently — a
        // per-stream Init would be a protocol error, and duplicate ack
        // forwarders would double-ack epochs.
        let mut init_send: crate::error::Result<()> = Ok(());
        self.init.call_once(|| {
            // PostPlugin-stage scope: exits on channel disconnect at plugin
            // teardown; drained after the dispatcher flush it serves.
            self.scope.spawn(process_plugin_metrics(
                self.plugin_channels.metrics.receiver.clone(),
                metrics_recorder.clone(),
                self.metric_metadata_id.clone(),
                self.scope.stage_token().clone(),
            ));

            // Forward plugin checkpoint acks to the coordinator independently of
            // batch arrival. An ack lands on the plugin output channel only after
            // the plugin's durable flush completes, and the terminal marker rides
            // the LAST batch — so an ack drained only from inside the batch loop
            // is never picked up: the loop is parked on a stream that ends only
            // once the coordinator finalizes the terminal epoch, which needs this
            // very ack. A dedicated task breaks that cycle. Same polling pattern
            // as process_plugin_metrics (the channel is a sync crossbeam channel;
            // a blocking recv() here would pin an executor thread).
            let ack_receiver = self.plugin_channels.output.receiver.clone();
            let sink_id = metric_metadata_id_to_reference_name(&self.metric_metadata_id)
                .unwrap_or_else(|| self.metric_metadata_id.clone());
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
                                        "Propagating checkpoint Ack with epoch {} from plugin",
                                        epoch.0
                                    );
                                    if let Err(e) = send(
                                        CHECKPOINT_COORDINATOR_CHANNEL,
                                        CheckpointMessage::Ack {
                                            epoch: CheckpointEpoch(epoch.0),
                                            sink_id: sink_id.clone(),
                                        },
                                    ) {
                                        warn!(
                                            "Stopping plugin ack forwarder: coordinator channel rejected ack for epoch {}: {}",
                                            epoch.0, e
                                        );
                                        break;
                                    }
                                }
                                _ => {
                                    warn!("Received unexpected message from plugin channel");
                                }
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            if ack_cancel.is_cancelled() {
                                debug!(
                                    "Scope cancelled and queue drained; stopping ack forwarder"
                                );
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

            // Sent through the shutdown-aware blocking facade (we are inside
            // a sync Once closure): bounded, and a disconnected channel
            // errors instead of panicking the writer.
            init_send = crate::plugin::send_to_plugin_blocking(
                &self.plugin_channels.input.sender,
                NonExhaustive::new(PluginMsg::Init),
                &self.metric_metadata_id,
            );
        });
        init_send?;

        let mut row_count = 0;

        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();

            let checkpoint_messages = extract_checkpoint_messages(batch.schema().metadata());
            trace!(
                "Extracted checkpoint messages from batch metadata: {:?}",
                checkpoint_messages
            );

            crate::plugin::send_to_plugin(
                &self.plugin_channels.input.sender,
                NonExhaustive::new(PluginMsg::NextBatch { data: batch.into() }),
                &self.metric_metadata_id,
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
                        crate::plugin::send_to_plugin(
                            &self.plugin_channels.input.sender,
                            NonExhaustive::new(PluginMsg::CheckpointMarker {
                                epoch: PluginCheckpointEpoch(epoch.0),
                            }),
                            &self.metric_metadata_id,
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
                            &self.plugin_channels.input.sender,
                            NonExhaustive::new(PluginMsg::CheckpointFinalizer {
                                epoch: PluginCheckpointEpoch(epoch.0),
                            }),
                            &self.metric_metadata_id,
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
    plugin_channels: Arc<PluginChannels>,
    num_records_before_stop: Option<u64>,
    metric_metadata_id: String,
    telemetry: Option<Telemetry>,
    /// See [`PluginSink::scope`].
    scope: Arc<crate::shutdown::ComponentScope>,
}

impl PluginSinkProvider {
    pub fn new(
        schema: SchemaRef,
        plugin_channels: Arc<PluginChannels>,
        num_records_before_stop: Option<u64>,
        metric_metadata_id: String,
        telemetry: Option<Telemetry>,
        scope: Arc<crate::shutdown::ComponentScope>,
    ) -> Self {
        Self {
            schema,
            plugin_channels,
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
        let sink = Arc::new(PluginSink::new(
            self.schema.clone(),
            self.plugin_channels.clone(),
            self.num_records_before_stop,
            self.metric_metadata_id.clone(),
            self.scope.clone(),
        ));
        let telemetry_sink = Arc::new(WrappingDataSink::new(
            sink,
            self.metric_metadata_id.clone(),
            None,
            self.telemetry.as_ref(),
        ));
        // Always one write stream: planning marks plugin sinks `Placement::Single`,
        // so the input is coalesced above this point on both the single-sink and
        // the fan-out path.
        Ok(Arc::new(ParallelSinkExec::new(
            input,
            telemetry_sink,
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
            channels.clone(),
            None,
            "plugin::ack_after_last_batch_sink".to_string(),
            crate::shutdown::ComponentScope::detached("test"),
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
