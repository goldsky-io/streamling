use crate::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointEpoch, CheckpointMessage,
    enrich_batch_metadata_with_checkpoints, extract_checkpoint_messages, now_ms,
};
use crate::utils::batch::enrich_batch_with_metadata;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::common::{DataFusionError, not_impl_err, project_schema};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
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
}

impl PluginSourceExec {
    pub fn new(
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        plugin_channels: Arc<PluginChannels>,
        internal_buffer_size: u32,
        metric_metadata_id: String,
    ) -> Self {
        let cached_properties = Self::compute_properties(schema.clone());
        Self {
            schema,
            projection,
            plugin_channels,
            cached_properties: Arc::new(cached_properties),
            internal_buffer_size,
            metric_metadata_id,
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
        write!(f, "PluginSourceExec")
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
        // A disconnected input channel means the plugin already exited (e.g.
        // it died during startup): surface an execution error instead of
        // panicking the executor thread.
        self.plugin_channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Init))
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "plugin input channel closed before Init could be sent \
                     (plugin exited early?): {e}"
                ))
            })?;

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
        let projection = self.projection.clone();
        let schema_for_flush = self.schema.clone();

        builder.spawn(async move {
            tokio::spawn(process_plugin_metrics(
                metrics_receiver,
                metrics_recorder,
                metric_metadata_id,
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
                                _ => {}
                            }
                        }

                        if checkpoint_receiver.is_empty() {
                            continue;
                        } else {
                            break;
                        }
                    } else {
                        tokio::task::yield_now().await;
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
                            plugin_input_sender
                                .send(NonExhaustive::new(PluginMsg::CheckpointMarker {
                                    epoch: PluginCheckpointEpoch(epoch.0),
                                }))
                                .unwrap();
                        }
                        Ok(CheckpointMessage::Finalizer(epoch)) => {
                            debug!(
                                "Propagating checkpoint Finalizer with epoch {} to plugin",
                                epoch.0
                            );
                            plugin_input_sender
                                .send(NonExhaustive::new(PluginMsg::CheckpointFinalizer {
                                    epoch: PluginCheckpointEpoch(epoch.0),
                                }))
                                .unwrap();
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
                let empty = RecordBatch::new_empty(schema_for_flush.clone());
                let mut metadata = schema_for_flush.metadata().clone();
                enrich_batch_metadata_with_checkpoints(&mut metadata, &checkpoint_buffer);
                match enrich_batch_with_metadata(empty, metadata) {
                    Ok(batch) => {
                        if tx.send(Ok(batch)).await.is_err() {
                            warn!(
                                "PluginSourceExec: downstream closed before the synthetic final batch could be sent"
                            );
                        }
                    }
                    Err(e) => warn!(
                        "PluginSourceExec: failed to build synthetic final batch: {:?}",
                        e
                    ),
                }
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
}

impl PluginSourceProvider {
    pub fn new(
        schema: SchemaRef,
        plugin_channels: Arc<PluginChannels>,
        internal_buffer_size: u32,
        metric_metadata_id: String,
    ) -> Self {
        Self {
            schema,
            plugin_channels,
            internal_buffer_size,
            metric_metadata_id,
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
}

impl PluginSink {
    fn new(
        schema: SchemaRef,
        plugin_channels: Arc<PluginChannels>,
        num_records_before_stop: Option<u64>,
        metric_metadata_id: String,
    ) -> Self {
        Self {
            schema,
            plugin_channels,
            num_records_before_stop,
            metric_metadata_id,
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
        tokio::spawn(process_plugin_metrics(
            self.plugin_channels.metrics.receiver.clone(),
            metrics_recorder.clone(),
            self.metric_metadata_id.clone(),
        ));

        // Forward plugin checkpoint acks to the coordinator independently of
        // batch arrival. An ack lands on the plugin output channel only after
        // the plugin's durable flush completes, and the terminal marker rides
        // the LAST batch — so an ack drained only from inside the batch loop
        // is never picked up: the loop is parked on a stream that ends only
        // once the coordinator finalizes the terminal epoch, which needs this
        // very ack. A dedicated task breaks that cycle. Same polling pattern
        // as process_plugin_metrics (the channel is a sync crossbeam channel;
        // a blocking recv() here would pin an executor thread); exits when the
        // plugin output channel disconnects at plugin teardown.
        let ack_receiver = self.plugin_channels.output.receiver.clone();
        let sink_id = metric_metadata_id_to_reference_name(&self.metric_metadata_id)
            .unwrap_or_else(|| self.metric_metadata_id.clone());
        tokio::spawn(async move {
            loop {
                match ack_receiver.try_recv() {
                    Ok(message) => match message.into_enum() {
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
                    },
                    Err(TryRecvError::Empty) => {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                    Err(TryRecvError::Disconnected) => {
                        debug!("Plugin output channel disconnected, stopping ack forwarder");
                        break;
                    }
                }
            }
        });

        let mut row_count = 0;

        self.plugin_channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Init))
            .unwrap();

        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();

            let checkpoint_messages = extract_checkpoint_messages(batch.schema().metadata());
            trace!(
                "Extracted checkpoint messages from batch metadata: {:?}",
                checkpoint_messages
            );

            self.plugin_channels
                .input
                .sender
                .send(NonExhaustive::new(PluginMsg::NextBatch {
                    data: batch.into(),
                }))
                .unwrap();

            // Send extracted checkpoint messages to plugin
            for message in checkpoint_messages {
                match message {
                    CheckpointMessage::Marker { epoch, .. } => {
                        debug!(
                            "Sending extracted checkpoint Marker with epoch {} to plugin",
                            epoch.0
                        );
                        self.plugin_channels
                            .input
                            .sender
                            .send(NonExhaustive::new(PluginMsg::CheckpointMarker {
                                epoch: PluginCheckpointEpoch(epoch.0),
                            }))
                            .unwrap();
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
                        self.plugin_channels
                            .input
                            .sender
                            .send(NonExhaustive::new(PluginMsg::CheckpointFinalizer {
                                epoch: PluginCheckpointEpoch(epoch.0),
                            }))
                            .unwrap();
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
}

impl PluginSinkProvider {
    pub fn new(
        schema: SchemaRef,
        plugin_channels: Arc<PluginChannels>,
        num_records_before_stop: Option<u64>,
        metric_metadata_id: String,
        telemetry: Option<Telemetry>,
    ) -> Self {
        Self {
            schema,
            plugin_channels,
            num_records_before_stop,
            metric_metadata_id,
            telemetry,
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
        ));
        let telemetry_sink = Arc::new(WrappingDataSink::new(
            sink,
            self.metric_metadata_id.clone(),
            None,
            self.telemetry.as_ref(),
        ));
        Ok(Arc::new(DataSinkExec::new(input, telemetry_sink, None)))
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
