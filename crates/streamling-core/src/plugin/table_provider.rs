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

use crate::checkpoints::channels::subscribe;
use crate::checkpoints::checkpoint_management::send_checkpoint_ack;
use crate::operators::wrapping::WrappingDataSink;
use crate::plugin::telemetry::process_plugin_metrics;
use crate::telemetry::provider::get_reference_name_from_metric_key;
use crate::telemetry::recorder::get_metrics_recorder;
use crate::topology::Telemetry;
use crate::utils::metrics::metric_metadata_id_to_reference_name;
use abi_stable::nonexhaustive_enum::NonExhaustive;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use std::sync::Arc;
use streamling_plugin::{PluginChannels, PluginCheckpointEpoch, PluginMsg};
use tracing::log::trace;
use tracing::{debug, error, warn};

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
        self.plugin_channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Init))
            .unwrap();

        let checkpoint_receiver = subscribe(CHECKPOINT_COORDINATOR_CHANNEL);

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

            loop {
                loop {
                    if !plugin_output_receiver.is_empty() {
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
    /// One `PluginMsg::Init` and one metrics-forwarder task regardless of how
    /// many partition streams `ParallelSinkExec` writes concurrently.
    init: std::sync::Once,
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
        self.init.call_once(|| {
            tokio::spawn(process_plugin_metrics(
                self.plugin_channels.metrics.receiver.clone(),
                metrics_recorder.clone(),
                self.metric_metadata_id.clone(),
            ));

            self.plugin_channels
                .input
                .sender
                .send(NonExhaustive::new(PluginMsg::Init))
                .unwrap();
        });

        let mut row_count = 0;

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

            if !self.plugin_channels.output.receiver.is_empty()
                && let Ok(message) = self.plugin_channels.output.receiver.recv()
            {
                match message.into_enum() {
                    Ok(PluginMsg::CheckpointAck { epoch }) => {
                        debug!(
                            "Propagating checkpoint Ack with epoch {} from plugin",
                            epoch.0
                        );

                        let sink_id =
                            metric_metadata_id_to_reference_name(&self.metric_metadata_id)
                                .unwrap_or_else(|| self.metric_metadata_id.clone());
                        send_checkpoint_ack(CheckpointEpoch(epoch.0), &sink_id);
                    }
                    _ => {
                        warn!("Received unexpected message from plugin channel");
                    }
                }
            }

            // Metrics are now handled by a separate task spawned above

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
