//! An operator that wraps any input LogicalPlan and executes plugin logic after every batch.

use crate::checkpoints::checkpoint_management::{
    CheckpointEpoch, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
    extract_checkpoint_messages, now_ms,
};
use crate::plugin::telemetry::process_plugin_metrics;
use crate::telemetry::recorder::get_metrics_recorder;
use crate::utils::batch::enrich_batch_with_metadata;
use abi_stable::nonexhaustive_enum::NonExhaustive;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use crossbeam::channel::TryRecvError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchemaRef, Statistics};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_input_stream,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use streamling_plugin::{PluginChannels, PluginCheckpointEpoch, PluginMsg};
use tracing::debug;
use tracing::log::trace;
use uuid::Uuid;

#[derive(PartialEq, Eq, Hash)]
pub struct PluginNode {
    pub input: LogicalPlan,
    output_schema: DFSchemaRef,
    // Use a simple string ID instead of the channels themselves
    // This is needed since the channels don't implement required traits for logical plans
    plugin_channel_id: String,
    internal_buffer_size: u32,
    metric_metadata_id: String,
}

// Implement PartialOrd as DFSchemaRef doesn't implement this trait, so excluded from the comparison.
impl PartialOrd for PluginNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            &self.input,
            &self.plugin_channel_id,
            &self.internal_buffer_size,
            &self.metric_metadata_id,
        )
            .partial_cmp(&(
                &other.input,
                &other.plugin_channel_id,
                &other.internal_buffer_size,
                &other.metric_metadata_id,
            ))
    }
}

lazy_static::lazy_static! {
    static ref PLUGIN_CHANNELS: Mutex<HashMap<String, Arc<PluginChannels>>> =
        Mutex::new(HashMap::new());
}

impl PluginNode {
    pub fn new(
        input: LogicalPlan,
        output_schema: DFSchemaRef,
        plugin_channels: Arc<PluginChannels>,
        internal_buffer_size: u32,
        metric_metadata_id: String,
    ) -> Self {
        let id = format!("plugin_channel_{}", Uuid::new_v4());

        PLUGIN_CHANNELS
            .lock()
            .unwrap()
            .insert(id.clone(), plugin_channels);

        Self {
            input,
            output_schema,
            plugin_channel_id: id,
            internal_buffer_size,
            metric_metadata_id,
        }
    }

    pub fn plugin_channels(&self) -> Arc<PluginChannels> {
        PLUGIN_CHANNELS
            .lock()
            .unwrap()
            .get(&self.plugin_channel_id)
            .unwrap()
            .clone()
    }
}

impl Debug for PluginNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for PluginNode {
    fn name(&self) -> &str {
        "Plugin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Plugin")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");

        Ok(Self {
            input: inputs.swap_remove(0),
            output_schema: self.output_schema.clone(),
            internal_buffer_size: self.internal_buffer_size,
            plugin_channel_id: self.plugin_channel_id.clone(),
            metric_metadata_id: self.metric_metadata_id.clone(),
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

pub struct PluginExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for PluginExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(plugin_node) = node.as_any().downcast_ref::<PluginNode>() {
                let input_physical = planner
                    .create_physical_plan(&plugin_node.input, session_state)
                    .await?;

                let out_arrow_schema: SchemaRef = Arc::new(
                    UserDefinedLogicalNodeCore::schema(plugin_node)
                        .as_arrow()
                        .clone(),
                );

                let plugin_exec = Arc::new(PluginExec::new(
                    input_physical,
                    out_arrow_schema,
                    plugin_node.internal_buffer_size,
                    plugin_node.plugin_channels(),
                    plugin_node.metric_metadata_id.clone(),
                ));
                Some(plugin_exec)
            } else {
                None
            },
        )
    }
}

struct PluginExec {
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    internal_buffer_size: u32,
    plugin_channels: Arc<PluginChannels>,
    cache: Arc<PlanProperties>,
    metric_metadata_id: String,
}

impl PluginExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        internal_buffer_size: u32,
        plugin_channels: Arc<PluginChannels>,
        metric_metadata_id: String,
    ) -> Self {
        let cache = Self::compute_properties(output_schema.clone());
        Self {
            input,
            output_schema,
            internal_buffer_size,
            plugin_channels,
            cache: Arc::new(cache),
            metric_metadata_id,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        )
    }
}

impl Debug for PluginExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PluginExec")
    }
}

impl DisplayAs for PluginExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "PluginExec: partitions={}",
                    self.properties().output_partitioning().partition_count()
                )
            }
        }
    }
}

impl ExecutionPlan for PluginExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PluginExec::new(
            children[0].clone(),
            self.output_schema.clone(),
            self.internal_buffer_size,
            self.plugin_channels.clone(),
            self.metric_metadata_id.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let data = execute_input_stream(
            Arc::clone(&self.input),
            Arc::clone(&self.input.schema()),
            partition,
            Arc::clone(&context),
        )?;

        let mut builder = RecordBatchReceiverStreamBuilder::new(
            self.schema(),
            self.internal_buffer_size as usize,
        );
        let tx = builder.tx();

        // Initialize the plugin
        self.plugin_channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Init))
            .unwrap();

        let plugin_input_sender = self.plugin_channels.input.sender.clone();
        let plugin_output_receiver = self.plugin_channels.output.receiver.clone();
        let metrics_receiver = self.plugin_channels.metrics.receiver.clone();
        let metric_metadata_id = self.metric_metadata_id.clone();
        let metrics_recorder = get_metrics_recorder();
        builder.spawn(async move {
            tokio::spawn(process_plugin_metrics(metrics_receiver,metrics_recorder.clone(), metric_metadata_id.clone()));

            let mut checkpoint_buffer: Vec<CheckpointMessage> = Vec::new();
            // Track created_at_ms for epochs so we can preserve timing through plugin round-trip
            let mut epoch_created_at: HashMap<u64, u64> = HashMap::new();

            let mut stream = data;

            'outer: while let Some(batch) = stream.next().await {
                match batch {
                    Ok(batch) => {
                        let checkpoint_messages = extract_checkpoint_messages(batch.schema().metadata());
                        trace!("Extracted checkpoint messages from batch metadata: {:?}", checkpoint_messages);

                        for message in checkpoint_messages {
                            match message {
                                CheckpointMessage::Marker { epoch, created_at_ms } => {
                                    // Store created_at_ms for this epoch so we can preserve it through plugin
                                    epoch_created_at.insert(epoch.0, created_at_ms);
                                    debug!(
                                        "Sending extracted checkpoint Marker with epoch {} to plugin",
                                        epoch.0
                                    );
                                    plugin_input_sender
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
                                    plugin_input_sender
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

                        plugin_input_sender
                            .send(NonExhaustive::new(PluginMsg::NextBatch {
                                data: batch.into(),
                            }))
                            .unwrap();

                        // TODO: should this be parallelized?
                        // Right now it processes batches sequentially:
                        // - It sends the batch to the plugin
                        // - It waits for the plugin to respond with the next batch, indefinitely
                        // This means that the plugin can process one batch at a time.
                        // Alternatively, we can handle batch replies in a spawned task
                        loop {
                            match plugin_output_receiver.try_recv().map(|m| m.into_enum())  {
                                Ok(Ok(PluginMsg::NextBatch { data })) => {
                                    let mut processed_batch: RecordBatch = data.into();

                                    // Attach buffered checkpoint messages to batch metadata
                                    if !checkpoint_buffer.is_empty() {
                                        debug!(
                                            "Attaching {} buffered checkpoint messages to batch",
                                            checkpoint_buffer.len()
                                        );

                                        let mut metadata = processed_batch.schema().metadata().clone();
                                        enrich_batch_metadata_with_checkpoints(&mut metadata, &checkpoint_buffer);
                                        processed_batch = enrich_batch_with_metadata(processed_batch, metadata)
                                            .expect("Failed to enrich batch with checkpoint metadata");

                                        checkpoint_buffer.clear();
                                    }

                                    tx.send(Ok(processed_batch)).await.unwrap(); // handle send error
                                    break;
                                }
                                Ok(Ok(PluginMsg::CheckpointMarker { epoch })) => {
                                    debug!(
                                        "Buffering checkpoint marker with epoch {} from plugin",
                                        epoch.0,
                                    );
                                    // Use stored created_at_ms or current time if not found
                                    // Remove entry to prevent unbounded HashMap growth
                                    let created_at_ms = epoch_created_at
                                        .remove(&epoch.0)
                                        .unwrap_or_else(now_ms);
                                    checkpoint_buffer.push(CheckpointMessage::Marker {
                                        epoch: CheckpointEpoch(epoch.0),
                                        created_at_ms,
                                    });
                                }
                                Ok(Ok(PluginMsg::CheckpointFinalizer { epoch })) => {
                                    debug!(
                                        "Buffering checkpoint finalizer with epoch {} from plugin",
                                        epoch.0
                                    );
                                    checkpoint_buffer.push(CheckpointMessage::Finalizer(CheckpointEpoch(epoch.0)));
                                }
                                Err(TryRecvError::Empty) => {
                                    tokio::task::yield_now().await;
                                }
                                Err(TryRecvError::Disconnected) => {
                                    break 'outer;
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        debug!("PluginExec [{}]: Error from input stream, transform will terminate: {}", metric_metadata_id, e);
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }

            Ok(())
        });

        Ok(builder.build())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}
