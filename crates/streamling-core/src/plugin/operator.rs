//! An operator that wraps any input LogicalPlan and executes plugin logic after every batch.

use crate::checkpoints::checkpoint_management::{
    CheckpointEpoch, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
    extract_checkpoint_messages, now_ms,
};
use crate::plugin::partitioned::{PluginInstance, PluginInstances};
use crate::plugin::telemetry::process_plugin_metrics;
use crate::telemetry::recorder::get_metrics_recorder;
use crate::utils::batch::enrich_batch_with_metadata;
use abi_stable::nonexhaustive_enum::NonExhaustive;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use crossbeam::channel::TryRecvError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::internal_err;
use datafusion::common::{DFSchemaRef, Statistics};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::ExecutionPlanProperties;
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
use streamling_plugin::{PluginCheckpointEpoch, PluginMsg};
use tracing::debug;
use tracing::log::trace;
use uuid::Uuid;

#[derive(PartialEq, Eq, Hash)]
pub struct PluginNode {
    pub input: LogicalPlan,
    output_schema: DFSchemaRef,
    // Use a simple string ID instead of the instances themselves
    // This is needed since the instances don't implement required traits for logical plans
    runtime_state_id: String,
    internal_buffer_size: u32,
    metric_metadata_id: String,
}

// Implement PartialOrd as DFSchemaRef doesn't implement this trait, so excluded from the comparison.
impl PartialOrd for PluginNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (
            &self.input,
            &self.runtime_state_id,
            &self.internal_buffer_size,
            &self.metric_metadata_id,
        )
            .partial_cmp(&(
                &other.input,
                &other.runtime_state_id,
                &other.internal_buffer_size,
                &other.metric_metadata_id,
            ))
    }
}

/// Instances AND the forwarder scope ride the registry together: logical
/// nodes must be Hash/Eq, so non-comparable runtime state is smuggled by id.
type PluginRuntimeState = (PluginInstances, Arc<crate::shutdown::ComponentScope>);

lazy_static::lazy_static! {
    static ref PLUGIN_RUNTIME_STATES: Mutex<HashMap<String, PluginRuntimeState>> =
        Mutex::new(HashMap::new());
}

impl PluginNode {
    pub fn new(
        input: LogicalPlan,
        output_schema: DFSchemaRef,
        instances: PluginInstances,
        internal_buffer_size: u32,
        metric_metadata_id: String,
        scope: Arc<crate::shutdown::ComponentScope>,
    ) -> Self {
        // Registry is keyed by metric_key(app_id, id) = "{app_id}::{id}".
        // A bare reference name misses the lookup and every plugin metric is dropped.
        debug_assert!(
            metric_metadata_id.contains("::"),
            "metric_metadata_id must be a metric_key(app_id, reference_name) composite; a bare reference name misses the metrics registry and every plugin metric is silently dropped"
        );
        let id = format!("plugin_runtime_{}", Uuid::new_v4());

        PLUGIN_RUNTIME_STATES
            .lock()
            .unwrap()
            .insert(id.clone(), (instances, scope));

        Self {
            input,
            output_schema,
            runtime_state_id: id,
            internal_buffer_size,
            metric_metadata_id,
        }
    }

    pub fn instances(&self) -> PluginInstances {
        PLUGIN_RUNTIME_STATES
            .lock()
            .unwrap()
            .get(&self.runtime_state_id)
            .unwrap()
            .0
            .clone()
    }

    pub fn scope(&self) -> Arc<crate::shutdown::ComponentScope> {
        PLUGIN_RUNTIME_STATES
            .lock()
            .unwrap()
            .get(&self.runtime_state_id)
            .unwrap()
            .1
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
            runtime_state_id: self.runtime_state_id.clone(),
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

                // A partitioned transform runs one instance per input stream:
                // its input is already as wide as it will run, either
                // inherited or widened by the exchange planned under it.
                let partitions = input_physical.output_partitioning().partition_count();
                let instances = plugin_node.instances();
                if let PluginInstances::Partitioned(plugin) = &instances {
                    plugin.check_width(
                        partitions,
                        &format!(
                            "its input is {partitions} streams wide; set `parallelism` on the \
                             transform to run it at a supported width"
                        ),
                    )?;
                }
                let instances = instances
                    .resolve(partitions, Some(input_physical.schema()))
                    .await?;

                let plugin_exec = Arc::new(PluginExec::new(
                    input_physical,
                    out_arrow_schema,
                    plugin_node.internal_buffer_size,
                    instances,
                    plugin_node.metric_metadata_id.clone(),
                    plugin_node.scope(),
                ));
                Some(plugin_exec)
            } else {
                None
            },
        )
    }
}

/// Runs input stream `i` through plugin instance `i`. A single-stream plugin
/// has one instance, and its input is coalesced to one stream.
struct PluginExec {
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    internal_buffer_size: u32,
    instances: Vec<PluginInstance>,
    cache: Arc<PlanProperties>,
    metric_metadata_id: String,
    scope: Arc<crate::shutdown::ComponentScope>,
}

impl PluginExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        internal_buffer_size: u32,
        instances: Vec<PluginInstance>,
        metric_metadata_id: String,
        scope: Arc<crate::shutdown::ComponentScope>,
    ) -> Self {
        let cache = Self::compute_properties(output_schema.clone(), instances.len());
        Self {
            input,
            output_schema,
            internal_buffer_size,
            instances,
            cache: Arc::new(cache),
            metric_metadata_id,
            scope,
        }
    }

    fn compute_properties(schema: SchemaRef, partitions: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            // Unknown even when the input was placed by key: the plugin may
            // rewrite the placement columns.
            Partitioning::UnknownPartitioning(partitions),
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
        if self.instances.len() == 1 {
            vec![Distribution::SinglePartition]
        } else {
            vec![Distribution::UnspecifiedDistribution]
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child_partitions = children[0].output_partitioning().partition_count();
        if self.instances.len() > 1 && child_partitions != self.instances.len() {
            return internal_err!(
                "PluginExec runs {} plugin instances but was given an input {child_partitions} partitions wide",
                self.instances.len()
            );
        }
        Ok(Arc::new(PluginExec::new(
            children[0].clone(),
            self.output_schema.clone(),
            self.internal_buffer_size,
            self.instances.clone(),
            self.metric_metadata_id.clone(),
            self.scope.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(instance) = self.instances.get(partition) else {
            return internal_err!(
                "PluginExec has {} partitions, got request for partition {partition}",
                self.instances.len()
            );
        };
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
        crate::plugin::send_to_plugin_blocking(
            &instance.channels.input.sender,
            NonExhaustive::new(PluginMsg::Init),
            &instance.key,
        )?;

        let plugin_input_sender = instance.channels.input.sender.clone();
        let plugin_label = instance.key.clone();
        let forwarder_scope = self.scope.clone();
        let plugin_output_receiver = instance.channels.output.receiver.clone();
        let metrics_receiver = instance.channels.metrics.receiver.clone();
        let metric_metadata_id = self.metric_metadata_id.clone();
        let metrics_recorder = get_metrics_recorder();
        builder.spawn(async move {
            // PostPlugin-stage scope: exits on channel disconnect at plugin
            // teardown; drained after the dispatcher flush it serves.
            forwarder_scope.spawn(process_plugin_metrics(
                metrics_receiver,
                metrics_recorder.clone(),
                metric_metadata_id.clone(),
                plugin_label.clone(),
                forwarder_scope.stage_token().clone(),
            ));

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
                                    crate::plugin::send_to_plugin(
                                        &plugin_input_sender,
                                        NonExhaustive::new(PluginMsg::CheckpointMarker {
                                            epoch: PluginCheckpointEpoch(epoch.0),
                                        }),
                                        &plugin_label,
                                    )
                                    .await?;
                                }
                                CheckpointMessage::Finalizer(epoch) => {
                                    debug!(
                                        "Sending extracted checkpoint Finalizer with epoch {} to plugin",
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
                                _ => {
                                    // ignore other messages
                                }
                            }
                        }

                        crate::plugin::send_to_plugin(
                            &plugin_input_sender,
                            NonExhaustive::new(PluginMsg::NextBatch { data: batch.into() }),
                            &plugin_label,
                        )
                        .await?;

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
                                Ok(Ok(PluginMsg::Error { message })) => {
                                    // Buffered markers are dropped with the
                                    // stream: an epoch covering rows the
                                    // failed plugin lost must never finalize.
                                    let _ = tx
                                        .send(Err(crate::plugin::plugin_failure(&plugin_label, &message)))
                                        .await;
                                    break 'outer;
                                }
                                Err(TryRecvError::Empty) => {
                                    tokio::time::sleep(super::IDLE_POLL_INTERVAL).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_config::AppConfig;
    use crate::checkpoints::checkpoint_management::CheckpointMessage;
    use crate::dynamic_table::DynamicTableRegistry;
    use crate::plugin::partitioned::{
        PartitionedPlugin, PluginInstance, PluginInstances, PluginKind,
    };
    use crate::plugin::test_plugins::{
        self, FAIL_BATCHES_AT, LEGACY_TRANSFORM, MAXIMUM, NODE, TRANSFORM,
        TRANSFORM_PARTITION_COLUMN, batch, ids, marker, shut_down, source_schema,
    };
    use crate::session::SessionManager;
    use arrow::array::{Array, UInt32Array};
    use datafusion::common::DFSchema;
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::logical_expr::{Extension, LogicalPlanBuilder};
    use datafusion::physical_plan::displayable;
    use std::time::Duration;

    const MARKER_EPOCH: u64 = 5;

    fn options(node: &str, extra: &[(&str, &str)]) -> HashMap<String, String> {
        extra
            .iter()
            .chain(&[(NODE, node)])
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn describe(node: &str, extra: &[(&str, &str)]) -> Arc<PartitionedPlugin> {
        test_plugins::install();
        PartitionedPlugin::describe(
            &AppConfig::load().unwrap(),
            node,
            TRANSFORM,
            PluginKind::Transform,
            Some(source_schema()),
            options(node, extra),
        )
        .unwrap()
        .unwrap()
    }

    fn metric_id(node: &str) -> String {
        format!("app::{node}")
    }

    /// Plans a plugin transform over a source `width` streams wide.
    async fn plan_over(
        width: usize,
        node: &str,
        instances: PluginInstances,
        output_schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session_manager =
            SessionManager::new(100, 10, DynamicTableRegistry::new(), width).unwrap();
        let source = MemTable::try_new(source_schema(), vec![vec![]; width]).unwrap();
        let scan = LogicalPlanBuilder::scan("blocks", provider_as_source(Arc::new(source)), None)
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(PluginNode::new(
                scan,
                Arc::new(DFSchema::try_from(output_schema.as_ref().clone()).unwrap()),
                instances,
                10,
                metric_id(node),
                crate::shutdown::ComponentScope::detached("test"),
            )),
        });
        session_manager.new_df(plan).create_physical_plan().await
    }

    fn render(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn partitioned_transform_inherits_its_input_width() {
        let transform = describe("inherits_width", &[]);

        let plan = plan_over(
            3,
            "inherits_width",
            PluginInstances::Partitioned(transform.clone()),
            transform.output_schema().unwrap(),
        )
        .await
        .unwrap();

        let rendered = render(&plan);
        assert!(rendered.contains("PluginExec: partitions=3"), "{rendered}");
        assert!(!rendered.contains("StreamingCoalesceExec"), "{rendered}");
        assert_eq!(shut_down(&transform).await.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn single_stream_transform_input_is_coalesced() {
        test_plugins::install();
        let legacy = crate::plugin::create_transform_plugin(
            &AppConfig::load().unwrap(),
            "coalesced".to_string(),
            LEGACY_TRANSFORM.to_string(),
            options("coalesced", &[]),
            source_schema(),
        )
        .unwrap();
        let instance = PluginInstance {
            key: "coalesced".to_string(),
            channels: Arc::new(legacy.channels.clone()),
            exit: legacy.exit.clone(),
        };

        let plan = plan_over(
            3,
            "coalesced",
            PluginInstances::Single(instance.clone()),
            legacy.output_schema.clone().unwrap(),
        )
        .await
        .unwrap();

        let rendered = render(&plan);
        assert!(rendered.contains("PluginExec: partitions=1"), "{rendered}");
        assert!(rendered.contains("StreamingCoalesceExec"), "{rendered}");
        crate::plugin::terminate_plugins(vec![(instance.key, (*instance.channels).clone())], None)
            .unwrap();
        legacy.execution_future.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unsupported_inherited_width_is_rejected() {
        let transform = describe("narrow_transform", &[(MAXIMUM, "2")]);

        let err = plan_over(
            3,
            "narrow_transform",
            PluginInstances::Partitioned(transform.clone()),
            transform.output_schema().unwrap(),
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(err.contains("between 1 and 2"), "{err}");
        assert!(
            err.contains("parallelism"),
            "the error must say how to fix it: {err}"
        );
        assert!(transform.take_execution_futures().is_empty());
    }

    /// Plans a plugin transform over a one-stream source whose input plan is
    /// shaped by `wrap_input`, as topology construction shapes it.
    async fn plan_wrapped(
        node: &str,
        transform: &Arc<PartitionedPlugin>,
        wrap_input: impl FnOnce(LogicalPlan) -> LogicalPlan,
        wrap_output: impl FnOnce(LogicalPlan) -> LogicalPlan,
    ) -> Arc<dyn ExecutionPlan> {
        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 4).unwrap();
        let source = MemTable::try_new(source_schema(), vec![vec![]]).unwrap();
        let scan = LogicalPlanBuilder::scan("blocks", provider_as_source(Arc::new(source)), None)
            .unwrap()
            .build()
            .unwrap();
        let output_schema = transform.output_schema().unwrap();
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(PluginNode::new(
                wrap_input(scan),
                Arc::new(DFSchema::try_from(output_schema.as_ref().clone()).unwrap()),
                PluginInstances::Partitioned(transform.clone()),
                10,
                metric_id(node),
                crate::shutdown::ComponentScope::detached("test"),
            )),
        });
        session_manager
            .new_df(wrap_output(plan))
            .create_physical_plan()
            .await
            .unwrap()
    }

    fn repartitioned(plan: LogicalPlan, target: Option<usize>, name: &str) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(crate::operators::repartition::RepartitionNode::new(
                plan,
                crate::operators::repartition::Placement::ByKey(vec!["id".to_string()]),
                target,
                name.to_string(),
            )),
        })
    }

    /// An explicit `parallelism` widens a narrow input through the exchange
    /// the plugin's placement asked for; rebatching happens per stream, above
    /// the exchange, and each stream gets its own instance.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn explicit_parallelism_widens_a_narrow_input() {
        let transform = describe("widened_transform", &[]);

        let plan = plan_wrapped(
            "widened_transform",
            &transform,
            |scan| {
                LogicalPlan::Extension(Extension {
                    node: Arc::new(crate::operators::rebatch::RebatchNode::new(
                        repartitioned(scan, Some(4), "widened_transform"),
                        10,
                        None,
                        "widened_transform".to_string(),
                    )),
                })
            },
            |plan| plan,
        )
        .await;

        let rendered = render(&plan);
        let position = |needle: &str| {
            rendered
                .find(needle)
                .unwrap_or_else(|| panic!("expected {needle:?} in plan:\n{rendered}"))
        };
        assert!(
            position("PluginExec: partitions=4")
                < position("RebatchExec(batch_size=10, partitions=4)")
        );
        assert!(
            position("RebatchExec(batch_size=10, partitions=4)")
                < position("StreamingRepartitionExec: partitions=4, keys=[id@0]")
        );
        assert_eq!(shut_down(&transform).await.len(), 4);
    }

    /// A plugin may rewrite the columns its input was placed by, so a keyed
    /// consumer downstream gets its own exchange instead of trusting the
    /// placement below the plugin.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn transform_output_placement_is_unknown() {
        let transform = describe("replaced_transform", &[]);

        let plan = plan_wrapped(
            "replaced_transform",
            &transform,
            |scan| repartitioned(scan, Some(2), "replaced_transform"),
            |plan| repartitioned(plan, None, "downstream_sink"),
        )
        .await;

        let rendered = render(&plan);
        assert_eq!(
            rendered.matches("StreamingRepartitionExec").count(),
            2,
            "{rendered}"
        );
        shut_down(&transform).await;
    }

    /// Checkpoint markers in a stream go to that stream's instance, and come
    /// back out on that stream only — one copy per stream, as every
    /// downstream alignment point expects.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn each_stream_is_transformed_by_its_own_instance() {
        let transform = describe("per_stream_transform", &[]);
        let instances = transform
            .instantiate(2, Some(source_schema()))
            .await
            .unwrap();
        let exec = PluginExec::new(
            test_plugins::input(vec![
                vec![batch(&[1, 2], &[marker(MARKER_EPOCH)]), batch(&[3], &[])],
                vec![batch(&[10], &[marker(MARKER_EPOCH)])],
            ]),
            transform.output_schema().unwrap(),
            10,
            instances,
            metric_id("per_stream_transform"),
            crate::shutdown::ComponentScope::detached("test"),
        );

        let expected_ids: [Vec<i64>; 2] = [vec![1, 2, 3], vec![10]];
        for (partition, expected) in expected_ids.iter().enumerate() {
            let batches: Vec<RecordBatch> = exec
                .execute(partition, Arc::new(TaskContext::default()))
                .unwrap()
                .map(|b| b.unwrap())
                .collect()
                .await;

            let all_ids: Vec<i64> = batches.iter().flat_map(ids).collect();
            assert_eq!(&all_ids, expected);
            for batch in &batches {
                let processed_by = batch
                    .column_by_name(TRANSFORM_PARTITION_COLUMN)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap();
                assert!(processed_by.iter().all(|p| p == Some(partition as u32)));
            }
            let markers: Vec<CheckpointMessage> = batches
                .iter()
                .flat_map(|b| extract_checkpoint_messages(b.schema().metadata()))
                .collect();
            assert_eq!(markers.len(), 1, "stream {partition}: {markers:?}");
            assert_eq!(
                test_plugins::log(&format!("per_stream_transform[{partition}]")).markers,
                [MARKER_EPOCH]
            );
        }
        shut_down(&transform).await;
    }

    /// The failed instance keeps draining until teardown, so the stream must
    /// learn of the failure from the instance's report instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn transform_failure_ends_the_stream_with_an_error() {
        let transform = describe("failing_transform", &[(FAIL_BATCHES_AT, "0")]);
        let instances = transform
            .instantiate(1, Some(source_schema()))
            .await
            .unwrap();
        let exec = PluginExec::new(
            test_plugins::input(vec![vec![batch(&[1], &[])]]),
            transform.output_schema().unwrap(),
            10,
            instances,
            metric_id("failing_transform"),
            crate::shutdown::ComponentScope::detached("test"),
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("the failure must reach the stream without waiting for teardown")
            .expect("the stream must yield the failure");

        let err = first.expect_err("the stream must fail").to_string();
        assert!(err.contains("batch failed on purpose"), "{err}");
        shut_down(&transform).await;
    }
}
