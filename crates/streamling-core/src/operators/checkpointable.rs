//! An operator that can wrap any input LogicalPlan and execute checkpointing logic after every batch.
//! It propagates the input data as is, preserving checkpoint messages in batch metadata.
//! This is useful for adding checkpointing support to SQL transformations.

use arrow::record_batch::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
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
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_input_stream,
};

use crate::checkpoints::checkpoint_management::{
    enrich_batch_metadata_with_checkpoints, extract_checkpoint_messages,
};
use crate::operators::filter::StreamingFilterExec;
use crate::operators::projection::StreamingProjectionExec;
use crate::operators::wrapping::WrappingExec;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::debug;

#[derive(PartialEq, Eq, PartialOrd, Hash)]
pub struct CheckpointableNode {
    pub input: LogicalPlan,
    internal_buffer_size: u32,
    reference_name: String,
}

impl CheckpointableNode {
    pub fn new(input: LogicalPlan, internal_buffer_size: u32, reference_name: String) -> Self {
        Self {
            input,
            internal_buffer_size,
            reference_name,
        }
    }
}

impl Debug for CheckpointableNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for CheckpointableNode {
    fn name(&self) -> &str {
        "Checkpointable"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Checkpointable")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");

        Ok(Self {
            input: inputs.swap_remove(0),
            internal_buffer_size: self.internal_buffer_size,
            reference_name: self.reference_name.clone(),
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

pub struct CheckpointableExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for CheckpointableExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(checkpointable_node) = node.as_any().downcast_ref::<CheckpointableNode>() {
                let input_physical = planner
                    .create_physical_plan(&checkpointable_node.input, session_state)
                    .await?;

                // Multi-partition inputs (e.g. from `UNION ALL`) are coalesced inside
                // `CheckpointableExec::execute`, which preserves checkpoint-marker metadata.
                // We deliberately avoid a stock `RepartitionExec` here, which would drop it.
                let exec = Arc::new(CheckpointableExec::new(
                    input_physical,
                    checkpointable_node.internal_buffer_size,
                    checkpointable_node.reference_name.clone(),
                ));
                Some(exec)
            } else {
                None
            },
        )
    }
}

/// Whether `plan` marks the edge of this transform's own physical subtree for
/// metric attribution.
///
/// Types opt in via [`crate::operators::TopologyBoundary`]. This walk still
/// downcasts the closed set because DataFusion's `ExecutionPlan` has no local
/// hook; adding a boundary type means `impl TopologyBoundary` plus one arm.
///
/// Three kinds of boundary exist:
/// - A [`WrappingExec`]: a *separate* topology node that records its own
///   `elapsed_compute`; descending into it would fold another node's compute
///   into this transform's aggregate, double-counting it.
/// - A nested [`CheckpointableExec`]: another transform's aggregation point
///   (its `metrics()` already covers its subtree).
/// - A source-owned [`StreamingFilterExec`] / [`StreamingProjectionExec`]:
///   `wrap_with_side_outputs_before_filter` re-applies a source's filter and
///   projection ABOVE the source's `WrappingExec` (so side outputs observe
///   pre-filter rows), which places those source-owned operators inside the
///   consuming transform's subtree. Without stopping at them, an expensive
///   source-level filter would be misattributed to the transform's compute.
fn is_topology_boundary(plan: &Arc<dyn ExecutionPlan>) -> bool {
    topology_boundary_of::<WrappingExec>(plan)
        .or_else(|| topology_boundary_of::<CheckpointableExec>(plan))
        .or_else(|| topology_boundary_of::<StreamingFilterExec>(plan))
        .or_else(|| topology_boundary_of::<StreamingProjectionExec>(plan))
        .unwrap_or(false)
}

fn topology_boundary_of<T: crate::operators::TopologyBoundary + ExecutionPlan + 'static>(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<bool> {
    plan.downcast_ref::<T>()
        .map(crate::operators::TopologyBoundary::bounds_metric_aggregation)
}

/// DataFusion's [`MetricsSet`] has no `is_empty()`. This helper is the
/// emptiness check `CheckpointableExec::metrics` uses to restore the
/// metric-less short-circuit, until DataFusion grows `is_empty()`.
fn metrics_set_is_empty(set: &MetricsSet) -> bool {
    set.iter().next().is_none()
}

/// Recursively merge the DataFusion `MetricsSet` of `plan` and its descendants
/// into `out`, stopping at any topology boundary (see [`is_topology_boundary`]).
///
/// Everything strictly between this `CheckpointableExec` and the next topology
/// boundary is this transform's own compute and belongs in its aggregate.
///
/// The boundary check is applied to `plan` itself, not just its children:
/// `WrappingExec` delegates both `metrics()` and `children()` to its inner
/// plan, so if the boundary node were reached as the recursion root (e.g. the
/// `CheckpointableExec`'s input is a passthrough SQL whose projection was
/// pushed down, leaving the upstream `WrappingExec` directly beneath it), a
/// children-only guard would collect its delegated metrics and descend into the
/// nested topology anyway.
fn collect_subtree_metrics(plan: &Arc<dyn ExecutionPlan>, out: &mut MetricsSet) {
    let mut visited = std::collections::HashSet::new();
    collect_subtree_metrics_inner(plan, out, &mut visited);
}

fn collect_subtree_metrics_inner(
    plan: &Arc<dyn ExecutionPlan>,
    out: &mut MetricsSet,
    visited: &mut std::collections::HashSet<usize>,
) {
    // A physical plan can be a DAG: one Arc-shared subplan reachable from two
    // parents (e.g. UNION ALL over a common input) must be counted once.
    // The visited-set key is Arc pointer identity, not operator equality —
    // two separately constructed plans wrapping the same operator would
    // both be walked (extremely unlikely in a single physical tree).
    if !visited.insert(Arc::as_ptr(plan) as *const () as usize) {
        return;
    }
    if is_topology_boundary(plan) {
        return;
    }
    if let Some(set) = plan.metrics() {
        for metric in set.iter() {
            // Capture only the variants the per-batch delta path exports
            // (`subtree_delta_metric_values`); dropping the rest here —
            // timestamps, output_rows/bytes/batches per operator per
            // partition — keeps the per-batch loop from re-walking dead
            // entries on every batch.
            match metric.value() {
                MetricValue::ElapsedCompute(_)
                | MetricValue::Count { .. }
                | MetricValue::Time { .. }
                | MetricValue::Gauge { .. } => out.push(Arc::clone(metric)),
                _ => {}
            }
        }
    }
    for child in plan.children() {
        collect_subtree_metrics_inner(child, out, visited);
    }
}

struct CheckpointableExec {
    input: Arc<dyn ExecutionPlan>,
    internal_buffer_size: u32,
    reference_name: String,
    cache: Arc<PlanProperties>,
}

impl crate::operators::TopologyBoundary for CheckpointableExec {
    /// Nested CheckpointableExec is another transform's aggregation point:
    /// its `metrics()` already covers its own subtree, so descending past it
    /// would double-count everything below (and misattribute it to this node).
    fn bounds_metric_aggregation(&self) -> bool {
        true
    }
}

impl CheckpointableExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        internal_buffer_size: u32,
        reference_name: String,
    ) -> Self {
        let cache = Self::compute_properties(input.schema());
        Self {
            input,
            internal_buffer_size,
            reference_name,
            cache: Arc::new(cache),
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

impl Debug for CheckpointableExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CheckpointableExec")
    }
}

impl DisplayAs for CheckpointableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "CheckpointableExec (for {})", self.input.name())
            }
        }
    }
}

impl ExecutionPlan for CheckpointableExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // We coalesce the input partitions ourselves in `execute` (preserving checkpoint
        // markers). Requiring `SinglePartition` here would make DataFusion insert a stock
        // `CoalescePartitionsExec`/`RepartitionExec`, which rebuild batches with a
        // metadata-less schema and silently drop the checkpoint markers.
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CheckpointableExec::new(
            children[0].clone(),
            self.internal_buffer_size,
            self.reference_name.clone(),
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let output_schema = self.schema();
        let mut builder = RecordBatchReceiverStreamBuilder::new(
            output_schema.clone(),
            self.internal_buffer_size as usize,
        );

        // Coalesce all input partitions into this single output partition, forwarding
        // each batch with its checkpoint-marker metadata intact. We merge here rather
        // than via a stock DataFusion `RepartitionExec`/`CoalescePartitionsExec`, which
        // rebuild batches with a metadata-less schema and would drop checkpoint markers,
        // stalling checkpoint finalization (e.g. for `UNION ALL`, which is multi-partition).
        let input_partitions = self.input.output_partitioning().partition_count();
        for input_partition in 0..input_partitions {
            let data = execute_input_stream(
                Arc::clone(&self.input),
                Arc::clone(&output_schema),
                input_partition,
                Arc::clone(&context),
            )?;
            let tx = builder.tx();
            let output_schema = output_schema.clone();
            let reference_name = self.reference_name.clone();

            builder.spawn(async move {
                let mut stream = data;

                while let Some(batch) = stream.next().await {
                    match batch {
                        Ok(batch) => {
                            // Extract checkpoint messages from input batch metadata
                            let checkpoint_messages =
                                extract_checkpoint_messages(batch.schema().metadata());

                            // Create output batch with checkpoint messages preserved in schema metadata
                            // Preserve existing metadata from output_schema and merge with checkpoint messages
                            let output_batch = if !checkpoint_messages.is_empty() {
                                let mut metadata: HashMap<String, String> =
                                    output_schema.metadata().clone();
                                enrich_batch_metadata_with_checkpoints(
                                    &mut metadata,
                                    &checkpoint_messages,
                                );
                                let enriched_schema = Arc::new(Schema::new_with_metadata(
                                    output_schema.fields().clone(),
                                    metadata,
                                ));
                                RecordBatch::try_new(enriched_schema, batch.columns().to_vec())
                                    .unwrap_or(batch)
                            } else {
                                batch
                            };

                            // Forward the batch to the output stream
                            if tx.send(Ok(output_batch)).await.is_err() {
                                // The receiver was dropped, stop processing
                                break;
                            }
                        }
                        Err(e) => {
                            debug!("CheckpointableExec [{}]: Error from inner SQL plan, stream will terminate: {}", reference_name, e);
                            let _ = tx.send(Err(e)).await;
                            break;
                        }
                    }
                }

                Ok(())
            });
        }

        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        // Aggregate DataFusion metrics across this transform's ENTIRE physical
        // subtree, not just the root operator.
        //
        // The topology for a SQL transform is
        // `WrappingExec -> CheckpointableExec -> <SQL plan>`, and `WrappingExec`
        // folds these DataFusion metrics into the node's `elapsed_compute` (via
        // `record_execution_plan_metrics`). Forwarding only `self.input.metrics()`
        // (the SQL plan's ROOT operator) drops the `elapsed_compute` recorded by
        // deeper operators — e.g. a `FilterExec` beneath a `ProjectionExec` does
        // the real work but sits below the root. With that deep compute missing,
        // the transform's reported `elapsed_compute` collapsed to near zero and
        // a compute-bound transform looked idle.
        //
        // Return `None` (not `Some(empty)`) when the subtree exposes nothing,
        // so per-batch consumers keep their metric-less short-circuit.
        let mut aggregated = MetricsSet::new();
        collect_subtree_metrics(&self.input, &mut aggregated);
        if metrics_set_is_empty(&aggregated) {
            None
        } else {
            Some(aggregated)
        }
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::checkpoint_management::{CheckpointEpoch, CheckpointMessage};
    use crate::optimizer::StreamlingPhysicalOptimizerRules;
    use arrow::array::{BooleanArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion::datasource::MemTable;
    use datafusion::datasource::TableProvider;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_expr::Partitioning as PhysicalPartitioning;
    use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::*;
    use futures::StreamExt;
    use std::time::Duration;

    fn create_test_schema_with_metadata(metadata: HashMap<String, String>) -> SchemaRef {
        Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("active", DataType::Boolean, false),
            ],
            metadata,
        ))
    }

    fn create_checkpoint_metadata(messages: &[CheckpointMessage]) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(&mut metadata, messages);
        metadata
    }

    fn create_test_batch_with_checkpoint(messages: &[CheckpointMessage]) -> RecordBatch {
        let metadata = create_checkpoint_metadata(messages);
        let schema = create_test_schema_with_metadata(metadata);
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    Some("charlie"),
                    Some("david"),
                    Some("eve"),
                ])),
                Arc::new(BooleanArray::from(vec![true, false, true, false, true])),
            ],
        )
        .unwrap()
    }

    /// A simple test source that emits a single batch with checkpoint metadata
    struct TestSourceExec {
        batch: RecordBatch,
        schema: SchemaRef,
        cache: Arc<PlanProperties>,
    }

    impl TestSourceExec {
        fn new(batch: RecordBatch) -> Self {
            let schema = batch.schema();
            let cache = PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                PhysicalPartitioning::UnknownPartitioning(1),
                datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                datafusion::physical_plan::execution_plan::Boundedness::Bounded,
            );
            Self {
                batch,
                schema,
                cache: Arc::new(cache),
            }
        }
    }

    impl std::fmt::Debug for TestSourceExec {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "TestSourceExec")
        }
    }

    impl DisplayAs for TestSourceExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "TestSourceExec")
        }
    }

    impl ExecutionPlan for TestSourceExec {
        fn name(&self) -> &'static str {
            "TestSourceExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.cache
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
            let batch = self.batch.clone();
            let schema = self.schema.clone();
            let stream = futures::stream::once(async move { Ok(batch) });
            Ok(Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
            ))
        }
    }

    /// Creates a SessionContext with our streaming optimizer rules (StreamingFilterExec, etc.)
    fn create_streaming_context() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rules(StreamlingPhysicalOptimizerRules::rules())
            .build();
        SessionContext::new_with_state(state)
    }

    /// Test that CheckpointableExec preserves checkpoint metadata from input batch
    #[tokio::test]
    async fn test_checkpointable_exec_preserves_metadata() {
        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(42),
            created_at_ms: 0,
        }];
        let batch = create_test_batch_with_checkpoint(&checkpoint_messages);

        // Create test source with checkpoint metadata
        let source = Arc::new(TestSourceExec::new(batch));

        // Wrap with CheckpointableExec
        let checkpointable = CheckpointableExec::new(source, 10, "test".to_string());

        // Execute and collect output
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let mut stream = checkpointable.execute(0, task_ctx).unwrap();

        let mut output_batches = vec![];
        while let Some(batch_result) = stream.next().await {
            output_batches.push(batch_result.unwrap());
        }

        assert_eq!(output_batches.len(), 1, "Should have 1 output batch");

        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_metadata = output_schema.metadata();

        // Extract and verify checkpoint messages
        let extracted = extract_checkpoint_messages(output_metadata);
        assert_eq!(extracted.len(), 1, "Should have 1 checkpoint message");
        assert!(
            matches!(
                &extracted[0],
                CheckpointMessage::Marker {
                    epoch: CheckpointEpoch(42),
                    created_at_ms: 0
                }
            ),
            "Should have correct checkpoint epoch"
        );
    }

    /// Test checkpoint propagation through SQL filter that returns partial rows
    #[tokio::test]
    async fn test_checkpoint_propagates_through_sql_filter() {
        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(100),
            created_at_ms: 0,
        }];
        let batch = create_test_batch_with_checkpoint(&checkpoint_messages);

        // Create MemTable with checkpoint metadata batch
        let schema = batch.schema();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

        // Use streaming context with our optimizer rules
        let ctx = create_streaming_context();
        ctx.register_table("source", Arc::new(mem_table)).unwrap();

        // SQL filter that returns partial rows (active = true returns 3 of 5 rows)
        let df = ctx
            .sql("SELECT * FROM source WHERE active = true")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty(), "Should have at least one batch");

        let output_batch = &batches[0];
        assert_eq!(output_batch.num_rows(), 3, "Filter should return 3 rows");

        // Verify checkpoint metadata is preserved
        let extracted = extract_checkpoint_messages(output_batch.schema().metadata());
        assert_eq!(
            extracted.len(),
            1,
            "Checkpoint message should propagate through SQL filter"
        );
        assert!(matches!(
            &extracted[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(100),
                created_at_ms: 0
            }
        ));
    }

    /// Test checkpoint propagation through SQL projection
    #[tokio::test]
    async fn test_checkpoint_propagates_through_sql_projection() {
        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(200),
            created_at_ms: 0,
        }];
        let batch = create_test_batch_with_checkpoint(&checkpoint_messages);

        // Create MemTable with checkpoint metadata batch
        let schema = batch.schema();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

        // Use streaming context with our optimizer rules
        let ctx = create_streaming_context();
        ctx.register_table("source", Arc::new(mem_table)).unwrap();

        // SQL projection that selects only some columns
        let df = ctx.sql("SELECT id, name FROM source").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty(), "Should have at least one batch");

        let output_batch = &batches[0];
        assert_eq!(output_batch.num_columns(), 2, "Should have 2 columns");
        assert_eq!(output_batch.num_rows(), 5, "Should have all 5 rows");

        // Verify checkpoint metadata is preserved
        let extracted = extract_checkpoint_messages(output_batch.schema().metadata());
        assert_eq!(
            extracted.len(),
            1,
            "Checkpoint message should propagate through SQL projection"
        );
        assert!(matches!(
            &extracted[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(200),
                created_at_ms: 0
            }
        ));
    }

    /// Test checkpoint propagation through SQL filter that returns empty results
    #[tokio::test]
    async fn test_checkpoint_propagates_through_empty_sql_filter() {
        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(300),
            created_at_ms: 0,
        }];
        let batch = create_test_batch_with_checkpoint(&checkpoint_messages);

        // Create MemTable with checkpoint metadata batch
        let schema = batch.schema();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();

        // Use streaming context with our optimizer rules (StreamingFilterExec)
        let ctx = create_streaming_context();
        ctx.register_table("source", Arc::new(mem_table)).unwrap();

        // SQL filter that matches nothing
        let df = ctx
            .sql("SELECT * FROM source WHERE id = 999")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        // With StreamingFilterExec, we should get a batch with 0 rows but metadata preserved
        assert_eq!(batches.len(), 1, "StreamingFilterExec should emit 1 batch");

        let output_batch = &batches[0];
        assert_eq!(
            output_batch.num_rows(),
            0,
            "Filter should return 0 rows for non-matching filter"
        );

        // Verify checkpoint metadata is preserved even on empty batch
        let extracted = extract_checkpoint_messages(output_batch.schema().metadata());
        assert_eq!(
            extracted.len(),
            1,
            "Checkpoint message should propagate through empty filter results"
        );
        assert!(matches!(
            &extracted[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(300),
                created_at_ms: 0
            }
        ));
    }

    /// Test that CheckpointableExec preserves existing schema metadata when adding checkpoint messages
    #[tokio::test]
    async fn test_checkpointable_exec_preserves_existing_metadata() {
        // Create batch with both checkpoint metadata AND other custom metadata
        let mut metadata = HashMap::new();
        metadata.insert("custom_key".to_string(), "custom_value".to_string());
        metadata.insert("another_key".to_string(), "another_value".to_string());

        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(999),
            created_at_ms: 0,
        }];
        enrich_batch_metadata_with_checkpoints(&mut metadata, &checkpoint_messages);

        let schema = create_test_schema_with_metadata(metadata);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .unwrap();

        // Verify input batch has both custom metadata and checkpoint messages
        let input_schema = batch.schema();
        let input_metadata = input_schema.metadata();
        assert_eq!(
            input_metadata.get("custom_key"),
            Some(&"custom_value".to_string())
        );
        assert_eq!(
            input_metadata.get("another_key"),
            Some(&"another_value".to_string())
        );
        let input_checkpoint = extract_checkpoint_messages(input_metadata);
        assert_eq!(input_checkpoint.len(), 1);

        // Create test source and wrap with CheckpointableExec
        let source = Arc::new(TestSourceExec::new(batch));
        let checkpointable = CheckpointableExec::new(source, 10, "test".to_string());

        // Execute and collect output
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let mut stream = checkpointable.execute(0, task_ctx).unwrap();

        let mut output_batches = vec![];
        while let Some(batch_result) = stream.next().await {
            output_batches.push(batch_result.unwrap());
        }

        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_metadata = output_schema.metadata();

        // Verify checkpoint messages are preserved
        let extracted = extract_checkpoint_messages(output_metadata);
        assert_eq!(extracted.len(), 1, "Checkpoint message should be preserved");
        assert!(matches!(
            &extracted[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(999),
                created_at_ms: 0
            }
        ));

        // Verify custom metadata is also preserved (not dropped)
        assert_eq!(
            output_metadata.get("custom_key"),
            Some(&"custom_value".to_string()),
            "Custom metadata 'custom_key' should be preserved"
        );
        assert_eq!(
            output_metadata.get("another_key"),
            Some(&"another_value".to_string()),
            "Custom metadata 'another_key' should be preserved"
        );
    }

    // ------------------------------------------------------------------
    // elapsed_compute (query latency) subtree aggregation regression.
    //
    // A SQL transform under-reported its `elapsed_compute` because
    // `CheckpointableExec::metrics` forwarded only the SQL plan's ROOT
    // operator, dropping compute recorded by deeper operators.
    // ------------------------------------------------------------------

    /// A test operator that records a fixed `elapsed_compute` per input batch
    /// via DataFusion's `BaselineMetrics`, modeling a compute-bound SQL operator
    /// (e.g. `FilterExec`). Used to prove `CheckpointableExec::metrics` gathers
    /// compute from operators *below* the SQL plan root, and stops at nested
    /// `WrappingExec` topology boundaries.
    #[derive(Debug)]
    struct ComputeExec {
        input: Arc<dyn ExecutionPlan>,
        per_batch_compute: Duration,
        metrics: ExecutionPlanMetricsSet,
    }

    impl ComputeExec {
        fn new(input: Arc<dyn ExecutionPlan>, per_batch_compute: Duration) -> Self {
            Self {
                input,
                per_batch_compute,
                metrics: ExecutionPlanMetricsSet::new(),
            }
        }
    }

    impl DisplayAs for ComputeExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "ComputeExec")
        }
    }

    impl ExecutionPlan for ComputeExec {
        fn name(&self) -> &'static str {
            "ComputeExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }
        fn schema(&self) -> SchemaRef {
            self.input.schema()
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }
        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(ComputeExec::new(
                children.swap_remove(0),
                self.per_batch_compute,
            )))
        }
        fn metrics(&self) -> Option<MetricsSet> {
            Some(self.metrics.clone_inner())
        }
        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let baseline = BaselineMetrics::new(&self.metrics, partition);
            let mut input = self.input.execute(partition, context)?;
            let compute = self.per_batch_compute;
            let schema = self.schema();
            let stream = async_stream::stream! {
                while let Some(item) = input.next().await {
                    match item {
                        Ok(batch) => {
                            // The timer measures wall-clock exactly as a real
                            // operator's `elapsed_compute` does; the sleep
                            // simulates the transform's own compute.
                            let timer = baseline.elapsed_compute().timer();
                            if !compute.is_zero() {
                                tokio::time::sleep(compute).await;
                            }
                            timer.done();
                            yield Ok(batch);
                        }
                        Err(e) => {
                            yield Err(e);
                            break;
                        }
                    }
                }
            };
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }
    }

    async fn multi_batch_source(num_batches: usize) -> Arc<dyn ExecutionPlan> {
        let batch = create_test_batch_with_checkpoint(&[]);
        let schema = batch.schema();
        let batches = std::iter::repeat_n(batch, num_batches).collect();
        let mem_table = MemTable::try_new(schema, vec![batches]).unwrap();
        let ctx = SessionContext::new();
        // Awaited directly: `block_on` inside a tokio worker panics the
        // moment the scanned future yields.
        mem_table.scan(&ctx.state(), None, &[], None).await.unwrap()
    }

    /// Execute partition 0 of `plan` to completion, discarding the batches.
    async fn drain(plan: &Arc<dyn ExecutionPlan>) {
        let stream = plan.execute(0, SessionContext::new().task_ctx()).unwrap();
        datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
    }

    /// Aggregated `elapsed_compute` of `plan`, in whole milliseconds.
    fn elapsed_ms(plan: &Arc<dyn ExecutionPlan>) -> u64 {
        plan.metrics()
            .and_then(|m| m.elapsed_compute())
            .unwrap_or(0) as u64
            / 1_000_000
    }

    /// Shared body for the boundary-exclusion tests: record ~45ms of real
    /// compute on an operator (executed directly, so a boundary node's own
    /// `execute()` side effects — e.g. `WrappingExec` initializing the
    /// process-global `LiveDataInspect` singleton — never run), place it
    /// below the boundary built by `make_boundary`, and assert
    /// `CheckpointableExec::metrics` excludes it. Thresholds: >=25ms proves
    /// the setup recorded compute (tolerating timer jitter); <10ms proves the
    /// boundary excluded it. Slack is for CI timer jitter — do not env-var-skip.
    async fn assert_boundary_excludes(
        make_boundary: impl FnOnce(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan>,
    ) {
        let below: Arc<dyn ExecutionPlan> = Arc::new(ComputeExec::new(
            multi_batch_source(3).await,
            Duration::from_millis(15),
        ));
        drain(&below).await;
        let below_ms = elapsed_ms(&below);
        assert!(
            below_ms >= 25,
            "test setup: compute below the boundary must have recorded \
             elapsed_compute, got {below_ms}ms"
        );

        let checkpointable: Arc<dyn ExecutionPlan> = Arc::new(CheckpointableExec::new(
            make_boundary(below),
            1,
            "t".to_string(),
        ));
        let aggregated_ms = elapsed_ms(&checkpointable);
        assert!(
            aggregated_ms < 10,
            "compute at/below a topology boundary must be excluded, got {aggregated_ms}ms"
        );
    }

    /// Regression: `CheckpointableExec::metrics` must aggregate `elapsed_compute`
    /// from operators *below* the SQL plan root. Previously it forwarded only
    /// `self.input.metrics()` (the root operator), so compute recorded by deeper
    /// operators was dropped and never reached the node's `elapsed_compute` —
    /// the reason a compute-bound SQL transform under-reported query latency.
    #[tokio::test]
    async fn checkpointable_metrics_aggregate_subtree_compute() {
        const NUM_BATCHES: usize = 3;
        let per_batch = Duration::from_millis(15);

        // Root is a zero-compute passthrough; the real work happens one level
        // deeper (mirroring a `FilterExec` beneath a `ProjectionExec`). Root-only
        // forwarding would therefore report ~0 compute.
        let deep_compute = Arc::new(ComputeExec::new(
            multi_batch_source(NUM_BATCHES).await,
            per_batch,
        ));
        let root: Arc<dyn ExecutionPlan> = Arc::new(ComputeExec::new(deep_compute, Duration::ZERO));

        let checkpointable: Arc<dyn ExecutionPlan> =
            Arc::new(CheckpointableExec::new(root, 1, "t".to_string()));

        drain(&checkpointable).await;

        // ~NUM_BATCHES * 15ms of deep compute; assert well below the nominal
        // total so scheduling jitter can't flake it, but far above the ~0 a
        // root-only forward would report. Slack is for CI timer jitter — do
        // not env-var-skip.
        let aggregated_ms = elapsed_ms(&checkpointable);
        assert!(
            aggregated_ms >= 25,
            "expected aggregated subtree elapsed_compute >= 25ms (got {aggregated_ms}ms); \
             deep-operator compute was dropped"
        );
    }

    /// `CheckpointableExec::metrics` must stop at a nested `WrappingExec`: that is
    /// a separate topology node recording its own `elapsed_compute`, so folding
    /// its subtree in here would double-count another node's compute against this
    /// transform's query latency.
    #[tokio::test]
    async fn checkpointable_metrics_stop_at_nested_wrapping_exec() {
        assert_boundary_excludes(|below| {
            let nested_wrapping: Arc<dyn ExecutionPlan> = Arc::new(WrappingExec::new(
                below,
                "nested_upstream".to_string(),
                vec![],
                vec![],
                None,
            ));
            // A zero-compute root above the boundary, so this transform's own
            // compute is ~0.
            Arc::new(ComputeExec::new(nested_wrapping, Duration::ZERO))
        })
        .await;
    }

    /// `CheckpointableExec::metrics` must also stop when the *root* of its input
    /// is itself a `WrappingExec` (e.g. a passthrough SQL transform whose
    /// projection was pushed down, leaving the upstream node's `WrappingExec`
    /// directly beneath the boundary). Because `WrappingExec` delegates both
    /// `metrics()` and `children()` to its inner plan, a boundary check that
    /// only inspected *children* would collect this node's delegated metrics and
    /// descend into the nested topology — folding another node's compute in.
    #[tokio::test]
    async fn checkpointable_metrics_stop_at_root_wrapping_exec() {
        // The `WrappingExec` is the input's ROOT — no intervening operator
        // between it and the `CheckpointableExec`. If the boundary check only
        // guarded children (not the root), this would collect the compute.
        assert_boundary_excludes(|below| {
            Arc::new(WrappingExec::new(
                below,
                "nested_upstream".to_string(),
                vec![],
                vec![],
                None,
            ))
        })
        .await;
    }

    /// A *source-owned* `StreamingFilterExec` (a source's filter re-applied
    /// above the source's `WrappingExec` by
    /// `wrap_with_side_outputs_before_filter`) must also act as a topology
    /// boundary: its compute belongs to the source, not to the consuming
    /// transform. A transform-owned filter (not marked) is still descended
    /// into.
    #[tokio::test]
    async fn checkpointable_metrics_stop_at_source_owned_filter() {
        use datafusion::physical_plan::expressions::lit;
        use datafusion::physical_plan::filter::FilterExec;

        assert_boundary_excludes(|below| {
            let original = FilterExec::try_new(lit(true), below).unwrap();
            Arc::new(
                StreamingFilterExec::from_original(original)
                    .unwrap()
                    .with_source_owned(),
            )
        })
        .await;

        // Control: the SAME shape without the source-owned mark is the
        // transform's own filter, so the compute below it IS collected.
        let below: Arc<dyn ExecutionPlan> = Arc::new(ComputeExec::new(
            multi_batch_source(3).await,
            Duration::from_millis(15),
        ));
        drain(&below).await;
        let original = FilterExec::try_new(lit(true), Arc::clone(&below)).unwrap();
        let own_filter: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingFilterExec::from_original(original).unwrap());
        let checkpointable: Arc<dyn ExecutionPlan> =
            Arc::new(CheckpointableExec::new(own_filter, 1, "t".to_string()));
        let own_ms = elapsed_ms(&checkpointable);
        assert!(
            own_ms >= 25,
            "a transform-owned filter must not bound the walk, got {own_ms}ms"
        );
    }

    /// Sibling of `checkpointable_metrics_stop_at_source_owned_filter`: a
    /// source-owned `StreamingProjectionExec` must bound the walk the same
    /// way. A transform-owned projection (not marked) is still descended into.
    #[tokio::test]
    async fn checkpointable_metrics_stop_at_source_owned_projection() {
        use datafusion::physical_expr::expressions::Column as PhysicalColumn;
        use datafusion::physical_plan::PhysicalExpr;
        use datafusion::physical_plan::projection::ProjectionExec;

        assert_boundary_excludes(|below| {
            let original = ProjectionExec::try_new(
                vec![(
                    Arc::new(PhysicalColumn::new("id", 0)) as Arc<dyn PhysicalExpr>,
                    "id".to_string(),
                )],
                below,
            )
            .unwrap();
            Arc::new(
                StreamingProjectionExec::from_original(original)
                    .unwrap()
                    .with_source_owned(),
            )
        })
        .await;

        // Control: the SAME shape without the source-owned mark is the
        // transform's own projection, so the compute below it IS collected.
        let below: Arc<dyn ExecutionPlan> = Arc::new(ComputeExec::new(
            multi_batch_source(3).await,
            Duration::from_millis(15),
        ));
        drain(&below).await;
        let original = ProjectionExec::try_new(
            vec![(
                Arc::new(PhysicalColumn::new("id", 0)) as Arc<dyn PhysicalExpr>,
                "id".to_string(),
            )],
            Arc::clone(&below),
        )
        .unwrap();
        let own_proj: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingProjectionExec::from_original(original).unwrap());
        let checkpointable: Arc<dyn ExecutionPlan> =
            Arc::new(CheckpointableExec::new(own_proj, 1, "t".to_string()));
        let own_ms = elapsed_ms(&checkpointable);
        assert!(
            own_ms >= 25,
            "a transform-owned projection must not bound the walk, got {own_ms}ms"
        );
    }

    /// A source-owned filter must refuse DataFusion's ProjectionPushdown
    /// swap outright: pushing the transform's projection below the boundary
    /// would exclude the transform's own projection compute from its metrics,
    /// and rebuilding the filter risks dropping the boundary mark. An
    /// unmarked (transform-owned) filter must still allow the swap.
    ///
    /// NOTE: the production session never runs ProjectionPushdown (the
    /// physical rule set is replaced by `StreamlingPhysicalOptimizerRules`,
    /// see session.rs); this covers the defense-in-depth guard for embedders
    /// and future rule-set changes.
    #[tokio::test]
    async fn source_owned_filter_refuses_projection_swap() {
        use datafusion::physical_expr::expressions::Column as PhysicalColumn;
        use datafusion::physical_plan::PhysicalExpr;
        use datafusion::physical_plan::expressions::lit;
        use datafusion::physical_plan::filter::FilterExec;
        use datafusion::physical_plan::projection::ProjectionExec;

        let input = multi_batch_source(1).await;
        let original = FilterExec::try_new(lit(true), input).unwrap();
        let filter: Arc<dyn ExecutionPlan> = Arc::new(
            StreamingFilterExec::from_original(original)
                .unwrap()
                .with_source_owned(),
        );

        // A narrowing projection directly above the filter, as ProjectionPushdown
        // would present it.
        let narrowing_projection = |child: &Arc<dyn ExecutionPlan>| {
            ProjectionExec::try_new(
                vec![(
                    Arc::new(PhysicalColumn::new("id", 0)) as Arc<dyn PhysicalExpr>,
                    "id".to_string(),
                )],
                Arc::clone(child),
            )
            .unwrap()
        };

        let swapped = filter
            .try_swapping_with_projection(&narrowing_projection(&filter))
            .unwrap();
        assert!(
            swapped.is_none(),
            "a source-owned filter must refuse the projection swap"
        );
        assert!(
            filter
                .downcast_ref::<StreamingFilterExec>()
                .expect("original filter still in place")
                .is_source_owned(),
            "refusing the swap must leave the original boundary mark in place"
        );

        // Control: the same shape without the mark swaps as usual.
        let original = FilterExec::try_new(lit(true), multi_batch_source(1).await).unwrap();
        let own_filter: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingFilterExec::from_original(original).unwrap());
        let swapped = own_filter
            .try_swapping_with_projection(&narrowing_projection(&own_filter))
            .unwrap();
        assert!(
            swapped.is_some(),
            "a transform-owned filter must still allow the swap"
        );
    }

    /// One Arc-shared subplan reachable from two parents (a DAG, e.g. UNION
    /// ALL over a common input) must have its metrics counted once, not once
    /// per path.
    #[tokio::test]
    async fn checkpointable_metrics_count_shared_subplan_once() {
        use datafusion::physical_plan::union::UnionExec;

        const NUM_BATCHES: usize = 3;
        let per_batch = Duration::from_millis(15);

        let shared: Arc<dyn ExecutionPlan> = Arc::new(ComputeExec::new(
            multi_batch_source(NUM_BATCHES).await,
            per_batch,
        ));
        drain(&shared).await;

        let single_ms = elapsed_ms(&shared);
        assert!(single_ms >= 25, "setup: expected recorded compute");

        let union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![Arc::clone(&shared), Arc::clone(&shared)]).unwrap();
        let checkpointable: Arc<dyn ExecutionPlan> =
            Arc::new(CheckpointableExec::new(union, 1, "t".to_string()));

        let aggregated_ms = elapsed_ms(&checkpointable);
        assert_eq!(
            aggregated_ms, single_ms,
            "shared subplan must be counted once, not per parent"
        );
    }
}
