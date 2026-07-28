//! An operator that can wrap any input LogicalPlan and execute checkpointing logic after every batch.
//! It propagates the input data as is, preserving checkpoint messages in batch metadata.
//! This is useful for adding checkpointing support to SQL transformations.

use async_trait::async_trait;
use datafusion::common::{DFSchemaRef, Statistics};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};

use crate::operators::coalesce::spawn_marker_preserving_forwarder;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

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

struct CheckpointableExec {
    input: Arc<dyn ExecutionPlan>,
    internal_buffer_size: u32,
    reference_name: String,
    cache: Arc<PlanProperties>,
}

impl CheckpointableExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        internal_buffer_size: u32,
        reference_name: String,
    ) -> Self {
        let cache = Self::compute_properties(&input);
        Self {
            input,
            internal_buffer_size,
            reference_name,
            cache: Arc::new(cache),
        }
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            // Partition-transparent: each input partition is forwarded as its own
            // output partition, so a multi-partition source stays parallel through
            // the transform all the way to the sink (`ParallelSinkExec` writes every
            // partition concurrently).
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
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
        // Partition-transparent, no distribution requirement. Requiring
        // `SinglePartition` here would make DataFusion insert a stock
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
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Forward this partition's batches with their checkpoint-marker metadata
        // intact, through a channel task so the transform keeps computing while
        // downstream (e.g. a sink write) is busy. Checkpoint markers ride per
        // partition: a marker is forwarded on the partition stream it arrived on
        // (no cross-partition barrier alignment — today's only multi-partition
        // source, the bounded file source, emits no markers).
        let output_schema = self.schema();
        let mut builder = RecordBatchReceiverStreamBuilder::new(
            output_schema.clone(),
            self.internal_buffer_size as usize,
        );
        spawn_marker_preserving_forwarder(
            &mut builder,
            &self.input,
            partition,
            &output_schema,
            &context,
            format!("CheckpointableExec [{}]", self.reference_name),
        )?;
        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::checkpoint_management::{
        CheckpointEpoch, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
        extract_checkpoint_messages,
    };
    use crate::optimizer::StreamlingPhysicalOptimizerRules;
    use arrow::array::{BooleanArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{Schema, SchemaRef};
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_expr::Partitioning as PhysicalPartitioning;
    use datafusion::prelude::*;
    use futures::StreamExt;
    use std::collections::HashMap;

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
}
