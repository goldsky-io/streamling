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
use datafusion::physical_plan::metrics::MetricsSet;
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
        self.input.metrics()
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
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_expr::Partitioning as PhysicalPartitioning;
    use datafusion::prelude::*;
    use futures::StreamExt;

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
