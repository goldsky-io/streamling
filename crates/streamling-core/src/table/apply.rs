//! Applies the table's input to its [`InMemoryTable`] and emits the table's
//! changelog.

use crate::table::InMemoryTable;
use async_trait::async_trait;
use datafusion::common::{DFSchemaRef, Statistics};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// [`TableApplyNode`]'s logical node name.
pub const TABLE_APPLY_NODE_NAME: &str = "TableApply";

/// Logical node whose output is the table's changelog, in the input's schema.
pub struct TableApplyNode {
    input: LogicalPlan,
    table: Arc<InMemoryTable>,
    deduplicate: bool,
}

impl TableApplyNode {
    pub fn new(input: LogicalPlan, table: Arc<InMemoryTable>, deduplicate: bool) -> Self {
        Self {
            input,
            table,
            deduplicate,
        }
    }
}

// The table holds data, not plan structure, so nodes compare by its name.
impl PartialEq for TableApplyNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.table.name == other.table.name
            && self.deduplicate == other.deduplicate
    }
}

impl Eq for TableApplyNode {}

impl Hash for TableApplyNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.table.name.hash(state);
        self.deduplicate.hash(state);
    }
}

impl PartialOrd for TableApplyNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (&self.input, &self.table.name, self.deduplicate).partial_cmp(&(
            &other.input,
            &other.table.name,
            other.deduplicate,
        ))
    }
}

impl fmt::Debug for TableApplyNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for TableApplyNode {
    fn name(&self) -> &str {
        TABLE_APPLY_NODE_NAME
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
        write!(f, "TableApply: {}", self.table.name)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Ok(Self::new(
            inputs.swap_remove(0),
            Arc::clone(&self.table),
            self.deduplicate,
        ))
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

pub struct TableApplyExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for TableApplyExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(node.as_any().downcast_ref::<TableApplyNode>().map(|apply| {
            Arc::new(TableApplyExec::new(
                Arc::clone(&physical_inputs[0]),
                Arc::clone(&apply.table),
                apply.deduplicate,
            )) as Arc<dyn ExecutionPlan>
        }))
    }
}

struct TableApplyExec {
    input: Arc<dyn ExecutionPlan>,
    table: Arc<InMemoryTable>,
    deduplicate: bool,
    cache: Arc<PlanProperties>,
}

impl TableApplyExec {
    fn new(input: Arc<dyn ExecutionPlan>, table: Arc<InMemoryTable>, deduplicate: bool) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Self {
            input,
            table,
            deduplicate,
            cache: Arc::new(cache),
        }
    }
}

impl fmt::Debug for TableApplyExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TableApplyExec")
    }
}

impl DisplayAs for TableApplyExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TableApplyExec: table={}", self.table.name)
    }
}

impl ExecutionPlan for TableApplyExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    /// One partition keeps each key's apply order equal to stream order.
    /// `EnforceSinglePartition` inserts the marker-preserving coalesce.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            Arc::clone(&self.table),
            self.deduplicate,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let table = Arc::clone(&self.table);
        let deduplicate = self.deduplicate;
        let changelog = self.input.execute(partition, context)?.map(move |batch| {
            batch.and_then(|batch| table.apply(&batch, deduplicate).map_err(Into::into))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            changelog,
        )))
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
    use crate::data::COLUMN_NAME_OP;
    use crate::dynamic_table::DynamicTableRegistry;
    use crate::operators::coalesce::StreamingCoalesceExec;
    use crate::session::SessionManager;
    use arrow::array::{AsArray, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
    use datafusion::datasource::MemTable;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::Extension;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;

    fn fields() -> Vec<Field> {
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]
    }

    fn table(schema: &SchemaRef) -> Arc<InMemoryTable> {
        Arc::new(
            InMemoryTable::try_new(
                "tokens",
                schema,
                &["id".to_string()],
                "app::tokens".to_string(),
            )
            .unwrap(),
        )
    }

    fn inserts(schema: &SchemaRef, ids: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(vec!["i"; ids.len()])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn forwards_checkpoint_markers_on_an_empty_batch() {
        let mut metadata = HashMap::new();
        enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(7),
                created_at_ms: 0,
            }],
        );
        let schema = Arc::new(Schema::new_with_metadata(fields(), metadata));
        let input = MemorySourceConfig::try_new_exec(
            &[vec![RecordBatch::new_empty(Arc::clone(&schema))]],
            Arc::clone(&schema),
            None,
        )
        .unwrap();
        let exec = Arc::new(TableApplyExec::new(input, table(&schema), true));

        let output = collect(exec, SessionContext::new().task_ctx())
            .await
            .unwrap();

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 0);
        assert!(matches!(
            extract_checkpoint_messages(output[0].schema().metadata()).as_slice(),
            [CheckpointMessage::Marker {
                epoch: CheckpointEpoch(7),
                ..
            }]
        ));
    }

    #[tokio::test]
    async fn multi_partition_input_is_coalesced_into_one_apply() {
        let schema = Arc::new(Schema::new(fields()));
        let source = MemTable::try_new(
            Arc::clone(&schema),
            vec![
                vec![inserts(&schema, &[1, 2, 3])],
                vec![inserts(&schema, &[4, 5, 6])],
            ],
        )
        .unwrap();
        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 2).unwrap();
        let ctx = session_manager.session_context();
        ctx.register_table("src", Arc::new(source)).unwrap();
        let scan = ctx.table("src").await.unwrap().into_unoptimized_plan();
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(TableApplyNode::new(scan, table(&schema), true)),
        });

        let physical = session_manager
            .new_df(plan)
            .create_physical_plan()
            .await
            .unwrap();
        let rendered = displayable(physical.as_ref()).indent(true).to_string();
        let apply = physical
            .downcast_ref::<TableApplyExec>()
            .unwrap_or_else(|| panic!("TableApplyExec must be the root:\n{rendered}"));
        assert!(
            apply
                .input
                .downcast_ref::<StreamingCoalesceExec>()
                .is_some(),
            "the apply's input must be coalesced:\n{rendered}"
        );

        let output = collect(physical, ctx.task_ctx()).await.unwrap();
        let mut ids: Vec<i64> = output
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        ids.sort();
        assert_eq!(ids, [1, 2, 3, 4, 5, 6]);
    }
}
