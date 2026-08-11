use crate::operators::coalesce::StreamingCoalesceExec;
use crate::operators::filter::StreamingFilterExec;
use crate::operators::projection::StreamingProjectionExec;
use crate::operators::repartition::{
    Exchange, StreamingRepartitionExec, satisfies_hash_distribution,
};
use crate::operators::unnest::StreamingUnnestExec;
use crate::session::StreamlingConfig;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::{Distribution, PhysicalExprRef};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::unnest::UnnestExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

/// A rule to rewrite `FilterExec` to `StreamingFilterExec`
#[derive(Clone, Debug)]
pub struct StreamingFilterRewritePhysicalOptimizerRule {}

impl StreamingFilterRewritePhysicalOptimizerRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StreamingFilterRewritePhysicalOptimizerRule {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for StreamingFilterRewritePhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|input_plan| {
            if let Some(original_filter) = input_plan.downcast_ref::<FilterExec>() {
                let streaming_filter =
                    StreamingFilterExec::from_original(original_filter.clone()).unwrap();
                Ok(Transformed::yes(Arc::new(streaming_filter)))
            } else {
                Ok(Transformed::no(input_plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "StreamingFilterRewrite"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A rule to rewrite `ProjectionExec` to `StreamingProjectionExec`
#[derive(Clone, Debug)]
pub struct StreamingProjectionRewritePhysicalOptimizerRule {}

impl StreamingProjectionRewritePhysicalOptimizerRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StreamingProjectionRewritePhysicalOptimizerRule {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for StreamingProjectionRewritePhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|input_plan| {
            if let Some(original_projection) = input_plan.downcast_ref::<ProjectionExec>() {
                let streaming_projection =
                    StreamingProjectionExec::from_original(original_projection.clone()).unwrap();
                Ok(Transformed::yes(Arc::new(streaming_projection)))
            } else {
                Ok(Transformed::no(input_plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "StreamingProjectionRewrite"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A rule to rewrite `UnnestExec` to `StreamingUnnestExec`
#[derive(Clone, Debug)]
pub struct StreamingUnnestRewritePhysicalOptimizerRule {}

impl StreamingUnnestRewritePhysicalOptimizerRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StreamingUnnestRewritePhysicalOptimizerRule {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for StreamingUnnestRewritePhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|input_plan| {
            if let Some(original_unnest) = input_plan.downcast_ref::<UnnestExec>() {
                let streaming_unnest =
                    StreamingUnnestExec::from_original(original_unnest.clone()).unwrap();
                Ok(Transformed::yes(Arc::new(streaming_unnest)))
            } else {
                Ok(Transformed::no(input_plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "StreamingUnnestRewrite"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Enforces declared `Distribution::SinglePartition` input requirements, and
/// fails fast on `HashPartitioned` requirements that cannot be met.
///
/// Streamling replaces DataFusion's default physical optimizer rule set, which
/// removes `EnforceDistribution` (and `SanityCheckPlan`), so declared input
/// distribution requirements are otherwise never satisfied — operators like
/// `MultiSinkExec` (and DataFusion's own `DataSinkExec`, still built by
/// non-streamling providers such as `MemTable`) pull only input partition 0
/// and would silently drop every other partition's rows. This rule restores
/// that invariant with streamling's marker-preserving `StreamingCoalesceExec`
/// (a stock `CoalescePartitionsExec` rebuilds batches with a metadata-less
/// schema and would drop checkpoint markers).
///
/// `HashPartitioned` requirements (e.g. `AggregateMode::FinalPartitioned`,
/// which DataFusion plans for GROUP BY whenever `target_partitions > 1`) are
/// satisfied trivially by single-partition input, but with a multi-partition
/// input each partition would finalize its groups independently — silently
/// wrong aggregates. This rule inserts a `StreamingRepartitionExec` for bounded
/// children; an unbounded child stays a planning error, because a partitioned
/// aggregate over a stream never emits and would buffer its groups forever.
#[derive(Clone, Debug)]
pub struct EnforceSinglePartitionPhysicalOptimizerRule {}

impl EnforceSinglePartitionPhysicalOptimizerRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for EnforceSinglePartitionPhysicalOptimizerRule {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for EnforceSinglePartitionPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let buffer_size = config
            .extensions
            .get::<StreamlingConfig>()
            .map(|c| c.internal_buffer_size)
            .unwrap_or_else(|| StreamlingConfig::default().internal_buffer_size);

        plan.transform_up(|input_plan| {
            // `UNION ALL` sums its children's partitions, and its branches can carry
            // checkpoint markers and CDC streams whose correctness (marker acking,
            // upsert/delete ordering, keep-last dedup) depends on one ordered stream
            // at the sink — merge it back to a single partition here. This is where
            // the pre-partition-transparent `CheckpointableExec` used to merge.
            if input_plan.downcast_ref::<UnionExec>().is_some()
                && input_plan.output_partitioning().partition_count() > 1
            {
                return Ok(Transformed::yes(Arc::new(StreamingCoalesceExec::new(
                    input_plan,
                    buffer_size,
                ))));
            }
            // Streamling removed DataFusion's `JoinSelection` rule, so a hash join
            // planned in `Auto` partition mode is never resolved and fails at
            // runtime with an internal error; fail at plan time instead. Top-level
            // JOINs are rejected by transform validation, but decorrelated
            // IN/EXISTS/scalar subqueries still plan joins.
            if let Some(join) = input_plan.downcast_ref::<HashJoinExec>()
                && matches!(join.partition_mode(), PartitionMode::Auto)
            {
                return plan_err!(
                    "the query plans a hash join in Auto partition mode, which \
                     streamling cannot execute; rewrite the query to avoid joins or \
                     IN/EXISTS/scalar subqueries in this context"
                );
            }
            let requirements = input_plan.required_input_distribution();
            let mut hash_repartitioned_children: Vec<Option<Vec<PhysicalExprRef>>> = Vec::new();
            for (child, distribution) in input_plan.children().iter().zip(requirements.iter()) {
                let Distribution::HashPartitioned(exprs) = distribution else {
                    hash_repartitioned_children.push(None);
                    continue;
                };
                if satisfies_hash_distribution(child.properties(), exprs) {
                    hash_repartitioned_children.push(None);
                    continue;
                }
                // A bounded child can be exchanged safely: every partition ends,
                // so a `FinalPartitioned` aggregate over it terminates. An
                // unbounded one would buffer its groups forever, and with
                // `SanityCheckPlan` disabled nothing else would catch it — fail
                // at plan time instead.
                if child.boundedness().is_unbounded() {
                    return plan_err!(
                        "'{}' requires hash-partitioned input but its input is an \
                         unbounded stream with {} partitions; rewrite the query to \
                         avoid partitioned aggregation or window functions over a \
                         multi-partition streaming source",
                        input_plan.name(),
                        child.output_partitioning().partition_count()
                    );
                }
                hash_repartitioned_children.push(Some(exprs.clone()));
            }
            if hash_repartitioned_children.iter().any(Option::is_some) {
                let target = input_plan.output_partitioning().partition_count().max(1);
                let new_children: Vec<Arc<dyn ExecutionPlan>> = input_plan
                    .children()
                    .iter()
                    .zip(hash_repartitioned_children)
                    .map(|(child, exprs)| match exprs {
                        Some(exprs) => Arc::new(StreamingRepartitionExec::new(
                            Arc::clone(child),
                            Exchange::Hash(exprs),
                            target,
                            buffer_size,
                        )) as Arc<dyn ExecutionPlan>,
                        None => Arc::clone(child),
                    })
                    .collect();
                return Ok(Transformed::yes(
                    input_plan.with_new_children(new_children)?,
                ));
            }
            let violated = input_plan.children().iter().zip(requirements.iter()).any(
                |(child, distribution)| {
                    matches!(distribution, Distribution::SinglePartition)
                        && child.output_partitioning().partition_count() > 1
                },
            );
            if !violated {
                return Ok(Transformed::no(input_plan));
            }

            let new_children: Vec<Arc<dyn ExecutionPlan>> = input_plan
                .children()
                .iter()
                .zip(requirements.iter())
                .map(|(child, distribution)| {
                    let partitions = child.output_partitioning().partition_count();
                    if matches!(distribution, Distribution::SinglePartition) && partitions > 1 {
                        Arc::new(StreamingCoalesceExec::new(Arc::clone(child), buffer_size))
                            as Arc<dyn ExecutionPlan>
                    } else {
                        Arc::clone(child)
                    }
                })
                .collect();
            Ok(Transformed::yes(
                input_plan.with_new_children(new_children)?,
            ))
        })
        .data()
    }

    fn name(&self) -> &str {
        "EnforceSinglePartition"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

pub struct StreamlingPhysicalOptimizerRules {}

impl StreamlingPhysicalOptimizerRules {
    pub fn rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        vec![
            Arc::new(StreamingFilterRewritePhysicalOptimizerRule::new()),
            Arc::new(StreamingProjectionRewritePhysicalOptimizerRule::new()),
            Arc::new(StreamingUnnestRewritePhysicalOptimizerRule::new()),
            Arc::new(EnforceSinglePartitionPhysicalOptimizerRule::new()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynamic_table::DynamicTableRegistry;
    use crate::session::SessionManager;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};

    fn batch_with_ids(schema: &Arc<Schema>, ids: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))]).unwrap()
    }

    fn count_operator<T: ExecutionPlan>(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let own = usize::from(plan.downcast_ref::<T>().is_some());
        own + plan
            .children()
            .iter()
            .map(|child| count_operator::<T>(child))
            .sum::<usize>()
    }

    fn count_streaming_coalesce(plan: &Arc<dyn ExecutionPlan>) -> usize {
        count_operator::<StreamingCoalesceExec>(plan)
    }

    /// `DataSinkExec` pulls only input partition 0, and streamling's session
    /// replaces DataFusion's default physical optimizer rules (no
    /// `EnforceDistribution`), so without `EnforceSinglePartition` a
    /// multi-partition input to a sink would be silently truncated to
    /// partition 0's rows.
    #[tokio::test]
    async fn multi_partition_sink_input_is_coalesced_and_fully_written() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let source = MemTable::try_new(
            schema.clone(),
            vec![
                vec![batch_with_ids(&schema, vec![1, 2, 3])],
                vec![batch_with_ids(&schema, vec![4, 5, 6])],
            ],
        )
        .unwrap();
        let target = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();

        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
        let ctx = session_manager.session_context();
        ctx.register_table("source_table", Arc::new(source))
            .unwrap();
        ctx.register_table("target_table", Arc::new(target))
            .unwrap();

        let insert = ctx
            .sql("INSERT INTO target_table SELECT id FROM source_table")
            .await
            .unwrap();
        let physical_plan = insert.clone().create_physical_plan().await.unwrap();
        assert_eq!(
            count_streaming_coalesce(&physical_plan),
            1,
            "the sink's SinglePartition requirement must be satisfied by a \
             marker-preserving coalesce"
        );

        insert.collect().await.unwrap();

        let written: usize = ctx
            .sql("SELECT id FROM target_table")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        assert_eq!(
            written, 6,
            "rows from every input partition must be written"
        );
    }

    /// `UNION ALL` sums its children's partitions and its branches can carry
    /// checkpoint markers and CDC streams that need one ordered stream at the
    /// sink; the rule must merge a multi-partition union back to a single
    /// partition (marker-preserving), and every branch's rows must survive.
    #[tokio::test]
    async fn multi_partition_union_is_coalesced() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let source = MemTable::try_new(
            schema.clone(),
            vec![vec![batch_with_ids(&schema, vec![1, 2, 3])]],
        )
        .unwrap();

        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
        let ctx = session_manager.session_context();
        ctx.register_table("source_table", Arc::new(source))
            .unwrap();

        let union = ctx
            .sql("SELECT id FROM source_table UNION ALL SELECT id FROM source_table")
            .await
            .unwrap();
        let physical_plan = union.clone().create_physical_plan().await.unwrap();
        assert_eq!(
            count_streaming_coalesce(&physical_plan),
            1,
            "the union's summed partitions must be merged by a marker-preserving \
             coalesce"
        );
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            1,
            "the plan above the union must be single-partition"
        );

        let rows: usize = union
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        assert_eq!(rows, 6, "every union branch's rows must survive the merge");
    }

    /// With `target_partitions > 1` DataFusion plans GROUP BY as
    /// `AggregateMode::FinalPartitioned`, which requires hash-partitioned
    /// input. Over a bounded multi-partition source the rule must insert a hash
    /// exchange so every partition finalizes a disjoint set of groups, and the
    /// aggregate must come out correct.
    #[tokio::test]
    async fn hash_partitioned_requirement_is_met_by_a_repartition() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let source = MemTable::try_new(
            schema.clone(),
            vec![
                vec![batch_with_ids(&schema, vec![1, 1, 2])],
                vec![batch_with_ids(&schema, vec![1, 2, 2])],
            ],
        )
        .unwrap();

        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
        let ctx = session_manager.session_context();
        if ctx.state().config().target_partitions() < 2 {
            // Single-core machine: DataFusion plans a single-partition
            // aggregation and the guarded case cannot occur.
            return;
        }
        ctx.register_table("source_table", Arc::new(source))
            .unwrap();

        let df = ctx
            .sql("SELECT id, count(*) AS n FROM source_table GROUP BY id")
            .await
            .unwrap();
        let physical_plan = df.clone().create_physical_plan().await.unwrap();
        assert_eq!(
            count_operator::<StreamingRepartitionExec>(&physical_plan),
            1,
            "the aggregate's HashPartitioned requirement must be met by an exchange"
        );

        let batches = df.collect().await.unwrap();
        let mut counts: Vec<(i32, i64)> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let ns = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|i| (ids.value(i), ns.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect();
        counts.sort();
        assert_eq!(
            counts,
            vec![(1, 3), (2, 3)],
            "each group must be finalized exactly once, across all partitions"
        );
    }

    /// An unbounded child is the case that stays a planning error: a
    /// `FinalPartitioned` aggregate over a stream never emits and would buffer
    /// its groups forever, and `SanityCheckPlan` is disabled.
    #[tokio::test]
    async fn hash_partitioned_requirement_over_an_unbounded_source_is_a_plan_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let consumer: Arc<dyn ExecutionPlan> =
            Arc::new(HashRequiringExec::new(Arc::new(StubSourceExec::new(
                schema,
                2,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            ))));

        let error = EnforceSinglePartitionPhysicalOptimizerRule::new()
            .optimize(consumer, &ConfigOptions::default())
            .expect_err("a keyed requirement over an unbounded source must not plan");
        assert!(
            error.to_string().contains("hash-partitioned"),
            "unexpected error: {error}"
        );
    }

    /// A multi-partition source with no rows: enough to exercise plan-time rules.
    #[derive(Debug)]
    struct StubSourceExec {
        schema: Arc<Schema>,
        cache: Arc<PlanProperties>,
    }

    impl StubSourceExec {
        fn new(schema: Arc<Schema>, partitions: usize, boundedness: Boundedness) -> Self {
            let cache = PlanProperties::new(
                datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
                datafusion::physical_expr::Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                boundedness,
            );
            Self {
                schema,
                cache: Arc::new(cache),
            }
        }
    }

    impl DisplayAs for StubSourceExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "StubSourceExec")
        }
    }

    impl ExecutionPlan for StubSourceExec {
        fn name(&self) -> &'static str {
            "StubSourceExec"
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
            _context: Arc<datafusion::execution::TaskContext>,
        ) -> Result<datafusion::execution::SendableRecordBatchStream> {
            Ok(Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    self.schema.clone(),
                    futures::stream::empty(),
                ),
            ))
        }
    }

    /// Stands in for `AggregateExec` in `FinalPartitioned` mode — the only thing
    /// the rule looks at is the declared distribution.
    #[derive(Debug)]
    struct HashRequiringExec {
        input: Arc<dyn ExecutionPlan>,
    }

    impl HashRequiringExec {
        fn new(input: Arc<dyn ExecutionPlan>) -> Self {
            Self { input }
        }
    }

    impl DisplayAs for HashRequiringExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "HashRequiringExec")
        }
    }

    impl ExecutionPlan for HashRequiringExec {
        fn name(&self) -> &'static str {
            "HashRequiringExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }
        fn required_input_distribution(&self) -> Vec<Distribution> {
            vec![Distribution::HashPartitioned(vec![Arc::new(
                datafusion::physical_expr::expressions::Column::new("id", 0),
            )])]
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(HashRequiringExec::new(Arc::clone(&children[0]))))
        }
        fn execute(
            &self,
            partition: usize,
            context: Arc<datafusion::execution::TaskContext>,
        ) -> Result<datafusion::execution::SendableRecordBatchStream> {
            self.input.execute(partition, context)
        }
    }
}
