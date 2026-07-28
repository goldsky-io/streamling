use crate::operators::coalesce::StreamingCoalesceExec;
use crate::operators::filter::StreamingFilterExec;
use crate::operators::projection::StreamingProjectionExec;
use crate::operators::unnest::StreamingUnnestExec;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::Distribution;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
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
/// wrong aggregates. Streamling has no repartition operator, so that case is
/// a planning error instead.
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
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|input_plan| {
            let requirements = input_plan.required_input_distribution();
            for (child, distribution) in input_plan.children().iter().zip(requirements.iter()) {
                if matches!(distribution, Distribution::HashPartitioned(_))
                    && child.output_partitioning().partition_count() > 1
                {
                    return plan_err!(
                        "'{}' requires hash-partitioned input but its input has {} \
                         partitions and streamling does not repartition; rewrite the \
                         query to avoid partitioned aggregation or window functions \
                         over a multi-partition source",
                        input_plan.name(),
                        child.output_partitioning().partition_count()
                    );
                }
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
                        Arc::new(StreamingCoalesceExec::new(Arc::clone(child), partitions))
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

    fn batch_with_ids(schema: &Arc<Schema>, ids: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))]).unwrap()
    }

    fn count_streaming_coalesce(plan: &Arc<dyn ExecutionPlan>) -> usize {
        let own = usize::from(plan.downcast_ref::<StreamingCoalesceExec>().is_some());
        own + plan
            .children()
            .iter()
            .map(|child| count_streaming_coalesce(child))
            .sum::<usize>()
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

        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new()).unwrap();
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

    /// With `target_partitions > 1` DataFusion plans GROUP BY as
    /// `AggregateMode::FinalPartitioned`, which requires hash-partitioned
    /// input. Streamling never repartitions, so over a multi-partition source
    /// each partition would finalize its groups independently — silently wrong
    /// aggregates. The rule must turn that into a planning error.
    #[tokio::test]
    async fn unsatisfiable_hash_partitioned_requirement_is_a_plan_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let source = MemTable::try_new(
            schema.clone(),
            vec![
                vec![batch_with_ids(&schema, vec![1, 1, 2])],
                vec![batch_with_ids(&schema, vec![1, 2, 2])],
            ],
        )
        .unwrap();

        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new()).unwrap();
        let ctx = session_manager.session_context();
        if ctx.state().config().target_partitions() < 2 {
            // Single-core machine: DataFusion plans a single-partition
            // aggregation and the guarded case cannot occur.
            return;
        }
        ctx.register_table("source_table", Arc::new(source))
            .unwrap();

        let error = ctx
            .sql("SELECT id, count(*) FROM source_table GROUP BY id")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .expect_err("partitioned aggregation over a multi-partition source must not plan");
        assert!(
            error.to_string().contains("hash-partitioned"),
            "unexpected error: {error}"
        );
    }
}
