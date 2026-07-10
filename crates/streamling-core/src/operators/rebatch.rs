use crate::utils::batch_accumulator::AsyncBatchAccumulator;
use async_trait::async_trait;
use datafusion::common::{DFSchemaRef, Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
// ============================================================================
// Physical plan: RebatchExec
// ============================================================================

/// A physical operator that accumulates input batches and emits merged,
/// re-sized batches based on row count and/or time interval thresholds.
///
/// Unlike `WrappingExec`, this operator performs no telemetry, side output
/// processing, or live inspection — it only re-batches.
#[derive(Debug)]
pub struct RebatchExec {
    inner: Arc<dyn ExecutionPlan>,
    batch_size: usize,
    batch_flush_interval: Option<Duration>,
    name: String,
}

impl RebatchExec {
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        batch_size: usize,
        batch_flush_interval: Option<Duration>,
        name: String,
    ) -> Self {
        Self {
            inner,
            batch_size,
            batch_flush_interval,
            name,
        }
    }
}

/// RebatchExec must be a *real* plan node, not a transparent alias for its
/// input. It used to `delegate!` most `ExecutionPlan` methods to `self.inner`
/// — including `children()` and the optimizer-negotiation hooks
/// (`try_swapping_with_projection`, `with_fetch`, `repartitioned`,
/// `supports_limit_pushdown`). Each of those hooks returns a rewritten
/// subtree that does NOT contain this wrapper, so DataFusion 54's hook-driven
/// physical optimizer rules (e.g. ProjectionPushdown) replaced the wrapper
/// with the returned plan and silently deleted the rebatcher; sinks then
/// received raw per-scan batches (one INSERT round-trip per scan batch on
/// Postgres). The hooks now fall back to the trait's no-op defaults, making
/// this node an optimizer barrier — see the tests module below.
impl ExecutionPlan for RebatchExec {
    fn name(&self) -> &str {
        "RebatchExec"
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        // Row-, order-, and partitioning-preserving: the input's properties
        // describe this node's output too.
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Rebatching is per-partition stream accumulation; don't invite the
        // optimizer to insert repartitions below it.
        vec![false]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "RebatchExec expects exactly one child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(RebatchExec::new(
            children.swap_remove(0),
            self.batch_size,
            self.batch_flush_interval,
            self.name.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let data = self.inner.execute(partition, context)?;
        let schema = data.schema();

        let accumulator = AsyncBatchAccumulator::new(self.batch_size, self.batch_flush_interval)
            .with_name(self.name.clone());

        let rebatched = accumulator.process_stream(data);

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, rebatched)))
    }
}

impl DisplayAs for RebatchExec {
    fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match format {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RebatchExec(batch_size={}", self.batch_size)?;
                if let Some(interval) = self.batch_flush_interval {
                    write!(f, ", interval={:?}", interval)?;
                }
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "RebatchExec")
            }
        }
    }
}

/// Effective rebatching configuration attached to a sink. Built from the
/// sink's topology fields (plus any sink-type-specific app_config defaults)
/// and consumed by the logical/physical rebatch wrappers. When both fields
/// are `None`, wrapping is a no-op.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Hash)]
pub struct RebatchConfig {
    pub batch_size: Option<u32>,
    pub batch_flush_interval: Option<Duration>,
}

impl RebatchConfig {
    pub fn new(batch_size: Option<u32>, batch_flush_interval: Option<Duration>) -> Self {
        Self {
            batch_size,
            batch_flush_interval,
        }
    }

    pub fn is_noop(&self) -> bool {
        self.batch_size.is_none() && self.batch_flush_interval.is_none()
    }
}

#[derive(PartialEq, Eq, PartialOrd, Hash, Clone)]
pub struct RebatchNode {
    pub input: LogicalPlan,
    pub batch_size: usize,
    pub batch_flush_interval: Option<Duration>,
    pub name: String,
}

impl RebatchNode {
    pub fn new(
        input: LogicalPlan,
        batch_size: usize,
        batch_flush_interval: Option<Duration>,
        name: String,
    ) -> Self {
        Self {
            input,
            batch_size,
            batch_flush_interval,
            name,
        }
    }
}

impl Debug for RebatchNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for RebatchNode {
    fn name(&self) -> &str {
        "RebatchNode"
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
        write!(f, "Rebatch batch_size={}", self.batch_size)?;
        if let Some(interval) = self.batch_flush_interval {
            write!(f, " interval={:?}", interval)?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Ok(Self {
            input: inputs.swap_remove(0),
            batch_size: self.batch_size,
            batch_flush_interval: self.batch_flush_interval,
            name: self.name.clone(),
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

pub struct RebatchExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for RebatchExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(rebatch_node) = node.as_any().downcast_ref::<RebatchNode>() {
                let input_physical = planner
                    .create_physical_plan(&rebatch_node.input, session_state)
                    .await?;

                Some(Arc::new(RebatchExec::new(
                    input_physical,
                    rebatch_node.batch_size,
                    rebatch_node.batch_flush_interval,
                    rebatch_node.name.clone(),
                )))
            } else {
                None
            },
        )
    }
}

#[cfg(test)]
mod tests {
    //! Regression check: DataFusion 54's ProjectionPushdown physical rule must
    //! not delete RebatchExec from the plan.
    //!
    //! RebatchExec used to `delegate!` `try_swapping_with_projection` to its
    //! inner plan. The inner plan's swap returns a rewritten subtree that does
    //! NOT contain the wrapper, so if the rule accepts it, the sink/script
    //! rebatcher silently disappears and sinks receive raw per-scan batches —
    //! on Postgres that means one INSERT round-trip per scan batch, which
    //! throttles high-rate pipelines with no error signal.

    use super::*;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;

    /// Plan shape mirroring a sink path: an outer projection above the
    /// sink-local rebatcher, a transform projection below it.
    fn sandwich() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let source = Arc::new(EmptyExec::new(schema.clone()));
        let inner_proj = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (col("a", &schema).unwrap(), "a".to_string()),
                    (col("b", &schema).unwrap(), "b".to_string()),
                ],
                source,
            )
            .unwrap(),
        );
        let rebatch = Arc::new(RebatchExec::new(
            inner_proj,
            1000,
            None,
            "postgres_sink".to_string(),
        ));
        Arc::new(
            ProjectionExec::try_new(
                vec![(col("a", &rebatch.schema()).unwrap(), "a".to_string())],
                rebatch,
            )
            .unwrap(),
        )
    }

    #[test]
    fn projection_pushdown_must_not_elide_rebatch() {
        let plan = sandwich();
        let before = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            before.contains("RebatchExec"),
            "test setup broken:\n{before}"
        );
        // children() must expose the real input: the inner projection has to
        // be visible to plan traversals (display, optimizer rewrites). With
        // the old delegated children(), the wrapper hid its input and this
        // was 1.
        assert_eq!(
            before.matches("ProjectionExec").count(),
            2,
            "inner child hidden from traversal:\n{before}"
        );

        let optimized = ProjectionPushdown::new()
            .optimize(plan, &ConfigOptions::default())
            .expect("ProjectionPushdown failed");
        let after = displayable(optimized.as_ref()).indent(true).to_string();

        println!("=== BEFORE ===\n{before}\n=== AFTER ===\n{after}");
        assert!(
            after.contains("RebatchExec"),
            "RebatchExec was ELIDED by ProjectionPushdown under df54!\n\
             === BEFORE ===\n{before}\n=== AFTER ===\n{after}"
        );
    }
}
