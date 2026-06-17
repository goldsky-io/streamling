use crate::utils::batch_accumulator::AsyncBatchAccumulator;
use async_trait::async_trait;
use datafusion::common::{DFSchemaRef, Result};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, OrderingRequirements};
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use delegate::delegate;
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

impl ExecutionPlan for RebatchExec {
    delegate! {
        to self.inner {
            fn name(&self) -> &str;
            fn schema(&self) -> arrow_schema::SchemaRef;
            fn properties(&self) -> &Arc<PlanProperties>;
            fn check_invariants(&self, check: InvariantLevel) -> Result<()>;
            fn required_input_distribution(&self) -> Vec<Distribution>;
            fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>>;
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn repartitioned(
                &self,
                target_partitions: usize,
                config: &ConfigOptions,
            ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
            fn metrics(&self) -> Option<MetricsSet>;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>>;
            fn supports_limit_pushdown(&self) -> bool;
            fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>>;
            fn cardinality_effect(&self) -> CardinalityEffect;
            fn try_swapping_with_projection(
                &self,
                projection: &ProjectionExec,
            ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_inner = self.inner.clone().with_new_children(children)?;
        Ok(Arc::new(RebatchExec::new(
            new_inner,
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
