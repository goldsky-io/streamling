use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr_common::physical_expr::fmt_sql;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use delegate::delegate;

use futures::stream::{Stream, StreamExt};
use tracing::log::trace;

/// Wrapper around DataFusion's ProjectionExec with streaming support
///
/// This is a clone of DataFusion's ProjectionExec, but with ability to properly
/// handle streaming context and metadata propagation. Method delegation is used to
/// keep the code up to date with DataFusion changes.
#[derive(Debug, Clone)]
pub struct StreamingProjectionExec {
    /// The projection expressions stored as tuples of (expression, output column name)
    expr: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// The schema once the projection has been applied to the input
    schema: SchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties
    cache: Arc<PlanProperties>,
    /// Copy of the original DataFusion ProjectionExec for method delegation
    original_projection: ProjectionExec,
    /// True when this projection belongs to an upstream source node and was
    /// re-applied above the source's `WrappingExec` (see
    /// `wrap_with_side_outputs_before_filter`). Marks a topology boundary for
    /// downstream metric aggregation.
    source_owned: bool,
}

impl StreamingProjectionExec {
    /// NOTE: always resets `source_owned` to `false`. A rebuild path that
    /// routes a source-owned projection through here silently drops the
    /// topology boundary mark — callers must re-apply it via
    /// [`Self::with_source_owned`] (see `wrap_with_side_outputs_before_filter`).
    pub fn from_original(original_projection: ProjectionExec) -> Result<Self> {
        Ok(Self {
            expr: original_projection
                .expr()
                .iter()
                .map(|pe| (pe.expr.clone(), pe.alias.clone()))
                .collect(),
            schema: original_projection.schema(),
            input: original_projection.input().clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            cache: original_projection.properties().clone(),
            original_projection,
            source_owned: false,
        })
    }

    /// Whether this projection is owned by an upstream source node
    /// (re-applied above the source's `WrappingExec`); see the
    /// `source_owned` field.
    ///
    /// Crate-internal: the mark is only set by
    /// `wrap_with_side_outputs_before_filter` (and tests that model that path).
    pub(crate) fn is_source_owned(&self) -> bool {
        self.source_owned
    }

    /// Consume `self`, returning it marked as owned by an upstream source
    /// node (see the `source_owned` field).
    ///
    /// Crate-internal: only `wrap_with_side_outputs_before_filter` should set
    /// this mark (tests may model that path).
    pub(crate) fn with_source_owned(mut self) -> Self {
        self.source_owned = true;
        self
    }

    /// The projection expressions
    pub fn expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.expr
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl crate::operators::TopologyBoundary for StreamingProjectionExec {
    fn bounds_metric_aggregation(&self) -> bool {
        self.is_source_owned()
    }
}

impl DisplayAs for StreamingProjectionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let expr: Vec<String> = self
                    .expr
                    .iter()
                    .map(|(e, alias)| {
                        let e = e.to_string();
                        if &e != alias {
                            format!("{e} as {alias}")
                        } else {
                            e
                        }
                    })
                    .collect();

                write!(f, "ProjectionExec: expr=[{}]", expr.join(", "))
            }
            DisplayFormatType::TreeRender => {
                for (i, (e, alias)) in self.expr().iter().enumerate() {
                    let expr_sql = fmt_sql(e.as_ref());
                    if &e.to_string() == alias {
                        writeln!(f, "expr{i}={expr_sql}")?;
                    } else {
                        writeln!(f, "{alias}={expr_sql}")?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for StreamingProjectionExec {
    // Delegate methods that can be forwarded to original_projection
    // Following the same pattern as StreamingFilterExec
    delegate! {
        to self.original_projection {
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn supports_limit_pushdown(&self) -> bool;
            fn cardinality_effect(&self) -> CardinalityEffect;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>>;
        }
    }

    fn name(&self) -> &'static str {
        "StreamingProjectionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_projection = ProjectionExec::try_new(self.expr.clone(), children.swap_remove(0))?;

        StreamingProjectionExec::from_original(new_projection).map(|mut e| {
            e.source_owned = self.source_owned;
            Arc::new(e) as _
        })
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start StreamingProjectionExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );
        Ok(Box::pin(StreamingProjectionStream {
            schema: self.schema.clone(),
            expr: self.expr.iter().map(|x| Arc::clone(&x.0)).collect(),
            input: self.input.execute(partition, context)?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Never unify across a topology boundary: merging a source-owned
        // projection with a downstream transform's projection would blend the
        // source's expression cost into the transform's metrics (and drop the
        // boundary mark). Keep the two projections stacked instead (the shared
        // fallback arm below).
        //
        // NOTE: the production session replaces DataFusion's physical
        // optimizer rules with `StreamlingPhysicalOptimizerRules` (see
        // session.rs), so ProjectionPushdown never runs there. This guard is
        // defense-in-depth for embedders and future rule-set changes.
        let maybe_unified = if self.source_owned {
            None
        } else {
            let streaming_projection = StreamingProjectionExec::from_original(projection.clone())?;
            try_unifying_projections(&streaming_projection, self)?
        };
        if let Some(new_plan) = maybe_unified {
            // To unify 3 or more sequential projections:
            remove_unnecessary_projections(new_plan).data().map(Some)
        } else {
            Ok(Some(Arc::new(StreamingProjectionExec::from_original(
                projection.clone(),
            )?)))
        }
    }
}

/// Streaming projection stream that wraps input and applies projection with metrics tracking
struct StreamingProjectionStream {
    schema: SchemaRef,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl StreamingProjectionStream {
    fn batch_project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // Records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        let arrays = self
            .expr
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        let enriched_schema = crate::utils::arrow::build_schema_from_columns(
            &self.schema,
            &arrays,
            batch.schema().metadata().clone(),
        );

        if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(enriched_schema, arrays, &options).map_err(Into::into)
        } else {
            RecordBatch::try_new(enriched_schema, arrays).map_err(Into::into)
        }
    }
}

impl Stream for StreamingProjectionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.batch_project(&batch)),
            other => other,
        });

        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for StreamingProjectionStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// This function checks if `plan` is a [`ProjectionExec`], and inspects its
/// input(s) to test whether it can push `plan` under its input(s). This function
/// will operate on the entire tree and may ultimately remove `plan` entirely
/// by leveraging source providers with built-in projection capabilities.
pub fn remove_unnecessary_projections(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let maybe_modified = if let Some(projection) = plan.downcast_ref::<StreamingProjectionExec>() {
        // If the projection does not cause any change on the input, we can
        // safely remove it:
        if is_projection_removable(projection) {
            return Ok(Transformed::yes(Arc::clone(projection.input())));
        }
        // If it does, check if we can push it under its child(ren):
        projection
            .input()
            .try_swapping_with_projection(&projection.original_projection)?
    } else {
        return Ok(Transformed::no(plan));
    };
    Ok(maybe_modified.map_or_else(|| Transformed::no(plan), Transformed::yes))
}

/// Compare the inputs and outputs of the projection. All expressions must be
/// columns without alias, and projection does not change the order of fields.
/// For example, if the input schema is `a, b`, `SELECT a, b` is removable,
/// but `SELECT b, a` and `SELECT a+1, b` and `SELECT a AS c, b` are not.
fn is_projection_removable(projection: &StreamingProjectionExec) -> bool {
    let exprs = projection.expr();
    exprs.iter().enumerate().all(|(idx, (expr, alias))| {
        let Some(col) = expr.downcast_ref::<Column>() else {
            return false;
        };
        col.name() == alias && col.index() == idx
    }) && exprs.len() == projection.input().schema().fields().len()
}

/// The function operates in two modes:
///
/// 1) When `sync_with_child` is `true`:
///
///    The function updates the indices of `expr` if the expression resides
///    in the input plan. For instance, given the expressions `a@1 + b@2`
///    and `c@0` with the input schema `c@2, a@0, b@1`, the expressions are
///    updated to `a@0 + b@1` and `c@2`.
///
/// 2) When `sync_with_child` is `false`:
///
///    The function determines how the expression would be updated if a projection
///    was placed before the plan associated with the expression. If the expression
///    cannot be rewritten after the projection, it returns `None`. For example,
///    given the expressions `c@0`, `a@1` and `b@2`, and the [`ProjectionExec`] with
///    an output schema of `a, c_new`, then `c@0` becomes `c_new@1`, `a@1` becomes
///    `a@0`, but `b@2` results in `None` since the projection does not include `b`.
pub fn update_expr(
    expr: &Arc<dyn PhysicalExpr>,
    projected_exprs: &[(Arc<dyn PhysicalExpr>, String)],
    sync_with_child: bool,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    #[derive(Debug, PartialEq)]
    enum RewriteState {
        /// The expression is unchanged.
        Unchanged,
        /// Some part of the expression has been rewritten
        RewrittenValid,
        /// Some part of the expression has been rewritten, but some column
        /// references could not be.
        RewrittenInvalid,
    }

    let mut state = RewriteState::Unchanged;

    let new_expr = Arc::clone(expr)
        .transform_up(|expr| {
            if state == RewriteState::RewrittenInvalid {
                return Ok(Transformed::no(expr));
            }

            let Some(column) = expr.downcast_ref::<Column>() else {
                return Ok(Transformed::no(expr));
            };
            if sync_with_child {
                state = RewriteState::RewrittenValid;
                // Update the index of `column`:
                Ok(Transformed::yes(Arc::clone(
                    &projected_exprs[column.index()].0,
                )))
            } else {
                // default to invalid, in case we can't find the relevant column
                state = RewriteState::RewrittenInvalid;
                // Determine how to update `column` to accommodate `projected_exprs`
                projected_exprs
                    .iter()
                    .enumerate()
                    .find_map(|(index, (projected_expr, alias))| {
                        projected_expr
                            .downcast_ref::<Column>()
                            .and_then(|projected_column| {
                                (column.name().eq(projected_column.name())
                                    && column.index() == projected_column.index())
                                .then(|| {
                                    state = RewriteState::RewrittenValid;
                                    Arc::new(Column::new(alias, index)) as _
                                })
                            })
                    })
                    .map_or_else(|| Ok(Transformed::no(expr)), |c| Ok(Transformed::yes(c)))
            }
        })
        .data();

    new_expr.map(|e| (state == RewriteState::RewrittenValid).then_some(e))
}

/// Unifies `projection` with its input (which is also a [`StreamingProjectionExec`]).
fn try_unifying_projections(
    projection: &StreamingProjectionExec,
    child: &StreamingProjectionExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let mut projected_exprs = vec![];
    let mut column_ref_map: HashMap<Column, usize> = HashMap::new();

    // Collect the column references usage in the outer projection.
    projection.expr().iter().for_each(|(expr, _)| {
        expr.apply(|expr| {
            Ok({
                if let Some(column) = expr.downcast_ref::<Column>() {
                    *column_ref_map.entry(column.clone()).or_default() += 1;
                }
                TreeNodeRecursion::Continue
            })
        })
        .unwrap();
    });
    // Merging these projections is not beneficial, e.g
    // If an expression is not trivial and it is referred more than 1, unifies projections will be
    // beneficial as caching mechanism for non-trivial computations.
    // See discussion in: https://github.com/apache/datafusion/issues/8296
    if column_ref_map.iter().any(|(column, count)| {
        *count > 1 && !is_expr_trivial(&Arc::clone(&child.expr()[column.index()].0))
    }) {
        return Ok(None);
    }
    for (expr, alias) in projection.expr() {
        // If there is no match in the input projection, we cannot unify these
        // projections. This case will arise if the projection expression contains
        // a `PhysicalExpr` variant `update_expr` doesn't support.
        let Some(expr) = update_expr(expr, child.expr(), true)? else {
            return Ok(None);
        };
        projected_exprs.push((expr, alias.clone()));
    }
    StreamingProjectionExec::from_original(ProjectionExec::try_new(
        projected_exprs,
        Arc::clone(child.input()),
    )?)
    .map(|e| Some(Arc::new(e) as _))
}

/// Checks if the given expression is trivial.
/// An expression is considered trivial if it is either a `Column` or a `Literal`.
fn is_expr_trivial(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<Column>().is_some() || expr.downcast_ref::<Literal>().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    #[test]
    fn from_original_resets_source_owned() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        let original = ProjectionExec::try_new(vec![(col, "id".to_string())], input).unwrap();
        let marked = StreamingProjectionExec::from_original(original.clone())
            .unwrap()
            .with_source_owned();
        assert!(marked.is_source_owned());
        let reset = StreamingProjectionExec::from_original(original).unwrap();
        assert!(
            !reset.is_source_owned(),
            "from_original always resets the topology-boundary mark"
        );
    }
}
