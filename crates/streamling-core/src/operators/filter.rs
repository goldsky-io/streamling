//! Clone of DataFusion's FilterExec, but with ability to emit empty batches, which is
//! important in the streaming context.
//! Some method delegation was added to make it easier to keep the code up to date with
//! DataFusion changes.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use itertools::Itertools;

use datafusion::physical_plan::common::can_project;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::projection::{
    EmbeddedProjection, ProjectionExec, make_with_child, try_embed_projection, update_expr,
};
use datafusion::physical_plan::{
    ColumnStatistics, DisplayAs, ExecutionPlanProperties, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan,
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
};

use crate::operators::projection::StreamingProjectionExec;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::stats::Precision;
use datafusion::common::{
    DataFusionError, Result, ScalarValue, internal_err, plan_err, project_schema,
};
use datafusion::config::ConfigOptions;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::equivalence::ProjectionMapping;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};
use datafusion::physical_expr::intervals::utils::check_support;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{
    AcrossPartitions, AnalysisContext, ConstExpr, ExprBoundaries, PhysicalExpr, analyze,
    conjunction, split_conjunction,
};
use datafusion::physical_expr_common::physical_expr::fmt_sql;
use datafusion::physical_plan::filter::FilterExec;
// Deprecated in df54 ("will be internal in the future") but no public replacement yet.
#[allow(deprecated)]
use datafusion::physical_plan::filter::collect_columns_from_predicate;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
    PushedDown,
};
use datafusion::physical_plan::metrics::MetricsSet;
use delegate::delegate;
use futures::stream::{Stream, StreamExt};
use tracing::trace;

const FILTER_EXEC_DEFAULT_SELECTIVITY: u8 = 20;

/// StreamingFilterExec evaluates a boolean predicate against all input batches to determine which rows to
/// include in its output batches.
#[derive(Debug, Clone)]
pub struct StreamingFilterExec {
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Selectivity for statistics. 0 = no rows, 100 = all rows
    default_selectivity: u8,
    /// Properties equivalence properties, partitioning, etc.
    cache: Arc<PlanProperties>,
    /// The projection indices of the columns in the output schema of join
    projection: Option<Vec<usize>>,
    /// Copy of the original FilterExec for method delegation
    original_filter: FilterExec,
    /// True when this filter belongs to an upstream source node and was
    /// re-applied above the source's `WrappingExec` (see
    /// `wrap_with_side_outputs_before_filter`). Marks a topology boundary for
    /// downstream metric aggregation so this operator's compute is not
    /// attributed to the consuming transform.
    source_owned: bool,
}

impl StreamingFilterExec {
    /// Create a FilterExec on an input
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
        original_filter: FilterExec,
    ) -> Result<Self> {
        match predicate.data_type(input.schema().as_ref())? {
            DataType::Boolean => {
                let default_selectivity = FILTER_EXEC_DEFAULT_SELECTIVITY;
                let cache =
                    Self::compute_properties(&input, &predicate, default_selectivity, None)?;
                Ok(Self {
                    predicate,
                    input: Arc::clone(&input),
                    metrics: ExecutionPlanMetricsSet::new(),
                    default_selectivity,
                    cache: Arc::new(cache),
                    projection: None,
                    original_filter,
                    source_owned: false,
                })
            }
            other => {
                plan_err!("Filter predicate must return BOOLEAN values, got {other:?}")
            }
        }
    }

    pub fn from_original(original_filter: FilterExec) -> Result<Self> {
        Ok(Self {
            predicate: original_filter.predicate().clone(),
            input: original_filter.input().clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            default_selectivity: original_filter.default_selectivity(),
            cache: original_filter.properties().clone(),
            projection: original_filter.projection().as_ref().map(|p| p.to_vec()),
            original_filter,
            source_owned: false,
        })
    }

    /// Whether this filter is owned by an upstream source node (re-applied
    /// above the source's `WrappingExec`); see the `source_owned` field.
    pub fn is_source_owned(&self) -> bool {
        self.source_owned
    }

    /// Mark this filter as owned by an upstream source node.
    pub fn mark_source_owned(&mut self) {
        self.source_owned = true;
    }

    pub fn with_default_selectivity(
        mut self,
        default_selectivity: u8,
    ) -> Result<Self, DataFusionError> {
        if default_selectivity > 100 {
            return plan_err!(
                "Default filter selectivity value needs to be less than or equal to 100"
            );
        }
        self.default_selectivity = default_selectivity;
        Ok(self)
    }

    /// Return new instance of [StreamingFilterExec] with the given projection.
    pub fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        //  Check if the projection is valid
        can_project(&self.schema(), projection.as_deref())?;

        let projection = match projection {
            Some(projection) => match &self.projection {
                Some(p) => Some(projection.iter().map(|i| p[*i]).collect()),
                None => Some(projection),
            },
            None => None,
        };

        let cache = Self::compute_properties(
            &self.input,
            &self.predicate,
            self.default_selectivity,
            projection.as_ref(),
        )?;
        Ok(Self {
            predicate: Arc::clone(&self.predicate),
            input: Arc::clone(&self.input),
            metrics: self.metrics.clone(),
            default_selectivity: self.default_selectivity,
            cache: Arc::new(cache),
            projection,
            original_filter: self.original_filter.clone(),
            source_owned: self.source_owned,
        })
    }

    /// The expression to filter on. This expression must evaluate to a boolean value.
    pub fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// The default selectivity
    pub fn default_selectivity(&self) -> u8 {
        self.default_selectivity
    }

    /// Projection
    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    /// Calculates `Statistics` for `FilterExec`, by applying selectivity (either default, or estimated) to input statistics.
    fn statistics_helper(
        schema: SchemaRef,
        input_stats: Statistics,
        predicate: &Arc<dyn PhysicalExpr>,
        default_selectivity: u8,
    ) -> Result<Statistics> {
        if !check_support(predicate, &schema) {
            let selectivity = default_selectivity as f64 / 100.0;
            let mut stats = input_stats.to_inexact();
            stats.num_rows = stats.num_rows.with_estimated_selectivity(selectivity);
            stats.total_byte_size = stats
                .total_byte_size
                .with_estimated_selectivity(selectivity);
            return Ok(stats);
        }

        let num_rows = input_stats.num_rows;
        let total_byte_size = input_stats.total_byte_size;
        let input_analysis_ctx =
            AnalysisContext::try_from_statistics(&schema, &input_stats.column_statistics)?;

        let analysis_ctx = analyze(predicate, input_analysis_ctx, &schema)?;

        // Estimate (inexact) selectivity of predicate
        let selectivity = analysis_ctx.selectivity.unwrap_or(1.0);
        let num_rows = num_rows.with_estimated_selectivity(selectivity);
        let total_byte_size = total_byte_size.with_estimated_selectivity(selectivity);

        let column_statistics =
            collect_new_statistics(&input_stats.column_statistics, analysis_ctx.boundaries);
        Ok(Statistics {
            num_rows,
            total_byte_size,
            column_statistics,
        })
    }

    fn extend_constants(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
    ) -> Vec<ConstExpr> {
        let mut res_constants = Vec::new();
        let input_eqs = input.equivalence_properties();

        let conjunctions = split_conjunction(predicate);
        for conjunction in conjunctions {
            if let Some(binary) = conjunction.downcast_ref::<BinaryExpr>()
                && binary.op() == &Operator::Eq
            {
                // Filter evaluates to single value for all partitions
                if input_eqs.is_expr_constant(binary.left()).is_some() {
                    let across = input_eqs
                        .is_expr_constant(binary.right())
                        .unwrap_or_default();
                    res_constants.push(ConstExpr::new(Arc::clone(binary.right()), across));
                } else if input_eqs.is_expr_constant(binary.right()).is_some() {
                    let across = input_eqs
                        .is_expr_constant(binary.left())
                        .unwrap_or_default();
                    res_constants.push(ConstExpr::new(Arc::clone(binary.left()), across));
                }
            }
        }
        res_constants
    }
    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        predicate: &Arc<dyn PhysicalExpr>,
        default_selectivity: u8,
        projection: Option<&Vec<usize>>,
    ) -> Result<PlanProperties> {
        // Combine the equal predicates with the input equivalence properties
        // to construct the equivalence properties:
        let stats = Self::statistics_helper(
            input.schema(),
            input.partition_statistics(None)?.as_ref().clone(),
            predicate,
            default_selectivity,
        )?;
        let mut eq_properties = input.equivalence_properties().clone();
        #[allow(deprecated)]
        let (equal_pairs, _) = collect_columns_from_predicate(predicate);
        for (lhs, rhs) in equal_pairs {
            eq_properties.add_equal_conditions(Arc::clone(lhs), Arc::clone(rhs))?
        }
        // Add the columns that have only one viable value (singleton) after
        // filtering to constants.
        let constants = collect_columns(predicate)
            .into_iter()
            .filter(|column| stats.column_statistics[column.index()].is_singleton())
            .map(|column| {
                let value = stats.column_statistics[column.index()]
                    .min_value
                    .get_value();
                let expr = Arc::new(column) as _;
                ConstExpr::new(expr, AcrossPartitions::Uniform(value.cloned()))
            });
        // This is for statistics
        eq_properties.add_constants(constants)?;
        // This is for logical constant (for example: a = '1', then a could be marked as a constant)
        // to do: how to deal with multiple situation to represent = (for example c1 between 0 and 0)
        eq_properties.add_constants(Self::extend_constants(input, predicate))?;

        let mut output_partitioning = input.output_partitioning().clone();
        // If contains projection, update the PlanProperties.
        if let Some(projection) = projection {
            let schema = eq_properties.schema();
            let projection_mapping = ProjectionMapping::from_indices(projection, schema)?;
            let out_schema = project_schema(schema, Some(projection))?;
            output_partitioning = output_partitioning.project(&projection_mapping, &eq_properties);
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        ))
    }
}

impl DisplayAs for StreamingFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_projections = if let Some(projection) = self.projection.as_ref() {
                    format!(
                        ", projection=[{}]",
                        projection
                            .iter()
                            .map(|index| format!(
                                "{}@{}",
                                self.input.schema().fields().get(*index).unwrap().name(),
                                index
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "".to_string()
                };
                write!(f, "FilterExec: {}{}", self.predicate, display_projections)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "predicate={}", fmt_sql(self.predicate.as_ref()))
            }
        }
    }
}

impl ExecutionPlan for StreamingFilterExec {
    // This helps with keeping the code up to date with DataFusion changes
    // We can delegate all methods that:
    // - don't modify the state of the struct
    // - don't return new instances of the struct
    // - don't depend on values that can change
    // - don't use FilterExecStream since we need to replace it with StreamingFilterExecStream
    // Finally, this also makes it possible to support methods that create structs without public
    // constructors, e.g. ChildFilterDescription in gather_filters_for_pushdown
    delegate! {
        to self.original_filter {
            fn maintains_input_order(&self) -> Vec<bool>;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>>;
            fn cardinality_effect(&self) -> CardinalityEffect;
            fn gather_filters_for_pushdown(&self, phase: FilterPushdownPhase,
                parent_filters: Vec<Arc<dyn PhysicalExpr>>,
                config: &ConfigOptions, ) -> Result<FilterDescription>;
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn name(&self) -> &'static str {
        "FilterExec"
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
        StreamingFilterExec::try_new(
            Arc::clone(&self.predicate),
            children.swap_remove(0),
            self.original_filter.clone(),
        )
        .and_then(|e| {
            let selectivity = e.default_selectivity();
            e.with_default_selectivity(selectivity)
        })
        .and_then(|e| e.with_projection(self.projection().cloned()))
        .map(|mut e| {
            e.source_owned = self.source_owned;
            Arc::new(e) as _
        })
    }

    /// Tries to swap `projection` with its input (`filter`). If possible, performs
    /// the swap and returns [`StreamingFilterExec`] as the top plan. Otherwise, returns `None`.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Never push a projection below a topology boundary: the swap would
        // place the downstream transform's own projection UNDER this
        // source-owned filter, excluding the transform's projection compute
        // from its metrics (and embedding would hide it inside the boundary
        // node). Mirrors the unification guard on StreamingProjectionExec.
        if self.source_owned {
            return Ok(None);
        }
        // If the projection does not narrow the schema, we should not try to push it down:
        if projection.expr().len() < projection.input().schema().fields().len() {
            // Each column in the predicate expression must exist after the projection.
            if let Some(new_predicate) = update_expr(self.predicate(), projection.expr(), false)? {
                return StreamingFilterExec::try_new(
                    new_predicate,
                    make_with_child(projection, self.input())?,
                    self.original_filter.clone(),
                )
                .and_then(|e| {
                    let selectivity = self.default_selectivity();
                    e.with_default_selectivity(selectivity)
                })
                .map(|e| Some(Arc::new(e) as _));
            }
        }
        try_embed_projection(projection, self)
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if !matches!(phase, FilterPushdownPhase::Pre) {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }
        // We absorb any parent filters that were not handled by our children
        let unsupported_parent_filters = child_pushdown_result
            .parent_filters
            .iter()
            .filter_map(|f| matches!(f.all(), PushedDown::No).then_some(Arc::clone(&f.filter)));
        let unsupported_self_filters = child_pushdown_result
            .self_filters
            .first()
            .expect("we have exactly one child")
            .iter()
            .filter_map(|f| match f.discriminant {
                PushedDown::Yes => None,
                PushedDown::No => Some(&f.predicate),
            })
            .cloned();

        let unhandled_filters = unsupported_parent_filters
            .into_iter()
            .chain(unsupported_self_filters)
            .collect_vec();

        // If we have unhandled filters, we need to create a new FilterExec
        let filter_input = Arc::clone(self.input());
        let new_predicate = conjunction(unhandled_filters);
        let updated_node = if new_predicate.eq(&lit(true)) {
            // FilterExec is no longer needed, but we may need to leave a projection in place
            match self.projection() {
                Some(projection_indices) => {
                    let filter_child_schema = filter_input.schema();
                    let proj_exprs = projection_indices
                        .iter()
                        .map(|p| {
                            let field = filter_child_schema.field(*p).clone();
                            (
                                Arc::new(Column::new(field.name(), *p)) as Arc<dyn PhysicalExpr>,
                                field.name().to_string(),
                            )
                        })
                        .collect::<Vec<_>>();
                    let mut replacement = StreamingProjectionExec::from_original(
                        ProjectionExec::try_new(proj_exprs, filter_input)?,
                    )?;
                    // The projection replaces a source-owned filter, so it
                    // inherits the topology-boundary mark.
                    if self.source_owned {
                        replacement.mark_source_owned();
                    }
                    Some(Arc::new(replacement) as Arc<dyn ExecutionPlan>)
                }
                None => {
                    // No projection needed, just return the input
                    Some(filter_input)
                }
            }
        } else if new_predicate.eq(&self.predicate) {
            // The new predicate is the same as our current predicate
            None
        } else {
            // Create a new FilterExec with the new predicate
            let new = StreamingFilterExec {
                predicate: Arc::clone(&new_predicate),
                input: Arc::clone(&filter_input),
                metrics: self.metrics.clone(),
                default_selectivity: self.default_selectivity,
                cache: Arc::new(Self::compute_properties(
                    &filter_input,
                    &new_predicate,
                    self.default_selectivity,
                    self.projection.as_ref(),
                )?),
                projection: None,
                original_filter: self.original_filter.clone(),
                source_owned: self.source_owned,
            };
            Some(Arc::new(new) as _)
        };

        Ok(FilterPushdownPropagation {
            filters: vec![PushedDown::Yes; child_pushdown_result.parent_filters.len()],
            updated_node,
        })
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start FilterExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(StreamingFilterExecStream {
            schema: self.schema(),
            predicate: Arc::clone(&self.predicate),
            input: self.input.execute(partition, context)?,
            baseline_metrics,
            projection: self.projection.clone(),
        }))
    }
}

impl EmbeddedProjection for StreamingFilterExec {
    fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        self.with_projection(projection)
    }
}

/// This function ensures that all bounds in the `ExprBoundaries` vector are
/// converted to closed bounds. If a lower/upper bound is initially open, it
/// is adjusted by using the next/previous value for its data type to convert
/// it into a closed bound.
fn collect_new_statistics(
    input_column_stats: &[ColumnStatistics],
    analysis_boundaries: Vec<ExprBoundaries>,
) -> Vec<ColumnStatistics> {
    analysis_boundaries
        .into_iter()
        .enumerate()
        .map(
            |(
                idx,
                ExprBoundaries {
                    interval,
                    distinct_count,
                    ..
                },
            )| {
                let Some(interval) = interval else {
                    // If the interval is `None`, we can say that there are no rows:
                    return ColumnStatistics {
                        null_count: Precision::Exact(0),
                        max_value: Precision::Exact(ScalarValue::Null),
                        min_value: Precision::Exact(ScalarValue::Null),
                        sum_value: Precision::Exact(ScalarValue::Null),
                        distinct_count: Precision::Exact(0),
                        byte_size: Precision::Absent,
                    };
                };
                let (lower, upper) = interval.into_bounds();
                let (min_value, max_value) = if lower.eq(&upper) {
                    (Precision::Exact(lower), Precision::Exact(upper))
                } else {
                    (Precision::Inexact(lower), Precision::Inexact(upper))
                };
                ColumnStatistics {
                    null_count: input_column_stats[idx].null_count.to_inexact(),
                    max_value,
                    min_value,
                    sum_value: Precision::Absent,
                    distinct_count: distinct_count.to_inexact(),
                    byte_size: Precision::Absent,
                }
            },
        )
        .collect()
}

/// The FilterExec streams wraps the input iterator and applies the predicate expression to
/// determine which rows to include in its output batches
struct StreamingFilterExecStream {
    /// Output schema after the projection
    schema: SchemaRef,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
    /// Runtime metrics recording
    baseline_metrics: BaselineMetrics,
    /// The projection indices of the columns in the input schema
    projection: Option<Vec<usize>>,
}

pub fn batch_filter(batch: &RecordBatch, predicate: &Arc<dyn PhysicalExpr>) -> Result<RecordBatch> {
    filter_and_project(batch, predicate, None, &batch.schema())
}

fn filter_and_project(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
    projection: Option<&Vec<usize>>,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            Ok(match (as_boolean_array(&array), projection) {
                // Apply filter array to record batch
                (Ok(filter_array), None) => {
                    let filtered = filter_record_batch(batch, filter_array)?;
                    let enriched_schema = crate::utils::arrow::build_schema_from_columns(
                        output_schema,
                        filtered.columns(),
                        batch.schema().metadata().clone(),
                    );
                    RecordBatch::try_new(enriched_schema, filtered.columns().to_vec())?
                }
                (Ok(filter_array), Some(projection)) => {
                    let projected_columns: Vec<ArrayRef> = projection
                        .iter()
                        .map(|i| Arc::clone(batch.column(*i)))
                        .collect();
                    let enriched_schema = crate::utils::arrow::build_schema_from_columns(
                        output_schema,
                        &projected_columns,
                        batch.schema().metadata().clone(),
                    );
                    let projected_batch =
                        RecordBatch::try_new(Arc::clone(&enriched_schema), projected_columns)?;
                    filter_record_batch(&projected_batch, filter_array)?
                }
                (Err(_), _) => {
                    return internal_err!("Cannot create filter_array from non-boolean predicates");
                }
            })
        })
}

impl Stream for StreamingFilterExecStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let timer = self.baseline_metrics.elapsed_compute().timer();
                let filtered_batch = filter_and_project(
                    &batch,
                    &self.predicate,
                    self.projection.as_ref(),
                    &self.schema,
                )?;
                timer.done();
                Poll::Ready(Some(Ok(filtered_batch)))
            }
            value => Poll::Ready(value),
        };
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for StreamingFilterExecStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::checkpoint_management::{
        CheckpointEpoch, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
        extract_checkpoint_messages,
    };
    use arrow::array::{BooleanArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion::physical_expr::expressions::Column;
    use std::collections::HashMap;

    fn create_test_schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]))
    }

    fn create_test_schema_with_metadata(metadata: HashMap<String, String>) -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new_with_metadata(
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

    #[test]
    fn test_batch_filter_preserves_checkpoint_metadata() {
        // Create metadata with checkpoint marker (as used in production)
        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(42),
            created_at_ms: 1000,
        }];
        let batch = create_test_batch_with_checkpoint(&checkpoint_messages);

        // Create a predicate that filters for active = true (should keep rows 0, 2, 4)
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(Column::new("active", 2));

        let result = batch_filter(&batch, &predicate).unwrap();

        // Verify the filter worked correctly
        assert_eq!(result.num_rows(), 3);

        // Verify checkpoint metadata is preserved
        let result_schema = result.schema();
        let extracted_messages = extract_checkpoint_messages(result_schema.metadata());
        assert_eq!(extracted_messages.len(), 1);
        assert!(matches!(
            &extracted_messages[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(42),
                ..
            }
        ));
    }

    #[test]
    fn test_filter_and_project_preserves_checkpoint_metadata_without_projection() {
        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(100),
            created_at_ms: 1000,
        }];
        let batch = create_test_batch_with_checkpoint(&checkpoint_messages);
        let output_schema = create_test_schema();

        // Filter for active = true
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(Column::new("active", 2));

        let result = filter_and_project(&batch, &predicate, None, &output_schema).unwrap();

        // Verify the filter worked correctly
        assert_eq!(result.num_rows(), 3);

        // Verify checkpoint metadata is preserved even though output_schema had no metadata
        let result_schema = result.schema();
        let extracted_messages = extract_checkpoint_messages(result_schema.metadata());
        assert_eq!(extracted_messages.len(), 1);
        assert!(matches!(
            &extracted_messages[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(100),
                ..
            }
        ));
    }

    #[test]
    fn test_filter_and_project_preserves_checkpoint_metadata_with_projection() {
        // Test with multiple checkpoint messages
        let checkpoint_messages = vec![
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(200),
                created_at_ms: 1000,
            },
            CheckpointMessage::Ack {
                epoch: CheckpointEpoch(199),
                sink_id: "test_sink".to_string(),
            },
        ];
        let batch = create_test_batch_with_checkpoint(&checkpoint_messages);

        // Create output schema with only id and name columns (projection removes 'active')
        let output_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Filter for active = true, project to just id and name
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(Column::new("active", 2));
        let projection = vec![0, 1]; // id and name columns

        let result =
            filter_and_project(&batch, &predicate, Some(&projection), &output_schema).unwrap();

        // Verify the filter and projection worked correctly
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema().fields().len(), 2);

        // Verify all checkpoint messages are preserved
        let result_schema = result.schema();
        let extracted_messages = extract_checkpoint_messages(result_schema.metadata());
        assert_eq!(extracted_messages.len(), 2);
        assert!(matches!(
            &extracted_messages[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(200),
                ..
            }
        ));
        assert!(matches!(
            &extracted_messages[1],
            CheckpointMessage::Ack {
                epoch: CheckpointEpoch(199),
                ..
            }
        ));
    }

    #[test]
    fn test_filter_preserves_checkpoint_metadata_when_all_rows_filtered() {
        let checkpoint_messages = vec![CheckpointMessage::Marker {
            epoch: CheckpointEpoch(300),
            created_at_ms: 1000,
        }];
        let metadata = create_checkpoint_metadata(&checkpoint_messages);

        // Create a batch where no rows match the filter
        let schema = create_test_schema_with_metadata(metadata);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    Some("charlie"),
                ])),
                Arc::new(BooleanArray::from(vec![false, false, false])), // all false
            ],
        )
        .unwrap();

        // Filter for active = true (should return empty batch)
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(Column::new("active", 2));

        let result = batch_filter(&batch, &predicate).unwrap();

        // Verify the result is empty
        assert_eq!(result.num_rows(), 0);

        // Verify checkpoint metadata is still preserved on empty batch
        let result_schema = result.schema();
        let extracted_messages = extract_checkpoint_messages(result_schema.metadata());
        assert_eq!(extracted_messages.len(), 1);
        assert!(matches!(
            &extracted_messages[0],
            CheckpointMessage::Marker {
                epoch: CheckpointEpoch(300),
                ..
            }
        ));
    }
}
