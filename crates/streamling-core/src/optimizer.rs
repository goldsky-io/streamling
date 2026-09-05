use crate::operators::broadcast::MultiSinkExec;
use crate::operators::coalesce::StreamingCoalesceExec;
use crate::operators::filter::StreamingFilterExec;
use crate::operators::parallel_sink::ParallelSinkExec;
use crate::operators::projection::StreamingProjectionExec;
use crate::operators::rebatch::RebatchExec;
use crate::operators::scan_sharing::BroadcastingExec;
use crate::operators::unnest::StreamingUnnestExec;
use crate::operators::wrapping::{BackpressureRole, WrappingDataSink, WrappingExec};
use crate::session::StreamlingConfig;
use crate::telemetry::provider::get_reference_name_from_metric_key;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
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

/// Stamps each node with the downstream consumer it feeds, so
/// `node_wait{state="blocked"}` is attributed per edge (`id=<producer>,
/// downstream_id=<consumer>`).
///
/// A manual top-down recursion (not `transform_up`/`transform_down`) because it
/// carries the *nearest enclosing named downstream* down the tree, which
/// stateless TreeNode passes can't express. Data flows leaves->root, so a node's
/// downstream is its parent. On descent:
///
/// - `DataSinkExec` / `ParallelSinkExec` (root): passes the sink's name (from
///   its `WrappingDataSink`) as the downstream for its child (the topmost
///   transform). Streamling connectors emit `ParallelSinkExec`; `DataSinkExec`
///   remains for MemTable / non-streamling sinks.
/// - `WrappingExec`: stamps `Edge(named_downstream)` (or `Unattributed`), then
///   becomes the named downstream for its children.
/// - `MultiSinkExec`: marks its producer `WrappingExec` as `FanOutProducer`
///   (the `BroadcastStream` emits per-sink edges instead) and continues below.
/// - `BroadcastingExec` (scan-sharing leaf): stamps `downstream_id` with the
///   consumer it feeds.
///
/// Scan-shared producer `WrappingExec`s are stashed in `SharedSourceHandle`
/// before this rule runs (unreachable here) and suppressed at construction.
#[derive(Clone, Debug, Default)]
pub struct DownstreamAttributionRule {}

impl DownstreamAttributionRule {
    /// Create a new `DownstreamAttributionRule`.
    pub fn new() -> Self {
        Self {}
    }
}

/// Recurse top-down carrying the nearest named downstream (plain name) and
/// whether the next `WrappingExec` should be suppressed (multi-sink producer).
///
/// This is the recursion entry point driven by
/// `DownstreamAttributionRule::optimize`; see `DownstreamAttributionRule` for
/// why a manual top-down walk is used instead of DataFusion's stateless
/// TreeNode passes.
///
/// Returns DataFusion's `Result` (not `StreamlingError`) by design: this is a
/// private helper for the `PhysicalOptimizerRule` impl, whose trait signature
/// fixes the error type, and every fallible call here (`with_new_children`) is
/// a DataFusion API. Errors feed the optimizer, never pipeline-level callers.
fn attribute_downstream(
    node: Arc<dyn ExecutionPlan>,
    named_downstream: Option<&str>,
    suppress_next_wrapping: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    // MultiSinkExec boundary: the single `input` is the fan-out producer.
    // Suppress its WrappingExec (the BroadcastStream emits per-sink edges) and
    // clear the named downstream below (the producer names its own children).
    if node.downcast_ref::<MultiSinkExec>().is_some() {
        let new_children = node
            .children()
            .into_iter()
            .map(|child| attribute_downstream(Arc::clone(child), None, true))
            .collect::<Result<Vec<_>>>()?;
        return node.with_new_children(new_children);
    }

    // WrappingExec: stamp this node's role, then recurse with this node as the
    // named downstream for its children.
    if let Some(wrapping) = node.downcast_ref::<WrappingExec>() {
        let role = if suppress_next_wrapping
            || matches!(
                wrapping.backpressure_role(),
                BackpressureRole::FanOutProducer
            ) {
            // Preserve construction-time suppression (scan sharing) and honor the
            // multi-sink boundary suppression.
            BackpressureRole::FanOutProducer
        } else if let Some(downstream) = named_downstream {
            BackpressureRole::Edge(downstream.to_string())
        } else {
            BackpressureRole::Unattributed
        };
        let child_downstream = get_reference_name_from_metric_key(wrapping.reference_name());
        // Recurse into the *immediate* `inner`, not `children()`. `WrappingExec`'s
        // `children()` is see-through (delegates to `inner`), so using it would
        // skip an adjacent `WrappingExec` — e.g. an elided-identity transform
        // sitting directly on its source, where `W(transform)` wraps `W(source)`
        // with no compute node between. Descending into `inner` visits and stamps
        // that source instead of jumping past it to its grandchildren.
        let attributed_inner = attribute_downstream(
            Arc::clone(wrapping.inner()),
            Some(child_downstream.as_str()),
            false,
        )?;
        return Ok(Arc::new(
            wrapping.clone_with_role_and_inner(role, attributed_inner),
        ));
    }

    // BroadcastingExec: scan-sharing leaf. Attribute to the immediate consumer.
    //
    // Each branch's sub-plan (`WrappingExec(<transform>) -> ... ->
    // BroadcastingExec`) is optimized before being embedded in the sink's plan,
    // so this leaf is already stamped with its immediate consumer (the
    // transform). Don't overwrite that with the terminal sink's name (the
    // transform isn't inlined, so `named_downstream` here is the sink). Only fall
    // back to `named_downstream` when unstamped — e.g. a sink reading the shared
    // source directly, no transform between.
    if let Some(broadcasting) = node.downcast_ref::<BroadcastingExec>() {
        if broadcasting.downstream_id().is_some() {
            return Ok(Arc::clone(&node));
        }
        return match named_downstream {
            Some(downstream) => Ok(Arc::new(
                broadcasting.with_downstream_id(downstream.to_string()),
            )),
            None => Ok(Arc::clone(&node)),
        };
    }

    // RebatchExec: a sink-local wrapper inserted above a sink's feeding transform
    // when the sink sets `batch_size` (single-sink path). It is not itself an
    // edge endpoint, so the named downstream passes through unchanged.
    if let Some(rebatch) = node.downcast_ref::<RebatchExec>() {
        let attributed_inner = attribute_downstream(
            Arc::clone(rebatch.inner()),
            named_downstream,
            suppress_next_wrapping,
        )?;
        return Ok(Arc::new(rebatch.clone_with_inner(attributed_inner)));
    }

    // DataSinkExec / ParallelSinkExec (root of a sink plan): the sink is the
    // named downstream for the topmost transform. Recover its plain name from
    // the WrappingDataSink. ParallelSinkExec is what streamling connectors
    // emit; DataSinkExec remains for MemTable / non-streamling sinks.
    // Name is extracted before the recurse so the downcast borrow ends before
    // `node` is moved into `with_new_children`.
    let sink_downstream = node
        .downcast_ref::<DataSinkExec>()
        .map(|dse| named_downstream_from_wrapping_sink(dse.sink(), named_downstream))
        .or_else(|| {
            node.downcast_ref::<ParallelSinkExec>()
                .map(|pse| named_downstream_from_wrapping_sink(pse.sink(), named_downstream))
        });
    if let Some(sink_downstream) = sink_downstream {
        let new_children = node
            .children()
            .into_iter()
            .map(|child| {
                attribute_downstream(
                    Arc::clone(child),
                    sink_downstream.as_deref(),
                    suppress_next_wrapping,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        return node.with_new_children(new_children);
    }

    // Generic node (filters, projections, coalesce, ...): pass the context
    // through unchanged — it is not a named edge endpoint.
    if node.children().is_empty() {
        return Ok(node);
    }
    let new_children = node
        .children()
        .into_iter()
        .map(|child| {
            attribute_downstream(Arc::clone(child), named_downstream, suppress_next_wrapping)
        })
        .collect::<Result<Vec<_>>>()?;
    node.with_new_children(new_children)
}

/// Recover the sink's plain name from a `WrappingDataSink`, falling back to the
/// inherited `named_downstream` (same fallback as the pre-#96 `DataSinkExec`
/// branch). Shared by `DataSinkExec` and `ParallelSinkExec` roots.
fn named_downstream_from_wrapping_sink(
    sink: &dyn DataSink,
    named_downstream: Option<&str>,
) -> Option<String> {
    sink.downcast_ref::<WrappingDataSink>()
        .map(|wrapping_sink| get_reference_name_from_metric_key(wrapping_sink.reference_name()))
        .or_else(|| named_downstream.map(str::to_string))
}

/// Attribute the edges inside a scan-shared producer's stashed `base_exec`.
///
/// When a node has multiple consumers, scan sharing stashes the producer's whole
/// sub-plan (`WrappingExec(producer) -> ... -> WrappingExec(source)`) inside a
/// `SharedSourceHandle` *before* `DownstreamAttributionRule` runs, so the
/// main-plan pass never reaches it. Without this, the upstream edges (e.g.
/// `source -> producer`) emit an untagged `blocked` series (no `downstream_id`).
///
/// Run the same top-down attribution over `base_exec` at construction: the
/// producer's own `WrappingExec` is already suppressed (`FanOutProducer`) — its
/// per-consumer edges are emitted by the `BroadcastStream` — so it is preserved,
/// while every upstream `WrappingExec` gets an `Edge(<nearest named downstream>)`
/// stamp (the producer for the immediate source, and so on up the chain).
/// DataFusion plan-rewrite errors propagate as scan/plan construction failures.
pub(crate) fn attribute_scan_shared_producer_base_exec(
    base_exec: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    attribute_downstream(base_exec, None, false)
}

impl PhysicalOptimizerRule for DownstreamAttributionRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        attribute_downstream(plan, None, false)
    }

    fn name(&self) -> &str {
        "DownstreamAttribution"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Enforces declared `Distribution::SinglePartition` input requirements.
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
/// `Distribution::HashPartitioned` is not handled: only joins and aggregations
/// declare it, and transform validation accepts neither. Keyed placement for
/// sinks is planned explicitly instead, by `RepartitionNode`.
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
            let requirements = input_plan.required_input_distribution();
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
            // Must run last: it reads the WrappingExec/BroadcastingExec/MultiSinkExec
            // nodes produced above and stamps downstream attribution onto them.
            Arc::new(DownstreamAttributionRule::new()),
        ]
    }
}

#[cfg(test)]
mod attribution_tests {
    use super::*;
    use crate::operators::broadcast::MultiSinkExec;
    use crate::operators::parallel_sink::ParallelSinkExec;
    use crate::operators::rebatch::RebatchExec;
    use crate::operators::scan_sharing::{BroadcastingExec, SharedSourceHandle};
    use crate::operators::wrapping::WrappingExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::sink::{DataSink, DataSinkExec};
    use datafusion::error::Result as DFResult;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
    use std::fmt;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn leaf() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    /// A non-transparent single-child node placed between two `WrappingExec`s so
    /// they don't fuse via `WrappingExec`'s delegated `children()`. In real plans
    /// the transform's computation plays this role.
    fn mid(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            ProjectionExec::try_new(
                vec![(col("v", &input.schema()).unwrap(), "v".to_string())],
                input,
            )
            .unwrap(),
        )
    }

    fn wrap(inner: Arc<dyn ExecutionPlan>, metric_key_name: &str) -> Arc<dyn ExecutionPlan> {
        Arc::new(WrappingExec::new(
            inner,
            metric_key_name.to_string(),
            vec![],
            vec![],
            None,
        ))
    }

    /// A single-sink `RebatchExec` (see-through `children()`, like the one the
    /// webhook/single-sink path inserts above the feeding transform).
    fn rebatch(inner: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(RebatchExec::new(inner, 8192, None, "rebatch".to_string()))
    }

    #[derive(Debug)]
    struct NoopDataSink {
        schema: SchemaRef,
    }

    impl DisplayAs for NoopDataSink {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "NoopDataSink")
        }
    }

    #[async_trait::async_trait]
    impl DataSink for NoopDataSink {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
        async fn write_all(
            &self,
            _data: SendableRecordBatchStream,
            _context: &Arc<TaskContext>,
        ) -> DFResult<u64> {
            Ok(0)
        }
    }

    fn wrapping_noop_sink(metric_key_name: &str) -> WrappingDataSink {
        WrappingDataSink::new(
            Arc::new(NoopDataSink { schema: schema() }),
            metric_key_name.to_string(),
            None,
            None,
        )
    }

    fn sink(input: Arc<dyn ExecutionPlan>, metric_key_name: &str) -> Arc<dyn ExecutionPlan> {
        Arc::new(DataSinkExec::new(
            input,
            Arc::new(wrapping_noop_sink(metric_key_name)),
            None,
        ))
    }

    /// Production single-sink root after parallel-exec (#96): every streamling
    /// `insert_into` returns `ParallelSinkExec` wrapping a `WrappingDataSink`.
    fn parallel_sink(
        input: Arc<dyn ExecutionPlan>,
        metric_key_name: &str,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(ParallelSinkExec::new(
            input,
            Arc::new(wrapping_noop_sink(metric_key_name)),
            get_reference_name_from_metric_key(metric_key_name),
        ))
    }

    fn role_of(node: &Arc<dyn ExecutionPlan>) -> BackpressureRole {
        node.downcast_ref::<WrappingExec>()
            .expect("expected a WrappingExec")
            .backpressure_role()
            .clone()
    }

    fn child(node: &Arc<dyn ExecutionPlan>, idx: usize) -> Arc<dyn ExecutionPlan> {
        Arc::clone(node.children()[idx])
    }

    fn run(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        DownstreamAttributionRule::new()
            .optimize(plan, &ConfigOptions::default())
            .unwrap()
    }

    /// A linear `source -> transform -> sink` chain stamps each `WrappingExec`
    /// with its immediate downstream's plain name (metric_key prefix stripped).
    #[test]
    fn linear_chain_gets_edge_stamps() {
        // DataSinkExec(pg_sink) <- W(sql) <- mid <- W(kafka_source) <- leaf
        let plan = sink(
            wrap(mid(wrap(leaf(), "app::kafka_source")), "app::sql"),
            "app::pg_sink",
        );
        let optimized = run(plan);

        let w_sql = child(&optimized, 0);
        assert_eq!(
            role_of(&w_sql),
            BackpressureRole::Edge("pg_sink".to_string())
        );
        let w_src = child(&w_sql, 0);
        assert_eq!(role_of(&w_src), BackpressureRole::Edge("sql".to_string()));
    }

    /// Same linear chain as `linear_chain_gets_edge_stamps`, but with the
    /// production `ParallelSinkExec` root. Without a dedicated branch the walk
    /// treats it as a generic node, so `named_downstream` stays `None` and both
    /// wrappers stay `Unattributed`.
    #[test]
    fn parallel_sink_linear_chain_gets_edge_stamps() {
        // ParallelSinkExec(pg_sink) <- W(sql) <- mid <- W(kafka_source) <- leaf
        let plan = parallel_sink(
            wrap(mid(wrap(leaf(), "app::kafka_source")), "app::sql"),
            "app::pg_sink",
        );
        let optimized = run(plan);

        let w_sql = child(&optimized, 0);
        assert_eq!(
            role_of(&w_sql),
            BackpressureRole::Edge("pg_sink".to_string())
        );
        let w_src = child(&w_sql, 0);
        assert_eq!(role_of(&w_src), BackpressureRole::Edge("sql".to_string()));
    }

    /// Regression: two `WrappingExec`s directly adjacent (no compute node between,
    /// as when an identity `SELECT` projection is elided so `W(sql)` wraps
    /// `W(source)` directly) must BOTH be stamped. `WrappingExec::children()` is
    /// see-through, so a `children()`-based walk would skip the inner `W(source)`;
    /// the rule recurses into `inner()` instead. Before that change the inner
    /// source stayed `Unattributed` (untagged) — this is the true root cause of
    /// the scan-share upstream-edge bug, which `mid()` masked in the tests above.
    #[test]
    fn adjacent_wrappers_without_compute_node_all_get_stamps() {
        // DataSinkExec(pg_sink) <- W(sql) <- W(source) <- leaf  (no `mid`)
        let plan = sink(
            wrap(wrap(leaf(), "app::source"), "app::sql"),
            "app::pg_sink",
        );
        let optimized = run(plan);

        let w_sql = child(&optimized, 0);
        assert_eq!(
            role_of(&w_sql),
            BackpressureRole::Edge("pg_sink".to_string())
        );
        // `child(&w_sql, 0)` would see-through past W(source); reach it via inner().
        let w_src = Arc::clone(
            w_sql
                .downcast_ref::<WrappingExec>()
                .expect("expected a WrappingExec")
                .inner(),
        );
        assert_eq!(role_of(&w_src), BackpressureRole::Edge("sql".to_string()));
    }

    /// A `MultiSinkExec` boundary suppresses the producer `WrappingExec` (the
    /// BroadcastStream emits its per-sink edges) while upstream linear edges keep
    /// their `Edge` stamps.
    #[test]
    fn multi_sink_suppresses_producer_keeps_upstream_edges() {
        let producer = wrap(mid(wrap(leaf(), "app::kafka_source")), "app::sql");
        // Sinks are not traversed by the rule (children() == [input]); empty is fine.
        let multi: Arc<dyn ExecutionPlan> = Arc::new(MultiSinkExec::new(
            producer,
            vec![],
            vec![],
            vec![],
            vec![],
            1,
            Some(Arc::from("app::sql")),
        ));
        let optimized = run(multi);

        let w_sql = child(&optimized, 0);
        assert_eq!(role_of(&w_sql), BackpressureRole::FanOutProducer);
        let w_src = child(&w_sql, 0);
        assert_eq!(role_of(&w_src), BackpressureRole::Edge("sql".to_string()));
    }

    /// A scan-sharing `BroadcastingExec` leaf under a consuming `WrappingExec`
    /// gets the consumer's plain name as its `downstream_id`.
    #[test]
    fn broadcasting_leaf_gets_consumer_downstream_id() {
        let handle = Arc::new(SharedSourceHandle::new(
            schema(),
            leaf(),
            1,
            1,
            Some(Arc::from("app::kafka_source")),
        ));
        let broadcasting: Arc<dyn ExecutionPlan> = Arc::new(
            BroadcastingExec::new(handle, None).expect("broadcasting plan should be valid"),
        );
        // DataSinkExec(pg_sink_a) <- W(sql_a) <- mid <- BroadcastingExec
        let plan = sink(wrap(mid(broadcasting), "app::sql_a"), "app::pg_sink_a");
        let optimized = run(plan);

        let w_sql = child(&optimized, 0);
        assert_eq!(
            role_of(&w_sql),
            BackpressureRole::Edge("pg_sink_a".to_string())
        );
        let broadcasting_out = child(&w_sql, 0);
        let broadcasting_exec = broadcasting_out
            .downcast_ref::<BroadcastingExec>()
            .expect("expected a BroadcastingExec");
        assert_eq!(broadcasting_exec.downstream_id(), Some("sql_a"));
    }

    fn broadcasting_leaf(downstream_id: Option<&str>) -> Arc<dyn ExecutionPlan> {
        let handle = Arc::new(SharedSourceHandle::new(
            schema(),
            leaf(),
            1,
            1,
            Some(Arc::from("app::kafka_source")),
        ));
        let exec = BroadcastingExec::new(handle, None).expect("broadcasting plan should be valid");
        match downstream_id {
            Some(id) => Arc::new(exec.with_downstream_id(id.to_string())),
            None => Arc::new(exec),
        }
    }

    /// A `BroadcastingExec` already stamped with its immediate consumer must NOT
    /// be overwritten with the terminal sink's name when the sink plan is later
    /// optimized. The transform isn't inlined into the sink plan, so the only
    /// named ancestor is the sink; preserving the stamp keeps the "attribute to
    /// the immediate consumer" semantics.
    #[test]
    fn broadcasting_preexisting_downstream_id_is_not_overwritten() {
        // DataSinkExec(webhook_slow) <- mid <- BroadcastingExec("slow_branch")
        // (no transform WrappingExec between the sink and the leaf).
        let plan = sink(
            mid(broadcasting_leaf(Some("slow_branch"))),
            "app::webhook_slow",
        );
        let optimized = run(plan);

        let mid_out = child(&optimized, 0);
        let broadcasting_out = child(&mid_out, 0);
        let broadcasting_exec = broadcasting_out
            .downcast_ref::<BroadcastingExec>()
            .expect("expected a BroadcastingExec");
        assert_eq!(broadcasting_exec.downstream_id(), Some("slow_branch"));
    }

    /// An unstamped `BroadcastingExec` that feeds a sink directly (a sink reading
    /// the shared source with no transform in between) is attributed to the sink
    /// — the documented fallback when there is no immediate consumer transform.
    #[test]
    fn broadcasting_direct_sink_gets_sink_downstream_id() {
        // DataSinkExec(pg_sink) <- mid <- BroadcastingExec (unstamped)
        let plan = sink(mid(broadcasting_leaf(None)), "app::pg_sink");
        let optimized = run(plan);

        let mid_out = child(&optimized, 0);
        let broadcasting_out = child(&mid_out, 0);
        let broadcasting_exec = broadcasting_out
            .downcast_ref::<BroadcastingExec>()
            .expect("expected a BroadcastingExec");
        assert_eq!(broadcasting_exec.downstream_id(), Some("pg_sink"));
    }

    /// A `WrappingExec` suppressed at construction (a scan-sharing producer) is
    /// not flipped back to `Edge` by the rule, even under a sink.
    #[test]
    fn construction_time_suppression_is_preserved() {
        let suppressed = Arc::new(
            WrappingExec::new(leaf(), "app::shared".to_string(), vec![], vec![], None)
                .suppress_backpressure(),
        ) as Arc<dyn ExecutionPlan>;
        // DataSinkExec(pg_sink) <- mid <- W(shared) [already FanOutProducer]
        let plan = sink(mid(suppressed), "app::pg_sink");
        let optimized = run(plan);

        // sink child is the (non-WrappingExec) mid; its child is W(shared).
        let mid_out = child(&optimized, 0);
        let w_shared = child(&mid_out, 0);
        assert_eq!(role_of(&w_shared), BackpressureRole::FanOutProducer);
    }

    /// With no resolvable downstream (no sink, no named ancestor), a linear
    /// `WrappingExec` stays `Unattributed` (the rare untagged fallback).
    #[test]
    fn unresolved_downstream_stays_unattributed() {
        let plan = wrap(leaf(), "app::orphan");
        let optimized = run(plan);
        assert_eq!(role_of(&optimized), BackpressureRole::Unattributed);
    }

    /// Regression (QA case B/D): a single-sink `RebatchExec` (inserted above the
    /// feeding transform when a sink sets `batch_size`) must not hide that
    /// transform from attribution. The rule recurses into `inner`, preserving the
    /// rebatch node while stamping the feeding transform with the sink name.
    #[test]
    fn rebatch_before_sink_still_attributes_feeding_transform() {
        // DataSinkExec(web_sink) <- RebatchExec <- W(sql) <- mid <- W(source) <- leaf
        let plan = sink(
            rebatch(wrap(mid(wrap(leaf(), "app::source")), "app::sql")),
            "app::web_sink",
        );
        let optimized = run(plan);

        // The sink's child is the RebatchExec; get the transform it wraps.
        let rebatch_node = child(&optimized, 0);
        let w_sql = Arc::clone(
            rebatch_node
                .downcast_ref::<RebatchExec>()
                .expect("expected a RebatchExec")
                .inner(),
        );
        // The transform feeding the sink is attributed to the sink...
        assert_eq!(
            role_of(&w_sql),
            BackpressureRole::Edge("web_sink".to_string())
        );
        // ...and its upstream source is attributed to the transform (not the sink).
        let w_src = child(&w_sql, 0);
        assert_eq!(role_of(&w_src), BackpressureRole::Edge("sql".to_string()));
    }

    /// Production e2e shape after parallel-exec: `ParallelSinkExec` ← `RebatchExec`
    /// ← W(sql) ← mid ← W(source). The rule must recover the sink name from
    /// `ParallelSinkExec`'s `WrappingDataSink` and pass it through `RebatchExec`.
    #[test]
    fn parallel_sink_rebatch_before_sink_still_attributes_feeding_transform() {
        // ParallelSinkExec(web_sink) <- RebatchExec <- W(sql) <- mid <- W(source) <- leaf
        let plan = parallel_sink(
            rebatch(wrap(mid(wrap(leaf(), "app::source")), "app::sql")),
            "app::web_sink",
        );
        let optimized = run(plan);

        let rebatch_node = child(&optimized, 0);
        let w_sql = Arc::clone(
            rebatch_node
                .downcast_ref::<RebatchExec>()
                .expect("expected a RebatchExec")
                .inner(),
        );
        assert_eq!(
            role_of(&w_sql),
            BackpressureRole::Edge("web_sink".to_string())
        );
        let w_src = child(&w_sql, 0);
        assert_eq!(role_of(&w_src), BackpressureRole::Edge("sql".to_string()));
    }

    /// Regression (latent variant of bug 2): a single-sink `RebatchExec` directly
    /// above a scan-sharing `BroadcastingExec` (a sink reading the shared source
    /// directly with `batch_size` set, no transform between) must still attribute
    /// the leaf to the sink. Before the `RebatchExec` branch, its see-through
    /// `children()` exposed the leaf's *empty* children, so the generic traversal
    /// returned early and the `BroadcastingExec` was never stamped (untagged).
    #[test]
    fn rebatch_above_broadcasting_leaf_attributes_to_sink() {
        // DataSinkExec(pg_sink) <- RebatchExec <- BroadcastingExec (unstamped)
        let plan = sink(rebatch(broadcasting_leaf(None)), "app::pg_sink");
        let optimized = run(plan);

        let rebatch_node = child(&optimized, 0);
        let broadcasting_out = Arc::clone(
            rebatch_node
                .downcast_ref::<RebatchExec>()
                .expect("expected a RebatchExec")
                .inner(),
        );
        let broadcasting_exec = broadcasting_out
            .downcast_ref::<BroadcastingExec>()
            .expect("expected a BroadcastingExec");
        assert_eq!(broadcasting_exec.downstream_id(), Some("pg_sink"));
    }

    /// Regression (QA case D): a scan-shared producer's sub-plan is stashed in the
    /// `SharedSourceHandle` before the main pass runs, so its upstream edges are
    /// attributed at construction via `attribute_scan_shared_producer_base_exec`.
    /// The producer stays suppressed (its per-consumer edges come from the
    /// `BroadcastStream`), while `source -> producer` gets an `Edge(producer)`
    /// stamp — previously the source emitted an untagged `blocked` series.
    #[test]
    fn scan_shared_producer_base_exec_attributes_upstream_edges() {
        // base_exec = W(producer)[suppressed] <- mid <- W(source) <- leaf
        let base_exec = Arc::new(
            WrappingExec::new(
                mid(wrap(leaf(), "app::source")),
                "app::producer".to_string(),
                vec![],
                vec![],
                None,
            )
            .suppress_backpressure(),
        ) as Arc<dyn ExecutionPlan>;

        let attributed = attribute_scan_shared_producer_base_exec(base_exec).unwrap();

        // The producer keeps its suppression.
        assert_eq!(role_of(&attributed), BackpressureRole::FanOutProducer);
        // `WrappingExec::children()` is see-through (delegates to `inner`), so one
        // hop past the producer reaches the upstream source `WrappingExec`.
        let w_source = child(&attributed, 0);
        assert_eq!(
            role_of(&w_source),
            BackpressureRole::Edge("producer".to_string())
        );
    }

    /// Regression (real QA case D shape): the scan-shared producer's identity
    /// projection is elided, so its stashed `base_exec` is `W(producer)[suppressed]`
    /// wrapping `W(source)` **directly** (no compute node between). The upstream
    /// `source -> producer` edge must still be tagged. This is the shape the e2e
    /// exercises; the `mid()`-separated test above did not catch it because
    /// `children()` only skips an *immediately* adjacent `WrappingExec`.
    #[test]
    fn scan_shared_producer_base_exec_attributes_fused_upstream_edge() {
        // base_exec = W(producer)[suppressed] <- W(source) <- leaf  (no `mid`)
        let base_exec = Arc::new(
            WrappingExec::new(
                wrap(leaf(), "app::source"),
                "app::producer".to_string(),
                vec![],
                vec![],
                None,
            )
            .suppress_backpressure(),
        ) as Arc<dyn ExecutionPlan>;

        let attributed = attribute_scan_shared_producer_base_exec(base_exec).unwrap();

        assert_eq!(role_of(&attributed), BackpressureRole::FanOutProducer);
        let w_source = Arc::clone(
            attributed
                .downcast_ref::<WrappingExec>()
                .expect("expected a WrappingExec")
                .inner(),
        );
        assert_eq!(
            role_of(&w_source),
            BackpressureRole::Edge("producer".to_string())
        );
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

    /// `UNION ALL` sums its children's partitions, and this rule deliberately
    /// leaves them summed: a union is just another multi-partition input now
    /// that the per-sink ack gate waits for every write stream and the sink-edge
    /// exchange keeps a key on one stream. Merging it back here would cost the
    /// branches their parallelism for nothing.
    ///
    /// Every branch's rows must still arrive.
    #[tokio::test]
    async fn multi_partition_union_stays_parallel() {
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
            0,
            "a union needs no coalesce; nothing above it required one"
        );
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            2,
            "the union's branches must stay on their own partitions"
        );

        let rows: usize = union
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        assert_eq!(rows, 6, "every union branch's rows must survive");
    }
}
