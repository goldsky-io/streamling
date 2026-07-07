use crate::operators::broadcast::MultiSinkExec;
use crate::operators::filter::StreamingFilterExec;
use crate::operators::projection::StreamingProjectionExec;
use crate::operators::scan_sharing::BroadcastingExec;
use crate::operators::unnest::StreamingUnnestExec;
use crate::operators::wrapping::{BackpressureRole, WrappingDataSink, WrappingExec};
use crate::telemetry::provider::get_reference_name_from_metric_key;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::unnest::UnnestExec;
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
            if let Some(original_filter) = input_plan.as_any().downcast_ref::<FilterExec>() {
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
            if let Some(original_projection) = input_plan.as_any().downcast_ref::<ProjectionExec>()
            {
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
            if let Some(original_unnest) = input_plan.as_any().downcast_ref::<UnnestExec>() {
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
/// - `DataSinkExec` (root): passes the sink's name (from its `WrappingDataSink`)
///   as the downstream for its child (the topmost transform).
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
    if node.as_any().is::<MultiSinkExec>() {
        let new_children = node
            .children()
            .into_iter()
            .map(|child| attribute_downstream(Arc::clone(child), None, true))
            .collect::<Result<Vec<_>>>()?;
        return node.with_new_children(new_children);
    }

    // WrappingExec: stamp this node's role, then recurse with this node as the
    // named downstream for its children.
    if let Some(wrapping) = node.as_any().downcast_ref::<WrappingExec>() {
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
        // Owned once so its `&str` can be lent to every child recursion below.
        let child_downstream = get_reference_name_from_metric_key(wrapping.reference_name());
        let new_children = wrapping
            .children()
            .into_iter()
            .map(|child| {
                attribute_downstream(Arc::clone(child), Some(child_downstream.as_str()), false)
            })
            .collect::<Result<Vec<_>>>()?;
        // Rebuild the node with the stamped role (only `backpressure_role`
        // changes — see WrappingExec::clone_with_role).
        let stamped: Arc<dyn ExecutionPlan> = Arc::new(wrapping.clone_with_role(role));
        return stamped.with_new_children(new_children);
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
    if let Some(broadcasting) = node.as_any().downcast_ref::<BroadcastingExec>() {
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

    // DataSinkExec (root of a sink plan): the sink is the named downstream for
    // the topmost transform. Recover its plain name from the WrappingDataSink.
    if let Some(dse) = node.as_any().downcast_ref::<DataSinkExec>() {
        let sink_downstream: Option<String> = dse
            .sink()
            .as_any()
            .downcast_ref::<WrappingDataSink>()
            .map(|wrapping_sink| get_reference_name_from_metric_key(wrapping_sink.reference_name()))
            .or_else(|| named_downstream.map(str::to_string));
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

    // Generic node (filters, projections, rebatch, ...): pass the context through
    // unchanged — it is not a named edge endpoint.
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

pub struct StreamlingPhysicalOptimizerRules {}

impl StreamlingPhysicalOptimizerRules {
    pub fn rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        vec![
            Arc::new(StreamingFilterRewritePhysicalOptimizerRule::new()),
            Arc::new(StreamingProjectionRewritePhysicalOptimizerRule::new()),
            Arc::new(StreamingUnnestRewritePhysicalOptimizerRule::new()),
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
    use crate::operators::scan_sharing::{BroadcastingExec, SharedSourceHandle};
    use crate::operators::wrapping::WrappingExec;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::sink::{DataSink, DataSinkExec};
    use datafusion::error::Result as DFResult;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
    use std::any::Any;
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
        Arc::new(CoalesceBatchesExec::new(input, 8192))
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
        fn as_any(&self) -> &dyn Any {
            self
        }
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

    fn sink(input: Arc<dyn ExecutionPlan>, metric_key_name: &str) -> Arc<dyn ExecutionPlan> {
        let wrapping_sink = WrappingDataSink::new(
            Arc::new(NoopDataSink { schema: schema() }),
            metric_key_name.to_string(),
            None,
            None,
        );
        Arc::new(DataSinkExec::new(input, Arc::new(wrapping_sink), None))
    }

    fn role_of(node: &Arc<dyn ExecutionPlan>) -> BackpressureRole {
        node.as_any()
            .downcast_ref::<WrappingExec>()
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
        let broadcasting: Arc<dyn ExecutionPlan> = Arc::new(BroadcastingExec::new(handle));
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
            .as_any()
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
        let exec = BroadcastingExec::new(handle);
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
            .as_any()
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
            .as_any()
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
}
