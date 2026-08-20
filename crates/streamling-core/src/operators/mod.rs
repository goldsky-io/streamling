pub mod broadcast;
pub mod checkpointable;
pub mod external_handlers;
pub mod filter;
pub mod inspect;
pub mod pg_aggregation;
pub mod planner;
pub mod projection;
pub mod rebatch;
pub mod scan_sharing;
pub mod unnest;
pub mod wasm_runner;
pub mod wrapping;

/// Operators that bound `CheckpointableExec` subtree metric aggregation.
///
/// DataFusion's `ExecutionPlan` has no local hook for this, so the walk still
/// downcasts a closed set; each type opts in here so "always bound" vs
/// source-owned lives on the operator, not as ad-hoc checks in the walk.
pub(crate) trait TopologyBoundary {
    fn bounds_metric_aggregation(&self) -> bool;
}
