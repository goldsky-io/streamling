use crate::operators::{
    broadcast, checkpointable, external_handlers, rebatch, wasm_runner, wrapping,
};
use crate::plugin;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use std::sync::Arc;

#[derive(Debug)]
pub struct StreamlingQueryPlanner {}

impl Default for StreamlingQueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamlingQueryPlanner {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl QueryPlanner for StreamlingQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(external_handlers::ExternalHandlerExtensionPlanner {}),
            Arc::new(broadcast::MultiSinkExtensionPlanner {}),
            Arc::new(checkpointable::CheckpointableExtensionPlanner {}),
            Arc::new(rebatch::RebatchExtensionPlanner {}),
            Arc::new(wrapping::WrappingExtensionPlanner {}),
            Arc::new(wasm_runner::WasmRunnerExtensionPlanner {}),
            Arc::new(plugin::operator::PluginExtensionPlanner {}),
        ]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
