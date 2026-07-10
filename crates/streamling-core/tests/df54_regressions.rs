//! Regression tests for two DataFusion 54 upgrade hazards.
//!
//! 1. `WrappingExec` (telemetry wrapper) must not be deleted by DataFusion
//!    54's hook-driven optimizer rules — same elision hazard as `RebatchExec`
//!    (see tests/rebatch_elision.rs): delegated
//!    `try_swapping_with_projection`/`with_fetch`/`repartitioned` returned
//!    inner subtrees that do not contain the wrapper.
//! 2. `ExtractLeafExpressions` (`__datafusion_extracted_N` aliasing) must be
//!    disabled: its aliases drift between logical and physical planning of
//!    user-defined extension nodes ("Extension planner for Wrapper ... created
//!    an ExecutionPlan with mismatched schema"), and its extraction columns
//!    leak into sinks (Arrow column-count mismatch, `Schema::field`
//!    out-of-bounds panic).

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};

use streamling_core::dynamic_table::DynamicTableRegistry;
use streamling_core::operators::wrapping::WrappingExec;
use streamling_core::session::SessionManager;

#[test]
fn projection_pushdown_must_not_elide_wrapping_exec() {
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
    let wrapped: Arc<dyn ExecutionPlan> = Arc::new(WrappingExec::new(
        inner_proj,
        "source_1".to_string(),
        vec![],
        vec![],
        None,
    ));
    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            vec![(col("a", &wrapped.schema()).unwrap(), "a".to_string())],
            wrapped,
        )
        .unwrap(),
    );

    let before = displayable(plan.as_ref()).indent(true).to_string();
    let optimized = ProjectionPushdown::new()
        .optimize(plan, &ConfigOptions::default())
        .expect("ProjectionPushdown failed");
    let after = displayable(optimized.as_ref()).indent(true).to_string();

    // WrappingExec displays as its inner node (delegated DisplayAs), so probe
    // the tree structurally instead of by name: the projection's child must
    // still be the wrapper (schema adjusted by WrappingExec), and the plan
    // must not have collapsed to projection-over-source.
    fn contains_wrapping(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<WrappingExec>().is_some()
            || plan.children().iter().any(|c| contains_wrapping(c))
    }
    assert!(
        contains_wrapping(&optimized),
        "WrappingExec was ELIDED by ProjectionPushdown under df54!\n\
         === BEFORE ===\n{before}\n=== AFTER ===\n{after}"
    );
}

#[test]
fn leaf_expression_extraction_is_disabled() {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap();
    let state = sm.session_state();
    assert!(
        !state
            .config_options()
            .optimizer
            .enable_leaf_expression_pushdown,
        "ExtractLeafExpressions must stay disabled: its __datafusion_extracted_N \
         aliases are not stable across re-optimization of plans containing \
         user-defined extension nodes (Wrapper/Rebatch/MultiSink), which makes \
         physical planning fail with a schema mismatch and leaks extraction \
         columns into sinks"
    );
}
