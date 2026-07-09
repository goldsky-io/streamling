//! Regression check: DataFusion 54's ProjectionPushdown physical rule must not
//! delete RebatchExec from the plan.
//!
//! RebatchExec `delegate!`s `try_swapping_with_projection` to its inner plan.
//! The inner plan's swap returns a rewritten subtree that does NOT contain the
//! wrapper, so if the rule accepts it, the sink/script rebatcher silently
//! disappears and sinks receive raw per-scan batches — on Postgres that means
//! one INSERT round-trip per scan batch, which throttles high-rate pipelines
//! with no error signal.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};

use streamling_core::operators::rebatch::RebatchExec;

/// Plan shape mirroring a sink path: an outer projection above the sink-local
/// rebatcher, a transform projection below it.
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
    // children() must expose the real input: the inner projection has to be
    // visible to plan traversals (display, optimizer rewrites). With the old
    // delegated children(), the wrapper hid its input and this was 1.
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
