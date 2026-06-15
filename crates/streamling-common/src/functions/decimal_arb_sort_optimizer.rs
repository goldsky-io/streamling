//! `OptimizerRule` that rewrites `ORDER BY decimal_arb_col` to
//! `ORDER BY decimal_arb_to_sort_key(decimal_arb_col)` so that the
//! underlying DataFusion sort path produces correct numeric ordering
//! across signs.
//!
//! Without this rule, plain `ORDER BY decimal_arb_col` falls through to
//! DataFusion's bytewise sort over `LargeBinary`, which is wrong for
//! negatives — the canonical encoding's sign byte `0xFF` byte-wise sorts
//! after `0x00`, placing negatives after non-negatives. The
//! `decimal_arb_to_sort_key` ScalarUDF (defined in `decimal_arb_ops.rs`,
//! T046) produces a `LargeBinary` sort key whose bytewise comparison
//! reproduces numeric order. This rule wraps each Sort expression that
//! resolves to a `decimal_arb` column in a call to that UDF.
//!
//! Composes with the `DecimalArbExprPlanner` from `decimal_arb_coercion.rs`:
//! the planner handles binary-op rewriting, this rule handles sort
//! rewriting. Together they deliver FR-005's "deterministic ordering ...
//! without requiring an explicit cast or function-call wrapper at the
//! call site."

use crate::functions::decimal_arb_ops::DecimalArbSortKeyFunc;
use crate::types::decimal_arb::DecimalArbType;
use datafusion::common::tree_node::Transformed;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, ScalarUDF, Sort, SortExpr};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::sync::Arc;

/// LogicalPlan-level rule that wraps `decimal_arb` references in
/// `Sort` expressions with `decimal_arb_to_sort_key(...)`.
#[derive(Debug)]
pub struct DecimalArbSortRewriteRule {
    sort_key_udf: Arc<ScalarUDF>,
}

impl Default for DecimalArbSortRewriteRule {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbSortRewriteRule {
    pub fn new() -> Self {
        Self {
            sort_key_udf: Arc::new(ScalarUDF::from(DecimalArbSortKeyFunc::new())),
        }
    }
}

impl OptimizerRule for DecimalArbSortRewriteRule {
    fn name(&self) -> &str {
        "decimal_arb_sort_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // TopDown lets us look at each Sort node as the optimizer walks
        // the plan; non-Sort nodes pass through unchanged.
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Sort(Sort { expr, input, fetch }) = plan else {
            return Ok(Transformed::no(plan));
        };

        let input_schema = input.schema();
        let mut transformed = false;
        let mut new_exprs: Vec<SortExpr> = Vec::with_capacity(expr.len());
        for sort_expr in expr {
            let SortExpr {
                expr,
                asc,
                nulls_first,
            } = sort_expr;

            // Resolve the expression's Field in the input schema. If we
            // can't resolve, leave it untouched — let the rest of the
            // pipeline decide whether to error.
            let Ok((_, field)) = expr.to_field(input_schema.as_ref()) else {
                new_exprs.push(SortExpr {
                    expr,
                    asc,
                    nulls_first,
                });
                continue;
            };

            if !DecimalArbType::is_decimal_arb_field(field.as_ref()) {
                new_exprs.push(SortExpr {
                    expr,
                    asc,
                    nulls_first,
                });
                continue;
            }

            // Wrap in decimal_arb_to_sort_key(expr).
            let wrapped = Expr::ScalarFunction(ScalarFunction {
                func: self.sort_key_udf.clone(),
                args: vec![expr],
            });
            new_exprs.push(SortExpr {
                expr: wrapped,
                asc,
                nulls_first,
            });
            transformed = true;
        }

        if !transformed {
            return Ok(Transformed::no(LogicalPlan::Sort(Sort {
                expr: new_exprs,
                input,
                fetch,
            })));
        }
        Ok(Transformed::yes(LogicalPlan::Sort(Sort {
            expr: new_exprs,
            input,
            fetch,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbValue};
    use arrow::array::LargeBinaryArray;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use std::str::FromStr;

    async fn make_session_with_rule_and_table() -> SessionContext {
        let amount_field = DecimalArbType::field("amount", 30, 4, true).unwrap();
        let id_field = Field::new("id", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![id_field, amount_field]));

        let mut amount_builder = DecimalArbArrayBuilder::with_capacity(5, "amount", 30, 4).unwrap();
        // Mixed signs and magnitudes to exercise the sort-correctness path:
        amount_builder.append_str("100").unwrap();
        amount_builder.append_str("-100").unwrap();
        amount_builder.append_str("0").unwrap();
        amount_builder.append_str("-1").unwrap();
        amount_builder.append_str("1000").unwrap();
        let (amount_arr, _, _) = amount_builder.finish().into_inner();

        let id_arr: arrow::array::ArrayRef =
            Arc::new(arrow::array::Int64Array::from(vec![1_i64, 2, 3, 4, 5]));
        let batch = RecordBatch::try_new(schema, vec![id_arr, Arc::new(amount_arr)]).unwrap();

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rule(Arc::new(DecimalArbSortRewriteRule::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    #[tokio::test]
    async fn sort_rule_orders_negatives_before_positives() {
        let ctx = make_session_with_rule_and_table().await;
        let df = ctx
            .sql("SELECT id, amount FROM t ORDER BY amount ASC")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        // Concatenate batches' id columns into a Vec<i64> in observed order.
        let mut ids = Vec::new();
        for batch in &batches {
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            for i in 0..id_col.len() {
                ids.push(id_col.value(i));
            }
        }
        // Numeric ascending order:
        //   id=2 (-100), id=4 (-1), id=3 (0), id=1 (100), id=5 (1000)
        assert_eq!(ids, vec![2, 4, 3, 1, 5]);
    }

    #[tokio::test]
    async fn sort_rule_orders_descending_correctly() {
        let ctx = make_session_with_rule_and_table().await;
        let df = ctx
            .sql("SELECT id FROM t ORDER BY amount DESC")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let mut ids = Vec::new();
        for batch in &batches {
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            for i in 0..id_col.len() {
                ids.push(id_col.value(i));
            }
        }
        assert_eq!(ids, vec![5, 1, 3, 4, 2]); // 1000, 100, 0, -1, -100
    }

    #[tokio::test]
    async fn sort_rule_passes_non_decimal_arb_sorts_through() {
        // Sanity: ORDER BY id (Int64) must keep working unchanged.
        let ctx = make_session_with_rule_and_table().await;
        let df = ctx.sql("SELECT id FROM t ORDER BY id ASC").await.unwrap();
        let batches = df.collect().await.unwrap();
        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        // 1, 2, 3, 4, 5 in order.
        let observed: Vec<i64> = (0..id_col.len()).map(|i| id_col.value(i)).collect();
        assert_eq!(observed, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn sort_result_preserves_decimal_arb_column_metadata() {
        // The Sort produces a Projection over the rewritten input; the
        // *output* `amount` column (selected explicitly) must remain a
        // proper decimal_arb column — wrapping in decimal_arb_to_sort_key
        // is internal to the sort key, not the projected value.
        let ctx = make_session_with_rule_and_table().await;
        let df = ctx
            .sql("SELECT amount FROM t ORDER BY amount ASC")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        let amount_field = batch.schema().field(0).clone();
        assert!(
            DecimalArbType::is_decimal_arb_field(&amount_field),
            "ORDER BY rewrite must not strip decimal_arb metadata from \
             the projected column"
        );
        let lba = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        // First (smallest) value should be -100.
        let v = DecimalArbValue::from_canonical_bytes_at_scale(lba.value(0), 4).unwrap();
        assert_eq!(v, DecimalArbValue::from_str("-100").unwrap());
    }
}
