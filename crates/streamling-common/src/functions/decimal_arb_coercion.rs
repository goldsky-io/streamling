//! `ExprPlanner` that auto-binds native SQL operators (`+`, `-`, `*`, `/`,
//! `%`, `=`, `!=`, `<`, `<=`, `>`, `>=`) to the `decimal_arb` ScalarUDFs
//! when at least one operand is a `streamling.decimal_arb` column.
//!
//! See `specs/001-decimal-arbitrary-precision/research.md` (R3) for the API
//! choice — confirmed by the T005 spike. Once the planner is registered
//! via `SessionContext::register_expr_planner`, an author can write:
//!
//! ```text
//! SELECT a + b FROM src;
//! SELECT * FROM src WHERE a < threshold;
//! ```
//!
//! and the rewriter substitutes the corresponding `decimal_arb_<op>` UDF
//! call. The fallback (`PlannerResult::Original`) lets DataFusion handle
//! everything else unchanged.
//!
//! Mixed-operand handling (FR-016, data-model.md E5):
//! - `decimal_arb × Decimal128(p, s)` and `decimal_arb × Decimal256(p, s)`:
//!   the planner inserts a `to_decimal_arb_from_decimal128/256` cast on the
//!   narrow side and dispatches to the matching `decimal_arb_<op>` UDF.
//! - `decimal_arb × Int*`: auto-coercion is deferred — although the
//!   `to_decimal_arb_from_int` cast UDF now exists, the planner
//!   intentionally does not insert it here yet, so the expression is left
//!   as-is and DataFusion's default planning surfaces the type-mismatch
//!   error. A follow-up will wire integer auto-coercion through this path.
//! - `decimal_arb × Float*`: per FR-013 / E5 floats are rejected — float ↔
//!   decimal is lossy and requires an explicit cast at the call site.
//!   The expression is left as-is.

use crate::functions::decimal_arb_ops::{
    DecimalArbAddFunc, DecimalArbDivFunc, DecimalArbEqFunc, DecimalArbGtFunc, DecimalArbGteFunc,
    DecimalArbLtFunc, DecimalArbLteFunc, DecimalArbModFunc, DecimalArbMulFunc, DecimalArbNeqFunc,
    DecimalArbSubFunc, ToDecimalArbFromDecimal128Func, ToDecimalArbFromDecimal256Func,
};
use crate::types::decimal_arb::DecimalArbType;
use arrow_schema::DataType;
use datafusion::common::{DFSchema, Result as DFResult};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::sqlparser::ast::BinaryOperator;
use datafusion::logical_expr::{Expr, ExprSchemable, ScalarUDF};
use std::sync::Arc;

/// `ExprPlanner` impl that rewrites binary expressions whose both operands
/// are `streamling.decimal_arb` columns into the corresponding ScalarUDF
/// call (`decimal_arb_add`, `_eq`, etc.). Other inputs pass through
/// unchanged.
#[derive(Debug)]
pub struct DecimalArbExprPlanner {
    add: Arc<ScalarUDF>,
    sub: Arc<ScalarUDF>,
    mul: Arc<ScalarUDF>,
    div: Arc<ScalarUDF>,
    rem: Arc<ScalarUDF>,
    eq: Arc<ScalarUDF>,
    neq: Arc<ScalarUDF>,
    lt: Arc<ScalarUDF>,
    lte: Arc<ScalarUDF>,
    gt: Arc<ScalarUDF>,
    gte: Arc<ScalarUDF>,
    cast_from_decimal128: Arc<ScalarUDF>,
    cast_from_decimal256: Arc<ScalarUDF>,
}

impl Default for DecimalArbExprPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbExprPlanner {
    pub fn new() -> Self {
        Self {
            add: Arc::new(ScalarUDF::from(DecimalArbAddFunc::new())),
            sub: Arc::new(ScalarUDF::from(DecimalArbSubFunc::new())),
            mul: Arc::new(ScalarUDF::from(DecimalArbMulFunc::new())),
            div: Arc::new(ScalarUDF::from(DecimalArbDivFunc::new())),
            rem: Arc::new(ScalarUDF::from(DecimalArbModFunc::new())),
            eq: Arc::new(ScalarUDF::from(DecimalArbEqFunc::new())),
            neq: Arc::new(ScalarUDF::from(DecimalArbNeqFunc::new())),
            lt: Arc::new(ScalarUDF::from(DecimalArbLtFunc::new())),
            lte: Arc::new(ScalarUDF::from(DecimalArbLteFunc::new())),
            gt: Arc::new(ScalarUDF::from(DecimalArbGtFunc::new())),
            gte: Arc::new(ScalarUDF::from(DecimalArbGteFunc::new())),
            cast_from_decimal128: Arc::new(ScalarUDF::from(ToDecimalArbFromDecimal128Func::new())),
            cast_from_decimal256: Arc::new(ScalarUDF::from(ToDecimalArbFromDecimal256Func::new())),
        }
    }

    /// Return the cast UDF that widens `dtype` into `decimal_arb`, or
    /// `None` for types we do not auto-coerce. Floats and ints
    /// intentionally produce `None` — see module docs.
    fn cast_udf_for(&self, dtype: &DataType) -> Option<Arc<ScalarUDF>> {
        match dtype {
            DataType::Decimal128(_, _) => Some(self.cast_from_decimal128.clone()),
            DataType::Decimal256(_, _) => Some(self.cast_from_decimal256.clone()),
            _ => None,
        }
    }

    /// Map a SQL `BinaryOperator` to the matching `decimal_arb_<op>` UDF.
    /// Returns `None` for operators we don't (yet) override (bitwise,
    /// string concat, etc.).
    fn udf_for(&self, op: &BinaryOperator) -> Option<&Arc<ScalarUDF>> {
        match op {
            BinaryOperator::Plus => Some(&self.add),
            BinaryOperator::Minus => Some(&self.sub),
            BinaryOperator::Multiply => Some(&self.mul),
            BinaryOperator::Divide => Some(&self.div),
            BinaryOperator::Modulo => Some(&self.rem),
            BinaryOperator::Eq => Some(&self.eq),
            BinaryOperator::NotEq => Some(&self.neq),
            BinaryOperator::Lt => Some(&self.lt),
            BinaryOperator::LtEq => Some(&self.lte),
            BinaryOperator::Gt => Some(&self.gt),
            BinaryOperator::GtEq => Some(&self.gte),
            _ => None,
        }
    }
}

impl ExprPlanner for DecimalArbExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> DFResult<PlannerResult<RawBinaryExpr>> {
        // Resolve both operands' field metadata in the input schema.
        // Bail out (Original) if either side can't be resolved — leaves
        // DataFusion's default planning untouched for non-decimal_arb cases.
        let Ok((_, left_field)) = expr.left.to_field(schema) else {
            return Ok(PlannerResult::Original(expr));
        };
        let Ok((_, right_field)) = expr.right.to_field(schema) else {
            return Ok(PlannerResult::Original(expr));
        };

        let left_is_arb = DecimalArbType::is_decimal_arb_field(left_field.as_ref());
        let right_is_arb = DecimalArbType::is_decimal_arb_field(right_field.as_ref());

        // No decimal_arb on either side — let DataFusion plan as usual.
        if !left_is_arb && !right_is_arb {
            return Ok(PlannerResult::Original(expr));
        }

        let Some(udf) = self.udf_for(&expr.op) else {
            return Ok(PlannerResult::Original(expr));
        };

        // Both operands are decimal_arb: dispatch directly.
        if left_is_arb && right_is_arb {
            let call = Expr::ScalarFunction(ScalarFunction {
                func: udf.clone(),
                args: vec![expr.left, expr.right],
            });
            return Ok(PlannerResult::Planned(call));
        }

        // Mixed-operand path: exactly one side is decimal_arb. Decide
        // whether we can auto-coerce the narrow side BEFORE consuming
        // `expr` — if not, fall back to `Original` so DataFusion's default
        // planner surfaces a clear type-mismatch error (or a follow-up
        // commit picks up Int* / Float* support).
        let narrow_dtype = if left_is_arb {
            right_field.data_type()
        } else {
            left_field.data_type()
        };
        let Some(cast_udf) = self.cast_udf_for(narrow_dtype) else {
            return Ok(PlannerResult::Original(expr));
        };

        let RawBinaryExpr { op: _, left, right } = expr;
        let (coerced_left, coerced_right) = if left_is_arb {
            (
                left,
                Expr::ScalarFunction(ScalarFunction {
                    func: cast_udf,
                    args: vec![right],
                }),
            )
        } else {
            (
                Expr::ScalarFunction(ScalarFunction {
                    func: cast_udf,
                    args: vec![left],
                }),
                right,
            )
        };

        let call = Expr::ScalarFunction(ScalarFunction {
            func: udf.clone(),
            args: vec![coerced_left, coerced_right],
        });
        Ok(PlannerResult::Planned(call))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbValue};
    use arrow::array::{Array, LargeBinaryArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::execution::FunctionRegistry;
    use datafusion::prelude::SessionContext;
    use std::str::FromStr;

    async fn make_session_with_decimal_arb_table() -> SessionContext {
        let a_field = DecimalArbType::field("a", 30, 4, true).unwrap();
        let b_field = DecimalArbType::field("b", 30, 4, true).unwrap();
        let schema = Arc::new(Schema::new(vec![a_field, b_field]));

        let mut a_builder = DecimalArbArrayBuilder::with_capacity(3, "a", 30, 4).unwrap();
        a_builder.append_str("12.5").unwrap();
        a_builder.append_str("100").unwrap();
        a_builder.append_str("-3").unwrap();
        let (a_arr, _, _) = a_builder.finish().into_inner();

        let mut b_builder = DecimalArbArrayBuilder::with_capacity(3, "b", 30, 4).unwrap();
        b_builder.append_str("0.25").unwrap();
        b_builder.append_str("100").unwrap();
        b_builder.append_str("7").unwrap();
        let (b_arr, _, _) = b_builder.finish().into_inner();

        let batch = RecordBatch::try_new(schema, vec![Arc::new(a_arr), Arc::new(b_arr)]).unwrap();
        let mut ctx = SessionContext::new();
        ctx.register_batch("t", batch).unwrap();
        ctx.register_expr_planner(Arc::new(DecimalArbExprPlanner::new()))
            .unwrap();
        ctx
    }

    fn decode_row_at_scale(
        batch: &RecordBatch,
        col: usize,
        row: usize,
        scale: u32,
    ) -> Option<DecimalArbValue> {
        let lba = batch
            .column(col)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("expected decimal_arb LargeBinary column");
        if lba.is_null(row) {
            None
        } else {
            Some(DecimalArbValue::from_canonical_bytes_at_scale(lba.value(row), scale).unwrap())
        }
    }

    #[tokio::test]
    async fn native_plus_dispatches_to_decimal_arb_add() {
        let ctx = make_session_with_decimal_arb_table().await;
        let df = ctx.sql("SELECT a + b AS sum FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        // add output rule: max(p1-s1, p2-s2) + max(s1, s2) + 1 = 27 + 4 + 1 = 32, scale = 4.
        // Decode at scale 4.
        let v0 = decode_row_at_scale(batch, 0, 0, 4).unwrap();
        let v1 = decode_row_at_scale(batch, 0, 1, 4).unwrap();
        let v2 = decode_row_at_scale(batch, 0, 2, 4).unwrap();
        assert_eq!(v0, DecimalArbValue::from_str("12.75").unwrap());
        assert_eq!(v1, DecimalArbValue::from_str("200").unwrap());
        assert_eq!(v2, DecimalArbValue::from_str("4").unwrap());
    }

    #[tokio::test]
    async fn native_minus_dispatches_to_decimal_arb_sub() {
        let ctx = make_session_with_decimal_arb_table().await;
        let df = ctx.sql("SELECT a - b AS diff FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        let v0 = decode_row_at_scale(batch, 0, 0, 4).unwrap();
        assert_eq!(v0, DecimalArbValue::from_str("12.25").unwrap());
    }

    #[tokio::test]
    async fn native_multiply_dispatches_to_decimal_arb_mul() {
        let ctx = make_session_with_decimal_arb_table().await;
        let df = ctx.sql("SELECT a * b AS prod FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        // mul output scale = s1 + s2 = 8.
        let v0 = decode_row_at_scale(batch, 0, 0, 8).unwrap();
        let v1 = decode_row_at_scale(batch, 0, 1, 8).unwrap();
        assert_eq!(v0, DecimalArbValue::from_str("3.125").unwrap());
        assert_eq!(v1, DecimalArbValue::from_str("10000").unwrap());
    }

    #[tokio::test]
    async fn native_divide_uses_default_div_scale() {
        let ctx = make_session_with_decimal_arb_table().await;
        let df = ctx.sql("SELECT a / b AS quotient FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        // div output scale = max(s1, 18) = 18.
        let v0 = decode_row_at_scale(batch, 0, 0, 18).unwrap();
        let v1 = decode_row_at_scale(batch, 0, 1, 18).unwrap();
        assert_eq!(v0, DecimalArbValue::from_str("50").unwrap());
        assert_eq!(v1, DecimalArbValue::from_str("1").unwrap());
    }

    #[tokio::test]
    async fn native_comparison_dispatches_to_decimal_arb_eq() {
        let ctx = make_session_with_decimal_arb_table().await;
        let df = ctx.sql("SELECT a = b AS eq FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        let bool_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(!bool_arr.value(0)); // 12.5 != 0.25
        assert!(bool_arr.value(1)); // 100 == 100
        assert!(!bool_arr.value(2));
    }

    #[tokio::test]
    async fn native_lt_filter_works_with_decimal_arb() {
        let ctx = make_session_with_decimal_arb_table().await;
        let df = ctx.sql("SELECT a, b FROM t WHERE a < b").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        // Only row 0 (12.5 < 0.25 is FALSE) and row 2 (-3 < 7 is TRUE) — wait,
        // 12.5 > 0.25, so row 0 fails. 100 == 100, row 1 fails. -3 < 7, row 2 keeps.
        assert_eq!(batch.num_rows(), 1);
        let v = decode_row_at_scale(batch, 0, 0, 4).unwrap();
        assert_eq!(v, DecimalArbValue::from_str("-3").unwrap());
    }

    #[tokio::test]
    async fn non_decimal_arb_columns_pass_through_unchanged() {
        // Confirm the planner is non-invasive: an Int64 + Int64 expression
        // continues to work via the built-in path.
        let ctx = SessionContext::new();
        let mut ctx = ctx;
        ctx.register_expr_planner(Arc::new(DecimalArbExprPlanner::new()))
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("x", arrow::datatypes::DataType::Int64, false),
            arrow_schema::Field::new("y", arrow::datatypes::DataType::Int64, false),
        ]));
        let x: arrow::array::ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![1_i64, 2, 3]));
        let y: arrow::array::ArrayRef =
            Arc::new(arrow::array::Int64Array::from(vec![10_i64, 20, 30]));
        let batch = RecordBatch::try_new(schema, vec![x, y]).unwrap();
        ctx.register_batch("nums", batch).unwrap();
        let df = ctx.sql("SELECT x + y AS s FROM nums").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        let int_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Int64 + Int64 must remain Int64");
        assert_eq!(int_arr.value(0), 11);
        assert_eq!(int_arr.value(1), 22);
        assert_eq!(int_arr.value(2), 33);
    }

    // ---------- Mixed-operand coercion (FR-016) ----------

    /// Build a session that exposes:
    /// - `t.a` : decimal_arb(30, 4) with values {12.5, 100, -3}
    /// - `t.d128` : Decimal128(20, 4) with values {0.25, 100, 7}
    /// - `t.d256` : Decimal256(50, 4) with values {0.25, 100, 7}
    /// - `t.i64` : Int64 with values {1, 2, 3}
    async fn make_session_with_mixed_columns() -> SessionContext {
        let a_field = DecimalArbType::field("a", 30, 4, true).unwrap();
        let d128_field =
            arrow_schema::Field::new("d128", arrow::datatypes::DataType::Decimal128(20, 4), true);
        let d256_field =
            arrow_schema::Field::new("d256", arrow::datatypes::DataType::Decimal256(50, 4), true);
        let i64_field = arrow_schema::Field::new("i64", arrow::datatypes::DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![
            a_field, d128_field, d256_field, i64_field,
        ]));

        let mut a_builder = DecimalArbArrayBuilder::with_capacity(3, "a", 30, 4).unwrap();
        a_builder.append_str("12.5").unwrap();
        a_builder.append_str("100").unwrap();
        a_builder.append_str("-3").unwrap();
        let (a_arr, _, _) = a_builder.finish().into_inner();

        // Decimal128(20, 4): 0.25 -> 2500, 100 -> 1_000_000, 7 -> 70_000.
        let d128 = arrow::array::Decimal128Array::from(vec![
            Some(2_500_i128),
            Some(1_000_000_i128),
            Some(70_000_i128),
        ])
        .with_precision_and_scale(20, 4)
        .unwrap();
        // Decimal256(50, 4): same logical values.
        let d256 = arrow::array::Decimal256Array::from(vec![
            Some(arrow::datatypes::i256::from_i128(2_500_i128)),
            Some(arrow::datatypes::i256::from_i128(1_000_000_i128)),
            Some(arrow::datatypes::i256::from_i128(70_000_i128)),
        ])
        .with_precision_and_scale(50, 4)
        .unwrap();
        let i64 = arrow::array::Int64Array::from(vec![1_i64, 2, 3]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(a_arr),
                Arc::new(d128),
                Arc::new(d256),
                Arc::new(i64),
            ],
        )
        .unwrap();
        let mut ctx = SessionContext::new();
        ctx.register_batch("t", batch).unwrap();
        ctx.register_expr_planner(Arc::new(DecimalArbExprPlanner::new()))
            .unwrap();
        ctx
    }

    #[tokio::test]
    async fn mixed_decimal_arb_plus_decimal128_dispatches() {
        // a (decimal_arb(30,4)) + d128 (Decimal128(20,4)) — RHS auto-cast.
        let ctx = make_session_with_mixed_columns().await;
        let df = ctx.sql("SELECT a + d128 AS s FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];

        // After coercion, both operands are decimal_arb. The decimal_arb_add
        // output type is widened per E5 add rule; the field carries the
        // decimal_arb extension metadata. Verify by decoding at scale 4.
        let out_field = batch.schema().field(0).clone();
        assert!(
            DecimalArbType::is_decimal_arb_field(&out_field),
            "result must be decimal_arb (got field: {:?})",
            out_field
        );
        // add output: max(p1-s1, p2-s2) + max(s1, s2) + 1
        //            = max(26, 16) + max(4, 4) + 1 = 31, scale = 4.
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&out_field),
            Some((31, 4))
        );
        let v0 = decode_row_at_scale(batch, 0, 0, 4).unwrap();
        let v1 = decode_row_at_scale(batch, 0, 1, 4).unwrap();
        let v2 = decode_row_at_scale(batch, 0, 2, 4).unwrap();
        assert_eq!(v0, DecimalArbValue::from_str("12.75").unwrap());
        assert_eq!(v1, DecimalArbValue::from_str("200").unwrap());
        assert_eq!(v2, DecimalArbValue::from_str("4").unwrap()); // -3 + 7
    }

    #[tokio::test]
    async fn mixed_decimal128_plus_decimal_arb_dispatches() {
        // Same as above but operands flipped — LHS auto-cast.
        let ctx = make_session_with_mixed_columns().await;
        let df = ctx.sql("SELECT d128 + a AS s FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        let out_field = batch.schema().field(0).clone();
        assert!(DecimalArbType::is_decimal_arb_field(&out_field));
        let v0 = decode_row_at_scale(batch, 0, 0, 4).unwrap();
        assert_eq!(v0, DecimalArbValue::from_str("12.75").unwrap());
    }

    #[tokio::test]
    async fn mixed_decimal_arb_plus_decimal256_dispatches() {
        // a (decimal_arb(30,4)) + d256 (Decimal256(50,4)) — RHS auto-cast.
        let ctx = make_session_with_mixed_columns().await;
        let df = ctx.sql("SELECT a + d256 AS s FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        let out_field = batch.schema().field(0).clone();
        assert!(
            DecimalArbType::is_decimal_arb_field(&out_field),
            "result must be decimal_arb (got field: {:?})",
            out_field
        );
        // add output: max(26, 46) + max(4, 4) + 1 = 51, scale = 4.
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&out_field),
            Some((51, 4))
        );
        let v0 = decode_row_at_scale(batch, 0, 0, 4).unwrap();
        let v1 = decode_row_at_scale(batch, 0, 1, 4).unwrap();
        assert_eq!(v0, DecimalArbValue::from_str("12.75").unwrap());
        assert_eq!(v1, DecimalArbValue::from_str("200").unwrap());
    }

    #[tokio::test]
    async fn mixed_decimal_arb_lt_decimal128_returns_boolean() {
        let ctx = make_session_with_mixed_columns().await;
        let df = ctx.sql("SELECT a < d128 AS lt FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = &batches[0];
        let bool_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("comparison must return Boolean");
        // a = {12.5, 100, -3}, d128 = {0.25, 100, 7}.
        assert!(!bool_arr.value(0)); // 12.5 < 0.25 ? no
        assert!(!bool_arr.value(1)); // 100 < 100 ? no
        assert!(bool_arr.value(2)); // -3 < 7 ? yes
    }

    #[tokio::test]
    async fn mixed_decimal_arb_plus_int64_is_deferred() {
        // Regression guard: until a `to_decimal_arb_from_int` UDF lands,
        // mixing decimal_arb with Int* MUST fall through to DataFusion's
        // default planning, which surfaces a type-mismatch error rather
        // than silently coercing through a path that doesn't exist yet.
        // This test pins that behavior so when the int path lands later,
        // it doesn't accidentally regress to a different error / silent
        // behavior — the test will need to be updated then.
        let ctx = make_session_with_mixed_columns().await;
        let result = ctx.sql("SELECT a + i64 AS s FROM t").await;
        // Either the SQL planning fails or query execution fails — both
        // are acceptable "deferred" outcomes; what we want to assert is
        // that we did NOT silently dispatch to decimal_arb_add.
        let outcome: Result<_, _> = match result {
            Ok(df) => df.collect().await.map(|_| ()),
            Err(e) => Err(e),
        };
        assert!(
            outcome.is_err(),
            "decimal_arb + Int64 should error today (deferred until a \
             to_decimal_arb_from_int cast UDF lands); got Ok(())"
        );
    }
}
