//! A [`FunctionRewrite`] that handles `decimal_arb` inside expression nodes the
//! binary-op `DecimalArbExprPlanner` (`decimal_arb_coercion.rs`) can't reach,
//! because they aren't `BinaryExpr`:
//!
//! **F1b — `BETWEEN` / `IN`** (`Expr::Between` / `Expr::InList`). DataFusion's
//! native coercion can't reconcile `LargeBinary` (decimal_arb storage) against
//! the bounds/list (e.g. `Int64` literals), so planning fails. We desugar:
//! - `x BETWEEN lo AND hi`    → `decimal_arb_gte(x, lo) AND decimal_arb_lte(x, hi)`
//! - `x NOT BETWEEN lo AND hi` → `decimal_arb_lt(x, lo) OR decimal_arb_gt(x, hi)`
//! - `x IN (a, …)`     → `decimal_arb_eq(x, a) OR …`
//! - `x NOT IN (a, …)` → `decimal_arb_neq(x, a) AND …`
//!   …coercing each bound/element to decimal_arb the same way the binary-op
//!   planner does (integer → scale-0 cast, Decimal128/256 → widening cast; floats
//!   are left alone so DataFusion surfaces the usual lossy-cast error).
//!
//! **F2 — `CASE` / `COALESCE`** over decimal_arb branches. DataFusion derives the
//! output field as bare `LargeBinary`, dropping the extension metadata, so a sink
//! treats the column as BYTEA (Postgres) or renders hex (JSON). We wrap the node
//! in `decimal_arb_with_meta(expr, p, s)` to restore the metadata — but only when
//! all (non-null) branches share a scale, since the canonical bytes carry no
//! scale of their own (mixed-scale branches are left untouched).
//!
//! **Why a `FunctionRewrite` and not an `OptimizerRule`:** `TypeCoercion` is an
//! *analyzer* pass that runs before any optimizer rule, and it fails on the
//! un-rewritten `BETWEEN`/`IN`. `FunctionRewrite`s run via `ApplyFunctionRewrites`
//! ahead of `TypeCoercion`, so we transform these nodes before coercion sees them.
//! (Recursion into sub-expressions is handled by the analyzer; `rewrite` only
//! inspects the top node.)

use crate::functions::decimal_arb_ops::{
    DecimalArbEqFunc, DecimalArbGtFunc, DecimalArbGteFunc, DecimalArbLtFunc, DecimalArbLteFunc,
    DecimalArbNeqFunc, DecimalArbToStringFunc, DecimalArbWithMetaFunc,
    ToDecimalArbFromDecimal128Func, ToDecimalArbFromDecimal256Func, ToDecimalArbFromIntFunc,
};
use crate::types::decimal_arb::DecimalArbType;
use arrow_schema::DataType;
use datafusion::common::ScalarValue;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result as DFResult};
use datafusion::logical_expr::expr::{Between, Case, Cast, InList, ScalarFunction, TryCast};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, Operator, ScalarUDF, lit};
use std::sync::Arc;

/// Precision used when coercing a 64-bit integer to decimal_arb at scale 0
/// (matches the binary-op planner). 20 digits covers any `i64`/`u64`.
const INT_COERCE_PRECISION: i64 = 20;

/// Is `dt` one of the Arrow string types a SQL `VARCHAR`/`TEXT` cast can land on?
fn is_text_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Is `expr` a bare `NULL` literal (of any type)?
fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(v, _) if v.is_null())
}

/// Rewrites `BETWEEN` / `IN` over `decimal_arb` into decimal_arb comparison UDFs.
#[derive(Debug)]
pub struct DecimalArbExprRewrite {
    eq: Arc<ScalarUDF>,
    neq: Arc<ScalarUDF>,
    lt: Arc<ScalarUDF>,
    lte: Arc<ScalarUDF>,
    gt: Arc<ScalarUDF>,
    gte: Arc<ScalarUDF>,
    cast_from_int: Arc<ScalarUDF>,
    cast_from_decimal128: Arc<ScalarUDF>,
    cast_from_decimal256: Arc<ScalarUDF>,
    with_meta: Arc<ScalarUDF>,
    to_string: Arc<ScalarUDF>,
}

impl Default for DecimalArbExprRewrite {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbExprRewrite {
    pub fn new() -> Self {
        Self {
            eq: Arc::new(ScalarUDF::from(DecimalArbEqFunc::new())),
            neq: Arc::new(ScalarUDF::from(DecimalArbNeqFunc::new())),
            lt: Arc::new(ScalarUDF::from(DecimalArbLtFunc::new())),
            lte: Arc::new(ScalarUDF::from(DecimalArbLteFunc::new())),
            gt: Arc::new(ScalarUDF::from(DecimalArbGtFunc::new())),
            gte: Arc::new(ScalarUDF::from(DecimalArbGteFunc::new())),
            cast_from_int: Arc::new(ScalarUDF::from(ToDecimalArbFromIntFunc::new())),
            cast_from_decimal128: Arc::new(ScalarUDF::from(ToDecimalArbFromDecimal128Func::new())),
            cast_from_decimal256: Arc::new(ScalarUDF::from(ToDecimalArbFromDecimal256Func::new())),
            with_meta: Arc::new(ScalarUDF::from(DecimalArbWithMetaFunc::new())),
            to_string: Arc::new(ScalarUDF::from(DecimalArbToStringFunc::new())),
        }
    }

    /// If every non-null branch resolves to `decimal_arb` and they all share the
    /// same scale, return the merged `(max precision, scale)` to stamp onto the
    /// enclosing CASE/COALESCE. Returns `None` (leave untouched) if any non-null
    /// branch is not decimal_arb, or the decimal_arb branches disagree on scale
    /// (stamping a single scale would misread the other branch's bytes).
    fn decimal_arb_branches_meta(
        &self,
        branches: &[&Expr],
        schema: &DFSchema,
    ) -> Option<(u32, u32)> {
        let mut p_max = 0u32;
        let mut scale: Option<u32> = None;
        let mut any = false;
        for b in branches {
            let (_, field) = b.to_field(schema).ok()?;
            // A null-typed branch (e.g. explicit `ELSE NULL`) doesn't constrain.
            if field.data_type() == &DataType::Null {
                continue;
            }
            let (p, s) = DecimalArbType::precision_scale_from_field(field.as_ref())?;
            any = true;
            p_max = p_max.max(p);
            match scale {
                None => scale = Some(s),
                Some(existing) if existing == s => {}
                Some(_) => return None, // mixed scales — unsafe to stamp
            }
        }
        scale.filter(|_| any).map(|s| (p_max, s))
    }

    /// Wrap `expr` in `decimal_arb_with_meta(expr, p, s)` to restore decimal_arb
    /// field metadata that CASE/COALESCE planning drops (F2).
    fn stamp_meta(&self, expr: Expr, precision: u32, scale: u32) -> Expr {
        Expr::ScalarFunction(ScalarFunction {
            func: self.with_meta.clone(),
            args: vec![expr, lit(precision as i64), lit(scale as i64)],
        })
    }

    /// Does `expr` resolve to a `decimal_arb` column in `schema`?
    fn is_decimal_arb(&self, expr: &Expr, schema: &DFSchema) -> bool {
        matches!(expr.to_field(schema), Ok((_, f)) if DecimalArbType::is_decimal_arb_field(f.as_ref()))
    }

    /// Coerce `operand` to decimal_arb based on its resolved type, mirroring the
    /// binary-op planner. Returns `None` for types we don't auto-coerce (floats,
    /// or anything unresolved) so the caller can leave the node untouched.
    fn coerce(&self, operand: Expr, schema: &DFSchema) -> Option<Expr> {
        let dtype = operand.to_field(schema).ok()?.1.data_type().clone();
        let call = |func: Arc<ScalarUDF>, args: Vec<Expr>| {
            Expr::ScalarFunction(ScalarFunction { func, args })
        };
        match dtype {
            // Already decimal_arb (LargeBinary storage): pass through.
            DataType::LargeBinary if self.is_decimal_arb(&operand, schema) => Some(operand),
            DataType::Decimal128(_, _) => {
                Some(call(self.cast_from_decimal128.clone(), vec![operand]))
            }
            DataType::Decimal256(_, _) => {
                Some(call(self.cast_from_decimal256.clone(), vec![operand]))
            }
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Some(call(
                self.cast_from_int.clone(),
                vec![operand, lit(INT_COERCE_PRECISION), lit(0_i64)],
            )),
            _ => None,
        }
    }

    /// `decimal_arb_to_string(expr)` — the canonical decimal text of a
    /// decimal_arb value.
    fn to_text(&self, expr: Expr) -> Expr {
        Expr::ScalarFunction(ScalarFunction {
            func: self.to_string.clone(),
            args: vec![expr],
        })
    }

    fn cmp(&self, udf: &Arc<ScalarUDF>, left: Expr, right: Expr) -> Expr {
        Expr::ScalarFunction(ScalarFunction {
            func: udf.clone(),
            args: vec![left, right],
        })
    }

    /// The decimal_arb UDF implementing `op`, or `None` if `op` isn't an
    /// ordering/equality comparison.
    fn cmp_udf(&self, op: Operator) -> Option<&Arc<ScalarUDF>> {
        Some(match op {
            Operator::Eq => &self.eq,
            Operator::NotEq => &self.neq,
            Operator::Lt => &self.lt,
            Operator::LtEq => &self.lte,
            Operator::Gt => &self.gt,
            Operator::GtEq => &self.gte,
            _ => return None,
        })
    }

    /// Stamp `(precision, scale)` back onto `expr` when every non-null branch in
    /// `branches` agrees; otherwise return `expr` unchanged.
    fn stamp_from_branches(&self, expr: Expr, branches: &[&Expr], schema: &DFSchema) -> Expr {
        match self.decimal_arb_branches_meta(branches, schema) {
            Some((p, s)) => self.stamp_meta(expr, p, s),
            None => expr,
        }
    }

    /// `CASE WHEN l IS NULL THEN r WHEN r IS NULL THEN l WHEN keep_left(l, r)
    /// THEN l ELSE r END` — one step of a GREATEST/LEAST fold, matching those
    /// functions' skip-nulls semantics.
    fn extreme_step(&self, keep_left: &Arc<ScalarUDF>, left: Expr, right: Expr) -> Expr {
        Expr::Case(Case {
            expr: None,
            when_then_expr: vec![
                (
                    Box::new(Expr::IsNull(Box::new(left.clone()))),
                    Box::new(right.clone()),
                ),
                (
                    Box::new(Expr::IsNull(Box::new(right.clone()))),
                    Box::new(left.clone()),
                ),
                (
                    Box::new(self.cmp(keep_left, left.clone(), right.clone())),
                    Box::new(left),
                ),
            ],
            else_expr: Some(Box::new(right)),
        })
    }

    /// Fold `args` into nested comparison CASEs. `keep_left` is `gte` for
    /// GREATEST / `array_max`, `lte` for LEAST / `array_min`.
    ///
    /// These functions pick a winner by comparing values, and DataFusion does
    /// that on the physical `LargeBinary` — where sign/magnitude bytes put every
    /// negative above every positive, and the same number at two scales has two
    /// encodings. Comparing through the decimal_arb UDFs restores numeric order.
    fn extreme_fold(
        &self,
        keep_left: &Arc<ScalarUDF>,
        args: &[Expr],
        schema: &DFSchema,
    ) -> Option<Expr> {
        let coerced = args
            .iter()
            .map(|a| self.coerce(a.clone(), schema))
            .collect::<Option<Vec<_>>>()?;
        let mut iter = coerced.into_iter();
        let first = iter.next()?;
        let folded = iter.fold(first, |acc, next| self.extreme_step(keep_left, acc, next));
        let branches: Vec<&Expr> = args.iter().collect();
        Some(self.stamp_from_branches(folded, &branches, schema))
    }

    /// Null-safe equality over decimal_arb:
    /// `(l IS NULL AND r IS NULL) OR (l IS NOT NULL AND r IS NOT NULL AND eq(l, r))`.
    fn null_safe_eq(&self, left: Expr, right: Expr) -> Expr {
        let both_null = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::IsNull(Box::new(left.clone()))),
            Operator::And,
            Box::new(Expr::IsNull(Box::new(right.clone()))),
        ));
        let both_present = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::IsNotNull(Box::new(left.clone()))),
            Operator::And,
            Box::new(Expr::IsNotNull(Box::new(right.clone()))),
        ));
        let present_and_equal = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(both_present),
            Operator::And,
            Box::new(self.cmp(&self.eq, left, right)),
        ));
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(both_null),
            Operator::Or,
            Box::new(present_and_equal),
        ))
    }

    /// Rewrite a single expression node. Only `Between`/`InList` over a
    /// decimal_arb subject are transformed; everything else passes through.
    fn rewrite_expr(&self, expr: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
        match expr {
            Expr::Between(Between {
                expr: subject,
                negated,
                low,
                high,
            }) => {
                if !self.is_decimal_arb(&subject, schema) {
                    return Ok(Transformed::no(Expr::Between(Between {
                        expr: subject,
                        negated,
                        low,
                        high,
                    })));
                }
                let (Some(low_c), Some(high_c)) = (
                    self.coerce(*low.clone(), schema),
                    self.coerce(*high.clone(), schema),
                ) else {
                    // A bound we can't coerce (e.g. a float) — leave the node
                    // so DataFusion surfaces its usual error.
                    return Ok(Transformed::no(Expr::Between(Between {
                        expr: subject,
                        negated,
                        low,
                        high,
                    })));
                };
                let rewritten = if negated {
                    // x < low OR x > high
                    Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(self.cmp(&self.lt, (*subject).clone(), low_c)),
                        Operator::Or,
                        Box::new(self.cmp(&self.gt, *subject, high_c)),
                    ))
                } else {
                    // x >= low AND x <= high
                    Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(self.cmp(&self.gte, (*subject).clone(), low_c)),
                        Operator::And,
                        Box::new(self.cmp(&self.lte, *subject, high_c)),
                    ))
                };
                Ok(Transformed::yes(rewritten))
            }
            Expr::InList(InList {
                expr: subject,
                list,
                negated,
            }) => {
                if list.is_empty() || !self.is_decimal_arb(&subject, schema) {
                    return Ok(Transformed::no(Expr::InList(InList {
                        expr: subject,
                        list,
                        negated,
                    })));
                }
                // A NULL element contributes UNKNOWN, not a comparison:
                // `x IN (a, NULL)` is `x = a OR NULL`, so it yields true when x
                // matches and NULL otherwise (and the dual for NOT IN). Bailing
                // out on the NULL — as this did — dropped the whole list back to
                // byte comparison.
                let mut coerced = Vec::with_capacity(list.len());
                let mut saw_null = false;
                for e in &list {
                    if is_null_literal(e) {
                        saw_null = true;
                        continue;
                    }
                    match self.coerce(e.clone(), schema) {
                        Some(c) => coerced.push(c),
                        None => {
                            return Ok(Transformed::no(Expr::InList(InList {
                                expr: subject,
                                list,
                                negated,
                            })));
                        }
                    }
                }
                // OR of eq (IN) / AND of neq (NOT IN).
                let (per_elem_udf, combine) = if negated {
                    (&self.neq, Operator::And)
                } else {
                    (&self.eq, Operator::Or)
                };
                let comparisons = coerced
                    .into_iter()
                    .map(|elem| self.cmp(per_elem_udf, (*subject).clone(), elem));
                let folded = comparisons.reduce(|acc, next| {
                    Expr::BinaryExpr(BinaryExpr::new(Box::new(acc), combine, Box::new(next)))
                });
                let rewritten = match (folded, saw_null) {
                    // `true OR NULL` = true, `false OR NULL` = NULL (and dually
                    // `false AND NULL` = false, `true AND NULL` = NULL).
                    (Some(folded), true) => Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(folded),
                        combine,
                        Box::new(lit(ScalarValue::Boolean(None))),
                    )),
                    (Some(folded), false) => folded,
                    // Every element was NULL: the result is UNKNOWN throughout.
                    (None, _) => lit(ScalarValue::Boolean(None)),
                };
                Ok(Transformed::yes(rewritten))
            }
            // F2: CASE whose branches are decimal_arb loses the extension
            // metadata on its output field. Re-stamp it (when all branches share
            // a scale) so downstream sinks treat it as NUMERIC(p, s), not BYTEA.
            Expr::Case(case) => {
                // A *simple* CASE (`CASE v WHEN w THEN …`) compares `v` against
                // each WHEN on the physical bytes, so `1` never matches `1.00`.
                // Desugar it into a searched CASE over decimal_arb equality.
                let mut desugared = false;
                let case = match &case.expr {
                    Some(operand) if self.is_decimal_arb(operand, schema) => {
                        let rewritten: Option<Vec<_>> = case
                            .when_then_expr
                            .iter()
                            .map(|(when, then)| {
                                let coerced = self.coerce((**when).clone(), schema)?;
                                Some((
                                    Box::new(self.cmp(&self.eq, (**operand).clone(), coerced)),
                                    then.clone(),
                                ))
                            })
                            .collect();
                        match rewritten {
                            Some(when_then_expr) => {
                                desugared = true;
                                Case {
                                    expr: None,
                                    when_then_expr,
                                    else_expr: case.else_expr.clone(),
                                }
                            }
                            None => case,
                        }
                    }
                    _ => case,
                };

                let mut branches: Vec<&Expr> = case
                    .when_then_expr
                    .iter()
                    .map(|(_, t)| t.as_ref())
                    .collect();
                if let Some(e) = &case.else_expr {
                    branches.push(e.as_ref());
                }
                match self.decimal_arb_branches_meta(&branches, schema) {
                    Some((p, s)) => Ok(Transformed::yes(self.stamp_meta(Expr::Case(case), p, s))),
                    None if desugared => Ok(Transformed::yes(Expr::Case(case))),
                    None => Ok(Transformed::no(Expr::Case(case))),
                }
            }
            // F2: builtin COALESCE over decimal_arb args, same treatment.
            Expr::ScalarFunction(sf) if sf.func.name() == "coalesce" => {
                let branches: Vec<&Expr> = sf.args.iter().collect();
                match self.decimal_arb_branches_meta(&branches, schema) {
                    Some((p, s)) => Ok(Transformed::yes(self.stamp_meta(
                        Expr::ScalarFunction(sf),
                        p,
                        s,
                    ))),
                    None => Ok(Transformed::no(Expr::ScalarFunction(sf))),
                }
            }
            // R5: `nullif(a, b)` is an equality test, so it compared bytes and
            // never matched across scales. `CASE WHEN eq(a, b) THEN NULL ELSE a END`.
            Expr::ScalarFunction(sf)
                if sf.func.name() == "nullif"
                    && sf.args.len() == 2
                    && self.is_decimal_arb(&sf.args[0], schema) =>
            {
                let Some(right) = self.coerce(sf.args[1].clone(), schema) else {
                    return Ok(Transformed::no(Expr::ScalarFunction(sf)));
                };
                let left = sf.args[0].clone();
                let case = Expr::Case(Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(self.cmp(&self.eq, left.clone(), right)),
                        Box::new(lit(ScalarValue::Null)),
                    )],
                    else_expr: Some(Box::new(left.clone())),
                });
                Ok(Transformed::yes(self.stamp_from_branches(
                    case,
                    &[&left],
                    schema,
                )))
            }
            // R5: GREATEST / LEAST and the two-argument-free `array_min` /
            // `array_max` over a literal array pick their winner by comparison.
            Expr::ScalarFunction(sf)
                if matches!(sf.func.name(), "greatest" | "least")
                    && sf.args.iter().any(|a| self.is_decimal_arb(a, schema)) =>
            {
                let keep_left = if sf.func.name() == "greatest" {
                    &self.gte
                } else {
                    &self.lte
                };
                match self.extreme_fold(keep_left, &sf.args, schema) {
                    Some(folded) => Ok(Transformed::yes(folded)),
                    None => Ok(Transformed::no(Expr::ScalarFunction(sf))),
                }
            }
            Expr::ScalarFunction(sf)
                if matches!(sf.func.name(), "array_min" | "array_max") && sf.args.len() == 1 =>
            {
                // Only the literal-array form (`array_min([a, b])`, which plans as
                // `make_array`) can be folded here; a genuine list *column* of
                // decimal_arb would need an element-wise UDF and is left alone.
                let Expr::ScalarFunction(inner) = &sf.args[0] else {
                    return Ok(Transformed::no(Expr::ScalarFunction(sf)));
                };
                if inner.func.name() != "make_array"
                    || !inner.args.iter().any(|a| self.is_decimal_arb(a, schema))
                {
                    return Ok(Transformed::no(Expr::ScalarFunction(sf)));
                }
                let keep_left = if sf.func.name() == "array_max" {
                    &self.gte
                } else {
                    &self.lte
                };
                match self.extreme_fold(keep_left, &inner.args, schema) {
                    Some(folded) => Ok(Transformed::yes(folded)),
                    None => Ok(Transformed::no(Expr::ScalarFunction(sf))),
                }
            }
            // R4/R5: a comparison the `DecimalArbExprPlanner` never saw, because
            // at SQL-planning time the operand wasn't yet known to be decimal_arb
            // — typically a CASE/COALESCE result, which only becomes decimal_arb
            // once the arms above stamp it. Left alone it compares raw bytes.
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if !self.is_decimal_arb(&left, schema) && !self.is_decimal_arb(&right, schema) {
                    return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                        left,
                        op,
                        right,
                    })));
                }
                let coerced = self
                    .coerce((*left).clone(), schema)
                    .zip(self.coerce((*right).clone(), schema));
                let Some((l, r)) = coerced else {
                    return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                        left,
                        op,
                        right,
                    })));
                };
                match op {
                    Operator::IsNotDistinctFrom => Ok(Transformed::yes(self.null_safe_eq(l, r))),
                    Operator::IsDistinctFrom => Ok(Transformed::yes(Expr::Not(Box::new(
                        self.null_safe_eq(l, r),
                    )))),
                    _ => match self.cmp_udf(op) {
                        Some(udf) => Ok(Transformed::yes(self.cmp(udf, l, r))),
                        // Arithmetic and everything else stays with the
                        // `DecimalArbExprPlanner`.
                        None => Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                            left,
                            op,
                            right,
                        }))),
                    },
                }
            }
            // R7: `CAST(x AS VARCHAR)` on decimal_arb hands Arrow the canonical
            // bytes, which render as control characters rather than the number.
            Expr::Cast(Cast { expr: inner, field })
                if is_text_type(field.data_type()) && self.is_decimal_arb(&inner, schema) =>
            {
                // Keep the cast so the column still has exactly the type the
                // query asked for (VARCHAR plans as LargeUtf8 here).
                Ok(Transformed::yes(Expr::Cast(Cast {
                    expr: Box::new(self.to_text(*inner)),
                    field,
                })))
            }
            Expr::TryCast(TryCast { expr: inner, field })
                if is_text_type(field.data_type()) && self.is_decimal_arb(&inner, schema) =>
            {
                Ok(Transformed::yes(Expr::TryCast(TryCast {
                    expr: Box::new(self.to_text(*inner)),
                    field,
                })))
            }
            other => Ok(Transformed::no(other)),
        }
    }
}

impl FunctionRewrite for DecimalArbExprRewrite {
    fn name(&self) -> &str {
        "decimal_arb_expr_rewrite"
    }

    fn rewrite(
        &self,
        expr: Expr,
        schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> DFResult<Transformed<Expr>> {
        // The analyzer recurses into sub-expressions for us and provides the
        // schema the expression resolves against; we only inspect the top node.
        self.rewrite_expr(expr, schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbValue};
    use arrow::array::{Array, Int64Array, LargeBinaryArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::{FunctionRegistry, SessionStateBuilder};
    use datafusion::prelude::SessionContext;
    use std::str::FromStr;

    /// `t(id Int64, amount decimal_arb(100, 18))` with values {5, -3, 0, 200}.
    async fn make_session() -> SessionContext {
        let id = Field::new("id", DataType::Int64, false);
        let amount = DecimalArbType::field("amount", 100, 18, false).unwrap();
        let schema = Arc::new(Schema::new(vec![id, amount]));

        let mut b = DecimalArbArrayBuilder::with_capacity(4, "amount", 100, 18).unwrap();
        b.append_str("5").unwrap();
        b.append_str("-3").unwrap();
        b.append_str("0").unwrap();
        b.append_str("200").unwrap();
        let (amount_arr, _, _) = b.finish().into_inner();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
                Arc::new(amount_arr),
            ],
        )
        .unwrap();

        let state = SessionStateBuilder::new().with_default_features().build();
        let mut ctx = SessionContext::new_with_state(state);
        ctx.register_function_rewrite(Arc::new(DecimalArbExprRewrite::new()))
            .unwrap();
        ctx.register_batch("t", batch).unwrap();
        ctx
    }

    async fn ids(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..col.len() {
                out.push(col.value(i));
            }
        }
        out.sort();
        out
    }

    #[tokio::test]
    async fn between_integer_literals_filters_correctly() {
        let ctx = make_session().await;
        // 5 and 0 are in [0, 100]; -3 below, 200 above.
        let got = ids(&ctx, "SELECT id FROM t WHERE amount BETWEEN 0 AND 100").await;
        assert_eq!(got, vec![1, 3]);
    }

    #[tokio::test]
    async fn not_between_integer_literals_filters_correctly() {
        let ctx = make_session().await;
        // Outside [0, 100]: -3 and 200.
        let got = ids(&ctx, "SELECT id FROM t WHERE amount NOT BETWEEN 0 AND 100").await;
        assert_eq!(got, vec![2, 4]);
    }

    #[tokio::test]
    async fn in_integer_literals_filters_correctly() {
        let ctx = make_session().await;
        let got = ids(&ctx, "SELECT id FROM t WHERE amount IN (5, 200)").await;
        assert_eq!(got, vec![1, 4]);
    }

    #[tokio::test]
    async fn not_in_integer_literals_filters_correctly() {
        let ctx = make_session().await;
        let got = ids(&ctx, "SELECT id FROM t WHERE amount NOT IN (5, 200)").await;
        assert_eq!(got, vec![2, 3]);
    }

    #[tokio::test]
    async fn between_in_projection_yields_boolean() {
        // BETWEEN in the SELECT list (Projection) must also rewrite and evaluate.
        let ctx = make_session().await;
        let batches = ctx
            .sql("SELECT id, amount BETWEEN 0 AND 100 AS in_range FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let flags: Vec<bool> = {
            let b = &batches[0];
            let c = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .unwrap();
            (0..c.len()).map(|i| c.value(i)).collect()
        };
        // ids 1..4 -> amounts {5, -3, 0, 200} -> {true, false, true, false}
        assert_eq!(flags, vec![true, false, true, false]);
    }

    #[tokio::test]
    async fn non_decimal_arb_between_passes_through() {
        // BETWEEN over a plain Int64 column must keep working via the builtin path.
        let ctx = make_session().await;
        let got = ids(&ctx, "SELECT id FROM t WHERE id BETWEEN 2 AND 3").await;
        assert_eq!(got, vec![2, 3]);
    }

    #[tokio::test]
    async fn between_result_round_trips_value() {
        // Sanity that the kept rows carry correct decimal values.
        let ctx = make_session().await;
        let batches = ctx
            .sql("SELECT amount FROM t WHERE amount BETWEEN 0 AND 100 ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let lba = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        let v0 = DecimalArbValue::from_canonical_bytes_at_scale(lba.value(0), 18).unwrap();
        assert_eq!(v0, DecimalArbValue::from_str("5").unwrap());
    }

    // ---------- F2: CASE / COALESCE metadata preservation ----------

    #[tokio::test]
    async fn case_over_decimal_arb_preserves_metadata() {
        let ctx = make_session().await;
        let batches = ctx
            .sql("SELECT id, CASE WHEN id > 1 THEN amount ELSE amount END AS chosen FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let field = batches[0]
            .schema()
            .field_with_name("chosen")
            .unwrap()
            .clone();
        assert!(
            DecimalArbType::is_decimal_arb_field(&field),
            "CASE over decimal_arb must retain (precision, scale) metadata (F2); got {field:?}"
        );
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&field),
            Some((100, 18))
        );
        // Value sanity: row 0 (id=1) -> amount 5.
        let lba = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        let v0 = DecimalArbValue::from_canonical_bytes_at_scale(lba.value(0), 18).unwrap();
        assert_eq!(v0, DecimalArbValue::from_str("5").unwrap());
    }

    #[tokio::test]
    async fn coalesce_over_decimal_arb_preserves_metadata() {
        let ctx = make_session().await;
        let batches = ctx
            .sql("SELECT COALESCE(amount, amount) AS c FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let field = batches[0].schema().field_with_name("c").unwrap().clone();
        assert!(
            DecimalArbType::is_decimal_arb_field(&field),
            "COALESCE over decimal_arb must retain metadata (F2); got {field:?}"
        );
    }

    #[tokio::test]
    async fn case_over_non_decimal_arb_is_untouched() {
        // CASE returning Int64 must NOT be wrapped — stays Int64.
        let ctx = make_session().await;
        let batches = ctx
            .sql("SELECT CASE WHEN id > 1 THEN id ELSE id END AS c FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let field = batches[0].schema().field_with_name("c").unwrap().clone();
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(!DecimalArbType::is_decimal_arb_field(&field));
    }
}
