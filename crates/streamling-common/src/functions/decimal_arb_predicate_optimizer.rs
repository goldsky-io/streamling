//! A [`FunctionRewrite`] that handles `decimal_arb` inside expression nodes the
//! binary-op `DecimalArbExprPlanner` (`decimal_arb_coercion.rs`) can't reach,
//! because they aren't `BinaryExpr` — or because the operand only becomes
//! decimal_arb after this rewrite has run on a sub-expression.
//!
//! `decimal_arb` is `LargeBinary` underneath and its scale lives only in field
//! metadata, so everything DataFusion does natively on the storage type is
//! wrong in one of two ways: it compares bytes (sign/magnitude order, and the
//! same number at two scales has two encodings) or it drops the metadata
//! (constructors, accessors, aggregates), after which every downstream consumer
//! reads the column as opaque bytes. Every arm below fixes one of those two.
//!
//! **Comparisons.** `BETWEEN` / `IN` / late-bound `BinaryExpr` comparisons are
//! routed to the `decimal_arb_*` comparison UDFs; the other operand is coerced
//! the way the binary-op planner does (integer → scale-0 cast, Decimal128/256 →
//! widening cast, quoted number → exact parse). A NULL `BETWEEN` bound stays
//! UNKNOWN on that side. `IS [NOT] DISTINCT FROM`, `NULLIF` and a *simple*
//! `CASE x WHEN …` are equality tests DataFusion evaluates natively and
//! correctly on bytes — once both operands sit at the same scale. They are
//! therefore kept native with the operands re-encoded to a common scale by
//! `decimal_arb_rescale` (exact), which also evaluates each operand exactly
//! once; desugaring them into `CASE`/`AND` duplicated volatile operands.
//!
//! **Value-returning nodes.** `CASE` / `COALESCE` (`NVL`, `IFNULL`, `NVL2`) /
//! `NULLIF` lose the extension metadata (DataFusion builds the output field
//! from the bare `LargeBinary`), so a sink treated the column as BYTEA and JSON
//! rendered hex. Branches are brought to one scale and the node is wrapped in
//! `decimal_arb_with_meta(expr, p, s)`, a pure relabel. `GREATEST` / `LEAST`
//! pick their winner by byte comparison and go to `decimal_arb_greatest` /
//! `decimal_arb_least`, which compare numerically and evaluate each argument
//! once.
//!
//! **Containers.** `make_array` / `named_struct` / `struct` / `row` keep the
//! element bytes but drop the metadata. Elements of an array literal are
//! unified to one scale, and the constructor is wrapped in
//! `decimal_arb_restamp(expr, template)`, which relabels the container's
//! element fields. `array_element` and the list functions that return the
//! input list get the same treatment; list membership / mutation functions
//! (`array_has`, `array_remove`, `array_append`, …) compare bytes, so their
//! list and scalar operands are re-encoded to a common scale first, and
//! `array_min` / `array_max` / `array_sort` over decimal_arb lists go to
//! numeric UDFs. Comparisons between two struct constructors unify their
//! children pairwise (equality) or desugar lexicographically (ordering).
//!
//! **Ordering inside aggregates and windows.** `ARRAY_AGG(v ORDER BY v)` and
//! `… OVER (ORDER BY v)` sort bytewise; the sort expressions are wrapped in
//! `decimal_arb_to_sort_key`, as the plan-level sort rule does for `ORDER BY`.
//!
//! **Text.** String builtins coerce the storage to text, so `length(v)`
//! counted encoding bytes and `v LIKE '2%'` matched nothing; `CAST(v AS
//! VARCHAR)` printed control characters. Those positions receive
//! `decimal_arb_to_string(v)`. A string *literal* compared with decimal_arb is
//! parsed exactly; a string *expression* is rejected at planning time, since
//! its scale can't be known and the comparison would silently fall back to
//! byte order.
//!
//! **Unary minus / `abs()`** are numeric-only in DataFusion and route to
//! `decimal_arb_neg` / `decimal_arb_abs`.
//!
//! **Why a `FunctionRewrite` and not an `OptimizerRule`:** `TypeCoercion` is an
//! *analyzer* pass that runs before any optimizer rule, and it fails on the
//! un-rewritten `BETWEEN`/`IN`. `FunctionRewrite`s run via `ApplyFunctionRewrites`
//! ahead of `TypeCoercion`, so we transform these nodes before coercion sees them.
//! (Recursion into sub-expressions is handled by the analyzer; `rewrite` only
//! inspects the top node, whose children have already been rewritten.)

use crate::functions::decimal_arb_ops::{
    DecimalArbAbsFunc, DecimalArbArrayExtremeFunc, DecimalArbArraySortFunc, DecimalArbEqFunc,
    DecimalArbExtremeFunc, DecimalArbGtFunc, DecimalArbGteFunc, DecimalArbLtFunc,
    DecimalArbLteFunc, DecimalArbNegFunc, DecimalArbNeqFunc, DecimalArbRescaleFunc,
    DecimalArbRestampFunc, DecimalArbSortKeyFunc, DecimalArbToStringFunc, DecimalArbWithMetaFunc,
    ToDecimalArbFromDecimal128Func, ToDecimalArbFromDecimal256Func, ToDecimalArbFromIntFunc,
    ToDecimalArbFromStringFunc,
};
use crate::types::decimal_arb::{DecimalArbType, DecimalArbValue, MAX_PRECISION};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::expr::{
    AggregateFunction, AggregateFunctionParams, Between, Case, Cast, InList, Like, ScalarFunction,
    Sort, TryCast, WindowFunction, WindowFunctionParams,
};
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, Operator, ScalarUDF, lit};
use std::str::FromStr;
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

/// The text of a non-null string literal, if `expr` is one.
pub(crate) fn text_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s.as_str()),
        _ => None,
    }
}

/// The smallest `(precision, scale)` that holds `value` exactly.
pub(crate) fn exact_precision_scale(value: &DecimalArbValue) -> (u32, u32) {
    let scale = value.fractional_digit_count() as u32;
    let int_digits = value.integer_digit_count() as u32;
    ((int_digits + scale).max(1), scale)
}

/// Common `(precision, scale)` for a set of decimal_arb fields: the widest
/// scale present, with enough integer digits for every member.
fn common_precision_scale(metas: &[(u32, u32)]) -> (u32, u32) {
    let s_out = metas.iter().map(|(_, s)| *s).max().unwrap_or(0);
    let int_max = metas
        .iter()
        .map(|(p, s)| p.saturating_sub(*s))
        .max()
        .unwrap_or(0);
    ((int_max + s_out).clamp(1, MAX_PRECISION), s_out)
}

/// The element field of a `List` / `LargeList` / `FixedSizeList` type.
fn list_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => Some(f),
        _ => None,
    }
}

/// Does `data_type` carry decimal_arb metadata on any field below it?
fn type_contains_decimal_arb(data_type: &DataType) -> bool {
    let field_has = |f: &FieldRef| {
        DecimalArbType::is_decimal_arb_field(f) || type_contains_decimal_arb(f.data_type())
    };
    match data_type {
        DataType::Struct(children) => children.iter().any(field_has),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => field_has(f),
        _ => false,
    }
}

/// One branch of a value-returning node after coercion.
enum Branch {
    /// A `NULL` branch: contributes no scale, passes through untouched.
    Null(Expr),
    /// A decimal_arb branch with its `(precision, scale)`.
    Arb(Expr, u32, u32),
}

/// A struct constructor's shape, for pairwise struct comparisons.
struct StructCtor {
    func: Arc<ScalarUDF>,
    /// `named_struct` interleaves name literals with values.
    named: bool,
    names: Vec<Expr>,
    values: Vec<Expr>,
}

/// Rewrites decimal_arb inside `BETWEEN` / `IN` / `CASE` / `COALESCE` /
/// `NULLIF` / `GREATEST` / `LEAST` / container constructors and list functions /
/// `CAST` / late-bound comparisons / aggregate and window `ORDER BY`.
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
    cast_from_string: Arc<ScalarUDF>,
    with_meta: Arc<ScalarUDF>,
    rescale: Arc<ScalarUDF>,
    restamp: Arc<ScalarUDF>,
    to_string: Arc<ScalarUDF>,
    sort_key: Arc<ScalarUDF>,
    neg: Arc<ScalarUDF>,
    abs: Arc<ScalarUDF>,
    greatest: Arc<ScalarUDF>,
    least: Arc<ScalarUDF>,
    array_min: Arc<ScalarUDF>,
    array_max: Arc<ScalarUDF>,
    array_sort: Arc<ScalarUDF>,
    coalesce: Arc<ScalarUDF>,
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
            cast_from_string: Arc::new(ScalarUDF::from(ToDecimalArbFromStringFunc::new())),
            with_meta: Arc::new(ScalarUDF::from(DecimalArbWithMetaFunc::new())),
            rescale: Arc::new(ScalarUDF::from(DecimalArbRescaleFunc::new())),
            restamp: Arc::new(ScalarUDF::from(DecimalArbRestampFunc::new())),
            to_string: Arc::new(ScalarUDF::from(DecimalArbToStringFunc::new())),
            sort_key: Arc::new(ScalarUDF::from(DecimalArbSortKeyFunc::new())),
            neg: Arc::new(ScalarUDF::from(DecimalArbNegFunc::new())),
            abs: Arc::new(ScalarUDF::from(DecimalArbAbsFunc::new())),
            greatest: Arc::new(ScalarUDF::from(DecimalArbExtremeFunc::greatest())),
            least: Arc::new(ScalarUDF::from(DecimalArbExtremeFunc::least())),
            array_min: Arc::new(ScalarUDF::from(DecimalArbArrayExtremeFunc::min())),
            array_max: Arc::new(ScalarUDF::from(DecimalArbArrayExtremeFunc::max())),
            array_sort: Arc::new(ScalarUDF::from(DecimalArbArraySortFunc::new())),
            coalesce: datafusion::functions::core::coalesce(),
        }
    }

    fn call(func: &Arc<ScalarUDF>, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(ScalarFunction {
            func: func.clone(),
            args,
        })
    }

    /// Wrap `expr` in `decimal_arb_with_meta(expr, p, s)` to restore decimal_arb
    /// field metadata that CASE/COALESCE planning drops. Only correct when
    /// `expr`'s bytes already sit at scale `s` — see [`Self::unify`].
    fn stamp_meta(&self, expr: Expr, precision: u32, scale: u32) -> Expr {
        Self::call(
            &self.with_meta,
            vec![expr, lit(precision as i64), lit(scale as i64)],
        )
    }

    /// `decimal_arb_rescale(expr, p, s)` — re-encode at scale `s`, value
    /// preserved; also accepts a list of decimal_arb.
    fn rescale(&self, expr: Expr, precision: u32, scale: u32) -> Expr {
        Self::call(
            &self.rescale,
            vec![expr, lit(precision as i64), lit(scale as i64)],
        )
    }

    /// `decimal_arb_restamp(expr, NULL::template)` — relabel the decimal_arb
    /// leaves of a container-typed `expr` with the metadata carried by
    /// `template`. The template travels as a typed NULL literal.
    fn restamp(&self, expr: Expr, template: &DataType) -> DFResult<Expr> {
        let template = Expr::Literal(ScalarValue::try_new_null(template)?, None);
        Ok(Self::call(&self.restamp, vec![expr, template]))
    }

    /// `decimal_arb_to_string(expr)` — the canonical decimal text of a
    /// decimal_arb value.
    fn to_text(&self, expr: Expr) -> Expr {
        Self::call(&self.to_string, vec![expr])
    }

    fn cmp(&self, udf: &Arc<ScalarUDF>, left: Expr, right: Expr) -> Expr {
        Self::call(udf, vec![left, right])
    }

    /// The field `expr` resolves to in `schema`. A scalar subquery is typed
    /// from its own plan so the metadata of the column it yields is visible —
    /// DataFusion types it from the bare data type.
    fn field_of(expr: &Expr, schema: &DFSchema) -> Option<FieldRef> {
        match expr {
            Expr::ScalarSubquery(subquery) => subquery.subquery.schema().fields().first().cloned(),
            Expr::Alias(alias) => Self::field_of(&alias.expr, schema),
            _ => expr.to_field(schema).ok().map(|(_, f)| f),
        }
    }

    /// Does `expr` resolve to a `decimal_arb` column in `schema`?
    fn is_decimal_arb(&self, expr: &Expr, schema: &DFSchema) -> bool {
        Self::field_of(expr, schema).is_some_and(|f| DecimalArbType::is_decimal_arb_field(&f))
    }

    /// `(precision, scale)` of `expr` if it resolves to decimal_arb.
    fn precision_scale(&self, expr: &Expr, schema: &DFSchema) -> Option<(u32, u32)> {
        let field = Self::field_of(expr, schema)?;
        DecimalArbType::precision_scale_from_field(&field)
    }

    /// `(precision, scale)` of the elements of `expr`, if it is a list of
    /// decimal_arb.
    fn list_precision_scale(&self, expr: &Expr, schema: &DFSchema) -> Option<(u32, u32)> {
        let field = Self::field_of(expr, schema)?;
        let element = list_element(field.data_type())?;
        DecimalArbType::precision_scale_from_field(element)
    }

    /// Coerce `operand` to decimal_arb based on its resolved type, mirroring the
    /// binary-op planner.
    ///
    /// - `Ok(Some(expr))`: the operand is (now) decimal_arb.
    /// - `Ok(None)`: not a type we convert (floats, NULL, …) — leave the node to
    ///   DataFusion, which errors on the lossy cases.
    /// - `Err`: a string *expression* whose scale can't be known. Left alone it
    ///   would not error: DataFusion casts the string to `LargeBinary` and
    ///   compares UTF-8 bytes with the canonical encoding, so the predicate is
    ///   silently false. Refuse with a hint instead.
    fn coerce(&self, operand: Expr, schema: &DFSchema) -> DFResult<Option<Expr>> {
        let Some(field) = Self::field_of(&operand, schema) else {
            return Ok(None);
        };
        let dtype = field.data_type().clone();
        Ok(match dtype {
            // Already decimal_arb (LargeBinary storage): pass through.
            DataType::LargeBinary if DecimalArbType::is_decimal_arb_field(&field) => Some(operand),
            DataType::Decimal128(_, _) => {
                Some(Self::call(&self.cast_from_decimal128, vec![operand]))
            }
            DataType::Decimal256(_, _) => {
                Some(Self::call(&self.cast_from_decimal256, vec![operand]))
            }
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Some(Self::call(
                &self.cast_from_int,
                vec![operand, lit(INT_COERCE_PRECISION), lit(0_i64)],
            )),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                if is_null_literal(&operand) {
                    // A typed NULL: DataFusion's null coercion yields NULL, which
                    // is the right answer.
                    return Ok(None);
                }
                match text_literal(&operand) {
                    Some(text) => {
                        let value = DecimalArbValue::from_str(text).map_err(|e| {
                            DataFusionError::Plan(format!(
                                "decimal_arb: string literal '{text}' is not a decimal number: {e}"
                            ))
                        })?;
                        let (p, s) = exact_precision_scale(&value);
                        Some(Self::call(
                            &self.cast_from_string,
                            vec![operand, lit(p as i64), lit(s as i64)],
                        ))
                    }
                    None => {
                        return Err(DataFusionError::Plan(format!(
                            "decimal_arb cannot be compared with the string expression `{operand}`: \
                             the comparison would fall back to raw byte order. Convert it explicitly \
                             with to_decimal_arb_from_string(<expr>, <precision>, <scale>)."
                        )));
                    }
                }
            }
            _ => None,
        })
    }

    /// Coerce every operand, or `None` if any cannot be.
    fn coerce_all(&self, operands: &[Expr], schema: &DFSchema) -> DFResult<Option<Vec<Expr>>> {
        let mut out = Vec::with_capacity(operands.len());
        for operand in operands {
            match self.coerce(operand.clone(), schema)? {
                Some(c) => out.push(c),
                None => return Ok(None),
            }
        }
        Ok(Some(out))
    }

    /// Bring every non-null member of `exprs` to decimal_arb at one common
    /// scale, and return the `(precision, scale)` they now share.
    ///
    /// The common scale is the widest present, so re-encoding a narrower
    /// member with `decimal_arb_rescale` is exact; the precision is the widest
    /// integer part plus that scale, so every member fits. NULL members pass
    /// through untouched. Returns `None` (leave the node to DataFusion) when no
    /// member is decimal_arb, or when some non-null member cannot be coerced.
    fn unify(
        &self,
        exprs: Vec<Expr>,
        schema: &DFSchema,
    ) -> DFResult<Option<(Vec<Expr>, u32, u32)>> {
        if !exprs.iter().any(|b| self.is_decimal_arb(b, schema)) {
            return Ok(None);
        }
        let mut items = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let is_null_typed = is_null_literal(&expr)
                || Self::field_of(&expr, schema).is_some_and(|f| f.data_type() == &DataType::Null);
            if is_null_typed {
                items.push(Branch::Null(expr));
                continue;
            }
            let Some(coerced) = self.coerce(expr, schema)? else {
                return Ok(None);
            };
            let Some((p, s)) = self.precision_scale(&coerced, schema) else {
                return Ok(None);
            };
            items.push(Branch::Arb(coerced, p, s));
        }
        let metas: Vec<(u32, u32)> = items
            .iter()
            .filter_map(|i| match i {
                Branch::Arb(_, p, s) => Some((*p, *s)),
                Branch::Null(_) => None,
            })
            .collect();
        if metas.is_empty() {
            return Ok(None);
        }
        let (p_out, s_out) = common_precision_scale(&metas);
        let exprs = items
            .into_iter()
            .map(|item| match item {
                Branch::Null(e) => e,
                Branch::Arb(e, _, s) if s == s_out => e,
                Branch::Arb(e, _, _) => self.rescale(e, p_out, s_out),
            })
            .collect();
        Ok(Some((exprs, p_out, s_out)))
    }

    /// Re-encode a decimal_arb expression to exactly `(p, s)` unless it is
    /// already there.
    fn at_scale(&self, expr: Expr, have: (u32, u32), want: (u32, u32)) -> Expr {
        if have == want {
            expr
        } else {
            self.rescale(expr, want.0, want.1)
        }
    }

    /// Wrap a container-valued `expr` so its decimal_arb leaves carry the
    /// metadata `template` describes.
    fn restamp_container(&self, expr: Expr, template: &DataType) -> DFResult<Transformed<Expr>> {
        Ok(Transformed::yes(self.restamp(expr, template)?))
    }

    /// `List<decimal_arb(p, s)>` template type.
    fn list_template(precision: u32, scale: u32) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(DecimalArbType::field(
            "item", precision, scale, true,
        )?)))
    }

    /// The struct constructor `expr` is (looking through a restamp), if any.
    fn struct_ctor(expr: &Expr) -> Option<StructCtor> {
        let Expr::ScalarFunction(sf) = expr else {
            return None;
        };
        if sf.func.name() == "decimal_arb_restamp" {
            return Self::struct_ctor(sf.args.first()?);
        }
        match sf.func.name() {
            "named_struct" => {
                if !sf.args.len().is_multiple_of(2) {
                    return None;
                }
                let names = sf.args.iter().step_by(2).cloned().collect();
                let values = sf.args.iter().skip(1).step_by(2).cloned().collect();
                Some(StructCtor {
                    func: sf.func.clone(),
                    named: true,
                    names,
                    values,
                })
            }
            "struct" | "row" => Some(StructCtor {
                func: sf.func.clone(),
                named: false,
                names: vec![],
                values: sf.args.clone(),
            }),
            _ => None,
        }
    }

    /// Rebuild a struct constructor with new children, restamped so decimal_arb
    /// children keep their metadata.
    fn rebuild_struct(
        &self,
        ctor: &StructCtor,
        values: Vec<Expr>,
        schema: &DFSchema,
    ) -> DFResult<Expr> {
        let args = if ctor.named {
            ctor.names
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .flat_map(|(n, v)| [n, v])
                .collect()
        } else {
            values.clone()
        };
        let expr = Self::call(&ctor.func, args);
        match self.struct_template(&values, &ctor.names, schema)? {
            Some(template) => self.restamp(expr, &template),
            None => Ok(expr),
        }
    }

    /// `Struct` template for a constructor whose children are `values`, or
    /// `None` when no child carries decimal_arb metadata.
    fn struct_template(
        &self,
        values: &[Expr],
        names: &[Expr],
        schema: &DFSchema,
    ) -> DFResult<Option<DataType>> {
        let mut fields = Vec::with_capacity(values.len());
        let mut any = false;
        for (i, value) in values.iter().enumerate() {
            let name = names
                .get(i)
                .and_then(text_literal)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("c{i}"));
            let field = match Self::field_of(value, schema) {
                Some(f) => {
                    any |= DecimalArbType::is_decimal_arb_field(&f)
                        || type_contains_decimal_arb(f.data_type());
                    Field::new(name, f.data_type().clone(), true)
                        .with_metadata(f.metadata().clone())
                }
                None => Field::new(name, DataType::Null, true),
            };
            fields.push(Arc::new(field));
        }
        Ok(any.then(|| DataType::Struct(fields.into())))
    }

    /// Replace decimal_arb arguments DataFusion is about to read as text with
    /// their canonical decimal text: DataFusion's own coercion is consulted,
    /// and an argument it would coerce to a string type is one it would
    /// misread. Our own `decimal_arb_*` / `to_decimal_arb_*` functions are
    /// exempt.
    fn textualize_args(
        &self,
        sf: ScalarFunction,
        schema: &DFSchema,
    ) -> DFResult<Transformed<Expr>> {
        let name = sf.func.name();
        if name.starts_with("decimal_arb_") || name.starts_with("to_decimal_arb") {
            return Ok(Transformed::no(Expr::ScalarFunction(sf)));
        }
        let arb: Vec<usize> = (0..sf.args.len())
            .filter(|&i| self.is_decimal_arb(&sf.args[i], schema))
            .collect();
        if arb.is_empty() {
            return Ok(Transformed::no(Expr::ScalarFunction(sf)));
        }
        let Ok(fields) = sf
            .args
            .iter()
            .map(|a| a.to_field(schema).map(|(_, f)| f))
            .collect::<DFResult<Vec<_>>>()
        else {
            return Ok(Transformed::no(Expr::ScalarFunction(sf)));
        };
        let Ok(coerced) = fields_with_udf(&fields, sf.func.as_ref()) else {
            // DataFusion will refuse these arguments itself.
            return Ok(Transformed::no(Expr::ScalarFunction(sf)));
        };
        let convert: Vec<usize> = arb
            .into_iter()
            .filter(|&i| coerced.get(i).is_some_and(|f| is_text_type(f.data_type())))
            .collect();
        if convert.is_empty() {
            return Ok(Transformed::no(Expr::ScalarFunction(sf)));
        }
        let args = sf
            .args
            .into_iter()
            .enumerate()
            .map(|(i, a)| {
                if convert.contains(&i) {
                    self.to_text(a)
                } else {
                    a
                }
            })
            .collect();
        Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
            func: sf.func,
            args,
        })))
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

    /// `ORDER BY` inside an aggregate / window call: a decimal_arb sort
    /// expression becomes its sort key, so the order is numeric.
    fn sort_keyed(&self, sorts: Vec<Sort>, schema: &DFSchema) -> (Vec<Sort>, bool) {
        let mut changed = false;
        let sorts = sorts
            .into_iter()
            .map(|sort| {
                if self.is_decimal_arb(&sort.expr, schema) {
                    changed = true;
                    Sort {
                        expr: Self::call(&self.sort_key, vec![sort.expr]),
                        ..sort
                    }
                } else {
                    sort
                }
            })
            .collect();
        (sorts, changed)
    }

    /// One side of a `BETWEEN`: the comparison, or UNKNOWN for a NULL bound.
    fn between_side(
        &self,
        udf: &Arc<ScalarUDF>,
        subject: &Expr,
        bound: Expr,
        schema: &DFSchema,
    ) -> DFResult<Option<Expr>> {
        if is_null_literal(&bound) {
            // `x >= NULL` is UNKNOWN; `AND`/`OR` with the other side then
            // follows three-valued logic exactly as DataFusion's own
            // desugaring would.
            return Ok(Some(lit(ScalarValue::Boolean(None))));
        }
        Ok(self
            .coerce(bound, schema)?
            .map(|b| self.cmp(udf, subject.clone(), b)))
    }

    /// Lexicographic ordering of two struct constructors, child by child:
    /// `l0 < r0 OR (l0 = r0 AND (l1 < r1 OR …))`. decimal_arb children compare
    /// through the UDFs, everything else natively.
    fn struct_ordering(
        &self,
        op: Operator,
        left: &[Expr],
        right: &[Expr],
        schema: &DFSchema,
    ) -> DFResult<Option<Expr>> {
        let strict = match op {
            Operator::Lt => Operator::Lt,
            Operator::LtEq => Operator::Lt,
            Operator::Gt => Operator::Gt,
            Operator::GtEq => Operator::Gt,
            _ => return Ok(None),
        };
        let inclusive = matches!(op, Operator::LtEq | Operator::GtEq);
        let compare = |op: Operator, l: &Expr, r: &Expr| -> DFResult<Expr> {
            if (self.is_decimal_arb(l, schema) || self.is_decimal_arb(r, schema))
                && let (Some(l), Some(r)) = (
                    self.coerce(l.clone(), schema)?,
                    self.coerce(r.clone(), schema)?,
                )
            {
                let udf = self.cmp_udf(op).expect("comparison operator");
                return Ok(self.cmp(udf, l, r));
            }
            Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(l.clone()),
                op,
                Box::new(r.clone()),
            )))
        };
        let n = left.len().min(right.len());
        let mut result: Option<Expr> = None;
        for i in (0..n).rev() {
            let last = i == n - 1;
            let step = if last && inclusive {
                compare(
                    if strict == Operator::Lt {
                        Operator::LtEq
                    } else {
                        Operator::GtEq
                    },
                    &left[i],
                    &right[i],
                )?
            } else {
                compare(strict, &left[i], &right[i])?
            };
            result = Some(match result {
                None => step,
                Some(rest) => Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(step),
                    Operator::Or,
                    Box::new(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(compare(Operator::Eq, &left[i], &right[i])?),
                        Operator::And,
                        Box::new(rest),
                    ))),
                )),
            });
        }
        Ok(result)
    }

    /// Comparison of two struct constructors with decimal_arb children.
    fn compare_structs(
        &self,
        left: StructCtor,
        op: Operator,
        right: StructCtor,
        schema: &DFSchema,
    ) -> DFResult<Option<Expr>> {
        if left.values.len() != right.values.len() {
            return Ok(None);
        }
        match op {
            Operator::Eq
            | Operator::NotEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom => {
                // Pairwise-unified children make the native struct comparison
                // a byte comparison of equal encodings.
                let mut l_vals = Vec::with_capacity(left.values.len());
                let mut r_vals = Vec::with_capacity(right.values.len());
                for (l, r) in left.values.iter().zip(&right.values) {
                    if self.is_decimal_arb(l, schema) || self.is_decimal_arb(r, schema) {
                        match self.unify(vec![l.clone(), r.clone()], schema)? {
                            Some((mut pair, _, _)) => {
                                r_vals.push(pair.pop().expect("two"));
                                l_vals.push(pair.pop().expect("two"));
                            }
                            None => return Ok(None),
                        }
                    } else {
                        l_vals.push(l.clone());
                        r_vals.push(r.clone());
                    }
                }
                let l = self.rebuild_struct(&left, l_vals, schema)?;
                let r = self.rebuild_struct(&right, r_vals, schema)?;
                Ok(Some(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(l),
                    op,
                    Box::new(r),
                ))))
            }
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                self.struct_ordering(op, &left.values, &right.values, schema)
            }
            _ => Ok(None),
        }
    }

    /// List functions over decimal_arb elements.
    ///
    /// Membership and mutation compare element bytes, so the list and the
    /// scalar operands are re-encoded to a common scale first; functions that
    /// return a list are relabelled with the element metadata (or re-encoded
    /// back to the input list's scale, which carries it).
    fn list_function(
        &self,
        sf: ScalarFunction,
        schema: &DFSchema,
    ) -> DFResult<Option<Transformed<Expr>>> {
        let name = sf.func.name();
        let list_meta = |i: usize| {
            sf.args
                .get(i)
                .and_then(|a| self.list_precision_scale(a, schema))
        };
        // A scalar operand: coerced to decimal_arb, with its (p, s).
        let scalar = |i: usize| -> DFResult<Option<(Expr, (u32, u32))>> {
            let Some(arg) = sf.args.get(i) else {
                return Ok(None);
            };
            if is_null_literal(arg) {
                return Ok(None);
            }
            let Some(coerced) = self.coerce(arg.clone(), schema)? else {
                return Ok(None);
            };
            Ok(self.precision_scale(&coerced, schema).map(|m| (coerced, m)))
        };
        let rebuilt = |args: Vec<Expr>| {
            Expr::ScalarFunction(ScalarFunction {
                func: sf.func.clone(),
                args,
            })
        };
        match name {
            // list[i]: the element keeps the list's metadata.
            "array_element" | "array_extract" | "list_element" | "list_extract" => {
                let Some((p, s)) = list_meta(0) else {
                    return Ok(None);
                };
                Ok(Some(Transformed::yes(self.stamp_meta(
                    Expr::ScalarFunction(sf),
                    p,
                    s,
                ))))
            }
            // list, scalar → bool / index: compare at a common scale.
            "array_has" | "list_has" | "array_contains" | "list_contains" | "array_position"
            | "list_position" | "array_indexof" | "list_indexof" | "array_positions"
            | "list_positions" => {
                let (Some(lm), Some((x, xm))) = (list_meta(0), scalar(1)?) else {
                    return Ok(None);
                };
                let common = common_precision_scale(&[lm, xm]);
                let mut args = sf.args.clone();
                args[0] = self.at_scale(args[0].clone(), lm, common);
                args[1] = self.at_scale(x, xm, common);
                Ok(Some(Transformed::yes(rebuilt(args))))
            }
            // list, list → bool: compare at a common scale.
            "array_has_any" | "array_has_all" | "list_has_any" | "list_has_all" => {
                let (Some(lm), Some(rm)) = (list_meta(0), list_meta(1)) else {
                    return Ok(None);
                };
                let common = common_precision_scale(&[lm, rm]);
                let mut args = sf.args.clone();
                args[0] = self.at_scale(args[0].clone(), lm, common);
                args[1] = self.at_scale(args[1].clone(), rm, common);
                Ok(Some(Transformed::yes(rebuilt(args))))
            }
            // list, scalar[, n] → list: match at a common scale, then return to
            // the list's own scale (exact: every element came from it).
            "array_remove" | "array_remove_n" | "array_remove_all" | "list_remove"
            | "list_remove_n" | "list_remove_all" => {
                let (Some(lm), Some((x, xm))) = (list_meta(0), scalar(1)?) else {
                    return Ok(None);
                };
                let common = common_precision_scale(&[lm, xm]);
                let mut args = sf.args.clone();
                args[0] = self.at_scale(args[0].clone(), lm, common);
                args[1] = self.at_scale(x, xm, common);
                // DataFusion's list coercion rebuilds the list type without
                // the element metadata, so relabel the result before
                // re-encoding it back to the list's own scale.
                let relabelled =
                    self.restamp(rebuilt(args), &Self::list_template(common.0, common.1)?)?;
                Ok(Some(Transformed::yes(
                    self.at_scale(relabelled, common, lm),
                )))
            }
            // list, from, to[, n] → list: the replacement must fit the list's
            // scale, which the final re-encoding checks.
            "array_replace" | "array_replace_n" | "array_replace_all" | "list_replace"
            | "list_replace_n" | "list_replace_all" => {
                let (Some(lm), Some((from, fm)), Some((to, tm))) =
                    (list_meta(0), scalar(1)?, scalar(2)?)
                else {
                    return Ok(None);
                };
                let common = common_precision_scale(&[lm, fm, tm]);
                let mut args = sf.args.clone();
                args[0] = self.at_scale(args[0].clone(), lm, common);
                args[1] = self.at_scale(from, fm, common);
                args[2] = self.at_scale(to, tm, common);
                let relabelled =
                    self.restamp(rebuilt(args), &Self::list_template(common.0, common.1)?)?;
                Ok(Some(Transformed::yes(
                    self.at_scale(relabelled, common, lm),
                )))
            }
            // list, scalar → list at the list's scale.
            "array_append" | "list_append" | "array_push_back" => {
                let (Some(lm), Some((x, xm))) = (list_meta(0), scalar(1)?) else {
                    return Ok(None);
                };
                let mut args = sf.args.clone();
                args[1] = self.at_scale(x, xm, lm);
                let template = Self::list_template(lm.0, lm.1)?;
                Ok(Some(self.restamp_container(rebuilt(args), &template)?))
            }
            // scalar, list → list at the list's scale.
            "array_prepend" | "list_prepend" | "array_push_front" => {
                let (Some((x, xm)), Some(lm)) = (scalar(0)?, list_meta(1)) else {
                    return Ok(None);
                };
                let mut args = sf.args.clone();
                args[0] = self.at_scale(x, xm, lm);
                let template = Self::list_template(lm.0, lm.1)?;
                Ok(Some(self.restamp_container(rebuilt(args), &template)?))
            }
            // list[, …] → same list: only the label is lost.
            "array_distinct" | "list_distinct" | "array_slice" | "list_slice"
            | "array_pop_front" | "list_pop_front" | "array_pop_back" | "list_pop_back"
            | "array_reverse" | "list_reverse" => {
                let Some((p, s)) = list_meta(0) else {
                    return Ok(None);
                };
                let template = Self::list_template(p, s)?;
                Ok(Some(
                    self.restamp_container(Expr::ScalarFunction(sf), &template)?,
                ))
            }
            // list, list[, …] → list: elements meet, so unify the lists.
            "array_union" | "list_union" | "array_intersect" | "list_intersect"
            | "array_except" | "list_except" | "array_concat" | "array_cat" | "list_concat"
            | "list_cat" => {
                let metas: Vec<(u32, u32)> = (0..sf.args.len()).filter_map(list_meta).collect();
                if metas.len() != sf.args.len() {
                    return Ok(None);
                }
                let common = common_precision_scale(&metas);
                let args = sf
                    .args
                    .iter()
                    .cloned()
                    .zip(metas)
                    .map(|(a, m)| self.at_scale(a, m, common))
                    .collect();
                let template = Self::list_template(common.0, common.1)?;
                Ok(Some(self.restamp_container(rebuilt(args), &template)?))
            }
            "array_min" | "list_min" | "array_max" | "list_max" if sf.args.len() == 1 => {
                if list_meta(0).is_none() {
                    return Ok(None);
                }
                let udf = if name.ends_with("min") {
                    &self.array_min
                } else {
                    &self.array_max
                };
                Ok(Some(Transformed::yes(Self::call(udf, sf.args))))
            }
            "array_sort" | "list_sort" => {
                if list_meta(0).is_none() {
                    return Ok(None);
                }
                Ok(Some(Transformed::yes(Self::call(
                    &self.array_sort,
                    sf.args,
                ))))
            }
            _ => Ok(None),
        }
    }

    /// Comparison nodes only (`BinaryExpr` comparisons, `BETWEEN`, `IN`), for
    /// the optimizer-time pass in `DecimalArbScaleUnifyRule`.
    ///
    /// The analyzer rewrites a plan bottom-up but keeps each node's schema as
    /// the SQL planner computed it, so a projection whose `CASE`/`COALESCE`
    /// became decimal_arb here still advertises bare `LargeBinary` to the
    /// query above it — `SELECT v < c FROM (SELECT CASE … AS v, c …)` was left
    /// as a byte comparison. Schemas are recomputed by the time the optimizer
    /// runs, so the same comparison rewrite is applied once more there.
    /// Everything else (value nodes, containers, casts) was handled by the
    /// analyzer pass and is not touched again.
    pub fn rewrite_comparison(&self, expr: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
        match &expr {
            Expr::BinaryExpr(BinaryExpr { op, .. })
                if self.cmp_udf(*op).is_some()
                    || matches!(op, Operator::IsDistinctFrom | Operator::IsNotDistinctFrom) =>
            {
                self.rewrite_expr(expr, schema)
            }
            Expr::Between(_) | Expr::InList(_) => self.rewrite_expr(expr, schema),
            _ => Ok(Transformed::no(expr)),
        }
    }

    /// Rewrite a single expression node; everything not involving decimal_arb
    /// passes through.
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
                // x BETWEEN lo AND hi  ⇔  x >= lo AND x <= hi
                // x NOT BETWEEN lo AND hi  ⇔  x < lo OR x > hi
                let (lo_udf, hi_udf, combine) = if negated {
                    (&self.lt, &self.gt, Operator::Or)
                } else {
                    (&self.gte, &self.lte, Operator::And)
                };
                let (Some(lo), Some(hi)) = (
                    self.between_side(lo_udf, &subject, *low.clone(), schema)?,
                    self.between_side(hi_udf, &subject, *high.clone(), schema)?,
                ) else {
                    return Ok(Transformed::no(Expr::Between(Between {
                        expr: subject,
                        negated,
                        low,
                        high,
                    })));
                };
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(lo),
                    combine,
                    Box::new(hi),
                ))))
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
                // matches and NULL otherwise (and the dual for NOT IN).
                let mut coerced = Vec::with_capacity(list.len());
                let mut saw_null = false;
                for e in &list {
                    if is_null_literal(e) {
                        saw_null = true;
                        continue;
                    }
                    match self.coerce(e.clone(), schema)? {
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
            Expr::Case(case) => {
                // A *simple* CASE (`CASE v WHEN w THEN …`) compares `v` against
                // each WHEN on the physical bytes, so `1` never matched `1.00`.
                // At one common scale that byte equality is exact, and the
                // operand is still evaluated once — a desugaring into searched
                // WHENs re-evaluated it per branch.
                let mut changed = false;
                let case = match case.expr {
                    Some(operand) => {
                        let mut subjects = vec![(*operand).clone()];
                        subjects.extend(case.when_then_expr.iter().map(|(w, _)| (**w).clone()));
                        match self.unify(subjects, schema)? {
                            Some((mut unified, _, _)) => {
                                changed = true;
                                let operand = unified.remove(0);
                                Case {
                                    expr: Some(Box::new(operand)),
                                    when_then_expr: case
                                        .when_then_expr
                                        .into_iter()
                                        .zip(unified)
                                        .map(|((_, then), when)| (Box::new(when), then))
                                        .collect(),
                                    else_expr: case.else_expr,
                                }
                            }
                            None => Case {
                                expr: Some(operand),
                                when_then_expr: case.when_then_expr,
                                else_expr: case.else_expr,
                            },
                        }
                    }
                    None => case,
                };

                // Bring every THEN/ELSE branch to one scale, then stamp the
                // result so sinks see NUMERIC(p, s), not BYTEA.
                let mut branches: Vec<Expr> = case
                    .when_then_expr
                    .iter()
                    .map(|(_, t)| (**t).clone())
                    .collect();
                if let Some(e) = &case.else_expr {
                    branches.push((**e).clone());
                }
                match self.unify(branches, schema)? {
                    Some((mut exprs, p, s)) => {
                        let else_expr = if case.else_expr.is_some() {
                            exprs.pop().map(Box::new)
                        } else {
                            None
                        };
                        let when_then_expr = case
                            .when_then_expr
                            .into_iter()
                            .zip(exprs)
                            .map(|((when, _), then)| (when, Box::new(then)))
                            .collect();
                        Ok(Transformed::yes(self.stamp_meta(
                            Expr::Case(Case {
                                expr: case.expr,
                                when_then_expr,
                                else_expr,
                            }),
                            p,
                            s,
                        )))
                    }
                    None if changed => Ok(Transformed::yes(Expr::Case(case))),
                    None => Ok(Transformed::no(Expr::Case(case))),
                }
            }
            // COALESCE and its NVL/IFNULL aliases over decimal_arb args. NVL and
            // IFNULL coerce LargeBinary to text in DataFusion, so they become
            // COALESCE, which keeps the bytes.
            Expr::ScalarFunction(sf) if matches!(sf.func.name(), "coalesce" | "nvl" | "ifnull") => {
                match self.unify(sf.args.clone(), schema)? {
                    Some((args, p, s)) => Ok(Transformed::yes(self.stamp_meta(
                        Self::call(&self.coalesce, args),
                        p,
                        s,
                    ))),
                    None => Ok(Transformed::no(Expr::ScalarFunction(sf))),
                }
            }
            // `nvl2(test, a, b)` returns `a` or `b`: unify those two.
            Expr::ScalarFunction(sf)
                if sf.func.name() == "nvl2"
                    && sf.args.len() == 3
                    && sf.args[1..].iter().any(|a| self.is_decimal_arb(a, schema)) =>
            {
                match self.unify(sf.args[1..].to_vec(), schema)? {
                    Some((branches, p, s)) => {
                        let mut args = vec![sf.args[0].clone()];
                        args.extend(branches);
                        Ok(Transformed::yes(self.stamp_meta(
                            Expr::ScalarFunction(ScalarFunction {
                                func: sf.func,
                                args,
                            }),
                            p,
                            s,
                        )))
                    }
                    None => Ok(Transformed::no(Expr::ScalarFunction(sf))),
                }
            }
            // `nullif(a, b)` is an equality test: native and exact once both
            // sides share a scale, `a` evaluated once. The result is `a` at
            // that scale, stamped.
            Expr::ScalarFunction(sf)
                if sf.func.name() == "nullif"
                    && sf.args.len() == 2
                    && sf.args.iter().any(|a| self.is_decimal_arb(a, schema)) =>
            {
                match self.unify(sf.args.clone(), schema)? {
                    Some((args, p, s)) => Ok(Transformed::yes(self.stamp_meta(
                        Expr::ScalarFunction(ScalarFunction {
                            func: sf.func,
                            args,
                        }),
                        p,
                        s,
                    ))),
                    None => Ok(Transformed::no(Expr::ScalarFunction(sf))),
                }
            }
            // GREATEST / LEAST pick their winner by comparison — numerically in
            // the UDFs, bytewise in the builtins. NULL arguments never win and
            // are dropped.
            Expr::ScalarFunction(sf)
                if matches!(sf.func.name(), "greatest" | "least")
                    && sf.args.iter().any(|a| self.is_decimal_arb(a, schema)) =>
            {
                let args: Vec<Expr> = sf
                    .args
                    .iter()
                    .filter(|a| !is_null_literal(a))
                    .cloned()
                    .collect();
                match self.coerce_all(&args, schema)? {
                    Some(coerced) if !coerced.is_empty() => {
                        let udf = if sf.func.name() == "greatest" {
                            &self.greatest
                        } else {
                            &self.least
                        };
                        Ok(Transformed::yes(Self::call(udf, coerced)))
                    }
                    _ => Ok(Transformed::no(Expr::ScalarFunction(sf))),
                }
            }
            // Array literal: elements at one scale, relabelled.
            Expr::ScalarFunction(sf)
                if matches!(sf.func.name(), "make_array" | "array" | "make_list")
                    && sf.args.iter().any(|a| self.is_decimal_arb(a, schema)) =>
            {
                match self.unify(sf.args.clone(), schema)? {
                    Some((args, p, s)) => {
                        let template = Self::list_template(p, s)?;
                        self.restamp_container(
                            Expr::ScalarFunction(ScalarFunction {
                                func: sf.func,
                                args,
                            }),
                            &template,
                        )
                    }
                    None => Ok(Transformed::no(Expr::ScalarFunction(sf))),
                }
            }
            // Struct constructors: relabel the decimal_arb children.
            Expr::ScalarFunction(sf)
                if matches!(sf.func.name(), "named_struct" | "struct" | "row")
                    && Self::struct_ctor(&Expr::ScalarFunction(sf.clone())).is_some() =>
            {
                let ctor = Self::struct_ctor(&Expr::ScalarFunction(sf.clone())).expect("checked");
                match self.struct_template(&ctor.values, &ctor.names, schema)? {
                    Some(template) => self.restamp_container(Expr::ScalarFunction(sf), &template),
                    None => Ok(Transformed::no(Expr::ScalarFunction(sf))),
                }
            }
            // A comparison the `DecimalArbExprPlanner` never saw, because at
            // SQL-planning time the operand wasn't yet known to be decimal_arb
            // — typically a CASE/COALESCE result, which only becomes decimal_arb
            // once the arms above stamp it. Left alone it compares raw bytes.
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let is_comparison = self.cmp_udf(op).is_some()
                    || matches!(op, Operator::IsDistinctFrom | Operator::IsNotDistinctFrom);
                let untouched = |left, right| {
                    Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                        left,
                        op,
                        right,
                    })))
                };
                if !is_comparison {
                    // Arithmetic and everything else stays with the planner /
                    // DataFusion; only comparisons are ours to route.
                    return untouched(left, right);
                }
                // Two struct constructors with decimal_arb children.
                if let (Some(l), Some(r)) = (Self::struct_ctor(&left), Self::struct_ctor(&right))
                    && (l.values.iter().any(|v| self.is_decimal_arb(v, schema))
                        || r.values.iter().any(|v| self.is_decimal_arb(v, schema)))
                {
                    return match self.compare_structs(l, op, r, schema)? {
                        Some(rewritten) => Ok(Transformed::yes(rewritten)),
                        None => untouched(left, right),
                    };
                }
                // Two lists of decimal_arb (`[a] = [b]`, `vals = other_vals`):
                // DataFusion compares the elements' bytes, which is exact once
                // both lists sit at one scale.
                if let (Some(lm), Some(rm)) = (
                    self.list_precision_scale(&left, schema),
                    self.list_precision_scale(&right, schema),
                ) {
                    if lm == rm {
                        return untouched(left, right);
                    }
                    let common = common_precision_scale(&[lm, rm]);
                    return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(self.at_scale(*left, lm, common)),
                        op,
                        Box::new(self.at_scale(*right, rm, common)),
                    ))));
                }
                if !self.is_decimal_arb(&left, schema) && !self.is_decimal_arb(&right, schema) {
                    return untouched(left, right);
                }
                match op {
                    // Null-safe equality is a byte comparison DataFusion gets
                    // right once both sides share a scale — and each side is
                    // evaluated once.
                    Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => {
                        match self.unify(vec![(*left).clone(), (*right).clone()], schema)? {
                            Some((mut pair, _, _)) => {
                                let r = pair.pop().expect("two");
                                let l = pair.pop().expect("two");
                                if l == *left && r == *right {
                                    // Already at one scale (the optimizer pass
                                    // revisits this node): nothing to change.
                                    return untouched(left, right);
                                }
                                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                                    Box::new(l),
                                    op,
                                    Box::new(r),
                                ))))
                            }
                            None => untouched(left, right),
                        }
                    }
                    _ => {
                        let (Some(l), Some(r)) = (
                            self.coerce((*left).clone(), schema)?,
                            self.coerce((*right).clone(), schema)?,
                        ) else {
                            return untouched(left, right);
                        };
                        let udf = self.cmp_udf(op).expect("checked above");
                        Ok(Transformed::yes(self.cmp(udf, l, r)))
                    }
                }
            }
            // A scalar subquery is executed by a physical expression whose
            // output field is a bare `LargeBinary`, so nothing downstream
            // would see its `(p, s)` — `a = (SELECT b …)` failed inside
            // `decimal_arb_eq`. Relabel it from the subquery's own schema (a
            // pure relabel; the bytes are already at that scale).
            Expr::ScalarSubquery(_) => {
                match Self::field_of(&expr, schema)
                    .and_then(|f| DecimalArbType::precision_scale_from_field(&f))
                {
                    Some((p, s)) => Ok(Transformed::yes(self.stamp_meta(expr, p, s))),
                    None => Ok(Transformed::no(expr)),
                }
            }
            // `CAST(x AS VARCHAR)` on decimal_arb hands Arrow the canonical
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
            // Unary minus and abs() are numeric-only in DataFusion and were
            // rejected at planning for decimal_arb; route them to the UDFs.
            Expr::Negative(inner) if self.is_decimal_arb(&inner, schema) => {
                Ok(Transformed::yes(Self::call(&self.neg, vec![*inner])))
            }
            Expr::ScalarFunction(sf)
                if sf.func.name() == "abs"
                    && sf.args.len() == 1
                    && self.is_decimal_arb(&sf.args[0], schema) =>
            {
                Ok(Transformed::yes(Self::call(
                    &self.abs,
                    vec![sf.args[0].clone()],
                )))
            }
            // LIKE / ILIKE / SIMILAR TO read the operand as text.
            Expr::Like(like) if self.is_decimal_arb(&like.expr, schema) => {
                Ok(Transformed::yes(Expr::Like(Like {
                    expr: Box::new(self.to_text(*like.expr)),
                    ..like
                })))
            }
            Expr::SimilarTo(like) if self.is_decimal_arb(&like.expr, schema) => {
                Ok(Transformed::yes(Expr::SimilarTo(Like {
                    expr: Box::new(self.to_text(*like.expr)),
                    ..like
                })))
            }
            // Aggregates: `string_agg` reads the value as text; any `ORDER BY`
            // inside the call sorts bytewise and gets sort keys. `array_agg`
            // keeps the bytes — its UDAF wrapper restores the element metadata.
            Expr::AggregateFunction(AggregateFunction { func, params }) => {
                let mut changed = false;
                let args = if func.name() == "string_agg" {
                    params
                        .args
                        .into_iter()
                        .map(|a| {
                            if self.is_decimal_arb(&a, schema) {
                                changed = true;
                                self.to_text(a)
                            } else {
                                a
                            }
                        })
                        .collect()
                } else {
                    params.args
                };
                let (order_by, sorted) = self.sort_keyed(params.order_by, schema);
                changed |= sorted;
                let rebuilt = Expr::AggregateFunction(AggregateFunction {
                    func,
                    params: AggregateFunctionParams {
                        args,
                        order_by,
                        ..params
                    },
                });
                Ok(if changed {
                    Transformed::yes(rebuilt)
                } else {
                    Transformed::no(rebuilt)
                })
            }
            // Window functions: `OVER (ORDER BY v)` sorts bytewise.
            Expr::WindowFunction(window) => {
                let WindowFunction { fun, params } = *window;
                let (order_by, changed) = self.sort_keyed(params.order_by, schema);
                let rebuilt = Expr::WindowFunction(Box::new(WindowFunction {
                    fun,
                    params: WindowFunctionParams { order_by, ..params },
                }));
                Ok(if changed {
                    Transformed::yes(rebuilt)
                } else {
                    Transformed::no(rebuilt)
                })
            }
            // List functions over decimal_arb elements, then any other scalar
            // function: string builtins get the decimal text instead of bytes.
            Expr::ScalarFunction(sf) => match self.list_function(sf.clone(), schema)? {
                Some(rewritten) => Ok(rewritten),
                None => self.textualize_args(sf, schema),
            },
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
