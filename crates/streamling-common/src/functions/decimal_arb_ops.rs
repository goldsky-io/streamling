//! Scalar UDFs for the `streamling.decimal_arb` extension type.
//!
//! See `specs/001-decimal-arbitrary-precision/contracts/scalar-udf-signatures.md`
//! for signatures and `research.md` (R3) for the planner-integration choice
//! that lets DataFusion's native operator surface dispatch here.
//!
//! Most ScalarUDFs land in US2 (T042–T045). This module currently exposes
//! the one helper sinks need today:
//!
//! - `DecimalArbToStringFunc` (`decimal_arb_to_string`) — converts a
//!   `LargeBinary` decimal_arb column into a `Utf8` column of canonical
//!   decimal strings. Used by Postgres / ClickHouse-string / JSON sinks
//!   to project decimal_arb to a bind-friendly form before the per-row
//!   binding path (which has no Field-metadata access).

use crate::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, MAX_PRECISION,
};
use crate::{streamling_user_bail, streamling_user_err};
use arrow::array::{Array, BooleanBuilder, LargeBinaryArray, LargeBinaryBuilder, StringBuilder};
use arrow_schema::FieldRef;
use bigdecimal::{BigDecimal, RoundingMode};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use num_traits::Zero;
use std::cmp::Ordering;
use std::sync::Arc;

/// Default scale for `decimal_arb_div` results (matches Postgres NUMERIC default).
pub const DEFAULT_DIV_SCALE: u32 = 18;

/// Cap a precision computation at `MAX_PRECISION` so widening rules don't
/// overflow the type's documented sanity guard.
fn cap_precision(p: u32) -> u32 {
    p.min(MAX_PRECISION)
}

/// Compute output `(precision, scale)` for the given binary op kind per
/// `data-model.md` E5 rules.
#[derive(Debug, Clone, Copy)]
enum BinaryOpKind {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

fn output_precision_scale(kind: BinaryOpKind, p1: u32, s1: u32, p2: u32, s2: u32) -> (u32, u32) {
    match kind {
        BinaryOpKind::Add | BinaryOpKind::Sub => {
            let s_out = s1.max(s2);
            let int1 = p1.saturating_sub(s1);
            let int2 = p2.saturating_sub(s2);
            let p_out = cap_precision(int1.max(int2) + s_out + 1);
            (p_out, s_out)
        }
        BinaryOpKind::Mul => {
            let p_out = cap_precision(p1 + p2 + 1);
            // Cap the scale at the (already-capped) precision: when widening
            // pushes `p1 + p2 + 1` past `MAX_PRECISION`, an uncapped
            // `s1 + s2` could exceed `p_out` and trip the
            // "scale cannot exceed precision" guard in `validate_precision_scale`,
            // turning a representable product into a runtime error.
            let s_out = (s1 + s2).min(p_out);
            (p_out, s_out)
        }
        BinaryOpKind::Div => {
            let s_out = s1.max(DEFAULT_DIV_SCALE);
            let int1 = p1.saturating_sub(s1);
            let p_out = cap_precision(int1 + s2 + s_out);
            (p_out, s_out)
        }
        BinaryOpKind::Mod => {
            let s_out = s1.max(s2);
            let int1 = p1.saturating_sub(s1);
            let int2 = p2.saturating_sub(s2);
            let p_out = cap_precision(int1.min(int2) + s_out);
            (p_out, s_out)
        }
    }
}

/// Read `(precision, scale)` from a `decimal_arb` field, or error with a
/// caller-friendly message if the field is not `decimal_arb`.
fn require_decimal_arb_field(field: &arrow_schema::Field, op_name: &str) -> Result<(u32, u32)> {
    DecimalArbType::precision_scale_from_field(field).ok_or_else(|| {
        datafusion::error::DataFusionError::from(streamling_user_err!(
            "{}: input field '{}' is not a streamling.decimal_arb column",
            op_name,
            field.name(),
        ))
    })
}

/// Decode a `LargeBinaryArray` value at the given `scale`. Returns `None`
/// for nulls.
fn decode_value(
    array: &LargeBinaryArray,
    idx: usize,
    scale: u32,
) -> Result<Option<DecimalArbValue>> {
    if array.is_null(idx) {
        return Ok(None);
    }
    let bytes = array.value(idx);
    Ok(Some(DecimalArbValue::from_canonical_bytes_at_scale(
        bytes, scale,
    )?))
}

/// Build the output `Field` for a unary or binary `decimal_arb` op.
fn build_output_field(name: &str, precision: u32, scale: u32) -> Result<FieldRef> {
    let field = DecimalArbType::field(name, precision, scale, true)?;
    Ok(Arc::new(field))
}

/// Shared invoker for binary `decimal_arb` ops. Decodes inputs at their
/// column scales, applies `op_fn`, encodes the result at the output scale
/// (with half-to-even rounding for excess fractional digits), and emits a
/// `LargeBinaryArray`.
fn invoke_binary<O>(
    args: ScalarFunctionArgs,
    op_name: &'static str,
    kind: BinaryOpKind,
    op_fn: O,
) -> Result<ColumnarValue>
where
    O: Fn(&BigDecimal, &BigDecimal) -> Result<BigDecimal>,
{
    if args.args.len() != 2 {
        streamling_user_bail!("{} requires two arguments", op_name);
    }
    if args.arg_fields.len() != 2 {
        streamling_user_bail!("{} requires two input fields", op_name);
    }
    let (p1, s1) = require_decimal_arb_field(args.arg_fields[0].as_ref(), op_name)?;
    let (p2, s2) = require_decimal_arb_field(args.arg_fields[1].as_ref(), op_name)?;
    let (p_out, s_out) = output_precision_scale(kind, p1, s1, p2, s2);

    let left = downcast_decimal_arb_array(&args.args[0], op_name, "left")?;
    let right = downcast_decimal_arb_array(&args.args[1], op_name, "right")?;

    let len = left.len().max(right.len());
    let column = args.return_field.name();
    let mut builder = DecimalArbArrayBuilder::with_capacity(len, column, p_out, s_out)?;
    for i in 0..len {
        let li = if left.len() == 1 { 0 } else { i };
        let ri = if right.len() == 1 { 0 } else { i };
        let lhs = decode_value(&left, li, s1)?;
        let rhs = decode_value(&right, ri, s2)?;
        match (lhs, rhs) {
            (Some(a), Some(b)) => {
                let result = op_fn(a.as_bigdecimal(), b.as_bigdecimal())?;
                let rounded = result.with_scale_round(s_out as i64, RoundingMode::HalfEven);
                builder.append_value(&DecimalArbValue::from_bigdecimal(rounded))?;
            }
            _ => builder.append_null(),
        }
    }
    let array = builder.finish();
    let (raw, _, _) = array.into_inner();
    Ok(ColumnarValue::Array(Arc::new(raw)))
}

fn downcast_decimal_arb_array(
    cv: &ColumnarValue,
    op_name: &str,
    side: &str,
) -> Result<LargeBinaryArray> {
    let array = match cv {
        ColumnarValue::Array(arr) => arr.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array()?,
    };
    array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .cloned()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::from(streamling_user_err!(
                "{} expects LargeBinary input for {} operand (got {:?})",
                op_name,
                side,
                array.data_type()
            ))
        })
}

/// Macro-free expansion of a binary-op ScalarUDF. Takes a unique struct
/// name, the SQL function name, the kind for output-precision rules, and a
/// closure-style `BigDecimal × BigDecimal → BigDecimal` body.
macro_rules! decimal_arb_binary_op {
    ($struct_name:ident, $sql_name:literal, $kind:expr, $op:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $struct_name {
            signature: Signature,
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::one_of(
                        vec![TypeSignature::Exact(vec![
                            DataType::LargeBinary,
                            DataType::LargeBinary,
                        ])],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $struct_name {
            fn name(&self) -> &str {
                $sql_name
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::LargeBinary)
            }
            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                let (p1, s1) = require_decimal_arb_field(args.arg_fields[0].as_ref(), $sql_name)?;
                let (p2, s2) = require_decimal_arb_field(args.arg_fields[1].as_ref(), $sql_name)?;
                let (p_out, s_out) = output_precision_scale($kind, p1, s1, p2, s2);
                build_output_field(self.name(), p_out, s_out)
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                invoke_binary(args, $sql_name, $kind, $op)
            }
        }
    };
}

decimal_arb_binary_op!(
    DecimalArbAddFunc,
    "decimal_arb_add",
    BinaryOpKind::Add,
    |a: &BigDecimal, b: &BigDecimal| Ok(a + b)
);

decimal_arb_binary_op!(
    DecimalArbSubFunc,
    "decimal_arb_sub",
    BinaryOpKind::Sub,
    |a: &BigDecimal, b: &BigDecimal| Ok(a - b)
);

decimal_arb_binary_op!(
    DecimalArbMulFunc,
    "decimal_arb_mul",
    BinaryOpKind::Mul,
    |a: &BigDecimal, b: &BigDecimal| Ok(a * b)
);

decimal_arb_binary_op!(
    DecimalArbDivFunc,
    "decimal_arb_div",
    BinaryOpKind::Div,
    |a: &BigDecimal, b: &BigDecimal| {
        if b.is_zero() {
            return Err(datafusion::error::DataFusionError::from(
                streamling_user_err!("decimal_arb_div: division by zero"),
            ));
        }
        // Round to a generous intermediate scale; the outer invoke_binary then
        // rounds again to the output scale via with_scale_round (idempotent).
        let intermediate_scale = (DEFAULT_DIV_SCALE as i64) + (a.fractional_digit_count().max(0));
        Ok(a.with_scale_round(intermediate_scale, RoundingMode::HalfEven) / b)
    }
);

decimal_arb_binary_op!(
    DecimalArbModFunc,
    "decimal_arb_mod",
    BinaryOpKind::Mod,
    |a: &BigDecimal, b: &BigDecimal| {
        if b.is_zero() {
            return Err(datafusion::error::DataFusionError::from(
                streamling_user_err!("decimal_arb_mod: modulo by zero"),
            ));
        }
        Ok(a % b)
    }
);

// =====================================================================
// Unary ops: neg, abs
// =====================================================================

fn invoke_unary<O>(
    args: ScalarFunctionArgs,
    op_name: &'static str,
    op_fn: O,
) -> Result<ColumnarValue>
where
    O: Fn(&BigDecimal) -> BigDecimal,
{
    if args.args.len() != 1 {
        streamling_user_bail!("{} requires one argument", op_name);
    }
    let (p, s) = require_decimal_arb_field(args.arg_fields[0].as_ref(), op_name)?;
    let input = downcast_decimal_arb_array(&args.args[0], op_name, "input")?;
    let len = input.len();
    let column = args.return_field.name();
    let mut builder = DecimalArbArrayBuilder::with_capacity(len, column, p, s)?;
    for i in 0..len {
        match decode_value(&input, i, s)? {
            None => builder.append_null(),
            Some(v) => {
                let result = op_fn(v.as_bigdecimal());
                builder.append_value(&DecimalArbValue::from_bigdecimal(result))?;
            }
        }
    }
    let (raw, _, _) = builder.finish().into_inner();
    Ok(ColumnarValue::Array(Arc::new(raw)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecimalArbNegFunc {
    signature: Signature,
}

impl Default for DecimalArbNegFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbNegFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::LargeBinary])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DecimalArbNegFunc {
    fn name(&self) -> &str {
        "decimal_arb_neg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (p, s) = require_decimal_arb_field(args.arg_fields[0].as_ref(), self.name())?;
        build_output_field(self.name(), p, s)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_unary(args, "decimal_arb_neg", |v| -v.clone())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecimalArbAbsFunc {
    signature: Signature,
}

impl Default for DecimalArbAbsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbAbsFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::LargeBinary])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DecimalArbAbsFunc {
    fn name(&self) -> &str {
        "decimal_arb_abs"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (p, s) = require_decimal_arb_field(args.arg_fields[0].as_ref(), self.name())?;
        build_output_field(self.name(), p, s)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke_unary(args, "decimal_arb_abs", |v| v.abs())
    }
}

// `LargeBinaryBuilder` is referenced by tests below (inlining a quick array
// constructor). Re-export so the unused-import lint doesn't fire when the
// helper goes unused outside tests.
#[allow(unused_imports)]
use LargeBinaryBuilder as _;

// =====================================================================
// Comparison ops: eq, neq, lt, lte, gt, gte
//
// Each takes two `decimal_arb` columns and produces a `Boolean` column.
// NULL propagates per SQL three-valued logic (NULL OP X is NULL).
// Comparison ignores declared scale: `decimal_arb("1.0", scale=1)` and
// `decimal_arb("1.000", scale=3)` compare equal because both decode to
// the same BigDecimal.
// =====================================================================

fn invoke_compare<O>(
    args: ScalarFunctionArgs,
    op_name: &'static str,
    cmp_fn: O,
) -> Result<ColumnarValue>
where
    O: Fn(Ordering) -> bool,
{
    if args.args.len() != 2 {
        streamling_user_bail!("{} requires two arguments", op_name);
    }
    if args.arg_fields.len() != 2 {
        streamling_user_bail!("{} requires two input fields", op_name);
    }
    let (_, s1) = require_decimal_arb_field(args.arg_fields[0].as_ref(), op_name)?;
    let (_, s2) = require_decimal_arb_field(args.arg_fields[1].as_ref(), op_name)?;

    let left = downcast_decimal_arb_array(&args.args[0], op_name, "left")?;
    let right = downcast_decimal_arb_array(&args.args[1], op_name, "right")?;
    let len = left.len().max(right.len());

    let mut builder = BooleanBuilder::with_capacity(len);
    for i in 0..len {
        let li = if left.len() == 1 { 0 } else { i };
        let ri = if right.len() == 1 { 0 } else { i };
        match (decode_value(&left, li, s1)?, decode_value(&right, ri, s2)?) {
            (Some(a), Some(b)) => builder.append_value(cmp_fn(a.cmp(&b))),
            _ => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

macro_rules! decimal_arb_cmp_op {
    ($struct_name:ident, $sql_name:literal, $cmp:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $struct_name {
            signature: Signature,
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::one_of(
                        vec![TypeSignature::Exact(vec![
                            DataType::LargeBinary,
                            DataType::LargeBinary,
                        ])],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $struct_name {
            fn name(&self) -> &str {
                $sql_name
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Boolean)
            }
            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                // Both inputs must be decimal_arb (validated lazily here so
                // mis-routed calls fail at planning time with a clear error).
                require_decimal_arb_field(args.arg_fields[0].as_ref(), $sql_name)?;
                require_decimal_arb_field(args.arg_fields[1].as_ref(), $sql_name)?;
                Ok(Arc::new(arrow_schema::Field::new(
                    self.name(),
                    DataType::Boolean,
                    true,
                )))
            }
            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                invoke_compare(args, $sql_name, $cmp)
            }
        }
    };
}

decimal_arb_cmp_op!(DecimalArbEqFunc, "decimal_arb_eq", |o: Ordering| o
    == Ordering::Equal);
decimal_arb_cmp_op!(DecimalArbNeqFunc, "decimal_arb_neq", |o: Ordering| o
    != Ordering::Equal);
decimal_arb_cmp_op!(DecimalArbLtFunc, "decimal_arb_lt", |o: Ordering| o
    == Ordering::Less);
decimal_arb_cmp_op!(DecimalArbLteFunc, "decimal_arb_lte", |o: Ordering| o
    != Ordering::Greater);
decimal_arb_cmp_op!(DecimalArbGtFunc, "decimal_arb_gt", |o: Ordering| o
    == Ordering::Greater);
decimal_arb_cmp_op!(DecimalArbGteFunc, "decimal_arb_gte", |o: Ordering| o
    != Ordering::Less);

/// Convert a `streamling.decimal_arb` column to a `Utf8` column of canonical
/// decimal strings.
///
/// The function reads the column's declared `scale` from the input Field's
/// extension metadata (per `contracts/arrow-extension-type.md` §2). Without
/// the metadata the function errors — the caller must thread the metadata
/// through (see `postgres/projection.rs` for the typical wiring).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecimalArbToStringFunc {
    signature: Signature,
}

impl Default for DecimalArbToStringFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbToStringFunc {
    pub fn new() -> Self {
        Self {
            // Accept LargeBinary; the field-metadata check happens in
            // invoke_with_args because Signature can't introspect metadata.
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::LargeBinary])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DecimalArbToStringFunc {
    fn name(&self) -> &str {
        "decimal_arb_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Preserve the input column's nullability: converting a non-null
        // decimal_arb yields a non-null string, so a non-nullable source
        // column stays non-nullable (sink schema fidelity).
        let nullable = args.arg_fields.first().is_none_or(|f| f.is_nullable());
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::Utf8,
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            streamling_user_bail!("decimal_arb_to_string requires one argument");
        }
        let input_field = args.arg_fields.first().ok_or_else(|| {
            streamling_user_err!("decimal_arb_to_string: missing input field metadata")
        })?;
        let (_, scale) =
            DecimalArbType::precision_scale_from_field(input_field.as_ref()).ok_or_else(
                || {
                    streamling_user_err!(
                        "decimal_arb_to_string: input column '{}' is not a streamling.decimal_arb field \
                         (missing extension metadata or wrong storage type)",
                        input_field.name(),
                    )
                },
            )?;

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };
        let binary = array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                streamling_user_err!(
                    "decimal_arb_to_string expects LargeBinary input (got {:?})",
                    array.data_type()
                )
            })?;

        let mut builder = StringBuilder::with_capacity(binary.len(), binary.value_data().len());
        for i in 0..binary.len() {
            if binary.is_null(i) {
                builder.append_null();
                continue;
            }
            let bytes = binary.value(i);
            let value =
                DecimalArbValue::from_canonical_bytes_at_scale(bytes, scale).map_err(|e| {
                    streamling_user_err!("decimal_arb_to_string: failed at row {}: {}", i, e)
                })?;
            builder.append_value(value.to_canonical_string());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// =====================================================================
// Sort-key projection (T046)
//
// Bytewise sort on canonical decimal_arb bytes is *wrong* for negatives
// (sign byte 0xFF sorts after 0x00). The `decimal_arb_to_sort_key`
// ScalarUDF exposes the row-encoding helper as a SQL function, so authors
// can write
//
//     SELECT * FROM src ORDER BY decimal_arb_to_sort_key(amount)
//
// to get correct numeric order across signs. Wrapping `ORDER BY col`
// automatically (so authors don't write the function name) requires a
// LogicalPlan OptimizerRule and is documented as a follow-up — without
// the rewrite, plain `ORDER BY decimal_arb_col` will produce incorrect
// order for negatives.
// =====================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecimalArbSortKeyFunc {
    signature: Signature,
}

impl Default for DecimalArbSortKeyFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbSortKeyFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::LargeBinary])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DecimalArbSortKeyFunc {
    fn name(&self) -> &str {
        "decimal_arb_to_sort_key"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        require_decimal_arb_field(args.arg_fields[0].as_ref(), self.name())?;
        // Output is plain LargeBinary (no decimal_arb metadata) — it's a
        // sort key, not a numeric value.
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::LargeBinary,
            true,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            streamling_user_bail!("decimal_arb_to_sort_key requires one argument");
        }
        require_decimal_arb_field(args.arg_fields[0].as_ref(), self.name())?;
        let input = downcast_decimal_arb_array(&args.args[0], self.name(), "input")?;
        let mut builder = LargeBinaryBuilder::with_capacity(input.len(), input.value_data().len());
        for i in 0..input.len() {
            if input.is_null(i) {
                builder.append_null();
            } else {
                let key = crate::types::decimal_arb::decimal_arb_to_sort_key(input.value(i));
                builder.append_value(&key);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// =====================================================================
// Cast UDFs (US4 / T068)
//
// Widening (always lossless):
//   to_decimal_arb_from_string(text, precision, scale) -> decimal_arb(p, s)
//   to_decimal_arb_from_decimal128(value)              -> decimal_arb(p, s)  // (p, s) inherited
//   to_decimal_arb_from_decimal256(value)              -> decimal_arb(p, s)  // (p, s) inherited
//
// Narrowing (half-to-even rounding for excess scale; FR-013 error on
// out-of-range integer digits):
//   decimal_arb_to_decimal128(value, precision, scale) -> Decimal128(p, s)
//   decimal_arb_to_decimal256(value, precision, scale) -> Decimal256(p, s)
//
// `decimal_arb_to_string` ships with US1 (T027) — no need to add a cast.
//
// Float and Int8/16/32 directions remain to be wrapped on demand;
// the DecimalArbArray helpers (T011) already exist for them.
// =====================================================================

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToDecimalArbFromStringFunc {
    signature: Signature,
}

impl Default for ToDecimalArbFromStringFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToDecimalArbFromStringFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Int64,
                ])],
                Volatility::Immutable,
            ),
        }
    }

    fn read_literal(value: &ColumnarValue, name: &str) -> Result<u32> {
        let v = match value {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Int64(Some(v))) => *v,
            other => {
                return Err(datafusion::error::DataFusionError::from(
                    streamling_user_err!(
                        "to_decimal_arb_from_string: {} must be an Int64 literal (got {:?})",
                        name,
                        other,
                    ),
                ));
            }
        };
        if v < 0 {
            return Err(datafusion::error::DataFusionError::from(
                streamling_user_err!(
                    "to_decimal_arb_from_string: {} must be non-negative (got {})",
                    name,
                    v
                ),
            ));
        }
        Ok(v as u32)
    }

    fn read_literal_arg(args: &ScalarFunctionArgs, idx: usize, name: &str) -> Result<u32> {
        Self::read_literal(&args.args[idx], name)
    }

    fn read_return_field_literal(args: &ReturnFieldArgs, idx: usize, name: &str) -> Result<u32> {
        match args.scalar_arguments.get(idx).copied().flatten() {
            Some(datafusion::scalar::ScalarValue::Int64(Some(v))) => {
                if *v < 0 {
                    Err(datafusion::error::DataFusionError::from(
                        streamling_user_err!(
                            "to_decimal_arb_from_string: {} must be non-negative (got {})",
                            name,
                            v
                        ),
                    ))
                } else {
                    Ok(*v as u32)
                }
            }
            _ => Err(datafusion::error::DataFusionError::from(
                streamling_user_err!(
                    "to_decimal_arb_from_string: {} must be a non-negative Int64 literal at planning time",
                    name,
                ),
            )),
        }
    }
}

impl ScalarUDFImpl for ToDecimalArbFromStringFunc {
    fn name(&self) -> &str {
        "to_decimal_arb_from_string"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let precision = Self::read_return_field_literal(&args, 1, "precision")?;
        let scale = Self::read_return_field_literal(&args, 2, "scale")?;
        build_output_field(self.name(), precision, scale)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            streamling_user_bail!("to_decimal_arb_from_string requires (text, precision, scale)");
        }
        let precision = Self::read_literal_arg(&args, 1, "precision")?;
        let scale = Self::read_literal_arg(&args, 2, "scale")?;

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };
        let strings = array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "to_decimal_arb_from_string: text input must be Utf8 (got {:?})",
                    array.data_type()
                ))
            })?;

        let column = args.return_field.name();
        let mut builder =
            DecimalArbArrayBuilder::with_capacity(strings.len(), column, precision, scale)?;
        for i in 0..strings.len() {
            if strings.is_null(i) {
                builder.append_null();
            } else {
                builder.append_str(strings.value(i))?;
            }
        }
        let (raw, _, _) = builder.finish().into_inner();
        Ok(ColumnarValue::Array(Arc::new(raw)))
    }
}

// ---------- Widening: from_decimal128 / from_decimal256 ----------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToDecimalArbFromDecimal128Func {
    signature: Signature,
}

impl Default for ToDecimalArbFromDecimal128Func {
    fn default() -> Self {
        Self::new()
    }
}

impl ToDecimalArbFromDecimal128Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn input_decimal128_precision_scale(
    field: &arrow_schema::Field,
    op_name: &str,
) -> Result<(u8, i8)> {
    match field.data_type() {
        DataType::Decimal128(p, s) => Ok((*p, *s)),
        other => Err(datafusion::error::DataFusionError::from(
            streamling_user_err!("{}: input must be Decimal128 (got {:?})", op_name, other,),
        )),
    }
}

fn input_decimal256_precision_scale(
    field: &arrow_schema::Field,
    op_name: &str,
) -> Result<(u8, i8)> {
    match field.data_type() {
        DataType::Decimal256(p, s) => Ok((*p, *s)),
        other => Err(datafusion::error::DataFusionError::from(
            streamling_user_err!("{}: input must be Decimal256 (got {:?})", op_name, other,),
        )),
    }
}

fn nonneg_scale(scale: i8, op_name: &str) -> Result<u32> {
    if scale < 0 {
        return Err(datafusion::error::DataFusionError::from(
            streamling_user_err!(
                "{}: input scale must be non-negative for decimal_arb (got {})",
                op_name,
                scale,
            ),
        ));
    }
    Ok(scale as u32)
}

impl ScalarUDFImpl for ToDecimalArbFromDecimal128Func {
    fn name(&self) -> &str {
        "to_decimal_arb_from_decimal128"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            streamling_user_bail!("to_decimal_arb_from_decimal128 requires one argument");
        }
        match &arg_types[0] {
            DataType::Decimal128(_, _) => Ok(arg_types.to_vec()),
            other => Err(datafusion::error::DataFusionError::from(
                streamling_user_err!(
                    "to_decimal_arb_from_decimal128 requires Decimal128 input (got {:?})",
                    other,
                ),
            )),
        }
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (p, s) = input_decimal128_precision_scale(args.arg_fields[0].as_ref(), self.name())?;
        let s = nonneg_scale(s, self.name())?;
        build_output_field(self.name(), p as u32, s)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            streamling_user_bail!("to_decimal_arb_from_decimal128 requires one argument");
        }
        let (p, s) = input_decimal128_precision_scale(args.arg_fields[0].as_ref(), self.name())?;
        let s = nonneg_scale(s, self.name())?;
        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };
        let dec = array
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "to_decimal_arb_from_decimal128: expected Decimal128Array (got {:?})",
                    array.data_type()
                ))
            })?;
        let column = args.return_field.name();
        let out = crate::types::decimal_arb::DecimalArbArray::from_decimal128(
            dec, s as i8, p as u32, s, column,
        )?;
        let (raw, _, _) = out.into_inner();
        Ok(ColumnarValue::Array(Arc::new(raw)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToDecimalArbFromDecimal256Func {
    signature: Signature,
}

impl Default for ToDecimalArbFromDecimal256Func {
    fn default() -> Self {
        Self::new()
    }
}

impl ToDecimalArbFromDecimal256Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToDecimalArbFromDecimal256Func {
    fn name(&self) -> &str {
        "to_decimal_arb_from_decimal256"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            streamling_user_bail!("to_decimal_arb_from_decimal256 requires one argument");
        }
        match &arg_types[0] {
            DataType::Decimal256(_, _) => Ok(arg_types.to_vec()),
            other => Err(datafusion::error::DataFusionError::from(
                streamling_user_err!(
                    "to_decimal_arb_from_decimal256 requires Decimal256 input (got {:?})",
                    other,
                ),
            )),
        }
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (p, s) = input_decimal256_precision_scale(args.arg_fields[0].as_ref(), self.name())?;
        let s = nonneg_scale(s, self.name())?;
        build_output_field(self.name(), p as u32, s)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            streamling_user_bail!("to_decimal_arb_from_decimal256 requires one argument");
        }
        let (p, s) = input_decimal256_precision_scale(args.arg_fields[0].as_ref(), self.name())?;
        let s = nonneg_scale(s, self.name())?;
        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };
        let dec = array
            .as_any()
            .downcast_ref::<arrow::array::Decimal256Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "to_decimal_arb_from_decimal256: expected Decimal256Array (got {:?})",
                    array.data_type()
                ))
            })?;
        let column = args.return_field.name();
        let out = crate::types::decimal_arb::DecimalArbArray::from_decimal256(
            dec, s as i8, p as u32, s, column,
        )?;
        let (raw, _, _) = out.into_inner();
        Ok(ColumnarValue::Array(Arc::new(raw)))
    }
}

// ---------- Widening: from_int (Int8/16/32/64/UInt8/16/32/64) ----------

/// `to_decimal_arb_from_int(value, precision_lit, scale_lit)` — converts
/// any signed/unsigned integer column into `decimal_arb(p, s)`. Lossless
/// when the value's magnitude fits the declared precision (FR-013 error
/// otherwise). The integer is treated as having scale 0; declared `scale`
/// just becomes the column's storage scale (the value is padded
/// internally on encoding).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToDecimalArbFromIntFunc {
    signature: Signature,
}

impl Default for ToDecimalArbFromIntFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToDecimalArbFromIntFunc {
    pub fn new() -> Self {
        let int_kinds = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ];
        Self {
            signature: Signature::one_of(
                int_kinds
                    .into_iter()
                    .map(|t| TypeSignature::Exact(vec![t, DataType::Int64, DataType::Int64]))
                    .collect(),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ToDecimalArbFromIntFunc {
    fn name(&self) -> &str {
        "to_decimal_arb_from_int"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let precision = ToDecimalArbFromStringFunc::read_return_field_literal(
            &args,
            1,
            "to_decimal_arb_from_int",
        )?;
        let scale = ToDecimalArbFromStringFunc::read_return_field_literal(
            &args,
            2,
            "to_decimal_arb_from_int",
        )?;
        build_output_field(self.name(), precision, scale)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            streamling_user_bail!("to_decimal_arb_from_int requires (value, precision, scale)");
        }
        let precision = ToDecimalArbFromStringFunc::read_literal_arg(&args, 1, "precision")?;
        let scale = ToDecimalArbFromStringFunc::read_literal_arg(&args, 2, "scale")?;

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };
        let column = args.return_field.name();
        let mut builder =
            DecimalArbArrayBuilder::with_capacity(array.len(), column, precision, scale)?;

        let push_i128 = |builder: &mut DecimalArbArrayBuilder, v: i128| -> Result<()> {
            let value = DecimalArbValue::from_bigint_and_scale(num_bigint::BigInt::from(v), 0);
            builder
                .append_value(&value)
                .map_err(datafusion::error::DataFusionError::from)
        };

        macro_rules! push_int_array {
            ($arr_ty:ty, $cast:expr) => {{
                let arr = array.as_any().downcast_ref::<$arr_ty>().ok_or_else(|| {
                    datafusion::error::DataFusionError::from(streamling_user_err!(
                        "to_decimal_arb_from_int: expected {} input (got {:?})",
                        stringify!($arr_ty),
                        array.data_type()
                    ))
                })?;
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        builder.append_null();
                    } else {
                        push_i128(&mut builder, $cast(arr.value(i)))?;
                    }
                }
            }};
        }

        match array.data_type() {
            DataType::Int8 => push_int_array!(arrow::array::Int8Array, |v: i8| v as i128),
            DataType::Int16 => push_int_array!(arrow::array::Int16Array, |v: i16| v as i128),
            DataType::Int32 => push_int_array!(arrow::array::Int32Array, |v: i32| v as i128),
            DataType::Int64 => push_int_array!(arrow::array::Int64Array, |v: i64| v as i128),
            DataType::UInt8 => push_int_array!(arrow::array::UInt8Array, |v: u8| v as i128),
            DataType::UInt16 => push_int_array!(arrow::array::UInt16Array, |v: u16| v as i128),
            DataType::UInt32 => push_int_array!(arrow::array::UInt32Array, |v: u32| v as i128),
            DataType::UInt64 => push_int_array!(arrow::array::UInt64Array, |v: u64| v as i128),
            other => {
                streamling_user_bail!(
                    "to_decimal_arb_from_int: unsupported input type {:?}",
                    other
                );
            }
        };

        let (raw, _, _) = builder.finish().into_inner();
        Ok(ColumnarValue::Array(Arc::new(raw)))
    }
}

// ---------- Narrowing: decimal_arb_to_decimal128 / 256 ----------

fn read_target_precision_scale(args: &ScalarFunctionArgs, op_name: &str) -> Result<(u8, i8)> {
    let p = match &args.args[1] {
        ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Int64(Some(v))) if *v >= 0 => *v,
        other => {
            return Err(datafusion::error::DataFusionError::from(
                streamling_user_err!(
                    "{}: target precision must be a non-negative Int64 literal (got {:?})",
                    op_name,
                    other,
                ),
            ));
        }
    };
    let s = match &args.args[2] {
        ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Int64(Some(v))) => *v,
        other => {
            return Err(datafusion::error::DataFusionError::from(
                streamling_user_err!(
                    "{}: target scale must be an Int64 literal (got {:?})",
                    op_name,
                    other,
                ),
            ));
        }
    };
    if p == 0 || p > i8::MAX as i64 {
        return Err(datafusion::error::DataFusionError::from(
            streamling_user_err!("{}: target precision out of range (got {})", op_name, p,),
        ));
    }
    if s < i8::MIN as i64 || s > i8::MAX as i64 {
        return Err(datafusion::error::DataFusionError::from(
            streamling_user_err!("{}: target scale out of range (got {})", op_name, s,),
        ));
    }
    Ok((p as u8, s as i8))
}

fn read_return_field_precision_scale(args: &ReturnFieldArgs, op_name: &str) -> Result<(u8, i8)> {
    let p = match args.scalar_arguments.get(1).copied().flatten() {
        Some(datafusion::scalar::ScalarValue::Int64(Some(v))) if *v > 0 && *v <= i8::MAX as i64 => {
            *v
        }
        _ => {
            return Err(datafusion::error::DataFusionError::from(
                streamling_user_err!(
                    "{}: target precision must be a positive Int64 literal at planning time",
                    op_name,
                ),
            ));
        }
    };
    let s = match args.scalar_arguments.get(2).copied().flatten() {
        Some(datafusion::scalar::ScalarValue::Int64(Some(v)))
            if *v >= i8::MIN as i64 && *v <= i8::MAX as i64 =>
        {
            *v
        }
        _ => {
            return Err(datafusion::error::DataFusionError::from(
                streamling_user_err!(
                    "{}: target scale must be an Int64 literal at planning time",
                    op_name,
                ),
            ));
        }
    };
    Ok((p as u8, s as i8))
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecimalArbToDecimal128Func {
    signature: Signature,
}

impl Default for DecimalArbToDecimal128Func {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbToDecimal128Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::LargeBinary,
                    DataType::Int64,
                    DataType::Int64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DecimalArbToDecimal128Func {
    fn name(&self) -> &str {
        "decimal_arb_to_decimal128"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Default; the real shape comes from return_field_from_args.
        Ok(DataType::Decimal128(38, 0))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (p, s) = read_return_field_precision_scale(&args, self.name())?;
        let nullable = args.arg_fields.first().is_none_or(|f| f.is_nullable());
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::Decimal128(p, s),
            nullable,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            streamling_user_bail!("decimal_arb_to_decimal128 requires (value, precision, scale)");
        }
        let (target_p, target_s) = read_target_precision_scale(&args, self.name())?;
        let input_field = args.arg_fields[0].as_ref();
        let (_, input_scale) = require_decimal_arb_field(input_field, self.name())?;

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };
        let lba = array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "{}: expected LargeBinary input",
                    self.name()
                ))
            })?;
        // Adopt as DecimalArbArray with the input scale and call to_decimal128.
        let column = args.return_field.name();
        let arb = crate::types::decimal_arb::DecimalArbArray::try_from_array_and_field(
            lba.clone(),
            input_field,
        )?;
        let _ = input_scale; // already known from try_from_array_and_field
        let out = arb.to_decimal128(target_p, target_s, column)?;
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DecimalArbToDecimal256Func {
    signature: Signature,
}

impl Default for DecimalArbToDecimal256Func {
    fn default() -> Self {
        Self::new()
    }
}

impl DecimalArbToDecimal256Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![
                    DataType::LargeBinary,
                    DataType::Int64,
                    DataType::Int64,
                ])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DecimalArbToDecimal256Func {
    fn name(&self) -> &str {
        "decimal_arb_to_decimal256"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal256(76, 0))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (p, s) = read_return_field_precision_scale(&args, self.name())?;
        let nullable = args.arg_fields.first().is_none_or(|f| f.is_nullable());
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::Decimal256(p, s),
            nullable,
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            streamling_user_bail!("decimal_arb_to_decimal256 requires (value, precision, scale)");
        }
        let (target_p, target_s) = read_target_precision_scale(&args, self.name())?;
        let input_field = args.arg_fields[0].as_ref();
        require_decimal_arb_field(input_field, self.name())?;

        let array = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };
        let lba = array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::from(streamling_user_err!(
                    "{}: expected LargeBinary input",
                    self.name()
                ))
            })?;
        let column = args.return_field.name();
        let arb = crate::types::decimal_arb::DecimalArbArray::try_from_array_and_field(
            lba.clone(),
            input_field,
        )?;
        let out = arb.to_decimal256(target_p, target_s, column)?;
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decimal_arb::DecimalArbArrayBuilder;
    use arrow::array::{BooleanArray, Decimal128Array, Decimal256Array, StringArray};
    use arrow_schema::Field;
    use std::str::FromStr;

    fn build_array(precision: u32, scale: u32, values: &[Option<&str>]) -> LargeBinaryArray {
        let mut b =
            DecimalArbArrayBuilder::with_capacity(values.len(), "x", precision, scale).unwrap();
        for v in values {
            match v {
                Some(s) => b.append_str(s).unwrap(),
                None => b.append_null(),
            }
        }
        let (raw, _, _) = b.finish().into_inner();
        raw
    }

    #[test]
    fn renders_canonical_decimal_strings() {
        let arr = build_array(100, 4, &[Some("12.3456"), None, Some("-0.0001"), Some("0")]);
        let field = DecimalArbType::field("amount", 100, 4, true).unwrap();
        let func = DecimalArbToStringFunc::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(arr))],
            arg_fields: vec![Arc::new(field.clone())],
            number_rows: 4,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = func.invoke_with_args(args).unwrap();
        let strings = match out {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("expected array"),
        };
        let s = strings.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "12.3456");
        assert!(s.is_null(1));
        assert_eq!(s.value(2), "-0.0001");
        assert_eq!(s.value(3), "0");
    }

    #[test]
    fn rejects_non_decimal_arb_field() {
        let arr = LargeBinaryArray::from_iter_values([&[0x00u8, 0x01u8] as &[u8]]);
        let field = Field::new("x", DataType::LargeBinary, true); // no metadata
        let func = DecimalArbToStringFunc::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(arr))],
            arg_fields: vec![Arc::new(field)],
            number_rows: 1,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        assert!(func.invoke_with_args(args).is_err());
    }

    // ------- T034 / T042: arithmetic UDFs -------

    fn build_decimal_arb_array(
        precision: u32,
        scale: u32,
        values: &[Option<&str>],
    ) -> LargeBinaryArray {
        let mut b =
            DecimalArbArrayBuilder::with_capacity(values.len(), "x", precision, scale).unwrap();
        for v in values {
            match v {
                Some(s) => b.append_str(s).unwrap(),
                None => b.append_null(),
            }
        }
        let (raw, _, _) = b.finish().into_inner();
        raw
    }

    fn invoke_binary_op(
        func: &dyn ScalarUDFImpl,
        lhs: (LargeBinaryArray, u32, u32, &str),
        rhs: (LargeBinaryArray, u32, u32, &str),
    ) -> Result<(LargeBinaryArray, u32, u32)> {
        let (lhs_arr, p1, s1, lhs_name) = lhs;
        let (rhs_arr, p2, s2, rhs_name) = rhs;
        let lhs_field = DecimalArbType::field(lhs_name, p1, s1, true).unwrap();
        let rhs_field = DecimalArbType::field(rhs_name, p2, s2, true).unwrap();
        let arg_fields = vec![Arc::new(lhs_field.clone()), Arc::new(rhs_field.clone())];
        // Compute return field via the UDF itself.
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        };
        let return_field = func.return_field_from_args(ret_args).unwrap();
        let (p_out, s_out) =
            DecimalArbType::precision_scale_from_field(return_field.as_ref()).unwrap();
        let n = lhs_arr.len().max(rhs_arr.len());
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(lhs_arr)),
                ColumnarValue::Array(Arc::new(rhs_arr)),
            ],
            arg_fields,
            number_rows: n,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = func.invoke_with_args(args)?;
        let arr = match out {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("expected array"),
        };
        Ok((
            arr.as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .clone(),
            p_out,
            s_out,
        ))
    }

    fn assert_eq_at(arr: &LargeBinaryArray, idx: usize, scale: u32, expected: &str) {
        let v = DecimalArbValue::from_canonical_bytes_at_scale(arr.value(idx), scale).unwrap();
        assert_eq!(
            v,
            DecimalArbValue::from_str(expected).unwrap(),
            "row {idx}: got {v}, expected {expected}"
        );
    }

    #[test]
    fn cast_udfs_preserve_input_nullability() {
        // Converting a non-null decimal_arb yields a non-null result, so the
        // ClickHouse/Postgres sink column keeps the source's nullability
        // (a non-nullable column must not silently become Nullable).
        for nullable in [true, false] {
            let input = Arc::new(DecimalArbType::field("v", 50, 10, nullable).unwrap());
            let args = ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&input),
                scalar_arguments: &[None],
            };
            let to_string = DecimalArbToStringFunc::new()
                .return_field_from_args(args)
                .unwrap();
            assert_eq!(
                to_string.is_nullable(),
                nullable,
                "decimal_arb_to_string must mirror input nullability"
            );

            // The Decimal casts take (value, precision, scale) literals.
            use datafusion::common::ScalarValue;
            let p = ScalarValue::Int64(Some(50));
            let s = ScalarValue::Int64(Some(10));
            let dec_args = ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&input),
                scalar_arguments: &[None, Some(&p), Some(&s)],
            };
            let to_dec128 = DecimalArbToDecimal128Func::new()
                .return_field_from_args(dec_args)
                .unwrap();
            assert_eq!(
                to_dec128.is_nullable(),
                nullable,
                "decimal_arb_to_decimal128 must mirror input nullability"
            );
        }
    }

    #[test]
    fn mul_caps_scale_at_capped_precision() {
        // When p1 + p2 + 1 overflows MAX_PRECISION, p_out is capped. The
        // scale (s1 + s2) must be capped to p_out too, otherwise
        // validate_precision_scale rejects scale > precision and a
        // representable product turns into a runtime error.
        let (p_out, s_out) = output_precision_scale(BinaryOpKind::Mul, 40000, 33000, 40000, 33000);
        assert_eq!(p_out, MAX_PRECISION, "precision should be capped");
        assert_eq!(s_out, p_out, "scale must be capped to the capped precision");
        assert!(
            s_out <= p_out,
            "scale ({s_out}) must not exceed precision ({p_out})"
        );
        // The capped (precision, scale) pair must itself be valid — building
        // a field runs the same precision/scale validation the op output does.
        DecimalArbType::field("mul_out", p_out, s_out, true)
            .expect("capped (p, s) must be a valid decimal_arb field");
    }

    #[test]
    fn add_widens_precision_and_keeps_max_scale() {
        let lhs = build_decimal_arb_array(100, 18, &[Some("12.34"), None, Some("-1")]);
        let rhs = build_decimal_arb_array(80, 20, &[Some("0.01"), Some("9"), None]);
        let func = DecimalArbAddFunc::new();
        let (out, p_out, s_out) =
            invoke_binary_op(&func, (lhs, 100, 18, "a"), (rhs, 80, 20, "b")).unwrap();
        // add rule: max(p1-s1, p2-s2) + max(s1,s2) + 1
        // = max(82, 60) + 20 + 1 = 103 → cap at 65535 → 103
        assert_eq!((p_out, s_out), (103, 20));
        assert_eq_at(&out, 0, s_out, "12.35");
        assert!(out.is_null(1)); // NULL propagates
        assert!(out.is_null(2));
    }

    #[test]
    fn sub_works_and_preserves_signs() {
        let lhs = build_decimal_arb_array(50, 5, &[Some("100"), Some("-50.5")]);
        let rhs = build_decimal_arb_array(50, 5, &[Some("99.5"), Some("-100")]);
        let func = DecimalArbSubFunc::new();
        let (out, _p, s) = invoke_binary_op(&func, (lhs, 50, 5, "a"), (rhs, 50, 5, "b")).unwrap();
        assert_eq_at(&out, 0, s, "0.5");
        assert_eq_at(&out, 1, s, "49.5");
    }

    #[test]
    fn mul_widens_precision_and_sums_scale() {
        let lhs = build_decimal_arb_array(50, 5, &[Some("1.5")]);
        let rhs = build_decimal_arb_array(60, 10, &[Some("2.5")]);
        let func = DecimalArbMulFunc::new();
        let (out, p_out, s_out) =
            invoke_binary_op(&func, (lhs, 50, 5, "a"), (rhs, 60, 10, "b")).unwrap();
        // mul rule: p1+p2+1 = 111, s1+s2 = 15
        assert_eq!((p_out, s_out), (111, 15));
        assert_eq_at(&out, 0, s_out, "3.75");
    }

    #[test]
    fn div_uses_default_scale_18_and_half_even_rounding() {
        let lhs = build_decimal_arb_array(80, 0, &[Some("1"), Some("10"), Some("-1")]);
        let rhs = build_decimal_arb_array(80, 0, &[Some("3"), Some("4"), Some("3")]);
        let func = DecimalArbDivFunc::new();
        let (out, _p, s_out) =
            invoke_binary_op(&func, (lhs, 80, 0, "a"), (rhs, 80, 0, "b")).unwrap();
        // div rule: s_out = max(s1, 18) = 18
        assert_eq!(s_out, 18);
        assert_eq_at(&out, 0, s_out, "0.333333333333333333");
        assert_eq_at(&out, 1, s_out, "2.5");
        assert_eq_at(&out, 2, s_out, "-0.333333333333333333");
    }

    #[test]
    fn div_by_zero_errors() {
        let lhs = build_decimal_arb_array(10, 0, &[Some("10")]);
        let rhs = build_decimal_arb_array(10, 0, &[Some("0")]);
        let func = DecimalArbDivFunc::new();
        let res = invoke_binary_op(&func, (lhs, 10, 0, "a"), (rhs, 10, 0, "b"));
        match res {
            Err(e) => assert!(format!("{}", e).contains("division by zero")),
            Ok(_) => panic!("expected division by zero error"),
        }
    }

    #[test]
    fn mod_returns_signed_remainder() {
        let lhs = build_decimal_arb_array(10, 0, &[Some("10"), Some("-10")]);
        let rhs = build_decimal_arb_array(10, 0, &[Some("3"), Some("3")]);
        let func = DecimalArbModFunc::new();
        let (out, _p, s_out) =
            invoke_binary_op(&func, (lhs, 10, 0, "a"), (rhs, 10, 0, "b")).unwrap();
        assert_eq_at(&out, 0, s_out, "1");
        assert_eq_at(&out, 1, s_out, "-1");
    }

    #[test]
    fn neg_flips_sign_and_preserves_value() {
        let arr = build_decimal_arb_array(10, 2, &[Some("12.34"), Some("-5"), Some("0"), None]);
        let func = DecimalArbNegFunc::new();
        let field = DecimalArbType::field("x", 10, 2, true).unwrap();
        let arg_fields = vec![Arc::new(field.clone())];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        };
        let return_field = func.return_field_from_args(ret_args).unwrap();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(arr))],
            arg_fields,
            number_rows: 4,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = match func.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let lba = out.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq_at(lba, 0, 2, "-12.34");
        assert_eq_at(lba, 1, 2, "5");
        assert_eq_at(lba, 2, 2, "0");
        assert!(lba.is_null(3));
    }

    #[test]
    fn abs_clears_sign() {
        let arr = build_decimal_arb_array(10, 2, &[Some("-12.34"), Some("5"), Some("-0")]);
        let func = DecimalArbAbsFunc::new();
        let field = DecimalArbType::field("x", 10, 2, true).unwrap();
        let arg_fields = vec![Arc::new(field.clone())];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        };
        let return_field = func.return_field_from_args(ret_args).unwrap();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(arr))],
            arg_fields,
            number_rows: 3,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = match func.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let lba = out.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq_at(lba, 0, 2, "12.34");
        assert_eq_at(lba, 1, 2, "5");
        assert_eq_at(lba, 2, 2, "0");
    }

    // ------- T035 / T043: comparison UDFs -------

    fn invoke_cmp(
        func: &dyn ScalarUDFImpl,
        lhs: (LargeBinaryArray, u32, u32, &str),
        rhs: (LargeBinaryArray, u32, u32, &str),
    ) -> BooleanArray {
        let (lhs_arr, p1, s1, lhs_name) = lhs;
        let (rhs_arr, p2, s2, rhs_name) = rhs;
        let lhs_field = DecimalArbType::field(lhs_name, p1, s1, true).unwrap();
        let rhs_field = DecimalArbType::field(rhs_name, p2, s2, true).unwrap();
        let arg_fields = vec![Arc::new(lhs_field), Arc::new(rhs_field)];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        };
        let return_field = func.return_field_from_args(ret_args).unwrap();
        let n = lhs_arr.len().max(rhs_arr.len());
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(lhs_arr)),
                ColumnarValue::Array(Arc::new(rhs_arr)),
            ],
            arg_fields,
            number_rows: n,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = func.invoke_with_args(args).unwrap();
        match out {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("comparison must return BooleanArray")
                .clone(),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn eq_treats_canonically_equal_values_equal() {
        // 1.0 (scale 1) and 1.000 (scale 3) decode to the same BigDecimal.
        let lhs = build_decimal_arb_array(10, 1, &[Some("1.0"), Some("0"), None]);
        let rhs = build_decimal_arb_array(10, 3, &[Some("1.000"), Some("0.000"), Some("1")]);
        let out = invoke_cmp(
            &DecimalArbEqFunc::new(),
            (lhs, 10, 1, "a"),
            (rhs, 10, 3, "b"),
        );
        assert!(out.value(0));
        assert!(out.value(1));
        assert!(out.is_null(2));
    }

    #[test]
    fn neq_complements_eq() {
        let lhs = build_decimal_arb_array(10, 0, &[Some("1"), Some("2"), None]);
        let rhs = build_decimal_arb_array(10, 0, &[Some("1"), Some("3"), Some("0")]);
        let out = invoke_cmp(
            &DecimalArbNeqFunc::new(),
            (lhs, 10, 0, "a"),
            (rhs, 10, 0, "b"),
        );
        assert!(!out.value(0));
        assert!(out.value(1));
        assert!(out.is_null(2));
    }

    #[test]
    fn ordering_works_across_signs() {
        // i256-style negative-sort regression guard at the comparison-UDF
        // layer: -100 < -1 < 0 < 1 < 100.
        let lhs = build_decimal_arb_array(10, 0, &[Some("-100"), Some("-1"), Some("0"), Some("1")]);
        let rhs = build_decimal_arb_array(10, 0, &[Some("-1"), Some("0"), Some("1"), Some("100")]);
        let out = invoke_cmp(
            &DecimalArbLtFunc::new(),
            (lhs, 10, 0, "a"),
            (rhs, 10, 0, "b"),
        );
        for i in 0..4 {
            assert!(
                out.value(i),
                "lhs[{i}] should be less than rhs[{i}] (signed ordering)"
            );
        }
    }

    #[test]
    fn lte_includes_equality() {
        let lhs = build_decimal_arb_array(10, 2, &[Some("1.5"), Some("1.5"), Some("2.0")]);
        let rhs = build_decimal_arb_array(10, 2, &[Some("1.5"), Some("2.0"), Some("1.5")]);
        let out = invoke_cmp(
            &DecimalArbLteFunc::new(),
            (lhs, 10, 2, "a"),
            (rhs, 10, 2, "b"),
        );
        assert!(out.value(0)); // 1.5 <= 1.5
        assert!(out.value(1)); // 1.5 <= 2.0
        assert!(!out.value(2)); // 2.0 > 1.5
    }

    #[test]
    fn gt_complements_lte() {
        let lhs = build_decimal_arb_array(10, 0, &[Some("5"), Some("3")]);
        let rhs = build_decimal_arb_array(10, 0, &[Some("3"), Some("5")]);
        let out = invoke_cmp(
            &DecimalArbGtFunc::new(),
            (lhs, 10, 0, "a"),
            (rhs, 10, 0, "b"),
        );
        assert!(out.value(0));
        assert!(!out.value(1));
    }

    #[test]
    fn gte_includes_equality() {
        let lhs = build_decimal_arb_array(10, 0, &[Some("5"), Some("5")]);
        let rhs = build_decimal_arb_array(10, 0, &[Some("5"), Some("3")]);
        let out = invoke_cmp(
            &DecimalArbGteFunc::new(),
            (lhs, 10, 0, "a"),
            (rhs, 10, 0, "b"),
        );
        assert!(out.value(0));
        assert!(out.value(1));
    }

    #[test]
    fn comparison_returns_null_for_either_null_operand() {
        // SQL three-valued logic: NULL = X, X = NULL, NULL = NULL all return NULL.
        let lhs = build_decimal_arb_array(10, 0, &[None, Some("1"), None]);
        let rhs = build_decimal_arb_array(10, 0, &[Some("1"), None, None]);
        let out = invoke_cmp(
            &DecimalArbEqFunc::new(),
            (lhs, 10, 0, "a"),
            (rhs, 10, 0, "b"),
        );
        for i in 0..3 {
            assert!(out.is_null(i), "row {i} must be NULL");
        }
    }

    #[test]
    fn comparison_rejects_non_decimal_arb_input_field() {
        let lhs_field = Field::new("x", DataType::LargeBinary, true);
        let rhs_field = DecimalArbType::field("y", 10, 0, true).unwrap();
        let arg_fields = vec![Arc::new(lhs_field), Arc::new(rhs_field)];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        };
        assert!(
            DecimalArbEqFunc::new()
                .return_field_from_args(ret_args)
                .is_err()
        );
    }

    // ------- T068: cast UDFs (minimal slice) -------

    #[test]
    fn to_decimal_arb_from_string_parses_at_declared_precision_scale() {
        let strings = StringArray::from(vec![Some("12.34"), None, Some("-99.5"), Some("0")]);
        let precision = datafusion::scalar::ScalarValue::Int64(Some(20));
        let scale = datafusion::scalar::ScalarValue::Int64(Some(2));

        let func = ToDecimalArbFromStringFunc::new();
        let arg_fields = vec![
            Arc::new(Field::new("text", DataType::Utf8, true)),
            Arc::new(Field::new("p", DataType::Int64, false)),
            Arc::new(Field::new("s", DataType::Int64, false)),
        ];
        let scalar_arguments: Vec<Option<&datafusion::scalar::ScalarValue>> =
            vec![None, Some(&precision), Some(&scale)];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        };
        let return_field = func.return_field_from_args(ret_args).unwrap();

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(strings)),
                ColumnarValue::Scalar(precision),
                ColumnarValue::Scalar(scale),
            ],
            arg_fields,
            number_rows: 4,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = func.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let lba = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq_at(lba, 0, 2, "12.34");
        assert!(lba.is_null(1));
        assert_eq_at(lba, 2, 2, "-99.5");
        assert_eq_at(lba, 3, 2, "0");
    }

    #[test]
    fn to_decimal_arb_from_string_rejects_value_exceeding_declared_precision() {
        let strings = StringArray::from(vec![Some("123456")]);
        let precision = datafusion::scalar::ScalarValue::Int64(Some(5));
        let scale = datafusion::scalar::ScalarValue::Int64(Some(0));
        let func = ToDecimalArbFromStringFunc::new();
        let arg_fields = vec![
            Arc::new(Field::new("text", DataType::Utf8, true)),
            Arc::new(Field::new("p", DataType::Int64, false)),
            Arc::new(Field::new("s", DataType::Int64, false)),
        ];
        let scalar_arguments: Vec<Option<&datafusion::scalar::ScalarValue>> =
            vec![None, Some(&precision), Some(&scale)];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        };
        let return_field = func.return_field_from_args(ret_args).unwrap();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(strings)),
                ColumnarValue::Scalar(precision),
                ColumnarValue::Scalar(scale),
            ],
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        assert!(func.invoke_with_args(args).is_err());
    }

    #[test]
    fn to_decimal_arb_from_string_rejects_garbage_input() {
        let strings = StringArray::from(vec![Some("not a number")]);
        let precision = datafusion::scalar::ScalarValue::Int64(Some(10));
        let scale = datafusion::scalar::ScalarValue::Int64(Some(0));
        let func = ToDecimalArbFromStringFunc::new();
        let arg_fields = vec![
            Arc::new(Field::new("text", DataType::Utf8, true)),
            Arc::new(Field::new("p", DataType::Int64, false)),
            Arc::new(Field::new("s", DataType::Int64, false)),
        ];
        let scalar_arguments: Vec<Option<&datafusion::scalar::ScalarValue>> =
            vec![None, Some(&precision), Some(&scale)];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        };
        let return_field = func.return_field_from_args(ret_args).unwrap();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(strings)),
                ColumnarValue::Scalar(precision),
                ColumnarValue::Scalar(scale),
            ],
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        assert!(func.invoke_with_args(args).is_err());
    }

    // ------- T068 cast widening: Decimal128/256 -> decimal_arb -------

    #[test]
    fn from_decimal128_widens_losslessly() {
        let dec = Decimal128Array::from(vec![Some(12345_i128), None, Some(-9876_i128)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let func = ToDecimalArbFromDecimal128Func::new();
        let arg_fields = vec![Arc::new(Field::new("x", DataType::Decimal128(10, 2), true))];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        };
        let return_field = func.return_field_from_args(ret_args).unwrap();
        assert_eq!(
            DecimalArbType::precision_scale_from_field(return_field.as_ref()),
            Some((10, 2))
        );
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(dec))],
            arg_fields,
            number_rows: 3,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = func.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let lba = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq_at(lba, 0, 2, "123.45");
        assert!(lba.is_null(1));
        assert_eq_at(lba, 2, 2, "-98.76");
    }

    #[test]
    fn from_decimal256_widens_losslessly() {
        let big = arrow::datatypes::i256::from_i128(12_345_678_901_234_i128);
        let dec = Decimal256Array::from(vec![Some(big), None])
            .with_precision_and_scale(40, 5)
            .unwrap();
        let func = ToDecimalArbFromDecimal256Func::new();
        let arg_fields = vec![Arc::new(Field::new("x", DataType::Decimal256(40, 5), true))];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        };
        let return_field = func.return_field_from_args(ret_args).unwrap();
        assert_eq!(
            DecimalArbType::precision_scale_from_field(return_field.as_ref()),
            Some((40, 5))
        );
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(dec))],
            arg_fields,
            number_rows: 2,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = func.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let lba = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq_at(lba, 0, 5, "123456789.01234");
        assert!(lba.is_null(1));
    }

    #[test]
    fn from_decimal128_rejects_non_decimal128_input_field() {
        let func = ToDecimalArbFromDecimal128Func::new();
        let arg_fields = vec![Arc::new(Field::new("x", DataType::Int64, false))];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        };
        assert!(func.return_field_from_args(ret_args).is_err());
    }

    // ------- T068 cast narrowing: decimal_arb -> Decimal128/256 -------

    fn invoke_to_decimal128(
        arb: LargeBinaryArray,
        input_p: u32,
        input_s: u32,
        target_p: i64,
        target_s: i64,
    ) -> Result<arrow::array::ArrayRef> {
        let func = DecimalArbToDecimal128Func::new();
        let input_field = DecimalArbType::field("v", input_p, input_s, true).unwrap();
        let arg_fields = vec![
            Arc::new(input_field),
            Arc::new(Field::new("p", DataType::Int64, false)),
            Arc::new(Field::new("s", DataType::Int64, false)),
        ];
        let p_lit = datafusion::scalar::ScalarValue::Int64(Some(target_p));
        let s_lit = datafusion::scalar::ScalarValue::Int64(Some(target_s));
        let scalar_arguments: Vec<Option<&datafusion::scalar::ScalarValue>> =
            vec![None, Some(&p_lit), Some(&s_lit)];
        let return_field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            })
            .unwrap();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(arb)),
                ColumnarValue::Scalar(p_lit),
                ColumnarValue::Scalar(s_lit),
            ],
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = func.invoke_with_args(args)?;
        match out {
            ColumnarValue::Array(a) => Ok(a),
            _ => panic!(),
        }
    }

    #[test]
    fn to_decimal128_narrows_within_range() {
        // Source decimal_arb declares scale 5; "1.2345" is stored at scale 5
        // as integer 123450 (one trailing zero added). "-7" → -700_000.
        let arb = build_decimal_arb_array(20, 5, &[Some("1.2345"), Some("-7"), None]);
        let out = invoke_to_decimal128(arb, 20, 5, 20, 5).unwrap();
        let dec = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec.value(0), 123_450_i128);
        assert_eq!(dec.value(1), -700_000_i128);
        assert!(dec.is_null(2));
    }

    #[test]
    fn to_decimal128_rejects_value_exceeding_target_precision() {
        // 100-digit value can't fit Decimal128(38).
        let mut s = String::with_capacity(40);
        s.push('1');
        for _ in 0..38 {
            s.push('0');
        }
        let arb = build_decimal_arb_array(100, 0, &[Some(&s)]);
        let err = invoke_to_decimal128(arb, 100, 0, 38, 0).unwrap_err();
        assert!(format!("{}", err).contains("Decimal128"));
    }

    #[test]
    fn to_decimal256_narrows_within_range() {
        let arb = build_decimal_arb_array(80, 10, &[Some("1234567890.0123456789")]);
        let func = DecimalArbToDecimal256Func::new();
        let input_field = DecimalArbType::field("v", 80, 10, true).unwrap();
        let arg_fields = vec![
            Arc::new(input_field),
            Arc::new(Field::new("p", DataType::Int64, false)),
            Arc::new(Field::new("s", DataType::Int64, false)),
        ];
        let p_lit = datafusion::scalar::ScalarValue::Int64(Some(50));
        let s_lit = datafusion::scalar::ScalarValue::Int64(Some(10));
        let scalar_arguments: Vec<Option<&datafusion::scalar::ScalarValue>> =
            vec![None, Some(&p_lit), Some(&s_lit)];
        let return_field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            })
            .unwrap();
        assert_eq!(
            return_field.data_type(),
            &DataType::Decimal256(50, 10),
            "return field must use the target (precision, scale)"
        );
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(arb)),
                ColumnarValue::Scalar(p_lit),
                ColumnarValue::Scalar(s_lit),
            ],
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = func.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!(),
        };
        let dec = arr.as_any().downcast_ref::<Decimal256Array>().unwrap();
        assert_eq!(dec.len(), 1);
        assert!(!dec.is_null(0));
    }

    // ------- T068 widening: from_int -------

    fn run_from_int(
        func: &ToDecimalArbFromIntFunc,
        arr: arrow::array::ArrayRef,
        input_dtype: DataType,
    ) -> LargeBinaryArray {
        let p_lit = datafusion::scalar::ScalarValue::Int64(Some(20));
        let s_lit = datafusion::scalar::ScalarValue::Int64(Some(0));
        let arg_fields = vec![
            Arc::new(Field::new("v", input_dtype, true)),
            Arc::new(Field::new("p", DataType::Int64, false)),
            Arc::new(Field::new("s", DataType::Int64, false)),
        ];
        let scalar_arguments: Vec<Option<&datafusion::scalar::ScalarValue>> =
            vec![None, Some(&p_lit), Some(&s_lit)];
        let return_field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            })
            .unwrap();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(arr),
                ColumnarValue::Scalar(p_lit),
                ColumnarValue::Scalar(s_lit),
            ],
            arg_fields,
            number_rows: 3,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        let out = func.invoke_with_args(args).unwrap();
        match out {
            ColumnarValue::Array(a) => a
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .clone(),
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn to_decimal_arb_from_int_works_for_int32() {
        use arrow::array::Int32Array;
        let func = ToDecimalArbFromIntFunc::new();
        let arr: arrow::array::ArrayRef =
            Arc::new(Int32Array::from(vec![Some(123_i32), None, Some(-7_i32)]));
        let dec = run_from_int(&func, arr, DataType::Int32);
        assert_eq_at(&dec, 0, 0, "123");
        assert!(dec.is_null(1));
        assert_eq_at(&dec, 2, 0, "-7");
    }

    #[test]
    fn to_decimal_arb_from_int_works_for_int64() {
        use arrow::array::Int64Array;
        let func = ToDecimalArbFromIntFunc::new();
        let arr: arrow::array::ArrayRef =
            Arc::new(Int64Array::from(vec![Some(123_i64), None, Some(-7_i64)]));
        let dec = run_from_int(&func, arr, DataType::Int64);
        assert_eq_at(&dec, 0, 0, "123");
        assert!(dec.is_null(1));
        assert_eq_at(&dec, 2, 0, "-7");
    }

    #[test]
    fn to_decimal_arb_from_int_works_for_uint64() {
        use arrow::array::UInt64Array;
        let func = ToDecimalArbFromIntFunc::new();
        let arr: arrow::array::ArrayRef =
            Arc::new(UInt64Array::from(vec![Some(0_u64), None, Some(u64::MAX)]));
        let dec = run_from_int(&func, arr, DataType::UInt64);
        assert_eq_at(&dec, 0, 0, "0");
        assert!(dec.is_null(1));
        assert_eq_at(&dec, 2, 0, "18446744073709551615");
    }

    #[test]
    fn to_decimal_arb_from_int_rejects_value_exceeding_precision() {
        use arrow::array::Int32Array;
        let func = ToDecimalArbFromIntFunc::new();
        let arr = Int32Array::from(vec![Some(99999_i32)]);
        let p_lit = datafusion::scalar::ScalarValue::Int64(Some(3));
        let s_lit = datafusion::scalar::ScalarValue::Int64(Some(0));
        let arg_fields = vec![
            Arc::new(Field::new("v", DataType::Int32, false)),
            Arc::new(Field::new("p", DataType::Int64, false)),
            Arc::new(Field::new("s", DataType::Int64, false)),
        ];
        let scalar_arguments: Vec<Option<&datafusion::scalar::ScalarValue>> =
            vec![None, Some(&p_lit), Some(&s_lit)];
        let return_field = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            })
            .unwrap();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(arr)),
                ColumnarValue::Scalar(p_lit),
                ColumnarValue::Scalar(s_lit),
            ],
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };
        assert!(func.invoke_with_args(args).is_err());
    }

    #[test]
    fn binary_op_rejects_non_decimal_arb_input_field() {
        // Arrays themselves don't matter for this test — only the field
        // metadata; constructing them keeps the helper signature parallel
        // to other tests.
        let _lhs = LargeBinaryArray::from_iter_values([&[0x00u8, 0x01u8] as &[u8]]);
        let _rhs = build_decimal_arb_array(10, 0, &[Some("1")]);
        let lhs_field = Field::new("x", DataType::LargeBinary, true); // no metadata
        let rhs_field = DecimalArbType::field("y", 10, 0, true).unwrap();
        let arg_fields = vec![Arc::new(lhs_field), Arc::new(rhs_field)];
        let func = DecimalArbAddFunc::new();
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        };
        assert!(func.return_field_from_args(ret_args).is_err());
    }

    // ------- Feature 002: native_int_kind hint propagation through ops -------
    //
    // These tests lock the *current* behavior: `build_output_field` calls
    // `DecimalArbType::field(...)` which produces a fresh field with the
    // decimal_arb extension keys and no `native_int_kind` hint. So the hint
    // is dropped on every binary-op output, regardless of whether the two
    // inputs agreed.
    //
    // This is acceptable because the hint exists to round-trip a column's
    // *origin* (UInt256 / Int256 source) to a matching native sink — once a
    // value goes through arithmetic, the result is no longer "the original
    // ClickHouse-side bytes," so dropping the hint and falling back to the
    // generic `Decimal(p, s)` (or `coerce_to: string`) sink path is the
    // safe default. The data-model documents this behavior under E1.

    use crate::types::decimal_arb::NativeIntKind;

    fn hinted_field(name: &str, precision: u32, scale: u32, kind: NativeIntKind) -> FieldRef {
        let field = DecimalArbType::field(name, precision, scale, true).unwrap();
        let with_hint = DecimalArbType::with_native_int_kind(field, kind).unwrap();
        Arc::new(with_hint)
    }

    fn run_add_return_field(lhs: FieldRef, rhs: FieldRef) -> FieldRef {
        let arg_fields = vec![lhs, rhs];
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        };
        DecimalArbAddFunc::new()
            .return_field_from_args(ret_args)
            .unwrap()
    }

    #[test]
    fn add_drops_native_int_kind_when_both_inputs_share_u256_hint() {
        let lhs = hinted_field("a", 78, 0, NativeIntKind::U256);
        let rhs = hinted_field("b", 78, 0, NativeIntKind::U256);
        let out = run_add_return_field(lhs, rhs);
        assert_eq!(
            DecimalArbType::native_int_kind_from_field_metadata(out.metadata()),
            None,
            "current behavior: binary-op output does not carry a native_int_kind \
             hint even when both inputs agreed — the result represents a new \
             value, not the original ClickHouse UInt256 bytes"
        );
    }

    #[test]
    fn add_drops_native_int_kind_when_inputs_have_mixed_hints() {
        let lhs = hinted_field("a", 78, 0, NativeIntKind::U256);
        let rhs = hinted_field("b", 78, 0, NativeIntKind::I256);
        let out = run_add_return_field(lhs, rhs);
        assert_eq!(
            DecimalArbType::native_int_kind_from_field_metadata(out.metadata()),
            None,
            "mixed-hint output drops the hint (ambiguous origin)"
        );
    }

    #[test]
    fn add_drops_native_int_kind_when_only_one_input_is_hinted() {
        let lhs = hinted_field("a", 78, 0, NativeIntKind::U256);
        let rhs = Arc::new(DecimalArbType::field("b", 78, 0, true).unwrap());
        let out = run_add_return_field(lhs, rhs);
        assert_eq!(
            DecimalArbType::native_int_kind_from_field_metadata(out.metadata()),
            None,
            "single-hinted input does not propagate the hint to the output"
        );
    }
}
