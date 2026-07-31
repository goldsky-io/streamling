//! Adversarial pass, agent 07 — **NULL and empty-input behaviour**.
//!
//! Focus areas:
//!
//! * Zero-length builders / arrays / batches, and the zero-row paths through
//!   every UDF and accumulator (an empty batch already caused one broadcast
//!   panic in this feature — `broadcast_len`).
//! * All-null columns, interleaved null/value patterns, leading/trailing nulls.
//! * `append_null` → `finish` → `value(i)` / `is_null(i)` / `len` / `is_empty`
//!   consistency, including the NULL-vs-zero distinction at the byte level
//!   (a NULL row stores *zero* bytes, the value `0` stores exactly one).
//! * NULL preservation through `from_decimal128` / `from_decimal256` /
//!   `to_decimal128` / `to_decimal256` / `to_string_array` /
//!   `from_string_array` / `try_from_array_and_field`.
//! * Nullable vs non-nullable field mismatch, and a NULL in a column declared
//!   non-nullable.
//!
//! Every test names the invariant it protects.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Decimal256Array, Int64Array, LargeBinaryArray,
    StringArray,
};
use arrow::datatypes::i256 as ArrowI256;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use streamling_common::functions::decimal_arb_aggregates::{
    DecimalArbAvgUdaf, DecimalArbExtremeUdaf, DecimalArbSumUdaf,
};
use streamling_common::functions::decimal_arb_ops::{
    DecimalArbAbsFunc, DecimalArbAddFunc, DecimalArbDivFunc, DecimalArbEqFunc, DecimalArbLtFunc,
    DecimalArbModFunc, DecimalArbMulFunc, DecimalArbNegFunc, DecimalArbSortKeyFunc,
    DecimalArbSubFunc, DecimalArbToDecimal128Func, DecimalArbToStringFunc, DecimalArbWithMetaFunc,
    ToDecimalArbFromDecimal128Func, ToDecimalArbFromIntFunc, ToDecimalArbFromStringFunc,
};
use streamling_common::types::decimal_arb::{
    DecimalArbArray, DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, NativeIntKind,
    decimal_arb_to_sort_key,
};
use streamling_common::types::decimal_arb_capability::{
    ColumnDirectiveView, ConnectorKind, validate_pipeline_decimal_arb,
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// `DecimalArbArray` is not `Debug`, so `unwrap_err()` is unavailable on
/// `Result<DecimalArbArray>`.
fn expect_err<T>(
    r: streamling_common::error::Result<T>,
    what: &str,
) -> streamling_common::error::StreamlingError {
    match r {
        Ok(_) => panic!("{what}: expected an error, got Ok"),
        Err(e) => e,
    }
}

fn v(s: &str) -> DecimalArbValue {
    DecimalArbValue::from_str(s).unwrap_or_else(|e| panic!("parse {s:?}: {e}"))
}

/// Build a `DecimalArbArray` from an `Option<&str>` pattern.
fn build(col: &str, p: u32, s: u32, vals: &[Option<&str>]) -> DecimalArbArray {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), col, p, s).unwrap();
    for x in vals {
        match x {
            Some(t) => b
                .append_str(t)
                .unwrap_or_else(|e| panic!("append {t:?} at ({p},{s}): {e}")),
            None => b.append_null(),
        }
    }
    b.finish()
}

/// Same, but return only the raw storage array.
fn raw(col: &str, p: u32, s: u32, vals: &[Option<&str>]) -> LargeBinaryArray {
    build(col, p, s, vals).into_inner().0
}

fn cfg() -> Arc<datafusion::config::ConfigOptions> {
    Arc::new(datafusion::config::ConfigOptions::default())
}

fn arb_field(name: &str, p: u32, s: u32) -> FieldRef {
    Arc::new(DecimalArbType::field(name, p, s, true).unwrap())
}

/// Invoke a unary decimal_arb UDF whose single input is a decimal_arb column.
fn invoke_unary_udf(
    func: &dyn ScalarUDFImpl,
    arr: LargeBinaryArray,
    p: u32,
    s: u32,
) -> datafusion::error::Result<ArrayRef> {
    let field = arb_field("x", p, s);
    let arg_fields = vec![Arc::clone(&field)];
    let ret = func
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        })
        .unwrap();
    let n = arr.len();
    let out = func.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::new(arr))],
        arg_fields,
        number_rows: n,
        return_field: ret,
        config_options: cfg(),
    })?;
    match out {
        ColumnarValue::Array(a) => Ok(a),
        other => panic!("expected array, got {other:?}"),
    }
}

/// Invoke a binary decimal_arb UDF over two decimal_arb columns.
fn invoke_binary_udf(
    func: &dyn ScalarUDFImpl,
    lhs: LargeBinaryArray,
    lp: u32,
    ls: u32,
    rhs: LargeBinaryArray,
    rp: u32,
    rs: u32,
    number_rows: usize,
) -> datafusion::error::Result<ArrayRef> {
    let lf = arb_field("l", lp, ls);
    let rf = arb_field("r", rp, rs);
    let arg_fields = vec![Arc::clone(&lf), Arc::clone(&rf)];
    let ret = func.return_field_from_args(ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &[None, None],
    })?;
    let out = func.invoke_with_args(ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(lhs)),
            ColumnarValue::Array(Arc::new(rhs)),
        ],
        arg_fields,
        number_rows,
        return_field: ret,
        config_options: cfg(),
    })?;
    match out {
        ColumnarValue::Array(a) => Ok(a),
        other => panic!("expected array, got {other:?}"),
    }
}

fn as_lb(a: &ArrayRef) -> &LargeBinaryArray {
    a.as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("LargeBinaryArray")
}

fn as_str_arr(a: &ArrayRef) -> &StringArray {
    a.as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray")
}

fn as_bool(a: &ArrayRef) -> &BooleanArray {
    a.as_any()
        .downcast_ref::<BooleanArray>()
        .expect("BooleanArray")
}

/// Build an accumulator for a decimal_arb input column of the given (p, s).
fn acc_for(udaf: &AggregateUDF, p: u32, s: u32, name: &str) -> Box<dyn Accumulator> {
    let field = DecimalArbType::field("x", p, s, true).unwrap();
    let schema = Schema::new(vec![field.clone()]);
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("x", 0))];
    let expr_fields: Vec<FieldRef> = vec![Arc::new(field)];
    let return_field: FieldRef = Arc::new(Field::new(name, DataType::LargeBinary, true));
    let args = AccumulatorArgs {
        return_field,
        schema: &schema,
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name,
        is_distinct: false,
        exprs: &exprs,
        expr_fields: &expr_fields,
    };
    udaf.accumulator(args)
        .unwrap_or_else(|e| panic!("accumulator for {name}: {e}"))
}

fn scalar_lb(v: ScalarValue) -> Option<Vec<u8>> {
    match v {
        ScalarValue::LargeBinary(b) => b,
        other => panic!("expected LargeBinary scalar, got {other:?}"),
    }
}

// ===========================================================================
// A. NULL-vs-zero at the byte level, and empty text inputs
// ===========================================================================

#[test]
fn zero_length_builder_finishes_to_zero_length_array() {
    let arr = build("x", 20, 4, &[]);
    assert_eq!(arr.len(), 0, "a builder with no appends must produce len 0");
    assert!(arr.is_empty(), "len 0 array must report is_empty");
}

#[test]
fn zero_length_array_keeps_declared_precision_and_scale() {
    let arr = build("x", 77, 18, &[]);
    assert_eq!(
        (arr.precision(), arr.scale()),
        (77, 18),
        "declared (precision, scale) must survive an empty build"
    );
}

#[test]
fn zero_length_array_has_no_nulls() {
    let arr = build("x", 10, 0, &[]);
    let (lb, _, _) = arr.into_inner();
    assert_eq!(lb.null_count(), 0, "empty array must have null_count 0");
    assert_eq!(lb.len(), 0);
}

#[test]
#[should_panic]
fn value_past_the_end_of_an_empty_array_panics_rather_than_returning_none() {
    // Documents the out-of-bounds contract: `value` is not a bounds-checked
    // accessor, it inherits Arrow's panic. If this ever starts returning
    // Ok(None) an empty batch would look like a one-row NULL batch.
    let arr = build("x", 10, 0, &[]);
    let _ = arr.value(0);
}

#[test]
#[should_panic]
fn value_past_the_end_of_a_populated_array_panics() {
    let arr = build("x", 10, 0, &[Some("1"), Some("2")]);
    let _ = arr.value(2);
}

#[test]
fn canonical_bytes_are_never_empty_for_any_value() {
    // A NULL row stores zero bytes; therefore no *value* may ever encode to
    // zero bytes, or NULL and a real value become indistinguishable.
    for text in [
        "0",
        "-0",
        "0.0",
        "0.0000",
        "1",
        "-1",
        "1e-10",
        "-1e-10",
        "123456789012345678901234567890",
        "-123456789012345678901234567890",
        "0.000000000000000001",
    ] {
        let value = v(text);
        for scale in [0u32, 1, 4, 18, 30] {
            let bytes = value.to_canonical_bytes_at_scale(scale);
            assert!(
                !bytes.is_empty(),
                "encoding of {text:?} at scale {scale} must never be empty"
            );
        }
    }
}

#[test]
fn zero_encodes_to_exactly_one_byte_not_zero_bytes() {
    let bytes = v("0").to_canonical_bytes_at_scale(0);
    assert_eq!(
        bytes,
        vec![0x00],
        "zero must be one sign byte — an empty payload is reserved for NULL"
    );
}

#[test]
fn null_row_stores_zero_length_value_bytes() {
    let lb = raw("x", 20, 2, &[None, Some("1.25"), None]);
    assert!(lb.is_null(0));
    assert_eq!(
        lb.value(0).len(),
        0,
        "a NULL row must occupy zero payload bytes"
    );
    assert_eq!(lb.value(2).len(), 0);
    assert!(!lb.value(1).is_empty(), "a real value must have bytes");
}

#[test]
fn decoding_a_null_rows_payload_directly_is_an_error_not_zero() {
    // Anyone who bypasses the validity bitmap must get an error, never 0.
    let lb = raw("x", 20, 2, &[None]);
    let err = expect_err(
        DecimalArbValue::from_canonical_bytes_at_scale(lb.value(0), 2),
        "decode empty payload",
    );
    assert!(
        err.to_string().to_lowercase().contains("empty"),
        "error must say the payload is empty, got: {err}"
    );
}

#[test]
fn is_null_is_equivalent_to_empty_payload_across_null_patterns() {
    let patterns: Vec<Vec<Option<&str>>> = vec![
        vec![],
        vec![None],
        vec![Some("0")],
        vec![None, None, None],
        vec![Some("1"), None, Some("-2"), None, Some("0")],
        vec![None, Some("0"), None],
        vec![Some("0"), Some("0"), None],
    ];
    for pat in patterns {
        let lb = raw("x", 30, 3, &pat);
        for i in 0..lb.len() {
            assert_eq!(
                lb.is_null(i),
                lb.value(i).is_empty(),
                "row {i} of {pat:?}: is_null must be exactly 'payload is empty'"
            );
        }
    }
}

#[test]
fn empty_string_is_rejected_not_parsed_as_zero() {
    assert!(
        DecimalArbValue::from_str("").is_err(),
        "the empty string must not parse as a decimal"
    );
}

#[test]
fn whitespace_only_string_is_rejected() {
    for s in [" ", "\t", "\n", "   "] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "whitespace-only {s:?} must not parse as a decimal"
        );
    }
}

#[test]
fn empty_string_in_a_string_array_is_rejected_not_treated_as_null() {
    let src = StringArray::from(vec![Some("")]);
    let _ = expect_err(
        DecimalArbArray::from_string_array(&src, 10, 0, "x"),
        "empty string element",
    );
}

#[test]
fn empty_string_element_rejection_names_the_column() {
    let src = StringArray::from(vec![Some("1"), Some("")]);
    let err = expect_err(
        DecimalArbArray::from_string_array(&src, 10, 0, "amount"),
        "empty string element",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("failed to parse") || msg.contains("amount"),
        "error should be actionable, got: {msg}"
    );
}

#[test]
fn empty_canonical_bytes_are_rejected_at_every_scale() {
    for scale in [0u32, 1, 18, 65_535] {
        let err = expect_err(
            DecimalArbValue::from_canonical_bytes_at_scale(&[], scale),
            "empty bytes",
        );
        assert!(
            err.to_string().to_lowercase().contains("empty"),
            "scale {scale}: {err}"
        );
    }
}

// ===========================================================================
// B. Builder NULL behaviour
// ===========================================================================

#[test]
fn append_null_then_finish_yields_null_at_that_index() {
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "x", 10, 2).unwrap();
    b.append_null();
    let arr = b.finish();
    assert_eq!(arr.len(), 1);
    assert!(arr.is_null(0), "the appended NULL must read back as NULL");
    assert_eq!(arr.value(0).unwrap(), None, "value() must be Ok(None)");
}

#[test]
fn all_null_array_reports_every_index_null() {
    let arr = build("x", 40, 6, &[None, None, None, None]);
    assert_eq!(arr.len(), 4);
    assert!(!arr.is_empty(), "an all-null array is not empty");
    for i in 0..4 {
        assert!(arr.is_null(i), "row {i} must be NULL");
        assert_eq!(arr.value(i).unwrap(), None, "row {i} value must be None");
    }
}

#[test]
fn all_null_array_null_count_equals_len() {
    for n in [1usize, 2, 5, 17] {
        let pat: Vec<Option<&str>> = vec![None; n];
        let lb = raw("x", 20, 0, &pat);
        assert_eq!(lb.null_count(), n, "all-null array of {n} rows");
        assert_eq!(lb.len(), n);
    }
}

#[test]
fn interleaved_null_value_pattern_is_preserved_exactly() {
    let pat = [
        Some("1"),
        None,
        Some("-2"),
        None,
        None,
        Some("0"),
        Some("3"),
        None,
    ];
    let arr = build("x", 30, 0, &pat);
    assert_eq!(arr.len(), pat.len());
    for (i, expected) in pat.iter().enumerate() {
        match expected {
            None => {
                assert!(arr.is_null(i), "row {i} should be NULL");
                assert_eq!(arr.value(i).unwrap(), None);
            }
            Some(t) => {
                assert!(!arr.is_null(i), "row {i} should not be NULL");
                assert_eq!(
                    arr.value(i).unwrap(),
                    Some(v(t)),
                    "row {i} must decode to {t}"
                );
            }
        }
    }
}

#[test]
fn leading_nulls_do_not_shift_subsequent_values() {
    let arr = build(
        "x",
        30,
        2,
        &[None, None, None, Some("12.34"), Some("-5.00")],
    );
    assert_eq!(arr.value(3).unwrap(), Some(v("12.34")));
    assert_eq!(arr.value(4).unwrap(), Some(v("-5")));
}

#[test]
fn trailing_nulls_do_not_corrupt_preceding_values() {
    let arr = build("x", 30, 2, &[Some("12.34"), Some("-5.00"), None, None]);
    assert_eq!(arr.value(0).unwrap(), Some(v("12.34")));
    assert_eq!(arr.value(1).unwrap(), Some(v("-5")));
    assert!(arr.is_null(2) && arr.is_null(3));
}

#[test]
fn a_null_between_two_values_does_not_shift_the_second() {
    let arr = build("x", 30, 4, &[Some("1.0001"), None, Some("2.0002")]);
    assert_eq!(arr.value(0).unwrap(), Some(v("1.0001")));
    assert_eq!(arr.value(2).unwrap(), Some(v("2.0002")));
}

#[test]
fn many_nulls_then_a_value_still_decodes_the_value() {
    let mut pat: Vec<Option<&str>> = vec![None; 200];
    pat.push(Some("-987654321.5"));
    let arr = build("x", 40, 1, &pat);
    assert_eq!(arr.len(), 201);
    assert_eq!(arr.value(200).unwrap(), Some(v("-987654321.5")));
    for i in 0..200 {
        assert!(arr.is_null(i));
    }
}

#[test]
fn append_null_never_validates_against_precision_or_scale() {
    // NULL has no digits, so it must be appendable to the tightest column.
    let mut b = DecimalArbArrayBuilder::with_capacity(0, "tiny", 1, 0).unwrap();
    b.append_null();
    b.append_null();
    let arr = b.finish();
    assert_eq!(arr.len(), 2);
    assert!(arr.is_null(0) && arr.is_null(1));
}

#[test]
fn a_rejected_append_does_not_add_a_row() {
    // FR-013 rejection must be atomic: the row must not land as NULL or junk.
    let mut b = DecimalArbArrayBuilder::with_capacity(4, "x", 3, 0).unwrap();
    b.append_str("12").unwrap();
    assert!(b.append_str("99999").is_err(), "must reject 5 int digits");
    b.append_null();
    let arr = b.finish();
    assert_eq!(
        arr.len(),
        2,
        "a rejected append must not contribute a row (got len {})",
        arr.len()
    );
    assert_eq!(arr.value(0).unwrap(), Some(v("12")));
    assert!(arr.is_null(1));
}

#[test]
fn builder_with_capacity_zero_still_accepts_appends() {
    let mut b = DecimalArbArrayBuilder::with_capacity(0, "x", 20, 2).unwrap();
    b.append_str("1.25").unwrap();
    b.append_null();
    let arr = b.finish();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr.value(0).unwrap(), Some(v("1.25")));
    assert!(arr.is_null(1));
}

#[test]
fn builder_capacity_smaller_than_the_number_of_appends_is_fine() {
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "x", 20, 2).unwrap();
    for i in 0..50 {
        if i % 3 == 0 {
            b.append_null();
        } else {
            b.append_str(&format!("{i}.25")).unwrap();
        }
    }
    let arr = b.finish();
    assert_eq!(arr.len(), 50);
    assert!(arr.is_null(0) && arr.is_null(3));
    assert_eq!(arr.value(1).unwrap(), Some(v("1.25")));
}

#[test]
fn all_null_array_still_carries_declared_precision_and_scale() {
    let arr = build("x", 100, 18, &[None, None]);
    assert_eq!((arr.precision(), arr.scale()), (100, 18));
}

#[test]
fn nulls_survive_into_inner_and_readoption() {
    let arr = build("amount", 50, 4, &[None, Some("1.5"), None]);
    let (lb, p, s) = arr.into_inner();
    assert_eq!((p, s), (50, 4));
    let field = DecimalArbType::field("amount", 50, 4, true).unwrap();
    let adopted = DecimalArbArray::try_from_array_and_field(lb, &field).unwrap();
    assert!(adopted.is_null(0));
    assert_eq!(adopted.value(1).unwrap(), Some(v("1.5")));
    assert!(adopted.is_null(2));
}

#[test]
fn builder_with_invalid_precision_scale_fails_even_with_zero_capacity() {
    // Validation must not be deferred to the first append.
    let _ = expect_err(
        DecimalArbArrayBuilder::with_capacity(0, "x", 0, 0),
        "precision 0",
    );
    let _ = expect_err(
        DecimalArbArrayBuilder::with_capacity(0, "x", 5, 6),
        "scale > precision",
    );
}

// ===========================================================================
// C. len / is_empty / is_null consistency
// ===========================================================================

#[test]
// The point of this test is precisely to compare `is_empty()` against the raw
// `len() == 0`, so the lint's suggestion would make it tautological.
#[allow(clippy::len_zero)]
fn is_empty_is_true_exactly_when_len_is_zero() {
    let patterns: Vec<Vec<Option<&str>>> = vec![
        vec![],
        vec![None],
        vec![Some("1")],
        vec![None, Some("1")],
        vec![None; 9],
    ];
    for pat in patterns {
        let arr = build("x", 20, 0, &pat);
        assert_eq!(
            arr.is_empty(),
            arr.len() == 0,
            "is_empty must agree with len == 0 for {pat:?}"
        );
    }
}

#[test]
fn wrapper_len_matches_inner_len() {
    for pat in [
        vec![],
        vec![None],
        vec![Some("1"), None, Some("2")],
        vec![None; 4],
    ] {
        let arr = build("x", 20, 0, &pat);
        let n = arr.len();
        let (lb, _, _) = arr.into_inner();
        assert_eq!(n, lb.len(), "wrapper len must equal storage len");
    }
}

#[test]
fn wrapper_is_null_matches_inner_is_null() {
    let pat = [Some("1"), None, Some("-2"), None, None];
    let arr = build("x", 20, 0, &pat);
    let flags: Vec<bool> = (0..arr.len()).map(|i| arr.is_null(i)).collect();
    let (lb, _, _) = arr.into_inner();
    let inner: Vec<bool> = (0..lb.len()).map(|i| lb.is_null(i)).collect();
    assert_eq!(flags, inner, "is_null must delegate faithfully");
}

#[test]
fn value_returns_none_exactly_for_the_null_indices() {
    let pat = [None, Some("7"), None, Some("8"), None];
    let arr = build("x", 20, 0, &pat);
    let nones: Vec<usize> = (0..arr.len())
        .filter(|i| arr.value(*i).unwrap().is_none())
        .collect();
    assert_eq!(
        nones,
        vec![0, 2, 4],
        "value() must return None exactly at the NULL indices"
    );
}

#[test]
fn all_null_array_is_not_reported_empty() {
    let arr = build("x", 20, 0, &[None, None]);
    assert!(
        !arr.is_empty(),
        "an all-null array has rows and must not be is_empty"
    );
}

#[test]
fn is_null_out_of_range_on_an_array_without_nulls_returns_false() {
    // Documents inherited Arrow behaviour: with no validity buffer the
    // out-of-range query is answered `false` rather than panicking. Pinned so
    // a change in this asymmetry is visible.
    let arr = build("x", 20, 0, &[Some("1")]);
    assert!(!arr.is_null(99));
}

#[test]
#[should_panic]
fn is_null_out_of_range_on_an_array_with_nulls_panics() {
    // The mirror of the previous test: once a validity buffer exists the same
    // out-of-range query panics instead.
    let arr = build("x", 20, 0, &[Some("1"), None]);
    let _ = arr.is_null(99);
}

#[test]
fn sliced_array_reports_slice_relative_nulls() {
    let lb = raw("x", 20, 2, &[Some("1.00"), None, Some("3.00"), None]);
    let sliced = lb.slice(1, 2);
    let field = DecimalArbType::field("x", 20, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(sliced, &field).unwrap();
    assert_eq!(arr.len(), 2, "slice length must be respected");
    assert!(
        arr.is_null(0),
        "slice row 0 is the original NULL at index 1"
    );
    assert_eq!(arr.value(1).unwrap(), Some(v("3")));
}

#[test]
fn slicing_to_zero_length_yields_an_empty_decimal_arb_array() {
    let lb = raw("x", 20, 2, &[Some("1.00"), None]);
    let sliced = lb.slice(1, 0);
    let field = DecimalArbType::field("x", 20, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(sliced, &field).unwrap();
    assert_eq!(arr.len(), 0);
    assert!(arr.is_empty());
}

#[test]
fn sliced_all_null_region_decodes_as_all_none() {
    let lb = raw("x", 20, 2, &[Some("1.00"), None, None, Some("2.00")]);
    let sliced = lb.slice(1, 2);
    let field = DecimalArbType::field("x", 20, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(sliced, &field).unwrap();
    assert_eq!(arr.value(0).unwrap(), None);
    assert_eq!(arr.value(1).unwrap(), None);
}

// ===========================================================================
// D. NULL / empty through the array conversions
// ===========================================================================

#[test]
fn from_decimal128_on_an_empty_source_yields_an_empty_array() {
    let src = Decimal128Array::from(Vec::<Option<i128>>::new())
        .with_precision_and_scale(10, 2)
        .unwrap();
    let arr = DecimalArbArray::from_decimal128(&src, 2, 50, 4, "x").unwrap();
    assert_eq!(arr.len(), 0);
    assert_eq!((arr.precision(), arr.scale()), (50, 4));
}

#[test]
fn from_decimal128_on_an_all_null_source_preserves_every_null() {
    let src = Decimal128Array::from(vec![None, None, None])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let arr = DecimalArbArray::from_decimal128(&src, 2, 50, 4, "x").unwrap();
    assert_eq!(arr.len(), 3);
    for i in 0..3 {
        assert!(arr.is_null(i), "row {i} must stay NULL");
    }
}

#[test]
fn from_decimal128_preserves_interleaved_nulls_and_values() {
    let src = Decimal128Array::from(vec![Some(125_i128), None, Some(-250_i128), None])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let arr = DecimalArbArray::from_decimal128(&src, 2, 50, 4, "x").unwrap();
    assert_eq!(arr.value(0).unwrap(), Some(v("1.25")));
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2).unwrap(), Some(v("-2.50")));
    assert!(arr.is_null(3));
}

#[test]
fn from_decimal128_null_count_is_preserved() {
    let src = Decimal128Array::from(vec![Some(1_i128), None, None, Some(2_i128)])
        .with_precision_and_scale(10, 0)
        .unwrap();
    let arr = DecimalArbArray::from_decimal128(&src, 0, 50, 0, "x").unwrap();
    let (lb, _, _) = arr.into_inner();
    assert_eq!(
        lb.null_count(),
        2,
        "null count must survive the widening cast"
    );
}

#[test]
fn from_decimal128_validates_target_precision_even_for_an_empty_source() {
    let src = Decimal128Array::from(Vec::<Option<i128>>::new())
        .with_precision_and_scale(10, 2)
        .unwrap();
    let _ = expect_err(
        DecimalArbArray::from_decimal128(&src, 2, 0, 0, "x"),
        "precision 0 with empty source",
    );
}

#[test]
fn from_decimal256_on_an_empty_source_yields_an_empty_array() {
    let src = Decimal256Array::from(Vec::<Option<ArrowI256>>::new())
        .with_precision_and_scale(40, 5)
        .unwrap();
    let arr = DecimalArbArray::from_decimal256(&src, 5, 60, 5, "x").unwrap();
    assert_eq!(arr.len(), 0);
    assert!(arr.is_empty());
}

#[test]
fn from_decimal256_on_an_all_null_source_preserves_every_null() {
    let src = Decimal256Array::from(vec![None, None])
        .with_precision_and_scale(40, 5)
        .unwrap();
    let arr = DecimalArbArray::from_decimal256(&src, 5, 60, 5, "x").unwrap();
    assert_eq!(arr.len(), 2);
    assert!(arr.is_null(0) && arr.is_null(1));
}

#[test]
fn from_decimal256_preserves_interleaved_nulls() {
    let src = Decimal256Array::from(vec![
        Some(ArrowI256::from_i128(12_345)),
        None,
        Some(ArrowI256::from_i128(-1)),
    ])
    .with_precision_and_scale(40, 2)
    .unwrap();
    let arr = DecimalArbArray::from_decimal256(&src, 2, 60, 2, "x").unwrap();
    assert_eq!(arr.value(0).unwrap(), Some(v("123.45")));
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2).unwrap(), Some(v("-0.01")));
}

#[test]
fn to_decimal128_on_an_empty_array_yields_an_empty_typed_array() {
    let arr = build("x", 50, 4, &[]);
    let out = arr.to_decimal128(20, 4, "x").unwrap();
    assert_eq!(out.len(), 0);
    assert_eq!(
        out.data_type(),
        &DataType::Decimal128(20, 4),
        "an empty result must still carry the declared Decimal128 type"
    );
}

#[test]
fn to_decimal128_on_an_all_null_array_preserves_nulls_and_type() {
    let arr = build("x", 50, 4, &[None, None, None]);
    let out = arr.to_decimal128(20, 4, "x").unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out.null_count(), 3);
    assert_eq!(out.data_type(), &DataType::Decimal128(20, 4));
}

#[test]
fn to_decimal128_preserves_null_positions() {
    let arr = build("x", 50, 2, &[None, Some("1.25"), None, Some("-3.00")]);
    let out = arr.to_decimal128(20, 2, "x").unwrap();
    assert!(out.is_null(0));
    assert_eq!(out.value(1), 125_i128);
    assert!(out.is_null(2));
    assert_eq!(out.value(3), -300_i128);
}

#[test]
fn to_decimal128_validates_precision_even_on_an_empty_array() {
    let arr = build("x", 50, 4, &[]);
    let _ = expect_err(arr.to_decimal128(0, 0, "x"), "precision 0, empty array");
    let arr = build("x", 50, 4, &[]);
    let _ = expect_err(arr.to_decimal128(39, 0, "x"), "precision 39, empty array");
}

#[test]
fn to_decimal128_validates_precision_even_on_an_all_null_array() {
    let arr = build("x", 50, 4, &[None, None]);
    let _ = expect_err(
        arr.to_decimal128(39, 0, "x"),
        "precision 39, all-null array",
    );
}

#[test]
fn to_decimal256_on_an_empty_array_yields_an_empty_typed_array() {
    let arr = build("x", 90, 4, &[]);
    let out = arr.to_decimal256(76, 4, "x").unwrap();
    assert_eq!(out.len(), 0);
    assert_eq!(out.data_type(), &DataType::Decimal256(76, 4));
}

#[test]
fn to_decimal256_on_an_all_null_array_preserves_nulls() {
    let arr = build("x", 90, 4, &[None, None, None, None]);
    let out = arr.to_decimal256(76, 4, "x").unwrap();
    assert_eq!(out.len(), 4);
    assert_eq!(out.null_count(), 4);
}

#[test]
fn to_decimal256_validates_precision_even_on_an_empty_array() {
    let arr = build("x", 90, 4, &[]);
    let _ = expect_err(arr.to_decimal256(0, 0, "x"), "precision 0, empty array");
    let arr = build("x", 90, 4, &[]);
    let _ = expect_err(arr.to_decimal256(77, 0, "x"), "precision 77, empty array");
}

#[test]
fn to_string_array_on_an_empty_array_yields_an_empty_string_array() {
    let arr = build("x", 30, 2, &[]);
    let out = arr.to_string_array().unwrap();
    assert_eq!(out.len(), 0);
    assert_eq!(out.data_type(), &DataType::Utf8);
}

#[test]
fn to_string_array_on_an_all_null_array_yields_all_nulls_not_empty_strings() {
    let arr = build("x", 30, 2, &[None, None, None]);
    let out = arr.to_string_array().unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out.null_count(), 3, "NULL must not become the empty string");
    for i in 0..3 {
        assert!(out.is_null(i), "row {i} must be NULL in the string array");
    }
}

#[test]
fn to_string_array_distinguishes_null_from_zero() {
    let arr = build("x", 30, 2, &[None, Some("0")]);
    let out = arr.to_string_array().unwrap();
    assert!(out.is_null(0), "NULL row must stay NULL");
    assert!(!out.is_null(1), "zero row must not become NULL");
    assert_eq!(out.value(1), "0");
}

#[test]
fn to_string_array_preserves_interleaved_null_positions() {
    let arr = build("x", 30, 4, &[Some("1.2345"), None, Some("-0.0001"), None]);
    let out = arr.to_string_array().unwrap();
    assert_eq!(out.value(0), "1.2345");
    assert!(out.is_null(1));
    assert_eq!(out.value(2), "-0.0001");
    assert!(out.is_null(3));
}

#[test]
fn from_string_array_on_an_empty_source_yields_an_empty_array() {
    let src = StringArray::from(Vec::<Option<&str>>::new());
    let arr = DecimalArbArray::from_string_array(&src, 30, 2, "x").unwrap();
    assert_eq!(arr.len(), 0);
    assert!(arr.is_empty());
}

#[test]
fn from_string_array_on_an_all_null_source_yields_all_nulls() {
    let src = StringArray::from(vec![None as Option<&str>, None, None]);
    let arr = DecimalArbArray::from_string_array(&src, 30, 2, "x").unwrap();
    assert_eq!(arr.len(), 3);
    for i in 0..3 {
        assert!(arr.is_null(i));
        assert_eq!(arr.value(i).unwrap(), None);
    }
}

#[test]
fn from_string_array_accepts_new_null_string_arrays() {
    let src = StringArray::new_null(4);
    let arr = DecimalArbArray::from_string_array(&src, 30, 2, "x").unwrap();
    assert_eq!(arr.len(), 4);
    let (lb, _, _) = arr.into_inner();
    assert_eq!(lb.null_count(), 4);
}

#[test]
fn from_string_array_validates_precision_even_on_an_empty_source() {
    let src = StringArray::from(Vec::<Option<&str>>::new());
    let _ = expect_err(
        DecimalArbArray::from_string_array(&src, 5, 6, "x"),
        "scale > precision on an empty source",
    );
}

#[test]
fn string_round_trip_preserves_null_positions_exactly() {
    let src = StringArray::from(vec![Some("1.25"), None, Some("0"), None, Some("-9.99")]);
    let arr = DecimalArbArray::from_string_array(&src, 30, 2, "x").unwrap();
    let back = arr.to_string_array().unwrap();
    assert_eq!(src.len(), back.len());
    for i in 0..src.len() {
        assert_eq!(
            src.is_null(i),
            back.is_null(i),
            "row {i}: NULL-ness must round-trip"
        );
        if !src.is_null(i) {
            assert_eq!(src.value(i), back.value(i), "row {i}");
        }
    }
}

#[test]
fn empty_string_round_trip_is_a_total_no_op() {
    let src = StringArray::from(Vec::<Option<&str>>::new());
    let arr = DecimalArbArray::from_string_array(&src, 30, 2, "x").unwrap();
    let back = arr.to_string_array().unwrap();
    assert_eq!(back.len(), 0);
}

#[test]
fn decimal128_round_trip_preserves_nulls_through_both_directions() {
    let src = Decimal128Array::from(vec![Some(125_i128), None, Some(-250_i128), None])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let arb = DecimalArbArray::from_decimal128(&src, 2, 50, 2, "x").unwrap();
    let back = arb.to_decimal128(10, 2, "x").unwrap();
    assert_eq!(back.len(), src.len());
    for i in 0..src.len() {
        assert_eq!(src.is_null(i), back.is_null(i), "row {i} NULL-ness");
        if !src.is_null(i) {
            assert_eq!(src.value(i), back.value(i), "row {i} value");
        }
    }
}

#[test]
fn decimal256_round_trip_preserves_nulls_through_both_directions() {
    let src = Decimal256Array::from(vec![Some(ArrowI256::from_i128(4_200)), None])
        .with_precision_and_scale(40, 2)
        .unwrap();
    let arb = DecimalArbArray::from_decimal256(&src, 2, 60, 2, "x").unwrap();
    let back = arb.to_decimal256(40, 2, "x").unwrap();
    assert_eq!(back.len(), 2);
    assert!(!back.is_null(0));
    assert!(back.is_null(1));
}

#[test]
fn empty_decimal128_round_trip_stays_empty() {
    let src = Decimal128Array::from(Vec::<Option<i128>>::new())
        .with_precision_and_scale(10, 2)
        .unwrap();
    let arb = DecimalArbArray::from_decimal128(&src, 2, 50, 2, "x").unwrap();
    let back = arb.to_decimal128(10, 2, "x").unwrap();
    assert_eq!(back.len(), 0);
    assert_eq!(back.null_count(), 0);
}

// ===========================================================================
// E. try_from_array_and_field with empty / null arrays and odd fields
// ===========================================================================

#[test]
fn adopting_an_empty_array_succeeds_and_keeps_metadata() {
    let lb = LargeBinaryArray::from(Vec::<Option<&[u8]>>::new());
    let field = DecimalArbType::field("x", 40, 8, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(lb, &field).unwrap();
    assert_eq!(arr.len(), 0);
    assert_eq!((arr.precision(), arr.scale()), (40, 8));
}

#[test]
fn adopting_a_new_null_array_yields_all_nones() {
    let lb = LargeBinaryArray::new_null(3);
    let field = DecimalArbType::field("x", 40, 8, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(lb, &field).unwrap();
    assert_eq!(arr.len(), 3);
    for i in 0..3 {
        assert_eq!(arr.value(i).unwrap(), None, "row {i}");
    }
}

#[test]
fn adopting_an_empty_array_with_a_plain_field_is_rejected() {
    let lb = LargeBinaryArray::from(Vec::<Option<&[u8]>>::new());
    let plain = Field::new("x", DataType::LargeBinary, true);
    let err = expect_err(
        DecimalArbArray::try_from_array_and_field(lb, &plain),
        "plain field",
    );
    assert!(
        err.to_string().contains("decimal_arb"),
        "error must mention decimal_arb: {err}"
    );
}

#[test]
fn adopting_does_not_reject_nulls_under_a_non_nullable_field() {
    // Pins current behaviour: `try_from_array_and_field` validates *metadata*
    // only, not the array's validity buffer against `Field::is_nullable`.
    // Arrow's RecordBatch construction is the layer that catches this (see the
    // nullability section below).
    let lb = raw("x", 20, 0, &[None, Some("1")]);
    let non_nullable = DecimalArbType::field("x", 20, 0, false).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(lb, &non_nullable).unwrap();
    assert!(arr.is_null(0));
}

#[test]
fn adopting_an_empty_array_under_a_non_nullable_field_is_fine() {
    let lb = LargeBinaryArray::from(Vec::<Option<&[u8]>>::new());
    let non_nullable = DecimalArbType::field("x", 20, 0, false).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(lb, &non_nullable).unwrap();
    assert_eq!(arr.len(), 0);
}

#[test]
fn a_field_with_the_extension_name_but_no_metadata_payload_has_no_precision_scale() {
    let mut md = HashMap::new();
    md.insert(
        DecimalArbType::EXTENSION_NAME_KEY.to_string(),
        DecimalArbType::EXTENSION_NAME.to_string(),
    );
    let f = Field::new("x", DataType::LargeBinary, true).with_metadata(md);
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "the name key alone makes is_decimal_arb_field true"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        None,
        "…but the (precision, scale) cannot be read"
    );
}

#[test]
fn adopting_under_a_metadata_less_decimal_arb_field_is_rejected() {
    let mut md = HashMap::new();
    md.insert(
        DecimalArbType::EXTENSION_NAME_KEY.to_string(),
        DecimalArbType::EXTENSION_NAME.to_string(),
    );
    let f = Field::new("x", DataType::LargeBinary, true).with_metadata(md);
    let lb = LargeBinaryArray::from(Vec::<Option<&[u8]>>::new());
    let _ = expect_err(
        DecimalArbArray::try_from_array_and_field(lb, &f),
        "metadata-less decimal_arb field",
    );
}

#[test]
fn an_empty_metadata_payload_string_yields_no_precision_scale() {
    let mut md = HashMap::new();
    md.insert(
        DecimalArbType::EXTENSION_NAME_KEY.to_string(),
        DecimalArbType::EXTENSION_NAME.to_string(),
    );
    md.insert(
        DecimalArbType::EXTENSION_METADATA_KEY.to_string(),
        String::new(),
    );
    let f = Field::new("x", DataType::LargeBinary, true).with_metadata(md);
    assert_eq!(DecimalArbType::precision_scale_from_field(&f), None);
}

#[test]
fn empty_metadata_map_is_not_decimal_arb() {
    assert!(!DecimalArbType::is_decimal_arb_metadata(&HashMap::new()));
    assert_eq!(
        DecimalArbType::native_int_kind_from_field_metadata(&HashMap::new()),
        None
    );
}

#[test]
fn native_int_kind_from_a_field_without_the_hint_is_none() {
    let f = DecimalArbType::field("x", 78, 0, false).unwrap();
    assert_eq!(DecimalArbType::native_int_kind_from_field(&f), None);
}

// ===========================================================================
// F. nullable vs non-nullable field mismatch
// ===========================================================================

#[test]
fn a_null_in_a_column_declared_non_nullable_is_rejected_by_record_batch() {
    let field = DecimalArbType::field("amount", 30, 2, false).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let lb = raw("amount", 30, 2, &[Some("1.00"), None]);
    let res = RecordBatch::try_new(schema, vec![Arc::new(lb)]);
    assert!(
        res.is_err(),
        "a NULL in a non-nullable decimal_arb column must be rejected"
    );
}

#[test]
fn an_all_null_column_declared_non_nullable_is_rejected() {
    let field = DecimalArbType::field("amount", 30, 2, false).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let lb = raw("amount", 30, 2, &[None, None]);
    assert!(RecordBatch::try_new(schema, vec![Arc::new(lb)]).is_err());
}

#[test]
fn an_all_null_column_declared_nullable_is_accepted() {
    let field = DecimalArbType::field("amount", 30, 2, true).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let lb = raw("amount", 30, 2, &[None, None]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(lb)]).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(0).null_count(), 2);
}

#[test]
fn a_zero_row_batch_on_a_non_nullable_decimal_arb_column_is_accepted() {
    let field = DecimalArbType::field("amount", 30, 2, false).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let lb = raw("amount", 30, 2, &[]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(lb)]).unwrap();
    assert_eq!(batch.num_rows(), 0, "an empty batch has no NULL to violate");
}

#[test]
fn a_zero_row_batch_keeps_decimal_arb_field_metadata() {
    let field = DecimalArbType::field("amount", 30, 2, true).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let lb = raw("amount", 30, 2, &[]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(lb)]).unwrap();
    let out = batch.schema();
    assert!(
        DecimalArbType::is_decimal_arb_field(out.field(0)),
        "an empty batch must not lose the extension metadata"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(out.field(0)),
        Some((30, 2))
    );
}

#[test]
fn non_nullable_decimal_arb_fields_are_still_recognized() {
    let f = DecimalArbType::field("x", 65, 30, false).unwrap();
    assert!(!f.is_nullable());
    assert!(DecimalArbType::is_decimal_arb_field(&f));
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((65, 30))
    );
}

#[test]
fn nullability_does_not_change_the_extension_metadata_bytes() {
    let a = DecimalArbType::field("x", 65, 30, true).unwrap();
    let b = DecimalArbType::field("x", 65, 30, false).unwrap();
    assert_eq!(
        a.metadata().get(DecimalArbType::EXTENSION_METADATA_KEY),
        b.metadata().get(DecimalArbType::EXTENSION_METADATA_KEY),
        "the (precision, scale) payload must not depend on nullability"
    );
}

#[test]
fn with_native_int_kind_preserves_non_nullability() {
    let base = DecimalArbType::field("gas", 78, 0, false).unwrap();
    let stamped = DecimalArbType::with_native_int_kind(base, NativeIntKind::U256).unwrap();
    assert!(
        !stamped.is_nullable(),
        "stamping the origin hint must not change nullability"
    );
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&stamped),
        Some(NativeIntKind::U256)
    );
}

#[test]
fn a_zero_row_batch_survives_arrow_ipc_with_metadata_intact() {
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;

    let field = DecimalArbType::field("amount", 44, 12, true).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let lb = raw("amount", 44, 12, &[]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb)]).unwrap();

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    let mut reader = StreamReader::try_new(buf.as_slice(), None).unwrap();
    let out_schema = reader.schema();
    assert_eq!(
        DecimalArbType::precision_scale_from_field(out_schema.field(0)),
        Some((44, 12)),
        "a zero-row batch must not lose decimal_arb metadata through IPC"
    );
    let first = reader.next().expect("one batch").unwrap();
    assert_eq!(first.num_rows(), 0);
}

#[test]
fn an_all_null_column_survives_arrow_ipc_with_its_null_count() {
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;

    let field = DecimalArbType::field("amount", 44, 12, true).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let lb = raw("amount", 44, 12, &[None, None, Some("1.5"), None]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb)]).unwrap();

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    let mut reader = StreamReader::try_new(buf.as_slice(), None).unwrap();
    let out = reader.next().expect("one batch").unwrap();
    assert_eq!(out.num_rows(), 4);
    assert_eq!(out.column(0).null_count(), 3, "null count must survive IPC");
    let lb = out
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .clone();
    let arr = DecimalArbArray::try_from_array_and_field(lb, out.schema().field(0)).unwrap();
    assert_eq!(arr.value(2).unwrap(), Some(v("1.5")));
    assert_eq!(arr.value(0).unwrap(), None);
}

#[test]
fn a_non_nullable_decimal_arb_field_survives_arrow_ipc() {
    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;

    let field = DecimalArbType::field("amount", 20, 2, false).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let lb = raw("amount", 20, 2, &[Some("1.00")]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(lb)]).unwrap();

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    let reader = StreamReader::try_new(buf.as_slice(), None).unwrap();
    let out_schema = reader.schema();
    assert!(
        !out_schema.field(0).is_nullable(),
        "non-nullability must survive IPC"
    );
    assert!(DecimalArbType::is_decimal_arb_field(out_schema.field(0)));
}

// ===========================================================================
// G. Scalar UDFs on zero-row and all-null input
// ===========================================================================

#[test]
fn to_string_udf_on_an_empty_column_returns_an_empty_column() {
    let out =
        invoke_unary_udf(&DecimalArbToStringFunc::new(), raw("x", 30, 2, &[]), 30, 2).unwrap();
    assert_eq!(out.len(), 0, "empty in, empty out");
    assert_eq!(out.data_type(), &DataType::Utf8);
}

#[test]
fn to_string_udf_on_an_all_null_column_returns_all_nulls() {
    let out = invoke_unary_udf(
        &DecimalArbToStringFunc::new(),
        raw("x", 30, 2, &[None, None, None]),
        30,
        2,
    )
    .unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out.null_count(), 3);
}

#[test]
fn to_string_udf_preserves_null_positions() {
    let out = invoke_unary_udf(
        &DecimalArbToStringFunc::new(),
        raw("x", 30, 2, &[None, Some("1.25"), None]),
        30,
        2,
    )
    .unwrap();
    let s = as_str_arr(&out);
    assert!(s.is_null(0));
    assert_eq!(s.value(1), "1.25");
    assert!(s.is_null(2));
}

#[test]
fn to_string_udf_on_a_null_scalar_returns_a_one_row_null() {
    let field = arb_field("x", 30, 2);
    let arg_fields = vec![Arc::clone(&field)];
    let out = DecimalArbToStringFunc::new()
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::LargeBinary(None))],
            arg_fields,
            number_rows: 1,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: cfg(),
        })
        .unwrap();
    let arr = match out {
        ColumnarValue::Array(a) => a,
        other => panic!("expected array, got {other:?}"),
    };
    assert_eq!(arr.len(), 1);
    assert_eq!(arr.null_count(), 1, "a NULL scalar must stay NULL");
}

#[test]
fn to_string_udf_return_field_is_non_nullable_for_a_non_nullable_input() {
    let input: FieldRef = Arc::new(DecimalArbType::field("x", 30, 2, false).unwrap());
    let f = DecimalArbToStringFunc::new()
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&input),
            scalar_arguments: &[None],
        })
        .unwrap();
    assert!(!f.is_nullable());
}

#[test]
fn sort_key_udf_on_an_empty_column_returns_an_empty_column() {
    let out = invoke_unary_udf(&DecimalArbSortKeyFunc::new(), raw("x", 30, 2, &[]), 30, 2).unwrap();
    assert_eq!(out.len(), 0);
}

#[test]
fn sort_key_udf_on_an_all_null_column_returns_all_nulls() {
    let out = invoke_unary_udf(
        &DecimalArbSortKeyFunc::new(),
        raw("x", 30, 2, &[None, None]),
        30,
        2,
    )
    .unwrap();
    assert_eq!(out.null_count(), 2, "NULL must not get a sort key");
}

#[test]
fn sort_key_udf_keeps_nulls_null_and_zero_keyed() {
    // A NULL must not collapse onto zero's sort key.
    let out = invoke_unary_udf(
        &DecimalArbSortKeyFunc::new(),
        raw("x", 30, 0, &[None, Some("0")]),
        30,
        0,
    )
    .unwrap();
    let lb = as_lb(&out);
    assert!(lb.is_null(0), "NULL row must stay NULL");
    assert!(!lb.is_null(1));
    assert_eq!(
        lb.value(1).to_vec(),
        decimal_arb_to_sort_key(&v("0").to_canonical_bytes_at_scale(0)),
        "zero must get the real zero sort key"
    );
}

#[test]
fn sort_key_helper_on_empty_bytes_returns_the_zero_key() {
    // Defensive documented behaviour: the helper never panics on an empty
    // payload, it produces the non-negative zero-length-magnitude key.
    assert_eq!(decimal_arb_to_sort_key(&[]), vec![1u8, 0, 0, 0, 0]);
}

#[test]
fn neg_udf_on_an_empty_column_returns_an_empty_column() {
    let out = invoke_unary_udf(&DecimalArbNegFunc::new(), raw("x", 30, 2, &[]), 30, 2).unwrap();
    assert_eq!(out.len(), 0);
}

#[test]
fn neg_udf_on_an_all_null_column_returns_all_nulls() {
    let out = invoke_unary_udf(
        &DecimalArbNegFunc::new(),
        raw("x", 30, 2, &[None, None]),
        30,
        2,
    )
    .unwrap();
    assert_eq!(out.null_count(), 2);
}

#[test]
fn neg_udf_preserves_null_positions_and_negates_values() {
    let out = invoke_unary_udf(
        &DecimalArbNegFunc::new(),
        raw("x", 30, 2, &[Some("1.25"), None, Some("-3.00")]),
        30,
        2,
    )
    .unwrap();
    let lb = as_lb(&out);
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(lb.value(0), 2).unwrap(),
        v("-1.25")
    );
    assert!(lb.is_null(1));
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(lb.value(2), 2).unwrap(),
        v("3")
    );
}

#[test]
fn abs_udf_on_an_empty_column_returns_an_empty_column() {
    let out = invoke_unary_udf(&DecimalArbAbsFunc::new(), raw("x", 30, 2, &[]), 30, 2).unwrap();
    assert_eq!(out.len(), 0);
}

#[test]
fn abs_udf_on_an_all_null_column_returns_all_nulls() {
    let out = invoke_unary_udf(
        &DecimalArbAbsFunc::new(),
        raw("x", 30, 2, &[None, None, None]),
        30,
        2,
    )
    .unwrap();
    assert_eq!(out.null_count(), 3);
}

#[test]
fn add_udf_on_two_empty_columns_returns_an_empty_column() {
    let out = invoke_binary_udf(
        &DecimalArbAddFunc::new(),
        raw("l", 20, 2, &[]),
        20,
        2,
        raw("r", 20, 2, &[]),
        20,
        2,
        0,
    )
    .unwrap();
    assert_eq!(out.len(), 0, "empty + empty must stay empty");
}

#[test]
fn add_udf_on_an_empty_column_and_a_broadcast_scalar_stays_empty() {
    // F1-shaped empty-batch guard: max(0, 1) would wrongly produce one row.
    let out = invoke_binary_udf(
        &DecimalArbAddFunc::new(),
        raw("l", 20, 2, &[]),
        20,
        2,
        raw("r", 20, 2, &[Some("1.00")]),
        20,
        2,
        0,
    )
    .unwrap();
    assert_eq!(
        out.len(),
        0,
        "an empty batch with a broadcast scalar must produce zero rows"
    );
}

#[test]
fn add_udf_with_the_scalar_on_the_left_of_an_empty_column_stays_empty() {
    let out = invoke_binary_udf(
        &DecimalArbAddFunc::new(),
        raw("l", 20, 2, &[Some("1.00")]),
        20,
        2,
        raw("r", 20, 2, &[]),
        20,
        2,
        0,
    )
    .unwrap();
    assert_eq!(out.len(), 0);
}

#[test]
fn every_binary_op_keeps_an_empty_batch_empty_with_a_broadcast_scalar() {
    let ops: Vec<Box<dyn ScalarUDFImpl>> = vec![
        Box::new(DecimalArbAddFunc::new()),
        Box::new(DecimalArbSubFunc::new()),
        Box::new(DecimalArbMulFunc::new()),
        Box::new(DecimalArbDivFunc::new()),
        Box::new(DecimalArbModFunc::new()),
    ];
    for op in &ops {
        for (l, r) in [(0usize, 1usize), (1, 0), (0, 0)] {
            let lv: Vec<Option<&str>> = if l == 0 { vec![] } else { vec![Some("2.00")] };
            let rv: Vec<Option<&str>> = if r == 0 { vec![] } else { vec![Some("2.00")] };
            let out = invoke_binary_udf(
                op.as_ref(),
                raw("l", 20, 2, &lv),
                20,
                2,
                raw("r", 20, 2, &rv),
                20,
                2,
                0,
            )
            .unwrap_or_else(|e| panic!("{} on ({l},{r}): {e}", op.name()));
            assert_eq!(
                out.len(),
                0,
                "{} with operand lengths ({l},{r}) must produce zero rows",
                op.name()
            );
        }
    }
}

#[test]
fn every_comparison_op_keeps_an_empty_batch_empty_with_a_broadcast_scalar() {
    let ops: Vec<Box<dyn ScalarUDFImpl>> = vec![
        Box::new(DecimalArbEqFunc::new()),
        Box::new(DecimalArbLtFunc::new()),
    ];
    for op in &ops {
        for (l, r) in [(0usize, 1usize), (1, 0), (0, 0)] {
            let lv: Vec<Option<&str>> = if l == 0 { vec![] } else { vec![Some("2.00")] };
            let rv: Vec<Option<&str>> = if r == 0 { vec![] } else { vec![Some("2.00")] };
            let out = invoke_binary_udf(
                op.as_ref(),
                raw("l", 20, 2, &lv),
                20,
                2,
                raw("r", 20, 2, &rv),
                20,
                2,
                0,
            )
            .unwrap();
            assert_eq!(
                out.len(),
                0,
                "{} with operand lengths ({l},{r}) must produce zero rows",
                op.name()
            );
        }
    }
}

#[test]
fn add_udf_propagates_null_from_either_side() {
    let out = invoke_binary_udf(
        &DecimalArbAddFunc::new(),
        raw("l", 20, 2, &[Some("1.00"), None, Some("3.00"), None]),
        20,
        2,
        raw("r", 20, 2, &[None, Some("2.00"), Some("4.00"), None]),
        20,
        2,
        4,
    )
    .unwrap();
    let lb = as_lb(&out);
    assert!(lb.is_null(0), "value + NULL must be NULL");
    assert!(lb.is_null(1), "NULL + value must be NULL");
    assert!(!lb.is_null(2), "value + value must not be NULL");
    assert!(lb.is_null(3), "NULL + NULL must be NULL");
}

#[test]
fn add_udf_on_two_all_null_columns_returns_all_nulls() {
    let out = invoke_binary_udf(
        &DecimalArbAddFunc::new(),
        raw("l", 20, 2, &[None, None, None]),
        20,
        2,
        raw("r", 20, 2, &[None, None, None]),
        20,
        2,
        3,
    )
    .unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out.null_count(), 3);
}

#[test]
fn add_udf_with_a_broadcast_null_scalar_nulls_the_whole_column() {
    let out = invoke_binary_udf(
        &DecimalArbAddFunc::new(),
        raw("l", 20, 2, &[Some("1.00"), Some("2.00"), Some("3.00")]),
        20,
        2,
        raw("r", 20, 2, &[None]),
        20,
        2,
        3,
    )
    .unwrap();
    assert_eq!(
        out.len(),
        3,
        "the scalar must broadcast to the column length"
    );
    assert_eq!(out.null_count(), 3, "adding a NULL scalar nulls every row");
}

#[test]
fn div_by_a_null_is_null_not_a_division_by_zero_error() {
    let out = invoke_binary_udf(
        &DecimalArbDivFunc::new(),
        raw("l", 20, 2, &[Some("1.00")]),
        20,
        2,
        raw("r", 20, 2, &[None]),
        20,
        2,
        1,
    )
    .unwrap_or_else(|e| panic!("div by NULL must not error: {e}"));
    assert_eq!(out.null_count(), 1);
}

#[test]
fn mod_by_a_null_is_null_not_a_modulo_by_zero_error() {
    let out = invoke_binary_udf(
        &DecimalArbModFunc::new(),
        raw("l", 20, 2, &[Some("1.00")]),
        20,
        2,
        raw("r", 20, 2, &[None]),
        20,
        2,
        1,
    )
    .unwrap_or_else(|e| panic!("mod by NULL must not error: {e}"));
    assert_eq!(out.null_count(), 1);
}

#[test]
fn div_of_an_all_null_column_by_zero_does_not_error() {
    // Every row is NULL, so the zero divisor is never reached: the op must
    // short-circuit on NULL rather than evaluate the division.
    let out = invoke_binary_udf(
        &DecimalArbDivFunc::new(),
        raw("l", 20, 2, &[None, None]),
        20,
        2,
        raw("r", 20, 2, &[Some("0.00")]),
        20,
        2,
        2,
    )
    .unwrap_or_else(|e| panic!("all-NULL numerator must not hit division by zero: {e}"));
    assert_eq!(out.null_count(), 2);
}

#[test]
fn comparison_udf_returns_null_for_null_operands() {
    let out = invoke_binary_udf(
        &DecimalArbEqFunc::new(),
        raw("l", 20, 2, &[Some("1.00"), None, Some("2.00")]),
        20,
        2,
        raw("r", 20, 2, &[None, Some("1.00"), Some("2.00")]),
        20,
        2,
        3,
    )
    .unwrap();
    let b = as_bool(&out);
    assert!(b.is_null(0), "value = NULL must be NULL, not false");
    assert!(b.is_null(1), "NULL = value must be NULL, not false");
    assert!(b.value(2), "2 = 2 must be true");
}

#[test]
fn comparison_of_two_nulls_is_null_not_true() {
    let out = invoke_binary_udf(
        &DecimalArbEqFunc::new(),
        raw("l", 20, 2, &[None]),
        20,
        2,
        raw("r", 20, 2, &[None]),
        20,
        2,
        1,
    )
    .unwrap();
    assert!(
        as_bool(&out).is_null(0),
        "NULL = NULL must be NULL under three-valued logic"
    );
}

#[test]
fn lt_of_an_all_null_column_is_all_null() {
    let out = invoke_binary_udf(
        &DecimalArbLtFunc::new(),
        raw("l", 20, 2, &[None, None, None]),
        20,
        2,
        raw("r", 20, 2, &[Some("1.00")]),
        20,
        2,
        3,
    )
    .unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out.null_count(), 3);
}

#[test]
fn with_meta_udf_passes_an_empty_column_through_unchanged() {
    let func = DecimalArbWithMetaFunc::new();
    let field = arb_field("x", 30, 2);
    let arg_fields = vec![
        Arc::clone(&field),
        Arc::new(Field::new("p", DataType::Int64, false)),
        Arc::new(Field::new("s", DataType::Int64, false)),
    ];
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(raw("x", 30, 2, &[]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(30))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            arg_fields,
            number_rows: 0,
            return_field: arb_field("out", 30, 2),
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => assert_eq!(a.len(), 0),
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn with_meta_udf_passes_all_nulls_through_unchanged() {
    let func = DecimalArbWithMetaFunc::new();
    let field = arb_field("x", 30, 2);
    let arg_fields = vec![
        Arc::clone(&field),
        Arc::new(Field::new("p", DataType::Int64, false)),
        Arc::new(Field::new("s", DataType::Int64, false)),
    ];
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(raw("x", 30, 2, &[None, Some("1.00"), None]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(30))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            arg_fields,
            number_rows: 3,
            return_field: arb_field("out", 30, 2),
            config_options: cfg(),
        })
        .unwrap();
    let a = match out {
        ColumnarValue::Array(a) => a,
        other => panic!("expected array, got {other:?}"),
    };
    assert_eq!(a.null_count(), 2, "passthrough must not change NULL-ness");
}

#[test]
fn with_meta_udf_return_field_mirrors_a_non_nullable_input() {
    let input: FieldRef = Arc::new(DecimalArbType::field("x", 30, 2, false).unwrap());
    let p = ScalarValue::Int64(Some(30));
    let s = ScalarValue::Int64(Some(2));
    let arg_fields = vec![
        Arc::clone(&input),
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let f = DecimalArbWithMetaFunc::new()
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, Some(&p), Some(&s)],
        })
        .unwrap();
    assert!(
        !f.is_nullable(),
        "non-nullable input must stay non-nullable"
    );
    assert!(DecimalArbType::is_decimal_arb_field(&f));
}

#[test]
fn from_string_udf_on_an_empty_column_returns_an_empty_column() {
    let func = ToDecimalArbFromStringFunc::new();
    let arg_fields = vec![
        Arc::new(Field::new("t", DataType::Utf8, true)) as FieldRef,
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(Vec::<Option<&str>>::new()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(30))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            arg_fields,
            number_rows: 0,
            return_field: arb_field("out", 30, 2),
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => assert_eq!(a.len(), 0),
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn from_string_udf_on_all_null_strings_returns_all_nulls() {
    let func = ToDecimalArbFromStringFunc::new();
    let arg_fields = vec![
        Arc::new(Field::new("t", DataType::Utf8, true)) as FieldRef,
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::new_null(3))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(30))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            arg_fields,
            number_rows: 3,
            return_field: arb_field("out", 30, 2),
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => {
            assert_eq!(a.len(), 3);
            assert_eq!(a.null_count(), 3);
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn from_int_udf_on_an_empty_column_returns_an_empty_column() {
    let func = ToDecimalArbFromIntFunc::new();
    let arg_fields = vec![
        Arc::new(Field::new("v", DataType::Int64, true)) as FieldRef,
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(Vec::<Option<i64>>::new()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(30))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            arg_fields,
            number_rows: 0,
            return_field: arb_field("out", 30, 0),
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => assert_eq!(a.len(), 0),
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn from_int_udf_preserves_nulls() {
    let func = ToDecimalArbFromIntFunc::new();
    let arg_fields = vec![
        Arc::new(Field::new("v", DataType::Int64, true)) as FieldRef,
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    Some(5_i64),
                    None,
                    Some(-7),
                ]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(30))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            arg_fields,
            number_rows: 3,
            return_field: arb_field("out", 30, 0),
            config_options: cfg(),
        })
        .unwrap();
    let a = match out {
        ColumnarValue::Array(a) => a,
        other => panic!("expected array, got {other:?}"),
    };
    let lb = as_lb(&a);
    assert!(!lb.is_null(0));
    assert!(lb.is_null(1), "NULL int must stay NULL");
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(lb.value(2), 0).unwrap(),
        v("-7")
    );
}

#[test]
fn from_int_udf_on_an_all_null_column_returns_all_nulls() {
    let func = ToDecimalArbFromIntFunc::new();
    let arg_fields = vec![
        Arc::new(Field::new("v", DataType::Int64, true)) as FieldRef,
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::new_null(4))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(30))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            arg_fields,
            number_rows: 4,
            return_field: arb_field("out", 30, 0),
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => assert_eq!(a.null_count(), 4),
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn from_decimal128_udf_on_an_empty_column_returns_an_empty_column() {
    let func = ToDecimalArbFromDecimal128Func::new();
    let input = Decimal128Array::from(Vec::<Option<i128>>::new())
        .with_precision_and_scale(10, 2)
        .unwrap();
    let arg_fields = vec![Arc::new(Field::new("v", DataType::Decimal128(10, 2), true)) as FieldRef];
    let ret = func
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        })
        .unwrap();
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            arg_fields,
            number_rows: 0,
            return_field: ret,
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => assert_eq!(a.len(), 0),
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn from_decimal128_udf_preserves_nulls() {
    let func = ToDecimalArbFromDecimal128Func::new();
    let input = Decimal128Array::from(vec![Some(125_i128), None])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let arg_fields = vec![Arc::new(Field::new("v", DataType::Decimal128(10, 2), true)) as FieldRef];
    let ret = func
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        })
        .unwrap();
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input))],
            arg_fields,
            number_rows: 2,
            return_field: ret,
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => {
            assert_eq!(a.len(), 2);
            assert_eq!(a.null_count(), 1);
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn to_decimal128_udf_on_an_empty_column_returns_an_empty_typed_column() {
    let func = DecimalArbToDecimal128Func::new();
    let field = arb_field("x", 30, 2);
    let arg_fields = vec![
        Arc::clone(&field),
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(raw("x", 30, 2, &[]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            arg_fields,
            number_rows: 0,
            return_field: Arc::new(Field::new("out", DataType::Decimal128(20, 2), true)),
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => {
            assert_eq!(a.len(), 0);
            assert_eq!(a.data_type(), &DataType::Decimal128(20, 2));
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn to_decimal128_udf_preserves_nulls() {
    let func = DecimalArbToDecimal128Func::new();
    let field = arb_field("x", 30, 2);
    let arg_fields = vec![
        Arc::clone(&field),
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(raw("x", 30, 2, &[None, Some("1.25"), None]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            arg_fields,
            number_rows: 3,
            return_field: Arc::new(Field::new("out", DataType::Decimal128(20, 2), true)),
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => {
            assert_eq!(a.null_count(), 2);
            let d = a.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(d.value(1), 125_i128);
        }
        other => panic!("expected array, got {other:?}"),
    }
}

// ===========================================================================
// H. Aggregates over empty and all-null input
// ===========================================================================

#[test]
fn sum_over_zero_rows_is_null_not_zero() {
    let udaf = DecimalArbSumUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "sum");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[])) as ArrayRef])
        .unwrap();
    assert_eq!(
        scalar_lb(acc.evaluate().unwrap()),
        None,
        "SUM over zero rows must be NULL, never 0"
    );
}

#[test]
fn sum_with_no_batches_at_all_is_null() {
    let udaf = DecimalArbSumUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "sum");
    assert_eq!(scalar_lb(acc.evaluate().unwrap()), None);
}

#[test]
fn sum_over_an_all_null_column_is_null_not_zero() {
    let udaf = DecimalArbSumUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "sum");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[None, None, None])) as ArrayRef])
        .unwrap();
    assert_eq!(
        scalar_lb(acc.evaluate().unwrap()),
        None,
        "SUM over an all-NULL column must be NULL"
    );
}

#[test]
fn sum_skips_nulls_but_counts_every_value() {
    let udaf = DecimalArbSumUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "sum");
    acc.update_batch(&[
        Arc::new(raw("x", 30, 2, &[Some("1.50"), None, Some("2.25"), None])) as ArrayRef,
    ])
    .unwrap();
    let bytes = scalar_lb(acc.evaluate().unwrap()).expect("non-null sum");
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 2).unwrap(),
        v("3.75")
    );
}

#[test]
fn sum_state_over_zero_rows_is_a_null_state_not_a_zero_state() {
    let udaf = DecimalArbSumUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "sum");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[])) as ArrayRef])
        .unwrap();
    let state = acc.state().unwrap();
    assert_eq!(state.len(), 1);
    assert_eq!(
        scalar_lb(state[0].clone()),
        None,
        "an empty partition's SUM state must be NULL so it merges as 'no rows'"
    );
}

#[test]
fn merging_only_null_sum_states_stays_null() {
    let udaf = DecimalArbSumUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "sum");
    acc.merge_batch(&[Arc::new(LargeBinaryArray::new_null(3)) as ArrayRef])
        .unwrap();
    assert_eq!(
        scalar_lb(acc.evaluate().unwrap()),
        None,
        "merging empty partitions must not manufacture a zero"
    );
}

#[test]
fn merging_an_empty_sum_state_batch_stays_null() {
    let udaf = DecimalArbSumUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "sum");
    acc.merge_batch(&[Arc::new(raw("x", 46, 2, &[])) as ArrayRef])
        .unwrap();
    assert_eq!(scalar_lb(acc.evaluate().unwrap()), None);
}

#[test]
fn sum_of_a_single_zero_row_is_zero_not_null() {
    // The NULL/zero distinction in the other direction.
    let udaf = DecimalArbSumUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "sum");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[Some("0")])) as ArrayRef])
        .unwrap();
    let bytes = scalar_lb(acc.evaluate().unwrap()).expect("SUM of one zero row must not be NULL");
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 2).unwrap(),
        v("0")
    );
}

#[test]
fn min_over_zero_rows_is_null() {
    let udaf = DecimalArbExtremeUdaf::min_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "min");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[])) as ArrayRef])
        .unwrap();
    assert_eq!(scalar_lb(acc.evaluate().unwrap()), None);
}

#[test]
fn max_over_zero_rows_is_null() {
    let udaf = DecimalArbExtremeUdaf::max_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "max");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[])) as ArrayRef])
        .unwrap();
    assert_eq!(scalar_lb(acc.evaluate().unwrap()), None);
}

#[test]
fn min_over_an_all_null_column_is_null() {
    let udaf = DecimalArbExtremeUdaf::min_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "min");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[None, None])) as ArrayRef])
        .unwrap();
    assert_eq!(scalar_lb(acc.evaluate().unwrap()), None);
}

#[test]
fn max_over_an_all_null_column_is_null() {
    let udaf = DecimalArbExtremeUdaf::max_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "max");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[None, None])) as ArrayRef])
        .unwrap();
    assert_eq!(scalar_lb(acc.evaluate().unwrap()), None);
}

#[test]
fn min_ignores_nulls_and_does_not_treat_them_as_negative_infinity() {
    let udaf = DecimalArbExtremeUdaf::min_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "min");
    acc.update_batch(&[
        Arc::new(raw("x", 30, 2, &[None, Some("5.00"), None, Some("-2.00")])) as ArrayRef,
    ])
    .unwrap();
    let bytes = scalar_lb(acc.evaluate().unwrap()).expect("non-null min");
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 2).unwrap(),
        v("-2")
    );
}

#[test]
fn max_ignores_nulls_and_does_not_treat_them_as_zero() {
    let udaf = DecimalArbExtremeUdaf::max_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "max");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[None, Some("-5.00"), None])) as ArrayRef])
        .unwrap();
    let bytes = scalar_lb(acc.evaluate().unwrap()).expect("non-null max");
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 2).unwrap(),
        v("-5"),
        "MAX of only negative values must not be pulled up to 0 by NULLs"
    );
}

#[test]
fn min_state_over_zero_rows_is_null() {
    let udaf = DecimalArbExtremeUdaf::min_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "min");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[])) as ArrayRef])
        .unwrap();
    assert_eq!(scalar_lb(acc.state().unwrap()[0].clone()), None);
}

#[test]
fn merging_only_null_min_states_stays_null() {
    let udaf = DecimalArbExtremeUdaf::min_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "min");
    acc.merge_batch(&[Arc::new(LargeBinaryArray::new_null(2)) as ArrayRef])
        .unwrap();
    assert_eq!(scalar_lb(acc.evaluate().unwrap()), None);
}

#[test]
fn avg_over_zero_rows_is_null_not_zero() {
    let udaf = DecimalArbAvgUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "avg");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[])) as ArrayRef])
        .unwrap();
    assert_eq!(
        scalar_lb(acc.evaluate().unwrap()),
        None,
        "AVG over zero rows must be NULL (no division by zero, no 0 result)"
    );
}

#[test]
fn avg_over_an_all_null_column_is_null() {
    let udaf = DecimalArbAvgUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "avg");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[None, None, None])) as ArrayRef])
        .unwrap();
    assert_eq!(scalar_lb(acc.evaluate().unwrap()), None);
}

#[test]
fn avg_counts_only_non_null_rows() {
    // 2 and 4 with two NULLs must average 3, not 1.5.
    let udaf = DecimalArbAvgUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "avg");
    acc.update_batch(&[
        Arc::new(raw("x", 30, 2, &[Some("2.00"), None, Some("4.00"), None])) as ArrayRef,
    ])
    .unwrap();
    let bytes = scalar_lb(acc.evaluate().unwrap()).expect("non-null avg");
    let got = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 3).unwrap();
    assert_eq!(got, v("3"), "NULLs must not dilute the AVG denominator");
}

#[test]
fn avg_state_over_zero_rows_reports_a_zero_count() {
    let udaf = DecimalArbAvgUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "avg");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[])) as ArrayRef])
        .unwrap();
    let state = acc.state().unwrap();
    assert_eq!(state.len(), 2, "AVG state is (sum, count)");
    assert_eq!(
        state[1],
        ScalarValue::Int64(Some(0)),
        "an empty partition must contribute a count of 0"
    );
}

#[test]
fn merging_zero_count_avg_states_still_evaluates_to_null() {
    let udaf = DecimalArbAvgUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "avg");
    // Two empty partitions: sum = 0, count = 0.
    let sums = raw("x", 46, 2, &[Some("0"), Some("0")]);
    let counts = Int64Array::from(vec![0_i64, 0]);
    acc.merge_batch(&[Arc::new(sums) as ArrayRef, Arc::new(counts) as ArrayRef])
        .unwrap();
    assert_eq!(
        scalar_lb(acc.evaluate().unwrap()),
        None,
        "merging only empty partitions must stay NULL, not become 0/0"
    );
}

#[test]
fn merging_an_empty_avg_state_batch_stays_null() {
    let udaf = DecimalArbAvgUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "avg");
    let sums = raw("x", 46, 2, &[]);
    let counts = Int64Array::from(Vec::<i64>::new());
    acc.merge_batch(&[Arc::new(sums) as ArrayRef, Arc::new(counts) as ArrayRef])
        .unwrap();
    assert_eq!(scalar_lb(acc.evaluate().unwrap()), None);
}

#[test]
fn avg_over_a_single_zero_row_is_zero_not_null() {
    let udaf = DecimalArbAvgUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "avg");
    acc.update_batch(&[Arc::new(raw("x", 30, 2, &[Some("0")])) as ArrayRef])
        .unwrap();
    let bytes = scalar_lb(acc.evaluate().unwrap()).expect("AVG of one zero row must not be NULL");
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 3).unwrap(),
        v("0")
    );
}

#[test]
fn repeated_empty_updates_never_flip_an_aggregate_off_null() {
    for (udaf, name) in [
        (DecimalArbSumUdaf::into_udaf(), "sum"),
        (DecimalArbExtremeUdaf::min_udaf(), "min"),
        (DecimalArbExtremeUdaf::max_udaf(), "max"),
        (DecimalArbAvgUdaf::into_udaf(), "avg"),
    ] {
        let mut acc = acc_for(&udaf, 30, 2, name);
        for _ in 0..5 {
            acc.update_batch(&[Arc::new(raw("x", 30, 2, &[])) as ArrayRef])
                .unwrap();
            acc.update_batch(&[Arc::new(raw("x", 30, 2, &[None, None])) as ArrayRef])
                .unwrap();
        }
        assert_eq!(
            scalar_lb(acc.evaluate().unwrap()),
            None,
            "{name} must still be NULL after only empty/all-NULL batches"
        );
    }
}

#[test]
fn aggregate_state_fields_are_derivable_for_an_all_null_column() {
    use datafusion::logical_expr::function::StateFieldsArgs;
    let input: FieldRef = Arc::new(DecimalArbType::field("x", 30, 2, true).unwrap());
    let udaf = DecimalArbSumUdaf::into_udaf();
    let fields = udaf
        .state_fields(StateFieldsArgs {
            name: "sum",
            input_fields: std::slice::from_ref(&input),
            return_field: Arc::new(Field::new("sum", DataType::LargeBinary, true)),
            ordering_fields: &[],
            is_distinct: false,
        })
        .unwrap();
    assert_eq!(fields.len(), 1);
    assert!(
        DecimalArbType::is_decimal_arb_field(fields[0].as_ref()),
        "the SUM state field must stay decimal_arb"
    );
}

// ===========================================================================
// I. Arrow kernels over empty / all-null decimal_arb columns
// ===========================================================================

#[test]
fn concat_of_an_empty_and_a_populated_column_preserves_values_and_nulls() {
    let empty = raw("x", 30, 2, &[]);
    let full = raw("x", 30, 2, &[Some("1.25"), None]);
    let out = arrow::compute::concat(&[&empty, &full]).unwrap();
    let lb = out
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .clone();
    let field = DecimalArbType::field("x", 30, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(lb, &field).unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr.value(0).unwrap(), Some(v("1.25")));
    assert!(arr.is_null(1));
}

#[test]
fn concat_of_two_empty_columns_stays_empty() {
    let a = raw("x", 30, 2, &[]);
    let b = raw("x", 30, 2, &[]);
    let out = arrow::compute::concat(&[&a, &b]).unwrap();
    assert_eq!(out.len(), 0);
}

#[test]
fn concat_of_an_all_null_and_a_populated_column_keeps_the_null_count() {
    let nulls = raw("x", 30, 2, &[None, None]);
    let full = raw("x", 30, 2, &[Some("1.25")]);
    let out = arrow::compute::concat(&[&nulls, &full]).unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out.null_count(), 2);
}

#[test]
fn filtering_everything_out_yields_a_decodable_zero_row_column() {
    let lb = raw("x", 30, 2, &[Some("1.25"), None, Some("2.50")]);
    let mask = BooleanArray::from(vec![false, false, false]);
    let out = arrow::compute::filter(&lb, &mask).unwrap();
    let filtered = out
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .clone();
    let field = DecimalArbType::field("x", 30, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(filtered, &field).unwrap();
    assert_eq!(arr.len(), 0);
    assert!(arr.is_empty());
}

#[test]
fn filtering_keeps_null_rows_that_the_mask_selects() {
    let lb = raw("x", 30, 2, &[Some("1.25"), None, Some("2.50")]);
    let mask = BooleanArray::from(vec![false, true, true]);
    let out = arrow::compute::filter(&lb, &mask).unwrap();
    let filtered = out
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .clone();
    let field = DecimalArbType::field("x", 30, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(filtered, &field).unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(
        arr.value(0).unwrap(),
        None,
        "the selected NULL must survive"
    );
    assert_eq!(arr.value(1).unwrap(), Some(v("2.50")));
}

#[test]
fn take_with_null_indices_produces_decodable_nulls() {
    let lb = raw("x", 30, 2, &[Some("1.25"), Some("2.50")]);
    let idx = Int64Array::from(vec![Some(1_i64), None, Some(0)]);
    let out = arrow::compute::take(&lb, &idx, None).unwrap();
    let taken = out
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .clone();
    let field = DecimalArbType::field("x", 30, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(taken, &field).unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr.value(0).unwrap(), Some(v("2.50")));
    assert_eq!(
        arr.value(1).unwrap(),
        None,
        "a NULL take index must yield NULL, not an empty-bytes decode error"
    );
    assert_eq!(arr.value(2).unwrap(), Some(v("1.25")));
}

#[test]
fn take_with_no_indices_yields_an_empty_column() {
    let lb = raw("x", 30, 2, &[Some("1.25")]);
    let idx = Int64Array::from(Vec::<i64>::new());
    let out = arrow::compute::take(&lb, &idx, None).unwrap();
    assert_eq!(out.len(), 0);
}

#[test]
fn arrow_is_null_kernel_agrees_with_the_wrapper() {
    let pat = [Some("1.25"), None, Some("0"), None];
    let lb = raw("x", 30, 2, &pat);
    let kernel = arrow::compute::kernels::boolean::is_null(&lb).unwrap();
    let field = DecimalArbType::field("x", 30, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(lb, &field).unwrap();
    for i in 0..arr.len() {
        assert_eq!(
            kernel.value(i),
            arr.is_null(i),
            "row {i}: Arrow's is_null must agree with DecimalArbArray::is_null"
        );
    }
}

#[test]
fn arrow_is_null_kernel_on_an_empty_column_is_empty() {
    let lb = raw("x", 30, 2, &[]);
    let kernel = arrow::compute::kernels::boolean::is_null(&lb).unwrap();
    assert_eq!(kernel.len(), 0);
}

// ===========================================================================
// J. Capability validation with empty schemas / directives
// ===========================================================================

#[test]
fn validating_an_empty_schema_succeeds() {
    let schema = Schema::empty();
    assert!(validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[]).is_ok());
}

#[test]
fn validating_a_schema_with_no_decimal_arb_fields_succeeds() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
    assert!(validate_pipeline_decimal_arb(&schema, ConnectorKind::KafkaProtobuf, &[]).is_ok());
}

#[test]
fn an_empty_directive_list_means_no_coercion_opt_in() {
    // 100 digits exceeds ClickHouse's native cap; with no directives it must
    // be rejected rather than silently accepted.
    let schema = Schema::new(vec![DecimalArbType::field("amount", 100, 2, true).unwrap()]);
    let err = validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &[])
        .expect_err("an over-wide column with no coerce_to must be rejected");
    assert_eq!(err.len(), 1);
    assert!(!err.is_empty());
}

#[test]
fn a_directive_for_a_different_column_does_not_opt_the_column_in() {
    let schema = Schema::new(vec![DecimalArbType::field("amount", 100, 2, true).unwrap()]);
    let directives = [ColumnDirectiveView {
        name: "other",
        coerce_to_string: true,
    }];
    assert!(
        validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &directives).is_err(),
        "a directive naming a different column must not opt this one in"
    );
}

#[test]
fn no_errors_container_reports_empty() {
    let schema = Schema::new(vec![DecimalArbType::field("amount", 10, 2, true).unwrap()]);
    assert!(
        validate_pipeline_decimal_arb(&schema, ConnectorKind::ClickHouse, &schema_directives())
            .is_ok()
    );
}

fn schema_directives() -> Vec<ColumnDirectiveView<'static>> {
    vec![]
}

#[test]
#[ignore = "FINDING: a decimal_arb field whose ARROW:extension:metadata payload is missing/unparseable is silently SKIPPED by validate_pipeline_decimal_arb instead of being rejected at config load"]
fn a_decimal_arb_field_with_unreadable_metadata_is_rejected_at_config_load() {
    let mut md = HashMap::new();
    md.insert(
        DecimalArbType::EXTENSION_NAME_KEY.to_string(),
        DecimalArbType::EXTENSION_NAME.to_string(),
    );
    let f = Field::new("amount", DataType::LargeBinary, true).with_metadata(md);
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "precondition: the field claims to be decimal_arb"
    );
    let schema = Schema::new(vec![f]);
    assert!(
        validate_pipeline_decimal_arb(&schema, ConnectorKind::KafkaProtobuf, &[]).is_err(),
        "a column that claims decimal_arb but has unreadable (precision, scale) must not be \
         waved through the capability check"
    );
}

// ===========================================================================
// K. Cross-cutting: NULL never becomes a value, values never become NULL
// ===========================================================================

#[test]
fn a_full_pipeline_of_conversions_preserves_the_null_mask() {
    let pat = [None, Some("1.25"), None, Some("0"), Some("-3.50"), None];
    let arr = build("x", 30, 2, &pat);
    let expected: Vec<bool> = pat.iter().map(|p| p.is_none()).collect();

    let strings = arr.to_string_array().unwrap();
    let via_string = DecimalArbArray::from_string_array(&strings, 30, 2, "x").unwrap();
    let d128 = via_string.to_decimal128(20, 2, "x").unwrap();
    let back = DecimalArbArray::from_decimal128(&d128, 2, 30, 2, "x").unwrap();

    for (i, want_null) in expected.iter().enumerate() {
        assert_eq!(
            strings.is_null(i),
            *want_null,
            "row {i} after to_string_array"
        );
        assert_eq!(
            via_string.is_null(i),
            *want_null,
            "row {i} after from_string_array"
        );
        assert_eq!(d128.is_null(i), *want_null, "row {i} after to_decimal128");
        assert_eq!(back.is_null(i), *want_null, "row {i} after from_decimal128");
    }
}

#[test]
fn a_full_pipeline_of_conversions_on_an_empty_column_stays_empty() {
    let arr = build("x", 30, 2, &[]);
    let strings = arr.to_string_array().unwrap();
    assert_eq!(strings.len(), 0);
    let via_string = DecimalArbArray::from_string_array(&strings, 30, 2, "x").unwrap();
    assert_eq!(via_string.len(), 0);
    let d128 = via_string.to_decimal128(20, 2, "x").unwrap();
    assert_eq!(d128.len(), 0);
    let back = DecimalArbArray::from_decimal128(&d128, 2, 30, 2, "x").unwrap();
    assert_eq!(back.len(), 0);
}

#[test]
fn a_full_pipeline_of_conversions_on_an_all_null_column_stays_all_null() {
    let arr = build("x", 30, 2, &[None, None, None]);
    let strings = arr.to_string_array().unwrap();
    let via_string = DecimalArbArray::from_string_array(&strings, 30, 2, "x").unwrap();
    let d128 = via_string.to_decimal128(20, 2, "x").unwrap();
    let back = DecimalArbArray::from_decimal128(&d128, 2, 30, 2, "x").unwrap();
    assert_eq!(back.len(), 3);
    for i in 0..3 {
        assert_eq!(back.value(i).unwrap(), None, "row {i} must stay NULL");
    }
}

#[test]
fn zero_never_degrades_into_null_across_scales() {
    for scale in [0u32, 1, 2, 9, 18] {
        let arr = build("z", 40, scale, &[Some("0")]);
        assert!(!arr.is_null(0), "zero at scale {scale} must not be NULL");
        assert_eq!(arr.value(0).unwrap(), Some(v("0")));
        let strings = arr.to_string_array().unwrap();
        assert!(!strings.is_null(0), "zero must render, not be NULL");
    }
}

#[test]
fn negative_zero_never_degrades_into_null() {
    let arr = build("z", 40, 4, &[Some("-0"), Some("-0.0000")]);
    for i in 0..2 {
        assert!(!arr.is_null(i));
        assert_eq!(arr.value(i).unwrap(), Some(v("0")));
    }
}

#[test]
fn a_mixed_batch_keeps_metadata_and_null_mask_through_record_batch() {
    let field = DecimalArbType::field("amount", 30, 2, true).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let lb = raw("amount", 30, 2, &[None, Some("1.25"), None]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(lb)]).unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.column(0).null_count(), 2);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(batch.schema().field(0)),
        Some((30, 2))
    );
}

#[test]
fn an_empty_batch_and_an_all_null_batch_share_the_same_schema() {
    let field = DecimalArbType::field("amount", 30, 2, true).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let empty =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(raw("amount", 30, 2, &[]))]).unwrap();
    let nulls = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(raw("amount", 30, 2, &[None, None]))],
    )
    .unwrap();
    assert_eq!(
        empty.schema(),
        nulls.schema(),
        "row content must not change the schema"
    );
}

// ===========================================================================
// L. The other empty: a NON-NULL row carrying a zero-length payload
//
// No value ever encodes to zero bytes, so a non-null empty payload can only
// come from corrupt / foreign data. Every consumer must reject it rather than
// silently read it as zero — otherwise NULL, corruption and 0 all collapse.
// ===========================================================================

/// A one-row column whose single row is NOT null but carries zero bytes.
fn non_null_empty_payload() -> LargeBinaryArray {
    LargeBinaryArray::from(vec![Some(&[] as &[u8])])
}

#[test]
fn a_non_null_empty_payload_is_not_reported_as_null() {
    let lb = non_null_empty_payload();
    assert_eq!(lb.len(), 1);
    assert!(!lb.is_null(0), "precondition: the row is valid, not NULL");
    assert_eq!(lb.value(0).len(), 0, "precondition: the payload is empty");
}

#[test]
fn value_on_a_non_null_empty_payload_errors_rather_than_decoding_zero() {
    let field = DecimalArbType::field("x", 30, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(non_null_empty_payload(), &field).unwrap();
    let err = expect_err(arr.value(0), "non-null empty payload");
    assert!(
        err.to_string().to_lowercase().contains("empty"),
        "must report the empty payload, got: {err}"
    );
}

#[test]
fn to_string_array_on_a_non_null_empty_payload_errors_rather_than_emitting_zero() {
    let field = DecimalArbType::field("x", 30, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(non_null_empty_payload(), &field).unwrap();
    let _ = expect_err(arr.to_string_array(), "to_string_array on empty payload");
}

#[test]
fn to_decimal128_on_a_non_null_empty_payload_errors() {
    let field = DecimalArbType::field("x", 30, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(non_null_empty_payload(), &field).unwrap();
    let _ = expect_err(
        arr.to_decimal128(20, 2, "x"),
        "to_decimal128 on empty payload",
    );
}

#[test]
fn to_decimal256_on_a_non_null_empty_payload_errors() {
    let field = DecimalArbType::field("x", 30, 2, true).unwrap();
    let arr = DecimalArbArray::try_from_array_and_field(non_null_empty_payload(), &field).unwrap();
    let _ = expect_err(
        arr.to_decimal256(40, 2, "x"),
        "to_decimal256 on empty payload",
    );
}

#[test]
fn to_string_udf_on_a_non_null_empty_payload_errors_with_the_row_index() {
    let res = invoke_unary_udf(
        &DecimalArbToStringFunc::new(),
        non_null_empty_payload(),
        30,
        2,
    );
    let err = res.expect_err("decimal_arb_to_string must reject an empty payload");
    assert!(
        err.to_string().contains("row 0"),
        "error should name the offending row, got: {err}"
    );
}

#[test]
fn neg_udf_on_a_non_null_empty_payload_errors() {
    assert!(
        invoke_unary_udf(&DecimalArbNegFunc::new(), non_null_empty_payload(), 30, 2).is_err(),
        "decimal_arb_neg must not treat an empty payload as zero"
    );
}

#[test]
fn add_udf_on_a_non_null_empty_payload_errors() {
    let res = invoke_binary_udf(
        &DecimalArbAddFunc::new(),
        non_null_empty_payload(),
        30,
        2,
        raw("r", 30, 2, &[Some("1.00")]),
        30,
        2,
        1,
    );
    assert!(
        res.is_err(),
        "decimal_arb_add must not treat an empty payload as zero"
    );
}

#[test]
fn comparison_udf_on_a_non_null_empty_payload_errors() {
    let res = invoke_binary_udf(
        &DecimalArbEqFunc::new(),
        non_null_empty_payload(),
        30,
        2,
        raw("r", 30, 2, &[Some("0")]),
        30,
        2,
        1,
    );
    assert!(
        res.is_err(),
        "decimal_arb_eq must not silently equate an empty payload to zero"
    );
}

#[test]
fn sum_over_a_non_null_empty_payload_errors_rather_than_skipping_it() {
    let udaf = DecimalArbSumUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "sum");
    assert!(
        acc.update_batch(&[Arc::new(non_null_empty_payload()) as ArrayRef])
            .is_err(),
        "SUM must not silently skip a corrupt empty payload as if it were NULL"
    );
}

#[test]
fn min_over_a_non_null_empty_payload_errors_rather_than_skipping_it() {
    let udaf = DecimalArbExtremeUdaf::min_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "min");
    assert!(
        acc.update_batch(&[Arc::new(non_null_empty_payload()) as ArrayRef])
            .is_err()
    );
}

#[test]
fn avg_over_a_non_null_empty_payload_errors_rather_than_skipping_it() {
    let udaf = DecimalArbAvgUdaf::into_udaf();
    let mut acc = acc_for(&udaf, 30, 2, "avg");
    assert!(
        acc.update_batch(&[Arc::new(non_null_empty_payload()) as ArrayRef])
            .is_err()
    );
}

#[test]
fn sort_key_udf_maps_a_non_null_empty_payload_onto_the_zero_key() {
    // Pinned divergence: `decimal_arb_to_sort_key` documents a defensive
    // branch for empty input, so unlike every other consumer it does NOT
    // reject a corrupt empty payload — it sorts it as zero. Recorded here so
    // any change in that deliberate choice is visible.
    let out = invoke_unary_udf(
        &DecimalArbSortKeyFunc::new(),
        non_null_empty_payload(),
        30,
        2,
    )
    .expect("the sort-key UDF tolerates an empty payload by design");
    let lb = as_lb(&out);
    assert!(!lb.is_null(0));
    assert_eq!(lb.value(0).to_vec(), vec![1u8, 0, 0, 0, 0]);
}

// ===========================================================================
// M. An empty batch must not mask plan-level validation
// ===========================================================================

fn plain_field(name: &str) -> FieldRef {
    Arc::new(Field::new(name, DataType::LargeBinary, true))
}

#[test]
fn to_string_udf_still_rejects_a_metadata_less_field_on_an_empty_batch() {
    let arg_fields = vec![plain_field("x")];
    let res = DecimalArbToStringFunc::new().invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::new(raw("x", 30, 2, &[])))],
        arg_fields,
        number_rows: 0,
        return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
        config_options: cfg(),
    });
    assert!(
        res.is_err(),
        "a zero-row batch must not hide a non-decimal_arb input field"
    );
}

#[test]
fn sort_key_udf_still_rejects_a_metadata_less_field_on_an_empty_batch() {
    let arg_fields = vec![plain_field("x")];
    let res = DecimalArbSortKeyFunc::new().invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::new(raw("x", 30, 2, &[])))],
        arg_fields,
        number_rows: 0,
        return_field: plain_field("out"),
        config_options: cfg(),
    });
    assert!(res.is_err());
}

#[test]
fn neg_udf_still_rejects_a_metadata_less_field_on_an_empty_batch() {
    let arg_fields = vec![plain_field("x")];
    let res = DecimalArbNegFunc::new().invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::new(raw("x", 30, 2, &[])))],
        arg_fields,
        number_rows: 0,
        return_field: plain_field("out"),
        config_options: cfg(),
    });
    assert!(res.is_err());
}

#[test]
fn add_udf_still_rejects_a_metadata_less_field_on_an_empty_batch() {
    let arg_fields = vec![plain_field("l"), arb_field("r", 30, 2)];
    let res = DecimalArbAddFunc::new().invoke_with_args(ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(raw("l", 30, 2, &[]))),
            ColumnarValue::Array(Arc::new(raw("r", 30, 2, &[]))),
        ],
        arg_fields,
        number_rows: 0,
        return_field: arb_field("out", 30, 2),
        config_options: cfg(),
    });
    assert!(res.is_err());
}

#[test]
fn comparison_udf_still_rejects_a_metadata_less_field_on_an_empty_batch() {
    let arg_fields = vec![arb_field("l", 30, 2), plain_field("r")];
    let res = DecimalArbEqFunc::new().invoke_with_args(ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(raw("l", 30, 2, &[]))),
            ColumnarValue::Array(Arc::new(raw("r", 30, 2, &[]))),
        ],
        arg_fields,
        number_rows: 0,
        return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
        config_options: cfg(),
    });
    assert!(res.is_err());
}

#[test]
fn to_decimal128_udf_still_rejects_a_metadata_less_field_on_an_empty_batch() {
    let arg_fields = vec![
        plain_field("x"),
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let res = DecimalArbToDecimal128Func::new().invoke_with_args(ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(raw("x", 30, 2, &[]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ],
        arg_fields,
        number_rows: 0,
        return_field: Arc::new(Field::new("out", DataType::Decimal128(20, 2), true)),
        config_options: cfg(),
    });
    assert!(res.is_err());
}

#[test]
fn from_string_udf_still_rejects_an_invalid_precision_on_an_empty_batch() {
    let arg_fields = vec![
        Arc::new(Field::new("t", DataType::Utf8, true)) as FieldRef,
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let res = ToDecimalArbFromStringFunc::new().invoke_with_args(ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(StringArray::from(Vec::<Option<&str>>::new()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
        ],
        arg_fields,
        number_rows: 0,
        return_field: plain_field("out"),
        config_options: cfg(),
    });
    assert!(
        res.is_err(),
        "precision 0 must be rejected even when there are no rows to build"
    );
}

#[test]
fn from_int_udf_still_rejects_scale_greater_than_precision_on_an_empty_batch() {
    let arg_fields = vec![
        Arc::new(Field::new("v", DataType::Int64, true)) as FieldRef,
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let res = ToDecimalArbFromIntFunc::new().invoke_with_args(ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(Vec::<Option<i64>>::new()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(6))),
        ],
        arg_fields,
        number_rows: 0,
        return_field: plain_field("out"),
        config_options: cfg(),
    });
    assert!(res.is_err());
}

// ===========================================================================
// N. NULL scalars, and operand-length mismatches around zero
// ===========================================================================

#[test]
fn a_null_scalar_operand_nulls_the_whole_output_column() {
    let func = DecimalArbAddFunc::new();
    let lf = arb_field("l", 20, 2);
    let rf = arb_field("r", 20, 2);
    let arg_fields = vec![Arc::clone(&lf), Arc::clone(&rf)];
    let ret = func
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        })
        .unwrap();
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(raw("l", 20, 2, &[Some("1.00"), Some("2.00")]))),
                ColumnarValue::Scalar(ScalarValue::LargeBinary(None)),
            ],
            arg_fields,
            number_rows: 2,
            return_field: ret,
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => {
            assert_eq!(a.len(), 2);
            assert_eq!(a.null_count(), 2, "adding a NULL literal nulls every row");
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn a_null_scalar_operand_against_an_empty_column_yields_zero_rows() {
    let func = DecimalArbAddFunc::new();
    let lf = arb_field("l", 20, 2);
    let rf = arb_field("r", 20, 2);
    let arg_fields = vec![Arc::clone(&lf), Arc::clone(&rf)];
    let ret = func
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        })
        .unwrap();
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(raw("l", 20, 2, &[]))),
                ColumnarValue::Scalar(ScalarValue::LargeBinary(None)),
            ],
            arg_fields,
            number_rows: 0,
            return_field: ret,
            config_options: cfg(),
        })
        .unwrap();
    match out {
        ColumnarValue::Array(a) => assert_eq!(
            a.len(),
            0,
            "a NULL literal must not resurrect rows in an empty batch"
        ),
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
#[ignore = "FINDING: broadcast_len's fallback arm returns max(0, n) for a (0, n) operand pair, and invoke_binary then indexes past the end of the empty operand — an out-of-bounds panic instead of a typed error"]
fn mismatched_operand_lengths_around_zero_produce_an_error_not_a_panic() {
    let res = invoke_binary_udf(
        &DecimalArbAddFunc::new(),
        raw("l", 20, 2, &[]),
        20,
        2,
        raw("r", 20, 2, &[Some("1.00"), Some("2.00"), Some("3.00")]),
        20,
        2,
        3,
    );
    assert!(
        res.is_err(),
        "an impossible (0, 3) operand pair must surface as a typed error, not a panic"
    );
}

#[test]
fn an_empty_column_name_still_produces_an_error_not_a_panic() {
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "", 3, 0).unwrap();
    let err = expect_err(b.append_str("99999"), "overflow with an empty column name");
    assert!(
        !err.to_string().is_empty(),
        "the error message must not be empty"
    );
}

#[test]
fn an_empty_field_name_is_still_a_valid_decimal_arb_field() {
    let f = DecimalArbType::field("", 30, 2, true).unwrap();
    assert!(DecimalArbType::is_decimal_arb_field(&f));
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((30, 2))
    );
}

// ===========================================================================
// O. Nullability is *trusted*, not verified — pin the trust boundary
// ===========================================================================

#[test]
fn to_string_udf_trusts_a_non_nullable_input_field_even_when_the_data_has_nulls() {
    // The UDF mirrors the declared nullability of its input field; it does not
    // scan the validity buffer. So a lying (non-nullable field, null-bearing
    // array) pair yields a non-nullable output field over null data. Arrow's
    // RecordBatch construction is the layer that catches the contradiction —
    // pinned here so a change to either half is visible.
    let input: FieldRef = Arc::new(DecimalArbType::field("x", 30, 2, false).unwrap());
    let ret = DecimalArbToStringFunc::new()
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&input),
            scalar_arguments: &[None],
        })
        .unwrap();
    assert!(!ret.is_nullable());

    let out = DecimalArbToStringFunc::new()
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(raw("x", 30, 2, &[None])))],
            arg_fields: vec![Arc::clone(&input)],
            number_rows: 1,
            return_field: Arc::clone(&ret),
            config_options: cfg(),
        })
        .unwrap();
    let arr = match out {
        ColumnarValue::Array(a) => a,
        other => panic!("expected array, got {other:?}"),
    };
    assert_eq!(arr.null_count(), 1, "the NULL row survives the conversion");

    // …and the contradiction is caught when the column meets its schema.
    let schema = Arc::new(Schema::new(vec![(*ret).clone()]));
    assert!(
        RecordBatch::try_new(schema, vec![arr]).is_err(),
        "a non-nullable declared column holding a NULL must be rejected somewhere"
    );
}

#[test]
fn binary_op_output_is_always_nullable_even_for_non_nullable_inputs() {
    // Widening direction is the safe one: NULL can appear in the output of an
    // op over two non-nullable columns only via errors, never silently — but
    // declaring the result nullable costs nothing and cannot corrupt a sink.
    let lf: FieldRef = Arc::new(DecimalArbType::field("l", 20, 2, false).unwrap());
    let rf: FieldRef = Arc::new(DecimalArbType::field("r", 20, 2, false).unwrap());
    let arg_fields = vec![lf, rf];
    let ret = DecimalArbAddFunc::new()
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        })
        .unwrap();
    assert!(ret.is_nullable());
    assert!(DecimalArbType::is_decimal_arb_field(&ret));
}

#[test]
fn comparison_output_field_is_always_nullable() {
    let lf: FieldRef = Arc::new(DecimalArbType::field("l", 20, 2, false).unwrap());
    let rf: FieldRef = Arc::new(DecimalArbType::field("r", 20, 2, false).unwrap());
    let arg_fields = vec![lf, rf];
    let ret = DecimalArbEqFunc::new()
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        })
        .unwrap();
    assert!(
        ret.is_nullable(),
        "three-valued comparison output must be declared nullable"
    );
    assert_eq!(ret.data_type(), &DataType::Boolean);
}

#[test]
fn to_decimal128_udf_return_field_mirrors_non_nullable_input() {
    let input: FieldRef = Arc::new(DecimalArbType::field("x", 30, 2, false).unwrap());
    let p = ScalarValue::Int64(Some(20));
    let s = ScalarValue::Int64(Some(2));
    let arg_fields = vec![
        Arc::clone(&input),
        Arc::new(Field::new("p", DataType::Int64, false)) as FieldRef,
        Arc::new(Field::new("s", DataType::Int64, false)) as FieldRef,
    ];
    let ret = DecimalArbToDecimal128Func::new()
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, Some(&p), Some(&s)],
        })
        .unwrap();
    assert!(!ret.is_nullable());
    assert_eq!(ret.data_type(), &DataType::Decimal128(20, 2));
}

#[test]
fn sort_key_output_field_is_nullable_and_carries_no_decimal_arb_metadata() {
    let input: FieldRef = Arc::new(DecimalArbType::field("x", 30, 2, false).unwrap());
    let arg_fields = vec![input];
    let ret = DecimalArbSortKeyFunc::new()
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        })
        .unwrap();
    assert!(ret.is_nullable(), "a NULL row has no sort key");
    assert!(
        !DecimalArbType::is_decimal_arb_field(&ret),
        "a sort key is not a decimal_arb value and must not claim to be one"
    );
}

#[test]
fn an_all_null_column_declared_non_nullable_fails_the_same_way_as_a_single_null() {
    let field = DecimalArbType::field("amount", 30, 2, false).unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let one = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(raw("amount", 30, 2, &[Some("1.00"), None]))],
    );
    let all = RecordBatch::try_new(schema, vec![Arc::new(raw("amount", 30, 2, &[None, None]))]);
    assert!(one.is_err() && all.is_err());
}
