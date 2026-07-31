//! Adversarial pass, agent 16 — **cast correctness in every direction**.
//!
//! Scope (all in-process, milliseconds, no k3s / network / fs):
//!
//! * `Utf8 -> decimal_arb`: the *acceptance set* of `DecimalArbValue::from_str`
//!   / `DecimalArbArrayBuilder::append_str` / `DecimalArbArray::from_string_array`
//!   / `to_decimal_arb_from_string`. Leading `+`, leading/trailing zeros,
//!   exponent notation, whitespace, empty string, non-numeric junk, multiple
//!   signs or dots, non-ASCII digits, extremely long digit strings.
//! * `decimal_arb -> Utf8`: `to_string_array` / `decimal_arb_to_string`.
//!   Scale fidelity, sign, NULLs, never-exponent form, re-parseability.
//! * `decimal_arb <-> Decimal128/Decimal256` at and beyond the 38 / 76 digit
//!   range limits and at the scale limits.
//! * `decimal_arb <- integer` at every integer type boundary.
//! * Float inputs: must be *rejected*, never silently truncated.
//!
//! Invariant under test throughout: **every lossy or unparseable cast must
//! `Err` with an actionable message; every lossless cast must round-trip
//! exactly.**
//!
//! Tests marked `#[ignore = "FINDING: ..."]` demonstrate a real product defect
//! and are left failing-but-ignored on purpose; do not "fix" them by weakening
//! the assertion.

use arrow::array::{
    Array, Decimal128Array, Decimal256Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::datatypes::i256 as ArrowI256;
use arrow_schema::{Field, FieldRef};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
};
use datafusion::scalar::ScalarValue;
use std::str::FromStr;
use std::sync::Arc;
use streamling_common::functions::decimal_arb_ops::{
    DecimalArbToDecimal128Func, DecimalArbToDecimal256Func, DecimalArbToStringFunc,
    ToDecimalArbFromDecimal128Func, ToDecimalArbFromDecimal256Func, ToDecimalArbFromIntFunc,
    ToDecimalArbFromStringFunc,
};
use streamling_common::types::decimal_arb::{
    DecimalArbArray, DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, MAX_PRECISION,
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn v(s: &str) -> DecimalArbValue {
    DecimalArbValue::from_str(s).unwrap_or_else(|e| panic!("expected {s:?} to parse: {e}"))
}

fn arb(column: &str, p: u32, s: u32, values: &[Option<&str>]) -> DecimalArbArray {
    let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), column, p, s)
        .unwrap_or_else(|e| panic!("builder ({p},{s}): {e}"));
    for x in values {
        match x {
            Some(t) => b
                .append_str(t)
                .unwrap_or_else(|e| panic!("append {t:?} at ({p},{s}): {e}")),
            None => b.append_null(),
        }
    }
    b.finish()
}

fn raw(column: &str, p: u32, s: u32, values: &[Option<&str>]) -> LargeBinaryArray {
    arb(column, p, s, values).into_inner().0
}

fn strings_of(a: &StringArray) -> Vec<Option<String>> {
    (0..a.len())
        .map(|i| {
            if a.is_null(i) {
                None
            } else {
                Some(a.value(i).to_string())
            }
        })
        .collect()
}

/// `unwrap_err().to_string()` for results whose `Ok` type is not `Debug`.
fn err_of<T, E: std::fmt::Display>(r: std::result::Result<T, E>) -> String {
    match r {
        Ok(_) => panic!("expected an error, got Ok"),
        Err(e) => e.to_string(),
    }
}

fn i64_field(name: &str) -> FieldRef {
    Arc::new(Field::new(name, DataType::Int64, false))
}

fn cfg() -> Arc<datafusion::config::ConfigOptions> {
    Arc::new(datafusion::config::ConfigOptions::default())
}

/// Build `ScalarFunctionArgs` for a UDF invocation.
fn sfa(
    args: Vec<ColumnarValue>,
    arg_fields: Vec<FieldRef>,
    number_rows: usize,
    return_field: FieldRef,
) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows,
        return_field,
        config_options: cfg(),
    }
}

fn as_array(cv: ColumnarValue) -> Arc<dyn Array> {
    match cv {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => s.to_array().expect("scalar to array"),
    }
}

/// Decode a `LargeBinaryArray` produced by a UDF back into canonical strings.
fn decode_arb(a: &Arc<dyn Array>, p: u32, s: u32) -> Vec<Option<String>> {
    let lba = a
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("decimal_arb output must be LargeBinary");
    let field = DecimalArbType::field("decoded", p, s, true).unwrap();
    let arb = DecimalArbArray::try_from_array_and_field(lba.clone(), &field).unwrap();
    strings_of(&arb.to_string_array().unwrap())
}

/// A long decimal string: `digits` integer digits (leading `1`, rest `7`).
fn long_digits(digits: usize) -> String {
    let mut s = String::with_capacity(digits);
    s.push('1');
    for _ in 1..digits {
        s.push('7');
    }
    s
}

// ===========================================================================
// A. Utf8 -> decimal_arb : the acceptance set
// ===========================================================================

#[test]
fn leading_plus_parses_and_equals_the_unsigned_spelling() {
    assert_eq!(v("+5"), v("5"), "'+5' must parse to the same value as '5'");
    assert_eq!(v("+0.25"), v("0.25"));
    assert_eq!(
        v("+5").to_canonical_bytes_at_scale(4),
        v("5").to_canonical_bytes_at_scale(4),
        "'+5' and '5' must encode to identical bytes (group/join key identity)",
    );
}

#[test]
fn leading_zeros_are_not_significant() {
    assert_eq!(v("0005"), v("5"));
    assert_eq!(v("-0005.5"), v("-5.5"));
    assert_eq!(v("0000").to_canonical_string(), "0");
}

#[test]
fn trailing_fractional_zeros_do_not_change_the_value() {
    assert_eq!(v("5.000"), v("5"));
    assert_eq!(
        v("5.000").fractional_digit_count(),
        0,
        "trailing zeros are not significant fractional digits",
    );
    assert_eq!(
        v("5.000").to_canonical_bytes_at_scale(3),
        v("5").to_canonical_bytes_at_scale(3),
    );
}

#[test]
fn bare_leading_dot_is_accepted_as_a_fraction() {
    let x = v(".5");
    assert_eq!(x, v("0.5"), "'.5' must equal '0.5'");
    assert_eq!(x.to_canonical_string(), "0.5");
    assert_eq!(x.integer_digit_count(), 0);
}

#[test]
fn bare_trailing_dot_is_accepted_as_an_integer() {
    assert_eq!(v("5."), v("5"), "'5.' must equal '5'");
    assert_eq!(v("5.").fractional_digit_count(), 0);
}

#[test]
fn lowercase_exponent_notation_is_accepted_and_expanded() {
    assert_eq!(v("1e3").to_canonical_string(), "1000");
    assert_eq!(v("1e3"), v("1000"));
}

#[test]
fn uppercase_exponent_notation_is_accepted_and_expanded() {
    assert_eq!(v("1E3"), v("1000"));
    assert_eq!(v("2.5E2"), v("250"));
}

#[test]
fn explicit_positive_exponent_is_accepted() {
    assert_eq!(v("1e+3"), v("1000"));
    assert_eq!(v("+1e+3"), v("1000"));
}

#[test]
fn negative_exponent_expands_to_a_fraction_not_exponent_text() {
    let x = v("1.2e-3");
    assert_eq!(
        x.to_canonical_string(),
        "0.0012",
        "canonical text must never be exponent notation",
    );
    assert_eq!(x.fractional_digit_count(), 4);
}

#[test]
fn empty_string_is_rejected() {
    let err = DecimalArbValue::from_str("").unwrap_err().to_string();
    assert!(
        err.contains("decimal_arb"),
        "empty-string parse error must name decimal_arb: {err}",
    );
}

#[test]
fn leading_whitespace_is_rejected() {
    assert!(
        DecimalArbValue::from_str(" 5").is_err(),
        "leading whitespace must not be silently trimmed",
    );
    assert!(DecimalArbValue::from_str("\t5").is_err());
}

#[test]
fn trailing_whitespace_is_rejected() {
    assert!(
        DecimalArbValue::from_str("5 ").is_err(),
        "trailing whitespace must not be silently trimmed",
    );
    assert!(DecimalArbValue::from_str("5\n").is_err());
    assert!(DecimalArbValue::from_str("5\r\n").is_err());
}

#[test]
fn interior_whitespace_is_rejected() {
    assert!(DecimalArbValue::from_str("1 000").is_err());
    assert!(DecimalArbValue::from_str("1. 5").is_err());
    assert!(DecimalArbValue::from_str("- 5").is_err());
}

#[test]
fn non_numeric_junk_is_rejected() {
    for s in ["abc", "not a number", "0x10", "1x", "x1", "5%", "$5", "5;"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} must be rejected as non-numeric",
        );
    }
}

#[test]
fn multiple_signs_are_rejected() {
    for s in ["--5", "++5", "+-5", "-+5", "5-", "5+", "-5-"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} (multiple/misplaced sign) must be rejected",
        );
    }
}

#[test]
fn multiple_decimal_points_are_rejected() {
    for s in ["1.2.3", "5..0", "..5", "1..", ".5.5"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} (multiple dots) must be rejected",
        );
    }
}

#[test]
fn a_lone_sign_or_dot_is_rejected() {
    for s in ["-", "+", ".", "-.", "+."] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} has no digits and must be rejected",
        );
    }
}

#[test]
fn dangling_or_empty_exponent_is_rejected() {
    for s in ["1e", "1e+", "1e-", "e5", "e", "1ee3", "1e3e4"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} (malformed exponent) must be rejected",
        );
    }
}

#[test]
fn fractional_exponent_is_rejected() {
    assert!(
        DecimalArbValue::from_str("1e2.5").is_err(),
        "a non-integer exponent must be rejected",
    );
}

#[test]
fn thousands_separators_are_rejected() {
    for s in ["1,000", "1'000", "1 000", "1.000,5"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} (locale separator) must be rejected",
        );
    }
}

#[test]
fn infinity_and_nan_spellings_are_rejected() {
    for s in ["inf", "-inf", "Infinity", "-Infinity", "NaN", "nan", "snan"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} is not a finite decimal and must be rejected",
        );
    }
}

#[test]
fn non_ascii_digits_are_rejected() {
    // Full-width (U+FF10..), Arabic-Indic (U+0660..), Devanagari (U+0966..).
    for s in ["１２３", "١٢٣", "१२३", "١.٥"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} (non-ASCII digits) must be rejected",
        );
    }
}

#[test]
fn invisible_characters_are_rejected() {
    for s in ["1.5\u{feff}", "\u{feff}1.5", "1.5\u{200b}", "1\u{0}"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} (invisible char) must be rejected",
        );
    }
}

#[test]
fn parse_error_quotes_the_offending_input() {
    let err = DecimalArbValue::from_str("12abc").unwrap_err().to_string();
    assert!(
        err.contains("12abc"),
        "parse error must quote the offending text so an operator can find the bad row: {err}",
    );
}

#[test]
#[ignore = "FINDING: '_' is silently accepted as a digit separator in every text->decimal_arb cast ('1_000' -> 1000)"]
fn underscore_digit_separator_is_rejected() {
    for s in ["1_000", "1_0_0", "1__0", "1_000.000_1"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?}: '_' is not valid in a SQL decimal literal and must be rejected, \
             not silently stripped",
        );
    }
}

#[test]
#[ignore = "FINDING: trailing/misplaced '_' is silently accepted ('1_' -> 1, '1_.5' -> 1.5, '1_e3' -> 1000)"]
fn misplaced_underscore_is_rejected() {
    for s in ["1_", "1_.5", "1._5", "1.5_", "1_e3"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?}: misplaced '_' must be rejected",
        );
    }
}

#[test]
fn leading_underscore_is_rejected() {
    // Documents the *inconsistency*: leading '_' errors while interior '_' does not.
    for s in ["_1", "-_5", "+_"] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} must be rejected",
        );
    }
}

#[test]
fn a_500_digit_integer_round_trips_through_text() {
    let s = long_digits(500);
    let x = v(&s);
    assert_eq!(x.integer_digit_count(), 500);
    assert_eq!(
        x.to_canonical_string(),
        s,
        "a 500-digit integer must survive text -> value -> text unchanged",
    );
}

#[test]
fn a_500_digit_fraction_round_trips_through_text() {
    let s = format!("0.{}", long_digits(500));
    let x = v(&s);
    assert_eq!(x.fractional_digit_count(), 500);
    assert_eq!(x.to_canonical_string(), s);
}

#[test]
fn a_value_wider_than_max_precision_is_rejected_by_check_fits() {
    let s = long_digits(MAX_PRECISION as usize + 1);
    let x = v(&s);
    let err = x
        .check_fits(MAX_PRECISION, 0, "wide")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("wide") && err.contains("integer"),
        "over-wide value must be rejected with a column-naming message: {err}",
    );
}

#[test]
fn an_astronomically_large_exponent_is_rejected_without_materializing_the_digits() {
    // "1e1000000000" parses (BigDecimal keeps the exponent lazily). It must be
    // *rejected* by check_fits — and the rejection must not try to render a
    // billion digits into the error message.
    let x = DecimalArbValue::from_str("1e1000000000").expect("exponent form parses");
    assert_eq!(x.integer_digit_count(), 1_000_000_001);
    let err = x
        .check_fits(MAX_PRECISION, 0, "huge")
        .unwrap_err()
        .to_string();
    assert!(
        err.len() < 4096,
        "error message must not materialize the full expansion (len {})",
        err.len(),
    );
    assert!(err.contains("huge"), "error must name the column: {err}");
}

#[test]
fn an_astronomically_small_exponent_is_rejected_by_scale() {
    let x = DecimalArbValue::from_str("1.5e-1000000000").expect("exponent form parses");
    let err = x
        .check_fits(MAX_PRECISION, 18, "tiny")
        .unwrap_err()
        .to_string();
    assert!(err.len() < 4096, "error message must stay bounded");
    assert!(
        err.contains("tiny") && err.contains("fractional"),
        "must be rejected on scale with a column-naming message: {err}",
    );
}

// ===========================================================================
// B. decimal_arb -> Utf8
// ===========================================================================

#[test]
fn to_string_array_pads_every_value_to_the_column_scale() {
    let a = arb("x", 20, 4, &[Some("1"), Some("-1"), Some("0.5")]);
    assert_eq!(
        strings_of(&a.to_string_array().unwrap()),
        vec![
            Some("1.0000".into()),
            Some("-1.0000".into()),
            Some("0.5000".into())
        ],
        "text cast must render exactly `scale` fractional digits",
    );
}

#[test]
#[ignore = "FINDING: zero renders as \"0\" instead of \"0.0000\" — the Utf8 cast drops scale padding for zero only"]
fn to_string_array_pads_zero_to_the_column_scale_like_every_other_value() {
    let a = arb(
        "x",
        20,
        4,
        &[Some("0"), Some("-0"), Some("0.0000"), Some("1")],
    );
    assert_eq!(
        strings_of(&a.to_string_array().unwrap()),
        vec![
            Some("0.0000".into()),
            Some("0.0000".into()),
            Some("0.0000".into()),
            Some("1.0000".into())
        ],
        "every row of a decimal_arb(20,4) column must render with 4 fractional \
         digits; zero must not be special-cased",
    );
}

#[test]
fn to_string_array_at_scale_zero_emits_bare_integers() {
    let a = arb("x", 20, 0, &[Some("0"), Some("1"), Some("-42")]);
    assert_eq!(
        strings_of(&a.to_string_array().unwrap()),
        vec![Some("0".into()), Some("1".into()), Some("-42".into())],
    );
}

#[test]
fn to_string_array_preserves_nulls() {
    let a = arb("x", 20, 2, &[Some("1"), None, Some("-1"), None]);
    let s = a.to_string_array().unwrap();
    assert_eq!(s.len(), 4);
    assert!(s.is_null(1) && s.is_null(3), "NULLs must stay NULL");
    assert!(!s.is_null(0) && !s.is_null(2));
}

#[test]
fn to_string_array_on_an_empty_array_yields_an_empty_string_array() {
    let a = arb("x", 20, 2, &[]);
    let s = a.to_string_array().unwrap();
    assert_eq!(s.len(), 0, "empty in, empty out");
}

#[test]
fn to_string_array_never_emits_exponent_notation() {
    // A value with 100 leading fractional zeros and one with 120 integer digits:
    // plain form is mandatory for Postgres NUMERIC / JSON digit-string consumers.
    let tiny = format!("0.{}1", "0".repeat(120));
    let huge = long_digits(120);
    let a = arb("x", 400, 130, &[Some(&tiny), Some(&huge)]);
    let s = a.to_string_array().unwrap();
    for i in 0..s.len() {
        let t = s.value(i);
        assert!(
            !t.contains('e') && !t.contains('E'),
            "row {i} rendered in exponent form: {t}",
        );
    }
}

#[test]
fn to_string_array_output_reparses_to_the_same_value() {
    let inputs = [
        "0",
        "1",
        "-1",
        "0.5",
        "-0.0001",
        "123456789.987654321",
        "-99999999999999999999",
    ];
    let a = arb(
        "x",
        200,
        18,
        &inputs.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
    );
    let s = a.to_string_array().unwrap();
    for i in 0..a.len() {
        let original = a.value(i).unwrap().unwrap();
        let reparsed = v(s.value(i));
        assert_eq!(
            original, reparsed,
            "row {i}: decimal_arb -> Utf8 -> decimal_arb must be identity",
        );
    }
}

#[test]
fn to_string_array_renders_negative_sign_exactly_once() {
    let a = arb("x", 20, 4, &[Some("-0.0001"), Some("-1234.5")]);
    let s = a.to_string_array().unwrap();
    assert_eq!(s.value(0), "-0.0001");
    assert_eq!(s.value(1), "-1234.5000");
    assert_eq!(s.value(0).matches('-').count(), 1);
}

#[test]
fn to_string_array_of_a_500_digit_column_is_exact() {
    let big = long_digits(500);
    let a = arb("x", 600, 0, &[Some(&big)]);
    assert_eq!(a.to_string_array().unwrap().value(0), big);
}

#[test]
fn decimal_arb_to_string_udf_matches_the_array_helper() {
    let values = [Some("12.3456"), None, Some("-0.0001"), Some("7")];
    let a = arb("x", 100, 4, &values);
    let expected = strings_of(&a.to_string_array().unwrap());

    let f = DecimalArbToStringFunc::new();
    let out = f
        .invoke_with_args(sfa(
            vec![ColumnarValue::Array(Arc::new(raw("x", 100, 4, &values)))],
            vec![Arc::new(DecimalArbType::field("x", 100, 4, true).unwrap())],
            values.len(),
            Arc::new(Field::new("out", DataType::Utf8, true)),
        ))
        .unwrap();
    let got = as_array(out);
    let got = got.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        strings_of(got),
        expected,
        "the SQL text cast and the array helper must agree row for row",
    );
}

#[test]
fn decimal_arb_to_string_udf_rejects_a_field_without_extension_metadata() {
    let f = DecimalArbToStringFunc::new();
    let err = f
        .invoke_with_args(sfa(
            vec![ColumnarValue::Array(Arc::new(raw(
                "x",
                10,
                0,
                &[Some("1")],
            )))],
            vec![Arc::new(Field::new("blob", DataType::LargeBinary, true))],
            1,
            Arc::new(Field::new("out", DataType::Utf8, true)),
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("blob") && err.contains("decimal_arb"),
        "must name the offending column and the expected type: {err}",
    );
}

#[test]
fn decimal_arb_to_string_udf_rejects_non_large_binary_input() {
    let f = DecimalArbToStringFunc::new();
    let err = f
        .invoke_with_args(sfa(
            vec![ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("1"),
            ])))],
            vec![Arc::new(DecimalArbType::field("x", 10, 0, true).unwrap())],
            1,
            Arc::new(Field::new("out", DataType::Utf8, true)),
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LargeBinary"), "message must say why: {err}");
}

#[test]
fn decimal_arb_to_string_udf_preserves_non_nullability() {
    let f = DecimalArbToStringFunc::new();
    let non_null = Arc::new(DecimalArbType::field("x", 10, 0, false).unwrap());
    let fields: Vec<FieldRef> = vec![non_null];
    let scalars: Vec<Option<&ScalarValue>> = vec![None];
    let out = f
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        })
        .unwrap();
    assert_eq!(out.data_type(), &DataType::Utf8);
    assert!(
        !out.is_nullable(),
        "casting a NOT NULL decimal_arb to text must stay NOT NULL",
    );
}

#[test]
fn decimal_arb_to_string_udf_reports_the_failing_row_on_corrupt_bytes() {
    // Row 1 carries an invalid sign byte; the error must localize it.
    let bad = LargeBinaryArray::from(vec![
        Some(&[0x00u8, 0x01u8][..]),
        Some(&[0x42u8, 0x01u8][..]),
    ]);
    let f = DecimalArbToStringFunc::new();
    let err = f
        .invoke_with_args(sfa(
            vec![ColumnarValue::Array(Arc::new(bad))],
            vec![Arc::new(DecimalArbType::field("x", 10, 0, true).unwrap())],
            2,
            Arc::new(Field::new("out", DataType::Utf8, true)),
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("row 1"),
        "error must localize the corrupt row: {err}",
    );
}

#[test]
#[ignore = "FINDING: decimal_arb_to_string returns a length-1 array for a scalar input, ignoring number_rows"]
fn decimal_arb_to_string_udf_honours_number_rows_for_a_scalar_input() {
    let bytes = raw("x", 20, 4, &[Some("1.5")]).value(0).to_vec();
    let f = DecimalArbToStringFunc::new();
    let out = f
        .invoke_with_args(sfa(
            vec![ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(bytes)))],
            vec![Arc::new(DecimalArbType::field("x", 20, 4, true).unwrap())],
            3,
            Arc::new(Field::new("out", DataType::Utf8, true)),
        ))
        .unwrap();
    match out {
        ColumnarValue::Scalar(_) => {}
        ColumnarValue::Array(a) => assert_eq!(
            a.len(),
            3,
            "a UDF returning an Array must return number_rows rows, or return a Scalar",
        ),
    }
}

// ===========================================================================
// C. Utf8 -> decimal_arb (array + UDF)
// ===========================================================================

#[test]
fn from_string_array_preserves_nulls_positionally() {
    let src = StringArray::from(vec![Some("1"), None, Some("-2"), None, Some("0")]);
    let a = DecimalArbArray::from_string_array(&src, 20, 2, "c").unwrap();
    assert_eq!(a.len(), 5);
    for i in 0..5 {
        assert_eq!(
            src.is_null(i),
            a.is_null(i),
            "row {i}: NULL-ness must be preserved by the text cast",
        );
    }
}

#[test]
fn from_string_array_on_empty_input_yields_an_empty_array() {
    let src = StringArray::from(Vec::<Option<&str>>::new());
    let a = DecimalArbArray::from_string_array(&src, 20, 2, "c").unwrap();
    assert!(a.is_empty());
    assert_eq!((a.precision(), a.scale()), (20, 2));
}

#[test]
fn from_string_array_normalizes_exponent_and_padding_spellings() {
    let src = StringArray::from(vec![
        Some("1e3"),
        Some("1000"),
        Some("+1000.00"),
        Some("01000"),
    ]);
    let a = DecimalArbArray::from_string_array(&src, 20, 4, "c").unwrap();
    let first = a.value(0).unwrap().unwrap();
    for i in 1..a.len() {
        assert_eq!(
            a.value(i).unwrap().unwrap(),
            first,
            "row {i}: all spellings of 1000 must land on one value",
        );
    }
    let bytes0 = raw("c", 20, 4, &[Some("1e3")]).value(0).to_vec();
    let bytes3 = raw("c", 20, 4, &[Some("01000")]).value(0).to_vec();
    assert_eq!(bytes0, bytes3, "and on identical bytes (group/join keys)");
}

#[test]
fn from_string_array_rejects_a_value_wider_than_the_declared_precision() {
    let src = StringArray::from(vec![Some("1234")]);
    let err = err_of(DecimalArbArray::from_string_array(&src, 3, 0, "amount"));
    assert!(
        err.contains("amount") && err.contains("1234"),
        "over-precision text cast must name the column and the value: {err}",
    );
}

#[test]
fn from_string_array_rejects_more_fractional_digits_than_the_declared_scale() {
    let src = StringArray::from(vec![Some("1.2345")]);
    let err = err_of(DecimalArbArray::from_string_array(&src, 20, 2, "amount"));
    assert!(
        err.contains("amount") && err.contains("scale"),
        "over-scale text cast must name the column and mention scale: {err}",
    );
}

#[test]
fn from_string_array_rejects_garbage_rather_than_nulling_it_out() {
    for bad in ["not-a-number", "", " 7", "1.2.3", "NaN"] {
        let src = StringArray::from(vec![Some("1"), Some(bad)]);
        assert!(
            DecimalArbArray::from_string_array(&src, 20, 2, "c").is_err(),
            "{bad:?} must abort the cast, never become NULL or 0",
        );
    }
}

#[test]
#[ignore = "FINDING: from_string_array/append_str parse errors name neither the column nor the row index"]
fn from_string_array_parse_error_names_the_column_and_row() {
    let src = StringArray::from(vec![Some("1"), Some("2"), Some("oops")]);
    let err = err_of(DecimalArbArray::from_string_array(&src, 20, 2, "amount"));
    assert!(
        err.contains("amount"),
        "a parse failure must name the column like every other cast error does: {err}",
    );
    assert!(
        err.contains('2'),
        "a parse failure must localize the row in a million-row batch: {err}",
    );
}

#[test]
fn text_cast_round_trips_exactly_when_the_text_is_already_canonical() {
    let inputs = vec![
        Some("1.0000"),
        None,
        Some("-0.0001"),
        Some("123456.7890"),
        Some("-99999999.9999"),
    ];
    let src = StringArray::from(inputs.clone());
    let a = DecimalArbArray::from_string_array(&src, 40, 4, "c").unwrap();
    assert_eq!(
        strings_of(&a.to_string_array().unwrap()),
        inputs
            .iter()
            .map(|x| x.map(|s| s.to_string()))
            .collect::<Vec<_>>(),
        "canonical text must survive Utf8 -> decimal_arb -> Utf8 byte-for-byte",
    );
}

#[test]
fn to_decimal_arb_from_string_udf_stamps_the_declared_precision_and_scale() {
    let f = ToDecimalArbFromStringFunc::new();
    let p = ScalarValue::Int64(Some(30));
    let s = ScalarValue::Int64(Some(6));
    let fields: Vec<FieldRef> = vec![
        Arc::new(Field::new("t", DataType::Utf8, true)),
        i64_field("p"),
        i64_field("s"),
    ];
    let scalars: Vec<Option<&ScalarValue>> = vec![None, Some(&p), Some(&s)];
    let out = f
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        })
        .unwrap();
    assert!(
        DecimalArbType::is_decimal_arb_field(&out),
        "the text cast's output field must carry decimal_arb metadata",
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out),
        Some((30, 6)),
    );
}

#[test]
fn to_decimal_arb_from_string_udf_converts_values_and_nulls() {
    let f = ToDecimalArbFromStringFunc::new();
    let src = StringArray::from(vec![Some("1.5"), None, Some("-2.25"), Some("0")]);
    let out = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(src)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            vec![
                Arc::new(Field::new("t", DataType::Utf8, true)),
                i64_field("p"),
                i64_field("s"),
            ],
            4,
            Arc::new(DecimalArbType::field("out", 20, 2, true).unwrap()),
        ))
        .unwrap();
    assert_eq!(
        decode_arb(&as_array(out), 20, 2),
        vec![
            Some("1.50".into()),
            None,
            Some("-2.25".into()),
            Some("0".into())
        ],
    );
}

#[test]
fn to_decimal_arb_from_string_udf_rejects_a_negative_precision_literal() {
    let f = ToDecimalArbFromStringFunc::new();
    let err = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("1")]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            vec![
                Arc::new(Field::new("t", DataType::Utf8, true)),
                i64_field("p"),
                i64_field("s"),
            ],
            1,
            Arc::new(DecimalArbType::field("out", 20, 0, true).unwrap()),
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("non-negative"),
        "must explain the constraint: {err}",
    );
}

#[test]
fn to_decimal_arb_from_string_udf_rejects_a_non_literal_precision() {
    let f = ToDecimalArbFromStringFunc::new();
    let err = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("1")]))),
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(20)]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            vec![
                Arc::new(Field::new("t", DataType::Utf8, true)),
                i64_field("p"),
                i64_field("s"),
            ],
            1,
            Arc::new(DecimalArbType::field("out", 20, 0, true).unwrap()),
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("literal"),
        "a per-row precision is not expressible; say so: {err}",
    );
}

#[test]
fn to_decimal_arb_from_string_udf_rejects_scale_greater_than_precision_at_plan_time() {
    let f = ToDecimalArbFromStringFunc::new();
    let p = ScalarValue::Int64(Some(4));
    let s = ScalarValue::Int64(Some(9));
    let fields: Vec<FieldRef> = vec![
        Arc::new(Field::new("t", DataType::Utf8, true)),
        i64_field("p"),
        i64_field("s"),
    ];
    let scalars: Vec<Option<&ScalarValue>> = vec![None, Some(&p), Some(&s)];
    let err = f
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        })
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("scale") && err.contains("precision"),
        "invalid (p,s) must fail at planning time: {err}",
    );
}

#[test]
fn to_decimal_arb_from_string_udf_rejects_precision_above_max() {
    let f = ToDecimalArbFromStringFunc::new();
    let over = (MAX_PRECISION as i64) + 1;
    let err = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("1")]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(over))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            vec![
                Arc::new(Field::new("t", DataType::Utf8, true)),
                i64_field("p"),
                i64_field("s"),
            ],
            1,
            Arc::new(DecimalArbType::field("out", 100, 0, true).unwrap()),
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("exceeds maximum"),
        "precision above MAX_PRECISION must be rejected: {err}",
    );
}

#[test]
fn to_decimal_arb_from_string_udf_rejects_non_utf8_input() {
    let f = ToDecimalArbFromStringFunc::new();
    let err = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(1)]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            vec![
                Arc::new(Field::new("t", DataType::Int64, true)),
                i64_field("p"),
                i64_field("s"),
            ],
            1,
            Arc::new(DecimalArbType::field("out", 20, 0, true).unwrap()),
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("Utf8"), "must state the expected type: {err}");
}

#[test]
fn to_decimal_arb_from_string_udf_aborts_on_a_bad_row_rather_than_nulling_it() {
    let f = ToDecimalArbFromStringFunc::new();
    let err = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec![Some("1"), Some("bogus")]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            vec![
                Arc::new(Field::new("t", DataType::Utf8, true)),
                i64_field("p"),
                i64_field("s"),
            ],
            2,
            Arc::new(DecimalArbType::field("out", 20, 0, true).unwrap()),
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("bogus"), "must quote the bad text: {err}");
}

#[test]
#[ignore = "FINDING: to_decimal_arb_from_string returns a length-1 array for a scalar input, ignoring number_rows"]
fn to_decimal_arb_from_string_udf_honours_number_rows_for_a_scalar_input() {
    let f = ToDecimalArbFromStringFunc::new();
    let out = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("1.5".into()))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            vec![
                Arc::new(Field::new("t", DataType::Utf8, true)),
                i64_field("p"),
                i64_field("s"),
            ],
            4,
            Arc::new(DecimalArbType::field("out", 20, 4, true).unwrap()),
        ))
        .unwrap();
    match out {
        ColumnarValue::Scalar(_) => {}
        ColumnarValue::Array(a) => assert_eq!(
            a.len(),
            4,
            "a UDF returning an Array must return number_rows rows, or return a Scalar",
        ),
    }
}

// ===========================================================================
// D. decimal_arb <-> Decimal128 / Decimal256
// ===========================================================================

#[test]
fn from_decimal128_widens_losslessly_and_preserves_nulls() {
    let src = Decimal128Array::from(vec![Some(1234_i128), None, Some(-9876_i128)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let a = DecimalArbArray::from_decimal128(&src, 2, 100, 2, "amount").unwrap();
    assert_eq!(
        strings_of(&a.to_string_array().unwrap()),
        vec![Some("12.34".into()), None, Some("-98.76".into())],
    );
}

#[test]
fn from_decimal128_handles_i128_min_and_max_exactly() {
    let src = Decimal128Array::from(vec![Some(i128::MIN), Some(i128::MAX)])
        .with_precision_and_scale(38, 0)
        .unwrap();
    let a = DecimalArbArray::from_decimal128(&src, 0, 60, 0, "c").unwrap();
    assert_eq!(
        strings_of(&a.to_string_array().unwrap()),
        vec![Some(i128::MIN.to_string()), Some(i128::MAX.to_string())],
        "the widening cast must be exact at the i128 extremes",
    );
}

#[test]
fn from_decimal128_with_a_negative_source_scale_multiplies_out() {
    let src = Decimal128Array::from(vec![Some(1234_i128)])
        .with_precision_and_scale(10, -2)
        .unwrap();
    let a = DecimalArbArray::from_decimal128(&src, -2, 40, 0, "c").unwrap();
    assert_eq!(
        a.to_string_array().unwrap().value(0),
        "123400",
        "Decimal128(p, -2) means value x 100",
    );
}

#[test]
fn from_decimal128_rejects_values_that_do_not_fit_the_target_precision() {
    // Arrow does not validate values against the declared precision, so a
    // Decimal128(3, 0) array can legally hold 99999. The cast must reject it,
    // never truncate.
    let src = Decimal128Array::from(vec![Some(99999_i128)])
        .with_precision_and_scale(3, 0)
        .unwrap();
    let err = err_of(DecimalArbArray::from_decimal128(&src, 0, 3, 0, "amount"));
    assert!(
        err.contains("amount") && err.contains("99999"),
        "over-wide source value must be reported, not truncated: {err}",
    );
}

#[test]
fn from_decimal128_rejects_narrowing_the_scale() {
    let src = Decimal128Array::from(vec![Some(1234_i128)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let err = err_of(DecimalArbArray::from_decimal128(&src, 2, 40, 0, "amount"));
    assert!(
        err.contains("amount") && err.contains("scale"),
        "12.34 cannot land in a scale-0 column silently: {err}",
    );
}

#[test]
fn from_decimal128_allows_scale_narrowing_when_the_digits_are_zero() {
    let src = Decimal128Array::from(vec![Some(1200_i128)])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let a = DecimalArbArray::from_decimal128(&src, 2, 40, 0, "c").unwrap();
    assert_eq!(a.to_string_array().unwrap().value(0), "12");
}

#[test]
#[ignore = "FINDING: from_decimal128 trusts the caller's source_scale over source.scale(), silently rescaling by powers of ten"]
fn from_decimal128_uses_the_arrays_own_scale_not_a_caller_supplied_one() {
    let src = Decimal128Array::from(vec![Some(123456_i128)])
        .with_precision_and_scale(10, 4)
        .unwrap(); // true value 12.3456
    let a = DecimalArbArray::from_decimal128(&src, 2, 40, 4, "c").unwrap();
    assert_eq!(
        a.to_string_array().unwrap().value(0),
        "12.3456",
        "the source array already knows its scale; a disagreeing parameter must \
         be rejected, not silently applied (got a 100x error)",
    );
}

#[test]
fn from_decimal256_widens_losslessly_and_preserves_nulls() {
    let src = Decimal256Array::from(vec![Some(ArrowI256::from_i128(123_456_789_012)), None])
        .with_precision_and_scale(40, 5)
        .unwrap();
    let a = DecimalArbArray::from_decimal256(&src, 5, 100, 5, "c").unwrap();
    assert_eq!(
        strings_of(&a.to_string_array().unwrap()),
        vec![Some("1234567.89012".into()), None],
    );
}

#[test]
fn from_decimal256_handles_i256_min_and_max_exactly() {
    let src = Decimal256Array::from(vec![Some(ArrowI256::MIN), Some(ArrowI256::MAX)])
        .with_precision_and_scale(76, 0)
        .unwrap();
    let a = DecimalArbArray::from_decimal256(&src, 0, 200, 0, "c").unwrap();
    let out = a.to_string_array().unwrap();
    assert_eq!(
        out.value(1),
        "57896044618658097711785492504343953926634992332820282019728792003956564819967",
        "i256::MAX must widen exactly",
    );
    assert_eq!(
        out.value(0),
        "-57896044618658097711785492504343953926634992332820282019728792003956564819968",
        "i256::MIN must widen exactly (no sign-extension bug)",
    );
}

#[test]
fn to_decimal128_narrows_exactly_when_the_value_fits() {
    let a = arb("x", 100, 4, &[Some("1.2345"), None, Some("-1.2345")]);
    let out = a.to_decimal128(38, 4, "x").unwrap();
    assert_eq!(out.value(0), 12345_i128);
    assert!(out.is_null(1), "NULLs must survive narrowing");
    assert_eq!(out.value(2), -12345_i128);
    assert_eq!(out.data_type(), &DataType::Decimal128(38, 4));
}

#[test]
fn to_decimal128_pads_when_widening_the_scale() {
    let a = arb("x", 100, 0, &[Some("7")]);
    let out = a.to_decimal128(38, 4, "x").unwrap();
    assert_eq!(
        out.value(0),
        70000_i128,
        "widening the scale must multiply, not reinterpret",
    );
}

#[test]
fn to_decimal128_rejects_precision_zero() {
    let a = arb("x", 10, 0, &[Some("1")]);
    let err = a.to_decimal128(0, 0, "amount").unwrap_err().to_string();
    assert!(
        err.contains("1..=38") && err.contains("amount"),
        "must state the legal range and the column: {err}",
    );
}

#[test]
fn to_decimal128_rejects_precision_above_38() {
    let a = arb("x", 10, 0, &[Some("1")]);
    let err = a.to_decimal128(39, 0, "amount").unwrap_err().to_string();
    assert!(err.contains("1..=38"), "must state the legal range: {err}");
}

#[test]
fn to_decimal128_rejects_scale_greater_than_precision() {
    let a = arb("x", 20, 4, &[Some("1.2345")]);
    let err = a.to_decimal128(2, 5, "amount").unwrap_err().to_string();
    assert!(
        err.contains("amount") && err.to_lowercase().contains("scale"),
        "an impossible Decimal128(2,5) target must be rejected: {err}",
    );
}

#[test]
fn to_decimal128_accepts_the_largest_38_digit_value() {
    let biggest = "9".repeat(38);
    let a = arb("x", 100, 0, &[Some(&biggest)]);
    let out = a.to_decimal128(38, 0, "x").unwrap();
    assert_eq!(out.value(0), i128::from_str(&biggest).unwrap());
}

#[test]
fn to_decimal128_rejects_the_smallest_39_digit_value() {
    let s = format!("1{}", "0".repeat(38)); // 10^38
    let a = arb("x", 100, 0, &[Some(&s)]);
    let err = a.to_decimal128(38, 0, "amount").unwrap_err().to_string();
    assert!(
        err.contains("amount") && err.contains("precision"),
        "10^38 does not fit Decimal128(38,0) and must error: {err}",
    );
}

#[test]
fn to_decimal128_rejects_the_negative_39_digit_boundary() {
    let s = format!("-1{}", "0".repeat(38));
    let a = arb("x", 100, 0, &[Some(&s)]);
    assert!(
        a.to_decimal128(38, 0, "amount").is_err(),
        "-10^38 must be rejected symmetrically with +10^38",
    );
}

#[test]
fn to_decimal128_accepts_the_negative_38_digit_boundary() {
    let s = format!("-{}", "9".repeat(38));
    let a = arb("x", 100, 0, &[Some(&s)]);
    let out = a.to_decimal128(38, 0, "x").unwrap();
    assert_eq!(out.value(0), i128::from_str(&s).unwrap());
}

#[test]
fn to_decimal128_rejects_values_that_overflow_after_scale_widening() {
    // 10^30 fits Decimal128(38,0) but not Decimal128(38,10).
    let s = format!("1{}", "0".repeat(30));
    let a = arb("x", 100, 0, &[Some(&s)]);
    assert!(a.to_decimal128(38, 0, "x").is_ok());
    assert!(
        a.to_decimal128(38, 10, "amount").is_err(),
        "10^40 does not fit Decimal128(38,10); the scale shift must be checked",
    );
}

#[test]
fn to_decimal256_narrows_exactly_when_the_value_fits() {
    let a = arb("x", 100, 4, &[Some("1.2345"), None]);
    let out = a.to_decimal256(76, 4, "x").unwrap();
    assert_eq!(out.value(0), ArrowI256::from_i128(12345));
    assert!(out.is_null(1));
    assert_eq!(out.data_type(), &DataType::Decimal256(76, 4));
}

#[test]
fn to_decimal256_rejects_precision_zero_and_above_76() {
    let a = arb("x", 10, 0, &[Some("1")]);
    for p in [0u8, 77, 255] {
        let err = a.to_decimal256(p, 0, "amount").unwrap_err().to_string();
        assert!(
            err.contains("1..=76"),
            "precision {p} must be rejected with the legal range: {err}",
        );
    }
}

#[test]
fn to_decimal256_accepts_the_largest_76_digit_value() {
    let biggest = "9".repeat(76);
    let a = arb("x", 100, 0, &[Some(&biggest)]);
    let out = a.to_decimal256(76, 0, "x").unwrap();
    assert_eq!(
        out.value(0).to_string(),
        biggest,
        "10^76 - 1 must fit Decimal256(76, 0)",
    );
}

#[test]
fn to_decimal256_rejects_the_smallest_77_digit_value() {
    let s = format!("1{}", "0".repeat(76));
    let a = arb("x", 100, 0, &[Some(&s)]);
    let err = a.to_decimal256(76, 0, "amount").unwrap_err().to_string();
    assert!(
        err.contains("amount") && err.contains("precision"),
        "10^76 must be rejected: {err}",
    );
}

#[test]
fn to_decimal256_rejects_i256_max_which_needs_77_digits() {
    let s = "57896044618658097711785492504343953926634992332820282019728792003956564819967";
    let a = arb("x", 200, 0, &[Some(s)]);
    let err = a.to_decimal256(76, 0, "amount").unwrap_err().to_string();
    assert!(
        err.contains("precision"),
        "i256::MAX has 77 digits and cannot fit Decimal256(76,0): {err}",
    );
}

#[test]
fn decimal128_round_trip_is_exact_for_every_representable_value() {
    let max38 = i128::from_str(&"9".repeat(38)).unwrap();
    let cases: [i128; 7] = [0, 1, -1, max38, -max38, 10_i128.pow(30), -7];
    let src = Decimal128Array::from(cases.iter().map(|c| Some(*c)).collect::<Vec<_>>())
        .with_precision_and_scale(38, 6)
        .unwrap();
    let a = DecimalArbArray::from_decimal128(&src, 6, 60, 6, "c").unwrap();
    let back = a.to_decimal128(38, 6, "c").unwrap();
    for (i, expected) in cases.iter().enumerate() {
        assert_eq!(
            back.value(i),
            *expected,
            "row {i}: Decimal128 -> decimal_arb -> Decimal128 must be identity",
        );
    }
}

#[test]
fn decimal128_round_trip_rejects_i128_extremes_that_exceed_precision_38() {
    // Arrow lets a Decimal128(38, 0) array hold i128::MAX (1.7 x 10^38), which
    // is *wider* than DECIMAL(38, 0) can represent. Round-tripping it must
    // surface an error, never wrap or truncate.
    let src = Decimal128Array::from(vec![Some(i128::MAX), Some(i128::MIN)])
        .with_precision_and_scale(38, 0)
        .unwrap();
    let a = DecimalArbArray::from_decimal128(&src, 0, 60, 0, "c").unwrap();
    for (i, expected) in [i128::MAX.to_string(), i128::MIN.to_string()]
        .iter()
        .enumerate()
    {
        assert_eq!(
            a.to_string_array().unwrap().value(i),
            *expected,
            "widening must still be exact",
        );
    }
    let err = err_of(a.to_decimal128(38, 0, "amount"));
    assert!(
        err.contains("amount") && err.contains("precision"),
        "narrowing back must reject the out-of-precision value: {err}",
    );
}

#[test]
fn decimal256_round_trip_is_exact_at_the_i256_extremes_when_precision_allows() {
    let cases = [ArrowI256::from_i128(0), ArrowI256::from_i128(-1)];
    let src = Decimal256Array::from(cases.iter().map(|c| Some(*c)).collect::<Vec<_>>())
        .with_precision_and_scale(76, 10)
        .unwrap();
    let a = DecimalArbArray::from_decimal256(&src, 10, 100, 10, "c").unwrap();
    let back = a.to_decimal256(76, 10, "c").unwrap();
    for (i, expected) in cases.iter().enumerate() {
        assert_eq!(back.value(i), *expected, "row {i} must round-trip");
    }
}

#[test]
fn decimal_arb_to_decimal128_udf_produces_the_declared_arrow_type() {
    let f = DecimalArbToDecimal128Func::new();
    let out = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(raw("x", 100, 4, &[Some("1.2345"), None]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            vec![
                Arc::new(DecimalArbType::field("x", 100, 4, true).unwrap()),
                i64_field("p"),
                i64_field("s"),
            ],
            2,
            Arc::new(Field::new("out", DataType::Decimal128(20, 4), true)),
        ))
        .unwrap();
    let out = as_array(out);
    assert_eq!(out.data_type(), &DataType::Decimal128(20, 4));
    let d = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(d.value(0), 12345_i128);
    assert!(d.is_null(1));
}

#[test]
fn decimal_arb_to_decimal128_udf_needs_literal_precision_and_scale() {
    let f = DecimalArbToDecimal128Func::new();
    let err = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(raw("x", 100, 4, &[Some("1.2345")]))),
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(20)]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            vec![
                Arc::new(DecimalArbType::field("x", 100, 4, true).unwrap()),
                i64_field("p"),
                i64_field("s"),
            ],
            1,
            Arc::new(Field::new("out", DataType::Decimal128(20, 4), true)),
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("literal"),
        "must explain the constraint: {err}"
    );
}

#[test]
fn decimal_arb_to_decimal128_udf_rejects_a_non_decimal_arb_input_field() {
    let f = DecimalArbToDecimal128Func::new();
    let err = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(raw("x", 100, 4, &[Some("1.2345")]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            vec![
                Arc::new(Field::new("blob", DataType::LargeBinary, true)),
                i64_field("p"),
                i64_field("s"),
            ],
            1,
            Arc::new(Field::new("out", DataType::Decimal128(20, 4), true)),
        ))
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("blob") && err.contains("decimal_arb"),
        "must name the column and the expected type: {err}",
    );
}

#[test]
fn decimal_arb_to_decimal128_udf_rejects_out_of_range_precision_at_runtime() {
    let f = DecimalArbToDecimal128Func::new();
    let err = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(raw("x", 100, 4, &[Some("1.2345")]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            vec![
                Arc::new(DecimalArbType::field("x", 100, 4, true).unwrap()),
                i64_field("p"),
                i64_field("s"),
            ],
            1,
            Arc::new(Field::new("out", DataType::Decimal128(100, 4), true)),
        ))
        .unwrap_err()
        .to_string();
    assert!(err.contains("1..=38"), "must state the legal range: {err}");
}

#[test]
#[ignore = "FINDING: decimal_arb_to_decimal128(x, 100, 0) plans an out-of-range DataType::Decimal128(100, 0) instead of erroring"]
fn decimal_arb_to_decimal128_udf_rejects_out_of_range_precision_at_plan_time() {
    let f = DecimalArbToDecimal128Func::new();
    let p = ScalarValue::Int64(Some(100));
    let s = ScalarValue::Int64(Some(0));
    let fields: Vec<FieldRef> = vec![
        Arc::new(DecimalArbType::field("x", 100, 4, true).unwrap()),
        i64_field("p"),
        i64_field("s"),
    ];
    let scalars: Vec<Option<&ScalarValue>> = vec![None, Some(&p), Some(&s)];
    let got = f.return_field_from_args(ReturnFieldArgs {
        arg_fields: &fields,
        scalar_arguments: &scalars,
    });
    assert!(
        got.is_err(),
        "planning must reject precision 100 rather than advertise the invalid \
         Arrow type {:?} to every downstream schema consumer",
        got.map(|f| f.data_type().clone()),
    );
}

#[test]
fn decimal_arb_to_decimal256_udf_produces_the_declared_arrow_type() {
    let f = DecimalArbToDecimal256Func::new();
    let out = f
        .invoke_with_args(sfa(
            vec![
                ColumnarValue::Array(Arc::new(raw("x", 100, 4, &[Some("-1.2345")]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(50))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(4))),
            ],
            vec![
                Arc::new(DecimalArbType::field("x", 100, 4, true).unwrap()),
                i64_field("p"),
                i64_field("s"),
            ],
            1,
            Arc::new(Field::new("out", DataType::Decimal256(50, 4), true)),
        ))
        .unwrap();
    let out = as_array(out);
    assert_eq!(out.data_type(), &DataType::Decimal256(50, 4));
    let d = out.as_any().downcast_ref::<Decimal256Array>().unwrap();
    assert_eq!(d.value(0), ArrowI256::from_i128(-12345));
}

#[test]
#[ignore = "FINDING: decimal_arb_to_decimal256(x, 100, 0) plans an out-of-range DataType::Decimal256(100, 0) instead of erroring"]
fn decimal_arb_to_decimal256_udf_rejects_out_of_range_precision_at_plan_time() {
    let f = DecimalArbToDecimal256Func::new();
    let p = ScalarValue::Int64(Some(100));
    let s = ScalarValue::Int64(Some(0));
    let fields: Vec<FieldRef> = vec![
        Arc::new(DecimalArbType::field("x", 100, 4, true).unwrap()),
        i64_field("p"),
        i64_field("s"),
    ];
    let scalars: Vec<Option<&ScalarValue>> = vec![None, Some(&p), Some(&s)];
    let got = f.return_field_from_args(ReturnFieldArgs {
        arg_fields: &fields,
        scalar_arguments: &scalars,
    });
    assert!(
        got.is_err(),
        "planning must reject precision 100 (Decimal256 max is 76), got {:?}",
        got.map(|f| f.data_type().clone()),
    );
}

#[test]
fn to_decimal_arb_from_decimal128_udf_inherits_precision_scale_and_metadata() {
    let f = ToDecimalArbFromDecimal128Func::new();
    let fields: Vec<FieldRef> = vec![Arc::new(Field::new("v", DataType::Decimal128(18, 6), true))];
    let scalars: Vec<Option<&ScalarValue>> = vec![None];
    let out = f
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        })
        .unwrap();
    assert!(DecimalArbType::is_decimal_arb_field(&out));
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out),
        Some((18, 6)),
        "the widening cast must inherit the source (precision, scale)",
    );
}

#[test]
fn to_decimal_arb_from_decimal128_udf_rejects_a_negative_source_scale() {
    let f = ToDecimalArbFromDecimal128Func::new();
    let fields: Vec<FieldRef> = vec![Arc::new(Field::new(
        "v",
        DataType::Decimal128(18, -2),
        true,
    ))];
    let scalars: Vec<Option<&ScalarValue>> = vec![None];
    let err = f
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        })
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("non-negative"),
        "decimal_arb has no negative scale; say so: {err}",
    );
}

#[test]
fn to_decimal_arb_from_decimal128_udf_converts_values_and_nulls() {
    let f = ToDecimalArbFromDecimal128Func::new();
    let src = Decimal128Array::from(vec![Some(12345_i128), None, Some(-1_i128)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let out = f
        .invoke_with_args(sfa(
            vec![ColumnarValue::Array(Arc::new(src))],
            vec![Arc::new(Field::new("v", DataType::Decimal128(18, 4), true))],
            3,
            Arc::new(DecimalArbType::field("out", 18, 4, true).unwrap()),
        ))
        .unwrap();
    assert_eq!(
        decode_arb(&as_array(out), 18, 4),
        vec![Some("1.2345".into()), None, Some("-0.0001".into())],
    );
}

#[test]
fn to_decimal_arb_from_decimal256_udf_inherits_precision_scale_and_metadata() {
    let f = ToDecimalArbFromDecimal256Func::new();
    let fields: Vec<FieldRef> = vec![Arc::new(Field::new(
        "v",
        DataType::Decimal256(70, 12),
        true,
    ))];
    let scalars: Vec<Option<&ScalarValue>> = vec![None];
    let out = f
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        })
        .unwrap();
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out),
        Some((70, 12)),
    );
}

#[test]
fn to_decimal_arb_from_decimal256_udf_converts_values_and_nulls() {
    let f = ToDecimalArbFromDecimal256Func::new();
    let src = Decimal256Array::from(vec![Some(ArrowI256::from_i128(-500)), None])
        .with_precision_and_scale(70, 2)
        .unwrap();
    let out = f
        .invoke_with_args(sfa(
            vec![ColumnarValue::Array(Arc::new(src))],
            vec![Arc::new(Field::new("v", DataType::Decimal256(70, 2), true))],
            2,
            Arc::new(DecimalArbType::field("out", 70, 2, true).unwrap()),
        ))
        .unwrap();
    assert_eq!(
        decode_arb(&as_array(out), 70, 2),
        vec![Some("-5.00".into()), None],
    );
}

// ===========================================================================
// E. integer -> decimal_arb
// ===========================================================================

fn invoke_from_int(
    array: Arc<dyn Array>,
    dt: DataType,
    p: i64,
    s: i64,
) -> datafusion::error::Result<Vec<Option<String>>> {
    let f = ToDecimalArbFromIntFunc::new();
    let n = array.len();
    let out = f.invoke_with_args(sfa(
        vec![
            ColumnarValue::Array(array),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(p))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(s))),
        ],
        vec![
            Arc::new(Field::new("v", dt, true)),
            i64_field("p"),
            i64_field("s"),
        ],
        n,
        Arc::new(DecimalArbType::field("out", p as u32, s as u32, true).unwrap()),
    ))?;
    Ok(decode_arb(&as_array(out), p as u32, s as u32))
}

#[test]
fn from_int_is_exact_at_the_i64_extremes() {
    let a = Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX)]));
    assert_eq!(
        invoke_from_int(a, DataType::Int64, 30, 0).unwrap(),
        vec![Some(i64::MIN.to_string()), Some(i64::MAX.to_string())],
    );
}

#[test]
fn from_int_is_exact_at_the_u64_maximum() {
    let a = Arc::new(UInt64Array::from(vec![Some(u64::MAX)]));
    assert_eq!(
        invoke_from_int(a, DataType::UInt64, 30, 0).unwrap(),
        vec![Some(u64::MAX.to_string())],
        "u64::MAX must not be reinterpreted as a negative i64",
    );
}

#[test]
fn from_int_is_exact_at_the_i8_extremes() {
    let a = Arc::new(Int8Array::from(vec![Some(i8::MIN), Some(i8::MAX)]));
    assert_eq!(
        invoke_from_int(a, DataType::Int8, 10, 0).unwrap(),
        vec![Some("-128".into()), Some("127".into())],
    );
}

#[test]
fn from_int_is_exact_at_the_i16_extremes() {
    let a = Arc::new(Int16Array::from(vec![Some(i16::MIN), Some(i16::MAX)]));
    assert_eq!(
        invoke_from_int(a, DataType::Int16, 10, 0).unwrap(),
        vec![Some("-32768".into()), Some("32767".into())],
    );
}

#[test]
fn from_int_is_exact_at_the_i32_extremes() {
    let a = Arc::new(Int32Array::from(vec![Some(i32::MIN), Some(i32::MAX)]));
    assert_eq!(
        invoke_from_int(a, DataType::Int32, 15, 0).unwrap(),
        vec![Some("-2147483648".into()), Some("2147483647".into())],
    );
}

#[test]
fn from_int_is_exact_at_the_unsigned_extremes() {
    assert_eq!(
        invoke_from_int(
            Arc::new(UInt8Array::from(vec![Some(u8::MAX)])),
            DataType::UInt8,
            10,
            0
        )
        .unwrap(),
        vec![Some("255".into())],
    );
    assert_eq!(
        invoke_from_int(
            Arc::new(UInt16Array::from(vec![Some(u16::MAX)])),
            DataType::UInt16,
            10,
            0
        )
        .unwrap(),
        vec![Some("65535".into())],
    );
    assert_eq!(
        invoke_from_int(
            Arc::new(UInt32Array::from(vec![Some(u32::MAX)])),
            DataType::UInt32,
            15,
            0
        )
        .unwrap(),
        vec![Some("4294967295".into())],
    );
}

#[test]
fn from_int_preserves_nulls() {
    let a = Arc::new(Int64Array::from(vec![Some(1), None, Some(-1)]));
    assert_eq!(
        invoke_from_int(a, DataType::Int64, 20, 0).unwrap(),
        vec![Some("1".into()), None, Some("-1".into())],
    );
}

#[test]
fn from_int_pads_the_declared_scale_without_changing_the_value() {
    let a = Arc::new(Int64Array::from(vec![Some(7), Some(-7)]));
    assert_eq!(
        invoke_from_int(a, DataType::Int64, 20, 3).unwrap(),
        vec![Some("7.000".into()), Some("-7.000".into())],
        "an integer cast into a scaled column must pad, not shift",
    );
}

#[test]
fn from_int_rejects_a_value_wider_than_the_declared_precision() {
    let a = Arc::new(Int64Array::from(vec![Some(i64::MAX)]));
    let err = invoke_from_int(a, DataType::Int64, 5, 0)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("integer digit"),
        "an integer that does not fit must error, never wrap: {err}",
    );
}

#[test]
fn from_int_precision_budget_accounts_for_the_declared_scale() {
    // decimal_arb(20, 18) has only 2 integer digits available.
    let a = Arc::new(Int64Array::from(vec![Some(999)]));
    assert!(
        invoke_from_int(a, DataType::Int64, 20, 18).is_err(),
        "999 needs 3 integer digits; decimal_arb(20,18) allows 2",
    );
}

#[test]
fn from_int_stamps_the_declared_precision_and_scale_on_the_output_field() {
    let f = ToDecimalArbFromIntFunc::new();
    let p = ScalarValue::Int64(Some(25));
    let s = ScalarValue::Int64(Some(5));
    let fields: Vec<FieldRef> = vec![
        Arc::new(Field::new("v", DataType::Int32, true)),
        i64_field("p"),
        i64_field("s"),
    ];
    let scalars: Vec<Option<&ScalarValue>> = vec![None, Some(&p), Some(&s)];
    let out = f
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        })
        .unwrap();
    assert!(DecimalArbType::is_decimal_arb_field(&out));
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out),
        Some((25, 5)),
    );
}

#[test]
fn from_int_rejects_a_utf8_input_array() {
    let a = Arc::new(StringArray::from(vec![Some("1")]));
    let err = invoke_from_int(a, DataType::Utf8, 20, 0)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("unsupported input type"),
        "must name the rejection reason: {err}",
    );
}

// ===========================================================================
// F. float inputs must be rejected, never silently truncated
// ===========================================================================

#[test]
fn from_int_rejects_a_float64_input_array_at_runtime() {
    let a = Arc::new(arrow::array::Float64Array::from(vec![Some(1.5f64)]));
    let err = invoke_from_int(a, DataType::Float64, 20, 2)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Float64"),
        "a float must never be truncated into an integer cast: {err}",
    );
}

#[test]
fn from_int_signature_does_not_accept_float_arguments() {
    let udf = ScalarUDF::from(ToDecimalArbFromIntFunc::new());
    for float in [DataType::Float32, DataType::Float64] {
        let fields: Vec<FieldRef> = vec![
            Arc::new(Field::new("v", float.clone(), true)),
            i64_field("p"),
            i64_field("s"),
        ];
        assert!(
            fields_with_udf(&fields, &udf).is_err(),
            "{float:?} must not coerce into to_decimal_arb_from_int (that would \
             silently truncate the fraction)",
        );
    }
}

#[test]
fn from_decimal128_signature_rejects_floats() {
    let f = ToDecimalArbFromDecimal128Func::new();
    for dt in [DataType::Float32, DataType::Float64] {
        let err = f
            .coerce_types(std::slice::from_ref(&dt))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Decimal128"),
            "{dt:?} must be rejected with the required type named: {err}",
        );
    }
}

#[test]
fn from_decimal256_signature_rejects_floats() {
    let f = ToDecimalArbFromDecimal256Func::new();
    for dt in [DataType::Float32, DataType::Float64] {
        let err = f
            .coerce_types(std::slice::from_ref(&dt))
            .unwrap_err()
            .to_string();
        assert!(err.contains("Decimal256"), "{dt:?} must be rejected: {err}");
    }
}

#[test]
fn from_decimal128_signature_rejects_decimal256_and_utf8() {
    let f = ToDecimalArbFromDecimal128Func::new();
    for dt in [
        DataType::Decimal256(40, 2),
        DataType::Utf8,
        DataType::Int64,
        DataType::LargeBinary,
    ] {
        assert!(
            f.coerce_types(std::slice::from_ref(&dt)).is_err(),
            "{dt:?} must not be silently accepted by the Decimal128 cast",
        );
    }
}

#[test]
fn from_decimal128_signature_rejects_the_wrong_arity() {
    let f = ToDecimalArbFromDecimal128Func::new();
    assert!(f.coerce_types(&[]).is_err());
    assert!(
        f.coerce_types(&[DataType::Decimal128(10, 2), DataType::Int64])
            .is_err(),
    );
}

#[test]
fn a_float_that_reaches_decimal_arb_carries_its_full_binary_expansion() {
    // There is no float -> decimal_arb cast. If a caller forces one through
    // BigDecimal, the *exact* binary value appears — it must not be silently
    // rounded to look like the literal the user typed.
    let x = DecimalArbValue::from_bigdecimal(bigdecimal::BigDecimal::try_from(0.1f64).unwrap());
    assert_ne!(
        x,
        v("0.1"),
        "0.1f64 is not 0.1; a float cast must not pretend otherwise",
    );
    assert!(
        x.fractional_digit_count() > 50,
        "the exact binary expansion has 55 fractional digits, got {}",
        x.fractional_digit_count(),
    );
}

#[test]
fn a_float_expansion_cannot_land_in_a_narrow_scale_column_silently() {
    let x = DecimalArbValue::from_bigdecimal(bigdecimal::BigDecimal::try_from(0.1f64).unwrap());
    let err = x.check_fits(30, 1, "amount").unwrap_err().to_string();
    assert!(
        err.contains("amount") && err.contains("fractional"),
        "a float expansion must be rejected by a scale-1 column, not rounded: {err}",
    );
}

#[test]
fn to_string_of_a_float_expansion_is_the_exact_value() {
    let x = DecimalArbValue::from_bigdecimal(bigdecimal::BigDecimal::try_from(0.5f64).unwrap());
    assert_eq!(
        x.to_canonical_string(),
        "0.5",
        "an exactly-representable float must render exactly",
    );
}

// ===========================================================================
// G. cross-direction consistency
// ===========================================================================

#[test]
fn text_and_decimal128_paths_agree_on_the_same_value() {
    let via_text =
        DecimalArbArray::from_string_array(&StringArray::from(vec![Some("12.3456")]), 30, 4, "c")
            .unwrap();
    let src = Decimal128Array::from(vec![Some(123456_i128)])
        .with_precision_and_scale(30, 4)
        .unwrap();
    let via_dec = DecimalArbArray::from_decimal128(&src, 4, 30, 4, "c").unwrap();
    assert_eq!(
        via_text.value(0).unwrap(),
        via_dec.value(0).unwrap(),
        "the Utf8 and Decimal128 cast paths must produce the same value",
    );
    assert_eq!(
        via_text.into_inner().0.value(0).to_vec(),
        via_dec.into_inner().0.value(0).to_vec(),
        "...and the same bytes",
    );
}

#[test]
fn text_and_integer_paths_agree_on_the_same_value() {
    let via_text =
        DecimalArbArray::from_string_array(&StringArray::from(vec![Some("-42")]), 20, 3, "c")
            .unwrap();
    let via_int = invoke_from_int(
        Arc::new(Int64Array::from(vec![Some(-42_i64)])),
        DataType::Int64,
        20,
        3,
    )
    .unwrap();
    assert_eq!(
        strings_of(&via_text.to_string_array().unwrap()),
        via_int,
        "the Utf8 and integer cast paths must agree",
    );
}

#[test]
fn decimal128_and_decimal256_narrowing_agree_where_both_are_representable() {
    let a = arb("x", 100, 6, &[Some("123.456789"), Some("-0.000001"), None]);
    let d128 = a.to_decimal128(38, 6, "x").unwrap();
    let d256 = a.to_decimal256(76, 6, "x").unwrap();
    for i in 0..a.len() {
        assert_eq!(d128.is_null(i), d256.is_null(i), "row {i} nullness");
        if !d128.is_null(i) {
            assert_eq!(
                ArrowI256::from_i128(d128.value(i)),
                d256.value(i),
                "row {i}: the two narrowing casts must agree",
            );
        }
    }
}

#[test]
fn a_full_cast_cycle_preserves_the_value() {
    // decimal_arb -> Utf8 -> decimal_arb -> Decimal128 -> decimal_arb
    let start = arb("x", 40, 6, &[Some("-98765.432100"), Some("0"), Some("1")]);
    let text = start.to_string_array().unwrap();
    let back = DecimalArbArray::from_string_array(&text, 40, 6, "x").unwrap();
    let d = back.to_decimal128(38, 6, "x").unwrap();
    let again = DecimalArbArray::from_decimal128(&d, 6, 40, 6, "x").unwrap();
    for i in 0..start.len() {
        assert_eq!(
            start.value(i).unwrap(),
            again.value(i).unwrap(),
            "row {i}: a lossless cast cycle must be the identity",
        );
    }
}

#[test]
fn cast_output_field_always_carries_decimal_arb_metadata() {
    // Every widening cast's output field must be recognizable downstream —
    // a bare LargeBinary output is the F2 failure shape.
    let s = ScalarValue::Int64(Some(2));
    let p = ScalarValue::Int64(Some(20));

    let from_string = ToDecimalArbFromStringFunc::new();
    let f1: Vec<FieldRef> = vec![
        Arc::new(Field::new("t", DataType::Utf8, true)),
        i64_field("p"),
        i64_field("s"),
    ];
    let sc1: Vec<Option<&ScalarValue>> = vec![None, Some(&p), Some(&s)];
    let out1 = from_string
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &f1,
            scalar_arguments: &sc1,
        })
        .unwrap();

    let from_int = ToDecimalArbFromIntFunc::new();
    let f2: Vec<FieldRef> = vec![
        Arc::new(Field::new("v", DataType::Int64, true)),
        i64_field("p"),
        i64_field("s"),
    ];
    let sc2: Vec<Option<&ScalarValue>> = vec![None, Some(&p), Some(&s)];
    let out2 = from_int
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &f2,
            scalar_arguments: &sc2,
        })
        .unwrap();

    let from_d128 = ToDecimalArbFromDecimal128Func::new();
    let f3: Vec<FieldRef> = vec![Arc::new(Field::new("v", DataType::Decimal128(20, 2), true))];
    let sc3: Vec<Option<&ScalarValue>> = vec![None];
    let out3 = from_d128
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &f3,
            scalar_arguments: &sc3,
        })
        .unwrap();

    for (name, f) in [
        ("to_decimal_arb_from_string", &out1),
        ("to_decimal_arb_from_int", &out2),
        ("to_decimal_arb_from_decimal128", &out3),
    ] {
        assert!(
            DecimalArbType::is_decimal_arb_field(f),
            "{name} output field lost decimal_arb metadata",
        );
        assert_eq!(
            DecimalArbType::precision_scale_from_field(f),
            Some((20, 2)),
            "{name} output field has the wrong (precision, scale)",
        );
    }
}

#[test]
fn narrowing_cast_output_fields_are_plain_arrow_decimals_without_arb_metadata() {
    let f = DecimalArbToDecimal128Func::new();
    let p = ScalarValue::Int64(Some(20));
    let s = ScalarValue::Int64(Some(2));
    let fields: Vec<FieldRef> = vec![
        Arc::new(DecimalArbType::field("x", 100, 2, true).unwrap()),
        i64_field("p"),
        i64_field("s"),
    ];
    let scalars: Vec<Option<&ScalarValue>> = vec![None, Some(&p), Some(&s)];
    let out = f
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        })
        .unwrap();
    assert_eq!(out.data_type(), &DataType::Decimal128(20, 2));
    assert!(
        !DecimalArbType::is_decimal_arb_field(&out),
        "a Decimal128 result must not claim to be decimal_arb",
    );
}

#[test]
fn narrowing_cast_preserves_source_nullability() {
    let f = DecimalArbToDecimal128Func::new();
    let p = ScalarValue::Int64(Some(20));
    let s = ScalarValue::Int64(Some(2));
    let fields: Vec<FieldRef> = vec![
        Arc::new(DecimalArbType::field("x", 100, 2, false).unwrap()),
        i64_field("p"),
        i64_field("s"),
    ];
    let scalars: Vec<Option<&ScalarValue>> = vec![None, Some(&p), Some(&s)];
    let out = f
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        })
        .unwrap();
    assert!(!out.is_nullable(), "NOT NULL must survive the cast");
}

#[test]
fn casts_on_an_empty_batch_do_not_panic() {
    let empty_text = StringArray::from(Vec::<Option<&str>>::new());
    assert_eq!(
        DecimalArbArray::from_string_array(&empty_text, 20, 2, "c")
            .unwrap()
            .len(),
        0,
    );
    let empty = arb("x", 20, 2, &[]);
    assert_eq!(empty.to_decimal128(20, 2, "x").unwrap().len(), 0);
    assert_eq!(empty.to_decimal256(40, 2, "x").unwrap().len(), 0);
    assert_eq!(empty.to_string_array().unwrap().len(), 0);

    let f = DecimalArbToStringFunc::new();
    let out = f
        .invoke_with_args(sfa(
            vec![ColumnarValue::Array(Arc::new(raw("x", 20, 2, &[])))],
            vec![Arc::new(DecimalArbType::field("x", 20, 2, true).unwrap())],
            0,
            Arc::new(Field::new("out", DataType::Utf8, true)),
        ))
        .unwrap();
    assert_eq!(as_array(out).len(), 0, "empty batch must stay empty");
}

#[test]
fn all_null_column_casts_to_all_null_in_every_direction() {
    let a = arb("x", 20, 2, &[None, None, None]);
    let s = a.to_string_array().unwrap();
    let d1 = a.to_decimal128(20, 2, "x").unwrap();
    let d2 = a.to_decimal256(40, 2, "x").unwrap();
    for i in 0..3 {
        assert!(s.is_null(i) && d1.is_null(i) && d2.is_null(i), "row {i}");
    }
}
