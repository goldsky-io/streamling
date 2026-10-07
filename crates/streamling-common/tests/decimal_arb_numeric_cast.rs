//! `decimal_arb_cast` / `decimal_arb_try_cast`: `CAST` / `TRY_CAST` of a
//! decimal_arb value to an integer, float or `DECIMAL(p ≤ 76, s)` type.
//!
//! The rules mirror arrow-cast for a native `Decimal256` input: integers take
//! the integral part truncated toward zero, decimals round a dropped digit
//! half away from zero, floats take the nearest value (±infinity past the
//! range). An out-of-range value fails `CAST` and is NULL under `TRY_CAST`.
use arrow::array::*;
use arrow::util::display::array_value_to_string;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::{
    logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl},
    scalar::ScalarValue,
};
use std::sync::Arc;
use streamling_common::{
    functions::decimal_arb_ops::DecimalArbCastFunc,
    types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, NativeIntKind},
};

const I64_MAX: &str = "9223372036854775807";
const I64_MAX_PLUS_1: &str = "9223372036854775808";
const I64_MIN: &str = "-9223372036854775808";
const I64_MIN_MINUS_1: &str = "-9223372036854775809";
const U64_MAX: &str = "18446744073709551615";
const U64_MAX_PLUS_1: &str = "18446744073709551616";
/// 2^256 − 1.
const U256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";
/// −2^255.
const I256_MIN: &str =
    "-57896044618658097711785492504343953926634992332820282019728792003956564819968";

fn arb_field(p: u32, s: u32, nullable: bool) -> FieldRef {
    Arc::new(DecimalArbType::field("v", p, s, nullable).unwrap())
}

fn arb_array(values: &[Option<&str>], p: u32, s: u32) -> ArrayRef {
    let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), "v", p, s).unwrap();
    for value in values {
        match value {
            Some(text) => b.append_str(text).unwrap(),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish().into_inner().0)
}

fn func(safe: bool) -> DecimalArbCastFunc {
    if safe {
        DecimalArbCastFunc::try_cast()
    } else {
        DecimalArbCastFunc::cast()
    }
}

/// Plan and invoke the UDF the way the analyzer-rewritten expression does.
fn invoke(
    safe: bool,
    input: ColumnarValue,
    field: FieldRef,
    target: &DataType,
) -> datafusion::error::Result<ColumnarValue> {
    let func = func(safe);
    let template = Arc::new(Field::new("template", target.clone(), true));
    let arg_fields = vec![field, template];
    let return_field = func.return_field_from_args(ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &[None, None],
    })?;
    assert_eq!(return_field.data_type(), target, "returns the cast type");
    let number_rows = match &input {
        ColumnarValue::Array(a) => a.len(),
        ColumnarValue::Scalar(_) => 1,
    };
    func.invoke_with_args(ScalarFunctionArgs {
        args: vec![
            input,
            ColumnarValue::Scalar(ScalarValue::try_new_null(target).unwrap()),
        ],
        arg_fields,
        number_rows,
        return_field,
        config_options: Arc::new(datafusion::config::ConfigOptions::default()),
    })
}

fn rendered(out: ColumnarValue) -> Vec<Option<String>> {
    let array = out.into_array(1).unwrap();
    (0..array.len())
        .map(|i| (!array.is_null(i)).then(|| array_value_to_string(&array, i).unwrap()))
        .collect()
}

/// `values` at `decimal_arb(p, s)` cast to `target`, rendered.
fn cast_at(
    safe: bool,
    values: &[Option<&str>],
    p: u32,
    s: u32,
    target: &DataType,
) -> datafusion::error::Result<Vec<Option<String>>> {
    let out = invoke(
        safe,
        ColumnarValue::Array(arb_array(values, p, s)),
        arb_field(p, s, true),
        target,
    )?;
    Ok(rendered(out))
}

fn cast(values: &[&str], p: u32, s: u32, target: &DataType) -> Vec<String> {
    let values: Vec<Option<&str>> = values.iter().copied().map(Some).collect();
    cast_at(false, &values, p, s, target)
        .unwrap()
        .into_iter()
        .map(Option::unwrap)
        .collect()
}

fn try_cast(values: &[&str], p: u32, s: u32, target: &DataType) -> Vec<Option<String>> {
    let values: Vec<Option<&str>> = values.iter().copied().map(Some).collect();
    cast_at(true, &values, p, s, target).unwrap()
}

fn cast_error(value: &str, p: u32, s: u32, target: &DataType) -> String {
    cast_at(false, &[Some(value)], p, s, target)
        .expect_err("out of range must fail CAST")
        .to_string()
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some(v.to_string())).collect()
}

#[test]
fn integer_cast_takes_the_integral_part_truncated_toward_zero() {
    let values = ["0", "1", "-1", "2.99", "-2.99", "0.5", "-0.5", "41.999999"];
    assert_eq!(
        cast(&values, 40, 6, &DataType::Int64),
        ["0", "1", "-1", "2", "-2", "0", "0", "41"]
    );
    assert_eq!(
        try_cast(&values, 40, 6, &DataType::Int64),
        some(&["0", "1", "-1", "2", "-2", "0", "0", "41"])
    );
}

#[test]
fn int64_bounds() {
    let values = [
        I64_MAX,
        I64_MIN,
        I64_MAX_PLUS_1,
        I64_MIN_MINUS_1,
        U64_MAX,
        U256_MAX,
    ];
    assert_eq!(
        try_cast(&values, 78, 0, &DataType::Int64),
        vec![
            Some(I64_MAX.into()),
            Some(I64_MIN.into()),
            None,
            None,
            None,
            None
        ]
    );
    assert_eq!(
        cast(&[I64_MAX, I64_MIN], 78, 0, &DataType::Int64),
        [I64_MAX, I64_MIN]
    );
    for value in [I64_MAX_PLUS_1, I64_MIN_MINUS_1, U64_MAX, U256_MAX] {
        let err = cast_error(value, 78, 0, &DataType::Int64);
        assert!(
            err.contains(value) && err.contains("Int64") && err.contains("out of range"),
            "{err}"
        );
    }
    // The integral part decides: a fraction does not push i64::MAX over.
    assert_eq!(
        cast(&["9223372036854775807.9"], 40, 1, &DataType::Int64),
        [I64_MAX]
    );
}

#[test]
fn uint64_bounds() {
    assert_eq!(
        try_cast(
            &[U64_MAX, U64_MAX_PLUS_1, "-1", "0", U256_MAX],
            78,
            0,
            &DataType::UInt64
        ),
        vec![Some(U64_MAX.into()), None, None, Some("0".into()), None]
    );
    assert!(cast_error(U64_MAX_PLUS_1, 78, 0, &DataType::UInt64).contains(U64_MAX_PLUS_1));
    assert!(cast_error("-1", 78, 0, &DataType::UInt64).contains("UInt64"));
    // -0.5 truncates to 0, which is in range for an unsigned type.
    assert_eq!(cast(&["-0.5"], 10, 1, &DataType::UInt64), ["0"]);
}

#[test]
fn narrow_integer_types_check_their_own_range() {
    let cases: [(DataType, &str, &str, &str, &str); 6] = [
        (DataType::Int8, "-128", "127", "-129", "128"),
        (DataType::Int16, "-32768", "32767", "-32769", "32768"),
        (
            DataType::Int32,
            "-2147483648",
            "2147483647",
            "-2147483649",
            "2147483648",
        ),
        (DataType::UInt8, "0", "255", "-1", "256"),
        (DataType::UInt16, "0", "65535", "-1", "65536"),
        (DataType::UInt32, "0", "4294967295", "-1", "4294967296"),
    ];
    for (target, min, max, below, above) in cases {
        assert_eq!(cast(&[min, max], 78, 0, &target), [min, max], "{target}");
        assert_eq!(
            try_cast(&[below, above, "1"], 78, 0, &target),
            vec![None, None, Some("1".into())],
            "{target}"
        );
        for value in [below, above] {
            let err = cast_error(value, 78, 0, &target);
            assert!(
                err.contains(value) && err.contains(&target.to_string()),
                "{err}"
            );
        }
    }
}

#[test]
fn float_cast_is_the_nearest_representable_value() {
    let floats = |target: &DataType, values: &[&str], p: u32, s: u32| -> Vec<f64> {
        cast(values, p, s, target)
            .iter()
            .map(|v| v.parse::<f64>().unwrap())
            .collect()
    };
    assert_eq!(
        floats(&DataType::Float64, &["0.1", "-0.1", "0", "2.5"], 10, 1),
        [0.1, -0.1, 0.0, 2.5]
    );
    // A value well beyond i64 and with many fractional digits.
    let wide = "123456789012345678901234567890.123456789012345678";
    assert_eq!(
        floats(&DataType::Float64, &[wide], 60, 18),
        [wide.parse::<f64>().unwrap()]
    );
    assert_eq!(
        floats(&DataType::Float64, &[U256_MAX, I256_MIN], 78, 0),
        [
            U256_MAX.parse::<f64>().unwrap(),
            I256_MIN.parse::<f64>().unwrap()
        ]
    );
    // Float32 rounds once, straight from the decimal.
    let out = cast(&["0.1", "16777217"], 20, 1, &DataType::Float32);
    assert_eq!(
        out.iter()
            .map(|v| v.parse::<f32>().unwrap())
            .collect::<Vec<_>>(),
        [0.1_f32, 16777216.0_f32]
    );
}

#[test]
fn float_cast_past_the_range_is_infinite_not_an_error() {
    // As arrow does for Decimal256 -> Float32; Float64 overflows only for a
    // decimal_arb wider than any native decimal.
    let big = format!("1{}", "0".repeat(400));
    let out = cast(&[&big, &format!("-{big}")], 401, 0, &DataType::Float64);
    assert_eq!(out, ["inf", "-inf"]);
    assert_eq!(cast(&[U256_MAX], 78, 0, &DataType::Float32), ["inf"]);
    assert_eq!(
        try_cast(&[U256_MAX], 78, 0, &DataType::Float32),
        some(&["inf"])
    );
}

#[test]
fn decimal_cast_rounds_half_away_from_zero() {
    let target = DataType::Decimal128(10, 2);
    assert_eq!(
        cast(
            &["1.245", "-1.245", "1.255", "0.005", "-0.005", "1.244", "7"],
            20,
            3,
            &target
        ),
        ["1.25", "-1.25", "1.26", "0.01", "-0.01", "1.24", "7.00"]
    );
    // Rescale up is exact.
    assert_eq!(
        cast(&["1.5", "-0.1"], 20, 1, &DataType::Decimal128(10, 4)),
        ["1.5000", "-0.1000"]
    );
    // To scale 0, the same rule (an integer cast truncates instead).
    assert_eq!(
        cast(&["2.5", "-2.5", "2.4"], 20, 1, &DataType::Decimal128(5, 0)),
        ["3", "-3", "2"]
    );
}

#[test]
fn decimal_cast_checks_the_target_precision() {
    let target = DataType::Decimal128(5, 2);
    assert_eq!(
        try_cast(&["999.99", "1000", "-999.994", "999.995"], 20, 3, &target),
        // 999.995 rounds to 1000.00, which no longer fits.
        vec![Some("999.99".into()), None, Some("-999.99".into()), None]
    );
    let err = cast_error("1000", 20, 3, &target);
    assert!(
        err.contains("1000") && err.contains("Decimal128(5, 2)"),
        "{err}"
    );

    // Decimal128(38, 0) holds 38 digits; u256 max has 78.
    assert!(try_cast(&[U256_MAX], 78, 0, &DataType::Decimal128(38, 0))[0].is_none());
    assert!(cast_error(U256_MAX, 78, 0, &DataType::Decimal128(38, 0)).contains(U256_MAX));
}

#[test]
fn decimal256_targets_keep_wide_values() {
    let ten_45 = format!("1{}", "0".repeat(45));
    assert_eq!(
        cast(&[&ten_45, "-12.34567"], 80, 5, &DataType::Decimal256(50, 4)),
        [format!("{ten_45}.0000"), "-12.3457".to_string()]
    );
    assert_eq!(
        cast(
            &[I64_MAX_PLUS_1, U64_MAX_PLUS_1],
            78,
            0,
            &DataType::Decimal256(76, 0)
        ),
        [I64_MAX_PLUS_1, U64_MAX_PLUS_1]
    );
    // 77 digits do not fit Decimal256(76, 0).
    assert_eq!(
        try_cast(&[U256_MAX], 78, 0, &DataType::Decimal256(76, 0)),
        vec![None]
    );
    assert!(cast_error(U256_MAX, 78, 0, &DataType::Decimal256(76, 0)).contains("Decimal256"));
}

#[test]
fn nulls_pass_through_and_sliced_arrays_read_their_own_window() {
    let values = [Some("1"), None, Some("-2.5"), Some(U256_MAX), None];
    assert_eq!(
        cast_at(true, &values, 79, 1, &DataType::Int64).unwrap(),
        vec![Some("1".into()), None, Some("-2".into()), None, None]
    );
    assert_eq!(
        cast_at(false, &values[..3], 79, 1, &DataType::Decimal128(10, 1)).unwrap(),
        vec![Some("1.0".into()), None, Some("-2.5".into())]
    );

    let array = arb_array(&values, 79, 1).slice(1, 3);
    let out = invoke(
        true,
        ColumnarValue::Array(array),
        arb_field(79, 1, true),
        &DataType::Int64,
    )
    .unwrap();
    assert_eq!(rendered(out), vec![None, Some("-2".into()), None]);
}

#[test]
fn scalar_input_yields_a_scalar() {
    let one_row = arb_array(&[Some("-7.9")], 20, 1);
    let scalar = ScalarValue::try_from_array(&one_row, 0).unwrap();
    for (safe, target, expected) in [
        (false, DataType::Int64, ScalarValue::Int64(Some(-7))),
        (true, DataType::Int32, ScalarValue::Int32(Some(-7))),
        (
            false,
            DataType::Decimal128(4, 0),
            ScalarValue::Decimal128(Some(-8), 4, 0),
        ),
    ] {
        match invoke(
            safe,
            ColumnarValue::Scalar(scalar.clone()),
            arb_field(20, 1, true),
            &target,
        )
        .unwrap()
        {
            ColumnarValue::Scalar(out) => assert_eq!(out, expected),
            other => panic!("expected a scalar, got {other:?}"),
        }
    }
    let null = ScalarValue::LargeBinary(None);
    match invoke(
        false,
        ColumnarValue::Scalar(null),
        arb_field(20, 1, true),
        &DataType::Int64,
    )
    .unwrap()
    {
        ColumnarValue::Scalar(out) => assert_eq!(out, ScalarValue::Int64(None)),
        other => panic!("expected a scalar, got {other:?}"),
    }
}

#[test]
fn signed_hinted_negatives_cast_by_value() {
    let field = Arc::new(
        DecimalArbType::with_native_int_kind(
            DecimalArbType::field("v", 78, 0, true).unwrap(),
            NativeIntKind::I256,
        )
        .unwrap(),
    );
    let array = arb_array(&[Some("-5"), Some(I256_MIN), Some(I64_MIN)], 78, 0);
    let out = invoke(true, ColumnarValue::Array(array), field, &DataType::Int64).unwrap();
    assert_eq!(
        rendered(out),
        vec![Some("-5".into()), None, Some(I64_MIN.into())]
    );
}

#[test]
fn result_nullability_follows_cast_and_try_cast() {
    let return_field = |safe: bool, nullable: bool, target: DataType| {
        let arg_fields = vec![
            arb_field(78, 0, nullable),
            Arc::new(Field::new("template", target, true)),
        ];
        func(safe).return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        })
    };
    assert!(
        !return_field(false, false, DataType::Int64)
            .unwrap()
            .is_nullable()
    );
    assert!(
        return_field(false, true, DataType::Int64)
            .unwrap()
            .is_nullable()
    );
    assert!(
        return_field(true, false, DataType::Int64)
            .unwrap()
            .is_nullable()
    );
    // Not a numeric target: refused rather than silently relabelled.
    assert!(return_field(false, true, DataType::Utf8).is_err());
    // Not a decimal_arb input.
    let arg_fields: Vec<FieldRef> = vec![
        Arc::new(Field::new("v", DataType::LargeBinary, true)),
        Arc::new(Field::new("template", DataType::Int64, true)),
    ];
    assert!(
        func(false)
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &[None, None],
            })
            .is_err()
    );
}

/// Every value a `Decimal256(76, 3)` can hold casts exactly as arrow-cast
/// casts the native decimal: same values, same NULLs under `TRY_CAST`, and an
/// error under `CAST` exactly where arrow errors.
///
/// One known exception: arrow narrows `i256` to a signed integer through a
/// range check that misses the bits between 64 and 128, so an integral part
/// of 2^64 or more whose low 64 bits fit comes out wrapped (2^64 → 0). The
/// decimal_arb cast reports it out of range instead.
#[test]
fn matches_arrow_cast_for_values_a_native_decimal_holds() {
    use arrow::compute::{CastOptions, cast_with_options};
    let values = [
        "0",
        "1",
        "-1",
        "0.499",
        "0.5",
        "-0.5",
        "2.675",
        "-2.675",
        "127.999",
        "128",
        "-128.999",
        "255.5",
        "65535.999",
        "99999999.995",
        "2147483648.001",
        "9223372036854775807.999",
        "9223372036854775808",
        "-9223372036854775808.5",
        "18446744073709551615.4",
        "18446744073709551616",
        "123456789012345678901234567890123456789.123",
        "-9999999999999999999999999999999999999999999999999999999999999999999999999.999",
    ];
    let targets = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Decimal128(10, 2),
        DataType::Decimal128(38, 0),
        DataType::Decimal128(12, 5),
        DataType::Decimal256(50, 1),
        DataType::Decimal256(76, 3),
    ];
    let native = |value: &str| -> ArrayRef {
        let array: ArrayRef = Arc::new(StringArray::from(vec![value]));
        cast_with_options(
            &array,
            &DataType::Decimal256(76, 3),
            &CastOptions::default(),
        )
        .unwrap()
    };
    for target in &targets {
        for value in values {
            let safe = CastOptions {
                safe: true,
                ..Default::default()
            };
            let unsafe_ = CastOptions {
                safe: false,
                ..Default::default()
            };
            let arrow_try = cast_with_options(&native(value), target, &safe).unwrap();
            let arrow_try =
                (!arrow_try.is_null(0)).then(|| array_value_to_string(&arrow_try, 0).unwrap());
            let ours_try = cast_at(true, &[Some(value)], 76, 3, target).unwrap();
            let ours_cast = cast_at(false, &[Some(value)], 76, 3, target)
                .ok()
                .map(|v| v[0].clone().unwrap());
            if ours_try != vec![arrow_try.clone()] && arrow_wraps(value, target) {
                assert_eq!(ours_try, vec![None], "TRY_CAST({value} AS {target})");
                assert_eq!(ours_cast, None, "CAST({value} AS {target})");
                continue;
            }
            assert_eq!(ours_try, vec![arrow_try], "TRY_CAST({value} AS {target})");
            let arrow_cast = cast_with_options(&native(value), target, &unsafe_)
                .ok()
                .map(|a| array_value_to_string(&a, 0).unwrap());
            assert_eq!(ours_cast, arrow_cast, "CAST({value} AS {target})");
        }
    }
    // Floats: arrow divides two f64s, so it may be an ulp off the nearest value.
    for value in values {
        let arrow =
            cast_with_options(&native(value), &DataType::Float64, &Default::default()).unwrap();
        let arrow = array_value_to_string(&arrow, 0)
            .unwrap()
            .parse::<f64>()
            .unwrap();
        let ours = cast(&[value], 76, 3, &DataType::Float64)[0]
            .parse::<f64>()
            .unwrap();
        assert!(
            (ours - arrow).abs() <= arrow.abs() * f64::EPSILON,
            "{value}: {ours} vs {arrow}"
        );
    }
}

/// The arrow defect described above: a signed integer target and an integral
/// part of at least 2^64.
fn arrow_wraps(value: &str, target: &DataType) -> bool {
    let signed = matches!(
        target,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    );
    let integral = value.split('.').next().unwrap().trim_start_matches('-');
    signed && integral.parse::<num_bigint::BigInt>().unwrap() > num_bigint::BigInt::from(u64::MAX)
}
