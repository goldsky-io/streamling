//! Fresh v3 numerical review: constructor contracts and exact mixed-type paths.
//! These assertions describe required numeric behavior; failing cases are kept
//! as regressions for the agent implementing fixes.
use arrow::array::*;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use bigdecimal::BigDecimal;
use datafusion::{
    execution::FunctionRegistry,
    logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl},
    prelude::SessionContext,
    scalar::ScalarValue,
};
use num_bigint::BigInt;
use num_traits::Zero;
use std::sync::Arc;
use streamling_common::{
    functions::{CommonFunctions, decimal_arb_coercion::DecimalArbExprPlanner, decimal_arb_ops::*},
    types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue},
};

fn session() -> SessionContext {
    let mut ctx = SessionContext::new();
    for udf in CommonFunctions::functions() {
        ctx.register_udf(udf);
    }
    ctx.register_expr_planner(Arc::new(DecimalArbExprPlanner::new()))
        .unwrap();
    ctx
}

fn field(name: &str, p: u32, s: u32) -> FieldRef {
    Arc::new(DecimalArbType::field(name, p, s, true).unwrap())
}

fn decimal_array(values: &[Option<BigInt>], p: u32, s: u32) -> ArrayRef {
    let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), "v", p, s).unwrap();
    for value in values {
        match value {
            Some(n) => b
                .append_value(&DecimalArbValue::from_bigint_and_scale(n.clone(), s.into()))
                .unwrap(),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish().into_inner().0)
}

fn decoded(array: &dyn Array, field: &Field) -> Vec<Option<BigDecimal>> {
    let (_, s) = DecimalArbType::precision_scale_from_field(field).unwrap();
    let a = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    (0..a.len())
        .map(|i| {
            (!a.is_null(i)).then(|| {
                DecimalArbValue::from_canonical_bytes_at_scale(a.value(i), s)
                    .unwrap()
                    .as_bigdecimal()
                    .clone()
            })
        })
        .collect()
}

fn constructor_args(integer: bool, p: i64, s: i64) -> (Vec<ColumnarValue>, Vec<FieldRef>) {
    let value = if integer {
        ScalarValue::Int64(Some(123))
    } else {
        ScalarValue::Utf8(Some("123".to_owned()))
    };
    (
        vec![
            ColumnarValue::Scalar(value.clone()),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(p))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(s))),
        ],
        vec![
            Arc::new(Field::new("v", value.data_type(), true)),
            Arc::new(Field::new("p", DataType::Int64, false)),
            Arc::new(Field::new("s", DataType::Int64, false)),
        ],
    )
}

#[test]
fn constructor_planning_rejects_precision_and_scale_that_wrap_u32() {
    let mut accepted = Vec::new();
    for integer in [false, true] {
        let udf: Box<dyn ScalarUDFImpl> = if integer {
            Box::new(ToDecimalArbFromIntFunc::new())
        } else {
            Box::new(ToDecimalArbFromStringFunc::new())
        };
        for (p, s) in [
            (65_536, 0),
            ((1_i64 << 32) + 100, 0),
            (100, 1_i64 << 32),
            ((1_i64 << 40) + 100, (1_i64 << 40) + 2),
        ] {
            let (args, fields) = constructor_args(integer, p, s);
            let scalars = args
                .iter()
                .map(|a| match a {
                    ColumnarValue::Scalar(s) => Some(s),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if let Ok(out) = udf.return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &scalars,
            }) {
                accepted.push(format!(
                    "{}({p},{s}) became {:?}",
                    udf.name(),
                    DecimalArbType::precision_scale_from_field(&out)
                ));
            }
        }
    }
    assert!(
        accepted.is_empty(),
        "invalid declarations accepted: {accepted:?}"
    );
}

#[test]
fn constructor_runtime_rejects_precision_and_scale_that_wrap_u32() {
    let mut accepted = Vec::new();
    for integer in [false, true] {
        let udf: Box<dyn ScalarUDFImpl> = if integer {
            Box::new(ToDecimalArbFromIntFunc::new())
        } else {
            Box::new(ToDecimalArbFromStringFunc::new())
        };
        for (p, s) in [
            (65_536, 0),
            ((1_i64 << 32) + 100, 0),
            (100, 1_i64 << 32),
            ((1_i64 << 40) + 100, (1_i64 << 40) + 2),
        ] {
            let (args, arg_fields) = constructor_args(integer, p, s);
            if let Ok(out) = udf.invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 1,
                return_field: field("result", 100, (s as u32).min(2)),
                config_options: Arc::default(),
            }) {
                accepted.push(format!("{}({p},{s}) returned {out:?}", udf.name()));
            }
        }
    }
    assert!(
        accepted.is_empty(),
        "invalid declarations accepted: {accepted:?}"
    );
}

#[tokio::test]
async fn public_sql_constructor_rejects_wrapped_declaration() {
    let ctx = session();
    let mut accepted = Vec::new();
    for sql in [
        "SELECT to_decimal_arb_from_string('123.45',4294967396,4294967298) AS x",
        "SELECT to_decimal_arb_from_int(123,4294967396,4294967296) AS x",
    ] {
        let result = match ctx.sql(sql).await {
            Ok(df) => df.collect().await,
            Err(e) => Err(e),
        };
        if let Ok(batches) = result {
            for b in batches {
                accepted.push(format!(
                    "{sql}: {:?}, values {:?}",
                    DecimalArbType::precision_scale_from_field(b.schema().field(0)),
                    decoded(b.column(0).as_ref(), b.schema().field(0)),
                ));
            }
        }
    }
    assert!(
        accepted.is_empty(),
        "invalid SQL type accepted: {accepted:?}"
    );
}

#[tokio::test]
async fn mixed_integer_native_decimal_expression_chains_are_exact() {
    let mut checked = 0;
    for (sa, sb) in [(0_u32, 0_i8), (2, 4), (18, 0), (43, 18), (120, 27)] {
        let ns: Vec<Option<BigInt>> = (0..40)
            .map(|i| {
                if i % 11 == 0 {
                    None
                } else {
                    Some(
                        BigInt::from(if i % 2 == 0 { 1 } else { -1 })
                            * (BigInt::from(10).pow(125) + BigInt::from(i * 7919 + 1)),
                    )
                }
            })
            .collect();
        let bs: Vec<Option<i128>> = (0..40)
            .map(|i| {
                (i % 13 != 0).then_some((i as i128 * 99991 + 7) * if i % 3 == 0 { -1 } else { 1 })
            })
            .collect();
        let ints: Vec<i64> = (0..40)
            .map(|i| match i % 4 {
                0 => i64::MIN,
                1 => i64::MAX,
                2 => -1,
                _ => 1,
            })
            .collect();
        let native = Decimal128Array::from(bs.clone())
            .with_precision_and_scale(28, sb)
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            field("a", 300, sa),
            Arc::new(Field::new("b", native.data_type().clone(), true)),
            Arc::new(Field::new("i", DataType::Int64, false)),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                decimal_array(&ns, 300, sa),
                Arc::new(native),
                Arc::new(Int64Array::from(ints.clone())),
            ],
        )
        .unwrap();
        let ctx = session();
        ctx.register_batch("t", batch).unwrap();
        for sql in [
            "SELECT (a+b)-b AS x, (a+i)-i AS y, (a*b)-(b*a) AS z FROM t",
            "SELECT (b+a)-b AS x, (i+a)-i AS y, (b*a)-(a*b) AS z FROM t",
        ] {
            let out = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let mut row = 0;
            for b in out {
                let values: Vec<_> = (0..3)
                    .map(|c| decoded(b.column(c).as_ref(), b.schema().field(c)))
                    .collect();
                for j in 0..b.num_rows() {
                    let a = ns[row]
                        .as_ref()
                        .map(|n| BigDecimal::new(n.clone(), sa as i64));
                    assert_eq!(
                        values[0][j],
                        if bs[row].is_some() { a.clone() } else { None },
                        "{sql}, scales ({sa},{sb}), row {row}"
                    );
                    assert_eq!(
                        values[1][j],
                        a.clone(),
                        "{sql}, scales ({sa},{sb}), row {row}"
                    );
                    assert_eq!(
                        values[2][j],
                        if a.is_some() && bs[row].is_some() {
                            Some(BigDecimal::zero())
                        } else {
                            None
                        }
                    );
                    checked += 3;
                    row += 1;
                }
            }
            assert_eq!(row, ns.len());
        }
    }
    eprintln!("{checked} mixed native decimal / extreme i64 expression-chain values passed");
}

#[tokio::test]
async fn unsigned_integer_extremes_and_scalar_broadcast_are_exact() {
    let ctx = session();
    let ints = [0_u64, 1, (1_u64 << 63) - 1, 1_u64 << 63, u64::MAX];
    let schema = Arc::new(Schema::new(vec![
        field("a", 100, 7),
        Arc::new(Field::new("u", DataType::UInt64, false)),
    ]));
    ctx.register_batch(
        "t",
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                decimal_array(&vec![Some(BigInt::from(125)); ints.len()], 100, 7),
                Arc::new(UInt64Array::from(ints.to_vec())),
            ],
        )
        .unwrap(),
    )
    .unwrap();
    let out = ctx
        .sql("SELECT (a+u)-u AS x, (u+a)-u AS y, a+1 AS z, 1+a AS w FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for batch in out {
        for c in 0..4 {
            let expected = BigDecimal::new(BigInt::from(if c < 2 { 125 } else { 10_000_125 }), 7);
            assert!(
                decoded(batch.column(c).as_ref(), batch.schema().field(c))
                    .into_iter()
                    .all(|v| v == Some(expected.clone()))
            );
        }
    }
}

#[test]
fn native_casts_cover_extreme_negative_scales_and_signed_ties() {
    // Hard-coded integer coefficients: unlike the implementation this oracle
    // does not call BigDecimal::with_scale_round or decimal division.
    for (input, target_scale, expected) in [
        ("1.25", 1_i8, 12_i128),
        ("1.35", 1, 14),
        ("-1.25", 1, -12),
        ("-1.35", 1, -14),
        ("1250", -2, 12),
        ("1350", -2, 14),
        ("-1250", -2, -12),
        ("-1350", -2, -14),
        ("5e127", -128, 0),
        ("15e127", -128, 2),
        ("-5e127", -128, 0),
        ("-15e127", -128, -2),
    ] {
        let mut b = DecimalArbArrayBuilder::with_capacity(3, "v", 300, 2).unwrap();
        b.append_null();
        b.append_str(input).unwrap();
        b.append_null();
        let a = b.finish();
        for native in [128, 256] {
            let actual = if native == 128 {
                let out = a.to_decimal128(38, target_scale, "v").unwrap();
                assert!(out.is_null(0) && out.is_null(2));
                BigInt::from(out.value(1))
            } else {
                let out = a.to_decimal256(76, target_scale, "v").unwrap();
                assert!(out.is_null(0) && out.is_null(2));
                BigInt::from_signed_bytes_be(&out.value(1).to_be_bytes())
            };
            assert_eq!(
                actual,
                BigInt::from(expected),
                "{input} to Decimal{native} scale {target_scale}"
            );
        }
    }
}

#[tokio::test]
async fn float_mixing_and_negative_native_scale_fail_explicitly() {
    let ctx = session();
    let native = Decimal128Array::from(vec![125_i128])
        .with_precision_and_scale(10, -2)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        field("a", 100, 0),
        Arc::new(Field::new("n", native.data_type().clone(), false)),
        Arc::new(Field::new("f", DataType::Float64, false)),
    ]));
    ctx.register_batch(
        "t",
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                decimal_array(&[Some(BigInt::from(1))], 100, 0),
                Arc::new(native),
                Arc::new(Float64Array::from(vec![0.1])),
            ],
        )
        .unwrap(),
    )
    .unwrap();
    for sql in [
        "SELECT a+f FROM t",
        "SELECT f+a FROM t",
        "SELECT a+n FROM t",
        "SELECT to_decimal_arb_from_decimal128(n) FROM t",
    ] {
        let result = match ctx.sql(sql).await {
            Ok(df) => df.collect().await,
            Err(e) => Err(e),
        };
        assert!(
            result.is_err(),
            "unsupported lossy conversion must reject: {sql}"
        );
    }
}

#[test]
fn decimal_text_parser_checks_value_not_lexical_digit_count() {
    let accepted = [
        ("000000000000000000000000000000001.230000", 3, 2, "1.23"),
        ("+00123e-2", 3, 2, "1.23"),
        ("123000e-5", 3, 2, "1.23"),
        (".0010000000000000000000", 3, 3, "0.001"),
        ("-0e-999", 1, 0, "0"),
        ("1e999", 1000, 0, "1e999"),
        ("-1e-999", 999, 999, "-1e-999"),
    ];
    for (input, p, s, expected) in accepted {
        let mut b = DecimalArbArrayBuilder::with_capacity(1, "v", p, s).unwrap();
        b.append_str(input).unwrap();
        let value = b.finish().value(0).unwrap().unwrap();
        assert_eq!(value, expected.parse::<DecimalArbValue>().unwrap());
    }
    for (input, p, s) in [
        ("1e999", 999, 0),
        ("1e-1000", 999, 999),
        ("0.0001", 3, 3),
        ("1", 3, 3),
        ("9.999", 3, 2),
    ] {
        let mut b = DecimalArbArrayBuilder::with_capacity(1, "v", p, s).unwrap();
        assert!(
            b.append_str(input).is_err(),
            "silently accepted {input} at ({p},{s})"
        );
    }
}

fn binary_values(
    udf: &dyn ScalarUDFImpl,
    left: &[Option<BigInt>],
    pl: u32,
    sl: u32,
    right: &[Option<BigInt>],
    pr: u32,
    sr: u32,
    scalar_right: bool,
) -> datafusion::common::Result<Vec<Option<BigDecimal>>> {
    let fields = vec![field("l", pl, sl), field("r", pr, sr)];
    let result_field = udf.return_field_from_args(ReturnFieldArgs {
        arg_fields: &fields,
        scalar_arguments: &[None, None],
    })?;
    let l = decimal_array(left, pl, sl);
    let r = decimal_array(right, pr, sr);
    let r = if scalar_right {
        ColumnarValue::Scalar(ScalarValue::try_from_array(&r, 0)?)
    } else {
        ColumnarValue::Array(r)
    };
    let output = udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(l), r],
        arg_fields: fields,
        number_rows: left.len(),
        return_field: result_field.clone(),
        config_options: Arc::default(),
    })?;
    let array = output.into_array(left.len())?;
    Ok(decoded(array.as_ref(), &result_field))
}

#[test]
fn all_binary_ops_preserve_empty_null_and_scalar_batch_shapes() {
    let ops: Vec<Box<dyn ScalarUDFImpl>> = vec![
        Box::new(DecimalArbAddFunc::new()),
        Box::new(DecimalArbSubFunc::new()),
        Box::new(DecimalArbMulFunc::new()),
        Box::new(DecimalArbDivFunc::new()),
        Box::new(DecimalArbModFunc::new()),
    ];
    for op in ops {
        for scalar_right in [false, true] {
            for r in [None, Some(BigInt::zero()), Some(BigInt::from(23))] {
                let out = binary_values(
                    op.as_ref(),
                    &[],
                    200,
                    30,
                    std::slice::from_ref(&r),
                    100,
                    2,
                    scalar_right,
                )
                .unwrap();
                assert!(
                    out.is_empty(),
                    "{} manufactured rows in an empty batch",
                    op.name()
                );
                let out = binary_values(
                    op.as_ref(),
                    &[None, None, None],
                    200,
                    30,
                    &[r],
                    100,
                    2,
                    scalar_right,
                )
                .unwrap();
                assert_eq!(
                    out,
                    vec![None, None, None],
                    "{} changed NULL values",
                    op.name()
                );
            }
        }
        // Split non-null / null rows must preserve identical arithmetic to
        // processing the non-null row by itself.
        let a = Some(BigInt::from(127));
        let b = Some(BigInt::from(-7));
        let one = binary_values(
            op.as_ref(),
            std::slice::from_ref(&a),
            200,
            30,
            std::slice::from_ref(&b),
            100,
            2,
            false,
        )
        .unwrap();
        let many = binary_values(
            op.as_ref(),
            &[a.clone(), None, a],
            200,
            30,
            &[b],
            100,
            2,
            true,
        )
        .unwrap();
        assert_eq!(
            many,
            vec![one[0].clone(), None, one[0].clone()],
            "{} scalar broadcast changes arithmetic",
            op.name()
        );
    }
}

#[test]
fn remainder_sign_and_extreme_scale_differences_follow_integer_oracle() {
    let mut checked = 0;
    for (sl, sr) in [
        (0, 0),
        (0, 18),
        (18, 0),
        (0, 1024),
        (1024, 0),
        (1024, 2048),
        (0, 32768),
        (32768, 0),
    ] {
        for sign_l in [-1_i64, 1] {
            for sign_r in [-1_i64, 1] {
                let left = BigInt::from(sign_l) * (BigInt::from(10).pow(100) + 12345_u32);
                let right = BigInt::from(sign_r * 991);
                let scale = sl.max(sr);
                let expected = (&left * BigInt::from(10).pow(scale - sl))
                    % (&right * BigInt::from(10).pow(scale - sr));
                let result = binary_values(
                    &DecimalArbModFunc::new(),
                    &[Some(left)],
                    sl + 110,
                    sl,
                    &[Some(right)],
                    sr + 10,
                    sr,
                    false,
                )
                .unwrap();
                assert_eq!(
                    result,
                    vec![Some(BigDecimal::new(expected, scale as i64))],
                    "modulo scales ({sl},{sr}), signs ({sign_l},{sign_r})"
                );
                checked += 1;
            }
        }
    }
    eprintln!(
        "{checked} remainder cases passed scaled-BigInt oracle, including 32,768-place scale differences"
    );
}

#[test]
fn precision_cap_accepts_exact_products_and_rejects_loss_of_information() {
    // Both fields (32768,32768) are legal. Their output scale is capped
    // from 65536 to 65535; only values exactly representable there may pass.
    for (l, r, expected) in [
        (
            BigInt::from(10),
            BigInt::from(1),
            Some(BigDecimal::new(BigInt::from(1), 65535)),
        ),
        (
            BigInt::from(-10),
            BigInt::from(1),
            Some(BigDecimal::new(BigInt::from(-1), 65535)),
        ),
        (BigInt::from(1), BigInt::from(1), None),
        (BigInt::from(5), BigInt::from(1), None),
        (BigInt::from(-5), BigInt::from(1), None),
        (BigInt::from(15), BigInt::from(1), None),
        (BigInt::zero(), BigInt::from(1), Some(BigDecimal::zero())),
    ] {
        let result = binary_values(
            &DecimalArbMulFunc::new(),
            &[Some(l.clone())],
            32768,
            32768,
            &[Some(r.clone())],
            32768,
            32768,
            false,
        );
        match expected {
            Some(v) => assert_eq!(
                result.unwrap(),
                vec![Some(v)],
                "exact capped product {l} * {r}"
            ),
            None => assert!(
                result.is_err(),
                "inexact capped product {l} * {r} silently rounded: {result:?}"
            ),
        }
    }
}

#[test]
fn native_precision_limit_rounding_carry_is_checked_after_rounding() {
    for (native, p) in [(128, 38_u32), (256, 76_u32)] {
        let limit = BigInt::from(10).pow(p);
        // At target scale zero, p digits followed by .5 would round to p+1
        // digits. .4 stays representable. Both signs must behave equally.
        for sign in [-1_i32, 1] {
            for (fraction, should_fit) in [(4_u32, true), (5, false), (6, false)] {
                let n = BigInt::from(sign) * ((&limit - 1_u32) * 10_u32 + fraction);
                let mut b = DecimalArbArrayBuilder::with_capacity(1, "v", 100, 1).unwrap();
                b.append_value(&DecimalArbValue::from_bigint_and_scale(n, 1))
                    .unwrap();
                let a = b.finish();
                let result = if native == 128 {
                    a.to_decimal128(p as u8, 0, "v")
                        .map(|v| BigInt::from(v.value(0)))
                } else {
                    a.to_decimal256(p as u8, 0, "v")
                        .map(|v| BigInt::from_signed_bytes_be(&v.value(0).to_be_bytes()))
                };
                if should_fit {
                    assert_eq!(result.unwrap(), BigInt::from(sign) * (&limit - 1_u32));
                } else {
                    assert!(
                        result.is_err(),
                        "rounded carry overflow silently accepted at Decimal{native}, sign {sign}, fraction {fraction}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn numeric_scalar_functions_match_native_values_or_reject_explicitly() {
    let ctx = session();
    let native = Decimal128Array::from(vec![Some(225_i128), Some(-225), Some(400), None])
        .with_precision_and_scale(20, 2)
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        field("a", 100, 2),
        Arc::new(Field::new("n", native.data_type().clone(), true)),
    ]));
    ctx.register_batch(
        "t",
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                decimal_array(
                    &[
                        Some(BigInt::from(225)),
                        Some(BigInt::from(-225)),
                        Some(BigInt::from(400)),
                        None,
                    ],
                    100,
                    2,
                ),
                Arc::new(native),
            ],
        )
        .unwrap(),
    )
    .unwrap();
    let mut rejected = Vec::new();
    let mut checked = Vec::new();
    let mut native_rejected = Vec::new();
    for expression in [
        "abs({v})",
        "signum({v})",
        "sign({v})",
        "floor({v})",
        "ceil({v})",
        "ceiling({v})",
        "round({v})",
        "round({v},1)",
        "trunc({v})",
        "trunc({v},1)",
        "power({v},2)",
        "pow({v},2)",
        "sqrt(abs({v}))",
        "exp({v})",
        "ln(abs({v}))",
        "log10(abs({v}))",
        "radians({v})",
        "degrees({v})",
        "sin({v})",
        "cos({v})",
    ] {
        let run =
            |column: &str| format!("SELECT {} AS x FROM t", expression.replace("{v}", column));
        let native = match ctx.sql(&run("n")).await {
            Ok(df) => df.collect().await,
            Err(e) => Err(e),
        };
        let Ok(native) = native else {
            native_rejected.push(expression);
            continue;
        };
        let result = match ctx.sql(&run("a")).await {
            Ok(df) => df.collect().await,
            Err(e) => Err(e),
        };
        let Ok(result) = result else {
            rejected.push(expression);
            continue;
        };
        fn numeric_cells(batches: &[arrow::record_batch::RecordBatch]) -> Vec<Option<BigDecimal>> {
            let mut values = Vec::new();
            for b in batches {
                if DecimalArbType::is_decimal_arb_field(b.schema().field(0)) {
                    values.extend(decoded(b.column(0).as_ref(), b.schema().field(0)));
                } else {
                    for row in 0..b.num_rows() {
                        let value = ScalarValue::try_from_array(b.column(0), row).unwrap();
                        values.push(if value.is_null() {None} else {
                            Some(value.to_string().parse::<BigDecimal>().unwrap_or_else(|_|panic!("successful scalar function returned nonnumeric {value:?}, field {:?}",b.schema().field(0))))
                        });
                    }
                }
            }
            values
        }
        assert_eq!(
            numeric_cells(&result),
            numeric_cells(&native),
            "scalar {expression} silently differs from native-decimal oracle"
        );
        checked.push(expression);
    }
    eprintln!(
        "scalar functions: {} explicit decimal_arb rejections, {} exact native-oracle matches, {} unavailable in native control; rejected={rejected:?}; matched={checked:?}; unavailable={native_rejected:?}",
        rejected.len(),
        checked.len(),
        native_rejected.len()
    );
}
