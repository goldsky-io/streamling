use arrow::array::{Array, LargeBinaryArray};
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use std::sync::Arc;
use streamling_common::functions::decimal_arb_ops::*;
use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};

fn invoke(
    op: &dyn ScalarUDFImpl,
    a: &str,
    pa: u32,
    sa: u32,
    b: &str,
    pb: u32,
    sb: u32,
) -> DecimalArbValue {
    invoke_checked(op, a, pa, sa, b, pb, sb).unwrap()
}

fn invoke_checked(
    op: &dyn ScalarUDFImpl,
    a: &str,
    pa: u32,
    sa: u32,
    b: &str,
    pb: u32,
    sb: u32,
) -> datafusion::common::Result<DecimalArbValue> {
    let mut ba = DecimalArbArrayBuilder::with_capacity(1, "a", pa, sa).unwrap();
    let mut bb = DecimalArbArrayBuilder::with_capacity(1, "b", pb, sb).unwrap();
    ba.append_str(a).unwrap();
    bb.append_str(b).unwrap();
    let fields = vec![
        Arc::new(DecimalArbType::field("a", pa, sa, false).unwrap()),
        Arc::new(DecimalArbType::field("b", pb, sb, false).unwrap()),
    ];
    let ret = op.return_field_from_args(ReturnFieldArgs {
        arg_fields: &fields,
        scalar_arguments: &[None, None],
    })?;
    let (_, out_scale) = DecimalArbType::precision_scale_from_field(&ret).unwrap();
    let out = op.invoke_with_args(ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::new(ba.finish().into_inner().0)),
            ColumnarValue::Array(Arc::new(bb.finish().into_inner().0)),
        ],
        arg_fields: fields,
        number_rows: 1,
        return_field: ret,
        config_options: Arc::default(),
    })?;
    Ok(match out {
        ColumnarValue::Array(arr) => DecimalArbValue::from_canonical_bytes_at_scale(
            arr.as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0),
            out_scale,
        )
        .unwrap(),
        _ => panic!("expected array"),
    })
}

// Independent oracle: exact integer quotient/remainder, with one HalfEven
// rounding at the requested output scale. Never uses BigDecimal division.
fn rational_div(a: &str, b: &str, scale: u32) -> DecimalArbValue {
    let a: DecimalArbValue = a.parse().unwrap();
    let b: DecimalArbValue = b.parse().unwrap();
    let (mut n, sa) = a.as_bigdecimal().as_bigint_and_exponent();
    let (mut d, sb) = b.as_bigdecimal().as_bigint_and_exponent();
    let shift = i64::from(scale) + sb - sa;
    if shift >= 0 {
        n *= BigInt::from(10).pow(shift as u32)
    } else {
        d *= BigInt::from(10).pow((-shift) as u32)
    }
    let neg = n.is_negative() != d.is_negative();
    n = n.abs();
    d = d.abs();
    let mut q = &n / &d;
    let twice_r = (&n % &d) * 2;
    if twice_r > d || (twice_r == d && !(&q % BigInt::from(2)).is_zero()) {
        q += 1
    }
    if neg {
        q = -q
    }
    DecimalArbValue::from_bigint_and_scale(q, scale.into())
}

#[test]
fn review_division_wide_integer_by_small_fraction() {
    let a = "999999999999999999999999999999999999999999999999999999999999999999999999999998";
    let b = "0.000000000000000003";
    let expected = rational_div(a, b, 18);
    let actual = invoke(&DecimalArbDivFunc::new(), a, 78, 0, b, 78, 18);
    assert_eq!(
        actual, expected,
        "exact rounded division: actual={actual}, expected={expected}"
    );
}

#[test]
fn review_division_scale_above_100() {
    let actual = invoke(&DecimalArbDivFunc::new(), "1", 200, 120, "3", 200, 120);
    let expected = rational_div("1", "3", 120);
    assert_eq!(
        actual, expected,
        "120 decimal places: actual={actual}, expected={expected}"
    );
}

#[test]
fn review_division_avoids_double_rounding_near_half_even_tie() {
    let a = format!("5{}1", "0".repeat(100));
    let b = format!("1{}", "0".repeat(120));
    let actual = invoke(&DecimalArbDivFunc::new(), &a, 150, 0, &b, 150, 0);
    let expected = rational_div(&a, &b, 18);
    assert_eq!(actual, expected, "actual={actual}, expected={expected}");
}

#[test]
fn review_multiplication_at_precision_cap_must_not_silently_become_zero() {
    let result = invoke_checked(
        &DecimalArbMulFunc::new(),
        "1e-40000",
        40001,
        40000,
        "1e-40000",
        40001,
        40000,
    );
    match result {
        Ok(actual) => assert_eq!(
            actual,
            "1e-80000".parse::<DecimalArbValue>().unwrap(),
            "multiplication must preserve the exact product or explicitly reject precision overflow"
        ),
        Err(error) => {
            let message = error.to_string().to_lowercase();
            assert!(
                ["precision", "scale", "overflow"]
                    .iter()
                    .any(|word| message.contains(word)),
                "expected an actionable representability error, got {error}"
            );
        }
    }
}

#[tokio::test]
async fn review_avg_wide_values_preserves_fraction() {
    use arrow::record_batch::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use streamling_common::functions::decimal_arb_aggregates::DecimalArbAvgUdaf;
    let ctx = SessionContext::new();
    ctx.register_udaf(DecimalArbAvgUdaf::into_udaf());
    let mut builder = DecimalArbArrayBuilder::with_capacity(3, "v", 150, 0).unwrap();
    let v = format!("1{}", "0".repeat(110));
    for text in [&v, "0", "0"] {
        builder.append_str(text).unwrap();
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            DecimalArbType::field("v", 150, 0, false).unwrap(),
        ])),
        vec![Arc::new(builder.finish().into_inner().0)],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();
    let batches = ctx
        .sql("SELECT AVG(v) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    // AVG's advertised output scale is input scale + 1.
    let actual = DecimalArbValue::from_canonical_bytes_at_scale(arr.value(0), 1).unwrap();
    let expected = rational_div(&v, "3", 1);
    assert_eq!(actual, expected, "actual={actual}, expected={expected}");
}

#[test]
fn review_division_grid_exact_oracle() {
    let mut failures = Vec::new();
    let mut checks = 0;
    for a in [
        "1",
        "-1",
        "7",
        "123456789012345678901234567890123456789012345678901234567890123456789012345678",
    ] {
        for b in ["3", "7", "-6", "0.000000000000000003"] {
            for s in [0, 18, 80, 100, 120] {
                let bs = if b.contains('.') { s.max(18) } else { s };
                let actual = invoke(&DecimalArbDivFunc::new(), a, 300, s, b, 300, bs);
                let expected = rational_div(a, b, s.max(18));
                checks += 1;
                if actual != expected {
                    failures.push(format!(
                        "a={a}, b={b}, scales={s}/{bs}: actual={actual}, expected={expected}"
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} / {checks} division cases wrong:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn review_codec_and_exact_arithmetic_grid() {
    let mut state = 0x15abc78_u64;
    for i in 0..2000 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let n = BigInt::from(state as i64) * BigInt::from(10).pow(i % 90) + BigInt::from(i);
        let sa = i % 65;
        let a = DecimalArbValue::from_bigint_and_scale(n, sa.into());
        let b = DecimalArbValue::from_bigint_and_scale(
            BigInt::from((i as i64 % 193) - 96),
            (i % 19).into(),
        );
        let atext = a.to_canonical_string();
        let btext = b.to_canonical_string();
        let bytes = a.to_canonical_bytes_at_scale(sa);
        assert_eq!(
            DecimalArbValue::from_canonical_bytes_at_scale(&bytes, sa).unwrap(),
            a
        );
        let av = a.as_bigdecimal();
        let bv = b.as_bigdecimal();
        let (ai, _) = av.as_bigint_and_exponent();
        let (bi, _) = bv.as_bigint_and_exponent();
        let sb = i % 19;
        let aligned_scale = sa.max(sb);
        let ax = &ai * BigInt::from(10).pow(aligned_scale - sa);
        let bx = &bi * BigInt::from(10).pow(aligned_scale - sb);
        for (op, expected) in [
            (
                Box::new(DecimalArbAddFunc::new()) as Box<dyn ScalarUDFImpl>,
                DecimalArbValue::from_bigint_and_scale(&ax + &bx, aligned_scale.into()),
            ),
            (
                Box::new(DecimalArbSubFunc::new()),
                DecimalArbValue::from_bigint_and_scale(&ax - &bx, aligned_scale.into()),
            ),
            (
                Box::new(DecimalArbMulFunc::new()),
                DecimalArbValue::from_bigint_and_scale(&ai * &bi, (sa + sb).into()),
            ),
        ] {
            assert_eq!(
                invoke(op.as_ref(), &atext, 300, sa, &btext, 300, sb),
                expected
            );
        }
        if !bv.is_zero() {
            assert_eq!(
                invoke(&DecimalArbModFunc::new(), &atext, 300, sa, &btext, 300, sb),
                DecimalArbValue::from_bigint_and_scale(ax % bx, aligned_scale.into())
            );
        }
    }
}
