//! Independent Python Fraction oracle for the repaired division/AVG path.
use arrow::{array::*, datatypes::Schema};
use bigdecimal::BigDecimal;
use datafusion::{
    logical_expr::{Accumulator, AggregateUDFImpl, function::AccumulatorArgs},
    physical_expr::{PhysicalExpr, expressions::Column},
    scalar::ScalarValue,
};
use num_bigint::BigInt;
use serde_json::Value;
use std::{str::FromStr, sync::Arc};
use streamling_common::{
    functions::{decimal_arb_aggregates::DecimalArbAvgUdaf, decimal_arb_ops::exact_div_at_scale},
    types::decimal_arb::{
        DecimalArbArray, DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
    },
};
fn oracle() -> Value {
    serde_json::from_str(include_str!("decimal_arb_math_oracle.json")).unwrap()
}
#[test]
fn exact_division_against_python_fraction_oracle() {
    let data = oracle();
    let cases = data["division"].as_array().unwrap();
    for c in cases {
        let a = BigDecimal::new(
            BigInt::from_str(c["n"].as_str().unwrap()).unwrap(),
            c["sa"].as_i64().unwrap(),
        );
        let b = BigDecimal::new(
            BigInt::from_str(c["d"].as_str().unwrap()).unwrap(),
            c["sb"].as_i64().unwrap(),
        );
        let scale = c["scale"].as_u64().unwrap() as u32;
        let actual = exact_div_at_scale(&a, &b, scale);
        let expected = BigDecimal::new(
            BigInt::from_str(c["expected"].as_str().unwrap()).unwrap(),
            scale as i64,
        );
        assert_eq!(actual, expected, "case {c}");
    }
    eprintln!(
        "{} exact division cases passed independent Python Fraction.__round__ oracle",
        cases.len()
    );
}
fn accumulator(scale: u32) -> Box<dyn Accumulator> {
    let field = Arc::new(DecimalArbType::field("v", 600, scale, true).unwrap());
    let schema = Schema::new(vec![field.clone()]);
    let exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("v", 0))];
    DecimalArbAvgUdaf::new()
        .accumulator(AccumulatorArgs {
            return_field: Arc::new(DecimalArbType::field("avg", 601, scale + 1, true).unwrap()),
            schema: &schema,
            ignore_nulls: true,
            order_bys: &[],
            is_reversed: false,
            name: "avg",
            is_distinct: false,
            exprs: &exprs,
            expr_fields: &[field],
        })
        .unwrap()
}
fn input(values: &[Value], scale: u32) -> ArrayRef {
    let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), "v", 600, scale).unwrap();
    for v in values {
        if v.is_null() {
            b.append_null();
        } else {
            let n = BigInt::from_str(v.as_str().unwrap()).unwrap();
            b.append_value(&DecimalArbValue::from_bigint_and_scale(n, scale as i64))
                .unwrap();
        }
    }
    Arc::new(b.finish().into_inner().0)
}
fn assert_value(actual: ScalarValue, expected: &Value, scale: u32) {
    match actual {
        ScalarValue::LargeBinary(None) => assert!(expected.is_null()),
        ScalarValue::LargeBinary(Some(bytes)) => {
            let actual = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, scale).unwrap();
            let expected = DecimalArbValue::from_bigint_and_scale(
                BigInt::from_str(expected.as_str().unwrap()).unwrap(),
                scale as i64,
            );
            assert_eq!(actual, expected);
        }
        other => panic!("{other:?}"),
    }
}
#[test]
fn avg_batch_partition_merge_against_python_fraction_oracle() {
    let data = oracle();
    let cases = data["avg"].as_array().unwrap();
    let mut checked = 0;
    for c in cases {
        let scale = c["scale"].as_u64().unwrap() as u32;
        let values = c["values"].as_array().unwrap();
        let expected = &c["expected"];
        for chunks in [1_usize, 2, 5, 100] {
            let mut serial = accumulator(scale);
            for chunk in values.chunks(chunks) {
                serial.update_batch(&[input(chunk, scale)]).unwrap();
                let _ = serial.evaluate().unwrap();
            }
            assert_value(serial.evaluate().unwrap(), expected, scale + 1);
            let mut combined = accumulator(scale);
            let mut sums = Vec::new();
            let mut counts = Vec::new();
            // Include an empty partition so empty-state merging is exercised.
            for chunk in std::iter::once(&[][..]).chain(values.chunks(chunks)) {
                let mut partial = accumulator(scale);
                partial.update_batch(&[input(chunk, scale)]).unwrap();
                let state = partial.state().unwrap();
                let ScalarValue::LargeBinary(sum) = state[0].clone() else {
                    panic!()
                };
                let ScalarValue::Int64(count) = state[1] else {
                    panic!()
                };
                sums.push(sum);
                counts.push(count);
            }
            combined
                .merge_batch(&[
                    Arc::new(LargeBinaryArray::from_iter(
                        sums.iter().map(|s| s.as_deref()),
                    )),
                    Arc::new(Int64Array::from(counts)),
                ])
                .unwrap();
            assert_value(combined.evaluate().unwrap(), expected, scale + 1);
            assert_value(combined.evaluate().unwrap(), expected, scale + 1);
            checked += 1;
        }
    }
    eprintln!("{checked} AVG batch/partition shapes passed Python Fraction oracle");
}

#[test]
fn native_narrowing_ties_and_precision_edges_python_fraction_oracle() {
    let data = oracle();
    let cases = data["casts"].as_array().unwrap();
    let mut accepted = 0;
    let mut rejected = 0;
    for c in cases {
        let p = c["precision"].as_u64().unwrap() as u8;
        let s = c["scale"].as_i64().unwrap() as i8;
        let source_scale = c["source_scale"].as_u64().unwrap() as u32;
        let v = DecimalArbValue::from_bigint_and_scale(
            BigInt::from_str(c["n"].as_str().unwrap()).unwrap(),
            source_scale as i64,
        );
        let mut b = DecimalArbArrayBuilder::with_capacity(3, "v", 600, source_scale).unwrap();
        b.append_null();
        b.append_value(&v).unwrap();
        b.append_null();
        let a = b.finish();
        let result = if c["bits"] == 128 {
            a.to_decimal128(p, s, "narrowed").map(|out| {
                assert!(out.is_null(0) && !out.is_null(1) && out.is_null(2));
                let actual = BigInt::from(out.value(1));
                let widened =
                    DecimalArbArray::from_decimal128(&out, s, 600, s.max(0) as u32, "v").unwrap();
                assert_eq!(
                    widened.value(1).unwrap().unwrap(),
                    DecimalArbValue::from_bigint_and_scale(actual.clone(), s as i64)
                );
                actual
            })
        } else {
            a.to_decimal256(p, s, "narrowed").map(|out| {
                assert!(out.is_null(0) && !out.is_null(1) && out.is_null(2));
                let actual = BigInt::from_signed_bytes_be(&out.value(1).to_be_bytes());
                let widened =
                    DecimalArbArray::from_decimal256(&out, s, 600, s.max(0) as u32, "v").unwrap();
                assert_eq!(
                    widened.value(1).unwrap().unwrap(),
                    DecimalArbValue::from_bigint_and_scale(actual.clone(), s as i64)
                );
                actual
            })
        };
        if c["fits"].as_bool().unwrap() {
            assert_eq!(
                result.unwrap(),
                BigInt::from_str(c["expected"].as_str().unwrap()).unwrap(),
                "case {c}"
            );
            accepted += 1;
        } else {
            assert!(result.is_err(), "precision overflow silently accepted: {c}");
            rejected += 1;
        }
    }
    eprintln!(
        "{accepted} exact native narrowing/widening round trips and {rejected} explicit precision overflows passed Fraction oracle"
    );
}
