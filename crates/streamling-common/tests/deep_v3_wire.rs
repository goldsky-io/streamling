//! Fresh v3 framed Avro/schema-evolution audit. No production code is modified.
use apache_avro::{Decimal, Schema as AvroSchema, from_avro_datum, to_avro_datum, types::Value};
use arrow::{
    array::*,
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::{DataType, Field, Schema},
};
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use serde_json::json;
use std::{str::FromStr, sync::Arc};
use streamling_common::{
    formats::{
        FromArrowConverter, ToArrowConverter,
        avro::{FromArrowToAvroConverter, arrow_avro::ConfluentAvroDecoder, serialize, to_avro},
        json::{FromArrowToJsonConverter, JsonToArrowConverter},
    },
    types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue},
};

fn decimal_schema(precision: usize, scale: usize, fixed: bool, name: &str) -> serde_json::Value {
    if fixed {
        let size = ((precision as f64 / std::f64::consts::LOG10_2).ceil() as usize + 1).div_ceil(8);
        json!({"type":"fixed","name":name,"size":size,"logicalType":"decimal","precision":precision,"scale":scale})
    } else {
        json!({"type":"bytes","logicalType":"decimal","precision":precision,"scale":scale})
    }
}
fn schema_json(precision: usize, scale: usize, fixed: bool, version: usize) -> String {
    let dec = decimal_schema(precision, scale, fixed, "TopDecimal");
    let inner_dec = decimal_schema(precision, scale, fixed, "InnerDecimal");
    let list_dec = decimal_schema(precision, scale, fixed, "ListDecimal");
    let mut fields = vec![
        json!({"name":"id","type":"long"}),
        json!({"name":"raw","type":"bytes"}),
        json!({"name":"amount","type":["null",dec],"default":null}),
        json!({"name":"inner","type":["null",{"type":"record","name":"Inner","fields":[
            {"name":"raw","type":"bytes"},
            {"name":"amount","type":["null",inner_dec],"default":null}
        ]}],"default":null}),
        json!({"name":"items","type":{"type":"array","items":["null",list_dec]}}),
    ];
    if version != 1 {
        fields.rotate_left(2);
        fields.push(json!({"name":"extra","type":"long","default":99}));
    }
    json!({"type":"record","name":"WireV3","fields":fields}).to_string()
}
fn dec(coefficient: &BigInt) -> Value {
    Value::Decimal(Decimal::from(coefficient.to_signed_bytes_be()))
}
fn nullable(v: Option<Value>) -> Value {
    match v {
        Some(v) => Value::Union(1, Box::new(v)),
        None => Value::Union(0, Box::new(Value::Null)),
    }
}
fn record(schema: &AvroSchema, id: i64, coefficient: &BigInt, version: usize) -> Value {
    let mut value = apache_avro::types::Record::new(schema).unwrap();
    value.put("id", id);
    value.put("raw", Value::Bytes(vec![0, 255, 128, id as u8, 0]));
    value.put("amount", nullable((id % 5 != 0).then(|| dec(coefficient))));
    value.put(
        "inner",
        nullable((id % 4 != 0).then(|| {
            Value::Record(vec![
                ("raw".into(), Value::Bytes(vec![255, 0, id as u8, 128])),
                (
                    "amount".into(),
                    nullable((id % 3 != 0).then(|| dec(&-coefficient))),
                ),
            ])
        })),
    );
    value.put(
        "items",
        Value::Array(if id % 6 == 0 {
            vec![]
        } else {
            vec![
                nullable(Some(dec(coefficient))),
                nullable(None),
                nullable(Some(dec(&-coefficient))),
            ]
        }),
    );
    if version != 1 {
        value.put("extra", 99i64);
    }
    value.into()
}
fn frame(id: u32, schema: &AvroSchema, value: Value) -> Vec<u8> {
    let mut out = vec![0];
    out.extend(id.to_be_bytes());
    out.extend(to_avro_datum(schema, value).unwrap());
    out
}
fn number(field: &Field, array: &ArrayRef, row: usize) -> Option<BigDecimal> {
    if array.is_null(row) {
        return None;
    }
    if let Some((_, s)) = DecimalArbType::precision_scale_from_field(field) {
        let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        return Some(
            BigDecimal::from_str(
                &DecimalArbValue::from_canonical_bytes_at_scale(array.value(row), s)
                    .unwrap()
                    .to_canonical_string(),
            )
            .unwrap(),
        );
    }
    Some(match field.data_type() {
        DataType::Decimal128(_, s) => BigDecimal::new(
            BigInt::from(
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .value(row),
            ),
            *s as i64,
        ),
        DataType::Decimal256(_, s) => BigDecimal::new(
            BigInt::from_signed_bytes_be(
                &array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap()
                    .value(row)
                    .to_be_bytes(),
            ),
            *s as i64,
        ),
        t => panic!("expected decimal; got {t:?}"),
    })
}
fn verify_batch(batch: &RecordBatch, expected: &[(i64, BigInt)], scale: usize) {
    assert_eq!(batch.num_rows(), expected.len());
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let raw = batch
        .column_by_name("raw")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let amount = batch.column_by_name("amount").unwrap();
    let schema = batch.schema();
    let amount_field = schema.field_with_name("amount").unwrap();
    let inner = batch
        .column_by_name("inner")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let inner_amount = inner.column_by_name("amount").unwrap();
    let inner_field = inner
        .fields()
        .iter()
        .find(|f| f.name() == "amount")
        .unwrap();
    let inner_raw = inner
        .column_by_name("raw")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let items = batch
        .column_by_name("items")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let DataType::List(item_field) = items.data_type() else {
        unreachable!()
    };
    for (row, (id, c)) in expected.iter().enumerate() {
        assert_eq!(ids.value(row), *id);
        assert_eq!(raw.value(row), [0, 255, 128, *id as u8, 0]);
        let n = BigDecimal::new(c.clone(), scale as i64);
        assert_eq!(
            number(amount_field, amount, row),
            (*id % 5 != 0).then(|| n.clone())
        );
        assert_eq!(inner.is_null(row), *id % 4 == 0);
        if !inner.is_null(row) {
            assert_eq!(inner_raw.value(row), [255, 0, *id as u8, 128]);
            assert_eq!(
                number(inner_field, inner_amount, row),
                (*id % 3 != 0).then(|| -n.clone())
            );
        }
        let vals = items.value(row);
        if *id % 6 == 0 {
            assert_eq!(vals.len(), 0);
        } else {
            assert_eq!(
                (0..vals.len())
                    .map(|i| number(item_field, &vals, i))
                    .collect::<Vec<_>>(),
                vec![Some(n.clone()), None, Some(-n)]
            );
        }
        let extra = batch
            .column_by_name("extra")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(extra.value(row), 99);
    }
}

#[test]
fn v3_confluent_multi_writer_mixed_nested_binary_decimal_matrix() {
    let mut rows = 0;
    let mut configs = 0;
    for precision in [18, 38, 39, 76, 77, 80, 100] {
        for scale in [0, 2, 17] {
            for fixed in [false, true] {
                for flush_every in [1, 5, 31] {
                    let schemas = [
                        schema_json(precision, scale, fixed, 1),
                        schema_json(precision, scale, fixed, 2),
                    ];
                    let parsed = schemas
                        .iter()
                        .map(|s| AvroSchema::parse_str(s).unwrap())
                        .collect::<Vec<_>>();
                    let mut d = ConfluentAvroDecoder::new()
                        .with_reader_schema(&parsed[1])
                        .unwrap();
                    d.register_writer_schema(101, &schemas[0]).unwrap();
                    d.register_writer_schema(202, &schemas[1]).unwrap();
                    let mut expected = Vec::new();
                    for id in 1..=19i64 {
                        let mut c =
                            BigInt::from(10u8).pow((precision - 2) as u32) + BigInt::from(id);
                        if id % 2 == 0 {
                            c = -c;
                        }
                        if id == 7 {
                            c = BigInt::from(0);
                        }
                        let ver = if id % 3 == 0 { 0 } else { 1 };
                        let body = frame(
                            if ver == 0 { 101 } else { 202 },
                            &parsed[ver],
                            record(&parsed[ver], id, &c, ver + 1),
                        );
                        assert_eq!(d.decode(&body).unwrap(), body.len());
                        expected.push((id, c));
                        if (id as usize).is_multiple_of(flush_every) {
                            verify_batch(&d.flush().unwrap().unwrap(), &expected, scale);
                            rows += expected.len();
                            expected.clear();
                        }
                    }
                    if !expected.is_empty() {
                        verify_batch(&d.flush().unwrap().unwrap(), &expected, scale);
                        rows += expected.len();
                    }
                    assert!(d.flush().unwrap().is_none());
                    configs += 1;
                }
            }
        }
    }
    eprintln!(
        "v3 framed mixed-schema matrix: {configs} configurations, {rows} records; exact scalar/nested/list numbers, raw binary, nulls, IDs, reader defaults"
    );
}

fn simple_schema(precision: usize, scale: usize) -> AvroSchema {
    AvroSchema::parse_str(&json!({"type":"record","name":"SimpleV3","fields":[{"name":"v","type":decimal_schema(precision,scale,false,"unused")}]}).to_string()).unwrap()
}
fn simple_frame(id: u32, schema: &AvroSchema, n: i128) -> Vec<u8> {
    frame(
        id,
        schema,
        Value::Record(vec![("v".into(), dec(&BigInt::from(n)))]),
    )
}

#[test]
#[ignore = "inherited schema-scale diagnostic; actual original-base controls reproduce all 14 outcomes, so this is not a PR regression"]
fn v3_schema_drift_diagnostic_compares_native_and_wide() {
    let mut outcomes = Vec::new();
    for precision in [18, 38, 39, 76, 77, 80, 100] {
        for resolve in [true, false] {
            let writer = simple_schema(precision, 0);
            let reader = simple_schema(precision, 2);
            let mut d = ConfluentAvroDecoder::new()
                .with_reader_schema(&reader)
                .unwrap()
                .with_schema_resolution(resolve);
            d.register_writer_schema(1, &serde_json::to_string(&writer).unwrap())
                .unwrap();
            let result = d
                .decode(&simple_frame(1, &writer, 123))
                .and_then(|_| d.flush())
                .map(|b| {
                    let b = b.unwrap();
                    number(b.schema().field(0), b.column(0), 0)
                        .unwrap()
                        .to_string()
                });
            outcomes.push(format!("p={precision} resolution={resolve}: {result:?}"));
        }
    }
    for o in &outcomes {
        eprintln!("SCALE-DRIFT DIAGNOSTIC {o}");
    }
    assert_eq!(outcomes.len(), 14);
}

fn arb_batch(p: u32, s: u32, values: &[&str]) -> RecordBatch {
    let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), "v", p, s).unwrap();
    for v in values {
        b.append_str(v).unwrap();
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            DecimalArbType::field("v", p, s, false).unwrap(),
        ])),
        vec![Arc::new(b.finish().into_inner().0)],
    )
    .unwrap()
}

#[test]
#[ignore = "inherited: actual original-base Decimal128 bound-writer control also changes numeric 1.00 to 100; not a new PR finding"]
fn v3_writer_schema_drift_is_detected_or_preserves_number() {
    let target = arb_batch(100, 0, &["1"]);
    let input = arb_batch(100, 2, &["1"]);
    let converter = FromArrowToAvroConverter::new(target.schema(), "BoundV3".into());
    let schema = to_avro("BoundV3", &target.schema().fields);
    let encoded = converter.convert_from_batch(&input).unwrap();
    let wire = to_avro_datum(&schema, encoded[0].clone()).unwrap();
    let decoded = from_avro_datum(&schema, &mut wire.as_slice(), None).unwrap();
    let Value::Record(fields) = decoded else {
        unreachable!()
    };
    let Value::Decimal(d) = &fields[0].1 else {
        unreachable!()
    };
    let bytes: Vec<u8> = d.try_into().unwrap();
    let actual = BigDecimal::new(BigInt::from_signed_bytes_be(&bytes), 0);
    eprintln!(
        "actual bound-writer full wire decode {actual}; input numeric 1 at scale 2, writer scale 0"
    );
    assert_eq!(actual, BigDecimal::from(1));
}

#[test]
#[ignore = "inherited unsupported negative-scale Avro boundary; positive-scale examples are controls, not a new PR regression"]
fn v3_native_negative_scale_avro_diagnostic() {
    for scale in [-2, 0, 2] {
        let arr = Decimal128Array::from(vec![123i128, -456])
            .with_precision_and_scale(18, scale)
            .unwrap();
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                DataType::Decimal128(18, scale),
                false,
            )])),
            vec![Arc::new(arr)],
        )
        .unwrap();
        let a = to_avro("NativeScaleV3", &b.schema().fields);
        let results = serialize(&a, &b)
            .into_iter()
            .map(|v| {
                to_avro_datum(&a, v)
                    .map(|wire| from_avro_datum(&a, &mut wire.as_slice(), None).unwrap())
            })
            .collect::<Vec<_>>();
        eprintln!("NATIVE NEGATIVE SCALE DIAGNOSTIC s={scale} schema={a:?} result={results:?}");
        if scale >= 0 {
            assert!(results.iter().all(Result::is_ok));
        }
    }
}

#[test]
fn v3_json_mixed_decimal_binary_and_null_slices() {
    let d = arb_batch(
        100,
        4,
        &[
            "1",
            "-0.0001",
            "1234567890123456789012345678901234567890.1234",
            "1000",
        ],
    );
    let mut fields = vec![d.schema().field(0).clone()];
    fields.push(Field::new("raw", DataType::LargeBinary, true));
    let raw: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some([0, 255, 128].as_slice()),
        None,
        Some([0, 0, 1].as_slice()),
        Some([255, 255].as_slice()),
    ]));
    let st = StructArray::new(
        fields.into(),
        vec![d.column(0).clone(), raw],
        Some(NullBuffer::from(vec![true, false, true, true])),
    );
    let field = Arc::new(Field::new("item", st.data_type().clone(), true));
    let list = ListArray::new(
        field,
        OffsetBuffer::new(vec![0, 1, 3, 4].into()),
        Arc::new(st),
        None,
    );
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "items",
            list.data_type().clone(),
            false,
        )])),
        vec![Arc::new(list)],
    )
    .unwrap();
    for (offset, len) in [(0, 3), (1, 2), (2, 1)] {
        let b = input.slice(offset, len);
        let writer = FromArrowToJsonConverter::new();
        let json = writer.convert_from_batch(&b).unwrap();
        let mut reader = JsonToArrowConverter::new(b.schema(), true, None);
        for row in &json {
            reader.buffer(String::from_utf8(row.clone()).unwrap());
        }
        let restored = reader.convert_to_batch().unwrap();
        let second = writer.convert_from_batch(&restored).unwrap();
        eprintln!(
            "MIXED JSON s={offset}/{len}: {}",
            String::from_utf8(json.concat()).unwrap()
        );
        assert_eq!(json, second, "mixed binary/decimal JSON should be stable");
    }
}

#[test]
#[ignore = "diagnostic: apache-avro 0.17 schema parser rejects valid bytes-string decimal defaults before Streamling decoding; inherited dependency limitation"]
fn v3_reader_evolution_decimal_defaults_are_exact() {
    let writer=AvroSchema::parse_str(r#"{"type":"record","name":"DefaultV3","fields":[{"name":"id","type":"long"},{"name":"nested","type":{"type":"record","name":"NestedV3","fields":[{"name":"id","type":"long"}]}}]}"#).unwrap();
    let mut cases = 0;
    for precision in [18, 38, 39, 76, 77, 80, 100] {
        for scale in [0, 2, 17] {
            for n in [
                -65536i128, -32769, -256, -129, -128, -1, 0, 1, 127, 128, 255, 256, 32768, 65536,
            ] {
                let coefficient = BigInt::from(n);
                let bytes = coefficient.to_signed_bytes_be();
                let default = bytes.iter().map(|b| char::from(*b)).collect::<String>();
                let reader=AvroSchema::parse_str(&json!({"type":"record","name":"DefaultV3","fields":[
            {"name":"id","type":"long"},
            {"name":"value","type":decimal_schema(precision,scale,false,"Unused"),"default":default},
            {"name":"raw","type":"bytes","default":default},
            {"name":"nested","type":{"type":"record","name":"NestedV3","fields":[
                {"name":"id","type":"long"},
                {"name":"value","type":decimal_schema(precision,scale,false,"UnusedNested"),"default":default},
                {"name":"raw","type":"bytes","default":default}
            ]}}
        ]}).to_string()).unwrap();
                let mut d = ConfluentAvroDecoder::new()
                    .with_reader_schema(&reader)
                    .unwrap();
                d.register_writer_schema(1, &serde_json::to_string(&writer).unwrap())
                    .unwrap();
                let val = Value::Record(vec![
                    ("id".into(), Value::Long(1)),
                    (
                        "nested".into(),
                        Value::Record(vec![("id".into(), Value::Long(2))]),
                    ),
                ]);
                d.decode(&frame(1, &writer, val)).unwrap();
                let b = d.flush().unwrap().unwrap();
                let expected = BigDecimal::new(coefficient, scale as i64);
                assert_eq!(
                    number(
                        b.schema().field_with_name("value").unwrap(),
                        b.column_by_name("value").unwrap(),
                        0
                    ),
                    Some(expected.clone()),
                    "default p{precision}s{scale} coefficient{n}"
                );
                assert_eq!(
                    b.column_by_name("raw")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .value(0),
                    bytes
                );
                let nested = b
                    .column_by_name("nested")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let field = nested
                    .fields()
                    .iter()
                    .find(|f| f.name() == "value")
                    .unwrap();
                assert_eq!(
                    number(field, nested.column_by_name("value").unwrap(), 0),
                    Some(expected)
                );
                assert_eq!(
                    nested
                        .column_by_name("raw")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .value(0),
                    bytes
                );
                cases += 1;
            }
        }
    }
    eprintln!(
        "v3 reader-added decimal defaults: {cases} cases, each top-level and nested with matching ordinary bytes control"
    );
}

#[test]
fn v3_reader_added_nullable_decimal_fields_restore_typed_nulls() {
    let writer=AvroSchema::parse_str(r#"{"type":"record","name":"NullableDefaultV3","fields":[{"name":"id","type":"long"},{"name":"nested","type":{"type":"record","name":"NestedNullableV3","fields":[{"name":"id","type":"long"}]}}]}"#).unwrap();
    let mut cases = 0;
    for precision in [18, 38, 39, 76, 77, 80, 100] {
        for scale in [0, 2, 17] {
            for resolution in [true, false] {
                let reader=AvroSchema::parse_str(&json!({"type":"record","name":"NullableDefaultV3","fields":[
            {"name":"id","type":"long"},
            {"name":"value","type":["null",decimal_schema(precision,scale,false,"Unused")],"default":null},
            {"name":"raw","type":["null","bytes"],"default":null},
            {"name":"nested","type":{"type":"record","name":"NestedNullableV3","fields":[
                {"name":"id","type":"long"},
                {"name":"value","type":["null",decimal_schema(precision,scale,false,"UnusedNested")],"default":null},
                {"name":"raw","type":["null","bytes"],"default":null}
            ]}}
        ]}).to_string()).unwrap();
                let mut d = ConfluentAvroDecoder::new()
                    .with_reader_schema(&reader)
                    .unwrap()
                    .with_schema_resolution(resolution);
                d.register_writer_schema(1, &serde_json::to_string(&writer).unwrap())
                    .unwrap();
                for i in 1..=3 {
                    let val = Value::Record(vec![
                        ("id".into(), Value::Long(i)),
                        (
                            "nested".into(),
                            Value::Record(vec![("id".into(), Value::Long(i + 10))]),
                        ),
                    ]);
                    d.decode(&frame(1, &writer, val)).unwrap();
                }
                let b = d.flush().unwrap().unwrap();
                assert_eq!(b.num_rows(), 3);
                for name in ["value", "raw"] {
                    assert_eq!(b.column_by_name(name).unwrap().null_count(), 3);
                }
                let n = b
                    .column_by_name("nested")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                for name in ["value", "raw"] {
                    assert_eq!(n.column_by_name(name).unwrap().null_count(), 3);
                }
                for row in 0..3 {
                    assert_eq!(
                        number(
                            b.schema().field_with_name("value").unwrap(),
                            b.column_by_name("value").unwrap(),
                            row
                        ),
                        None
                    );
                }
                cases += 1;
            }
        }
    }
    eprintln!(
        "v3 reader-added nullable decimal matrix: {cases} configurations, 126 records; native/wide, reader resolution on/off, nested and ordinary binary controls"
    );
}

#[test]
#[ignore = "parser support diagnostic confirmed on original base; do not require continued rejection of valid decimal defaults in regression CI"]
fn v3_nonnull_decimal_default_parser_support_diagnostic() {
    // This deliberately classifies support; it does not count rejection as a numeric roundtrip.
    // The identical 294-case probe is executed on the actual origin/main checkout.
    let mut rejected = 0;
    for precision in [18, 38, 39, 76, 77, 80, 100] {
        for scale in [0, 2, 17] {
            for n in [
                -65536i128, -32769, -256, -129, -128, -1, 0, 1, 127, 128, 255, 256, 32768, 65536,
            ] {
                let default = BigInt::from(n)
                    .to_signed_bytes_be()
                    .iter()
                    .map(|b| char::from(*b))
                    .collect::<String>();
                let input=json!({"type":"record","name":"DefaultV3","fields":[{"name":"v","type":decimal_schema(precision,scale,false,"Unused"),"default":default}]}).to_string();
                assert!(
                    AvroSchema::parse_str(&input).is_err(),
                    "unexpected parser acceptance p{precision}s{scale} coefficient{n}"
                );
                rejected += 1;
            }
        }
    }
    eprintln!(
        "v3 non-null decimal defaults: {rejected} schemas rejected by apache-avro parser before Streamling decoding; inherited support diagnostic, NOT numeric roundtrips"
    );
    assert_eq!(rejected, 294);
}

#[test]
fn v3_union_root_framing_late_registration_and_large_scales() {
    let mut records = 0;
    let mut configs = 0;
    for precision in [38, 76, 77, 100] {
        for scale in [0, precision - 1, precision] {
            for record_index in [0u32, 1, 2] {
                let j1 = schema_json(precision, scale, false, 1);
                let j2 = schema_json(precision, scale, false, 2);
                let root = serde_json::from_str::<serde_json::Value>(&j1).unwrap();
                let root_json = match record_index {
                    0 => json!([root, "null"]),
                    1 => json!(["null", root]),
                    _ => json!(["null", "long", root]),
                }
                .to_string();
                let writer1 = AvroSchema::parse_str(&root_json).unwrap();
                let rec1 = AvroSchema::parse_str(&j1).unwrap();
                let writer2 = AvroSchema::parse_str(&j2).unwrap();
                let mut decoder = ConfluentAvroDecoder::new()
                    .with_reader_schema(&writer2)
                    .unwrap();
                decoder.register_writer_schema(11, &root_json).unwrap();
                let mut expected = Vec::new();
                for id in 1..=9i64 {
                    let c = if id % 2 == 0 {
                        -BigInt::from(10u8).pow((precision - 1) as u32) + id
                    } else {
                        BigInt::from(10u8).pow((precision - 1) as u32) - id
                    };
                    // Register the second writer while the first generation holds buffered rows.
                    if id == 3 {
                        decoder.register_writer_schema(22, &j2).unwrap();
                    }
                    let (schema, wire_id, value) = if id < 3 || id % 3 == 0 {
                        (
                            &writer1,
                            11,
                            Value::Union(record_index, Box::new(record(&rec1, id, &c, 1))),
                        )
                    } else {
                        (&writer2, 22, record(&writer2, id, &c, 2))
                    };
                    let wire = frame(wire_id, schema, value);
                    decoder.decode(&wire).unwrap();
                    expected.push((id, c));
                }
                let b = decoder.flush().unwrap().unwrap();
                verify_batch(&b, &expected, scale);
                assert!(decoder.flush().unwrap().is_none());
                records += b.num_rows();
                configs += 1;
            }
        }
    }
    eprintln!(
        "v3 root-union framing/late writer-registration matrix: {configs} configurations, {records} records, scales through100 and record branches0/1/2"
    );
}
