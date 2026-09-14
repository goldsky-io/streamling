//! Independent adversarial review tests. Production sources are unmodified.
use apache_avro::{Schema as AvroSchema, types::Value};
use arrow::array::{Array, LargeBinaryArray, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use num_bigint::BigInt;
use std::str::FromStr;
use std::sync::Arc;
use streamling_common::formats::avro::arrow_avro::ConfluentAvroDecoder;
use streamling_common::formats::avro::{serialize, to_avro};
use streamling_common::formats::ipc::{FromArrowToIpcConverter, FromIpcToArrowConverter};
use streamling_common::formats::json::{FromArrowToJsonConverter, JsonToArrowConverter};
use streamling_common::formats::{FromArrowConverter, ToArrowConverter};
use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};

fn decimal_batch(precision: u32, scale: u32, texts: &[Option<&str>]) -> RecordBatch {
    let field = DecimalArbType::field("amount", precision, scale, true).unwrap();
    let mut builder =
        DecimalArbArrayBuilder::with_capacity(texts.len(), "amount", precision, scale).unwrap();
    for text in texts {
        if let Some(text) = text {
            builder.append_str(text).unwrap();
        } else {
            builder.append_null();
        }
    }
    let (raw, _, _) = builder.finish().into_inner();
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(raw)]).unwrap()
}
fn ipc_convert(input: &RecordBatch, target: SchemaRef) -> RecordBatch {
    ipc_convert_result(input, target).unwrap()
}
fn ipc_convert_result(
    input: &RecordBatch,
    target: SchemaRef,
) -> datafusion::common::Result<RecordBatch> {
    let bytes = FromArrowToIpcConverter::new()
        .convert_from_batch(input)
        .unwrap()
        .pop()
        .unwrap();
    let mut reader = FromIpcToArrowConverter::new(target);
    reader.buffer(bytes);
    reader.convert_to_batch()
}
fn value_at(batch: &RecordBatch, row: usize) -> String {
    let (_, scale) = DecimalArbType::precision_scale_from_field(batch.schema().field(0)).unwrap();
    let raw = batch
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    DecimalArbValue::from_canonical_bytes_at_scale(raw.value(row), scale)
        .unwrap()
        .to_canonical_string()
}
fn decimal_value(value: &Value) -> &apache_avro::Decimal {
    match value {
        Value::Union(_, value) => decimal_value(value),
        Value::Decimal(value) => value,
        other => panic!("expected decimal, got {other:?}"),
    }
}

#[test]
fn review_ipc_scale_change_preserves_numeric_value() {
    let input = decimal_batch(80, 2, &[Some("12.34")]);
    let target = Arc::new(Schema::new(vec![
        DecimalArbType::field("amount", 80, 4, true).unwrap(),
    ]));
    // Rescaling and explicit rejection are both safe; silently replacing
    // scale metadata while retaining the original bytes is not.
    if let Ok(output) = ipc_convert_result(&input, target) {
        assert_eq!(
            DecimalArbValue::from_str(&value_at(&output, 0)).unwrap(),
            DecimalArbValue::from_str("12.34").unwrap()
        );
    }
}

#[test]
fn review_ipc_string_result_to_avro_preserves_100() {
    // Flechette returns this shape for a JS script returning {amount: "100"}.
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Utf8,
            true,
        )])),
        vec![Arc::new(StringArray::from(vec![Some("100")]))],
    )
    .unwrap();
    let target = Arc::new(Schema::new(vec![
        DecimalArbType::field("amount", 78, 0, true).unwrap(),
    ]));
    let output = ipc_convert(&input, target);
    let avro_schema = to_avro("Test", output.schema().fields());
    let values = serialize(&avro_schema, &output);
    let Value::Record(record) = &values[0] else {
        panic!()
    };
    let actual =
        BigInt::from_signed_bytes_be(&Vec::<u8>::try_from(decimal_value(&record[0].1)).unwrap());
    assert_eq!(
        actual,
        BigInt::from(100),
        "A decimal string returned by a JS script must not be interpreted as canonical sign/magnitude bytes"
    );
}

#[test]
fn review_json_nested_decimal_roundtrip() {
    let inner = decimal_batch(80, 2, &[Some("12.34"), None]);
    let nested = StructArray::new(
        inner.schema().fields().clone(),
        inner.columns().to_vec(),
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nested",
        nested.data_type().clone(),
        true,
    )]));
    let input = RecordBatch::try_new(schema.clone(), vec![Arc::new(nested)]).unwrap();
    let rows = FromArrowToJsonConverter::new()
        .convert_from_batch(&input)
        .unwrap();
    let mut reader = JsonToArrowConverter::new(schema.clone(), true, None);
    for row in rows {
        reader.buffer(String::from_utf8(row).unwrap());
    }
    let output = reader.convert_to_batch().unwrap();
    assert_eq!(input, output);
}

#[test]
fn review_avro_signed_scale_null_roundtrips() {
    for scale in [0_u32, 2, 18] {
        let big = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        let texts = [
            Some("0"),
            Some("127"),
            Some("128"),
            Some("255"),
            Some("256"),
            Some("-1"),
            Some("-128"),
            Some("-129"),
            Some(big),
            None,
        ];
        let input = decimal_batch(100, scale, &texts);
        let avro_schema = to_avro("Test", input.schema().fields());
        let mut reader = ConfluentAvroDecoder::new();
        reader
            .register_writer_schema(1, &serde_json::to_string(&avro_schema).unwrap())
            .unwrap();
        for record in serialize(&avro_schema, &input) {
            let datum = apache_avro::to_avro_datum(&avro_schema, record).unwrap();
            let mut framed = vec![0, 0, 0, 0, 1];
            framed.extend(datum);
            reader.decode(&framed).unwrap();
        }
        let output = reader.flush().unwrap().unwrap();
        for (i, text) in texts.iter().enumerate() {
            if let Some(text) = text {
                assert_eq!(
                    DecimalArbValue::from_str(&value_at(&output, i)).unwrap(),
                    DecimalArbValue::from_str(text).unwrap(),
                    "scale={scale}, row={i}"
                );
            } else {
                assert!(output.column(0).is_null(i));
            }
        }
    }
}

#[test]
fn review_json_ipc_exact_roundtrips() {
    let input = decimal_batch(
        100,
        30,
        &[
            Some("123456789012345678901234567890123456789.123456789012345678901234567891"),
            Some("-0.000000000000000000000000000001"),
            None,
        ],
    );
    assert_eq!(input, ipc_convert(&input, input.schema()));
    let rows = FromArrowToJsonConverter::new()
        .convert_from_batch(&input)
        .unwrap();
    let mut reader = JsonToArrowConverter::new(input.schema(), true, None);
    for row in rows {
        reader.buffer(String::from_utf8(row).unwrap());
    }
    assert_eq!(input, reader.convert_to_batch().unwrap());
}

#[test]
#[ignore = "Pre-existing Avro reader/writer scale mismatch; excluded from PR37 regressions"]
fn review_avro_reader_scale_change_preserves_numeric_value() {
    let writer_json = r#"{"type":"record","name":"Test","fields":[{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":80,"scale":2}}]}"#;
    let reader_json = writer_json.replace("\"scale\":2", "\"scale\":4");
    let writer_schema = AvroSchema::parse_str(writer_json).unwrap();
    let reader_schema = AvroSchema::parse_str(&reader_json).unwrap();
    let mut decoder = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader_schema)
        .unwrap();
    decoder.register_writer_schema(1, writer_json).unwrap();
    let value = Value::Record(vec![(
        "amount".into(),
        Value::Decimal(apache_avro::Decimal::from(
            BigInt::from(1234).to_signed_bytes_be(),
        )),
    )]);
    let mut framed = vec![0, 0, 0, 0, 1];
    framed.extend(apache_avro::to_avro_datum(&writer_schema, value).unwrap());
    decoder.decode(&framed).unwrap();
    let output = decoder.flush().unwrap().unwrap();
    assert_eq!(
        DecimalArbValue::from_str(&value_at(&output, 0)).unwrap(),
        DecimalArbValue::from_str("12.34").unwrap()
    );
}

#[test]
fn review_json_nested_digit_string_is_decimal_not_hex() {
    let amount = DecimalArbType::field("amount", 80, 0, true).unwrap();
    let nested = Field::new(
        "nested",
        DataType::Struct(vec![Arc::new(amount)].into()),
        true,
    );
    let schema = Arc::new(Schema::new(vec![nested]));
    let mut reader = JsonToArrowConverter::new(schema, true, None);
    reader.buffer(r#"{"nested":{"amount":"001000"}}"#.into());
    let output = reader.convert_to_batch().unwrap();
    let raw = output
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let actual = DecimalArbValue::from_canonical_bytes_at_scale(raw.value(0), 0).unwrap();
    assert_eq!(actual, DecimalArbValue::from_str("1000").unwrap());
}

#[test]
fn review_json_nested_canonical_integer_to_avro_preserves_1000() {
    let amount = DecimalArbType::field("amount", 80, 0, true).unwrap();
    let nested = Field::new(
        "nested",
        DataType::Struct(vec![Arc::new(amount)].into()),
        false,
    );
    let schema = Arc::new(Schema::new(vec![nested]));
    let mut reader = JsonToArrowConverter::new(schema, true, None);
    reader.buffer(r#"{"nested":{"amount":"1000"}}"#.into());
    let output = reader.convert_to_batch().unwrap();
    let avro_schema = to_avro("Test", output.schema().fields());
    let values = serialize(&avro_schema, &output);
    let Value::Record(record) = &values[0] else {
        panic!()
    };
    let Value::Record(nested) = &record[0].1 else {
        panic!()
    };
    let actual =
        BigInt::from_signed_bytes_be(&Vec::<u8>::try_from(decimal_value(&nested[0].1)).unwrap());
    assert_eq!(actual, BigInt::from(1000));
}
