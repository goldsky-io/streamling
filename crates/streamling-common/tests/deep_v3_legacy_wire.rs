//! Self-contained companion-plugin wire compatibility controls.
//! Contract pinned to goldsky-plugins 11d5e0b: FSB32, streamling.u256 metadata,
//! and unsigned big-endian payloads. The actual-source include remains a scratch
//! audit probe; these tests do not require a sibling repository checkout.
use apache_avro::{from_avro_datum, to_avro_datum, types::Value};
use arrow::{
    array::*,
    buffer::OffsetBuffer,
    compute::kernels::cast_utils::parse_decimal,
    datatypes::{DataType, Decimal256Type, Field, Schema},
};
use num_bigint::BigUint;
use std::sync::Arc;
use streamling_common::formats::{
    FromArrowConverter,
    avro::{
        FromArrowToAvroConverter, arrow_avro::ConfluentAvroDecoder,
        post_process_avro_schema_for_writing, to_avro,
    },
};

fn legacy_u256_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(32), nullable).with_metadata(
        std::collections::HashMap::from([(
            "ARROW:extension:name".into(),
            "streamling.u256".into(),
        )]),
    )
}

fn unsigned_be_fixture(text: &str) -> [u8; 32] {
    let text = text.strip_prefix("0x").unwrap_or(text);
    let magnitude = BigUint::parse_bytes(text.as_bytes(), 16)
        .unwrap()
        .to_bytes_be();
    assert!(magnitude.len() <= 32);
    let mut out = [0; 32];
    out[32 - magnitude.len()..].copy_from_slice(&magnitude);
    out
}

fn input() -> RecordBatch {
    let texts = [
        Some("0x0"),
        Some("0x1"),
        Some("0xff"),
        Some("0x100"),
        Some("0xde0b6b3a7640000"),
        Some("0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        Some("0x8000000000000000000000000000000000000000000000000000000000000000"),
        Some("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
        None,
    ];
    let values: Vec<_> = texts.iter().map(|s| s.map(unsigned_be_fixture)).collect();
    let a = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        values.iter().map(|v| v.as_ref().map(|v| v.as_slice())),
        32,
    )
    .unwrap();
    let decimal_field = legacy_u256_field("value", true);
    let binary_field = Field::new("blob", DataType::FixedSizeBinary(32), true);
    let st = StructArray::new(
        vec![decimal_field.clone(), binary_field.clone()].into(),
        vec![Arc::new(a.clone()), Arc::new(a.clone())],
        None,
    );
    let list = ListArray::new(
        Arc::new(Field::new("item", st.data_type().clone(), false)),
        OffsetBuffer::new((0..=a.len() as i32).collect::<Vec<_>>().into()),
        Arc::new(st.clone()),
        None,
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            decimal_field,
            binary_field,
            Field::new("nested", st.data_type().clone(), false),
            Field::new("items", list.data_type().clone(), false),
        ])),
        vec![
            Arc::new(a.clone()),
            Arc::new(a),
            Arc::new(st),
            Arc::new(list),
        ],
    )
    .unwrap()
}
fn value_unwrap(v: &Value) -> &Value {
    if let Value::Union(_, v) = v {
        value_unwrap(v)
    } else {
        v
    }
}
fn record_field<'a>(v: &'a Value, key: &str) -> &'a Value {
    let Value::Record(fs) = value_unwrap(v) else {
        panic!("expected record {v:?}")
    };
    value_unwrap(&fs.iter().find(|(k, _)| k == key).unwrap().1)
}

#[test]
fn v3_legacy_u256_avro_bytes_contract() {
    let input = input();
    let source = input
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let converter = FromArrowToAvroConverter::new(input.schema(), "LegacyPluginV3".into());
    let schema = to_avro("LegacyPluginV3", &input.schema().fields);
    let wrapped = post_process_avro_schema_for_writing(schema.clone(), None);
    let schema_json = serde_json::to_string(&schema).unwrap();
    // Record the existing wire contract explicitly: it is bytes, not an Avro numeric logical type.
    assert!(!schema_json.contains("decimal"));
    assert!(!schema_json.contains("streamling.u256"));
    let mut decoder = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    decoder
        .register_writer_schema(7, &serde_json::to_string(&wrapped).unwrap())
        .unwrap();
    for (row, value) in converter
        .convert_from_batch(&input)
        .unwrap()
        .into_iter()
        .enumerate()
    {
        let wire = to_avro_datum(&wrapped, Value::Union(1, Box::new(value))).unwrap();
        let decoded = from_avro_datum(&wrapped, &mut wire.as_slice(), None).unwrap();
        let check = |v: &Value| {
            if source.is_null(row) {
                assert_eq!(v, &Value::Null);
            } else {
                let Value::Bytes(b) = v else {
                    panic!("expected bytes {v:?}")
                };
                assert_eq!(b, source.value(row));
            }
        };
        check(record_field(&decoded, "value"));
        check(record_field(&decoded, "blob"));
        check(record_field(record_field(&decoded, "nested"), "value"));
        check(record_field(record_field(&decoded, "nested"), "blob"));
        let Value::Array(items) = record_field(&decoded, "items") else {
            panic!()
        };
        assert_eq!(items.len(), 1);
        check(record_field(&items[0], "value"));
        check(record_field(&items[0], "blob"));
        let mut frame = vec![0];
        frame.extend(7u32.to_be_bytes());
        frame.extend(wire);
        decoder.decode(&frame).unwrap();
    }
    let decoded = decoder.flush().unwrap().unwrap();
    assert_eq!(decoded.num_rows(), input.num_rows());
    assert_eq!(decoded.schema().field(0).data_type(), &DataType::Binary);
    assert!(decoded.schema().field(0).metadata().is_empty());
    let bytes = decoded
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    for row in 0..input.num_rows() {
        let check_array = |array: &ArrayRef, index: usize| {
            let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(a.is_null(index), source.is_null(row));
            if !source.is_null(row) {
                assert_eq!(a.value(index), source.value(row));
            }
        };
        check_array(decoded.column(0), row);
        check_array(decoded.column(1), row);
        let nested = decoded
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        check_array(nested.column(0), row);
        check_array(nested.column(1), row);
        let list = decoded
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(row);
        let entry = list.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(entry.len(), 1);
        check_array(entry.column(0), 0);
        check_array(entry.column(1), 0);
        assert_eq!(bytes.is_null(row), source.is_null(row));
        if !source.is_null(row) {
            assert_eq!(bytes.value(row), source.value(row));
            eprintln!(
                "PLUGIN U256 wire preserves unsigned bytes {}",
                BigUint::from_bytes_be(bytes.value(row))
            );
        }
    }
    eprintln!(
        "PLUGIN U256 contract: 9 rows × 6 leaves passed real Avro wire and framed Arrow reader; both tagged U256 and ordinary FSB32 emit bytes without numeric metadata"
    );
}

#[test]
fn v3_legacy_u256_avro_slice_matrix() {
    let source = input();
    let mut cases = 0;
    for offset in 0..source.num_rows() {
        for len in 1..=source.num_rows() - offset {
            let b = source.slice(offset, len);
            let s = to_avro("SliceV3", &b.schema().fields);
            let converter = FromArrowToAvroConverter::new(b.schema(), "SliceV3".into());
            let values = converter.convert_from_batch(&b).unwrap();
            assert_eq!(values.len(), len);
            for (i, value) in values.into_iter().enumerate() {
                let wire = to_avro_datum(&s, value).unwrap();
                let actual = from_avro_datum(&s, &mut wire.as_slice(), None).unwrap();
                let expected = source
                    .column(0)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                let val = record_field(&actual, "value");
                if expected.is_null(offset + i) {
                    assert_eq!(val, &Value::Null);
                } else {
                    assert_eq!(val, &Value::Bytes(expected.value(offset + i).to_vec()));
                }
            }
            cases += 1;
        }
    }
    eprintln!("PLUGIN U256 slice matrix {cases} slices, ordinary and nested schema present");
    assert_eq!(cases, 45);
}

#[test]
fn v3_companion_canton_native_decimal256_signed_avro() {
    // Exact parser and field shape used by companion src/canton/schema.rs:42–51,58–61.
    // Canton does not emit the retired streamling.i256 extension.
    let texts = [
        "-0.0030234306",
        "0.0000000001",
        "1234567890123456789012345678.1234567890",
        "-1234567890123456789012345678.1234567890",
        "0",
        "0.0100000000",
    ];
    let values = texts
        .iter()
        .map(|v| parse_decimal::<Decimal256Type>(v, 76, 10).unwrap())
        .collect::<Vec<_>>();
    let a = Decimal256Array::from(values.clone())
        .with_precision_and_scale(76, 10)
        .unwrap();
    for nested in [false, true] {
        let f = Field::new("amount", DataType::Decimal256(76, 10), false);
        let (field, array): (Field, ArrayRef) = if nested {
            let st = StructArray::new(vec![f].into(), vec![Arc::new(a.clone())], None);
            (
                Field::new("transfer_fee", st.data_type().clone(), false),
                Arc::new(st),
            )
        } else {
            (f, Arc::new(a.clone()))
        };
        let b = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
        let s = to_avro("CantonV3", &b.schema().fields);
        let converter = FromArrowToAvroConverter::new(b.schema(), "CantonV3".into());
        let mut reader = ConfluentAvroDecoder::new().with_reader_schema(&s).unwrap();
        reader
            .register_writer_schema(12, &serde_json::to_string(&s).unwrap())
            .unwrap();
        for (i, value) in converter
            .convert_from_batch(&b)
            .unwrap()
            .into_iter()
            .enumerate()
        {
            let encoded = to_avro_datum(&s, value);
            eprintln!(
                "CANTON native nested={nested} input={} encoded={}",
                texts[i],
                encoded.is_ok()
            );
            let wire = encoded.unwrap();
            let decoded = from_avro_datum(&s, &mut wire.as_slice(), None).unwrap();
            let parent = if nested {
                record_field(&decoded, "transfer_fee")
            } else {
                &decoded
            };
            let Value::Decimal(d) = record_field(parent, "amount") else {
                panic!()
            };
            let bytes: Vec<u8> = d.try_into().unwrap();
            assert_eq!(
                num_bigint::BigInt::from_signed_bytes_be(&bytes),
                num_bigint::BigInt::from_signed_bytes_be(&values[i].to_be_bytes())
            );
            let mut frame = vec![0];
            frame.extend(12u32.to_be_bytes());
            frame.extend(wire);
            reader.decode(&frame).unwrap();
        }
        let decoded = reader.flush().unwrap().unwrap();
        let array = if nested {
            decoded
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0)
        } else {
            decoded.column(0)
        };
        let array = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
        assert_eq!(array.values().as_ref(), values);
        assert_eq!(array.precision(), 76);
        assert_eq!(array.scale(), 10);
        eprintln!(
            "CANTON framed native reader nested={nested}: all6 signed/scaled values and declared Decimal256(76,10) preserved"
        );
    }
}
