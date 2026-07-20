//! Adversarial integration tests for record-name aliasing, reader/target derivation,
//! `coerce_batch_to_target` edges, and empty-batch / flush-reuse semantics.
//!
//! Area owner: PR #60 hardening (arrow-avro drop-in). Every assertion encodes the CORRECT
//! contract derived from the source under test + the vendored oracle, not merely what the
//! code happens to do today. A failure here is a real decode/coercion bug.

use apache_avro::Decimal;
use apache_avro::types::{Record, Value};
use apache_avro::{Schema as AvroWriterSchema, to_avro_datum};
use arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int32Array, Int64Array, LargeBinaryArray,
    ListArray, PrimitiveArray, StringArray, StructArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{Decimal128Type, Decimal256Type, i256 as ArrowI256};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;

use streamling_common::formats::avro::arrow_avro::{
    AVRO_DECIMAL_SCALE_META, ConfluentAvroDecoder, coerce_batch_to_target, i256_be_bytes,
    rewrite_writer_schema, u256_be_bytes,
};
use streamling_common::formats::avro::convert_avro_schema_to_arrow;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn confluent_frame(id: u32, body: &[u8]) -> Vec<u8> {
    let mut f = vec![0x00];
    f.extend_from_slice(&id.to_be_bytes());
    f.extend_from_slice(body);
    f
}

fn parse(json: &str) -> AvroWriterSchema {
    AvroWriterSchema::parse_str(json).unwrap()
}

fn target_of(json: &str) -> SchemaRef {
    convert_avro_schema_to_arrow(parse(json))
}

/// The `ARROW:extension:name` metadata streamling stamps on u256/i256 FixedSizeBinary(32) fields.
fn ext_meta(name: &str) -> HashMap<String, String> {
    HashMap::from([("ARROW:extension:name".to_string(), name.to_string())])
}

fn schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

fn batch(fields: Vec<Field>, cols: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(schema(fields), cols).unwrap()
}

fn bin(rows: &[&[u8]]) -> ArrayRef {
    Arc::new(BinaryArray::from_iter_values(rows.iter().copied()))
}

fn large_bin(rows: &[&[u8]]) -> ArrayRef {
    Arc::new(LargeBinaryArray::from_iter_values(rows.iter().copied()))
}

fn i64s(vals: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}

fn i32s(vals: &[i32]) -> ArrayRef {
    Arc::new(Int32Array::from(vals.to_vec()))
}

fn fsb32(a: &ArrayRef) -> &FixedSizeBinaryArray {
    a.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap()
}

// Common schemas used across aliasing / evolution tests.
const DEC100_0: &str = r#"{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}]}"#;

const READER_RENAME: &str = r#"{"type":"record","name":"ReaderRec","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":7}]}"#;
const WRITER_A: &str = r#"{"type":"record","name":"WriterA","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"}]}"#;
const WRITER_B: &str = r#"{"type":"record","name":"WriterB","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"}]}"#;

// ===========================================================================
// GROUP 1: coerce_batch_to_target — missing columns, ordering, extras, empties
// ===========================================================================

#[test]
fn coerce_fills_missing_nullable_top_level_column_with_nulls() {
    let target = schema(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]);
    let src = batch(
        vec![Field::new("a", DataType::Int64, false)],
        vec![i64s(&[1, 2, 3])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(out.schema(), target, "output schema is exactly the target");
    assert_eq!(out.num_rows(), 3);
    assert_eq!(
        out.column(1).null_count(),
        3,
        "missing nullable column is all-null"
    );
    let a = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(a.values(), &[1, 2, 3], "present column preserved");
}

#[test]
fn coerce_missing_required_top_level_column_errors() {
    let target = schema(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("req", DataType::Utf8, false),
    ]);
    let src = batch(
        vec![Field::new("a", DataType::Int64, false)],
        vec![i64s(&[1, 2])],
    );
    assert!(
        coerce_batch_to_target(&src, &target).is_err(),
        "a missing REQUIRED target column must error, not silently null-fill"
    );
}

#[test]
fn coerce_missing_required_when_only_field_absent_errors() {
    let target = schema(vec![Field::new("req", DataType::Int64, false)]);
    let src = batch(
        vec![Field::new("other", DataType::Int64, false)],
        vec![i64s(&[9])],
    );
    assert!(coerce_batch_to_target(&src, &target).is_err());
}

#[test]
fn coerce_reorders_columns_by_name() {
    // target order [b, a]; source order [a, b].
    let target = schema(vec![
        Field::new("b", DataType::Utf8, false),
        Field::new("a", DataType::Int64, false),
    ]);
    let src = batch(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ],
        vec![i64s(&[10]), Arc::new(StringArray::from(vec!["x"]))],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(out.schema().field(0).name(), "b");
    assert_eq!(out.schema().field(1).name(), "a");
    let a = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(a.value(0), 10, "column matched by name, not position");
}

#[test]
fn coerce_drops_source_columns_absent_from_target() {
    let target = schema(vec![Field::new("keep", DataType::Int64, false)]);
    let src = batch(
        vec![
            Field::new("keep", DataType::Int64, false),
            Field::new("drop_me", DataType::Int64, false),
        ],
        vec![i64s(&[1]), i64s(&[999])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(
        out.num_columns(),
        1,
        "target drives the column set; extras dropped"
    );
    assert_eq!(out.schema().field(0).name(), "keep");
}

#[test]
fn coerce_multiple_missing_nullable_columns_all_null() {
    let target = schema(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Int32, true),
    ]);
    let src = batch(
        vec![Field::new("a", DataType::Int64, false)],
        vec![i64s(&[1, 2])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(out.column(1).null_count(), 2);
    assert_eq!(out.column(2).null_count(), 2);
    assert_eq!(out.column(2).len(), 2);
}

#[test]
fn coerce_required_present_but_another_required_missing_errors() {
    let target = schema(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, false),
    ]);
    let src = batch(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, true),
        ],
        vec![i64s(&[1]), i64s(&[2])],
    );
    assert!(
        coerce_batch_to_target(&src, &target).is_err(),
        "missing required c errors"
    );
}

#[test]
fn coerce_empty_batch_preserves_zero_rows() {
    let target = schema(vec![Field::new("a", DataType::Int64, false)]);
    let src = batch(
        vec![Field::new("a", DataType::Int64, false)],
        vec![i64s(&[])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(out.num_rows(), 0);
    assert_eq!(out.schema(), target);
}

#[test]
fn coerce_empty_batch_missing_nullable_column_is_zero_length_null() {
    let target = schema(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]);
    let src = batch(
        vec![Field::new("a", DataType::Int64, false)],
        vec![i64s(&[])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(out.num_rows(), 0);
    assert_eq!(
        out.column(1).len(),
        0,
        "null-fill respects the (zero) row count"
    );
    assert_eq!(out.column(1).null_count(), 0);
}

#[test]
fn coerce_empty_batch_missing_required_column_still_errors() {
    let target = schema(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("req", DataType::Utf8, false),
    ]);
    let src = batch(
        vec![Field::new("a", DataType::Int64, false)],
        vec![i64s(&[])],
    );
    assert!(
        coerce_batch_to_target(&src, &target).is_err(),
        "missing required errors even for an empty batch"
    );
}

#[test]
fn coerce_identity_schema_roundtrips() {
    let target = schema(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]);
    let src = batch(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ],
        vec![i64s(&[5, 6]), Arc::new(StringArray::from(vec!["p", "q"]))],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let a = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(a.values(), &[5, 6]);
    let s = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(s.value(1), "q");
}

// ===========================================================================
// GROUP 2: coerce leaf conversions — u256 / i256 / decimal / scaled-string / cast
// ===========================================================================

#[test]
fn coerce_u256_positive_from_binary() {
    let target = target_of(DEC100_0); // field "v" => FixedSizeBinary(32) + u256 metadata
    for input in [
        vec![0x00u8],
        vec![0x01],
        vec![0x7F],
        vec![0x00, 0xFF],
        vec![0x12, 0x34],
        vec![0x7F, 0xFF, 0xFF, 0xFF],
    ] {
        let src = batch(
            vec![Field::new("v", DataType::Binary, false)],
            vec![bin(&[&input])],
        );
        let out = coerce_batch_to_target(&src, &target)
            .unwrap_or_else(|e| panic!("u256 coerce of {input:?} failed: {e:?}"));
        let col = fsb32(out.column(0));
        assert_eq!(col.value(0).len(), 32, "u256 is FixedSizeBinary(32)");
        assert_eq!(
            col.value(0),
            u256_be_bytes(&input).unwrap().as_slice(),
            "u256 bytes must be big-endian zero-extended for {input:?}"
        );
    }
}

#[test]
fn coerce_u256_negative_rejected() {
    let target = target_of(DEC100_0);
    for input in [vec![0x80u8], vec![0xFF], vec![0x80, 0x00], vec![0xFF, 0xFF]] {
        let src = batch(
            vec![Field::new("v", DataType::Binary, false)],
            vec![bin(&[&input])],
        );
        assert!(
            coerce_batch_to_target(&src, &target).is_err(),
            "negative decimal {input:?} must error for a u256 target, not wrap"
        );
    }
}

#[test]
fn coerce_u256_zero_is_all_zero_bytes() {
    let target = target_of(DEC100_0);
    let src = batch(
        vec![Field::new("v", DataType::Binary, false)],
        vec![bin(&[&[0x00]])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(fsb32(out.column(0)).value(0), &[0u8; 32]);
}

#[test]
fn coerce_u256_from_large_binary_source() {
    let target = target_of(DEC100_0);
    let src = batch(
        vec![Field::new("v", DataType::LargeBinary, false)],
        vec![large_bin(&[&[0x01, 0x00]])], // 256
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(
        fsb32(out.column(0)).value(0),
        u256_be_bytes(&[0x01, 0x00]).unwrap().as_slice(),
        "u256 coercion must accept LargeBinary source too"
    );
}

#[test]
fn coerce_u256_full_32_byte_value() {
    let target = target_of(DEC100_0);
    let mut input = vec![0u8; 32];
    input[0] = 0x12;
    input[31] = 0xCD;
    let src = batch(
        vec![Field::new("v", DataType::Binary, false)],
        vec![bin(&[&input])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(fsb32(out.column(0)).value(0), input.as_slice());
}

#[test]
fn coerce_u256_nullable_null_passes_through() {
    // Nullable u256 target (from a ["null", decimal] union) with a null input row.
    const NULLABLE_U256: &str = r#"{"type":"record","name":"R","fields":[{"name":"v","type":["null",{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}]}]}"#;
    let target = target_of(NULLABLE_U256);
    assert!(target.field(0).is_nullable());
    let col: ArrayRef = Arc::new(BinaryArray::from_iter(vec![Some(vec![0x01u8]), None]));
    let src = batch(vec![Field::new("v", DataType::Binary, true)], vec![col]);
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let c = fsb32(out.column(0));
    assert!(!c.is_null(0));
    assert!(
        c.is_null(1),
        "null decimal row stays null through u256 coercion"
    );
}

fn i256_target() -> SchemaRef {
    schema(vec![
        Field::new("v", DataType::FixedSizeBinary(32), false)
            .with_metadata(ext_meta("streamling.i256")),
    ])
}

#[test]
fn coerce_i256_positive_from_binary() {
    let target = i256_target();
    for input in [vec![0x01u8], vec![0x00], vec![0x7F, 0xFF], vec![0x00, 0x80]] {
        let src = batch(
            vec![Field::new("v", DataType::Binary, false)],
            vec![bin(&[&input])],
        );
        let out = coerce_batch_to_target(&src, &target).unwrap();
        assert_eq!(
            fsb32(out.column(0)).value(0),
            i256_be_bytes(&input).unwrap().as_slice(),
            "i256 positive {input:?}"
        );
    }
}

#[test]
fn coerce_i256_negative_sign_extends() {
    let target = i256_target();
    let src = batch(
        vec![Field::new("v", DataType::Binary, false)],
        vec![bin(&[&[0xFF]])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(
        fsb32(out.column(0)).value(0),
        &[0xFFu8; 32],
        "i256 of 0xFF (=-1) sign-extends to all-0xFF (unlike u256 which rejects it)"
    );
}

#[test]
fn coerce_i256_negative_two_byte() {
    let target = i256_target();
    // 0x80,0x00 = -32768, sign bit set.
    let src = batch(
        vec![Field::new("v", DataType::Binary, false)],
        vec![bin(&[&[0x80, 0x00]])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(
        fsb32(out.column(0)).value(0),
        i256_be_bytes(&[0x80, 0x00]).unwrap().as_slice()
    );
}

#[test]
fn coerce_i256_zero() {
    let target = i256_target();
    let src = batch(
        vec![Field::new("v", DataType::Binary, false)],
        vec![bin(&[&[0x00]])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(fsb32(out.column(0)).value(0), &[0u8; 32]);
}

fn dec128_target(p: u8, s: i8) -> SchemaRef {
    schema(vec![Field::new("d", DataType::Decimal128(p, s), false)])
}

#[test]
fn coerce_decimal128_from_binary_twos_complement() {
    let cases: &[(&[u8], i128)] = &[
        (&[0x00], 0),
        (&[0x01], 1),
        (&[0x7F], 127),
        (&[0xFF], -1),
        (&[0x80], -128),
        (&[0x04, 0xD2], 1234),
        (&[0x01, 0x00], 256),
    ];
    let target = dec128_target(20, 0);
    for (bytes, expected) in cases {
        let src = batch(
            vec![Field::new("d", DataType::Binary, false)],
            vec![bin(&[bytes])],
        );
        let out = coerce_batch_to_target(&src, &target).unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Decimal128Type>>()
            .unwrap();
        assert_eq!(col.value(0), *expected, "decimal128 from {bytes:?}");
        assert_eq!(col.data_type(), &DataType::Decimal128(20, 0));
    }
}

#[test]
fn coerce_decimal128_carries_scale_metadata_in_type() {
    // The raw unscaled integer is stored verbatim; only the Decimal128(p,s) type label carries scale.
    let target = dec128_target(10, 2);
    let src = batch(
        vec![Field::new("d", DataType::Binary, false)],
        vec![bin(&[&[0x04, 0xD2]])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<PrimitiveArray<Decimal128Type>>()
        .unwrap();
    assert_eq!(col.value(0), 1234, "unscaled integer preserved");
    assert_eq!(col.data_type(), &DataType::Decimal128(10, 2));
}

#[test]
fn coerce_decimal128_passthrough_relabels_precision_scale() {
    // Source is already Decimal128 (arrow-avro decoded a <=38 precision decimal natively);
    // coercion must relabel to the target p/s WITHOUT rescaling the stored integer.
    let src_arr: ArrayRef = Arc::new(
        PrimitiveArray::<Decimal128Type>::from_iter_values([1234i128])
            .with_data_type(DataType::Decimal128(10, 2)),
    );
    let src = batch(
        vec![Field::new("d", DataType::Decimal128(10, 2), false)],
        vec![src_arr],
    );
    let target = dec128_target(20, 2);
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<PrimitiveArray<Decimal128Type>>()
        .unwrap();
    assert_eq!(col.value(0), 1234, "stored integer unchanged on relabel");
    assert_eq!(col.data_type(), &DataType::Decimal128(20, 2));
}

#[test]
fn coerce_decimal256_from_binary() {
    let target = schema(vec![Field::new("d", DataType::Decimal256(50, 0), false)]);
    let cases: &[(&[u8], ArrowI256)] = &[
        (&[0x00], ArrowI256::from_i128(0)),
        (&[0x01], ArrowI256::from_i128(1)),
        (&[0xFF], ArrowI256::from_i128(-1)),
        (&[0x04, 0xD2], ArrowI256::from_i128(1234)),
    ];
    for (bytes, expected) in cases {
        let src = batch(
            vec![Field::new("d", DataType::Binary, false)],
            vec![bin(&[bytes])],
        );
        let out = coerce_batch_to_target(&src, &target).unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<PrimitiveArray<Decimal256Type>>()
            .unwrap();
        assert_eq!(col.value(0), *expected, "decimal256 from {bytes:?}");
    }
}

const SCALED_85_4: &str = r#"{"type":"record","name":"R","fields":[{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":85,"scale":4}}]}"#;

#[test]
fn coerce_scaled_high_precision_decimal_to_string() {
    let target = target_of(SCALED_85_4);
    assert_eq!(target.field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        target
            .field(0)
            .metadata()
            .get(AVRO_DECIMAL_SCALE_META)
            .map(String::as_str),
        Some("4")
    );
    // (bytes, expected string) — unscaled BE two's-complement, scale 4, trailing zeros trimmed.
    let cases: &[(&[u8], &str)] = &[
        (&[0x12, 0xD6, 0x87], "123.4567"), // 1234567
        (&[0x00], "0"),                    // 0
        (&[0xFF], "-0.0001"),              // -1
        (&[0x27, 0x10], "1"),              // 10000 -> 1.0000 -> 1
        (&[0x03, 0xE8], "0.1"),            // 1000 -> 0.1000 -> 0.1
    ];
    for (bytes, expected) in cases {
        let src = batch(
            vec![Field::new("amt", DataType::Binary, false)],
            vec![bin(&[bytes])],
        );
        let out = coerce_batch_to_target(&src, &target).unwrap();
        let col = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            col.value(0),
            *expected,
            "scaled decimal string for {bytes:?}"
        );
    }
}

#[test]
fn coerce_scaled_decimal_string_nullable_null_row() {
    const NULLABLE_SCALED: &str = r#"{"type":"record","name":"R","fields":[{"name":"amt","type":["null",{"type":"bytes","logicalType":"decimal","precision":85,"scale":4}]}]}"#;
    let target = target_of(NULLABLE_SCALED);
    assert!(target.field(0).is_nullable());
    let col: ArrayRef = Arc::new(BinaryArray::from_iter(vec![Some(vec![0x01u8]), None]));
    let src = batch(vec![Field::new("amt", DataType::Binary, true)], vec![col]);
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let s = out
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(s.value(0), "0.0001");
    assert!(
        s.is_null(1),
        "null decimal row stays null, not empty string"
    );
}

#[test]
fn coerce_plain_utf8_without_scale_meta_passes_through() {
    // A plain string field (no AVRO_DECIMAL_SCALE_META) must NOT go through decimal formatting.
    let target = schema(vec![Field::new("s", DataType::Utf8, false)]);
    let src = batch(
        vec![Field::new("s", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["hello", "world"]))],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let s = out
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(s.value(0), "hello");
    assert_eq!(s.value(1), "world");
}

#[test]
fn coerce_plain_cast_int32_to_int64() {
    let target = schema(vec![Field::new("n", DataType::Int64, false)]);
    let src = batch(
        vec![Field::new("n", DataType::Int32, false)],
        vec![i32s(&[7, -3, 0])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let n = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(n.values(), &[7, -3, 0], "widening cast preserves value");
    assert_eq!(n.data_type(), &DataType::Int64);
}

#[test]
fn coerce_identical_primitive_passes_through() {
    let target = schema(vec![Field::new("n", DataType::Int64, false)]);
    let src = batch(
        vec![Field::new("n", DataType::Int64, false)],
        vec![i64s(&[42])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let n = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(n.value(0), 42);
}

// ===========================================================================
// GROUP 3: coerce_list / coerce_struct (direct)
// ===========================================================================

fn list_i32(offsets: Vec<i32>, values: Vec<i32>) -> (ArrayRef, Field) {
    let elem = Arc::new(Field::new("element", DataType::Int32, true));
    let arr = ListArray::try_new(
        elem.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(Int32Array::from(values)),
        None,
    )
    .unwrap();
    let field = Field::new("l", arr.data_type().clone(), false);
    (Arc::new(arr), field)
}

#[test]
fn coerce_list_casts_element_type() {
    let (arr, src_field) = list_i32(vec![0, 2, 3], vec![10, 20, 30]);
    let src = batch(vec![src_field], vec![arr]);
    let target = schema(vec![Field::new(
        "l",
        DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
        false,
    )]);
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let list = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.len(), 2, "two lists preserved");
    let v0 = list.value(0);
    let v0 = v0.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(v0.values(), &[10, 20], "elements widened to Int64");
    let v1 = list.value(1);
    let v1 = v1.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(v1.values(), &[30]);
}

#[test]
fn coerce_list_preserves_offsets_and_empty_sublist() {
    // lists: [10,20], [], [30]
    let (arr, src_field) = list_i32(vec![0, 2, 2, 3], vec![10, 20, 30]);
    let src = batch(vec![src_field], vec![arr]);
    let target = schema(vec![Field::new(
        "l",
        DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
        false,
    )]);
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let list = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.len(), 3);
    assert_eq!(list.value(1).len(), 0, "empty sublist stays empty");
    assert_eq!(list.value(2).len(), 1);
}

#[test]
fn coerce_list_identical_element_passthrough() {
    let elem = Arc::new(Field::new("element", DataType::Int64, true));
    let arr = ListArray::try_new(
        elem.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2])),
        i64s(&[1, 2]),
        None,
    )
    .unwrap();
    let src = batch(
        vec![Field::new("l", arr.data_type().clone(), false)],
        vec![Arc::new(arr)],
    );
    let target = schema(vec![Field::new("l", DataType::List(elem), false)]);
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let list = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let v = list.value(0);
    assert_eq!(
        v.as_any().downcast_ref::<Int64Array>().unwrap().values(),
        &[1, 2]
    );
}

fn struct_with(fields: Vec<Field>, cols: Vec<ArrayRef>) -> (ArrayRef, Field) {
    let arr = StructArray::new(Fields::from(fields), cols, None);
    let f = Field::new("s", arr.data_type().clone(), false);
    (Arc::new(arr), f)
}

#[test]
fn coerce_struct_casts_child_type() {
    let (arr, src_field) = struct_with(
        vec![Field::new("n", DataType::Int32, false)],
        vec![i32s(&[1, 2])],
    );
    let src = batch(vec![src_field], vec![arr]);
    let target = schema(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![Field::new("n", DataType::Int64, false)])),
        false,
    )]);
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let st = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let n = st
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(n.values(), &[1, 2]);
}

#[test]
fn coerce_struct_fills_missing_nullable_child() {
    let (arr, src_field) = struct_with(
        vec![Field::new("a", DataType::Int64, false)],
        vec![i64s(&[1, 2, 3])],
    );
    let src = batch(vec![src_field], vec![arr]);
    let target = schema(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ])),
        false,
    )]);
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let st = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(st.column_by_name("b").unwrap().null_count(), 3);
}

#[test]
fn coerce_struct_missing_required_child_errors() {
    let (arr, src_field) = struct_with(
        vec![Field::new("a", DataType::Int64, false)],
        vec![i64s(&[1])],
    );
    let src = batch(vec![src_field], vec![arr]);
    let target = schema(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false),
        ])),
        false,
    )]);
    assert!(
        coerce_batch_to_target(&src, &target).is_err(),
        "missing required nested field must error"
    );
}

#[test]
fn coerce_struct_reorders_and_drops_children() {
    let (arr, src_field) = struct_with(
        vec![
            Field::new("a", DataType::Int64, false),
            Field::new("extra", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ],
        vec![
            i64s(&[1]),
            i64s(&[99]),
            Arc::new(StringArray::from(vec!["x"])),
        ],
    );
    let src = batch(vec![src_field], vec![arr]);
    let target = schema(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![
            Field::new("b", DataType::Utf8, false),
            Field::new("a", DataType::Int64, false),
        ])),
        false,
    )]);
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let st = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(st.num_columns(), 2, "extra child dropped");
    assert_eq!(
        st.column(0).data_type(),
        &DataType::Utf8,
        "reordered: b first"
    );
}

#[test]
fn coerce_list_of_struct_nested() {
    // List<Struct{n: Int32}> -> List<Struct{n: Int64}>
    let inner = StructArray::new(
        Fields::from(vec![Field::new("n", DataType::Int32, false)]),
        vec![i32s(&[1, 2, 3])],
        None,
    );
    let elem = Arc::new(Field::new("element", inner.data_type().clone(), true));
    let list = ListArray::try_new(
        elem,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 3])),
        Arc::new(inner),
        None,
    )
    .unwrap();
    let src = batch(
        vec![Field::new("l", list.data_type().clone(), false)],
        vec![Arc::new(list)],
    );
    let target = schema(vec![Field::new(
        "l",
        DataType::List(Arc::new(Field::new(
            "element",
            DataType::Struct(Fields::from(vec![Field::new("n", DataType::Int64, false)])),
            true,
        ))),
        false,
    )]);
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let list = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let first = list.value(0);
    let st = first.as_any().downcast_ref::<StructArray>().unwrap();
    let n = st
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(n.values(), &[1, 2]);
}

// ===========================================================================
// GROUP 4: record-name aliasing
// ===========================================================================

#[test]
fn alias_writer_name_differs_from_reader_decodes() {
    let reader = parse(READER_RENAME);
    let writer = parse(WRITER_A);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(10, WRITER_A).unwrap();
    for i in 1..=4i64 {
        let mut rec = Record::new(&writer).unwrap();
        rec.put("id", Value::Long(i));
        rec.put("data", Value::String(format!("row{i}")));
        d.decode(&confluent_frame(10, &to_avro_datum(&writer, rec).unwrap()))
            .expect("differently-named writer must decode via top-level alias");
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 4);
    assert_eq!(
        b.num_columns(),
        3,
        "target = reader schema (id, data, version)"
    );
}

#[test]
fn alias_fills_reader_only_default_field() {
    let reader = parse(READER_RENAME);
    let writer = parse(WRITER_A);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(10, WRITER_A).unwrap();
    let mut rec = Record::new(&writer).unwrap();
    rec.put("id", Value::Long(1));
    rec.put("data", Value::String("x".into()));
    d.decode(&confluent_frame(10, &to_avro_datum(&writer, rec).unwrap()))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    let v = b
        .column(b.schema().index_of("version").unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(
        v.value(0),
        7,
        "reader-only field resolved to its default under aliasing"
    );
}

#[test]
fn alias_same_name_fast_path_decodes() {
    const SAME: &str = r#"{"type":"record","name":"Same","fields":[{"name":"id","type":"long"}]}"#;
    let s = parse(SAME);
    let mut d = ConfluentAvroDecoder::new().with_reader_schema(&s).unwrap();
    d.register_writer_schema(1, SAME).unwrap();
    let mut rec = Record::new(&s).unwrap();
    rec.put("id", Value::Long(55));
    d.decode(&confluent_frame(1, &to_avro_datum(&s, rec).unwrap()))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    let id = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.value(0), 55);
}

#[test]
fn alias_two_differently_named_writers_both_decode() {
    let reader = parse(READER_RENAME);
    let wa = parse(WRITER_A);
    let wb = parse(WRITER_B);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(10, WRITER_A).unwrap();
    d.register_writer_schema(20, WRITER_B).unwrap();

    let mut ra = Record::new(&wa).unwrap();
    ra.put("id", Value::Long(1));
    ra.put("data", Value::String("a".into()));
    d.decode(&confluent_frame(10, &to_avro_datum(&wa, ra).unwrap()))
        .unwrap();

    let mut rb = Record::new(&wb).unwrap();
    rb.put("id", Value::Long(2));
    rb.put("data", Value::String("b".into()));
    d.decode(&confluent_frame(20, &to_avro_datum(&wb, rb).unwrap()))
        .unwrap();

    let b = d.flush().unwrap().expect("batch");
    assert_eq!(
        b.num_rows(),
        2,
        "both differently-named writers alias onto the reader"
    );
    let ids = b
        .column(b.schema().index_of("id").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut got: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
    got.sort();
    assert_eq!(got, vec![1, 2]);
}

#[test]
fn alias_mixed_same_and_different_names() {
    // reader "R"; writer1 "R" (fast path) id1; writer2 "Other" (alias) id2.
    const R: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;
    const OTHER: &str =
        r#"{"type":"record","name":"Other","fields":[{"name":"id","type":"long"}]}"#;
    let reader = parse(R);
    let w_same = parse(R);
    let w_other = parse(OTHER);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, R).unwrap();
    d.register_writer_schema(2, OTHER).unwrap();

    let mut r1 = Record::new(&w_same).unwrap();
    r1.put("id", Value::Long(100));
    d.decode(&confluent_frame(1, &to_avro_datum(&w_same, r1).unwrap()))
        .unwrap();
    let mut r2 = Record::new(&w_other).unwrap();
    r2.put("id", Value::Long(200));
    d.decode(&confluent_frame(2, &to_avro_datum(&w_other, r2).unwrap()))
        .unwrap();

    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 2);
}

#[test]
fn alias_namespaced_writer_name_differs() {
    // Both namespaced; full names differ -> alias is a dotted (fully-qualified) name and resolves.
    const READER: &str = r#"{"type":"record","name":"Rec","namespace":"com.x","fields":[{"name":"id","type":"long"}]}"#;
    const WRITER: &str = r#"{"type":"record","name":"Rec","namespace":"com.y","fields":[{"name":"id","type":"long"}]}"#;
    let reader = parse(READER);
    let writer = parse(WRITER);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, WRITER).unwrap();
    let mut rec = Record::new(&writer).unwrap();
    rec.put("id", Value::Long(9));
    d.decode(&confluent_frame(1, &to_avro_datum(&writer, rec).unwrap()))
        .expect("namespaced full-name alias must resolve");
    let b = d.flush().unwrap().expect("batch");
    let id = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.value(0), 9);
}

#[test]
fn alias_target_schema_comes_from_reader_not_writer() {
    let reader = parse(READER_RENAME);
    let d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    let target = d.target_schema().expect("target set from reader");
    assert_eq!(target.fields().len(), 3);
    assert_eq!(target.field(0).name(), "id");
    assert_eq!(target.field(0).data_type(), &DataType::Int64);
    assert_eq!(target.field(2).name(), "version");
    assert_eq!(target.field(2).data_type(), &DataType::Int32);
}

// ===========================================================================
// GROUP 5: nested named-type name mismatch -> must ERROR cleanly (documented limit)
// ===========================================================================

#[test]
fn nested_record_name_mismatch_errors_not_panics() {
    const READER: &str = r#"{"type":"record","name":"Root","fields":[{"name":"child","type":{"type":"record","name":"ChildReader","fields":[{"name":"a","type":"long"}]}}]}"#;
    const WRITER: &str = r#"{"type":"record","name":"Root","fields":[{"name":"child","type":{"type":"record","name":"ChildWriter","fields":[{"name":"a","type":"long"}]}}]}"#;
    let reader = parse(READER);
    let writer = parse(WRITER);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, WRITER).unwrap();
    let mut rec = Record::new(&writer).unwrap();
    rec.put(
        "child",
        Value::Record(vec![("a".to_string(), Value::Long(5))]),
    );
    let body = to_avro_datum(&writer, rec).unwrap();
    let res = d.decode(&confluent_frame(1, &body));
    assert!(
        res.is_err(),
        "top-level names match but nested record name differs -> arrow-avro name resolution must error cleanly"
    );
}

#[test]
fn nested_record_in_array_name_mismatch_errors() {
    const READER: &str = r#"{"type":"record","name":"Root","fields":[{"name":"xs","type":{"type":"array","items":{"type":"record","name":"ItemR","fields":[{"name":"a","type":"long"}]}}}]}"#;
    const WRITER: &str = r#"{"type":"record","name":"Root","fields":[{"name":"xs","type":{"type":"array","items":{"type":"record","name":"ItemW","fields":[{"name":"a","type":"long"}]}}}]}"#;
    let reader = parse(READER);
    let writer = parse(WRITER);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, WRITER).unwrap();
    let mut rec = Record::new(&writer).unwrap();
    rec.put(
        "xs",
        Value::Array(vec![Value::Record(vec![("a".to_string(), Value::Long(1))])]),
    );
    let body = to_avro_datum(&writer, rec).unwrap();
    assert!(
        d.decode(&confluent_frame(1, &body)).is_err(),
        "nested record-in-array name mismatch must error, not panic"
    );
}

#[test]
fn nested_record_in_union_name_mismatch_errors() {
    const READER: &str = r#"{"type":"record","name":"Root","fields":[{"name":"child","type":["null",{"type":"record","name":"ChildReader","fields":[{"name":"a","type":"long"}]}],"default":null}]}"#;
    const WRITER: &str = r#"{"type":"record","name":"Root","fields":[{"name":"child","type":["null",{"type":"record","name":"ChildWriter","fields":[{"name":"a","type":"long"}]}],"default":null}]}"#;
    let reader = parse(READER);
    let writer = parse(WRITER);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, WRITER).unwrap();
    let mut rec = Record::new(&writer).unwrap();
    rec.put(
        "child",
        Value::Union(
            1,
            Box::new(Value::Record(vec![("a".to_string(), Value::Long(3))])),
        ),
    );
    let body = to_avro_datum(&writer, rec).unwrap();
    assert!(
        d.decode(&confluent_frame(1, &body)).is_err(),
        "nested record-in-union name mismatch must error, not panic"
    );
}

#[test]
fn nested_record_mismatch_but_topmost_alias_still_errors() {
    // Top-level ALSO renamed (alias path engaged) AND nested renamed: aliasing only covers the
    // top level, so this must still error rather than silently succeed.
    const READER: &str = r#"{"type":"record","name":"OuterR","fields":[{"name":"child","type":{"type":"record","name":"ChildReader","fields":[{"name":"a","type":"long"}]}}]}"#;
    const WRITER: &str = r#"{"type":"record","name":"OuterW","fields":[{"name":"child","type":{"type":"record","name":"ChildWriter","fields":[{"name":"a","type":"long"}]}}]}"#;
    let reader = parse(READER);
    let writer = parse(WRITER);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, WRITER).unwrap();
    let mut rec = Record::new(&writer).unwrap();
    rec.put(
        "child",
        Value::Record(vec![("a".to_string(), Value::Long(5))]),
    );
    let body = to_avro_datum(&writer, rec).unwrap();
    assert!(d.decode(&confluent_frame(1, &body)).is_err());
}

// ===========================================================================
// GROUP 6: no reader schema -> target derived from first registered writer
// ===========================================================================

#[test]
fn no_schema_target_is_none() {
    let d = ConfluentAvroDecoder::new();
    assert!(
        d.target_schema().is_none(),
        "no reader and no writer => no target yet"
    );
}

#[test]
fn no_reader_target_derived_from_first_writer() {
    const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, W).unwrap();
    let target = d.target_schema().expect("target derived from first writer");
    assert_eq!(
        target,
        &target_of(W),
        "derived target matches convert_avro_schema_to_arrow(writer)"
    );
    assert_eq!(target.field(0).data_type(), &DataType::Int64);
    assert_eq!(target.field(1).data_type(), &DataType::Utf8);
}

#[test]
fn no_reader_decode_and_flush_works() {
    const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"id","type":"long"}]}"#;
    let w = parse(W);
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, W).unwrap();
    let mut rec = Record::new(&w).unwrap();
    rec.put("id", Value::Long(1234));
    d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    let id = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(
        id.value(0),
        1234,
        "decodes against writer with no reader set"
    );
}

#[test]
fn no_reader_target_frozen_by_first_writer() {
    const W1: &str = r#"{"type":"record","name":"W1","fields":[{"name":"id","type":"long"}]}"#;
    const W2: &str = r#"{"type":"record","name":"W2","fields":[{"name":"id","type":"long"},{"name":"extra","type":"string"}]}"#;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, W1).unwrap();
    d.register_writer_schema(2, W2).unwrap();
    let target = d.target_schema().unwrap();
    assert_eq!(
        target.fields().len(),
        1,
        "target frozen from FIRST writer, not re-derived on the 2nd"
    );
    assert_eq!(target.field(0).name(), "id");
}

#[test]
fn no_reader_u256_decode() {
    let w = parse(DEC100_0);
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, DEC100_0).unwrap();
    let mut payload = [0u8; 32];
    payload[31] = 0x2A;
    let mut rec = Record::new(&w).unwrap();
    rec.put("v", Value::Decimal(Decimal::from(payload.to_vec())));
    d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    let col = fsb32(b.column(0));
    assert_eq!(
        col.value(0),
        &payload,
        "u256 round-trips with no reader schema set"
    );
}

#[test]
fn has_writer_schema_reflects_registration() {
    const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"id","type":"long"}]}"#;
    let mut d = ConfluentAvroDecoder::new();
    assert!(!d.has_writer_schema(5));
    d.register_writer_schema(5, W).unwrap();
    assert!(d.has_writer_schema(5));
    assert!(!d.has_writer_schema(6));
}

// ===========================================================================
// GROUP 7: flush / empty batch / reuse across cycles
// ===========================================================================

#[test]
fn flush_with_schema_but_no_rows_is_none() {
    const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"id","type":"long"}]}"#;
    let w = parse(W);
    let mut d = ConfluentAvroDecoder::new().with_reader_schema(&w).unwrap();
    d.register_writer_schema(1, W).unwrap();
    assert!(
        d.flush().unwrap().is_none(),
        "no decode => flush yields Ok(None)"
    );
}

#[test]
fn flush_without_any_schema_errors() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(d.flush().is_err(), "flush with no target schema must error");
}

#[test]
fn flush_twice_second_is_none() {
    const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"id","type":"long"}]}"#;
    let w = parse(W);
    let mut d = ConfluentAvroDecoder::new().with_reader_schema(&w).unwrap();
    d.register_writer_schema(1, W).unwrap();
    let mut rec = Record::new(&w).unwrap();
    rec.put("id", Value::Long(1));
    d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
        .unwrap();
    assert_eq!(d.flush().unwrap().expect("batch").num_rows(), 1);
    assert!(
        d.flush().unwrap().is_none(),
        "second flush after drain yields None"
    );
}

#[test]
fn decode_flush_decode_flush_second_batch_independent() {
    const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"id","type":"long"}]}"#;
    let w = parse(W);
    let mut d = ConfluentAvroDecoder::new().with_reader_schema(&w).unwrap();
    d.register_writer_schema(1, W).unwrap();

    let mut r1 = Record::new(&w).unwrap();
    r1.put("id", Value::Long(11));
    d.decode(&confluent_frame(1, &to_avro_datum(&w, r1).unwrap()))
        .unwrap();
    let b1 = d.flush().unwrap().expect("batch1");
    assert_eq!(b1.num_rows(), 1);
    assert_eq!(
        b1.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        11
    );

    let mut r2 = Record::new(&w).unwrap();
    r2.put("id", Value::Long(22));
    d.decode(&confluent_frame(1, &to_avro_datum(&w, r2).unwrap()))
        .unwrap();
    let b2 = d.flush().unwrap().expect("batch2");
    assert_eq!(
        b2.num_rows(),
        1,
        "second batch does NOT carry the first batch's row"
    );
    assert_eq!(
        b2.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        22
    );
}

#[test]
fn multiple_decodes_one_flush_accumulate() {
    const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"id","type":"long"}]}"#;
    let w = parse(W);
    let mut d = ConfluentAvroDecoder::new().with_reader_schema(&w).unwrap();
    d.register_writer_schema(1, W).unwrap();
    for i in 1..=5i64 {
        let mut rec = Record::new(&w).unwrap();
        rec.put("id", Value::Long(i));
        d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 5);
    let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let sum: i64 = (0..ids.len()).map(|i| ids.value(i)).sum();
    assert_eq!(sum, 15);
}

#[test]
fn schema_registration_survives_flush() {
    const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"id","type":"long"}]}"#;
    let w = parse(W);
    let mut d = ConfluentAvroDecoder::new().with_reader_schema(&w).unwrap();
    d.register_writer_schema(1, W).unwrap();
    let mut rec = Record::new(&w).unwrap();
    rec.put("id", Value::Long(1));
    d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
        .unwrap();
    let _ = d.flush().unwrap();
    assert!(
        d.has_writer_schema(1),
        "registered writer schemas are retained across flush"
    );
    // and the decoder can be reused without re-registering
    let mut rec2 = Record::new(&w).unwrap();
    rec2.put("id", Value::Long(2));
    d.decode(&confluent_frame(1, &to_avro_datum(&w, rec2).unwrap()))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(
        b.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}

#[test]
fn flush_none_then_decode_then_some() {
    const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"id","type":"long"}]}"#;
    let w = parse(W);
    let mut d = ConfluentAvroDecoder::new().with_reader_schema(&w).unwrap();
    d.register_writer_schema(1, W).unwrap();
    assert!(d.flush().unwrap().is_none());
    let mut rec = Record::new(&w).unwrap();
    rec.put("id", Value::Long(7));
    d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
        .unwrap();
    assert_eq!(d.flush().unwrap().expect("batch").num_rows(), 1);
}

#[test]
fn many_flush_cycles_stay_independent() {
    const W: &str = r#"{"type":"record","name":"W","fields":[{"name":"id","type":"long"}]}"#;
    let w = parse(W);
    let mut d = ConfluentAvroDecoder::new().with_reader_schema(&w).unwrap();
    d.register_writer_schema(1, W).unwrap();
    for cycle in 0..10i64 {
        let mut rec = Record::new(&w).unwrap();
        rec.put("id", Value::Long(cycle));
        d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
            .unwrap();
        let b = d.flush().unwrap().expect("batch");
        assert_eq!(b.num_rows(), 1, "cycle {cycle} has exactly its own row");
        assert_eq!(
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            cycle
        );
    }
}

// ===========================================================================
// GROUP 8: skip_schema_resolution interactions with derivation/coercion
// ===========================================================================

#[test]
fn skip_resolution_different_name_still_decodes() {
    // With resolution off there is no reader-schema name check; coercion is by field name.
    const READER: &str = r#"{"type":"record","name":"ReaderRec","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"}]}"#;
    let reader = parse(READER);
    let writer = parse(WRITER_A);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap()
        .with_schema_resolution(false);
    d.register_writer_schema(10, WRITER_A).unwrap();
    let mut rec = Record::new(&writer).unwrap();
    rec.put("id", Value::Long(3));
    rec.put("data", Value::String("z".into()));
    d.decode(&confluent_frame(10, &to_avro_datum(&writer, rec).unwrap()))
        .expect("skip-resolution decodes against writer regardless of record name");
    let b = d.flush().unwrap().expect("batch");
    let id = b
        .column(b.schema().index_of("id").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.value(0), 3);
}

#[test]
fn skip_resolution_nullable_reader_only_field_is_null() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"extra","type":["int","null"],"default":42}]}"#;
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;
    let reader = parse(READER);
    let writer = parse(WRITER);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap()
        .with_schema_resolution(false);
    d.register_writer_schema(2, WRITER).unwrap();
    let mut rec = Record::new(&writer).unwrap();
    rec.put("id", Value::Long(1));
    d.decode(&confluent_frame(2, &to_avro_datum(&writer, rec).unwrap()))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    let extra = b
        .column(b.schema().index_of("extra").unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(
        extra.is_null(0),
        "skip-resolution must NOT fill reader defaults; nullable reader-only field is null"
    );
}

#[test]
fn skip_resolution_required_reader_only_field_flush_errors() {
    // Reader has a REQUIRED field the writer lacks; under skip-resolution the decoded batch omits
    // it and coerce_batch_to_target cannot null-fill a required column -> flush errors cleanly.
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"req","type":"int"}]}"#;
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;
    let reader = parse(READER);
    let writer = parse(WRITER);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap()
        .with_schema_resolution(false);
    d.register_writer_schema(2, WRITER).unwrap();
    let mut rec = Record::new(&writer).unwrap();
    rec.put("id", Value::Long(1));
    d.decode(&confluent_frame(2, &to_avro_datum(&writer, rec).unwrap()))
        .unwrap();
    assert!(
        d.flush().is_err(),
        "a required reader-only field missing under skip-resolution must surface as an error"
    );
}

// ===========================================================================
// GROUP 9: rewrite_writer_schema (public API) sanity
// ===========================================================================

#[test]
fn rewrite_strips_high_precision_decimal() {
    let json = rewrite_writer_schema(DEC100_0).unwrap();
    assert!(
        !json.contains("decimal"),
        "precision>76 decimal logicalType stripped: {json}"
    );
    assert!(json.contains("bytes"), "underlying bytes type retained");
}

#[test]
fn rewrite_keeps_low_precision_decimal() {
    const LOW: &str = r#"{"type":"record","name":"R","fields":[{"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":20,"scale":5}}]}"#;
    let json = rewrite_writer_schema(LOW).unwrap();
    assert!(
        json.contains("decimal"),
        "precision<=76 decimal is NOT stripped: {json}"
    );
}

#[test]
fn rewrite_strips_only_high_precision_in_mixed_schema() {
    const MIXED: &str = r#"{"type":"record","name":"R","fields":[
        {"name":"lo","type":{"type":"bytes","logicalType":"decimal","precision":18,"scale":2}},
        {"name":"hi","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}
    ]}"#;
    let json = rewrite_writer_schema(MIXED).unwrap();
    // The low-precision decimal survives (still one "decimal"); the high one is gone.
    assert!(json.contains("decimal"), "low-precision decimal retained");
    assert!(
        !json.contains(r#""precision":100"#),
        "high-precision decimal precision removed"
    );
}

#[test]
fn rewrite_output_is_valid_avro() {
    let json = rewrite_writer_schema(DEC100_0).unwrap();
    assert!(
        AvroWriterSchema::parse_str(&json).is_ok(),
        "rewritten schema re-parses"
    );
}

#[test]
fn rewrite_nested_high_precision_stripped_recursively() {
    const NESTED: &str = r#"{"type":"record","name":"R","fields":[
        {"name":"xfers","type":{"type":"array","items":{"type":"record","name":"X","fields":[
            {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}
        ]}}}
    ]}"#;
    let json = rewrite_writer_schema(NESTED).unwrap();
    assert!(
        !json.contains("decimal"),
        "nested high-precision decimal stripped recursively: {json}"
    );
}

// ===========================================================================
// GROUP 10: end-to-end nested coercion through the decoder (list/struct/u256)
// ===========================================================================

#[test]
fn e2e_nested_list_struct_u256_and_nested_decimal128() {
    // top-level u256 (>76,scale0) + array<record{who: string, amt: high-precision decimal}>.
    // Per the vendored oracle, a NESTED high-precision decimal maps to Decimal128(p,s) (NOT the
    // top-level u256/Utf8 special-casing, which only rewrites root record fields).
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[
        {"name":"top","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}},
        {"name":"xfers","type":["null",{"type":"array","items":{"type":"record","name":"X","fields":[
            {"name":"who","type":["null","string"],"default":null},
            {"name":"amt","type":["null",{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}],"default":null}
        ]}}],"default":null}
    ]}"#;
    let schema = parse(SCHEMA);
    let mut top = [0u8; 32];
    top[31] = 0x09;
    let mut rec = Record::new(&schema).unwrap();
    rec.put("top", Value::Decimal(Decimal::from(top.to_vec())));
    let inner = Value::Record(vec![
        (
            "who".to_string(),
            Value::Union(1, Box::new(Value::String("bob".into()))),
        ),
        (
            "amt".to_string(),
            Value::Union(
                1,
                Box::new(Value::Decimal(Decimal::from(vec![0x12, 0xD6, 0x87]))),
            ), // 1234567
        ),
    ]);
    rec.put(
        "xfers",
        Value::Union(1, Box::new(Value::Array(vec![inner]))),
    );
    let body = to_avro_datum(&schema, rec).unwrap();

    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, SCHEMA).unwrap();
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");

    // top u256
    assert_eq!(fsb32(b.column(0)).value(0), &top);
    // xfers list<struct{who: Utf8, amt: Decimal128(100,0)}>
    let list = b
        .column(b.schema().index_of("xfers").unwrap())
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let st = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let amt_col = st.column_by_name("amt").unwrap();
    assert_eq!(
        amt_col.data_type(),
        &DataType::Decimal128(100, 0),
        "nested high-precision decimal maps to Decimal128, not u256/Utf8"
    );
    let amt = amt_col
        .as_any()
        .downcast_ref::<PrimitiveArray<Decimal128Type>>()
        .unwrap();
    assert_eq!(
        amt.value(0),
        1_234_567_i128,
        "nested decimal keeps its unscaled integer"
    );
    let who = st
        .column_by_name("who")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(who.value(0), "bob");
}

#[test]
fn e2e_empty_array_column_decodes_to_empty_list() {
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[
        {"name":"id","type":"long"},
        {"name":"xs","type":{"type":"array","items":"long"}}
    ]}"#;
    let schema = parse(SCHEMA);
    let mut rec = Record::new(&schema).unwrap();
    rec.put("id", Value::Long(1));
    rec.put("xs", Value::Array(vec![]));
    let body = to_avro_datum(&schema, rec).unwrap();
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, SCHEMA).unwrap();
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    let list = b
        .column(b.schema().index_of("xs").unwrap())
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list.value(0).len(), 0, "empty avro array => empty list");
}

// ===========================================================================
// GROUP 11: coerce output-schema fidelity & metadata
// ===========================================================================

#[test]
fn coerce_output_schema_carries_target_metadata() {
    let target = target_of(DEC100_0); // field carries u256 extension metadata
    assert!(
        !target.field(0).metadata().is_empty(),
        "u256 target field has metadata"
    );
    let src = batch(
        vec![Field::new("v", DataType::Binary, false)],
        vec![bin(&[&[0x01]])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(
        out.schema().field(0).metadata(),
        target.field(0).metadata(),
        "coerced batch preserves the target field's extension metadata"
    );
}

#[test]
fn coerce_preserves_row_count_across_column_kinds() {
    let target = schema(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true), // missing -> null-filled
    ]);
    let src = batch(
        vec![Field::new("a", DataType::Int64, false)],
        vec![i64s(&[1, 2, 3, 4])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    for c in out.columns() {
        assert_eq!(c.len(), 4, "every column keeps the batch row count");
    }
}

#[test]
fn coerce_binary_target_binary_source_passthrough() {
    // A plain bytes field (Binary target, no decimal metadata) passes through unchanged.
    let target = schema(vec![Field::new("raw", DataType::Binary, false)]);
    let src = batch(
        vec![Field::new("raw", DataType::Binary, false)],
        vec![bin(&[&[1, 2, 3]])],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(col.value(0), &[1, 2, 3]);
}

#[test]
fn coerce_fixed_size_binary_passthrough() {
    // fixed(4) -> FixedSizeBinary(4) target, source already that type, no u256/i256 metadata.
    const FIXED4: &str = r#"{"type":"record","name":"R","fields":[{"name":"h","type":{"type":"fixed","name":"H","size":4}}]}"#;
    let target = target_of(FIXED4);
    assert_eq!(target.field(0).data_type(), &DataType::FixedSizeBinary(4));
    let arr = FixedSizeBinaryArray::try_from_iter(vec![vec![1u8, 2, 3, 4]].into_iter()).unwrap();
    let src = batch(
        vec![Field::new("h", DataType::FixedSizeBinary(4), false)],
        vec![Arc::new(arr)],
    );
    let out = coerce_batch_to_target(&src, &target).unwrap();
    assert_eq!(fsb32(out.column(0)).value(0), &[1, 2, 3, 4]);
}

#[test]
fn e2e_reader_derived_target_matches_oracle_types() {
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[
        {"name":"b","type":"boolean"},
        {"name":"i","type":"int"},
        {"name":"l","type":"long"},
        {"name":"f","type":"float"},
        {"name":"d","type":"double"},
        {"name":"s","type":"string"},
        {"name":"by","type":"bytes"}
    ]}"#;
    let s = parse(SCHEMA);
    let d = ConfluentAvroDecoder::new().with_reader_schema(&s).unwrap();
    let t = d.target_schema().unwrap();
    assert_eq!(t.field(0).data_type(), &DataType::Boolean);
    assert_eq!(t.field(1).data_type(), &DataType::Int32);
    assert_eq!(t.field(2).data_type(), &DataType::Int64);
    assert_eq!(t.field(3).data_type(), &DataType::Float32);
    assert_eq!(t.field(4).data_type(), &DataType::Float64);
    assert_eq!(t.field(5).data_type(), &DataType::Utf8);
    assert_eq!(t.field(6).data_type(), &DataType::Binary);
}
