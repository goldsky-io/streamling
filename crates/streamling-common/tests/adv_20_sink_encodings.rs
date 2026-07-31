//! Adversarial agent 20 — decimal_arb at the **serialization boundaries**.
//!
//! Three sinks, one question each:
//!
//! * **Avro** (`formats::avro::{to_avro, serialize}`) — does a `decimal_arb`
//!   column surface as an Avro `decimal` logical type carrying the *declared*
//!   precision and scale, at the top level and nested inside records / arrays /
//!   unions (the F7 regression surface)? And do the canonical bytes
//!   (`[sign][BE magnitude]`) become the *exact* two's-complement unscaled
//!   integer bytes Avro requires?
//! * **JSON** (`formats::json`) — does a `decimal_arb` render as a decimal
//!   *number* string (never hex, never base64), at the top level and
//!   recursively through structs / lists / maps (the F6 regression surface)?
//! * **Arrow IPC** (`formats::ipc`) — does the extension metadata survive a
//!   write-then-read round trip?
//!
//! Every assertion pins an exact encoded byte string / JSON text, not merely
//! "encoding succeeded". Tests marked `#[ignore = "FINDING: …"]` encode the
//! behaviour the contract implies and currently fail against the product.

use std::str::FromStr;
use std::sync::Arc;

use apache_avro::Schema as AvroSchema;
use apache_avro::schema::{DecimalSchema, RecordSchema};
use apache_avro::types::Value as AvroValue;

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Int64Array, LargeBinaryArray, LargeListArray, ListArray,
    MapArray, RecordBatch, StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Fields, Schema};

use num_bigint::BigInt;

use streamling_common::formats::avro::{serialize, to_avro};
use streamling_common::formats::ipc::{FromArrowToIpcConverter, FromIpcToArrowConverter};
use streamling_common::formats::json::{FromArrowToJsonConverter, JsonToArrowConverter};
use streamling_common::formats::{FromArrowConverter, ToArrowConverter};
use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, NativeIntKind,
};

// =====================================================================
// Helpers
// =====================================================================

fn arb_field(name: &str, p: u32, s: u32, nullable: bool) -> Field {
    DecimalArbType::field(name, p, s, nullable).expect("decimal_arb field")
}

/// Build the raw `LargeBinaryArray` backing a decimal_arb column.
fn arb_col(name: &str, p: u32, s: u32, vals: &[Option<&str>]) -> LargeBinaryArray {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), name, p, s).expect("builder");
    for v in vals {
        match v {
            Some(x) => b.append_str(x).expect("append_str"),
            None => b.append_null(),
        }
    }
    b.finish().into_inner().0
}

/// Single-column decimal_arb batch.
fn arb_batch(
    name: &str,
    p: u32,
    s: u32,
    nullable: bool,
    vals: &[Option<&str>],
) -> (Arc<Schema>, RecordBatch) {
    let field = arb_field(name, p, s, nullable);
    let schema = Arc::new(Schema::new(vec![field]));
    let col = arb_col(name, p, s, vals);
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(col) as ArrayRef]).expect("batch");
    (schema, batch)
}

// ---- Avro schema navigation ----

fn avro_record(s: &AvroSchema) -> &RecordSchema {
    match s {
        AvroSchema::Record(r) => r,
        other => panic!("expected avro record schema, got {other:?}"),
    }
}

fn avro_field_schema<'a>(s: &'a AvroSchema, name: &str) -> &'a AvroSchema {
    let r = avro_record(s);
    let idx = *r
        .lookup
        .get(name)
        .unwrap_or_else(|| panic!("avro record has no field '{name}' (lookup: {:?})", r.lookup));
    &r.fields[idx].schema
}

/// Second (non-null) variant of a nullable field's union.
fn union_value_variant(s: &AvroSchema) -> &AvroSchema {
    match s {
        AvroSchema::Union(u) => {
            let variants = u.variants();
            assert_eq!(
                variants.len(),
                2,
                "nullable avro field must be a 2-variant union, got {variants:?}"
            );
            assert!(
                matches!(variants[0], AvroSchema::Null),
                "first union variant must be null, got {:?}",
                variants[0]
            );
            &variants[1]
        }
        other => panic!("expected union schema, got {other:?}"),
    }
}

fn avro_array_items(s: &AvroSchema) -> &AvroSchema {
    match s {
        AvroSchema::Array(a) => &a.items,
        other => panic!("expected array schema, got {other:?}"),
    }
}

fn as_decimal_schema(s: &AvroSchema) -> &DecimalSchema {
    match s {
        AvroSchema::Decimal(d) => d,
        other => panic!("expected avro decimal logical type, got {other:?}"),
    }
}

// ---- Avro value navigation ----

fn avro_value_field<'a>(v: &'a AvroValue, name: &str) -> &'a AvroValue {
    match v {
        AvroValue::Record(fields) => fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v)
            .unwrap_or_else(|| panic!("avro record value has no field '{name}'")),
        other => panic!("expected avro record value, got {other:?}"),
    }
}

fn unwrap_union_value(v: &AvroValue) -> &AvroValue {
    match v {
        AvroValue::Union(_, inner) => inner,
        other => other,
    }
}

/// Exact bytes carried by an Avro `decimal` value (sign-extended to the length
/// the writer produced — i.e. the literal payload that lands on the wire).
fn avro_decimal_bytes(v: &AvroValue) -> Vec<u8> {
    match unwrap_union_value(v) {
        AvroValue::Decimal(d) => <Vec<u8>>::try_from(d).expect("decimal -> bytes"),
        other => panic!("expected AvroValue::Decimal, got {other:?}"),
    }
}

fn avro_array_items_value(v: &AvroValue) -> &Vec<AvroValue> {
    match unwrap_union_value(v) {
        AvroValue::Array(items) => items,
        other => panic!("expected AvroValue::Array, got {other:?}"),
    }
}

/// Serialize a one-column decimal_arb batch and return the exact Avro decimal
/// payload bytes for row 0.
fn avro_bytes_for(p: u32, s: u32, text: &str) -> Vec<u8> {
    let (schema, batch) = arb_batch("amount", p, s, false, &[Some(text)]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    assert_eq!(rows.len(), 1, "one input row must produce one avro record");
    avro_decimal_bytes(avro_value_field(&rows[0], "amount"))
}

/// Independent reference implementation of "unscaled two's-complement
/// big-endian", derived from the decimal text rather than from the product's
/// canonical bytes.
fn reference_unscaled_be(text: &str, scale: u32) -> Vec<u8> {
    let bd = bigdecimal::BigDecimal::from_str(text).expect("parse");
    let scaled = bd.with_scale_round(scale as i64, bigdecimal::RoundingMode::HalfEven);
    let (bi, exp) = scaled.into_bigint_and_exponent();
    assert_eq!(
        exp, scale as i64,
        "reference rescale must land on the scale"
    );
    bi.to_signed_bytes_be()
}

// ---- JSON ----

fn json_rows(batch: &RecordBatch) -> Vec<String> {
    FromArrowToJsonConverter::new()
        .convert_from_batch(batch)
        .expect("json conversion")
        .into_iter()
        .map(|b| String::from_utf8(b).expect("utf8"))
        .collect()
}

fn json_row0(batch: &RecordBatch) -> String {
    json_rows(batch).swap_remove(0)
}

fn json_for(p: u32, s: u32, text: &str) -> String {
    let (_, batch) = arb_batch("amount", p, s, false, &[Some(text)]);
    json_row0(&batch)
}

// ---- structural builders ----

fn struct_of(children: Vec<(Field, ArrayRef)>) -> (Fields, StructArray) {
    let fields: Fields = children
        .iter()
        .map(|(f, _)| Arc::new(f.clone()))
        .collect::<Vec<_>>()
        .into();
    let cols: Vec<ArrayRef> = children.into_iter().map(|(_, a)| a).collect();
    let sa = StructArray::new(fields.clone(), cols, None);
    (fields, sa)
}

fn list_of(item: Field, values: ArrayRef, offsets: Vec<i32>) -> (Field, ListArray) {
    let item = Arc::new(item);
    let arr = ListArray::new(
        item.clone(),
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    );
    (Field::new("items", DataType::List(item), false), arr)
}

// =====================================================================
// SECTION A — canonical bytes → Avro unscaled two's-complement bytes
// =====================================================================

#[test]
fn avro_zero_at_scale_zero_encodes_as_single_zero_byte() {
    assert_eq!(
        avro_bytes_for(10, 0, "0"),
        vec![0x00],
        "avro decimal for zero must be the single byte 0x00"
    );
}

#[test]
fn avro_zero_at_scale_four_encodes_as_single_zero_byte() {
    assert_eq!(
        avro_bytes_for(10, 4, "0.0000"),
        vec![0x00],
        "zero is zero at every scale; unscaled value is still 0"
    );
}

#[test]
fn avro_negative_zero_encodes_identically_to_positive_zero() {
    assert_eq!(
        avro_bytes_for(10, 3, "-0.000"),
        avro_bytes_for(10, 3, "0.000"),
        "-0 and +0 must produce byte-identical avro payloads"
    );
}

#[test]
fn avro_one_at_scale_zero_is_0x01() {
    assert_eq!(avro_bytes_for(10, 0, "1"), vec![0x01]);
}

#[test]
fn avro_minus_one_at_scale_zero_is_0xff() {
    assert_eq!(
        avro_bytes_for(10, 0, "-1"),
        vec![0xFF],
        "-1 in two's complement big-endian is the single byte 0xFF"
    );
}

#[test]
fn avro_127_is_single_byte_0x7f() {
    assert_eq!(avro_bytes_for(10, 0, "127"), vec![0x7F]);
}

#[test]
fn avro_128_needs_a_leading_zero_sign_byte() {
    assert_eq!(
        avro_bytes_for(10, 0, "128"),
        vec![0x00, 0x80],
        "positive 128 must be sign-extended so it is not read as -128"
    );
}

#[test]
fn avro_minus_128_is_single_byte_0x80() {
    assert_eq!(avro_bytes_for(10, 0, "-128"), vec![0x80]);
}

#[test]
fn avro_minus_129_is_two_bytes() {
    assert_eq!(avro_bytes_for(10, 0, "-129"), vec![0xFF, 0x7F]);
}

#[test]
fn avro_255_is_sign_extended() {
    assert_eq!(avro_bytes_for(10, 0, "255"), vec![0x00, 0xFF]);
}

#[test]
fn avro_256_is_two_bytes_no_padding() {
    assert_eq!(avro_bytes_for(10, 0, "256"), vec![0x01, 0x00]);
}

#[test]
fn avro_minus_256_is_two_bytes() {
    assert_eq!(avro_bytes_for(10, 0, "-256"), vec![0xFF, 0x00]);
}

#[test]
fn avro_32767_is_two_bytes() {
    assert_eq!(avro_bytes_for(10, 0, "32767"), vec![0x7F, 0xFF]);
}

#[test]
fn avro_32768_is_sign_extended_to_three_bytes() {
    assert_eq!(avro_bytes_for(10, 0, "32768"), vec![0x00, 0x80, 0x00]);
}

#[test]
fn avro_minus_32768_is_two_bytes() {
    assert_eq!(avro_bytes_for(10, 0, "-32768"), vec![0x80, 0x00]);
}

#[test]
fn avro_one_at_scale_four_encodes_unscaled_10000() {
    assert_eq!(
        avro_bytes_for(10, 4, "1"),
        vec![0x27, 0x10],
        "value 1 in a scale-4 column is unscaled 10000 == 0x2710"
    );
}

#[test]
fn avro_minus_one_at_scale_four_encodes_unscaled_minus_10000() {
    assert_eq!(avro_bytes_for(10, 4, "-1"), vec![0xD8, 0xF0]);
}

#[test]
fn avro_123_45_at_scale_two_encodes_unscaled_12345() {
    assert_eq!(avro_bytes_for(10, 2, "123.45"), vec![0x30, 0x39]);
}

#[test]
fn avro_minus_123_45_at_scale_two_encodes_unscaled_minus_12345() {
    assert_eq!(avro_bytes_for(10, 2, "-123.45"), vec![0xCF, 0xC7]);
}

#[test]
fn avro_trailing_zeros_do_not_change_encoding() {
    assert_eq!(
        avro_bytes_for(10, 4, "1.2300"),
        avro_bytes_for(10, 4, "1.23"),
        "1.23 and 1.2300 are the same number and must encode identically"
    );
}

#[test]
fn avro_leading_plus_and_leading_zeros_do_not_change_encoding() {
    assert_eq!(
        avro_bytes_for(10, 2, "+05.50"),
        avro_bytes_for(10, 2, "5.5")
    );
}

#[test]
fn avro_hundred_digit_value_matches_reference_encoding() {
    let big = format!("1{}", "0".repeat(99));
    assert_eq!(
        avro_bytes_for(120, 0, &big),
        reference_unscaled_be(&big, 0),
        "wide (>256-bit) magnitudes must encode losslessly"
    );
}

#[test]
fn avro_negative_hundred_digit_value_matches_reference_encoding() {
    let big = format!("-9{}", "8".repeat(99));
    assert_eq!(
        avro_bytes_for(120, 0, &big),
        reference_unscaled_be(&big, 0),
        "wide negative magnitudes must encode losslessly"
    );
}

#[test]
fn avro_value_with_scale_equal_to_precision_encodes_reference_bytes() {
    let text = "0.123456789012345678901234567890";
    assert_eq!(
        avro_bytes_for(30, 30, text),
        reference_unscaled_be(text, 30),
        "scale == precision (pure fraction) must still encode the unscaled integer"
    );
}

#[test]
fn avro_bytes_decode_back_to_the_original_value() {
    for text in [
        "0",
        "1",
        "-1",
        "123.45",
        "-123.45",
        "0.00001",
        "-0.00001",
        "99999999999999999999.99999",
    ] {
        let bytes = avro_bytes_for(60, 5, text);
        let bigint = BigInt::from_signed_bytes_be(&bytes);
        let back = DecimalArbValue::from_bigint_and_scale(bigint, 5);
        assert_eq!(
            back,
            DecimalArbValue::from_str(text).unwrap(),
            "avro decimal payload for '{text}' must decode back to the same number"
        );
    }
}

#[test]
fn avro_null_cell_takes_union_branch_zero() {
    let (schema, batch) = arb_batch("amount", 20, 2, true, &[Some("1.00"), None]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    match avro_value_field(&rows[1], "amount") {
        AvroValue::Union(idx, inner) => {
            assert_eq!(*idx, 0, "null must select the null union branch");
            assert!(matches!(**inner, AvroValue::Null));
        }
        other => panic!("nullable decimal_arb must serialize as a union, got {other:?}"),
    }
}

#[test]
fn avro_non_null_cell_of_nullable_column_takes_union_branch_one() {
    let (schema, batch) = arb_batch("amount", 20, 2, true, &[Some("1.00"), None]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    match avro_value_field(&rows[0], "amount") {
        AvroValue::Union(idx, inner) => {
            assert_eq!(*idx, 1, "non-null must select the value union branch");
            assert_eq!(
                avro_decimal_bytes(inner),
                vec![0x64],
                "1.00 at scale 2 is unscaled 100 == 0x64"
            );
        }
        other => panic!("nullable decimal_arb must serialize as a union, got {other:?}"),
    }
}

#[test]
fn avro_empty_canonical_payload_is_treated_as_zero() {
    // Defensive path: a zero-length cell must not panic the row encoder.
    let field = arb_field("amount", 20, 2, false);
    let schema = Arc::new(Schema::new(vec![field]));
    let col = LargeBinaryArray::from(vec![Some(b"".as_ref())]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col) as ArrayRef]).unwrap();
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    assert_eq!(
        avro_decimal_bytes(avro_value_field(&rows[0], "amount")),
        vec![0x00],
        "empty canonical payload must encode as avro decimal zero"
    );
}

// =====================================================================
// SECTION B — Avro schema shape for decimal_arb
// =====================================================================

#[test]
fn avro_schema_top_level_decimal_arb_is_decimal_logical_type() {
    let schema = Arc::new(Schema::new(vec![arb_field("amount", 100, 18, false)]));
    let avro = to_avro("T", &schema.fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "amount"));
    assert_eq!(
        (d.precision, d.scale),
        (100, 18),
        "avro decimal must carry the declared (precision, scale)"
    );
}

#[test]
fn avro_schema_decimal_inner_type_is_bytes() {
    let schema = Arc::new(Schema::new(vec![arb_field("amount", 100, 18, false)]));
    let avro = to_avro("T", &schema.fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "amount"));
    assert!(
        matches!(*d.inner, AvroSchema::Bytes),
        "decimal_arb must ride on avro `bytes`, got {:?}",
        d.inner
    );
}

#[test]
fn avro_schema_nullable_decimal_arb_is_union_null_then_decimal() {
    let schema = Arc::new(Schema::new(vec![arb_field("amount", 80, 30, true)]));
    let avro = to_avro("T", &schema.fields);
    let inner = union_value_variant(avro_field_schema(&avro, "amount"));
    let d = as_decimal_schema(inner);
    assert_eq!((d.precision, d.scale), (80, 30));
}

#[test]
fn avro_schema_precision_one_scale_zero() {
    let schema = Arc::new(Schema::new(vec![arb_field("x", 1, 0, false)]));
    let avro = to_avro("T", &schema.fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "x"));
    assert_eq!((d.precision, d.scale), (1, 0));
}

#[test]
fn avro_schema_precision_38_scale_10_is_not_downgraded() {
    let schema = Arc::new(Schema::new(vec![arb_field("x", 38, 10, false)]));
    let avro = to_avro("T", &schema.fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "x"));
    assert_eq!((d.precision, d.scale), (38, 10));
}

#[test]
fn avro_schema_precision_76_scale_38() {
    let schema = Arc::new(Schema::new(vec![arb_field("x", 76, 38, false)]));
    let avro = to_avro("T", &schema.fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "x"));
    assert_eq!((d.precision, d.scale), (76, 38));
}

#[test]
fn avro_schema_precision_beyond_decimal256_is_preserved() {
    let schema = Arc::new(Schema::new(vec![arb_field("x", 1000, 500, false)]));
    let avro = to_avro("T", &schema.fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "x"));
    assert_eq!(
        (d.precision, d.scale),
        (1000, 500),
        "precision far beyond Decimal256 must not be clamped on the write path"
    );
}

#[test]
fn avro_schema_max_precision_is_preserved() {
    let schema = Arc::new(Schema::new(vec![arb_field("x", 65_535, 0, false)]));
    let avro = to_avro("T", &schema.fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "x"));
    assert_eq!((d.precision, d.scale), (65_535, 0));
}

#[test]
fn avro_schema_scale_equal_to_precision_is_preserved() {
    let schema = Arc::new(Schema::new(vec![arb_field("x", 40, 40, false)]));
    let avro = to_avro("T", &schema.fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "x"));
    assert_eq!((d.precision, d.scale), (40, 40));
}

#[test]
fn avro_schema_native_int_kind_hint_does_not_disturb_decimal_logical_type() {
    let field =
        DecimalArbType::with_native_int_kind(arb_field("bal", 78, 0, false), NativeIntKind::U256)
            .unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let avro = to_avro("T", &schema.fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "bal"));
    assert_eq!(
        (d.precision, d.scale),
        (78, 0),
        "the u256 origin hint must not perturb the avro decimal mapping"
    );
}

#[test]
fn avro_schema_plain_large_binary_stays_bytes() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "blob",
        DataType::LargeBinary,
        false,
    )]));
    let avro = to_avro("T", &schema.fields);
    assert!(
        matches!(avro_field_schema(&avro, "blob"), AvroSchema::Bytes),
        "LargeBinary without decimal_arb metadata must stay plain avro bytes"
    );
}

#[test]
fn avro_schema_decimal128_and_decimal_arb_both_decimal() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("d128", DataType::Decimal128(10, 2), false),
        arb_field("arb", 100, 4, false),
    ]));
    let avro = to_avro("T", &schema.fields);
    assert_eq!(
        (
            as_decimal_schema(avro_field_schema(&avro, "d128")).precision,
            as_decimal_schema(avro_field_schema(&avro, "d128")).scale
        ),
        (10, 2)
    );
    assert_eq!(
        (
            as_decimal_schema(avro_field_schema(&avro, "arb")).precision,
            as_decimal_schema(avro_field_schema(&avro, "arb")).scale
        ),
        (100, 4)
    );
}

#[test]
fn avro_schema_two_decimal_arb_columns_keep_independent_scales() {
    let schema = Arc::new(Schema::new(vec![
        arb_field("a", 30, 2, false),
        arb_field("b", 30, 18, false),
    ]));
    let avro = to_avro("T", &schema.fields);
    assert_eq!(as_decimal_schema(avro_field_schema(&avro, "a")).scale, 2);
    assert_eq!(as_decimal_schema(avro_field_schema(&avro, "b")).scale, 18);
}

#[test]
fn avro_schema_field_name_with_dots_is_sanitized_and_still_decimal() {
    let schema = Arc::new(Schema::new(vec![arb_field("a.b", 30, 2, false)]));
    let avro = to_avro("T", &schema.fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "a__b"));
    assert_eq!((d.precision, d.scale), (30, 2));
}

#[test]
fn avro_schema_json_text_carries_decimal_logical_type() {
    let schema = Arc::new(Schema::new(vec![arb_field("amount", 100, 18, false)]));
    let avro = to_avro("T", &schema.fields);
    let json = serde_json::to_string(&avro).expect("serialize avro schema");
    assert!(
        json.contains("\"logicalType\":\"decimal\""),
        "emitted avro schema JSON must advertise the decimal logical type: {json}"
    );
    assert!(
        json.contains("\"precision\":100"),
        "emitted avro schema JSON must carry precision 100: {json}"
    );
}

#[test]
fn avro_schema_serialized_json_does_not_leak_extension_metadata_keys() {
    let schema = Arc::new(Schema::new(vec![arb_field("amount", 100, 18, false)]));
    let avro = to_avro("T", &schema.fields);
    let json = serde_json::to_string(&avro).unwrap();
    assert!(
        !json.contains("ARROW:extension"),
        "arrow extension metadata keys must not leak into the avro schema: {json}"
    );
}

// =====================================================================
// SECTION C — nested Avro (records / arrays / unions) — F7 surface
// =====================================================================

fn nested_struct_schema(inner: Vec<Field>, struct_nullable: bool) -> Arc<Schema> {
    let fields: Fields = inner.into_iter().map(Arc::new).collect::<Vec<_>>().into();
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("inner", DataType::Struct(fields), struct_nullable),
    ]))
}

#[test]
fn avro_schema_decimal_arb_inside_non_null_struct_keeps_logical_type() {
    let schema = nested_struct_schema(vec![arb_field("amt", 100, 6, false)], false);
    let avro = to_avro("R", &schema.fields);
    let inner = avro_field_schema(&avro, "inner");
    let d = as_decimal_schema(avro_field_schema(inner, "amt"));
    assert_eq!(
        (d.precision, d.scale),
        (100, 6),
        "nested decimal_arb must keep its decimal logicalType (F7)"
    );
}

#[test]
fn avro_schema_decimal_arb_inside_nullable_struct_keeps_logical_type() {
    let schema = nested_struct_schema(vec![arb_field("amt", 100, 6, false)], true);
    let avro = to_avro("R", &schema.fields);
    let inner = union_value_variant(avro_field_schema(&avro, "inner"));
    let d = as_decimal_schema(avro_field_schema(inner, "amt"));
    assert_eq!((d.precision, d.scale), (100, 6));
}

#[test]
fn avro_schema_nullable_decimal_arb_inside_struct_is_union() {
    let schema = nested_struct_schema(vec![arb_field("amt", 100, 6, true)], false);
    let avro = to_avro("R", &schema.fields);
    let inner = avro_field_schema(&avro, "inner");
    let d = as_decimal_schema(union_value_variant(avro_field_schema(inner, "amt")));
    assert_eq!((d.precision, d.scale), (100, 6));
}

#[test]
fn avro_schema_decimal_arb_two_levels_deep_keeps_logical_type() {
    let lvl2: Fields = vec![Arc::new(arb_field("amt", 90, 9, false))].into();
    let lvl1: Fields = vec![Arc::new(Field::new("mid", DataType::Struct(lvl2), false))].into();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "outer",
        DataType::Struct(lvl1),
        false,
    )]));
    let avro = to_avro("R", &schema.fields);
    let outer = avro_field_schema(&avro, "outer");
    let mid = avro_field_schema(outer, "mid");
    let d = as_decimal_schema(avro_field_schema(mid, "amt"));
    assert_eq!((d.precision, d.scale), (90, 9));
}

#[test]
fn avro_schema_list_of_decimal_arb_items_keep_logical_type() {
    let item = Arc::new(arb_field("item", 100, 3, false));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amts",
        DataType::List(item),
        false,
    )]));
    let avro = to_avro("R", &schema.fields);
    let items = avro_array_items(avro_field_schema(&avro, "amts"));
    let d = as_decimal_schema(items);
    assert_eq!(
        (d.precision, d.scale),
        (100, 3),
        "decimal_arb array items must keep the decimal logicalType (F7)"
    );
}

#[test]
fn avro_schema_list_of_nullable_decimal_arb_items_is_union() {
    let item = Arc::new(arb_field("item", 100, 3, true));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amts",
        DataType::List(item),
        false,
    )]));
    let avro = to_avro("R", &schema.fields);
    let items = avro_array_items(avro_field_schema(&avro, "amts"));
    let d = as_decimal_schema(union_value_variant(items));
    assert_eq!((d.precision, d.scale), (100, 3));
}

#[test]
fn avro_schema_list_of_struct_with_decimal_arb_keeps_logical_type() {
    let inner: Fields = vec![Arc::new(arb_field("amt", 100, 0, false))].into();
    let item = Arc::new(Field::new("row", DataType::Struct(inner), false));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "rows",
        DataType::List(item),
        false,
    )]));
    let avro = to_avro("R", &schema.fields);
    let items = avro_array_items(avro_field_schema(&avro, "rows"));
    let d = as_decimal_schema(avro_field_schema(items, "amt"));
    assert_eq!((d.precision, d.scale), (100, 0));
}

#[test]
fn avro_schema_nullable_list_of_decimal_arb_is_union_of_array() {
    let item = Arc::new(arb_field("item", 50, 2, false));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amts",
        DataType::List(item),
        true,
    )]));
    let avro = to_avro("R", &schema.fields);
    let arr = union_value_variant(avro_field_schema(&avro, "amts"));
    let d = as_decimal_schema(avro_array_items(arr));
    assert_eq!((d.precision, d.scale), (50, 2));
}

#[test]
fn avro_values_decimal_arb_inside_non_null_struct_encode_exact_bytes() {
    let amt = arb_col("amt", 100, 2, &[Some("123.45")]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 2, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("inner", DataType::Struct(fields), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(sa) as ArrayRef,
        ],
    )
    .unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let inner = avro_value_field(&rows[0], "inner");
    assert_eq!(
        avro_decimal_bytes(avro_value_field(inner, "amt")),
        vec![0x30, 0x39],
        "nested decimal_arb must encode the unscaled integer, not raw canonical bytes"
    );
}

#[test]
fn avro_values_negative_decimal_arb_inside_struct_encode_exact_bytes() {
    let amt = arb_col("amt", 100, 2, &[Some("-123.45")]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 2, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(sa) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let inner = avro_value_field(&rows[0], "inner");
    assert_eq!(
        avro_decimal_bytes(avro_value_field(inner, "amt")),
        vec![0xCF, 0xC7]
    );
}

#[test]
fn avro_values_nullable_decimal_arb_inside_struct_null_takes_branch_zero() {
    let amt = arb_col("amt", 100, 2, &[None]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 2, true),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(sa) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let inner = avro_value_field(&rows[0], "inner");
    match avro_value_field(inner, "amt") {
        AvroValue::Union(idx, v) => {
            assert_eq!(*idx, 0);
            assert!(matches!(**v, AvroValue::Null));
        }
        other => panic!("expected union, got {other:?}"),
    }
}

#[test]
fn avro_values_list_of_decimal_arb_encodes_each_element_exactly() {
    let values = arb_col("item", 100, 2, &[Some("1.00"), Some("-1.00"), Some("0")]);
    let (list_field, list) = list_of(
        arb_field("item", 100, 2, false),
        Arc::new(values) as ArrayRef,
        vec![0, 3],
    );
    let schema = Arc::new(Schema::new(vec![list_field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let items = avro_array_items_value(avro_value_field(&rows[0], "items"));
    assert_eq!(items.len(), 3, "all three list elements must be emitted");
    assert_eq!(avro_decimal_bytes(&items[0]), vec![0x64]);
    assert_eq!(avro_decimal_bytes(&items[1]), vec![0x9C]);
    assert_eq!(avro_decimal_bytes(&items[2]), vec![0x00]);
}

#[test]
fn avro_values_list_of_nullable_decimal_arb_preserves_null_elements() {
    let values = arb_col("item", 100, 0, &[Some("7"), None]);
    let (list_field, list) = list_of(
        arb_field("item", 100, 0, true),
        Arc::new(values) as ArrayRef,
        vec![0, 2],
    );
    let schema = Arc::new(Schema::new(vec![list_field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let items = avro_array_items_value(avro_value_field(&rows[0], "items"));
    assert!(
        matches!(items[0], AvroValue::Union(1, _)),
        "non-null list element must take branch 1, got {:?}",
        items[0]
    );
    assert!(
        matches!(items[1], AvroValue::Union(0, _)),
        "null list element must take branch 0, got {:?}",
        items[1]
    );
}

#[test]
fn avro_values_empty_list_of_decimal_arb_encodes_empty_array() {
    let values = arb_col("item", 100, 0, &[]);
    let (list_field, list) = list_of(
        arb_field("item", 100, 0, false),
        Arc::new(values) as ArrayRef,
        vec![0, 0],
    );
    let schema = Arc::new(Schema::new(vec![list_field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    assert!(
        avro_array_items_value(avro_value_field(&rows[0], "items")).is_empty(),
        "an empty list must encode as an empty avro array"
    );
}

#[test]
fn avro_values_list_of_struct_with_decimal_arb_encodes_exact_bytes() {
    let amt = arb_col("amt", 100, 0, &[Some("255"), Some("-1")]);
    let (inner_fields, inner) = struct_of(vec![(
        arb_field("amt", 100, 0, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let item = Arc::new(Field::new("row", DataType::Struct(inner_fields), false));
    let list = ListArray::new(
        item.clone(),
        OffsetBuffer::new(vec![0i32, 2].into()),
        Arc::new(inner) as ArrayRef,
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "rows",
        DataType::List(item),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let items = avro_array_items_value(avro_value_field(&rows[0], "rows"));
    assert_eq!(items.len(), 2);
    assert_eq!(
        avro_decimal_bytes(avro_value_field(&items[0], "amt")),
        vec![0x00, 0xFF],
        "decimal_arb inside an array-of-records must be a real avro decimal (F7)"
    );
    assert_eq!(
        avro_decimal_bytes(avro_value_field(&items[1], "amt")),
        vec![0xFF]
    );
}

#[test]
fn avro_values_decimal_arb_two_levels_deep_encodes_exact_bytes() {
    let amt = arb_col("amt", 90, 0, &[Some("256")]);
    let (lvl2_fields, lvl2) = struct_of(vec![(
        arb_field("amt", 90, 0, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let (lvl1_fields, lvl1) = struct_of(vec![(
        Field::new("mid", DataType::Struct(lvl2_fields), false),
        Arc::new(lvl2) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "outer",
        DataType::Struct(lvl1_fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(lvl1) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let mid = avro_value_field(avro_value_field(&rows[0], "outer"), "mid");
    assert_eq!(
        avro_decimal_bytes(avro_value_field(mid, "amt")),
        vec![0x01, 0x00]
    );
}

#[test]
fn avro_values_multiple_rows_each_get_their_own_record() {
    let (schema, batch) = arb_batch("amount", 40, 1, false, &[Some("1.0"), Some("2.0")]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        avro_decimal_bytes(avro_value_field(&rows[0], "amount")),
        [10]
    );
    assert_eq!(
        avro_decimal_bytes(avro_value_field(&rows[1], "amount")),
        [20]
    );
}

// ---- known-gap probes on the nested Avro write path ----

#[test]
#[ignore = "FINDING: to_avro panics on List<Struct> whose item field is nullable (arrow's default); get_field_schema requires a Record but sees the item Union"]
fn avro_list_of_struct_with_nullable_item_serializes() {
    let amt = arb_col("amt", 100, 0, &[Some("5")]);
    let (inner_fields, inner) = struct_of(vec![(
        arb_field("amt", 100, 0, false),
        Arc::new(amt) as ArrayRef,
    )]);
    // arrow's ListBuilder produces a *nullable* item field by default.
    let item = Arc::new(Field::new("item", DataType::Struct(inner_fields), true));
    let list = ListArray::new(
        item.clone(),
        OffsetBuffer::new(vec![0i32, 1].into()),
        Arc::new(inner) as ArrayRef,
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "rows",
        DataType::List(item),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let items = avro_array_items_value(avro_value_field(&rows[0], "rows"));
    assert_eq!(
        avro_decimal_bytes(avro_value_field(unwrap_union_value(&items[0]), "amt")),
        vec![0x05],
        "a nullable struct item must still encode its nested decimal_arb"
    );
}

/// Two `List<Struct>` columns whose item fields both carry arrow's default name
/// `"item"` generate two nested Avro records both named `item_item`. When the
/// two structs are *identical* apache-avro accepts the duplicate definition, so
/// this shape works — pinned here so a change in the name-derivation scheme
/// (`field_to_avro`'s hardcoded `"item"` parent) doesn't silently regress it.
#[test]
fn avro_two_identical_list_of_struct_columns_with_default_item_name_serialize() {
    let make = |v: &str| {
        let amt = arb_col("amt", 100, 0, &[Some(v)]);
        let (inner_fields, inner) = struct_of(vec![(
            arb_field("amt", 100, 0, false),
            Arc::new(amt) as ArrayRef,
        )]);
        let item = Arc::new(Field::new("item", DataType::Struct(inner_fields), false));
        let list = ListArray::new(
            item.clone(),
            OffsetBuffer::new(vec![0i32, 1].into()),
            Arc::new(inner) as ArrayRef,
            None,
        );
        (item, list)
    };
    let (item_a, list_a) = make("1");
    let (item_b, list_b) = make("2");
    let schema = Arc::new(Schema::new(vec![
        Field::new("transfers", DataType::List(item_a), false),
        Field::new("traces", DataType::List(item_b), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(list_a) as ArrayRef, Arc::new(list_b) as ArrayRef],
    )
    .unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    assert_eq!(
        avro_decimal_bytes(avro_value_field(
            &avro_array_items_value(avro_value_field(&rows[0], "traces"))[0],
            "amt"
        )),
        vec![0x02],
        "two array-of-record columns must not collide in the generated avro schema"
    );
}

/// The sharper version of the name-derivation question: `field_to_avro` names
/// list-item records from the hardcoded literal `"item"`, so two `List<Struct>`
/// columns with arrow's default item name generate two nested Avro records both
/// called `item_item` — even when the two structs have *different* shapes. This
/// pins that the second column still serializes its own fields (and not the
/// first column's record definition).
#[test]
fn avro_two_differently_shaped_list_of_struct_columns_serialize() {
    let amt = arb_col("amt", 100, 0, &[Some("1")]);
    let (a_fields, a_struct) = struct_of(vec![(
        arb_field("amt", 100, 0, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let a_item = Arc::new(Field::new("item", DataType::Struct(a_fields), false));
    let a_list = ListArray::new(
        a_item.clone(),
        OffsetBuffer::new(vec![0i32, 1].into()),
        Arc::new(a_struct) as ArrayRef,
        None,
    );

    let value = arb_col("value", 100, 2, &[Some("2.50")]);
    let (b_fields, b_struct) = struct_of(vec![
        (
            arb_field("value", 100, 2, false),
            Arc::new(value) as ArrayRef,
        ),
        (
            Field::new("gas", DataType::Int64, false),
            Arc::new(Int64Array::from(vec![21000])) as ArrayRef,
        ),
    ]);
    let b_item = Arc::new(Field::new("item", DataType::Struct(b_fields), false));
    let b_list = ListArray::new(
        b_item.clone(),
        OffsetBuffer::new(vec![0i32, 1].into()),
        Arc::new(b_struct) as ArrayRef,
        None,
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("transfers", DataType::List(a_item), false),
        Field::new("traces", DataType::List(b_item), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(a_list) as ArrayRef, Arc::new(b_list) as ArrayRef],
    )
    .unwrap();

    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let traces = avro_array_items_value(avro_value_field(&rows[0], "traces"));
    assert_eq!(
        avro_decimal_bytes(avro_value_field(&traces[0], "value")),
        vec![0x00, 0xFA],
        "the second array-of-records column must serialize its own struct shape, \
         not the first column's"
    );
}

#[test]
#[ignore = "FINDING: serialize panics on List<List<decimal_arb>> — the inner list re-enters get_field_schema, which requires a Record schema"]
fn avro_nested_list_of_list_of_decimal_arb_serializes() {
    let values = arb_col("item", 100, 0, &[Some("1"), Some("2")]);
    let inner_item = Arc::new(arb_field("item", 100, 0, false));
    let inner_list = ListArray::new(
        inner_item.clone(),
        OffsetBuffer::new(vec![0i32, 2].into()),
        Arc::new(values) as ArrayRef,
        None,
    );
    let outer_item = Arc::new(Field::new("item", DataType::List(inner_item), false));
    let outer_list = ListArray::new(
        outer_item.clone(),
        OffsetBuffer::new(vec![0i32, 1].into()),
        Arc::new(inner_list) as ArrayRef,
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nested",
        DataType::List(outer_item),
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(outer_list) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let outer = avro_array_items_value(avro_value_field(&rows[0], "nested"));
    let inner = avro_array_items_value(&outer[0]);
    assert_eq!(avro_decimal_bytes(&inner[0]), vec![0x01]);
}

#[test]
#[ignore = "FINDING: serialize hits `unimplemented!(\"unsupported data type: LargeList\")` — to_avro happily maps LargeList to an avro array but the value encoder has no LargeList arm"]
fn avro_large_list_of_decimal_arb_serializes() {
    let values = arb_col("item", 100, 0, &[Some("3")]);
    let item = Arc::new(arb_field("item", 100, 0, false));
    let list = LargeListArray::new(
        item.clone(),
        OffsetBuffer::new(vec![0i64, 1].into()),
        Arc::new(values) as ArrayRef,
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amts",
        DataType::LargeList(item),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let items = avro_array_items_value(avro_value_field(&rows[0], "amts"));
    assert_eq!(avro_decimal_bytes(&items[0]), vec![0x03]);
}

#[test]
#[ignore = "FINDING: serialize hits `unimplemented!(\"unsupported data type: FixedSizeList\")` — to_avro maps FixedSizeList to an avro array but the value encoder has no arm for it"]
fn avro_fixed_size_list_of_decimal_arb_serializes() {
    let values = arb_col("item", 100, 0, &[Some("3"), Some("4")]);
    let item = Arc::new(arb_field("item", 100, 0, false));
    let list = FixedSizeListArray::new(item.clone(), 2, Arc::new(values) as ArrayRef, None);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amts",
        DataType::FixedSizeList(item, 2),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let items = avro_array_items_value(avro_value_field(&rows[0], "amts"));
    assert_eq!(avro_decimal_bytes(&items[1]), vec![0x04]);
}

// =====================================================================
// SECTION D — real Avro binary datums (exact wire bytes)
// =====================================================================

#[test]
fn avro_datum_non_null_decimal_arb_has_exact_wire_bytes() {
    let (schema, batch) = arb_batch("amount", 10, 2, false, &[Some("123.45")]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    let datum = apache_avro::to_avro_datum(&avro, rows[0].clone()).expect("encode datum");
    assert_eq!(
        datum,
        vec![0x04, 0x30, 0x39],
        "wire form is avro bytes: zigzag length 2 (0x04) then the unscaled BE bytes"
    );
}

#[test]
fn avro_datum_zero_has_exact_wire_bytes() {
    let (schema, batch) = arb_batch("amount", 10, 2, false, &[Some("0")]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    let datum = apache_avro::to_avro_datum(&avro, rows[0].clone()).unwrap();
    assert_eq!(datum, vec![0x02, 0x00], "zero is one byte of payload");
}

#[test]
fn avro_datum_nullable_non_null_prefixes_union_branch_one() {
    let (schema, batch) = arb_batch("amount", 10, 2, true, &[Some("123.45")]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    let datum = apache_avro::to_avro_datum(&avro, rows[0].clone()).unwrap();
    assert_eq!(
        datum,
        vec![0x02, 0x04, 0x30, 0x39],
        "union branch 1 encodes as zigzag(1) == 0x02"
    );
}

#[test]
fn avro_datum_nullable_null_is_single_zero_byte() {
    let (schema, batch) = arb_batch("amount", 10, 2, true, &[None]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    let datum = apache_avro::to_avro_datum(&avro, rows[0].clone()).unwrap();
    assert_eq!(
        datum,
        vec![0x00],
        "union branch 0 (null) carries no payload"
    );
}

#[test]
fn avro_datum_negative_value_has_exact_wire_bytes() {
    let (schema, batch) = arb_batch("amount", 10, 2, false, &[Some("-123.45")]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    let datum = apache_avro::to_avro_datum(&avro, rows[0].clone()).unwrap();
    assert_eq!(datum, vec![0x04, 0xCF, 0xC7]);
}

#[test]
fn avro_datum_round_trips_back_to_the_same_decimal() {
    let (schema, batch) = arb_batch("amount", 60, 8, false, &[Some("-98765.43210987")]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    let datum = apache_avro::to_avro_datum(&avro, rows[0].clone()).unwrap();
    let mut cursor = std::io::Cursor::new(datum);
    let read = apache_avro::from_avro_datum(&avro, &mut cursor, None).expect("decode datum");
    let bytes = avro_decimal_bytes(avro_value_field(&read, "amount"));
    let back = DecimalArbValue::from_bigint_and_scale(BigInt::from_signed_bytes_be(&bytes), 8);
    assert_eq!(
        back,
        DecimalArbValue::from_str("-98765.43210987").unwrap(),
        "an avro datum must decode back to the original decimal"
    );
}

#[test]
fn avro_datum_wide_precision_round_trips() {
    let big = format!("-1{}", "7".repeat(99));
    let (schema, batch) = arb_batch("amount", 120, 0, false, &[Some(&big)]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    let datum = apache_avro::to_avro_datum(&avro, rows[0].clone()).unwrap();
    let mut cursor = std::io::Cursor::new(datum);
    let read = apache_avro::from_avro_datum(&avro, &mut cursor, None).unwrap();
    let bytes = avro_decimal_bytes(avro_value_field(&read, "amount"));
    let back = DecimalArbValue::from_bigint_and_scale(BigInt::from_signed_bytes_be(&bytes), 0);
    assert_eq!(back, DecimalArbValue::from_str(&big).unwrap());
}

#[test]
fn avro_datum_nested_struct_decimal_round_trips() {
    let amt = arb_col("amt", 100, 4, &[Some("-0.0001")]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 4, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(sa) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let datum = apache_avro::to_avro_datum(&avro, rows[0].clone())
        .expect("nested decimal must be encodable against the generated schema (F7)");
    let mut cursor = std::io::Cursor::new(datum);
    let read = apache_avro::from_avro_datum(&avro, &mut cursor, None).unwrap();
    let bytes = avro_decimal_bytes(avro_value_field(avro_value_field(&read, "inner"), "amt"));
    let back = DecimalArbValue::from_bigint_and_scale(BigInt::from_signed_bytes_be(&bytes), 4);
    assert_eq!(back, DecimalArbValue::from_str("-0.0001").unwrap());
}

#[test]
fn avro_datum_array_of_decimal_round_trips() {
    let values = arb_col("item", 100, 2, &[Some("1.00"), Some("-1.00")]);
    let (list_field, list) = list_of(
        arb_field("item", 100, 2, false),
        Arc::new(values) as ArrayRef,
        vec![0, 2],
    );
    let schema = Arc::new(Schema::new(vec![list_field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list) as ArrayRef]).unwrap();
    let avro = to_avro("R", &schema.fields);
    let rows = serialize(&avro, &batch);
    let datum = apache_avro::to_avro_datum(&avro, rows[0].clone())
        .expect("array-of-decimal must be encodable against the generated schema (F7)");
    let mut cursor = std::io::Cursor::new(datum);
    let read = apache_avro::from_avro_datum(&avro, &mut cursor, None).unwrap();
    let items = avro_array_items_value(avro_value_field(&read, "items"));
    assert_eq!(avro_decimal_bytes(&items[0]), vec![0x64]);
    assert_eq!(avro_decimal_bytes(&items[1]), vec![0x9C]);
}

#[test]
fn avro_datum_rejects_nothing_for_wide_precision_schema() {
    // precision 1000 is far outside any fixed-width decimal; the schema must
    // still be usable for encoding (no silent demotion to plain bytes).
    let (schema, batch) = arb_batch("amount", 1000, 2, false, &[Some("1.00")]);
    let avro = to_avro("T", &schema.fields);
    let rows = serialize(&avro, &batch);
    let datum = apache_avro::to_avro_datum(&avro, rows[0].clone())
        .expect("wide-precision decimal schema must still accept Value::Decimal");
    assert_eq!(datum, vec![0x02, 0x64]);
}

// =====================================================================
// SECTION E — JSON, top level (F6 surface)
// =====================================================================

#[test]
fn json_top_level_decimal_arb_renders_decimal_text() {
    assert_eq!(json_for(20, 2, "123.45"), r#"{"amount":"123.45"}"#);
}

#[test]
fn json_top_level_negative_renders_with_minus_sign() {
    assert_eq!(json_for(20, 2, "-123.45"), r#"{"amount":"-123.45"}"#);
}

#[test]
fn json_top_level_pads_to_the_column_scale() {
    assert_eq!(
        json_for(20, 3, "5"),
        r#"{"amount":"5.000"}"#,
        "the column scale is part of the storage contract and must show in the text"
    );
}

#[test]
fn json_top_level_zero_at_scale_zero_renders_zero() {
    assert_eq!(json_for(20, 0, "0"), r#"{"amount":"0"}"#);
}

#[test]
#[ignore = "FINDING: zero is rendered as \"0\" instead of the scale-padded \"0.0000\"; every other value in the same column is scale-padded, so a scale-4 column emits a mix of \"0\" and \"5.0000\""]
fn json_top_level_zero_is_scale_padded_like_every_other_value() {
    assert_eq!(
        json_for(20, 4, "0"),
        r#"{"amount":"0.0000"}"#,
        "zero must be scale-padded exactly like non-zero values in the same column"
    );
}

#[test]
fn json_top_level_negative_zero_renders_without_minus_sign() {
    assert_eq!(
        json_for(20, 4, "-0.0000"),
        json_for(20, 4, "0.0000"),
        "-0 and +0 must render identically"
    );
}

#[test]
fn json_top_level_null_renders_explicit_null() {
    let (_, batch) = arb_batch("amount", 20, 2, true, &[None]);
    assert_eq!(json_row0(&batch), r#"{"amount":null}"#);
}

#[test]
fn json_top_level_hundred_digit_value_renders_all_digits() {
    let big = format!("1{}", "0".repeat(99));
    let out = json_for(120, 0, &big);
    assert_eq!(out, format!(r#"{{"amount":"{big}"}}"#));
}

#[test]
fn json_top_level_never_emits_hex_of_canonical_bytes() {
    // 255 at scale 0 has canonical bytes 00 FF; the hex rendering would be "00ff".
    let out = json_for(20, 0, "255");
    assert_eq!(out, r#"{"amount":"255"}"#);
    assert!(
        !out.contains("00ff") && !out.contains("00FF"),
        "decimal_arb must never surface as hex of its canonical bytes (F6): {out}"
    );
}

#[test]
fn json_top_level_never_emits_base64_of_canonical_bytes() {
    let out = json_for(20, 0, "255");
    assert!(
        !out.contains("AP8="),
        "decimal_arb must never surface as base64 of its canonical bytes: {out}"
    );
}

#[test]
fn json_top_level_output_parses_back_as_the_same_number() {
    for text in ["0", "1", "-1", "1234.5678", "-0.0001"] {
        let out = json_for(40, 4, text);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let s = v["amount"]
            .as_str()
            .expect("decimal_arb renders as a string");
        assert_eq!(
            DecimalArbValue::from_str(s).unwrap(),
            DecimalArbValue::from_str(text).unwrap(),
            "JSON text for '{text}' must re-parse to the same number (got '{s}')"
        );
    }
}

#[test]
fn json_top_level_text_contains_only_decimal_characters() {
    for text in ["0", "12345", "-12345", "1.5", "-1.5"] {
        let out = json_for(40, 4, text);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        let s = v["amount"].as_str().unwrap();
        assert!(
            s.chars()
                .all(|c| c.is_ascii_digit() || c == '.' || c == '-'),
            "rendered decimal '{s}' must contain only digits, '.' and '-'"
        );
    }
}

#[test]
fn json_two_decimal_arb_columns_keep_independent_scales() {
    let schema = Arc::new(Schema::new(vec![
        arb_field("a", 20, 1, false),
        arb_field("b", 20, 5, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arb_col("a", 20, 1, &[Some("1")])) as ArrayRef,
            Arc::new(arb_col("b", 20, 5, &[Some("1")])) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(json_row0(&batch), r#"{"a":"1.0","b":"1.00000"}"#);
}

#[test]
fn json_decimal_arb_next_to_plain_columns_leaves_them_untouched() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        arb_field("amount", 20, 2, false),
        Field::new("tag", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            Arc::new(arb_col("amount", 20, 2, &[Some("1.50")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(json_row0(&batch), r#"{"id":7,"amount":"1.50","tag":"x"}"#);
}

#[test]
fn json_native_int_kind_hint_does_not_change_rendering() {
    let field =
        DecimalArbType::with_native_int_kind(arb_field("bal", 78, 0, false), NativeIntKind::U256)
            .unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(arb_col("bal", 78, 0, &[Some("115792089237316195")])) as ArrayRef],
    )
    .unwrap();
    assert_eq!(json_row0(&batch), r#"{"bal":"115792089237316195"}"#);
}

#[test]
fn json_multiple_rows_produce_one_object_each() {
    let (_, batch) = arb_batch("amount", 20, 2, true, &[Some("1.00"), None, Some("-2.50")]);
    let rows = json_rows(&batch);
    assert_eq!(
        rows,
        vec![
            r#"{"amount":"1.00"}"#.to_string(),
            r#"{"amount":null}"#.to_string(),
            r#"{"amount":"-2.50"}"#.to_string(),
        ]
    );
}

#[test]
fn json_decimal_arb_field_with_unparseable_metadata_errors_instead_of_panicking() {
    // A LargeBinary field that advertises the extension name but carries no
    // (precision, scale) payload must produce a typed error, not a panic.
    let mut field = Field::new("amount", DataType::LargeBinary, false);
    field = field.with_metadata(std::collections::HashMap::from([(
        DecimalArbType::EXTENSION_NAME_KEY.to_string(),
        DecimalArbType::EXTENSION_NAME.to_string(),
    )]));
    let schema = Arc::new(Schema::new(vec![field]));
    let col = LargeBinaryArray::from(vec![Some(b"\x00\x01".as_ref())]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(col) as ArrayRef]).unwrap();
    let err = FromArrowToJsonConverter::new()
        .convert_from_batch(&batch)
        .expect_err("missing precision/scale metadata must surface as an error");
    assert!(
        err.to_string().contains("amount"),
        "error must name the offending column: {err}"
    );
}

#[test]
fn json_empty_batch_produces_no_rows() {
    let (_, batch) = arb_batch("amount", 20, 2, true, &[]);
    assert!(json_rows(&batch).is_empty());
}

// =====================================================================
// SECTION F — JSON, nested (F6 regression surface)
// =====================================================================

#[test]
fn json_decimal_arb_inside_struct_renders_value_not_hex() {
    let amt = arb_col("amt", 100, 0, &[Some("123456789012345678901234567890")]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 0, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sa) as ArrayRef]).unwrap();
    assert_eq!(
        json_row0(&batch),
        r#"{"inner":{"amt":"123456789012345678901234567890"}}"#
    );
}

#[test]
fn json_decimal_arb_inside_struct_keeps_column_scale() {
    let amt = arb_col("amt", 100, 4, &[Some("2.5")]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 4, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sa) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"inner":{"amt":"2.5000"}}"#);
}

#[test]
fn json_negative_decimal_arb_inside_struct_renders_minus_sign() {
    let amt = arb_col("amt", 100, 2, &[Some("-7.25")]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 2, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sa) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"inner":{"amt":"-7.25"}}"#);
}

#[test]
fn json_null_decimal_arb_inside_struct_renders_null() {
    let amt = arb_col("amt", 100, 2, &[None]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 2, true),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sa) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"inner":{"amt":null}}"#);
}

#[test]
fn json_struct_with_two_decimal_arb_fields_renders_both() {
    let a = arb_col("a", 100, 1, &[Some("1")]);
    let b = arb_col("b", 100, 3, &[Some("-2")]);
    let (fields, sa) = struct_of(vec![
        (arb_field("a", 100, 1, false), Arc::new(a) as ArrayRef),
        (arb_field("b", 100, 3, false), Arc::new(b) as ArrayRef),
    ]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sa) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"inner":{"a":"1.0","b":"-2.000"}}"#);
}

#[test]
fn json_struct_mixing_decimal_arb_and_plain_fields_renders_both() {
    let amt = arb_col("amt", 100, 0, &[Some("42")]);
    let (fields, sa) = struct_of(vec![
        (
            Field::new("label", DataType::Utf8, false),
            Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
        ),
        (arb_field("amt", 100, 0, false), Arc::new(amt) as ArrayRef),
    ]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sa) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"inner":{"label":"x","amt":"42"}}"#);
}

#[test]
fn json_list_of_decimal_arb_renders_each_element() {
    let values = arb_col("item", 100, 2, &[Some("1"), Some("-2"), Some("0")]);
    let (list_field, list) = list_of(
        arb_field("item", 100, 2, false),
        Arc::new(values) as ArrayRef,
        vec![0, 3],
    );
    let schema = Arc::new(Schema::new(vec![list_field]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(list) as ArrayRef]).unwrap();
    assert_eq!(
        json_row0(&batch),
        r#"{"items":["1.00","-2.00","0"]}"#,
        "each list element must be decimal text, never hex (F6)"
    );
}

#[test]
fn json_list_of_decimal_arb_preserves_null_elements() {
    let values = arb_col("item", 100, 0, &[Some("1"), None]);
    let (list_field, list) = list_of(
        arb_field("item", 100, 0, true),
        Arc::new(values) as ArrayRef,
        vec![0, 2],
    );
    let schema = Arc::new(Schema::new(vec![list_field]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(list) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"items":["1",null]}"#);
}

#[test]
fn json_empty_list_of_decimal_arb_renders_empty_array() {
    let values = arb_col("item", 100, 0, &[]);
    let (list_field, list) = list_of(
        arb_field("item", 100, 0, false),
        Arc::new(values) as ArrayRef,
        vec![0, 0],
    );
    let schema = Arc::new(Schema::new(vec![list_field]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(list) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"items":[]}"#);
}

#[test]
fn json_list_of_decimal_arb_splits_rows_by_offsets() {
    let values = arb_col("item", 100, 0, &[Some("1"), Some("2"), Some("3")]);
    let (list_field, list) = list_of(
        arb_field("item", 100, 0, false),
        Arc::new(values) as ArrayRef,
        vec![0, 1, 3],
    );
    let schema = Arc::new(Schema::new(vec![list_field]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(list) as ArrayRef]).unwrap();
    assert_eq!(
        json_rows(&batch),
        vec![
            r#"{"items":["1"]}"#.to_string(),
            r#"{"items":["2","3"]}"#.to_string()
        ],
        "per-row slicing must not shift the list values window"
    );
}

#[test]
fn json_large_list_of_decimal_arb_renders_each_element() {
    let values = arb_col("item", 100, 1, &[Some("1"), Some("-1")]);
    let item = Arc::new(arb_field("item", 100, 1, false));
    let list = LargeListArray::new(
        item.clone(),
        OffsetBuffer::new(vec![0i64, 2].into()),
        Arc::new(values) as ArrayRef,
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "items",
        DataType::LargeList(item),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(list) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"items":["1.0","-1.0"]}"#);
}

#[test]
fn json_fixed_size_list_of_decimal_arb_renders_each_element() {
    let values = arb_col("item", 100, 0, &[Some("1"), Some("2")]);
    let item = Arc::new(arb_field("item", 100, 0, false));
    let list = FixedSizeListArray::new(item.clone(), 2, Arc::new(values) as ArrayRef, None);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "items",
        DataType::FixedSizeList(item, 2),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(list) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"items":["1","2"]}"#);
}

fn map_batch(
    value_field: Field,
    values: ArrayRef,
    keys: Vec<&str>,
    offsets: Vec<i32>,
) -> RecordBatch {
    let key_field = Field::new("keys", DataType::Utf8, false);
    let entry_fields: Fields = vec![Arc::new(key_field), Arc::new(value_field)].into();
    let entries = StructArray::new(
        entry_fields.clone(),
        vec![Arc::new(StringArray::from(keys)) as ArrayRef, values],
        None,
    );
    let entry_field = Arc::new(Field::new("entries", DataType::Struct(entry_fields), false));
    let map = MapArray::new(
        entry_field.clone(),
        OffsetBuffer::new(offsets.into()),
        entries,
        None,
        false,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "m",
        DataType::Map(entry_field, false),
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(map) as ArrayRef]).unwrap()
}

#[test]
fn json_map_with_decimal_arb_values_renders_decimal_text() {
    let values = arb_col("values", 100, 2, &[Some("1"), Some("-2")]);
    let batch = map_batch(
        arb_field("values", 100, 2, false),
        Arc::new(values) as ArrayRef,
        vec!["a", "b"],
        vec![0, 2],
    );
    assert_eq!(
        json_row0(&batch),
        r#"{"m":{"a":"1.00","b":"-2.00"}}"#,
        "decimal_arb map values must render as decimal text, never hex (F6)"
    );
}

#[test]
fn json_map_with_null_decimal_arb_value_renders_null() {
    let values = arb_col("values", 100, 0, &[None]);
    let batch = map_batch(
        arb_field("values", 100, 0, true),
        Arc::new(values) as ArrayRef,
        vec!["a"],
        vec![0, 1],
    );
    assert_eq!(json_row0(&batch), r#"{"m":{"a":null}}"#);
}

#[test]
fn json_map_splits_rows_by_offsets() {
    let values = arb_col("values", 100, 0, &[Some("1"), Some("2")]);
    let batch = map_batch(
        arb_field("values", 100, 0, false),
        Arc::new(values) as ArrayRef,
        vec!["a", "b"],
        vec![0, 1, 2],
    );
    assert_eq!(
        json_rows(&batch),
        vec![
            r#"{"m":{"a":"1"}}"#.to_string(),
            r#"{"m":{"b":"2"}}"#.to_string()
        ]
    );
}

#[test]
fn json_list_of_struct_with_decimal_arb_renders_values() {
    let amt = arb_col(
        "amt",
        100,
        0,
        &[Some("123456789012345678901234567890"), Some("7")],
    );
    let (inner_fields, inner) = struct_of(vec![(
        arb_field("amt", 100, 0, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let item = Arc::new(Field::new("item", DataType::Struct(inner_fields), true));
    let list = ListArray::new(
        item.clone(),
        OffsetBuffer::new(vec![0i32, 2].into()),
        Arc::new(inner) as ArrayRef,
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("items", DataType::List(item), false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(list) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(
        json_row0(&batch),
        r#"{"id":1,"items":[{"amt":"123456789012345678901234567890"},{"amt":"7"}]}"#
    );
}

#[test]
fn json_struct_containing_list_of_decimal_arb_renders_values() {
    let values = arb_col("item", 100, 1, &[Some("1"), Some("2")]);
    let item = Arc::new(arb_field("item", 100, 1, false));
    let list = ListArray::new(
        item.clone(),
        OffsetBuffer::new(vec![0i32, 2].into()),
        Arc::new(values) as ArrayRef,
        None,
    );
    let (fields, sa) = struct_of(vec![(
        Field::new("amts", DataType::List(item), false),
        Arc::new(list) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sa) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"inner":{"amts":["1.0","2.0"]}}"#);
}

#[test]
fn json_list_of_list_of_decimal_arb_renders_values() {
    let values = arb_col("item", 100, 0, &[Some("1"), Some("2"), Some("3")]);
    let inner_item = Arc::new(arb_field("item", 100, 0, false));
    let inner_list = ListArray::new(
        inner_item.clone(),
        OffsetBuffer::new(vec![0i32, 2, 3].into()),
        Arc::new(values) as ArrayRef,
        None,
    );
    let outer_item = Arc::new(Field::new("item", DataType::List(inner_item), false));
    let outer_list = ListArray::new(
        outer_item.clone(),
        OffsetBuffer::new(vec![0i32, 2].into()),
        Arc::new(inner_list) as ArrayRef,
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nested",
        DataType::List(outer_item),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(outer_list) as ArrayRef]).unwrap();
    assert_eq!(json_row0(&batch), r#"{"nested":[["1","2"],["3"]]}"#);
}

#[test]
fn json_three_levels_of_nesting_renders_decimal_text() {
    let amt = arb_col("amt", 100, 2, &[Some("9.99")]);
    let (lvl3_fields, lvl3) = struct_of(vec![(
        arb_field("amt", 100, 2, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let item = Arc::new(Field::new("item", DataType::Struct(lvl3_fields), false));
    let list = ListArray::new(
        item.clone(),
        OffsetBuffer::new(vec![0i32, 1].into()),
        Arc::new(lvl3) as ArrayRef,
        None,
    );
    let (lvl1_fields, lvl1) = struct_of(vec![(
        Field::new("rows", DataType::List(item), false),
        Arc::new(list) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "outer",
        DataType::Struct(lvl1_fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(lvl1) as ArrayRef]).unwrap();
    assert_eq!(
        json_row0(&batch),
        r#"{"outer":{"rows":[{"amt":"9.99"}]}}"#,
        "struct → list → struct → decimal_arb must still render decimal text (F6)"
    );
}

#[test]
fn json_nested_output_contains_no_hex_digits_of_canonical_bytes() {
    // 255 at scale 0 → canonical bytes 00 FF. Hex would show up as "00ff".
    let amt = arb_col("amt", 100, 0, &[Some("255")]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 0, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sa) as ArrayRef]).unwrap();
    let out = json_row0(&batch);
    assert_eq!(out, r#"{"inner":{"amt":"255"}}"#);
    assert!(!out.to_lowercase().contains("00ff"), "F6 regression: {out}");
}

#[test]
fn json_container_without_decimal_arb_passes_through_unchanged() {
    let (fields, sa) = struct_of(vec![(
        Field::new("label", DataType::Utf8, false),
        Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![
        arb_field("amount", 20, 0, false),
        Field::new("inner", DataType::Struct(fields), false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arb_col("amount", 20, 0, &[Some("3")])) as ArrayRef,
            Arc::new(sa) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(
        json_row0(&batch),
        r#"{"amount":"3","inner":{"label":"x"}}"#,
        "rewriting decimal_arb leaves must not disturb sibling containers"
    );
}

#[test]
fn json_nested_decimal_arb_renders_identically_to_top_level() {
    let top = json_for(100, 6, "-12345.678901");
    let amt = arb_col("amt", 100, 6, &[Some("-12345.678901")]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 6, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sa) as ArrayRef]).unwrap();
    let nested = json_row0(&batch);
    let top_v: serde_json::Value = serde_json::from_str(&top).unwrap();
    let nested_v: serde_json::Value = serde_json::from_str(&nested).unwrap();
    assert_eq!(
        top_v["amount"], nested_v["inner"]["amt"],
        "nesting must not change the rendered text"
    );
}

// =====================================================================
// SECTION G — JSON read path
// =====================================================================

#[test]
fn json_read_top_level_decimal_arb_round_trips() {
    let field = arb_field("amount", 80, 6, true);
    let schema = Arc::new(Schema::new(vec![field]));
    let mut c = JsonToArrowConverter::new(schema.clone(), true, None);
    c.buffer(r#"{"amount":"-12345.678901"}"#.to_string());
    let batch = c.convert_to_batch().unwrap();
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(col.value(0), 6).unwrap(),
        DecimalArbValue::from_str("-12345.678901").unwrap()
    );
}

#[test]
fn json_read_preserves_extension_metadata_on_the_output_field() {
    let field = arb_field("amount", 80, 6, true);
    let schema = Arc::new(Schema::new(vec![field]));
    let mut c = JsonToArrowConverter::new(schema.clone(), true, None);
    c.buffer(r#"{"amount":"1.5"}"#.to_string());
    let batch = c.convert_to_batch().unwrap();
    let out = batch.schema().field(0).clone();
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out),
        Some((80, 6)),
        "the decoded batch must still advertise decimal_arb(80, 6)"
    );
}

#[test]
fn json_read_null_stays_null() {
    let field = arb_field("amount", 80, 6, true);
    let schema = Arc::new(Schema::new(vec![field]));
    let mut c = JsonToArrowConverter::new(schema.clone(), true, None);
    c.buffer(r#"{"amount":null}"#.to_string());
    let batch = c.convert_to_batch().unwrap();
    assert!(batch.column(0).is_null(0));
}

#[test]
fn json_read_rejects_value_exceeding_declared_precision() {
    let schema = Arc::new(Schema::new(vec![arb_field("x", 5, 0, true)]));
    let mut c = JsonToArrowConverter::new(schema, true, None);
    c.buffer(r#"{"x":"123456"}"#.to_string());
    let err = c.convert_to_batch().unwrap_err();
    assert!(
        err.to_string().contains("'x'"),
        "precision-overflow error must name the column: {err}"
    );
}

#[test]
fn json_write_then_read_round_trip_is_numerically_stable_at_top_level() {
    let (_, batch) = arb_batch(
        "amount",
        60,
        8,
        true,
        &[Some("1.5"), None, Some("-0.00000001")],
    );
    let json = format!("[{}]", json_rows(&batch).join(","));
    let mut c = JsonToArrowConverter::new(batch.schema(), false, None);
    c.buffer(json);
    let back = c.convert_to_batch().unwrap();
    let col = back
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(col.value(0), 8).unwrap(),
        DecimalArbValue::from_str("1.5").unwrap()
    );
    assert!(col.is_null(1));
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(col.value(2), 8).unwrap(),
        DecimalArbValue::from_str("-0.00000001").unwrap()
    );
}

#[test]
#[ignore = "FINDING: F6's fix is write-only. JsonToArrowConverter only rewrites TOP-LEVEL decimal_arb to Utf8, so a nested decimal_arb stays LargeBinary and arrow-json hex-decodes the decimal text — '123456' silently becomes the bytes 12 34 56"]
fn json_read_nested_decimal_arb_round_trips() {
    let inner: Fields = vec![Arc::new(arb_field("amt", 100, 0, false))].into();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("inner", DataType::Struct(inner), false),
    ]));
    let mut c = JsonToArrowConverter::new(schema, true, None);
    c.buffer(r#"{"id":1,"inner":{"amt":"123456"}}"#.to_string());
    let batch = c
        .convert_to_batch()
        .expect("nested decimal_arb must decode");
    let sa = batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let col = sa
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(col.value(0), 0).unwrap(),
        DecimalArbValue::from_str("123456").unwrap(),
        "a nested decimal_arb read from JSON must decode to the written number"
    );
}

// =====================================================================
// SECTION H — Arrow IPC round trip
// =====================================================================

fn ipc_round_trip(batch: &RecordBatch, target: Arc<Schema>) -> RecordBatch {
    let bytes = FromArrowToIpcConverter::new()
        .convert_from_batch(batch)
        .expect("ipc write");
    assert_eq!(
        bytes.len(),
        1,
        "a non-empty batch must produce one IPC blob"
    );
    let mut r = FromIpcToArrowConverter::new(target);
    r.buffer(bytes.into_iter().next().unwrap());
    r.convert_to_batch().expect("ipc read")
}

#[test]
fn ipc_preserves_decimal_arb_extension_metadata() {
    let (schema, batch) = arb_batch("amount", 100, 18, true, &[Some("1.5")]);
    let back = ipc_round_trip(&batch, schema);
    let f = back.schema().field(0).clone();
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "decimal_arb extension metadata must survive Arrow IPC"
    );
}

#[test]
fn ipc_preserves_precision_and_scale() {
    let (schema, batch) = arb_batch("amount", 100, 18, true, &[Some("1.5")]);
    let back = ipc_round_trip(&batch, schema);
    let f = back.schema().field(0).clone();
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((100, 18))
    );
}

#[test]
fn ipc_preserves_canonical_bytes_exactly() {
    let (schema, batch) = arb_batch("amount", 60, 4, true, &[Some("123.45"), Some("-123.45")]);
    let original = batch
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .clone();
    let back = ipc_round_trip(&batch, schema);
    let restored = back
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(restored.value(0), original.value(0));
    assert_eq!(restored.value(1), original.value(1));
}

#[test]
fn ipc_preserves_nulls() {
    let (schema, batch) = arb_batch("amount", 60, 4, true, &[Some("1"), None, Some("2")]);
    let back = ipc_round_trip(&batch, schema);
    assert!(!back.column(0).is_null(0));
    assert!(back.column(0).is_null(1));
    assert!(!back.column(0).is_null(2));
}

#[test]
fn ipc_preserves_native_int_kind_hint() {
    let field =
        DecimalArbType::with_native_int_kind(arb_field("bal", 78, 0, true), NativeIntKind::U256)
            .unwrap();
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arb_col("bal", 78, 0, &[Some("5")])) as ArrayRef],
    )
    .unwrap();
    let back = ipc_round_trip(&batch, schema);
    let f = back.schema().field(0).clone();
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        Some(NativeIntKind::U256),
        "the native_int_kind origin hint must survive Arrow IPC"
    );
}

#[test]
fn ipc_preserves_metadata_of_nested_struct_decimal_arb() {
    let amt = arb_col("amt", 100, 6, &[Some("1.5")]);
    let (fields, sa) = struct_of(vec![(
        arb_field("amt", 100, 6, false),
        Arc::new(amt) as ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(fields),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(sa) as ArrayRef]).unwrap();
    let back = ipc_round_trip(&batch, schema);
    let DataType::Struct(children) = back.schema().field(0).data_type().clone() else {
        panic!("expected struct");
    };
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&children[0]),
        Some((100, 6)),
        "nested decimal_arb metadata must survive Arrow IPC"
    );
}

#[test]
fn ipc_preserves_metadata_of_list_item_decimal_arb() {
    let values = arb_col("item", 100, 3, &[Some("1")]);
    let (list_field, list) = list_of(
        arb_field("item", 100, 3, false),
        Arc::new(values) as ArrayRef,
        vec![0, 1],
    );
    let schema = Arc::new(Schema::new(vec![list_field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list) as ArrayRef]).unwrap();
    let back = ipc_round_trip(&batch, schema);
    let DataType::List(item) = back.schema().field(0).data_type().clone() else {
        panic!("expected list");
    };
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&item),
        Some((100, 3)),
        "decimal_arb list-item metadata must survive Arrow IPC"
    );
}

#[test]
fn ipc_preserves_multiple_decimal_arb_columns() {
    let schema = Arc::new(Schema::new(vec![
        arb_field("a", 30, 2, false),
        arb_field("b", 40, 9, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arb_col("a", 30, 2, &[Some("1")])) as ArrayRef,
            Arc::new(arb_col("b", 40, 9, &[Some("2")])) as ArrayRef,
        ],
    )
    .unwrap();
    let back = ipc_round_trip(&batch, schema);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&back.schema().field(0).clone()),
        Some((30, 2))
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&back.schema().field(1).clone()),
        Some((40, 9))
    );
}

#[test]
fn ipc_round_trip_is_schema_identical() {
    let (schema, batch) = arb_batch("amount", 100, 18, true, &[Some("1.5")]);
    let back = ipc_round_trip(&batch, schema.clone());
    assert_eq!(
        back.schema(),
        schema,
        "the restored schema must equal the written one, metadata included"
    );
}

#[test]
fn ipc_round_trip_of_wide_value_is_lossless() {
    let big = format!("-1{}.{}", "3".repeat(80), "7".repeat(18));
    let (schema, batch) = arb_batch("amount", 120, 18, true, &[Some(&big)]);
    let back = ipc_round_trip(&batch, schema);
    let col = back
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(col.value(0), 18).unwrap(),
        DecimalArbValue::from_str(&big).unwrap()
    );
}

#[test]
fn ipc_write_is_deterministic() {
    let (_, batch) = arb_batch("amount", 60, 4, true, &[Some("1.5"), None]);
    let a = FromArrowToIpcConverter::new()
        .convert_from_batch(&batch)
        .unwrap();
    let b = FromArrowToIpcConverter::new()
        .convert_from_batch(&batch)
        .unwrap();
    assert_eq!(
        a, b,
        "IPC serialization of an identical batch must be byte-identical"
    );
}

#[test]
fn ipc_empty_batch_produces_no_blobs() {
    let (_, batch) = arb_batch("amount", 60, 4, true, &[]);
    assert!(
        FromArrowToIpcConverter::new()
            .convert_from_batch(&batch)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn ipc_then_json_still_renders_decimal_text() {
    let (schema, batch) = arb_batch("amount", 60, 4, true, &[Some("-1.5")]);
    let back = ipc_round_trip(&batch, schema);
    assert_eq!(
        json_row0(&back),
        r#"{"amount":"-1.5000"}"#,
        "a batch that survived IPC must still render as decimal text, not hex"
    );
}

#[test]
fn ipc_then_avro_still_encodes_decimal_logical_type() {
    let (schema, batch) = arb_batch("amount", 60, 2, false, &[Some("123.45")]);
    let back = ipc_round_trip(&batch, schema);
    let avro = to_avro("T", &back.schema().fields);
    let d = as_decimal_schema(avro_field_schema(&avro, "amount"));
    assert_eq!((d.precision, d.scale), (60, 2));
    let rows = serialize(&avro, &back);
    assert_eq!(
        avro_decimal_bytes(avro_value_field(&rows[0], "amount")),
        vec![0x30, 0x39]
    );
}

// =====================================================================
// SECTION I — cross-sink agreement
// =====================================================================

#[test]
fn avro_and_json_agree_on_the_same_number_across_signs_and_scales() {
    for (p, s, text) in [
        (40u32, 0u32, "0"),
        (40, 0, "1"),
        (40, 0, "-1"),
        (40, 4, "1.2345"),
        (40, 4, "-1.2345"),
        (40, 10, "0.0000000001"),
        (40, 10, "-0.0000000001"),
    ] {
        let bytes = avro_bytes_for(p, s, text);
        let from_avro =
            DecimalArbValue::from_bigint_and_scale(BigInt::from_signed_bytes_be(&bytes), s as i64);
        let json = json_for(p, s, text);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        let from_json = DecimalArbValue::from_str(v["amount"].as_str().unwrap()).unwrap();
        assert_eq!(
            from_avro, from_json,
            "avro and json encodings of '{text}' (p={p}, s={s}) must decode to the same number"
        );
    }
}

#[test]
fn avro_and_json_agree_on_wide_precision_values() {
    let big = format!("-1{}", "6".repeat(90));
    let bytes = avro_bytes_for(120, 0, &big);
    let from_avro = DecimalArbValue::from_bigint_and_scale(BigInt::from_signed_bytes_be(&bytes), 0);
    let json = json_for(120, 0, &big);
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    let from_json = DecimalArbValue::from_str(v["amount"].as_str().unwrap()).unwrap();
    assert_eq!(from_avro, from_json);
    assert_eq!(from_avro, DecimalArbValue::from_str(&big).unwrap());
}

#[test]
fn numerically_equal_inputs_produce_identical_avro_payloads() {
    let a = avro_bytes_for(30, 2, "5");
    for form in ["5.0", "5.00", "05", "+5", "+05.00"] {
        assert_eq!(
            avro_bytes_for(30, 2, form),
            a,
            "'{form}' must produce the same avro payload as '5'"
        );
    }
}

#[test]
fn numerically_equal_inputs_produce_identical_json_text() {
    let a = json_for(30, 2, "5");
    for form in ["5.0", "5.00", "05", "+5", "+05.00"] {
        assert_eq!(
            json_for(30, 2, form),
            a,
            "'{form}' must render the same JSON text as '5'"
        );
    }
}

#[test]
fn avro_payload_length_is_minimal_two_complement() {
    // 1 at scale 0 needs exactly one byte; padding would be a wire regression.
    assert_eq!(avro_bytes_for(65_535, 0, "1").len(), 1);
    assert_eq!(avro_bytes_for(65_535, 0, "-1").len(), 1);
    assert_eq!(avro_bytes_for(65_535, 0, "0").len(), 1);
}

#[test]
fn avro_payload_does_not_depend_on_declared_precision() {
    assert_eq!(
        avro_bytes_for(10, 2, "1.23"),
        avro_bytes_for(1000, 2, "1.23"),
        "the wire payload is the unscaled integer; declared precision must not change it"
    );
}

#[test]
fn avro_payload_does_depend_on_declared_scale() {
    assert_ne!(
        avro_bytes_for(30, 2, "1"),
        avro_bytes_for(30, 4, "1"),
        "the unscaled integer must reflect the column scale"
    );
    assert_eq!(avro_bytes_for(30, 2, "1"), vec![0x64]);
    assert_eq!(avro_bytes_for(30, 4, "1"), vec![0x27, 0x10]);
}
