//! Adversarial integration tests for standard Arrow decimal decode/coercion via the arrow-avro
//! path (PR #60): avro `decimal` with precision <= 38 -> `Decimal128(p,s)` and 38 < precision <= 76
//! -> `Decimal256(p,s)`.
//!
//! Oracle (from `convert_avro_schema_to_arrow` + the removed vendored `resolve_decimal` /
//! `resolve_decimal_256`):
//!   * `Decimal128Array::value(0)` / `Decimal256Array::value(0)` returns the *unscaled* integer,
//!     reconstructed from the wire bytes by sign-extending the big-endian two's-complement bytes.
//!   * The field data type is `Decimal128(p,s)` / `Decimal256(p,s)` exactly, precision/scale from
//!     the avro schema.
//!   * TOP-LEVEL fields use precision to pick Decimal128 vs Decimal256; NESTED decimals (inside a
//!     record/array) always map to `Decimal128(p,s)` regardless of precision (vendored behavior).
//!   * precision 77 with scale 0 leaves the decimal family (u256 FixedSizeBinary(32)); precision 77
//!     with scale > 0 maps to Utf8.
//!
//! Each test decodes a Confluent-framed message and asserts the exact CORRECT value/type, so a
//! decode or coercion regression fails the assertion (or panics, which is also a test failure).

use apache_avro::Decimal;
use apache_avro::types::{Record, Value};
use apache_avro::{Schema as AvroWriterSchema, to_avro_datum};
use arrow::array::{Array, Decimal128Array, Decimal256Array, ListArray, StructArray};
use arrow::datatypes::i256;
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use bigdecimal::num_bigint::BigInt;
use streamling_common::formats::avro::arrow_avro::ConfluentAvroDecoder;
use streamling_common::formats::avro::convert_avro_schema_to_arrow;

// ---------------------------------------------------------------------------
// Framing + schema-building helpers
// ---------------------------------------------------------------------------

fn confluent_frame(id: u32, body: &[u8]) -> Vec<u8> {
    let mut f = vec![0x00];
    f.extend_from_slice(&id.to_be_bytes());
    f.extend_from_slice(body);
    f
}

/// A `bytes`-backed decimal logicalType JSON fragment.
fn decimal_type(p: u64, s: u64) -> String {
    format!(r#"{{"type":"bytes","logicalType":"decimal","precision":{p},"scale":{s}}}"#)
}

/// A single-field record schema JSON: `{record rec_name { field_name: field_type }}`.
fn record_type_json(rec_name: &str, field_name: &str, field_type: &str) -> String {
    format!(
        r#"{{"type":"record","name":"{rec_name}","fields":[{{"name":"{field_name}","type":{field_type}}}]}}"#
    )
}

fn array_field_json(item_type: &str) -> String {
    format!(r#"{{"type":"array","items":{item_type}}}"#)
}

fn single_decimal_schema(p: u64, s: u64) -> String {
    record_type_json("R", "v", &decimal_type(p, s))
}

// ---------------------------------------------------------------------------
// Encode/decode helpers
// ---------------------------------------------------------------------------

fn decode_record(schema_json: &str, values: Vec<(&str, Value)>) -> RecordBatch {
    let schema = AvroWriterSchema::parse_str(schema_json).unwrap();
    let mut rec = Record::new(&schema).unwrap();
    for (k, v) in values {
        rec.put(k, v);
    }
    let body = to_avro_datum(&schema, rec).unwrap();
    let id = 1u32;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, schema_json).unwrap();
    d.decode(&confluent_frame(id, &body)).unwrap();
    d.flush().unwrap().expect("a batch")
}

fn decode_rows(schema_json: &str, rows: Vec<Vec<(&str, Value)>>) -> RecordBatch {
    let schema = AvroWriterSchema::parse_str(schema_json).unwrap();
    let id = 1u32;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, schema_json).unwrap();
    for vals in rows {
        let mut rec = Record::new(&schema).unwrap();
        for (k, v) in vals {
            rec.put(k, v);
        }
        let body = to_avro_datum(&schema, rec).unwrap();
        d.decode(&confluent_frame(id, &body)).unwrap();
    }
    d.flush().unwrap().expect("a batch")
}

/// Minimal big-endian two's-complement bytes for an i128 (matches num-bigint's encoding).
fn be(v: i128) -> Vec<u8> {
    BigInt::from(v).to_signed_bytes_be()
}

fn be_big(v: &BigInt) -> Vec<u8> {
    v.to_signed_bytes_be()
}

fn dec_val(bytes: Vec<u8>) -> Value {
    Value::Decimal(Decimal::from(bytes))
}

fn nines_i128(n: usize) -> i128 {
    "9".repeat(n).parse().unwrap()
}

fn nines_big(n: usize) -> BigInt {
    "9".repeat(n).parse().unwrap()
}

fn pow10_big(n: usize) -> BigInt {
    format!("1{}", "0".repeat(n)).parse().unwrap()
}

/// Independent i256 oracle: sign-extend the minimal two's-complement bytes to 32 bytes.
fn i256_of(v: &BigInt) -> i256 {
    let bytes = v.to_signed_bytes_be();
    assert!(bytes.len() <= 32, "value too wide for i256 test oracle");
    let negative = !bytes.is_empty() && (bytes[0] & 0x80) != 0;
    let fill = if negative { 0xFFu8 } else { 0x00 };
    let mut ext = [fill; 32];
    let n = bytes.len();
    ext[32 - n..].copy_from_slice(&bytes);
    i256::from_be_bytes(ext)
}

fn i256_from_i128(v: i128) -> i256 {
    i256_of(&BigInt::from(v))
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

fn dec128_col(b: &RecordBatch) -> &Decimal128Array {
    b.column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("top-level column is Decimal128Array")
}

fn dec256_col(b: &RecordBatch) -> &Decimal256Array {
    b.column(0)
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .expect("top-level column is Decimal256Array")
}

fn assert_dec128(b: &RecordBatch, expected: i128, p: u8, s: i8) {
    assert_eq!(
        b.schema().field(0).data_type(),
        &DataType::Decimal128(p, s),
        "field type must be Decimal128({p},{s})"
    );
    let a = dec128_col(b);
    assert!(!a.is_null(0), "value must be non-null");
    assert_eq!(a.value(0), expected, "unscaled i128 value");
    assert_eq!(a.precision(), p, "precision");
    assert_eq!(a.scale(), s, "scale");
}

fn assert_dec256(b: &RecordBatch, expected: i256, p: u8, s: i8) {
    assert_eq!(
        b.schema().field(0).data_type(),
        &DataType::Decimal256(p, s),
        "field type must be Decimal256({p},{s})"
    );
    let a = dec256_col(b);
    assert!(!a.is_null(0), "value must be non-null");
    assert_eq!(a.value(0), expected, "unscaled i256 value");
    assert_eq!(a.precision(), p, "precision");
    assert_eq!(a.scale(), s, "scale");
}

/// Decode a single top-level Decimal128 value from explicit wire bytes.
fn dec128_bytes(p: u64, s: u64, bytes: Vec<u8>) -> RecordBatch {
    decode_record(&single_decimal_schema(p, s), vec![("v", dec_val(bytes))])
}

fn dec128_i128(p: u64, s: u64, v: i128) -> RecordBatch {
    dec128_bytes(p, s, be(v))
}

fn dec256_bytes(p: u64, s: u64, bytes: Vec<u8>) -> RecordBatch {
    decode_record(&single_decimal_schema(p, s), vec![("v", dec_val(bytes))])
}

fn dec256_big(p: u64, s: u64, v: &BigInt) -> RecordBatch {
    dec256_bytes(p, s, be_big(v))
}

// ===========================================================================
// Decimal128 (precision <= 38): top-level, native passthrough path
// ===========================================================================

#[test]
fn dec128_p1_s0_zero() {
    // zero encoded as a single 0x00 byte.
    assert_dec128(&dec128_bytes(1, 0, vec![0x00]), 0, 1, 0);
}

#[test]
fn dec128_p1_s0_max_positive() {
    assert_dec128(&dec128_i128(1, 0, 9), 9, 1, 0);
}

#[test]
fn dec128_p1_s0_max_negative() {
    assert_dec128(&dec128_i128(1, 0, -9), -9, 1, 0);
}

#[test]
fn dec128_p2_s0_positive() {
    assert_dec128(&dec128_i128(2, 0, 42), 42, 2, 0);
}

#[test]
fn dec128_p2_s0_negative() {
    assert_dec128(&dec128_i128(2, 0, -42), -42, 2, 0);
}

#[test]
fn dec128_p4_s0_1234() {
    assert_dec128(&dec128_i128(4, 0, 1234), 1234, 4, 0);
}

#[test]
fn dec128_p4_s0_neg_1234() {
    assert_dec128(&dec128_i128(4, 0, -1234), -1234, 4, 0);
}

#[test]
fn dec128_p10_s2_positive() {
    // 12.34 -> unscaled 1234, scale 2.
    assert_dec128(&dec128_i128(10, 2, 1234), 1234, 10, 2);
}

#[test]
fn dec128_p10_s2_negative() {
    assert_dec128(&dec128_i128(10, 2, -1234), -1234, 10, 2);
}

#[test]
fn dec128_p10_s0_one() {
    assert_dec128(&dec128_i128(10, 0, 1), 1, 10, 0);
}

#[test]
fn dec128_p10_s0_minus_one() {
    assert_dec128(&dec128_i128(10, 0, -1), -1, 10, 0);
}

#[test]
fn dec128_p9_s0_max() {
    assert_dec128(&dec128_i128(9, 0, 999_999_999), 999_999_999, 9, 0);
}

#[test]
fn dec128_p9_s0_min() {
    assert_dec128(&dec128_i128(9, 0, -999_999_999), -999_999_999, 9, 0);
}

#[test]
fn dec128_p18_s0_max() {
    let v = nines_i128(18);
    assert_dec128(&dec128_i128(18, 0, v), v, 18, 0);
}

#[test]
fn dec128_p18_s6_positive() {
    assert_dec128(
        &dec128_i128(18, 6, 123_456_789_012_345_678),
        123_456_789_012_345_678,
        18,
        6,
    );
}

#[test]
fn dec128_p18_s6_negative() {
    assert_dec128(
        &dec128_i128(18, 6, -123_456_789_012_345_678),
        -123_456_789_012_345_678,
        18,
        6,
    );
}

#[test]
fn dec128_p38_s0_max_magnitude() {
    // 38 nines is the largest unscaled integer representable at precision 38; still fits i128.
    let v = nines_i128(38);
    assert_dec128(&dec128_i128(38, 0, v), v, 38, 0);
}

#[test]
fn dec128_p38_s0_min_magnitude() {
    let v = -nines_i128(38);
    assert_dec128(&dec128_i128(38, 0, v), v, 38, 0);
}

#[test]
fn dec128_p38_s10_positive() {
    let v = nines_i128(30);
    assert_dec128(&dec128_i128(38, 10, v), v, 38, 10);
}

#[test]
fn dec128_p38_s38_scale_eq_precision() {
    // scale == precision: value 0.000...12345
    assert_dec128(&dec128_i128(38, 38, 12345), 12345, 38, 38);
}

#[test]
fn dec128_p37_s0_value() {
    let v = nines_i128(37);
    assert_dec128(&dec128_i128(37, 0, v), v, 37, 0);
}

#[test]
fn dec128_p20_s5_value() {
    assert_dec128(&dec128_i128(20, 5, -7_654_321), -7_654_321, 20, 5);
}

#[test]
fn dec128_p3_s3_scale_eq_precision_small() {
    assert_dec128(&dec128_i128(3, 3, 123), 123, 3, 3);
}

#[test]
fn dec128_sign_extension_neg_one_short_byte() {
    // -1 in a single 0xFF byte must sign-extend to i128 -1.
    assert_dec128(&dec128_bytes(10, 0, vec![0xFF]), -1, 10, 0);
}

#[test]
fn dec128_sign_extension_neg_1234_minimal_bytes() {
    // -1234 minimal two's-complement = 0xFB 0x2E.
    assert_dec128(&dec128_bytes(10, 0, vec![0xFB, 0x2E]), -1234, 10, 0);
}

#[test]
fn dec128_sign_extension_neg_128_single_byte() {
    assert_dec128(&dec128_bytes(10, 0, vec![0x80]), -128, 10, 0);
}

#[test]
fn dec128_nonminimal_positive_leading_zeros() {
    // 1234 with redundant leading zero bytes must decode to 1234.
    assert_dec128(
        &dec128_bytes(10, 0, vec![0x00, 0x00, 0x04, 0xD2]),
        1234,
        10,
        0,
    );
}

#[test]
fn dec128_nonminimal_negative_leading_ff() {
    // -1234 with redundant leading 0xFF sign bytes must decode to -1234.
    assert_dec128(
        &dec128_bytes(10, 0, vec![0xFF, 0xFF, 0xFB, 0x2E]),
        -1234,
        10,
        0,
    );
}

#[test]
fn dec128_zero_single_byte_scale5() {
    assert_dec128(&dec128_bytes(10, 5, vec![0x00]), 0, 10, 5);
}

#[test]
fn dec128_positive_256_two_bytes() {
    // 0x01 0x00 = 256.
    assert_dec128(&dec128_bytes(10, 0, vec![0x01, 0x00]), 256, 10, 0);
}

#[test]
fn dec128_precision_scale_metadata_stable() {
    let b = dec128_i128(12, 4, 42);
    let a = dec128_col(&b);
    assert_eq!(a.precision(), 12);
    assert_eq!(a.scale(), 4);
    assert_eq!(a.value(0), 42);
}

#[test]
fn dec128_p5_s2_negative_boundary() {
    assert_dec128(&dec128_i128(5, 2, -99999), -99999, 5, 2);
}

#[test]
fn dec128_p8_s0_i32_max_range() {
    assert_dec128(&dec128_i128(8, 0, 99_999_999), 99_999_999, 8, 0);
}

#[test]
fn dec128_p38_s2_large_negative() {
    let v = -nines_i128(36);
    assert_dec128(&dec128_i128(38, 2, v), v, 38, 2);
}

#[test]
fn dec128_p15_s7_mixed() {
    assert_dec128(&dec128_i128(15, 7, -1), -1, 15, 7);
}

// ===========================================================================
// Decimal256 (38 < precision <= 76): top-level, native passthrough path
// ===========================================================================

#[test]
fn dec256_p39_s0_small_positive() {
    assert_dec256(
        &dec256_big(39, 0, &BigInt::from(12345)),
        i256_from_i128(12345),
        39,
        0,
    );
}

#[test]
fn dec256_p39_s0_small_negative() {
    assert_dec256(
        &dec256_big(39, 0, &BigInt::from(-12345)),
        i256_from_i128(-12345),
        39,
        0,
    );
}

#[test]
fn dec256_p39_s0_needs_i256() {
    // 39 nines exceeds i128::MAX; must round-trip through the i256 path.
    let v = nines_big(39);
    assert_dec256(&dec256_big(39, 0, &v), i256_of(&v), 39, 0);
}

#[test]
fn dec256_p39_s0_needs_i256_negative() {
    let v = -nines_big(39);
    assert_dec256(&dec256_big(39, 0, &v), i256_of(&v), 39, 0);
}

#[test]
fn dec256_p39_s0_ten_pow_38() {
    // 10^38 fits i128 but the field is Decimal256; verify the boundary crossing.
    let v = pow10_big(38);
    assert_dec256(&dec256_big(39, 0, &v), i256_of(&v), 39, 0);
}

#[test]
fn dec256_p39_s5_value() {
    let v = nines_big(34);
    assert_dec256(&dec256_big(39, 5, &v), i256_of(&v), 39, 5);
}

#[test]
fn dec256_p40_s0_boundary() {
    let v = pow10_big(39);
    assert_dec256(&dec256_big(40, 0, &v), i256_of(&v), 40, 0);
}

#[test]
fn dec256_p50_s10_large_positive() {
    let v = nines_big(45);
    assert_dec256(&dec256_big(50, 10, &v), i256_of(&v), 50, 10);
}

#[test]
fn dec256_p50_s10_large_negative() {
    let v = -nines_big(45);
    assert_dec256(&dec256_big(50, 10, &v), i256_of(&v), 50, 10);
}

#[test]
fn dec256_p60_s0_value() {
    let v = nines_big(55);
    assert_dec256(&dec256_big(60, 0, &v), i256_of(&v), 60, 0);
}

#[test]
fn dec256_p76_s0_max_magnitude() {
    // 76 nines is the largest unscaled integer at precision 76.
    let v = nines_big(76);
    assert_dec256(&dec256_big(76, 0, &v), i256_of(&v), 76, 0);
}

#[test]
fn dec256_p76_s0_min_magnitude() {
    let v = -nines_big(76);
    assert_dec256(&dec256_big(76, 0, &v), i256_of(&v), 76, 0);
}

#[test]
fn dec256_p76_s18_value() {
    let v = nines_big(70);
    assert_dec256(&dec256_big(76, 18, &v), i256_of(&v), 76, 18);
}

#[test]
fn dec256_p76_s76_scale_eq_precision() {
    assert_dec256(
        &dec256_big(76, 76, &BigInt::from(12345)),
        i256_from_i128(12345),
        76,
        76,
    );
}

#[test]
fn dec256_p39_s0_zero() {
    assert_dec256(&dec256_bytes(39, 0, vec![0x00]), i256_from_i128(0), 39, 0);
}

#[test]
fn dec256_p76_s0_zero() {
    assert_dec256(&dec256_bytes(76, 0, vec![0x00]), i256_from_i128(0), 76, 0);
}

#[test]
fn dec256_p39_s0_one() {
    assert_dec256(
        &dec256_big(39, 0, &BigInt::from(1)),
        i256_from_i128(1),
        39,
        0,
    );
}

#[test]
fn dec256_p39_s0_minus_one() {
    assert_dec256(
        &dec256_big(39, 0, &BigInt::from(-1)),
        i256_from_i128(-1),
        39,
        0,
    );
}

#[test]
fn dec256_sign_extension_neg_one_short_byte() {
    // -1 in a single 0xFF byte must sign-extend to i256 -1.
    assert_dec256(&dec256_bytes(39, 0, vec![0xFF]), i256_from_i128(-1), 39, 0);
}

#[test]
fn dec256_sign_extension_neg_1234_minimal() {
    assert_dec256(
        &dec256_bytes(39, 0, vec![0xFB, 0x2E]),
        i256_from_i128(-1234),
        39,
        0,
    );
}

#[test]
fn dec256_nonminimal_positive_leading_zeros() {
    assert_dec256(
        &dec256_bytes(39, 0, vec![0x00, 0x00, 0x04, 0xD2]),
        i256_from_i128(1234),
        39,
        0,
    );
}

#[test]
fn dec256_nonminimal_negative_leading_ff() {
    assert_dec256(
        &dec256_bytes(39, 0, vec![0xFF, 0xFF, 0xFB, 0x2E]),
        i256_from_i128(-1234),
        39,
        0,
    );
}

#[test]
fn dec256_value_just_above_i128_max() {
    // i128::MAX + 1 = 2^127, must be exact in i256.
    let two_pow_127: BigInt = "170141183460469231731687303715884105728".parse().unwrap();
    assert_dec256(
        &dec256_big(50, 0, &two_pow_127),
        i256_of(&two_pow_127),
        50,
        0,
    );
}

#[test]
fn dec256_value_negative_below_i128_min() {
    let neg: BigInt = "-170141183460469231731687303715884105729".parse().unwrap();
    assert_dec256(&dec256_big(50, 0, &neg), i256_of(&neg), 50, 0);
}

#[test]
fn dec256_p50_s0_two_pow_200() {
    // 2^200, exercises a wide (>16 byte) positive magnitude through the i256 path.
    let v: BigInt = "1606938044258990275541962092341162602522202993782792835301376"
        .parse()
        .unwrap();
    assert_dec256(&dec256_big(70, 0, &v), i256_of(&v), 70, 0);
}

#[test]
fn dec256_precision_scale_metadata_stable() {
    let b = dec256_big(41, 20, &BigInt::from(999));
    let a = dec256_col(&b);
    assert_eq!(a.precision(), 41);
    assert_eq!(a.scale(), 20);
    assert_eq!(a.value(0), i256_from_i128(999));
}

#[test]
fn dec256_p45_s0_negative_power_of_ten() {
    let v = -pow10_big(40);
    assert_dec256(&dec256_big(45, 0, &v), i256_of(&v), 45, 0);
}

#[test]
fn dec256_p76_s0_large_power_of_ten() {
    let v = pow10_big(70);
    assert_dec256(&dec256_big(76, 0, &v), i256_of(&v), 76, 0);
}

#[test]
fn dec256_p41_s20_negative() {
    let v = -nines_big(35);
    assert_dec256(&dec256_big(41, 20, &v), i256_of(&v), 41, 20);
}

// ===========================================================================
// Precision boundaries and family membership (oracle checks)
// ===========================================================================

fn target_field0_type(schema_json: &str) -> DataType {
    let schema = AvroWriterSchema::parse_str(schema_json).unwrap();
    convert_avro_schema_to_arrow(schema)
        .field(0)
        .data_type()
        .clone()
}

#[test]
fn boundary_p38_maps_to_decimal128() {
    assert_eq!(
        target_field0_type(&single_decimal_schema(38, 0)),
        DataType::Decimal128(38, 0)
    );
}

#[test]
fn boundary_p39_maps_to_decimal256() {
    assert_eq!(
        target_field0_type(&single_decimal_schema(39, 0)),
        DataType::Decimal256(39, 0)
    );
}

#[test]
fn boundary_p76_maps_to_decimal256() {
    assert_eq!(
        target_field0_type(&single_decimal_schema(76, 0)),
        DataType::Decimal256(76, 0)
    );
}

#[test]
fn boundary_p77_scale0_leaves_decimal_family() {
    // precision 77, scale 0 -> u256 FixedSizeBinary(32), NOT a decimal type.
    let dt = target_field0_type(&single_decimal_schema(77, 0));
    assert!(
        matches!(dt, DataType::FixedSizeBinary(32)),
        "p77 s0 must map to FixedSizeBinary(32), got {dt:?}"
    );
    assert!(
        !matches!(dt, DataType::Decimal128(..) | DataType::Decimal256(..)),
        "p77 s0 must NOT be a decimal type"
    );
}

#[test]
fn boundary_p77_scale1_maps_to_utf8() {
    // precision 77, scale > 0 -> Utf8 (scaled high-precision decimal), NOT a decimal type.
    let dt = target_field0_type(&single_decimal_schema(77, 1));
    assert_eq!(dt, DataType::Utf8, "p77 s1 must map to Utf8");
    assert!(!matches!(
        dt,
        DataType::Decimal128(..) | DataType::Decimal256(..)
    ));
}

#[test]
fn boundary_p1_maps_to_decimal128() {
    assert_eq!(
        target_field0_type(&single_decimal_schema(1, 0)),
        DataType::Decimal128(1, 0)
    );
}

#[test]
fn boundary_p38_s38_maps_to_decimal128() {
    assert_eq!(
        target_field0_type(&single_decimal_schema(38, 38)),
        DataType::Decimal128(38, 38)
    );
}

#[test]
fn boundary_p76_s76_maps_to_decimal256() {
    assert_eq!(
        target_field0_type(&single_decimal_schema(76, 76)),
        DataType::Decimal256(76, 76)
    );
}

#[test]
fn oracle_decoded_schema_matches_convert_dec128() {
    let sj = single_decimal_schema(20, 4);
    let expected = target_field0_type(&sj);
    let b = dec128_i128(20, 4, 12345);
    assert_eq!(b.schema().field(0).data_type(), &expected);
    assert_eq!(expected, DataType::Decimal128(20, 4));
}

#[test]
fn oracle_decoded_schema_matches_convert_dec256() {
    let sj = single_decimal_schema(45, 6);
    let expected = target_field0_type(&sj);
    let b = dec256_big(45, 6, &BigInt::from(777));
    assert_eq!(b.schema().field(0).data_type(), &expected);
    assert_eq!(expected, DataType::Decimal256(45, 6));
}

// ===========================================================================
// Nested Decimal128 (record / array): passthrough (p<=38) and binary-reinterpret (p>76)
// ===========================================================================

fn struct_decimal_schema(p: u64, s: u64) -> String {
    // R { inner: Inner { amt: decimal(p,s) } }
    record_type_json(
        "R",
        "inner",
        &record_type_json("Inner", "amt", &decimal_type(p, s)),
    )
}

fn array_decimal_schema(p: u64, s: u64) -> String {
    // R { vals: array<decimal(p,s)> }
    record_type_json("R", "vals", &array_field_json(&decimal_type(p, s)))
}

fn array_struct_amt_schema(p: u64, s: u64) -> String {
    // R { xfers: array< X { amt: decimal(p,s) } > }  (the traces "amt" shape)
    record_type_json(
        "R",
        "xfers",
        &array_field_json(&record_type_json("X", "amt", &decimal_type(p, s))),
    )
}

fn struct_amt(bytes: Vec<u8>) -> Value {
    Value::Record(vec![("amt".to_string(), dec_val(bytes))])
}

fn nested_struct_amt(b: &RecordBatch) -> &Decimal128Array {
    let st = b
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("inner is a Struct");
    // leak via transmute-free: return by re-borrowing through a helper isn't possible; instead
    // assert here is done by callers using this reference immediately.
    st.column_by_name("amt")
        .expect("amt field")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("amt is Decimal128Array")
}

#[test]
fn nested_struct_decimal128_native_p10_s2() {
    let b = decode_record(
        &struct_decimal_schema(10, 2),
        vec![("inner", struct_amt(be(1234)))],
    );
    let inner = b.schema().field(0).data_type().clone();
    let DataType::Struct(fields) = inner else {
        panic!("inner not a struct: {inner:?}");
    };
    let amt = fields.iter().find(|f| f.name() == "amt").unwrap();
    assert_eq!(amt.data_type(), &DataType::Decimal128(10, 2));
    let col = nested_struct_amt(&b);
    assert_eq!(col.value(0), 1234);
}

#[test]
fn nested_struct_decimal128_native_negative() {
    let b = decode_record(
        &struct_decimal_schema(18, 4),
        vec![("inner", struct_amt(be(-98765)))],
    );
    assert_eq!(nested_struct_amt(&b).value(0), -98765);
}

#[test]
fn nested_struct_decimal128_native_zero() {
    let b = decode_record(
        &struct_decimal_schema(10, 0),
        vec![("inner", struct_amt(vec![0x00]))],
    );
    assert_eq!(nested_struct_amt(&b).value(0), 0);
}

#[test]
fn nested_struct_decimal128_binary_path_p100_s0_value() {
    // precision 100 -> stripped to bytes, decoded as Binary, reinterpreted to Decimal128(100,0).
    let b = decode_record(
        &struct_decimal_schema(100, 0),
        vec![("inner", struct_amt(be(1234)))],
    );
    let inner = b.schema().field(0).data_type().clone();
    let DataType::Struct(fields) = inner else {
        panic!("inner not a struct");
    };
    let amt = fields.iter().find(|f| f.name() == "amt").unwrap();
    assert_eq!(amt.data_type(), &DataType::Decimal128(100, 0));
    assert_eq!(nested_struct_amt(&b).value(0), 1234);
}

#[test]
fn nested_struct_decimal128_binary_path_p100_s0_negative() {
    let b = decode_record(
        &struct_decimal_schema(100, 0),
        vec![("inner", struct_amt(be(-1234)))],
    );
    assert_eq!(nested_struct_amt(&b).value(0), -1234);
}

#[test]
fn nested_struct_decimal128_binary_path_p100_s0_zero() {
    let b = decode_record(
        &struct_decimal_schema(100, 0),
        vec![("inner", struct_amt(vec![0x00]))],
    );
    assert_eq!(nested_struct_amt(&b).value(0), 0);
}

#[test]
fn nested_struct_decimal128_binary_path_sign_extension_short_bytes() {
    // -1 as a single 0xFF byte through the nested binary reinterpret path.
    let b = decode_record(
        &struct_decimal_schema(100, 0),
        vec![("inner", struct_amt(vec![0xFF]))],
    );
    assert_eq!(nested_struct_amt(&b).value(0), -1);
}

#[test]
fn nested_array_decimal128_native_multi() {
    let arr = Value::Array(vec![dec_val(be(1)), dec_val(be(2)), dec_val(be(-3))]);
    let b = decode_record(&array_decimal_schema(10, 0), vec![("vals", arr)]);
    let list = b.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let vals = list
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("element Decimal128Array");
    assert_eq!(vals.len(), 3);
    assert_eq!(vals.value(0), 1);
    assert_eq!(vals.value(1), 2);
    assert_eq!(vals.value(2), -3);
}

#[test]
fn nested_array_decimal128_native_element_type() {
    let arr = Value::Array(vec![dec_val(be(500))]);
    let b = decode_record(&array_decimal_schema(12, 3), vec![("vals", arr)]);
    let DataType::List(elem) = b.schema().field(0).data_type().clone() else {
        panic!("vals not a List");
    };
    assert_eq!(elem.data_type(), &DataType::Decimal128(12, 3));
}

#[test]
fn nested_array_decimal128_binary_path_p100() {
    let arr = Value::Array(vec![dec_val(be(1234)), dec_val(be(-5678))]);
    let b = decode_record(&array_decimal_schema(100, 0), vec![("vals", arr)]);
    let list = b.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let vals = list
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(vals.value(0), 1234);
    assert_eq!(vals.value(1), -5678);
}

#[test]
fn nested_array_decimal128_empty_array() {
    let b = decode_record(
        &array_decimal_schema(10, 0),
        vec![("vals", Value::Array(vec![]))],
    );
    let list = b.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list.value(0).len(), 0, "empty inner array");
}

#[test]
fn nested_array_struct_amt_shape_binary_p100() {
    // Full traces shape: array< record { amt: decimal(100,0) } >.
    let xfers = Value::Array(vec![struct_amt(be(1234)), struct_amt(be(-1))]);
    let b = decode_record(&array_struct_amt_schema(100, 0), vec![("xfers", xfers)]);
    let list = b.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let st = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amt.value(0), 1234);
    assert_eq!(amt.value(1), -1);
}

#[test]
fn nested_array_struct_amt_shape_native_p18() {
    let xfers = Value::Array(vec![struct_amt(be(777)), struct_amt(be(888))]);
    let b = decode_record(&array_struct_amt_schema(18, 0), vec![("xfers", xfers)]);
    let list = b.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let st = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amt.value(0), 777);
    assert_eq!(amt.value(1), 888);
}

#[test]
fn nested_struct_in_struct_decimal128() {
    // R { outer: Outer { inner: Inner { amt: decimal(10,2) } } }
    let schema = record_type_json(
        "R",
        "outer",
        &record_type_json(
            "Outer",
            "inner",
            &record_type_json("Inner", "amt", &decimal_type(10, 2)),
        ),
    );
    let inner = Value::Record(vec![("amt".to_string(), dec_val(be(4242)))]);
    let outer = Value::Record(vec![("inner".to_string(), inner)]);
    let b = decode_record(&schema, vec![("outer", outer)]);
    let ostruct = b.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    let istruct = ostruct
        .column_by_name("inner")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let amt = istruct
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amt.value(0), 4242);
}

// --- Adversarial nested cases: 38 < precision <= 76 nested decimal.
// The nested target is Decimal128(p,s) (nested always maps to Decimal128), but arrow-avro decodes
// a precision-in-(38,76] decimal as Decimal256, so coerce_array falls to `cast(Decimal256 ->
// Decimal128(p,s))` with p > 38 (an out-of-range Decimal128 precision). Vendored behavior mapped
// nested decimals to i128 directly, so the CORRECT unscaled value is the small i128 below. If the
// cast rejects the >38 precision (or drops precision), these fail — a real coercion finding.

#[test]
fn nested_struct_decimal_p50_adversarial_cast() {
    let b = decode_record(
        &struct_decimal_schema(50, 0),
        vec![("inner", struct_amt(be(12345)))],
    );
    // Nested target is Decimal128(50,0); vendored contract is the i128 unscaled value 12345.
    assert_eq!(nested_struct_amt(&b).value(0), 12345);
}

#[test]
fn nested_array_decimal_p40_adversarial_cast() {
    let arr = Value::Array(vec![dec_val(be(99)), dec_val(be(-100))]);
    let b = decode_record(&array_decimal_schema(40, 0), vec![("vals", arr)]);
    let list = b.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let vals = list
        .values()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(vals.value(0), 99);
    assert_eq!(vals.value(1), -100);
}

// ===========================================================================
// Multi-row batches and nullable decimal columns
// ===========================================================================

#[test]
fn multi_row_decimal128_values() {
    let rows = vec![
        vec![("v", dec_val(be(10)))],
        vec![("v", dec_val(be(20)))],
        vec![("v", dec_val(be(30)))],
    ];
    let b = decode_rows(&single_decimal_schema(10, 0), rows);
    let a = dec128_col(&b);
    assert_eq!(a.len(), 3);
    assert_eq!(a.value(0), 10);
    assert_eq!(a.value(1), 20);
    assert_eq!(a.value(2), 30);
}

#[test]
fn multi_row_decimal128_negatives_and_zero() {
    let rows = vec![
        vec![("v", dec_val(be(-1)))],
        vec![("v", dec_val(vec![0x00]))],
        vec![("v", dec_val(be(1)))],
    ];
    let b = decode_rows(&single_decimal_schema(12, 3), rows);
    let a = dec128_col(&b);
    assert_eq!(a.value(0), -1);
    assert_eq!(a.value(1), 0);
    assert_eq!(a.value(2), 1);
}

#[test]
fn multi_row_decimal128_max_and_min_same_batch() {
    let mx = nines_i128(38);
    let rows = vec![vec![("v", dec_val(be(mx)))], vec![("v", dec_val(be(-mx)))]];
    let b = decode_rows(&single_decimal_schema(38, 0), rows);
    let a = dec128_col(&b);
    assert_eq!(a.value(0), mx);
    assert_eq!(a.value(1), -mx);
}

#[test]
fn multi_row_decimal256_values() {
    let a = nines_big(76);
    let z = BigInt::from(0);
    let n = -nines_big(50);
    let rows = vec![
        vec![("v", dec_val(be_big(&a)))],
        vec![("v", dec_val(vec![0x00]))],
        vec![("v", dec_val(be_big(&n)))],
    ];
    let b = decode_rows(&single_decimal_schema(76, 0), rows);
    let col = dec256_col(&b);
    assert_eq!(col.value(0), i256_of(&a));
    assert_eq!(col.value(1), i256_of(&z));
    assert_eq!(col.value(2), i256_of(&n));
}

#[test]
fn multi_row_decimal256_sign_mix() {
    let rows = vec![
        vec![("v", dec_val(be_big(&BigInt::from(5))))],
        vec![("v", dec_val(be_big(&BigInt::from(-5))))],
    ];
    let b = decode_rows(&single_decimal_schema(45, 2), rows);
    let col = dec256_col(&b);
    assert_eq!(col.value(0), i256_from_i128(5));
    assert_eq!(col.value(1), i256_from_i128(-5));
}

#[test]
fn multi_row_decimal128_precision_scale_stable_across_rows() {
    let rows = vec![vec![("v", dec_val(be(1)))], vec![("v", dec_val(be(2)))]];
    let b = decode_rows(&single_decimal_schema(22, 7), rows);
    let a = dec128_col(&b);
    assert_eq!(a.precision(), 22);
    assert_eq!(a.scale(), 7);
    assert_eq!(a.len(), 2);
}

#[test]
fn multi_row_decimal128_row_count_matches() {
    let rows: Vec<Vec<(&str, Value)>> = (0..5)
        .map(|i| vec![("v", dec_val(be(i as i128)))])
        .collect();
    let b = decode_rows(&single_decimal_schema(10, 0), rows);
    assert_eq!(b.num_rows(), 5);
    let a = dec128_col(&b);
    for i in 0..5 {
        assert_eq!(a.value(i), i as i128);
    }
}

fn nullable_decimal_schema(p: u64, s: u64) -> String {
    record_type_json("R", "v", &format!(r#"["null",{}]"#, decimal_type(p, s)))
}

fn dec_present(bytes: Vec<u8>) -> Value {
    Value::Union(1, Box::new(dec_val(bytes)))
}

fn dec_null() -> Value {
    Value::Union(0, Box::new(Value::Null))
}

#[test]
fn nullable_decimal128_value_row() {
    let b = decode_record(
        &nullable_decimal_schema(10, 2),
        vec![("v", dec_present(be(555)))],
    );
    assert!(b.schema().field(0).is_nullable());
    let a = dec128_col(&b);
    assert!(!a.is_null(0));
    assert_eq!(a.value(0), 555);
}

#[test]
fn nullable_decimal128_null_row() {
    let b = decode_record(&nullable_decimal_schema(10, 2), vec![("v", dec_null())]);
    let a = dec128_col(&b);
    assert!(a.is_null(0), "null union branch must decode to null");
}

#[test]
fn nullable_decimal128_mixed_null_and_value() {
    let rows = vec![
        vec![("v", dec_present(be(100)))],
        vec![("v", dec_null())],
        vec![("v", dec_present(be(-200)))],
    ];
    let b = decode_rows(&nullable_decimal_schema(15, 3), rows);
    let a = dec128_col(&b);
    assert_eq!(a.value(0), 100);
    assert!(a.is_null(1));
    assert_eq!(a.value(2), -200);
}

#[test]
fn nullable_decimal128_two_nulls_then_value() {
    let rows = vec![
        vec![("v", dec_null())],
        vec![("v", dec_null())],
        vec![("v", dec_present(be(9)))],
    ];
    let b = decode_rows(&nullable_decimal_schema(10, 0), rows);
    let a = dec128_col(&b);
    assert!(a.is_null(0));
    assert!(a.is_null(1));
    assert_eq!(a.value(2), 9);
}

#[test]
fn nullable_decimal128_all_null_column() {
    let rows = vec![vec![("v", dec_null())], vec![("v", dec_null())]];
    let b = decode_rows(&nullable_decimal_schema(10, 0), rows);
    let a = dec128_col(&b);
    assert_eq!(a.null_count(), 2);
}

#[test]
fn nullable_decimal256_value_row() {
    let v = nines_big(60);
    let b = decode_record(
        &nullable_decimal_schema(70, 0),
        vec![("v", dec_present(be_big(&v)))],
    );
    assert!(b.schema().field(0).is_nullable());
    let a = dec256_col(&b);
    assert!(!a.is_null(0));
    assert_eq!(a.value(0), i256_of(&v));
}

#[test]
fn nullable_decimal256_mixed() {
    let big = nines_big(50);
    let rows = vec![
        vec![("v", dec_present(be_big(&big)))],
        vec![("v", dec_null())],
        vec![("v", dec_present(be_big(&(-big.clone()))))],
    ];
    let b = decode_rows(&nullable_decimal_schema(55, 0), rows);
    let a = dec256_col(&b);
    assert_eq!(a.value(0), i256_of(&big));
    assert!(a.is_null(1));
    assert_eq!(a.value(2), i256_of(&(-big)));
}

#[test]
fn decimal256_boundary_p39_batch_values() {
    let a = nines_big(39);
    let rows = vec![
        vec![("v", dec_val(be_big(&a)))],
        vec![("v", dec_val(vec![0x00]))],
    ];
    let b = decode_rows(&single_decimal_schema(39, 0), rows);
    let col = dec256_col(&b);
    assert_eq!(col.value(0), i256_of(&a));
    assert_eq!(col.value(1), i256_from_i128(0));
}

#[test]
fn decimal128_two_field_record_both_decimals() {
    // Two decimal fields of different widths in one record; verify independent coercion.
    let schema = format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"a","type":{}}},{{"name":"b","type":{}}}]}}"#,
        decimal_type(10, 2),
        decimal_type(20, 5)
    );
    let b = decode_record(
        &schema,
        vec![("a", dec_val(be(1234))), ("b", dec_val(be(-99)))],
    );
    let a_col = b
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let b_col = b
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(a_col.value(0), 1234);
    assert_eq!(a_col.scale(), 2);
    assert_eq!(b_col.value(0), -99);
    assert_eq!(b_col.scale(), 5);
}

#[test]
fn decimal_mixed_dec128_and_dec256_in_record() {
    let schema = format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"small","type":{}}},{{"name":"big","type":{}}}]}}"#,
        decimal_type(18, 4),
        decimal_type(60, 8)
    );
    let big = nines_big(55);
    let b = decode_record(
        &schema,
        vec![("small", dec_val(be(4242))), ("big", dec_val(be_big(&big)))],
    );
    let small = b
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let bigc = b
        .column(1)
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();
    assert_eq!(small.value(0), 4242);
    assert_eq!(bigc.value(0), i256_of(&big));
    assert_eq!(
        b.schema().field(0).data_type(),
        &DataType::Decimal128(18, 4)
    );
    assert_eq!(
        b.schema().field(1).data_type(),
        &DataType::Decimal256(60, 8)
    );
}
