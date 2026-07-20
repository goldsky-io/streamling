//! Adversarial integration tests for the u256 / i256 high-precision integer decimal
//! reinterpretation path in `arrow_avro.rs`.
//!
//! Area: top-level avro `decimal` with precision > 76 and scale 0 -> u256 (unsigned) /
//! i256 (signed) `FixedSizeBinary(32)`. Exercises:
//!   * `u256_be_bytes` / `i256_be_bytes` directly with hand-built byte slices (exact [u8;32]);
//!   * the schema oracle (`convert_avro_schema_to_arrow`) at the precision boundary (76 vs 77 vs 100);
//!   * `coerce_batch_to_target` reinterpreting `Binary` columns into u256/i256 FixedSizeBinary(32);
//!   * end-to-end `ConfluentAvroDecoder` decode of avro decimals into u256.
//!
//! Assertions target the CORRECT value derived from the two's-complement / big-endian contract,
//! not whatever the code happens to produce.

use std::collections::HashMap;
use std::sync::Arc;

use apache_avro::Decimal;
use apache_avro::types::{Record, Value};
use apache_avro::{Schema as AvroWriterSchema, to_avro_datum};
use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use streamling_common::formats::avro::arrow_avro::{
    ConfluentAvroDecoder, coerce_batch_to_target, i256_be_bytes, u256_be_bytes,
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

/// Independent reference: unsigned big-endian, right-aligned, zero-padded to 32 bytes.
/// Valid oracle only for inputs that are NOT error cases (fit unsigned in 32 bytes).
fn zpad(bytes: &[u8]) -> [u8; 32] {
    let mut a = [0u8; 32];
    let n = bytes.len().min(32);
    a[32 - n..].copy_from_slice(&bytes[bytes.len() - n..]);
    a
}

/// Independent reference: sign-extended two's-complement, right-aligned to 32 bytes.
/// Valid oracle only for non-error inputs (fit signed in 32 bytes).
fn sext(bytes: &[u8]) -> [u8; 32] {
    let neg = !bytes.is_empty() && (bytes[0] & 0x80) != 0;
    let fill = if neg { 0xFFu8 } else { 0x00 };
    let mut a = [fill; 32];
    let n = bytes.len().min(32);
    a[32 - n..].copy_from_slice(&bytes[bytes.len() - n..]);
    a
}

const EXT_META_KEY: &str = "ARROW:extension:name";

fn u256_meta() -> HashMap<String, String> {
    HashMap::from([(EXT_META_KEY.to_string(), "streamling.u256".to_string())])
}

fn i256_meta() -> HashMap<String, String> {
    HashMap::from([(EXT_META_KEY.to_string(), "streamling.i256".to_string())])
}

/// Build a single-column `Binary`/`LargeBinary` batch and coerce it against a
/// `FixedSizeBinary(32)` target carrying the given extension metadata. Returns the coerced
/// FixedSizeBinaryArray, or a stringified error (the crate error type is opaque here).
fn coerce_binary(
    rows: Vec<Option<Vec<u8>>>,
    meta: HashMap<String, String>,
    large: bool,
) -> std::result::Result<FixedSizeBinaryArray, String> {
    let refs: Vec<Option<&[u8]>> = rows.iter().map(|o| o.as_deref()).collect();
    let (input, in_dt): (Arc<dyn Array>, DataType) = if large {
        let a: LargeBinaryArray = refs.into_iter().collect();
        (Arc::new(a) as Arc<dyn Array>, DataType::LargeBinary)
    } else {
        let a: BinaryArray = refs.into_iter().collect();
        (Arc::new(a) as Arc<dyn Array>, DataType::Binary)
    };
    let in_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("v", in_dt, true)]));
    let batch = RecordBatch::try_new(in_schema, vec![input]).map_err(|e| format!("{e:?}"))?;

    let tgt_field = Field::new("v", DataType::FixedSizeBinary(32), true).with_metadata(meta);
    let target: SchemaRef = Arc::new(Schema::new(vec![tgt_field]));
    let out = coerce_batch_to_target(&batch, &target).map_err(|e| format!("{e:?}"))?;
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("coerced column must be FixedSizeBinary(32)")
        .clone();
    Ok(col)
}

// Convenience: coerce one non-null value and unwrap to its 32 bytes.
fn coerce_one_u256(bytes: &[u8]) -> std::result::Result<[u8; 32], String> {
    let col = coerce_binary(vec![Some(bytes.to_vec())], u256_meta(), false)?;
    let mut out = [0u8; 32];
    out.copy_from_slice(col.value(0));
    Ok(out)
}

fn coerce_one_i256(bytes: &[u8]) -> std::result::Result<[u8; 32], String> {
    let col = coerce_binary(vec![Some(bytes.to_vec())], i256_meta(), false)?;
    let mut out = [0u8; 32];
    out.copy_from_slice(col.value(0));
    Ok(out)
}

// ===========================================================================
// SECTION 1: u256_be_bytes direct
// ===========================================================================

#[test]
fn u256_empty_slice_is_zero() {
    assert_eq!(u256_be_bytes(&[]).unwrap(), [0u8; 32]);
}

#[test]
fn u256_single_zero_byte_is_zero() {
    assert_eq!(u256_be_bytes(&[0x00]).unwrap(), [0u8; 32]);
}

#[test]
fn u256_single_one() {
    assert_eq!(u256_be_bytes(&[0x01]).unwrap(), zpad(&[0x01]));
}

#[test]
fn u256_single_max_positive_byte_0x7f() {
    assert_eq!(u256_be_bytes(&[0x7F]).unwrap(), zpad(&[0x7F]));
}

#[test]
fn u256_single_0x80_high_bit_set_is_error() {
    // Top bit set on the leading byte => treated as negative => rejected.
    assert!(u256_be_bytes(&[0x80]).is_err());
}

#[test]
fn u256_single_0xff_is_error() {
    assert!(u256_be_bytes(&[0xFF]).is_err());
}

#[test]
fn u256_two_bytes() {
    assert_eq!(u256_be_bytes(&[0x01, 0x02]).unwrap(), zpad(&[0x01, 0x02]));
}

#[test]
fn u256_leading_zero_two_bytes() {
    assert_eq!(u256_be_bytes(&[0x00, 0x01]).unwrap(), zpad(&[0x01]));
}

#[test]
fn u256_31_bytes_left_padded() {
    let input = [0x01u8; 31];
    let got = u256_be_bytes(&input).unwrap();
    let mut expected = [0x01u8; 32];
    expected[0] = 0x00;
    assert_eq!(got, expected);
}

#[test]
fn u256_exactly_32_zero_bytes() {
    assert_eq!(u256_be_bytes(&[0u8; 32]).unwrap(), [0u8; 32]);
}

#[test]
fn u256_exactly_32_bytes_top_bit_clear_roundtrips() {
    let mut input = [0u8; 32];
    input[0] = 0x7F;
    input[31] = 0xFF;
    input[15] = 0xAB;
    assert_eq!(u256_be_bytes(&input).unwrap(), input);
}

#[test]
fn u256_32_bytes_top_bit_set_is_error() {
    // A bare 32-byte value with the high bit set is a NEGATIVE two's-complement number => error.
    let mut input = [0u8; 32];
    input[0] = 0x80;
    assert!(u256_be_bytes(&input).is_err());
}

#[test]
fn u256_32_bytes_all_ff_is_error() {
    // All 0xFF (=-1 signed) must be rejected for u256; the max must arrive as 33 bytes.
    assert!(u256_be_bytes(&[0xFF; 32]).is_err());
}

#[test]
fn u256_max_via_33_bytes_leading_zero() {
    // Canonical positive encoding of 2^256-1: a leading 0x00 sign byte + 32×0xFF.
    let mut input = vec![0x00u8];
    input.extend_from_slice(&[0xFF; 32]);
    assert_eq!(u256_be_bytes(&input).unwrap(), [0xFF; 32]);
}

#[test]
fn u256_33_bytes_all_zero_strips_to_zero() {
    assert_eq!(u256_be_bytes(&[0u8; 33]).unwrap(), [0u8; 32]);
}

#[test]
fn u256_40_bytes_all_zero_strips_to_zero() {
    assert_eq!(u256_be_bytes(&[0u8; 40]).unwrap(), [0u8; 32]);
}

#[test]
fn u256_33_bytes_leading_zero_then_value() {
    // [0x00, 0x01, 0x00×31] -> strip the leading 0x00 -> [0x01, 0x00×31].
    let mut input = vec![0x00u8, 0x01];
    input.extend_from_slice(&[0x00; 31]);
    let mut expected = [0u8; 32];
    expected[0] = 0x01;
    assert_eq!(u256_be_bytes(&input).unwrap(), expected);
}

#[test]
fn u256_33_bytes_leading_zero_then_high_bit_value_ok() {
    // [0x00, 0x80, 0×31]: leading 0x00 stripped; result high bit set is FINE for unsigned.
    // (Contrast with i256, where the same magnitude overflows signed range.)
    let mut input = vec![0x00u8, 0x80];
    input.extend_from_slice(&[0x00; 31]);
    let mut expected = [0u8; 32];
    expected[0] = 0x80;
    assert_eq!(u256_be_bytes(&input).unwrap(), expected);
}

#[test]
fn u256_34_bytes_two_leading_zeros_stripped() {
    let mut input = vec![0x00u8, 0x00, 0x01];
    input.extend_from_slice(&[0x00; 31]);
    let mut expected = [0u8; 32];
    expected[0] = 0x01;
    assert_eq!(u256_be_bytes(&input).unwrap(), expected);
}

#[test]
fn u256_33_bytes_no_leading_zero_is_error() {
    // First byte 0x01 (top bit clear, not negative) but 33 significant bytes => too large.
    let mut input = vec![0x01u8];
    input.extend_from_slice(&[0x00; 32]);
    assert!(u256_be_bytes(&input).is_err());
}

#[test]
fn u256_33_bytes_leading_zero_but_still_oversized_after_one_strip_ok() {
    // [0x00, 0x7F, 0xFF×31] -> strip one 0x00 -> exactly 32 bytes, top bit clear -> ok.
    let mut input = vec![0x00u8, 0x7F];
    input.extend_from_slice(&[0xFF; 31]);
    let mut expected = [0xFFu8; 32];
    expected[0] = 0x7F;
    assert_eq!(u256_be_bytes(&input).unwrap(), expected);
}

#[test]
fn u256_64_bytes_all_zero_ok() {
    assert_eq!(u256_be_bytes(&[0u8; 64]).unwrap(), [0u8; 32]);
}

#[test]
fn u256_64_bytes_only_low_bytes_set_ok() {
    // Many leading zeros with a small value at the very end.
    let mut input = vec![0u8; 63];
    input.push(0x2A); // 42
    assert_eq!(u256_be_bytes(&input).unwrap(), zpad(&[0x2A]));
}

#[test]
fn u256_mid_magnitude_value() {
    let mut input = [0u8; 32];
    input[0] = 0x12;
    input[1] = 0x34;
    input[30] = 0xAB;
    input[31] = 0xCD;
    assert_eq!(u256_be_bytes(&input).unwrap(), input);
}

#[test]
fn u256_16_bytes_value() {
    let input = [0x11u8; 16];
    let got = u256_be_bytes(&input).unwrap();
    let mut expected = [0u8; 32];
    expected[16..].copy_from_slice(&input);
    assert_eq!(got, expected);
}

#[test]
fn u256_33_bytes_leading_zero_zero_then_nonzero_is_error() {
    // [0x00, 0x01, 0xFF×31]: strip one 0x00 -> 32 bytes [0x01, 0xFF×31], top bit clear -> ok,
    // value fits. Assert exact result.
    let mut input = vec![0x00u8, 0x01];
    input.extend_from_slice(&[0xFF; 31]);
    let mut expected = [0xFFu8; 32];
    expected[0] = 0x01;
    assert_eq!(u256_be_bytes(&input).unwrap(), expected);
}

// ===========================================================================
// SECTION 2: i256_be_bytes direct
// ===========================================================================

#[test]
fn i256_empty_slice_is_zero() {
    assert_eq!(i256_be_bytes(&[]).unwrap(), [0u8; 32]);
}

#[test]
fn i256_single_zero() {
    assert_eq!(i256_be_bytes(&[0x00]).unwrap(), [0u8; 32]);
}

#[test]
fn i256_single_one() {
    assert_eq!(i256_be_bytes(&[0x01]).unwrap(), sext(&[0x01]));
}

#[test]
fn i256_single_0x7f_positive() {
    assert_eq!(i256_be_bytes(&[0x7F]).unwrap(), sext(&[0x7F]));
}

#[test]
fn i256_neg_one_all_ff() {
    // 0xFF = -1 => sign-extends to all 0xFF.
    assert_eq!(i256_be_bytes(&[0xFF]).unwrap(), [0xFF; 32]);
}

#[test]
fn i256_neg_128() {
    // 0x80 = -128 => 0xFF×31 then 0x80.
    let mut expected = [0xFFu8; 32];
    expected[31] = 0x80;
    assert_eq!(i256_be_bytes(&[0x80]).unwrap(), expected);
}

#[test]
fn i256_neg_two() {
    let mut expected = [0xFFu8; 32];
    expected[31] = 0xFE;
    assert_eq!(i256_be_bytes(&[0xFE]).unwrap(), expected);
}

#[test]
fn i256_positive_128_via_two_bytes() {
    // [0x00, 0x80] = +128 (leading 0x00 keeps it positive).
    let mut expected = [0u8; 32];
    expected[31] = 0x80;
    assert_eq!(i256_be_bytes(&[0x00, 0x80]).unwrap(), expected);
}

#[test]
fn i256_neg_32768_via_two_bytes() {
    // [0x80, 0x00] = -32768.
    let mut expected = [0xFFu8; 32];
    expected[30] = 0x80;
    expected[31] = 0x00;
    assert_eq!(i256_be_bytes(&[0x80, 0x00]).unwrap(), expected);
}

#[test]
fn i256_neg_two_two_bytes() {
    // [0xFF, 0xFE] = -2.
    let mut expected = [0xFFu8; 32];
    expected[31] = 0xFE;
    assert_eq!(i256_be_bytes(&[0xFF, 0xFE]).unwrap(), expected);
}

#[test]
fn i256_32_bytes_all_ff_is_neg_one() {
    // Unlike u256, all-0xFF is a VALID i256 (=-1).
    assert_eq!(i256_be_bytes(&[0xFF; 32]).unwrap(), [0xFF; 32]);
}

#[test]
fn i256_min_value_2pow255() {
    // [0x80, 0×31] = -2^255 (INT256_MIN).
    let mut input = [0u8; 32];
    input[0] = 0x80;
    assert_eq!(i256_be_bytes(&input).unwrap(), input);
}

#[test]
fn i256_max_value_2pow255_minus_1() {
    // [0x7F, 0xFF×31] = 2^255-1 (INT256_MAX).
    let mut input = [0xFFu8; 32];
    input[0] = 0x7F;
    assert_eq!(i256_be_bytes(&input).unwrap(), input);
}

#[test]
fn i256_33_bytes_all_ff_trims_to_neg_one() {
    assert_eq!(i256_be_bytes(&[0xFF; 33]).unwrap(), [0xFF; 32]);
}

#[test]
fn i256_33_bytes_all_zero_trims_to_zero() {
    assert_eq!(i256_be_bytes(&[0x00; 33]).unwrap(), [0u8; 32]);
}

#[test]
fn i256_33_bytes_leading_zero_positive_trims() {
    // [0x00, 0x01, 0×31] -> +2^248, leading redundant 0x00 dropped.
    let mut input = vec![0x00u8, 0x01];
    input.extend_from_slice(&[0x00; 31]);
    let mut expected = [0u8; 32];
    expected[0] = 0x01;
    assert_eq!(i256_be_bytes(&input).unwrap(), expected);
}

#[test]
fn i256_33_bytes_leading_zero_then_high_bit_overflows_error() {
    // [0x00, 0x80, 0×31] = +2^255 which does NOT fit signed => cannot drop the 0x00 => error.
    let mut input = vec![0x00u8, 0x80];
    input.extend_from_slice(&[0x00; 31]);
    assert!(i256_be_bytes(&input).is_err());
}

#[test]
fn i256_33_bytes_leading_ff_then_positive_hi_bit_clear_error() {
    // [0xFF, 0x7F, ...]: dropping 0xFF would flip sign => cannot trim => 33 bytes => error.
    let mut input = vec![0xFFu8, 0x7F];
    input.extend_from_slice(&[0x00; 31]);
    assert!(i256_be_bytes(&input).is_err());
}

#[test]
fn i256_33_bytes_leading_ff_negative_trims() {
    // [0xFF, 0xFF, 0×... ] where second byte high bit set -> can drop one 0xFF.
    let mut input = vec![0xFFu8, 0xFF];
    input.extend_from_slice(&[0x00; 31]); // 33 bytes total
    // After dropping one 0xFF: [0xFF, 0x00×31] (32 bytes) = a large negative.
    let mut expected = [0x00u8; 32];
    expected[0] = 0xFF;
    assert_eq!(i256_be_bytes(&input).unwrap(), expected);
}

#[test]
fn i256_34_bytes_all_ff_trims_to_neg_one() {
    assert_eq!(i256_be_bytes(&[0xFF; 34]).unwrap(), [0xFF; 32]);
}

#[test]
fn i256_34_bytes_all_zero_trims_to_zero() {
    assert_eq!(i256_be_bytes(&[0x00; 34]).unwrap(), [0u8; 32]);
}

#[test]
fn i256_oversized_positive_no_trim_error() {
    // [0x7F, 0xFF×32] (33 bytes): leading byte != 0x00 padding, cannot trim => too large.
    let mut input = vec![0x7Fu8];
    input.extend_from_slice(&[0xFF; 32]);
    assert!(i256_be_bytes(&input).is_err());
}

#[test]
fn i256_oversized_negative_no_trim_error() {
    // [0x80, 0×32] (33 bytes) = -2^256, leading byte != 0xFF padding, cannot trim => error.
    let mut input = vec![0x80u8];
    input.extend_from_slice(&[0x00; 32]);
    assert!(i256_be_bytes(&input).is_err());
}

#[test]
fn i256_16_bytes_negative_sign_extends() {
    let input = [0xFFu8; 16];
    let got = i256_be_bytes(&input).unwrap();
    assert_eq!(got, [0xFF; 32]); // -1
}

#[test]
fn i256_16_bytes_positive() {
    let mut input = [0u8; 16];
    input[15] = 0x2A;
    let got = i256_be_bytes(&input).unwrap();
    assert_eq!(got, sext(&input));
}

#[test]
fn i256_leading_zero_then_positive_two_bytes() {
    // [0x00, 0x7F] = +127.
    let mut expected = [0u8; 32];
    expected[31] = 0x7F;
    assert_eq!(i256_be_bytes(&[0x00, 0x7F]).unwrap(), expected);
}

#[test]
fn i256_neg_large_three_bytes() {
    // [0x80, 0x00, 0x01] = -(2^23) + 1 = -8388607.
    let input = [0x80u8, 0x00, 0x01];
    assert_eq!(i256_be_bytes(&input).unwrap(), sext(&input));
}

#[test]
fn i256_35_bytes_negative_trims_multiple() {
    // Five extra 0xFF sign bytes on -1.
    let input = [0xFFu8; 37];
    assert_eq!(i256_be_bytes(&input).unwrap(), [0xFF; 32]);
}

#[test]
fn i256_contrast_with_u256_on_all_ff_32() {
    // Same bytes: i256 accepts (=-1), u256 rejects (negative).
    assert!(i256_be_bytes(&[0xFF; 32]).is_ok());
    assert!(u256_be_bytes(&[0xFF; 32]).is_err());
}

// ===========================================================================
// SECTION 3: schema oracle (convert_avro_schema_to_arrow) precision boundary
// ===========================================================================

fn schema_with_decimal(precision: u64, scale: u64) -> SchemaRef {
    let json = format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"v","type":{{"type":"bytes","logicalType":"decimal","precision":{precision},"scale":{scale}}}}}]}}"#
    );
    let avro = AvroWriterSchema::parse_str(&json).unwrap();
    convert_avro_schema_to_arrow(avro)
}

#[test]
fn oracle_precision_76_scale0_is_decimal256_not_u256() {
    let s = schema_with_decimal(76, 0);
    assert_eq!(s.field(0).data_type(), &DataType::Decimal256(76, 0));
    assert!(!s.field(0).metadata().contains_key(EXT_META_KEY));
}

#[test]
fn oracle_precision_77_scale0_is_u256() {
    let s = schema_with_decimal(77, 0);
    assert_eq!(s.field(0).data_type(), &DataType::FixedSizeBinary(32));
    assert_eq!(
        s.field(0).metadata().get(EXT_META_KEY).map(String::as_str),
        Some("streamling.u256")
    );
}

#[test]
fn oracle_precision_100_scale0_is_u256() {
    let s = schema_with_decimal(100, 0);
    assert_eq!(s.field(0).data_type(), &DataType::FixedSizeBinary(32));
    assert_eq!(
        s.field(0).metadata().get(EXT_META_KEY).map(String::as_str),
        Some("streamling.u256")
    );
}

#[test]
fn oracle_u256_is_not_tagged_i256() {
    let s = schema_with_decimal(90, 0);
    // The u256 metadata value must be exactly the u256 extension, never i256.
    assert_ne!(
        s.field(0).metadata().get(EXT_META_KEY).map(String::as_str),
        Some("streamling.i256")
    );
}

#[test]
fn oracle_precision_39_scale0_is_decimal256() {
    let s = schema_with_decimal(39, 0);
    assert_eq!(s.field(0).data_type(), &DataType::Decimal256(39, 0));
}

#[test]
fn oracle_precision_38_scale0_is_decimal128() {
    let s = schema_with_decimal(38, 0);
    assert_eq!(s.field(0).data_type(), &DataType::Decimal128(38, 0));
}

#[test]
fn oracle_precision_77_scale1_is_utf8_not_u256() {
    let s = schema_with_decimal(77, 1);
    assert_eq!(s.field(0).data_type(), &DataType::Utf8);
    assert!(!s.field(0).metadata().contains_key(EXT_META_KEY));
}

#[test]
fn oracle_precision_100_scale18_is_utf8() {
    let s = schema_with_decimal(100, 18);
    assert_eq!(s.field(0).data_type(), &DataType::Utf8);
}

#[test]
fn oracle_nullable_u256() {
    let json = r#"{"type":"record","name":"R","fields":[{"name":"v","type":["null",{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}]}]}"#;
    let avro = AvroWriterSchema::parse_str(json).unwrap();
    let s = convert_avro_schema_to_arrow(avro);
    assert_eq!(s.field(0).data_type(), &DataType::FixedSizeBinary(32));
    assert!(s.field(0).is_nullable());
    assert_eq!(
        s.field(0).metadata().get(EXT_META_KEY).map(String::as_str),
        Some("streamling.u256")
    );
}

#[test]
#[should_panic]
fn oracle_precision_101_panics() {
    // MAX_SCHEMA_PRECISION is 100; anything above panics in post-processing.
    let _ = schema_with_decimal(101, 0);
}

// ===========================================================================
// SECTION 4: coerce_batch_to_target -> u256
// ===========================================================================

#[test]
fn coerce_u256_zero() {
    assert_eq!(coerce_one_u256(&[0u8; 32]).unwrap(), [0u8; 32]);
}

#[test]
fn coerce_u256_one_from_short_bytes() {
    assert_eq!(coerce_one_u256(&[0x01]).unwrap(), zpad(&[0x01]));
}

#[test]
fn coerce_u256_empty_bytes_is_zero() {
    assert_eq!(coerce_one_u256(&[]).unwrap(), [0u8; 32]);
}

#[test]
fn coerce_u256_full_32_bytes() {
    let mut input = [0u8; 32];
    input[0] = 0x7F;
    input[31] = 0x01;
    assert_eq!(coerce_one_u256(&input).unwrap(), input);
}

#[test]
fn coerce_u256_max_via_33_bytes() {
    let mut input = vec![0x00u8];
    input.extend_from_slice(&[0xFF; 32]);
    assert_eq!(coerce_one_u256(&input).unwrap(), [0xFF; 32]);
}

#[test]
fn coerce_u256_negative_errors() {
    // High bit set => negative => coercion must fail (not panic, not silently wrap).
    let mut input = [0u8; 32];
    input[0] = 0x80;
    assert!(coerce_one_u256(&input).is_err());
}

#[test]
fn coerce_u256_all_ff_32_errors() {
    assert!(coerce_one_u256(&[0xFF; 32]).is_err());
}

#[test]
fn coerce_u256_oversized_errors() {
    let mut input = vec![0x01u8];
    input.extend_from_slice(&[0x00; 32]);
    assert!(coerce_one_u256(&input).is_err());
}

#[test]
fn coerce_u256_null_value() {
    let col = coerce_binary(vec![None], u256_meta(), false).unwrap();
    assert_eq!(col.len(), 1);
    assert!(col.is_null(0));
}

#[test]
fn coerce_u256_batch_mixed_zero_positive_max() {
    let mut max = vec![0x00u8];
    max.extend_from_slice(&[0xFF; 32]);
    let rows = vec![
        Some(vec![0u8; 32]), // 0
        Some(vec![0x01]),    // 1
        Some(max),           // 2^256-1
        None,                // null
    ];
    let col = coerce_binary(rows, u256_meta(), false).unwrap();
    assert_eq!(col.len(), 4);
    assert_eq!(col.value(0), &[0u8; 32]);
    assert_eq!(col.value(1), &zpad(&[0x01]));
    assert_eq!(col.value(2), &[0xFF; 32]);
    assert!(col.is_null(3));
}

#[test]
fn coerce_u256_batch_errors_if_any_row_negative() {
    let rows = vec![Some(vec![0x01]), Some(vec![0x80, 0x00])];
    assert!(coerce_binary(rows, u256_meta(), false).is_err());
}

#[test]
fn coerce_u256_from_large_binary_source() {
    // as_binary_iter must handle LargeBinary too.
    let mut input = [0u8; 32];
    input[31] = 0x2A;
    let col = coerce_binary(vec![Some(input.to_vec())], u256_meta(), true).unwrap();
    assert_eq!(col.value(0), &input);
}

// ===========================================================================
// SECTION 5: coerce_batch_to_target -> i256 (reachable only via hand-built metadata)
// ===========================================================================

#[test]
fn coerce_i256_zero() {
    assert_eq!(coerce_one_i256(&[0u8; 32]).unwrap(), [0u8; 32]);
}

#[test]
fn coerce_i256_neg_one() {
    assert_eq!(coerce_one_i256(&[0xFF]).unwrap(), [0xFF; 32]);
}

#[test]
fn coerce_i256_min() {
    let mut input = [0u8; 32];
    input[0] = 0x80;
    assert_eq!(coerce_one_i256(&input).unwrap(), input);
}

#[test]
fn coerce_i256_max() {
    let mut input = [0xFFu8; 32];
    input[0] = 0x7F;
    assert_eq!(coerce_one_i256(&input).unwrap(), input);
}

#[test]
fn coerce_i256_positive_from_short_bytes() {
    assert_eq!(coerce_one_i256(&[0x01]).unwrap(), sext(&[0x01]));
}

#[test]
fn coerce_i256_neg_128_from_short_bytes() {
    let mut expected = [0xFFu8; 32];
    expected[31] = 0x80;
    assert_eq!(coerce_one_i256(&[0x80]).unwrap(), expected);
}

#[test]
fn coerce_i256_null_value() {
    let col = coerce_binary(vec![None], i256_meta(), false).unwrap();
    assert!(col.is_null(0));
}

#[test]
fn coerce_i256_batch_mixed_signs() {
    let mut min = [0u8; 32];
    min[0] = 0x80;
    let rows = vec![
        Some(vec![0u8; 32]), // 0
        Some(vec![0xFF]),    // -1
        Some(vec![0x01]),    // 1
        Some(min.to_vec()),  // INT256_MIN
        None,
    ];
    let col = coerce_binary(rows, i256_meta(), false).unwrap();
    assert_eq!(col.value(0), &[0u8; 32]);
    assert_eq!(col.value(1), &[0xFF; 32]);
    assert_eq!(col.value(2), &sext(&[0x01]));
    assert_eq!(col.value(3), &min);
    assert!(col.is_null(4));
}

#[test]
fn coerce_i256_oversized_errors() {
    let mut input = vec![0x7Fu8];
    input.extend_from_slice(&[0xFF; 32]);
    assert!(coerce_one_i256(&input).is_err());
}

#[test]
fn coerce_i256_from_large_binary_source() {
    let col = coerce_binary(vec![Some(vec![0xFF])], i256_meta(), true).unwrap();
    assert_eq!(col.value(0), &[0xFF; 32]);
}

// ===========================================================================
// SECTION 6: end-to-end ConfluentAvroDecoder -> u256
// ===========================================================================

const U256_SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}]}"#;
const NULLABLE_U256_SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"v","type":["null",{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}]}]}"#;

fn decode_u256_payload(payload: Vec<u8>) -> std::result::Result<[u8; 32], String> {
    let schema = AvroWriterSchema::parse_str(U256_SCHEMA).unwrap();
    let mut rec = Record::new(&schema).unwrap();
    rec.put("v", Value::Decimal(Decimal::from(payload)));
    let body = to_avro_datum(&schema, rec).unwrap();

    let id: u32 = 1;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, U256_SCHEMA).unwrap();
    d.decode(&confluent_frame(id, &body))
        .map_err(|e| format!("{e:?}"))?;
    let batch = d.flush().map_err(|e| format!("{e:?}"))?.ok_or("no batch")?;
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or("not FixedSizeBinary(32)")?;
    let mut out = [0u8; 32];
    out.copy_from_slice(col.value(0));
    Ok(out)
}

#[test]
fn e2e_u256_zero() {
    assert_eq!(decode_u256_payload(vec![0u8; 32]).unwrap(), [0u8; 32]);
}

#[test]
fn e2e_u256_one() {
    let mut payload = [0u8; 32];
    payload[31] = 0x01;
    assert_eq!(decode_u256_payload(payload.to_vec()).unwrap(), payload);
}

#[test]
fn e2e_u256_mid_magnitude_roundtrips() {
    let mut payload = [0u8; 32];
    payload[0] = 0x12;
    payload[1] = 0x34;
    payload[30] = 0xAB;
    payload[31] = 0xCD;
    assert_eq!(decode_u256_payload(payload.to_vec()).unwrap(), payload);
}

#[test]
fn e2e_u256_high_bit_clear_max_32() {
    let mut payload = [0xFFu8; 32];
    payload[0] = 0x7F; // keep top bit clear so it stays positive on the wire
    assert_eq!(decode_u256_payload(payload.to_vec()).unwrap(), payload);
}

#[test]
fn e2e_u256_negative_value_flush_errors() {
    // A negative unscaled value (top bit set, 32 bytes) must be rejected at flush time.
    let mut payload = [0u8; 32];
    payload[0] = 0x80;
    assert!(decode_u256_payload(payload.to_vec()).is_err());
}

#[test]
fn e2e_u256_neg_one_flush_errors() {
    assert!(decode_u256_payload(vec![0xFF]).is_err());
}

#[test]
fn e2e_u256_field_is_tagged() {
    let schema = AvroWriterSchema::parse_str(U256_SCHEMA).unwrap();
    let mut rec = Record::new(&schema).unwrap();
    let mut payload = [0u8; 32];
    payload[31] = 0x09;
    rec.put("v", Value::Decimal(Decimal::from(payload.to_vec())));
    let body = to_avro_datum(&schema, rec).unwrap();

    let id: u32 = 5;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, U256_SCHEMA).unwrap();
    d.decode(&confluent_frame(id, &body)).unwrap();
    let batch = d.flush().unwrap().expect("batch");
    let f = batch.schema().field(0).clone();
    assert_eq!(f.data_type(), &DataType::FixedSizeBinary(32));
    assert_eq!(
        f.metadata().get(EXT_META_KEY).map(String::as_str),
        Some("streamling.u256")
    );
}

#[test]
fn e2e_u256_multi_row_batch() {
    let schema = AvroWriterSchema::parse_str(U256_SCHEMA).unwrap();
    let id: u32 = 3;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, U256_SCHEMA).unwrap();

    let payloads: Vec<[u8; 32]> = (0..5u8)
        .map(|k| {
            let mut p = [0u8; 32];
            p[31] = k;
            p
        })
        .collect();
    for p in &payloads {
        let mut rec = Record::new(&schema).unwrap();
        rec.put("v", Value::Decimal(Decimal::from(p.to_vec())));
        let body = to_avro_datum(&schema, rec).unwrap();
        d.decode(&confluent_frame(id, &body)).unwrap();
    }
    let batch = d.flush().unwrap().expect("batch");
    assert_eq!(batch.num_rows(), 5);
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    for (i, p) in payloads.iter().enumerate() {
        assert_eq!(col.value(i), p, "row {i}");
    }
}

#[test]
fn e2e_nullable_u256_null_and_value() {
    let schema = AvroWriterSchema::parse_str(NULLABLE_U256_SCHEMA).unwrap();
    let id: u32 = 8;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, NULLABLE_U256_SCHEMA).unwrap();

    // Row 0: null (union branch 0).
    let mut rec0 = Record::new(&schema).unwrap();
    rec0.put("v", Value::Union(0, Box::new(Value::Null)));
    let body0 = to_avro_datum(&schema, rec0).unwrap();
    d.decode(&confluent_frame(id, &body0)).unwrap();

    // Row 1: value 7 (union branch 1).
    let mut payload = [0u8; 32];
    payload[31] = 0x07;
    let mut rec1 = Record::new(&schema).unwrap();
    rec1.put(
        "v",
        Value::Union(1, Box::new(Value::Decimal(Decimal::from(payload.to_vec())))),
    );
    let body1 = to_avro_datum(&schema, rec1).unwrap();
    d.decode(&confluent_frame(id, &body1)).unwrap();

    let batch = d.flush().unwrap().expect("batch");
    assert_eq!(batch.num_rows(), 2);
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!(col.is_null(0), "row 0 must be null");
    assert!(!col.is_null(1));
    assert_eq!(col.value(1), &payload);
    assert!(batch.schema().field(0).is_nullable());
}

#[test]
fn e2e_nullable_u256_all_null_batch() {
    let schema = AvroWriterSchema::parse_str(NULLABLE_U256_SCHEMA).unwrap();
    let id: u32 = 9;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, NULLABLE_U256_SCHEMA).unwrap();
    for _ in 0..3 {
        let mut rec = Record::new(&schema).unwrap();
        rec.put("v", Value::Union(0, Box::new(Value::Null)));
        let body = to_avro_datum(&schema, rec).unwrap();
        d.decode(&confluent_frame(id, &body)).unwrap();
    }
    let batch = d.flush().unwrap().expect("batch");
    assert_eq!(batch.num_rows(), 3);
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!((0..3).all(|i| col.is_null(i)));
}
