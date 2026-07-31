//! Adversarial agent 11 — `ConfluentAvroDecoder` against hostile wire payloads.
//!
//! Everything here builds a **real Confluent frame** (`0x00` + 4-byte BE schema
//! id + avro datum) and pushes it through
//! `streamling_common::formats::avro::arrow_avro::ConfluentAvroDecoder`.
//!
//! The avro bodies are hand-encoded (zigzag varint + raw payload) rather than
//! produced by `apache_avro::to_avro_datum`, because the whole point is to
//! control the exact bytes on the wire: leading `0x00`/`0xFF` padding, magnitudes
//! wider than the declared precision, empty payloads, sign-extension boundaries.
//! `apache_avro::Decimal` normalises some of that away.
//!
//! Contract under test (`arrow_avro.rs::binary_to_decimal_arb` +
//! `contracts/arrow-extension-type.md` §3):
//!
//!   wire bytes  --BigInt::from_signed_bytes_be-->  exact integer
//!   exact integer + column scale  -->  canonical `[sign][minimal BE magnitude]`
//!
//! A decoded `decimal_arb` value must equal the exact two's-complement integer
//! on the wire. Any silent reinterpretation (truncation, sign flip, rescale,
//! zero-fill) is a finding.
//!
//! Tests marked `#[ignore = "FINDING: ..."]` currently fail against the product;
//! they encode the behaviour the contract implies.

use std::sync::Arc;

use apache_avro::Schema as AvroWriterSchema;
use arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, LargeBinaryArray, ListArray,
    StringArray, StructArray,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{Field, Schema};
use bigdecimal::num_bigint::{BigInt, Sign};

use streamling_common::formats::avro::arrow_avro::{ConfluentAvroDecoder, coerce_batch_to_target};
use streamling_common::formats::avro::convert_avro_schema_to_arrow;
use streamling_common::types::decimal_arb::{DecimalArbType, DecimalArbValue};

// ---------------------------------------------------------------------------
// wire helpers — hand-rolled avro encoding so the payload bytes are exact
// ---------------------------------------------------------------------------

/// `2^k` as a `BigInt` (spelled out so shift-type inference stays unambiguous).
fn pow2(k: u32) -> BigInt {
    BigInt::from(1u8) << k
}

/// Zigzag + varint encode an avro `long`/`int`.
fn avro_long(v: i64) -> Vec<u8> {
    let mut n = ((v << 1) ^ (v >> 63)) as u64;
    let mut out = Vec::new();
    loop {
        if n & !0x7F == 0 {
            out.push(n as u8);
            return out;
        }
        out.push(((n & 0x7F) as u8) | 0x80);
        n >>= 7;
    }
}

/// avro `bytes`: length-prefixed payload.
fn avro_bytes(b: &[u8]) -> Vec<u8> {
    let mut o = avro_long(b.len() as i64);
    o.extend_from_slice(b);
    o
}

/// Confluent framing: `0x00` magic + big-endian schema id + body.
fn frame(id: u32, body: &[u8]) -> Vec<u8> {
    let mut f = vec![0x00];
    f.extend_from_slice(&id.to_be_bytes());
    f.extend_from_slice(body);
    f
}

/// `record R { v: bytes decimal(p, s) }`
fn bytes_dec_schema(p: usize, s: usize) -> String {
    format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"v","type":{{"type":"bytes","logicalType":"decimal","precision":{p},"scale":{s}}}}}]}}"#
    )
}

/// `record R { v: fixed(size) decimal(p, s) }`
fn fixed_dec_schema(p: usize, s: usize, size: usize) -> String {
    format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"v","type":{{"type":"fixed","name":"F{size}","size":{size},"logicalType":"decimal","precision":{p},"scale":{s}}}}}]}}"#
    )
}

/// `record R { v: ["null", bytes decimal(p, s)] }`
fn nullable_bytes_dec_schema(p: usize, s: usize) -> String {
    format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"v","type":["null",{{"type":"bytes","logicalType":"decimal","precision":{p},"scale":{s}}}],"default":null}}]}}"#
    )
}

/// The canonical decimal_arb payload for `n`, derived straight from the spec
/// (`[sign][big-endian magnitude, leading zeros stripped]`), independently of
/// the product's encoder.
fn expected_canonical(n: &BigInt) -> Vec<u8> {
    let (sign, mag) = n.to_bytes_be();
    let start = mag.iter().position(|&b| b != 0).unwrap_or(mag.len());
    let mut out = vec![if sign == Sign::Minus { 0xFFu8 } else { 0x00u8 }];
    out.extend_from_slice(&mag[start..]);
    out
}

/// Register `schema_json` under id 1, decode every body, flush.
fn decode_bodies(schema_json: &str, bodies: &[Vec<u8>]) -> Result<Option<RecordBatch>, String> {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, schema_json)
        .map_err(|e| e.to_string())?;
    for b in bodies {
        d.decode(&frame(1, b)).map_err(|e| e.to_string())?;
    }
    d.flush().map_err(|e| e.to_string())
}

/// Assert column 0 is a `decimal_arb(p, s)` field and hand back its storage.
fn arb_col(batch: &RecordBatch, p: usize, s: usize) -> &LargeBinaryArray {
    let f = batch.schema().field(0).clone();
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "decode dropped the decimal_arb tag from the output field: {f:?}"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((p as u32, s as u32)),
        "decode lost the declared (precision, scale) on the output field"
    );
    batch
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("decimal_arb storage must be LargeBinary")
}

/// Decode one bytes-backed `decimal(p, s)` payload; return its canonical bytes.
fn decode_top_bytes(p: usize, s: usize, wire: &[u8]) -> Vec<u8> {
    let schema = bytes_dec_schema(p, s);
    let batch = decode_bodies(&schema, &[avro_bytes(wire)])
        .expect("decode must succeed")
        .expect("a batch");
    assert_eq!(batch.num_rows(), 1, "exactly one row per frame");
    arb_col(&batch, p, s).value(0).to_vec()
}

/// Decode one fixed(size)-backed `decimal(p, s)` payload; canonical bytes out.
fn decode_top_fixed(p: usize, s: usize, size: usize, wire: &[u8]) -> Vec<u8> {
    assert_eq!(
        wire.len(),
        size,
        "fixed payload must be exactly `size` bytes"
    );
    let schema = fixed_dec_schema(p, s, size);
    let batch = decode_bodies(&schema, &[wire.to_vec()])
        .expect("decode must succeed")
        .expect("a batch");
    arb_col(&batch, p, s).value(0).to_vec()
}

/// The core invariant: the decoded canonical bytes must be exactly the spec
/// encoding of `BigInt::from_signed_bytes_be(wire)` at the column scale.
fn assert_bytes_wire_exact(p: usize, s: usize, wire: &[u8]) {
    let got = decode_top_bytes(p, s, wire);
    let n = BigInt::from_signed_bytes_be(wire);
    assert_eq!(
        got,
        expected_canonical(&n),
        "decimal({p},{s}) wire {wire:02x?} must decode to the exact two's-complement value {n}"
    );
    let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&got, s as u32).unwrap();
    assert_eq!(
        decoded,
        DecimalArbValue::from_bigint_and_scale(n.clone(), s as i64),
        "decoded decimal_arb value must equal the wire integer {n} scaled by 10^-{s}"
    );
}

fn assert_fixed_wire_exact(p: usize, s: usize, size: usize, wire: &[u8]) {
    let got = decode_top_fixed(p, s, size, wire);
    let n = BigInt::from_signed_bytes_be(wire);
    assert_eq!(
        got,
        expected_canonical(&n),
        "fixed({size}) decimal({p},{s}) wire {wire:02x?} must decode to exactly {n}"
    );
}

/// `[0x00 * pad]` ++ big-endian magnitude of a positive `n`.
fn be_padded(n: &BigInt, total: usize) -> Vec<u8> {
    let (_, mag) = n.to_bytes_be();
    assert!(mag.len() <= total);
    let mut out = vec![0u8; total - mag.len()];
    out.extend_from_slice(&mag);
    out
}

// ===========================================================================
// 1. Small / boundary integers on a bytes-backed decimal(100, 0)
// ===========================================================================

#[test]
fn zero_single_zero_byte_decodes_to_zero() {
    assert_bytes_wire_exact(100, 0, &[0x00]);
    assert_eq!(decode_top_bytes(100, 0, &[0x00]), vec![0x00]);
}

#[test]
fn empty_byte_payload_decodes_to_zero_and_not_an_error() {
    // A zero-length avro `bytes` decimal is degenerate but must not corrupt or
    // panic; two's-complement of no bytes is 0.
    assert_eq!(
        decode_top_bytes(100, 0, &[]),
        vec![0x00],
        "empty decimal payload must decode as zero, not as garbage"
    );
}

#[test]
fn positive_one_single_byte() {
    assert_bytes_wire_exact(100, 0, &[0x01]);
}

#[test]
fn negative_one_is_0xff_not_255() {
    let got = decode_top_bytes(100, 0, &[0xFF]);
    assert_eq!(
        got,
        vec![0xFF, 0x01],
        "0xFF is two's-complement -1, not unsigned 255"
    );
}

#[test]
fn single_byte_0x7f_is_positive_127() {
    let got = decode_top_bytes(100, 0, &[0x7F]);
    assert_eq!(got, vec![0x00, 0x7F], "0x7F is +127");
}

#[test]
fn single_byte_0x80_is_negative_128() {
    let got = decode_top_bytes(100, 0, &[0x80]);
    assert_eq!(got, vec![0xFF, 0x80], "0x80 is -128 (sign bit set)");
}

#[test]
fn positive_128_requires_a_zero_sign_byte_on_the_wire() {
    assert_bytes_wire_exact(100, 0, &[0x00, 0x80]);
    assert_eq!(decode_top_bytes(100, 0, &[0x00, 0x80]), vec![0x00, 0x80]);
}

#[test]
fn negative_129_two_byte_form() {
    let got = decode_top_bytes(100, 0, &[0xFF, 0x7F]);
    assert_eq!(got, vec![0xFF, 0x81], "0xFF7F is -129");
}

#[test]
fn unsigned_255_needs_two_wire_bytes() {
    assert_bytes_wire_exact(100, 0, &[0x00, 0xFF]);
}

#[test]
fn positive_256_two_bytes() {
    assert_bytes_wire_exact(100, 0, &[0x01, 0x00]);
}

#[test]
fn negative_256_two_bytes() {
    let got = decode_top_bytes(100, 0, &[0xFF, 0x00]);
    assert_eq!(got, vec![0xFF, 0x01, 0x00], "0xFF00 is -256");
}

#[test]
fn negative_two_is_0xfe() {
    assert_eq!(decode_top_bytes(100, 0, &[0xFE]), vec![0xFF, 0x02]);
}

#[test]
fn every_single_byte_value_decodes_as_signed_twos_complement() {
    for b in 0u16..=255 {
        let wire = [b as u8];
        let got = decode_top_bytes(100, 0, &wire);
        let want = expected_canonical(&BigInt::from(b as i8 as i64));
        assert_eq!(
            got, want,
            "single wire byte {b:#04x} must decode as signed {} ",
            b as u8 as i8
        );
    }
}

#[test]
fn two_byte_sign_boundary_sweep() {
    for hi in [0x00u8, 0x01, 0x7F, 0x80, 0xFE, 0xFF] {
        for lo in [0x00u8, 0x01, 0x7F, 0x80, 0xFF] {
            let wire = [hi, lo];
            let got = decode_top_bytes(100, 0, &wire);
            let want = expected_canonical(&BigInt::from(i16::from_be_bytes(wire) as i64));
            assert_eq!(got, want, "two-byte wire {wire:02x?} misdecoded");
        }
    }
}

// ===========================================================================
// 2. Redundant sign padding must not change the value
// ===========================================================================

#[test]
fn leading_zero_padding_does_not_change_a_positive_value() {
    let minimal = decode_top_bytes(100, 0, &[0x01]);
    for pad in 1..=40usize {
        let mut wire = vec![0x00u8; pad];
        wire.push(0x01);
        assert_eq!(
            decode_top_bytes(100, 0, &wire),
            minimal,
            "{pad} leading 0x00 bytes must not change the decoded value"
        );
    }
}

#[test]
fn leading_ff_padding_does_not_change_a_negative_value() {
    let minimal = decode_top_bytes(100, 0, &[0xFF]);
    for pad in 1..=40usize {
        let mut wire = vec![0xFFu8; pad];
        wire.push(0xFF);
        assert_eq!(
            decode_top_bytes(100, 0, &wire),
            minimal,
            "{pad} leading 0xFF bytes must not change the decoded -1"
        );
    }
}

#[test]
fn an_all_zero_payload_of_any_length_is_zero() {
    for len in 1..=48usize {
        assert_eq!(
            decode_top_bytes(100, 0, &vec![0x00; len]),
            vec![0x00],
            "{len} zero bytes must canonicalise to the single 0x00 zero encoding"
        );
    }
}

#[test]
fn an_all_ff_payload_of_any_length_is_minus_one() {
    for len in 1..=48usize {
        assert_eq!(
            decode_top_bytes(100, 0, &vec![0xFF; len]),
            vec![0xFF, 0x01],
            "{len} 0xFF bytes must all decode to -1"
        );
    }
}

#[test]
fn padded_and_minimal_encodings_of_the_same_value_produce_identical_bytes() {
    // GROUP BY / DISTINCT / join keys compare the canonical bytes directly, so
    // two wire spellings of one number must land on the same payload.
    let n = BigInt::parse_bytes(b"1234567890123456789012345678901234567890", 10).unwrap();
    let minimal = n.to_signed_bytes_be();
    let padded = {
        let mut v = vec![0x00u8; 9];
        v.extend_from_slice(&minimal);
        v
    };
    assert_eq!(
        decode_top_bytes(100, 0, &minimal),
        decode_top_bytes(100, 0, &padded),
        "padded and minimal wire spellings must be byte-identical after decode"
    );
}

#[test]
fn negative_padded_and_minimal_encodings_are_identical() {
    let n = BigInt::parse_bytes(b"-987654321098765432109876543210987654321", 10).unwrap();
    let minimal = n.to_signed_bytes_be();
    let mut padded = vec![0xFFu8; 7];
    padded.extend_from_slice(&minimal);
    assert_eq!(
        decode_top_bytes(100, 0, &minimal),
        decode_top_bytes(100, 0, &padded),
        "0xFF-padded negative must equal its minimal spelling"
    );
}

// ===========================================================================
// 3. Width boundaries — min/max at every byte width
// ===========================================================================

#[test]
fn max_signed_value_at_widths_1_to_8() {
    for w in 1..=8usize {
        let mut wire = vec![0xFFu8; w];
        wire[0] = 0x7F;
        assert_bytes_wire_exact(100, 0, &wire);
    }
}

#[test]
fn min_signed_value_at_widths_1_to_8() {
    for w in 1..=8usize {
        let mut wire = vec![0x00u8; w];
        wire[0] = 0x80;
        assert_bytes_wire_exact(100, 0, &wire);
    }
}

#[test]
fn max_signed_value_at_widths_9_to_16() {
    for w in 9..=16usize {
        let mut wire = vec![0xFFu8; w];
        wire[0] = 0x7F;
        assert_bytes_wire_exact(100, 0, &wire);
    }
}

#[test]
fn min_signed_value_at_widths_9_to_16() {
    for w in 9..=16usize {
        let mut wire = vec![0x00u8; w];
        wire[0] = 0x80;
        assert_bytes_wire_exact(100, 0, &wire);
    }
}

#[test]
fn max_signed_value_at_widths_17_to_32() {
    for w in 17..=32usize {
        let mut wire = vec![0xFFu8; w];
        wire[0] = 0x7F;
        assert_bytes_wire_exact(100, 0, &wire);
    }
}

#[test]
fn min_signed_value_at_widths_17_to_32() {
    for w in 17..=32usize {
        let mut wire = vec![0x00u8; w];
        wire[0] = 0x80;
        assert_bytes_wire_exact(100, 0, &wire);
    }
}

#[test]
fn i128_max_and_min_cross_the_decimal128_boundary_losslessly() {
    assert_bytes_wire_exact(100, 0, &i128::MAX.to_be_bytes());
    assert_bytes_wire_exact(100, 0, &i128::MIN.to_be_bytes());
}

#[test]
fn two_pow_128_is_not_truncated_to_zero() {
    // The classic 128-bit truncation bug: 2^128 has zero low 128 bits.
    let n: BigInt = pow2(128);
    let wire = n.to_signed_bytes_be();
    let got = decode_top_bytes(100, 0, &wire);
    assert_eq!(
        got,
        expected_canonical(&n),
        "2^128 must not be truncated to its (zero) low 128 bits"
    );
    let v = DecimalArbValue::from_canonical_bytes_at_scale(&got, 0).unwrap();
    assert_eq!(
        v.to_canonical_string(),
        "340282366920938463463374607431768211456"
    );
}

#[test]
fn two_pow_256_is_not_truncated_to_zero() {
    let n: BigInt = pow2(256);
    let wire = n.to_signed_bytes_be();
    assert_eq!(decode_top_bytes(100, 0, &wire), expected_canonical(&n));
}

#[test]
fn u256_max_round_trips_exactly() {
    let n: BigInt = pow2(256) - 1;
    let wire = n.to_signed_bytes_be();
    let got = decode_top_bytes(100, 0, &wire);
    assert_eq!(got, expected_canonical(&n), "2^256-1 must survive intact");
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&got, 0)
            .unwrap()
            .to_canonical_string(),
        "115792089237316195423570985008687907853269984665640564039457584007913129639935"
    );
}

#[test]
fn i256_min_round_trips_exactly() {
    let n: BigInt = -pow2(255);
    let wire = n.to_signed_bytes_be();
    assert_eq!(decode_top_bytes(100, 0, &wire), expected_canonical(&n));
}

#[test]
fn exactly_100_digit_value_at_declared_precision_100() {
    let s = "9".repeat(100);
    let n = BigInt::parse_bytes(s.as_bytes(), 10).unwrap();
    let got = decode_top_bytes(100, 0, &n.to_signed_bytes_be());
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&got, 0)
            .unwrap()
            .to_canonical_string(),
        s,
        "a value at exactly the declared precision must survive"
    );
}

#[test]
fn magnitude_wider_than_declared_precision_is_still_decoded_losslessly() {
    // 120 digits into a decimal(100, 0) column. The decode path is lossless by
    // design (validation happens at the sink); it must never silently truncate.
    let s = "1".repeat(120);
    let n = BigInt::parse_bytes(s.as_bytes(), 10).unwrap();
    let got = decode_top_bytes(100, 0, &n.to_signed_bytes_be());
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&got, 0)
            .unwrap()
            .to_canonical_string(),
        s,
        "an over-precision wire value must be preserved, not truncated"
    );
}

#[test]
fn negative_magnitude_wider_than_declared_precision_is_preserved() {
    let s = format!("-{}", "7".repeat(150));
    let n = BigInt::parse_bytes(s.as_bytes(), 10).unwrap();
    let got = decode_top_bytes(100, 0, &n.to_signed_bytes_be());
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&got, 0)
            .unwrap()
            .to_canonical_string(),
        s
    );
}

#[test]
fn one_kilobyte_payload_decodes_without_loss() {
    let mut wire = vec![0u8; 1024];
    wire[0] = 0x01;
    wire[1023] = 0x7F;
    assert_bytes_wire_exact(100, 0, &wire);
}

#[test]
fn powers_of_two_up_to_2_pow_600_all_round_trip() {
    for k in [0u32, 1, 7, 8, 63, 64, 127, 128, 200, 255, 256, 400, 600] {
        let n: BigInt = pow2(k);
        assert_eq!(
            decode_top_bytes(100, 0, &n.to_signed_bytes_be()),
            expected_canonical(&n),
            "2^{k} misdecoded"
        );
    }
}

#[test]
fn negated_powers_of_two_all_round_trip() {
    for k in [0u32, 1, 7, 8, 63, 64, 127, 128, 255, 256, 400] {
        let n: BigInt = -pow2(k);
        assert_eq!(
            decode_top_bytes(100, 0, &n.to_signed_bytes_be()),
            expected_canonical(&n),
            "-2^{k} misdecoded"
        );
    }
}

// ===========================================================================
// 4. Scale handling
// ===========================================================================

#[test]
fn scaled_decimal_places_the_point_from_the_column_scale() {
    let got = decode_top_bytes(85, 4, &[0x12, 0xD6, 0x87]); // 1_234_567
    let v = DecimalArbValue::from_canonical_bytes_at_scale(&got, 4).unwrap();
    assert_eq!(v.to_canonical_string(), "123.4567");
}

#[test]
fn scaled_negative_decimal_places_the_point_correctly() {
    let n = BigInt::from(-1_234_567i64);
    let got = decode_top_bytes(85, 4, &n.to_signed_bytes_be());
    let v = DecimalArbValue::from_canonical_bytes_at_scale(&got, 4).unwrap();
    assert_eq!(v.to_canonical_string(), "-123.4567");
}

#[test]
fn scaled_zero_is_the_single_zero_byte() {
    assert_eq!(
        decode_top_bytes(85, 4, &[0x00]),
        vec![0x00],
        "zero at any scale is the one-byte 0x00 encoding"
    );
}

#[test]
fn scaled_unit_in_the_last_place() {
    let got = decode_top_bytes(85, 4, &[0x01]);
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&got, 4)
            .unwrap()
            .to_canonical_string(),
        "0.0001"
    );
}

#[test]
fn scale_preserves_trailing_zero_unscaled_digits() {
    // unscaled 1_230_000 at scale 4 == 123.0000. The canonical bytes must still
    // carry the full unscaled integer, otherwise the value silently rescales.
    let n = BigInt::from(1_230_000i64);
    let got = decode_top_bytes(85, 4, &n.to_signed_bytes_be());
    assert_eq!(
        got,
        expected_canonical(&n),
        "trailing zeros of the unscaled integer must not be normalised away"
    );
}

#[test]
fn scale_equal_to_precision_round_trips() {
    let got = decode_top_bytes(85, 85, &[0x01]);
    let v = DecimalArbValue::from_canonical_bytes_at_scale(&got, 85).unwrap();
    assert_eq!(v.to_canonical_string(), format!("0.{}1", "0".repeat(84)));
}

#[test]
fn scale_100_at_precision_100_round_trips() {
    let n = BigInt::parse_bytes(b"123456789", 10).unwrap();
    let got = decode_top_bytes(100, 100, &n.to_signed_bytes_be());
    assert_eq!(got, expected_canonical(&n));
    let v = DecimalArbValue::from_canonical_bytes_at_scale(&got, 100).unwrap();
    assert_eq!(
        v.to_canonical_string(),
        format!("0.{}123456789", "0".repeat(91))
    );
}

#[test]
fn every_scale_from_1_to_20_preserves_the_unscaled_integer() {
    let n = BigInt::parse_bytes(b"98765432109876543210", 10).unwrap();
    let wire = n.to_signed_bytes_be();
    for s in 1..=20usize {
        assert_eq!(
            decode_top_bytes(100, s, &wire),
            expected_canonical(&n),
            "scale {s} must not alter the unscaled integer on the wire"
        );
    }
}

#[test]
fn scaled_wide_value_beyond_decimal256_round_trips() {
    let n =
        BigInt::parse_bytes(b"-12345678901234567890123456789012345678901234567890", 10).unwrap();
    let got = decode_top_bytes(90, 18, &n.to_signed_bytes_be());
    assert_eq!(got, expected_canonical(&n));
    let v = DecimalArbValue::from_canonical_bytes_at_scale(&got, 18).unwrap();
    assert_eq!(
        v.to_canonical_string(),
        "-12345678901234567890123456789012.345678901234567890"
    );
}

#[test]
fn precision_77_scale_0_carries_the_u256_native_int_hint_and_still_decodes_negatives() {
    // The 77..=78 window is stamped `native_int_kind=u256`, but the avro wire
    // encoding is signed. A negative value must still decode exactly.
    let n = BigInt::from(-42i64);
    let got = decode_top_bytes(77, 0, &n.to_signed_bytes_be());
    assert_eq!(
        got,
        expected_canonical(&n),
        "negative value in a 77-digit column"
    );
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&got, 0)
            .unwrap()
            .to_canonical_string(),
        "-42"
    );
}

#[test]
fn precision_79_has_no_native_int_hint_but_decodes_the_same() {
    let n = BigInt::from(-42i64);
    assert_eq!(
        decode_top_bytes(79, 0, &n.to_signed_bytes_be()),
        expected_canonical(&n)
    );
}

// ===========================================================================
// 5. fixed(n)-backed decimals
// ===========================================================================

#[test]
fn fixed_widths_1_to_8_min_and_max() {
    for n in 1..=8usize {
        let mut maxv = vec![0xFFu8; n];
        maxv[0] = 0x7F;
        assert_fixed_wire_exact(100, 0, n, &maxv);
        let mut minv = vec![0x00u8; n];
        minv[0] = 0x80;
        assert_fixed_wire_exact(100, 0, n, &minv);
    }
}

#[test]
fn fixed_widths_9_to_16_min_and_max() {
    for n in 9..=16usize {
        let mut maxv = vec![0xFFu8; n];
        maxv[0] = 0x7F;
        assert_fixed_wire_exact(100, 0, n, &maxv);
        let mut minv = vec![0x00u8; n];
        minv[0] = 0x80;
        assert_fixed_wire_exact(100, 0, n, &minv);
    }
}

#[test]
fn fixed_widths_17_to_24_min_and_max() {
    for n in 17..=24usize {
        let mut maxv = vec![0xFFu8; n];
        maxv[0] = 0x7F;
        assert_fixed_wire_exact(100, 0, n, &maxv);
        let mut minv = vec![0x00u8; n];
        minv[0] = 0x80;
        assert_fixed_wire_exact(100, 0, n, &minv);
    }
}

#[test]
fn fixed_widths_25_to_32_min_and_max() {
    for n in 25..=32usize {
        let mut maxv = vec![0xFFu8; n];
        maxv[0] = 0x7F;
        assert_fixed_wire_exact(100, 0, n, &maxv);
        let mut minv = vec![0x00u8; n];
        minv[0] = 0x80;
        assert_fixed_wire_exact(100, 0, n, &minv);
    }
}

#[test]
fn fixed_wider_than_32_bytes_still_decodes_losslessly() {
    for n in [33usize, 40, 48, 64, 96] {
        let mut wire = vec![0x00u8; n];
        wire[1] = 0x01;
        wire[n - 1] = 0x7F;
        assert_fixed_wire_exact(100, 0, n, &wire);
    }
}

#[test]
fn fixed_negative_wider_than_32_bytes_sign_extends() {
    for n in [33usize, 48, 64] {
        let mut wire = vec![0xFFu8; n];
        wire[n - 1] = 0x01;
        assert_fixed_wire_exact(100, 0, n, &wire);
    }
}

#[test]
fn fixed_all_zero_of_any_width_is_zero() {
    for n in [1usize, 4, 16, 32, 33, 64] {
        assert_eq!(
            decode_top_fixed(100, 0, n, &vec![0x00; n]),
            vec![0x00],
            "fixed({n}) all-zero must be canonical zero"
        );
    }
}

#[test]
fn fixed_all_ff_of_any_width_is_minus_one() {
    for n in [1usize, 4, 16, 32, 33, 64] {
        assert_eq!(
            decode_top_fixed(100, 0, n, &vec![0xFF; n]),
            vec![0xFF, 0x01],
            "fixed({n}) all-0xFF must be -1"
        );
    }
}

#[test]
fn fixed_zero_padded_value_equals_the_bytes_backed_spelling() {
    let n = BigInt::parse_bytes(b"31415926535897932384626433832795", 10).unwrap();
    let via_bytes = decode_top_bytes(100, 0, &n.to_signed_bytes_be());
    let via_fixed = decode_top_fixed(100, 0, 32, &be_padded(&n, 32));
    assert_eq!(
        via_bytes, via_fixed,
        "fixed(32) zero-padded and bytes-minimal spellings must agree"
    );
}

#[test]
fn fixed_scaled_decimal_round_trips() {
    let n = BigInt::from(1_234_567i64);
    let got = decode_top_fixed(85, 4, 32, &be_padded(&n, 32));
    assert_eq!(
        DecimalArbValue::from_canonical_bytes_at_scale(&got, 4)
            .unwrap()
            .to_canonical_string(),
        "123.4567"
    );
}

#[test]
fn fixed_payload_shorter_than_declared_size_is_rejected() {
    // fixed(32) declared, only 8 bytes on the wire → the frame is truncated.
    let schema = fixed_dec_schema(100, 0, 32);
    let r = decode_bodies(&schema, &[vec![0x01u8; 8]]);
    match r {
        Err(_) => {}
        Ok(None) => {}
        Ok(Some(b)) => panic!(
            "a truncated fixed(32) payload produced {} row(s) instead of erroring",
            b.num_rows()
        ),
    }
}

// ===========================================================================
// 6. Nullable unions around the decimal
// ===========================================================================

#[test]
fn null_branch_yields_a_null_decimal_arb_value() {
    let schema = nullable_bytes_dec_schema(100, 0);
    let batch = decode_bodies(&schema, &[avro_long(0)])
        .expect("decode")
        .expect("batch");
    let col = arb_col(&batch, 100, 0);
    assert!(
        col.is_null(0),
        "the avro null branch must decode as SQL NULL"
    );
}

#[test]
fn value_branch_after_null_branch_keeps_row_alignment() {
    let schema = nullable_bytes_dec_schema(100, 0);
    let mut b1 = avro_long(1);
    b1.extend_from_slice(&avro_bytes(&[0x2A]));
    let mut b3 = avro_long(1);
    b3.extend_from_slice(&avro_bytes(&[0xD6])); // -42
    let batch = decode_bodies(&schema, &[b1, avro_long(0), b3])
        .expect("decode")
        .expect("batch");
    assert_eq!(batch.num_rows(), 3);
    let col = arb_col(&batch, 100, 0);
    assert_eq!(col.value(0), &[0x00, 0x2A][..], "row 0 == 42");
    assert!(col.is_null(1), "row 1 is NULL");
    assert_eq!(col.value(2), &[0xFF, 0x2A][..], "row 2 == -42");
}

#[test]
fn all_null_column_has_no_stray_values() {
    let schema = nullable_bytes_dec_schema(100, 0);
    let bodies: Vec<Vec<u8>> = (0..8).map(|_| avro_long(0)).collect();
    let batch = decode_bodies(&schema, &bodies)
        .expect("decode")
        .expect("batch");
    let col = arb_col(&batch, 100, 0);
    assert_eq!(col.null_count(), 8, "every row must be NULL");
}

#[test]
fn nullable_zero_is_distinguishable_from_null() {
    let schema = nullable_bytes_dec_schema(100, 0);
    let mut zero = avro_long(1);
    zero.extend_from_slice(&avro_bytes(&[0x00]));
    let batch = decode_bodies(&schema, &[avro_long(0), zero])
        .expect("decode")
        .expect("batch");
    let col = arb_col(&batch, 100, 0);
    assert!(col.is_null(0), "NULL row");
    assert!(!col.is_null(1), "zero is a value, not a NULL");
    assert_eq!(col.value(1), &[0x00][..]);
}

#[test]
#[ignore = "FINDING: a field-level [\"null\", decimal] union accepts ANY branch index (5, -1) as the value branch — a corrupted union byte silently yields a non-null decimal instead of an error"]
fn out_of_range_union_branch_index_is_rejected_not_silently_decoded() {
    let schema = nullable_bytes_dec_schema(100, 0);
    let mut body = avro_long(5); // only branches 0 and 1 exist
    body.extend_from_slice(&avro_bytes(&[0x01]));
    let r = decode_bodies(&schema, &[body]);
    assert!(
        r.is_err() || r.as_ref().unwrap().is_none(),
        "an out-of-range union branch index must not produce a row"
    );
}

#[test]
#[ignore = "FINDING: a negative field-level union branch index (-1) decodes as the value branch instead of being rejected"]
fn negative_union_branch_index_is_rejected() {
    let schema = nullable_bytes_dec_schema(100, 0);
    let mut body = avro_long(-1);
    body.extend_from_slice(&avro_bytes(&[0x01]));
    let r = decode_bodies(&schema, &[body]);
    assert!(
        r.is_err() || r.as_ref().unwrap().is_none(),
        "a negative union branch index must not produce a row"
    );
}

// ===========================================================================
// 7. Top-level union (["null", record]) Debezium framing
// ===========================================================================

fn union_root(record_json: &str) -> String {
    format!(r#"["null",{record_json}]"#)
}

#[test]
fn union_rooted_writer_strips_the_branch_prefix_and_decodes_the_decimal() {
    let rec = bytes_dec_schema(100, 0);
    let json = union_root(&rec);
    let mut body = avro_long(1); // record branch
    body.extend_from_slice(&avro_bytes(&[0x01, 0x00]));
    let batch = decode_bodies(&json, &[body])
        .expect("decode")
        .expect("batch");
    assert_eq!(arb_col(&batch, 100, 0).value(0), &[0x00, 0x01, 0x00][..]);
}

#[test]
fn union_rooted_record_at_index_zero_decodes() {
    let rec = bytes_dec_schema(100, 0);
    let json = format!(r#"[{rec},"null"]"#);
    let mut body = avro_long(0); // record is branch 0 here
    body.extend_from_slice(&avro_bytes(&[0x7F]));
    let batch = decode_bodies(&json, &[body])
        .expect("decode")
        .expect("batch");
    assert_eq!(arb_col(&batch, 100, 0).value(0), &[0x00, 0x7F][..]);
}

#[test]
fn union_rooted_record_at_index_two_decodes() {
    let rec = bytes_dec_schema(100, 0);
    let json = format!(r#"["null","string",{rec}]"#);
    let mut body = avro_long(2);
    body.extend_from_slice(&avro_bytes(&[0xFF]));
    let batch = decode_bodies(&json, &[body])
        .expect("decode")
        .expect("batch");
    assert_eq!(arb_col(&batch, 100, 0).value(0), &[0xFF, 0x01][..]);
}

#[test]
fn union_rooted_null_branch_body_is_rejected_with_a_typed_error() {
    let rec = bytes_dec_schema(100, 0);
    let json = union_root(&rec);
    let err = decode_bodies(&json, &[avro_long(0)]).expect_err("null root must error");
    assert!(
        err.contains("union branch"),
        "expected a union-branch error, got: {err}"
    );
}

#[test]
fn union_rooted_wrong_branch_index_is_rejected() {
    let rec = bytes_dec_schema(100, 0);
    let json = union_root(&rec);
    let mut body = avro_long(7);
    body.extend_from_slice(&avro_bytes(&[0x01]));
    assert!(
        decode_bodies(&json, &[body]).is_err(),
        "a branch index that is not the record branch must be rejected"
    );
}

#[test]
fn union_rooted_empty_body_reports_a_truncated_long() {
    let rec = bytes_dec_schema(100, 0);
    let json = union_root(&rec);
    let err = decode_bodies(&json, &[vec![]]).expect_err("empty body must error");
    assert!(
        err.contains("truncated"),
        "expected 'truncated avro long', got: {err}"
    );
}

#[test]
fn union_rooted_overlong_varint_is_rejected_not_panicked() {
    let rec = bytes_dec_schema(100, 0);
    let json = union_root(&rec);
    // 10 continuation bytes → shift > 63 → the decoder must error, not wrap.
    let body = vec![0x80u8; 10];
    let err = decode_bodies(&json, &[body]).expect_err("overlong varint must error");
    assert!(
        err.contains("overflow") || err.contains("union branch"),
        "expected an avro long overflow error, got: {err}"
    );
}

#[test]
fn union_root_with_no_record_branch_is_rejected_at_registration() {
    let mut d = ConfluentAvroDecoder::new();
    let err = d
        .register_writer_schema(1, r#"["null","string"]"#)
        .expect_err("a record-less union root is unsupported");
    assert!(
        err.to_string().contains("record branch"),
        "expected a 'no record branch' error, got: {err}"
    );
}

#[test]
fn union_rooted_wide_decimal_survives_the_reframing() {
    let rec = bytes_dec_schema(100, 0);
    let json = union_root(&rec);
    let n: BigInt = pow2(300) - 12345;
    let mut body = avro_long(1);
    body.extend_from_slice(&avro_bytes(&n.to_signed_bytes_be()));
    let batch = decode_bodies(&json, &[body])
        .expect("decode")
        .expect("batch");
    assert_eq!(
        arb_col(&batch, 100, 0).value(0),
        expected_canonical(&n).as_slice(),
        "the union prefix strip must not shift the decimal payload"
    );
}

#[test]
fn mixed_union_and_plain_writer_ids_each_use_their_own_framing_for_decimals() {
    let rec = bytes_dec_schema(100, 0);
    let json = union_root(&rec);
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &json).unwrap();
    d.register_writer_schema(2, &rec).unwrap();

    let mut union_body = avro_long(1);
    union_body.extend_from_slice(&avro_bytes(&[0x11]));
    d.decode(&frame(1, &union_body)).unwrap();
    d.decode(&frame(2, &avro_bytes(&[0x22]))).unwrap();

    let batch = d.flush().unwrap().expect("batch");
    assert_eq!(batch.num_rows(), 2);
    let col = arb_col(&batch, 100, 0);
    let mut seen: Vec<Vec<u8>> = (0..2).map(|i| col.value(i).to_vec()).collect();
    seen.sort();
    assert_eq!(
        seen,
        vec![vec![0x00, 0x11], vec![0x00, 0x22]],
        "each writer id must decode with its own root framing"
    );
}

// ===========================================================================
// 8. Confluent frame validation
// ===========================================================================

#[test]
fn frames_shorter_than_five_bytes_are_rejected() {
    for len in 0..5usize {
        let mut d = ConfluentAvroDecoder::new();
        d.register_writer_schema(1, &bytes_dec_schema(100, 0))
            .unwrap();
        let err = d
            .decode(&vec![0x00u8; len])
            .expect_err("short frame must error");
        assert!(
            err.to_string().contains("shorter than 5 bytes"),
            "len {len}: expected a short-frame error, got {err}"
        );
    }
}

#[test]
fn a_non_zero_magic_byte_is_rejected() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    let mut f = frame(1, &avro_bytes(&[0x01]));
    f[0] = 0xC3; // avro single-object encoding marker
    let err = d.decode(&f).expect_err("bad magic must error");
    assert!(
        err.to_string().contains("magic"),
        "expected a magic-byte error, got {err}"
    );
}

#[test]
fn an_unknown_schema_id_errors_instead_of_panicking() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    let r = d.decode(&frame(999, &avro_bytes(&[0x01])));
    assert!(
        r.is_err(),
        "an unregistered schema id must be a typed error"
    );
}

#[test]
fn an_unknown_schema_id_does_not_destroy_previously_decoded_rows() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    d.decode(&frame(1, &avro_bytes(&[0x05]))).unwrap();
    let _ = d.decode(&frame(999, &avro_bytes(&[0x06])));
    let batch = d.flush().unwrap().expect("the good row must survive");
    assert_eq!(
        batch.num_rows(),
        1,
        "the already-decoded row must not be lost"
    );
    assert_eq!(arb_col(&batch, 100, 0).value(0), &[0x00, 0x05][..]);
}

#[test]
fn an_empty_body_after_the_frame_header_does_not_produce_a_row() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    let r = d.decode(&frame(1, &[]));
    if r.is_ok() {
        let out = d.flush().unwrap();
        assert!(
            out.as_ref().map(|b| b.num_rows()).unwrap_or(0) == 0,
            "a body-less frame must not materialise a phantom row"
        );
    }
}

#[test]
fn a_truncated_bytes_payload_never_yields_a_wrong_value() {
    // Declares 8 bytes but supplies 3.
    let mut body = avro_long(8);
    body.extend_from_slice(&[0x01, 0x02, 0x03]);
    let r = decode_bodies(&bytes_dec_schema(100, 0), &[body]);
    match r {
        Err(_) | Ok(None) => {}
        Ok(Some(b)) => panic!(
            "a truncated payload produced {} row(s): silent short-read",
            b.num_rows()
        ),
    }
}

#[test]
fn a_negative_bytes_length_is_rejected() {
    let mut body = avro_long(-4);
    body.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
    let r = decode_bodies(&bytes_dec_schema(100, 0), &[body]);
    match r {
        Err(_) | Ok(None) => {}
        Ok(Some(b)) => panic!("a negative bytes length produced {} row(s)", b.num_rows()),
    }
}

#[test]
fn an_absurd_bytes_length_is_rejected_without_allocating_it() {
    let mut body = avro_long(i32::MAX as i64);
    body.extend_from_slice(&[0x01, 0x02]);
    let r = decode_bodies(&bytes_dec_schema(100, 0), &[body]);
    match r {
        Err(_) | Ok(None) => {}
        Ok(Some(b)) => panic!("an absurd length header produced {} row(s)", b.num_rows()),
    }
}

#[test]
fn trailing_junk_after_the_datum_never_corrupts_row_zero() {
    let mut body = avro_bytes(&[0x2A]);
    body.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let r = decode_bodies(&bytes_dec_schema(100, 0), &[body]);
    if let Ok(Some(b)) = r {
        assert!(b.num_rows() >= 1);
        assert_eq!(
            arb_col(&b, 100, 0).value(0),
            &[0x00, 0x2A][..],
            "row 0 must still be 42 when junk trails the datum"
        );
    }
}

#[test]
fn schema_id_zero_and_u32_max_are_usable() {
    for id in [0u32, u32::MAX] {
        let mut d = ConfluentAvroDecoder::new();
        d.register_writer_schema(id, &bytes_dec_schema(100, 0))
            .unwrap();
        assert!(d.has_writer_schema(id), "id {id} must be registered");
        d.decode(&frame(id, &avro_bytes(&[0x09]))).unwrap();
        let b = d.flush().unwrap().expect("batch");
        assert_eq!(arb_col(&b, 100, 0).value(0), &[0x00, 0x09][..]);
    }
}

#[test]
fn has_writer_schema_is_false_for_unregistered_ids() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    assert!(d.has_writer_schema(1));
    assert!(!d.has_writer_schema(2));
}

#[test]
fn flush_without_any_schema_is_an_error_not_a_panic() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(
        d.flush().is_err(),
        "flushing with no schema must be a typed error"
    );
}

#[test]
fn flush_with_no_decoded_rows_returns_none() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    assert!(d.flush().unwrap().is_none(), "no rows decoded → no batch");
}

#[test]
fn a_second_flush_returns_none_and_does_not_duplicate_rows() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    d.decode(&frame(1, &avro_bytes(&[0x01]))).unwrap();
    assert_eq!(d.flush().unwrap().expect("batch").num_rows(), 1);
    assert!(
        d.flush().unwrap().is_none(),
        "a second flush must not re-emit the same rows"
    );
}

#[test]
fn the_decoder_is_reusable_after_a_flush() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    d.decode(&frame(1, &avro_bytes(&[0x01]))).unwrap();
    let _ = d.flush().unwrap();
    d.decode(&frame(1, &avro_bytes(&[0x02]))).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 1, "post-flush decoding starts a fresh batch");
    assert_eq!(arb_col(&b, 100, 0).value(0), &[0x00, 0x02][..]);
}

#[test]
fn target_schema_is_derived_from_the_first_registered_writer() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(d.target_schema().is_none());
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    let t = d.target_schema().expect("target schema").clone();
    assert!(DecimalArbType::is_decimal_arb_field(t.field(0)));
}

// ===========================================================================
// 9. Multiple writer generations in one batch
// ===========================================================================

#[test]
fn two_writer_ids_with_the_same_shape_produce_all_rows_in_order() {
    let schema = bytes_dec_schema(100, 0);
    let reader = AvroWriterSchema::parse_str(&schema).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, &schema).unwrap();
    d.register_writer_schema(2, &schema).unwrap();
    d.decode(&frame(1, &avro_bytes(&[0x01]))).unwrap();
    d.decode(&frame(2, &avro_bytes(&[0x02]))).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 2);
    let col = arb_col(&b, 100, 0);
    assert_eq!(
        col.value(0),
        &[0x00, 0x01][..],
        "generation order preserved"
    );
    assert_eq!(col.value(1), &[0x00, 0x02][..]);
}

#[test]
fn alternating_writer_ids_preserve_row_order_and_values() {
    let schema = bytes_dec_schema(100, 0);
    let reader = AvroWriterSchema::parse_str(&schema).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, &schema).unwrap();
    d.register_writer_schema(2, &schema).unwrap();
    let ids = [1u32, 2, 1, 2, 1];
    for (i, id) in ids.iter().enumerate() {
        d.decode(&frame(*id, &avro_bytes(&[(i + 1) as u8])))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 5, "no rows lost across 5 generations");
    let col = arb_col(&b, 100, 0);
    for i in 0..5 {
        assert_eq!(
            col.value(i),
            &[0x00, (i + 1) as u8][..],
            "row {i} out of order or corrupted across writer-id switches"
        );
    }
}

#[test]
fn registering_a_new_writer_id_mid_generation_does_not_drop_buffered_rows() {
    let schema = bytes_dec_schema(100, 0);
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &schema).unwrap();
    d.decode(&frame(1, &avro_bytes(&[0x0A]))).unwrap();
    d.register_writer_schema(2, &schema).unwrap();
    d.decode(&frame(1, &avro_bytes(&[0x0B]))).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(
        b.num_rows(),
        2,
        "mid-generation registration lost buffered rows"
    );
}

#[test]
fn wide_decimals_survive_concatenation_across_generations() {
    let schema = bytes_dec_schema(100, 0);
    let reader = AvroWriterSchema::parse_str(&schema).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, &schema).unwrap();
    d.register_writer_schema(2, &schema).unwrap();
    let a: BigInt = pow2(300) + 7;
    let b_val: BigInt = -(pow2(260) + BigInt::from(3));
    d.decode(&frame(1, &avro_bytes(&a.to_signed_bytes_be())))
        .unwrap();
    d.decode(&frame(2, &avro_bytes(&b_val.to_signed_bytes_be())))
        .unwrap();
    let batch = d.flush().unwrap().expect("batch");
    let col = arb_col(&batch, 100, 0);
    assert_eq!(col.value(0), expected_canonical(&a).as_slice());
    assert_eq!(col.value(1), expected_canonical(&b_val).as_slice());
}

#[test]
fn a_hundred_rows_of_varied_widths_all_decode_exactly() {
    let schema = bytes_dec_schema(100, 0);
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &schema).unwrap();
    let mut expect: Vec<Vec<u8>> = Vec::new();
    for i in 0..100i64 {
        let n = if i % 3 == 0 {
            BigInt::from(i) << (i as u32 % 200)
        } else if i % 3 == 1 {
            -(BigInt::from(i + 1) << (i as u32 % 150))
        } else {
            BigInt::from(i)
        };
        expect.push(expected_canonical(&n));
        d.decode(&frame(1, &avro_bytes(&n.to_signed_bytes_be())))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 100);
    let col = arb_col(&b, 100, 0);
    for (i, want) in expect.iter().enumerate() {
        assert_eq!(col.value(i), want.as_slice(), "row {i} decoded incorrectly");
    }
}

#[test]
fn schema_evolution_adding_a_field_keeps_the_decimal_column_intact() {
    const V1: &str = r#"{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}]}"#;
    const V2: &str = r#"{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}},{"name":"tag","type":"int","default":9}]}"#;
    let reader = AvroWriterSchema::parse_str(V2).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(2, V2).unwrap();
    d.register_writer_schema(1, V1).unwrap();
    d.decode(&frame(1, &avro_bytes(&[0x11]))).unwrap();
    let mut v2body = avro_bytes(&[0x22]);
    v2body.extend_from_slice(&avro_long(3));
    d.decode(&frame(2, &v2body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 2);
    let col = arb_col(&b, 100, 0);
    assert_eq!(
        col.value(0),
        &[0x00, 0x11][..],
        "v1 decimal survived resolution"
    );
    assert_eq!(
        col.value(1),
        &[0x00, 0x22][..],
        "v2 decimal survived resolution"
    );
    let tags = b
        .column(b.schema().index_of("tag").unwrap())
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .expect("tag is int32");
    assert_eq!(tags.value(0), 9, "v1 row gets the reader default");
    assert_eq!(tags.value(1), 3);
}

#[test]
#[ignore = "FINDING: the writer schema's decimal scale is ignored — under skip_schema_resolution a writer decimal(85,4) value is rescaled by the reader's scale, silently multiplying it by 100"]
fn skip_schema_resolution_honours_the_writer_decimal_scale() {
    // `with_schema_resolution(false)` promises "decode each message against its
    // own writer schema with no resolution". The writer says scale 4, so the
    // unscaled 1_234_567 is 123.4567. The reader's scale 2 must not be applied.
    let writer_json = bytes_dec_schema(85, 4);
    let reader_json = bytes_dec_schema(85, 2);
    let reader = AvroWriterSchema::parse_str(&reader_json).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap()
        .with_schema_resolution(false);
    d.register_writer_schema(2, &writer_json).unwrap();
    d.decode(&frame(2, &avro_bytes(&[0x12, 0xD6, 0x87])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    let (_, s) = DecimalArbType::precision_scale_from_field(b.schema().field(0)).unwrap();
    let v = DecimalArbValue::from_canonical_bytes_at_scale(arb_col(&b, 85, s as usize).value(0), s)
        .unwrap();
    assert_eq!(
        v.to_canonical_string(),
        "123.4567",
        "the writer's decimal scale must not be replaced by the reader's"
    );
}

#[test]
#[ignore = "FINDING: with no reader schema the target scale comes from the FIRST registered writer, so a second writer id with a different decimal scale is silently rescaled"]
fn a_second_writer_with_a_different_scale_is_not_silently_rescaled() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(85, 4))
        .unwrap();
    d.register_writer_schema(2, &bytes_dec_schema(85, 2))
        .unwrap();
    // unscaled 1_234_567 written by writer 2 at scale 2 == 12345.67
    d.decode(&frame(2, &avro_bytes(&[0x12, 0xD6, 0x87])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    let (_, s) = DecimalArbType::precision_scale_from_field(b.schema().field(0)).unwrap();
    let v = DecimalArbValue::from_canonical_bytes_at_scale(
        b.column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(0),
        s,
    )
    .unwrap();
    assert_eq!(
        v.to_canonical_string(),
        "12345.67",
        "writer 2's scale (2) must drive its own rows, not writer 1's scale (4)"
    );
}

// ===========================================================================
// 10. Nested decimals (struct / list) on the wire
// ===========================================================================

const NESTED_STRUCT: &str = r#"{"type":"record","name":"R","fields":[
    {"name":"inner","type":{"type":"record","name":"I","fields":[
        {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}
    ]}}
]}"#;

fn nested_amt(batch: &RecordBatch) -> &LargeBinaryArray {
    let st = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("inner is a struct");
    let f = st.fields().iter().find(|f| f.name() == "amt").unwrap();
    assert!(
        DecimalArbType::is_decimal_arb_field(f),
        "nested wide decimal must stay decimal_arb, got {:?}",
        f.data_type()
    );
    st.column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("nested decimal_arb storage is LargeBinary")
}

#[test]
fn nested_wide_decimal_decodes_exactly() {
    let n: BigInt = pow2(200) + 999;
    let batch = decode_bodies(NESTED_STRUCT, &[avro_bytes(&n.to_signed_bytes_be())])
        .expect("decode")
        .expect("batch");
    assert_eq!(
        nested_amt(&batch).value(0),
        expected_canonical(&n).as_slice()
    );
}

#[test]
fn nested_negative_wide_decimal_decodes_exactly() {
    let n: BigInt = -(pow2(199) + BigInt::from(1));
    let batch = decode_bodies(NESTED_STRUCT, &[avro_bytes(&n.to_signed_bytes_be())])
        .expect("decode")
        .expect("batch");
    assert_eq!(
        nested_amt(&batch).value(0),
        expected_canonical(&n).as_slice()
    );
}

#[test]
fn nested_decimal_with_leading_padding_matches_the_minimal_spelling() {
    let n = BigInt::from(1234567i64);
    let minimal = decode_bodies(NESTED_STRUCT, &[avro_bytes(&n.to_signed_bytes_be())])
        .unwrap()
        .unwrap();
    let padded = decode_bodies(NESTED_STRUCT, &[avro_bytes(&be_padded(&n, 24))])
        .unwrap()
        .unwrap();
    assert_eq!(
        nested_amt(&minimal).value(0),
        nested_amt(&padded).value(0),
        "padding a nested decimal must not change its canonical bytes"
    );
}

#[test]
fn nested_decimal_zero_is_canonical_zero() {
    let batch = decode_bodies(NESTED_STRUCT, &[avro_bytes(&[0x00, 0x00, 0x00])])
        .unwrap()
        .unwrap();
    assert_eq!(nested_amt(&batch).value(0), &[0x00][..]);
}

const NESTED_D128: &str = r#"{"type":"record","name":"R","fields":[
    {"name":"inner","type":{"type":"record","name":"I","fields":[
        {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":38,"scale":0}}
    ]}}
]}"#;

#[test]
fn nested_decimal128_accepts_a_sign_extended_over_wide_payload() {
    // 20 bytes, but the top 4 are pure sign extension → fits i128.
    let mut wire = vec![0x00u8; 20];
    wire[19] = 0x2A;
    let batch = decode_bodies(NESTED_D128, &[avro_bytes(&wire)])
        .expect("sign-extended payload must be accepted")
        .expect("batch");
    let st = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .expect("nested decimal(38,0) is Decimal128");
    assert_eq!(amt.value(0), 42i128);
}

#[test]
fn nested_decimal128_rejects_a_genuinely_wider_payload_instead_of_truncating() {
    let n: BigInt = pow2(130);
    let r = decode_bodies(NESTED_D128, &[avro_bytes(&n.to_signed_bytes_be())]);
    assert!(
        r.is_err(),
        "a >128-bit value in a nested Decimal128 column must error, not truncate to 0"
    );
}

#[test]
fn nested_decimal128_negative_sign_extension_is_accepted() {
    let mut wire = vec![0xFFu8; 20];
    wire[19] = 0xD6; // -42
    let batch = decode_bodies(NESTED_D128, &[avro_bytes(&wire)])
        .expect("decode")
        .expect("batch");
    let st = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .unwrap();
    assert_eq!(amt.value(0), -42i128);
}

const NESTED_D256: &str = r#"{"type":"record","name":"R","fields":[
    {"name":"inner","type":{"type":"record","name":"I","fields":[
        {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":60,"scale":0}}
    ]}}
]}"#;

#[test]
fn nested_decimal256_rejects_a_wider_than_256_bit_payload() {
    let n: BigInt = pow2(260);
    let r = decode_bodies(NESTED_D256, &[avro_bytes(&n.to_signed_bytes_be())]);
    assert!(
        r.is_err(),
        "a >256-bit value in a nested Decimal256 column must error, not truncate"
    );
}

#[test]
fn nested_decimal256_accepts_the_full_255_bit_range() {
    let n: BigInt = pow2(255) - 1;
    let batch = decode_bodies(NESTED_D256, &[avro_bytes(&n.to_signed_bytes_be())])
        .expect("decode")
        .expect("batch");
    let st = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Decimal256Array>()
        .expect("nested decimal(60,0) is Decimal256");
    assert_eq!(amt.value(0).to_string(), n.to_string());
}

const LIST_WIDE: &str = r#"{"type":"record","name":"R","fields":[
    {"name":"xs","type":{"type":"array","items":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}}
]}"#;

#[test]
fn list_of_wide_decimals_decodes_every_element_exactly() {
    let values: Vec<BigInt> = vec![
        BigInt::from(0),
        BigInt::from(-1),
        pow2(300) - 5,
        BigInt::from(255),
    ];
    // avro array: block count, items..., 0 terminator
    let mut body = avro_long(values.len() as i64);
    for v in &values {
        body.extend_from_slice(&avro_bytes(&v.to_signed_bytes_be()));
    }
    body.extend_from_slice(&avro_long(0));
    let batch = decode_bodies(LIST_WIDE, &[body])
        .expect("decode")
        .expect("batch");
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("xs is a List");
    let vals = list
        .values()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("list elements are decimal_arb LargeBinary");
    assert_eq!(vals.len(), values.len());
    for (i, v) in values.iter().enumerate() {
        assert_eq!(
            vals.value(i),
            expected_canonical(v).as_slice(),
            "list element {i} misdecoded"
        );
    }
}

#[test]
fn an_empty_list_of_wide_decimals_decodes_to_zero_elements() {
    let batch = decode_bodies(LIST_WIDE, &[avro_long(0)])
        .expect("decode")
        .expect("batch");
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_length(0), 0, "empty avro array → empty list");
}

// ===========================================================================
// 11. coerce_batch_to_target — the byte reinterpretation in isolation
// ===========================================================================

fn arb_target(p: u32, s: u32) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        DecimalArbType::field("v", p, s, true).unwrap(),
    ]))
}

fn src_batch(col: ArrayRef) -> RecordBatch {
    let field = Field::new("v", col.data_type().clone(), true);
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![col]).unwrap()
}

#[test]
fn coerce_binary_source_to_decimal_arb_matches_the_wire_integer() {
    let payloads: Vec<Option<&[u8]>> = vec![
        Some(&[0x00]),
        Some(&[0xFF]),
        Some(&[0x7F]),
        Some(&[0x80]),
        Some(&[0x00, 0x80]),
        None,
    ];
    let src: ArrayRef = Arc::new(BinaryArray::from(payloads.clone()));
    let out = coerce_batch_to_target(&src_batch(src), &arb_target(100, 0)).unwrap();
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    for (i, p) in payloads.iter().enumerate() {
        match p {
            None => assert!(col.is_null(i), "row {i} must stay NULL"),
            Some(bytes) => assert_eq!(
                col.value(i),
                expected_canonical(&BigInt::from_signed_bytes_be(bytes)).as_slice(),
                "row {i} ({bytes:02x?}) misreinterpreted"
            ),
        }
    }
}

#[test]
fn coerce_large_binary_source_to_decimal_arb() {
    let src: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
        Some(&[0x01u8, 0x00][..]),
        Some(&[0xFF, 0x00][..]),
    ]));
    let out = coerce_batch_to_target(&src_batch(src), &arb_target(100, 0)).unwrap();
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(col.value(0), &[0x00, 0x01, 0x00][..], "+256");
    assert_eq!(col.value(1), &[0xFF, 0x01, 0x00][..], "-256");
}

#[test]
fn coerce_fixed_size_binary_source_to_decimal_arb() {
    let vals: Vec<Option<[u8; 4]>> = vec![Some([0x00, 0x00, 0x01, 0x00]), Some([0xFF; 4]), None];
    let src: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(vals.iter().map(|v| v.as_ref()), 4)
            .unwrap(),
    );
    let out = coerce_batch_to_target(&src_batch(src), &arb_target(100, 0)).unwrap();
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(col.value(0), &[0x00, 0x01, 0x00][..]);
    assert_eq!(col.value(1), &[0xFF, 0x01][..], "all-0xFF fixed(4) is -1");
    assert!(col.is_null(2));
}

#[test]
fn coerce_of_an_empty_batch_yields_an_empty_decimal_arb_column() {
    let src: ArrayRef = Arc::new(BinaryArray::from(Vec::<Option<&[u8]>>::new()));
    let out = coerce_batch_to_target(&src_batch(src), &arb_target(100, 0)).unwrap();
    assert_eq!(out.num_rows(), 0);
    assert!(DecimalArbType::is_decimal_arb_field(out.schema().field(0)));
}

#[test]
fn coerce_rejects_a_non_binary_source_for_a_decimal_arb_target() {
    let src: ArrayRef = Arc::new(StringArray::from(vec!["not bytes"]));
    let r = coerce_batch_to_target(&src_batch(src), &arb_target(100, 0));
    assert!(
        r.is_err(),
        "a Utf8 source for a decimal_arb target must be a typed error, not a panic"
    );
}

#[test]
fn coerce_preserves_the_decimal_arb_metadata_on_the_output_field() {
    let src: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[0x01u8][..])]));
    let out = coerce_batch_to_target(&src_batch(src), &arb_target(90, 7)).unwrap();
    let f = out.schema().field(0).clone();
    assert!(DecimalArbType::is_decimal_arb_field(&f));
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((90, 7)),
        "coercion must not drop the declared precision/scale"
    );
}

#[test]
fn coerce_scales_the_value_by_the_target_field_scale() {
    let src: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[0x12u8, 0xD6, 0x87][..])]));
    let out = coerce_batch_to_target(&src_batch(src), &arb_target(90, 4)).unwrap();
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let v = DecimalArbValue::from_canonical_bytes_at_scale(col.value(0), 4).unwrap();
    assert_eq!(v.to_canonical_string(), "123.4567");
    assert_eq!(
        col.value(0),
        &[0x00, 0x12, 0xD6, 0x87][..],
        "the unscaled integer must survive verbatim in the canonical payload"
    );
}

#[test]
fn coerce_of_an_all_null_binary_column_stays_all_null() {
    let src: ArrayRef = Arc::new(BinaryArray::from(vec![None::<&[u8]>, None, None]));
    let out = coerce_batch_to_target(&src_batch(src), &arb_target(100, 0)).unwrap();
    assert_eq!(out.column(0).null_count(), 3);
}

#[test]
fn coerce_of_an_empty_binary_value_is_zero_not_null() {
    let src: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&[][..])]));
    let out = coerce_batch_to_target(&src_batch(src), &arb_target(100, 0)).unwrap();
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert!(!col.is_null(0), "an empty payload is a value, not a NULL");
    assert_eq!(col.value(0), &[0x00][..]);
}

#[test]
fn coerce_reports_a_missing_required_target_field() {
    let src: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
    let batch = src_batch(src);
    let target = Arc::new(Schema::new(vec![
        DecimalArbType::field("missing", 100, 0, false).unwrap(),
    ]));
    assert!(
        coerce_batch_to_target(&batch, &target).is_err(),
        "a missing required target field must error"
    );
}

#[test]
fn coerce_fills_a_missing_nullable_target_field_with_nulls() {
    let src: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
    let batch = src_batch(src);
    let target = Arc::new(Schema::new(vec![
        DecimalArbType::field("absent", 100, 0, true).unwrap(),
    ]));
    let out = coerce_batch_to_target(&batch, &target).unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(out.column(0).null_count(), 3);
}

// ===========================================================================
// 12. Schema-level robustness reachable from the decode path
// ===========================================================================

#[test]
#[ignore = "FINDING: ConfluentAvroDecoder::register_writer_schema returns Result but PANICS on a registry-served decimal with precision > 100 (post_process_avro_schema_for_reading panic!)"]
fn registering_a_writer_schema_with_precision_over_100_errors_instead_of_panicking() {
    let mut d = ConfluentAvroDecoder::new();
    let r = d.register_writer_schema(1, &bytes_dec_schema(101, 0));
    assert!(
        r.is_err(),
        "an unsupported precision must surface as a typed error, not a process panic"
    );
}

#[test]
fn registering_a_writer_schema_with_precision_exactly_100_succeeds() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(
        d.register_writer_schema(1, &bytes_dec_schema(100, 0))
            .is_ok()
    );
}

#[test]
fn registering_malformed_json_is_an_error_not_a_panic() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(d.register_writer_schema(1, "{not json").is_err());
}

#[test]
fn registering_a_json_scalar_is_an_error_not_a_panic() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(
        d.register_writer_schema(1, "12345").is_err(),
        "a non-schema JSON scalar must be rejected"
    );
}

#[test]
fn decoding_before_any_schema_is_registered_is_an_error() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(d.decode(&frame(1, &avro_bytes(&[0x01]))).is_err());
}

#[test]
fn the_target_field_keeps_its_decimal_arb_tag_for_every_precision_above_76() {
    for p in [77usize, 78, 79, 90, 100] {
        let schema = AvroWriterSchema::parse_str(&bytes_dec_schema(p, 0)).unwrap();
        let target = convert_avro_schema_to_arrow(schema);
        assert!(
            DecimalArbType::is_decimal_arb_field(target.field(0)),
            "precision {p} must map to decimal_arb"
        );
        assert_eq!(
            DecimalArbType::precision_scale_from_field(target.field(0)),
            Some((p as u32, 0))
        );
    }
}

#[test]
fn a_decimal_arb_column_decoded_from_the_wire_is_readable_as_a_decimal_arb_array() {
    use streamling_common::types::decimal_arb::DecimalArbArray;
    let schema = bytes_dec_schema(100, 0);
    let n: BigInt = pow2(250) + 1;
    let batch = decode_bodies(&schema, &[avro_bytes(&n.to_signed_bytes_be())])
        .unwrap()
        .unwrap();
    let field = batch.schema().field(0).clone();
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .clone();
    let da = DecimalArbArray::try_from_array_and_field(arr, &field)
        .expect("a decoded column must be a valid DecimalArbArray");
    let v = da.value(0).unwrap().expect("row 0 is not null");
    assert_eq!(v.to_canonical_string(), n.to_string());
}

#[test]
fn decoded_values_compare_equal_to_values_parsed_from_their_decimal_string() {
    use std::str::FromStr;
    for s in [
        "0",
        "1",
        "-1",
        "12345678901234567890123456789012345678901234567890",
    ] {
        let n = BigInt::parse_bytes(s.as_bytes(), 10).unwrap();
        let got = decode_top_bytes(100, 0, &n.to_signed_bytes_be());
        let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&got, 0).unwrap();
        assert_eq!(
            decoded,
            DecimalArbValue::from_str(s).unwrap(),
            "wire-decoded {s} must equal the parsed literal"
        );
    }
}

#[test]
fn numerically_equal_wire_spellings_hash_to_the_same_canonical_bytes() {
    // Join keys and GROUP BY compare the canonical payload byte-for-byte.
    let spellings: Vec<Vec<u8>> = vec![
        vec![0x2A],
        vec![0x00, 0x2A],
        vec![0x00, 0x00, 0x2A],
        vec![0x00; 30]
            .into_iter()
            .chain(std::iter::once(0x2A))
            .collect(),
    ];
    let first = decode_top_bytes(100, 0, &spellings[0]);
    for s in &spellings[1..] {
        assert_eq!(
            decode_top_bytes(100, 0, s),
            first,
            "spelling {s:02x?} must produce identical canonical bytes"
        );
    }
}

#[test]
fn negative_zero_on_the_wire_decodes_to_canonical_positive_zero() {
    // There is no negative zero in two's complement, but a producer can emit
    // 0xFF-padded zero-magnitude bytes; those are -1, not -0. What must never
    // happen is the invalid `[0xFF]` canonical payload.
    for wire in [vec![0x00u8], vec![0x00, 0x00], vec![0x00; 16]] {
        let got = decode_top_bytes(100, 0, &wire);
        assert_eq!(got, vec![0x00], "zero must be the single 0x00 payload");
        assert!(
            DecimalArbValue::from_canonical_bytes_at_scale(&got, 0).is_ok(),
            "the decoded zero payload must be a legal canonical encoding"
        );
    }
}

#[test]
fn every_decoded_payload_is_a_legal_canonical_encoding() {
    // Round-trip closure: whatever the wire says, the emitted payload must be
    // accepted by `from_canonical_bytes_at_scale` (never `[0xFF]`, never empty).
    let wires: Vec<Vec<u8>> = vec![
        vec![],
        vec![0x00],
        vec![0xFF],
        vec![0x80],
        vec![0x7F],
        vec![0x00, 0x00],
        vec![0xFF, 0xFF],
        vec![0xFF, 0x00],
        vec![0x00; 33],
        vec![0xFF; 33],
    ];
    for w in &wires {
        let got = decode_top_bytes(100, 0, w);
        assert!(!got.is_empty(), "payload for {w:02x?} must not be empty");
        assert!(
            got[0] == 0x00 || got[0] == 0xFF,
            "payload for {w:02x?} has an illegal sign byte {:#04x}",
            got[0]
        );
        assert!(
            !(got[0] == 0xFF && got.len() == 1),
            "payload for {w:02x?} is the illegal negative-zero encoding"
        );
        DecimalArbValue::from_canonical_bytes_at_scale(&got, 0)
            .unwrap_or_else(|e| panic!("payload for {w:02x?} is not decodable: {e}"));
    }
}

#[test]
fn the_decoded_payload_never_carries_a_redundant_leading_magnitude_zero() {
    for w in [
        vec![0x00u8, 0x00, 0x01],
        vec![0x00; 20],
        vec![0xFF, 0xFF, 0xFE],
    ] {
        let got = decode_top_bytes(100, 0, &w);
        assert!(
            got.len() == 1 || got[1] != 0x00,
            "payload {got:02x?} for wire {w:02x?} is not minimal"
        );
    }
}

#[test]
fn a_wide_positive_value_never_flips_sign() {
    // A magnitude whose top bit is set must have been sign-extended by the
    // producer; if it was not, the value is negative by definition. Verify both
    // spellings land where two's complement says they do.
    let with_pad = decode_top_bytes(100, 0, &[0x00, 0xFF, 0xFF]);
    let without_pad = decode_top_bytes(100, 0, &[0xFF, 0xFF]);
    assert_eq!(with_pad, vec![0x00, 0xFF, 0xFF], "0x00FFFF is +65535");
    assert_eq!(without_pad, vec![0xFF, 0x01], "0xFFFF is -1");
    assert_ne!(with_pad, without_pad);
}

#[test]
fn decode_returns_a_non_zero_consumed_count_for_a_complete_frame() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    let n = d.decode(&frame(1, &avro_bytes(&[0x01]))).unwrap();
    assert!(n > 0, "a complete frame must report progress, got {n}");
}

#[test]
fn many_frames_in_sequence_do_not_leak_rows_between_flushes() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &bytes_dec_schema(100, 0))
        .unwrap();
    for round in 0..5u8 {
        for i in 0..4u8 {
            d.decode(&frame(1, &avro_bytes(&[round * 4 + i + 1])))
                .unwrap();
        }
        let b = d.flush().unwrap().expect("batch");
        assert_eq!(
            b.num_rows(),
            4,
            "round {round} must contain exactly its own rows"
        );
        let col = arb_col(&b, 100, 0);
        for i in 0..4usize {
            assert_eq!(col.value(i), &[0x00, round * 4 + i as u8 + 1][..]);
        }
    }
}
