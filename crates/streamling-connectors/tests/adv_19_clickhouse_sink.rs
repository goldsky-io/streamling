//! Adversarial coverage for the ClickHouse **sink** side of `decimal_arb`:
//!
//!   * `streamling_connectors::table_providers::clickhouse::decimal_arb_to_clickhouse_native`
//!     — canonical `[sign][BE magnitude]` → 32-byte **little-endian**
//!     two's-complement `FixedSizeBinary(32)` for ClickHouse `UInt256` /
//!     `Int256`.
//!   * `ClickHouseClient::clickhouse_column_type` — the DDL type the sink
//!     declares for a column (`UInt256` / `Int256` / `Decimal(p, s)` /
//!     `String` / hard rejection).
//!
//! Why this matters: a wrong-endian or wrong-sign encode is **completely
//! silent** on the write path. The INSERT succeeds, ClickHouse stores the
//! bytes verbatim, and the corruption only surfaces as garbage numbers in the
//! warehouse. Every byte-level assertion below therefore uses **asymmetric**
//! values (distinct bytes in every position, magnitudes that are not
//! palindromes) so that a byte-reversal, a nibble swap, or a sign flip cannot
//! pass by accident.
//!
//! All oracles here are deliberately independent re-implementations
//! (schoolbook base-256 arithmetic + Arrow's `i256` wrapping arithmetic), not
//! calls back into the product's BigInt path.
//!
//! Pure in-process: no network, no filesystem, no sleeps, no randomness.

use arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array,
    LargeBinaryArray, LargeBinaryBuilder, StringArray,
};
use arrow::datatypes::i256 as ArrowI256;
use arrow_schema::{DataType, Field, FieldRef};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use streamling_config::{CoercionTarget, ColumnDirective};
use streamling_connectors::table_providers::clickhouse::{
    ClickHouseClient, clickhouse_native_to_decimal_arb, decimal_arb_to_clickhouse_native,
};
use streamling_core::types::decimal_arb::{DecimalArbType, DecimalArbValue, NativeIntKind};

// ===========================================================================
// Well-known 256-bit boundary constants (exact decimal expansions)
// ===========================================================================

/// 2^256 − 1 — UInt256 max, 78 digits.
const U256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";
/// 2^256 — one past UInt256 max. Does not fit 32 bytes.
const TWO_POW_256: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639936";
/// 2^255 — first value that does NOT fit a signed Int256.
const TWO_POW_255: &str =
    "57896044618658097711785492504343953926634992332820282019728792003956564819968";
/// 2^255 − 1 — Int256 max.
const I256_MAX: &str =
    "57896044618658097711785492504343953926634992332820282019728792003956564819967";
/// −2^255 — Int256 min.
const I256_MIN: &str =
    "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
/// −(2^255 + 1) — one below Int256 min.
const BELOW_I256_MIN: &str =
    "-57896044618658097711785492504343953926634992332820282019728792003956564819969";
/// −(2^256 − 1) — magnitude fits 32 bytes but the signed value is far below
/// Int256 min.
const NEG_U256_MAX: &str =
    "-115792089237316195423570985008687907853269984665640564039457584007913129639935";
/// 10^77 — 78 digits, still below 2^256 − 1.
const TEN_POW_77: &str =
    "100000000000000000000000000000000000000000000000000000000000000000000000000000";
/// 10^76 — 77 digits, comfortably inside the signed Int256 range (2^255 ≈ 5.79e76).
const TEN_POW_76: &str =
    "10000000000000000000000000000000000000000000000000000000000000000000000000000";
/// 78 nines — fits decimal_arb(78, 0) but exceeds 2^256 − 1.
const NINE_78: &str =
    "999999999999999999999999999999999999999999999999999999999999999999999999999999";

// ===========================================================================
// Independent oracles
// ===========================================================================

/// Schoolbook base-256 accumulation: decimal digit string → 32-byte
/// big-endian buffer. Panics if the value does not fit 32 bytes.
fn be32_abs(digits: &str) -> [u8; 32] {
    let mut acc = [0u8; 32];
    for ch in digits.bytes() {
        assert!(
            ch.is_ascii_digit(),
            "oracle wants digits only, got {digits:?}"
        );
        let mut carry = u16::from(ch - b'0');
        for b in acc.iter_mut().rev() {
            let v = u16::from(*b) * 10 + carry;
            *b = (v & 0xFF) as u8;
            carry = v >> 8;
        }
        assert_eq!(carry, 0, "oracle overflow: {digits} does not fit 32 bytes");
    }
    acc
}

/// Two's complement (arithmetic negation) of a 32-byte big-endian buffer.
fn twos(mut be: [u8; 32]) -> [u8; 32] {
    for b in be.iter_mut() {
        *b = !*b;
    }
    let mut carry = 1u16;
    for b in be.iter_mut().rev() {
        let v = u16::from(*b) + carry;
        *b = (v & 0xFF) as u8;
        carry = v >> 8;
        if carry == 0 {
            break;
        }
    }
    be
}

/// Big-endian → little-endian.
fn to_le(mut be: [u8; 32]) -> [u8; 32] {
    be.reverse();
    be
}

/// Expected ClickHouse 32-byte LE payload for a signed decimal integer string.
/// This is the oracle every byte-level test compares against.
fn expected_le(dec: &str) -> [u8; 32] {
    match dec.strip_prefix('-') {
        Some(mag) => to_le(twos(be32_abs(mag))),
        None => to_le(be32_abs(dec.strip_prefix('+').unwrap_or(dec))),
    }
}

/// Second, structurally different oracle: accumulate the value with Arrow's
/// 256-bit wrapping arithmetic and ask *it* for the LE bytes. Two's-complement
/// bit patterns are identical for signed and unsigned interpretation, so this
/// is valid for the whole `[-2^255, 2^256)` span.
fn expected_le_via_arrow_i256(dec: &str) -> [u8; 32] {
    let neg = dec.starts_with('-');
    let digits = dec.trim_start_matches(['-', '+']);
    let mut acc = ArrowI256::ZERO;
    let ten = ArrowI256::from_i128(10);
    for ch in digits.bytes() {
        assert!(ch.is_ascii_digit(), "oracle wants digits only");
        acc = acc
            .wrapping_mul(ten)
            .wrapping_add(ArrowI256::from_i128(i128::from(ch - b'0')));
    }
    if neg {
        acc = acc.wrapping_neg();
    }
    acc.to_le_bytes()
}

/// Long division by 10: 32-byte big-endian buffer → decimal digit string.
/// Lets a test start from an arbitrary *byte pattern* and derive the decimal
/// literal to feed the product, which is the sharpest possible endianness
/// probe.
fn dec_from_be32(be: &[u8; 32]) -> String {
    let mut cur = *be;
    let mut digits = Vec::new();
    while cur.iter().any(|&b| b != 0) {
        let mut rem: u16 = 0;
        for b in cur.iter_mut() {
            let v = (rem << 8) | u16::from(*b);
            *b = (v / 10) as u8;
            rem = v % 10;
        }
        digits.push(b'0' + rem as u8);
    }
    if digits.is_empty() {
        return "0".to_string();
    }
    digits.reverse();
    String::from_utf8(digits).unwrap()
}

// ===========================================================================
// Fixtures
// ===========================================================================

fn arb_field(name: &str, p: u32, s: u32) -> Field {
    DecimalArbType::field(name, p, s, true).unwrap()
}

fn hinted_field(name: &str, p: u32, s: u32, kind: NativeIntKind) -> FieldRef {
    Arc::new(DecimalArbType::with_native_int_kind(arb_field(name, p, s), kind).unwrap())
}

/// Standard wide-int column shape used by the byte tests: decimal_arb(78, 0).
fn u256_field() -> FieldRef {
    hinted_field("balance", 78, 0, NativeIntKind::U256)
}

fn i256_field() -> FieldRef {
    hinted_field("delta", 78, 0, NativeIntKind::I256)
}

fn canon(dec: &str, scale: u32) -> Vec<u8> {
    DecimalArbValue::from_str(dec)
        .unwrap()
        .to_canonical_bytes_at_scale(scale)
}

fn arr_of(decs: &[&str], scale: u32) -> LargeBinaryArray {
    let mut b = LargeBinaryBuilder::new();
    for d in decs {
        b.append_value(canon(d, scale));
    }
    b.finish()
}

fn arr_opt(decs: &[Option<&str>], scale: u32) -> LargeBinaryArray {
    let mut b = LargeBinaryBuilder::new();
    for d in decs {
        match d {
            Some(x) => b.append_value(canon(x, scale)),
            None => b.append_null(),
        }
    }
    b.finish()
}

/// Build a `LargeBinaryArray` straight from raw canonical byte payloads
/// (lets tests inject payloads the canonical encoder would never produce).
fn arr_raw(rows: &[Option<&[u8]>]) -> LargeBinaryArray {
    let mut b = LargeBinaryBuilder::new();
    for r in rows {
        match r {
            Some(x) => b.append_value(x),
            None => b.append_null(),
        }
    }
    b.finish()
}

fn fsb(out: &ArrayRef) -> &FixedSizeBinaryArray {
    out.as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("sink must emit FixedSizeBinary(32)")
}

/// Encode one value and return its 32 LE bytes.
fn enc_one(dec: &str, field: &FieldRef) -> [u8; 32] {
    let arr = arr_of(&[dec], 0);
    let out = decimal_arb_to_clickhouse_native(&arr, field)
        .unwrap_or_else(|e| panic!("encode of {dec} failed: {e}"));
    let f = fsb(&out);
    let mut buf = [0u8; 32];
    buf.copy_from_slice(f.value(0));
    buf
}

fn enc_err(dec: &str, field: &FieldRef) -> String {
    let arr = arr_of(&[dec], 0);
    decimal_arb_to_clickhouse_native(&arr, field)
        .map(|_| ())
        .expect_err("expected the sink to reject this value, not encode it")
        .to_string()
}

// ===========================================================================
// 0. Oracle self-checks (a broken oracle would invalidate everything below)
// ===========================================================================

#[test]
fn oracle_roundtrips_small_decimal_strings() {
    for s in ["0", "1", "9", "10", "255", "256", "65535", "123456789"] {
        assert_eq!(dec_from_be32(&be32_abs(s)), s, "oracle broken for {s}");
    }
}

#[test]
fn oracle_roundtrips_u256_max() {
    assert_eq!(
        dec_from_be32(&be32_abs(U256_MAX)),
        U256_MAX,
        "oracle must round-trip the 78-digit UInt256 maximum"
    );
}

#[test]
fn oracle_u256_max_is_all_ones_be() {
    assert_eq!(
        be32_abs(U256_MAX),
        [0xFFu8; 32],
        "2^256-1 must be 32 bytes of 0xFF"
    );
}

#[test]
fn oracle_two_pow_255_has_only_the_top_bit_be() {
    let be = be32_abs(TWO_POW_255);
    assert_eq!(be[0], 0x80, "2^255 BE byte 0 must be 0x80");
    assert!(
        be[1..].iter().all(|&b| b == 0),
        "2^255 BE tail must be zero"
    );
}

#[test]
fn oracle_two_independent_oracles_agree_on_positive_values() {
    for s in ["0", "1", "255", "256", I256_MAX, TEN_POW_77, U256_MAX] {
        assert_eq!(
            expected_le(s),
            expected_le_via_arrow_i256(s),
            "the two oracles disagree on {s}; one of them is wrong"
        );
    }
}

#[test]
fn oracle_two_independent_oracles_agree_on_negative_values() {
    for s in ["-1", "-255", "-256", I256_MIN, NEG_U256_MAX] {
        assert_eq!(
            expected_le(s),
            expected_le_via_arrow_i256(s),
            "the two oracles disagree on {s}; one of them is wrong"
        );
    }
}

// ===========================================================================
// 1. Endianness — asymmetric byte patterns (the core of this file)
// ===========================================================================

/// A byte pattern with 32 *distinct* values, no symmetry whatsoever.
/// Reversing it, rotating it, or swapping halves all produce different bytes.
fn asymmetric_pattern() -> [u8; 32] {
    let mut be = [0u8; 32];
    for (i, b) in be.iter_mut().enumerate() {
        // 0x11, 0x22, 0x33 ... strictly increasing, top byte 0x11 keeps the
        // value inside Int256 (sign bit clear) so both kinds accept it.
        *b = ((i as u8) + 1).wrapping_mul(7).wrapping_add(0x03);
    }
    be[0] = 0x11; // clear the sign bit deliberately
    be
}

#[test]
fn u256_asymmetric_pattern_is_emitted_byte_reversed() {
    let be = asymmetric_pattern();
    let dec = dec_from_be32(&be);
    let got = enc_one(&dec, &u256_field());
    assert_eq!(
        got,
        to_le(be),
        "UInt256 emission must be the exact byte reversal of the big-endian magnitude"
    );
}

#[test]
fn i256_asymmetric_pattern_is_emitted_byte_reversed() {
    let be = asymmetric_pattern();
    let dec = dec_from_be32(&be);
    let got = enc_one(&dec, &i256_field());
    assert_eq!(
        got,
        to_le(be),
        "Int256 emission of a positive value must equal the UInt256 emission"
    );
}

#[test]
fn asymmetric_pattern_encoding_is_not_big_endian() {
    // Guards specifically against "forgot to reverse".
    let be = asymmetric_pattern();
    let dec = dec_from_be32(&be);
    let got = enc_one(&dec, &u256_field());
    assert_ne!(got, be, "emitted bytes must NOT be big-endian");
}

#[test]
fn asymmetric_pattern_low_byte_lands_at_index_zero() {
    let be = asymmetric_pattern();
    let dec = dec_from_be32(&be);
    let got = enc_one(&dec, &u256_field());
    assert_eq!(
        got[0], be[31],
        "LE byte 0 must be the least-significant byte of the value"
    );
    assert_eq!(
        got[31], be[0],
        "LE byte 31 must be the most-significant byte of the value"
    );
}

#[test]
fn negative_asymmetric_pattern_is_twos_complement_le() {
    let be = asymmetric_pattern();
    let dec = format!("-{}", dec_from_be32(&be));
    let got = enc_one(&dec, &i256_field());
    assert_eq!(
        got,
        to_le(twos(be)),
        "negative Int256 must be two's complement of the magnitude, then LE"
    );
}

#[test]
fn negative_asymmetric_pattern_is_not_sign_magnitude() {
    // A sign-magnitude encoder would emit the magnitude with a sign bit set;
    // ClickHouse Int256 is two's complement.
    let be = asymmetric_pattern();
    let dec = format!("-{}", dec_from_be32(&be));
    let got = enc_one(&dec, &i256_field());
    let mut sign_magnitude = be;
    sign_magnitude[0] |= 0x80;
    assert_ne!(
        got,
        to_le(sign_magnitude),
        "Int256 must not be encoded sign-magnitude"
    );
}

#[test]
fn value_256_puts_one_in_le_byte_one() {
    let got = enc_one("256", &u256_field());
    assert_eq!(got[0], 0x00, "256 LE byte 0 must be 0x00");
    assert_eq!(got[1], 0x01, "256 LE byte 1 must be 0x01");
    assert!(got[2..].iter().all(|&b| b == 0), "rest must be zero");
}

#[test]
fn value_255_puts_ff_in_le_byte_zero_only() {
    let got = enc_one("255", &u256_field());
    assert_eq!(got[0], 0xFF);
    assert!(got[1..].iter().all(|&b| b == 0), "255 occupies one byte");
}

#[test]
fn value_two_pow_248_puts_one_in_the_top_le_byte() {
    // 2^248 == 0x01 followed by 31 zero bytes big-endian.
    let mut be = [0u8; 32];
    be[0] = 1;
    let dec = dec_from_be32(&be);
    let got = enc_one(&dec, &u256_field());
    assert_eq!(got[31], 0x01, "2^248 must set the most-significant LE byte");
    assert!(got[..31].iter().all(|&b| b == 0));
}

#[test]
fn multi_byte_value_matches_arrow_i256_le_bytes() {
    // Cross-oracle: Arrow's own 256-bit type must agree byte for byte.
    for dec in [
        "1",
        "255",
        "256",
        "65537",
        "4294967297",
        "18446744073709551617",
        I256_MAX,
        TEN_POW_77,
    ] {
        assert_eq!(
            enc_one(dec, &u256_field()),
            expected_le_via_arrow_i256(dec),
            "sink LE bytes must match arrow i256::to_le_bytes for {dec}"
        );
    }
}

#[test]
fn negative_values_match_arrow_i256_le_bytes() {
    for dec in [
        "-1",
        "-2",
        "-255",
        "-256",
        "-65537",
        "-18446744073709551617",
        I256_MIN,
    ] {
        assert_eq!(
            enc_one(dec, &i256_field()),
            expected_le_via_arrow_i256(dec),
            "sink LE bytes must match arrow i256::to_le_bytes for {dec}"
        );
    }
}

#[test]
fn every_power_of_two_lands_in_exactly_one_le_bit() {
    // 2^k for k = 0..248 step 8 -> exactly one 0x01 byte, at LE index k/8.
    for k in 0..32usize {
        let mut be = [0u8; 32];
        be[31 - k] = 0x01;
        let dec = dec_from_be32(&be);
        let got = enc_one(&dec, &u256_field());
        assert_eq!(got[k], 0x01, "2^(8*{k}) must set LE byte {k}");
        assert_eq!(
            got.iter().filter(|&&b| b != 0).count(),
            1,
            "2^(8*{k}) must set exactly one byte"
        );
    }
}

#[test]
fn each_byte_position_is_independently_addressable() {
    // Set byte i (BE) to a unique marker and check it lands at LE 31-i.
    for i in 1..32usize {
        let mut be = [0u8; 32];
        be[i] = 0xA5;
        let dec = dec_from_be32(&be);
        let got = enc_one(&dec, &u256_field());
        assert_eq!(
            got[31 - i],
            0xA5,
            "BE byte {i} must land at LE byte {}",
            31 - i
        );
    }
}

#[test]
fn ten_pow_77_matches_oracle_exactly() {
    assert_eq!(
        enc_one(TEN_POW_77, &u256_field()),
        expected_le(TEN_POW_77),
        "10^77 must encode to the oracle's LE bytes"
    );
}

#[test]
fn u256_max_is_all_ones_le() {
    assert_eq!(
        enc_one(U256_MAX, &u256_field()),
        [0xFFu8; 32],
        "2^256-1 must be 32 bytes of 0xFF"
    );
}

#[test]
fn i256_max_top_le_byte_is_7f() {
    let got = enc_one(I256_MAX, &i256_field());
    assert_eq!(got[31], 0x7F, "Int256 max top LE byte must be 0x7F");
    assert!(got[..31].iter().all(|&b| b == 0xFF));
    assert_eq!(got, expected_le(I256_MAX));
}

#[test]
fn i256_min_top_le_byte_is_80() {
    let got = enc_one(I256_MIN, &i256_field());
    assert_eq!(got[31], 0x80, "Int256 min top LE byte must be 0x80");
    assert!(got[..31].iter().all(|&b| b == 0x00));
    assert_eq!(got, expected_le(I256_MIN));
}

#[test]
fn i256_min_matches_arrow_min_constant() {
    assert_eq!(
        enc_one(I256_MIN, &i256_field()),
        ArrowI256::MIN.to_le_bytes(),
        "Int256 min must match arrow's i256::MIN bit pattern"
    );
}

#[test]
fn i256_max_matches_arrow_max_constant() {
    assert_eq!(
        enc_one(I256_MAX, &i256_field()),
        ArrowI256::MAX.to_le_bytes(),
        "Int256 max must match arrow's i256::MAX bit pattern"
    );
}

#[test]
fn u256_value_above_i256_max_keeps_unsigned_bit_pattern() {
    // 2^255 under u256 is legal and its bit pattern has the top bit set.
    let got = enc_one(TWO_POW_255, &u256_field());
    assert_eq!(
        got[31], 0x80,
        "2^255 as UInt256 sets the top LE byte to 0x80"
    );
    assert!(got[..31].iter().all(|&b| b == 0));
    assert_eq!(got, expected_le(TWO_POW_255));
}

#[test]
fn negation_is_exact_for_a_battery_of_magnitudes() {
    for dec in ["1", "7", "255", "256", "65535", "4294967296", TEN_POW_76] {
        let pos = enc_one(dec, &i256_field());
        let neg = enc_one(&format!("-{dec}"), &i256_field());
        // pos + neg must be zero mod 2^256.
        let mut carry = 0u16;
        for i in 0..32 {
            let s = u16::from(pos[i]) + u16::from(neg[i]) + carry;
            assert_eq!(s & 0xFF, 0, "byte {i} of {dec} + -{dec} must be zero");
            carry = s >> 8;
        }
        assert_eq!(carry, 1, "the final carry out must be the 2^256 wrap");
    }
}

// ===========================================================================
// 2. Negative values into an unsigned UInt256 channel — MUST error
// ===========================================================================

#[test]
fn u256_rejects_negative_one_rather_than_wrapping() {
    let arr = arr_of(&["-1"], 0);
    let res = decimal_arb_to_clickhouse_native(&arr, &u256_field());
    assert!(
        res.is_err(),
        "-1 into a UInt256 column must error, not wrap to 2^256-1"
    );
}

#[test]
fn u256_negative_one_does_not_silently_become_u256_max() {
    // The specific silent corruption we care about.
    match decimal_arb_to_clickhouse_native(&arr_of(&["-1"], 0), &u256_field()) {
        Err(_) => {}
        Ok(out) => panic!(
            "-1 encoded into UInt256 as {:?} instead of erroring",
            fsb(&out).value(0)
        ),
    }
}

#[test]
fn u256_rejects_small_negative_values() {
    for dec in ["-1", "-2", "-9", "-255", "-256", "-1000000"] {
        let msg = enc_err(dec, &u256_field());
        assert!(
            msg.contains("negative"),
            "error for {dec} must say the value is negative: {msg}"
        );
    }
}

#[test]
fn u256_rejects_int256_min() {
    let msg = enc_err(I256_MIN, &u256_field());
    assert!(msg.contains("negative"), "{msg}");
}

#[test]
fn u256_rejects_negative_u256_max() {
    let msg = enc_err(NEG_U256_MAX, &u256_field());
    assert!(msg.contains("negative"), "{msg}");
}

#[test]
fn u256_negative_error_names_the_column() {
    let field = hinted_field("wallet_balance", 78, 0, NativeIntKind::U256);
    let msg = enc_err("-42", &field);
    assert!(
        msg.contains("wallet_balance"),
        "error must name the offending column: {msg}"
    );
}

#[test]
fn u256_negative_error_names_the_hint_and_remediation() {
    let msg = enc_err("-42", &u256_field());
    assert!(
        msg.contains("native_int_kind=u256"),
        "error must name the violated contract: {msg}"
    );
    assert!(
        msg.contains("i256") || msg.contains("coerce_to"),
        "error must offer a remediation: {msg}"
    );
}

#[test]
fn u256_negative_error_reports_the_offending_row_index() {
    let arr = arr_of(&["1", "2", "3", "-4", "5"], 0);
    let msg = decimal_arb_to_clickhouse_native(&arr, &u256_field())
        .map(|_| ())
        .expect_err("must reject")
        .to_string();
    assert!(
        msg.contains("row 3"),
        "error must point at the real row index (3): {msg}"
    );
}

#[test]
fn u256_negative_after_nulls_reports_the_array_index_not_the_value_index() {
    let arr = arr_opt(&[None, None, Some("7"), Some("-7")], 0);
    let msg = decimal_arb_to_clickhouse_native(&arr, &u256_field())
        .map(|_| ())
        .expect_err("must reject")
        .to_string();
    assert!(
        msg.contains("row 3"),
        "row index must count NULLs too, expected row 3: {msg}"
    );
}

#[test]
fn u256_rejects_a_batch_where_only_the_last_row_is_negative() {
    let mut decs: Vec<String> = (0..64).map(|i| i.to_string()).collect();
    decs.push("-1".to_string());
    let refs: Vec<&str> = decs.iter().map(|s| s.as_str()).collect();
    let arr = arr_of(&refs, 0);
    assert!(
        decimal_arb_to_clickhouse_native(&arr, &u256_field()).is_err(),
        "one bad row in 65 must fail the whole batch"
    );
}

#[test]
fn i256_accepts_the_negatives_u256_rejects() {
    for dec in ["-1", "-255", I256_MIN] {
        assert!(
            decimal_arb_to_clickhouse_native(&arr_of(&[dec], 0), &i256_field()).is_ok(),
            "{dec} must be accepted on the signed channel"
        );
        assert!(
            decimal_arb_to_clickhouse_native(&arr_of(&[dec], 0), &u256_field()).is_err(),
            "{dec} must be rejected on the unsigned channel"
        );
    }
}

// ===========================================================================
// 3. Magnitude / range overflow
// ===========================================================================

#[test]
fn u256_rejects_two_pow_256() {
    let msg = enc_err(TWO_POW_256, &u256_field());
    assert!(
        msg.contains("32 bytes") || msg.contains("out of range"),
        "2^256 must be rejected as out of range: {msg}"
    );
}

#[test]
fn u256_rejects_78_nines() {
    let msg = enc_err(NINE_78, &u256_field());
    assert!(
        msg.contains("UInt256") || msg.contains("32 bytes"),
        "78 nines exceeds 2^256-1 and must be rejected: {msg}"
    );
}

#[test]
fn u256_overflow_error_names_the_column_and_row() {
    let field = hinted_field("supply", 78, 0, NativeIntKind::U256);
    let arr = arr_of(&["1", NINE_78], 0);
    let msg = decimal_arb_to_clickhouse_native(&arr, &field)
        .map(|_| ())
        .expect_err("must reject")
        .to_string();
    assert!(msg.contains("supply"), "{msg}");
    assert!(msg.contains("row 1"), "{msg}");
}

#[test]
fn u256_accepts_exactly_u256_max_but_not_one_more() {
    assert!(decimal_arb_to_clickhouse_native(&arr_of(&[U256_MAX], 0), &u256_field()).is_ok());
    assert!(decimal_arb_to_clickhouse_native(&arr_of(&[TWO_POW_256], 0), &u256_field()).is_err());
}

#[test]
fn i256_rejects_exactly_two_pow_255() {
    let msg = enc_err(TWO_POW_255, &i256_field());
    assert!(
        msg.contains("2^255") || msg.contains("Int256"),
        "2^255 must be rejected on the signed channel: {msg}"
    );
}

#[test]
fn i256_accepts_two_pow_255_minus_one() {
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&[I256_MAX], 0), &i256_field()).is_ok(),
        "2^255-1 is exactly representable and must be accepted"
    );
}

#[test]
fn i256_two_pow_255_boundary_is_exclusive_on_the_signed_side() {
    assert!(decimal_arb_to_clickhouse_native(&arr_of(&[I256_MAX], 0), &i256_field()).is_ok());
    assert!(decimal_arb_to_clickhouse_native(&arr_of(&[TWO_POW_255], 0), &i256_field()).is_err());
}

#[test]
fn u256_two_pow_255_boundary_is_inclusive_on_the_unsigned_side() {
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&[TWO_POW_255], 0), &u256_field()).is_ok(),
        "2^255 is a perfectly ordinary UInt256 value"
    );
}

#[test]
fn i256_two_pow_255_does_not_silently_flip_negative() {
    match decimal_arb_to_clickhouse_native(&arr_of(&[TWO_POW_255], 0), &i256_field()) {
        Err(_) => {}
        Ok(out) => {
            let b = fsb(&out).value(0);
            panic!("2^255 encoded as Int256 {b:?} — this reads back as -2^255 in ClickHouse");
        }
    }
}

#[test]
fn i256_rejects_u256_max() {
    let msg = enc_err(U256_MAX, &i256_field());
    assert!(msg.contains("delta"), "must name the column: {msg}");
}

#[test]
fn i256_rejects_every_positive_with_the_sign_bit_set() {
    // Sample the space just above 2^255.
    for extra in ["0", "1", "2", "1000000000000000000000"] {
        let mut be = be32_abs(TWO_POW_255);
        // add `extra` by re-deriving through the oracle
        let base = dec_from_be32(&be);
        let combined = add_decimal(&base, extra);
        be = be32_abs(&combined);
        assert_eq!(be[0] & 0x80, 0x80, "test setup: sign bit must be set");
        assert!(
            decimal_arb_to_clickhouse_native(&arr_of(&[combined.as_str()], 0), &i256_field())
                .is_err(),
            "{combined} has the Int256 sign bit set and must be rejected"
        );
    }
}

/// Tiny decimal addition helper for the test above (schoolbook, non-negative).
fn add_decimal(a: &str, b: &str) -> String {
    let mut out = Vec::new();
    let (mut ai, mut bi) = (a.bytes().rev(), b.bytes().rev());
    let mut carry = 0u8;
    loop {
        let x = ai.next().map(|c| c - b'0');
        let y = bi.next().map(|c| c - b'0');
        if x.is_none() && y.is_none() && carry == 0 {
            break;
        }
        let s = x.unwrap_or(0) + y.unwrap_or(0) + carry;
        out.push(b'0' + (s % 10));
        carry = s / 10;
    }
    if out.is_empty() {
        return "0".to_string();
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}

#[test]
fn i256_accepts_exactly_negative_two_pow_255() {
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&[I256_MIN], 0), &i256_field()).is_ok(),
        "-2^255 is Int256 min and must be accepted"
    );
}

#[test]
fn i256_rejects_one_below_negative_two_pow_255() {
    let msg = enc_err(BELOW_I256_MIN, &i256_field());
    assert!(
        msg.contains("Int256") || msg.contains("-2^255") || msg.contains("negative"),
        "must name the signed-range constraint: {msg}"
    );
}

#[test]
fn i256_below_min_does_not_silently_flip_positive() {
    match decimal_arb_to_clickhouse_native(&arr_of(&[BELOW_I256_MIN], 0), &i256_field()) {
        Err(_) => {}
        Ok(out) => {
            let b = fsb(&out).value(0);
            panic!("-(2^255+1) encoded as {b:?} — this reads back positive in ClickHouse");
        }
    }
}

#[test]
fn i256_rejects_negative_u256_max() {
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&[NEG_U256_MAX], 0), &i256_field()).is_err(),
        "-(2^256-1) is far below Int256 min and must be rejected"
    );
}

#[test]
fn i256_rejects_negative_magnitude_over_32_bytes() {
    let dec = format!("-{TWO_POW_256}");
    let msg = enc_err(&dec, &i256_field());
    assert!(
        msg.contains("32 bytes") || msg.contains("Int256") || msg.contains("range"),
        "{msg}"
    );
}

#[test]
fn overflow_error_mentions_the_native_type_name() {
    let msg = enc_err(TWO_POW_256, &u256_field());
    assert!(
        msg.contains("UInt256"),
        "UInt256 overflow must name UInt256: {msg}"
    );
    let msg = enc_err(&format!("-{TWO_POW_256}"), &i256_field());
    assert!(
        msg.contains("Int256"),
        "Int256 overflow must name Int256: {msg}"
    );
}

// ===========================================================================
// 4. Zero and negative zero
// ===========================================================================

#[test]
fn zero_encodes_to_all_zero_bytes_u256() {
    assert_eq!(enc_one("0", &u256_field()), [0u8; 32]);
}

#[test]
fn zero_encodes_to_all_zero_bytes_i256() {
    assert_eq!(enc_one("0", &i256_field()), [0u8; 32]);
}

#[test]
fn negative_zero_encodes_identically_to_positive_zero() {
    assert_eq!(
        enc_one("-0", &u256_field()),
        enc_one("0", &u256_field()),
        "-0 must not take the negative branch"
    );
    assert_eq!(
        enc_one("-0", &i256_field()),
        [0u8; 32],
        "-0 must encode to all zero bytes on the signed channel too"
    );
}

#[test]
fn negative_zero_is_not_rejected_by_the_unsigned_channel() {
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&["-0"], 0), &u256_field()).is_ok(),
        "-0 canonicalizes to +0 and must be accepted by a UInt256 column"
    );
}

#[test]
fn zero_spellings_all_produce_identical_bytes() {
    let base = enc_one("0", &u256_field());
    for s in ["0", "-0", "+0", "00", "0.0", "0.000", "-0.0", "000000"] {
        assert_eq!(
            enc_one(s, &u256_field()),
            base,
            "{s} must encode identically to 0"
        );
    }
}

#[test]
fn positive_spellings_all_produce_identical_bytes() {
    let base = enc_one("5", &u256_field());
    for s in ["5", "+5", "05", "5.0", "5.00", "0005"] {
        assert_eq!(
            enc_one(s, &u256_field()),
            base,
            "{s} must encode identically to 5 (GROUP BY / join keys depend on it)"
        );
    }
}

#[test]
fn raw_negative_zero_canonical_payload_is_rejected_by_u256() {
    // [0xFF] with an empty magnitude is the illegal "negative zero" canonical
    // payload the decoder explicitly rejects. The sink must not accept it.
    let arr = arr_raw(&[Some(&[0xFFu8])]);
    assert!(
        decimal_arb_to_clickhouse_native(&arr, &u256_field()).is_err(),
        "illegal negative-zero canonical payload must not encode"
    );
}

#[test]
fn raw_negative_zero_canonical_payload_is_rejected_by_i256() {
    let arr = arr_raw(&[Some(&[0xFFu8])]);
    assert!(
        decimal_arb_to_clickhouse_native(&arr, &i256_field()).is_err(),
        "illegal negative-zero canonical payload must not encode as Int256 zero"
    );
}

#[test]
fn empty_canonical_payload_is_rejected() {
    let arr = arr_raw(&[Some(&[])]);
    let msg = decimal_arb_to_clickhouse_native(&arr, &u256_field())
        .map(|_| ())
        .expect_err("empty canonical bytes must be rejected")
        .to_string();
    assert!(msg.contains("empty"), "{msg}");
}

#[test]
fn zero_round_trips_through_the_read_side() {
    let out = decimal_arb_to_clickhouse_native(&arr_of(&["0"], 0), &u256_field()).unwrap();
    let back = clickhouse_native_to_decimal_arb(out.as_ref(), &u256_field()).unwrap();
    let lb = back.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert_eq!(lb.value(0), canon("0", 0).as_slice());
}

// ===========================================================================
// 5. Invalid sign bytes / malformed canonical payloads
// ===========================================================================

#[test]
fn invalid_sign_byte_is_rejected() {
    for bad in [0x01u8, 0x02, 0x7F, 0x80, 0xFE] {
        let payload = [bad, 0x01];
        let arr = arr_raw(&[Some(&payload)]);
        let msg = decimal_arb_to_clickhouse_native(&arr, &u256_field())
            .map(|_| ())
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("sign byte"),
            "sign byte 0x{bad:02X} must be rejected by name: {msg}"
        );
    }
}

#[test]
fn invalid_sign_byte_error_reports_the_byte_value() {
    let arr = arr_raw(&[Some(&[0x7Fu8, 0x01])]);
    let msg = decimal_arb_to_clickhouse_native(&arr, &u256_field())
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(msg.contains("0x7F"), "error must echo the bad byte: {msg}");
}

#[test]
fn magnitude_of_exactly_32_bytes_is_accepted() {
    let mut payload = vec![0x00u8];
    payload.extend_from_slice(&[0xFFu8; 32]);
    let arr = arr_raw(&[Some(&payload[..])]);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    assert_eq!(fsb(&out).value(0), [0xFFu8; 32]);
}

#[test]
fn magnitude_of_33_bytes_is_rejected() {
    let mut payload = vec![0x00u8];
    payload.extend_from_slice(&[0x01u8; 33]);
    let arr = arr_raw(&[Some(&payload[..])]);
    let msg = decimal_arb_to_clickhouse_native(&arr, &u256_field())
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(
        msg.contains("33 bytes"),
        "must report the real width: {msg}"
    );
}

#[test]
fn non_minimal_magnitude_with_leading_zeros_still_encodes_correctly() {
    // Not produced by the canonical encoder, but a lenient decode is fine as
    // long as the *value* is right.
    let payload = [0x00u8, 0x00, 0x00, 0x01, 0x00];
    let arr = arr_raw(&[Some(&payload)]);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    assert_eq!(
        fsb(&out).value(0),
        expected_le("256"),
        "leading zero bytes must not change the value"
    );
}

// ===========================================================================
// 6. NULL handling
// ===========================================================================

#[test]
fn nulls_are_preserved_positionally() {
    let arr = arr_opt(&[Some("1"), None, Some("2"), None, Some("3")], 0);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    let f = fsb(&out);
    assert_eq!(f.len(), 5);
    for (i, expect_null) in [false, true, false, true, false].iter().enumerate() {
        assert_eq!(f.is_null(i), *expect_null, "null mask wrong at row {i}");
    }
}

#[test]
fn null_count_is_preserved() {
    let arr = arr_opt(&[None, Some("1"), None, None], 0);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    assert_eq!(fsb(&out).null_count(), 3);
}

#[test]
fn non_null_values_around_nulls_keep_their_bytes() {
    let arr = arr_opt(&[None, Some(U256_MAX), None, Some("1")], 0);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    let f = fsb(&out);
    assert_eq!(f.value(1), [0xFFu8; 32]);
    assert_eq!(f.value(3), expected_le("1"));
}

#[test]
fn all_null_array_produces_all_null_output() {
    let arr = arr_opt(&[None, None, None], 0);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    let f = fsb(&out);
    assert_eq!(f.len(), 3);
    assert_eq!(f.null_count(), 3);
}

#[test]
fn all_null_array_still_has_fixed_size_binary_32_type() {
    let arr = arr_opt(&[None, None], 0);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    assert_eq!(
        out.data_type(),
        &DataType::FixedSizeBinary(32),
        "an all-NULL column must still declare FixedSizeBinary(32)"
    );
}

#[test]
fn empty_array_produces_empty_fixed_size_binary_32() {
    let arr = arr_of(&[], 0);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    assert_eq!(out.len(), 0);
    assert_eq!(
        out.data_type(),
        &DataType::FixedSizeBinary(32),
        "an empty batch must still declare the right width, or the INSERT schema breaks"
    );
}

#[test]
fn nulls_do_not_mask_a_later_error() {
    let arr = arr_opt(&[None, None, Some("-1")], 0);
    assert!(
        decimal_arb_to_clickhouse_native(&arr, &u256_field()).is_err(),
        "a bad row after NULLs must still fail"
    );
}

#[test]
fn nulls_before_an_error_do_not_shift_the_reported_row() {
    let arr = arr_opt(&[None, Some("1"), None, Some(NINE_78)], 0);
    let msg = decimal_arb_to_clickhouse_native(&arr, &u256_field())
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(msg.contains("row 3"), "{msg}");
}

#[test]
fn null_rows_carry_a_zero_width_32_slot() {
    // Arrow requires the value buffer to still be 32 bytes wide per slot.
    let arr = arr_opt(&[Some("1"), None], 0);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    let f = fsb(&out);
    assert_eq!(f.value_length(), 32);
    assert_eq!(
        f.value(1).len(),
        32,
        "null slot must still be 32 bytes wide"
    );
}

// ===========================================================================
// 7. native_int_kind hint present vs absent
// ===========================================================================

#[test]
fn missing_hint_is_rejected() {
    let field: FieldRef = Arc::new(arb_field("amount", 78, 0));
    let arr = arr_of(&["1"], 0);
    let msg = decimal_arb_to_clickhouse_native(&arr, &field)
        .map(|_| ())
        .expect_err("no hint means no native channel")
        .to_string();
    assert!(msg.contains("native_int_kind"), "{msg}");
    assert!(msg.contains("amount"), "must name the column: {msg}");
}

#[test]
fn missing_hint_is_rejected_even_for_an_empty_array() {
    let field: FieldRef = Arc::new(arb_field("amount", 78, 0));
    let arr = arr_of(&[], 0);
    assert!(
        decimal_arb_to_clickhouse_native(&arr, &field).is_err(),
        "hint validation must not depend on there being rows"
    );
}

#[test]
fn non_decimal_arb_field_is_rejected() {
    let field: FieldRef = Arc::new(Field::new("blob", DataType::LargeBinary, true));
    let arr = arr_raw(&[Some(&[0x00u8, 0x01])]);
    assert!(
        decimal_arb_to_clickhouse_native(&arr, &field).is_err(),
        "a plain LargeBinary column is not decimal_arb and must not encode"
    );
}

#[test]
fn unrecognized_hint_value_is_treated_as_absent() {
    let base = arb_field("amount", 78, 0);
    let mut md = base.metadata().clone();
    md.insert(
        DecimalArbType::NATIVE_INT_KIND_KEY.to_string(),
        "u512".to_string(),
    );
    let field: FieldRef = Arc::new(base.with_metadata(md));
    let msg = decimal_arb_to_clickhouse_native(&arr_of(&["1"], 0), &field)
        .map(|_| ())
        .expect_err("an unknown hint must not be silently treated as u256")
        .to_string();
    assert!(msg.contains("native_int_kind"), "{msg}");
}

#[test]
fn hint_parsing_is_case_insensitive() {
    for raw in ["u256", "U256", " U256 ", "U256"] {
        let base = arb_field("amount", 78, 0);
        let mut md = base.metadata().clone();
        md.insert(DecimalArbType::NATIVE_INT_KIND_KEY.to_string(), raw.into());
        let field: FieldRef = Arc::new(base.with_metadata(md));
        assert!(
            decimal_arb_to_clickhouse_native(&arr_of(&["1"], 0), &field).is_ok(),
            "hint {raw:?} must parse as u256"
        );
    }
}

#[test]
fn i256_hint_string_is_recognized_case_insensitively() {
    for raw in ["i256", "I256", "  i256"] {
        let base = arb_field("amount", 78, 0);
        let mut md = base.metadata().clone();
        md.insert(DecimalArbType::NATIVE_INT_KIND_KEY.to_string(), raw.into());
        let field: FieldRef = Arc::new(base.with_metadata(md));
        assert!(
            decimal_arb_to_clickhouse_native(&arr_of(&["-1"], 0), &field).is_ok(),
            "hint {raw:?} must parse as i256 and accept negatives"
        );
    }
}

#[test]
fn hint_alone_without_extension_metadata_is_not_enough() {
    let mut md = HashMap::new();
    md.insert(
        DecimalArbType::NATIVE_INT_KIND_KEY.to_string(),
        "u256".to_string(),
    );
    let field: FieldRef = Arc::new(Field::new("x", DataType::LargeBinary, true).with_metadata(md));
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&["1"], 0), &field).is_err(),
        "the hint must not activate on a non-decimal_arb field"
    );
}

#[test]
fn hint_cannot_be_stamped_on_a_non_decimal_arb_field() {
    let f = Field::new("x", DataType::Int64, true);
    assert!(
        DecimalArbType::with_native_int_kind(f, NativeIntKind::U256).is_err(),
        "§E1: only decimal_arb fields may carry native_int_kind"
    );
}

// ===========================================================================
// 8. Wrong input array types
// ===========================================================================

#[test]
fn binary_array_input_is_rejected() {
    let arr = BinaryArray::from(vec![Some(&[0x00u8, 0x01][..])]);
    let msg = decimal_arb_to_clickhouse_native(&arr, &u256_field())
        .map(|_| ())
        .expect_err("Binary is not LargeBinary")
        .to_string();
    assert!(msg.contains("LargeBinaryArray"), "{msg}");
}

#[test]
fn string_array_input_is_rejected() {
    let arr = StringArray::from(vec![Some("1")]);
    assert!(decimal_arb_to_clickhouse_native(&arr, &u256_field()).is_err());
}

#[test]
fn int64_array_input_is_rejected() {
    let arr = Int64Array::from(vec![1i64, 2]);
    assert!(decimal_arb_to_clickhouse_native(&arr, &u256_field()).is_err());
}

#[test]
fn already_native_fixed_size_binary_input_is_rejected() {
    // Guards against double conversion (which would byte-reverse twice).
    let mut b = FixedSizeBinaryBuilder::with_capacity(1, 32);
    b.append_value([0u8; 32]).unwrap();
    let arr = b.finish();
    assert!(
        decimal_arb_to_clickhouse_native(&arr, &u256_field()).is_err(),
        "an already-converted column must not be converted a second time"
    );
}

#[test]
fn wrong_input_type_error_names_the_column() {
    let arr = Int64Array::from(vec![1i64]);
    let msg = decimal_arb_to_clickhouse_native(&arr, &u256_field())
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(msg.contains("balance"), "{msg}");
}

// ===========================================================================
// 9. Structural output guarantees
// ===========================================================================

#[test]
fn output_length_always_matches_input_length() {
    for n in [0usize, 1, 2, 7, 64, 129] {
        let decs: Vec<String> = (0..n).map(|i| i.to_string()).collect();
        let refs: Vec<&str> = decs.iter().map(|s| s.as_str()).collect();
        let out = decimal_arb_to_clickhouse_native(&arr_of(&refs, 0), &u256_field()).unwrap();
        assert_eq!(out.len(), n, "row count changed for n={n}");
    }
}

#[test]
fn every_emitted_row_is_exactly_32_bytes() {
    let arr = arr_of(&["0", "1", U256_MAX, TEN_POW_77, TWO_POW_255], 0);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    let f = fsb(&out);
    for i in 0..f.len() {
        assert_eq!(f.value(i).len(), 32, "row {i} is not 32 bytes");
    }
}

#[test]
fn output_data_type_is_fixed_size_binary_32() {
    let out = decimal_arb_to_clickhouse_native(&arr_of(&["1"], 0), &u256_field()).unwrap();
    assert_eq!(out.data_type(), &DataType::FixedSizeBinary(32));
}

#[test]
fn a_large_mixed_batch_encodes_every_row_correctly() {
    let mut decs = Vec::new();
    for i in 0..100u32 {
        // Deterministic, spread across byte boundaries.
        let v = (i as u128) * 0x0102_0304_0506_0708u128 + 1;
        decs.push(v.to_string());
    }
    let refs: Vec<&str> = decs.iter().map(|s| s.as_str()).collect();
    let out = decimal_arb_to_clickhouse_native(&arr_of(&refs, 0), &u256_field()).unwrap();
    let f = fsb(&out);
    for (i, d) in decs.iter().enumerate() {
        assert_eq!(f.value(i), expected_le(d), "row {i} ({d}) mis-encoded");
    }
}

#[test]
fn sliced_input_array_encodes_the_slice_not_the_original() {
    let arr = arr_of(&["1", "2", "3", "4"], 0);
    let sliced = arr.slice(2, 2);
    let out = decimal_arb_to_clickhouse_native(&sliced, &u256_field()).unwrap();
    let f = fsb(&out);
    assert_eq!(f.len(), 2, "slice length must be honoured");
    assert_eq!(
        f.value(0),
        expected_le("3"),
        "slice offset must be honoured"
    );
    assert_eq!(f.value(1), expected_le("4"));
}

#[test]
fn sliced_input_with_nulls_keeps_the_right_null_mask() {
    let arr = arr_opt(&[Some("1"), None, Some("3"), None], 0);
    let sliced = arr.slice(1, 3);
    let out = decimal_arb_to_clickhouse_native(&sliced, &u256_field()).unwrap();
    let f = fsb(&out);
    assert!(f.is_null(0));
    assert!(!f.is_null(1));
    assert!(f.is_null(2));
}

// ===========================================================================
// 10. Round-trip through the read side
// ===========================================================================

fn assert_round_trip(dec: &str, field: &FieldRef) {
    let arr = arr_of(&[dec], 0);
    let native = decimal_arb_to_clickhouse_native(&arr, field).unwrap();
    let back = clickhouse_native_to_decimal_arb(native.as_ref(), field).unwrap();
    let lb = back.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert_eq!(
        lb.value(0),
        canon(dec, 0).as_slice(),
        "sink→source round trip lost the value for {dec}"
    );
}

#[test]
fn u256_round_trip_battery() {
    for dec in [
        "0",
        "1",
        "255",
        "256",
        "65535",
        "18446744073709551616",
        TEN_POW_77,
        TWO_POW_255,
        I256_MAX,
        U256_MAX,
    ] {
        assert_round_trip(dec, &u256_field());
    }
}

#[test]
fn i256_round_trip_battery() {
    for dec in [
        "0", "1", "-1", "255", "-255", "256", "-256", "-65536", I256_MAX, I256_MIN,
    ] {
        assert_round_trip(dec, &i256_field());
    }
}

#[test]
fn i256_round_trip_of_the_asymmetric_pattern() {
    let be = asymmetric_pattern();
    let dec = dec_from_be32(&be);
    assert_round_trip(&dec, &i256_field());
    assert_round_trip(&format!("-{dec}"), &i256_field());
}

#[test]
fn round_trip_preserves_nulls() {
    let arr = arr_opt(&[Some("1"), None, Some("2")], 0);
    let native = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    let back = clickhouse_native_to_decimal_arb(native.as_ref(), &u256_field()).unwrap();
    assert_eq!(back.len(), 3);
    assert!(back.is_null(1));
    assert!(!back.is_null(0) && !back.is_null(2));
}

#[test]
fn reading_a_u256_bit_pattern_as_i256_changes_the_value() {
    // Sanity: the signed/unsigned distinction is real, so a mis-set hint is a
    // genuine corruption vector (and therefore worth the errors above).
    let native = decimal_arb_to_clickhouse_native(&arr_of(&[U256_MAX], 0), &u256_field()).unwrap();
    let as_i256 = clickhouse_native_to_decimal_arb(native.as_ref(), &i256_field()).unwrap();
    let lb = as_i256.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert_eq!(
        lb.value(0),
        canon("-1", 0).as_slice(),
        "the same 32 bytes must read as -1 under the signed hint"
    );
}

// ===========================================================================
// 11. clickhouse_column_type — decimal_arb Decimal(p, s) path
// ===========================================================================

fn col_type(field: &Field, d: Option<&ColumnDirective>) -> String {
    ClickHouseClient::clickhouse_column_type(field, d).unwrap()
}

fn col_type_err(field: &Field, d: Option<&ColumnDirective>) -> String {
    ClickHouseClient::clickhouse_column_type(field, d)
        .expect_err("expected a config-load rejection")
        .to_string()
}

fn coerce_string(name: &str) -> ColumnDirective {
    ColumnDirective {
        name: name.to_string(),
        coerce_to: Some(CoercionTarget::String),
    }
}

#[test]
fn narrow_decimal_arb_maps_to_clickhouse_decimal() {
    assert_eq!(col_type(&arb_field("a", 38, 10), None), "Decimal(38, 10)");
}

#[test]
fn minimum_precision_decimal_arb_maps_to_decimal_1_0() {
    assert_eq!(col_type(&arb_field("a", 1, 0), None), "Decimal(1, 0)");
}

#[test]
fn decimal_arb_at_the_76_digit_cap_is_native() {
    assert_eq!(col_type(&arb_field("a", 76, 0), None), "Decimal(76, 0)");
    assert_eq!(col_type(&arb_field("a", 76, 76), None), "Decimal(76, 76)");
}

#[test]
fn decimal_arb_one_past_the_cap_is_rejected_without_opt_in() {
    let msg = col_type_err(&arb_field("amount", 77, 0), None);
    assert!(msg.contains("amount"), "{msg}");
    assert!(msg.contains("76"), "must cite the 76-digit cap: {msg}");
}

#[test]
fn decimal_arb_scale_equals_precision_is_still_native_under_the_cap() {
    assert_eq!(col_type(&arb_field("a", 20, 20), None), "Decimal(20, 20)");
}

#[test]
fn decimal_type_string_format_has_a_space_after_the_comma() {
    // The DDL string is concatenated into CREATE TABLE verbatim.
    let t = col_type(&arb_field("a", 10, 2), None);
    assert_eq!(t, "Decimal(10, 2)", "exact DDL text matters: {t}");
}

#[test]
fn precision_and_scale_are_not_swapped_in_the_ddl() {
    let t = col_type(&arb_field("a", 30, 4), None);
    assert_eq!(t, "Decimal(30, 4)");
    assert_ne!(t, "Decimal(4, 30)");
}

#[test]
fn decimal_arb_ddl_matches_arrow_field_to_clickhouse_for_narrow_columns() {
    for (p, s) in [(1u32, 0u32), (10, 2), (38, 10), (76, 30)] {
        let f = arb_field("a", p, s);
        assert_eq!(
            col_type(&f, None),
            ClickHouseClient::arrow_field_to_clickhouse(&f),
            "the directive-aware wrapper must agree with the legacy mapping at ({p}, {s})"
        );
    }
}

// ===========================================================================
// 12. clickhouse_column_type — String fallback / opt-in
// ===========================================================================

#[test]
fn wide_decimal_arb_with_coerce_to_string_maps_to_string() {
    let d = coerce_string("amount");
    assert_eq!(col_type(&arb_field("amount", 100, 18), Some(&d)), "String");
}

#[test]
fn wide_decimal_arb_at_max_precision_with_opt_in_maps_to_string() {
    let d = coerce_string("amount");
    assert_eq!(
        col_type(&arb_field("amount", 65_535, 0), Some(&d)),
        "String"
    );
}

#[test]
fn wide_decimal_arb_at_max_precision_without_opt_in_is_rejected() {
    let msg = col_type_err(&arb_field("amount", 65_535, 0), None);
    assert!(msg.contains("amount"), "{msg}");
    assert!(msg.contains("coerce_to: string"), "{msg}");
}

#[test]
fn directive_without_coerce_to_behaves_like_no_directive() {
    let d = ColumnDirective {
        name: "amount".to_string(),
        coerce_to: None,
    };
    let with = ClickHouseClient::clickhouse_column_type(&arb_field("amount", 100, 18), Some(&d));
    let without = ClickHouseClient::clickhouse_column_type(&arb_field("amount", 100, 18), None);
    assert!(with.is_err() && without.is_err(), "both must reject");
}

#[test]
fn coerce_to_string_does_not_downgrade_a_narrow_column() {
    let d = coerce_string("a");
    assert_eq!(
        col_type(&arb_field("a", 38, 10), Some(&d)),
        "Decimal(38, 10)",
        "an opt-in must not force a natively-representable column to String"
    );
}

#[test]
fn rejection_message_carries_every_diagnostic_field() {
    let msg = col_type_err(&arb_field("ledger.amount", 120, 30), None);
    assert!(msg.contains("ledger.amount"), "column: {msg}");
    assert!(msg.contains("decimal_arb(120, 30)"), "declared type: {msg}");
    assert!(msg.contains("clickhouse"), "connector: {msg}");
    assert!(msg.contains("coerce_to: string"), "remediation: {msg}");
}

#[test]
fn legacy_mapping_silently_returns_string_where_the_wrapper_rejects() {
    // Documents the deliberate divergence: `arrow_field_to_clickhouse` keeps
    // the old silent fallback, `clickhouse_column_type` hard-rejects.
    let f = arb_field("amount", 100, 18);
    assert_eq!(ClickHouseClient::arrow_field_to_clickhouse(&f), "String");
    assert!(ClickHouseClient::clickhouse_column_type(&f, None).is_err());
}

// ===========================================================================
// 13. clickhouse_column_type — native_int_kind hinted columns
// ===========================================================================

#[test]
fn u256_hinted_column_declares_uint256() {
    let f = hinted_field("balance", 78, 0, NativeIntKind::U256);
    assert_eq!(col_type(&f, None), "UInt256");
}

#[test]
fn i256_hinted_column_declares_int256() {
    let f = hinted_field("delta", 78, 0, NativeIntKind::I256);
    assert_eq!(col_type(&f, None), "Int256");
}

#[test]
fn hinted_column_at_precision_77_still_declares_the_native_type() {
    let f = hinted_field("balance", 77, 0, NativeIntKind::U256);
    assert_eq!(col_type(&f, None), "UInt256");
}

#[test]
fn hinted_column_below_the_decimal_cap_still_declares_the_native_type() {
    // (38, 0) + hint: capability says Native and the hint wins over Decimal.
    let f = hinted_field("balance", 38, 0, NativeIntKind::U256);
    assert_eq!(col_type(&f, None), "UInt256");
}

#[test]
fn hinted_column_with_nonzero_scale_does_not_declare_a_native_int() {
    // Scale != 0 cannot be a UInt256; must fall back to Decimal.
    let f = hinted_field("balance", 40, 2, NativeIntKind::U256);
    assert_eq!(col_type(&f, None), "Decimal(40, 2)");
}

#[test]
fn hinted_wide_column_with_nonzero_scale_is_rejected_without_opt_in() {
    let f = hinted_field("balance", 78, 2, NativeIntKind::U256);
    let msg = col_type_err(&f, None);
    assert!(msg.contains("balance"), "{msg}");
}

#[test]
fn hinted_column_past_precision_78_is_rejected_without_opt_in() {
    let f = hinted_field("balance", 79, 0, NativeIntKind::U256);
    let msg = col_type_err(&f, None);
    assert!(
        msg.contains("balance"),
        "the native channel stops at 78 digits: {msg}"
    );
}

#[test]
fn hinted_column_past_precision_78_with_opt_in_maps_to_string() {
    let f = hinted_field("balance", 79, 0, NativeIntKind::U256);
    let d = coerce_string("balance");
    assert_eq!(col_type(&f, Some(&d)), "String");
}

#[test]
fn hint_does_not_leak_onto_unhinted_wide_columns() {
    let f = arb_field("balance", 78, 0);
    assert!(
        ClickHouseClient::clickhouse_column_type(&f, None).is_err(),
        "decimal_arb(78, 0) without a hint has no native channel and must reject"
    );
}

#[test]
fn unhinted_78_digit_column_with_opt_in_maps_to_string() {
    let f = arb_field("balance", 78, 0);
    let d = coerce_string("balance");
    assert_eq!(col_type(&f, Some(&d)), "String");
}

#[test]
fn u256_and_i256_hints_produce_different_ddl() {
    let u = hinted_field("x", 78, 0, NativeIntKind::U256);
    let i = hinted_field("x", 78, 0, NativeIntKind::I256);
    assert_ne!(
        col_type(&u, None),
        col_type(&i, None),
        "signedness must survive into the DDL"
    );
}

#[test]
fn ddl_native_int_names_are_exactly_clickhouse_spelling() {
    assert_eq!(
        col_type(&hinted_field("x", 78, 0, NativeIntKind::U256), None),
        "UInt256"
    );
    assert_eq!(
        col_type(&hinted_field("x", 78, 0, NativeIntKind::I256), None),
        "Int256"
    );
}

#[test]
fn unrecognized_hint_string_falls_back_to_the_unhinted_decision() {
    let base = arb_field("balance", 78, 0);
    let mut md = base.metadata().clone();
    md.insert(
        DecimalArbType::NATIVE_INT_KIND_KEY.to_string(),
        "uint256".to_string(), // not the canonical spelling
    );
    let f = base.with_metadata(md);
    assert!(
        ClickHouseClient::clickhouse_column_type(&f, None).is_err(),
        "an unparseable hint must not be treated as u256"
    );
}

// ===========================================================================
// 14. clickhouse_column_type — normalized FixedSizeBinary(32) shape
// ===========================================================================

/// The shape `normalize_schema_for_clickhouse` produces: FSB(32) carrying the
/// original decimal_arb metadata (extension keys + hint).
fn normalized_fsb(name: &str, p: u32, s: u32, kind: Option<NativeIntKind>) -> Field {
    let base = arb_field(name, p, s);
    let base = match kind {
        Some(k) => DecimalArbType::with_native_int_kind(base, k).unwrap(),
        None => base,
    };
    Field::new(name, DataType::FixedSizeBinary(32), true).with_metadata(base.metadata().clone())
}

#[test]
fn normalized_fsb_with_u256_hint_declares_uint256() {
    let f = normalized_fsb("balance", 78, 0, Some(NativeIntKind::U256));
    assert_eq!(col_type(&f, None), "UInt256");
}

#[test]
fn normalized_fsb_with_i256_hint_declares_int256() {
    let f = normalized_fsb("delta", 78, 0, Some(NativeIntKind::I256));
    assert_eq!(col_type(&f, None), "Int256");
}

#[test]
fn normalized_fsb_ddl_matches_the_pre_normalization_ddl() {
    for kind in [NativeIntKind::U256, NativeIntKind::I256] {
        let pre = hinted_field("x", 78, 0, kind);
        let post = normalized_fsb("x", 78, 0, Some(kind));
        assert_eq!(
            col_type(&pre, None),
            col_type(&post, None),
            "normalizing the Arrow shape must not change the ClickHouse DDL"
        );
    }
}

#[test]
fn normalized_fsb_ignores_a_coerce_to_string_directive() {
    // Documents current behavior: the FSB(32) branch returns before the
    // directive is consulted.
    let f = normalized_fsb("balance", 78, 0, Some(NativeIntKind::U256));
    let d = coerce_string("balance");
    assert_eq!(col_type(&f, Some(&d)), "UInt256");
}

#[test]
fn plain_fixed_size_binary_without_metadata_maps_to_fixed_string() {
    let f = Field::new("h", DataType::FixedSizeBinary(32), false);
    assert_eq!(col_type(&f, None), "FixedString(32)");
}

#[test]
fn fixed_size_binary_of_other_widths_maps_to_fixed_string() {
    for w in [1i32, 8, 20, 64] {
        let f = Field::new("h", DataType::FixedSizeBinary(w), false);
        assert_eq!(col_type(&f, None), format!("FixedString({w})"));
    }
}

// ===========================================================================
// 15. clickhouse_column_type — non-decimal_arb passthrough
// ===========================================================================

#[test]
fn non_decimal_arb_types_delegate_to_the_legacy_mapping() {
    let cases: Vec<(DataType, &str)> = vec![
        (DataType::Boolean, "UInt8"),
        (DataType::Int8, "Int8"),
        (DataType::Int16, "Int16"),
        (DataType::Int32, "Int32"),
        (DataType::Int64, "Int64"),
        (DataType::UInt8, "UInt8"),
        (DataType::UInt16, "UInt16"),
        (DataType::UInt32, "UInt32"),
        (DataType::UInt64, "UInt64"),
        (DataType::Float32, "Float32"),
        (DataType::Float64, "Float64"),
        (DataType::Utf8, "String"),
        (DataType::LargeUtf8, "String"),
        (DataType::Date32, "Date"),
    ];
    for (dt, expect) in cases {
        let f = Field::new("c", dt.clone(), false);
        assert_eq!(col_type(&f, None), expect, "mapping wrong for {dt:?}");
    }
}

#[test]
fn plain_large_binary_maps_to_string_not_decimal() {
    let f = Field::new("blob", DataType::LargeBinary, false);
    assert_eq!(
        col_type(&f, None),
        "String",
        "a LargeBinary column without decimal_arb metadata is not a decimal"
    );
}

#[test]
fn arrow_decimal128_delegates_unchanged() {
    let f = Field::new("p", DataType::Decimal128(20, 5), false);
    assert_eq!(col_type(&f, None), "Decimal(20, 5)");
}

#[test]
fn arrow_decimal256_within_cap_delegates_unchanged() {
    let f = Field::new("p", DataType::Decimal256(70, 30), false);
    assert_eq!(col_type(&f, None), "Decimal(70, 30)");
}

#[test]
fn non_decimal_arb_field_never_errors() {
    for dt in [
        DataType::Boolean,
        DataType::Int64,
        DataType::Utf8,
        DataType::Binary,
        DataType::Date32,
    ] {
        let f = Field::new("c", dt.clone(), true);
        assert!(
            ClickHouseClient::clickhouse_column_type(&f, None).is_ok(),
            "non-decimal_arb {dt:?} must never be rejected"
        );
    }
}

#[test]
fn directive_on_a_non_decimal_arb_column_is_ignored() {
    let f = Field::new("c", DataType::Int64, false);
    let d = coerce_string("c");
    assert_eq!(col_type(&f, Some(&d)), "Int64");
}

// ===========================================================================
// 16. DDL ↔ data-path agreement (the pairing that prevents silent corruption)
// ===========================================================================

/// A column whose DDL says `UInt256` must have data the encoder can emit as
/// 32 LE bytes, and vice versa. This pairing is where a mismatch corrupts.
#[test]
fn uint256_ddl_implies_the_encoder_accepts_the_column() {
    let f = hinted_field("balance", 78, 0, NativeIntKind::U256);
    assert_eq!(col_type(&f, None), "UInt256");
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&[U256_MAX], 0), &f).is_ok(),
        "DDL and data path must agree"
    );
}

#[test]
fn int256_ddl_implies_the_encoder_accepts_negatives() {
    let f = hinted_field("delta", 78, 0, NativeIntKind::I256);
    assert_eq!(col_type(&f, None), "Int256");
    assert!(decimal_arb_to_clickhouse_native(&arr_of(&[I256_MIN], 0), &f).is_ok());
}

#[test]
fn decimal_ddl_columns_are_not_routed_through_the_native_encoder() {
    // (40, 2) + hint declares Decimal(40, 2); pushing it through the native
    // encoder would drop the scale, so the sink must not do that. Assert the
    // encoder's behaviour is at least loud about being the wrong path by
    // producing the *unscaled* integer, which is why the guard upstream
    // matters.
    let f = hinted_field("balance", 40, 2, NativeIntKind::U256);
    assert_eq!(col_type(&f, None), "Decimal(40, 2)");
    let arr = arr_of(&["1.23"], 2);
    let out = decimal_arb_to_clickhouse_native(&arr, &f).unwrap();
    assert_eq!(
        fsb(&out).value(0),
        expected_le("123"),
        "the native encoder is scale-blind — it emits the unscaled integer, \
         which is exactly why only scale==0 columns may reach it"
    );
}

#[test]
fn string_ddl_columns_never_reach_the_native_encoder_shape() {
    let f = arb_field("amount", 100, 18);
    let d = coerce_string("amount");
    assert_eq!(col_type(&f, Some(&d)), "String");
    let fr: FieldRef = Arc::new(f);
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&["1.5"], 18), &fr).is_err(),
        "a String-coerced column has no native_int_kind and must not encode natively"
    );
}

#[test]
fn a_rejected_ddl_column_also_fails_the_native_encoder() {
    let f = arb_field("amount", 100, 18);
    assert!(ClickHouseClient::clickhouse_column_type(&f, None).is_err());
    let fr: FieldRef = Arc::new(f);
    assert!(decimal_arb_to_clickhouse_native(&arr_of(&["1"], 18), &fr).is_err());
}

// ===========================================================================
// 17. Scale interaction on the native path
// ===========================================================================

#[test]
fn scale_zero_column_encodes_the_integer_value_verbatim() {
    for dec in ["7", "1000", "123456789"] {
        assert_eq!(enc_one(dec, &u256_field()), expected_le(dec));
    }
}

#[test]
fn a_fractional_literal_in_a_scale_zero_column_is_half_even_rounded_before_encoding() {
    // Documented canonical-encoder behaviour; asserted here so a change in
    // rounding mode is caught at the sink boundary.
    assert_eq!(enc_one("2.5", &u256_field()), expected_le("2"));
    assert_eq!(enc_one("3.5", &u256_field()), expected_le("4"));
    assert_eq!(enc_one("2.4", &u256_field()), expected_le("2"));
    assert_eq!(enc_one("2.6", &u256_field()), expected_le("3"));
}

#[test]
fn a_fractional_negative_literal_rounds_half_even_too() {
    assert_eq!(enc_one("-2.5", &i256_field()), expected_le("-2"));
    assert_eq!(enc_one("-3.5", &i256_field()), expected_le("-4"));
}

#[test]
fn rounding_to_zero_does_not_produce_a_negative_zero_error_on_u256() {
    // -0.4 rounds to 0 at scale 0, which must be accepted by a UInt256 column.
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&["-0.4"], 0), &u256_field()).is_ok(),
        "a negative value that rounds to zero must encode as zero, not error"
    );
    assert_eq!(enc_one("-0.4", &u256_field()), [0u8; 32]);
}

#[test]
fn a_value_rounding_up_across_the_i256_boundary_is_still_rejected() {
    // 2^255 - 0.5 rounds half-even to 2^255 - hmm, exactly .5 with an even
    // integer part rounds down. Use .6 so it rounds up past the boundary.
    let dec = format!("{}.6", I256_MAX);
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&[dec.as_str()], 0), &i256_field()).is_err(),
        "a value that rounds up to 2^255 must be rejected, not silently flipped"
    );
}

#[test]
fn a_value_rounding_up_across_the_u256_boundary_is_still_rejected() {
    let dec = format!("{}.6", U256_MAX);
    assert!(
        decimal_arb_to_clickhouse_native(&arr_of(&[dec.as_str()], 0), &u256_field()).is_err(),
        "a value that rounds up to 2^256 must be rejected, not truncated"
    );
}

// ===========================================================================
// 18. Determinism / idempotence
// ===========================================================================

#[test]
fn encoding_is_deterministic_across_repeated_calls() {
    let f = u256_field();
    let first = enc_one(TEN_POW_77, &f);
    for _ in 0..5 {
        assert_eq!(enc_one(TEN_POW_77, &f), first);
    }
}

#[test]
fn encoding_is_independent_of_batch_position() {
    let solo = enc_one(TEN_POW_77, &u256_field());
    let arr = arr_of(&["1", "2", TEN_POW_77, "3"], 0);
    let out = decimal_arb_to_clickhouse_native(&arr, &u256_field()).unwrap();
    assert_eq!(
        fsb(&out).value(2),
        solo,
        "a value's bytes must not depend on its row index"
    );
}

#[test]
fn encoding_is_independent_of_the_column_name() {
    let a = enc_one(TEN_POW_77, &hinted_field("a", 78, 0, NativeIntKind::U256));
    let b = enc_one(
        TEN_POW_77,
        &hinted_field("some_other_name", 78, 0, NativeIntKind::U256),
    );
    assert_eq!(a, b);
}

#[test]
fn encoding_is_independent_of_declared_precision_for_in_range_values() {
    for p in [39u32, 50, 60, 77, 78] {
        assert_eq!(
            enc_one("1000000", &hinted_field("x", p, 0, NativeIntKind::U256)),
            expected_le("1000000"),
            "precision {p} changed the bytes"
        );
    }
}

#[test]
fn positive_values_encode_identically_under_both_hints() {
    for dec in ["0", "1", "255", TEN_POW_76, I256_MAX] {
        assert_eq!(
            enc_one(dec, &u256_field()),
            enc_one(dec, &i256_field()),
            "non-negative {dec} must have the same bytes under u256 and i256"
        );
    }
}

#[test]
fn field_nullability_does_not_change_the_encoding() {
    let nn = Arc::new(
        DecimalArbType::with_native_int_kind(
            DecimalArbType::field("x", 78, 0, false).unwrap(),
            NativeIntKind::U256,
        )
        .unwrap(),
    );
    assert_eq!(enc_one(TEN_POW_77, &nn), expected_le(TEN_POW_77));
}

// ===========================================================================
// 19. Nullability of the emitted ClickHouse DDL
//
// `arrow_field_to_clickhouse` wraps a nullable Arrow field's type in
// `Nullable(...)` before returning. Every `decimal_arb` branch — the narrow
// `Decimal(p, s)` path, the `coerce_to: string` path, the hinted
// `UInt256`/`Int256` path, and the normalized `FixedSizeBinary(32)` path —
// returns *before* that wrapper runs, so a nullable decimal_arb column gets a
// NON-nullable ClickHouse column. The sink's own encoder
// (`decimal_arb_to_clickhouse_native`) explicitly preserves NULL rows, so the
// two halves disagree: NULLs are shipped into a column that cannot hold them.
// ===========================================================================

/// Baseline: for every non-decimal_arb type the wrapper is applied. This is
/// the behaviour the decimal_arb branches are expected to match.
#[test]
fn nullable_non_decimal_arb_columns_are_wrapped_in_nullable() {
    for (dt, inner) in [
        (DataType::Int64, "Int64"),
        (DataType::Utf8, "String"),
        (DataType::Decimal128(20, 5), "Decimal(20, 5)"),
        (DataType::FixedSizeBinary(32), "FixedString(32)"),
        (DataType::LargeBinary, "String"),
    ] {
        let f = Field::new("c", dt.clone(), true);
        assert_eq!(
            col_type(&f, None),
            format!("Nullable({inner})"),
            "nullable {dt:?} must be declared Nullable"
        );
    }
}

/// The sink really does emit NULL rows for a nullable hinted column — so the
/// DDL nullability question below is not academic.
#[test]
fn the_native_encoder_emits_null_rows_for_a_nullable_hinted_column() {
    let f = u256_field();
    assert!(f.is_nullable(), "fixture must be nullable");
    let out = decimal_arb_to_clickhouse_native(&arr_opt(&[Some("1"), None], 0), &f).unwrap();
    assert_eq!(
        fsb(&out).null_count(),
        1,
        "the sink ships a NULL for this column; the DDL must be able to hold it"
    );
}

#[test]
#[ignore = "FINDING: nullable decimal_arb columns get a non-Nullable ClickHouse DDL type \
            (every decimal_arb branch of clickhouse_column_type returns before the \
            Nullable() wrapper in arrow_field_to_clickhouse)"]
fn nullable_narrow_decimal_arb_column_is_declared_nullable() {
    let f = arb_field("amount", 38, 10);
    assert!(f.is_nullable());
    assert_eq!(
        col_type(&f, None),
        "Nullable(Decimal(38, 10))",
        "a nullable decimal_arb column must produce a Nullable ClickHouse column; \
         a bare Decimal(38, 10) turns every NULL into 0 on INSERT"
    );
}

#[test]
#[ignore = "FINDING: nullable decimal_arb + native_int_kind=u256 declares bare UInt256, \
            so NULL balances land in ClickHouse as 0"]
fn nullable_u256_hinted_column_is_declared_nullable() {
    let f = hinted_field("balance", 78, 0, NativeIntKind::U256);
    assert!(f.is_nullable());
    assert_eq!(
        col_type(&f, None),
        "Nullable(UInt256)",
        "the encoder preserves NULL rows for this column, so the DDL must accept NULL"
    );
}

#[test]
#[ignore = "FINDING: nullable decimal_arb + native_int_kind=i256 declares bare Int256"]
fn nullable_i256_hinted_column_is_declared_nullable() {
    let f = hinted_field("delta", 78, 0, NativeIntKind::I256);
    assert_eq!(col_type(&f, None), "Nullable(Int256)");
}

#[test]
#[ignore = "FINDING: nullable decimal_arb coerced to string declares bare String"]
fn nullable_string_coerced_decimal_arb_column_is_declared_nullable() {
    let f = arb_field("amount", 100, 18);
    let d = coerce_string("amount");
    assert_eq!(col_type(&f, Some(&d)), "Nullable(String)");
}

#[test]
#[ignore = "FINDING: the normalized FixedSizeBinary(32) shape also loses the Nullable() wrapper"]
fn nullable_normalized_fsb_column_is_declared_nullable() {
    let f = normalized_fsb("balance", 78, 0, Some(NativeIntKind::U256));
    assert!(f.is_nullable());
    assert_eq!(col_type(&f, None), "Nullable(UInt256)");
}

#[test]
#[ignore = "FINDING: nullability handling differs between decimal_arb and every other type"]
fn nullability_handling_is_consistent_between_decimal_arb_and_other_types() {
    // Same nullability, same treatment: either both get wrapped or neither.
    let int_nullable = col_type(&Field::new("c", DataType::Int64, true), None);
    let int_plain = col_type(&Field::new("c", DataType::Int64, false), None);
    let arb_nullable = col_type(&arb_field("c", 10, 2), None);
    let arb_plain = col_type(&DecimalArbType::field("c", 10, 2, false).unwrap(), None);
    assert_eq!(
        int_nullable != int_plain,
        arb_nullable != arb_plain,
        "nullability must change the emitted DDL for decimal_arb exactly as it does \
         for Int64 (int: {int_nullable}/{int_plain}, decimal_arb: {arb_nullable}/{arb_plain})"
    );
}

/// Non-nullable decimal_arb columns are fine today — pin that so a fix for
/// the finding above does not over-correct and wrap non-nullable columns.
#[test]
fn non_nullable_decimal_arb_columns_are_not_wrapped_in_nullable() {
    let f = DecimalArbType::field("amount", 38, 10, false).unwrap();
    assert_eq!(col_type(&f, None), "Decimal(38, 10)");
    let f = DecimalArbType::with_native_int_kind(
        DecimalArbType::field("balance", 78, 0, false).unwrap(),
        NativeIntKind::U256,
    )
    .unwrap();
    assert_eq!(col_type(&f, None), "UInt256");
}

// ===========================================================================
// 20. Does the remediation the encoder recommends actually work?
//
// When a negative value hits a `native_int_kind=u256` column (or a value ≥
// 2^255 hits an `i256` column) the encoder errors with:
//
//   "... Change the column's hint to i256, or route through a wider
//    non-native ClickHouse type via `coerce_to: string`."
//
// A user following that advice sets `coerce_to: string` on the column. But
// `capability_for_decimal_arb` checks the native_int_kind hint *before* it
// looks at `coerce_to_string`, so a hinted (≤78, 0) column returns `Native`
// unconditionally and the directive never takes effect — the DDL still says
// UInt256 and the same encode error recurs on the next batch.
// ===========================================================================

#[test]
fn the_u256_negative_error_recommends_coerce_to_string() {
    let msg = enc_err("-1", &u256_field());
    assert!(
        msg.contains("coerce_to: string"),
        "test premise: the error recommends coerce_to: string ({msg})"
    );
}

#[test]
fn the_i256_overflow_error_recommends_coerce_to_string() {
    let msg = enc_err(TWO_POW_255, &i256_field());
    assert!(
        msg.contains("coerce_to: string"),
        "test premise: the error recommends coerce_to: string ({msg})"
    );
}

#[test]
#[ignore = "FINDING: `coerce_to: string` is silently ignored on native_int_kind-hinted \
            columns (the hint short-circuits capability_for_decimal_arb before the \
            opt-in is read), so the remediation the encoder's own error message \
            recommends is a no-op"]
fn coerce_to_string_overrides_a_u256_hint_in_the_ddl() {
    let f = hinted_field("balance", 78, 0, NativeIntKind::U256);
    let d = coerce_string("balance");
    assert_eq!(
        col_type(&f, Some(&d)),
        "String",
        "an explicit coerce_to: string must win over the origin hint, otherwise a \
         pipeline whose u256-hinted column carries a negative value can never be \
         un-stuck by the remediation the error message names"
    );
}

#[test]
#[ignore = "FINDING: `coerce_to: string` is silently ignored on native_int_kind-hinted columns"]
fn coerce_to_string_overrides_an_i256_hint_in_the_ddl() {
    let f = hinted_field("delta", 78, 0, NativeIntKind::I256);
    let d = coerce_string("delta");
    assert_eq!(col_type(&f, Some(&d)), "String");
}

#[test]
#[ignore = "FINDING: `coerce_to: string` is silently ignored on native_int_kind-hinted columns"]
fn coerce_to_string_is_honoured_regardless_of_the_native_int_hint() {
    // Same column shape, same directive — the only difference is the hint.
    // The user's explicit opt-in should produce the same DDL either way.
    let unhinted = arb_field("v", 78, 0);
    let hinted = hinted_field("v", 78, 0, NativeIntKind::U256);
    let d = coerce_string("v");
    assert_eq!(
        col_type(&hinted, Some(&d)),
        col_type(&unhinted, Some(&d)),
        "an explicit coerce_to: string must not be silently discarded just because \
         the column carries an origin hint"
    );
}

/// API-contract note: `clickhouse_column_type` applies whatever directive it
/// is handed without re-checking the name — name matching is the caller's job
/// (`ColumnDirective::find`). Pinned so a future refactor that starts
/// name-matching internally, or a caller that stops matching, is caught.
#[test]
fn clickhouse_column_type_does_not_name_match_the_directive_it_is_given() {
    let f = arb_field("amount", 100, 18);
    let wrong = coerce_string("some_other_column");
    assert_eq!(
        col_type(&f, Some(&wrong)),
        "String",
        "the directive is applied verbatim; callers must do the name lookup"
    );
}
