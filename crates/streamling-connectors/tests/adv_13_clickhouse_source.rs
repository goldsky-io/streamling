//! Adversarial coverage for `clickhouse_native_to_decimal_arb` — the read-side
//! inverse of the ClickHouse `decimal_arb` sink encoder.
//!
//! ClickHouse stores `UInt256` / `Int256` **little-endian** and hands them to
//! Arrow as `FixedSizeBinary(32)`; `decimal_arb` canonical bytes are
//! `[sign_byte][big-endian minimal magnitude]`. Endianness, the signed vs
//! unsigned interpretation of one and the same 32 bytes, and the minimality of
//! the emitted magnitude are therefore the three places where a silent data
//! corruption can hide.
//!
//! Everything here is pure in-process function testing: no network, no
//! filesystem, no sleeps, no randomness.
//!
//! Functions under test:
//!   `streamling_connectors::table_providers::clickhouse::{
//!        clickhouse_native_to_decimal_arb, decimal_arb_to_clickhouse_native }`

use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
    FixedSizeBinaryBuilder, Int64Array, LargeBinaryArray, StringArray,
};
use arrow::datatypes::i256 as ArrowI256;
use arrow_schema::{DataType, Field, FieldRef};
use std::str::FromStr;
use std::sync::Arc;
use streamling_connectors::table_providers::clickhouse::{
    clickhouse_native_to_decimal_arb, decimal_arb_to_clickhouse_native,
};
use streamling_core::types::decimal_arb::{DecimalArbType, DecimalArbValue, NativeIntKind};

// ---------------------------------------------------------------------------
// Well-known decimal constants
// ---------------------------------------------------------------------------

/// 2^256 − 1 (UInt256 max, 78 digits).
const U256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";
/// 2^255 (first value that does NOT fit a signed Int256).
const TWO_POW_255: &str =
    "57896044618658097711785492504343953926634992332820282019728792003956564819968";
/// 2^255 − 1 (Int256 max).
const I256_MAX: &str =
    "57896044618658097711785492504343953926634992332820282019728792003956564819967";
/// −2^255 (Int256 min).
const I256_MIN: &str =
    "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
/// 10^50.
const TEN_POW_50: &str = "100000000000000000000000000000000000000000000000000";
/// 10^77.
const TEN_POW_77: &str =
    "100000000000000000000000000000000000000000000000000000000000000000000000000000";

// ---------------------------------------------------------------------------
// Independent oracles (deliberately *not* reusing the product's BigInt path)
// ---------------------------------------------------------------------------

/// Big-endian 32-byte buffer for a non-negative decimal digit string,
/// computed by schoolbook `acc = acc*10 + d` over a byte array.
fn be_from_dec(s: &str) -> [u8; 32] {
    let mut acc = [0u8; 32];
    for ch in s.bytes() {
        assert!(
            ch.is_ascii_digit(),
            "oracle input must be digits, got {s:?}"
        );
        let mut carry = u16::from(ch - b'0');
        for b in acc.iter_mut().rev() {
            let v = u16::from(*b) * 10 + carry;
            *b = (v & 0xFF) as u8;
            carry = v >> 8;
        }
        assert_eq!(carry, 0, "oracle overflow building 32-byte BE for {s}");
    }
    acc
}

/// Two's complement (negate) of a 32-byte big-endian buffer.
fn twos_be(mut be: [u8; 32]) -> [u8; 32] {
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

/// LE bytes ClickHouse would hold for an unsigned decimal string.
fn le_u(s: &str) -> [u8; 32] {
    to_le(be_from_dec(s))
}

/// LE bytes ClickHouse would hold for a signed decimal string (two's complement).
fn le_i(s: &str) -> [u8; 32] {
    match s.strip_prefix('-') {
        Some(mag) => to_le(twos_be(be_from_dec(mag))),
        None => le_u(s),
    }
}

/// LE bytes for a `u128`, zero-extended to 32 bytes. Completely independent of
/// the decimal-string oracle.
fn le_from_u128(v: u128) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&v.to_le_bytes());
    out
}

/// LE bytes for an `i128`, sign-extended to 32 bytes.
fn le_from_i128(v: i128) -> [u8; 32] {
    let fill = if v < 0 { 0xFFu8 } else { 0x00u8 };
    let mut out = [fill; 32];
    out[..16].copy_from_slice(&v.to_le_bytes());
    out
}

// ---------------------------------------------------------------------------
// Field / array helpers
// ---------------------------------------------------------------------------

fn arb_field(name: &str, p: u32, s: u32, kind: Option<NativeIntKind>) -> FieldRef {
    let f = DecimalArbType::field(name, p, s, true)
        .unwrap_or_else(|e| panic!("DecimalArbType::field({p},{s}): {e:?}"));
    let f = match kind {
        Some(k) => DecimalArbType::with_native_int_kind(f, k).expect("with_native_int_kind"),
        None => f,
    };
    Arc::new(f)
}

fn u_field() -> FieldRef {
    arb_field("v", 78, 0, Some(NativeIntKind::U256))
}

fn i_field() -> FieldRef {
    arb_field("v", 78, 0, Some(NativeIntKind::I256))
}

/// decimal_arb field carrying a raw (possibly non-canonical) hint string.
fn field_with_raw_hint(raw: &str) -> FieldRef {
    let f = DecimalArbType::field("v", 78, 0, true).unwrap();
    let mut md = f.metadata().clone();
    md.insert(
        DecimalArbType::NATIVE_INT_KIND_KEY.to_string(),
        raw.to_string(),
    );
    Arc::new(f.with_metadata(md))
}

fn fsb(rows: &[Option<[u8; 32]>]) -> FixedSizeBinaryArray {
    let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 32);
    for r in rows {
        match r {
            Some(v) => b.append_value(v).expect("append 32-byte row"),
            None => b.append_null(),
        }
    }
    b.finish()
}

fn fsb_one(le: [u8; 32]) -> FixedSizeBinaryArray {
    fsb(&[Some(le)])
}

fn fsb_width(rows: &[&[u8]], width: i32) -> FixedSizeBinaryArray {
    let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), width);
    for r in rows {
        b.append_value(r).expect("append row of declared width");
    }
    b.finish()
}

/// Read a whole column and return it as a `LargeBinaryArray` of canonical bytes.
fn read_col(field: &FieldRef, arr: &dyn Array) -> LargeBinaryArray {
    let out = clickhouse_native_to_decimal_arb(arr, field)
        .unwrap_or_else(|e| panic!("clickhouse_native_to_decimal_arb must succeed: {e}"));
    out.as_any()
        .downcast_ref::<LargeBinaryArray>()
        .expect("read side must produce LargeBinaryArray")
        .clone()
}

/// Canonical bytes produced for a single LE row.
fn read_canonical(field: &FieldRef, le: [u8; 32]) -> Vec<u8> {
    let arr = fsb_one(le);
    read_col(field, &arr).value(0).to_vec()
}

/// Decoded plain-decimal string for a single LE row, at the field's scale.
fn read_str(field: &FieldRef, le: [u8; 32]) -> String {
    let bytes = read_canonical(field, le);
    let (_p, scale) = DecimalArbType::precision_scale_from_field(field).expect("decimal_arb field");
    DecimalArbValue::from_canonical_bytes_at_scale(&bytes, scale)
        .expect("read side must emit decodable canonical bytes")
        .to_canonical_string()
}

/// Sink direction: canonical decimal strings → ClickHouse LE FixedSizeBinary(32).
fn sink(field: &FieldRef, vals: &[Option<&str>]) -> ArrayRef {
    let (_p, scale) = DecimalArbType::precision_scale_from_field(field).expect("decimal_arb field");
    let owned: Vec<Option<Vec<u8>>> = vals
        .iter()
        .map(|v| {
            v.map(|s| {
                DecimalArbValue::from_str(s)
                    .unwrap_or_else(|e| panic!("from_str({s:?}): {e:?}"))
                    .to_canonical_bytes_at_scale(scale)
            })
        })
        .collect();
    let refs: Vec<Option<&[u8]>> = owned.iter().map(|o| o.as_deref()).collect();
    let arr = LargeBinaryArray::from(refs);
    decimal_arb_to_clickhouse_native(&arr, field)
        .unwrap_or_else(|e| panic!("decimal_arb_to_clickhouse_native must succeed: {e}"))
}

fn sink_le(field: &FieldRef, v: &str) -> [u8; 32] {
    let out = sink(field, &[Some(v)]);
    let f = out
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("sink emits FixedSizeBinaryArray");
    let mut buf = [0u8; 32];
    buf.copy_from_slice(f.value(0));
    buf
}

/// Full round trip: canonical → ClickHouse LE → canonical. Returns the
/// recovered canonical bytes so callers can assert *byte* identity, which is
/// strictly stronger than numeric equality.
fn round_trip_canonical(field: &FieldRef, v: &str) -> Vec<u8> {
    let le = sink(field, &[Some(v)]);
    read_col(field, le.as_ref()).value(0).to_vec()
}

fn canonical_of(field: &FieldRef, v: &str) -> Vec<u8> {
    let (_p, scale) = DecimalArbType::precision_scale_from_field(field).expect("decimal_arb field");
    DecimalArbValue::from_str(v)
        .unwrap()
        .to_canonical_bytes_at_scale(scale)
}

fn assert_round_trip(field: &FieldRef, v: &str) {
    let got = round_trip_canonical(field, v);
    let want = canonical_of(field, v);
    assert_eq!(
        got, want,
        "round trip decimal_arb -> ClickHouse LE -> decimal_arb must be byte-identical for {v}"
    );
}

fn err_of(field: &FieldRef, arr: &dyn Array) -> String {
    match clickhouse_native_to_decimal_arb(arr, field) {
        Ok(_) => panic!("expected an error, got Ok"),
        Err(e) => e.to_string(),
    }
}

// ===========================================================================
// 1. Little-endian byte order, unsigned, verified against a u128 oracle
// ===========================================================================

#[test]
fn u256_reads_zero_as_zero() {
    assert_eq!(
        read_str(&u_field(), le_from_u128(0)),
        "0",
        "all-zero LE bytes must decode to 0"
    );
}

#[test]
fn u256_reads_one_from_the_lowest_le_byte() {
    let le = le_from_u128(1);
    assert_eq!(le[0], 1, "oracle sanity: LE puts the low byte first");
    assert_eq!(
        read_str(&u_field(), le),
        "1",
        "LE 0x01 00.. must decode to 1, not to 2^248"
    );
}

#[test]
fn u256_reads_255() {
    assert_eq!(read_str(&u_field(), le_from_u128(255)), "255");
}

#[test]
fn u256_reads_256_which_lives_in_the_second_le_byte() {
    let le = le_from_u128(256);
    assert_eq!((le[0], le[1]), (0, 1), "oracle sanity for 256 in LE");
    assert_eq!(
        read_str(&u_field(), le),
        "256",
        "a byte-reversed read would produce 2^240 here"
    );
}

#[test]
fn u256_reads_65535() {
    assert_eq!(read_str(&u_field(), le_from_u128(65_535)), "65535");
}

#[test]
fn u256_reads_65536() {
    assert_eq!(read_str(&u_field(), le_from_u128(65_536)), "65536");
}

#[test]
fn u256_reads_two_pow_31() {
    assert_eq!(
        read_str(&u_field(), le_from_u128(1u128 << 31)),
        "2147483648"
    );
}

#[test]
fn u256_reads_two_pow_32() {
    assert_eq!(
        read_str(&u_field(), le_from_u128(1u128 << 32)),
        "4294967296"
    );
}

#[test]
fn u256_reads_two_pow_63() {
    assert_eq!(
        read_str(&u_field(), le_from_u128(1u128 << 63)),
        "9223372036854775808"
    );
}

#[test]
fn u256_reads_two_pow_63_minus_one() {
    assert_eq!(
        read_str(&u_field(), le_from_u128((1u128 << 63) - 1)),
        "9223372036854775807"
    );
}

#[test]
fn u256_reads_two_pow_64() {
    assert_eq!(
        read_str(&u_field(), le_from_u128(1u128 << 64)),
        "18446744073709551616"
    );
}

#[test]
fn u256_reads_two_pow_64_minus_one() {
    assert_eq!(
        read_str(&u_field(), le_from_u128(u64::MAX as u128)),
        "18446744073709551615"
    );
}

#[test]
fn u256_reads_two_pow_127() {
    assert_eq!(
        read_str(&u_field(), le_from_u128(1u128 << 127)),
        "170141183460469231731687303715884105728"
    );
}

#[test]
fn u256_reads_u128_max() {
    assert_eq!(
        read_str(&u_field(), le_from_u128(u128::MAX)),
        "340282366920938463463374607431768211455"
    );
}

#[test]
fn u256_reads_asymmetric_u64_pattern() {
    // 0x0123456789ABCDEF — every byte distinct, so a wrong-endian read cannot
    // accidentally produce the same number.
    let v: u128 = 0x0123_4567_89AB_CDEF;
    assert_eq!(read_str(&u_field(), le_from_u128(v)), v.to_string());
}

#[test]
fn u256_reads_asymmetric_u128_pattern() {
    let v: u128 = 0x0011_2233_4455_6677_8899_AABB_CCDD_EEFF;
    assert_eq!(read_str(&u_field(), le_from_u128(v)), v.to_string());
}

#[test]
fn u256_asymmetric_pattern_is_not_the_byte_reversed_value() {
    // Guard the guard: if the read side silently reversed, this test's
    // expectation would coincide with the reversed reading. Prove it does not.
    let v: u128 = 0x0123_4567_89AB_CDEF;
    let le = le_from_u128(v);
    let reversed_reading = read_str(&u_field(), to_le(le));
    assert_ne!(
        reversed_reading,
        v.to_string(),
        "the chosen pattern must not be a byte palindrome, else the test is vacuous"
    );
}

#[test]
fn u256_low_and_high_le_bytes_are_not_interchangeable() {
    let mut low = [0u8; 32];
    low[0] = 1;
    let mut high = [0u8; 32];
    high[31] = 1;
    let f = u_field();
    assert_eq!(read_str(&f, low), "1");
    assert_ne!(
        read_str(&f, high),
        "1",
        "byte 31 is the most significant byte in LE, it cannot decode to 1"
    );
}

#[test]
fn u256_highest_le_byte_is_the_most_significant() {
    let mut high = [0u8; 32];
    high[31] = 1;
    let mut want = vec![0x00u8, 0x01];
    want.extend_from_slice(&[0u8; 31]);
    assert_eq!(
        read_canonical(&u_field(), high),
        want,
        "LE byte 31 is the top big-endian byte, i.e. 2^248"
    );
}

// ===========================================================================
// 2. Wide values, verified against the decimal-string oracle
// ===========================================================================

#[test]
fn u256_reads_two_pow_255() {
    assert_eq!(read_str(&u_field(), le_u(TWO_POW_255)), TWO_POW_255);
}

#[test]
fn u256_reads_two_pow_255_minus_one() {
    assert_eq!(read_str(&u_field(), le_u(I256_MAX)), I256_MAX);
}

#[test]
fn u256_reads_max_from_all_ones() {
    assert_eq!(
        read_str(&u_field(), [0xFFu8; 32]),
        U256_MAX,
        "all-ones LE read as unsigned must be 2^256-1"
    );
}

#[test]
fn u256_reads_ten_pow_50() {
    assert_eq!(read_str(&u_field(), le_u(TEN_POW_50)), TEN_POW_50);
}

#[test]
fn u256_reads_ten_pow_77() {
    assert_eq!(read_str(&u_field(), le_u(TEN_POW_77)), TEN_POW_77);
}

#[test]
fn u256_max_has_78_digits() {
    let s = read_str(&u_field(), [0xFFu8; 32]);
    assert_eq!(s.len(), 78, "2^256-1 is a 78-digit decimal, got {s}");
}

#[test]
fn u256_two_pow_255_high_bit_is_in_last_le_byte() {
    let le = le_u(TWO_POW_255);
    assert_eq!(
        le[31], 0x80,
        "oracle sanity: 2^255 sets bit 7 of LE byte 31"
    );
    assert!(le[..31].iter().all(|&b| b == 0));
}

#[test]
fn canonical_output_is_sign_byte_followed_by_big_endian_magnitude() {
    // BE = 01 02 03 ... 20 : maximally asymmetric, no leading zeros.
    let mut be = [0u8; 32];
    for (i, b) in be.iter_mut().enumerate() {
        *b = (i + 1) as u8;
    }
    let got = read_canonical(&u_field(), to_le(be));
    let mut want = vec![0x00u8];
    want.extend_from_slice(&be);
    assert_eq!(
        got, want,
        "canonical must be [0x00][big-endian magnitude]; a wrong-endian read \
         would emit the byte-reversed magnitude"
    );
}

#[test]
fn canonical_output_strips_leading_zero_bytes() {
    let got = read_canonical(&u_field(), le_from_u128(1));
    assert_eq!(
        got,
        vec![0x00, 0x01],
        "magnitude must be minimal, not 32 zero-padded bytes"
    );
}

#[test]
fn canonical_zero_is_a_lone_sign_byte() {
    assert_eq!(
        read_canonical(&u_field(), [0u8; 32]),
        vec![0x00],
        "zero must encode as a single 0x00 byte"
    );
}

#[test]
fn canonical_output_for_u256_never_uses_the_negative_sign_byte() {
    let f = u_field();
    for le in [
        [0u8; 32],
        [0xFFu8; 32],
        le_u(TWO_POW_255),
        le_from_u128(1),
        le_from_u128(u128::MAX),
    ] {
        assert_eq!(
            read_canonical(&f, le)[0],
            0x00,
            "u256 columns are unsigned; sign byte must always be 0x00"
        );
    }
}

#[test]
fn canonical_output_magnitude_has_no_leading_zero_byte() {
    let f = u_field();
    for le in [
        le_from_u128(1),
        le_from_u128(256),
        le_from_u128(u64::MAX as u128),
        le_u(TEN_POW_50),
    ] {
        let c = read_canonical(&f, le);
        assert!(c.len() >= 2, "non-zero values need a magnitude");
        assert_ne!(c[1], 0x00, "leading magnitude byte must be non-zero: {c:?}");
    }
}

// ===========================================================================
// 3. Signed (i256) interpretation of the same 32 bytes
// ===========================================================================

#[test]
fn i256_reads_zero() {
    assert_eq!(read_str(&i_field(), [0u8; 32]), "0");
}

#[test]
fn i256_reads_one() {
    assert_eq!(read_str(&i_field(), le_from_i128(1)), "1");
}

#[test]
fn i256_reads_negative_one_from_all_ones() {
    assert_eq!(
        read_str(&i_field(), [0xFFu8; 32]),
        "-1",
        "all-ones two's complement is -1 for a signed column"
    );
}

#[test]
fn i256_reads_negative_255() {
    assert_eq!(read_str(&i_field(), le_from_i128(-255)), "-255");
}

#[test]
fn i256_reads_negative_256() {
    assert_eq!(read_str(&i_field(), le_from_i128(-256)), "-256");
}

#[test]
fn i256_reads_negative_two_pow_63() {
    assert_eq!(
        read_str(&i_field(), le_from_i128(-(1i128 << 63))),
        "-9223372036854775808"
    );
}

#[test]
fn i256_reads_i128_min() {
    assert_eq!(
        read_str(&i_field(), le_from_i128(i128::MIN)),
        "-170141183460469231731687303715884105728"
    );
}

#[test]
fn i256_reads_i128_max() {
    assert_eq!(
        read_str(&i_field(), le_from_i128(i128::MAX)),
        "170141183460469231731687303715884105727"
    );
}

#[test]
fn i256_reads_max_signed_value() {
    assert_eq!(read_str(&i_field(), le_u(I256_MAX)), I256_MAX);
}

#[test]
fn i256_reads_min_signed_value() {
    let le = le_i(I256_MIN);
    assert_eq!(le[31], 0x80, "oracle sanity: -2^255 is 0x80 00..00 BE");
    assert_eq!(read_str(&i_field(), le), I256_MIN);
}

#[test]
fn i256_reads_the_two_pow_255_bit_pattern_as_min_negative() {
    // The unsigned value 2^255 and the signed value -2^255 share bytes.
    assert_eq!(
        read_str(&i_field(), le_u(TWO_POW_255)),
        I256_MIN,
        "for an i256 column the top bit is the sign bit"
    );
}

#[test]
fn i256_all_ones_is_not_read_as_u256_max() {
    assert_ne!(
        read_str(&i_field(), [0xFFu8; 32]),
        U256_MAX,
        "the native_int_kind hint must actually change the interpretation"
    );
}

#[test]
fn same_bytes_differ_between_u256_and_i256_when_high_bit_set() {
    let le = [0xFFu8; 32];
    assert_eq!(read_str(&u_field(), le), U256_MAX);
    assert_eq!(read_str(&i_field(), le), "-1");
}

#[test]
fn same_bytes_agree_between_u256_and_i256_when_high_bit_clear() {
    for le in [
        le_from_u128(0),
        le_from_u128(1),
        le_from_u128(u128::MAX),
        le_u(I256_MAX),
    ] {
        assert_eq!(
            read_str(&u_field(), le),
            read_str(&i_field(), le),
            "with the sign bit clear both interpretations must coincide"
        );
    }
}

#[test]
fn i256_negative_canonical_sign_byte_is_ff() {
    let c = read_canonical(&i_field(), [0xFFu8; 32]);
    assert_eq!(c[0], 0xFF, "negative values carry sign byte 0xFF");
}

#[test]
fn i256_negative_one_canonical_is_exactly_two_bytes() {
    assert_eq!(
        read_canonical(&i_field(), [0xFFu8; 32]),
        vec![0xFF, 0x01],
        "-1 must be sign 0xFF plus the minimal magnitude 0x01"
    );
}

#[test]
fn i256_negative_magnitude_is_minimal() {
    let c = read_canonical(&i_field(), le_from_i128(-256));
    assert_eq!(c, vec![0xFF, 0x01, 0x00], "-256 magnitude is 0x0100");
}

#[test]
fn i256_min_canonical_magnitude_is_32_bytes() {
    let c = read_canonical(&i_field(), le_i(I256_MIN));
    assert_eq!(c.len(), 33, "sign byte plus 32 magnitude bytes");
    assert_eq!(c[0], 0xFF);
    assert_eq!(c[1], 0x80);
    assert!(c[2..].iter().all(|&b| b == 0));
}

#[test]
fn i256_zero_never_encodes_as_negative_zero() {
    let c = read_canonical(&i_field(), [0u8; 32]);
    assert_eq!(
        c,
        vec![0x00],
        "negative zero is an invalid canonical encoding"
    );
}

#[test]
fn i256_reads_asymmetric_negative_pattern() {
    let v: i128 = -0x0123_4567_89AB_CDEF;
    assert_eq!(read_str(&i_field(), le_from_i128(v)), v.to_string());
}

#[test]
fn i256_negative_values_are_dense_around_zero() {
    let f = i_field();
    for v in [-1i128, -2, -3, -127, -128, -129, -32768, -32769] {
        assert_eq!(
            read_str(&f, le_from_i128(v)),
            v.to_string(),
            "two's complement decode failed for {v}"
        );
    }
}

#[test]
fn i256_positive_values_are_dense_around_zero() {
    let f = i_field();
    for v in [1i128, 2, 3, 127, 128, 129, 32767, 32768] {
        assert_eq!(read_str(&f, le_from_i128(v)), v.to_string());
    }
}

// ===========================================================================
// 4. Nulls, ordering, slicing, array shape
// ===========================================================================

#[test]
fn null_rows_are_preserved() {
    let f = u_field();
    let arr = fsb(&[Some(le_from_u128(1)), None, Some(le_from_u128(2))]);
    let out = read_col(&f, &arr);
    assert!(!out.is_null(0));
    assert!(out.is_null(1), "null must survive the read conversion");
    assert!(!out.is_null(2));
}

#[test]
fn null_rows_do_not_shift_the_surrounding_values() {
    let f = u_field();
    let arr = fsb(&[
        None,
        Some(le_from_u128(7)),
        None,
        Some(le_from_u128(8)),
        None,
    ]);
    let out = read_col(&f, &arr);
    let dec = |i: usize| {
        DecimalArbValue::from_canonical_bytes_at_scale(out.value(i), 0)
            .unwrap()
            .to_canonical_string()
    };
    assert_eq!(dec(1), "7");
    assert_eq!(dec(3), "8");
    assert_eq!(out.len(), 5);
}

#[test]
fn all_null_array_reads_as_all_null() {
    let f = i_field();
    let arr = fsb(&[None, None, None]);
    let out = read_col(&f, &arr);
    assert_eq!(out.null_count(), 3);
    assert_eq!(out.len(), 3);
}

#[test]
fn empty_array_reads_as_empty() {
    let f = u_field();
    let arr = fsb(&[]);
    let out = read_col(&f, &arr);
    assert_eq!(out.len(), 0, "empty input must not fabricate rows");
}

#[test]
fn output_data_type_is_large_binary() {
    let f = u_field();
    let arr = fsb_one(le_from_u128(5));
    let out = clickhouse_native_to_decimal_arb(&arr, &f).unwrap();
    assert_eq!(
        out.data_type(),
        &DataType::LargeBinary,
        "decimal_arb storage is LargeBinary"
    );
}

#[test]
fn output_length_matches_input_length() {
    let f = u_field();
    let rows: Vec<Option<[u8; 32]>> = (0..17u128).map(|v| Some(le_from_u128(v))).collect();
    let arr = fsb(&rows);
    assert_eq!(read_col(&f, &arr).len(), 17);
}

#[test]
fn output_null_count_matches_input_null_count() {
    let f = u_field();
    let arr = fsb(&[Some(le_from_u128(1)), None, None, Some(le_from_u128(4))]);
    assert_eq!(read_col(&f, &arr).null_count(), arr.null_count());
}

#[test]
fn multi_row_batch_preserves_value_order() {
    let f = u_field();
    let vals: Vec<u128> = vec![0, 1, 2, 300, 70_000, 1 << 40, u64::MAX as u128];
    let rows: Vec<Option<[u8; 32]>> = vals.iter().map(|v| Some(le_from_u128(*v))).collect();
    let out = read_col(&f, &fsb(&rows));
    for (i, v) in vals.iter().enumerate() {
        let got = DecimalArbValue::from_canonical_bytes_at_scale(out.value(i), 0)
            .unwrap()
            .to_canonical_string();
        assert_eq!(got, v.to_string(), "row {i} out of order or corrupted");
    }
}

#[test]
fn sliced_input_reads_the_correct_rows() {
    let f = u_field();
    let arr = fsb(&[
        Some(le_from_u128(10)),
        Some(le_from_u128(20)),
        Some(le_from_u128(30)),
        Some(le_from_u128(40)),
    ]);
    let sliced: ArrayRef = Array::slice(&arr, 1, 2);
    let out = read_col(&f, sliced.as_ref());
    assert_eq!(out.len(), 2, "slice length must be honoured");
    let dec = |i: usize| {
        DecimalArbValue::from_canonical_bytes_at_scale(out.value(i), 0)
            .unwrap()
            .to_canonical_string()
    };
    assert_eq!(dec(0), "20", "slice offset ignored -> silent row shift");
    assert_eq!(dec(1), "30");
}

#[test]
fn sliced_input_respects_the_null_mask() {
    let f = u_field();
    let arr = fsb(&[Some(le_from_u128(1)), None, Some(le_from_u128(3)), None]);
    let sliced: ArrayRef = Array::slice(&arr, 1, 3);
    let out = read_col(&f, sliced.as_ref());
    assert!(out.is_null(0), "sliced null mask lost");
    assert!(!out.is_null(1));
    assert!(out.is_null(2));
}

#[test]
fn sliced_to_zero_length_reads_as_empty() {
    let f = u_field();
    let arr = fsb(&[Some(le_from_u128(1)), Some(le_from_u128(2))]);
    let sliced: ArrayRef = Array::slice(&arr, 2, 0);
    let out = read_col(&f, sliced.as_ref());
    assert_eq!(out.len(), 0);
}

#[test]
fn large_batch_round_trips_every_row() {
    let f = i_field();
    let vals: Vec<i128> = (-40i128..40).collect();
    let rows: Vec<Option<[u8; 32]>> = vals.iter().map(|v| Some(le_from_i128(*v))).collect();
    let out = read_col(&f, &fsb(&rows));
    for (i, v) in vals.iter().enumerate() {
        let got = DecimalArbValue::from_canonical_bytes_at_scale(out.value(i), 0)
            .unwrap()
            .to_canonical_string();
        assert_eq!(got, v.to_string(), "row {i}");
    }
}

// ===========================================================================
// 5. Hint handling and input-type rejection
// ===========================================================================

#[test]
fn missing_native_int_kind_hint_is_rejected() {
    let f = arb_field("balance", 78, 0, None);
    let msg = err_of(&f, &fsb_one(le_from_u128(1)));
    assert!(
        msg.contains("native_int_kind"),
        "error must name the missing hint: {msg}"
    );
}

#[test]
fn missing_hint_error_names_the_column() {
    let f = arb_field("balance", 78, 0, None);
    let msg = err_of(&f, &fsb_one(le_from_u128(1)));
    assert!(msg.contains("balance"), "error must name the column: {msg}");
}

#[test]
fn non_decimal_arb_field_is_rejected() {
    let f: FieldRef = Arc::new(Field::new("v", DataType::LargeBinary, true));
    let msg = err_of(&f, &fsb_one(le_from_u128(1)));
    assert!(
        !msg.is_empty(),
        "a plain LargeBinary field is not decimal_arb and must be rejected"
    );
}

#[test]
fn plain_binary_field_is_not_decimal_arb_and_is_rejected() {
    let f: FieldRef = Arc::new(Field::new("v", DataType::Binary, true));
    let msg = err_of(&f, &fsb_one(le_from_u128(1)));
    assert!(!msg.is_empty(), "Binary is not decimal_arb storage: {msg}");
}

#[test]
fn unrecognized_hint_value_is_rejected() {
    let f = field_with_raw_hint("u512");
    let msg = err_of(&f, &fsb_one(le_from_u128(1)));
    assert!(
        msg.contains("native_int_kind"),
        "an unparseable hint must be treated as absent: {msg}"
    );
}

#[test]
fn empty_hint_value_is_rejected() {
    let f = field_with_raw_hint("");
    let msg = err_of(&f, &fsb_one(le_from_u128(1)));
    assert!(msg.contains("native_int_kind"), "{msg}");
}

#[test]
fn uppercase_hint_is_accepted_case_insensitively() {
    let f = field_with_raw_hint("U256");
    assert_eq!(
        read_str(&f, le_from_u128(9)),
        "9",
        "NativeIntKind::parse is documented case-insensitive"
    );
}

#[test]
fn mixed_case_signed_hint_selects_the_signed_interpretation() {
    let f = field_with_raw_hint("I256");
    assert_eq!(read_str(&f, [0xFFu8; 32]), "-1");
}

#[test]
fn whitespace_padded_hint_is_accepted() {
    let f = field_with_raw_hint("  i256  ");
    assert_eq!(read_str(&f, [0xFFu8; 32]), "-1");
}

#[test]
fn large_binary_input_array_is_rejected() {
    let f = u_field();
    let arr = LargeBinaryArray::from(vec![Some(&[0u8; 32][..])]);
    let msg = err_of(&f, &arr);
    assert!(
        msg.contains("FixedSizeBinary"),
        "must name the expected storage: {msg}"
    );
}

#[test]
fn binary_input_array_is_rejected() {
    let f = u_field();
    let arr = BinaryArray::from(vec![Some(&[0u8; 32][..])]);
    let msg = err_of(&f, &arr);
    assert!(msg.contains("FixedSizeBinary"), "{msg}");
}

#[test]
fn int64_input_array_is_rejected() {
    let f = u_field();
    let arr = Int64Array::from(vec![1i64, 2]);
    let msg = err_of(&f, &arr);
    assert!(msg.contains("FixedSizeBinary"), "{msg}");
}

#[test]
fn utf8_input_array_is_rejected() {
    // The Utf8 (coerce_to: string) path has its own reader; the native reader
    // must not silently accept text.
    let f = u_field();
    let arr = StringArray::from(vec![Some("123")]);
    let msg = err_of(&f, &arr);
    assert!(msg.contains("FixedSizeBinary"), "{msg}");
}

#[test]
fn decimal128_input_array_is_rejected() {
    let f = u_field();
    let arr = Decimal128Array::from(vec![123i128])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let msg = err_of(&f, &arr);
    assert!(
        msg.contains("FixedSizeBinary"),
        "a ClickHouse Decimal(p,s) column must not be read as a native wide int: {msg}"
    );
}

#[test]
fn decimal256_input_array_is_rejected() {
    // Decimal256 is 32 bytes wide internally — exactly the trap where a
    // width-only check would let a Decimal(76, s) column through.
    let f = u_field();
    let arr = Decimal256Array::from(vec![ArrowI256::from_i128(123)])
        .with_precision_and_scale(40, 2)
        .unwrap();
    let msg = err_of(&f, &arr);
    assert!(msg.contains("FixedSizeBinary"), "{msg}");
}

#[test]
fn wrong_input_type_error_names_the_column() {
    let f = arb_field("balance", 78, 0, Some(NativeIntKind::U256));
    let msg = err_of(&f, &Int64Array::from(vec![1i64]));
    assert!(msg.contains("balance"), "{msg}");
}

#[test]
fn wrong_input_type_error_reports_the_actual_type() {
    let f = u_field();
    let msg = err_of(&f, &Int64Array::from(vec![1i64]));
    assert!(
        msg.contains("Int64"),
        "error should show what arrived: {msg}"
    );
}

#[test]
fn hint_check_precedes_input_type_check() {
    // Both are wrong; the message should still be actionable.
    let f = arb_field("balance", 78, 0, None);
    let msg = err_of(&f, &Int64Array::from(vec![1i64]));
    assert!(msg.contains("balance"), "{msg}");
}

// ===========================================================================
// 6. Wrong FixedSizeBinary widths
// ===========================================================================

#[test]
#[ignore = "FINDING: clickhouse_native_to_decimal_arb panics (copy_from_slice length mismatch) on FixedSizeBinary widths other than 32 instead of returning a typed error"]
fn fixed_size_binary_16_is_rejected_with_an_error_not_a_panic() {
    let f = u_field();
    let arr = fsb_width(&[&[0u8; 16][..]], 16);
    let msg = err_of(&f, &arr);
    assert!(
        msg.contains("FixedSizeBinary"),
        "a 16-byte column (e.g. ClickHouse UUID / Decimal128) must be rejected: {msg}"
    );
}

#[test]
#[ignore = "FINDING: clickhouse_native_to_decimal_arb panics on FixedSizeBinary(31) instead of returning a typed error"]
fn fixed_size_binary_31_is_rejected_with_an_error_not_a_panic() {
    let f = u_field();
    let arr = fsb_width(&[&[0u8; 31][..]], 31);
    let msg = err_of(&f, &arr);
    assert!(msg.contains("FixedSizeBinary"), "{msg}");
}

#[test]
#[ignore = "FINDING: clickhouse_native_to_decimal_arb panics on FixedSizeBinary(33) instead of returning a typed error"]
fn fixed_size_binary_33_is_rejected_with_an_error_not_a_panic() {
    let f = u_field();
    let arr = fsb_width(&[&[0u8; 33][..]], 33);
    let msg = err_of(&f, &arr);
    assert!(msg.contains("FixedSizeBinary"), "{msg}");
}

#[test]
#[ignore = "FINDING: clickhouse_native_to_decimal_arb panics on non-32-byte FixedSizeBinary widths instead of returning a typed error"]
fn fixed_size_binary_1_is_rejected_with_an_error_not_a_panic() {
    let f = u_field();
    let arr = fsb_width(&[&[7u8][..]], 1);
    let msg = err_of(&f, &arr);
    assert!(msg.contains("FixedSizeBinary"), "{msg}");
}

#[test]
fn fixed_size_binary_32_with_all_null_rows_of_wrong_width_is_not_reachable() {
    // Sanity: a FixedSizeBinary(32) array whose only rows are null never
    // touches the 32-byte copy path, so it must succeed.
    let f = u_field();
    let mut b = FixedSizeBinaryBuilder::with_capacity(2, 32);
    b.append_null();
    b.append_null();
    let arr = b.finish();
    assert_eq!(read_col(&f, &arr).null_count(), 2);
}

// ===========================================================================
// 7. Scale semantics
// ===========================================================================

#[test]
fn scale_zero_reads_the_raw_integer() {
    let f = arb_field("v", 78, 0, Some(NativeIntKind::U256));
    assert_eq!(read_str(&f, le_from_u128(12_345)), "12345");
}

#[test]
fn scale_two_places_the_decimal_point() {
    let f = arb_field("v", 78, 2, Some(NativeIntKind::U256));
    assert_eq!(
        read_str(&f, le_from_u128(12_345)),
        "123.45",
        "the raw integer is the unscaled value at the field's scale"
    );
}

#[test]
fn scale_18_reads_the_smallest_representable_unit() {
    let f = arb_field("v", 78, 18, Some(NativeIntKind::U256));
    assert_eq!(read_str(&f, le_from_u128(1)), "0.000000000000000001");
}

#[test]
fn scale_equal_to_precision_is_supported() {
    let f = arb_field("v", 18, 18, Some(NativeIntKind::U256));
    assert_eq!(read_str(&f, le_from_u128(1)), "0.000000000000000001");
}

#[test]
fn canonical_bytes_are_independent_of_the_declared_scale() {
    // The scale lives on the Field, never in the bytes.
    let a = read_canonical(
        &arb_field("v", 78, 0, Some(NativeIntKind::U256)),
        le_from_u128(7),
    );
    let b = read_canonical(
        &arb_field("v", 78, 9, Some(NativeIntKind::U256)),
        le_from_u128(7),
    );
    assert_eq!(
        a, b,
        "canonical magnitude must not change with the column scale"
    );
}

#[test]
fn reading_the_same_bytes_at_two_scales_differs_by_a_power_of_ten() {
    let two = arb_field("v", 78, 2, Some(NativeIntKind::U256));
    let four = arb_field("v", 78, 4, Some(NativeIntKind::U256));
    assert_eq!(read_str(&two, le_from_u128(12_345)), "123.45");
    assert_eq!(read_str(&four, le_from_u128(12_345)), "1.2345");
}

#[test]
fn negative_scaled_values_read_correctly() {
    let f = arb_field("v", 78, 2, Some(NativeIntKind::I256));
    assert_eq!(read_str(&f, le_from_i128(-1)), "-0.01");
    assert_eq!(read_str(&f, le_from_i128(-12_345)), "-123.45");
}

#[test]
fn scaled_zero_still_reads_as_plain_zero() {
    let f = arb_field("v", 78, 6, Some(NativeIntKind::I256));
    assert_eq!(
        read_canonical(&f, [0u8; 32]),
        vec![0x00],
        "zero has no scale-dependent encoding"
    );
}

#[test]
fn scale_does_not_change_the_null_mask() {
    let f = arb_field("v", 78, 12, Some(NativeIntKind::U256));
    let arr = fsb(&[None, Some(le_from_u128(1))]);
    let out = read_col(&f, &arr);
    assert!(out.is_null(0));
    assert!(!out.is_null(1));
}

#[test]
fn wide_scaled_value_reads_without_truncation() {
    let f = arb_field("v", 78, 18, Some(NativeIntKind::U256));
    // 2^200 unscaled, read at scale 18.
    let unscaled = "1606938044258990275541962092341162602522202993782792835301376";
    let got = read_str(&f, le_u(unscaled));
    assert_eq!(
        got.replace('.', "").trim_start_matches('0'),
        unscaled,
        "no digits may be lost when a scale is applied"
    );
}

// ===========================================================================
// 8. Full round trip: decimal_arb -> ClickHouse LE -> decimal_arb
// ===========================================================================

#[test]
fn round_trip_u256_zero() {
    assert_round_trip(&u_field(), "0");
}

#[test]
fn round_trip_u256_one() {
    assert_round_trip(&u_field(), "1");
}

#[test]
fn round_trip_u256_small_values() {
    let f = u_field();
    for v in [
        "2", "9", "10", "99", "100", "255", "256", "257", "65535", "65536",
    ] {
        assert_round_trip(&f, v);
    }
}

#[test]
fn round_trip_u256_powers_of_two() {
    let f = u_field();
    for e in [7u32, 8, 15, 16, 31, 32, 63, 64, 100, 127] {
        let v = (1u128 << e).to_string();
        assert_round_trip(&f, &v);
    }
    // 2^128 and 2^200 exceed u128, so use the decimal-string oracle.
    assert_round_trip(&f, "340282366920938463463374607431768211456");
    assert_round_trip(
        &f,
        "1606938044258990275541962092341162602522202993782792835301376",
    );
}

#[test]
fn round_trip_u256_two_pow_255() {
    assert_round_trip(&u_field(), TWO_POW_255);
}

#[test]
fn round_trip_u256_two_pow_255_minus_one() {
    assert_round_trip(&u_field(), I256_MAX);
}

#[test]
fn round_trip_u256_max() {
    assert_round_trip(&u_field(), U256_MAX);
}

#[test]
fn round_trip_u256_ten_powers() {
    let f = u_field();
    for v in [TEN_POW_50, TEN_POW_77, "1000000000000000000000000"] {
        assert_round_trip(&f, v);
    }
}

#[test]
fn round_trip_u256_asymmetric_values() {
    let f = u_field();
    let vals: [u128; 3] = [
        0x0123_4567_89AB_CDEF,
        0x0011_2233_4455_6677_8899_AABB_CCDD_EEFF,
        u128::MAX,
    ];
    for v in vals {
        assert_round_trip(&f, &v.to_string());
    }
}

#[test]
fn round_trip_i256_zero() {
    assert_round_trip(&i_field(), "0");
}

#[test]
fn round_trip_i256_one_and_negative_one() {
    let f = i_field();
    assert_round_trip(&f, "1");
    assert_round_trip(&f, "-1");
}

#[test]
fn round_trip_i256_negative_small_values() {
    let f = i_field();
    for v in ["-2", "-9", "-10", "-99", "-100", "-255", "-256", "-257"] {
        assert_round_trip(&f, v);
    }
}

#[test]
fn round_trip_i256_negative_powers_of_two() {
    let f = i_field();
    for e in [7u32, 8, 15, 16, 31, 32, 63, 64, 100, 127] {
        let v = format!("-{}", 1u128 << e);
        assert_round_trip(&f, &v);
    }
}

#[test]
fn round_trip_i256_max() {
    assert_round_trip(&i_field(), I256_MAX);
}

#[test]
fn round_trip_i256_min() {
    assert_round_trip(&i_field(), I256_MIN);
}

#[test]
fn round_trip_i256_around_the_signed_boundary() {
    let f = i_field();
    for v in [
        "57896044618658097711785492504343953926634992332820282019728792003956564819966",
        I256_MAX,
        "-57896044618658097711785492504343953926634992332820282019728792003956564819967",
        I256_MIN,
    ] {
        assert_round_trip(&f, v);
    }
}

#[test]
fn round_trip_i256_asymmetric_negative_values() {
    let f = i_field();
    for v in [
        "-81985529216486895",
        "-170141183460469231731687303715884105728",
    ] {
        assert_round_trip(&f, v);
    }
}

#[test]
fn round_trip_is_byte_identical_not_merely_numerically_equal() {
    let f = u_field();
    let got = round_trip_canonical(&f, "1000");
    assert_eq!(
        got,
        vec![0x00, 0x03, 0xE8],
        "the recovered canonical bytes must be the minimal form"
    );
}

#[test]
fn round_trip_preserves_null_positions() {
    let f = u_field();
    let le = sink(&f, &[Some("1"), None, Some("2"), None]);
    let out = read_col(&f, le.as_ref());
    assert_eq!(out.len(), 4);
    assert!(!out.is_null(0));
    assert!(out.is_null(1));
    assert!(!out.is_null(2));
    assert!(out.is_null(3));
}

#[test]
fn round_trip_of_a_multi_row_batch_preserves_every_value() {
    let f = i_field();
    let vals = ["-5", "0", "5", "-100000", "123456789012345678901234567890"];
    let le = sink(&f, &vals.map(Some));
    let out = read_col(&f, le.as_ref());
    for (i, v) in vals.iter().enumerate() {
        assert_eq!(
            out.value(i).to_vec(),
            canonical_of(&f, v),
            "row {i} ({v}) corrupted by the round trip"
        );
    }
}

#[test]
fn round_trip_scaled_values_u256() {
    let f = arb_field("v", 78, 18, Some(NativeIntKind::U256));
    for v in ["0", "1.5", "0.000000000000000001", "123456789.123456789"] {
        assert_round_trip(&f, v);
    }
}

#[test]
fn round_trip_scaled_values_i256() {
    let f = arb_field("v", 78, 18, Some(NativeIntKind::I256));
    for v in ["-1.5", "-0.000000000000000001", "-123456789.123456789"] {
        assert_round_trip(&f, v);
    }
}

#[test]
fn round_trip_scaled_value_keeps_the_decimal_point_position() {
    let f = arb_field("v", 78, 4, Some(NativeIntKind::I256));
    let le = sink(&f, &[Some("-123.4500")]);
    let out = read_col(&f, le.as_ref());
    let got = DecimalArbValue::from_canonical_bytes_at_scale(out.value(0), 4)
        .unwrap()
        .to_canonical_string();
    assert_eq!(got, "-123.4500", "scale must survive the ClickHouse hop");
}

#[test]
fn round_trip_trailing_zero_forms_collapse_to_one_encoding() {
    let f = arb_field("v", 78, 4, Some(NativeIntKind::U256));
    let a = round_trip_canonical(&f, "5");
    let b = round_trip_canonical(&f, "5.0");
    let c = round_trip_canonical(&f, "05.0000");
    assert_eq!(a, b, "numerically equal values must share one encoding");
    assert_eq!(b, c, "GROUP BY / join keys depend on this");
}

#[test]
fn round_trip_of_negative_zero_yields_positive_zero() {
    let f = arb_field("v", 78, 4, Some(NativeIntKind::I256));
    assert_eq!(
        round_trip_canonical(&f, "-0"),
        vec![0x00],
        "there is exactly one encoding of zero"
    );
}

// ---- reverse direction: LE bytes -> decimal_arb -> LE bytes ----

fn assert_le_identity(field: &FieldRef, le: [u8; 32]) {
    let canonical = read_canonical(field, le);
    let arr = LargeBinaryArray::from(vec![Some(&canonical[..])]);
    let out = decimal_arb_to_clickhouse_native(&arr, field)
        .unwrap_or_else(|e| panic!("re-emitting the read value must succeed: {e}"));
    let f = out
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("FixedSizeBinaryArray");
    assert_eq!(
        f.value(0),
        &le[..],
        "ClickHouse LE bytes must survive a decimal_arb round trip unchanged"
    );
}

#[test]
fn reverse_round_trip_u256_zero_and_one() {
    let f = u_field();
    assert_le_identity(&f, [0u8; 32]);
    assert_le_identity(&f, le_from_u128(1));
}

#[test]
fn reverse_round_trip_u256_all_ones() {
    assert_le_identity(&u_field(), [0xFFu8; 32]);
}

#[test]
fn reverse_round_trip_u256_two_pow_255() {
    assert_le_identity(&u_field(), le_u(TWO_POW_255));
}

#[test]
fn reverse_round_trip_u256_asymmetric_bytes() {
    let mut be = [0u8; 32];
    for (i, b) in be.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(7).wrapping_add(1);
    }
    be[0] = 0x13; // keep it non-zero so the magnitude is a full 32 bytes
    assert_le_identity(&u_field(), to_le(be));
}

#[test]
fn reverse_round_trip_i256_negative_one() {
    assert_le_identity(&i_field(), [0xFFu8; 32]);
}

#[test]
fn reverse_round_trip_i256_min() {
    assert_le_identity(&i_field(), le_i(I256_MIN));
}

#[test]
fn reverse_round_trip_i256_max() {
    assert_le_identity(&i_field(), le_u(I256_MAX));
}

#[test]
fn reverse_round_trip_i256_dense_range() {
    let f = i_field();
    for v in [
        -3i128,
        -2,
        -1,
        0,
        1,
        2,
        3,
        -1000,
        1000,
        i128::MIN,
        i128::MAX,
    ] {
        assert_le_identity(&f, le_from_i128(v));
    }
}

#[test]
fn reverse_round_trip_u256_dense_range() {
    let f = u_field();
    for v in [0u128, 1, 2, 255, 256, 1 << 64, u128::MAX] {
        assert_le_identity(&f, le_from_u128(v));
    }
}

#[test]
fn reverse_round_trip_scaled_field_is_still_byte_identity() {
    let f = arb_field("v", 78, 12, Some(NativeIntKind::U256));
    assert_le_identity(&f, le_from_u128(123_456_789_012_345));
}

// ===========================================================================
// 9. Cross-hint behaviour and sink/source contract coupling
// ===========================================================================

#[test]
fn sink_and_source_agree_on_the_le_layout_for_one() {
    let f = u_field();
    assert_eq!(
        sink_le(&f, "1"),
        le_from_u128(1),
        "the sink must emit exactly what the source expects to read"
    );
}

#[test]
fn sink_and_source_agree_on_the_le_layout_for_asymmetric_values() {
    let f = u_field();
    let v: u128 = 0x0123_4567_89AB_CDEF;
    assert_eq!(sink_le(&f, &v.to_string()), le_from_u128(v));
}

#[test]
fn sink_and_source_agree_on_the_le_layout_for_negatives() {
    let f = i_field();
    assert_eq!(sink_le(&f, "-1"), le_from_i128(-1));
    assert_eq!(sink_le(&f, "-256"), le_from_i128(-256));
}

#[test]
fn sink_and_source_agree_at_the_signed_minimum() {
    let f = i_field();
    assert_eq!(sink_le(&f, I256_MIN), le_i(I256_MIN));
}

#[test]
fn a_value_written_as_u256_reads_back_negative_under_an_i256_hint() {
    // The hint is load-bearing: the same 32 bytes mean different numbers.
    let u = u_field();
    let i = i_field();
    let le = sink_le(&u, TWO_POW_255);
    assert_eq!(read_str(&u, le), TWO_POW_255);
    assert_eq!(
        read_str(&i, le),
        I256_MIN,
        "reading u256 storage with an i256 hint silently flips the sign"
    );
}

#[test]
fn a_negative_value_read_as_u256_becomes_a_huge_positive() {
    let i = i_field();
    let u = u_field();
    let le = sink_le(&i, "-1");
    assert_eq!(read_str(&i, le), "-1");
    assert_eq!(read_str(&u, le), U256_MAX);
}

#[test]
fn values_read_from_u256_storage_are_always_reemittable_to_u256() {
    let f = u_field();
    for le in [[0u8; 32], [0xFFu8; 32], le_u(TWO_POW_255), le_from_u128(42)] {
        let canonical = read_canonical(&f, le);
        let arr = LargeBinaryArray::from(vec![Some(&canonical[..])]);
        assert!(
            decimal_arb_to_clickhouse_native(&arr, &f).is_ok(),
            "the read side must never produce a value the sink rejects"
        );
    }
}

#[test]
fn values_read_from_i256_storage_are_always_reemittable_to_i256() {
    let f = i_field();
    for le in [
        [0u8; 32],
        [0xFFu8; 32],
        le_i(I256_MIN),
        le_u(I256_MAX),
        le_from_i128(-77),
    ] {
        let canonical = read_canonical(&f, le);
        let arr = LargeBinaryArray::from(vec![Some(&canonical[..])]);
        assert!(
            decimal_arb_to_clickhouse_native(&arr, &f).is_ok(),
            "the read side must never produce a value the sink rejects"
        );
    }
}

#[test]
fn a_u256_value_above_the_signed_ceiling_is_rejected_by_an_i256_sink() {
    let u = u_field();
    let i = i_field();
    let canonical = read_canonical(&u, le_u(TWO_POW_255));
    let arr = LargeBinaryArray::from(vec![Some(&canonical[..])]);
    let err = decimal_arb_to_clickhouse_native(&arr, &i).unwrap_err();
    assert!(
        err.to_string().contains("Int256"),
        "2^255 must not be silently written into an Int256 column: {err}"
    );
}

#[test]
fn read_output_never_contains_an_invalid_sign_byte() {
    let f = i_field();
    for le in [
        [0u8; 32],
        [0xFFu8; 32],
        le_i(I256_MIN),
        le_u(I256_MAX),
        le_from_i128(-1),
        le_from_i128(1),
    ] {
        let c = read_canonical(&f, le);
        assert!(
            c[0] == 0x00 || c[0] == 0xFF,
            "sign byte must be 0x00 or 0xFF, got 0x{:02X}",
            c[0]
        );
    }
}

#[test]
fn read_output_is_never_empty_for_a_non_null_row() {
    let f = i_field();
    for le in [[0u8; 32], [0xFFu8; 32], le_from_i128(-1)] {
        assert!(
            !read_canonical(&f, le).is_empty(),
            "canonical bytes always carry at least the sign byte"
        );
    }
}

#[test]
fn read_output_magnitude_never_exceeds_32_bytes() {
    let f = i_field();
    for le in [[0xFFu8; 32], le_i(I256_MIN), le_u(I256_MAX)] {
        let c = read_canonical(&f, le);
        assert!(
            c.len() <= 33,
            "sign byte plus at most 32 magnitude bytes, got {}",
            c.len()
        );
    }
}

#[test]
fn u256_read_output_magnitude_never_exceeds_32_bytes() {
    let c = read_canonical(&u_field(), [0xFFu8; 32]);
    assert_eq!(c.len(), 33);
}

#[test]
#[ignore = "FINDING: clickhouse_native_to_decimal_arb ignores the field's declared precision, emitting decimal_arb values that violate the column contract (no check_fits on the read path)"]
fn read_respects_the_declared_precision_of_the_target_field() {
    // decimal_arb(10, 0) can hold at most 10 integer digits; a 78-digit value
    // arriving from ClickHouse storage should be surfaced as an error, not
    // silently admitted into the pipeline.
    let f = arb_field("balance", 10, 0, Some(NativeIntKind::U256));
    let msg = err_of(&f, &fsb_one([0xFFu8; 32]));
    assert!(
        msg.contains("balance"),
        "out-of-precision read must name the column: {msg}"
    );
}

#[test]
fn read_of_a_non_nullable_field_still_reports_nulls_faithfully() {
    // A non-nullable target field does not license dropping the null mask.
    let f = DecimalArbType::field("v", 78, 0, false).unwrap();
    let f: FieldRef =
        Arc::new(DecimalArbType::with_native_int_kind(f, NativeIntKind::U256).unwrap());
    let arr = fsb(&[None, Some(le_from_u128(1))]);
    let out = read_col(&f, &arr);
    assert!(
        out.is_null(0),
        "nulls must not be silently rewritten to zero"
    );
}

#[test]
fn read_does_not_leak_raw_bytes_in_error_messages() {
    let f = arb_field("secret_balance", 78, 0, None);
    let msg = err_of(&f, &fsb_one(le_from_u128(0xDEAD_BEEF)));
    assert!(
        !msg.contains("3735928559") && !msg.to_lowercase().contains("deadbeef"),
        "error messages must not echo column data: {msg}"
    );
}

#[test]
fn repeated_reads_of_the_same_input_are_deterministic() {
    let f = i_field();
    let le = le_i(I256_MIN);
    let a = read_canonical(&f, le);
    let b = read_canonical(&f, le);
    assert_eq!(a, b, "conversion must be pure");
}

#[test]
fn reading_does_not_mutate_the_input_array() {
    let f = u_field();
    let arr = fsb_one(le_from_u128(1234));
    let before = arr.value(0).to_vec();
    let _ = read_col(&f, &arr);
    assert_eq!(arr.value(0).to_vec(), before, "input must not be mutated");
}

#[test]
fn every_boundary_value_survives_a_double_round_trip() {
    let f = i_field();
    for v in [
        "0",
        "1",
        "-1",
        I256_MAX,
        I256_MIN,
        "-170141183460469231731687303715884105728",
    ] {
        let once = round_trip_canonical(&f, v);
        let arr = LargeBinaryArray::from(vec![Some(&once[..])]);
        let le = decimal_arb_to_clickhouse_native(&arr, &f).unwrap();
        let twice = read_col(&f, le.as_ref()).value(0).to_vec();
        assert_eq!(once, twice, "conversion must be idempotent for {v}");
    }
}

#[test]
fn u256_every_boundary_value_survives_a_double_round_trip() {
    let f = u_field();
    for v in ["0", "1", U256_MAX, TWO_POW_255, TEN_POW_77] {
        let once = round_trip_canonical(&f, v);
        let arr = LargeBinaryArray::from(vec![Some(&once[..])]);
        let le = decimal_arb_to_clickhouse_native(&arr, &f).unwrap();
        let twice = read_col(&f, le.as_ref()).value(0).to_vec();
        assert_eq!(once, twice, "conversion must be idempotent for {v}");
    }
}
