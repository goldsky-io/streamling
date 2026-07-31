//! Adversarial pass, agent 06 — **silent truncation / wrap / round**.
//!
//! Focus: every code path in `types::decimal_arb` that could quietly change a
//! value instead of returning `Err`. Specifically:
//!
//! * `DecimalArbValue::to_canonical_bytes_at_scale` — applies a *defensive*
//!   `with_scale_round(HalfEven)`. Prove exactly when that can change a value.
//! * `DecimalArbValue::check_fits` — at and one step past the precision and
//!   scale limits, and at `MAX_PRECISION`.
//! * `DecimalArbArray::{to_decimal128, to_decimal256}` — range and scale
//!   narrowing; every lossy narrowing must error, never wrap.
//! * `DecimalArbArray::{from_decimal128, from_decimal256}` — at `i128::MIN`,
//!   `i128::MAX`, `i256::MIN`, `i256::MAX`.
//!
//! Tests that pin *documented but silent* behaviour say so in their name and
//! doc comment, so a future change that makes them error is visible.

use arrow::array::{Array, Decimal128Array, Decimal256Array, LargeBinaryArray, StringArray};
use arrow::datatypes::i256 as ArrowI256;
use arrow::datatypes::{DataType, Field};
use num_bigint::BigInt;
use std::str::FromStr;
use streamling_common::types::decimal_arb::{
    DecimalArbArray, DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, MAX_PRECISION,
    decimal_arb_to_sort_key,
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// `DecimalArbArray` has no `Debug`, so `unwrap_err()` is unavailable on
/// `Result<DecimalArbArray>`. This extracts the error (or fails loudly).
fn expect_err<T>(
    r: streamling_common::error::Result<T>,
    what: &str,
) -> streamling_common::error::StreamlingError {
    match r {
        Ok(_) => panic!("{what}: expected an error, got Ok"),
        Err(e) => e,
    }
}

fn v(s: &str) -> DecimalArbValue {
    DecimalArbValue::from_str(s).unwrap_or_else(|e| panic!("parse {s:?}: {e}"))
}

fn build(col: &str, p: u32, s: u32, vals: &[Option<&str>]) -> DecimalArbArray {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), col, p, s).unwrap();
    for x in vals {
        match x {
            Some(t) => b
                .append_str(t)
                .unwrap_or_else(|e| panic!("append {t:?} at ({p},{s}): {e}")),
            None => b.append_null(),
        }
    }
    b.finish()
}

fn nines(n: usize) -> String {
    "9".repeat(n)
}

/// `10^n` as a decimal string (n+1 digits).
fn pow10(n: usize) -> String {
    let mut s = String::with_capacity(n + 1);
    s.push('1');
    s.push_str(&"0".repeat(n));
    s
}

/// Build a decimal string with `int_digits` integer nines and `frac_digits`
/// fractional nines. `int_digits == 0` yields a leading `0`.
fn shaped(int_digits: usize, frac_digits: usize) -> String {
    let ip = if int_digits == 0 {
        "0".to_string()
    } else {
        nines(int_digits)
    };
    if frac_digits == 0 {
        ip
    } else {
        format!("{}.{}", ip, nines(frac_digits))
    }
}

fn i256_max() -> ArrowI256 {
    let mut b = [0xFFu8; 32];
    b[0] = 0x7F;
    ArrowI256::from_be_bytes(b)
}

fn i256_min() -> ArrowI256 {
    let mut b = [0x00u8; 32];
    b[0] = 0x80;
    ArrowI256::from_be_bytes(b)
}

const I128_MAX_STR: &str = "170141183460469231731687303715884105727";
const I128_MIN_STR: &str = "-170141183460469231731687303715884105728";
const I256_MAX_STR: &str =
    "57896044618658097711785492504343953926634992332820282019728792003956564819967";
const I256_MIN_STR: &str =
    "-57896044618658097711785492504343953926634992332820282019728792003956564819968";

/// Adopt a raw `LargeBinaryArray` as a `decimal_arb` array with the given
/// declared `(precision, scale)`.
fn adopt(raw: LargeBinaryArray, p: u32, s: u32) -> DecimalArbArray {
    let field = DecimalArbType::field("adopted", p, s, true).unwrap();
    DecimalArbArray::try_from_array_and_field(raw, &field).unwrap()
}

// ===========================================================================
// A. to_canonical_bytes_at_scale — exactness vs. the defensive HalfEven round
// ===========================================================================

#[test]
fn zero_encodes_as_a_lone_sign_byte_at_every_scale() {
    for scale in 0u32..=64 {
        let bytes = v("0").to_canonical_bytes_at_scale(scale);
        assert_eq!(
            bytes,
            vec![0x00u8],
            "zero must encode as the single byte 0x00 at scale {scale}, got {bytes:?}"
        );
    }
}

#[test]
fn negative_zero_encodes_identically_to_positive_zero() {
    for scale in [0u32, 1, 7, 18, 40] {
        assert_eq!(
            v("-0").to_canonical_bytes_at_scale(scale),
            v("0").to_canonical_bytes_at_scale(scale),
            "-0 and 0 must produce byte-identical payloads (join/GROUP BY key) at scale {scale}"
        );
        assert_eq!(
            v("-0.000").to_canonical_bytes_at_scale(scale),
            v("0").to_canonical_bytes_at_scale(scale),
            "-0.000 must encode as canonical zero at scale {scale}"
        );
    }
}

#[test]
fn encode_decode_round_trip_is_total_over_a_value_scale_matrix() {
    let values = [
        "0",
        "-0",
        "1",
        "-1",
        "255",
        "256",
        "-256",
        "65535",
        "65536",
        "0.5",
        "-0.5",
        "1.25",
        "-1.25",
        "0.0001",
        "-0.0001",
        "123456789012345678901234567890",
        "-123456789012345678901234567890",
        "0.000000000000000000001",
    ];
    for s in values {
        for scale in 0u32..=24 {
            let val = v(s);
            let bytes = val.to_canonical_bytes_at_scale(scale);
            let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, scale)
                .unwrap_or_else(|e| {
                    panic!("encode/decode must be total: {s} at scale {scale} failed: {e}")
                });
            // Exactness is only promised when the target scale can hold every
            // significant fractional digit.
            if val.fractional_digit_count() <= scale as u64 {
                assert_eq!(
                    decoded, val,
                    "round-trip must be exact for {s} at scale {scale} \
                     (significant fractional digits fit)"
                );
            }
        }
    }
}

#[test]
fn encode_is_exact_whenever_target_scale_covers_significant_fraction() {
    for s in ["1.23", "-1.23", "0.000001", "1000000", "-7.5"] {
        let val = v(s);
        let need = val.fractional_digit_count() as u32;
        for scale in need..need + 8 {
            let bytes = val.to_canonical_bytes_at_scale(scale);
            let back = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, scale).unwrap();
            assert_eq!(back, val, "{s} must survive encoding at scale {scale}");
        }
    }
}

/// DOCUMENTED-SILENT: the encoder applies `with_scale_round(HalfEven)`, so a
/// direct call with a target scale below the value's significant fraction
/// silently returns a *different* number. `check_fits` is the only guard.
#[test]
fn encode_silently_rounds_when_target_scale_is_below_significant_fraction() {
    let val = v("1.2345");
    assert!(
        val.check_fits(10, 2, "col").is_err(),
        "check_fits must reject 1.2345 against scale 2 — it is the only guard \
         against the encoder's silent round"
    );
    let bytes = val.to_canonical_bytes_at_scale(2);
    let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 2).unwrap();
    assert_eq!(
        decoded,
        v("1.23"),
        "encoder silently rounds to the target scale"
    );
    assert_ne!(
        decoded, val,
        "the silently rounded payload is NOT the original value"
    );
}

#[test]
fn encode_half_even_rounds_positive_ties_to_even() {
    let cases = [
        ("0.5", "0"),
        ("1.5", "2"),
        ("2.5", "2"),
        ("3.5", "4"),
        ("4.5", "4"),
    ];
    for (input, expect) in cases {
        let bytes = v(input).to_canonical_bytes_at_scale(0);
        let got = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 0).unwrap();
        assert_eq!(
            got,
            v(expect),
            "half-even at scale 0: {input} must encode as {expect}"
        );
    }
}

#[test]
fn encode_half_even_rounds_negative_ties_to_even() {
    let cases = [
        ("-0.5", "0"),
        ("-1.5", "-2"),
        ("-2.5", "-2"),
        ("-3.5", "-4"),
        ("-4.5", "-4"),
    ];
    for (input, expect) in cases {
        let bytes = v(input).to_canonical_bytes_at_scale(0);
        let got = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 0).unwrap();
        assert_eq!(
            got,
            v(expect),
            "half-even at scale 0: {input} must encode as {expect}"
        );
    }
}

#[test]
fn encode_half_even_rounds_ties_to_even_at_scale_two() {
    let cases = [("0.045", "0.04"), ("0.055", "0.06"), ("0.065", "0.06")];
    for (input, expect) in cases {
        let bytes = v(input).to_canonical_bytes_at_scale(2);
        let got = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 2).unwrap();
        assert_eq!(got, v(expect), "half-even at scale 2: {input} -> {expect}");
    }
}

/// DOCUMENTED-SILENT: a sub-ULP value collapses to zero rather than erroring.
#[test]
fn encode_silently_collapses_sub_ulp_positive_to_zero() {
    let val = v("0.4");
    let bytes = val.to_canonical_bytes_at_scale(0);
    assert_eq!(bytes, vec![0x00u8], "0.4 at scale 0 encodes as zero");
    assert!(
        val.check_fits(10, 0, "col").is_err(),
        "check_fits must be the thing that rejects 0.4 against scale 0"
    );
}

/// Negative sub-ULP values must collapse to the *positive-zero* encoding.
/// If the encoder ever emitted `[0xFF]` (negative zero) the payload would be
/// undecodable, so this is a totality guard, not just a cosmetic one.
#[test]
fn encode_of_negative_sub_ulp_yields_decodable_positive_zero() {
    for s in ["-0.4", "-0.5", "-0.0001", "-0.49999999999"] {
        let bytes = v(s).to_canonical_bytes_at_scale(0);
        assert_eq!(
            bytes,
            vec![0x00u8],
            "{s} at scale 0 must encode as canonical positive zero, got {bytes:?}"
        );
        assert!(
            DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 0).is_ok(),
            "{s} must not produce an undecodable negative-zero payload"
        );
    }
}

#[test]
fn encoder_never_emits_an_undecodable_negative_zero_payload() {
    let values = [
        "-0",
        "-0.0",
        "-0.4",
        "-0.5",
        "-0.05",
        "-0.004",
        "-0.00000000001",
        "-1",
        "-1000000",
    ];
    for s in values {
        for scale in 0u32..=12 {
            let bytes = v(s).to_canonical_bytes_at_scale(scale);
            assert!(
                !(bytes.len() == 1 && bytes[0] == 0xFF),
                "encoder produced the rejected negative-zero payload for {s} at scale {scale}"
            );
            assert!(
                DecimalArbValue::from_canonical_bytes_at_scale(&bytes, scale).is_ok(),
                "every emitted payload must decode: {s} at scale {scale}"
            );
        }
    }
}

#[test]
fn numerically_equal_literals_produce_identical_bytes() {
    let forms = ["5", "5.0", "5.00", "05", "+5", "5.000000"];
    let reference = v("5").to_canonical_bytes_at_scale(4);
    for f in forms {
        assert_eq!(
            v(f).to_canonical_bytes_at_scale(4),
            reference,
            "{f} must encode byte-identically to 5 at scale 4 (GROUP BY / join key)"
        );
    }
}

#[test]
fn exponent_notation_and_plain_form_produce_identical_bytes() {
    let pairs = [("1E+2", "100"), ("1e2", "100"), ("1.5E3", "1500")];
    for (exp_form, plain) in pairs {
        assert_eq!(
            v(exp_form).to_canonical_bytes_at_scale(0),
            v(plain).to_canonical_bytes_at_scale(0),
            "{exp_form} and {plain} are the same number and must encode identically"
        );
    }
}

#[test]
fn encoded_magnitude_never_carries_a_leading_zero_byte() {
    let values = [
        "1",
        "255",
        "256",
        "65536",
        "16777216",
        "4294967296",
        "18446744073709551616",
        "-256",
        "-4294967296",
    ];
    for s in values {
        for scale in [0u32, 1, 5] {
            let bytes = v(s).to_canonical_bytes_at_scale(scale);
            assert!(
                bytes.len() >= 2,
                "{s} must have a magnitude at scale {scale}"
            );
            assert_ne!(
                bytes[1], 0x00,
                "magnitude must be minimal (no leading 0x00) for {s} at scale {scale}: {bytes:?}"
            );
        }
    }
}

#[test]
fn encoding_of_256_uses_exactly_two_magnitude_bytes() {
    assert_eq!(
        v("256").to_canonical_bytes_at_scale(0),
        vec![0x00, 0x01, 0x00]
    );
    assert_eq!(
        v("-256").to_canonical_bytes_at_scale(0),
        vec![0xFF, 0x01, 0x00]
    );
    assert_eq!(v("255").to_canonical_bytes_at_scale(0), vec![0x00, 0xFF]);
}

#[test]
fn padding_to_a_larger_scale_preserves_the_value() {
    let val = v("1.0");
    for scale in 1u32..=30 {
        let bytes = val.to_canonical_bytes_at_scale(scale);
        let back = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, scale).unwrap();
        assert_eq!(
            back, val,
            "padding to scale {scale} must not change the value"
        );
    }
}

/// DOCUMENTED HAZARD: the scale is not in the bytes. Decoding at a scale other
/// than the encoding scale silently shifts the decimal point.
#[test]
fn decoding_at_a_mismatched_scale_silently_shifts_the_decimal_point() {
    let val = v("1.23");
    let bytes = val.to_canonical_bytes_at_scale(2);
    let wrong = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 4).unwrap();
    assert_eq!(wrong, v("0.0123"), "decoding at scale 4 shifts by 10^2");
    assert_ne!(wrong, val, "mismatched scale yields a different number");
}

#[test]
fn sign_byte_matches_the_sign_of_the_rounded_value() {
    for s in ["-1", "-0.5", "-1000000", "-0.000000001"] {
        for scale in [0u32, 9, 18] {
            let val = v(s);
            let bytes = val.to_canonical_bytes_at_scale(scale);
            let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, scale).unwrap();
            let expect_negative = decoded != v("0");
            assert_eq!(
                bytes[0] == 0xFF,
                expect_negative,
                "sign byte must describe the *encoded* value for {s} at scale {scale}"
            );
        }
    }
}

#[test]
fn trailing_fractional_zeros_do_not_change_the_encoding() {
    for scale in [0u32, 3, 9] {
        assert_eq!(
            v("1.2300").to_canonical_bytes_at_scale(scale),
            v("1.23").to_canonical_bytes_at_scale(scale),
            "trailing fractional zeros must be non-significant at scale {scale}"
        );
    }
}

// ===========================================================================
// B. from_canonical_bytes_at_scale — decoder strictness
// ===========================================================================

#[test]
fn decoder_rejects_empty_payload() {
    let err = DecimalArbValue::from_canonical_bytes_at_scale(&[], 0).unwrap_err();
    assert!(
        err.to_string().contains("empty"),
        "empty payload must be rejected: {err}"
    );
}

#[test]
fn decoder_rejects_negative_zero_payload() {
    let err = DecimalArbValue::from_canonical_bytes_at_scale(&[0xFF], 0).unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("negative zero"),
        "0xFF with no magnitude must be rejected: {err}"
    );
}

#[test]
fn decoder_rejects_every_sign_byte_other_than_00_and_ff() {
    for b in 0u16..=255 {
        let b = b as u8;
        let payload = [b, 0x01];
        let res = DecimalArbValue::from_canonical_bytes_at_scale(&payload, 0);
        if b == 0x00 || b == 0xFF {
            assert!(res.is_ok(), "sign byte 0x{b:02x} must be accepted");
        } else {
            assert!(
                res.is_err(),
                "sign byte 0x{b:02x} must be rejected, not silently reinterpreted"
            );
        }
    }
}

#[test]
fn decoder_rejects_sign_byte_0x01() {
    assert!(DecimalArbValue::from_canonical_bytes_at_scale(&[0x01, 0x05], 0).is_err());
}

#[test]
fn decoder_rejects_sign_byte_0x7f() {
    assert!(DecimalArbValue::from_canonical_bytes_at_scale(&[0x7F, 0x05], 0).is_err());
}

#[test]
fn decoder_rejects_sign_byte_0xfe() {
    assert!(DecimalArbValue::from_canonical_bytes_at_scale(&[0xFE, 0x05], 0).is_err());
}

#[test]
fn decoder_rejects_sign_byte_0x80() {
    assert!(DecimalArbValue::from_canonical_bytes_at_scale(&[0x80, 0x05], 0).is_err());
}

/// The decoder is lenient about non-minimal magnitudes; re-encoding must
/// still produce the minimal form, so a foreign non-minimal payload and a
/// locally produced one are numerically equal but NOT byte-equal.
#[test]
fn decoder_accepts_non_minimal_magnitude_but_reencodes_minimally() {
    let padded = [0x00u8, 0x00, 0x00, 0x01];
    let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&padded, 0).unwrap();
    assert_eq!(
        decoded,
        v("1"),
        "leading zero bytes must not change the value"
    );
    assert_eq!(
        decoded.to_canonical_bytes_at_scale(0),
        vec![0x00, 0x01],
        "re-encoding must be minimal"
    );
    assert_ne!(
        decoded.to_canonical_bytes_at_scale(0).as_slice(),
        &padded[..],
        "non-minimal payloads are numerically equal but not byte-equal — \
         byte-keyed grouping would split them"
    );
}

#[test]
fn decoder_handles_a_long_magnitude_exactly() {
    let val = v(&nines(200));
    let bytes = val.to_canonical_bytes_at_scale(0);
    let back = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 0).unwrap();
    assert_eq!(back, val, "200-digit magnitude must decode exactly");
    assert_eq!(back.to_canonical_string(), nines(200));
}

#[test]
fn decoder_places_the_point_correctly_at_large_scales() {
    // BigInt 1 decoded at scale N is 10^-N.
    for scale in [1u32, 5, 18, 38, 64, 100] {
        let decoded = DecimalArbValue::from_canonical_bytes_at_scale(&[0x00, 0x01], scale).unwrap();
        let expect = format!("0.{}1", "0".repeat(scale as usize - 1));
        assert_eq!(
            decoded.to_canonical_string(),
            expect,
            "decoding BigInt(1) at scale {scale} must be 10^-{scale}"
        );
    }
}

#[test]
fn decoder_never_panics_on_arbitrary_two_byte_payloads() {
    for a in 0u16..=255 {
        for b in [0x00u8, 0x01, 0x7F, 0x80, 0xFF] {
            let _ = DecimalArbValue::from_canonical_bytes_at_scale(&[a as u8, b], 7);
        }
    }
}

// ===========================================================================
// C. check_fits — precision / scale boundaries
// ===========================================================================

#[test]
fn check_fits_rejects_precision_zero() {
    assert!(v("1").check_fits(0, 0, "c").is_err());
}

#[test]
fn check_fits_rejects_precision_above_max() {
    assert!(v("1").check_fits(MAX_PRECISION + 1, 0, "c").is_err());
    assert!(v("1").check_fits(u32::MAX, 0, "c").is_err());
}

#[test]
fn check_fits_accepts_precision_exactly_at_max() {
    assert!(
        v("1").check_fits(MAX_PRECISION, 0, "c").is_ok(),
        "MAX_PRECISION itself must be accepted"
    );
    assert!(
        v("0.1")
            .check_fits(MAX_PRECISION, MAX_PRECISION, "c")
            .is_ok(),
        "scale == precision == MAX_PRECISION must be accepted"
    );
}

#[test]
fn check_fits_rejects_scale_greater_than_precision() {
    assert!(v("1").check_fits(10, 11, "c").is_err());
    assert!(v("0").check_fits(1, 2, "c").is_err());
}

#[test]
fn check_fits_accepts_exactly_the_integer_digit_limit() {
    for p in 1u32..=12 {
        for s in 0..=p {
            let int_digits = (p - s) as usize;
            let val = v(&shaped(int_digits, s as usize));
            assert!(
                val.check_fits(p, s, "c").is_ok(),
                "({p},{s}) must accept {val} — exactly p-s integer and s fractional digits"
            );
        }
    }
}

#[test]
fn check_fits_rejects_one_past_the_integer_digit_limit() {
    for p in 1u32..=12 {
        for s in 0..=p {
            let int_digits = (p - s) as usize + 1;
            let val = v(&shaped(int_digits, s as usize));
            assert!(
                val.check_fits(p, s, "c").is_err(),
                "({p},{s}) must reject {val} — one integer digit too many"
            );
        }
    }
}

#[test]
fn check_fits_rejects_one_past_the_scale_limit() {
    for p in 2u32..=12 {
        for s in 0..p {
            let int_digits = (p - s) as usize;
            let val = v(&shaped(int_digits, s as usize + 1));
            assert!(
                val.check_fits(p, s, "c").is_err(),
                "({p},{s}) must reject {val} — one significant fractional digit too many"
            );
        }
    }
}

#[test]
fn check_fits_accepts_zero_for_every_valid_precision_scale() {
    for p in 1u32..=20 {
        for s in 0..=p {
            assert!(
                v("0").check_fits(p, s, "c").is_ok(),
                "zero must fit ({p},{s})"
            );
            assert!(
                v("-0").check_fits(p, s, "c").is_ok(),
                "negative zero must fit ({p},{s})"
            );
        }
    }
}

#[test]
fn check_fits_ignores_non_significant_trailing_fractional_zeros() {
    assert!(v("1.000").check_fits(1, 0, "c").is_ok());
    assert!(v("1.2300").check_fits(3, 2, "c").is_ok());
    assert!(v("-45.00000").check_fits(2, 0, "c").is_ok());
}

#[test]
fn check_fits_with_precision_equal_to_scale_admits_only_sub_unit_values() {
    assert!(v("0.99").check_fits(2, 2, "c").is_ok());
    assert!(v("-0.99").check_fits(2, 2, "c").is_ok());
    assert!(
        v("1").check_fits(2, 2, "c").is_err(),
        "1 has one integer digit but p-s == 0"
    );
    assert!(v("1.00").check_fits(2, 2, "c").is_err());
}

#[test]
fn check_fits_uses_magnitude_only_for_negatives() {
    assert!(v("-1234").check_fits(4, 0, "c").is_ok());
    assert!(v("-1234").check_fits(3, 0, "c").is_err());
    assert!(v("-0.001").check_fits(3, 3, "c").is_ok());
    assert!(v("-0.001").check_fits(2, 2, "c").is_err());
}

#[test]
fn check_fits_rejects_precisely_the_values_the_encoder_would_round() {
    // For every (p, s), a value with s+1 significant fractional digits is
    // exactly the class the encoder would silently round; check_fits must
    // reject all of them.
    for s in 0u32..=8 {
        let p = s + 3;
        let val = v(&format!("1.{}", nines(s as usize + 1)));
        assert!(
            val.check_fits(p, s, "c").is_err(),
            "({p},{s}) must reject {val} before the encoder rounds it"
        );
        let rounded_bytes = val.to_canonical_bytes_at_scale(s);
        let rounded = DecimalArbValue::from_canonical_bytes_at_scale(&rounded_bytes, s).unwrap();
        assert_ne!(
            rounded, val,
            "the encoder really would have changed the value at ({p},{s})"
        );
    }
}

#[test]
fn check_fits_error_names_the_column_and_the_value() {
    let err = v("12345").check_fits(3, 0, "my_col").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("my_col"), "error must name the column: {msg}");
    assert!(msg.contains("12345"), "error must name the value: {msg}");

    let err = v("1.2345").check_fits(10, 2, "frac_col").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("frac_col"),
        "error must name the column: {msg}"
    );
    assert!(msg.contains("scale"), "error must mention scale: {msg}");
}

#[test]
fn integer_and_fractional_digit_counts_agree_with_check_fits() {
    let cases = [
        ("0", 0u64, 0u64),
        ("1", 1, 0),
        ("100", 3, 0),
        ("1000", 4, 0),
        ("-1000", 4, 0),
        ("0.5", 0, 1),
        ("0.05", 0, 2),
        ("1.23", 1, 2),
        ("1.000", 1, 0),
        ("100.5", 3, 1),
    ];
    for (s, int_expect, frac_expect) in cases {
        let val = v(s);
        assert_eq!(
            val.integer_digit_count(),
            int_expect,
            "integer_digit_count({s})"
        );
        assert_eq!(
            val.fractional_digit_count(),
            frac_expect,
            "fractional_digit_count({s})"
        );
        let p = (int_expect + frac_expect).max(1) as u32;
        let sc = frac_expect as u32;
        assert!(
            val.check_fits(p, sc, "c").is_ok(),
            "{s} must fit its own minimal ({p},{sc})"
        );
    }
}

// ===========================================================================
// D. builder integrity — never store a value different from the one appended
// ===========================================================================

#[test]
fn builder_stores_exactly_the_value_appended_across_a_grid() {
    let values = [
        "0", "1", "-1", "0.5", "-0.5", "12.34", "-12.34", "999", "-999",
    ];
    for p in 1u32..=10 {
        for s in 0..=p {
            for src in values {
                let val = v(src);
                let mut b = DecimalArbArrayBuilder::with_capacity(1, "c", p, s).unwrap();
                match b.append_value(&val) {
                    Err(_) => {
                        assert!(
                            val.check_fits(p, s, "c").is_err(),
                            "builder rejected {src} at ({p},{s}) but check_fits accepts it"
                        );
                    }
                    Ok(()) => {
                        let arr = b.finish();
                        assert_eq!(
                            arr.value(0).unwrap(),
                            Some(val.clone()),
                            "builder must store {src} unchanged at ({p},{s})"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn builder_rejects_excess_fraction_rather_than_rounding_it_away() {
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "amount", 10, 2).unwrap();
    let err = b.append_str("1.239").unwrap_err();
    assert!(
        err.to_string().contains("amount"),
        "rejection must name the column: {err}"
    );
    let arr = b.finish();
    assert_eq!(arr.len(), 0, "a rejected append must not produce a row");
}

#[test]
fn builder_rejects_excess_integer_digits() {
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "amount", 3, 0).unwrap();
    assert!(b.append_str("1000").is_err());
    assert_eq!(b.finish().len(), 0);
}

#[test]
fn failed_appends_leave_the_array_length_and_ordering_intact() {
    let mut b = DecimalArbArrayBuilder::with_capacity(4, "c", 4, 0).unwrap();
    b.append_str("1").unwrap();
    assert!(
        b.append_str("99999").is_err(),
        "must reject 5 integer digits"
    );
    b.append_null();
    assert!(b.append_str("1.5").is_err(), "must reject fractional digit");
    b.append_str("2").unwrap();
    let arr = b.finish();
    assert_eq!(arr.len(), 3, "only successful appends may add rows");
    assert_eq!(arr.value(0).unwrap(), Some(v("1")));
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2).unwrap(), Some(v("2")));
}

#[test]
fn builder_with_capacity_rejects_invalid_precision_scale() {
    assert!(DecimalArbArrayBuilder::with_capacity(1, "c", 0, 0).is_err());
    assert!(DecimalArbArrayBuilder::with_capacity(1, "c", MAX_PRECISION + 1, 0).is_err());
    assert!(DecimalArbArrayBuilder::with_capacity(1, "c", 5, 6).is_err());
}

#[test]
fn builder_append_str_rejects_garbage_without_appending() {
    let mut b = DecimalArbArrayBuilder::with_capacity(2, "c", 10, 2).unwrap();
    for bad in [
        "", " ", "abc", "1.2.3", "--1", "1,000", "0x10", "NaN", "inf",
    ] {
        assert!(
            b.append_str(bad).is_err(),
            "{bad:?} must not parse as a decimal_arb value"
        );
    }
    assert_eq!(b.finish().len(), 0);
}

#[test]
fn builder_at_max_precision_accepts_and_round_trips_a_small_value() {
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "c", MAX_PRECISION, 0).unwrap();
    b.append_str("-12345").unwrap();
    let arr = b.finish();
    assert_eq!(arr.precision(), MAX_PRECISION);
    assert_eq!(arr.value(0).unwrap(), Some(v("-12345")));
}

#[test]
fn empty_builder_finishes_to_an_empty_array() {
    let arr = build("c", 10, 2, &[]);
    assert_eq!(arr.len(), 0);
    assert!(arr.is_empty());
    assert!(arr.to_string_array().unwrap().is_empty());
    assert_eq!(arr.to_decimal128(10, 2, "c").unwrap().len(), 0);
    assert_eq!(arr.to_decimal256(10, 2, "c").unwrap().len(), 0);
}

// ===========================================================================
// E. to_decimal128 — range and precision narrowing
// ===========================================================================

#[test]
fn to_decimal128_rejects_precision_zero() {
    let arr = build("c", 40, 0, &[Some("1")]);
    let err = arr.to_decimal128(0, 0, "c").unwrap_err();
    assert!(err.to_string().contains("1..=38"), "{err}");
}

#[test]
fn to_decimal128_rejects_precision_above_38() {
    let arr = build("c", 40, 0, &[Some("1")]);
    for p in [39u8, 40, 76, 255] {
        assert!(
            arr.to_decimal128(p, 0, "c").is_err(),
            "Decimal128 precision {p} must be rejected"
        );
    }
}

#[test]
fn to_decimal128_accepts_precision_exactly_38() {
    let arr = build("c", 40, 0, &[Some("1")]);
    let out = arr.to_decimal128(38, 0, "c").unwrap();
    assert_eq!(out.value(0), 1_i128);
}

#[test]
fn to_decimal128_accepts_the_largest_38_digit_value() {
    let s = nines(38);
    let arr = build("c", 100, 0, &[Some(&s)]);
    let out = arr.to_decimal128(38, 0, "c").unwrap();
    assert_eq!(
        out.value(0),
        99_999_999_999_999_999_999_999_999_999_999_999_999_i128,
        "10^38 - 1 must narrow exactly"
    );
}

#[test]
fn to_decimal128_rejects_ten_to_the_38() {
    let s = pow10(38);
    let arr = build("c", 100, 0, &[Some(&s)]);
    let err = arr.to_decimal128(38, 0, "c").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("Decimal128"), "{msg}");
    assert!(msg.contains("'c'"), "error must name the column: {msg}");
}

#[test]
fn to_decimal128_accepts_the_most_negative_38_digit_value() {
    let s = format!("-{}", nines(38));
    let arr = build("c", 100, 0, &[Some(&s)]);
    let out = arr.to_decimal128(38, 0, "c").unwrap();
    assert_eq!(
        out.value(0),
        -99_999_999_999_999_999_999_999_999_999_999_999_999_i128
    );
}

#[test]
fn to_decimal128_rejects_negative_ten_to_the_38() {
    let s = format!("-{}", pow10(38));
    let arr = build("c", 100, 0, &[Some(&s)]);
    assert!(
        arr.to_decimal128(38, 0, "c").is_err(),
        "-10^38 must not wrap into Decimal128(38,0)"
    );
}

#[test]
fn to_decimal128_rejects_i128_max_instead_of_wrapping() {
    let arr = build("c", 100, 0, &[Some(I128_MAX_STR)]);
    let err = arr.to_decimal128(38, 0, "c").unwrap_err();
    assert!(
        err.to_string().contains("Decimal128"),
        "i128::MAX exceeds Decimal128(38,0) and must error: {err}"
    );
}

#[test]
fn to_decimal128_rejects_i128_min_instead_of_wrapping() {
    let arr = build("c", 100, 0, &[Some(I128_MIN_STR)]);
    assert!(
        arr.to_decimal128(38, 0, "c").is_err(),
        "i128::MIN exceeds Decimal128(38,0) and must error"
    );
}

#[test]
fn to_decimal128_rejects_values_beyond_the_i128_width() {
    let s = pow10(40); // 10^40 needs more than 16 signed bytes
    let arr = build("c", 100, 0, &[Some(&s)]);
    let err = arr.to_decimal128(38, 0, "c").unwrap_err();
    assert!(
        err.to_string().contains("Decimal128"),
        "10^40 must error, not wrap: {err}"
    );
}

#[test]
fn to_decimal128_errors_when_scaling_up_overflows_precision() {
    let arr = build("c", 100, 0, &[Some("1")]);
    // 1 at scale 38 is 10^38, which does not fit precision 38.
    assert!(
        arr.to_decimal128(38, 38, "c").is_err(),
        "1 cannot be represented in Decimal128(38,38)"
    );
    // ... but it does fit at scale 37.
    let out = arr.to_decimal128(38, 37, "c").unwrap();
    assert_eq!(out.value(0), 10_i128.pow(37));
}

#[test]
fn to_decimal128_errors_when_rounding_pushes_the_value_past_precision() {
    let arr = build("c", 10, 2, &[Some("9.99")]);
    assert!(
        arr.to_decimal128(2, 1, "c").is_err(),
        "9.99 rounds to 10.0 which needs 3 digits, not 2"
    );
}

#[test]
fn to_decimal128_preserves_nulls_and_length() {
    let arr = build("c", 20, 2, &[Some("1.00"), None, Some("-2.50"), None]);
    let out = arr.to_decimal128(20, 2, "c").unwrap();
    assert_eq!(out.len(), 4);
    assert_eq!(out.value(0), 100);
    assert!(out.is_null(1));
    assert_eq!(out.value(2), -250);
    assert!(out.is_null(3));
}

#[test]
fn to_decimal128_negative_target_scale_scales_by_a_power_of_ten() {
    let arr = build("c", 30, 0, &[Some("12345"), Some("-12345")]);
    let out = arr.to_decimal128(10, -2, "c").unwrap();
    // Decimal128(10, -2) stores unscaled 123 to mean 12300.
    assert_eq!(out.value(0), 123, "12345 -> 123 x 10^2 (half-even)");
    assert_eq!(out.value(1), -123);
    assert_eq!(out.data_type(), &DataType::Decimal128(10, -2));
}

#[test]
fn to_decimal128_rejects_scale_greater_than_precision() {
    let arr = build("c", 30, 0, &[Some("1")]);
    assert!(
        arr.to_decimal128(5, 10, "c").is_err(),
        "Arrow forbids positive scale > precision"
    );
}

#[test]
fn to_decimal128_result_datatype_matches_the_request() {
    let arr = build("c", 30, 4, &[Some("1.2345")]);
    let out = arr.to_decimal128(20, 4, "c").unwrap();
    assert_eq!(out.data_type(), &DataType::Decimal128(20, 4));
}

#[test]
fn to_decimal128_precision_boundary_grid_accepts_and_rejects_exactly() {
    for p in [1u8, 2, 5, 10, 20, 38] {
        let max_val = nines(p as usize);
        let over = pow10(p as usize);
        let arr_ok = build("c", 100, 0, &[Some(&max_val)]);
        assert!(
            arr_ok.to_decimal128(p, 0, "c").is_ok(),
            "10^{p} - 1 must fit Decimal128({p},0)"
        );
        let arr_bad = build("c", 100, 0, &[Some(&over)]);
        assert!(
            arr_bad.to_decimal128(p, 0, "c").is_err(),
            "10^{p} must NOT fit Decimal128({p},0)"
        );
        let arr_neg = build("c", 100, 0, &[Some(&format!("-{over}"))]);
        assert!(
            arr_neg.to_decimal128(p, 0, "c").is_err(),
            "-10^{p} must NOT fit Decimal128({p},0)"
        );
    }
}

/// DOCUMENTED-SILENT: narrowing the *scale* rounds half-to-even without any
/// error, unlike `from_decimal128`, which rejects the identical loss.
#[test]
fn to_decimal128_silently_rounds_excess_scale() {
    let arr = build("c", 30, 4, &[Some("1.2345"), Some("1.2355")]);
    let out = arr.to_decimal128(10, 3, "c").unwrap();
    assert_eq!(out.value(0), 1234, "1.2345 -> 1.234 (half-even)");
    assert_eq!(out.value(1), 1236, "1.2355 -> 1.236 (half-even)");
}

#[test]
fn to_decimal128_half_even_ties_match_the_encoder() {
    let arr = build(
        "c",
        30,
        1,
        &[Some("0.5"), Some("1.5"), Some("2.5"), Some("-2.5")],
    );
    let out = arr.to_decimal128(10, 0, "c").unwrap();
    assert_eq!(out.value(0), 0);
    assert_eq!(out.value(1), 2);
    assert_eq!(out.value(2), 2);
    assert_eq!(out.value(3), -2);
}

#[test]
fn to_decimal128_propagates_corrupt_payload_errors() {
    let raw = LargeBinaryArray::from(vec![Some([0x42u8, 0x01].as_ref())]);
    let arr = adopt(raw, 20, 0);
    assert!(
        arr.to_decimal128(10, 0, "c").is_err(),
        "a corrupt sign byte must surface as an error, not a wrong number"
    );
}

#[test]
fn to_decimal128_of_zero_is_zero_at_every_scale() {
    let arr = build("c", 30, 0, &[Some("0"), Some("-0")]);
    for scale in [0i8, 5, 18, 38] {
        let out = arr.to_decimal128(38, scale, "c").unwrap();
        assert_eq!(out.value(0), 0, "zero at scale {scale}");
        assert_eq!(out.value(1), 0, "negative zero at scale {scale}");
    }
}

// ===========================================================================
// F. to_decimal256 — range and precision narrowing
// ===========================================================================

#[test]
fn to_decimal256_rejects_precision_zero() {
    let arr = build("c", 100, 0, &[Some("1")]);
    let err = arr.to_decimal256(0, 0, "c").unwrap_err();
    assert!(err.to_string().contains("1..=76"), "{err}");
}

#[test]
fn to_decimal256_rejects_precision_above_76() {
    let arr = build("c", 100, 0, &[Some("1")]);
    for p in [77u8, 78, 100, 255] {
        assert!(
            arr.to_decimal256(p, 0, "c").is_err(),
            "Decimal256 precision {p} must be rejected"
        );
    }
}

#[test]
fn to_decimal256_accepts_the_largest_76_digit_value() {
    let s = nines(76);
    let arr = build("c", 200, 0, &[Some(&s)]);
    let out = arr.to_decimal256(76, 0, "c").unwrap();
    assert_eq!(out.len(), 1);
    assert!(!out.is_null(0));
    assert_eq!(out.value(0).to_string(), s, "10^76 - 1 must narrow exactly");
}

#[test]
fn to_decimal256_rejects_ten_to_the_76() {
    let s = pow10(76);
    let arr = build("c", 200, 0, &[Some(&s)]);
    let err = arr.to_decimal256(76, 0, "c").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("precision"),
        "10^76 fits an i256 but not precision 76 — must be a precision error: {msg}"
    );
}

#[test]
fn to_decimal256_accepts_the_most_negative_76_digit_value() {
    let s = format!("-{}", nines(76));
    let arr = build("c", 200, 0, &[Some(&s)]);
    let out = arr.to_decimal256(76, 0, "c").unwrap();
    assert_eq!(out.value(0).to_string(), s);
}

#[test]
fn to_decimal256_rejects_values_beyond_the_i256_width() {
    // 10^77 does not fit an i256 at all (i256::MAX ~ 5.79e76).
    let s = pow10(77);
    let arr = build("c", 200, 0, &[Some(&s)]);
    let err = arr.to_decimal256(76, 0, "c").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("overflows Decimal256"),
        "10^77 exceeds the i256 width and must be an overflow error, not a wrap: {msg}"
    );
}

#[test]
fn to_decimal256_rejects_i256_max_and_min_at_precision_76() {
    for s in [I256_MAX_STR, I256_MIN_STR] {
        let arr = build("c", 200, 0, &[Some(s)]);
        assert!(
            arr.to_decimal256(76, 0, "c").is_err(),
            "{s} is 78 digits and must not fit Decimal256(76,0)"
        );
    }
}

#[test]
fn to_decimal256_errors_when_scaling_up_overflows_precision() {
    let arr = build("c", 200, 0, &[Some("1")]);
    assert!(
        arr.to_decimal256(76, 76, "c").is_err(),
        "1 cannot be represented in Decimal256(76,76)"
    );
    let out = arr.to_decimal256(76, 75, "c").unwrap();
    assert_eq!(out.value(0).to_string(), pow10(75));
}

#[test]
fn to_decimal256_preserves_nulls_and_length() {
    let arr = build("c", 100, 3, &[None, Some("1.500"), None, Some("-1.500")]);
    let out = arr.to_decimal256(40, 3, "c").unwrap();
    assert_eq!(out.len(), 4);
    assert!(out.is_null(0));
    assert_eq!(out.value(1).to_string(), "1500");
    assert!(out.is_null(2));
    assert_eq!(out.value(3).to_string(), "-1500");
}

#[test]
fn to_decimal256_negative_target_scale_scales_by_a_power_of_ten() {
    let arr = build("c", 100, 0, &[Some("123456")]);
    let out = arr.to_decimal256(20, -3, "c").unwrap();
    assert_eq!(out.value(0).to_string(), "123", "123456 -> 123 x 10^3");
    assert_eq!(out.data_type(), &DataType::Decimal256(20, -3));
}

/// DOCUMENTED-SILENT: same half-even scale narrowing as `to_decimal128`.
#[test]
fn to_decimal256_silently_rounds_excess_scale() {
    let arr = build("c", 100, 4, &[Some("1.2345")]);
    let out = arr.to_decimal256(20, 2, "c").unwrap();
    assert_eq!(out.value(0).to_string(), "123", "1.2345 -> 1.23");
}

#[test]
fn to_decimal256_precision_boundary_grid_accepts_and_rejects_exactly() {
    for p in [1u8, 5, 38, 39, 60, 76] {
        let max_val = nines(p as usize);
        let over = pow10(p as usize);
        assert!(
            build("c", 200, 0, &[Some(&max_val)])
                .to_decimal256(p, 0, "c")
                .is_ok(),
            "10^{p} - 1 must fit Decimal256({p},0)"
        );
        assert!(
            build("c", 200, 0, &[Some(&over)])
                .to_decimal256(p, 0, "c")
                .is_err(),
            "10^{p} must NOT fit Decimal256({p},0)"
        );
    }
}

#[test]
fn to_decimal256_propagates_corrupt_payload_errors() {
    let raw = LargeBinaryArray::from(vec![Some([0xFEu8, 0x01].as_ref())]);
    let arr = adopt(raw, 40, 0);
    assert!(arr.to_decimal256(40, 0, "c").is_err());
}

// ===========================================================================
// G. from_decimal128 / from_decimal256 at the extremes
// ===========================================================================

#[test]
fn from_decimal128_preserves_i128_max_exactly() {
    let src = Decimal128Array::from(vec![Some(i128::MAX)]);
    let arr = DecimalArbArray::from_decimal128(&src, 0, 100, 0, "c").unwrap();
    assert_eq!(
        arr.value(0).unwrap().unwrap().to_canonical_string(),
        I128_MAX_STR,
        "widening i128::MAX must be lossless"
    );
}

#[test]
fn from_decimal128_preserves_i128_min_exactly() {
    let src = Decimal128Array::from(vec![Some(i128::MIN)]);
    let arr = DecimalArbArray::from_decimal128(&src, 0, 100, 0, "c").unwrap();
    assert_eq!(
        arr.value(0).unwrap().unwrap().to_canonical_string(),
        I128_MIN_STR,
        "widening i128::MIN must be lossless (no sign-extension bug)"
    );
}

#[test]
fn from_decimal128_rejects_a_target_precision_that_cannot_hold_the_value() {
    let src = Decimal128Array::from(vec![Some(i128::MAX)]);
    let err = expect_err(
        DecimalArbArray::from_decimal128(&src, 0, 38, 0, "amount"),
        "i128::MAX into precision 38",
    );
    assert!(
        err.to_string().contains("amount"),
        "must reject rather than truncate: {err}"
    );
}

#[test]
fn from_decimal128_rejects_a_target_scale_that_would_round() {
    let src = Decimal128Array::from(vec![Some(12345_i128)]);
    // source scale 4 => 1.2345; target scale 2 would need rounding.
    let err = expect_err(
        DecimalArbArray::from_decimal128(&src, 4, 20, 2, "amount"),
        "1.2345 into scale 2",
    );
    assert!(
        err.to_string().contains("scale"),
        "widening must refuse to silently round: {err}"
    );
}

#[test]
fn from_decimal128_handles_negative_source_scale() {
    let src = Decimal128Array::from(vec![Some(5_i128)]);
    let arr = DecimalArbArray::from_decimal128(&src, -3, 20, 0, "c").unwrap();
    assert_eq!(
        arr.value(0).unwrap().unwrap(),
        v("5000"),
        "source scale -3 means value x 10^3"
    );
}

#[test]
fn from_decimal128_handles_source_scale_38() {
    let src = Decimal128Array::from(vec![Some(1_i128)]);
    let arr = DecimalArbArray::from_decimal128(&src, 38, 100, 38, "c").unwrap();
    let expect = format!("0.{}1", "0".repeat(37));
    assert_eq!(
        arr.value(0).unwrap().unwrap().to_canonical_string(),
        expect,
        "10^-38 must widen exactly"
    );
}

#[test]
fn from_decimal128_preserves_nulls_and_length() {
    let src = Decimal128Array::from(vec![None, Some(1_i128), None, Some(-1_i128)]);
    let arr = DecimalArbArray::from_decimal128(&src, 0, 20, 0, "c").unwrap();
    assert_eq!(arr.len(), 4);
    assert!(arr.is_null(0));
    assert_eq!(arr.value(1).unwrap(), Some(v("1")));
    assert!(arr.is_null(2));
    assert_eq!(arr.value(3).unwrap(), Some(v("-1")));
}

#[test]
fn from_decimal128_round_trips_back_to_decimal128() {
    let values: Vec<Option<i128>> = vec![
        Some(0),
        Some(1),
        Some(-1),
        Some(i64::MAX as i128),
        Some(i64::MIN as i128),
        None,
        Some(99_999_999_999_999_999_999_999_999_999_999_999_999),
        Some(-99_999_999_999_999_999_999_999_999_999_999_999_999),
    ];
    let src = Decimal128Array::from(values.clone());
    let arb = DecimalArbArray::from_decimal128(&src, 6, 100, 6, "c").unwrap();
    let back = arb.to_decimal128(38, 6, "c").unwrap();
    assert_eq!(back.len(), values.len());
    for (i, expect) in values.iter().enumerate() {
        match expect {
            None => assert!(back.is_null(i), "null at {i}"),
            Some(x) => assert_eq!(back.value(i), *x, "value at {i} must round-trip exactly"),
        }
    }
}

#[test]
fn from_decimal256_preserves_i256_max_exactly() {
    let src = Decimal256Array::from(vec![Some(i256_max())]);
    let arr = DecimalArbArray::from_decimal256(&src, 0, 200, 0, "c").unwrap();
    assert_eq!(
        arr.value(0).unwrap().unwrap().to_canonical_string(),
        I256_MAX_STR,
        "widening i256::MAX must be lossless"
    );
}

#[test]
fn from_decimal256_preserves_i256_min_exactly() {
    let src = Decimal256Array::from(vec![Some(i256_min())]);
    let arr = DecimalArbArray::from_decimal256(&src, 0, 200, 0, "c").unwrap();
    assert_eq!(
        arr.value(0).unwrap().unwrap().to_canonical_string(),
        I256_MIN_STR,
        "widening i256::MIN must be lossless (two's-complement sign extension)"
    );
}

#[test]
fn from_decimal256_rejects_a_target_precision_that_cannot_hold_the_value() {
    let src = Decimal256Array::from(vec![Some(i256_max())]);
    let err = expect_err(
        DecimalArbArray::from_decimal256(&src, 0, 76, 0, "amount"),
        "i256::MAX into precision 76",
    );
    assert!(
        err.to_string().contains("amount"),
        "78-digit i256::MAX must not silently fit precision 76: {err}"
    );
}

#[test]
fn from_decimal256_rejects_a_target_scale_that_would_round() {
    let src = Decimal256Array::from(vec![Some(ArrowI256::from_i128(12345))]);
    assert!(
        DecimalArbArray::from_decimal256(&src, 4, 20, 2, "amount").is_err(),
        "widening must refuse to silently round"
    );
}

#[test]
fn from_decimal256_handles_negative_source_scale() {
    let src = Decimal256Array::from(vec![Some(ArrowI256::from_i128(7))]);
    let arr = DecimalArbArray::from_decimal256(&src, -5, 30, 0, "c").unwrap();
    assert_eq!(arr.value(0).unwrap().unwrap(), v("700000"));
}

#[test]
fn from_decimal256_preserves_nulls_and_length() {
    let src = Decimal256Array::from(vec![
        None,
        Some(ArrowI256::from_i128(-1)),
        None,
        Some(ArrowI256::from_i128(1)),
    ]);
    let arr = DecimalArbArray::from_decimal256(&src, 0, 40, 0, "c").unwrap();
    assert_eq!(arr.len(), 4);
    assert!(arr.is_null(0));
    assert_eq!(arr.value(1).unwrap(), Some(v("-1")));
    assert!(arr.is_null(2));
    assert_eq!(arr.value(3).unwrap(), Some(v("1")));
}

#[test]
fn from_decimal256_round_trips_back_to_decimal256() {
    let values = [
        ArrowI256::from_i128(0),
        ArrowI256::from_i128(1),
        ArrowI256::from_i128(-1),
        ArrowI256::from_i128(i128::MAX),
        ArrowI256::from_i128(i128::MIN),
    ];
    let src = Decimal256Array::from(values.iter().copied().map(Some).collect::<Vec<_>>());
    let arb = DecimalArbArray::from_decimal256(&src, 0, 200, 0, "c").unwrap();
    let back = arb.to_decimal256(76, 0, "c").unwrap();
    for (i, expect) in values.iter().enumerate() {
        assert_eq!(
            back.value(i),
            *expect,
            "value at {i} must round-trip exactly"
        );
    }
}

#[test]
fn from_decimal256_of_i256_min_survives_a_full_round_trip_at_precision_78() {
    let src = Decimal256Array::from(vec![Some(i256_min())]);
    let arb = DecimalArbArray::from_decimal256(&src, 0, 100, 0, "c").unwrap();
    // Precision 78 exceeds the Decimal256 cap of 76, so the narrowing must
    // error rather than wrap; the widened value itself is still exact.
    assert!(arb.to_decimal256(76, 0, "c").is_err());
    assert_eq!(
        arb.value(0).unwrap().unwrap().to_canonical_string(),
        I256_MIN_STR
    );
}

#[test]
fn from_decimal128_and_from_decimal256_agree_on_shared_values() {
    for x in [0_i128, 1, -1, 123456789, -987654321, i64::MAX as i128] {
        let a =
            DecimalArbArray::from_decimal128(&Decimal128Array::from(vec![Some(x)]), 3, 60, 3, "c")
                .unwrap();
        let b = DecimalArbArray::from_decimal256(
            &Decimal256Array::from(vec![Some(ArrowI256::from_i128(x))]),
            3,
            60,
            3,
            "c",
        )
        .unwrap();
        assert_eq!(
            a.value(0).unwrap(),
            b.value(0).unwrap(),
            "Decimal128 and Decimal256 widening must agree for {x}"
        );
    }
}

// ===========================================================================
// H. string conversions — no silent truncation on the text path
// ===========================================================================

#[test]
fn from_string_array_rejects_excess_fraction_rather_than_rounding() {
    let src = StringArray::from(vec![Some("1.239")]);
    let err = expect_err(
        DecimalArbArray::from_string_array(&src, 10, 2, "amount"),
        "1.239 into scale 2",
    );
    assert!(
        err.to_string().contains("amount"),
        "string ingest must reject, not round: {err}"
    );
}

#[test]
fn from_string_array_rejects_excess_integer_digits() {
    let src = StringArray::from(vec![Some("1000")]);
    assert!(DecimalArbArray::from_string_array(&src, 3, 0, "c").is_err());
}

#[test]
fn from_string_array_rejects_empty_and_blank_strings() {
    for bad in ["", " ", "\t", "."] {
        let src = StringArray::from(vec![Some(bad)]);
        assert!(
            DecimalArbArray::from_string_array(&src, 10, 2, "c").is_err(),
            "{bad:?} must not be accepted as a decimal"
        );
    }
}

#[test]
fn from_string_array_accepts_exponent_notation_without_loss() {
    let src = StringArray::from(vec![Some("1E+3"), Some("-2.5e2")]);
    let arr = DecimalArbArray::from_string_array(&src, 10, 0, "c").unwrap();
    assert_eq!(arr.value(0).unwrap(), Some(v("1000")));
    assert_eq!(arr.value(1).unwrap(), Some(v("-250")));
}

#[test]
fn string_array_round_trip_is_exact() {
    let inputs = vec![
        Some("1.2345"),
        None,
        Some("-0.0001"),
        Some("0"),
        Some("99999999999999999999.0001"),
    ];
    let src = StringArray::from(inputs.clone());
    let arr = DecimalArbArray::from_string_array(&src, 100, 4, "c").unwrap();
    let back = arr.to_string_array().unwrap();
    for (i, x) in inputs.iter().enumerate() {
        match x {
            None => assert!(back.is_null(i)),
            Some(s) => assert_eq!(back.value(i), *s, "row {i} must round-trip exactly"),
        }
    }
}

#[test]
fn to_string_array_emits_plain_form_for_large_and_tiny_values() {
    let big = nines(80);
    let out = build("c", 200, 0, &[Some(&big)]).to_string_array().unwrap();
    assert_eq!(out.value(0), big, "no exponent notation for large values");

    let tiny = build("c", 200, 20, &[Some("0.00000000000000000001")])
        .to_string_array()
        .unwrap();
    assert_eq!(
        tiny.value(0),
        "0.00000000000000000001",
        "no exponent notation for tiny values"
    );
}

#[test]
fn to_string_array_preserves_nulls() {
    let arr = build("c", 10, 2, &[None, Some("1.00"), None]);
    let out = arr.to_string_array().unwrap();
    assert!(out.is_null(0));
    assert!(!out.is_null(1));
    assert!(out.is_null(2));
}

// ===========================================================================
// I. MAX_PRECISION
// ===========================================================================

#[test]
fn field_construction_accepts_max_precision_and_rejects_one_past() {
    assert!(DecimalArbType::field("c", MAX_PRECISION, 0, true).is_ok());
    assert!(DecimalArbType::field("c", MAX_PRECISION, MAX_PRECISION, true).is_ok());
    assert!(DecimalArbType::field("c", MAX_PRECISION + 1, 0, true).is_err());
    assert!(DecimalArbType::field("c", 0, 0, true).is_err());
    assert!(DecimalArbType::field("c", 10, 11, true).is_err());
}

#[test]
fn metadata_round_trips_at_max_precision() {
    let f = DecimalArbType::field("c", MAX_PRECISION, 1234, true).unwrap();
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((MAX_PRECISION, 1234)),
        "MAX_PRECISION metadata must survive serialization"
    );
}

#[test]
fn a_thousand_digit_value_round_trips_through_the_builder() {
    let s: String = std::iter::repeat_n("1234567891", 100).collect();
    assert_eq!(s.len(), 1000);
    let arr = build("big", 1000, 0, &[Some(&s)]);
    assert_eq!(
        arr.value(0).unwrap().unwrap().to_canonical_string(),
        s,
        "1000-digit value must survive encode/decode"
    );
    // One digit past the declared precision must be rejected.
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "big", 999, 0).unwrap();
    assert!(b.append_str(&s).is_err(), "1000 digits must not fit p=999");
}

#[test]
fn a_max_precision_digit_value_round_trips_through_the_builder() {
    let s: String = std::iter::repeat_n("123456789", 7282).collect::<String>()[..65535].to_string();
    assert_eq!(s.len(), MAX_PRECISION as usize);
    let val = v(&s);
    assert_eq!(val.integer_digit_count(), MAX_PRECISION as u64);
    assert!(val.check_fits(MAX_PRECISION, 0, "c").is_ok());
    assert!(
        val.check_fits(MAX_PRECISION - 1, 0, "c").is_err(),
        "a MAX_PRECISION-digit value must not fit MAX_PRECISION-1"
    );
    let bytes = val.to_canonical_bytes_at_scale(0);
    let back = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 0).unwrap();
    assert_eq!(back, val, "MAX_PRECISION-digit value must round-trip");
}

#[test]
fn max_precision_fractional_split_is_enforced() {
    // (MAX_PRECISION, MAX_PRECISION - 3) leaves exactly 3 integer digits.
    let p = MAX_PRECISION;
    let s = MAX_PRECISION - 3;
    assert!(v("999").check_fits(p, s, "c").is_ok());
    assert!(
        v("1000").check_fits(p, s, "c").is_err(),
        "4 integer digits must not fit p-s == 3"
    );
}

// ===========================================================================
// J. adoption, sort keys, and residual hazards
// ===========================================================================

#[test]
fn adopting_an_array_under_a_mismatched_scale_silently_shifts_values() {
    // The bytes carry no scale; adoption trusts the Field. This documents the
    // hazard so a future scale-consistency check has a test to update.
    let arr = build("c", 100, 4, &[Some("3.1416")]);
    let (raw, _, _) = arr.into_inner();
    let reinterpreted = adopt(raw, 100, 2);
    assert_eq!(
        reinterpreted.value(0).unwrap(),
        Some(v("314.16")),
        "adoption at the wrong scale silently shifts the decimal point"
    );
}

#[test]
fn adoption_rejects_a_field_without_decimal_arb_metadata() {
    let (raw, _, _) = build("c", 10, 0, &[Some("1")]).into_inner();
    let plain = Field::new("c", DataType::LargeBinary, true);
    assert!(DecimalArbArray::try_from_array_and_field(raw, &plain).is_err());
}

#[test]
fn array_value_errors_on_an_empty_payload_instead_of_panicking() {
    let raw = LargeBinaryArray::from(vec![Some([].as_ref())]);
    let arr = adopt(raw, 10, 0);
    assert!(
        arr.value(0).is_err(),
        "a zero-length payload must be an error, not a panic or a zero"
    );
}

#[test]
fn array_value_errors_on_a_negative_zero_payload() {
    let raw = LargeBinaryArray::from(vec![Some([0xFFu8].as_ref())]);
    let arr = adopt(raw, 10, 0);
    assert!(arr.value(0).is_err());
}

#[test]
fn sort_keys_of_numerically_equal_values_are_identical() {
    let a = decimal_arb_to_sort_key(&v("5").to_canonical_bytes_at_scale(3));
    let b = decimal_arb_to_sort_key(&v("5.000").to_canonical_bytes_at_scale(3));
    let c = decimal_arb_to_sort_key(&v("+5.0").to_canonical_bytes_at_scale(3));
    assert_eq!(a, b, "5 and 5.000 must share a sort key at scale 3");
    assert_eq!(a, c, "+5.0 must share the same sort key");
}

#[test]
fn sort_keys_of_zero_and_negative_zero_are_identical() {
    let a = decimal_arb_to_sort_key(&v("0").to_canonical_bytes_at_scale(6));
    let b = decimal_arb_to_sort_key(&v("-0").to_canonical_bytes_at_scale(6));
    assert_eq!(a, b, "-0 and 0 must not split a GROUP BY");
}

#[test]
fn sort_keys_reproduce_numeric_order_across_the_i128_boundary() {
    let inputs = [
        I128_MIN_STR,
        "-170141183460469231731687303715884105727",
        "-1",
        "0",
        "1",
        I128_MAX_STR,
        "170141183460469231731687303715884105728",
    ];
    let mut keyed: Vec<(Vec<u8>, DecimalArbValue)> = inputs
        .iter()
        .map(|s| {
            let val = v(s);
            (
                decimal_arb_to_sort_key(&val.to_canonical_bytes_at_scale(0)),
                val,
            )
        })
        .collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    for w in keyed.windows(2) {
        assert!(
            w[0].1 <= w[1].1,
            "bytewise sort-key order must match numeric order: {} vs {}",
            w[0].1,
            w[1].1
        );
    }
}

#[test]
fn narrowing_then_widening_is_the_identity_when_nothing_is_lost() {
    let arr = build("c", 60, 4, &[Some("1.2345"), Some("-9999.0001"), None]);
    let d128 = arr.to_decimal128(38, 4, "c").unwrap();
    let back = DecimalArbArray::from_decimal128(&d128, 4, 60, 4, "c").unwrap();
    for i in 0..arr.len() {
        assert_eq!(
            arr.value(i).unwrap(),
            back.value(i).unwrap(),
            "lossless narrow+widen must be the identity at row {i}"
        );
    }
}

#[test]
fn widening_then_narrowing_is_the_identity_for_decimal256() {
    let arr = build(
        "c",
        200,
        6,
        &[Some("123456.789012"), Some("-0.000001"), None],
    );
    let d256 = arr.to_decimal256(76, 6, "c").unwrap();
    let back = DecimalArbArray::from_decimal256(&d256, 6, 200, 6, "c").unwrap();
    for i in 0..arr.len() {
        assert_eq!(arr.value(i).unwrap(), back.value(i).unwrap(), "row {i}");
    }
}

#[test]
fn precision_scale_metadata_survives_a_builder_finish() {
    for (p, s) in [
        (1u32, 0u32),
        (38, 10),
        (76, 0),
        (1000, 500),
        (MAX_PRECISION, 0),
    ] {
        let arr = build("c", p, s, &[]);
        assert_eq!((arr.precision(), arr.scale()), (p, s));
    }
}

#[test]
fn value_index_out_of_bounds_is_not_silently_zero() {
    // Guard: reading past the end must not return a plausible-looking value.
    let arr = build("c", 10, 0, &[Some("1")]);
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| arr.value(5)));
    match res {
        Err(_) => {} // panicking on an out-of-bounds index is acceptable
        Ok(Ok(Some(x))) => panic!("out-of-bounds read returned a value: {x}"),
        Ok(_) => {}
    }
}

#[test]
fn zero_is_the_only_value_encoding_to_a_single_byte() {
    for s in ["0", "-0", "0.0", "0.000000"] {
        assert_eq!(v(s).to_canonical_bytes_at_scale(9).len(), 1, "{s}");
    }
    for s in ["1", "-1", "0.000000001", "-0.000000001"] {
        assert!(
            v(s).to_canonical_bytes_at_scale(9).len() > 1,
            "{s} must have a magnitude at scale 9"
        );
    }
}

#[test]
fn equal_values_at_different_declared_scales_produce_equal_decoded_values() {
    let a = build("c", 40, 2, &[Some("1.5")]);
    let b = build("c", 40, 18, &[Some("1.5")]);
    assert_eq!(
        a.value(0).unwrap(),
        b.value(0).unwrap(),
        "the declared scale must not change the decoded number"
    );
    assert_ne!(
        a.as_inner().value(0),
        b.as_inner().value(0),
        "but the byte payloads differ — cross-scale byte comparison is unsafe"
    );
}

// ===========================================================================
// K. Second pass — deeper hunting for silent loss / non-error failure modes
// ===========================================================================

/// A value far below the target ULP narrows to a plain `0` with no error at
/// all. This is the sharpest form of the documented half-even narrowing:
/// a non-zero amount becomes zero at the sink.
#[test]
fn narrowing_a_high_scale_column_silently_produces_zero_for_decimal128() {
    let tiny = format!("0.{}1", "0".repeat(59)); // 1e-60
    let arr = build("c", 120, 60, &[Some(&tiny)]);
    assert_ne!(
        arr.value(0).unwrap(),
        Some(v("0")),
        "precondition: the stored value is non-zero"
    );
    let out = arr.to_decimal128(38, 10, "c").unwrap();
    assert_eq!(
        out.value(0),
        0,
        "1e-60 narrows to Decimal128(38,10) as zero — silently, with no error"
    );
}

#[test]
fn narrowing_a_high_scale_column_silently_produces_zero_for_decimal256() {
    let tiny = format!("0.{}1", "0".repeat(199)); // 1e-200
    let arr = build("c", 400, 200, &[Some(&tiny)]);
    let out = arr.to_decimal256(76, 38, "c").unwrap();
    assert_eq!(
        out.value(0).to_string(),
        "0",
        "1e-200 narrows to Decimal256(76,38) as zero — silently"
    );
}

/// A column whose declared scale exceeds `i8::MAX` can never be narrowed
/// losslessly, because the Arrow target scale is an `i8`. The narrowing still
/// succeeds and quietly drops the tail.
#[test]
fn columns_with_scale_beyond_the_arrow_scale_cap_narrow_silently() {
    let tiny = format!("0.{}7", "0".repeat(149)); // 7e-150
    let arr = build("c", 400, 200, &[Some(&tiny)]);
    // Arrow caps Decimal128/256 scale at 38/76, so a scale-200 decimal_arb
    // column can never be narrowed losslessly at all.
    assert!(
        arr.to_decimal128(38, 127, "c").is_err(),
        "Arrow must reject a scale above its Decimal128 cap"
    );
    assert!(
        arr.to_decimal256(76, 100, "c").is_err(),
        "Arrow must reject a scale above its Decimal256 cap"
    );
    // At the maximum representable scale the value is silently dropped to 0.
    let out = arr.to_decimal128(38, 38, "c").unwrap();
    assert_eq!(
        out.value(0),
        0,
        "7e-150 cannot be represented at scale 38; the value is dropped \
         to zero without an error"
    );
    let out256 = arr.to_decimal256(76, 76, "c").unwrap();
    assert_eq!(out256.value(0).to_string(), "0");
}

#[test]
fn to_decimal128_output_always_respects_the_declared_precision() {
    let corpus = [
        "0",
        "1",
        "-1",
        "0.5",
        "-0.5",
        "999999",
        "-999999",
        "12345.6789",
        "-12345.6789",
        "100000000000000000000",
        "-100000000000000000000",
    ];
    for s in corpus {
        let arr = build("c", 200, 8, &[Some(s)]);
        for p in 1u8..=38 {
            for scale in [0i8, 2, 8] {
                if let Ok(out) = arr.to_decimal128(p, scale, "c") {
                    let bound = 10i128.pow(p as u32);
                    let got = out.value(0);
                    assert!(
                        got > -bound && got < bound,
                        "to_decimal128({p},{scale}) on {s} produced {got}, \
                         outside |v| < 10^{p} — a wrapped or unvalidated value"
                    );
                }
            }
        }
    }
}

#[test]
fn to_decimal256_output_always_respects_the_declared_precision() {
    let corpus = [
        "0",
        "1",
        "-1",
        "0.5",
        "12345.6789",
        "-12345.6789",
        "1000000000000000000000000000000000000000000000000000000000000",
    ];
    for s in corpus {
        let arr = build("c", 200, 8, &[Some(s)]);
        for p in [1u8, 2, 10, 38, 39, 60, 76] {
            for scale in [0i8, 2, 8] {
                if let Ok(out) = arr.to_decimal256(p, scale, "c") {
                    let text = out.value(0).to_string();
                    let digits = text.trim_start_matches('-').trim_start_matches('0');
                    assert!(
                        digits.len() <= p as usize,
                        "to_decimal256({p},{scale}) on {s} produced {text} \
                         ({} digits) — beyond the declared precision",
                        digits.len()
                    );
                }
            }
        }
    }
}

#[test]
fn every_value_the_builder_accepts_survives_a_wider_corpus_grid() {
    let corpus = [
        "0",
        "-0",
        "1",
        "-1",
        "0.1",
        "-0.1",
        "0.0000001",
        "-0.0000001",
        "9999999999",
        "-9999999999",
        "1.0000001",
        "123456789.987654321",
        "-123456789.987654321",
        "1E+18",
        "1E-18",
        "-1E+18",
        "-1E-18",
    ];
    for s in corpus {
        let val = v(s);
        for p in [1u32, 2, 9, 18, 19, 38, 65, 100] {
            for scale in [0u32, 1, 7, 18] {
                if scale > p {
                    continue;
                }
                let mut b = DecimalArbArrayBuilder::with_capacity(1, "c", p, scale).unwrap();
                match b.append_value(&val) {
                    Err(_) => assert!(
                        val.check_fits(p, scale, "c").is_err(),
                        "builder and check_fits disagree for {s} at ({p},{scale})"
                    ),
                    Ok(()) => {
                        let arr = b.finish();
                        assert_eq!(
                            arr.value(0).unwrap(),
                            Some(val.clone()),
                            "{s} must be stored unchanged at ({p},{scale})"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn absurd_exponent_strings_are_rejected_or_stored_exactly() {
    let inputs = [
        "1E+1000000",
        "1E-1000000",
        "1E+2147483647",
        "1E-2147483647",
        "1E+9223372036854775807",
        // "1E+9223372036854775808" is covered separately: it panics today.
        "1E-9223372036854775808",
        "1E+99999999999999999999",
        "-1E+1000000",
    ];
    for s in inputs {
        let src = StringArray::from(vec![Some(s)]);
        match DecimalArbArray::from_string_array(&src, 100, 10, "c") {
            Err(_) => {}
            Ok(arr) => {
                let stored = arr
                    .value(0)
                    .expect("stored payload must decode")
                    .expect("non-null");
                assert_eq!(
                    stored,
                    v(s),
                    "{s} was accepted by the builder but stored as a different number"
                );
            }
        }
    }
}

#[test]
fn absurd_exponent_values_are_rejected_by_check_fits_without_panicking() {
    for s in ["1E+1000000", "1E-1000000", "1E+2147483647", "1E-2147483647"] {
        let Ok(val) = DecimalArbValue::from_str(s) else {
            continue;
        };
        assert!(
            val.check_fits(100, 10, "c").is_err(),
            "{s} cannot fit (100,10) and must be rejected"
        );
    }
}

#[test]
fn extreme_bigint_scales_do_not_panic_in_digit_counting() {
    for scale in [0i64, -1, 1, i64::MAX, i64::MIN + 1] {
        let val = DecimalArbValue::from_bigint_and_scale(BigInt::from(7), scale);
        let _ = val.integer_digit_count();
        let _ = val.fractional_digit_count();
        let _ = val.check_fits(100, 10, "c");
    }
}

/// FINDING (F-06-A). `integer_digit_count` computes `(-frac) as u64` on the
/// normalized `BigDecimal` scale. When that scale is exactly `i64::MIN` the
/// negation overflows and the process panics
/// (`decimal_arb.rs:462: attempt to negate with overflow`) instead of
/// returning a typed error. Every `check_fits` caller inherits the panic.
#[test]
#[ignore = "FINDING: integer_digit_count panics with negate-overflow when the value's scale is i64::MIN"]
fn digit_counting_at_scale_i64_min_must_not_panic() {
    let val = DecimalArbValue::from_bigint_and_scale(BigInt::from(7), i64::MIN);
    let _ = val.integer_digit_count();
    let _ = val.check_fits(100, 10, "c");
}

/// FINDING (F-06-A), user-reachable form. `bigdecimal` happily parses an
/// exponent of `2^63`, producing a value whose scale is exactly `i64::MIN`.
/// Ingesting that text through `from_string_array` — i.e. any string column
/// being cast into a `decimal_arb` column — panics the agent rather than
/// producing the FR-013 "does not fit" user error.
#[test]
#[ignore = "FINDING: string ingest of '1E+9223372036854775808' panics (negate overflow) instead of returning a user error"]
fn string_ingest_of_an_exponent_of_two_to_the_63_must_not_panic() {
    // Precondition: the text really does parse into a DecimalArbValue.
    assert!(
        DecimalArbValue::from_str("1E+9223372036854775808").is_ok(),
        "precondition: bigdecimal accepts an exponent of 2^63"
    );
    let src = StringArray::from(vec![Some("1E+9223372036854775808")]);
    let res = DecimalArbArray::from_string_array(&src, 100, 10, "c");
    assert!(
        res.is_err(),
        "a value with 2^63 integer digits must be a user error, not a panic"
    );
}

/// Same defect through the plain builder entry point.
#[test]
#[ignore = "FINDING: DecimalArbArrayBuilder::append_str panics on '1E+9223372036854775808'"]
fn builder_append_str_of_an_exponent_of_two_to_the_63_must_not_panic() {
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "amount", 100, 10).unwrap();
    assert!(
        b.append_str("1E+9223372036854775808").is_err(),
        "append_str must reject, not panic"
    );
}

#[test]
fn from_string_array_rejects_exponent_forms_that_exceed_the_declared_precision() {
    for (text, p, s) in [("1E+5", 5u32, 0u32), ("1E-5", 10, 4), ("-1E+5", 5, 0)] {
        let src = StringArray::from(vec![Some(text)]);
        assert!(
            DecimalArbArray::from_string_array(&src, p, s, "c").is_err(),
            "{text} must not fit ({p},{s}) — exponent forms must be checked too"
        );
    }
}

#[test]
fn parse_of_plus_and_leading_zero_forms_matches_the_plain_form() {
    for (a, b) in [
        ("+1.5", "1.5"),
        ("0001.5", "1.5"),
        ("+0.5", "0.5"),
        ("-0001.5", "-1.5"),
    ] {
        assert_eq!(v(a), v(b), "{a} and {b} must be the same value");
        assert_eq!(
            v(a).to_canonical_bytes_at_scale(4),
            v(b).to_canonical_bytes_at_scale(4),
            "{a} and {b} must encode identically"
        );
    }
}

#[test]
fn scaling_up_never_loses_precision_for_the_whole_grid() {
    for s in ["1.5", "-1.5", "0.0001", "-0.0001", "12345"] {
        let val = v(s);
        let need = val.fractional_digit_count() as u32;
        for target in need..=need + 40 {
            let bytes = val.to_canonical_bytes_at_scale(target);
            let back = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, target).unwrap();
            assert_eq!(back, val, "{s} must be exact when padded to scale {target}");
        }
    }
}

#[test]
fn to_decimal128_and_to_decimal256_agree_when_both_succeed() {
    let corpus = [
        "0",
        "1",
        "-1",
        "0.25",
        "-0.25",
        "999999.5",
        "-999999.5",
        "1234567890",
    ];
    for s in corpus {
        let arr = build("c", 200, 4, &[Some(s)]);
        for p in [10u8, 20, 38] {
            for scale in [0i8, 2, 4] {
                let a = arr.to_decimal128(p, scale, "c");
                let b = arr.to_decimal256(p, scale, "c");
                match (a, b) {
                    (Ok(x), Ok(y)) => assert_eq!(
                        x.value(0).to_string(),
                        y.value(0).to_string(),
                        "Decimal128 and Decimal256 narrowing of {s} at ({p},{scale}) \
                         must agree"
                    ),
                    (Err(_), Err(_)) => {}
                    (a, b) => panic!(
                        "narrowing {s} at ({p},{scale}) disagreed on success: \
                         d128 ok={} d256 ok={}",
                        a.is_ok(),
                        b.is_ok()
                    ),
                }
            }
        }
    }
}

#[test]
fn decimal128_round_trip_through_string_array_is_exact() {
    let arr = build("c", 60, 6, &[Some("1.500000"), Some("-0.000001"), None]);
    let text = arr.to_string_array().unwrap();
    let back = DecimalArbArray::from_string_array(&text, 60, 6, "c").unwrap();
    for i in 0..arr.len() {
        assert_eq!(
            arr.value(i).unwrap(),
            back.value(i).unwrap(),
            "string round-trip must be exact at row {i}"
        );
    }
}

#[test]
fn a_value_rejected_by_check_fits_is_also_rejected_by_every_ingest_path() {
    let bad = "1.239"; // 3 significant fractional digits
    let (p, s) = (10u32, 2u32);
    assert!(v(bad).check_fits(p, s, "c").is_err());

    let mut b = DecimalArbArrayBuilder::with_capacity(1, "c", p, s).unwrap();
    assert!(b.append_str(bad).is_err(), "append_str must reject it");

    let src = StringArray::from(vec![Some(bad)]);
    assert!(
        DecimalArbArray::from_string_array(&src, p, s, "c").is_err(),
        "from_string_array must reject it"
    );

    let d128 = Decimal128Array::from(vec![Some(1239_i128)]);
    assert!(
        DecimalArbArray::from_decimal128(&d128, 3, p, s, "c").is_err(),
        "from_decimal128 must reject the same loss"
    );

    let d256 = Decimal256Array::from(vec![Some(ArrowI256::from_i128(1239))]);
    assert!(
        DecimalArbArray::from_decimal256(&d256, 3, p, s, "c").is_err(),
        "from_decimal256 must reject the same loss"
    );
}

/// The narrowing casts are the one family that does NOT reject the loss that
/// every ingest path rejects. Pinned so a future policy change is visible.
#[test]
fn narrowing_casts_accept_the_very_loss_that_ingest_rejects() {
    let arr = build("c", 20, 3, &[Some("1.239")]);
    assert!(
        v("1.239").check_fits(10, 2, "c").is_err(),
        "ingest rejects 1.239 at scale 2"
    );
    let out = arr
        .to_decimal128(10, 2, "c")
        .expect("narrowing accepts the same loss");
    assert_eq!(out.value(0), 124, "and silently returns the rounded value");
}

#[test]
fn sort_key_length_prefix_is_stable_for_equal_magnitudes() {
    let a = decimal_arb_to_sort_key(&v("1").to_canonical_bytes_at_scale(0));
    let b = decimal_arb_to_sort_key(&v("1.000").to_canonical_bytes_at_scale(0));
    assert_eq!(a, b);
    assert_eq!(a.len(), 6, "1 sign + 4 length + 1 magnitude byte");
}

#[test]
fn adopted_arrays_report_the_field_precision_and_scale() {
    let (raw, _, _) = build("c", 100, 4, &[Some("1.2345")]).into_inner();
    let field = DecimalArbType::field("c", 77, 9, true).unwrap();
    let adopted = DecimalArbArray::try_from_array_and_field(raw, &field).unwrap();
    assert_eq!((adopted.precision(), adopted.scale()), (77, 9));
}

#[test]
fn empty_large_binary_array_adopts_and_converts_cleanly() {
    let empty: Vec<Option<&[u8]>> = vec![];
    let raw = LargeBinaryArray::from(empty);
    let arr = adopt(raw, 20, 2);
    assert!(arr.is_empty());
    assert_eq!(arr.to_decimal128(20, 2, "c").unwrap().len(), 0);
    assert_eq!(arr.to_string_array().unwrap().len(), 0);
}
