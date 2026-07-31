//! Adversarial pass, agent 08 — **round-trip and canonicalization invariants**.
//!
//! Focus: the canonical byte codec in `types::decimal_arb` and everything that
//! depends on it being a *total, deterministic, injective-on-values* function.
//!
//! * `value -> to_canonical_bytes_at_scale -> from_canonical_bytes_at_scale ->
//!   value` across signs, magnitudes (10^0 .. 10^200), tiny fractions, and
//!   values with 60+ integer and 30+ fractional digits.
//! * `string -> value -> string` stability and `to_plain_string` non-exponent
//!   form.
//! * encode at scale A, decode at scale B — the scale is *not* in the bytes,
//!   so this is a documented hazard; pin the exact factor.
//! * **Byte-identity of numerically-equal spellings** (`5`, `5.0`, `05`, `+5`,
//!   `5.000`, `5e0`, `0.5e1`, `500e-2`). GROUP BY, DISTINCT, and hash-join keys
//!   are computed over these bytes, so any divergence silently splits groups.
//! * Negative zero in every spelling, and the minimal-form leading-zero
//!   stripping.
//! * The exact rejection set of `from_canonical_bytes_at_scale`.
//!
//! Corpus is fixed and deterministic — no RNG.

use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field};
use num_bigint::BigInt;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use streamling_common::types::decimal_arb::{
    DecimalArbArray, DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, MAX_PRECISION,
    decimal_arb_to_sort_key,
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn v(s: &str) -> DecimalArbValue {
    DecimalArbValue::from_str(s).unwrap_or_else(|e| panic!("parse {s:?}: {e}"))
}

fn enc(s: &str, scale: u32) -> Vec<u8> {
    v(s).to_canonical_bytes_at_scale(scale)
}

fn dec(bytes: &[u8], scale: u32) -> DecimalArbValue {
    DecimalArbValue::from_canonical_bytes_at_scale(bytes, scale)
        .unwrap_or_else(|e| panic!("decode {bytes:02x?} at scale {scale}: {e}"))
}

fn hash_of<T: Hash>(t: &T) -> u64 {
    let mut h = DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

/// `10^k` as a plain decimal string.
fn pow10(k: usize) -> String {
    let mut s = String::with_capacity(k + 1);
    s.push('1');
    for _ in 0..k {
        s.push('0');
    }
    s
}

/// `10^-k` as a plain decimal string (`k >= 1`).
fn neg_pow10(k: usize) -> String {
    let mut s = String::with_capacity(k + 3);
    s.push_str("0.");
    for _ in 0..(k - 1) {
        s.push('0');
    }
    s.push('1');
    s
}

/// A value with 65 integer digits and 35 fractional digits, all non-zero so
/// nothing is lost to normalization.
fn big_mixed() -> String {
    let int: String = (0..65u8).map(|i| char::from(b'1' + (i % 9))).collect();
    let frac: String = (0..35u8)
        .map(|i| char::from(b'1' + (i.wrapping_mul(3) % 9)))
        .collect();
    format!("{int}.{frac}")
}

/// Deterministic corpus. Every entry is exactly representable at scale 10 and
/// all entries are numerically distinct.
const CORPUS: &[&str] = &[
    "0",
    "1",
    "-1",
    "2",
    "-2",
    "9",
    "-9",
    "10",
    "-10",
    "127",
    "-127",
    "128",
    "-128",
    "255",
    "-255",
    "256",
    "-256",
    "65535",
    "-65535",
    "65536",
    "-65536",
    "16777215",
    "-16777215",
    "16777216",
    "-16777216",
    "4294967295",
    "-4294967295",
    "4294967296",
    "-4294967296",
    "0.1",
    "-0.1",
    "0.01",
    "-0.01",
    "0.5",
    "-0.5",
    "0.0000000001",
    "-0.0000000001",
    "3.1415926535",
    "-3.1415926535",
    "123456789.123456789",
    "-123456789.123456789",
    "99999999999999999999",
    "-99999999999999999999",
    "100000000000000000000",
    "-100000000000000000000",
];

/// The set of spellings that must all mean the number five.
const FIVE_SPELLINGS: &[&str] = &[
    "5",
    "5.0",
    "5.00",
    "5.000",
    "05",
    "005",
    "+5",
    "+5.0",
    "+05.000",
    "5e0",
    "5E0",
    "0.5e1",
    "500e-2",
    "0.05E+2",
    "+0000005.0000",
];

/// Every spelling of zero we can think of.
const ZERO_SPELLINGS: &[&str] = &[
    "0",
    "-0",
    "+0",
    "0.0",
    "-0.0",
    "+0.0",
    "0.000000",
    "-0.000000",
    "00",
    "-00",
    "0e0",
    "-0e0",
    "0E+10",
    "-0E+10",
    "0e-10",
    "-0e-10",
    "0.0e100",
    "-0.0e-100",
    "000.000",
];

// ===========================================================================
// A. Byte-identity of numerically-equal spellings
//    (GROUP BY / DISTINCT / hash-join keys are these bytes)
// ===========================================================================

#[test]
fn five_spellings_produce_identical_bytes_at_scale_0() {
    let want = enc("5", 0);
    for s in FIVE_SPELLINGS {
        assert_eq!(
            enc(s, 0),
            want,
            "spelling {s:?} of 5 must encode byte-identically at scale 0 \
             (GROUP BY/DISTINCT group on these bytes)"
        );
    }
}

#[test]
fn five_spellings_produce_identical_bytes_at_scale_6() {
    let want = enc("5", 6);
    for s in FIVE_SPELLINGS {
        assert_eq!(
            enc(s, 6),
            want,
            "spelling {s:?} of 5 must encode byte-identically at scale 6"
        );
    }
}

#[test]
fn five_spellings_produce_identical_bytes_at_scale_18() {
    let want = enc("5", 18);
    for s in FIVE_SPELLINGS {
        assert_eq!(
            enc(s, 18),
            want,
            "spelling {s:?} of 5 must encode byte-identically at scale 18"
        );
    }
}

#[test]
fn five_spellings_produce_identical_bytes_at_every_scale_0_to_40() {
    for scale in 0..=40u32 {
        let want = enc("5", scale);
        for s in FIVE_SPELLINGS {
            assert_eq!(
                enc(s, scale),
                want,
                "spelling {s:?} of 5 diverged at scale {scale}"
            );
        }
    }
}

#[test]
fn negative_five_spellings_produce_identical_bytes() {
    let want = enc("-5", 4);
    for s in ["-5", "-5.0", "-05", "-5.0000", "-5e0", "-0.5e1", "-500e-2"] {
        assert_eq!(
            enc(s, 4),
            want,
            "negative spelling {s:?} must encode byte-identically"
        );
    }
}

#[test]
fn zero_spellings_all_produce_the_single_zero_byte_at_scale_0() {
    for s in ZERO_SPELLINGS {
        assert_eq!(
            enc(s, 0),
            vec![0x00u8],
            "zero spelling {s:?} must encode to the lone 0x00 sign byte"
        );
    }
}

#[test]
fn zero_spellings_all_produce_the_single_zero_byte_at_every_scale() {
    for scale in [0u32, 1, 2, 7, 18, 38, 100] {
        for s in ZERO_SPELLINGS {
            assert_eq!(
                enc(s, scale),
                vec![0x00u8],
                "zero spelling {s:?} at scale {scale} must stay the lone 0x00 byte"
            );
        }
    }
}

#[test]
fn leading_zeros_never_change_the_encoding() {
    for base in ["1", "42", "-42", "0.25", "-0.25", "999999999"] {
        let want = enc(base, 8);
        for pad in ["0", "00", "0000000000"] {
            let padded = if let Some(rest) = base.strip_prefix('-') {
                format!("-{pad}{rest}")
            } else {
                format!("{pad}{base}")
            };
            assert_eq!(
                enc(&padded, 8),
                want,
                "leading zeros in {padded:?} changed the canonical bytes"
            );
        }
    }
}

#[test]
fn trailing_fractional_zeros_never_change_the_encoding() {
    for base in ["1", "1.5", "-1.5", "0.0001", "-0.0001", "123.456"] {
        let want = enc(base, 10);
        for extra in 0..6 {
            let mut padded = base.to_string();
            if !padded.contains('.') {
                padded.push('.');
            }
            for _ in 0..extra {
                padded.push('0');
            }
            assert_eq!(
                enc(&padded, 10),
                want,
                "trailing zeros in {padded:?} changed the canonical bytes"
            );
        }
    }
}

#[test]
fn explicit_plus_sign_never_changes_the_encoding() {
    for base in ["1", "0.5", "123456789012345678901234567890", "0"] {
        assert_eq!(
            enc(&format!("+{base}"), 12),
            enc(base, 12),
            "leading '+' changed the encoding of {base:?}"
        );
    }
}

#[test]
fn exponent_and_plain_spellings_of_the_same_number_agree() {
    let cases = [
        ("500", "5e2"),
        ("500", "5E2"),
        ("500", "0.5e3"),
        ("0.005", "5e-3"),
        ("-0.005", "-5e-3"),
        ("1000000", "1e6"),
        ("-1000000", "-1e6"),
    ];
    for (plain, expo) in cases {
        assert_eq!(
            enc(plain, 6),
            enc(expo, 6),
            "{plain:?} and {expo:?} must encode identically"
        );
    }
}

#[test]
fn equal_values_always_encode_to_equal_bytes_over_corpus() {
    for a in CORPUS {
        for b in CORPUS {
            let (va, vb) = (v(a), v(b));
            if va == vb {
                assert_eq!(
                    va.to_canonical_bytes_at_scale(10),
                    vb.to_canonical_bytes_at_scale(10),
                    "{a:?} == {b:?} but bytes differ"
                );
            }
        }
    }
}

#[test]
fn distinct_corpus_values_never_collide_on_bytes_at_scale_10() {
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    for s in CORPUS {
        let bytes = enc(s, 10);
        assert!(
            seen.insert(bytes.clone()),
            "byte collision for {s:?} -> {bytes:02x?}; distinct values must not \
             share canonical bytes (join/GROUP BY keys would merge rows)"
        );
    }
    assert_eq!(seen.len(), CORPUS.len());
}

#[test]
fn equal_values_hash_equal_and_encode_equal() {
    for s in FIVE_SPELLINGS {
        let a = v("5");
        let b = v(s);
        assert_eq!(a, b, "{s:?} must equal 5");
        assert_eq!(hash_of(&a), hash_of(&b), "{s:?} must hash equal to 5");
        assert_eq!(
            a.to_canonical_bytes_at_scale(9),
            b.to_canonical_bytes_at_scale(9)
        );
    }
}

#[test]
fn byte_equality_implies_value_equality_over_corpus_at_many_scales() {
    for scale in [0u32, 1, 5, 10] {
        for a in CORPUS {
            for b in CORPUS {
                if enc(a, scale) == enc(b, scale) {
                    assert_eq!(
                        dec(&enc(a, scale), scale),
                        dec(&enc(b, scale), scale),
                        "{a:?} and {b:?} share bytes at scale {scale} but decode differently"
                    );
                }
            }
        }
    }
}

// ===========================================================================
// B. value -> bytes -> value round trip
// ===========================================================================

#[test]
fn roundtrip_is_identity_for_the_whole_corpus_at_scale_10() {
    for s in CORPUS {
        let orig = v(s);
        let back = dec(&orig.to_canonical_bytes_at_scale(10), 10);
        assert_eq!(orig, back, "round trip changed {s:?} at scale 10");
    }
}

#[test]
fn roundtrip_is_identity_for_the_whole_corpus_at_scale_38() {
    for s in CORPUS {
        let orig = v(s);
        let back = dec(&orig.to_canonical_bytes_at_scale(38), 38);
        assert_eq!(orig, back, "round trip changed {s:?} at scale 38");
    }
}

#[test]
fn roundtrip_is_identity_for_integers_at_scale_0() {
    for s in CORPUS {
        let orig = v(s);
        if orig.fractional_digit_count() != 0 {
            continue;
        }
        let back = dec(&orig.to_canonical_bytes_at_scale(0), 0);
        assert_eq!(
            orig, back,
            "integer {s:?} did not survive scale-0 round trip"
        );
    }
}

#[test]
fn roundtrip_is_identity_for_zero_at_every_scale() {
    let zero = v("0");
    for scale in [0u32, 1, 2, 3, 18, 38, 76, 100, 1000] {
        let back = dec(&zero.to_canonical_bytes_at_scale(scale), scale);
        assert_eq!(back, zero, "zero broke at scale {scale}");
        assert_eq!(back.to_canonical_string(), "0");
    }
}

#[test]
fn roundtrip_is_identity_for_plus_and_minus_one_at_every_scale_0_to_64() {
    for scale in 0..=64u32 {
        for s in ["1", "-1"] {
            let orig = v(s);
            let back = dec(&orig.to_canonical_bytes_at_scale(scale), scale);
            assert_eq!(orig, back, "{s:?} broke at scale {scale}");
        }
    }
}

#[test]
fn roundtrip_is_identity_for_powers_of_ten_1_to_200_at_scale_0() {
    for k in 1..=200usize {
        let s = pow10(k);
        let orig = v(&s);
        let back = dec(&orig.to_canonical_bytes_at_scale(0), 0);
        assert_eq!(orig, back, "10^{k} did not survive scale-0 round trip");
        assert_eq!(back.to_canonical_string(), s, "10^{k} string form changed");
    }
}

#[test]
fn roundtrip_is_identity_for_negative_powers_of_ten_1_to_200_at_scale_0() {
    for k in 1..=200usize {
        let s = format!("-{}", pow10(k));
        let orig = v(&s);
        let back = dec(&orig.to_canonical_bytes_at_scale(0), 0);
        assert_eq!(orig, back, "-10^{k} did not survive round trip");
        assert_eq!(back.to_canonical_string(), s);
    }
}

#[test]
fn roundtrip_is_identity_for_powers_of_ten_at_scale_18() {
    for k in 1..=120usize {
        let s = pow10(k);
        let orig = v(&s);
        let back = dec(&orig.to_canonical_bytes_at_scale(18), 18);
        assert_eq!(orig, back, "10^{k} broke at scale 18");
    }
}

#[test]
fn roundtrip_is_identity_for_tiny_fractions_10_to_the_minus_1_through_40() {
    for k in 1..=40usize {
        let s = neg_pow10(k);
        let orig = v(&s);
        let back = dec(&orig.to_canonical_bytes_at_scale(k as u32), k as u32);
        assert_eq!(orig, back, "10^-{k} broke at its own scale");
        let back_wide = dec(&orig.to_canonical_bytes_at_scale(60), 60);
        assert_eq!(orig, back_wide, "10^-{k} broke at scale 60");
    }
}

#[test]
fn roundtrip_is_identity_for_negative_tiny_fractions() {
    for k in 1..=40usize {
        let s = format!("-{}", neg_pow10(k));
        let orig = v(&s);
        let back = dec(&orig.to_canonical_bytes_at_scale(50), 50);
        assert_eq!(orig, back, "-10^-{k} broke at scale 50");
        assert_eq!(
            orig.to_canonical_bytes_at_scale(50)[0],
            0xFF,
            "-10^-{k} must carry the negative sign byte"
        );
    }
}

#[test]
fn roundtrip_is_identity_for_65_integer_and_35_fractional_digits() {
    let s = big_mixed();
    let orig = v(&s);
    assert_eq!(orig.integer_digit_count(), 65);
    assert_eq!(orig.fractional_digit_count(), 35);
    let back = dec(&orig.to_canonical_bytes_at_scale(35), 35);
    assert_eq!(
        orig, back,
        "100-significant-digit value did not survive round trip"
    );
    assert_eq!(back.to_canonical_string(), s);
}

#[test]
fn roundtrip_of_65_integer_35_fractional_is_stable_at_wider_scales() {
    let s = big_mixed();
    let orig = v(&s);
    for scale in [35u32, 36, 40, 64, 100] {
        let back = dec(&orig.to_canonical_bytes_at_scale(scale), scale);
        assert_eq!(orig, back, "big mixed value broke at scale {scale}");
    }
}

#[test]
fn roundtrip_is_identity_for_negative_big_mixed_value() {
    let s = format!("-{}", big_mixed());
    let orig = v(&s);
    let back = dec(&orig.to_canonical_bytes_at_scale(35), 35);
    assert_eq!(orig, back);
    assert_eq!(back.to_canonical_string(), s);
}

#[test]
fn roundtrip_preserves_sign_for_every_corpus_entry() {
    for s in CORPUS {
        let orig = v(s);
        let bytes = orig.to_canonical_bytes_at_scale(10);
        let negative = s.starts_with('-');
        assert_eq!(
            bytes[0],
            if negative { 0xFF } else { 0x00 },
            "sign byte wrong for {s:?}"
        );
        assert_eq!(dec(&bytes, 10), orig);
    }
}

#[test]
fn encode_is_idempotent_through_decode() {
    for s in CORPUS {
        for scale in [0u32, 3, 10, 25] {
            let orig = v(s);
            if orig.fractional_digit_count() > scale as u64 {
                continue;
            }
            let once = orig.to_canonical_bytes_at_scale(scale);
            let twice = dec(&once, scale).to_canonical_bytes_at_scale(scale);
            assert_eq!(
                once, twice,
                "encode(decode(encode({s:?}))) differs at scale {scale}"
            );
        }
    }
}

#[test]
fn triple_roundtrip_converges_for_corpus() {
    for s in CORPUS {
        let a = v(s);
        let b = dec(&a.to_canonical_bytes_at_scale(20), 20);
        let c = dec(&b.to_canonical_bytes_at_scale(20), 20);
        let d = dec(&c.to_canonical_bytes_at_scale(20), 20);
        assert_eq!(a, b, "{s:?} first round trip");
        assert_eq!(b, c, "{s:?} second round trip");
        assert_eq!(c, d, "{s:?} third round trip");
    }
}

#[test]
fn roundtrip_at_max_i64_ish_scales_stays_exact_for_one() {
    // Wide but tractable scales; 10^scale magnitudes only.
    for scale in [200u32, 512, 1000] {
        let orig = v("1");
        let back = dec(&orig.to_canonical_bytes_at_scale(scale), scale);
        assert_eq!(orig, back, "1 broke at scale {scale}");
    }
}

#[test]
fn roundtrip_at_max_precision_scale_stays_exact() {
    let orig = v("1");
    let bytes = orig.to_canonical_bytes_at_scale(MAX_PRECISION);
    let back = dec(&bytes, MAX_PRECISION);
    assert_eq!(
        orig, back,
        "1 must round trip even at the declared MAX_PRECISION scale"
    );
}

#[test]
fn roundtrip_holds_for_every_single_byte_magnitude() {
    for m in 1..=255u8 {
        let s = m.to_string();
        let bytes = enc(&s, 0);
        assert_eq!(bytes, vec![0x00, m], "magnitude {m} encoding");
        assert_eq!(dec(&bytes, 0), v(&s));
        let nbytes = enc(&format!("-{s}"), 0);
        assert_eq!(nbytes, vec![0xFF, m], "negative magnitude {m} encoding");
        assert_eq!(dec(&nbytes, 0), v(&format!("-{s}")));
    }
}

#[test]
fn roundtrip_holds_across_byte_width_boundaries() {
    // 2^(8k) - 1 and 2^(8k) for k = 1..=12: exactly where the magnitude grows.
    for k in 1..=12u32 {
        let boundary = BigInt::from(1u8) << (8 * k);
        for delta in [-1i32, 0, 1] {
            let n = &boundary + BigInt::from(delta);
            let s = n.to_string();
            let orig = v(&s);
            assert_eq!(dec(&enc(&s, 0), 0), orig, "boundary {s} broke");
            let ns = format!("-{s}");
            assert_eq!(dec(&enc(&ns, 0), 0), v(&ns), "boundary {ns} broke");
        }
    }
}

// ===========================================================================
// C. string -> value -> string
// ===========================================================================

#[test]
fn canonical_string_round_trips_through_from_str_over_corpus() {
    for s in CORPUS {
        let a = v(s);
        let text = a.to_canonical_string();
        let b = v(&text);
        assert_eq!(a, b, "{s:?} -> {text:?} -> value changed");
        assert_eq!(
            text,
            b.to_canonical_string(),
            "to_canonical_string is not idempotent for {s:?}"
        );
    }
}

#[test]
fn canonical_string_never_uses_exponent_notation() {
    let cases = [
        "1e100",
        "-1e100",
        "1e-100",
        "-1e-100",
        "1e300",
        "0.000000000000000000000000000001",
    ];
    for s in cases {
        let text = v(s).to_canonical_string();
        assert!(
            !text.contains('e') && !text.contains('E'),
            "canonical string for {s:?} used exponent form: {text}"
        );
    }
}

#[test]
fn canonical_string_of_input_without_exponent_is_the_input() {
    for s in [
        "0",
        "1",
        "-1",
        "123.456",
        "-123.456",
        "0.0001",
        "-0.0001",
        "1000000000",
    ] {
        assert_eq!(
            v(s).to_canonical_string(),
            s,
            "canonical string changed a plain input"
        );
    }
}

#[test]
fn canonical_string_strips_leading_zeros_but_keeps_trailing_scale() {
    assert_eq!(v("0005").to_canonical_string(), "5");
    assert_eq!(v("-0005").to_canonical_string(), "-5");
    // Trailing fractional zeros are part of the value's declared scale and are
    // preserved verbatim by `to_plain_string`.
    assert_eq!(v("5.000").to_canonical_string(), "5.000");
    assert_eq!(v("-5.000").to_canonical_string(), "-5.000");
}

#[test]
fn every_zero_spelling_renders_as_plain_zero() {
    for s in ZERO_SPELLINGS {
        assert_eq!(
            v(s).to_canonical_string(),
            "0",
            "zero spelling {s:?} must render as \"0\""
        );
    }
}

#[test]
fn string_value_string_is_stable_for_powers_of_ten() {
    for k in 0..=200usize {
        let s = pow10(k);
        assert_eq!(v(&s).to_canonical_string(), s, "10^{k} string changed");
    }
}

#[test]
fn string_value_string_is_stable_for_tiny_fractions() {
    for k in 1..=60usize {
        let s = neg_pow10(k);
        assert_eq!(v(&s).to_canonical_string(), s, "10^-{k} string changed");
    }
}

#[test]
fn display_matches_to_canonical_string_over_corpus() {
    for s in CORPUS {
        let a = v(s);
        assert_eq!(
            format!("{a}"),
            a.to_canonical_string(),
            "Display and to_canonical_string diverged for {s:?}"
        );
    }
}

#[test]
fn decoded_value_string_reflects_the_column_scale() {
    // A value decoded at scale N carries scale N, so its plain string has
    // exactly N fractional digits.
    for scale in 1..=12u32 {
        let back = dec(&enc("1", scale), scale);
        let text = back.to_canonical_string();
        let frac = text.split_once('.').map(|(_, f)| f.len()).unwrap_or(0);
        assert_eq!(
            frac, scale as usize,
            "decoded 1 at scale {scale} rendered as {text:?}"
        );
    }
}

#[test]
fn string_roundtrip_through_bytes_preserves_numeric_value_even_when_text_changes() {
    for s in ["1", "1.5", "-1.5", "42", "-42", "0.25"] {
        for scale in [4u32, 10, 18] {
            let orig = v(s);
            let back = dec(&orig.to_canonical_bytes_at_scale(scale), scale);
            assert_eq!(orig, back, "{s:?} at scale {scale}");
            assert_eq!(
                v(&back.to_canonical_string()),
                orig,
                "re-parsing the rendered text of {s:?} at scale {scale} changed the value"
            );
        }
    }
}

// ===========================================================================
// D. cross-scale encode/decode
// ===========================================================================

#[test]
fn decoding_one_scale_too_low_multiplies_by_ten() {
    for s in ["1", "-1", "42", "-42", "123.456"] {
        let bytes = enc(s, 6);
        let right = dec(&bytes, 6);
        let wrong = dec(&bytes, 5);
        let expected = DecimalArbValue::from_bigdecimal(
            right.as_bigdecimal().clone() * bigdecimal::BigDecimal::from(10),
        );
        assert_eq!(
            wrong, expected,
            "decoding {s:?} one scale low must be exactly 10x"
        );
    }
}

#[test]
fn decoding_one_scale_too_high_divides_by_ten() {
    for s in ["1", "-1", "42", "-42"] {
        let bytes = enc(s, 6);
        let right = dec(&bytes, 6);
        let wrong = dec(&bytes, 7);
        assert_eq!(
            wrong.as_bigdecimal().clone() * bigdecimal::BigDecimal::from(10),
            right.as_bigdecimal().clone(),
            "decoding {s:?} one scale high must be exactly 1/10"
        );
    }
}

#[test]
fn cross_scale_decode_is_exactly_a_power_of_ten_factor() {
    for s in ["7", "-7", "1234", "-1234"] {
        let a = 12u32;
        let bytes = enc(s, a);
        for b in 0..=24u32 {
            let got = dec(&bytes, b);
            let expected_shift = a as i64 - b as i64;
            let factor = bigdecimal::BigDecimal::new(BigInt::from(1), -expected_shift);
            let expected = DecimalArbValue::from_bigdecimal(v(s).as_bigdecimal().clone() * factor);
            assert_eq!(
                got, expected,
                "encode({s:?}, {a}) decoded at {b} must be value * 10^{expected_shift}"
            );
        }
    }
}

#[test]
fn zero_decodes_to_zero_at_any_scale_regardless_of_encoding_scale() {
    let bytes = enc("0", 30);
    for scale in [0u32, 1, 7, 30, 65, 1000] {
        assert_eq!(
            dec(&bytes, scale),
            v("0"),
            "zero must stay zero at any scale"
        );
    }
}

#[test]
fn same_scale_decode_is_the_only_scale_that_reproduces_the_value() {
    // Non-zero values: decoding at any other scale must give a different value.
    for s in ["3", "-3", "0.5", "-0.5"] {
        let orig = v(s);
        let bytes = orig.to_canonical_bytes_at_scale(9);
        for scale in 0..=18u32 {
            let got = dec(&bytes, scale);
            if scale == 9 {
                assert_eq!(got, orig, "{s:?} must round trip at its own scale");
            } else {
                assert_ne!(
                    got, orig,
                    "{s:?} decoded at scale {scale} must not silently equal the original"
                );
            }
        }
    }
}

#[test]
fn array_adopted_with_a_mismatched_field_scale_silently_rescales() {
    // Documented hazard: the scale is not in the bytes, so adopting a
    // LargeBinaryArray with a Field whose scale differs from the one it was
    // built at silently changes every value. Pin the exact factor so a future
    // change that adds a guard is visible.
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "amt", 30, 4).unwrap();
    b.append_str("1.2345").unwrap();
    let (raw, _, _) = b.finish().into_inner();

    let field = DecimalArbType::field("amt", 30, 2, true).unwrap();
    let adopted = DecimalArbArray::try_from_array_and_field(raw, &field).unwrap();
    assert_eq!(adopted.scale(), 2);
    assert_eq!(
        adopted.value(0).unwrap(),
        Some(v("123.45")),
        "adopting at scale 2 bytes written at scale 4 rescales by 100"
    );
}

// ===========================================================================
// E. negative zero
// ===========================================================================

#[test]
fn negative_zero_parses_equal_to_positive_zero() {
    assert_eq!(v("-0"), v("0"));
    assert_eq!(v("-0.0"), v("0"));
    assert_eq!(v("-0.000000000000"), v("0"));
    assert_eq!(v("-0e10"), v("0"));
}

#[test]
fn negative_zero_hashes_equal_to_positive_zero() {
    for s in ZERO_SPELLINGS {
        assert_eq!(
            hash_of(&v(s)),
            hash_of(&v("0")),
            "zero spelling {s:?} must hash like 0"
        );
    }
}

#[test]
fn negative_zero_encodes_identically_to_positive_zero() {
    for scale in [0u32, 1, 6, 18, 38] {
        assert_eq!(
            enc("-0", scale),
            enc("0", scale),
            "-0 and 0 must be byte-identical at scale {scale}"
        );
    }
}

#[test]
fn negative_zero_sort_key_equals_positive_zero_sort_key() {
    let a = decimal_arb_to_sort_key(&enc("-0", 6));
    let b = decimal_arb_to_sort_key(&enc("0", 6));
    assert_eq!(a, b, "-0 and 0 must produce the same sort key");
}

#[test]
fn negative_value_rounding_to_zero_never_emits_the_negative_sign_byte() {
    // -0.4 at scale 0 rounds to 0; the encoder must not emit 0xFF with an
    // empty magnitude (which the decoder rejects) — that would make encoding
    // non-total.
    for s in ["-0.4", "-0.04", "-0.0000001", "-0.5", "-0.49999"] {
        let bytes = enc(s, 0);
        assert_eq!(
            bytes[0], 0x00,
            "{s:?} rounded to zero must carry the positive sign byte, got {bytes:02x?}"
        );
        assert_eq!(bytes, vec![0x00], "{s:?} at scale 0 must be the lone 0x00");
        // ... and the encoding must still be decodable.
        assert_eq!(dec(&bytes, 0), v("0"));
    }
}

#[test]
fn encoding_is_total_no_value_ever_produces_undecodable_bytes() {
    let inputs: Vec<String> = CORPUS
        .iter()
        .map(|s| s.to_string())
        .chain((1..=30).map(neg_pow10))
        .chain((1..=30).map(|k| format!("-{}", neg_pow10(k))))
        .chain(
            ["-0.5", "-0.05", "0.5", "0.05"]
                .iter()
                .map(|s| s.to_string()),
        )
        .collect();
    for s in &inputs {
        for scale in 0..=6u32 {
            let bytes = v(s).to_canonical_bytes_at_scale(scale);
            assert!(
                DecimalArbValue::from_canonical_bytes_at_scale(&bytes, scale).is_ok(),
                "encoding {s:?} at scale {scale} produced undecodable bytes {bytes:02x?}"
            );
        }
    }
}

#[test]
fn decoder_rejects_bare_negative_zero() {
    let err = DecimalArbValue::from_canonical_bytes_at_scale(&[0xFF], 0).unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("negative zero"),
        "error must name negative zero: {err}"
    );
}

#[test]
fn decoder_rejects_bare_negative_zero_at_every_scale() {
    for scale in [0u32, 1, 18, 65535] {
        assert!(
            DecimalArbValue::from_canonical_bytes_at_scale(&[0xFF], scale).is_err(),
            "0xFF alone must be rejected at scale {scale}"
        );
    }
}

#[test]
#[ignore = "FINDING: from_canonical_bytes_at_scale accepts [0xFF, 0x00...] (negative zero with a \
            padded magnitude); the emptiness check only counts bytes, so zero gets a second, \
            non-canonical byte spelling"]
fn decoder_rejects_negative_zero_with_padded_magnitude() {
    // `[0xFF, 0x00]` is a negative sign byte over a magnitude that is
    // numerically zero. It is exactly the encoding the decoder claims to
    // reject, but the emptiness check only looks at the byte count.
    for bytes in [
        vec![0xFFu8, 0x00],
        vec![0xFFu8, 0x00, 0x00],
        vec![0xFFu8, 0x00, 0x00, 0x00, 0x00],
    ] {
        let r = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 0);
        assert!(
            r.is_err(),
            "{bytes:02x?} is a negative-zero encoding and must be rejected, \
             but it decoded to {:?}",
            r.ok()
        );
    }
}

#[test]
#[ignore = "FINDING: [0xFF, 0x00] is accepted as zero but decimal_arb_to_sort_key gives it a \
            negative-prefixed key, so it sorts between -1 and 0 instead of at 0"]
fn accepted_payloads_sort_where_their_decoded_value_sorts() {
    // Consequence of the above. Any payload the decoder *accepts* must produce
    // the same sort key as the canonical encoding of the value it decodes to,
    // or ORDER BY places the row in the wrong position.
    for bytes in [vec![0xFFu8, 0x00], vec![0xFFu8, 0x00, 0x00]] {
        if let Ok(value) = DecimalArbValue::from_canonical_bytes_at_scale(&bytes, 0) {
            assert_eq!(
                decimal_arb_to_sort_key(&bytes),
                decimal_arb_to_sort_key(&value.to_canonical_bytes_at_scale(0)),
                "payload {bytes:02x?} is accepted and decodes to {value}, but its \
                 sort key differs from the canonical encoding of {value}"
            );
        }
    }
}

// ===========================================================================
// F. minimal form / leading-zero stripping
// ===========================================================================

#[test]
fn encoded_magnitude_never_starts_with_a_zero_byte() {
    let inputs: Vec<String> = CORPUS
        .iter()
        .map(|s| s.to_string())
        .chain((1..=80).map(pow10))
        .chain((1..=80).map(|k| format!("-{}", pow10(k))))
        .collect();
    for s in &inputs {
        for scale in [0u32, 1, 7, 18] {
            let bytes = v(s).to_canonical_bytes_at_scale(scale);
            if bytes.len() > 1 {
                assert_ne!(
                    bytes[1], 0x00,
                    "magnitude for {s:?} at scale {scale} has a leading zero byte: {bytes:02x?}"
                );
            }
        }
    }
}

#[test]
fn zero_is_exactly_one_byte() {
    for scale in [0u32, 1, 18, 65535] {
        assert_eq!(
            enc("0", scale).len(),
            1,
            "zero must encode to exactly one byte at scale {scale}"
        );
    }
}

#[test]
fn encoded_length_matches_minimal_big_endian_magnitude_length() {
    for s in CORPUS {
        let val = v(s);
        let bytes = val.to_canonical_bytes_at_scale(0);
        let scaled = val
            .as_bigdecimal()
            .with_scale_round(0, bigdecimal::RoundingMode::HalfEven);
        let (digits, _) = scaled.into_bigint_and_exponent();
        let (_, mag) = digits.to_bytes_be();
        let expected = if digits == BigInt::from(0) {
            0
        } else {
            mag.len()
        };
        assert_eq!(
            bytes.len() - 1,
            expected,
            "magnitude length for {s:?} is not minimal: {bytes:02x?}"
        );
    }
}

#[test]
fn magnitude_length_grows_at_the_expected_byte_boundaries() {
    let cases: &[(&str, usize)] = &[
        ("0", 0),
        ("1", 1),
        ("255", 1),
        ("256", 2),
        ("65535", 2),
        ("65536", 3),
        ("16777215", 3),
        ("16777216", 4),
        ("4294967295", 4),
        ("4294967296", 5),
    ];
    for (s, want) in cases {
        assert_eq!(
            enc(s, 0).len() - 1,
            *want,
            "{s:?} should need {want} magnitude byte(s)"
        );
        assert_eq!(
            enc(&format!("-{s}"), 0).len() - 1,
            *want,
            "-{s} should need {want} magnitude byte(s)"
        );
    }
}

#[test]
fn magnitude_length_is_monotone_non_decreasing_in_magnitude() {
    let mut last = 0usize;
    for k in 0..=120usize {
        let len = enc(&pow10(k), 0).len();
        assert!(
            len >= last,
            "magnitude length shrank going from 10^{} to 10^{k}",
            k.saturating_sub(1)
        );
        last = len;
    }
}

#[test]
fn positive_and_negative_of_same_magnitude_differ_only_in_the_sign_byte() {
    for s in CORPUS {
        if s.starts_with('-') || *s == "0" {
            continue;
        }
        let pos = enc(s, 10);
        let neg = enc(&format!("-{s}"), 10);
        assert_eq!(pos.len(), neg.len(), "length differs for ±{s}");
        assert_eq!(pos[0], 0x00);
        assert_eq!(neg[0], 0xFF);
        assert_eq!(&pos[1..], &neg[1..], "magnitudes differ for ±{s}");
    }
}

#[test]
fn decoder_accepts_non_minimal_magnitudes_and_agrees_with_the_minimal_form() {
    for (minimal, padded) in [
        (vec![0x00u8, 0x01], vec![0x00u8, 0x00, 0x01]),
        (vec![0x00u8, 0x01], vec![0x00u8, 0x00, 0x00, 0x00, 0x01]),
        (vec![0xFFu8, 0x01], vec![0xFFu8, 0x00, 0x01]),
        (vec![0x00u8, 0xFF], vec![0x00u8, 0x00, 0xFF]),
    ] {
        assert_eq!(
            dec(&minimal, 0),
            dec(&padded, 0),
            "non-minimal {padded:02x?} must decode like {minimal:02x?}"
        );
    }
}

#[test]
fn re_encoding_a_non_minimal_payload_produces_the_minimal_form() {
    let padded = [0x00u8, 0x00, 0x00, 0x2A];
    let value = dec(&padded, 0);
    assert_eq!(value, v("42"));
    assert_eq!(
        value.to_canonical_bytes_at_scale(0),
        vec![0x00, 0x2A],
        "re-encoding must canonicalize to minimal form"
    );
}

#[test]
fn sort_key_requires_minimal_form_to_be_order_preserving() {
    // Characterization: the sort-key encoding compares magnitude *length*
    // first, so it is only correct on minimal-form payloads. This documents
    // the precondition — a non-minimal payload sorts wrong.
    let one_padded = decimal_arb_to_sort_key(&[0x00, 0x00, 0x01]);
    let two_fifty_five = decimal_arb_to_sort_key(&[0x00, 0xFF]);
    assert!(
        one_padded > two_fifty_five,
        "documenting that non-minimal payloads break the sort key ordering"
    );
}

// ===========================================================================
// G. decoder rejection set
// ===========================================================================

#[test]
fn decoder_rejects_empty_input() {
    let err = DecimalArbValue::from_canonical_bytes_at_scale(&[], 0).unwrap_err();
    assert!(
        err.to_string().contains("empty"),
        "empty-input error must say so: {err}"
    );
}

#[test]
fn decoder_rejects_empty_input_at_every_scale() {
    for scale in [0u32, 1, 18, 65535] {
        assert!(
            DecimalArbValue::from_canonical_bytes_at_scale(&[], scale).is_err(),
            "empty input must be rejected at scale {scale}"
        );
    }
}

#[test]
fn decoder_rejects_every_sign_byte_other_than_00_and_ff() {
    for b in 0u16..=255 {
        let b = b as u8;
        if b == 0x00 || b == 0xFF {
            continue;
        }
        let r = DecimalArbValue::from_canonical_bytes_at_scale(&[b], 0);
        assert!(r.is_err(), "sign byte 0x{b:02x} alone must be rejected");
        let r = DecimalArbValue::from_canonical_bytes_at_scale(&[b, 0x01], 0);
        assert!(
            r.is_err(),
            "sign byte 0x{b:02x} with a magnitude must be rejected"
        );
    }
}

#[test]
fn invalid_sign_byte_error_names_the_byte() {
    for b in [0x01u8, 0x7F, 0x80, 0xFE] {
        let err = DecimalArbValue::from_canonical_bytes_at_scale(&[b, 0x01], 0).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("sign byte"),
            "error should say 'sign byte': {msg}"
        );
        assert!(
            msg.contains(&format!("{b:02x}")),
            "error should include the offending byte 0x{b:02x}: {msg}"
        );
    }
}

#[test]
fn decoder_never_panics_on_any_two_byte_input() {
    for a in 0u16..=255 {
        for b in 0u16..=255 {
            let _ = DecimalArbValue::from_canonical_bytes_at_scale(&[a as u8, b as u8], 6);
        }
    }
}

#[test]
fn decoder_never_panics_on_any_one_byte_input() {
    for a in 0u16..=255 {
        let _ = DecimalArbValue::from_canonical_bytes_at_scale(&[a as u8], 0);
    }
}

#[test]
fn decoder_accepts_long_magnitudes_without_truncation() {
    // 256-byte magnitude, all 0xFF.
    let mut bytes = vec![0x00u8];
    bytes.extend(std::iter::repeat_n(0xFFu8, 256));
    let value = dec(&bytes, 0);
    let expected: BigInt = (BigInt::from(1u8) << 2048usize) - BigInt::from(1u8);
    assert_eq!(
        value,
        v(&expected.to_string()),
        "long magnitudes must decode without truncation"
    );
    assert_eq!(
        value.to_canonical_bytes_at_scale(0),
        bytes,
        "long magnitudes must re-encode identically"
    );
}

#[test]
fn decoder_error_type_is_a_streamling_error_not_a_panic() {
    let r = DecimalArbValue::from_canonical_bytes_at_scale(&[0x42], 0);
    match r {
        Ok(_) => panic!("0x42 must not decode"),
        Err(e) => assert!(!e.to_string().is_empty(), "error message must be non-empty"),
    }
}

// ===========================================================================
// H. half-even rounding at encode time (documented defensive behaviour)
// ===========================================================================

#[test]
fn encode_applies_half_even_rounding_at_ties_scale_0() {
    let cases: &[(&str, &str)] = &[
        ("0.5", "0"),
        ("1.5", "2"),
        ("2.5", "2"),
        ("3.5", "4"),
        ("4.5", "4"),
        ("-0.5", "0"),
        ("-1.5", "-2"),
        ("-2.5", "-2"),
        ("-3.5", "-4"),
    ];
    for (input, want) in cases {
        assert_eq!(
            enc(input, 0),
            enc(want, 0),
            "half-even rounding of {input:?} should give {want:?}"
        );
    }
}

#[test]
fn encode_applies_half_even_rounding_at_scale_2() {
    let cases: &[(&str, &str)] = &[
        ("0.125", "0.12"),
        ("0.135", "0.14"),
        ("0.145", "0.14"),
        ("0.155", "0.16"),
        ("-0.125", "-0.12"),
        ("-0.135", "-0.14"),
    ];
    for (input, want) in cases {
        assert_eq!(
            enc(input, 2),
            enc(want, 2),
            "half-even rounding of {input:?} at scale 2 should give {want:?}"
        );
    }
}

#[test]
fn encode_rounding_is_symmetric_under_negation() {
    for s in [
        "0.5",
        "1.5",
        "2.5",
        "0.125",
        "0.135",
        "1.0000001",
        "999.9995",
    ] {
        let pos = enc(s, 2);
        let neg = enc(&format!("-{s}"), 2);
        assert_eq!(
            pos.len(),
            neg.len(),
            "rounding of ±{s} produced different magnitudes"
        );
        assert_eq!(&pos[1..], &neg[1..], "rounding of ±{s} is not symmetric");
    }
}

#[test]
fn rounding_at_encode_is_the_only_source_of_value_change() {
    // If the value's significant fractional digits fit the target scale, the
    // encode/decode pair must be exact. This is the contract `check_fits`
    // exists to guarantee.
    let inputs: Vec<String> = CORPUS
        .iter()
        .map(|s| s.to_string())
        .chain([big_mixed(), format!("-{}", big_mixed())])
        .collect();
    for s in &inputs {
        let val = v(s);
        let need = val.fractional_digit_count() as u32;
        for scale in need..(need + 5) {
            let back = dec(&val.to_canonical_bytes_at_scale(scale), scale);
            assert_eq!(
                val, back,
                "{s:?} needs scale {need} but changed when encoded at {scale}"
            );
        }
    }
}

#[test]
fn check_fits_success_implies_exact_roundtrip() {
    let combos: &[(u32, u32)] = &[(38, 0), (38, 10), (65, 20), (100, 35), (200, 100)];
    let inputs: Vec<String> = CORPUS
        .iter()
        .map(|s| s.to_string())
        .chain([big_mixed(), format!("-{}", big_mixed())])
        .collect();
    for (p, s) in combos {
        for input in &inputs {
            let val = v(input);
            if val.check_fits(*p, *s, "col").is_err() {
                continue;
            }
            let back = dec(&val.to_canonical_bytes_at_scale(*s), *s);
            assert_eq!(
                val, back,
                "check_fits({p},{s}) accepted {input:?} but the round trip was lossy"
            );
        }
    }
}

// ===========================================================================
// I. builder / array round trip
// ===========================================================================

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

#[test]
fn builder_round_trips_the_entire_corpus_at_scale_10() {
    let vals: Vec<Option<&str>> = CORPUS.iter().map(|s| Some(*s)).collect();
    let arr = build("amt", 60, 10, &vals);
    for (i, s) in CORPUS.iter().enumerate() {
        assert_eq!(
            arr.value(i).unwrap(),
            Some(v(s)),
            "corpus entry {s:?} did not survive the builder"
        );
    }
}

#[test]
fn builder_preserves_nulls_interleaved_with_values() {
    let arr = build(
        "amt",
        40,
        6,
        &[Some("1"), None, Some("-1"), None, None, Some("0")],
    );
    assert_eq!(arr.len(), 6);
    assert_eq!(arr.value(0).unwrap(), Some(v("1")));
    assert!(arr.is_null(1));
    assert_eq!(arr.value(2).unwrap(), Some(v("-1")));
    assert!(arr.is_null(3));
    assert!(arr.is_null(4));
    assert_eq!(arr.value(5).unwrap(), Some(v("0")));
}

#[test]
// Indexing pairs the Arrow accessor raw.value(i) with the spelling that produced
// it for the failure message; an iterator adaptor would lose that pairing.
#[allow(clippy::needless_range_loop)]
fn builder_produces_byte_identical_payloads_for_equal_spellings() {
    let vals: Vec<Option<&str>> = FIVE_SPELLINGS.iter().map(|s| Some(*s)).collect();
    let arr = build("amt", 30, 6, &vals);
    let raw = arr.as_inner();
    let first = raw.value(0).to_vec();
    for i in 1..raw.len() {
        assert_eq!(
            raw.value(i),
            first.as_slice(),
            "spelling {:?} produced different array bytes than {:?}",
            FIVE_SPELLINGS[i],
            FIVE_SPELLINGS[0]
        );
    }
}

#[test]
// Indexing pairs the Arrow accessor raw.value(i) with the spelling that produced
// it for the failure message; an iterator adaptor would lose that pairing.
#[allow(clippy::needless_range_loop)]
fn builder_produces_the_same_payload_for_all_zero_spellings() {
    let vals: Vec<Option<&str>> = ZERO_SPELLINGS.iter().map(|s| Some(*s)).collect();
    let arr = build("amt", 30, 6, &vals);
    let raw = arr.as_inner();
    for i in 0..raw.len() {
        assert_eq!(
            raw.value(i),
            &[0x00u8],
            "zero spelling {:?} produced {:02x?}",
            ZERO_SPELLINGS[i],
            raw.value(i)
        );
    }
}

#[test]
fn array_payload_matches_to_canonical_bytes_at_scale_exactly() {
    let vals: Vec<Option<&str>> = CORPUS.iter().map(|s| Some(*s)).collect();
    let arr = build("amt", 60, 10, &vals);
    let raw = arr.as_inner();
    for (i, s) in CORPUS.iter().enumerate() {
        assert_eq!(
            raw.value(i),
            v(s).to_canonical_bytes_at_scale(10).as_slice(),
            "array payload for {s:?} diverged from the value encoder"
        );
    }
}

#[test]
fn array_round_trips_through_into_inner_and_try_from_array_and_field() {
    let vals: Vec<Option<&str>> = CORPUS.iter().map(|s| Some(*s)).collect();
    let arr = build("amt", 60, 10, &vals);
    let (raw, p, s) = arr.into_inner();
    assert_eq!((p, s), (60, 10));
    let field = DecimalArbType::field("amt", p, s, true).unwrap();
    let adopted = DecimalArbArray::try_from_array_and_field(raw, &field).unwrap();
    for (i, want) in CORPUS.iter().enumerate() {
        assert_eq!(
            adopted.value(i).unwrap(),
            Some(v(want)),
            "adoption changed corpus entry {want:?}"
        );
    }
}

#[test]
fn string_array_round_trip_preserves_values_for_the_corpus() {
    let strings = StringArray::from(CORPUS.iter().map(|s| Some(*s)).collect::<Vec<_>>());
    let arr = DecimalArbArray::from_string_array(&strings, 60, 10, "amt").unwrap();
    let back = arr.to_string_array().unwrap();
    for (i, s) in CORPUS.iter().enumerate() {
        assert_eq!(
            v(back.value(i)),
            v(s),
            "string round trip changed the value of {s:?} (got {:?})",
            back.value(i)
        );
    }
}

#[test]
fn string_array_round_trip_preserves_nulls() {
    let strings = StringArray::from(vec![Some("1"), None, Some("-1"), None]);
    let arr = DecimalArbArray::from_string_array(&strings, 20, 4, "amt").unwrap();
    let back = arr.to_string_array().unwrap();
    assert_eq!(back.len(), 4);
    assert!(!back.is_null(0));
    assert!(back.is_null(1));
    assert!(!back.is_null(2));
    assert!(back.is_null(3));
}

#[test]
fn string_array_round_trip_is_a_fixed_point_after_one_pass() {
    let strings = StringArray::from(vec![Some("1"), Some("1.5"), Some("0"), Some("-42")]);
    let once = DecimalArbArray::from_string_array(&strings, 20, 4, "amt")
        .unwrap()
        .to_string_array()
        .unwrap();
    let twice = DecimalArbArray::from_string_array(&once, 20, 4, "amt")
        .unwrap()
        .to_string_array()
        .unwrap();
    for i in 0..once.len() {
        assert_eq!(
            once.value(i),
            twice.value(i),
            "string round trip is not a fixed point at row {i}"
        );
    }
}

#[test]
#[ignore = "FINDING: to_string_array renders zero as \"0\" while every other row of the same \
            scale-N column is padded to N fractional digits (canonicalize() drops the scale \
            for zero only)"]
fn every_row_of_a_scaled_column_renders_with_the_same_fractional_width() {
    // A decimal_arb column at scale 4 must render every non-null row with the
    // same number of fractional digits, or downstream string sinks / DISTINCT
    // over the rendered text will treat equal-shaped values inconsistently.
    let arr = build(
        "amt",
        20,
        4,
        &[Some("0"), Some("1"), Some("1.5"), Some("-2")],
    );
    let strings = arr.to_string_array().unwrap();
    let widths: Vec<usize> = (0..strings.len())
        .map(|i| {
            strings
                .value(i)
                .split_once('.')
                .map(|(_, f)| f.len())
                .unwrap_or(0)
        })
        .collect();
    let rendered: Vec<&str> = (0..strings.len()).map(|i| strings.value(i)).collect();
    assert!(
        widths.iter().all(|w| *w == widths[0]),
        "scale-4 column rendered rows with inconsistent fractional widths {widths:?}: {rendered:?}"
    );
}

#[test]
fn builder_rejects_values_that_would_be_silently_rounded() {
    // If the value needs more significant fractional digits than the column
    // scale, the builder must reject rather than let the encoder round.
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "amt", 20, 2).unwrap();
    assert!(
        b.append_str("1.005").is_err(),
        "builder must not accept a value the encoder would round"
    );
}

#[test]
fn builder_accepts_values_whose_extra_fractional_digits_are_zeros() {
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "amt", 20, 2).unwrap();
    b.append_str("1.00000000").unwrap();
    let arr = b.finish();
    assert_eq!(arr.value(0).unwrap(), Some(v("1")));
    assert_eq!(arr.as_inner().value(0), &[0x00, 0x64]); // 100 at scale 2
}

#[test]
fn array_value_decoding_is_stable_across_repeated_reads() {
    let arr = build("amt", 40, 8, &[Some("1.5"), Some("-1.5"), Some("0")]);
    for _ in 0..5 {
        assert_eq!(arr.value(0).unwrap(), Some(v("1.5")));
        assert_eq!(arr.value(1).unwrap(), Some(v("-1.5")));
        assert_eq!(arr.value(2).unwrap(), Some(v("0")));
    }
}

#[test]
fn empty_array_round_trips() {
    let arr = build("amt", 10, 2, &[]);
    assert_eq!(arr.len(), 0);
    assert!(arr.is_empty());
    let strings = arr.to_string_array().unwrap();
    assert_eq!(strings.len(), 0);
}

#[test]
fn array_of_only_nulls_round_trips() {
    let arr = build("amt", 10, 2, &[None, None, None]);
    assert_eq!(arr.len(), 3);
    for i in 0..3 {
        assert!(arr.is_null(i));
        assert_eq!(arr.value(i).unwrap(), None);
    }
    let strings = arr.to_string_array().unwrap();
    assert!((0..3).all(|i| strings.is_null(i)));
}

// ===========================================================================
// J. ordering / hashing consistency with the canonical bytes
// ===========================================================================

#[test]
fn sort_key_order_matches_value_order_over_the_corpus() {
    let scale = 10u32;
    for a in CORPUS {
        for b in CORPUS {
            let (va, vb) = (v(a), v(b));
            let ka = decimal_arb_to_sort_key(&va.to_canonical_bytes_at_scale(scale));
            let kb = decimal_arb_to_sort_key(&vb.to_canonical_bytes_at_scale(scale));
            assert_eq!(
                ka.cmp(&kb),
                va.cmp(&vb),
                "sort key order disagrees with value order for {a:?} vs {b:?}"
            );
        }
    }
}

#[test]
fn sort_key_order_matches_value_order_for_powers_of_ten() {
    let mut vals: Vec<DecimalArbValue> = Vec::new();
    for k in 0..=60usize {
        vals.push(v(&pow10(k)));
        vals.push(v(&format!("-{}", pow10(k))));
    }
    vals.push(v("0"));
    for a in &vals {
        for b in &vals {
            let ka = decimal_arb_to_sort_key(&a.to_canonical_bytes_at_scale(0));
            let kb = decimal_arb_to_sort_key(&b.to_canonical_bytes_at_scale(0));
            assert_eq!(
                ka.cmp(&kb),
                a.cmp(b),
                "sort key order disagrees for {a} vs {b}"
            );
        }
    }
}

#[test]
fn sort_key_is_a_function_of_the_value_not_the_spelling() {
    let want = decimal_arb_to_sort_key(&enc("5", 6));
    for s in FIVE_SPELLINGS {
        assert_eq!(
            decimal_arb_to_sort_key(&enc(s, 6)),
            want,
            "spelling {s:?} produced a different sort key"
        );
    }
}

#[test]
fn hash_is_a_function_of_the_value_not_the_spelling() {
    let want = hash_of(&v("5"));
    for s in FIVE_SPELLINGS {
        assert_eq!(hash_of(&v(s)), want, "spelling {s:?} hashed differently");
    }
}

#[test]
fn hash_survives_the_byte_round_trip() {
    for s in CORPUS {
        let orig = v(s);
        let back = dec(&orig.to_canonical_bytes_at_scale(20), 20);
        assert_eq!(
            hash_of(&orig),
            hash_of(&back),
            "hash of {s:?} changed across the byte round trip (hash-join keys would split)"
        );
    }
}

#[test]
fn equality_survives_the_byte_round_trip_for_every_pair() {
    for a in CORPUS {
        for b in CORPUS {
            let (ra, rb) = (dec(&enc(a, 15), 15), dec(&enc(b, 15), 15));
            assert_eq!(
                ra == rb,
                v(a) == v(b),
                "equality of {a:?} vs {b:?} changed across the round trip"
            );
        }
    }
}

#[test]
fn ordering_survives_the_byte_round_trip_for_every_pair() {
    for a in CORPUS {
        for b in CORPUS {
            let (ra, rb) = (dec(&enc(a, 15), 15), dec(&enc(b, 15), 15));
            assert_eq!(
                ra.cmp(&rb),
                v(a).cmp(&v(b)),
                "ordering of {a:?} vs {b:?} changed across the round trip"
            );
        }
    }
}

#[test]
fn ordering_is_a_total_order_over_the_corpus() {
    let vals: Vec<DecimalArbValue> = CORPUS.iter().map(|s| v(s)).collect();
    for a in &vals {
        assert_eq!(a.cmp(a), std::cmp::Ordering::Equal, "{a} is not reflexive");
        for b in &vals {
            assert_eq!(
                a.cmp(b),
                b.cmp(a).reverse(),
                "cmp is not antisymmetric for {a} vs {b}"
            );
        }
    }
}

#[test]
fn ordering_is_transitive_over_the_corpus() {
    let mut vals: Vec<DecimalArbValue> = CORPUS.iter().map(|s| v(s)).collect();
    vals.sort();
    for w in vals.windows(2) {
        assert!(
            w[0] <= w[1],
            "sorted corpus is not ordered: {} > {}",
            w[0],
            w[1]
        );
    }
}

#[test]
fn sorting_by_sort_key_yields_the_same_permutation_as_sorting_by_value() {
    let mut by_value: Vec<DecimalArbValue> = CORPUS.iter().map(|s| v(s)).collect();
    by_value.sort();
    let mut by_key: Vec<(Vec<u8>, DecimalArbValue)> = CORPUS
        .iter()
        .map(|s| {
            let val = v(s);
            (
                decimal_arb_to_sort_key(&val.to_canonical_bytes_at_scale(10)),
                val,
            )
        })
        .collect();
    by_key.sort_by(|a, b| a.0.cmp(&b.0));
    let by_key: Vec<DecimalArbValue> = by_key.into_iter().map(|(_, v)| v).collect();
    assert_eq!(
        by_value, by_key,
        "sorting by sort key must reproduce numeric ordering"
    );
}

// ===========================================================================
// K. digit-count invariants that the round trip depends on
// ===========================================================================

#[test]
fn integer_digit_count_is_stable_across_the_byte_round_trip_at_matching_scale() {
    for s in CORPUS {
        let orig = v(s);
        let scale = orig.fractional_digit_count() as u32;
        let back = dec(&orig.to_canonical_bytes_at_scale(scale), scale);
        assert_eq!(
            orig.integer_digit_count(),
            back.integer_digit_count(),
            "integer digit count of {s:?} changed across the round trip"
        );
    }
}

#[test]
fn fractional_digit_count_is_stable_across_the_byte_round_trip() {
    for s in CORPUS {
        let orig = v(s);
        for scale in [10u32, 20, 30] {
            let back = dec(&orig.to_canonical_bytes_at_scale(scale), scale);
            assert_eq!(
                orig.fractional_digit_count(),
                back.fractional_digit_count(),
                "significant fractional digit count of {s:?} changed at scale {scale}"
            );
        }
    }
}

#[test]
fn integer_digit_count_matches_the_string_form() {
    for s in CORPUS {
        let val = v(s);
        let text = val.to_canonical_string();
        let int_part = text.trim_start_matches('-').split('.').next().unwrap();
        let expected = if int_part == "0" { 0 } else { int_part.len() };
        assert_eq!(
            val.integer_digit_count(),
            expected as u64,
            "integer_digit_count disagrees with the rendered form of {s:?} ({text})"
        );
    }
}

#[test]
fn integer_digit_count_of_powers_of_ten_is_k_plus_one() {
    for k in 0..=200usize {
        assert_eq!(
            v(&pow10(k)).integer_digit_count(),
            (k + 1) as u64,
            "10^{k} should have {} integer digits",
            k + 1
        );
    }
}

#[test]
fn fractional_digit_count_of_tiny_powers_is_k() {
    for k in 1..=60usize {
        assert_eq!(
            v(&neg_pow10(k)).fractional_digit_count(),
            k as u64,
            "10^-{k} should have {k} significant fractional digits"
        );
    }
}

#[test]
fn zero_reports_zero_digits_in_every_spelling() {
    for s in ZERO_SPELLINGS {
        let val = v(s);
        assert_eq!(val.integer_digit_count(), 0, "{s:?} integer digits");
        assert_eq!(val.fractional_digit_count(), 0, "{s:?} fractional digits");
    }
}

#[test]
fn digit_counts_are_spelling_independent() {
    for s in FIVE_SPELLINGS {
        let val = v(s);
        assert_eq!(val.integer_digit_count(), 1, "{s:?} integer digits");
        assert_eq!(val.fractional_digit_count(), 0, "{s:?} fractional digits");
    }
}

// ===========================================================================
// L. from_bigint_and_scale <-> canonical bytes agreement
// ===========================================================================

#[test]
fn from_bigint_and_scale_agrees_with_decoding_the_same_magnitude() {
    for magnitude in [0i64, 1, 2, 255, 256, 65535, 65536, 1_000_000, i64::MAX] {
        for scale in [0u32, 1, 6, 18] {
            let from_components =
                DecimalArbValue::from_bigint_and_scale(BigInt::from(magnitude), scale as i64);
            let bytes = if magnitude == 0 {
                vec![0x00u8]
            } else {
                let mut b = vec![0x00u8];
                b.extend_from_slice(&BigInt::from(magnitude).to_bytes_be().1);
                b
            };
            assert_eq!(
                dec(&bytes, scale),
                from_components,
                "magnitude {magnitude} at scale {scale} disagrees"
            );
        }
    }
}

#[test]
fn from_bigint_and_scale_negative_agrees_with_decoding() {
    for magnitude in [1i64, 2, 255, 256, 65536, 1_000_000] {
        for scale in [0u32, 3, 12] {
            let from_components =
                DecimalArbValue::from_bigint_and_scale(BigInt::from(-magnitude), scale as i64);
            let mut bytes = vec![0xFFu8];
            bytes.extend_from_slice(&BigInt::from(magnitude).to_bytes_be().1);
            assert_eq!(
                dec(&bytes, scale),
                from_components,
                "-{magnitude} at scale {scale} disagrees"
            );
        }
    }
}

#[test]
fn from_bigint_and_scale_with_negative_scale_round_trips() {
    // Negative scale means implicit trailing zeros: BigInt(1) at scale -3 is 1000.
    let val = DecimalArbValue::from_bigint_and_scale(BigInt::from(1), -3);
    assert_eq!(val, v("1000"));
    assert_eq!(val.to_canonical_string(), "1000");
    assert_eq!(dec(&val.to_canonical_bytes_at_scale(0), 0), val);
}

#[test]
fn from_bigint_and_scale_zero_is_canonical_zero_at_any_scale() {
    for scale in [-10i64, -1, 0, 1, 18, 1000] {
        let val = DecimalArbValue::from_bigint_and_scale(BigInt::from(0), scale);
        assert_eq!(val, v("0"), "zero at scale {scale}");
        assert_eq!(val.to_canonical_string(), "0");
        assert_eq!(val.to_canonical_bytes_at_scale(6), vec![0x00]);
    }
}

#[test]
fn from_bigdecimal_matches_from_str_over_corpus() {
    for s in CORPUS {
        let bd = bigdecimal::BigDecimal::from_str(s).unwrap();
        assert_eq!(
            DecimalArbValue::from_bigdecimal(bd.clone()),
            v(s),
            "from_bigdecimal disagrees with from_str for {s:?}"
        );
        assert_eq!(
            DecimalArbValue::from_bigdecimal(bd).to_canonical_bytes_at_scale(10),
            enc(s, 10),
        );
    }
}

#[test]
fn into_bigdecimal_round_trips_back_through_from_bigdecimal() {
    for s in CORPUS {
        let a = v(s);
        let bd = a.clone().into_bigdecimal();
        assert_eq!(DecimalArbValue::from_bigdecimal(bd), a, "{s:?}");
    }
}

#[test]
fn as_bigdecimal_is_numerically_equal_to_the_parsed_input() {
    for s in CORPUS {
        let a = v(s);
        assert_eq!(
            *a.as_bigdecimal(),
            bigdecimal::BigDecimal::from_str(s).unwrap(),
            "as_bigdecimal diverged for {s:?}"
        );
    }
}

// ===========================================================================
// M. parser rejection / acceptance surface
// ===========================================================================

#[test]
fn from_str_rejects_obviously_malformed_input() {
    let mut accepted = Vec::new();
    for s in [
        "", " ", "abc", "1.2.3", "--1", "1-", "+-1", "0x10", "1,000", "NaN", "inf", "Infinity",
        ".", "-", "+", "e5", "1e", "1e+", "1 2", "_1", "_", "0b1", "٣", "1٫5",
    ] {
        if let Ok(v) = DecimalArbValue::from_str(s) {
            accepted.push(format!("{s:?} -> {v}"));
        }
    }
    assert!(
        accepted.is_empty(),
        "the strict parser accepted malformed input: {accepted:?}"
    );
}

#[test]
#[ignore = "FINDING: DecimalArbValue::from_str silently accepts '_' digit separators, so \
            string ingest turns '1_000' into 1000 instead of erroring"]
fn from_str_rejects_underscore_digit_separators() {
    // `from_str` is documented as "Strict — rejects malformed input", and it is
    // the ingest path for `DecimalArbArrayBuilder::append_str`,
    // `DecimalArbArray::from_string_array`, and the
    // `to_decimal_arb_from_string(text, p, s)` SQL UDF. A malformed upstream
    // text value must surface as an error, not be silently coerced.
    let mut accepted = Vec::new();
    for s in ["1_000", "1__0", "1_.5", "1._5", "1_e2", "1_0_0_0"] {
        if let Ok(v) = DecimalArbValue::from_str(s) {
            accepted.push(format!("{s:?} -> {v}"));
        }
    }
    assert!(
        accepted.is_empty(),
        "underscore-separated text must not parse as a number: {accepted:?}"
    );
}

#[test]
fn from_str_rejects_surrounding_whitespace() {
    for s in [" 1", "1 ", "\t1", "1\n", " 1 "] {
        assert!(
            DecimalArbValue::from_str(s).is_err(),
            "{s:?} (whitespace-padded) must be rejected"
        );
    }
}

#[test]
fn from_str_error_message_quotes_the_offending_input() {
    let err = DecimalArbValue::from_str("not-a-number").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("not-a-number"),
        "parse error should quote the input: {msg}"
    );
}

#[test]
fn from_str_accepts_every_spelling_in_the_five_corpus() {
    for s in FIVE_SPELLINGS {
        assert!(
            DecimalArbValue::from_str(s).is_ok(),
            "{s:?} should be an accepted spelling of 5"
        );
    }
}

#[test]
fn from_str_accepts_every_spelling_in_the_zero_corpus() {
    for s in ZERO_SPELLINGS {
        assert!(
            DecimalArbValue::from_str(s).is_ok(),
            "{s:?} should be an accepted spelling of 0"
        );
    }
}

// ===========================================================================
// N. field metadata round trip (the scale the codec depends on)
// ===========================================================================

fn arb_field_with_raw_metadata(raw: &str) -> Field {
    let mut md = std::collections::HashMap::new();
    md.insert(
        DecimalArbType::EXTENSION_NAME_KEY.to_string(),
        DecimalArbType::EXTENSION_NAME.to_string(),
    );
    md.insert(
        DecimalArbType::EXTENSION_METADATA_KEY.to_string(),
        raw.to_string(),
    );
    Field::new("amt", DataType::LargeBinary, true).with_metadata(md)
}

#[test]
fn field_metadata_round_trips_precision_and_scale_over_a_grid() {
    for p in [1u32, 2, 38, 76, 100, 1000, MAX_PRECISION] {
        for s in [0u32, 1, p / 2, p] {
            let f = DecimalArbType::field("amt", p, s, true).unwrap();
            assert_eq!(
                DecimalArbType::precision_scale_from_field(&f),
                Some((p, s)),
                "({p},{s}) did not round trip through field metadata"
            );
        }
    }
}

#[test]
fn field_metadata_parser_accepts_reversed_key_order() {
    let f = arb_field_with_raw_metadata(r#"{"scale":2,"precision":10}"#);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((10, 2)),
        "key order must not matter"
    );
}

#[test]
fn field_metadata_parser_tolerates_whitespace() {
    let f = arb_field_with_raw_metadata(r#"{ "precision" : 10 , "scale" : 2 }"#);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((10, 2)),
        "whitespace must be tolerated"
    );
}

#[test]
fn field_metadata_parser_rejects_bad_payloads() {
    for raw in [
        "",
        "{}",
        "not json",
        r#"{"precision":10}"#,
        r#"{"scale":2}"#,
        r#"{"precision":0,"scale":0}"#,
        r#"{"precision":10,"scale":11}"#,
        r#"{"precision":-1,"scale":0}"#,
        r#"{"precision":4294967296,"scale":0}"#,
        r#"{"precision":10,"scale":2,"extra":3}"#,
        r#"{"precision":10,"scale":2,}"#,
        r#"{"precision":1.5,"scale":0}"#,
    ] {
        let f = arb_field_with_raw_metadata(raw);
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&f),
            None,
            "malformed metadata {raw:?} must not yield a precision/scale"
        );
    }
}

#[test]
fn field_metadata_precision_above_max_is_rejected() {
    assert!(DecimalArbType::field("amt", MAX_PRECISION + 1, 0, true).is_err());
    assert!(DecimalArbType::metadata(MAX_PRECISION + 1, 0).is_err());
    assert!(DecimalArbType::field("amt", MAX_PRECISION, MAX_PRECISION, true).is_ok());
}

#[test]
fn field_metadata_serialization_is_byte_stable_across_the_grid() {
    for (p, s) in [(1u32, 0u32), (38, 10), (100, 18), (MAX_PRECISION, 0)] {
        let md = DecimalArbType::metadata(p, s).unwrap();
        assert_eq!(
            md.get(DecimalArbType::EXTENSION_METADATA_KEY).unwrap(),
            &format!(r#"{{"precision":{p},"scale":{s}}}"#),
            "metadata layout changed for ({p},{s}); at-rest fields would stop parsing"
        );
    }
}

#[test]
fn array_adoption_needs_both_storage_type_and_metadata() {
    let (raw, _, _) = build("amt", 10, 2, &[Some("1")]).into_inner();
    let plain = Field::new("amt", DataType::LargeBinary, true);
    assert!(
        DecimalArbArray::try_from_array_and_field(raw, &plain).is_err(),
        "a bare LargeBinary field must not be adoptable as decimal_arb"
    );
}

// ===========================================================================
// O. exhaustive small-value sweeps
// ===========================================================================

#[test]
fn every_integer_in_minus_1000_to_1000_round_trips_at_scale_0() {
    for n in -1000i64..=1000 {
        let s = n.to_string();
        let orig = v(&s);
        let back = dec(&orig.to_canonical_bytes_at_scale(0), 0);
        assert_eq!(orig, back, "{n} broke at scale 0");
        assert_eq!(back.to_canonical_string(), s, "{n} string form changed");
    }
}

#[test]
fn every_integer_in_minus_500_to_500_round_trips_at_scale_9() {
    for n in -500i64..=500 {
        let s = n.to_string();
        let orig = v(&s);
        let back = dec(&orig.to_canonical_bytes_at_scale(9), 9);
        assert_eq!(orig, back, "{n} broke at scale 9");
    }
}

#[test]
fn every_two_decimal_value_in_minus_5_to_5_round_trips() {
    for n in -500i64..=500 {
        let sign = if n < 0 { "-" } else { "" };
        let a = n.abs();
        let s = format!("{sign}{}.{:02}", a / 100, a % 100);
        let orig = v(&s);
        let back = dec(&orig.to_canonical_bytes_at_scale(2), 2);
        assert_eq!(orig, back, "{s} broke at scale 2");
    }
}

#[test]
fn every_integer_in_minus_300_to_300_has_distinct_bytes() {
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    for n in -300i64..=300 {
        let bytes = enc(&n.to_string(), 0);
        assert!(
            seen.insert(bytes.clone()),
            "byte collision at {n}: {bytes:02x?}"
        );
    }
    assert_eq!(seen.len(), 601);
}

#[test]
fn every_integer_in_minus_300_to_300_sorts_correctly_by_sort_key() {
    let mut pairs: Vec<(Vec<u8>, i64)> = (-300i64..=300)
        .map(|n| (decimal_arb_to_sort_key(&enc(&n.to_string(), 0)), n))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let ns: Vec<i64> = pairs.into_iter().map(|(_, n)| n).collect();
    let expected: Vec<i64> = (-300i64..=300).collect();
    assert_eq!(
        ns, expected,
        "sort key ordering broke on a dense integer sweep"
    );
}

#[test]
fn scales_0_through_64_all_round_trip_a_fixed_value() {
    let orig = v("123.456");
    for scale in 3..=64u32 {
        let back = dec(&orig.to_canonical_bytes_at_scale(scale), scale);
        assert_eq!(orig, back, "123.456 broke at scale {scale}");
    }
}

#[test]
fn scales_0_through_64_all_round_trip_a_fixed_negative_value() {
    let orig = v("-123.456");
    for scale in 3..=64u32 {
        let back = dec(&orig.to_canonical_bytes_at_scale(scale), scale);
        assert_eq!(orig, back, "-123.456 broke at scale {scale}");
        assert_eq!(orig.to_canonical_bytes_at_scale(scale)[0], 0xFF);
    }
}

#[test]
fn magnitude_bytes_are_big_endian() {
    // 0x0102 = 258, 0x010203 = 66051 — pins byte order so a future
    // little-endian slip is caught.
    assert_eq!(enc("258", 0), vec![0x00, 0x01, 0x02]);
    assert_eq!(enc("66051", 0), vec![0x00, 0x01, 0x02, 0x03]);
    assert_eq!(enc("-258", 0), vec![0xFF, 0x01, 0x02]);
    assert_eq!(dec(&[0x00, 0x01, 0x02], 0), v("258"));
    assert_eq!(dec(&[0xFF, 0x01, 0x02, 0x03], 0), v("-66051"));
}

#[test]
fn scale_shifts_the_magnitude_by_exactly_a_factor_of_ten_per_step() {
    for scale in 0..=20u32 {
        let bytes = enc("7", scale);
        let magnitude = BigInt::from_bytes_be(num_bigint::Sign::Plus, &bytes[1..]);
        let expected = BigInt::from(7) * BigInt::from(10).pow(scale);
        assert_eq!(
            magnitude, expected,
            "encoding 7 at scale {scale} should store 7*10^{scale}"
        );
    }
}

#[test]
fn negative_scale_shift_matches_for_negative_values() {
    for scale in 0..=20u32 {
        let bytes = enc("-7", scale);
        assert_eq!(bytes[0], 0xFF);
        let magnitude = BigInt::from_bytes_be(num_bigint::Sign::Plus, &bytes[1..]);
        let expected = BigInt::from(7) * BigInt::from(10).pow(scale);
        assert_eq!(magnitude, expected, "encoding -7 at scale {scale}");
    }
}
