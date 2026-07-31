//! Adversarial agent 09 — `DecimalArbType` / `DecimalArbExtension` metadata handling.
//!
//! Focus: the Arrow `ExtensionType` impl (`serialize_metadata` /
//! `deserialize_metadata` / `try_new` / `supports_data_type`), the hand-rolled
//! `{"precision":N,"scale":M}` parser behind it, `(precision, scale)` boundary
//! validation, `NativeIntKind::parse`, the `native_int_kind` field-metadata
//! hint round-trip, and `is_decimal_arb_metadata` against lookalike metadata.
//!
//! Every test asserts one property and says which invariant broke.

use std::collections::HashMap;
use std::str::FromStr;

use arrow::array::LargeBinaryArray;
use arrow_schema::extension::ExtensionType;
use arrow_schema::{ArrowError, DataType, Field};

use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbParams, DecimalArbType, DecimalArbValue, MAX_PRECISION,
    NativeIntKind,
};
// `DecimalArbExtension` and `DecimalArbArray` come from the same module.
use streamling_common::types::decimal_arb::{DecimalArbArray, DecimalArbExtension};

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

/// Run the extension's metadata deserializer on a raw string payload.
fn de(raw: &str) -> Result<DecimalArbParams, ArrowError> {
    <DecimalArbExtension as ExtensionType>::deserialize_metadata(Some(raw))
}

/// Run the extension's metadata serializer for a valid `(precision, scale)`.
fn ser(precision: u32, scale: u32) -> String {
    DecimalArbExtension::new(precision, scale)
        .expect("valid precision/scale")
        .serialize_metadata()
        .expect("decimal_arb always serializes metadata")
}

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

/// A metadata map that looks exactly like a real decimal_arb field's.
fn good_map(precision: u32, scale: u32) -> HashMap<String, String> {
    DecimalArbType::metadata(precision, scale).expect("valid precision/scale")
}

// =====================================================================
// A. serialize_metadata — byte-stable layout
// =====================================================================

#[test]
fn serialize_metadata_emits_exact_historical_layout() {
    assert_eq!(
        ser(38, 9),
        r#"{"precision":38,"scale":9}"#,
        "on-wire metadata layout changed; at-rest decimal_arb fields would stop parsing"
    );
}

#[test]
fn serialize_metadata_has_no_whitespace() {
    let s = ser(100, 18);
    assert!(
        !s.contains(' ') && !s.contains('\n') && !s.contains('\t'),
        "metadata payload must be whitespace-free for byte-stable schema comparison, got {s:?}"
    );
}

#[test]
fn serialize_metadata_orders_precision_before_scale() {
    let s = ser(10, 2);
    let p = s.find("precision").expect("precision key present");
    let sc = s.find("scale").expect("scale key present");
    assert!(
        p < sc,
        "key order must stay precision-then-scale for byte-identical schemas, got {s:?}"
    );
}

#[test]
fn serialize_metadata_is_never_none() {
    for (p, s) in [(1u32, 0u32), (1, 1), (65535, 0), (65535, 65535)] {
        assert!(
            DecimalArbExtension::new(p, s)
                .unwrap()
                .serialize_metadata()
                .is_some(),
            "serialize_metadata returned None for ({p},{s}); Arrow would clear the metadata key"
        );
    }
}

#[test]
fn serialize_metadata_at_max_precision_is_exact() {
    assert_eq!(
        ser(MAX_PRECISION, MAX_PRECISION),
        r#"{"precision":65535,"scale":65535}"#,
        "MAX_PRECISION metadata layout drifted"
    );
}

#[test]
fn serialize_metadata_zero_scale_is_not_elided() {
    assert_eq!(
        ser(20, 0),
        r#"{"precision":20,"scale":0}"#,
        "scale=0 must still be emitted explicitly; omitting it breaks the parser"
    );
}

// =====================================================================
// B. serialize -> deserialize round-trip
// =====================================================================

#[test]
fn metadata_round_trips_for_a_broad_matrix() {
    let cases: &[(u32, u32)] = &[
        (1, 0),
        (1, 1),
        (2, 0),
        (2, 1),
        (2, 2),
        (10, 0),
        (10, 5),
        (10, 10),
        (38, 9),
        (76, 38),
        (100, 18),
        (1000, 999),
        (65534, 0),
        (65535, 0),
        (65535, 1),
        (65535, 65534),
        (65535, 65535),
    ];
    for &(p, s) in cases {
        let parsed =
            de(&ser(p, s)).unwrap_or_else(|e| panic!("round-trip failed for ({p},{s}): {e}"));
        assert_eq!(
            (parsed.precision, parsed.scale),
            (p, s),
            "serialize/deserialize is not identity for ({p},{s})"
        );
    }
}

#[test]
fn round_trip_through_field_preserves_precision_and_scale() {
    for &(p, s) in &[(1u32, 0u32), (1, 1), (65535, 65535), (100, 18)] {
        let f = DecimalArbType::field("amount", p, s, true).unwrap();
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&f),
            Some((p, s)),
            "field metadata round-trip lost (precision, scale) for ({p},{s})"
        );
    }
}

#[test]
fn round_trip_through_arrow_extension_api_preserves_params() {
    let f = DecimalArbType::field("amount", 4242, 42, false).unwrap();
    let ext = f
        .try_extension_type::<DecimalArbExtension>()
        .expect("field built by field() must be readable via the Arrow extension API");
    assert_eq!(
        (ext.precision(), ext.scale()),
        (4242, 42),
        "Arrow ExtensionType round-trip lost params"
    );
}

#[test]
fn deserialized_params_equal_the_extensions_own_metadata() {
    let ext = DecimalArbExtension::new(77, 30).unwrap();
    let parsed = de(&ext.serialize_metadata().unwrap()).unwrap();
    assert_eq!(
        &parsed,
        ExtensionType::metadata(&ext),
        "deserialize_metadata does not reproduce the extension's own Metadata value"
    );
}

#[test]
fn round_trip_is_stable_under_repetition() {
    let mut raw = ser(1234, 56);
    for i in 0..5 {
        let p = de(&raw).unwrap_or_else(|e| panic!("iteration {i} failed: {e}"));
        let next = DecimalArbExtension::new(p.precision, p.scale)
            .unwrap()
            .serialize_metadata()
            .unwrap();
        assert_eq!(
            next, raw,
            "metadata is not a fixed point under repeated de/serialize at iteration {i}"
        );
        raw = next;
    }
}

// =====================================================================
// C. deserialize_metadata — missing / empty / structural garbage
// =====================================================================

#[test]
fn deserialize_metadata_rejects_none() {
    assert!(
        <DecimalArbExtension as ExtensionType>::deserialize_metadata(None).is_err(),
        "absent extension metadata must be an error, not a silent default"
    );
}

#[test]
fn deserialize_metadata_rejects_empty_string() {
    assert!(
        de("").is_err(),
        "empty metadata payload must be rejected, not treated as {{}}"
    );
}

#[test]
fn deserialize_metadata_rejects_whitespace_only() {
    for raw in ["   ", "\t", "\n", " \r\n \t "] {
        assert!(
            de(raw).is_err(),
            "whitespace-only metadata {raw:?} must be rejected"
        );
    }
}

#[test]
fn deserialize_metadata_rejects_empty_object() {
    assert!(
        de("{}").is_err(),
        "'{{}}' carries neither precision nor scale and must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_missing_precision_key() {
    assert!(
        de(r#"{"scale":2}"#).is_err(),
        "metadata without 'precision' must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_missing_scale_key() {
    assert!(
        de(r#"{"precision":10}"#).is_err(),
        "metadata without 'scale' must be rejected (scale must never default silently)"
    );
}

#[test]
fn deserialize_metadata_rejects_unclosed_object() {
    assert!(
        de(r#"{"precision":10,"scale":2"#).is_err(),
        "unterminated JSON object must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_unopened_object() {
    assert!(
        de(r#""precision":10,"scale":2}"#).is_err(),
        "payload without a leading '{{' must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_bare_pairs_without_braces() {
    assert!(
        de(r#""precision":10,"scale":2"#).is_err(),
        "brace-less key/value list must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_json_array() {
    assert!(de("[10,2]").is_err(), "a JSON array is not valid metadata");
}

#[test]
fn deserialize_metadata_rejects_bare_number() {
    assert!(de("10").is_err(), "a bare number is not valid metadata");
}

#[test]
fn deserialize_metadata_rejects_trailing_comma() {
    assert!(
        de(r#"{"precision":10,"scale":2,}"#).is_err(),
        "trailing comma yields an empty field and must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_leading_comma() {
    assert!(
        de(r#"{,"precision":10,"scale":2}"#).is_err(),
        "leading comma yields an empty field and must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_double_comma() {
    assert!(
        de(r#"{"precision":10,,"scale":2}"#).is_err(),
        "empty field between commas must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_field_without_colon() {
    assert!(
        de(r#"{"precision" 10,"scale":2}"#).is_err(),
        "field without ':' separator must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_nested_object_value() {
    assert!(
        de(r#"{"precision":{"a":1},"scale":2}"#).is_err(),
        "a nested object where a u32 is expected must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_null_values() {
    assert!(
        de(r#"{"precision":null,"scale":null}"#).is_err(),
        "null precision/scale must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_bom_prefixed_payload() {
    let raw = format!("\u{feff}{}", ser(10, 2));
    assert!(
        de(&raw).is_err(),
        "a BOM-prefixed payload is not the canonical layout and must be rejected, not silently accepted"
    );
}

#[test]
fn deserialize_metadata_rejects_embedded_nul() {
    assert!(
        de("{\"precision\":1\u{0}0,\"scale\":2}").is_err(),
        "NUL inside a numeric value must be rejected"
    );
}

#[test]
fn deserialize_metadata_does_not_panic_on_long_garbage() {
    let raw = format!("{{{}}}", "a".repeat(20_000));
    assert!(
        de(&raw).is_err(),
        "a very long garbage payload must return an error, not panic"
    );
}

#[test]
fn deserialize_metadata_does_not_panic_on_many_commas() {
    let raw = format!("{{{}}}", ",".repeat(10_000));
    assert!(
        de(&raw).is_err(),
        "a comma-only payload must return an error, not panic"
    );
}

#[test]
fn deserialize_metadata_does_not_panic_on_many_colons() {
    let raw = format!("{{{}}}", ":".repeat(10_000));
    assert!(
        de(&raw).is_err(),
        "a colon-only payload must return an error, not panic"
    );
}

// =====================================================================
// D. deserialize_metadata — non-numeric / out-of-range values
// =====================================================================

#[test]
fn deserialize_metadata_rejects_non_numeric_precision() {
    assert!(
        de(r#"{"precision":abc,"scale":2}"#).is_err(),
        "non-numeric precision must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_quoted_numeric_values() {
    assert!(
        de(r#"{"precision":"10","scale":"2"}"#).is_err(),
        "string-typed precision/scale is not the canonical layout and must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_negative_precision() {
    assert!(
        de(r#"{"precision":-1,"scale":0}"#).is_err(),
        "negative precision must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_negative_scale() {
    assert!(
        de(r#"{"precision":10,"scale":-1}"#).is_err(),
        "negative scale must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_negative_zero_scale() {
    assert!(
        de(r#"{"precision":10,"scale":-0}"#).is_err(),
        "'-0' is not a canonical u32 and must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_fractional_precision() {
    assert!(
        de(r#"{"precision":10.5,"scale":2}"#).is_err(),
        "fractional precision must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_exponent_notation() {
    assert!(
        de(r#"{"precision":1e2,"scale":0}"#).is_err(),
        "exponent-notation precision must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_hex_precision() {
    assert!(
        de(r#"{"precision":0x10,"scale":0}"#).is_err(),
        "hex precision must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_u32_overflow() {
    assert!(
        de(r#"{"precision":4294967296,"scale":0}"#).is_err(),
        "precision above u32::MAX must be rejected, not wrapped"
    );
}

#[test]
fn deserialize_metadata_rejects_u32_max_precision() {
    assert!(
        de(r#"{"precision":4294967295,"scale":0}"#).is_err(),
        "u32::MAX precision exceeds MAX_PRECISION and must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_hundred_digit_precision() {
    let raw = format!(r#"{{"precision":{},"scale":0}}"#, "9".repeat(100));
    assert!(
        de(&raw).is_err(),
        "a 100-digit precision must be rejected without panicking"
    );
}

#[test]
fn deserialize_metadata_rejects_scale_overflowing_u32() {
    assert!(
        de(r#"{"precision":10,"scale":4294967296}"#).is_err(),
        "scale above u32::MAX must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_internal_whitespace_in_number() {
    assert!(
        de(r#"{"precision":1 0,"scale":2}"#).is_err(),
        "whitespace inside a number must be rejected, not silently truncated"
    );
}

#[test]
fn deserialize_metadata_rejects_underscore_separated_number() {
    assert!(
        de(r#"{"precision":1_0,"scale":2}"#).is_err(),
        "Rust-style digit separators are not JSON and must be rejected"
    );
}

// =====================================================================
// E. deserialize_metadata — key handling
// =====================================================================

#[test]
fn deserialize_metadata_rejects_unknown_extra_key() {
    assert!(
        de(r#"{"precision":10,"scale":2,"extra":1}"#).is_err(),
        "unknown metadata keys must be rejected, not ignored"
    );
}

#[test]
fn deserialize_metadata_rejects_unknown_key_in_first_position() {
    assert!(
        de(r#"{"extra":1,"precision":10,"scale":2}"#).is_err(),
        "unknown key before the known keys must still be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_empty_key_name() {
    assert!(
        de(r#"{"":1,"precision":10,"scale":2}"#).is_err(),
        "empty key name must be rejected"
    );
}

#[test]
fn deserialize_metadata_key_matching_is_case_sensitive() {
    for raw in [
        r#"{"Precision":10,"scale":2}"#,
        r#"{"precision":10,"Scale":2}"#,
        r#"{"PRECISION":10,"SCALE":2}"#,
    ] {
        assert!(
            de(raw).is_err(),
            "key matching must be case-sensitive; {raw:?} was accepted"
        );
    }
}

#[test]
fn deserialize_metadata_rejects_key_with_inner_whitespace() {
    assert!(
        de(r#"{"pre cision":10,"scale":2}"#).is_err(),
        "a key with embedded whitespace is unknown and must be rejected"
    );
}

#[test]
fn deserialize_metadata_accepts_reversed_key_order() {
    let p =
        de(r#"{"scale":2,"precision":10}"#).expect("key order must not matter for a JSON object");
    assert_eq!(
        (p.precision, p.scale),
        (10, 2),
        "reversed key order produced the wrong (precision, scale)"
    );
}

#[test]
fn deserialize_metadata_duplicate_keys_take_the_last_value() {
    // Matches serde_json's last-wins rule (the documented migration target),
    // so this pins the behaviour rather than leaving it undefined.
    let p = de(r#"{"precision":10,"precision":20,"scale":2}"#)
        .expect("duplicate keys are last-wins, as in serde_json");
    assert_eq!(
        (p.precision, p.scale),
        (20, 2),
        "duplicate-key resolution is not last-wins"
    );
}

#[test]
fn deserialize_metadata_duplicate_keys_still_validated() {
    // Last-wins must not bypass validation: the final value is invalid.
    assert!(
        de(r#"{"precision":10,"precision":0,"scale":0}"#).is_err(),
        "the winning duplicate value must still be validated (precision 0)"
    );
}

#[test]
fn deserialize_metadata_duplicate_keys_do_not_resurrect_earlier_valid_value() {
    assert!(
        de(r#"{"precision":10,"scale":2,"scale":99}"#).is_err(),
        "a later invalid scale must not be masked by an earlier valid one"
    );
}

// =====================================================================
// F. deserialize_metadata — whitespace / leniency surface
// =====================================================================

#[test]
fn deserialize_metadata_tolerates_surrounding_whitespace() {
    let p = de("   {\"precision\":10,\"scale\":2}\n ")
        .expect("outer whitespace must be tolerated (IPC writers may pretty-print)");
    assert_eq!((p.precision, p.scale), (10, 2));
}

#[test]
fn deserialize_metadata_tolerates_spaces_around_tokens() {
    let p = de(r#"{ "precision" : 10 , "scale" : 2 }"#)
        .expect("pretty-printed metadata must still parse");
    assert_eq!(
        (p.precision, p.scale),
        (10, 2),
        "pretty-printed metadata parsed to the wrong values"
    );
}

#[test]
fn deserialize_metadata_tolerates_newlines_between_fields() {
    let p =
        de("{\n  \"precision\": 10,\n  \"scale\": 2\n}").expect("multi-line metadata must parse");
    assert_eq!((p.precision, p.scale), (10, 2));
}

#[test]
fn deserialize_metadata_accepts_leading_zero_padded_numbers() {
    // Not canonical JSON, but the u32 parser accepts it; pin the behaviour so a
    // future switch to serde_json (which rejects leading zeros) is a conscious change.
    let p = de(r#"{"precision":010,"scale":02}"#).expect("zero-padded numbers currently parse");
    assert_eq!(
        (p.precision, p.scale),
        (10, 2),
        "zero-padded numbers must parse as decimal, not octal"
    );
}

#[test]
fn deserialize_metadata_accepts_plus_prefixed_numbers() {
    // Rust's u32 FromStr accepts a leading '+'. Pin it so the leniency is visible.
    let p = de(r#"{"precision":+10,"scale":+2}"#).expect("'+' prefixed numbers currently parse");
    assert_eq!((p.precision, p.scale), (10, 2));
}

// =====================================================================
// G. validation applied during deserialization
// =====================================================================

#[test]
fn deserialize_metadata_rejects_zero_precision() {
    assert!(
        de(r#"{"precision":0,"scale":0}"#).is_err(),
        "precision 0 is invalid and must be rejected at deserialize time too"
    );
}

#[test]
fn deserialize_metadata_rejects_scale_greater_than_precision() {
    assert!(
        de(r#"{"precision":10,"scale":11}"#).is_err(),
        "scale > precision must be rejected at deserialize time"
    );
}

#[test]
fn deserialize_metadata_rejects_precision_above_max() {
    let raw = format!(r#"{{"precision":{},"scale":0}}"#, MAX_PRECISION + 1);
    assert!(
        de(&raw).is_err(),
        "precision MAX_PRECISION+1 must be rejected at deserialize time"
    );
}

#[test]
fn deserialize_metadata_accepts_precision_exactly_max() {
    let raw = format!(r#"{{"precision":{},"scale":0}}"#, MAX_PRECISION);
    let p = de(&raw).expect("MAX_PRECISION must be accepted");
    assert_eq!(p.precision, MAX_PRECISION);
}

#[test]
fn deserialize_metadata_accepts_scale_equal_to_precision() {
    let p = de(r#"{"precision":10,"scale":10}"#).expect("scale == precision is legal");
    assert_eq!((p.precision, p.scale), (10, 10));
}

// =====================================================================
// H. try_new / supports_data_type / validate
// =====================================================================

#[test]
fn try_new_accepts_large_binary_storage() {
    let ext = <DecimalArbExtension as ExtensionType>::try_new(
        &DataType::LargeBinary,
        DecimalArbParams {
            precision: 10,
            scale: 2,
        },
    )
    .expect("LargeBinary is the declared storage type");
    assert_eq!((ext.precision(), ext.scale()), (10, 2));
}

#[test]
fn try_new_rejects_every_non_large_binary_storage_type() {
    let params = DecimalArbParams {
        precision: 10,
        scale: 2,
    };
    let rejected = [
        DataType::Binary,
        DataType::BinaryView,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Int64,
        DataType::Float64,
        DataType::Null,
        DataType::Boolean,
        DataType::FixedSizeBinary(32),
        DataType::Decimal128(38, 9),
        DataType::Decimal256(76, 18),
    ];
    for dt in rejected {
        assert!(
            <DecimalArbExtension as ExtensionType>::try_new(&dt, params).is_err(),
            "storage type {dt:?} must not be accepted for decimal_arb"
        );
    }
}

#[test]
fn try_new_rejects_invalid_params_even_on_valid_storage() {
    for (p, s) in [(0u32, 0u32), (10, 11), (MAX_PRECISION + 1, 0), (0, 5)] {
        assert!(
            <DecimalArbExtension as ExtensionType>::try_new(
                &DataType::LargeBinary,
                DecimalArbParams {
                    precision: p,
                    scale: s,
                },
            )
            .is_err(),
            "try_new accepted invalid params ({p},{s}) — validation bypassed"
        );
    }
}

#[test]
fn supports_data_type_matches_try_new() {
    let ext = DecimalArbExtension::new(10, 2).unwrap();
    for dt in [
        DataType::LargeBinary,
        DataType::Binary,
        DataType::Utf8,
        DataType::FixedSizeBinary(32),
    ] {
        let via_supports = ext.supports_data_type(&dt).is_ok();
        let via_try_new = <DecimalArbExtension as ExtensionType>::try_new(
            &dt,
            DecimalArbParams {
                precision: 10,
                scale: 2,
            },
        )
        .is_ok();
        assert_eq!(
            via_supports, via_try_new,
            "supports_data_type and try_new disagree on {dt:?}"
        );
    }
}

#[test]
fn validate_rejects_wrong_storage_type() {
    assert!(
        <DecimalArbExtension as ExtensionType>::validate(
            &DataType::Binary,
            DecimalArbParams {
                precision: 10,
                scale: 2
            },
        )
        .is_err(),
        "ExtensionType::validate must reject non-LargeBinary storage"
    );
}

#[test]
fn extension_new_rejects_invalid_and_accepts_valid_boundaries() {
    assert!(
        DecimalArbExtension::new(0, 0).is_err(),
        "precision 0 must be rejected"
    );
    assert!(
        DecimalArbExtension::new(0, 5).is_err(),
        "precision 0 with nonzero scale must be rejected"
    );
    assert!(
        DecimalArbExtension::new(1, 0).is_ok(),
        "(1,0) is the minimal legal decimal_arb"
    );
    assert!(
        DecimalArbExtension::new(1, 1).is_ok(),
        "(1,1) is legal: one fractional digit, zero integer digits"
    );
    assert!(
        DecimalArbExtension::new(1, 2).is_err(),
        "scale > precision must be rejected"
    );
    assert!(
        DecimalArbExtension::new(MAX_PRECISION, 0).is_ok(),
        "MAX_PRECISION must be accepted"
    );
    assert!(
        DecimalArbExtension::new(MAX_PRECISION + 1, 0).is_err(),
        "MAX_PRECISION+1 must be rejected"
    );
    assert!(
        DecimalArbExtension::new(u32::MAX, 0).is_err(),
        "u32::MAX precision must be rejected"
    );
}

#[test]
fn extension_accessors_agree_with_metadata() {
    let ext = DecimalArbExtension::new(500, 250).unwrap();
    let m = ExtensionType::metadata(&ext);
    assert_eq!(
        (ext.precision(), ext.scale()),
        (m.precision, m.scale),
        "precision()/scale() disagree with the ExtensionType::metadata payload"
    );
}

#[test]
fn extension_equality_is_by_params() {
    assert_eq!(
        DecimalArbExtension::new(10, 2).unwrap(),
        DecimalArbExtension::new(10, 2).unwrap(),
        "same params must compare equal"
    );
    assert_ne!(
        DecimalArbExtension::new(10, 2).unwrap(),
        DecimalArbExtension::new(10, 3).unwrap(),
        "different scale must compare unequal"
    );
    assert_ne!(
        DecimalArbExtension::new(10, 2).unwrap(),
        DecimalArbExtension::new(11, 2).unwrap(),
        "different precision must compare unequal"
    );
}

#[test]
fn extension_name_constant_is_the_registered_name() {
    assert_eq!(
        <DecimalArbExtension as ExtensionType>::NAME,
        DecimalArbType::EXTENSION_NAME,
        "the ExtensionType NAME and DecimalArbType::EXTENSION_NAME must not diverge"
    );
    assert_eq!(
        DecimalArbType::EXTENSION_NAME,
        "streamling.decimal_arb",
        "the extension name is a wire contract and must not change"
    );
}

#[test]
fn extension_metadata_keys_are_the_standard_arrow_keys() {
    assert_eq!(
        DecimalArbType::EXTENSION_NAME_KEY,
        "ARROW:extension:name",
        "extension name key must be the standard Arrow key"
    );
    assert_eq!(
        DecimalArbType::EXTENSION_METADATA_KEY,
        "ARROW:extension:metadata",
        "extension metadata key must be the standard Arrow key"
    );
}

#[test]
fn max_precision_constant_is_pinned() {
    assert_eq!(
        MAX_PRECISION, 65_535,
        "MAX_PRECISION is part of the validated schema contract"
    );
}

// =====================================================================
// I. DecimalArbType::field / metadata construction
// =====================================================================

#[test]
fn field_has_exactly_the_two_extension_keys() {
    let f = DecimalArbType::field("amount", 10, 2, true).unwrap();
    let mut keys: Vec<&str> = f.metadata().keys().map(|s| s.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            DecimalArbType::EXTENSION_METADATA_KEY,
            DecimalArbType::EXTENSION_NAME_KEY
        ],
        "field() must stamp exactly the two Arrow extension keys"
    );
}

#[test]
fn field_storage_type_is_large_binary() {
    let f = DecimalArbType::field("amount", 10, 2, true).unwrap();
    assert_eq!(
        f.data_type(),
        &DataType::LargeBinary,
        "decimal_arb storage type must be LargeBinary"
    );
    assert_eq!(
        DecimalArbType::new(),
        DataType::LargeBinary,
        "DecimalArbType::new() must report LargeBinary"
    );
}

#[test]
fn field_preserves_name_and_nullability() {
    for nullable in [true, false] {
        let f = DecimalArbType::field("weird name.with:chars", 10, 2, nullable).unwrap();
        assert_eq!(
            f.name(),
            "weird name.with:chars",
            "field name was rewritten"
        );
        assert_eq!(f.is_nullable(), nullable, "nullability was rewritten");
    }
}

#[test]
fn field_rejects_invalid_precision_scale() {
    assert!(DecimalArbType::field("x", 0, 0, true).is_err(), "(0,0)");
    assert!(
        DecimalArbType::field("x", 10, 11, true).is_err(),
        "scale > precision"
    );
    assert!(
        DecimalArbType::field("x", MAX_PRECISION + 1, 0, true).is_err(),
        "precision above MAX_PRECISION"
    );
}

#[test]
fn field_accepts_boundary_precision_scale() {
    assert!(DecimalArbType::field("x", 1, 0, true).is_ok(), "(1,0)");
    assert!(DecimalArbType::field("x", 1, 1, true).is_ok(), "(1,1)");
    assert!(
        DecimalArbType::field("x", MAX_PRECISION, MAX_PRECISION, true).is_ok(),
        "(MAX,MAX)"
    );
}

#[test]
fn type_metadata_matches_field_metadata_regardless_of_name() {
    let m = DecimalArbType::metadata(100, 18).unwrap();
    for name in ["a", "", "decimal_arb", "totally.different"] {
        let f = DecimalArbType::field(name, 100, 18, true).unwrap();
        assert_eq!(
            f.metadata(),
            &m,
            "field metadata must not depend on the column name (name={name:?})"
        );
    }
}

#[test]
fn type_metadata_mirrors_serialize_metadata_exactly() {
    let m = DecimalArbType::metadata(38, 9).unwrap();
    assert_eq!(
        m.get(DecimalArbType::EXTENSION_METADATA_KEY)
            .map(String::as_str),
        Some(ser(38, 9).as_str()),
        "DecimalArbType::metadata and serialize_metadata must produce identical payloads"
    );
}

#[test]
fn type_metadata_rejects_invalid_precision_scale() {
    assert!(DecimalArbType::metadata(0, 0).is_err(), "(0,0)");
    assert!(DecimalArbType::metadata(0, 1).is_err(), "(0,1)");
    assert!(
        DecimalArbType::metadata(10, 11).is_err(),
        "scale > precision"
    );
    assert!(
        DecimalArbType::metadata(MAX_PRECISION + 1, 0).is_err(),
        "MAX_PRECISION+1"
    );
    assert!(
        DecimalArbType::metadata(u32::MAX, u32::MAX).is_err(),
        "u32::MAX precision"
    );
}

#[test]
fn field_is_recognized_by_has_valid_extension_type() {
    let f = DecimalArbType::field("amount", 10, 2, true).unwrap();
    assert!(
        f.has_valid_extension_type::<DecimalArbExtension>(),
        "field() output must validate under the Arrow extension API"
    );
}

// =====================================================================
// J. is_decimal_arb_metadata / is_decimal_arb_field against lookalikes
// =====================================================================

#[test]
fn is_decimal_arb_metadata_accepts_the_canonical_map() {
    assert!(
        DecimalArbType::is_decimal_arb_metadata(&good_map(10, 2)),
        "the canonical metadata map must be recognized"
    );
}

#[test]
fn is_decimal_arb_metadata_rejects_empty_map() {
    assert!(
        !DecimalArbType::is_decimal_arb_metadata(&HashMap::new()),
        "an empty metadata map is not decimal_arb"
    );
}

#[test]
fn is_decimal_arb_metadata_rejects_lookalike_extension_names() {
    let lookalikes = [
        "streamling.decimal_arb2",
        "streamling.decimal_ar",
        "streamling.decimal-arb",
        "decimal_arb",
        "STREAMLING.DECIMAL_ARB",
        "Streamling.Decimal_Arb",
        " streamling.decimal_arb",
        "streamling.decimal_arb ",
        "\"streamling.decimal_arb\"",
        "streamling.decimal_arb\n",
        "",
        "streamling.decimal_arbitrary",
        "xstreamling.decimal_arb",
    ];
    for name in lookalikes {
        let m = map(&[(DecimalArbType::EXTENSION_NAME_KEY, name)]);
        assert!(
            !DecimalArbType::is_decimal_arb_metadata(&m),
            "extension name {name:?} must NOT be treated as decimal_arb"
        );
    }
}

#[test]
fn is_decimal_arb_metadata_rejects_wrong_key_casing() {
    for key in [
        "arrow:extension:name",
        "ARROW:EXTENSION:NAME",
        "Arrow:Extension:Name",
        "ARROW:extension:Name",
    ] {
        let m = map(&[(key, DecimalArbType::EXTENSION_NAME)]);
        assert!(
            !DecimalArbType::is_decimal_arb_metadata(&m),
            "metadata key {key:?} is not the Arrow key and must not be honoured"
        );
    }
}

#[test]
fn is_decimal_arb_metadata_ignores_the_metadata_payload() {
    // The name key alone decides; a valid payload without the name key is not enough.
    let only_payload = map(&[(
        DecimalArbType::EXTENSION_METADATA_KEY,
        r#"{"precision":10,"scale":2}"#,
    )]);
    assert!(
        !DecimalArbType::is_decimal_arb_metadata(&only_payload),
        "a payload without the extension name must not be recognized"
    );
}

#[test]
fn is_decimal_arb_metadata_true_even_when_payload_is_garbage() {
    // Documents the deliberate split: name-based detection, payload validated later.
    let m = map(&[
        (
            DecimalArbType::EXTENSION_NAME_KEY,
            DecimalArbType::EXTENSION_NAME,
        ),
        (DecimalArbType::EXTENSION_METADATA_KEY, "garbage"),
    ]);
    assert!(
        DecimalArbType::is_decimal_arb_metadata(&m),
        "detection is by extension name only"
    );
}

#[test]
fn is_decimal_arb_field_requires_large_binary_storage() {
    let meta = good_map(10, 2);
    for dt in [
        DataType::Binary,
        DataType::BinaryView,
        DataType::Utf8,
        DataType::FixedSizeBinary(32),
        DataType::Decimal128(38, 9),
    ] {
        let f = Field::new("x", dt.clone(), true).with_metadata(meta.clone());
        assert!(
            !DecimalArbType::is_decimal_arb_field(&f),
            "metadata alone must not make {dt:?} a decimal_arb field"
        );
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&f),
            None,
            "precision_scale_from_field must refuse storage type {dt:?}"
        );
    }
}

#[test]
fn is_decimal_arb_field_requires_metadata() {
    let f = Field::new("blob", DataType::LargeBinary, true);
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "a plain LargeBinary column is not decimal_arb"
    );
}

#[test]
fn is_decimal_arb_field_ignores_unrelated_extra_metadata() {
    let mut meta = good_map(10, 2);
    meta.insert("some.vendor.key".into(), "whatever".into());
    meta.insert("PARQUET:field_id".into(), "7".into());
    let f = Field::new("x", DataType::LargeBinary, true).with_metadata(meta);
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "unrelated metadata keys must not defeat decimal_arb recognition"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((10, 2)),
        "unrelated metadata keys must not disturb (precision, scale) extraction"
    );
}

#[test]
fn precision_scale_from_field_returns_none_for_missing_payload() {
    let m = map(&[(
        DecimalArbType::EXTENSION_NAME_KEY,
        DecimalArbType::EXTENSION_NAME,
    )]);
    let f = Field::new("x", DataType::LargeBinary, true).with_metadata(m);
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "name key alone is enough for is_decimal_arb_field"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        None,
        "a field with no payload must not yield a (precision, scale)"
    );
}

#[test]
fn precision_scale_from_field_returns_none_for_each_malformed_payload() {
    let payloads = [
        "",
        "   ",
        "{}",
        "garbage",
        r#"{"precision":10}"#,
        r#"{"scale":2}"#,
        r#"{"precision":0,"scale":0}"#,
        r#"{"precision":10,"scale":11}"#,
        r#"{"precision":-1,"scale":0}"#,
        r#"{"precision":4294967296,"scale":0}"#,
        r#"{"precision":65536,"scale":0}"#,
        r#"{"precision":"10","scale":"2"}"#,
        r#"{"precision":10,"scale":2,"extra":3}"#,
        r#"["precision",10]"#,
    ];
    for payload in payloads {
        let m = map(&[
            (
                DecimalArbType::EXTENSION_NAME_KEY,
                DecimalArbType::EXTENSION_NAME,
            ),
            (DecimalArbType::EXTENSION_METADATA_KEY, payload),
        ]);
        let f = Field::new("x", DataType::LargeBinary, true).with_metadata(m);
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&f),
            None,
            "malformed payload {payload:?} must not yield a (precision, scale)"
        );
    }
}

#[test]
fn precision_scale_from_field_rejects_a_different_extension_name() {
    let m = map(&[
        (DecimalArbType::EXTENSION_NAME_KEY, "arrow.json"),
        (
            DecimalArbType::EXTENSION_METADATA_KEY,
            r#"{"precision":10,"scale":2}"#,
        ),
    ]);
    let f = Field::new("x", DataType::LargeBinary, true).with_metadata(m);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        None,
        "a decimal_arb-shaped payload under a different extension name must be ignored"
    );
}

#[test]
fn precision_scale_from_field_does_not_panic_on_hostile_payloads() {
    for payload in [
        "{".repeat(5_000),
        "}".repeat(5_000),
        format!("{{{}}}", "\"precision\":1,".repeat(2_000)),
        format!("{{\"precision\":{},\"scale\":0}}", "1".repeat(5_000)),
    ] {
        let m = map(&[
            (
                DecimalArbType::EXTENSION_NAME_KEY,
                DecimalArbType::EXTENSION_NAME,
            ),
            (DecimalArbType::EXTENSION_METADATA_KEY, payload.as_str()),
        ]);
        let f = Field::new("x", DataType::LargeBinary, true).with_metadata(m);
        // Only requirement: no panic. Value may be None (or Some for the
        // repeated-precision case, which is last-wins).
        let _ = DecimalArbType::precision_scale_from_field(&f);
    }
}

// =====================================================================
// K. NativeIntKind::parse
// =====================================================================

#[test]
fn native_int_kind_parses_canonical_forms() {
    assert_eq!(NativeIntKind::parse("u256"), Some(NativeIntKind::U256));
    assert_eq!(NativeIntKind::parse("i256"), Some(NativeIntKind::I256));
}

#[test]
fn native_int_kind_parse_is_case_insensitive() {
    for (raw, expected) in [
        ("U256", NativeIntKind::U256),
        ("u256", NativeIntKind::U256),
        ("I256", NativeIntKind::I256),
        ("i256", NativeIntKind::I256),
    ] {
        assert_eq!(
            NativeIntKind::parse(raw),
            Some(expected),
            "parse must be case-insensitive for {raw:?}"
        );
    }
}

#[test]
fn native_int_kind_parse_trims_surrounding_whitespace() {
    for raw in [" u256", "u256 ", "\tu256\n", "  U256  ", "\r\nI256\r\n"] {
        assert!(
            NativeIntKind::parse(raw).is_some(),
            "parse must trim surrounding whitespace, failed on {raw:?}"
        );
    }
}

#[test]
fn native_int_kind_parse_rejects_unknown_values() {
    let rejected = [
        "",
        " ",
        "u",
        "256",
        "u255",
        "u2560",
        "uint256",
        "int256",
        "u128",
        "i128",
        "u256x",
        "xu256",
        "u 256",
        "u_256",
        "u-256",
        "null",
        "none",
        "true",
        "U256\u{0}",
        "ｕ256",
    ];
    for raw in rejected {
        assert_eq!(
            NativeIntKind::parse(raw),
            None,
            "unknown native_int_kind {raw:?} must parse to None (forward compatibility)"
        );
    }
}

#[test]
fn native_int_kind_as_str_is_lowercase_canonical() {
    assert_eq!(NativeIntKind::U256.as_str(), "u256");
    assert_eq!(NativeIntKind::I256.as_str(), "i256");
}

#[test]
fn native_int_kind_as_str_parse_round_trip() {
    for kind in [NativeIntKind::U256, NativeIntKind::I256] {
        assert_eq!(
            NativeIntKind::parse(kind.as_str()),
            Some(kind),
            "as_str/parse round-trip broke for {kind:?}"
        );
    }
}

#[test]
fn native_int_kind_key_constant_is_pinned() {
    assert_eq!(
        DecimalArbType::NATIVE_INT_KIND_KEY,
        "streamling.native_int_kind",
        "the hint key is a wire contract shared with sinks"
    );
}

#[test]
fn native_int_kind_key_is_not_an_arrow_extension_key() {
    assert_ne!(
        DecimalArbType::NATIVE_INT_KIND_KEY,
        DecimalArbType::EXTENSION_METADATA_KEY,
        "the hint must live outside the Arrow extension payload"
    );
    assert_ne!(
        DecimalArbType::NATIVE_INT_KIND_KEY,
        DecimalArbType::EXTENSION_NAME_KEY
    );
}

// =====================================================================
// L. with_native_int_kind / native_int_kind_from_field
// =====================================================================

#[test]
fn with_native_int_kind_round_trips_both_kinds() {
    for kind in [NativeIntKind::U256, NativeIntKind::I256] {
        let base = DecimalArbType::field("v", 78, 0, true).unwrap();
        let hinted = DecimalArbType::with_native_int_kind(base, kind).unwrap();
        assert_eq!(
            DecimalArbType::native_int_kind_from_field(&hinted),
            Some(kind),
            "native_int_kind hint did not round-trip for {kind:?}"
        );
    }
}

#[test]
fn with_native_int_kind_stores_the_canonical_lowercase_string() {
    let base = DecimalArbType::field("v", 78, 0, true).unwrap();
    let hinted = DecimalArbType::with_native_int_kind(base, NativeIntKind::U256).unwrap();
    assert_eq!(
        hinted
            .metadata()
            .get(DecimalArbType::NATIVE_INT_KIND_KEY)
            .map(String::as_str),
        Some("u256"),
        "the hint must be stored in canonical lowercase form"
    );
}

#[test]
fn with_native_int_kind_preserves_precision_and_scale() {
    let base = DecimalArbType::field("v", 78, 0, true).unwrap();
    let hinted = DecimalArbType::with_native_int_kind(base, NativeIntKind::I256).unwrap();
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&hinted),
        Some((78, 0)),
        "stamping the hint must not disturb (precision, scale)"
    );
}

#[test]
fn with_native_int_kind_keeps_the_field_decimal_arb() {
    let base = DecimalArbType::field("v", 78, 0, false).unwrap();
    let hinted = DecimalArbType::with_native_int_kind(base, NativeIntKind::U256).unwrap();
    assert!(
        DecimalArbType::is_decimal_arb_field(&hinted),
        "the hint must not break decimal_arb recognition"
    );
    assert!(
        hinted.has_valid_extension_type::<DecimalArbExtension>(),
        "the extra metadata key must not invalidate the Arrow extension type"
    );
}

#[test]
fn with_native_int_kind_preserves_name_nullability_and_storage() {
    let base = DecimalArbType::field("balance", 78, 0, false).unwrap();
    let hinted = DecimalArbType::with_native_int_kind(base, NativeIntKind::U256).unwrap();
    assert_eq!(hinted.name(), "balance", "field name changed");
    assert!(!hinted.is_nullable(), "nullability changed");
    assert_eq!(
        hinted.data_type(),
        &DataType::LargeBinary,
        "storage type changed"
    );
}

#[test]
fn with_native_int_kind_adds_exactly_one_metadata_key() {
    let base = DecimalArbType::field("v", 78, 0, true).unwrap();
    let before = base.metadata().len();
    let hinted = DecimalArbType::with_native_int_kind(base, NativeIntKind::U256).unwrap();
    assert_eq!(
        hinted.metadata().len(),
        before + 1,
        "stamping the hint must add exactly one metadata key"
    );
}

#[test]
fn with_native_int_kind_preserves_unrelated_metadata() {
    let mut meta = good_map(78, 0);
    meta.insert("vendor.key".into(), "keep-me".into());
    let base = Field::new("v", DataType::LargeBinary, true).with_metadata(meta);
    let hinted = DecimalArbType::with_native_int_kind(base, NativeIntKind::I256).unwrap();
    assert_eq!(
        hinted.metadata().get("vendor.key").map(String::as_str),
        Some("keep-me"),
        "stamping the hint must not drop unrelated metadata"
    );
}

#[test]
fn with_native_int_kind_overwrites_a_previous_hint() {
    let base = DecimalArbType::field("v", 78, 0, true).unwrap();
    let u = DecimalArbType::with_native_int_kind(base, NativeIntKind::U256).unwrap();
    let i = DecimalArbType::with_native_int_kind(u, NativeIntKind::I256).unwrap();
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&i),
        Some(NativeIntKind::I256),
        "re-stamping must overwrite the previous hint"
    );
    assert_eq!(
        i.metadata()
            .values()
            .filter(|v| v.as_str() == "u256")
            .count(),
        0,
        "the stale u256 hint must not survive"
    );
}

#[test]
fn with_native_int_kind_is_idempotent() {
    let base = DecimalArbType::field("v", 78, 0, true).unwrap();
    let once = DecimalArbType::with_native_int_kind(base, NativeIntKind::U256).unwrap();
    let twice = DecimalArbType::with_native_int_kind(once.clone(), NativeIntKind::U256).unwrap();
    assert_eq!(
        once.metadata(),
        twice.metadata(),
        "stamping the same hint twice must be idempotent"
    );
}

#[test]
fn with_native_int_kind_rejects_plain_large_binary_field() {
    let f = Field::new("blob", DataType::LargeBinary, true);
    assert!(
        DecimalArbType::with_native_int_kind(f, NativeIntKind::U256).is_err(),
        "the hint must only be applicable to decimal_arb fields"
    );
}

#[test]
fn with_native_int_kind_rejects_non_decimal_arb_types() {
    for dt in [
        DataType::Int64,
        DataType::Utf8,
        DataType::Binary,
        DataType::FixedSizeBinary(32),
        DataType::Decimal256(76, 0),
    ] {
        let f = Field::new("v", dt.clone(), true);
        assert!(
            DecimalArbType::with_native_int_kind(f, NativeIntKind::U256).is_err(),
            "the hint must be rejected on a {dt:?} field"
        );
    }
}

#[test]
fn with_native_int_kind_rejects_decimal_arb_metadata_on_wrong_storage() {
    let f = Field::new("v", DataType::FixedSizeBinary(32), true).with_metadata(good_map(78, 0));
    assert!(
        DecimalArbType::with_native_int_kind(f, NativeIntKind::U256).is_err(),
        "decimal_arb metadata on non-LargeBinary storage must not accept the hint"
    );
}

#[test]
fn native_int_kind_from_field_is_none_when_absent() {
    let f = DecimalArbType::field("v", 78, 0, true).unwrap();
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        None,
        "a decimal_arb field without the hint must report None"
    );
}

#[test]
fn native_int_kind_from_field_is_none_for_unrecognized_hint_values() {
    for raw in ["", " ", "u128", "uint256", "U 256", "garbage", "0"] {
        let mut meta = good_map(78, 0);
        meta.insert(DecimalArbType::NATIVE_INT_KIND_KEY.into(), raw.into());
        let f = Field::new("v", DataType::LargeBinary, true).with_metadata(meta);
        assert_eq!(
            DecimalArbType::native_int_kind_from_field(&f),
            None,
            "unrecognized hint {raw:?} must be treated as absent, not as a default kind"
        );
    }
}

#[test]
fn native_int_kind_from_field_accepts_uppercase_stored_hint() {
    let mut meta = good_map(78, 0);
    meta.insert(DecimalArbType::NATIVE_INT_KIND_KEY.into(), "U256".into());
    let f = Field::new("v", DataType::LargeBinary, true).with_metadata(meta);
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        Some(NativeIntKind::U256),
        "a hint written by another producer in uppercase must still be honoured"
    );
}

#[test]
fn native_int_kind_from_field_ignores_hint_on_non_decimal_arb_field() {
    let f = Field::new("v", DataType::Binary, true)
        .with_metadata(map(&[(DecimalArbType::NATIVE_INT_KIND_KEY, "u256")]));
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        None,
        "the hint is only meaningful on decimal_arb fields"
    );
}

#[test]
fn native_int_kind_from_field_ignores_hint_when_storage_is_wrong() {
    let mut meta = good_map(78, 0);
    meta.insert(DecimalArbType::NATIVE_INT_KIND_KEY.into(), "u256".into());
    let f = Field::new("v", DataType::FixedSizeBinary(32), true).with_metadata(meta);
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        None,
        "native_int_kind_from_field requires LargeBinary storage (use the _metadata variant)"
    );
}

#[test]
fn native_int_kind_from_field_metadata_reads_normalized_storage() {
    // The documented ClickHouse path: storage normalized away from LargeBinary,
    // metadata kept. The map-based accessor must still see the hint.
    let mut meta = good_map(78, 0);
    meta.insert(DecimalArbType::NATIVE_INT_KIND_KEY.into(), "u256".into());
    assert_eq!(
        DecimalArbType::native_int_kind_from_field_metadata(&meta),
        Some(NativeIntKind::U256),
        "the map-based accessor must work after storage normalization"
    );
}

#[test]
fn native_int_kind_from_field_metadata_requires_decimal_arb_name() {
    let m = map(&[(DecimalArbType::NATIVE_INT_KIND_KEY, "u256")]);
    assert_eq!(
        DecimalArbType::native_int_kind_from_field_metadata(&m),
        None,
        "the hint must be ignored without the decimal_arb extension name"
    );
}

#[test]
fn native_int_kind_from_field_metadata_rejects_lookalike_extension_name() {
    let m = map(&[
        (
            DecimalArbType::EXTENSION_NAME_KEY,
            "streamling.decimal_arb2",
        ),
        (DecimalArbType::NATIVE_INT_KIND_KEY, "u256"),
    ]);
    assert_eq!(
        DecimalArbType::native_int_kind_from_field_metadata(&m),
        None,
        "a lookalike extension name must not unlock the hint"
    );
}

#[test]
fn native_int_kind_from_field_metadata_is_none_for_unknown_values() {
    for raw in ["u128", "", "  ", "i255", "junk"] {
        let mut meta = good_map(78, 0);
        meta.insert(DecimalArbType::NATIVE_INT_KIND_KEY.into(), raw.into());
        assert_eq!(
            DecimalArbType::native_int_kind_from_field_metadata(&meta),
            None,
            "unknown hint {raw:?} must be None from the map accessor too"
        );
    }
}

#[test]
fn native_int_kind_field_and_metadata_accessors_agree_on_large_binary() {
    for raw in ["u256", "i256", "U256", "junk", ""] {
        let mut meta = good_map(78, 0);
        meta.insert(DecimalArbType::NATIVE_INT_KIND_KEY.into(), raw.into());
        let f = Field::new("v", DataType::LargeBinary, true).with_metadata(meta.clone());
        assert_eq!(
            DecimalArbType::native_int_kind_from_field(&f),
            DecimalArbType::native_int_kind_from_field_metadata(&meta),
            "field and metadata accessors disagree for hint {raw:?} on LargeBinary storage"
        );
    }
}

#[test]
fn native_int_kind_metadata_accessor_tolerates_broken_precision_payload() {
    // The hint lives outside the extension payload, so it must be readable
    // even if the (precision, scale) payload is unparseable.
    let m = map(&[
        (
            DecimalArbType::EXTENSION_NAME_KEY,
            DecimalArbType::EXTENSION_NAME,
        ),
        (DecimalArbType::EXTENSION_METADATA_KEY, "garbage"),
        (DecimalArbType::NATIVE_INT_KIND_KEY, "i256"),
    ]);
    assert_eq!(
        DecimalArbType::native_int_kind_from_field_metadata(&m),
        Some(NativeIntKind::I256),
        "the hint must survive independently of the extension payload"
    );
}

// =====================================================================
// M. (precision, scale) boundary semantics end-to-end
// =====================================================================

#[test]
fn builder_rejects_the_same_invalid_precision_scale_as_the_extension() {
    for (p, s) in [
        (0u32, 0u32),
        (0, 1),
        (10, 11),
        (MAX_PRECISION + 1, 0),
        (u32::MAX, 0),
    ] {
        let builder_ok = DecimalArbArrayBuilder::with_capacity(1, "c", p, s).is_ok();
        let ext_ok = DecimalArbExtension::new(p, s).is_ok();
        assert_eq!(
            builder_ok, ext_ok,
            "builder and extension disagree on validity of ({p},{s})"
        );
        assert!(!builder_ok, "({p},{s}) must be rejected by the builder");
    }
}

#[test]
fn precision_one_scale_one_admits_only_pure_fractions() {
    let v_ok = DecimalArbValue::from_str("0.5").unwrap();
    assert!(
        v_ok.check_fits(1, 1, "c").is_ok(),
        "(1,1) must admit 0.5 (0 integer digits, 1 fractional digit)"
    );
    let v_bad = DecimalArbValue::from_str("1").unwrap();
    assert!(
        v_bad.check_fits(1, 1, "c").is_err(),
        "(1,1) leaves 0 integer digits, so 1 must not fit"
    );
}

#[test]
fn precision_one_scale_zero_admits_one_integer_digit() {
    assert!(
        DecimalArbValue::from_str("9")
            .unwrap()
            .check_fits(1, 0, "c")
            .is_ok(),
        "(1,0) must admit a single-digit integer"
    );
    assert!(
        DecimalArbValue::from_str("10")
            .unwrap()
            .check_fits(1, 0, "c")
            .is_err(),
        "(1,0) must reject a two-digit integer"
    );
}

#[test]
fn scale_equal_to_precision_leaves_no_integer_digits() {
    let one = DecimalArbValue::from_str("1").unwrap();
    for p in [1u32, 2, 5, 38] {
        assert!(
            one.check_fits(p, p, "c").is_err(),
            "scale == precision == {p} leaves 0 integer digits; 1 must not fit"
        );
    }
}

#[test]
fn check_fits_rejects_invalid_precision_scale_before_looking_at_the_value() {
    let zero = DecimalArbValue::from_str("0").unwrap();
    assert!(
        zero.check_fits(0, 0, "c").is_err(),
        "check_fits must reject precision 0 even for the value 0"
    );
    assert!(
        zero.check_fits(10, 11, "c").is_err(),
        "check_fits must reject scale > precision even for the value 0"
    );
    assert!(
        zero.check_fits(MAX_PRECISION + 1, 0, "c").is_err(),
        "check_fits must reject precision above MAX_PRECISION"
    );
}

#[test]
fn max_precision_field_and_builder_agree() {
    assert!(
        DecimalArbType::field("v", MAX_PRECISION, 0, true).is_ok(),
        "MAX_PRECISION field must build"
    );
    assert!(
        DecimalArbArrayBuilder::with_capacity(1, "v", MAX_PRECISION, 0).is_ok(),
        "MAX_PRECISION builder must build"
    );
    assert!(
        DecimalArbType::field("v", MAX_PRECISION + 1, 0, true).is_err(),
        "MAX_PRECISION+1 field must be rejected"
    );
    assert!(
        DecimalArbArrayBuilder::with_capacity(1, "v", MAX_PRECISION + 1, 0).is_err(),
        "MAX_PRECISION+1 builder must be rejected"
    );
}

#[test]
fn every_entry_point_agrees_on_precision_scale_validity() {
    let cases: &[(u32, u32)] = &[
        (0, 0),
        (0, 1),
        (1, 0),
        (1, 1),
        (1, 2),
        (2, 2),
        (2, 3),
        (10, 10),
        (10, 11),
        (65535, 65535),
        (65536, 0),
        (65535, 65536),
    ];
    for &(p, s) in cases {
        let via_ext = DecimalArbExtension::new(p, s).is_ok();
        let via_field = DecimalArbType::field("v", p, s, true).is_ok();
        let via_meta = DecimalArbType::metadata(p, s).is_ok();
        let via_builder = DecimalArbArrayBuilder::with_capacity(1, "v", p, s).is_ok();
        assert_eq!(
            (via_ext, via_field, via_meta, via_builder),
            (via_ext, via_ext, via_ext, via_ext),
            "entry points disagree on the validity of ({p},{s})"
        );
    }
}

#[test]
fn deserialize_agrees_with_constructor_on_validity() {
    let cases: &[(u32, u32)] = &[
        (0, 0),
        (1, 0),
        (1, 1),
        (1, 2),
        (10, 11),
        (65535, 65535),
        (65536, 0),
    ];
    for &(p, s) in cases {
        let raw = format!(r#"{{"precision":{p},"scale":{s}}}"#);
        assert_eq!(
            de(&raw).is_ok(),
            DecimalArbExtension::new(p, s).is_ok(),
            "deserialize_metadata and DecimalArbExtension::new disagree on ({p},{s})"
        );
    }
}

// =====================================================================
// N. metadata <-> array adoption (try_from_array_and_field)
// =====================================================================

fn one_value_array(precision: u32, scale: u32, text: &str) -> LargeBinaryArray {
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "v", precision, scale).unwrap();
    b.append_value(&DecimalArbValue::from_str(text).unwrap())
        .unwrap();
    b.finish().into_inner().0
}

#[test]
fn try_from_array_and_field_adopts_the_fields_precision_and_scale() {
    let arr = one_value_array(20, 4, "1.2345");
    let field = DecimalArbType::field("v", 20, 4, true).unwrap();
    let adopted = DecimalArbArray::try_from_array_and_field(arr, &field).unwrap();
    assert_eq!(
        (adopted.precision(), adopted.scale()),
        (20, 4),
        "adopted array must take (precision, scale) from the field metadata"
    );
    assert_eq!(
        adopted.value(0).unwrap(),
        Some(DecimalArbValue::from_str("1.2345").unwrap()),
        "adoption must not corrupt the decoded value"
    );
}

#[test]
fn try_from_array_and_field_rejects_a_plain_large_binary_field() {
    let arr = one_value_array(20, 4, "1.2345");
    let field = Field::new("v", DataType::LargeBinary, true);
    assert!(
        DecimalArbArray::try_from_array_and_field(arr, &field).is_err(),
        "adoption must require decimal_arb metadata"
    );
}

#[test]
fn try_from_array_and_field_rejects_malformed_metadata_without_panicking() {
    for payload in ["", "{}", "garbage", r#"{"precision":0,"scale":0}"#] {
        let arr = one_value_array(20, 4, "1.2345");
        let m = map(&[
            (
                DecimalArbType::EXTENSION_NAME_KEY,
                DecimalArbType::EXTENSION_NAME,
            ),
            (DecimalArbType::EXTENSION_METADATA_KEY, payload),
        ]);
        let field = Field::new("v", DataType::LargeBinary, true).with_metadata(m);
        assert!(
            DecimalArbArray::try_from_array_and_field(arr, &field).is_err(),
            "adoption must reject malformed payload {payload:?} with an error, not a panic"
        );
    }
}

#[test]
fn try_from_array_and_field_with_mismatched_scale_reinterprets_the_value() {
    // Bytes were written at scale 4; the field declares scale 2. Adoption
    // trusts the field, so the value is re-read at the wrong magnitude. This
    // is a documented "field is the source of truth" behaviour — pin it so a
    // change is deliberate.
    let arr = one_value_array(20, 4, "1.2345");
    let field = DecimalArbType::field("v", 20, 2, true).unwrap();
    let adopted = DecimalArbArray::try_from_array_and_field(arr, &field).unwrap();
    assert_eq!(
        adopted.scale(),
        2,
        "the field's scale must win over the array's original scale"
    );
    assert_eq!(
        adopted.value(0).unwrap(),
        Some(DecimalArbValue::from_str("123.45").unwrap()),
        "decoding at the field's scale must be a pure reinterpretation of the same digits"
    );
}

#[test]
fn array_into_inner_reports_the_builder_precision_and_scale() {
    let mut b = DecimalArbArrayBuilder::with_capacity(2, "v", 30, 6).unwrap();
    b.append_value(&DecimalArbValue::from_str("1.5").unwrap())
        .unwrap();
    b.append_null();
    let (_, p, s) = b.finish().into_inner();
    assert_eq!(
        (p, s),
        (30, 6),
        "the array must carry the builder's declared (precision, scale)"
    );
}

#[test]
fn builder_array_and_field_metadata_stay_consistent() {
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "v", 30, 6).unwrap();
    b.append_value(&DecimalArbValue::from_str("1.5").unwrap())
        .unwrap();
    let arr = b.finish();
    let field = DecimalArbType::field("v", arr.precision(), arr.scale(), true).unwrap();
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&field),
        Some((arr.precision(), arr.scale())),
        "field metadata built from an array must describe that array"
    );
}

// =====================================================================
// O. cross-cutting metadata identity properties
// =====================================================================

#[test]
fn identical_precision_scale_produce_byte_identical_metadata_maps() {
    // Schema equality across pipeline stages depends on this.
    let a = DecimalArbType::metadata(100, 18).unwrap();
    let b = DecimalArbType::metadata(100, 18).unwrap();
    assert_eq!(a, b, "metadata maps for the same params must be identical");
}

#[test]
fn different_precision_scale_produce_different_metadata_maps() {
    assert_ne!(
        DecimalArbType::metadata(100, 18).unwrap(),
        DecimalArbType::metadata(100, 17).unwrap(),
        "differing scale must be visible in the metadata map"
    );
    assert_ne!(
        DecimalArbType::metadata(100, 18).unwrap(),
        DecimalArbType::metadata(101, 18).unwrap(),
        "differing precision must be visible in the metadata map"
    );
}

#[test]
fn fields_with_same_params_compare_equal() {
    assert_eq!(
        DecimalArbType::field("v", 10, 2, true).unwrap(),
        DecimalArbType::field("v", 10, 2, true).unwrap(),
        "identically-declared decimal_arb fields must compare equal"
    );
}

#[test]
fn fields_differing_only_in_hint_compare_unequal() {
    let base = DecimalArbType::field("v", 78, 0, true).unwrap();
    let hinted = DecimalArbType::with_native_int_kind(base.clone(), NativeIntKind::U256).unwrap();
    assert_ne!(
        base, hinted,
        "the native_int_kind hint must be part of field identity"
    );
}

#[test]
fn params_struct_is_transparent_and_comparable() {
    let p = DecimalArbParams {
        precision: 10,
        scale: 2,
    };
    assert_eq!(p.precision, 10);
    assert_eq!(p.scale, 2);
    assert_eq!(
        p,
        DecimalArbParams {
            precision: 10,
            scale: 2
        },
        "DecimalArbParams equality must be structural"
    );
}

#[test]
fn deserialize_error_does_not_leak_control_characters_unbounded() {
    // Error messages get surfaced to users; make sure a hostile payload still
    // produces a bounded, non-panicking error.
    let payload = format!("{{\"precision\":{},\"scale\":0}}", "\u{7}".repeat(1_000));
    let err = de(&payload).expect_err("control-character payload must be an error");
    assert!(
        !err.to_string().is_empty(),
        "error message must be non-empty"
    );
}

// =====================================================================
// P. JSON interoperability of the metadata payload
//
// The payload is written into Arrow schemas that non-Rust readers parse
// with a real JSON parser, so it must be valid JSON with the documented
// shape — and payloads produced by a real JSON serializer must be
// accepted back by the hand-rolled parser.
// =====================================================================

#[test]
fn serialized_metadata_is_valid_json() {
    for &(p, s) in &[(1u32, 0u32), (10, 2), (65535, 65535)] {
        let raw = ser(p, s);
        let v: serde_json::Value = serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("metadata payload {raw:?} is not valid JSON: {e}"));
        assert_eq!(
            v["precision"].as_u64(),
            Some(p as u64),
            "JSON readers must see precision {p} in {raw:?}"
        );
        assert_eq!(
            v["scale"].as_u64(),
            Some(s as u64),
            "JSON readers must see scale {s} in {raw:?}"
        );
    }
}

#[test]
fn serialized_metadata_json_object_has_exactly_two_members() {
    let v: serde_json::Value = serde_json::from_str(&ser(10, 2)).unwrap();
    let obj = v
        .as_object()
        .expect("metadata payload must be a JSON object");
    assert_eq!(
        obj.len(),
        2,
        "the payload must carry exactly precision and scale, got {obj:?}"
    );
}

#[test]
fn serialized_metadata_numbers_are_json_integers_not_strings() {
    let v: serde_json::Value = serde_json::from_str(&ser(10, 2)).unwrap();
    assert!(
        v["precision"].is_u64() && v["scale"].is_u64(),
        "precision/scale must be JSON numbers so non-Rust readers do not need to re-parse strings"
    );
}

#[test]
fn payload_produced_by_serde_json_is_accepted() {
    let raw = serde_json::to_string(&serde_json::json!({"precision": 10, "scale": 2})).unwrap();
    let p = de(&raw)
        .unwrap_or_else(|e| panic!("a payload written by serde_json must be accepted: {e}"));
    assert_eq!((p.precision, p.scale), (10, 2));
}

#[test]
fn pretty_printed_payload_produced_by_serde_json_is_accepted() {
    let raw =
        serde_json::to_string_pretty(&serde_json::json!({"precision": 10, "scale": 2})).unwrap();
    let p = de(&raw).unwrap_or_else(|e| {
        panic!("a pretty-printed payload from another producer must be accepted: {e}")
    });
    assert_eq!(
        (p.precision, p.scale),
        (10, 2),
        "pretty-printed payload parsed to the wrong values"
    );
}

#[test]
fn payload_is_stable_across_repeated_serialization() {
    let a = ser(12345, 678);
    let b = ser(12345, 678);
    assert_eq!(
        a, b,
        "serialize_metadata must be deterministic; schema comparison depends on it"
    );
}

// =====================================================================
// Q. metadata survival across Field transformations
// =====================================================================

#[test]
fn metadata_survives_field_rename() {
    let f = DecimalArbType::field("old", 10, 2, true)
        .unwrap()
        .with_name("new");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "renaming a field must not drop decimal_arb metadata"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((10, 2))
    );
}

#[test]
fn metadata_survives_nullability_change() {
    let f = DecimalArbType::field("v", 10, 2, false)
        .unwrap()
        .with_nullable(true);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((10, 2)),
        "toggling nullability must not disturb the extension payload"
    );
}

#[test]
fn metadata_outlives_a_storage_type_change_but_recognition_does_not() {
    // Arrow keeps metadata when the data type is swapped. Recognition must
    // still require LargeBinary so a retyped column is not decoded as decimal.
    let f = DecimalArbType::field("v", 10, 2, true)
        .unwrap()
        .with_data_type(DataType::Utf8);
    assert!(
        DecimalArbType::is_decimal_arb_metadata(f.metadata()),
        "metadata is retained by Arrow across a data-type swap"
    );
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "a retyped field must NOT be treated as decimal_arb — its bytes are no longer canonical"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        None,
        "a retyped field must not expose a (precision, scale)"
    );
}

#[test]
fn replacing_metadata_wholesale_removes_recognition() {
    let f = DecimalArbType::field("v", 10, 2, true)
        .unwrap()
        .with_metadata(HashMap::new());
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "clearing metadata must clear decimal_arb recognition"
    );
}

#[test]
fn merging_two_identical_decimal_arb_fields_preserves_the_type() {
    let mut a = DecimalArbType::field("v", 10, 2, true).unwrap();
    let b = DecimalArbType::field("v", 10, 2, true).unwrap();
    a.try_merge(&b)
        .expect("identical decimal_arb fields must merge");
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&a),
        Some((10, 2)),
        "merging identical fields must preserve (precision, scale)"
    );
}

#[test]
fn merging_decimal_arb_fields_with_different_params_is_rejected() {
    let mut a = DecimalArbType::field("v", 10, 2, true).unwrap();
    let b = DecimalArbType::field("v", 20, 4, true).unwrap();
    assert!(
        a.try_merge(&b).is_err(),
        "merging decimal_arb columns with conflicting (precision, scale) must not silently pick one"
    );
}

#[test]
fn merging_decimal_arb_into_it_does_not_drop_the_hint() {
    let base = DecimalArbType::field("v", 78, 0, true).unwrap();
    let mut a = DecimalArbType::with_native_int_kind(base.clone(), NativeIntKind::U256).unwrap();
    let b = DecimalArbType::with_native_int_kind(base, NativeIntKind::U256).unwrap();
    a.try_merge(&b).expect("identical hinted fields must merge");
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&a),
        Some(NativeIntKind::U256),
        "merging must preserve the native_int_kind hint"
    );
}

#[test]
fn merging_fields_with_conflicting_hints_is_rejected() {
    let base = DecimalArbType::field("v", 78, 0, true).unwrap();
    let mut a = DecimalArbType::with_native_int_kind(base.clone(), NativeIntKind::U256).unwrap();
    let b = DecimalArbType::with_native_int_kind(base, NativeIntKind::I256).unwrap();
    assert!(
        a.try_merge(&b).is_err(),
        "u256 and i256 origins must not be silently unified"
    );
}

#[test]
fn merging_plain_large_binary_into_decimal_arb_keeps_decimal_arb() {
    let mut a = DecimalArbType::field("v", 10, 2, true).unwrap();
    let b = Field::new("v", DataType::LargeBinary, true);
    a.try_merge(&b).expect("merge must succeed");
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&a),
        Some((10, 2)),
        "an unannotated sibling must not strip decimal_arb metadata"
    );
}

#[test]
fn merging_decimal_arb_into_plain_large_binary_adopts_the_metadata() {
    // Documents an Arrow hazard rather than a Streamling bug: `Field::try_merge`
    // copies metadata wholesale onto a field that had none, so a plain binary
    // column merged with a decimal_arb column ends up *claiming* to be
    // decimal_arb. Streamling never calls `try_merge` itself; pinned here so
    // that if a code path ever starts doing so, the risk is already documented.
    let mut a = Field::new("v", DataType::LargeBinary, true);
    let b = DecimalArbType::field("v", 10, 2, true).unwrap();
    a.try_merge(&b).expect("merge must succeed");
    assert!(
        DecimalArbType::is_decimal_arb_field(&a),
        "Arrow's try_merge adopts metadata from the annotated side"
    );
}

// =====================================================================
// R. residual hostile-payload coverage
// =====================================================================

#[test]
fn deserialize_metadata_rejects_two_concatenated_objects() {
    let raw = format!("{}{}", ser(10, 2), ser(20, 4));
    assert!(
        de(&raw).is_err(),
        "two concatenated payloads must be rejected, not parsed as the first or last"
    );
}

#[test]
fn deserialize_metadata_rejects_object_wrapped_in_an_array() {
    let raw = format!("[{}]", ser(10, 2));
    assert!(
        de(&raw).is_err(),
        "an array-wrapped payload must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_doubly_braced_payload() {
    let raw = format!("{{{}}}", ser(10, 2));
    assert!(
        de(&raw).is_err(),
        "a doubly-braced payload must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_comment_annotated_payload() {
    assert!(
        de(r#"{"precision":10,"scale":2} // hi"#).is_err(),
        "trailing non-JSON text must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_unicode_digit_lookalikes() {
    // Fullwidth digits must not be accepted as ASCII digits.
    assert!(
        de("{\"precision\":\u{ff11}\u{ff10},\"scale\":0}").is_err(),
        "fullwidth digits must not parse as a u32"
    );
}

#[test]
fn deserialize_metadata_rejects_arabic_indic_digits() {
    assert!(
        de("{\"precision\":\u{0661}\u{0660},\"scale\":0}").is_err(),
        "non-ASCII digits must not parse as a u32"
    );
}

#[test]
fn deserialize_metadata_rejects_whitespace_only_value() {
    assert!(
        de(r#"{"precision":  ,"scale":2}"#).is_err(),
        "an empty precision value must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_missing_value_after_colon() {
    assert!(
        de(r#"{"precision":10,"scale":}"#).is_err(),
        "a missing scale value must be rejected"
    );
}

#[test]
fn deserialize_metadata_rejects_boolean_values() {
    assert!(
        de(r#"{"precision":true,"scale":false}"#).is_err(),
        "boolean precision/scale must be rejected"
    );
}

#[test]
fn every_rejected_payload_leaves_the_field_unusable_rather_than_defaulted() {
    // A rejected payload must never fall back to a default (precision, scale):
    // decoding at the wrong scale silently shifts the decimal point.
    for payload in [
        "",
        "{}",
        r#"{"precision":10}"#,
        r#"{"scale":2}"#,
        "junk",
        r#"{"precision":0,"scale":0}"#,
    ] {
        let m = map(&[
            (
                DecimalArbType::EXTENSION_NAME_KEY,
                DecimalArbType::EXTENSION_NAME,
            ),
            (DecimalArbType::EXTENSION_METADATA_KEY, payload),
        ]);
        let f = Field::new("v", DataType::LargeBinary, true).with_metadata(m);
        assert!(
            !f.has_valid_extension_type::<DecimalArbExtension>(),
            "payload {payload:?} must not validate as a decimal_arb extension"
        );
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&f),
            None,
            "payload {payload:?} must not yield a defaulted (precision, scale)"
        );
    }
}
