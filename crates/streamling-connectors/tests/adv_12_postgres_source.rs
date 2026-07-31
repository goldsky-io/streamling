//! Adversarial coverage for the PostgreSQL <-> Arrow type mapping used by the
//! Postgres source/sink paths, with a focus on the `decimal_arb` routing bands
//! (p <= 38 -> Decimal128, 38 < p <= 76 -> Decimal256, p > 76 -> decimal_arb).
//!
//! Everything here is pure function testing: no network, no filesystem, no
//! sleeps. The functions under test are
//! `streamling_core::utils::pg::{postgres_type_to_arrow_type,
//! postgres_type_to_arrow_field, get_postgres_type_info,
//! arrow_field_to_postgres_type}` plus the connector-side cast plumbing in
//! `streamling_connectors::table_providers::postgres::query_builder`.

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;
use streamling_connectors::table_providers::postgres::query_builder::PostgresQueryBuilder;
use streamling_core::data::COLUMN_NAME_OP;
use streamling_core::types::decimal_arb::{DecimalArbType, NativeIntKind};
use streamling_core::utils::pg::{
    arrow_field_to_postgres_type, get_postgres_type_info, postgres_type_to_arrow_field,
    postgres_type_to_arrow_type,
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn dt(pg: &str) -> DataType {
    postgres_type_to_arrow_type(pg)
        .unwrap_or_else(|e| panic!("postgres_type_to_arrow_type({pg:?}) must succeed: {e:?}"))
}

fn fld(pg: &str) -> Field {
    postgres_type_to_arrow_field(pg, "c", true)
        .unwrap_or_else(|e| panic!("postgres_type_to_arrow_field({pg:?}) must succeed: {e:?}"))
}

fn arb(p: u32, s: u32) -> Field {
    DecimalArbType::field("c", p, s, true)
        .unwrap_or_else(|e| panic!("DecimalArbType::field({p},{s}) must succeed: {e:?}"))
}

fn schema_of(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// The two entry points must never disagree about *which* Arrow storage type a
/// type string denotes. `postgres_type_to_arrow_field` is documented as
/// "`postgres_type_to_arrow_type` + metadata", so whenever both succeed the
/// field's `DataType` must equal the bare `DataType`.
fn assert_storage_types_agree(pg: &str) {
    let a = postgres_type_to_arrow_type(pg);
    let b = postgres_type_to_arrow_field(pg, "c", true);
    if let (Ok(a), Ok(b)) = (&a, &b) {
        assert_eq!(
            a,
            b.data_type(),
            "storage type disagreement for {pg:?}: type={a:?} field={:?}",
            b.data_type()
        );
    }
}

// ---------------------------------------------------------------------------
// 1. NUMERIC precision band edges — 38 / 76 / 78 and neighbours
// ---------------------------------------------------------------------------

#[test]
fn numeric_precision_1_routes_to_decimal128() {
    assert_eq!(dt("NUMERIC(1, 0)"), DataType::Decimal128(1, 0));
}

#[test]
fn numeric_precision_37_routes_to_decimal128() {
    assert_eq!(dt("NUMERIC(37, 5)"), DataType::Decimal128(37, 5));
}

#[test]
fn numeric_precision_38_is_the_last_decimal128() {
    assert_eq!(
        dt("NUMERIC(38, 0)"),
        DataType::Decimal128(38, 0),
        "p=38 is the inclusive top of the Decimal128 band"
    );
}

#[test]
fn numeric_precision_39_is_the_first_decimal256() {
    assert_eq!(
        dt("NUMERIC(39, 0)"),
        DataType::Decimal256(39, 0),
        "p=39 must step up to Decimal256, not overflow Decimal128"
    );
}

#[test]
fn numeric_precision_75_routes_to_decimal256() {
    assert_eq!(dt("NUMERIC(75, 10)"), DataType::Decimal256(75, 10));
}

#[test]
fn numeric_precision_76_is_the_last_decimal256() {
    assert_eq!(
        dt("NUMERIC(76, 0)"),
        DataType::Decimal256(76, 0),
        "p=76 is the inclusive top of the Decimal256 band"
    );
}

#[test]
fn numeric_precision_77_is_the_first_decimal_arb() {
    assert_eq!(
        dt("NUMERIC(77, 0)"),
        DataType::LargeBinary,
        "p=77 must route to decimal_arb storage"
    );
    assert!(
        DecimalArbType::is_decimal_arb_field(&fld("NUMERIC(77, 0)")),
        "p=77 field must carry decimal_arb metadata"
    );
}

#[test]
fn numeric_precision_78_routes_to_decimal_arb() {
    assert_eq!(dt("NUMERIC(78, 0)"), DataType::LargeBinary);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&fld("NUMERIC(78, 0)")),
        Some((78, 0))
    );
}

#[test]
fn numeric_precision_79_routes_to_decimal_arb() {
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&fld("NUMERIC(79, 3)")),
        Some((79, 3))
    );
}

#[test]
fn numeric_precision_100_routes_to_decimal_arb_with_exact_params() {
    let f = fld("NUMERIC(100, 18)");
    assert!(DecimalArbType::is_decimal_arb_field(&f));
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((100, 18)),
        "precision/scale must survive the mapping unchanged"
    );
}

#[test]
fn numeric_at_max_precision_65535_routes_to_decimal_arb() {
    let f = fld("NUMERIC(65535, 0)");
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((65535, 0)),
        "MAX_PRECISION must be accepted verbatim"
    );
}

#[test]
fn numeric_precision_band_sweep_is_monotonic() {
    // Sweep every precision across both boundaries and check the band rule.
    for p in 1u32..=90 {
        let pg = format!("NUMERIC({p}, 0)");
        let got = dt(&pg);
        let want = if p <= 38 {
            DataType::Decimal128(p as u8, 0)
        } else if p <= 76 {
            DataType::Decimal256(p as u8, 0)
        } else {
            DataType::LargeBinary
        };
        assert_eq!(got, want, "band rule violated at precision {p}");
    }
}

#[test]
fn decimal_arb_metadata_present_exactly_above_76() {
    for p in 1u32..=90 {
        let f = fld(&format!("NUMERIC({p}, 0)"));
        let is_arb = DecimalArbType::is_decimal_arb_field(&f);
        assert_eq!(
            is_arb,
            p > 76,
            "decimal_arb metadata must appear iff precision > 76 (p={p}, is_arb={is_arb})"
        );
    }
}

#[test]
fn scale_is_preserved_across_the_whole_band_sweep() {
    for p in [10u32, 38, 39, 76, 77, 100, 500] {
        for s in [0u32, 1, 9] {
            if s > p {
                continue;
            }
            let pg = format!("NUMERIC({p}, {s})");
            match dt(&pg) {
                DataType::Decimal128(gp, gs) => {
                    assert_eq!((gp as u32, gs as u32), (p, s), "lost scale for {pg}")
                }
                DataType::Decimal256(gp, gs) => {
                    assert_eq!((gp as u32, gs as u32), (p, s), "lost scale for {pg}")
                }
                DataType::LargeBinary => assert_eq!(
                    DecimalArbType::precision_scale_from_field(&fld(&pg)),
                    Some((p, s)),
                    "lost scale for {pg}"
                ),
                other => panic!("unexpected type {other:?} for {pg}"),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 2. DECIMAL alias parity
// ---------------------------------------------------------------------------

#[test]
fn decimal_alias_matches_numeric_in_decimal128_band() {
    assert_eq!(dt("DECIMAL(20, 5)"), dt("NUMERIC(20, 5)"));
}

#[test]
fn decimal_alias_matches_numeric_in_decimal256_band() {
    assert_eq!(dt("DECIMAL(60, 5)"), dt("NUMERIC(60, 5)"));
}

#[test]
fn decimal_alias_matches_numeric_in_decimal_arb_band() {
    let a = fld("DECIMAL(100, 18)");
    let b = fld("NUMERIC(100, 18)");
    assert_eq!(a.data_type(), b.data_type());
    assert_eq!(
        a.metadata(),
        b.metadata(),
        "DECIMAL alias must be identical"
    );
}

#[test]
fn decimal_alias_bare_matches_bare_numeric() {
    assert_eq!(dt("DECIMAL"), dt("NUMERIC"));
}

#[test]
fn decimal_alias_gets_the_u256_hint_at_78_0() {
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&fld("DECIMAL(78, 0)")),
        Some(NativeIntKind::U256),
        "the u256 hint must not be NUMERIC-spelling specific"
    );
}

#[test]
fn decimal_alias_agrees_with_field_path_across_the_band() {
    for p in [1u32, 38, 39, 76, 77, 100] {
        assert_storage_types_agree(&format!("DECIMAL({p}, 0)"));
    }
}

// ---------------------------------------------------------------------------
// 3. Case and whitespace variations
// ---------------------------------------------------------------------------

#[test]
fn lowercase_numeric_is_accepted() {
    assert_eq!(dt("numeric(20, 5)"), DataType::Decimal128(20, 5));
}

#[test]
fn mixed_case_numeric_is_accepted() {
    assert_eq!(dt("NuMeRiC(20, 5)"), DataType::Decimal128(20, 5));
}

#[test]
fn mixed_case_wide_numeric_still_reaches_decimal_arb() {
    let f = fld("NuMeRiC(100, 18)");
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((100, 18)),
        "case folding must happen before the band decision"
    );
}

#[test]
fn leading_and_trailing_whitespace_is_tolerated() {
    assert_eq!(dt("   NUMERIC(20, 5)   "), DataType::Decimal128(20, 5));
}

#[test]
fn whitespace_between_name_and_paren_is_tolerated() {
    assert_eq!(dt("NUMERIC  (20, 5)"), DataType::Decimal128(20, 5));
}

#[test]
fn tab_between_name_and_paren_is_tolerated() {
    assert_eq!(dt("NUMERIC\t(20,5)"), DataType::Decimal128(20, 5));
}

#[test]
fn newline_between_name_and_paren_is_tolerated() {
    assert_eq!(dt("NUMERIC\n(20,5)"), DataType::Decimal128(20, 5));
}

#[test]
fn whitespace_inside_params_is_trimmed() {
    assert_eq!(dt("NUMERIC(  100 ,  18  )"), DataType::LargeBinary);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&fld("NUMERIC(  100 ,  18  )")),
        Some((100, 18))
    );
}

#[test]
fn whitespace_heavy_78_0_still_gets_the_u256_hint() {
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&fld("  numeric (  78 , 0 )  ")),
        Some(NativeIntKind::U256),
        "hint detection must run on parsed values, not the raw string"
    );
}

#[test]
fn bare_numeric_with_surrounding_whitespace_keeps_default_routing() {
    assert_eq!(dt("  numeric  "), DataType::Decimal128(38, 9));
}

#[test]
fn leading_zeros_in_precision_are_parsed_numerically() {
    assert_eq!(dt("NUMERIC(0078, 0)"), DataType::LargeBinary);
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&fld("NUMERIC(0078, 0)")),
        Some(NativeIntKind::U256),
        "078 must be read as 78, hint included"
    );
}

#[test]
fn explicit_plus_sign_in_params_is_parsed() {
    // Rust's integer FromStr accepts a leading '+'; pin the resulting behaviour.
    assert_eq!(dt("NUMERIC(+20, +5)"), DataType::Decimal128(20, 5));
}

#[test]
fn negative_zero_scale_is_normalised_to_zero() {
    assert_eq!(dt("NUMERIC(20, -0)"), DataType::Decimal128(20, 0));
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&fld("NUMERIC(78, -0)")),
        Some(NativeIntKind::U256),
        "-0 scale must be treated as 0 for the u256 hint"
    );
}

// ---------------------------------------------------------------------------
// 4. Bare NUMERIC / DECIMAL (no parameters)
// ---------------------------------------------------------------------------

#[test]
fn bare_numeric_maps_to_decimal128_38_9() {
    assert_eq!(dt("NUMERIC"), DataType::Decimal128(38, 9));
}

#[test]
fn bare_numeric_field_carries_no_decimal_arb_metadata() {
    let f = fld("NUMERIC");
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "unparameterised NUMERIC must not claim to be decimal_arb"
    );
    assert!(
        f.metadata().is_empty(),
        "no stray metadata on a plain field"
    );
}

#[test]
#[ignore = "FINDING: bare (unconstrained) Postgres NUMERIC maps to Decimal128(38,9), silently truncating the arbitrary-precision values decimal_arb exists to carry"]
fn bare_numeric_should_not_silently_narrow_unconstrained_values() {
    // Postgres `NUMERIC` with no typmod is unconstrained (up to 131072 integral
    // digits). Mapping it to Decimal128(38, 9) means a uint256 stored in an
    // unconstrained NUMERIC column overflows on read with no error at schema
    // resolution time. The correct target for "unbounded" is decimal_arb.
    let f = fld("NUMERIC");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "unconstrained NUMERIC must map to decimal_arb, got {:?}",
        f.data_type()
    );
}

#[test]
fn bare_numeric_and_parameterised_38_9_agree() {
    assert_eq!(dt("NUMERIC"), dt("NUMERIC(38, 9)"));
}

#[test]
fn bare_numeric_storage_types_agree_between_entry_points() {
    assert_storage_types_agree("NUMERIC");
    assert_storage_types_agree("DECIMAL");
}

// ---------------------------------------------------------------------------
// 5. Negative scale
// ---------------------------------------------------------------------------

#[test]
fn negative_scale_in_decimal128_band_is_passed_through() {
    assert_eq!(
        dt("NUMERIC(20, -3)"),
        DataType::Decimal128(20, -3),
        "Postgres 15+ permits negative scale; Decimal128 represents it natively"
    );
}

#[test]
fn negative_scale_in_decimal256_band_is_passed_through() {
    assert_eq!(dt("NUMERIC(50, -3)"), DataType::Decimal256(50, -3));
}

#[test]
fn negative_scale_at_precision_76_is_still_decimal256() {
    assert_eq!(dt("NUMERIC(76, -1)"), DataType::Decimal256(76, -1));
}

#[test]
fn negative_scale_above_76_is_rejected_by_the_field_path() {
    let err = postgres_type_to_arrow_field("NUMERIC(100, -2)", "c", true);
    assert!(
        err.is_err(),
        "decimal_arb has no negative-scale representation; must reject"
    );
}

#[test]
fn negative_scale_at_77_is_rejected_by_the_field_path() {
    assert!(postgres_type_to_arrow_field("NUMERIC(77, -1)", "c", true).is_err());
}

#[test]
fn negative_scale_rejection_message_does_not_leak_beyond_the_type() {
    let err = postgres_type_to_arrow_field("NUMERIC(100, -2)", "c", true).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("negative scale"),
        "error must explain the negative-scale rejection, got: {msg}"
    );
}

#[test]
#[ignore = "FINDING: postgres_type_to_arrow_type accepts NUMERIC(p>76, negative scale) and returns bare LargeBinary while postgres_type_to_arrow_field rejects it — the DataType path yields an untyped byte column"]
fn negative_scale_above_76_must_not_silently_become_plain_large_binary() {
    // The two entry points must agree on validity. Today the DataType path
    // returns Ok(LargeBinary) for a spec the Field path calls an error, so a
    // caller on the DataType path (e.g. pg_aggregation's `override_type`)
    // materialises a plain, metadata-free LargeBinary column: raw bytes where
    // a number was expected.
    assert!(
        postgres_type_to_arrow_type("NUMERIC(100, -2)").is_err(),
        "must reject the same input the Field path rejects, got {:?}",
        postgres_type_to_arrow_type("NUMERIC(100, -2)")
    );
}

// ---------------------------------------------------------------------------
// 6. Scale > precision
// ---------------------------------------------------------------------------

#[test]
fn scale_greater_than_precision_above_76_is_rejected_by_the_field_path() {
    assert!(
        postgres_type_to_arrow_field("NUMERIC(100, 200)", "c", true).is_err(),
        "decimal_arb requires scale <= precision"
    );
}

#[test]
fn scale_equal_to_precision_above_76_is_accepted() {
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&fld("NUMERIC(100, 100)")),
        Some((100, 100)),
        "scale == precision is the inclusive boundary and must be legal"
    );
}

#[test]
fn scale_one_above_precision_above_76_is_rejected() {
    assert!(postgres_type_to_arrow_field("NUMERIC(100, 101)", "c", true).is_err());
}

#[test]
#[ignore = "FINDING: postgres_type_to_arrow_type returns Ok(LargeBinary) for NUMERIC(100,200) (scale > precision) which postgres_type_to_arrow_field rejects — the two entry points disagree on validity"]
fn scale_greater_than_precision_above_76_must_not_be_accepted_by_the_type_path() {
    assert!(
        postgres_type_to_arrow_type("NUMERIC(100, 200)").is_err(),
        "got {:?}",
        postgres_type_to_arrow_type("NUMERIC(100, 200)")
    );
}

#[test]
fn scale_greater_than_precision_in_decimal128_band_is_passed_through() {
    // Postgres 15+ allows scale > precision. Decimal128(5, 10) is not a legal
    // Arrow decimal, but the mapping produces it; pin the behaviour so a
    // deliberate change is visible.
    assert_eq!(dt("NUMERIC(5, 10)"), DataType::Decimal128(5, 10));
}

// ---------------------------------------------------------------------------
// 7. i8 scale truncation (the sharpest edge in this file)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "FINDING: NUMERIC scale is cast with `as i8`, so a legal Postgres scale > 127 wraps to a negative Arrow scale (e.g. NUMERIC(10,200) -> Decimal128(10,-56)) — silent 10^256 value corruption"]
fn numeric_scale_above_i8_range_must_not_wrap_negative() {
    match postgres_type_to_arrow_type("NUMERIC(10, 200)") {
        Err(_) => { /* rejecting loudly is acceptable */ }
        Ok(DataType::Decimal128(_, s)) => assert_eq!(
            s as i32, 200,
            "scale 200 wrapped to {s} via `as i8` — values are rescaled by 10^256"
        ),
        Ok(other) => panic!("unexpected type for NUMERIC(10, 200): {other:?}"),
    }
}

#[test]
#[ignore = "FINDING: NUMERIC scale is cast with `as i8`, so a legal Postgres negative scale below -128 flips sign (NUMERIC(10,-1000) -> Decimal128(10,24))"]
fn numeric_scale_below_i8_range_must_not_flip_sign() {
    match postgres_type_to_arrow_type("NUMERIC(10, -1000)") {
        Err(_) => {}
        Ok(DataType::Decimal128(_, s)) => assert!(
            s < 0,
            "negative scale -1000 became {s} via `as i8` — the sign flipped"
        ),
        Ok(other) => panic!("unexpected type: {other:?}"),
    }
}

#[test]
#[ignore = "FINDING: the `as i8` scale truncation also affects the Decimal256 band (NUMERIC(50,200) -> Decimal256(50,-56))"]
fn numeric_scale_truncation_also_affects_the_decimal256_band() {
    match postgres_type_to_arrow_type("NUMERIC(50, 200)") {
        Err(_) => {}
        Ok(DataType::Decimal256(_, s)) => assert_eq!(s as i32, 200, "scale wrapped to {s}"),
        Ok(other) => panic!("unexpected type: {other:?}"),
    }
}

#[test]
#[ignore = "FINDING: NUMERIC(10,128) maps to Decimal128(10,-128) — the exact i8 wrap point"]
fn numeric_scale_128_must_not_become_negative_128() {
    match postgres_type_to_arrow_type("NUMERIC(10, 128)") {
        Err(_) => {}
        Ok(DataType::Decimal128(_, s)) => assert_ne!(
            s, -128,
            "scale 128 wrapped to the i8 minimum; the column now scales by 10^128 the wrong way"
        ),
        Ok(other) => panic!("unexpected type: {other:?}"),
    }
}

#[test]
fn numeric_scale_127_is_the_last_faithfully_representable_positive_scale() {
    // Documents where the cliff is: 127 survives, 128 does not.
    assert_eq!(dt("NUMERIC(10, 127)"), DataType::Decimal128(10, 127));
}

#[test]
fn numeric_scale_minus_128_is_the_last_faithfully_representable_negative_scale() {
    assert_eq!(dt("NUMERIC(10, -128)"), DataType::Decimal128(10, -128));
}

#[test]
fn scale_within_i8_range_round_trips_exactly() {
    for s in -128i32..=127 {
        let pg = format!("NUMERIC(38, {s})");
        match dt(&pg) {
            DataType::Decimal128(_, got) => {
                assert_eq!(got as i32, s, "scale {s} not preserved for {pg}")
            }
            other => panic!("unexpected type {other:?} for {pg}"),
        }
    }
}

// ---------------------------------------------------------------------------
// 8. Precision 0 and absurd precisions
// ---------------------------------------------------------------------------

#[test]
fn precision_zero_does_not_panic() {
    let _ = postgres_type_to_arrow_type("NUMERIC(0, 0)");
    let _ = postgres_type_to_arrow_field("NUMERIC(0, 0)", "c", true);
}

#[test]
fn precision_zero_stays_in_the_decimal128_band() {
    assert_eq!(
        dt("NUMERIC(0, 0)"),
        DataType::Decimal128(0, 0),
        "p=0 falls in the p<=38 branch; pin the behaviour"
    );
}

#[test]
fn precision_zero_single_arg_form_does_not_panic() {
    assert_eq!(dt("NUMERIC(0)"), DataType::Decimal128(0, 0));
}

#[test]
fn precision_above_max_precision_is_rejected_by_the_field_path() {
    assert!(
        postgres_type_to_arrow_field("NUMERIC(65536, 0)", "c", true).is_err(),
        "MAX_PRECISION is 65535; 65536 must be rejected"
    );
}

#[test]
fn precision_at_u32_max_is_rejected_by_the_field_path() {
    assert!(postgres_type_to_arrow_field("NUMERIC(4294967295, 0)", "c", true).is_err());
}

#[test]
fn precision_above_u32_max_is_a_parse_error_on_both_paths() {
    assert!(postgres_type_to_arrow_type("NUMERIC(4294967296, 0)").is_err());
    assert!(postgres_type_to_arrow_field("NUMERIC(4294967296, 0)", "c", true).is_err());
}

#[test]
fn twenty_digit_precision_is_a_parse_error_not_a_panic() {
    assert!(postgres_type_to_arrow_type("NUMERIC(99999999999999999999, 0)").is_err());
    assert!(postgres_type_to_arrow_field("NUMERIC(99999999999999999999, 0)", "c", true).is_err());
}

#[test]
#[ignore = "FINDING: postgres_type_to_arrow_type returns Ok(LargeBinary) for NUMERIC(65536,0), a precision the decimal_arb field path rejects as above MAX_PRECISION"]
fn precision_above_max_precision_must_not_be_accepted_by_the_type_path() {
    assert!(
        postgres_type_to_arrow_type("NUMERIC(65536, 0)").is_err(),
        "got {:?}",
        postgres_type_to_arrow_type("NUMERIC(65536, 0)")
    );
}

#[test]
fn max_precision_boundary_is_accepted_but_one_past_is_not() {
    assert!(postgres_type_to_arrow_field("NUMERIC(65535, 0)", "c", true).is_ok());
    assert!(postgres_type_to_arrow_field("NUMERIC(65536, 0)", "c", true).is_err());
}

#[test]
fn precision_zero_field_path_matches_type_path() {
    assert_storage_types_agree("NUMERIC(0, 0)");
}

// ---------------------------------------------------------------------------
// 9. Malformed type strings
// ---------------------------------------------------------------------------

#[test]
fn unterminated_parenthesis_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(10, 2").is_err());
    assert!(postgres_type_to_arrow_field("NUMERIC(10, 2", "c", true).is_err());
}

#[test]
fn stray_closing_parenthesis_is_rejected() {
    assert!(
        postgres_type_to_arrow_type("NUMERIC)").is_err(),
        "'numeric)' is not a known base type"
    );
}

#[test]
fn empty_parameter_list_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC()").is_err());
}

#[test]
fn comma_only_parameter_list_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(,)").is_err());
}

#[test]
fn missing_scale_after_comma_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(10,)").is_err());
}

#[test]
fn missing_precision_before_comma_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(,2)").is_err());
}

#[test]
fn non_numeric_precision_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(abc, 2)").is_err());
}

#[test]
fn non_numeric_scale_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(10, abc)").is_err());
}

#[test]
fn fractional_precision_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(10.5, 2)").is_err());
}

#[test]
fn space_separated_params_without_comma_are_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(10 2)").is_err());
}

#[test]
fn nested_parentheses_in_params_are_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC((10), 2)").is_err());
}

#[test]
fn hex_literal_precision_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(0x10, 2)").is_err());
}

#[test]
fn underscore_separated_precision_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(1_0, 2)").is_err());
}

#[test]
fn fullwidth_unicode_digits_are_rejected_without_panicking() {
    assert!(postgres_type_to_arrow_type("NUMERIC(\u{ff11}\u{ff10}, 2)").is_err());
}

#[test]
fn empty_type_string_is_rejected() {
    assert!(postgres_type_to_arrow_type("").is_err());
    assert!(postgres_type_to_arrow_field("", "c", true).is_err());
}

#[test]
fn whitespace_only_type_string_is_rejected() {
    assert!(postgres_type_to_arrow_type("   ").is_err());
}

#[test]
fn unknown_base_type_is_rejected_with_the_original_spelling() {
    let err = postgres_type_to_arrow_type("MoNeY").unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("MoNeY"),
        "error must echo the original casing for diagnosability, got: {msg}"
    );
}

#[test]
#[ignore = "FINDING: NUMERIC(10,2,3) is accepted and the third parameter silently ignored — a malformed typmod produces a column instead of an error"]
fn extra_numeric_parameters_must_be_rejected() {
    assert!(
        postgres_type_to_arrow_type("NUMERIC(10, 2, 3)").is_err(),
        "got {:?}",
        postgres_type_to_arrow_type("NUMERIC(10, 2, 3)")
    );
}

#[test]
#[ignore = "FINDING: the array type NUMERIC(10,2)[] is silently mapped to the scalar Decimal128(10,2) because parsing only looks at the first '(' and last ')'"]
fn numeric_array_type_must_not_map_to_a_scalar_decimal() {
    assert!(
        postgres_type_to_arrow_type("NUMERIC(10,2)[]").is_err(),
        "array types must not be accepted as scalars, got {:?}",
        postgres_type_to_arrow_type("NUMERIC(10,2)[]")
    );
}

#[test]
#[ignore = "FINDING: NUMERIC(100,18)[] is silently mapped to a scalar decimal_arb(100,18) field"]
fn wide_numeric_array_type_must_not_map_to_a_scalar_decimal_arb() {
    let f = postgres_type_to_arrow_field("NUMERIC(100,18)[]", "c", true);
    assert!(
        f.is_err(),
        "array of wide numerics must not become a scalar decimal_arb, got {:?}",
        f.map(|f| f.data_type().clone())
    );
}

#[test]
fn bare_numeric_array_type_is_rejected() {
    // Without a typmod there is no '(' to hide behind, so this one does fail.
    assert!(postgres_type_to_arrow_type("NUMERIC[]").is_err());
}

#[test]
fn trailing_sql_noise_after_the_typmod_is_ignored() {
    // Documents the lenient behaviour: everything after the last ')' is ignored,
    // so a full column definition parses as the bare type.
    assert_eq!(
        dt("NUMERIC(10, 2) NOT NULL"),
        DataType::Decimal128(10, 2),
        "parsing keys off the first '(' and last ')' only"
    );
}

#[test]
fn malformed_inputs_never_panic() {
    let nasty = [
        "",
        " ",
        "(",
        ")",
        "()",
        "numeric(",
        "numeric)",
        "numeric()",
        "numeric(,",
        "numeric(,)",
        "numeric(-)",
        "numeric(-,-)",
        "numeric(1,",
        "numeric(,1)",
        "numeric(1,2,3,4,5)",
        "numeric(  )",
        "numeric(\0)",
        "numeric(\u{1F600})",
        "\u{1F600}",
        "NUMERIC(99999999999999999999999999,0)",
        "NUMERIC(-1,0)",
        "NUMERIC(1,-99999999999999999999)",
        "decimal(",
        "decimal()",
        "DECIMAL(NaN, NaN)",
        "DECIMAL(inf, 0)",
        "numeric(0,0)",
        "numeric (78 , 0)",
        "NUMERIC(10,2)[][]",
        "numeric(1e3,0)",
    ];
    for input in nasty {
        let _ = postgres_type_to_arrow_type(input);
        let _ = postgres_type_to_arrow_field(input, "c", true);
    }
}

#[test]
fn negative_precision_is_rejected() {
    assert!(postgres_type_to_arrow_type("NUMERIC(-1, 0)").is_err());
}

#[test]
fn storage_types_agree_for_every_well_formed_input_in_a_sweep() {
    for p in [1u32, 5, 38, 39, 50, 76, 77, 78, 100, 65535] {
        for s in [0u32, 1, 5] {
            if s > p {
                continue;
            }
            assert_storage_types_agree(&format!("NUMERIC({p}, {s})"));
            assert_storage_types_agree(&format!("decimal({p},{s})"));
        }
    }
}

// ---------------------------------------------------------------------------
// 10. The u256 native_int_kind hint
// ---------------------------------------------------------------------------

#[test]
fn numeric_78_0_carries_the_u256_hint() {
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&fld("NUMERIC(78, 0)")),
        Some(NativeIntKind::U256)
    );
}

#[test]
fn numeric_78_0_hint_does_not_disturb_precision_scale_metadata() {
    let f = fld("NUMERIC(78, 0)");
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((78, 0)),
        "the hint must be a separate metadata key, not replace the extension metadata"
    );
    assert!(DecimalArbType::is_decimal_arb_field(&f));
}

#[test]
fn numeric_78_1_does_not_carry_the_u256_hint() {
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&fld("NUMERIC(78, 1)")),
        None,
        "the hint is specific to the (78, 0) uint256 shape"
    );
}

#[test]
fn numeric_77_0_does_not_carry_the_u256_hint() {
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&fld("NUMERIC(77, 0)")),
        None
    );
}

#[test]
fn numeric_79_0_does_not_carry_the_u256_hint() {
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&fld("NUMERIC(79, 0)")),
        None
    );
}

#[test]
fn numeric_100_18_does_not_carry_the_u256_hint() {
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&fld("NUMERIC(100, 18)")),
        None
    );
}

#[test]
fn hint_is_absent_for_every_precision_except_78_at_scale_zero() {
    for p in 77u32..=90 {
        let hint = DecimalArbType::native_int_kind_from_field(&fld(&format!("NUMERIC({p}, 0)")));
        assert_eq!(
            hint,
            if p == 78 {
                Some(NativeIntKind::U256)
            } else {
                None
            },
            "u256 hint must appear at exactly precision 78 scale 0 (p={p})"
        );
    }
}

#[test]
fn hint_never_appears_below_the_decimal_arb_band() {
    for p in 1u32..=76 {
        let f = fld(&format!("NUMERIC({p}, 0)"));
        assert_eq!(
            DecimalArbType::native_int_kind_from_field(&f),
            None,
            "narrow numerics are not decimal_arb and cannot carry the hint (p={p})"
        );
    }
}

#[test]
fn hint_is_never_i256_from_the_postgres_side() {
    for pg in ["NUMERIC(78, 0)", "DECIMAL(78,0)", "numeric(78,0)"] {
        assert_ne!(
            DecimalArbType::native_int_kind_from_field(&fld(pg)),
            Some(NativeIntKind::I256),
            "Postgres NUMERIC carries no signedness; i256 must never be inferred"
        );
    }
}

#[test]
fn nullable_flag_is_honoured_for_hinted_fields() {
    let f = postgres_type_to_arrow_field("NUMERIC(78, 0)", "amount", false).unwrap();
    assert!(
        !f.is_nullable(),
        "nullability must survive the hint stamping"
    );
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        Some(NativeIntKind::U256)
    );
}

#[test]
fn field_name_is_preserved_through_the_decimal_arb_path() {
    let f = postgres_type_to_arrow_field("NUMERIC(100, 18)", "weird name-1", true).unwrap();
    assert_eq!(f.name(), "weird name-1");
}

#[test]
fn field_name_is_preserved_through_the_hinted_path() {
    let f = postgres_type_to_arrow_field("NUMERIC(78, 0)", "balance", true).unwrap();
    assert_eq!(f.name(), "balance");
}

#[test]
fn wide_numeric_field_metadata_matches_the_canonical_constructor() {
    let from_pg = postgres_type_to_arrow_field("NUMERIC(100, 18)", "c", true).unwrap();
    let canonical = arb(100, 18);
    assert_eq!(
        from_pg.metadata(),
        canonical.metadata(),
        "the Postgres path must produce byte-identical extension metadata"
    );
}

#[test]
fn nullable_flag_is_honoured_for_plain_decimal_arb_fields() {
    let f = postgres_type_to_arrow_field("NUMERIC(100, 18)", "c", false).unwrap();
    assert!(!f.is_nullable());
}

// ---------------------------------------------------------------------------
// 11. arrow_field_to_postgres_type as an inverse
// ---------------------------------------------------------------------------

#[test]
fn decimal_arb_field_renders_as_numeric_with_the_same_params() {
    assert_eq!(
        arrow_field_to_postgres_type(&arb(100, 18)),
        "NUMERIC(100, 18)"
    );
}

#[test]
fn decimal_arb_round_trips_through_the_postgres_type_string() {
    for (p, s) in [(77u32, 0u32), (78, 0), (100, 18), (200, 100), (65535, 0)] {
        let pg = arrow_field_to_postgres_type(&arb(p, s));
        let back = postgres_type_to_arrow_field(&pg, "c", true)
            .unwrap_or_else(|e| panic!("{pg} must re-parse: {e:?}"));
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&back),
            Some((p, s)),
            "round trip lost params for decimal_arb({p}, {s}) via {pg}"
        );
    }
}

#[test]
fn decimal_arb_pg_type_string_is_idempotent() {
    let once = arrow_field_to_postgres_type(&arb(100, 18));
    let back = postgres_type_to_arrow_field(&once, "c", true).unwrap();
    let twice = arrow_field_to_postgres_type(&back);
    assert_eq!(
        once, twice,
        "the rendered type string must be a fixed point"
    );
}

#[test]
fn decimal128_round_trips_through_the_postgres_type_string() {
    for (p, s) in [(1u8, 0i8), (10, 2), (38, 0), (38, 38)] {
        let f = Field::new("c", DataType::Decimal128(p, s), true);
        let pg = arrow_field_to_postgres_type(&f);
        assert_eq!(
            dt(&pg),
            DataType::Decimal128(p, s),
            "round trip failed for {pg}"
        );
    }
}

#[test]
fn decimal256_above_38_round_trips_as_decimal256() {
    for (p, s) in [(39u8, 0i8), (50, 10), (76, 38)] {
        let f = Field::new("c", DataType::Decimal256(p, s), true);
        let pg = arrow_field_to_postgres_type(&f);
        assert_eq!(
            dt(&pg),
            DataType::Decimal256(p, s),
            "round trip failed for {pg}"
        );
    }
}

#[test]
fn decimal256_at_or_below_38_narrows_to_decimal128_on_round_trip() {
    // Documented lossiness: the type string carries no width, only (p, s), so
    // a narrow Decimal256 comes back as Decimal128. Value range is unchanged.
    let f = Field::new("c", DataType::Decimal256(20, 4), true);
    assert_eq!(
        dt(&arrow_field_to_postgres_type(&f)),
        DataType::Decimal128(20, 4)
    );
}

#[test]
fn decimal_arb_below_77_narrows_to_a_fixed_width_decimal_on_round_trip() {
    // A decimal_arb(40, 2) column renders as NUMERIC(40, 2), which re-reads as
    // Decimal256(40, 2). The extension metadata is dropped but the numeric
    // range is fully preserved, so this is lossless for values.
    let pg = arrow_field_to_postgres_type(&arb(40, 2));
    assert_eq!(pg, "NUMERIC(40, 2)");
    let back = postgres_type_to_arrow_field(&pg, "c", true).unwrap();
    assert_eq!(back.data_type(), &DataType::Decimal256(40, 2));
    assert!(!DecimalArbType::is_decimal_arb_field(&back));
}

#[test]
fn decimal_arb_below_39_narrows_to_decimal128_on_round_trip() {
    let back = postgres_type_to_arrow_field(&arrow_field_to_postgres_type(&arb(20, 2)), "c", true)
        .unwrap();
    assert_eq!(back.data_type(), &DataType::Decimal128(20, 2));
}

#[test]
fn i256_hinted_decimal_arb_78_0_comes_back_as_u256() {
    // Pins the documented asymmetry: Postgres NUMERIC has no signedness, so the
    // (78, 0) shape is always re-read as u256. An i256-origin column that goes
    // out to Postgres and back loses its signed origin hint.
    let signed = DecimalArbType::with_native_int_kind(arb(78, 0), NativeIntKind::I256).unwrap();
    let pg = arrow_field_to_postgres_type(&signed);
    assert_eq!(pg, "NUMERIC(78, 0)");
    let back = postgres_type_to_arrow_field(&pg, "c", true).unwrap();
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&back),
        Some(NativeIntKind::U256),
        "documented: the Postgres side has no i256 path"
    );
}

#[test]
fn u256_hint_does_not_change_the_rendered_postgres_type() {
    let hinted = DecimalArbType::with_native_int_kind(arb(78, 0), NativeIntKind::U256).unwrap();
    assert_eq!(
        arrow_field_to_postgres_type(&hinted),
        arrow_field_to_postgres_type(&arb(78, 0)),
        "the origin hint must not leak into the DDL type"
    );
}

#[test]
fn wide_numeric_ddl_string_is_valid_input_to_the_parser_for_every_band() {
    for p in [1u32, 38, 39, 76, 77, 78, 100, 1000] {
        let f = if p > 76 {
            arb(p, 0)
        } else {
            fld(&format!("NUMERIC({p}, 0)"))
        };
        let pg = arrow_field_to_postgres_type(&f);
        assert!(
            postgres_type_to_arrow_type(&pg).is_ok(),
            "emitted DDL type {pg:?} must be re-parseable"
        );
    }
}

#[test]
fn rendered_type_uses_a_space_after_the_comma() {
    assert_eq!(
        arrow_field_to_postgres_type(&arb(100, 18)),
        "NUMERIC(100, 18)"
    );
    assert_eq!(
        arrow_field_to_postgres_type(&Field::new("c", DataType::Decimal128(10, 2), true)),
        "NUMERIC(10, 2)"
    );
}

#[test]
fn uint64_renders_as_numeric_20_0_without_a_space() {
    // Historical spelling difference; pinned so a formatting change is visible.
    assert_eq!(
        arrow_field_to_postgres_type(&Field::new("c", DataType::UInt64, true)),
        "NUMERIC(20,0)"
    );
}

// ---------------------------------------------------------------------------
// 12. get_postgres_type_info — cast / bind decisions
// ---------------------------------------------------------------------------

#[test]
fn decimal_arb_column_binds_as_string_with_a_numeric_cast() {
    let info = get_postgres_type_info(&arb(100, 18));
    assert_eq!(info.column_type, "NUMERIC(100, 18)");
    assert_eq!(
        info.string_cast_sql,
        Some("numeric(100,18)".to_string()),
        "decimal_arb must bind as text and cast server-side"
    );
}

#[test]
fn decimal_arb_cast_sql_is_lowercase_and_compact() {
    let info = get_postgres_type_info(&arb(78, 0));
    assert_eq!(info.string_cast_sql.as_deref(), Some("numeric(78,0)"));
}

#[test]
fn decimal_arb_cast_matches_the_declared_column_type_semantically() {
    for (p, s) in [(77u32, 0u32), (78, 0), (100, 18), (65535, 0)] {
        let info = get_postgres_type_info(&arb(p, s));
        assert_eq!(info.column_type, format!("NUMERIC({p}, {s})"));
        assert_eq!(
            info.string_cast_sql,
            Some(format!("numeric({p},{s})")),
            "cast and column type must describe the same numeric for ({p}, {s})"
        );
    }
}

#[test]
fn decimal_arb_always_requires_a_cast() {
    for (p, s) in [(1u32, 0u32), (38, 9), (77, 0), (100, 18), (1000, 500)] {
        assert!(
            get_postgres_type_info(&arb(p, s)).string_cast_sql.is_some(),
            "decimal_arb({p}, {s}) must never bind natively"
        );
    }
}

#[test]
fn u256_hint_does_not_change_the_cast_decision() {
    let hinted = DecimalArbType::with_native_int_kind(arb(78, 0), NativeIntKind::U256).unwrap();
    assert_eq!(
        get_postgres_type_info(&hinted),
        get_postgres_type_info(&arb(78, 0))
    );
}

#[test]
fn i256_hint_does_not_change_the_cast_decision() {
    let hinted = DecimalArbType::with_native_int_kind(arb(78, 0), NativeIntKind::I256).unwrap();
    assert_eq!(
        get_postgres_type_info(&hinted),
        get_postgres_type_info(&arb(78, 0))
    );
}

#[test]
fn decimal_arb_is_checked_before_the_large_binary_catch_all() {
    // The whole point of the pre-check: a decimal_arb column must not fall
    // through to BYTEA just because its storage type is LargeBinary.
    assert_ne!(
        get_postgres_type_info(&arb(100, 18)).column_type,
        "BYTEA",
        "decimal_arb must not be shaped as BYTEA"
    );
}

#[test]
fn plain_large_binary_without_metadata_stays_bytea() {
    let info = get_postgres_type_info(&Field::new("blob", DataType::LargeBinary, true));
    assert_eq!(info.column_type, "BYTEA");
    assert_eq!(info.string_cast_sql, None);
}

#[test]
fn large_binary_with_unrelated_metadata_stays_bytea() {
    let mut md = HashMap::new();
    md.insert("some.key".to_string(), "value".to_string());
    let f = Field::new("blob", DataType::LargeBinary, true).with_metadata(md);
    assert_eq!(get_postgres_type_info(&f).column_type, "BYTEA");
}

#[test]
fn decimal_arb_metadata_on_a_non_large_binary_type_is_not_honoured() {
    // Documents the ClickHouse-normalisation hazard: a field that keeps the
    // decimal_arb metadata but whose storage type has been rewritten to
    // FixedSizeBinary(32) is *not* recognised and lands in BYTEA.
    let f = Field::new("c", DataType::FixedSizeBinary(32), true)
        .with_metadata(arb(78, 0).metadata().clone());
    let info = get_postgres_type_info(&f);
    assert_eq!(
        info.column_type, "BYTEA",
        "storage-type mismatch means the metadata is ignored"
    );
    assert_eq!(info.string_cast_sql, None);
}

#[test]
#[ignore = "FINDING: a LargeBinary field advertising ARROW:extension:name=streamling.decimal_arb but with missing/corrupt extension metadata is silently mapped to BYTEA instead of erroring, writing canonical decimal bytes into a byte column"]
fn decimal_arb_named_field_with_corrupt_metadata_must_not_silently_become_bytea() {
    let mut md = HashMap::new();
    md.insert(
        DecimalArbType::EXTENSION_NAME_KEY.to_string(),
        DecimalArbType::EXTENSION_NAME.to_string(),
    );
    md.insert(
        DecimalArbType::EXTENSION_METADATA_KEY.to_string(),
        "{\"precision\":}".to_string(),
    );
    let f = Field::new("c", DataType::LargeBinary, true).with_metadata(md);
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "precondition: the field still advertises the extension name"
    );
    assert_ne!(
        get_postgres_type_info(&f).column_type,
        "BYTEA",
        "a column claiming to be decimal_arb must not degrade to raw bytes"
    );
}

#[test]
fn uint64_binds_as_string_with_a_numeric_cast() {
    let info = get_postgres_type_info(&Field::new("c", DataType::UInt64, true));
    assert_eq!(info.column_type, "NUMERIC(20,0)");
    assert_eq!(info.string_cast_sql.as_deref(), Some("numeric(20,0)"));
}

#[test]
fn decimal128_binds_as_string_with_a_numeric_cast() {
    let info = get_postgres_type_info(&Field::new("c", DataType::Decimal128(10, 2), true));
    assert_eq!(info.string_cast_sql.as_deref(), Some("numeric(10,2)"));
}

#[test]
fn decimal256_binds_as_string_with_a_numeric_cast() {
    let info = get_postgres_type_info(&Field::new("c", DataType::Decimal256(50, 10), true));
    assert_eq!(info.column_type, "NUMERIC(50, 10)");
    assert_eq!(info.string_cast_sql.as_deref(), Some("numeric(50,10)"));
}

#[test]
fn integer_types_bind_natively_without_a_cast() {
    for t in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::Boolean,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
    ] {
        assert_eq!(
            get_postgres_type_info(&Field::new("c", t.clone(), true)).string_cast_sql,
            None,
            "{t:?} must bind natively"
        );
    }
}

#[test]
fn every_numeric_shaped_column_type_carries_a_cast() {
    // Invariant: if the DDL type is NUMERIC(...) then the bind path must be
    // string + cast, otherwise sqlx would try to encode a Rust native type
    // into a NUMERIC column.
    let fields = vec![
        arb(100, 18),
        arb(78, 0),
        Field::new("c", DataType::UInt64, true),
        Field::new("c", DataType::Decimal128(10, 2), true),
        Field::new("c", DataType::Decimal256(50, 10), true),
    ];
    for f in fields {
        let info = get_postgres_type_info(&f);
        if info.column_type.starts_with("NUMERIC") {
            assert!(
                info.string_cast_sql.is_some(),
                "NUMERIC column {:?} must bind as string with a cast",
                f.data_type()
            );
        }
    }
}

#[test]
fn cast_sql_is_always_a_lowercase_form_of_the_column_type() {
    for f in [arb(100, 18), arb(1, 0), arb(65535, 0)] {
        let info = get_postgres_type_info(&f);
        let cast = info.string_cast_sql.unwrap();
        let normalised = info.column_type.to_lowercase().replace(' ', "");
        assert_eq!(
            cast, normalised,
            "cast expression must denote the same type as the DDL column type"
        );
    }
}

#[test]
fn arrow_field_to_postgres_type_agrees_with_get_postgres_type_info() {
    for f in [
        arb(100, 18),
        arb(78, 0),
        Field::new("c", DataType::LargeBinary, true),
        Field::new("c", DataType::Int64, true),
        Field::new("c", DataType::Decimal256(50, 10), true),
    ] {
        assert_eq!(
            arrow_field_to_postgres_type(&f),
            get_postgres_type_info(&f).column_type,
            "the two Arrow->PG entry points must not diverge"
        );
    }
}

#[test]
fn decimal_arb_with_scale_equal_to_precision_renders_correctly() {
    let info = get_postgres_type_info(&arb(100, 100));
    assert_eq!(info.column_type, "NUMERIC(100, 100)");
    assert_eq!(info.string_cast_sql.as_deref(), Some("numeric(100,100)"));
}

#[test]
fn decimal_arb_never_renders_a_negative_scale() {
    for (p, s) in [(77u32, 0u32), (100, 18), (65535, 65535)] {
        let ct = get_postgres_type_info(&arb(p, s)).column_type;
        assert!(
            !ct.contains('-'),
            "decimal_arb scale is unsigned; {ct} must not contain a minus sign"
        );
    }
}

// ---------------------------------------------------------------------------
// 13. Connector-side cast plumbing (PostgresQueryBuilder)
// ---------------------------------------------------------------------------

#[test]
fn cast_map_marks_decimal_arb_columns_for_casting() {
    let schema = schema_of(vec![Field::new("id", DataType::Int64, false), arb(100, 18)]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["id", "c"]));
    assert_eq!(map.get("id"), Some(&None::<String>));
    assert_eq!(map.get("c"), Some(&Some("numeric(100,18)".to_string())));
}

#[test]
fn cast_map_omits_columns_not_in_the_requested_list() {
    let schema = schema_of(vec![Field::new("id", DataType::Int64, false), arb(100, 18)]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["id"]));
    assert!(
        !map.contains_key("c"),
        "cast map must be scoped to the projected columns"
    );
}

#[test]
fn cast_map_excludes_the_gs_op_column() {
    let schema = schema_of(vec![
        Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        arb(100, 18),
    ]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&[COLUMN_NAME_OP, "c"]));
    assert!(!map.contains_key(COLUMN_NAME_OP));
    assert!(map.contains_key("c"));
}

#[test]
fn cast_map_handles_the_u256_hinted_column_identically() {
    let hinted = DecimalArbType::with_native_int_kind(arb(78, 0), NativeIntKind::U256).unwrap();
    let schema = schema_of(vec![hinted]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["c"]));
    assert_eq!(map.get("c"), Some(&Some("numeric(78,0)".to_string())));
}

#[test]
fn values_clause_applies_the_decimal_arb_cast_to_every_placeholder() {
    let schema = schema_of(vec![arb(100, 18)]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["c"]));
    let clause = PostgresQueryBuilder::build_values_clause(2, 1, &names(&["c"]), &map);
    assert_eq!(clause, "($1::numeric(100,18)), ($2::numeric(100,18))");
}

#[test]
fn values_clause_leaves_native_columns_uncast() {
    let schema = schema_of(vec![Field::new("id", DataType::Int64, false)]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["id"]));
    assert_eq!(
        PostgresQueryBuilder::build_values_clause(1, 1, &names(&["id"]), &map),
        "($1)"
    );
}

#[test]
fn values_clause_mixes_cast_and_native_columns_in_order() {
    let schema = schema_of(vec![
        Field::new("id", DataType::Int64, false),
        arb(100, 18),
        Field::new("name", DataType::Utf8, true),
    ]);
    let cols = names(&["id", "c", "name"]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &cols);
    assert_eq!(
        PostgresQueryBuilder::build_values_clause(1, 3, &cols, &map),
        "($1, $2::numeric(100,18), $3)"
    );
}

#[test]
fn complete_upsert_query_casts_decimal_arb_columns() {
    let schema = schema_of(vec![Field::new("id", DataType::Int64, false), arb(100, 18)]);
    let sql = PostgresQueryBuilder::build_complete_upsert_query(
        "public",
        "t",
        &names(&["id", "c"]),
        &names(&["id"]),
        1,
        Some(&schema),
        "update",
        None,
        "t",
        None,
    );
    assert!(
        sql.contains("$2::numeric(100,18)"),
        "decimal_arb column must carry its cast into the INSERT: {sql}"
    );
}

#[test]
fn complete_upsert_query_without_original_schema_emits_no_casts() {
    let sql = PostgresQueryBuilder::build_complete_upsert_query(
        "public",
        "t",
        &names(&["id", "c"]),
        &names(&["id"]),
        1,
        None,
        "update",
        None,
        "t",
        None,
    );
    assert!(!sql.contains("::"), "no schema means no casts: {sql}");
}

#[test]
fn checkpoint_epoch_column_is_never_cast() {
    let schema = schema_of(vec![Field::new("id", DataType::Int64, false), arb(100, 18)]);
    let sql = PostgresQueryBuilder::build_complete_upsert_query(
        "public",
        "t",
        &names(&["id", "c"]),
        &names(&["id"]),
        1,
        Some(&schema),
        "update",
        None,
        "t",
        Some(7),
    );
    assert!(sql.contains("_gs_checkpoint_epoch"), "{sql}");
    assert!(
        sql.contains("$3)") || sql.contains("$3,") || sql.contains("$3 "),
        "the epoch placeholder must be uncast: {sql}"
    );
}

#[test]
fn multi_row_upsert_numbers_placeholders_consecutively_with_casts() {
    let schema = schema_of(vec![arb(78, 0)]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["c"]));
    let clause = PostgresQueryBuilder::build_values_clause(3, 1, &names(&["c"]), &map);
    assert_eq!(
        clause,
        "($1::numeric(78,0)), ($2::numeric(78,0)), ($3::numeric(78,0))"
    );
}

#[test]
fn cast_expression_from_a_wide_decimal_arb_is_syntactically_well_formed() {
    let schema = schema_of(vec![arb(65535, 0)]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["c"]));
    let cast = map.get("c").unwrap().clone().unwrap();
    assert!(
        cast.starts_with("numeric(") && cast.ends_with(')'),
        "{cast}"
    );
    assert_eq!(cast.matches('(').count(), 1);
    assert_eq!(cast.matches(')').count(), 1);
}

#[test]
fn cast_map_is_stable_across_repeated_calls() {
    let schema = schema_of(vec![arb(100, 18)]);
    let a = PostgresQueryBuilder::build_cast_map(&schema, &names(&["c"]));
    let b = PostgresQueryBuilder::build_cast_map(&schema, &names(&["c"]));
    assert_eq!(a, b);
}

// ---------------------------------------------------------------------------
// 14. Non-decimal type strings (guard rails around the numeric branch)
// ---------------------------------------------------------------------------

#[test]
fn common_scalar_types_map_as_documented() {
    assert_eq!(dt("TEXT"), DataType::Utf8);
    assert_eq!(dt("VARCHAR"), DataType::Utf8);
    assert_eq!(dt("CHAR"), DataType::Utf8);
    assert_eq!(dt("SMALLINT"), DataType::Int16);
    assert_eq!(dt("INTEGER"), DataType::Int32);
    assert_eq!(dt("INT"), DataType::Int32);
    assert_eq!(dt("BIGINT"), DataType::Int64);
    assert_eq!(dt("REAL"), DataType::Float32);
    assert_eq!(dt("DOUBLE PRECISION"), DataType::Float64);
    assert_eq!(dt("FLOAT"), DataType::Float64);
    assert_eq!(dt("BOOLEAN"), DataType::Boolean);
    assert_eq!(dt("BOOL"), DataType::Boolean);
    assert_eq!(dt("BYTEA"), DataType::Binary);
    assert_eq!(dt("DATE"), DataType::Date32);
    assert_eq!(dt("JSONB"), DataType::Utf8);
    assert_eq!(dt("JSON"), DataType::Utf8);
}

#[test]
fn varchar_with_a_length_modifier_is_accepted() {
    assert_eq!(
        dt("VARCHAR(255)"),
        DataType::Utf8,
        "the typmod must be ignored for string types, not rejected"
    );
}

#[test]
fn timestamp_variants_map_to_microsecond_timestamps() {
    assert_eq!(
        dt("TIMESTAMP"),
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
    );
    assert_eq!(
        dt("TIMESTAMPTZ"),
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
    );
}

#[test]
fn timestamp_with_time_zone_spelling_is_rejected_loudly() {
    // information_schema spells it out; the mapper only knows `timestamptz`.
    // Failing loudly (rather than silently producing TEXT) is the safe outcome.
    assert!(postgres_type_to_arrow_type("timestamp with time zone").is_err());
}

#[test]
fn character_varying_spelling_is_rejected_loudly() {
    assert!(postgres_type_to_arrow_type("character varying").is_err());
}

#[test]
fn postgres_internal_int_aliases_are_rejected_loudly() {
    for t in ["int2", "int4", "int8", "float4", "float8"] {
        assert!(
            postgres_type_to_arrow_type(t).is_err(),
            "{t} is not in the supported set; it must error rather than default"
        );
    }
}

#[test]
fn non_numeric_types_never_produce_decimal_arb_fields() {
    for t in [
        "TEXT",
        "VARCHAR",
        "CHAR",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "REAL",
        "BOOLEAN",
        "BYTEA",
        "DATE",
        "JSONB",
        "TIMESTAMP",
    ] {
        assert!(
            !DecimalArbType::is_decimal_arb_field(&fld(t)),
            "{t} must not be mistaken for decimal_arb"
        );
    }
}

#[test]
fn non_numeric_types_agree_between_entry_points() {
    for t in [
        "TEXT",
        "VARCHAR(10)",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "REAL",
        "DOUBLE PRECISION",
        "BOOLEAN",
        "BYTEA",
        "DATE",
        "JSONB",
        "TIMESTAMP",
        "TIMESTAMPTZ",
    ] {
        assert_storage_types_agree(t);
    }
}

#[test]
fn substring_types_do_not_match_the_numeric_branch() {
    for t in ["numerical", "decimals", "prenumeric", "mynumeric(10,2)"] {
        assert!(
            postgres_type_to_arrow_type(t).is_err(),
            "{t} must not be treated as NUMERIC"
        );
    }
}

#[test]
fn numeric_prefix_with_a_paren_still_requires_an_exact_base_match() {
    assert!(postgres_type_to_arrow_type("numeric_wide(100, 18)").is_err());
}
