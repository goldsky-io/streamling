//! Adversarial agent 10 — `convert_avro_schema_to_arrow` routing across the
//! full avro decimal matrix.
//!
//! Routing contract under test (from
//! `crates/streamling-common/src/formats/avro/schema.rs`):
//!
//! | avro `decimal(p, s)`      | arrow type                                  |
//! |---------------------------|---------------------------------------------|
//! | `p <= 38`                 | `Decimal128(p, s)`                          |
//! | `39 <= p <= 76`           | `Decimal256(p, s)`                          |
//! | `p in 77..=78`, `s == 0`  | `decimal_arb(p, 0)` + `native_int_kind=u256`|
//! | `p > 78`, `s == 0`        | `decimal_arb(p, 0)`, **no** hint            |
//! | `p > 76`, `s != 0`        | `decimal_arb(p, s)`, **no** hint            |
//!
//! Plus: `MAX_SCHEMA_PRECISION = 100` guards the *top-level* path.
//!
//! Every test asserts one property and says which invariant broke. Tests that
//! are `#[ignore]`d carry a `FINDING:` note — they encode the behaviour the
//! contract implies, and currently fail against the product.

use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use apache_avro::schema::{
    DecimalSchema, Name, RecordField, RecordFieldOrder, RecordSchema, Schema as AvroSchema,
};
use arrow_schema::{DataType, Field};

use streamling_common::formats::avro::arrow_avro::AVRO_DECIMAL_SCALE_META;
use streamling_common::formats::avro::{
    convert_avro_schema_to_arrow, post_process_avro_schema_for_reading,
};
use streamling_common::types::decimal_arb::{DecimalArbType, NativeIntKind};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// A bytes-backed avro `decimal(p, s)` type literal.
fn bytes_dec(p: usize, s: usize) -> String {
    format!(r#"{{"type":"bytes","logicalType":"decimal","precision":{p},"scale":{s}}}"#)
}

/// A fixed-backed avro `decimal(p, s)` type literal. `size` is chosen large
/// enough for `p` digits (~2.41 digits/byte, so `p/2 + 2` always suffices).
fn fixed_dec(unique_name: &str, p: usize, s: usize) -> String {
    let size = p / 2 + 2;
    format!(
        r#"{{"type":"fixed","name":"{unique_name}","size":{size},"logicalType":"decimal","precision":{p},"scale":{s}}}"#
    )
}

fn nullable(inner: &str) -> String {
    format!(r#"["null",{inner}]"#)
}

fn array_of(inner: &str) -> String {
    format!(r#"{{"type":"array","items":{inner}}}"#)
}

fn map_of(inner: &str) -> String {
    format!(r#"{{"type":"map","values":{inner}}}"#)
}

fn struct_of(rec_name: &str, field_name: &str, inner: &str) -> String {
    format!(
        r#"{{"type":"record","name":"{rec_name}","fields":[{{"name":"{field_name}","type":{inner}}}]}}"#
    )
}

/// Build a top-level avro record from `(field_name, type_json)` pairs.
fn record_of(fields: &[(&str, String)]) -> AvroSchema {
    let body: Vec<String> = fields
        .iter()
        .map(|(n, t)| format!(r#"{{"name":"{n}","type":{t}}}"#))
        .collect();
    let json = format!(
        r#"{{"type":"record","name":"adv10","fields":[{}]}}"#,
        body.join(",")
    );
    AvroSchema::parse_str(&json).unwrap_or_else(|e| panic!("avro parse failed for {json}: {e}"))
}

/// Convert a single-field record and hand back the resulting arrow field.
fn convert_one(type_json: &str) -> Field {
    let schema = record_of(&[("f", type_json.to_string())]);
    let arrow = convert_avro_schema_to_arrow(schema);
    assert_eq!(
        arrow.fields().len(),
        1,
        "single-field avro record must yield exactly one arrow field"
    );
    arrow.field(0).clone()
}

fn convert_dec(p: usize, s: usize) -> Field {
    convert_one(&bytes_dec(p, s))
}

/// The band the routing table says `(p, s)` belongs to.
#[derive(Debug, PartialEq, Eq)]
enum Band {
    D128,
    D256,
    ArbHinted,
    ArbPlain,
}

fn expected_band(p: usize, s: usize) -> Band {
    if p <= 38 {
        Band::D128
    } else if p <= 76 {
        Band::D256
    } else if s == 0 && (77..=78).contains(&p) {
        Band::ArbHinted
    } else {
        Band::ArbPlain
    }
}

/// Assert that `f` matches the band the routing table prescribes for `(p, s)`,
/// including the exact carried precision/scale and hint presence.
fn assert_band(p: usize, s: usize, f: &Field, ctx: &str) {
    match expected_band(p, s) {
        Band::D128 => {
            assert_eq!(
                f.data_type(),
                &DataType::Decimal128(p as u8, s as i8),
                "{ctx}: avro decimal({p},{s}) must route to Decimal128({p},{s}) (p<=38 band)"
            );
            assert!(
                !DecimalArbType::is_decimal_arb_field(f),
                "{ctx}: avro decimal({p},{s}) is in the Decimal128 band and must not \
                 be stamped as decimal_arb"
            );
        }
        Band::D256 => {
            assert_eq!(
                f.data_type(),
                &DataType::Decimal256(p as u8, s as i8),
                "{ctx}: avro decimal({p},{s}) must route to Decimal256({p},{s}) (39..=76 band)"
            );
            assert!(
                !DecimalArbType::is_decimal_arb_field(f),
                "{ctx}: avro decimal({p},{s}) is in the Decimal256 band and must not \
                 be stamped as decimal_arb"
            );
        }
        Band::ArbHinted | Band::ArbPlain => {
            assert!(
                DecimalArbType::is_decimal_arb_field(f),
                "{ctx}: avro decimal({p},{s}) has p>76 and must route to decimal_arb, got {:?} \
                 meta={:?}",
                f.data_type(),
                f.metadata()
            );
            assert_eq!(
                f.data_type(),
                &DataType::LargeBinary,
                "{ctx}: decimal_arb storage type must be LargeBinary for decimal({p},{s})"
            );
            assert_eq!(
                DecimalArbType::precision_scale_from_field(f),
                Some((p as u32, s as u32)),
                "{ctx}: decimal_arb field must carry the declared avro precision/scale ({p},{s})"
            );
            let want_hint = expected_band(p, s) == Band::ArbHinted;
            assert_eq!(
                DecimalArbType::native_int_kind_from_field(f),
                if want_hint {
                    Some(NativeIntKind::U256)
                } else {
                    None
                },
                "{ctx}: native_int_kind hint must be present iff p in 77..=78 and s == 0 \
                 (p={p}, s={s})"
            );
        }
    }
}

/// Manually build a record schema carrying an arbitrary `DecimalSchema`
/// (bypasses apache-avro's parse-time validation, which silently downgrades
/// invalid decimal logical types to their underlying primitive).
fn manual_decimal_record(field_name: &str, precision: usize, scale: usize) -> AvroSchema {
    AvroSchema::Record(RecordSchema {
        name: Name::new("adv10manual").unwrap(),
        aliases: None,
        doc: None,
        fields: vec![RecordField {
            name: field_name.to_string(),
            doc: None,
            aliases: None,
            default: None,
            schema: AvroSchema::Decimal(DecimalSchema {
                precision,
                scale,
                inner: Box::new(AvroSchema::Bytes),
            }),
            order: RecordFieldOrder::Ascending,
            position: 0,
            custom_attributes: BTreeMap::new(),
        }],
        lookup: BTreeMap::new(),
        attributes: BTreeMap::new(),
    })
}

/// Run `f`, returning `true` if it panicked. Panic output is silenced for the
/// duration so an expected panic doesn't pollute the test log.
fn panics<F: FnOnce()>(f: F) -> bool {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let r = catch_unwind(AssertUnwindSafe(f));
    std::panic::set_hook(prev);
    r.is_err()
}

fn dict_value_type(dt: &DataType) -> &DataType {
    match dt {
        DataType::Dictionary(_, v) => v,
        other => panic!("expected a Dictionary data type for an avro map, got {other:?}"),
    }
}

fn list_element(dt: &DataType) -> &Field {
    match dt {
        DataType::List(f) => f,
        other => panic!("expected a List data type for an avro array, got {other:?}"),
    }
}

fn struct_field<'a>(dt: &'a DataType, name: &str) -> &'a Field {
    match dt {
        DataType::Struct(fs) => fs
            .iter()
            .find(|f| f.name() == name)
            .unwrap_or_else(|| panic!("struct has no field {name}")),
        other => panic!("expected a Struct data type for a nested avro record, got {other:?}"),
    }
}

// ===========================================================================
// A. bytes-backed, scale 0 — every band edge
// ===========================================================================

#[test]
fn p1_s0_routes_to_decimal128() {
    assert_band(1, 0, &convert_dec(1, 0), "p1_s0");
}

#[test]
fn p2_s0_routes_to_decimal128() {
    assert_band(2, 0, &convert_dec(2, 0), "p2_s0");
}

#[test]
fn p18_s0_routes_to_decimal128() {
    assert_band(18, 0, &convert_dec(18, 0), "p18_s0");
}

#[test]
fn p37_s0_routes_to_decimal128() {
    assert_band(37, 0, &convert_dec(37, 0), "p37_s0");
}

#[test]
fn p38_s0_is_the_last_decimal128() {
    let f = convert_dec(38, 0);
    assert_eq!(
        f.data_type(),
        &DataType::Decimal128(38, 0),
        "p=38 is the inclusive top of the Decimal128 band"
    );
}

#[test]
fn p39_s0_is_the_first_decimal256() {
    let f = convert_dec(39, 0);
    assert_eq!(
        f.data_type(),
        &DataType::Decimal256(39, 0),
        "p=39 must cross into Decimal256; an off-by-one here silently truncates to 128 bits"
    );
}

#[test]
fn p40_s0_routes_to_decimal256() {
    assert_band(40, 0, &convert_dec(40, 0), "p40_s0");
}

#[test]
fn p75_s0_routes_to_decimal256() {
    assert_band(75, 0, &convert_dec(75, 0), "p75_s0");
}

#[test]
fn p76_s0_is_the_last_decimal256() {
    let f = convert_dec(76, 0);
    assert_eq!(
        f.data_type(),
        &DataType::Decimal256(76, 0),
        "p=76 is the inclusive top of the Decimal256 band"
    );
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "p=76 must not be promoted to decimal_arb"
    );
}

#[test]
fn p77_s0_is_the_first_decimal_arb_and_is_hinted() {
    let f = convert_dec(77, 0);
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "p=77 must cross into decimal_arb; Decimal256 cannot hold 77 digits"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((77, 0))
    );
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        Some(NativeIntKind::U256),
        "decimal(77,0) is inside the ClickHouse native-int window and must be hinted u256"
    );
}

#[test]
fn p78_s0_is_the_last_hinted_precision() {
    let f = convert_dec(78, 0);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((78, 0))
    );
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        Some(NativeIntKind::U256),
        "p=78 is the inclusive top of the hinted 77..=78 window"
    );
}

#[test]
fn p79_s0_is_decimal_arb_without_hint() {
    let f = convert_dec(79, 0);
    assert!(DecimalArbType::is_decimal_arb_field(&f));
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((79, 0))
    );
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        None,
        "p=79 is past UInt256's 78-digit ceiling; a hint here would promise storage \
         that silently overflows"
    );
}

#[test]
fn p80_s0_is_decimal_arb_without_hint() {
    assert_band(80, 0, &convert_dec(80, 0), "p80_s0");
}

#[test]
fn p99_s0_is_decimal_arb_without_hint() {
    assert_band(99, 0, &convert_dec(99, 0), "p99_s0");
}

#[test]
fn p100_s0_is_decimal_arb_without_hint() {
    let f = convert_dec(100, 0);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((100, 0))
    );
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        None,
        "p=100 must not carry dead native_int_kind metadata"
    );
}

// ===========================================================================
// B. non-zero scale across the same edges
// ===========================================================================

#[test]
fn p38_s1_routes_to_decimal128() {
    assert_band(38, 1, &convert_dec(38, 1), "p38_s1");
}

#[test]
fn p38_s38_routes_to_decimal128_with_full_scale() {
    let f = convert_dec(38, 38);
    assert_eq!(
        f.data_type(),
        &DataType::Decimal128(38, 38),
        "scale == precision must survive the p<=38 band unchanged"
    );
}

#[test]
fn p39_s1_routes_to_decimal256() {
    assert_band(39, 1, &convert_dec(39, 1), "p39_s1");
}

#[test]
fn p39_s38_routes_to_decimal256() {
    assert_band(39, 38, &convert_dec(39, 38), "p39_s38");
}

#[test]
fn p76_s38_routes_to_decimal256() {
    let f = convert_dec(76, 38);
    assert_eq!(
        f.data_type(),
        &DataType::Decimal256(76, 38),
        "the Decimal256 band's top edge must keep its scale"
    );
}

#[test]
fn p77_s1_routes_to_decimal_arb_without_hint() {
    let f = convert_dec(77, 1);
    assert!(DecimalArbType::is_decimal_arb_field(&f));
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((77, 1))
    );
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        None,
        "a fractional decimal(77,1) is not integer-shaped and must not claim u256 origin"
    );
}

#[test]
fn p77_s38_routes_to_decimal_arb_without_hint() {
    assert_band(77, 38, &convert_dec(77, 38), "p77_s38");
}

#[test]
fn p78_s1_routes_to_decimal_arb_without_hint() {
    let f = convert_dec(78, 1);
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        None,
        "scale != 0 must suppress the hint even inside the 77..=78 precision window"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((78, 1))
    );
}

#[test]
fn p78_s38_routes_to_decimal_arb_without_hint() {
    assert_band(78, 38, &convert_dec(78, 38), "p78_s38");
}

#[test]
fn p79_s1_routes_to_decimal_arb_without_hint() {
    assert_band(79, 1, &convert_dec(79, 1), "p79_s1");
}

#[test]
fn p100_s1_routes_to_decimal_arb_without_hint() {
    assert_band(100, 1, &convert_dec(100, 1), "p100_s1");
}

#[test]
fn p100_s38_routes_to_decimal_arb_without_hint() {
    let f = convert_dec(100, 38);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((100, 38)),
        "wide fractional decimals must keep precision AND scale (FR-018)"
    );
}

#[test]
fn p100_s100_routes_to_decimal_arb() {
    let f = convert_dec(100, 100);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((100, 100)),
        "scale == precision at the top of the schema-precision range must survive"
    );
}

// ===========================================================================
// C. the native_int_kind hint, exactly
// ===========================================================================

#[test]
fn hint_absent_at_p76_s0() {
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&convert_dec(76, 0)),
        None,
        "p=76 is Decimal256, not decimal_arb; it can never carry the hint"
    );
}

#[test]
fn hint_value_is_exactly_the_u256_string() {
    let f = convert_dec(77, 0);
    assert_eq!(
        f.metadata()
            .get(DecimalArbType::NATIVE_INT_KIND_KEY)
            .map(String::as_str),
        Some("u256"),
        "the raw hint value must be the canonical lowercase `u256` token"
    );
}

#[test]
fn hint_key_is_the_documented_metadata_key() {
    assert_eq!(
        DecimalArbType::NATIVE_INT_KIND_KEY,
        "streamling.native_int_kind",
        "the hint key is a cross-crate contract with the ClickHouse sink"
    );
    assert!(
        convert_dec(78, 0)
            .metadata()
            .contains_key("streamling.native_int_kind")
    );
}

#[test]
fn hint_is_never_i256_from_the_avro_source_path() {
    for p in [77usize, 78] {
        let f = convert_dec(p, 0);
        assert_ne!(
            DecimalArbType::native_int_kind_from_field(&f),
            Some(NativeIntKind::I256),
            "avro carries no signedness convention; the source path must never infer i256 (p={p})"
        );
    }
}

#[test]
fn decimal256_field_carries_no_native_int_kind_key() {
    for (p, s) in [(39usize, 0usize), (50, 3), (76, 0)] {
        let f = convert_dec(p, s);
        assert!(
            !f.metadata()
                .contains_key(DecimalArbType::NATIVE_INT_KIND_KEY),
            "Decimal256 fields must not carry a native_int_kind hint (p={p}, s={s})"
        );
    }
}

#[test]
fn decimal128_field_carries_no_native_int_kind_key() {
    for (p, s) in [(1usize, 0usize), (18, 6), (38, 0)] {
        let f = convert_dec(p, s);
        assert!(
            !f.metadata()
                .contains_key(DecimalArbType::NATIVE_INT_KIND_KEY),
            "Decimal128 fields must not carry a native_int_kind hint (p={p}, s={s})"
        );
    }
}

#[test]
fn hint_window_is_exactly_77_to_78_over_the_whole_precision_range() {
    for p in 1usize..=100 {
        let f = convert_dec(p, 0);
        let got = DecimalArbType::native_int_kind_from_field(&f);
        let want = if (77..=78).contains(&p) {
            Some(NativeIntKind::U256)
        } else {
            None
        };
        assert_eq!(
            got, want,
            "native_int_kind window drifted at precision {p} (scale 0)"
        );
    }
}

#[test]
fn hint_is_suppressed_for_every_non_zero_scale_at_p77_and_p78() {
    for p in [77usize, 78] {
        for s in 1usize..=38 {
            let f = convert_dec(p, s);
            assert_eq!(
                DecimalArbType::native_int_kind_from_field(&f),
                None,
                "decimal({p},{s}) is fractional and must not be hinted as a native integer"
            );
        }
    }
}

// ===========================================================================
// D. sweeps
// ===========================================================================

#[test]
fn sweep_scale_0_precision_1_to_100() {
    for p in 1usize..=100 {
        assert_band(p, 0, &convert_dec(p, 0), &format!("sweep s=0 p={p}"));
    }
}

#[test]
fn sweep_scale_1_precision_1_to_100() {
    for p in 1usize..=100 {
        assert_band(p, 1, &convert_dec(p, 1), &format!("sweep s=1 p={p}"));
    }
}

#[test]
fn sweep_scale_2_precision_2_to_100() {
    for p in 2usize..=100 {
        assert_band(p, 2, &convert_dec(p, 2), &format!("sweep s=2 p={p}"));
    }
}

#[test]
fn sweep_scale_18_precision_18_to_100() {
    for p in 18usize..=100 {
        assert_band(p, 18, &convert_dec(p, 18), &format!("sweep s=18 p={p}"));
    }
}

#[test]
fn sweep_scale_38_precision_38_to_100() {
    for p in 38usize..=100 {
        assert_band(p, 38, &convert_dec(p, 38), &format!("sweep s=38 p={p}"));
    }
}

#[test]
fn sweep_full_matrix_precision_1_to_100_scale_0_to_38() {
    for p in 1usize..=100 {
        for s in 0usize..=38.min(p) {
            assert_band(p, s, &convert_dec(p, s), &format!("matrix p={p} s={s}"));
        }
    }
}

#[test]
fn sweep_confirms_exactly_three_band_transitions_at_scale_0() {
    // Walking p upward at scale 0, the arrow type must change exactly at
    // 38→39 (128→256), 76→77 (256→arb+hint) and 78→79 (hint drop).
    let mut transitions = Vec::new();
    let mut prev = describe(&convert_dec(1, 0));
    for p in 2usize..=100 {
        let cur = describe(&convert_dec(p, 0));
        if cur != prev {
            transitions.push(p);
        }
        prev = cur;
    }
    assert_eq!(
        transitions,
        vec![39, 77, 79],
        "scale-0 band transitions must occur only at p=39, p=77 and p=79"
    );
}

#[test]
fn sweep_confirms_exactly_two_band_transitions_at_scale_1() {
    let mut transitions = Vec::new();
    let mut prev = describe(&convert_dec(1, 1));
    for p in 2usize..=100 {
        let cur = describe(&convert_dec(p, 1));
        if cur != prev {
            transitions.push(p);
        }
        prev = cur;
    }
    assert_eq!(
        transitions,
        vec![39, 77],
        "at scale 1 the only band transitions are 38→39 and 76→77 (no hint window)"
    );
}

/// Band-shape summary of a converted field, precision/scale erased so that a
/// transition means "the routing decision changed", not "p changed".
fn describe(f: &Field) -> String {
    let kind = match f.data_type() {
        DataType::Decimal128(_, s) => format!("d128/s{s}"),
        DataType::Decimal256(_, s) => format!("d256/s{s}"),
        DataType::LargeBinary if DecimalArbType::is_decimal_arb_field(f) => {
            let s = DecimalArbType::precision_scale_from_field(f).unwrap().1;
            format!("arb/s{s}")
        }
        other => format!("{other:?}"),
    };
    match DecimalArbType::native_int_kind_from_field(f) {
        Some(k) => format!("{kind}+{}", k.as_str()),
        None => kind,
    }
}

#[test]
fn sweep_arrow_fixed_width_bands_report_the_declared_precision_and_scale() {
    for p in 1usize..=76 {
        for s in [0usize, 1, p.min(7)] {
            let f = convert_dec(p, s);
            match f.data_type() {
                DataType::Decimal128(ap, asc) => {
                    assert_eq!(
                        (*ap as usize, *asc as usize),
                        (p, s),
                        "Decimal128 must echo the declared avro precision/scale"
                    );
                }
                DataType::Decimal256(ap, asc) => {
                    assert_eq!(
                        (*ap as usize, *asc as usize),
                        (p, s),
                        "Decimal256 must echo the declared avro precision/scale"
                    );
                }
                other => panic!("p={p} s={s} should be a fixed-width decimal, got {other:?}"),
            }
        }
    }
}

#[test]
fn sweep_every_wide_precision_is_decimal_arb_not_utf8() {
    for p in 77usize..=100 {
        for s in [0usize, 1, 38] {
            let f = convert_dec(p, s);
            assert_ne!(
                f.data_type(),
                &DataType::Utf8,
                "decimal({p},{s}) must not fall back to the lossy Utf8 mapping (FR-018)"
            );
            assert!(
                DecimalArbType::is_decimal_arb_field(&f),
                "decimal({p},{s}) must be decimal_arb"
            );
        }
    }
}

// ===========================================================================
// E. nullable unions
// ===========================================================================

#[test]
fn nullable_p10_is_decimal128_and_nullable() {
    let f = convert_one(&nullable(&bytes_dec(10, 2)));
    assert_eq!(f.data_type(), &DataType::Decimal128(10, 2));
    assert!(
        f.is_nullable(),
        "[\"null\", decimal] must yield a nullable arrow field"
    );
}

#[test]
fn nullable_p38_is_decimal128_and_nullable() {
    let f = convert_one(&nullable(&bytes_dec(38, 0)));
    assert_eq!(f.data_type(), &DataType::Decimal128(38, 0));
    assert!(f.is_nullable());
}

#[test]
fn nullable_p39_is_decimal256_and_nullable() {
    let f = convert_one(&nullable(&bytes_dec(39, 0)));
    assert_eq!(f.data_type(), &DataType::Decimal256(39, 0));
    assert!(f.is_nullable());
}

#[test]
fn nullable_p76_is_decimal256_and_nullable() {
    let f = convert_one(&nullable(&bytes_dec(76, 18)));
    assert_eq!(f.data_type(), &DataType::Decimal256(76, 18));
    assert!(f.is_nullable());
}

#[test]
fn nullable_p77_s0_keeps_the_u256_hint() {
    let f = convert_one(&nullable(&bytes_dec(77, 0)));
    assert!(
        f.is_nullable(),
        "nullability must survive the decimal_arb rebuild"
    );
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        Some(NativeIntKind::U256),
        "wrapping decimal(77,0) in a nullable union must not drop the native_int_kind hint"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((77, 0))
    );
}

#[test]
fn nullable_p78_s0_keeps_the_u256_hint() {
    let f = convert_one(&nullable(&bytes_dec(78, 0)));
    assert!(f.is_nullable());
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        Some(NativeIntKind::U256)
    );
}

#[test]
fn nullable_p79_s0_has_no_hint() {
    let f = convert_one(&nullable(&bytes_dec(79, 0)));
    assert!(f.is_nullable());
    assert_eq!(DecimalArbType::native_int_kind_from_field(&f), None);
}

#[test]
fn nullable_p100_s5_is_nullable_decimal_arb() {
    let f = convert_one(&nullable(&bytes_dec(100, 5)));
    assert!(f.is_nullable());
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((100, 5))
    );
}

#[test]
fn union_with_null_last_still_resolves_to_the_decimal() {
    // ["decimal", "null"] is legal avro; the decimal must still win.
    let f = convert_one(&format!(r#"[{},"null"]"#, bytes_dec(100, 3)));
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "union variant order must not change the routed type"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((100, 3))
    );
    assert!(
        f.is_nullable(),
        "a two-variant union containing null is nullable regardless of order"
    );
}

#[test]
fn bare_decimal_field_is_not_nullable() {
    for (p, s) in [(10usize, 2usize), (50, 2), (100, 2), (77, 0)] {
        let f = convert_dec(p, s);
        assert!(
            !f.is_nullable(),
            "a bare (non-union) avro decimal({p},{s}) field must be non-nullable"
        );
    }
}

#[test]
fn nullability_is_preserved_across_the_whole_precision_sweep() {
    for p in 1usize..=100 {
        let f = convert_one(&nullable(&bytes_dec(p, 0)));
        assert!(
            f.is_nullable(),
            "nullable union at precision {p} lost its nullability during decimal routing"
        );
    }
}

// ===========================================================================
// F. fixed-backed decimals must route identically to bytes-backed
// ===========================================================================

#[test]
fn fixed_backed_p1_matches_bytes_backed() {
    assert_band(1, 0, &convert_one(&fixed_dec("fx1", 1, 0)), "fixed p1");
}

#[test]
fn fixed_backed_p38_is_decimal128() {
    assert_band(38, 0, &convert_one(&fixed_dec("fx38", 38, 0)), "fixed p38");
}

#[test]
fn fixed_backed_p39_is_decimal256() {
    assert_band(39, 0, &convert_one(&fixed_dec("fx39", 39, 0)), "fixed p39");
}

#[test]
fn fixed_backed_p76_is_decimal256() {
    assert_band(76, 0, &convert_one(&fixed_dec("fx76", 76, 0)), "fixed p76");
}

#[test]
fn fixed_backed_p77_s0_is_hinted_decimal_arb() {
    let f = convert_one(&fixed_dec("fx77", 77, 0));
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        Some(NativeIntKind::U256),
        "fixed-backed decimal(77,0) must be hinted exactly like the bytes-backed form"
    );
}

#[test]
fn fixed_backed_p78_s0_is_hinted_decimal_arb() {
    let f = convert_one(&fixed_dec("fx78", 78, 0));
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        Some(NativeIntKind::U256)
    );
}

#[test]
fn fixed_backed_p79_s0_has_no_hint() {
    let f = convert_one(&fixed_dec("fx79", 79, 0));
    assert!(DecimalArbType::is_decimal_arb_field(&f));
    assert_eq!(DecimalArbType::native_int_kind_from_field(&f), None);
}

#[test]
fn fixed_backed_p100_s18_is_decimal_arb() {
    let f = convert_one(&fixed_dec("fx100", 100, 18));
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((100, 18))
    );
}

#[test]
fn fixed_backed_never_leaks_fixed_size_binary() {
    for p in [1usize, 38, 39, 76, 77, 78, 79, 100] {
        let f = convert_one(&fixed_dec(&format!("fxl{p}"), p, 0));
        assert!(
            !matches!(f.data_type(), DataType::FixedSizeBinary(_)),
            "fixed-backed decimal(p={p}) must be routed as a decimal, not raw FixedSizeBinary"
        );
    }
}

#[test]
fn fixed_backed_nullable_p77_keeps_hint_and_nullability() {
    let f = convert_one(&nullable(&fixed_dec("fxn77", 77, 0)));
    assert!(f.is_nullable());
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&f),
        Some(NativeIntKind::U256)
    );
}

#[test]
fn fixed_and_bytes_backing_agree_across_the_band_edges() {
    for (p, s) in [
        (38usize, 0usize),
        (38, 5),
        (39, 0),
        (76, 18),
        (77, 0),
        (78, 0),
        (79, 0),
        (100, 7),
    ] {
        let a = convert_dec(p, s);
        let b = convert_one(&fixed_dec(&format!("fxc{p}_{s}"), p, s));
        assert_eq!(
            a.data_type(),
            b.data_type(),
            "backing type (bytes vs fixed) must not change routing for decimal({p},{s})"
        );
        assert_eq!(
            DecimalArbType::native_int_kind_from_field(&a),
            DecimalArbType::native_int_kind_from_field(&b),
            "backing type must not change the native_int_kind hint for decimal({p},{s})"
        );
    }
}

#[test]
fn fixed_size_larger_than_needed_does_not_change_routing() {
    let json = r#"{"type":"fixed","name":"big","size":64,"logicalType":"decimal","precision":20,"scale":4}"#;
    let f = convert_one(json);
    assert_eq!(
        f.data_type(),
        &DataType::Decimal128(20, 4),
        "an over-sized fixed backing must not affect precision-based routing"
    );
}

// ===========================================================================
// G. nested decimals — records, arrays, maps
// ===========================================================================

#[test]
fn nested_struct_decimal_p10_is_decimal128() {
    let f = convert_one(&struct_of("r1", "d", &bytes_dec(10, 2)));
    assert_eq!(
        struct_field(f.data_type(), "d").data_type(),
        &DataType::Decimal128(10, 2)
    );
}

#[test]
fn nested_struct_decimal_p50_is_decimal256() {
    let f = convert_one(&struct_of("r2", "d", &bytes_dec(50, 8)));
    assert_eq!(
        struct_field(f.data_type(), "d").data_type(),
        &DataType::Decimal256(50, 8),
        "nested decimals above 38 digits must be Decimal256, not a malformed Decimal128"
    );
}

#[test]
fn nested_struct_decimal_p39_crosses_to_decimal256() {
    let f = convert_one(&struct_of("r3", "d", &bytes_dec(39, 0)));
    assert_eq!(
        struct_field(f.data_type(), "d").data_type(),
        &DataType::Decimal256(39, 0),
        "the nested band edge must match the top-level one at 38/39"
    );
}

#[test]
fn nested_struct_decimal_p38_stays_decimal128() {
    let f = convert_one(&struct_of("r4", "d", &bytes_dec(38, 0)));
    assert_eq!(
        struct_field(f.data_type(), "d").data_type(),
        &DataType::Decimal128(38, 0)
    );
}

#[test]
fn nested_struct_decimal_p76_stays_decimal256() {
    let f = convert_one(&struct_of("r5", "d", &bytes_dec(76, 0)));
    assert_eq!(
        struct_field(f.data_type(), "d").data_type(),
        &DataType::Decimal256(76, 0)
    );
}

#[test]
fn nested_struct_decimal_p77_becomes_decimal_arb_with_metadata() {
    let f = convert_one(&struct_of("r6", "d", &bytes_dec(77, 5)));
    let inner = struct_field(f.data_type(), "d");
    assert!(
        DecimalArbType::is_decimal_arb_field(inner),
        "a nested decimal(77,5) must be decimal_arb, not a truncating Decimal128"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(inner),
        Some((77, 5))
    );
}

#[test]
fn nested_struct_decimal_p100_becomes_decimal_arb_with_metadata() {
    let f = convert_one(&struct_of("r7", "d", &bytes_dec(100, 18)));
    let inner = struct_field(f.data_type(), "d");
    assert_eq!(
        DecimalArbType::precision_scale_from_field(inner),
        Some((100, 18))
    );
    assert_eq!(inner.data_type(), &DataType::LargeBinary);
}

#[test]
fn nested_struct_nullable_union_decimal_is_seen_through() {
    let f = convert_one(&struct_of("r8", "d", &nullable(&bytes_dec(100, 2))));
    let inner = struct_field(f.data_type(), "d");
    assert!(
        DecimalArbType::is_decimal_arb_field(inner),
        "find_decimal_schema must see through a nullable union inside a nested record"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(inner),
        Some((100, 2))
    );
}

#[test]
fn nested_struct_wide_integer_decimal_carries_no_native_int_kind_hint() {
    // Characterisation: the u256 hint is applied only by the top-level fixup.
    let f = convert_one(&struct_of("r9", "d", &bytes_dec(77, 0)));
    let inner = struct_field(f.data_type(), "d");
    assert!(DecimalArbType::is_decimal_arb_field(inner));
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(inner),
        None,
        "nested decimal(77,0) is documented to route without the ClickHouse hint"
    );
}

#[test]
fn array_of_decimal_p10_keeps_decimal128_element() {
    let f = convert_one(&array_of(&bytes_dec(10, 2)));
    assert_eq!(
        list_element(f.data_type()).data_type(),
        &DataType::Decimal128(10, 2)
    );
}

#[test]
fn array_of_decimal_p50_keeps_decimal256_element() {
    let f = convert_one(&array_of(&bytes_dec(50, 4)));
    assert_eq!(
        list_element(f.data_type()).data_type(),
        &DataType::Decimal256(50, 4)
    );
}

#[test]
fn array_of_wide_decimal_keeps_precision_scale_on_the_element_field() {
    let f = convert_one(&array_of(&bytes_dec(100, 6)));
    let elem = list_element(f.data_type());
    assert!(
        DecimalArbType::is_decimal_arb_field(elem),
        "array<decimal(100,6)> element must be decimal_arb"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(elem),
        Some((100, 6)),
        "the list element Field must retain the decimal_arb precision/scale"
    );
}

#[test]
fn array_of_nullable_wide_decimal_keeps_precision_scale() {
    let f = convert_one(&array_of(&nullable(&bytes_dec(90, 3))));
    let elem = list_element(f.data_type());
    assert_eq!(
        DecimalArbType::precision_scale_from_field(elem),
        Some((90, 3))
    );
}

#[test]
fn array_of_struct_of_wide_decimal_keeps_precision_scale() {
    let f = convert_one(&array_of(&struct_of("r10", "d", &bytes_dec(80, 9))));
    let elem = list_element(f.data_type());
    let inner = struct_field(elem.data_type(), "d");
    assert_eq!(
        DecimalArbType::precision_scale_from_field(inner),
        Some((80, 9)),
        "two levels of nesting must not erase decimal_arb parameters"
    );
}

#[test]
fn array_band_edges_match_the_top_level_bands() {
    for (p, s) in [(38usize, 0usize), (39, 0), (76, 0), (77, 0), (100, 0)] {
        let f = convert_one(&array_of(&bytes_dec(p, s)));
        let elem = list_element(f.data_type());
        let want_arb = p > 76;
        assert_eq!(
            DecimalArbType::is_decimal_arb_field(elem),
            want_arb,
            "array element band for decimal({p},{s}) disagrees with the top-level band"
        );
    }
}

#[test]
fn map_of_decimal_p10_keeps_decimal128_value_type() {
    let f = convert_one(&map_of(&bytes_dec(10, 2)));
    assert_eq!(
        dict_value_type(f.data_type()),
        &DataType::Decimal128(10, 2),
        "map<decimal(10,2)> must keep the value precision/scale in the dictionary value type"
    );
}

#[test]
fn map_of_decimal_p50_keeps_decimal256_value_type() {
    let f = convert_one(&map_of(&bytes_dec(50, 4)));
    assert_eq!(
        dict_value_type(f.data_type()),
        &DataType::Decimal256(50, 4),
        "map<decimal(50,4)> must keep the value precision/scale"
    );
}

#[test]
fn map_of_decimal_uses_a_utf8_key_type() {
    let f = convert_one(&map_of(&bytes_dec(10, 2)));
    match f.data_type() {
        DataType::Dictionary(k, _) => assert_eq!(
            k.as_ref(),
            &DataType::Utf8,
            "avro map keys are always strings"
        ),
        other => panic!("expected Dictionary for avro map, got {other:?}"),
    }
}

#[test]
#[ignore = "FINDING: map<decimal(p>76)> loses precision/scale — Dictionary carries only the value DataType (LargeBinary), and the decimal_arb extension metadata lives on the discarded value Field"]
fn map_of_wide_decimal_preserves_precision_and_scale() {
    let f = convert_one(&map_of(&bytes_dec(100, 6)));
    let vt = dict_value_type(f.data_type());
    assert!(
        vt != &DataType::LargeBinary || DecimalArbType::is_decimal_arb_metadata(f.metadata()),
        "map<decimal(100,6)> was reduced to Dictionary(Utf8, LargeBinary) with no decimal_arb \
         metadata anywhere on the field — precision/scale are unrecoverable, so the values \
         decode as opaque bytes. Narrow maps (p<=76) keep p/s in the value DataType; only the \
         decimal_arb band loses it. field={f:?}"
    );
}

#[test]
#[ignore = "FINDING: map<decimal(p>76)> loses precision/scale (see map_of_wide_decimal_preserves_precision_and_scale)"]
fn map_of_wide_decimal_at_p77_preserves_precision_and_scale() {
    let f = convert_one(&map_of(&bytes_dec(77, 0)));
    let vt = dict_value_type(f.data_type());
    assert!(
        vt != &DataType::LargeBinary || DecimalArbType::is_decimal_arb_metadata(f.metadata()),
        "map<decimal(77,0)> lost its precision/scale: {f:?}"
    );
}

#[test]
fn map_of_wide_decimal_is_at_least_not_silently_narrowed_to_decimal128() {
    let f = convert_one(&map_of(&bytes_dec(100, 6)));
    let vt = dict_value_type(f.data_type());
    assert!(
        !matches!(vt, DataType::Decimal128(_, _)),
        "map<decimal(100,6)> must never be narrowed to a 128-bit decimal (silent truncation)"
    );
}

#[test]
fn map_band_edge_at_38_39_is_correct() {
    assert_eq!(
        dict_value_type(convert_one(&map_of(&bytes_dec(38, 0))).data_type()),
        &DataType::Decimal128(38, 0)
    );
    assert_eq!(
        dict_value_type(convert_one(&map_of(&bytes_dec(39, 0))).data_type()),
        &DataType::Decimal256(39, 0),
        "map value band must cross at 38/39 like every other path"
    );
}

#[test]
fn map_band_edge_at_76_77_is_correct() {
    assert_eq!(
        dict_value_type(convert_one(&map_of(&bytes_dec(76, 0))).data_type()),
        &DataType::Decimal256(76, 0)
    );
    assert_eq!(
        dict_value_type(convert_one(&map_of(&bytes_dec(77, 0))).data_type()),
        &DataType::LargeBinary,
        "map value at p=77 must move off the fixed-width decimals"
    );
}

#[test]
fn top_level_array_field_is_not_rewritten_by_the_decimal_fixup() {
    // `find_decimal_schema` must not reach through an array, or the whole
    // column would be replaced by a scalar decimal.
    let f = convert_one(&array_of(&bytes_dec(100, 2)));
    assert!(
        matches!(f.data_type(), DataType::List(_)),
        "a top-level array<decimal> column must stay a List, got {:?}",
        f.data_type()
    );
}

#[test]
fn top_level_map_field_is_not_rewritten_by_the_decimal_fixup() {
    let f = convert_one(&map_of(&bytes_dec(100, 2)));
    assert!(
        matches!(f.data_type(), DataType::Dictionary(_, _)),
        "a top-level map<decimal> column must stay a Dictionary, got {:?}",
        f.data_type()
    );
}

#[test]
fn top_level_struct_field_is_not_rewritten_by_the_decimal_fixup() {
    let f = convert_one(&struct_of("r11", "d", &bytes_dec(100, 2)));
    assert!(
        matches!(f.data_type(), DataType::Struct(_)),
        "a top-level record<decimal> column must stay a Struct, got {:?}",
        f.data_type()
    );
}

#[test]
fn nested_struct_with_mixed_decimal_bands_routes_each_field_independently() {
    let json = format!(
        r#"{{"type":"record","name":"mixed","fields":[
            {{"name":"a","type":{}}},
            {{"name":"b","type":{}}},
            {{"name":"c","type":{}}},
            {{"name":"d","type":{}}}
        ]}}"#,
        bytes_dec(10, 2),
        bytes_dec(50, 2),
        bytes_dec(77, 0),
        bytes_dec(100, 4)
    );
    let f = convert_one(&json);
    let dt = f.data_type();
    assert_eq!(
        struct_field(dt, "a").data_type(),
        &DataType::Decimal128(10, 2)
    );
    assert_eq!(
        struct_field(dt, "b").data_type(),
        &DataType::Decimal256(50, 2)
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(struct_field(dt, "c")),
        Some((77, 0))
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(struct_field(dt, "d")),
        Some((100, 4))
    );
}

#[test]
fn nested_wide_decimal_is_never_left_as_a_malformed_decimal128() {
    for p in 77usize..=100 {
        let f = convert_one(&struct_of(&format!("rr{p}"), "d", &bytes_dec(p, 0)));
        let inner = struct_field(f.data_type(), "d");
        assert!(
            !matches!(inner.data_type(), DataType::Decimal128(_, _)),
            "nested decimal({p},0) must not be left as Decimal128 (silently truncates to 128 bits)"
        );
    }
}

#[test]
fn nested_decimal_band_sweep_matches_the_documented_table() {
    for p in 1usize..=100 {
        let f = convert_one(&struct_of(&format!("rs{p}"), "d", &bytes_dec(p, 0)));
        let inner = struct_field(f.data_type(), "d");
        let got = match inner.data_type() {
            DataType::Decimal128(_, _) => "d128",
            DataType::Decimal256(_, _) => "d256",
            DataType::LargeBinary if DecimalArbType::is_decimal_arb_field(inner) => "arb",
            other => panic!("nested decimal({p},0) produced unexpected {other:?}"),
        };
        let want = if p <= 38 {
            "d128"
        } else if p <= 76 {
            "d256"
        } else {
            "arb"
        };
        assert_eq!(got, want, "nested band wrong at precision {p}");
    }
}

// ===========================================================================
// H. the Utf8 fallback (reached when decimal_arb rejects the parameters)
// ===========================================================================

#[test]
fn utf8_fallback_is_reached_when_scale_exceeds_precision_in_the_arb_band() {
    // Built manually: apache-avro's parser silently downgrades an invalid
    // decimal logical type, so this shape can only be produced in-process
    // (e.g. by the scale-clamping in post_process_avro_schema_for_reading).
    let f = convert_avro_schema_to_arrow(manual_decimal_record("f", 90, 95))
        .field(0)
        .clone();
    assert_eq!(
        f.data_type(),
        &DataType::Utf8,
        "decimal_arb rejects scale > precision, so the wide band must fall back to Utf8"
    );
}

#[test]
fn utf8_fallback_carries_the_avro_decimal_scale_metadata() {
    let f = convert_avro_schema_to_arrow(manual_decimal_record("f", 90, 95))
        .field(0)
        .clone();
    assert_eq!(
        f.metadata()
            .get(AVRO_DECIMAL_SCALE_META)
            .map(String::as_str),
        Some("95"),
        "the Utf8 fallback must carry the avro scale so coerce_array can render the raw \
         decimal bytes as a scale-aware decimal string instead of reinterpreting them as text"
    );
}

#[test]
fn utf8_fallback_scale_metadata_key_is_the_documented_constant() {
    assert_eq!(
        AVRO_DECIMAL_SCALE_META, "avro.decimal.scale",
        "the fallback metadata key is a contract with the arrow-avro decode path"
    );
}

#[test]
fn utf8_fallback_preserves_the_field_name() {
    let f = convert_avro_schema_to_arrow(manual_decimal_record("weird_amount", 90, 95))
        .field(0)
        .clone();
    assert_eq!(f.name(), "weird_amount");
}

#[test]
fn utf8_fallback_is_not_stamped_as_decimal_arb() {
    let f = convert_avro_schema_to_arrow(manual_decimal_record("f", 90, 95))
        .field(0)
        .clone();
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "a Utf8 fallback field must not advertise the decimal_arb extension"
    );
}

#[test]
fn utf8_fallback_scale_metadata_survives_a_larger_scale() {
    let f = convert_avro_schema_to_arrow(manual_decimal_record("f", 80, 100))
        .field(0)
        .clone();
    assert_eq!(f.data_type(), &DataType::Utf8);
    assert_eq!(
        f.metadata()
            .get(AVRO_DECIMAL_SCALE_META)
            .map(String::as_str),
        Some("100"),
        "the fallback must report the (clamped) avro scale verbatim"
    );
}

#[test]
fn avro_parser_downgrades_scale_greater_than_precision_instead_of_producing_a_decimal() {
    // Guards the assumption behind the manual-construction tests above: a
    // wire schema can never carry scale > precision.
    let schema = AvroSchema::parse_str(
        r#"{"type":"record","name":"t","fields":[
            {"name":"f","type":{"type":"bytes","logicalType":"decimal","precision":2,"scale":5}}]}"#,
    )
    .expect("parse must succeed (the logical type is dropped, not rejected)");
    let f = convert_avro_schema_to_arrow(schema).field(0).clone();
    assert_eq!(
        f.data_type(),
        &DataType::Binary,
        "an invalid decimal logical type degrades to its bytes backing, so no decimal \
         routing happens at all"
    );
}

#[test]
fn avro_parser_downgrades_zero_precision_instead_of_producing_a_decimal() {
    let schema = AvroSchema::parse_str(
        r#"{"type":"record","name":"t","fields":[
            {"name":"f","type":{"type":"bytes","logicalType":"decimal","precision":0,"scale":0}}]}"#,
    )
    .expect("parse must succeed");
    let f = convert_avro_schema_to_arrow(schema).field(0).clone();
    assert_eq!(
        f.data_type(),
        &DataType::Binary,
        "precision 0 is invalid; the decimal logical type must be dropped, not routed as \
         Decimal128(0, 0)"
    );
}

#[test]
#[ignore = "FINDING: post_process scale-clamping can emit DataType::Decimal256(p, s) with s > p, an Arrow-invalid decimal type that panics on array construction"]
fn clamped_scale_never_produces_a_decimal256_with_scale_above_precision() {
    // precision 50 / scale 150 -> scale clamped to MAX_SCHEMA_PRECISION (100),
    // then routed by precision into the Decimal256 band as Decimal256(50, 100).
    let f = convert_avro_schema_to_arrow(manual_decimal_record("f", 50, 150))
        .field(0)
        .clone();
    match f.data_type() {
        DataType::Decimal256(p, s) => assert!(
            (*s as i32) <= (*p as i32),
            "produced Arrow-invalid Decimal256({p},{s}): scale exceeds precision"
        ),
        DataType::Decimal128(p, s) => assert!(
            (*s as i32) <= (*p as i32),
            "produced Arrow-invalid Decimal128({p},{s}): scale exceeds precision"
        ),
        _ => {}
    }
}

// ===========================================================================
// I. MAX_SCHEMA_PRECISION guard, robustness, and structural invariants
// ===========================================================================

#[test]
fn precision_101_panics_at_the_top_level_guard() {
    assert!(
        panics(|| {
            let _ = convert_dec(101, 0);
        }),
        "MAX_SCHEMA_PRECISION is 100; precision 101 must be rejected at the top level"
    );
}

#[test]
fn precision_120_panics_at_the_top_level_guard() {
    assert!(
        panics(|| {
            let _ = convert_dec(120, 5);
        }),
        "precision 120 exceeds MAX_SCHEMA_PRECISION and must not be silently accepted"
    );
}

#[test]
fn every_precision_101_to_120_is_rejected_at_the_top_level() {
    for p in 101usize..=120 {
        assert!(
            panics(|| {
                let _ = convert_dec(p, 0);
            }),
            "precision {p} > MAX_SCHEMA_PRECISION must not silently produce a field"
        );
    }
}

#[test]
fn every_precision_1_to_100_is_accepted_at_the_top_level() {
    for p in 1usize..=100 {
        assert!(
            !panics(|| {
                let _ = convert_dec(p, 0);
            }),
            "precision {p} is within MAX_SCHEMA_PRECISION and must convert without panicking"
        );
    }
}

#[test]
#[ignore = "FINDING: the MAX_SCHEMA_PRECISION=100 guard is top-level only — nested decimals with p in 101..=65535 convert happily to decimal_arb, so the guard is both inconsistent and unnecessary"]
fn precision_guard_applies_consistently_to_nested_decimals() {
    let nested_ok = !panics(|| {
        let _ = convert_one(&struct_of("rg", "d", &bytes_dec(120, 0)));
    });
    let top_ok = !panics(|| {
        let _ = convert_dec(120, 0);
    });
    assert_eq!(
        nested_ok, top_ok,
        "decimal(120,0) is rejected (panic) at the top level but accepted when nested \
         inside a record — the same wire value succeeds or crashes depending only on \
         nesting depth"
    );
}

#[test]
fn nested_precision_above_the_guard_still_produces_decimal_arb() {
    // Characterisation of the asymmetry above: nesting bypasses the guard.
    let f = convert_one(&struct_of("rg2", "d", &bytes_dec(120, 0)));
    let inner = struct_field(f.data_type(), "d");
    assert_eq!(
        DecimalArbType::precision_scale_from_field(inner),
        Some((120, 0)),
        "nested wide decimals are routed to decimal_arb with no MAX_SCHEMA_PRECISION check"
    );
}

#[test]
#[ignore = "FINDING: convert_avro_schema_to_arrow unwraps to_arrow_schema(), so a named-type reference (Schema::Ref) — legal Avro, produced whenever a record type is reused — panics instead of surfacing the graceful error the converter deliberately builds"]
fn named_type_reference_does_not_panic() {
    let json = r#"{
        "type":"record","name":"outer","fields":[
            {"name":"a","type":{"type":"record","name":"inner","fields":[
                {"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":2}}]}},
            {"name":"b","type":"inner"}
        ]}"#;
    let schema = AvroSchema::parse_str(json).expect("named-type reuse is valid avro");
    assert!(
        !panics(|| {
            let _ = convert_avro_schema_to_arrow(schema.clone());
        }),
        "reusing a record type by name panics the schema conversion (unwrap on the \
         ParsePrimitive error that schema_to_field_with_props deliberately returns)"
    );
}

#[test]
#[ignore = "FINDING: post_process_avro_schema_for_reading unwraps the record lookup, so a top-level union with no record variant panics instead of erroring"]
fn top_level_union_without_a_record_variant_does_not_panic() {
    let schema = AvroSchema::parse_str(r#"["null","string"]"#).unwrap();
    assert!(
        !panics(|| {
            let _ = convert_avro_schema_to_arrow(schema.clone());
        }),
        "a top-level union carrying no record variant panics on `record_schema.unwrap()`"
    );
}

#[test]
#[ignore = "FINDING: a multi-variant union containing a decimal is collapsed to the bare decimal type, silently discarding the other branches (unions without a decimal correctly become DataType::Union)"]
fn multi_variant_union_with_a_decimal_is_not_collapsed() {
    let f = convert_one(&format!(r#"["null","string",{}]"#, bytes_dec(10, 2)));
    assert!(
        matches!(f.data_type(), DataType::Union(_, _)),
        "[\"null\",\"string\",decimal(10,2)] became {:?}; the string branch is now \
         unrepresentable, while [\"null\",\"string\",\"int\"] correctly becomes a Union",
        f.data_type()
    );
}

#[test]
fn multi_variant_union_without_a_decimal_becomes_a_union() {
    // Control for the test above: the non-decimal path keeps the union.
    let f = convert_one(r#"["null","string","int"]"#);
    assert!(
        matches!(f.data_type(), DataType::Union(_, _)),
        "a 3-variant union of plain types must map to DataType::Union, got {:?}",
        f.data_type()
    );
}

#[test]
fn top_level_union_wrapping_a_record_still_routes_decimals() {
    let json = format!(
        r#"["null",{{"type":"record","name":"env","fields":[{{"name":"amt","type":{}}}]}}]"#,
        bytes_dec(77, 0)
    );
    let schema = AvroSchema::parse_str(&json).unwrap();
    let arrow = convert_avro_schema_to_arrow(schema);
    assert_eq!(arrow.fields().len(), 1);
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(arrow.field(0)),
        Some(NativeIntKind::U256),
        "the Kafka envelope union form must route decimals identically to a bare record"
    );
}

#[test]
fn field_names_are_preserved_across_every_band() {
    for (p, s) in [(10usize, 2usize), (50, 2), (77, 0), (79, 0), (100, 4)] {
        let schema = record_of(&[("my_amount_col", bytes_dec(p, s))]);
        let arrow = convert_avro_schema_to_arrow(schema);
        assert_eq!(
            arrow.field(0).name(),
            "my_amount_col",
            "field name lost while routing decimal({p},{s})"
        );
    }
}

#[test]
fn non_decimal_neighbours_are_untouched() {
    let schema = record_of(&[
        ("id", "\"long\"".to_string()),
        ("amt", bytes_dec(100, 0)),
        ("name", "\"string\"".to_string()),
        ("flag", "\"boolean\"".to_string()),
    ]);
    let arrow = convert_avro_schema_to_arrow(schema);
    assert_eq!(arrow.field(0).data_type(), &DataType::Int64);
    assert!(DecimalArbType::is_decimal_arb_field(arrow.field(1)));
    assert_eq!(arrow.field(2).data_type(), &DataType::Utf8);
    assert_eq!(arrow.field(3).data_type(), &DataType::Boolean);
}

#[test]
fn decimal_fixup_stays_index_aligned_with_many_interleaved_fields() {
    let mut fields: Vec<(String, String)> = Vec::new();
    for i in 0..30usize {
        fields.push((format!("pad{i}"), "\"int\"".to_string()));
        let p = 1 + (i * 3) % 100;
        fields.push((format!("dec{i}"), bytes_dec(p, 0)));
    }
    let refs: Vec<(&str, String)> = fields
        .iter()
        .map(|(n, t)| (n.as_str(), t.clone()))
        .collect();
    let arrow = convert_avro_schema_to_arrow(record_of(&refs));
    for i in 0..30usize {
        let pad = arrow.field(i * 2);
        assert_eq!(
            pad.data_type(),
            &DataType::Int32,
            "index alignment slipped: pad{i} was rewritten by the decimal fixup"
        );
        let p = 1 + (i * 3) % 100;
        assert_band(p, 0, arrow.field(i * 2 + 1), &format!("interleaved dec{i}"));
    }
}

#[test]
fn every_band_yields_exactly_one_arrow_field_per_avro_field() {
    let schema = record_of(&[
        ("a", bytes_dec(38, 0)),
        ("b", bytes_dec(39, 0)),
        ("c", bytes_dec(76, 0)),
        ("d", bytes_dec(77, 0)),
        ("e", bytes_dec(78, 0)),
        ("f", bytes_dec(79, 0)),
        ("g", bytes_dec(100, 9)),
    ]);
    let arrow = convert_avro_schema_to_arrow(schema);
    assert_eq!(
        arrow.fields().len(),
        7,
        "decimal routing must be 1:1 with the avro record fields"
    );
}

#[test]
fn all_seven_band_representatives_route_correctly_in_one_schema() {
    let schema = record_of(&[
        ("a", bytes_dec(38, 0)),
        ("b", bytes_dec(39, 0)),
        ("c", bytes_dec(76, 0)),
        ("d", bytes_dec(77, 0)),
        ("e", bytes_dec(78, 0)),
        ("f", bytes_dec(79, 0)),
        ("g", bytes_dec(100, 9)),
    ]);
    let arrow = convert_avro_schema_to_arrow(schema);
    assert_band(38, 0, arrow.field(0), "multi a");
    assert_band(39, 0, arrow.field(1), "multi b");
    assert_band(76, 0, arrow.field(2), "multi c");
    assert_band(77, 0, arrow.field(3), "multi d");
    assert_band(78, 0, arrow.field(4), "multi e");
    assert_band(79, 0, arrow.field(5), "multi f");
    assert_band(100, 9, arrow.field(6), "multi g");
}

#[test]
fn fixed_width_decimal_fields_carry_no_extension_metadata_at_all() {
    for (p, s) in [(1usize, 0usize), (38, 10), (39, 0), (76, 18)] {
        let f = convert_dec(p, s);
        assert!(
            !f.metadata()
                .contains_key(DecimalArbType::EXTENSION_NAME_KEY),
            "decimal({p},{s}) is a fixed-width arrow decimal and must not carry \
             ARROW:extension:name"
        );
        assert!(
            !f.metadata()
                .contains_key(DecimalArbType::EXTENSION_METADATA_KEY),
            "decimal({p},{s}) must not carry ARROW:extension:metadata"
        );
    }
}

#[test]
fn decimal_arb_fields_carry_the_canonical_extension_name() {
    for (p, s) in [(77usize, 0usize), (79, 0), (100, 18)] {
        let f = convert_dec(p, s);
        assert_eq!(
            f.metadata()
                .get(DecimalArbType::EXTENSION_NAME_KEY)
                .map(String::as_str),
            Some("streamling.decimal_arb"),
            "decimal({p},{s}) must advertise the canonical extension name"
        );
    }
}

#[test]
fn decimal_arb_extension_metadata_is_the_canonical_json_shape() {
    let f = convert_dec(100, 18);
    assert_eq!(
        f.metadata()
            .get(DecimalArbType::EXTENSION_METADATA_KEY)
            .map(String::as_str),
        Some(r#"{"precision":100,"scale":18}"#),
        "the extension metadata payload is a cross-crate contract"
    );
}

#[test]
fn decimal_arb_storage_type_is_large_binary_across_the_whole_wide_band() {
    for p in 77usize..=100 {
        let f = convert_dec(p, 0);
        assert_eq!(
            f.data_type(),
            &DataType::LargeBinary,
            "decimal_arb storage must be LargeBinary at precision {p} (BinaryView would be \
             auto-expanded at output)"
        );
    }
}

#[test]
fn no_decimal_in_the_supported_range_ever_routes_to_utf8() {
    for p in 1usize..=100 {
        for s in [0usize, 1, p.min(38)] {
            let f = convert_dec(p, s);
            assert_ne!(
                f.data_type(),
                &DataType::Utf8,
                "decimal({p},{s}) must never take the lossy Utf8 fallback"
            );
        }
    }
}

#[test]
fn conversion_is_deterministic_for_the_same_schema() {
    for (p, s) in [(38usize, 0usize), (77, 0), (78, 0), (100, 5)] {
        let a = convert_dec(p, s);
        let b = convert_dec(p, s);
        assert_eq!(
            a, b,
            "converting the same avro decimal({p},{s}) twice produced different fields"
        );
    }
}

#[test]
fn post_process_preserves_precision_and_scale_within_limits() {
    let processed = post_process_avro_schema_for_reading(manual_decimal_record("f", 77, 3));
    match processed {
        AvroSchema::Record(r) => match &r.fields[0].schema {
            AvroSchema::Decimal(d) => {
                assert_eq!(
                    (d.precision, d.scale),
                    (77, 3),
                    "in-range (p,s) must pass through"
                )
            }
            other => panic!("expected a Decimal schema, got {other:?}"),
        },
        other => panic!("expected a Record schema, got {other:?}"),
    }
}

#[test]
fn post_process_clamps_scale_to_max_schema_precision() {
    let processed = post_process_avro_schema_for_reading(manual_decimal_record("f", 90, 250));
    match processed {
        AvroSchema::Record(r) => match &r.fields[0].schema {
            AvroSchema::Decimal(d) => {
                assert_eq!(
                    d.scale, 100,
                    "scale must clamp to MAX_SCHEMA_PRECISION (100)"
                );
                assert_eq!(
                    d.precision, 90,
                    "precision must not be altered by scale clamping"
                );
            }
            other => panic!("expected a Decimal schema, got {other:?}"),
        },
        other => panic!("expected a Record schema, got {other:?}"),
    }
}

#[test]
fn post_process_leaves_non_decimal_fields_alone() {
    let schema = record_of(&[
        ("s", "\"string\"".to_string()),
        ("i", "\"int\"".to_string()),
    ]);
    let processed = post_process_avro_schema_for_reading(schema);
    match processed {
        AvroSchema::Record(r) => {
            assert!(matches!(r.fields[0].schema, AvroSchema::String));
            assert!(matches!(r.fields[1].schema, AvroSchema::Int));
        }
        other => panic!("expected a Record schema, got {other:?}"),
    }
}

#[test]
fn post_process_does_not_reach_into_nested_records() {
    // Characterisation: the precision guard/clamp only walks top-level fields.
    let schema = record_of(&[("s", struct_of("inner_pp", "d", &bytes_dec(100, 4)))]);
    let processed = post_process_avro_schema_for_reading(schema);
    match processed {
        AvroSchema::Record(r) => match &r.fields[0].schema {
            AvroSchema::Record(inner) => match &inner.fields[0].schema {
                AvroSchema::Decimal(d) => assert_eq!((d.precision, d.scale), (100, 4)),
                other => panic!("expected nested Decimal, got {other:?}"),
            },
            other => panic!("expected nested Record, got {other:?}"),
        },
        other => panic!("expected a Record schema, got {other:?}"),
    }
}

#[test]
fn record_with_no_fields_converts_to_an_empty_schema() {
    let schema = AvroSchema::parse_str(r#"{"type":"record","name":"empty","fields":[]}"#).unwrap();
    let arrow = convert_avro_schema_to_arrow(schema);
    assert_eq!(arrow.fields().len(), 0);
}

#[test]
fn enum_and_string_fields_are_not_confused_with_decimals() {
    let schema = record_of(&[
        (
            "e",
            r#"{"type":"enum","name":"col","symbols":["A","B"]}"#.to_string(),
        ),
        ("s", "\"string\"".to_string()),
        ("amt", bytes_dec(100, 2)),
    ]);
    let arrow = convert_avro_schema_to_arrow(schema);
    assert_eq!(arrow.field(0).data_type(), &DataType::Utf8);
    assert!(!DecimalArbType::is_decimal_arb_field(arrow.field(0)));
    assert_eq!(arrow.field(1).data_type(), &DataType::Utf8);
    assert!(!DecimalArbType::is_decimal_arb_field(arrow.field(1)));
    assert!(DecimalArbType::is_decimal_arb_field(arrow.field(2)));
}

#[test]
fn plain_bytes_field_is_binary_not_a_decimal() {
    let f = convert_one("\"bytes\"");
    assert_eq!(
        f.data_type(),
        &DataType::Binary,
        "a bytes field without a decimal logical type must stay Binary"
    );
}

#[test]
fn big_decimal_logical_type_is_not_routed_by_the_decimal_bands() {
    let f = convert_one(r#"{"type":"bytes","logicalType":"big-decimal"}"#);
    assert_eq!(
        f.data_type(),
        &DataType::LargeBinary,
        "avro `big-decimal` maps to LargeBinary and must not be mistaken for decimal_arb"
    );
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "a bare LargeBinary big-decimal must NOT satisfy is_decimal_arb_field (no metadata)"
    );
}

#[test]
fn uuid_fixed_is_not_routed_as_a_decimal() {
    let f = convert_one(r#"{"type":"fixed","name":"u","size":16,"logicalType":"uuid"}"#);
    assert_eq!(f.data_type(), &DataType::FixedSizeBinary(16));
}

#[test]
fn decimal_with_doc_on_the_record_field_still_routes_by_precision() {
    let json = format!(
        r#"{{"type":"record","name":"withdoc","fields":[
            {{"name":"amt","doc":"money","type":{}}}]}}"#,
        bytes_dec(100, 2)
    );
    let schema = AvroSchema::parse_str(&json).unwrap();
    let arrow = convert_avro_schema_to_arrow(schema);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(arrow.field(0)),
        Some((100, 2)),
        "a field-level doc must not disturb decimal routing"
    );
}

#[test]
fn decimal_with_a_default_value_still_routes_by_precision() {
    let json = format!(
        r#"{{"type":"record","name":"withdefault","fields":[
            {{"name":"amt","type":{},"default":"\u0000"}}]}}"#,
        nullable(&bytes_dec(77, 0))
    );
    let schema = match AvroSchema::parse_str(&json) {
        Ok(s) => s,
        Err(_) => AvroSchema::parse_str(&format!(
            r#"{{"type":"record","name":"withdefault2","fields":[
                {{"name":"amt","type":{},"default":null}}]}}"#,
            nullable(&bytes_dec(77, 0))
        ))
        .expect("nullable field with a null default is valid avro"),
    };
    let arrow = convert_avro_schema_to_arrow(schema);
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(arrow.field(0)),
        Some(NativeIntKind::U256),
        "a field default must not disturb decimal routing or the hint"
    );
}

#[test]
fn hinted_field_precision_scale_reads_back_through_the_public_accessor() {
    for p in [77usize, 78] {
        let f = convert_dec(p, 0);
        let (gp, gs) = DecimalArbType::precision_scale_from_field(&f)
            .expect("a hinted decimal_arb field must expose (precision, scale)");
        assert_eq!(
            (gp, gs),
            (p as u32, 0),
            "the native_int_kind hint must not corrupt the extension metadata"
        );
    }
}

#[test]
fn hint_does_not_leak_onto_a_neighbouring_wide_decimal() {
    let schema = record_of(&[
        ("hinted", bytes_dec(78, 0)),
        ("plain", bytes_dec(79, 0)),
        ("frac", bytes_dec(78, 2)),
    ]);
    let arrow = convert_avro_schema_to_arrow(schema);
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(arrow.field(0)),
        Some(NativeIntKind::U256)
    );
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(arrow.field(1)),
        None,
        "the hint must not bleed from field 0 onto its neighbour"
    );
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(arrow.field(2)),
        None,
        "the hint must not bleed onto a fractional decimal(78,2)"
    );
}

#[test]
fn decimal_arb_never_appears_below_precision_77() {
    for p in 1usize..=76 {
        for s in [0usize, 1] {
            if s > p {
                continue;
            }
            let f = convert_dec(p, s);
            assert!(
                !DecimalArbType::is_decimal_arb_field(&f),
                "decimal({p},{s}) fits a fixed-width arrow decimal and must not be promoted \
                 to decimal_arb (that would cost a full byte-level re-encode)"
            );
        }
    }
}

#[test]
fn fixed_width_decimal_never_appears_above_precision_76() {
    for p in 77usize..=100 {
        for s in [0usize, 1, 38] {
            let f = convert_dec(p, s);
            assert!(
                !matches!(
                    f.data_type(),
                    DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
                ),
                "decimal({p},{s}) cannot fit a fixed-width arrow decimal; routing it there \
                 truncates silently"
            );
        }
    }
}
