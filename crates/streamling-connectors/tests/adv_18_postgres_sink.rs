//! Adversarial coverage for the **PostgreSQL sink write path**, agent 18.
//!
//! Focus (per assignment): the decimal-point placement surface that produced
//! finding F3 (`Decimal128`/`Decimal256`/`decimal_arb` → Postgres `NUMERIC`),
//! plus the `cast_map` / query-builder shape and the generated upsert SQL for
//! `decimal_arb` columns.
//!
//! ## What is reachable and what is not
//!
//! `unscaled_to_numeric_string` and `bind_arrow_value_to_query` live in
//! `postgres::value_binding`, which is a **private** module
//! (`mod value_binding;` in `postgres/mod.rs`), so an integration test cannot
//! call them directly. What *is* reachable — and is the actual production
//! decimal-point-placement path for `decimal_arb` — is
//! `PostgresSinkTableProvider::insert_into`, which installs
//! `build_projection_for_postgres` (a `DecimalArbToStringFunc` projection)
//! ahead of the sink. Running that projection in-process gives the exact
//! `NUMERIC` literal the sink binds, with no database involved.
//!
//! So the value tests below drive the real sink provider and assert on the
//! projected strings; the SQL tests drive `PostgresQueryBuilder` directly.
//! Everything is deterministic, in-process, and finishes in milliseconds.

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::i256;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use streamling_config::PostgresSinkConfig;
use streamling_connectors::table_providers::postgres::PostgresSinkTableProvider;
use streamling_connectors::table_providers::postgres::query_builder::{
    ALLOWED_UPDATE_WHERE_OPS, PostgresQueryBuilder, validate_update_where,
};
use streamling_core::data::COLUMN_NAME_OP;
use streamling_core::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, NativeIntKind,
};
use streamling_core::utils::pg::{
    arrow_field_to_postgres_type, get_postgres_type_info, override_schema_for_postgres_insert,
    postgres_type_to_arrow_field,
};

// ===========================================================================
// helpers
// ===========================================================================

fn cfg() -> PostgresSinkConfig {
    PostgresSinkConfig {
        host: "localhost".into(),
        port: "5432".into(),
        user: "u".into(),
        pass: "p".into(),
        db: "d".into(),
        sslmode: "disable".into(),
        batch_flush_interval: "1000".into(),
        batch_size: 100,
        statement_timeout_secs: 60,
        pool_acquire_timeout_secs: 30,
        pool_idle_timeout_secs: 600,
        pool_max_lifetime_secs: 1800,
    }
}

fn arb_field(name: &str, p: u32, s: u32, nullable: bool) -> Field {
    DecimalArbType::field(name, p, s, nullable)
        .unwrap_or_else(|e| panic!("DecimalArbType::field({name},{p},{s}) must succeed: {e}"))
}

fn arb_column(name: &str, p: u32, s: u32, values: &[Option<&str>]) -> ArrayRef {
    let mut b = DecimalArbArrayBuilder::with_capacity(values.len().max(1), name, p, s)
        .unwrap_or_else(|e| panic!("builder({name},{p},{s}) must succeed: {e}"));
    for v in values {
        match v {
            Some(x) => {
                let val = DecimalArbValue::from_str(x)
                    .unwrap_or_else(|e| panic!("DecimalArbValue::from_str({x:?}): {e}"));
                b.append_value(&val).unwrap_or_else(|e| {
                    panic!("append_value({x:?}) into ({p},{s}) must succeed: {e}")
                });
            }
            None => b.append_null(),
        }
    }
    Arc::new(b.finish().into_inner().0)
}

/// Build the sink provider for `schema` and return the plan node that feeds it
/// (i.e. the output of `build_projection_for_postgres`) together with the
/// projected rows for column `col_idx`, decoded as strings.
async fn sink_input_plan(
    schema: SchemaRef,
    batch: RecordBatch,
    primary_key: Option<&str>,
    append_only: bool,
) -> (SchemaRef, Vec<RecordBatch>) {
    let ctx = SessionContext::new();
    let state = ctx.state();
    let mem = MemTable::try_new(schema.clone(), vec![vec![batch]]).expect("MemTable::try_new");
    let input = mem.scan(&state, None, &[], None).await.expect("scan");
    let provider = PostgresSinkTableProvider::new(
        "metric".into(),
        schema.clone(),
        cfg(),
        "tbl".into(),
        "public".into(),
        None,
        None,
        "src".into(),
        primary_key.map(|s| s.to_string()),
        "update".into(),
        None,
        append_only,
        false,
        "ref".into(),
        None,
        None,
    );
    let plan = provider
        .insert_into(&state, input, InsertOp::Append)
        .await
        .expect("insert_into must build a plan without touching the database");
    let projected = plan.children()[0].clone();
    let out_schema = projected.schema();
    let batches = collect(projected, ctx.task_ctx())
        .await
        .expect("projection must execute");
    (out_schema, batches)
}

fn strings_of(batches: &[RecordBatch], col: usize) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| {
                panic!(
                    "projected column {col} must be Utf8, got {:?}",
                    b.column(col).data_type()
                )
            });
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i).to_string())
            });
        }
    }
    out
}

/// Full write-path projection of a single `decimal_arb` column.
async fn project_arb(p: u32, s: u32, values: &[Option<&str>]) -> Vec<Option<String>> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", p, s, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![arb_column("amt", p, s, values)])
        .expect("RecordBatch::try_new");
    let (_, batches) = sink_input_plan(schema, batch, None, false).await;
    strings_of(&batches, 0)
}

async fn project_one(p: u32, s: u32, value: &str) -> String {
    project_arb(p, s, &[Some(value)])
        .await
        .remove(0)
        .expect("a non-null decimal_arb input must project to a non-null NUMERIC literal")
}

/// Returns `Some(reason)` when `s` is not a literal Postgres would accept for
/// `NUMERIC(precision, scale)`. This is the exact invariant F3 violated: the
/// magnitude was inflated by 10^scale so wide-enough columns overflowed.
fn numeric_fit_error(s: &str, precision: u32, scale: u32) -> Option<String> {
    if s.is_empty() {
        return Some("empty literal".into());
    }
    if s.contains('e') || s.contains('E') {
        return Some(format!("exponent notation is not a NUMERIC literal: {s:?}"));
    }
    let body = match s.strip_prefix('-') {
        Some(rest) => rest,
        None => s,
    };
    if body.starts_with('-') || body.starts_with('+') {
        return Some(format!("stray sign after leading sign: {s:?}"));
    }
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    if body.matches('.').count() > 1 {
        return Some(format!("more than one decimal point: {s:?}"));
    }
    if int_part.is_empty() {
        return Some(format!(
            "missing integer part (expected leading '0'): {s:?}"
        ));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit()) {
        return Some(format!("non-digit in integer part: {s:?}"));
    }
    if s.contains('.') && frac_part.is_empty() {
        return Some(format!("trailing decimal point: {s:?}"));
    }
    if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
        return Some(format!("non-digit in fractional part: {s:?}"));
    }
    if frac_part.len() as u32 > scale {
        return Some(format!(
            "{} fractional digits exceed declared scale {scale}: {s:?}",
            frac_part.len()
        ));
    }
    let int_digits = int_part.trim_start_matches('0').len() as u32;
    if int_digits > precision - scale {
        return Some(format!(
            "{int_digits} integer digits exceed precision-scale = {}: {s:?}",
            precision - scale
        ));
    }
    None
}

fn assert_fits(s: &str, precision: u32, scale: u32, ctx: &str) {
    if let Some(why) = numeric_fit_error(s, precision, scale) {
        panic!(
            "[{ctx}] projected literal does not fit NUMERIC({precision},{scale}): {why} \
             — a Postgres sink would either reject the row or store a wrong magnitude"
        );
    }
}

/// `10^-scale` written as a plain decimal literal.
fn ulp(scale: u32) -> String {
    if scale == 0 {
        "1".to_string()
    } else {
        format!("0.{}1", "0".repeat(scale as usize - 1))
    }
}

/// `1` rendered at `scale` fractional digits.
fn one_at_scale(scale: u32) -> String {
    if scale == 0 {
        "1".to_string()
    } else {
        format!("1.{}", "0".repeat(scale as usize))
    }
}

fn names(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

fn cast_of(map: &HashMap<String, Option<String>>, col: &str) -> Option<String> {
    map.get(col)
        .unwrap_or_else(|| panic!("cast_map must contain an entry for column {col:?}"))
        .clone()
}

fn norm(sql: &str) -> String {
    sql.to_lowercase().replace(' ', "")
}

// ===========================================================================
// A. decimal_arb value projection — decimal-point placement (F3 surface)
// ===========================================================================

#[tokio::test]
async fn scale0_positive_integer_projects_without_a_decimal_point() {
    assert_eq!(
        project_one(20, 0, "12345").await,
        "12345",
        "scale 0 must emit the integer verbatim with no point"
    );
}

#[tokio::test]
async fn scale0_negative_integer_keeps_the_sign_and_has_no_point() {
    assert_eq!(
        project_one(20, 0, "-12345").await,
        "-12345",
        "scale 0 negative must be '-12345'"
    );
}

#[tokio::test]
async fn scale0_zero_projects_as_a_bare_zero() {
    assert_eq!(
        project_one(20, 0, "0").await,
        "0",
        "zero at scale 0 must be '0'"
    );
}

#[tokio::test]
async fn scale2_value_splits_point_two_digits_from_the_right() {
    assert_eq!(
        project_one(10, 2, "123.45").await,
        "123.45",
        "point must sit `scale` digits from the right, not be appended"
    );
}

#[tokio::test]
async fn scale2_negative_value_splits_point_two_digits_from_the_right() {
    assert_eq!(
        project_one(10, 2, "-123.45").await,
        "-123.45",
        "sign must precede the integer digits, point still 2 from the right"
    );
}

#[tokio::test]
async fn scale2_sub_one_magnitude_gets_a_leading_zero_before_the_point() {
    assert_eq!(
        project_one(10, 2, "0.45").await,
        "0.45",
        "a magnitude < 1 must render as '0.45', never '.45'"
    );
}

#[tokio::test]
async fn scale2_negative_sub_one_puts_the_sign_before_the_leading_zero() {
    assert_eq!(
        project_one(10, 2, "-0.45").await,
        "-0.45",
        "sign must precede the padded leading zero, i.e. '-0.45' not '0.-45'"
    );
}

#[tokio::test]
async fn scale3_one_thousandth_positive_pads_two_leading_fraction_zeros() {
    assert_eq!(
        project_one(10, 3, "0.001").await,
        "0.001",
        "unscaled '1' at scale 3 must render as '0.001'"
    );
}

#[tokio::test]
async fn scale3_one_thousandth_negative_pads_two_leading_fraction_zeros() {
    assert_eq!(
        project_one(10, 3, "-0.001").await,
        "-0.001",
        "unscaled '-1' at scale 3 must render as '-0.001'"
    );
}

#[tokio::test]
async fn scale18_smallest_positive_unit_renders_all_seventeen_padding_zeros() {
    assert_eq!(
        project_one(38, 18, "0.000000000000000001").await,
        "0.000000000000000001",
        "1 wei at scale 18 must keep exactly 18 fractional digits"
    );
}

#[tokio::test]
async fn scale18_smallest_negative_unit_renders_sign_then_padding_zeros() {
    assert_eq!(
        project_one(38, 18, "-0.000000000000000001").await,
        "-0.000000000000000001",
        "-1 wei at scale 18 must be '-0.000000000000000001'"
    );
}

#[tokio::test]
async fn integer_input_is_padded_out_to_the_column_scale() {
    assert_eq!(
        project_one(10, 3, "5").await,
        "5.000",
        "an integer stored in a scale-3 column must render with 3 fractional digits"
    );
}

#[tokio::test]
async fn negative_integer_input_is_padded_out_to_the_column_scale() {
    assert_eq!(
        project_one(10, 3, "-5").await,
        "-5.000",
        "a negative integer in a scale-3 column must render as '-5.000'"
    );
}

#[tokio::test]
async fn zero_at_high_scale_projects_as_a_bare_zero() {
    assert_eq!(
        project_one(38, 18, "0").await,
        "0",
        "zero canonicalizes to scale 0, so it must project as '0'"
    );
}

#[tokio::test]
async fn negative_zero_projects_without_a_sign() {
    assert_eq!(
        project_one(38, 18, "-0").await,
        "0",
        "'-0' must never reach Postgres as '-0.000…' — it is numerically zero"
    );
}

#[tokio::test]
async fn null_decimal_arb_projects_as_sql_null() {
    assert_eq!(
        project_arb(38, 6, &[None]).await,
        vec![None],
        "a NULL decimal_arb must stay NULL, not become the string \"null\""
    );
}

#[tokio::test]
async fn nulls_and_values_keep_their_row_order_through_the_projection() {
    let got = project_arb(20, 4, &[Some("1.5"), None, Some("-2.25"), None, Some("0")]).await;
    assert_eq!(
        got,
        vec![
            Some("1.5000".to_string()),
            None,
            Some("-2.2500".to_string()),
            None,
            Some("0".to_string()),
        ],
        "row order and null placement must survive the sink projection"
    );
}

#[tokio::test]
async fn all_scales_0_to_38_render_one_ulp_positive_exactly() {
    for s in 0u32..=38 {
        let v = ulp(s);
        let got = project_one(76, s, &v).await;
        assert_eq!(
            got, v,
            "scale {s}: smallest representable unit must round-trip to {v:?}"
        );
    }
}

#[tokio::test]
async fn all_scales_0_to_38_render_one_ulp_negative_exactly() {
    for s in 0u32..=38 {
        let v = format!("-{}", ulp(s));
        let got = project_one(76, s, &v).await;
        assert_eq!(
            got, v,
            "scale {s}: negative smallest unit must round-trip to {v:?}"
        );
    }
}

#[tokio::test]
async fn all_scales_0_to_38_pad_integer_one_out_to_the_column_scale() {
    for s in 0u32..=38 {
        let expected = one_at_scale(s);
        let got = project_one(76, s, "1").await;
        assert_eq!(
            got, expected,
            "scale {s}: integer 1 must render with exactly {s} fractional digits"
        );
    }
}

#[tokio::test]
async fn all_scales_0_to_38_render_zero_as_a_bare_zero() {
    for s in 0u32..=38 {
        assert_eq!(
            project_one(76, s, "0").await,
            "0",
            "scale {s}: zero must project as '0'"
        );
    }
}

#[tokio::test]
async fn all_scales_0_to_38_produce_literals_that_fit_the_declared_numeric() {
    for s in 0u32..=38 {
        let p = 76;
        for v in [
            ulp(s),
            format!("-{}", ulp(s)),
            "1".to_string(),
            "-1".to_string(),
            "0".to_string(),
        ] {
            let got = project_one(p, s, &v).await;
            assert_fits(&got, p, s, &format!("scale {s}, input {v:?}"));
        }
    }
}

#[tokio::test]
async fn all_scales_0_to_38_never_emit_exponent_notation() {
    for s in 0u32..=38 {
        for v in [ulp(s), format!("-{}", ulp(s))] {
            let got = project_one(76, s, &v).await;
            assert!(
                !got.contains('e') && !got.contains('E'),
                "scale {s}: Postgres NUMERIC cannot parse exponent form, got {got:?}"
            );
        }
    }
}

#[tokio::test]
async fn all_scales_1_to_38_start_sub_one_magnitudes_with_zero_dot() {
    for s in 1u32..=38 {
        let got = project_one(76, s, &ulp(s)).await;
        assert!(
            got.starts_with("0."),
            "scale {s}: a magnitude below 1 must start with '0.', got {got:?}"
        );
    }
}

#[tokio::test]
async fn all_scales_1_to_38_put_the_minus_sign_before_the_padded_zero() {
    for s in 1u32..=38 {
        let got = project_one(76, s, &format!("-{}", ulp(s))).await;
        assert!(
            got.starts_with("-0."),
            "scale {s}: negative sub-one magnitude must start with '-0.', got {got:?}"
        );
        assert_eq!(
            got.matches('-').count(),
            1,
            "scale {s}: exactly one sign character expected, got {got:?}"
        );
    }
}

#[tokio::test]
async fn all_scales_0_to_38_round_trip_back_to_the_same_numeric_value() {
    for s in 0u32..=38 {
        for v in [ulp(s), format!("-{}", ulp(s)), "1".into(), "-1".into()] {
            let got = project_one(76, s, &v).await;
            let parsed = DecimalArbValue::from_str(&got)
                .unwrap_or_else(|e| panic!("scale {s}: projected {got:?} must reparse: {e}"));
            let orig = DecimalArbValue::from_str(&v).unwrap();
            assert_eq!(
                parsed, orig,
                "scale {s}: projected literal {got:?} must equal the source value {v:?}"
            );
        }
    }
}

#[tokio::test]
async fn all_scales_0_to_38_render_the_widest_value_the_column_admits() {
    // precision 40 with scale s leaves 40 - s integer digits; use all of them.
    for s in 0u32..=38 {
        let p = 40u32;
        let int_digits = (p - s) as usize;
        let mut v = "9".repeat(int_digits);
        if s > 0 {
            v.push('.');
            v.push_str(&"9".repeat(s as usize));
        }
        if int_digits == 0 {
            v = format!("0.{}", "9".repeat(s as usize));
        }
        let got = project_one(p, s, &v).await;
        assert_eq!(
            got, v,
            "scale {s}: the widest in-range value must round-trip unchanged"
        );
        assert_fits(&got, p, s, &format!("widest value at scale {s}"));
    }
}

#[tokio::test]
async fn all_fractional_column_scale_equals_precision_renders_leading_zero_dot() {
    // The F3 `Decimal128(10,10)` shape, expressed for decimal_arb.
    let v = format!("0.{}", "1234567890");
    let got = project_one(10, 10, &v).await;
    assert_eq!(
        got, "0.1234567890",
        "scale == precision means every digit is fractional; magnitude must stay < 1"
    );
    assert_fits(&got, 10, 10, "scale == precision");
}

#[tokio::test]
async fn all_fractional_column_scale_equals_precision_negative() {
    let got = project_one(10, 10, "-0.1234567890").await;
    assert_eq!(
        got, "-0.1234567890",
        "negative all-fractional value must keep sign in front of '0.'"
    );
}

#[tokio::test]
async fn high_scale_wide_column_splits_integer_and_fraction_at_the_right_place() {
    // The F3 `Decimal256(60,30)` shape: 30 integer digits + 30 fractional.
    let v = format!("{}.{}", "1".repeat(30), "1".repeat(30));
    let got = project_one(76, 30, &v).await;
    assert_eq!(
        got, v,
        "30 integer digits must not become 60 — that was the F3 magnitude inflation"
    );
    assert_fits(&got, 76, 30, "p=76 s=30");
}

#[tokio::test]
async fn decimal_arb_p100_s18_wide_value_keeps_every_digit() {
    let v = format!("{}.{}", "7".repeat(82), "3".repeat(18));
    let got = project_one(100, 18, &v).await;
    assert_eq!(
        got, v,
        "a 100-digit decimal_arb must not lose or gain digits"
    );
    assert_fits(&got, 100, 18, "p=100 s=18");
}

#[tokio::test]
async fn decimal_arb_p78_s0_u256_maximum_projects_all_78_digits() {
    let v = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    let got = project_one(78, 0, v).await;
    assert_eq!(
        got, v,
        "uint256 max in NUMERIC(78,0) must project verbatim with no point"
    );
    assert_fits(&got, 78, 0, "u256 max");
}

#[tokio::test]
async fn leading_fraction_zeros_are_preserved_not_stripped() {
    assert_eq!(
        project_one(20, 6, "0.000123").await,
        "0.000123",
        "leading zeros inside the fraction carry magnitude and must not be dropped"
    );
}

#[tokio::test]
async fn trailing_fraction_zeros_are_padded_out_to_the_column_scale() {
    assert_eq!(
        project_one(20, 6, "1.5").await,
        "1.500000",
        "a scale-6 column must emit 6 fractional digits for 1.5"
    );
}

#[tokio::test]
async fn value_with_fewer_digits_than_scale_still_yields_one_integer_zero() {
    for s in [1u32, 2, 5, 9, 17, 33, 38] {
        let got = project_one(76, s, &ulp(s)).await;
        let int_part = got.split('.').next().unwrap();
        assert_eq!(
            int_part, "0",
            "scale {s}: integer part must be exactly \"0\", got {got:?}"
        );
    }
}

#[tokio::test]
async fn boundary_just_below_one_stays_below_one() {
    let got = project_one(20, 4, "0.9999").await;
    assert_eq!(got, "0.9999", "0.9999 must not round or shift to 9999");
}

#[tokio::test]
async fn boundary_exactly_one_crosses_to_a_single_integer_digit() {
    let got = project_one(20, 4, "1.0000").await;
    assert_eq!(got, "1.0000", "1.0000 must have exactly one integer digit");
}

#[tokio::test]
async fn tenth_and_hundredth_boundaries_render_with_correct_padding() {
    assert_eq!(project_one(20, 3, "0.1").await, "0.100");
    assert_eq!(project_one(20, 3, "0.01").await, "0.010");
    assert_eq!(project_one(20, 3, "0.001").await, "0.001");
}

#[tokio::test]
async fn negative_tenth_and_hundredth_boundaries_render_with_correct_padding() {
    assert_eq!(project_one(20, 3, "-0.1").await, "-0.100");
    assert_eq!(project_one(20, 3, "-0.01").await, "-0.010");
    assert_eq!(project_one(20, 3, "-0.001").await, "-0.001");
}

#[tokio::test]
async fn numerically_equal_inputs_project_to_the_same_literal() {
    let a = project_arb(20, 3, &[Some("5"), Some("5.0"), Some("05"), Some("+5")]).await;
    assert_eq!(
        a,
        vec![
            Some("5.000".to_string()),
            Some("5.000".to_string()),
            Some("5.000".to_string()),
            Some("5.000".to_string()),
        ],
        "numerically equal decimal_arb inputs must bind identically at the sink"
    );
}

#[tokio::test]
async fn a_batch_of_mixed_magnitudes_all_fit_the_declared_numeric() {
    let p = 38;
    let s = 18;
    let inputs = [
        "0",
        "1",
        "-1",
        "0.000000000000000001",
        "-0.000000000000000001",
        "12345678901234567890.123456789012345678",
        "-12345678901234567890.123456789012345678",
        "0.5",
        "-0.5",
    ];
    let vals: Vec<Option<&str>> = inputs.iter().map(|s| Some(*s)).collect();
    let got = project_arb(p, s, &vals).await;
    for (i, g) in got.iter().enumerate() {
        let g = g.as_ref().expect("non-null input projects non-null");
        assert_fits(g, p, s, &format!("row {i} input {:?}", inputs[i]));
    }
}

#[tokio::test]
async fn projection_preserves_the_row_count() {
    let vals: Vec<Option<&str>> = (0..64)
        .map(|i| if i % 7 == 0 { None } else { Some("1.25") })
        .collect();
    let got = project_arb(20, 2, &vals).await;
    assert_eq!(
        got.len(),
        64,
        "the sink projection must be row-preserving (1 output row per input row)"
    );
}

#[tokio::test]
async fn projection_output_column_is_utf8() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 38, 6, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![arb_column("amt", 38, 6, &[Some("1.5")])],
    )
    .unwrap();
    let (out, _) = sink_input_plan(schema, batch, None, false).await;
    assert_eq!(
        out.field(0).data_type(),
        &DataType::Utf8,
        "decimal_arb must be projected to Utf8 for the string bind path"
    );
}

#[tokio::test]
async fn projection_output_column_keeps_the_source_column_name() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("total_value", 38, 6, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![arb_column("total_value", 38, 6, &[Some("1.5")])],
    )
    .unwrap();
    let (out, _) = sink_input_plan(schema, batch, None, false).await;
    assert_eq!(
        out.field(0).name(),
        "total_value",
        "the projected column must keep its name or the INSERT column list mismatches"
    );
}

#[tokio::test]
async fn projection_output_column_no_longer_advertises_decimal_arb_metadata() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 38, 6, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![arb_column("amt", 38, 6, &[Some("1.5")])],
    )
    .unwrap();
    let (out, _) = sink_input_plan(schema, batch, None, false).await;
    assert!(
        !DecimalArbType::is_decimal_arb_field(out.field(0)),
        "a Utf8 column must not claim to be decimal_arb; the cast_map reads the ORIGINAL schema"
    );
}

#[tokio::test]
async fn projection_preserves_a_nullable_column_as_nullable() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 38, 6, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![arb_column("amt", 38, 6, &[Some("1.5"), None])],
    )
    .unwrap();
    let (out, _) = sink_input_plan(schema, batch, None, false).await;
    assert!(
        out.field(0).is_nullable(),
        "nullability must survive the projection so NULL binds stay legal"
    );
}

#[tokio::test]
async fn projection_preserves_a_non_nullable_column_as_non_nullable() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 38, 6, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![arb_column("amt", 38, 6, &[Some("1.5")])],
    )
    .unwrap();
    let (out, _) = sink_input_plan(schema, batch, None, false).await;
    assert!(
        !out.field(0).is_nullable(),
        "a NOT NULL source column must stay NOT NULL in the sink schema"
    );
}

#[tokio::test]
async fn non_decimal_columns_pass_through_the_projection_untouched() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        arb_field("amt", 38, 6, true),
        Field::new("tag", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![7i64, 8])) as ArrayRef,
            arb_column("amt", 38, 6, &[Some("1.5"), Some("-2")]),
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ],
    )
    .unwrap();
    let (out, batches) = sink_input_plan(schema, batch, None, false).await;
    assert_eq!(
        out.field(0).data_type(),
        &DataType::Int64,
        "Int64 untouched"
    );
    assert_eq!(out.field(2).data_type(), &DataType::Utf8, "Utf8 untouched");
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id stays Int64");
    assert_eq!(
        (ids.value(0), ids.value(1)),
        (7, 8),
        "non-decimal values must be carried through unchanged"
    );
}

#[tokio::test]
async fn projection_preserves_column_order_in_a_mixed_schema() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        arb_field("amt", 38, 6, true),
        Field::new("tag", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64])) as ArrayRef,
            arb_column("amt", 38, 6, &[Some("1.5")]),
            Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
        ],
    )
    .unwrap();
    let (out, _) = sink_input_plan(schema, batch, None, false).await;
    let got: Vec<&str> = out.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        got,
        vec!["id", "amt", "tag"],
        "column order feeds positional placeholder binding; it must not be reordered"
    );
}

#[tokio::test]
async fn two_decimal_arb_columns_with_different_scales_are_each_scaled_correctly() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        arb_field("a", 38, 2, true),
        arb_field("b", 38, 8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            arb_column("a", 38, 2, &[Some("1")]),
            arb_column("b", 38, 8, &[Some("1")]),
        ],
    )
    .unwrap();
    let (_, batches) = sink_input_plan(schema, batch, None, false).await;
    assert_eq!(
        strings_of(&batches, 0),
        vec![Some("1.00".to_string())],
        "column 'a' must use its own scale 2"
    );
    assert_eq!(
        strings_of(&batches, 1),
        vec![Some("1.00000000".to_string())],
        "column 'b' must use its own scale 8, not column a's"
    );
}

#[tokio::test]
async fn a_schema_without_decimal_arb_or_nested_types_is_not_reprojected() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
        ],
    )
    .unwrap();
    let (out, _) = sink_input_plan(schema.clone(), batch, None, false).await;
    assert_eq!(
        out.fields().len(),
        2,
        "no projection should be inserted when nothing needs conversion"
    );
    assert_eq!(out.field(0).data_type(), &DataType::Int64);
}

#[tokio::test]
async fn decimal128_columns_are_left_native_for_the_binder_to_format() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "amt",
        DataType::Decimal128(10, 2),
        true,
    )]));
    let arr = Decimal128Array::from(vec![12345i128])
        .with_precision_and_scale(10, 2)
        .unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr) as ArrayRef]).unwrap();
    let (out, _) = sink_input_plan(schema, batch, None, false).await;
    assert_eq!(
        out.field(0).data_type(),
        &DataType::Decimal128(10, 2),
        "Decimal128 must reach the binder as Decimal128 so the unscaled value is placed correctly"
    );
}

#[tokio::test]
async fn decimal256_columns_are_left_native_for_the_binder_to_format() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "amt",
        DataType::Decimal256(60, 30),
        true,
    )]));
    let arr = Decimal256Array::from(vec![i256::from_i128(12345i128)])
        .with_precision_and_scale(60, 30)
        .unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr) as ArrayRef]).unwrap();
    let (out, _) = sink_input_plan(schema, batch, None, false).await;
    assert_eq!(
        out.field(0).data_type(),
        &DataType::Decimal256(60, 30),
        "Decimal256 must reach the binder natively"
    );
}

#[tokio::test]
async fn decimal_arb_column_named_with_a_dot_still_projects() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("a.b", 20, 2, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![arb_column("a.b", 20, 2, &[Some("1.25")])],
    )
    .unwrap();
    let (out, batches) = sink_input_plan(schema, batch, None, false).await;
    assert_eq!(
        out.field(0).name(),
        "a.b",
        "a dotted column name must not be split into relation.column by the projection"
    );
    assert_eq!(strings_of(&batches, 0), vec![Some("1.25".to_string())]);
}

#[tokio::test]
async fn projection_runs_the_same_way_when_a_primary_key_is_configured() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        arb_field("amt", 38, 4, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64])) as ArrayRef,
            arb_column("amt", 38, 4, &[Some("-0.0001")]),
        ],
    )
    .unwrap();
    let (_, batches) = sink_input_plan(schema, batch, Some("id"), false).await;
    assert_eq!(
        strings_of(&batches, 1),
        vec![Some("-0.0001".to_string())],
        "configuring a primary key must not change value formatting"
    );
}

#[tokio::test]
async fn projection_runs_the_same_way_in_append_only_mode() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        arb_field("amt", 38, 4, true),
        Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            arb_column("amt", 38, 4, &[Some("-0.0001")]),
            Arc::new(StringArray::from(vec!["i"])) as ArrayRef,
        ],
    )
    .unwrap();
    let (_, batches) = sink_input_plan(schema, batch, None, true).await;
    assert_eq!(
        strings_of(&batches, 0),
        vec![Some("-0.0001".to_string())],
        "append-only mode must not change decimal formatting"
    );
}

#[tokio::test]
async fn empty_batch_projects_to_zero_rows_without_error() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 20, 2, true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![arb_column("amt", 20, 2, &[])]).unwrap();
    let (_, batches) = sink_input_plan(schema, batch, None, false).await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 0,
        "an empty batch must project to zero rows, not panic"
    );
}

#[tokio::test]
async fn every_projected_literal_reparses_and_refits_its_column() {
    let cases: &[(u32, u32, &str)] = &[
        (10, 0, "999999999"),
        (10, 10, "0.0000000001"),
        (38, 38, "0.00000000000000000000000000000000000001"),
        (38, 19, "1234567890123456789.1234567890123456789"),
        (76, 0, "-1"),
        (76, 38, "-0.00000000000000000000000000000000000001"),
        (
            100,
            50,
            "0.00000000000000000000000000000000000000000000000001",
        ),
        (
            78,
            0,
            "-115792089237316195423570985008687907853269984665640564039457584007913129639935",
        ),
    ];
    for (p, s, v) in cases {
        let got = project_one(*p, *s, v).await;
        assert_fits(&got, *p, *s, &format!("({p},{s}) input {v:?}"));
        let back = DecimalArbValue::from_str(&got)
            .unwrap_or_else(|e| panic!("({p},{s}) projected {got:?} must reparse: {e}"));
        assert_eq!(
            back,
            DecimalArbValue::from_str(v).unwrap(),
            "({p},{s}) projected literal {got:?} must be numerically equal to {v:?}"
        );
    }
}

#[tokio::test]
async fn projected_literal_never_starts_with_a_bare_decimal_point() {
    for s in 1u32..=20 {
        for v in [ulp(s), format!("-{}", ulp(s))] {
            let got = project_one(40, s, &v).await;
            assert!(
                !got.starts_with('.') && !got.starts_with("-."),
                "scale {s}: '.5' is not a portable NUMERIC literal, got {got:?}"
            );
        }
    }
}

#[tokio::test]
async fn projected_literal_never_ends_with_a_bare_decimal_point() {
    for s in 0u32..=20 {
        let got = project_one(40, s, "1").await;
        assert!(
            !got.ends_with('.'),
            "scale {s}: trailing '.' is not a valid NUMERIC literal, got {got:?}"
        );
    }
}

#[tokio::test]
async fn projected_literal_contains_at_most_one_decimal_point() {
    for s in 0u32..=38 {
        let got = project_one(76, s, &ulp(s)).await;
        assert!(
            got.matches('.').count() <= 1,
            "scale {s}: more than one point in {got:?}"
        );
    }
}

// ===========================================================================
// B. cast_map / VALUES clause / upsert SQL for decimal columns
// ===========================================================================

#[test]
fn cast_map_gives_decimal_arb_a_numeric_cast_with_its_declared_precision_and_scale() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 100, 18, true)]));
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["amt"]));
    assert_eq!(
        cast_of(&map, "amt"),
        Some("numeric(100,18)".to_string()),
        "the bound decimal string must be cast back to the column's exact NUMERIC type"
    );
}

#[test]
fn cast_map_decimal_arb_scale_zero_is_numeric_p_0() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 78, 0, true)]));
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["amt"]));
    assert_eq!(cast_of(&map, "amt"), Some("numeric(78,0)".to_string()));
}

#[test]
fn cast_map_decimal_arb_scale_equal_to_precision_is_preserved() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 38, 38, true)]));
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["amt"]));
    assert_eq!(
        cast_of(&map, "amt"),
        Some("numeric(38,38)".to_string()),
        "an all-fractional column must not have its scale clamped"
    );
}

#[test]
fn cast_map_decimal_arb_at_max_precision_is_preserved() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 65535, 0, true)]));
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["amt"]));
    assert_eq!(cast_of(&map, "amt"), Some("numeric(65535,0)".to_string()));
}

#[test]
fn cast_map_cast_and_ddl_column_type_agree_for_decimal_arb() {
    for (p, s) in [(78u32, 0u32), (100, 18), (38, 38), (200, 100), (76, 30)] {
        let f = arb_field("amt", p, s, true);
        let ddl = arrow_field_to_postgres_type(&f);
        let cast = get_postgres_type_info(&f)
            .string_cast_sql
            .expect("decimal_arb must bind as string with a cast");
        assert_eq!(
            norm(&ddl),
            norm(&cast),
            "({p},{s}): the CREATE TABLE type and the INSERT cast must be the same NUMERIC type"
        );
    }
}

#[test]
fn cast_map_decimal128_all_fractional_shape_keeps_scale_equal_to_precision() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "amt",
        DataType::Decimal128(10, 10),
        true,
    )]));
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["amt"]));
    assert_eq!(
        cast_of(&map, "amt"),
        Some("numeric(10,10)".to_string()),
        "the F3 Decimal128(10,10) shape must still cast to numeric(10,10)"
    );
}

#[test]
fn cast_map_decimal256_high_scale_shape_is_exact() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "amt",
        DataType::Decimal256(60, 30),
        true,
    )]));
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["amt"]));
    assert_eq!(
        cast_of(&map, "amt"),
        Some("numeric(60,30)".to_string()),
        "the F3 Decimal256(60,30) shape must cast to numeric(60,30)"
    );
}

#[test]
fn cast_map_plain_large_binary_gets_no_numeric_cast() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "blob",
        DataType::LargeBinary,
        true,
    )]));
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["blob"]));
    assert_eq!(
        cast_of(&map, "blob"),
        None,
        "LargeBinary without decimal_arb metadata is BYTEA and must not get a numeric cast"
    );
}

#[test]
fn cast_map_uint64_still_maps_to_numeric_20_0() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "slot",
        DataType::UInt64,
        true,
    )]));
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["slot"]));
    assert_eq!(cast_of(&map, "slot"), Some("numeric(20,0)".to_string()));
}

#[test]
fn cast_map_omits_the_gs_op_column() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        arb_field("amt", 100, 18, true),
        Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
    ]));
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["amt", COLUMN_NAME_OP]));
    assert!(
        !map.contains_key(COLUMN_NAME_OP),
        "_gs_op is never inserted as a data column and must stay out of the cast map"
    );
}

#[test]
fn cast_map_only_contains_requested_columns() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        arb_field("a", 100, 18, true),
        arb_field("b", 100, 18, true),
    ]));
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["a"]));
    assert!(map.contains_key("a"), "requested column must be present");
    assert!(
        !map.contains_key("b"),
        "a column not in the INSERT list must not appear in the cast map"
    );
}

#[test]
fn cast_map_is_keyed_by_name_not_by_schema_position() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        arb_field("amt", 40, 20, true),
    ]));
    // Ask for the columns in the reverse of schema order.
    let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["amt", "id"]));
    assert_eq!(cast_of(&map, "amt"), Some("numeric(40,20)".to_string()));
    assert_eq!(cast_of(&map, "id"), None);
}

#[test]
fn values_clause_applies_the_numeric_cast_to_a_decimal_arb_placeholder() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        arb_field("amt", 100, 18, true),
    ]));
    let cols = names(&["id", "amt"]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &cols);
    let clause = PostgresQueryBuilder::build_values_clause(1, 2, &cols, &map);
    assert_eq!(
        clause, "($1, $2::numeric(100,18))",
        "the decimal_arb placeholder must carry its numeric cast"
    );
}

#[test]
fn values_clause_numbers_placeholders_contiguously_across_rows() {
    let cols = names(&["a", "b", "c"]);
    let map: HashMap<String, Option<String>> = HashMap::new();
    let clause = PostgresQueryBuilder::build_values_clause(3, 3, &cols, &map);
    assert_eq!(
        clause, "($1, $2, $3), ($4, $5, $6), ($7, $8, $9)",
        "placeholder numbering must be contiguous or binds land on the wrong column"
    );
}

#[test]
fn values_clause_repeats_the_same_cast_for_every_row() {
    let cols = names(&["amt"]);
    let mut map = HashMap::new();
    map.insert("amt".to_string(), Some("numeric(100,18)".to_string()));
    let clause = PostgresQueryBuilder::build_values_clause(3, 1, &cols, &map);
    assert_eq!(
        clause, "($1::numeric(100,18)), ($2::numeric(100,18)), ($3::numeric(100,18))",
        "every row's decimal placeholder needs the cast, not just the first"
    );
}

#[test]
fn values_clause_for_zero_rows_is_empty() {
    let cols = names(&["a"]);
    let map: HashMap<String, Option<String>> = HashMap::new();
    assert_eq!(
        PostgresQueryBuilder::build_values_clause(0, 1, &cols, &map),
        "",
        "zero rows must not emit a stray '()'"
    );
}

#[test]
fn values_clause_leaves_placeholders_beyond_the_column_list_uncast() {
    // This is the `_gs_checkpoint_epoch` slot: one extra placeholder per row.
    let cols = names(&["amt"]);
    let mut map = HashMap::new();
    map.insert("amt".to_string(), Some("numeric(100,18)".to_string()));
    let clause = PostgresQueryBuilder::build_values_clause(1, 2, &cols, &map);
    assert_eq!(
        clause, "($1::numeric(100,18), $2)",
        "the epoch placeholder is a BIGINT and must not inherit a numeric cast"
    );
}

#[test]
fn complete_upsert_query_casts_a_decimal_arb_column() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        arb_field("amt", 100, 18, true),
    ]));
    let q = PostgresQueryBuilder::build_complete_upsert_query(
        "public",
        "t",
        &names(&["id", "amt"]),
        &names(&["id"]),
        1,
        Some(&schema),
        "update",
        None,
        "t",
        None,
    );
    assert!(
        q.contains("$2::numeric(100,18)"),
        "decimal_arb placeholder must be cast in the generated upsert; got: {q}"
    );
    assert!(
        !q.contains("$1::"),
        "an Int64 primary key must not be cast; got: {q}"
    );
}

#[test]
fn complete_upsert_query_casts_a_decimal_arb_primary_key_in_the_values_list() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        arb_field("key", 78, 0, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let q = PostgresQueryBuilder::build_complete_upsert_query(
        "public",
        "t",
        &names(&["key", "v"]),
        &names(&["key"]),
        1,
        Some(&schema),
        "update",
        None,
        "t",
        None,
    );
    assert!(
        q.contains("$1::numeric(78,0)"),
        "a decimal_arb PRIMARY KEY still binds as text and needs the cast; got: {q}"
    );
}

#[test]
fn complete_upsert_query_leaves_the_checkpoint_epoch_placeholder_uncast() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 100, 18, true)]));
    let q = PostgresQueryBuilder::build_complete_upsert_query(
        "public",
        "t",
        &names(&["amt"]),
        &names(&[]),
        1,
        Some(&schema),
        "update",
        None,
        "t",
        Some(9),
    );
    assert!(
        q.contains("($1::numeric(100,18), $2)"),
        "epoch placeholder must be plain; got: {q}"
    );
}

#[test]
fn complete_upsert_query_casts_every_row_of_a_multi_row_decimal_arb_insert() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 40, 20, true)]));
    let q = PostgresQueryBuilder::build_complete_upsert_query(
        "public",
        "t",
        &names(&["amt"]),
        &names(&[]),
        3,
        Some(&schema),
        "update",
        None,
        "t",
        None,
    );
    assert_eq!(
        q.matches("::numeric(40,20)").count(),
        3,
        "one cast per row is required; got: {q}"
    );
}

#[test]
fn complete_upsert_query_without_an_original_schema_emits_no_casts() {
    let q = PostgresQueryBuilder::build_complete_upsert_query(
        "public",
        "t",
        &names(&["a", "b"]),
        &names(&["a"]),
        1,
        None,
        "update",
        None,
        "t",
        None,
    );
    assert!(
        !q.contains("::"),
        "with no schema there is nothing to cast; got: {q}"
    );
}

#[test]
fn upsert_update_set_excludes_primary_key_columns() {
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "public",
        "t",
        &names(&["k", "amt"]),
        &names(&["k"]),
        "update",
        None,
        "t",
    );
    assert!(
        q.contains(r#""amt" = EXCLUDED."amt""#),
        "non-PK column must be updated; got: {q}"
    );
    assert!(
        !q.contains(r#""k" = EXCLUDED."k""#),
        "a conflict-target column must not appear in SET; got: {q}"
    );
}

#[test]
fn upsert_with_only_primary_key_columns_degrades_to_do_nothing() {
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "public",
        "t",
        &names(&["k"]),
        &names(&["k"]),
        "update",
        None,
        "t",
    );
    assert!(q.contains("DO NOTHING"), "got: {q}");
    assert!(!q.contains("DO UPDATE SET"), "got: {q}");
}

#[test]
fn upsert_with_composite_pk_covering_all_columns_degrades_to_do_nothing() {
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "public",
        "t",
        &names(&["a", "b"]),
        &names(&["a", "b"]),
        "update",
        None,
        "t",
    );
    assert!(
        q.contains("DO NOTHING") && !q.contains("DO UPDATE SET"),
        "an empty SET list is a Postgres syntax error; got: {q}"
    );
}

#[test]
fn upsert_without_a_primary_key_has_no_on_conflict_clause() {
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "public",
        "t",
        &names(&["a"]),
        &names(&[]),
        "update",
        None,
        "t",
    );
    assert!(
        !q.contains("ON CONFLICT"),
        "no PK means no conflict target; got: {q}"
    );
}

#[test]
fn upsert_placeholder_count_equals_the_column_count() {
    for n in 1usize..=8 {
        let cols: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
        let (_, ph) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "t",
            &cols,
            &names(&[]),
            "update",
            None,
            "t",
        );
        assert_eq!(ph, n, "placeholders per row must equal the column count");
    }
}

#[test]
fn complete_upsert_query_placeholder_count_is_rows_times_columns() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        arb_field("a", 40, 20, true),
        arb_field("b", 40, 20, true),
    ]));
    for rows in 1usize..=4 {
        let q = PostgresQueryBuilder::build_complete_upsert_query(
            "public",
            "t",
            &names(&["a", "b"]),
            &names(&[]),
            rows,
            Some(&schema),
            "update",
            None,
            "t",
            None,
        );
        let last = format!("${}", rows * 2);
        assert!(
            q.contains(&last),
            "expected {rows} rows x 2 columns to reach {last}; got: {q}"
        );
        assert!(
            !q.contains(&format!("${}", rows * 2 + 1)),
            "placeholder numbering overshot for {rows} rows; got: {q}"
        );
    }
}

#[test]
fn upsert_quotes_the_schema_and_table_identifiers() {
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "my_schema",
        "my_table",
        &names(&["a"]),
        &names(&[]),
        "update",
        None,
        "my_table",
    );
    assert!(
        q.contains(r#""my_schema"."my_table""#),
        "identifiers must be double-quoted; got: {q}"
    );
}

#[test]
fn upsert_quotes_every_column_identifier() {
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "s",
        "t",
        &names(&["Amount", "user"]),
        &names(&[]),
        "update",
        None,
        "t",
    );
    assert!(
        q.contains(r#""Amount", "user""#),
        "mixed-case and reserved words need quoting; got: {q}"
    );
}

#[test]
fn on_conflict_nothing_suppresses_the_update_set_and_the_where_clause() {
    let uw = BTreeMap::from([("v".to_string(), ">".to_string())]);
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "s",
        "t",
        &names(&["k", "v"]),
        &names(&["k"]),
        "nothing",
        Some(&uw),
        "t",
    );
    assert!(q.contains("DO NOTHING"), "got: {q}");
    assert!(
        !q.contains("WHERE"),
        "DO NOTHING has no SET to guard; got: {q}"
    );
}

#[test]
fn update_where_clause_is_emitted_only_for_the_update_strategy() {
    let uw = BTreeMap::from([("v".to_string(), ">".to_string())]);
    let (upd, _) = PostgresQueryBuilder::build_upsert_query(
        "s",
        "t",
        &names(&["k", "v"]),
        &names(&["k"]),
        "update",
        Some(&uw),
        "dest",
    );
    assert!(
        upd.contains(r#"WHERE EXCLUDED."v" > "dest"."v""#),
        "got: {upd}"
    );
}

#[test]
fn update_where_conditions_are_anded_in_deterministic_order() {
    let uw = BTreeMap::from([
        ("a".to_string(), ">".to_string()),
        ("b".to_string(), ">=".to_string()),
    ]);
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "s",
        "t",
        &names(&["k", "a", "b"]),
        &names(&["k"]),
        "update",
        Some(&uw),
        "dest",
    );
    let expected = r#"WHERE EXCLUDED."a" > "dest"."a" AND EXCLUDED."b" >= "dest"."b""#;
    assert!(
        q.contains(expected),
        "BTreeMap ordering must give a stable, prepared-statement-cacheable SQL text; got: {q}"
    );
}

#[test]
fn update_where_with_an_unlisted_operator_is_dropped_from_the_sql() {
    let uw = BTreeMap::from([("v".to_string(), "LIKE".to_string())]);
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "s",
        "t",
        &names(&["k", "v"]),
        &names(&["k"]),
        "update",
        Some(&uw),
        "dest",
    );
    assert!(
        !q.contains("LIKE"),
        "a non-whitelisted operator must never be interpolated into SQL; got: {q}"
    );
}

#[test]
fn update_where_operator_whitespace_is_trimmed_before_the_whitelist_check() {
    let uw = BTreeMap::from([("v".to_string(), "  >=  ".to_string())]);
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "s",
        "t",
        &names(&["k", "v"]),
        &names(&["k"]),
        "update",
        Some(&uw),
        "dest",
    );
    assert!(
        q.contains(r#"EXCLUDED."v" >= "dest"."v""#),
        "padded operators must be accepted after trimming; got: {q}"
    );
}

#[test]
fn update_where_on_a_decimal_arb_column_compares_the_column_not_a_literal() {
    let uw = BTreeMap::from([("amt".to_string(), ">".to_string())]);
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "s",
        "t",
        &names(&["k", "amt"]),
        &names(&["k"]),
        "update",
        Some(&uw),
        "dest",
    );
    assert!(
        q.contains(r#"EXCLUDED."amt" > "dest"."amt""#),
        "the guard must compare NUMERIC columns, so no cast is needed here; got: {q}"
    );
}

#[test]
fn checkpoint_epoch_column_is_appended_to_the_insert_column_list() {
    let q = PostgresQueryBuilder::build_complete_upsert_query(
        "s",
        "t",
        &names(&["k", "amt"]),
        &names(&["k"]),
        1,
        None,
        "update",
        None,
        "t",
        Some(3),
    );
    assert!(
        q.contains(r#""k", "amt", "_gs_checkpoint_epoch""#),
        "got: {q}"
    );
}

#[test]
fn checkpoint_epoch_is_not_added_to_the_conflict_target() {
    let q = PostgresQueryBuilder::build_complete_upsert_query(
        "s",
        "t",
        &names(&["k", "amt"]),
        &names(&["k"]),
        1,
        None,
        "update",
        None,
        "t",
        Some(3),
    );
    assert!(
        q.contains(r#"ON CONFLICT ("k")"#),
        "the conflict target must stay the declared primary key; got: {q}"
    );
}

#[test]
fn upsert_query_text_is_stable_across_identical_calls() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        arb_field("a", 40, 20, true),
        arb_field("b", 40, 20, true),
    ]));
    let build = || {
        PostgresQueryBuilder::build_complete_upsert_query(
            "s",
            "t",
            &names(&["a", "b"]),
            &names(&[]),
            2,
            Some(&schema),
            "update",
            None,
            "t",
            None,
        )
    };
    assert_eq!(
        build(),
        build(),
        "unstable SQL text defeats Postgres prepared-statement caching"
    );
}

#[test]
fn values_placeholder_placement_matches_the_column_name_list_order() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        arb_field("amt", 40, 20, true),
        Field::new("id", DataType::Int64, false),
    ]));
    // Column list order is id, amt — the opposite of the schema order.
    let cols = names(&["id", "amt"]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &cols);
    let clause = PostgresQueryBuilder::build_values_clause(1, 2, &cols, &map);
    assert_eq!(
        clause, "($1, $2::numeric(40,20))",
        "the cast must follow the column NAME at that position, not the schema position"
    );
}

// ===========================================================================
// C. DDL <-> bind agreement for the sink
// ===========================================================================

#[test]
fn decimal_arb_ddl_is_numeric_with_declared_precision_and_scale() {
    let f = arb_field("amt", 100, 18, true);
    assert_eq!(arrow_field_to_postgres_type(&f), "NUMERIC(100, 18)");
}

#[test]
fn decimal_arb_ddl_scale_zero_is_numeric_p_0() {
    let f = arb_field("amt", 78, 0, true);
    assert_eq!(arrow_field_to_postgres_type(&f), "NUMERIC(78, 0)");
}

#[test]
fn decimal_arb_ddl_keeps_the_scale_it_was_declared_with() {
    // Whatever precision policy applies, the *scale* must never drift: it is
    // what decides where the decimal point sits in the bound literal.
    for (p, s) in [(78u32, 0u32), (100, 18), (200, 199), (1000, 30)] {
        let ddl = arrow_field_to_postgres_type(&arb_field("amt", p, s, true));
        let declared_scale: u32 = ddl
            .trim_end_matches(')')
            .rsplit(',')
            .next()
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(declared_scale, s, "scale drifted in {ddl}");
    }
}

#[test]
fn decimal_arb_ddl_round_trips_back_to_a_decimal_arb_field() {
    for (p, s) in [(78u32, 1u32), (100, 18), (200, 100), (1000, 0)] {
        let f = arb_field("amt", p, s, true);
        let ddl = arrow_field_to_postgres_type(&f);
        let back = postgres_type_to_arrow_field(&ddl, "amt", true)
            .unwrap_or_else(|e| panic!("{ddl} must map back to a field: {e}"));
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&back),
            Some((p, s)),
            "{ddl} must round-trip to decimal_arb({p},{s}) or the sink table and the \
             reader disagree about where the point sits"
        );
    }
}

#[test]
fn numeric_78_0_round_trips_with_the_u256_native_int_hint() {
    let f = arb_field("amt", 78, 0, true);
    let ddl = arrow_field_to_postgres_type(&f);
    let back = postgres_type_to_arrow_field(&ddl, "amt", true).unwrap();
    assert_eq!(
        DecimalArbType::native_int_kind_from_field(&back),
        Some(NativeIntKind::U256),
        "NUMERIC(78,0) is the conventional uint256 shape and must carry the hint back"
    );
}

#[test]
fn decimal128_ddl_and_cast_agree_across_the_whole_scale_range() {
    for s in 0u8..=38 {
        let f = Field::new("amt", DataType::Decimal128(38, s as i8), true);
        let ddl = arrow_field_to_postgres_type(&f);
        let cast = get_postgres_type_info(&f).string_cast_sql.unwrap();
        assert_eq!(
            norm(&ddl),
            norm(&cast),
            "Decimal128(38,{s}): DDL and INSERT cast must be the same NUMERIC type"
        );
    }
}

#[test]
fn decimal256_ddl_and_cast_agree_across_a_range_of_scales() {
    for s in [0i8, 1, 9, 18, 30, 38, 50, 60] {
        let f = Field::new("amt", DataType::Decimal256(76, s), true);
        let ddl = arrow_field_to_postgres_type(&f);
        let cast = get_postgres_type_info(&f).string_cast_sql.unwrap();
        assert_eq!(norm(&ddl), norm(&cast), "Decimal256(76,{s}) mismatch");
    }
}

#[test]
fn decimal_arb_is_bound_as_a_string_and_therefore_must_carry_a_cast() {
    let f = arb_field("amt", 100, 18, true);
    assert!(
        get_postgres_type_info(&f).string_cast_sql.is_some(),
        "decimal_arb reaches the binder as Utf8; without a cast Postgres sees TEXT"
    );
}

#[test]
fn override_schema_for_postgres_insert_leaves_decimal_arb_untouched() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 100, 18, true)]));
    let out = override_schema_for_postgres_insert(&schema);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(out.field(0)),
        Some((100, 18)),
        "decimal_arb is handled by the projection, not by the schema override; its \
         (precision, scale) must survive so the cast map still finds it"
    );
}

#[test]
fn override_schema_for_postgres_insert_converts_nested_types_to_utf8() {
    let inner = Field::new("item", DataType::Int64, true);
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "l",
        DataType::List(Arc::new(inner)),
        true,
    )]));
    let out = override_schema_for_postgres_insert(&schema);
    assert_eq!(out.field(0).data_type(), &DataType::Utf8);
}

#[test]
fn override_schema_for_postgres_insert_preserves_field_order_and_names() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        arb_field("amt", 100, 18, true),
    ]));
    let out = override_schema_for_postgres_insert(&schema);
    let got: Vec<&str> = out.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(got, vec!["id", "amt"]);
}

#[tokio::test]
async fn projected_literal_fits_the_ddl_type_the_sink_would_create() {
    // Only p > 76 round-trips back to decimal_arb; the narrower bands come
    // back as Decimal128/Decimal256, which is covered separately below.
    for (p, s) in [(78u32, 0u32), (100, 18), (200, 100), (1000, 30)] {
        let f = arb_field("amt", p, s, true);
        let ddl = arrow_field_to_postgres_type(&f);
        let back = postgres_type_to_arrow_field(&ddl, "amt", true).unwrap();
        let (dp, ds) = DecimalArbType::precision_scale_from_field(&back)
            .unwrap_or_else(|| panic!("{ddl} must map back to a decimal_arb field"));
        assert_eq!((dp, ds), (p, s), "{ddl} must round-trip precision/scale");
        for v in [ulp(s), format!("-{}", ulp(s)), "0".into(), "1".into()] {
            let got = project_one(p, s, &v).await;
            assert_fits(&got, dp, ds, &format!("{ddl} input {v:?}"));
        }
    }
}

#[tokio::test]
async fn narrow_decimal_arb_ddl_comes_back_as_a_numerically_equivalent_decimal_type() {
    // decimal_arb(p, s) with p <= 76 emits NUMERIC(p, s), which the reader maps
    // to Decimal128/Decimal256. That is fine as long as (precision, scale) —
    // and therefore where the point sits — is preserved exactly.
    for (p, s) in [(38u32, 38u32), (76, 30), (40, 20), (10, 0)] {
        let ddl = arrow_field_to_postgres_type(&arb_field("amt", p, s, true));
        let back = postgres_type_to_arrow_field(&ddl, "amt", true).unwrap();
        let got = match back.data_type() {
            DataType::Decimal128(bp, bs) => (*bp as u32, *bs as u32),
            DataType::Decimal256(bp, bs) => (*bp as u32, *bs as u32),
            other => panic!("{ddl} mapped to an unexpected type {other:?}"),
        };
        assert_eq!(
            got,
            (p, s),
            "{ddl} must preserve (precision, scale) across the narrowing round trip"
        );
        let projected = project_one(p, s, &ulp(s)).await;
        assert_fits(&projected, p, s, &format!("narrowed {ddl}"));
    }
}

/// PostgreSQL caps an explicit `NUMERIC(p, s)` declaration at precision 1000,
/// but `decimal_arb`'s `MAX_PRECISION` is 65_535 and the mapping emits the
/// declared precision verbatim in both the DDL and the per-placeholder cast.
#[test]
#[ignore = "FINDING: decimal_arb precision above 1000 is emitted verbatim as NUMERIC(p, s), which PostgreSQL rejects (max explicit NUMERIC precision is 1000)"]
fn decimal_arb_ddl_precision_stays_within_the_postgres_numeric_limit() {
    for p in [1001u32, 5000, 65535] {
        let ddl = arrow_field_to_postgres_type(&arb_field("amt", p, 0, true));
        let declared: u32 = ddl
            .trim_start_matches("NUMERIC(")
            .split(',')
            .next()
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            declared <= 1000,
            "PostgreSQL rejects an explicit NUMERIC precision above 1000; emitting {ddl} \
             makes CREATE TABLE fail for a decimal_arb({p}, 0) column"
        );
    }
}

#[test]
#[ignore = "FINDING: the same over-1000 precision also lands in the ::numeric(p,s) cast on every VALUES placeholder, so even a pre-existing table cannot be inserted into"]
fn decimal_arb_insert_cast_precision_stays_within_the_postgres_numeric_limit() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 65535, 0, true)]));
    let q = PostgresQueryBuilder::build_complete_upsert_query(
        "s",
        "t",
        &names(&["amt"]),
        &names(&[]),
        1,
        Some(&schema),
        "update",
        None,
        "t",
        None,
    );
    assert!(
        !q.contains("numeric(65535,0)"),
        "a cast to numeric(65535,0) is not a valid Postgres type; got: {q}"
    );
}

#[test]
fn decimal_arb_ddl_at_the_postgres_limit_is_still_emitted_exactly() {
    assert_eq!(
        arrow_field_to_postgres_type(&arb_field("amt", 1000, 30, true)),
        "NUMERIC(1000, 30)",
        "precision 1000 is the largest Postgres accepts and must pass through unchanged"
    );
}

// ===========================================================================
// D. DELETE path
// ===========================================================================

#[test]
fn delete_query_with_no_primary_key_is_empty() {
    assert_eq!(
        PostgresQueryBuilder::build_delete_query("s", "t", &names(&[]), 3),
        "",
        "without a PK there is no safe DELETE; an empty string is the guard"
    );
}

#[test]
fn delete_query_single_pk_numbers_one_placeholder_per_row() {
    let q = PostgresQueryBuilder::build_delete_query("s", "t", &names(&["k"]), 3);
    assert!(q.contains("($1), ($2), ($3)"), "got: {q}");
}

#[test]
fn delete_query_composite_pk_numbers_placeholders_row_major() {
    let q = PostgresQueryBuilder::build_delete_query("s", "t", &names(&["a", "b"]), 2);
    assert!(q.contains("($1, $2), ($3, $4)"), "got: {q}");
}

#[test]
fn delete_query_quotes_the_primary_key_identifiers() {
    let q = PostgresQueryBuilder::build_delete_query("s", "t", &names(&["a", "b"]), 1);
    assert!(q.contains(r#"("a", "b") IN"#), "got: {q}");
}

#[test]
fn delete_query_quotes_the_schema_and_table() {
    let q = PostgresQueryBuilder::build_delete_query("my_s", "my_t", &names(&["k"]), 1);
    assert!(q.contains(r#"DELETE FROM "my_s"."my_t""#), "got: {q}");
}

#[test]
fn delete_query_with_zero_rows_emits_no_value_tuples() {
    let q = PostgresQueryBuilder::build_delete_query("s", "t", &names(&["k"]), 0);
    assert!(
        !q.contains("$1"),
        "zero rows must not produce a placeholder; got: {q}"
    );
}

#[test]
#[ignore = "FINDING: build_delete_query emits no ::numeric cast, so a decimal_arb / Decimal128 / UInt64 primary key — bound as TEXT by value_binding — is compared against a NUMERIC column (numeric = text has no operator in Postgres)"]
fn delete_query_casts_a_decimal_arb_primary_key_like_the_insert_does() {
    // The INSERT path casts the string bind back to NUMERIC:
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("key", 78, 0, false)]));
    let insert = PostgresQueryBuilder::build_complete_upsert_query(
        "s",
        "t",
        &names(&["key"]),
        &names(&["key"]),
        1,
        Some(&schema),
        "update",
        None,
        "t",
        None,
    );
    assert!(
        insert.contains("::numeric(78,0)"),
        "precondition: the INSERT path casts the decimal_arb bind; got: {insert}"
    );
    // The DELETE path binds the *same* projected Utf8 value for the same
    // column but has no schema parameter at all, so it cannot cast.
    let delete = PostgresQueryBuilder::build_delete_query("s", "t", &names(&["key"]), 1);
    assert!(
        delete.contains("::numeric(78,0)"),
        "the DELETE placeholder binds the same TEXT value as the INSERT and must be \
         cast to the column's NUMERIC type; got: {delete}"
    );
}

#[test]
#[ignore = "FINDING: same DELETE cast gap for a UInt64 primary key (bound as String -> NUMERIC(20,0) column)"]
fn delete_query_casts_a_uint64_primary_key_like_the_insert_does() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "slot",
        DataType::UInt64,
        false,
    )]));
    let insert = PostgresQueryBuilder::build_complete_upsert_query(
        "s",
        "t",
        &names(&["slot"]),
        &names(&["slot"]),
        1,
        Some(&schema),
        "update",
        None,
        "t",
        None,
    );
    assert!(insert.contains("::numeric(20,0)"), "precondition: {insert}");
    let delete = PostgresQueryBuilder::build_delete_query("s", "t", &names(&["slot"]), 1);
    assert!(
        delete.contains("::numeric(20,0)"),
        "UInt64 binds as a String; the DELETE comparison needs the same cast; got: {delete}"
    );
}

#[test]
fn delete_query_placeholder_count_is_rows_times_pk_columns() {
    for rows in 1usize..=4 {
        let q = PostgresQueryBuilder::build_delete_query("s", "t", &names(&["a", "b"]), rows);
        let last = format!("${}", rows * 2);
        assert!(
            q.contains(&last),
            "expected {last} for {rows} rows; got: {q}"
        );
        assert!(
            !q.contains(&format!("${}", rows * 2 + 1)),
            "overshot for {rows} rows; got: {q}"
        );
    }
}

// ===========================================================================
// E. update_where validation
// ===========================================================================

#[test]
fn validate_update_where_accepts_a_decimal_arb_column() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 100, 18, true)]));
    let uw = BTreeMap::from([("amt".to_string(), ">".to_string())]);
    assert!(
        validate_update_where(&uw, &schema, "sink").is_ok(),
        "a decimal_arb column is a legal update_where guard"
    );
}

#[test]
fn validate_update_where_rejects_a_column_missing_from_the_schema() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", 100, 18, true)]));
    let uw = BTreeMap::from([("nope".to_string(), ">".to_string())]);
    let err = validate_update_where(&uw, &schema, "sink")
        .expect_err("an unknown column must fail fast at startup")
        .to_string();
    assert!(err.contains("nope"), "error must name the column: {err}");
    assert!(err.contains("sink"), "error must name the sink: {err}");
}

#[test]
fn validate_update_where_rejects_a_non_whitelisted_operator() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    for bad in ["LIKE", "IS", "@>", "; DROP TABLE t --", "=="] {
        let uw = BTreeMap::from([("v".to_string(), bad.to_string())]);
        assert!(
            validate_update_where(&uw, &schema, "sink").is_err(),
            "operator {bad:?} must be rejected, it is interpolated straight into SQL"
        );
    }
}

#[test]
fn validate_update_where_accepts_every_advertised_operator() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    for op in ALLOWED_UPDATE_WHERE_OPS {
        let uw = BTreeMap::from([("v".to_string(), op.to_string())]);
        assert!(
            validate_update_where(&uw, &schema, "sink").is_ok(),
            "advertised operator {op:?} must validate"
        );
    }
}

#[test]
fn validate_update_where_trims_operator_whitespace() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let uw = BTreeMap::from([("v".to_string(), " >= ".to_string())]);
    assert!(
        validate_update_where(&uw, &schema, "sink").is_ok(),
        "a padded operator is valid after trimming, matching the SQL builder"
    );
}

#[test]
fn validate_update_where_accepts_an_empty_map() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    assert!(validate_update_where(&BTreeMap::new(), &schema, "sink").is_ok());
}

#[test]
fn validate_update_where_agrees_with_the_operators_the_builder_accepts() {
    // Anything validate_update_where allows must actually appear in the SQL,
    // otherwise a configured guard silently does nothing.
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    for op in ALLOWED_UPDATE_WHERE_OPS {
        let uw = BTreeMap::from([("v".to_string(), op.to_string())]);
        assert!(validate_update_where(&uw, &schema, "sink").is_ok());
        let (q, _) = PostgresQueryBuilder::build_upsert_query(
            "s",
            "t",
            &names(&["k", "v"]),
            &names(&["k"]),
            "update",
            Some(&uw),
            "dest",
        );
        assert!(
            q.contains(&format!(r#"EXCLUDED."v" {op} "dest"."v""#)),
            "operator {op:?} validated but was dropped from the SQL; got: {q}"
        );
    }
}

#[test]
#[ignore = "FINDING: identifiers are wrapped in double quotes without escaping embedded quotes, so a column/table name containing '\"' breaks out of the quoted identifier"]
fn identifiers_containing_a_double_quote_are_escaped() {
    let (q, _) = PostgresQueryBuilder::build_upsert_query(
        "s",
        "t",
        &names(&[r#"ev"il"#]),
        &names(&[]),
        "update",
        None,
        "t",
    );
    assert!(
        q.contains(r#""ev""il""#),
        "a double quote inside an identifier must be doubled; got: {q}"
    );
}

// ===========================================================================
// F. cross-checks tying the value path to the SQL path
// ===========================================================================

#[tokio::test]
async fn the_projected_literal_and_the_cast_map_describe_the_same_numeric_type() {
    for (p, s) in [(38u32, 18u32), (78, 0), (100, 50), (40, 40)] {
        let schema: SchemaRef = Arc::new(Schema::new(vec![arb_field("amt", p, s, true)]));
        let map = PostgresQueryBuilder::build_cast_map(&schema, &names(&["amt"]));
        let cast = cast_of(&map, "amt").expect("decimal_arb must have a cast");
        assert_eq!(cast, format!("numeric({p},{s})"));
        for v in [ulp(s), format!("-{}", ulp(s)), "0".into()] {
            let got = project_one(p, s, &v).await;
            assert_fits(&got, p, s, &format!("cast {cast} input {v:?}"));
        }
    }
}

#[tokio::test]
async fn a_full_sink_schema_projects_and_casts_consistently() {
    let p = 100;
    let s = 18;
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        arb_field("amt", p, s, true),
        Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])) as ArrayRef,
            arb_column("amt", p, s, &[Some("0.000000000000000001"), Some("-1")]),
            Arc::new(StringArray::from(vec!["i", "d"])) as ArrayRef,
        ],
    )
    .unwrap();
    let (_, batches) = sink_input_plan(schema.clone(), batch, Some("id"), false).await;
    let vals = strings_of(&batches, 1);
    assert_eq!(
        vals,
        vec![
            Some("0.000000000000000001".to_string()),
            Some("-1.000000000000000000".to_string()),
        ],
        "sink projection must place the point at scale {s}"
    );

    // The columns that reach the INSERT exclude _gs_op in normal mode.
    let cols = names(&["id", "amt"]);
    let map = PostgresQueryBuilder::build_cast_map(&schema, &cols);
    assert_eq!(cast_of(&map, "amt"), Some(format!("numeric({p},{s})")));
    for v in vals.iter().flatten() {
        assert_fits(v, p, s, "full sink schema");
    }
}

#[tokio::test]
async fn every_scale_produces_a_literal_whose_fraction_length_is_zero_or_the_scale() {
    // to_plain_string keeps the decoded scale, so a non-zero value must carry
    // exactly `scale` fractional digits; only the canonicalized zero may differ.
    for s in 0u32..=30 {
        let got = project_one(60, s, "1").await;
        let frac = got.split('.').nth(1).map(|f| f.len()).unwrap_or(0) as u32;
        assert_eq!(
            frac, s,
            "scale {s}: a non-zero value must render exactly {s} fractional digits, got {got:?}"
        );
    }
}

#[tokio::test]
async fn sign_never_appears_anywhere_but_the_first_character() {
    for s in 0u32..=20 {
        for v in [format!("-{}", ulp(s)), "-1".to_string()] {
            let got = project_one(50, s, &v).await;
            assert!(got.starts_with('-'), "expected leading sign in {got:?}");
            assert!(
                !got[1..].contains('-'),
                "the sign must precede the padded zeros, got {got:?}"
            );
        }
    }
}
