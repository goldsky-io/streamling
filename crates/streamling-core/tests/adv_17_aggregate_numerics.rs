//! Adversarial pass 17 — *numeric* correctness of the `decimal_arb`
//! aggregate accumulators (`SUM` / `AVG` / `MIN` / `MAX`).
//!
//! Agent 03 owns aggregate *plan shapes* (GROUP BY / HAVING / FILTER /
//! GROUPING SETS / windows). This file owns the arithmetic: does the number
//! that comes out of the accumulator equal the number a mathematician would
//! write down?
//!
//! Coverage:
//!   * SUM accumulating far past the declared input precision (the whole
//!     point of the `p + 16` headroom), including carries across byte
//!     boundaries and values past `i128`/`u256`.
//!   * SUM of alternating large positive/negative values that swing outside
//!     the declared precision mid-stream and cancel back into range.
//!   * AVG scale widening (`s -> s + 1`) and its half-to-even rounding,
//!     with every exact-halfway case at scales 0, 2 and 18.
//!   * AVG over a single row, over rows summing to zero, and over all-NULL.
//!   * MIN/MAX where the canonical byte order diverges from numeric order
//!     (sign byte, magnitude length, negative magnitudes).
//!   * 10k+ row aggregates split across many record batches, exercising
//!     repeated `Accumulator::update_batch` on one accumulator instance.
//!     (Partial/final `merge_batch` combination is NOT reachable through SQL
//!     in this session — see `register_chunks`.)
//!   * NULL-skipping: `SUM` of all NULLs must be NULL, never 0; a NULL row
//!     must not enter AVG's denominator.
//!
//! Decoding policy (matches the accumulators in
//! `streamling-common/src/functions/decimal_arb_aggregates.rs`):
//!   * SUM / MIN / MAX emit bytes at the *input* scale `s`.
//!   * AVG emits bytes at `s + 1`.
//!
//! The output field carries no decimal_arb metadata today (already reported
//! by agent 03 for AVG), so the scale is supplied explicitly at decode time
//! rather than read back off the schema — this file deliberately does not
//! re-litigate the metadata question.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;

use streamling_core::dynamic_table::DynamicTableRegistry;
use streamling_core::session::SessionManager;
use streamling_core::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};

// =====================================================================
// Harness
// =====================================================================

fn session() -> SessionManager {
    SessionManager::new(8192, 10, DynamicTableRegistry::new()).expect("SessionManager::new")
}

fn dv(s: &str) -> DecimalArbValue {
    DecimalArbValue::from_str(s).unwrap_or_else(|e| panic!("parse decimal_arb `{s}`: {e}"))
}

/// `Some(..)` values from string literals.
fn vs(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

/// `n` copies of `v`.
fn rep(v: &str, n: usize) -> Vec<Option<String>> {
    (0..n).map(|_| Some(v.to_string())).collect()
}

fn arb_array(name: &str, p: u32, s: u32, values: &[Option<String>]) -> LargeBinaryArray {
    let mut b = DecimalArbArrayBuilder::with_capacity(values.len().max(1), name, p, s)
        .unwrap_or_else(|e| panic!("builder({name}, {p}, {s}): {e}"));
    for v in values {
        match v {
            Some(x) => b
                .append_value(&dv(x))
                .unwrap_or_else(|e| panic!("append `{x}` to decimal_arb({p}, {s}): {e}")),
            None => b.append_null(),
        }
    }
    b.finish().into_inner().0
}

fn amt_schema(p: u32, s: u32) -> SchemaRef {
    Arc::new(Schema::new(vec![
        DecimalArbType::field("amt", p, s, true).unwrap(),
    ]))
}

fn amt_batch(schema: &SchemaRef, p: u32, s: u32, values: &[Option<String>]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arb_array("amt", p, s, values))],
    )
    .unwrap()
}

/// `t(amt decimal_arb(p, s))` — one partition, one batch.
fn register_single(sm: &SessionManager, table: &str, p: u32, s: u32, values: &[Option<String>]) {
    let schema = amt_schema(p, s);
    let batch = amt_batch(&schema, p, s, values);
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
}

/// One record batch per slice inside a single partition, plus a constant `g`
/// column so the query can be written as `GROUP BY g`.
///
/// This is the *grouped* accumulator path fed one batch at a time, which is
/// how repeated `Accumulator::update_batch` calls get exercised. It is
/// deliberately NOT a multi-partition MemTable: this session's physical
/// optimizer emits one row per input partition instead of merging partial
/// aggregates, so a multi-partition layout cannot express "one answer" here.
/// That behaviour is already reported by adversarial agent 03 and is out of
/// scope for this file; the consequence for us is that `merge_batch` is not
/// reachable through SQL in this harness.
fn register_chunks(
    sm: &SessionManager,
    table: &str,
    p: u32,
    s: u32,
    chunks: &[Vec<Option<String>>],
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        DecimalArbType::field("amt", p, s, true).unwrap(),
    ]));
    let part: Vec<RecordBatch> = chunks
        .iter()
        .map(|vals| {
            let g = StringArray::from(vec!["all"; vals.len()]);
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(g), Arc::new(arb_array("amt", p, s, vals))],
            )
            .unwrap()
        })
        .collect();
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![part]).unwrap()),
    )
    .unwrap();
}

/// Several record batches inside a *single* partition — forces repeated
/// `update_batch` calls on one accumulator instance.
fn register_batches(
    sm: &SessionManager,
    table: &str,
    p: u32,
    s: u32,
    batches: &[Vec<Option<String>>],
) {
    let schema = amt_schema(p, s);
    let part: Vec<RecordBatch> = batches
        .iter()
        .map(|vals| amt_batch(&schema, p, s, vals))
        .collect();
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![part]).unwrap()),
    )
    .unwrap();
}

/// `t(g Utf8, amt decimal_arb(p, s))`, one partition.
fn register_grouped(
    sm: &SessionManager,
    table: &str,
    p: u32,
    s: u32,
    rows: &[(&str, Option<String>)],
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        DecimalArbType::field("amt", p, s, true).unwrap(),
    ]));
    let g: Vec<&str> = rows.iter().map(|(g, _)| *g).collect();
    let amt: Vec<Option<String>> = rows.iter().map(|(_, a)| a.clone()).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(g)),
            Arc::new(arb_array("amt", p, s, &amt)),
        ],
    )
    .unwrap();
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
}

async fn try_sql(sm: &SessionManager, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let plan = sm
        .create_logical_plan(sql.to_string())
        .await
        .map_err(|e| format!("PLAN: {e}"))?;
    sm.new_df(plan)
        .collect()
        .await
        .map_err(|e| format!("EXEC: {e}"))
}

async fn sql_ok(sm: &SessionManager, sql: &str) -> Vec<RecordBatch> {
    try_sql(sm, sql)
        .await
        .unwrap_or_else(|e| panic!("query should succeed:\n  {sql}\n  {e}"))
}

async fn sql_err(sm: &SessionManager, sql: &str) -> String {
    match try_sql(sm, sql).await {
        Ok(b) => panic!(
            "query should have failed but returned {} row(s):\n  {sql}",
            b.iter().map(|x| x.num_rows()).sum::<usize>()
        ),
        Err(e) => e,
    }
}

fn column_index(batches: &[RecordBatch], name: &str) -> usize {
    batches[0]
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("no output column `{name}` in {:?}", batches[0].schema()))
}

/// Decode a decimal_arb-encoded output column at `scale`.
fn arb_vals(batches: &[RecordBatch], name: &str, scale: u32) -> Vec<Option<DecimalArbValue>> {
    let idx = column_index(batches, name);
    let mut out = Vec::new();
    for b in batches {
        let f = b.schema().field(idx).clone();
        assert_eq!(
            f.data_type(),
            &DataType::LargeBinary,
            "column `{name}` should use decimal_arb LargeBinary storage, got {f:?}"
        );
        let a = b
            .column(idx)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap_or_else(|| panic!("column `{name}` is not a LargeBinaryArray"));
        for i in 0..a.len() {
            if a.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(
                    DecimalArbValue::from_canonical_bytes_at_scale(a.value(i), scale)
                        .unwrap_or_else(|e| {
                            panic!("decode `{name}` row {i} at scale {scale}: {e}")
                        }),
                ));
            }
        }
    }
    out
}

/// Single-row aggregate result decoded at `scale`.
fn one(batches: &[RecordBatch], name: &str, scale: u32) -> Option<DecimalArbValue> {
    let v = arb_vals(batches, name, scale);
    assert_eq!(v.len(), 1, "expected exactly one aggregate row, got {v:?}");
    v.into_iter().next().unwrap()
}

fn i64_vals(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let idx = column_index(batches, name);
    let mut out = Vec::new();
    for b in batches {
        let a = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| {
                panic!(
                    "column `{name}` is not Int64: {:?}",
                    b.schema().field(idx).data_type()
                )
            });
        for i in 0..a.len() {
            out.push(if a.is_null(i) { None } else { Some(a.value(i)) });
        }
    }
    out
}

fn text_vals(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let idx = column_index(batches, name);
    let mut out = Vec::new();
    for b in batches {
        let a = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("group column is Utf8");
        for i in 0..a.len() {
            out.push(a.value(i).to_string());
        }
    }
    out
}

fn map_arb(
    batches: &[RecordBatch],
    gcol: &str,
    acol: &str,
    scale: u32,
) -> BTreeMap<String, Option<DecimalArbValue>> {
    let g = text_vals(batches, gcol);
    let a = arb_vals(batches, acol, scale);
    assert_eq!(g.len(), a.len(), "group/agg column length mismatch");
    g.into_iter().zip(a).collect()
}

/// Convenience: run `SELECT <agg>(amt) AS r FROM <table>` and decode at `scale`.
async fn agg(sm: &SessionManager, sql: &str, scale: u32) -> Option<DecimalArbValue> {
    let b = sql_ok(sm, sql).await;
    one(&b, "r", scale)
}

// =====================================================================
// 1. SUM — exact accumulation past the declared input precision
// =====================================================================

#[tokio::test]
async fn sum_of_ten_thousand_ones_is_exactly_ten_thousand() {
    let sm = session();
    register_single(&sm, "t", 30, 0, &rep("1", 10_000));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("10000")),
        "SUM over 10k rows must be exact; any drift means the accumulator is \
         not adding in arbitrary precision"
    );
}

#[tokio::test]
async fn sum_of_ten_thousand_rows_split_into_four_chunks_is_exact() {
    let sm = session();
    let parts: Vec<Vec<Option<String>>> = (0..4).map(|_| rep("1", 2_500)).collect();
    register_chunks(&sm, "t", 30, 0, &parts);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t GROUP BY g", 0).await,
        Some(dv("10000")),
        "accumulating 4 separate record batches lost rows"
    );
}

#[tokio::test]
async fn sum_of_ten_thousand_rows_split_into_many_batches_is_exact() {
    let sm = session();
    let batches: Vec<Vec<Option<String>>> = (0..20).map(|_| rep("1", 500)).collect();
    register_batches(&sm, "t", 30, 0, &batches);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("10000")),
        "repeated update_batch on one accumulator lost rows"
    );
}

#[tokio::test]
async fn sum_grows_far_past_the_declared_input_precision() {
    // decimal_arb(3, 0) holds at most 999; the SUM of 10k of them needs 7
    // digits. The p+16 headroom is exactly what makes this legal.
    let sm = session();
    register_single(&sm, "t", 3, 0, &rep("999", 10_000));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("9990000")),
        "SUM must be allowed to exceed the input column's precision"
    );
}

#[tokio::test]
async fn sum_of_a_single_digit_column_over_ten_thousand_rows() {
    let sm = session();
    register_single(&sm, "t", 1, 0, &rep("9", 10_000));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("90000")),
        "decimal_arb(1, 0) SUM must widen to 5 digits without truncation"
    );
}

#[tokio::test]
async fn sum_keeps_the_input_scale_and_never_rounds_it() {
    let sm = session();
    register_single(&sm, "t", 40, 18, &vs(&["0.000000000000000001"]));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 18).await,
        Some(dv("0.000000000000000001")),
        "SUM must emit at the input scale; a rounded scale silently eats the \
         smallest representable unit"
    );
}

#[tokio::test]
async fn sum_of_ten_thousand_smallest_units_at_scale_eighteen_is_exact() {
    let sm = session();
    register_single(&sm, "t", 40, 18, &rep("0.000000000000000001", 10_000));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 18).await,
        Some(dv("0.00000000000001")),
        "10000 x 1e-18 must be exactly 1e-14"
    );
}

#[tokio::test]
async fn sum_at_scale_eighteen_across_batches_keeps_every_fractional_digit() {
    let sm = session();
    register_chunks(
        &sm,
        "t",
        40,
        18,
        &[
            vs(&["1.111111111111111111"]),
            vs(&["2.222222222222222222"]),
            vs(&["3.333333333333333333"]),
        ],
    );
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t GROUP BY g", 18).await,
        Some(dv("6.666666666666666666")),
        "batching must not shift the fraction"
    );
}

#[tokio::test]
async fn sum_carries_across_a_magnitude_byte_boundary() {
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["255", "1"]));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("256")),
        "255 + 1 must carry into a second magnitude byte"
    );
}

#[tokio::test]
async fn sum_of_two_hundred_fifty_six_ones_crosses_the_byte_boundary() {
    let sm = session();
    register_single(&sm, "t", 10, 0, &rep("1", 256));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("256"))
    );
}

#[tokio::test]
async fn sum_crosses_the_two_byte_boundary_exactly() {
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["65535", "1"]));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("65536"))
    );
}

#[tokio::test]
async fn sum_of_two_i128_maxima_exceeds_i128_exactly() {
    let sm = session();
    register_single(
        &sm,
        "t",
        60,
        0,
        &rep("170141183460469231731687303715884105727", 2),
    );
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("340282366920938463463374607431768211454")),
        "SUM must not silently wrap at i128"
    );
}

#[tokio::test]
async fn sum_of_two_u256_maxima_exceeds_u256_exactly() {
    let u256_max = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    let expected = "231584178474632390847141970017375815706539969331281128078915168015826259279870";
    let sm = session();
    register_single(&sm, "t", 100, 0, &rep(u256_max, 2));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv(expected)),
        "SUM must not silently wrap at u256 either"
    );
}

#[tokio::test]
async fn sum_of_three_hundred_digit_nines_is_exact() {
    let nines = "9".repeat(100);
    let expected = format!("2{}7", "9".repeat(99));
    let sm = session();
    register_single(&sm, "t", 120, 0, &rep(&nines, 3));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv(&expected)),
        "3 x (10^100 - 1) must be exact at 100+ digits"
    );
}

#[tokio::test]
async fn sum_of_ten_thousand_hundred_digit_values_is_exact() {
    let one_e99 = format!("1{}", "0".repeat(99));
    let expected = format!("1{}", "0".repeat(103)); // 10_000 * 10^99 = 10^103
    let sm = session();
    register_single(&sm, "t", 200, 0, &rep(&one_e99, 10_000));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv(&expected)),
        "accumulating 10k 100-digit values must stay exact"
    );
}

#[tokio::test]
async fn sum_of_powers_of_ten_forms_a_repunit() {
    let vals: Vec<Option<String>> = (0..40)
        .map(|i| Some(format!("1{}", "0".repeat(i))))
        .collect();
    let sm = session();
    register_single(&sm, "t", 60, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv(&"1".repeat(40))),
        "sum of 10^0..10^39 must be 40 ones"
    );
}

#[tokio::test]
async fn sum_of_the_first_ten_thousand_integers_matches_the_closed_form() {
    let vals: Vec<Option<String>> = (1..=10_000i64).map(|i| Some(i.to_string())).collect();
    let sm = session();
    register_single(&sm, "t", 30, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("50005000")),
        "n(n+1)/2 for n = 10000"
    );
}

#[tokio::test]
async fn sum_of_the_first_ten_thousand_integers_is_chunk_independent() {
    let vals: Vec<Option<String>> = (1..=10_000i64).map(|i| Some(i.to_string())).collect();
    let parts: Vec<Vec<Option<String>>> = vals.chunks(1_000).map(|c| c.to_vec()).collect();
    let sm = session();
    register_chunks(&sm, "t", 30, 0, &parts);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t GROUP BY g", 0).await,
        Some(dv("50005000")),
        "10 batches must produce the same total as one"
    );
}

#[tokio::test]
async fn sum_result_is_identical_for_one_and_for_five_chunks() {
    let vals: Vec<Option<String>> = (1..=5_000i64)
        .map(|i| Some(format!("{i}.{:02}", i % 100)))
        .collect();
    let sm_one = session();
    register_chunks(&sm_one, "t", 30, 2, std::slice::from_ref(&vals));
    let a = agg(&sm_one, "SELECT SUM(amt) AS r FROM t GROUP BY g", 2).await;

    let sm_many = session();
    let parts: Vec<Vec<Option<String>>> = vals.chunks(1_000).map(|c| c.to_vec()).collect();
    register_chunks(&sm_many, "t", 30, 2, &parts);
    let b = agg(&sm_many, "SELECT SUM(amt) AS r FROM t GROUP BY g", 2).await;

    assert_eq!(
        a, b,
        "SUM must be independent of how the input is split into batches"
    );
    assert!(a.is_some(), "SUM over 5000 non-null rows must not be NULL");
}

#[tokio::test]
async fn sum_of_a_scale_two_column_over_ten_thousand_rows_is_exact() {
    let sm = session();
    register_single(&sm, "t", 30, 2, &rep("0.01", 10_000));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 2).await,
        Some(dv("100.00")),
        "10000 x 0.01 must be exactly 100"
    );
}

#[tokio::test]
async fn sum_group_by_over_ten_thousand_rows_matches_per_group_totals() {
    let rows: Vec<(&str, Option<String>)> = (0..10_000)
        .map(|i| {
            if i % 2 == 0 {
                ("even", Some("2".to_string()))
            } else {
                ("odd", Some("3".to_string()))
            }
        })
        .collect();
    let sm = session();
    register_grouped(&sm, "gt", 30, 0, &rows);
    let b = sql_ok(&sm, "SELECT g, SUM(amt) AS r FROM gt GROUP BY g").await;
    let m = map_arb(&b, "g", "r", 0);
    assert_eq!(m.get("even"), Some(&Some(dv("10000"))));
    assert_eq!(m.get("odd"), Some(&Some(dv("15000"))));
}

// =====================================================================
// 2. SUM — sign cancellation, including swings outside declared precision
// =====================================================================

#[tokio::test]
async fn sum_of_ten_thousand_alternating_signs_cancels_to_exact_zero() {
    let big = "12345678901234567890.123456789012345678";
    let neg = format!("-{big}");
    let mut vals = Vec::with_capacity(10_000);
    for i in 0..10_000 {
        vals.push(Some(if i % 2 == 0 {
            big.to_string()
        } else {
            neg.clone()
        }));
    }
    let sm = session();
    register_single(&sm, "t", 60, 18, &vals);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 18).await,
        Some(dv("0")),
        "5000 + / 5000 - of the same magnitude must cancel to exactly zero"
    );
}

#[tokio::test]
async fn sum_that_cancels_to_zero_returns_zero_not_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["5.25", "-5.25"]));
    let r = agg(&sm, "SELECT SUM(amt) AS r FROM t", 2).await;
    assert!(
        r.is_some(),
        "SUM of rows that cancel must be 0, not NULL — NULL means `no rows`"
    );
    assert_eq!(r, Some(dv("0")));
}

#[tokio::test]
async fn sum_swings_past_the_declared_precision_and_cancels_back_into_range() {
    // decimal_arb(20, 0) holds at most 20 digits. Ten maxima push the running
    // sum to 21 digits; ten negatives bring it back to zero, which fits again.
    let nines = "9".repeat(20);
    let neg = format!("-{nines}");
    let mut vals = rep(&nines, 10);
    vals.extend(rep(&neg, 10));
    let sm = session();
    register_single(&sm, "t", 20, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("0")),
        "an intermediate sum wider than the declared precision must not be \
         clamped, truncated or rejected when the final total fits"
    );
}

#[tokio::test]
async fn sum_swings_past_the_declared_precision_and_settles_on_a_small_residue() {
    let nines = "9".repeat(20);
    let neg = format!("-{nines}");
    let mut vals = rep(&nines, 10);
    vals.extend(rep(&neg, 10));
    vals.push(Some("7".to_string()));
    let sm = session();
    register_single(&sm, "t", 20, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("7")),
        "the residue after cancellation must survive the wide intermediate"
    );
}

#[tokio::test]
async fn sum_cancels_when_the_positives_and_negatives_live_in_separate_batches() {
    let big = "9".repeat(30);
    let neg = format!("-{big}");
    let sm = session();
    register_chunks(&sm, "t", 40, 0, &[rep(&big, 5_000), rep(&neg, 5_000)]);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t GROUP BY g", 0).await,
        Some(dv("0")),
        "a positive-only batch followed by a negative-only batch must land on \
         exactly zero"
    );
}

#[tokio::test]
async fn sum_with_an_odd_number_of_alternating_values_leaves_one_value() {
    let mut vals = Vec::new();
    for i in 0..10_001 {
        vals.push(Some(if i % 2 == 0 {
            "1000000000000000000000000".to_string()
        } else {
            "-1000000000000000000000000".to_string()
        }));
    }
    let sm = session();
    register_single(&sm, "t", 40, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("1000000000000000000000000")),
        "10001 alternating values must leave exactly one un-cancelled value"
    );
}

#[tokio::test]
async fn sum_of_all_negative_values_stays_negative_and_exact() {
    let sm = session();
    register_single(&sm, "t", 30, 2, &rep("-1.25", 10_000));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 2).await,
        Some(dv("-12500.00"))
    );
}

#[tokio::test]
async fn sum_of_a_negative_and_a_larger_positive_flips_the_sign_correctly() {
    let sm = session();
    register_single(&sm, "t", 40, 0, &vs(&["-100000000000000000000", "1"]));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("-99999999999999999999")),
        "a borrow across the whole magnitude must be exact"
    );
}

#[tokio::test]
async fn sum_of_a_positive_and_a_larger_negative_flips_the_sign_correctly() {
    let sm = session();
    register_single(&sm, "t", 40, 0, &vs(&["1", "-100000000000000000000"]));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("-99999999999999999999"))
    );
}

#[tokio::test]
async fn sum_of_a_value_and_its_negation_at_scale_eighteen_is_exact_zero() {
    let sm = session();
    register_single(
        &sm,
        "t",
        40,
        18,
        &vs(&["0.000000000000000001", "-0.000000000000000001"]),
    );
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 18).await,
        Some(dv("0"))
    );
}

#[tokio::test]
async fn sum_of_a_zig_zag_that_never_repeats_a_magnitude_is_exact() {
    // +1 -2 +3 -4 ... +9999 -10000  ==>  -5000
    let vals: Vec<Option<String>> = (1..=10_000i64)
        .map(|i| {
            Some(if i % 2 == 1 {
                format!("{i}")
            } else {
                format!("-{i}")
            })
        })
        .collect();
    let sm = session();
    register_single(&sm, "t", 30, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("-5000"))
    );
}

#[tokio::test]
async fn sum_of_a_zig_zag_across_batches_matches_the_single_batch_result() {
    let vals: Vec<Option<String>> = (1..=10_000i64)
        .map(|i| {
            Some(if i % 2 == 1 {
                format!("{i}")
            } else {
                format!("-{i}")
            })
        })
        .collect();
    let parts: Vec<Vec<Option<String>>> = vals.chunks(2_500).map(|c| c.to_vec()).collect();
    let sm = session();
    register_chunks(&sm, "t", 30, 0, &parts);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t GROUP BY g", 0).await,
        Some(dv("-5000"))
    );
}

// =====================================================================
// 3. SUM — NULL semantics
// =====================================================================

#[tokio::test]
async fn sum_of_all_nulls_across_batches_is_null_not_zero() {
    let sm = session();
    register_chunks(
        &sm,
        "t",
        20,
        2,
        &[vec![None, None], vec![None], vec![None, None, None]],
    );
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t GROUP BY g", 2).await,
        None,
        "SUM of an all-NULL column must be NULL; returning 0 fabricates data"
    );
}

#[tokio::test]
async fn sum_of_a_single_null_row_is_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[None]);
    assert_eq!(agg(&sm, "SELECT SUM(amt) AS r FROM t", 2).await, None);
}

#[tokio::test]
async fn sum_of_null_and_zero_is_zero_not_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[None, Some("0".to_string())]);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 2).await,
        Some(dv("0")),
        "one real zero row makes the SUM non-NULL"
    );
}

#[tokio::test]
async fn an_all_null_batch_does_not_zero_out_the_other_batches() {
    let sm = session();
    register_chunks(
        &sm,
        "t",
        20,
        2,
        &[vec![None, None, None], vs(&["1.50", "2.50"]), vec![None]],
    );
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t GROUP BY g", 2).await,
        Some(dv("4.00")),
        "an all-NULL batch must be a no-op for the accumulator"
    );
}

#[tokio::test]
async fn sum_skips_every_other_null_row_over_ten_thousand_rows() {
    let vals: Vec<Option<String>> = (0..10_000)
        .map(|i| if i % 2 == 0 { Some("1".into()) } else { None })
        .collect();
    let sm = session();
    register_single(&sm, "t", 30, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("5000")),
        "NULLs must contribute nothing, not zero-widen the accumulator"
    );
}

#[tokio::test]
async fn sum_with_nulls_equals_the_sum_of_the_same_values_without_them() {
    let with_nulls: Vec<Option<String>> = vec![
        Some("1.10".into()),
        None,
        Some("2.20".into()),
        None,
        None,
        Some("3.30".into()),
    ];
    let without: Vec<Option<String>> = vs(&["1.10", "2.20", "3.30"]);
    let sm_a = session();
    register_single(&sm_a, "a", 20, 2, &with_nulls);
    let sm_b = session();
    register_single(&sm_b, "b", 20, 2, &without);
    assert_eq!(
        agg(&sm_a, "SELECT SUM(amt) AS r FROM a", 2).await,
        agg(&sm_b, "SELECT SUM(amt) AS r FROM b", 2).await,
        "NULL rows changed the SUM"
    );
}

#[tokio::test]
async fn sum_is_null_while_count_star_is_nonzero_for_an_all_null_column() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[None, None, None]);
    let b = sql_ok(
        &sm,
        "SELECT SUM(amt) AS r, COUNT(*) AS n, COUNT(amt) AS c FROM t",
    )
    .await;
    assert_eq!(one(&b, "r", 2), None, "SUM of all NULLs must be NULL");
    assert_eq!(
        i64_vals(&b, "n"),
        vec![Some(3)],
        "COUNT(*) counts NULL rows"
    );
    assert_eq!(
        i64_vals(&b, "c"),
        vec![Some(0)],
        "COUNT(col) must skip NULLs"
    );
}

#[tokio::test]
async fn sum_over_an_empty_input_is_null_not_zero() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["1.00"]));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t WHERE amt IS NULL", 2).await,
        None,
        "SUM over zero rows must be NULL"
    );
}

#[tokio::test]
async fn sum_of_a_single_zero_row_is_zero_not_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["0.00"]));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 2).await,
        Some(dv("0")),
        "a stored zero is a value, not an absence"
    );
}

// =====================================================================
// 4. AVG — scale widening and half-to-even rounding
// =====================================================================

#[tokio::test]
async fn avg_of_a_quarter_tie_rounds_down_to_even() {
    // (1+1+1+2)/4 = 1.25, widened scale 1, half-to-even -> 1.2
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["1", "1", "1", "2"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("1.2")),
        "AVG must round half-to-even, not half-up (1.25 -> 1.2)"
    );
}

#[tokio::test]
async fn avg_of_a_three_quarter_tie_rounds_up_to_even() {
    // (3+4+4+4)/4 = 3.75 -> 3.8
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["3", "4", "4", "4"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("3.8")),
        "3.75 must round to the even neighbour 3.8"
    );
}

#[tokio::test]
async fn avg_of_two_and_a_quarter_ties_down() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["2", "2", "2", "3"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("2.2"))
    );
}

#[tokio::test]
async fn avg_of_two_and_three_quarters_ties_up() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["2", "3", "3", "3"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("2.8"))
    );
}

#[tokio::test]
async fn avg_of_a_quarter_below_one_ties_down() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["0", "0", "0", "1"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("0.2")),
        "0.25 -> 0.2 under half-to-even"
    );
}

#[tokio::test]
async fn avg_of_three_quarters_below_one_ties_up() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["0", "1", "1", "1"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("0.8")),
        "0.75 -> 0.8 under half-to-even"
    );
}

#[tokio::test]
async fn avg_of_an_exact_half_needs_no_rounding_at_the_widened_scale() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["1", "2"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("1.5")),
        "the extra digit of scale exists precisely to hold this"
    );
}

#[tokio::test]
async fn avg_of_negative_quarter_tie_rounds_toward_even() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["-1", "-1", "-1", "-2"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("-1.2")),
        "-1.25 must round to -1.2, symmetric with the positive case"
    );
}

#[tokio::test]
async fn avg_of_negative_three_quarter_tie_rounds_toward_even() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["-3", "-4", "-4", "-4"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("-3.8"))
    );
}

#[tokio::test]
async fn avg_rounding_is_sign_symmetric() {
    let sm_p = session();
    register_single(&sm_p, "p", 20, 0, &vs(&["1", "1", "1", "2"]));
    let pos = agg(&sm_p, "SELECT AVG(amt) AS r FROM p", 1).await.unwrap();
    let sm_n = session();
    register_single(&sm_n, "n", 20, 0, &vs(&["-1", "-1", "-1", "-2"]));
    let neg = agg(&sm_n, "SELECT AVG(amt) AS r FROM n", 1).await.unwrap();
    assert_eq!(
        pos.to_canonical_string(),
        neg.to_canonical_string().trim_start_matches('-'),
        "AVG rounding must not be biased by sign"
    );
}

#[tokio::test]
async fn avg_of_one_third_truncates_toward_the_nearer_tenth() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["0", "0", "1"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("0.3")),
        "1/3 = 0.333... -> 0.3"
    );
}

#[tokio::test]
async fn avg_of_two_thirds_rounds_up() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["0", "1", "1"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("0.7")),
        "2/3 = 0.666... -> 0.7"
    );
}

#[tokio::test]
async fn avg_of_one_sixth_rounds_up() {
    let mut vals = vs(&["1"]);
    vals.extend(rep("0", 5));
    let sm = session();
    register_single(&sm, "t", 20, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("0.2")),
        "1/6 = 0.1666... -> 0.2"
    );
}

#[tokio::test]
async fn avg_of_five_sixths_rounds_down() {
    let mut vals = rep("1", 5);
    vals.push(Some("0".into()));
    let sm = session();
    register_single(&sm, "t", 20, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("0.8")),
        "5/6 = 0.8333... -> 0.8"
    );
}

#[tokio::test]
async fn avg_of_one_eighth_rounds_down() {
    let mut vals = vs(&["1"]);
    vals.extend(rep("0", 7));
    let sm = session();
    register_single(&sm, "t", 20, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("0.1")),
        "0.125 is below the 0.05 midpoint of the discarded tail -> 0.1"
    );
}

#[tokio::test]
async fn avg_of_three_eighths_rounds_up() {
    let mut vals = rep("1", 3);
    vals.extend(rep("0", 5));
    let sm = session();
    register_single(&sm, "t", 20, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("0.4")),
        "0.375 -> 0.4"
    );
}

#[tokio::test]
async fn avg_of_five_eighths_rounds_down() {
    let mut vals = rep("1", 5);
    vals.extend(rep("0", 3));
    let sm = session();
    register_single(&sm, "t", 20, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("0.6")),
        "0.625 -> 0.6"
    );
}

#[tokio::test]
async fn avg_of_seven_eighths_rounds_up() {
    let mut vals = rep("1", 7);
    vals.push(Some("0".into()));
    let sm = session();
    register_single(&sm, "t", 20, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("0.9")),
        "0.875 -> 0.9"
    );
}

#[tokio::test]
async fn avg_at_scale_two_widens_to_scale_three() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["0.01", "0.02"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await,
        Some(dv("0.015")),
        "the widened scale must carry the half-cent"
    );
}

#[tokio::test]
async fn avg_at_scale_two_ties_down_to_even() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["0.01", "0.01", "0.01", "0.02"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await,
        Some(dv("0.012")),
        "0.0125 -> 0.012 under half-to-even at scale 3"
    );
}

#[tokio::test]
async fn avg_at_scale_two_ties_up_to_even() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["0.01", "0.02", "0.02", "0.02"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await,
        Some(dv("0.018")),
        "0.0175 -> 0.018 under half-to-even at scale 3"
    );
}

#[tokio::test]
async fn avg_at_scale_two_of_a_half_cent_is_exact() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["0.00", "0.01"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await,
        Some(dv("0.005"))
    );
}

#[tokio::test]
async fn avg_at_scale_two_of_whole_amounts_is_exact() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["1.00", "2.00"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await,
        Some(dv("1.5"))
    );
}

#[tokio::test]
async fn avg_at_scale_two_of_a_third_rounds_at_scale_three() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["0.01", "0.00", "0.00"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await,
        Some(dv("0.003")),
        "0.01/3 = 0.00333... -> 0.003"
    );
}

#[tokio::test]
async fn avg_at_scale_two_of_two_thirds_rounds_up_at_scale_three() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["0.02", "0.00", "0.00"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await,
        Some(dv("0.007")),
        "0.02/3 = 0.00666... -> 0.007"
    );
}

#[tokio::test]
async fn avg_at_scale_eighteen_widens_to_scale_nineteen() {
    let sm = session();
    register_single(
        &sm,
        "t",
        40,
        18,
        &vs(&["0.000000000000000001", "0.000000000000000002"]),
    );
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 19).await,
        Some(dv("0.0000000000000000015")),
        "the 19th fractional digit must be materialised, not dropped"
    );
}

#[tokio::test]
async fn avg_at_scale_eighteen_ties_to_even_at_the_new_digit() {
    let sm = session();
    register_single(
        &sm,
        "t",
        40,
        18,
        &vs(&[
            "0.000000000000000001",
            "0.000000000000000001",
            "0.000000000000000001",
            "0.000000000000000002",
        ]),
    );
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 19).await,
        Some(dv("0.0000000000000000012")),
        "1.25e-18 -> 1.2e-18 under half-to-even"
    );
}

#[tokio::test]
async fn avg_ties_do_not_depend_on_row_order() {
    let sm_a = session();
    register_single(&sm_a, "a", 20, 0, &vs(&["1", "1", "1", "2"]));
    let sm_b = session();
    register_single(&sm_b, "b", 20, 0, &vs(&["2", "1", "1", "1"]));
    assert_eq!(
        agg(&sm_a, "SELECT AVG(amt) AS r FROM a", 1).await,
        agg(&sm_b, "SELECT AVG(amt) AS r FROM b", 1).await,
        "row order changed a half-to-even tie"
    );
}

#[tokio::test]
async fn avg_ties_do_not_depend_on_the_batch_split() {
    let sm_a = session();
    register_chunks(&sm_a, "a", 20, 0, &[vs(&["1", "1", "1", "2"])]);
    let sm_b = session();
    register_chunks(
        &sm_b,
        "b",
        20,
        0,
        &[vs(&["1"]), vs(&["1"]), vs(&["1"]), vs(&["2"])],
    );
    assert_eq!(
        agg(&sm_a, "SELECT AVG(amt) AS r FROM a GROUP BY g", 1).await,
        Some(dv("1.2"))
    );
    assert_eq!(
        agg(&sm_b, "SELECT AVG(amt) AS r FROM b GROUP BY g", 1).await,
        Some(dv("1.2")),
        "rounding must happen once on the final (sum, count), not per batch"
    );
}

#[tokio::test]
async fn avg_is_not_the_mean_of_per_batch_means() {
    // Batch means are 1 and 4; the true mean is (1+4+4+4)/4 = 3.25 -> 3.2.
    // Averaging the per-batch means would give 2.5.
    let sm = session();
    register_chunks(&sm, "t", 20, 0, &[vs(&["1"]), vs(&["4", "4", "4"])]);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t GROUP BY g", 1).await,
        Some(dv("3.2")),
        "AVG must accumulate (sum, count), not average per-batch averages"
    );
}

// =====================================================================
// 5. AVG — single row, zero, and NULL semantics
// =====================================================================

#[tokio::test]
async fn avg_of_a_single_row_is_that_row_at_the_widened_scale() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&["7"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("7")),
        "AVG of one row must be that row exactly"
    );
}

#[tokio::test]
async fn avg_of_a_single_fractional_row_is_that_row() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["-12.34"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await,
        Some(dv("-12.34"))
    );
}

#[tokio::test]
async fn avg_of_a_single_hundred_digit_row_is_that_row() {
    let big = format!("1{}", "2".repeat(104));
    let sm = session();
    register_single(&sm, "t", 200, 0, &vs(&[&big]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&big)),
        "dividing by one must never lose digits"
    );
}

#[tokio::test]
async fn avg_of_a_single_row_beside_nulls_is_that_row() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[None, Some("7".into()), None, None]);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("7")),
        "NULLs must not enter the denominator"
    );
}

#[tokio::test]
async fn avg_of_identical_rows_is_that_row() {
    let sm = session();
    register_single(&sm, "t", 30, 2, &rep("42.42", 10_000));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await,
        Some(dv("42.42")),
        "10k identical rows must average to themselves"
    );
}

#[tokio::test]
async fn avg_of_all_zeros_is_zero_not_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &rep("0.00", 5));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await,
        Some(dv("0")),
        "a mean of zero is a value, not an absence"
    );
}

#[tokio::test]
async fn avg_of_values_summing_to_zero_is_zero_not_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["-5.25", "5.25"]));
    let r = agg(&sm, "SELECT AVG(amt) AS r FROM t", 3).await;
    assert!(r.is_some(), "AVG must not collapse a zero mean to NULL");
    assert_eq!(r, Some(dv("0")));
}

#[tokio::test]
async fn avg_of_all_nulls_across_batches_is_null_not_zero() {
    let sm = session();
    register_chunks(&sm, "t", 20, 2, &[vec![None, None], vec![None], vec![None]]);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t GROUP BY g", 3).await,
        None,
        "AVG of an all-NULL column must be NULL; 0 would be a fabricated mean"
    );
}

#[tokio::test]
async fn avg_over_an_empty_input_is_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["1.00"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t WHERE amt IS NULL", 3).await,
        None
    );
}

#[tokio::test]
async fn avg_divides_by_the_non_null_count_only() {
    // 1 + 2 + 3 = 6 over 3 non-null rows -> 2, not 6/5 = 1.2.
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        0,
        &[
            Some("1".into()),
            None,
            Some("2".into()),
            None,
            Some("3".into()),
        ],
    );
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("2")),
        "NULL rows must not be counted in the denominator"
    );
}

#[tokio::test]
async fn avg_denominator_ignores_nulls_over_ten_thousand_rows() {
    // 5000 rows of 4 and 5000 NULLs -> 4, not 2.
    let vals: Vec<Option<String>> = (0..10_000)
        .map(|i| if i % 2 == 0 { Some("4".into()) } else { None })
        .collect();
    let sm = session();
    register_single(&sm, "t", 30, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("4")),
        "half the rows being NULL must not halve the mean"
    );
}

#[tokio::test]
async fn avg_with_an_entirely_null_batch_is_unaffected() {
    let sm = session();
    register_chunks(
        &sm,
        "t",
        20,
        0,
        &[vec![None, None, None], vs(&["2", "4"]), vec![None]],
    );
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t GROUP BY g", 1).await,
        Some(dv("3")),
        "an all-NULL batch must contribute neither sum nor count"
    );
}

#[tokio::test]
async fn avg_with_nulls_equals_avg_of_the_same_values_without_them() {
    let with_nulls = vec![
        Some("1".to_string()),
        None,
        Some("2".to_string()),
        None,
        Some("6".to_string()),
    ];
    let without = vs(&["1", "2", "6"]);
    let sm_a = session();
    register_single(&sm_a, "a", 20, 0, &with_nulls);
    let sm_b = session();
    register_single(&sm_b, "b", 20, 0, &without);
    assert_eq!(
        agg(&sm_a, "SELECT AVG(amt) AS r FROM a", 1).await,
        agg(&sm_b, "SELECT AVG(amt) AS r FROM b", 1).await
    );
}

#[tokio::test]
async fn avg_of_the_first_ten_thousand_integers_is_exact_at_the_widened_scale() {
    let vals: Vec<Option<String>> = (1..=10_000i64).map(|i| Some(i.to_string())).collect();
    let sm = session();
    register_single(&sm, "t", 30, 0, &vals);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("5000.5")),
        "the widened scale must hold the exact .5"
    );
}

#[tokio::test]
async fn avg_of_ten_thousand_rows_is_chunk_size_independent() {
    let vals: Vec<Option<String>> = (1..=10_000i64).map(|i| Some(i.to_string())).collect();
    let parts: Vec<Vec<Option<String>>> = vals.chunks(1_250).map(|c| c.to_vec()).collect();
    let sm = session();
    register_chunks(&sm, "t", 30, 0, &parts);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t GROUP BY g", 1).await,
        Some(dv("5000.5"))
    );
}

#[tokio::test]
async fn avg_of_ten_thousand_rows_is_batch_boundary_independent() {
    let vals: Vec<Option<String>> = (1..=10_000i64).map(|i| Some(i.to_string())).collect();
    let batches: Vec<Vec<Option<String>>> = vals.chunks(37).map(|c| c.to_vec()).collect();
    let sm = session();
    register_batches(&sm, "t", 30, 0, &batches);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv("5000.5")),
        "271 uneven record batches must not change the mean"
    );
}

#[tokio::test]
async fn avg_group_by_matches_the_per_group_means() {
    let rows: Vec<(&str, Option<String>)> = vec![
        ("a", Some("1".into())),
        ("a", Some("2".into())),
        ("b", Some("1".into())),
        ("b", Some("1".into())),
        ("b", Some("1".into())),
        ("b", Some("2".into())),
        ("c", None),
    ];
    let sm = session();
    register_grouped(&sm, "gt", 20, 0, &rows);
    let b = sql_ok(&sm, "SELECT g, AVG(amt) AS r FROM gt GROUP BY g").await;
    let m = map_arb(&b, "g", "r", 1);
    assert_eq!(m.get("a"), Some(&Some(dv("1.5"))));
    assert_eq!(
        m.get("b"),
        Some(&Some(dv("1.2"))),
        "1.25 must tie to even inside a group too"
    );
    assert_eq!(
        m.get("c"),
        Some(&None),
        "an all-NULL group averages to NULL"
    );
}

// =====================================================================
// 6. AVG — arbitrary-precision division
// =====================================================================

#[tokio::test]
async fn avg_of_an_exactly_divisible_hundred_digit_value_is_exact() {
    // sum = 3X, count = 3, X has 105 digits. The division terminates, so no
    // significant-digit cap can bite.
    let x = format!("1{}", "2".repeat(104));
    let sm = session();
    register_single(&sm, "t", 200, 0, &rep(&x, 3));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&x)),
        "AVG of three identical values must be that value"
    );
}

#[tokio::test]
async fn avg_of_ten_thousand_identical_hundred_digit_values_is_exact() {
    let x = format!("7{}", "3".repeat(99));
    let sm = session();
    register_single(&sm, "t", 200, 0, &rep(&x, 10_000));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&x))
    );
}

#[tokio::test]
async fn avg_with_a_ninety_eight_digit_quotient_keeps_the_fractional_digit() {
    // 10^98 / 3 = 33..3.333...  (98 integer digits). The quotient needs 99
    // significant digits, one under bigdecimal's 100-digit division cap, so
    // the widened scale digit still materialises.
    let a = format!("1{}", "0".repeat(98));
    let expected = format!("{}.3", "3".repeat(98));
    let sm = session();
    register_single(
        &sm,
        "t",
        200,
        0,
        &[Some(a), Some("0".into()), Some("0".into())],
    );
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&expected)),
        "the AVG's extra scale digit must be a real digit of the quotient"
    );
}

#[tokio::test]
#[ignore = "FINDING: AVG(decimal_arb) divides with bigdecimal's 100-significant-digit default, so once the quotient reaches 100 integer digits the widened fractional digit is silently emitted as 0 (10^100/3 returns ...3.0 instead of ...3.3)"]
async fn avg_with_a_hundred_digit_quotient_still_produces_the_fractional_digit() {
    let a = format!("1{}", "0".repeat(100));
    let expected = format!("{}.3", "3".repeat(100));
    let sm = session();
    register_single(
        &sm,
        "t",
        200,
        0,
        &[Some(a), Some("0".into()), Some("0".into())],
    );
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&expected)),
        "10^100 / 3 must be ...3.3 at the widened scale; a trailing .0 means the \
         division was capped at 100 significant digits and the fraction was lost"
    );
}

#[tokio::test]
#[ignore = "FINDING: same 100-digit division cap, rounding-up direction — 2*10^100/3 comes back rounded to a whole number instead of carrying the .7 the widened scale promises"]
async fn avg_with_a_hundred_digit_quotient_rounds_the_fraction_not_the_integer() {
    let a = format!("2{}", "0".repeat(100));
    let expected = format!("{}.7", "6".repeat(100));
    let sm = session();
    register_single(
        &sm,
        "t",
        200,
        0,
        &[Some(a), Some("0".into()), Some("0".into())],
    );
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&expected)),
        "2*10^100 / 3 = 66..6.666... must round to 66..6.7, not to the integer 66..67"
    );
}

#[tokio::test]
async fn avg_at_scale_eighteen_with_an_eighty_one_digit_integer_part_is_exact() {
    // 81 integer digits + 19 fractional = 100 significant digits: exactly at
    // the cap, so this still comes out right. Contrast with the next test.
    let a = format!("1{}", "0".repeat(81));
    let expected = format!("{}.{}", "3".repeat(81), "3".repeat(19));
    let sm = session();
    register_single(
        &sm,
        "t",
        140,
        18,
        &[Some(a), Some("0".into()), Some("0".into())],
    );
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 19).await,
        Some(dv(&expected))
    );
}

#[tokio::test]
#[ignore = "FINDING: AVG at scale 18 loses the widened 19th fractional digit once the integer part reaches 82 digits (100-significant-digit division cap) — for a decimal_arb(100, 18) token column this is a silently wrong mean"]
async fn avg_at_scale_eighteen_with_an_eighty_two_digit_integer_part_is_exact() {
    let a = format!("1{}", "0".repeat(82));
    let expected = format!("{}.{}", "3".repeat(82), "3".repeat(19));
    let sm = session();
    register_single(
        &sm,
        "t",
        140,
        18,
        &[Some(a), Some("0".into()), Some("0".into())],
    );
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 19).await,
        Some(dv(&expected)),
        "the 19th fractional digit must be 3, not a fabricated 0"
    );
}

#[tokio::test]
async fn sum_of_the_same_hundred_digit_input_is_exact_even_though_avg_is_not() {
    // Control: the SUM accumulator adds in exact BigDecimal arithmetic, so the
    // precision cap is specific to AVG's division step.
    let a = format!("1{}", "0".repeat(100));
    let sm = session();
    register_single(
        &sm,
        "t",
        200,
        0,
        &[Some(a.clone()), Some("0".into()), Some("0".into())],
    );
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv(&a)),
        "SUM must stay exact at 101 digits"
    );
}

#[tokio::test]
async fn avg_of_a_two_row_high_precision_pair_halves_exactly() {
    // Division by 2 always terminates, so high precision is safe here.
    let a = format!("1{}", "0".repeat(120));
    let expected = format!("5{}", "0".repeat(119));
    let sm = session();
    register_single(&sm, "t", 200, 0, &[Some(a), Some("0".into())]);
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&expected)),
        "halving a 121-digit value must stay exact"
    );
}

// =====================================================================
// 7. MIN / MAX — numeric order vs canonical byte order
// =====================================================================

#[tokio::test]
async fn min_prefers_the_smaller_number_over_the_lexicographically_smaller_bytes() {
    // 255 -> [00 FF]; 256 -> [00 01 00]. Bytewise 256 < 255.
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["255", "256"]));
    assert_eq!(
        agg(&sm, "SELECT MIN(amt) AS r FROM t", 0).await,
        Some(dv("255")),
        "MIN must compare numerically, not over the canonical byte encoding"
    );
}

#[tokio::test]
async fn max_prefers_the_larger_number_over_the_lexicographically_larger_bytes() {
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["255", "256"]));
    assert_eq!(
        agg(&sm, "SELECT MAX(amt) AS r FROM t", 0).await,
        Some(dv("256"))
    );
}

#[tokio::test]
async fn min_across_the_sign_boundary_picks_the_negative() {
    // -300 -> [FF 01 2C]; 5 -> [00 05]. Bytewise 5 < -300.
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["-300", "5"]));
    assert_eq!(
        agg(&sm, "SELECT MIN(amt) AS r FROM t", 0).await,
        Some(dv("-300")),
        "the 0xFF sign byte must not make negatives sort high"
    );
}

#[tokio::test]
async fn max_across_the_sign_boundary_picks_the_positive() {
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["-300", "5"]));
    assert_eq!(
        agg(&sm, "SELECT MAX(amt) AS r FROM t", 0).await,
        Some(dv("5"))
    );
}

#[tokio::test]
async fn min_among_negatives_of_different_magnitude_byte_lengths() {
    // -1 -> [FF 01]; -256 -> [FF 01 00]. Bytewise -1 sorts before -256.
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["-1", "-256"]));
    assert_eq!(
        agg(&sm, "SELECT MIN(amt) AS r FROM t", 0).await,
        Some(dv("-256")),
        "a longer negative magnitude is a *smaller* number"
    );
}

#[tokio::test]
async fn max_among_negatives_of_different_magnitude_byte_lengths() {
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["-1", "-256"]));
    assert_eq!(
        agg(&sm, "SELECT MAX(amt) AS r FROM t", 0).await,
        Some(dv("-1"))
    );
}

#[tokio::test]
async fn min_max_over_a_growing_byte_length_ladder() {
    let sm = session();
    register_single(
        &sm,
        "t",
        30,
        0,
        &vs(&["1", "256", "65536", "16777216", "4294967296"]),
    );
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 0), Some(dv("1")));
    assert_eq!(one(&b, "hi", 0), Some(dv("4294967296")));
}

#[tokio::test]
async fn min_max_over_a_negative_byte_length_ladder() {
    let sm = session();
    register_single(
        &sm,
        "t",
        30,
        0,
        &vs(&["-1", "-256", "-65536", "-16777216", "-4294967296"]),
    );
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 0), Some(dv("-4294967296")));
    assert_eq!(one(&b, "hi", 0), Some(dv("-1")));
}

#[tokio::test]
async fn min_max_of_equal_magnitude_opposite_signs() {
    let sm = session();
    register_single(&sm, "t", 10, 2, &vs(&["-7.25", "7.25"]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 2), Some(dv("-7.25")));
    assert_eq!(one(&b, "hi", 2), Some(dv("7.25")));
}

#[tokio::test]
async fn min_max_at_scale_four_distinguish_the_last_fractional_digit() {
    let sm = session();
    register_single(&sm, "t", 12, 4, &vs(&["1.5000", "1.4999", "1.5001"]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 4), Some(dv("1.4999")));
    assert_eq!(one(&b, "hi", 4), Some(dv("1.5001")));
}

#[tokio::test]
async fn min_max_treat_mixed_textual_scales_as_the_same_number() {
    // `1.5`, `1.50`, `1.500000` all encode to the same bytes at scale 4.
    let sm = session();
    register_single(&sm, "t", 12, 4, &vs(&["1.5", "1.50", "1.500000", "1.4999"]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 4), Some(dv("1.4999")));
    assert_eq!(
        one(&b, "hi", 4),
        Some(dv("1.5")),
        "textual scale must not affect the extreme"
    );
}

#[tokio::test]
async fn min_max_over_mixed_signs_and_scales() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        4,
        &vs(&["-0.0001", "0.0001", "-1000.0000", "999.9999", "0"]),
    );
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 4), Some(dv("-1000")));
    assert_eq!(one(&b, "hi", 4), Some(dv("999.9999")));
}

#[tokio::test]
async fn min_of_zero_and_negative_zero_and_signed_values() {
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["0", "-0", "3", "-3"]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 0), Some(dv("-3")));
    assert_eq!(one(&b, "hi", 0), Some(dv("3")));
}

#[tokio::test]
async fn min_max_when_zero_is_the_extreme() {
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["0", "5", "9"]));
    assert_eq!(
        agg(&sm, "SELECT MIN(amt) AS r FROM t", 0).await,
        Some(dv("0")),
        "zero's one-byte encoding must not be mistaken for absent"
    );
}

#[tokio::test]
async fn max_when_zero_is_the_largest_value() {
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["0", "-5", "-9"]));
    assert_eq!(
        agg(&sm, "SELECT MAX(amt) AS r FROM t", 0).await,
        Some(dv("0"))
    );
}

#[tokio::test]
async fn min_max_of_all_negative_values() {
    let sm = session();
    register_single(&sm, "t", 10, 0, &vs(&["-1", "-2", "-3"]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 0), Some(dv("-3")));
    assert_eq!(one(&b, "hi", 0), Some(dv("-1")));
}

#[tokio::test]
async fn min_max_of_hundred_digit_values_differing_in_the_last_digit() {
    let hi = "9".repeat(100);
    let lo = format!("{}8", "9".repeat(99));
    let sm = session();
    register_single(&sm, "t", 120, 0, &vs(&[&hi, &lo]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 0), Some(dv(&lo)));
    assert_eq!(one(&b, "hi", 0), Some(dv(&hi)));
}

#[tokio::test]
async fn min_max_of_negative_hundred_digit_values() {
    let a = format!("-{}", "9".repeat(100));
    let b_val = format!("-{}8", "9".repeat(99));
    let sm = session();
    register_single(&sm, "t", 120, 0, &vs(&[&a, &b_val]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 0), Some(dv(&a)));
    assert_eq!(one(&b, "hi", 0), Some(dv(&b_val)));
}

#[tokio::test]
async fn min_max_of_values_spanning_the_i128_boundary() {
    let sm = session();
    register_single(
        &sm,
        "t",
        60,
        0,
        &vs(&[
            "170141183460469231731687303715884105727",
            "170141183460469231731687303715884105728",
            "-170141183460469231731687303715884105728",
        ]),
    );
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(
        one(&b, "lo", 0),
        Some(dv("-170141183460469231731687303715884105728"))
    );
    assert_eq!(
        one(&b, "hi", 0),
        Some(dv("170141183460469231731687303715884105728"))
    );
}

#[tokio::test]
async fn min_max_find_extremes_planted_in_the_middle_of_ten_thousand_rows() {
    let mut vals = rep("0", 10_000);
    vals[4_321] = Some("-99999".into());
    vals[8_765] = Some("99999".into());
    let sm = session();
    register_single(&sm, "t", 20, 0, &vals);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 0), Some(dv("-99999")));
    assert_eq!(one(&b, "hi", 0), Some(dv("99999")));
}

#[tokio::test]
async fn min_max_find_extremes_that_live_in_the_last_batch() {
    let mut last = rep("0", 100);
    last.push(Some("-42".into()));
    last.push(Some("4200".into()));
    let sm = session();
    register_chunks(
        &sm,
        "t",
        20,
        0,
        &[rep("1", 100), rep("2", 100), rep("3", 100), last],
    );
    let b = sql_ok(
        &sm,
        "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t GROUP BY g",
    )
    .await;
    assert_eq!(
        one(&b, "lo", 0),
        Some(dv("-42")),
        "an extreme arriving in the last batch must survive"
    );
    assert_eq!(one(&b, "hi", 0), Some(dv("4200")));
}

#[tokio::test]
async fn min_max_across_batches_stay_numeric_not_bytewise() {
    // Each batch's own extreme is fine; combining them is where a bytewise
    // comparison would go wrong (255 vs 256, -1 vs -256).
    let sm = session();
    register_chunks(
        &sm,
        "t",
        20,
        0,
        &[vs(&["255"]), vs(&["256"]), vs(&["-1"]), vs(&["-256"])],
    );
    let b = sql_ok(
        &sm,
        "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t GROUP BY g",
    )
    .await;
    assert_eq!(one(&b, "lo", 0), Some(dv("-256")));
    assert_eq!(one(&b, "hi", 0), Some(dv("256")));
}

#[tokio::test]
async fn min_max_skip_nulls_across_batches() {
    let sm = session();
    register_chunks(
        &sm,
        "t",
        20,
        0,
        &[vec![None, None], vs(&["-9", "4"]), vec![None]],
    );
    let b = sql_ok(
        &sm,
        "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t GROUP BY g",
    )
    .await;
    assert_eq!(one(&b, "lo", 0), Some(dv("-9")));
    assert_eq!(one(&b, "hi", 0), Some(dv("4")));
}

#[tokio::test]
async fn min_max_of_all_nulls_across_batches_are_null() {
    let sm = session();
    register_chunks(&sm, "t", 20, 0, &[vec![None], vec![None, None]]);
    let b = sql_ok(
        &sm,
        "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t GROUP BY g",
    )
    .await;
    assert_eq!(one(&b, "lo", 0), None);
    assert_eq!(one(&b, "hi", 0), None);
}

#[tokio::test]
async fn min_and_max_of_a_single_row_are_that_row() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["-3.14"]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 2), Some(dv("-3.14")));
    assert_eq!(one(&b, "hi", 2), Some(dv("-3.14")));
}

#[tokio::test]
async fn min_and_max_of_repeated_identical_values_are_that_value() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &rep("4.00", 1_000));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 2), Some(dv("4")));
    assert_eq!(one(&b, "hi", 2), Some(dv("4")));
}

#[tokio::test]
async fn min_max_emit_at_the_input_scale_not_a_widened_one() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["1.00", "2.50"]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(
        one(&b, "lo", 2),
        Some(dv("1.00")),
        "MIN must keep the input scale"
    );
    assert_eq!(one(&b, "hi", 2), Some(dv("2.50")));
    // Decoding at a shifted scale must NOT accidentally agree — that would mean
    // the value was zero or the scale was ambiguous.
    assert_ne!(one(&b, "hi", 3), Some(dv("2.50")));
}

#[tokio::test]
async fn min_max_group_by_over_ten_thousand_rows() {
    let rows: Vec<(&str, Option<String>)> = (0..10_000)
        .map(|i| {
            if i % 2 == 0 {
                ("even", Some(format!("{}", i as i64 - 5_000)))
            } else {
                ("odd", Some(format!("{}", 5_000 - i as i64)))
            }
        })
        .collect();
    let sm = session();
    register_grouped(&sm, "gt", 20, 0, &rows);
    let b = sql_ok(
        &sm,
        "SELECT g, MIN(amt) AS lo, MAX(amt) AS hi FROM gt GROUP BY g",
    )
    .await;
    let lo = map_arb(&b, "g", "lo", 0);
    let hi = map_arb(&b, "g", "hi", 0);
    assert_eq!(lo.get("even"), Some(&Some(dv("-5000"))));
    assert_eq!(hi.get("even"), Some(&Some(dv("4998"))));
    assert_eq!(lo.get("odd"), Some(&Some(dv("-4999"))));
    assert_eq!(hi.get("odd"), Some(&Some(dv("4999"))));
}

#[tokio::test]
async fn min_max_with_nulls_equal_those_without_the_nulls() {
    let with_nulls = vec![
        None,
        Some("-2".to_string()),
        None,
        Some("9".to_string()),
        None,
    ];
    let without = vs(&["-2", "9"]);
    let sm_a = session();
    register_single(&sm_a, "a", 20, 0, &with_nulls);
    let sm_b = session();
    register_single(&sm_b, "b", 20, 0, &without);
    let a = sql_ok(&sm_a, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM a").await;
    let bb = sql_ok(&sm_b, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM b").await;
    assert_eq!(one(&a, "lo", 0), one(&bb, "lo", 0));
    assert_eq!(one(&a, "hi", 0), one(&bb, "hi", 0));
}

#[tokio::test]
async fn min_max_at_scale_eighteen_compare_the_smallest_units() {
    let sm = session();
    register_single(
        &sm,
        "t",
        40,
        18,
        &vs(&[
            "0.000000000000000001",
            "0.000000000000000002",
            "-0.000000000000000001",
        ]),
    );
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 18), Some(dv("-0.000000000000000001")));
    assert_eq!(one(&b, "hi", 18), Some(dv("0.000000000000000002")));
}

// =====================================================================
// 8. Cross-aggregate consistency
// =====================================================================

#[tokio::test]
async fn sum_equals_count_times_the_value_for_identical_rows() {
    let sm = session();
    register_single(&sm, "t", 30, 2, &rep("1.25", 10_000));
    let b = sql_ok(&sm, "SELECT SUM(amt) AS r, COUNT(amt) AS n FROM t").await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(10_000)]);
    assert_eq!(one(&b, "r", 2), Some(dv("12500.00")));
}

#[tokio::test]
async fn avg_times_count_reconstructs_the_sum_when_the_division_is_exact() {
    let sm = session();
    register_single(&sm, "t", 30, 0, &vs(&["2", "4", "6", "8"]));
    let b = sql_ok(
        &sm,
        "SELECT SUM(amt) AS s, AVG(amt) AS a, COUNT(amt) AS n FROM t",
    )
    .await;
    assert_eq!(one(&b, "s", 0), Some(dv("20")));
    assert_eq!(one(&b, "a", 1), Some(dv("5")));
    assert_eq!(i64_vals(&b, "n"), vec![Some(4)]);
}

#[tokio::test]
async fn min_is_at_most_avg_which_is_at_most_max() {
    let vals: Vec<Option<String>> = (1..=1_000i64)
        .map(|i| Some(format!("{}", i * 7 - 3_000)))
        .collect();
    let sm = session();
    register_single(&sm, "t", 30, 0, &vals);
    let b = sql_ok(
        &sm,
        "SELECT MIN(amt) AS lo, AVG(amt) AS a, MAX(amt) AS hi FROM t",
    )
    .await;
    let lo = one(&b, "lo", 0).unwrap();
    let a = one(&b, "a", 1).unwrap();
    let hi = one(&b, "hi", 0).unwrap();
    assert!(lo <= a, "MIN {lo} must be <= AVG {a}");
    assert!(a <= hi, "AVG {a} must be <= MAX {hi}");
}

#[tokio::test]
async fn the_sum_of_the_group_sums_equals_the_total_sum() {
    let rows: Vec<(&str, Option<String>)> = (0..3_000)
        .map(|i| {
            let g = ["a", "b", "c"][i % 3];
            (g, Some(format!("{}", i as i64 - 1_500)))
        })
        .collect();
    let sm = session();
    register_grouped(&sm, "gt", 30, 0, &rows);
    let total = sql_ok(&sm, "SELECT SUM(amt) AS r FROM gt").await;
    let per_group = sql_ok(&sm, "SELECT g, SUM(amt) AS r FROM gt GROUP BY g").await;
    let m = map_arb(&per_group, "g", "r", 0);
    // i in 0..3000, value = i - 1500, groups by i % 3:
    //   a: i = 0,3,..,2997 -> 1498500 - 1500000 = -1500
    //   b: i = 1,4,..,2998 -> 1499500 - 1500000 =  -500
    //   c: i = 2,5,..,2999 -> 1500500 - 1500000 =   500
    assert_eq!(m.get("a"), Some(&Some(dv("-1500"))));
    assert_eq!(m.get("b"), Some(&Some(dv("-500"))));
    assert_eq!(m.get("c"), Some(&Some(dv("500"))));
    assert_eq!(
        one(&total, "r", 0),
        Some(dv("-1500")),
        "group sums (-1500 + -500 + 500) must reconstruct the grand total exactly"
    );
}

#[tokio::test]
async fn sum_and_avg_agree_when_there_is_exactly_one_row() {
    let sm = session();
    register_single(&sm, "t", 30, 2, &vs(&["-987.65"]));
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s, AVG(amt) AS a FROM t").await;
    assert_eq!(one(&b, "s", 2), Some(dv("-987.65")));
    assert_eq!(one(&b, "a", 3), Some(dv("-987.65")));
}

#[tokio::test]
async fn all_four_aggregates_are_consistent_over_ten_thousand_rows() {
    let vals: Vec<Option<String>> = (0..10_000i64)
        .map(|i| Some((i - 5_000).to_string()))
        .collect();
    let sm = session();
    register_single(&sm, "t", 30, 0, &vals);
    let b = sql_ok(
        &sm,
        "SELECT SUM(amt) AS s, AVG(amt) AS a, MIN(amt) AS lo, MAX(amt) AS hi, COUNT(amt) AS n FROM t",
    )
    .await;
    // sum(i-5000 for i in 0..10000) = 49995000 - 50000000 = -5000
    assert_eq!(one(&b, "s", 0), Some(dv("-5000")));
    assert_eq!(one(&b, "a", 1), Some(dv("-0.5")));
    assert_eq!(one(&b, "lo", 0), Some(dv("-5000")));
    assert_eq!(one(&b, "hi", 0), Some(dv("4999")));
    assert_eq!(i64_vals(&b, "n"), vec![Some(10_000)]);
}

#[tokio::test]
async fn all_four_aggregates_are_null_over_an_all_null_column() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[None, None, None]);
    let b = sql_ok(
        &sm,
        "SELECT SUM(amt) AS s, AVG(amt) AS a, MIN(amt) AS lo, MAX(amt) AS hi FROM t",
    )
    .await;
    assert_eq!(one(&b, "s", 2), None, "SUM of all NULLs must be NULL");
    assert_eq!(one(&b, "a", 3), None, "AVG of all NULLs must be NULL");
    assert_eq!(one(&b, "lo", 2), None);
    assert_eq!(one(&b, "hi", 2), None);
}

#[tokio::test]
async fn aggregates_over_a_single_zero_row_are_all_zero_not_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &vs(&["0.00"]));
    let b = sql_ok(
        &sm,
        "SELECT SUM(amt) AS s, AVG(amt) AS a, MIN(amt) AS lo, MAX(amt) AS hi FROM t",
    )
    .await;
    assert_eq!(one(&b, "s", 2), Some(dv("0")));
    assert_eq!(one(&b, "a", 3), Some(dv("0")));
    assert_eq!(one(&b, "lo", 2), Some(dv("0")));
    assert_eq!(one(&b, "hi", 2), Some(dv("0")));
}

// =====================================================================
// 9. Precision-cap boundaries
// =====================================================================

#[tokio::test]
async fn sum_at_the_maximum_precision_reports_overflow_instead_of_wrapping() {
    // SUM widens by 16 digits but clamps at MAX_PRECISION (65535), so two
    // full-width values genuinely cannot be represented. Whatever happens, it
    // must not be a silent truncation or a panic.
    let nines = "9".repeat(65_535);
    let sm = session();
    register_single(&sm, "t", 65_535, 0, &rep(&nines, 2));
    let e = sql_err(&sm, "SELECT SUM(amt) AS r FROM t").await;
    let head: String = e.chars().take(200).collect();
    assert!(
        e.contains("integer digit") && e.contains("sum"),
        "SUM overflow at MAX_PRECISION must be an actionable user error naming \
         the aggregate and the digit budget, got: {head}"
    );
}

#[tokio::test]
async fn sum_of_one_row_at_the_maximum_precision_still_works() {
    let nines = "9".repeat(65_535);
    let sm = session();
    register_single(&sm, "t", 65_535, 0, &vs(&[&nines]));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv(&nines)),
        "a single-row SUM can never overflow"
    );
}

#[tokio::test]
#[ignore = "FINDING: AVG caps its output precision at MAX_PRECISION while still widening the scale by 1, so the integer-digit budget shrinks by one and AVG of a single full-width decimal_arb(65535, 0) row fails check_fits even though the mean equals the input"]
async fn avg_of_one_row_at_the_maximum_precision_cannot_overflow() {
    let nines = "9".repeat(65_535);
    let sm = session();
    register_single(&sm, "t", 65_535, 0, &vs(&[&nines]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&nines)),
        "the mean of one row is that row; it must fit whatever the declared \
         precision is"
    );
}

#[tokio::test]
async fn min_max_at_the_maximum_precision_round_trip() {
    let nines = "9".repeat(65_535);
    let neg = format!("-{nines}");
    let sm = session();
    register_single(&sm, "t", 65_535, 0, &vs(&[&nines, &neg, "0"]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 0), Some(dv(&neg)));
    assert_eq!(one(&b, "hi", 0), Some(dv(&nines)));
}

#[tokio::test]
async fn sum_of_sixteen_extra_digits_of_headroom_is_usable() {
    // decimal_arb(1, 0) widens to 17 digits for SUM. 10^5 rows would be needed
    // to exhaust it; verify the headroom at least admits a 5-digit total and
    // that nothing clamps at the input precision.
    let sm = session();
    register_single(&sm, "t", 1, 0, &rep("9", 11_111));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 0).await,
        Some(dv("99999")),
        "the SUM headroom must actually be usable"
    );
}

#[tokio::test]
async fn avg_output_precision_admits_the_widest_input_value() {
    // decimal_arb(20, 0) -> AVG(21, 1): a full-width 20-digit value plus one
    // extra fractional digit is exactly 21 digits, so this must fit.
    let nines = "9".repeat(20);
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&[&nines]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&nines))
    );
}

#[tokio::test]
async fn avg_of_two_full_width_values_stays_inside_the_widened_precision() {
    let nines = "9".repeat(20);
    let sm = session();
    register_single(&sm, "t", 20, 0, &rep(&nines, 2));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&nines)),
        "the mean of two equal values is that value"
    );
}

#[tokio::test]
async fn avg_of_two_full_width_values_one_apart_needs_the_extra_scale_digit() {
    let hi = "9".repeat(20);
    let lo = format!("{}8", "9".repeat(19));
    let expected = format!("{}8.5", "9".repeat(19));
    let sm = session();
    register_single(&sm, "t", 20, 0, &vs(&[&hi, &lo]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 1).await,
        Some(dv(&expected)),
        "the widened scale must be able to hold .5 next to a full-width integer"
    );
}

#[tokio::test]
async fn sum_at_a_scale_equal_to_the_precision_is_exact() {
    // decimal_arb(5, 5): values are pure fractions like 0.12345.
    let sm = session();
    register_single(&sm, "t", 5, 5, &vs(&["0.12345", "0.54321", "-0.00001"]));
    assert_eq!(
        agg(&sm, "SELECT SUM(amt) AS r FROM t", 5).await,
        Some(dv("0.66665"))
    );
}

#[tokio::test]
async fn avg_at_a_scale_equal_to_the_precision_widens_by_one_digit() {
    let sm = session();
    register_single(&sm, "t", 5, 5, &vs(&["0.00001", "0.00002"]));
    assert_eq!(
        agg(&sm, "SELECT AVG(amt) AS r FROM t", 6).await,
        Some(dv("0.000015")),
        "AVG(5, 5) -> (6, 6) must hold the extra fractional digit"
    );
}

#[tokio::test]
async fn min_max_at_a_scale_equal_to_the_precision() {
    let sm = session();
    register_single(&sm, "t", 5, 5, &vs(&["0.12345", "-0.54321", "0"]));
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(one(&b, "lo", 5), Some(dv("-0.54321")));
    assert_eq!(one(&b, "hi", 5), Some(dv("0.12345")));
}
