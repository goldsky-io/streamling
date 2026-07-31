//! Adversarial pass 03 — DF54 aggregation over `streamling.decimal_arb`.
//!
//! Focus: SUM / AVG / MIN / MAX / COUNT / COUNT(DISTINCT), GROUP BY (single
//! and composite keys), HAVING, `FILTER (WHERE ...)`, GROUPING SETS / ROLLUP /
//! CUBE, and window functions over `decimal_arb`.
//!
//! Everything runs in-process through `SessionManager`, which registers the
//! full decimal_arb stack (UDAFs, ExprPlanner, FunctionRewrite, sort rule).
//!
//! Assertion policy: aggregate *values* are decoded from the canonical byte
//! encoding at the scale the aggregate is specified to emit
//! (SUM/MIN/MAX -> input scale, AVG -> input scale + 1), and group-key /
//! pass-through columns additionally assert the `streamling.decimal_arb`
//! extension metadata survives.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, LargeBinaryArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::{ArrayFormatter, FormatOptions};
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

fn arb_array(name: &str, p: u32, s: u32, values: &[Option<&str>]) -> LargeBinaryArray {
    let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), name, p, s)
        .unwrap_or_else(|e| panic!("builder({name}, {p}, {s}): {e}"));
    for v in values {
        match v {
            Some(x) => b
                .append_value(
                    &DecimalArbValue::from_str(x).unwrap_or_else(|e| panic!("parse {x}: {e}")),
                )
                .unwrap_or_else(|e| panic!("append {x} to ({p},{s}): {e}")),
            None => b.append_null(),
        }
    }
    b.finish().into_inner().0
}

/// `t(amt decimal_arb(p, s))` — one partition.
fn register_single(sm: &SessionManager, table: &str, p: u32, s: u32, values: &[Option<&str>]) {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("amt", p, s, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arb_array("amt", p, s, values))],
    )
    .unwrap();
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
}

/// `t(amt decimal_arb(p, s))` split over several partitions, so DataFusion
/// runs a real two-phase (partial + final `merge_batch`) aggregation.
fn register_partitioned(
    sm: &SessionManager,
    table: &str,
    p: u32,
    s: u32,
    parts: &[&[Option<&str>]],
) {
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("amt", p, s, true).unwrap(),
    ]));
    let partitions: Vec<Vec<RecordBatch>> = parts
        .iter()
        .map(|vals| {
            vec![
                RecordBatch::try_new(schema.clone(), vec![Arc::new(arb_array("amt", p, s, vals))])
                    .unwrap(),
            ]
        })
        .collect();
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, partitions).unwrap()),
    )
    .unwrap();
}

/// `t(g Utf8, k Int64, amt decimal_arb(p, s))`.
fn register_grouped(
    sm: &SessionManager,
    table: &str,
    p: u32,
    s: u32,
    rows: &[(&str, i64, Option<&str>)],
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("k", DataType::Int64, false),
        DecimalArbType::field("amt", p, s, true).unwrap(),
    ]));
    let gs: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let ks: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let vs: Vec<Option<&str>> = rows.iter().map(|r| r.2).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(gs)),
            Arc::new(Int64Array::from(ks)),
            Arc::new(arb_array("amt", p, s, &vs)),
        ],
    )
    .unwrap();
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
}

/// `t(id Int64, a decimal_arb(p, s), b decimal_arb(p, s))` — composite keys.
fn register_two_arb(
    sm: &SessionManager,
    table: &str,
    p: u32,
    s: u32,
    rows: &[(i64, Option<&str>, Option<&str>)],
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("a", p, s, true).unwrap(),
        DecimalArbType::field("b", p, s, true).unwrap(),
    ]));
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let a: Vec<Option<&str>> = rows.iter().map(|r| r.1).collect();
    let b: Vec<Option<&str>> = rows.iter().map(|r| r.2).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(arb_array("a", p, s, &a)),
            Arc::new(arb_array("b", p, s, &b)),
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

fn expect(values: &[Option<&str>]) -> Vec<Option<DecimalArbValue>> {
    values
        .iter()
        .map(|v| v.map(|x| DecimalArbValue::from_str(x).unwrap()))
        .collect()
}

fn column_index(batches: &[RecordBatch], name: &str) -> usize {
    batches[0]
        .schema()
        .index_of(name)
        .unwrap_or_else(|_| panic!("no output column `{name}` in {:?}", batches[0].schema()))
}

/// Decode a `decimal_arb`-encoded output column at `scale`.
fn arb_vals(batches: &[RecordBatch], name: &str, scale: u32) -> Vec<Option<DecimalArbValue>> {
    let idx = column_index(batches, name);
    let mut out = Vec::new();
    for b in batches {
        let f = b.schema().field(idx).clone();
        assert_eq!(
            f.data_type(),
            &DataType::LargeBinary,
            "column `{name}` should be decimal_arb storage (LargeBinary), got {f:?}"
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

/// Integer aggregate columns: `COUNT`/`SUM` come back as `Int64`, ranking
/// window functions (`ROW_NUMBER`) as `UInt64`.
fn i64_vals(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let idx = column_index(batches, name);
    let mut out = Vec::new();
    for b in batches {
        let c = b.column(idx);
        if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
            for i in 0..a.len() {
                out.push(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
        } else if let Some(a) = c.as_any().downcast_ref::<arrow::array::UInt64Array>() {
            for i in 0..a.len() {
                out.push(if a.is_null(i) {
                    None
                } else {
                    Some(a.value(i) as i64)
                });
            }
        } else {
            panic!(
                "column `{name}` is not an integer column: {:?}",
                b.schema().field(idx).data_type()
            );
        }
    }
    out
}

/// Stringify a non-decimal_arb column (group labels) so results can be keyed
/// deterministically regardless of the order groups come back in.
fn text_vals(batches: &[RecordBatch], name: &str) -> Vec<String> {
    let idx = column_index(batches, name);
    let opts = FormatOptions::default().with_null("NULL");
    let mut out = Vec::new();
    for b in batches {
        let f = ArrayFormatter::try_new(b.column(idx).as_ref(), &opts).unwrap();
        for i in 0..b.num_rows() {
            out.push(f.value(i).to_string());
        }
    }
    out
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn output_field(batches: &[RecordBatch], name: &str) -> Field {
    batches[0]
        .schema()
        .field_with_name(name)
        .unwrap_or_else(|_| panic!("no output column `{name}`"))
        .clone()
}

/// `{group label -> decoded decimal_arb aggregate}`
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

/// `{group label -> Int64 aggregate}`
fn map_i64(batches: &[RecordBatch], gcol: &str, acol: &str) -> BTreeMap<String, Option<i64>> {
    let g = text_vals(batches, gcol);
    let a = i64_vals(batches, acol);
    assert_eq!(g.len(), a.len(), "group/agg column length mismatch");
    g.into_iter().zip(a).collect()
}

/// The standard grouped fixture, `decimal_arb(20, 2)`:
///
/// | g | k | amt    |               a: sum 30.50 min 10.00 max 20.50 avg 15.25 cnt 2/3
/// |---|---|--------|               b: sum  0.00 min -5.25 max  5.25 avg  0.00 cnt 2/2
/// | a | 1 |  10.00 |               c: sum NULL  min NULL  max NULL  avg NULL  cnt 0/2
/// | a | 2 |  20.50 |               d: sum 100.00 (single-row group)
/// | a | 3 |  NULL  |
/// | b | 4 |  -5.25 |
/// | b | 5 |   5.25 |
/// | c | 6 |  NULL  |
/// | c | 7 |  NULL  |
/// | d | 8 | 100.00 |
fn standard(sm: &SessionManager) {
    register_grouped(
        sm,
        "gt",
        20,
        2,
        &[
            ("a", 1, Some("10.00")),
            ("a", 2, Some("20.50")),
            ("a", 3, None),
            ("b", 4, Some("-5.25")),
            ("b", 5, Some("5.25")),
            ("c", 6, None),
            ("c", 7, None),
            ("d", 8, Some("100.00")),
        ],
    );
}

const SUM_SCALE: u32 = 2; // SUM preserves the input scale
const AVG_SCALE: u32 = 3; // AVG widens the input scale by one

// =====================================================================
// 1. SUM — values
// =====================================================================

#[tokio::test]
async fn sum_of_a_single_row_is_that_row() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("42.75")]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(rows(&b), 1, "scalar aggregate must emit exactly one row");
    assert_eq!(arb_vals(&b, "s", 2), expect(&[Some("42.75")]));
}

#[tokio::test]
async fn sum_over_an_empty_table_emits_one_null_row() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(
        rows(&b),
        1,
        "SUM over zero rows is one NULL row, not zero rows"
    );
    assert_eq!(arb_vals(&b, "s", 2), vec![None]);
}

#[tokio::test]
async fn sum_over_an_all_null_column_is_null_not_zero() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[None, None, None]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        vec![None],
        "SQL SUM of only NULLs is NULL, never 0"
    );
}

#[tokio::test]
async fn sum_skips_nulls_but_keeps_the_rest() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.25"), None, Some("2.75"), None]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(arb_vals(&b, "s", 2), expect(&[Some("4.00")]));
}

#[tokio::test]
async fn sum_of_opposite_signs_cancels_to_exact_zero() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("-5.25"), Some("5.25")]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("0")]),
        "cancelling sum must be zero (and must not encode as negative zero)"
    );
}

#[tokio::test]
async fn sum_of_all_negative_values_stays_negative() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        2,
        &[Some("-1.10"), Some("-2.20"), Some("-3.30")],
    );
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(arb_vals(&b, "s", 2), expect(&[Some("-6.60")]));
}

#[tokio::test]
async fn sum_of_values_far_beyond_i128_is_exact() {
    // 40-digit values: outside Decimal128 entirely.
    let sm = session();
    let a = "1234567890123456789012345678901234567890";
    let c = "9876543210987654321098765432109876543210";
    register_single(&sm, "t", 60, 0, &[Some(a), Some(c)]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(
        arb_vals(&b, "s", 0),
        expect(&[Some("11111111101111111110111111111011111111100")]),
        "arbitrary-precision SUM must not truncate to 128 bits"
    );
}

#[tokio::test]
async fn sum_preserves_the_declared_input_scale() {
    // SUM's contract is "widen precision by 16, keep scale". Decoding the
    // output at the input scale must therefore reproduce the exact total.
    let sm = session();
    register_single(&sm, "t", 30, 6, &[Some("0.000001"), Some("0.000002")]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(arb_vals(&b, "s", 6), expect(&[Some("0.000003")]));
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn sum_merges_partial_states_across_partitions() {
    let sm = session();
    register_partitioned(
        &sm,
        "t",
        20,
        2,
        &[
            &[Some("1.00"), Some("2.00")],
            &[Some("3.00"), None],
            &[Some("-4.00")],
        ],
    );
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("2.00")]),
        "two-phase (partial + merge_batch) SUM lost or double-counted rows"
    );
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn sum_merge_of_all_null_partitions_is_null() {
    let sm = session();
    register_partitioned(&sm, "t", 20, 2, &[&[None, None], &[None]]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(arb_vals(&b, "s", 2), vec![None]);
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn sum_merge_where_only_one_partition_has_data() {
    let sm = session();
    register_partitioned(&sm, "t", 20, 2, &[&[None], &[Some("7.77")], &[]]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(arb_vals(&b, "s", 2), expect(&[Some("7.77")]));
}

#[tokio::test]
async fn sum_group_by_single_key_matches_per_group_totals() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT g, SUM(amt) AS s FROM gt GROUP BY g").await;
    let got = map_arb(&b, "g", "s", SUM_SCALE);
    assert_eq!(got.len(), 4, "expected four groups, got {got:?}");
    assert_eq!(got["a"], Some(DecimalArbValue::from_str("30.50").unwrap()));
    assert_eq!(got["b"], Some(DecimalArbValue::from_str("0").unwrap()));
    assert_eq!(got["c"], None, "an all-NULL group must SUM to NULL");
    assert_eq!(got["d"], Some(DecimalArbValue::from_str("100.00").unwrap()));
}

#[tokio::test]
async fn sum_group_of_size_one_returns_the_row_itself() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, SUM(amt) AS s FROM gt WHERE g = 'd' GROUP BY g",
    )
    .await;
    assert_eq!(rows(&b), 1);
    assert_eq!(arb_vals(&b, "s", SUM_SCALE), expect(&[Some("100.00")]));
}

#[tokio::test]
async fn sum_group_by_over_empty_input_returns_zero_rows() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, SUM(amt) AS s FROM gt WHERE g = 'zzz' GROUP BY g",
    )
    .await;
    assert_eq!(
        rows(&b),
        0,
        "GROUP BY over an empty relation must produce no rows (unlike a bare scalar aggregate)"
    );
}

#[tokio::test]
async fn sum_group_by_the_decimal_arb_column_itself() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        2,
        &[Some("1.00"), Some("1.00"), Some("2.00"), Some("-1.00")],
    );
    let b = sql_ok(&sm, "SELECT amt, SUM(amt) AS s FROM t GROUP BY amt").await;
    let keys = arb_vals(&b, "amt", 2);
    let sums = arb_vals(&b, "s", 2);
    let mut got: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in keys.iter().zip(sums.iter()) {
        got.insert(
            k.as_ref().unwrap().to_canonical_string(),
            v.as_ref().unwrap().to_canonical_string(),
        );
    }
    assert_eq!(got.len(), 3, "expected 3 distinct keys: {got:?}");
    assert_eq!(got["1.00"], "2.00");
    assert_eq!(got["2.00"], "2.00");
    assert_eq!(got["-1.00"], "-1.00");
}

#[tokio::test]
async fn sum_of_one_hundred_rows_is_exact() {
    let sm = session();
    let vals: Vec<Option<&str>> = vec![Some("0.01"); 100];
    register_single(&sm, "t", 20, 2, &vals);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("1.00")]),
        "100 x 0.01 must be exactly 1.00 (no float drift)"
    );
}

/// `SUM(DISTINCT x)` is rewritten to an inner GROUP BY by DataFusion's
/// `SingleDistinctToGroupBy` rule, so the decimal_arb accumulator (which
/// ignores `AccumulatorArgs::is_distinct`) still gets the right answer.
#[tokio::test]
async fn sum_distinct_deduplicates_before_adding() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        2,
        &[Some("1.00"), Some("1.00"), Some("1.00"), Some("2.00")],
    );
    let b = sql_ok(&sm, "SELECT SUM(DISTINCT amt) AS s FROM t").await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("3.00")]),
        "SUM(DISTINCT) must add each distinct value once (1.00 + 2.00 = 3.00)"
    );
}

#[tokio::test]
async fn sum_distinct_differs_from_the_plain_sum() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        2,
        &[Some("1.00"), Some("1.00"), Some("1.00"), Some("2.00")],
    );
    let d = sql_ok(&sm, "SELECT SUM(DISTINCT amt) AS s FROM t").await;
    let n = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(arb_vals(&d, "s", 2), expect(&[Some("3.00")]));
    assert_eq!(arb_vals(&n, "s", 2), expect(&[Some("5.00")]));
    assert_ne!(
        arb_vals(&d, "s", 2),
        arb_vals(&n, "s", 2),
        "SUM(DISTINCT) must not silently collapse into the plain SUM"
    );
}

/// `SingleDistinctToGroupBy` only fires when *all* distinct aggregates share
/// one argument. With two different distinct arguments DataFusion must hand
/// `is_distinct = true` to the accumulator itself — and the decimal_arb
/// `SumAccumulator` never reads that flag.
#[tokio::test]
#[ignore = "FINDING: SUM(DISTINCT decimal_arb) alongside a second, differently-argued DISTINCT aggregate silently returns the non-distinct SUM (AccumulatorArgs::is_distinct is ignored)"]
async fn sum_distinct_still_deduplicates_beside_another_distinct_aggregate() {
    let sm = session();
    register_grouped(
        &sm,
        "dd",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 1, Some("1.00")),
            ("a", 2, Some("1.00")),
            ("a", 2, Some("2.00")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT SUM(DISTINCT amt) AS s, COUNT(DISTINCT k) AS n FROM dd",
    )
    .await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(2)]);
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("3.00")]),
        "SUM(DISTINCT amt) over {{1.00, 2.00}} is 3.00, not the 5.00 plain sum"
    );
}

#[tokio::test]
async fn sum_of_non_decimal_arb_column_still_uses_the_builtin() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT SUM(k) AS s FROM gt").await;
    assert_eq!(i64_vals(&b, "s"), vec![Some(36)]);
}

// =====================================================================
// 2. AVG
// =====================================================================

#[tokio::test]
async fn avg_of_a_single_row_is_that_row() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("42.75")]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(arb_vals(&b, "a", AVG_SCALE), expect(&[Some("42.75")]));
}

#[tokio::test]
async fn avg_over_an_empty_table_is_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(rows(&b), 1);
    assert_eq!(arb_vals(&b, "a", AVG_SCALE), vec![None]);
}

#[tokio::test]
async fn avg_of_all_nulls_is_null_not_zero() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[None, None]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(
        arb_vals(&b, "a", AVG_SCALE),
        vec![None],
        "count==0 must yield NULL, not 0/0"
    );
}

#[tokio::test]
async fn avg_divides_by_the_non_null_count_only() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00"), Some("3.00"), None]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(
        arb_vals(&b, "a", AVG_SCALE),
        expect(&[Some("2.00")]),
        "AVG must divide by 2 (non-null rows), not 3"
    );
}

#[tokio::test]
async fn avg_rounds_half_to_even_at_the_widened_scale() {
    // scale 0 input -> scale 1 output. (1 + 2) / 2 = 1.5 is exact at scale 1.
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("1"), Some("2")]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(arb_vals(&b, "a", 1), expect(&[Some("1.5")]));
}

#[tokio::test]
async fn avg_of_a_third_rounds_half_to_even_not_half_up() {
    // (1 + 1 + 1) / 3 = 1 exactly; (1 + 2) / 3 is the interesting case.
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("0"), Some("0"), Some("1")]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    // 1/3 = 0.333... -> scale 1 -> 0.3
    assert_eq!(arb_vals(&b, "a", 1), expect(&[Some("0.3")]));
}

#[tokio::test]
async fn avg_ties_round_to_even_downward() {
    // (0 + 1) / 2 = 0.5 at scale 0 input -> output scale 1 holds it exactly.
    // Use scale-0 output by averaging into a .x5 tie at the widened scale:
    // (0.05 + 0.10) / 2 = 0.075 -> scale 2+1=3 keeps 0.075 exactly.
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("0.05"), Some("0.10")]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(arb_vals(&b, "a", AVG_SCALE), expect(&[Some("0.075")]));
}

#[tokio::test]
async fn avg_of_negatives_stays_negative() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("-2.00"), Some("-4.00")]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(arb_vals(&b, "a", AVG_SCALE), expect(&[Some("-3.00")]));
}

#[tokio::test]
async fn avg_across_signs_can_be_zero() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("-5.25"), Some("5.25")]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(arb_vals(&b, "a", AVG_SCALE), expect(&[Some("0")]));
}

#[tokio::test]
async fn avg_group_by_matches_per_group_means() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT g, AVG(amt) AS a FROM gt GROUP BY g").await;
    let got = map_arb(&b, "g", "a", AVG_SCALE);
    assert_eq!(got["a"], Some(DecimalArbValue::from_str("15.25").unwrap()));
    assert_eq!(got["b"], Some(DecimalArbValue::from_str("0").unwrap()));
    assert_eq!(got["c"], None, "all-NULL group averages to NULL");
    assert_eq!(got["d"], Some(DecimalArbValue::from_str("100.00").unwrap()));
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn avg_merges_sum_and_count_state_across_partitions() {
    let sm = session();
    register_partitioned(
        &sm,
        "t",
        20,
        2,
        &[&[Some("1.00"), Some("2.00")], &[Some("3.00")], &[None]],
    );
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(
        arb_vals(&b, "a", AVG_SCALE),
        expect(&[Some("2.00")]),
        "AVG (sum, count) merge across partitions is wrong"
    );
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn avg_merge_where_one_partition_is_entirely_null() {
    let sm = session();
    register_partitioned(&sm, "t", 20, 2, &[&[None, None], &[Some("4.00")]]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(arb_vals(&b, "a", AVG_SCALE), expect(&[Some("4.00")]));
}

#[tokio::test]
async fn mean_alias_resolves_to_the_same_decimal_arb_avg() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00"), Some("3.00")]);
    let a = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    let m = sql_ok(&sm, "SELECT MEAN(amt) AS a FROM t").await;
    assert_eq!(
        arb_vals(&a, "a", AVG_SCALE),
        arb_vals(&m, "a", AVG_SCALE),
        "the MEAN alias must route to the decimal_arb AVG accumulator"
    );
}

#[tokio::test]
async fn avg_distinct_deduplicates_before_averaging() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        2,
        &[Some("1.00"), Some("1.00"), Some("1.00"), Some("3.00")],
    );
    let b = sql_ok(&sm, "SELECT AVG(DISTINCT amt) AS a FROM t").await;
    assert_eq!(
        arb_vals(&b, "a", AVG_SCALE),
        expect(&[Some("2.00")]),
        "AVG(DISTINCT) over {{1.00, 3.00}} is 2.00"
    );
}

#[tokio::test]
async fn avg_distinct_differs_from_the_plain_avg() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        2,
        &[Some("1.00"), Some("1.00"), Some("1.00"), Some("3.00")],
    );
    let d = sql_ok(&sm, "SELECT AVG(DISTINCT amt) AS a FROM t").await;
    let n = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(arb_vals(&d, "a", AVG_SCALE), expect(&[Some("2.00")]));
    assert_eq!(arb_vals(&n, "a", AVG_SCALE), expect(&[Some("1.50")]));
}

#[tokio::test]
#[ignore = "FINDING: AVG(decimal_arb) emits bytes at scale+1 but its output field carries no decimal_arb metadata, so a consumer reading the declared column scale is off by 10x"]
async fn avg_output_field_advertises_the_widened_scale() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00"), Some("2.00")]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    let f = output_field(&b, "a");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "AVG output must stay a decimal_arb field: {f:?}"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((21, 3)),
        "AVG widens (p, s) by one each; the output field must say so, otherwise \
         nothing downstream can decode the bytes correctly"
    );
}

/// Documents the concrete corruption the missing AVG metadata causes: the
/// bytes are at scale 3 while the only scale anyone can read off the plan is
/// the input's 2, which decodes to a value 10x too large.
#[tokio::test]
async fn avg_bytes_decoded_at_the_input_scale_are_ten_times_too_large() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00"), Some("3.00")]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(arb_vals(&b, "a", AVG_SCALE), expect(&[Some("2.00")]));
    assert_eq!(
        arb_vals(&b, "a", SUM_SCALE),
        expect(&[Some("20.00")]),
        "if this no longer reproduces, AVG stopped shifting the scale silently"
    );
}

// =====================================================================
// 3. MIN / MAX
// =====================================================================

#[tokio::test]
async fn min_of_a_single_row_is_that_row() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("-9.99")]);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS m FROM t").await;
    assert_eq!(arb_vals(&b, "m", 2), expect(&[Some("-9.99")]));
}

#[tokio::test]
async fn max_of_a_single_row_is_that_row() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("-9.99")]);
    let b = sql_ok(&sm, "SELECT MAX(amt) AS m FROM t").await;
    assert_eq!(arb_vals(&b, "m", 2), expect(&[Some("-9.99")]));
}

#[tokio::test]
async fn min_over_empty_is_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[]);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS m FROM t").await;
    assert_eq!(arb_vals(&b, "m", 2), vec![None]);
}

#[tokio::test]
async fn max_over_all_nulls_is_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[None, None]);
    let b = sql_ok(&sm, "SELECT MAX(amt) AS m FROM t").await;
    assert_eq!(arb_vals(&b, "m", 2), vec![None]);
}

#[tokio::test]
async fn min_picks_the_most_negative_not_the_smallest_bytes() {
    // Bytewise, sign byte 0xFF sorts *after* 0x00, so a byte-order MIN would
    // return 0 (or the smallest positive). Numeric MIN must be -100.
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        0,
        &[Some("100"), Some("0"), Some("-100"), Some("-1")],
    );
    let b = sql_ok(&sm, "SELECT MIN(amt) AS m FROM t").await;
    assert_eq!(
        arb_vals(&b, "m", 0),
        expect(&[Some("-100")]),
        "MIN over decimal_arb must be numeric, not bytewise over the canonical encoding"
    );
}

#[tokio::test]
async fn max_picks_the_largest_number_not_the_largest_bytes() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        0,
        &[Some("100"), Some("0"), Some("-100"), Some("-1")],
    );
    let b = sql_ok(&sm, "SELECT MAX(amt) AS m FROM t").await;
    assert_eq!(arb_vals(&b, "m", 0), expect(&[Some("100")]));
}

#[tokio::test]
async fn max_compares_across_magnitude_byte_lengths() {
    // 255 encodes as [0x00, 0xFF]; 256 as [0x00, 0x01, 0x00]. A bytewise
    // compare would call 255 larger than 256.
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("255"), Some("256")]);
    let b = sql_ok(&sm, "SELECT MAX(amt) AS m FROM t").await;
    assert_eq!(
        arb_vals(&b, "m", 0),
        expect(&[Some("256")]),
        "256 > 255 even though its leading magnitude byte is smaller"
    );
}

#[tokio::test]
async fn min_compares_across_magnitude_byte_lengths() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("255"), Some("256")]);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS m FROM t").await;
    assert_eq!(arb_vals(&b, "m", 0), expect(&[Some("255")]));
}

#[tokio::test]
async fn min_among_negatives_of_different_byte_lengths() {
    // -255 -> [0xFF, 0xFF]; -256 -> [0xFF, 0x01, 0x00]. Numeric min is -256.
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("-255"), Some("-256")]);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS m FROM t").await;
    assert_eq!(arb_vals(&b, "m", 0), expect(&[Some("-256")]));
}

#[tokio::test]
async fn min_max_skip_nulls() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[None, Some("3.00"), None, Some("-3.00")]);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(arb_vals(&b, "lo", 2), expect(&[Some("-3.00")]));
    assert_eq!(arb_vals(&b, "hi", 2), expect(&[Some("3.00")]));
}

#[tokio::test]
async fn min_max_group_by_matches_per_group_extremes() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, MIN(amt) AS lo, MAX(amt) AS hi FROM gt GROUP BY g",
    )
    .await;
    let lo = map_arb(&b, "g", "lo", 2);
    let hi = map_arb(&b, "g", "hi", 2);
    assert_eq!(lo["a"], Some(DecimalArbValue::from_str("10.00").unwrap()));
    assert_eq!(hi["a"], Some(DecimalArbValue::from_str("20.50").unwrap()));
    assert_eq!(lo["b"], Some(DecimalArbValue::from_str("-5.25").unwrap()));
    assert_eq!(hi["b"], Some(DecimalArbValue::from_str("5.25").unwrap()));
    assert_eq!(lo["c"], None);
    assert_eq!(hi["c"], None);
    assert_eq!(lo["d"], Some(DecimalArbValue::from_str("100.00").unwrap()));
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn min_max_merge_across_partitions_stays_numeric() {
    let sm = session();
    register_partitioned(
        &sm,
        "t",
        20,
        0,
        &[&[Some("100")], &[Some("-100")], &[Some("0")], &[None]],
    );
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(
        arb_vals(&b, "lo", 0),
        expect(&[Some("-100")]),
        "the cross-partition MIN merge must not fall back to bytewise compare"
    );
    assert_eq!(arb_vals(&b, "hi", 0), expect(&[Some("100")]));
}

#[tokio::test]
async fn min_distinct_is_a_no_op() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("5"), Some("5"), Some("-2")]);
    let b = sql_ok(&sm, "SELECT MIN(DISTINCT amt) AS m FROM t").await;
    assert_eq!(arb_vals(&b, "m", 0), expect(&[Some("-2")]));
}

#[tokio::test]
async fn max_distinct_is_a_no_op() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("5"), Some("5"), Some("-2")]);
    let b = sql_ok(&sm, "SELECT MAX(DISTINCT amt) AS m FROM t").await;
    assert_eq!(arb_vals(&b, "m", 0), expect(&[Some("5")]));
}

#[tokio::test]
async fn min_max_over_huge_precision_values() {
    let sm = session();
    let big = "99999999999999999999999999999999999999999999999999";
    let neg = "-99999999999999999999999999999999999999999999999999";
    register_single(&sm, "t", 60, 0, &[Some(big), Some(neg), Some("0")]);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(arb_vals(&b, "lo", 0), expect(&[Some(neg)]));
    assert_eq!(arb_vals(&b, "hi", 0), expect(&[Some(big)]));
}

#[tokio::test]
async fn min_of_fractional_values_uses_full_scale() {
    let sm = session();
    register_single(
        &sm,
        "t",
        30,
        6,
        &[Some("0.000002"), Some("0.000001"), Some("0.000010")],
    );
    let b = sql_ok(&sm, "SELECT MIN(amt) AS m FROM t").await;
    assert_eq!(arb_vals(&b, "m", 6), expect(&[Some("0.000001")]));
}

#[tokio::test]
async fn min_max_of_non_decimal_arb_column_still_uses_the_builtin() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT MIN(k) AS lo, MAX(k) AS hi FROM gt").await;
    assert_eq!(i64_vals(&b, "lo"), vec![Some(1)]);
    assert_eq!(i64_vals(&b, "hi"), vec![Some(8)]);
}

// =====================================================================
// 4. COUNT / COUNT(DISTINCT)
// =====================================================================

#[tokio::test]
async fn count_star_over_empty_is_zero() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[]);
    let b = sql_ok(&sm, "SELECT COUNT(*) AS n FROM t").await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(0)]);
}

#[tokio::test]
async fn count_column_over_empty_is_zero() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[]);
    let b = sql_ok(&sm, "SELECT COUNT(amt) AS n FROM t").await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(0)]);
}

#[tokio::test]
async fn count_star_counts_null_rows_but_count_column_does_not() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00"), None, None]);
    let b = sql_ok(&sm, "SELECT COUNT(*) AS n, COUNT(amt) AS c FROM t").await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(3)]);
    assert_eq!(i64_vals(&b, "c"), vec![Some(1)]);
}

#[tokio::test]
async fn count_returns_int64_not_decimal_arb() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00")]);
    let b = sql_ok(&sm, "SELECT COUNT(amt) AS n FROM t").await;
    let f = output_field(&b, "n");
    assert_eq!(f.data_type(), &DataType::Int64);
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "COUNT must not inherit decimal_arb metadata from its argument: {f:?}"
    );
}

#[tokio::test]
async fn count_distinct_collapses_numerically_equal_textual_forms() {
    let sm = session();
    // "5", "5.0", "05", "+5" all canonicalize to the same encoded value.
    register_single(
        &sm,
        "t",
        20,
        0,
        &[Some("5"), Some("5.0"), Some("05"), Some("+5"), Some("7")],
    );
    let b = sql_ok(&sm, "SELECT COUNT(DISTINCT amt) AS n FROM t").await;
    assert_eq!(
        i64_vals(&b, "n"),
        vec![Some(2)],
        "COUNT(DISTINCT) must see 5/5.0/05/+5 as one value"
    );
}

#[tokio::test]
async fn count_distinct_treats_plus_and_minus_zero_as_one_value() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("0"), Some("-0"), Some("0.0")]);
    let b = sql_ok(&sm, "SELECT COUNT(DISTINCT amt) AS n FROM t").await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(1)]);
}

#[tokio::test]
async fn count_distinct_ignores_nulls() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[None, Some("1"), None, Some("1")]);
    let b = sql_ok(&sm, "SELECT COUNT(DISTINCT amt) AS n FROM t").await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(1)]);
}

#[tokio::test]
async fn count_distinct_over_all_nulls_is_zero() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[None, None]);
    let b = sql_ok(&sm, "SELECT COUNT(DISTINCT amt) AS n FROM t").await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(0)]);
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn count_distinct_across_partitions_is_deduplicated() {
    let sm = session();
    register_partitioned(
        &sm,
        "t",
        20,
        0,
        &[
            &[Some("1"), Some("2")],
            &[Some("2"), Some("3")],
            &[Some("1")],
        ],
    );
    let b = sql_ok(&sm, "SELECT COUNT(DISTINCT amt) AS n FROM t").await;
    assert_eq!(
        i64_vals(&b, "n"),
        vec![Some(3)],
        "cross-partition COUNT(DISTINCT) must dedupe {{1,2,3}}"
    );
}

#[tokio::test]
async fn count_group_by_counts_per_group() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, COUNT(*) AS n, COUNT(amt) AS c FROM gt GROUP BY g",
    )
    .await;
    let n = map_i64(&b, "g", "n");
    let c = map_i64(&b, "g", "c");
    assert_eq!(n["a"], Some(3));
    assert_eq!(c["a"], Some(2));
    assert_eq!(n["c"], Some(2));
    assert_eq!(
        c["c"],
        Some(0),
        "COUNT(col) over an all-NULL group is 0, not NULL"
    );
    assert_eq!(n["d"], Some(1));
    assert_eq!(c["d"], Some(1));
}

#[tokio::test]
async fn count_distinct_group_by_counts_per_group() {
    let sm = session();
    register_grouped(
        &sm,
        "gt2",
        20,
        0,
        &[
            ("a", 1, Some("1")),
            ("a", 2, Some("1.0")),
            ("a", 3, Some("2")),
            ("b", 4, Some("9")),
            ("b", 5, None),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT g, COUNT(DISTINCT amt) AS n FROM gt2 GROUP BY g",
    )
    .await;
    let n = map_i64(&b, "g", "n");
    assert_eq!(n["a"], Some(2), "1 and 1.0 are the same value");
    assert_eq!(n["b"], Some(1));
}

#[tokio::test]
async fn count_star_and_sum_agree_on_group_cardinality() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT COUNT(*) AS n FROM gt").await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(8)]);
}

// =====================================================================
// 5. GROUP BY semantics
// =====================================================================

#[tokio::test]
async fn group_by_decimal_arb_key_keeps_extension_metadata_on_the_output_field() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00"), Some("1.00"), Some("2.00")]);
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    let f = output_field(&b, "amt");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "the GROUP BY key column must remain a decimal_arb field: {f:?}"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((20, 2)),
        "the group key must carry the source column's (precision, scale)"
    );
}

#[tokio::test]
async fn group_by_null_key_forms_its_own_group() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("1"), None, None, Some("2")]);
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_eq!(rows(&b), 3, "NULL must form exactly one extra group");
    let keys = arb_vals(&b, "amt", 0);
    let ns = i64_vals(&b, "n");
    let null_pos = keys
        .iter()
        .position(|k| k.is_none())
        .expect("a NULL group key row must be present");
    assert_eq!(
        ns[null_pos],
        Some(2),
        "both NULL rows must land in the single NULL group"
    );
}

#[tokio::test]
async fn group_by_collapses_numerically_equal_keys_at_nonzero_scale() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        4,
        &[Some("5"), Some("5.0"), Some("5.0000"), Some("05.00")],
    );
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_eq!(
        rows(&b),
        1,
        "5 / 5.0 / 5.0000 / 05.00 must be one group at scale 4"
    );
    assert_eq!(i64_vals(&b, "n"), vec![Some(4)]);
    assert_eq!(arb_vals(&b, "amt", 4), expect(&[Some("5")]));
}

#[tokio::test]
async fn group_by_separates_negative_and_positive_keys_of_the_same_magnitude() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("7"), Some("-7"), Some("7")]);
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_eq!(rows(&b), 2, "7 and -7 are different groups");
    let mut got: BTreeMap<String, i64> = BTreeMap::new();
    for (k, n) in arb_vals(&b, "amt", 0).iter().zip(i64_vals(&b, "n")) {
        got.insert(k.as_ref().unwrap().to_canonical_string(), n.unwrap());
    }
    assert_eq!(got["7"], 2);
    assert_eq!(got["-7"], 1);
}

#[tokio::test]
async fn group_by_composite_key_of_two_decimal_arb_columns() {
    let sm = session();
    register_two_arb(
        &sm,
        "t2",
        20,
        2,
        &[
            (1, Some("1.00"), Some("2.00")),
            (2, Some("1.0"), Some("2.000")),
            (3, Some("1.00"), Some("3.00")),
            (4, Some("-1.00"), Some("2.00")),
        ],
    );
    let b = sql_ok(&sm, "SELECT a, b, COUNT(*) AS n FROM t2 GROUP BY a, b").await;
    assert_eq!(
        rows(&b),
        3,
        "(1,2) must merge across textual forms, leaving (1,2), (1,3), (-1,2)"
    );
    let a = arb_vals(&b, "a", 2);
    let bb = arb_vals(&b, "b", 2);
    let n = i64_vals(&b, "n");
    let mut got: BTreeMap<(String, String), i64> = BTreeMap::new();
    for i in 0..a.len() {
        got.insert(
            (
                a[i].as_ref().unwrap().to_canonical_string(),
                bb[i].as_ref().unwrap().to_canonical_string(),
            ),
            n[i].unwrap(),
        );
    }
    assert_eq!(got[&("1.00".to_string(), "2.00".to_string())], 2);
    assert_eq!(got[&("1.00".to_string(), "3.00".to_string())], 1);
    assert_eq!(got[&("-1.00".to_string(), "2.00".to_string())], 1);
}

#[tokio::test]
async fn group_by_composite_key_mixing_utf8_and_decimal_arb() {
    let sm = session();
    register_grouped(
        &sm,
        "gt3",
        20,
        0,
        &[
            ("a", 1, Some("1")),
            ("a", 2, Some("1.0")),
            ("a", 3, Some("2")),
            ("b", 4, Some("1")),
        ],
    );
    let b = sql_ok(&sm, "SELECT g, amt, COUNT(*) AS n FROM gt3 GROUP BY g, amt").await;
    assert_eq!(rows(&b), 3, "(a,1), (a,2), (b,1)");
    let g = text_vals(&b, "g");
    let amt = arb_vals(&b, "amt", 0);
    let n = i64_vals(&b, "n");
    let mut got: BTreeMap<(String, String), i64> = BTreeMap::new();
    for i in 0..g.len() {
        got.insert(
            (g[i].clone(), amt[i].as_ref().unwrap().to_canonical_string()),
            n[i].unwrap(),
        );
    }
    assert_eq!(got[&("a".to_string(), "1".to_string())], 2);
    assert_eq!(got[&("a".to_string(), "2".to_string())], 1);
    assert_eq!(got[&("b".to_string(), "1".to_string())], 1);
}

#[tokio::test]
async fn group_by_composite_key_with_a_null_component() {
    let sm = session();
    register_two_arb(
        &sm,
        "t2",
        20,
        0,
        &[
            (1, Some("1"), None),
            (2, Some("1"), None),
            (3, Some("1"), Some("0")),
        ],
    );
    let b = sql_ok(&sm, "SELECT a, b, COUNT(*) AS n FROM t2 GROUP BY a, b").await;
    assert_eq!(rows(&b), 2, "(1, NULL) and (1, 0) are distinct groups");
    let bb = arb_vals(&b, "b", 0);
    let n = i64_vals(&b, "n");
    let null_pos = bb.iter().position(|v| v.is_none()).expect("NULL b group");
    assert_eq!(n[null_pos], Some(2));
}

#[tokio::test]
async fn group_by_all_singleton_groups() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        0,
        &[Some("1"), Some("2"), Some("3"), Some("4")],
    );
    let b = sql_ok(&sm, "SELECT amt, SUM(amt) AS s FROM t GROUP BY amt").await;
    assert_eq!(rows(&b), 4);
    for (k, s) in arb_vals(&b, "amt", 0).iter().zip(arb_vals(&b, "s", 0)) {
        assert_eq!(
            k.as_ref(),
            s.as_ref(),
            "in a singleton group SUM(amt) must equal the key"
        );
    }
}

#[tokio::test]
async fn group_by_one_giant_group() {
    let sm = session();
    let vals: Vec<Option<&str>> = [Some("3")].repeat(500);
    register_single(&sm, "t", 20, 0, &vals);
    let b = sql_ok(
        &sm,
        "SELECT amt, COUNT(*) AS n, SUM(amt) AS s FROM t GROUP BY amt",
    )
    .await;
    assert_eq!(rows(&b), 1);
    assert_eq!(i64_vals(&b, "n"), vec![Some(500)]);
    assert_eq!(arb_vals(&b, "s", 0), expect(&[Some("1500")]));
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn group_by_a_decimal_arb_key_across_partitions_merges_groups() {
    let sm = session();
    register_partitioned(
        &sm,
        "t",
        20,
        0,
        &[
            &[Some("1"), Some("2")],
            &[Some("1")],
            &[Some("2"), Some("1")],
        ],
    );
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_eq!(
        rows(&b),
        2,
        "the same key in different partitions is one group"
    );
    let mut got: BTreeMap<String, i64> = BTreeMap::new();
    for (k, n) in arb_vals(&b, "amt", 0).iter().zip(i64_vals(&b, "n")) {
        got.insert(k.as_ref().unwrap().to_canonical_string(), n.unwrap());
    }
    assert_eq!(got["1"], 3);
    assert_eq!(got["2"], 2);
}

#[tokio::test]
async fn group_by_key_values_round_trip_exactly() {
    let sm = session();
    let big = "123456789012345678901234567890.12345678";
    register_single(&sm, "t", 60, 8, &[Some(big), Some(big)]);
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_eq!(rows(&b), 1);
    assert_eq!(arb_vals(&b, "amt", 8), expect(&[Some(big)]));
}

#[tokio::test]
async fn group_by_with_where_filter_applies_before_aggregation() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, COUNT(*) AS n FROM gt WHERE amt IS NOT NULL GROUP BY g",
    )
    .await;
    let n = map_i64(&b, "g", "n");
    assert_eq!(n["a"], Some(2));
    assert_eq!(n["b"], Some(2));
    assert_eq!(n["d"], Some(1));
    assert!(
        !n.contains_key("c"),
        "group c has no non-NULL rows and must disappear entirely: {n:?}"
    );
}

#[tokio::test]
async fn group_by_with_a_decimal_arb_predicate_on_the_key() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, SUM(amt) AS s FROM gt WHERE amt > 0 GROUP BY g",
    )
    .await;
    let s = map_arb(&b, "g", "s", SUM_SCALE);
    assert_eq!(s["a"], Some(DecimalArbValue::from_str("30.50").unwrap()));
    assert_eq!(
        s["b"],
        Some(DecimalArbValue::from_str("5.25").unwrap()),
        "the -5.25 row must be filtered out before SUM"
    );
    assert_eq!(s["d"], Some(DecimalArbValue::from_str("100.00").unwrap()));
}

#[tokio::test]
async fn distinct_projection_of_a_decimal_arb_column_keeps_metadata() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00"), Some("1.0"), Some("2.00")]);
    let b = sql_ok(&sm, "SELECT DISTINCT amt FROM t").await;
    assert_eq!(rows(&b), 2);
    let f = output_field(&b, "amt");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "SELECT DISTINCT must not strip decimal_arb metadata: {f:?}"
    );
}

// =====================================================================
// 6. HAVING
// =====================================================================

#[tokio::test]
async fn having_on_count_star_filters_groups() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, COUNT(*) AS n FROM gt GROUP BY g HAVING COUNT(*) > 1",
    )
    .await;
    let n = map_i64(&b, "g", "n");
    assert_eq!(n.len(), 3, "a, b, c have >1 row; d has exactly one: {n:?}");
    assert!(!n.contains_key("d"));
}

#[tokio::test]
async fn having_on_count_of_a_decimal_arb_column_filters_groups() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, COUNT(amt) AS c FROM gt GROUP BY g HAVING COUNT(amt) = 0",
    )
    .await;
    let c = map_i64(&b, "g", "c");
    assert_eq!(c.len(), 1, "only group c is entirely NULL: {c:?}");
    assert_eq!(c["c"], Some(0));
}

#[tokio::test]
async fn having_that_excludes_everything_returns_no_rows() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, COUNT(*) AS n FROM gt GROUP BY g HAVING COUNT(*) > 100",
    )
    .await;
    assert_eq!(rows(&b), 0);
}

#[tokio::test]
async fn having_on_a_decimal_arb_group_key_compared_to_an_integer() {
    // The group key keeps its decimal_arb metadata, so the ExprPlanner can
    // coerce the integer literal — unlike an aggregate output.
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        0,
        &[Some("1"), Some("5"), Some("5"), Some("-9")],
    );
    let b = sql_ok(
        &sm,
        "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt HAVING amt > 0",
    )
    .await;
    assert_eq!(rows(&b), 2, "only the 1 and 5 groups survive");
    let mut got: BTreeMap<String, i64> = BTreeMap::new();
    for (k, n) in arb_vals(&b, "amt", 0).iter().zip(i64_vals(&b, "n")) {
        got.insert(k.as_ref().unwrap().to_canonical_string(), n.unwrap());
    }
    assert_eq!(got["1"], 1);
    assert_eq!(got["5"], 2);
}

#[tokio::test]
async fn having_combined_with_where_applies_both() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, COUNT(*) AS n FROM gt WHERE amt IS NOT NULL GROUP BY g HAVING COUNT(*) = 2",
    )
    .await;
    let n = map_i64(&b, "g", "n");
    assert_eq!(n.len(), 2, "a and b each keep two non-NULL rows: {n:?}");
    assert_eq!(n["a"], Some(2));
    assert_eq!(n["b"], Some(2));
}

// =====================================================================
// 7. FILTER (WHERE ...)
// =====================================================================

#[tokio::test]
async fn filter_clause_restricts_sum_to_matching_rows() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT SUM(amt) FILTER (WHERE k <= 2) AS s FROM gt").await;
    assert_eq!(
        arb_vals(&b, "s", SUM_SCALE),
        expect(&[Some("30.50")]),
        "FILTER must restrict SUM to k in {{1, 2}}"
    );
}

#[tokio::test]
async fn filter_clause_restricts_count_to_matching_rows() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT COUNT(*) FILTER (WHERE k > 5) AS n FROM gt").await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(3)]);
}

#[tokio::test]
async fn filter_clause_that_matches_nothing_yields_null_sum() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT SUM(amt) FILTER (WHERE k > 100) AS s FROM gt").await;
    assert_eq!(arb_vals(&b, "s", SUM_SCALE), vec![None]);
}

#[tokio::test]
async fn filter_clause_that_matches_nothing_yields_zero_count() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT COUNT(*) FILTER (WHERE k > 100) AS n FROM gt").await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(0)]);
}

#[tokio::test]
async fn filter_clause_on_min_and_max() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT MIN(amt) FILTER (WHERE g = 'b') AS lo, MAX(amt) FILTER (WHERE g = 'b') AS hi FROM gt",
    )
    .await;
    assert_eq!(arb_vals(&b, "lo", 2), expect(&[Some("-5.25")]));
    assert_eq!(arb_vals(&b, "hi", 2), expect(&[Some("5.25")]));
}

#[tokio::test]
async fn filter_clause_on_avg() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT AVG(amt) FILTER (WHERE g = 'a') AS a FROM gt").await;
    assert_eq!(arb_vals(&b, "a", AVG_SCALE), expect(&[Some("15.25")]));
}

#[tokio::test]
async fn filter_clause_combined_with_group_by() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, SUM(amt) FILTER (WHERE amt > 0) AS s FROM gt GROUP BY g",
    )
    .await;
    let s = map_arb(&b, "g", "s", SUM_SCALE);
    assert_eq!(s["a"], Some(DecimalArbValue::from_str("30.50").unwrap()));
    assert_eq!(s["b"], Some(DecimalArbValue::from_str("5.25").unwrap()));
    assert_eq!(s["c"], None);
    assert_eq!(s["d"], Some(DecimalArbValue::from_str("100.00").unwrap()));
}

#[tokio::test]
async fn filter_clause_using_a_decimal_arb_predicate() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT COUNT(*) FILTER (WHERE amt > 0) AS n FROM gt").await;
    assert_eq!(
        i64_vals(&b, "n"),
        vec![Some(4)],
        "10.00, 20.50, 5.25 and 100.00 are > 0"
    );
}

#[tokio::test]
async fn filter_clause_on_count_distinct() {
    let sm = session();
    register_grouped(
        &sm,
        "gt4",
        20,
        0,
        &[
            ("a", 1, Some("1")),
            ("a", 2, Some("1")),
            ("a", 3, Some("2")),
            ("b", 4, Some("3")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT COUNT(DISTINCT amt) FILTER (WHERE g = 'a') AS n FROM gt4",
    )
    .await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(2)]);
}

// =====================================================================
// 8. GROUPING SETS / ROLLUP / CUBE
// =====================================================================

#[tokio::test]
async fn rollup_over_a_decimal_arb_key_adds_a_grand_total_row() {
    let sm = session();
    register_grouped(
        &sm,
        "r",
        20,
        0,
        &[
            ("a", 1, Some("1")),
            ("a", 2, Some("2")),
            ("b", 3, Some("4")),
        ],
    );
    let b = sql_ok(&sm, "SELECT g, SUM(amt) AS s FROM r GROUP BY ROLLUP(g)").await;
    let s = map_arb(&b, "g", "s", 0);
    assert_eq!(s.len(), 3, "a, b and the NULL grand total: {s:?}");
    assert_eq!(s["a"], Some(DecimalArbValue::from_str("3").unwrap()));
    assert_eq!(s["b"], Some(DecimalArbValue::from_str("4").unwrap()));
    assert_eq!(
        s["NULL"],
        Some(DecimalArbValue::from_str("7").unwrap()),
        "the ROLLUP super-aggregate row must sum every group"
    );
}

#[tokio::test]
async fn rollup_over_two_keys_produces_all_prefix_levels() {
    let sm = session();
    register_grouped(
        &sm,
        "r",
        20,
        0,
        &[
            ("a", 1, Some("1")),
            ("a", 2, Some("2")),
            ("b", 1, Some("4")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT g, k, SUM(amt) AS s FROM r GROUP BY ROLLUP(g, k)",
    )
    .await;
    // levels: (g,k) 3 rows, (g) 2 rows, () 1 row = 6
    assert_eq!(rows(&b), 6, "ROLLUP(g, k) has 3 grouping levels");
}

#[tokio::test]
async fn cube_over_two_keys_produces_every_subset() {
    let sm = session();
    register_grouped(
        &sm,
        "r",
        20,
        0,
        &[
            ("a", 1, Some("1")),
            ("a", 2, Some("2")),
            ("b", 1, Some("4")),
        ],
    );
    let b = sql_ok(&sm, "SELECT g, k, SUM(amt) AS s FROM r GROUP BY CUBE(g, k)").await;
    // (g,k): 3, (g): 2, (k): 2, (): 1 => 8
    assert_eq!(rows(&b), 8, "CUBE(g, k) must emit every subset level");
}

#[tokio::test]
async fn grouping_sets_with_a_decimal_arb_key_column() {
    let sm = session();
    register_grouped(
        &sm,
        "r",
        20,
        0,
        &[
            ("a", 1, Some("1")),
            ("a", 2, Some("1")),
            ("b", 3, Some("2")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT g, amt, COUNT(*) AS n FROM r GROUP BY GROUPING SETS ((g), (amt))",
    )
    .await;
    // (g): a=2, b=1 ; (amt): 1=2, 2=1  => 4 rows
    assert_eq!(rows(&b), 4);
    let total: i64 = i64_vals(&b, "n").into_iter().flatten().sum();
    assert_eq!(total, 6, "each grouping set must cover all 3 rows");
}

#[tokio::test]
async fn grouping_sets_keep_decimal_arb_metadata_on_the_key_column() {
    let sm = session();
    register_grouped(
        &sm,
        "r",
        20,
        2,
        &[("a", 1, Some("1.50")), ("b", 2, Some("2.50"))],
    );
    let b = sql_ok(
        &sm,
        "SELECT g, amt, COUNT(*) AS n FROM r GROUP BY GROUPING SETS ((g), (amt))",
    )
    .await;
    let f = output_field(&b, "amt");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "GROUPING SETS must not strip decimal_arb metadata from a key: {f:?}"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((20, 2))
    );
}

#[tokio::test]
async fn rollup_super_aggregate_row_has_a_null_decimal_arb_key() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("1"), Some("2")]);
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY ROLLUP(amt)").await;
    assert_eq!(rows(&b), 3, "two keys plus the grand total");
    let keys = arb_vals(&b, "amt", 0);
    let ns = i64_vals(&b, "n");
    let null_pos = keys
        .iter()
        .position(|k| k.is_none())
        .expect("the ROLLUP total row must have a NULL decimal_arb key");
    assert_eq!(ns[null_pos], Some(2));
}

#[tokio::test]
async fn cube_over_a_decimal_arb_key_and_a_text_key() {
    let sm = session();
    register_grouped(&sm, "r", 20, 0, &[("a", 1, Some("1")), ("b", 2, Some("1"))]);
    let b = sql_ok(
        &sm,
        "SELECT g, amt, SUM(amt) AS s FROM r GROUP BY CUBE(g, amt)",
    )
    .await;
    // (g,amt): 2, (g): 2, (amt): 1, (): 1 => 6
    assert_eq!(rows(&b), 6);
    let sums = arb_vals(&b, "s", 0);
    let total_rows = sums
        .iter()
        .filter(|v| **v == Some(DecimalArbValue::from_str("2").unwrap()))
        .count();
    assert_eq!(
        total_rows, 2,
        "the (amt) and () levels both total 2: {sums:?}"
    );
}

#[tokio::test]
async fn grouping_sets_with_the_empty_set_is_the_grand_total() {
    let sm = session();
    register_grouped(&sm, "r", 20, 0, &[("a", 1, Some("1")), ("b", 2, Some("2"))]);
    let b = sql_ok(
        &sm,
        "SELECT g, SUM(amt) AS s FROM r GROUP BY GROUPING SETS ((g), ())",
    )
    .await;
    let s = map_arb(&b, "g", "s", 0);
    assert_eq!(s["NULL"], Some(DecimalArbValue::from_str("3").unwrap()));
}

// =====================================================================
// 9. Window functions
// =====================================================================

#[tokio::test]
async fn row_number_over_an_int_order_is_stable() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        0,
        &[
            ("a", 1, Some("10")),
            ("a", 2, Some("-5")),
            ("b", 3, Some("7")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, ROW_NUMBER() OVER (ORDER BY k) AS rn FROM w ORDER BY k",
    )
    .await;
    assert_eq!(i64_vals(&b, "rn"), vec![Some(1), Some(2), Some(3)]);
}

#[tokio::test]
#[ignore = "FINDING: `OVER (ORDER BY <decimal_arb>)` fails to plan (analyzer type_coercion) — DecimalArbSortRewriteRule only matches LogicalPlan::Sort, so a window's ORDER BY over decimal_arb is never handled"]
async fn row_number_over_a_decimal_arb_order_ranks_numerically() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        0,
        &[
            ("x", 1, Some("100")),
            ("x", 2, Some("-100")),
            ("x", 3, Some("0")),
            ("x", 4, Some("-1")),
            ("x", 5, Some("1000")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, ROW_NUMBER() OVER (ORDER BY amt) AS rn FROM w ORDER BY k",
    )
    .await;
    // numeric ascending: -100 (k=2), -1 (k=4), 0 (k=3), 100 (k=1), 1000 (k=5)
    assert_eq!(
        i64_vals(&b, "rn"),
        vec![Some(4), Some(1), Some(3), Some(2), Some(5)],
        "ROW_NUMBER() OVER (ORDER BY decimal_arb) must rank by numeric value"
    );
}

/// Documents the current shape: `OVER (ORDER BY <decimal_arb>)` does not even
/// reach execution — it dies in the analyzer's `type_coercion` pass.
#[tokio::test]
async fn window_order_by_decimal_arb_currently_fails_in_type_coercion() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        0,
        &[("x", 1, Some("100")), ("x", 2, Some("-100"))],
    );
    let err = sql_err(
        &sm,
        "SELECT k, ROW_NUMBER() OVER (ORDER BY amt) AS rn FROM w ORDER BY k",
    )
    .await;
    eprintln!("window ORDER BY decimal_arb error: {err}");
    assert!(
        err.contains("type_coercion"),
        "expected the analyzer type_coercion failure for a decimal_arb window ORDER BY; got: {err}"
    );
}

#[tokio::test]
async fn sum_over_an_empty_window_frame_spans_the_whole_partition() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 2, Some("2.00")),
            ("b", 3, Some("4.00")),
        ],
    );
    let b = sql_ok(&sm, "SELECT k, SUM(amt) OVER () AS s FROM w ORDER BY k").await;
    let s = arb_vals(&b, "s", 2);
    assert_eq!(
        s,
        expect(&[Some("7.00"), Some("7.00"), Some("7.00")]),
        "SUM(amt) OVER () must be the whole-relation total on every row"
    );
}

#[tokio::test]
#[ignore = "FINDING: any window function with PARTITION BY fails under SessionManager with a DataFusion internal assertion ('All partition by columns should have an ordering') — the custom physical optimizer rule set omits EnforceDistribution/EnforceSorting"]
async fn sum_over_partition_by_computes_per_partition_totals() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 2, Some("2.00")),
            ("b", 3, Some("4.00")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, SUM(amt) OVER (PARTITION BY g) AS s FROM w ORDER BY k",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("3.00"), Some("3.00"), Some("4.00")])
    );
}

#[tokio::test]
#[ignore = "FINDING: any window function with PARTITION BY fails under SessionManager with a DataFusion internal assertion ('All partition by columns should have an ordering') — the custom physical optimizer rule set omits EnforceDistribution/EnforceSorting"]
async fn sum_over_partition_skips_nulls_within_the_partition() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 2, None),
            ("a", 3, Some("2.00")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, SUM(amt) OVER (PARTITION BY g) AS s FROM w ORDER BY k",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("3.00"), Some("3.00"), Some("3.00")])
    );
}

#[tokio::test]
#[ignore = "FINDING: any window function with PARTITION BY fails under SessionManager with a DataFusion internal assertion ('All partition by columns should have an ordering') — the custom physical optimizer rule set omits EnforceDistribution/EnforceSorting"]
async fn sum_over_an_all_null_partition_is_null() {
    let sm = session();
    register_grouped(&sm, "w", 20, 2, &[("a", 1, None), ("a", 2, None)]);
    let b = sql_ok(
        &sm,
        "SELECT k, SUM(amt) OVER (PARTITION BY g) AS s FROM w ORDER BY k",
    )
    .await;
    assert_eq!(arb_vals(&b, "s", 2), vec![None, None]);
}

#[tokio::test]
async fn running_sum_over_order_by_int_accumulates() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 2, Some("2.00")),
            ("a", 3, Some("3.00")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, SUM(amt) OVER (ORDER BY k) AS s FROM w ORDER BY k",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("1.00"), Some("3.00"), Some("6.00")]),
        "the default frame is UNBOUNDED PRECEDING .. CURRENT ROW, i.e. a running total"
    );
}

#[tokio::test]
#[ignore = "FINDING: any window function with PARTITION BY fails under SessionManager with a DataFusion internal assertion ('All partition by columns should have an ordering') — the custom physical optimizer rule set omits EnforceDistribution/EnforceSorting"]
async fn running_sum_over_order_by_int_within_partitions() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("b", 2, Some("2.00")),
            ("a", 3, Some("3.00")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, SUM(amt) OVER (PARTITION BY g ORDER BY k) AS s FROM w ORDER BY k",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("1.00"), Some("2.00"), Some("4.00")])
    );
}

#[tokio::test]
#[ignore = "FINDING: any window function with PARTITION BY fails under SessionManager with a DataFusion internal assertion ('All partition by columns should have an ordering') — the custom physical optimizer rule set omits EnforceDistribution/EnforceSorting"]
async fn min_and_max_over_a_window_partition_are_numeric() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        0,
        &[
            ("a", 1, Some("100")),
            ("a", 2, Some("-100")),
            ("a", 3, Some("255")),
            ("a", 4, Some("256")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, MIN(amt) OVER (PARTITION BY g) AS lo, MAX(amt) OVER (PARTITION BY g) AS hi \
         FROM w ORDER BY k",
    )
    .await;
    let lo = arb_vals(&b, "lo", 0);
    let hi = arb_vals(&b, "hi", 0);
    assert!(
        lo.iter()
            .all(|v| *v == Some(DecimalArbValue::from_str("-100").unwrap())),
        "windowed MIN must be numeric: {lo:?}"
    );
    assert!(
        hi.iter()
            .all(|v| *v == Some(DecimalArbValue::from_str("256").unwrap())),
        "windowed MAX must be numeric: {hi:?}"
    );
}

#[tokio::test]
#[ignore = "FINDING: any window function with PARTITION BY fails under SessionManager with a DataFusion internal assertion ('All partition by columns should have an ordering') — the custom physical optimizer rule set omits EnforceDistribution/EnforceSorting"]
async fn avg_over_a_window_partition_widens_the_scale() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        2,
        &[("a", 1, Some("1.00")), ("a", 2, Some("2.00"))],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, AVG(amt) OVER (PARTITION BY g) AS a FROM w ORDER BY k",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "a", AVG_SCALE),
        expect(&[Some("1.50"), Some("1.50")])
    );
}

#[tokio::test]
#[ignore = "FINDING: any window function with PARTITION BY fails under SessionManager with a DataFusion internal assertion ('All partition by columns should have an ordering') — the custom physical optimizer rule set omits EnforceDistribution/EnforceSorting"]
async fn count_over_a_window_partition_counts_non_nulls() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 2, None),
            ("a", 3, Some("2.00")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, COUNT(amt) OVER (PARTITION BY g) AS n, COUNT(*) OVER (PARTITION BY g) AS m \
         FROM w ORDER BY k",
    )
    .await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(2), Some(2), Some(2)]);
    assert_eq!(i64_vals(&b, "m"), vec![Some(3), Some(3), Some(3)]);
}

#[tokio::test]
#[ignore = "FINDING: any window function with PARTITION BY fails under SessionManager with a DataFusion internal assertion ('All partition by columns should have an ordering') — the custom physical optimizer rule set omits EnforceDistribution/EnforceSorting"]
async fn partition_by_a_decimal_arb_column_groups_numerically_equal_values() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        2,
        &[
            ("x", 1, Some("5")),
            ("x", 2, Some("5.00")),
            ("x", 3, Some("6")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, COUNT(*) OVER (PARTITION BY amt) AS n FROM w ORDER BY k",
    )
    .await;
    assert_eq!(
        i64_vals(&b, "n"),
        vec![Some(2), Some(2), Some(1)],
        "PARTITION BY decimal_arb must treat 5 and 5.00 as one partition"
    );
}

#[tokio::test]
async fn sliding_window_frame_over_decimal_arb_sum_is_rejected_clearly() {
    // The decimal_arb SumAccumulator has no `retract_batch`, so
    // `create_sliding_accumulator` deliberately bails. It must be a clear
    // error, never a silently wrong running total.
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 2, Some("2.00")),
            ("a", 3, Some("3.00")),
        ],
    );
    let err = sql_err(
        &sm,
        "SELECT k, SUM(amt) OVER (ORDER BY k ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM w",
    )
    .await;
    assert!(
        err.contains("sliding") || err.contains("retract"),
        "a sliding decimal_arb window must fail with an explicit message; got: {err}"
    );
}

#[tokio::test]
async fn window_output_column_is_large_binary_storage() {
    let sm = session();
    register_grouped(&sm, "w", 20, 2, &[("a", 1, Some("1.00"))]);
    let b = sql_ok(&sm, "SELECT SUM(amt) OVER () AS s FROM w").await;
    assert_eq!(
        output_field(&b, "s").data_type(),
        &DataType::LargeBinary,
        "a decimal_arb window aggregate must stay in decimal_arb storage"
    );
}

#[tokio::test]
async fn window_pass_through_column_keeps_decimal_arb_metadata() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        2,
        &[("a", 1, Some("1.00")), ("a", 2, Some("2.00"))],
    );
    let b = sql_ok(
        &sm,
        "SELECT amt, ROW_NUMBER() OVER (ORDER BY k) AS rn FROM w",
    )
    .await;
    let f = output_field(&b, "amt");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "a column merely carried past a window function must keep its metadata: {f:?}"
    );
}

// =====================================================================
// 10. Ordering of aggregate results
// =====================================================================

#[tokio::test]
async fn order_by_a_decimal_arb_group_key_is_numeric() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        0,
        &[
            Some("100"),
            Some("-100"),
            Some("0"),
            Some("-1"),
            Some("1000"),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt ORDER BY amt ASC",
    )
    .await;
    let keys: Vec<String> = arb_vals(&b, "amt", 0)
        .into_iter()
        .map(|v| v.unwrap().to_canonical_string())
        .collect();
    assert_eq!(
        keys,
        vec!["-100", "-1", "0", "100", "1000"],
        "ORDER BY on a decimal_arb group key must be numeric (sort-key rewrite)"
    );
}

#[tokio::test]
#[ignore = "FINDING: ORDER BY over an aggregate of decimal_arb sorts bytewise — the aggregate output field has no decimal_arb metadata so DecimalArbSortRewriteRule skips it"]
async fn order_by_a_decimal_arb_sum_is_numeric() {
    let sm = session();
    register_grouped(
        &sm,
        "o",
        20,
        0,
        &[
            ("a", 1, Some("100")),
            ("b", 2, Some("-100")),
            ("c", 3, Some("0")),
            ("d", 4, Some("-1")),
            ("e", 5, Some("1000")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT g, SUM(amt) AS s FROM o GROUP BY g ORDER BY s ASC",
    )
    .await;
    assert_eq!(
        text_vals(&b, "g"),
        vec!["b", "d", "c", "a", "e"],
        "ORDER BY SUM(decimal_arb) must order by numeric value"
    );
}

/// Documents the current (bytewise) ordering of aggregate outputs.
#[tokio::test]
async fn order_by_a_decimal_arb_sum_currently_orders_bytewise() {
    let sm = session();
    register_grouped(
        &sm,
        "o",
        20,
        0,
        &[
            ("a", 1, Some("100")),
            ("b", 2, Some("-100")),
            ("c", 3, Some("0")),
            ("d", 4, Some("-1")),
            ("e", 5, Some("1000")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT g, SUM(amt) AS s FROM o GROUP BY g ORDER BY s ASC",
    )
    .await;
    assert_eq!(
        text_vals(&b, "g"),
        vec!["c", "e", "a", "d", "b"],
        "the bytewise aggregate-ordering finding no longer reproduces"
    );
}

#[tokio::test]
async fn order_by_count_is_unaffected_by_decimal_arb() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, COUNT(*) AS n FROM gt GROUP BY g ORDER BY n DESC, g ASC",
    )
    .await;
    assert_eq!(text_vals(&b, "g"), vec!["a", "b", "c", "d"]);
    assert_eq!(i64_vals(&b, "n"), vec![Some(3), Some(2), Some(2), Some(1)]);
}

#[tokio::test]
async fn order_by_a_decimal_arb_group_key_descending_is_numeric() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("100"), Some("-100"), Some("0")]);
    let b = sql_ok(
        &sm,
        "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt ORDER BY amt DESC",
    )
    .await;
    let keys: Vec<String> = arb_vals(&b, "amt", 0)
        .into_iter()
        .map(|v| v.unwrap().to_canonical_string())
        .collect();
    assert_eq!(keys, vec!["100", "0", "-100"]);
}

#[tokio::test]
async fn limit_after_group_by_on_a_decimal_arb_key_is_numeric() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        0,
        &[Some("100"), Some("-100"), Some("0"), Some("-1")],
    );
    let b = sql_ok(
        &sm,
        "SELECT amt FROM t GROUP BY amt ORDER BY amt ASC LIMIT 2",
    )
    .await;
    let keys: Vec<String> = arb_vals(&b, "amt", 0)
        .into_iter()
        .map(|v| v.unwrap().to_canonical_string())
        .collect();
    assert_eq!(
        keys,
        vec!["-100", "-1"],
        "TopK over a decimal_arb key must be numeric"
    );
}

// =====================================================================
// 11. Metadata / shape assertions on aggregate outputs
// =====================================================================

#[tokio::test]
async fn sum_output_is_large_binary_storage() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00")]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(output_field(&b, "s").data_type(), &DataType::LargeBinary);
}

#[tokio::test]
async fn min_output_is_large_binary_storage() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00")]);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS m FROM t").await;
    assert_eq!(output_field(&b, "m").data_type(), &DataType::LargeBinary);
}

#[tokio::test]
async fn aggregate_output_is_nullable() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00")]);
    let b = sql_ok(
        &sm,
        "SELECT SUM(amt) AS s, MIN(amt) AS m, AVG(amt) AS a FROM t",
    )
    .await;
    for c in ["s", "m", "a"] {
        assert!(
            output_field(&b, c).is_nullable(),
            "aggregate `{c}` must be nullable — an empty/all-NULL input yields NULL"
        );
    }
}

#[tokio::test]
async fn group_key_metadata_survives_a_subquery_boundary() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00"), Some("1.00")]);
    let b = sql_ok(
        &sm,
        "SELECT amt FROM (SELECT amt, COUNT(*) AS n FROM t GROUP BY amt) x",
    )
    .await;
    let f = output_field(&b, "amt");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "decimal_arb metadata must survive a subquery projection: {f:?}"
    );
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn min_still_numeric_when_the_input_comes_through_a_union_all() {
    // If UNION ALL drops the extension metadata, `min` silently falls back to
    // the built-in bytewise LargeBinary MIN and returns the wrong extreme.
    let sm = session();
    register_single(&sm, "t1", 20, 0, &[Some("100")]);
    register_single(&sm, "t2", 20, 0, &[Some("-100")]);
    let b = sql_ok(
        &sm,
        "SELECT MIN(amt) AS m FROM (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "m", 0),
        expect(&[Some("-100")]),
        "MIN across a UNION ALL must stay numeric — a metadata drop here silently \
         switches to the built-in bytewise MIN"
    );
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn sum_still_exact_when_the_input_comes_through_a_union_all() {
    let sm = session();
    register_single(&sm, "t1", 20, 0, &[Some("100")]);
    register_single(&sm, "t2", 20, 0, &[Some("-100")]);
    let b = sql_ok(
        &sm,
        "SELECT SUM(amt) AS s FROM (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u",
    )
    .await;
    assert_eq!(arb_vals(&b, "s", 0), expect(&[Some("0")]));
}

#[tokio::test]
async fn aggregate_of_a_case_expression_over_decimal_arb_is_numeric() {
    // The FunctionRewrite re-stamps CASE output metadata (F2 fix), so the
    // decimal_arb MIN accumulator — not the bytewise built-in — must run.
    let sm = session();
    register_grouped(
        &sm,
        "c",
        20,
        0,
        &[("a", 1, Some("100")), ("a", 2, Some("-100"))],
    );
    let b = sql_ok(
        &sm,
        "SELECT MIN(CASE WHEN k = 1 THEN amt ELSE amt END) AS m FROM c",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "m", 0),
        expect(&[Some("-100")]),
        "MIN over a metadata-restamped CASE must use the numeric accumulator"
    );
}

#[tokio::test]
async fn aggregate_of_a_coalesce_over_decimal_arb_is_numeric() {
    let sm = session();
    register_grouped(
        &sm,
        "c",
        20,
        0,
        &[("a", 1, Some("100")), ("a", 2, Some("-100"))],
    );
    let b = sql_ok(&sm, "SELECT MAX(COALESCE(amt, amt)) AS m FROM c").await;
    assert_eq!(arb_vals(&b, "m", 0), expect(&[Some("100")]));
}

#[tokio::test]
async fn sum_over_a_plain_large_binary_column_is_rejected() {
    // A bare LargeBinary column (no extension metadata) reaches the
    // decimal_arb coerce path by DataType alone; it must produce a clear
    // error, never a bytes-as-numbers result.
    let sm = session();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "blob",
        DataType::LargeBinary,
        true,
    )]));
    let arr = arrow::array::LargeBinaryArray::from_iter_values([b"\x00\x01".as_ref()]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
    sm.register_table(
        "bt",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    let err = sql_err(&sm, "SELECT SUM(blob) AS s FROM bt").await;
    assert!(
        err.contains("not supported") || err.to_lowercase().contains("decimal_arb"),
        "SUM over a plain LargeBinary column must be rejected explicitly, never \
         reinterpreted as canonical decimal bytes; got: {err}"
    );
}

// =====================================================================
// 12. Mixed / interaction cases
// =====================================================================

#[tokio::test]
async fn several_decimal_arb_aggregates_in_one_query_agree() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT SUM(amt) AS s, MIN(amt) AS lo, MAX(amt) AS hi, AVG(amt) AS a, COUNT(amt) AS n \
         FROM gt WHERE g = 'a'",
    )
    .await;
    assert_eq!(arb_vals(&b, "s", 2), expect(&[Some("30.50")]));
    assert_eq!(arb_vals(&b, "lo", 2), expect(&[Some("10.00")]));
    assert_eq!(arb_vals(&b, "hi", 2), expect(&[Some("20.50")]));
    assert_eq!(arb_vals(&b, "a", AVG_SCALE), expect(&[Some("15.25")]));
    assert_eq!(i64_vals(&b, "n"), vec![Some(2)]);
}

#[tokio::test]
async fn decimal_arb_and_int64_aggregates_coexist_in_one_query() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT SUM(amt) AS s, SUM(k) AS ki, AVG(k) AS ka FROM gt",
    )
    .await;
    assert_eq!(arb_vals(&b, "s", 2), expect(&[Some("130.50")]));
    assert_eq!(i64_vals(&b, "ki"), vec![Some(36)]);
    let ka = output_field(&b, "ka");
    assert_eq!(
        ka.data_type(),
        &DataType::Float64,
        "AVG over Int64 must still route to the built-in Float64 average"
    );
}

#[tokio::test]
async fn two_aggregates_over_columns_with_different_scales() {
    let sm = session();
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("a", 20, 0, true).unwrap(),
        DecimalArbType::field("b", 20, 4, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arb_array("a", 20, 0, &[Some("1"), Some("2")])),
            Arc::new(arb_array("b", 20, 4, &[Some("0.0001"), Some("0.0002")])),
        ],
    )
    .unwrap();
    sm.register_table(
        "ms",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    let b = sql_ok(&sm, "SELECT SUM(a) AS sa, SUM(b) AS sb FROM ms").await;
    assert_eq!(
        arb_vals(&b, "sa", 0),
        expect(&[Some("3")]),
        "each aggregate must use its own column's scale"
    );
    assert_eq!(arb_vals(&b, "sb", 4), expect(&[Some("0.0003")]));
}

#[tokio::test]
async fn aggregate_over_a_scale_zero_column_never_gains_a_fraction() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("1"), Some("2"), Some("4")]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s, MIN(amt) AS lo FROM t").await;
    for v in arb_vals(&b, "s", 0).into_iter().flatten() {
        assert_eq!(
            v.fractional_digit_count(),
            0,
            "SUM over scale 0 must stay integral: {v}"
        );
    }
    assert_eq!(arb_vals(&b, "lo", 0), expect(&[Some("1")]));
}

#[tokio::test]
async fn nested_aggregation_over_a_grouped_subquery() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT COUNT(*) AS n FROM (SELECT g, COUNT(*) AS c FROM gt GROUP BY g) x",
    )
    .await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(4)]);
}

#[tokio::test]
async fn aggregate_over_a_group_by_result_reaggregated_by_count() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        0,
        &[Some("1"), Some("1"), Some("2"), Some("3"), Some("3")],
    );
    let b = sql_ok(
        &sm,
        "SELECT COUNT(*) AS groups FROM (SELECT amt FROM t GROUP BY amt) x",
    )
    .await;
    assert_eq!(i64_vals(&b, "groups"), vec![Some(3)]);
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn sum_of_a_group_of_five_hundred_rows_across_partitions() {
    let sm = session();
    let part: Vec<Option<&str>> = [Some("2")].repeat(250);
    register_partitioned(&sm, "t", 20, 0, &[&part, &part]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s, COUNT(*) AS n FROM t").await;
    assert_eq!(arb_vals(&b, "s", 0), expect(&[Some("1000")]));
    assert_eq!(i64_vals(&b, "n"), vec![Some(500)]);
}

#[tokio::test]
async fn aggregate_with_a_where_that_removes_every_row() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT SUM(amt) AS s, COUNT(*) AS n, MIN(amt) AS lo FROM gt WHERE k > 1000",
    )
    .await;
    assert_eq!(
        rows(&b),
        1,
        "a scalar aggregate over no rows still emits one row"
    );
    assert_eq!(arb_vals(&b, "s", 2), vec![None]);
    assert_eq!(arb_vals(&b, "lo", 2), vec![None]);
    assert_eq!(i64_vals(&b, "n"), vec![Some(0)]);
}

#[tokio::test]
async fn group_by_then_order_by_count_then_limit() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT g, COUNT(*) AS n FROM gt GROUP BY g ORDER BY n DESC, g ASC LIMIT 1",
    )
    .await;
    assert_eq!(text_vals(&b, "g"), vec!["a"]);
    assert_eq!(i64_vals(&b, "n"), vec![Some(3)]);
}

#[tokio::test]
async fn sum_of_values_whose_total_needs_more_digits_than_the_column_precision() {
    // Declared precision 3 (scale 0) holds values up to 999; the SUM headroom
    // is +16 digits, so a total of 1998 must NOT be rejected.
    let sm = session();
    register_single(&sm, "t", 3, 0, &[Some("999"), Some("999")]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(
        arb_vals(&b, "s", 0),
        expect(&[Some("1998")]),
        "the +16-digit SUM headroom must absorb totals wider than the column precision"
    );
}

#[tokio::test]
async fn avg_of_values_at_the_column_precision_edge() {
    let sm = session();
    register_single(&sm, "t", 3, 0, &[Some("999"), Some("999")]);
    let b = sql_ok(&sm, "SELECT AVG(amt) AS a FROM t").await;
    assert_eq!(arb_vals(&b, "a", 1), expect(&[Some("999")]));
}

#[tokio::test]
async fn min_max_of_values_at_the_column_precision_edge() {
    let sm = session();
    register_single(&sm, "t", 3, 0, &[Some("999"), Some("-999")]);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(arb_vals(&b, "lo", 0), expect(&[Some("-999")]));
    assert_eq!(arb_vals(&b, "hi", 0), expect(&[Some("999")]));
}

#[tokio::test]
async fn group_by_a_column_declared_with_scale_equal_to_precision() {
    // All-fractional column: precision == scale, so every value is < 1.
    let sm = session();
    register_single(
        &sm,
        "t",
        4,
        4,
        &[Some("0.1234"), Some("0.1234"), Some("-0.5")],
    );
    let b = sql_ok(&sm, "SELECT amt, SUM(amt) AS s FROM t GROUP BY amt").await;
    assert_eq!(rows(&b), 2);
    let mut got: BTreeMap<String, String> = BTreeMap::new();
    for (k, s) in arb_vals(&b, "amt", 4).iter().zip(arb_vals(&b, "s", 4)) {
        got.insert(
            k.as_ref().unwrap().to_canonical_string(),
            s.unwrap().to_canonical_string(),
        );
    }
    assert_eq!(got["0.1234"], "0.2468");
    assert_eq!(got["-0.5000"], "-0.5000");
}

#[tokio::test]
async fn count_distinct_over_a_high_precision_column() {
    let sm = session();
    let a = "12345678901234567890123456789012345678901234567890";
    let c = "12345678901234567890123456789012345678901234567891";
    register_single(&sm, "t", 60, 0, &[Some(a), Some(a), Some(c)]);
    let b = sql_ok(&sm, "SELECT COUNT(DISTINCT amt) AS n FROM t").await;
    assert_eq!(
        i64_vals(&b, "n"),
        vec![Some(2)],
        "values differing only in the last of 50 digits must be distinct"
    );
}

#[tokio::test]
async fn group_by_distinguishes_values_differing_in_the_last_digit() {
    let sm = session();
    let a = "12345678901234567890123456789012345678901234567890";
    let c = "12345678901234567890123456789012345678901234567891";
    register_single(&sm, "t", 60, 0, &[Some(a), Some(a), Some(c)]);
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_eq!(rows(&b), 2);
    let mut got: BTreeMap<String, i64> = BTreeMap::new();
    for (k, n) in arb_vals(&b, "amt", 0).iter().zip(i64_vals(&b, "n")) {
        got.insert(k.as_ref().unwrap().to_canonical_string(), n.unwrap());
    }
    assert_eq!(got[a], 2);
    assert_eq!(got[c], 1);
}

#[tokio::test]
async fn aggregates_are_deterministic_across_repeated_runs() {
    let sm = session();
    standard(&sm);
    let sql = "SELECT g, SUM(amt) AS s FROM gt GROUP BY g";
    let first = map_arb(&sql_ok(&sm, sql).await, "g", "s", SUM_SCALE);
    for _ in 0..3 {
        let again = map_arb(&sql_ok(&sm, sql).await, "g", "s", SUM_SCALE);
        assert_eq!(first, again, "grouped aggregation must be deterministic");
    }
}

#[tokio::test]
async fn empty_group_by_list_behaves_like_a_scalar_aggregate() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM gt GROUP BY 1 - 1").await;
    assert_eq!(rows(&b), 1);
    assert_eq!(arb_vals(&b, "s", 2), expect(&[Some("130.50")]));
}

#[tokio::test]
async fn sum_and_count_agree_with_manual_totals_on_the_standard_fixture() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s, COUNT(amt) AS c FROM gt").await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("130.50")]),
        "10.00 + 20.50 - 5.25 + 5.25 + 100.00 = 130.50"
    );
    assert_eq!(i64_vals(&b, "c"), vec![Some(5)]);
}

// =====================================================================
// 13. Controls that isolate the two infrastructure findings
//
// `SessionManager::new` calls
// `SessionStateBuilder::with_physical_optimizer_rules(StreamlingPhysicalOptimizerRules::rules())`,
// which *replaces* DataFusion's default physical rule set with three custom
// rules. `EnforceDistribution`, `EnforceSorting` and `CoalescePartitions`
// therefore never run. The tests below pin the observable consequences and
// prove they are (a) not decimal_arb-specific and (b) caused by the streamling
// session configuration rather than by DataFusion itself.
// =====================================================================

/// A plain `Int64` column shows the same per-partition aggregation, so the
/// defect is in the session configuration, not in the decimal_arb UDAFs.
#[tokio::test]
async fn multi_partition_int64_sum_currently_emits_one_row_per_partition() {
    let sm = session();
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let parts: Vec<Vec<RecordBatch>> = vec![
        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
            )
            .unwrap(),
        ],
        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![3_i64, 4]))],
            )
            .unwrap(),
        ],
    ];
    sm.register_table("ip", Arc::new(MemTable::try_new(schema, parts).unwrap()))
        .unwrap();
    let b = sql_ok(&sm, "SELECT SUM(i) AS s FROM ip").await;
    assert_eq!(
        rows(&b),
        2,
        "a scalar aggregate must return ONE row; two rows means the partial \
         aggregates were never merged (this reproduces for Int64 too, so it is \
         not a decimal_arb defect)"
    );
    let mut got = i64_vals(&b, "s");
    got.sort();
    assert_eq!(
        got,
        vec![Some(3), Some(7)],
        "per-partition partial sums leaked out"
    );
}

#[tokio::test]
#[ignore = "FINDING: SessionManager replaces DataFusion's default physical optimizer rules with three custom ones, dropping EnforceDistribution/CoalescePartitions — an aggregate over a multi-partition input emits one row PER PARTITION instead of one merged result"]
async fn multi_partition_int64_sum_should_emit_one_merged_row() {
    let sm = session();
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let parts: Vec<Vec<RecordBatch>> = vec![
        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
            )
            .unwrap(),
        ],
        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![3_i64, 4]))],
            )
            .unwrap(),
        ],
    ];
    sm.register_table("ip", Arc::new(MemTable::try_new(schema, parts).unwrap()))
        .unwrap();
    let b = sql_ok(&sm, "SELECT SUM(i) AS s FROM ip").await;
    assert_eq!(
        rows(&b),
        1,
        "SUM over a multi-partition table is a single total"
    );
    assert_eq!(i64_vals(&b, "s"), vec![Some(10)]);
}

/// Control: a stock DataFusion `SessionContext` (default physical rules) over
/// the very same multi-partition `MemTable` merges correctly. This is what
/// isolates the defect to the streamling rule set.
#[tokio::test]
async fn stock_datafusion_session_merges_multi_partition_aggregates() {
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let parts: Vec<Vec<RecordBatch>> = vec![
        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
            )
            .unwrap(),
        ],
        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![3_i64, 4]))],
            )
            .unwrap(),
        ],
    ];
    ctx.register_table("ip", Arc::new(MemTable::try_new(schema, parts).unwrap()))
        .unwrap();
    let b = ctx
        .sql("SELECT SUM(i) AS s FROM ip")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        rows(&b),
        1,
        "stock DataFusion merges the partial aggregates — the streamling session does not"
    );
    assert_eq!(i64_vals(&b, "s"), vec![Some(10)]);
}

/// The same defect makes `SUM(decimal_arb)` over a multi-partition source
/// return several partial totals. Pinned as the current shape so a fix flips
/// the ignored tests above.
#[tokio::test]
async fn multi_partition_decimal_arb_sum_currently_emits_partial_totals() {
    let sm = session();
    register_partitioned(&sm, "t", 20, 2, &[&[Some("1.00")], &[Some("2.00")]]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s FROM t").await;
    assert_eq!(
        rows(&b),
        2,
        "decimal_arb inherits the unmerged-partition defect"
    );
    let mut got: Vec<String> = arb_vals(&b, "s", 2)
        .into_iter()
        .map(|v| v.unwrap().to_canonical_string())
        .collect();
    got.sort();
    assert_eq!(got, vec!["1.00", "2.00"]);
}

/// Documents the current shape of the window `PARTITION BY` failure: it is a
/// hard DataFusion *internal* assertion, not a user-facing error, and it hits
/// plain Int64 window functions too.
#[tokio::test]
async fn window_partition_by_currently_fails_with_an_internal_assertion() {
    let sm = session();
    register_grouped(&sm, "w", 20, 0, &[("a", 1, Some("1")), ("b", 2, Some("2"))]);
    let err = sql_err(&sm, "SELECT COUNT(*) OVER (PARTITION BY g) AS n FROM w").await;
    assert!(
        err.contains("All partition by columns should have an ordering"),
        "expected the missing-EnforceSorting internal assertion; got: {err}"
    );
    // Not decimal_arb-specific: the same query shape over the Int64 column
    // fails identically.
    let err2 = sql_err(&sm, "SELECT COUNT(*) OVER (PARTITION BY k) AS n FROM w").await;
    assert!(
        err2.contains("All partition by columns should have an ordering"),
        "PARTITION BY over a plain Int64 column fails the same way; got: {err2}"
    );
}

/// Control: stock DataFusion runs the identical `PARTITION BY` window fine.
#[tokio::test]
async fn stock_datafusion_session_supports_window_partition_by() {
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("k", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "b"])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
        ],
    )
    .unwrap();
    ctx.register_table(
        "w",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    let b = ctx
        .sql("SELECT COUNT(*) OVER (PARTITION BY g) AS n FROM w")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        rows(&b),
        3,
        "stock DataFusion plans PARTITION BY windows; the streamling session panics on them"
    );
}

/// Metadata survival through `UNION ALL` is testable independently of the
/// partition-merge defect (no aggregate involved).
#[tokio::test]
async fn union_all_preserves_decimal_arb_metadata() {
    let sm = session();
    register_single(&sm, "u1", 20, 2, &[Some("1.00")]);
    register_single(&sm, "u2", 20, 2, &[Some("2.00")]);
    let b = sql_ok(&sm, "SELECT amt FROM u1 UNION ALL SELECT amt FROM u2").await;
    let f = output_field(&b, "amt");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "UNION ALL must not strip decimal_arb metadata (a drop here silently \
         reroutes MIN/MAX/SUM to the bytewise built-ins): {f:?}"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((20, 2))
    );
}

/// A single-partition `UNION ALL` (both arms materialised into one relation by
/// a preceding aggregation) still aggregates numerically.
#[tokio::test]
async fn aggregate_over_a_single_partition_union_is_numeric() {
    let sm = session();
    register_single(&sm, "u1", 20, 0, &[Some("100"), Some("-100")]);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM u1").await;
    assert_eq!(arb_vals(&b, "lo", 0), expect(&[Some("-100")]));
    assert_eq!(arb_vals(&b, "hi", 0), expect(&[Some("100")]));
}

// =====================================================================
// 14. Follow-ups on the DISTINCT and window-ordering findings
// =====================================================================

/// Control for the multi-DISTINCT finding: the same query shape over the
/// built-in Int64 `sum` *is* correct, so the defect is specific to the
/// decimal_arb accumulator ignoring `AccumulatorArgs::is_distinct`.
#[tokio::test]
async fn sum_distinct_beside_another_distinct_aggregate_is_correct_for_int64() {
    let sm = session();
    register_grouped(
        &sm,
        "dd",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 1, Some("1.00")),
            ("b", 2, Some("1.00")),
            ("b", 2, Some("2.00")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT SUM(DISTINCT k) AS s, COUNT(DISTINCT g) AS n FROM dd",
    )
    .await;
    assert_eq!(
        i64_vals(&b, "s"),
        vec![Some(3)],
        "the built-in Int64 SUM honours DISTINCT (1 + 2), so the decimal_arb path is the outlier"
    );
    assert_eq!(i64_vals(&b, "n"), vec![Some(2)]);
}

/// Documents the current (wrong) result of the multi-DISTINCT finding.
#[tokio::test]
async fn sum_distinct_beside_another_distinct_currently_returns_the_plain_sum() {
    let sm = session();
    register_grouped(
        &sm,
        "dd",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 1, Some("1.00")),
            ("a", 2, Some("1.00")),
            ("a", 2, Some("2.00")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT SUM(DISTINCT amt) AS s, COUNT(DISTINCT k) AS n FROM dd",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "s", 2),
        expect(&[Some("5.00")]),
        "the ignored DISTINCT finding no longer reproduces — re-check it"
    );
}

#[tokio::test]
async fn avg_distinct_beside_another_distinct_currently_returns_the_plain_avg() {
    let sm = session();
    register_grouped(
        &sm,
        "dd",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 1, Some("1.00")),
            ("a", 2, Some("1.00")),
            ("a", 2, Some("3.00")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT AVG(DISTINCT amt) AS a, COUNT(DISTINCT k) AS n FROM dd",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "a", AVG_SCALE),
        expect(&[Some("1.50")]),
        "AVG(DISTINCT) beside a second DISTINCT argument silently averages all four rows"
    );
}

#[tokio::test]
#[ignore = "FINDING: AVG(DISTINCT decimal_arb) alongside a second, differently-argued DISTINCT aggregate silently returns the non-distinct AVG (AccumulatorArgs::is_distinct is ignored)"]
async fn avg_distinct_still_deduplicates_beside_another_distinct_aggregate() {
    let sm = session();
    register_grouped(
        &sm,
        "dd",
        20,
        2,
        &[
            ("a", 1, Some("1.00")),
            ("a", 1, Some("1.00")),
            ("a", 2, Some("1.00")),
            ("a", 2, Some("3.00")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT AVG(DISTINCT amt) AS a, COUNT(DISTINCT k) AS n FROM dd",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "a", AVG_SCALE),
        expect(&[Some("2.00")]),
        "AVG(DISTINCT) over {{1.00, 3.00}} is 2.00"
    );
}

#[tokio::test]
#[ignore = "FINDING: a window ORDER BY over decimal_arb with an explicit ROWS frame plans but the ordering is silently dropped (missing EnforceSorting), so the window walks raw input order"]
async fn window_rows_frame_ordered_by_decimal_arb_uses_numeric_order() {
    let sm = session();
    register_grouped(
        &sm,
        "w",
        20,
        0,
        &[
            ("x", 1, Some("100")),
            ("x", 2, Some("-100")),
            ("x", 3, Some("0")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, SUM(k) OVER (ORDER BY amt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
         AS rs FROM w ORDER BY k",
    )
    .await;
    // numeric ascending is -100 (k=2), 0 (k=3), 100 (k=1)
    // running sums of k: k=2 -> 2, k=3 -> 5, k=1 -> 6
    assert_eq!(
        i64_vals(&b, "rs"),
        vec![Some(6), Some(2), Some(5)],
        "a window ordered by a decimal_arb column must walk rows in numeric order"
    );
}

// =====================================================================
// 15. Extra aggregate edge cases
// =====================================================================

#[tokio::test]
async fn sum_of_all_zero_rows_is_zero_not_null() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("0"), Some("0.00"), Some("-0")]);
    let b = sql_ok(&sm, "SELECT SUM(amt) AS s, COUNT(amt) AS n FROM t").await;
    assert_eq!(arb_vals(&b, "s", 2), expect(&[Some("0")]));
    assert_eq!(i64_vals(&b, "n"), vec![Some(3)]);
}

#[tokio::test]
async fn group_by_merges_zero_and_negative_zero() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("0"), Some("-0"), Some("0.00")]);
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_eq!(rows(&b), 1, "+0 and -0 must never split into two groups");
    assert_eq!(i64_vals(&b, "n"), vec![Some(3)]);
}

#[tokio::test]
async fn min_max_of_a_constant_column_are_equal() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("7.50"), Some("7.5"), Some("7.500")]);
    let b = sql_ok(&sm, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(arb_vals(&b, "lo", 2), arb_vals(&b, "hi", 2));
    assert_eq!(arb_vals(&b, "lo", 2), expect(&[Some("7.50")]));
}

#[tokio::test]
async fn group_with_exactly_one_null_and_one_value() {
    let sm = session();
    register_grouped(&sm, "gx", 20, 2, &[("a", 1, None), ("a", 2, Some("3.00"))]);
    let b = sql_ok(
        &sm,
        "SELECT g, SUM(amt) AS s, MIN(amt) AS lo, AVG(amt) AS a, COUNT(amt) AS n FROM gx GROUP BY g",
    )
    .await;
    assert_eq!(arb_vals(&b, "s", 2), expect(&[Some("3.00")]));
    assert_eq!(arb_vals(&b, "lo", 2), expect(&[Some("3.00")]));
    assert_eq!(arb_vals(&b, "a", AVG_SCALE), expect(&[Some("3.00")]));
    assert_eq!(i64_vals(&b, "n"), vec![Some(1)]);
}

#[tokio::test]
async fn having_on_count_distinct_filters_groups() {
    let sm = session();
    register_grouped(
        &sm,
        "hd",
        20,
        0,
        &[
            ("a", 1, Some("1")),
            ("a", 2, Some("1.0")),
            ("b", 3, Some("1")),
            ("b", 4, Some("2")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT g, COUNT(DISTINCT amt) AS n FROM hd GROUP BY g HAVING COUNT(DISTINCT amt) > 1",
    )
    .await;
    assert_eq!(
        text_vals(&b, "g"),
        vec!["b"],
        "group a has one distinct value (1 == 1.0), group b has two"
    );
}

#[tokio::test]
async fn group_by_then_having_then_order_by_the_decimal_arb_key() {
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        0,
        &[
            Some("-5"),
            Some("-5"),
            Some("3"),
            Some("3"),
            Some("3"),
            Some("9"),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt HAVING COUNT(*) > 1 ORDER BY amt ASC",
    )
    .await;
    let keys: Vec<String> = arb_vals(&b, "amt", 0)
        .into_iter()
        .map(|v| v.unwrap().to_canonical_string())
        .collect();
    assert_eq!(keys, vec!["-5", "3"]);
    assert_eq!(i64_vals(&b, "n"), vec![Some(2), Some(3)]);
}

#[tokio::test]
async fn filter_clause_whose_predicate_is_null_for_some_rows() {
    // `amt > 0` is NULL for the NULL rows; a NULL FILTER predicate must
    // exclude the row (it is not TRUE), not include it.
    let sm = session();
    standard(&sm);
    let b = sql_ok(&sm, "SELECT COUNT(*) FILTER (WHERE amt > 0) AS n FROM gt").await;
    assert_eq!(
        i64_vals(&b, "n"),
        vec![Some(4)],
        "rows where the FILTER predicate evaluates to NULL must not be counted"
    );
}

#[tokio::test]
async fn rollup_totals_match_the_plain_grand_total() {
    let sm = session();
    register_grouped(
        &sm,
        "r",
        20,
        2,
        &[
            ("a", 1, Some("1.25")),
            ("a", 2, Some("2.25")),
            ("b", 3, Some("4.50")),
        ],
    );
    let rollup = sql_ok(&sm, "SELECT g, SUM(amt) AS s FROM r GROUP BY ROLLUP(g)").await;
    let plain = sql_ok(&sm, "SELECT SUM(amt) AS s FROM r").await;
    let total = map_arb(&rollup, "g", "s", SUM_SCALE);
    assert_eq!(
        total["NULL"],
        arb_vals(&plain, "s", SUM_SCALE)[0],
        "the ROLLUP super-aggregate must equal the plain grand total"
    );
}

#[tokio::test]
async fn cube_levels_all_sum_to_the_same_grand_total() {
    let sm = session();
    register_grouped(
        &sm,
        "r",
        20,
        0,
        &[
            ("a", 1, Some("1")),
            ("a", 2, Some("2")),
            ("b", 1, Some("4")),
        ],
    );
    let b = sql_ok(&sm, "SELECT g, k, SUM(amt) AS s FROM r GROUP BY CUBE(g, k)").await;
    // (g,k) rows total 7, (g) rows total 7, (k) rows total 7, () row is 7.
    let vals = arb_vals(&b, "s", 0);
    let seven = DecimalArbValue::from_str("7").unwrap();
    assert_eq!(
        vals.iter().filter(|v| **v == Some(seven.clone())).count(),
        1,
        "exactly one CUBE row is the grand total: {vals:?}"
    );
}

#[tokio::test]
async fn grouping_sets_over_only_the_decimal_arb_key() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("1"), Some("1"), Some("2")]);
    let b = sql_ok(
        &sm,
        "SELECT amt, COUNT(*) AS n FROM t GROUP BY GROUPING SETS ((amt), ())",
    )
    .await;
    assert_eq!(rows(&b), 3, "two keys plus the empty grouping set");
    let ns: i64 = i64_vals(&b, "n").into_iter().flatten().sum();
    assert_eq!(ns, 6, "3 rows counted once per grouping set");
}

#[tokio::test]
async fn aggregate_over_a_column_whose_values_share_a_magnitude_prefix() {
    // 0x0100 (256) vs 0x01 (1): the shorter magnitude is a byte-prefix of the
    // longer one, the classic bytewise-comparison trap.
    let sm = session();
    register_single(
        &sm,
        "t",
        20,
        0,
        &[Some("1"), Some("256"), Some("-1"), Some("-256")],
    );
    let b = sql_ok(
        &sm,
        "SELECT MIN(amt) AS lo, MAX(amt) AS hi, SUM(amt) AS s FROM t",
    )
    .await;
    assert_eq!(arb_vals(&b, "lo", 0), expect(&[Some("-256")]));
    assert_eq!(arb_vals(&b, "hi", 0), expect(&[Some("256")]));
    assert_eq!(arb_vals(&b, "s", 0), expect(&[Some("0")]));
}

#[tokio::test]
async fn group_by_distinguishes_prefix_magnitudes() {
    let sm = session();
    register_single(&sm, "t", 20, 0, &[Some("1"), Some("256"), Some("1")]);
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_eq!(rows(&b), 2);
    let mut got: BTreeMap<String, i64> = BTreeMap::new();
    for (k, n) in arb_vals(&b, "amt", 0).iter().zip(i64_vals(&b, "n")) {
        got.insert(k.as_ref().unwrap().to_canonical_string(), n.unwrap());
    }
    assert_eq!(got["1"], 2);
    assert_eq!(got["256"], 1);
}

#[tokio::test]
async fn count_of_a_decimal_arb_expression_still_returns_int64() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT COUNT(CASE WHEN amt > 0 THEN amt ELSE NULL END) AS n FROM gt",
    )
    .await;
    assert_eq!(i64_vals(&b, "n"), vec![Some(4)]);
}

#[tokio::test]
async fn sum_over_a_case_expression_of_decimal_arb_is_exact() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT SUM(CASE WHEN g = 'a' THEN amt ELSE NULL END) AS s FROM gt",
    )
    .await;
    assert_eq!(
        arb_vals(&b, "s", SUM_SCALE),
        expect(&[Some("30.50")]),
        "SUM over a metadata-restamped CASE must use the decimal_arb accumulator"
    );
}

#[tokio::test]
async fn aggregate_result_can_be_grouped_again_in_an_outer_query() {
    let sm = session();
    standard(&sm);
    let b = sql_ok(
        &sm,
        "SELECT c, COUNT(*) AS m FROM (SELECT g, COUNT(*) AS c FROM gt GROUP BY g) x GROUP BY c",
    )
    .await;
    let m = map_i64(&b, "c", "m");
    assert_eq!(m["3"], Some(1));
    assert_eq!(m["2"], Some(2));
    assert_eq!(m["1"], Some(1));
}

#[tokio::test]
async fn group_by_alias_of_a_decimal_arb_column_keeps_metadata() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00"), Some("1.00")]);
    let b = sql_ok(&sm, "SELECT amt AS key, COUNT(*) AS n FROM t GROUP BY key").await;
    let f = output_field(&b, "key");
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "aliasing a decimal_arb group key must not drop its metadata: {f:?}"
    );
    assert_eq!(arb_vals(&b, "key", 2), expect(&[Some("1.00")]));
}

#[tokio::test]
async fn group_by_ordinal_position_of_a_decimal_arb_column() {
    let sm = session();
    register_single(&sm, "t", 20, 2, &[Some("1.00"), Some("1.00"), Some("2.00")]);
    let b = sql_ok(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY 1").await;
    assert_eq!(rows(&b), 2);
    let f = output_field(&b, "amt");
    assert!(DecimalArbType::is_decimal_arb_field(&f), "{f:?}");
}

/// Control for the window-ordering finding: a window `ORDER BY` over a plain
/// `Int64` column is *also* ignored under `SessionManager`, which pins the
/// cause on the missing `EnforceSorting` physical rule rather than on
/// decimal_arb. Input rows are deliberately supplied out of `k` order.
#[tokio::test]
async fn window_order_by_int64_currently_ignores_the_ordering() {
    let sm = session();
    register_grouped(
        &sm,
        "wo",
        20,
        0,
        &[
            ("x", 3, Some("1")),
            ("x", 1, Some("1")),
            ("x", 2, Some("1")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, SUM(k) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
         AS rs FROM wo ORDER BY k",
    )
    .await;
    // Correct running sums in k order would be [1, 3, 6] for k = 1, 2, 3.
    // Observed: the window walks the *input* order 3, 1, 2 -> 3, 4, 6, which
    // reported against k = 1, 2, 3 is [4, 6, 3].
    assert_eq!(
        i64_vals(&b, "rs"),
        vec![Some(4), Some(6), Some(3)],
        "the ignored-window-ORDER-BY finding no longer reproduces"
    );
}

#[tokio::test]
#[ignore = "FINDING: a window's ORDER BY is silently ignored under SessionManager (the custom physical optimizer rule set omits EnforceSorting), so running/ranking window results are computed in raw input order"]
async fn window_order_by_int64_should_sort_the_window_input() {
    let sm = session();
    register_grouped(
        &sm,
        "wo",
        20,
        0,
        &[
            ("x", 3, Some("1")),
            ("x", 1, Some("1")),
            ("x", 2, Some("1")),
        ],
    );
    let b = sql_ok(
        &sm,
        "SELECT k, SUM(k) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
         AS rs FROM wo ORDER BY k",
    )
    .await;
    assert_eq!(
        i64_vals(&b, "rs"),
        vec![Some(1), Some(3), Some(6)],
        "a window ORDER BY must actually order the window input"
    );
}
