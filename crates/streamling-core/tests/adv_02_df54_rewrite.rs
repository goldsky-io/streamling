//! Adversarial coverage for the `DecimalArbExprRewrite` `FunctionRewrite`
//! surface (agent 02).
//!
//! The rewrite runs inside the analyzer, *before* `TypeCoercion`, and is the
//! only thing that makes `BETWEEN` / `IN` over `decimal_arb` plan at all (F1b)
//! and the only thing that puts the extension metadata back onto a
//! `CASE`/`COALESCE` output field (F2). Everything here drives it through the
//! real `SessionManager` stack (UDFs + `DecimalArbExprPlanner` +
//! `DecimalArbExprRewrite` + `DecimalArbSortRewriteRule`), so a test failing
//! here is a failure a real pipeline would hit.
//!
//! Every assertion checks one of three properties:
//!   * the rewrite fires when it should (right rows, right values),
//!   * the output field keeps `(precision, scale)` metadata — a metadata-less
//!     `LargeBinary` column is silent corruption at the sink, not a cosmetic
//!     issue,
//!   * where the rewrite deliberately bails out, the failure is a clean
//!     DataFusion error rather than wrong data.

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, LargeBinaryArray, RecordBatch,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::error::Result as DFResult;

use streamling_core::dynamic_table::DynamicTableRegistry;
use streamling_core::session::SessionManager;
use streamling_core::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn dv(s: &str) -> DecimalArbValue {
    DecimalArbValue::from_str(s).expect("test literal must parse")
}

/// Build a decimal_arb column (field + array) from textual values.
fn arb_col(name: &str, p: u32, s: u32, vals: &[Option<&str>]) -> (Field, ArrayRef) {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), name, p, s).unwrap();
    for v in vals {
        match v {
            Some(x) => b.append_value(&dv(x)).unwrap(),
            None => b.append_null(),
        }
    }
    let (raw, _, _) = b.finish().into_inner();
    (
        DecimalArbType::field(name, p, s, true).unwrap(),
        Arc::new(raw) as ArrayRef,
    )
}

fn session() -> SessionManager {
    SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap()
}

/// The workhorse table.
///
/// ```text
/// id | amt(100,0) | alt(100,0) | amt2(100,2) | small(20,0) | fl      | txt   | flag
///  1 |          5 |        999 |        5.00 |           5 |   5.0   | "5"   |    1
///  2 |         -3 |        888 |       -3.00 |          -3 |  -3.0   | "-3"  |    0
///  3 |          0 |          1 |        0.00 |           0 |   0.0   | "0"   |    1
///  4 |        200 |          2 |      200.00 |         200 | 200.0   | "200" |    0
///  5 |       NULL |          3 |        NULL |        NULL |   1.5   | "x"   |    1
///  6 |          7 |          4 |        7.00 |           7 |   7.0   | "7"   |    0
/// ```
///
/// `amt` / `amt2` / `small` hold the *same numbers*: `amt` vs `amt2` differ only
/// in scale (byte encodings differ), `amt` vs `small` differ only in precision
/// (byte encodings identical).
fn register_main(sm: &SessionManager) {
    let (f_amt, c_amt) = arb_col(
        "amt",
        100,
        0,
        &[
            Some("5"),
            Some("-3"),
            Some("0"),
            Some("200"),
            None,
            Some("7"),
        ],
    );
    let (f_alt, c_alt) = arb_col(
        "alt",
        100,
        0,
        &[
            Some("999"),
            Some("888"),
            Some("1"),
            Some("2"),
            Some("3"),
            Some("4"),
        ],
    );
    let (f_amt2, c_amt2) = arb_col(
        "amt2",
        100,
        2,
        &[
            Some("5.00"),
            Some("-3.00"),
            Some("0.00"),
            Some("200.00"),
            None,
            Some("7.00"),
        ],
    );
    let (f_small, c_small) = arb_col(
        "small",
        20,
        0,
        &[
            Some("5"),
            Some("-3"),
            Some("0"),
            Some("200"),
            None,
            Some("7"),
        ],
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        f_amt,
        f_alt,
        f_amt2,
        f_small,
        Field::new("fl", DataType::Float64, true),
        Field::new("txt", DataType::Utf8, true),
        Field::new("flag", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5, 6])),
            c_amt,
            c_alt,
            c_amt2,
            c_small,
            Arc::new(Float64Array::from(vec![5.0, -3.0, 0.0, 200.0, 1.5, 7.0])),
            Arc::new(StringArray::from(vec!["5", "-3", "0", "200", "x", "7"])),
            Arc::new(Int64Array::from(vec![1_i64, 0, 1, 0, 1, 0])),
        ],
    )
    .unwrap();
    sm.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
}

/// A second table used for `IN (SELECT ...)` probes: `u(v decimal_arb(100,0))`
/// and `u2(v decimal_arb(100,2))` hold the same numbers at different scales.
fn register_probe_tables(sm: &SessionManager) {
    let (f, c) = arb_col("v", 100, 0, &[Some("5"), Some("200")]);
    let schema = Arc::new(Schema::new(vec![f]));
    let batch = RecordBatch::try_new(schema.clone(), vec![c]).unwrap();
    sm.register_table(
        "u",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();

    let (f2, c2) = arb_col("v", 100, 2, &[Some("5.00"), Some("200.00")]);
    let schema2 = Arc::new(Schema::new(vec![f2]));
    let batch2 = RecordBatch::try_new(schema2.clone(), vec![c2]).unwrap();
    sm.register_table(
        "u2",
        Arc::new(MemTable::try_new(schema2, vec![vec![batch2]]).unwrap()),
    )
    .unwrap();
}

fn main_session() -> SessionManager {
    let sm = session();
    register_main(&sm);
    sm
}

async fn run(sm: &SessionManager, sql: &str) -> DFResult<Vec<RecordBatch>> {
    let plan = sm.create_logical_plan(sql.to_string()).await?;
    sm.new_df(plan).collect().await
}

/// Run and unwrap with the SQL in the panic message.
async fn ok(sm: &SessionManager, sql: &str) -> Vec<RecordBatch> {
    match run(sm, sql).await {
        Ok(b) => b,
        Err(e) => panic!("query should plan and execute\n  sql: {sql}\n  err: {e}"),
    }
}

/// Run expecting a failure; returns the error text.
async fn err(sm: &SessionManager, sql: &str) -> String {
    match run(sm, sql).await {
        Ok(b) => panic!(
            "query unexpectedly succeeded (returned {} row(s))\n  sql: {sql}",
            b.iter().map(|x| x.num_rows()).sum::<usize>()
        ),
        Err(e) => e.to_string(),
    }
}

fn out_field(batches: &[RecordBatch], name: &str) -> Field {
    assert!(
        !batches.is_empty(),
        "no batches returned — cannot inspect the output schema for '{name}'"
    );
    batches[0]
        .schema()
        .field_with_name(name)
        .unwrap_or_else(|_| panic!("output has no column '{name}'"))
        .clone()
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of("id").expect("query must project id");
        let c = b
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        for i in 0..c.len() {
            out.push(c.value(i));
        }
    }
    out.sort_unstable();
    out
}

fn bools(batches: &[RecordBatch], name: &str) -> Vec<Option<bool>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let c = b
            .column(idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap_or_else(|| {
                panic!(
                    "column '{name}' should be Boolean, got {:?}",
                    b.column(idx).data_type()
                )
            });
        for i in 0..c.len() {
            out.push(if c.is_null(i) { None } else { Some(c.value(i)) });
        }
    }
    out
}

/// Decode a `LargeBinary` decimal_arb output column at an explicit scale.
fn arb_at(batches: &[RecordBatch], name: &str, scale: u32) -> Vec<Option<DecimalArbValue>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let c = b
            .column(idx)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap_or_else(|| {
                panic!(
                    "column '{name}' should be decimal_arb storage (LargeBinary), got {:?}",
                    b.column(idx).data_type()
                )
            });
        for i in 0..c.len() {
            if c.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(
                    DecimalArbValue::from_canonical_bytes_at_scale(c.value(i), scale)
                        .expect("decimal_arb bytes must decode"),
                ));
            }
        }
    }
    out
}

/// Decode a decimal_arb output column using the *scale declared on the output
/// field*. Panics loudly if the metadata is gone — that is the F2 failure mode.
fn arb(batches: &[RecordBatch], name: &str) -> Vec<Option<DecimalArbValue>> {
    let f = out_field(batches, name);
    let (_, scale) = DecimalArbType::precision_scale_from_field(&f)
        .unwrap_or_else(|| panic!("output column '{name}' carries no decimal_arb metadata: {f:?}"));
    arb_at(batches, name, scale)
}

fn assert_arb_meta(batches: &[RecordBatch], name: &str, expect: (u32, u32), what: &str) {
    let f = out_field(batches, name);
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "{what}: output column '{name}' lost decimal_arb metadata (a sink would treat it as raw BYTEA): {f:?}"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some(expect),
        "{what}: output column '{name}' has the wrong (precision, scale)"
    );
}

fn some(vals: &[&str]) -> Vec<Option<DecimalArbValue>> {
    vals.iter()
        .map(|v| if *v == "!" { None } else { Some(dv(v)) })
        .collect()
}

// ===========================================================================
// A. BETWEEN — basics with integer bounds
// ===========================================================================

#[tokio::test]
async fn between_int_bounds_selects_only_rows_inside_the_range() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 0 AND 100").await;
    assert_eq!(
        ids(&b),
        vec![1, 3, 6],
        "BETWEEN over decimal_arb must keep exactly {{5, 0, 7}}"
    );
}

#[tokio::test]
async fn between_lower_bound_is_inclusive() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 5 AND 100").await;
    assert!(
        ids(&b).contains(&1),
        "BETWEEN must include a row equal to the LOW bound (amt = 5)"
    );
}

#[tokio::test]
async fn between_upper_bound_is_inclusive() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 0 AND 7").await;
    assert!(
        ids(&b).contains(&6),
        "BETWEEN must include a row equal to the HIGH bound (amt = 7)"
    );
}

#[tokio::test]
async fn between_equal_bounds_behaves_like_equality() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 5 AND 5").await;
    assert_eq!(ids(&b), vec![1], "BETWEEN x AND x must match only x");
}

#[tokio::test]
async fn between_reversed_bounds_returns_no_rows() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 100 AND 0").await;
    assert_eq!(
        ids(&b),
        Vec::<i64>::new(),
        "BETWEEN high AND low is an empty range in SQL"
    );
}

#[tokio::test]
async fn not_between_reversed_bounds_returns_every_non_null_row() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt NOT BETWEEN 100 AND 0").await;
    assert_eq!(
        ids(&b),
        vec![1, 2, 3, 4, 6],
        "NOT BETWEEN over an empty range must keep every non-NULL row"
    );
}

#[tokio::test]
async fn between_negative_bounds_select_negative_values() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN -10 AND -1").await;
    assert_eq!(
        ids(&b),
        vec![2],
        "BETWEEN with negative bounds must compare numerically, not bytewise \
         (0xFF sign byte sorts above 0x00)"
    );
}

#[tokio::test]
async fn between_spanning_zero_includes_both_signs() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN -3 AND 5").await;
    assert_eq!(
        ids(&b),
        vec![1, 2, 3],
        "a range spanning zero must include negative, zero and positive values"
    );
}

#[tokio::test]
async fn not_between_int_bounds_selects_outside_rows() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt NOT BETWEEN 0 AND 100").await;
    assert_eq!(ids(&b), vec![2, 4], "NOT BETWEEN must keep {{-3, 200}}");
}

#[tokio::test]
async fn between_and_not_between_partition_the_non_null_rows() {
    let sm = main_session();
    let inside = ids(&ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 0 AND 100").await);
    let outside = ids(&ok(&sm, "SELECT id FROM t WHERE amt NOT BETWEEN 0 AND 100").await);
    let mut all: Vec<i64> = inside.iter().chain(outside.iter()).copied().collect();
    all.sort_unstable();
    assert_eq!(
        all,
        vec![1, 2, 3, 4, 6],
        "BETWEEN and NOT BETWEEN must partition the non-NULL rows exactly once each"
    );
}

#[tokio::test]
async fn between_excludes_null_subject_rows() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN -100000 AND 100000").await;
    assert!(
        !ids(&b).contains(&5),
        "a NULL decimal_arb subject must not satisfy BETWEEN"
    );
}

#[tokio::test]
async fn not_between_excludes_null_subject_rows() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt NOT BETWEEN 0 AND 1").await;
    assert!(
        !ids(&b).contains(&5),
        "a NULL decimal_arb subject must not satisfy NOT BETWEEN either"
    );
}

#[tokio::test]
async fn between_in_projection_yields_boolean_with_null_for_null_subject() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, amt BETWEEN 0 AND 100 AS r FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        bools(&b, "r"),
        vec![
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(true)
        ],
        "BETWEEN in the SELECT list must be three-valued (NULL subject -> NULL)"
    );
}

#[tokio::test]
async fn not_between_in_projection_is_the_negation_of_between() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, amt BETWEEN 0 AND 100 AS a, amt NOT BETWEEN 0 AND 100 AS n FROM t ORDER BY id",
    )
    .await;
    let a = bools(&b, "a");
    let n = bools(&b, "n");
    for (i, (x, y)) in a.iter().zip(n.iter()).enumerate() {
        assert_eq!(
            *y,
            x.map(|v| !v),
            "row {i}: NOT BETWEEN must be the exact three-valued negation of BETWEEN"
        );
    }
}

#[tokio::test]
async fn between_filter_keeps_decimal_arb_metadata_on_projected_column() {
    let sm = main_session();
    let b = ok(&sm, "SELECT amt FROM t WHERE amt BETWEEN 0 AND 100").await;
    assert_arb_meta(&b, "amt", (100, 0), "BETWEEN filter");
}

#[tokio::test]
async fn between_filter_returns_correct_decoded_values() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT amt FROM t WHERE amt BETWEEN 0 AND 100 ORDER BY id",
    )
    .await;
    assert_eq!(
        arb(&b, "amt"),
        some(&["5", "0", "7"]),
        "rows kept by BETWEEN must decode to the original values"
    );
}

// ===========================================================================
// B. BETWEEN — bound types
// ===========================================================================

#[tokio::test]
#[ignore = "FINDING F-A: fractional and >i64 SQL literals arrive as Float64 (DataFusion's parse_float_as_decimal defaults to false), which DecimalArbExprRewrite::coerce refuses, so no decimal_arb column can be filtered by a fractional or >i64 literal"]
async fn between_decimal_literal_bounds_on_scale0_column() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 4.5 AND 5.5").await;
    assert_eq!(
        ids(&b),
        vec![1],
        "a Decimal128 bound must be widened to decimal_arb and compared numerically"
    );
}

#[tokio::test]
#[ignore = "FINDING F-A: fractional and >i64 SQL literals arrive as Float64 (DataFusion's parse_float_as_decimal defaults to false), which DecimalArbExprRewrite::coerce refuses, so no decimal_arb column can be filtered by a fractional or >i64 literal"]
async fn between_mixed_int_and_decimal_bounds() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 0 AND 7.5").await;
    assert_eq!(
        ids(&b),
        vec![1, 3, 6],
        "an Int64 low bound and a Decimal128 high bound must both coerce"
    );
}

#[tokio::test]
async fn between_int_bounds_on_scale2_column_compares_numerically() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt2 BETWEEN 0 AND 100").await;
    assert_eq!(
        ids(&b),
        vec![1, 3, 6],
        "scale-0 bounds against a scale-2 column must compare by value, not by raw bytes"
    );
}

#[tokio::test]
#[ignore = "FINDING F-A: fractional and >i64 SQL literals arrive as Float64 (DataFusion's parse_float_as_decimal defaults to false), which DecimalArbExprRewrite::coerce refuses, so no decimal_arb column can be filtered by a fractional or >i64 literal"]
async fn between_decimal_bounds_on_scale2_column() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt2 BETWEEN 4.99 AND 5.01").await;
    assert_eq!(ids(&b), vec![1], "5.00 must fall inside [4.99, 5.01]");
}

#[tokio::test]
#[ignore = "FINDING F-A: fractional and >i64 SQL literals arrive as Float64 (DataFusion's parse_float_as_decimal defaults to false), which DecimalArbExprRewrite::coerce refuses, so no decimal_arb column can be filtered by a fractional or >i64 literal"]
async fn between_fractional_bound_excludes_by_a_hundredth() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt2 BETWEEN 5.01 AND 6.00").await;
    assert_eq!(
        ids(&b),
        Vec::<i64>::new(),
        "5.00 must NOT fall inside [5.01, 6.00] — fractional bounds must not be truncated"
    );
}

#[tokio::test]
async fn between_bounds_from_decimal_arb_columns() {
    let sm = main_session();
    // alt is 999/888/1/2/3/4 — amt BETWEEN 0 AND alt keeps rows where amt <= alt.
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 0 AND alt").await;
    assert_eq!(
        ids(&b),
        vec![1, 3],
        "decimal_arb column bounds must pass through the coercion unchanged"
    );
}

#[tokio::test]
async fn between_bounds_from_cross_scale_decimal_arb_columns() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN amt2 AND amt2").await;
    assert_eq!(
        ids(&b),
        vec![1, 2, 3, 4, 6],
        "amt and amt2 hold identical numbers at different scales, so every non-NULL \
         row must satisfy amt BETWEEN amt2 AND amt2"
    );
}

#[tokio::test]
async fn between_bounds_from_int_column() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN id AND 300").await;
    assert_eq!(
        ids(&b),
        vec![1, 4, 6],
        "an Int64 *column* bound must coerce exactly like an Int64 literal"
    );
}

#[tokio::test]
async fn between_bound_expressions_are_evaluated() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 0 + 1 AND 2 * 4").await;
    assert_eq!(
        ids(&b),
        vec![1, 6],
        "arithmetic bound expressions must be coerced and evaluated (range [1, 8])"
    );
}

#[tokio::test]
#[ignore = "FINDING F-A: fractional and >i64 SQL literals arrive as Float64 (DataFusion's parse_float_as_decimal defaults to false), which DecimalArbExprRewrite::coerce refuses, so no decimal_arb column can be filtered by a fractional or >i64 literal"]
async fn between_bound_larger_than_i64_still_plans() {
    let sm = main_session();
    // 23 digits — beyond i64, lands as Decimal128.
    let b = ok(
        &sm,
        "SELECT id FROM t WHERE amt BETWEEN 0 AND 99999999999999999999999",
    )
    .await;
    assert_eq!(
        ids(&b),
        vec![1, 3, 4, 6],
        "a bound wider than i64 is the whole point of decimal_arb and must still work"
    );
}

#[tokio::test]
#[ignore = "FINDING F-A: fractional and >i64 SQL literals arrive as Float64 (DataFusion's parse_float_as_decimal defaults to false), which DecimalArbExprRewrite::coerce refuses, so no decimal_arb column can be filtered by a fractional or >i64 literal"]
async fn between_bound_larger_than_decimal128_still_plans() {
    let sm = main_session();
    // 40 digits — beyond Decimal128's 38-digit precision.
    let sql = "SELECT id FROM t WHERE amt BETWEEN 0 AND 9999999999999999999999999999999999999999";
    let b = ok(&sm, sql).await;
    assert_eq!(
        ids(&b),
        vec![1, 3, 4, 6],
        "a 40-digit bound must widen through the Decimal256 path"
    );
}

#[tokio::test]
async fn between_float_bound_is_rejected_not_silently_coerced() {
    let sm = main_session();
    let e = err(
        &sm,
        "SELECT id FROM t WHERE amt BETWEEN CAST(0.0 AS DOUBLE) AND CAST(100.0 AS DOUBLE)",
    )
    .await;
    assert!(
        !e.is_empty(),
        "a float bound must surface a DataFusion error (lossy float<->decimal is rejected \
         by design), not silently coerce"
    );
}

#[tokio::test]
async fn not_between_float_bound_is_rejected() {
    let sm = main_session();
    let e = err(
        &sm,
        "SELECT id FROM t WHERE amt NOT BETWEEN CAST(0.0 AS DOUBLE) AND CAST(100.0 AS DOUBLE)",
    )
    .await;
    assert!(!e.is_empty(), "NOT BETWEEN must reject float bounds too");
}

#[tokio::test]
async fn between_one_float_bound_is_rejected() {
    let sm = main_session();
    let e = err(
        &sm,
        "SELECT id FROM t WHERE amt BETWEEN 0 AND CAST(100.0 AS DOUBLE)",
    )
    .await;
    assert!(
        !e.is_empty(),
        "a single un-coercible bound must abort the whole rewrite, not half-coerce it"
    );
}

#[tokio::test]
async fn between_float_column_bound_is_rejected() {
    let sm = main_session();
    let e = err(&sm, "SELECT id FROM t WHERE amt BETWEEN 0 AND fl").await;
    assert!(
        !e.is_empty(),
        "a Float64 column bound must be rejected the same way a float literal is"
    );
}

#[tokio::test]
#[ignore = "FINDING F-B: a Utf8 bound/element makes the rewrite bail and DataFusion then byte-compares the ASCII text against the canonical encoding — the query succeeds and silently returns the wrong rows"]
async fn between_string_bound_must_not_silently_byte_compare() {
    let sm = main_session();
    // `coerce` returns None for Utf8, so the rewrite bails and DataFusion casts the
    // string bounds to LargeBinary — comparing the ASCII bytes of "0"/"100" against
    // canonical decimal_arb bytes. Either reject the query or compare numerically;
    // silently returning the wrong row set is not acceptable.
    match run(&sm, "SELECT id FROM t WHERE amt BETWEEN '0' AND '100'").await {
        Err(_) => {}
        Ok(b) => assert_eq!(
            ids(&b),
            vec![1, 3, 6],
            "string BETWEEN bounds over decimal_arb planned successfully but compared raw \
             bytes: the query silently returned the wrong rows instead of erroring"
        ),
    }
}

#[tokio::test]
#[ignore = "FINDING F-C: a NULL bound/element makes the rewrite bail (Null is not coercible) and TypeCoercion then rejects the whole predicate, so valid three-valued SQL fails to plan"]
async fn between_null_bound_yields_no_rows_not_a_planning_failure() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt BETWEEN NULL AND 100").await;
    assert_eq!(
        ids(&b),
        Vec::<i64>::new(),
        "`x BETWEEN NULL AND hi` is valid SQL (predicate is NULL -> no rows); it must not \
         fail to plan just because the subject is decimal_arb"
    );
}

#[tokio::test]
#[ignore = "FINDING F-C: a NULL bound/element makes the rewrite bail (Null is not coercible) and TypeCoercion then rejects the whole predicate, so valid three-valued SQL fails to plan"]
async fn between_null_bound_in_projection_is_null() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, amt BETWEEN 0 AND NULL AS r FROM t ORDER BY id",
    )
    .await;
    assert!(
        bools(&b, "r").iter().all(|v| *v != Some(true)),
        "a NULL bound can never make BETWEEN true"
    );
}

// ===========================================================================
// C. BETWEEN — subject variants and non-decimal_arb regressions
// ===========================================================================

#[tokio::test]
async fn between_over_decimal_arb_arithmetic_subject() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE (amt + alt) BETWEEN 0 AND 100").await;
    // amt+alt: 1004, 885, 1, 202, NULL, 11 -> ids 3 (1) and 6 (11) in [0, 100]
    assert_eq!(
        ids(&b),
        vec![3, 6],
        "a decimal_arb *expression* subject must be recognised by the rewrite"
    );
}

#[tokio::test]
async fn between_over_negated_decimal_arb_subject() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE (0 - amt) BETWEEN 0 AND 100").await;
    // -amt: -5, 3, 0, -200, NULL, -7 -> ids 2, 3
    assert_eq!(
        ids(&b),
        vec![2, 3],
        "negation via `0 - amt` must stay a decimal_arb subject"
    );
}

#[tokio::test]
async fn between_over_case_subject_is_rewritten() {
    let sm = main_session();
    let sql = "SELECT id FROM t WHERE (CASE WHEN flag = 1 THEN amt ELSE alt END) BETWEEN 0 AND 100";
    let b = ok(&sm, sql).await;
    // flag=1 -> amt (5, 0, NULL) ; flag=0 -> alt (888, 2, 4)
    assert_eq!(
        ids(&b),
        vec![1, 3, 4, 6],
        "a CASE subject re-stamped with decimal_arb metadata must be usable in BETWEEN"
    );
}

#[tokio::test]
async fn between_over_coalesce_subject_is_rewritten() {
    let sm = main_session();
    let sql = "SELECT id FROM t WHERE COALESCE(amt, alt) BETWEEN 0 AND 100";
    let b = ok(&sm, sql).await;
    // COALESCE: 5, -3, 0, 200, 3, 7 -> ids 1, 3, 5, 6
    assert_eq!(
        ids(&b),
        vec![1, 3, 5, 6],
        "a COALESCE subject re-stamped with decimal_arb metadata must be usable in BETWEEN"
    );
}

#[tokio::test]
async fn between_on_int_column_is_untouched() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE id BETWEEN 2 AND 3").await;
    assert_eq!(
        ids(&b),
        vec![2, 3],
        "the rewrite must not disturb BETWEEN over a plain Int64 column"
    );
}

#[tokio::test]
async fn between_on_float_column_is_untouched() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE fl BETWEEN 1.0 AND 6.0").await;
    assert_eq!(
        ids(&b),
        vec![1, 5],
        "the rewrite must not disturb BETWEEN over a Float64 column"
    );
}

#[tokio::test]
async fn between_on_string_column_is_untouched() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE txt BETWEEN '0' AND '5'").await;
    assert_eq!(
        ids(&b),
        vec![1, 3, 4],
        "the rewrite must not disturb BETWEEN over a Utf8 column"
    );
}

#[tokio::test]
async fn between_composes_with_and_or() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id FROM t WHERE amt BETWEEN 0 AND 10 AND flag = 1",
    )
    .await;
    assert_eq!(
        ids(&b),
        vec![1, 3],
        "the rewritten BETWEEN must compose with other predicates"
    );
}

#[tokio::test]
async fn nested_not_of_between_is_correct() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE NOT (amt BETWEEN 0 AND 100)").await;
    assert_eq!(
        ids(&b),
        vec![2, 4],
        "NOT(BETWEEN) must agree with NOT BETWEEN"
    );
}

// ===========================================================================
// D. IN / NOT IN
// ===========================================================================

#[tokio::test]
async fn in_int_literals_selects_matching_rows() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (5, 200)").await;
    assert_eq!(
        ids(&b),
        vec![1, 4],
        "IN over decimal_arb must match by value"
    );
}

#[tokio::test]
async fn not_in_int_literals_selects_complement() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt NOT IN (5, 200)").await;
    assert_eq!(
        ids(&b),
        vec![2, 3, 6],
        "NOT IN must keep the non-NULL complement"
    );
}

#[tokio::test]
async fn in_single_element_list() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (7)").await;
    assert_eq!(ids(&b), vec![6], "a one-element IN list must still rewrite");
}

#[tokio::test]
async fn not_in_single_element_list() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt NOT IN (7)").await;
    assert_eq!(ids(&b), vec![1, 2, 3, 4], "a one-element NOT IN list");
}

#[tokio::test]
async fn in_duplicate_elements_does_not_duplicate_rows() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (5, 5, 5, 200, 5)").await;
    assert_eq!(
        ids(&b),
        vec![1, 4],
        "duplicate IN elements must not duplicate result rows"
    );
}

#[tokio::test]
async fn not_in_duplicate_elements_is_stable() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt NOT IN (5, 5, 200)").await;
    assert_eq!(ids(&b), vec![2, 3, 6], "duplicate NOT IN elements");
}

#[tokio::test]
async fn in_with_no_matching_element_returns_empty() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (11, 12, 13)").await;
    assert_eq!(ids(&b), Vec::<i64>::new(), "no element matches -> no rows");
}

#[tokio::test]
async fn in_negative_literals() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (-3, -5)").await;
    assert_eq!(
        ids(&b),
        vec![2],
        "negative IN elements must compare numerically"
    );
}

#[tokio::test]
async fn in_zero_matches_positive_and_negative_zero_encodings() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (0)").await;
    assert_eq!(ids(&b), vec![3], "0 must match the canonical zero encoding");
}

#[tokio::test]
#[ignore = "FINDING F-A: fractional and >i64 SQL literals arrive as Float64 (DataFusion's parse_float_as_decimal defaults to false), which DecimalArbExprRewrite::coerce refuses, so no decimal_arb column can be filtered by a fractional or >i64 literal"]
async fn in_mixed_int_and_decimal_literals() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (5, 7.0, 999.5)").await;
    assert_eq!(
        ids(&b),
        vec![1, 6],
        "a list mixing Int64 and Decimal128 elements must coerce element-wise"
    );
}

#[tokio::test]
#[ignore = "FINDING F-A: fractional and >i64 SQL literals arrive as Float64 (DataFusion's parse_float_as_decimal defaults to false), which DecimalArbExprRewrite::coerce refuses, so no decimal_arb column can be filtered by a fractional or >i64 literal"]
async fn in_decimal_literals_against_scale2_column() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt2 IN (5.00, 7)").await;
    assert_eq!(
        ids(&b),
        vec![1, 6],
        "cross-scale IN elements must compare by value (5.00 == 5.00, 7 == 7.00)"
    );
}

#[tokio::test]
async fn in_int_literal_against_scale2_column_matches_by_value() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt2 IN (5)").await;
    assert_eq!(
        ids(&b),
        vec![1],
        "5 (scale 0) must equal 5.00 (scale 2) — raw byte equality would say no"
    );
}

#[tokio::test]
#[ignore = "FINDING F-A: fractional and >i64 SQL literals arrive as Float64 (DataFusion's parse_float_as_decimal defaults to false), which DecimalArbExprRewrite::coerce refuses, so no decimal_arb column can be filtered by a fractional or >i64 literal"]
async fn in_bound_larger_than_i64() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id FROM t WHERE amt IN (200, 99999999999999999999999)",
    )
    .await;
    assert_eq!(ids(&b), vec![4], "a >i64 IN element must still coerce");
}

#[tokio::test]
async fn in_large_literal_list_of_100_elements() {
    let sm = main_session();
    let list: Vec<String> = (1..=100).map(|i| i.to_string()).collect();
    let sql = format!("SELECT id FROM t WHERE amt IN ({})", list.join(", "));
    let b = ok(&sm, &sql).await;
    assert_eq!(
        ids(&b),
        vec![1, 6],
        "a 100-element IN list must fold into a correct OR chain"
    );
}

#[tokio::test]
async fn not_in_large_literal_list_of_100_elements() {
    let sm = main_session();
    let list: Vec<String> = (1..=100).map(|i| i.to_string()).collect();
    let sql = format!("SELECT id FROM t WHERE amt NOT IN ({})", list.join(", "));
    let b = ok(&sm, &sql).await;
    assert_eq!(
        ids(&b),
        vec![2, 3, 4],
        "a 100-element NOT IN list must fold into a correct AND chain"
    );
}

#[tokio::test]
async fn in_excludes_null_subject_rows() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (5, 200, 0, -3, 7)").await;
    assert!(!ids(&b).contains(&5), "a NULL subject can never satisfy IN");
}

#[tokio::test]
async fn not_in_excludes_null_subject_rows() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt NOT IN (11)").await;
    assert!(
        !ids(&b).contains(&5),
        "a NULL subject can never satisfy NOT IN"
    );
}

#[tokio::test]
#[ignore = "FINDING F-C: a NULL bound/element makes the rewrite bail (Null is not coercible) and TypeCoercion then rejects the whole predicate, so valid three-valued SQL fails to plan"]
async fn in_list_containing_null_still_matches_a_real_element() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (5, NULL)").await;
    assert_eq!(
        ids(&b),
        vec![1],
        "`x IN (5, NULL)` is TRUE when x = 5 (SQL three-valued IN); a NULL element must \
         not make the whole predicate unplannable"
    );
}

#[tokio::test]
#[ignore = "FINDING F-C: a NULL bound/element makes the rewrite bail (Null is not coercible) and TypeCoercion then rejects the whole predicate, so valid three-valued SQL fails to plan"]
async fn in_list_containing_null_yields_null_for_non_matching_rows() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id, amt IN (5, NULL) AS r FROM t ORDER BY id").await;
    let r = bools(&b, "r");
    assert_eq!(
        r[0],
        Some(true),
        "amt = 5 must be TRUE even with a NULL in the list"
    );
    assert_eq!(
        r[1], None,
        "a non-matching row with a NULL in the list must be NULL, not FALSE"
    );
}

#[tokio::test]
#[ignore = "FINDING F-C: a NULL bound/element makes the rewrite bail (Null is not coercible) and TypeCoercion then rejects the whole predicate, so valid three-valued SQL fails to plan"]
async fn not_in_list_containing_null_returns_no_rows() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt NOT IN (5, NULL)").await;
    assert_eq!(
        ids(&b),
        Vec::<i64>::new(),
        "`x NOT IN (.., NULL)` is never TRUE in SQL"
    );
}

#[tokio::test]
async fn in_float_element_is_rejected() {
    let sm = main_session();
    let e = err(&sm, "SELECT id FROM t WHERE amt IN (CAST(5.0 AS DOUBLE))").await;
    assert!(
        !e.is_empty(),
        "a float IN element must surface an error, not a silent lossy compare"
    );
}

#[tokio::test]
async fn in_one_float_element_aborts_the_whole_rewrite() {
    let sm = main_session();
    let e = err(
        &sm,
        "SELECT id FROM t WHERE amt IN (5, CAST(7.0 AS DOUBLE))",
    )
    .await;
    assert!(
        !e.is_empty(),
        "one un-coercible element must abort the rewrite for the whole list"
    );
}

#[tokio::test]
#[ignore = "FINDING F-B: a Utf8 bound/element makes the rewrite bail and DataFusion then byte-compares the ASCII text against the canonical encoding — the query succeeds and silently returns the wrong rows"]
async fn in_string_element_must_not_silently_byte_compare() {
    let sm = main_session();
    match run(&sm, "SELECT id FROM t WHERE amt IN ('5')").await {
        Err(_) => {}
        Ok(b) => assert_eq!(
            ids(&b),
            vec![1],
            "a string IN element over decimal_arb planned successfully but compared the \
             ASCII bytes of '5' against the canonical encoding: the query silently returned \
             the wrong rows instead of erroring"
        ),
    }
}

#[tokio::test]
async fn in_list_of_decimal_arb_columns() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (alt, small)").await;
    assert_eq!(
        ids(&b),
        vec![1, 2, 3, 4, 6],
        "`small` holds the same values as `amt`, so every non-NULL row must match"
    );
}

#[tokio::test]
async fn in_list_of_cross_scale_decimal_arb_columns() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IN (amt2)").await;
    assert_eq!(
        ids(&b),
        vec![1, 2, 3, 4, 6],
        "amt2 holds the same numbers at scale 2; IN must compare by value, not bytes"
    );
}

#[tokio::test]
async fn not_in_list_of_decimal_arb_columns() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt NOT IN (alt)").await;
    assert_eq!(
        ids(&b),
        vec![1, 2, 3, 4, 6],
        "no row has amt = alt, so NOT IN keeps every non-NULL row"
    );
}

#[tokio::test]
async fn in_filter_keeps_decimal_arb_metadata_on_projected_column() {
    let sm = main_session();
    let b = ok(&sm, "SELECT amt FROM t WHERE amt IN (5, 200)").await;
    assert_arb_meta(&b, "amt", (100, 0), "IN filter");
}

#[tokio::test]
async fn in_filter_returns_correct_decoded_values() {
    let sm = main_session();
    let b = ok(&sm, "SELECT amt FROM t WHERE amt IN (5, 200) ORDER BY id").await;
    assert_eq!(
        arb(&b, "amt"),
        some(&["5", "200"]),
        "rows kept by IN must decode to the original values"
    );
}

#[tokio::test]
async fn in_in_projection_yields_boolean_with_null_for_null_subject() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id, amt IN (5, 7) AS r FROM t ORDER BY id").await;
    assert_eq!(
        bools(&b, "r"),
        vec![
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(true)
        ],
        "IN in the SELECT list must be three-valued"
    );
}

#[tokio::test]
async fn not_in_in_projection_is_the_negation_of_in() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, amt IN (5, 7) AS a, amt NOT IN (5, 7) AS n FROM t ORDER BY id",
    )
    .await;
    let a = bools(&b, "a");
    let n = bools(&b, "n");
    for (i, (x, y)) in a.iter().zip(n.iter()).enumerate() {
        assert_eq!(
            *y,
            x.map(|v| !v),
            "row {i}: NOT IN must be the exact three-valued negation of IN"
        );
    }
}

#[tokio::test]
async fn in_on_int_column_is_untouched() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE id IN (2, 3)").await;
    assert_eq!(
        ids(&b),
        vec![2, 3],
        "the rewrite must not disturb IN over Int64"
    );
}

#[tokio::test]
async fn in_on_string_column_is_untouched() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE txt IN ('5', 'x')").await;
    assert_eq!(
        ids(&b),
        vec![1, 5],
        "the rewrite must not disturb IN over Utf8"
    );
}

#[tokio::test]
async fn in_on_float_column_is_untouched() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE fl IN (1.5, 7.0)").await;
    assert_eq!(
        ids(&b),
        vec![5, 6],
        "the rewrite must not disturb IN over Float64"
    );
}

#[tokio::test]
async fn in_subquery_same_scale_matches_by_value() {
    let sm = session();
    register_main(&sm);
    register_probe_tables(&sm);
    // `IN (subquery)` lowers to a semi-join, which this session's physical planner
    // rejects wholesale ("unsupported PartitionMode Auto") — for Int64 just as much
    // as for decimal_arb. Assert the decimal_arb path is never *worse* than the
    // Int64 path, so this test starts checking real semantics the day joins work.
    let control = run(&sm, "SELECT id FROM t WHERE id IN (SELECT id FROM t)").await;
    let got = run(&sm, "SELECT id FROM t WHERE amt IN (SELECT v FROM u)").await;
    if control.is_err() {
        assert!(
            got.is_err(),
            "IN (subquery) is unsupported for Int64 in this session, so it must not \
             silently succeed with decimal_arb either"
        );
        return;
    }
    let b = got.expect("IN (subquery) works for Int64, so it must work for decimal_arb");
    assert_eq!(
        ids(&b),
        vec![1, 4],
        "IN (subquery) over same-scale decimal_arb must match 5 and 200"
    );
}

#[tokio::test]
async fn in_subquery_cross_scale_matches_by_value() {
    let sm = session();
    register_main(&sm);
    register_probe_tables(&sm);
    let control = run(&sm, "SELECT id FROM t WHERE id IN (SELECT id FROM t)").await;
    let got = run(&sm, "SELECT id FROM t WHERE amt IN (SELECT v FROM u2)").await;
    if control.is_err() {
        assert!(
            got.is_err(),
            "IN (subquery) is unsupported for Int64 in this session, so it must not \
             silently succeed with decimal_arb either"
        );
        return;
    }
    let b = got.expect("IN (subquery) works for Int64, so it must work for decimal_arb");
    assert_eq!(
        ids(&b),
        vec![1, 4],
        "u2 holds 5.00/200.00 at scale 2 — numerically identical to amt's 5/200. \
         IN (subquery) must compare decimal_arb by value; a raw LargeBinary compare \
         silently drops both rows"
    );
}

#[tokio::test]
async fn not_in_subquery_same_scale_is_complement() {
    let sm = session();
    register_main(&sm);
    register_probe_tables(&sm);
    let b = ok(&sm, "SELECT id FROM t WHERE amt NOT IN (SELECT v FROM u)").await;
    assert_eq!(
        ids(&b),
        vec![2, 3, 6],
        "NOT IN (subquery) must be the complement over non-NULL rows"
    );
}

// ===========================================================================
// E. CASE
// ===========================================================================

#[tokio::test]
async fn searched_case_over_decimal_arb_keeps_metadata() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE WHEN flag = 1 THEN amt ELSE alt END AS c FROM t ORDER BY id",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "searched CASE");
}

#[tokio::test]
async fn searched_case_over_decimal_arb_selects_right_branch_values() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE WHEN flag = 1 THEN amt ELSE alt END AS c FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "888", "0", "2", "!", "4"]),
        "CASE must pick THEN when flag = 1 and ELSE otherwise"
    );
}

#[tokio::test]
async fn case_precision_is_the_max_of_the_branches() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT CASE WHEN flag = 1 THEN small ELSE amt END AS c FROM t",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "CASE over decimal_arb(20,0)/(100,0)");
}

#[tokio::test]
async fn case_precision_max_is_order_independent() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT CASE WHEN flag = 1 THEN amt ELSE small END AS c FROM t",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "CASE with branches swapped");
}

#[tokio::test]
async fn case_without_else_keeps_metadata() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE WHEN flag = 1 THEN amt END AS c FROM t ORDER BY id",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "CASE without ELSE");
}

#[tokio::test]
async fn case_without_else_yields_null_for_unmatched_rows() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE WHEN flag = 1 THEN amt END AS c FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "!", "0", "!", "!", "!"]),
        "an unmatched CASE without ELSE must be NULL"
    );
}

#[tokio::test]
async fn case_with_explicit_else_null_keeps_metadata() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE WHEN flag = 1 THEN amt ELSE NULL END AS c FROM t ORDER BY id",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "CASE ... ELSE NULL");
}

#[tokio::test]
async fn case_with_explicit_else_null_yields_correct_values() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE WHEN flag = 1 THEN amt ELSE NULL END AS c FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "!", "0", "!", "!", "!"]),
        "ELSE NULL must not disturb the THEN values"
    );
}

#[tokio::test]
async fn case_with_null_then_branch_keeps_metadata_from_the_other_branch() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE WHEN flag = 1 THEN NULL ELSE amt END AS c FROM t ORDER BY id",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "CASE with a NULL THEN branch");
}

#[tokio::test]
async fn case_with_null_then_branch_yields_correct_values() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE WHEN flag = 1 THEN NULL ELSE amt END AS c FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        arb(&b, "c"),
        some(&["!", "-3", "!", "200", "!", "7"]),
        "a NULL THEN branch must not corrupt the ELSE values"
    );
}

#[tokio::test]
async fn case_with_all_null_branches_is_null_typed_not_decimal_arb() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE WHEN flag = 1 THEN NULL ELSE NULL END AS c FROM t ORDER BY id",
    )
    .await;
    let f = out_field(&b, "c");
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "an all-NULL CASE constrains nothing and must not be stamped as decimal_arb: {f:?}"
    );
}

#[tokio::test]
async fn case_with_eight_when_branches_keeps_metadata() {
    let sm = main_session();
    let sql = "SELECT CASE \
        WHEN id = 1 THEN amt WHEN id = 2 THEN alt WHEN id = 3 THEN small \
        WHEN id = 4 THEN amt WHEN id = 5 THEN alt WHEN id = 6 THEN small \
        WHEN id = 7 THEN amt WHEN id = 8 THEN alt ELSE small END AS c FROM t";
    let b = ok(&sm, sql).await;
    assert_arb_meta(&b, "c", (100, 0), "CASE with 8 WHEN branches");
}

#[tokio::test]
async fn case_with_eight_when_branches_selects_correct_values() {
    let sm = main_session();
    let sql = "SELECT id, CASE \
        WHEN id = 1 THEN amt WHEN id = 2 THEN alt WHEN id = 3 THEN small \
        WHEN id = 4 THEN amt WHEN id = 5 THEN alt WHEN id = 6 THEN small \
        WHEN id = 7 THEN amt WHEN id = 8 THEN alt ELSE small END AS c FROM t ORDER BY id";
    let b = ok(&sm, sql).await;
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "888", "0", "200", "3", "7"]),
        "each WHEN branch must select from its own column"
    );
}

#[tokio::test]
#[ignore = "FINDING F-E: mixed-scale CASE/COALESCE branches are 'left untouched', which emits a metadata-less LargeBinary column whose rows use two different encodings — silent data corruption at the sink instead of an error"]
async fn mixed_scale_case_must_not_produce_an_undecodable_column() {
    // amt is scale 0, amt2 is scale 2 — the canonical bytes carry no scale, so a
    // column mixing both encodings cannot be decoded at ANY single scale. The
    // rewrite refuses to stamp metadata here; the query must therefore fail
    // rather than hand a sink a metadata-less LargeBinary column.
    let sm = main_session();
    let sql = "SELECT id, CASE WHEN flag = 1 THEN amt ELSE amt2 END AS c FROM t ORDER BY id";
    match run(&sm, sql).await {
        Err(_) => {}
        Ok(b) => {
            let f = out_field(&b, "c");
            assert!(
                DecimalArbType::is_decimal_arb_field(&f),
                "mixed-scale CASE returned a bare LargeBinary column with no decimal_arb \
                 metadata: a Postgres sink writes it as BYTEA and a JSON sink renders hex. \
                 Field: {f:?}"
            );
        }
    }
}

#[tokio::test]
#[ignore = "FINDING F-E: mixed-scale CASE/COALESCE branches are 'left untouched', which emits a metadata-less LargeBinary column whose rows use two different encodings — silent data corruption at the sink instead of an error"]
async fn mixed_scale_case_values_cannot_be_read_back_at_any_single_scale() {
    let sm = main_session();
    let sql = "SELECT id, CASE WHEN flag = 1 THEN amt ELSE amt2 END AS c FROM t ORDER BY id";
    let Ok(b) = run(&sm, sql).await else {
        return; // rejecting the query is the acceptable outcome
    };
    // Expected numbers, whichever encoding wins: 5, -3, 0, 200, NULL, 7.
    let expected = some(&["5", "-3", "0", "200", "!", "7"]);
    let scale0 = arb_at(&b, "c", 0);
    let scale2 = arb_at(&b, "c", 2);
    assert!(
        scale0 == expected || scale2 == expected,
        "mixed-scale CASE produced a column that decodes correctly at NEITHER scale 0 \
         ({scale0:?}) NOR scale 2 ({scale2:?}); expected {expected:?}. Every consumer of \
         this column gets numerically wrong values."
    );
}

#[tokio::test]
#[ignore = "FINDING F-D: CASE/COALESCE mixing a decimal_arb branch with an integer literal branch is not coerced by the rewrite and fails to plan"]
async fn case_with_int_literal_else_branch_is_usable() {
    // `CASE WHEN c THEN <decimal column> ELSE 0 END` is everyday SQL.
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE WHEN flag = 1 THEN amt ELSE 0 END AS c FROM t ORDER BY id",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "CASE with an Int64 ELSE branch");
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "0", "0", "0", "!", "0"]),
        "the Int64 ELSE branch must be widened to decimal_arb"
    );
}

#[tokio::test]
async fn simple_case_over_int_subject_keeps_metadata() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE flag WHEN 1 THEN amt ELSE alt END AS c FROM t ORDER BY id",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "simple CASE over an Int64 subject");
}

#[tokio::test]
async fn simple_case_over_int_subject_selects_correct_values() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE flag WHEN 1 THEN amt ELSE alt END AS c FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "888", "0", "2", "!", "4"]),
        "simple CASE must select the same values as the searched form"
    );
}

#[tokio::test]
#[ignore = "FINDING F-K: the simple CASE form `CASE <decimal_arb> WHEN <int> ...` is not reached by the rewrite and fails to plan, unlike the searched form"]
async fn simple_case_over_decimal_arb_subject_plans() {
    // `CASE amt WHEN 5 THEN ... END` desugars to an equality against the
    // decimal_arb subject — the same LargeBinary-vs-Int64 mismatch BETWEEN/IN
    // needed the rewrite for.
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, CASE amt WHEN 5 THEN 1 ELSE 0 END AS c FROM t ORDER BY id",
    )
    .await;
    let idx = b[0].schema().index_of("c").unwrap();
    let c = b[0]
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("c should be Int64");
    assert_eq!(
        c.value(0),
        1,
        "`CASE amt WHEN 5 ...` must match the row whose amt is 5"
    );
}

#[tokio::test]
async fn nested_case_keeps_metadata() {
    let sm = main_session();
    let sql = "SELECT id, CASE WHEN flag = 1 \
                 THEN CASE WHEN id = 1 THEN amt ELSE alt END \
                 ELSE small END AS c FROM t ORDER BY id";
    let b = ok(&sm, sql).await;
    assert_arb_meta(&b, "c", (100, 0), "nested CASE");
}

#[tokio::test]
async fn nested_case_selects_correct_values() {
    let sm = main_session();
    let sql = "SELECT id, CASE WHEN flag = 1 \
                 THEN CASE WHEN id = 1 THEN amt ELSE alt END \
                 ELSE small END AS c FROM t ORDER BY id";
    let b = ok(&sm, sql).await;
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "-3", "1", "200", "3", "7"]),
        "the nested CASE must resolve inner-then-outer"
    );
}

#[tokio::test]
async fn triple_nested_case_keeps_metadata() {
    let sm = main_session();
    let sql = "SELECT CASE WHEN flag = 1 THEN \
                 CASE WHEN id = 1 THEN CASE WHEN id > 0 THEN amt ELSE alt END ELSE alt END \
                 ELSE small END AS c FROM t";
    let b = ok(&sm, sql).await;
    assert_arb_meta(&b, "c", (100, 0), "triple-nested CASE");
}

#[tokio::test]
async fn case_inside_coalesce_keeps_metadata() {
    let sm = main_session();
    let sql = "SELECT COALESCE(CASE WHEN flag = 1 THEN amt END, alt) AS c FROM t";
    let b = ok(&sm, sql).await;
    assert_arb_meta(&b, "c", (100, 0), "CASE inside COALESCE");
}

#[tokio::test]
async fn coalesce_inside_case_keeps_metadata() {
    let sm = main_session();
    let sql = "SELECT CASE WHEN flag = 1 THEN COALESCE(amt, alt) ELSE small END AS c FROM t";
    let b = ok(&sm, sql).await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE inside CASE");
}

#[tokio::test]
async fn case_over_int_column_stays_int64() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT CASE WHEN flag = 1 THEN id ELSE 0 END AS c FROM t",
    )
    .await;
    let f = out_field(&b, "c");
    assert_eq!(
        f.data_type(),
        &DataType::Int64,
        "CASE over Int64 must stay Int64"
    );
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "an Int64 CASE must never be stamped as decimal_arb"
    );
}

#[tokio::test]
async fn case_over_string_column_stays_utf8() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT CASE WHEN flag = 1 THEN txt ELSE 'z' END AS c FROM t",
    )
    .await;
    let f = out_field(&b, "c");
    assert!(
        matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8),
        "CASE over Utf8 must stay a string type, got {:?}",
        f.data_type()
    );
}

#[tokio::test]
async fn case_over_float_column_stays_float() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT CASE WHEN flag = 1 THEN fl ELSE 0.0 END AS c FROM t",
    )
    .await;
    let f = out_field(&b, "c");
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "a float CASE must never be stamped as decimal_arb: {f:?}"
    );
}

#[tokio::test]
async fn case_with_decimal_arb_condition_and_branches() {
    let sm = main_session();
    let sql = "SELECT id, CASE WHEN amt > 0 THEN amt ELSE alt END AS c FROM t ORDER BY id";
    let b = ok(&sm, sql).await;
    assert_arb_meta(&b, "c", (100, 0), "CASE with a decimal_arb condition");
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "888", "1", "200", "3", "7"]),
        "the decimal_arb comparison in WHEN must drive branch selection"
    );
}

#[tokio::test]
#[ignore = "FINDING F-I: the metadata stamp lands in the analyzer, after the ExprPlanner has run, so a CASE/COALESCE result cannot be used in decimal_arb arithmetic (hard error) and comparisons against it silently byte-compare"]
async fn case_result_is_usable_in_decimal_arb_arithmetic() {
    let sm = main_session();
    let sql = "SELECT id, (CASE WHEN flag = 1 THEN amt ELSE alt END) + amt AS c \
               FROM t ORDER BY id";
    let b = ok(&sm, sql).await;
    assert_eq!(
        arb(&b, "c"),
        some(&["10", "885", "0", "202", "!", "11"]),
        "a re-stamped CASE result must behave as a first-class decimal_arb operand"
    );
}

#[tokio::test]
#[ignore = "FINDING F-I: the metadata stamp lands in the analyzer, after the ExprPlanner has run, so a CASE/COALESCE result cannot be used in decimal_arb arithmetic (hard error) and comparisons against it silently byte-compare"]
async fn case_result_is_usable_in_a_decimal_arb_comparison() {
    let sm = main_session();
    let sql = "SELECT id FROM t WHERE (CASE WHEN flag = 1 THEN amt ELSE alt END) > amt";
    let b = ok(&sm, sql).await;
    // flag=1 rows compare amt > amt (false); flag=0 rows compare alt > amt:
    // row 2 (888 > -3) true, row 4 (2 > 200) false, row 6 (4 > 7) false.
    assert_eq!(
        ids(&b),
        vec![2],
        "a re-stamped CASE result must be comparable against a decimal_arb column \
         *numerically*; a raw LargeBinary compare ranks the 0xFF-signed -3 above 888 \
         and silently drops the row"
    );
}

#[tokio::test]
async fn case_result_groups_numerically() {
    let sm = main_session();
    let sql = "SELECT CASE WHEN flag = 1 THEN amt ELSE amt END AS c, COUNT(*) AS n \
               FROM t GROUP BY 1";
    let b = ok(&sm, sql).await;
    let total: i64 = {
        let idx = b[0].schema().index_of("n").unwrap();
        let mut s = 0;
        for batch in &b {
            let c = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..c.len() {
                s += c.value(i);
            }
        }
        s
    };
    assert_eq!(total, 6, "GROUP BY over a CASE result must not lose rows");
}

#[tokio::test]
async fn case_result_orders_numerically_across_signs() {
    let sm = main_session();
    let sql = "SELECT CASE WHEN flag = 1 THEN amt ELSE amt END AS c FROM t \
               WHERE amt IS NOT NULL ORDER BY c ASC";
    let b = ok(&sm, sql).await;
    assert_eq!(
        arb(&b, "c"),
        some(&["-3", "0", "5", "7", "200"]),
        "ORDER BY a decimal_arb CASE result must sort numerically; a bytewise sort puts \
         the negative (0xFF sign byte) last"
    );
}

#[tokio::test]
async fn case_in_where_clause_with_between_and_in_combined() {
    let sm = main_session();
    let sql = "SELECT id FROM t \
               WHERE (CASE WHEN flag = 1 THEN amt ELSE alt END) BETWEEN 0 AND 10 \
                 AND amt IN (5, 0, 7, 200)";
    let b = ok(&sm, sql).await;
    // CASE: 5, 888, 0, 2, NULL, 4 -> in [0,10]: ids 1, 3, 4, 6; all of those
    // rows also have amt in (5, 0, 7, 200).
    assert_eq!(
        ids(&b),
        vec![1, 3, 4, 6],
        "BETWEEN over a CASE subject must compose with IN on the same row"
    );
}

// ===========================================================================
// F. COALESCE
// ===========================================================================

#[tokio::test]
async fn coalesce_two_decimal_arb_args_keeps_metadata() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(amt, alt) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE/2");
}

#[tokio::test]
async fn coalesce_two_decimal_arb_args_returns_first_non_null() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id, COALESCE(amt, alt) AS c FROM t ORDER BY id").await;
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "-3", "0", "200", "3", "7"]),
        "COALESCE must return the first non-NULL argument"
    );
}

#[tokio::test]
async fn coalesce_three_args_keeps_metadata() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(amt, alt, small) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE/3");
}

#[tokio::test]
async fn coalesce_four_args_keeps_metadata() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(amt, alt, small, amt) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE/4");
}

#[tokio::test]
async fn coalesce_five_args_keeps_metadata() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT COALESCE(amt, alt, small, amt, alt) AS c FROM t",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE/5");
}

#[tokio::test]
async fn coalesce_six_args_keeps_metadata() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT COALESCE(amt, alt, small, amt, alt, small) AS c FROM t",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE/6");
}

#[tokio::test]
async fn coalesce_seven_args_keeps_metadata() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT COALESCE(amt, alt, small, amt, alt, small, amt) AS c FROM t",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE/7");
}

#[tokio::test]
async fn coalesce_eight_args_keeps_metadata_and_values() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, COALESCE(amt, alt, small, amt, alt, small, amt, alt) AS c \
         FROM t ORDER BY id",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE/8");
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "-3", "0", "200", "3", "7"]),
        "an 8-argument COALESCE must still return the first non-NULL value"
    );
}

#[tokio::test]
async fn coalesce_single_arg_keeps_metadata() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(amt) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE/1");
}

#[tokio::test]
async fn coalesce_precision_is_the_max_of_the_args() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(small, amt) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE(decimal_arb(20,0), (100,0))");
}

#[tokio::test]
async fn coalesce_precision_max_is_order_independent() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(amt, small) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE with args swapped");
}

#[tokio::test]
async fn coalesce_all_narrow_args_keeps_the_narrow_precision() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(small, small) AS c FROM t").await;
    assert_arb_meta(&b, "c", (20, 0), "COALESCE over two decimal_arb(20,0) args");
}

#[tokio::test]
#[ignore = "FINDING F-D2: COALESCE(decimal_arb, NULL) fails to plan inside DataFusion's coalesce coercion, so the rewrite's 'a NULL branch does not constrain' path is unreachable for COALESCE (it does work for CASE)"]
async fn coalesce_with_trailing_null_literal_keeps_metadata() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(amt, NULL) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE(x, NULL)");
}

#[tokio::test]
#[ignore = "FINDING F-D2: COALESCE(decimal_arb, NULL) fails to plan inside DataFusion's coalesce coercion, so the rewrite's 'a NULL branch does not constrain' path is unreachable for COALESCE (it does work for CASE)"]
async fn coalesce_with_leading_null_literal_keeps_metadata() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, COALESCE(NULL, amt) AS c FROM t ORDER BY id",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE(NULL, x)");
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "-3", "0", "200", "!", "7"]),
        "a leading NULL literal must not change the result values"
    );
}

#[tokio::test]
#[ignore = "FINDING F-D: CASE/COALESCE mixing a decimal_arb branch with an integer literal branch is not coerced by the rewrite and fails to plan"]
async fn coalesce_with_int_literal_fallback_is_usable() {
    // `COALESCE(<decimal column>, 0)` is everyday SQL.
    let sm = main_session();
    let b = ok(&sm, "SELECT id, COALESCE(amt, 0) AS c FROM t ORDER BY id").await;
    assert_arb_meta(&b, "c", (100, 0), "COALESCE(decimal_arb, 0)");
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "-3", "0", "200", "0", "7"]),
        "the Int64 fallback must be widened to decimal_arb, filling the NULL row with 0"
    );
}

#[tokio::test]
#[ignore = "FINDING F-E: mixed-scale CASE/COALESCE branches are 'left untouched', which emits a metadata-less LargeBinary column whose rows use two different encodings — silent data corruption at the sink instead of an error"]
async fn mixed_scale_coalesce_must_not_produce_an_undecodable_column() {
    let sm = main_session();
    let sql = "SELECT id, COALESCE(amt, amt2) AS c FROM t ORDER BY id";
    match run(&sm, sql).await {
        Err(_) => {}
        Ok(b) => {
            let f = out_field(&b, "c");
            assert!(
                DecimalArbType::is_decimal_arb_field(&f),
                "mixed-scale COALESCE returned a bare LargeBinary column with no decimal_arb \
                 metadata; a sink writes it as BYTEA. Field: {f:?}"
            );
        }
    }
}

#[tokio::test]
async fn mixed_scale_coalesce_values_cannot_be_read_back_at_any_single_scale() {
    let sm = main_session();
    let sql = "SELECT id, COALESCE(amt, amt2) AS c FROM t ORDER BY id";
    let Ok(b) = run(&sm, sql).await else {
        return;
    };
    let expected = some(&["5", "-3", "0", "200", "!", "7"]);
    let scale0 = arb_at(&b, "c", 0);
    let scale2 = arb_at(&b, "c", 2);
    assert!(
        scale0 == expected || scale2 == expected,
        "mixed-scale COALESCE decodes correctly at NEITHER scale 0 ({scale0:?}) NOR scale 2 \
         ({scale2:?}); expected {expected:?}"
    );
}

#[tokio::test]
async fn coalesce_uppercase_and_mixed_case_spelling_both_stamp_metadata() {
    let sm = main_session();
    let upper = ok(&sm, "SELECT COALESCE(amt, alt) AS c FROM t").await;
    let mixed = ok(&sm, "SELECT CoAlEsCe(amt, alt) AS c FROM t").await;
    assert_arb_meta(&upper, "c", (100, 0), "COALESCE (upper)");
    assert_arb_meta(&mixed, "c", (100, 0), "CoAlEsCe (mixed case)");
}

#[tokio::test]
async fn nested_coalesce_keeps_metadata() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT COALESCE(COALESCE(amt, alt), small) AS c FROM t",
    )
    .await;
    assert_arb_meta(&b, "c", (100, 0), "nested COALESCE");
}

#[tokio::test]
async fn coalesce_over_int_column_stays_int64() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(id, flag) AS c FROM t").await;
    let f = out_field(&b, "c");
    assert_eq!(
        f.data_type(),
        &DataType::Int64,
        "COALESCE over Int64 must stay Int64"
    );
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "an Int64 COALESCE must never be stamped as decimal_arb"
    );
}

#[tokio::test]
async fn coalesce_over_string_column_is_untouched() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(txt, 'z') AS c FROM t").await;
    let f = out_field(&b, "c");
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "a Utf8 COALESCE must never be stamped as decimal_arb: {f:?}"
    );
}

#[tokio::test]
async fn coalesce_of_all_null_literals_is_not_decimal_arb() {
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(NULL, NULL) AS c FROM t").await;
    let f = out_field(&b, "c");
    assert!(
        !DecimalArbType::is_decimal_arb_field(&f),
        "COALESCE(NULL, NULL) constrains nothing and must not be stamped: {f:?}"
    );
}

#[tokio::test]
#[ignore = "FINDING F-I: the metadata stamp lands in the analyzer, after the ExprPlanner has run, so a CASE/COALESCE result cannot be used in decimal_arb arithmetic (hard error) and comparisons against it silently byte-compare"]
async fn coalesce_result_is_usable_in_decimal_arb_arithmetic() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, COALESCE(amt, alt) + small AS c FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        arb(&b, "c"),
        some(&["10", "-6", "0", "400", "!", "14"]),
        "a re-stamped COALESCE result must be a first-class decimal_arb operand"
    );
}

#[tokio::test]
async fn coalesce_result_is_usable_in_in_list() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE COALESCE(amt, alt) IN (3, 5)").await;
    assert_eq!(
        ids(&b),
        vec![1, 5],
        "IN over a COALESCE subject must see decimal_arb metadata"
    );
}

#[tokio::test]
#[ignore = "FINDING F-J: coalesce_meta still only accepts FixedSizeBinary(32) (the retired U256 storage) and errors on decimal_arb's LargeBinary"]
async fn coalesce_meta_udf_preserves_decimal_arb_metadata() {
    // `coalesce_meta` is a separate Streamling UDF that promises to keep the
    // first argument's field metadata; it must not regress for decimal_arb.
    let sm = main_session();
    let b = ok(&sm, "SELECT coalesce_meta(amt, alt) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "coalesce_meta");
}

// ===========================================================================
// G. Neighbouring conditional functions the rewrite does NOT cover
// ===========================================================================

#[tokio::test]
#[ignore = "FINDING F-G: NULLIF over decimal_arb is not covered by the rewrite — it drops the extension metadata and compares raw bytes, so equal cross-scale values are not detected"]
async fn nullif_over_decimal_arb_keeps_metadata() {
    let sm = main_session();
    let b = ok(&sm, "SELECT NULLIF(amt, alt) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "NULLIF");
}

#[tokio::test]
async fn nullif_returns_null_for_equal_same_scale_values() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id, NULLIF(amt, small) AS c FROM t ORDER BY id").await;
    assert_eq!(
        arb_at(&b, "c", 0),
        some(&["!", "!", "!", "!", "!", "!"]),
        "amt and small are equal in every row, so NULLIF must be NULL everywhere"
    );
}

#[tokio::test]
#[ignore = "FINDING F-G: NULLIF over decimal_arb is not covered by the rewrite — it drops the extension metadata and compares raw bytes, so equal cross-scale values are not detected"]
async fn nullif_returns_null_for_equal_cross_scale_values() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id, NULLIF(amt, amt2) AS c FROM t ORDER BY id").await;
    assert_eq!(
        arb_at(&b, "c", 0),
        some(&["!", "!", "!", "!", "!", "!"]),
        "amt and amt2 hold identical numbers at different scales; NULLIF must compare by \
         value, not by raw canonical bytes"
    );
}

#[tokio::test]
async fn nullif_keeps_the_value_when_arguments_differ() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id, NULLIF(amt, alt) AS c FROM t ORDER BY id").await;
    assert_eq!(
        arb_at(&b, "c", 0),
        some(&["5", "-3", "0", "200", "!", "7"]),
        "NULLIF must pass through the first argument when the two differ"
    );
}

#[tokio::test]
#[ignore = "FINDING F-F: GREATEST/LEAST over decimal_arb fall through to a bytewise LargeBinary comparison and return numerically wrong values, and drop the extension metadata"]
async fn greatest_over_decimal_arb_returns_the_numeric_maximum() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id, GREATEST(amt, alt) AS c FROM t ORDER BY id").await;
    assert_eq!(
        arb_at(&b, "c", 0),
        some(&["999", "888", "1", "200", "3", "7"]),
        "GREATEST over decimal_arb must compare numerically; a bytewise LargeBinary compare \
         ranks the 0xFF-signed negative above every positive"
    );
}

#[tokio::test]
#[ignore = "FINDING F-F: GREATEST/LEAST over decimal_arb fall through to a bytewise LargeBinary comparison and return numerically wrong values, and drop the extension metadata"]
async fn least_over_decimal_arb_returns_the_numeric_minimum() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id, LEAST(amt, alt) AS c FROM t ORDER BY id").await;
    assert_eq!(
        arb_at(&b, "c", 0),
        some(&["5", "-3", "0", "2", "3", "4"]),
        "LEAST over decimal_arb must compare numerically, not bytewise"
    );
}

#[tokio::test]
#[ignore = "FINDING F-F: GREATEST/LEAST over decimal_arb fall through to a bytewise LargeBinary comparison and return numerically wrong values, and drop the extension metadata"]
async fn greatest_over_decimal_arb_keeps_metadata() {
    let sm = main_session();
    let b = ok(&sm, "SELECT GREATEST(amt, alt) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "GREATEST");
}

#[tokio::test]
#[ignore = "FINDING F-F: GREATEST/LEAST over decimal_arb fall through to a bytewise LargeBinary comparison and return numerically wrong values, and drop the extension metadata"]
async fn least_over_decimal_arb_keeps_metadata() {
    let sm = main_session();
    let b = ok(&sm, "SELECT LEAST(amt, alt) AS c FROM t").await;
    assert_arb_meta(&b, "c", (100, 0), "LEAST");
}

#[tokio::test]
async fn is_not_distinct_from_is_true_for_equal_same_scale_values() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, amt IS NOT DISTINCT FROM small AS r FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        bools(&b, "r"),
        vec![Some(true); 6],
        "amt and small are equal (including both NULL on row 5), so IS NOT DISTINCT FROM \
         must be TRUE everywhere"
    );
}

#[tokio::test]
#[ignore = "FINDING F-H: IS [NOT] DISTINCT FROM over decimal_arb is handled by neither the ExprPlanner nor the rewrite: cross-scale equal values compare as distinct (silently wrong) and an integer literal operand fails to plan"]
async fn is_not_distinct_from_is_true_for_equal_cross_scale_values() {
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT id, amt IS NOT DISTINCT FROM amt2 AS r FROM t ORDER BY id",
    )
    .await;
    assert_eq!(
        bools(&b, "r"),
        vec![Some(true); 6],
        "amt and amt2 hold identical numbers at different scales; IS NOT DISTINCT FROM must \
         compare by value, not by canonical bytes"
    );
}

#[tokio::test]
#[ignore = "FINDING F-H: IS [NOT] DISTINCT FROM over decimal_arb is handled by neither the ExprPlanner nor the rewrite: cross-scale equal values compare as distinct (silently wrong) and an integer literal operand fails to plan"]
async fn is_distinct_from_is_false_for_equal_cross_scale_values() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IS DISTINCT FROM amt2").await;
    assert_eq!(
        ids(&b),
        Vec::<i64>::new(),
        "no row has a numerically different amt/amt2, so IS DISTINCT FROM must select nothing"
    );
}

#[tokio::test]
async fn is_distinct_from_treats_null_as_a_value() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IS DISTINCT FROM alt").await;
    assert_eq!(
        ids(&b),
        vec![1, 2, 3, 4, 5, 6],
        "IS DISTINCT FROM must be TRUE (not NULL) when exactly one side is NULL"
    );
}

#[tokio::test]
async fn is_not_distinct_from_null_literal_detects_the_null_row() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IS NOT DISTINCT FROM NULL").await;
    assert_eq!(
        ids(&b),
        vec![5],
        "`x IS NOT DISTINCT FROM NULL` must select exactly the NULL row"
    );
}

#[tokio::test]
#[ignore = "FINDING F-H: IS [NOT] DISTINCT FROM over decimal_arb is handled by neither the ExprPlanner nor the rewrite: cross-scale equal values compare as distinct (silently wrong) and an integer literal operand fails to plan"]
async fn is_distinct_from_int_literal_compares_by_value() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt IS NOT DISTINCT FROM 5").await;
    assert_eq!(
        ids(&b),
        vec![1],
        "`decimal_arb IS NOT DISTINCT FROM <int literal>` must coerce the literal the same \
         way `=` does"
    );
}

// ===========================================================================
// H. Cross-cutting regression guards
// ===========================================================================

#[tokio::test]
async fn plain_projection_of_decimal_arb_keeps_metadata() {
    let sm = main_session();
    let b = ok(&sm, "SELECT amt FROM t").await;
    assert_arb_meta(&b, "amt", (100, 0), "plain projection");
}

#[tokio::test]
async fn scale2_projection_keeps_its_own_scale() {
    let sm = main_session();
    let b = ok(&sm, "SELECT amt2 FROM t").await;
    assert_arb_meta(&b, "amt2", (100, 2), "plain projection of a scale-2 column");
}

#[tokio::test]
async fn scale2_values_decode_at_their_declared_scale() {
    let sm = main_session();
    let b = ok(&sm, "SELECT amt2 FROM t ORDER BY id").await;
    assert_eq!(
        arb(&b, "amt2"),
        some(&["5.00", "-3.00", "0.00", "200.00", "!", "7.00"]),
        "a scale-2 column must decode to its declared scale"
    );
}

#[tokio::test]
async fn between_does_not_alter_the_row_count_of_a_star_projection() {
    let sm = main_session();
    let b = ok(&sm, "SELECT * FROM t WHERE amt BETWEEN -1000 AND 1000").await;
    let n: usize = b.iter().map(|x| x.num_rows()).sum();
    assert_eq!(
        n, 5,
        "the rewrite must not drop or duplicate rows in a star projection"
    );
}

#[tokio::test]
async fn star_projection_through_between_keeps_all_decimal_arb_metadata() {
    let sm = main_session();
    let b = ok(&sm, "SELECT * FROM t WHERE amt BETWEEN -1000 AND 1000").await;
    assert_arb_meta(&b, "amt", (100, 0), "star projection");
    assert_arb_meta(&b, "amt2", (100, 2), "star projection");
    assert_arb_meta(&b, "small", (20, 0), "star projection");
}

#[tokio::test]
async fn between_and_in_agree_on_the_same_row_set() {
    let sm = main_session();
    let between = ids(&ok(&sm, "SELECT id FROM t WHERE amt BETWEEN 5 AND 7").await);
    let inlist = ids(&ok(&sm, "SELECT id FROM t WHERE amt IN (5, 6, 7)").await);
    assert_eq!(
        between, inlist,
        "BETWEEN 5 AND 7 and IN (5, 6, 7) must agree over integer-valued data"
    );
}

#[tokio::test]
async fn rewrite_is_idempotent_across_repeated_planning() {
    let sm = main_session();
    let sql = "SELECT CASE WHEN flag = 1 THEN amt ELSE alt END AS c FROM t";
    let first = ok(&sm, sql).await;
    let second = ok(&sm, sql).await;
    assert_eq!(
        out_field(&first, "c"),
        out_field(&second, "c"),
        "planning the same query twice must produce the same output field — a rewrite that \
         re-wraps its own output would drift"
    );
}

#[tokio::test]
async fn case_metadata_survives_a_subquery_boundary() {
    let sm = main_session();
    let sql = "SELECT c FROM (SELECT CASE WHEN flag = 1 THEN amt ELSE alt END AS c FROM t)";
    let b = ok(&sm, sql).await;
    assert_arb_meta(&b, "c", (100, 0), "CASE through a subquery");
}

#[tokio::test]
async fn between_over_a_subquery_projected_decimal_arb_column() {
    let sm = main_session();
    let sql = "SELECT id FROM (SELECT id, amt FROM t) WHERE amt BETWEEN 0 AND 100";
    let b = ok(&sm, sql).await;
    assert_eq!(
        ids(&b),
        vec![1, 3, 6],
        "BETWEEN must still rewrite when the decimal_arb column comes through a subquery"
    );
}

#[tokio::test]
async fn in_over_a_subquery_projected_decimal_arb_column() {
    let sm = main_session();
    let sql = "SELECT id FROM (SELECT id, amt FROM t) WHERE amt IN (5, 200)";
    let b = ok(&sm, sql).await;
    assert_eq!(
        ids(&b),
        vec![1, 4],
        "IN must still rewrite when the decimal_arb column comes through a subquery"
    );
}

#[tokio::test]
async fn case_over_a_subquery_projected_decimal_arb_column_keeps_metadata() {
    let sm = main_session();
    let sql = "SELECT CASE WHEN id = 1 THEN amt ELSE amt END AS c FROM (SELECT id, amt FROM t)";
    let b = ok(&sm, sql).await;
    assert_arb_meta(&b, "c", (100, 0), "CASE over a subquery column");
}

#[tokio::test]
async fn between_inside_a_case_condition() {
    let sm = main_session();
    let sql = "SELECT id, CASE WHEN amt BETWEEN 0 AND 10 THEN 1 ELSE 0 END AS c \
               FROM t ORDER BY id";
    let b = ok(&sm, sql).await;
    let idx = b[0].schema().index_of("c").unwrap();
    let c = b[0]
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("c should be Int64");
    let got: Vec<i64> = (0..c.len()).map(|i| c.value(i)).collect();
    assert_eq!(
        got,
        vec![1, 0, 1, 0, 0, 1],
        "a BETWEEN nested inside a CASE condition must be rewritten too"
    );
}

#[tokio::test]
async fn in_inside_a_case_condition() {
    let sm = main_session();
    let sql = "SELECT id, CASE WHEN amt IN (5, 7) THEN 1 ELSE 0 END AS c FROM t ORDER BY id";
    let b = ok(&sm, sql).await;
    let idx = b[0].schema().index_of("c").unwrap();
    let c = b[0]
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("c should be Int64");
    let got: Vec<i64> = (0..c.len()).map(|i| c.value(i)).collect();
    assert_eq!(
        got,
        vec![1, 0, 0, 0, 0, 1],
        "an IN nested inside a CASE condition must be rewritten too"
    );
}

#[tokio::test]
async fn between_inside_a_case_branch_over_decimal_arb() {
    let sm = main_session();
    let sql = "SELECT id, CASE WHEN amt BETWEEN 0 AND 10 THEN amt ELSE alt END AS c \
               FROM t ORDER BY id";
    let b = ok(&sm, sql).await;
    assert_arb_meta(&b, "c", (100, 0), "CASE whose condition is a BETWEEN");
    assert_eq!(
        arb(&b, "c"),
        some(&["5", "888", "0", "2", "3", "7"]),
        "the rewritten BETWEEN condition must drive branch selection correctly"
    );
}

#[tokio::test]
async fn having_clause_with_decimal_arb_between() {
    let sm = main_session();
    let sql = "SELECT flag, COUNT(*) AS n FROM t WHERE amt BETWEEN -1000 AND 1000 \
               GROUP BY flag HAVING COUNT(*) > 0 ORDER BY flag";
    let b = ok(&sm, sql).await;
    let idx = b[0].schema().index_of("n").unwrap();
    let c = b[0]
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let total: i64 = (0..c.len()).map(|i| c.value(i)).sum();
    assert_eq!(
        total, 5,
        "a rewritten BETWEEN must survive aggregation + HAVING"
    );
}

#[tokio::test]
async fn distinct_over_a_case_result_dedupes_numerically() {
    let sm = main_session();
    let sql = "SELECT DISTINCT CASE WHEN flag = 1 THEN amt ELSE amt END AS c FROM t";
    let b = ok(&sm, sql).await;
    let n: usize = b.iter().map(|x| x.num_rows()).sum();
    assert_eq!(
        n, 6,
        "the six values {{5, -3, 0, 200, NULL, 7}} are all distinct — DISTINCT over a CASE \
         result must not collapse or split them"
    );
}

// ===========================================================================
// I. Root-cause isolation for the literal-typing gap
//
// The `Decimal128`/`Decimal256` arms of `DecimalArbExprRewrite::coerce` are
// correct — they are simply never reached by a plain SQL literal, because
// DataFusion types `4.99` as Float64 unless `parse_float_as_decimal` is on.
// These tests pin the working half so a future fix can't regress it.
// ===========================================================================

#[tokio::test]
async fn between_with_explicit_decimal128_cast_bounds_works() {
    let sm = main_session();
    let sql = "SELECT id FROM t \
               WHERE amt BETWEEN CAST(4.5 AS DECIMAL(10, 2)) AND CAST(5.5 AS DECIMAL(10, 2))";
    let b = ok(&sm, sql).await;
    assert_eq!(
        ids(&b),
        vec![1],
        "an explicitly-cast Decimal128 bound must widen to decimal_arb and compare numerically"
    );
}

#[tokio::test]
async fn between_with_explicit_decimal256_cast_bounds_works() {
    let sm = main_session();
    // DECIMAL(50, 0) is beyond Decimal128's 38 digits, so this exercises the
    // to_decimal_arb_from_decimal256 arm of the rewrite's coercion.
    let sql = "SELECT id FROM t \
               WHERE amt BETWEEN CAST(0 AS DECIMAL(50, 0)) AND CAST(100 AS DECIMAL(50, 0))";
    let b = ok(&sm, sql).await;
    assert_eq!(
        ids(&b),
        vec![1, 3, 6],
        "an explicitly-cast Decimal256 bound must widen to decimal_arb and compare numerically"
    );
}

#[tokio::test]
async fn in_with_explicit_decimal128_cast_elements_works() {
    let sm = main_session();
    let sql = "SELECT id FROM t WHERE amt IN (CAST(5.0 AS DECIMAL(10, 1)), \
               CAST(200.0 AS DECIMAL(10, 1)))";
    let b = ok(&sm, sql).await;
    assert_eq!(
        ids(&b),
        vec![1, 4],
        "explicitly-cast Decimal128 IN elements must widen to decimal_arb"
    );
}

#[tokio::test]
async fn in_with_explicit_decimal256_cast_element_beyond_i64_works() {
    let sm = main_session();
    let sql = "SELECT id FROM t WHERE amt IN (CAST(200 AS DECIMAL(50, 0)), \
               CAST(99999999999999999999999 AS DECIMAL(50, 0)))";
    let b = ok(&sm, sql).await;
    assert_eq!(
        ids(&b),
        vec![4],
        "a >i64 IN element written with an explicit DECIMAL cast must work — this is the \
         documented workaround for the plain-literal gap"
    );
}

#[tokio::test]
async fn fractional_decimal_cast_bound_on_scale2_column_works() {
    let sm = main_session();
    let sql = "SELECT id FROM t \
               WHERE amt2 BETWEEN CAST(5.01 AS DECIMAL(10, 2)) AND CAST(6.00 AS DECIMAL(10, 2))";
    let b = ok(&sm, sql).await;
    assert_eq!(
        ids(&b),
        Vec::<i64>::new(),
        "5.00 must not fall inside [5.01, 6.00] once the bound reaches decimal_arb intact"
    );
}

#[tokio::test]
#[ignore = "FINDING F-A: fractional and >i64 SQL literals arrive as Float64 (DataFusion's parse_float_as_decimal defaults to false), which DecimalArbExprRewrite::coerce refuses, so no decimal_arb column can be filtered by a fractional or >i64 literal"]
async fn equality_against_a_fractional_literal_plans() {
    let sm = main_session();
    let b = ok(&sm, "SELECT id FROM t WHERE amt2 = 5.00").await;
    assert_eq!(
        ids(&b),
        vec![1],
        "`decimal_col = 4.99`-style equality is the same literal-typing gap BETWEEN/IN hit"
    );
}

#[tokio::test]
async fn coalesce_int_literal_fallback_works_for_a_plain_int_column() {
    // Control for F-D: the `COALESCE(col, 0)` shape is fine for Int64, so the
    // failure for decimal_arb is specific to the extension type.
    let sm = main_session();
    let b = ok(&sm, "SELECT COALESCE(id, 0) AS c FROM t").await;
    let f = out_field(&b, "c");
    assert_eq!(
        f.data_type(),
        &DataType::Int64,
        "COALESCE(int_col, 0) must keep working"
    );
}

#[tokio::test]
async fn case_int_literal_else_works_for_a_plain_int_column() {
    // Control for F-D on the CASE side.
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT CASE WHEN flag = 1 THEN id ELSE 0 END AS c FROM t",
    )
    .await;
    let f = out_field(&b, "c");
    assert_eq!(
        f.data_type(),
        &DataType::Int64,
        "CASE ... ELSE 0 must keep working for Int64"
    );
}

#[tokio::test]
async fn greatest_and_least_still_work_for_a_plain_int_column() {
    // Control for F-F: GREATEST/LEAST themselves are fine; only the decimal_arb
    // storage type defeats them.
    let sm = main_session();
    let b = ok(
        &sm,
        "SELECT GREATEST(id, flag) AS g, LEAST(id, flag) AS l FROM t ORDER BY id",
    )
    .await;
    let g = b[0]
        .column(b[0].schema().index_of("g").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        (0..g.len()).map(|i| g.value(i)).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6],
        "GREATEST over Int64 must keep working"
    );
}

#[tokio::test]
async fn nullif_still_works_for_a_plain_int_column() {
    // Control for F-G.
    let sm = main_session();
    let b = ok(&sm, "SELECT NULLIF(id, 3) AS c FROM t ORDER BY id").await;
    let c = b[0]
        .column(b[0].schema().index_of("c").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(c.is_null(2), "NULLIF over Int64 must keep working");
}
