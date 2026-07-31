//! Adversarial coverage for **optimizer-rule interactions** with the
//! `streamling.decimal_arb` extension type, through the full `SessionManager`
//! stack (UDFs + `DecimalArbExprPlanner` + `DecimalArbExprRewrite` +
//! `DecimalArbSortRewriteRule` + DataFusion 54's default analyzer/optimizer).
//!
//! Areas probed:
//!   * `DecimalArbSortRewriteRule` — ORDER BY over columns, aliases, ordinals,
//!     expressions, aggregates, through subqueries / CTEs / unions / joins,
//!     with LIMIT (TopK), with NULLS FIRST/LAST, and the contexts the rule
//!     does *not* see (window ORDER BY, aggregate ORDER BY).
//!   * projection pushdown / `optimize_projections`
//!   * filter pushdown and predicate combination
//!   * common-subexpression elimination
//!   * expression simplification & constant folding over decimal_arb
//!   * limit pushdown
//!   * JOIN (inner/left/right/full/semi/anti/self/USING) on decimal_arb keys
//!   * UNION / UNION ALL / EXCEPT / INTERSECT
//!   * CTEs, nested subqueries, correlated subqueries, views
//!
//! **Every value assertion is paired with a metadata assertion** where a
//! decimal_arb column reaches the output: a query that returns the right digits
//! but drops `streamling.decimal_arb` from the output field silently corrupts
//! data at the sink (F2's shape), and a query whose optimized plan reorders or
//! folds a decimal_arb comparison incorrectly silently corrupts rows.

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, LargeBinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use streamling_core::dynamic_table::DynamicTableRegistry;
use streamling_core::session::SessionManager;
use streamling_core::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};

// =====================================================================
// Harness
// =====================================================================

/// Build a `decimal_arb(p, s)` storage array from canonical decimal strings.
fn arb_array(name: &str, p: u32, s: u32, vals: &[Option<&str>]) -> ArrayRef {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), name, p, s).unwrap();
    for v in vals {
        match v {
            Some(x) => b
                .append_value(&DecimalArbValue::from_str(x).unwrap())
                .unwrap(),
            None => b.append_null(),
        }
    }
    let (raw, _, _) = b.finish().into_inner();
    Arc::new(raw)
}

fn int_array(vals: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(vals.to_vec()))
}

fn str_array(vals: &[&str]) -> ArrayRef {
    Arc::new(arrow::array::StringArray::from(vals.to_vec()))
}

/// A session with the full decimal_arb stack and these tables:
///
/// ```text
/// t (id Int64, amt decimal_arb(30,4), alt decimal_arb(30,4), grp Utf8)
///     id  |  amt   | alt | grp
///      1  |   100  |  5  |  a
///      2  |  -100  |  4  |  b
///      3  |     0  |  3  |  a
///      4  |    -1  |  2  |  b
///      5  |  1000  |  1  |  a
///
/// u (id Int64, amt decimal_arb(30,4))     -> 10:100, 11:-1, 12:7
/// w (id Int64, amt decimal_arb(20,2))     -> 20:100, 21:-1, 22:7   (DIFFERENT scale)
/// tn(id Int64, amt decimal_arb(30,4) NULLable, alt decimal_arb(30,4))
///                                          -> 1:10, 2:NULL, 3:-10, 4:NULL ; alt 1..4
/// ```
fn session() -> SessionContext {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap();
    let ctx = sm.session_context();

    let t_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("amt", 30, 4, true).unwrap(),
        DecimalArbType::field("alt", 30, 4, true).unwrap(),
        Field::new("grp", DataType::Utf8, false),
    ]));
    let t = RecordBatch::try_new(
        t_schema,
        vec![
            int_array(&[1, 2, 3, 4, 5]),
            arb_array(
                "amt",
                30,
                4,
                &[
                    Some("100"),
                    Some("-100"),
                    Some("0"),
                    Some("-1"),
                    Some("1000"),
                ],
            ),
            arb_array(
                "alt",
                30,
                4,
                &[Some("5"), Some("4"), Some("3"), Some("2"), Some("1")],
            ),
            str_array(&["a", "b", "a", "b", "a"]),
        ],
    )
    .unwrap();
    ctx.register_batch("t", t).unwrap();

    let u_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("amt", 30, 4, true).unwrap(),
    ]));
    let u = RecordBatch::try_new(
        u_schema,
        vec![
            int_array(&[10, 11, 12]),
            arb_array("amt", 30, 4, &[Some("100"), Some("-1"), Some("7")]),
        ],
    )
    .unwrap();
    ctx.register_batch("u", u).unwrap();

    let w_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("amt", 20, 2, true).unwrap(),
    ]));
    let w = RecordBatch::try_new(
        w_schema,
        vec![
            int_array(&[20, 21, 22]),
            arb_array("amt", 20, 2, &[Some("100"), Some("-1"), Some("7")]),
        ],
    )
    .unwrap();
    ctx.register_batch("w", w).unwrap();

    let tn_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("amt", 30, 4, true).unwrap(),
        DecimalArbType::field("alt", 30, 4, true).unwrap(),
    ]));
    let tn = RecordBatch::try_new(
        tn_schema,
        vec![
            int_array(&[1, 2, 3, 4]),
            arb_array("amt", 30, 4, &[Some("10"), None, Some("-10"), None]),
            arb_array("alt", 30, 4, &[Some("1"), Some("2"), Some("3"), Some("4")]),
        ],
    )
    .unwrap();
    ctx.register_batch("tn", tn).unwrap();

    ctx
}

/// Plan + execute, returning the *output batch* schema (what a sink sees) and
/// the batches. Panics with the SQL text on any planning/execution failure.
async fn run(ctx: &SessionContext, sql: &str) -> (SchemaRef, Vec<RecordBatch>) {
    let df = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("PLANNING FAILED for `{sql}`: {e}"));
    let logical: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("EXECUTION FAILED for `{sql}`: {e}"));
    let schema = batches.first().map(|b| b.schema()).unwrap_or(logical);
    (schema, batches)
}

/// Plan + execute expecting an error; returns the error string.
async fn run_err(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(e) => e.to_string(),
        Ok(df) => match df.collect().await {
            Err(e) => e.to_string(),
            Ok(b) => panic!(
                "expected `{sql}` to fail, but it produced {} row(s)",
                b.iter().map(|x| x.num_rows()).sum::<usize>()
            ),
        },
    }
}

fn field_of(schema: &SchemaRef, name: &str) -> Field {
    schema
        .field_with_name(name)
        .unwrap_or_else(|_| {
            panic!(
                "output has no column `{name}` (columns: {:?})",
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            )
        })
        .clone()
}

/// Assert that `name` is still a decimal_arb column with exactly `(p, s)`.
fn assert_arb_meta(schema: &SchemaRef, name: &str, p: u32, s: u32, ctx_msg: &str) {
    let f = field_of(schema, name);
    assert!(
        DecimalArbType::is_decimal_arb_field(&f),
        "{ctx_msg}: output column `{name}` LOST streamling.decimal_arb metadata \
         (a sink would treat it as raw BYTEA); field = {f:?}"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&f),
        Some((p, s)),
        "{ctx_msg}: output column `{name}` has the wrong (precision, scale); field = {f:?}"
    );
}

/// Decode a decimal_arb column using the scale declared on the *output field*.
/// Panics if the metadata was dropped — that is itself the defect.
fn arb_values(schema: &SchemaRef, batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let f = field_of(schema, name);
    let (_, scale) = DecimalArbType::precision_scale_from_field(&f).unwrap_or_else(|| {
        panic!("column `{name}` is not decimal_arb in the output schema: {f:?}")
    });
    arb_values_at(batches, name, scale)
}

/// Decode a decimal_arb column at an explicitly supplied scale.
fn arb_values_at(batches: &[RecordBatch], name: &str, scale: u32) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let c = b
            .column_by_name(name)
            .unwrap_or_else(|| panic!("batch has no column `{name}`"))
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap_or_else(|| panic!("column `{name}` is not LargeBinary storage"));
        for i in 0..c.len() {
            if c.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(norm(
                    DecimalArbValue::from_canonical_bytes_at_scale(c.value(i), scale)
                        .unwrap_or_else(|e| {
                            panic!("column `{name}` row {i} is not decodable at scale {scale}: {e}")
                        })
                        .to_canonical_string(),
                )));
            }
        }
    }
    out
}

/// `to_canonical_string()` is scale-preserving ("100.0000" at scale 4). These
/// tests compare *numeric* values, so trailing fractional zeros are trimmed;
/// the declared scale is asserted separately via `assert_arb_meta`.
fn norm(s: String) -> String {
    if !s.contains('.') {
        return s;
    }
    let t = s.trim_end_matches('0');
    let t = t.strip_suffix('.').unwrap_or(t);
    if t.is_empty() || t == "-" {
        "0".to_string()
    } else {
        t.to_string()
    }
}

fn ints(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let c = b
            .column_by_name(name)
            .unwrap_or_else(|| panic!("batch has no column `{name}`"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column `{name}` is not Int64"));
        for i in 0..c.len() {
            out.push(if c.is_null(i) { None } else { Some(c.value(i)) });
        }
    }
    out
}

fn bools(batches: &[RecordBatch], name: &str) -> Vec<Option<bool>> {
    let mut out = Vec::new();
    for b in batches {
        let c = b
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap_or_else(|| panic!("column `{name}` is not Boolean"));
        for i in 0..c.len() {
            out.push(if c.is_null(i) { None } else { Some(c.value(i)) });
        }
    }
    out
}

fn nrows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Collect `id` values in observed order (only meaningful when the query has a
/// total ORDER BY).
async fn ordered_ids(ctx: &SessionContext, sql: &str) -> Vec<Option<i64>> {
    let (_, b) = run(ctx, sql).await;
    ints(&b, "id")
}

/// Collect `id` values, sorted — for queries whose row order is unspecified.
async fn sorted_ids(ctx: &SessionContext, sql: &str) -> Vec<i64> {
    let (_, b) = run(ctx, sql).await;
    let mut v: Vec<i64> = ints(&b, "id").into_iter().flatten().collect();
    v.sort_unstable();
    v
}

/// Sorted multiset of a decimal_arb column's canonical strings (order-free).
fn sorted_arb(schema: &SchemaRef, batches: &[RecordBatch], name: &str) -> Vec<String> {
    let mut v: Vec<String> = arb_values(schema, batches, name)
        .into_iter()
        .map(|o| o.unwrap_or_else(|| "NULL".to_string()))
        .collect();
    v.sort();
    v
}

/// The optimized logical plan, rendered.
async fn optimized_plan_text(ctx: &SessionContext, sql: &str) -> String {
    let df = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("PLANNING FAILED for `{sql}`: {e}"));
    let plan = df
        .into_optimized_plan()
        .unwrap_or_else(|e| panic!("OPTIMIZATION FAILED for `{sql}`: {e}"));
    format!("{}", plan.display_indent())
}

// =====================================================================
// A. DecimalArbSortRewriteRule — the core optimizer rule
// =====================================================================

#[tokio::test]
async fn order_by_decimal_arb_asc_sorts_numerically_not_bytewise() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY amt ASC").await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "ORDER BY decimal_arb ASC must be numeric (-100, -1, 0, 100, 1000); a \
         bytewise LargeBinary sort would place the 0xFF-signed negatives last"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_desc_sorts_numerically() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY amt DESC").await;
    assert_eq!(
        ids,
        vec![Some(5), Some(1), Some(3), Some(4), Some(2)],
        "ORDER BY decimal_arb DESC must be numeric (1000, 100, 0, -1, -100)"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_default_direction_is_ascending_numeric() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY amt").await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "bare ORDER BY decimal_arb (no ASC/DESC) must default to numeric ascending"
    );
}

#[tokio::test]
async fn order_by_projected_decimal_arb_keeps_metadata_and_values() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t ORDER BY amt ASC").await;
    assert_arb_meta(
        &s,
        "amt",
        30,
        4,
        "ORDER BY over a projected decimal_arb column",
    );
    assert_eq!(
        arb_values(&s, &b, "amt"),
        vec![
            Some("-100".into()),
            Some("-1".into()),
            Some("0".into()),
            Some("100".into()),
            Some("1000".into())
        ],
        "the sort-key wrapper must not leak into the projected value"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_not_in_select_list_does_not_leak_sort_key_column() {
    let ctx = session();
    let (s, _) = run(&ctx, "SELECT id FROM t ORDER BY amt ASC").await;
    assert_eq!(
        s.fields().len(),
        1,
        "ORDER BY on a non-projected decimal_arb column must not leak the \
         sort-key/missing-column into the output schema: {:?}",
        s.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn select_star_order_by_decimal_arb_keeps_exactly_the_table_columns() {
    let ctx = session();
    let (s, _) = run(&ctx, "SELECT * FROM t ORDER BY amt ASC").await;
    let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["id", "amt", "alt", "grp"],
        "SELECT * with a decimal_arb ORDER BY must not add a sort-key column"
    );
}

#[tokio::test]
async fn order_by_alias_of_decimal_arb_column_is_rewritten() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id, amt AS a FROM t ORDER BY a ASC").await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "ORDER BY over an *alias* of a decimal_arb column must still sort numerically"
    );
}

#[tokio::test]
async fn order_by_ordinal_position_of_decimal_arb_column_is_rewritten() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT amt, id FROM t ORDER BY 1 ASC").await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "ORDER BY <ordinal> pointing at a decimal_arb column must sort numerically"
    );
}

#[tokio::test]
async fn order_by_qualified_decimal_arb_column_is_rewritten() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT t.id FROM t ORDER BY t.amt ASC").await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "table-qualified ORDER BY must resolve to the decimal_arb field and be rewritten"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_addition_expression_is_rewritten() {
    let ctx = session();
    // amt + alt = 105, -96, 3, 1, 1001  ->  ascending: -96(2), 1(4), 3(3), 105(1), 1001(5)
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY amt + alt ASC").await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "ORDER BY over a decimal_arb *expression* must sort by the numeric sum"
    );
}

#[tokio::test]
async fn order_by_alias_of_decimal_arb_expression_is_rewritten() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id, amt - alt AS d FROM t ORDER BY d ASC").await;
    // amt - alt = 95, -104, -3, -3, 999 -> -104(2), -3(3), -3(4), 95(1), 999(5)
    let got: Vec<i64> = ids.into_iter().flatten().collect();
    assert_eq!(
        got[0], 2,
        "smallest difference (-104) must come first; got {got:?}"
    );
    assert_eq!(
        got[4], 5,
        "largest difference (999) must come last; got {got:?}"
    );
    assert!(
        got[1..3].contains(&3) && got[1..3].contains(&4),
        "the two -3 rows must occupy positions 1..2; got {got:?}"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_abs_expression_is_rewritten() {
    let ctx = session();
    let ids = ordered_ids(
        &ctx,
        "SELECT id FROM t ORDER BY decimal_arb_abs(amt) ASC, id ASC",
    )
    .await;
    // |amt| = 100, 100, 0, 1, 1000 -> 0(3), 1(4), 100(1), 100(2), 1000(5)
    assert_eq!(
        ids,
        vec![Some(3), Some(4), Some(1), Some(2), Some(5)],
        "ORDER BY decimal_arb_abs(col) must sort by absolute numeric value"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_neg_expression_is_rewritten() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY decimal_arb_neg(amt) ASC").await;
    // -amt = -100, 100, 0, 1, -1000 -> -1000(5), -100(1), 0(3), 1(4), 100(2)
    assert_eq!(
        ids,
        vec![Some(5), Some(1), Some(3), Some(4), Some(2)],
        "ORDER BY decimal_arb_neg(col) must sort by the negated numeric value"
    );
}

#[tokio::test]
async fn order_by_case_over_decimal_arb_is_rewritten() {
    let ctx = session();
    // CASE ... THEN amt ELSE amt END is amt for every row.
    let ids = ordered_ids(
        &ctx,
        "SELECT id FROM t ORDER BY CASE WHEN id > 0 THEN amt ELSE amt END ASC",
    )
    .await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "the F2 metadata re-stamp must make a CASE over decimal_arb visible to \
         DecimalArbSortRewriteRule, so ORDER BY CASE sorts numerically"
    );
}

#[tokio::test]
async fn order_by_coalesce_over_decimal_arb_is_rewritten() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM tn ORDER BY COALESCE(amt, alt) ASC").await;
    // COALESCE(amt, alt) = 10, 2, -10, 4  -> -10(3), 2(2), 4(4), 10(1)
    assert_eq!(
        ids,
        vec![Some(3), Some(2), Some(4), Some(1)],
        "COALESCE over decimal_arb keeps its metadata (F2), so ORDER BY COALESCE \
         must sort numerically across signs"
    );
}

#[tokio::test]
async fn order_by_two_keys_decimal_arb_then_int() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY amt ASC, id DESC").await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "a multi-key sort whose first key is decimal_arb must rewrite only that key"
    );
}

#[tokio::test]
async fn order_by_two_keys_int_then_decimal_arb() {
    let ctx = session();
    // grp: a(1,3,5) b(2,4); within each group order by amt.
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY grp ASC, amt ASC").await;
    assert_eq!(
        ids,
        vec![Some(3), Some(1), Some(5), Some(2), Some(4)],
        "a non-decimal_arb leading key must be untouched while the trailing \
         decimal_arb key is still rewritten"
    );
}

#[tokio::test]
async fn order_by_two_decimal_arb_keys_rewrites_both() {
    let ctx = session();
    // amt is unique, so alt never breaks a tie; assert the primary order holds.
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY amt ASC, alt ASC").await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "two decimal_arb sort keys must both be wrapped without disturbing order"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_with_limit_is_a_correct_topk() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY amt ASC LIMIT 2").await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4)],
        "Sort+fetch (TopK) must keep the two numerically smallest rows (-100, -1)"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_desc_with_limit_is_a_correct_topk() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY amt DESC LIMIT 2").await;
    assert_eq!(
        ids,
        vec![Some(5), Some(1)],
        "descending TopK must keep the two numerically largest rows (1000, 100)"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_with_limit_and_offset() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY amt ASC LIMIT 2 OFFSET 2").await;
    assert_eq!(
        ids,
        vec![Some(3), Some(1)],
        "LIMIT/OFFSET pushdown under a rewritten decimal_arb sort must skip the \
         two smallest and return 0 then 100"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_limit_zero_returns_no_rows() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id FROM t ORDER BY amt ASC LIMIT 0").await;
    assert_eq!(
        nrows(&b),
        0,
        "LIMIT 0 under a rewritten sort must return no rows"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_limit_larger_than_input_returns_all_rows_in_order() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY amt ASC LIMIT 100").await;
    assert_eq!(
        ids,
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "an over-large LIMIT must not truncate or reorder the rewritten sort"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_nulls_last_places_nulls_after_values() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM tn ORDER BY amt ASC NULLS LAST").await;
    assert_eq!(
        ids[0..2],
        [Some(3), Some(1)],
        "NULLS LAST: -10 then 10 must come first; got {ids:?}"
    );
    let tail: Vec<i64> = ids[2..].iter().map(|x| x.unwrap()).collect();
    let mut tail_sorted = tail.clone();
    tail_sorted.sort_unstable();
    assert_eq!(
        tail_sorted,
        vec![2, 4],
        "NULLS LAST: the two NULL-amt rows must be last; got {ids:?}"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_nulls_first_places_nulls_before_values() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM tn ORDER BY amt ASC NULLS FIRST").await;
    let head: Vec<i64> = ids[0..2].iter().map(|x| x.unwrap()).collect();
    let mut head_sorted = head.clone();
    head_sorted.sort_unstable();
    assert_eq!(
        head_sorted,
        vec![2, 4],
        "NULLS FIRST: the NULL-amt rows must lead; got {ids:?}"
    );
    assert_eq!(
        ids[2..],
        [Some(3), Some(1)],
        "NULLS FIRST: values must still follow in numeric order; got {ids:?}"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_desc_nulls_last_keeps_numeric_order() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM tn ORDER BY amt DESC NULLS LAST").await;
    assert_eq!(
        ids[0..2],
        [Some(1), Some(3)],
        "DESC NULLS LAST: 10 then -10; got {ids:?}"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_inside_subquery_survives_the_outer_projection() {
    let ctx = session();
    let ids = ordered_ids(
        &ctx,
        "SELECT id FROM (SELECT id, amt FROM t ORDER BY amt ASC LIMIT 3)",
    )
    .await;
    let mut got: Vec<i64> = ids.into_iter().flatten().collect();
    got.sort_unstable();
    assert_eq!(
        got,
        vec![2, 3, 4],
        "the inner ORDER BY ... LIMIT 3 must select the three numerically \
         smallest rows (-100, -1, 0) even after the outer projection prunes `amt`"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_inside_cte_is_rewritten() {
    let ctx = session();
    let ids = ordered_ids(
        &ctx,
        "WITH c AS (SELECT id, amt FROM t ORDER BY amt ASC LIMIT 2) SELECT id FROM c",
    )
    .await;
    let mut got: Vec<i64> = ids.into_iter().flatten().collect();
    got.sort_unstable();
    assert_eq!(
        got,
        vec![2, 4],
        "a decimal_arb ORDER BY inside a CTE must be rewritten too"
    );
}

#[tokio::test]
#[ignore = "FINDING E2 (generic, pre-dates decimal_arb): with the default physical rules replaced, EnforceSorting never inserts CoalescePartitionsExec/SortPreservingMergeExec under a SortExec, so ORDER BY over a multi-partition input (any UNION) silently returns ONLY the first branch's rows."]
async fn order_by_decimal_arb_after_union_all_is_rewritten() {
    let ctx = session();
    let ids = ordered_ids(
        &ctx,
        "SELECT id, amt FROM t UNION ALL SELECT id, amt FROM u ORDER BY amt ASC",
    )
    .await;
    // combined: 100(1), -100(2), 0(3), -1(4), 1000(5), 100(10), -1(11), 7(12)
    // numeric asc: -100(2), -1(4|11), -1(11|4), 0(3), 7(12), 100(1|10), 100(10|1), 1000(5)
    let got: Vec<i64> = ids.into_iter().flatten().collect();
    assert_eq!(got[0], 2, "smallest (-100) first; got {got:?}");
    assert_eq!(got[7], 5, "largest (1000) last; got {got:?}");
    assert_eq!(got[3], 3, "0 must sit at index 3; got {got:?}");
    assert_eq!(got[4], 12, "7 must sit at index 4; got {got:?}");
}

#[tokio::test]
async fn order_by_decimal_arb_after_join_is_rewritten() {
    let ctx = session();
    let ids = ordered_ids(
        &ctx,
        "SELECT t.id AS id FROM t JOIN u ON t.amt = u.amt ORDER BY t.amt ASC",
    )
    .await;
    assert_eq!(
        ids,
        vec![Some(4), Some(1)],
        "post-join ORDER BY on a decimal_arb join key must be numeric (-1 then 100)"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_after_distinct_is_rewritten() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT DISTINCT amt FROM t ORDER BY amt ASC").await;
    assert_arb_meta(&s, "amt", 30, 4, "DISTINCT + ORDER BY");
    assert_eq!(
        arb_values(&s, &b, "amt"),
        vec![
            Some("-100".into()),
            Some("-1".into()),
            Some("0".into()),
            Some("100".into()),
            Some("1000".into())
        ],
        "DISTINCT then ORDER BY over decimal_arb must be numerically ordered"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_group_by_key_is_rewritten() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt ORDER BY amt ASC",
    )
    .await;
    assert_arb_meta(&s, "amt", 30, 4, "GROUP BY key + ORDER BY");
    assert_eq!(
        arb_values(&s, &b, "amt"),
        vec![
            Some("-100".into()),
            Some("-1".into()),
            Some("0".into()),
            Some("100".into()),
            Some("1000".into())
        ],
        "GROUP BY decimal_arb then ORDER BY that key must order numerically"
    );
}

#[tokio::test]
#[ignore = "FINDING D1: SUM/MIN/MAX/AVG over decimal_arb return an output Field with NO streamling.decimal_arb metadata (bare LargeBinary), so DecimalArbSortRewriteRule/DecimalArbExprPlanner cannot see the aggregate as decimal_arb."]
async fn order_by_min_aggregate_over_decimal_arb_orders_numerically() {
    let ctx = session();
    // grp a -> {100, 0, 1000} min = 0 ; grp b -> {-100, -1} min = -100
    // numeric ascending by MIN(amt): b (-100) then a (0)
    let (_, b) = run(
        &ctx,
        "SELECT grp, MIN(amt) AS m FROM t GROUP BY grp ORDER BY MIN(amt) ASC",
    )
    .await;
    let groups: Vec<String> = b
        .iter()
        .flat_map(|batch| {
            let c = batch
                .column_by_name("grp")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            (0..c.len())
                .map(|i| c.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        groups,
        vec!["b".to_string(), "a".to_string()],
        "ORDER BY MIN(decimal_arb) must order groups by the numeric minimum \
         (-100 before 0), not by the bytewise LargeBinary encoding"
    );
}

#[tokio::test]
#[ignore = "FINDING D1: SUM/MIN/MAX/AVG over decimal_arb return an output Field with NO streamling.decimal_arb metadata (bare LargeBinary), so DecimalArbSortRewriteRule/DecimalArbExprPlanner cannot see the aggregate as decimal_arb."]
async fn order_by_sum_aggregate_over_decimal_arb_orders_numerically() {
    let ctx = session();
    // grp a -> 100 + 0 + 1000 = 1100 ; grp b -> -100 + -1 = -101
    let (_, b) = run(
        &ctx,
        "SELECT grp, SUM(amt) AS s FROM t GROUP BY grp ORDER BY SUM(amt) ASC",
    )
    .await;
    let groups: Vec<String> = b
        .iter()
        .flat_map(|batch| {
            let c = batch
                .column_by_name("grp")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            (0..c.len())
                .map(|i| c.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        groups,
        vec!["b".to_string(), "a".to_string()],
        "ORDER BY SUM(decimal_arb) must order groups by the numeric sum \
         (-101 before 1100)"
    );
}

#[tokio::test]
async fn sort_rewrite_is_idempotent_across_optimizer_passes() {
    let ctx = session();
    let plan = optimized_plan_text(&ctx, "SELECT id FROM t ORDER BY amt ASC").await;
    let occurrences = plan.matches("decimal_arb_to_sort_key").count();
    assert_eq!(
        occurrences, 1,
        "DecimalArbSortRewriteRule must be idempotent: the optimizer runs it on \
         every pass, so a non-idempotent rule nests the wrapper. Plan:\n{plan}"
    );
}

#[tokio::test]
async fn sort_rewrite_fires_exactly_once_per_decimal_arb_sort_key() {
    let ctx = session();
    let plan = optimized_plan_text(&ctx, "SELECT id FROM t ORDER BY amt ASC, alt DESC").await;
    assert_eq!(
        plan.matches("decimal_arb_to_sort_key").count(),
        2,
        "two decimal_arb sort keys => exactly two wrappers. Plan:\n{plan}"
    );
}

#[tokio::test]
async fn sort_rewrite_does_not_touch_non_decimal_arb_sorts() {
    let ctx = session();
    let plan = optimized_plan_text(&ctx, "SELECT id FROM t ORDER BY id ASC, grp DESC").await;
    assert!(
        !plan.contains("decimal_arb_to_sort_key"),
        "a sort with no decimal_arb key must not be rewritten. Plan:\n{plan}"
    );
}

#[tokio::test]
async fn non_decimal_arb_order_by_still_sorts_correctly() {
    let ctx = session();
    let ids = ordered_ids(&ctx, "SELECT id FROM t ORDER BY id DESC").await;
    assert_eq!(
        ids,
        vec![Some(5), Some(4), Some(3), Some(2), Some(1)],
        "registering DecimalArbSortRewriteRule must not perturb ordinary sorts"
    );
}

#[tokio::test]
#[ignore = "FINDING D4: window functions over a decimal_arb ORDER BY / PARTITION BY fail with a DataFusion Internal error (RANGE frame over LargeBinary / 'All partition by columns should have an ordering')."]
async fn window_row_number_over_decimal_arb_order_is_numeric() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY amt ASC) AS rn FROM t ORDER BY id ASC",
    )
    .await;
    let rn = ints(&b, "rn");
    // amt = 100(1), -100(2), 0(3), -1(4), 1000(5)
    // numeric ranks: 1->4, 2->1, 3->3, 4->2, 5->5
    assert_eq!(
        rn,
        vec![Some(4), Some(1), Some(3), Some(2), Some(5)],
        "ROW_NUMBER() OVER (ORDER BY decimal_arb) must use numeric order. \
         DecimalArbSortRewriteRule only rewrites LogicalPlan::Sort nodes, so a \
         window ORDER BY falls through to the bytewise LargeBinary comparator, \
         which ranks the 0xFF-signed negatives last"
    );
}

#[tokio::test]
#[ignore = "FINDING D4: window functions over a decimal_arb ORDER BY / PARTITION BY fail with a DataFusion Internal error (RANGE frame over LargeBinary / 'All partition by columns should have an ordering')."]
async fn window_rank_over_decimal_arb_order_is_numeric() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT id, RANK() OVER (ORDER BY amt ASC) AS rk FROM t ORDER BY id ASC",
    )
    .await;
    assert_eq!(
        ints(&b, "rk"),
        vec![Some(4), Some(1), Some(3), Some(2), Some(5)],
        "RANK() OVER (ORDER BY decimal_arb) must rank by numeric value"
    );
}

#[tokio::test]
#[ignore = "FINDING D4: window functions over a decimal_arb ORDER BY / PARTITION BY fail with a DataFusion Internal error (RANGE frame over LargeBinary / 'All partition by columns should have an ordering')."]
async fn window_lag_over_decimal_arb_order_uses_numeric_predecessor() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT id, LAG(id) OVER (ORDER BY amt ASC) AS prev FROM t ORDER BY id ASC",
    )
    .await;
    // numeric order: 2(-100), 4(-1), 3(0), 1(100), 5(1000)
    // prev by id:    id1 <- 3, id2 <- NULL, id3 <- 4, id4 <- 2, id5 <- 1
    assert_eq!(
        ints(&b, "prev"),
        vec![Some(3), None, Some(4), Some(2), Some(1)],
        "LAG over a decimal_arb window ORDER BY must follow numeric order"
    );
}

#[tokio::test]
#[ignore = "FINDING D4: window functions over a decimal_arb ORDER BY / PARTITION BY fail with a DataFusion Internal error (RANGE frame over LargeBinary / 'All partition by columns should have an ordering')."]
async fn window_partition_by_decimal_arb_groups_by_value() {
    let ctx = session();
    // Every amt is distinct, so each partition has exactly one row.
    let (_, b) = run(
        &ctx,
        "SELECT id, COUNT(*) OVER (PARTITION BY amt) AS c FROM t ORDER BY id ASC",
    )
    .await;
    assert_eq!(
        ints(&b, "c"),
        vec![Some(1), Some(1), Some(1), Some(1), Some(1)],
        "PARTITION BY decimal_arb partitions on canonical bytes; five distinct \
         values must give five singleton partitions"
    );
}

#[tokio::test]
#[ignore = "FINDING D3: an aggregate's ORDER BY over decimal_arb (FIRST_VALUE(x ORDER BY amt)) is not a LogicalPlan::Sort, so DecimalArbSortRewriteRule never sees it and the ordering is bytewise, not numeric."]
async fn aggregate_order_by_over_decimal_arb_picks_the_numeric_minimum() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT FIRST_VALUE(id ORDER BY amt ASC) AS f FROM t").await;
    assert_eq!(
        ints(&b, "f"),
        vec![Some(2)],
        "FIRST_VALUE(id ORDER BY decimal_arb) must pick the row with the \
         numerically smallest amt (-100 => id 2). The aggregate's ORDER BY is \
         not a LogicalPlan::Sort, so DecimalArbSortRewriteRule never sees it and \
         the bytewise comparator picks 0 (id 3) instead"
    );
}

#[tokio::test]
#[ignore = "FINDING D3: an aggregate's ORDER BY over decimal_arb (FIRST_VALUE(x ORDER BY amt)) is not a LogicalPlan::Sort, so DecimalArbSortRewriteRule never sees it and the ordering is bytewise, not numeric."]
async fn aggregate_order_by_over_decimal_arb_picks_the_numeric_maximum() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT LAST_VALUE(id ORDER BY amt ASC) AS l FROM t").await;
    assert_eq!(
        ints(&b, "l"),
        vec![Some(5)],
        "LAST_VALUE(id ORDER BY decimal_arb) must pick the numerically largest \
         amt (1000 => id 5)"
    );
}

#[tokio::test]
async fn min_max_aggregates_over_decimal_arb_are_numeric() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT MIN(amt) AS lo, MAX(amt) AS hi FROM t").await;
    assert_eq!(
        arb_values_at(&b, "lo", 4),
        vec![Some("-100".into())],
        "MIN over decimal_arb must be numeric, not bytewise"
    );
    assert_eq!(
        arb_values_at(&b, "hi", 4),
        vec![Some("1000".into())],
        "MAX over decimal_arb must be numeric, not bytewise"
    );
    let _ = s;
}

// =====================================================================
// B. Projection pushdown / optimize_projections
// =====================================================================

#[tokio::test]
async fn projection_pushdown_through_subquery_preserves_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt FROM (SELECT id, amt, alt, grp FROM t) ORDER BY amt ASC",
    )
    .await;
    assert_arb_meta(&s, "amt", 30, 4, "projection pushdown through a subquery");
    assert_eq!(arb_values(&s, &b, "amt")[0], Some("-100".into()));
}

#[tokio::test]
async fn projection_pushdown_three_levels_deep_preserves_metadata() {
    let ctx = session();
    let sql = "SELECT amt FROM (SELECT amt, id FROM (SELECT amt, id, grp FROM \
               (SELECT * FROM t))) ORDER BY amt ASC";
    let (s, b) = run(&ctx, sql).await;
    assert_arb_meta(&s, "amt", 30, 4, "three nested projections");
    assert_eq!(
        arb_values(&s, &b, "amt"),
        vec![
            Some("-100".into()),
            Some("-1".into()),
            Some("0".into()),
            Some("100".into()),
            Some("1000".into())
        ],
        "values must survive three levels of projection pruning"
    );
}

#[tokio::test]
async fn projection_pushdown_drops_unused_decimal_arb_column_without_error() {
    let ctx = session();
    let (s, _) = run(&ctx, "SELECT id FROM (SELECT id, amt, alt FROM t)").await;
    assert_eq!(
        s.fields().len(),
        1,
        "the unused decimal_arb columns must be pruned, leaving only `id`"
    );
}

#[tokio::test]
async fn projection_pushdown_keeps_only_the_referenced_decimal_arb_column() {
    let ctx = session();
    let plan = optimized_plan_text(&ctx, "SELECT alt FROM t ORDER BY alt ASC").await;
    assert!(
        !plan.contains("t.amt"),
        "`amt` is unreferenced and must be pruned from the scan. Plan:\n{plan}"
    );
}

#[tokio::test]
async fn select_star_preserves_decimal_arb_metadata_on_every_column() {
    let ctx = session();
    let (s, _) = run(&ctx, "SELECT * FROM t").await;
    assert_arb_meta(&s, "amt", 30, 4, "SELECT *");
    assert_arb_meta(&s, "alt", 30, 4, "SELECT *");
}

#[tokio::test]
async fn projection_alias_chain_preserves_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT x AS y FROM (SELECT amt AS x FROM t) ORDER BY y ASC",
    )
    .await;
    assert_arb_meta(&s, "y", 30, 4, "alias chain amt -> x -> y");
    assert_eq!(arb_values(&s, &b, "y")[0], Some("-100".into()));
}

#[tokio::test]
#[ignore = "FINDING E2 (generic, pre-dates decimal_arb): with the default physical rules replaced, EnforceSorting never inserts CoalescePartitionsExec/SortPreservingMergeExec under a SortExec, so ORDER BY over a multi-partition input (any UNION) silently returns ONLY the first branch's rows."]
async fn projection_pushdown_through_union_all_preserves_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt FROM (SELECT id, amt FROM t UNION ALL SELECT id, amt FROM u) ORDER BY amt ASC",
    )
    .await;
    assert_arb_meta(&s, "amt", 30, 4, "projection pushdown through UNION ALL");
    assert_eq!(nrows(&b), 8, "UNION ALL of 5 + 3 rows");
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn projection_pushdown_through_join_preserves_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT t.amt AS a FROM t JOIN u ON t.id = u.id - 9 ORDER BY t.amt ASC",
    )
    .await;
    assert_arb_meta(&s, "a", 30, 4, "projection pushdown through a join");
    assert_eq!(nrows(&b), 3, "ids 1,2,3 join u ids 10,11,12");
}

#[tokio::test]
async fn projection_pushdown_through_cte_preserves_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "WITH c AS (SELECT id, amt, alt FROM t) SELECT amt FROM c ORDER BY amt ASC",
    )
    .await;
    assert_arb_meta(&s, "amt", 30, 4, "projection pushdown through a CTE");
    assert_eq!(arb_values(&s, &b, "amt")[4], Some("1000".into()));
}

#[tokio::test]
async fn duplicate_decimal_arb_projection_keeps_metadata_on_both_outputs() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt AS a1, amt AS a2 FROM t ORDER BY id ASC").await;
    assert_arb_meta(&s, "a1", 30, 4, "duplicated projection");
    assert_arb_meta(&s, "a2", 30, 4, "duplicated projection");
    assert_eq!(arb_values(&s, &b, "a1"), arb_values(&s, &b, "a2"));
}

#[tokio::test]
async fn projection_of_decimal_arb_expression_keeps_widened_metadata() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt + alt AS s FROM t ORDER BY id ASC").await;
    // add: s = max(4,4) = 4, p = max(30-4, 30-4) + 4 + 1 = 31
    assert_arb_meta(&s, "s", 31, 4, "decimal_arb + decimal_arb projection");
    assert_eq!(
        arb_values(&s, &b, "s"),
        vec![
            Some("105".into()),
            Some("-96".into()),
            Some("3".into()),
            Some("1".into()),
            Some("1001".into())
        ]
    );
}

// =====================================================================
// C. Filter pushdown / predicate handling
// =====================================================================

#[tokio::test]
async fn filter_on_decimal_arb_pushed_to_scan_returns_correct_rows() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt > 0").await,
        vec![1, 5],
        "WHERE amt > 0 must keep only 100 and 1000"
    );
}

#[tokio::test]
async fn filter_on_decimal_arb_negative_threshold() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt < 0").await,
        vec![2, 4],
        "WHERE amt < 0 must keep -100 and -1 (not a bytewise comparison)"
    );
}

#[tokio::test]
async fn filter_on_outer_query_is_pushed_into_the_subquery() {
    let ctx = session();
    let plan =
        optimized_plan_text(&ctx, "SELECT id FROM (SELECT id, amt FROM t) WHERE amt > 0").await;
    assert!(
        plan.contains("decimal_arb_gt"),
        "the decimal_arb predicate must survive pushdown into the subquery. Plan:\n{plan}"
    );
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM (SELECT id, amt FROM t) WHERE amt > 0").await,
        vec![1, 5],
        "pushing the filter down must not change the result set"
    );
}

#[tokio::test]
async fn filter_pushed_through_union_all_applies_to_both_branches() {
    let ctx = session();
    let sql = "SELECT id FROM (SELECT id, amt FROM t UNION ALL SELECT id, amt FROM u) \
               WHERE amt > 0";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1, 5, 10, 12],
        "a decimal_arb predicate pushed through UNION ALL must be applied to \
         *both* inputs (t: 100, 1000; u: 100, 7)"
    );
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn filter_pushed_through_join_applies_to_the_correct_side() {
    let ctx = session();
    let sql = "SELECT t.id AS id FROM t JOIN u ON t.id = u.id - 9 WHERE t.amt < 0";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![2],
        "the decimal_arb predicate must be pushed onto `t` only (id 2, amt -100)"
    );
}

#[tokio::test]
async fn conjunction_of_decimal_arb_and_int_predicates() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt > 0 AND id > 1").await,
        vec![5],
        "AND of a decimal_arb and an Int64 predicate"
    );
}

#[tokio::test]
async fn disjunction_of_decimal_arb_and_int_predicates() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt > 500 OR id = 2").await,
        vec![2, 5],
        "OR of a decimal_arb and an Int64 predicate"
    );
}

#[tokio::test]
async fn negated_decimal_arb_predicate() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE NOT (amt > 0)").await,
        vec![2, 3, 4],
        "NOT (amt > 0) must keep -100, 0 and -1"
    );
}

#[tokio::test]
async fn column_to_column_decimal_arb_predicate() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt > alt").await,
        vec![1, 5],
        "amt > alt: 100>5 and 1000>1 only"
    );
}

#[tokio::test]
async fn between_predicate_inside_a_subquery() {
    let ctx = session();
    let sql = "SELECT id FROM (SELECT id, amt FROM t WHERE amt BETWEEN -1 AND 100)";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1, 3, 4],
        "BETWEEN over decimal_arb inside a subquery must desugar and filter correctly"
    );
}

#[tokio::test]
async fn in_list_predicate_inside_a_cte() {
    let ctx = session();
    let sql = "WITH c AS (SELECT id, amt FROM t WHERE amt IN (0, 1000)) SELECT id FROM c";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![3, 5],
        "IN over decimal_arb inside a CTE must desugar to OR-of-eq"
    );
}

#[tokio::test]
async fn not_in_list_predicate_pushed_down() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt NOT IN (0, 1000)").await,
        vec![1, 2, 4],
        "NOT IN over decimal_arb must desugar to AND-of-neq"
    );
}

#[tokio::test]
#[ignore = "FINDING D1: SUM/MIN/MAX/AVG over decimal_arb return an output Field with NO streamling.decimal_arb metadata (bare LargeBinary), so DecimalArbSortRewriteRule/DecimalArbExprPlanner cannot see the aggregate as decimal_arb."]
async fn having_over_decimal_arb_aggregate_filters_groups() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT grp FROM t GROUP BY grp HAVING SUM(amt) > 0 ORDER BY grp",
    )
    .await;
    let groups: Vec<String> = b
        .iter()
        .flat_map(|batch| {
            let c = batch
                .column_by_name("grp")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            (0..c.len())
                .map(|i| c.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        groups,
        vec!["a".to_string()],
        "HAVING SUM(decimal_arb) > 0 must keep only group `a` (1100), not `b` (-101)"
    );
}

#[tokio::test]
async fn duplicate_identical_predicates_do_not_change_the_result() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt > 0 AND amt > 0").await,
        vec![1, 5],
        "a repeated decimal_arb predicate must be idempotent"
    );
}

#[tokio::test]
async fn contradictory_decimal_arb_predicates_return_no_rows() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id FROM t WHERE amt > 0 AND amt < 0").await;
    assert_eq!(
        nrows(&b),
        0,
        "amt > 0 AND amt < 0 is unsatisfiable for every row"
    );
}

#[tokio::test]
async fn filter_on_null_decimal_arb_uses_three_valued_logic() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM tn WHERE amt > -100").await,
        vec![1, 3],
        "NULL amt rows must be excluded (comparison yields NULL, not TRUE)"
    );
}

#[tokio::test]
async fn is_null_on_decimal_arb_is_not_rewritten_and_works() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM tn WHERE amt IS NULL").await,
        vec![2, 4],
        "IS NULL over decimal_arb must select exactly the null rows"
    );
}

#[tokio::test]
async fn is_not_null_on_decimal_arb_works() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM tn WHERE amt IS NOT NULL").await,
        vec![1, 3],
        "IS NOT NULL over decimal_arb must select exactly the non-null rows"
    );
}

// =====================================================================
// D. Common-subexpression elimination
// =====================================================================

#[tokio::test]
async fn cse_over_repeated_decimal_arb_arithmetic_keeps_both_outputs_correct() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt + alt AS x, amt + alt AS y FROM t ORDER BY id ASC",
    )
    .await;
    assert_arb_meta(&s, "x", 31, 4, "CSE of a repeated decimal_arb sum");
    assert_arb_meta(&s, "y", 31, 4, "CSE of a repeated decimal_arb sum");
    let x = arb_values(&s, &b, "x");
    let y = arb_values(&s, &b, "y");
    assert_eq!(
        x, y,
        "the eliminated common subexpression must feed both outputs"
    );
    assert_eq!(x[0], Some("105".into()));
    assert_eq!(x[1], Some("-96".into()));
}

#[tokio::test]
async fn cse_extracts_the_repeated_expression_exactly_once() {
    let ctx = session();
    let plan = optimized_plan_text(&ctx, "SELECT amt + alt AS x, amt + alt AS y FROM t").await;
    assert_eq!(
        plan.matches("decimal_arb_add").count(),
        1,
        "CommonSubexprEliminate must collapse the two identical decimal_arb_add \
         calls into one. Plan:\n{plan}"
    );
}

#[tokio::test]
async fn cse_over_repeated_predicate_keeps_filter_semantics() {
    let ctx = session();
    assert_eq!(
        sorted_ids(
            &ctx,
            "SELECT id FROM t WHERE amt + alt > 0 AND amt + alt < 200"
        )
        .await,
        vec![1, 3, 4],
        "CSE over a repeated decimal_arb sum inside a filter must not change \
         which rows pass (105, 3, 1 are in (0, 200))"
    );
}

#[tokio::test]
async fn cse_between_select_list_and_filter() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT id, amt * alt AS p FROM t WHERE amt * alt > 0 ORDER BY id ASC",
    )
    .await;
    // mul: p = 30+30+1 = 61, s = min(4+4, 61) = 8
    assert_arb_meta(&s, "p", 61, 8, "CSE across SELECT and WHERE");
    assert_eq!(ints(&b, "id"), vec![Some(1), Some(5)]);
    assert_eq!(
        arb_values(&s, &b, "p"),
        vec![Some("500".into()), Some("1000".into())]
    );
}

#[tokio::test]
async fn cse_over_repeated_case_expression_preserves_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT CASE WHEN id > 2 THEN amt ELSE alt END AS c1, \
                CASE WHEN id > 2 THEN amt ELSE alt END AS c2 \
         FROM t ORDER BY id ASC",
    )
    .await;
    assert_arb_meta(&s, "c1", 30, 4, "CSE over a repeated CASE (F2 re-stamp)");
    assert_arb_meta(&s, "c2", 30, 4, "CSE over a repeated CASE (F2 re-stamp)");
    assert_eq!(
        arb_values(&s, &b, "c1"),
        vec![
            Some("5".into()),
            Some("4".into()),
            Some("0".into()),
            Some("-1".into()),
            Some("1000".into())
        ],
        "CSE must not disturb the CASE branch selection"
    );
}

#[tokio::test]
async fn cse_over_repeated_coalesce_preserves_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT COALESCE(amt, alt) AS c1, COALESCE(amt, alt) AS c2 FROM tn ORDER BY id ASC",
    )
    .await;
    assert_arb_meta(
        &s,
        "c1",
        30,
        4,
        "CSE over a repeated COALESCE (F2 re-stamp)",
    );
    assert_arb_meta(
        &s,
        "c2",
        30,
        4,
        "CSE over a repeated COALESCE (F2 re-stamp)",
    );
    assert_eq!(
        arb_values(&s, &b, "c1"),
        vec![
            Some("10".into()),
            Some("2".into()),
            Some("-10".into()),
            Some("4".into())
        ]
    );
}

#[tokio::test]
async fn cse_across_order_by_and_select_list() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT id, amt + alt AS x FROM t ORDER BY amt + alt ASC",
    )
    .await;
    assert_arb_meta(&s, "x", 31, 4, "CSE across ORDER BY and SELECT");
    assert_eq!(
        ints(&b, "id"),
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "sharing the sum between ORDER BY and the SELECT list must keep the sort numeric"
    );
}

#[tokio::test]
async fn cse_over_repeated_to_string_conversion() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT decimal_arb_to_string(amt) AS s1, decimal_arb_to_string(amt) AS s2 \
         FROM t ORDER BY id ASC",
    )
    .await;
    let read = |name: &str| -> Vec<String> {
        b.iter()
            .flat_map(|batch| {
                let c = batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                (0..c.len())
                    .map(|i| c.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    };
    assert_eq!(read("s1"), read("s2"));
    assert_eq!(read("s1")[1], "-100.0000");
}

#[tokio::test]
async fn cse_does_not_merge_differently_scaled_conversions() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt + alt AS a, amt - alt AS b FROM t ORDER BY id ASC",
    )
    .await;
    assert_eq!(arb_values(&s, &b, "a")[0], Some("105".into()));
    assert_eq!(arb_values(&s, &b, "b")[0], Some("95".into()));
}

// =====================================================================
// E. Expression simplification & constant folding
// =====================================================================

#[tokio::test]
async fn true_and_decimal_arb_predicate_is_simplified_without_changing_rows() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE TRUE AND amt > 0").await,
        vec![1, 5],
        "simplifying `TRUE AND p` to `p` must not change the result"
    );
}

#[tokio::test]
async fn false_or_decimal_arb_predicate_is_simplified_without_changing_rows() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE FALSE OR amt > 0").await,
        vec![1, 5],
        "simplifying `FALSE OR p` to `p` must not change the result"
    );
}

#[tokio::test]
async fn true_or_decimal_arb_predicate_short_circuits_to_all_rows() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE TRUE OR amt > 0").await,
        vec![1, 2, 3, 4, 5],
        "`TRUE OR p` must keep every row"
    );
}

#[tokio::test]
async fn false_and_decimal_arb_predicate_short_circuits_to_no_rows() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id FROM t WHERE FALSE AND amt > 0").await;
    assert_eq!(nrows(&b), 0, "`FALSE AND p` must drop every row");
}

#[tokio::test]
async fn tautological_int_predicate_beside_decimal_arb_predicate() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE 1 = 1 AND amt > 0").await,
        vec![1, 5],
        "constant-folding `1 = 1` must not disturb the decimal_arb predicate"
    );
}

#[tokio::test]
async fn contradictory_int_predicate_beside_decimal_arb_predicate() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id FROM t WHERE 1 = 0 AND amt > 0").await;
    assert_eq!(
        nrows(&b),
        0,
        "constant-folding `1 = 0` must empty the result"
    );
}

#[tokio::test]
async fn double_negation_of_a_decimal_arb_predicate() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE NOT (NOT (amt > 0))").await,
        vec![1, 5],
        "NOT NOT p must simplify back to p"
    );
}

#[tokio::test]
async fn is_true_over_a_decimal_arb_predicate() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE (amt > 0) IS TRUE").await,
        vec![1, 5],
        "`p IS TRUE` over a decimal_arb comparison must behave like `p`"
    );
}

#[tokio::test]
async fn is_not_true_over_a_decimal_arb_predicate_includes_nulls() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM tn WHERE (amt > 0) IS NOT TRUE").await,
        vec![2, 3, 4],
        "`p IS NOT TRUE` must include the NULL-amt rows (2, 4) and -10 (3)"
    );
}

#[tokio::test]
async fn self_equality_on_decimal_arb_is_not_folded_to_true_for_nulls() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM tn WHERE amt = amt").await,
        vec![1, 3],
        "`x = x` must NOT be constant-folded to TRUE over a nullable decimal_arb \
         column — the NULL rows must drop out"
    );
}

#[tokio::test]
async fn self_inequality_on_decimal_arb_yields_no_rows() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id FROM t WHERE amt != amt").await;
    assert_eq!(
        nrows(&b),
        0,
        "`x != x` must be false for every non-null row"
    );
}

#[tokio::test]
async fn single_element_in_list_over_decimal_arb() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt IN (1000)").await,
        vec![5],
        "a one-element IN list must desugar to a single decimal_arb_eq"
    );
}

#[tokio::test]
async fn degenerate_between_with_equal_bounds() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt BETWEEN 0 AND 0").await,
        vec![3],
        "BETWEEN x AND x must behave like equality"
    );
}

#[tokio::test]
async fn degenerate_not_between_with_equal_bounds() {
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt NOT BETWEEN 0 AND 0").await,
        vec![1, 2, 4, 5],
        "NOT BETWEEN x AND x must be the complement of equality"
    );
}

#[tokio::test]
async fn inverted_between_bounds_select_nothing() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id FROM t WHERE amt BETWEEN 100 AND 0").await;
    assert_eq!(
        nrows(&b),
        0,
        "BETWEEN with low > high is unsatisfiable, not silently swapped"
    );
}

#[tokio::test]
async fn case_with_constant_true_condition_folds_to_the_then_branch_with_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT CASE WHEN TRUE THEN amt ELSE alt END AS c FROM t ORDER BY id ASC",
    )
    .await;
    assert_arb_meta(
        &s,
        "c",
        30,
        4,
        "constant-folding a CASE whose branches are decimal_arb",
    );
    assert_eq!(
        arb_values(&s, &b, "c"),
        vec![
            Some("100".into()),
            Some("-100".into()),
            Some("0".into()),
            Some("-1".into()),
            Some("1000".into())
        ],
        "CASE WHEN TRUE must fold to the THEN branch (amt) and keep its metadata"
    );
}

#[tokio::test]
async fn case_with_constant_false_condition_folds_to_the_else_branch_with_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT CASE WHEN FALSE THEN amt ELSE alt END AS c FROM t ORDER BY id ASC",
    )
    .await;
    assert_arb_meta(&s, "c", 30, 4, "constant-folding a CASE to its ELSE branch");
    assert_eq!(
        arb_values(&s, &b, "c"),
        vec![
            Some("5".into()),
            Some("4".into()),
            Some("3".into()),
            Some("2".into()),
            Some("1".into())
        ]
    );
}

#[tokio::test]
async fn coalesce_of_a_single_decimal_arb_argument_keeps_metadata() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT COALESCE(amt) AS c FROM t ORDER BY id ASC").await;
    assert_arb_meta(&s, "c", 30, 4, "COALESCE with one decimal_arb argument");
    assert_eq!(arb_values(&s, &b, "c")[1], Some("-100".into()));
}

#[tokio::test]
#[ignore = "FINDING D5: COALESCE(decimal_arb_col, NULL) fails to plan ('No function matches ... coalesce(LargeBinary, Null)') while COALESCE(int_col, NULL) plans fine."]
async fn coalesce_with_explicit_null_branch_keeps_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT COALESCE(amt, NULL) AS c FROM tn ORDER BY id ASC",
    )
    .await;
    assert_arb_meta(
        &s,
        "c",
        30,
        4,
        "COALESCE(decimal_arb, NULL) — the NULL branch must not veto the re-stamp",
    );
    assert_eq!(
        arb_values(&s, &b, "c"),
        vec![Some("10".into()), None, Some("-10".into()), None]
    );
}

#[tokio::test]
async fn case_with_explicit_null_else_keeps_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT CASE WHEN id <= 2 THEN amt ELSE NULL END AS c FROM t ORDER BY id ASC",
    )
    .await;
    assert_arb_meta(&s, "c", 30, 4, "CASE with an explicit NULL ELSE branch");
    assert_eq!(
        arb_values(&s, &b, "c"),
        vec![Some("100".into()), Some("-100".into()), None, None, None]
    );
}

#[tokio::test]
async fn constant_folded_integer_literal_bound_still_carries_decimal_arb_metadata() {
    let ctx = session();
    // `amt > 0` becomes decimal_arb_gt(amt, to_decimal_arb_from_int(0, 20, 0)).
    // SimplifyExpressions' const-evaluator folds the inner cast to a literal;
    // that literal must keep its decimal_arb field metadata or the comparison
    // UDF errors with "input field is not a streamling.decimal_arb column".
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM t WHERE amt > 0").await,
        vec![1, 5],
        "constant-folding the coerced integer bound must preserve its \
         decimal_arb field metadata"
    );
}

#[tokio::test]
async fn constant_folded_bound_in_projection_still_evaluates() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id, amt >= 0 AS nonneg FROM t ORDER BY id ASC").await;
    assert_eq!(
        bools(&b, "nonneg"),
        vec![Some(true), Some(false), Some(true), Some(false), Some(true)],
        "a folded decimal_arb comparison in the SELECT list must still evaluate"
    );
}

#[tokio::test]
async fn arithmetic_with_literal_zero_is_not_folded_away_but_stays_correct() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt + 0 AS z FROM t ORDER BY id ASC").await;
    // add(decimal_arb(30,4), decimal_arb(20,0)) -> s = 4, p = max(26, 20) + 4 + 1 = 31
    assert_arb_meta(&s, "z", 31, 4, "amt + 0");
    assert_eq!(
        arb_values(&s, &b, "z"),
        vec![
            Some("100".into()),
            Some("-100".into()),
            Some("0".into()),
            Some("-1".into()),
            Some("1000".into())
        ],
        "adding the literal 0 must be value-preserving"
    );
}

#[tokio::test]
async fn multiplication_by_literal_one_is_value_preserving() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt * 1 AS z FROM t ORDER BY id ASC").await;
    assert_eq!(
        arb_values(&s, &b, "z"),
        vec![
            Some("100".into()),
            Some("-100".into()),
            Some("0".into()),
            Some("-1".into()),
            Some("1000".into())
        ],
        "multiplying by the literal 1 must be value-preserving"
    );
}

// =====================================================================
// F. Limit pushdown
// =====================================================================

#[tokio::test]
async fn limit_without_order_returns_the_requested_row_count() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id FROM t LIMIT 3").await;
    assert_eq!(nrows(&b), 3, "LIMIT 3 must return exactly three rows");
}

#[tokio::test]
async fn limit_pushed_into_a_subquery_keeps_decimal_arb_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt FROM (SELECT amt FROM t ORDER BY amt ASC LIMIT 3) ORDER BY amt ASC",
    )
    .await;
    assert_arb_meta(&s, "amt", 30, 4, "limit pushdown through a subquery");
    assert_eq!(
        arb_values(&s, &b, "amt"),
        vec![Some("-100".into()), Some("-1".into()), Some("0".into())]
    );
}

#[tokio::test]
async fn limit_outside_a_filtered_subquery() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT id FROM (SELECT id, amt FROM t WHERE amt > 0) ORDER BY id ASC LIMIT 1",
    )
    .await;
    assert_eq!(
        ints(&b, "id"),
        vec![Some(1)],
        "LIMIT above a pushed-down decimal_arb filter must take from the filtered set"
    );
}

#[tokio::test]
#[ignore = "FINDING E2 (generic, pre-dates decimal_arb): with the default physical rules replaced, EnforceSorting never inserts CoalescePartitionsExec/SortPreservingMergeExec under a SortExec, so ORDER BY over a multi-partition input (any UNION) silently returns ONLY the first branch's rows."]
async fn limit_through_union_all_still_yields_correct_total() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT id FROM (SELECT id, amt FROM t UNION ALL SELECT id, amt FROM u) \
         ORDER BY amt ASC LIMIT 3",
    )
    .await;
    let got: Vec<i64> = ints(&b, "id").into_iter().flatten().collect();
    assert_eq!(got.len(), 3);
    assert_eq!(
        got[0], 2,
        "the numerically smallest value across both branches is -100 (id 2); got {got:?}"
    );
    let mut tail = got[1..].to_vec();
    tail.sort_unstable();
    assert_eq!(
        tail,
        vec![4, 11],
        "the next two are the -1 rows from t and u; got {got:?}"
    );
}

#[tokio::test]
async fn limit_with_distinct_over_decimal_arb() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT DISTINCT amt FROM t ORDER BY amt DESC LIMIT 2").await;
    assert_arb_meta(&s, "amt", 30, 4, "DISTINCT + ORDER BY + LIMIT");
    assert_eq!(
        arb_values(&s, &b, "amt"),
        vec![Some("1000".into()), Some("100".into())]
    );
}

#[tokio::test]
async fn offset_beyond_the_row_count_returns_nothing() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id FROM t ORDER BY amt ASC LIMIT 5 OFFSET 10").await;
    assert_eq!(nrows(&b), 0, "an OFFSET past the end must return no rows");
}

// =====================================================================
// G. Joins on decimal_arb keys
// =====================================================================

#[tokio::test]
async fn inner_join_on_decimal_arb_equality_matches_numerically() {
    let ctx = session();
    let sql = "SELECT t.id AS id FROM t JOIN u ON t.amt = u.amt";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1, 4],
        "INNER JOIN on decimal_arb equality must match 100 and -1"
    );
}

#[tokio::test]
async fn inner_join_on_decimal_arb_preserves_metadata_of_both_sides() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT t.amt AS l, u.amt AS r FROM t JOIN u ON t.amt = u.amt ORDER BY t.amt ASC",
    )
    .await;
    assert_arb_meta(&s, "l", 30, 4, "inner join output (left key)");
    assert_arb_meta(&s, "r", 30, 4, "inner join output (right key)");
    assert_eq!(
        arb_values(&s, &b, "l"),
        vec![Some("-1".into()), Some("100".into())]
    );
    assert_eq!(arb_values(&s, &b, "l"), arb_values(&s, &b, "r"));
}

#[tokio::test]
async fn left_join_on_decimal_arb_keeps_unmatched_left_rows() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT t.id AS id, u.id AS uid FROM t LEFT JOIN u ON t.amt = u.amt ORDER BY t.id ASC",
    )
    .await;
    assert_eq!(nrows(&b), 5, "LEFT JOIN must keep all five left rows");
    assert_eq!(
        ints(&b, "uid"),
        vec![Some(10), None, None, Some(11), None],
        "only 100 (id 1) and -1 (id 4) have a match"
    );
}

#[tokio::test]
async fn right_join_on_decimal_arb_keeps_unmatched_right_rows() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT t.id AS id, u.id AS uid FROM t RIGHT JOIN u ON t.amt = u.amt ORDER BY u.id ASC",
    )
    .await;
    assert_eq!(nrows(&b), 3, "RIGHT JOIN must keep all three right rows");
    assert_eq!(
        ints(&b, "id"),
        vec![Some(1), Some(4), None],
        "u row 12 (amt 7) has no match in t"
    );
}

#[tokio::test]
async fn full_outer_join_on_decimal_arb_keeps_both_unmatched_sides() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT t.id AS id, u.id AS uid FROM t FULL OUTER JOIN u ON t.amt = u.amt",
    )
    .await;
    assert_eq!(
        nrows(&b),
        6,
        "FULL OUTER: 2 matched + 3 unmatched left + 1 unmatched right"
    );
}

#[tokio::test]
async fn semi_join_via_exists_over_decimal_arb() {
    let ctx = session();
    let sql = "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.amt = t.amt)";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1, 4],
        "EXISTS (semi join) over a decimal_arb correlation must match 100 and -1"
    );
}

#[tokio::test]
async fn anti_join_via_not_exists_over_decimal_arb() {
    let ctx = session();
    let sql = "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.amt = t.amt)";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![2, 3, 5],
        "NOT EXISTS (anti join) must keep the unmatched -100, 0, 1000 rows"
    );
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn in_subquery_over_decimal_arb_behaves_like_a_semi_join() {
    let ctx = session();
    let sql = "SELECT id FROM t WHERE amt IN (SELECT amt FROM u)";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1, 4],
        "IN (subquery) over decimal_arb must match numerically-equal rows"
    );
}

#[tokio::test]
async fn not_in_subquery_over_decimal_arb_behaves_like_an_anti_join() {
    let ctx = session();
    let sql = "SELECT id FROM t WHERE amt NOT IN (SELECT amt FROM u)";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![2, 3, 5],
        "NOT IN (subquery) over decimal_arb must exclude the matched rows"
    );
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn join_using_a_decimal_arb_column_at_the_same_scale() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT t.id AS id FROM t JOIN u USING (amt)").await;
    let mut got: Vec<i64> = ints(&b, "id").into_iter().flatten().collect();
    got.sort_unstable();
    assert_eq!(
        got,
        vec![1, 4],
        "JOIN ... USING(decimal_arb) at a shared scale must match on canonical bytes"
    );
}

#[tokio::test]
async fn join_on_decimal_arb_with_an_extra_decimal_arb_filter() {
    let ctx = session();
    let sql = "SELECT t.id AS id FROM t JOIN u ON t.amt = u.amt WHERE t.amt > 0";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1],
        "an additional decimal_arb predicate on a join must be pushed down safely"
    );
}

#[tokio::test]
async fn join_on_a_decimal_arb_inequality_is_a_correct_nested_loop_join() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT t.id AS id, u.id AS uid FROM t JOIN u ON t.amt > u.amt",
    )
    .await;
    // t.amt: 100, -100, 0, -1, 1000 ; u.amt: 100, -1, 7
    // 100 > {-1, 7} ; -100 > {} ; 0 > {-1} ; -1 > {} ; 1000 > {100, -1, 7}
    assert_eq!(
        nrows(&b),
        6,
        "the inequality join must produce 2 + 0 + 1 + 0 + 3 = 6 pairs"
    );
}

#[tokio::test]
async fn self_join_on_decimal_arb_equality() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT a.id AS id FROM t a JOIN t b ON a.amt = b.amt").await;
    assert_eq!(
        nrows(&b),
        5,
        "every amt is distinct, so a self join on amt yields exactly five rows"
    );
}

#[tokio::test]
async fn three_way_join_on_decimal_arb_keys() {
    let ctx = session();
    let sql = "SELECT t.id AS id FROM t JOIN u ON t.amt = u.amt JOIN t t2 ON t2.amt = u.amt";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1, 4],
        "a three-way decimal_arb join must still match only 100 and -1"
    );
}

#[tokio::test]
async fn join_on_decimal_arb_across_different_scales_matches_numerically() {
    let ctx = session();
    // t.amt is decimal_arb(30,4), w.amt is decimal_arb(20,2) — same numbers.
    let sql = "SELECT t.id AS id FROM t JOIN w ON t.amt = w.amt";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1, 4],
        "`ON a = b` routes through decimal_arb_eq, which decodes each side at its \
         own declared scale, so 100 (scale 4) must match 100 (scale 2)"
    );
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn join_using_across_different_scales_matches_numerically() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT t.id AS id FROM t JOIN w USING (amt)").await;
    let mut got: Vec<i64> = ints(&b, "id").into_iter().flatten().collect();
    got.sort_unstable();
    assert_eq!(
        got,
        vec![1, 4],
        "JOIN ... USING(decimal_arb) must agree with `ON a.amt = b.amt`: both are \
         NUMERIC equality. USING builds a raw LargeBinary equijoin, and the \
         canonical encoding is scale-relative, so scale-4 and scale-2 encodings \
         of the same number never compare equal"
    );
}

#[tokio::test]
async fn left_join_then_order_by_decimal_arb_is_numeric() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT t.id AS id FROM t LEFT JOIN u ON t.amt = u.amt ORDER BY t.amt ASC",
    )
    .await;
    assert_eq!(
        ints(&b, "id"),
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "post-LEFT-JOIN ORDER BY on decimal_arb must stay numeric"
    );
}

#[tokio::test]
async fn join_output_of_a_decimal_arb_expression_keeps_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT t.amt + u.amt AS s FROM t JOIN u ON t.amt = u.amt ORDER BY t.amt ASC",
    )
    .await;
    assert_arb_meta(&s, "s", 31, 4, "arithmetic across a join");
    assert_eq!(
        arb_values(&s, &b, "s"),
        vec![Some("-2".into()), Some("200".into())]
    );
}

// =====================================================================
// H. Set operations
// =====================================================================

#[tokio::test]
async fn union_all_over_decimal_arb_keeps_every_row_and_the_metadata() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t UNION ALL SELECT amt FROM u").await;
    assert_arb_meta(&s, "amt", 30, 4, "UNION ALL over decimal_arb");
    assert_eq!(nrows(&b), 8);
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-1", "-1", "-100", "0", "100", "100", "1000", "7"],
        "UNION ALL must carry through every value unchanged"
    );
}

#[tokio::test]
#[ignore = "FINDING E3 (generic, pre-dates decimal_arb): AggregateExec(FinalPartitioned) runs per input partition with no hash RepartitionExec, so UNION (DISTINCT) does not deduplicate across union branches (SELECT id FROM t UNION SELECT id FROM t returns 10 rows, not 5)."]
async fn union_distinct_over_decimal_arb_dedupes_numerically_equal_values() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t UNION SELECT amt FROM u").await;
    assert_arb_meta(&s, "amt", 30, 4, "UNION (distinct) over decimal_arb");
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-1", "-100", "0", "100", "1000", "7"],
        "UNION must dedupe 100 and -1, leaving six distinct values"
    );
}

#[tokio::test]
async fn union_all_of_a_table_with_itself_doubles_the_rows() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT amt FROM t UNION ALL SELECT amt FROM t").await;
    assert_eq!(nrows(&b), 10);
}

#[tokio::test]
#[ignore = "FINDING E3 (generic, pre-dates decimal_arb): AggregateExec(FinalPartitioned) runs per input partition with no hash RepartitionExec, so UNION (DISTINCT) does not deduplicate across union branches (SELECT id FROM t UNION SELECT id FROM t returns 10 rows, not 5)."]
async fn union_distinct_of_a_table_with_itself_is_idempotent() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t UNION SELECT amt FROM t").await;
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-1", "-100", "0", "100", "1000"],
        "UNION with itself must collapse to the five distinct values"
    );
}

#[tokio::test]
async fn three_way_union_all_over_decimal_arb() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt FROM t UNION ALL SELECT amt FROM u UNION ALL SELECT amt FROM t",
    )
    .await;
    assert_arb_meta(&s, "amt", 30, 4, "three-way UNION ALL");
    assert_eq!(nrows(&b), 13);
}

#[tokio::test]
async fn nested_union_all_flattening_preserves_values() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt FROM (SELECT amt FROM t UNION ALL SELECT amt FROM u) \
         UNION ALL SELECT amt FROM u",
    )
    .await;
    assert_arb_meta(&s, "amt", 30, 4, "nested UNION ALL (EliminateNestedUnion)");
    assert_eq!(nrows(&b), 11);
}

#[tokio::test]
#[ignore = "FINDING E2 (generic, pre-dates decimal_arb): with the default physical rules replaced, EnforceSorting never inserts CoalescePartitionsExec/SortPreservingMergeExec under a SortExec, so ORDER BY over a multi-partition input (any UNION) silently returns ONLY the first branch's rows."]
async fn union_all_then_group_by_decimal_arb() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt, COUNT(*) AS n FROM (SELECT amt FROM t UNION ALL SELECT amt FROM u) \
         GROUP BY amt ORDER BY amt ASC",
    )
    .await;
    assert_arb_meta(&s, "amt", 30, 4, "GROUP BY after UNION ALL");
    assert_eq!(
        arb_values(&s, &b, "amt"),
        vec![
            Some("-100".into()),
            Some("-1".into()),
            Some("0".into()),
            Some("7".into()),
            Some("100".into()),
            Some("1000".into())
        ]
    );
    assert_eq!(
        ints(&b, "n"),
        vec![Some(1), Some(2), Some(1), Some(1), Some(2), Some(1)],
        "the duplicated 100 and -1 must each group to a count of 2"
    );
}

#[tokio::test]
async fn union_all_then_filter_on_decimal_arb() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt FROM (SELECT amt FROM t UNION ALL SELECT amt FROM u) WHERE amt < 0",
    )
    .await;
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-1", "-1", "-100"],
        "a decimal_arb filter over a UNION ALL must see both branches"
    );
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn except_over_decimal_arb_removes_matching_values() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t EXCEPT SELECT amt FROM u").await;
    assert_arb_meta(&s, "amt", 30, 4, "EXCEPT over decimal_arb");
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-100", "0", "1000"],
        "EXCEPT must remove the 100 and -1 rows present in u"
    );
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn intersect_over_decimal_arb_keeps_common_values() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t INTERSECT SELECT amt FROM u").await;
    assert_arb_meta(&s, "amt", 30, 4, "INTERSECT over decimal_arb");
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-1", "100"],
        "INTERSECT must keep exactly the shared 100 and -1"
    );
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn except_of_a_table_with_itself_is_empty() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT amt FROM t EXCEPT SELECT amt FROM t").await;
    assert_eq!(nrows(&b), 0, "X EXCEPT X must be empty");
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn intersect_of_a_table_with_itself_is_the_distinct_table() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t INTERSECT SELECT amt FROM t").await;
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-1", "-100", "0", "100", "1000"]
    );
}

#[tokio::test]
#[ignore = "FINDING E2 (generic, pre-dates decimal_arb): with the default physical rules replaced, EnforceSorting never inserts CoalescePartitionsExec/SortPreservingMergeExec under a SortExec, so ORDER BY over a multi-partition input (any UNION) silently returns ONLY the first branch's rows."]
async fn union_then_order_by_decimal_arb_is_numeric() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT amt FROM t UNION SELECT amt FROM u ORDER BY amt ASC",
    )
    .await;
    assert_eq!(
        arb_values(&s, &b, "amt"),
        vec![
            Some("-100".into()),
            Some("-1".into()),
            Some("0".into()),
            Some("7".into()),
            Some("100".into()),
            Some("1000".into())
        ],
        "ORDER BY after a UNION must be numerically ordered"
    );
}

#[tokio::test]
#[ignore = "FINDING D2: UNION / UNION ALL of two decimal_arb columns with different declared scales silently re-interprets one branch's canonical bytes at the other branch's scale (values off by 10^|s1-s2|)."]
async fn union_all_across_different_scales_does_not_corrupt_values() {
    let ctx = session();
    // t.amt is decimal_arb(30,4); w.amt is decimal_arb(20,2) holding the same
    // numbers (100, -1, 7). The union's output field declares ONE scale, but the
    // canonical byte encoding is scale-relative, so the narrower branch's rows
    // are re-interpreted at the wrong scale.
    let (s, b) = run(&ctx, "SELECT amt FROM t UNION ALL SELECT amt FROM w").await;
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-1", "-1", "-100", "0", "100", "100", "1000", "7"],
        "UNION ALL of two decimal_arb columns with different declared scales must \
         preserve every value; decoding the scale-2 branch at the union's \
         declared scale silently divides those rows by 100"
    );
}

#[tokio::test]
#[ignore = "FINDING D2: UNION / UNION ALL of two decimal_arb columns with different declared scales silently re-interprets one branch's canonical bytes at the other branch's scale (values off by 10^|s1-s2|)."]
async fn union_distinct_across_different_scales_dedupes_numerically() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t UNION SELECT amt FROM w").await;
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-1", "-100", "0", "100", "1000", "7"],
        "UNION over differently-scaled decimal_arb columns must dedupe by NUMERIC \
         value: 100 (scale 4) and 100 (scale 2) are the same number"
    );
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn intersect_across_different_scales_matches_numerically() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t INTERSECT SELECT amt FROM w").await;
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-1", "100"],
        "INTERSECT over differently-scaled decimal_arb columns must match \
         numerically (100 and -1 are present in both)"
    );
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn except_across_different_scales_removes_numerically_equal_values() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t EXCEPT SELECT amt FROM w").await;
    assert_eq!(
        sorted_arb(&s, &b, "amt"),
        vec!["-100", "0", "1000"],
        "EXCEPT over differently-scaled decimal_arb columns must remove the \
         numerically-equal 100 and -1"
    );
}

#[tokio::test]
async fn union_all_of_decimal_arb_with_a_null_branch_keeps_metadata() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT amt FROM t UNION ALL SELECT amt FROM tn").await;
    assert_arb_meta(&s, "amt", 30, 4, "UNION ALL with a nullable branch");
    assert_eq!(nrows(&b), 9);
    let vals = arb_values(&s, &b, "amt");
    assert_eq!(
        vals.iter().filter(|v| v.is_none()).count(),
        2,
        "the two NULL rows from `tn` must survive the union"
    );
}

// =====================================================================
// I. CTEs, nested subqueries, correlated subqueries, views
// =====================================================================

#[tokio::test]
async fn simple_cte_preserves_decimal_arb_metadata_and_values() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "WITH c AS (SELECT id, amt FROM t) SELECT amt FROM c ORDER BY amt ASC",
    )
    .await;
    assert_arb_meta(&s, "amt", 30, 4, "simple CTE");
    assert_eq!(arb_values(&s, &b, "amt")[0], Some("-100".into()));
}

#[tokio::test]
async fn cte_referenced_twice_in_a_self_join() {
    let ctx = session();
    let sql = "WITH c AS (SELECT id, amt FROM t) \
               SELECT a.id AS id FROM c a JOIN c b ON a.amt = b.amt";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1, 2, 3, 4, 5],
        "a CTE referenced twice and self-joined on decimal_arb must match each \
         row with itself exactly once"
    );
}

#[tokio::test]
async fn cte_with_a_decimal_arb_expression_keeps_widened_metadata() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "WITH c AS (SELECT id, amt + alt AS s FROM t) SELECT s FROM c ORDER BY s ASC",
    )
    .await;
    assert_arb_meta(&s, "s", 31, 4, "decimal_arb expression inside a CTE");
    assert_eq!(arb_values(&s, &b, "s")[0], Some("-96".into()));
}

#[tokio::test]
async fn cte_chain_of_two_preserves_metadata() {
    let ctx = session();
    let sql = "WITH c1 AS (SELECT id, amt FROM t WHERE amt > -50), \
                    c2 AS (SELECT id, amt FROM c1 WHERE amt < 500) \
               SELECT amt FROM c2 ORDER BY amt ASC";
    let (s, b) = run(&ctx, sql).await;
    assert_arb_meta(&s, "amt", 30, 4, "chained CTEs");
    assert_eq!(
        arb_values(&s, &b, "amt"),
        vec![Some("-1".into()), Some("0".into()), Some("100".into())],
        "chained decimal_arb filters must compose numerically"
    );
}

#[tokio::test]
async fn nested_subqueries_four_levels_deep_preserve_metadata() {
    let ctx = session();
    let sql = "SELECT amt FROM (SELECT amt FROM (SELECT amt FROM (SELECT amt FROM t))) \
               ORDER BY amt ASC";
    let (s, b) = run(&ctx, sql).await;
    assert_arb_meta(&s, "amt", 30, 4, "four nested subqueries");
    assert_eq!(arb_values(&s, &b, "amt").len(), 5);
}

#[tokio::test]
async fn scalar_subquery_over_decimal_arb_in_the_select_list() {
    let ctx = session();
    let (s, b) = run(
        &ctx,
        "SELECT id, (SELECT MIN(amt) FROM t) AS lo FROM t ORDER BY id ASC LIMIT 1",
    )
    .await;
    assert_eq!(
        arb_values_at(&b, "lo", 4),
        vec![Some("-100".into())],
        "a scalar subquery returning decimal_arb must return the numeric minimum"
    );
    let _ = s;
}

#[tokio::test]
async fn scalar_subquery_over_decimal_arb_in_a_predicate() {
    let ctx = session();
    let sql = "SELECT id FROM t WHERE amt = (SELECT MAX(amt) FROM t)";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![5],
        "comparing a decimal_arb column to a scalar subquery must match id 5 (1000)"
    );
}

#[tokio::test]
#[ignore = "FINDING D1: SUM/MIN/MAX/AVG over decimal_arb return an output Field with NO streamling.decimal_arb metadata (bare LargeBinary), so DecimalArbSortRewriteRule/DecimalArbExprPlanner cannot see the aggregate as decimal_arb."]
async fn correlated_subquery_over_decimal_arb() {
    let ctx = session();
    let sql = "SELECT id FROM t WHERE amt > (SELECT MIN(amt) FROM u WHERE u.id > 10)";
    // MIN over u where id > 10 -> min(-1, 7) = -1 ; t.amt > -1 -> 100, 0, 1000
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1, 3, 5],
        "a correlated/limited scalar subquery over decimal_arb must compare numerically"
    );
}

#[tokio::test]
async fn view_over_decimal_arb_preserves_metadata_and_values() {
    let ctx = session();
    ctx.sql("CREATE VIEW v_amt AS SELECT id, amt FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let (s, b) = run(&ctx, "SELECT amt FROM v_amt ORDER BY amt ASC").await;
    assert_arb_meta(&s, "amt", 30, 4, "SELECT from a view over decimal_arb");
    assert_eq!(
        arb_values(&s, &b, "amt"),
        vec![
            Some("-100".into()),
            Some("-1".into()),
            Some("0".into()),
            Some("100".into()),
            Some("1000".into())
        ]
    );
}

#[tokio::test]
async fn filtered_view_over_decimal_arb_pushes_the_predicate_down() {
    let ctx = session();
    ctx.sql("CREATE VIEW v_pos AS SELECT id, amt FROM t WHERE amt > 0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        sorted_ids(&ctx, "SELECT id FROM v_pos").await,
        vec![1, 5],
        "a decimal_arb predicate baked into a view must survive view expansion"
    );
}

#[tokio::test]
async fn view_with_a_decimal_arb_expression_keeps_widened_metadata() {
    let ctx = session();
    ctx.sql("CREATE VIEW v_sum AS SELECT id, amt + alt AS s FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let (s, b) = run(&ctx, "SELECT s FROM v_sum ORDER BY s ASC").await;
    assert_arb_meta(&s, "s", 31, 4, "view exposing a decimal_arb expression");
    assert_eq!(arb_values(&s, &b, "s")[4], Some("1001".into()));
}

#[tokio::test]
async fn view_joined_with_a_base_table_on_decimal_arb() {
    let ctx = session();
    ctx.sql("CREATE VIEW v_join AS SELECT id, amt FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let sql = "SELECT v.id AS id FROM v_join v JOIN u ON v.amt = u.amt";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![1, 4],
        "joining a view's decimal_arb column against a base table must match numerically"
    );
}

#[tokio::test]
async fn view_ordered_by_decimal_arb_is_rewritten_at_the_outer_level() {
    let ctx = session();
    ctx.sql("CREATE VIEW v_ord AS SELECT id, amt FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let (_, b) = run(&ctx, "SELECT id FROM v_ord ORDER BY amt ASC").await;
    assert_eq!(
        ints(&b, "id"),
        vec![Some(2), Some(4), Some(3), Some(1), Some(5)],
        "ORDER BY over a view's decimal_arb column must still be rewritten"
    );
}

#[tokio::test]
#[ignore = "FINDING E2 (generic, pre-dates decimal_arb): with the default physical rules replaced, EnforceSorting never inserts CoalescePartitionsExec/SortPreservingMergeExec under a SortExec, so ORDER BY over a multi-partition input (any UNION) silently returns ONLY the first branch's rows."]
async fn union_inside_a_cte_then_ordered_by_decimal_arb() {
    let ctx = session();
    let sql = "WITH c AS (SELECT id, amt FROM t UNION ALL SELECT id, amt FROM u) \
               SELECT id FROM c ORDER BY amt ASC LIMIT 3";
    let (_, b) = run(&ctx, sql).await;
    let mut got: Vec<i64> = ints(&b, "id").into_iter().flatten().collect();
    got.sort_unstable();
    assert_eq!(
        got,
        vec![2, 4, 11],
        "the three numerically smallest rows across the union are -100 (id 2) and \
         the two -1 rows (id 4 from t, id 11 from u)"
    );
}

#[tokio::test]
async fn group_by_inside_a_cte_then_joined_on_decimal_arb() {
    let ctx = session();
    let sql = "WITH g AS (SELECT amt, COUNT(*) AS n FROM t GROUP BY amt) \
               SELECT u.id AS id FROM g JOIN u ON g.amt = u.amt";
    assert_eq!(
        sorted_ids(&ctx, sql).await,
        vec![10, 11],
        "a GROUP BY key of decimal_arb must join correctly against another table"
    );
}

#[tokio::test]
async fn deeply_nested_filter_and_sort_combination_stays_correct() {
    let ctx = session();
    let sql = "SELECT id FROM ( \
                 SELECT id, amt FROM ( \
                   SELECT id, amt FROM t WHERE amt > -200 \
                 ) WHERE amt < 500 \
               ) ORDER BY amt DESC";
    let (_, b) = run(&ctx, sql).await;
    assert_eq!(
        ints(&b, "id"),
        vec![Some(1), Some(3), Some(4), Some(2)],
        "stacked decimal_arb filters plus a rewritten sort: 100, 0, -1, -100"
    );
}

#[tokio::test]
async fn empty_result_from_a_decimal_arb_filter_keeps_the_output_schema() {
    let ctx = session();
    let df = ctx
        .sql("SELECT amt FROM t WHERE amt > 100000")
        .await
        .unwrap();
    let logical: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await.unwrap();
    assert_eq!(nrows(&batches), 0);
    assert_arb_meta(
        &logical,
        "amt",
        30,
        4,
        "an empty result must still declare decimal_arb metadata",
    );
}

#[tokio::test]
async fn decimal_arb_comparison_against_a_wrongly_typed_operand_errors_cleanly() {
    let ctx = session();
    let msg = run_err(&ctx, "SELECT id FROM t WHERE amt > CAST(1 AS DOUBLE)").await;
    assert!(
        !msg.contains("panicked"),
        "a float comparison must surface a typed error, not a panic: {msg}"
    );
}

#[tokio::test]
async fn optimizer_does_not_fold_a_decimal_arb_comparison_into_a_constant() {
    let ctx = session();
    let plan = optimized_plan_text(&ctx, "SELECT id FROM t WHERE amt > 0").await;
    assert!(
        plan.contains("decimal_arb_gt"),
        "the decimal_arb comparison must survive optimization as a real predicate, \
         not be folded to a constant. Plan:\n{plan}"
    );
}

#[tokio::test]
async fn optimizer_keeps_the_filter_below_the_projection() {
    let ctx = session();
    let plan =
        optimized_plan_text(&ctx, "SELECT id, amt FROM t WHERE amt > 0 ORDER BY amt ASC").await;
    let filter_at = plan.find("Filter:");
    let sort_at = plan.find("Sort:");
    assert!(filter_at.is_some(), "expected a Filter node. Plan:\n{plan}");
    assert!(sort_at.is_some(), "expected a Sort node. Plan:\n{plan}");
    assert!(
        sort_at.unwrap() < filter_at.unwrap(),
        "the Filter must sit *below* the Sort so the TopK only sees matching \
         rows. Plan:\n{plan}"
    );
}

// =====================================================================
// J. Root-cause probes: decimal_arb metadata on aggregate outputs
//    (`DecimalArbSortRewriteRule` and `DecimalArbExprPlanner` both key off
//     the field metadata, so losing it silently degrades to bytewise
//     LargeBinary semantics.)
// =====================================================================

#[tokio::test]
#[ignore = "FINDING D1: SUM/MIN/MAX/AVG over decimal_arb return an output Field with NO streamling.decimal_arb metadata (bare LargeBinary), so DecimalArbSortRewriteRule/DecimalArbExprPlanner cannot see the aggregate as decimal_arb."]
async fn min_aggregate_output_field_keeps_decimal_arb_metadata() {
    let ctx = session();
    let (s, b) = run(&ctx, "SELECT MIN(amt) AS m FROM t").await;
    assert_arb_meta(&s, "m", 30, 4, "MIN over a decimal_arb column");
    assert_eq!(arb_values_at(&b, "m", 4), vec![Some("-100".into())]);
}

#[tokio::test]
#[ignore = "FINDING D1: SUM/MIN/MAX/AVG over decimal_arb return an output Field with NO streamling.decimal_arb metadata (bare LargeBinary), so DecimalArbSortRewriteRule/DecimalArbExprPlanner cannot see the aggregate as decimal_arb."]
async fn max_aggregate_output_field_keeps_decimal_arb_metadata() {
    let ctx = session();
    let (s, _) = run(&ctx, "SELECT MAX(amt) AS m FROM t").await;
    assert_arb_meta(&s, "m", 30, 4, "MAX over a decimal_arb column");
}

#[tokio::test]
#[ignore = "FINDING D1: SUM/MIN/MAX/AVG over decimal_arb return an output Field with NO streamling.decimal_arb metadata (bare LargeBinary), so DecimalArbSortRewriteRule/DecimalArbExprPlanner cannot see the aggregate as decimal_arb."]
async fn sum_aggregate_output_field_keeps_decimal_arb_metadata() {
    let ctx = session();
    // E6 widening: SUM keeps the scale and adds 16 digits of precision.
    let (s, _) = run(&ctx, "SELECT SUM(amt) AS s FROM t").await;
    assert_arb_meta(&s, "s", 46, 4, "SUM over a decimal_arb column");
}

#[tokio::test]
#[ignore = "FINDING D1: SUM/MIN/MAX/AVG over decimal_arb return an output Field with NO streamling.decimal_arb metadata (bare LargeBinary), so DecimalArbSortRewriteRule/DecimalArbExprPlanner cannot see the aggregate as decimal_arb."]
async fn avg_aggregate_output_field_keeps_decimal_arb_metadata() {
    let ctx = session();
    let (s, _) = run(&ctx, "SELECT AVG(amt) AS a FROM t").await;
    assert_arb_meta(&s, "a", 31, 5, "AVG over a decimal_arb column");
}

#[tokio::test]
#[ignore = "FINDING D1: SUM/MIN/MAX/AVG over decimal_arb return an output Field with NO streamling.decimal_arb metadata (bare LargeBinary), so DecimalArbSortRewriteRule/DecimalArbExprPlanner cannot see the aggregate as decimal_arb."]
async fn arithmetic_over_a_decimal_arb_aggregate_plans_and_evaluates() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT MIN(amt) + 1 AS m FROM t").await;
    assert_eq!(
        arb_values_at(&b, "m", 4),
        vec![Some("-99".into())],
        "`MIN(decimal_arb) + 1` must dispatch through DecimalArbExprPlanner; it \
         currently fails to plan with `Cannot coerce arithmetic expression \
         LargeBinary + Int64`"
    );
}

#[tokio::test]
#[ignore = "FINDING D1: SUM/MIN/MAX/AVG over decimal_arb return an output Field with NO streamling.decimal_arb metadata (bare LargeBinary), so DecimalArbSortRewriteRule/DecimalArbExprPlanner cannot see the aggregate as decimal_arb."]
async fn comparison_against_a_decimal_arb_scalar_subquery_is_numeric_not_bytewise() {
    let ctx = session();
    // (SELECT MIN(amt) FROM u WHERE u.id > 10) = -1. Rows with amt > -1: 100, 0, 1000.
    // The subquery result is a bare LargeBinary, so DecimalArbExprPlanner leaves the
    // `>` alone and DataFusion compares the canonical bytes: -1 encodes 0xFF... which
    // is byte-greater than every non-negative encoding, so ZERO rows come back.
    assert_eq!(
        sorted_ids(
            &ctx,
            "SELECT id FROM t WHERE amt > (SELECT MIN(amt) FROM u WHERE u.id > 10)"
        )
        .await,
        vec![1, 3, 5],
        "comparing a decimal_arb column against a decimal_arb-valued scalar \
         subquery must be numeric; it currently degrades to a bytewise \
         LargeBinary comparison and silently returns the wrong rows"
    );
}

#[tokio::test]
async fn group_by_key_metadata_survives_even_though_aggregate_metadata_does_not() {
    // Control for D1: the GROUP BY *key* keeps its metadata; only the aggregate
    // output loses it. This isolates the defect to the UDAF return field.
    let ctx = session();
    let (s, _) = run(&ctx, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_arb_meta(&s, "amt", 30, 4, "GROUP BY key");
}

// =====================================================================
// K. Window functions over decimal_arb
// =====================================================================

#[tokio::test]
#[ignore = "FINDING E4 (generic, pre-dates decimal_arb): EnforceSorting never runs, so a window function's ORDER BY is not materialised - BoundedWindowAggExec consumes the input in arrival order and the OVER (ORDER BY ...) clause is silently ignored."]
async fn window_row_number_with_explicit_rows_frame_respects_the_order_by() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT id, CAST(ROW_NUMBER() OVER (ORDER BY amt ASC \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS BIGINT) AS rn \
         FROM t ORDER BY id ASC",
    )
    .await;
    assert_eq!(
        ints(&b, "rn"),
        vec![Some(4), Some(1), Some(3), Some(2), Some(5)],
        "an explicit ROWS frame avoids the RANGE-over-LargeBinary error, but the \
         window's ORDER BY is then ignored entirely and rows are numbered in \
         arrival order"
    );
}

#[tokio::test]
#[ignore = "FINDING E4 (generic, pre-dates decimal_arb): EnforceSorting never runs, so a window function's ORDER BY is not materialised - BoundedWindowAggExec consumes the input in arrival order and the OVER (ORDER BY ...) clause is silently ignored."]
async fn window_row_number_over_an_int_order_by_respects_the_order_by() {
    // Control for E4: the same defect with a plain Int64 ORDER BY, proving it is
    // not decimal_arb-specific.
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT id, CAST(ROW_NUMBER() OVER (ORDER BY grp ASC, id DESC \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS BIGINT) AS rn \
         FROM t ORDER BY id ASC",
    )
    .await;
    // grp a = ids 5,3,1 (id DESC) then grp b = ids 4,2  ->  rn by id:
    //   id1 -> 3, id2 -> 5, id3 -> 2, id4 -> 4, id5 -> 1
    assert_eq!(
        ints(&b, "rn"),
        vec![Some(3), Some(5), Some(2), Some(4), Some(1)],
        "a window ORDER BY over ordinary columns is ignored too - the defect is \
         in the physical-optimizer configuration, not in decimal_arb"
    );
}

// =====================================================================
// L. Controls proving the engine-level findings are type-agnostic
// =====================================================================

#[tokio::test]
#[ignore = "FINDING E2 (generic, pre-dates decimal_arb): with the default physical rules replaced, EnforceSorting never inserts CoalescePartitionsExec/SortPreservingMergeExec under a SortExec, so ORDER BY over a multi-partition input (any UNION) silently returns ONLY the first branch's rows."]
async fn int64_order_by_over_union_all_keeps_every_row() {
    let ctx = session();
    let ids = ordered_ids(
        &ctx,
        "SELECT id FROM (SELECT id FROM t UNION ALL SELECT id FROM u) ORDER BY id ASC",
    )
    .await;
    assert_eq!(
        ids,
        vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(10),
            Some(11),
            Some(12)
        ],
        "ORDER BY over a UNION ALL must return all 8 rows; it currently returns \
         only the 5 rows of the first branch. Plain Int64 columns - not \
         decimal_arb-specific"
    );
}

#[tokio::test]
#[ignore = "FINDING E2 (generic, pre-dates decimal_arb): with the default physical rules replaced, EnforceSorting never inserts CoalescePartitionsExec/SortPreservingMergeExec under a SortExec, so ORDER BY over a multi-partition input (any UNION) silently returns ONLY the first branch's rows."]
async fn count_star_over_union_all_returns_one_row() {
    let ctx = session();
    let (_, b) = run(
        &ctx,
        "SELECT COUNT(*) AS n FROM (SELECT id FROM t UNION ALL SELECT id FROM u)",
    )
    .await;
    assert_eq!(
        nrows(&b),
        1,
        "a global COUNT(*) over a UNION ALL must be a single row; the missing \
         EnforceDistribution leaves one partial row per union branch"
    );
}

#[tokio::test]
#[ignore = "FINDING E3 (generic, pre-dates decimal_arb): AggregateExec(FinalPartitioned) runs per input partition with no hash RepartitionExec, so UNION (DISTINCT) does not deduplicate across union branches (SELECT id FROM t UNION SELECT id FROM t returns 10 rows, not 5)."]
async fn int64_union_distinct_dedupes_across_branches() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT id FROM t UNION SELECT id FROM t").await;
    assert_eq!(
        nrows(&b),
        5,
        "UNION (DISTINCT) of a table with itself must collapse to 5 rows; it \
         returns 10. Plain Int64 - not decimal_arb-specific"
    );
}

#[tokio::test]
#[ignore = "FINDING E1 (generic, pre-dates decimal_arb): SessionManager REPLACES DataFusion's default physical optimizer rules with StreamlingPhysicalOptimizerRules::rules(), so EnforceDistribution never runs and every hash equi-join fails at execution with `Invalid HashJoinExec, unsupported PartitionMode Auto`. Blocks JOIN..USING / EXCEPT / INTERSECT / IN(subquery) over decimal_arb."]
async fn int64_hash_equijoin_executes() {
    let ctx = session();
    let (_, b) = run(&ctx, "SELECT t.id AS id FROM t JOIN u ON t.id = u.id - 9").await;
    assert_eq!(
        nrows(&b),
        3,
        "an Int64 hash equi-join must execute; it fails with `Invalid \
         HashJoinExec, unsupported PartitionMode Auto`. Not decimal_arb-specific"
    );
}

#[tokio::test]
async fn nested_loop_join_over_decimal_arb_still_executes() {
    // Control for E1: because `a.amt = b.amt` is rewritten to decimal_arb_eq
    // (a ScalarFunction, not an Eq BinaryExpr), the planner picks a nested-loop
    // join and the query DOES run — which is why only USING / set-ops break.
    let ctx = session();
    assert_eq!(
        sorted_ids(&ctx, "SELECT t.id AS id FROM t JOIN u ON t.amt = u.amt").await,
        vec![1, 4],
        "decimal_arb ON-equality joins route through a nested-loop join and work"
    );
}
