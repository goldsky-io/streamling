//! Adversarial coverage for `decimal_arb` **field-metadata survival across every
//! logical-plan node** — the F2 bug class, generalized.
//!
//! A `decimal_arb` column is a `LargeBinary` array plus two Arrow field-metadata
//! keys (`ARROW:extension:name` / `ARROW:extension:metadata`). Every sink in this
//! codebase decides "NUMERIC(p, s)" vs "raw BYTEA / hex string" purely from that
//! metadata. If any plan node drops it, the query still returns the right bytes,
//! nothing errors anywhere, and the data silently lands at the sink as a blob.
//! That is the highest-value silent failure in the decimal_arb work.
//!
//! So: for plain projection, aliasing, qualified refs, `SELECT *`, derived
//! tables, CTEs, UNION (ALL/DISTINCT), INTERSECT/EXCEPT, every JOIN flavour,
//! DISTINCT / DISTINCT ON, LIMIT / OFFSET, ORDER BY (which is rewritten by
//! `DecimalArbSortRewriteRule`), GROUP BY passthrough, aggregates, HAVING,
//! window functions, UNNEST, struct field access, repartitioning, and the
//! DataFrame API — this file asserts the output field STILL satisfies
//! `DecimalArbType::is_decimal_arb_field` with the right `(precision, scale)`,
//! and (where it is meaningful) that the decoded values are right too.
//!
//! Assertions deliberately check the *physical* output batch schema — the thing
//! a sink actually sees — falling back to the DataFrame's logical schema only
//! when a query legitimately produces zero batches.

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int64Array, LargeBinaryArray, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Partitioning, col};
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

fn arb_field(name: &str, p: u32, s: u32) -> Field {
    DecimalArbType::field(name, p, s, true).expect("decimal_arb field")
}

/// Build the raw `LargeBinary` storage array for a decimal_arb column.
fn arb_array(name: &str, p: u32, s: u32, vals: &[Option<&str>]) -> LargeBinaryArray {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), name, p, s)
        .expect("DecimalArbArrayBuilder::with_capacity");
    for v in vals {
        match v {
            Some(x) => b
                .append_value(&DecimalArbValue::from_str(x).expect("parse decimal"))
                .expect("append_value"),
            None => b.append_null(),
        }
    }
    b.finish().into_inner().0
}

/// Register `table(amt decimal_arb(p, s))`.
fn register_arb(sm: &SessionManager, table: &str, p: u32, s: u32, vals: &[Option<&str>]) {
    register_arb_named(sm, table, "amt", p, s, vals);
}

fn register_arb_named(
    sm: &SessionManager,
    table: &str,
    col_name: &str,
    p: u32,
    s: u32,
    vals: &[Option<&str>],
) {
    let schema = Arc::new(Schema::new(vec![arb_field(col_name, p, s)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arb_array(col_name, p, s, vals))],
    )
    .expect("RecordBatch::try_new");
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("MemTable")),
    )
    .expect("register_table");
}

/// Register `table(id Int64, grp Utf8, amt decimal_arb(p, s))`.
fn register_std(
    sm: &SessionManager,
    table: &str,
    p: u32,
    s: u32,
    rows: &[(i64, &str, Option<&str>)],
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Utf8, false),
        arb_field("amt", p, s),
    ]));
    let ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let grps: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let amts: Vec<Option<&str>> = rows.iter().map(|r| r.2).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(grps)),
            Arc::new(arb_array("amt", p, s, &amts)),
        ],
    )
    .expect("RecordBatch::try_new");
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("MemTable")),
    )
    .expect("register_table");
}

/// Register `table(id Int64, items List<decimal_arb(p, s)>)` with one list per row.
fn register_list_of_arb(
    sm: &SessionManager,
    table: &str,
    p: u32,
    s: u32,
    lists: &[Vec<Option<&str>>],
) {
    let item_field = Arc::new(arb_field("item", p, s));
    let mut flat: Vec<Option<&str>> = Vec::new();
    let mut offsets: Vec<i32> = vec![0];
    for l in lists {
        flat.extend(l.iter().copied());
        offsets.push(flat.len() as i32);
    }
    let values: ArrayRef = Arc::new(arb_array("item", p, s, &flat));
    let list = ListArray::new(
        item_field.clone(),
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("items", DataType::List(item_field), true),
    ]));
    let ids: Vec<i64> = (0..lists.len() as i64).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(list)],
    )
    .expect("RecordBatch::try_new");
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("MemTable")),
    )
    .expect("register_table");
}

/// Register `table(id Int64, st Struct{amt decimal_arb(p, s)})`.
fn register_struct_of_arb(sm: &SessionManager, table: &str, p: u32, s: u32, vals: &[Option<&str>]) {
    let inner = Arc::new(arb_field("amt", p, s));
    let fields: Fields = vec![inner.clone()].into();
    let st = StructArray::new(
        fields.clone(),
        vec![Arc::new(arb_array("amt", p, s, vals)) as ArrayRef],
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("st", DataType::Struct(fields), true),
    ]));
    let ids: Vec<i64> = (0..vals.len() as i64).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(st)],
    )
    .expect("RecordBatch::try_new");
    sm.register_table(
        table,
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("MemTable")),
    )
    .expect("register_table");
}

/// Result of running a query: the plan's logical schema plus the collected batches.
struct Out {
    logical: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl Out {
    /// The schema a sink actually sees. Falls back to the logical schema only
    /// when the query legitimately produced no batches at all.
    fn schema(&self) -> SchemaRef {
        match self.batches.first() {
            Some(b) => b.schema(),
            None => self.logical.clone(),
        }
    }

    fn field(&self, name: &str) -> Field {
        let schema = self.schema();
        schema
            .field_with_name(name)
            .unwrap_or_else(|_| {
                panic!(
                    "output has no column `{name}`; columns = {:?}",
                    schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                )
            })
            .clone()
    }

    fn field_at(&self, idx: usize) -> Field {
        self.schema().field(idx).clone()
    }

    fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Decode a decimal_arb column into canonical strings (row order preserved).
    fn decode(&self, name: &str, scale: u32) -> Vec<Option<String>> {
        let mut out = Vec::new();
        for b in &self.batches {
            let c = b
                .column_by_name(name)
                .unwrap_or_else(|| panic!("no column `{name}` in batch"));
            let lba = c
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap_or_else(|| {
                    panic!("column `{name}` is {:?}, not LargeBinary", c.data_type())
                });
            for i in 0..lba.len() {
                if lba.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(
                        DecimalArbValue::from_canonical_bytes_at_scale(lba.value(i), scale)
                            .expect("decode canonical bytes")
                            .to_canonical_string(),
                    ));
                }
            }
        }
        out
    }

    fn decode_sorted(&self, name: &str, scale: u32) -> Vec<Option<String>> {
        let mut v = self.decode(name, scale);
        v.sort();
        v
    }

    fn int_col(&self, name: &str) -> Vec<Option<i64>> {
        let mut out = Vec::new();
        for b in &self.batches {
            let c = b
                .column_by_name(name)
                .unwrap_or_else(|| panic!("no column `{name}` in batch"));
            let a = c
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap_or_else(|| panic!("column `{name}` is {:?}, not Int64", c.data_type()));
            for i in 0..a.len() {
                out.push(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
        }
        out
    }
}

async fn run(sm: &SessionManager, sql: &str) -> Out {
    let plan = sm
        .create_logical_plan(sql.to_string())
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{sql}`: {e}"));
    let df = sm.new_df(plan);
    let logical: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("execution failed for `{sql}`: {e}"));
    Out { logical, batches }
}

/// Plan a query through the session's `SessionState`, bypassing only the
/// bigint SQL *text* pre-pass in `SessionManager::create_logical_plan`
/// (`preprocess_bigint_sql` hard-rejects `JOIN` in `FROM` with "JOIN queries
/// not supported", so join shapes are unreachable through that entry point).
/// The session state is the same one `SessionManager` builds — decimal_arb
/// UDFs, `DecimalArbExprPlanner`, `DecimalArbExprRewrite`, and
/// `DecimalArbSortRewriteRule` all registered.
async fn plan_arrow_schema(sm: &SessionManager, sql: &str) -> Schema {
    let plan = sm
        .session_state()
        .create_logical_plan(sql)
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{sql}`: {e}"));
    plan.schema().as_arrow().clone()
}

/// The schema a sink is built from: `LogicalPlan::schema()` on the *unoptimized*
/// plan (see `find_plan_and_schema` / `pipeline_plans` in `crates/streamling`,
/// which feed `plan.schema().inner()` straight into every sink's TableProvider).
async fn plan_field(sm: &SessionManager, sql: &str, name: &str) -> Field {
    let schema = plan_arrow_schema(sm, sql).await;
    schema
        .field_with_name(name)
        .unwrap_or_else(|_| {
            panic!(
                "plan schema for `{sql}` has no column `{name}`; columns = {:?}",
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            )
        })
        .clone()
}

/// The core assertion of this file.
fn assert_arb(field: &Field, p: u32, s: u32, what: &str) {
    assert!(
        DecimalArbType::is_decimal_arb_field(field),
        "{what}: output field `{}` LOST decimal_arb metadata \
         (a sink would emit this as raw BYTEA/hex). data_type={:?} metadata={:?}",
        field.name(),
        field.data_type(),
        field.metadata(),
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(field),
        Some((p, s)),
        "{what}: output field `{}` carries the wrong (precision, scale)",
        field.name(),
    );
}

/// Convenience: run a single-`amt`-column query and assert the metadata survived.
async fn assert_amt_arb(sm: &SessionManager, sql: &str, p: u32, s: u32) -> Out {
    let out = run(sm, sql).await;
    assert_arb(&out.field("amt"), p, s, sql);
    out
}

const V: &[Option<&str>] = &[Some("5"), Some("-3"), Some("0"), Some("200"), None];

fn std_rows() -> Vec<(i64, &'static str, Option<&'static str>)> {
    vec![
        (1, "a", Some("5")),
        (2, "a", Some("-3")),
        (3, "b", Some("0")),
        (4, "b", Some("200")),
        (5, "c", None),
    ]
}

// =====================================================================
// A. Plain projection, aliasing, SELECT *
// =====================================================================

#[tokio::test]
async fn plain_projection_preserves_decimal_arb_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_amt_arb(&sm, "SELECT amt FROM t", 100, 0).await;
}

#[tokio::test]
async fn plain_projection_preserves_values() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = assert_amt_arb(&sm, "SELECT amt FROM t", 100, 0).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![
            Some("5".into()),
            Some("-3".into()),
            Some("0".into()),
            Some("200".into()),
            None
        ],
        "projection must not alter decimal_arb values"
    );
}

#[tokio::test]
async fn select_star_preserves_decimal_arb_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT * FROM t").await;
    assert_arb(&out.field("amt"), 100, 0, "SELECT *");
}

#[tokio::test]
async fn select_star_keeps_non_arb_columns_untouched() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT * FROM t").await;
    assert_eq!(out.field("id").data_type(), &DataType::Int64);
    assert!(
        !DecimalArbType::is_decimal_arb_field(&out.field("id")),
        "plain Int64 column must not acquire decimal_arb metadata"
    );
}

#[tokio::test]
async fn qualified_star_through_table_alias_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT x.* FROM t AS x").await;
    assert_arb(&out.field("amt"), 100, 0, "SELECT x.*");
}

#[tokio::test]
async fn aliased_column_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = run(&sm, "SELECT amt AS renamed FROM t").await;
    assert_arb(&out.field("renamed"), 100, 0, "SELECT amt AS renamed");
}

#[tokio::test]
async fn alias_chain_through_two_levels_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = run(
        &sm,
        "SELECT b AS c FROM (SELECT a AS b FROM (SELECT amt AS a FROM t))",
    )
    .await;
    assert_arb(&out.field("c"), 100, 0, "three-level alias chain");
}

#[tokio::test]
async fn qualified_column_reference_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    assert_amt_arb(&sm, "SELECT t.amt FROM t", 100, 0).await;
}

#[tokio::test]
async fn table_alias_qualified_column_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT x.amt AS amt FROM t AS x").await;
    assert_arb(&out.field("amt"), 100, 0, "aliased-table qualified ref");
}

#[tokio::test]
async fn duplicated_projection_of_same_column_preserves_both() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = run(&sm, "SELECT amt AS one, amt AS two FROM t").await;
    assert_arb(&out.field("one"), 100, 0, "first copy");
    assert_arb(&out.field("two"), 100, 0, "second copy");
}

#[tokio::test]
async fn projection_reorder_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT amt, grp, id FROM t").await;
    assert_arb(&out.field_at(0), 100, 0, "reordered projection position 0");
}

#[tokio::test]
async fn projection_mixed_with_literals_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = run(&sm, "SELECT 1 AS lit, amt, 'x' AS s FROM t").await;
    assert_arb(&out.field("amt"), 100, 0, "projection with literals");
}

#[tokio::test]
async fn projection_of_two_arb_columns_keeps_each_own_metadata() {
    let sm = session();
    let schema = Arc::new(Schema::new(vec![
        arb_field("a", 100, 0),
        arb_field("b", 40, 18),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arb_array("a", 100, 0, &[Some("7")])),
            Arc::new(arb_array("b", 40, 18, &[Some("1.5")])),
        ],
    )
    .unwrap();
    sm.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    let out = run(&sm, "SELECT a, b FROM t").await;
    assert_arb(&out.field("a"), 100, 0, "column a");
    assert_arb(&out.field("b"), 40, 18, "column b");
}

// =====================================================================
// B. Precision/scale variations
// =====================================================================

#[tokio::test]
async fn scale_18_column_keeps_scale_18_through_projection() {
    let sm = session();
    register_arb(
        &sm,
        "t",
        100,
        18,
        &[Some("1.5"), Some("-0.000000000000000001")],
    );
    let out = assert_amt_arb(&sm, "SELECT amt FROM t", 100, 18).await;
    assert_eq!(
        out.decode("amt", 18),
        vec![
            Some("1.500000000000000000".into()),
            Some("-0.000000000000000001".into())
        ],
        "scale-18 values must decode at scale 18"
    );
}

#[tokio::test]
async fn precision_one_scale_zero_survives_projection() {
    let sm = session();
    register_arb(&sm, "t", 1, 0, &[Some("7"), Some("-9")]);
    assert_amt_arb(&sm, "SELECT amt FROM t", 1, 0).await;
}

#[tokio::test]
async fn precision_equal_to_scale_survives_projection() {
    let sm = session();
    register_arb(&sm, "t", 5, 5, &[Some("0.12345"), Some("-0.00001")]);
    assert_amt_arb(&sm, "SELECT amt FROM t", 5, 5).await;
}

#[tokio::test]
async fn max_precision_column_survives_projection() {
    let sm = session();
    register_arb(&sm, "t", 65_535, 0, &[Some("1"), Some("-1")]);
    assert_amt_arb(&sm, "SELECT amt FROM t", 65_535, 0).await;
}

#[tokio::test]
async fn all_null_arb_column_still_carries_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 6, &[None, None, None]);
    let out = assert_amt_arb(&sm, "SELECT amt FROM t", 100, 6).await;
    assert_eq!(out.decode("amt", 6), vec![None, None, None]);
}

#[tokio::test]
async fn empty_table_projection_still_carries_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 3, &[]);
    let out = run(&sm, "SELECT amt FROM t").await;
    assert_arb(&out.field("amt"), 100, 3, "empty table scan");
}

// =====================================================================
// C. Derived tables, subqueries, CTEs
// =====================================================================

#[tokio::test]
async fn derived_table_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_amt_arb(&sm, "SELECT amt FROM (SELECT amt FROM t)", 100, 0).await;
}

#[tokio::test]
async fn derived_table_with_alias_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_amt_arb(&sm, "SELECT s.amt FROM (SELECT * FROM t) AS s", 100, 0).await;
}

#[tokio::test]
async fn three_level_nested_subquery_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let sql = "SELECT amt FROM (SELECT amt FROM (SELECT amt FROM (SELECT amt FROM t) a) b) c";
    assert_amt_arb(&sm, sql, 100, 0).await;
}

#[tokio::test]
async fn derived_table_with_filter_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = assert_amt_arb(
        &sm,
        "SELECT amt FROM (SELECT amt, id FROM t WHERE id < 3) s",
        100,
        0,
    )
    .await;
    assert_eq!(out.num_rows(), 2, "filter inside derived table must apply");
}

#[tokio::test]
async fn derived_table_with_order_by_inside_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_amt_arb(
        &sm,
        "SELECT amt FROM (SELECT amt FROM t ORDER BY amt) s",
        100,
        0,
    )
    .await;
}

#[tokio::test]
async fn cte_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_amt_arb(
        &sm,
        "WITH c AS (SELECT amt FROM t) SELECT amt FROM c",
        100,
        0,
    )
    .await;
}

#[tokio::test]
async fn cte_with_column_rename_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = run(
        &sm,
        "WITH c AS (SELECT amt AS v FROM t) SELECT v AS w FROM c",
    )
    .await;
    assert_arb(&out.field("w"), 100, 0, "CTE with rename");
}

#[tokio::test]
async fn nested_ctes_preserve_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let sql = "WITH a AS (SELECT amt FROM t), b AS (SELECT amt FROM a) SELECT amt FROM b";
    assert_amt_arb(&sm, sql, 100, 0).await;
}

#[tokio::test]
async fn cte_referenced_twice_in_union_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let sql = "WITH c AS (SELECT amt FROM t) \
               SELECT amt FROM c UNION ALL SELECT amt FROM c";
    let out = assert_amt_arb(&sm, sql, 100, 0).await;
    assert_eq!(
        out.num_rows(),
        10,
        "UNION ALL of a CTE with itself doubles rows"
    );
}

#[tokio::test]
async fn cte_over_cte_over_derived_table_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 4, &[Some("1.0001")]);
    let sql = "WITH a AS (SELECT amt FROM (SELECT amt FROM t) x), \
                    b AS (SELECT amt FROM a) \
               SELECT amt FROM (SELECT amt FROM b) y";
    assert_amt_arb(&sm, sql, 100, 4).await;
}

#[tokio::test]
#[ignore = "FINDING: a scalar subquery whose SELECT list is a decimal_arb \
            column produces a bare LargeBinary output field — Expr::ScalarSubquery \
            derives its field from the subquery's DataType only, so the \
            (precision, scale) is lost"]
async fn scalar_subquery_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "SELECT (SELECT amt FROM t WHERE id = 1) AS amt FROM t WHERE id = 2";
    let out = run(&sm, sql).await;
    assert_arb(&out.field("amt"), 100, 0, "scalar subquery output");
}

/// Silent half of the scalar-subquery finding: correct bytes, no metadata.
#[tokio::test]
async fn scalar_subquery_over_decimal_arb_returns_bare_large_binary() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "SELECT (SELECT amt FROM t WHERE id = 1) AS amt FROM t WHERE id = 2";
    let out = run(&sm, sql).await;
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out.field("amt")),
        None,
        "if this is now Some(..) the scalar-subquery finding is fixed"
    );
    assert_eq!(out.decode("amt", 0), vec![Some("5".into())]);
}

// =====================================================================
// D. Set operations
// =====================================================================

fn two_arb_tables(sm: &SessionManager, p: u32, s: u32) {
    register_arb(sm, "t1", p, s, &[Some("5"), Some("-3")]);
    register_arb(sm, "t2", p, s, &[Some("7"), Some("0")]);
}

#[tokio::test]
async fn union_all_preserves_metadata() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let out = assert_amt_arb(
        &sm,
        "SELECT amt FROM t1 UNION ALL SELECT amt FROM t2",
        100,
        0,
    )
    .await;
    assert_eq!(out.num_rows(), 4);
}

#[tokio::test]
async fn union_all_preserves_values() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let out = run(&sm, "SELECT amt FROM t1 UNION ALL SELECT amt FROM t2").await;
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![
            Some("-3".into()),
            Some("0".into()),
            Some("5".into()),
            Some("7".into())
        ]
    );
}

#[tokio::test]
async fn union_distinct_preserves_metadata() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    assert_amt_arb(&sm, "SELECT amt FROM t1 UNION SELECT amt FROM t2", 100, 0).await;
}

#[tokio::test]
async fn three_way_union_all_preserves_metadata() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    register_arb(&sm, "t3", 100, 0, &[Some("9")]);
    let sql = "SELECT amt FROM t1 UNION ALL SELECT amt FROM t2 UNION ALL SELECT amt FROM t3";
    let out = assert_amt_arb(&sm, sql, 100, 0).await;
    assert_eq!(out.num_rows(), 5);
}

#[tokio::test]
async fn union_all_of_a_table_with_itself_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 9, &[Some("1.5")]);
    assert_amt_arb(&sm, "SELECT amt FROM t UNION ALL SELECT amt FROM t", 100, 9).await;
}

#[tokio::test]
async fn union_all_with_differing_precision_same_scale_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t1", 100, 0, &[Some("5")]);
    register_arb(&sm, "t2", 40, 0, &[Some("7")]);
    let out = run(&sm, "SELECT amt FROM t1 UNION ALL SELECT amt FROM t2").await;
    assert!(
        DecimalArbType::is_decimal_arb_field(&out.field("amt")),
        "UNION ALL of two decimal_arb columns that agree on scale must stay \
         decimal_arb; got {:?}",
        out.field("amt")
    );
    // Whichever precision wins, the scale must be the shared one — decoding at a
    // different scale silently changes every value.
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out.field("amt")).map(|(_, s)| s),
        Some(0),
        "UNION ALL must keep the shared scale"
    );
}

#[tokio::test]
async fn union_all_with_null_only_branch_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t1", 100, 0, &[Some("5")]);
    register_arb(&sm, "t2", 100, 0, &[None]);
    let out = assert_amt_arb(
        &sm,
        "SELECT amt FROM t1 UNION ALL SELECT amt FROM t2",
        100,
        0,
    )
    .await;
    assert_eq!(out.decode_sorted("amt", 0), vec![None, Some("5".into())]);
}

// INTERSECT / EXCEPT lower to a HashJoinExec, which cannot execute in this
// session (see `hash_join_plans_can_execute`), so assert on the sink-facing
// plan schema.
#[tokio::test]
async fn intersect_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t1", 100, 0, &[Some("5"), Some("-3")]);
    register_arb(&sm, "t2", 100, 0, &[Some("5"), Some("7")]);
    let sql = "SELECT amt FROM t1 INTERSECT SELECT amt FROM t2";
    assert_arb(&plan_field(&sm, sql, "amt").await, 100, 0, sql);
}

#[tokio::test]
async fn except_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t1", 100, 0, &[Some("5"), Some("-3")]);
    register_arb(&sm, "t2", 100, 0, &[Some("5")]);
    let sql = "SELECT amt FROM t1 EXCEPT SELECT amt FROM t2";
    assert_arb(&plan_field(&sm, sql, "amt").await, 100, 0, sql);
}

#[tokio::test]
async fn union_all_inside_derived_table_preserves_metadata() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let sql = "SELECT amt FROM (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u";
    assert_amt_arb(&sm, sql, 100, 0).await;
}

#[tokio::test]
async fn union_all_dataframe_api_preserves_metadata() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let p1 = sm
        .create_logical_plan("SELECT amt FROM t1".into())
        .await
        .unwrap();
    let p2 = sm
        .create_logical_plan("SELECT amt FROM t2".into())
        .await
        .unwrap();
    let df = sm.new_df(p1).union(sm.new_df(p2)).unwrap();
    let batches = df.collect().await.unwrap();
    let field = batches[0].schema().field_with_name("amt").unwrap().clone();
    assert_arb(&field, 100, 0, "DataFrame::union");
}

// =====================================================================
// E. Joins
//
// `SessionManager::create_logical_plan` runs `preprocess_bigint_sql`, which
// hard-rejects any `JOIN` in `FROM`, and this session's physical rule set
// (`StreamlingPhysicalOptimizerRules::rules()` REPLACES DataFusion's defaults,
// so `EnforceDistribution` never runs) cannot execute a `HashJoinExec`. Join
// coverage therefore asserts on the *logical plan schema* — which is exactly
// what the product feeds to sinks (`find_plan_and_schema` in
// `crates/streamling/src/lib.rs` passes `plan.schema().inner()` straight into
// every sink's TableProvider), so it is the schema that decides BYTEA vs
// NUMERIC at the sink.
// =====================================================================

fn join_tables(sm: &SessionManager) {
    register_std(
        sm,
        "l",
        100,
        0,
        &[(1, "a", Some("5")), (2, "b", Some("-3"))],
    );
    register_std(
        sm,
        "r",
        60,
        2,
        &[(1, "a", Some("1.25")), (3, "c", Some("9.75"))],
    );
}

#[tokio::test]
async fn inner_join_preserves_left_side_metadata() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT l.amt AS lamt FROM l JOIN r ON l.id = r.id";
    assert_arb(&plan_field(&sm, sql, "lamt").await, 100, 0, sql);
}

#[tokio::test]
async fn inner_join_preserves_right_side_metadata() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT r.amt AS ramt FROM l JOIN r ON l.id = r.id";
    assert_arb(&plan_field(&sm, sql, "ramt").await, 60, 2, sql);
}

#[tokio::test]
async fn inner_join_preserves_both_sides_metadata() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT l.amt AS lamt, r.amt AS ramt FROM l JOIN r ON l.id = r.id";
    assert_arb(
        &plan_field(&sm, sql, "lamt").await,
        100,
        0,
        "both-sides left",
    );
    assert_arb(
        &plan_field(&sm, sql, "ramt").await,
        60,
        2,
        "both-sides right",
    );
}

#[tokio::test]
async fn inner_join_star_preserves_metadata_on_both_amt_columns() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT * FROM l JOIN r ON l.id = r.id";
    let schema = plan_arrow_schema(&sm, sql).await;
    let arb: Vec<&Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref())
        .filter(|f| f.name() == "amt")
        .collect();
    assert_eq!(arb.len(), 2, "join star should expose both amt columns");
    for f in arb {
        assert!(
            DecimalArbType::is_decimal_arb_field(f),
            "join `SELECT *` dropped decimal_arb metadata from an amt column: {f:?}"
        );
    }
}

#[tokio::test]
async fn left_join_preserves_metadata_on_both_sides() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT l.amt AS lamt, r.amt AS ramt FROM l LEFT JOIN r ON l.id = r.id";
    assert_arb(
        &plan_field(&sm, sql, "lamt").await,
        100,
        0,
        "LEFT JOIN preserved side",
    );
    assert_arb(
        &plan_field(&sm, sql, "ramt").await,
        60,
        2,
        "LEFT JOIN nullable side",
    );
}

#[tokio::test]
async fn left_join_makes_null_extended_side_nullable_without_losing_metadata() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT r.amt AS ramt FROM l LEFT JOIN r ON l.id = r.id";
    let f = plan_field(&sm, sql, "ramt").await;
    assert!(
        f.is_nullable(),
        "LEFT JOIN must make the right side nullable"
    );
    assert_arb(&f, 60, 2, "LEFT JOIN nullability rewrite");
}

#[tokio::test]
async fn right_join_preserves_metadata_on_both_sides() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT l.amt AS lamt, r.amt AS ramt FROM l RIGHT JOIN r ON l.id = r.id";
    assert_arb(
        &plan_field(&sm, sql, "lamt").await,
        100,
        0,
        "RIGHT JOIN nullable side",
    );
    assert_arb(
        &plan_field(&sm, sql, "ramt").await,
        60,
        2,
        "RIGHT JOIN preserved side",
    );
}

#[tokio::test]
async fn full_outer_join_preserves_metadata_on_both_sides() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT l.amt AS lamt, r.amt AS ramt FROM l FULL OUTER JOIN r ON l.id = r.id";
    assert_arb(
        &plan_field(&sm, sql, "lamt").await,
        100,
        0,
        "FULL OUTER left",
    );
    assert_arb(
        &plan_field(&sm, sql, "ramt").await,
        60,
        2,
        "FULL OUTER right",
    );
}

#[tokio::test]
async fn cross_join_preserves_metadata_on_both_sides() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT l.amt AS lamt, r.amt AS ramt FROM l CROSS JOIN r";
    assert_arb(
        &plan_field(&sm, sql, "lamt").await,
        100,
        0,
        "CROSS JOIN left",
    );
    assert_arb(
        &plan_field(&sm, sql, "ramt").await,
        60,
        2,
        "CROSS JOIN right",
    );
}

#[tokio::test]
async fn implicit_comma_join_preserves_metadata() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT l.amt AS lamt, r.amt AS ramt FROM l, r WHERE l.id = r.id";
    assert_arb(
        &plan_field(&sm, sql, "lamt").await,
        100,
        0,
        "comma join left",
    );
    assert_arb(
        &plan_field(&sm, sql, "ramt").await,
        60,
        2,
        "comma join right",
    );
}

#[tokio::test]
async fn join_using_clause_preserves_metadata() {
    let sm = session();
    register_std(&sm, "l", 100, 0, &[(1, "a", Some("5"))]);
    register_std(&sm, "r", 100, 0, &[(1, "a", Some("6"))]);
    let sql = "SELECT l.amt AS lamt FROM l JOIN r USING (id)";
    assert_arb(&plan_field(&sm, sql, "lamt").await, 100, 0, "JOIN USING");
}

#[tokio::test]
async fn self_join_preserves_metadata_on_both_aliases() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "SELECT a.amt AS x, b.amt AS y FROM t a JOIN t b ON a.id = b.id";
    assert_arb(&plan_field(&sm, sql, "x").await, 100, 0, "self-join a");
    assert_arb(&plan_field(&sm, sql, "y").await, 100, 0, "self-join b");
}

#[tokio::test]
async fn three_table_join_preserves_metadata_everywhere() {
    let sm = session();
    register_std(&sm, "a", 100, 0, &[(1, "x", Some("1"))]);
    register_std(&sm, "b", 50, 1, &[(1, "x", Some("2.5"))]);
    register_std(&sm, "c", 30, 3, &[(1, "x", Some("0.125"))]);
    let sql = "SELECT a.amt AS aa, b.amt AS bb, c.amt AS cc \
               FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id";
    assert_arb(&plan_field(&sm, sql, "aa").await, 100, 0, "3-join a");
    assert_arb(&plan_field(&sm, sql, "bb").await, 50, 1, "3-join b");
    assert_arb(&plan_field(&sm, sql, "cc").await, 30, 3, "3-join c");
}

#[tokio::test]
async fn join_on_decimal_arb_equality_key_preserves_metadata() {
    let sm = session();
    register_arb_named(&sm, "l", "amt", 100, 0, &[Some("5"), Some("-3")]);
    register_arb_named(&sm, "r", "amt", 100, 0, &[Some("5"), Some("9")]);
    let sql = "SELECT l.amt AS lamt, r.amt AS ramt FROM l JOIN r ON l.amt = r.amt";
    assert_arb(
        &plan_field(&sm, sql, "lamt").await,
        100,
        0,
        "decimal_arb join key left",
    );
    assert_arb(
        &plan_field(&sm, sql, "ramt").await,
        100,
        0,
        "decimal_arb join key right",
    );
}

#[tokio::test]
async fn join_inside_cte_preserves_metadata() {
    let sm = session();
    join_tables(&sm);
    let sql = "WITH j AS (SELECT l.amt AS lamt, r.amt AS ramt FROM l JOIN r ON l.id = r.id) \
               SELECT lamt, ramt FROM j";
    assert_arb(
        &plan_field(&sm, sql, "lamt").await,
        100,
        0,
        "join-in-CTE left",
    );
    assert_arb(
        &plan_field(&sm, sql, "ramt").await,
        60,
        2,
        "join-in-CTE right",
    );
}

#[tokio::test]
async fn cte_referenced_twice_in_join_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "WITH c AS (SELECT id, amt FROM t) \
               SELECT l.amt AS lamt, r.amt AS ramt FROM c l JOIN c r ON l.id = r.id";
    assert_arb(
        &plan_field(&sm, sql, "lamt").await,
        100,
        0,
        "CTE self-join left",
    );
    assert_arb(
        &plan_field(&sm, sql, "ramt").await,
        100,
        0,
        "CTE self-join right",
    );
}

#[tokio::test]
async fn semi_join_via_exists_preserves_metadata() {
    let sm = session();
    register_std(
        &sm,
        "l",
        100,
        0,
        &[(1, "a", Some("5")), (2, "b", Some("-3"))],
    );
    register_std(&sm, "r", 100, 0, &[(1, "a", Some("1"))]);
    let sql = "SELECT amt FROM l WHERE EXISTS (SELECT 1 FROM r WHERE r.id = l.id)";
    assert_arb(&plan_field(&sm, sql, "amt").await, 100, 0, sql);
}

#[tokio::test]
async fn anti_join_via_not_exists_preserves_metadata() {
    let sm = session();
    register_std(
        &sm,
        "l",
        100,
        0,
        &[(1, "a", Some("5")), (2, "b", Some("-3"))],
    );
    register_std(&sm, "r", 100, 0, &[(1, "a", Some("1"))]);
    let sql = "SELECT amt FROM l WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.id = l.id)";
    assert_arb(&plan_field(&sm, sql, "amt").await, 100, 0, sql);
}

#[tokio::test]
async fn in_subquery_semi_join_preserves_metadata() {
    let sm = session();
    register_std(
        &sm,
        "l",
        100,
        0,
        &[(1, "a", Some("5")), (2, "b", Some("-3"))],
    );
    register_std(&sm, "r", 100, 0, &[(1, "a", Some("1"))]);
    let sql = "SELECT amt FROM l WHERE id IN (SELECT id FROM r)";
    assert_arb(&plan_field(&sm, sql, "amt").await, 100, 0, sql);
}

#[tokio::test]
async fn decimal_arb_in_subquery_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_arb_named(&sm, "l", "amt", 100, 0, &[Some("5"), Some("-3")]);
    register_arb_named(&sm, "r", "amt", 100, 0, &[Some("5")]);
    let sql = "SELECT amt FROM l WHERE amt IN (SELECT amt FROM r)";
    assert_arb(&plan_field(&sm, sql, "amt").await, 100, 0, sql);
}

#[tokio::test]
async fn correlated_scalar_subquery_side_does_not_corrupt_outer_metadata() {
    let sm = session();
    register_std(&sm, "l", 100, 0, &[(1, "a", Some("5"))]);
    register_std(&sm, "r", 60, 2, &[(1, "a", Some("1.25"))]);
    let sql = "SELECT l.amt AS amt, (SELECT COUNT(*) FROM r WHERE r.id = l.id) AS n FROM l";
    assert_arb(&plan_field(&sm, sql, "amt").await, 100, 0, sql);
}

/// Loud (not silent) defect, recorded for completeness: any plan that lowers to
/// a `HashJoinExec` cannot execute in a `SessionManager` session.
#[tokio::test]
#[ignore = "FINDING: hash-join plans (INTERSECT/EXCEPT/IN-subquery/EXISTS) plan \
            fine but fail at execute() with 'Invalid HashJoinExec, unsupported \
            PartitionMode Auto' — StreamlingPhysicalOptimizerRules::rules() \
            REPLACES DataFusion's default physical rules, so EnforceDistribution \
            (which resolves PartitionMode::Auto) never runs"]
async fn hash_join_plans_can_execute() {
    let sm = session();
    register_arb(&sm, "t1", 100, 0, &[Some("5"), Some("-3")]);
    register_arb(&sm, "t2", 100, 0, &[Some("5")]);
    let out = run(&sm, "SELECT amt FROM t1 INTERSECT SELECT amt FROM t2").await;
    assert_eq!(out.decode("amt", 0), vec![Some("5".into())]);
}

// =====================================================================
// F. DISTINCT / LIMIT / OFFSET / ORDER BY
// =====================================================================

#[tokio::test]
async fn distinct_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("5"), Some("-3")]);
    let out = assert_amt_arb(&sm, "SELECT DISTINCT amt FROM t", 100, 0).await;
    assert_eq!(out.num_rows(), 2);
}

#[tokio::test]
async fn distinct_on_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT DISTINCT ON (grp) grp, amt FROM t ORDER BY grp").await;
    assert_arb(&out.field("amt"), 100, 0, "DISTINCT ON");
}

#[tokio::test]
async fn distinct_over_multiple_arb_columns_preserves_each() {
    let sm = session();
    let schema = Arc::new(Schema::new(vec![
        arb_field("a", 100, 0),
        arb_field("b", 20, 4),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arb_array("a", 100, 0, &[Some("1"), Some("1")])),
            Arc::new(arb_array("b", 20, 4, &[Some("2.5"), Some("2.5")])),
        ],
    )
    .unwrap();
    sm.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    let out = run(&sm, "SELECT DISTINCT a, b FROM t").await;
    assert_arb(&out.field("a"), 100, 0, "DISTINCT multi a");
    assert_arb(&out.field("b"), 20, 4, "DISTINCT multi b");
    assert_eq!(out.num_rows(), 1);
}

#[tokio::test]
async fn limit_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = assert_amt_arb(&sm, "SELECT amt FROM t LIMIT 2", 100, 0).await;
    assert_eq!(out.num_rows(), 2);
}

#[tokio::test]
async fn offset_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = assert_amt_arb(&sm, "SELECT amt FROM t OFFSET 3", 100, 0).await;
    assert_eq!(out.num_rows(), 2);
}

#[tokio::test]
async fn limit_with_offset_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = assert_amt_arb(&sm, "SELECT amt FROM t LIMIT 2 OFFSET 1", 100, 0).await;
    assert_eq!(out.num_rows(), 2);
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("-3".into()), Some("0".into())]
    );
}

#[tokio::test]
async fn limit_zero_still_reports_decimal_arb_schema() {
    let sm = session();
    register_arb(&sm, "t", 100, 7, V);
    let out = run(&sm, "SELECT amt FROM t LIMIT 0").await;
    assert_arb(&out.field("amt"), 100, 7, "LIMIT 0 output schema");
    assert_eq!(out.num_rows(), 0);
}

#[tokio::test]
async fn order_by_decimal_arb_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_amt_arb(&sm, "SELECT amt FROM t ORDER BY amt", 100, 0).await;
}

#[tokio::test]
async fn order_by_decimal_arb_desc_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_amt_arb(&sm, "SELECT amt FROM t ORDER BY amt DESC", 100, 0).await;
}

#[tokio::test]
async fn order_by_decimal_arb_does_not_leak_sort_key_column() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = run(&sm, "SELECT amt FROM t ORDER BY amt").await;
    assert_eq!(
        out.column_names(),
        vec!["amt".to_string()],
        "DecimalArbSortRewriteRule must not leak a decimal_arb_to_sort_key column \
         into the output schema"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_orders_numerically_across_signs() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = run(&sm, "SELECT amt FROM t ORDER BY amt ASC NULLS LAST").await;
    assert_eq!(
        out.decode("amt", 0),
        vec![
            Some("-3".into()),
            Some("0".into()),
            Some("5".into()),
            Some("200".into()),
            None
        ],
        "ORDER BY over decimal_arb must be numeric, not bytewise"
    );
}

#[tokio::test]
async fn order_by_decimal_arb_with_limit_topk_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = assert_amt_arb(&sm, "SELECT amt FROM t ORDER BY amt LIMIT 2", 100, 0).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("-3".into()), Some("0".into())],
        "TopK path must still order numerically"
    );
}

#[tokio::test]
async fn order_by_ordinal_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_amt_arb(&sm, "SELECT amt FROM t ORDER BY 1", 100, 0).await;
}

#[tokio::test]
async fn order_by_other_column_preserves_arb_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    assert_amt_arb(&sm, "SELECT amt, id FROM t ORDER BY id DESC", 100, 0).await;
}

#[tokio::test]
async fn order_by_alias_of_decimal_arb_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = run(&sm, "SELECT amt AS v FROM t ORDER BY v").await;
    assert_arb(&out.field("v"), 100, 0, "ORDER BY alias");
}

#[tokio::test]
async fn order_by_nulls_first_preserves_metadata_and_placement() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = assert_amt_arb(
        &sm,
        "SELECT amt FROM t ORDER BY amt ASC NULLS FIRST",
        100,
        0,
    )
    .await;
    assert_eq!(out.decode("amt", 0)[0], None, "NULLS FIRST must come first");
}

#[tokio::test]
async fn order_by_inside_cte_then_outer_projection_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let sql = "WITH c AS (SELECT amt FROM t ORDER BY amt) SELECT amt FROM c";
    assert_amt_arb(&sm, sql, 100, 0).await;
}

#[tokio::test]
async fn distinct_then_order_by_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("5"), Some("-3")]);
    let out = assert_amt_arb(&sm, "SELECT DISTINCT amt FROM t ORDER BY amt", 100, 0).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("-3".into()), Some("5".into())]
    );
}

// =====================================================================
// G. GROUP BY / aggregates / HAVING / window
// =====================================================================

#[tokio::test]
async fn group_by_decimal_arb_passthrough_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("5"), Some("-3")]);
    let out = run(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_arb(&out.field("amt"), 100, 0, "GROUP BY key passthrough");
}

#[tokio::test]
async fn group_by_decimal_arb_scale_18_passthrough_preserves_scale() {
    let sm = session();
    register_arb(&sm, "t", 100, 18, &[Some("1.5"), Some("1.5")]);
    let out = run(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt").await;
    assert_arb(&out.field("amt"), 100, 18, "GROUP BY key scale 18");
    assert_eq!(
        out.decode("amt", 18),
        vec![Some("1.500000000000000000".into())]
    );
}

#[tokio::test]
async fn group_by_multiple_arb_keys_preserves_each() {
    let sm = session();
    let schema = Arc::new(Schema::new(vec![
        arb_field("a", 100, 0),
        arb_field("b", 20, 4),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arb_array("a", 100, 0, &[Some("1"), Some("1")])),
            Arc::new(arb_array("b", 20, 4, &[Some("2.5"), Some("2.5")])),
        ],
    )
    .unwrap();
    sm.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    let out = run(&sm, "SELECT a, b, COUNT(*) AS n FROM t GROUP BY a, b").await;
    assert_arb(&out.field("a"), 100, 0, "multi-key GROUP BY a");
    assert_arb(&out.field("b"), 20, 4, "multi-key GROUP BY b");
}

#[tokio::test]
async fn group_by_then_outer_projection_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("5")]);
    let sql = "SELECT amt FROM (SELECT amt, COUNT(*) AS n FROM t GROUP BY amt) g";
    assert_amt_arb(&sm, sql, 100, 0).await;
}

// ---------------------------------------------------------------------
// FINDING (aggregates): none of `DecimalArbSumUdaf` / `DecimalArbExtremeUdaf`
// / `DecimalArbAvgUdaf` override `AggregateUDFImpl::return_field` — they only
// override `return_type`, so DataFusion's default `return_field` builds a bare
// `Field::new(name, LargeBinary, nullable)` with an EMPTY metadata map. The
// accumulators still emit correct canonical decimal_arb bytes, so nothing
// errors: the column simply arrives at the sink as BYTEA/hex. This is the
// exact F2 shape, on the aggregate path.
//
// AVG is the worst case: `avg_output_precision_scale` widens the scale by one,
// so the emitted bytes are at scale s+1 while the (absent) metadata gives a
// consumer no way to learn that — decoding at the input scale is off by 10x.
// ---------------------------------------------------------------------

/// Values are correct — only the metadata is gone. This passing test pins the
/// "silent" half of the aggregate finding.
#[tokio::test]
async fn min_over_decimal_arb_returns_correct_value_but_bare_large_binary() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT MIN(amt) AS amt FROM t").await;
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("-3".into())],
        "MIN(decimal_arb) must compute numerically"
    );
    assert_eq!(
        out.field("amt").data_type(),
        &DataType::LargeBinary,
        "storage type is still LargeBinary"
    );
}

#[tokio::test]
#[ignore = "FINDING: MIN(decimal_arb) output field carries NO decimal_arb \
            metadata — DecimalArbExtremeUdaf overrides return_type but not \
            return_field, so the sink sees bare LargeBinary (BYTEA/hex)"]
async fn min_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT MIN(amt) AS amt FROM t").await;
    assert_arb(&out.field("amt"), 100, 0, "MIN(decimal_arb)");
    assert_eq!(out.decode("amt", 0), vec![Some("-3".into())]);
}

#[tokio::test]
#[ignore = "FINDING: MAX(decimal_arb) output field carries NO decimal_arb \
            metadata (DecimalArbExtremeUdaf has no return_field override)"]
async fn max_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT MAX(amt) AS amt FROM t").await;
    assert_arb(&out.field("amt"), 100, 0, "MAX(decimal_arb)");
    assert_eq!(out.decode("amt", 0), vec![Some("200".into())]);
}

#[tokio::test]
#[ignore = "FINDING: SUM(decimal_arb) output field carries NO decimal_arb \
            metadata (DecimalArbSumUdaf has no return_field override), so the \
            E6 widened precision p+16 is invisible to the sink"]
async fn sum_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT SUM(amt) AS amt FROM t").await;
    assert!(
        DecimalArbType::is_decimal_arb_field(&out.field("amt")),
        "SUM(decimal_arb) lost metadata: {:?}",
        out.field("amt")
    );
    let (_, s) = DecimalArbType::precision_scale_from_field(&out.field("amt")).unwrap();
    assert_eq!(s, 0, "SUM must keep the input scale");
    assert_eq!(out.decode("amt", 0), vec![Some("202".into())]);
}

#[tokio::test]
#[ignore = "FINDING: AVG(decimal_arb) output field carries NO decimal_arb \
            metadata AND AVG widens the scale by 1, so the emitted bytes are \
            at scale s+1 with nothing in the schema saying so — a consumer \
            that assumes the input scale reads every AVG 10x too large"]
async fn avg_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("2"), Some("4")]);
    let out = run(&sm, "SELECT AVG(amt) AS amt FROM t").await;
    assert!(
        DecimalArbType::is_decimal_arb_field(&out.field("amt")),
        "AVG(decimal_arb) lost metadata: {:?}",
        out.field("amt")
    );
}

/// Demonstrates the concrete damage of the AVG half of the finding: the bytes
/// are at scale 1 (AVG widens scale by 1) while the schema says nothing, so a
/// consumer reading at the input scale 0 gets `30` instead of `3.0`.
#[tokio::test]
async fn avg_over_decimal_arb_emits_scale_shifted_bytes_with_no_scale_in_schema() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("2"), Some("4")]);
    let out = run(&sm, "SELECT AVG(amt) AS amt FROM t").await;
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out.field("amt")),
        None,
        "if this now returns Some(..) the AVG metadata finding is fixed — \
         delete this test and un-ignore avg_over_decimal_arb_preserves_metadata"
    );
    assert_eq!(
        out.decode("amt", 1),
        vec![Some("3.0".into())],
        "AVG output bytes are at scale s+1 = 1"
    );
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("30".into())],
        "…and reading them at the input scale 0 silently yields 30, not 3"
    );
}

#[tokio::test]
#[ignore = "FINDING: grouped MIN(decimal_arb) output field carries NO \
            decimal_arb metadata (see min_over_decimal_arb_preserves_metadata)"]
async fn grouped_min_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT grp, MIN(amt) AS amt FROM t GROUP BY grp").await;
    assert_arb(&out.field("amt"), 100, 0, "grouped MIN");
}

#[tokio::test]
#[ignore = "FINDING: grouped MAX(decimal_arb) output field carries NO \
            decimal_arb metadata"]
async fn grouped_max_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT grp, MAX(amt) AS amt FROM t GROUP BY grp").await;
    assert_arb(&out.field("amt"), 100, 0, "grouped MAX");
}

#[tokio::test]
#[ignore = "FINDING: grouped SUM(decimal_arb) output field carries NO \
            decimal_arb metadata"]
async fn grouped_sum_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(&sm, "SELECT grp, SUM(amt) AS amt FROM t GROUP BY grp").await;
    assert!(
        DecimalArbType::is_decimal_arb_field(&out.field("amt")),
        "grouped SUM(decimal_arb) lost metadata: {:?}",
        out.field("amt")
    );
}

#[tokio::test]
#[ignore = "FINDING: MIN(decimal_arb) drops the metadata, so a scale-18 column \
            loses the only record of its scale on the way to the sink"]
async fn min_of_scale_18_column_keeps_scale_18() {
    let sm = session();
    register_arb(&sm, "t", 100, 18, &[Some("1.5"), Some("-2.25")]);
    let out = run(&sm, "SELECT MIN(amt) AS amt FROM t").await;
    assert_arb(&out.field("amt"), 100, 18, "MIN scale 18");
    assert_eq!(
        out.decode("amt", 18),
        vec![Some("-2.250000000000000000".into())]
    );
}

/// The GROUP BY *key* keeps its metadata even when the aggregate beside it
/// loses it — showing the loss is specific to the UDAF return field, not to
/// the Aggregate node.
#[tokio::test]
async fn group_by_key_keeps_metadata_even_when_aggregate_loses_it() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("5"), Some("-3")]);
    let out = run(&sm, "SELECT amt, MIN(amt) AS m FROM t GROUP BY amt").await;
    assert_arb(
        &out.field("amt"),
        100,
        0,
        "GROUP BY key beside an aggregate",
    );
}

#[tokio::test]
async fn having_over_decimal_arb_group_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("5"), Some("-3")]);
    let out = run(
        &sm,
        "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt HAVING COUNT(*) > 1",
    )
    .await;
    assert_arb(&out.field("amt"), 100, 0, "GROUP BY + HAVING");
    assert_eq!(out.decode("amt", 0), vec![Some("5".into())]);
}

#[tokio::test]
async fn group_by_rollup_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("-3")]);
    let out = run(&sm, "SELECT amt, COUNT(*) AS n FROM t GROUP BY ROLLUP(amt)").await;
    assert_arb(&out.field("amt"), 100, 0, "GROUP BY ROLLUP");
}

#[tokio::test]
async fn group_by_grouping_sets_over_decimal_arb_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(
        &sm,
        "SELECT amt, grp, COUNT(*) AS n FROM t GROUP BY GROUPING SETS ((amt), (grp))",
    )
    .await;
    assert_arb(&out.field("amt"), 100, 0, "GROUPING SETS");
}

#[tokio::test]
async fn count_distinct_over_decimal_arb_counts_numerically() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("5.0"), Some("-3")]);
    let out = run(&sm, "SELECT COUNT(DISTINCT amt) AS n FROM t").await;
    assert_eq!(
        out.int_col("n"),
        vec![Some(2)],
        "5 and 5.0 must be one distinct decimal_arb value"
    );
}

// `PARTITION BY <decimal_arb>` cannot execute in this session (EnforceSorting
// is not in `StreamlingPhysicalOptimizerRules::rules()`, so the window operator
// rejects the unsorted partition key), so assert on the sink-facing plan schema.
#[tokio::test]
async fn window_partition_by_decimal_arb_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "SELECT amt, ROW_NUMBER() OVER (PARTITION BY amt ORDER BY id) AS rn FROM t";
    assert_arb(&plan_field(&sm, sql, "amt").await, 100, 0, sql);
}

#[tokio::test]
#[ignore = "FINDING: a window with PARTITION BY over a decimal_arb column \
            plans but fails at execute() with 'Expects PARTITION BY expression \
            to be ordered' — StreamlingPhysicalOptimizerRules::rules() replaces \
            DataFusion's default physical rules, so EnforceSorting never runs"]
async fn window_partition_by_decimal_arb_executes() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(
        &sm,
        "SELECT amt, ROW_NUMBER() OVER (PARTITION BY amt ORDER BY id) AS rn FROM t",
    )
    .await;
    assert_arb(&out.field("amt"), 100, 0, "window PARTITION BY decimal_arb");
}

#[tokio::test]
async fn window_order_by_decimal_arb_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = run(
        &sm,
        "SELECT amt, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t",
    )
    .await;
    assert_arb(
        &out.field("amt"),
        100,
        0,
        "window with passthrough decimal_arb",
    );
}

// =====================================================================
// H. UNNEST / nested types
// =====================================================================

#[tokio::test]
async fn list_of_decimal_arb_projection_preserves_element_metadata() {
    let sm = session();
    register_list_of_arb(&sm, "t", 100, 0, &[vec![Some("1"), Some("2")]]);
    let out = run(&sm, "SELECT items FROM t").await;
    let f = out.field("items");
    let DataType::List(inner) = f.data_type() else {
        panic!("expected List, got {:?}", f.data_type());
    };
    assert!(
        DecimalArbType::is_decimal_arb_field(inner.as_ref()),
        "List element lost decimal_arb metadata: {inner:?}"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(inner.as_ref()),
        Some((100, 0))
    );
}

// ---------------------------------------------------------------------
// FINDING (UNNEST): the List *element* field carries full decimal_arb metadata
// (asserted above), but the column UNNEST produces from it is a bare
// `LargeBinary` with an empty metadata map — in BOTH the logical plan schema
// and the emitted batches. The values are correct canonical bytes, so nothing
// errors; the column just reaches the sink as BYTEA/hex. This is F2/F6's shape
// on the UNNEST path, and it is how array-of-decimal event data (the common
// blockchain-log shape this product ingests) gets flattened.
// ---------------------------------------------------------------------

#[tokio::test]
#[ignore = "FINDING: UNNEST(list<decimal_arb>) drops the element's decimal_arb \
            metadata — the output column is bare LargeBinary in both the plan \
            schema and the batches, so the sink emits BYTEA/hex"]
async fn unnest_of_decimal_arb_list_preserves_metadata() {
    let sm = session();
    register_list_of_arb(
        &sm,
        "t",
        100,
        0,
        &[vec![Some("1"), Some("2")], vec![Some("-3")]],
    );
    let out = run(&sm, "SELECT UNNEST(items) AS amt FROM t").await;
    assert_arb(&out.field("amt"), 100, 0, "UNNEST(list<decimal_arb>)");
}

/// Pins the "silent" half of the UNNEST finding: right bytes, no metadata.
#[tokio::test]
async fn unnest_of_decimal_arb_list_yields_right_bytes_with_no_metadata() {
    let sm = session();
    register_list_of_arb(
        &sm,
        "t",
        100,
        0,
        &[vec![Some("1"), Some("2")], vec![Some("-3")]],
    );
    let out = run(&sm, "SELECT UNNEST(items) AS amt FROM t").await;
    assert_eq!(out.field("amt").data_type(), &DataType::LargeBinary);
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out.field("amt")),
        None,
        "if this is now Some(..) the UNNEST metadata finding is fixed — delete \
         this test and un-ignore unnest_of_decimal_arb_list_preserves_metadata"
    );
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![Some("-3".into()), Some("1".into()), Some("2".into())],
        "the bytes themselves are still valid canonical decimal_arb"
    );
}

/// The loss is already present in the *plan* schema, which is what sinks read.
#[tokio::test]
#[ignore = "FINDING: UNNEST(list<decimal_arb>) drops the metadata in the \
            unoptimized LogicalPlan schema too — the schema every sink's \
            TableProvider is built from (find_plan_and_schema)"]
async fn unnest_plan_schema_preserves_metadata() {
    let sm = session();
    register_list_of_arb(&sm, "t", 100, 0, &[vec![Some("1")]]);
    let sql = "SELECT UNNEST(items) AS amt FROM t";
    assert_arb(&plan_field(&sm, sql, "amt").await, 100, 0, sql);
}

#[tokio::test]
async fn unnest_of_decimal_arb_list_preserves_values() {
    let sm = session();
    register_list_of_arb(
        &sm,
        "t",
        100,
        0,
        &[vec![Some("1"), Some("2")], vec![Some("-3")]],
    );
    let out = run(&sm, "SELECT UNNEST(items) AS amt FROM t").await;
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![Some("-3".into()), Some("1".into()), Some("2".into())]
    );
}

#[tokio::test]
#[ignore = "FINDING: UNNEST drops decimal_arb metadata — for a scale-18 list \
            that means the scale needed to interpret the bytes is gone entirely"]
async fn unnest_scale_18_list_keeps_scale_18() {
    let sm = session();
    register_list_of_arb(&sm, "t", 100, 18, &[vec![Some("1.5")]]);
    let out = run(&sm, "SELECT UNNEST(items) AS amt FROM t").await;
    assert_arb(&out.field("amt"), 100, 18, "UNNEST scale 18");
    assert_eq!(
        out.decode("amt", 18),
        vec![Some("1.500000000000000000".into())]
    );
}

/// A downstream `WHERE amt > 0` over an UNNEST result: because the metadata is
/// gone, the decimal_arb comparison machinery no longer recognises the column.
#[tokio::test]
#[ignore = "FINDING: UNNEST drops decimal_arb metadata, so downstream nodes \
            (here a WHERE over the unnested column) no longer see a \
            decimal_arb column at all"]
async fn unnest_then_filter_preserves_metadata() {
    let sm = session();
    register_list_of_arb(&sm, "t", 100, 0, &[vec![Some("1"), Some("-2")]]);
    let sql = "SELECT amt FROM (SELECT UNNEST(items) AS amt FROM t) u WHERE amt > 0";
    assert_amt_arb(&sm, sql, 100, 0).await;
}

#[tokio::test]
#[ignore = "FINDING: UNNEST drops decimal_arb metadata, so a downstream \
            ORDER BY is no longer rewritten by DecimalArbSortRewriteRule and \
            falls back to bytewise LargeBinary ordering"]
async fn unnest_then_order_by_preserves_metadata() {
    let sm = session();
    register_list_of_arb(&sm, "t", 100, 0, &[vec![Some("1"), Some("-2"), Some("30")]]);
    let sql = "SELECT amt FROM (SELECT UNNEST(items) AS amt FROM t) u ORDER BY amt";
    let out = assert_amt_arb(&sm, sql, 100, 0).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("-2".into()), Some("1".into()), Some("30".into())]
    );
}

/// The consequence of the metadata loss above, made concrete: `ORDER BY` over
/// an unnested decimal_arb column sorts BYTEWISE, so `-2` lands last.
#[tokio::test]
async fn unnest_then_order_by_sorts_bytewise_not_numerically() {
    let sm = session();
    register_list_of_arb(&sm, "t", 100, 0, &[vec![Some("1"), Some("-2"), Some("30")]]);
    let sql = "SELECT amt FROM (SELECT UNNEST(items) AS amt FROM t) u ORDER BY amt";
    let out = run(&sm, sql).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("1".into()), Some("30".into()), Some("-2".into())],
        "with the metadata gone the sort rule cannot fire, so the negative \
         value (sign byte 0xFF) sorts AFTER the positives — if this test \
         starts failing, the UNNEST metadata finding has been fixed"
    );
}

#[tokio::test]
#[ignore = "FINDING: DataFrame::unnest_columns over a list<decimal_arb> drops \
            the element's decimal_arb metadata"]
async fn unnest_dataframe_api_preserves_metadata() {
    let sm = session();
    register_list_of_arb(&sm, "t", 100, 0, &[vec![Some("1"), Some("2")]]);
    let plan = sm
        .create_logical_plan("SELECT items FROM t".into())
        .await
        .unwrap();
    let df = sm.new_df(plan).unnest_columns(&["items"]).unwrap();
    let batches = df.collect().await.unwrap();
    let field = batches[0]
        .schema()
        .field_with_name("items")
        .unwrap()
        .clone();
    assert_arb(&field, 100, 0, "DataFrame::unnest_columns");
}

#[tokio::test]
async fn struct_column_projection_preserves_inner_metadata() {
    let sm = session();
    register_struct_of_arb(&sm, "t", 100, 0, &[Some("5"), Some("-3")]);
    let out = run(&sm, "SELECT st FROM t").await;
    let f = out.field("st");
    let DataType::Struct(fields) = f.data_type() else {
        panic!("expected Struct, got {:?}", f.data_type());
    };
    assert!(
        DecimalArbType::is_decimal_arb_field(fields[0].as_ref()),
        "struct child lost decimal_arb metadata: {:?}",
        fields[0]
    );
}

#[tokio::test]
async fn struct_field_access_preserves_metadata() {
    let sm = session();
    register_struct_of_arb(&sm, "t", 100, 0, &[Some("5"), Some("-3")]);
    let out = run(&sm, "SELECT st['amt'] AS amt FROM t").await;
    assert_arb(&out.field("amt"), 100, 0, "struct field access");
}

#[tokio::test]
async fn struct_field_access_preserves_values() {
    let sm = session();
    register_struct_of_arb(&sm, "t", 100, 0, &[Some("5"), Some("-3")]);
    let out = run(&sm, "SELECT st['amt'] AS amt FROM t").await;
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![Some("-3".into()), Some("5".into())]
    );
}

// =====================================================================
// I. Repartitioning
// =====================================================================

async fn repartitioned(sm: &SessionManager, sql: &str, part: Partitioning) -> Out {
    let plan = sm
        .create_logical_plan(sql.to_string())
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{sql}`: {e}"));
    let df = sm
        .new_df(plan)
        .repartition(part)
        .unwrap_or_else(|e| panic!("repartition failed for `{sql}`: {e}"));
    let logical: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("execution failed for `{sql}`: {e}"));
    Out { logical, batches }
}

#[tokio::test]
async fn round_robin_repartition_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = repartitioned(&sm, "SELECT amt FROM t", Partitioning::RoundRobinBatch(4)).await;
    assert_arb(&out.field("amt"), 100, 0, "RoundRobinBatch repartition");
}

#[tokio::test]
async fn round_robin_repartition_preserves_values() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = repartitioned(&sm, "SELECT amt FROM t", Partitioning::RoundRobinBatch(4)).await;
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![
            None,
            Some("-3".into()),
            Some("0".into()),
            Some("200".into()),
            Some("5".into())
        ]
    );
}

#[tokio::test]
async fn hash_repartition_on_decimal_arb_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = repartitioned(
        &sm,
        "SELECT amt FROM t",
        Partitioning::Hash(vec![col("amt")], 4),
    )
    .await;
    assert_arb(&out.field("amt"), 100, 0, "Hash(decimal_arb) repartition");
}

#[tokio::test]
async fn hash_repartition_on_other_column_preserves_arb_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = repartitioned(
        &sm,
        "SELECT id, amt FROM t",
        Partitioning::Hash(vec![col("id")], 3),
    )
    .await;
    assert_arb(&out.field("amt"), 100, 0, "Hash(id) repartition");
}

#[tokio::test]
async fn repartition_of_scale_18_column_keeps_scale() {
    let sm = session();
    register_arb(&sm, "t", 100, 18, &[Some("1.5"), Some("-2.25")]);
    let out = repartitioned(&sm, "SELECT amt FROM t", Partitioning::RoundRobinBatch(2)).await;
    assert_arb(&out.field("amt"), 100, 18, "repartition scale 18");
}

#[tokio::test]
async fn repartition_over_union_all_preserves_metadata() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let out = repartitioned(
        &sm,
        "SELECT amt FROM t1 UNION ALL SELECT amt FROM t2",
        Partitioning::RoundRobinBatch(4),
    )
    .await;
    assert_arb(&out.field("amt"), 100, 0, "repartition over UNION ALL");
}

#[tokio::test]
async fn repartition_over_derived_table_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let out = repartitioned(
        &sm,
        "SELECT amt FROM (SELECT amt, id FROM t WHERE id > 0) s",
        Partitioning::RoundRobinBatch(2),
    )
    .await;
    assert_arb(&out.field("amt"), 100, 0, "repartition over derived table");
}

#[tokio::test]
async fn hash_repartition_on_decimal_arb_keeps_every_row() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = repartitioned(
        &sm,
        "SELECT amt FROM t",
        Partitioning::Hash(vec![col("amt")], 4),
    )
    .await;
    assert_eq!(
        out.num_rows(),
        5,
        "hash-repartitioning on a decimal_arb key must not drop rows"
    );
}

// =====================================================================
// J. Filters, DataFrame API, F1b/F2 regressions inside deeper plans
// =====================================================================

#[tokio::test]
async fn filter_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    assert_amt_arb(&sm, "SELECT amt FROM t WHERE id > 1", 100, 0).await;
}

#[tokio::test]
async fn filter_on_decimal_arb_comparison_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = assert_amt_arb(&sm, "SELECT amt FROM t WHERE amt > 0", 100, 0).await;
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![Some("200".into()), Some("5".into())]
    );
}

#[tokio::test]
async fn filter_with_between_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = assert_amt_arb(&sm, "SELECT amt FROM t WHERE amt BETWEEN 0 AND 100", 100, 0).await;
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![Some("0".into()), Some("5".into())]
    );
}

#[tokio::test]
async fn filter_with_in_list_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = assert_amt_arb(&sm, "SELECT amt FROM t WHERE amt IN (5, 200)", 100, 0).await;
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![Some("200".into()), Some("5".into())]
    );
}

#[tokio::test]
async fn filter_with_not_in_list_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_amt_arb(&sm, "SELECT amt FROM t WHERE amt NOT IN (5)", 100, 0).await;
}

#[tokio::test]
async fn filter_with_is_null_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let out = assert_amt_arb(&sm, "SELECT amt FROM t WHERE amt IS NOT NULL", 100, 0).await;
    assert_eq!(out.num_rows(), 4);
}

#[tokio::test]
async fn case_inside_derived_table_preserves_metadata_at_top() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "SELECT chosen FROM \
               (SELECT CASE WHEN id > 2 THEN amt ELSE amt END AS chosen FROM t) c";
    let out = run(&sm, sql).await;
    assert_arb(
        &out.field("chosen"),
        100,
        0,
        "CASE inside derived table (F2)",
    );
}

#[tokio::test]
async fn case_inside_cte_preserves_metadata_at_top() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "WITH c AS (SELECT CASE WHEN id > 2 THEN amt ELSE amt END AS chosen FROM t) \
               SELECT chosen FROM c";
    let out = run(&sm, sql).await;
    assert_arb(&out.field("chosen"), 100, 0, "CASE inside CTE (F2)");
}

#[tokio::test]
async fn coalesce_inside_cte_preserves_metadata_at_top() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "WITH c AS (SELECT COALESCE(amt, amt) AS v FROM t) SELECT v FROM c";
    let out = run(&sm, sql).await;
    assert_arb(&out.field("v"), 100, 0, "COALESCE inside CTE (F2)");
}

#[tokio::test]
async fn case_in_union_branch_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "SELECT CASE WHEN id > 2 THEN amt ELSE amt END AS v FROM t \
               UNION ALL SELECT amt AS v FROM t";
    let out = run(&sm, sql).await;
    assert_arb(&out.field("v"), 100, 0, "CASE in UNION ALL branch (F2)");
}

/// CASE over a join's output: the analyzer-stage F2 re-stamp is what supplies
/// the metadata at runtime, and the join's plan schema is what a sink reads —
/// so here we can only check the plan side, and it is (per the F2 gap finding)
/// expected to be bare. Assert the join at least did not corrupt the *input*
/// columns' metadata.
#[tokio::test]
async fn case_over_join_output_leaves_join_input_metadata_intact() {
    let sm = session();
    join_tables(&sm);
    let sql = "SELECT l.amt AS raw, CASE WHEN l.id = 1 THEN l.amt ELSE l.amt END AS v \
               FROM l JOIN r ON l.id = r.id";
    assert_arb(
        &plan_field(&sm, sql, "raw").await,
        100,
        0,
        "join input column",
    );
}

#[tokio::test]
async fn coalesce_across_two_same_scale_columns_preserves_metadata() {
    let sm = session();
    let schema = Arc::new(Schema::new(vec![
        arb_field("a", 100, 0),
        arb_field("b", 40, 0),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arb_array("a", 100, 0, &[Some("5"), None])),
            Arc::new(arb_array("b", 40, 0, &[Some("9"), Some("11")])),
        ],
    )
    .unwrap();
    sm.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    let out = run(&sm, "SELECT COALESCE(a, b) AS v FROM t").await;
    assert!(
        DecimalArbType::is_decimal_arb_field(&out.field("v")),
        "COALESCE over two same-scale decimal_arb columns lost metadata: {:?}",
        out.field("v")
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out.field("v")).map(|(_, s)| s),
        Some(0),
        "COALESCE must keep the shared scale — a wrong scale silently rescales \
         every value"
    );
    assert_eq!(
        out.decode("v", 0),
        vec![Some("5".into()), Some("11".into())]
    );
}

#[tokio::test]
async fn coalesce_over_mixed_scale_columns_is_left_untouched_not_mis_stamped() {
    // Mixed scales cannot be stamped safely (the canonical bytes carry no
    // scale), so the rewrite must decline rather than stamp a wrong scale.
    let sm = session();
    let schema = Arc::new(Schema::new(vec![
        arb_field("a", 100, 0),
        arb_field("b", 40, 4),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arb_array("a", 100, 0, &[Some("5")])),
            Arc::new(arb_array("b", 40, 4, &[Some("9.5")])),
        ],
    )
    .unwrap();
    sm.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    let out = run(&sm, "SELECT COALESCE(a, b) AS v FROM t").await;
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out.field("v")),
        None,
        "mixed-scale COALESCE must NOT be stamped with one branch's scale — \
         that would silently rescale the other branch's bytes"
    );
}

#[tokio::test]
async fn dataframe_select_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let plan = sm
        .create_logical_plan("SELECT id, amt FROM t".into())
        .await
        .unwrap();
    let batches = sm
        .new_df(plan)
        .select(vec![col("amt")])
        .unwrap()
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema().field_with_name("amt").unwrap().clone();
    assert_arb(&field, 100, 0, "DataFrame::select");
}

#[tokio::test]
async fn dataframe_filter_preserves_metadata() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let plan = sm
        .create_logical_plan("SELECT id, amt FROM t".into())
        .await
        .unwrap();
    let batches = sm
        .new_df(plan)
        .filter(col("id").gt(datafusion::prelude::lit(1_i64)))
        .unwrap()
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema().field_with_name("amt").unwrap().clone();
    assert_arb(&field, 100, 0, "DataFrame::filter");
}

#[tokio::test]
async fn dataframe_distinct_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("5")]);
    let plan = sm
        .create_logical_plan("SELECT amt FROM t".into())
        .await
        .unwrap();
    let batches = sm.new_df(plan).distinct().unwrap().collect().await.unwrap();
    let field = batches[0].schema().field_with_name("amt").unwrap().clone();
    assert_arb(&field, 100, 0, "DataFrame::distinct");
}

#[tokio::test]
async fn dataframe_limit_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let plan = sm
        .create_logical_plan("SELECT amt FROM t".into())
        .await
        .unwrap();
    let batches = sm
        .new_df(plan)
        .limit(1, Some(2))
        .unwrap()
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema().field_with_name("amt").unwrap().clone();
    assert_arb(&field, 100, 0, "DataFrame::limit");
}

#[tokio::test]
async fn dataframe_join_preserves_metadata_on_both_sides() {
    let sm = session();
    join_tables(&sm);
    let lp = sm
        .create_logical_plan("SELECT id, amt AS lamt FROM l".into())
        .await
        .unwrap();
    let rp = sm
        .create_logical_plan("SELECT id AS rid, amt AS ramt FROM r".into())
        .await
        .unwrap();
    let df = sm
        .new_df(lp)
        .join(
            sm.new_df(rp),
            datafusion::common::JoinType::Inner,
            &["id"],
            &["rid"],
            None,
        )
        .unwrap();
    // HashJoinExec cannot execute here (see `hash_join_plans_can_execute`), so
    // assert on the plan schema, which is what the sinks are built from.
    let schema = df.schema().as_arrow().clone();
    assert_arb(
        &schema.field_with_name("lamt").unwrap().clone(),
        100,
        0,
        "DataFrame::join left",
    );
    assert_arb(
        &schema.field_with_name("ramt").unwrap().clone(),
        60,
        2,
        "DataFrame::join right",
    );
}

#[tokio::test]
async fn dataframe_intersect_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t1", 100, 0, &[Some("5"), Some("-3")]);
    register_arb(&sm, "t2", 100, 0, &[Some("5")]);
    let p1 = sm
        .create_logical_plan("SELECT amt FROM t1".into())
        .await
        .unwrap();
    let p2 = sm
        .create_logical_plan("SELECT amt FROM t2".into())
        .await
        .unwrap();
    let schema = sm
        .new_df(p1)
        .intersect(sm.new_df(p2))
        .unwrap()
        .schema()
        .as_arrow()
        .clone();
    let field = schema.field_with_name("amt").unwrap().clone();
    assert_arb(&field, 100, 0, "DataFrame::intersect");
}

#[tokio::test]
async fn dataframe_except_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t1", 100, 0, &[Some("5"), Some("-3")]);
    register_arb(&sm, "t2", 100, 0, &[Some("5")]);
    let p1 = sm
        .create_logical_plan("SELECT amt FROM t1".into())
        .await
        .unwrap();
    let p2 = sm
        .create_logical_plan("SELECT amt FROM t2".into())
        .await
        .unwrap();
    let schema = sm
        .new_df(p1)
        .except(sm.new_df(p2))
        .unwrap()
        .schema()
        .as_arrow()
        .clone();
    let field = schema.field_with_name("amt").unwrap().clone();
    assert_arb(&field, 100, 0, "DataFrame::except");
}

#[tokio::test]
async fn dataframe_with_column_preserves_existing_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let plan = sm
        .create_logical_plan("SELECT amt FROM t".into())
        .await
        .unwrap();
    let batches = sm
        .new_df(plan)
        .with_column("extra", datafusion::prelude::lit(1_i64))
        .unwrap()
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema().field_with_name("amt").unwrap().clone();
    assert_arb(&field, 100, 0, "DataFrame::with_column");
}

#[tokio::test]
async fn dataframe_sort_by_decimal_arb_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    let plan = sm
        .create_logical_plan("SELECT amt FROM t".into())
        .await
        .unwrap();
    let batches = sm
        .new_df(plan)
        .sort(vec![col("amt").sort(true, false)])
        .unwrap()
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema().field_with_name("amt").unwrap().clone();
    assert_arb(&field, 100, 0, "DataFrame::sort");
}

// =====================================================================
// K. Logical-vs-physical schema agreement
//
// A plan whose *logical* schema says decimal_arb but whose emitted batches
// say bare LargeBinary (or vice versa) is exactly the silent-corruption
// shape: the sink's CREATE TABLE is derived from one and the rows from the
// other.
// =====================================================================

async fn assert_logical_matches_physical(sm: &SessionManager, sql: &str, col_name: &str) {
    let plan = sm
        .create_logical_plan(sql.to_string())
        .await
        .unwrap_or_else(|e| panic!("planning failed for `{sql}`: {e}"));
    let df = sm.new_df(plan);
    let logical = df.schema().as_arrow().clone();
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|e| panic!("execution failed for `{sql}`: {e}"));
    let lf = logical.field_with_name(col_name).unwrap().clone();
    let batch = batches.first().expect("expected at least one output batch");
    let pf = batch.schema().field_with_name(col_name).unwrap().clone();
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&lf),
        DecimalArbType::precision_scale_from_field(&pf),
        "`{sql}`: logical schema and emitted batch schema disagree about \
         decimal_arb metadata for `{col_name}` (logical={lf:?} physical={pf:?})"
    );
}

#[tokio::test]
async fn logical_and_physical_agree_for_projection() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_logical_matches_physical(&sm, "SELECT amt FROM t", "amt").await;
}

#[tokio::test]
async fn logical_and_physical_agree_for_order_by() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_logical_matches_physical(&sm, "SELECT amt FROM t ORDER BY amt", "amt").await;
}

#[tokio::test]
async fn logical_and_physical_agree_for_group_by() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("5")]);
    assert_logical_matches_physical(&sm, "SELECT amt FROM t GROUP BY amt", "amt").await;
}

#[tokio::test]
async fn logical_and_physical_agree_for_union_all() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    assert_logical_matches_physical(
        &sm,
        "SELECT amt FROM t1 UNION ALL SELECT amt FROM t2",
        "amt",
    )
    .await;
}

#[tokio::test]
async fn logical_and_physical_agree_for_subquery_alias() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_logical_matches_physical(&sm, "SELECT amt FROM (SELECT amt FROM t) s", "amt").await;
}

#[tokio::test]
async fn logical_and_physical_agree_for_limit_offset() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_logical_matches_physical(&sm, "SELECT amt FROM t LIMIT 3 OFFSET 1", "amt").await;
}

#[tokio::test]
async fn logical_and_physical_agree_for_distinct() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, &[Some("5"), Some("5")]);
    assert_logical_matches_physical(&sm, "SELECT DISTINCT amt FROM t", "amt").await;
}

// ---------------------------------------------------------------------
// FINDING (F2 fix is only half-applied): `DecimalArbExprRewrite` restores
// CASE/COALESCE metadata via `decimal_arb_with_meta`, but it is a
// `FunctionRewrite`, which runs in the ANALYZER — i.e. only when the plan is
// optimized on the way to a physical plan. The *unoptimized* `LogicalPlan`
// schema, which `crates/streamling/src/lib.rs` hands to every sink
// (`find_plan_and_schema` → `plan.schema().inner()` → the sink's
// TableProvider, and `track_primary_key_for_transform_or_sink`), still says
// bare `LargeBinary`. So a `CASE`/`COALESCE` over decimal_arb produces a sink
// table declared BYTEA while the rows streaming into it carry decimal_arb
// metadata: exactly the F2 failure the fix was supposed to close, one layer up.
// ---------------------------------------------------------------------

#[tokio::test]
#[ignore = "FINDING: the F2 CASE/COALESCE metadata fix (DecimalArbExprRewrite) \
            is a FunctionRewrite that only runs in the analyzer, so the \
            unoptimized LogicalPlan schema — the schema every sink is built \
            from — still reports bare LargeBinary while the batches carry \
            decimal_arb metadata"]
async fn logical_and_physical_agree_for_case() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    assert_logical_matches_physical(
        &sm,
        "SELECT CASE WHEN id > 2 THEN amt ELSE amt END AS v FROM t",
        "v",
    )
    .await;
}

#[tokio::test]
#[ignore = "FINDING: same as logical_and_physical_agree_for_case, for COALESCE"]
async fn logical_and_physical_agree_for_coalesce() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    assert_logical_matches_physical(&sm, "SELECT COALESCE(amt, amt) AS v FROM t", "v").await;
}

#[tokio::test]
#[ignore = "FINDING: the sink-facing LogicalPlan schema for a CASE over \
            decimal_arb has no decimal_arb metadata (the F2 fix runs only in \
            the analyzer), so the sink declares the column BYTEA/hex"]
async fn case_over_decimal_arb_preserves_metadata_in_sink_facing_plan_schema() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "SELECT CASE WHEN id > 2 THEN amt ELSE amt END AS v FROM t";
    assert_arb(&plan_field(&sm, sql, "v").await, 100, 0, sql);
}

#[tokio::test]
#[ignore = "FINDING: same as above, for COALESCE"]
async fn coalesce_over_decimal_arb_preserves_metadata_in_sink_facing_plan_schema() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "SELECT COALESCE(amt, amt) AS v FROM t";
    assert_arb(&plan_field(&sm, sql, "v").await, 100, 0, sql);
}

/// Pins the divergence itself: plan schema says "not decimal_arb", batches say
/// "decimal_arb(100, 0)". Passing means the bug is still present.
#[tokio::test]
async fn case_metadata_diverges_between_plan_schema_and_batches() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "SELECT CASE WHEN id > 2 THEN amt ELSE amt END AS v FROM t";
    let plan_f = plan_field(&sm, sql, "v").await;
    let out = run(&sm, sql).await;
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&plan_f),
        None,
        "if this is now Some(..) the sink-facing CASE metadata gap is fixed — \
         un-ignore case_over_decimal_arb_preserves_metadata_in_sink_facing_plan_schema"
    );
    assert_eq!(
        DecimalArbType::precision_scale_from_field(&out.field("v")),
        Some((100, 0)),
        "the runtime batches DO carry the metadata (the F2 fix), which is what \
         makes the plan-schema gap silent rather than loud"
    );
}

/// A plain column reference does not have the gap — establishing that the
/// divergence is specific to the analyzer-stage rewrite, not to plan schemas.
#[tokio::test]
async fn plain_column_has_no_plan_versus_batch_metadata_gap() {
    let sm = session();
    register_std(&sm, "t", 100, 0, &std_rows());
    let sql = "SELECT amt FROM t";
    assert_arb(&plan_field(&sm, sql, "amt").await, 100, 0, "plan schema");
    assert_arb(&run(&sm, sql).await.field("amt"), 100, 0, "batch schema");
}

#[tokio::test]
async fn logical_and_physical_agree_for_unnest() {
    let sm = session();
    register_list_of_arb(&sm, "t", 100, 0, &[vec![Some("1"), Some("2")]]);
    assert_logical_matches_physical(&sm, "SELECT UNNEST(items) AS amt FROM t", "amt").await;
}

#[tokio::test]
async fn logical_and_physical_agree_for_min_aggregate() {
    let sm = session();
    register_arb(&sm, "t", 100, 0, V);
    assert_logical_matches_physical(&sm, "SELECT MIN(amt) AS amt FROM t", "amt").await;
}

#[tokio::test]
async fn logical_and_physical_agree_for_deep_nesting() {
    let sm = session();
    register_arb(&sm, "t", 100, 12, &[Some("1.5")]);
    let sql = "WITH a AS (SELECT amt FROM t) \
               SELECT amt FROM (SELECT amt FROM (SELECT amt FROM a ORDER BY amt LIMIT 10) x) y";
    assert_logical_matches_physical(&sm, sql, "amt").await;
}

// =====================================================================
// L. Long composite pipelines — every node stacked
// =====================================================================

#[tokio::test]
async fn full_pipeline_cte_join_group_order_limit_preserves_metadata() {
    let sm = session();
    register_std(&sm, "l", 100, 0, &std_rows());
    register_std(&sm, "r", 100, 0, &std_rows());
    let sql = "WITH j AS (SELECT l.grp AS grp, l.amt AS amt FROM l JOIN r ON l.id = r.id) \
               SELECT grp, amt FROM j GROUP BY grp, amt ORDER BY grp LIMIT 10";
    assert_arb(
        &plan_field(&sm, sql, "amt").await,
        100,
        0,
        "CTE→join→group→order→limit",
    );
}

// ---------------------------------------------------------------------
// FINDING (silent row loss): any aggregation (DISTINCT / GROUP BY) placed over
// a multi-partition input — a UNION ALL is the everyday case — silently drops
// every row that is not in partition 0. `StreamlingPhysicalOptimizerRules::rules()`
// REPLACES DataFusion's default physical rule set, so `EnforceDistribution`
// never runs and no `CoalescePartitionsExec` is inserted between the partial
// and final aggregate. No error is raised; the output is simply short.
// ---------------------------------------------------------------------

#[tokio::test]
#[ignore = "FINDING: DISTINCT over a UNION ALL silently returns only the rows \
            from the first union branch (partition 0) — EnforceDistribution is \
            missing from StreamlingPhysicalOptimizerRules, so no \
            CoalescePartitionsExec is inserted before the final aggregate"]
async fn full_pipeline_union_distinct_order_preserves_metadata() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let sql = "SELECT DISTINCT amt FROM \
               (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u \
               ORDER BY amt";
    let out = assert_amt_arb(&sm, sql, 100, 0).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![
            Some("-3".into()),
            Some("0".into()),
            Some("5".into()),
            Some("7".into())
        ],
        "DISTINCT over a UNION ALL of decimal_arb must keep all four values, \
         numerically ordered"
    );
}

#[tokio::test]
#[ignore = "FINDING: DISTINCT over a UNION ALL drops the second branch's rows \
            entirely (see full_pipeline_union_distinct_order_preserves_metadata)"]
async fn union_distinct_order_by_with_offset_keeps_all_remaining_rows() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let sql = "SELECT DISTINCT amt FROM \
               (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u \
               ORDER BY amt OFFSET 1";
    let out = assert_amt_arb(&sm, sql, 100, 0).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("0".into()), Some("5".into()), Some("7".into())],
        "OFFSET 1 over 4 numerically ordered rows must return the last 3"
    );
}

#[tokio::test]
async fn group_by_over_union_all_keeps_every_branch() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let sql = "SELECT amt, COUNT(*) AS n FROM \
               (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u GROUP BY amt";
    let out = run(&sm, sql).await;
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![
            Some("-3".into()),
            Some("0".into()),
            Some("5".into()),
            Some("7".into())
        ],
        "GROUP BY over UNION ALL must see all branches"
    );
}

#[tokio::test]
#[ignore = "FINDING: COUNT(*) over a UNION ALL undercounts — it only sees the \
            first branch's partition"]
async fn count_star_over_union_all_counts_every_branch() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let sql = "SELECT COUNT(*) AS n FROM \
               (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u";
    let out = run(&sm, sql).await;
    assert_eq!(out.int_col("n"), vec![Some(4)], "2 + 2 rows");
}

#[tokio::test]
#[ignore = "FINDING: GROUP BY over a UNION ALL never runs a final aggregate \
            stage — with a key present in both branches it emits one PARTIAL \
            group row per branch (5→2 and 5→1) instead of one merged group \
            (5→3). EnforceDistribution, which would insert the \
            CoalescePartitionsExec, is missing from \
            StreamlingPhysicalOptimizerRules::rules()"]
async fn group_by_over_union_all_with_shared_key_merges_the_groups() {
    let sm = session();
    register_arb(&sm, "t1", 100, 0, &[Some("5"), Some("5")]);
    register_arb(&sm, "t2", 100, 0, &[Some("5")]);
    let sql = "SELECT amt, COUNT(*) AS n FROM \
               (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u GROUP BY amt";
    let out = run(&sm, sql).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("5".into())],
        "a key present in both union branches must yield exactly one group"
    );
    assert_eq!(out.int_col("n"), vec![Some(3)], "2 + 1 rows in that group");
}

/// Pins today's (wrong) grouped output so a fix is detectable.
#[tokio::test]
async fn group_by_over_union_all_with_shared_key_currently_emits_partial_groups() {
    let sm = session();
    register_arb(&sm, "t1", 100, 0, &[Some("5"), Some("5")]);
    register_arb(&sm, "t2", 100, 0, &[Some("5")]);
    let sql = "SELECT amt, COUNT(*) AS n FROM \
               (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u GROUP BY amt";
    let out = run(&sm, sql).await;
    let mut counts = out.int_col("n");
    counts.sort();
    assert_eq!(
        counts,
        vec![Some(1), Some(2)],
        "if this fails, GROUP BY over UNION ALL now merges partial groups — \
         un-ignore group_by_over_union_all_with_shared_key_merges_the_groups"
    );
}

/// Scope probe: DISTINCT (without ORDER BY) over a UNION ALL of plain Int64 is
/// fine, so the loss is not a blanket "unions are broken" problem.
#[tokio::test]
async fn distinct_over_union_all_of_plain_int64_keeps_every_branch() {
    let sm = session();
    for (name, vals) in [("i1", vec![1_i64, 2]), ("i2", vec![3_i64, 4])] {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))]).unwrap();
        sm.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }
    let out = run(
        &sm,
        "SELECT DISTINCT v FROM (SELECT v FROM i1 UNION ALL SELECT v FROM i2) u",
    )
    .await;
    let mut got = out.int_col("v");
    got.sort();
    assert_eq!(got, vec![Some(1), Some(2), Some(3), Some(4)]);
}

/// DISTINCT *without* ORDER BY over the same union keeps all four values —
/// isolating the row loss to the ORDER BY (Sort) node, which requires a single
/// input partition and gets none.
#[tokio::test]
async fn distinct_over_union_all_without_order_by_keeps_every_branch() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let sql = "SELECT DISTINCT amt FROM \
               (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u";
    let out = run(&sm, sql).await;
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![
            Some("-3".into()),
            Some("0".into()),
            Some("5".into()),
            Some("7".into())
        ]
    );
}

#[tokio::test]
#[ignore = "FINDING: ORDER BY over a UNION ALL silently returns only the rows \
            of the first branch — SortExec requires a single input partition \
            and EnforceDistribution (which inserts CoalescePartitionsExec) is \
            not in StreamlingPhysicalOptimizerRules::rules()"]
async fn order_by_over_union_all_keeps_every_branch() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let sql = "SELECT amt FROM (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u \
               ORDER BY amt";
    let out = run(&sm, sql).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![
            Some("-3".into()),
            Some("0".into()),
            Some("5".into()),
            Some("7".into())
        ],
        "ORDER BY must not drop union branches"
    );
}

#[tokio::test]
#[ignore = "FINDING: ORDER BY over a UNION ALL drops branches for plain Int64 \
            too — the defect is in the physical rule set, not decimal_arb"]
async fn order_by_over_union_all_of_plain_int64_keeps_every_branch() {
    let sm = session();
    for (name, vals) in [("i1", vec![1_i64, 2]), ("i2", vec![3_i64, 4])] {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))]).unwrap();
        sm.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }
    let out = run(
        &sm,
        "SELECT v FROM (SELECT v FROM i1 UNION ALL SELECT v FROM i2) u ORDER BY v",
    )
    .await;
    assert_eq!(
        out.int_col("v"),
        vec![Some(1), Some(2), Some(3), Some(4)],
        "ORDER BY must not drop union branches"
    );
}

#[tokio::test]
#[ignore = "FINDING: LIMIT over a UNION ALL only sees the first branch's \
            partition, so it can return fewer rows than requested even though \
            enough rows exist"]
async fn limit_over_union_all_sees_every_branch() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let out = run(
        &sm,
        "SELECT amt FROM (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u LIMIT 3",
    )
    .await;
    assert_eq!(out.num_rows(), 3, "4 rows available, LIMIT 3 must return 3");
}

/// Pins today's (wrong) ORDER BY behaviour so a fix is detectable.
#[tokio::test]
async fn order_by_over_union_all_currently_returns_only_the_first_branch() {
    let sm = session();
    two_arb_tables(&sm, 100, 0);
    let sql = "SELECT amt FROM (SELECT amt FROM t1 UNION ALL SELECT amt FROM t2) u \
               ORDER BY amt";
    let out = run(&sm, sql).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("-3".into()), Some("5".into())],
        "t1 = [5, -3], t2 = [7, 0]: only t1 survives the ORDER BY — if this \
         fails, the union/ORDER BY row-loss finding has been fixed"
    );
}

/// A DISTINCT over a SINGLE-partition input is fine — isolating the defect to
/// the multi-partition (UNION ALL) case.
#[tokio::test]
async fn distinct_over_single_partition_input_keeps_every_row() {
    let sm = session();
    register_arb(
        &sm,
        "t",
        100,
        0,
        &[Some("5"), Some("-3"), Some("7"), Some("0")],
    );
    let out = run(&sm, "SELECT DISTINCT amt FROM t").await;
    assert_eq!(
        out.decode_sorted("amt", 0),
        vec![
            Some("-3".into()),
            Some("0".into()),
            Some("5".into()),
            Some("7".into())
        ]
    );
}

#[tokio::test]
async fn order_by_decimal_arb_with_offset_keeps_all_remaining_rows() {
    let sm = session();
    register_arb(
        &sm,
        "t",
        100,
        0,
        &[Some("5"), Some("-3"), Some("7"), Some("0")],
    );
    let out = assert_amt_arb(&sm, "SELECT amt FROM t ORDER BY amt OFFSET 1", 100, 0).await;
    assert_eq!(
        out.decode("amt", 0),
        vec![Some("0".into()), Some("5".into()), Some("7".into())],
        "ORDER BY decimal_arb + OFFSET must skip exactly one row"
    );
}

#[tokio::test]
async fn distinct_decimal_arb_with_offset_keeps_all_remaining_rows() {
    let sm = session();
    register_arb(
        &sm,
        "t",
        100,
        0,
        &[Some("5"), Some("-3"), Some("7"), Some("0")],
    );
    let out = assert_amt_arb(&sm, "SELECT DISTINCT amt FROM t OFFSET 1", 100, 0).await;
    assert_eq!(
        out.num_rows(),
        3,
        "DISTINCT + OFFSET 1 over 4 distinct rows"
    );
}

#[tokio::test]
#[ignore = "FINDING: UNNEST drops decimal_arb metadata, so a CASE downstream \
            of it is no longer recognised as decimal_arb and the F2 re-stamp \
            never fires — the whole unnest→case→group chain reaches the sink \
            as BYTEA"]
async fn full_pipeline_unnest_case_group_preserves_metadata() {
    let sm = session();
    register_list_of_arb(&sm, "t", 100, 0, &[vec![Some("1"), Some("2"), Some("2")]]);
    let sql = "SELECT v, COUNT(*) AS n FROM \
               (SELECT CASE WHEN true THEN u ELSE u END AS v \
                FROM (SELECT UNNEST(items) AS u FROM t) x) y \
               GROUP BY v";
    let out = run(&sm, sql).await;
    assert_arb(&out.field("v"), 100, 0, "unnest→case→group");
}

#[tokio::test]
async fn full_pipeline_subquery_filter_sort_repartition_preserves_metadata() {
    let sm = session();
    register_std(
        &sm,
        "t",
        100,
        6,
        &[(1, "a", Some("1.5")), (2, "b", Some("-2.25"))],
    );
    let out = repartitioned(
        &sm,
        "SELECT amt FROM (SELECT amt FROM t WHERE id >= 1 ORDER BY amt) s",
        Partitioning::RoundRobinBatch(2),
    )
    .await;
    assert_arb(
        &out.field("amt"),
        100,
        6,
        "subquery→filter→sort→repartition",
    );
    assert_eq!(
        out.decode_sorted("amt", 6),
        vec![Some("-2.250000".into()), Some("1.500000".into())]
    );
}

#[tokio::test]
async fn deeply_nested_ten_level_projection_preserves_metadata() {
    let sm = session();
    register_arb(&sm, "t", 100, 2, &[Some("1.25")]);
    let mut sql = String::from("SELECT amt FROM t");
    for _ in 0..10 {
        sql = format!("SELECT amt FROM ({sql}) q");
    }
    let out = assert_amt_arb(&sm, &sql, 100, 2).await;
    assert_eq!(out.decode("amt", 2), vec![Some("1.25".into())]);
}
