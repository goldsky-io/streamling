//! Literal comparisons against decimal_arb through Streamling's supported-plan
//! entry point (SQL preprocessor included).
//!
//! `WHERE amount > '1000…'` used to fall back to DataFusion's `Utf8 →
//! LargeBinary` coercion and compare UTF-8 bytes with the canonical encoding,
//! so the filter silently dropped every row. A bare numeric literal that
//! DataFusion types as Float64 (wider than u64, fractional, or with an
//! exponent) lost its digits before any decimal_arb rule ran and then failed
//! coercion outright; the preprocessor now quotes it next to a decimal_arb
//! operand, and the planner parses the text exactly.
use arrow::array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use streamling_common::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType};
use streamling_core::dynamic_table::DynamicTableRegistry;
use streamling_core::session::SessionManager;

fn session() -> SessionManager {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    let mut b = DecimalArbArrayBuilder::with_capacity(3, "v", 110, 2).unwrap();
    for v in ["-1", "2", "123456789012345678901234567890.5"] {
        b.append_str(v).unwrap();
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("v", 110, 2, false).unwrap(),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(b.finish().into_inner().0),
        ],
    )
    .unwrap();
    sm.session_context().register_batch("t", batch).unwrap();
    sm
}

async fn ids(sql: &str) -> Vec<i64> {
    let sm = session();
    let (plan, source) = sm
        .create_supported_logical_plan(sql.to_owned())
        .await
        .unwrap();
    assert_eq!(source, "t");
    let batches = sm.new_df(plan).collect().await.unwrap();
    let mut out: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn supported_plan_string_literal_predicates_are_numeric() {
    assert_eq!(ids("SELECT id FROM t WHERE v > '0'").await, vec![2, 3]);
    assert_eq!(ids("SELECT id FROM t WHERE v = '2.00'").await, vec![2]);
    assert_eq!(ids("SELECT id FROM t WHERE v < '-0.5'").await, vec![1]);
    assert_eq!(
        ids("SELECT id FROM t WHERE v = '123456789012345678901234567890.5'").await,
        vec![3]
    );
    assert_eq!(
        ids("SELECT id FROM t WHERE v IN ('2', '123456789012345678901234567890.50')").await,
        vec![2, 3]
    );
    assert_eq!(
        ids("SELECT id FROM t WHERE v BETWEEN '-1' AND '2'").await,
        vec![1, 2]
    );
}

/// The first column of every row, rendered, sorted.
async fn col0(sql: &str) -> Vec<String> {
    use arrow::util::display::array_value_to_string;
    let sm = session();
    let (plan, _) = sm
        .create_supported_logical_plan(sql.to_owned())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let batches = sm
        .new_df(plan)
        .collect()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out: Vec<String> = batches
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(|i| array_value_to_string(b.column(0), i).unwrap()))
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn supported_plan_wide_and_fractional_numeric_literals_keep_their_digits() {
    // Rows: v = -1.00, 2.00, 123456789012345678901234567890.50.
    let cases: [(&str, &[&str]); 25] = [
        // Wider than u64: Float64 would plan as 999999999999999983222784 and
        // then fail coercion against LargeBinary.
        (
            "SELECT id FROM t WHERE v > 1000000000000000000000000",
            &["3"],
        ),
        (
            "SELECT id FROM t WHERE 1000000000000000000000000 < v",
            &["3"],
        ),
        ("SELECT id FROM t WHERE v > 18446744073709551616", &["3"]),
        ("SELECT id FROM t WHERE v > 1e24", &["3"]),
        // Every digit counts: Float64 would have carried
        // 123456789012345680000000000000 and matched nothing.
        (
            "SELECT id FROM t WHERE v = 123456789012345678901234567890.5",
            &["3"],
        ),
        ("SELECT id FROM t WHERE v = 2.000000000000000000001", &[]),
        // Fractional literals.
        ("SELECT id FROM t WHERE v < -0.5", &["1"]),
        ("SELECT id FROM t WHERE -v > 0.5", &["1"]),
        ("SELECT id FROM t WHERE abs(v) > 1.5", &["2", "3"]),
        ("SELECT id FROM t WHERE v * 1.5 > 2.9", &["2", "3"]),
        ("SELECT id FROM t WHERE v * '1.5' > 2.9", &["2", "3"]),
        (
            "SELECT id FROM t WHERE v + 1000000000000000000000000 > 1000000000000000000000001",
            &["2", "3"],
        ),
        // BETWEEN / IN / CASE / function arguments / array literals.
        ("SELECT id FROM t WHERE v BETWEEN -0.5 AND 2.5", &["2"]),
        (
            "SELECT id FROM t WHERE v BETWEEN 1000000000000000000000000 AND 999999999999999999999999999999999",
            &["3"],
        ),
        (
            "SELECT id FROM t WHERE 1000000000000000000000000 BETWEEN v AND 999999999999999999999999999999999",
            &["2", "3"],
        ),
        (
            "SELECT id FROM t WHERE v IN (2, 1000000000000000000000000)",
            &["2"],
        ),
        ("SELECT id FROM t WHERE coalesce(v, 0.5) > 1.5", &["2", "3"]),
        ("SELECT id FROM t WHERE greatest(v, 2.5) = 2.5", &["1", "2"]),
        ("SELECT id FROM t WHERE nullif(v, 2.0) IS NULL", &["2"]),
        (
            "SELECT id FROM t WHERE CASE WHEN id = 1 THEN v ELSE 0.5 END < 1",
            &["1", "2", "3"],
        ),
        (
            "SELECT id FROM t WHERE CASE v WHEN 2.0 THEN 1 ELSE 0 END = 1",
            &["2"],
        ),
        ("SELECT id FROM t WHERE array_has([v, 0.5], 2.0)", &["2"]),
        // Aliases through CTEs and derived tables.
        (
            "SELECT id FROM (SELECT id, v AS g FROM t) s WHERE g > 1000000000000000000000000",
            &["3"],
        ),
        // Non-decimal operands keep DataFusion's own typing.
        ("SELECT id FROM t WHERE id > 0.5", &["1", "2", "3"]),
        (
            "SELECT id FROM t WHERE id * 1.5 > 2 AND v <> 2.5",
            &["2", "3"],
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(col0(sql).await, expected, "{sql}");
    }
}

/// `CAST(<decimal_arb> AS TEXT)` renders the number wherever the operand
/// comes from: a CTE alias, a derived-table alias, or an arithmetic result —
/// the plan-level rewrite does not depend on the preprocessor's column set.
#[tokio::test]
async fn supported_plan_text_casts_of_aliased_decimal_arb_render_the_number() {
    let rows = ["-1.00", "123456789012345678901234567890.50", "2.00"];
    assert_eq!(
        col0("WITH c AS (SELECT id, v AS g FROM t) SELECT CAST(g AS TEXT) FROM c").await,
        rows
    );
    assert_eq!(
        col0("SELECT CAST(g AS VARCHAR) FROM (SELECT id, v AS g FROM t) s").await,
        rows
    );
    assert_eq!(
        col0(
            "WITH c AS (SELECT id, v AS g FROM t) SELECT id FROM c WHERE CAST(g AS TEXT) = '2.00'"
        )
        .await,
        ["2"]
    );
    assert_eq!(
        col0("SELECT id FROM (SELECT id, v AS g FROM t) s WHERE CAST(g AS VARCHAR) = '2.00'").await,
        ["2"]
    );
    assert_eq!(
        col0("SELECT CAST(v + 1 AS TEXT) FROM t").await,
        ["0", "123456789012345678901234567891.50", "3.00"]
    );
}
