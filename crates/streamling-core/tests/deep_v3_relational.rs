//! Bounded relational numerical compatibility probes, with exact native controls.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
};
use datafusion::{common::ScalarValue, datasource::MemTable};
use std::sync::Arc;
use streamling_common::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType};
use streamling_core::{dynamic_table::DynamicTableRegistry, session::SessionManager};

fn session(native: bool) -> SessionManager {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    let values = [
        Some(255i128),
        Some(256),
        Some(255),
        Some(256),
        None,
        Some(-1),
        Some(0),
    ];
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7]))];
    for (name, scale) in [("a", 0), ("b", 2)] {
        if native {
            fields.push(Field::new(name, DataType::Decimal128(30, scale), true));
            arrays.push(Arc::new(
                Decimal128Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v * 10i128.pow(scale as u32)))
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(30, scale)
                .unwrap(),
            ));
        } else {
            fields.push(DecimalArbType::field(name, 100, scale as u32, true).unwrap());
            let mut builder =
                DecimalArbArrayBuilder::with_capacity(values.len(), name, 100, scale as u32)
                    .unwrap();
            for v in values {
                match v {
                    Some(v) => builder.append_str(&v.to_string()).unwrap(),
                    None => builder.append_null(),
                }
            }
            arrays.push(Arc::new(builder.finish().into_inner().0));
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    sm.session_context()
        .register_table(
            "t",
            Arc::new(
                MemTable::try_new(schema, vec![vec![batch.slice(0, 3), batch.slice(3, 4)]])
                    .unwrap(),
            ),
        )
        .unwrap();
    sm
}
async fn execute(native: bool, inner: &str) -> Result<Vec<String>, String> {
    let sm = session(native);
    let sql = format!("SELECT ({inner}) AS result FROM t WHERE id=1");
    let (plan, _) = sm
        .create_supported_logical_plan(sql.clone())
        .await
        .map_err(|e| e.to_string())?;
    let df = sm.new_df(plan);
    let physical = df.create_physical_plan().await.map_err(|e| e.to_string())?;
    eprintln!(
        "RELATIONAL_PLAN native={native} sql={sql}\n{}",
        datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
    );
    let rows = datafusion::physical_plan::collect(physical, Arc::new(df.task_ctx()))
        .await
        .map_err(|e| e.to_string())?;
    rows.iter()
        .flat_map(|b| {
            (0..b.num_rows())
                .map(move |i| ScalarValue::try_from_array(b.column(0), i).map(|v| format!("{v:?}")))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn relational_aggregate_group_window_matrix() {
    let cases = [
        (
            "min_lt_max",
            "SELECT MIN(a)<MAX(a) FROM t WHERE id<=2",
            "Boolean(true)",
        ),
        (
            "min_eq_scaled_min",
            "SELECT MIN(a)=MIN(b) FROM t WHERE id<=2",
            "Boolean(true)",
        ),
        (
            "max_eq_scaled_max",
            "SELECT MAX(a)=MAX(b) FROM t WHERE id<=2",
            "Boolean(true)",
        ),
        (
            "sum_eq_scaled_sum",
            "SELECT SUM(a)=SUM(b) FROM t WHERE id<=2",
            "Boolean(true)",
        ),
        (
            "avg_eq_scaled_avg",
            "SELECT AVG(a)=AVG(b) FROM t WHERE id<=2",
            "Boolean(true)",
        ),
        (
            "filtered_sum_order",
            "SELECT SUM(a) FILTER(WHERE id=1)<SUM(a) FILTER(WHERE id=2) FROM t",
            "Boolean(true)",
        ),
        (
            "filtered_avg_order",
            "SELECT AVG(a) FILTER(WHERE id=1)<AVG(a) FILTER(WHERE id=2) FROM t",
            "Boolean(true)",
        ),
        (
            "having_min_max",
            "SELECT COUNT(*) FROM(SELECT MIN(a) lo,MAX(a) hi FROM t WHERE id<=2 HAVING MIN(a)<MAX(a)) q",
            "Int64(1)",
        ),
        (
            "derived_aggregate_filter",
            "SELECT COUNT(*) FROM(SELECT MIN(a) lo,MAX(a) hi FROM t WHERE id<=2) q WHERE lo<hi",
            "Int64(1)",
        ),
        (
            "group_key_count",
            "SELECT COUNT(*) FROM(SELECT a,COUNT(*) n FROM t GROUP BY a) q",
            "Int64(5)",
        ),
        (
            "pair_group_key_count",
            "SELECT COUNT(*) FROM(SELECT a,b,COUNT(*) n FROM t GROUP BY a,b) q",
            "Int64(5)",
        ),
        (
            "rollup_count",
            "SELECT COUNT(*) FROM(SELECT a,COUNT(*) n FROM t GROUP BY ROLLUP(a)) q",
            "Int64(6)",
        ),
        (
            "rollup_pair_count",
            "SELECT COUNT(*) FROM(SELECT a,b,COUNT(*) n FROM t GROUP BY ROLLUP(a,b)) q",
            "Int64(11)",
        ),
        (
            "grouping_sets_count",
            "SELECT COUNT(*) FROM(SELECT a,b,COUNT(*) n FROM t GROUP BY GROUPING SETS((a,b),(a),(b),())) q",
            "Int64(16)",
        ),
        (
            "cube_count",
            "SELECT COUNT(*) FROM(SELECT a,b,COUNT(*) n FROM t GROUP BY CUBE(a,b)) q",
            "Int64(16)",
        ),
        (
            "grouped_counts",
            "SELECT SUM(n) FROM(SELECT a,b,COUNT(*) n FROM t GROUP BY GROUPING SETS((a,b),(a),(b),())) q",
            "Int64(28)",
        ),
        (
            "count_distinct_a",
            "SELECT COUNT(DISTINCT a) FROM t",
            "Int64(4)",
        ),
        (
            "count_distinct_b",
            "SELECT COUNT(DISTINCT b) FROM t",
            "Int64(4)",
        ),
        (
            "count_distinct_a_plus_b",
            "SELECT COUNT(DISTINCT a+b) FROM t",
            "Int64(4)",
        ),
        (
            "grouped_scaled_key_equal",
            "SELECT COUNT(*) FROM(SELECT a,b FROM t WHERE a IS NOT NULL GROUP BY a,b) q WHERE a=b",
            "Int64(4)",
        ),
        (
            "window_partition_count",
            "SELECT MAX(n) FROM(SELECT COUNT(*) OVER(PARTITION BY a,b) n FROM t) q",
            "Int64(2)",
        ),
        (
            "window_partition_total",
            "SELECT SUM(n) FROM(SELECT COUNT(*) OVER(PARTITION BY a) n FROM t) q",
            "Int64(11)",
        ),
        (
            "window_order_first",
            "SELECT id FROM(SELECT id,ROW_NUMBER() OVER(ORDER BY a ASC NULLS LAST) rn FROM t) q WHERE rn=1",
            "Int64(6)",
        ),
        (
            "window_dense_rank_256",
            "SELECT MAX(rn) FROM(SELECT a,b,DENSE_RANK() OVER(ORDER BY a ASC NULLS LAST) rn FROM t) q WHERE a=b",
            "UInt64(4)",
        ),
        (
            "window_fullframe_min",
            "SELECT MIN(x)=MIN(a) FROM(SELECT a,MIN(a) OVER(ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) x FROM t) q",
            "Boolean(true)",
        ),
        (
            "running_window_min_max_comparison",
            "SELECT lo<hi FROM(SELECT id,MIN(a) OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) lo,MAX(a) OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) hi FROM t WHERE id<=2) q WHERE id=2",
            "Boolean(true)",
        ),
        (
            "following_window_max_comparison",
            "SELECT a<hi FROM(SELECT id,a,MAX(a) OVER(ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) hi FROM t WHERE id<=2) q WHERE id=1",
            "Boolean(true)",
        ),
    ];
    let mut matched = Vec::new();
    let mut wrong = Vec::new();
    let mut excluded = Vec::new();
    for (name, sql, expected) in cases {
        let native = execute(true, sql).await;
        let arb = execute(false, sql).await;
        let expected = vec![expected.to_owned()];
        if native.as_ref() != Ok(&expected) {
            excluded.push(serde_json::json!({"name":name,"sql":sql,"reason":"native does not match independent expected value","native":native,"arb":arb,"expected":expected}));
        } else if arb.as_ref() == Ok(&expected) {
            matched.push(name);
        } else if arb.is_err() {
            excluded.push(serde_json::json!({"name":name,"sql":sql,"reason":"explicit decimal error, not silent result","native":native,"arb":arb,"expected":expected}));
        } else {
            wrong.push(serde_json::json!({"name":name,"sql":sql,"native":native,"arb":arb,"expected":expected}));
        }
    }
    eprintln!(
        "RELATIONAL_REPORT={}",
        serde_json::json!({"matched":matched,"wrong":wrong,"excluded":excluded})
    );
    assert!(
        wrong.is_empty(),
        "{} actual silent relational discrepancies",
        wrong.len()
    );
}
