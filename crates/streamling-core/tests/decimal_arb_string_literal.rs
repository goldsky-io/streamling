//! String-literal comparisons against decimal_arb through Streamling's
//! supported-plan entry point. `WHERE amount > '1000…'` used to fall back to
//! DataFusion's `Utf8 → LargeBinary` coercion and compare UTF-8 bytes with the
//! canonical encoding, so the filter silently dropped every row.
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
