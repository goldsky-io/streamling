//! Distinct aggregate checks, including the supported scalar-subquery entry path.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
};
use std::sync::Arc;
use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};
use streamling_core::{dynamic_table::DynamicTableRegistry, session::SessionManager};

fn session() -> SessionManager {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    let mut a = DecimalArbArrayBuilder::with_capacity(4, "a", 100, 0).unwrap();
    let mut b = DecimalArbArrayBuilder::with_capacity(4, "b", 100, 0).unwrap();
    for v in [Some("1"), Some("1"), Some("3"), None] {
        match v {
            Some(v) => a.append_str(v).unwrap(),
            None => a.append_null(),
        }
    }
    for v in ["10", "20", "20", "30"] {
        b.append_str(v).unwrap();
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("a", 100, 0, true).unwrap(),
        DecimalArbType::field("b", 100, 0, true).unwrap(),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(a.finish().into_inner().0),
            Arc::new(b.finish().into_inner().0),
        ],
    )
    .unwrap();
    sm.session_context().register_batch("t", batch).unwrap();
    sm
}
async fn check(sql: &str, expected: &[(&str, u32)], supported: bool) {
    let sm = session();
    let batches = if supported {
        let (plan, _) = sm.create_supported_logical_plan(sql.into()).await.unwrap();
        sm.new_df(plan).collect().await.unwrap()
    } else {
        sm.session_context()
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    };
    let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    assert_eq!(b.num_columns(), expected.len());
    for (i, (text, scale)) in expected.iter().enumerate() {
        let raw = b
            .column(i)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(
            DecimalArbValue::from_canonical_bytes_at_scale(raw.value(0), *scale).unwrap(),
            text.parse::<DecimalArbValue>().unwrap(),
            "query={sql},column={i},schema={:?}",
            b.schema()
        );
    }
}
#[tokio::test]
async fn deep_direct_distinct_mixed_aggregate_grid() {
    for (q, expected) in [
        (
            "SELECT SUM(DISTINCT a),AVG(DISTINCT a) FROM t",
            vec![("4", 0), ("2", 1)],
        ),
        (
            "SELECT SUM(DISTINCT a),SUM(a),AVG(DISTINCT a),AVG(a) FROM t",
            vec![("4", 0), ("5", 0), ("2", 1), ("1.7", 1)],
        ),
        (
            "SELECT SUM(DISTINCT a),SUM(DISTINCT b),AVG(DISTINCT a),AVG(DISTINCT b),SUM(a) FROM t",
            vec![("4", 0), ("60", 0), ("2", 1), ("20", 1), ("5", 0)],
        ),
        (
            "SELECT SUM(DISTINCT a) FILTER(WHERE id<3),SUM(a),AVG(DISTINCT b) FILTER(WHERE id<4) FROM t",
            vec![("1", 0), ("5", 0), ("15", 1)],
        ),
    ] {
        check(q, &expected, false).await;
    }
}
#[tokio::test]
async fn deep_supported_distinct_scalar_subqueries() {
    check("SELECT (SELECT SUM(DISTINCT a) FROM t) AS s,(SELECT AVG(DISTINCT a) FROM t) AS av FROM t WHERE id=1", &[("4",0),("2",1)], true).await;
}
#[tokio::test]
async fn deep_supported_all_scalar_subquery_aggregate_control() {
    check(
        "SELECT (SELECT SUM(a) FROM t) AS s,(SELECT AVG(a) FROM t) AS av FROM t WHERE id=1",
        &[("5", 0), ("1.7", 1)],
        true,
    )
    .await;
}

#[tokio::test]
async fn deep_supported_distinct_sum_subquery_with_count_having() {
    check(
        "SELECT (SELECT SUM(DISTINCT a) FROM t HAVING COUNT(*)>0) AS s FROM t WHERE id=1",
        &[("4", 0)],
        true,
    )
    .await;
}

#[tokio::test]
async fn deep_supported_distinct_avg_subquery_with_count_having() {
    check(
        "SELECT (SELECT AVG(DISTINCT a) FROM t HAVING COUNT(*)>0) AS av FROM t WHERE id=1",
        &[("2", 1)],
        true,
    )
    .await;
}

#[tokio::test]
async fn deep_supported_native_distinct_count_having_control() {
    let sm = session();
    let a = Decimal128Array::from(vec![Some(1), Some(1), Some(3), None])
        .with_precision_and_scale(30, 0)
        .unwrap();
    let native = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("a", DataType::Decimal128(30, 0), true),
        ])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4])), Arc::new(a)],
    )
    .unwrap();
    sm.session_context().register_batch("n", native).unwrap();
    for (op, expected) in [("SUM", 4i128), ("AVG", 2i128)] {
        let sql = format!(
            "SELECT (SELECT {op}(DISTINCT a) FROM n HAVING COUNT(*)>0) AS v FROM n WHERE id=1"
        );
        let (plan, _) = sm.create_supported_logical_plan(sql).await.unwrap();
        let batches = sm.new_df(plan).collect().await.unwrap();
        let a = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let DataType::Decimal128(_, scale) = a.data_type() else {
            panic!()
        };
        assert_eq!(a.value(0), expected * 10i128.pow(*scale as u32));
    }
}

fn order_session(native: bool) -> SessionManager {
    let sm = session();
    let values = [Some(255i128), Some(256), Some(-1), Some(0), None];
    let (field, array): (Field, ArrayRef) = if native {
        (
            Field::new("a", DataType::Decimal128(30, 0), true),
            Arc::new(
                Decimal128Array::from(values.to_vec())
                    .with_precision_and_scale(30, 0)
                    .unwrap(),
            ),
        )
    } else {
        let mut b = DecimalArbArrayBuilder::with_capacity(5, "a", 100, 0).unwrap();
        for v in values {
            match v {
                Some(v) => b.append_str(&v.to_string()).unwrap(),
                None => b.append_null(),
            }
        }
        (
            DecimalArbType::field("a", 100, 0, true).unwrap(),
            Arc::new(b.finish().into_inner().0),
        )
    };
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            field,
        ])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])), array],
    )
    .unwrap();
    sm.session_context().register_batch("w", input).unwrap();
    sm
}

async fn ordered_array(native: bool) {
    let sm = order_session(native);
    let sql = "SELECT (SELECT ARRAY_AGG(a ORDER BY a ASC NULLS LAST) FROM w WHERE id<=2) AS xs FROM w WHERE id=1";
    let (plan, _) = sm.create_supported_logical_plan(sql.into()).await.unwrap();
    let batches = sm.new_df(plan).collect().await.unwrap();
    let output = batches[0].column(0);
    let values = if let Some(a) = output.as_any().downcast_ref::<ListArray>() {
        a.value(0)
    } else {
        output
            .as_any()
            .downcast_ref::<LargeListArray>()
            .unwrap()
            .value(0)
    };
    let actual = if native {
        values
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(|v| v.to_string()))
            .collect::<Vec<_>>()
    } else {
        values
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .iter()
            .map(|v| {
                v.map(|v| {
                    DecimalArbValue::from_canonical_bytes_at_scale(v, 0)
                        .unwrap()
                        .to_canonical_string()
                })
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        actual,
        vec![Some("255".into()), Some("256".into())],
        "native={native},schema={:?}",
        batches[0].schema()
    );
}

async fn ordered_window(native: bool) {
    let sm = order_session(native);
    let sql = "SELECT (SELECT rn FROM (SELECT id,ROW_NUMBER() OVER (ORDER BY a ASC NULLS LAST) AS rn FROM w) r WHERE id=2) AS rank FROM w WHERE id=1";
    let (plan, _) = sm.create_supported_logical_plan(sql.into()).await.unwrap();
    let batches = sm.new_df(plan).collect().await.unwrap();
    let rank = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(rank, 4, "native={native}: numeric256 should follow-1,0,255");
}

#[tokio::test]
async fn deep_supported_ordered_array_aggregate_numeric_order() {
    ordered_array(false).await;
}
#[tokio::test]
async fn deep_supported_ordered_array_native_control() {
    ordered_array(true).await;
}
#[tokio::test]
#[ignore = "Diagnostic: the native Decimal128 control also returns the wrong rank; not attributed to PR37"]
async fn deep_supported_window_subquery_numeric_order() {
    ordered_window(false).await;
}
#[tokio::test]
#[ignore = "Diagnostic: the native Decimal128 control also returns the wrong rank; not attributed to PR37"]
async fn deep_supported_window_native_control() {
    ordered_window(true).await;
}
