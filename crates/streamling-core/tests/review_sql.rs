//! Confirm review findings through Streamling's actual supported-plan entry point.
use arrow::array::{BooleanArray, Int64Array, LargeBinaryArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};
use streamling_core::dynamic_table::DynamicTableRegistry;
use streamling_core::session::SessionManager;

fn session() -> SessionManager {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    let values = |name: &str, scale: u32, vals: &[&str]| {
        let mut builder =
            DecimalArbArrayBuilder::with_capacity(vals.len(), name, 110, scale).unwrap();
        for v in vals {
            builder.append_str(v).unwrap();
        }
        Arc::new(builder.finish().into_inner().0) as arrow::array::ArrayRef
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("v", 110, 2, false).unwrap(),
        DecimalArbType::field("q", 110, 0, false).unwrap(),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            values("v", 2, &["-1", "2"]),
            values("q", 0, &["1", "2"]),
        ],
    )
    .unwrap();
    sm.session_context().register_batch("t", batch).unwrap();
    sm
}

async fn query(sql: &str) -> Vec<RecordBatch> {
    let sm = session();
    let (plan, source) = sm
        .create_supported_logical_plan(sql.to_owned())
        .await
        .unwrap();
    assert_eq!(source, "t");
    eprintln!("SQL {sql}\nPLAN {}", plan.display_indent());
    sm.new_df(plan).collect().await.unwrap()
}

#[tokio::test]
async fn supported_case_filter_keeps_negative_value() {
    let batches = query("SELECT id FROM t WHERE (CASE WHEN id=1 THEN v ELSE to_decimal_arb_from_string('2',110,2) END) < to_decimal_arb_from_string('0',110,2)").await;
    let ids: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(ids, vec![1]);
}

#[tokio::test]
async fn supported_coalesce_filter_keeps_negative_value() {
    let batches = query("SELECT id FROM t WHERE coalesce(v,to_decimal_arb_from_string('0',110,2)) < to_decimal_arb_from_string('0',110,2)").await;
    let ids: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(ids, vec![1]);
}

#[tokio::test]
async fn supported_null_safe_equality_matches_native_equality() {
    let batches = query("SELECT q = to_decimal_arb_from_string('1',110,2) AS eq, q IS NOT DISTINCT FROM to_decimal_arb_from_string('1',110,2) AS ndeq FROM t").await;
    for b in batches {
        assert_eq!(b.column(0), b.column(1));
    }
}

#[tokio::test]
async fn supported_simple_case_uses_numeric_equality() {
    let batches = query("SELECT CASE q WHEN to_decimal_arb_from_string('1',110,2) THEN 10 ELSE 20 END AS result FROM t").await;
    let actual = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(actual.values().as_ref(), &[10, 20]);
}

#[tokio::test]
async fn supported_nullif_uses_numeric_equality() {
    let batches =
        query("SELECT nullif(q,to_decimal_arb_from_string('1',110,2)) IS NULL FROM t").await;
    let actual = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        actual.iter().collect::<Vec<_>>(),
        vec![Some(true), Some(false)]
    );
}

#[tokio::test]
async fn supported_greatest_uses_numeric_order() {
    let batches =
        query("SELECT greatest(v,to_decimal_arb_from_string('0',110,2)) AS v FROM t").await;
    let a = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let actual = DecimalArbValue::from_canonical_bytes_at_scale(a.value(0), 2).unwrap();
    assert_eq!(actual.to_canonical_string(), "0");
}

#[tokio::test]
async fn supported_in_with_null_uses_numeric_equality() {
    let batches = query("SELECT q IN (to_decimal_arb_from_string('1',110,2), NULL) FROM t").await;
    let actual = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(actual.iter().collect::<Vec<_>>(), vec![Some(true), None]);
}

#[tokio::test]
#[ignore = "Pre-existing unquoted numeric literal conversion through Float64"]
async fn wide_numeric_literal_cast_preserves_all_digits() {
    let batches =
        query("SELECT CAST(123456789012345678901234567890123456789 AS DECIMAL(110,0)) AS v FROM t")
            .await;
    let a = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let actual = DecimalArbValue::from_canonical_bytes_at_scale(a.value(0), 0).unwrap();
    assert_eq!(
        actual.to_canonical_string(),
        "123456789012345678901234567890123456789"
    );
}

#[tokio::test]
#[ignore = "Diagnostic only: prints cast outcomes without correctness assertions"]
async fn supported_decimal_and_text_cast_probe() {
    let sm = session();
    for sql in [
        "SELECT CAST(v AS DECIMAL(38,18)) AS result FROM t",
        "SELECT CAST(v AS DECIMAL(100,18)) AS result FROM t",
        "SELECT CAST(v + to_decimal_arb_from_string('0',110,2) AS DECIMAL(100,18)) AS result FROM t",
        "SELECT CAST(CASE WHEN id=1 THEN v ELSE to_decimal_arb_from_string('2',110,2) END AS TEXT) AS result FROM t",
        "SELECT CAST(coalesce(v,to_decimal_arb_from_string('0',110,2)) AS TEXT) AS result FROM t",
    ] {
        let result = match sm.create_supported_logical_plan(sql.to_owned()).await {
            Ok((plan, _)) => sm.new_df(plan).collect().await,
            Err(error) => Err(error),
        };
        eprintln!("CAST PROBE {sql}\n{result:?}");
        if let Ok(batches) = result {
            for batch in batches {
                eprintln!("RESULT SCHEMA {:?}", batch.schema());
            }
        }
    }
}

#[tokio::test]
#[ignore = "Diagnostic only: intermediate Avro values do not prove wire corruption; planned field is bytes"]
async fn supported_union_avro_sink_preserves_each_numeric_value() {
    use streamling_common::formats::FromArrowConverter;
    use streamling_common::formats::avro::{FromArrowToAvroConverter, to_avro};
    let sm = session();
    let sql = "SELECT q AS value FROM t WHERE id=1 UNION ALL SELECT to_decimal_arb_from_string('1',110,2) AS value FROM t WHERE id=1";
    let (plan, _) = sm
        .create_supported_logical_plan(sql.to_owned())
        .await
        .unwrap();
    let output_schema = Arc::new(plan.schema().as_arrow().clone());
    eprintln!(
        "UNION PLAN {}\nDECLARED SCHEMA {:?}\nAVRO {:?}",
        plan.display_indent(),
        output_schema,
        to_avro("Test", output_schema.fields())
    );
    let converter = FromArrowToAvroConverter::new(output_schema, "Test".into());
    let batches = sm.new_df(plan).collect().await.unwrap();
    let records: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            eprintln!("UNION INPUT SCHEMA {:?}", b.schema());
            converter.convert_from_batch(b).unwrap()
        })
        .collect();
    eprintln!("UNION AVRO RECORDS {records:?}");
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0], records[1],
        "Both UNION branches hold numeric 1 and share one fixed Avro writer schema; their serialized decimals must match"
    );
}

#[tokio::test]
async fn supported_text_cast_after_case_preserves_numeric_text() {
    let batches =
        query("SELECT CAST(CASE WHEN id=1 THEN q ELSE q END AS VARCHAR) AS result FROM t").await;
    let actual = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .unwrap();
            a.iter().map(|s| s.unwrap().to_owned()).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec!["1", "2"]);
}

#[tokio::test]
async fn supported_text_cast_after_coalesce_preserves_numeric_text() {
    let batches = query(
        "SELECT CAST(coalesce(q,to_decimal_arb_from_string('0',110,0)) AS VARCHAR) AS result FROM t",
    )
    .await;
    let actual = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .unwrap();
            a.iter().map(|s| s.unwrap().to_owned()).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec!["1", "2"]);
}

#[tokio::test]
async fn supported_positive_greatest_preserves_numeric_order() {
    let batches=query("SELECT greatest(to_decimal_arb_from_int(id+254,78,0),to_decimal_arb_from_string('256',78,0)) AS result FROM t").await;
    let actual = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            (0..b.num_rows())
                .map(|r| {
                    DecimalArbValue::from_canonical_bytes_at_scale(a.value(r), 0)
                        .unwrap()
                        .to_canonical_string()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec!["256", "256"]);
}

#[tokio::test]
async fn supported_positive_array_min_preserves_numeric_order() {
    let batches=query("SELECT array_min([to_decimal_arb_from_int(id+254,78,0),to_decimal_arb_from_string('256',78,0)]) AS result FROM t").await;
    let actual = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            (0..b.num_rows())
                .map(|r| {
                    DecimalArbValue::from_canonical_bytes_at_scale(a.value(r), 0)
                        .unwrap()
                        .to_canonical_string()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec!["255", "256"]);
}

#[tokio::test]
async fn supported_positive_case_comparison_preserves_numeric_order() {
    let batches=query("SELECT (CASE WHEN id=1 THEN to_decimal_arb_from_int(id+254,78,0) ELSE to_decimal_arb_from_string('0',78,0) END) < to_decimal_arb_from_string('256',78,0) AS result FROM t").await;
    let actual = batches
        .iter()
        .flat_map(|b| {
            let a = b.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
            a.iter().collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![Some(true), Some(true)]);
}
