//! Numerical compatibility checks with production SQL topology wrappers.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::{MemTable, ViewTable, provider_as_source},
    logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, dml::InsertOp},
    physical_plan::displayable,
};
use std::{sync::Arc, time::Duration};
use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};
use streamling_core::{
    dynamic_table::DynamicTableRegistry,
    operators::{
        checkpointable::CheckpointableNode,
        wrapping::{WrappingNode, WrappingSourceTableProvider},
    },
    session::SessionManager,
};
fn source(native: bool, membership: bool) -> MemTable {
    let values = if membership {
        vec![Some(255i128), Some(256), Some(-1), None]
    } else {
        vec![Some(1), Some(1), Some(3), None]
    };
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))];
    for (name, scale, values) in [
        ("a", 0, values),
        ("b", 2, vec![Some(25600), Some(25500), Some(0), Some(100)]),
    ] {
        if native {
            fields.push(Field::new(
                name,
                DataType::Decimal128(30, scale as i8),
                true,
            ));
            columns.push(Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(30, scale as i8)
                    .unwrap(),
            ));
        } else {
            fields.push(DecimalArbType::field(name, 100, scale, true).unwrap());
            let mut builder = DecimalArbArrayBuilder::with_capacity(4, name, 100, scale).unwrap();
            for v in values {
                match v {
                    Some(v) => builder
                        .append_value(&DecimalArbValue::from_bigint_and_scale(
                            v.into(),
                            scale as i64,
                        ))
                        .unwrap(),
                    None => builder.append_null(),
                }
            }
            columns.push(Arc::new(builder.finish().into_inner().0));
        }
    }
    fields.push(Field::new("_gs_op", DataType::Utf8, false));
    columns.push(Arc::new(StringArray::from(vec!["i", "i", "i", "i"])));
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    // Two batches prove the aggregate survives the production per-batch wrapper.
    MemTable::try_new(schema, vec![vec![batch.slice(0, 2), batch.slice(2, 2)]]).unwrap()
}
async fn topology(native: bool, membership: bool, sql: &str) -> Vec<RecordBatch> {
    let sm = SessionManager::new(100, 10, DynamicTableRegistry::new(), 1).unwrap();
    let ctx = sm.session_context();
    let provider = WrappingSourceTableProvider::new(
        Arc::new(source(native, membership)),
        format!("deep_v3_source_{}", uuid::Uuid::new_v4()),
        None,
        None,
    );
    ctx.register_table("t", Arc::new(provider)).unwrap();
    let (sql_plan, name) = sm.create_supported_logical_plan(sql.into()).await.unwrap();
    assert_eq!(name, "t");
    let checkpoint = LogicalPlan::Extension(Extension {
        node: Arc::new(CheckpointableNode::new(sql_plan, 10, "deep_v3_sql".into())),
    });
    let wrapped = LogicalPlan::Extension(Extension {
        node: Arc::new(WrappingNode::new_with_non_null_cols(
            checkpoint,
            format!("deep_v3_transform_{}", uuid::Uuid::new_v4()),
            false,
            vec!["id".into()],
            None,
        )),
    });
    ctx.register_table("v3_view", Arc::new(ViewTable::new(wrapped, None)))
        .unwrap();
    let view = ctx.table("v3_view").await.unwrap().into_unoptimized_plan();
    let target = Arc::new(MemTable::try_new(view.schema().inner().clone(), vec![vec![]]).unwrap());
    ctx.register_table("target", target.clone()).unwrap();
    let insert = LogicalPlanBuilder::insert_into(
        view,
        "target",
        provider_as_source(target),
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap();
    let df = sm.new_df(insert);
    let physical = df.create_physical_plan().await.unwrap();
    let displayed = displayable(physical.as_ref()).indent(true).to_string();
    eprintln!("TOPOLOGY native={native} SQL={sql}\n{displayed}");
    assert!(displayed.contains("CheckpointableExec"));
    fn has_wrapper(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> bool {
        plan.downcast_ref::<streamling_core::operators::wrapping::WrappingExec>()
            .is_some()
            || plan.children().iter().any(|p| has_wrapper(p))
    }
    assert!(
        has_wrapper(&physical),
        "production telemetry wrapper must remain present"
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        datafusion::physical_plan::collect(physical, Arc::new(df.task_ctx())),
    )
    .await
    .expect("bounded topology must drain")
    .unwrap();
    let rows = ctx.table("target").await.unwrap().collect().await.unwrap();
    eprintln!("SINK_ROWS native={native}: {rows:?}");
    rows
}
fn first_number(batches: &[RecordBatch], name: &str, scale: u32) -> String {
    let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let a = b.column_by_name(name).unwrap();
    if let Some(a) = a.as_any().downcast_ref::<LargeBinaryArray>() {
        DecimalArbValue::from_canonical_bytes_at_scale(a.value(0), scale)
            .unwrap()
            .as_bigdecimal()
            .normalized()
            .to_string()
    } else if let Some(a) = a.as_any().downcast_ref::<Decimal128Array>() {
        DecimalArbValue::from_bigint_and_scale(a.value(0).into(), a.scale() as i64)
            .as_bigdecimal()
            .normalized()
            .to_string()
    } else {
        panic!("unexpected number type {:?}", a.data_type())
    }
}
async fn check_aggregate(native: bool, fun: &str) {
    let sql = format!(
        "SELECT id,(SELECT {fun}(DISTINCT a) FROM t HAVING COUNT(*)>0) AS value FROM t WHERE id=1"
    );
    let rows = topology(native, false, &sql).await;
    assert_eq!(
        first_number(&rows, "value", u32::from(fun == "AVG")),
        if fun == "SUM" { "4" } else { "2" }
    );
}
#[tokio::test]
async fn wrapped_sum_distinct_numeric() {
    check_aggregate(false, "SUM").await;
}
#[tokio::test]
async fn wrapped_avg_distinct_numeric() {
    check_aggregate(false, "AVG").await;
}
#[tokio::test]
async fn wrapped_sum_distinct_native_control() {
    check_aggregate(true, "SUM").await;
}
#[tokio::test]
async fn wrapped_avg_distinct_native_control() {
    check_aggregate(true, "AVG").await;
}
async fn check_membership(native: bool) {
    let rows = topology(
        native,
        true,
        "SELECT id FROM t WHERE a NOT IN(SELECT b FROM t)",
    )
    .await;
    let mut ids = rows
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![3]);
}
#[tokio::test]
async fn wrapped_not_in_numeric() {
    check_membership(false).await;
}
#[tokio::test]
async fn wrapped_not_in_native_control() {
    check_membership(true).await;
}

async fn membership_gate(native: bool, rhs: &str, expected: &[i64]) {
    let rows = topology(
        native,
        true,
        &format!("SELECT id FROM t WHERE a NOT IN({rhs})"),
    )
    .await;
    let mut ids = rows
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    eprintln!("MEMBERSHIP_GATE native={native} rhs={rhs}: actual={ids:?} expected={expected:?}");
    assert_eq!(ids, expected);
}
#[tokio::test]
async fn wrapped_not_in_empty_gate() {
    for native in [true, false] {
        membership_gate(native, "SELECT b FROM t WHERE FALSE", &[1, 2, 3, 4]).await;
    }
}
#[tokio::test]
async fn wrapped_not_in_null_gate() {
    for native in [true, false] {
        membership_gate(native, "SELECT NULL FROM t", &[]).await;
    }
}
#[tokio::test]
async fn wrapped_not_in_fractional_false_collision() {
    membership_gate(
        true,
        "SELECT CAST('2.55' AS DECIMAL(30,2)) FROM t",
        &[1, 2, 3],
    )
    .await;
    membership_gate(
        false,
        "SELECT to_decimal_arb_from_string('2.55',100,2) FROM t",
        &[1, 2, 3],
    )
    .await;
}

async fn check_aggregate_text(native: bool, fun: &str) {
    let subquery = format!("(SELECT {fun}(DISTINCT a) FROM t HAVING COUNT(*)>0)");
    let expr = if native {
        format!("CAST({subquery} AS VARCHAR)")
    } else {
        format!(
            "decimal_arb_to_string(decimal_arb_with_meta({subquery},116,{}))",
            u32::from(fun == "AVG")
        )
    };
    let rows = topology(
        native,
        false,
        &format!("SELECT id,{expr} AS value FROM t WHERE id=1"),
    )
    .await;
    let b = rows.iter().find(|b| b.num_rows() > 0).unwrap();
    let text =
        arrow::util::display::array_value_to_string(b.column_by_name("value").unwrap().as_ref(), 0)
            .unwrap();
    eprintln!("NUMERIC_TEXT native={native} {fun}: {text}");
    assert_eq!(
        text.parse::<DecimalArbValue>().unwrap(),
        if fun == "SUM" { "4" } else { "2" }
            .parse::<DecimalArbValue>()
            .unwrap()
    );
}
#[tokio::test]
#[ignore = "Diagnostic excluded: decimal_arb_with_meta is internal and not registered as a SQL function"]
async fn wrapped_sum_distinct_text_consumer() {
    check_aggregate_text(false, "SUM").await;
}
#[tokio::test]
#[ignore = "Diagnostic excluded: decimal_arb_with_meta is internal and not registered as a SQL function"]
async fn wrapped_avg_distinct_text_consumer() {
    check_aggregate_text(false, "AVG").await;
}
#[tokio::test]
async fn wrapped_native_aggregate_text_controls() {
    for fun in ["SUM", "AVG"] {
        check_aggregate_text(true, fun).await;
    }
}

#[tokio::test]
async fn wrapped_wide_literal_text_reachability() {
    let rows=topology(false,false,"SELECT id,decimal_arb_to_string(CAST(18446744073709551617 AS DECIMAL(77,0))) AS value FROM t WHERE id=1").await;
    let b = rows.iter().find(|b| b.num_rows() > 0).unwrap();
    assert_eq!(
        arrow::util::display::array_value_to_string(b.column_by_name("value").unwrap().as_ref(), 0)
            .unwrap(),
        "18446744073709551617"
    );
}
#[tokio::test]
async fn wrapped_quoted_wide_literal_text_control() {
    let rows=topology(false,false,"SELECT id,decimal_arb_to_string(CAST('18446744073709551617' AS DECIMAL(77,0))) AS value FROM t WHERE id=1").await;
    let b = rows.iter().find(|b| b.num_rows() > 0).unwrap();
    assert_eq!(
        arrow::util::display::array_value_to_string(b.column_by_name("value").unwrap().as_ref(), 0)
            .unwrap(),
        "18446744073709551617"
    );
}

fn original_or_wrapped_context(wrapped: bool) -> datafusion::prelude::SessionContext {
    use streamling_common::functions::decimal_arb_aggregates::{
        DecimalArbAvgUdaf, DecimalArbExtremeUdaf, DecimalArbSumUdaf,
    };
    // Identical stock DataFusion optimizer/settings isolate only the four UDAF replacements.
    let ctx = datafusion::prelude::SessionContext::new_with_config(
        datafusion::prelude::SessionConfig::new().with_target_partitions(1),
    );
    if wrapped {
        ctx.register_udaf(DecimalArbSumUdaf::into_udaf());
        ctx.register_udaf(DecimalArbAvgUdaf::into_udaf());
        ctx.register_udaf(DecimalArbExtremeUdaf::min_udaf());
        ctx.register_udaf(DecimalArbExtremeUdaf::max_udaf());
    }
    ctx
}
async fn native_snapshot(
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Result<serde_json::Value, String> {
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let fields = df
        .schema()
        .fields()
        .iter()
        .map(|f| format!("{}:{:?}:{}", f.name(), f.data_type(), f.is_nullable()))
        .collect::<Vec<_>>();
    let batches = df.collect().await.map_err(|e| e.to_string())?;
    let mut rows = vec![];
    for b in &batches {
        for i in 0..b.num_rows() {
            let mut row = vec![];
            for a in b.columns() {
                row.push(format!(
                    "{:?}",
                    datafusion::common::ScalarValue::try_from_array(a, i)
                        .map_err(|e| e.to_string())?
                ));
            }
            rows.push(row);
        }
    }
    Ok(serde_json::json!({"fields":fields,"rows":rows}))
}
fn compatibility_cases() -> Vec<(String, ArrayRef)> {
    use arrow::datatypes::{Int8Type, Int16Type, TimeUnit};
    let mut cases = vec![];
    let types = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal32(9, 2),
        DataType::Decimal64(18, 4),
        DataType::Decimal128(38, 8),
        DataType::Decimal256(76, 18),
    ];
    for dt in types {
        let negative = if dt.is_unsigned_integer() { "2" } else { "-2" };
        let strings = StringArray::from(vec![
            None,
            Some("1"),
            Some("1"),
            Some("3"),
            Some(negative),
            Some("0"),
            None,
        ]);
        cases.push((
            format!("{dt:?}"),
            arrow::compute::cast(&strings, &dt).unwrap(),
        ));
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        let dt = DataType::Duration(unit);
        cases.push((
            format!("{dt:?}"),
            arrow::compute::cast(
                &Int64Array::from(vec![
                    None,
                    Some(1),
                    Some(1),
                    Some(3),
                    Some(-2),
                    Some(0),
                    None,
                ]),
                &dt,
            )
            .unwrap(),
        ));
    }
    cases.push(("Null".into(), Arc::new(NullArray::new(7))));
    cases.push((
        "Float64-special".into(),
        Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(-0.0),
            Some(0.0),
            None,
            Some(1.0),
        ])),
    ));
    cases.push((
        "UInt64-large".into(),
        Arc::new(UInt64Array::from(vec![
            Some(9_007_199_254_740_993),
            Some(9_007_199_254_740_993),
            Some(1),
            Some(0),
            None,
            Some(2),
            Some(3),
        ])),
    ));
    for dt in [
        DataType::Int64,
        DataType::Float64,
        DataType::Decimal128(20, 2),
        DataType::Decimal256(50, 10),
    ] {
        let values = arrow::compute::cast(&StringArray::from(vec!["1", "3", "-2"]), &dt).unwrap();
        let dict = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![
                None,
                Some(0),
                Some(0),
                Some(1),
                Some(2),
                Some(1),
                None,
            ]),
            values,
        )
        .unwrap();
        cases.push((format!("Dictionary({dt:?})"), Arc::new(dict)));
        let values = arrow::compute::cast(
            &StringArray::from(vec![None, Some("1"), Some("-2"), Some("3")]),
            &dt,
        )
        .unwrap();
        let run =
            RunArray::<Int16Type>::try_new(&Int16Array::from(vec![1, 3, 5, 7]), values.as_ref())
                .unwrap();
        cases.push((format!("RunEndEncoded({dt:?})"), Arc::new(run)));
    }
    cases.push((
        "Utf8".into(),
        Arc::new(StringArray::from(vec![
            None,
            Some("255"),
            Some("256"),
            Some("-1"),
            Some("0"),
            Some("1"),
            None,
        ])),
    ));
    cases.push((
        "LargeBinary".into(),
        Arc::new(LargeBinaryArray::from(vec![
            None,
            Some(&b"255"[..]),
            Some(&b"256"[..]),
            Some(&b"-1"[..]),
            Some(&b"0"[..]),
            Some(&b"1"[..]),
            None,
        ])),
    ));
    cases
}
#[tokio::test]
async fn ordinary_aggregate_wrapper_compatibility_matrix() {
    let mut matched = 0;
    let mut both_rejected = vec![];
    let mut wrong = vec![];
    let mut unexpected_errors = vec![];
    let queries = [
        "SELECT sum(v) AS s,avg(v) AS a,min(v) AS n,max(v) AS x FROM input",
        "SELECT sum(DISTINCT v) AS s,avg(DISTINCT v) AS a,min(DISTINCT v) AS n,max(DISTINCT v) AS x,count(*) AS c FROM input",
        "SELECT g,sum(v) AS s,avg(v) AS a,min(v) AS n,max(v) AS x FROM input GROUP BY g ORDER BY g",
        "SELECT sum(v) FILTER(WHERE id<>3) AS s,avg(v) FILTER(WHERE id%2=0) AS a,min(v) FILTER(WHERE id>1) AS n,max(v) FILTER(WHERE id<7) AS x FROM input",
        "SELECT sum(v) AS s,avg(v) AS a,min(v) AS n,max(v) AS x FROM input WHERE id<0",
        "SELECT sum(v) AS s,avg(v) AS a,min(v) AS n,max(v) AS x FROM input WHERE id IN(1,7)",
        "SELECT id,sum(v) OVER(ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s,avg(v) OVER(ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a,min(v) OVER(ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS n,max(v) OVER(ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS x FROM input ORDER BY id",
        "SELECT min(v) AS n,max(v) AS x FROM input",
    ];
    for (label, values) in compatibility_cases() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("g", DataType::Int64, false),
            Field::new("v", values.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7])),
                Arc::new(Int64Array::from(vec![0, 1, 0, 1, 0, 1, 0])),
                values,
            ],
        )
        .unwrap();
        let original = original_or_wrapped_context(false);
        let wrapped = original_or_wrapped_context(true);
        for ctx in [&original, &wrapped] {
            ctx.register_table(
                "input",
                Arc::new(
                    MemTable::try_new(
                        schema.clone(),
                        vec![vec![batch.slice(0, 3), batch.slice(3, 4)]],
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        }
        for sql in queries {
            let a = native_snapshot(&original, sql).await;
            let b = native_snapshot(&wrapped, sql).await;
            match (a, b) {
                (Ok(a), Ok(b)) if a == b => matched += 1,
                (Ok(a), Ok(b)) => {
                    wrong.push(serde_json::json!({"type":label,"sql":sql,"original":a,"wrapped":b}))
                }
                (Err(a), Err(b)) => both_rejected
                    .push(serde_json::json!({"type":label,"sql":sql,"original":a,"wrapped":b})),
                (a, b) => unexpected_errors
                    .push(serde_json::json!({"type":label,"sql":sql,"original":a,"wrapped":b})),
            }
        }
    }
    eprintln!(
        "NATIVE_WRAPPER_REPORT={}",
        serde_json::json!({"matched":matched,"both_rejected":both_rejected,"wrong":wrong,"unexpected_errors":unexpected_errors})
    );
    assert!(
        wrong.is_empty() && unexpected_errors.is_empty(),
        "{} mismatches,{} one-sided errors",
        wrong.len(),
        unexpected_errors.len()
    );
}

async fn check_aggregate_boolean(native: bool, fun: &str) {
    let expected = if fun == "SUM" { "4" } else { "2" };
    let scale = u32::from(fun == "AVG");
    let rhs = if native {
        format!("CAST('{expected}' AS DECIMAL(30,{scale}))")
    } else {
        format!("to_decimal_arb_from_string('{expected}',100,{scale})")
    };
    let sql = format!(
        "SELECT id,(SELECT {fun}(DISTINCT a) FROM t HAVING COUNT(*)>0)={rhs} AS value FROM t WHERE id=1"
    );
    let rows = topology(native, false, &sql).await;
    let b = rows.iter().find(|b| b.num_rows() > 0).unwrap();
    let a = b
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    eprintln!(
        "AGGREGATE_BOOLEAN native={native} {fun}: null={} value={}",
        a.is_null(0),
        a.value(0)
    );
    assert!(!a.is_null(0) && a.value(0));
}
#[tokio::test]
async fn wrapped_sum_distinct_boolean_consumer() {
    check_aggregate_boolean(false, "SUM").await;
}
#[tokio::test]
async fn wrapped_avg_distinct_boolean_consumer() {
    check_aggregate_boolean(false, "AVG").await;
}
#[tokio::test]
async fn wrapped_native_aggregate_boolean_controls() {
    for fun in ["SUM", "AVG"] {
        check_aggregate_boolean(true, fun).await;
    }
}
