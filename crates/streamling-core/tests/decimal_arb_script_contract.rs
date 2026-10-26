//! Adversarial PR37 tests through the actual embedded WASM runtime.
use arrow::array::{BooleanArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::{
    execution::SessionStateBuilder,
    logical_expr::{Extension, LogicalPlan},
    prelude::SessionContext,
};
use std::{collections::BTreeMap, sync::Arc};
use streamling_common::{
    formats::avro::{serialize, to_avro},
    types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType},
};
use streamling_core::operators::{planner::StreamlingQueryPlanner, wasm_runner::WasmRunnerNode};

async fn run(script: &str, output_schema: Option<BTreeMap<String, String>>) -> Vec<RecordBatch> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);
    let schema = Arc::new(Schema::new(vec![
        DecimalArbType::field("amount", 78, 0, true).unwrap(),
        Field::new("_gs_op", DataType::Utf8, false),
    ]));
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "amount", 78, 0).unwrap();
    b.append_str("0").unwrap();
    let (raw, _, _) = b.finish().into_inner();
    let input = RecordBatch::try_new(
        schema,
        vec![Arc::new(raw), Arc::new(StringArray::from(vec!["i"]))],
    )
    .unwrap();
    ctx.register_batch("t", input).unwrap();
    let node = WasmRunnerNode::new(
        ctx.table("t").await.unwrap().into_optimized_plan().unwrap(),
        "javascript".into(),
        script.into(),
        None,
        1000,
        output_schema,
    );
    ctx.execute_logical_plan(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
    .await
    .unwrap()
    .collect()
    .await
    .unwrap()
}
#[tokio::test]
async fn wasm_zero_comparison_preserves_existing_string_contract() {
    let output = run(
        "row => ({is_zero: row.amount === '0'})",
        Some(BTreeMap::from([("is_zero".into(), "boolean".into())])),
    )
    .await;
    let flag = output[0]
        .column_by_name("is_zero")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(
        flag.value(0),
        "Input zero was exposed as opaque binary instead of the previous decimal string"
    );
}
fn assert_avro_decimal_value(output: &RecordBatch, text: &str) {
    let mut builder = DecimalArbArrayBuilder::with_capacity(1, "amount", 78, 0).unwrap();
    builder.append_str(text).unwrap();
    let (raw, _, _) = builder.finish().into_inner();
    let mut expected_columns = output.columns().to_vec();
    expected_columns[output.schema().index_of("amount").unwrap()] = Arc::new(raw);
    let expected = RecordBatch::try_new(output.schema(), expected_columns).unwrap();
    let schema = to_avro("Test", output.schema().fields());
    assert_eq!(serialize(&schema, output), serialize(&schema, &expected));
}
#[tokio::test]
async fn wasm_string_100_roundtrips_to_avro() {
    let output = run("row => ({amount: '100'})", None).await;
    assert_avro_decimal_value(&output[0], "100");
}
#[tokio::test]
async fn wasm_decimal_identity_preserves_value() {
    let output = run("row => ({amount: row.amount})", None).await;
    assert_avro_decimal_value(&output[0], "0");
}
