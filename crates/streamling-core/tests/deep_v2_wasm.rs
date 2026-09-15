//! Actual embedded WASM checks for decimal leaves inside containers.
use arrow::{
    array::*,
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema},
};
use datafusion::{
    execution::SessionStateBuilder,
    logical_expr::{Extension, LogicalPlan},
    prelude::SessionContext,
};
use std::sync::Arc;
use streamling_common::{
    formats::avro::{serialize, to_avro},
    types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType},
};
use streamling_core::operators::{planner::StreamlingQueryPlanner, wasm_runner::WasmRunnerNode};
fn input(kind: &str) -> RecordBatch {
    let field = Arc::new(DecimalArbType::field("amount", 80, 0, true).unwrap());
    let mut b = DecimalArbArrayBuilder::with_capacity(2, "amount", 80, 0).unwrap();
    b.append_str("1000").unwrap();
    b.append_str("-256").unwrap();
    let (raw, _, _) = b.finish().into_inner();
    let raw: ArrayRef = Arc::new(raw);
    let nested: ArrayRef = match kind {
        "struct" => Arc::new(StructArray::new(vec![field].into(), vec![raw], None)),
        "list" => Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 1, 2].into()),
            raw,
            None,
        )),
        "list_struct" => {
            let st = StructArray::new(vec![field].into(), vec![raw], None);
            let field = Arc::new(Field::new("item", st.data_type().clone(), false));
            Arc::new(ListArray::new(
                field,
                OffsetBuffer::new(vec![0, 1, 2].into()),
                Arc::new(st),
                None,
            ))
        }
        _ => panic!(),
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("nested", nested.data_type().clone(), true),
        Field::new("_gs_op", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![nested, Arc::new(StringArray::from(vec!["i", "i"]))],
    )
    .unwrap()
}
async fn echo(kind: &str) {
    let input = input(kind);
    let expected_schema = to_avro("Test", input.schema().fields());
    let expected = serialize(&expected_schema, &input);
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_batch("t", input).unwrap();
    let node = WasmRunnerNode::new(
        ctx.table("t").await.unwrap().into_optimized_plan().unwrap(),
        "javascript".into(),
        "row => ({nested: row.nested})".into(),
        None,
        1000,
        None,
    );
    let batches = ctx
        .execute_logical_plan(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let actual: Vec<_> = batches
        .iter()
        .flat_map(|b| serialize(&expected_schema, b))
        .collect();
    assert_eq!(
        actual, expected,
        "A script echo must not alter decimal values inside {kind}"
    );
}
#[tokio::test]
async fn deep_wasm_nested_struct_echo() {
    echo("struct").await;
}
#[tokio::test]
async fn deep_wasm_nested_list_echo() {
    echo("list").await;
}
#[tokio::test]
async fn deep_wasm_nested_list_struct_echo() {
    echo("list_struct").await;
}

#[tokio::test]
async fn deep_wasm_all_null_decimal_echo() {
    let field = DecimalArbType::field("amount", 80, 0, true).unwrap();
    let mut b = DecimalArbArrayBuilder::with_capacity(2, "amount", 80, 0).unwrap();
    b.append_null();
    b.append_null();
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            field,
            Field::new("_gs_op", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(b.finish().into_inner().0),
            Arc::new(StringArray::from(vec!["i", "i"])),
        ],
    )
    .unwrap();
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_batch("t", input).unwrap();
    let node = WasmRunnerNode::new(
        ctx.table("t").await.unwrap().into_optimized_plan().unwrap(),
        "javascript".into(),
        "row => ({amount: row.amount})".into(),
        None,
        1000,
        None,
    );
    let result = ctx
        .execute_logical_plan(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
        .await
        .unwrap()
        .collect()
        .await;
    let batches = result.unwrap_or_else(|e| {
        panic!(
            "identity script on a nullable all-null decimal batch failed: {}",
            e.to_string().lines().next().unwrap()
        )
    });
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    for b in batches {
        assert_eq!(
            b.column_by_name("amount").unwrap().null_count(),
            b.num_rows()
        );
    }
}

#[test]
fn deep_native_plugin_ffi_preserves_nested_decimal_fields_and_values() {
    for kind in ["struct", "list", "list_struct"] {
        for (offset, len) in [(0, 2), (1, 1)] {
            let input = input(kind).slice(offset, len);
            let ffi: streamling_plugin::ffi::SafeArrowArray = input.clone().into();
            let output: RecordBatch = ffi.into();
            assert_eq!(
                output.schema().fields(),
                input.schema().fields(),
                "{kind} slice{offset}:{len}"
            );
            assert_eq!(
                output.columns(),
                input.columns(),
                "{kind} slice{offset}:{len}"
            );
        }
    }
}
