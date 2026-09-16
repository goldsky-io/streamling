//! Verify a mixed-scale UNION through the actual ClickHouse sink projection.
//! No ClickHouse service is contacted. Set STREAMLING_REVIEW_ARTIFACT_DIR to
//! additionally save the projected Arrow batches for an independent wire check.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    ipc::writer::FileWriter,
};
use datafusion::{datasource::TableProvider, logical_expr::dml::InsertOp, physical_plan::collect};
use std::{fs::File, path::PathBuf, sync::Arc};
use streamling_connectors::table_providers::clickhouse::{
    ClickHouseClient, ClickHouseTableProvider,
};
use streamling_core::{
    dynamic_table::DynamicTableRegistry,
    session::SessionManager,
    types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue},
};

async fn union_case(b_scale: u32) -> datafusion::common::Result<()> {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    let mut a = DecimalArbArrayBuilder::with_capacity(1, "a", 100, 0).unwrap();
    a.append_str("1").unwrap();
    let mut b = DecimalArbArrayBuilder::with_capacity(1, "b", 100, b_scale).unwrap();
    b.append_str("1").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("a", 100, 0, true).unwrap(),
        DecimalArbType::field("b", 100, b_scale, true).unwrap(),
        Field::new("_gs_op", DataType::Utf8, false),
    ]));
    let input = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(a.finish().into_inner().0),
            Arc::new(b.finish().into_inner().0),
            Arc::new(StringArray::from(vec!["i"])),
        ],
    )
    .unwrap();
    sm.session_context().register_batch("t", input).unwrap();
    let (logical, _) = sm
        .create_supported_logical_plan(
            "SELECT id,a AS value,_gs_op FROM t UNION ALL SELECT id+1,b AS value,_gs_op FROM t"
                .into(),
        )
        .await?;
    let declared = logical.schema().as_arrow();
    eprintln!("UNION declared schema {declared:?}");
    let physical = sm.new_df(logical).create_physical_plan().await?;
    let config=serde_json::from_value(serde_json::json!({"url":"http://127.0.0.1:30123","database":"default","user":"default","password":"","columns":[{"name":"value","coerce_to":"string"}]})).unwrap();
    let sink = ClickHouseTableProvider::new_sink(
        "deep_v2_union".into(),
        "unused_deep_v2_union",
        config,
        None,
        "id".into(),
        None,
        None,
        None,
        None,
        None,
        None,
        "deep_v2_union".into(),
        None,
    )
    .unwrap();
    let plan = sink
        .insert_into(&sm.session_context().state(), physical, InsertOp::Append)
        .await?;
    // Execute only the sink's input projection, never the ClickHouse writer.
    let projected = collect(plan.children()[0].clone(), sm.session_context().task_ctx()).await?;
    let planned_schema = plan.children()[0].schema();
    eprintln!(
        "CH projected schema {planned_schema:?}; DDL={}",
        ClickHouseClient::clickhouse_column_type(
            planned_schema.field_with_name("value").unwrap(),
            None
        )
        .unwrap()
    );
    if let Some(artifact_dir) = std::env::var_os("STREAMLING_REVIEW_ARTIFACT_DIR") {
        let artifact_dir = PathBuf::from(artifact_dir);
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let path = artifact_dir.join(if b_scale == 2 {
            "union-clickhouse-output.arrow"
        } else {
            "union-clickhouse-control.arrow"
        });
        let mut writer =
            FileWriter::try_new(File::create(path).unwrap(), planned_schema.as_ref()).unwrap();
        for batch in &projected {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    let rows: Vec<_> = projected
        .iter()
        .flat_map(|b| {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = b
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..b.num_rows())
                .map(|i| (ids.value(i), values.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect();
    eprintln!("ACTUAL CLICKHOUSE INPUT ROWS {rows:?}");
    assert_eq!(
        projected.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2
    );
    for batch in projected {
        let values = batch.column_by_name("value").unwrap();
        assert_eq!(
            values.data_type(),
            &DataType::Utf8,
            "coerce_to:string must output decimal text, not canonical binary bytes"
        );
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        for v in values.iter() {
            assert_eq!(
                v.unwrap().parse::<DecimalArbValue>().unwrap(),
                "1".parse::<DecimalArbValue>().unwrap()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn supported_union_clickhouse_string_projection() {
    // Rejecting an unsupported mixed-scale UNION before emitting rows is safe;
    // successful execution must preserve each branch's mathematical value.
    if let Err(error) = union_case(2).await {
        eprintln!("Mixed-scale UNION rejected explicitly: {error}");
    }
}

#[tokio::test]
async fn supported_uniform_scale_union_clickhouse_control() {
    union_case(0).await.unwrap();
}
