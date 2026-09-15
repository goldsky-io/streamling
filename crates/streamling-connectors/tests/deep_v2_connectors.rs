//! Additional boundary probes for PR 37 at 6e6072c. No production changes.
use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, LargeBinaryArray, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use std::str::FromStr;
use std::sync::Arc;
use streamling_connectors::table_providers::clickhouse::{
    clickhouse_native_to_decimal_arb, clickhouse_string_to_decimal_arb,
    decimal_arb_to_clickhouse_native,
};
use streamling_core::functions::json_string::JsonStringFunc;
use streamling_core::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, NativeIntKind,
};
use streamling_core::types::decimal_arb_capability::{
    ConnectorKind, validate_pipeline_decimal_arb,
};

fn decimal(precision: u32, scale: u32, values: &[Option<&str>]) -> (Arc<Field>, ArrayRef) {
    let f = Arc::new(DecimalArbType::field("amount", precision, scale, true).unwrap());
    let mut b =
        DecimalArbArrayBuilder::with_capacity(values.len(), "amount", precision, scale).unwrap();
    for v in values {
        match v {
            Some(v) => b.append_str(v).unwrap(),
            None => b.append_null(),
        }
    }
    (f, Arc::new(b.finish().into_inner().0))
}

fn json_string(field: Arc<Field>, array: ArrayRef) -> ArrayRef {
    match JsonStringFunc::new()
        .invoke_with_args(ScalarFunctionArgs {
            number_rows: array.len(),
            args: vec![ColumnarValue::Array(array)],
            arg_fields: vec![field],
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            config_options: Arc::default(),
        })
        .unwrap()
    {
        ColumnarValue::Array(a) => a,
        other => panic!("expected array, got {other:?}"),
    }
}

#[test]
fn deep_v2_json_string_decimal_keeps_numeric_text() {
    let (f, a) = decimal(100, 2, &[Some("1.23"), Some("-4.56"), None]);
    let out = json_string(f, a);
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        out.iter().collect::<Vec<_>>(),
        vec![Some("\"1.23\""), Some("\"-4.56\""), None]
    );
}

#[test]
fn deep_v2_postgres_nested_json_projection_control() {
    let (f, a) = decimal(100, 2, &[Some("1.23"), Some("-4.56"), None]);
    let nested = StructArray::new(vec![f].into(), vec![a], None);
    let field = Arc::new(Field::new("account", nested.data_type().clone(), false));
    assert!(
        validate_pipeline_decimal_arb(
            &Schema::new(vec![field.clone()]),
            ConnectorKind::Postgres,
            &[]
        )
        .is_ok()
    );
    let out = json_string(field, Arc::new(nested));
    let out = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        out.iter().collect::<Vec<_>>(),
        vec![
            Some("{\"amount\":\"1.23\"}"),
            Some("{\"amount\":\"-4.56\"}"),
            Some("{\"amount\":null}")
        ]
    );
}

#[tokio::test]
async fn deep_v2_supported_json_string_preserves_decimal_value() {
    use streamling_core::dynamic_table::DynamicTableRegistry;
    use streamling_core::session::SessionManager;
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    let (field, values) = decimal(100, 2, &[Some("1.23"), Some("-4.56")]);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![values]).unwrap();
    sm.session_context().register_batch("t", batch).unwrap();
    let (plan, _) = sm
        .create_supported_logical_plan("SELECT json_string(amount) AS value FROM t".to_string())
        .await
        .unwrap();
    let batches = sm.new_df(plan).collect().await.unwrap();
    let actual: Vec<serde_json::Value> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| serde_json::from_str(s.unwrap()).unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        actual,
        vec![serde_json::json!("1.23"), serde_json::json!("-4.56")]
    );
}

#[test]
fn deep_v2_clickhouse_string_strictness_and_null_control() {
    let field = Arc::new(DecimalArbType::field("amount", 100, 2, true).unwrap());
    let input = StringArray::from(vec![Some("1.2300000"), Some("-4.56"), Some("1.23e2"), None]);
    let out = clickhouse_string_to_decimal_arb(&input, &field).unwrap();
    let out = out.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    for (i, expected) in ["1.23", "-4.56", "123"].iter().enumerate() {
        assert_eq!(
            DecimalArbValue::from_canonical_bytes_at_scale(out.value(i), 2).unwrap(),
            DecimalArbValue::from_str(expected).unwrap()
        );
    }
    assert!(out.is_null(3));
    for rejected in [
        "1.2300001",
        "NaN",
        "Infinity",
        "-Infinity",
        "",
        " ",
        "999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.00",
    ] {
        assert!(
            clickhouse_string_to_decimal_arb(&StringArray::from(vec![rejected]), &field).is_err(),
            "must reject {rejected:?}"
        );
    }
}

#[test]
fn deep_v2_clickhouse_native_signed_boundary_control() {
    let maximum_signed =
        "57896044618658097711785492504343953926634992332820282019728792003956564819967";
    let minimum_signed =
        "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
    for (kind, texts) in [
        (
            NativeIntKind::I256,
            vec!["0", "1", "-1", "255", "256", maximum_signed, minimum_signed],
        ),
        (
            NativeIntKind::U256,
            vec![
                "0",
                "1",
                "255",
                "256",
                "115792089237316195423570985008687907853269984665640564039457584007913129639935",
            ],
        ),
    ] {
        let (field, a) = decimal(78, 0, &texts.iter().map(|v| Some(*v)).collect::<Vec<_>>());
        let field =
            Arc::new(DecimalArbType::with_native_int_kind(field.as_ref().clone(), kind).unwrap());
        let wire = decimal_arb_to_clickhouse_native(a.as_ref(), &field).unwrap();
        let raw_wire = wire
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for (i, text) in texts.iter().enumerate() {
            let value = DecimalArbValue::from_str(text).unwrap();
            let (integer, _) = value.as_bigdecimal().as_bigint_and_exponent();
            let expected = if *text == "0" {
                vec![0; 32]
            } else if text.starts_with('-') {
                let mut little_endian = integer.to_signed_bytes_le();
                little_endian.resize(32, 0xff);
                little_endian
            } else {
                let (_, mut little_endian) = integer.to_bytes_le();
                little_endian.resize(32, 0);
                little_endian
            };
            assert_eq!(
                raw_wire.value(i),
                expected,
                "independent little-endian wire oracle for {kind:?}: {text}"
            );
        }
        let out = clickhouse_native_to_decimal_arb(wire.as_ref(), &field).unwrap();
        assert_eq!(a.as_ref(), out.as_ref());
    }
}

#[test]
fn deep_v2_clickhouse_native_reader_enforces_declared_precision() {
    // 10^77 fits UInt256 but not the Avro-derived decimal_arb(77,0) target.
    let (wide, a) = decimal(
        78,
        0,
        &[Some(
            "100000000000000000000000000000000000000000000000000000000000000000000000000000",
        )],
    );
    let wide = Arc::new(
        DecimalArbType::with_native_int_kind(wide.as_ref().clone(), NativeIntKind::U256).unwrap(),
    );
    let wire = decimal_arb_to_clickhouse_native(a.as_ref(), &wide).unwrap();
    let narrow = Arc::new(
        DecimalArbType::with_native_int_kind(
            DecimalArbType::field("amount", 77, 0, true).unwrap(),
            NativeIntKind::U256,
        )
        .unwrap(),
    );
    let result = clickhouse_native_to_decimal_arb(wire.as_ref(), &narrow);
    assert!(
        result.is_err(),
        "native read accepted 78 digits into decimal_arb(77,0)"
    );
}

#[test]
#[ignore = "Malformed-schema diagnostic: real source stamping only attaches native integer hints at scale 0; no reachable scale-2 native integer source was established"]
fn deep_v2_native_reader_rejects_fractional_hint_instead_of_changing_value() {
    // Diagnostic guard: current actual source stamping only adds hints at scale 0.
    let field = Arc::new(
        DecimalArbType::with_native_int_kind(
            DecimalArbType::field("amount", 78, 2, true).unwrap(),
            NativeIntKind::U256,
        )
        .unwrap(),
    );
    let mut bytes = [0u8; 32];
    bytes[0] = 123;
    let input = FixedSizeBinaryArray::try_from_iter([bytes.as_slice()].into_iter()).unwrap();
    let result = clickhouse_native_to_decimal_arb(&input, &field);
    match result {
        Err(_) => {}
        Ok(out) => {
            let out = out.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let actual = DecimalArbValue::from_canonical_bytes_at_scale(out.value(0), 2).unwrap();
            assert_eq!(
                actual,
                DecimalArbValue::from_str("123").unwrap(),
                "a native integer read must retain its mathematical value"
            );
        }
    }
}

#[test]
fn deep_v2_decimal_dedup_same_scale_control() {
    use streamling_core::utils::dedup::deduplicate_record_batch;
    let (field, amounts) = decimal(
        100,
        2,
        &[
            Some("1.23"),
            Some("1.230"),
            Some("2.56"),
            Some("-1.23"),
            None,
            None,
        ],
    );
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![amounts]).unwrap();
    let out = deduplicate_record_batch(&batch, "amount").unwrap();
    assert_eq!(out.num_rows(), 4);
}

#[tokio::test]
async fn deep_v2_supported_unnest_preserves_unrelated_decimal_field() {
    use arrow::array::{Int64Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use streamling_core::dynamic_table::DynamicTableRegistry;
    use streamling_core::formats::FromArrowConverter;
    use streamling_core::formats::json::FromArrowToJsonConverter;
    use streamling_core::session::SessionManager;

    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    let (amount_field, amounts) = decimal(100, 2, &[Some("1.23")]);
    let items = ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, false)),
        OffsetBuffer::new(vec![0, 2].into()),
        Arc::new(Int64Array::from(vec![1, 2])),
        None,
    );
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            amount_field.as_ref().clone(),
            Field::new("items", items.data_type().clone(), false),
        ])),
        vec![amounts, Arc::new(items)],
    )
    .unwrap();
    sm.session_context().register_batch("t", input).unwrap();
    let (plan, _) = sm
        .create_supported_logical_plan("SELECT amount, UNNEST(items) AS i FROM t".to_string())
        .await
        .unwrap();
    eprintln!("UNNEST PLAN {}", plan.display_indent());
    let batches = sm.new_df(plan).collect().await.unwrap();
    let json = FromArrowToJsonConverter::new();
    let rows: Vec<serde_json::Value> = batches
        .iter()
        .flat_map(|b| {
            eprintln!("UNNEST OUTPUT SCHEMA {:?}", b.schema());
            json.convert_from_batch(b)
                .unwrap()
                .into_iter()
                .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"amount":"1.23", "i":1}),
            serde_json::json!({"amount":"1.23", "i":2})
        ]
    );
}

#[tokio::test]
async fn deep_v2_supported_unnest_decimal_list_preserves_value() {
    use arrow::array::ListArray;
    use arrow::buffer::OffsetBuffer;
    use streamling_core::dynamic_table::DynamicTableRegistry;
    use streamling_core::formats::FromArrowConverter;
    use streamling_core::formats::json::FromArrowToJsonConverter;
    use streamling_core::session::SessionManager;

    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    let (amount_field, amounts) = decimal(100, 2, &[Some("1.23"), Some("-4.56")]);
    let items = ListArray::new(
        amount_field,
        OffsetBuffer::new(vec![0, 2].into()),
        amounts,
        None,
    );
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "items",
            items.data_type().clone(),
            false,
        )])),
        vec![Arc::new(items)],
    )
    .unwrap();
    sm.session_context().register_batch("t", input).unwrap();
    let (plan, _) = sm
        .create_supported_logical_plan("SELECT UNNEST(items) AS amount FROM t".to_string())
        .await
        .unwrap();
    eprintln!("UNNEST DECIMAL PLAN {}", plan.display_indent());
    let batches = sm.new_df(plan).collect().await.unwrap();
    let json = FromArrowToJsonConverter::new();
    let rows: Vec<serde_json::Value> = batches
        .iter()
        .flat_map(|b| {
            eprintln!("UNNEST DECIMAL OUTPUT SCHEMA {:?}", b.schema());
            json.convert_from_batch(b)
                .unwrap()
                .into_iter()
                .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"amount":"1.23"}),
            serde_json::json!({"amount":"-4.56"})
        ]
    );
}

#[test]
fn deep_v2_retired_fixed_width_pk_dedup_control() {
    use streamling_core::utils::dedup::deduplicate_record_batch;
    let mut one = [0u8; 32];
    one[31] = 1;
    let mut two = [0u8; 32];
    two[31] = 2;
    let old = FixedSizeBinaryArray::try_from_iter(
        [one.as_slice(), one.as_slice(), two.as_slice()].into_iter(),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "amount",
            old.data_type().clone(),
            false,
        )])),
        vec![Arc::new(old)],
    )
    .unwrap();
    assert_eq!(
        deduplicate_record_batch(&batch, "amount")
            .unwrap()
            .num_rows(),
        2
    );
}

#[tokio::test]
async fn deep_v2_clickhouse_default_native_pk_projection_is_deduplicable() {
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use streamling_connectors::table_providers::clickhouse::ClickHouseTableProvider;
    use streamling_core::utils::dedup::deduplicate_record_batch;

    let (field, values) = decimal(78, 0, &[Some("1"), Some("1"), Some("2")]);
    let field =
        DecimalArbType::with_native_int_kind(field.as_ref().clone(), NativeIntKind::U256).unwrap();
    let schema = Arc::new(Schema::new(vec![
        field,
        Field::new("_gs_op", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![values, Arc::new(StringArray::from(vec!["i", "i", "i"]))],
    )
    .unwrap();
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
    let ctx = SessionContext::new();
    let config = serde_json::from_value(serde_json::json!({"url":"http://127.0.0.1:30123", "database":"default", "user":"default", "password":""})).unwrap();
    let sink = ClickHouseTableProvider::new_sink(
        "deep_v2".into(),
        "unused_deep_v2",
        config,
        None,
        "amount".into(),
        None,
        None,
        None,
        None,
        None,
        None,
        "deep_v2".into(),
        None,
    )
    .unwrap();
    let plan = sink
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await
        .unwrap();
    // Collect only the real sink's input projection. No network request or write occurs.
    let projected = collect(plan.children()[0].clone(), ctx.task_ctx())
        .await
        .unwrap();
    assert_eq!(
        projected[0].column_by_name("amount").unwrap().data_type(),
        &DataType::LargeBinary
    );
    assert_eq!(
        deduplicate_record_batch(&projected[0], "amount")
            .unwrap()
            .num_rows(),
        2
    );
}
