//! Original-base wire-to-SQL attribution; no production code is modified.
use streamling_common::formats::{
    FromArrowConverter, avro::arrow_avro::ConfluentAvroDecoder, json::FromArrowToJsonConverter,
};
use streamling_core::{dynamic_table::DynamicTableRegistry, session::SessionManager};

#[tokio::test]
async fn original_avro_p100_scaled_list_unnest_json() {
    let schema = r#"{"type":"record","name":"NestedDecimalBase","fields":[{"name":"items","type":{"type":"array","items":{"type":"bytes","logicalType":"decimal","precision":100,"scale":2}}}]}"#;
    let mut decoder = ConfluentAvroDecoder::new();
    decoder.register_writer_schema(1, schema).unwrap();
    eprintln!("BASE_P100_TARGET {:?}", decoder.target_schema());
    // Confluent schema id1, array blockcount2, bytes1=0x7b, bytes2=0xfe38,
    // end-of-array. Avro decimal uses signed big-endian coefficients123/-456.
    let frame = [0u8, 0, 0, 0, 1, 4, 2, 0x7b, 4, 0xfe, 0x38, 0];
    decoder.decode(&frame).unwrap();
    let batch = decoder.flush().unwrap().unwrap();
    eprintln!("BASE_P100_DECODED {batch:?}");
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    sm.session_context().register_batch("t", batch).unwrap();
    let (plan, _) = sm
        .create_supported_logical_plan("SELECT UNNEST(items) AS amount FROM t".into())
        .await
        .unwrap();
    let batches = sm.new_df(plan).collect().await.unwrap();
    let json = FromArrowToJsonConverter::new();
    let rows = batches
        .iter()
        .flat_map(|b| json.convert_from_batch(b).unwrap())
        .map(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).unwrap())
        .collect::<Vec<_>>();
    eprintln!("BASE_P100_UNNEST_JSON {rows:?}");
    let amounts = rows
        .iter()
        .map(|row| {
            let value = &row["amount"];
            value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string())
        })
        .collect::<Vec<_>>();
    assert_eq!(amounts, vec!["1.23", "-4.56"]);
}
