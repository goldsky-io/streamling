//! Adversarial e2e tests: COMPLEX types carrying decimal_arb (nested struct,
//! array-of-records) across boundaries that natively support nesting — JSON
//! (print) and Avro (Kafka sink). The goal is to KNOW and pin the behavior:
//! nested decimal_arb either round-trips or it doesn't, and the test records
//! which. Nested input is built directly as an avro datum (the harness's
//! flat producers can't express nested decimals).
//!
//! (Postgres has no nested-column type and ClickHouse nested decimal_arb is not
//! wired, so those boundaries are documented in DECIMAL_ARB_COVERAGE.md rather
//! than exercised here.)

use apache_avro::types::Value;
use apache_avro::Decimal;
use num_bigint::BigInt;
use std::str::FromStr;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

// record { id: long, inner: record { amt: decimal(100,0) } }
const NESTED_STRUCT_SCHEMA: &str = r#"{
    "type": "record", "name": "R",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "inner", "type": {"type": "record", "name": "Inner", "fields": [
            {"name": "amt", "type": {"type": "bytes", "logicalType": "decimal", "precision": 100, "scale": 0}}
        ]}}
    ]
}"#;

// record { id: long, items: array<record { amt: decimal(100,0) }> }  (the "traces" shape)
const ARRAY_STRUCT_SCHEMA: &str = r#"{
    "type": "record", "name": "R",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "items", "type": {"type": "array", "items": {"type": "record", "name": "X", "fields": [
            {"name": "amt", "type": {"type": "bytes", "logicalType": "decimal", "precision": 100, "scale": 0}}
        ]}}}
    ]
}"#;

const BIG: &str = "123456789012345678901234567890"; // 30-digit value, > 2^64

fn decimal_val(unscaled: &str) -> Value {
    Value::Decimal(Decimal::from(
        BigInt::from_str(unscaled).unwrap().to_signed_bytes_be(),
    ))
}

/// nested struct with a decimal_arb child -> Print (JSON). Documents whether the
/// JSON serializer renders the nested decimal as its value.
#[tokio::test]
async fn nested_struct_decimal_arb_to_print_json() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    let rec = Value::Record(vec![
        ("id".to_string(), Value::Long(1)),
        (
            "inner".to_string(),
            Value::Record(vec![("amt".to_string(), decimal_val(BIG))]),
        ),
    ]);
    ctx.kafka
        .produce_avro_value(NESTED_STRUCT_SCHEMA, rec)
        .await
        .unwrap();

    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {input}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  out:
    type: print
    from: src
"#,
        input = ctx.kafka_topic,
    );
    let captured = ctx
        .run_pipeline_with_capture(&yaml, base_opts().record_limit(1))
        .await
        .unwrap();

    let blob = format!("{:?}", captured.column_values("inner"));
    // F6 FIXED: the JSON serializer now rewrites decimal_arb leaves recursively
    // (by field metadata), so a decimal_arb NESTED in a struct renders as its
    // decimal value, not the raw canonical bytes in hex.
    assert!(
        blob.contains(BIG),
        "nested decimal_arb must render its decimal value (F6 fixed); got: {blob}"
    );
    assert!(
        !blob.contains("00018ee90ff6c373e0ee4e3f0ad2"),
        "nested decimal_arb must NOT render as canonical-byte hex (F6 regressed); got: {blob}"
    );
}

/// array-of-records each with a decimal_arb -> Print (JSON). The blockchain
/// "transfers" shape. Documents the JSON serializer behavior for nested arrays.
#[tokio::test]
async fn array_of_decimal_arb_to_print_json() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    let rec = Value::Record(vec![
        ("id".to_string(), Value::Long(1)),
        (
            "items".to_string(),
            Value::Array(vec![
                Value::Record(vec![("amt".to_string(), decimal_val(BIG))]),
                Value::Record(vec![("amt".to_string(), decimal_val("7"))]),
            ]),
        ),
    ]);
    ctx.kafka
        .produce_avro_value(ARRAY_STRUCT_SCHEMA, rec)
        .await
        .unwrap();

    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {input}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  out:
    type: print
    from: src
"#,
        input = ctx.kafka_topic,
    );
    let captured = ctx
        .run_pipeline_with_capture(&yaml, base_opts().record_limit(1))
        .await
        .unwrap();

    let blob = format!("{:?}", captured.column_values("items"));
    // F6 FIXED: array<record<decimal_arb>> now renders each element's decimal
    // value (recursive metadata-driven rewrite), not the canonical bytes as hex.
    assert!(
        blob.contains(BIG) && blob.contains('7'),
        "array<decimal_arb> must render element values (F6 fixed); got: {blob}"
    );
    assert!(
        !blob.contains("00018ee90ff6c373e0ee4e3f0ad2"),
        "array<decimal_arb> must NOT render as canonical-byte hex (F6 regressed); got: {blob}"
    );
}

/// nested struct with decimal_arb -> Kafka **Avro sink** -> re-read -> Print JSON.
/// F7 FIXED: the avro schema builder now emits nested decimal_arb with the
/// `decimal` logicalType (it no longer round-trips the struct schema through
/// canonical_form, which stripped logicalType), so the sink encodes the nested
/// value. The re-read + print also confirms the nested value survives end-to-end
/// (decode C1 + render F6).
#[tokio::test]
async fn nested_struct_decimal_arb_kafka_avro_sink_round_trip() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    let rec = Value::Record(vec![
        ("id".to_string(), Value::Long(1)),
        (
            "inner".to_string(),
            Value::Record(vec![("amt".to_string(), decimal_val(BIG))]),
        ),
    ]);
    ctx.kafka
        .produce_avro_value(NESTED_STRUCT_SCHEMA, rec)
        .await
        .unwrap();

    let out_topic = ctx.create_kafka_topic("nestout").await.unwrap();

    // Pipeline 1: kafka(avro) source -> kafka(avro) sink. Must succeed now.
    let p1 = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {input}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  ksink:
    type: kafka
    from: src
    topic: {output}
    topic_partitions: 1
    data_format: avro
"#,
        input = ctx.kafka_topic,
        output = out_topic.topic,
    );
    let s1 = ctx
        .run_pipeline_with_opts(&p1, base_opts().record_limit(1))
        .await
        .unwrap();
    assert!(
        s1.success(),
        "nested decimal_arb must encode to the Avro sink (F7 fixed)"
    );

    // Pipeline 2: read the avro-sink output back -> Print (JSON). The nested
    // decimal must render its value (decode C1 + JSON render F6), not hex.
    let p2 = format!(
        r#"
sources:
  src2:
    type: kafka
    topic: {output}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  out:
    type: print
    from: src2
"#,
        output = out_topic.topic,
    );
    let captured = ctx
        .run_pipeline_with_capture(&p2, base_opts().record_limit(1))
        .await
        .unwrap();
    let blob = format!("{:?}", captured.column_values("inner"));
    assert!(
        blob.contains(BIG),
        "nested decimal_arb must survive the Avro sink round-trip with its value; got: {blob}"
    );
    assert!(
        !blob.contains("00018ee90ff6c373e0ee4e3f0ad2"),
        "round-tripped nested decimal_arb must not render as hex; got: {blob}"
    );
}
