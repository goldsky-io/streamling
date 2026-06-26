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
    // KNOWN GAP (F6): the JSON serializer special-cases only TOP-LEVEL decimal_arb
    // columns. A decimal_arb NESTED in a struct/array is emitted as its raw
    // canonical bytes in hex, NOT its decimal value. Pinned here; when F6 is fixed
    // the value will appear and this tripwire fails — flip it to assert the value.
    assert!(
        blob.contains("00018ee90ff6c373e0ee4e3f0ad2"),
        "expected nested decimal_arb to render as canonical-byte hex (F6); got: {blob}"
    );
    assert!(
        !blob.contains(BIG),
        "F6 may be fixed: nested decimal_arb now renders its value — update this test"
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
    // KNOWN GAP (F6): same as the nested-struct case — array<record<decimal_arb>>
    // renders the canonical bytes as hex, not the values.
    assert!(
        blob.contains("00018ee90ff6c373e0ee4e3f0ad2") && blob.contains("0007"),
        "expected array<decimal_arb> to render as canonical-byte hex (F6); got: {blob}"
    );
    assert!(
        !blob.contains(BIG),
        "F6 may be fixed: nested decimal_arb now renders its value — update this test"
    );
}

/// nested struct with decimal_arb -> Kafka **Avro sink**. KNOWN GAP (F7): the
/// avro schema builder emits nested decimal_arb as plain `Bytes`, so the encode
/// fails. This test pins that failure.
#[tokio::test]
async fn nested_struct_decimal_arb_kafka_avro_sink_fails_f7() {
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
    // KNOWN GAP (F7): the Kafka Avro SINK does not handle NESTED decimal_arb. The
    // `to_avro` schema builder emits nested decimal_arb fields as plain `Bytes`
    // (only top-level fields get the `decimal` logicalType), so the writer's
    // `Value::Decimal` fails to encode ("Unsupported value-schema combination:
    // Decimal vs Bytes") and the sink errors. Pinned here; when nested-aware avro
    // schema generation lands, restore the round-trip + value assertion.
    let out = ctx
        .run_pipeline_raw(
            &p1,
            base_opts()
                .record_limit(1)
                .timeout(std::time::Duration::from_secs(20)),
        )
        .await;
    match out {
        Ok(o) => {
            assert!(
                !o.status.success(),
                "F7 may be fixed: nested decimal_arb now encodes to avro — restore the round-trip test"
            );
            assert!(
                !o.stderr.contains("panicked"),
                "avro sink must not panic on nested decimal_arb: {}",
                o.stderr
            );
        }
        // Timed out because the sink retried the non-retriable encode error (F4).
        Err(_) => {}
    }
}
