//! Adversarial e2e tests: decimal_arb across SINK boundaries that weren't
//! previously covered — Kafka(Avro) sink, Webhook (JSON), Print (JSON).
//!
//! decimal_arb is produced via Kafka(Avro) `produce_decimal_record` (id long +
//! one decimal(p,s)). Each test documents whether the boundary round-trips the
//! value. (Postgres/ClickHouse sinks are covered elsewhere; MySQL/SQS sinks do
//! not exist in streamling; file/CSV + JSON sources do not yield decimal_arb.)

use serde::Deserialize;
use sqlx::FromRow;
use streamling_e2e::resources::WebhookResource;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

fn decimal_schema(precision: u32, scale: u32) -> String {
    format!(
        r#"{{
            "type": "record",
            "name": "Amt",
            "fields": [
                {{"name": "id", "type": "long"}},
                {{"name": "amount", "type": {{"type": "bytes", "logicalType": "decimal", "precision": {precision}, "scale": {scale}}}}}
            ]
        }}"#
    )
}

#[derive(Debug, FromRow, Deserialize)]
struct IdText {
    #[allow(dead_code)]
    id: i64,
    t: String,
}

/// BOUNDARY: Kafka **Avro sink**. decimal_arb -> avro decimal on the wire ->
/// re-decoded by a second pipeline -> Postgres NUMERIC. A lossless round-trip
/// proves the avro WRITER emits a valid decimal logicalType that re-reads.
#[tokio::test]
async fn kafka_avro_sink_decimal_arb_round_trip() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    let schema = decimal_schema(100, 18);
    let cases: [(i64, &str); 3] = [
        (1, "1234567890123456789"),   // 1.234567890123456789
        (2, "-99000000000000000000"), // -99.0
        (3, "0"),
    ];
    for (id, unscaled) in &cases {
        ctx.kafka
            .produce_decimal_record(&schema, *id, "amount", unscaled)
            .await
            .unwrap();
    }

    let out_topic = ctx.create_kafka_topic("decout").await.unwrap();

    // Pipeline 1: Kafka(Avro) decimal source -> Kafka(Avro) sink.
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
        .run_pipeline_with_opts(&p1, base_opts().record_limit(3))
        .await
        .unwrap();
    assert!(s1.success(), "kafka->kafka(avro) pipeline should succeed");

    // Pipeline 2: read the sink's output topic back -> Postgres NUMERIC.
    ctx.postgres
        .execute("CREATE TABLE amounts (id BIGINT PRIMARY KEY, amount NUMERIC(100,18) NOT NULL)")
        .await
        .unwrap();
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
  pg:
    type: postgres
    from: src2
    table: amounts
    schema: public
    primary_key: id
    on_conflict: update
"#,
        output = out_topic.topic,
    );
    let s2 = ctx
        .run_pipeline_with_opts(&p2, base_opts().record_limit(3))
        .await
        .unwrap();
    assert!(
        s2.success(),
        "reading the avro-sink topic back should succeed"
    );

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, amount::text AS t FROM public.amounts ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        3,
        "all 3 decimals must survive the Kafka avro sink round-trip"
    );
    assert_eq!(rows[0].t, "1.234567890123456789");
    assert_eq!(rows[1].t, "-99.000000000000000000");
    assert_eq!(rows[2].t, "0.000000000000000000");
}

/// BOUNDARY: Webhook (HTTP/JSON) sink. decimal_arb is serialized via the JSON
/// converter. Asserts the canonical decimal value appears in the request body.
#[tokio::test]
async fn webhook_sink_decimal_arb() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let webhook = WebhookResource::new().await.unwrap();

    let schema = decimal_schema(100, 18);
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", "1234567890123456789")
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
  wh:
    type: webhook
    from: src
    url: {url}
"#,
        input = ctx.kafka_topic,
        url = webhook.webhook_url(),
    );
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(1))
        .await
        .unwrap();
    assert!(status.success(), "webhook pipeline should succeed");

    let got = webhook
        .wait_for_requests(1, std::time::Duration::from_secs(10))
        .await;
    assert!(got, "webhook should have received >=1 request");

    let bodies = webhook.get_request_bodies_as_json();
    let blob = serde_json::to_string(&bodies).unwrap();
    assert!(
        blob.contains("1.234567890123456789"),
        "webhook JSON body must carry the canonical decimal value, got: {blob}"
    );
}

/// BOUNDARY: Print (JSON) sink. decimal_arb is serialized via the JSON converter
/// and captured from the print output.
#[tokio::test]
async fn print_sink_decimal_arb() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    let schema = decimal_schema(100, 18);
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", "1234567890123456789")
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

    assert!(
        captured.has_column("amount"),
        "print output must include the amount column"
    );
    let vals = captured.column_values("amount");
    let blob = format!("{vals:?}");
    assert!(
        blob.contains("1.234567890123456789"),
        "print output must carry the canonical decimal value, got: {blob}"
    );
}
