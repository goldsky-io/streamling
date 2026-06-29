//! Multi-sink e2e tests.
//!
//! These tests verify that streamling can correctly read from a source and write to multiple sinks.
//! Ported from crates/streamling/tests/pipeline.rs (test_pipeline_end_to_end_with_multi_sink)

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

// ============================================================================
// Test Record Types
// ============================================================================

/// Basic test record structure
#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    id: i64,
    value: String,
    timestamp: i64,
}

const TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"},
        {"name": "timestamp", "type": "long"}
    ]
}"#;

// ============================================================================
// Scenario 1: Kafka source to multiple sinks (PostgreSQL + ClickHouse)
// ============================================================================

/// Test reading from Kafka and writing to both PostgreSQL and ClickHouse.
#[tokio::test]
async fn test_multi_sink_postgres_and_clickhouse() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    // Register schema for Kafka topic
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce test records
    let records_to_produce: i64 = 100;
    let records: Vec<TestRecord> = (1..=records_to_produce)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Run pipeline: Kafka source → PostgreSQL sink + ClickHouse sink
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: multi_test_pg
    schema: public
    primary_key: id
    on_conflict: update

  ch_sink:
    type: clickhouse
    from: kafka_source
    table: multi_test_ch
    primary_key: id
    schema_override:
      _gs_op: "String"
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify PostgreSQL sink received all records
    let pg_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.multi_test_pg")
        .await
        .expect("Failed to query PostgreSQL count");

    assert_eq!(
        pg_count, records_to_produce,
        "PostgreSQL should have {} records",
        records_to_produce
    );

    // Verify ClickHouse sink received all records
    let ch_count: u64 = clickhouse
        .count("SELECT COUNT(*) FROM multi_test_ch")
        .await
        .expect("Failed to query ClickHouse count");

    assert_eq!(
        ch_count, records_to_produce as u64,
        "ClickHouse should have {} records",
        records_to_produce
    );

    // Verify data consistency between sinks
    let pg_ids: Vec<(i64,)> = ctx
        .postgres
        .query("SELECT id FROM public.multi_test_pg WHERE id IN (1, 50, 100) ORDER BY id")
        .await
        .expect("Failed to query PostgreSQL ids");

    assert_eq!(pg_ids.len(), 3, "Should have ids 1, 50, 100 in PostgreSQL");
}

// ============================================================================
// Scenario 2: Kafka source to PostgreSQL + Webhook sinks
// ============================================================================

/// Test reading from Kafka and writing to both PostgreSQL and Webhook
#[tokio::test]
async fn test_multi_sink_postgres_and_webhook() {
    init_tracing();

    use streamling_e2e::resources::WebhookResource;

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Register schema
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Start webhook server
    let webhook = WebhookResource::new()
        .await
        .expect("Failed to start webhook server");

    // Produce test records
    let records_to_produce: i64 = 50;
    let records: Vec<TestRecord> = (1..=records_to_produce)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Run pipeline: Kafka source → PostgreSQL sink + Webhook sink
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: multi_test_pg_webhook
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 10
    batch_flush_interval: 100ms

  webhook_sink:
    type: webhook
    from: kafka_source
    url: {webhook_url}
    one_row_per_request: true
    payload_version: 0
"#,
        topic = ctx.kafka_topic,
        webhook_url = webhook.webhook_url(),
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Small delay to ensure batches are flushed
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify PostgreSQL sink received all records
    let pg_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.multi_test_pg_webhook")
        .await
        .expect("Failed to query PostgreSQL count");

    assert_eq!(
        pg_count, records_to_produce,
        "PostgreSQL should have {} records",
        records_to_produce
    );

    // Wait for webhook to receive all requests
    let received_all = webhook
        .wait_for_requests(
            records_to_produce as usize,
            std::time::Duration::from_secs(10),
        )
        .await;

    assert!(
        received_all,
        "Expected {} webhook requests, got {}",
        records_to_produce,
        webhook.request_count()
    );
}

// ============================================================================
// Scenario 2b: Backpressure attribution — one slow sink in a multi-sink fan-out
// ============================================================================

/// A deliberately slow webhook sink alongside a fast Postgres sink must:
///   1. backpressure the shared upstream (source `node_backpressured_time` > 0), and
///   2. attribute the broadcast's blocked-send time to the slow sink via the
///      `downstream_id` label (`backpressure_blocked_send{downstream_id="webhook_sink"}`).
///
/// This is the end-to-end proof of the "which sink is struggling" signal.
#[tokio::test]
async fn test_multi_sink_backpressure_attribution() {
    init_tracing();

    use streamling_e2e::resources::{PrometheusResource, WebhookResource};

    let ctx = match TestContext::with_options(TestContextOptions::new().with_prometheus()).await {
        Ok(ctx) => ctx,
        Err(e) => {
            eprintln!("Skipping test - could not create context: {}", e);
            return;
        }
    };

    let prometheus = match &ctx.prometheus {
        Some(p) => p,
        None => {
            eprintln!("Skipping test - Prometheus not configured");
            return;
        }
    };

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Slow sink: each webhook request blocks 100ms. With a default broadcast
    // buffer of 10, the producer fills the webhook consumer's channel then
    // blocks on it, accruing attributed blocked-send time while the fast
    // Postgres sink drains freely.
    let webhook = WebhookResource::new()
        .await
        .expect("Failed to start webhook server");
    webhook.set_delay(std::time::Duration::from_millis(100));

    let records_to_produce: i64 = 30;
    let records: Vec<TestRecord> = (1..=records_to_produce)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: multi_test_pg_backpressure
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1

  webhook_sink:
    type: webhook
    from: kafka_source
    url: {webhook_url}
    one_row_per_request: true
    payload_version: 0
    batch_size: 1
"#,
        topic = ctx.kafka_topic,
        webhook_url = webhook.webhook_url(),
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .timeout(std::time::Duration::from_secs(120))
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Both sinks must still receive every record despite the slow webhook.
    let pg_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.multi_test_pg_backpressure")
        .await
        .expect("Failed to query PostgreSQL count");
    assert_eq!(
        pg_count, records_to_produce,
        "Postgres should have all rows"
    );

    assert!(
        webhook
            .wait_for_requests(
                records_to_produce as usize,
                std::time::Duration::from_secs(30)
            )
            .await,
        "Expected {} webhook requests, got {}",
        records_to_produce,
        webhook.request_count()
    );

    // Give metrics time to flush to Prometheus.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Aggregate across series (sum) so cross-run series from other tests only
    // add to the total and cannot mask this run's backpressure.
    let blocked_webhook_query = format!(
        "sum({})",
        PrometheusResource::backpressure_blocked_send_query("webhook_sink", None)
    );
    let blocked_webhook = prometheus
        .wait_for_metric_at_least(&blocked_webhook_query, 50, 30, 500)
        .await
        .expect("slow webhook sink must accrue attributed blocked-send time");
    assert!(
        blocked_webhook >= 50,
        "expected substantial blocked-send time attributed to webhook_sink, got {blocked_webhook}ms"
    );

    let source_backpressure_query = format!(
        "sum({})",
        PrometheusResource::node_backpressured_time_query("kafka_source", None)
    );
    let source_backpressure = prometheus
        .wait_for_metric_at_least(&source_backpressure_query, 1, 30, 500)
        .await
        .expect("source must be backpressured by the stalled fan-out");
    assert!(
        source_backpressure >= 1,
        "expected the source to register backpressure, got {source_backpressure}ms"
    );
}

// ============================================================================
// Scenario 3: One branch fails and pipeline should fail fast
// ============================================================================

/// Test that a terminal error in one branch fails the full pipeline, even when another sink is healthy.
///
/// This protects against silently "losing" a failed branch while healthy branches keep running.
#[tokio::test]
async fn test_multi_sink_fails_fast_when_one_sink_errors() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=20)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Intentionally use a transform that fails at runtime:
    // `to_u256(value)` expects a numeric string, but test records contain "value_N".
    // One blackhole sink consumes the healthy branch, another consumes the failing branch.
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  broken_transform:
    sql: "SELECT id, to_u256(value) as amount FROM kafka_source"

sinks:
  healthy_blackhole_sink:
    type: blackhole
    from: kafka_source

  broken_blackhole_sink:
    type: blackhole
    from: broken_transform
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_raw(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records.len() as u64)
                .timeout(std::time::Duration::from_secs(40)),
        )
        .await
        .expect("Pipeline should exit with an error, not time out");

    assert!(
        !output.status.success(),
        "Pipeline should fail when one branch errors"
    );
}
