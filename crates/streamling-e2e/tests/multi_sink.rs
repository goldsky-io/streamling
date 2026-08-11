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

// ============================================================================
// Scenario 4: Checkpoints must finalize when a transform-adding sink (ClickHouse)
// shares a source with another sink (webhook) through the multi-sink broadcast
// ============================================================================

/// Regression: checkpoint markers ride on per-batch schema metadata, and the
/// per-sink plans inside `MultiSinkExec` live outside the tree the physical
/// optimizer traverses. A ClickHouse sink's `insert_into` adds a stock
/// `ProjectionExec`; un-rewritten, it rebuilds every batch with its plan-time
/// schema and silently drops the markers — the sink keeps writing but never
/// acks an epoch, and the coordinator stalls forever on "missing sinks".
///
/// The webhook branch adds no transform nodes, so it acks fine — masking the
/// stall from delivery-only assertions. This test pins the fix by asserting
/// checkpoint *finalization*, not just delivery:
///   1. At least one epoch finalizes ("Epoch finalized" — requires ALL sinks,
///      including the ClickHouse branch behind the projection, to ack).
///   2. No stall symptoms ("missing sinks" / "still waiting for finalization").
///   3. Both sinks still receive every record.
///
/// Records are produced in two waves with the second wave delayed past several
/// 1s checkpoint intervals, so the run provably spans at least one full
/// epoch cycle before the record limit stops the pipeline.
#[tokio::test]
async fn test_multi_sink_checkpoint_finalizes_with_clickhouse_and_webhook() {
    init_tracing();

    use streamling_e2e::resources::WebhookResource;

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let webhook = WebhookResource::new()
        .await
        .expect("Failed to start webhook server");

    let total_records: i64 = 100;
    let first_wave: Vec<TestRecord> = (1..=total_records / 2)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();
    let second_wave: Vec<TestRecord> = (total_records / 2 + 1..=total_records)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&first_wave)
        .await
        .expect("Failed to produce first wave");

    // Kafka source → ClickHouse sink + webhook sink sharing the same source
    // node, which engages the multi-sink broadcast. The ClickHouse branch is
    // the transform-adding one (its insert_into interposes a projection).
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
  ch_sink:
    type: clickhouse
    from: kafka_source
    table: multi_sink_checkpoint_ch
    primary_key: id
    schema_override:
      _gs_op: "String"

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

    // The pipeline cannot hit the record limit before the delayed second wave
    // arrives, so it stays up across several checkpoint intervals with data
    // flowing on both sides of the markers.
    let run_pipeline = ctx.run_pipeline_raw(
        &pipeline,
        PipelineOpts::new()
            .record_limit(total_records as u64)
            .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
            .env("RUST_LOG", "info")
            .timeout(std::time::Duration::from_secs(120)),
    );
    let produce_second_wave = async {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        ctx.kafka
            .produce_avro_records(&second_wave)
            .await
            .expect("Failed to produce second wave");
    };
    let (output, _) = tokio::join!(run_pipeline, produce_second_wave);
    let output = output.expect("Pipeline execution failed");

    assert!(
        output.status.success(),
        "Pipeline should exit successfully; stderr:\n{}",
        output.stderr
    );

    // Positive proof the ClickHouse branch acked: an epoch only finalizes once
    // every sink (including the one behind the sink-side projection) acks it.
    assert!(
        output.stderr.contains("Epoch finalized"),
        "No checkpoint epoch finalized — a marker was dropped before a sink ack.\nstderr:\n{}",
        output.stderr
    );

    // And no stall symptoms while the pipeline ran.
    assert!(
        !output.stderr.contains("missing sinks"),
        "Checkpoint coordinator reported missing sink acks.\nstderr:\n{}",
        output.stderr
    );
    assert!(
        !output.stderr.contains("still waiting for finalization"),
        "Checkpoint epoch stalled waiting for a sink ack.\nstderr:\n{}",
        output.stderr
    );

    // Delivery still works on both branches.
    let ch_count: u64 = clickhouse
        .count("SELECT COUNT(*) FROM multi_sink_checkpoint_ch")
        .await
        .expect("Failed to query ClickHouse count");
    assert_eq!(
        ch_count, total_records as u64,
        "ClickHouse should have {} records",
        total_records
    );

    let received_all = webhook
        .wait_for_requests(total_records as usize, std::time::Duration::from_secs(10))
        .await;
    assert!(
        received_all,
        "Expected {} webhook requests, got {}",
        total_records,
        webhook.request_count()
    );
}

// ============================================================================
// Fan-out over a parallel source
// ============================================================================

/// A fan-out used to force its input back to a single stream: `MultiSinkExec`
/// declared a `SinglePartition` requirement, so adding a second sink silently
/// serialized a parallel source.
///
/// With a parallel Kafka source, each of the N input partitions now gets its own
/// broadcast and its own write stream per sink. Both sinks must still receive
/// every row exactly once, and the upsert keys must stay intact — the two sinks
/// share one exchange, keyed on the primary key they agree on.
#[tokio::test]
async fn test_multi_sink_over_a_parallel_source() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    let topic = ctx
        .create_kafka_topic_with_partitions("parallel_fanout", 4)
        .await
        .expect("Failed to create multi-partition topic");
    topic
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records_to_produce: i64 = 100;
    let records: Vec<TestRecord> = (1..=records_to_produce)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{i}"),
            timestamp: 1000 + i,
        })
        .collect();
    topic
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    parallelism: 2
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: parallel_fanout_pg
    schema: public
    primary_key: id
    on_conflict: update

  ch_sink:
    type: clickhouse
    from: kafka_source
    table: parallel_fanout_ch
    primary_key: id
    schema_override:
      _gs_op: "String"
"#,
        topic = topic.topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "Streamling should exit successfully");

    let pg_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.parallel_fanout_pg")
        .await
        .expect("Failed to query PostgreSQL count");
    assert_eq!(
        pg_count, records_to_produce,
        "every row must reach the PostgreSQL sink across both partitions"
    );

    let ch_count: u64 = clickhouse
        .count("SELECT COUNT(*) FROM parallel_fanout_ch FINAL")
        .await
        .expect("Failed to query ClickHouse count");
    assert_eq!(
        ch_count, records_to_produce as u64,
        "every row must reach the ClickHouse sink across both partitions"
    );

    // No key may be missing from either sink — a dropped partition would show up
    // as a gap rather than a count mismatch if duplicates were also present.
    let missing: Vec<(i64,)> = ctx
        .postgres
        .query(
            "SELECT s.id FROM generate_series(1, 100) AS s(id) \
             LEFT JOIN public.parallel_fanout_pg o ON o.id = s.id \
             WHERE o.id IS NULL ORDER BY s.id",
        )
        .await
        .expect("Failed to query missing ids");
    assert!(
        missing.is_empty(),
        "fan-out lost rows: {:?}",
        missing.iter().map(|r| r.0).collect::<Vec<_>>()
    );
}
