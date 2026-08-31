//! Webhook sink e2e tests.
//!
//! These tests verify that streamling can correctly read from Kafka and send to webhook endpoints.
//! Ported from crates/streamling/tests/pipeline.rs (test_pipeline_end_to_end, test_pipeline_end_to_end_webhook_sink_multiple_batches)

use axum::http::StatusCode;
use serde::Serialize;
use streamling_e2e::resources::WebhookResource;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

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
// Scenario 1: Basic Kafka to Webhook sink
// ============================================================================

/// Basic test: read records from Kafka and send to webhook endpoint
/// Ported from: test_pipeline_end_to_end
#[tokio::test]
async fn test_webhook_sink_basic() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Register schema for input topic
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Start webhook server
    let webhook = WebhookResource::new()
        .await
        .expect("Failed to start webhook server");

    // Produce test records
    let records_to_produce = 100;
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

    // Run pipeline: Kafka source → Webhook sink
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
        .run_pipeline(&pipeline, records_to_produce as u64)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Wait a bit for all requests to be processed
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

    // Verify the request count
    assert_eq!(
        webhook.request_count(),
        records_to_produce as usize,
        "Should have received {} webhook requests",
        records_to_produce
    );
}

// ============================================================================
// Scenario 2: Webhook sink with multiple batches
// ============================================================================

/// Test with multiple batches of records
/// Ported from: test_pipeline_end_to_end_webhook_sink_multiple_batches
#[tokio::test]
async fn test_webhook_sink_multiple_batches() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Start webhook server
    let webhook = WebhookResource::new()
        .await
        .expect("Failed to start webhook server");

    // Produce more records (enough to span multiple batches)
    let records_to_produce = 500;
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

    // Run pipeline with longer timeout for larger batch
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
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Wait for all requests to be processed
    let received_all = webhook
        .wait_for_requests(
            records_to_produce as usize,
            std::time::Duration::from_secs(30),
        )
        .await;

    assert!(
        received_all,
        "Expected {} webhook requests, got {}",
        records_to_produce,
        webhook.request_count()
    );
}

#[tokio::test]
async fn test_webhook_sink_skip_on_error_continues_after_500() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let webhook = WebhookResource::new_with_response_plan(vec![
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::OK,
        StatusCode::OK,
    ])
    .await
    .expect("Failed to start webhook server");

    let records: Vec<TestRecord> = (1..=3)
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
  webhook_sink:
    type: webhook
    from: kafka_source
    url: {webhook_url}
    one_row_per_request: true
    payload_version: 0
    skip_on_error: true
"#,
        topic = ctx.kafka_topic,
        webhook_url = webhook.webhook_url(),
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records.len() as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let received_all = webhook
        .wait_for_requests(records.len(), std::time::Duration::from_secs(10))
        .await;

    assert!(
        received_all,
        "Expected {} webhook requests, got {}",
        records.len(),
        webhook.request_count()
    );
}

// ============================================================================
// Scenario 4: Concurrent write streams
// ============================================================================

/// `parallelism` on a webhook sink turns into concurrent `write_all` calls, fed
/// by a hash exchange on the sink's primary key so a key is never delivered by
/// two streams at once.
///
/// Run at a width above and below the source's, so the width is pinned to the
/// sink's own declaration rather than to whatever it inherited. Every row must
/// still arrive exactly once through either exchange direction.
#[tokio::test]
async fn webhook_sink_parallelism_widens_the_write_streams() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    let topic = ctx
        .create_kafka_topic_with_partitions("webhook_parallel", 4)
        .await
        .expect("Failed to create multi-partition topic");
    topic
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records_to_produce = 60;
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

    // 2 -> 3 widens the sink beyond its source, 2 -> 1 narrows it to a coalesce.
    for sink_parallelism in [3, 1] {
        let webhook = WebhookResource::new()
            .await
            .expect("Failed to start webhook server");

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
  webhook_sink:
    type: webhook
    from: kafka_source
    url: {webhook_url}
    primary_key: id
    parallelism: {sink_parallelism}
    one_row_per_request: true
    payload_version: 0
"#,
            topic = topic.topic,
            webhook_url = webhook.webhook_url(),
        );

        let output = ctx
            .run_pipeline_raw(
                &pipeline,
                PipelineOpts::new()
                    .record_limit(records_to_produce as u64)
                    .env("RUST_LOG", "info")
                    .timeout(std::time::Duration::from_secs(120)),
            )
            .await
            .expect("Streamling execution failed");

        assert!(
            output.status.success(),
            "Pipeline should exit successfully at sink parallelism {sink_parallelism}; \
             stderr:\n{}",
            output.stderr
        );
        assert!(
            output
                .stderr
                .contains(&format!("ParallelSinkExec: partitions={sink_parallelism}")),
            "the webhook sink must write from {sink_parallelism} stream(s); stderr:\n{}",
            output.stderr
        );
        // Widening goes through the keyed exchange; narrowing to one stream is a
        // coalesce instead, since one stream trivially holds every key.
        let expected_node = if sink_parallelism > 1 {
            format!("StreamingRepartitionExec: partitions={sink_parallelism}, keys=[id@")
        } else {
            "StreamingCoalesceExec".to_string()
        };
        assert!(
            output.stderr.contains(&expected_node),
            "expected {expected_node:?} at sink parallelism {sink_parallelism}; stderr:\n{}",
            output.stderr
        );

        let received_all = webhook
            .wait_for_requests(
                records_to_produce as usize,
                std::time::Duration::from_secs(30),
            )
            .await;
        assert!(
            received_all,
            "Expected {} webhook requests at sink parallelism {sink_parallelism}, got {}",
            records_to_produce,
            webhook.request_count()
        );

        // A count alone cannot separate loss from duplication; the exact id set
        // can. Arrival order across concurrent streams is meaningless, so sort.
        let mut delivered_ids: Vec<i64> = webhook
            .get_request_bodies_as_json()
            .iter()
            .filter_map(|body| body.get("id").and_then(|id| id.as_i64()))
            .collect();
        delivered_ids.sort();
        assert_eq!(
            delivered_ids,
            (1..=records_to_produce).collect::<Vec<i64>>(),
            "every row must be delivered exactly once at sink parallelism {sink_parallelism}"
        );
    }
}
