//! Kafka sink e2e tests.
//!
//! These tests verify that streamling can correctly read from Kafka and write to Kafka.
//! Ported from crates/streamling/tests/pipeline.rs (test_kafka_sink_metrics, test_kafka_sink_metrics_multi_batch)

use serde::Serialize;
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
// Scenario 1: Basic Kafka to Kafka sink
// ============================================================================

/// Basic test: read records from Kafka source and write to Kafka sink
#[tokio::test]
async fn test_kafka_sink_basic() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let output_topic = ctx
        .create_kafka_topic("output")
        .await
        .expect("Failed to create output topic");

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

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  kafka_sink:
    type: kafka
    from: kafka_source
    topic: {output_topic}
    topic_partitions: 1
    data_format: avro
"#,
        input_topic = ctx.kafka_topic,
        output_topic = output_topic.topic,
    );

    let status = ctx
        .run_pipeline(&pipeline, records_to_produce as u64)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let messages = ctx
        .consume_kafka_messages(&output_topic.topic, records_to_produce as usize + 10)
        .await
        .expect("Failed to consume messages from output topic");

    assert!(
        messages.len() >= records_to_produce as usize,
        "Expected at least {} messages in output topic, got {}",
        records_to_produce,
        messages.len()
    );

    let id_strs: Vec<String> = messages
        .iter()
        .map(|(_, _, id_str)| id_str.clone())
        .collect();
    assert!(
        id_strs.iter().any(|s| s.starts_with("id=1,")),
        "Should contain id=1"
    );
    assert!(
        id_strs.iter().any(|s| s.starts_with("id=50,")),
        "Should contain id=50"
    );
    assert!(
        id_strs.iter().any(|s| s.starts_with("id=100,")),
        "Should contain id=100"
    );
}

// ============================================================================
// Scenario 2: Multiple batches through Kafka sink
// ============================================================================

/// Test with multiple batches of records
#[tokio::test]
async fn test_kafka_sink_multiple_batches() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let output_topic = ctx
        .create_kafka_topic("output")
        .await
        .expect("Failed to create output topic");

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

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  kafka_sink:
    type: kafka
    from: kafka_source
    topic: {output_topic}
    topic_partitions: 1
    data_format: avro
"#,
        input_topic = ctx.kafka_topic,
        output_topic = output_topic.topic,
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

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let messages = ctx
        .consume_kafka_messages(&output_topic.topic, records_to_produce as usize + 10)
        .await
        .expect("Failed to consume messages from output topic");

    assert!(
        messages.len() >= records_to_produce as usize,
        "Expected at least {} messages in output topic, got {}",
        records_to_produce,
        messages.len()
    );

    let id_strs: Vec<String> = messages
        .iter()
        .map(|(_, _, id_str)| id_str.clone())
        .collect();
    assert!(
        id_strs.iter().any(|s| s.starts_with("id=1,")),
        "Should contain id=1"
    );
    assert!(
        id_strs
            .iter()
            .any(|s| s.starts_with(&format!("id={},", records_to_produce))),
        "Should contain id={}",
        records_to_produce
    );
}

// ============================================================================
// Scenario 3: Parallel producers deliver all messages
// ============================================================================

/// Test that parallelism > 1 delivers all messages correctly with no data loss.
/// NOTE: primary_key is NOT set on the sink to avoid WrappingDataSink deduplication,
/// which would reduce the record count and prevent the pipeline from exiting.
/// Per-key ordering correctness (same key -> same producer) is verified in the
/// unit test `test_key_hash_producer_routing` in kafka.rs.
#[tokio::test]
async fn test_kafka_sink_parallel_producers() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let output_topic = ctx
        .create_kafka_topic("output")
        .await
        .expect("Failed to create output topic");

    let records_to_produce = 200;
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
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  kafka_sink:
    type: kafka
    from: kafka_source
    topic: {output_topic}
    topic_partitions: 4
    data_format: avro
    parallelism: 2
"#,
        input_topic = ctx.kafka_topic,
        output_topic = output_topic.topic,
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

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let messages = ctx
        .consume_kafka_messages(&output_topic.topic, records_to_produce as usize + 10)
        .await
        .expect("Failed to consume messages from output topic");

    assert!(
        messages.len() >= records_to_produce as usize,
        "Expected at least {} messages in output topic with parallelism=2, got {}",
        records_to_produce,
        messages.len()
    );

    let id_strs: Vec<String> = messages
        .iter()
        .map(|(_, _, id_str)| id_str.clone())
        .collect();
    assert!(
        id_strs.iter().any(|s| s.starts_with("id=1,")),
        "Should contain id=1"
    );
    assert!(
        id_strs.iter().any(|s| s.starts_with("id=100,")),
        "Should contain id=100"
    );
    assert!(
        id_strs
            .iter()
            .any(|s| s.starts_with(&format!("id={},", records_to_produce))),
        "Should contain id={}",
        records_to_produce
    );
}

// ============================================================================
// Scenario 4: Pre-existing topic skips create_topics (no Create perm needed)
// ============================================================================

/// When the output topic already exists, the sink detects it via metadata and
/// skips the create_topics call entirely. We verify this through log output:
/// the "skipping creation" message must appear, and "Successfully created topic"
/// must NOT appear. This proves Create permissions are never exercised.
#[tokio::test]
async fn test_kafka_sink_preexisting_topic() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let output_topic = ctx
        .create_kafka_topic("preexisting")
        .await
        .expect("Failed to create output topic");

    let records_to_produce = 50;
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
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  kafka_sink:
    type: kafka
    from: kafka_source
    topic: {output_topic}
    topic_partitions: 1
    data_format: avro
"#,
        input_topic = ctx.kafka_topic,
        output_topic = output_topic.topic,
    );

    let output = ctx
        .run_pipeline_raw(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .env("RUST_LOG", "streamling_connectors=debug,info"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(
        output.status.success(),
        "Streamling should exit successfully"
    );

    let logs = format!("{}\n{}", output.stdout, output.stderr);

    assert!(
        logs.contains("already exists, skipping creation"),
        "Expected 'skipping creation' log when topic pre-exists.\nLogs:\n{}",
        logs
    );
    assert!(
        !logs.contains("Successfully created topic"),
        "create_topics should NOT have been called for a pre-existing topic.\nLogs:\n{}",
        logs
    );

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let messages = ctx
        .consume_kafka_messages(&output_topic.topic, records_to_produce as usize + 10)
        .await
        .expect("Failed to consume messages from output topic");

    assert!(
        messages.len() >= records_to_produce as usize,
        "Expected at least {} messages in pre-existing output topic, got {}",
        records_to_produce,
        messages.len()
    );
}

// ============================================================================
// Scenario 5: Sink handles a topic it didn't pre-create
// ============================================================================

/// The output topic is NOT pre-created by the test harness. The sink should
/// handle this gracefully — either by auto-creating via create_topics or by
/// relying on broker-side auto-creation detected through fetch_metadata.
/// Either way, data must flow end-to-end.
#[tokio::test]
async fn test_kafka_sink_auto_create_topic() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let auto_topic = format!("test_{}_autocreate", &ctx.test_id[..8]);

    let records_to_produce = 50;
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
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  kafka_sink:
    type: kafka
    from: kafka_source
    topic: {output_topic}
    topic_partitions: 1
    data_format: avro
"#,
        input_topic = ctx.kafka_topic,
        output_topic = auto_topic,
    );

    let status = ctx
        .run_pipeline(&pipeline, records_to_produce as u64)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let messages = ctx
        .consume_kafka_messages(&auto_topic, records_to_produce as usize + 10)
        .await
        .expect("Failed to consume messages from auto-created output topic");

    assert!(
        messages.len() >= records_to_produce as usize,
        "Expected at least {} messages in auto-created output topic, got {}",
        records_to_produce,
        messages.len()
    );
}

// ============================================================================
// Scenario 6: Kafka source initial fetch timeout retries on idle topic
// ============================================================================

/// Verifies Kafka source startup does not fail when no message arrives within
/// the initial fetch timeout. We force a 1s timeout, delay the first message so
/// at least one retry occurs, and assert the retry debug log is present.
#[tokio::test]
async fn test_kafka_source_initial_fetch_timeout_retries() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let output_topic = ctx
        // Avro schema names are derived from topic names and must not contain '-'.
        .create_kafka_topic("timeout_retry_output")
        .await
        .expect("Failed to create output topic");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {input_topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  kafka_sink:
    type: kafka
    from: kafka_source
    topic: {output_topic}
    topic_partitions: 1
    data_format: avro
"#,
        input_topic = ctx.kafka_topic,
        output_topic = output_topic.topic,
    );

    let delayed_records = vec![TestRecord {
        id: 1,
        value: "delayed".to_string(),
        timestamp: 1001,
    }];

    let produce_after_delay = async {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        ctx.kafka
            .produce_avro_records(&delayed_records)
            .await
            .expect("Failed to produce delayed record");
    };

    let run_pipeline = ctx.run_pipeline_raw(
        &pipeline,
        PipelineOpts::new()
            .record_limit(1)
            .timeout(std::time::Duration::from_secs(30))
            .env("STREAMLING__KAFKA_SOURCE__CONSUMER_FETCH_TIMEOUT_SEC", "1")
            .env("RUST_LOG", "streamling_connectors=debug,info"),
    );

    let (_produced, output) = tokio::join!(produce_after_delay, run_pipeline);
    let output = output.expect("Streamling execution failed");

    assert!(
        output.status.success(),
        "Pipeline should succeed even after initial fetch timeout retries.\nstdout:\n{}\nstderr:\n{}",
        output.stdout,
        output.stderr
    );

    let logs = format!("{}\n{}", output.stdout, output.stderr);
    assert!(
        logs.contains("initial message fetch timed out after 1s")
            && logs.contains("no new records yet; retrying"),
        "Expected debug retry log for initial fetch timeout.\nLogs:\n{}",
        logs
    );
}
