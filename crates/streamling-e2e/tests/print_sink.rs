//! Print/Blackhole sink e2e tests.
//!
//! These tests verify that streamling can correctly read from Kafka and write to print/blackhole sinks.
//! Since these sinks don't produce output we can query, we verify pipeline execution completes successfully.
//! Ported from crates/streamling/tests/pipeline.rs (test_print_sink_*, test_blackhole_sink_*)
//!
//! Note: The original tests verified Prometheus metrics, which are not available in the new e2e system.
//! These tests focus on verifying pipeline execution correctness.

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
// Scenario 1: Basic print sink
// ============================================================================

/// Basic test: read records from Kafka and write to print sink
/// Ported from: test_print_sink_metrics
#[tokio::test]
async fn test_print_sink_basic() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Register schema for input topic
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

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

    // Run pipeline: Kafka source → Print sink
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
  print_sink:
    type: print
    from: kafka_source
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .timeout(std::time::Duration::from_secs(60)), // Longer timeout for graceful shutdown
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");
}

// ============================================================================
// Scenario 2: Print sink with early exit
// ============================================================================

/// Test that print sink exits early when record limit is reached before all input is processed
/// Ported from: test_print_sink_exits_early_with_more_input_to_process
#[tokio::test]
async fn test_print_sink_early_exit() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce more records than we'll process
    let records_to_produce = 100;
    let stop_at = 50;

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

    // Run pipeline with early stop
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
  print_sink:
    type: print
    from: kafka_source
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline(&pipeline, stop_at)
        .await
        .expect("Streamling execution failed");

    assert!(
        status.success(),
        "Streamling should exit successfully even with early stop"
    );
}

// ============================================================================
// Scenario 3: Print sink with multiple batches
// ============================================================================

/// Test print sink with enough records to span multiple batches
/// Ported from: test_print_sink_metrics_with_multiple_batch
#[tokio::test]
async fn test_print_sink_multiple_batches() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

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
  print_sink:
    type: print
    from: kafka_source
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
}

// ============================================================================
// Scenario 4: Blackhole sink with multiple batches
// ============================================================================

/// Test blackhole sink - data is consumed but not written anywhere
/// Ported from: test_blackhole_sink_multi_batch_metrics
#[tokio::test]
async fn test_blackhole_sink() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce records
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

    // Run pipeline: Kafka source → Blackhole sink
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
  blackhole_sink:
    type: blackhole
    from: kafka_source
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
}
