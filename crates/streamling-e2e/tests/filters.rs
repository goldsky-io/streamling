//! Kafka source filter e2e tests.
//!
//! These tests verify that streamling can correctly apply filters at the Kafka source level.
//! Ported from crates/streamling/tests/pipeline.rs (test_kafka_source_with_filter, etc.)
//!
//! Note: The original tests used MemorySink. These have been converted to use PostgreSQL sink.

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

// ============================================================================
// Test Record Types
// ============================================================================

/// Test record with a block number for filtering
#[derive(Debug, Clone, Serialize)]
struct BlockRecord {
    id: i64,
    block: i64,
    data: String,
}

const BLOCK_SCHEMA: &str = r#"{
    "type": "record",
    "name": "BlockRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "block", "type": "long"},
        {"name": "data", "type": "string"}
    ]
}"#;

// ============================================================================
// Scenario 1: Basic numeric filter
// ============================================================================

/// Test Kafka source with a numeric filter (block > 60)
/// Ported from: test_kafka_source_with_filter
#[tokio::test]
async fn test_kafka_source_filter_basic() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(BLOCK_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce 100 records with blocks 1-100
    let records_to_produce: i64 = 100;
    let records: Vec<BlockRecord> = (1..=records_to_produce)
        .map(|i| BlockRecord {
            id: i,
            block: i,
            data: format!("data_{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Run pipeline with filter "block > 60" - should pass 40 records (61-100)
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    filter: "block > 60"

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: filter_basic_test
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    // Process enough records - filter runs at source level so we may need to process more
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(40) // Expect 40 records to pass through
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify only filtered records made it through
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.filter_basic_test")
        .await
        .expect("Failed to query count");

    assert_eq!(count, 40, "Filter 'block > 60' should pass 40 records");

    // Verify min block is 61
    let min_block: Vec<(i64,)> = ctx
        .postgres
        .query("SELECT MIN(block) FROM public.filter_basic_test")
        .await
        .expect("Failed to query min block");

    assert_eq!(min_block[0].0, 61, "Minimum block should be 61");
}

// ============================================================================
// Scenario 2: Filter with partial matches
// ============================================================================

/// Test Kafka source filter where some records match
/// Ported from: test_kafka_source_with_filter_no_matches (modified)
#[tokio::test]
async fn test_kafka_source_filter_partial_matches() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(BLOCK_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce 10 records with blocks 1-10
    let records_to_produce: i64 = 10;
    let records: Vec<BlockRecord> = (1..=records_to_produce)
        .map(|i| BlockRecord {
            id: i,
            block: i,
            data: format!("data_{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Run pipeline with filter "block > 5" - should pass 5 records (6-10)
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    filter: "block > 5"

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: filter_partial_test
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(5)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify 5 records made it through
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.filter_partial_test")
        .await
        .expect("Failed to query count");

    assert_eq!(count, 5, "Filter 'block > 5' should pass 5 records");
}

// ============================================================================
// Scenario 3: String comparison filter
// ============================================================================

/// Test Kafka source with a string comparison filter
/// Ported from: test_kafka_source_filter_with_string_comparison
#[tokio::test]
async fn test_kafka_source_filter_string() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(BLOCK_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce records with varying data values
    // Half with "match" data, half with other data
    let records_to_produce: i64 = 100;
    let records: Vec<BlockRecord> = (1..=records_to_produce)
        .map(|i| BlockRecord {
            id: i,
            block: i,
            data: if i % 2 == 0 {
                "target_data".to_string()
            } else {
                format!("other_{}", i)
            },
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Run pipeline with string filter - should pass 50 records
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    filter: "data = 'target_data'"

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: filter_string_test
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(50)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify 50 records made it through
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.filter_string_test")
        .await
        .expect("Failed to query count");

    assert_eq!(
        count, 50,
        "Filter should pass 50 records with 'target_data'"
    );

    // Verify all records have the expected data value
    let all_match = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.filter_string_test WHERE data = 'target_data'")
        .await
        .expect("Failed to query matching records");

    assert_eq!(
        all_match, 50,
        "All records should have data = 'target_data'"
    );
}
