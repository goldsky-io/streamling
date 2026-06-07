//! SQL transform e2e tests.
//!
//! These tests verify that streamling can correctly apply SQL transforms to data.
//! Ported from crates/streamling/tests/pipeline.rs (test_pipeline_end_to_end_with_transforms)
//!
//! Note: The original test used webhook sink and HTTP handler. These have been converted
//! to use PostgreSQL sink for proper e2e verification without external services.

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
    value: String,
}

const BLOCK_SCHEMA: &str = r#"{
    "type": "record",
    "name": "BlockRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "block", "type": "long"},
        {"name": "value", "type": "string"}
    ]
}"#;

// ============================================================================
// Scenario 1: SQL transform with filtering
// ============================================================================

/// Test SQL transform that filters records based on a condition
/// Ported from: test_pipeline_end_to_end_with_transforms
#[tokio::test]
async fn test_sql_transform_filter() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Register schema
    ctx.kafka
        .register_schema(BLOCK_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce records with varying block numbers
    let records_to_produce: i64 = 100;
    let records: Vec<BlockRecord> = (1..=records_to_produce)
        .map(|i| BlockRecord {
            id: i,
            block: i, // block equals id, so filter WHERE block < 51 keeps half
            value: format!("value_{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Run pipeline: Kafka source → SQL transform (filter) → PostgreSQL sink
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  sql_filter:
    type: sql
    sql: "SELECT id, block, value FROM kafka_source WHERE block <= 50"
    primary_key: id

sinks:
  pg_sink:
    type: postgres
    from: sql_filter
    table: transform_filter_test
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    // Filter passes 50 records (block <= 50), so record_limit should be 50
    let expected_filtered_count = 50;
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(expected_filtered_count as u64)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify only filtered records made it through
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.transform_filter_test")
        .await
        .expect("Failed to query count");

    assert_eq!(count, 50, "Should have exactly 50 records (block <= 50)");

    // Verify the highest block is 50
    let max_block: Vec<(i64,)> = ctx
        .postgres
        .query("SELECT MAX(block) FROM public.transform_filter_test")
        .await
        .expect("Failed to query max block");

    assert_eq!(max_block[0].0, 50, "Maximum block should be 50");
}

// ============================================================================
// Scenario 2: SQL transform with column projection
// ============================================================================

/// Test SQL transform that projects/renames columns
#[tokio::test]
async fn test_sql_transform_projection() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(BLOCK_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce test records
    let records_to_produce: i64 = 50;
    let records: Vec<BlockRecord> = (1..=records_to_produce)
        .map(|i| BlockRecord {
            id: i,
            block: i * 10,
            value: format!("original_{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Run pipeline: Kafka source → SQL transform (project/rename) → PostgreSQL sink
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  sql_project:
    type: sql
    sql: "SELECT id, block * 2 as double_block, CONCAT('transformed_', value) as modified_value FROM kafka_source"
    primary_key: id

sinks:
  pg_sink:
    type: postgres
    from: sql_project
    table: transform_projection_test
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
                .record_limit(records_to_produce as u64)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify all records made it through
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.transform_projection_test")
        .await
        .expect("Failed to query count");

    assert_eq!(
        count, records_to_produce,
        "Should have all {} records",
        records_to_produce
    );

    // Verify the transformation was applied (block doubled)
    let sample: Vec<(i64, i64, String)> = ctx
        .postgres
        .query("SELECT id, double_block, modified_value FROM public.transform_projection_test WHERE id = 1")
        .await
        .expect("Failed to query sample");

    assert_eq!(sample.len(), 1, "Should have exactly one record with id=1");
    assert_eq!(sample[0].1, 20, "double_block should be 20 (10 * 2)");
    assert!(
        sample[0].2.starts_with("transformed_"),
        "modified_value should start with 'transformed_'"
    );
}
