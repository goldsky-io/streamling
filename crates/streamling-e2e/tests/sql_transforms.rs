//! SQL transform e2e tests.
//!
//! These tests verify SQL transform behavior including:
//! - _gs_op column propagation and preservation
//! - UNION query handling
//! - Flink-compatible string functions
//! - SQL filter metrics
//!
//! Ported from crates/streamling/tests/pipeline.rs

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

// ============================================================================
// Test Record Types
// ============================================================================

/// Test record matching the standard test schema (block, id, data)
#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    block: i64,
    id: String,
    data: String,
}

const TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TestMessage",
    "fields": [
        {"name": "block", "type": "long"},
        {"name": "id", "type": "string"},
        {"name": "data", "type": "string"}
    ]
}"#;

// ============================================================================
// Helper functions
// ============================================================================

fn create_test_records(count: usize) -> Vec<TestRecord> {
    (1..=count)
        .map(|i| TestRecord {
            block: i as i64,
            id: format!("id_{}", i),
            data: format!("data{}", i),
        })
        .collect()
}

// ============================================================================
// Scenario 1: _gs_op propagation tests
// ============================================================================

/// Test that _gs_op is auto-propagated when SQL transform doesn't explicitly select it
#[tokio::test]
async fn test_sql_transform_propagates_gs_op_when_missing() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Register schema and produce test data
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records = create_test_records(10);
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline: Kafka source -> SQL transform (omitting _gs_op) -> print sink
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  sql_transform:
    type: sql
    sql: "SELECT id, data FROM kafka_source"
    primary_key: id

sinks:
  print_sink:
    type: print
    from: sql_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(10))
        .await
        .expect("Pipeline should complete successfully");

    // Verify _gs_op was auto-propagated into the output
    assert!(
        output.has_column("_gs_op"),
        "_gs_op should be present in output schema even when not explicitly selected. Got columns: {:?}",
        output.column_names()
    );

    // Verify we got the expected number of rows
    assert_eq!(output.len(), 10, "Should have processed 10 records");
}

/// Test that explicitly selecting _gs_op preserves it (no duplication)
#[tokio::test]
async fn test_sql_transform_preserves_existing_gs_op() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records = create_test_records(10);
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline: SQL transform explicitly includes _gs_op
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  sql_transform:
    type: sql
    sql: "SELECT id, data, _gs_op FROM kafka_source"
    primary_key: id

sinks:
  print_sink:
    type: print
    from: sql_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(10))
        .await
        .expect("Pipeline should complete successfully");

    // Verify _gs_op is present exactly once (no duplication)
    let columns = output.column_names();
    let gs_op_count = columns.iter().filter(|c| *c == "_gs_op").count();
    assert_eq!(
        gs_op_count, 1,
        "_gs_op should appear exactly once in schema"
    );
}

// ============================================================================
// Scenario 2: UNION query _gs_op handling
// ============================================================================

/// Test that _gs_op is propagated across UNION ALL when not explicitly selected
#[tokio::test]
async fn test_sql_union_propagates_gs_op_when_missing() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records = create_test_records(10);
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline: UNION ALL without _gs_op in either branch
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  sql_transform:
    type: sql
    sql: "SELECT id, data FROM kafka_source UNION ALL SELECT id, data FROM kafka_source"
    primary_key: id

sinks:
  print_sink:
    type: print
    from: sql_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(20)) // 10 records * 2 (UNION ALL)
        .await
        .expect("Pipeline should complete successfully");

    // Verify _gs_op was auto-propagated across the union
    assert!(
        output.has_column("_gs_op"),
        "_gs_op should be present in output schema for UNION"
    );

    // UNION ALL should double the records
    assert_eq!(output.len(), 20, "UNION ALL should produce 20 records");
}

/// Test that explicitly selecting _gs_op in UNION branches preserves it once
#[tokio::test]
async fn test_sql_union_preserves_existing_gs_op() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records = create_test_records(10);
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline: UNION ALL with _gs_op explicitly in both branches
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  sql_transform:
    type: sql
    sql: "SELECT id, data, _gs_op FROM kafka_source UNION ALL SELECT id, data, _gs_op FROM kafka_source"
    primary_key: id

sinks:
  print_sink:
    type: print
    from: sql_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(20))
        .await
        .expect("Pipeline should complete successfully");

    // Verify _gs_op is present exactly once (no duplication)
    let columns = output.column_names();
    let gs_op_count = columns.iter().filter(|c| *c == "_gs_op").count();
    assert_eq!(
        gs_op_count, 1,
        "_gs_op should appear exactly once in schema for UNION"
    );
}

// ============================================================================
// Scenario 3: Flink string function compatibility
// ============================================================================

/// Test that Flink-compatible string functions work in SQL transforms
#[tokio::test]
async fn test_sql_transform_uses_flink_string_functions() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records = create_test_records(10);
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline: SQL transform using Flink string functions
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  sql_transform:
    type: sql
    sql: |
      SELECT
        id,
        data,
        _gs_op,
        charLength(data) AS data_len,
        TRANSLATE(data || 'a', 'a', 'z') AS translated,
        REGEXP(data, '^data') AS matches_prefix
      FROM kafka_source
    primary_key: id

sinks:
  print_sink:
    type: print
    from: sql_transform
    sample_every: 1
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(10))
        .await
        .expect("Pipeline should complete successfully");

    // Verify computed columns exist
    assert!(
        output.has_column("data_len"),
        "data_len column should exist"
    );
    assert!(
        output.has_column("translated"),
        "translated column should exist"
    );
    assert!(
        output.has_column("matches_prefix"),
        "matches_prefix column should exist"
    );

    // Verify we got results
    assert!(!output.is_empty(), "Should have processed records");

    // Verify charLength works correctly
    for row in output.rows() {
        if let Some(data) = row.data.get("data").and_then(|v| v.as_str()) {
            if let Some(data_len) = row.data.get("data_len").and_then(|v| v.as_i64()) {
                assert_eq!(
                    data_len as usize,
                    data.len(),
                    "charLength should match actual string length"
                );
            }
        }
    }
}

// ============================================================================
// Scenario 4: SQL filter with metrics verification
// ============================================================================

/// Test that SQL WHERE filter shows different input/output row counts in metrics
#[tokio::test]
async fn test_pipeline_sql_filter_diff_in_input_output_rows() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_prometheus())
        .await
        .expect("Failed to create test context");

    let prometheus = ctx
        .prometheus
        .as_ref()
        .expect("Prometheus should be available");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Create 100 records with block values 1-100
    let records = create_test_records(100);
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline: Filter where block % 2 = 0 (should pass ~50 records)
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  sql_transform:
    type: sql
    sql: "SELECT id, data, _gs_op FROM kafka_source WHERE block % 2 = 0"
    primary_key: id

sinks:
  blackhole_sink:
    type: blackhole
    from: sql_transform
"#,
        topic = ctx.kafka_topic
    );

    let _status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new().record_limit(50), // Expecting ~50 records to pass filter
        )
        .await
        .expect("Pipeline should complete successfully");

    // Build queries for this test's instance
    use streamling_e2e::resources::PrometheusResource;
    let input_query = PrometheusResource::input_rows_query("sql_transform", Some(&ctx.test_id));
    let output_query = PrometheusResource::output_rows_query("sql_transform", Some(&ctx.test_id));

    // Verify input rows metric (should be ~100, all records processed)
    let input_rows = prometheus
        .wait_for_metric_at_least(&input_query, 50, 10, 500)
        .await;
    assert!(
        input_rows.is_ok(),
        "Should have input rows metric for sql_transform: {:?}",
        input_rows
    );

    // Verify output rows metric (should be ~50, only filtered records)
    let output_rows = prometheus
        .wait_for_metric_at_least(&output_query, 50, 10, 500)
        .await;
    assert!(
        output_rows.is_ok(),
        "Should have output rows metric for sql_transform: {:?}",
        output_rows
    );
}

/// SQL transform `elapsed_compute` is the DataFusion subtree **delta** path
/// (wall-clock around `next().await` is suppressed when the SQL plan exposes
/// metrics). Must exceed the 1ms series seed so this fails if only the seed
/// or wrapping idle-wait were recorded.
#[tokio::test]
async fn test_sql_transform_elapsed_compute_emitted() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_prometheus())
        .await
        .expect("Failed to create test context");

    let prometheus = ctx
        .prometheus
        .as_ref()
        .expect("Prometheus should be available");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Enough rows of a CPU-ish projection that DataFusion's elapsed_compute
    // on the SQL subtree exceeds the 1ms per-node series seed.
    let records = create_test_records(50);
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
    primary_key: id

transforms:
  sql_transform:
    type: sql
    sql: "SELECT id, md5(repeat(data, 8000)) AS h, _gs_op FROM kafka_source"
    primary_key: id

sinks:
  blackhole_sink:
    type: blackhole
    from: sql_transform
"#,
        topic = ctx.kafka_topic
    );

    let _status = ctx
        .run_pipeline_with_opts(&pipeline, PipelineOpts::new().record_limit(50))
        .await
        .expect("Pipeline should complete successfully");

    use streamling_e2e::resources::PrometheusResource;
    let elapsed_query =
        PrometheusResource::elapsed_compute_query("sql_transform", Some(&ctx.test_id));

    // Seed records 1ms; the compute-bound `md5(repeat(...))` projection is
    // SQL subtree elapsed_compute (WrappingExec suppresses wall-clock idle-wait
    // when DF metrics exist). Require a strictly larger sum so this fails if
    // we only ever emit the seed.
    let elapsed = prometheus
        .wait_for_metric_at_least(&elapsed_query, 2, 15, 500)
        .await;
    assert!(
        elapsed.is_ok(),
        "sql_transform elapsed_compute must exceed the 1ms series seed via subtree deltas (wall-clock suppressed for SQL): {:?}",
        elapsed
    );

    // Histogram sample count must stay small: seed (1) + per-batch *deltas*,
    // not seed + wall-clock-per-batch + deltas. 50 rows in default batches
    // plus seed is well under a wall-clock-per-row inflation.
    let count_query = format!(
        "streamling_elapsed_compute_milliseconds_count{{id=\"sql_transform\",instance=\"{}\"}}",
        ctx.test_id
    );
    let sample_count = prometheus
        .query_count(&count_query)
        .await
        .expect("query elapsed_compute count")
        .expect("elapsed_compute _count series must exist after seed + compute");
    assert!(
        sample_count < 50,
        "SQL elapsed_compute sample count {sample_count} looks like wall-clock-per-batch; expected seed + subtree deltas only ({count_query})"
    );
}

// ============================================================================
// Scenario 5: Chained SQL transforms with comments containing apostrophes
// ============================================================================

/// Regression test: SQL comments with apostrophes (e.g. `-- don't`) must not
/// break the topology sort that determines transform execution order.
/// Previously, apostrophes in comments confused the hand-rolled string-literal
/// stripper, causing downstream transforms to fail with "table not found".
#[tokio::test]
async fn test_chained_sql_transforms_with_comment_apostrophes() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records = create_test_records(10);
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline: three chained SQL transforms where the middle one has
    // comments containing apostrophes.  The topology sort must correctly
    // detect that step2 depends on step1 and step3 depends on step2.
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  step1:
    type: sql
    sql: |
      SELECT
        id,
        block,
        -- don't remove this comment — it has apostrophes
        CONCAT('prefix_', data) AS prefixed_data
      FROM kafka_source
    primary_key: id

  step2:
    type: sql
    sql: |
      SELECT
        id,
        block,
        -- it's important that this comment doesn't break parsing
        prefixed_data,
        block * 2 AS double_block
      FROM step1
      WHERE block > 0
    primary_key: id

  step3:
    type: sql
    sql: "SELECT id, double_block, prefixed_data FROM step2"
    primary_key: id

sinks:
  print_sink:
    type: print
    from: step3
    sample_every: 1
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_with_capture(&pipeline, PipelineOpts::new().record_limit(10))
        .await
        .expect("Pipeline should complete — apostrophes in comments must not break topology sort");

    assert!(
        output.has_column("double_block"),
        "step2's computed column should propagate through step3"
    );
    assert!(
        output.has_column("prefixed_data"),
        "step1's computed column should propagate through"
    );
    assert_eq!(output.len(), 10, "Should have processed all 10 records");
}
