//! Validation e2e tests.
//!
//! These tests verify pipeline validation behavior including:
//! - Primary key column validation
//! - Invalid SQL transform detection
//! - Undefined dynamic table detection
//!
//! Ported from crates/streamling/tests/pipeline.rs

use serde::{Deserialize, Serialize};
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

// ============================================================================
// Test Record Types
// ============================================================================

/// Test record with standard fields
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
// Validation Tests
// ============================================================================

/// Test that pipeline fails when primary_key column doesn't exist in schema.
///
/// This validates that Streamling properly checks the primary_key configuration
/// against the actual schema fields before starting the pipeline.
///
/// Ported from: test_pipeline_primary_key_check
#[tokio::test]
async fn test_pipeline_primary_key_check() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Register schema and produce test records
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (0..10)
        .map(|i| TestRecord {
            block: i,
            id: format!("id_{}", i),
            data: format!("data{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Pipeline with non-existent primary key - should fail validation
    let pipeline = format!(
        r#"
sources:
  test_kafka_source:
    type: kafka
    topic: {topic}
    primary_key: does_not_exist

transforms: {{}}

sinks:
  blackhole_sink:
    type: blackhole
    from: test_kafka_source
"#,
        topic = ctx.kafka_topic
    );

    // Run pipeline with raw output to get stderr containing the error message
    let output = ctx
        .run_pipeline_raw(&pipeline, PipelineOpts::new().record_limit(10))
        .await
        .expect("Failed to run pipeline");

    // The pipeline should fail (non-zero exit status)
    assert!(
        !output.status.success(),
        "Pipeline should have failed due to invalid primary key"
    );

    // Check that the error message mentions primary key validation
    let combined_output = format!("{}\n{}", output.stdout, output.stderr);
    assert!(
        combined_output.contains("Primary key validation failed for node 'test_kafka_source'")
            && combined_output.contains("columns [\"does_not_exist\"] not found in schema")
            && combined_output
                .contains("Available columns: [\"block\", \"id\", \"data\", \"_gs_op\"]"),
        "Expected primary key validation error with details, got stdout:\n{}\nstderr:\n{}",
        output.stdout,
        output.stderr
    );
}

/// Test that pipeline fails when a SQL transform contains invalid SQL.
///
/// This validates that Streamling reports a clear error when the SQL cannot be parsed
/// or planned, rather than silently misbehaving.
#[tokio::test]
async fn test_pipeline_invalid_sql_transform() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (0..5)
        .map(|i| TestRecord {
            block: i,
            id: format!("id_{}", i),
            data: format!("data{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"
sources:
  test_kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  bad_sql:
    type: sql
    sql: "SELEKT id, data FORM test_kafka_source"
    primary_key: id

sinks:
  blackhole_sink:
    type: blackhole
    from: bad_sql
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_raw(&pipeline, PipelineOpts::new().record_limit(5))
        .await
        .expect("Failed to run pipeline");

    assert!(
        !output.status.success(),
        "Pipeline should have failed due to invalid SQL"
    );

    let combined_output = format!("{}\n{}", output.stdout, output.stderr);
    assert!(
        combined_output.contains("SQL") || combined_output.contains("sql"),
        "Error should mention SQL, got stdout:\n{}\nstderr:\n{}",
        output.stdout,
        output.stderr
    );
}

/// Test that pipeline fails when a SQL transform references a dynamic table
/// that is not defined in the pipeline topology.
///
/// This validates that Streamling detects the use of `dynamic_table_check()` UDFs
/// whose referenced table name does not correspond to any transform in the pipeline.
#[tokio::test]
async fn test_pipeline_undefined_dynamic_table() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (0..5)
        .map(|i| TestRecord {
            block: i,
            id: format!("id_{}", i),
            data: format!("data{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"
sources:
  test_kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms:
  filtered:
    type: sql
    sql: "SELECT id, data FROM test_kafka_source WHERE dynamic_table_check('nonexistent_table', id)"
    primary_key: id

sinks:
  blackhole_sink:
    type: blackhole
    from: filtered
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_raw(&pipeline, PipelineOpts::new().record_limit(5))
        .await
        .expect("Failed to run pipeline");

    assert!(
        !output.status.success(),
        "Pipeline should have failed due to undefined dynamic table"
    );

    let combined_output = format!("{}\n{}", output.stdout, output.stderr);
    assert!(
        combined_output.contains("dynamic table 'nonexistent_table'")
            && combined_output.contains("not defined in pipeline topology"),
        "Error should mention the undefined dynamic table, got stdout:\n{}\nstderr:\n{}",
        output.stdout,
        output.stderr
    );
}

// ============================================================================
// Structured validation output (--validate flag)
// ============================================================================

/// Mirror of the JSON structure emitted by `--validate`.
/// Defined locally because the e2e crate intentionally has no dependency on
/// the streamling binary crate.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ValidationOutput {
    success: bool,
    is_valid: bool,
    errors: Vec<String>,
    warnings: Vec<String>,
}

/// Test that `--validate` produces structured JSON on stdout and exits with
/// code 1 when the pipeline is invalid.
///
/// Uses the same invalid-primary-key pipeline as `test_pipeline_primary_key_check`
/// but verifies the machine-readable JSON contract instead of free-form stderr.
#[tokio::test]
async fn test_pipeline_validate_json_output() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (0..10)
        .map(|i| TestRecord {
            block: i,
            id: format!("id_{}", i),
            data: format!("data{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"
sources:
  test_kafka_source:
    type: kafka
    topic: {topic}
    primary_key: does_not_exist

transforms: {{}}

sinks:
  blackhole_sink:
    type: blackhole
    from: test_kafka_source
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_raw(
            &pipeline,
            PipelineOpts::new().record_limit(10).arg("--validate"),
        )
        .await
        .expect("Failed to run pipeline");

    assert!(
        !output.status.success(),
        "Pipeline with invalid primary key should exit non-zero under --validate"
    );

    // stdout must be valid JSON matching the ValidationOutput schema
    let validation: ValidationOutput = serde_json::from_str(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "Failed to parse validation JSON from stdout: {}\nstdout was:\n{}",
            e, output.stdout
        )
    });

    assert!(
        validation.success,
        "User-facing validation error → success should be true (validation ran)"
    );
    assert!(
        !validation.is_valid,
        "Pipeline has errors → is_valid should be false"
    );
    assert!(
        !validation.errors.is_empty(),
        "Expected at least one error entry"
    );

    let all_errors = validation.errors.join("\n");
    assert!(
        all_errors.contains("Primary key validation failed for node 'test_kafka_source': columns [\"does_not_exist\"] not found in schema. Available columns: [\"block\", \"id\", \"data\", \"_gs_op\"]"),
        "Errors should mention the primary key issue, got: {:?}",
        validation.errors
    );
}

// ============================================================================
// Invalid column in source filter
// ============================================================================

/// Schema whose numeric field is called `block_number`, NOT `number`.
const BLOCK_NUMBER_SCHEMA: &str = r#"{
    "type": "record",
    "name": "BlockRecord",
    "fields": [
        {"name": "block_number", "type": "long"},
        {"name": "id", "type": "string"},
        {"name": "data", "type": "string"}
    ]
}"#;

#[derive(Debug, Clone, Serialize)]
struct BlockRecord {
    block_number: i64,
    id: String,
    data: String,
}

/// Test that a source filter referencing a non-existent column fails validation
/// with a helpful "No field named …" error.
///
/// This simulates the scenario where a dataset source (e.g. `hyperevm.raw_blocks`)
/// is preprocessed into a kafka source and the user-supplied filter references
/// `number` while the actual schema exposes `block_number`.
#[tokio::test]
async fn test_invalid_column_in_source_filter() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(BLOCK_NUMBER_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<BlockRecord> = (0..5)
        .map(|i| BlockRecord {
            block_number: 27535200 + i,
            id: format!("id_{}", i),
            data: format!("data{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"
sources:
  blocks:
    type: kafka
    topic: {topic}
    filter: number > 27535200 and number < 27568220

transforms: {{}}

sinks:
  blackhole_sink:
    type: blackhole
    from: blocks
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_raw(
            &pipeline,
            PipelineOpts::new().record_limit(5).arg("--validate"),
        )
        .await
        .expect("Failed to run pipeline");

    assert!(
        !output.status.success(),
        "Pipeline with invalid filter column should exit non-zero"
    );

    let validation: ValidationOutput = serde_json::from_str(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "Failed to parse validation JSON from stdout: {}\nstdout was:\n{}",
            e, output.stdout
        )
    });

    // This error originates from DataFusion (schema error) which is classified
    // as internal. Ideally filter column errors would be user-facing, but the
    // DataFusion→StreamlingError conversion marks them internal.
    assert!(
        !validation.success,
        "Internal error → success should be false (validation could not complete)"
    );
    assert!(
        !validation.is_valid,
        "Pipeline has errors → is_valid should be false"
    );
    assert!(
        !validation.errors.is_empty(),
        "Expected at least one error entry"
    );

    let all_errors = validation.errors.join("\n");
    assert!(
        all_errors.contains("kafka source 'blocks': failed to create Kafka source"),
        "Error should mention the Kafka source context, got: {:?}",
        validation.errors
    );
    assert!(
        all_errors.contains("No field named number")
            && all_errors.contains("Did you mean 'block_number'"),
        "Error should mention the missing column 'number' and suggest 'block_number', got: {:?}",
        validation.errors
    );
    let schema_error_count = all_errors
        .matches("Schema error: No field named number")
        .count();
    assert_eq!(
        schema_error_count, 1,
        "Schema error should appear exactly once (no duplicates), got: {:?}",
        validation.errors
    );
}

// boolean-predicate regression test removed in feature 002: the bug class
// (bigint SQL preprocessor wrapping non-u256 operands with `to_u256()` on
// boolean / comparison expressions) is structurally impossible after the
// BigIntKind machinery and the `to_u256` / `to_i256` UDFs were deleted.
// Wide-integer columns now flow through decimal_arb + DataFusion's
// `ExprPlanner`, which handles operand classification natively.
