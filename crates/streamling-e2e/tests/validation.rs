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

// ============================================================================
// STRM-6281: Kafka sink requires a resolvable primary key
// ============================================================================

/// A Kafka sink whose upstream has no primary key — none in the sink config, none on
/// the source, and none discoverable from the Avro schema — must fail validation up
/// front instead of crashlooping at runtime with `primary key column '' not found in
/// batch`. This mirrors the abstract-dataset-with-no-CMS-primary-key scenario, where
/// the preprocessor emits `primary_key: ""` that used to flow through to the sink.
#[tokio::test]
async fn test_kafka_sink_without_primary_key_fails_validation() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // TEST_SCHEMA has no `doc` with primaryKeys, so nothing is discovered from Avro.
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

    let output_topic = ctx
        .create_kafka_topic("output")
        .await
        .expect("Failed to create output topic");

    // No primary_key on the source OR the sink, and no embedded Avro primary key:
    // there is nothing to resolve or propagate to the Kafka sink.
    let pipeline = format!(
        r#"
sources:
  test_kafka_source:
    type: kafka
    topic: {input_topic}
    starting_offsets: earliest

transforms: {{}}

sinks:
  kafka_sink:
    type: kafka
    from: test_kafka_source
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
            PipelineOpts::new().record_limit(5).arg("--validate"),
        )
        .await
        .expect("Failed to run pipeline");

    assert!(
        !output.status.success(),
        "Kafka sink without a resolvable primary key should exit non-zero under --validate"
    );

    let validation: ValidationOutput = serde_json::from_str(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "Failed to parse validation JSON from stdout: {}\nstdout was:\n{}\nstderr was:\n{}",
            e, output.stdout, output.stderr
        )
    });

    assert!(
        !validation.is_valid,
        "Pipeline with an unresolvable Kafka sink primary key should be invalid, got: {:?}",
        validation
    );
    // Attributed as a platform error (internal), since every dataset is expected to
    // declare a primary key — so `--validate` reports success: false.
    assert!(
        !validation.success,
        "A missing Kafka sink primary key is a platform (internal) error, so success should be false, got: {:?}",
        validation
    );

    let all_errors = validation.errors.join("\n");
    assert!(
        all_errors.contains("kafka sink 'kafka_sink' requires a primary key"),
        "Errors should mention the Kafka sink primary key requirement, got: {:?}",
        validation.errors
    );
}

// ============================================================================
// Unknown plugin reference must be a validation error, not a panic
// ============================================================================

/// A sink referencing a plugin type that is not present in the plugin bundle
/// used to make the validator `panic!("Plugin <name> not found!")` — exiting 101
/// with empty stdout, which the control plane then misreported. `--validate` must
/// instead emit structured JSON reporting the pipeline as invalid.
#[tokio::test]
async fn test_validate_unknown_plugin_is_error_not_panic() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (0..3)
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

    // `definitely_not_a_real_plugin` is not a built-in sink type, so it binds to the
    // plugin sink variant and is resolved against the (empty) plugin registry.
    let pipeline = format!(
        r#"
sources:
  test_kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  missing_plugin_sink:
    type: definitely_not_a_real_plugin
    from: test_kafka_source
    primary_key: id
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_raw(
            &pipeline,
            PipelineOpts::new().record_limit(3).arg("--validate"),
        )
        .await
        .expect("Failed to run pipeline");

    // The key regression: stdout must be parseable JSON (the validator did not panic).
    let validation: ValidationOutput = serde_json::from_str(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "Failed to parse validation JSON from stdout (validator likely panicked): {}\nstdout was:\n{}\nstderr was:\n{}",
            e, output.stdout, output.stderr
        )
    });

    assert!(
        !validation.is_valid,
        "Pipeline referencing an unknown plugin must be invalid, got: {:?}",
        validation
    );
    assert!(
        !validation.errors.is_empty(),
        "Expected at least one error entry, got: {:?}",
        validation
    );

    let all_errors = validation.errors.join("\n");
    assert!(
        all_errors.contains("definitely_not_a_real_plugin")
            && all_errors.contains("is not available"),
        "Error should name the missing plugin, got: {:?}",
        validation.errors
    );
}

// ============================================================================
// STRM-5695: u256 comparison combined with boolean predicate via AND/OR
// ============================================================================

/// Schema with a string field that will be converted to u256 via SQL transform,
/// plus a string field used in a boolean comparison.
const U256_FILTER_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TraceRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "call_type", "type": "string"},
        {"name": "value_str", "type": "string"}
    ]
}"#;

#[derive(Debug, Clone, Serialize)]
struct TraceRecord {
    id: i64,
    call_type: String,
    value_str: String,
}

/// Regression test for STRM-5695: the bigint SQL preprocessor used to treat
/// comparison and logical operators (AND, OR, >, <, =, <>) as bigint-producing,
/// wrapping non-u256 operands with `to_u256()` and causing type errors.
///
/// This test uses a two-transform chain:
///   1. Convert a string column to u256
///   2. Filter with `call_type <> 'delegatecall' AND amount > 0`
///
/// Prior to the fix, the preprocessor would wrap `call_type` with `to_u256()`
/// in the AND expression, producing a type error that only surfaced during
/// physical plan creation. The `--validate` flag now creates physical plans,
/// so this test also exercises the validation improvement.
#[tokio::test]
async fn test_validate_u256_comparison_with_boolean_predicate() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(U256_FILTER_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TraceRecord> = vec![
        TraceRecord {
            id: 1,
            call_type: "call".to_string(),
            value_str: "1000".to_string(),
        },
        TraceRecord {
            id: 2,
            call_type: "delegatecall".to_string(),
            value_str: "500".to_string(),
        },
        TraceRecord {
            id: 3,
            call_type: "call".to_string(),
            value_str: "0".to_string(),
        },
    ];

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Two-transform chain: first creates a u256 column, second filters using
    // a boolean predicate (string comparison) AND a u256 comparison.
    // The second transform's input schema has `amount` as u256, which triggers
    // the bigint SQL preprocessor.
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  with_amount:
    type: sql
    sql: "SELECT id, call_type, to_u256(value_str) as amount FROM kafka_source"
    primary_key: id

  filtered:
    type: sql
    sql: "SELECT id, call_type, amount FROM with_amount WHERE call_type <> 'delegatecall' AND amount > 0"
    primary_key: id

sinks:
  blackhole_sink:
    type: blackhole
    from: filtered
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_raw(
            &pipeline,
            PipelineOpts::new().record_limit(3).arg("--validate"),
        )
        .await
        .expect("Failed to run pipeline");

    let validation: ValidationOutput = serde_json::from_str(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "Failed to parse validation JSON from stdout: {}\nstdout was:\n{}\nstderr was:\n{}",
            e, output.stdout, output.stderr
        )
    });

    assert!(
        validation.success,
        "Validation should have run successfully, got errors: {:?}",
        validation.errors
    );
    assert!(
        validation.is_valid,
        "Pipeline should be valid — u256 comparison combined with boolean predicate \
         must not corrupt the SQL. Errors: {:?}",
        validation.errors
    );
    assert!(
        validation.errors.is_empty(),
        "Expected no validation errors, got: {:?}",
        validation.errors
    );
    assert!(
        output.status.success(),
        "Pipeline should exit zero when validation passes.\nstdout:\n{}\nstderr:\n{}",
        output.stdout,
        output.stderr
    );
}

// ============================================================================
// postgres_aggregate sink must not connect to Postgres during --validate
// ============================================================================

/// Test that `--validate` passes for a `postgres_aggregate` sink without a
/// reachable Postgres.
///
/// Secrets are not resolved during dry-run validation, so the validator may
/// have no usable DB credentials. The aggregation sink used to eagerly create
/// its trigger/tables during plan building, which made every
/// postgres_aggregate pipeline fail validation with "pool timed out while
/// waiting for an open connection" regardless of the target database.
#[tokio::test]
async fn test_validate_postgres_aggregate_sink_without_postgres() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (0..3)
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
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  postgres_agg:
    type: postgres_aggregate
    from: test_kafka_source
    landing_table: blocks_landing
    agg_table: block_totals
    schema: streamling
    primary_key: id
    group_by:
      data:
        type: text
    aggregate:
      total_blocks:
        from: block
        fn: sum
"#,
        topic = ctx.kafka_topic
    );

    // Point the sink at an address where nothing listens: validation must not
    // need Postgres at all. The short acquire timeout keeps this test fast if
    // the connect-during-validate regression ever comes back.
    let output = ctx
        .run_pipeline_raw(
            &pipeline,
            PipelineOpts::new()
                .arg("--validate")
                .env("STREAMLING__POSTGRES_SINK__HOST", "127.0.0.1")
                .env("STREAMLING__POSTGRES_SINK__PORT", "9")
                .env("STREAMLING__POSTGRES_SINK__POOL_ACQUIRE_TIMEOUT_SECS", "2"),
        )
        .await
        .expect("Failed to run pipeline");

    let validation: ValidationOutput = serde_json::from_str(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "Failed to parse validation JSON from stdout: {}\nstdout was:\n{}\nstderr was:\n{}",
            e, output.stdout, output.stderr
        )
    });

    assert!(
        validation.success && validation.is_valid && validation.errors.is_empty(),
        "postgres_aggregate pipeline should validate without a reachable Postgres, \
         got errors: {:?}\nstderr:\n{}",
        validation.errors,
        output.stderr
    );
    assert!(
        output.status.success(),
        "Pipeline should exit zero when validation passes.\nstdout:\n{}\nstderr:\n{}",
        output.stdout,
        output.stderr
    );
}
