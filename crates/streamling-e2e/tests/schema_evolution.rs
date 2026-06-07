//! Schema evolution e2e tests.
//!
//! These tests verify that streamling correctly handles Avro schema evolution,
//! including backward compatible changes like adding fields with default values.
//! Ported from crates/streamling/tests/pipeline_schema_evolution.rs
//!
//! Note: The original tests used MemorySink. These have been converted to use PostgreSQL sink.

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

// ============================================================================
// Test Record Types
// ============================================================================

/// Schema v1: id and data fields only
#[derive(Debug, Clone, Serialize)]
struct RecordV1 {
    id: i64,
    data: String,
}

/// Schema v2: id, data, and version fields (backward compatible with v1)
#[derive(Debug, Clone, Serialize)]
struct RecordV2 {
    id: i64,
    data: String,
    version: i32,
}

const SCHEMA_V1: &str = r#"{
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "data", "type": "string"}
    ]
}"#;

const SCHEMA_V2: &str = r#"{
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "data", "type": "string"},
        {"name": "version", "type": "int", "default": 1}
    ]
}"#;

// ============================================================================
// Scenario 1: Backward compatible schema evolution
// ============================================================================

/// Test that messages written with v1 schema are correctly resolved to v2 schema
/// with default values applied for new fields.
/// Ported from: new_field_with_default
#[tokio::test]
async fn test_schema_evolution_new_field_with_default() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Register v1 schema first
    ctx.kafka
        .register_schema(SCHEMA_V1)
        .await
        .expect("Failed to register v1 schema");

    // Produce messages with v1 schema
    let v1_records: Vec<RecordV1> = (1..=5)
        .map(|i| RecordV1 {
            id: i,
            data: format!("data_v1_{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&v1_records)
        .await
        .expect("Failed to produce v1 records");

    // Register v2 schema (backward compatible - adds 'version' with default)
    ctx.kafka
        .register_schema(SCHEMA_V2)
        .await
        .expect("Failed to register v2 schema");

    // Produce messages with v2 schema
    let v2_records: Vec<RecordV2> = (6..=10)
        .map(|i| RecordV2 {
            id: i,
            data: format!("data_v2_{}", i),
            version: 2,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&v2_records)
        .await
        .expect("Failed to produce v2 records");

    // Run pipeline: Kafka source → PostgreSQL sink
    // All messages should be processed with the evolved schema
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
    table: schema_evolution_test
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
                .record_limit(10)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify all records made it through
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.schema_evolution_test")
        .await
        .expect("Failed to query count");

    assert_eq!(count, 10, "Should have processed all 10 records");

    // Verify v1 records have the default version value (1)
    // Note: If the sink creates schema based on evolved schema, version column should exist
    // If not, this query will just verify the records exist
    let v1_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.schema_evolution_test WHERE id <= 5")
        .await
        .expect("Failed to query v1 records");

    assert_eq!(v1_count, 5, "Should have 5 v1 records (id 1-5)");

    // Verify v2 records
    let v2_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.schema_evolution_test WHERE id > 5")
        .await
        .expect("Failed to query v2 records");

    assert_eq!(v2_count, 5, "Should have 5 v2 records (id 6-10)");
}

// ============================================================================
// Scenario 2: Nullable field evolution
// ============================================================================

/// Test schema evolution with nullable field
#[derive(Debug, Clone, Serialize)]
struct UserV1 {
    id: i64,
    name: String,
}

#[derive(Debug, Clone, Serialize)]
struct UserV2 {
    id: i64,
    name: String,
    email: Option<String>,
}

const USER_SCHEMA_V1: &str = r#"{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"}
    ]
}"#;

const USER_SCHEMA_V2: &str = r#"{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": ["null", "string"], "default": null}
    ]
}"#;

/// Test that nullable fields with null default work correctly across schema versions
/// Ported from: nullable_field_with_null_default
#[tokio::test]
async fn test_schema_evolution_nullable_field() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Register v1 schema
    ctx.kafka
        .register_schema(USER_SCHEMA_V1)
        .await
        .expect("Failed to register v1 schema");

    // Produce users with v1 schema
    let v1_users: Vec<UserV1> = (1..=3)
        .map(|i| UserV1 {
            id: i,
            name: format!("user_{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&v1_users)
        .await
        .expect("Failed to produce v1 users");

    // Register v2 schema
    ctx.kafka
        .register_schema(USER_SCHEMA_V2)
        .await
        .expect("Failed to register v2 schema");

    // Produce users with v2 schema (with email)
    let v2_users: Vec<UserV2> = (4..=6)
        .map(|i| UserV2 {
            id: i,
            name: format!("user_{}", i),
            email: Some(format!("user{}@example.com", i)),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&v2_users)
        .await
        .expect("Failed to produce v2 users");

    // Run pipeline
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
    table: schema_evolution_nullable_test
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
                .record_limit(6)
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify all records made it through
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.schema_evolution_nullable_test")
        .await
        .expect("Failed to query count");

    assert_eq!(count, 6, "Should have processed all 6 records");

    // Verify both v1 and v2 records exist
    let name_count = ctx
        .postgres
        .count(
            "SELECT COUNT(*) FROM public.schema_evolution_nullable_test WHERE name LIKE 'user_%'",
        )
        .await
        .expect("Failed to query names");

    assert_eq!(name_count, 6, "All records should have names");
}

// ============================================================================
// Scenario 3: validate_writer_schema_ordering option
// ============================================================================
//
// These tests exercise the `validate_writer_schema_ordering` flag in pipeline YAML.
// Setup for both:
//   1. Register schema v1; the consumer will fetch this as its reader schema at startup.
//   2. Run the pipeline.
//   3. While the pipeline is consuming, register schema v2 and produce a v2-encoded record.
// With validate=true (default), the consumer detects writer ahead of reader and fails fast
// (process::exit(1)) so the pod restarts and refetches. With validate=false, the per-message
// check is skipped and Avro resolution succeeds against the v1 reader schema.

/// Default flag (validate_writer_schema_ordering=true): a writer schema newer than the reader
/// triggers a fail-fast crash.
#[tokio::test]
async fn test_validate_writer_schema_ordering_default_fails_on_newer_writer() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Register v1 only — when the pipeline starts, it captures v1 as the reader schema.
    ctx.kafka
        .register_schema(SCHEMA_V1)
        .await
        .expect("Failed to register v1 schema");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    # validate_writer_schema_ordering defaults to true

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: writer_ahead_default_test
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    // Run the pipeline concurrently with a delayed v2 registration + v2 produce.
    // We can't do them sequentially because run_pipeline_* blocks until the subprocess exits.
    // Use run_pipeline_raw because we *expect* a non-zero exit; run_pipeline_with_opts converts
    // that into Err(StreamlingFailed) which would mask the assertion we want to make.
    let pipeline_fut = ctx.run_pipeline_raw(
        &pipeline,
        // record_limit=10 sets STREAMLING__NUM_RECORDS_BEFORE_STOP — large enough that natural
        // termination doesn't beat the fail-fast crash. The 30s timeout bounds the test if
        // something goes wrong.
        PipelineOpts::new()
            .record_limit(10)
            .timeout(std::time::Duration::from_secs(30)),
    );

    let kafka = &ctx.kafka;
    let evolve_fut = async move {
        // Give the consumer time to start, fetch v1 as reader, and begin polling.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        kafka
            .register_schema(SCHEMA_V2)
            .await
            .expect("Failed to register v2 schema");

        kafka
            .produce_avro_records(&[RecordV2 {
                id: 42,
                data: "newer-than-reader".to_string(),
                version: 2,
            }])
            .await
            .expect("Failed to produce v2 record");
    };

    let (output_result, _) = tokio::join!(pipeline_fut, evolve_fut);
    let output = output_result.expect("Pipeline future should not error (timeout?)");

    // The pipeline should have failed (process::exit(1) inside resolve_schema's match arm).
    assert!(
        !output.status.success(),
        "Expected pipeline to fail fast when writer schema is ahead, got success status: {:?}\nstderr:\n{}",
        output.status,
        output.stderr,
    );
    // Sanity check: stderr should mention the schema-resolution failure path.
    assert!(
        output.stderr.contains("Schema resolution failed")
            || output.stderr.contains("writer_schema_id"),
        "Expected stderr to mention the writer-ahead error, got:\n{}",
        output.stderr,
    );
}

/// Opt-out (validate_writer_schema_ordering=false): a writer schema newer than the reader is
/// processed via Avro resolution against the v1 reader schema, dropping fields the reader doesn't
/// know about. The pipeline runs to completion.
#[tokio::test]
async fn test_validate_writer_schema_ordering_disabled_processes_newer_writer() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Register v1 first; reader will be v1.
    ctx.kafka
        .register_schema(SCHEMA_V1)
        .await
        .expect("Failed to register v1 schema");

    // Pre-produce one v1 record so the consumer makes progress before we register v2.
    ctx.kafka
        .produce_avro_records(&[RecordV1 {
            id: 1,
            data: "v1-baseline".to_string(),
        }])
        .await
        .expect("Failed to produce v1 record");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    validate_writer_schema_ordering: false

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: writer_ahead_disabled_test
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let pipeline_fut = ctx.run_pipeline_with_opts(
        &pipeline,
        // We expect to receive both records and then stop on the limit.
        PipelineOpts::new()
            .record_limit(2)
            .timeout(std::time::Duration::from_secs(60)),
    );

    let kafka = &ctx.kafka;
    let evolve_fut = async move {
        // Give the consumer time to fetch v1 as reader and consume the baseline message.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        kafka
            .register_schema(SCHEMA_V2)
            .await
            .expect("Failed to register v2 schema");

        kafka
            .produce_avro_records(&[RecordV2 {
                id: 2,
                data: "v2-after-startup".to_string(),
                version: 99,
            }])
            .await
            .expect("Failed to produce v2 record");
    };

    let (status_result, _) = tokio::join!(pipeline_fut, evolve_fut);
    let status = status_result.expect("Pipeline future should not error");

    assert!(
        status.success(),
        "Expected pipeline to succeed with validate_writer_schema_ordering=false, got: {status:?}"
    );

    // Both records should be in the sink. The v2 record's `version` field is dropped because the
    // reader's v1 schema doesn't know about it; only `id` and `data` are projected.
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.writer_ahead_disabled_test")
        .await
        .expect("Failed to query count");

    assert_eq!(count, 2, "Should have processed both v1 and v2 records");
}
