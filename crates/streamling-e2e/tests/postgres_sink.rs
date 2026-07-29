//! PostgreSQL sink e2e tests.
//!
//! These tests verify that streamling can correctly read from Kafka and write to PostgreSQL.
//! Ported from crates/streamling/tests/pipeline_postgres_sink.rs

use serde::Serialize;
use std::time::Duration;
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

/// JSONB test record with nested types
#[derive(Debug, Clone, Serialize)]
struct JsonbTestRecord {
    id: i64,
    metadata: Metadata,
    tags: Vec<String>,
    data: String,
}

#[derive(Debug, Clone, Serialize)]
struct Metadata {
    key: String,
    value: i32,
}

const JSONB_SCHEMA: &str = r#"{
    "type": "record",
    "name": "JsonbTestMessage",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "metadata", "type": {"type": "record", "name": "Metadata", "fields": [
            {"name": "key", "type": "string"},
            {"name": "value", "type": "int"}
        ]}},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "data", "type": "string"}
    ]
}"#;

/// Composite primary key test record
#[derive(Debug, Clone, Serialize)]
struct CompositePkRecord {
    id: i64,
    version: i64,
    value: String,
}

const COMPOSITE_PK_SCHEMA: &str = r#"{
    "type": "record",
    "name": "CompositePkTestMessage",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "version", "type": "long"},
        {"name": "value", "type": "string"}
    ]
}"#;

// ============================================================================
// Scenario 1: Basic Kafka to Postgres sink
// ============================================================================

/// Basic test: read records from Kafka and write to PostgreSQL
#[tokio::test]
async fn test_basic_postgres_sink() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=10)
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
  pg_sink:
    type: postgres
    from: kafka_source
    table: test_basic
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline(&pipeline, 10)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_basic")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 10, "Should have 10 records in output table");

    let rows: Vec<(i64, String, i64)> = ctx
        .postgres
        .query("SELECT id, value, timestamp FROM public.test_basic WHERE id = 1")
        .await
        .expect("Failed to query record");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "value_1");
    assert_eq!(rows[0].2, 1001);
}

// ============================================================================
// Scenario 2: Multiple Batches
// ============================================================================

/// Test multiple batch processing
#[tokio::test]
async fn test_multiple_batches() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Create 18 records to span multiple batches
    let records: Vec<TestRecord> = (1..=18)
        .map(|i| TestRecord {
            id: i,
            value: format!("batch_value_{}", i),
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
  pg_sink:
    type: postgres
    from: kafka_source
    table: test_batches
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 10
    batch_flush_interval: 1000ms
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline(&pipeline, 18)
        .await
        .expect("Streamling execution failed");

    assert!(status.success());

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_batches")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 18, "Should have 18 records after multiple batches");
}

// ============================================================================
// Scenario 3: JSONB Types
// ============================================================================

/// Test nested struct and array types stored as JSONB
#[tokio::test]
async fn test_jsonb_types() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(JSONB_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<JsonbTestRecord> = (1..=10)
        .map(|i| JsonbTestRecord {
            id: i,
            metadata: Metadata {
                key: format!("key_{}", i - 1),
                value: ((i - 1) * 10) as i32,
            },
            tags: vec![format!("tag_{}", i - 1), format!("tag_{}", i + 99)],
            data: format!("data_{}", i - 1),
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
  pg_sink:
    type: postgres
    from: kafka_source
    table: test_jsonb
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline(&pipeline, 10)
        .await
        .expect("Streamling execution failed");

    assert!(status.success());

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_jsonb")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 10, "Should have 10 records");

    // Verify JSONB type for metadata column
    let jsonb_check: Vec<(String,)> = ctx
        .postgres
        .query("SELECT pg_typeof(metadata)::text FROM public.test_jsonb LIMIT 1")
        .await
        .expect("Failed to check type");
    assert!(
        jsonb_check[0].0.contains("jsonb"),
        "metadata should be JSONB type"
    );

    // Verify JSONB type for tags column
    let tags_check: Vec<(String,)> = ctx
        .postgres
        .query("SELECT pg_typeof(tags)::text FROM public.test_jsonb LIMIT 1")
        .await
        .expect("Failed to check type");
    assert!(
        tags_check[0].0.contains("jsonb"),
        "tags should be JSONB type"
    );
}

// ============================================================================
// Scenario 4: Deduplication (Upsert)
// ============================================================================

/// Test deduplication: records with same primary key should be deduplicated
#[tokio::test]
async fn test_deduplication() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Send 5 records with duplicate IDs:
    // id=1: "first" -> "third" -> "fifth" (3 records, keep latest)
    // id=2: "second" (1 record)
    // id=3: "fourth" (1 record)
    let records = vec![
        TestRecord {
            id: 1,
            value: "first".to_string(),
            timestamp: 100,
        },
        TestRecord {
            id: 2,
            value: "second".to_string(),
            timestamp: 200,
        },
        TestRecord {
            id: 1,
            value: "third".to_string(),
            timestamp: 300,
        },
        TestRecord {
            id: 3,
            value: "fourth".to_string(),
            timestamp: 400,
        },
        TestRecord {
            id: 1,
            value: "fifth".to_string(),
            timestamp: 500,
        },
    ];

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
  pg_sink:
    type: postgres
    from: kafka_source
    table: test_dedup
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(5)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success());

    // Should have 3 unique records after deduplication
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_dedup")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 3, "Should have 3 unique records after deduplication");

    // Verify id=1 has the latest value
    let rows: Vec<(i64, String, i64)> = ctx
        .postgres
        .query("SELECT id, value, timestamp FROM public.test_dedup WHERE id = 1")
        .await
        .expect("Failed to query record");
    assert_eq!(rows[0].1, "fifth", "id=1 should have latest value 'fifth'");
    assert_eq!(rows[0].2, 500, "id=1 should have latest timestamp 500");

    // Verify other records
    let id2: Vec<(String,)> = ctx
        .postgres
        .query("SELECT value FROM public.test_dedup WHERE id = 2")
        .await
        .expect("Failed to query");
    assert_eq!(id2[0].0, "second");

    let id3: Vec<(String,)> = ctx
        .postgres
        .query("SELECT value FROM public.test_dedup WHERE id = 3")
        .await
        .expect("Failed to query");
    assert_eq!(id3[0].0, "fourth");
}

// ============================================================================
// Scenario 5: Delete Operations
// ============================================================================

/// Test delete operations via Kafka delete headers (dbz.op='d')
///
/// This test produces all records upfront (inserts then deletes) and runs
/// a single pipeline to process them all. The final state should reflect
/// that id=1 and id=2 were deleted, leaving only id=3.
#[tokio::test]
async fn test_delete_operations() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce all records in order: 3 inserts, then 2 deletes
    // Final state should be: only id=3 remains
    let insert_records = vec![
        TestRecord {
            id: 1,
            value: "value_1".to_string(),
            timestamp: 100,
        },
        TestRecord {
            id: 2,
            value: "value_2".to_string(),
            timestamp: 200,
        },
        TestRecord {
            id: 3,
            value: "value_3".to_string(),
            timestamp: 300,
        },
    ];

    ctx.kafka
        .produce_avro_records(&insert_records)
        .await
        .expect("Failed to produce insert records");

    // Delete id=1 and id=2
    ctx.kafka
        .produce_avro_records_with_op(
            &[
                TestRecord {
                    id: 1,
                    value: "".to_string(),
                    timestamp: 0,
                },
                TestRecord {
                    id: 2,
                    value: "".to_string(),
                    timestamp: 0,
                },
            ],
            "d",
        )
        .await
        .expect("Failed to produce delete records");

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
    table: test_delete
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    // Process all 5 records: 3 inserts + 2 deletes
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(5)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success());

    // Verify only id=3 remains (id=1 and id=2 were deleted)
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_delete")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 1, "Should have 1 record after delete operations");

    let remaining: Vec<(i64,)> = ctx
        .postgres
        .query("SELECT id FROM public.test_delete")
        .await
        .expect("Failed to query");
    assert_eq!(remaining[0].0, 3, "Only id=3 should remain");
}

// ============================================================================
// Scenario 6: Composite Primary Key
// ============================================================================

/// Test composite (multi-column) primary key
#[tokio::test]
async fn test_composite_primary_key() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(COMPOSITE_PK_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Records with composite PK (id, version):
    // (1,1) "initial_v1" -> (1,1) "updated_v1" (duplicate, should update)
    // (1,2) "value_v2"
    // (2,1) "value_v1"
    // (2,2) "value_v2"
    let records = vec![
        CompositePkRecord {
            id: 1,
            version: 1,
            value: "initial_v1".to_string(),
        },
        CompositePkRecord {
            id: 1,
            version: 2,
            value: "value_v2".to_string(),
        },
        CompositePkRecord {
            id: 2,
            version: 1,
            value: "value_v1".to_string(),
        },
        CompositePkRecord {
            id: 2,
            version: 2,
            value: "value_v2".to_string(),
        },
        CompositePkRecord {
            id: 1,
            version: 1,
            value: "updated_v1".to_string(),
        }, // duplicate
    ];

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
  pg_sink:
    type: postgres
    from: kafka_source
    table: test_composite_pk
    schema: public
    primary_key: id,version
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(5)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success());

    // Should have 4 unique (id,version) combinations
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_composite_pk")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 4, "Should have 4 unique records");

    // Verify (1,1) has updated value
    let row_1_1: Vec<(String,)> = ctx
        .postgres
        .query("SELECT value FROM public.test_composite_pk WHERE id = 1 AND version = 1")
        .await
        .expect("Failed to query");
    assert_eq!(
        row_1_1[0].0, "updated_v1",
        "(1,1) should have updated value"
    );

    // Verify other combinations exist
    let row_1_2: Vec<(String,)> = ctx
        .postgres
        .query("SELECT value FROM public.test_composite_pk WHERE id = 1 AND version = 2")
        .await
        .expect("Failed to query");
    assert_eq!(row_1_2[0].0, "value_v2");

    let row_2_1: Vec<(String,)> = ctx
        .postgres
        .query("SELECT value FROM public.test_composite_pk WHERE id = 2 AND version = 1")
        .await
        .expect("Failed to query");
    assert_eq!(row_2_1[0].0, "value_v1");

    let row_2_2: Vec<(String,)> = ctx
        .postgres
        .query("SELECT value FROM public.test_composite_pk WHERE id = 2 AND version = 2")
        .await
        .expect("Failed to query");
    assert_eq!(row_2_2[0].0, "value_v2");
}

// ============================================================================
// Scenario 7: On Conflict Behavior
// ============================================================================

/// Test on_conflict='nothing' behavior (skip duplicates)
#[tokio::test]
async fn test_on_conflict_nothing() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Insert initial record
    let initial = vec![TestRecord {
        id: 1,
        value: "original".to_string(),
        timestamp: 100,
    }];

    ctx.kafka
        .produce_avro_records(&initial)
        .await
        .expect("Failed to produce records");

    // First pipeline with on_conflict='update' to insert
    let pipeline_update = format!(
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
    table: test_on_conflict
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_update,
            PipelineOpts::new()
                .record_limit(1)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success());

    // Verify initial insert
    let rows: Vec<(String,)> = ctx
        .postgres
        .query("SELECT value FROM public.test_on_conflict WHERE id = 1")
        .await
        .expect("Failed to query");
    assert_eq!(rows[0].0, "original");

    // Produce a conflicting record
    let conflict = vec![TestRecord {
        id: 1,
        value: "should_not_update".to_string(),
        timestamp: 200,
    }];

    ctx.kafka
        .produce_avro_records(&conflict)
        .await
        .expect("Failed to produce records");

    // Run with on_conflict='nothing' - should NOT update
    let pipeline_nothing = format!(
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
    table: test_on_conflict
    schema: public
    primary_key: id
    on_conflict: nothing
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_nothing,
            PipelineOpts::new()
                .record_limit(1)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success());

    // Value should still be 'original' (not updated)
    let rows: Vec<(String,)> = ctx
        .postgres
        .query("SELECT value FROM public.test_on_conflict WHERE id = 1")
        .await
        .expect("Failed to query");
    assert_eq!(
        rows[0].0, "original",
        "Value should remain 'original' with on_conflict='nothing'"
    );
}

// ============================================================================
// Scenario 8: Conditional Update (update_where)
// ============================================================================

/// Test update_where to only update rows when the incoming timestamp is
/// strictly greater than the existing one. This exercises the WHERE clause on
/// ON CONFLICT DO UPDATE SET.
///
/// Steps:
///  1. Insert 3 rows via a normal `on_conflict: update` pipeline.
///  2. Produce 3 updates: id=1 newer ts (should update), id=2 older ts
///     (should NOT update), id=3 equal ts (should NOT update).
///  3. Run a second pipeline with `update_where` and verify.
#[tokio::test]
async fn test_update_where() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // -- Phase 1: seed initial rows --
    let initial_records = vec![
        TestRecord {
            id: 1,
            value: "original_1".to_string(),
            timestamp: 100,
        },
        TestRecord {
            id: 2,
            value: "original_2".to_string(),
            timestamp: 200,
        },
        TestRecord {
            id: 3,
            value: "original_3".to_string(),
            timestamp: 300,
        },
    ];

    ctx.kafka
        .produce_avro_records(&initial_records)
        .await
        .expect("Failed to produce initial records");

    let pipeline_seed = format!(
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
    table: test_cond_update
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_seed,
            PipelineOpts::new()
                .record_limit(3)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "Seed pipeline should succeed");

    // Verify seed data
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_cond_update")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 3, "Should have 3 seeded rows");

    // -- Phase 2: produce updates with varied timestamps --
    let update_records = vec![
        TestRecord {
            id: 1,
            value: "updated_1".to_string(),
            timestamp: 999, // newer than 100 → should update
        },
        TestRecord {
            id: 2,
            value: "should_not_update".to_string(),
            timestamp: 50, // older than 200 → should NOT update
        },
        TestRecord {
            id: 3,
            value: "should_not_update_either".to_string(),
            timestamp: 300, // equal to 300 → should NOT update (strict >)
        },
    ];

    ctx.kafka
        .produce_avro_records(&update_records)
        .await
        .expect("Failed to produce update records");

    let pipeline_conditional = format!(
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
    table: test_cond_update
    schema: public
    primary_key: id
    on_conflict: update
    update_where:
      timestamp: '>'
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    // record_limit(6): re-reads all 6 records from earliest (3 originals + 3 updates).
    // The originals hit ON CONFLICT WHERE with equal timestamps so are no-ops.
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_conditional,
            PipelineOpts::new()
                .record_limit(6)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(
        status.success(),
        "Conditional update pipeline should succeed"
    );

    // -- Phase 3: verify results --
    // id=1: was ts=100, got ts=999 → should be updated
    let row1: Vec<(String, i64)> = ctx
        .postgres
        .query("SELECT value, timestamp FROM public.test_cond_update WHERE id = 1")
        .await
        .expect("Failed to query id=1");
    assert_eq!(
        row1[0].0, "updated_1",
        "id=1 should be updated (newer timestamp)"
    );
    assert_eq!(row1[0].1, 999);

    // id=2: was ts=200, got ts=50 → should NOT be updated
    let row2: Vec<(String, i64)> = ctx
        .postgres
        .query("SELECT value, timestamp FROM public.test_cond_update WHERE id = 2")
        .await
        .expect("Failed to query id=2");
    assert_eq!(
        row2[0].0, "original_2",
        "id=2 should NOT be updated (older timestamp)"
    );
    assert_eq!(row2[0].1, 200);

    // id=3: was ts=300, got ts=300 → should NOT be updated (strict >)
    let row3: Vec<(String, i64)> = ctx
        .postgres
        .query("SELECT value, timestamp FROM public.test_cond_update WHERE id = 3")
        .await
        .expect("Failed to query id=3");
    assert_eq!(
        row3[0].0, "original_3",
        "id=3 should NOT be updated (equal timestamp, strict >)"
    );
    assert_eq!(row3[0].1, 300);

    // Still 3 rows total (no new rows inserted)
    let final_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_cond_update")
        .await
        .expect("Failed to query final count");
    assert_eq!(final_count, 3, "Should still have exactly 3 rows");
}

// ============================================================================
// Scenario 9: Parallel Postgres Sink (basic)
// ============================================================================

/// Test that parallelism > 1 correctly inserts all records.
/// Uses parallelism: 4 with enough records to span multiple parallel slices.
#[tokio::test]
async fn test_parallel_postgres_sink() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=100)
        .map(|i| TestRecord {
            id: i,
            value: format!("parallel_value_{}", i),
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
  pg_sink:
    type: postgres
    from: kafka_source
    table: test_parallel
    schema: public
    primary_key: id
    on_conflict: update
    parallelism: 4
    batch_size: 25
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline(&pipeline, 100)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_parallel")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 100, "Should have all 100 records");

    // Spot-check a few records for correctness
    let rows: Vec<(i64, String, i64)> = ctx
        .postgres
        .query("SELECT id, value, timestamp FROM public.test_parallel WHERE id = 50")
        .await
        .expect("Failed to query record");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 50);
    assert_eq!(rows[0].1, "parallel_value_50");
    assert_eq!(rows[0].2, 1050);
}

// ============================================================================
// Scenario 10: Parallel Postgres Sink with Deduplication
// ============================================================================

/// Test that parallelism works correctly with deduplication.
/// Duplicate primary keys should be deduplicated before parallel inserts,
/// preventing conflicts.
#[tokio::test]
async fn test_parallel_postgres_sink_deduplication() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Create records with duplicates: id 1-5 each appear twice
    let mut records = Vec::new();
    for i in 1..=5 {
        records.push(TestRecord {
            id: i,
            value: format!("original_{}", i),
            timestamp: 1000 + i,
        });
    }
    for i in 1..=5 {
        records.push(TestRecord {
            id: i,
            value: format!("updated_{}", i),
            timestamp: 2000 + i,
        });
    }

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
  pg_sink:
    type: postgres
    from: kafka_source
    table: test_parallel_dedup
    schema: public
    primary_key: id
    on_conflict: update
    parallelism: 3
    batch_size: 5
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(10)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success());

    // Should have 5 unique records
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_parallel_dedup")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 5, "Should have 5 unique records after deduplication");

    // Verify records have the latest values
    let rows: Vec<(i64, String, i64)> = ctx
        .postgres
        .query("SELECT id, value, timestamp FROM public.test_parallel_dedup ORDER BY id")
        .await
        .expect("Failed to query records");
    assert_eq!(rows.len(), 5);
    for (idx, row) in rows.iter().enumerate() {
        let expected_id = (idx + 1) as i64;
        assert_eq!(row.0, expected_id);
        assert_eq!(row.1, format!("updated_{}", expected_id));
        assert_eq!(row.2, 2000 + expected_id);
    }
}

// ============================================================================
// Scenario 11: Parallel Postgres Sink with Deletes
// ============================================================================

/// Test that parallelism works correctly with delete operations.
#[tokio::test]
async fn test_parallel_postgres_sink_deletes() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Insert 10 records
    let insert_records: Vec<TestRecord> = (1..=10)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&insert_records)
        .await
        .expect("Failed to produce insert records");

    // Delete odd-numbered records (1, 3, 5, 7, 9)
    let delete_records: Vec<TestRecord> = (1..=10)
        .step_by(2)
        .map(|i| TestRecord {
            id: i,
            value: String::new(),
            timestamp: 0,
        })
        .collect();

    ctx.kafka
        .produce_avro_records_with_op(&delete_records, "d")
        .await
        .expect("Failed to produce delete records");

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
    table: test_parallel_delete
    schema: public
    primary_key: id
    on_conflict: update
    parallelism: 3
    batch_size: 5
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    // 10 inserts + 5 deletes = 15 total records
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(15)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success());

    // Should have 5 even-numbered records remaining
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_parallel_delete")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 5, "Should have 5 records after deleting odds");

    let remaining: Vec<(i64,)> = ctx
        .postgres
        .query("SELECT id FROM public.test_parallel_delete ORDER BY id")
        .await
        .expect("Failed to query");
    let ids: Vec<i64> = remaining.iter().map(|r| r.0).collect();
    assert_eq!(ids, vec![2, 4, 6, 8, 10], "Only even IDs should remain");
}

// ============================================================================
// Scenario 12: Generic batch accumulation on sink (batch_size / batch_flush_interval)
// ============================================================================

/// Test that the generic batch_size / batch_flush_interval config on a sink
/// correctly accumulates many small input batches and writes all records.
///
/// Forces the Kafka source to emit one-row batches (RECORD_BATCH_SIZE=1),
/// then configures the sink with batch_size=25 so the wrapping layer merges
/// them before writing to Postgres.
#[tokio::test]
async fn test_generic_batch_accumulation_on_sink() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=50)
        .map(|i| TestRecord {
            id: i,
            value: format!("batch_value_{}", i),
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
    batch_size: 1

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: test_batch_accumulation
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 25
    batch_flush_interval: 500ms
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&pipeline, PipelineOpts::new().record_limit(50))
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_batch_accumulation")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 50, "All 50 records should be written");
}

// ============================================================================
// Scenario 13: Read-only failover recovery (SQLSTATE 25006)
// ============================================================================

/// Simulate a primary→replica failover by flipping the test database to
/// `default_transaction_read_only = on` while the sink is mid-flight, then
/// flipping it back. The sink's existing connections are terminated each time
/// so their next reconnect inherits the new GUC. Verifies that
///   1) writes are provably blocked during the read-only window (the pool's
///      `before_acquire` gate evicts read-only connections instead of reusing
///      them forever),
///   2) the retry loop survives the window without exiting, and
///   3) all records land once writes are accepted again — without a restart.
///
/// Records are produced in two waves: wave 2 arrives only after the database
/// is read-only, so the pipeline cannot finish before the failover is
/// exercised — the test cannot pass vacuously.
///
/// Scope is per-database via `ALTER DATABASE "<test_db>" SET ...`, so parallel
/// tests against other isolated databases are unaffected.
#[tokio::test]
async fn test_postgres_sink_read_only_failover() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let make_records = |range: std::ops::RangeInclusive<i64>| -> Vec<TestRecord> {
        range
            .map(|i| TestRecord {
                id: i,
                value: format!("ro_value_{}", i),
                timestamp: 1000 + i,
            })
            .collect()
    };

    // Wave 1: enough records to get the sink writing, but not enough to let
    // the pipeline (record_limit = 50) finish before the failover is injected.
    ctx.kafka
        .produce_avro_records(&make_records(1..=25))
        .await
        .expect("Failed to produce wave 1");

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
    table: test_read_only_failover
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 5
    batch_flush_interval: 200ms
"#,
        topic = ctx.kafka_topic,
    );

    // Run the pipeline in the background so the test can flip the read-only
    // GUC while the sink is mid-flight.
    let pipeline_path = ctx.temp_dir.path().join("pipeline.yaml");
    std::fs::write(&pipeline_path, &pipeline).expect("Failed to write pipeline file");
    let mut env_vars = ctx.build_env_vars();
    env_vars.push((
        "STREAMLING__NUM_RECORDS_BEFORE_STOP".to_string(),
        "50".to_string(),
    ));
    let binary_path = ctx.config.streamling_bin.clone();

    let pipeline_task = tokio::spawn(async move {
        streamling_e2e::streamling::run_streamling(
            &pipeline_path,
            binary_path.as_deref(),
            &env_vars,
        )
        .await
    });

    // Wait until the sink has written at least one row, confirming the happy
    // path is live before we break it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let n = ctx
            .postgres
            .count("SELECT COUNT(*) FROM public.test_read_only_failover")
            .await
            .unwrap_or(0);
        if n > 0 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("sink did not write any rows within 30s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let db = ctx.pg_database.clone();
    let quoted_db = db.replace('"', "\"\"");
    let escaped_db = db.replace('\'', "''");

    // Flip read-only on and force existing sink sessions to reconnect into the
    // new default, so the next INSERT raises SQLSTATE 25006.
    ctx.postgres
        .admin_execute(&format!(
            "ALTER DATABASE \"{}\" SET default_transaction_read_only = on",
            quoted_db
        ))
        .await
        .expect("ALTER DATABASE on failed");
    ctx.postgres
        .admin_execute(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = '{}' AND pid <> pg_backend_pid()",
            escaped_db
        ))
        .await
        .expect("pg_terminate_backend (on) failed");

    // Wave 2: produced only after the database is read-only, so these rows
    // cannot land until the failover resolves.
    ctx.kafka
        .produce_avro_records(&make_records(26..=50))
        .await
        .expect("Failed to produce wave 2");

    // Hold the read-only window long enough for the sink to hit 25006 and
    // exercise the connection-eviction path.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Writes must be blocked while the database is read-only; if all 50 rows
    // are present the failover was never actually exercised.
    let mid_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_read_only_failover")
        .await
        .expect("Failed to query count during read-only window");
    assert!(
        mid_count < 50,
        "read-only window did not block writes (count = {})",
        mid_count
    );

    // Resolve the failover. Deliberately do NOT terminate backends here: the
    // sink's pooled sessions still carry `default_transaction_read_only = on`
    // from when they connected, so recovery depends entirely on the pool's
    // `before_acquire` gate evicting them — exactly what this test defends.
    ctx.postgres
        .admin_execute(&format!(
            "ALTER DATABASE \"{}\" SET default_transaction_read_only = off",
            quoted_db
        ))
        .await
        .expect("ALTER DATABASE off failed");

    let status = tokio::time::timeout(Duration::from_secs(90), pipeline_task)
        .await
        .expect("pipeline did not finish within 90s after recovery")
        .expect("pipeline task panicked")
        .expect("pipeline returned error");
    assert!(
        status.success(),
        "Streamling should exit successfully after recovery"
    );

    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_read_only_failover")
        .await
        .expect("Failed to query count after recovery");
    assert_eq!(count, 50, "All 50 records should be written after recovery");
}
