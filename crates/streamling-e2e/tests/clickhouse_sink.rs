//! Kafka to ClickHouse sink e2e tests.
//!
//! These tests verify that streamling can correctly read from Kafka and write to ClickHouse.
//!
//! ## Test Scenarios
//!
//! | Test                           | Description                                          |
//! |--------------------------------|------------------------------------------------------|
//! | `test_basic_kafka_to_clickhouse` | Basic Kafka → ClickHouse data flow                  |
//! | `test_multiple_batches`        | Multiple batch processing (100 records)              |
//! | `test_schema_override`         | Int64 → DateTime64 type conversion                   |
//! | `test_is_deleted_injection`    | Verifies is_deleted column is injected for CDC       |
//! | `test_deduplication`           | Primary key based deduplication (ReplacingMergeTree) |
//! | `test_sink_batch_size_from_env_drives_flush` | STREAMLING__CLICKHOUSE_SINK__BATCH_SIZE reaches the rebatcher |

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

/// Test record structure
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

/// Helper to build ClickHouse sink env vars
fn clickhouse_env(ctx: &TestContext) -> Vec<(String, String)> {
    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");
    vec![
        (
            "STREAMLING__CLICKHOUSE_SINK__URL".to_string(),
            ctx.config.clickhouse_url.clone(),
        ),
        (
            "STREAMLING__CLICKHOUSE_SINK__DATABASE".to_string(),
            clickhouse.database.clone(),
        ),
        (
            "STREAMLING__CLICKHOUSE_SINK__USER".to_string(),
            "default".to_string(),
        ),
        (
            "STREAMLING__CLICKHOUSE_SINK__PASSWORD".to_string(),
            String::new(),
        ),
    ]
}

// =============================================================================
// Basic Tests
// =============================================================================

/// Basic test: read records from Kafka and write to ClickHouse.
/// Also exercises `compression: gzip` end-to-end against real ClickHouse so
/// we cover the gzip request path (the default is zstd).
#[tokio::test]
async fn test_basic_kafka_to_clickhouse() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    // Register schema and produce test data
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
  ch_sink:
    type: clickhouse
    from: kafka_source
    table: test_output
    primary_key: id
    compression: gzip
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new().record_limit(10);
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify results in ClickHouse
    let count = clickhouse
        .count("SELECT COUNT(*) FROM test_output")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 10, "Should have 10 records in output table");
}

/// `STREAMLING__CLICKHOUSE_SINK__BATCH_SIZE` must reach the sink's rebatcher.
///
/// Proven by making the size trigger the *only* way rows can be written: the
/// flush interval is pushed out to an hour, so if the env-configured batch size
/// were ignored (the embedded default is 100000, far above the 10 records
/// produced) nothing would ever flush and the pipeline would never reach its
/// record limit. Passing therefore means the override took effect; a regression
/// hangs rather than silently asserting the wrong thing.
#[tokio::test]
async fn test_sink_batch_size_from_env_drives_flush() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    clickhouse
        .execute(
            "CREATE TABLE env_batch_size_test (\
                 id Int64, value String, timestamp Int64, \
                 is_deleted UInt8 DEFAULT 0, insert_time DateTime DEFAULT now() \
             ) ENGINE = ReplacingMergeTree(insert_time) ORDER BY id",
        )
        .await
        .expect("Failed to pre-create sink table");

    let total_records = 10;
    let records: Vec<TestRecord> = (1..=total_records)
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
  ch_sink:
    type: clickhouse
    from: kafka_source
    table: env_batch_size_test
    primary_key: id
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new()
        .record_limit(total_records as u64)
        .env("STREAMLING__CLICKHOUSE_SINK__BATCH_SIZE", "10")
        .env("STREAMLING__CLICKHOUSE_SINK__BATCH_FLUSH_INTERVAL", "1h")
        .env("STREAMLING__RECORD_BATCH_SIZE", "1");
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let count = clickhouse
        .count("SELECT COUNT(*) FROM env_batch_size_test")
        .await
        .expect("Failed to query count");
    assert_eq!(
        count, total_records as u64,
        "all rows must be written, which is only possible if the env-configured \
         batch_size (10) drove the flush -- the 1h interval cannot have"
    );
}

// =============================================================================
// Multiple Batches Test
// =============================================================================

/// Test processing multiple batches of records.
/// Uses `compression: lz4` to exercise the lz4 request path against real
/// ClickHouse.
#[tokio::test]
async fn test_multiple_batches() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce 25 records (will span multiple batches with batch_size=5)
    let total_records = 25;
    let records: Vec<TestRecord> = (1..=total_records)
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
  ch_sink:
    type: clickhouse
    from: kafka_source
    table: test_multi_batch
    primary_key: id
    compression: lz4
    batch_size: 5
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new().record_limit(total_records as u64);
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let count = clickhouse
        .count("SELECT COUNT(*) FROM test_multi_batch")
        .await
        .expect("Failed to query count");

    assert_eq!(
        count, total_records as u64,
        "Should have {} records in output table",
        total_records
    );
}

// =============================================================================
// Schema Override Test
// =============================================================================

/// Record with timestamp fields for schema override test
#[derive(Debug, Clone, Serialize)]
struct TimestampRecord {
    id: i64,
    created_at: i64,
    updated_at: i64,
    value: String,
}

const TIMESTAMP_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TimestampRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "created_at", "type": "long"},
        {"name": "updated_at", "type": "long"},
        {"name": "value", "type": "string"}
    ]
}"#;

/// Test schema override: Int64 → DateTime64 type conversion
#[tokio::test]
async fn test_schema_override() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    ctx.kafka
        .register_schema(TIMESTAMP_SCHEMA)
        .await
        .expect("Failed to register schema");

    let base_timestamp = 1700000000i64; // Nov 14, 2023
    let total_records = 5;
    let records: Vec<TimestampRecord> = (0..total_records)
        .map(|i| TimestampRecord {
            id: i,
            created_at: base_timestamp + i * 3600, // Add 1 hour per record
            updated_at: base_timestamp + i * 7200, // Add 2 hours per record
            value: format!("value_{}", i),
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Use schema_override to convert Int64 → DateTime64
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
  ch_sink:
    type: clickhouse
    from: kafka_source
    table: schema_override_output
    primary_key: id
    schema_override:
      created_at: "DateTime64(3)"
      updated_at: "DateTime64(3) CODEC(Delta, ZSTD)"
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new().record_limit(total_records as u64);
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify record count
    let count = clickhouse
        .count("SELECT COUNT(*) FROM schema_override_output")
        .await
        .expect("Failed to query count");
    assert_eq!(
        count, total_records as u64,
        "Should have {} records",
        total_records
    );

    // Verify schema: check that DateTime64 columns were created correctly
    let columns = clickhouse
        .get_column_types("schema_override_output")
        .await
        .expect("Failed to get column types");

    let created_at_type = columns
        .iter()
        .find(|(name, _)| name == "created_at")
        .map(|(_, t)| t.as_str());
    assert!(
        created_at_type == Some("DateTime64(3)"),
        "created_at should be DateTime64(3), got: {:?}",
        created_at_type
    );

    let updated_at_type = columns
        .iter()
        .find(|(name, _)| name == "updated_at")
        .map(|(_, t)| t.as_str());
    assert!(
        updated_at_type == Some("DateTime64(3)"),
        "updated_at should be DateTime64(3), got: {:?}",
        updated_at_type
    );
}

// =============================================================================
// is_deleted Injection Test
// =============================================================================

/// Test that is_deleted column is automatically injected for CDC support
#[tokio::test]
async fn test_is_deleted_injection() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let total_records = 10;
    let records: Vec<TestRecord> = (1..=total_records)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    // All records are inserts (op='c')
    ctx.kafka
        .produce_avro_records_with_op(&records, "c")
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
  ch_sink:
    type: clickhouse
    from: kafka_source
    table: cdc_test_output
    primary_key: id
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new().record_limit(total_records as u64);
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify record count
    let count = clickhouse
        .count("SELECT COUNT(*) FROM cdc_test_output")
        .await
        .expect("Failed to query count");
    assert_eq!(
        count, total_records as u64,
        "Should have {} records",
        total_records
    );

    // Verify is_deleted column exists and all rows have is_deleted = 0 (inserts)
    let columns = clickhouse
        .get_column_types("cdc_test_output")
        .await
        .expect("Failed to get column types");

    let is_deleted_col = columns.iter().find(|(name, _)| name == "is_deleted");
    assert!(
        is_deleted_col.is_some(),
        "is_deleted column should be automatically added"
    );
    assert_eq!(
        is_deleted_col.unwrap().1,
        "UInt8",
        "is_deleted should be UInt8"
    );

    // Verify all records have is_deleted = 0
    let deleted_count = clickhouse
        .count("SELECT COUNT(*) FROM cdc_test_output WHERE is_deleted = 1")
        .await
        .expect("Failed to query deleted count");
    assert_eq!(deleted_count, 0, "All records should have is_deleted = 0");
}

// =============================================================================
// Deduplication Test (ReplacingMergeTree behavior)
// =============================================================================

/// Test deduplication with primary key (ReplacingMergeTree).
/// Like every test in this file that omits `compression:`, this exercises the
/// default codec (zstd) end-to-end against real ClickHouse.
#[tokio::test]
async fn test_deduplication() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce records with duplicate IDs - later records should replace earlier ones
    let records = vec![
        TestRecord {
            id: 1,
            value: "first_1".to_string(),
            timestamp: 100,
        },
        TestRecord {
            id: 2,
            value: "first_2".to_string(),
            timestamp: 200,
        },
        TestRecord {
            id: 1,
            value: "updated_1".to_string(),
            timestamp: 300,
        }, // Duplicate, should replace
        TestRecord {
            id: 3,
            value: "first_3".to_string(),
            timestamp: 400,
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
  ch_sink:
    type: clickhouse
    from: kafka_source
    table: dedup_test_output
    primary_key: id
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new().record_limit(2); // 4 records produced
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let count = clickhouse
        .count("SELECT COUNT(*) FROM dedup_test_output FINAL")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 3, "Should have 3 unique records after deduplication");

    // Verify id=1 has the updated value
    #[derive(Row, Deserialize)]
    struct ResultRow {
        value: String,
    }

    let rows: Vec<ResultRow> = clickhouse
        .query("SELECT value FROM dedup_test_output WHERE id = 1")
        .await
        .expect("Failed to query value");

    assert_eq!(rows.len(), 1, "Should have exactly one row for id=1");
    assert_eq!(
        rows[0].value, "updated_1",
        "id=1 should have updated value after deduplication"
    );
}

// =============================================================================
// Delete Operations Test
// =============================================================================

/// Test delete operations via dbz.op='d' header
#[tokio::test]
async fn test_delete_operations() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce initial records (3 inserts)
    let initial_records = vec![
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
        .produce_avro_records_with_op(&initial_records, "c")
        .await
        .expect("Failed to produce initial records");

    // Produce delete records for id=1 and id=2
    let delete_records = vec![
        TestRecord {
            id: 1,
            value: String::new(),
            timestamp: 0,
        },
        TestRecord {
            id: 2,
            value: String::new(),
            timestamp: 0,
        },
    ];
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
  ch_sink:
    type: clickhouse
    from: kafka_source
    table: delete_test_output
    primary_key: id
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new().record_limit(initial_records.len() as u64);
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // With ReplacingMergeTree(insert_time, is_deleted), FINAL removes deleted rows entirely
    let active_count = clickhouse
        .count("SELECT COUNT(*) FROM delete_test_output FINAL WHERE is_deleted = 0")
        .await
        .expect("Failed to query active count");
    assert_eq!(
        active_count, 1,
        "Should have 1 active record (id=3) after deletes"
    );

    let deleted_count = clickhouse
        .count("SELECT COUNT(*) FROM delete_test_output FINAL WHERE is_deleted = 1")
        .await
        .expect("Failed to query deleted count");
    assert_eq!(
        deleted_count, 0,
        "Deleted records should be cleaned up by ReplacingMergeTree"
    );
}

// =============================================================================
// Append-Only Mode: false (ALTER TABLE DELETE)
// =============================================================================

/// Test append_only_mode: false — inserts go via INSERT, deletes go via ALTER TABLE DELETE.
/// Unlike the default mode (ReplacingMergeTree with is_deleted), this mode physically removes
/// rows from ClickHouse.
#[tokio::test]
async fn test_append_only_mode_false() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce 3 inserts followed by 1 delete
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
        .produce_avro_records_with_op(&insert_records, "c")
        .await
        .expect("Failed to produce insert records");

    // Delete id=2
    let delete_records = vec![TestRecord {
        id: 2,
        value: String::new(),
        timestamp: 0,
    }];
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
  ch_sink:
    type: clickhouse
    from: kafka_source
    table: append_only_false_test
    primary_key: id
    append_only_mode: false
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new().record_limit(insert_records.len() as u64);
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // With append_only_mode: false, there should be no is_deleted column
    let columns = clickhouse
        .get_column_types("append_only_false_test")
        .await
        .expect("Failed to get column types");

    let has_is_deleted = columns.iter().any(|(name, _)| name == "is_deleted");
    assert!(
        !has_is_deleted,
        "append_only_mode: false should NOT create an is_deleted column"
    );

    let has_insert_time = columns.iter().any(|(name, _)| name == "insert_time");
    assert!(
        !has_insert_time,
        "append_only_mode: false should NOT create an insert_time column"
    );

    // The DELETE should have physically removed id=2
    let total_count = clickhouse
        .count("SELECT COUNT(*) FROM append_only_false_test")
        .await
        .expect("Failed to query total count");
    assert_eq!(
        total_count, 2,
        "Should have 2 records after DELETE removed id=2"
    );

    // Verify the remaining records are id=1 and id=3
    #[derive(Row, Deserialize)]
    struct IdRow {
        id: i64,
    }

    let mut rows: Vec<IdRow> = clickhouse
        .query("SELECT id FROM append_only_false_test ORDER BY id")
        .await
        .expect("Failed to query rows");

    let ids: Vec<i64> = rows.drain(..).map(|r| r.id).collect();
    assert_eq!(ids, vec![1, 3], "Remaining records should be id=1 and id=3");
}

/// Test append_only_mode: false with updates — updates should be treated as inserts (upserts)
#[tokio::test]
async fn test_append_only_mode_false_with_updates() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Insert 2 records, then update one of them
    let insert_records = vec![
        TestRecord {
            id: 1,
            value: "original".to_string(),
            timestamp: 100,
        },
        TestRecord {
            id: 2,
            value: "value_2".to_string(),
            timestamp: 200,
        },
    ];
    ctx.kafka
        .produce_avro_records_with_op(&insert_records, "c")
        .await
        .expect("Failed to produce insert records");

    // Update id=1
    let update_records = vec![TestRecord {
        id: 1,
        value: "updated".to_string(),
        timestamp: 300,
    }];
    ctx.kafka
        .produce_avro_records_with_op(&update_records, "u")
        .await
        .expect("Failed to produce update records");

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
  ch_sink:
    type: clickhouse
    from: kafka_source
    table: append_only_false_update_test
    primary_key: id
    append_only_mode: false
    batch_size: 1
"#,
        topic = ctx.kafka_topic,
    );

    // The sink's record limit counts WRITTEN rows, and with
    // append_only_mode: false rows are deduplicated by primary key within a
    // batch before writing. Depending on how the consumer batches the two
    // produce calls, the 3 input records yield 2 or 3 written rows — no
    // input-based limit is deterministic on its own. Force one record per
    // batch (sink batch_size: 1 + record batch size 1, per the repo test
    // guidance) so every produced record is written and counted, then count
    // them all.
    let mut opts = PipelineOpts::new()
        .record_limit((insert_records.len() + update_records.len()) as u64)
        .env("STREAMLING__RECORD_BATCH_SIZE", "1");
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // ReplacingMergeTree should deduplicate, keeping the latest version
    let count = clickhouse
        .count("SELECT COUNT(*) FROM append_only_false_update_test FINAL")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 2, "Should have 2 unique records");

    #[derive(Row, Deserialize)]
    struct ValueRow {
        value: String,
    }

    let rows: Vec<ValueRow> = clickhouse
        .query("SELECT value FROM append_only_false_update_test FINAL WHERE id = 1")
        .await
        .expect("Failed to query value");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].value, "updated",
        "id=1 should have the updated value"
    );
}

// =============================================================================
// SELECT * EXCEPT with same-name re-add
// =============================================================================

/// `SELECT * EXCEPT (col, _gs_op), <expr> AS col2, <expr> AS _gs_op` — the
/// wildcard excludes a source column and the enriched `_gs_op`, then re-adds
/// computed columns, one under the same name. The sink batch must match the
/// transform's declared schema (no duplicated/leftover columns).
#[tokio::test]
async fn test_select_except_same_name_readd() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Pre-create the sink table WITHOUT the `ts` column: the upstream `*`
    // has widened since the table was created (source schema evolution), so
    // the transform now emits one more column than the table has. The write
    // path must tolerate extra batch columns (project to the table schema),
    // as it did before the DataFusion 54 upgrade.
    clickhouse
        .execute(
            "CREATE TABLE select_except_readd_test (\
                 id Int64, value String, is_deleted UInt8 DEFAULT 0, \
                 insert_time DateTime DEFAULT now() \
             ) ENGINE = ReplacingMergeTree(insert_time) ORDER BY id",
        )
        .await
        .expect("Failed to pre-create sink table");

    let total_records = 4;
    let records: Vec<TestRecord> = (1..=total_records)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records_with_op(&records, "c")
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

transforms:
  except_transform:
    type: sql
    primary_key: id
    sql: >-
      SELECT * EXCEPT (timestamp, _gs_op), timestamp AS ts,
      CASE WHEN _gs_op = 'c' THEN 'i' ELSE _gs_op END AS _gs_op
      FROM kafka_source

sinks:
  ch_sink:
    type: clickhouse
    from: except_transform
    table: select_except_readd_test
    primary_key: id
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new().record_limit(total_records as u64);
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    let count = clickhouse
        .count("SELECT COUNT(*) FROM select_except_readd_test FINAL")
        .await
        .expect("Failed to query count");
    assert_eq!(count, total_records as u64, "all rows written");

    // Value-level check: by-name pairing must not mislabel the surviving
    // columns when the extra one is dropped.
    #[derive(Row, Deserialize)]
    struct OutRow {
        id: i64,
        value: String,
    }
    let rows: Vec<OutRow> = clickhouse
        .query("SELECT id, value FROM select_except_readd_test FINAL ORDER BY id")
        .await
        .expect("Failed to query rows");
    let got: Vec<(i64, &str)> = rows.iter().map(|r| (r.id, r.value.as_str())).collect();
    assert_eq!(
        got,
        vec![
            (1, "value_1"),
            (2, "value_2"),
            (3, "value_3"),
            (4, "value_4")
        ],
        "surviving columns must keep their own data"
    );
}

// =============================================================================
// Wide-source EXCEPT shape against a pre-created two-arg ReplacingMergeTree
// =============================================================================

const WIDE_POSITIONS_SCHEMA: &str = r#"{
    "type": "record",
    "name": "topLevelRecord",
    "fields": [
        {"name": "user_addr", "type": "string"},
        {"name": "token_id", "type": "string"},
        {"name": "amount", "type": ["null", "string"], "default": null},
        {"name": "avg_price", "type": ["null", "string"], "default": null},
        {"name": "realized_pnl", "type": ["null", "string"], "default": null},
        {"name": "total_bought", "type": ["null", "string"], "default": null},
        {"name": "last_updated_block", "type": ["null", "long"], "default": null},
        {"name": "_sm_version", "type": "long"},
        {"name": "_sm_deleted", "type": "boolean"}
    ]
}"#;

#[derive(serde::Serialize)]
struct WidePositionRecord {
    user_addr: String,
    token_id: String,
    amount: Option<String>,
    avg_price: Option<String>,
    realized_pnl: Option<String>,
    total_bought: Option<String>,
    last_updated_block: Option<i64>,
    _sm_version: i64,
    _sm_deleted: bool,
}

/// A 9-field source, a 4-way `SELECT * EXCEPT` with a rename and a same-named
/// recomputed `_gs_op`, writing into a pre-created two-argument
/// ReplacingMergeTree(version, is_deleted) table. Mirrors a pipeline shape
/// that fails on DataFusion 54 with "number of columns(9) must match number
/// of fields(8)" raised from inside the engine's sink machinery.
#[tokio::test]
async fn test_wide_source_except_precreated_versioned_table() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");

    ctx.kafka
        .register_schema(WIDE_POSITIONS_SCHEMA)
        .await
        .expect("Failed to register schema");

    clickhouse
        .execute(
            "CREATE TABLE wide_positions_out (\
                 token_id String, amount Nullable(String), avg_price Nullable(String), \
                 realized_pnl Nullable(String), total_bought Nullable(String), \
                 last_updated_block Nullable(Int64), user String, \
                 is_deleted UInt8, insert_time DateTime DEFAULT now() \
             ) ENGINE = ReplacingMergeTree(insert_time, is_deleted) \
             ORDER BY (user, token_id)",
        )
        .await
        .expect("Failed to pre-create sink table");

    let records: Vec<WidePositionRecord> = (1..=4)
        .map(|i| WidePositionRecord {
            user_addr: format!("0xuser{i}"),
            token_id: format!("token{i}"),
            amount: Some(format!("{i}")),
            avg_price: Some("1.5".to_string()),
            realized_pnl: None,
            total_bought: Some(format!("{i}0")),
            last_updated_block: Some(1000 + i),
            _sm_version: i,
            _sm_deleted: false,
        })
        .collect();

    ctx.kafka
        .produce_avro_records_with_op(&records, "c")
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"
sources:
  user_positions:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: user_addr

transforms:
  positions_transform:
    type: sql
    primary_key: user
    sql: >-
      SELECT * EXCEPT (_sm_version, _sm_deleted, _gs_op, user_addr),
      user_addr AS user,
      CASE WHEN _sm_deleted = false THEN 'i' ELSE 'd' END AS _gs_op
      FROM user_positions

sinks:
  clickhouse_sink:
    type: clickhouse
    from: positions_transform
    table: wide_positions_out
    primary_key: user
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new().record_limit(4);
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "Streamling should exit successfully");

    let count = clickhouse
        .count("SELECT COUNT(*) FROM wide_positions_out FINAL")
        .await
        .expect("Failed to query count");
    assert_eq!(count, 4, "all rows written");
}
