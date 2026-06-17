//! Hybrid source e2e tests.
//!
//! These tests verify that streamling can correctly read from hybrid sources
//! (combining bounded sources like ClickHouse with unbounded sources like Kafka).
//! Ported from crates/streamling/tests/hybrid_source.rs (test_hybrid_source_end_to_end_clickhouse_to_kafka)

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

// ============================================================================
// Test Record Types
// ============================================================================

/// Test record for Kafka messages - must match the schema used in original integration tests
/// Note: id is STRING to match ClickHouse schema for hybrid source unification
#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    block: i64,
    id: String,
    data: String,
    timestamp: i64,
}

/// Schema matching original integration tests - id is string, not long
const TEST_SCHEMA: &str = r#"{"type":"record","name":"TestMessage","fields":[
    {"name":"block","type":"long"},
    {"name":"id","type":"string"},
    {"name":"data","type":"string"},
    {"name":"timestamp","type":"long"}
]}"#;

// ============================================================================
// Scenario 1: Hybrid source from ClickHouse to Kafka
// ============================================================================

/// Test hybrid source that reads from ClickHouse (bounded) and Kafka (unbounded)
/// Ported from: test_hybrid_source_end_to_end_clickhouse_to_kafka
#[tokio::test]
async fn test_hybrid_clickhouse_to_kafka() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    // Create ClickHouse source table
    // Note: id must be String to match Kafka schema for hybrid source unification
    clickhouse
        .execute(
            "CREATE TABLE hybrid_source_test (
                block Int64,
                id String,
                data String,
                timestamp Int64,
                is_deleted UInt8
            ) ENGINE = MergeTree()
            ORDER BY (block, id)",
        )
        .await
        .expect("Failed to create ClickHouse table");

    // Insert some records into ClickHouse (bounded source data)
    // Using string IDs that match the original test pattern
    clickhouse
        .execute(
            "INSERT INTO hybrid_source_test VALUES
            (1, 'Alice', 'A', 0, 0),
            (2, 'Bob', 'B', 0, 0),
            (3, 'Charlie', 'C', 0, 0)",
        )
        .await
        .expect("Failed to insert ClickHouse data");

    // Register schema for Kafka
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce some records to Kafka (unbounded source data)
    // Using string IDs that don't overlap with ClickHouse IDs (Alice, Bob, Charlie)
    let kafka_records: Vec<TestRecord> = (1..=3)
        .map(|i| TestRecord {
            block: 100 + i,
            id: format!("kafka_user_{}", i),
            data: format!("kafka_data_{}", i),
            timestamp: 2000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&kafka_records)
        .await
        .expect("Failed to produce Kafka records");

    // Create offset table in ClickHouse for hybrid source to track where Kafka should start
    // The offset table stores topic/partition/offset for Kafka consumer positioning
    clickhouse
        .execute(
            "CREATE TABLE kafka_offsets (
                topic String,
                partition Int32,
                offset UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");

    // Run pipeline: Hybrid source (ClickHouse bounded + Kafka unbounded) → PostgreSQL sink
    // Note: streamling handles _gs_op internally - ClickHouse is_deleted and Kafka dbz.op header
    let pipeline = format!(
        r#"
sources:
  hybrid_source:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: hybrid_source_test
        columns: block,id,data,timestamp
    unbounded_source:
      source_type: kafka
      topic: {kafka_topic}
      start_at: earliest
    offset_table:
      topic_name: {kafka_topic}
      table_name: kafka_offsets
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: hybrid_source
    table: hybrid_results
    schema: public
    primary_key: id
    on_conflict: update
"#,
        kafka_topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(6) // 3 from ClickHouse + 3 from Kafka
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify records from both sources made it through
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hybrid_results")
        .await
        .expect("Failed to query count");

    // We expect at least the ClickHouse records
    // (Kafka records may or may not be included depending on hybrid source phase completion)
    assert!(
        count >= 3,
        "Should have at least 3 records from ClickHouse, got {}",
        count
    );

    // Verify ClickHouse records are present (they have data='A', 'B', 'C')
    let ch_data: Vec<(String,)> = ctx
        .postgres
        .query(
            "SELECT id FROM public.hybrid_results WHERE id IN ('Alice', 'Bob', 'Charlie') ORDER BY id",
        )
        .await
        .expect("Failed to query ClickHouse data");

    assert_eq!(
        ch_data.len(),
        3,
        "Should have 3 ClickHouse records (Alice, Bob, Charlie)"
    );

    // Verify Kafka records are present (they have id='kafka_user_1', 'kafka_user_2', 'kafka_user_3')
    let kafka_data: Vec<(String,)> = ctx
        .postgres
        .query("SELECT id FROM public.hybrid_results WHERE id LIKE 'kafka_user_%' ORDER BY id")
        .await
        .expect("Failed to query Kafka data");

    assert_eq!(
        kafka_data.len(),
        3,
        "Should have 3 Kafka records (kafka_user_1, kafka_user_2, kafka_user_3)"
    );
}

// ============================================================================
// Scenario 2: Hybrid source with filters
// ============================================================================

/// Test hybrid source with filter expressions on both bounded and unbounded sources
/// Ported from: test_hybrid_source_with_filters
///
/// Setup:
/// - ClickHouse: 50 records (30 with block=0, 20 with block=1)
/// - Kafka: 50 records with block values 1-50
///
/// Filters:
/// - ClickHouse: block = 1 (keeps 20 records with ids 30-49)
/// - Kafka: block > 25 (keeps 25 records with block values 26-50)
///
/// Expected: 45 total records (20 from ClickHouse + 25 from Kafka)
#[tokio::test]
async fn test_hybrid_source_with_filters() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    // Create ClickHouse source table
    clickhouse
        .execute(
            "CREATE TABLE hybrid_filter_test_ch (
                block Int64,
                id String,
                data String,
                timestamp Int64,
                is_deleted UInt8
            ) ENGINE = MergeTree()
            ORDER BY (block, id)",
        )
        .await
        .expect("Failed to create ClickHouse table");

    // Insert 50 records:
    // - 30 records with block=0 (ids ch_id_0 to ch_id_29) - will be filtered out
    // - 20 records with block=1 (ids ch_id_30 to ch_id_49) - will pass filter
    let mut insert_values = Vec::new();
    for i in 0..30 {
        insert_values.push(format!(
            "(0, 'ch_id_{}', 'ch_data_{}', {}, 0)",
            i,
            i,
            1700000000 + i
        ));
    }
    for i in 30..50 {
        insert_values.push(format!(
            "(1, 'ch_id_{}', 'ch_data_{}', {}, 0)",
            i,
            i,
            1700000000 + i
        ));
    }

    let insert_query = format!(
        "INSERT INTO hybrid_filter_test_ch (block, id, data, timestamp, is_deleted) VALUES {}",
        insert_values.join(", ")
    );
    clickhouse
        .execute(&insert_query)
        .await
        .expect("Failed to insert ClickHouse data");

    // Register schema for Kafka
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce 50 Kafka records with block values 1-50
    // Filter: block > 25, so records with block 26-50 (25 records) will pass
    let kafka_records: Vec<TestRecord> = (1..=50)
        .map(|i| TestRecord {
            block: i,
            id: format!("kafka_id_{}", i),
            data: format!("kafka_data_{}", i),
            timestamp: 1700000000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&kafka_records)
        .await
        .expect("Failed to produce Kafka records");

    // Create offset table for hybrid source
    clickhouse
        .execute(
            "CREATE TABLE kafka_offsets_filter (
                topic String,
                partition Int32,
                offset UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");

    // Run pipeline with filters on both sources
    let pipeline = format!(
        r#"
sources:
  hybrid_filtered_source:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: hybrid_filter_test_ch
        columns: block,id,data,timestamp
        filter: "block = 1"
    unbounded_source:
      source_type: kafka
      topic: {kafka_topic}
      start_at: earliest
      filter: "block > 25"
    offset_table:
      topic_name: {kafka_topic}
      table_name: kafka_offsets_filter
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: hybrid_filtered_source
    table: hybrid_filter_results
    schema: public
    primary_key: id
    on_conflict: update
"#,
        kafka_topic = ctx.kafka_topic,
    );

    // Expected: 20 from ClickHouse (block=1) + 25 from Kafka (block > 25) = 45 total
    let expected_total = 45u64;

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(expected_total)
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Verify total count
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hybrid_filter_results")
        .await
        .expect("Failed to query count");

    assert_eq!(
        count, expected_total as i64,
        "Should have {} records total (20 ClickHouse + 25 Kafka), got {}",
        expected_total, count
    );

    // Verify ClickHouse records: should only have ids ch_id_30 to ch_id_49 (block=1)
    let ch_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hybrid_filter_results WHERE id LIKE 'ch_id_%'")
        .await
        .expect("Failed to query ClickHouse count");

    assert_eq!(
        ch_count, 20,
        "Should have 20 ClickHouse records with block=1, got {}",
        ch_count
    );

    // Verify Kafka records: should have ids kafka_id_26 to kafka_id_50 (block > 25)
    let kafka_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hybrid_filter_results WHERE id LIKE 'kafka_id_%'")
        .await
        .expect("Failed to query Kafka count");

    assert_eq!(
        kafka_count, 25,
        "Should have 25 Kafka records with block > 25, got {}",
        kafka_count
    );
}

// ============================================================================
// Scenario 3: Job mode terminates after bounded phase
// ============================================================================

/// Test that with job_mode enabled, the hybrid source terminates after the bounded
/// (ClickHouse) phase completes without transitioning to the unbounded (Kafka) phase.
///
/// Setup:
/// - ClickHouse: 3 records (bounded source)
/// - Kafka: 3 records (unbounded source — should NOT be consumed)
///
/// With STREAMLING__JOB_MODE=true, the pipeline should:
/// 1. Process all ClickHouse records
/// 2. Terminate cleanly (exit 0) without consuming Kafka records
/// 3. Only ClickHouse records appear in the sink
#[tokio::test]
async fn test_hybrid_source_job_mode_terminates() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    // Create ClickHouse source table (bounded data)
    clickhouse
        .execute(
            "CREATE TABLE hybrid_job_mode_test (
                block Int64,
                id String,
                data String,
                timestamp Int64,
                is_deleted UInt8
            ) ENGINE = MergeTree()
            ORDER BY (block, id)",
        )
        .await
        .expect("Failed to create ClickHouse table");

    // Insert bounded-phase records
    clickhouse
        .execute(
            "INSERT INTO hybrid_job_mode_test VALUES
            (1, 'ch_1', 'bounded_A', 100, 0),
            (2, 'ch_2', 'bounded_B', 200, 0),
            (3, 'ch_3', 'bounded_C', 300, 0)",
        )
        .await
        .expect("Failed to insert ClickHouse data");

    // Register Kafka schema and produce unbounded records (should NOT be consumed)
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let kafka_records: Vec<TestRecord> = (1..=3)
        .map(|i| TestRecord {
            block: 100 + i,
            id: format!("kafka_{}", i),
            data: format!("unbounded_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&kafka_records)
        .await
        .expect("Failed to produce Kafka records");

    // Create offset table
    clickhouse
        .execute(
            "CREATE TABLE kafka_offsets_job_mode (
                topic String,
                partition Int32,
                offset UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");

    let pipeline = format!(
        r#"
sources:
  hybrid_source:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: hybrid_job_mode_test
        columns: block,id,data,timestamp
    unbounded_source:
      source_type: kafka
      topic: {kafka_topic}
      start_at: earliest
    offset_table:
      topic_name: {kafka_topic}
      table_name: kafka_offsets_job_mode
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: hybrid_source
    table: hybrid_job_mode_results
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#,
        kafka_topic = ctx.kafka_topic,
    );

    // No record_limit — job mode should cause natural termination after bounded phase
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .env("STREAMLING__JOB_MODE", "true")
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Pipeline execution failed");

    assert!(
        status.success(),
        "Job mode pipeline should terminate successfully after bounded phase"
    );

    // Verify only bounded-phase (ClickHouse) records are in the sink
    let total_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hybrid_job_mode_results")
        .await
        .expect("Failed to query count");

    assert_eq!(
        total_count, 3,
        "Should have exactly 3 records from bounded phase, got {}",
        total_count
    );

    // Verify ClickHouse records are present
    let ch_records: Vec<(String,)> = ctx
        .postgres
        .query("SELECT id FROM public.hybrid_job_mode_results WHERE id LIKE 'ch_%' ORDER BY id")
        .await
        .expect("Failed to query ClickHouse records");

    assert_eq!(
        ch_records.len(),
        3,
        "Should have 3 ClickHouse records, got {}",
        ch_records.len()
    );

    // Verify NO Kafka records are present (unbounded phase was skipped)
    let kafka_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hybrid_job_mode_results WHERE id LIKE 'kafka_%'")
        .await
        .expect("Failed to query Kafka count");

    assert_eq!(
        kafka_count, 0,
        "Should have 0 Kafka records (unbounded phase skipped in job mode), got {}",
        kafka_count
    );
}

// ============================================================================
// Scenario 4: Hybrid resume from high savepoint/checkpoint
// ============================================================================

/// Regression test for hybrid bounded ClickHouse resume:
/// when resuming from a high saved split, execution must start from that
/// cursor (not from block range origin 0).
///
/// This uses a high `start_at` and a small `block_range` so any reset to 0
/// would require scanning a very large number of empty ranges and likely time out.
#[tokio::test]
async fn test_hybrid_clickhouse_resume_from_high_saved_split() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE hybrid_resume_test (
                block Int64,
                id String,
                data String,
                timestamp Int64,
                is_deleted UInt8
            ) ENGINE = MergeTree()
            ORDER BY (block, id)",
        )
        .await
        .expect("Failed to create ClickHouse table");

    // Keep a tiny low range to ensure we can distinguish a true high-cursor resume
    // from a full restart in assertions.
    let mut values: Vec<String> = (0..50)
        .map(|i| {
            format!(
                "({}, 'low_{}', 'low_data_{}', {}, 0)",
                i,
                i,
                i,
                1_700_000_000 + i
            )
        })
        .collect();

    // High range is intentionally far from zero. With block_range=100, a reset to 0
    // would require scanning many empty ranges before reaching this cursor.
    let high_start = 5_000_000i64;
    let high_rows = 20_000i64;
    values.extend((0..high_rows).map(|i| {
        let block = high_start + i;
        format!(
            "({}, 'high_{}', 'high_data_{}', {}, 0)",
            block,
            i,
            i,
            1_800_000_000 + i
        )
    }));

    for chunk in values.chunks(200) {
        clickhouse
            .execute(&format!(
                "INSERT INTO hybrid_resume_test (block, id, data, timestamp, is_deleted) VALUES {}",
                chunk.join(", ")
            ))
            .await
            .expect("Failed to insert ClickHouse data");
    }

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    clickhouse
        .execute(
            "CREATE TABLE kafka_offsets_hybrid_resume (
                topic String,
                partition Int32,
                offset UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");

    let state_table = format!("hybrid_resume_{}", ctx.test_id.replace("-", "_"));
    let application_id = format!("hybrid_resume_{}", ctx.test_id);
    let checkpoint_interval_sec =
        std::env::var("STREAMLING_E2E_HYBRID_RESUME_CHECKPOINT_INTERVAL_SEC")
            .unwrap_or_else(|_| "1".to_string());

    // Ensure ClickHouse inserts are visible before starting run 1.
    let expected_high_rows = high_rows as u64;
    let ready_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while tokio::time::Instant::now() < ready_deadline {
        let visible_rows = clickhouse
            .count("SELECT COUNT(*) FROM hybrid_resume_test WHERE id LIKE 'high_%'")
            .await
            .expect("Failed to query ClickHouse visibility for run 1");
        if visible_rows >= expected_high_rows {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let pipeline_run1 = format!(
        r#"
sources:
  hybrid_source:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: hybrid_resume_test
        columns: block,id,data,timestamp
        start_at: "{high_start}"
    unbounded_source:
      source_type: kafka
      topic: {kafka_topic}
      start_at: earliest
    offset_table:
      topic_name: {kafka_topic}
      table_name: kafka_offsets_hybrid_resume
    primary_key: id

transforms: {{}}

sinks:
  blackhole_sink:
    type: blackhole
    from: hybrid_source
"#,
        high_start = high_start,
        kafka_topic = ctx.kafka_topic
    );

    let status_1 = ctx
        .run_pipeline_with_opts(
            &pipeline_run1,
            PipelineOpts::new()
                .record_limit(800)
                .timeout(std::time::Duration::from_secs(240))
                .env("STREAMLING__APPLICATION_ID", &application_id)
                .env("STREAMLING__STATE_BACKEND__BACKEND_TYPE", "Postgres")
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__HOST",
                    &ctx.postgres.host,
                )
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__PORT",
                    ctx.postgres.port.to_string(),
                )
                .env("STREAMLING__STATE_BACKEND__POSTGRES__USER", "postgres")
                .env("STREAMLING__STATE_BACKEND__POSTGRES__PASSWORD", "postgres")
                .env("STREAMLING__STATE_BACKEND__POSTGRES__DB", &ctx.pg_database)
                .env("STREAMLING__STATE_BACKEND__POSTGRES__SSLMODE", "disable")
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__STATE_TABLE_NAME",
                    &state_table,
                )
                .env(
                    "STREAMLING__CHECKPOINT_INTERVAL_SEC",
                    &checkpoint_interval_sec,
                )
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE", "1")
                .env("STREAMLING__CLICKHOUSE_SOURCE__BLOCK_RANGE", "100")
                .env("STREAMLING__JOB_MODE", "true"),
        )
        .await
        .expect("Pipeline run 1 failed");

    assert!(status_1.success(), "Pipeline run 1 should succeed");

    // Poll for persisted clickhouse split checkpoint key.
    // This is the hard precondition for a valid resume assertion in run 2.
    let poll_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut state_keys: Vec<String> = Vec::new();
    let mut has_clickhouse_split = false;
    while tokio::time::Instant::now() < poll_deadline {
        let rows: Vec<(String,)> = ctx
            .postgres
            .query(&format!(
                "SELECT \"key\" FROM streamling.\"{}\" ORDER BY \"key\"",
                state_table
            ))
            .await
            .expect("Failed to query state keys");
        state_keys = rows.into_iter().map(|row| row.0).collect();
        has_clickhouse_split = state_keys
            .iter()
            .any(|k| k.starts_with("clickhouse_source:"));
        if has_clickhouse_split {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(
        has_clickhouse_split,
        "Run 1 did not persist clickhouse split key within timeout; state keys={:?}",
        state_keys
    );

    let run1_offset_count = clickhouse
        .count("SELECT COUNT(*) FROM kafka_offsets_hybrid_resume")
        .await
        .expect("Failed to query Kafka offset table after run 1");
    assert_eq!(
        run1_offset_count, 0,
        "Run 1 should stay in bounded stage and avoid unbounded Kafka offsets writes"
    );

    // Reset only the hybrid phase-state key so run 2 starts from the bounded phase.
    // Keep ClickHouse split checkpoint keys intact so the bounded source can resume
    // from the saved split cursor.
    ctx.postgres
        .execute(&format!(
            "DELETE FROM streamling.\"{}\" WHERE \"key\" = 'hybrid_source_hybrid_source_v1'",
            state_table
        ))
        .await
        .expect("Failed to clear hybrid phase-state key");

    let pipeline_run2 = format!(
        r#"
sources:
  hybrid_source:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: hybrid_resume_test
        columns: block,id,data,timestamp
    unbounded_source:
      source_type: kafka
      topic: {kafka_topic}
      start_at: earliest
    offset_table:
      topic_name: {kafka_topic}
      table_name: kafka_offsets_hybrid_resume
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: hybrid_source
    table: hybrid_resume_run2
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#,
        kafka_topic = ctx.kafka_topic
    );

    let status_2 = ctx
        .run_pipeline_with_opts(
            &pipeline_run2,
            PipelineOpts::new()
                .record_limit(40)
                .timeout(std::time::Duration::from_secs(120))
                .env("STREAMLING__APPLICATION_ID", &application_id)
                .env("STREAMLING__STATE_BACKEND__BACKEND_TYPE", "Postgres")
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__HOST",
                    &ctx.postgres.host,
                )
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__PORT",
                    ctx.postgres.port.to_string(),
                )
                .env("STREAMLING__STATE_BACKEND__POSTGRES__USER", "postgres")
                .env("STREAMLING__STATE_BACKEND__POSTGRES__PASSWORD", "postgres")
                .env("STREAMLING__STATE_BACKEND__POSTGRES__DB", &ctx.pg_database)
                .env("STREAMLING__STATE_BACKEND__POSTGRES__SSLMODE", "disable")
                .env(
                    "STREAMLING__STATE_BACKEND__POSTGRES__STATE_TABLE_NAME",
                    &state_table,
                )
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__CLICKHOUSE_SOURCE__PAGE_SIZE", "20")
                .env("STREAMLING__CLICKHOUSE_SOURCE__BLOCK_RANGE", "100")
                .env("STREAMLING__JOB_MODE", "true"),
        )
        .await
        .expect("Pipeline run 2 failed");

    assert!(status_2.success(), "Pipeline run 2 should succeed");

    let count_run2 = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hybrid_resume_run2")
        .await
        .expect("Failed to query run 2 count");
    assert!(count_run2 > 0, "Run 2 should process resumed rows");

    let high_count_run2 = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hybrid_resume_run2 WHERE id LIKE 'high_%'")
        .await
        .expect("Failed to query high_* count for run 2");
    assert!(
        high_count_run2 > 0,
        "Run 2 should include bounded high_* rows from resumed ClickHouse scan, got {}",
        high_count_run2
    );

    let low_count_run2 = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hybrid_resume_run2 WHERE id LIKE 'low_%'")
        .await
        .expect("Failed to query low_* count for run 2");
    assert_eq!(
        low_count_run2, 0,
        "Run 2 should not restart from low_* rows, got {} low_* rows",
        low_count_run2
    );

    let min_high_block_run2: Vec<(i64,)> = ctx
        .postgres
        .query("SELECT MIN(block) FROM public.hybrid_resume_run2 WHERE id LIKE 'high_%'")
        .await
        .expect("Failed to query min high block for run 2");
    let min_high_block_run2 = min_high_block_run2[0].0;

    assert!(
        min_high_block_run2 >= high_start,
        "Run 2 high_* rows should remain in high range (min_high_block={}, high_start={})",
        min_high_block_run2,
        high_start
    );
}

// ============================================================================
// Scenario 5: Job mode validation rejects non-hybrid sources
// ============================================================================

/// Test that enabling job_mode on a pipeline without hybrid sources fails at
/// validation with a clear error message listing the unsupported source(s).
#[tokio::test]
async fn test_job_mode_validation_rejects_non_hybrid() {
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
            data: format!("data_{}", i),
            timestamp: i * 100,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"
sources:
  my_kafka_source:
    type: kafka
    topic: {topic}
    primary_key: id

transforms: {{}}

sinks:
  blackhole_sink:
    type: blackhole
    from: my_kafka_source
"#,
        topic = ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_raw(
            &pipeline,
            PipelineOpts::new()
                .env("STREAMLING__JOB_MODE", "true")
                .timeout(std::time::Duration::from_secs(30)),
        )
        .await
        .expect("Failed to run pipeline");

    assert!(
        !output.status.success(),
        "Pipeline should fail when job_mode is enabled without hybrid sources"
    );

    let combined_output = format!("{}\n{}", output.stdout, output.stderr);
    assert!(
        combined_output.contains("job_mode")
            && combined_output.contains("do not support it")
            && combined_output.contains("'my_kafka_source'"),
        "Error should mention job_mode, unsupported source, and the source name, got:\n{}",
        combined_output
    );
}

// ============================================================================
// Scenario 6: Checkpoints fire on interval after unbounded transition
// ============================================================================

/// Verify that once a hybrid pipeline transitions from the bounded phase to the
/// unbounded phase, checkpoint-driven state persistence continues to happen on
/// the configured `CHECKPOINT_INTERVAL_SEC` cadence.
///
/// Strategy:
/// 1. Run a hybrid (ClickHouse bounded → Kafka unbounded) pipeline against a
///    Postgres state backend with `CHECKPOINT_INTERVAL_SEC=1`.
/// 2. Concurrently produce one Kafka record every ~200ms so the kafka source's
///    persisted offset state genuinely changes on each checkpoint.
/// 3. Wait for the kafka source state key (`hybrid_source:<topic>:<partition>`)
///    to appear in `streamling.<state_table>` — this signals the unbounded phase
///    has begun (the hybrid source only engages the kafka provider after
///    bounded phases finish).
/// 4. Sample that row's JSONB content every 250ms for 10s. Each distinct value
///    across consecutive samples counts as one checkpoint-driven state change.
/// 5. Assert at least 5 change events (loose lower bound; expected ~8–10 for a
///    1s interval over a 10s window) and that the median gap between consecutive
///    change events sits in [500ms, 3000ms].
#[tokio::test]
async fn test_hybrid_source_checkpoints_after_unbounded_transition() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    // Small bounded dataset so the bounded phase completes quickly.
    clickhouse
        .execute(
            "CREATE TABLE hybrid_unbounded_ckpt_test (
                block Int64,
                id String,
                data String,
                timestamp Int64,
                is_deleted UInt8
            ) ENGINE = MergeTree()
            ORDER BY (block, id)",
        )
        .await
        .expect("Failed to create ClickHouse table");

    clickhouse
        .execute(
            "INSERT INTO hybrid_unbounded_ckpt_test VALUES
            (1, 'Alice', 'A', 0, 0),
            (2, 'Bob', 'B', 0, 0),
            (3, 'Charlie', 'C', 0, 0)",
        )
        .await
        .expect("Failed to insert ClickHouse data");

    // Register schema and bootstrap a handful of kafka records so the kafka
    // source has something to consume immediately upon transition.
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let bootstrap: Vec<TestRecord> = (1..=5)
        .map(|i| TestRecord {
            block: 100 + i,
            id: format!("bootstrap_{}", i),
            data: format!("boot_data_{}", i),
            timestamp: 2000 + i,
        })
        .collect();
    ctx.kafka
        .produce_avro_records(&bootstrap)
        .await
        .expect("Failed to produce bootstrap Kafka records");

    // Offset table is required by the hybrid offset_provider.
    clickhouse
        .execute(
            "CREATE TABLE kafka_offsets_unbounded_ckpt (
                topic String,
                partition Int32,
                offset UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");

    let state_table = format!("hybrid_unbounded_ckpt_{}", ctx.test_id.replace('-', "_"));
    let application_id = format!("hybrid_unbounded_ckpt_{}", ctx.test_id);

    let pipeline = format!(
        r#"
sources:
  hybrid_source:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: hybrid_unbounded_ckpt_test
        columns: block,id,data,timestamp
    unbounded_source:
      source_type: kafka
      topic: {kafka_topic}
      start_at: earliest
    offset_table:
      topic_name: {kafka_topic}
      table_name: kafka_offsets_unbounded_ckpt
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: hybrid_source
    table: hybrid_unbounded_ckpt_results
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 10
    batch_flush_interval: 100ms
"#,
        kafka_topic = ctx.kafka_topic,
    );

    let opts = PipelineOpts::new()
        // Sized for the producer that runs for ~25 s at 5 rec/s: 3 bounded +
        // 5 bootstrap + ~125 streamed ≈ 130 max kafka records reach the
        // sink. record_limit=100 keeps the pipeline running through the 10 s
        // observation window and then terminates cleanly via
        // STREAMLING__NUM_RECORDS_BEFORE_STOP before kafka stalls.
        .record_limit(100)
        .timeout(std::time::Duration::from_secs(90))
        .env("STREAMLING__APPLICATION_ID", &application_id)
        .env("STREAMLING__STATE_BACKEND__BACKEND_TYPE", "Postgres")
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__HOST",
            &ctx.postgres.host,
        )
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__PORT",
            ctx.postgres.port.to_string(),
        )
        .env("STREAMLING__STATE_BACKEND__POSTGRES__USER", "postgres")
        .env("STREAMLING__STATE_BACKEND__POSTGRES__PASSWORD", "postgres")
        .env("STREAMLING__STATE_BACKEND__POSTGRES__DB", &ctx.pg_database)
        .env("STREAMLING__STATE_BACKEND__POSTGRES__SSLMODE", "disable")
        .env(
            "STREAMLING__STATE_BACKEND__POSTGRES__STATE_TABLE_NAME",
            &state_table,
        )
        .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
        .env("STREAMLING__RECORD_BATCH_SIZE", "10");

    let pipeline_fut = ctx.run_pipeline_with_opts(&pipeline, opts);

    // CI-tunable knob (see scenario 4's `STREAMLING_E2E_HYBRID_RESUME_*` for
    // the precedent). Default 10s gives ~10 expected change events at the 1s
    // checkpoint interval. The producer deadline below is derived from this
    // so bumping the window automatically extends production.
    let sample_window_secs: u64 =
        std::env::var("STREAMLING_E2E_HYBRID_UNBOUNDED_SAMPLE_WINDOW_SEC")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);

    // Background producer: ~5 records/sec, runs longer than the observation
    // window so the kafka source's persisted offset keeps advancing on every
    // checkpoint until the observer is done.
    let producer_window_secs = sample_window_secs + 15;
    let producer_fut = async {
        let producer_deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs(producer_window_secs);
        let mut next_id: u64 = 1;
        while tokio::time::Instant::now() < producer_deadline {
            let rec = TestRecord {
                block: 200 + next_id as i64,
                id: format!("live_user_{}", next_id),
                data: format!("live_data_{}", next_id),
                timestamp: 3000 + next_id as i64,
            };
            // Producer errors are non-fatal: the pipeline (or test) may have
            // already terminated, in which case observer assertions own the
            // pass/fail decision.
            if let Err(e) = ctx.kafka.produce_avro_records(&[rec]).await {
                tracing::warn!("Producer side task error (non-fatal): {}", e);
                break;
            }
            next_id += 1;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    };

    // Observer: phase 1 waits for unbounded transition, phase 2 samples,
    // phase 3 asserts on change-event count and cadence.
    let observer_fut = async {
        // Phase 1: wait up to 90s for the kafka source state key to appear.
        // Hybrid's unbounded provider only writes that key once the bounded
        // phases have completed, so its presence is a reliable transition
        // signal. The state table is created lazily by streamling, so query
        // errors during early polls are expected; we treat them as "not yet".
        let key_query = format!(
            "SELECT key FROM streamling.\"{}\" \
             WHERE key LIKE 'hybrid_source:%' \
             ORDER BY key",
            state_table
        );

        let transition_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(90);
        let mut kafka_key: Option<String> = None;
        while tokio::time::Instant::now() < transition_deadline {
            if let Ok(rows) = ctx.postgres.query::<(String,)>(&key_query).await {
                if let Some(first) = rows.first() {
                    kafka_key = Some(first.0.clone());
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }

        let kafka_key = match kafka_key {
            Some(k) => k,
            None => {
                // Dump whatever state keys we can see for diagnostics.
                let all_keys_query = format!(
                    "SELECT key FROM streamling.\"{}\" ORDER BY key",
                    state_table
                );
                let all_keys = ctx
                    .postgres
                    .query::<(String,)>(&all_keys_query)
                    .await
                    .unwrap_or_default();
                panic!(
                    "Hybrid pipeline did not transition to unbounded phase within 90s; \
                     no key matching 'hybrid_source:%' in state table '{}'. \
                     Observed keys: {:?}",
                    state_table,
                    all_keys.iter().map(|r| &r.0).collect::<Vec<_>>()
                );
            }
        };
        tracing::info!("Observed kafka source state key: {}", kafka_key);

        // Phase 2: sample the kafka state row's JSONB content every 250ms
        // for 10s. `data::text` produces a stable textual encoding of the
        // JSONB value, so byte-equal text ↔ semantically equal state.
        let kafka_key_sql_escaped = kafka_key.replace('\'', "''");
        let data_query = format!(
            "SELECT data::text FROM streamling.\"{}\" WHERE key = '{}'",
            state_table, kafka_key_sql_escaped
        );

        // CI-tunable change-event floor (mirrors the
        // `STREAMLING_E2E_HYBRID_RESUME_*` pattern in scenario 4). Default 5
        // is a loose lower bound for a 10s window at 1s checkpoint interval;
        // if a slow CI runner proves flaky, lower this without touching the
        // test source.
        let min_change_events: usize =
            std::env::var("STREAMLING_E2E_HYBRID_UNBOUNDED_MIN_CHANGE_EVENTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5);

        let sample_start = tokio::time::Instant::now();
        let sample_window = std::time::Duration::from_secs(sample_window_secs);
        let sample_interval = std::time::Duration::from_millis(250);
        let mut samples: Vec<(u64, String)> = Vec::new();
        while sample_start.elapsed() < sample_window {
            if let Ok(rows) = ctx.postgres.query::<(String,)>(&data_query).await {
                if let Some(row) = rows.first() {
                    samples.push((sample_start.elapsed().as_millis() as u64, row.0.clone()));
                }
            }
            tokio::time::sleep(sample_interval).await;
        }

        assert!(
            !samples.is_empty(),
            "Observer captured zero samples for kafka source state row '{}'",
            kafka_key
        );

        // Phase 3: count change events and analyze gaps.
        let change_offsets_ms: Vec<u64> = samples
            .windows(2)
            .filter_map(|w| if w[0].1 != w[1].1 { Some(w[1].0) } else { None })
            .collect();

        tracing::info!(
            "Captured {} samples; {} change events at offsets (ms): {:?}",
            samples.len(),
            change_offsets_ms.len(),
            change_offsets_ms
        );

        // Lower bound is loose: 1s interval over N s window ≈ N events; require
        // `min_change_events` (default 5 for a 10s window).
        assert!(
            change_offsets_ms.len() >= min_change_events,
            "Expected >= {} checkpoint-driven state changes in the {}s window \
             after the unbounded transition, got {}. samples={}, \
             change_offsets_ms={:?}, final_state_jsonb_text={:?}",
            min_change_events,
            sample_window_secs,
            change_offsets_ms.len(),
            samples.len(),
            change_offsets_ms,
            samples.last().map(|s| &s.1)
        );

        // Median gap should sit near CHECKPOINT_INTERVAL_SEC=1s. Allow a wide
        // band to absorb scheduling jitter and the 250ms polling alias.
        let mut gaps: Vec<u64> = change_offsets_ms.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(
            !gaps.is_empty(),
            "Cannot compute checkpoint-interval gaps from < 2 change events"
        );
        gaps.sort_unstable();
        let median_gap_ms = gaps[gaps.len() / 2];
        assert!(
            (500..=3000).contains(&median_gap_ms),
            "Median gap between checkpoint-driven state changes was {} ms; \
             expected within [500, 3000] (CHECKPOINT_INTERVAL_SEC=1). gaps_ms={:?}",
            median_gap_ms,
            gaps
        );
    };

    let (pipeline_result, _, _) = tokio::join!(pipeline_fut, producer_fut, observer_fut);
    let status = pipeline_result.expect("Pipeline execution failed");
    assert!(status.success(), "Pipeline should complete successfully");

    // Sanity check: at minimum the bounded ClickHouse rows should have landed
    // in the sink. We don't bound the upper count because the pipeline keeps
    // ingesting unbounded records up to `record_limit`.
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.hybrid_unbounded_ckpt_results")
        .await
        .expect("Failed to query sink count");
    assert!(
        count >= 3,
        "Sink should contain at least 3 bounded records, got {}",
        count
    );
}

// ============================================================================
// Scenario: Hybrid source ClickHouse Nullable(UInt128) → u256 endianness
// ============================================================================

/// Avro schema with a Decimal logical type at precision > 76 — converts to
/// streamling u256 (FixedSizeBinary(32) + u256 extension metadata).
const U256_SCHEMA: &str = r#"{
    "type": "record",
    "name": "U256TestMessage",
    "fields": [
        {"name": "block", "type": "long"},
        {"name": "id", "type": "string"},
        {"name": "wei_value", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 78, "scale": 0}], "default": null}
    ]
}"#;

/// Regression for the ClickHouse → streamling u256 endianness asymmetry.
///
/// ClickHouse stores UInt128/UInt256 little-endian and the hybrid layer's
/// schema adapter widens `Nullable(UInt128)` to `Nullable(UInt256)` via a
/// server-side CAST. ClickHouse emits UInt256 as Arrow `FixedSizeBinary(32)`
/// in little-endian, while streamling stores big-endian. Before
/// `normalize_batch_from_clickhouse` was added on the read side, a logical
/// `1` arrived as `2^248` and downstream u256 arithmetic / serialization
/// produced garbage.
///
/// Job mode lets the bounded phase finish and exits without consuming Kafka,
/// so the unbounded source is only present to declare the u256 schema (which
/// drives the bounded CAST + the read-side normalization).
#[tokio::test]
async fn test_hybrid_clickhouse_uint128_widened_to_u256_preserves_value() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE hybrid_u256_test (
                block Int64,
                id String,
                wei_value Nullable(UInt128),
                is_deleted UInt8
            ) ENGINE = MergeTree()
            ORDER BY (block, id)",
        )
        .await
        .expect("Failed to create ClickHouse table");

    // Spread of magnitudes that would all collapse to ~2^248-range garbage if
    // the LE-stored bytes were re-read big-endian.
    clickhouse
        .execute(
            "INSERT INTO hybrid_u256_test (block, id, wei_value, is_deleted) VALUES
            (1, 'one',  1, 0),
            (2, 'eth',  1000000000000000000, 0),
            (3, 'gwei', 100000000000, 0),
            (4, 'null', NULL, 0)",
        )
        .await
        .expect("Failed to insert");

    // Register the unbounded schema. We never produce Kafka records — job mode
    // terminates after the bounded phase — but the topic + schema must exist
    // because the hybrid validates the unbounded schema at startup and uses it
    // as the source of truth for u256 metadata.
    ctx.kafka
        .register_schema(U256_SCHEMA)
        .await
        .expect("Failed to register schema");

    clickhouse
        .execute(
            "CREATE TABLE kafka_offsets_u256 (
                topic String,
                partition Int32,
                offset UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");

    let pipeline = format!(
        r#"
sources:
  hybrid_source:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: hybrid_u256_test
        columns: block,id,wei_value
    unbounded_source:
      source_type: kafka
      topic: {kafka_topic}
      start_at: earliest
    offset_table:
      topic_name: {kafka_topic}
      table_name: kafka_offsets_u256
    primary_key: id

transforms:
  stringify:
    from: hybrid_source
    sql: "SELECT block, id, u256_to_string(wei_value) AS wei_str FROM hybrid_source"

sinks:
  pg_sink:
    type: postgres
    from: stringify
    table: hybrid_u256_results
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#,
        kafka_topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .env("STREAMLING__JOB_MODE", "true")
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Pipeline execution failed");

    assert!(
        status.success(),
        "Pipeline should exit successfully in job mode"
    );

    let rows: Vec<(String, Option<String>)> = ctx
        .postgres
        .query("SELECT id, wei_str FROM public.hybrid_u256_results ORDER BY id")
        .await
        .expect("Failed to query postgres");

    let by_id: std::collections::HashMap<String, Option<String>> = rows.into_iter().collect();

    // Logical 1 must arrive as "1", not "452312848583266388373324160190187140051835877600158453279131187530910662656"
    // (which is 2^248, the value bytes_to_u256 yields when fed LE bytes for 1).
    assert_eq!(
        by_id.get("one"),
        Some(&Some("1".to_string())),
        "Bounded ClickHouse u256 value 1 must arrive intact, got {:?}",
        by_id.get("one")
    );
    assert_eq!(
        by_id.get("eth"),
        Some(&Some("1000000000000000000".to_string())),
        "Bounded ClickHouse u256 value 1e18 must arrive intact, got {:?}",
        by_id.get("eth")
    );
    assert_eq!(
        by_id.get("gwei"),
        Some(&Some("100000000000".to_string())),
        "Bounded ClickHouse u256 value 1e11 must arrive intact, got {:?}",
        by_id.get("gwei")
    );
    assert_eq!(
        by_id.get("null"),
        Some(&None),
        "Nullable u256 must propagate NULL, got {:?}",
        by_id.get("null")
    );
}
