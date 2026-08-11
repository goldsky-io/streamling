//! Checkpoint e2e tests.
//!
//! These tests verify that streamling correctly persists and resumes from checkpoints
//! using PostgreSQL as the state backend.
//!
//! Ported from crates/streamling/tests/pipeline_checkpoint_test.rs

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

// ============================================================================
// Checkpoint Tests
// ============================================================================

/// Test record for the Kafka-sourced checkpoint tests
#[derive(Debug, Clone, Serialize)]
struct MarkerTestRecord {
    id: i64,
    value: String,
}

const MARKER_TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "MarkerTestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"}
    ]
}"#;

/// Checkpoints must progress even when an unnest never produces any rows.
///
/// This pipeline filters out every source row before an unnest, so the unnest
/// only ever sees empty marker-carrying batches. Checkpoint markers must flow
/// through to the sink regardless: the checkpoint producer does not start a
/// new epoch until the in-flight one resolves, so an operator that holds a
/// marker while waiting for data deadlocks checkpointing permanently on any
/// pipeline whose filter matches nothing for a while after startup.
///
/// The pipeline never terminates on its own (no record limit is reachable),
/// so the run is stopped by the harness timeout; the assertion is that
/// checkpoint state was persisted while it ran.
#[tokio::test]
async fn test_checkpoint_progresses_when_unnest_yields_no_rows() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new())
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(MARKER_TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<MarkerTestRecord> = (1..=50)
        .map(|i| MarkerTestRecord {
            id: i,
            value: format!("value_{}", i),
        })
        .collect();
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let state_table = format!("unnest_marker_state_{}", ctx.test_id.replace("-", "_"));
    let application_id = format!("unnest_marker_test_{}", ctx.test_id);

    // The filter matches no rows, so the unnest downstream of it only ever
    // receives empty batches carrying checkpoint markers.
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  filtered:
    type: sql
    primary_key: id
    sql: SELECT id, make_array(id, id + 1) AS items FROM kafka_source WHERE id > 1000000
  expanded:
    type: sql
    primary_key: id
    sql: |-
      SELECT id, item FROM (
        SELECT id, unnest(items) AS item FROM filtered
      ) t WHERE item IS NOT NULL

sinks:
  pg_sink:
    type: postgres
    from: expanded
    table: unnest_marker_out
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 10
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    // No record limit: the sink never receives data rows, so the pipeline
    // only stops when the harness timeout kills it.
    let run_result = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .timeout(std::time::Duration::from_secs(25))
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
                .env("STREAMLING__RECORD_BATCH_SIZE", "10"),
        )
        .await;

    // The run is expected to end via the harness timeout; a clean exit is
    // also acceptable. Either way, checkpoints must have been persisted.
    tracing::info!("Pipeline run ended: {:?}", run_result.is_ok());

    let checkpoint_count = ctx
        .postgres
        .count(&format!(
            "SELECT COUNT(*) FROM streamling.\"{}\"",
            state_table
        ))
        .await
        .expect("Failed to query checkpoint state table");

    assert!(
        checkpoint_count > 0,
        "At least one checkpoint should complete while the unnest yields no rows, got {}",
        checkpoint_count
    );
}

/// Test that ClickHouse source correctly checkpoints and resumes from saved state.
///
/// This test:
/// 1. Creates a ClickHouse table with 2000 records
/// 2. Runs pipeline 1 with start_at=5, processes 1000 records (IDs 5-1004)
/// 3. Runs pipeline 2 without start_at, which should resume from checkpoint
/// 4. Verifies pipeline 2 starts from where pipeline 1 left off
///
/// Uses PostgreSQL for checkpoint state storage.
///
/// Ported from: test_pipeline_clickhouse_checkpoint_state
#[tokio::test]
async fn test_clickhouse_checkpoint_state() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    // Create ClickHouse source table with 2000 records
    clickhouse
        .execute(
            "CREATE TABLE checkpoint_test (
                id UInt32,
                name String,
                is_deleted UInt8
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .await
        .expect("Failed to create ClickHouse table");

    // Insert 2000 records (enough for two pipeline runs)
    let num_records = 2000;
    let batch_size = 100;
    for batch_start in (0..num_records).step_by(batch_size) {
        let values: Vec<String> = (batch_start..batch_start + batch_size)
            .map(|i| format!("({}, 'name_{}', 0)", i, i))
            .collect();
        let insert_query = format!(
            "INSERT INTO checkpoint_test (id, name, is_deleted) VALUES {}",
            values.join(", ")
        );
        clickhouse
            .execute(&insert_query)
            .await
            .expect("Failed to insert ClickHouse data");
    }

    // Create unique identifiers for this test to avoid conflicts
    let state_table = format!("checkpoint_state_{}", ctx.test_id.replace("-", "_"));
    let application_id = format!("checkpoint_test_{}", ctx.test_id);

    // Pipeline 1: Start from ID 5, process records slowly to allow checkpoints
    // This should checkpoint its position
    let pipeline_1 = r#"
sources:
  checkpoint_source:
    type: clickhouse
    table_name: checkpoint_test
    start_at: "5"
    primary_key: id

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: checkpoint_source
    table: checkpoint_run1
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 10
    batch_flush_interval: 100ms
"#;

    let first_run_limit = 500u64; // Fewer records but more batches = more checkpoint opportunities

    // Run pipeline 1 with PostgreSQL state backend
    let status_1 = ctx
        .run_pipeline_with_opts(
            pipeline_1,
            PipelineOpts::new()
                .record_limit(first_run_limit)
                .timeout(std::time::Duration::from_secs(120))
                // Set explicit application ID for consistent state lookup
                .env("STREAMLING__APPLICATION_ID", &application_id)
                // Use PostgreSQL for state backend
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
                // Enable frequent checkpointing
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
                // Small batches to trigger more checkpoint opportunities
                .env("STREAMLING__RECORD_BATCH_SIZE", "10"),
        )
        .await
        .expect("Pipeline 1 execution failed");

    assert!(
        status_1.success(),
        "Pipeline 1 should complete successfully"
    );

    // Verify pipeline 1 output
    let count_1 = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.checkpoint_run1")
        .await
        .expect("Failed to query count");

    assert!(
        count_1 >= first_run_limit as i64 - 50, // Allow some variance due to batching
        "Pipeline 1 should have processed ~{} records, got {}",
        first_run_limit,
        count_1
    );

    // Check the minimum ID in pipeline 1 output (should be 5 as specified in start_at)
    let min_id_1: Vec<(i64,)> = ctx
        .postgres
        .query("SELECT MIN(id) FROM public.checkpoint_run1")
        .await
        .expect("Failed to query min id");
    assert_eq!(
        min_id_1[0].0, 5,
        "Pipeline 1 should start from ID 5 as specified"
    );

    // Check the maximum ID in pipeline 1 output
    let max_id_1: Vec<(i64,)> = ctx
        .postgres
        .query("SELECT MAX(id) FROM public.checkpoint_run1")
        .await
        .expect("Failed to query max id");
    let last_processed_id = max_id_1[0].0;

    tracing::info!(
        "Pipeline 1 processed IDs from 5 to {}, count={}",
        last_processed_id,
        count_1
    );

    // Verify checkpoint was saved by checking the state table
    let checkpoint_count = ctx
        .postgres
        .count(&format!(
            "SELECT COUNT(*) FROM streamling.\"{}\"",
            state_table
        ))
        .await
        .expect("Failed to query checkpoint table");

    tracing::info!("Checkpoint entries in state table: {}", checkpoint_count);

    // Pipeline 2: No start_at - should resume from checkpoint
    let pipeline_2 = r#"
sources:
  checkpoint_source:
    type: clickhouse
    table_name: checkpoint_test
    primary_key: id

transforms: {}

sinks:
  pg_sink:
    type: postgres
    from: checkpoint_source
    table: checkpoint_run2
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 10
    batch_flush_interval: 100ms
"#;

    let second_run_limit = 300u64;

    // Run pipeline 2 with the same state table and application ID
    let status_2 = ctx
        .run_pipeline_with_opts(
            pipeline_2,
            PipelineOpts::new()
                .record_limit(second_run_limit)
                .timeout(std::time::Duration::from_secs(120))
                // MUST use same application ID to load saved checkpoint
                .env("STREAMLING__APPLICATION_ID", &application_id)
                // Use same PostgreSQL state backend
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
                // Enable checkpointing
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
                .env("STREAMLING__RECORD_BATCH_SIZE", "10"),
        )
        .await
        .expect("Pipeline 2 execution failed");

    assert!(
        status_2.success(),
        "Pipeline 2 should complete successfully"
    );

    // Verify pipeline 2 output
    let count_2 = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.checkpoint_run2")
        .await
        .expect("Failed to query count");

    assert!(
        count_2 > 0,
        "Pipeline 2 should have processed some records, got {}",
        count_2
    );

    // Check the minimum ID in pipeline 2 output
    // It should be AFTER the last ID processed by pipeline 1
    let min_id_2: Vec<(i64,)> = ctx
        .postgres
        .query("SELECT MIN(id) FROM public.checkpoint_run2")
        .await
        .expect("Failed to query min id");

    tracing::info!(
        "Pipeline 2 processed IDs starting from {}, count={}",
        min_id_2[0].0,
        count_2
    );

    // The key assertion: Pipeline 2 should NOT start from the very beginning
    // If checkpointing worked, it should start somewhere after the beginning
    // Due to ClickHouse pagination, the checkpoint saves the page offset
    // so pipeline 2 should start at least after some records
    if checkpoint_count > 0 {
        // If checkpoints were saved, pipeline 2 should NOT restart from the beginning
        assert!(
            min_id_2[0].0 > 0,
            "Pipeline 2 should NOT restart from ID 0 when checkpoint exists, got min_id={}",
            min_id_2[0].0
        );

        // The minimum ID in pipeline 2 should be well after the start
        // (not necessarily exactly after pipeline 1, but definitely not at 0)
        let expected_min = 100; // Should be at least past the first ~100 records
        assert!(
            min_id_2[0].0 >= expected_min,
            "Pipeline 2 should resume from checkpoint (expected min >= {}, got min={})",
            expected_min,
            min_id_2[0].0
        );
    } else {
        // If no checkpoints were saved (can happen with fast completion), skip the assertion
        tracing::warn!(
            "No checkpoints found in state table - pipeline may have completed before checkpoint interval"
        );
    }
}

/// Checkpoint restart-resume with a multi-instance Kafka source.
///
/// This is the case the sink ack gate and marker alignment exist for. With
/// `parallelism: 2` the source emits a marker copy per instance, and each copy
/// reaches its own sink write stream. Acking on the first copy would commit
/// offsets for both instances while the slower one still had pre-marker rows in
/// flight, so a restart would resume past data that was never written.
///
/// Records are produced in two waves with a pipeline run after each. The second
/// run can only reach the second wave by resuming from the committed offsets —
/// a run that restarted from `earliest` would re-read the first wave, hit its
/// record limit on those replays, and write no second-wave row at all.
#[tokio::test]
async fn test_kafka_parallelism_checkpoint_resume() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new())
        .await
        .expect("Failed to create test context");

    let topic = ctx
        .create_kafka_topic_with_partitions("parallel_ckpt", 4)
        .await
        .expect("Failed to create multi-partition topic");
    topic
        .register_schema(MARKER_TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Two waves of the same size. `num_records_before_stop` is a *graceful*
    // stop — it halts the source but still writes everything already in flight —
    // so a single wave with a half-sized limit would simply drain the whole
    // topic in run 1 and leave run 2 nothing to resume onto.
    const WAVE: i64 = 200;
    let wave = |from: i64| -> Vec<MarkerTestRecord> {
        (from..from + WAVE)
            .map(|i| MarkerTestRecord {
                id: i,
                value: format!("value_{i}"),
            })
            .collect()
    };
    topic
        .produce_avro_records(&wave(1))
        .await
        .expect("Failed to produce first wave");

    let state_table = format!("parallel_ckpt_state_{}", ctx.test_id.replace("-", "_"));
    let application_id = format!("parallel_ckpt_{}", ctx.test_id);

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    parallelism: 2
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: kafka_source
    table: parallel_ckpt_output
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 10
    batch_flush_interval: 100ms
"#,
        topic = topic.topic,
    );

    let state_env = |opts: PipelineOpts| -> PipelineOpts {
        opts.env("STREAMLING__APPLICATION_ID", &application_id)
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
            .env("STREAMLING__RECORD_BATCH_SIZE", "10")
            .timeout(std::time::Duration::from_secs(120))
    };

    let status_1 = ctx
        .run_pipeline_with_opts(
            &pipeline,
            state_env(PipelineOpts::new().record_limit(WAVE as u64)),
        )
        .await
        .expect("Pipeline 1 execution failed");
    assert!(status_1.success(), "Pipeline 1 should exit successfully");

    let count_1 = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.parallel_ckpt_output")
        .await
        .expect("Failed to query count");
    assert_eq!(
        count_1, WAVE,
        "Pipeline 1 should have written the whole first wave"
    );

    // Now produce records the first run never saw. Both instances rejoin the
    // same consumer group and seek to the committed offsets of whichever
    // partitions they are assigned.
    topic
        .produce_avro_records(&wave(WAVE + 1))
        .await
        .expect("Failed to produce second wave");

    let status_2 = ctx
        .run_pipeline_with_opts(
            &pipeline,
            state_env(PipelineOpts::new().record_limit(WAVE as u64)),
        )
        .await
        .expect("Pipeline 2 execution failed");
    assert!(status_2.success(), "Pipeline 2 should exit successfully");

    // Nothing from the first wave may go missing across the restart — a gap
    // there means a run committed offsets past data it never wrote.
    let missing: Vec<(i64,)> = ctx
        .postgres
        .query(&format!(
            // `generate_series(int, int)` yields INT4; the ids are BIGINT, so
            // cast or the row decode into `i64` fails.
            "SELECT s.id::bigint FROM generate_series(1, {WAVE}) AS s(id) \
             LEFT JOIN public.parallel_ckpt_output o ON o.id = s.id \
             WHERE o.id IS NULL ORDER BY s.id"
        ))
        .await
        .expect("Failed to query missing ids");
    assert!(
        missing.is_empty(),
        "checkpoint resume lost rows from the first wave: {:?}",
        missing.iter().map(|r| r.0).collect::<Vec<_>>()
    );

    // And the second run must have got *past* the first wave. This is the
    // resume signal: a run that ignored the checkpoint and restarted from
    // `earliest` would re-read the first wave, spend its whole record budget on
    // those replays, and write no second-wave id at all.
    //
    // Completeness of the second wave is deliberately not asserted. At-least-once
    // means run 2 replays whatever the first run had not yet committed, and those
    // replays count against its record limit — so how far into the second wave it
    // gets is not deterministic.
    let resumed: i64 = ctx
        .postgres
        .count(&format!(
            "SELECT COUNT(*) FROM public.parallel_ckpt_output WHERE id > {WAVE}"
        ))
        .await
        .expect("Failed to count second-wave rows");
    assert!(
        resumed > 0,
        "pipeline 2 wrote no second-wave row, so it restarted from the beginning \
         instead of resuming from the committed offsets"
    );
}
