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

/// Checkpointing must progress through a `UNION ALL`.
///
/// A union sums its branches' partitions, so each branch delivers its own copy
/// of every checkpoint epoch to the sink. The coordinator finalizes an epoch on
/// the *first* ack per sink name, so before `MarkerAligner` and the per-sink ack
/// gate existed, a union could finalize an epoch — committing source offsets —
/// while the slower branch still had pre-marker rows in flight.
///
/// This pins the two properties that must hold regardless of whether the union's
/// branches are merged back into one stream or written concurrently: epochs
/// actually finalize (checkpoint state is persisted rather than stalling), and
/// no row is lost.
///
/// It does not prove the absence of *premature* finalization — that is a race
/// with no deterministic e2e signal. What it does cover is the regression risk
/// of letting a union stay multi-partition.
#[tokio::test]
async fn test_checkpoint_progresses_through_a_union() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new())
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(MARKER_TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    const RECORDS: i64 = 50;
    let records: Vec<MarkerTestRecord> = (1..=RECORDS)
        .map(|i| MarkerTestRecord {
            id: i,
            value: format!("value_{i}"),
        })
        .collect();
    ctx.kafka
        .produce_avro_records_keyed(&records, |r| r.id.to_string())
        .await
        .expect("Failed to produce records");

    let state_table = format!("union_marker_state_{}", ctx.test_id.replace('-', "_"));
    let application_id = format!("union_marker_{}", ctx.test_id);

    // Each source row flows down both union branches, so the sink sees every id
    // twice and the upsert collapses them back to one row per id.
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  both_branches:
    type: sql
    primary_key: id
    sql: >
      SELECT id, value, _gs_op FROM kafka_source
      UNION ALL
      SELECT id, value, _gs_op FROM kafka_source

sinks:
  pg_sink:
    type: postgres
    from: both_branches
    table: union_marker_out
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    // No record limit: 100 writes finish in well under a second, which is
    // shorter than the checkpoint interval, so a limit-bounded run would exit
    // before a single epoch was ever started. Run on a timeout instead, like
    // the unnest test above, so several epochs have time to finalize.
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
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await;

    // The run ends via the harness timeout; a clean exit is also acceptable.
    tracing::info!("Pipeline run ended: {:?}", run_result.is_ok());

    // Epochs finalized rather than stalling on a marker copy that never arrived.
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
        "at least one checkpoint must finalize while markers flow through the union"
    );

    // Both branches delivered every row.
    let missing: Vec<(i64,)> = ctx
        .postgres
        .query(&format!(
            "SELECT s.id::bigint FROM generate_series(1, {RECORDS}) AS s(id) \
             LEFT JOIN public.union_marker_out o ON o.id = s.id \
             WHERE o.id IS NULL ORDER BY s.id"
        ))
        .await
        .expect("Failed to query missing ids");
    assert!(
        missing.is_empty(),
        "union lost rows: {:?}",
        missing.iter().map(|r| r.0).collect::<Vec<_>>()
    );
}

/// Rows per CSV file in the bounded file source checkpoint tests.
const FILE_ROWS: i64 = 20;

/// Writes `files` CSV files of [`FILE_ROWS`] rows with consecutive ids, so an
/// id's file is `(id - 1) / FILE_ROWS`.
fn write_id_files(ctx: &TestContext, dir_name: &str, files: i64) -> std::path::PathBuf {
    let dir = ctx.temp_dir.path().join(dir_name);
    std::fs::create_dir_all(&dir).expect("create data dir");
    for file in 0..files {
        let mut csv = String::from("id,value\n");
        for row in 0..FILE_ROWS {
            let id = file * FILE_ROWS + row + 1;
            csv.push_str(&format!("{id},value_{id}\n"));
        }
        std::fs::write(dir.join(format!("part_{file:04}.csv")), csv).expect("write csv");
    }
    dir
}

/// A two-partition file source in `mode` into Postgres, one row per INSERT, so a
/// read of a few hundred files spans several one-second checkpoint intervals.
fn file_pipeline(dir: &std::path::Path, table: &str, mode: &str) -> String {
    format!(
        r#"
sources:
  file_src:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    parallelism: 2
    mode:
      type: {mode}

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: file_src
    table: {table}
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        path = dir.display(),
    )
}

async fn written_ids(ctx: &TestContext, table: &str) -> std::collections::BTreeSet<i64> {
    let rows: Vec<(i64,)> = ctx
        .postgres
        .query(&format!("SELECT id FROM public.{table}"))
        .await
        .expect("Failed to query written ids");
    rows.into_iter().map(|row| row.0).collect()
}

/// The ids written to `table` so far — empty while the sink has yet to create
/// it, so a progress wait can poll from before the pipeline starts.
async fn written_ids_so_far(ctx: &TestContext, table: &str) -> std::collections::BTreeSet<i64> {
    let rows: Vec<(i64,)> = ctx
        .postgres
        .query(&format!("SELECT id FROM public.{table}"))
        .await
        .unwrap_or_default();
    rows.into_iter().map(|row| row.0).collect()
}

/// How long a progress wait gives the pipeline before the test goes on to fail
/// on its assertions rather than hang.
const PROGRESS_WAIT_LIMIT: std::time::Duration = std::time::Duration::from_secs(120);
const PROGRESS_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Resolves once `table` holds at least `rows`, so a SIGTERM lands at the same
/// point in the read whatever the machine's speed. The sink creates the table on
/// startup, so a count that fails counts as zero.
async fn wait_for_rows(ctx: &TestContext, table: &str, rows: i64) {
    let deadline = std::time::Instant::now() + PROGRESS_WAIT_LIMIT;
    while std::time::Instant::now() < deadline {
        let written = ctx
            .postgres
            .count(&format!("SELECT COUNT(*) FROM public.{table}"))
            .await
            .unwrap_or(0);
        if written >= rows {
            return;
        }
        tokio::time::sleep(PROGRESS_POLL_INTERVAL).await;
    }
}

/// Resolves once `table` holds at least `rows` rows and the pipeline has
/// persisted checkpoint state, so a SIGTERM lands mid-read and after an epoch
/// finalized — the progress a resumed run needs — whatever the machine's speed.
async fn wait_for_checkpointed_rows(ctx: &TestContext, table: &str, rows: i64, state_table: &str) {
    let deadline = std::time::Instant::now() + PROGRESS_WAIT_LIMIT;
    while std::time::Instant::now() < deadline {
        let written = ctx
            .postgres
            .count(&format!("SELECT COUNT(*) FROM public.{table}"))
            .await
            .unwrap_or(0);
        let checkpointed = ctx
            .postgres
            .count(&format!(
                "SELECT COUNT(*) FROM streamling.\"{state_table}\""
            ))
            .await
            .unwrap_or(0);
        if written >= rows && checkpointed > 0 {
            return;
        }
        tokio::time::sleep(PROGRESS_POLL_INTERVAL).await;
    }
}

/// Resolves once `table` holds every id missing from `already_written`, so a
/// resumed run is stopped when it has caught up rather than after a guessed
/// duration.
async fn wait_until_caught_up(
    ctx: &TestContext,
    table: &str,
    already_written: &std::collections::BTreeSet<i64>,
    total: i64,
) {
    let deadline = std::time::Instant::now() + PROGRESS_WAIT_LIMIT;
    while std::time::Instant::now() < deadline {
        let written = written_ids_so_far(ctx, table).await;
        if (1..=total).all(|id| already_written.contains(&id) || written.contains(&id)) {
            return;
        }
        tokio::time::sleep(PROGRESS_POLL_INTERVAL).await;
    }
}

/// The epoch numbers in log lines of the form `{prefix}{epoch}`.
fn logged_epochs<'a>(logs: &'a str, prefix: &'a str) -> impl Iterator<Item = u64> + 'a {
    logs.lines().filter_map(move |line| {
        let digits: String = line
            .split(prefix)
            .nth(1)?
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        digits.parse().ok()
    })
}

/// A bounded file source takes part in checkpointing: periodic epochs finalize
/// while it reads, and a terminal epoch closes it once every file is read.
///
/// Before, no batch from the bounded scan carried a marker, so the sink never
/// acked and every epoch stalled until the timeout removed it. Unlike the unnest
/// test above, this pipeline terminates on its own.
#[tokio::test]
async fn test_bounded_file_source_checkpoints_while_reading() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new())
        .await
        .expect("Failed to create test context");

    const FILES: i64 = 300;
    let dir = write_id_files(&ctx, "bounded_ckpt_data", FILES);
    let state_table = format!("bounded_file_ckpt_state_{}", ctx.test_id.replace('-', "_"));
    let application_id = format!("bounded_file_ckpt_{}", ctx.test_id);

    let output = ctx
        .run_pipeline_raw(
            &file_pipeline(&dir, "bounded_file_ckpt_out", "bounded"),
            PipelineOpts::new()
                .timeout(std::time::Duration::from_secs(180))
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
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1"),
        )
        .await
        .expect("Pipeline execution failed");
    let logs = &output.stderr;
    assert!(
        output.status.success(),
        "the bounded pipeline should finish on its own; stderr tail:\n{}",
        &logs[logs.len().saturating_sub(4000)..]
    );

    let terminal_epoch = logged_epochs(logs, "Begin terminal checkpoint: epoch ")
        .next()
        .expect("the source must close with a terminal checkpoint");
    assert!(
        logged_epochs(logs, "Epoch finalized: ").any(|epoch| epoch < terminal_epoch),
        "a periodic epoch must finalize while the source reads (terminal epoch {terminal_epoch})"
    );
    assert!(
        logs.contains(&format!(
            "terminal epoch {terminal_epoch} finalized; progress persisted"
        )),
        "the terminal epoch must finalize and persist the source's progress"
    );

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
        "the source's progress must be persisted"
    );

    assert_eq!(
        written_ids(&ctx, "bounded_file_ckpt_out").await.len() as i64,
        FILES * FILE_ROWS,
        "every row must be written"
    );
}

/// A bounded file source resumes from its checkpointed progress.
///
/// Run 1 is stopped by SIGTERM once it has written a fraction of the rows, so
/// the stop lands mid-read whatever the machine's speed; a fixed delay lets a
/// fast host finish the whole read, leaving nothing to resume. Its partitions
/// then stop at a batch boundary
/// and close with a terminal checkpoint covering the files they fully emitted.
/// Run 2 reuses the state backend and must read only the rest, re-reading at most
/// the file each partition had in flight. Run 3 finds every file covered and
/// reads nothing.
///
/// A record limit can't stop run 1: the sink that reaches it stops consuming,
/// which drops the source's stream and aborts its terminal round-trip.
#[cfg(unix)]
#[tokio::test]
async fn test_bounded_file_source_resumes_from_checkpoint() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new())
        .await
        .expect("Failed to create test context");

    const FILES: i64 = 400;
    const PARTITIONS: usize = 2;
    const ROWS_BEFORE_SIGTERM: i64 = FILES * FILE_ROWS / 8;
    let dir = write_id_files(&ctx, "bounded_resume_data", FILES);

    // One application id and one Sqlite state file across every run, like a
    // pod restart with a persistent state volume. Only the sink table changes,
    // so each run's writes can be told apart.
    let application_id = format!("bounded_file_resume_{}", ctx.test_id);
    let state_path = std::env::temp_dir()
        .join(format!("bounded_file_resume_state_{}.sqlite", ctx.test_id))
        .to_string_lossy()
        .into_owned();
    let opts = || {
        PipelineOpts::new()
            .timeout(std::time::Duration::from_secs(180))
            .env("STREAMLING__APPLICATION_ID", &application_id)
            .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
            // A small channel keeps the drained tail short, so the terminal
            // epoch finalizes well inside the shutdown budget.
            .env("STREAMLING__INTERNAL_BUFFER_SIZE", "1")
            .env("STREAMLING__STATE_BACKEND__BACKEND_TYPE", "Sqlite")
            .env(
                "STREAMLING__STATE_BACKEND__SQLITE__DATABASE_PATH",
                &state_path,
            )
    };

    let (status, _) = ctx
        .run_pipeline_with_sigterm_when(
            &file_pipeline(&dir, "bounded_resume_run1", "bounded"),
            opts(),
            wait_for_rows(&ctx, "bounded_resume_run1", ROWS_BEFORE_SIGTERM),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("Run 1 execution failed");
    assert!(
        status.success(),
        "run 1 should drain and exit after SIGTERM"
    );

    let total = (FILES * FILE_ROWS) as usize;
    let run_1 = written_ids(&ctx, "bounded_resume_run1").await;
    assert!(
        !run_1.is_empty() && run_1.len() < total,
        "SIGTERM must land mid-read for the resume to mean anything; run 1 wrote {} of {total} rows \
         (it should stop near {ROWS_BEFORE_SIGTERM})",
        run_1.len()
    );

    let status = ctx
        .run_pipeline_with_opts(
            &file_pipeline(&dir, "bounded_resume_run2", "bounded"),
            opts(),
        )
        .await
        .expect("Run 2 execution failed");
    assert!(status.success(), "run 2 should read the rest and finish");
    let run_2 = written_ids(&ctx, "bounded_resume_run2").await;

    let missing: Vec<i64> = (1..=FILES * FILE_ROWS)
        .filter(|id| !run_1.contains(id) && !run_2.contains(id))
        .collect();
    assert!(missing.is_empty(), "the resumed run lost rows: {missing:?}");

    let reread_files: std::collections::BTreeSet<i64> = run_1
        .intersection(&run_2)
        .map(|id| (id - 1) / FILE_ROWS)
        .collect();
    assert!(
        reread_files.len() <= PARTITIONS,
        "run 2 may re-read only the files in flight when run 1 stopped (one per partition); \
         it re-read files {reread_files:?}"
    );

    let output = ctx
        .run_pipeline_raw(
            &file_pipeline(&dir, "bounded_resume_run3", "bounded"),
            opts(),
        )
        .await
        .expect("Run 3 execution failed");
    assert!(
        output.status.success(),
        "run 3 should finish without reading"
    );
    assert!(
        output
            .stderr
            .contains(&format!("0 of {FILES} listed file(s) left to read")),
        "rerunning a finished job must read nothing"
    );
}

/// A two-partition continuous file source resumes from its checkpointed
/// watermark.
///
/// Run 1 is stopped by SIGTERM once it has written a fraction of the rows AND
/// persisted checkpoint state. A continuous source closes without a terminal
/// checkpoint, so the resume only means anything if a periodic epoch finalized
/// first; waiting on both conditions stops the run at the same point on any
/// machine, where a fixed delay or a record limit stops it wherever the host's
/// speed happens to put it.
///
/// Run 2 resumes from that epoch: it must write every row run 1 didn't,
/// re-reading only the files the epoch didn't cover rather than everything run 1
/// read. It never finishes on its own, so it is stopped once it has caught up.
/// The partitions finish files out of order, so the persisted progress includes
/// files committed past one still being read.
///
/// State lives in Postgres rather than Sqlite so the test can see when progress
/// has been persisted.
#[cfg(unix)]
#[tokio::test]
async fn test_continuous_file_source_resumes_from_checkpoint() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new())
        .await
        .expect("Failed to create test context");

    // Enough files that the read outlasts the checkpoint interval on a fast host.
    const FILES: i64 = 600;
    const ROWS_BEFORE_SIGTERM: i64 = FILES * FILE_ROWS / 8;
    const EXIT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);
    let dir = write_id_files(&ctx, "continuous_resume_data", FILES);

    // One application id and one state table across both runs, like a pod
    // restart with a persistent state volume.
    let application_id = format!("continuous_file_resume_{}", ctx.test_id);
    let state_table = format!(
        "continuous_file_resume_state_{}",
        ctx.test_id.replace('-', "_")
    );
    let opts = || {
        PipelineOpts::new()
            .env("STREAMLING__APPLICATION_ID", &application_id)
            .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
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
            .timeout(std::time::Duration::from_secs(180))
    };

    let (status, run_1_logs) = ctx
        .run_pipeline_with_sigterm_when(
            &file_pipeline(&dir, "continuous_resume_run1", "continuous"),
            opts(),
            wait_for_checkpointed_rows(
                &ctx,
                "continuous_resume_run1",
                ROWS_BEFORE_SIGTERM,
                &state_table,
            ),
            EXIT_DEADLINE,
        )
        .await
        .expect("Run 1 execution failed");
    assert!(
        status.success(),
        "run 1 should drain and exit after SIGTERM"
    );
    assert!(
        logged_epochs(&run_1_logs, "Epoch finalized: ")
            .next()
            .is_some(),
        "an epoch must finalize while run 1 reads, or it persists no progress to resume from"
    );

    let total = (FILES * FILE_ROWS) as usize;
    let run_1 = written_ids(&ctx, "continuous_resume_run1").await;
    assert!(
        !run_1.is_empty() && run_1.len() < total,
        "the SIGTERM must land mid-read; run 1 wrote {} of {total} rows \
         (it should stop near {ROWS_BEFORE_SIGTERM})",
        run_1.len()
    );

    let (status, _) = ctx
        .run_pipeline_with_sigterm_when(
            &file_pipeline(&dir, "continuous_resume_run2", "continuous"),
            opts(),
            wait_until_caught_up(&ctx, "continuous_resume_run2", &run_1, FILES * FILE_ROWS),
            EXIT_DEADLINE,
        )
        .await
        .expect("Run 2 execution failed");
    assert!(
        status.success(),
        "run 2 should drain and exit after SIGTERM"
    );
    let run_2 = written_ids(&ctx, "continuous_resume_run2").await;

    let missing: Vec<i64> = (1..=FILES * FILE_ROWS)
        .filter(|id| !run_1.contains(id) && !run_2.contains(id))
        .collect();
    assert!(
        missing.is_empty(),
        "the resumed run lost rows (or did not catch up within {PROGRESS_WAIT_LIMIT:?}): {missing:?}"
    );

    let file_of = |id: &i64| (id - 1) / FILE_ROWS;
    let run_1_files: std::collections::BTreeSet<i64> = run_1.iter().map(file_of).collect();
    let reread_files: std::collections::BTreeSet<i64> =
        run_1.intersection(&run_2).map(file_of).collect();
    assert!(
        reread_files.len() < run_1_files.len(),
        "run 2 must resume from the checkpointed watermark; it re-read {} of the {} files run 1 wrote",
        reread_files.len(),
        run_1_files.len()
    );
}
