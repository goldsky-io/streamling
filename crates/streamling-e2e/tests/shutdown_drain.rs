//! Graceful-shutdown / drain e2e tests.
//!
//! Regression tests for the shutdown investigation (shutdown-investigation.md):
//! a multi-source/multi-sink pipeline must drain every record from every
//! bounded source into every sink before terminating (job mode), and a SIGTERM
//! must produce a prompt, clean exit with no tail loss (streaming mode) —
//! instead of hanging until the k8s grace period expires and losing the last
//! buffered batches.

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

/// Test record for Kafka messages — id is String to match the ClickHouse
/// schema for hybrid source unification.
#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    block: i64,
    id: String,
    data: String,
    timestamp: i64,
}

const TEST_SCHEMA: &str = r#"{"type":"record","name":"TestMessage","fields":[
    {"name":"block","type":"long"},
    {"name":"id","type":"string"},
    {"name":"data","type":"string"},
    {"name":"timestamp","type":"long"}
]}"#;

// ============================================================================
// Job mode: multi-source → multi-sink completion barrier
// ============================================================================

/// The repro shape (oasis-consensus-pubsub-repro1, scaled down): N bounded
/// sources fanning 1:1 into N sinks, with deliberately different sizes so one
/// branch completes while the other is still producing.
///
/// Contract under test:
/// 1. The pipeline does NOT tear down when the first branch finishes — every
///    sink receives its source's complete data set.
/// 2. The first branch's drained sink is dropped from the coordinator's
///    expected-ack set (`sink_completed`), so in-flight epochs — including the
///    terminal one — still finalize on the remaining live sinks instead of
///    stalling forever (the multi-source finalization stall).
/// 3. Both completing sources share one terminal checkpoint epoch and each
///    delivers its marker inline to its own sink, so the tail of BOTH branches
///    is covered by a finalized checkpoint before exit.
/// 4. The process exits 0 well within the harness timeout (no hang).
///
/// A 1s checkpoint interval keeps real epochs in flight while the branches
/// complete at different times, which is exactly the window where the old
/// coordinator stalled.
#[tokio::test]
async fn test_job_mode_multi_source_multi_sink_drains_all_records() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");
    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    // Branch A: small bounded table (completes first).
    clickhouse
        .execute(
            "CREATE TABLE drain_source_a (
                block Int64,
                id String,
                data String,
                timestamp Int64,
                is_deleted UInt8
            ) ENGINE = MergeTree()
            ORDER BY (block, id)",
        )
        .await
        .expect("Failed to create ClickHouse table A");
    let insert_a = (1..=5)
        .map(|i| format!("({i}, 'a_{i:04}', 'branch_a', {}, 0)", 100 + i))
        .collect::<Vec<_>>()
        .join(", ");
    clickhouse
        .execute(&format!("INSERT INTO drain_source_a VALUES {insert_a}"))
        .await
        .expect("Failed to insert into table A");

    // Branch B: larger bounded table (still producing when A completes).
    clickhouse
        .execute(
            "CREATE TABLE drain_source_b (
                block Int64,
                id String,
                data String,
                timestamp Int64,
                is_deleted UInt8
            ) ENGINE = MergeTree()
            ORDER BY (block, id)",
        )
        .await
        .expect("Failed to create ClickHouse table B");
    let insert_b = (1..=200)
        .map(|i| format!("({i}, 'b_{i:04}', 'branch_b', {}, 0)", 100 + i))
        .collect::<Vec<_>>()
        .join(", ");
    clickhouse
        .execute(&format!("INSERT INTO drain_source_b VALUES {insert_b}"))
        .await
        .expect("Failed to insert into table B");

    // Each hybrid source needs an unbounded Kafka phase (never consumed in job
    // mode, but the provider is constructed at topology build time) and an
    // offset table.
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema on topic A");
    let topic_b = ctx
        .create_kafka_topic("drain_b")
        .await
        .expect("Failed to create topic B");
    topic_b
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema on topic B");

    clickhouse
        .execute(
            "CREATE TABLE kafka_offsets_drain (
                topic String,
                partition Int32,
                offset UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");

    let application_id = format!("shutdown_drain_{}", ctx.test_id);

    let pipeline = format!(
        r#"
sources:
  source_a:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: drain_source_a
        columns: block,id,data,timestamp
    unbounded_source:
      source_type: kafka
      topic: {topic_a}
      start_at: earliest
    offset_table:
      topic_name: {topic_a}
      table_name: kafka_offsets_drain
    primary_key: id
  source_b:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: drain_source_b
        columns: block,id,data,timestamp
    unbounded_source:
      source_type: kafka
      topic: {topic_b}
      start_at: earliest
    offset_table:
      topic_name: {topic_b}
      table_name: kafka_offsets_drain
    primary_key: id

transforms: {{}}

sinks:
  sink_a:
    type: postgres
    from: source_a
    table: drain_results_a
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
  sink_b:
    type: postgres
    from: source_b
    table: drain_results_b
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
"#,
        topic_a = ctx.kafka_topic,
        topic_b = topic_b.topic,
    );

    // No record limit: termination is bounded-phase completion + job mode.
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .timeout(std::time::Duration::from_secs(120))
                .env("STREAMLING__JOB_MODE", "true")
                .env("STREAMLING__APPLICATION_ID", &application_id)
                .env("STREAMLING__RECORD_BATCH_SIZE", "10")
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1"),
        )
        .await
        .expect("Pipeline execution failed (hang or crash before completion)");
    assert!(
        status.success(),
        "Job-mode multi-source pipeline should exit 0 after both branches drain"
    );

    // Every record from every source must be in its sink — the branch that
    // finished LAST is the regression: the old run loop could tear down when
    // the first branch's plugin/sink completed, cancelling the rest mid-flight.
    let count_a = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.drain_results_a")
        .await
        .expect("Failed to count sink A");
    assert_eq!(count_a, 5, "sink A must contain all of branch A's records");

    let count_b = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.drain_results_b")
        .await
        .expect("Failed to count sink B");
    assert_eq!(
        count_b, 200,
        "sink B must contain all of branch B's records"
    );
}

// ============================================================================
// Streaming mode: SIGTERM drains and exits promptly
// ============================================================================

/// The k8s-stop contract for a streaming (`job: false`) pipeline: on SIGTERM
/// the process must drain in-flight work and exit cleanly, promptly.
///
/// Contract under test:
/// 1. SIGTERM is observed (there is exactly one top-level handler) — the old
///    code could swallow it entirely in listener-recreation windows, leaving
///    the pipeline running until SIGKILL.
/// 2. The source stops, the sink drains everything the source produced, the
///    terminal checkpoint finalizes, and the process exits 0.
/// 3. Exit happens within the deadline (30s, the default k8s grace period) —
///    no lag-task rd_kafka_destroy deadlock, no wedged worker at teardown.
/// 4. No records are lost: everything consumed before the signal is in the
///    sink after exit.
#[cfg(unix)]
#[tokio::test]
async fn test_sigterm_drains_and_exits_promptly() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new())
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    const NUM_RECORDS: usize = 500;
    let records: Vec<TestRecord> = (1..=NUM_RECORDS as i64)
        .map(|i| TestRecord {
            block: i,
            id: format!("sig_{i:05}"),
            data: format!("payload_{i}"),
            timestamp: 1000 + i,
        })
        .collect();
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let application_id = format!("sigterm_drain_{}", ctx.test_id);

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
    table: sigterm_drain_results
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 50
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    // Give the pipeline time to start and consume the seeded records, then
    // SIGTERM while it idles on the live topic. The exit deadline matches the
    // default k8s grace period: exceeding it is exactly the hang-then-SIGKILL
    // failure this suite guards against.
    let status = ctx
        .run_pipeline_with_sigterm(
            &pipeline,
            PipelineOpts::new()
                .env("STREAMLING__APPLICATION_ID", &application_id)
                .env("STREAMLING__RECORD_BATCH_SIZE", "50")
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1"),
            std::time::Duration::from_secs(15),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("streamling must exit within the grace period after SIGTERM");

    assert!(
        status.success(),
        "SIGTERM shutdown must be a clean exit (code 0), got: {:?}",
        status.code()
    );

    // No tail loss: everything consumed before the signal must be durable in
    // the sink. The upsert primary key makes the count duplicate-free.
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.sigterm_drain_results")
        .await
        .expect("Failed to count sink rows");
    assert_eq!(
        count, NUM_RECORDS as i64,
        "all records consumed before SIGTERM must be drained to the sink"
    );
}
