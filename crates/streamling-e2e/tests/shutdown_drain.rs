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
///    sink after exit. Records keep being PRODUCED right up to the signal (a
///    background producer runs concurrently with the pipeline), so the signal
///    genuinely lands mid-stream — the drain path is exercised on in-flight
///    data, not only on an idle pipeline that finished long before the signal.
///    Sequential zero-padded ids let us assert a gap-free prefix
///    (`count == numeric(max(id))`): nothing consumed before the signal was
///    dropped mid-drain.
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

    // Run the pipeline and a background producer CONCURRENTLY: fresh records
    // keep arriving right up to (and past) the signal, so SIGTERM lands while
    // data is genuinely in flight. The exit deadline matches the default k8s
    // grace period: exceeding it is exactly the hang-then-SIGKILL failure this
    // suite guards against.
    const SIGNAL_AFTER_SECS: u64 = 15;
    let run = ctx.run_pipeline_with_sigterm(
        &pipeline,
        PipelineOpts::new()
            .env("STREAMLING__APPLICATION_ID", &application_id)
            .env("STREAMLING__RECORD_BATCH_SIZE", "50")
            .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1"),
        std::time::Duration::from_secs(SIGNAL_AFTER_SECS),
        std::time::Duration::from_secs(30),
    );
    let producer = async {
        // Produce small batches every 250ms until just past the signal, ids
        // continuing the seeded sequence so the whole stream stays sequential.
        let mut next_id = NUM_RECORDS as i64 + 1;
        let rounds = SIGNAL_AFTER_SECS * 4 + 4;
        for _ in 0..rounds {
            let batch: Vec<TestRecord> = (next_id..next_id + 10)
                .map(|i| TestRecord {
                    block: i,
                    id: format!("sig_{i:05}"),
                    data: format!("payload_{i}"),
                    timestamp: 1000 + i,
                })
                .collect();
            if ctx.kafka.produce_avro_records(&batch).await.is_err() {
                break;
            }
            next_id += 10;
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    };
    let (status, _) = tokio::join!(run, producer);
    let status = status.expect("streamling must exit within the grace period after SIGTERM");

    assert!(
        status.success(),
        "SIGTERM shutdown must be a clean exit (code 0), got: {:?}",
        status.code()
    );

    // No tail loss and no mid-stream gaps: ids are sequential and zero-padded,
    // consumption is in order (single partition), and the upsert primary key
    // makes the count duplicate-free — so everything the pipeline consumed up
    // to the signal forms a contiguous prefix, and `count == numeric(max(id))`
    // proves no consumed record was dropped during the drain.
    let rows: Vec<(i64, Option<String>)> = ctx
        .postgres
        .query("SELECT COUNT(*), MAX(id) FROM public.sigterm_drain_results")
        .await
        .expect("Failed to query sink rows");
    let (count, max_id) = (rows[0].0, rows[0].1.clone().unwrap_or_default());
    assert!(
        count >= NUM_RECORDS as i64,
        "all pre-seeded records must be drained to the sink (got {count})"
    );
    let max_numeric: i64 = max_id
        .strip_prefix("sig_")
        .and_then(|n| n.parse().ok())
        .expect("max id should be a sig_NNNNN key");
    assert_eq!(
        count, max_numeric,
        "sink rows must form a gap-free prefix of the produced stream \
         (count {count} vs max id {max_numeric}): a gap means a record consumed \
         before SIGTERM was dropped mid-drain"
    );
}

// ============================================================================
// Streaming mode: SIGTERM during a BOUNDED phase drains and exits promptly
// ============================================================================

/// SIGTERM while a hybrid source is still in its bounded (ClickHouse) phase:
/// the scan must end early, the terminal checkpoint must cover what was
/// emitted, and the process must exit 0 well inside the k8s grace period.
///
/// Regression: the shutdown signal used to be observed only by the Kafka
/// unbounded phase (via the hybrid shutdown watcher) and by the post-stream
/// check in the hybrid forwarding loop — a bounded scan sat in
/// `stream.next()` until the whole table had been read, blew the shutdown
/// budget, and the watchdog force-exited without a terminal drain. The table
/// here is large enough that the scan is still running when the signal
/// lands; if a fast machine finishes it first the test degrades to the
/// (already covered) streaming-phase path rather than flaking.
#[cfg(unix)]
#[tokio::test]
async fn test_sigterm_during_bounded_phase_drains_and_exits() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");
    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE bounded_sigterm_source (
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
    // Server-side seed: large enough that the bounded scan (throttled by the
    // downstream Postgres sink) is still in flight when the signal fires.
    clickhouse
        .execute(
            "INSERT INTO bounded_sigterm_source
             SELECT number, concat('bp_', toString(number)), 'bounded_payload',
                    1000 + number, 0
             FROM numbers(500000)",
        )
        .await
        .expect("Failed to seed ClickHouse table");

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");
    clickhouse
        .execute(
            "CREATE TABLE kafka_offsets_bounded_sigterm (
                topic String,
                partition Int32,
                offset UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");

    let application_id = format!("bounded_sigterm_{}", ctx.test_id);

    let pipeline = format!(
        r#"
sources:
  source_a:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: bounded_sigterm_source
        columns: block,id,data,timestamp
    unbounded_source:
      source_type: kafka
      topic: {topic}
      start_at: earliest
    offset_table:
      topic_name: {topic}
      table_name: kafka_offsets_bounded_sigterm
    primary_key: id

transforms: {{}}

sinks:
  pg_sink:
    type: postgres
    from: source_a
    table: bounded_sigterm_results
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 200
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_sigterm(
            &pipeline,
            PipelineOpts::new()
                .env("STREAMLING__APPLICATION_ID", &application_id)
                .env("STREAMLING__RECORD_BATCH_SIZE", "100")
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1"),
            std::time::Duration::from_secs(6),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("streamling must exit within the grace period after SIGTERM mid-bounded-scan");
    assert!(
        status.success(),
        "SIGTERM during a bounded phase must be a clean exit (code 0), got: {:?}",
        status.code()
    );

    // The scan ran for several seconds before the signal — some prefix of the
    // table must have been drained into the sink. Completeness is NOT
    // expected: ending the scan early is the point of the test.
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.bounded_sigterm_results")
        .await
        .expect("Failed to count sink rows");
    assert!(
        count > 0,
        "sink must contain the drained prefix of the bounded scan"
    );
}

// ============================================================================
// Job mode: plugin sink terminal ack must finalize the terminal epoch
// ============================================================================

/// Build the in-repo example plugin (`plugin_examples/basic`, registers the
/// `print_sink` sink plugin) as a cdylib and return the shared-library path.
/// It lives in its own cargo workspace, so this is a separate (cached) build.
async fn build_basic_example_plugin() -> std::path::PathBuf {
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/streamling-e2e must sit two levels below the repo root")
        .to_path_buf();
    let plugin_dir = repo_root.join("plugin_examples/basic");

    let status = tokio::process::Command::new("cargo")
        .args(["build", "--lib"])
        .current_dir(&plugin_dir)
        .status()
        .await
        .expect("failed to invoke cargo build for plugin_examples/basic");
    assert!(status.success(), "building plugin_examples/basic failed");

    let debug_dir = plugin_dir.join("target/debug");
    [
        "libplugin_example_basic.so",
        "libplugin_example_basic.dylib",
    ]
    .iter()
    .map(|name| debug_dir.join(name))
    .find(|p| p.exists())
    .expect("built plugin cdylib not found in plugin_examples/basic/target/debug")
}

/// Job-mode pipelines with FFI plugin sinks must terminate: the terminal
/// checkpoint marker rides the LAST batch of the stream, and the plugin's
/// `CheckpointAck` reaches the host only after the plugin has processed the
/// marker — strictly after the sink's batch loop has parked on the exhausted
/// stream. The ack must still be forwarded to the checkpoint coordinator so
/// the terminal epoch finalizes and the process exits.
///
/// Regression: ack propagation used to happen only from inside the sink's
/// batch loop (at most one ack per incoming batch), so the terminal ack was
/// never drained — the coordinator retried "Checkpoint epoch 1 timed out"
/// forever and the job hung until an external kill. Streaming pipelines
/// masked this because continuous batches kept draining the channel.
#[tokio::test]
async fn test_job_mode_plugin_sink_terminal_ack_exits() {
    init_tracing();

    let plugin_lib = build_basic_example_plugin().await;

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");
    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    // Small bounded table: the whole job completes within a few batches, so
    // the terminal marker rides the last (often only) batch the sink sees.
    clickhouse
        .execute(
            "CREATE TABLE plugin_drain_source (
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
    let inserts = (1..=20)
        .map(|i| format!("({i}, 'p_{i:04}', 'plugin_branch', {}, 0)", 100 + i))
        .collect::<Vec<_>>()
        .join(", ");
    clickhouse
        .execute(&format!("INSERT INTO plugin_drain_source VALUES {inserts}"))
        .await
        .expect("Failed to insert into ClickHouse table");

    // Hybrid source scaffolding: the unbounded Kafka phase is never consumed
    // in job mode but the provider is constructed at topology build time.
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");
    clickhouse
        .execute(
            "CREATE TABLE kafka_offsets_plugin_drain (
                topic String,
                partition Int32,
                offset UInt32
            ) ENGINE = MergeTree()
            ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");

    let application_id = format!("plugin_drain_{}", ctx.test_id);

    // `print_sink` is not a built-in sink type, so the config resolves it as
    // a plugin sink and binds it to the plugin registered under that id.
    let pipeline = format!(
        r#"
sources:
  source_a:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: plugin_drain_source
        columns: block,id,data,timestamp
    unbounded_source:
      source_type: kafka
      topic: {topic}
      start_at: earliest
    offset_table:
      topic_name: {topic}
      table_name: kafka_offsets_plugin_drain
    primary_key: id

transforms: {{}}

sinks:
  sink_a:
    type: print_sink
    from: source_a
"#,
        topic = ctx.kafka_topic,
    );

    // No record limit: termination is bounded-phase completion + job mode.
    // A regression wedges the process on terminal-epoch finalization, so the
    // harness timeout is the failure detector — keep it well below the suite
    // timeout so a hang fails fast instead of stalling CI.
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .timeout(std::time::Duration::from_secs(120))
                .env("STREAMLING__JOB_MODE", "true")
                .env("STREAMLING__APPLICATION_ID", &application_id)
                .env("STREAMLING__RECORD_BATCH_SIZE", "10")
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
                .env(
                    "STREAMLING__PLUGIN__PATH",
                    plugin_lib.to_string_lossy().as_ref(),
                ),
        )
        .await
        .expect("Pipeline execution failed (hang on terminal ack, or crash)");
    assert!(
        status.success(),
        "job-mode pipeline with a plugin sink must exit 0 after the terminal \
         checkpoint finalizes"
    );
}
