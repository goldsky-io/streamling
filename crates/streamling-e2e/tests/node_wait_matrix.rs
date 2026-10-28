//! Node-wait scenario matrix.
//!
//! One four-node pipeline, `m_src` (Kafka) → `m_sql` (SQL) → `m_handler`
//! (external handler) → `m_sink` (webhook), run once per scenario with one
//! deliberate bottleneck. Each test asserts that `node_wait` names the
//! bottleneck: it has the largest `busy`, the edge into it is `blocked`, and
//! the nodes after it are `starved`. Queries are pinned to the run's
//! `instance`, so other runs cannot satisfy them.

use serde::Serialize;
use std::time::{Duration, Instant};
use streamling_e2e::resources::{ExternalHandlerResource, PrometheusResource, WebhookResource};
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

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

const NODES: [&str; 4] = ["m_src", "m_sql", "m_handler", "m_sink"];

fn records(range: std::ops::RangeInclusive<i64>) -> Vec<TestRecord> {
    range
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{i}"),
            timestamp: 1000 + i,
        })
        .collect()
}

struct Matrix {
    ctx: TestContext,
    handler: ExternalHandlerResource,
    webhook: WebhookResource,
}

impl Matrix {
    async fn new() -> Self {
        init_tracing();
        let ctx = TestContext::with_options(TestContextOptions::new().with_prometheus())
            .await
            .expect("Failed to create test context");
        assert!(ctx.prometheus.is_some(), "Prometheus must be configured");
        ctx.kafka
            .register_schema(TEST_SCHEMA)
            .await
            .expect("Failed to register schema");
        let handler = ExternalHandlerResource::new()
            .await
            .expect("Failed to start handler server");
        let webhook = WebhookResource::new()
            .await
            .expect("Failed to start webhook server");
        Self {
            ctx,
            handler,
            webhook,
        }
    }

    /// `digest` is the SQL expression for an extra column; it sets how much
    /// work `m_sql` does per row.
    fn pipeline(&self, digest: &str) -> String {
        format!(
            r#"
sources:
  m_src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  m_sql:
    type: sql
    sql: "SELECT id, value, timestamp, {digest} AS digest FROM m_src"
    primary_key: id
  m_handler:
    type: handler
    from: m_sql
    url: {handler_url}
    one_row_per_request: false
    payload_version: 0
    primary_key: id

sinks:
  m_sink:
    type: webhook
    from: m_handler
    url: {webhook_url}
    one_row_per_request: false
    payload_version: 0
"#,
            topic = self.ctx.kafka_topic,
            handler_url = self.handler.passthrough_handler_url(),
            webhook_url = self.webhook.webhook_url(),
        )
    }

    fn opts(&self, record_limit: u64, batch_size: usize) -> PipelineOpts {
        PipelineOpts::new()
            .record_limit(record_limit)
            .timeout(Duration::from_secs(180))
            .env("STREAMLING__RECORD_BATCH_SIZE", batch_size.to_string())
            .env("STREAMLING__INTERNAL_BUFFER_SIZE", "1")
            .env("STREAMLING__EXTERNAL_HTTP_HANDLER__BUFFER_SIZE", "1")
    }

    async fn run(&self, digest: &str, record_limit: u64, batch_size: usize) {
        let status = self
            .ctx
            .run_pipeline_with_opts(&self.pipeline(digest), self.opts(record_limit, batch_size))
            .await
            .expect("Streamling execution failed");
        assert!(status.success(), "Streamling should exit successfully");
    }

    fn prometheus(&self) -> &PrometheusResource {
        prometheus(&self.ctx)
    }

    async fn snapshot(&self) -> Snapshot {
        snapshot(&self.ctx, &NODES).await
    }
}

fn prometheus(ctx: &TestContext) -> &PrometheusResource {
    ctx.prometheus
        .as_ref()
        .expect("Prometheus must be configured")
}

async fn ms(ctx: &TestContext, query: String) -> u64 {
    prometheus(ctx)
        .query_count(&format!("sum({query})"))
        .await
        .expect("Prometheus query failed")
        .unwrap_or(0)
}

/// Final `node_wait` totals of this run for a linear chain of `nodes`, once
/// the last export landed.
async fn snapshot(ctx: &TestContext, nodes: &[&'static str]) -> Snapshot {
    // The exporter pushes every second; give the final push time to land.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let id = Some(ctx.test_id.as_str());
    let mut snapshot = Snapshot::default();
    for &node in nodes {
        snapshot.starved.push((
            node,
            ms(ctx, PrometheusResource::starved_by_id_query(node, id)).await,
        ));
        snapshot.busy.push((
            node,
            ms(ctx, PrometheusResource::busy_by_id_query(node, id)).await,
        ));
    }
    for edge in nodes.windows(2) {
        snapshot.blocked.push((
            edge[0],
            ms(
                ctx,
                PrometheusResource::blocked_edge_query(edge[0], edge[1], id),
            )
            .await,
        ));
    }
    eprintln!("node_wait snapshot: {snapshot:?}");
    snapshot
}

/// Milliseconds per node (`starved`, `busy`) and per edge (`blocked`, keyed by
/// the producer; the consumer is the next node).
#[derive(Debug, Default)]
struct Snapshot {
    starved: Vec<(&'static str, u64)>,
    busy: Vec<(&'static str, u64)>,
    blocked: Vec<(&'static str, u64)>,
}

impl Snapshot {
    fn get(series: &[(&'static str, u64)], node: &str) -> u64 {
        series
            .iter()
            .find(|(n, _)| *n == node)
            .map_or(0, |(_, ms)| *ms)
    }

    fn starved(&self, node: &str) -> u64 {
        Self::get(&self.starved, node)
    }

    fn busy(&self, node: &str) -> u64 {
        Self::get(&self.busy, node)
    }

    fn blocked(&self, producer: &str) -> u64 {
        Self::get(&self.blocked, producer)
    }

    fn assert_busiest(&self, node: &str) {
        let top = self.busy(node);
        for (other, ms) in &self.busy {
            if *other != node {
                assert!(
                    top > *ms,
                    "{node} must be the busiest node ({top}ms), but {other} has {ms}ms: {self:?}"
                );
            }
        }
    }
}

/// A slow sink: its write time is `busy`, and the handler feeding it is
/// `blocked` on it.
#[tokio::test]
async fn slow_sink_is_busy_and_blocks_its_feeder() {
    let m = Matrix::new().await;
    m.webhook.set_delay(Duration::from_millis(75));
    m.ctx
        .kafka
        .produce_avro_records(&records(1..=40))
        .await
        .expect("Failed to produce records");

    m.run("value", 40, 1).await;
    let s = m.snapshot().await;

    s.assert_busiest("m_sink");
    assert!(s.busy("m_sink") >= 2_000, "{s:?}");
    assert!(s.busy("m_sink") > s.starved("m_sink"), "{s:?}");
    assert!(s.blocked("m_handler") >= 1_000, "{s:?}");
}

/// A slow handler: its request time is `busy`, the SQL node feeding it is
/// `blocked` on it, and the sink after it is `starved`.
#[tokio::test]
async fn slow_handler_is_busy_blocks_upstream_and_starves_downstream() {
    let m = Matrix::new().await;
    m.handler.set_delay(Duration::from_millis(75));
    m.ctx
        .kafka
        .produce_avro_records(&records(1..=40))
        .await
        .expect("Failed to produce records");

    m.run("value", 40, 1).await;
    let s = m.snapshot().await;

    s.assert_busiest("m_handler");
    assert!(s.busy("m_handler") >= 2_000, "{s:?}");
    assert!(s.blocked("m_sql") >= 1_000, "{s:?}");
    assert!(s.starved("m_sink") >= 1_000, "{s:?}");
    assert!(s.starved("m_sink") > s.busy("m_sink"), "{s:?}");
}

/// A CPU-heavy SQL transform: its DataFusion compute is `busy`, not `starved`.
#[tokio::test]
async fn heavy_sql_compute_is_busy_not_starved() {
    let m = Matrix::new().await;
    m.ctx
        .kafka
        .produce_avro_records(&records(1..=2_000))
        .await
        .expect("Failed to produce records");

    m.run("encode(sha256(repeat(value, 60000)), 'hex')", 2_000, 100)
        .await;
    let s = m.snapshot().await;

    s.assert_busiest("m_sql");
    assert!(s.busy("m_sql") >= 1_000, "{s:?}");
    // `m_sql` waits only while `m_src` has nothing for it. Compute leaking
    // into `starved` would push it a whole `busy` past the source's time.
    assert!(
        s.starved("m_sql") < s.starved("m_src") + s.busy("m_src") + s.busy("m_sql") / 2,
        "{s:?}"
    );
    // The source is held back by the SQL compute.
    assert!(s.blocked("m_src") >= s.busy("m_sql") / 2, "{s:?}");
}

/// A source with no data most of the time: every node waits on it, so every
/// node is mostly `starved`, and nothing is held back.
#[tokio::test]
async fn idle_source_starves_every_node() {
    let m = Matrix::new().await;
    let produce_slowly = async {
        for i in 1..=10 {
            m.ctx
                .kafka
                .produce_avro_records(&records(i..=i))
                .await
                .expect("Failed to produce record");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    tokio::join!(m.run("value", 10, 1), produce_slowly);
    let s = m.snapshot().await;

    assert!(s.starved("m_src") >= 2_000, "{s:?}");
    for node in NODES {
        assert!(
            s.starved(node) > s.busy(node),
            "{node} must be mostly starved: {s:?}"
        );
    }
    assert!(s.starved("m_sink") >= 2_000, "{s:?}");
    for producer in &NODES[..3] {
        assert!(
            s.blocked(producer) < s.starved("m_src"),
            "{producer} must not be held back: {s:?}"
        );
    }
}

/// A sink that hangs: its open write shows as `busy` while it is still
/// hanging, not only after it finishes.
#[tokio::test]
async fn a_hanging_sink_is_reported_while_it_hangs() {
    let m = Matrix::new().await;
    m.webhook.set_delay(Duration::from_secs(6));
    m.ctx
        .kafka
        .produce_avro_records(&records(1..=3))
        .await
        .expect("Failed to produce records");

    let busy_query = format!(
        "sum({})",
        PrometheusResource::busy_by_id_query("m_sink", Some(&m.ctx.test_id))
    );
    let started = Instant::now();
    let run = async {
        m.run("value", 3, 1).await;
        started.elapsed()
    };
    let probe = async {
        let busy = m
            .prometheus()
            .wait_for_metric_at_least(&busy_query, 1_000, 60, 250)
            .await
            .expect("the hanging write must show as busy");
        (busy, started.elapsed())
    };
    let (run_done, (busy, seen_at)) = tokio::join!(run, probe);

    // 3 writes x 6s: the probe must see busy while writes are still hanging.
    assert!(busy >= 1_000, "busy {busy}ms");
    assert!(
        seen_at + Duration::from_secs(3) < run_done,
        "busy must be visible during the hang: seen at {seen_at:?}, run ended at {run_done:?}"
    );
}

// ============================================================================
// Hybrid source: ClickHouse (bounded) then Kafka (unbounded)
// ============================================================================

#[derive(Debug, Clone, Serialize)]
struct HybridRecord {
    block: i64,
    id: String,
    data: String,
    timestamp: i64,
}

const HYBRID_SCHEMA: &str = r#"{"type":"record","name":"HybridRecord","fields":[
    {"name":"block","type":"long"},
    {"name":"id","type":"string"},
    {"name":"data","type":"string"},
    {"name":"timestamp","type":"long"}
]}"#;

const HYBRID_NODES: [&str; 2] = ["h_src", "h_sink"];

fn hybrid_records(range: std::ops::RangeInclusive<i64>) -> Vec<HybridRecord> {
    range
        .map(|i| HybridRecord {
            block: 1_000 + i,
            id: format!("kafka_{i}"),
            data: format!("data_{i}"),
            timestamp: 2_000 + i,
        })
        .collect()
}

/// A hybrid source with `bounded_rows` rows in ClickHouse, then Kafka.
async fn hybrid_context(bounded_rows: i64) -> (TestContext, WebhookResource) {
    init_tracing();
    let ctx = TestContext::with_options(
        TestContextOptions::new()
            .with_prometheus()
            .with_clickhouse(),
    )
    .await
    .expect("Failed to create test context");
    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");
    clickhouse
        .execute(
            "CREATE TABLE nw_hybrid (block Int64, id String, data String, timestamp Int64, \
             is_deleted UInt8) ENGINE = MergeTree() ORDER BY (block, id)",
        )
        .await
        .expect("Failed to create ClickHouse table");
    let rows: Vec<String> = (1..=bounded_rows)
        .map(|i| format!("({i}, 'ch_{i}', 'ch_data_{i}', {i}, 0)"))
        .collect();
    clickhouse
        .execute(&format!("INSERT INTO nw_hybrid VALUES {}", rows.join(", ")))
        .await
        .expect("Failed to insert ClickHouse rows");
    clickhouse
        .execute(
            "CREATE TABLE nw_hybrid_offsets (topic String, partition Int32, offset UInt32) \
             ENGINE = MergeTree() ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");
    ctx.kafka
        .register_schema(HYBRID_SCHEMA)
        .await
        .expect("Failed to register schema");
    let webhook = WebhookResource::new()
        .await
        .expect("Failed to start webhook server");
    (ctx, webhook)
}

fn hybrid_pipeline(ctx: &TestContext, webhook: &WebhookResource) -> String {
    format!(
        r#"
sources:
  h_src:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: nw_hybrid
        columns: block,id,data,timestamp
    unbounded_source:
      source_type: kafka
      topic: {topic}
      start_at: earliest
    offset_table:
      topic_name: {topic}
      table_name: nw_hybrid_offsets
    primary_key: id

transforms: {{}}

sinks:
  h_sink:
    type: webhook
    from: h_src
    url: {webhook_url}
    one_row_per_request: false
    payload_version: 0
"#,
        topic = ctx.kafka_topic,
        webhook_url = webhook.webhook_url(),
    )
}

async fn run_hybrid(ctx: &TestContext, webhook: &WebhookResource, record_limit: u64) {
    let status = ctx
        .run_pipeline_with_opts(
            &hybrid_pipeline(ctx, webhook),
            PipelineOpts::new()
                .record_limit(record_limit)
                .timeout(Duration::from_secs(180))
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__INTERNAL_BUFFER_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "Streamling should exit successfully");
}

/// Every `node_wait` series a hybrid's phases (`h_src_bounded_N`,
/// `h_src_unbounded`) emitted in this run.
async fn hybrid_phase_node_wait(ctx: &TestContext) -> u64 {
    ms(
        ctx,
        format!(
            "streamling_node_wait_milliseconds_total{{id=~\"h_src_.+\",instance=\"{}\"}}",
            ctx.test_id
        ),
    )
    .await
}

/// A hybrid source feeding a slow sink: the triad is under the hybrid's YAML
/// name (its phases emit none), the sink is busy, and the hybrid is blocked
/// on it.
#[tokio::test]
async fn hybrid_source_reports_under_its_name_and_is_blocked_by_a_slow_sink() {
    let (ctx, webhook) = hybrid_context(20).await;
    webhook.set_delay(Duration::from_millis(100));
    ctx.kafka
        .produce_avro_records(&hybrid_records(1..=20))
        .await
        .expect("Failed to produce records");

    run_hybrid(&ctx, &webhook, 40).await;
    let s = snapshot(&ctx, &HYBRID_NODES).await;

    s.assert_busiest("h_sink");
    assert!(s.busy("h_sink") >= 1_000, "{s:?}");
    assert!(s.blocked("h_src") >= 500, "{s:?}");
    assert!(
        s.starved("h_src") + s.busy("h_src") > 0,
        "the hybrid's own meter must be claimed by its phases: {s:?}"
    );
    assert_eq!(hybrid_phase_node_wait(&ctx).await, 0, "{s:?}");
}

/// A hybrid source whose unbounded phase mostly waits for Kafka: it is
/// `starved`, and so is the sink after it.
#[tokio::test]
async fn hybrid_source_waiting_on_kafka_is_starved() {
    let (ctx, webhook) = hybrid_context(3).await;
    let produce_slowly = async {
        for i in 1..=8 {
            ctx.kafka
                .produce_avro_records(&hybrid_records(i..=i))
                .await
                .expect("Failed to produce record");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    tokio::join!(run_hybrid(&ctx, &webhook, 11), produce_slowly);
    let s = snapshot(&ctx, &HYBRID_NODES).await;

    assert!(s.starved("h_src") >= 2_000, "{s:?}");
    assert!(s.starved("h_src") > s.busy("h_src"), "{s:?}");
    assert!(s.starved("h_sink") > s.busy("h_sink"), "{s:?}");
    assert_eq!(hybrid_phase_node_wait(&ctx).await, 0, "{s:?}");
}

// ============================================================================
// File source
// ============================================================================

const FILE_NODES: [&str; 2] = ["f_src", "f_sink"];

/// Write one single-row CSV file, renamed into place so a poll never sees it
/// half-written.
fn write_csv(dir: &std::path::Path, id: i64) {
    let staged = dir.join(format!("part_{id:04}.csv.tmp"));
    std::fs::write(&staged, format!("id,name\n{id},name_{id}\n")).expect("write csv");
    std::fs::rename(&staged, dir.join(format!("part_{id:04}.csv"))).expect("rename csv");
}

async fn file_context() -> (TestContext, WebhookResource, std::path::PathBuf) {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_prometheus())
        .await
        .expect("Failed to create test context");
    let dir = ctx.temp_dir.path().join("node_wait_files");
    std::fs::create_dir_all(&dir).expect("create data dir");
    let webhook = WebhookResource::new()
        .await
        .expect("Failed to start webhook server");
    (ctx, webhook, dir)
}

fn file_pipeline(dir: &std::path::Path, mode: &str, webhook: &WebhookResource) -> String {
    format!(
        r#"
sources:
  f_src:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    parallelism: 1
    mode:
{mode}

transforms: {{}}

sinks:
  f_sink:
    type: webhook
    from: f_src
    url: {webhook_url}
    one_row_per_request: false
    payload_version: 0
"#,
        path = dir.display(),
        webhook_url = webhook.webhook_url(),
    )
}

/// A bounded file source feeding a slow sink: the sink is busy and the file
/// source is blocked on it. One partition, so batches reach the sink one after
/// another and fill its write queue.
#[tokio::test]
async fn file_source_is_blocked_by_a_slow_sink() {
    let (ctx, webhook, dir) = file_context().await;
    webhook.set_delay(Duration::from_millis(100));
    for id in 1..=30 {
        write_csv(&dir, id);
    }

    let status = ctx
        .run_pipeline_with_opts(
            &file_pipeline(&dir, "      type: bounded", &webhook),
            PipelineOpts::new()
                .timeout(Duration::from_secs(180))
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__INTERNAL_BUFFER_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "Streamling should exit successfully");
    let s = snapshot(&ctx, &FILE_NODES).await;

    s.assert_busiest("f_sink");
    assert!(s.busy("f_sink") >= 2_000, "{s:?}");
    assert!(s.blocked("f_src") >= 500, "{s:?}");
}

/// A continuous file source between new files: its idle polls are `starved`,
/// and so is the sink after it.
#[tokio::test]
async fn continuous_file_source_idle_polls_are_starved() {
    let (ctx, webhook, dir) = file_context().await;
    write_csv(&dir, 1);

    // Start-up takes seconds; add files only once the pipeline is running, so
    // the polls between them are really idle.
    let add_files_slowly = async {
        assert!(
            webhook.wait_for_requests(1, Duration::from_secs(60)).await,
            "the first file must reach the sink"
        );
        for id in 2..=5 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            write_csv(&dir, id);
        }
    };
    let run = async {
        let status = ctx
            .run_pipeline_with_opts(
                &file_pipeline(
                    &dir,
                    "      type: continuous\n      poll_interval: 1s",
                    &webhook,
                ),
                PipelineOpts::new()
                    .record_limit(5)
                    .timeout(Duration::from_secs(180)),
            )
            .await
            .expect("Streamling execution failed");
        assert!(status.success(), "Streamling should exit successfully");
    };
    tokio::join!(run, add_files_slowly);
    let s = snapshot(&ctx, &FILE_NODES).await;

    assert!(s.starved("f_src") >= 2_000, "{s:?}");
    assert!(s.starved("f_src") > s.busy("f_src"), "{s:?}");
    assert!(s.starved("f_sink") > s.busy("f_sink"), "{s:?}");
}
