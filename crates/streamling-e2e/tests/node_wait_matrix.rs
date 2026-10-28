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
        self.ctx.prometheus.as_ref().expect("Prometheus")
    }

    async fn ms(&self, query: String) -> u64 {
        self.prometheus()
            .query_count(&format!("sum({query})"))
            .await
            .expect("Prometheus query failed")
            .unwrap_or(0)
    }

    /// Final `node_wait` totals of this run, once the last export landed.
    async fn snapshot(&self) -> Snapshot {
        // The exporter pushes every second; give the final push time to land.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let id = Some(self.ctx.test_id.as_str());
        let mut snapshot = Snapshot::default();
        for node in NODES {
            snapshot.starved.push((
                node,
                self.ms(PrometheusResource::starved_by_id_query(node, id))
                    .await,
            ));
            snapshot.busy.push((
                node,
                self.ms(PrometheusResource::busy_by_id_query(node, id))
                    .await,
            ));
        }
        for edge in NODES.windows(2) {
            snapshot.blocked.push((
                edge[0],
                self.ms(PrometheusResource::blocked_edge_query(edge[0], edge[1], id))
                    .await,
            ));
        }
        eprintln!("node_wait snapshot: {snapshot:?}");
        snapshot
    }
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
