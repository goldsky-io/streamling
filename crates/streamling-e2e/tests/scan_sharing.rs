//! Scan-sharing e2e tests.
//!
//! When a single source has more than one distinct consumer (here: two SQL
//! transforms), streamling auto-enables scan sharing: the source is scanned once
//! and a `BroadcastStream` fans the rows out to each consumer. This is a
//! different code path from multi-sink fan-out (a single multi-sink group counts
//! as one consumer).
//!
//! These tests prove the node-wait `blocked` edge-metric works on the
//! scan-sharing path: a slow consumer's blocked-send time is attributed to it
//! via the `downstream_id` label on
//! `streamling_node_wait_milliseconds_total{state="blocked"}`, while a fast
//! consumer sharing the same source is not charged. The shared
//! producer's per-consumer edges are attributed to the *immediate* consumer —
//! the transform that reads the shared source (`downstream_id="slow_branch"` /
//! `"fast_branch"`) — not the terminal sink behind it.

use serde::Serialize;
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

/// One shared Kafka source feeds two transforms (so scan sharing turns on): a
/// fast branch (Postgres) and a deliberately slow branch (a webhook that blocks
/// 100ms per request). The scan-sharing `BroadcastStream` must:
///   1. accrue blocked time on the shared producer's fan-out edge to the slow
///      branch (`node_wait{state="blocked", id="scanshare_source",
///      downstream_id="slow_branch"}`), attributed to the immediate consumer
///      transform (not the webhook sink behind it), and
///   2. charge that edge materially more than the fast branch's edge
///      (`downstream_id="fast_branch"`) — the per-consumer isolation guarantee of
///      the BroadcastStream.
///
/// `STREAMLING__INTERNAL_BUFFER_SIZE=1` shrinks every internal channel (including
/// the broadcast's per-consumer channels) to one batch so the slow webhook gates
/// the shared producer promptly instead of the two-stage transform→sink pipeline
/// absorbing the whole run before backpressure can register.
#[tokio::test]
async fn test_scan_sharing_backpressure_attribution() {
    init_tracing();

    use streamling_e2e::resources::{PrometheusResource, WebhookResource};

    let ctx = match TestContext::with_options(TestContextOptions::new().with_prometheus()).await {
        Ok(ctx) => ctx,
        Err(e) => {
            eprintln!("Skipping test - could not create context: {}", e);
            return;
        }
    };

    let prometheus = match &ctx.prometheus {
        Some(p) => p,
        None => {
            eprintln!("Skipping test - Prometheus not configured");
            return;
        }
    };

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Slow branch: each webhook request blocks 100ms, so the slow_branch consumer
    // drains the broadcast slowly, its channel fills, and the shared producer
    // blocks on it — accruing attributed blocked-send time while the fast
    // Postgres branch drains freely.
    let webhook = WebhookResource::new()
        .await
        .expect("Failed to start webhook server");
    webhook.set_delay(std::time::Duration::from_millis(100));

    let records_to_produce: i64 = 30;
    let records: Vec<TestRecord> = (1..=records_to_produce)
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

    // Two transforms reading the same source => scan sharing on `scanshare_source`.
    // Unique node names keep the per-id/per-downstream metric series isolated from
    // other tests.
    let pipeline = format!(
        r#"
sources:
  scanshare_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  fast_branch:
    type: sql
    sql: "SELECT id, value, timestamp FROM scanshare_source"
    primary_key: id
  slow_branch:
    type: sql
    sql: "SELECT id, value, timestamp FROM scanshare_source"
    primary_key: id

sinks:
  pg_fast:
    type: postgres
    from: fast_branch
    table: scanshare_fast
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1

  webhook_slow:
    type: webhook
    from: slow_branch
    url: {webhook_url}
    one_row_per_request: true
    payload_version: 0
    batch_size: 1
"#,
        topic = ctx.kafka_topic,
        webhook_url = webhook.webhook_url(),
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .timeout(std::time::Duration::from_secs(120))
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__INTERNAL_BUFFER_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // The fast Postgres branch drains every record.
    let pg_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.scanshare_fast")
        .await
        .expect("Failed to query PostgreSQL count");
    assert_eq!(
        pg_count, records_to_produce,
        "Postgres (fast branch) should receive all rows"
    );

    // The slow webhook branch is expected to lag (that lag is the backpressure
    // under test); just sanity-check it received traffic.
    assert!(
        webhook
            .wait_for_requests(1, std::time::Duration::from_secs(10))
            .await,
        "slow webhook branch should have received at least one request, got {}",
        webhook.request_count()
    );

    // Give metrics time to flush to Prometheus.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // The shared producer's fan-out edge to the slow branch must accrue blocked
    // send time, attributed via `downstream_id="slow_branch"` (the immediate
    // consumer transform — this is the scan-sharing BroadcastStream attribution
    // under test).
    let slow_edge_query = format!(
        "sum({})",
        PrometheusResource::backpressure_by_downstream_query("slow_branch", None)
    );
    let slow_edge = prometheus
        .wait_for_metric_at_least(&slow_edge_query, 50, 30, 500)
        .await
        .expect("slow scan-sharing consumer must accrue attributed blocked-send time");
    assert!(
        slow_edge >= 50,
        "expected substantial backpressure attributed to slow_branch, got {slow_edge}ms"
    );

    // The fast branch shares the same source but must be charged far less — the
    // BroadcastStream isolates per-consumer blocked time, so a single slow
    // consumer cannot smear backpressure onto its fast sibling.
    let fast_edge_query = format!(
        "sum({})",
        PrometheusResource::backpressure_by_downstream_query("fast_branch", None)
    );
    let fast_edge = prometheus
        .query_count(&fast_edge_query)
        .await
        .expect("query failed")
        .unwrap_or(0);
    assert!(
        fast_edge < slow_edge,
        "fast_branch ({fast_edge}ms) should be charged less backpressure than slow_branch ({slow_edge}ms)"
    );
}

/// Guards **bug 1's wiring**: the edge INTO a scan-sharing *producer* must carry a
/// `downstream_id`, not emit as an untagged series.
///
/// Here the scan-shared node is a **transform over a source**
/// (`up_source -> shared_producer(scan-shared) -> {sp_fast, sp_slow}`), not a raw
/// source leaf as in the test above. Because `shared_producer` has two consumers,
/// scan sharing stashes its whole sub-plan
/// (`W(shared_producer) -> ... -> W(up_source)`) inside a `SharedSourceHandle`
/// before `DownstreamAttributionRule` runs, so the main pass can't reach
/// `W(up_source)`. Without the construction-time attribution fix, `W(up_source)`
/// stays `Unattributed` and the `up_source -> shared_producer` edge emits with
/// **no** `downstream_id` — i.e. `downstream_id="shared_producer"` would be absent
/// (query = 0). The slow webhook branch pushes backpressure all the way up to the
/// source, so with the fix the upstream edge is tagged and non-zero.
#[tokio::test]
async fn test_scan_sharing_upstream_edge_attribution() {
    init_tracing();

    use streamling_e2e::resources::{PrometheusResource, WebhookResource};

    let ctx = match TestContext::with_options(TestContextOptions::new().with_prometheus()).await {
        Ok(ctx) => ctx,
        Err(e) => {
            eprintln!("Skipping test - could not create context: {}", e);
            return;
        }
    };

    let prometheus = match &ctx.prometheus {
        Some(p) => p,
        None => {
            eprintln!("Skipping test - Prometheus not configured");
            return;
        }
    };

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let webhook = WebhookResource::new()
        .await
        .expect("Failed to start webhook server");
    webhook.set_delay(std::time::Duration::from_millis(100));

    let records_to_produce: i64 = 30;
    let records: Vec<TestRecord> = (1..=records_to_produce)
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

    // `shared_producer` is a transform read by two downstream transforms, so scan
    // sharing turns on for it (not for `up_source`, which has a single consumer).
    // Unique node names keep this test's metric series isolated from the sibling.
    let pipeline = format!(
        r#"
sources:
  up_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  shared_producer:
    type: sql
    sql: "SELECT id, value, timestamp FROM up_source"
    primary_key: id
  sp_fast:
    type: sql
    sql: "SELECT id, value, timestamp FROM shared_producer"
    primary_key: id
  sp_slow:
    type: sql
    sql: "SELECT id, value, timestamp FROM shared_producer"
    primary_key: id

sinks:
  sp_pg_fast:
    type: postgres
    from: sp_fast
    table: scanshare_upstream_fast
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1

  sp_webhook_slow:
    type: webhook
    from: sp_slow
    url: {webhook_url}
    one_row_per_request: true
    payload_version: 0
    batch_size: 1
"#,
        topic = ctx.kafka_topic,
        webhook_url = webhook.webhook_url(),
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .timeout(std::time::Duration::from_secs(120))
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__INTERNAL_BUFFER_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // Fast branch drains every record.
    let pg_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.scanshare_upstream_fast")
        .await
        .expect("Failed to query PostgreSQL count");
    assert_eq!(
        pg_count, records_to_produce,
        "Postgres (fast branch) should receive all rows"
    );

    // Slow webhook branch received traffic (its lag drives the backpressure).
    assert!(
        webhook
            .wait_for_requests(1, std::time::Duration::from_secs(10))
            .await,
        "slow webhook branch should have received at least one request, got {}",
        webhook.request_count()
    );

    // Give metrics time to flush to Prometheus.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Bug-1 wiring guard: the `up_source -> shared_producer` edge (into the
    // scan-shared producer) must be attributed with `downstream_id="shared_producer"`.
    // Before the construction-time attribution of the stashed `base_exec`, this
    // edge emitted untagged (no `downstream_id`), so this query would be 0.
    let upstream_edge_query = format!(
        "sum({})",
        PrometheusResource::backpressure_by_downstream_query("shared_producer", None)
    );
    let upstream_edge = prometheus
        .wait_for_metric_at_least(&upstream_edge_query, 1, 30, 500)
        .await
        .expect(
            "up_source -> shared_producer edge must be tagged with downstream_id=shared_producer",
        );
    assert!(
        upstream_edge >= 1,
        "expected the up_source->shared_producer edge to be tagged and non-zero, got {upstream_edge}ms"
    );
}

/// Guards **bug 2's wiring** end-to-end in the exact shape it was first observed
/// (QA case B): a *linear* `source -> transform -> webhook` chain where the single
/// webhook sink sets `batch_size: 1`, so the pipeline inserts a `RebatchExec`
/// between the feeding transform and the sink.
///
/// The attribution rule recurses into `RebatchExec::inner()` (plus the
/// `WrappingExec` inner-recursion), so the feeding transform is tagged
/// `downstream_id="<webhook sink>"` even with the sink-local rebatcher between
/// the transform and sink.
///
/// The assertion pins BOTH `id` (the feeding transform) and `downstream_id` (the
/// sink). This matters: pre-fix the sink name was stamped on the *wrong* node, so
/// a downstream-only query could pass spuriously — this exact
/// `(id="bp2_xform", downstream_id="bp2_web")` series only exists once the feeding
/// transform itself is correctly attributed. `bp2_xform` carries a `WHERE` so it
/// is not an identity projection that could be inlined away (which would leave no
/// transform node to attribute).
#[tokio::test]
async fn test_linear_rebatch_webhook_edge_attribution() {
    init_tracing();

    use streamling_e2e::resources::WebhookResource;

    let ctx = match TestContext::with_options(TestContextOptions::new().with_prometheus()).await {
        Ok(ctx) => ctx,
        Err(e) => {
            eprintln!("Skipping test - could not create context: {}", e);
            return;
        }
    };

    let prometheus = match &ctx.prometheus {
        Some(p) => p,
        None => {
            eprintln!("Skipping test - Prometheus not configured");
            return;
        }
    };

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let webhook = WebhookResource::new()
        .await
        .expect("Failed to start webhook server");
    webhook.set_delay(std::time::Duration::from_millis(100));

    let records_to_produce: i64 = 30;
    let records: Vec<TestRecord> = (1..=records_to_produce)
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

    // Linear chain, single webhook sink with `batch_size: 1` => a `RebatchExec`
    // is inserted between `bp2_xform` and `bp2_web`. The `WHERE` keeps every row
    // (ids 1..=30) but makes the transform non-identity so it is a real node.
    let pipeline = format!(
        r#"
sources:
  bp2_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  bp2_xform:
    type: sql
    sql: "SELECT id, value, timestamp FROM bp2_source WHERE id > 0"
    primary_key: id

sinks:
  bp2_web:
    type: webhook
    from: bp2_xform
    url: {webhook_url}
    one_row_per_request: true
    payload_version: 0
    batch_size: 1
"#,
        topic = ctx.kafka_topic,
        webhook_url = webhook.webhook_url(),
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records_to_produce as u64)
                .timeout(std::time::Duration::from_secs(120))
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__INTERNAL_BUFFER_SIZE", "1")
                // Keep the webhook sink from prefetching the full input before
                // its artificial delay applies; otherwise there may be no
                // sustained linear backpressure for WrappingExec to measure.
                .env("STREAMLING__EXTERNAL_HTTP_HANDLER__BUFFER_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success(), "Streamling should exit successfully");

    // The slow webhook received traffic (its lag is the backpressure under test).
    assert!(
        webhook
            .wait_for_requests(1, std::time::Duration::from_secs(10))
            .await,
        "webhook sink should have received at least one request, got {}",
        webhook.request_count()
    );

    // Give metrics time to flush to Prometheus.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // The `bp2_xform -> bp2_web` edge must be attributed to the *feeding transform*
    // (`id="bp2_xform"`) AND the *sink* (`downstream_id="bp2_web"`). Both labels are
    // pinned so the pre-fix mislabel (sink name stamped on the wrong upstream node)
    // cannot satisfy this query.
    let edge_query = concat!(
        "sum(streamling_node_wait_milliseconds_total",
        "{state=\"blocked\",id=\"bp2_xform\",downstream_id=\"bp2_web\"})"
    );
    let edge = prometheus
        .wait_for_metric_at_least(edge_query, 1, 30, 500)
        .await
        .expect("bp2_xform -> bp2_web edge must be tagged (id=bp2_xform, downstream_id=bp2_web)");
    assert!(
        edge >= 1,
        "expected the bp2_xform->bp2_web edge to be tagged and non-zero, got {edge}ms"
    );
}
