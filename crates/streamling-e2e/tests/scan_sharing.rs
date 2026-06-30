//! Scan-sharing e2e tests.
//!
//! When a single source has more than one distinct consumer (here: two SQL
//! transforms), streamling auto-enables scan sharing: the source is scanned once
//! and a `BroadcastStream` fans the rows out to each consumer. This is a
//! different code path from multi-sink fan-out (a single multi-sink group counts
//! as one consumer).
//!
//! These tests prove the unified backpressure edge-metric works on the
//! scan-sharing path: a slow consumer's blocked-send time is attributed to it
//! via the `downstream_id` label on `streamling_backpressure_milliseconds_total`,
//! while a fast consumer sharing the same source is not charged. As in the
//! multi-sink fan-out, the producer's per-consumer edges are attributed to the
//! terminal sink (`downstream_id="webhook_slow"` / `"pg_fast"`), not the
//! intermediate transform.

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
///   1. accrue backpressure on the shared producer's fan-out edge to the slow
///      sink (`backpressure{id="scanshare_source", downstream_id="webhook_slow"}`),
///      and
///   2. charge that edge materially more than the fast sink's edge
///      (`downstream_id="pg_fast"`) — the per-consumer isolation guarantee of the
///      BroadcastStream.
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

    // The shared producer's fan-out edge to the slow sink must accrue blocked
    // send time, attributed via `downstream_id="webhook_slow"` (this is the
    // scan-sharing BroadcastStream attribution under test). As with multi-sink,
    // the edge is named for the terminal sink, not the intermediate transform.
    let slow_edge_query = format!(
        "sum({})",
        PrometheusResource::backpressure_by_downstream_query("webhook_slow", None)
    );
    let slow_edge = prometheus
        .wait_for_metric_at_least(&slow_edge_query, 50, 30, 500)
        .await
        .expect("slow scan-sharing consumer must accrue attributed blocked-send time");
    assert!(
        slow_edge >= 50,
        "expected substantial backpressure attributed to webhook_slow, got {slow_edge}ms"
    );

    // The fast sink shares the same source but must be charged far less — the
    // BroadcastStream isolates per-consumer blocked time, so a single slow
    // consumer cannot smear backpressure onto its fast sibling.
    let fast_edge_query = format!(
        "sum({})",
        PrometheusResource::backpressure_by_downstream_query("pg_fast", None)
    );
    let fast_edge = prometheus
        .query_count(&fast_edge_query)
        .await
        .expect("query failed")
        .unwrap_or(0);
    assert!(
        fast_edge < slow_edge,
        "pg_fast ({fast_edge}ms) should be charged less backpressure than webhook_slow ({slow_edge}ms)"
    );
}
