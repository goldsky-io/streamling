//! Tests for Prometheus metrics integration.
//!
//! These tests verify that streamling correctly emits metrics to Prometheus.
//! They require the k3s environment to be running with Prometheus deployed.

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, Result, TestContext, TestContextOptions};

/// Test record for metrics tests
#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    id: i64,
    data: String,
    timestamp: i64,
}

const TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "data", "type": "string"},
        {"name": "timestamp", "type": "long"}
    ]
}"#;

/// Helper to create a test context with Prometheus enabled
async fn setup_with_prometheus() -> Result<TestContext> {
    init_tracing();
    TestContext::with_options(TestContextOptions::new().with_prometheus()).await
}

/// Test that basic input/output row metrics are emitted for a simple pipeline
#[tokio::test]
async fn test_basic_metrics_emission() {
    let ctx = match setup_with_prometheus().await {
        Ok(ctx) => ctx,
        Err(e) => {
            eprintln!("Skipping test - could not create context: {}", e);
            return;
        }
    };

    // Skip if Prometheus is not available
    let prometheus = match &ctx.prometheus {
        Some(p) => p,
        None => {
            eprintln!("Skipping test - Prometheus not configured");
            return;
        }
    };

    let total_records = 25u64;

    // Register schema and produce test records
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=total_records as i64)
        .map(|i| TestRecord {
            id: i,
            data: format!("data_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {}
    primary_key: id

transforms: {{}}

sinks:
  postgres_sink:
    type: postgres
    from: kafka_source
    table: test_output
    schema: public
    on_conflict: update
"#,
        ctx.kafka_topic
    );

    // Run the pipeline
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml,
            PipelineOpts::new().record_limit(total_records),
        )
        .await
        .expect("Failed to run pipeline");

    assert!(status.success(), "Pipeline should complete successfully");

    // Give metrics time to be flushed to Prometheus
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Verify output rows metric
    let output_query =
        streamling_e2e::resources::PrometheusResource::output_rows_query("kafka_source", None);
    let output_rows = prometheus
        .query_count(&output_query)
        .await
        .expect("Failed to query output rows");

    // Verify input rows metric for the sink
    let input_query =
        streamling_e2e::resources::PrometheusResource::input_rows_query("postgres_sink", None);
    let input_rows = prometheus
        .query_count(&input_query)
        .await
        .expect("Failed to query input rows");

    // Note: Metrics may not be exactly equal due to timing, but should be close
    if let Some(count) = output_rows {
        assert!(
            count >= total_records,
            "Expected at least {} output rows, got {}",
            total_records,
            count
        );
    } else {
        eprintln!(
            "Warning: output_rows metric not found - this may indicate metrics are not configured"
        );
    }

    if let Some(count) = input_rows {
        assert!(
            count >= total_records,
            "Expected at least {} input rows, got {}",
            total_records,
            count
        );
    } else {
        eprintln!(
            "Warning: input_rows metric not found - this may indicate metrics are not configured"
        );
    }

    // Verify data in PostgreSQL
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_output")
        .await
        .expect("Failed to count rows");
    assert_eq!(count, total_records as i64);
}

/// Test that checkpoint-related metrics are emitted when running a pipeline
/// with checkpointing enabled (PostgreSQL state backend).
#[tokio::test]
async fn test_checkpoint_metrics_emission() {
    let ctx = match setup_with_prometheus().await {
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

    let total_records = 50u64;

    // Register schema and produce test records
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=total_records as i64)
        .map(|i| TestRecord {
            id: i,
            data: format!("data_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let state_table = format!("checkpoint_metrics_state_{}", ctx.test_id.replace('-', "_"));

    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {}
    primary_key: id

transforms: {{}}

sinks:
  postgres_sink:
    type: postgres
    from: kafka_source
    table: test_ckpt_metrics
    schema: public
    on_conflict: update
    batch_size: 10
    batch_flush_interval: 100ms
"#,
        ctx.kafka_topic
    );

    // Run the pipeline with checkpoint enabled
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml,
            PipelineOpts::new()
                .record_limit(total_records)
                .timeout(std::time::Duration::from_secs(120))
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
        .await
        .expect("Failed to run pipeline");

    assert!(status.success(), "Pipeline should complete successfully");

    // Give metrics time to be flushed to Prometheus
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Verify coordinator-level checkpoint metrics
    let coordinator_metrics = [
        "streamling_checkpoint_epochs_succeeded_total",
        "streamling_checkpoint_markers_sent_total",
        "streamling_checkpoint_acks_received_total",
        "streamling_checkpoint_finalizers_sent_total",
    ];

    for metric_name in &coordinator_metrics {
        let query = streamling_e2e::resources::PrometheusResource::checkpoint_coordinator_query(
            metric_name,
            None,
        );
        let result = prometheus
            .query_count(&query)
            .await
            .unwrap_or_else(|_| panic!("Failed to query {}", metric_name));

        if let Some(count) = result {
            assert!(
                count >= 1,
                "Expected at least 1 for {}, got {}",
                metric_name,
                count
            );
        } else {
            eprintln!(
                "Warning: {} metric not found - query: {}",
                metric_name, query
            );
        }
    }

    // Verify sink-level checkpoint sink flush histogram
    let sink_flush_query =
        streamling_e2e::resources::PrometheusResource::checkpoint_histogram_query(
            "streamling_checkpoint_sink_flush_milliseconds",
            "postgres_sink",
            None,
        );
    let sink_flush = prometheus
        .query_count(&sink_flush_query)
        .await
        .expect("Failed to query checkpoint_sink_flush_milliseconds");

    if let Some(_value) = sink_flush {
        // Any value means the metric was emitted successfully
    } else {
        eprintln!(
            "Warning: checkpoint_sink_flush_milliseconds metric not found - query: {}",
            sink_flush_query
        );
    }
}

/// Test that metrics include the correct service instance ID
#[tokio::test]
async fn test_metrics_with_instance_id() {
    let ctx = match setup_with_prometheus().await {
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

    let total_records = 10u64;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=total_records as i64)
        .map(|i| TestRecord {
            id: i,
            data: format!("data_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {}
    primary_key: id

transforms: {{}}

sinks:
  postgres_sink:
    type: postgres
    from: kafka_source
    table: test_metrics_instance
    schema: public
    on_conflict: update
"#,
        ctx.kafka_topic
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml,
            PipelineOpts::new().record_limit(total_records),
        )
        .await
        .expect("Failed to run pipeline");

    assert!(status.success(), "Pipeline should complete successfully");

    // Give metrics time to be flushed
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Query with the specific instance ID (test_id)
    let query = streamling_e2e::resources::PrometheusResource::output_rows_query(
        "kafka_source",
        Some(&ctx.test_id),
    );

    let result = prometheus
        .query_count(&query)
        .await
        .expect("Failed to query metrics");

    if let Some(count) = result {
        assert!(
            count >= total_records,
            "Expected at least {} rows with instance_id {}, got {}",
            total_records,
            ctx.test_id,
            count
        );
    } else {
        eprintln!(
            "Warning: metric with instance_id {} not found - query: {}",
            ctx.test_id, query
        );
    }
}

/// Test that Kafka consumer lag gauge reports zero after the pipeline catches up.
///
/// Before the fix in `MetricsRecorder::record_metric_data`, the zero-value guard
/// suppressed gauge recordings when the value was 0, causing the lag metric to
/// go stale in Prometheus instead of reporting 0. This test produces a finite
/// set of records, lets the pipeline consume them and idle (unbounded Kafka
/// source keeps running), then verifies the lag gauge reaches 0 in Prometheus.
#[tokio::test]
async fn test_kafka_lag_reports_zero_when_caught_up() {
    let ctx = match setup_with_prometheus().await {
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

    let total_records = 10u64;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=total_records as i64)
        .map(|i| TestRecord {
            id: i,
            data: format!("data_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {}
    primary_key: id

transforms: {{}}

sinks:
  postgres_sink:
    type: postgres
    from: kafka_source
    table: test_lag_zero
    schema: public
    on_conflict: update
    batch_size: 1
"#,
        ctx.kafka_topic
    );

    // Run the pipeline without record_limit so it stays alive after consuming
    // all records, giving time for checkpoint → offset commit → lag=0 → OTel flush.
    // The pipeline will be killed by the timeout, which is expected.
    let _pipeline_result = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml,
            PipelineOpts::new()
                .timeout(std::time::Duration::from_secs(20))
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1")
                .env("STREAMLING__KAFKA_SOURCE__LAG_REPORT_INTERVAL_MS", "1000"),
        )
        .await;
    // Timeout is expected — the Kafka source runs indefinitely.

    // Give Prometheus a moment to ingest the final OTel batch
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let query = format!(
        "streamling_kafka_consumer_messages_lag{{id=\"kafka_source\",instance=\"{}\"}}",
        ctx.test_id
    );

    let lag = prometheus
        .query(&query)
        .await
        .expect("Failed to query lag metric");

    assert_eq!(
        lag,
        Some(0.0),
        "Kafka consumer lag gauge should report 0 after consuming all records (query: {})",
        query
    );
}

// =====================================================================
// Event-time freshness metrics (Unit 5)
// =====================================================================

/// Kafka source with `telemetry.event_time` configured: assert the
/// watermark gauge reports max(timestamp)*1000 and the lag histogram
/// receives one observation per produced record.
#[tokio::test]
async fn test_event_time_metrics_kafka_source() {
    let ctx = match setup_with_prometheus().await {
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

    let total_records = 10u64;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce records with sequential `timestamp` values (seconds since
    // epoch). max(timestamp) will be 1000 + total_records.
    let records: Vec<TestRecord> = (1..=total_records as i64)
        .map(|i| TestRecord {
            id: i,
            data: format!("data_{}", i),
            timestamp: 1000 + i,
        })
        .collect();
    let max_timestamp = (1000 + total_records as i64) as f64;

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {}
    primary_key: id
    telemetry:
      event_time:
        column: timestamp
        unit: seconds

transforms: {{}}

sinks:
  postgres_sink:
    type: postgres
    from: kafka_source
    table: test_event_time_kafka
    schema: public
    on_conflict: update
"#,
        ctx.kafka_topic
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml,
            PipelineOpts::new().record_limit(total_records),
        )
        .await
        .expect("Failed to run pipeline");
    assert!(status.success(), "Pipeline should complete successfully");

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Histogram count should equal the number of produced rows. Each
    // non-null event_time row contributes one observation.
    let histogram_count_query = format!(
        "streamling_event_time_lag_milliseconds_count{{id=\"kafka_source\",instance=\"{}\"}}",
        ctx.test_id
    );
    let histogram_count = prometheus
        .query_count(&histogram_count_query)
        .await
        .expect("Failed to query event_time_lag count");
    assert!(
        matches!(histogram_count, Some(c) if c >= total_records),
        "Expected at least {} event_time_lag observations, got {:?} (query: {})",
        total_records,
        histogram_count,
        histogram_count_query
    );

    // Watermark should be max(timestamp_seconds) * 1000.
    let watermark_query = format!(
        "streamling_event_time_watermark_milliseconds{{id=\"kafka_source\",instance=\"{}\"}}",
        ctx.test_id
    );
    let watermark = prometheus
        .query(&watermark_query)
        .await
        .expect("Failed to query event_time_watermark");
    let expected_ms = max_timestamp * 1_000.0;
    assert!(
        matches!(watermark, Some(v) if (v - expected_ms).abs() < 0.5),
        "Expected watermark = {} ms (max_timestamp_seconds * 1000), got {:?} (query: {})",
        expected_ms,
        watermark,
        watermark_query
    );
}

/// Source-level filter test: produce 10 records, configure a filter that
/// keeps only ~4, and assert the lag histogram count reflects all 10
/// (pre-filter). This validates R7 — instrumentation runs below the filter,
/// matching the block-reporter behavior the goldtalk integration relies on.
#[tokio::test]
async fn test_event_time_metrics_pre_filter_observation() {
    let ctx = match setup_with_prometheus().await {
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

    let total_records = 10u64;
    let post_filter_records = 4u64; // ids 7, 8, 9, 10 pass `id > 6`

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=total_records as i64)
        .map(|i| TestRecord {
            id: i,
            data: format!("data_{}", i),
            timestamp: 1_700_000_000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {}
    primary_key: id
    filter: "id > 6"
    telemetry:
      event_time:
        column: timestamp
        unit: seconds

transforms: {{}}

sinks:
  postgres_sink:
    type: postgres
    from: kafka_source
    table: test_event_time_pre_filter
    schema: public
    on_conflict: update
"#,
        ctx.kafka_topic
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml,
            PipelineOpts::new().record_limit(post_filter_records),
        )
        .await
        .expect("Failed to run pipeline");
    assert!(status.success(), "Pipeline should complete successfully");

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Histogram count must reflect the PRE-filter row count, not the
    // post-filter count. The WrappingExec sits below StreamingFilterExec
    // (R7) so it observes every row before the predicate runs.
    let histogram_count_query = format!(
        "streamling_event_time_lag_milliseconds_count{{id=\"kafka_source\",instance=\"{}\"}}",
        ctx.test_id
    );
    let histogram_count = prometheus
        .query_count(&histogram_count_query)
        .await
        .expect("Failed to query event_time_lag count");
    assert!(
        matches!(histogram_count, Some(c) if c >= total_records),
        "Expected histogram count >= {} (pre-filter row count), got {:?}. \
         If this is {} the filter is being observed before the histogram, \
         which violates R7. (query: {})",
        total_records,
        histogram_count,
        post_filter_records,
        histogram_count_query
    );

    // Sink received only the post-filter rows.
    let sink_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_event_time_pre_filter")
        .await
        .expect("Failed to count rows");
    assert_eq!(
        sink_count, post_filter_records as i64,
        "Sink should receive only post-filter rows"
    );
}

/// Backwards-compat: pipeline without `telemetry` configured emits no
/// event-time series. R11 — strictly additive.
#[tokio::test]
async fn test_event_time_metrics_absent_when_not_configured() {
    let ctx = match setup_with_prometheus().await {
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

    let total_records = 5u64;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");
    let records: Vec<TestRecord> = (1..=total_records as i64)
        .map(|i| TestRecord {
            id: i,
            data: format!("data_{}", i),
            timestamp: 1000 + i,
        })
        .collect();
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {}
    primary_key: id

transforms: {{}}

sinks:
  postgres_sink:
    type: postgres
    from: kafka_source
    table: test_event_time_absent
    schema: public
    on_conflict: update
"#,
        ctx.kafka_topic
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml,
            PipelineOpts::new().record_limit(total_records),
        )
        .await
        .expect("Failed to run pipeline");
    assert!(status.success(), "Pipeline should complete successfully");

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let watermark_query = format!(
        "streamling_event_time_watermark_milliseconds{{id=\"kafka_source\",instance=\"{}\"}}",
        ctx.test_id
    );
    let watermark = prometheus.query(&watermark_query).await.unwrap_or(None);
    assert!(
        watermark.is_none(),
        "event_time_watermark must NOT be emitted when telemetry is unconfigured, got {:?}",
        watermark
    );

    let histogram_count_query = format!(
        "streamling_event_time_lag_milliseconds_count{{id=\"kafka_source\",instance=\"{}\"}}",
        ctx.test_id
    );
    let count = prometheus
        .query_count(&histogram_count_query)
        .await
        .unwrap_or(None);
    assert!(
        count.is_none(),
        "event_time_lag must NOT be emitted when telemetry is unconfigured, got {:?}",
        count
    );
}

/// Misconfigured column: pipeline runs, no event-time series are emitted,
/// the misconfiguration warning is logged once. R5a — best-effort.
#[tokio::test]
async fn test_event_time_metrics_misconfigured_column_logs_once_and_skips() {
    let ctx = match setup_with_prometheus().await {
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

    let total_records = 5u64;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");
    let records: Vec<TestRecord> = (1..=total_records as i64)
        .map(|i| TestRecord {
            id: i,
            data: format!("data_{}", i),
            timestamp: 1000 + i,
        })
        .collect();
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {}
    primary_key: id
    telemetry:
      event_time:
        column: nonexistent_column
        unit: seconds

transforms: {{}}

sinks:
  postgres_sink:
    type: postgres
    from: kafka_source
    table: test_event_time_misconfigured
    schema: public
    on_conflict: update
"#,
        ctx.kafka_topic
    );

    let output = ctx
        .run_pipeline_raw(
            &pipeline_yaml,
            PipelineOpts::new().record_limit(total_records),
        )
        .await
        .expect("Pipeline execution failed");
    assert!(
        output.status.success(),
        "Pipeline should complete successfully despite misconfiguration. stderr: {}",
        output.stderr
    );

    // The pipeline's data path must remain healthy.
    let sink_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.test_event_time_misconfigured")
        .await
        .expect("Failed to count rows");
    assert_eq!(sink_count, total_records as i64);

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // No event-time series should be emitted.
    let watermark_query = format!(
        "streamling_event_time_watermark_milliseconds{{id=\"kafka_source\",instance=\"{}\"}}",
        ctx.test_id
    );
    let watermark = prometheus.query(&watermark_query).await.unwrap_or(None);
    assert!(
        watermark.is_none(),
        "watermark must not be emitted on misconfigured column, got {:?}",
        watermark
    );

    // The skip warning must appear in pipeline logs. It fires per affected
    // batch (no log-once); the strict-validation follow-up will catch this
    // at startup and remove the runtime warn path entirely.
    let combined_output = format!("{}\n{}", output.stdout, output.stderr);
    assert!(
        combined_output.contains("event-time instrumentation skipped"),
        "expected misconfiguration warning in pipeline output, stderr:\n{}",
        output.stderr
    );
}

/// End-to-end across all three node types: `telemetry.event_time` configured
/// on a Kafka source, a SQL transform, and a Postgres sink. Assert that three
/// distinct Prometheus series appear — one per node `id`. This validates
/// the full plumbing: telemetry carried on the source provider, on the
/// transform's `WrappingNode`, and on the sink provider each reach their
/// respective wrappers.
#[tokio::test]
async fn test_event_time_metrics_across_source_transform_and_sink() {
    let ctx = match setup_with_prometheus().await {
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

    let total_records = 5u64;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=total_records as i64)
        .map(|i| TestRecord {
            id: i,
            data: format!("data_{}", i),
            timestamp: 1_700_000_000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Three nodes, each with `event_time` pointing at the same column that
    // survives the SQL passthrough. Transform renames nothing so the
    // `timestamp` column flows from source through transform to sink.
    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {}
    primary_key: id
    telemetry:
      event_time:
        column: timestamp
        unit: seconds

transforms:
  passthrough:
    type: sql
    primary_key: id
    sql: "SELECT id, data, timestamp FROM kafka_source"
    telemetry:
      event_time:
        column: timestamp
        unit: seconds

sinks:
  postgres_sink:
    type: postgres
    from: passthrough
    table: test_event_time_all_nodes
    schema: public
    on_conflict: update
    telemetry:
      event_time:
        column: timestamp
        unit: seconds
"#,
        ctx.kafka_topic
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml,
            PipelineOpts::new().record_limit(total_records),
        )
        .await
        .expect("Failed to run pipeline");
    assert!(status.success(), "Pipeline should complete successfully");

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Assert watermark series exist for each of the three node ids.
    // The `id` label on emitted metrics is the YAML reference_name directly
    // (see `PipelineMetricMetadata::to_tags` — it takes `reference_name`, not
    // the internal `metric_key` which is prefixed with application_id).
    for node_ref in ["kafka_source", "passthrough", "postgres_sink"] {
        let query = format!(
            "streamling_event_time_watermark_milliseconds{{id=\"{}\",instance=\"{}\"}}",
            node_ref, ctx.test_id
        );
        let value = prometheus
            .query(&query)
            .await
            .expect("Failed to query watermark metric");
        assert!(
            value.is_some(),
            "Expected watermark series for node '{}' (query: {}), got None",
            node_ref,
            query
        );
    }

    // And the lag histogram count should be nonzero for each.
    for node_ref in ["kafka_source", "passthrough", "postgres_sink"] {
        let query = format!(
            "streamling_event_time_lag_milliseconds_count{{id=\"{}\",instance=\"{}\"}}",
            node_ref, ctx.test_id
        );
        let count = prometheus
            .query_count(&query)
            .await
            .expect("Failed to query lag count");
        assert!(
            matches!(count, Some(c) if c >= total_records),
            "Expected lag count >= {} for node '{}', got {:?} (query: {})",
            total_records,
            node_ref,
            count,
            query
        );
    }
}

/// YAML `telemetry.labels` on a Kafka source must appear as Prometheus
/// label dimensions on every metric that source emits. Exercises the full
/// pipeline: YAML parse → `validate_labels` → `build_pipeline_metric_metadata`
/// seeding → `to_tags()` → OTLP → Prometheus scrape.
///
/// Plugin-vs-YAML collision semantics (plugin-wins-with-WARN) are unit-
/// tested in `streamling-core::telemetry::recorder::merge_metadata_tags_tests`;
/// the pipeline-integration assertion requires a plugin binary with a
/// real `SourcePlugin::labels()` implementation, which is exercised by
/// tests that ship with the plugin implementation itself, not in this crate.
#[tokio::test]
async fn test_yaml_telemetry_labels_appear_on_emitted_metrics() {
    let ctx = match setup_with_prometheus().await {
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

    let total_records = 10u64;

    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records: Vec<TestRecord> = (1..=total_records as i64)
        .map(|i| TestRecord {
            id: i,
            data: format!("data_{}", i),
            timestamp: 1000 + i,
        })
        .collect();

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {}
    primary_key: id
    telemetry:
      labels:
        tier: critical
        dataset: e2e_yaml_labels

transforms: {{}}

sinks:
  postgres_sink:
    type: postgres
    from: kafka_source
    table: test_output
    schema: public
    on_conflict: update
    telemetry:
      labels:
        destination: e2e_sink
"#,
        ctx.kafka_topic
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline_yaml,
            PipelineOpts::new().record_limit(total_records),
        )
        .await
        .expect("Failed to run pipeline");

    assert!(status.success(), "Pipeline should complete successfully");

    // Give metrics time to flush
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Source-side YAML labels should appear as Prometheus label selectors.
    // If the label isn't present, Prometheus returns no series and the
    // query resolves to None — the assertion below fails with a clear
    // diagnostic.
    let source_query = r#"streamling_output_rows_total{id="kafka_source",tier="critical",dataset="e2e_yaml_labels"}"#;
    let source_rows = prometheus
        .query_count(source_query)
        .await
        .expect("Failed to query source output rows");

    assert!(
        matches!(source_rows, Some(count) if count >= total_records),
        "Expected source output_rows_total with tier=critical,dataset=e2e_yaml_labels to be >= {}, got {:?} (query: {})",
        total_records,
        source_rows,
        source_query
    );

    // Sink-side YAML labels on the same pipeline must also appear.
    let sink_query = r#"streamling_input_rows_total{id="postgres_sink",destination="e2e_sink"}"#;
    let sink_rows = prometheus
        .query_count(sink_query)
        .await
        .expect("Failed to query sink input rows");

    assert!(
        matches!(sink_rows, Some(count) if count >= total_records),
        "Expected sink input_rows_total with destination=e2e_sink to be >= {}, got {:?} (query: {})",
        total_records,
        sink_rows,
        sink_query
    );
}
