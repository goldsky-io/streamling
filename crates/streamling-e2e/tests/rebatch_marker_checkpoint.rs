//! Checkpoints must progress when a `script` transform's rebatcher only ever
//! sees marker-carrying empty batches: parking those means no epoch is ever
//! acked, and the whole backlog acks at once on shutdown.

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

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

/// `batch_config` are the batching lines of the `script` transform, already
/// indented to the transform's field level.
async fn assert_checkpoints_progress(batch_config: &str) {
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

    let state_table = format!("script_marker_state_{}", ctx.test_id.replace("-", "_"));
    let application_id = format!("script_marker_test_{}", ctx.test_id);

    // `WHERE id > 1000000` matches nothing, so `upper` only ever receives
    // zero-row batches carrying checkpoint markers.
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
    sql: SELECT id, value FROM kafka_source WHERE id > 1000000
  upper:
    type: script
    from: filtered
    language: javascript
    primary_key: id
{batch_config}
    script: |
      function(input) {{
        return {{
          id: input.id,
          value: input.value.toUpperCase(),
        }};
      }}

sinks:
  pg_sink:
    type: postgres
    from: upper
    table: script_marker_out
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 10
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    // No record limit is reachable (the sink never sees a data row), so the
    // harness timeout ends the run. The assertion is that epochs finalized
    // while it ran.
    // A graceful drain would release parked markers; the harness timeout kills without one.
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
        "at least one checkpoint must finalize while the script rebatcher sees \
         only markers, got {}",
        checkpoint_count
    );
}

/// `batch_size: 0` installs a passthrough accumulator with no timer at all.
#[tokio::test]
async fn test_checkpoint_progresses_when_script_rebatcher_sees_only_markers() {
    assert_checkpoints_progress("    batch_size: 0").await;
}

/// `batch_size` alone: the size threshold is never reached, so the defaulted
/// flush interval is the backstop.
#[tokio::test]
async fn test_checkpoint_progresses_with_defaulted_script_flush_interval() {
    assert_checkpoints_progress("    batch_size: 100").await;
}
