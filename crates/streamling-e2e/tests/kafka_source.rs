//! Kafka source e2e tests.
//!
//! Verifies the Kafka source can decode JSON-encoded message payloads using a
//! config-supplied input `schema` (no Schema Registry). Each message payload is a
//! single UTF-8 JSON object produced via `produce_json_records`.

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

/// Test record serialized as a plain JSON object into the Kafka payload.
#[derive(Debug, Clone, Serialize)]
struct JsonRecord {
    id: i64,
    name: String,
    amount: f64,
    active: bool,
}

/// Basic happy path: produce JSON messages, decode them via `data_format: json`
/// plus an explicit schema, and verify the decoded rows on a print sink.
#[tokio::test]
async fn kafka_json_source_decodes_payloads() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // JSON source needs no Schema Registry — produce raw JSON objects (no dbz.op header).
    let records: Vec<JsonRecord> = vec![
        JsonRecord {
            id: 1,
            name: "alpha".to_string(),
            amount: 10.5,
            active: true,
        },
        JsonRecord {
            id: 2,
            name: "beta".to_string(),
            amount: 20.0,
            active: false,
        },
        JsonRecord {
            id: 3,
            name: "gamma".to_string(),
            amount: 30.25,
            active: true,
        },
    ];

    ctx.kafka
        .produce_json_records(&records)
        .await
        .expect("Failed to produce JSON records");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    data_format: json
    schema:
      id: int64
      name: string
      amount: float64
      active: boolean

transforms: {{}}

sinks:
  print_sink:
    type: print
    from: kafka_source
    sample_every: 1
"#,
        topic = ctx.kafka_topic,
    );

    let output = ctx
        .run_pipeline_with_capture(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records.len() as u64)
                // One row per batch so the print sink emits every decoded row.
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Pipeline execution failed");

    assert_eq!(output.len(), 3, "expected 3 decoded JSON rows");

    // Every declared column plus the synthesized _gs_op should be present.
    for column in ["id", "name", "amount", "active", "_gs_op"] {
        assert!(
            output.has_column(column),
            "missing column '{}'; got {:?}",
            column,
            output.column_names()
        );
    }

    // No dbz.op header was set, so each row defaults to an insert.
    for row in output.rows() {
        assert_eq!(row.row_kind, "Insert", "JSON rows must default to inserts");
    }
    for op in output.column_values("_gs_op") {
        assert_eq!(op.as_str(), Some("i"), "synthesized _gs_op must be 'i'");
    }

    // Values round-trip from the JSON payloads (order is preserved on a single partition,
    // but assert by id to stay robust).
    let mut ids: Vec<i64> = output
        .column_values("id")
        .iter()
        .filter_map(|v| v.as_i64())
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);

    let row_two = output
        .rows()
        .iter()
        .find(|r| r.data.get("id").and_then(|v| v.as_i64()) == Some(2))
        .expect("row with id=2");
    assert_eq!(
        row_two.data.get("name").and_then(|v| v.as_str()),
        Some("beta")
    );
    assert_eq!(
        row_two.data.get("amount").and_then(|v| v.as_f64()),
        Some(20.0)
    );
    assert_eq!(
        row_two.data.get("active").and_then(|v| v.as_bool()),
        Some(false)
    );
}

/// Test for column projection. When a downstream query prunes payload columns,
/// the JSON source must decode only the projected columns so the batch stays aligned with
/// the projected schema. Before the JSON path applied the column projection, this pipeline
/// failed with a `RecordBatch` column-count mismatch.
#[tokio::test]
async fn kafka_json_source_respects_column_projection() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    let records: Vec<JsonRecord> = vec![
        JsonRecord {
            id: 1,
            name: "alpha".to_string(),
            amount: 10.5,
            active: true,
        },
        JsonRecord {
            id: 2,
            name: "beta".to_string(),
            amount: 20.0,
            active: false,
        },
        JsonRecord {
            id: 3,
            name: "gamma".to_string(),
            amount: 30.25,
            active: true,
        },
    ];

    ctx.kafka
        .produce_json_records(&records)
        .await
        .expect("Failed to produce JSON records");

    // The SQL transform selects a subset, pruning `amount` and `active` from the source scan.
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    data_format: json
    schema:
      id: int64
      name: string
      amount: float64
      active: boolean

transforms:
  pick_columns:
    type: sql
    sql: "SELECT id, name FROM kafka_source"
    primary_key: id

sinks:
  print_sink:
    type: print
    from: pick_columns
    sample_every: 1
"#,
        topic = ctx.kafka_topic,
    );

    let output = ctx
        .run_pipeline_with_capture(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records.len() as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Pipeline execution failed");

    assert_eq!(output.len(), 3, "expected 3 projected rows");

    // The selected columns (plus the propagated _gs_op) survive...
    for column in ["id", "name", "_gs_op"] {
        assert!(
            output.has_column(column),
            "missing column '{}'; got {:?}",
            column,
            output.column_names()
        );
    }
    // ...and the pruned payload columns are gone.
    assert!(
        !output.has_column("amount"),
        "amount should be projected out; got {:?}",
        output.column_names()
    );
    assert!(
        !output.has_column("active"),
        "active should be projected out; got {:?}",
        output.column_names()
    );

    let mut ids: Vec<i64> = output
        .column_values("id")
        .iter()
        .filter_map(|v| v.as_i64())
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
}

/// `starting_offsets` given as a timestamp must skip everything produced before
/// that instant: the source resolves the timestamp to a concrete offset per
/// partition before it starts consuming, instead of replaying the topic.
#[tokio::test]
async fn kafka_json_source_starts_from_timestamp() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    let record = |id: i64, name: &str| JsonRecord {
        id,
        name: name.to_string(),
        amount: id as f64,
        active: true,
    };

    // Two batches separated by a gap wide enough that the cutoff below lands
    // strictly between their producer-assigned timestamps.
    let before: Vec<JsonRecord> = vec![record(1, "before-one"), record(2, "before-two")];
    ctx.kafka
        .produce_json_records(&before)
        .await
        .expect("Failed to produce the pre-cutoff records");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let cutoff_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is after the Unix epoch")
        .as_millis() as u64;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let after: Vec<JsonRecord> = vec![record(3, "after-one"), record(4, "after-two")];
    ctx.kafka
        .produce_json_records(&after)
        .await
        .expect("Failed to produce the post-cutoff records");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: "{cutoff_ms}"
    primary_key: id
    data_format: json
    schema:
      id: int64
      name: string
      amount: float64
      active: boolean

transforms: {{}}

sinks:
  print_sink:
    type: print
    from: kafka_source
    sample_every: 1
"#,
        topic = ctx.kafka_topic,
        cutoff_ms = cutoff_ms,
    );

    let output = ctx
        .run_pipeline_with_capture(
            &pipeline,
            PipelineOpts::new()
                .record_limit(after.len() as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .timeout(std::time::Duration::from_secs(60)),
        )
        .await
        .expect("Pipeline execution failed");

    let mut ids: Vec<i64> = output
        .column_values("id")
        .iter()
        .filter_map(|v| v.as_i64())
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![3, 4],
        "only records produced at or after the cutoff timestamp should be read"
    );
}
