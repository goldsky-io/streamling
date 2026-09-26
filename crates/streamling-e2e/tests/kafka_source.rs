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

/// A Kafka source with `parallelism: N` runs N consumer instances in one shared
/// group, so the broker splits the topic's partitions between them.
///
/// The property under test is that nothing is lost or duplicated by the split:
/// every produced row must arrive exactly once, no matter which instance read it.
#[tokio::test]
async fn kafka_source_parallelism_reads_every_partition_exactly_once() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Four partitions so a `parallelism: 2` source gets two apiece; keyed
    // production spreads the records instead of piling them onto partition 0.
    let topic = ctx
        .create_kafka_topic_with_partitions("parallel_src", 4)
        .await
        .expect("Failed to create multi-partition topic");

    let records: Vec<JsonRecord> = (1..=40)
        .map(|i| JsonRecord {
            id: i,
            name: format!("row_{i}"),
            amount: i as f64,
            active: i % 2 == 0,
        })
        .collect();

    topic
        .produce_json_records_keyed(&records, |r| r.id.to_string())
        .await
        .expect("Failed to produce JSON records");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    parallelism: 2
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
        topic = topic.topic,
    );

    let output = ctx
        .run_pipeline_with_capture(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records.len() as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .timeout(std::time::Duration::from_secs(120)),
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
        (1..=40).collect::<Vec<i64>>(),
        "every row must be delivered exactly once across the two consumer instances"
    );
}

/// A Kafka source is `UnknownPartitioning`, never `Hash`: Kafka places records
/// by murmur2 over the message key, which is unrelated to the hash a keyed sink
/// needs. A parallel source feeding a keyed sink must therefore still get a
/// sink-edge exchange, and the sink must see each key on exactly one stream.
#[tokio::test]
async fn kafka_source_parallelism_keeps_keys_together_at_a_keyed_sink() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    let topic = ctx
        .create_kafka_topic_with_partitions("parallel_keyed", 4)
        .await
        .expect("Failed to create multi-partition topic");

    // Ten keys, each updated three times. Keep-last dedup at the sink is only
    // correct if all three updates of a key travel one stream in order.
    let records: Vec<JsonRecord> = (0..30)
        .map(|i| JsonRecord {
            id: i % 10,
            name: format!("v{}", i / 10),
            amount: i as f64,
            active: true,
        })
        .collect();

    topic
        .produce_json_records_keyed(&records, |r| r.id.to_string())
        .await
        .expect("Failed to produce JSON records");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    parallelism: 2
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
        topic = topic.topic,
    );

    let output = ctx
        .run_pipeline_with_capture(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records.len() as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Pipeline execution failed");

    assert_eq!(
        output.len(),
        30,
        "no rows may be lost or duplicated by the exchange"
    );
    for key in 0..10i64 {
        let versions: Vec<String> = output
            .rows()
            .iter()
            .filter(|r| r.data.get("id").and_then(|v| v.as_i64()) == Some(key))
            .filter_map(|r| {
                r.data
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            })
            .collect();
        assert_eq!(
            versions,
            vec!["v0", "v1", "v2"],
            "key {key} must arrive in produced order on a single stream"
        );
    }
}
