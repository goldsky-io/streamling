//! Usage metrics for billing: `input_rows_delta`, `input_bytes_delta` and
//! `output_bytes_delta`, plus the temporality of every exported row series.
//!
//! These read the raw OTLP export through [`OtlpCaptureResource`]: Prometheus'
//! OTLP receiver drops delta sums, so it cannot see the billing series.

use std::collections::BTreeMap;
use std::fs;
use std::time::Duration;

use serde::Serialize;
use streamling_e2e::resources::{OtlpCaptureResource, SumPoint, Temporality};
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

#[derive(Serialize)]
struct JsonRecord {
    id: i64,
    name: String,
}

fn json_record(id: i64) -> JsonRecord {
    JsonRecord {
        id,
        name: format!("name_{id}"),
    }
}

fn opts_with(
    capture: &OtlpCaptureResource,
    application_id: &str,
    opts: PipelineOpts,
) -> PipelineOpts {
    capture
        .env(application_id)
        .into_iter()
        .fold(opts, |opts, (k, v)| opts.env(k, v))
}

fn sum(points: &[SumPoint]) -> u64 {
    points.iter().map(|p| p.value).sum()
}

/// Kafka (JSON) source with a filter → SQL transform → blackhole sink, fed in two
/// waves so the counters export at least twice.
///
/// - Rows: the source pulls in 10 and keeps 7; the transform and the sink get 7.
/// - Bytes: the source's input bytes are exactly the Kafka payload bytes.
/// - Temporality: every `*_delta` series is delta and monotonic, every row
///   `*_total` series is cumulative. Delta points carry only the change since the
///   previous export, so they sum to the cumulative total.
#[tokio::test]
async fn kafka_usage_metrics_counts_and_temporality() {
    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    let capture = OtlpCaptureResource::new()
        .await
        .expect("Failed to start OTLP capture");

    let first_wave: Vec<JsonRecord> = (1..=5).map(json_record).collect();
    let second_wave: Vec<JsonRecord> = (6..=10).map(json_record).collect();
    let payload_bytes: u64 = first_wave
        .iter()
        .chain(&second_wave)
        .map(|r| serde_json::to_vec(r).unwrap().len() as u64)
        .sum();
    ctx.kafka
        .produce_json_records(&first_wave)
        .await
        .expect("Failed to produce first wave");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
    data_format: json
    filter: "id > 3"
    schema:
      id: int64
      name: string

transforms:
  sql_transform:
    type: sql
    sql: "SELECT id, name, _gs_op FROM kafka_source"
    primary_key: id

sinks:
  blackhole_sink:
    type: blackhole
    from: sql_transform
"#,
        topic = ctx.kafka_topic,
    );

    let opts = opts_with(
        &capture,
        &ctx.test_id,
        PipelineOpts::new()
            .record_limit(7)
            .timeout(Duration::from_secs(90)),
    );
    let (status, produced) = tokio::join!(ctx.run_pipeline_with_opts(&pipeline, opts), async {
        // Wait until the first wave has been exported on its own, so the two waves
        // land in separate delta exports.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while sum(&capture.node_points("streamling_input_rows_delta", "kafka_source")) < 5
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        ctx.kafka.produce_json_records(&second_wave).await
    });
    produced.expect("Failed to produce second wave");
    assert!(
        status.expect("Pipeline failed").success(),
        "pipeline should exit cleanly"
    );

    let points = capture.sum_points();
    assert!(!points.is_empty(), "no OTLP points received");

    // Temporality of every row and byte series.
    let mut temporality: BTreeMap<&str, (Temporality, bool)> = BTreeMap::new();
    for p in &points {
        let entry = temporality
            .entry(p.metric.as_str())
            .or_insert((p.temporality, p.monotonic));
        assert_eq!(
            *entry,
            (p.temporality, p.monotonic),
            "{} mixes temporalities",
            p.metric
        );
    }
    for name in [
        "streamling_input_rows_delta",
        "streamling_output_rows_delta",
        "streamling_input_bytes_delta",
        "streamling_output_bytes_delta",
    ] {
        assert_eq!(
            temporality.get(name),
            Some(&(Temporality::Delta, true)),
            "{name} must be a monotonic delta sum; got {temporality:?}"
        );
    }
    for name in ["streamling_input_rows", "streamling_output_rows"] {
        assert_eq!(
            temporality.get(name),
            Some(&(Temporality::Cumulative, true)),
            "{name} must be a monotonic cumulative sum; got {temporality:?}"
        );
    }

    // Row counts per node.
    let delta = |metric: &str, id: &str| sum(&capture.node_points(metric, id));
    for (metric, id, expected) in [
        ("streamling_input_rows_delta", "kafka_source", 10),
        ("streamling_output_rows_delta", "kafka_source", 7),
        ("streamling_input_rows_delta", "sql_transform", 7),
        ("streamling_output_rows_delta", "sql_transform", 7),
        ("streamling_input_rows_delta", "blackhole_sink", 7),
    ] {
        assert_eq!(delta(metric, id), expected, "{metric} of {id}");
    }

    // Bytes: wire bytes in, Arrow bytes out; no other node reports them.
    assert_eq!(
        delta("streamling_input_bytes_delta", "kafka_source"),
        payload_bytes,
        "source input bytes = Kafka payload bytes"
    );
    assert!(delta("streamling_output_bytes_delta", "blackhole_sink") > 0);
    for id in ["sql_transform", "blackhole_sink"] {
        assert_eq!(
            delta("streamling_input_bytes_delta", id),
            0,
            "{id} has no input bytes"
        );
    }
    for id in ["kafka_source", "sql_transform"] {
        assert_eq!(
            delta("streamling_output_bytes_delta", id),
            0,
            "{id} has no output bytes"
        );
    }

    // Delta vs cumulative: each delta point is the change since the last export,
    // so the deltas add up to the last cumulative value, and with two waves no
    // single delta point holds the whole total.
    for (id, total) in [
        ("kafka_source", 10),
        ("sql_transform", 7),
        ("blackhole_sink", 7),
    ] {
        let deltas: Vec<u64> = capture
            .node_points("streamling_input_rows_delta", id)
            .iter()
            .map(|p| p.value)
            .filter(|v| *v > 0)
            .collect();
        let cumulative = capture.node_points("streamling_input_rows", id);
        assert!(
            cumulative.windows(2).all(|w| w[0].value <= w[1].value),
            "{id}: cumulative input_rows must never decrease"
        );
        assert_eq!(
            cumulative.last().map(|p| p.value),
            Some(total),
            "{id}: last cumulative"
        );
        assert_eq!(
            deltas.iter().sum::<u64>(),
            total,
            "{id}: deltas sum to the total"
        );
        assert!(
            deltas.len() >= 2 && deltas.iter().all(|v| *v < total),
            "{id}: two waves must export as separate deltas, got {deltas:?}"
        );
    }
}

/// A bounded file source reports each file's full size as input bytes and every
/// row as input, once the files are read.
#[tokio::test]
async fn file_source_input_bytes_are_file_sizes() {
    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    let capture = OtlpCaptureResource::new()
        .await
        .expect("Failed to start OTLP capture");

    let data_dir = ctx.temp_dir.path().join("usage_csv");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let files = [
        ("a.csv", "id,name\n1,alice\n2,bob\n"),
        ("b.csv", "id,name\n3,carol\n4,dave\n5,erin\n"),
    ];
    for (name, contents) in files {
        fs::write(data_dir.join(name), contents).expect("write csv");
    }
    let file_bytes: u64 = files.iter().map(|(_, c)| c.len() as u64).sum();

    let pipeline = format!(
        r#"
sources:
  file_src:
    type: file
    path: {path}/
    format: csv
    primary_key: id
    mode:
      type: bounded

transforms: {{}}

sinks:
  blackhole_sink:
    type: blackhole
    from: file_src
"#,
        path = data_dir.display()
    );
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            opts_with(
                &capture,
                &ctx.test_id,
                PipelineOpts::new().timeout(Duration::from_secs(60)),
            ),
        )
        .await
        .expect("Pipeline failed");
    assert!(status.success(), "pipeline should exit cleanly");

    let delta = |metric: &str, id: &str| sum(&capture.node_points(metric, id));
    assert_eq!(
        delta("streamling_input_bytes_delta", "file_src"),
        file_bytes
    );
    assert_eq!(delta("streamling_input_rows_delta", "file_src"), 5);
    assert_eq!(delta("streamling_input_rows_delta", "blackhole_sink"), 5);
}

/// A hybrid source reports its input rows and bytes on the hybrid node itself:
/// its ClickHouse and Kafka phases count into it and report none of their own.
#[tokio::test]
async fn hybrid_source_input_counts_on_the_hybrid_node() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");
    let capture = OtlpCaptureResource::new()
        .await
        .expect("Failed to start OTLP capture");
    let clickhouse = ctx.clickhouse.as_ref().expect("ClickHouse not initialized");

    clickhouse
        .execute(
            "CREATE TABLE usage_hybrid (
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
    clickhouse
        .execute(
            "INSERT INTO usage_hybrid VALUES
            (1, 'a', 'A', 0, 0), (2, 'b', 'B', 0, 0), (3, 'c', 'C', 0, 0)",
        )
        .await
        .expect("Failed to insert ClickHouse rows");
    clickhouse
        .execute(
            "CREATE TABLE usage_hybrid_offsets (
                topic String, partition Int32, offset UInt32
            ) ENGINE = MergeTree() ORDER BY (topic, partition)",
        )
        .await
        .expect("Failed to create offset table");

    #[derive(Serialize)]
    struct AvroRecord {
        block: i64,
        id: String,
        data: String,
        timestamp: i64,
    }
    ctx.kafka
        .register_schema(
            r#"{"type":"record","name":"UsageHybrid","fields":[
                {"name":"block","type":"long"},
                {"name":"id","type":"string"},
                {"name":"data","type":"string"},
                {"name":"timestamp","type":"long"}
            ]}"#,
        )
        .await
        .expect("Failed to register schema");
    let kafka_records: Vec<AvroRecord> = (1..=3)
        .map(|i| AvroRecord {
            block: 100 + i,
            id: format!("k{i}"),
            data: format!("kafka_{i}"),
            timestamp: i,
        })
        .collect();
    ctx.kafka
        .produce_avro_records(&kafka_records)
        .await
        .expect("Failed to produce Kafka records");

    let pipeline = format!(
        r#"
sources:
  hybrid_source:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: usage_hybrid
        columns: block,id,data,timestamp
    unbounded_source:
      source_type: kafka
      topic: {topic}
      start_at: earliest
    offset_table:
      topic_name: {topic}
      table_name: usage_hybrid_offsets
    primary_key: id

transforms: {{}}

sinks:
  blackhole_sink:
    type: blackhole
    from: hybrid_source
"#,
        topic = ctx.kafka_topic,
    );
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            opts_with(
                &capture,
                &ctx.test_id,
                PipelineOpts::new()
                    .record_limit(6)
                    .timeout(Duration::from_secs(120)),
            ),
        )
        .await
        .expect("Pipeline failed");
    assert!(status.success(), "pipeline should exit cleanly");

    let delta = |metric: &str, id: &str| sum(&capture.node_points(metric, id));
    let hybrid_input = delta("streamling_input_rows_delta", "hybrid_source");
    assert!(
        hybrid_input >= 3,
        "the hybrid node counts at least the 3 ClickHouse rows; got {hybrid_input}"
    );
    assert_eq!(
        hybrid_input,
        delta("streamling_output_rows_delta", "hybrid_source"),
        "with no filter, the hybrid node's input equals its output"
    );
    assert!(delta("streamling_input_bytes_delta", "hybrid_source") > 0);
    for phase in ["hybrid_source_bounded_0", "hybrid_source_unbounded"] {
        for metric in [
            "streamling_input_rows_delta",
            "streamling_input_bytes_delta",
        ] {
            assert!(
                capture.node_points(metric, phase).is_empty(),
                "{phase} must not report {metric}; the hybrid node counts it"
            );
        }
    }
}
