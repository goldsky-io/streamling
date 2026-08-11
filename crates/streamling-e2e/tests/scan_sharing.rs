//! Scan-sharing e2e tests.
//!
//! When two or more transforms read the same source, streamling scans it once
//! and broadcasts the rows. The broadcast used to coalesce a multi-partition
//! source down to a single stream before fanning out, so enabling scan sharing
//! silently cost the pipeline its parallelism. Partition j of the shared source
//! now feeds partition j of every consumer.

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

#[derive(Debug, Clone, Serialize)]
struct TestRecord {
    id: i64,
    value: String,
}

const TEST_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "value", "type": "string"}
    ]
}"#;

/// Two transforms share one parallel Kafka source. Both must receive every row
/// of every partition — a partition that never gets broadcast to one consumer is
/// silent, partial data loss on that branch only.
#[tokio::test]
async fn test_scan_sharing_over_a_parallel_source() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new())
        .await
        .expect("Failed to create test context");

    let topic = ctx
        .create_kafka_topic_with_partitions("shared_parallel", 4)
        .await
        .expect("Failed to create multi-partition topic");
    topic
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let total_records: i64 = 100;
    let records: Vec<TestRecord> = (1..=total_records)
        .map(|i| TestRecord {
            id: i,
            value: format!("value_{i}"),
        })
        .collect();
    topic
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // Two transforms reading the same source is what turns scan sharing on.
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    parallelism: 2
    starting_offsets: earliest
    primary_key: id

transforms:
  branch_one:
    type: sql
    primary_key: id
    sql: SELECT id, value, _gs_op FROM kafka_source

  branch_two:
    type: sql
    primary_key: id
    sql: SELECT id, value, _gs_op FROM kafka_source

sinks:
  sink_one:
    type: postgres
    from: branch_one
    table: scan_shared_one
    schema: public
    primary_key: id
    on_conflict: update

  sink_two:
    type: postgres
    from: branch_two
    table: scan_shared_two
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = topic.topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(total_records as u64)
                .timeout(std::time::Duration::from_secs(120)),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "Streamling should exit successfully");

    for table in ["scan_shared_one", "scan_shared_two"] {
        let count = ctx
            .postgres
            .count(&format!("SELECT COUNT(*) FROM public.{table}"))
            .await
            .expect("Failed to query count");
        assert_eq!(
            count, total_records,
            "{table} must receive every row of every shared-source partition"
        );

        let missing: Vec<(i64,)> = ctx
            .postgres
            .query(&format!(
                "SELECT s.id FROM generate_series(1, 100) AS s(id) \
                 LEFT JOIN public.{table} o ON o.id = s.id \
                 WHERE o.id IS NULL ORDER BY s.id"
            ))
            .await
            .expect("Failed to query missing ids");
        assert!(
            missing.is_empty(),
            "{table} lost rows: {:?}",
            missing.iter().map(|r| r.0).collect::<Vec<_>>()
        );
    }
}
