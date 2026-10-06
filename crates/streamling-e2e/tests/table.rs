//! `table` transform e2e tests.

use serde::Serialize;
use std::time::Duration;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

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

const TABLE_TRANSFORM: &str = r#"
transforms:
  latest_values:
    type: table
    backend_type: in_memory
    primary_key: id
    deduplicate: false
    sql: SELECT id, value FROM kafka_source
"#;

fn record(id: i64, value: &str) -> TestRecord {
    TestRecord {
        id,
        value: value.to_string(),
    }
}

fn kafka_source(topic: &str) -> String {
    format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
"#
    )
}

/// Upserts, repeated keys and deletes (one of them of a never-seen key)
/// leave the table's final state in a Postgres sink that reads its changelog.
#[tokio::test]
async fn test_table_changelog_to_postgres() {
    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let upserts = [
        record(1, "a"),
        record(2, "b"),
        record(3, "c"),
        record(1, "a2"),
        record(3, "c2"),
    ];
    let deletes = [record(2, ""), record(99, "")];
    ctx.kafka
        .produce_avro_records(&upserts)
        .await
        .expect("Failed to produce upserts");
    ctx.kafka
        .produce_avro_records_with_op(&deletes, "d")
        .await
        .expect("Failed to produce deletes");

    let pipeline = format!(
        r#"{source}{TABLE_TRANSFORM}
sinks:
  pg_sink:
    type: postgres
    from: latest_values
    table: table_changelog
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        source = kafka_source(&ctx.kafka_topic),
    );

    // Without deduplication, each input row produces exactly one changelog row.
    let changelog_rows = (upserts.len() + deletes.len()) as u64;
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(changelog_rows)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success());

    let rows: Vec<(i64, String)> = ctx
        .postgres
        .query("SELECT id, value FROM public.table_changelog ORDER BY id")
        .await
        .expect("Failed to query");
    assert_eq!(rows, vec![(1, "a2".to_string()), (3, "c2".to_string())]);
}

/// A table nothing reads still acks checkpoints, so its source commits offsets.
#[tokio::test]
async fn test_table_without_readers_commits_offsets() {
    const NUM_RECORDS: i64 = 10;
    const COMMIT_TIMEOUT: Duration = Duration::from_secs(60);
    const POLL_INTERVAL: Duration = Duration::from_millis(250);
    const EXIT_DEADLINE: Duration = Duration::from_secs(30);

    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");
    let records: Vec<TestRecord> = (1..=NUM_RECORDS).map(|id| record(id, "v")).collect();
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        "{}{TABLE_TRANSFORM}\nsinks: {{}}\n",
        kafka_source(&ctx.kafka_topic)
    );
    let group_id = format!("e2e-test-{}-kafka_source", ctx.test_id);
    let all_committed = async {
        let deadline = tokio::time::Instant::now() + COMMIT_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            if ctx.kafka.committed_offset(&group_id).await.ok().flatten() == Some(NUM_RECORDS) {
                return;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    };

    let (status, _stderr) = ctx
        .run_pipeline_with_sigterm_when(
            &pipeline,
            PipelineOpts::new().env("STREAMLING__CHECKPOINT_INTERVAL_SEC", "1"),
            all_committed,
            EXIT_DEADLINE,
        )
        .await
        .expect("Streamling must exit after SIGTERM");

    assert!(status.success());
    assert_eq!(
        ctx.kafka.committed_offset(&group_id).await.unwrap(),
        Some(NUM_RECORDS),
        "offsets must commit through the table's acks"
    );
}
