//! `table` transform e2e tests.

use serde::Serialize;
use std::time::Duration;
use streamling_e2e::resources::KafkaResource;
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
/// leave the table's final state in a Postgres sink that reads the table.
#[tokio::test]
async fn test_table_output_to_postgres() {
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
    table: table_output
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        source = kafka_source(&ctx.kafka_topic),
    );

    // The table's node emits its SQL output, one row per input row.
    let output_rows = (upserts.len() + deletes.len()) as u64;
    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(output_rows)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success());

    let rows: Vec<(i64, String)> = ctx
        .postgres
        .query("SELECT id, value FROM public.table_output ORDER BY id")
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

#[derive(Debug, Clone, Serialize)]
struct Rate {
    address: String,
    amount: i64,
}

#[derive(Debug, Clone, Serialize)]
struct Transaction {
    id: i64,
    address: String,
}

const RATE_SCHEMA: &str = r#"{
    "type": "record",
    "name": "Rate",
    "fields": [
        {"name": "address", "type": "string"},
        {"name": "amount", "type": "long"}
    ]
}"#;

const TRANSACTION_SCHEMA: &str = r#"{
    "type": "record",
    "name": "Transaction",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "address", "type": "string"}
    ]
}"#;

const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Polls a `count(*)` query until it returns `expected`, or `timeout` passes.
async fn wait_for_count(ctx: &TestContext, query: &str, expected: i64, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if ctx.postgres.count(query).await.ok() == Some(expected) {
            return true;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    false
}

/// A SQL transform on another source enriches rows from the table's live
/// rows: the latest value per key, and NULL for a key the table lacks.
#[tokio::test]
async fn test_lookup_eq_reads_the_table() {
    const LOAD_TIMEOUT: Duration = Duration::from_secs(60);
    const EXIT_DEADLINE: Duration = Duration::from_secs(30);
    const RATES_LOADED: &str = "SELECT count(*) FROM public.lookup_rates \
         WHERE (address, amount) IN (('0xa', 11), ('0xb', 20))";
    const ENRICHED: &str = "SELECT count(*) FROM public.lookup_enriched";

    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    ctx.kafka
        .register_schema(RATE_SCHEMA)
        .await
        .expect("Failed to register the rate schema");
    let transactions = ctx
        .create_kafka_topic("transactions")
        .await
        .expect("Failed to create the transactions topic");
    transactions
        .register_schema(TRANSACTION_SCHEMA)
        .await
        .expect("Failed to register the transaction schema");
    let rate = |address: &str, amount| Rate {
        address: address.to_string(),
        amount,
    };
    ctx.kafka
        .produce_avro_records(&[rate("0xa", 10), rate("0xb", 20), rate("0xa", 11)])
        .await
        .expect("Failed to produce rates");

    let pipeline = format!(
        r#"
sources:
  rates:
    type: kafka
    topic: {rates_topic}
    starting_offsets: earliest
    primary_key: address
  transactions:
    type: kafka
    topic: {transactions_topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  rates_mapping:
    type: table
    backend_type: in_memory
    primary_key: address
    sql: SELECT address, amount FROM rates
  enriched:
    type: sql
    primary_key: id
    sql: SELECT id, lookup_eq('rates_mapping', 'address', address, 'amount') AS calc_amount FROM transactions
sinks:
  rates_pg:
    type: postgres
    from: rates_mapping
    table: lookup_rates
    schema: public
    primary_key: address
    batch_size: 1
    batch_flush_interval: 100ms
  enriched_pg:
    type: postgres
    from: enriched
    table: lookup_enriched
    schema: public
    primary_key: id
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        rates_topic = ctx.kafka_topic,
        transactions_topic = transactions.topic,
    );

    // Rates reach Postgres only after the table applied them, so
    // transactions are produced once lookups are certain to hit.
    let signal_when = async {
        if !wait_for_count(&ctx, RATES_LOADED, 2, LOAD_TIMEOUT).await {
            return;
        }
        let transaction = |id, address: &str| Transaction {
            id,
            address: address.to_string(),
        };
        transactions
            .produce_avro_records(&[
                transaction(1, "0xa"),
                transaction(2, "0xb"),
                transaction(3, "0xc"),
            ])
            .await
            .expect("Failed to produce transactions");
        wait_for_count(&ctx, ENRICHED, 3, LOAD_TIMEOUT).await;
    };

    let (status, _stderr) = ctx
        .run_pipeline_with_sigterm_when(
            &pipeline,
            PipelineOpts::new().env("STREAMLING__RECORD_BATCH_SIZE", "1"),
            signal_when,
            EXIT_DEADLINE,
        )
        .await
        .expect("Streamling must exit after SIGTERM");
    assert!(status.success());

    let rows: Vec<(i64, Option<i64>)> = ctx
        .postgres
        .query("SELECT id, calc_amount FROM public.lookup_enriched ORDER BY id")
        .await
        .expect("Failed to query");
    assert_eq!(rows, vec![(1, Some(11)), (2, Some(20)), (3, None)]);
}

const SINGLE_PARTITION: i32 = 1;
const SELF_JOIN_PARTITIONS: i32 = 2;

/// A lookup into a table built from the reader's own source sees the reader's
/// row: the table applies each batch before the source hands it to the reader.
/// One row per batch, so a stale or missed lookup shows up in the final state.
/// Records are keyed by id, so all versions of a key share a Kafka partition.
async fn assert_self_join_never_misses(ctx: &TestContext, topic: &KafkaResource, parallelism: i32) {
    const NUM_IDS: i64 = 200;
    const NUM_UPDATED: i64 = 50;
    const MISMATCHES: &str =
        "SELECT count(*) FROM public.self_join WHERE looked_up IS DISTINCT FROM value";
    let wrong_final_values = format!(
        "SELECT count(*) FROM public.self_join_table WHERE value <> \
         CASE WHEN id <= {NUM_UPDATED} THEN 'u' || id ELSE 'v' || id END"
    );

    topic
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");
    let records: Vec<TestRecord> = (1..=NUM_IDS)
        .map(|id| record(id, &format!("v{id}")))
        .chain((1..=NUM_UPDATED).map(|id| record(id, &format!("u{id}"))))
        .collect();
    topic
        .produce_avro_records_keyed(&records, |record| record.id.to_string())
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"{source}    parallelism: {parallelism}{TABLE_TRANSFORM}  joined:
    type: sql
    primary_key: id
    sql: SELECT id, value, lookup_eq('latest_values', 'id', id, 'value') AS looked_up FROM kafka_source
sinks:
  pg_sink:
    type: postgres
    from: joined
    table: self_join
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
  table_sink:
    type: postgres
    from: latest_values
    table: self_join_table
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        source = kafka_source(&topic.topic),
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records.len() as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success());

    assert_eq!(
        ctx.postgres
            .count("SELECT count(*) FROM public.self_join")
            .await
            .expect("Failed to count rows"),
        NUM_IDS
    );
    assert_eq!(
        ctx.postgres
            .count(MISMATCHES)
            .await
            .expect("Failed to count mismatches"),
        0,
        "every lookup must see the row being processed"
    );
    assert_eq!(
        ctx.postgres
            .count("SELECT count(*) FROM public.self_join_table")
            .await
            .expect("Failed to count table rows"),
        NUM_IDS
    );
    assert_eq!(
        ctx.postgres
            .count(&wrong_final_values)
            .await
            .expect("Failed to count wrong final values"),
        0,
        "the table's output must end with every key's last value"
    );
}

#[tokio::test]
async fn test_lookup_eq_self_join_never_misses() {
    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    assert_self_join_never_misses(&ctx, &ctx.kafka, SINGLE_PARTITION).await;
}

/// Each source partition applies its own batches, so the guarantee holds in
/// every partition.
#[tokio::test]
async fn test_lookup_eq_self_join_never_misses_across_partitions() {
    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    let topic = ctx
        .create_kafka_topic_with_partitions("self_join", SELF_JOIN_PARTITIONS)
        .await
        .expect("Failed to create multi-partition topic");
    assert_self_join_never_misses(&ctx, &topic, SELF_JOIN_PARTITIONS).await;
}

/// A table reading a SQL transform is applied inside that transform's node.
#[tokio::test]
async fn test_table_reads_a_sql_transform() {
    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");

    let records = [record(1, "a"), record(2, "b"), record(1, "a2")];
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"{source}
transforms:
  clean:
    type: sql
    primary_key: id
    sql: SELECT id, value FROM kafka_source
  latest_values:
    type: table
    backend_type: in_memory
    primary_key: id
    sql: SELECT id, value FROM clean
sinks:
  pg_sink:
    type: postgres
    from: latest_values
    table: table_over_transform
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        source = kafka_source(&ctx.kafka_topic),
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(records.len() as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success());

    let rows: Vec<(i64, String)> = ctx
        .postgres
        .query("SELECT id, value FROM public.table_over_transform ORDER BY id")
        .await
        .expect("Failed to query");
    assert_eq!(rows, vec![(1, "a2".to_string()), (2, "b".to_string())]);
}

/// A table over a filtered source holds only the rows the filter keeps, and is
/// still applied before the source hands a batch to its other readers.
#[tokio::test]
async fn test_table_over_a_filtered_source() {
    const NUM_IDS: i64 = 20;
    const KEPT_IDS: i64 = NUM_IDS / 2;
    const MISMATCHES: &str =
        "SELECT count(*) FROM public.filtered_join WHERE looked_up IS DISTINCT FROM value";

    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    ctx.kafka
        .register_schema(TEST_SCHEMA)
        .await
        .expect("Failed to register schema");
    let records: Vec<TestRecord> = (1..=NUM_IDS)
        .map(|id| record(id, &format!("v{id}")))
        .collect();
    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    let pipeline = format!(
        r#"{source}    filter: "id % 2 = 0"{TABLE_TRANSFORM}  joined:
    type: sql
    primary_key: id
    sql: SELECT id, value, lookup_eq('latest_values', 'id', id, 'value') AS looked_up FROM kafka_source
sinks:
  pg_sink:
    type: postgres
    from: joined
    table: filtered_join
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
  table_sink:
    type: postgres
    from: latest_values
    table: filtered_table
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        source = kafka_source(&ctx.kafka_topic),
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(KEPT_IDS as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success());

    let rows: Vec<(i64, String)> = ctx
        .postgres
        .query("SELECT id, value FROM public.filtered_table ORDER BY id")
        .await
        .expect("Failed to query");
    let kept: Vec<(i64, String)> = (1..=NUM_IDS)
        .filter(|id| id % 2 == 0)
        .map(|id| (id, format!("v{id}")))
        .collect();
    assert_eq!(
        rows, kept,
        "the table must hold only the rows the filter keeps"
    );
    assert_eq!(
        ctx.postgres
            .count("SELECT count(*) FROM public.filtered_join")
            .await
            .expect("Failed to count rows"),
        KEPT_IDS
    );
    assert_eq!(
        ctx.postgres
            .count(MISMATCHES)
            .await
            .expect("Failed to count mismatches"),
        0,
        "every lookup must see the row being processed"
    );
}
