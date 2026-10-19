//! `table` transform e2e tests.

use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

#[derive(Debug, Clone, Serialize)]
struct TimedRate {
    address: String,
    amount: i64,
    block_timestamp: i64,
}

const TIMED_RATE_SCHEMA: &str = r#"{
    "type": "record",
    "name": "TimedRate",
    "fields": [
        {"name": "address", "type": "string"},
        {"name": "amount", "type": "long"},
        {"name": "block_timestamp", "type": "long"}
    ]
}"#;

/// A windowed table garbage-collects the rows outside its window: a lookup
/// of one misses, and a lookup of a row inside the window hits. The table's
/// output still carries both rows.
#[tokio::test]
async fn test_table_window_evicts_rows_outside_it() {
    const LOAD_TIMEOUT: Duration = Duration::from_secs(60);
    const EXIT_DEADLINE: Duration = Duration::from_secs(30);
    const GC_INTERVAL: &str = "500ms";
    /// A few of the table's GC intervals.
    const GC_WAIT: Duration = Duration::from_secs(3);
    const TWO_HOURS_SECS: i64 = 2 * 3600;
    const RATES_LOADED: &str = "SELECT count(*) FROM public.window_rates";
    const ENRICHED: &str = "SELECT count(*) FROM public.window_enriched";

    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    ctx.kafka
        .register_schema(TIMED_RATE_SCHEMA)
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
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Clock before the Unix epoch")
        .as_secs() as i64;
    let rate = |address: &str, amount, block_timestamp| TimedRate {
        address: address.to_string(),
        amount,
        block_timestamp,
    };
    ctx.kafka
        .produce_avro_records(&[rate("0xa", 10, now - TWO_HOURS_SECS), rate("0xb", 20, now)])
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
    window: block_timestamp >= extract(epoch from now() - interval '1 hour')
    sql: SELECT address, amount, block_timestamp FROM rates
  enriched:
    type: sql
    primary_key: id
    sql: SELECT id, lookup_eq('rates_mapping', 'address', address, 'amount') AS calc_amount FROM transactions
sinks:
  rates_pg:
    type: postgres
    from: rates_mapping
    table: window_rates
    schema: public
    primary_key: address
    batch_size: 1
    batch_flush_interval: 100ms
  enriched_pg:
    type: postgres
    from: enriched
    table: window_enriched
    schema: public
    primary_key: id
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        rates_topic = ctx.kafka_topic,
        transactions_topic = transactions.topic,
    );

    // Rates reach Postgres only after the table applied them. The idle rates
    // source keeps delivering empty batches, which run the table's GC passes.
    let signal_when = async {
        if !wait_for_count(&ctx, RATES_LOADED, 2, LOAD_TIMEOUT).await {
            return;
        }
        tokio::time::sleep(GC_WAIT).await;
        let transaction = |id, address: &str| Transaction {
            id,
            address: address.to_string(),
        };
        transactions
            .produce_avro_records(&[transaction(1, "0xa"), transaction(2, "0xb")])
            .await
            .expect("Failed to produce transactions");
        wait_for_count(&ctx, ENRICHED, 2, LOAD_TIMEOUT).await;
    };

    let (status, _stderr) = ctx
        .run_pipeline_with_sigterm_when(
            &pipeline,
            PipelineOpts::new()
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__TABLE__WINDOW_GC_INTERVAL", GC_INTERVAL),
            signal_when,
            EXIT_DEADLINE,
        )
        .await
        .expect("Streamling must exit after SIGTERM");
    assert!(status.success());

    let rows: Vec<(i64, Option<i64>)> = ctx
        .postgres
        .query("SELECT id, calc_amount FROM public.window_enriched ORDER BY id")
        .await
        .expect("Failed to query");
    assert_eq!(rows, vec![(1, None), (2, Some(20))]);
}

#[derive(Debug, Clone, Serialize)]
struct Transfer {
    id: i64,
    address: String,
    amount: i64,
    block_timestamp: i64,
}

const TRANSFER_SCHEMA: &str = r#"{
    "type": "record",
    "name": "Transfer",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "address", "type": "string"},
        {"name": "amount", "type": "long"},
        {"name": "block_timestamp", "type": "long"}
    ]
}"#;

/// A `table_agg` over a windowed table aggregates its live rows: an upsert
/// replaces the key's previous amount, a delete and a window eviction drop
/// theirs, and a lookup of an address without rows misses.
#[tokio::test]
async fn test_table_agg_sliding_window() {
    const LOAD_TIMEOUT: Duration = Duration::from_secs(60);
    const EXIT_DEADLINE: Duration = Duration::from_secs(30);
    const GC_INTERVAL: &str = "500ms";
    /// A few of the table's GC intervals.
    const GC_WAIT: Duration = Duration::from_secs(3);
    const TWO_HOURS_SECS: i64 = 2 * 3600;
    const UPSERTS_LOADED: &str = "SELECT count(*) FROM public.agg_transfers \
         WHERE (id, amount) IN ((1, 30), (2, 5), (3, 7))";
    const DELETED: &str = "SELECT count(*) FROM public.agg_transfers WHERE id = 4";
    const ENRICHED: &str = "SELECT count(*) FROM public.agg_enriched";

    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    ctx.kafka
        .register_schema(TRANSFER_SCHEMA)
        .await
        .expect("Failed to register the transfer schema");
    let transactions = ctx
        .create_kafka_topic("transactions")
        .await
        .expect("Failed to create the transactions topic");
    transactions
        .register_schema(TRANSACTION_SCHEMA)
        .await
        .expect("Failed to register the transaction schema");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Clock before the Unix epoch")
        .as_secs() as i64;
    let transfer = |id, address: &str, amount, block_timestamp| Transfer {
        id,
        address: address.to_string(),
        amount,
        block_timestamp,
    };
    ctx.kafka
        .produce_avro_records(&[
            transfer(1, "0xa", 10, now),
            transfer(2, "0xa", 5, now - TWO_HOURS_SECS),
            transfer(3, "0xb", 7, now),
            transfer(4, "0xb", 100, now),
            transfer(1, "0xa", 30, now),
        ])
        .await
        .expect("Failed to produce transfers");
    ctx.kafka
        .produce_avro_records_with_op(&[transfer(4, "0xb", 100, now)], "d")
        .await
        .expect("Failed to produce the delete");

    let pipeline = format!(
        r#"
sources:
  transfers:
    type: kafka
    topic: {transfers_topic}
    starting_offsets: earliest
    primary_key: id
  transactions:
    type: kafka
    topic: {transactions_topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  transfer_window:
    type: table
    backend_type: in_memory
    primary_key: id
    window: block_timestamp >= extract(epoch from now() - interval '1 hour')
    sql: SELECT id, address, amount, block_timestamp FROM transfers
  volumes:
    type: table_agg
    sql: SELECT address, sum(amount) AS volume, count(*) AS transfers FROM transfer_window GROUP BY address
  enriched:
    type: sql
    primary_key: id
    sql: |
      SELECT
        id,
        lookup_eq('volumes', 'address', address, 'volume') AS volume,
        lookup_eq('volumes', 'address', address, 'transfers') AS transfers
      FROM transactions
sinks:
  transfers_pg:
    type: postgres
    from: transfer_window
    table: agg_transfers
    schema: public
    primary_key: id
    on_conflict: update
    batch_size: 1
    batch_flush_interval: 100ms
  enriched_pg:
    type: postgres
    from: enriched
    table: agg_enriched
    schema: public
    primary_key: id
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        transfers_topic = ctx.kafka_topic,
        transactions_topic = transactions.topic,
    );

    // Transfers reach Postgres only after the table, and so the aggregate,
    // applied them. The idle transfers source keeps delivering empty batches,
    // which run the table's GC passes.
    let signal_when = async {
        if !wait_for_count(&ctx, UPSERTS_LOADED, 3, LOAD_TIMEOUT).await
            || !wait_for_count(&ctx, DELETED, 0, LOAD_TIMEOUT).await
        {
            return;
        }
        tokio::time::sleep(GC_WAIT).await;
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
            PipelineOpts::new()
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__TABLE__WINDOW_GC_INTERVAL", GC_INTERVAL),
            signal_when,
            EXIT_DEADLINE,
        )
        .await
        .expect("Streamling must exit after SIGTERM");
    assert!(status.success());

    let rows: Vec<(i64, Option<i64>, Option<i64>)> = ctx
        .postgres
        .query("SELECT id, volume, transfers FROM public.agg_enriched ORDER BY id")
        .await
        .expect("Failed to query");
    assert_eq!(
        rows,
        vec![
            (1, Some(30), Some(1)),
            (2, Some(7), Some(1)),
            (3, None, None)
        ]
    );
}

/// Replaying month-old transfers through a `watermark()` window keeps the
/// hour before the latest transfer; a `now()` window would evict them all.
/// The table runs ahead of the lookups, so a lookup may see later transfers,
/// but never more than the window holds after some apply, and never the late
/// transfer, which is evicted in its own apply. The last lookup sees the
/// final window.
#[tokio::test]
async fn test_table_watermark_window_backfill() {
    const MONTH_SECS: i64 = 30 * 24 * 3600;
    const MINUTE_SECS: i64 = 60;
    /// The most transfers the window holds after any apply: minutes 0, 30, 50.
    const MOST_IN_WINDOW: i64 = 3;
    const LATE_AMOUNT: i64 = 100;

    init_tracing();
    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");
    ctx.kafka
        .register_schema(TRANSFER_SCHEMA)
        .await
        .expect("Failed to register the transfer schema");
    let start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Clock before the Unix epoch")
        .as_secs() as i64
        - MONTH_SECS;
    let transfer = |id, minutes: i64, amount| Transfer {
        id,
        address: "0xa".to_string(),
        amount,
        block_timestamp: start + minutes * MINUTE_SECS,
    };
    let transfers = [
        transfer(1, 0, 1),
        transfer(2, 30, 1),
        transfer(3, 50, 1),
        transfer(4, 120, 1),
        transfer(5, 10, LATE_AMOUNT),
        transfer(6, 125, 1),
    ];
    ctx.kafka
        .produce_avro_records(&transfers)
        .await
        .expect("Failed to produce transfers");

    let pipeline = format!(
        r#"
sources:
  transfers:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms:
  transfer_window:
    type: table
    backend_type: in_memory
    primary_key: id
    window: block_timestamp >= watermark(block_timestamp) - 3600
    sql: SELECT id, address, amount, block_timestamp FROM transfers
  hour_counts:
    type: table_agg
    sql: SELECT address, count(*) AS transfers, sum(amount) AS volume FROM transfer_window GROUP BY address
  enriched:
    type: sql
    primary_key: id
    sql: |
      SELECT
        id,
        lookup_eq('hour_counts', 'address', address, 'transfers') AS hour_transfers,
        lookup_eq('hour_counts', 'address', address, 'volume') AS hour_volume
      FROM transfers
sinks:
  enriched_pg:
    type: postgres
    from: enriched
    table: backfill_enriched
    schema: public
    primary_key: id
    batch_size: 1
    batch_flush_interval: 100ms
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(transfers.len() as u64)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1"),
        )
        .await
        .expect("Streamling execution failed");
    assert!(status.success());

    let rows: Vec<(i64, Option<i64>, Option<i64>)> = ctx
        .postgres
        .query("SELECT id, hour_transfers, hour_volume FROM public.backfill_enriched ORDER BY id")
        .await
        .expect("Failed to query");
    assert_eq!(rows.len(), transfers.len());
    for (id, hour_transfers, hour_volume) in &rows {
        assert!(
            hour_transfers.is_some_and(|count| (1..=MOST_IN_WINDOW).contains(&count)),
            "transfer {id} saw {hour_transfers:?} transfers"
        );
        assert!(
            hour_volume.is_some_and(|volume| volume < LATE_AMOUNT),
            "transfer {id} saw the late transfer: volume {hour_volume:?}"
        );
    }
    // Minutes 120 and 125.
    assert_eq!(rows.last(), Some(&(6, Some(2), Some(2))));
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
