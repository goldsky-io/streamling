//! PostgreSQL aggregation sink e2e tests.
//!
//! These tests verify that streamling can correctly read from Kafka and perform aggregations
//! while writing to PostgreSQL.

use serde::Serialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

// ============================================================================
// Test Record Types
// ============================================================================

#[derive(Debug, Clone, Serialize)]
struct Transfer {
    id: i64,
    account: String,
    amount: i64,
}

const TRANSFER_SCHEMA: &str = r#"{
    "type": "record",
    "name": "Transfer",
    "doc": "{\"primaryKeys\":[\"id\"]}",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "account", "type": "string"},
        {"name": "amount", "type": "long"}
    ]
}"#;

// ============================================================================
// Scenario: Basic Aggregation with Inserts and Deletes
// ============================================================================

/// Test basic aggregation with inserts and deletes
///
/// Test data:
/// - alice: +100 (id=1, insert), +50 (id=2, insert), -30 (id=3, delete) = 120
/// - bob: +200 (id=4, insert), +75 (id=5, insert), -100 (id=6, delete), -50 (id=7, delete) = 125
#[tokio::test]
async fn test_basic_aggregation_with_inserts_and_deletes() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TRANSFER_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce insert records
    let insert_records = vec![
        Transfer {
            id: 1,
            account: "alice".to_string(),
            amount: 100,
        },
        Transfer {
            id: 2,
            account: "alice".to_string(),
            amount: 50,
        },
        Transfer {
            id: 4,
            account: "bob".to_string(),
            amount: 200,
        },
        Transfer {
            id: 5,
            account: "bob".to_string(),
            amount: 75,
        },
        Transfer {
            id: 8,
            account: "alice".to_string(),
            amount: 0,
        },
    ];

    ctx.kafka
        .produce_avro_records(&insert_records)
        .await
        .expect("Failed to produce insert records");

    // Produce delete records
    let delete_records = vec![
        Transfer {
            id: 3,
            account: "alice".to_string(),
            amount: 30,
        },
        Transfer {
            id: 6,
            account: "bob".to_string(),
            amount: 100,
        },
        Transfer {
            id: 7,
            account: "bob".to_string(),
            amount: 50,
        },
    ];

    ctx.kafka
        .produce_avro_records_with_op(&delete_records, "d")
        .await
        .expect("Failed to produce delete records");

    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  postgres_agg:
    type: postgres_aggregate
    from: kafka_source
    landing_table: transfers
    agg_table: account_balances
    schema: streamling
    batch_flush_interval: 1s
    batch_size: 10
    primary_key: id
    group_by:
      account:
        type: text
    aggregate:
      balance:
        from: amount
        fn: sum
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(8)
                .env("STREAMLING__RECORD_BATCH_SIZE", "10")
                .env("STREAMLING__RECORD_BATCH_INTERVAL_MS", "1000"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success());

    // Verify landing table (append-only with composite PK)
    let landing_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM streamling.transfers")
        .await
        .expect("Failed to count landing table rows");

    assert_eq!(
        landing_count, 8,
        "Landing table should have exactly 8 rows (unique id+op combinations)"
    );

    // Verify _gs_op column exists in landing table
    let has_gs_op: Vec<(bool,)> = ctx
        .postgres
        .query(
            r#"SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'streamling'
                AND table_name = 'transfers'
                AND column_name = '_gs_op'
            )"#,
        )
        .await
        .expect("Failed to check _gs_op column");

    assert!(
        has_gs_op[0].0,
        "Landing table should have _gs_op column in append-only mode"
    );

    // Verify composite primary key exists
    let pk_columns: Vec<(String,)> = ctx
        .postgres
        .query(
            r#"SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '"streamling"."transfers"'::regclass AND i.indisprimary
            ORDER BY a.attnum"#,
        )
        .await
        .expect("Failed to fetch primary key columns");

    let pk_col_names: Vec<String> = pk_columns.iter().map(|(name,)| name.clone()).collect();
    assert!(
        pk_col_names.contains(&"id".to_string()) && pk_col_names.contains(&"_gs_op".to_string()),
        "Landing table should have composite PK (id, _gs_op), got: {:?}",
        pk_col_names
    );

    // Verify aggregation table
    let agg_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM streamling.account_balances")
        .await
        .expect("Failed to count aggregation table rows");

    assert_eq!(agg_count, 2, "Aggregation table should have 2 accounts");

    // Verify alice's balance: +100 (insert), +50 (insert), -30 (delete) = 120
    let alice_balance: Vec<(String, i64)> = ctx
        .postgres
        .query("SELECT account, balance FROM streamling.account_balances WHERE account = 'alice'")
        .await
        .expect("Failed to fetch alice's balance");

    assert_eq!(alice_balance.len(), 1);
    assert_eq!(
        alice_balance[0].1, 120,
        "Alice's balance should be 120 (100 + 50 - 30)"
    );

    // Verify bob's balance: +200 (insert), +75 (insert), -100 (delete), -50 (delete) = 125
    let bob_balance: Vec<(String, i64)> = ctx
        .postgres
        .query("SELECT account, balance FROM streamling.account_balances WHERE account = 'bob'")
        .await
        .expect("Failed to fetch bob's balance");

    assert_eq!(bob_balance.len(), 1);
    assert_eq!(
        bob_balance[0].1, 125,
        "Bob's balance should be 125 (200 + 75 - 100 - 50)"
    );
}

// ============================================================================
// Scenario: Deduplication
// ============================================================================

/// Test that duplicate records (same primary key and operation) are deduplicated
///
/// Test data:
/// - Send id=1, alice, 100 (insert) THREE times
/// - Send id=2, bob, 50 (insert) TWO times
/// - Expected: alice=100 (not 300), bob=50 (not 100)
#[tokio::test]
async fn test_aggregation_deduplication() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(TRANSFER_SCHEMA)
        .await
        .expect("Failed to register schema");

    // Produce duplicate records - same id and operation sent multiple times
    let records = vec![
        // alice's transfer sent 3 times (should be deduplicated to 1)
        Transfer {
            id: 1,
            account: "alice".to_string(),
            amount: 100,
        },
        Transfer {
            id: 1,
            account: "alice".to_string(),
            amount: 100,
        },
        Transfer {
            id: 1,
            account: "alice".to_string(),
            amount: 100,
        },
        // bob's transfer sent 2 times (should be deduplicated to 1)
        Transfer {
            id: 2,
            account: "bob".to_string(),
            amount: 50,
        },
        Transfer {
            id: 2,
            account: "bob".to_string(),
            amount: 50,
        },
    ];

    ctx.kafka
        .produce_avro_records(&records)
        .await
        .expect("Failed to produce records");

    // batch size 1 is needed to avoid built-in deduplication
    let pipeline = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  postgres_agg:
    type: postgres_aggregate
    from: kafka_source
    landing_table: transfers_dedup
    agg_table: balances_dedup
    schema: streamling
    batch_flush_interval: 1s
    batch_size: 1
    primary_key: id
    group_by:
      account:
        type: text
    aggregate:
      balance:
        from: amount
        fn: sum
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(
            &pipeline,
            PipelineOpts::new()
                .record_limit(5)
                .env("STREAMLING__RECORD_BATCH_SIZE", "1")
                .env("STREAMLING__RECORD_BATCH_INTERVAL_MS", "1000"),
        )
        .await
        .expect("Streamling execution failed");

    assert!(status.success());

    // Verify landing table has only 2 rows (deduplicated by id+_gs_op)
    let landing_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM streamling.transfers_dedup")
        .await
        .expect("Failed to count landing table rows");

    assert_eq!(
        landing_count, 2,
        "Landing table should have 2 rows after deduplication (not 5)"
    );

    // Verify aggregation table has 2 accounts
    let agg_count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM streamling.balances_dedup")
        .await
        .expect("Failed to count aggregation table rows");

    assert_eq!(agg_count, 2, "Aggregation table should have 2 accounts");

    // Verify alice's balance is 100 (not 300 from triple-counting)
    let alice_balance: Vec<(String, i64)> = ctx
        .postgres
        .query("SELECT account, balance FROM streamling.balances_dedup WHERE account = 'alice'")
        .await
        .expect("Failed to fetch alice's balance");

    assert_eq!(alice_balance.len(), 1);
    assert_eq!(
        alice_balance[0].1, 100,
        "Alice's balance should be 100 (deduplicated, not 300)"
    );

    // Verify bob's balance is 50 (not 100 from double-counting)
    let bob_balance: Vec<(String, i64)> = ctx
        .postgres
        .query("SELECT account, balance FROM streamling.balances_dedup WHERE account = 'bob'")
        .await
        .expect("Failed to fetch bob's balance");

    assert_eq!(bob_balance.len(), 1);
    assert_eq!(
        bob_balance[0].1, 50,
        "Bob's balance should be 50 (deduplicated, not 100)"
    );
}
