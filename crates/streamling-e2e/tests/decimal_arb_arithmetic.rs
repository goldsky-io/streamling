//! decimal_arb arithmetic SQL-transform e2e test.
//!
//! Validates that SQL operations on `decimal_arb` columns produce
//! byte-exact results when round-tripped through Kafka Avro → SQL
//! transform → Postgres NUMERIC.
//!
//! - **T038**: arithmetic (`+`) — auto-coerces wider precision per E5
//!   rules, no manual cast required. Negative values exercise the sign
//!   path (T046 regression guard).
//!
//! Aggregate and ORDER BY testing for decimal_arb columns is already
//! pinned by streamling-common unit tests:
//! - `decimal_arb_aggregates::tests` covers SUM/MIN/MAX/AVG/COUNT.
//! - `decimal_arb_sort_optimizer::tests` covers the i256-style negative
//!   sort regression at the LogicalPlan rewrite layer.
//!
//! The streamling streaming model doesn't accept bare aggregates or
//! window functions in transform SQL (those go through the
//! `postgres_aggregate` sink), so the E2E shape isn't a fit for those
//! features.

use serde::Deserialize;
use sqlx::FromRow;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

const WIDE_DECIMAL_SCHEMA: &str = r#"{
    "type": "record",
    "name": "Payment",
    "fields": [
        {"name": "id", "type": "long"},
        {
            "name": "amount",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 100,
                "scale": 18
            }
        }
    ]
}"#;

#[derive(Debug, FromRow, Deserialize)]
struct PaymentRow {
    #[allow(dead_code)]
    id: i64,
    amount_text: String,
}

fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

/// T038: `SELECT id, amount + amount AS doubled FROM src` produces the
/// arithmetic doubling byte-for-byte. No manual cast required.
#[tokio::test]
async fn test_decimal_arb_addition_via_sql_transform() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute(
            "CREATE TABLE doubled (\
                 id BIGINT PRIMARY KEY, \
                 doubled NUMERIC(101, 18) NOT NULL\
             )",
        )
        .await
        .unwrap();

    // Produce three records.
    let cases: [(i64, &str, &str); 3] = [
        (1, "1234567890123456789", "1.234567890123456789"),
        (2, "1000000000000000000", "1.000000000000000000"),
        (3, "-99000000000000000000", "-99.000000000000000000"),
    ];
    for (id, unscaled, _) in &cases {
        ctx.kafka
            .produce_decimal_record(WIDE_DECIMAL_SCHEMA, *id, "amount", unscaled)
            .await
            .unwrap();
    }

    let pipeline = format!(
        r#"
sources:
  payments_in:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  doubled:
    type: sql
    sql: "SELECT id, amount + amount AS doubled FROM payments_in"
    primary_key: id

sinks:
  out:
    type: postgres
    from: doubled
    table: doubled
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&pipeline, base_opts().record_limit(3))
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "Streamling should exit successfully");

    let rows: Vec<PaymentRow> = ctx
        .postgres
        .query("SELECT id, doubled::text AS amount_text FROM public.doubled ORDER BY id")
        .await
        .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].amount_text, "2.469135780246913578"); // 1.234… + 1.234…
    assert_eq!(rows[1].amount_text, "2.000000000000000000"); // 1 + 1
    assert_eq!(rows[2].amount_text, "-198.000000000000000000"); // -99 + -99
}

// Aggregate and ORDER BY e2e tests are intentionally NOT here — see the
// module-level comment for why. The corresponding unit tests live in
// `streamling-common::functions::decimal_arb_aggregates::tests` and
// `streamling-common::functions::decimal_arb_sort_optimizer::tests`.
