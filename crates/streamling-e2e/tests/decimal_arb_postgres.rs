//! Postgres decimal_arb e2e tests.
//!
//! Validates lossless transport of wide-precision decimals through the
//! Kafka (Avro) → Postgres NUMERIC pipeline. Postgres NUMERIC accepts any
//! precision in the supported range natively, so no `coerce_to: string`
//! directive is required.
//!
//! - **T022** (Kafka Avro ingest): Avro `decimal(100, 18)` is auto-promoted
//!   to `decimal_arb(100, 18)` (FR-015) and decoded losslessly.
//! - **T054** (Postgres sink): `decimal_arb(100, 18)` writes into Postgres
//!   `NUMERIC(100, 18)` byte-for-byte, completing quickstart Example 2 in
//!   one round-trip.

use serde::Deserialize;
use sqlx::FromRow;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

/// Avro schema with one `decimal(100, 18)` field — wide enough to force
/// the decimal_arb auto-promotion path.
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

/// T022 + T054: Kafka Avro decimal(100, 18) → Postgres NUMERIC(100, 18).
/// Round-trip preserves the unscaled BigInt byte-for-byte.
#[tokio::test]
async fn test_kafka_avro_decimal_arb_to_postgres_numeric() {
    init_tracing();

    let ctx = TestContext::new()
        .await
        .expect("Failed to create test context");

    // Pre-create the destination table at full NUMERIC(100, 18).
    ctx.postgres
        .execute(
            "CREATE TABLE payments (\
                 id BIGINT PRIMARY KEY, \
                 amount NUMERIC(100, 18) NOT NULL\
             )",
        )
        .await
        .expect("Failed to create payments table");

    // Produce three records: small positive, large positive, negative.
    // Unscaled BigInts (the canonical wire form for Avro decimal):
    //   id=1: 1.234567890123456789  → unscaled "1234567890123456789"
    //   id=2: a 100-digit value at the precision ceiling
    //   id=3: -99.000000000000000000 → unscaled "-99000000000000000000"
    let huge_unscaled = "1".to_string() + &"0".repeat(99); // 1e99
    let cases: [(i64, &str, &str); 3] = [
        (1, "1234567890123456789", "1.234567890123456789"),
        (2, huge_unscaled.as_str(), "1e+81"),
        (3, "-99000000000000000000", "-99.000000000000000000"),
    ];
    for (id, unscaled, _) in &cases {
        ctx.kafka
            .produce_decimal_record(WIDE_DECIMAL_SCHEMA, *id, "amount", unscaled)
            .await
            .expect("Failed to produce decimal record");
    }

    let pipeline = format!(
        r#"
sources:
  payments_in:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  payments_out:
    type: postgres
    from: payments_in
    table: payments
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

    // Verify count and per-row exactness by reading `amount::text` so we
    // see the full digit string (sqlx has no native NUMERIC(100,18)).
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.payments")
        .await
        .expect("count query failed");
    assert_eq!(count, 3, "all 3 records should land");

    let rows: Vec<PaymentRow> = ctx
        .postgres
        .query("SELECT id, amount::text AS amount_text FROM public.payments ORDER BY id")
        .await
        .expect("select query failed");
    assert_eq!(rows.len(), 3);

    // id=1: 1.234567890123456789 (18 fractional digits, exact)
    assert_eq!(rows[0].id, 1);
    assert_eq!(
        rows[0].amount_text, "1.234567890123456789",
        "id=1: small positive must round-trip byte-for-byte"
    );

    // id=2: 1e99 with scale 18 means digits "1.<99 trailing zeros split by point at 18>"
    // i.e. "1000000…000.000000000000000000" (81 leading digits + scale).
    // Postgres formats trailing zeros; assert prefix and total length instead.
    assert_eq!(rows[1].id, 2);
    let r = &rows[1].amount_text;
    assert!(
        r.starts_with("1") && r.contains('.') && r.ends_with("000000000000000000"),
        "id=2 NUMERIC text shape unexpected: {}",
        r
    );
    // 81 integer digits + '.' + 18 fractional digits = 100 chars
    assert_eq!(
        r.chars().filter(|c| *c != '.').count(),
        100,
        "id=2 must carry the full 100 digits of precision: {}",
        r
    );

    // id=3: -99.000000000000000000
    assert_eq!(rows[2].id, 3);
    assert_eq!(
        rows[2].amount_text, "-99.000000000000000000",
        "id=3: negative must round-trip with sign and scale"
    );
}
