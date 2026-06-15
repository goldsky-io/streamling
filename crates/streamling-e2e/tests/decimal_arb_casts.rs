//! decimal_arb cast e2e tests.
//!
//! Validates the SQL-callable cast UDFs (T068) and the wide-DECIMAL CAST
//! preprocessor routing (T070) at the pipeline level.
//!
//! - **T067**: a pipeline that takes a `decimal_arb(100, 18)` column from
//!   Kafka Avro and runs `CAST(amount AS VARCHAR)` produces the canonical
//!   decimal text, byte-for-byte. This exercises the
//!   `decimal_arb_to_string` UDF wired into the projection layer.

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

fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

/// T067 (partial): `decimal_arb_to_string(decimal_arb_col)` emits the
/// canonical decimal text directly via the cast UDF (T068). The
/// destination column is Postgres TEXT.
///
/// Note: the implicit `CAST(decimal_arb_col AS VARCHAR)` path is *not*
/// supported — DataFusion's built-in cast tries to interpret the
/// LargeBinary bytes as UTF-8 and fails. Use the explicit UDF, or rely
/// on connector-side canonical-text projection (which is what the
/// Postgres sink does automatically via `build_projection_for_postgres`).
#[tokio::test]
async fn test_cast_decimal_arb_to_varchar() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute(
            "CREATE TABLE casted (\
                 id BIGINT PRIMARY KEY, \
                 amount_text TEXT NOT NULL\
             )",
        )
        .await
        .unwrap();

    // Two cases: small positive and negative.
    let cases: [(i64, &str, &str); 2] = [
        (1, "1234567890123456789", "1.234567890123456789"),
        (2, "-99000000000000000000", "-99.000000000000000000"),
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
  casted:
    type: sql
    sql: "SELECT id, decimal_arb_to_string(amount) AS amount_text FROM payments_in"
    primary_key: id

sinks:
  out:
    type: postgres
    from: casted
    table: casted
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&pipeline, base_opts().record_limit(2))
        .await
        .expect("Streamling execution failed");
    assert!(status.success(), "Streamling should exit successfully");

    #[derive(FromRow)]
    struct Row {
        id: i64,
        amount_text: String,
    }
    let rows: Vec<Row> = ctx
        .postgres
        .query("SELECT id, amount_text FROM public.casted ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].amount_text, "1.234567890123456789");
    assert_eq!(rows[1].id, 2);
    assert_eq!(rows[1].amount_text, "-99.000000000000000000");
}
