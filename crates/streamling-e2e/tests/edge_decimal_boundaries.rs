//! Adversarial e2e tests: decimal_arb precision/scale BOUNDARIES through the
//! Kafka(Avro) -> Postgres NUMERIC pipeline.
//!
//! These probe the edges where the avro->arrow decimal routing switches
//! representation (Decimal128 <= 38 < Decimal256 <= 76 < decimal_arb) and where
//! byte-width changes (16/17/32/33 bytes), looking for silent truncation,
//! precision loss, or runtime errors. Failures here are findings.

use serde::Deserialize;
use sqlx::FromRow;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

#[derive(Debug, FromRow, Deserialize)]
struct IdText {
    #[allow(dead_code)]
    id: i64,
    t: String,
}

fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

/// Avro record schema: `id long` + `amount decimal(precision, scale)` (bytes).
fn decimal_schema(precision: u32, scale: u32) -> String {
    format!(
        r#"{{
            "type": "record",
            "name": "Amt",
            "fields": [
                {{"name": "id", "type": "long"}},
                {{"name": "amount", "type": {{"type": "bytes", "logicalType": "decimal", "precision": {precision}, "scale": {scale}}}}}
            ]
        }}"#
    )
}

/// Ingest `(id, unscaled)` decimal records through Kafka Avro -> Postgres into a
/// table `amounts(id BIGINT PK, amount <pg_type>)`, returning `(id, amount::text)`
/// rows ordered by id. Asserts the pipeline exits successfully.
async fn ingest_to_pg(
    ctx: &TestContext,
    precision: u32,
    scale: u32,
    pg_type: &str,
    cases: &[(i64, &str)],
) -> Vec<IdText> {
    ctx.postgres
        .execute(&format!(
            "CREATE TABLE amounts (id BIGINT PRIMARY KEY, amount {pg_type} NOT NULL)"
        ))
        .await
        .expect("create table");

    let schema = decimal_schema(precision, scale);
    for (id, unscaled) in cases {
        ctx.kafka
            .produce_decimal_record(&schema, *id, "amount", unscaled)
            .await
            .expect("produce decimal record");
    }

    let pipeline = format!(
        r#"
sources:
  amt_in:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id
transforms: {{}}
sinks:
  amt_out:
    type: postgres
    from: amt_in
    table: amounts
    schema: public
    primary_key: id
    on_conflict: update
"#,
        topic = ctx.kafka_topic,
    );

    let status = ctx
        .run_pipeline_with_opts(&pipeline, base_opts().record_limit(cases.len() as u64))
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    ctx.postgres
        .query("SELECT id, amount::text AS t FROM public.amounts ORDER BY id")
        .await
        .expect("query")
}

// ---------------------------------------------------------------------------
// Decimal128 territory (precision <= 38)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dec128_precision_1_scale_0() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let rows = ingest_to_pg(&ctx, 1, 0, "NUMERIC(1,0)", &[(1, "7"), (2, "0"), (3, "-9")]).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].t, "7");
    assert_eq!(rows[1].t, "0");
    assert_eq!(rows[2].t, "-9");
}

#[tokio::test]
async fn dec128_max_precision_38_scale_0() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    // 38 nines: largest Decimal128 magnitude shape.
    let nines = "9".repeat(38);
    let rows = ingest_to_pg(&ctx, 38, 0, "NUMERIC(38,0)", &[(1, nines.as_str())]).await;
    assert_eq!(rows[0].t, nines);
}

#[tokio::test]
async fn dec128_scale_equals_precision() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    // decimal(10,10): all fractional. unscaled 1234567890 -> 0.1234567890.
    // F3 FIXED: the unscaled integer now binds with the point placed `scale`
    // from the right, so 0.1234567890 lands correctly in NUMERIC(40,10).
    let rows = ingest_to_pg(&ctx, 10, 10, "NUMERIC(40,10)", &[(1, "1234567890")]).await;
    assert_eq!(rows[0].t, "0.1234567890");
}

#[tokio::test]
async fn dec128_negative_near_min() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    // Large negative Decimal128(38,2). F3 FIXED: 36 integer digits + ".99".
    let neg = format!("-{}", "9".repeat(38));
    let rows = ingest_to_pg(&ctx, 38, 2, "NUMERIC(40,2)", &[(1, neg.as_str())]).await;
    assert_eq!(rows[0].t, format!("-{}.99", "9".repeat(36)));
}

// ---------------------------------------------------------------------------
// Decimal256 territory (38 < precision <= 76)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dec256_precision_39_boundary() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    // Precision 39 just crosses out of Decimal128 into Decimal256.
    let val = "1".to_string() + &"0".repeat(38); // 1e38 (39 digits)
    let rows = ingest_to_pg(&ctx, 39, 0, "NUMERIC(39,0)", &[(1, val.as_str())]).await;
    assert_eq!(rows[0].t, val);
}

#[tokio::test]
async fn dec256_max_precision_76() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let nines = "9".repeat(76);
    let rows = ingest_to_pg(&ctx, 76, 0, "NUMERIC(76,0)", &[(1, nines.as_str())]).await;
    assert_eq!(
        rows[0].t, nines,
        "76-digit value must survive Decimal256 path"
    );
}

#[tokio::test]
async fn dec256_seventeen_byte_value() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    // 2^130 needs 17 bytes (> Decimal128's 16) — the truncation boundary.
    let v = "1361129467683753853853498429727072845824"; // 2^130
    let rows = ingest_to_pg(&ctx, 50, 0, "NUMERIC(50,0)", &[(1, v)]).await;
    assert_eq!(rows[0].t, v, "17-byte value must not truncate to 128 bits");
}

#[tokio::test]
async fn dec256_negative_high_scale() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    // Negative high-scale Decimal256(60,30). F3 FIXED: 30 integer + 30 fractional
    // digits, fits NUMERIC(80,30) (the old code produced 60 integer digits).
    let neg = format!("-{}", "1".repeat(60));
    let rows = ingest_to_pg(&ctx, 60, 30, "NUMERIC(80,30)", &[(1, neg.as_str())]).await;
    assert_eq!(rows[0].t, format!("-{}.{}", "1".repeat(30), "1".repeat(30)));
}

// ---------------------------------------------------------------------------
// decimal_arb territory (precision > 76)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn decarb_precision_77_scale_0() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    // 77 -> decimal_arb with u256 hint. 2^256-1 is 78 digits, so use a 77-digit value.
    let v = "9".repeat(77);
    let rows = ingest_to_pg(&ctx, 77, 0, "NUMERIC(77,0)", &[(1, v.as_str())]).await;
    assert_eq!(rows[0].t, v);
}

#[tokio::test]
async fn decarb_precision_100_scale_0() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let v = "1".to_string() + &"0".repeat(99); // 1e99 (100 digits)
    let rows = ingest_to_pg(&ctx, 100, 0, "NUMERIC(100,0)", &[(1, v.as_str())]).await;
    assert_eq!(rows[0].t, v);
}

#[tokio::test]
async fn decarb_precision_100_scale_18() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    // 33-byte territory: a value needing > 32 bytes unscaled.
    let v = "1".to_string() + &"0".repeat(95); // 1e95
    let rows = ingest_to_pg(&ctx, 100, 18, "NUMERIC(100,18)", &[(1, v.as_str())]).await;
    // 1e95 unscaled at scale 18 -> 77 integer digits + 18 fractional.
    assert!(rows[0].t.contains('.'));
    assert_eq!(
        rows[0].t.chars().filter(|c| c.is_ascii_digit()).count(),
        95 + 1
    );
}

#[tokio::test]
async fn decarb_zero_and_negative_mix() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let rows = ingest_to_pg(
        &ctx,
        90,
        10,
        "NUMERIC(90,10)",
        &[(1, "0"), (2, "-1"), (3, "1")],
    )
    .await;
    assert_eq!(rows[0].t, "0.0000000000");
    assert_eq!(rows[1].t, "-0.0000000001");
    assert_eq!(rows[2].t, "0.0000000001");
}

#[tokio::test]
async fn decarb_leading_zero_unscaled_bytes() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    // Small value in a wide column — exercises minimal-magnitude canonical encoding.
    let rows = ingest_to_pg(
        &ctx,
        80,
        0,
        "NUMERIC(80,0)",
        &[(1, "1"), (2, "255"), (3, "256")],
    )
    .await;
    assert_eq!(rows[0].t, "1");
    assert_eq!(rows[1].t, "255");
    assert_eq!(rows[2].t, "256");
}

#[tokio::test]
async fn decarb_value_exceeds_u256_range_78_digits() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    // 2^256 - 1 = 78 digits, beyond the 77..=78 u256-hint window's safe range
    // for some sinks; Postgres NUMERIC should still take it losslessly.
    let v = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    let rows = ingest_to_pg(&ctx, 100, 0, "NUMERIC(100,0)", &[(1, v)]).await;
    assert_eq!(
        rows[0].t, v,
        "2^256-1 must round-trip through decimal_arb -> NUMERIC"
    );
}
