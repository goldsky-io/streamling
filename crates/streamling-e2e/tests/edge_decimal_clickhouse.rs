//! Adversarial e2e tests: `decimal_arb` (and narrow Decimal) routing through
//! the Kafka(Avro) -> ClickHouse sink, exercising every materialization the
//! ClickHouse table provider can emit for a decimal column:
//!
//!   - native `UInt256` (decimal(77..=78, 0) -> u256 hint),
//!   - `String` via the FR-019 `coerce_to: string` opt-in (wide decimal_arb,
//!     p > 76 with a non-zero scale, which ClickHouse Decimal cannot hold),
//!   - narrow `Decimal(p, s)` / `Decimal256` (p <= 76).
//!
//! The goal is to surface runtime errors and silent corruption at the
//! representation-switch boundaries (Decimal128 <= 38 < Decimal256 <= 76 <
//! decimal_arb, and the 77..=78 u256-hint window). Some assertions document
//! EXPECTED failures (findings) — every test still compiles and runs a real
//! pipeline against produced input.

use serde::Deserialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

/// Row shape for read-back: `id` + the decimal column as text via `toString`.
#[derive(clickhouse::Row, Deserialize)]
struct IdAmount {
    id: i64,
    amount: String,
}

fn base_opts() -> PipelineOpts {
    PipelineOpts::new()
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "")
}

/// ClickHouse connection env, copied from `decimal_arb_clickhouse.rs` /
/// `wide_int_clickhouse.rs`.
fn clickhouse_env(ctx: &TestContext) -> Vec<(String, String)> {
    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");
    vec![
        (
            "STREAMLING__CLICKHOUSE_SINK__URL".to_string(),
            ctx.config.clickhouse_url.clone(),
        ),
        (
            "STREAMLING__CLICKHOUSE_SINK__DATABASE".to_string(),
            clickhouse.database.clone(),
        ),
        (
            "STREAMLING__CLICKHOUSE_SINK__USER".to_string(),
            "default".to_string(),
        ),
        (
            "STREAMLING__CLICKHOUSE_SINK__PASSWORD".to_string(),
            String::new(),
        ),
    ]
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

/// The canonical ClickHouse sink for these tests: one source `src`, one
/// `clickhouse` sink writing the produced records to `table` keyed on `id`.
/// Mirrors the sink YAML in `decimal_arb_clickhouse.rs` / `wide_int_clickhouse.rs`
/// (streamling auto-creates the table; no `engine`/`order_by` keys required).
fn pipeline_yaml(topic: &str, table: &str) -> String {
    format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms: {{}}

sinks:
  out:
    type: clickhouse
    from: src
    table: {table}
    primary_key: id
"#
    )
}

/// Build pipeline opts with the ClickHouse connection env folded in.
fn opts_with_clickhouse(ctx: &TestContext, record_limit: u64) -> PipelineOpts {
    let mut opts = base_opts().record_limit(record_limit);
    for (k, v) in clickhouse_env(ctx) {
        opts = opts.env(&k, &v);
    }
    opts
}

/// Read back `(id, toString(amount))` rows ordered by id.
async fn read_back(ctx: &TestContext, table: &str) -> Vec<IdAmount> {
    let clickhouse = ctx.clickhouse.as_ref().unwrap();
    clickhouse
        .query::<IdAmount>(&format!(
            "SELECT id, toString(amount) AS amount FROM {table} ORDER BY id"
        ))
        .await
        .expect("read back rows")
}

/// Fetch the emitted ClickHouse column type for `amount`.
async fn amount_column_type(ctx: &TestContext, table: &str) -> String {
    let clickhouse = ctx.clickhouse.as_ref().unwrap();
    let columns = clickhouse
        .get_column_types(table)
        .await
        .unwrap_or_else(|e| panic!("table {table} should exist: {e}"));
    columns
        .iter()
        .find(|(name, _)| name == "amount")
        .unwrap_or_else(|| panic!("amount column should exist in {table}; got {columns:?}"))
        .1
        .clone()
}

// ===========================================================================
// 1. decimal(77, 0) -> UInt256; mid-range (30-digit) value round-trips.
// ===========================================================================
#[tokio::test]
async fn dec77_scale0_uint256_round_trip() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(77, 0);
    let v = "123456789012345678901234567890"; // 30 digits
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", v)
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec77");
    let status = ctx
        .run_pipeline_with_opts(&yaml, opts_with_clickhouse(&ctx, 1))
        .await
        .expect("pipeline run");
    assert!(status.success(), "decimal(77,0) -> UInt256 should succeed");

    assert_eq!(
        amount_column_type(&ctx, "dec77").await,
        "UInt256",
        "decimal(77,0) must materialize as native UInt256"
    );
    let rows = read_back(&ctx, "dec77").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].amount, v, "30-digit value must round-trip exact");
}

// ===========================================================================
// 2. decimal(78, 0) -> UInt256; 77-digit value (stays inside 2^256 range).
// ===========================================================================
#[tokio::test]
async fn dec78_scale0_uint256_high_value() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(78, 0);
    // 77 nines < 2^256-1 (which is 78 digits) — safely in UInt256 range.
    let v = "9".repeat(77);
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", &v)
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec78_high");
    let status = ctx
        .run_pipeline_with_opts(&yaml, opts_with_clickhouse(&ctx, 1))
        .await
        .expect("pipeline run");
    assert!(status.success(), "decimal(78,0) high value should succeed");

    assert_eq!(amount_column_type(&ctx, "dec78_high").await, "UInt256");
    let rows = read_back(&ctx, "dec78_high").await;
    assert_eq!(
        rows[0].amount, v,
        "77-nines must round-trip through UInt256"
    );
}

// ===========================================================================
// 3. decimal(78, 0) value 0 -> UInt256 "0".
// ===========================================================================
#[tokio::test]
async fn dec78_scale0_uint256_zero() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(78, 0);
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", "0")
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec78_zero");
    let status = ctx
        .run_pipeline_with_opts(&yaml, opts_with_clickhouse(&ctx, 1))
        .await
        .expect("pipeline run");
    assert!(status.success(), "decimal(78,0) zero should succeed");

    assert_eq!(amount_column_type(&ctx, "dec78_zero").await, "UInt256");
    let rows = read_back(&ctx, "dec78_zero").await;
    assert_eq!(rows[0].amount, "0", "zero must round-trip as UInt256 \"0\"");
}

// ===========================================================================
// 4. decimal(90, 0) wide (no native u256 hint because p > 78) WITHOUT
//    coerce_to:string -> EXPECTED config-load rejection.
// ===========================================================================
#[tokio::test]
async fn dec90_scale0_no_coerce_rejected_at_config_load() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(90, 0);
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", "123")
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec90_reject");
    let out = ctx
        .run_pipeline_raw(&yaml, opts_with_clickhouse(&ctx, 1))
        .await
        .expect("pipeline binary should run");

    assert!(
        !out.status.success(),
        "wide decimal_arb(90,0) without coerce_to:string must be rejected. stdout:\n{}\nstderr:\n{}",
        out.stdout,
        out.stderr
    );
    let combined = format!("{}\n{}", out.stdout, out.stderr);
    assert!(
        combined.contains("amount"),
        "rejection should name the offending column: {combined}"
    );
}

// ===========================================================================
// 5. decimal(90, 0) WITH coerce_to:string -> stored as String; round-trips.
// ===========================================================================
#[tokio::test]
async fn dec90_scale0_coerce_string_round_trip() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(90, 0);
    let v = "1".to_string() + &"0".repeat(85); // 86-digit value, beyond UInt256
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", &v)
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec90_str");
    let mut opts = opts_with_clickhouse(&ctx, 1);
    opts = opts.env(
        "STREAMLING__CLICKHOUSE_SINK__COLUMNS",
        r#"[{"name":"amount","coerce_to":"string"}]"#,
    );
    let out = ctx
        .run_pipeline_raw(&yaml, opts)
        .await
        .expect("pipeline binary should run");
    assert!(
        out.status.success(),
        "decimal(90,0) with coerce_to:string should succeed. stdout:\n{}\nstderr:\n{}",
        out.stdout,
        out.stderr
    );

    assert_eq!(
        amount_column_type(&ctx, "dec90_str").await,
        "String",
        "coerce_to:string must materialize as ClickHouse String"
    );
    let rows = read_back(&ctx, "dec90_str").await;
    assert_eq!(
        rows[0].amount, v,
        "86-digit value must round-trip as String"
    );
}

// ===========================================================================
// 6. decimal(100, 18) wide fractional WITH coerce_to:string -> String.
// ===========================================================================
#[tokio::test]
async fn dec100_scale18_coerce_string_round_trip() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    // unscaled 1234567890123456789 at scale 18 -> 1.234567890123456789
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", "1234567890123456789")
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec100_str");
    let mut opts = opts_with_clickhouse(&ctx, 1);
    opts = opts.env(
        "STREAMLING__CLICKHOUSE_SINK__COLUMNS",
        r#"[{"name":"amount","coerce_to":"string"}]"#,
    );
    let out = ctx
        .run_pipeline_raw(&yaml, opts)
        .await
        .expect("pipeline binary should run");
    assert!(
        out.status.success(),
        "decimal(100,18) with coerce_to:string should succeed. stdout:\n{}\nstderr:\n{}",
        out.stdout,
        out.stderr
    );

    assert_eq!(amount_column_type(&ctx, "dec100_str").await, "String");
    let rows = read_back(&ctx, "dec100_str").await;
    assert_eq!(
        rows[0].amount, "1.234567890123456789",
        "fractional wide decimal_arb must store canonical decimal text"
    );
}

// ===========================================================================
// 7. narrow decimal(20, 2) -> ClickHouse Decimal(20, 2); value round-trips.
// ===========================================================================
#[tokio::test]
async fn dec20_scale2_native_decimal_round_trip() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(20, 2);
    // unscaled 123456789012345678 at scale 2 -> 1234567890123456.78
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", "123456789012345678")
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec20");
    let status = ctx
        .run_pipeline_with_opts(&yaml, opts_with_clickhouse(&ctx, 1))
        .await
        .expect("pipeline run");
    assert!(status.success(), "narrow decimal(20,2) should succeed");

    let col = amount_column_type(&ctx, "dec20").await;
    assert!(
        col.starts_with("Decimal"),
        "narrow decimal(20,2) must materialize as a ClickHouse Decimal type, got {col}"
    );
    let rows = read_back(&ctx, "dec20").await;
    assert_eq!(rows[0].amount, "1234567890123456.78");
}

// ===========================================================================
// 8. narrow decimal(38, 10) -> Decimal128 range; value round-trips.
// ===========================================================================
#[tokio::test]
async fn dec38_scale10_decimal128_round_trip() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(38, 10);
    // unscaled 123456789012345 at scale 10 -> 12345.6789012345 (genuinely
    // fractional, so the round-trip exercises the scale, not just an integer).
    let unscaled = "123456789012345".to_string();
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", &unscaled)
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec38");
    let status = ctx
        .run_pipeline_with_opts(&yaml, opts_with_clickhouse(&ctx, 1))
        .await
        .expect("pipeline run");
    assert!(status.success(), "narrow decimal(38,10) should succeed");

    let col = amount_column_type(&ctx, "dec38").await;
    assert!(
        col.starts_with("Decimal"),
        "decimal(38,10) must be a ClickHouse Decimal type, got {col}"
    );
    let rows = read_back(&ctx, "dec38").await;
    assert_eq!(
        rows[0].amount, "12345.6789012345",
        "decimal(38,10) value must round-trip losslessly through ClickHouse Decimal"
    );
}

// ===========================================================================
// 9. decimal(50, 0) (38 < p <= 76) -> Decimal(50,0) / Decimal256; round-trip.
// ===========================================================================
#[tokio::test]
async fn dec50_scale0_decimal256_round_trip() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(50, 0);
    // 2^130 = 39-digit value, > 128 bits, well inside Decimal256.
    let v = "1361129467683753853853498429727072845824";
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", v)
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec50");
    let status = ctx
        .run_pipeline_with_opts(&yaml, opts_with_clickhouse(&ctx, 1))
        .await
        .expect("pipeline run");
    assert!(status.success(), "decimal(50,0) should succeed");

    let col = amount_column_type(&ctx, "dec50").await;
    assert!(
        col.starts_with("Decimal"),
        "decimal(50,0) must be a ClickHouse Decimal type, got {col}"
    );
    let rows = read_back(&ctx, "dec50").await;
    assert_eq!(
        rows[0].amount, v,
        "17-byte value must not truncate to 128 bits"
    );
}

// ===========================================================================
// 10. negative value into decimal(78, 0) u256-hinted column.
//     UInt256 is UNSIGNED -> EXPECTED sink error / failure for the negative.
// ===========================================================================
#[tokio::test]
async fn dec78_scale0_negative_into_uint256_errors() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(78, 0);
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", "-1")
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec78_neg");
    // EXPECTATION: decimal(78,0) carries the u256 (UNSIGNED) native hint, so a
    // negative magnitude has no representable UInt256 encoding. The sink should
    // surface an error rather than silently storing a wrapped/garbage value.
    let out = ctx
        .run_pipeline_raw(&yaml, opts_with_clickhouse(&ctx, 1))
        .await
        .expect("pipeline binary should run");

    // Document the expected failure: a negative into an unsigned-hinted column
    // must NOT succeed silently. If it does, that is a finding (silent wrap).
    assert!(
        !out.status.success(),
        "negative value into UInt256-hinted decimal(78,0) must fail at the sink, \
         not store a wrapped value. stdout:\n{}\nstderr:\n{}",
        out.stdout,
        out.stderr
    );
}

// ===========================================================================
// 11. decimal(77, 0) max in-range value (76 nines) -> UInt256 exact.
// ===========================================================================
#[tokio::test]
async fn dec77_scale0_max_in_range_round_trip() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(77, 0);
    // 76 nines: largest magnitude that fits a 77-precision column and is safely
    // below 2^256-1 (78 digits).
    let v = "9".repeat(76);
    ctx.kafka
        .produce_decimal_record(&schema, 1, "amount", &v)
        .await
        .unwrap();

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec77_max");
    let status = ctx
        .run_pipeline_with_opts(&yaml, opts_with_clickhouse(&ctx, 1))
        .await
        .expect("pipeline run");
    assert!(status.success(), "decimal(77,0) 76-nines should succeed");

    assert_eq!(amount_column_type(&ctx, "dec77_max").await, "UInt256");
    let rows = read_back(&ctx, "dec77_max").await;
    assert_eq!(
        rows[0].amount, v,
        "76-nines must round-trip byte-exact via UInt256"
    );
}

// ===========================================================================
// 12. multiple rows, mixed magnitudes, decimal(78, 0) -> all UInt256.
// ===========================================================================
#[tokio::test]
async fn dec78_scale0_mixed_magnitudes_round_trip() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    let schema = decimal_schema(78, 0);
    let big = "9".repeat(77);
    let cases: [(i64, &str); 3] = [
        (1, "42"),                             // small
        (2, "123456789012345678901234567890"), // medium (30 digits)
        (3, big.as_str()),                     // large (77 nines)
    ];
    for (id, unscaled) in cases.iter() {
        ctx.kafka
            .produce_decimal_record(&schema, *id, "amount", unscaled)
            .await
            .unwrap();
    }

    let yaml = pipeline_yaml(&ctx.kafka_topic, "dec78_mixed");
    let status = ctx
        .run_pipeline_with_opts(&yaml, opts_with_clickhouse(&ctx, 3))
        .await
        .expect("pipeline run");
    assert!(
        status.success(),
        "decimal(78,0) mixed magnitudes should succeed"
    );

    assert_eq!(amount_column_type(&ctx, "dec78_mixed").await, "UInt256");
    let rows = read_back(&ctx, "dec78_mixed").await;
    assert_eq!(rows.len(), 3);
    for (i, (expected_id, expected)) in cases.iter().enumerate() {
        assert_eq!(rows[i].id, *expected_id);
        assert_eq!(
            rows[i].amount, *expected,
            "row {i} (id={expected_id}) must round-trip exact via UInt256"
        );
    }
}
