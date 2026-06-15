//! ClickHouse decimal_arb e2e tests.
//!
//! Validates the pipeline-startup config-load wiring landed in T033/T064/T062:
//! a Kafka source with an Avro `decimal(p>76, s>0)` field auto-promotes to
//! `decimal_arb(p, s)`. Routing that column to ClickHouse must:
//!
//! - **T055**: reject at config load if the sink has no `coerce_to: string`
//!   directive (ClickHouse Decimal caps at 76 digits — FR-011 / FR-012).
//! - **T056**: succeed with the directive, emitting the column as ClickHouse
//!   `String` (FR-019 explicit opt-in).

use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

/// Avro schema with one wide-precision decimal field that triggers the
/// `decimal_arb(100, 18)` auto-promotion path.
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

// ============================================================================
// T055: ClickHouse rejects wide decimal_arb without coerce_to: string
// ============================================================================

/// A pipeline with a Kafka source carrying a `decimal_arb(100, 18)` column
/// must fail at config load when routed to ClickHouse with no
/// `coerce_to: string` directive (FR-011 / FR-012).
#[tokio::test]
async fn test_clickhouse_rejects_wide_decimal_arb_at_config_load() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    ctx.kafka
        .register_schema(WIDE_DECIMAL_SCHEMA)
        .await
        .expect("Failed to register schema");

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
    type: clickhouse
    from: payments_in
    table: payments
    primary_key: id
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new()
        .record_limit(1)
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "");
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }

    let output = ctx
        .run_pipeline_raw(&pipeline, opts)
        .await
        .expect("pipeline binary should run");

    assert!(
        !output.status.success(),
        "pipeline should reject at config load, got success"
    );

    let combined = format!("{}\n{}", output.stdout, output.stderr);
    assert!(
        combined.contains("amount"),
        "error names the offending column: {}",
        combined
    );
    assert!(
        combined.contains("clickhouse"),
        "error names the destination connector: {}",
        combined
    );
    assert!(
        combined.contains("coerce_to: string"),
        "error suggests the FR-019 opt-in: {}",
        combined
    );
}

// ============================================================================
// T056: ClickHouse accepts wide decimal_arb with coerce_to: string
// ============================================================================

/// The same pipeline as T055, but with `coerce_to: string` on the wide
/// decimal_arb column. CREATE TABLE should emit `String`, the pipeline
/// runs successfully, and the column carries canonical decimal text.
#[tokio::test]
async fn test_clickhouse_accepts_wide_decimal_arb_with_coerce_to_string() {
    init_tracing();

    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .expect("Failed to create test context");

    // Produce one record: amount = 1.234567890123456789 (18 frac digits).
    // The unscaled BigInt is "1234567890123456789".
    ctx.kafka
        .produce_decimal_record(WIDE_DECIMAL_SCHEMA, 1, "amount", "1234567890123456789")
        .await
        .expect("Failed to produce decimal record");

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
    type: clickhouse
    from: payments_in
    table: payments
    primary_key: id
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = PipelineOpts::new()
        .record_limit(1)
        .timeout(std::time::Duration::from_secs(60))
        .env("STREAMLING__PLUGIN__PATH", "")
        .env("STREAMLING__PLUGIN__PREPROCESSOR_IDS", "")
        .env("STREAMLING__PLUGIN__SIDE_OUTPUT_IDS", "");
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }
    // FR-019 opt-in via JSON-encoded column directive list (the env-var
    // shape — see `deserialize_optional_column_directives`).
    opts = opts.env(
        "STREAMLING__CLICKHOUSE_SINK__COLUMNS",
        r#"[{"name":"amount","coerce_to":"string"}]"#,
    );

    let output = ctx
        .run_pipeline_raw(&pipeline, opts)
        .await
        .expect("pipeline binary should run");

    assert!(
        output.status.success(),
        "pipeline should start with coerce_to: string. stdout:\n{}\nstderr:\n{}",
        output.stdout,
        output.stderr,
    );

    // Verify CREATE TABLE emitted the column as String, not Decimal.
    let clickhouse = ctx
        .clickhouse
        .as_ref()
        .expect("ClickHouse should be enabled");
    let columns = clickhouse
        .get_column_types("payments")
        .await
        .expect("payments table should exist");
    let amount = columns
        .iter()
        .find(|(name, _)| name == "amount")
        .expect("amount column should exist");
    assert_eq!(
        amount.1, "String",
        "wide decimal_arb with coerce_to: string must materialize as ClickHouse String, got {}",
        amount.1,
    );

    // C1 regression: the column type alone passing isn't enough — assert the
    // stored VALUE is the canonical decimal text. Before the sink learned to
    // convert decimal_arb, the raw canonical sign+magnitude bytes were shipped
    // verbatim and stored as a garbage String, which this assertion catches.
    let sample = clickhouse
        .get_sample_data_formatted("payments", 1)
        .await
        .expect("should read back payments sample data");
    assert!(
        sample.contains("1.234567890123456789"),
        "wide decimal_arb coerced to String must store canonical decimal text \
         (1.234567890123456789), got sample:\n{sample}",
    );
}
