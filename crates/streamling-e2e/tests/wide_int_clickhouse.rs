//! ClickHouse UInt256 / Int256 round-trip e2e tests.
//!
//! After feature 002, an Avro `decimal(p, 0)` source field with `p > 76`
//! auto-promotes to `decimal_arb(p, 0) + native_int_kind=u256/i256`. The
//! ClickHouse sink consumes the hint and:
//!   1. CREATE TABLE emits `UInt256` / `Int256` for the column.
//!   2. INSERT converts canonical decimal_arb bytes to 32-byte LE for
//!      ClickHouse-native storage.
//!
//! These tests verify the full round-trip end-to-end.
//!
//! Spec § US4 Acceptance Scenarios.

use serde::Deserialize;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};

const U256_SCHEMA: &str = r#"{
    "type": "record",
    "name": "Gas",
    "fields": [
        {"name": "id", "type": "long"},
        {
            "name": "gas_used",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 78,
                "scale": 0
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

/// US4 Acceptance Scenario 2: Avro `decimal(78, 0)` → ClickHouse `UInt256`
/// round-trip with native storage. The destination column type is
/// `UInt256` (not `Decimal(78, 0)`, not `String`).
#[tokio::test]
async fn test_uint256_clickhouse_round_trip() {
    init_tracing();
    let ctx = TestContext::with_options(TestContextOptions::new().with_clickhouse())
        .await
        .unwrap();

    ctx.kafka
        .register_schema(U256_SCHEMA)
        .await
        .expect("register schema");

    // Produce 3 unsigned wide-int values. The 78-digit value below has
    // exactly 78 digits, sized for the column's declared precision.
    let cases: [(i64, &str); 3] = [
        (1, "0"),
        (2, "12345"),
        (
            3,
            "999999999999999999999999999999999999999999999999999999999999999999999999999",
        ),
    ];
    for (id, unscaled) in cases.iter() {
        ctx.kafka
            .produce_decimal_record(U256_SCHEMA, *id, "gas_used", unscaled)
            .await
            .unwrap();
    }

    let pipeline = format!(
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
    table: gas_log
    primary_key: id
"#,
        topic = ctx.kafka_topic,
    );

    let mut opts = base_opts().record_limit(3);
    for (k, v) in clickhouse_env(&ctx) {
        opts = opts.env(&k, &v);
    }
    let status = ctx
        .run_pipeline_with_opts(&pipeline, opts)
        .await
        .expect("pipeline run");
    assert!(status.success(), "pipeline should succeed");

    // Verify CREATE TABLE emitted `UInt256` (not Decimal/String).
    let clickhouse = ctx.clickhouse.as_ref().unwrap();
    let columns = clickhouse
        .get_column_types("gas_log")
        .await
        .expect("gas_log table exists");
    let gas_used = columns
        .iter()
        .find(|(name, _)| name == "gas_used")
        .expect("gas_used column exists");
    assert_eq!(
        gas_used.1, "UInt256",
        "decimal_arb with native_int_kind=u256 must materialize as UInt256, got {}",
        gas_used.1,
    );

    // Verify values round-trip byte-exact via ClickHouse `toString`.
    #[derive(clickhouse::Row, Deserialize)]
    struct Row {
        id: i64,
        gas_used: String,
    }
    let rows: Vec<Row> = clickhouse
        .query("SELECT id, toString(gas_used) AS gas_used FROM gas_log ORDER BY id")
        .await
        .expect("select");
    assert_eq!(rows.len(), 3);
    for (i, (expected_id, expected_unscaled)) in cases.iter().enumerate() {
        assert_eq!(rows[i].id, *expected_id);
        assert_eq!(
            rows[i].gas_used, *expected_unscaled,
            "row {} (id={}) must round-trip byte-exact",
            i, expected_id
        );
    }
}

// Note: a prior e2e test `test_int256_clickhouse_round_trip_with_negatives`
// exercised Avro `decimal(77, 0)` → ClickHouse `Int256` round-trip with
// negative values. That auto-routing was retired (see PR #715 Bugbot
// finding "Avro precision 77 unsigned regression"): the historic Avro
// reader mapped *all* `decimal(p > 76, 0)` to U256, and inferring i256
// from p=77 silently re-classified existing unsigned data. With the
// current routing every `decimal(p, 0)` in 77..=78 carries the u256
// hint. The Int256 byte-conversion path is still exercised by unit
// tests in `streamling-connectors::table_providers::clickhouse::
// feature_002_byte_conversion_tests` (which construct an i256-hinted
// field directly). Restoring an end-to-end signed wide-int round-trip
// requires a future YAML-side `signed` directive on the source.
