//! E2E reproduction of the wide-int text-cast regression: `CAST(wide_int_col AS TEXT)` works
//! in a streamling SQL transform without explicit UDF invocation.
//!
//! Pre-feature-002 this would have failed with:
//!   "Unsupported CAST from LargeBinary to Utf8View"
//! because the legacy u256/i256 path stored values as FixedSizeBinary(32)
//! and DataFusion has no built-in cast from that storage to text.
//!
//! The fix lives in the bigint SQL preprocessor: it rewrites
//! `CAST(decimal_arb_col AS TEXT/VARCHAR/STRING/CHAR)` (case-insensitive)
//! to `decimal_arb_to_string(decimal_arb_col)` *before* the SQL hits
//! DataFusion's planner.
//!
//! This test is the end-to-end confirmation that the rebuild closes the
//! original bug as a side effect of the architectural fix.

use sqlx::FromRow;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

const WIDE_INT_SCHEMA: &str = r#"{
    "type": "record",
    "name": "Trace",
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

/// Wide-int text-cast end-to-end: the canonical YAML pattern
/// `SELECT * EXCEPT col, CAST(col AS TEXT) AS col FROM src` works
/// against a source whose column auto-promotes to decimal_arb.
#[tokio::test]
async fn test_cast_wide_int_as_text_pipeline() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute(
            "CREATE TABLE traces_out (\
                 id BIGINT PRIMARY KEY, \
                 gas_used TEXT NOT NULL\
             )",
        )
        .await
        .unwrap();

    let cases: [(i64, &str); 3] = [
        (1, "12345"),
        (2, "0"),
        // 78-digit value near the u256 ceiling.
        (
            3,
            "123456789012345678901234567890123456789012345678901234567890123456789012345678",
        ),
    ];
    for (id, unscaled) in cases.iter() {
        ctx.kafka
            .produce_decimal_record(WIDE_INT_SCHEMA, *id, "gas_used", unscaled)
            .await
            .unwrap();
    }

    // The canonical CAST-AS-TEXT YAML pattern.
    let pipeline = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  casted_traces:
    type: sql
    sql: "SELECT * EXCEPT (gas_used), CAST(gas_used AS TEXT) AS gas_used FROM src"
    primary_key: id

sinks:
  out:
    type: postgres
    from: casted_traces
    table: traces_out
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
    assert!(
        status.success(),
        "wide-int text-cast: pipeline must start and run successfully"
    );

    #[derive(FromRow)]
    struct Row {
        id: i64,
        gas_used: String,
    }
    let rows: Vec<Row> = ctx
        .postgres
        .query("SELECT id, gas_used FROM public.traces_out ORDER BY id")
        .await
        .unwrap();

    assert_eq!(rows.len(), 3);
    for (i, (expected_id, expected_unscaled)) in cases.iter().enumerate() {
        assert_eq!(rows[i].id, *expected_id);
        assert_eq!(
            rows[i].gas_used, *expected_unscaled,
            "CAST AS TEXT must produce the canonical decimal string for value at row {}",
            i,
        );
    }
}
