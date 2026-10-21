//! E2E: `CAST` / `TRY_CAST` of decimal_arb columns to integer, float and
//! `DECIMAL` types, Kafka (Avro) -> SQL transform -> Postgres.
//!
//! An Avro `decimal(p > 76, s)` column auto-promotes to decimal_arb. These
//! pipelines cover the casts that used to fail to plan with "Unsupported CAST
//! from LargeBinary to ...":
//!
//! - `CAST(x AS BIGINT)` in a WHERE clause filters numerically, truncating
//!   toward zero;
//! - `CAST(x AS NUMERIC)` (no precision) keeps the exact value, written to a
//!   Postgres `NUMERIC` column digit for digit;
//! - `TRY_CAST(x AS BIGINT)` is NULL past the BIGINT range, and
//!   `CAST(x AS DOUBLE)` is the nearest double;
//! - `CAST(x AS DECIMAL(20, 2))` rounds a dropped digit half away from zero.
//!
//! Sink columns that receive a computed value are unconstrained `NUMERIC`, so
//! Postgres stores what the pipeline wrote instead of rounding it again.

use num_bigint::BigInt;
use serde::Deserialize;
use sqlx::FromRow;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

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

/// Produce `(id, unscaled amount)` records, in order, on the test topic.
async fn produce(ctx: &TestContext, precision: u32, scale: u32, cases: &[(i64, String)]) {
    let schema = decimal_schema(precision, scale);
    for (id, unscaled) in cases {
        ctx.kafka
            .produce_decimal_record(&schema, *id, "amount", unscaled)
            .await
            .expect("produce decimal record");
    }
}

/// One SQL transform from the Kafka topic into `public.results`.
fn sql_pipeline(topic: &str, sql: &str) -> String {
    format!(
        r#"
sources:
  amt_in:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  t:
    type: sql
    sql: "{sql}"
    primary_key: id

sinks:
  out:
    type: postgres
    from: t
    table: results
    schema: public
    primary_key: id
    on_conflict: update
"#,
    )
}

/// `value * 10^scale` as a base-10 string: the Avro unscaled form.
fn unscaled(value: &str, scale: u32) -> String {
    let (int, frac) = value.split_once('.').unwrap_or((value, ""));
    assert!(
        frac.len() <= scale as usize,
        "{value} has more than {scale} fractional digits"
    );
    let digits = format!("{int}{frac}{}", "0".repeat(scale as usize - frac.len()));
    BigInt::parse_bytes(digits.as_bytes(), 10)
        .expect("decimal digits")
        .to_string()
}

fn u256_max() -> BigInt {
    (BigInt::from(1) << 256u32) - 1
}

#[derive(Debug, FromRow, Deserialize)]
struct IdText {
    id: i64,
    t: String,
}

async fn result_texts(ctx: &TestContext) -> Vec<(i64, String)> {
    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t::text AS t FROM public.results ORDER BY id")
        .await
        .expect("select results");
    rows.into_iter().map(|r| (r.id, r.t)).collect()
}

// ---------------------------------------------------------------------------
// (a) WHERE CAST(x AS BIGINT) >= lo AND CAST(x AS BIGINT) <= hi
// ---------------------------------------------------------------------------

/// The bounds are inclusive and the cast truncates toward zero, so 999.99
/// (999) is below the range and 5000.75 (5000) is inside it. The projected
/// `CAST(amount AS BIGINT)` is the same truncated value, as a BIGINT column.
#[tokio::test]
async fn where_cast_as_bigint_keeps_exactly_the_rows_in_range() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute(
            "CREATE TABLE results (\
                 id BIGINT PRIMARY KEY, \
                 amount NUMERIC(100, 18) NOT NULL, \
                 whole BIGINT NOT NULL\
             )",
        )
        .await
        .unwrap();

    let cases = [
        (1, "999.99"),  // 999: below the lower bound
        (2, "1000"),    // lower bound, inclusive
        (3, "2500.5"),  // 2500
        (4, "5000.75"), // 5000: upper bound after truncation
        (5, "5001"),    // above the upper bound
        (6, "-1000"),   // negative, below
        (7, "4999"),    // kept; produced last so every row before it was read
    ];
    let records: Vec<(i64, String)> = cases.iter().map(|(id, v)| (*id, unscaled(v, 18))).collect();
    produce(&ctx, 100, 18, &records).await;

    let yaml = sql_pipeline(
        &ctx.kafka_topic,
        "SELECT id, amount AS amount, CAST(amount AS BIGINT) AS whole FROM amt_in \
         WHERE CAST(amount AS BIGINT) >= 1000 AND CAST(amount AS BIGINT) <= 5000",
    );
    // The sink counts written rows: the 4 kept rows, the last of which is the
    // last record produced.
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(4))
        .await
        .expect("pipeline run");
    assert!(status.success(), "CAST AS BIGINT in WHERE should run");

    #[derive(Debug, FromRow)]
    struct Row {
        id: i64,
        amount: String,
        whole: i64,
    }
    let rows: Vec<Row> = ctx
        .postgres
        .query("SELECT id, amount::text AS amount, whole FROM public.results ORDER BY id")
        .await
        .unwrap();
    let got: Vec<(i64, &str, i64)> = rows
        .iter()
        .map(|r| (r.id, r.amount.as_str(), r.whole))
        .collect();
    assert_eq!(
        got,
        vec![
            (2, "1000.000000000000000000", 1000),
            (3, "2500.500000000000000000", 2500),
            (4, "5000.750000000000000000", 5000),
            (7, "4999.000000000000000000", 4999),
        ],
        "only rows whose integral part is in [1000, 5000] pass"
    );
}

// ---------------------------------------------------------------------------
// (b) CAST(x AS NUMERIC) keeps the exact value
// ---------------------------------------------------------------------------

/// `decimal(78, 0)`, the 256-bit integer shape: 2^256 - 1 is written whole,
/// where DataFusion's default `Decimal128(38, 10)` for a bare NUMERIC would
/// fail past 28 integer digits.
#[tokio::test]
async fn cast_as_numeric_keeps_u256_max_exact() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC NOT NULL)")
        .await
        .unwrap();

    let max = u256_max().to_string();
    assert_eq!(max.len(), 78);
    let records = vec![
        (1, max.clone()),
        (2, "0".to_string()),
        (3, "12345678901234567890123456789".to_string()),
    ];
    produce(&ctx, 78, 0, &records).await;

    let yaml = sql_pipeline(
        &ctx.kafka_topic,
        "SELECT id, CAST(amount AS NUMERIC) AS t FROM amt_in",
    );
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(3))
        .await
        .expect("pipeline run");
    assert!(status.success(), "CAST AS NUMERIC should run");

    assert_eq!(
        result_texts(&ctx).await,
        vec![
            (1, max),
            (2, "0".to_string()),
            (3, "12345678901234567890123456789".to_string()),
        ]
    );
}

/// `decimal(100, 18)`: every fractional digit survives, where DataFusion's
/// default `Decimal128(38, 10)` for a bare NUMERIC would round to ten.
#[tokio::test]
async fn cast_as_numeric_keeps_scaled_values_exact() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC NOT NULL)")
        .await
        .unwrap();

    // 82 integer digits and 18 fractional digits: the full decimal(100, 18).
    let widest = format!("{}.{}", "9".repeat(82), "9".repeat(18));
    let values = [
        (1, "1.234567890123456789".to_string()),
        (2, "-0.000000000000000001".to_string()),
        (3, widest.clone()),
        (4, format!("{}.5", u256_max())),
    ];
    let records: Vec<(i64, String)> = values
        .iter()
        .map(|(id, v)| (*id, unscaled(v, 18)))
        .collect();
    produce(&ctx, 100, 18, &records).await;

    let yaml = sql_pipeline(
        &ctx.kafka_topic,
        "SELECT id, CAST(amount AS NUMERIC) AS t FROM amt_in",
    );
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(4))
        .await
        .expect("pipeline run");
    assert!(status.success(), "CAST AS NUMERIC should run");

    assert_eq!(
        result_texts(&ctx).await,
        vec![
            (1, "1.234567890123456789".to_string()),
            (2, "-0.000000000000000001".to_string()),
            (3, widest),
            (4, format!("{}.500000000000000000", u256_max())),
        ]
    );
}

// ---------------------------------------------------------------------------
// (c) TRY_CAST(x AS BIGINT) and CAST(x AS DOUBLE)
// ---------------------------------------------------------------------------

/// Past the BIGINT range TRY_CAST is NULL; DOUBLE is the nearest double,
/// including 2^53 + 1 (a tie, to even) and 2^256 - 1 (to 2^256).
#[tokio::test]
async fn try_cast_bigint_out_of_range_is_null_and_double_is_nearest() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute(
            "CREATE TABLE results (\
                 id BIGINT PRIMARY KEY, \
                 b BIGINT, \
                 f DOUBLE PRECISION NOT NULL\
             )",
        )
        .await
        .unwrap();

    let max = u256_max().to_string();
    let values: Vec<(i64, String)> = vec![
        (1, i64::MAX.to_string()),
        (2, "9223372036854775808".to_string()), // i64::MAX + 1
        (3, max.clone()),
        (4, "9007199254740993".to_string()), // 2^53 + 1
        (5, i64::MIN.to_string()),
        (6, "-9223372036854775809".to_string()), // i64::MIN - 1
        (7, "-42".to_string()),
    ];
    produce(&ctx, 80, 0, &values).await;

    let yaml = sql_pipeline(
        &ctx.kafka_topic,
        "SELECT id, TRY_CAST(amount AS BIGINT) AS b, CAST(amount AS DOUBLE) AS f FROM amt_in",
    );
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(values.len() as u64))
        .await
        .expect("pipeline run");
    assert!(
        status.success(),
        "TRY_CAST AS BIGINT / CAST AS DOUBLE should run"
    );

    #[derive(Debug, FromRow)]
    struct Row {
        id: i64,
        b: Option<i64>,
        f: f64,
    }
    let rows: Vec<Row> = ctx
        .postgres
        .query("SELECT id, b, f FROM public.results ORDER BY id")
        .await
        .unwrap();
    let got: Vec<(i64, Option<i64>)> = rows.iter().map(|r| (r.id, r.b)).collect();
    assert_eq!(
        got,
        vec![
            (1, Some(i64::MAX)),
            (2, None),
            (3, None),
            (4, Some(9_007_199_254_740_993)),
            (5, Some(i64::MIN)),
            (6, None),
            (7, Some(-42)),
        ],
        "TRY_CAST AS BIGINT is the value in range and NULL past it"
    );

    // Rust's float parsing is correctly rounded: the nearest double.
    for (row, (_, text)) in rows.iter().zip(&values) {
        let nearest: f64 = text.parse().unwrap();
        assert_eq!(
            row.f.to_bits(),
            nearest.to_bits(),
            "id={}: CAST({text} AS DOUBLE) = {} but the nearest double is {nearest}",
            row.id,
            row.f
        );
    }
    assert_eq!(rows[3].f, 9_007_199_254_740_992.0);
    assert_eq!(rows[2].f, 2f64.powi(256));
}

// ---------------------------------------------------------------------------
// (d) CAST(x AS DECIMAL(20, 2)) rounds half away from zero
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cast_as_decimal_20_2_rounds_half_away_from_zero() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC NOT NULL)")
        .await
        .unwrap();

    let cases = [
        (1, "1.005", "1.01"),
        (2, "-1.005", "-1.01"),
        (3, "0.125", "0.13"), // half-to-even would give 0.12
        (4, "-0.125", "-0.13"),
        (5, "2.0049", "2.00"),
        (6, "-2.0049", "-2.00"),
        (7, "999999999999999999.994", "999999999999999999.99"),
        (8, "7", "7.00"),
    ];
    // decimal(80, 4): the source keeps every digit that the cast drops.
    let records: Vec<(i64, String)> = cases
        .iter()
        .map(|(id, v, _)| (*id, unscaled(v, 4)))
        .collect();
    produce(&ctx, 80, 4, &records).await;

    let yaml = sql_pipeline(
        &ctx.kafka_topic,
        "SELECT id, CAST(amount AS DECIMAL(20, 2)) AS t FROM amt_in",
    );
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(cases.len() as u64))
        .await
        .expect("pipeline run");
    assert!(status.success(), "CAST AS DECIMAL(20, 2) should run");

    let expected: Vec<(i64, String)> = cases
        .iter()
        .map(|(id, _, rounded)| (*id, rounded.to_string()))
        .collect();
    assert_eq!(result_texts(&ctx).await, expected);
}
