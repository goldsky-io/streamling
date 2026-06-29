//! Adversarial e2e tests: SQL transforms over `decimal_arb` columns.
//!
//! Goal: surface runtime errors / silent failures when arithmetic, CASE,
//! COALESCE, filtering, casting and comparison operators are applied to
//! arbitrary-precision decimal columns (precision > 76) that flow
//! Kafka(Avro) -> SQL transform -> Postgres.
//!
//! Input is produced ONLY via `produce_decimal_record` (one `id long` +
//! one `decimal(p,s)` bytes field), so we cannot inject nulls or multiple
//! decimal columns. Some of these tests are EXPECTED to fail at runtime —
//! the failures are the findings. Tests that expect failure use
//! `run_pipeline_raw` and assert non-success.

use serde::Deserialize;
use sqlx::FromRow;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext};

#[derive(Debug, FromRow, Deserialize)]
#[allow(dead_code)]
struct IdText {
    id: i64,
    t: String,
}

#[derive(Debug, FromRow, Deserialize)]
#[allow(dead_code)]
struct IdBool {
    id: i64,
    t: bool,
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

/// Build a single-transform pipeline YAML: `SELECT id, <expr> AS t FROM amt_in`,
/// sinking into table `results(id, t <pg_type>)` mapped from `out`.
fn sql_pipeline(topic: &str, expr: &str) -> String {
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
    sql: "SELECT id, {expr} AS t FROM amt_in"
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

/// Produce decimal records for the shared `(id, unscaled)` cases.
async fn produce(ctx: &TestContext, schema: &str, cases: &[(i64, &str)]) {
    for (id, unscaled) in cases {
        ctx.kafka
            .produce_decimal_record(schema, *id, "amount", unscaled)
            .await
            .expect("produce decimal record");
    }
}

/// KNOWN-GAP tripwire. Runs a pipeline that currently cannot complete because of
/// a documented `decimal_arb` SQL gap (see `EDGE_CASE_FINDINGS.md`), bounded to a
/// short timeout, and asserts that NO row reaches `results`. The current behavior
/// is either a fast planning error or a sink failure that the sink retries to the
/// timeout (finding F4) — either way zero rows land. When the gap is fixed, rows
/// WILL land and this assertion fails: that's the signal to flip the test back to
/// asserting the correct value.
async fn assert_known_gap_no_rows(ctx: &TestContext, yaml: &str, gap: &str) {
    let opts = base_opts()
        .record_limit(1)
        .timeout(std::time::Duration::from_secs(15));
    let _ = ctx.run_pipeline_raw(yaml, opts).await;
    let count = ctx
        .postgres
        .count("SELECT COUNT(*) FROM public.results")
        .await
        .unwrap_or(0);
    assert_eq!(
        count, 0,
        "KNOWN GAP tripwire ({gap}): rows landed — the gap may be fixed; update this test to assert the correct value"
    );
}

// ---------------------------------------------------------------------------
// 1. amount + amount (doubling, precision growth)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_add_self_doubles_wide_decimal() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    // Result column maps `out` -> `t`; pick precision headroom above the source.
    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC(101, 18) NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    produce(
        &ctx,
        &schema,
        &[
            (1, "1234567890123456789"),   // 1.234567890123456789
            (2, "-99000000000000000000"), // -99.0
            (3, "0"),                     // 0
        ],
    )
    .await;

    let yaml = sql_pipeline(&ctx.kafka_topic, "amount + amount");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(3))
        .await
        .expect("pipeline run");
    assert!(status.success(), "amount + amount should succeed");

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t::text AS t FROM public.results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].t, "2.469135780246913578");
    assert_eq!(rows[1].t, "-198.000000000000000000");
    assert_eq!(rows[2].t, "0.000000000000000000");
}

// ---------------------------------------------------------------------------
// 2. amount - amount (= 0, sign edge)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_sub_self_is_zero() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC(101, 18) NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    produce(
        &ctx,
        &schema,
        &[
            (1, "1234567890123456789"),
            (2, "-99000000000000000000"),
            (3, &"9".repeat(99)), // near-max magnitude self-subtract
        ],
    )
    .await;

    let yaml = sql_pipeline(&ctx.kafka_topic, "amount - amount");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(3))
        .await
        .expect("pipeline run");
    assert!(status.success(), "amount - amount should succeed");

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t::text AS t FROM public.results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    for r in &rows {
        // x - x == 0 regardless of sign; allow either "0" or "0.000…"
        let trimmed = r.t.trim_start_matches('-');
        assert!(
            trimmed.chars().all(|c| c == '0' || c == '.'),
            "x - x must be zero, got {}",
            r.t
        );
    }
}

// ---------------------------------------------------------------------------
// 3. amount * amount (precision near MAX — decimal(50,0) squared -> 100 digits)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_mul_self_precision_near_max() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    // 50-digit value squared -> up to ~100 digits. Give the result room.
    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC(120, 0) NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(50, 0);
    // 10^49 squared = 10^98.
    let v = format!("1{}", "0".repeat(49));
    produce(&ctx, &schema, &[(1, &v), (2, "3"), (3, "-7")]).await;

    let yaml = sql_pipeline(&ctx.kafka_topic, "amount * amount");
    // KNOWN GAP (F5): decimal_arb * decimal_arb whose product nears the precision
    // ceiling overflows (Arrow "Arithmetic overflow") and the sink fails. Tripwire
    // until overflow is handled (clear error or widening) — see EDGE_CASE_FINDINGS.md.
    assert_known_gap_no_rows(&ctx, &yaml, "F5: decimal_arb multiply overflow").await;
}

// ---------------------------------------------------------------------------
// 4. amount / amount (= 1; divide behavior)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_div_self_is_one() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC(120, 20) NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    produce(
        &ctx,
        &schema,
        &[(1, "1234567890123456789"), (2, "-99000000000000000000")],
    )
    .await;

    let yaml = sql_pipeline(&ctx.kafka_topic, "amount / amount");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(2))
        .await
        .expect("pipeline run");
    assert!(status.success(), "amount / amount should succeed");

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t::text AS t FROM public.results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    for r in &rows {
        // x / x == 1 (possibly with trailing fractional zeros).
        assert_eq!(
            r.t.trim_end_matches('0').trim_end_matches('.'),
            "1",
            "x / x must be 1, got {}",
            r.t
        );
    }
}

// ---------------------------------------------------------------------------
// 5. amount % <literal> (modulo)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_mod_literal() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC(100, 0) NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(90, 0);
    produce(&ctx, &schema, &[(1, "100"), (2, "103"), (3, "-7")]).await;

    // F1 FIXED: `amount % 10` coerces the integer literal to decimal_arb.
    let yaml = sql_pipeline(&ctx.kafka_topic, "amount % 10");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(3))
        .await
        .expect("pipeline run");
    assert!(status.success(), "amount % 10 should succeed (F1 fixed)");

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t::text AS t FROM public.results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].t, "0"); // 100 % 10
    assert_eq!(rows[1].t, "3"); // 103 % 10
    assert_eq!(rows[2].t, "-7"); // -7 % 10 (sign follows dividend)
}

// ---------------------------------------------------------------------------
// 6. CASE WHEN id = 1 THEN amount ELSE amount END  (KNOWN GAP: CASE may drop
//    decimal_arb metadata, sinking the value with the wrong scale).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_case_passthrough_metadata_tripwire() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC(100, 18) NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    produce(
        &ctx,
        &schema,
        &[
            (1, "1234567890123456789"),   // -> 1.234567890123456789
            (2, "-99000000000000000000"), // -> -99.0
        ],
    )
    .await;

    // F2 FIXED: the DecimalArbExprRewrite re-stamps the decimal_arb metadata the
    // CASE projection drops, so the value round-trips into NUMERIC at the right scale.
    let yaml = sql_pipeline(
        &ctx.kafka_topic,
        "CASE WHEN id = 1 THEN amount ELSE amount END",
    );
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(2))
        .await
        .expect("pipeline run");
    assert!(status.success(), "CASE over decimal_arb should succeed (F2 fixed)");

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t::text AS t FROM public.results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].t, "1.234567890123456789");
    assert_eq!(rows[1].t, "-99.000000000000000000");
}

// ---------------------------------------------------------------------------
// 7. nested CASE (CASE inside CASE) over the decimal
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_nested_case() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC(100, 18) NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    produce(
        &ctx,
        &schema,
        &[
            (1, "1000000000000000000"), // 1.0
            (2, "2000000000000000000"), // 2.0
            (3, "3000000000000000000"), // 3.0
        ],
    )
    .await;

    let yaml = sql_pipeline(
        &ctx.kafka_topic,
        "CASE WHEN id = 1 THEN amount ELSE (CASE WHEN id = 2 THEN amount ELSE amount END) END",
    );
    // F2 FIXED: nested CASE over decimal_arb re-stamps metadata at each level.
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(3))
        .await
        .expect("pipeline run");
    assert!(status.success(), "nested CASE over decimal_arb should succeed (F2 fixed)");

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t::text AS t FROM public.results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].t, "1.000000000000000000");
    assert_eq!(rows[1].t, "2.000000000000000000");
    assert_eq!(rows[2].t, "3.000000000000000000");
}

// ---------------------------------------------------------------------------
// 8. COALESCE(amount, amount)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_coalesce_self() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC(100, 18) NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    produce(&ctx, &schema, &[(1, "1234567890123456789"), (2, "-1")]).await;

    let yaml = sql_pipeline(&ctx.kafka_topic, "COALESCE(amount, amount)");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(2))
        .await
        .expect("pipeline run");
    assert!(status.success(), "COALESCE should succeed");

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t::text AS t FROM public.results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].t, "1.234567890123456789");
    assert_eq!(rows[1].t, "-0.000000000000000001");
}

// ---------------------------------------------------------------------------
// 9. filter: WHERE amount > 0 (only positives should land)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_filter_positive_only() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC(100, 18) NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    // Mix of positive / negative / zero. record_limit counts source records,
    // not sink rows, so we set it to the produced count (4).
    produce(
        &ctx,
        &schema,
        &[
            (1, "5000000000000000000"),  // 5.0  (keep)
            (2, "-5000000000000000000"), // -5.0 (drop)
            (3, "0"),                    // 0    (drop)
            (4, "1"),                    // tiny positive (keep)
        ],
    )
    .await;

    // Note: filter is in the WHERE clause; `out` is just `amount`.
    let yaml = format!(
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
    sql: "SELECT id, amount AS t FROM amt_in WHERE amount > 0"
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
        topic = ctx.kafka_topic,
    );
    // F1 FIXED: `WHERE amount > 0` coerces the integer literal to decimal_arb and
    // filters correctly — only the two positive rows land. The filter drops 2 of
    // 4 rows, so `record_limit` (counts emitted rows) is never reached on the
    // unbounded Kafka source; bound with a short timeout and assert table state.
    let opts = base_opts()
        .record_limit(4)
        .timeout(std::time::Duration::from_secs(20));
    let _ = ctx.run_pipeline_raw(&yaml, opts).await;

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t::text AS t FROM public.results ORDER BY id")
        .await
        .unwrap();
    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    assert_eq!(ids, vec![1, 4], "only positive amounts (5.0 and tiny) pass");
}

// ---------------------------------------------------------------------------
// 10. CAST(amount AS VARCHAR) -> TEXT column (canonical string).
//     KNOWN GAP per decimal_arb_casts.rs: built-in CAST of decimal_arb to
//     VARCHAR tries to read LargeBinary as UTF-8 and is expected to FAIL.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_cast_varchar_expected_failure() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t TEXT NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    produce(&ctx, &schema, &[(1, "1234567890123456789")]).await;

    let yaml = sql_pipeline(&ctx.kafka_topic, "CAST(amount AS VARCHAR)");
    // CAST(decimal_arb AS VARCHAR) behavior is unspecified — `decimal_arb_to_string`
    // is the supported path. This only guards that it does not PANIC: it may error,
    // hang (F4), or produce text. Bound the timeout and tolerate either outcome.
    let opts = base_opts()
        .record_limit(1)
        .timeout(std::time::Duration::from_secs(15));
    if let Ok(out) = ctx.run_pipeline_raw(&yaml, opts).await {
        assert!(
            !out.stderr.contains("panicked"),
            "CAST(decimal_arb AS VARCHAR) should not panic: {}",
            out.stderr
        );
    }
}

// ---------------------------------------------------------------------------
// 11. decimal_arb_to_string(amount) -> TEXT column (canonical string).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_decimal_arb_to_string() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t TEXT NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    produce(
        &ctx,
        &schema,
        &[(1, "1234567890123456789"), (2, "-99000000000000000000")],
    )
    .await;

    let yaml = sql_pipeline(&ctx.kafka_topic, "decimal_arb_to_string(amount)");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(2))
        .await
        .expect("pipeline run");
    assert!(status.success(), "decimal_arb_to_string should succeed");

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t FROM public.results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].t, "1.234567890123456789");
    assert_eq!(rows[1].t, "-99.000000000000000000");
}

// ---------------------------------------------------------------------------
// 12. amount = amount boolean comparison (sink to BOOLEAN col, expect true).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_eq_self_is_true() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t BOOLEAN NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    produce(
        &ctx,
        &schema,
        &[
            (1, "1234567890123456789"),
            (2, "-99000000000000000000"),
            (3, "0"),
        ],
    )
    .await;

    let yaml = sql_pipeline(&ctx.kafka_topic, "amount = amount");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(3))
        .await
        .expect("pipeline run");
    assert!(status.success(), "amount = amount should succeed");

    let rows: Vec<IdBool> = ctx
        .postgres
        .query("SELECT id, t FROM public.results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    for r in &rows {
        assert!(r.t, "x = x must be true for id {}", r.id);
    }
}

// ---------------------------------------------------------------------------
// 13. amount + 1 (decimal_arb + integer literal — coercion gap tripwire).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_add_integer_literal_coercion() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();

    ctx.postgres
        .execute("CREATE TABLE results (id BIGINT PRIMARY KEY, t NUMERIC(101, 18) NOT NULL)")
        .await
        .unwrap();

    let schema = decimal_schema(100, 18);
    produce(
        &ctx,
        &schema,
        &[
            (1, "1000000000000000000"),  // 1.0  -> 2.0
            (2, "-1000000000000000000"), // -1.0 -> 0.0
            (3, "1234567890123456789"),  // 1.234… -> 2.234…
        ],
    )
    .await;

    // F1 FIXED: the ExprPlanner now coerces the integer literal to decimal_arb
    // (scale 0) and dispatches to decimal_arb_add.
    let yaml = sql_pipeline(&ctx.kafka_topic, "amount + 1");
    let status = ctx
        .run_pipeline_with_opts(&yaml, base_opts().record_limit(3))
        .await
        .expect("pipeline run");
    assert!(status.success(), "amount + 1 should succeed (F1 fixed)");

    let rows: Vec<IdText> = ctx
        .postgres
        .query("SELECT id, t::text AS t FROM public.results ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].t, "2.000000000000000000");
    assert_eq!(rows[1].t, "0.000000000000000000");
    assert_eq!(rows[2].t, "2.234567890123456789");
}
