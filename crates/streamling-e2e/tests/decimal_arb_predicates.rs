//! Adversarial e2e tests: runnable scalar SQL predicates over decimal_arb that
//! weren't previously covered — BETWEEN, IN, IS [NOT] NULL. (No JOIN / window /
//! bare aggregates — streamling's streaming transforms can't run those.)
//!
//! Input is a single `amount decimal(100,18)` column via the proven
//! `produce_decimal_record`. Self-referential predicates (`amount BETWEEN amount
//! AND amount`) exercise BETWEEN/IN over decimal_arb (byte comparison, no
//! coercion needed). A literal-bounded BETWEEN pins F1b — `Between`/`InList`
//! bypass the binary-op ExprPlanner, so literal bounds aren't coerced (distinct
//! from the now-fixed binary-op literal coercion, F1).

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

fn decimal_schema(precision: u32, scale: u32) -> String {
    format!(
        r#"{{
            "type": "record", "name": "Amt",
            "fields": [
                {{"name": "id", "type": "long"}},
                {{"name": "amount", "type": {{"type": "bytes", "logicalType": "decimal", "precision": {precision}, "scale": {scale}}}}}
            ]
        }}"#
    )
}

#[derive(Debug, FromRow, Deserialize)]
struct IdRow {
    id: i64,
}

/// Produce `(id, amount)` decimal rows, run `SELECT id FROM src WHERE <pred>`,
/// and return the ids that landed. Bounded timeout, tolerant of a never-reached
/// record_limit (unbounded Kafka source + filtering predicate).
async fn ids_passing(ctx: &TestContext, pred: &str, cases: &[(i64, &str)]) -> Vec<i64> {
    ctx.postgres
        .execute("CREATE TABLE r (id BIGINT PRIMARY KEY, amount NUMERIC(100,18) NOT NULL)")
        .await
        .unwrap();
    let schema = decimal_schema(100, 18);
    for (id, unscaled) in cases {
        ctx.kafka
            .produce_decimal_record(&schema, *id, "amount", unscaled)
            .await
            .unwrap();
    }
    // Project `amount` too (the proven decimal_arb transform shape) so the sink
    // has a payload column beyond the primary key.
    let yaml = format!(
        r#"
sources:
  src:
    type: kafka
    topic: {input}
    starting_offsets: earliest
    primary_key: id
transforms:
  t:
    type: sql
    sql: "SELECT id, amount AS amount FROM src WHERE {pred}"
    primary_key: id
sinks:
  out:
    type: postgres
    from: t
    table: r
    schema: public
    primary_key: id
    on_conflict: update
"#,
        input = ctx.kafka_topic,
    );
    let opts = base_opts()
        .record_limit(cases.len() as u64)
        .timeout(std::time::Duration::from_secs(20));
    let _ = ctx.run_pipeline_raw(&yaml, opts).await;
    let rows: Vec<IdRow> = ctx
        .postgres
        .query("SELECT id FROM public.r ORDER BY id")
        .await
        .unwrap();
    rows.into_iter().map(|r| r.id).collect()
}

/// `amount BETWEEN amount AND amount` — desugars to `amount >= amount AND amount
/// <= amount`, both operands decimal_arb (no literal). Every row passes.
#[tokio::test]
async fn between_self_decimal_arb() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let ids = ids_passing(
        &ctx,
        "amount BETWEEN amount AND amount",
        &[
            (1, "5000000000000000000"),
            (2, "-3000000000000000000"),
            (3, "0"),
        ],
    )
    .await;
    assert_eq!(
        ids,
        vec![1, 2, 3],
        "BETWEEN over decimal_arb (self bounds) must pass all rows"
    );
}

/// `amount IN (amount)` — desugars to `amount = amount`, both decimal_arb.
#[tokio::test]
async fn in_self_decimal_arb() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let ids = ids_passing(
        &ctx,
        "amount IN (amount)",
        &[(1, "5000000000000000000"), (2, "-3000000000000000000")],
    )
    .await;
    assert_eq!(
        ids,
        vec![1, 2],
        "IN over decimal_arb (self) must pass all rows"
    );
}

/// `amount IS NOT NULL` on a non-nullable decimal_arb — every row passes.
#[tokio::test]
async fn is_not_null_decimal_arb() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let ids = ids_passing(&ctx, "amount IS NOT NULL", &[(1, "1"), (2, "2")]).await;
    assert_eq!(
        ids,
        vec![1, 2],
        "IS NOT NULL must pass every non-null decimal_arb"
    );
}

/// `amount IS NULL` on a non-nullable decimal_arb — no row passes.
#[tokio::test]
async fn is_null_decimal_arb_none() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let ids = ids_passing(&ctx, "amount IS NULL", &[(1, "1"), (2, "2")]).await;
    assert!(
        ids.is_empty(),
        "IS NULL must drop every non-null decimal_arb, got {ids:?}"
    );
}

/// `amount BETWEEN 0 AND 100` — integer literal bounds. F1b FIXED: a
/// `FunctionRewrite` (`DecimalArbPredicateRewrite`) desugars decimal_arb
/// `Between`/`InList` into the decimal_arb comparison UDFs *before* TypeCoercion,
/// coercing the integer bounds. amount is decimal(100,18): 5.0 is in range,
/// -3.0 is below 0, 200 is above 100.
#[tokio::test]
async fn between_int_literals() {
    init_tracing();
    let ctx = TestContext::new().await.unwrap();
    let ids = ids_passing(
        &ctx,
        "amount BETWEEN 0 AND 100",
        &[
            (1, "5000000000000000000"),   // 5.0  -> in range
            (2, "-3000000000000000000"),  // -3.0 -> below 0
            (3, "200000000000000000000"), // 200  -> above 100
        ],
    )
    .await;
    assert_eq!(
        ids,
        vec![1],
        "only the in-range value passes BETWEEN 0 AND 100 (F1b fixed); got {ids:?}"
    );
}
