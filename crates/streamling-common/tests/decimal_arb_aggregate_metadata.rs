//! Regression coverage for the aggregate-output cluster: `SUM`/`AVG`/`MIN`/`MAX`
//! over decimal_arb used to return a bare `LargeBinary` with no `(precision,
//! scale)` metadata, so everything downstream fell back to byte semantics —
//! hex in JSON, bytewise `ORDER BY` / `HAVING`, and bytewise nested extremes.
//! `SUM(DISTINCT)` / `AVG(DISTINCT)` also ignored `DISTINCT` whenever another
//! aggregate stopped DataFusion from rewriting them into a GROUP BY.

use arrow::array::{Array, BooleanArray, Int64Array, LargeBinaryArray, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::{FunctionRegistry, SessionStateBuilder, SessionStateDefaults};
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::str::FromStr;
use std::sync::Arc;
use streamling_common::formats::FromArrowConverter;
use streamling_common::formats::json::FromArrowToJsonConverter;
use streamling_common::functions::CommonFunctions;
use streamling_common::functions::decimal_arb_aggregates::{
    DecimalArbArrayAggUdaf, DecimalArbAvgUdaf, DecimalArbExtremeUdaf, DecimalArbSumUdaf,
};
use streamling_common::functions::decimal_arb_coercion::DecimalArbExprPlanner;
use streamling_common::functions::decimal_arb_predicate_optimizer::DecimalArbExprRewrite;
use streamling_common::functions::decimal_arb_scale_unify::DecimalArbScaleUnifyRule;
use streamling_common::functions::decimal_arb_sort_optimizer::DecimalArbSortRewriteRule;
use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};

fn decimal_arb_planners() -> Vec<Arc<dyn ExprPlanner>> {
    let mut planners: Vec<Arc<dyn ExprPlanner>> = vec![Arc::new(DecimalArbExprPlanner::new())];
    planners.extend(SessionStateDefaults::default_expr_planners());
    planners
}

fn session() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(4))
        .with_default_features()
        // Our planner must run ahead of DataFusion's for array literals.
        .with_expr_planners(decimal_arb_planners())
        .with_optimizer_rule(Arc::new(DecimalArbSortRewriteRule::new()))
        .with_optimizer_rule(Arc::new(DecimalArbScaleUnifyRule::new()))
        .build();
    let mut ctx = SessionContext::new_with_state(state);
    for udf in CommonFunctions::functions() {
        ctx.register_udf(udf);
    }
    ctx.register_udaf(DecimalArbSumUdaf::into_udaf());
    ctx.register_udaf(DecimalArbAvgUdaf::into_udaf());
    ctx.register_udaf(DecimalArbExtremeUdaf::min_udaf());
    ctx.register_udaf(DecimalArbExtremeUdaf::max_udaf());
    ctx.register_udaf(DecimalArbArrayAggUdaf::into_udaf());
    ctx.register_function_rewrite(Arc::new(DecimalArbExprRewrite::new()))
        .unwrap();
    ctx
}

/// `t(k, v)` with `v` decimal_arb(110, 2), split into several batches (so
/// partial → final merge paths run under 4 target partitions).
fn table(ctx: &SessionContext, rows: &[(i64, Option<&str>)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        DecimalArbType::field("v", 110, 2, true).unwrap(),
    ]));
    let mut batches = Vec::new();
    for chunk in rows.chunks(2) {
        let mut b = DecimalArbArrayBuilder::with_capacity(chunk.len(), "v", 110, 2).unwrap();
        for (_, v) in chunk {
            match v {
                Some(v) => b.append_str(v).unwrap(),
                None => b.append_null(),
            }
        }
        let ks = Int64Array::from_iter_values(chunk.iter().map(|(k, _)| *k));
        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(ks), Arc::new(b.finish().into_inner().0)],
            )
            .unwrap(),
        );
    }
    let mem = MemTable::try_new(schema, batches.into_iter().map(|b| vec![b]).collect()).unwrap();
    ctx.register_table("t", Arc::new(mem)).unwrap();
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

fn d(s: &str) -> Option<DecimalArbValue> {
    Some(DecimalArbValue::from_str(s).unwrap())
}

/// Column `col` decoded at the scale its OUTPUT FIELD declares — what a sink
/// does. Panics if the field has no decimal_arb metadata: that is the bug.
fn decimals(batches: &[RecordBatch], col: usize) -> Vec<Option<DecimalArbValue>> {
    let mut out = Vec::new();
    for b in batches {
        let (_, scale) = DecimalArbType::precision_scale_from_field(b.schema().field(col))
            .expect("aggregate output must carry decimal_arb metadata");
        let a = b
            .column(col)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        for i in 0..a.len() {
            out.push(if a.is_null(i) {
                None
            } else {
                Some(DecimalArbValue::from_canonical_bytes_at_scale(a.value(i), scale).unwrap())
            });
        }
    }
    out
}

fn ints(batches: &[RecordBatch], col: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

const FIVE_GROUPS: &[(i64, Option<&str>)] = &[
    (1, Some("-1.00")),
    (1, Some("-3.00")),
    (2, Some("2.00")),
    (3, Some("0.50")),
    (4, Some("-0.50")),
    (5, Some("100.00")),
    (5, None),
];

#[tokio::test]
async fn aggregate_outputs_carry_precision_and_scale() {
    let ctx = session();
    table(&ctx, &[(1, Some("1.25")), (1, Some("2.50"))]);
    let b = query(
        &ctx,
        "SELECT SUM(v) AS s, AVG(v) AS a, MIN(v) AS mi, MAX(v) AS ma FROM t",
    )
    .await;
    let meta = |i: usize| DecimalArbType::precision_scale_from_field(b[0].schema().field(i));
    assert_eq!(meta(0), Some((126, 2)), "SUM widens precision by 16");
    assert_eq!(meta(1), Some((111, 3)), "AVG widens both by 1");
    assert_eq!(meta(2), Some((110, 2)));
    assert_eq!(meta(3), Some((110, 2)));
    assert_eq!(decimals(&b, 0), vec![d("3.75")]);
    assert_eq!(decimals(&b, 1), vec![d("1.875")]);
    assert_eq!(decimals(&b, 2), vec![d("1.25")]);
    assert_eq!(decimals(&b, 3), vec![d("2.50")]);
}

#[tokio::test]
async fn order_by_sum_is_numeric() {
    let ctx = session();
    table(&ctx, FIVE_GROUPS);
    let b = query(&ctx, "SELECT k FROM t GROUP BY k ORDER BY SUM(v)").await;
    // sums: k1=-4.00, k2=2.00, k3=0.50, k4=-0.50, k5=100.00
    assert_eq!(ints(&b, 0), vec![1, 4, 3, 2, 5]);
    let b = query(&ctx, "SELECT k FROM t GROUP BY k ORDER BY MAX(v) DESC").await;
    assert_eq!(ints(&b, 0), vec![5, 2, 3, 4, 1]);
}

#[tokio::test]
async fn having_over_sum_compares_numerically() {
    let ctx = session();
    table(&ctx, FIVE_GROUPS);
    let b = query(
        &ctx,
        "SELECT k FROM t GROUP BY k HAVING SUM(v) > to_decimal_arb_from_string('0', 110, 2) ORDER BY k",
    )
    .await;
    assert_eq!(ints(&b, 0), vec![2, 3, 5]);
    let b = query(
        &ctx,
        "SELECT k FROM t GROUP BY k HAVING SUM(v) > 0 ORDER BY k",
    )
    .await;
    assert_eq!(ints(&b, 0), vec![2, 3, 5]);
    let b = query(
        &ctx,
        "SELECT k FROM t GROUP BY k HAVING SUM(v) < '0' ORDER BY k",
    )
    .await;
    assert_eq!(ints(&b, 0), vec![1, 4]);
}

#[tokio::test]
async fn avg_equals_max_for_a_constant_group() {
    let ctx = session();
    table(
        &ctx,
        &[(1, Some("5.00")), (1, Some("5.00")), (1, Some("5.00"))],
    );
    let b = query(&ctx, "SELECT AVG(v) = MAX(v), MIN(v) = MAX(v) FROM t").await;
    let col = |i: usize| {
        b[0].column(i)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    };
    assert!(col(0), "5.000 (scale 3) must equal 5.00 (scale 2)");
    assert!(col(1));
}

#[tokio::test]
async fn sum_and_avg_distinct_honour_distinct_next_to_other_aggregates() {
    let ctx = session();
    table(
        &ctx,
        &[
            (1, Some("1.25")),
            (1, Some("1.25")),
            (1, Some("2.50")),
            (1, Some("1.25")),
            (1, None),
        ],
    );
    // COUNT(*) alongside stops the SingleDistinctToGroupBy rewrite, so the
    // accumulator itself must dedupe — across 4 partitions.
    let b = query(
        &ctx,
        "SELECT SUM(DISTINCT v), COUNT(*), AVG(DISTINCT v) FROM t",
    )
    .await;
    assert_eq!(decimals(&b, 0), vec![d("3.75")]);
    assert_eq!(ints(&b, 1), vec![5]);
    assert_eq!(decimals(&b, 2), vec![d("1.875")]);
    // …and the plain aggregates still count duplicates.
    let b = query(&ctx, "SELECT SUM(v), AVG(v) FROM t").await;
    assert_eq!(decimals(&b, 0), vec![d("6.25")]);
    // AVG widens the scale by exactly one: 6.25 / 4 = 1.5625 → 1.562 half-even.
    assert_eq!(decimals(&b, 1), vec![d("1.562")]);
}

#[tokio::test]
async fn min_max_next_to_count_distinct_are_numeric() {
    let ctx = session();
    table(
        &ctx,
        &[
            (1, Some("-1.00")),
            (1, Some("-2.00")),
            (1, Some("-3.00")),
            (2, Some("0.50")),
            (2, Some("100.00")),
            (2, Some("-0.25")),
        ],
    );
    let b = query(
        &ctx,
        "SELECT k, MIN(v), MAX(v), COUNT(DISTINCT v) FROM t GROUP BY k ORDER BY k",
    )
    .await;
    assert_eq!(decimals(&b, 1), vec![d("-3.00"), d("-0.25")]);
    assert_eq!(decimals(&b, 2), vec![d("-1.00"), d("100.00")]);
    assert_eq!(ints(&b, 3), vec![3, 3]);
}

#[tokio::test]
async fn nested_extremes_and_sums_over_group_results() {
    let ctx = session();
    table(&ctx, FIVE_GROUPS);
    let b = query(
        &ctx,
        "SELECT MAX(s), MIN(s), SUM(s) FROM (SELECT k, MAX(v) AS s FROM t GROUP BY k)",
    )
    .await;
    // group maxes: -1.00, 2.00, 0.50, -0.50, 100.00
    assert_eq!(decimals(&b, 0), vec![d("100.00")]);
    assert_eq!(decimals(&b, 1), vec![d("-1.00")]);
    assert_eq!(decimals(&b, 2), vec![d("101.00")]);
    let b = query(
        &ctx,
        "SELECT SUM(s) FROM (SELECT k, SUM(v) AS s FROM t GROUP BY k)",
    )
    .await;
    assert_eq!(decimals(&b, 0), vec![d("98.00")]);
}

#[tokio::test]
async fn sum_result_prints_and_serialises_as_a_number() {
    let ctx = session();
    table(&ctx, &[(1, Some("1.25")), (1, Some("2.50"))]);
    let b = query(&ctx, "SELECT decimal_arb_to_string(SUM(v)) AS s FROM t").await;
    let s = b[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(DecimalArbValue::from_str(s).unwrap(), d("3.75").unwrap());

    let b = query(&ctx, "SELECT SUM(v) AS s FROM t").await;
    let rows = FromArrowToJsonConverter::new()
        .convert_from_batch(&b[0])
        .unwrap();
    let json = String::from_utf8(rows[0].clone()).unwrap();
    assert_eq!(
        json.trim(),
        r#"{"s":"3.75"}"#,
        "JSON must carry the number, not hex bytes"
    );
}
