//! Adversarial SQL review tests for PR 37; production is unchanged.
use arrow::array::{BooleanArray, Int64Array, LargeBinaryArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::{FunctionRegistry, SessionStateBuilder};
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use streamling_common::functions::CommonFunctions;
use streamling_common::functions::decimal_arb_aggregates::{
    DecimalArbAvgUdaf, DecimalArbExtremeUdaf, DecimalArbSumUdaf,
};
use streamling_common::functions::decimal_arb_coercion::DecimalArbExprPlanner;
use streamling_common::functions::decimal_arb_predicate_optimizer::DecimalArbExprRewrite;
use streamling_common::functions::decimal_arb_sort_optimizer::DecimalArbSortRewriteRule;
use streamling_common::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue,
};

fn session() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_optimizer_rule(Arc::new(DecimalArbSortRewriteRule::new()))
        .build();
    let mut ctx = SessionContext::new_with_state(state);
    for udf in CommonFunctions::functions() {
        ctx.register_udf(udf);
    }
    ctx.register_udaf(DecimalArbSumUdaf::into_udaf());
    ctx.register_udaf(DecimalArbAvgUdaf::into_udaf());
    ctx.register_udaf(DecimalArbExtremeUdaf::min_udaf());
    ctx.register_udaf(DecimalArbExtremeUdaf::max_udaf());
    ctx.register_expr_planner(Arc::new(DecimalArbExprPlanner::new()))
        .unwrap();
    ctx.register_function_rewrite(Arc::new(DecimalArbExprRewrite::new()))
        .unwrap();
    ctx
}

fn table(ctx: &SessionContext, name: &str, scale: u32, vals: &[&str]) {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), "v", 110, scale).unwrap();
    for val in vals {
        b.append_str(val).unwrap();
    }
    let (array, _, _) = b.finish().into_inner();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("v", 110, scale, false).unwrap(),
    ]));
    let ids = Int64Array::from_iter_values(1..=vals.len() as i64);
    ctx.register_batch(
        name,
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(array)]).unwrap(),
    )
    .unwrap();
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let df = ctx.sql(sql).await.unwrap();
    eprintln!(
        "SQL {sql}\nPLAN {}",
        df.clone().into_optimized_plan().unwrap().display_indent()
    );
    let batches = df.collect().await.unwrap();
    eprintln!("BATCHES {batches:?}");
    batches
}

fn decimal(batch: &RecordBatch, col: usize, row: usize, scale: u32) -> String {
    let a = batch
        .column(col)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    DecimalArbValue::from_canonical_bytes_at_scale(a.value(row), scale)
        .unwrap()
        .as_bigdecimal()
        .normalized()
        .to_string()
}

#[tokio::test]
#[ignore = "Secondary gap: normal streaming transforms reject Aggregate plans"]
async fn aggregate_outputs_keep_scale() {
    let ctx = session();
    table(&ctx, "t", 2, &["1.25", "2.50"]);
    let batches = query(
        &ctx,
        "SELECT SUM(v) AS s, AVG(v) AS a, MIN(v) AS mi, MAX(v) AS ma FROM t",
    )
    .await;
    let actual: Vec<_> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| DecimalArbType::precision_scale_from_field(f))
        .collect();
    assert_eq!(
        actual,
        vec![
            Some((126, 2)),
            Some((111, 3)),
            Some((110, 2)),
            Some((110, 2))
        ]
    );
}

#[tokio::test]
async fn sum_distinct_honors_duplicates() {
    let ctx = session();
    table(&ctx, "t", 2, &["1.25", "1.25", "2.50"]);
    let batches = query(&ctx, "SELECT SUM(DISTINCT v) FROM t").await;
    assert_eq!(decimal(&batches[0], 0, 0, 2), "3.75");
}

#[tokio::test]
async fn avg_distinct_honors_duplicates() {
    let ctx = session();
    table(&ctx, "t", 2, &["1.25", "1.25", "2.50"]);
    let batches = query(&ctx, "SELECT AVG(DISTINCT v) FROM t").await;
    assert_eq!(decimal(&batches[0], 0, 0, 3), "1.875");
}

#[tokio::test]
async fn union_all_preserves_different_scales() {
    let ctx = session();
    table(&ctx, "a", 0, &["1"]);
    table(&ctx, "b", 2, &["1"]);
    let batches = query(&ctx, "SELECT v FROM a UNION ALL SELECT v FROM b").await;
    let vals: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            let scale = DecimalArbType::precision_scale_from_field(b.schema().field(0))
                .expect("union decimal metadata")
                .1;
            (0..b.num_rows()).map(move |row| decimal(b, 0, row, scale))
        })
        .collect();
    assert_eq!(vals, vec!["1", "1"]);
}

#[tokio::test]
async fn is_not_distinct_matches_decimal_equality() {
    let ctx = session();
    table(&ctx, "t", 0, &["1", "2"]);
    let batches = query(&ctx, "SELECT v = to_decimal_arb_from_string('1', 110, 2) AS eq, v IS NOT DISTINCT FROM to_decimal_arb_from_string('1', 110, 2) AS ndeq FROM t ORDER BY id").await;
    let eq = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let ndeq = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(eq, ndeq);
}

#[tokio::test]
async fn simple_case_compares_different_scales() {
    let ctx = session();
    table(&ctx, "t", 0, &["1"]);
    let batches = query(
        &ctx,
        "SELECT CASE v WHEN to_decimal_arb_from_string('1', 110, 2) THEN 10 ELSE 20 END FROM t",
    )
    .await;
    let actual = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(actual, 10);
}

#[tokio::test]
#[ignore = "Secondary gap: normal streaming transforms reject WindowAggr plans"]
async fn window_order_uses_numeric_order() {
    let ctx = session();
    table(&ctx, "t", 2, &["-1", "0", "2"]);
    let batches = query(&ctx, "SELECT id, ROW_NUMBER() OVER (ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn FROM t ORDER BY id").await;
    let a = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    assert_eq!(a.values().as_ref(), &[1, 2, 3]);
}

#[tokio::test]
async fn greatest_uses_numeric_order() {
    let ctx = session();
    table(&ctx, "t", 2, &["-1", "2"]);
    let batches = query(
        &ctx,
        "SELECT greatest(v, to_decimal_arb_from_string('0', 110, 2)) AS v FROM t ORDER BY id",
    )
    .await;
    assert_eq!(decimal(&batches[0], 0, 0, 2), "0");
}

#[tokio::test]
#[ignore = "Secondary gap: explicit metadata failure, excluded from confirmed silent failures"]
async fn case_of_mixed_scales_preserves_value() {
    let ctx = session();
    table(&ctx, "t", 0, &["1", "2"]);
    let batches = query(&ctx, "SELECT CASE WHEN id=1 THEN v ELSE to_decimal_arb_from_string('1',110,2) END AS v FROM t ORDER BY id").await;
    let s = DecimalArbType::precision_scale_from_field(batches[0].schema().field(0))
        .expect("CASE decimal metadata")
        .1;
    assert_eq!(decimal(&batches[0], 0, 0, s), "1");
    assert_eq!(decimal(&batches[0], 0, 1, s), "1");
}

#[tokio::test]
async fn binary_comparison_after_case_uses_numeric_order() {
    let ctx = session();
    table(&ctx, "t", 2, &["-1", "2"]);
    let batches = query(&ctx, "SELECT (CASE WHEN id=1 THEN v ELSE to_decimal_arb_from_string('2',110,2) END) < to_decimal_arb_from_string('0',110,2) AS less FROM t").await;
    let b = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(b.values().iter().collect::<Vec<_>>(), vec![true, false]);
}

#[tokio::test]
async fn binary_comparison_after_coalesce_uses_numeric_order() {
    let ctx = session();
    table(&ctx, "t", 2, &["-1", "2"]);
    let batches = query(&ctx, "SELECT coalesce(v, to_decimal_arb_from_string('0',110,2)) < to_decimal_arb_from_string('0',110,2) AS less FROM t").await;
    let b = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(b.values().iter().collect::<Vec<_>>(), vec![true, false]);
}

#[tokio::test]
async fn nullif_compares_different_scales() {
    let ctx = session();
    table(&ctx, "t", 0, &["1"]);
    let batches = query(
        &ctx,
        "SELECT nullif(v, to_decimal_arb_from_string('1',110,2)) IS NULL FROM t",
    )
    .await;
    let b = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(b.value(0));
}

#[tokio::test]
async fn array_min_uses_numeric_order() {
    let ctx = session();
    table(&ctx, "t", 2, &["-1"]);
    let batches = query(
        &ctx,
        "SELECT array_min([v, to_decimal_arb_from_string('0',110,2)]) AS v FROM t",
    )
    .await;
    assert_eq!(decimal(&batches[0], 0, 0, 2), "-1");
}

#[tokio::test]
async fn union_same_source_preserves_different_scales() {
    let ctx = session();
    table(&ctx, "t", 0, &["1"]);
    let batches = query(
        &ctx,
        "SELECT v FROM t UNION ALL SELECT to_decimal_arb_from_string('1',110,2) AS v FROM t",
    )
    .await;
    let vals: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            let scale = DecimalArbType::precision_scale_from_field(b.schema().field(0))
                .expect("union decimal metadata")
                .1;
            (0..b.num_rows()).map(move |row| decimal(b, 0, row, scale))
        })
        .collect();
    assert_eq!(vals, vec!["1", "1"]);
}

#[tokio::test]
#[ignore = "Secondary gap: explicit planning failure, excluded from confirmed silent failures"]
async fn union_downstream_comparison_uses_each_scale() {
    let ctx = session();
    table(&ctx, "t", 0, &["1"]);
    let batches = query(&ctx, "SELECT decimal_arb_to_string(v) AS text, v = 1 AS eq FROM (SELECT v FROM t UNION ALL SELECT to_decimal_arb_from_string('1',110,2) AS v FROM t) AS u").await;
    let actual: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            let a = b.column(1).as_any().downcast_ref::<BooleanArray>().unwrap();
            a.iter().collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(actual, vec![Some(true), Some(true)]);
}

#[tokio::test]
async fn in_null_element_keeps_decimal_equality() {
    let ctx = session();
    table(&ctx, "t", 0, &["1"]);
    let batches = query(
        &ctx,
        "SELECT v IN (to_decimal_arb_from_string('1',110,2), NULL) AS matched FROM t",
    )
    .await;
    let b = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(b.iter().collect::<Vec<_>>(), vec![Some(true)]);
}
