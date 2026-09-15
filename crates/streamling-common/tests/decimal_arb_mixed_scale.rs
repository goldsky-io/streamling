//! Regression coverage for two silent-failure classes in the decimal_arb
//! planner rewrite:
//!
//! 1. **Mixed scales in value-returning nodes.** `greatest(amount, 0)`,
//!    `coalesce(amount, 0)`, `CASE … ELSE 0 END`, `array_min([amount, 0])`
//!    mix a scaled column with a scale-0 literal. The result used to be a
//!    `LargeBinary` column with rows encoded at *different* scales and no
//!    metadata, so anything downstream read half the rows wrong.
//! 2. **String literals.** `amount > '1000…'` used to fall back to DataFusion's
//!    `Utf8 → LargeBinary` coercion and compare UTF-8 bytes with the canonical
//!    encoding — silently false.

use arrow::array::{Array, BooleanArray, Int64Array, LargeBinaryArray, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::{FunctionRegistry, SessionStateBuilder, SessionStateDefaults};
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::prelude::SessionContext;
use std::str::FromStr;
use std::sync::Arc;
use streamling_common::functions::CommonFunctions;
use streamling_common::functions::decimal_arb_aggregates::{
    DecimalArbArrayAggUdaf, DecimalArbAvgUdaf, DecimalArbExtremeUdaf, DecimalArbSumUdaf,
};
use streamling_common::functions::decimal_arb_coercion::DecimalArbExprPlanner;
use streamling_common::functions::decimal_arb_ops::DecimalArbRescaleFunc;
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

fn decimal_column(name: &str, scale: u32, vals: &[Option<&str>]) -> (Field, Arc<dyn Array>) {
    let mut b = DecimalArbArrayBuilder::with_capacity(vals.len(), name, 110, scale).unwrap();
    for v in vals {
        match v {
            Some(v) => b.append_str(v).unwrap(),
            None => b.append_null(),
        }
    }
    let (array, _, _) = b.finish().into_inner();
    (
        DecimalArbType::field(name, 110, scale, true).unwrap(),
        Arc::new(array),
    )
}

/// `t(id, v, w, s)`: `v` decimal_arb scale 2, `w` decimal_arb scale 0, `s` Utf8.
fn table(ctx: &SessionContext, v: &[Option<&str>], w: &[Option<&str>], s: &[&str]) {
    let n = v.len();
    let (vf, va) = decimal_column("v", 2, v);
    let (wf, wa) = decimal_column("w", 0, w);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        vf,
        wf,
        Field::new("s", DataType::Utf8, false),
    ]));
    let ids = Int64Array::from_iter_values(1..=n as i64);
    ctx.register_batch(
        "t",
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ids),
                va,
                wa,
                Arc::new(StringArray::from(s.to_vec())),
            ],
        )
        .unwrap(),
    )
    .unwrap();
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

/// Every value of column 0, decoded at the scale the OUTPUT FIELD declares —
/// exactly what a sink does. Panics if the field carries no decimal_arb
/// metadata, because that is the failure mode under test.
fn decimals(batches: &[RecordBatch]) -> Vec<Option<DecimalArbValue>> {
    let mut out = Vec::new();
    for b in batches {
        let (_, scale) = DecimalArbType::precision_scale_from_field(b.schema().field(0))
            .expect("result column must carry decimal_arb metadata");
        let a = b
            .column(0)
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

fn d(s: &str) -> Option<DecimalArbValue> {
    Some(DecimalArbValue::from_str(s).unwrap())
}

fn bools(batches: &[RecordBatch]) -> Vec<Option<bool>> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect()
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn strings(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| s.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Mixed scales
// ---------------------------------------------------------------------------

#[tokio::test]
async fn greatest_with_int_literal_carries_one_scale() {
    let ctx = session();
    table(
        &ctx,
        &[Some("-1"), Some("2"), None],
        &[Some("1"), Some("2"), Some("3")],
        &["", "", ""],
    );
    let b = query(&ctx, "SELECT greatest(v, 0) FROM t ORDER BY id").await;
    let (_, scale) = DecimalArbType::precision_scale_from_field(b[0].schema().field(0)).unwrap();
    assert_eq!(scale, 2, "result must sit at the column's scale");
    // greatest skips NULLs: greatest(NULL, 0) = 0
    assert_eq!(decimals(&b), vec![d("0"), d("2"), d("0")]);
}

#[tokio::test]
async fn greatest_result_compares_and_prints_correctly_downstream() {
    let ctx = session();
    table(
        &ctx,
        &[Some("-1"), Some("2")],
        &[Some("1"), Some("2")],
        &["", ""],
    );
    let b = query(
        &ctx,
        "SELECT greatest(v, 0) > to_decimal_arb_from_string('1', 110, 2) FROM t ORDER BY id",
    )
    .await;
    assert_eq!(bools(&b), vec![Some(false), Some(true)]);

    let b = query(
        &ctx,
        "SELECT decimal_arb_to_string(greatest(v, 0)) FROM t ORDER BY id",
    )
    .await;
    let got: Vec<_> = strings(&b)
        .iter()
        .map(|s| DecimalArbValue::from_str(s).unwrap())
        .collect();
    assert_eq!(got, vec![d("0").unwrap(), d("2").unwrap()]);

    let b = query(&ctx, "SELECT greatest(v, 0) + v FROM t ORDER BY id").await;
    assert_eq!(decimals(&b), vec![d("-1"), d("4")]);
}

#[tokio::test]
async fn least_with_decimal128_literal_widens_to_column_scale() {
    let ctx = session();
    table(
        &ctx,
        &[Some("-1"), Some("2")],
        &[Some("1"), Some("2")],
        &["", ""],
    );
    // '1.5' parses as an exact decimal at scale 1; the column is scale 2.
    let b = query(&ctx, "SELECT least(v, '1.5') FROM t ORDER BY id").await;
    let (_, scale) = DecimalArbType::precision_scale_from_field(b[0].schema().field(0)).unwrap();
    assert_eq!(scale, 2);
    assert_eq!(decimals(&b), vec![d("-1"), d("1.5")]);
}

#[tokio::test]
async fn coalesce_with_int_literal_fills_nulls_at_column_scale() {
    let ctx = session();
    table(
        &ctx,
        &[None, Some("2.25")],
        &[Some("1"), Some("2")],
        &["", ""],
    );
    let b = query(&ctx, "SELECT coalesce(v, 0) FROM t ORDER BY id").await;
    assert_eq!(decimals(&b), vec![d("0"), d("2.25")]);
}

#[tokio::test]
async fn case_with_int_literal_else_keeps_values() {
    let ctx = session();
    table(
        &ctx,
        &[Some("-1"), Some("2")],
        &[Some("1"), Some("2")],
        &["", ""],
    );
    let b = query(
        &ctx,
        "SELECT CASE WHEN id = 1 THEN v ELSE 0 END FROM t ORDER BY id",
    )
    .await;
    assert_eq!(decimals(&b), vec![d("-1"), d("0")]);
    // …and the value survives a downstream comparison.
    let b = query(
        &ctx,
        "SELECT (CASE WHEN id = 1 THEN v ELSE 0 END) < to_decimal_arb_from_string('0', 110, 2) FROM t ORDER BY id",
    )
    .await;
    assert_eq!(bools(&b), vec![Some(true), Some(false)]);
}

#[tokio::test]
async fn case_over_two_columns_of_different_scales() {
    let ctx = session();
    // v is scale 2, w is scale 0 — both decimal_arb.
    table(
        &ctx,
        &[Some("1.5"), Some("2.5")],
        &[Some("7"), Some("8")],
        &["", ""],
    );
    let b = query(
        &ctx,
        "SELECT CASE WHEN id = 1 THEN v ELSE w END FROM t ORDER BY id",
    )
    .await;
    let (_, scale) = DecimalArbType::precision_scale_from_field(b[0].schema().field(0)).unwrap();
    assert_eq!(scale, 2, "widest scale wins");
    assert_eq!(decimals(&b), vec![d("1.5"), d("8")]);
}

#[tokio::test]
async fn array_min_and_max_with_int_literal() {
    let ctx = session();
    table(
        &ctx,
        &[Some("-1"), Some("2")],
        &[Some("1"), Some("2")],
        &["", ""],
    );
    let b = query(&ctx, "SELECT array_min([v, 0]) FROM t ORDER BY id").await;
    assert_eq!(decimals(&b), vec![d("-1"), d("0")]);
    let b = query(&ctx, "SELECT array_max([v, 0]) FROM t ORDER BY id").await;
    assert_eq!(decimals(&b), vec![d("0"), d("2")]);
}

#[tokio::test]
async fn nullif_keeps_left_scale() {
    let ctx = session();
    // w is 1 (scale 0); compared with 1.00 (scale 2) they are equal → NULL.
    table(
        &ctx,
        &[Some("1"), Some("2")],
        &[Some("1"), Some("2")],
        &["", ""],
    );
    let b = query(&ctx, "SELECT nullif(w, '1.00') FROM t ORDER BY id").await;
    let (_, scale) = DecimalArbType::precision_scale_from_field(b[0].schema().field(0)).unwrap();
    assert_eq!(scale, 0);
    assert_eq!(decimals(&b), vec![None, d("2")]);
}

#[tokio::test]
async fn sql_unary_minus_and_abs_route_to_decimal_arb() {
    let ctx = session();
    table(
        &ctx,
        &[Some("-1.5"), Some("2.25"), None],
        &[Some("1"), Some("2"), Some("3")],
        &["", "", ""],
    );
    let b = query(&ctx, "SELECT -v FROM t ORDER BY id").await;
    assert_eq!(decimals(&b), vec![d("1.5"), d("-2.25"), None]);
    let b = query(&ctx, "SELECT abs(v) FROM t ORDER BY id").await;
    assert_eq!(decimals(&b), vec![d("1.5"), d("2.25"), None]);
    // …and the results keep flowing through decimal_arb comparisons.
    let b = query(&ctx, "SELECT abs(v) > 2 FROM t ORDER BY id").await;
    assert_eq!(bools(&b), vec![Some(false), Some(true), None]);
}

// ---------------------------------------------------------------------------
// String literals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn string_literal_comparisons_are_numeric() {
    let ctx = session();
    table(
        &ctx,
        &[Some("1"), Some("2")],
        &[Some("1"), Some("2")],
        &["", ""],
    );
    assert_eq!(
        ids(&query(&ctx, "SELECT id FROM t WHERE w = '1'").await),
        vec![1]
    );
    assert_eq!(
        ids(&query(&ctx, "SELECT id FROM t WHERE w = '1.00'").await),
        vec![1]
    );
    assert_eq!(
        ids(&query(&ctx, "SELECT id FROM t WHERE w > '0.5' ORDER BY id").await),
        vec![1, 2]
    );
    assert_eq!(
        ids(&query(&ctx, "SELECT id FROM t WHERE w IN ('1', '3')").await),
        vec![1]
    );
    assert_eq!(
        ids(&query(&ctx, "SELECT id FROM t WHERE w BETWEEN '0' AND '1'").await),
        vec![1]
    );
    assert_eq!(
        ids(&query(&ctx, "SELECT id FROM t WHERE '2' <= w").await),
        vec![2]
    );
    // A wide literal — the case users quote precisely because unquoted goes
    // through Float64.
    let wide = "123456789012345678901234567890";
    let ctx2 = session();
    table(
        &ctx2,
        &[Some(wide), Some("2")],
        &[Some(wide), Some("2")],
        &["", ""],
    );
    assert_eq!(
        ids(&query(&ctx2, &format!("SELECT id FROM t WHERE w = '{wide}'")).await),
        vec![1]
    );
    assert_eq!(
        ids(&query(&ctx2, &format!("SELECT id FROM t WHERE v >= '{wide}'")).await),
        vec![1]
    );
}

#[tokio::test]
async fn string_column_comparison_is_rejected_not_byte_compared() {
    let ctx = session();
    table(
        &ctx,
        &[Some("1"), Some("2")],
        &[Some("1"), Some("2")],
        &["1", "2"],
    );
    let err = ctx
        .sql("SELECT id FROM t WHERE w = s")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("comparing decimal_arb with a string column must fail loudly");
    assert!(
        err.to_string().contains("to_decimal_arb_from_string"),
        "error should point at the explicit conversion: {err}"
    );
}

#[tokio::test]
async fn non_numeric_string_literal_is_a_planning_error() {
    let ctx = session();
    table(&ctx, &[Some("1")], &[Some("1")], &[""]);
    let err = ctx
        .sql("SELECT id FROM t WHERE w = 'abc'")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("non-numeric literal must not silently compare false");
    assert!(err.to_string().contains("not a decimal number"), "{err}");
}

// ---------------------------------------------------------------------------
// decimal_arb_rescale
// ---------------------------------------------------------------------------

fn invoke_rescale(
    vals: &[Option<&str>],
    scale_in: u32,
    p: u32,
    s: u32,
) -> Result<Vec<Option<DecimalArbValue>>, String> {
    let (field, array) = decimal_column("x", scale_in, vals);
    let func = DecimalArbRescaleFunc::new();
    let fields = vec![
        Arc::new(field),
        Arc::new(Field::new("p", DataType::Int64, false)),
        Arc::new(Field::new("s", DataType::Int64, false)),
    ];
    let p_lit = datafusion::scalar::ScalarValue::Int64(Some(p as i64));
    let s_lit = datafusion::scalar::ScalarValue::Int64(Some(s as i64));
    let ret = func
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &[None, Some(&p_lit), Some(&s_lit)],
        })
        .map_err(|e| e.to_string())?;
    let (_, out_scale) = DecimalArbType::precision_scale_from_field(&ret).unwrap();
    let out = func
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(array),
                ColumnarValue::Scalar(p_lit),
                ColumnarValue::Scalar(s_lit),
            ],
            arg_fields: fields,
            number_rows: vals.len(),
            return_field: ret,
            config_options: Arc::default(),
        })
        .map_err(|e| e.to_string())?;
    let ColumnarValue::Array(arr) = out else {
        panic!("expected array")
    };
    let arr = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    Ok((0..arr.len())
        .map(|i| {
            (!arr.is_null(i)).then(|| {
                DecimalArbValue::from_canonical_bytes_at_scale(arr.value(i), out_scale).unwrap()
            })
        })
        .collect())
}

#[test]
fn rescale_widens_exactly_and_keeps_nulls() {
    let got = invoke_rescale(&[Some("1"), Some("-2"), None], 0, 10, 3).unwrap();
    assert_eq!(got, vec![d("1"), d("-2"), None]);
}

#[test]
fn rescale_refuses_to_drop_significant_digits() {
    let err = invoke_rescale(&[Some("1.25")], 2, 10, 1).expect_err("must not round silently");
    assert!(err.contains("fractional"), "{err}");
}

// ---------------------------------------------------------------------------
// Plan-level: UNION branches and JOIN keys at different scales
// ---------------------------------------------------------------------------

fn register(ctx: &SessionContext, name: &str, scale: u32, rows: &[(i64, Option<&str>)]) {
    let mut b = DecimalArbArrayBuilder::with_capacity(rows.len(), "v", 110, scale).unwrap();
    for (_, v) in rows {
        match v {
            Some(v) => b.append_str(v).unwrap(),
            None => b.append_null(),
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        DecimalArbType::field("v", 110, scale, true).unwrap(),
    ]));
    let ids = Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id));
    ctx.register_batch(
        name,
        RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(b.finish().into_inner().0)],
        )
        .unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn union_all_across_scales_merges_numerically() {
    let ctx = session();
    register(&ctx, "a", 0, &[(1, Some("1")), (2, Some("7"))]);
    register(&ctx, "b", 2, &[(1, Some("1.00")), (2, Some("7.25"))]);
    let u = "(SELECT v FROM a UNION ALL SELECT v FROM b)";

    let b = query(&ctx, &format!("SELECT v FROM {u} x ORDER BY v")).await;
    assert_eq!(decimals(&b), vec![d("1"), d("1"), d("7"), d("7.25")]);

    let b = query(&ctx, &format!("SELECT COUNT(DISTINCT v) FROM {u} x")).await;
    assert_eq!(ids(&b), vec![3], "1 and 1.00 are the same number");

    let b = query(&ctx, &format!("SELECT SUM(v) FROM {u} x")).await;
    assert_eq!(decimals(&b), vec![d("16.25")]);

    let b = query(
        &ctx,
        &format!("SELECT decimal_arb_to_string(v) FROM {u} x GROUP BY v ORDER BY v"),
    )
    .await;
    let got: Vec<_> = strings(&b)
        .iter()
        .map(|s| DecimalArbValue::from_str(s).unwrap())
        .collect();
    assert_eq!(
        got,
        vec![d("1").unwrap(), d("7").unwrap(), d("7.25").unwrap()]
    );
}

#[tokio::test]
async fn join_using_and_natural_join_across_scales_match() {
    let ctx = session();
    register(
        &ctx,
        "a",
        0,
        &[(1, Some("1")), (2, Some("2")), (3, Some("3"))],
    );
    register(
        &ctx,
        "b",
        2,
        &[(1, Some("1.00")), (2, Some("3.00")), (3, Some("4.00"))],
    );

    let b = query(&ctx, "SELECT a.id FROM a JOIN b USING (v) ORDER BY a.id").await;
    assert_eq!(ids(&b), vec![1, 3]);
    let b = query(&ctx, "SELECT b.id FROM a JOIN b USING (v) ORDER BY b.id").await;
    assert_eq!(ids(&b), vec![1, 2]);
    // NATURAL joins on both shared columns, id and v.
    let b = query(&ctx, "SELECT a.id FROM a NATURAL JOIN b").await;
    assert_eq!(ids(&b), vec![1]);
}

#[tokio::test]
async fn in_subquery_across_scales_is_numeric() {
    let ctx = session();
    register(
        &ctx,
        "a",
        0,
        &[(1, Some("1")), (2, Some("2")), (3, Some("3"))],
    );
    register(
        &ctx,
        "b",
        2,
        &[(1, Some("1.00")), (2, Some("3.00")), (3, Some("4.00"))],
    );
    let b = query(
        &ctx,
        "SELECT id FROM a WHERE v IN (SELECT v FROM b) ORDER BY id",
    )
    .await;
    assert_eq!(ids(&b), vec![1, 3]);
    let b = query(
        &ctx,
        "SELECT id FROM a WHERE v NOT IN (SELECT v FROM b) ORDER BY id",
    )
    .await;
    assert_eq!(ids(&b), vec![2]);
}

// ---------------------------------------------------------------------------
// String builtins and containers see the decimal text, not the bytes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn like_and_string_functions_operate_on_decimal_text() {
    let ctx = session();
    table(
        &ctx,
        &[Some("2.56"), Some("300.00"), Some("1.00"), Some("-5.00")],
        &[Some("1"), Some("2"), Some("3"), Some("4")],
        &["", "", "", ""],
    );
    assert_eq!(
        ids(&query(&ctx, "SELECT id FROM t WHERE v LIKE '2%'").await),
        vec![1]
    );
    assert_eq!(
        ids(&query(&ctx, "SELECT id FROM t WHERE v NOT LIKE '%.00' ORDER BY id").await),
        vec![1]
    );
    let b = query(&ctx, "SELECT length(v) FROM t ORDER BY id").await;
    let lens: Vec<i64> = b
        .iter()
        .flat_map(|b| {
            let a = b.column(0);
            (0..a.len())
                .map(|i| {
                    arrow::array::cast::as_primitive_array::<arrow::datatypes::Int32Type>(a)
                        .value(i) as i64
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(lens, vec![4, 6, 4, 5]);
    let b = query(&ctx, "SELECT concat(v, 'x') FROM t ORDER BY id").await;
    assert_eq!(strings(&b), vec!["2.56x", "300.00x", "1.00x", "-5.00x"]);
}

#[tokio::test]
async fn try_constructor_yields_null_instead_of_failing() {
    let ctx = session();
    table(
        &ctx,
        &[Some("1"), Some("2"), Some("3"), Some("4")],
        &[Some("1"), Some("2"), Some("3"), Some("4")],
        &["1.5", "abc", "123456", "-0.25"],
    );
    // TRY_CAST(s AS DECIMAL(77, 2)) is rewritten to this by the preprocessor:
    // a value that does not parse or does not fit becomes NULL, the rest
    // convert exactly.
    let b = query(
        &ctx,
        "SELECT try_to_decimal_arb_from_string(s, 5, 2) FROM t ORDER BY id",
    )
    .await;
    let (p, scale) = DecimalArbType::precision_scale_from_field(b[0].schema().field(0)).unwrap();
    assert_eq!((p, scale), (5, 2));
    assert_eq!(decimals(&b), vec![d("1.50"), None, None, d("-0.25")]);
    // The throwing constructor keeps failing on the same input.
    ctx.sql("SELECT to_decimal_arb_from_string(s, 5, 2) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("plain CAST must still reject a non-numeric value");
}

#[tokio::test]
async fn nvl_and_ifnull_are_unified_and_stamped() {
    let ctx = session();
    table(
        &ctx,
        &[None, Some("2.25")],
        &[Some("1"), Some("2")],
        &["", ""],
    );
    let b = query(&ctx, "SELECT nvl(v, 0) FROM t ORDER BY id").await;
    assert_eq!(decimals(&b), vec![d("0"), d("2.25")]);
    let b = query(&ctx, "SELECT ifnull(v, w) FROM t ORDER BY id").await;
    assert_eq!(decimals(&b), vec![d("1"), d("2.25")]);
    let b = query(&ctx, "SELECT nvl2(v, v, 0) FROM t ORDER BY id").await;
    assert_eq!(decimals(&b), vec![d("0"), d("2.25")]);
}

#[tokio::test]
async fn containers_carry_decimal_text_to_the_json_sink() {
    use streamling_common::formats::FromArrowConverter;
    use streamling_common::formats::json::FromArrowToJsonConverter;
    let ctx = session();
    table(
        &ctx,
        &[Some("2.56"), Some("-5.00")],
        &[Some("1"), Some("2")],
        &["", ""],
    );
    let json = |b: &[RecordBatch]| -> Vec<String> {
        b.iter()
            .flat_map(|b| {
                FromArrowToJsonConverter::new()
                    .convert_from_batch(b)
                    .unwrap()
                    .into_iter()
                    .map(|r| String::from_utf8(r).unwrap())
            })
            .collect()
    };
    let b = query(&ctx, "SELECT named_struct('a', v) AS x FROM t ORDER BY id").await;
    assert_eq!(
        json(&b),
        vec![r#"{"x":{"a":"2.56"}}"#, r#"{"x":{"a":"-5.00"}}"#]
    );
    let b = query(&ctx, "SELECT make_array(v) AS x FROM t ORDER BY id").await;
    assert_eq!(json(&b), vec![r#"{"x":["2.56"]}"#, r#"{"x":["-5.00"]}"#]);
    let b = query(&ctx, "SELECT array_agg(v ORDER BY id) AS x FROM t").await;
    assert_eq!(json(&b), vec![r#"{"x":["2.56","-5.00"]}"#]);
    // greatest(v, NULL) used to lose the metadata too.
    let b = query(&ctx, "SELECT greatest(v, NULL) FROM t ORDER BY id").await;
    assert_eq!(decimals(&b), vec![d("2.56"), d("-5.00")]);
}
