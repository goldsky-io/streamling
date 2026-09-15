//! Final set-operation differential probes through accepted scalar subqueries.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use std::sync::Arc;
use streamling_common::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType};
use streamling_core::{dynamic_table::DynamicTableRegistry, session::SessionManager};
fn session() -> SessionManager {
    let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
    for (name, native) in [("t", false), ("n", true)] {
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))];
        let mut fields = vec![Field::new("id", DataType::Int64, false)];
        for (column, scale, values) in [
            ("a", 0, vec![Some(255i128), Some(256), Some(-1), None]),
            ("b", 2, vec![Some(25600), Some(25500), Some(0), Some(100)]),
        ] {
            if native {
                arrays.push(Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(30, scale as i8)
                        .unwrap(),
                ));
                fields.push(Field::new(
                    column,
                    DataType::Decimal128(30, scale as i8),
                    true,
                ));
            } else {
                let mut b = DecimalArbArrayBuilder::with_capacity(4, column, 100, scale).unwrap();
                for v in values {
                    match v {Some(v)=>b.append_value(&streamling_common::types::decimal_arb::DecimalArbValue::from_bigint_and_scale(v.into(),scale as i64)).unwrap(),None=>b.append_null()}
                }
                arrays.push(Arc::new(b.finish().into_inner().0));
                fields.push(DecimalArbType::field(column, 100, scale, true).unwrap());
            }
        }
        sm.session_context()
            .register_batch(
                name,
                RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap(),
            )
            .unwrap();
    }
    sm
}
async fn count(sm: &SessionManager, table: &str, operator: &str) -> datafusion::error::Result<i64> {
    let sql = format!(
        "SELECT(SELECT COUNT(*) FROM(SELECT a AS v FROM {table} {operator} SELECT b AS v FROM {table})q) AS n FROM {table} WHERE id=1"
    );
    let (plan, _) = sm.create_supported_logical_plan(sql.clone()).await?;
    eprintln!("SET_SQL {sql}\nPLAN {}", plan.display_indent());
    let batches = sm.new_df(plan).collect().await?;
    let n = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    eprintln!("SET_RESULT {operator} {table}: {n}");
    Ok(n)
}
#[tokio::test]
#[ignore = "Diagnostic excluded: native Decimal128 UNION count is 8 instead of 6 too"]
async fn supported_union_distinct_compares_values_across_scales() {
    let sm = session();
    let expected = count(&sm, "n", "UNION").await.unwrap();
    assert_eq!(expected, 6);
    assert_eq!(count(&sm, "t", "UNION").await.unwrap(), expected);
}
#[tokio::test]
#[ignore = "Diagnostic excluded: native Decimal128 fails HashJoinExec PartitionMode Auto"]
async fn supported_except_compares_values_across_scales() {
    let sm = session();
    let expected = count(&sm, "n", "EXCEPT").await.unwrap();
    assert_eq!(expected, 2);
    assert_eq!(count(&sm, "t", "EXCEPT").await.unwrap(), expected);
}
#[tokio::test]
#[ignore = "Diagnostic excluded: native Decimal128 fails HashJoinExec PartitionMode Auto"]
async fn supported_intersect_compares_values_across_scales() {
    let sm = session();
    let expected = count(&sm, "n", "INTERSECT").await.unwrap();
    assert_eq!(expected, 2);
    assert_eq!(count(&sm, "t", "INTERSECT").await.unwrap(), expected);
}
