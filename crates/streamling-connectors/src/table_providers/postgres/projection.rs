use arrow_schema::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Result, ToDFSchema};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use std::sync::Arc;
use streamling_core::functions::decimal_arb_ops::{
    DecimalArbToStringFunc, LegacyWideIntToDecimalArbFunc,
};
use streamling_core::functions::json_string::JsonStringFunc;
use streamling_core::types::decimal_arb::DecimalArbType;
use streamling_core::types::decimal_arb_legacy::legacy_wide_int_kind;
// The retired U256/I256 imports are gone; only
// decimal_arb and nested types need projection to Utf8 for PG insert.

/// Build projection expressions to convert decimal_arb and nested types to
/// Utf8 for PostgreSQL insertion. (The U256/I256 paths are retired.)
pub fn build_projection_for_postgres(
    state: &dyn Session,
    input: Arc<dyn ExecutionPlan>,
    input_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let df_schema = input_schema.clone().to_dfschema()?;
    let mut projection_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
        Vec::new();
    let mut needs_projection = false;

    for f in input_schema.fields() {
        let is_decimal_arb = DecimalArbType::is_decimal_arb_field(f);
        // Retired plugin wide-int columns are upgraded to decimal_arb first,
        // then projected to text like any decimal_arb; left alone they bound
        // their raw big-endian bytes into a BYTEA column.
        let is_legacy_wide_int = legacy_wide_int_kind(f).is_some();
        let is_nested_json = matches!(
            f.data_type(),
            datafusion::arrow::datatypes::DataType::Struct(_)
                | datafusion::arrow::datatypes::DataType::List(_)
                | datafusion::arrow::datatypes::DataType::LargeList(_)
                | datafusion::arrow::datatypes::DataType::FixedSizeList(_, _)
                | datafusion::arrow::datatypes::DataType::Map(_, _)
        );

        let logical_expr: datafusion::logical_expr::Expr = if is_decimal_arb || is_legacy_wide_int {
            needs_projection = true;
            let column = datafusion::logical_expr::Expr::Column(
                datafusion::common::Column::from_name(f.name()),
            );
            let decimal = if is_legacy_wide_int {
                datafusion::logical_expr::Expr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction {
                        func: Arc::new(datafusion::logical_expr::ScalarUDF::from(
                            LegacyWideIntToDecimalArbFunc::new(),
                        )),
                        args: vec![column],
                    },
                )
            } else {
                column
            };
            datafusion::logical_expr::Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::new(datafusion::logical_expr::ScalarUDF::from(
                        DecimalArbToStringFunc::new(),
                    )),
                    args: vec![decimal],
                },
            )
            .alias(f.name())
        } else if is_nested_json {
            needs_projection = true;
            datafusion::logical_expr::Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::new(datafusion::logical_expr::ScalarUDF::from(
                        JsonStringFunc::new(),
                    )),
                    args: vec![datafusion::logical_expr::Expr::Column(
                        datafusion::common::Column::from_name(f.name()),
                    )],
                },
            )
            .alias(f.name())
        } else {
            datafusion::logical_expr::Expr::Column(datafusion::common::Column::from_name(f.name()))
        };

        let phys = state.create_physical_expr(logical_expr, &df_schema)?;
        projection_exprs.push((phys, f.name().to_string()));
    }

    if needs_projection {
        Ok(Arc::new(ProjectionExec::try_new(projection_exprs, input)?))
    } else {
        Ok(input)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ListArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionContext;
    use streamling_core::types::decimal_arb::DecimalArbArrayBuilder;

    /// Nested leaves wider than Postgres NUMERIC's precision cap are still
    /// written exactly as JSONB strings by the projection.
    #[tokio::test]
    async fn nested_decimal_arb_leaves_become_exact_json_values() {
        let wide_digits = "9".repeat(1200);
        let wide_leaf = Arc::new(DecimalArbType::field("item", 1200, 0, true).unwrap());
        let mut builder = DecimalArbArrayBuilder::with_capacity(1, "item", 1200, 0).unwrap();
        builder.append_str(&wide_digits).unwrap();
        let (raw, _, _) = builder.finish().into_inner();
        let wide = ListArray::try_new(
            Arc::clone(&wide_leaf),
            OffsetBuffer::new(vec![0, 1, 1].into()),
            Arc::new(raw),
            None,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wide",
            DataType::List(wide_leaf),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(wide)]).unwrap();
        let ctx = SessionContext::new();
        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None).unwrap();
        let plan = build_projection_for_postgres(&ctx.state(), input, &schema).unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        let text = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text.value(0), format!("[\"{wide_digits}\"]"));
        assert_eq!(text.value(1), "[]");
    }
}
