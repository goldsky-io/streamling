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
