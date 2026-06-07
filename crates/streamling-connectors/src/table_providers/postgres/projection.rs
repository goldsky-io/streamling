use arrow_schema::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Result, ToDFSchema};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use std::sync::Arc;
use streamling_core::functions::json_string::JsonStringFunc;
use streamling_core::functions::{i256_ops::I256ToStringFunc, u256_ops::U256ToStringFunc};
use streamling_core::types::{i256::I256Type, u256::U256Type};

/// Build projection expressions to convert U256/I256 and nested types to Utf8 for PostgreSQL insertion
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
        let is_u256 = matches!(
            f.data_type(),
            datafusion::arrow::datatypes::DataType::FixedSizeBinary(32)
        ) && U256Type::is_u256_metadata(f.metadata());
        let is_i256 = matches!(
            f.data_type(),
            datafusion::arrow::datatypes::DataType::FixedSizeBinary(32)
        ) && I256Type::is_i256_metadata(f.metadata());
        let is_nested_json = matches!(
            f.data_type(),
            datafusion::arrow::datatypes::DataType::Struct(_)
                | datafusion::arrow::datatypes::DataType::List(_)
                | datafusion::arrow::datatypes::DataType::LargeList(_)
                | datafusion::arrow::datatypes::DataType::FixedSizeList(_, _)
                | datafusion::arrow::datatypes::DataType::Map(_, _)
        );

        let logical_expr: datafusion::logical_expr::Expr = if is_u256 {
            needs_projection = true;
            datafusion::logical_expr::Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::new(datafusion::logical_expr::ScalarUDF::from(
                        U256ToStringFunc::new(),
                    )),
                    args: vec![datafusion::logical_expr::Expr::Column(
                        datafusion::common::Column::from_name(f.name()),
                    )],
                },
            )
            .alias(f.name())
        } else if is_i256 {
            needs_projection = true;
            datafusion::logical_expr::Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::new(datafusion::logical_expr::ScalarUDF::from(
                        I256ToStringFunc::new(),
                    )),
                    args: vec![datafusion::logical_expr::Expr::Column(
                        datafusion::common::Column::from_name(f.name()),
                    )],
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
