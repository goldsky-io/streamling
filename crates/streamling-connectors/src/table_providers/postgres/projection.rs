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
    use arrow::array::{Array, ArrayRef, Int64Array, ListArray, StringArray, StructArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionContext;
    use streamling_core::types::decimal_arb::{DecimalArbArrayBuilder, NativeIntKind};

    const U256_MAX: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    const I256_MIN: &str =
        "-57896044618658097711785492504343953926634992332820282019728792003956564819968";

    fn leaves(values: &[Option<&str>], precision: u32) -> ArrayRef {
        let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), "v", precision, 0).unwrap();
        for v in values {
            match v {
                Some(s) => b.append_str(s).unwrap(),
                None => b.append_null(),
            }
        }
        let (raw, _, _) = b.finish().into_inner();
        Arc::new(raw)
    }

    fn hinted(name: &str, kind: NativeIntKind) -> Arc<Field> {
        Arc::new(
            DecimalArbType::with_native_int_kind(
                DecimalArbType::field(name, 78, 0, true).unwrap(),
                kind,
            )
            .unwrap(),
        )
    }

    /// Container columns holding decimal_arb leaves reach Postgres as JSONB
    /// text through this projection. Every leaf must be its exact decimal
    /// value — 256-bit boundaries, a null under a null struct, and a leaf
    /// wider than NUMERIC's precision cap (which no NUMERIC column holds,
    /// but a JSON string does).
    #[tokio::test]
    async fn nested_decimal_arb_leaves_become_exact_json_values() {
        let value = hinted("value", NativeIntKind::U256);
        let trace_fields: Fields = vec![Arc::clone(&value)].into();
        let trace = Arc::new(Field::new(
            "item",
            DataType::Struct(trace_fields.clone()),
            true,
        ));
        let structs = StructArray::try_new(
            trace_fields,
            vec![leaves(
                &[
                    Some("1"),
                    Some("1000000000000000000"),
                    Some("7"),
                    Some(U256_MAX),
                ],
                78,
            )],
            Some(NullBuffer::from(vec![true, true, false, true])),
        )
        .unwrap();
        let traces = ListArray::try_new(
            Arc::clone(&trace),
            OffsetBuffer::new(vec![0, 2, 4].into()),
            Arc::new(structs),
            None,
        )
        .unwrap();

        let signed_leaf = hinted("item", NativeIntKind::I256);
        let signed = ListArray::try_new(
            Arc::clone(&signed_leaf),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            leaves(&[Some("-1"), Some(I256_MIN)], 78),
            None,
        )
        .unwrap();

        let wide_digits = "9".repeat(1200);
        let wide_leaf = Arc::new(DecimalArbType::field("item", 1200, 0, true).unwrap());
        let wide = ListArray::try_new(
            Arc::clone(&wide_leaf),
            OffsetBuffer::new(vec![0, 1, 1].into()),
            leaves(&[Some(&wide_digits)], 1200),
            None,
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("traces", DataType::List(trace), false),
            Field::new("signed", DataType::List(signed_leaf), false),
            Field::new("wide", DataType::List(wide_leaf), false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(traces),
                Arc::new(signed),
                Arc::new(wide),
            ],
        )
        .unwrap();

        // The config-load check agrees: a nested leaf wider than NUMERIC's
        // cap is not rejected, since it is written as JSONB text.
        use streamling_core::types::decimal_arb_capability::{
            ConnectorKind, validate_pipeline_decimal_arb,
        };
        validate_pipeline_decimal_arb(&schema, ConnectorKind::Postgres, &[]).unwrap();

        let ctx = SessionContext::new();
        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None).unwrap();
        let plan = build_projection_for_postgres(&ctx.state(), input, &schema).unwrap();
        let out = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        let out = &out[0];
        let text = |col: usize| -> Vec<String> {
            let s = out
                .column(col)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..s.len()).map(|i| s.value(i).to_string()).collect()
        };
        let parse = |s: &str| serde_json::from_str::<serde_json::Value>(s).unwrap();

        let traces = text(1);
        assert_eq!(
            parse(&traces[0]),
            serde_json::json!([{"value": "1"}, {"value": "1000000000000000000"}])
        );
        assert_eq!(
            parse(&traces[1]),
            serde_json::json!([null, {"value": U256_MAX}])
        );
        let signed = text(2);
        assert_eq!(parse(&signed[0]), serde_json::json!(["-1"]));
        assert_eq!(parse(&signed[1]), serde_json::json!([I256_MIN]));
        let wide = text(3);
        assert_eq!(parse(&wide[0]), serde_json::json!([wide_digits]));
        assert_eq!(parse(&wide[1]), serde_json::json!([]));
    }
}
