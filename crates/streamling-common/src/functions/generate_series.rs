use crate::streamling_user_bail;
use arrow_schema::FieldRef;
use datafusion::arrow::array::{Array, Int64Array, Int64Builder, ListBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::sync::Arc;

use crate::functions::util::{get_optional_arg_or_default, get_typed_array, validate_arg_count};

/// Generates a series of numbers from start to stop with an optional step.
///
/// Creates an array containing a sequence of integers from `start` to `stop` (inclusive),
/// incrementing by `step`. If no step is provided, defaults to 1.
///
/// # Arguments
/// * `start` - The starting value of the series
/// * `stop` - The ending value of the series (inclusive)
/// * `step` - Optional step size (defaults to 1 if not provided, cannot be 0)
///
/// # Returns
/// An array of integers representing the series
///
/// # Examples
/// * `generate_series(1, 10)` returns `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`
/// * `generate_series(1, 10, 2)` returns `[1, 3, 5, 7, 9]`
/// * `generate_series(10, 1, 1)` returns `[]` (incorrect step direction)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GenerateSeriesFunc {
    signature: Signature,
}

impl Default for GenerateSeriesFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl GenerateSeriesFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GenerateSeriesFunc {
    fn name(&self) -> &str {
        "_gs_generate_series"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Int64,
            false,
        ))))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int64,
                false,
            ))),
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        validate_arg_count(&args, 2, Some(3), self.name())?;

        let start_array = get_typed_array::<Int64Array>(&args.args[0], "start")?;
        let stop_array = get_typed_array::<Int64Array>(&args.args[1], "stop")?;
        let step_array = get_optional_arg_or_default(
            &args,
            2,
            |len| Arc::new(Int64Array::from(vec![1; len])),
            "step",
        )?;

        let mut list_builder = ListBuilder::new(Int64Builder::new());

        (0..start_array.len()).try_for_each(|i| -> Result<()> {
            if start_array.is_null(i) || stop_array.is_null(i) {
                list_builder.append(false);
                return Ok(());
            }

            let start = start_array.value(i);
            let stop = stop_array.value(i);
            let step = if step_array.is_null(i) {
                1
            } else {
                let step_val = step_array.value(i);
                if step_val == 0 {
                    streamling_user_bail!("Step size cannot be 0 in _gs_generate_series!");
                }
                step_val
            };

            if step > 0 && start <= stop {
                (start..=stop)
                    .step_by(step as usize)
                    .for_each(|v| list_builder.values().append_value(v));
            } else if step < 0 && start >= stop {
                let mut current = start;
                while current >= stop {
                    list_builder.values().append_value(current);
                    current += step;
                }
            }

            list_builder.append(true);
            Ok(())
        })?;

        let result_array = list_builder.finish();
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_generate_a_series() {
        let func = GenerateSeriesFunc::new();

        let start_array = Int64Array::from(vec![1]);
        let stop_array = Int64Array::from(vec![10]);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(start_array)),
                ColumnarValue::Array(Arc::new(stop_array)),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("start", DataType::Int64, false)),
                Arc::new(arrow_schema::Field::new("stop", DataType::Int64, false)),
            ],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new(
                "result",
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Int64,
                    false,
                ))),
                false,
            )),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let list_array = result_array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::ListArray>()
                .unwrap();

            let value = list_array.value(0);
            let int_array = value.as_any().downcast_ref::<Int64Array>().unwrap();

            assert_eq!(int_array.len(), 10);
            let expected = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            for (i, expected_val) in expected.iter().enumerate() {
                assert_eq!(int_array.value(i), *expected_val);
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_generate_a_series_with_a_step() {
        let func = GenerateSeriesFunc::new();

        let start_array = Int64Array::from(vec![1]);
        let stop_array = Int64Array::from(vec![10]);
        let step_array = Int64Array::from(vec![2]);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(start_array)),
                ColumnarValue::Array(Arc::new(stop_array)),
                ColumnarValue::Array(Arc::new(step_array)),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("start", DataType::Int64, false)),
                Arc::new(arrow_schema::Field::new("stop", DataType::Int64, false)),
                Arc::new(arrow_schema::Field::new("step", DataType::Int64, false)),
            ],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new(
                "result",
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Int64,
                    false,
                ))),
                false,
            )),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let list_array = result_array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::ListArray>()
                .unwrap();

            let value = list_array.value(0);
            let int_array = value.as_any().downcast_ref::<Int64Array>().unwrap();

            assert_eq!(int_array.len(), 5);
            let expected = [1, 3, 5, 7, 9];
            for (i, expected_val) in expected.iter().enumerate() {
                assert_eq!(int_array.value(i), *expected_val);
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_not_allow_0_as_a_step() {
        let func = GenerateSeriesFunc::new();

        let start_array = Int64Array::from(vec![1]);
        let stop_array = Int64Array::from(vec![2]);
        let step_array = Int64Array::from(vec![0]);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(start_array)),
                ColumnarValue::Array(Arc::new(stop_array)),
                ColumnarValue::Array(Arc::new(step_array)),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("start", DataType::Int64, false)),
                Arc::new(arrow_schema::Field::new("stop", DataType::Int64, false)),
                Arc::new(arrow_schema::Field::new("step", DataType::Int64, false)),
            ],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new(
                "result",
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Int64,
                    false,
                ))),
                false,
            )),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("Step size cannot be 0 in _gs_generate_series!")
        );
    }

    #[test]
    fn test_generate_empty_array_with_incorrect_sign_on_step() {
        let func = GenerateSeriesFunc::new();

        let start_array = Int64Array::from(vec![10]);
        let stop_array = Int64Array::from(vec![1]);
        let step_array = Int64Array::from(vec![1]);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(start_array)),
                ColumnarValue::Array(Arc::new(stop_array)),
                ColumnarValue::Array(Arc::new(step_array)),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("start", DataType::Int64, false)),
                Arc::new(arrow_schema::Field::new("stop", DataType::Int64, false)),
                Arc::new(arrow_schema::Field::new("step", DataType::Int64, false)),
            ],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new(
                "result",
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Int64,
                    false,
                ))),
                false,
            )),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let list_array = result_array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::ListArray>()
                .unwrap();

            let value = list_array.value(0);
            let int_array = value.as_any().downcast_ref::<Int64Array>().unwrap();

            assert_eq!(int_array.len(), 0);
        } else {
            panic!("Expected array result");
        }
    }
}
