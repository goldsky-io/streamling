use arrow_schema::FieldRef;
use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use num_bigint::BigInt;
use num_traits::Num;
use std::sync::Arc;

use crate::functions::util::get_three_args;

/// Converts numbers between different bases.
///
/// Takes a number as a string in one base and converts it to another base.
/// Supports bases from 2 to 36.
///
/// # Arguments
/// * `number` - The number to convert (as a string)
/// * `from_base` - The base of the input number (2-36)
/// * `to_base` - The base to convert to (2-36)
///
/// # Returns
/// The number represented in the target base as a string
///
/// # Examples
/// * `conv_base('FF', 16, 10)` returns `'255'`
/// * `conv_base('1010', 2, 16)` returns `'a'`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvBaseFunc {
    signature: Signature,
}

impl Default for ConvBaseFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ConvBaseFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Int32, DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ConvBaseFunc {
    fn name(&self) -> &str {
        "_gs_conv_base"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::Utf8,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Handle both Int32 and String types for bases
        let (number_array, from_base_array, to_base_array) = match args.args.get(1) {
            Some(ColumnarValue::Array(arr)) if arr.data_type() == &DataType::Int32 => {
                let (number, from, to) = get_three_args::<StringArray, Int32Array, Int32Array>(
                    &args,
                    self.name(),
                    "number",
                    "from_base",
                    "to_base",
                )?;
                (number, from, to)
            }
            _ => {
                // Try to parse as strings
                let (number, from_str, to_str) =
                    get_three_args::<StringArray, StringArray, StringArray>(
                        &args,
                        self.name(),
                        "number",
                        "from_base",
                        "to_base",
                    )?;

                let from_int: Vec<Option<i32>> = (0..from_str.len())
                    .map(|i| {
                        if from_str.is_null(i) {
                            None
                        } else {
                            from_str.value(i).parse::<i32>().ok()
                        }
                    })
                    .collect();

                let to_int: Vec<Option<i32>> = (0..to_str.len())
                    .map(|i| {
                        if to_str.is_null(i) {
                            None
                        } else {
                            to_str.value(i).parse::<i32>().ok()
                        }
                    })
                    .collect();

                (
                    number,
                    Arc::new(Int32Array::from(from_int)),
                    Arc::new(Int32Array::from(to_int)),
                )
            }
        };

        let result_values: Vec<Option<String>> = (0..number_array.len())
            .map(|i| {
                if number_array.is_null(i) || from_base_array.is_null(i) || to_base_array.is_null(i)
                {
                    return None;
                }

                let number = number_array.value(i);
                let from_base = from_base_array.value(i) as u32;
                let to_base = to_base_array.value(i) as u32;

                if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_base) {
                    return None;
                }

                // Parse the number in the source base
                match BigInt::from_str_radix(number, from_base) {
                    Ok(big_num) => Some(big_num.to_str_radix(to_base)),
                    Err(_) => None,
                }
            })
            .collect();

        let result_array = StringArray::from(result_values);
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conv_base_hex_to_decimal() {
        let func = ConvBaseFunc::new();

        let number_array = StringArray::from(vec!["FF", "10", "A"]);
        let from_base_array = Int32Array::from(vec![16, 16, 16]);
        let to_base_array = Int32Array::from(vec![10, 10, 10]);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(number_array)),
                ColumnarValue::Array(Arc::new(from_base_array)),
                ColumnarValue::Array(Arc::new(to_base_array)),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("number", DataType::Utf8, false)),
                Arc::new(arrow_schema::Field::new(
                    "from_base",
                    DataType::Int32,
                    false,
                )),
                Arc::new(arrow_schema::Field::new("to_base", DataType::Int32, false)),
            ],
            number_rows: 3,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let string_array = result_array.as_any().downcast_ref::<StringArray>().unwrap();

            assert_eq!(string_array.value(0), "255");
            assert_eq!(string_array.value(1), "16");
            assert_eq!(string_array.value(2), "10");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_conv_base_binary_to_hex() {
        let func = ConvBaseFunc::new();

        let number_array = StringArray::from(vec!["1010", "1111"]);
        let from_base_array = StringArray::from(vec!["2", "2"]);
        let to_base_array = StringArray::from(vec!["16", "16"]);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(number_array)),
                ColumnarValue::Array(Arc::new(from_base_array)),
                ColumnarValue::Array(Arc::new(to_base_array)),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("number", DataType::Utf8, false)),
                Arc::new(arrow_schema::Field::new("from_base", DataType::Utf8, false)),
                Arc::new(arrow_schema::Field::new("to_base", DataType::Utf8, false)),
            ],
            number_rows: 2,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();
        if let ColumnarValue::Array(result_array) = result {
            let string_array = result_array.as_any().downcast_ref::<StringArray>().unwrap();

            assert_eq!(string_array.value(0), "a");
            assert_eq!(string_array.value(1), "f");
        } else {
            panic!("Expected array result");
        }
    }
}
