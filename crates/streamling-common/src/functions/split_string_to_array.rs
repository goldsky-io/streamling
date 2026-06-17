use arrow_schema::FieldRef;
use datafusion::arrow::array::{Array, ListBuilder, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::sync::Arc;

use crate::functions::util::{get_optional_arg_or_default, get_typed_array, validate_arg_count};

/// Splits a string into an array of substrings based on a separator.
///
/// This function takes a string and splits it into an array of substrings using the specified
/// separator. If no separator is provided, it defaults to splitting on spaces.
///
/// # Arguments
/// * `string` - The string to split
/// * `separator` - Optional separator string (defaults to space if not provided)
///
/// # Returns
/// An array of substrings
///
/// # Examples
/// * `split_string_to_array('1 2 34 5')` returns `['1', '2', '34', '5']`
/// * `split_string_to_array('1-2-5', '-')` returns `['1', '2', '5']`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SplitStringToArrayFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SplitStringToArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SplitStringToArrayFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["_gs_split_string_by".to_string()],
        }
    }
}

impl ScalarUDFImpl for SplitStringToArrayFunc {
    fn name(&self) -> &str {
        "_gs_split_string_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Utf8,
            false,
        ))))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(arrow_schema::Field::new(
            self.name(),
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Utf8,
                false,
            ))),
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        validate_arg_count(&args, 1, Some(2), self.name())?;

        let string_array = get_typed_array::<StringArray>(&args.args[0], "string")?;
        let separator_array = get_optional_arg_or_default(
            &args,
            1,
            |len| Arc::new(StringArray::from(vec![" "; len])),
            "separator",
        )?;

        let mut list_builder = ListBuilder::new(StringBuilder::new());

        (0..string_array.len()).for_each(|i| {
            if string_array.is_null(i) {
                list_builder.append(false);
            } else {
                let string = string_array.value(i);
                let separator = if separator_array.is_null(i) {
                    " "
                } else {
                    separator_array.value(i)
                };

                string.split(separator).for_each(|part| {
                    list_builder.values().append_value(part);
                });
                list_builder.append(true);
            }
        });

        let result_array = list_builder.finish();
        Ok(ColumnarValue::Array(Arc::new(result_array)))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_split_strings_correctly() {
        let func = SplitStringToArrayFunc::new();
        let params = "1 2 34 5";

        let string_array = StringArray::from(vec![params]);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(string_array))],
            arg_fields: vec![Arc::new(arrow_schema::Field::new(
                "string",
                DataType::Utf8,
                false,
            ))],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new(
                "result",
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Utf8,
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
            let string_array = value.as_any().downcast_ref::<StringArray>().unwrap();

            assert_eq!(string_array.len(), 4);
            assert_eq!(string_array.value(0), "1");
            assert_eq!(string_array.value(1), "2");
            assert_eq!(string_array.value(2), "34");
            assert_eq!(string_array.value(3), "5");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_split_strings_correctly_with_custom_separator() {
        let func = SplitStringToArrayFunc::new();
        let params = "1-2-5";
        let separator = "-";

        let string_array = StringArray::from(vec![params]);
        let separator_array = StringArray::from(vec![separator]);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(string_array)),
                ColumnarValue::Array(Arc::new(separator_array)),
            ],
            arg_fields: vec![
                Arc::new(arrow_schema::Field::new("string", DataType::Utf8, false)),
                Arc::new(arrow_schema::Field::new("separator", DataType::Utf8, false)),
            ],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new(
                "result",
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Utf8,
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
            let string_array = value.as_any().downcast_ref::<StringArray>().unwrap();

            assert_eq!(string_array.len(), 3);
            assert_eq!(string_array.value(0), "1");
            assert_eq!(string_array.value(1), "2");
            assert_eq!(string_array.value(2), "5");
        } else {
            panic!("Expected array result");
        }
    }
}
