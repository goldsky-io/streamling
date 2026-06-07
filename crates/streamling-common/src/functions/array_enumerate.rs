//! The `array_enumerate` function pairs each element in an array with its 0-based index.
//!
//! This function takes a list/array column and returns a new list where each element
//! is a struct containing both the original value and its index position.
//!
//! ## Usage
//!
//! ```sql
//! SELECT array_enumerate(my_array_column) FROM table_name;
//! ```
//!
//! ## Example
//!
//! Given a table with an array column:
//! ```sql
//! -- Input: transactions = [tx1, tx2, tx3]
//! SELECT unnest(array_enumerate(transactions)) as indexed_tx FROM blocks;
//!
//! -- Output:
//! -- indexed_tx.index | indexed_tx.value
//! -- 0               | tx1
//! -- 1               | tx2
//! -- 2               | tx3
//! ```

use arrow_schema::{Field as SchemaField, FieldRef};
use datafusion::arrow::array::{Array, ArrayRef, Int64Builder, ListArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use crate::{streamling_err, streamling_user_bail, streamling_user_err};

#[derive(Debug)]
pub struct ArrayEnumerateFunc {
    signature: Signature,
}

impl Default for ArrayEnumerateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayEnumerateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }

    fn create_struct_fields(element_type: DataType) -> Fields {
        Fields::from(vec![
            Field::new("index", DataType::Int64, false),
            Field::new("value", element_type, true),
        ])
    }
}

impl ScalarUDFImpl for ArrayEnumerateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_enumerate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(streamling_err!("return_field_from_args should be called instead").into())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 1 {
            streamling_user_bail!("array_enumerate function expects exactly 1 argument");
        }

        let input_field = &args.arg_fields[0];
        let input_type = input_field.data_type();

        // Extract the inner element type from the list
        let element_type = match input_type {
            DataType::List(field) => field.data_type().clone(),
            _ => {
                streamling_user_bail!(
                    "array_enumerate function expects a list/array as input, got: {:?}",
                    input_type
                );
            }
        };

        // Create struct type with index and value
        let struct_fields = Self::create_struct_fields(element_type);

        let return_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(struct_fields),
            false,
        )));

        Ok(SchemaField::new(self.name(), return_type, false).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let start_time = Instant::now();

        if args.args.len() != 1 {
            streamling_user_bail!("{} function expects exactly 1 argument", self.name());
        }

        let input_array = match &args.args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(_) => {
                streamling_user_bail!("{} function requires array input", self.name());
            }
        };

        let list_array = input_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| streamling_user_err!("Input must be a list array"))?;

        // Zero-copy optimization: reuse the flattened values buffer from the input list
        let values_array = list_array.values();
        let total_elements = values_array.len();

        // Build index array efficiently using builder
        let mut index_builder = Int64Builder::with_capacity(total_elements);
        let offsets = list_array.value_offsets();

        // Generate indices for each row based on offsets
        for window in offsets.windows(2) {
            let row_len = (window[1] - window[0]) as i64;
            for i in 0..row_len {
                index_builder.append_value(i);
            }
        }
        let indices_array = Arc::new(index_builder.finish()) as ArrayRef;

        // Create struct array that combines (index, value) - zero copy for values
        let struct_fields = Self::create_struct_fields(values_array.data_type().clone());
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("index", DataType::Int64, false)),
                indices_array,
            ),
            (
                Arc::new(Field::new("value", values_array.data_type().clone(), true)),
                values_array.clone(),
            ),
        ]);

        // Reuse offset and null metadata from the original ListArray
        let list_field = Arc::new(Field::new("item", DataType::Struct(struct_fields), false));

        let result_list = ListArray::new(
            list_field,
            list_array.offsets().clone(), // zero-copy reuse of offsets
            Arc::new(struct_array),
            list_array.nulls().cloned(), // reuse null bitmap
        );

        let elapsed_ms = start_time.elapsed().as_millis();
        debug!(
            "Processed {} enumerated items in {} ms",
            total_elements, elapsed_ms
        );

        Ok(ColumnarValue::Array(Arc::new(result_list)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::OffsetBuffer;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::logical_expr::ScalarFunctionArgs;
    use std::sync::Arc;

    #[test]
    fn test_array_enumerate_string_array() {
        let func = ArrayEnumerateFunc::new();

        // Create test data: [["a", "b"], ["x", "y", "z"]] manually (JSON conversion is too complex for nested arrays)
        let values = vec!["a", "b", "x", "y", "z"];
        let string_array = Arc::new(StringArray::from(values));
        let offsets = vec![0i32, 2, 5];
        let offsets_buffer = OffsetBuffer::new(offsets.into());
        let field = Arc::new(Field::new("item", DataType::Utf8, false));
        let list_array = Arc::new(ListArray::new(field, offsets_buffer, string_array, None));

        let return_field = Field::new(
            "array_enumerate",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("index", DataType::Int64, false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                false,
            ))),
            false,
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(list_array)],
            arg_fields: vec![
                Field::new(
                    "list",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                    false,
                )
                .into(),
            ],
            number_rows: 2,
            return_field: return_field.into(),
        };

        let result = func.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let result_list = result_array.as_any().downcast_ref::<ListArray>().unwrap();

            // Check first row: ["a", "b"] -> [(0, "a"), (1, "b")]
            let first_row = result_list.value(0);
            let first_struct = first_row.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(first_struct.len(), 2);

            let first_indices = first_struct
                .column_by_name("index")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let first_values = first_struct
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            assert_eq!(first_indices.value(0), 0);
            assert_eq!(first_indices.value(1), 1);
            assert_eq!(first_values.value(0), "a");
            assert_eq!(first_values.value(1), "b");

            // Check second row: ["x", "y", "z"] -> [(0, "x"), (1, "y"), (2, "z")]
            let second_row = result_list.value(1);
            let second_struct = second_row.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(second_struct.len(), 3);

            let second_indices = second_struct
                .column_by_name("index")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let second_values = second_struct
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            assert_eq!(second_indices.value(0), 0);
            assert_eq!(second_indices.value(1), 1);
            assert_eq!(second_indices.value(2), 2);
            assert_eq!(second_values.value(0), "x");
            assert_eq!(second_values.value(1), "y");
            assert_eq!(second_values.value(2), "z");
        } else {
            panic!("Expected array result");
        }
    }
}
