use arrow_schema::{Field, FieldRef, Fields};
use datafusion::arrow::array::{Array, ArrayRef, ListArray, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

use crate::functions::util::{get_all_args_as_type, validate_arg_count};
use crate::streamling_user_err;

/// Zips multiple arrays into an array of structs (rows).
///
/// Takes 2-4 arrays and combines them element-wise into an array of structs,
/// where each struct contains one element from each input array at the same index.
/// All input arrays must have the same length.
///
/// # Arguments
/// * `arr1` - First array
/// * `arr2` - Second array  
/// * `arr3` - Optional third array
/// * `arr4` - Optional fourth array
///
/// # Returns
/// An array of structs where each struct contains elements from all input arrays
///
/// # Examples
/// * `zip_arrays([1,2,3], [4,5,6])` returns `[(1,4), (2,5), (3,6)]`
/// * `zip_arrays([1,2,3], ['a','b','c'])` returns `[(1,'a'), (2,'b'), (3,'c')]`
///
/// # Usage in SQL
/// ```sql
/// SELECT contract, id, value
/// FROM source
/// CROSS JOIN UNNEST(_gs_zip_arrays(ids, values)) AS t (id, value)
/// ```
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ZipArraysFunc {
    signature: Signature,
}

impl Default for ZipArraysFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ZipArraysFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ZipArraysFunc {
    fn name(&self) -> &str {
        "_gs_zip_arrays"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let mut field_types = Vec::with_capacity(arg_types.len());

        for (i, arg_type) in arg_types.iter().enumerate() {
            if let DataType::List(field) = arg_type {
                // The execution path builds these struct fields as nullable (zipped
                // elements may be absent); df54 asserts produced == promised, so the
                // declared nullability must match the produced one (always `true`).
                field_types.push(Field::new(
                    format!("f{}", i),
                    field.data_type().clone(),
                    true,
                ));
            } else {
                return Err(streamling_user_err!(
                    "All arguments to {} must be arrays",
                    self.name()
                )
                .into());
            }
        }

        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(field_types)),
            false,
        ))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let mut field_types = Vec::with_capacity(args.arg_fields.len());

        for (i, field) in args.arg_fields.iter().enumerate() {
            if let DataType::List(list_field) = field.data_type() {
                // Match the produced struct field nullability (always `true`) — see note
                // in `return_type`; df54 asserts the produced array type equals this.
                field_types.push(Field::new(
                    format!("f{}", i),
                    list_field.data_type().clone(),
                    true,
                ));
            } else {
                return Err(streamling_user_err!(
                    "All arguments to {} must be arrays",
                    self.name()
                )
                .into());
            }
        }

        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(field_types)),
                false,
            ))),
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        validate_arg_count(&args, 2, Some(4), self.name())?;

        let arrays = get_all_args_as_type::<ListArray>(&args, "list array")?;

        let num_rows = arrays[0].len();

        // Build output offsets - one for each input row plus the final offset
        let mut offsets = vec![0i32];
        let mut all_struct_arrays = Vec::new();

        for row_idx in 0..num_rows {
            if arrays.iter().any(|arr| arr.is_null(row_idx)) {
                // For null rows, we still need to advance the offset but add no data
                offsets.push(*offsets.last().unwrap());
                continue;
            }

            let inner_arrays: Vec<ArrayRef> = arrays.iter().map(|arr| arr.value(row_idx)).collect();

            let inner_len = inner_arrays[0].len();
            if !inner_arrays.iter().all(|arr| arr.len() == inner_len) {
                return Err(streamling_user_err!(
                    "Arrays must have the same length when using _gs_zip_arrays!"
                )
                .into());
            }

            let field_types: Vec<Field> = inner_arrays
                .iter()
                .enumerate()
                .map(|(i, arr)| Field::new(format!("f{}", i), arr.data_type().clone(), true))
                .collect();

            let struct_array = StructArray::try_new(Fields::from(field_types), inner_arrays, None)?;

            all_struct_arrays.push(struct_array);
            offsets.push(offsets.last().unwrap() + inner_len as i32);
        }

        // Concatenate all struct arrays into a single values array
        let values = if !all_struct_arrays.is_empty() {
            let refs: Vec<&dyn Array> = all_struct_arrays.iter().map(|a| a as &dyn Array).collect();
            Arc::new(datafusion::arrow::compute::concat(&refs)?)
        } else {
            // Create empty struct array with proper fields
            let field_types: Vec<Field> = arrays
                .iter()
                .enumerate()
                .map(|(i, arr)| Field::new(format!("f{}", i), arr.value_type(), true))
                .collect();
            datafusion::arrow::array::new_empty_array(&DataType::Struct(Fields::from(field_types)))
        };

        let list_field = Field::new("item", values.data_type().clone(), false);
        let list_array = ListArray::try_new(
            Arc::new(list_field),
            OffsetBuffer::new(offsets.into()),
            values,
            None,
        )?;

        Ok(ColumnarValue::Array(Arc::new(list_array)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, ListBuilder, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_zip_arrays() {
        let func = ZipArraysFunc::new();

        let mut list_builder1 = ListBuilder::new(datafusion::arrow::array::Int32Builder::new());
        list_builder1.values().append_value(1);
        list_builder1.values().append_value(2);
        list_builder1.values().append_value(3);
        list_builder1.append(true);
        let array1 = Arc::new(list_builder1.finish());

        let mut list_builder2 = ListBuilder::new(datafusion::arrow::array::Int32Builder::new());
        list_builder2.values().append_value(4);
        list_builder2.values().append_value(5);
        list_builder2.values().append_value(6);
        list_builder2.append(true);
        let array2 = Arc::new(list_builder2.finish());

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(array1.clone()),
                ColumnarValue::Array(array2.clone()),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arr1", array1.data_type().clone(), false)),
                Arc::new(Field::new("arr2", array2.data_type().clone(), false)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Null, false)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let list_array = result_array.as_any().downcast_ref::<ListArray>().unwrap();

            assert_eq!(list_array.len(), 1);
            let value = list_array.value(0);
            let struct_array = value.as_any().downcast_ref::<StructArray>().unwrap();

            assert_eq!(struct_array.len(), 3);
            assert_eq!(struct_array.num_columns(), 2);

            let col0 = struct_array
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col0.value(0), 1);
            assert_eq!(col0.value(1), 2);
            assert_eq!(col0.value(2), 3);

            let col1 = struct_array
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col1.value(0), 4);
            assert_eq!(col1.value(1), 5);
            assert_eq!(col1.value(2), 6);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_zip_arrays_with_different_types() {
        let func = ZipArraysFunc::new();

        let mut list_builder1 = ListBuilder::new(datafusion::arrow::array::Int32Builder::new());
        list_builder1.values().append_value(1);
        list_builder1.values().append_value(2);
        list_builder1.values().append_value(3);
        list_builder1.append(true);
        let array1 = Arc::new(list_builder1.finish());

        let mut list_builder2 = ListBuilder::new(datafusion::arrow::array::StringBuilder::new());
        list_builder2.values().append_value("a");
        list_builder2.values().append_value("b");
        list_builder2.values().append_value("c");
        list_builder2.append(true);
        let array2 = Arc::new(list_builder2.finish());

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(array1.clone()),
                ColumnarValue::Array(array2.clone()),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arr1", array1.data_type().clone(), false)),
                Arc::new(Field::new("arr2", array2.data_type().clone(), false)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Null, false)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(result_array) = result {
            let list_array = result_array.as_any().downcast_ref::<ListArray>().unwrap();

            assert_eq!(list_array.len(), 1);
            let value = list_array.value(0);
            let struct_array = value.as_any().downcast_ref::<StructArray>().unwrap();

            assert_eq!(struct_array.len(), 3);
            assert_eq!(struct_array.num_columns(), 2);

            let col0 = struct_array
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col0.value(0), 1);
            assert_eq!(col0.value(1), 2);
            assert_eq!(col0.value(2), 3);

            let col1 = struct_array
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(col1.value(0), "a");
            assert_eq!(col1.value(1), "b");
            assert_eq!(col1.value(2), "c");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_not_zip_arrays_with_different_length() {
        let func = ZipArraysFunc::new();

        let mut list_builder1 = ListBuilder::new(datafusion::arrow::array::StringBuilder::new());
        list_builder1.values().append_value("a");
        list_builder1.values().append_value("b");
        list_builder1.values().append_value("c");
        list_builder1.append(true);
        let array1 = Arc::new(list_builder1.finish());

        let mut list_builder2 = ListBuilder::new(datafusion::arrow::array::StringBuilder::new());
        list_builder2.values().append_value("x");
        list_builder2.values().append_value("y");
        list_builder2.append(true);
        let array2 = Arc::new(list_builder2.finish());

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(array1.clone()),
                ColumnarValue::Array(array2.clone()),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arr1", array1.data_type().clone(), false)),
                Arc::new(Field::new("arr2", array2.data_type().clone(), false)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Null, false)),
            config_options: ::std::sync::Arc::new(::datafusion::config::ConfigOptions::default()),
        };

        let result = func.invoke_with_args(args);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("Arrays must have the same length when using _gs_zip_arrays!")
        );
    }
}
