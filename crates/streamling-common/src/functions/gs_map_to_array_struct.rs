use datafusion::arrow::array::{
    Array, ArrayRef, ListArray, MapArray, StringArray, StringBuilder, StructArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use std::sync::Arc;

use crate::{streamling_user_bail, streamling_user_err};

/// Converts a Map to a List of structs
///
/// Takes a Map<Utf8, Utf8> and converts it to List<Struct<key: Utf8, value: Utf8>>
///
/// # Arguments
/// * `map_input` - A Map with key-value pairs
///
/// # Returns
/// A List of Struct with key and value fields
///
/// # Examples
/// * `gs_map_to_array_struct(params_map)` converts Map to array of key-value structs
pub fn gs_map_to_array_struct_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        streamling_user_bail!("gs_map_to_array_struct expects exactly 1 argument");
    }

    match &args[0] {
        // Handle scalar case - not really applicable for Map, treat as null
        ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
        // Handle array case
        ColumnarValue::Array(array) => {
            let map_array = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| streamling_user_err!("Expected MapArray input"))?;

            let num_rows = map_array.len();
            let mut result_lists = Vec::new();

            for i in 0..num_rows {
                let converted = convert_map_to_list_struct(map_array, i)?;
                result_lists.push(Arc::new(converted) as ArrayRef);
            }

            // Concatenate all results
            if result_lists.is_empty() {
                let empty = datafusion::arrow::array::new_empty_array(&get_output_type());
                return Ok(ColumnarValue::Array(empty));
            }

            if result_lists.len() == 1 {
                return Ok(ColumnarValue::Array(result_lists[0].clone()));
            }

            let list_refs: Vec<&dyn Array> = result_lists
                .iter()
                .map(|s| s.as_ref() as &dyn Array)
                .collect();
            let combined = datafusion::arrow::compute::concat(&list_refs)?;

            Ok(ColumnarValue::Array(combined))
        }
    }
}

/// Convert a Map entry to a List<Struct>
fn convert_map_to_list_struct(map_array: &MapArray, row_idx: usize) -> Result<ListArray> {
    // Get the struct entries for this map row
    let entries = map_array.entries();
    let struct_array = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| streamling_user_err!("Map entries must be StructArray"))?;

    // Get the offset range for this row
    let offsets = map_array.offsets();
    let start = offsets[row_idx] as usize;
    let end = offsets[row_idx + 1] as usize;

    // Extract keys and values for this range
    let keys_column = struct_array.column(0);
    let values_column = struct_array.column(1);

    let keys_array = keys_column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| streamling_user_err!("Map keys must be Utf8"))?;

    let values_array = values_column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| streamling_user_err!("Map values must be Utf8"))?;

    // Build the struct array for the list
    let mut key_builder = StringBuilder::new();
    let mut value_builder = StringBuilder::new();

    for i in start..end {
        if keys_array.is_null(i) {
            key_builder.append_null();
        } else {
            key_builder.append_value(keys_array.value(i));
        }

        if values_array.is_null(i) {
            value_builder.append_null();
        } else {
            value_builder.append_value(values_array.value(i));
        }
    }

    let key_array = Arc::new(key_builder.finish());
    let value_array = Arc::new(value_builder.finish());

    let struct_fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]);

    let struct_array =
        StructArray::try_new(struct_fields.clone(), vec![key_array, value_array], None)?;

    // Create ListArray with single entry containing all structs
    let list_offsets = OffsetBuffer::new(vec![0i32, struct_array.len() as i32].into());
    let list_field = Field::new("item", DataType::Struct(struct_fields), false);

    let list_array = ListArray::new(
        Arc::new(list_field),
        list_offsets,
        Arc::new(struct_array),
        None,
    );

    Ok(list_array)
}

/// Get the output schema for the UDF
fn get_output_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        false,
    )))
}

/// Get the expected input schema
fn get_input_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            false,
        )),
        false, // keys are not sorted
    )
}

/// Creates the gs_map_to_array_struct UDF
pub fn create_gs_map_to_array_struct_udf() -> ScalarUDF {
    create_udf(
        "_gs_map_to_array_struct",
        vec![get_input_type()],
        get_output_type(),
        Volatility::Immutable,
        Arc::new(gs_map_to_array_struct_impl),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, ListArray, MapArray, StringArray, StructArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{DataType, Field, Fields};
    use std::sync::Arc;

    #[test]
    fn test_convert_map_to_array_struct() {
        // Create a Map with some key-value pairs
        let mut key_builder = StringBuilder::new();
        let mut value_builder = StringBuilder::new();

        key_builder.append_value("amount");
        value_builder.append_value("100");
        key_builder.append_value("recipient");
        value_builder.append_value("0x123");

        let key_array = Arc::new(key_builder.finish());
        let value_array = Arc::new(value_builder.finish());

        let struct_array = StructArray::try_new(
            Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ]),
            vec![key_array, value_array],
            None,
        )
        .unwrap();

        let entry_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            false,
        );

        let offsets = OffsetBuffer::new(vec![0i32, 2i32].into());
        let map_array = MapArray::new(Arc::new(entry_field), offsets, struct_array, None, false);

        // Test conversion - pass Map directly
        let args = vec![ColumnarValue::Array(Arc::new(map_array))];
        let result = gs_map_to_array_struct_impl(&args).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                assert_eq!(arr.len(), 1);
                let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap();

                // Check that we got a list
                assert!(matches!(list_arr.data_type(), DataType::List(_)));

                // Check the list contains the correct values
                let list_values = list_arr.value(0);
                let struct_arr = list_values.as_any().downcast_ref::<StructArray>().unwrap();
                assert_eq!(struct_arr.len(), 2); // 2 key-value pairs

                // Check the keys
                let key_col = struct_arr.column_by_name("key").unwrap();
                let key_arr = key_col.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(key_arr.value(0), "amount");
                assert_eq!(key_arr.value(1), "recipient");

                // Check the values
                let value_col = struct_arr.column_by_name("value").unwrap();
                let value_arr = value_col.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(value_arr.value(0), "100");
                assert_eq!(value_arr.value(1), "0x123");
            }
            _ => panic!("Expected array result"),
        }
    }
}
