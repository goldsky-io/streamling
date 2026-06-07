use crate::{streamling_user_bail, streamling_user_err};
use datafusion::arrow::array::LargeStringArray;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, ListArray, StringArray, StringBuilder,
    StructArray, UInt8Array,
};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use std::sync::Arc;

/// Extracts an array from ColumnarValue, converting scalars to arrays if needed
pub fn get_array_from_arg(arg: &ColumnarValue) -> Result<ArrayRef> {
    match arg {
        ColumnarValue::Array(array) => Ok(array.clone()),
        ColumnarValue::Scalar(scalar) => scalar.to_array(),
    }
}

/// Extracts and downcasts an array argument to a specific type
pub fn get_typed_array<T: Array + Clone + 'static>(
    arg: &ColumnarValue,
    arg_name: &str,
) -> Result<Arc<T>> {
    let array = get_array_from_arg(arg)?;
    let typed = array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| streamling_user_err!("{} must be of correct type", arg_name))?;
    Ok(Arc::new(typed.clone()))
}

/// Validates the number of arguments
pub fn validate_arg_count(
    args: &ScalarFunctionArgs,
    min: usize,
    max: Option<usize>,
    function_name: &str,
) -> Result<()> {
    let count = args.args.len();
    match max {
        Some(max_count) if count < min || count > max_count => Err(streamling_user_err!(
            "{} expects {} to {} arguments, got {}",
            function_name,
            min,
            max_count,
            count
        )
        .into()),
        None if count != min => Err(streamling_user_err!(
            "{} expects exactly {} arguments, got {}",
            function_name,
            min,
            count
        )
        .into()),
        _ => Ok(()),
    }
}

/// Helper to get an optional third argument with a default value
pub fn get_optional_arg_or_default<T: Array + Clone + 'static>(
    args: &ScalarFunctionArgs,
    index: usize,
    default_fn: impl FnOnce(usize) -> Arc<T>,
    arg_name: &str,
) -> Result<Arc<T>> {
    if args.args.len() > index {
        get_typed_array(&args.args[index], arg_name)
    } else {
        // Get length from first argument
        let first_array = get_array_from_arg(&args.args[0])?;
        Ok(default_fn(first_array.len()))
    }
}

/// Extracts all arguments as a specific array type
pub fn get_all_args_as_type<T: Array + Clone + 'static>(
    args: &ScalarFunctionArgs,
    type_name: &str,
) -> Result<Vec<Arc<T>>> {
    args.args
        .iter()
        .enumerate()
        .map(|(i, arg)| get_typed_array(arg, &format!("Argument {} ({})", i + 1, type_name)))
        .collect()
}

/// Checks if all arrays have the same length
pub fn validate_same_length<T: Array>(arrays: &[Arc<T>], function_name: &str) -> Result<()> {
    if arrays.is_empty() {
        return Ok(());
    }

    let first_len = arrays[0].len();
    if !arrays.iter().all(|arr| arr.len() == first_len) {
        Err(streamling_user_err!(
            "All arrays must have the same length when using {}!",
            function_name
        )
        .into())
    } else {
        Ok(())
    }
}

/// Common pattern for simple unary string-to-string transformations
pub fn unary_string_to_string<F>(
    args: &ScalarFunctionArgs,
    function_name: &str,
    transform: F,
) -> Result<ColumnarValue>
where
    F: Fn(&str) -> Option<String>,
{
    validate_arg_count(args, 1, None, function_name)?;
    let string_array = get_typed_array::<StringArray>(&args.args[0], "string")?;

    let result_values: Vec<Option<String>> = (0..string_array.len())
        .map(|i| {
            if string_array.is_null(i) {
                None
            } else {
                transform(string_array.value(i))
            }
        })
        .collect();

    Ok(ColumnarValue::Array(Arc::new(StringArray::from(
        result_values,
    ))))
}

/// Common pattern for unary string-to-binary transformations
pub fn unary_string_to_binary<F>(
    args: &ScalarFunctionArgs,
    function_name: &str,
    transform: F,
) -> Result<ColumnarValue>
where
    F: Fn(&str) -> Vec<u8>,
{
    validate_arg_count(args, 1, None, function_name)?;
    let string_array = get_typed_array::<StringArray>(&args.args[0], "string")?;

    let mut builder = BinaryBuilder::new();

    for i in 0..string_array.len() {
        if string_array.is_null(i) {
            builder.append_value([]);
        } else {
            let bytes = transform(string_array.value(i));
            builder.append_value(&bytes);
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

/// Common pattern for building string arrays from iteration
pub fn build_string_array<F>(len: usize, builder_fn: F) -> Result<ArrayRef>
where
    F: Fn(usize, &mut StringBuilder) -> Result<bool>,
{
    let mut builder = StringBuilder::new();

    for i in 0..len {
        let is_valid = builder_fn(i, &mut builder)?;
        if !is_valid {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Helper for extracting a single typed array argument
pub fn get_single_arg<T: Array + Clone + 'static>(
    args: &ScalarFunctionArgs,
    function_name: &str,
    arg_type: &str,
) -> Result<Arc<T>> {
    validate_arg_count(args, 1, None, function_name)?;
    get_typed_array(&args.args[0], arg_type)
}

/// Helper for extracting two typed array arguments
pub fn get_two_args<T1: Array + Clone + 'static, T2: Array + Clone + 'static>(
    args: &ScalarFunctionArgs,
    function_name: &str,
    arg1_type: &str,
    arg2_type: &str,
) -> Result<(Arc<T1>, Arc<T2>)> {
    validate_arg_count(args, 2, None, function_name)?;
    let arg1 = get_typed_array(&args.args[0], arg1_type)?;
    let arg2 = get_typed_array(&args.args[1], arg2_type)?;
    Ok((arg1, arg2))
}

/// Helper for extracting three typed array arguments
pub fn get_three_args<
    T1: Array + Clone + 'static,
    T2: Array + Clone + 'static,
    T3: Array + Clone + 'static,
>(
    args: &ScalarFunctionArgs,
    function_name: &str,
    arg1_type: &str,
    arg2_type: &str,
    arg3_type: &str,
) -> Result<(Arc<T1>, Arc<T2>, Arc<T3>)> {
    validate_arg_count(args, 3, None, function_name)?;
    let arg1 = get_typed_array(&args.args[0], arg1_type)?;
    let arg2 = get_typed_array(&args.args[1], arg2_type)?;
    let arg3 = get_typed_array(&args.args[2], arg3_type)?;
    Ok((arg1, arg2, arg3))
}

/// Common pattern for unary binary-to-string transformations
pub fn unary_binary_to_string<F>(
    args: &ScalarFunctionArgs,
    function_name: &str,
    transform: F,
) -> Result<ColumnarValue>
where
    F: Fn(&[u8]) -> String,
{
    validate_arg_count(args, 1, None, function_name)?;
    let binary_array = get_typed_array::<BinaryArray>(&args.args[0], "bytes")?;

    let result_values: Vec<Option<String>> = (0..binary_array.len())
        .map(|i| {
            if binary_array.is_null(i) {
                None
            } else {
                Some(transform(binary_array.value(i)))
            }
        })
        .collect();

    Ok(ColumnarValue::Array(Arc::new(StringArray::from(
        result_values,
    ))))
}

/// Downcasts an ArrayRef to a specific array type with error handling
pub fn downcast_array<'a, T: Array + 'static>(
    array: &'a ArrayRef,
    expected_type: &str,
) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| streamling_user_err!("Expected {}", expected_type).into())
}

/// Extracts a column from a struct array and downcasts it to the expected type
pub fn get_struct_column<'a, T: Array + 'static>(
    struct_array: &'a StructArray,
    column_index: usize,
    expected_type: &str,
) -> Result<&'a T> {
    let column = struct_array.column(column_index);
    downcast_array(column, expected_type)
}

/// Extracts multiple columns from a struct array as a tuple
pub fn get_struct_columns_2<'a, T1: Array + 'static, T2: Array + 'static>(
    struct_array: &'a StructArray,
    type1: &str,
    type2: &str,
) -> Result<(&'a T1, &'a T2)> {
    let col1 = get_struct_column::<T1>(struct_array, 0, type1)?;
    let col2 = get_struct_column::<T2>(struct_array, 1, type2)?;
    Ok((col1, col2))
}

/// Downcasts a field of a StructArray by name into a concrete Arrow array type.
pub fn struct_column_as<'a, T: Array + 'static>(
    struct_array: &'a StructArray,
    field_name: &str,
    fn_name: &str,
) -> Result<&'a T> {
    struct_array
        .column_by_name(field_name)
        .and_then(|a| a.as_any().downcast_ref::<T>())
        .ok_or_else(|| {
            streamling_user_err!("{}: missing or invalid field '{}'", fn_name, field_name).into()
        })
}

/// Gets the values array of a ListArray as a StructArray reference.
pub fn list_values_as_struct<'a>(list: &'a ListArray, fn_name: &str) -> Result<&'a StructArray> {
    list.values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            streamling_user_err!("{}: expected StructArray as list values", fn_name).into()
        })
}

/// Optionally downcasts a field of a StructArray by name into a concrete Arrow array type.
pub fn struct_opt_column_as<'a, T: Array + 'static>(
    struct_array: &'a StructArray,
    field_name: &str,
) -> Option<&'a T> {
    struct_array
        .column_by_name(field_name)
        .and_then(|a| a.as_any().downcast_ref::<T>())
}

/// Convenience getter for optional String at index from a StringArray.
pub fn opt_string(arr: &StringArray, i: usize) -> Option<String> {
    if arr.is_null(i) {
        None
    } else {
        Some(arr.value(i).to_string())
    }
}

/// Convenience getter for optional u8 at index from a UInt8Array.
pub fn opt_u8(arr: &UInt8Array, i: usize) -> Option<u8> {
    if arr.is_null(i) {
        None
    } else {
        Some(arr.value(i))
    }
}

// ===============================
// Scalar extraction utilities
// ===============================

/// Extract a string value from a ColumnarValue scalar (no row index)
/// Useful when working with single scalar values
pub fn extract_string_scalar(col_val: &ColumnarValue) -> Result<String> {
    match col_val {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) => Ok(value.clone()),
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => Ok(String::new()),
        _ => Err(streamling_user_err!("Expected Utf8 scalar").into()),
    }
}

/// Extract a string value from a ColumnarValue at a specific row index
/// Handles both scalar and array cases
pub fn extract_string_scalar_at_index(col_val: &ColumnarValue, row_idx: usize) -> Result<String> {
    match col_val {
        ColumnarValue::Scalar(scalar_value) => match scalar_value {
            ScalarValue::Utf8(Some(value)) => Ok(value.clone()),
            ScalarValue::Utf8(None) => Ok(String::new()),
            ScalarValue::LargeUtf8(Some(value)) => Ok(value.clone()),
            ScalarValue::LargeUtf8(None) => Ok(String::new()),
            _ => Err(streamling_user_err!("Expected Utf8 scalar, got: {:?}", scalar_value).into()),
        },
        ColumnarValue::Array(array) => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                if row_idx >= arr.len() {
                    streamling_user_bail!(
                        "Row index {} out of bounds (len: {})",
                        row_idx,
                        arr.len()
                    );
                }
                if arr.is_null(row_idx) {
                    Ok(String::new())
                } else {
                    Ok(arr.value(row_idx).to_string())
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                if row_idx >= arr.len() {
                    streamling_user_bail!(
                        "Row index {} out of bounds (len: {})",
                        row_idx,
                        arr.len()
                    );
                }
                if arr.is_null(row_idx) {
                    Ok(String::new())
                } else {
                    Ok(arr.value(row_idx).to_string())
                }
            } else {
                Err(streamling_user_err!(
                    "Expected StringArray/LargeStringArray, got: {:?}",
                    array.data_type()
                )
                .into())
            }
        }
    }
}

/// Returns an Option<String> from a UTF-8 argument at a given row.
/// - Supports both Utf8 and LargeUtf8 scalars (broadcast to all rows)
/// - Supports both StringArray and LargeStringArray arrays
/// - Returns Ok(None) if the value is NULL at the row
pub fn get_utf8_arg_at_opt(
    args: &ScalarFunctionArgs,
    index: usize,
    row_idx: usize,
    arg_name: &str,
) -> Result<Option<String>> {
    if index >= args.args.len() {
        streamling_user_bail!("Missing argument '{}' at index {}", arg_name, index);
    }
    match &args.args[index] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(Some(s.clone())),
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => Ok(None),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Ok(Some(s.clone())),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => Ok(None),
        ColumnarValue::Scalar(other) => Err(streamling_user_err!(
            "{} must be Utf8/LargeUtf8 scalar, got {:?}",
            arg_name,
            other
        )
        .into()),
        ColumnarValue::Array(array) => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                if row_idx >= arr.len() {
                    streamling_user_bail!(
                        "{} index {} out of bounds (len: {})",
                        arg_name,
                        row_idx,
                        arr.len()
                    );
                }
                if arr.is_null(row_idx) {
                    Ok(None)
                } else {
                    Ok(Some(arr.value(row_idx).to_string()))
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                if row_idx >= arr.len() {
                    streamling_user_bail!(
                        "{} index {} out of bounds (len: {})",
                        arg_name,
                        row_idx,
                        arr.len()
                    );
                }
                if arr.is_null(row_idx) {
                    Ok(None)
                } else {
                    Ok(Some(arr.value(row_idx).to_string()))
                }
            } else {
                Err(streamling_user_err!(
                    "{} must be StringArray/LargeStringArray or Utf8 scalar, got {:?}",
                    arg_name,
                    array.data_type()
                )
                .into())
            }
        }
    }
}

/// Extract a u8 value from a ColumnarValue at a specific row index
/// Handles both scalar and array cases
pub fn extract_u8_scalar_at_index(col_val: &ColumnarValue, row_idx: usize) -> Result<u8> {
    match col_val {
        ColumnarValue::Scalar(scalar_value) => match scalar_value {
            ScalarValue::UInt8(Some(value)) => Ok(*value),
            _ => Err(streamling_user_err!("Expected UInt8 scalar, got: {:?}", scalar_value).into()),
        },
        ColumnarValue::Array(array) => {
            let uint8_array = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                streamling_user_err!("Expected UInt8Array, got: {:?}", array.data_type())
            })?;

            if row_idx >= uint8_array.len() {
                streamling_user_bail!(
                    "Row index {} out of bounds (len: {})",
                    row_idx,
                    uint8_array.len()
                );
            }

            if uint8_array.is_null(row_idx) {
                Err(streamling_user_err!("UInt8 value is null at row {}", row_idx).into())
            } else {
                Ok(uint8_array.value(row_idx))
            }
        }
    }
}

/// Extract a list of strings from a ColumnarValue scalar
/// Expects the scalar to be a ScalarValue::List containing strings
pub fn extract_string_list_scalar(col_val: &ColumnarValue) -> Result<Vec<String>> {
    match col_val {
        ColumnarValue::Scalar(ScalarValue::List(arr)) => {
            let mut result = Vec::new();
            if let Some(string_array) = arr.as_any().downcast_ref::<StringArray>() {
                for i in 0..string_array.len() {
                    if !string_array.is_null(i) {
                        result.push(string_array.value(i).to_string());
                    }
                }
            }
            Ok(result)
        }
        _ => Ok(Vec::new()),
    }
}

/// Extract a list of strings from a ListArray value (ArrayRef)
/// Used when extracting individual list elements from a ListArray
pub fn extract_string_list_from_array_ref(list_value: ArrayRef) -> Result<Vec<String>> {
    if let Some(string_array) = list_value.as_any().downcast_ref::<StringArray>() {
        let mut result = Vec::new();
        for i in 0..string_array.len() {
            if !string_array.is_null(i) {
                result.push(string_array.value(i).to_string());
            }
        }
        Ok(result)
    } else {
        Ok(Vec::new())
    }
}

/// Extract a list of strings from a ColumnarValue at a specific row index
/// Handles arrays of lists (ListArray) and scalar lists
pub fn extract_string_list_at_index(
    col_val: &ColumnarValue,
    row_idx: usize,
) -> Result<Vec<String>> {
    match col_val {
        ColumnarValue::Array(array) => {
            if let Some(list_array) = array.as_any().downcast_ref::<ListArray>() {
                if row_idx >= list_array.len() {
                    streamling_user_bail!(
                        "Row index {} out of bounds (len: {})",
                        row_idx,
                        list_array.len()
                    );
                }

                if list_array.is_null(row_idx) {
                    return Ok(vec![]); // Handle null list
                }

                let list_value = list_array.value(row_idx);
                extract_string_list_from_array_ref(list_value)
            } else {
                Err(streamling_user_err!(
                    "Expected ListArray at row {}, got: {:?}",
                    row_idx,
                    array.data_type()
                )
                .into())
            }
        }
        ColumnarValue::Scalar(_) => {
            Err(streamling_user_err!("Expected Array for string list, got Scalar").into())
        }
    }
}
