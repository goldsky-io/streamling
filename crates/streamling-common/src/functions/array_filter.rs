use crate::error::ResultExt;
use crate::functions::util::{get_array_from_arg, list_values_as_struct, validate_arg_count};
use crate::{streamling_err, streamling_user_bail, streamling_user_err};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{Field as SchemaField, FieldRef};
use datafusion::arrow::array::{
    Array, BooleanArray, BooleanBuilder, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, ListArray, NullArray, StringArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::compute as arrow_compute;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// array_filter(list<struct>, field_name_utf8, value_utf8) -> list<struct>
///
/// Filters each list, keeping only elements where the named struct field (Utf8)
/// equals the provided value (Utf8), or both are NULL. If the list is null, the result is null.
/// If the field is missing or not Utf8, an error is returned.
#[derive(Debug)]
pub struct ArrayFilterFunc {
    signature: Signature,
}

impl Default for ArrayFilterFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFilterFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

/// Controls the return shape of the shared filter executor
pub enum ArrayFilterReturn {
    All,
    First,
}

/// Shared executor for array filtering. When `return_mode` is `All`, returns a `ListArray` of
/// filtered elements. When `First`, returns a `StructArray` with one element per row (or null).
pub fn eval_array_filter(
    args: &ScalarFunctionArgs,
    function_name: &str,
    return_mode: ArrayFilterReturn,
) -> Result<ColumnarValue> {
    validate_arg_count(args, 3, None, function_name)?;

    // Get input list as ArrayRef
    let input_array = get_array_from_arg(&args.args[0])?;
    let list_array = input_array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| streamling_user_err!("First argument must be a list array"))?;

    // Early return for empty input (avoid arg length checks when number of rows is 0)
    if list_array.len() == 0 {
        let element_struct = list_values_as_struct(list_array, function_name)?;
        let fields = match element_struct.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        };
        let empty_values: Arc<dyn Array> =
            datafusion::arrow::array::new_empty_array(&DataType::Struct(fields));
        let out_field = Arc::new(Field::new("item", empty_values.data_type().clone(), false));
        let out_offsets = arrow_buffer::OffsetBuffer::new(vec![0i32].into());
        let result = ListArray::new(out_field, out_offsets, empty_values, None);
        return Ok(ColumnarValue::Array(Arc::new(result)));
    }

    // field name and value as arrays (scalars will broadcast to batch length)
    let field_name_arr = get_array_from_arg(&args.args[1])?;
    let value_arr = get_array_from_arg(&args.args[2])?;
    let field_names = field_name_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| streamling_user_err!("Second argument (field_name) must be Utf8"))?;
    // Accept Utf8, Bool, numeric, or a NULL array (to filter for NULLs in the field)
    let values_null = value_arr.as_any().downcast_ref::<NullArray>();

    // Accept broadcasting: allow len == 1 to be used for all rows
    let fh_len = field_names.len();
    let val_len = value_arr.len();
    let list_len = list_array.len();
    let field_broadcast = fh_len == 1;
    let value_broadcast = val_len == 1;
    let lengths_ok =
        (fh_len == list_len || field_broadcast) && (val_len == list_len || value_broadcast);
    if !lengths_ok {
        streamling_user_bail!(
            "{}: field_name and value must align with input rows ({} != {} or {} != {})",
            function_name,
            fh_len,
            list_len,
            val_len,
            list_len
        );
    }

    // Values array across all rows
    let element_struct = list_values_as_struct(list_array, function_name)?;

    // Offsets per row
    let in_offsets = list_array.value_offsets();

    // Collect matches per row and global keep indices
    let mut keep_indices: Vec<usize> = Vec::new();

    // Normalized accessors (handle broadcast and nulls once)
    let get_field_name = |row: usize| -> Result<&str> {
        if field_broadcast {
            if field_names.is_null(0) {
                streamling_user_bail!("{}: field_name cannot be null", function_name)
            } else {
                Ok(field_names.value(0))
            }
        } else if field_names.is_null(row) {
            streamling_user_bail!("{}: field_name cannot be null", function_name)
        } else {
            Ok(field_names.value(row))
        }
    };
    let get_value_utf8 = |row: usize| -> Result<Option<&str>> {
        if let Some(values) = value_arr.as_any().downcast_ref::<StringArray>() {
            if value_broadcast {
                Ok(if values.is_null(0) {
                    None
                } else {
                    Some(values.value(0))
                })
            } else {
                Ok(if values.is_null(row) {
                    None
                } else {
                    Some(values.value(row))
                })
            }
        } else if values_null.is_some() {
            Ok(None)
        } else {
            streamling_user_bail!(
                "{}: value type must be Utf8 or NULL to compare with Utf8 field",
                function_name
            )
        }
    };

    for row in 0..list_array.len() {
        let start = in_offsets[row] as usize;
        let end = in_offsets[row + 1] as usize;

        if list_array.is_null(row) {
            continue;
        }

        let field_name = get_field_name(row)?;

        // Locate the field column and branch by its data type
        let col = element_struct.column_by_name(field_name).ok_or_else(|| {
            streamling_user_err!(
                "{}: missing or invalid field '{}'",
                function_name,
                field_name
            )
        })?;

        match col.data_type() {
            DataType::Utf8 => {
                let field_utf8 = col.as_any().downcast_ref::<StringArray>().unwrap();
                let target_opt = get_value_utf8(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(target) => {
                            if !field_utf8.is_null(idx) && field_utf8.value(idx) == target {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field_utf8.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::Boolean => {
                let field = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                let get_val = |r: usize| -> Result<Option<bool>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<BooleanArray>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be Boolean or NULL to compare with Boolean field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(target) => {
                            if !field.is_null(idx) && field.value(idx) == target {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::Int8 => {
                let field = col.as_any().downcast_ref::<Int8Array>().unwrap();
                let get_val = |r: usize| -> Result<Option<i8>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<Int8Array>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be Int8 or NULL to compare with Int8 field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(t) => {
                            if !field.is_null(idx) && field.value(idx) == t {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::Int16 => {
                let field = col.as_any().downcast_ref::<Int16Array>().unwrap();
                let get_val = |r: usize| -> Result<Option<i16>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<Int16Array>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be Int16 or NULL to compare with Int16 field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(t) => {
                            if !field.is_null(idx) && field.value(idx) == t {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::Int32 => {
                let field = col.as_any().downcast_ref::<Int32Array>().unwrap();
                let get_val = |r: usize| -> Result<Option<i32>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<Int32Array>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be Int32 or NULL to compare with Int32 field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(t) => {
                            if !field.is_null(idx) && field.value(idx) == t {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::Int64 => {
                let field = col.as_any().downcast_ref::<Int64Array>().unwrap();
                let get_val = |r: usize| -> Result<Option<i64>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<Int64Array>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be Int64 or NULL to compare with Int64 field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(t) => {
                            if !field.is_null(idx) && field.value(idx) == t {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::UInt8 => {
                let field = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                let get_val = |r: usize| -> Result<Option<u8>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<UInt8Array>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be UInt8 or NULL to compare with UInt8 field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(t) => {
                            if !field.is_null(idx) && field.value(idx) == t {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::UInt16 => {
                let field = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                let get_val = |r: usize| -> Result<Option<u16>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<UInt16Array>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be UInt16 or NULL to compare with UInt16 field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(t) => {
                            if !field.is_null(idx) && field.value(idx) == t {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::UInt32 => {
                let field = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                let get_val = |r: usize| -> Result<Option<u32>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<UInt32Array>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be UInt32 or NULL to compare with UInt32 field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(t) => {
                            if !field.is_null(idx) && field.value(idx) == t {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::UInt64 => {
                let field = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                let get_val = |r: usize| -> Result<Option<u64>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<UInt64Array>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be UInt64 or NULL to compare with UInt64 field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(t) => {
                            if !field.is_null(idx) && field.value(idx) == t {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::Float32 => {
                let field = col.as_any().downcast_ref::<Float32Array>().unwrap();
                let get_val = |r: usize| -> Result<Option<f32>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<Float32Array>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be Float32 or NULL to compare with Float32 field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(t) => {
                            if !field.is_null(idx) && field.value(idx) == t {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            DataType::Float64 => {
                let field = col.as_any().downcast_ref::<Float64Array>().unwrap();
                let get_val = |r: usize| -> Result<Option<f64>> {
                    if let Some(v) = value_arr.as_any().downcast_ref::<Float64Array>() {
                        if value_broadcast {
                            Ok(if v.is_null(0) { None } else { Some(v.value(0)) })
                        } else {
                            Ok(if v.is_null(r) { None } else { Some(v.value(r)) })
                        }
                    } else if values_null.is_some() {
                        Ok(None)
                    } else {
                        streamling_user_bail!(
                            "{}: value type must be Float64 or NULL to compare with Float64 field",
                            function_name
                        )
                    }
                };
                let target_opt = get_val(row)?;
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    match target_opt {
                        Some(t) => {
                            if !field.is_null(idx) && field.value(idx) == t {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                        None => {
                            if field.is_null(idx) {
                                keep_indices.push(idx);
                                if matches!(return_mode, ArrayFilterReturn::First) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            other => {
                streamling_user_bail!(
                    "{}: unsupported field type {:?} for array_filter",
                    function_name,
                    other
                );
            }
        }
    }

    // Build mask from keep_indices
    let total_elems = element_struct.len();
    let mut mask_builder = BooleanBuilder::with_capacity(total_elems);
    let mut keep_iter = keep_indices.iter().copied().peekable();
    for i in 0..total_elems {
        if let Some(&next) = keep_iter.peek()
            && next == i
        {
            mask_builder.append_value(true);
            keep_iter.next();
            continue;
        }
        mask_builder.append_value(false);
    }
    let mask = Arc::new(mask_builder.finish());

    // Apply filter to the values struct
    let filtered_values = arrow_compute::filter(element_struct as &dyn Array, mask.as_ref())
        .streamling_context("arrow filter failed")?;

    // Rebuild ListArray with updated offsets per row, honoring First mode by early-stopping per row
    let mut out_offsets: Vec<i32> = Vec::with_capacity(list_array.len() + 1);
    out_offsets.push(0);
    let mut cursor: i32 = 0;
    let mut idx_pos = 0usize;
    for row in 0..list_array.len() {
        if list_array.is_null(row) {
            out_offsets.push(cursor);
            continue;
        }
        let start = in_offsets[row] as usize;
        let end = in_offsets[row + 1] as usize;
        let mut kept = 0i32;
        for idx in start..end {
            if idx_pos < keep_indices.len() && keep_indices[idx_pos] == idx {
                kept += 1;
                idx_pos += 1;
                if matches!(return_mode, ArrayFilterReturn::First) {
                    while idx_pos < keep_indices.len() && keep_indices[idx_pos] < end {
                        idx_pos += 1;
                    }
                    break;
                }
            }
        }
        cursor += kept;
        out_offsets.push(cursor);
    }

    let out_field = Arc::new(Field::new(
        "item",
        element_struct.data_type().clone(),
        false,
    ));
    let out_offsets = OffsetBuffer::new(out_offsets.into());
    let out_nulls = list_array.nulls().cloned();
    let result = ListArray::new(out_field, out_offsets, filtered_values, out_nulls);
    Ok(ColumnarValue::Array(Arc::new(result)))
}

impl ScalarUDFImpl for ArrayFilterFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_filter"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(streamling_err!("return_field_from_args should be called instead").into())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 3 {
            streamling_user_bail!(
                "array_filter expects 3 arguments (list<struct>, field_name, value)"
            );
        }
        let input_field = &args.arg_fields[0];
        let list_dt = match input_field.data_type() {
            DataType::List(field) => DataType::List(field.clone()),
            other => {
                streamling_user_bail!(
                    "array_filter expects first argument to be a list/array, got {:?}",
                    other
                );
            }
        };
        Ok(Arc::new(SchemaField::new(self.name(), list_dt, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        eval_array_filter(&args, self.name(), ArrayFilterReturn::All)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::OffsetBuffer;
    use datafusion::arrow::array::{Array, Int32Array, ListArray, StringArray, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields};
    use datafusion::logical_expr::ScalarFunctionArgs;
    use std::sync::Arc;

    fn make_test_list_of_structs() -> Arc<ListArray> {
        // Build concatenated struct values across all rows
        // Row 0: 3 items -> kinds ["foo","bar","foo"]
        // Row 1: 2 items -> kinds ["bar","baz"]
        let kinds =
            Arc::new(StringArray::from(vec!["foo", "bar", "foo", "bar", "baz"])) as Arc<dyn Array>;
        let vals = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as Arc<dyn Array>;
        let fields = Fields::from(vec![
            Field::new("kind", DataType::Utf8, true),
            Field::new("val", DataType::Int32, true),
        ]);
        let struct_array = StructArray::try_new(fields.clone(), vec![kinds, vals], None).unwrap();

        // Offsets per row: [0, 3, 5]
        let offsets = OffsetBuffer::new(vec![0i32, 3, 5].into());
        let list_field = Arc::new(Field::new("item", DataType::Struct(fields), false));
        Arc::new(ListArray::new(
            list_field,
            offsets,
            Arc::new(struct_array),
            None,
        ))
    }

    #[test]
    fn test_array_filter_matches_and_empty() {
        let func = ArrayFilterFunc::new();
        let list_array = make_test_list_of_structs();

        // field_name and value arrays length must equal number of rows (2)
        let field_names = Arc::new(StringArray::from(vec!["kind", "kind"])) as Arc<dyn Array>;
        let values = Arc::new(StringArray::from(vec!["foo", "foo"])) as Arc<dyn Array>;

        let arg0_field = Field::new(
            "list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("kind", DataType::Utf8, true),
                    Field::new("val", DataType::Int32, true),
                ])),
                false,
            ))),
            false,
        );
        let arg1_field = Field::new("field_name", DataType::Utf8, false);
        let arg2_field = Field::new("value", DataType::Utf8, true);

        let return_field = Field::new("array_filter", arg0_field.data_type().clone(), false);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(list_array.clone()),
                ColumnarValue::Array(field_names),
                ColumnarValue::Array(values),
            ],
            arg_fields: vec![arg0_field.into(), arg1_field.into(), arg2_field.into()],
            number_rows: 2,
            return_field: return_field.into(),
        };

        let result = func.invoke_with_args(args).unwrap();
        let out = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let out_list = out.as_any().downcast_ref::<ListArray>().unwrap();

        // Row 0 should have 2 kept items (two "foo")
        let row0_values = out_list.value(0);
        let row0_struct = row0_values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row0_struct.len(), 2);

        let kinds = row0_struct
            .column_by_name("kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(kinds.value(0), "foo");
        assert_eq!(kinds.value(1), "foo");

        // Row 1 should have 0 kept items
        let row1_values = out_list.value(1);
        let row1_struct = row1_values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row1_struct.len(), 0);
    }

    #[test]
    fn test_array_filter_null_value_matches_null_field() {
        let func = ArrayFilterFunc::new();

        // Build list with NULLs in the "kind" field
        let kinds = Arc::new(StringArray::from(vec![
            Some("foo"),
            None,
            Some("bar"),
            None,
            Some("baz"),
        ])) as Arc<dyn Array>;
        let vals = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as Arc<dyn Array>;
        let fields = Fields::from(vec![
            Field::new("kind", DataType::Utf8, true),
            Field::new("val", DataType::Int32, true),
        ]);
        let struct_array = StructArray::try_new(fields.clone(), vec![kinds, vals], None).unwrap();

        let offsets = OffsetBuffer::new(vec![0i32, 3, 5].into());
        let list_field = Arc::new(Field::new("item", DataType::Struct(fields), false));
        let list_array = Arc::new(ListArray::new(
            list_field,
            offsets,
            Arc::new(struct_array),
            None,
        ));

        // field_name per-row
        let field_names = Arc::new(StringArray::from(vec!["kind", "kind"])) as Arc<dyn Array>;
        // value: row 0 = NULL (match NULL kinds), row 1 = "bar"
        let values = Arc::new(StringArray::from(vec![None, Some("bar")])) as Arc<dyn Array>;

        let arg0_field = Field::new(
            "list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("kind", DataType::Utf8, true),
                    Field::new("val", DataType::Int32, true),
                ])),
                false,
            ))),
            false,
        );
        let arg1_field = Field::new("field_name", DataType::Utf8, false);
        let arg2_field = Field::new("value", DataType::Utf8, true);

        let return_field = Field::new("array_filter", arg0_field.data_type().clone(), false);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(list_array.clone()),
                ColumnarValue::Array(field_names),
                ColumnarValue::Array(values),
            ],
            arg_fields: vec![arg0_field.into(), arg1_field.into(), arg2_field.into()],
            number_rows: 2,
            return_field: return_field.into(),
        };

        let result = func.invoke_with_args(args).unwrap();
        let out = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let out_list = out.as_any().downcast_ref::<ListArray>().unwrap();

        // Row 0: one NULL kind should be kept
        let row0_values = out_list.value(0);
        let row0_struct = row0_values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row0_struct.len(), 1);
        let row0_kinds = row0_struct
            .column_by_name("kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(row0_kinds.is_null(0));

        // Row 1: value = "bar"; kinds are [NULL, "baz"] -> no match
        let row1_values = out_list.value(1);
        let row1_struct = row1_values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row1_struct.len(), 0);
    }

    #[test]
    fn test_array_filter_numeric_match() {
        let func = ArrayFilterFunc::new();
        let list_array = make_test_list_of_structs();

        // Filter field "val" (Int32) for value 2 on row0, and 999 on row1
        let field_names = Arc::new(StringArray::from(vec!["val", "val"])) as Arc<dyn Array>;
        let values = Arc::new(Int32Array::from(vec![Some(2), Some(999)])) as Arc<dyn Array>;

        let arg0_field = Field::new(
            "list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("kind", DataType::Utf8, true),
                    Field::new("val", DataType::Int32, true),
                ])),
                false,
            ))),
            false,
        );
        let arg1_field = Field::new("field_name", DataType::Utf8, false);
        let arg2_field = Field::new("value", DataType::Int32, true);

        let return_field = Field::new("array_filter", arg0_field.data_type().clone(), false);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(list_array.clone()),
                ColumnarValue::Array(field_names),
                ColumnarValue::Array(values),
            ],
            arg_fields: vec![arg0_field.into(), arg1_field.into(), arg2_field.into()],
            number_rows: 2,
            return_field: return_field.into(),
        };

        let result = func.invoke_with_args(args).unwrap();
        let out = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let out_list = out.as_any().downcast_ref::<ListArray>().unwrap();

        // Row 0 should keep the single element where val == 2
        let row0_values = out_list.value(0);
        let row0_struct = row0_values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row0_struct.len(), 1);
        let row0_vals = row0_struct
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(row0_vals.value(0), 2);

        // Row 1 should keep none for 999
        let row1_values = out_list.value(1);
        let row1_struct = row1_values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row1_struct.len(), 0);
    }
}
