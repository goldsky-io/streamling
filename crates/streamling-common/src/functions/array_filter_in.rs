use crate::error::ResultExt;
use crate::functions::util::{get_array_from_arg, list_values_as_struct, validate_arg_count};
use crate::{streamling_err, streamling_user_bail, streamling_user_err};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{Field as SchemaField, FieldRef};
use datafusion::arrow::array::{Array, BooleanBuilder, Int64Array, ListArray, StringArray};
use datafusion::arrow::compute as arrow_compute;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

/// array_filter_in(list<struct>, field_name_utf8, values_list) -> list<struct>
///
/// Filters each list, keeping only elements where the named struct field
/// matches ANY value in the provided values list. If the list is null, the result is null.
/// If the field is missing or has an unsupported type, an error is returned.
///
/// Example:
/// ```sql
/// array_filter_in(changes, 'ledger_entry_type', ARRAY['account', 'trustline'])
/// ```
#[derive(Debug)]
pub struct ArrayFilterInFunc {
    signature: Signature,
}

impl Default for ArrayFilterInFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFilterInFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayFilterInFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_filter_in"
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
                "array_filter_in expects 3 arguments (list<struct>, field_name, values_list)"
            );
        }
        let input_field = &args.arg_fields[0];
        let list_dt = match input_field.data_type() {
            DataType::List(field) => DataType::List(field.clone()),
            other => {
                streamling_user_bail!(
                    "array_filter_in expects first argument to be a list/array, got {:?}",
                    other
                );
            }
        };
        Ok(Arc::new(SchemaField::new(self.name(), list_dt, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        eval_array_filter_in(&args, self.name())
    }
}

/// Core evaluation logic for array_filter_in
fn eval_array_filter_in(args: &ScalarFunctionArgs, function_name: &str) -> Result<ColumnarValue> {
    validate_arg_count(args, 3, None, function_name)?;

    // Get input list as ArrayRef
    let input_array = get_array_from_arg(&args.args[0])?;
    let list_array = input_array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| streamling_user_err!("First argument must be a list array"))?;

    // Get the input list's inner field to preserve its name in the output
    let input_list_field = match list_array.data_type() {
        DataType::List(field) => field.clone(),
        _ => streamling_user_bail!("Expected List type"),
    };

    // Early return for empty input
    if list_array.len() == 0 {
        let element_struct = list_values_as_struct(list_array, function_name)?;
        let fields = match element_struct.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        };
        let empty_values: Arc<dyn Array> =
            datafusion::arrow::array::new_empty_array(&DataType::Struct(fields));
        let out_field = Arc::new(Field::new(
            input_list_field.name(),
            empty_values.data_type().clone(),
            input_list_field.is_nullable(),
        ));
        let out_offsets = OffsetBuffer::new(vec![0i32].into());
        let result = ListArray::new(out_field, out_offsets, empty_values, None);
        return Ok(ColumnarValue::Array(Arc::new(result)));
    }

    // Get field name (must be a scalar string or single-element array)
    let field_name_arr = get_array_from_arg(&args.args[1])?;
    let field_names = field_name_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| streamling_user_err!("Second argument (field_name) must be Utf8"))?;

    if field_names.len() != 1 && field_names.len() != list_array.len() {
        streamling_user_bail!(
            "{}: field_name must be a scalar or match input row count",
            function_name
        );
    }
    let field_broadcast = field_names.len() == 1;

    // Get the values list (third argument) - this is a List of values to match
    let values_array = get_array_from_arg(&args.args[2])?;

    // Extract the target values to match against
    let target_values = extract_target_values(&values_array, function_name)?;

    // Values array across all rows
    let element_struct = list_values_as_struct(list_array, function_name)?;

    // Offsets per row
    let in_offsets = list_array.value_offsets();

    // Collect indices of elements to keep
    let mut keep_indices: Vec<usize> = Vec::new();

    let get_field_name = |row: usize| -> Result<&str> {
        let idx = if field_broadcast { 0 } else { row };
        if field_names.is_null(idx) {
            Err(streamling_user_err!("{}: field_name cannot be null", function_name).into())
        } else {
            Ok(field_names.value(idx))
        }
    };

    for row in 0..list_array.len() {
        let start = in_offsets[row] as usize;
        let end = in_offsets[row + 1] as usize;

        if list_array.is_null(row) {
            continue;
        }

        let field_name = get_field_name(row)?;

        // Locate the field column
        let col = element_struct.column_by_name(field_name).ok_or_else(|| {
            streamling_user_err!(
                "{}: missing or invalid field '{}'",
                function_name,
                field_name
            )
        })?;

        // Match based on field type
        match col.data_type() {
            DataType::Utf8 => {
                let field_utf8 = col.as_any().downcast_ref::<StringArray>().unwrap();
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    if !field_utf8.is_null(idx) {
                        let value = field_utf8.value(idx);
                        if target_values.string_values.contains(value) {
                            keep_indices.push(idx);
                        }
                    } else if target_values.has_null {
                        // Keep nulls if null is in the target values
                        keep_indices.push(idx);
                    }
                }
            }
            DataType::Int64 => {
                let field_int = col.as_any().downcast_ref::<Int64Array>().unwrap();
                for idx in start..end {
                    if element_struct.is_null(idx) {
                        continue;
                    }
                    if !field_int.is_null(idx) {
                        let value = field_int.value(idx);
                        if target_values.int_values.contains(&value) {
                            keep_indices.push(idx);
                        }
                    } else if target_values.has_null {
                        keep_indices.push(idx);
                    }
                }
            }
            other => {
                streamling_user_bail!(
                    "{}: unsupported field type {:?} for array_filter_in (supported: Utf8, Int64)",
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

    // Rebuild ListArray with updated offsets per row
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
            }
        }
        cursor += kept;
        out_offsets.push(cursor);
    }

    let out_field = Arc::new(Field::new(
        input_list_field.name(),
        element_struct.data_type().clone(),
        input_list_field.is_nullable(),
    ));
    let out_offsets = OffsetBuffer::new(out_offsets.into());
    let out_nulls = list_array.nulls().cloned();
    let result = ListArray::new(out_field, out_offsets, filtered_values, out_nulls);
    Ok(ColumnarValue::Array(Arc::new(result)))
}

/// Container for target values to match against
struct TargetValues {
    string_values: HashSet<String>,
    int_values: HashSet<i64>,
    has_null: bool,
}

/// Extract target values from the third argument (can be a List or scalar)
fn extract_target_values(
    values_array: &Arc<dyn Array>,
    function_name: &str,
) -> Result<TargetValues> {
    let mut result = TargetValues {
        string_values: HashSet::new(),
        int_values: HashSet::new(),
        has_null: false,
    };

    // Handle List<Utf8> or List<Int64>
    if let Some(list_array) = values_array.as_any().downcast_ref::<ListArray>() {
        // We expect a single list (the values to match)
        if list_array.len() == 0 {
            return Ok(result);
        }

        // Get the values from the first (and should be only) list element
        let values = list_array.value(0);

        if let Some(string_arr) = values.as_any().downcast_ref::<StringArray>() {
            for i in 0..string_arr.len() {
                if string_arr.is_null(i) {
                    result.has_null = true;
                } else {
                    result.string_values.insert(string_arr.value(i).to_string());
                }
            }
        } else if let Some(int_arr) = values.as_any().downcast_ref::<Int64Array>() {
            for i in 0..int_arr.len() {
                if int_arr.is_null(i) {
                    result.has_null = true;
                } else {
                    result.int_values.insert(int_arr.value(i));
                }
            }
        } else {
            streamling_user_bail!(
                "{}: values list must contain Utf8 or Int64 elements, got {:?}",
                function_name,
                values.data_type()
            );
        }
    } else if let Some(string_arr) = values_array.as_any().downcast_ref::<StringArray>() {
        // Handle case where it's passed as a direct array (not wrapped in List)
        for i in 0..string_arr.len() {
            if string_arr.is_null(i) {
                result.has_null = true;
            } else {
                result.string_values.insert(string_arr.value(i).to_string());
            }
        }
    } else {
        streamling_user_bail!(
            "{}: third argument must be a list of values to match, got {:?}",
            function_name,
            values_array.data_type()
        );
    }

    Ok(result)
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
        // Row 0: 3 items -> types ["account","trustline","offer"]
        // Row 1: 2 items -> types ["trustline","data"]
        let types = Arc::new(StringArray::from(vec![
            "account",
            "trustline",
            "offer",
            "trustline",
            "data",
        ])) as Arc<dyn Array>;
        let vals = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as Arc<dyn Array>;
        let fields = Fields::from(vec![
            Field::new("ledger_entry_type", DataType::Utf8, true),
            Field::new("val", DataType::Int32, true),
        ]);
        let struct_array = StructArray::try_new(fields.clone(), vec![types, vals], None).unwrap();

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

    fn make_values_list(values: Vec<&str>) -> Arc<ListArray> {
        let string_array = StringArray::from(values);
        let offsets = OffsetBuffer::new(vec![0i32, string_array.len() as i32].into());
        let list_field = Arc::new(Field::new("item", DataType::Utf8, true));
        Arc::new(ListArray::new(
            list_field,
            offsets,
            Arc::new(string_array),
            None,
        ))
    }

    #[test]
    fn test_array_filter_in_matches_multiple() {
        let func = ArrayFilterInFunc::new();
        let list_array = make_test_list_of_structs();

        // Filter for "account" and "trustline"
        let field_names = Arc::new(StringArray::from(vec!["ledger_entry_type"])) as Arc<dyn Array>;
        let values_list = make_values_list(vec!["account", "trustline"]);

        let arg0_field = Field::new(
            "list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("ledger_entry_type", DataType::Utf8, true),
                    Field::new("val", DataType::Int32, true),
                ])),
                false,
            ))),
            false,
        );
        let arg1_field = Field::new("field_name", DataType::Utf8, false);
        let arg2_field = Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        );

        let return_field = Field::new("array_filter_in", arg0_field.data_type().clone(), false);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(list_array.clone()),
                ColumnarValue::Array(field_names),
                ColumnarValue::Array(values_list),
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

        // Row 0: should have 2 kept items ("account" and "trustline", not "offer")
        let row0_values = out_list.value(0);
        let row0_struct = row0_values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row0_struct.len(), 2);

        let types = row0_struct
            .column_by_name("ledger_entry_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(types.value(0), "account");
        assert_eq!(types.value(1), "trustline");

        // Row 1: should have 1 kept item ("trustline", not "data")
        let row1_values = out_list.value(1);
        let row1_struct = row1_values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row1_struct.len(), 1);

        let types = row1_struct
            .column_by_name("ledger_entry_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(types.value(0), "trustline");
    }

    #[test]
    fn test_array_filter_in_no_matches() {
        let func = ArrayFilterInFunc::new();
        let list_array = make_test_list_of_structs();

        // Filter for values that don't exist
        let field_names = Arc::new(StringArray::from(vec!["ledger_entry_type"])) as Arc<dyn Array>;
        let values_list = make_values_list(vec!["nonexistent1", "nonexistent2"]);

        let arg0_field = Field::new(
            "list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("ledger_entry_type", DataType::Utf8, true),
                    Field::new("val", DataType::Int32, true),
                ])),
                false,
            ))),
            false,
        );
        let arg1_field = Field::new("field_name", DataType::Utf8, false);
        let arg2_field = Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        );

        let return_field = Field::new("array_filter_in", arg0_field.data_type().clone(), false);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(list_array.clone()),
                ColumnarValue::Array(field_names),
                ColumnarValue::Array(values_list),
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

        // Both rows should be empty
        assert_eq!(out_list.value(0).len(), 0);
        assert_eq!(out_list.value(1).len(), 0);
    }
}
