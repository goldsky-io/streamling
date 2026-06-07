use arrow_schema::{Field as SchemaField, FieldRef};
use datafusion::arrow::array::Array;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;

use crate::{streamling_err, streamling_user_bail};

/// to_large_list(list_column) -> large_list
///
/// Converts a List array (i32 offsets) to a LargeList array (i64 offsets).
/// This allows processing arrays that would otherwise overflow the i32 offset limit
/// during operations like unnest.
///
/// Example:
/// ```sql
/// SELECT unnest(to_large_list(my_array_column)) FROM table
/// ```
#[derive(Debug)]
pub struct ToLargeListFunc {
    signature: Signature,
}

impl Default for ToLargeListFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ToLargeListFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToLargeListFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_large_list"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(streamling_err!("return_field_from_args should be called instead").into())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 1 {
            streamling_user_bail!("to_large_list expects exactly 1 argument (list column)");
        }

        let input_field = &args.arg_fields[0];
        let large_list_dt = match input_field.data_type() {
            DataType::List(inner) => DataType::LargeList(inner.clone()),
            DataType::LargeList(inner) => DataType::LargeList(inner.clone()), // Already large, pass through
            DataType::FixedSizeList(inner, _) => DataType::LargeList(inner.clone()),
            other => {
                streamling_user_bail!("to_large_list expects a list/array type, got {:?}", other);
            }
        };

        Ok(Arc::new(SchemaField::new(self.name(), large_list_dt, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            streamling_user_bail!("to_large_list expects exactly 1 argument");
        }

        let input = match &args.args[0] {
            ColumnarValue::Array(arr) => arr.clone(),
            ColumnarValue::Scalar(s) => s.to_array()?,
        };

        let result = match input.data_type() {
            DataType::List(inner) => {
                // Cast List to LargeList
                let target_type = DataType::LargeList(inner.clone());
                cast(&input, &target_type)?
            }
            DataType::LargeList(_) => {
                // Already a LargeList, return as-is
                input
            }
            DataType::FixedSizeList(inner, _) => {
                // Cast FixedSizeList to LargeList
                let target_type = DataType::LargeList(inner.clone());
                cast(&input, &target_type)?
            }
            other => {
                streamling_user_bail!("to_large_list expects a list/array type, got {:?}", other);
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::OffsetBuffer;
    use datafusion::arrow::array::{Int32Array, LargeListArray, ListArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn test_to_large_list_converts_list() {
        let func = ToLargeListFunc::new();

        // Create a simple List<Int32>
        let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let offsets = OffsetBuffer::new(vec![0i32, 2, 5].into());
        let list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_array = ListArray::new(list_field.clone(), offsets, Arc::new(values), None);

        let arg_field = Field::new("col", DataType::List(list_field.clone()), true);
        let return_field = Field::new(
            "to_large_list",
            DataType::LargeList(list_field.clone()),
            true,
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(list_array))],
            arg_fields: vec![arg_field.into()],
            number_rows: 2,
            return_field: return_field.into(),
        };

        let result = func.invoke_with_args(args).unwrap();
        let out = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };

        // Verify it's now a LargeListArray
        assert!(out.as_any().downcast_ref::<LargeListArray>().is_some());
        assert_eq!(out.len(), 2);

        let large_list = out.as_any().downcast_ref::<LargeListArray>().unwrap();
        // Row 0: [1, 2]
        assert_eq!(large_list.value(0).len(), 2);
        // Row 1: [3, 4, 5]
        assert_eq!(large_list.value(1).len(), 3);
    }

    #[test]
    fn test_to_large_list_passthrough_already_large() {
        let func = ToLargeListFunc::new();

        // Create a LargeList<Int32>
        let values = Int32Array::from(vec![1, 2, 3]);
        let offsets = OffsetBuffer::new(vec![0i64, 2, 3].into());
        let list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let large_list_array =
            LargeListArray::new(list_field.clone(), offsets, Arc::new(values), None);

        let arg_field = Field::new("col", DataType::LargeList(list_field.clone()), true);
        let return_field = Field::new(
            "to_large_list",
            DataType::LargeList(list_field.clone()),
            true,
        );

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(large_list_array))],
            arg_fields: vec![arg_field.into()],
            number_rows: 2,
            return_field: return_field.into(),
        };

        let result = func.invoke_with_args(args).unwrap();
        let out = match result {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };

        // Should still be a LargeListArray
        assert!(out.as_any().downcast_ref::<LargeListArray>().is_some());
    }
}
