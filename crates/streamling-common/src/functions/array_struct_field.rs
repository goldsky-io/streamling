use crate::functions::util::{list_values_as_struct, validate_arg_count};
use crate::{streamling_user_bail, streamling_user_err};
use arrow_schema::{Field as SchemaField, FieldRef};
use datafusion::arrow::array::{Array, ArrayRef, ListArray, StringArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::sync::Arc;

/// `array_struct_field(list, 'field') -> List<Utf8>`
///
/// Projects one `Utf8` field out of every struct element of a `List<Struct>`,
/// producing a `List<Utf8>` with the same shape (offsets and row nulls are
/// reused zero-copy; only the values array is swapped for the struct's child
/// column). Null struct elements become null strings.
///
/// Typical use: turn a nested column into a flat string list that can feed
/// `dynamic_table_check('tbl', array_struct_field(items, 'account'))`.
///
/// The field name must be a string literal. The named field must exist and be
/// `Utf8`; anything else is a user error at execution time (the name is a
/// runtime value, so it cannot be checked during type resolution).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayStructFieldFunc {
    signature: Signature,
}

impl Default for ArrayStructFieldFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayStructFieldFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayStructFieldFunc {
    fn name(&self) -> &str {
        "array_struct_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        ))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 2 {
            streamling_user_bail!("array_struct_field expects exactly 2 arguments");
        }
        match args.arg_fields[0].data_type() {
            DataType::List(f) if matches!(f.data_type(), DataType::Struct(_)) => {}
            other => streamling_user_bail!(
                "array_struct_field expects a list of structs as first argument, got: {:?}",
                other
            ),
        }
        Ok(Arc::new(SchemaField::new(
            self.name(),
            self.return_type(&[])?,
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        validate_arg_count(&args, 2, None, self.name())?;

        let list = match &args.args[0] {
            ColumnarValue::Array(arr) => {
                arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    streamling_user_err!(
                        "array_struct_field expects a list of structs as first argument, got: {:?}",
                        arr.data_type()
                    )
                })?
            }
            ColumnarValue::Scalar(_) => {
                streamling_user_bail!("array_struct_field requires an array as first argument")
            }
        };

        let field_name = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(name))) => name,
            _ => streamling_user_bail!(
                "array_struct_field expects a non-null string literal field name as second argument"
            ),
        };

        let structs = list_values_as_struct(list, self.name())?;
        let child = structs.column_by_name(field_name).ok_or_else(|| {
            streamling_user_err!(
                "array_struct_field: struct has no field '{}' (available: {})",
                field_name,
                structs
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
        let child_strings = child
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                streamling_user_err!(
                    "array_struct_field: field '{}' must be Utf8, got {:?}",
                    field_name,
                    child.data_type()
                )
            })?;

        // Null struct elements have unspecified values in the child arrays, so
        // the struct's own null mask must be merged into the extracted column.
        // Offsets/values buffers are reused as-is, preserving any slice offset.
        let combined_nulls = NullBuffer::union(structs.nulls(), child_strings.nulls());
        let values: ArrayRef = Arc::new(StringArray::new(
            child_strings.offsets().clone(),
            child_strings.values().clone(),
            combined_nulls,
        ));

        // Zero-copy: reuse the input's offsets and row nulls, swap the values.
        let out_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let result = ListArray::try_new(
            out_field,
            list.offsets().clone(),
            values,
            list.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int32Array, StructArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::Fields;

    /// rows: [[{a:"x",b:1},{a:"y",b:2}], NULL, [], [{a:"z",b:3}]]
    fn make_list(struct_nulls: Option<NullBuffer>) -> (ListArray, Vec<FieldRef>) {
        let a: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), Some("y"), Some("z")]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let struct_fields = Fields::from(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let offsets = OffsetBuffer::new(vec![0i32, 2, 2, 2, 3].into());
        let nulls = NullBuffer::from(vec![true, false, true, true]);
        let structs =
            StructArray::try_new(struct_fields.clone(), vec![a, b], struct_nulls).unwrap();
        let item_nullable = structs.null_count() > 0;
        let list_field = Arc::new(Field::new(
            "item",
            DataType::Struct(struct_fields),
            item_nullable,
        ));
        let list = ListArray::new(list_field, offsets, Arc::new(structs), Some(nulls));
        let arg_fields = vec![
            Arc::new(SchemaField::new("list", list.data_type().clone(), true)) as FieldRef,
            Arc::new(SchemaField::new("field", DataType::Utf8, false)) as FieldRef,
        ];
        (list, arg_fields)
    }

    fn invoke(list: ListArray, arg_fields: Vec<FieldRef>, field: &str) -> Result<ColumnarValue> {
        ArrayStructFieldFunc::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(list)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(field.to_string()))),
            ],
            arg_fields,
            number_rows: 4,
            return_field: Arc::new(SchemaField::new("out", DataType::Null, true)),
            config_options: Arc::new(::datafusion::config::ConfigOptions::default()),
        })
    }

    #[allow(clippy::type_complexity)]
    fn result_rows(result: ColumnarValue) -> (Vec<Option<Vec<Option<String>>>>, Vec<bool>) {
        let ColumnarValue::Array(arr) = result else {
            panic!("expected array result")
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        let mut rows = Vec::new();
        let mut nulls = Vec::new();
        for i in 0..list.len() {
            nulls.push(list.is_null(i));
            let values = list.value(i);
            let strings = values.as_any().downcast_ref::<StringArray>().unwrap();
            rows.push(Some(
                (0..strings.len())
                    .map(|j| {
                        if strings.is_null(j) {
                            None
                        } else {
                            Some(strings.value(j).to_string())
                        }
                    })
                    .collect(),
            ));
        }
        (rows, nulls)
    }

    #[test]
    fn extracts_field_zero_copy_shape() {
        let (list, arg_fields) = make_list(None);
        let (rows, nulls) = result_rows(invoke(list, arg_fields, "a").expect("extract failed"));
        assert_eq!(nulls, vec![false, true, false, false]);
        assert_eq!(
            rows,
            vec![
                Some(vec![Some("x".into()), Some("y".into())]),
                Some(vec![]),
                Some(vec![]),
                Some(vec![Some("z".into())]),
            ]
        );
    }

    #[test]
    fn struct_null_elements_become_null_strings() {
        // second struct element is null
        let (list, arg_fields) = make_list(Some(NullBuffer::from(vec![true, false, true])));
        let (rows, _) = result_rows(invoke(list, arg_fields, "a").expect("extract failed"));
        assert_eq!(
            rows[0],
            Some(vec![Some("x".into()), None]),
            "null struct element must yield a null string, not garbage"
        );
    }

    #[test]
    fn missing_field_is_user_error() {
        let (list, arg_fields) = make_list(None);
        let err = invoke(list, arg_fields, "nope").unwrap_err();
        assert!(
            err.to_string().contains("no field 'nope'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn non_utf8_field_is_user_error() {
        let (list, arg_fields) = make_list(None);
        let err = invoke(list, arg_fields, "b").unwrap_err();
        assert!(
            err.to_string().contains("must be Utf8"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn empty_struct_array_is_safe() {
        // Defensive: a list whose struct values array has length zero must not
        // panic on offset/null indexing (one row with an empty list).
        use datafusion::arrow::array::new_empty_array;
        let empty_struct = new_empty_array(&DataType::Struct(Fields::from(vec![Field::new(
            "a",
            DataType::Utf8,
            true,
        )])));
        let field = Arc::new(Field::new("item", empty_struct.data_type().clone(), false));
        let offsets = OffsetBuffer::new(vec![0i32, 0].into());
        let list = ListArray::new(field, offsets, empty_struct, None);
        let arg_fields = vec![
            Arc::new(SchemaField::new("list", list.data_type().clone(), true)) as FieldRef,
            Arc::new(SchemaField::new("field", DataType::Utf8, false)) as FieldRef,
        ];
        let result = invoke(list, arg_fields, "a").expect("extract failed");
        let ColumnarValue::Array(arr) = result else {
            panic!("expected array result")
        };
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list.value(0).len(), 0);
    }
}
