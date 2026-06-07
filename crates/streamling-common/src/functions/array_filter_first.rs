use crate::error::ResultExt;
use crate::functions::array_filter::{ArrayFilterReturn, eval_array_filter};
use crate::utils::arrow::safe_take;
use crate::{streamling_bail, streamling_err, streamling_user_bail};
use arrow_schema::{Field as SchemaField, FieldRef};
use datafusion::arrow::array::{Array, Int64Array, ListArray, PrimitiveArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// array_filter_first(list<struct>, field_name_utf8, value_utf8) -> struct | null
///
/// For each row, finds the first element in the list where the named Utf8 field
/// equals the provided value, returning that struct or null if no match.
#[derive(Debug)]
pub struct ArrayFilterFirstFunc {
    signature: Signature,
}

impl Default for ArrayFilterFirstFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFilterFirstFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(3), Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ArrayFilterFirstFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_filter_first"
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
                "array_filter_first expects 3 arguments (list<struct>, field_name, value)"
            );
        }
        let input_field = &args.arg_fields[0];
        let dt = match input_field.data_type() {
            DataType::List(field) => match field.data_type() {
                DataType::Struct(fields) => DataType::Struct(fields.clone()),
                other => {
                    streamling_user_bail!(
                        "array_filter_first expects list of struct; got list of {:?}",
                        other
                    );
                }
            },
            other => {
                streamling_user_bail!(
                    "array_filter_first expects first argument to be a list/array, got {:?}",
                    other
                );
            }
        };
        Ok(Arc::new(SchemaField::new(self.name(), dt, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Use shared helper to get a ListArray with at most 1 element per row
        let list_cv = eval_array_filter(&args, self.name(), ArrayFilterReturn::First)?;
        let list_array = match list_cv {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(_) => {
                streamling_bail!("array_filter_first: expected array result from helper");
            }
        };
        let list =
            list_array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| -> DataFusionError {
                    streamling_err!("array_filter_first: helper did not return a list").into()
                })?;

        // Values are concatenated struct rows; build indices to select first per row
        let values = list.values();
        let struct_values =
            values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| -> DataFusionError {
                    streamling_err!("array_filter_first: list values must be StructArray").into()
                })?;

        let offsets = list.value_offsets();
        let mut validity: Vec<bool> = Vec::with_capacity(list.len());
        let mut first_indices: Vec<Option<i64>> = Vec::with_capacity(list.len());
        for row in 0..list.len() {
            if list.is_null(row) {
                first_indices.push(None);
                validity.push(false);
                continue;
            }
            let start = offsets[row] as i64;
            let end = offsets[row + 1] as i64;
            if end > start {
                first_indices.push(Some(start));
                validity.push(true);
            } else {
                first_indices.push(None);
                validity.push(false);
            }
        }
        let indices: PrimitiveArray<Int64Type> = Int64Array::from(first_indices);

        // Take per child with indices to form one struct per row
        // Use safe_take to handle potential overflow with large string/binary arrays
        let mut out_children: Vec<Arc<dyn datafusion::arrow::array::Array>> =
            Vec::with_capacity(struct_values.num_columns());
        for col in struct_values.columns() {
            let taken =
                safe_take(col, &indices).streamling_context("array_filter_first: take failed")?;
            out_children.push(taken);
        }

        // Build fields from actual output types to handle type promotions (e.g., Utf8 -> LargeUtf8)
        let original_fields = match struct_values.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        };
        let new_fields: Vec<Arc<datafusion::arrow::datatypes::Field>> = original_fields
            .iter()
            .zip(out_children.iter())
            .map(|(f, arr)| {
                Arc::new(datafusion::arrow::datatypes::Field::new(
                    f.name(),
                    arr.data_type().clone(),
                    f.is_nullable(),
                ))
            })
            .collect();
        let out_struct = datafusion::arrow::array::StructArray::try_new(
            new_fields.into(),
            out_children,
            Some(validity.into()),
        )
        .streamling_context("array_filter_first: build struct failed")?;

        Ok(ColumnarValue::Array(Arc::new(out_struct)))
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
        let kinds =
            Arc::new(StringArray::from(vec!["foo", "bar", "foo", "bar", "baz"])) as Arc<dyn Array>;
        let vals = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as Arc<dyn Array>;
        let fields = Fields::from(vec![
            Field::new("kind", DataType::Utf8, true),
            Field::new("val", DataType::Int32, true),
        ]);
        let struct_array = StructArray::try_new(fields.clone(), vec![kinds, vals], None).unwrap();

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
    fn test_array_filter_first_found_and_not_found() {
        let func = ArrayFilterFirstFunc::new();
        let list_array = make_test_list_of_structs();

        let field_names = Arc::new(StringArray::from(vec!["kind", "kind"])) as Arc<dyn Array>;
        let values = Arc::new(StringArray::from(vec!["foo", "qux"])) as Arc<dyn Array>;

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

        let return_field = Field::new(
            "array_filter_first",
            DataType::Struct(Fields::from(vec![
                Field::new("kind", DataType::Utf8, true),
                Field::new("val", DataType::Int32, true),
            ])),
            true,
        );

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
        let out_struct = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(out_struct.len(), 2);

        let kinds = out_struct
            .column_by_name("kind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = out_struct
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Row 0: first match is ("foo", 1)
        assert_eq!(kinds.value(0), "foo");
        assert_eq!(vals.value(0), 1);

        // Row 1: no match => null
        assert!(kinds.is_null(1));
        assert!(vals.is_null(1));
    }

    #[test]
    fn test_array_filter_first_with_all_string_struct() {
        // Test with a struct containing multiple string fields
        // This exercises the safe_take path more thoroughly
        let func = ArrayFilterFirstFunc::new();

        // Create struct array with all string fields
        let ids = Arc::new(StringArray::from(vec![
            "id1", "id2", "id3", "id4", "id5", "id6",
        ])) as Arc<dyn Array>;
        let names = Arc::new(StringArray::from(vec![
            "Alice", "Bob", "Charlie", "David", "Eve", "Frank",
        ])) as Arc<dyn Array>;
        let types =
            Arc::new(StringArray::from(vec!["A", "B", "A", "C", "A", "B"])) as Arc<dyn Array>;

        let fields = Fields::from(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("type", DataType::Utf8, true),
        ]);
        let struct_array =
            StructArray::try_new(fields.clone(), vec![ids, names, types], None).unwrap();

        // Create list: [[row0, row1, row2], [row3, row4, row5]]
        let offsets = OffsetBuffer::new(vec![0i32, 3, 6].into());
        let list_field = Arc::new(Field::new("item", DataType::Struct(fields.clone()), false));
        let list_array = Arc::new(ListArray::new(
            list_field,
            offsets,
            Arc::new(struct_array),
            None,
        ));

        let field_names = Arc::new(StringArray::from(vec!["type", "type"])) as Arc<dyn Array>;
        let values = Arc::new(StringArray::from(vec!["A", "B"])) as Arc<dyn Array>;

        let arg0_field = Field::new(
            "list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(fields.clone()),
                false,
            ))),
            false,
        );
        let arg1_field = Field::new("field_name", DataType::Utf8, false);
        let arg2_field = Field::new("value", DataType::Utf8, true);

        let return_field = Field::new("array_filter_first", DataType::Struct(fields), true);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(list_array),
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
        let out_struct = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(out_struct.len(), 2);

        let out_ids = out_struct
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let out_names = out_struct
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let out_types = out_struct
            .column_by_name("type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        // Row 0: first match for type="A" is (id1, Alice, A)
        assert_eq!(out_ids.value(0), "id1");
        assert_eq!(out_names.value(0), "Alice");
        assert_eq!(out_types.value(0), "A");

        // Row 1: first match for type="B" is (id4, David -> wait, that's wrong)
        // Actually row3 is David with type C, row4 is Eve with type A, row5 is Frank with type B
        // So first B in second list is Frank
        assert_eq!(out_ids.value(1), "id6");
        assert_eq!(out_names.value(1), "Frank");
        assert_eq!(out_types.value(1), "B");
    }

    #[test]
    fn test_array_filter_first_empty_lists() {
        // Test with empty lists to verify null handling in safe_take path
        let func = ArrayFilterFirstFunc::new();

        let fields = Fields::from(vec![
            Field::new("kind", DataType::Utf8, true),
            Field::new("val", DataType::Int32, true),
        ]);

        // Create an empty struct array for list values
        let kinds = Arc::new(StringArray::from(Vec::<&str>::new())) as Arc<dyn Array>;
        let vals = Arc::new(Int32Array::from(Vec::<i32>::new())) as Arc<dyn Array>;
        let struct_array = StructArray::try_new(fields.clone(), vec![kinds, vals], None).unwrap();

        // Create list with two empty lists
        let offsets = OffsetBuffer::new(vec![0i32, 0, 0].into());
        let list_field = Arc::new(Field::new("item", DataType::Struct(fields.clone()), false));
        let list_array = Arc::new(ListArray::new(
            list_field,
            offsets,
            Arc::new(struct_array),
            None,
        ));

        let field_names = Arc::new(StringArray::from(vec!["kind", "kind"])) as Arc<dyn Array>;
        let values = Arc::new(StringArray::from(vec!["foo", "bar"])) as Arc<dyn Array>;

        let arg0_field = Field::new(
            "list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(fields.clone()),
                false,
            ))),
            false,
        );
        let arg1_field = Field::new("field_name", DataType::Utf8, false);
        let arg2_field = Field::new("value", DataType::Utf8, true);

        let return_field = Field::new("array_filter_first", DataType::Struct(fields), true);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(list_array),
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
        let out_struct = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(out_struct.len(), 2);

        // Both rows should be null since the lists are empty
        assert!(out_struct.is_null(0));
        assert!(out_struct.is_null(1));
    }
}
