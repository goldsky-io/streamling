//! Arrow byte size of a batch's rows, measured for the `input_bytes_delta` and
//! `output_bytes_delta` usage metrics.

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericByteViewArray, GenericListArray, GenericListViewArray,
    OffsetSizeTrait, RunArray, UInt64Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{
    ByteViewType, DataType, Int16Type, Int32Type, Int64Type, RunEndIndexType,
};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;

/// Bytes the rows of `batch` would occupy if copied into fresh arrays of the same types.
///
/// A slice counts only its own rows, never the buffers it shares with other slices, at every
/// nesting level. Dictionary and run-end encoded columns count their decoded values, so the
/// size does not depend on how the producer encoded them.
pub fn batch_data_bytes(batch: &RecordBatch) -> Result<usize, ArrowError> {
    batch
        .columns()
        .iter()
        .map(|column| array_bytes(column.as_ref()))
        .sum()
}

fn array_bytes(array: &dyn Array) -> Result<usize, ArrowError> {
    let len = array.len();
    let validity = array.nulls().map_or(0, |_| len.div_ceil(8));
    match array.data_type() {
        // Leaf layouts: `get_slice_memory_size` sizes exactly the slice's rows.
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(..)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => array.to_data().get_slice_memory_size(),
        DataType::Utf8View => Ok(validity + view_bytes(array.as_string_view())),
        DataType::BinaryView => Ok(validity + view_bytes(array.as_binary_view())),
        DataType::List(_) => Ok(validity + list_bytes(array.as_list::<i32>())?),
        DataType::LargeList(_) => Ok(validity + list_bytes(array.as_list::<i64>())?),
        DataType::ListView(_) => Ok(validity + list_view_bytes(array.as_list_view::<i32>())?),
        DataType::LargeListView(_) => Ok(validity + list_view_bytes(array.as_list_view::<i64>())?),
        DataType::Map(..) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            let start = offsets[0] as usize;
            let entries = map.entries().slice(start, offsets[len] as usize - start);
            Ok(validity + len * size_of::<i32>() + array_bytes(&entries)?)
        }
        // Arrow slices fixed-size list values and struct fields together with the parent.
        DataType::FixedSizeList(..) => {
            Ok(validity + array_bytes(array.as_fixed_size_list().values().as_ref())?)
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .try_fold(validity, |total, field| {
                Ok(total + array_bytes(field.as_ref())?)
            }),
        DataType::Union(..) => union_bytes(array),
        DataType::Dictionary(..) => {
            let dictionary = array.as_any_dictionary();
            array_bytes(take(dictionary.values().as_ref(), dictionary.keys(), None)?.as_ref())
        }
        DataType::RunEndEncoded(run_ends, _) => match run_ends.data_type() {
            DataType::Int16 => run_end_bytes(array.as_run::<Int16Type>()),
            DataType::Int32 => run_end_bytes(array.as_run::<Int32Type>()),
            DataType::Int64 => run_end_bytes(array.as_run::<Int64Type>()),
            other => Err(ArrowError::InvalidArgumentError(format!(
                "invalid run-end index type {other}"
            ))),
        },
    }
}

/// 16 bytes per view, plus each valid value too long to be inlined in its view.
fn view_bytes<T: ByteViewType + ?Sized>(array: &GenericByteViewArray<T>) -> usize {
    const MAX_INLINE_LEN: usize = 12;
    let out_of_line: usize = array
        .views()
        .iter()
        .enumerate()
        .filter(|&(row, _)| array.is_valid(row))
        .map(|(_, view)| *view as u32 as usize)
        .filter(|&value_len| value_len > MAX_INLINE_LEN)
        .sum();
    array.len() * 16 + out_of_line
}

/// The offsets of the rows, plus only the child values those rows reference.
fn list_bytes<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> Result<usize, ArrowError> {
    let offsets = list.value_offsets();
    let start = offsets[0].as_usize();
    let values = list
        .values()
        .slice(start, offsets[list.len()].as_usize() - start);
    Ok(list.len() * size_of::<O>() + array_bytes(values.as_ref())?)
}

/// The offsets and sizes of the rows, plus each valid row's child values.
fn list_view_bytes<O: OffsetSizeTrait>(
    list: &GenericListViewArray<O>,
) -> Result<usize, ArrowError> {
    let values = list.values();
    (0..list.len()).filter(|&row| list.is_valid(row)).try_fold(
        list.len() * 2 * size_of::<O>(),
        |total, row| {
            let row_values = values.slice(
                list.value_offsets()[row].as_usize(),
                list.value_sizes()[row].as_usize(),
            );
            Ok(total + array_bytes(row_values.as_ref())?)
        },
    )
}

/// One type id per row; a dense union adds an offset per row and only the referenced child
/// values, a sparse union every child sliced to the rows.
fn union_bytes(array: &dyn Array) -> Result<usize, ArrowError> {
    let union = array.as_union();
    let type_ids = union.len();
    match union.offsets() {
        // ponytail: one child slice per row; group rows per child if dense unions get hot.
        Some(_) => (0..union.len()).try_fold(type_ids * (1 + size_of::<i32>()), |total, row| {
            let child = union.child(union.type_id(row));
            Ok(total + array_bytes(child.slice(union.value_offset(row), 1).as_ref())?)
        }),
        None => {
            let DataType::Union(fields, _) = union.data_type() else {
                unreachable!("as_union only returns union arrays")
            };
            fields.iter().try_fold(type_ids, |total, (type_id, _)| {
                Ok(total + array_bytes(union.child(type_id).as_ref())?)
            })
        }
    }
}

fn run_end_bytes<R: RunEndIndexType>(run: &RunArray<R>) -> Result<usize, ArrowError> {
    let physical: UInt64Array = (0..run.len())
        .map(|row| run.get_physical_index(row) as u64)
        .collect();
    let decoded: ArrayRef = take(run.values().as_ref(), &physical, None)?;
    array_bytes(decoded.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        DictionaryArray, FixedSizeListBuilder, Int32Builder, Int64Array, Int64Builder, ListBuilder,
        MapBuilder, StringArray, StringBuilder, StringViewArray, StructArray, UnionArray,
    };
    use datafusion::arrow::datatypes::{Field, UnionFields};
    use std::sync::Arc;

    fn batch(column: ArrayRef) -> RecordBatch {
        RecordBatch::try_from_iter([("c", column)]).unwrap()
    }

    fn bytes(column: ArrayRef) -> usize {
        batch_data_bytes(&batch(column)).unwrap()
    }

    /// The size a slice reports must equal the size of the same rows copied into a fresh array.
    fn assert_slice_counts_only_its_rows(column: ArrayRef, offset: usize, len: usize) {
        let full = batch(column);
        let sliced = full.slice(offset, len);
        let compacted = batch(take_rows(full.column(0), offset, len));
        assert_eq!(
            batch_data_bytes(&sliced).unwrap(),
            batch_data_bytes(&compacted).unwrap(),
            "{:?}",
            full.column(0).data_type()
        );
    }

    fn take_rows(column: &ArrayRef, offset: usize, len: usize) -> ArrayRef {
        let indices: UInt64Array = (offset as u64..(offset + len) as u64).collect();
        take(column.as_ref(), &indices, None).unwrap()
    }

    #[test]
    fn primitive_slice_counts_only_its_rows() {
        let column: ArrayRef = Arc::new(Int64Array::from_iter_values(0..100));
        assert_eq!(bytes(column.slice(10, 20)), 20 * 8);
    }

    #[test]
    fn utf8_slice_counts_offsets_values_and_validity() {
        let column: ArrayRef = Arc::new(StringArray::from(vec![
            Some("aaaa"),
            None,
            Some("bbbbbbbb"),
            Some("cc"),
        ]));
        // Rows 1..3: two i32 offsets, 8 value bytes, one validity byte.
        assert_eq!(bytes(column.slice(1, 2)), 2 * 4 + 8 + 1);
    }

    #[test]
    fn view_counts_out_of_line_values_of_valid_rows() {
        let long = "x".repeat(40);
        let column: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("short"),
            Some(long.as_str()),
            None,
            Some(long.as_str()),
        ]));
        assert_eq!(bytes(column.clone()), 4 * 16 + 2 * 40 + 1);
        assert_eq!(bytes(column.slice(0, 1)), 16 + 1);
    }

    #[test]
    fn list_slice_counts_only_its_child_values() {
        let mut builder = ListBuilder::new(Int64Builder::new());
        for row in 0..1000 {
            builder.values().append_slice(&[row; 10]);
            builder.append(true);
        }
        let column: ArrayRef = Arc::new(builder.finish());
        assert_eq!(bytes(column.slice(500, 1)), 4 + 10 * 8);
        assert_slice_counts_only_its_rows(column, 500, 7);
    }

    #[test]
    fn map_slice_counts_only_its_entries() {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        for row in 0..100 {
            builder.keys().append_value(format!("key{row}"));
            builder.values().append_value(row);
            builder.append(true).unwrap();
        }
        assert_slice_counts_only_its_rows(Arc::new(builder.finish()), 40, 3);
    }

    #[test]
    fn fixed_size_list_slice_counts_only_its_child_values() {
        let mut builder = FixedSizeListBuilder::new(Int64Builder::new(), 4);
        for row in 0..100 {
            builder.values().append_slice(&[row; 4]);
            builder.append(true);
        }
        let column: ArrayRef = Arc::new(builder.finish());
        assert_eq!(bytes(column.slice(10, 2)), 2 * 4 * 8);
    }

    #[test]
    fn view_inside_struct_counts_out_of_line_values() {
        let values: ArrayRef = Arc::new(StringViewArray::from_iter_values(
            (0..1000).map(|row| format!("{row:040}")),
        ));
        let column: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("s", DataType::Utf8View, false)),
            values,
        )]));
        assert_eq!(bytes(column.clone()), 1000 * (16 + 40));
        assert_eq!(bytes(column.slice(3, 1)), 16 + 40);
    }

    #[test]
    fn dictionary_counts_decoded_values_of_its_rows() {
        let dictionary: DictionaryArray<Int32Type> = (0..1000)
            .map(|row| format!("{row:040}"))
            .collect::<Vec<_>>()
            .iter()
            .map(String::as_str)
            .collect();
        let column: ArrayRef = Arc::new(dictionary);
        // One decoded Utf8 row: a 4-byte offset and its 40 value bytes.
        assert_eq!(bytes(column.slice(7, 1)), 4 + 40);
    }

    #[test]
    fn dense_union_counts_only_referenced_child_values() {
        let ints: ArrayRef = Arc::new(Int64Array::from_iter_values(0..100));
        let strings: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..100).map(|row| format!("{row:08}")),
        ));
        let union = UnionArray::try_new(
            union_fields(),
            vec![0_i8, 1, 0, 1].into(),
            Some(vec![5_i32, 6, 7, 8].into()),
            vec![ints, strings],
        )
        .unwrap();
        // Rows 1..3: two type ids, two offsets, one string (4 + 8) and one Int64.
        assert_eq!(
            bytes(Arc::new(union.slice(1, 2))),
            2 * (1 + 4) + (4 + 8) + 8
        );
    }

    #[test]
    fn sparse_union_counts_every_child_sliced_to_its_rows() {
        let ints: ArrayRef = Arc::new(Int64Array::from_iter_values(0..4));
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["aa", "bbbb", "cc", "dddd"]));
        let union = UnionArray::try_new(
            union_fields(),
            vec![0_i8, 1, 0, 1].into(),
            None,
            vec![ints, strings],
        )
        .unwrap();
        // Rows 1..3: two type ids, two Int64 and two strings ("bbbb", "cc").
        assert_eq!(bytes(Arc::new(union.slice(1, 2))), 2 + 2 * 8 + 2 * 4 + 6);
    }

    fn union_fields() -> UnionFields {
        UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("i", DataType::Int64, false),
                Field::new("s", DataType::Utf8, false),
            ],
        )
        .unwrap()
    }

    #[test]
    fn run_end_encoded_counts_decoded_values_of_its_rows() {
        let run: RunArray<Int32Type> = vec!["aaaa"; 50]
            .into_iter()
            .chain(vec!["bbbbbbbb"; 50])
            .collect();
        let column: ArrayRef = Arc::new(run);
        // Rows 48..52: two "aaaa" and two "bbbbbbbb" as plain Utf8.
        assert_eq!(bytes(column.slice(48, 4)), 4 * 4 + 2 * 4 + 2 * 8);
    }
}
