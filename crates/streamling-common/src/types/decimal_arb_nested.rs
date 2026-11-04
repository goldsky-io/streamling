//! Rebuild a column with every `decimal_arb` leaf nested below it rewritten.
//!
//! A sink that stores containers in a native layout (ClickHouse `Array` /
//! `Tuple` / `Map`) cannot hand a nested decimal_arb leaf over as its
//! canonical `LargeBinary` bytes: each leaf has to become the concrete type
//! the destination declares for it. This walk rebuilds the containers around
//! the leaves and leaves the per-leaf decision to the caller. It covers the
//! same layouts as the text bridge in [`crate::formats::decimal_arb_text`]:
//! list views and dictionary / run-end encodings are cast to their plain
//! layout first, and a leaf under a union is refused. A sliced list or map
//! has its child trimmed to the slice's window first, so only the elements
//! the slice owns are converted.
//!
//! Leaves are addressed by a dotted path from the column (`traces.item.value`),
//! the same form the config-load validator reports.

use crate::error::Result;
use crate::formats::decimal_arb_text::{
    field_contains_decimal_arb, field_with_type, to_plain_layout, trim_list_child,
};
use crate::streamling_err;
use crate::types::decimal_arb::DecimalArbType;
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, MutableArrayData,
    OffsetSizeTrait, StructArray, make_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use std::sync::Arc;

/// Rewrites one decimal_arb leaf, given its field, values and dotted path,
/// into the field and values that replace it.
pub type LeafRewrite<'a> = dyn FnMut(&Field, &ArrayRef, &str) -> Result<(Field, ArrayRef)> + 'a;

/// Does `field` hold a `decimal_arb` leaf *below* it (it is a container, or a
/// dictionary- / run-end-encoded leaf, not a plain decimal_arb column itself)?
pub fn contains_nested_decimal_arb(field: &Field) -> bool {
    !DecimalArbType::is_decimal_arb_field(field) && field_contains_decimal_arb(field)
}

/// Rebuild `(field, array)` with every decimal_arb leaf below it replaced by
/// what `leaf` returns for it; containers keep their logical values and validity. A
/// struct's nulls are pushed down to its children first, so `leaf` sees a
/// slot under a null struct as null rather than whatever placeholder bytes
/// the producer left there; a child of a nullable struct that receives them
/// is declared nullable. Fields without a decimal_arb leaf come back
/// unchanged. Children hidden by a null list or map are removed before conversion;
/// fixed-size lists restore their hidden slots as nulls after conversion.
/// Decoded map keys must be non-null and keep a non-nullable declaration.
pub fn rewrite_decimal_arb_leaves(
    field: &Field,
    array: &ArrayRef,
    path: &str,
    leaf: &mut LeafRewrite<'_>,
) -> Result<(Field, ArrayRef)> {
    if DecimalArbType::is_decimal_arb_field(field) {
        return leaf(field, array, path);
    }
    if !field_contains_decimal_arb(field) {
        return Ok((field.clone(), Arc::clone(array)));
    }
    if let Some((plain, plain_array)) = to_plain_layout(field, array)? {
        return rewrite_decimal_arb_leaves(&plain, &plain_array, path, leaf);
    }
    let downcast_err = |what: &str| {
        streamling_err!(
            "expected {} for column '{}', got {:?}",
            what,
            path,
            array.data_type(),
        )
    };
    let mut child = |c: &FieldRef, values: &ArrayRef| -> Result<(FieldRef, ArrayRef)> {
        let child_path = format!("{}.{}", path, c.name());
        let (f, a) = rewrite_decimal_arb_leaves(c, values, &child_path, leaf)?;
        Ok((Arc::new(f), a))
    };
    let (data_type, rebuilt): (DataType, ArrayRef) = match field.data_type() {
        DataType::Struct(children) => {
            let sa = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray"))?;
            let mut fields = Vec::with_capacity(children.len());
            let mut columns = Vec::with_capacity(children.len());
            for ((c, own), column) in children.iter().zip(sa.fields()).zip(sa.columns()) {
                let (f, a) = if field_contains_decimal_arb(c) {
                    // An encoded child (dictionary / run-end) cannot take a
                    // null buffer of its own, so it is unwrapped before the
                    // struct's nulls are pushed into it.
                    match to_plain_layout(c, column)? {
                        Some((plain, plain_column)) => child(
                            &Arc::new(under_struct_nulls(&plain, field)),
                            &with_parent_nulls(&plain_column, sa.nulls())?,
                        )?,
                        None => child(
                            &Arc::new(under_struct_nulls(c, field)),
                            &with_parent_nulls(column, sa.nulls())?,
                        )?,
                    }
                } else {
                    // Kept as the array declares it: a batch is accepted with
                    // nested field names and metadata that differ from its
                    // schema's, and `StructArray::try_new` compares the two
                    // exactly.
                    (Arc::clone(own), Arc::clone(column))
                };
                fields.push(f);
                columns.push(a);
            }
            let fields = Fields::from(fields);
            let sa = StructArray::try_new(fields.clone(), columns, sa.nulls().cloned())?;
            (DataType::Struct(fields), Arc::new(sa))
        }
        DataType::List(c) => {
            let la = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| downcast_err("ListArray"))?;
            let (offsets, values) = visible_list_child(la.offsets(), la.values(), la.nulls());
            let (f, values) = child(c, &values)?;
            let la = ListArray::try_new(Arc::clone(&f), offsets, values, la.nulls().cloned())?;
            (DataType::List(f), Arc::new(la))
        }
        DataType::LargeList(c) => {
            let la = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| downcast_err("LargeListArray"))?;
            let (offsets, values) = visible_list_child(la.offsets(), la.values(), la.nulls());
            let (f, values) = child(c, &values)?;
            let la = LargeListArray::try_new(Arc::clone(&f), offsets, values, la.nulls().cloned())?;
            (DataType::LargeList(f), Arc::new(la))
        }
        DataType::FixedSizeList(c, n) => {
            let fa = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| downcast_err("FixedSizeListArray"))?;
            let (f, values) = if let Some(nulls) = fa.nulls().filter(|n| n.null_count() > 0) {
                // Fixed-size lists must restore the hidden slots as nulls.
                // Arrow permits these masked nulls even for a non-nullable
                // item field, so its declaration stays unchanged.
                let width = fa.value_length() as usize;
                let data = fa.values().to_data();
                let mut visible = MutableArrayData::new(
                    vec![&data],
                    false,
                    (fa.len() - nulls.null_count()) * width,
                );
                for (start, end) in nulls.valid_slices() {
                    visible.extend(0, start * width, end * width);
                }
                let (f, values) = child(c, &make_array(visible.freeze()))?;
                let data = values.to_data();
                let mut restored = MutableArrayData::new(vec![&data], true, fa.values().len());
                let mut row = 0;
                let mut value = 0;
                for (start, end) in nulls.valid_slices() {
                    restored.extend_nulls((start - row) * width);
                    let len = (end - start) * width;
                    restored.extend(0, value, value + len);
                    value += len;
                    row = end;
                }
                restored.extend_nulls((fa.len() - row) * width);
                (f, make_array(restored.freeze()))
            } else {
                child(c, fa.values())?
            };
            // The length is given explicitly: `FixedSizeListArray::new` derives
            // it from the values, which for a zero-width list means zero rows.
            let fa = FixedSizeListArray::try_new_with_length(
                Arc::clone(&f),
                *n,
                values,
                fa.nulls().cloned(),
                fa.len(),
            )?;
            (DataType::FixedSizeList(f, *n), Arc::new(fa))
        }
        DataType::Map(entry_field, sorted) => {
            let ma = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| downcast_err("MapArray"))?;
            let entries: ArrayRef = Arc::new(ma.entries().clone());
            let (offsets, entries) = visible_list_child(ma.offsets(), &entries, ma.nulls());
            let original_entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray (map entries)"))?;
            if original_entries.column(0).logical_null_count() > 0 {
                return Err(streamling_err!(
                    "null map key at '{}.{}.{}'",
                    path,
                    entry_field.name(),
                    original_entries.fields()[0].name(),
                ));
            }
            let (f, entries) = child(entry_field, &entries)?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray (map entries)"))?;
            // Dictionary decoding normally widens nullability, but map keys
            // cannot be nullable, including in the empty schema walk.
            let (f, entries) = if entries.fields()[0].is_nullable() {
                let mut fields = entries.fields().to_vec();
                fields[0] = Arc::new(fields[0].as_ref().clone().with_nullable(false));
                let fields = Fields::from(fields);
                let entries = StructArray::try_new(
                    fields.clone(),
                    entries.columns().to_vec(),
                    entries.nulls().cloned(),
                )?;
                (
                    Arc::new(field_with_type(&f, DataType::Struct(fields))),
                    entries,
                )
            } else {
                (f, entries.clone())
            };
            let ma = MapArray::try_new(
                Arc::clone(&f),
                offsets,
                entries,
                ma.nulls().cloned(),
                *sorted,
            )?;
            (DataType::Map(f, *sorted), Arc::new(ma))
        }
        other => return Err(unsupported_layout(path, other)),
    };
    Ok((field_with_type(field, data_type), rebuilt))
}

/// Keep only child spans belonging to visible list/map rows. Null rows may
/// have nonempty spans containing arbitrary placeholders; unlike fixed-size
/// lists, their offsets can be collapsed without changing logical values.
/// Removing those spans also preserves non-nullable map entries and keys.
fn visible_list_child<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    child: &ArrayRef,
    nulls: Option<&NullBuffer>,
) -> (OffsetBuffer<O>, ArrayRef) {
    let Some(nulls) = nulls.filter(|n| n.null_count() > 0) else {
        return trim_list_child(offsets, child);
    };
    let visible_offsets =
        OffsetBuffer::<O>::from_lengths(offsets.windows(2).enumerate().map(|(row, pair)| {
            if nulls.is_valid(row) {
                (pair[1] - pair[0]).as_usize()
            } else {
                0
            }
        }));
    let visible_len = visible_offsets[visible_offsets.len() - 1].as_usize();
    if visible_len == (offsets[offsets.len() - 1] - offsets[0]).as_usize() {
        return trim_list_child(offsets, child);
    }
    // Unlike selection kernels, ArrayData packing retains encoded child fields.
    let data = child.to_data();
    let mut visible = MutableArrayData::new(vec![&data], false, visible_len);
    for (start, end) in nulls.valid_slices() {
        visible.extend(0, offsets[start].as_usize(), offsets[end].as_usize());
    }
    (visible_offsets, make_array(visible.freeze()))
}

/// The field a leaf-holding child of `parent` (a struct) is rewritten as.
///
/// The array walk pushes the struct's nulls into such a child, so a child
/// declared non-nullable under a nullable struct (legal Arrow: its slots
/// under a null row are simply masked) comes out holding nulls. It is
/// widened to nullable here, so the rewritten field
/// admits the nulls its values now carry — and a sink deriving its column
/// types from the field (ClickHouse `Nullable(..)`) declares them.
fn under_struct_nulls(child: &Field, parent: &Field) -> Field {
    if parent.is_nullable() && !child.is_nullable() {
        child.clone().with_nullable(true)
    } else {
        child.clone()
    }
}

/// `array` with `parent`'s nulls OR-ed into its own validity.
fn with_parent_nulls(array: &ArrayRef, parent: Option<&NullBuffer>) -> Result<ArrayRef> {
    let Some(parent) = parent else {
        return Ok(Arc::clone(array));
    };
    let merged = NullBuffer::union(Some(parent), array.nulls());
    let data = array.to_data().into_builder().nulls(merged).build()?;
    Ok(make_array(data))
}

/// The walk found a decimal_arb leaf under a layout it does not rebuild
/// (today: a `Union`, which the config-load validator already rejects).
fn unsupported_layout(path: &str, layout: &DataType) -> crate::error::StreamlingError {
    streamling_err!(
        "decimal_arb inside a {:?} column ('{}') cannot be converted; flatten the column \
         in a transform",
        layout,
        path,
    )
}

/// Shared fixture for the per-format nested decimal_arb tests: the plugin
/// call-trace shape with 256-bit integer leaves at their boundaries.
#[cfg(test)]
pub(crate) mod fixtures {
    use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, NativeIntKind};
    use arrow::array::{ArrayRef, Int64Array, ListArray, RecordBatch, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow_schema::{DataType, Field, Fields, Schema};
    use std::sync::Arc;

    pub(crate) const U256_MAX: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    pub(crate) const I256_MIN: &str =
        "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
    pub(crate) const I256_MAX: &str =
        "57896044618658097711785492504343953926634992332820282019728792003956564819967";

    fn hinted(name: &str, kind: NativeIntKind) -> Arc<Field> {
        Arc::new(
            DecimalArbType::with_native_int_kind(
                DecimalArbType::field(name, 78, 0, true).unwrap(),
                kind,
            )
            .unwrap(),
        )
    }

    pub(crate) fn leaves(values: &[Option<&str>]) -> ArrayRef {
        let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), "v", 78, 0).unwrap();
        for v in values {
            match v {
                Some(s) => b.append_str(s).unwrap(),
                None => b.append_null(),
            }
        }
        let (raw, _, _) = b.finish().into_inner();
        Arc::new(raw)
    }

    /// Two rows:
    /// - `traces: List<Struct<value: decimal_arb(78, 0) u256>>` —
    ///   `[{1}, {10^18}]` and `[{2^256 - 1}, {null}]`;
    /// - `signed: List<decimal_arb(78, 0) i256>` — `[-1, 0]` and
    ///   `[-2^255, 2^255 - 1]`.
    pub(crate) fn wide_int_traces_batch() -> RecordBatch {
        let value = hinted("value", NativeIntKind::U256);
        let trace_fields: Fields = vec![value].into();
        let trace = Arc::new(Field::new(
            "item",
            DataType::Struct(trace_fields.clone()),
            true,
        ));
        let structs = StructArray::try_new(
            trace_fields,
            vec![leaves(&[
                Some("1"),
                Some("1000000000000000000"),
                Some(U256_MAX),
                None,
            ])],
            None,
        )
        .unwrap();
        let traces = ListArray::try_new(
            Arc::clone(&trace),
            OffsetBuffer::new(vec![0, 2, 4].into()),
            Arc::new(structs),
            None,
        )
        .unwrap();
        let signed_leaf = hinted("item", NativeIntKind::I256);
        let signed = ListArray::try_new(
            Arc::clone(&signed_leaf),
            OffsetBuffer::new(vec![0, 2, 4].into()),
            leaves(&[Some("-1"), Some("0"), Some(I256_MIN), Some(I256_MAX)]),
            None,
        )
        .unwrap();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("traces", DataType::List(trace), false),
                Field::new("signed", DataType::List(signed_leaf), false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(traces),
                Arc::new(signed),
            ],
        )
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{U256_MAX, leaves as leaf_array};
    use super::*;
    use crate::formats::decimal_arb_text::decimal_arb_to_strings;
    use arrow::array::{AsArray, Int32Array, StringArray, StructArray as SA, new_empty_array};
    use arrow::buffer::OffsetBuffer;
    use arrow::compute::cast;

    fn leaf_field(name: &str) -> FieldRef {
        Arc::new(DecimalArbType::field(name, 78, 0, true).unwrap())
    }

    fn strings(array: &ArrayRef) -> Vec<Option<String>> {
        let s = array.as_any().downcast_ref::<StringArray>().unwrap();
        s.iter().map(|v| v.map(str::to_string)).collect()
    }

    /// Rewrites every leaf to canonical text, tagging its field with the path
    /// it was reached by, and returns the non-null values the leaf rewrite
    /// was handed: values hidden by a container's null bitmap must not be.
    fn rewrite(field: &Field, array: &ArrayRef) -> (Field, ArrayRef, Vec<String>) {
        let mut visited = Vec::new();
        let (f, a) = rewrite_decimal_arb_leaves(field, array, field.name(), &mut |f, a, path| {
            let text = decimal_arb_to_strings(f, a).unwrap();
            visited.extend(text.iter().flatten().map(str::to_string));
            Ok((
                Field::new(f.name(), DataType::Utf8, f.is_nullable())
                    .with_metadata([("path".to_string(), path.to_string())].into()),
                Arc::new(text),
            ))
        })
        .unwrap();
        assert_eq!(f.data_type(), a.data_type());
        a.to_data().validate_full().unwrap();
        (f, a, visited)
    }

    fn run_sibling_struct(custom_fields: bool) -> ArrayRef {
        use arrow::array::RunArray;
        use arrow::datatypes::Int32Type;
        let runs = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1, 2, 3]),
            &Int32Array::from(vec![11, 22, 33]),
        )
        .unwrap();
        let sibling = if custom_fields {
            let named = |name, nullable| {
                Arc::new(
                    Field::new(name, DataType::Int32, nullable)
                        .with_metadata([("producer".into(), "custom".into())].into()),
                )
            };
            make_array(
                runs.to_data()
                    .into_builder()
                    .data_type(DataType::RunEndEncoded(
                        named("ends", false),
                        named("payload", false),
                    ))
                    .build()
                    .unwrap(),
            )
        } else {
            Arc::new(runs) as ArrayRef
        };
        Arc::new(
            SA::try_new(
                vec![
                    leaf_field("amount"),
                    Arc::new(Field::new("sibling", sibling.data_type().clone(), true)),
                ]
                .into(),
                vec![leaf_array(&[Some("1"), Some("7"), Some(U256_MAX)]), sibling],
                None,
            )
            .unwrap(),
        )
    }

    #[test]
    fn masked_containers_preserve_run_end_sibling_fields_and_values() {
        let values = run_sibling_struct(true);
        let original = values.as_struct().fields()[1].clone();
        let item = Arc::new(Field::new("item", values.data_type().clone(), true));
        let nulls = Some(NullBuffer::from(vec![true, false, true]));
        let fixed: ArrayRef = Arc::new(
            FixedSizeListArray::try_new(item.clone(), 1, values.clone(), nulls.clone()).unwrap(),
        );
        let list: ArrayRef = Arc::new(
            ListArray::try_new(
                item.clone(),
                OffsetBuffer::new(vec![0, 1, 2, 3].into()),
                values.clone(),
                nulls.clone(),
            )
            .unwrap(),
        );
        let large: ArrayRef = Arc::new(
            LargeListArray::try_new(
                item,
                OffsetBuffer::new(vec![0_i64, 1, 2, 3].into()),
                values.clone(),
                nulls.clone(),
            )
            .unwrap(),
        );
        let entries = SA::try_new(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(Field::new("value", values.data_type().clone(), true)),
            ]
            .into(),
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"])), values],
            None,
        )
        .unwrap();
        let map: ArrayRef = Arc::new(
            MapArray::try_new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(vec![0, 1, 2, 3].into()),
                entries,
                nulls,
                false,
            )
            .unwrap(),
        );
        for array in [fixed, list, large, map] {
            for input in [array.clone(), array.slice(1, 2)] {
                let field = Field::new("container", input.data_type().clone(), true);
                let (_, output, visited) = rewrite(&field, &input);
                assert_eq!(visited.last().unwrap(), U256_MAX);
                for row in 0..output.len() {
                    if output.is_null(row) {
                        continue;
                    }
                    let structs = match output.data_type() {
                        DataType::FixedSizeList(_, _) => output.as_fixed_size_list().value(row),
                        DataType::List(_) => output.as_list::<i32>().value(row),
                        DataType::LargeList(_) => output.as_list::<i64>().value(row),
                        DataType::Map(_, _) => {
                            let entries = output.as_map().value(row);
                            entries.column(1).clone()
                        }
                        _ => unreachable!(),
                    };
                    assert_eq!(structs.as_struct().fields()[1], original);
                    let sibling =
                        cast(structs.as_struct().column(1).as_ref(), &DataType::Int32).unwrap();
                    let expected = if input.len() == 3 && row == 0 { 11 } else { 33 };
                    assert_eq!(
                        sibling
                            .as_primitive::<arrow::datatypes::Int32Type>()
                            .value(0),
                        expected
                    );
                }
            }
        }
    }

    #[test]
    fn all_null_fixed_lists_restore_run_end_siblings_without_indices() {
        for custom in [false, true] {
            let values = run_sibling_struct(custom);
            let sibling = values.as_struct().fields()[1].clone();
            let array: ArrayRef = Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("item", values.data_type().clone(), true)),
                    1,
                    values,
                    Some(NullBuffer::from(vec![false, false, false])),
                )
                .unwrap(),
            );
            let field = Field::new("f", array.data_type().clone(), true);
            let (_, output, visited) = rewrite(&field, &array);
            assert!(visited.is_empty());
            assert_eq!(output.logical_null_count(), 3);
            let values = output.as_fixed_size_list().values();
            assert_eq!(values.logical_null_count(), 3);
            assert_eq!(values.as_struct().fields()[1], sibling);
        }
    }

    fn dictionary_key_map(null_value: bool) -> ArrayRef {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;
        let keys: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1]),
                leaf_array(&[Some(U256_MAX), if null_value { None } else { Some("7") }]),
            )
            .unwrap(),
        );
        let key = Field::new("key", keys.data_type().clone(), false)
            .with_metadata(leaf_field("key").metadata().clone());
        let entries = SA::try_new(
            vec![
                Arc::new(key.clone().with_nullable(true)),
                Arc::new(Field::new("value", DataType::Utf8, false)),
            ]
            .into(),
            vec![keys, Arc::new(StringArray::from(vec!["max", "seven"]))],
            None,
        )
        .unwrap();
        // ArrayData validates physical dictionary validity; MapArray also
        // accepts these non-null indices even when a decoded value is null.
        let entries = make_array(
            entries
                .to_data()
                .into_builder()
                .data_type(DataType::Struct(
                    vec![
                        Arc::new(key),
                        Arc::new(Field::new("value", DataType::Utf8, false)),
                    ]
                    .into(),
                ))
                .build()
                .unwrap(),
        )
        .as_struct()
        .clone();
        Arc::new(
            MapArray::try_new(
                Arc::new(
                    Field::new("entries", entries.data_type().clone(), false)
                        .with_metadata([("owner".into(), "producer".into())].into()),
                ),
                OffsetBuffer::new(vec![0, 2].into()),
                entries,
                None,
                true,
            )
            .unwrap(),
        )
    }

    #[test]
    fn dictionary_decimal_map_keys_remain_non_nullable() {
        let array = dictionary_key_map(false);
        let field = Field::new("m", array.data_type().clone(), false);
        for input in [array.clone(), new_empty_array(array.data_type())] {
            let (rewritten, output, _) = rewrite(&field, &input);
            let DataType::Map(entries, sorted) = rewritten.data_type() else {
                panic!()
            };
            assert!(*sorted);
            assert_eq!(entries.name(), "entries");
            assert_eq!(entries.metadata().get("owner").unwrap(), "producer");
            let DataType::Struct(fields) = entries.data_type() else {
                panic!()
            };
            assert!(!fields[0].is_nullable());
            assert_eq!(fields[0].name(), "key");
            assert_eq!(fields[0].metadata().get("path").unwrap(), "m.entries.key");
            if !input.is_empty() {
                assert_eq!(
                    strings(output.as_map().keys()),
                    [Some(U256_MAX.into()), Some("7".into())]
                );
            }
        }
    }

    #[test]
    fn dictionary_decimal_map_keys_reject_decoded_nulls() {
        let array = dictionary_key_map(true);
        let field = Field::new("m", array.data_type().clone(), false);
        let error = rewrite_decimal_arb_leaves(&field, &array, "m", &mut |f, a, _| {
            Ok((
                field_with_type(f, DataType::Utf8),
                Arc::new(decimal_arb_to_strings(f, a)?),
            ))
        })
        .unwrap_err();
        assert!(error.to_string().contains("m.entries.key"), "{error}");
        assert!(error.to_string().contains("null"), "{error}");
    }

    #[test]
    fn null_lists_and_maps_skip_hidden_leaf_values() {
        let item = Arc::new(DecimalArbType::field("item", 78, 0, false).unwrap());
        let list: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::clone(&item),
                OffsetBuffer::new(vec![0, 1, 3, 4, 4].into()),
                leaf_array(&[Some("-1"), Some("7"), Some("8"), Some("-1")]),
                Some(NullBuffer::from(vec![false, true, false, true])),
            )
            .unwrap(),
        );
        let large = cast(list.as_ref(), &DataType::LargeList(Arc::clone(&item))).unwrap();
        for array in [Arc::clone(&list), large] {
            let field = Field::new("l", array.data_type().clone(), true);
            let (_, hidden, visited) = rewrite(&field, &array.slice(0, 1));
            assert!(visited.is_empty());
            assert!(hidden.is_null(0));
            let (sliced_field, sliced, visited) = rewrite(&field, &array.slice(1, 2));
            assert_eq!(visited, ["7", "8"]);
            assert!(sliced.is_null(1));
            let (DataType::List(item) | DataType::LargeList(item)) = sliced_field.data_type()
            else {
                panic!("{sliced_field:?}")
            };
            let sliced = cast(sliced.as_ref(), &DataType::List(Arc::clone(item))).unwrap();
            assert_eq!(sliced.as_list::<i32>().value_offsets(), &[0, 2, 2]);
            let (rewritten_field, rewritten, visited) = rewrite(&field, &array);
            assert_eq!(visited, ["7", "8"]);
            assert_eq!(rewritten.len(), 4);
            assert!(rewritten.is_null(0));
            assert!(rewritten.is_null(2));
            let (DataType::List(text_item) | DataType::LargeList(text_item)) =
                rewritten_field.data_type()
            else {
                panic!("{rewritten_field:?}")
            };
            let rewritten =
                cast(rewritten.as_ref(), &DataType::List(Arc::clone(text_item))).unwrap();
            let rewritten = rewritten.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(rewritten.value_offsets(), &[0, 0, 2, 2, 2]);
            assert!(matches!(rewritten.data_type(), DataType::List(child) if !child.is_nullable()));
            assert_eq!(
                strings(rewritten.values()),
                [Some("7".into()), Some("8".into())]
            );
        }

        // Nulls inherited from a struct must reach a list's descendants too.
        let nonnull_list: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::clone(&item),
                OffsetBuffer::new(vec![0, 1, 2].into()),
                leaf_array(&[Some("-1"), Some("7")]),
                None,
            )
            .unwrap(),
        );
        let child = Arc::new(Field::new("l", nonnull_list.data_type().clone(), false));
        let structs: ArrayRef = Arc::new(
            SA::try_new(
                vec![child].into(),
                vec![nonnull_list],
                Some(NullBuffer::from(vec![false, true])),
            )
            .unwrap(),
        );
        let field = Field::new("s", structs.data_type().clone(), true);
        let (_, _, visited) = rewrite(&field, &structs);
        assert_eq!(visited, ["7"]);

        let key = Arc::new(Field::new("key", DataType::Utf8, false));
        let value = Arc::new(item.as_ref().clone().with_name("value"));
        let entries = SA::try_new(
            vec![key, value].into(),
            vec![
                Arc::new(StringArray::from(vec!["hidden", "kept"])),
                leaf_array(&[Some("-1"), Some("7")]),
            ],
            None,
        )
        .unwrap();
        let entry = Arc::new(Field::new("entries", entries.data_type().clone(), false));
        let map: ArrayRef = Arc::new(
            MapArray::try_new(
                entry,
                OffsetBuffer::new(vec![0, 1, 2].into()),
                entries,
                Some(NullBuffer::from(vec![false, true])),
                false,
            )
            .unwrap(),
        );
        let field = Field::new("m", map.data_type().clone(), true);
        let (_, rewritten, visited) = rewrite(&field, &map);
        assert_eq!(visited, ["7"]);
        let rewritten = rewritten.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(rewritten.value_offsets(), &[0, 0, 1]);
        assert_eq!(rewritten.keys().null_count(), 0);
        assert!(!rewritten.entries().fields()[0].is_nullable());
        assert!(!rewritten.entries().fields()[1].is_nullable());

        // Restoring hidden fixed-size-list slots must also work for a map
        // child, whose entries and keys can never be made nullable.
        let unmasked_map = make_array(map.to_data().into_builder().nulls(None).build().unwrap());
        let item = Arc::new(Field::new("item", unmasked_map.data_type().clone(), false));
        let fixed: ArrayRef = Arc::new(
            FixedSizeListArray::try_new(
                item,
                1,
                unmasked_map,
                Some(NullBuffer::from(vec![false, true])),
            )
            .unwrap(),
        );
        let field = Field::new("f", fixed.data_type().clone(), true);
        let (field, rewritten, visited) = rewrite(&field, &fixed);
        assert_eq!(visited, ["7"]);
        let DataType::FixedSizeList(item, _) = field.data_type() else {
            panic!()
        };
        assert!(!item.is_nullable());
        let fixed = rewritten
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert!(fixed.is_null(0));
        let map = fixed.values().as_any().downcast_ref::<MapArray>().unwrap();
        assert!(map.is_null(0));
        assert_eq!(map.keys().null_count(), 0);
        assert!(!map.entries().fields()[0].is_nullable());
    }

    #[test]
    fn null_fixed_size_lists_convert_only_visible_elements() {
        let item = Arc::new(DecimalArbType::field("item", 78, 0, false).unwrap());
        for valid in [vec![false, true, false], vec![false, false, false]] {
            let array: ArrayRef = Arc::new(
                FixedSizeListArray::try_new(
                    Arc::clone(&item),
                    2,
                    leaf_array(&[
                        Some("-1"),
                        Some("-1"),
                        Some("7"),
                        Some("8"),
                        Some("-1"),
                        Some("-1"),
                    ]),
                    Some(NullBuffer::from(valid.clone())),
                )
                .unwrap(),
            );
            let field = Field::new("f", array.data_type().clone(), true);
            let (field, rewritten, visited) = rewrite(&field, &array);
            assert_eq!(visited, if valid[1] { vec!["7", "8"] } else { vec![] });
            let DataType::FixedSizeList(child, _) = field.data_type() else {
                panic!()
            };
            assert!(!child.is_nullable());
            let rewritten = rewritten
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            assert_eq!(rewritten.len(), 3);
            assert_eq!(rewritten.value_length(), 2);
            for (row, valid) in valid.into_iter().enumerate() {
                assert_eq!(rewritten.is_valid(row), valid);
                if valid {
                    assert_eq!(
                        strings(&rewritten.value(row)),
                        [Some("7".into()), Some("8".into())]
                    );
                }
            }
        }
        let empty: ArrayRef = Arc::new(
            FixedSizeListArray::try_new_with_length(
                item,
                0,
                leaf_array(&[]),
                Some(NullBuffer::from(vec![false, true, false])),
                3,
            )
            .unwrap(),
        );
        let field = Field::new("empty", empty.data_type().clone(), true);
        let (_, empty, visited) = rewrite(&field, &empty);
        assert_eq!(empty.len(), 3);
        assert!(visited.is_empty());
    }

    #[test]
    fn list_of_struct_leaves_are_rewritten_with_nulls_and_slicing() {
        let value = leaf_field("value");
        let id = Arc::new(Field::new("id", DataType::Int32, true));
        let item = Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::clone(&id), Arc::clone(&value)].into()),
            true,
        ));
        // Five structs: the third is null (its leaf slot holds a value the
        // producer left there), the fourth has a null leaf.
        let structs = SA::try_new(
            vec![id, value].into(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                leaf_array(&[
                    Some("1"),
                    Some("1000000000000000000"),
                    Some("7"),
                    None,
                    Some(U256_MAX),
                ]),
            ],
            Some(NullBuffer::from(vec![true, true, false, true, true])),
        )
        .unwrap();
        // Rows: [s0, s1], null, [], [s2, s3, s4]
        let list = ListArray::try_new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0, 2, 2, 2, 5].into()),
            Arc::new(structs),
            Some(NullBuffer::from(vec![true, false, true, true])),
        )
        .unwrap();
        let field = Field::new("traces", DataType::List(item), true);
        let array: ArrayRef = Arc::new(list);

        let (f, a, _) = rewrite(&field, &array);
        let DataType::List(item) = f.data_type() else {
            panic!("{f:?}")
        };
        let DataType::Struct(children) = item.data_type() else {
            panic!("{item:?}")
        };
        assert_eq!(children[1].data_type(), &DataType::Utf8);
        assert_eq!(
            children[1].metadata().get("path").map(String::as_str),
            Some("traces.item.value")
        );
        let la = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(la.is_null(1));
        let leaves = la
            .values()
            .as_any()
            .downcast_ref::<SA>()
            .unwrap()
            .column(1)
            .clone();
        assert_eq!(
            strings(&leaves),
            vec![
                Some("1".into()),
                Some("1000000000000000000".into()),
                None, // masked by the null struct
                None,
                Some(U256_MAX.into()),
            ]
        );

        // A sliced column keeps its window.
        let sliced = array.slice(2, 2);
        let (_, a, _) = rewrite(&field, &sliced);
        let la = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(la.len(), 2);
        assert_eq!(la.value_length(0), 0);
        let last = la.value(1);
        let leaves = last
            .as_any()
            .downcast_ref::<SA>()
            .unwrap()
            .column(1)
            .clone();
        assert_eq!(strings(&leaves)[2].as_deref(), Some(U256_MAX));
    }

    #[test]
    fn large_list_fixed_size_list_and_map_values_are_rewritten() {
        let value = leaf_field("item");
        let large = LargeListArray::try_new(
            Arc::clone(&value),
            OffsetBuffer::new(vec![0_i64, 1, 3].into()),
            leaf_array(&[Some("-5"), None, Some("10")]),
            None,
        )
        .unwrap();
        let field = Field::new("l", DataType::LargeList(Arc::clone(&value)), false);
        let (_, a, _) = rewrite(&field, &(Arc::new(large) as ArrayRef));
        let la = a.as_any().downcast_ref::<LargeListArray>().unwrap();
        assert_eq!(
            strings(la.values()),
            vec![Some("-5".into()), None, Some("10".into())]
        );

        let fixed = FixedSizeListArray::try_new(
            Arc::clone(&value),
            2,
            leaf_array(&[Some("1"), Some("2"), Some("3"), Some("4")]),
            None,
        )
        .unwrap();
        let field = Field::new("f", DataType::FixedSizeList(Arc::clone(&value), 2), false);
        let (_, a, _) = rewrite(&field, &(Arc::new(fixed) as ArrayRef));
        let fa = a.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(
            strings(&fa.value(1)),
            vec![Some("3".into()), Some("4".into())]
        );

        let key = Arc::new(Field::new("key", DataType::Utf8, false));
        let val = leaf_field("value");
        let entries = SA::try_new(
            vec![Arc::clone(&key), Arc::clone(&val)].into(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                leaf_array(&[Some("12345678901234567890123"), None]),
            ],
            None,
        )
        .unwrap();
        let entry_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![key, val].into()),
            false,
        ));
        let map = MapArray::try_new(
            Arc::clone(&entry_field),
            OffsetBuffer::new(vec![0, 2].into()),
            entries,
            None,
            false,
        )
        .unwrap();
        let field = Field::new("m", DataType::Map(entry_field, false), true);
        let (f, a, _) = rewrite(&field, &(Arc::new(map) as ArrayRef));
        let DataType::Map(entries, _) = f.data_type() else {
            panic!("{f:?}")
        };
        let DataType::Struct(kv) = entries.data_type() else {
            panic!("{entries:?}")
        };
        assert_eq!(
            kv[1].metadata().get("path").map(String::as_str),
            Some("m.entries.value")
        );
        let ma = a.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(
            strings(ma.values()),
            vec![Some("12345678901234567890123".into()), None]
        );
    }

    #[test]
    fn list_view_is_rewritten_through_its_plain_layout() {
        let value = leaf_field("item");
        let list: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::clone(&value),
                OffsetBuffer::new(vec![0, 1].into()),
                leaf_array(&[Some("42")]),
                None,
            )
            .unwrap(),
        );
        let view = cast(list.as_ref(), &DataType::ListView(Arc::clone(&value))).unwrap();
        let field = Field::new("v", DataType::ListView(value), true);
        let (f, a, _) = rewrite(&field, &view);
        assert!(matches!(f.data_type(), DataType::List(_)), "{f:?}");
        let la = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(strings(la.values()), vec![Some("42".into())]);
    }

    /// `ree: RunEndEncoded<Int32, values: decimal_arb(78, 0)>` holding
    /// `[1, 1, null, 10^18]`.
    fn run_end_encoded_leaf() -> (Field, ArrayRef) {
        use arrow::array::RunArray;
        use arrow::datatypes::Int32Type;
        let values = leaf_field("values");
        let run_ends = Int32Array::from(vec![2, 3, 4]);
        let ree = RunArray::<Int32Type>::try_new(
            &run_ends,
            leaf_array(&[Some("1"), None, Some("1000000000000000000")]).as_ref(),
        )
        .unwrap();
        let DataType::RunEndEncoded(run_ends_field, _) = ree.data_type().clone() else {
            unreachable!()
        };
        let field = Field::new("ree", DataType::RunEndEncoded(run_ends_field, values), true);
        // Carry the leaf's metadata in the array's own type, as a producer does.
        let data = ree
            .to_data()
            .into_builder()
            .data_type(field.data_type().clone())
            .build()
            .unwrap();
        (field, make_array(data))
    }

    #[test]
    fn run_end_encoded_leaf_keeps_its_decimal_arb_metadata() {
        // The leaf's extension metadata lives on the run-end encoding's
        // values field; unwrapping to the plain type used to keep only the
        // outer field's (empty) metadata, so the leaf was never converted and
        // its canonical bytes went out as-is.
        let (field, array) = run_end_encoded_leaf();
        let (f, a, _) = rewrite(&field, &array);
        assert_eq!(f.data_type(), &DataType::Utf8);
        assert_eq!(f.name(), "ree");
        assert_eq!(f.metadata().get("path").map(String::as_str), Some("ree"));
        assert_eq!(
            strings(&a),
            vec![
                Some("1".into()),
                Some("1".into()),
                None,
                Some("1000000000000000000".into()),
            ]
        );

        // Under a nullable struct: the struct's nulls cannot be pushed into
        // the encoded child itself, so it is unwrapped first.
        let s = SA::try_new(
            vec![Arc::new(field)].into(),
            vec![array],
            Some(NullBuffer::from(vec![true, false, true, true])),
        )
        .unwrap();
        let s_field = Field::new("s", s.data_type().clone(), true);
        let (_, a, _) = rewrite(&s_field, &(Arc::new(s) as ArrayRef));
        let leaf = a.as_any().downcast_ref::<SA>().unwrap().column(0).clone();
        assert_eq!(
            strings(&leaf),
            vec![
                Some("1".into()),
                None, // masked by the null struct
                None,
                Some("1000000000000000000".into()),
            ]
        );
    }

    #[test]
    fn sliced_lists_and_maps_convert_only_the_elements_they_own() {
        // A 1000-row List<decimal_arb>, one element per row.
        let n = 1000;
        let item = leaf_field("item");
        let texts: Vec<String> = (0..n).map(|i| i.to_string()).collect();
        let leaves: Vec<Option<&str>> = texts.iter().map(|t| Some(t.as_str())).collect();
        let list = ListArray::try_new(
            Arc::clone(&item),
            OffsetBuffer::from_lengths(std::iter::repeat_n(1, n)),
            leaf_array(&leaves),
            None,
        )
        .unwrap();
        let field = Field::new("l", DataType::List(item), true);
        let array: ArrayRef = Arc::new(list);

        let sliced = array.slice(500, 2);
        let (_, a, visited) = rewrite(&field, &sliced);
        assert_eq!(
            visited,
            ["500", "501"],
            "only the slice's own elements are converted"
        );
        let la = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(la.len(), 2);
        assert_eq!(la.value_offsets(), &[0, 1, 2]);
        assert_eq!(strings(&la.value(0)), vec![Some("500".into())]);
        assert_eq!(strings(&la.value(1)), vec![Some("501".into())]);

        // A FixedSizeList's child is windowed by arrow when it is sliced
        // (`FixedSizeListArray::slice` and `From<ArrayData>` both take
        // `values.slice(offset * size, len * size)`), so the walk converts
        // only the slice's own elements here too.
        let fixed = cast(
            array.as_ref(),
            &DataType::FixedSizeList(leaf_field("item"), 1),
        )
        .unwrap();
        let fixed_field = Field::new("l", fixed.data_type().clone(), true);
        let (_, a, visited) = rewrite(&fixed_field, &fixed.slice(700, 3));
        assert_eq!(
            visited.len(),
            3,
            "only the slice's own elements are converted"
        );
        let fa = a.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(fa.len(), 3);
        assert_eq!(strings(&fa.value(2)), vec![Some("702".into())]);
        // The same slice taken through `ArrayData`, as a sink receives it.
        let (_, a, _) = rewrite(&fixed_field, &make_array(fixed.to_data().slice(700, 3)));
        let fa = a.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(
            strings(fa.values()),
            vec![Some("700".into()), Some("701".into()), Some("702".into())]
        );

        // Same for a LargeList and a Map.
        let large = cast(array.as_ref(), &DataType::LargeList(leaf_field("item"))).unwrap();
        let large_field = Field::new("l", large.data_type().clone(), true);
        let (_, a, visited) = rewrite(&large_field, &large.slice(10, 3));
        assert_eq!(visited.len(), 3);
        let la = a.as_any().downcast_ref::<LargeListArray>().unwrap();
        assert_eq!(strings(&la.value(2)), vec![Some("12".into())]);

        let key = Arc::new(Field::new("key", DataType::Utf8, false));
        let val = leaf_field("value");
        let entries = SA::try_new(
            vec![Arc::clone(&key), Arc::clone(&val)].into(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                leaf_array(&[Some("1"), Some("2"), Some("3"), Some("4")]),
            ],
            None,
        )
        .unwrap();
        let entry_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![key, val].into()),
            false,
        ));
        let map: ArrayRef = Arc::new(
            MapArray::try_new(
                Arc::clone(&entry_field),
                OffsetBuffer::new(vec![0, 2, 3, 4].into()),
                entries,
                None,
                false,
            )
            .unwrap(),
        );
        let map_field = Field::new("m", DataType::Map(entry_field, false), true);
        let (_, a, visited) = rewrite(&map_field, &map.slice(1, 1));
        assert_eq!(visited.len(), 1);
        let ma = a.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(strings(ma.values()), vec![Some("3".into())]);
    }

    /// The leaf of each struct in `item: Struct<value>` (one list row
    /// holding them all), with the rewritten leaf's field.
    fn struct_leaf(field: &Field, array: &ArrayRef) -> (FieldRef, ArrayRef) {
        let (f, a, _) = rewrite(field, array);
        let DataType::List(item) = f.data_type() else {
            panic!("{f:?}")
        };
        let DataType::Struct(children) = item.data_type() else {
            panic!("{item:?}")
        };
        let la = a.as_any().downcast_ref::<ListArray>().unwrap();
        let leaf = la
            .values()
            .as_any()
            .downcast_ref::<SA>()
            .unwrap()
            .column(0)
            .clone();
        (Arc::clone(&children[0]), leaf)
    }

    #[test]
    fn non_nullable_leaf_under_a_nullable_struct_is_rewritten_as_nullable() {
        // `value` is declared non-nullable inside a nullable struct — legal
        // Arrow: its slot under a null struct is masked, not null. Pushing
        // the struct's nulls down makes that slot null, so the rewritten
        // leaf must be declared nullable for the field to admit its values.
        let value = Arc::new(DecimalArbType::field("value", 78, 0, false).unwrap());
        let list_of = |nullable_struct: bool, nulls: Option<NullBuffer>| {
            let item = Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Arc::clone(&value)].into()),
                nullable_struct,
            ));
            let structs = SA::try_new(
                vec![Arc::clone(&value)].into(),
                vec![leaf_array(&[Some("1"), Some("7"), Some("3")])],
                nulls,
            )
            .unwrap();
            let list = ListArray::try_new(
                Arc::clone(&item),
                OffsetBuffer::new(vec![0, 3].into()),
                Arc::new(structs),
                None,
            )
            .unwrap();
            (
                Field::new("traces", DataType::List(item), false),
                Arc::new(list) as ArrayRef,
            )
        };

        let (field, array) = list_of(true, Some(NullBuffer::from(vec![true, false, true])));
        let (leaf_field, leaf) = struct_leaf(&field, &array);
        assert_eq!(
            strings(&leaf),
            vec![Some("1".into()), None, Some("3".into())]
        );
        assert!(leaf_field.is_nullable(), "{leaf_field:?}");
        assert_eq!(leaf_field.is_nullable(), leaf.null_count() > 0);

        // The declaration does not depend on the batch: a nullable struct
        // with no null row in it widens the leaf all the same, so every
        // batch agrees.
        let (field, array) = list_of(true, None);
        let (leaf_field, leaf) = struct_leaf(&field, &array);
        assert_eq!(leaf.null_count(), 0);
        assert!(leaf_field.is_nullable(), "{leaf_field:?}");

        // A non-nullable struct has no nulls to push down: the leaf keeps
        // its declaration.
        let (field, array) = list_of(false, None);
        let (leaf_field, leaf) = struct_leaf(&field, &array);
        assert_eq!(leaf.null_count(), 0);
        assert!(!leaf_field.is_nullable(), "{leaf_field:?}");
    }

    /// `Dictionary<Int32, LargeBinary>` holding `[7, 7, null, 2^256 - 1]`,
    /// with the decimal_arb metadata on the dictionary field itself — the
    /// Arrow convention for a dictionary-encoded extension type.
    fn dictionary_encoded_leaf(name: &str) -> (Field, ArrayRef) {
        let plain = leaf_array(&[Some("7"), Some("7"), None, Some(U256_MAX)]);
        let dict = cast(
            plain.as_ref(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeBinary)),
        )
        .unwrap();
        let field = Field::new(name, dict.data_type().clone(), true)
            .with_metadata(leaf_field(name).metadata().clone());
        (field, dict)
    }

    #[test]
    fn dictionary_encoded_leaf_with_metadata_on_its_field_is_rewritten() {
        let expected = vec![
            Some("7".to_string()),
            Some("7".to_string()),
            None,
            Some(U256_MAX.to_string()),
        ];

        // As a column of its own.
        let (field, array) = dictionary_encoded_leaf("amt");
        assert!(contains_nested_decimal_arb(&field));
        let (f, a, _) = rewrite(&field, &array);
        assert_eq!(f.data_type(), &DataType::Utf8);
        assert_eq!(f.metadata().get("path").map(String::as_str), Some("amt"));
        assert_eq!(strings(&a), expected);

        // As a list's items.
        let (item, values) = dictionary_encoded_leaf("item");
        let item = Arc::new(item);
        let list = ListArray::try_new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0, 4].into()),
            values,
            None,
        )
        .unwrap();
        let field = Field::new("l", DataType::List(item), true);
        assert!(contains_nested_decimal_arb(&field));
        let (f, a, _) = rewrite(&field, &(Arc::new(list) as ArrayRef));
        let DataType::List(item) = f.data_type() else {
            panic!("{f:?}")
        };
        assert_eq!(
            item.metadata().get("path").map(String::as_str),
            Some("l.item")
        );
        let la = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(strings(la.values()), expected);

        // As a struct child: unwrapped before the struct's nulls go in.
        let (child, values) = dictionary_encoded_leaf("v");
        let s = SA::try_new(
            vec![Arc::new(child)].into(),
            vec![values],
            Some(NullBuffer::from(vec![true, false, true, true])),
        )
        .unwrap();
        let s_field = Field::new("s", s.data_type().clone(), true);
        let (_, a, _) = rewrite(&s_field, &(Arc::new(s) as ArrayRef));
        let leaf = a.as_any().downcast_ref::<SA>().unwrap().column(0).clone();
        assert_eq!(
            strings(&leaf),
            vec![expected[0].clone(), None, None, expected[3].clone()]
        );
    }

    #[test]
    fn columns_without_leaves_pass_through_and_unions_are_refused() {
        let plain = Field::new(
            "p",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        );
        assert!(!contains_nested_decimal_arb(&plain));
        assert!(!contains_nested_decimal_arb(
            &DecimalArbType::field("top", 10, 0, true).unwrap()
        ));
        assert_eq!(
            rewrite(&plain, &new_empty_array(plain.data_type())).0,
            plain
        );

        let union = Field::new(
            "u",
            DataType::Union(
                arrow_schema::UnionFields::try_new(vec![0], vec![leaf_field("amt")]).unwrap(),
                arrow_schema::UnionMode::Dense,
            ),
            true,
        );
        assert!(contains_nested_decimal_arb(&union));
        let empty = new_empty_array(union.data_type());
        let err = rewrite_decimal_arb_leaves(&union, &empty, "u", &mut |_, _, _| unreachable!())
            .unwrap_err();
        assert!(err.to_string().contains("'u'"), "{err}");
    }
}
