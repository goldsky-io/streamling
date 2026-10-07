//! Rebuild a column with every `decimal_arb` leaf nested below it rewritten.
//!
//! A sink that stores containers in a native layout (ClickHouse `Array` /
//! `Tuple` / `Map`) cannot hand a nested decimal_arb leaf over as its
//! canonical `LargeBinary` bytes: each leaf has to become the concrete type
//! the destination declares for it. These walks rebuild the containers around
//! the leaves and leave the per-leaf decision to the caller. They cover the
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
    field_contains_decimal_arb, field_with_type, plain_layout_field, to_plain_layout,
    trim_list_child,
};
use crate::streamling_err;
use crate::types::decimal_arb::DecimalArbType;
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
    make_array,
};
use arrow::buffer::NullBuffer;
use arrow_schema::{DataType, Field, FieldRef, Fields};
use std::sync::Arc;

/// Rewrites one decimal_arb leaf, given its field and dotted path, into the
/// field that replaces it.
pub type LeafFieldRewrite<'a> = dyn FnMut(&Field, &str) -> Result<Field> + 'a;

/// Rewrites one decimal_arb leaf, given its field, values and dotted path,
/// into the field and values that replace it.
pub type LeafRewrite<'a> = dyn FnMut(&Field, &ArrayRef, &str) -> Result<(Field, ArrayRef)> + 'a;

/// Does `field` hold a `decimal_arb` leaf *below* it (it is a container, or a
/// dictionary- / run-end-encoded leaf, not a plain decimal_arb column itself)?
pub fn contains_nested_decimal_arb(field: &Field) -> bool {
    !DecimalArbType::is_decimal_arb_field(field) && field_contains_decimal_arb(field)
}

/// Schema-only mirror of [`rewrite_decimal_arb_leaves`]: the field `field`
/// becomes once `leaf` has rewritten every decimal_arb leaf in it. Fields
/// without a decimal_arb leaf come back unchanged.
pub fn rewrite_decimal_arb_leaf_fields(
    field: &Field,
    path: &str,
    leaf: &mut LeafFieldRewrite<'_>,
) -> Result<Field> {
    if DecimalArbType::is_decimal_arb_field(field) {
        return leaf(field, path);
    }
    if !field_contains_decimal_arb(field) {
        return Ok(field.clone());
    }
    if let Some(plain) = plain_layout_field(field) {
        return rewrite_decimal_arb_leaf_fields(&plain, path, leaf);
    }
    let mut child = |c: &FieldRef| -> Result<FieldRef> {
        let child_path = format!("{}.{}", path, c.name());
        Ok(Arc::new(rewrite_decimal_arb_leaf_fields(
            c,
            &child_path,
            leaf,
        )?))
    };
    let data_type = match field.data_type() {
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|c| {
                    if field_contains_decimal_arb(c) {
                        child(&Arc::new(under_struct_nulls(c, field)))
                    } else {
                        Ok(Arc::clone(c))
                    }
                })
                .collect::<Result<Fields>>()?,
        ),
        DataType::List(c) => DataType::List(child(c)?),
        DataType::LargeList(c) => DataType::LargeList(child(c)?),
        DataType::FixedSizeList(c, n) => DataType::FixedSizeList(child(c)?, *n),
        DataType::Map(c, sorted) => DataType::Map(child(c)?, *sorted),
        other => return Err(unsupported_layout(path, other)),
    };
    Ok(field_with_type(field, data_type))
}

/// Rebuild `(field, array)` with every decimal_arb leaf below it replaced by
/// what `leaf` returns for it; containers keep their offsets and validity. A
/// struct's nulls are pushed down to its children first, so `leaf` sees a
/// slot under a null struct as null rather than whatever placeholder bytes
/// the producer left there; a child of a nullable struct that receives them
/// is declared nullable. Fields without a decimal_arb leaf come back
/// unchanged.
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
            let (offsets, values) = trim_list_child(la.offsets(), la.values());
            let (f, values) = child(c, &values)?;
            let la = ListArray::try_new(Arc::clone(&f), offsets, values, la.nulls().cloned())?;
            (DataType::List(f), Arc::new(la))
        }
        DataType::LargeList(c) => {
            let la = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| downcast_err("LargeListArray"))?;
            let (offsets, values) = trim_list_child(la.offsets(), la.values());
            let (f, values) = child(c, &values)?;
            let la = LargeListArray::try_new(Arc::clone(&f), offsets, values, la.nulls().cloned())?;
            (DataType::LargeList(f), Arc::new(la))
        }
        DataType::FixedSizeList(c, n) => {
            let fa = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| downcast_err("FixedSizeListArray"))?;
            let (f, values) = child(c, fa.values())?;
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
            let (offsets, entries) = trim_list_child(ma.offsets(), &entries);
            let (f, entries) = child(entry_field, &entries)?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray (map entries)"))?
                .clone();
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

/// The field a leaf-holding child of `parent` (a struct) is rewritten as.
///
/// The array walk pushes the struct's nulls into such a child, so a child
/// declared non-nullable under a nullable struct (legal Arrow: its slots
/// under a null row are simply masked) comes out holding nulls. It is
/// widened to nullable here, in both walks alike, so the rewritten field
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

    fn leaves(values: &[Option<&str>]) -> ArrayRef {
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
    use super::*;
    use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbValue};
    use arrow::array::{Int32Array, LargeBinaryArray, StringArray, StructArray as SA};
    use arrow::buffer::OffsetBuffer;
    use arrow::compute::cast;

    fn leaf_field(name: &str) -> FieldRef {
        Arc::new(DecimalArbType::field(name, 78, 0, true).unwrap())
    }

    fn leaf_array(values: &[Option<&str>]) -> ArrayRef {
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

    /// The test leaf rewrite: decimal_arb → canonical text, tagging the
    /// field with the path it was reached by.
    fn to_text(field: &Field, array: &ArrayRef, path: &str) -> Result<(Field, ArrayRef)> {
        let lb = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        let (_, scale) = DecimalArbType::precision_scale_from_field(field).unwrap();
        let text: StringArray = (0..lb.len())
            .map(|i| {
                (!lb.is_null(i)).then(|| {
                    DecimalArbValue::from_canonical_bytes_at_scale(lb.value(i), scale)
                        .unwrap()
                        .to_canonical_string()
                })
            })
            .collect();
        Ok((
            Field::new(field.name(), DataType::Utf8, field.is_nullable())
                .with_metadata([("path".to_string(), path.to_string())].into()),
            Arc::new(text),
        ))
    }

    fn to_text_field(field: &Field, path: &str) -> Result<Field> {
        Ok(
            Field::new(field.name(), DataType::Utf8, field.is_nullable())
                .with_metadata([("path".to_string(), path.to_string())].into()),
        )
    }

    fn strings(array: &ArrayRef) -> Vec<Option<String>> {
        let s = array.as_any().downcast_ref::<StringArray>().unwrap();
        s.iter().map(|v| v.map(str::to_string)).collect()
    }

    /// Rewrites through both walks and checks they agree on the field.
    fn rewrite(field: &Field, array: &ArrayRef) -> (Field, ArrayRef) {
        let (f, a) = rewrite_decimal_arb_leaves(field, array, field.name(), &mut to_text).unwrap();
        let schema_only =
            rewrite_decimal_arb_leaf_fields(field, field.name(), &mut to_text_field).unwrap();
        assert_eq!(f, schema_only, "schema and array walks disagree");
        assert_eq!(f.data_type(), a.data_type());
        (f, a)
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
                    Some("115792089237316195423570985008687907853269984665640564039457584007913129639935"),
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

        let (f, a) = rewrite(&field, &array);
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
                Some("115792089237316195423570985008687907853269984665640564039457584007913129639935".into()),
            ]
        );

        // A sliced column keeps its window.
        let sliced = array.slice(2, 2);
        let (_, a) = rewrite(&field, &sliced);
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
        assert_eq!(
            strings(&leaves)[2].as_deref(),
            Some("115792089237316195423570985008687907853269984665640564039457584007913129639935")
        );
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
        let (_, a) = rewrite(&field, &(Arc::new(large) as ArrayRef));
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
        let (_, a) = rewrite(&field, &(Arc::new(fixed) as ArrayRef));
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
        let (f, a) = rewrite(&field, &(Arc::new(map) as ArrayRef));
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
        let (f, a) = rewrite(&field, &view);
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
        let (f, a) = rewrite(&field, &array);
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
        let (_, a) = rewrite(&s_field, &(Arc::new(s) as ArrayRef));
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

        let mut seen = 0;
        let mut counting = |f: &Field, a: &ArrayRef, path: &str| {
            seen += a.len();
            to_text(f, a, path)
        };
        let sliced = array.slice(500, 2);
        let (_, a) = rewrite_decimal_arb_leaves(&field, &sliced, "l", &mut counting).unwrap();
        assert_eq!(seen, 2, "only the slice's own elements are converted");
        let la = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(la.len(), 2);
        assert_eq!(la.value_offsets(), &[0, 1, 2]);
        assert_eq!(strings(&la.value(0)), vec![Some("500".into())]);
        assert_eq!(strings(&la.value(1)), vec![Some("501".into())]);

        // Same for a LargeList and a Map.
        let large = cast(array.as_ref(), &DataType::LargeList(leaf_field("item"))).unwrap();
        let large_field = Field::new("l", large.data_type().clone(), true);
        seen = 0;
        let mut counting = |f: &Field, a: &ArrayRef, path: &str| {
            seen += a.len();
            to_text(f, a, path)
        };
        let (_, a) =
            rewrite_decimal_arb_leaves(&large_field, &large.slice(10, 3), "l", &mut counting)
                .unwrap();
        assert_eq!(seen, 3);
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
        seen = 0;
        let mut counting = |f: &Field, a: &ArrayRef, path: &str| {
            seen += a.len();
            to_text(f, a, path)
        };
        let (_, a) =
            rewrite_decimal_arb_leaves(&map_field, &map.slice(1, 1), "m", &mut counting).unwrap();
        assert_eq!(seen, 1);
        let ma = a.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(strings(ma.values()), vec![Some("3".into())]);
    }

    /// The leaf of each struct in `item: Struct<value>` (one list row
    /// holding them all), with the rewritten leaf's field.
    fn struct_leaf(field: &Field, array: &ArrayRef) -> (FieldRef, ArrayRef) {
        let (f, a) = rewrite(field, array);
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
        // batch (and the schema-only walk `rewrite` checks against) agrees.
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
        let plain = leaf_array(&[
            Some("7"),
            Some("7"),
            None,
            Some("115792089237316195423570985008687907853269984665640564039457584007913129639935"),
        ]);
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
            Some(
                "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                    .to_string(),
            ),
        ];

        // As a column of its own.
        let (field, array) = dictionary_encoded_leaf("amt");
        assert!(contains_nested_decimal_arb(&field));
        let (f, a) = rewrite(&field, &array);
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
        let (f, a) = rewrite(&field, &(Arc::new(list) as ArrayRef));
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
        let (_, a) = rewrite(&s_field, &(Arc::new(s) as ArrayRef));
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
            rewrite_decimal_arb_leaf_fields(&plain, "p", &mut to_text_field).unwrap(),
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
        let err = rewrite_decimal_arb_leaf_fields(&union, "u", &mut to_text_field).unwrap_err();
        assert!(err.to_string().contains("'u'"), "{err}");
    }
}
