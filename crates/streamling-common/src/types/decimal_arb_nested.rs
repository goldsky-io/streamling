//! Rebuild a column with every `decimal_arb` leaf nested below it rewritten.
//!
//! A sink that stores containers in a native layout (ClickHouse `Array` /
//! `Tuple` / `Map`) cannot hand a nested decimal_arb leaf over as its
//! canonical `LargeBinary` bytes: each leaf has to become the concrete type
//! the destination declares for it. These walks rebuild the containers around
//! the leaves and leave the per-leaf decision to the caller. They cover the
//! same layouts as the text bridge in [`crate::formats::decimal_arb_text`]:
//! list views and dictionary / run-end encodings are cast to their plain
//! layout first, and a leaf under a union is refused.
//!
//! Leaves are addressed by a dotted path from the column (`traces.item.value`),
//! the same form the config-load validator reports.

use crate::error::Result;
use crate::formats::decimal_arb_text::{field_contains_decimal_arb, field_with_type, plain_layout};
use crate::streamling_err;
use crate::types::decimal_arb::DecimalArbType;
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
    make_array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow_schema::{DataType, Field, FieldRef, Fields};
use std::sync::Arc;

/// Rewrites one decimal_arb leaf, given its field and dotted path, into the
/// field that replaces it.
pub type LeafFieldRewrite<'a> = dyn FnMut(&Field, &str) -> Result<Field> + 'a;

/// Rewrites one decimal_arb leaf, given its field, values and dotted path,
/// into the field and values that replace it.
pub type LeafRewrite<'a> = dyn FnMut(&Field, &ArrayRef, &str) -> Result<(Field, ArrayRef)> + 'a;

/// Does `field` hold a `decimal_arb` leaf *below* it (it is a container, not
/// a decimal_arb column itself)?
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
    if let Some(plain) = plain_layout(field.data_type()) {
        return rewrite_decimal_arb_leaf_fields(&field_with_type(field, plain), path, leaf);
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
                .map(&mut child)
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
/// the producer left there. Fields without a decimal_arb leaf come back
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
    if let Some(plain) = plain_layout(field.data_type()) {
        let plain_array = cast(array.as_ref(), &plain)?;
        return rewrite_decimal_arb_leaves(
            &field_with_type(field, plain),
            &plain_array,
            path,
            leaf,
        );
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
            for (c, column) in children.iter().zip(sa.columns()) {
                let (f, a) = if field_contains_decimal_arb(c) {
                    child(c, &with_parent_nulls(column, sa.nulls())?)?
                } else {
                    (Arc::clone(c), Arc::clone(column))
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
            let (f, values) = child(c, la.values())?;
            let la = ListArray::try_new(
                Arc::clone(&f),
                la.offsets().clone(),
                values,
                la.nulls().cloned(),
            )?;
            (DataType::List(f), Arc::new(la))
        }
        DataType::LargeList(c) => {
            let la = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| downcast_err("LargeListArray"))?;
            let (f, values) = child(c, la.values())?;
            let la = LargeListArray::try_new(
                Arc::clone(&f),
                la.offsets().clone(),
                values,
                la.nulls().cloned(),
            )?;
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
            let (f, entries) = child(entry_field, &entries)?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray (map entries)"))?
                .clone();
            let ma = MapArray::try_new(
                Arc::clone(&f),
                ma.offsets().clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbValue};
    use arrow::array::{Int32Array, LargeBinaryArray, StringArray, StructArray as SA};
    use arrow::buffer::OffsetBuffer;

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
