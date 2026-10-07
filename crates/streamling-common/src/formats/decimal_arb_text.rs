//! Recursive `decimal_arb` ⇄ canonical-decimal-text bridge.
//!
//! `decimal_arb` is `LargeBinary` underneath, so any format that carries it as
//! text — JSON, and the Arrow IPC payload handed to script transforms — has to
//! convert the leaves explicitly. Doing it by `cast` instead reinterprets the
//! raw bytes: a decimal string becomes canonical sign/magnitude bytes, and
//! canonical bytes become hex. Both directions below walk Struct / List /
//! LargeList / FixedSizeList / Map so *nested* leaves get the same treatment as
//! top-level ones.

use crate::streamling_err;
use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue};
use arrow_schema::{DataType, Field, Fields};
use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeBinaryArray, LargeListArray, ListArray, MapArray,
    OffsetSizeTrait, StringArray, StructArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::cast;
use datafusion::common::{DataFusionError, Result};
use std::str::FromStr;
use std::sync::Arc;

/// Returns `true` if `field` is `decimal_arb`, or contains a `decimal_arb`
/// leaf nested anywhere inside a Struct / List / LargeList / FixedSizeList /
/// Map. Used to decide whether a batch needs the decimal_arb → Utf8 rewrite
/// before text serialization.
pub(crate) fn field_contains_decimal_arb(field: &Field) -> bool {
    DecimalArbType::is_decimal_arb_field(field)
        || is_encoded_decimal_arb_field(field)
        || type_contains_decimal_arb(field.data_type())
}

/// Is `field` a dictionary- or run-end-encoded `decimal_arb` leaf whose
/// extension metadata sits on the encoded field itself?
///
/// That is the Arrow convention for a dictionary-encoded extension type: the
/// field carries the extension name and the dictionary type, whose value type
/// is the storage type. [`DecimalArbType::is_decimal_arb_field`] requires a
/// `LargeBinary` type and so does not see such a leaf; unwrapped to its plain
/// layout (see [`plain_layout_field`], which keeps the field's metadata) it
/// is an ordinary decimal_arb leaf.
pub(crate) fn is_encoded_decimal_arb_field(field: &Field) -> bool {
    matches!(
        field.data_type(),
        DataType::Dictionary(..) | DataType::RunEndEncoded(..)
    ) && DecimalArbType::is_decimal_arb_metadata(field.metadata())
        && plain_layout_field(field)
            .is_some_and(|plain| DecimalArbType::is_decimal_arb_field(&plain))
}

/// Does a container type hold a `decimal_arb` field anywhere below it? (A
/// bare `DataType` cannot itself carry the extension metadata.)
fn type_contains_decimal_arb(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(children) => children.iter().any(|f| field_contains_decimal_arb(f)),
        DataType::List(c)
        | DataType::LargeList(c)
        | DataType::FixedSizeList(c, _)
        | DataType::ListView(c)
        | DataType::LargeListView(c)
        | DataType::Map(c, _) => field_contains_decimal_arb(c),
        DataType::Dictionary(_, values) => type_contains_decimal_arb(values),
        DataType::RunEndEncoded(_, values) => field_contains_decimal_arb(values),
        DataType::Union(fields, _) => fields.iter().any(|(_, f)| field_contains_decimal_arb(f)),
        _ => false,
    }
}

/// The plain (offset-based, un-encoded) layout `data_type` maps to: list views
/// become lists and dictionary / run-end encodings unwrap to their value type.
/// Everything the text bridge walks is expressed in these layouts; the
/// encoded variants are `cast` to them first.
pub(crate) fn plain_layout(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::ListView(c) => Some(DataType::List(Arc::clone(c))),
        DataType::LargeListView(c) => Some(DataType::LargeList(Arc::clone(c))),
        DataType::Dictionary(_, values) => {
            Some(plain_layout(values).unwrap_or_else(|| values.as_ref().clone()))
        }
        DataType::RunEndEncoded(_, values) => {
            Some(plain_layout(values.data_type()).unwrap_or_else(|| values.data_type().clone()))
        }
        _ => None,
    }
}

/// [`plain_layout`] at the field level: `field` rebuilt with its plain layout,
/// or `None` when it already is one.
///
/// A run-end encoding is the one layout whose values sit in a field of their
/// own, so a decimal_arb leaf held directly by it carries its extension
/// metadata on that values field. Rebuilding the outer field with only the
/// unwrapped type dropped that metadata: the leaf was no longer recognised as
/// decimal_arb, and its canonical bytes went out as an opaque binary/string
/// value. The unwrapped field takes the values field's metadata instead (and
/// the outer field's name, which is the one the batch carries).
pub(crate) fn plain_layout_field(field: &Field) -> Option<Field> {
    match field.data_type() {
        DataType::Dictionary(_, values) => {
            // Keep unwrapping at the field level: a dictionary's values may
            // themselves be run-end encoded, with extension metadata and
            // nullability on their own values field. Unwrapping only the
            // DataType loses both before the run-end branch can see them.
            let values = field_with_type(field, values.as_ref().clone());
            Some(plain_layout_field(&values).unwrap_or(values))
        }
        DataType::RunEndEncoded(_, values) => {
            let values = plain_layout_field(values).unwrap_or_else(|| values.as_ref().clone());
            // The run-end array carries no validity of its own; its nulls are
            // the values field's, which the unwrapped field has to admit.
            let nullable = field.is_nullable() || values.is_nullable();
            Some(if DecimalArbType::is_decimal_arb_field(&values) {
                Field::new(field.name(), values.data_type().clone(), nullable)
                    .with_metadata(values.metadata().clone())
            } else {
                field_with_type(field, values.data_type().clone()).with_nullable(nullable)
            })
        }
        other => plain_layout(other).map(|plain| field_with_type(field, plain)),
    }
}

/// `(field, array)` cast to its plain layout (see [`plain_layout_field`]), or
/// `None` when it already is one.
pub(crate) fn to_plain_layout(
    field: &Field,
    array: &ArrayRef,
) -> std::result::Result<Option<(Field, ArrayRef)>, arrow_schema::ArrowError> {
    let Some(plain) = plain_layout_field(field) else {
        return Ok(None);
    };
    let plain_array = cast(array.as_ref(), plain.data_type())?;
    Ok(Some((plain, plain_array)))
}

/// The part of a list's (or map's) child that its offsets reference, with
/// the offsets rebased to start at zero.
///
/// Slicing a `ListArray` / `LargeListArray` / `MapArray` slices only its
/// offsets; the child keeps every element of the original array. A walk that
/// rewrites the child as a whole therefore converts — and range-checks —
/// elements that belong to rows outside the slice, once per slice. Sinks
/// routinely receive such zero-copy slices (repartitioning hands each output
/// partition a slice of one gathered batch), so the walks trim the child to
/// the window first.
pub(crate) fn trim_list_child<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    child: &ArrayRef,
) -> (OffsetBuffer<O>, ArrayRef) {
    // An offset buffer always holds at least one entry.
    let first = offsets[0];
    let last = offsets[offsets.len() - 1];
    let (start, end) = (first.as_usize(), last.as_usize());
    if start == 0 && end == child.len() {
        return (offsets.clone(), Arc::clone(child));
    }
    let rebased: Vec<O> = offsets.iter().map(|o| *o - first).collect();
    (
        OffsetBuffer::new(rebased.into()),
        child.slice(start, end - start),
    )
}

/// Rebuild `orig` with a new `DataType`, preserving its name, nullability, and
/// metadata.
pub(crate) fn field_with_type(orig: &Field, data_type: DataType) -> Field {
    Field::new(orig.name(), data_type, orig.is_nullable()).with_metadata(orig.metadata().clone())
}

/// Convert a `decimal_arb` `LargeBinaryArray` to a `StringArray` of canonical
/// decimal text (nulls preserved), reading the scale from the field metadata.
pub(crate) fn decimal_arb_to_strings(field: &Field, array: &ArrayRef) -> Result<StringArray> {
    let (_, scale) = DecimalArbType::precision_scale_from_field(field).ok_or_else(|| {
        DataFusionError::from(streamling_err!(
            "decimal_arb field '{}' missing precision/scale metadata",
            field.name(),
        ))
    })?;
    let lba = array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            DataFusionError::from(streamling_err!(
                "expected LargeBinaryArray for decimal_arb field '{}', got {:?}",
                field.name(),
                array.data_type(),
            ))
        })?;
    let mut values: Vec<Option<String>> = Vec::with_capacity(lba.len());
    for row_idx in 0..lba.len() {
        if lba.is_null(row_idx) {
            values.push(None);
        } else {
            let value = DecimalArbValue::from_canonical_bytes_at_scale(lba.value(row_idx), scale)?;
            values.push(Some(value.to_canonical_string()));
        }
    }
    Ok(StringArray::from(values))
}

/// Recursively rewrite `(field, array)` so every `decimal_arb` leaf — top-level
/// or nested inside Struct / List / LargeList / FixedSizeList / Map — becomes a
/// Utf8 canonical-decimal string as canonical decimal text. Non-decimal_arb leaves and
/// containers without any decimal_arb descendant are returned unchanged.
pub(crate) fn decimal_arb_leaves_to_text(
    field: &Field,
    array: &ArrayRef,
) -> Result<(Field, ArrayRef)> {
    if DecimalArbType::is_decimal_arb_field(field) {
        let strings = decimal_arb_to_strings(field, array)?;
        return Ok((
            Field::new(field.name(), DataType::Utf8, field.is_nullable()),
            Arc::new(strings) as ArrayRef,
        ));
    }

    // Containers with no decimal_arb descendant pass through untouched.
    if !field_contains_decimal_arb(field) {
        return Ok((field.clone(), array.clone()));
    }

    // A list view or a dictionary-/run-end-encoded container: the arrow-json
    // writer rendered the leaves inside these as hex because the walk below
    // never reached them. Cast to the plain layout and walk that instead.
    if let Some((plain, plain_array)) =
        to_plain_layout(field, array).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
    {
        return decimal_arb_leaves_to_text(&plain, &plain_array);
    }

    let downcast_err = |what: &str| {
        DataFusionError::from(streamling_err!(
            "expected {} for field '{}', got {:?}",
            what,
            field.name(),
            array.data_type(),
        ))
    };

    match field.data_type() {
        DataType::Struct(children) => {
            let sa = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray"))?;
            let mut new_fields: Vec<Arc<Field>> = Vec::with_capacity(children.len());
            let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(children.len());
            for (child, col) in children.iter().zip(sa.columns()) {
                let (nf, na) = decimal_arb_leaves_to_text(child, col)?;
                new_fields.push(Arc::new(nf));
                new_cols.push(na);
            }
            let fields: Fields = new_fields.into();
            let new_arr = StructArray::new(fields.clone(), new_cols, sa.nulls().cloned());
            Ok((
                field_with_type(field, DataType::Struct(fields)),
                Arc::new(new_arr) as ArrayRef,
            ))
        }
        DataType::List(child) => {
            let la = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| downcast_err("ListArray"))?;
            let (offsets, values) = trim_list_child(la.offsets(), la.values());
            let (nf, nv) = decimal_arb_leaves_to_text(child, &values)?;
            let nf = Arc::new(nf);
            let new_arr = ListArray::new(nf.clone(), offsets, nv, la.nulls().cloned());
            Ok((
                field_with_type(field, DataType::List(nf)),
                Arc::new(new_arr) as ArrayRef,
            ))
        }
        DataType::LargeList(child) => {
            let la = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| downcast_err("LargeListArray"))?;
            let (offsets, values) = trim_list_child(la.offsets(), la.values());
            let (nf, nv) = decimal_arb_leaves_to_text(child, &values)?;
            let nf = Arc::new(nf);
            let new_arr = LargeListArray::new(nf.clone(), offsets, nv, la.nulls().cloned());
            Ok((
                field_with_type(field, DataType::LargeList(nf)),
                Arc::new(new_arr) as ArrayRef,
            ))
        }
        DataType::FixedSizeList(child, n) => {
            let fa = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| downcast_err("FixedSizeListArray"))?;
            let (nf, nv) = decimal_arb_leaves_to_text(child, fa.values())?;
            let nf = Arc::new(nf);
            // The length is given explicitly: `FixedSizeListArray::new` derives
            // it from the values, which for a zero-width list means zero rows.
            let new_arr = FixedSizeListArray::try_new_with_length(
                nf.clone(),
                *n,
                nv,
                fa.nulls().cloned(),
                fa.len(),
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            Ok((
                field_with_type(field, DataType::FixedSizeList(nf, *n)),
                Arc::new(new_arr) as ArrayRef,
            ))
        }
        DataType::Map(entry_field, sorted) => {
            let ma = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| downcast_err("MapArray"))?;
            let entries: ArrayRef = Arc::new(ma.entries().clone());
            let (offsets, entries) = trim_list_child(ma.offsets(), &entries);
            let (nef, nea) = decimal_arb_leaves_to_text(entry_field, &entries)?;
            let nef = Arc::new(nef);
            let new_entries = nea
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray (map entries)"))?
                .clone();
            let new_arr = MapArray::new(
                nef.clone(),
                offsets,
                new_entries,
                ma.nulls().cloned(),
                *sorted,
            );
            Ok((
                field_with_type(field, DataType::Map(nef, *sorted)),
                Arc::new(new_arr) as ArrayRef,
            ))
        }
        // `field_contains_decimal_arb` saw a leaf under a layout this walk
        // does not rebuild (today: a `Union`). Passing the array through here
        // handed canonical bytes to the text writer, which rendered them as
        // hex — a wrong value with no error. Refuse instead.
        other => Err(DataFusionError::from(streamling_err!(
            "decimal_arb inside a {:?} column ('{}') cannot be serialised as text; \
             flatten the column in a transform",
            other,
            field.name(),
        ))),
    }
}

/// Field-level mirror of [`decimalize_for_json`]: every `decimal_arb` leaf —
/// nested ones included — becomes `Utf8` in the schema handed to the text-oriented
/// reader.
///
/// Without this the reader sees `LargeBinary` and *hex-decodes* the JSON string:
/// `"001000"` became `0x00 0x10 0x00` = 4096 instead of one thousand, and
/// `"12.34"` failed outright as invalid hex. Only top-level fields were
/// rewritten before, so exactly the nested leaves fell into that path.
pub(crate) fn decimal_arb_leaves_as_text_field(field: &Field) -> Field {
    if DecimalArbType::is_decimal_arb_field(field) || is_encoded_decimal_arb_field(field) {
        return Field::new(field.name(), DataType::Utf8, field.is_nullable());
    }
    if !field_contains_decimal_arb(field) {
        return field.clone();
    }
    match field.data_type() {
        DataType::Struct(children) => {
            let rewritten: Fields = children
                .iter()
                .map(|c| Arc::new(decimal_arb_leaves_as_text_field(c)))
                .collect::<Vec<_>>()
                .into();
            field_with_type(field, DataType::Struct(rewritten))
        }
        DataType::List(child) => field_with_type(
            field,
            DataType::List(Arc::new(decimal_arb_leaves_as_text_field(child))),
        ),
        DataType::LargeList(child) => field_with_type(
            field,
            DataType::LargeList(Arc::new(decimal_arb_leaves_as_text_field(child))),
        ),
        DataType::FixedSizeList(child, n) => field_with_type(
            field,
            DataType::FixedSizeList(Arc::new(decimal_arb_leaves_as_text_field(child)), *n),
        ),
        DataType::ListView(child) => field_with_type(
            field,
            DataType::ListView(Arc::new(decimal_arb_leaves_as_text_field(child))),
        ),
        DataType::LargeListView(child) => field_with_type(
            field,
            DataType::LargeListView(Arc::new(decimal_arb_leaves_as_text_field(child))),
        ),
        DataType::Map(entry_field, sorted) => field_with_type(
            field,
            DataType::Map(
                Arc::new(decimal_arb_leaves_as_text_field(entry_field)),
                *sorted,
            ),
        ),
        _ => field.clone(),
    }
}

/// `source` with the metadata of `target`'s decimal_arb leaves filled in
/// wherever the source leaf is bare `LargeBinary`.
///
/// Bytes that lost their metadata in transit are read at the target's scale —
/// the long-standing assumption for a metadata-less payload — while a leaf
/// that kept its own metadata keeps it: a different scale on the way in is a
/// real re-encoding, not a relabel. Struct children are paired by name.
pub(crate) fn overlay_decimal_arb_metadata(source: &Field, target: &Field) -> Field {
    if DecimalArbType::is_decimal_arb_field(target) {
        return match source.data_type() {
            DataType::LargeBinary if !DecimalArbType::is_decimal_arb_field(source) => {
                Field::new(source.name(), DataType::LargeBinary, source.is_nullable())
                    .with_metadata(target.metadata().clone())
            }
            _ => source.clone(),
        };
    }
    let child = |s: &Arc<Field>, t: &Arc<Field>| Arc::new(overlay_decimal_arb_metadata(s, t));
    match (source.data_type(), target.data_type()) {
        (DataType::Struct(sc), DataType::Struct(tc)) => {
            let children: Vec<Arc<Field>> = sc
                .iter()
                .map(|s| match tc.iter().find(|t| t.name() == s.name()) {
                    Some(t) => child(s, t),
                    None => Arc::clone(s),
                })
                .collect();
            field_with_type(source, DataType::Struct(children.into()))
        }
        (
            DataType::List(s),
            DataType::List(t) | DataType::LargeList(t) | DataType::FixedSizeList(t, _),
        ) => field_with_type(source, DataType::List(child(s, t))),
        (
            DataType::LargeList(s),
            DataType::List(t) | DataType::LargeList(t) | DataType::FixedSizeList(t, _),
        ) => field_with_type(source, DataType::LargeList(child(s, t))),
        (
            DataType::FixedSizeList(s, n),
            DataType::List(t) | DataType::LargeList(t) | DataType::FixedSizeList(t, _),
        ) => field_with_type(source, DataType::FixedSizeList(child(s, t), *n)),
        (DataType::Map(s, sorted), DataType::Map(t, _)) => {
            field_with_type(source, DataType::Map(child(s, t), *sorted))
        }
        _ => source.clone(),
    }
}

/// Inverse of [`decimalize_for_json`]: rebuild every `decimal_arb` leaf of
/// `target` from the canonical-decimal strings the reader produced.
pub(crate) fn decimal_arb_leaves_from_text(target: &Field, array: &ArrayRef) -> Result<ArrayRef> {
    let downcast_err = |what: &str| {
        DataFusionError::from(streamling_err!(
            "expected {} for field '{}', got {:?}",
            what,
            target.name(),
            array.data_type(),
        ))
    };

    if DecimalArbType::is_decimal_arb_field(target) {
        let (precision, scale) =
            DecimalArbType::precision_scale_from_field(target).ok_or_else(|| {
                DataFusionError::from(streamling_err!(
                    "decimal_arb field '{}' missing precision/scale metadata",
                    target.name(),
                ))
            })?;
        let strings = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| downcast_err("StringArray"))?;
        let mut builder =
            DecimalArbArrayBuilder::with_capacity(strings.len(), target.name(), precision, scale)?;
        for row_idx in 0..strings.len() {
            if strings.is_null(row_idx) {
                builder.append_null();
            } else {
                builder.append_value(&DecimalArbValue::from_str(strings.value(row_idx))?)?;
            }
        }
        let (raw, _, _) = builder.finish().into_inner();
        return Ok(Arc::new(raw) as ArrayRef);
    }

    // An encoded leaf is restored as its plain decimal_arb layout, then
    // re-encoded the way the target declares it.
    if is_encoded_decimal_arb_field(target)
        && let Some(plain) = plain_layout_field(target)
    {
        let restored = decimal_arb_leaves_from_text(&plain, array)?;
        return Ok(cast(restored.as_ref(), target.data_type())?);
    }

    if !field_contains_decimal_arb(target) {
        return Ok(array.clone());
    }

    match target.data_type() {
        DataType::Struct(children) => {
            let sa = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray"))?;
            let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(children.len());
            for (child, col) in children.iter().zip(sa.columns()) {
                new_cols.push(decimal_arb_leaves_from_text(child, col)?);
            }
            Ok(Arc::new(StructArray::new(
                children.clone(),
                new_cols,
                sa.nulls().cloned(),
            )) as ArrayRef)
        }
        DataType::List(child) => {
            let la = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| downcast_err("ListArray"))?;
            let values = decimal_arb_leaves_from_text(child, la.values())?;
            Ok(Arc::new(ListArray::new(
                child.clone(),
                la.offsets().clone(),
                values,
                la.nulls().cloned(),
            )) as ArrayRef)
        }
        DataType::LargeList(child) => {
            let la = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| downcast_err("LargeListArray"))?;
            let values = decimal_arb_leaves_from_text(child, la.values())?;
            Ok(Arc::new(LargeListArray::new(
                child.clone(),
                la.offsets().clone(),
                values,
                la.nulls().cloned(),
            )) as ArrayRef)
        }
        DataType::FixedSizeList(child, n) => {
            let fa = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| downcast_err("FixedSizeListArray"))?;
            let values = decimal_arb_leaves_from_text(child, fa.values())?;
            Ok(Arc::new(
                FixedSizeListArray::try_new_with_length(
                    child.clone(),
                    *n,
                    values,
                    fa.nulls().cloned(),
                    fa.len(),
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            ) as ArrayRef)
        }
        DataType::Map(entry_field, sorted) => {
            let ma = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| downcast_err("MapArray"))?;
            let entries: ArrayRef = Arc::new(ma.entries().clone());
            let new_entries = decimal_arb_leaves_from_text(entry_field, &entries)?;
            let new_entries = new_entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray (map entries)"))?
                .clone();
            Ok(Arc::new(MapArray::new(
                entry_field.clone(),
                ma.offsets().clone(),
                new_entries,
                ma.nulls().cloned(),
                *sorted,
            )) as ArrayRef)
        }
        // Mirror of the forward direction: a decimal_arb leaf under a layout
        // the walk does not rebuild would otherwise come back as whatever the
        // text reader produced for it.
        other => Err(DataFusionError::from(streamling_err!(
            "decimal_arb inside a {:?} column ('{}') cannot be restored from text; \
             flatten the column in a transform",
            other,
            target.name(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decimal_arb::DecimalArbArrayBuilder;
    use datafusion::arrow::array::{Int32Array, RunArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::Int32Type;

    fn leaves(values: &[&str]) -> ArrayRef {
        let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), "v", 78, 0).unwrap();
        for v in values {
            b.append_str(v).unwrap();
        }
        let (raw, _, _) = b.finish().into_inner();
        Arc::new(raw)
    }

    fn texts(array: &ArrayRef) -> Vec<Option<String>> {
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect()
    }

    /// A sliced list or map shares its whole child with the array it was cut
    /// from; the text bridge converts only the window the slice owns.
    #[test]
    fn sliced_lists_and_maps_convert_only_their_own_elements() {
        let item = Arc::new(DecimalArbType::field("item", 78, 0, true).unwrap());
        let all: Vec<String> = (0..100).map(|i| i.to_string()).collect();
        let list: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::clone(&item),
                OffsetBuffer::from_lengths(std::iter::repeat_n(1, 100)),
                leaves(&all.iter().map(String::as_str).collect::<Vec<_>>()),
                None,
            )
            .unwrap(),
        );
        let field = Field::new("l", DataType::List(item), true);
        let (f, a) = decimal_arb_leaves_to_text(&field, &list.slice(40, 2)).unwrap();
        let la = a.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            la.values().len(),
            2,
            "only the slice's elements are rewritten"
        );
        assert_eq!(la.value_offsets(), &[0, 1, 2]);
        assert_eq!(
            texts(la.values()),
            vec![Some("40".into()), Some("41".into())]
        );
        assert_eq!(f.data_type(), a.data_type());

        // A sliced FixedSizeList's child is already windowed by arrow.
        let fixed = cast(
            list.as_ref(),
            &DataType::FixedSizeList(
                Arc::new(DecimalArbType::field("item", 78, 0, true).unwrap()),
                1,
            ),
        )
        .unwrap();
        let field = Field::new("f", fixed.data_type().clone(), true);
        let (_, a) = decimal_arb_leaves_to_text(&field, &fixed.slice(40, 2)).unwrap();
        let fa = a.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(fa.len(), 2);
        assert_eq!(
            texts(fa.values()),
            vec![Some("40".into()), Some("41".into())]
        );

        let key = Arc::new(Field::new("key", DataType::Utf8, false));
        let value = Arc::new(DecimalArbType::field("value", 78, 0, true).unwrap());
        let entry = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![Arc::clone(&key), Arc::clone(&value)].into()),
            false,
        ));
        let entries = StructArray::try_new(
            vec![key, value].into(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                leaves(&["1", "2", "3"]),
            ],
            None,
        )
        .unwrap();
        let map: ArrayRef = Arc::new(
            MapArray::try_new(
                Arc::clone(&entry),
                OffsetBuffer::new(vec![0, 1, 2, 3].into()),
                entries,
                None,
                false,
            )
            .unwrap(),
        );
        let field = Field::new("m", DataType::Map(entry, false), true);
        let (_, a) = decimal_arb_leaves_to_text(&field, &map.slice(2, 1)).unwrap();
        let ma = a.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(ma.entries().len(), 1);
        assert_eq!(texts(ma.values()), vec![Some("3".into())]);
    }

    /// A run-end-encoded column's nulls live in its values field, so the
    /// unwrapped field is nullable whenever the values are, whatever the
    /// outer field declared.
    #[test]
    fn run_end_encoded_unwrap_admits_the_values_nulls() {
        let ree = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1, 2]),
            &Int32Array::from(vec![Some(1), None]),
        )
        .unwrap();
        let field = Field::new("r", ree.data_type().clone(), false);
        let plain = plain_layout_field(&field).unwrap();
        assert_eq!(plain.data_type(), &DataType::Int32);
        assert!(plain.is_nullable());
        assert_eq!(plain.name(), "r");
        let (_, array) = to_plain_layout(&field, &(Arc::new(ree) as ArrayRef))
            .unwrap()
            .unwrap();
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn mixed_dictionary_and_run_end_fields_preserve_decimal_metadata() {
        use crate::types::decimal_arb::NativeIntKind;

        let leaf = DecimalArbType::with_native_int_kind(
            DecimalArbType::field("values", 78, 0, true).unwrap(),
            NativeIntKind::U256,
        )
        .unwrap();
        for wrappers in ["dr", "ddr", "rdr", "drd", "drdrd"] {
            let mut field = leaf.clone();
            for wrapper in wrappers.chars().rev() {
                field = match wrapper {
                    // Dictionary values have no Field of their own, so the
                    // extension metadata belongs to the dictionary field.
                    'd' => field_with_type(
                        &field,
                        DataType::Dictionary(
                            Box::new(DataType::Int32),
                            Box::new(field.data_type().clone()),
                        ),
                    ),
                    'r' => Field::new(
                        "encoded",
                        DataType::RunEndEncoded(
                            Arc::new(Field::new("run_ends", DataType::Int32, false)),
                            Arc::new(field),
                        ),
                        false,
                    ),
                    _ => unreachable!(),
                };
            }
            field = field.with_name("amount");
            assert!(field_contains_decimal_arb(&field), "{wrappers}");
            let plain = plain_layout_field(&field).unwrap();
            assert_eq!(plain.name(), "amount", "{wrappers}");
            assert_eq!(plain.data_type(), &DataType::LargeBinary, "{wrappers}");
            assert_eq!(plain.metadata(), leaf.metadata(), "{wrappers}");
            assert!(plain.is_nullable(), "{wrappers}");
        }
    }

    #[test]
    fn dictionary_over_run_end_decimal_keeps_text_and_avro_values() {
        use crate::formats::avro::{serialize, try_to_avro};
        use apache_avro::{Decimal, from_avro_datum, to_avro_datum, types::Value};
        use datafusion::arrow::array::{DictionaryArray, RecordBatch, make_array};
        use datafusion::arrow::datatypes::Schema;

        let mut builder = DecimalArbArrayBuilder::with_capacity(3, "values", 78, 0).unwrap();
        builder.append_str("1").unwrap();
        builder.append_null();
        builder.append_str("1000000000000000000").unwrap();
        let (values, _, _) = builder.finish().into_inner();
        let run =
            RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 3, 5]), &values).unwrap();
        let DataType::RunEndEncoded(run_ends, _) = run.data_type() else {
            unreachable!()
        };
        let run_type = DataType::RunEndEncoded(
            Arc::clone(run_ends),
            Arc::new(DecimalArbType::field("values", 78, 0, true).unwrap()),
        );
        let run = make_array(
            run.to_data()
                .into_builder()
                .data_type(run_type)
                .build()
                .unwrap(),
        );
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![4, 0, 2, 1]), run).unwrap(),
        );
        // The outer dictionary has no metadata and no physical nulls. The
        // decimal extension and logical null both belong to its REE values.
        let field = Field::new("amount", dictionary.data_type().clone(), false);
        let (text_field, text) = decimal_arb_leaves_to_text(&field, &dictionary).unwrap();
        assert_eq!(text_field.data_type(), &DataType::Utf8);
        assert!(text_field.is_nullable());
        assert_eq!(
            texts(&text),
            vec![
                Some("1000000000000000000".into()),
                Some("1".into()),
                None,
                Some("1".into())
            ],
        );

        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![dictionary]).unwrap();
        let schema = try_to_avro("EncodedAmounts", batch.schema().fields()).unwrap();
        let rows = serialize(&schema, &batch);
        for (row, expected) in
            rows.into_iter()
                .zip([Some(1_000_000_000_000_000_000_i64), Some(1), None, Some(1)])
        {
            let encoded = to_avro_datum(&schema, row).unwrap();
            let decoded = from_avro_datum(&schema, &mut encoded.as_slice(), None).unwrap();
            let expected = match expected {
                Some(value) => Value::Union(
                    1,
                    Box::new(Value::Decimal(Decimal::from(value.to_be_bytes().to_vec()))),
                ),
                None => Value::Union(0, Box::new(Value::Null)),
            };
            assert_eq!(decoded, Value::Record(vec![("amount".into(), expected)]));
        }
    }
}
