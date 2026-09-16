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
    StringArray, StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::common::{DataFusionError, Result};
use std::str::FromStr;
use std::sync::Arc;

/// Returns `true` if `field` is `decimal_arb`, or contains a `decimal_arb`
/// leaf nested anywhere inside a Struct / List / LargeList / FixedSizeList /
/// Map. Used to decide whether a batch needs the decimal_arb → Utf8 rewrite
/// before text serialization.
pub(crate) fn field_contains_decimal_arb(field: &Field) -> bool {
    DecimalArbType::is_decimal_arb_field(field) || type_contains_decimal_arb(field.data_type())
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
        _ => false,
    }
}

/// The plain (offset-based, un-encoded) layout `data_type` maps to: list views
/// become lists and dictionary / run-end encodings unwrap to their value type.
/// Everything the text bridge walks is expressed in these layouts; the
/// encoded variants are `cast` to them first.
fn plain_layout(data_type: &DataType) -> Option<DataType> {
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
    if let Some(plain) = plain_layout(field.data_type()) {
        let plain_array = cast(array.as_ref(), &plain)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        return decimal_arb_leaves_to_text(&field_with_type(field, plain), &plain_array);
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
            let (nf, nv) = decimal_arb_leaves_to_text(child, la.values())?;
            let nf = Arc::new(nf);
            let new_arr = ListArray::new(nf.clone(), la.offsets().clone(), nv, la.nulls().cloned());
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
            let (nf, nv) = decimal_arb_leaves_to_text(child, la.values())?;
            let nf = Arc::new(nf);
            let new_arr =
                LargeListArray::new(nf.clone(), la.offsets().clone(), nv, la.nulls().cloned());
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
            let (nef, nea) = decimal_arb_leaves_to_text(entry_field, &entries)?;
            let nef = Arc::new(nef);
            let new_entries = nea
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray (map entries)"))?
                .clone();
            let new_arr = MapArray::new(
                nef.clone(),
                ma.offsets().clone(),
                new_entries,
                ma.nulls().cloned(),
                *sorted,
            );
            Ok((
                field_with_type(field, DataType::Map(nef, *sorted)),
                Arc::new(new_arr) as ArrayRef,
            ))
        }
        // Unreachable: field_contains_decimal_arb was true but the type is not
        // a known container — return unchanged rather than erroring.
        _ => Ok((field.clone(), array.clone())),
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
    if DecimalArbType::is_decimal_arb_field(field) {
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
        _ => Ok(array.clone()),
    }
}
