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
use datafusion::common::{DataFusionError, Result};
use std::str::FromStr;
use std::sync::Arc;

/// Returns `true` if `field` is `decimal_arb`, or contains a `decimal_arb`
/// leaf nested anywhere inside a Struct / List / LargeList / FixedSizeList /
/// Map. Used to decide whether a batch needs the decimal_arb → Utf8 rewrite
/// before text serialization.
pub(crate) fn field_contains_decimal_arb(field: &Field) -> bool {
    if DecimalArbType::is_decimal_arb_field(field) {
        return true;
    }
    match field.data_type() {
        DataType::Struct(children) => children.iter().any(|f| field_contains_decimal_arb(f)),
        DataType::List(c)
        | DataType::LargeList(c)
        | DataType::FixedSizeList(c, _)
        | DataType::Map(c, _) => field_contains_decimal_arb(c),
        _ => false,
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
            let new_arr = FixedSizeListArray::new(nf.clone(), *n, nv, fa.nulls().cloned());
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
            Ok(Arc::new(FixedSizeListArray::new(
                child.clone(),
                *n,
                values,
                fa.nulls().cloned(),
            )) as ArrayRef)
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
