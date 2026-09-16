//! Bridge for the retired `streamling.u256` / `streamling.i256` wire types.
//!
//! Streamling replaced the fixed-width `FixedSizeBinary(32)` integer types
//! with `decimal_arb`, but the companion plugins still produce them: 32
//! big-endian bytes, two's complement for the signed variant. Everything that
//! understood those bytes was removed together with the types, so a legacy
//! column reaching a sink was written verbatim — ClickHouse read the big-endian
//! bytes as its little-endian `UInt256` (`1` became `2^248`) and the JSON
//! writer printed hex. This module recognises the legacy fields and converts
//! them to `decimal_arb(78, 0)` carrying the matching `native_int_kind` hint,
//! so the ordinary decimal paths take over from there.

use crate::error::Result;
use crate::streamling_err;
use crate::types::decimal_arb::{
    DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue, NativeIntKind,
};
use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, FixedSizeListArray,
    LargeBinaryArray, LargeListArray, ListArray, MapArray, RecordBatch, StructArray,
};
use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
use arrow_schema::{DataType, Field, Fields, Schema};
use num_bigint::{BigInt, Sign};
use std::sync::Arc;

/// `ARROW:extension:name` of the retired unsigned 256-bit integer type.
pub const LEGACY_U256_EXTENSION_NAME: &str = "streamling.u256";
/// `ARROW:extension:name` of the retired signed 256-bit integer type.
pub const LEGACY_I256_EXTENSION_NAME: &str = "streamling.i256";
/// Digits needed for any 256-bit integer (2^256 ≈ 1.16 × 10^77).
pub const LEGACY_WIDE_INT_PRECISION: u32 = 78;

/// The legacy wide-int kind `field` declares: `FixedSizeBinary(32)` tagged
/// `streamling.u256` / `streamling.i256`. `None` for every other field.
pub fn legacy_wide_int_kind(field: &Field) -> Option<NativeIntKind> {
    if !matches!(field.data_type(), DataType::FixedSizeBinary(32)) {
        return None;
    }
    match field.metadata().get(EXTENSION_TYPE_NAME_KEY)?.as_str() {
        LEGACY_U256_EXTENSION_NAME => Some(NativeIntKind::U256),
        LEGACY_I256_EXTENSION_NAME => Some(NativeIntKind::I256),
        _ => None,
    }
}

/// Does `field`, or any field nested below it, carry a legacy wide-int type?
pub fn field_contains_legacy_wide_int(field: &Field) -> bool {
    if legacy_wide_int_kind(field).is_some() {
        return true;
    }
    match field.data_type() {
        DataType::Struct(children) => children.iter().any(|c| field_contains_legacy_wide_int(c)),
        DataType::List(c)
        | DataType::LargeList(c)
        | DataType::FixedSizeList(c, _)
        | DataType::Map(c, _) => field_contains_legacy_wide_int(c),
        _ => false,
    }
}

/// The `decimal_arb(78, 0)` field a legacy wide-int field upgrades to, hinted
/// with its origin kind so ClickHouse keeps using `UInt256` / `Int256`.
pub fn legacy_wide_int_as_decimal_arb_field(field: &Field, kind: NativeIntKind) -> Result<Field> {
    let upgraded = DecimalArbType::field(
        field.name(),
        LEGACY_WIDE_INT_PRECISION,
        0,
        field.is_nullable(),
    )?;
    // Keep whatever unrelated metadata the producer attached.
    let mut metadata = field.metadata().clone();
    metadata.retain(|k, _| !k.starts_with("ARROW:extension:"));
    metadata.extend(upgraded.metadata().clone());
    DecimalArbType::with_native_int_kind(upgraded.with_metadata(metadata), kind)
}

/// Re-encode a legacy big-endian wide-int column as canonical decimal_arb
/// bytes at scale 0.
pub fn legacy_wide_int_to_decimal_arb(
    array: &dyn Array,
    field: &Field,
    kind: NativeIntKind,
) -> Result<ArrayRef> {
    let fsb = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            streamling_err!(
                "legacy {} column '{}' must be FixedSizeBinary(32), got {:?}",
                kind.as_str(),
                field.name(),
                array.data_type(),
            )
        })?;
    let mut builder = DecimalArbArrayBuilder::with_capacity(
        fsb.len(),
        field.name(),
        LEGACY_WIDE_INT_PRECISION,
        0,
    )?;
    for i in 0..fsb.len() {
        if fsb.is_null(i) {
            builder.append_null();
            continue;
        }
        let be = fsb.value(i);
        let int = match kind {
            NativeIntKind::U256 => BigInt::from_bytes_be(Sign::Plus, be),
            NativeIntKind::I256 => BigInt::from_signed_bytes_be(be),
        };
        builder.append_value(&DecimalArbValue::from_bigint_and_scale(int, 0))?;
    }
    let (raw, _, _) = builder.finish().into_inner();
    Ok(Arc::new(raw))
}

/// Schema-only mirror of [`upgrade_legacy_wide_ints`]: the field `field`
/// becomes once every legacy leaf below it is upgraded. `None` when it holds
/// no legacy leaf.
pub fn upgrade_legacy_wide_int_field(field: &Field) -> Result<Option<Field>> {
    if let Some(kind) = legacy_wide_int_kind(field) {
        return Ok(Some(legacy_wide_int_as_decimal_arb_field(field, kind)?));
    }
    if !field_contains_legacy_wide_int(field) {
        return Ok(None);
    }
    let retype = |dt: DataType| {
        Field::new(field.name(), dt, field.is_nullable()).with_metadata(field.metadata().clone())
    };
    let upgrade_child = |c: &Arc<Field>| -> Result<Arc<Field>> {
        Ok(upgrade_legacy_wide_int_field(c)?
            .map(Arc::new)
            .unwrap_or_else(|| Arc::clone(c)))
    };
    Ok(Some(match field.data_type() {
        DataType::Struct(children) => {
            let children = children
                .iter()
                .map(upgrade_child)
                .collect::<Result<Vec<_>>>()?;
            retype(DataType::Struct(Fields::from(children)))
        }
        DataType::List(c) => retype(DataType::List(upgrade_child(c)?)),
        DataType::LargeList(c) => retype(DataType::LargeList(upgrade_child(c)?)),
        DataType::FixedSizeList(c, n) => retype(DataType::FixedSizeList(upgrade_child(c)?, *n)),
        DataType::Map(c, sorted) => retype(DataType::Map(upgrade_child(c)?, *sorted)),
        _ => return Ok(None),
    }))
}

/// Upgrade every legacy wide-int leaf below `(field, array)` to decimal_arb.
/// `None` when the column holds no legacy leaf.
pub fn upgrade_legacy_wide_ints(
    field: &Field,
    array: &ArrayRef,
) -> Result<Option<(Field, ArrayRef)>> {
    if let Some(kind) = legacy_wide_int_kind(field) {
        return Ok(Some((
            legacy_wide_int_as_decimal_arb_field(field, kind)?,
            legacy_wide_int_to_decimal_arb(array.as_ref(), field, kind)?,
        )));
    }
    if !field_contains_legacy_wide_int(field) {
        return Ok(None);
    }
    let downcast_err = |what: &str| {
        streamling_err!(
            "expected {} for field '{}', got {:?}",
            what,
            field.name(),
            array.data_type(),
        )
    };
    let retype = |dt: DataType| {
        Field::new(field.name(), dt, field.is_nullable()).with_metadata(field.metadata().clone())
    };
    let upgrade_child = |c: &Arc<Field>, values: &ArrayRef| -> Result<(Arc<Field>, ArrayRef)> {
        Ok(match upgrade_legacy_wide_ints(c, values)? {
            Some((f, a)) => (Arc::new(f), a),
            None => (Arc::clone(c), Arc::clone(values)),
        })
    };
    Ok(Some(match field.data_type() {
        DataType::Struct(children) => {
            let sa = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray"))?;
            let mut fields = Vec::with_capacity(children.len());
            let mut columns = Vec::with_capacity(children.len());
            for (child, column) in children.iter().zip(sa.columns()) {
                let (f, a) = upgrade_child(child, column)?;
                fields.push(f);
                columns.push(a);
            }
            let fields = Fields::from(fields);
            let sa = StructArray::new(fields.clone(), columns, sa.nulls().cloned());
            (retype(DataType::Struct(fields)), Arc::new(sa) as ArrayRef)
        }
        DataType::List(child) => {
            let la = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| downcast_err("ListArray"))?;
            let (f, values) = upgrade_child(child, la.values())?;
            let la = ListArray::new(
                Arc::clone(&f),
                la.offsets().clone(),
                values,
                la.nulls().cloned(),
            );
            (retype(DataType::List(f)), Arc::new(la) as ArrayRef)
        }
        DataType::LargeList(child) => {
            let la = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| downcast_err("LargeListArray"))?;
            let (f, values) = upgrade_child(child, la.values())?;
            let la = LargeListArray::new(
                Arc::clone(&f),
                la.offsets().clone(),
                values,
                la.nulls().cloned(),
            );
            (retype(DataType::LargeList(f)), Arc::new(la) as ArrayRef)
        }
        DataType::FixedSizeList(child, n) => {
            let fa = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| downcast_err("FixedSizeListArray"))?;
            let (f, values) = upgrade_child(child, fa.values())?;
            let fa = FixedSizeListArray::try_new_with_length(
                Arc::clone(&f),
                *n,
                values,
                fa.nulls().cloned(),
                fa.len(),
            )?;
            (
                retype(DataType::FixedSizeList(f, *n)),
                Arc::new(fa) as ArrayRef,
            )
        }
        DataType::Map(entry_field, sorted) => {
            let ma = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| downcast_err("MapArray"))?;
            let entries: ArrayRef = Arc::new(ma.entries().clone());
            let (f, entries) = upgrade_child(entry_field, &entries)?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray (map entries)"))?
                .clone();
            let ma = MapArray::new(
                Arc::clone(&f),
                ma.offsets().clone(),
                entries,
                ma.nulls().cloned(),
                *sorted,
            );
            (retype(DataType::Map(f, *sorted)), Arc::new(ma) as ArrayRef)
        }
        _ => return Ok(None),
    }))
}

/// Upgrade every legacy wide-int column of `batch`. `None` when the batch
/// carries none, so callers can keep the original untouched.
/// Inverse of [`legacy_wide_int_to_decimal_arb`]: re-encode integer-valued
/// decimal_arb bytes (as `field` declares them) into the legacy 32-byte
/// big-endian wire form `kind` names — two's complement for `i256`. A value
/// with a fractional part or outside the 256-bit range is an error, never
/// truncated bytes.
pub fn decimal_arb_to_legacy_wide_int(
    array: &dyn Array,
    field: &Field,
    kind: NativeIntKind,
) -> Result<ArrayRef> {
    let bin = array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            streamling_err!(
                "decimal_arb column '{}' must be LargeBinary to re-encode as legacy {}, got {:?}",
                field.name(),
                kind.as_str(),
                array.data_type(),
            )
        })?;
    let scale = DecimalArbType::precision_scale_from_field(field)
        .map(|(_, s)| s)
        .unwrap_or(0);
    let mut builder = FixedSizeBinaryBuilder::with_capacity(bin.len(), 32);
    for i in 0..bin.len() {
        if bin.is_null(i) {
            builder.append_null();
            continue;
        }
        let value = DecimalArbValue::from_canonical_bytes_at_scale(bin.value(i), scale)?;
        if value.fractional_digit_count() != 0 {
            return Err(streamling_err!(
                "column '{}' row {}: value {} has a fractional part and cannot be written as legacy {}",
                field.name(),
                i,
                value.to_canonical_string(),
                kind.as_str(),
            ));
        }
        let (int, _) = value
            .as_bigdecimal()
            .with_scale(0)
            .into_bigint_and_exponent();
        let out_of_range = || {
            streamling_err!(
                "column '{}' row {}: value {} is out of range for legacy {} (256-bit)",
                field.name(),
                i,
                value.to_canonical_string(),
                kind.as_str(),
            )
        };
        let mut be = match kind {
            NativeIntKind::U256 => {
                let (sign, magnitude) = int.to_bytes_be();
                if sign == Sign::Minus || magnitude.len() > 32 {
                    return Err(out_of_range());
                }
                let mut be = vec![0_u8; 32 - magnitude.len()];
                be.extend_from_slice(&magnitude);
                be
            }
            NativeIntKind::I256 => {
                let signed = int.to_signed_bytes_be();
                if signed.len() > 32 {
                    return Err(out_of_range());
                }
                let fill = if int.sign() == Sign::Minus {
                    0xFF
                } else {
                    0x00
                };
                let mut be = vec![fill; 32 - signed.len()];
                be.extend_from_slice(&signed);
                be
            }
        };
        debug_assert_eq!(be.len(), 32);
        be.truncate(32);
        builder.append_value(&be)?;
    }
    Ok(Arc::new(builder.finish()))
}

/// Inverse of [`upgrade_legacy_wide_ints`]: `array` has the shape
/// [`upgrade_legacy_wide_int_field`] gives `target` (decimal_arb where
/// `target` has a legacy leaf); rebuild it in `target`'s own wire shape.
/// `None` when `target` holds no legacy leaf.
pub fn downgrade_legacy_wide_ints(target: &Field, array: &ArrayRef) -> Result<Option<ArrayRef>> {
    if let Some(kind) = legacy_wide_int_kind(target) {
        let upgraded = legacy_wide_int_as_decimal_arb_field(target, kind)?;
        return Ok(Some(decimal_arb_to_legacy_wide_int(
            array.as_ref(),
            &upgraded,
            kind,
        )?));
    }
    if !field_contains_legacy_wide_int(target) {
        return Ok(None);
    }
    let downcast_err = |what: &str| {
        streamling_err!(
            "expected {} for field '{}', got {:?}",
            what,
            target.name(),
            array.data_type(),
        )
    };
    let downgrade_child = |c: &Arc<Field>, values: &ArrayRef| -> Result<ArrayRef> {
        Ok(downgrade_legacy_wide_ints(c, values)?.unwrap_or_else(|| Arc::clone(values)))
    };
    Ok(Some(match target.data_type() {
        DataType::Struct(children) => {
            let sa = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray"))?;
            if sa.num_columns() != children.len() {
                return Err(downcast_err("StructArray with matching children"));
            }
            let columns = children
                .iter()
                .zip(sa.columns())
                .map(|(child, column)| downgrade_child(child, column))
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::new(
                children.clone(),
                columns,
                sa.nulls().cloned(),
            )) as ArrayRef
        }
        DataType::List(child) => {
            let la = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| downcast_err("ListArray"))?;
            let values = downgrade_child(child, la.values())?;
            Arc::new(ListArray::new(
                Arc::clone(child),
                la.offsets().clone(),
                values,
                la.nulls().cloned(),
            )) as ArrayRef
        }
        DataType::LargeList(child) => {
            let la = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| downcast_err("LargeListArray"))?;
            let values = downgrade_child(child, la.values())?;
            Arc::new(LargeListArray::new(
                Arc::clone(child),
                la.offsets().clone(),
                values,
                la.nulls().cloned(),
            )) as ArrayRef
        }
        DataType::FixedSizeList(child, n) => {
            let fa = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| downcast_err("FixedSizeListArray"))?;
            let values = downgrade_child(child, fa.values())?;
            Arc::new(FixedSizeListArray::try_new_with_length(
                Arc::clone(child),
                *n,
                values,
                fa.nulls().cloned(),
                fa.len(),
            )?) as ArrayRef
        }
        DataType::Map(entry_field, sorted) => {
            let ma = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| downcast_err("MapArray"))?;
            let entries: ArrayRef = Arc::new(ma.entries().clone());
            let entries = downgrade_child(entry_field, &entries)?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| downcast_err("StructArray (map entries)"))?
                .clone();
            Arc::new(MapArray::new(
                Arc::clone(entry_field),
                ma.offsets().clone(),
                entries,
                ma.nulls().cloned(),
                *sorted,
            )) as ArrayRef
        }
        _ => return Ok(None),
    }))
}

pub fn upgrade_legacy_wide_int_batch(batch: &RecordBatch) -> Result<Option<RecordBatch>> {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|f| field_contains_legacy_wide_int(f))
    {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        match upgrade_legacy_wide_ints(field, column)? {
            Some((f, a)) => {
                fields.push(Arc::new(f));
                columns.push(a);
            }
            None => {
                fields.push(Arc::clone(field));
                columns.push(Arc::clone(column));
            }
        }
    }
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    Ok(Some(RecordBatch::try_new(schema, columns)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn legacy_field(name: &str, ext: &str) -> Field {
        Field::new(name, DataType::FixedSizeBinary(32), true).with_metadata(HashMap::from([(
            EXTENSION_TYPE_NAME_KEY.to_string(),
            ext.to_string(),
        )]))
    }

    fn be32(v: i128) -> [u8; 32] {
        let mut b = if v < 0 { [0xff_u8; 32] } else { [0_u8; 32] };
        b[16..].copy_from_slice(&v.to_be_bytes());
        b
    }

    fn decoded(array: &ArrayRef) -> Vec<Option<String>> {
        let raw = array
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();
        (0..raw.len())
            .map(|i| {
                (!raw.is_null(i)).then(|| {
                    DecimalArbValue::from_canonical_bytes_at_scale(raw.value(i), 0)
                        .unwrap()
                        .to_canonical_string()
                })
            })
            .collect()
    }

    #[test]
    fn recognises_only_tagged_fsb32() {
        assert_eq!(
            legacy_wide_int_kind(&legacy_field("v", LEGACY_U256_EXTENSION_NAME)),
            Some(NativeIntKind::U256)
        );
        assert_eq!(
            legacy_wide_int_kind(&legacy_field("v", LEGACY_I256_EXTENSION_NAME)),
            Some(NativeIntKind::I256)
        );
        assert_eq!(
            legacy_wide_int_kind(&Field::new("v", DataType::FixedSizeBinary(32), true)),
            None
        );
        let decimal = DecimalArbType::field("v", 78, 0, true).unwrap();
        assert_eq!(legacy_wide_int_kind(&decimal), None);
    }

    #[test]
    fn unsigned_big_endian_values_are_preserved() {
        let field = legacy_field("v", LEGACY_U256_EXTENSION_NAME);
        let values = [1_i128, 16, 256, 1_000_000_000_000_000_000];
        let bytes: Vec<_> = values.iter().map(|v| be32(*v)).collect();
        let array: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                bytes.iter().map(|b| Some(b.as_slice())).chain([None]),
                32,
            )
            .unwrap(),
        );
        let (upgraded, converted) = upgrade_legacy_wide_ints(&field, &array).unwrap().unwrap();
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&upgraded),
            Some((LEGACY_WIDE_INT_PRECISION, 0))
        );
        assert_eq!(
            DecimalArbType::native_int_kind_from_field(&upgraded),
            Some(NativeIntKind::U256)
        );
        let mut expected: Vec<Option<String>> =
            values.iter().map(|v| Some(v.to_string())).collect();
        expected.push(None);
        assert_eq!(decoded(&converted), expected);
    }

    #[test]
    fn signed_twos_complement_values_are_preserved() {
        let field = legacy_field("v", LEGACY_I256_EXTENSION_NAME);
        let values = [-1_i128, 1, -256, i128::MIN, i128::MAX];
        let bytes: Vec<_> = values.iter().map(|v| be32(*v)).collect();
        let array: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter(bytes.iter().map(|b| b.as_slice())).unwrap(),
        );
        let (upgraded, converted) = upgrade_legacy_wide_ints(&field, &array).unwrap().unwrap();
        assert_eq!(
            DecimalArbType::native_int_kind_from_field(&upgraded),
            Some(NativeIntKind::I256)
        );
        assert_eq!(
            decoded(&converted),
            values
                .iter()
                .map(|v| Some(v.to_string()))
                .collect::<Vec<_>>()
        );
        // The unsigned type reads the same bytes as a large positive number.
        let all_ones: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter([[0xff_u8; 32].as_slice()].into_iter()).unwrap(),
        );
        let (_, as_unsigned) =
            upgrade_legacy_wide_ints(&legacy_field("v", LEGACY_U256_EXTENSION_NAME), &all_ones)
                .unwrap()
                .unwrap();
        assert_eq!(
            decoded(&as_unsigned),
            vec![Some(
                "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                    .to_string()
            )]
        );
    }

    #[test]
    fn nested_legacy_leaves_are_upgraded_and_plain_batches_untouched() {
        let leaf = Arc::new(legacy_field("v", LEGACY_U256_EXTENSION_NAME));
        let values: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                [be32(16).as_slice(), be32(18).as_slice()].into_iter(),
            )
            .unwrap(),
        );
        let list = ListArray::new(
            leaf,
            arrow::buffer::OffsetBuffer::new(vec![0_i32, 2].into()),
            values,
            None,
        );
        let field = Field::new("nested", list.data_type().clone(), true);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                field,
                Field::new("id", DataType::Int32, false),
            ])),
            vec![
                Arc::new(list),
                Arc::new(arrow::array::Int32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let upgraded = upgrade_legacy_wide_int_batch(&batch).unwrap().unwrap();
        let upgraded_schema = upgraded.schema();
        let DataType::List(child) = upgraded_schema.field(0).data_type() else {
            panic!("list expected");
        };
        assert!(DecimalArbType::is_decimal_arb_field(child));
        let list = upgraded
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .clone();
        assert_eq!(
            decoded(&list.values().clone()),
            vec![Some("16".to_string()), Some("18".to_string())]
        );
        assert_eq!(upgraded.schema().field(1), batch.schema().field(1));

        let plain = batch.project(&[1]).unwrap();
        assert!(upgrade_legacy_wide_int_batch(&plain).unwrap().is_none());
    }

    #[test]
    fn downgrade_is_the_exact_inverse_of_upgrade() {
        // Unsigned: 0, 1, 2^248 (the bytes ClickHouse mis-read as `1`), max.
        let u_max = [0xff_u8; 32];
        let mut two_248 = [0_u8; 32];
        two_248[0] = 1;
        let u_values = [be32(0), be32(1), two_248, u_max];
        let field = legacy_field("u", LEGACY_U256_EXTENSION_NAME);
        let array: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [
                    Some(u_values[0]),
                    Some(u_values[1]),
                    None,
                    Some(u_values[2]),
                    Some(u_values[3]),
                ]
                .into_iter(),
                32,
            )
            .unwrap(),
        );
        let (upgraded_field, upgraded) = upgrade_legacy_wide_ints(&field, &array).unwrap().unwrap();
        assert!(DecimalArbType::is_decimal_arb_field(&upgraded_field));
        let back = downgrade_legacy_wide_ints(&field, &upgraded)
            .unwrap()
            .unwrap();
        assert_eq!(back.as_ref(), array.as_ref());

        // Signed: -1, i256 min, i256 max, -(2^128).
        let mut i_min = [0_u8; 32];
        i_min[0] = 0x80;
        let mut i_max = [0xff_u8; 32];
        i_max[0] = 0x7f;
        let mut neg_2_128 = [0xff_u8; 32];
        neg_2_128[15] = 0xff;
        neg_2_128[16..].fill(0);
        let field = legacy_field("i", LEGACY_I256_EXTENSION_NAME);
        let array: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                [
                    be32(-1).as_slice(),
                    i_min.as_slice(),
                    i_max.as_slice(),
                    neg_2_128.as_slice(),
                    be32(7).as_slice(),
                ]
                .into_iter(),
            )
            .unwrap(),
        );
        let (_, upgraded) = upgrade_legacy_wide_ints(&field, &array).unwrap().unwrap();
        assert_eq!(
            decoded(&upgraded)[3],
            Some("-340282366920938463463374607431768211456".to_string())
        );
        let back = downgrade_legacy_wide_ints(&field, &upgraded)
            .unwrap()
            .unwrap();
        assert_eq!(back.as_ref(), array.as_ref());
    }

    #[test]
    fn downgrade_rejects_values_the_wire_type_cannot_hold() {
        let encode = |text: &str| -> ArrayRef {
            let mut b = DecimalArbArrayBuilder::with_capacity(1, "v", 78, 0).unwrap();
            b.append_str(text).unwrap();
            Arc::new(b.finish().into_inner().0)
        };
        let u = legacy_field("v", LEGACY_U256_EXTENSION_NAME);
        let i = legacy_field("v", LEGACY_I256_EXTENSION_NAME);
        let two_256 =
            "115792089237316195423570985008687907853269984665640564039457584007913129639936";
        let two_255 =
            "57896044618658097711785492504343953926634992332820282019728792003956564819968";
        for (field, text) in [
            (&u, "-1"),
            (&u, two_256),
            (&i, two_255),
            (
                &i,
                "-57896044618658097711785492504343953926634992332820282019728792003956564819969",
            ),
        ] {
            let err = downgrade_legacy_wide_ints(field, &encode(text))
                .expect_err("out of range must not be truncated")
                .to_string();
            assert!(err.contains("out of range"), "{err}");
        }
        // The boundaries themselves round-trip.
        for (field, text) in [
            (&u, two_255),
            (
                &i,
                "-57896044618658097711785492504343953926634992332820282019728792003956564819968",
            ),
        ] {
            let bytes = downgrade_legacy_wide_ints(field, &encode(text))
                .unwrap()
                .unwrap();
            let (_, again) = upgrade_legacy_wide_ints(field, &bytes).unwrap().unwrap();
            assert_eq!(decoded(&again), vec![Some(text.to_string())]);
        }
    }
}
