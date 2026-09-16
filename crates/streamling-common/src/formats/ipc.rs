use crate::formats::decimal_arb_text::{
    decimal_arb_leaves_as_text_field, decimal_arb_leaves_from_text, decimal_arb_leaves_to_text,
    field_contains_decimal_arb, overlay_decimal_arb_metadata,
};
use crate::formats::{FromArrowConverter, ToArrowConverter};
use crate::streamling_err;
use crate::types::decimal_arb_legacy::{
    downgrade_legacy_wide_ints, field_contains_legacy_wide_int, upgrade_legacy_wide_int_batch,
    upgrade_legacy_wide_int_field,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::ipc::{reader::FileReader, writer::FileWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use std::io::Cursor;
use std::sync::Arc;

pub struct FromArrowToIpcConverter {}

impl FromArrowToIpcConverter {
    pub fn new() -> Self {
        Self {}
    }

    fn to_ipc(&self, batch: &RecordBatch) -> Result<Vec<u8>> {
        // This converter feeds the script-transform boundary, where the other
        // side is JavaScript. Wide numbers have always been exposed there as
        // decimal strings (`row.amount === '0'`), and scripts depend on it.
        // Retiring U256/I256 dropped that conversion along with them, so
        // decimal_arb columns started arriving as opaque byte arrays. Convert
        // every decimal_arb leaf — nested ones too — back to canonical decimal
        // text; `convert_batch_to_original_schema` parses it on the way back.
        //
        // A companion plugin may still hand over the retired FixedSizeBinary(32)
        // `streamling.u256` / `streamling.i256` columns. Scripts always saw those
        // as decimal strings too, so upgrade them to decimal_arb first and let
        // the same text bridge carry them.
        let upgraded = upgrade_legacy_wide_int_batch(batch).map_err(DataFusionError::from)?;
        let batch = upgraded.as_ref().unwrap_or(batch);
        let needs_transform = batch
            .schema()
            .fields()
            .iter()
            .any(|f| field_contains_decimal_arb(f));

        let transformed_batch = if needs_transform {
            let mut new_fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
            let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
            for (idx, field) in batch.schema().fields().iter().enumerate() {
                let (nf, na) = decimal_arb_leaves_to_text(field, batch.column(idx))?;
                new_fields.push(nf);
                new_columns.push(na);
            }
            RecordBatch::try_new(Arc::new(Schema::new(new_fields)), new_columns)?
        } else {
            batch.clone()
        };

        // Serialize to Arrow IPC file format
        let mut buf = Vec::new();
        let mut writer = FileWriter::try_new(&mut buf, transformed_batch.schema().as_ref())
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        writer
            .write(&transformed_batch)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        writer
            .finish()
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        Ok(buf)
    }
}

impl Default for FromArrowToIpcConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl FromArrowConverter<Vec<u8>> for FromArrowToIpcConverter {
    fn convert_from_batch(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        // For Arrow IPC, we serialize the entire batch as a single IPC stream
        Ok(vec![self.to_ipc(batch)?])
    }
}

pub struct FromIpcToArrowConverter {
    schema: SchemaRef,
    ipc_buffers: Vec<Vec<u8>>,
}

impl FromIpcToArrowConverter {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            ipc_buffers: Vec::new(),
        }
    }

    fn convert_batch_from_ipc(&self, ipc_bytes: &[u8]) -> Result<RecordBatch> {
        // Handle empty or minimal IPC bytes - this can happen when TypeScript transforms
        // return an empty array [] or null for all rows, which gets serialized as an
        // IPC file with no batches or an invalid/minimal footer
        if ipc_bytes.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let cursor = Cursor::new(ipc_bytes);
        let mut reader = match FileReader::try_new(cursor, None) {
            Ok(reader) => reader,
            Err(e) => {
                // If the IPC file is invalid (e.g., empty table from flechette creates
                // an IPC file with invalid footer), return an empty batch with target schema.
                // This handles the case where JS runtime creates tableFromArrays({ _dummy: [] })
                // which may serialize to an IPC file that Arrow cannot parse.
                let error_msg = e.to_string();
                if error_msg.contains("Unable to get record batches")
                    || error_msg.contains("Footer")
                    || error_msg.contains("empty")
                {
                    return Ok(RecordBatch::new_empty(self.schema.clone()));
                }
                return Err(DataFusionError::ArrowError(Box::new(e), None));
            }
        };

        // Read the first (and typically only) batch from the IPC file
        // Handle empty IPC files gracefully
        let batch = match reader.next() {
            Some(result) => result.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            None => {
                // No batches in IPC file - return empty batch with target schema
                return Ok(RecordBatch::new_empty(self.schema.clone()));
            }
        };

        // If the batch has 0 rows, return an empty batch with the target schema
        // This ensures schema consistency even for empty results
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Convert batch to match the target schema (handles type conversions and U256/I256)
        self.convert_batch_to_original_schema(batch)
    }

    /// Rebuild the column `target` describes — a decimal_arb leaf, or a
    /// container with decimal_arb leaves at any depth — from whatever the IPC
    /// payload actually carried.
    ///
    /// Canonical decimal text is the value-preserving intermediate. Leaves that
    /// arrive as decimal_arb bytes (their metadata survived; or bare
    /// `LargeBinary` where `target` says decimal_arb, read at the target's
    /// scale) are rendered to text; the column is shaped to the text form of
    /// `target` — struct children paired by name, List ↔ LargeList, Utf8View /
    /// dictionary strings and all-null columns converted with Arrow's `cast`;
    /// and the leaves are parsed back at the target `(precision, scale)`,
    /// which validates every value.
    ///
    /// That covers the shapes a plain `cast` got wrong — `"100"` handed over as
    /// UTF-8 bytes read as 12336, 12.34 relabelled under a scale-4 target read
    /// as 0.1234, and every nested leaf, which the old top-level-only
    /// conversion never reached — and, unlike a same-`DataType` passthrough,
    /// it rejects a 1000 stored under `decimal_arb(80, 0)` when the target is
    /// `decimal_arb(2, 0)`.
    fn restore_decimal_arb(
        target: &Field,
        source_field: &Field,
        source: &ArrayRef,
    ) -> Result<ArrayRef> {
        // Identical declaration, metadata included: nothing to convert.
        if source_field.data_type() == target.data_type()
            && source_field.metadata() == target.metadata()
        {
            return Ok(source.clone());
        }
        let overlaid = overlay_decimal_arb_metadata(source_field, target);
        let (_, text) = decimal_arb_leaves_to_text(&overlaid, source)?;
        let text_target = decimal_arb_leaves_as_text_field(target);
        let shaped = Self::shape_like(&text_target, &text)?;
        decimal_arb_leaves_from_text(target, &shaped)
    }

    /// Bring `array` to `target.data_type()`: struct children are matched by
    /// name (a child the payload lacks is all-null), list layouts are
    /// converted, and every remaining difference goes through Arrow's `cast`.
    fn shape_like(target: &Field, array: &ArrayRef) -> Result<ArrayRef> {
        use datafusion::arrow::array::{
            FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray, new_null_array,
        };
        use datafusion::arrow::compute::cast;

        if array.data_type() == target.data_type() {
            return Ok(array.clone());
        }
        let arrow_err = |e| DataFusionError::ArrowError(Box::new(e), None);
        let downcast_err = |what: &str| {
            DataFusionError::from(streamling_err!(
                "expected {} for field '{}', got {:?}",
                what,
                target.name(),
                array.data_type(),
            ))
        };
        match (target.data_type(), array.data_type()) {
            (DataType::Struct(tc), DataType::Struct(sc)) => {
                let sa = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| downcast_err("StructArray"))?;
                let columns = tc
                    .iter()
                    .map(|t| match sc.iter().position(|s| s.name() == t.name()) {
                        Some(j) => Self::shape_like(t, sa.column(j)),
                        None => Ok(new_null_array(t.data_type(), sa.len())),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(StructArray::new(
                    tc.clone(),
                    columns,
                    sa.nulls().cloned(),
                )))
            }
            (DataType::List(t), DataType::List(_)) => {
                let la = array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| downcast_err("ListArray"))?;
                let values = Self::shape_like(t, la.values())?;
                Ok(Arc::new(ListArray::new(
                    t.clone(),
                    la.offsets().clone(),
                    values,
                    la.nulls().cloned(),
                )))
            }
            (DataType::LargeList(t), DataType::LargeList(_)) => {
                let la = array
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(|| downcast_err("LargeListArray"))?;
                let values = Self::shape_like(t, la.values())?;
                Ok(Arc::new(LargeListArray::new(
                    t.clone(),
                    la.offsets().clone(),
                    values,
                    la.nulls().cloned(),
                )))
            }
            (DataType::FixedSizeList(t, n), DataType::FixedSizeList(_, m)) if n == m => {
                let fa = array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| downcast_err("FixedSizeListArray"))?;
                let values = Self::shape_like(t, fa.values())?;
                Ok(Arc::new(
                    FixedSizeListArray::try_new_with_length(
                        t.clone(),
                        *n,
                        values,
                        fa.nulls().cloned(),
                        fa.len(),
                    )
                    .map_err(arrow_err)?,
                ))
            }
            (DataType::Map(t, sorted), DataType::Map(_, _)) => {
                let ma = array
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| downcast_err("MapArray"))?;
                let entries: ArrayRef = Arc::new(ma.entries().clone());
                let entries = Self::shape_like(t, &entries)?;
                let entries = entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| downcast_err("StructArray (map entries)"))?
                    .clone();
                Ok(Arc::new(MapArray::new(
                    t.clone(),
                    ma.offsets().clone(),
                    entries,
                    ma.nulls().cloned(),
                    *sorted,
                )))
            }
            // Another list layout: convert the layout with the payload's own
            // element type, then shape the elements.
            (
                DataType::List(_),
                DataType::LargeList(s)
                | DataType::FixedSizeList(s, _)
                | DataType::ListView(s)
                | DataType::LargeListView(s),
            ) => {
                let interim =
                    cast(array.as_ref(), &DataType::List(s.clone())).map_err(arrow_err)?;
                Self::shape_like(target, &interim)
            }
            (
                DataType::LargeList(_),
                DataType::List(s)
                | DataType::FixedSizeList(s, _)
                | DataType::ListView(s)
                | DataType::LargeListView(s),
            ) => {
                let interim =
                    cast(array.as_ref(), &DataType::LargeList(s.clone())).map_err(arrow_err)?;
                Self::shape_like(target, &interim)
            }
            (DataType::FixedSizeList(_, n), DataType::List(s) | DataType::LargeList(s)) => {
                let interim = cast(array.as_ref(), &DataType::FixedSizeList(s.clone(), *n))
                    .map_err(arrow_err)?;
                Self::shape_like(target, &interim)
            }
            // Encoded containers: unwrap to the value layout first.
            (_, DataType::Dictionary(_, values)) => {
                let interim = cast(array.as_ref(), values).map_err(arrow_err)?;
                Self::shape_like(target, &interim)
            }
            (_, DataType::RunEndEncoded(_, values)) => {
                let interim = cast(array.as_ref(), values.data_type()).map_err(arrow_err)?;
                Self::shape_like(target, &interim)
            }
            _ => cast(array.as_ref(), target.data_type()).map_err(arrow_err),
        }
    }

    fn convert_batch_to_original_schema(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // A script (or a Rust plugin) may return the retired FixedSizeBinary(32)
        // `streamling.u256` / `streamling.i256` columns verbatim; as decimal_arb
        // they take the value-checked restoration path below instead of being
        // reinterpreted byte-for-byte.
        let batch = match upgrade_legacy_wide_int_batch(&batch).map_err(DataFusionError::from)? {
            Some(upgraded) => upgraded,
            None => batch,
        };
        let needs_conversion = batch.schema() != self.schema;

        if !needs_conversion {
            return Ok(batch);
        }

        let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        let mut new_fields: Vec<Field> = Vec::with_capacity(self.schema.fields().len());

        let batch_schema = batch.schema();
        for (idx, target_field) in self.schema.fields().iter().enumerate() {
            // Always match columns by name first (flechette may create columns in different order)
            // Fall back to index if name doesn't match
            let (source_col_opt, source_field_opt): (Option<&ArrayRef>, Option<&Field>) =
                match batch_schema.field_with_name(target_field.name()) {
                    Ok(found_field) => {
                        let col_idx = batch_schema.index_of(found_field.name()).unwrap_or(idx);
                        (Some(batch.column(col_idx)), Some(found_field))
                    }
                    Err(_) => {
                        // Field not found by name, try by index
                        if idx < batch.num_columns() {
                            (
                                Some(batch.column(idx)),
                                batch_schema.fields().get(idx).map(|f| f.as_ref()),
                            )
                        } else {
                            (None, None)
                        }
                    }
                };

            if let Some(source_col) = source_col_opt {
                // Convert column type if needed to match target schema
                let source_field = source_field_opt.cloned().unwrap_or_else(|| {
                    Field::new(target_field.name(), source_col.data_type().clone(), true)
                });

                if field_contains_decimal_arb(target_field) {
                    // decimal_arb is LargeBinary underneath, so neither the
                    // type-equality check nor Arrow's `cast` can tell a correct
                    // conversion from a reinterpretation of raw bytes.
                    new_columns.push(Self::restore_decimal_arb(
                        target_field,
                        &source_field,
                        source_col,
                    )?);
                } else if field_contains_legacy_wide_int(target_field) {
                    // The pipeline still declares the retired wire type (a script
                    // with no output schema inherits its plugin input's). Restore
                    // through the decimal_arb form it upgrades to — text parsed and
                    // range-checked — then re-encode the legacy bytes.
                    let upgraded_target = upgrade_legacy_wide_int_field(target_field)
                        .map_err(DataFusionError::from)?
                        .ok_or_else(|| {
                            DataFusionError::from(streamling_err!(
                                "field '{}' has a legacy wide-int leaf but no upgraded form",
                                target_field.name(),
                            ))
                        })?;
                    let restored =
                        Self::restore_decimal_arb(&upgraded_target, &source_field, source_col)?;
                    let downgraded = downgrade_legacy_wide_ints(target_field, &restored)
                        .map_err(DataFusionError::from)?
                        .unwrap_or(restored);
                    new_columns.push(downgraded);
                } else if source_field.data_type() == target_field.data_type() {
                    // Types match, use as-is
                    new_columns.push(source_col.clone());
                } else {
                    // Need type conversion - use Arrow's cast function
                    use datafusion::arrow::compute::cast;
                    let converted = cast(source_col, target_field.data_type())
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                    new_columns.push(Arc::new(converted) as ArrayRef);
                }
                new_fields.push(target_field.as_ref().clone());
            } else {
                // Column missing - create column with default values
                // For _gs_op, use "i" (Insert) as default, otherwise use null
                if target_field.name() == crate::data::COLUMN_NAME_OP {
                    use datafusion::arrow::array::StringArray;
                    let default_value = crate::data::RowKind::Insert.to_str();
                    let default_array =
                        StringArray::from(vec![default_value.as_str(); batch.num_rows()]);
                    new_columns.push(Arc::new(default_array) as ArrayRef);
                } else {
                    use datafusion::arrow::array::new_null_array;
                    new_columns.push(new_null_array(target_field.data_type(), batch.num_rows()));
                }
                new_fields.push(target_field.as_ref().clone());
            }
        }

        let new_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(new_schema.clone(), new_columns.clone()).map_err(|e| {
            let expected_fields: Vec<String> = new_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let input_batch_fields: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            DataFusionError::from(streamling_err!(
                "failed to create RecordBatch from IPC: {}; \
                 created {} columns for {} schema fields: [{}]; \
                 input batch had {} columns: [{}]",
                e,
                new_columns.len(),
                new_schema.fields().len(),
                expected_fields.join(", "),
                batch.num_columns(),
                input_batch_fields.join(", ")
            ))
        })
    }
}

impl ToArrowConverter<Vec<u8>> for FromIpcToArrowConverter {
    fn buffer(&mut self, value: Vec<u8>) {
        self.ipc_buffers.push(value);
    }

    fn convert_to_batch(&mut self) -> Result<RecordBatch> {
        if self.ipc_buffers.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Combine all IPC buffers into a single batch
        // For now, we'll process each IPC buffer and concatenate the batches
        let mut batches = Vec::new();
        for ipc_bytes in &self.ipc_buffers {
            let batch = self.convert_batch_from_ipc(ipc_bytes)?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        // Clear buffers after processing
        self.ipc_buffers.clear();

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Concatenate all batches into one
        if batches.len() == 1 {
            batches.into_iter().next().ok_or_else(|| {
                DataFusionError::from(streamling_err!(
                    "expected at least one IPC batch but batches vector was empty"
                ))
            })
        } else {
            use datafusion::arrow::compute::concat_batches;
            let first_schema = batches[0].schema();
            let batch_schemas: Vec<String> = batches
                .iter()
                .map(|b| {
                    format!(
                        "{} fields: [{}]",
                        b.schema().fields().len(),
                        b.schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
                .collect();
            concat_batches(&first_schema, &batches).map_err(|e| {
                DataFusionError::from(streamling_err!(
                    "failed to concatenate {} IPC batches: {}; batch schemas: {}",
                    batches.len(),
                    e,
                    batch_schemas.join("; ")
                ))
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::FromArrowConverter;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::DataType;

    #[test]
    fn test_ipc_converter_handles_empty_buffer() {
        // Test that convert_to_batch returns an empty batch when no IPC buffers are provided
        // This simulates the case where TypeScript transforms return an empty array []
        use datafusion::arrow::array::{Int64Array, StringArray};

        // Create a target schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create converter without buffering any IPC data
        let mut from_ipc_converter = FromIpcToArrowConverter::new(schema.clone());

        // Convert should return an empty batch with the correct schema
        let batch = from_ipc_converter.convert_to_batch().unwrap();

        // Verify the batch is empty but has the correct schema
        assert_eq!(batch.num_rows(), 0, "Batch should have 0 rows");
        assert_eq!(batch.num_columns(), 2, "Batch should have 2 columns");
        assert_eq!(batch.schema(), schema, "Schema should match target schema");

        // Verify columns exist and are of correct type
        let id_col = batch.column(0);
        assert!(
            id_col.as_any().downcast_ref::<Int64Array>().is_some(),
            "id column should be Int64Array"
        );

        let name_col = batch.column(1);
        assert!(
            name_col.as_any().downcast_ref::<StringArray>().is_some(),
            "name column should be StringArray"
        );
    }

    #[test]
    fn test_ipc_converter_handles_zero_row_batch() {
        // Test that convert_to_batch correctly handles a batch with 0 rows
        // This tests the schema conversion when the IPC file contains an empty batch
        use datafusion::arrow::array::{Int64Array, StringBuilder};

        // Create input schema (what we serialize)
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create an empty batch
        let id_array = Int64Array::from(Vec::<i64>::new());
        let mut name_builder = StringBuilder::new();
        let name_array = Arc::new(name_builder.finish()) as ArrayRef;

        let empty_batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![Arc::new(id_array) as ArrayRef, name_array],
        )
        .unwrap();

        assert_eq!(empty_batch.num_rows(), 0);

        // Convert to IPC
        let to_ipc_converter = FromArrowToIpcConverter::new();
        let ipc_bytes_vec = to_ipc_converter.convert_from_batch(&empty_batch).unwrap();

        // Note: An empty batch (0 rows) converts to an empty vector of IPC bytes
        // because convert_from_batch returns vec![] for empty batches
        assert!(
            ipc_bytes_vec.is_empty(),
            "Empty batch should produce empty IPC bytes"
        );

        // When we try to convert back with no IPC data, we should get an empty batch
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("output_id", DataType::Int64, true),
            Field::new("output_name", DataType::Utf8, true),
        ]));

        let mut from_ipc_converter = FromIpcToArrowConverter::new(target_schema.clone());
        // Don't buffer anything (simulates empty IPC output)
        let restored_batch = from_ipc_converter.convert_to_batch().unwrap();

        // Should get empty batch with target schema
        assert_eq!(restored_batch.num_rows(), 0);
        assert_eq!(restored_batch.schema(), target_schema);
    }

    // ------- T031: decimal_arb survives Arrow IPC round-trip -------
    //
    // The IPC writer leaves non-u256/i256 fields untouched and Arrow IPC
    // preserves field metadata natively, so decimal_arb columns flow through
    // without conversion. The reader's `convert_batch_to_original_schema`
    // sees matching schemas (LargeBinary + same extension metadata) and
    // early-returns the batch as-is. This test pins that behavior.

    #[test]
    fn test_arrow_ipc_arrow_roundtrip_with_decimal_arb() {
        use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue};
        use std::str::FromStr;

        let field = DecimalArbType::field("amount", 100, 18, true).unwrap();
        let schema = Arc::new(Schema::new(vec![field]));

        // Build a batch with three values including a 100-digit one,
        // a NULL, and a negative.
        let mut s = String::with_capacity(101);
        s.push('1');
        for _ in 0..81 {
            s.push('0');
        }
        s.push_str(".000000000000000001");

        let mut b = DecimalArbArrayBuilder::with_capacity(3, "amount", 100, 18).unwrap();
        b.append_str(&s).unwrap();
        b.append_null();
        b.append_str("-99.5").unwrap();
        let (raw, _, _) = b.finish().into_inner();
        let original_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(raw) as ArrayRef]).unwrap();

        // Round-trip: Arrow -> IPC -> Arrow.
        let to_ipc = FromArrowToIpcConverter::new();
        let ipc_bytes_vec = to_ipc.convert_from_batch(&original_batch).unwrap();
        assert_eq!(ipc_bytes_vec.len(), 1);

        let mut from_ipc = FromIpcToArrowConverter::new(schema.clone());
        from_ipc.buffer(ipc_bytes_vec.into_iter().next().unwrap());
        let restored_batch = from_ipc.convert_to_batch().unwrap();

        assert_eq!(restored_batch.num_rows(), 3);
        assert_eq!(restored_batch.num_columns(), 1);

        // Field metadata must round-trip — that's what makes the column a
        // decimal_arb column rather than plain LargeBinary downstream.
        let restored_field = restored_batch.schema().field(0).clone();
        assert!(
            DecimalArbType::is_decimal_arb_field(&restored_field),
            "decimal_arb extension metadata must survive Arrow IPC round-trip"
        );
        assert_eq!(
            DecimalArbType::precision_scale_from_field(&restored_field),
            Some((100, 18)),
        );

        // Values must round-trip byte-for-byte (canonical encoding is stable).
        let restored = restored_batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::LargeBinaryArray>()
            .expect("decimal_arb storage type is LargeBinary");

        let v0 = DecimalArbValue::from_canonical_bytes_at_scale(restored.value(0), 18).unwrap();
        assert_eq!(v0, DecimalArbValue::from_str(&s).unwrap());
        assert!(restored.is_null(1));
        let v2 = DecimalArbValue::from_canonical_bytes_at_scale(restored.value(2), 18).unwrap();
        assert_eq!(v2, DecimalArbValue::from_str("-99.5").unwrap());
    }

    /// A companion plugin still emits the retired `streamling.u256` /
    /// `streamling.i256` columns (big-endian FixedSizeBinary(32)). Scripts
    /// must keep seeing them as decimal strings, and a script whose output
    /// schema inherits the legacy type must get the legacy bytes back —
    /// value-checked, not reinterpreted.
    #[test]
    fn test_legacy_wide_int_columns_cross_the_script_boundary_as_decimal_text() {
        use crate::types::decimal_arb_legacy::{
            LEGACY_I256_EXTENSION_NAME, LEGACY_U256_EXTENSION_NAME,
        };
        use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
        use datafusion::arrow::array::{
            FixedSizeBinaryArray, Int32Array, StringArray, StructArray,
        };
        use datafusion::arrow::ipc::reader::FileReader;
        use std::collections::HashMap;

        let legacy = |name: &str, ext: &str| {
            Field::new(name, DataType::FixedSizeBinary(32), true).with_metadata(HashMap::from([(
                EXTENSION_TYPE_NAME_KEY.to_string(),
                ext.to_string(),
            )]))
        };
        let be32 = |v: i128| {
            let mut b = if v < 0 { [0xff_u8; 32] } else { [0_u8; 32] };
            b[16..].copy_from_slice(&v.to_be_bytes());
            b
        };
        let mut two_248 = [0_u8; 32];
        two_248[0] = 1;
        let u_max = [0xff_u8; 32];

        let amount = legacy("amount", LEGACY_U256_EXTENSION_NAME);
        let delta = Arc::new(legacy("delta", LEGACY_I256_EXTENSION_NAME));
        let nested = Field::new(
            "nested",
            DataType::Struct(vec![Arc::clone(&delta)].into()),
            true,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            amount,
            nested,
        ]));
        let amounts: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(be32(1)), None, Some(two_248), Some(u_max)].into_iter(),
                32,
            )
            .unwrap(),
        );
        let deltas: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                [
                    be32(-1),
                    be32(0),
                    be32(-170141183460469231731687303715884105728),
                    be32(42),
                ]
                .iter()
                .map(|b| b.as_slice()),
            )
            .unwrap(),
        );
        let nested_array: ArrayRef =
            Arc::new(StructArray::new(vec![delta].into(), vec![deltas], None));
        let original = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                amounts,
                nested_array,
            ],
        )
        .unwrap();

        // Arrow -> IPC: what the script receives.
        let ipc_bytes = FromArrowToIpcConverter::new()
            .convert_from_batch(&original)
            .unwrap()
            .remove(0);
        let wire = FileReader::try_new(Cursor::new(ipc_bytes.as_slice()), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(wire.schema().field(1).data_type(), &DataType::Utf8);
        let amount_text = wire
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(amount_text.value(0), "1");
        assert!(amount_text.is_null(1));
        assert_eq!(
            amount_text.value(2),
            "452312848583266388373324160190187140051835877600158453279131187530910662656"
        );
        assert_eq!(
            amount_text.value(3),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
        let delta_text = wire
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        assert_eq!(delta_text.value(0), "-1");
        assert_eq!(delta_text.value(1), "0");
        assert_eq!(
            delta_text.value(2),
            "-170141183460469231731687303715884105728"
        );
        assert_eq!(delta_text.value(3), "42");

        // IPC -> Arrow under the inherited legacy schema: identical bytes.
        let mut from_ipc = FromIpcToArrowConverter::new(schema.clone());
        from_ipc.buffer(ipc_bytes);
        let restored = from_ipc.convert_to_batch().unwrap();
        assert_eq!(restored.schema(), schema);
        assert_eq!(restored, original);

        // A script that writes new values hands text back; it is parsed and
        // range-checked against the legacy type, never copied as bytes.
        let text_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Utf8, true),
            Field::new(
                "nested",
                DataType::Struct(vec![Arc::new(Field::new("delta", DataType::Utf8, true))].into()),
                true,
            ),
        ]));
        let script_output = |amount: &str, delta: &str| {
            let deltas: ArrayRef = Arc::new(StringArray::from(vec![delta]));
            let nested: ArrayRef = Arc::new(StructArray::new(
                vec![Arc::new(Field::new("delta", DataType::Utf8, true))].into(),
                vec![deltas],
                None,
            ));
            let batch = RecordBatch::try_new(
                text_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![9])),
                    Arc::new(StringArray::from(vec![amount])),
                    nested,
                ],
            )
            .unwrap();
            FromArrowToIpcConverter::new()
                .convert_from_batch(&batch)
                .unwrap()
                .remove(0)
        };
        let mut from_ipc = FromIpcToArrowConverter::new(schema.clone());
        from_ipc.buffer(script_output("100", "-100"));
        let restored = from_ipc.convert_to_batch().unwrap();
        let amount = restored
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(amount.value(0), be32(100).as_slice());
        let delta = restored
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .clone();
        assert_eq!(delta.value(0), be32(-100).as_slice());

        for (amount, delta) in [("-1", "0"), ("1.5", "0"), ("abc", "0"), ("1", "1e80")] {
            let mut from_ipc = FromIpcToArrowConverter::new(schema.clone());
            from_ipc.buffer(script_output(amount, delta));
            from_ipc
                .convert_to_batch()
                .expect_err("a value the legacy wire type cannot hold must fail loudly");
        }
    }
}
