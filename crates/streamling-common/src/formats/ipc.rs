use crate::formats::{FromArrowConverter, ToArrowConverter};
use crate::streamling_err;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::arrow::array::ArrayRef;
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
        // Feature 002 (Retire U256/I256): the previous U256/I256 → Utf8
        // string-conversion at IPC write time is no longer needed — wide
        // integers flow through decimal_arb (LargeBinary) which Arrow IPC
        // serializes natively, preserving the field's extension metadata.
        let transformed_batch = batch.clone();

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

    fn convert_batch_to_original_schema(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // Feature 002: U256/I256 conversion no longer needed (those types
        // are retired in favor of decimal_arb, which Arrow IPC carries
        // natively).
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

                if source_field.data_type() == target_field.data_type() {
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
}
