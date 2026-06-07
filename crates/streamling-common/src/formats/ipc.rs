use crate::formats::{FromArrowConverter, ToArrowConverter};
use crate::types::i256::{I256Type, i256_to_bytes, string_to_i256};
use crate::types::u256::{U256Type, bytes_to_u256, string_to_u256, u256_to_bytes, u256_to_string};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray,
};
use datafusion::arrow::ipc::{reader::FileReader, writer::FileWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use std::io::Cursor;
use std::sync::Arc;

use crate::streamling_err;

pub struct FromArrowToIpcConverter {}

impl FromArrowToIpcConverter {
    pub fn new() -> Self {
        Self {}
    }

    fn to_ipc(&self, batch: &RecordBatch) -> Result<Vec<u8>> {
        // If the schema contains U256/I256 extension fields, convert those columns to Utf8 (decimal strings)
        // This matches the JSON converter behavior
        let has_u256_or_i256 = batch
            .schema()
            .fields()
            .iter()
            .any(|f| U256Type::is_u256_field(f) || I256Type::is_i256_field(f));

        let transformed_batch = if has_u256_or_i256 {
            let mut new_fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
            let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

            for (idx, field) in batch.schema().fields().iter().enumerate() {
                if U256Type::is_u256_field(field) {
                    // Convert FixedSizeBinary(32) -> Utf8 with decimal string
                    let col = batch.column(idx);
                    let fsb = col
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| {
                            DataFusionError::from(streamling_err!(
                                "expected FixedSizeBinaryArray for U256 field '{}', got {:?}",
                                field.name(),
                                col.data_type()
                            ))
                        })?;

                    let mut string_values: Vec<Option<String>> = Vec::with_capacity(fsb.len());
                    for row_idx in 0..fsb.len() {
                        if fsb.is_null(row_idx) {
                            string_values.push(None);
                        } else {
                            let bytes = fsb.value(row_idx);
                            debug_assert_eq!(bytes.len(), 32);
                            let mut fixed: [u8; 32] = [0u8; 32];
                            fixed.copy_from_slice(bytes);
                            let val = bytes_to_u256(&fixed);
                            string_values.push(Some(u256_to_string(&val)));
                        }
                    }

                    let string_array = StringArray::from(string_values);
                    new_columns.push(Arc::new(string_array) as ArrayRef);
                    new_fields.push(Field::new(
                        field.name(),
                        DataType::Utf8,
                        field.is_nullable(),
                    ));
                } else if I256Type::is_i256_field(field) {
                    // Convert FixedSizeBinary(32) -> Utf8 with decimal string
                    let col = batch.column(idx);
                    let fsb = col
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| {
                            DataFusionError::from(streamling_err!(
                                "expected FixedSizeBinaryArray for I256 field '{}', got {:?}",
                                field.name(),
                                col.data_type()
                            ))
                        })?;

                    let mut string_values: Vec<Option<String>> = Vec::with_capacity(fsb.len());
                    for row_idx in 0..fsb.len() {
                        if fsb.is_null(row_idx) {
                            string_values.push(None);
                        } else {
                            let bytes = fsb.value(row_idx);
                            debug_assert_eq!(bytes.len(), 32);
                            let mut fixed: [u8; 32] = [0u8; 32];
                            fixed.copy_from_slice(bytes);
                            let i256_val = crate::types::i256::bytes_to_i256(&fixed);
                            let string_val = crate::types::i256::i256_to_string(&i256_val);
                            string_values.push(Some(string_val));
                        }
                    }

                    let string_array = StringArray::from(string_values);
                    new_columns.push(Arc::new(string_array) as ArrayRef);
                    new_fields.push(Field::new(
                        field.name(),
                        DataType::Utf8,
                        field.is_nullable(),
                    ));
                } else {
                    new_columns.push(batch.column(idx).clone());
                    new_fields.push(field.as_ref().clone());
                }
            }

            let new_schema = Arc::new(Schema::new(new_fields));
            RecordBatch::try_new(new_schema, new_columns)?
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

    fn convert_batch_to_original_schema(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // Check if we need any conversions (type mismatches or U256/I256)
        let needs_conversion = batch.schema() != self.schema
            || self
                .schema
                .fields()
                .iter()
                .any(|f| U256Type::is_u256_field(f) || I256Type::is_i256_field(f));

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

            if U256Type::is_u256_field(target_field) {
                // Convert Utf8 -> FixedSizeBinary(32) for U256
                let col = source_col_opt.ok_or_else(|| {
                    DataFusionError::from(streamling_err!(
                        "missing column for U256 field '{}'",
                        target_field.name()
                    ))
                })?;
                let string_array = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::from(streamling_err!(
                        "expected StringArray for U256 field '{}', got {:?}",
                        target_field.name(),
                        col.data_type()
                    ))
                })?;

                let mut builder = FixedSizeBinaryBuilder::with_capacity(string_array.len(), 32);
                for row_idx in 0..string_array.len() {
                    if string_array.is_null(row_idx) {
                        builder.append_null();
                    } else {
                        let str_val = string_array.value(row_idx);
                        let u256_val = string_to_u256(str_val)?;
                        let bytes = u256_to_bytes(&u256_val);
                        builder.append_value(bytes).map_err(|e| {
                            DataFusionError::from(streamling_err!(
                                "failed to append U256 value for field '{}': {}",
                                target_field.name(),
                                e
                            ))
                        })?;
                    }
                }
                new_columns.push(Arc::new(builder.finish()) as ArrayRef);
                new_fields.push(target_field.as_ref().clone());
            } else if I256Type::is_i256_field(target_field) {
                // Convert Utf8 -> FixedSizeBinary(32) for I256
                let col = source_col_opt.ok_or_else(|| {
                    DataFusionError::from(streamling_err!(
                        "missing column for I256 field '{}'",
                        target_field.name()
                    ))
                })?;
                let string_array = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::from(streamling_err!(
                        "expected StringArray for I256 field '{}', got {:?}",
                        target_field.name(),
                        col.data_type()
                    ))
                })?;

                let mut builder = FixedSizeBinaryBuilder::with_capacity(string_array.len(), 32);
                for row_idx in 0..string_array.len() {
                    if string_array.is_null(row_idx) {
                        builder.append_null();
                    } else {
                        let str_val = string_array.value(row_idx);
                        let i256_val = string_to_i256(str_val)?;
                        let bytes = i256_to_bytes(&i256_val);
                        builder.append_value(bytes).map_err(|e| {
                            DataFusionError::from(streamling_err!(
                                "failed to append I256 value for field '{}': {}",
                                target_field.name(),
                                e
                            ))
                        })?;
                    }
                }
                new_columns.push(Arc::new(builder.finish()) as ArrayRef);
                new_fields.push(target_field.as_ref().clone());
            } else if let Some(source_col) = source_col_opt {
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
    use crate::types::u256::{U256, U256Type, u256_to_bytes};
    use datafusion::arrow::array::FixedSizeBinaryBuilder;

    #[test]
    fn test_arrow_ipc_arrow_roundtrip_with_u256() {
        // Build a schema with a U256 field
        let u256_field =
            Field::new("u256", U256Type::new(), true).with_metadata(U256Type::metadata());
        let schema = Arc::new(Schema::new(vec![u256_field]));

        // Create original batch with U256 values
        let value1 = U256::from(12345u64);
        let value2 = U256::from(67890u64);
        let bytes1 = u256_to_bytes(&value1);
        let bytes2 = u256_to_bytes(&value2);

        let mut builder = FixedSizeBinaryBuilder::with_capacity(2, 32);
        builder.append_value(bytes1).unwrap();
        builder.append_value(bytes2).unwrap();
        let array = Arc::new(builder.finish()) as ArrayRef;

        let original_batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        // Convert Arrow -> IPC
        let to_ipc_converter = FromArrowToIpcConverter::new();
        let ipc_bytes_vec = to_ipc_converter
            .convert_from_batch(&original_batch)
            .unwrap();
        assert_eq!(ipc_bytes_vec.len(), 1);

        // Convert IPC -> Arrow
        let mut from_ipc_converter = FromIpcToArrowConverter::new(schema.clone());
        from_ipc_converter.buffer(ipc_bytes_vec.into_iter().next().unwrap());
        let restored_batch = from_ipc_converter.convert_to_batch().unwrap();

        // Verify round-trip
        assert_eq!(restored_batch.num_rows(), 2);
        assert_eq!(restored_batch.num_columns(), 1);

        // Check that the schema has the U256 metadata preserved
        let restored_schema = restored_batch.schema();
        let restored_field = restored_schema.field(0);
        assert!(U256Type::is_u256_field(restored_field));

        // Check the values
        let restored_col = restored_batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Expected FixedSizeBinaryArray");

        let mut fixed0 = [0u8; 32];
        fixed0.copy_from_slice(restored_col.value(0));
        let val0 = crate::types::u256::bytes_to_u256(&fixed0);
        assert_eq!(val0, value1);

        let mut fixed1 = [0u8; 32];
        fixed1.copy_from_slice(restored_col.value(1));
        let val1 = crate::types::u256::bytes_to_u256(&fixed1);
        assert_eq!(val1, value2);
    }

    #[test]
    fn test_arrow_ipc_arrow_roundtrip_with_u256_and_other_fields() {
        // Build a schema with U256 and other fields
        let u256_field =
            Field::new("amount", U256Type::new(), true).with_metadata(U256Type::metadata());
        let int_field = Field::new("id", DataType::Int32, false);
        let string_field = Field::new("name", DataType::Utf8, true);
        let schema = Arc::new(Schema::new(vec![int_field, u256_field, string_field]));

        // Create Int32 column
        use datafusion::arrow::array::{Int32Array, StringBuilder};
        let int_array = Int32Array::from(vec![1, 2]);

        // Create U256 column
        let value1 = U256::from(999999999999u64);
        let value2 = U256::from(123456789012345u64);
        let bytes1 = u256_to_bytes(&value1);
        let bytes2 = u256_to_bytes(&value2);
        let mut builder = FixedSizeBinaryBuilder::with_capacity(2, 32);
        builder.append_value(bytes1).unwrap();
        builder.append_value(bytes2).unwrap();
        let u256_array = Arc::new(builder.finish()) as ArrayRef;

        // Create String column
        let mut string_builder = StringBuilder::new();
        string_builder.append_value("alice");
        string_builder.append_value("bob");
        let string_array = Arc::new(string_builder.finish()) as ArrayRef;

        let original_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(int_array) as ArrayRef, u256_array, string_array],
        )
        .unwrap();

        // Convert Arrow -> IPC -> Arrow
        let to_ipc_converter = FromArrowToIpcConverter::new();
        let ipc_bytes_vec = to_ipc_converter
            .convert_from_batch(&original_batch)
            .unwrap();

        let mut from_ipc_converter = FromIpcToArrowConverter::new(schema.clone());
        from_ipc_converter.buffer(ipc_bytes_vec.into_iter().next().unwrap());
        let restored_batch = from_ipc_converter.convert_to_batch().unwrap();

        // Verify round-trip
        assert_eq!(restored_batch.num_rows(), 2);
        assert_eq!(restored_batch.num_columns(), 3);

        // Check Int32 column
        let id_col = restored_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Expected Int32Array");
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);

        // Check U256 column
        let restored_u256_col = restored_batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Expected FixedSizeBinaryArray");

        let mut fixed0 = [0u8; 32];
        fixed0.copy_from_slice(restored_u256_col.value(0));
        let val0 = crate::types::u256::bytes_to_u256(&fixed0);
        assert_eq!(val0, value1);

        let mut fixed1 = [0u8; 32];
        fixed1.copy_from_slice(restored_u256_col.value(1));
        let val1 = crate::types::u256::bytes_to_u256(&fixed1);
        assert_eq!(val1, value2);

        // Check String column
        let name_col = restored_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        assert_eq!(name_col.value(0), "alice");
        assert_eq!(name_col.value(1), "bob");
    }

    #[test]
    fn test_arrow_ipc_arrow_roundtrip_with_large_u256_values() {
        // Test with values that span the full 256-bit range
        let u256_field =
            Field::new("big_num", U256Type::new(), true).with_metadata(U256Type::metadata());
        let schema = Arc::new(Schema::new(vec![u256_field]));

        // Create values that use more than 64 bits
        let value1 = U256::max_value(); // Maximum U256 value
        let value2 = U256::max_value() / U256::from(2u64); // Half of max
        let value3 = U256::from(0u64); // Zero
        let bytes1 = u256_to_bytes(&value1);
        let bytes2 = u256_to_bytes(&value2);
        let bytes3 = u256_to_bytes(&value3);

        let mut builder = FixedSizeBinaryBuilder::with_capacity(3, 32);
        builder.append_value(bytes1).unwrap();
        builder.append_value(bytes2).unwrap();
        builder.append_value(bytes3).unwrap();
        let array = Arc::new(builder.finish()) as ArrayRef;

        let original_batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

        // Convert Arrow -> IPC -> Arrow
        let to_ipc_converter = FromArrowToIpcConverter::new();
        let ipc_bytes_vec = to_ipc_converter
            .convert_from_batch(&original_batch)
            .unwrap();

        let mut from_ipc_converter = FromIpcToArrowConverter::new(schema.clone());
        from_ipc_converter.buffer(ipc_bytes_vec.into_iter().next().unwrap());
        let restored_batch = from_ipc_converter.convert_to_batch().unwrap();

        // Verify round-trip
        let restored_col = restored_batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("Expected FixedSizeBinaryArray");

        let mut fixed0 = [0u8; 32];
        fixed0.copy_from_slice(restored_col.value(0));
        let val0 = crate::types::u256::bytes_to_u256(&fixed0);
        assert_eq!(val0, value1);

        let mut fixed1 = [0u8; 32];
        fixed1.copy_from_slice(restored_col.value(1));
        let val1 = crate::types::u256::bytes_to_u256(&fixed1);
        assert_eq!(val1, value2);

        let mut fixed2 = [0u8; 32];
        fixed2.copy_from_slice(restored_col.value(2));
        let val2 = crate::types::u256::bytes_to_u256(&fixed2);
        assert_eq!(val2, value3);
    }

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
}
