use crate::formats::{FromArrowConverter, ToArrowConverter};
use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue};
use crate::types::i256::{I256Type, i256_to_bytes, string_to_i256};
use crate::types::u256::{U256Type, bytes_to_u256, string_to_u256, u256_to_bytes, u256_to_string};
use arrow_json::reader::Decoder;
use arrow_json::writer::JsonFormat;
use arrow_json::{ReaderBuilder, WriterBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, LargeBinaryArray, StringArray,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;
use tracing::error;

use crate::{streamling_err, streamling_user_err};

#[derive(Debug, Default)]
// Formats json without any characters separating items
pub struct NoDelimiter {}
impl JsonFormat for NoDelimiter {}

pub struct FromArrowToJsonConverter {}

impl FromArrowToJsonConverter {
    pub fn new() -> Self {
        Self {}
    }

    fn to_json(&self, batch: &RecordBatch) -> Result<Vec<u8>> {
        // If the schema contains U256 or decimal_arb extension fields, convert
        // those columns to Utf8 (decimal strings) so the standard arrow-json
        // writer can serialize them as JSON strings.
        let needs_transform = batch
            .schema()
            .fields()
            .iter()
            .any(|f| U256Type::is_u256_field(f) || DecimalArbType::is_decimal_arb_field(f));

        let transformed_batch =
            if needs_transform {
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

                        // Build a StringArray for the decimal string representation
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
                    } else if DecimalArbType::is_decimal_arb_field(field) {
                        // Convert decimal_arb LargeBinary -> Utf8 with canonical decimal text.
                        let (_, scale) = DecimalArbType::precision_scale_from_field(field)
                            .ok_or_else(|| {
                                DataFusionError::from(streamling_err!(
                                    "decimal_arb field '{}' missing precision/scale metadata",
                                    field.name(),
                                ))
                            })?;
                        let col = batch.column(idx);
                        let lba =
                        col.as_any().downcast_ref::<LargeBinaryArray>().ok_or_else(|| {
                            DataFusionError::from(streamling_err!(
                                "expected LargeBinaryArray for decimal_arb field '{}', got {:?}",
                                field.name(),
                                col.data_type(),
                            ))
                        })?;
                        let mut string_values: Vec<Option<String>> = Vec::with_capacity(lba.len());
                        for row_idx in 0..lba.len() {
                            if lba.is_null(row_idx) {
                                string_values.push(None);
                            } else {
                                let value = DecimalArbValue::from_canonical_bytes_at_scale(
                                    lba.value(row_idx),
                                    scale,
                                )?;
                                string_values.push(Some(value.to_canonical_string()));
                            }
                        }
                        new_columns.push(Arc::new(StringArray::from(string_values)) as ArrayRef);
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

        let buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<Vec<u8>, NoDelimiter>(buf);
        writer.write(&transformed_batch)?;
        writer.finish()?;
        let buf = writer.into_inner();

        Ok(buf)
    }
}

impl Default for FromArrowToJsonConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl FromArrowConverter<Vec<u8>> for FromArrowToJsonConverter {
    fn convert_from_batch(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let mut buffer = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let row = batch.slice(i, 1);
            buffer.push(self.to_json(&row)?);
        }

        Ok(buffer)
    }
}

pub struct JsonToArrowConverter {
    schema: SchemaRef,
    values: Vec<String>,
    decoder: Decoder,
    single_row_mode: bool,
    field_to_extract: Option<String>,
}

impl JsonToArrowConverter {
    /// Create a new JSON to Arrow converter
    /// `single_row_mode` - if true, each JSON string represents a single row, otherwise a JSON array of objects is expected
    /// `field_to_extract` - if set, the field to extract from the JSON object. This allows to "unwrap" a JSON object, e.g. an envelope
    pub fn new(schema: SchemaRef, single_row_mode: bool, field_to_extract: Option<String>) -> Self {
        // If the schema contains U256/I256 or decimal_arb extension fields,
        // create a transformed schema where those fields are converted to Utf8
        // so the standard arrow-json decoder accepts incoming JSON strings.
        let needs_transform = schema.fields().iter().any(|f| {
            U256Type::is_u256_field(f)
                || I256Type::is_i256_field(f)
                || DecimalArbType::is_decimal_arb_field(f)
        });

        let decoder = if needs_transform {
            let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
            for field in schema.fields().iter() {
                if U256Type::is_u256_field(field)
                    || I256Type::is_i256_field(field)
                    || DecimalArbType::is_decimal_arb_field(field)
                {
                    new_fields.push(Field::new(
                        field.name(),
                        DataType::Utf8,
                        field.is_nullable(),
                    ));
                } else {
                    new_fields.push(field.as_ref().clone());
                }
            }
            let transformed_schema = Arc::new(Schema::new(new_fields));
            ReaderBuilder::new(transformed_schema)
                .build_decoder()
                .unwrap()
        } else {
            ReaderBuilder::new(schema.clone()).build_decoder().unwrap()
        };

        Self {
            schema,
            values: Vec::new(),
            decoder,
            single_row_mode,
            field_to_extract,
        }
    }

    fn extract_field_from(field: String, value: &Value) -> Result<Value> {
        match value.get(field.as_str()) {
            Some(v) => Ok(v.clone()),
            None => Err(streamling_user_err!("field '{}' not found in JSON object", field).into()),
        }
    }

    /// Convert a decoded batch back to the original schema, converting Utf8
    /// fields back to U256 / I256 / decimal_arb where applicable.
    fn convert_batch_to_original_schema(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let needs_transform = self.schema.fields().iter().any(|f| {
            U256Type::is_u256_field(f)
                || I256Type::is_i256_field(f)
                || DecimalArbType::is_decimal_arb_field(f)
        });

        if !needs_transform {
            return Ok(batch);
        }

        let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        let mut new_fields: Vec<Field> = Vec::with_capacity(batch.num_columns());

        for (idx, field) in self.schema.fields().iter().enumerate() {
            if U256Type::is_u256_field(field) {
                // Convert Utf8 -> FixedSizeBinary(32) for U256
                let col = batch.column(idx);
                let string_array = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::from(streamling_err!(
                        "expected StringArray for U256 field '{}', got {:?}",
                        field.name(),
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
                                field.name(),
                                e
                            ))
                        })?;
                    }
                }
                new_columns.push(Arc::new(builder.finish()) as ArrayRef);
                new_fields.push(field.as_ref().clone());
            } else if DecimalArbType::is_decimal_arb_field(field) {
                // Convert Utf8 -> decimal_arb LargeBinary at the declared scale.
                let (precision, scale) = DecimalArbType::precision_scale_from_field(field)
                    .ok_or_else(|| {
                        DataFusionError::from(streamling_err!(
                            "decimal_arb field '{}' missing precision/scale metadata",
                            field.name(),
                        ))
                    })?;
                let col = batch.column(idx);
                let string_array = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::from(streamling_err!(
                        "expected StringArray for decimal_arb field '{}', got {:?}",
                        field.name(),
                        col.data_type(),
                    ))
                })?;
                let mut builder = DecimalArbArrayBuilder::with_capacity(
                    string_array.len(),
                    field.name(),
                    precision,
                    scale,
                )?;
                for row_idx in 0..string_array.len() {
                    if string_array.is_null(row_idx) {
                        builder.append_null();
                    } else {
                        let value = DecimalArbValue::from_str(string_array.value(row_idx))?;
                        builder.append_value(&value)?;
                    }
                }
                let (raw, _, _) = builder.finish().into_inner();
                new_columns.push(Arc::new(raw) as ArrayRef);
                new_fields.push(field.as_ref().clone());
            } else if I256Type::is_i256_field(field) {
                // Convert Utf8 -> FixedSizeBinary(32) for I256
                let col = batch.column(idx);
                let string_array = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::from(streamling_err!(
                        "expected StringArray for I256 field '{}', got {:?}",
                        field.name(),
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
                                field.name(),
                                e
                            ))
                        })?;
                    }
                }
                new_columns.push(Arc::new(builder.finish()) as ArrayRef);
                new_fields.push(field.as_ref().clone());
            } else {
                new_columns.push(batch.column(idx).clone());
                new_fields.push(field.as_ref().clone());
            }
        }

        let new_schema = Arc::new(Schema::new(new_fields));
        RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl ToArrowConverter<String> for JsonToArrowConverter {
    fn buffer(&mut self, value: String) {
        self.values.push(value);
    }

    fn convert_to_batch(&mut self) -> Result<RecordBatch> {
        if self.values.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        if self.single_row_mode {
            for value in &self.values {
                let row: Value = serde_json::from_str(value.as_str()).map_err(|e| {
                    let json_preview = if value.len() > 500 {
                        format!(
                            "{}... (truncated, total length: {})",
                            &value[..500],
                            value.len()
                        )
                    } else {
                        value.clone()
                    };
                    error!(
                        "Failed to parse JSON in single_row_mode: {}. JSON content: {}",
                        e, json_preview
                    );
                    DataFusionError::from(streamling_user_err!(
                        "failed to parse JSON in single-row mode: {}",
                        e
                    ))
                })?;
                let row = match &self.field_to_extract {
                    Some(field) => Self::extract_field_from(field.clone(), &row)?,
                    None => row,
                };
                let rows = vec![row];
                self.decoder.serialize(&rows)?;
            }
        } else {
            for value in &self.values {
                let rows: Vec<Value> = serde_json::from_str(value.as_str()).map_err(|e| {
                    let json_preview = if value.len() > 500 {
                        format!(
                            "{}... (truncated, total length: {})",
                            &value[..500],
                            value.len()
                        )
                    } else {
                        value.clone()
                    };
                    error!(
                        "Failed to parse JSON in batch mode: {}. JSON content: {}",
                        e, json_preview
                    );
                    DataFusionError::from(streamling_user_err!(
                        "failed to parse JSON in batch mode: {}",
                        e
                    ))
                })?;
                let rows = match &self.field_to_extract {
                    Some(field) => rows
                        .iter()
                        .map(|row| Self::extract_field_from(field.clone(), row))
                        .collect::<Result<Vec<Value>>>()?,
                    None => rows,
                };
                self.decoder.serialize(&rows)?;
            }
        }

        match self.decoder.flush() {
            Ok(Some(batch)) => {
                self.values.clear();
                // Convert the batch back to the original schema if U256/I256 fields were transformed
                self.convert_batch_to_original_schema(batch)
            }
            Ok(None) => {
                self.values.clear();
                Ok(RecordBatch::new_empty(self.schema.clone()))
            }
            Err(e) => Err(DataFusionError::ArrowError(Box::new(e), None)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_from_arrow_to_json_converter() {
        let schema = create_test_schema();

        let a = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let b = StringArray::from(vec![Some("foo"), None, Some("bar")]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(a) as ArrayRef, Arc::new(b) as ArrayRef],
        )
        .unwrap();

        let converter = FromArrowToJsonConverter::new();
        let rows = converter.convert_from_batch(&batch).unwrap();

        assert_eq!(rows.len(), 3);

        let expected = vec![
            r#"{"a":1,"b":"foo"}"#.as_bytes().to_vec(),
            r#"{"a":2,"b":null}"#.as_bytes().to_vec(),
            r#"{"a":3,"b":"bar"}"#.as_bytes().to_vec(),
        ];

        assert_eq!(rows, expected);
    }

    #[test]
    fn test_json_to_arrow_converter_batch() {
        let schema = create_test_schema();

        let mut converter = JsonToArrowConverter::new(schema.clone(), false, None);

        converter.buffer(
            r#"[{"a":1,"b":"foo"},
        {"a":2,"b":null},
        {"a":3,"b":"bar"}]"#
                .to_string(),
        );

        let batch = converter.convert_to_batch().unwrap();

        assert_from_json_to_arrow_conversion(batch);
    }

    #[test]
    fn test_json_to_arrow_converter_single_row() {
        let schema = create_test_schema();

        let mut converter = JsonToArrowConverter::new(schema.clone(), true, None);

        converter.buffer(r#"{"a":1,"b":"foo"}"#.to_string());
        converter.buffer(r#"{"a":2,"b":null}"#.to_string());
        converter.buffer(r#"{"a":3,"b":"bar"}"#.to_string());

        let batch = converter.convert_to_batch().unwrap();

        assert_from_json_to_arrow_conversion(batch);
    }

    #[test]
    fn test_json_to_arrow_converter_batch_with_envelope() {
        let schema = create_test_schema();

        let mut converter =
            JsonToArrowConverter::new(schema.clone(), false, Some("data".to_string()));

        converter.buffer(
            r#"[{"metadata":{"op":"i"},"data":{"a":1,"b":"foo"}},
        {"metadata":{"op":"i"},"data":{"a":2,"b":null}},
        {"metadata":{"op":"i"},"data":{"a":3,"b":"bar"}}]"#
                .to_string(),
        );

        let batch = converter.convert_to_batch().unwrap();

        assert_from_json_to_arrow_conversion(batch);
    }

    #[test]
    fn test_json_to_arrow_converter_single_row_with_envelope() {
        let schema = create_test_schema();

        let mut converter =
            JsonToArrowConverter::new(schema.clone(), true, Some("data".to_string()));

        converter.buffer(r#"{"metadata":{"op":"i"},"data":{"a":1,"b":"foo"}}"#.to_string());
        converter.buffer(r#"{"metadata":{"op":"i"},"data":{"a":2,"b":null}}"#.to_string());
        converter.buffer(r#"{"metadata":{"op":"i"},"data":{"a":3,"b":"bar"}}"#.to_string());

        let batch = converter.convert_to_batch().unwrap();

        assert_from_json_to_arrow_conversion(batch);
    }

    #[test]
    fn test_from_arrow_to_json_with_u256() {
        // Build a schema with a U256 field
        let u256_field =
            Field::new("u256", U256Type::new(), true).with_metadata(U256Type::metadata());
        let schema = Arc::new(Schema::new(vec![u256_field]));

        // Build a FixedSizeBinary(32) array with U256 bytes
        let value = crate::types::u256::U256::from(12345u64);
        let bytes = crate::types::u256::u256_to_bytes(&value);
        let mut builder = FixedSizeBinaryBuilder::with_capacity(1, 32);
        builder.append_value(bytes.as_slice()).unwrap();
        let array = builder.finish();

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as ArrayRef]).unwrap();

        let converter = FromArrowToJsonConverter::new();
        let rows = converter.convert_from_batch(&batch).unwrap();
        assert_eq!(rows.len(), 1);
        let json_str = String::from_utf8(rows[0].clone()).unwrap();
        assert_eq!(json_str, r#"{"u256":"12345"}"#);
    }
    #[test]
    fn test_json_to_arrow_converter_with_u256_single_row() {
        // Build a schema with a U256 field
        let u256_field =
            Field::new("u256", U256Type::new(), true).with_metadata(U256Type::metadata());
        let schema = Arc::new(Schema::new(vec![u256_field]));

        let mut converter = JsonToArrowConverter::new(schema.clone(), true, None);

        converter.buffer(r#"{"u256":"12345"}"#.to_string());
        converter.buffer(r#"{"u256":"67890"}"#.to_string());
        converter.buffer(r#"{"u256":null}"#.to_string());

        let batch = converter.convert_to_batch().unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);

        let u256_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        assert_eq!(u256_col.len(), 3);
        assert!(!u256_col.is_null(0));
        assert!(!u256_col.is_null(1));
        assert!(u256_col.is_null(2));

        // Verify the first value
        let bytes0 = u256_col.value(0);
        let mut fixed0: [u8; 32] = [0u8; 32];
        fixed0.copy_from_slice(bytes0);
        let val0 = bytes_to_u256(&fixed0);
        assert_eq!(val0, crate::types::u256::U256::from(12345u64));

        // Verify the second value
        let bytes1 = u256_col.value(1);
        let mut fixed1: [u8; 32] = [0u8; 32];
        fixed1.copy_from_slice(bytes1);
        let val1 = bytes_to_u256(&fixed1);
        assert_eq!(val1, crate::types::u256::U256::from(67890u64));
    }

    #[test]
    fn test_json_to_arrow_converter_with_u256_and_other_fields() {
        // Build a schema with U256 and other fields
        let u256_field =
            Field::new("u256", U256Type::new(), true).with_metadata(U256Type::metadata());
        let int_field = Field::new("id", DataType::Int32, false);
        let string_field = Field::new("name", DataType::Utf8, true);
        let schema = Arc::new(Schema::new(vec![int_field, u256_field, string_field]));

        let mut converter = JsonToArrowConverter::new(schema.clone(), false, None);

        converter.buffer(
            r#"[{"id":1,"u256":"12345","name":"foo"},{"id":2,"u256":"67890","name":null}]"#
                .to_string(),
        );

        let batch = converter.convert_to_batch().unwrap();

        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 2);

        // Check Int32 column
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);

        // Check U256 column
        let u256_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!(!u256_col.is_null(0));
        assert!(!u256_col.is_null(1));

        let bytes0 = u256_col.value(0);
        let mut fixed0: [u8; 32] = [0u8; 32];
        fixed0.copy_from_slice(bytes0);
        let val0 = bytes_to_u256(&fixed0);
        assert_eq!(val0, crate::types::u256::U256::from(12345u64));

        let bytes1 = u256_col.value(1);
        let mut fixed1: [u8; 32] = [0u8; 32];
        fixed1.copy_from_slice(bytes1);
        let val1 = bytes_to_u256(&fixed1);
        assert_eq!(val1, crate::types::u256::U256::from(67890u64));

        // Check String column
        let name_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "foo");
        assert!(name_col.is_null(1));
    }

    #[test]
    fn test_json_to_arrow_converter_u256_round_trip() {
        // Test round-trip: Arrow -> JSON -> Arrow
        let u256_field =
            Field::new("u256", U256Type::new(), true).with_metadata(U256Type::metadata());
        let schema = Arc::new(Schema::new(vec![u256_field]));

        // Create original batch
        let value1 = crate::types::u256::U256::from(12345u64);
        let value2 = crate::types::u256::U256::from(67890u64);
        let bytes1 = crate::types::u256::u256_to_bytes(&value1);
        let bytes2 = crate::types::u256::u256_to_bytes(&value2);
        let mut builder = FixedSizeBinaryBuilder::with_capacity(2, 32);
        builder.append_value(bytes1.as_slice()).unwrap();
        builder.append_value(bytes2.as_slice()).unwrap();
        let array = builder.finish();
        let original_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as ArrayRef]).unwrap();

        // Convert to JSON
        let to_json_converter = FromArrowToJsonConverter::new();
        let json_rows = to_json_converter
            .convert_from_batch(&original_batch)
            .unwrap();

        // Convert back from JSON
        let mut from_json_converter = JsonToArrowConverter::new(schema.clone(), true, None);
        for row in json_rows {
            let json_str = String::from_utf8(row).unwrap();
            from_json_converter.buffer(json_str);
        }
        let round_trip_batch = from_json_converter.convert_to_batch().unwrap();

        // Verify round-trip
        assert_eq!(round_trip_batch.num_rows(), original_batch.num_rows());
        assert_eq!(round_trip_batch.num_columns(), original_batch.num_columns());

        let original_col = original_batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let round_trip_col = round_trip_batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        for i in 0..original_col.len() {
            let orig_bytes = original_col.value(i);
            let rt_bytes = round_trip_col.value(i);
            assert_eq!(orig_bytes, rt_bytes);
        }
    }

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]))
    }

    fn assert_from_json_to_arrow_conversion(batch: RecordBatch) {
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(a, &Int32Array::from(vec![1, 2, 3]));
        assert_eq!(b, &StringArray::from(vec![Some("foo"), None, Some("bar")]));
    }

    // ------- decimal_arb JSON round-trip (T030 / T019) -------

    #[test]
    fn test_from_arrow_to_json_with_decimal_arb() {
        // Build a schema with a single decimal_arb(100, 18) field.
        let field = DecimalArbType::field("amount", 100, 18, true).unwrap();
        let schema = Arc::new(Schema::new(vec![field]));

        // Build a one-row batch with a 100-digit value.
        let mut s = String::with_capacity(101);
        s.push('1');
        for _ in 0..81 {
            s.push('0');
        }
        s.push('.');
        s.push_str("000000000000000001");

        let mut b = DecimalArbArrayBuilder::with_capacity(1, "amount", 100, 18).unwrap();
        b.append_str(&s).unwrap();
        let (raw, _, _) = b.finish().into_inner();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(raw) as ArrayRef]).unwrap();

        let converter = FromArrowToJsonConverter::new();
        let rows = converter.convert_from_batch(&batch).unwrap();
        assert_eq!(rows.len(), 1);
        let json_str = String::from_utf8(rows[0].clone()).unwrap();
        assert_eq!(json_str, format!(r#"{{"amount":"{}"}}"#, s));
    }

    #[test]
    fn test_decimal_arb_round_trip_through_json() {
        // Schema: id (Int64) + amount (decimal_arb(80, 40)).
        let id = Field::new("id", DataType::Int64, false);
        let amount = DecimalArbType::field("amount", 80, 40, true).unwrap();
        let schema = Arc::new(Schema::new(vec![id, amount]));

        let mut converter = JsonToArrowConverter::new(schema.clone(), false, None);
        converter.buffer(
            r#"[{"id":1,"amount":"1234567890.987654321098765432109876543210"},
                {"id":2,"amount":null},
                {"id":3,"amount":"-0.0000000000000000000000000000000000000001"}]"#
                .to_string(),
        );

        let batch = converter.convert_to_batch().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        let amount_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert!(amount_col.is_null(1), "row 1 amount must be NULL");

        let v0 = DecimalArbValue::from_canonical_bytes_at_scale(amount_col.value(0), 40).unwrap();
        assert_eq!(
            v0,
            DecimalArbValue::from_str("1234567890.987654321098765432109876543210").unwrap()
        );

        let v2 = DecimalArbValue::from_canonical_bytes_at_scale(amount_col.value(2), 40).unwrap();
        assert_eq!(
            v2,
            DecimalArbValue::from_str("-0.0000000000000000000000000000000000000001").unwrap()
        );

        // Re-serialize and confirm the round-trip preserves the *numeric*
        // value. The canonical string after a (parse → encode at scale=40 →
        // decode at scale=40 → format) round-trip pads the original 30-digit
        // fractional input with 10 trailing zeros, because the column scale
        // (40) is part of the storage contract; this is correct per the
        // Arrow extension-type contract §3.
        let writer = FromArrowToJsonConverter::new();
        let serialized = writer.convert_from_batch(&batch).unwrap();
        let row0 = String::from_utf8(serialized[0].clone()).unwrap();
        assert!(
            row0.contains(r#""amount":"1234567890.9876543210987654321098765432100000000000""#),
            "row0 after round-trip should contain the column-scale-padded value: {}",
            row0,
        );
        let row1 = String::from_utf8(serialized[1].clone()).unwrap();
        assert!(row1.contains(r#""amount":null"#));
    }

    #[test]
    fn test_json_to_arrow_decimal_arb_rejects_value_exceeding_declared_precision() {
        // (precision, scale) = (5, 0); a 6-digit value must surface FR-013 error.
        let field = DecimalArbType::field("x", 5, 0, true).unwrap();
        let schema = Arc::new(Schema::new(vec![field]));
        let mut converter = JsonToArrowConverter::new(schema, true, None);
        converter.buffer(r#"{"x":"123456"}"#.to_string());
        let err = converter.convert_to_batch().unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("'x'"), "error must name the column: {}", msg);
    }
}
