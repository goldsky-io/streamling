use crate::formats::{FromArrowConverter, ToArrowConverter};
use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue};
// Feature 002 (Retire U256/I256): U256/I256 imports removed — wide
// integers flow through decimal_arb only.
use arrow_json::reader::Decoder;
use arrow_json::writer::JsonFormat;
use arrow_json::{ReaderBuilder, WriterBuilder};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeBinaryArray, LargeListArray, ListArray, MapArray,
    StringArray, StructArray,
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
        // If the schema carries any decimal_arb extension field — at the top
        // level OR nested inside a Struct / List / Map — rewrite those leaves to
        // Utf8 (canonical decimal text) so the standard arrow-json writer emits
        // the value, not the raw canonical bytes as hex. (Top-level-only handling
        // was the cause of F6: nested decimal_arb serialized as hex.)
        let needs_transform = batch
            .schema()
            .fields()
            .iter()
            .any(|f| field_contains_decimal_arb(f));

        let transformed_batch = if needs_transform {
            let mut new_fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
            let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
            for (idx, field) in batch.schema().fields().iter().enumerate() {
                let (nf, na) = decimalize_for_json(field, batch.column(idx))?;
                new_fields.push(nf);
                new_columns.push(na);
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

/// Returns `true` if `field` is `decimal_arb`, or contains a `decimal_arb`
/// leaf nested anywhere inside a Struct / List / LargeList / FixedSizeList /
/// Map. Used to decide whether a batch needs the decimal_arb → Utf8 rewrite
/// before JSON serialization.
fn field_contains_decimal_arb(field: &Field) -> bool {
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
fn field_with_type(orig: &Field, data_type: DataType) -> Field {
    Field::new(orig.name(), data_type, orig.is_nullable()).with_metadata(orig.metadata().clone())
}

/// Convert a `decimal_arb` `LargeBinaryArray` to a `StringArray` of canonical
/// decimal text (nulls preserved), reading the scale from the field metadata.
fn decimal_arb_to_strings(field: &Field, array: &ArrayRef) -> Result<StringArray> {
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
/// Utf8 canonical-decimal string for JSON output. Non-decimal_arb leaves and
/// containers without any decimal_arb descendant are returned unchanged.
fn decimalize_for_json(field: &Field, array: &ArrayRef) -> Result<(Field, ArrayRef)> {
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
                let (nf, na) = decimalize_for_json(child, col)?;
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
            let (nf, nv) = decimalize_for_json(child, la.values())?;
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
            let (nf, nv) = decimalize_for_json(child, la.values())?;
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
            let (nf, nv) = decimalize_for_json(child, fa.values())?;
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
            let (nef, nea) = decimalize_for_json(entry_field, &entries)?;
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
        // Feature 002: only decimal_arb fields need Utf8 transformation for
        // JSON decoding now — U256/I256 are retired.
        let needs_transform = schema
            .fields()
            .iter()
            .any(|f| DecimalArbType::is_decimal_arb_field(f));

        let decoder = if needs_transform {
            let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
            for field in schema.fields().iter() {
                if DecimalArbType::is_decimal_arb_field(field) {
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
    /// fields back to decimal_arb where applicable. (Feature 002 retired
    /// U256/I256 — those conversions are gone.)
    fn convert_batch_to_original_schema(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let needs_transform = self
            .schema
            .fields()
            .iter()
            .any(|f| DecimalArbType::is_decimal_arb_field(f));

        if !needs_transform {
            return Ok(batch);
        }

        let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        let mut new_fields: Vec<Field> = Vec::with_capacity(batch.num_columns());

        for (idx, field) in self.schema.fields().iter().enumerate() {
            if DecimalArbType::is_decimal_arb_field(field) {
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

    /// Test schema with two fields: `a` (Int32, non-nullable) and
    /// `b` (Utf8, nullable). Used by the basic JSON converter tests.
    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]))
    }

    /// Round-trip a RecordBatch through `FromArrowToJsonConverter` →
    /// `JsonToArrowConverter` (batch mode) and assert the resulting
    /// batch equals the original. Used by the basic converter tests.
    fn assert_from_json_to_arrow_conversion(input: RecordBatch) {
        let schema = input.schema();
        // FromArrow → JSON
        let from_arrow = FromArrowToJsonConverter::new();
        let json_rows: Vec<Vec<u8>> = from_arrow.convert_from_batch(&input).unwrap();
        // Combine the per-row JSON objects into a JSON array string for batch-mode parse.
        let json_array = format!(
            "[{}]",
            json_rows
                .iter()
                .map(|row| std::str::from_utf8(row).unwrap().to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        let mut to_arrow = JsonToArrowConverter::new(schema, false, None);
        to_arrow.buffer(json_array);
        let output = to_arrow.convert_to_batch().unwrap();
        assert_eq!(output.num_rows(), input.num_rows());
        assert_eq!(output.num_columns(), input.num_columns());
    }

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

    /// A schema covering only a subset of the JSON object's fields decodes just those
    /// columns and ignores the rest. The Kafka JSON source relies on this to apply column
    /// projection by decoding against a projected payload schema.
    #[test]
    fn test_json_to_arrow_converter_subset_schema_ignores_extra_fields() {
        let subset_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let mut converter = JsonToArrowConverter::new(subset_schema, true, None);
        converter.buffer(r#"{"a":1,"b":"foo"}"#.to_string());
        converter.buffer(r#"{"a":2,"b":"bar"}"#.to_string());

        let batch = converter.convert_to_batch().unwrap();

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 2);
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(a, &Int32Array::from(vec![1, 2]));
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

    // ------- nested decimal_arb JSON serialization (F6) -------

    /// A `decimal_arb` nested inside a struct must serialize as its decimal
    /// value, not the raw canonical bytes as hex (F6 regression guard).
    #[test]
    fn nested_struct_decimal_arb_serializes_value_not_hex() {
        let big = "123456789012345678901234567890"; // 30 digits, > 2^64
        let amt_field = DecimalArbType::field("amt", 100, 0, false).unwrap();
        let mut b = DecimalArbArrayBuilder::with_capacity(1, "amt", 100, 0).unwrap();
        b.append_str(big).unwrap();
        let (amt_raw, _, _) = b.finish().into_inner();

        let inner_fields = Fields::from(vec![Arc::new(amt_field)]);
        let inner = StructArray::new(
            inner_fields.clone(),
            vec![Arc::new(amt_raw) as ArrayRef],
            None,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("inner", DataType::Struct(inner_fields), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(inner) as ArrayRef,
            ],
        )
        .unwrap();

        let rows = FromArrowToJsonConverter::new()
            .convert_from_batch(&batch)
            .unwrap();
        let json = String::from_utf8(rows[0].clone()).unwrap();
        assert_eq!(json, format!(r#"{{"id":1,"inner":{{"amt":"{big}"}}}}"#));
    }

    /// An array of records each carrying a `decimal_arb` (the blockchain
    /// "transfers"/"traces" shape) must serialize each element's value, not hex.
    #[test]
    fn array_of_struct_decimal_arb_serializes_values_not_hex() {
        use datafusion::arrow::buffer::OffsetBuffer;

        let amt_field = DecimalArbType::field("amt", 100, 0, false).unwrap();
        let mut b = DecimalArbArrayBuilder::with_capacity(2, "amt", 100, 0).unwrap();
        b.append_str("123456789012345678901234567890").unwrap();
        b.append_str("7").unwrap();
        let (amt_raw, _, _) = b.finish().into_inner();

        let item_fields = Fields::from(vec![Arc::new(amt_field)]);
        let items_struct = StructArray::new(
            item_fields.clone(),
            vec![Arc::new(amt_raw) as ArrayRef],
            None,
        );
        let item_field = Arc::new(Field::new("item", DataType::Struct(item_fields), false));
        // Single row whose list holds both structs.
        let offsets = OffsetBuffer::new(vec![0, 2].into());
        let list = ListArray::new(
            item_field.clone(),
            offsets,
            Arc::new(items_struct) as ArrayRef,
            None,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("items", DataType::List(item_field), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(list) as ArrayRef,
            ],
        )
        .unwrap();

        let rows = FromArrowToJsonConverter::new()
            .convert_from_batch(&batch)
            .unwrap();
        let json = String::from_utf8(rows[0].clone()).unwrap();
        assert_eq!(
            json,
            r#"{"id":1,"items":[{"amt":"123456789012345678901234567890"},{"amt":"7"}]}"#
        );
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
