use crate::data::COLUMN_NAME_OP;
use crate::formats::decimal_arb_text::{
    decimal_arb_leaves_as_text_field, decimal_arb_leaves_from_text, decimal_arb_leaves_to_text,
    field_contains_decimal_arb,
};
use crate::formats::{FromArrowConverter, ToArrowConverter};
use crate::types::decimal_arb_legacy::{
    downgrade_legacy_wide_ints, field_contains_legacy_wide_int, upgrade_legacy_wide_int_batch,
    upgrade_legacy_wide_int_field,
};
// U256/I256 are retired — wide integers flow through decimal_arb only.
use arrow_json::reader::Decoder;
use arrow_json::writer::JsonFormat;
use arrow_json::{ReaderBuilder, WriterBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{error, warn};

use crate::streamling_user_err;

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
        // A plugin source may still hand over the retired FixedSizeBinary(32)
        // `streamling.u256` / `streamling.i256` columns; as decimal_arb they
        // print their value below, where the raw bytes printed as hex.
        let upgraded = upgrade_legacy_wide_int_batch(batch).map_err(DataFusionError::from)?;
        let batch = upgraded.as_ref().unwrap_or(batch);

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
                let (nf, na) = decimal_arb_leaves_to_text(field, batch.column(idx))?;
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
        // Every existing caller passes a schema already known to be decodable (a schema this
        // same converter round-trips through `FromArrowToJsonConverter`, or one built from a
        // fixed set of Arrow types); an unsupported type here is a bug in the caller, not user
        // input, so this panics instead of returning an error. Callers that build the schema
        // from user input (a script transform's `schema:` YAML, an upstream source schema)
        // must use `try_new`.
        Self::try_new(schema, single_row_mode, field_to_extract, false)
            .expect("schema is decodable: not user-supplied here, see fn doc")
    }

    /// Fallible variant of [`Self::new`] for schemas built from user input: reports a schema
    /// type arrow_json can't decode as an error naming the offending field(s) instead of
    /// panicking.
    ///
    /// `coerce_primitive` casts a value of the wrong JS-originating JSON kind into the declared
    /// column type instead of raising an error (a string into a number column, a float into an
    /// int column, a number into a string column).
    pub fn try_new(
        schema: SchemaRef,
        single_row_mode: bool,
        field_to_extract: Option<String>,
        coerce_primitive: bool,
    ) -> Result<Self> {
        let decoder = Self::build_decoder(&schema, coerce_primitive)?;
        Ok(Self {
            schema,
            values: Vec::new(),
            decoder,
            single_row_mode,
            field_to_extract,
        })
    }

    /// Builds the decoder for `schema`.
    ///
    /// decimal_arb leaves decode as Utf8 and convert back afterwards in
    /// `convert_batch_to_original_schema`. The rewrite is recursive: a decimal_arb nested in a
    /// struct/list/map needs it just as much as a top-level one.
    ///
    /// A retired `streamling.u256` / `streamling.i256` leaf needs the same treatment. The
    /// writer upgrades those columns to decimal_arb text on the way out
    /// (`upgrade_legacy_wide_int_batch`), so a script hands back decimal text; left declared as
    /// `FixedSizeBinary(32)` here, arrow-json reads that text as HEX — 32 wrong bytes for an
    /// all-hex 64-character number, and a hard error for any other width.
    ///
    /// Errors when `schema` declares a type arrow_json can't decode (e.g. an Interval type):
    /// callers that build the schema from user input (a script transform's `schema:` YAML) must
    /// report that as a user error, not panic.
    fn build_decoder(schema: &SchemaRef, coerce_primitive: bool) -> Result<Decoder> {
        let needs_transform = schema.fields().iter().any(|f| decodes_as_other_type(f));

        let decoder_schema = if needs_transform {
            let new_fields = schema
                .fields()
                .iter()
                .map(|f| {
                    // arrow_json has no Dictionary decoder: decode the value type and cast
                    // back in `convert_batch_to_original_schema`.
                    // ponytail: other nested dictionaries still error; a nested
                    // dictionary-encoded decimal_arb leaf is read as text by the
                    // leaf rewrite like any other.
                    // A dictionary-encoded decimal_arb leaf keeps its metadata
                    // on the field, so the value type it decodes as is a
                    // decimal_arb leaf and is read as text like any other.
                    if let DataType::Dictionary(_, value) = f.data_type() {
                        return Ok(decimal_arb_leaves_as_text_field(
                            &f.as_ref().clone().with_data_type(value.as_ref().clone()),
                        ));
                    }
                    // Legacy leaves become their decimal_arb equivalent first, so the text
                    // rewrite reaches them too.
                    let upgraded =
                        upgrade_legacy_wide_int_field(f).map_err(DataFusionError::from)?;
                    Ok(decimal_arb_leaves_as_text_field(
                        upgraded.as_ref().unwrap_or(f),
                    ))
                })
                .collect::<Result<Vec<Field>>>()?;
            Arc::new(Schema::new(new_fields))
        } else {
            schema.clone()
        };

        ReaderBuilder::new(decoder_schema.clone())
            .with_coerce_primitive(coerce_primitive)
            .build_decoder()
            .map_err(|e| {
                // arrow_json names the offending type but not the field. Probe each field on
                // its own so the error names the field(s) the user must change.
                let offending: Vec<&str> = decoder_schema
                    .fields()
                    .iter()
                    .filter(|f| {
                        ReaderBuilder::new(Arc::new(Schema::new(vec![f.as_ref().clone()])))
                            .build_decoder()
                            .is_err()
                    })
                    .map(|f| f.name().as_str())
                    .collect();
                let fields_named = if offending.is_empty() {
                    String::new()
                } else {
                    format!(" (offending fields: {})", offending.join(", "))
                };
                DataFusionError::from(streamling_user_err!(
                    "unsupported type in script transform schema{fields_named}: {}",
                    e
                ))
            })
    }

    /// Decodes newline-delimited JSON (one JSON object per line) into a single `RecordBatch`
    /// against this converter's schema. Used to decode WASM script transform output, where the
    /// JS runtime writes one JSON object per output row.
    ///
    /// A declared column missing from a row decodes as null. Empty input returns an empty batch.
    pub fn decode_ndjson(&mut self, bytes: &[u8]) -> Result<RecordBatch> {
        if bytes.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // A key the script returns that the output schema doesn't declare is dropped by
        // arrow_json. Warn once per process so schema drift (renamed or derived columns)
        // shows up instead of silently null downstream. Every row is scanned, so a key
        // returned only by some rows is still caught.
        // ponytail: the scan stops after DRIFT_SCAN_ROWS rows process-wide so healthy
        // pipelines stop paying for the extra parse; drift first appearing later goes unwarned.
        const DRIFT_SCAN_ROWS: usize = 10_000;
        static ROWS_LEFT_TO_SCAN: AtomicUsize = AtomicUsize::new(DRIFT_SCAN_ROWS);
        let rows_left = ROWS_LEFT_TO_SCAN.load(Ordering::Relaxed);
        if rows_left > 0 {
            let (unknown, scanned) = unknown_output_keys(&self.schema, bytes, rows_left);
            if unknown.is_empty() {
                let _ =
                    ROWS_LEFT_TO_SCAN.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                        Some(left.saturating_sub(scanned))
                    });
            } else {
                warn!(
                    "script transform output contains keys not in the output schema \
                     (they are dropped): {:?}",
                    unknown
                );
                ROWS_LEFT_TO_SCAN.store(0, Ordering::Relaxed);
            }
        }

        let map_decode_err = |e: arrow_schema::ArrowError| {
            DataFusionError::from(streamling_user_err!(
                "script transform output does not match the output schema: {}",
                e
            ))
        };

        let mut batches = Vec::new();
        let mut offset = 0;
        while offset < bytes.len() {
            let read = self
                .decoder
                .decode(&bytes[offset..])
                .map_err(map_decode_err)?;
            offset += read;
            if let Some(batch) = self.decoder.flush().map_err(map_decode_err)? {
                batches.push(batch);
            }
            if read == 0 {
                // The decoder made no progress with bytes still on the input. Trailing
                // whitespace is fine (nothing left to decode); anything else is a malformed
                // tail that would otherwise be dropped silently.
                if bytes[offset..].iter().all(|b| b.is_ascii_whitespace()) {
                    break;
                }
                return Err(DataFusionError::from(streamling_user_err!(
                    "script transform output does not match the output schema: \
                     could not decode JSON starting at byte offset {offset}"
                )));
            }
        }
        if let Some(batch) = self.decoder.flush().map_err(map_decode_err)? {
            batches.push(batch);
        }

        let batch = if batches.is_empty() {
            RecordBatch::new_empty(self.schema.clone())
        } else {
            let decoder_schema = batches[0].schema();
            concat_batches(&decoder_schema, &batches)?
        };

        self.convert_batch_to_original_schema(batch)
    }

    fn extract_field_from(field: String, value: &Value) -> Result<Value> {
        match value.get(field.as_str()) {
            Some(v) => Ok(v.clone()),
            None => Err(streamling_user_err!("field '{}' not found in JSON object", field).into()),
        }
    }

    /// Convert a decoded batch back to the original schema: the Utf8 leaves
    /// the decoder produced become decimal_arb again, and a leaf the original
    /// schema declares as a retired `streamling.u256` / `streamling.i256`
    /// goes one step further, back to its `FixedSizeBinary(32)` wire shape
    /// through the value-checked bridge (which rejects a value the wire type
    /// cannot hold rather than truncating it). A dictionary column, decoded as
    /// its value type, is cast back to the dictionary.
    fn convert_batch_to_original_schema(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let needs_transform = self
            .schema
            .fields()
            .iter()
            .any(|f| decodes_as_other_type(f));

        if !needs_transform {
            return Ok(batch);
        }

        let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        for (idx, field) in self.schema.fields().iter().enumerate() {
            let upgraded = upgrade_legacy_wide_int_field(field).map_err(DataFusionError::from)?;
            let target = upgraded.as_ref().unwrap_or(field);
            let arr = decimal_arb_leaves_from_text(target, batch.column(idx))?;
            let arr =
                match downgrade_legacy_wide_ints(field, &arr).map_err(DataFusionError::from)? {
                    Some(downgraded) => downgraded,
                    None => arr,
                };
            let arr = if arr.data_type() == field.data_type() {
                arr
            } else {
                cast(&arr, field.data_type())?
            };
            new_columns.push(arr);
        }

        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Whether `field` decodes from JSON through a different Arrow type than it declares
/// (decimal_arb and legacy wide ints as text, a dictionary as its value type).
fn decodes_as_other_type(field: &Field) -> bool {
    field_contains_decimal_arb(field)
        || field_contains_legacy_wide_int(field)
        || matches!(field.data_type(), DataType::Dictionary(..))
}

/// Keys in the first `max_rows` NDJSON rows of `bytes` that `schema` does not declare, and
/// the number of rows scanned. `_gs_op` is runtime plumbing (`collectResults` always emits
/// it) and is never reported.
fn unknown_output_keys(
    schema: &Schema,
    bytes: &[u8],
    max_rows: usize,
) -> (BTreeSet<String>, usize) {
    let mut unknown = BTreeSet::new();
    let mut scanned = 0;
    for line in bytes.split(|&b| b == b'\n').take(max_rows) {
        scanned += 1;
        if let Ok(Value::Object(obj)) = serde_json::from_slice(line) {
            unknown.extend(
                obj.into_iter()
                    .map(|(k, _)| k)
                    .filter(|k| k != COLUMN_NAME_OP && schema.field_with_name(k).is_err()),
            );
        }
    }
    (unknown, scanned)
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
    use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue};
    use arrow_schema::{DataType, Fields};
    use datafusion::arrow::array::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use std::str::FromStr;
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

    // ------- decimal_arb JSON round-trip -------

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

    /// A row missing a declared column decodes as null. With `coerce_primitive` on, a number
    /// written into a Utf8 column parses as its string form ("42" for the integer 42).
    #[test]
    fn test_decode_ndjson() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        let mut converter = JsonToArrowConverter::try_new(schema, true, None, true).unwrap();

        let ndjson = "{\"a\":1,\"b\":42}\n{\"a\":2}\n";
        let batch = converter.decode_ndjson(ndjson.as_bytes()).unwrap();

        assert_eq!(batch.num_rows(), 2);
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

        assert_eq!(a, &Int32Array::from(vec![1, 2]));
        assert_eq!(b.value(0), "42");
        assert!(b.is_null(1));
    }

    /// Script transform output carries decimal_arb values as decimal text (the form the
    /// input writer hands the script); `decode_ndjson` must turn that text back into the
    /// declared decimal_arb column, not leave it as Utf8 or reject it.
    #[test]
    fn test_decode_ndjson_decimal_arb() {
        let big = "123456789012345678901234567890";
        let schema = Arc::new(Schema::new(vec![
            DecimalArbType::field("amount", 100, 0, true).unwrap(),
        ]));

        let mut converter =
            JsonToArrowConverter::try_new(schema.clone(), true, None, true).unwrap();
        let ndjson = format!("{{\"amount\":\"{big}\"}}\n{{\"amount\":null}}\n");
        let batch = converter.decode_ndjson(ndjson.as_bytes()).unwrap();

        assert_eq!(batch.schema(), schema);
        let amount = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(
            DecimalArbValue::from_canonical_bytes_at_scale(amount.value(0), 0).unwrap(),
            DecimalArbValue::from_str(big).unwrap()
        );
        assert!(amount.is_null(1));
    }

    /// The fallible constructor used by the script transform's `process_batch` reports an
    /// undecodable schema as an error naming the offending field, instead of panicking the
    /// process. This is the constructor that sees user-influenced schemas (a transform's
    /// `schema:` YAML, or an upstream input schema).
    #[test]
    fn test_try_new_rejects_unsupported_type_without_panicking() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ok", DataType::Int64, true),
            Field::new(
                "bad",
                DataType::Interval(arrow_schema::IntervalUnit::YearMonth),
                true,
            ),
        ]));

        let err = match JsonToArrowConverter::try_new(schema, true, None, true) {
            Ok(_) => panic!("try_new should reject an undecodable schema"),
            Err(e) => e,
        };
        let message = err.to_string();
        assert!(
            message.contains("offending fields: bad"),
            "error should name the offending field, got: {message}"
        );
    }

    /// A segment the decoder can't make progress on (a malformed trailing line) surfaces as
    /// an error instead of being silently dropped; trailing whitespace after the last line
    /// stays accepted.
    #[test]
    fn test_decode_ndjson_rejects_malformed_tail() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let mut converter =
            JsonToArrowConverter::try_new(schema.clone(), true, None, true).unwrap();

        let err = converter.decode_ndjson(b"{\"a\":1}\nnot json").unwrap_err();
        assert!(
            err.to_string().contains("does not match the output schema"),
            "unexpected error: {err}"
        );

        let mut converter = JsonToArrowConverter::try_new(schema, true, None, true).unwrap();
        let batch = converter.decode_ndjson(b"{\"a\":1}\n\n").unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_unknown_output_keys_scans_every_row() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let bytes = b"{\"a\":1,\"_gs_op\":\"i\"}\n{\"a\":2,\"extra\":3}\n{\"late\":4}";

        let (unknown, scanned) = unknown_output_keys(&schema, bytes, usize::MAX);
        assert_eq!(
            unknown,
            BTreeSet::from(["extra".to_string(), "late".to_string()])
        );
        assert_eq!(scanned, 3);

        let (unknown, scanned) = unknown_output_keys(&schema, bytes, 2);
        assert_eq!(unknown, BTreeSet::from(["extra".to_string()]));
        assert_eq!(scanned, 2);
    }

    /// A dictionary-encoded decimal_arb column carrying the extension
    /// metadata on its own field (the Arrow convention) is written as its
    /// decimal text and read back from it — not as hex of the raw bytes.
    #[test]
    fn dictionary_encoded_decimal_arb_round_trips_as_decimal_text() {
        use datafusion::arrow::compute::cast;
        let mut b = DecimalArbArrayBuilder::with_capacity(3, "amount", 30, 2).unwrap();
        b.append_str("12.34").unwrap();
        b.append_null();
        b.append_str("12.34").unwrap();
        let (raw, _, _) = b.finish().into_inner();
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeBinary));
        let dict = cast(&raw, &dict_type).unwrap();
        let field = Field::new("amount", dict_type, true).with_metadata(
            DecimalArbType::field("amount", 30, 2, true)
                .unwrap()
                .metadata()
                .clone(),
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![dict]).unwrap();

        let rows = FromArrowToJsonConverter::new()
            .convert_from_batch(&batch)
            .unwrap();
        let rows: Vec<String> = rows
            .into_iter()
            .map(|r| String::from_utf8(r).unwrap())
            .collect();
        assert!(rows[0].contains(r#""amount":"12.34""#), "{rows:?}");
        assert!(rows[2].contains(r#""amount":"12.34""#), "{rows:?}");

        let mut converter = JsonToArrowConverter::new(Arc::clone(&schema), false, None);
        converter.buffer(r#"[{"amount":"12.34"},{"amount":null},{"amount":"-0.5"}]"#.to_string());
        let read = converter.convert_to_batch().unwrap();
        assert_eq!(read.schema(), schema);
        let plain = cast(read.column(0), &DataType::LargeBinary).unwrap();
        let plain = plain.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        let values: Vec<Option<String>> = (0..plain.len())
            .map(|i| {
                (!plain.is_null(i)).then(|| {
                    DecimalArbValue::from_canonical_bytes_at_scale(plain.value(i), 2)
                        .unwrap()
                        .to_canonical_string()
                })
            })
            .collect();
        assert_eq!(
            values,
            vec![Some("12.34".to_string()), None, Some("-0.50".to_string())]
        );
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

    /// 256-bit integer leaves nested in `List<Struct<..>>` / `List<..>` (the
    /// plugin call-trace shape) serialize as their exact decimal values — the
    /// encoding the Kafka JSON, webhook and print sinks share.
    #[test]
    fn nested_wide_int_leaves_serialize_as_their_values() {
        use crate::types::decimal_arb_nested::fixtures::{
            I256_MAX, I256_MIN, U256_MAX, wide_int_traces_batch,
        };
        let rows = FromArrowToJsonConverter::new()
            .convert_from_batch(&wide_int_traces_batch())
            .unwrap();
        let rows: Vec<String> = rows
            .into_iter()
            .map(|r| String::from_utf8(r).unwrap())
            .collect();
        assert_eq!(
            rows,
            [
                r#"{"id":1,"traces":[{"value":"1"},{"value":"1000000000000000000"}],"signed":["-1","0"]}"#
                    .to_string(),
                format!(
                    r#"{{"id":2,"traces":[{{"value":"{U256_MAX}"}},{{"value":null}}],"signed":["{I256_MIN}","{I256_MAX}"]}}"#
                ),
            ]
        );
    }

    /// A run-end-encoded decimal_arb column keeps its metadata on the values
    /// field; unwrapping it used to drop that metadata, and the leaf printed
    /// as the hex of its canonical bytes. A sliced batch prints its own rows.
    #[test]
    fn run_end_encoded_and_sliced_nested_leaves_serialize_as_their_values() {
        use crate::types::decimal_arb_nested::fixtures::{U256_MAX, wide_int_traces_batch};
        use datafusion::arrow::datatypes::Int32Type;

        let values = Arc::new(DecimalArbType::field("values", 78, 0, true).unwrap());
        let mut b = DecimalArbArrayBuilder::with_capacity(2, "v", 78, 0).unwrap();
        b.append_str("1").unwrap();
        b.append_str(U256_MAX).unwrap();
        let (raw, _, _) = b.finish().into_inner();
        let ree = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1, 2]), &raw).unwrap();
        let DataType::RunEndEncoded(run_ends, _) = ree.data_type().clone() else {
            unreachable!()
        };
        let ree_type = DataType::RunEndEncoded(run_ends, values);
        let ree = make_array(
            ree.to_data()
                .into_builder()
                .data_type(ree_type.clone())
                .build()
                .unwrap(),
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("ree", ree_type, true)])),
            vec![ree],
        )
        .unwrap();
        let rows: Vec<String> = FromArrowToJsonConverter::new()
            .convert_from_batch(&batch)
            .unwrap()
            .into_iter()
            .map(|r| String::from_utf8(r).unwrap())
            .collect();
        assert_eq!(
            rows,
            [
                r#"{"ree":"1"}"#.to_string(),
                format!(r#"{{"ree":"{U256_MAX}"}}"#)
            ]
        );

        let sliced = wide_int_traces_batch().slice(1, 1);
        let rows: Vec<String> = FromArrowToJsonConverter::new()
            .convert_from_batch(&sliced)
            .unwrap()
            .into_iter()
            .map(|r| String::from_utf8(r).unwrap())
            .collect();
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0].starts_with(&format!(
                r#"{{"id":2,"traces":[{{"value":"{U256_MAX}"}},{{"value":null}}]"#
            )),
            "{}",
            rows[0]
        );
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
        // (precision, scale) = (5, 0); a 6-digit value must be rejected.
        let field = DecimalArbType::field("x", 5, 0, true).unwrap();
        let schema = Arc::new(Schema::new(vec![field]));
        let mut converter = JsonToArrowConverter::new(schema, true, None);
        converter.buffer(r#"{"x":"123456"}"#.to_string());
        let err = converter.convert_to_batch().unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("'x'"), "error must name the column: {}", msg);
    }
}
