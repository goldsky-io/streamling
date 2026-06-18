//! arrow-avro-based decode support (DataFusion 54's `arrow-avro` crate).
//!
//! arrow-avro decodes avro directly into Arrow `RecordBatch`es, but it hard-errors on
//! avro `decimal` with precision > 76 (`DECIMAL256_MAX_PRECISION`) — which is exactly how
//! streamling's blockchain `u256`/`i256` values arrive on the wire (precision 77–100).
//!
//! The lever (validated in the `arrow-avro-u256-prototype` worktree): in the Confluent/SOE
//! path we control the *writer* schema registered in arrow-avro's `SchemaStore`. If we strip
//! the high-precision `decimal` logicalType down to its underlying `bytes`, arrow-avro decodes
//! the (wire-identical) raw bytes into a `BinaryArray`; we then reinterpret those columns into
//! `FixedSizeBinary(32)` carrying the `streamling.u256`/`i256` extension metadata.
//!
//! This module provides the two pure pieces:
//!   - [`rewrite_writer_schema`] — schema JSON transform + the list of fields to reinterpret.
//!   - [`reinterpret_batch`] — post-decode column conversion.
//!
//! NOTE: currently handles *top-level* record fields (the common blockchain-record shape).
//! Nested records/lists of high-precision decimals are a follow-up.

use crate::types::i256::I256Type;
use crate::types::u256::U256Type;
use arrow::array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray};
use arrow::record_batch::RecordBatch;
use arrow_avro::reader::{Decoder, ReaderBuilder};
use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use serde_json::Value as Json;
use std::sync::Arc;

/// Arrow's `Decimal256` max precision; avro decimals above this can't be decoded natively
/// by arrow-avro and are routed to the 256-bit extension types.
const DECIMAL256_MAX_PRECISION: u64 = 76;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BigIntKind {
    U256,
    I256,
}

/// A top-level field that arrow-avro will decode as `bytes` (because we stripped its
/// high-precision `decimal` logicalType) and that must be reinterpreted afterward.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalOverride {
    pub field_index: usize,
    pub kind: BigIntKind,
}

/// Rewrite a writer avro schema (JSON) so arrow-avro can decode it: any top-level field
/// whose type is a `decimal` with precision > 76 is downgraded to its underlying `bytes`.
/// Returns the rewritten JSON (to register in the `SchemaStore`) and the fields to
/// reinterpret post-decode.
///
/// Avro `bytes` and `decimal`-on-`bytes` share an identical wire encoding, so decoding the
/// same bytes with the downgraded schema is lossless.
pub fn rewrite_writer_schema(writer_json: &str) -> Result<(String, Vec<DecimalOverride>)> {
    let mut root: Json = serde_json::from_str(writer_json)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut overrides = Vec::new();

    if let Some(fields) = root.get_mut("fields").and_then(Json::as_array_mut) {
        for (idx, field) in fields.iter_mut().enumerate() {
            if let Some(ty) = field.get_mut("type") {
                if let Some(kind) = high_precision_decimal_kind(ty) {
                    downgrade_decimal_to_bytes(ty);
                    overrides.push(DecimalOverride {
                        field_index: idx,
                        kind,
                    });
                }
            }
        }
    }

    let json = serde_json::to_string(&root).map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok((json, overrides))
}

/// Detect a `decimal` logicalType with precision > 76 in a field's `type` (handling a
/// nullable `["null", {decimal}]` union). Returns the 256-bit kind to produce, or `None`.
fn high_precision_decimal_kind(ty: &Json) -> Option<BigIntKind> {
    match ty {
        Json::Object(_) => decimal_kind_from_obj(ty),
        // Nullable union: ["null", {decimal...}] (or the decimal first)
        Json::Array(variants) => variants.iter().find_map(decimal_kind_from_obj),
        _ => None,
    }
}

fn decimal_kind_from_obj(ty: &Json) -> Option<BigIntKind> {
    let obj = ty.as_object()?;
    if obj.get("logicalType").and_then(Json::as_str) != Some("decimal") {
        return None;
    }
    let precision = obj.get("precision").and_then(Json::as_u64)?;
    if precision <= DECIMAL256_MAX_PRECISION {
        return None;
    }
    // Matches `convert_avro_schema_to_arrow`: precision > 76 with scale 0 is treated as an
    // unsigned 256-bit integer (blockchain `uint256`). (Signed/scaled high-precision decimals
    // are a separate path and not rewritten here.)
    let scale = obj.get("scale").and_then(Json::as_u64).unwrap_or(0);
    if scale == 0 {
        Some(BigIntKind::U256)
    } else {
        None
    }
}

/// Replace a `decimal` type (possibly inside a nullable union) with its underlying `bytes`.
fn downgrade_decimal_to_bytes(ty: &mut Json) {
    match ty {
        Json::Object(_) => {
            if decimal_kind_from_obj(ty).is_some() {
                *ty = Json::String("bytes".to_string());
            }
        }
        Json::Array(variants) => {
            for v in variants.iter_mut() {
                if decimal_kind_from_obj(v).is_some() {
                    *v = Json::String("bytes".to_string());
                }
            }
        }
        _ => {}
    }
}

/// Reinterpret the columns flagged by [`rewrite_writer_schema`]: arrow-avro decoded them as
/// `Binary` (raw big-endian two's-complement avro decimal bytes); convert each to
/// `FixedSizeBinary(32)` carrying the u256/i256 extension metadata.
pub fn reinterpret_batch(batch: RecordBatch, overrides: &[DecimalOverride]) -> Result<RecordBatch> {
    if overrides.is_empty() {
        return Ok(batch);
    }
    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();

    for ov in overrides {
        let col = &columns[ov.field_index];
        let bin = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "arrow-avro reinterpret: expected BinaryArray at field {}, got {:?}",
                    ov.field_index,
                    col.data_type()
                ))
            })?;

        let converted = convert_binary_to_256(bin, ov.kind)?;
        let name = schema.field(ov.field_index).name();
        let nullable = schema.field(ov.field_index).is_nullable();
        let (data_type, metadata) = match ov.kind {
            BigIntKind::U256 => (U256Type::new(), U256Type::metadata()),
            BigIntKind::I256 => (I256Type::new(), I256Type::metadata()),
        };
        columns[ov.field_index] = Arc::new(converted);
        fields[ov.field_index] =
            Arc::new(Field::new(name, data_type, nullable).with_metadata(metadata));
    }

    let new_schema: SchemaRef = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(new_schema, columns).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn convert_binary_to_256(bin: &BinaryArray, kind: BigIntKind) -> Result<FixedSizeBinaryArray> {
    let mut out: Vec<Option<[u8; 32]>> = Vec::with_capacity(bin.len());
    for i in 0..bin.len() {
        if bin.is_null(i) {
            out.push(None);
        } else {
            let bytes = bin.value(i);
            let v = match kind {
                BigIntKind::U256 => u256_be_bytes(bytes)?,
                BigIntKind::I256 => i256_be_bytes(bytes)?,
            };
            out.push(Some(v));
        }
    }
    FixedSizeBinaryArray::try_from_sparse_iter_with_size(out.into_iter(), 32)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Big-endian two's-complement avro decimal bytes → 32-byte big-endian u256 (zero-extended).
/// Rejects negative values. (Extracted from `arrow_array_reader::resolve_u256`.)
pub fn u256_be_bytes(bytes: &[u8]) -> Result<[u8; 32]> {
    if !bytes.is_empty() && (bytes[0] & 0x80) != 0 {
        return Err(DataFusionError::Internal(
            "Failed to convert negative decimal to U256 - negative values not supported".to_string(),
        ));
    }
    let mut bytes = bytes.to_vec();
    while bytes.len() > 32 && bytes[0] == 0x00 {
        bytes.remove(0);
    }
    if bytes.len() > 32 {
        return Err(DataFusionError::Internal(format!(
            "Failed to convert decimal to U256 - data too large ({} bytes, max 32)",
            bytes.len()
        )));
    }
    let mut result = [0u8; 32];
    result[32 - bytes.len()..].copy_from_slice(&bytes);
    Ok(result)
}

/// Big-endian two's-complement avro decimal bytes → 32-byte big-endian i256 (sign-extended).
/// (Extracted from `arrow_array_reader::resolve_i256`.)
pub fn i256_be_bytes(bytes: &[u8]) -> Result<[u8; 32]> {
    let mut bytes = bytes.to_vec();
    let negative = !bytes.is_empty() && (bytes[0] & 0x80) != 0;
    let padding_byte = if negative { 0xFF } else { 0x00 };
    while bytes.len() > 32 && bytes[0] == padding_byte {
        if bytes.len() > 1 && ((bytes[1] & 0x80) != 0) == negative {
            bytes.remove(0);
        } else {
            break;
        }
    }
    if bytes.len() > 32 {
        return Err(DataFusionError::Internal(format!(
            "Failed to convert decimal to I256 - data too large ({} bytes, max 32)",
            bytes.len()
        )));
    }
    let mut result = [padding_byte; 32];
    result[32 - bytes.len()..].copy_from_slice(&bytes);
    Ok(result)
}

/// A Confluent-framed avro → Arrow decoder built on arrow-avro, with streamling's u256/i256
/// reinterpret applied to each flushed batch.
///
/// Writer schemas are registered by their Confluent registry id (the high-precision `decimal`
/// logicalType is stripped before registration so arrow-avro can decode them — see
/// [`rewrite_writer_schema`]). An optional reader schema drives schema resolution and the output
/// column layout; the u256/i256 reinterpret overrides are taken from the reader schema, or — when
/// no reader schema is set — from the first registered writer schema.
///
/// This is the reusable decode core the Kafka source plugs into (replacing the
/// `schema_registry_converter` + `AvroArrowArrayReader` path).
pub struct ConfluentAvroDecoder {
    store: SchemaStore,
    reader_schema_json: Option<String>,
    overrides: Vec<DecimalOverride>,
    overrides_from_reader: bool,
    decoder: Option<Decoder>,
}

impl Default for ConfluentAvroDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfluentAvroDecoder {
    pub fn new() -> Self {
        Self {
            store: SchemaStore::new_with_type(FingerprintAlgorithm::Id),
            reader_schema_json: None,
            overrides: Vec::new(),
            overrides_from_reader: false,
            decoder: None,
        }
    }

    /// Set the reader schema (the topic's current schema). The reinterpret overrides are computed
    /// from it, and the rewritten reader schema drives arrow-avro's schema resolution.
    pub fn with_reader_schema(mut self, reader_json: &str) -> Result<Self> {
        let (rewritten, overrides) = rewrite_writer_schema(reader_json)?;
        self.reader_schema_json = Some(rewritten);
        self.overrides = overrides;
        self.overrides_from_reader = true;
        self.decoder = None;
        Ok(self)
    }

    /// Register a writer schema under its Confluent registry id (high-precision decimals stripped).
    pub fn register_writer_schema(&mut self, id: u32, writer_json: &str) -> Result<()> {
        let (rewritten, overrides) = rewrite_writer_schema(writer_json)?;
        if !self.overrides_from_reader && self.overrides.is_empty() {
            self.overrides = overrides;
        }
        self.store
            .set(Fingerprint::Id(id), AvroSchema::new(rewritten))
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        self.decoder = None; // force rebuild so the new schema is visible
        Ok(())
    }

    fn ensure_decoder(&mut self) -> Result<&mut Decoder> {
        if self.decoder.is_none() {
            let mut builder = ReaderBuilder::new().with_writer_schema_store(self.store.clone());
            if let Some(js) = &self.reader_schema_json {
                builder = builder.with_reader_schema(AvroSchema::new(js.clone()));
            }
            self.decoder = Some(
                builder
                    .build_decoder()
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            );
        }
        Ok(self.decoder.as_mut().unwrap())
    }

    /// Decode one Confluent-framed message: `0x00` + 4-byte big-endian schema id + avro body.
    pub fn decode(&mut self, framed: &[u8]) -> Result<usize> {
        self.ensure_decoder()?
            .decode(framed)
            .map_err(|e| DataFusionError::Internal(format!("arrow-avro decode failed: {e}")))
    }

    /// Flush accumulated rows into a `RecordBatch`, reinterpreting u256/i256 columns.
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        let overrides = self.overrides.clone();
        let batch = self
            .ensure_decoder()?
            .flush()
            .map_err(|e| DataFusionError::Internal(format!("arrow-avro flush failed: {e}")))?;
        match batch {
            Some(b) => Ok(Some(reinterpret_batch(b, &overrides)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::{Record, Value};
    use apache_avro::{to_avro_datum, Decimal, Schema as AvroWriterSchema};
    use arrow_avro::reader::ReaderBuilder;
    use arrow_avro::schema::{AvroSchema, Fingerprint, SchemaStore};

    const DECIMAL_SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}]}"#;

    #[test]
    fn rewrite_strips_high_precision_decimal() {
        let (json, overrides) = rewrite_writer_schema(DECIMAL_SCHEMA).unwrap();
        assert_eq!(
            overrides,
            vec![DecimalOverride {
                field_index: 0,
                kind: BigIntKind::U256
            }]
        );
        // The decimal logicalType is gone; field is plain bytes.
        assert!(!json.contains("decimal"), "decimal logicalType not stripped: {json}");
        assert!(json.contains(r#""type":"bytes""#) || json.contains(r#""bytes""#));
    }

    #[test]
    fn end_to_end_u256_decode_via_arrow_avro() {
        // 32-byte payload, the value a u256 would carry.
        let mut payload = [0u8; 32];
        payload[0] = 0x12;
        payload[1] = 0x34;
        payload[30] = 0xAB;
        payload[31] = 0xCD;

        // Body encoded with the REAL decimal schema — the actual on-wire bytes.
        let decimal_schema = AvroWriterSchema::parse_str(DECIMAL_SCHEMA).unwrap();
        let mut rec = Record::new(&decimal_schema).unwrap();
        rec.put("v", Value::Decimal(Decimal::from(payload.to_vec())));
        let body = to_avro_datum(&decimal_schema, rec).unwrap();

        // Register the REWRITTEN (bytes) schema and SOE-frame the body.
        let (rewritten_json, overrides) = rewrite_writer_schema(DECIMAL_SCHEMA).unwrap();
        let mut store = SchemaStore::new();
        let fp = store.register(AvroSchema::new(rewritten_json)).unwrap();
        let rabin = match fp {
            Fingerprint::Rabin(r) => r,
            other => panic!("unexpected fingerprint: {other:?}"),
        };
        let mut framed = vec![0xC3, 0x01];
        framed.extend_from_slice(&rabin.to_le_bytes());
        framed.extend_from_slice(&body);

        let mut decoder = ReaderBuilder::new()
            .with_writer_schema_store(store)
            .build_decoder()
            .unwrap();
        decoder.decode(&framed).unwrap();
        let batch = decoder.flush().unwrap().expect("a batch");

        // Reinterpret the bytes column into u256.
        let batch = reinterpret_batch(batch, &overrides).unwrap();

        let field = batch.schema().field(0).clone();
        assert!(U256Type::is_u256_field(&field), "field not tagged u256: {field:?}");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("FixedSizeBinary(32)");
        assert_eq!(col.value_length(), 32);
        assert_eq!(col.value(0), &payload, "u256 bytes round-trip");
    }

    #[test]
    fn confluent_decoder_u256_end_to_end() {
        // Top byte high bit must be clear (positive two's-complement) for a valid u256.
        let mut payload = [0u8; 32];
        payload[0] = 0x12;
        payload[15] = 0x55;
        payload[31] = 0xBB;

        let decimal_schema = AvroWriterSchema::parse_str(DECIMAL_SCHEMA).unwrap();
        let mut rec = Record::new(&decimal_schema).unwrap();
        rec.put("v", Value::Decimal(Decimal::from(payload.to_vec())));
        let body = to_avro_datum(&decimal_schema, rec).unwrap();

        // Register the writer schema under a Confluent registry id.
        let schema_id: u32 = 42;
        let mut decoder = ConfluentAvroDecoder::new();
        decoder
            .register_writer_schema(schema_id, DECIMAL_SCHEMA)
            .unwrap();

        // Confluent wire format: 0x00 + 4-byte big-endian schema id + avro body.
        let mut framed = vec![0x00];
        framed.extend_from_slice(&schema_id.to_be_bytes());
        framed.extend_from_slice(&body);

        decoder.decode(&framed).unwrap();
        let batch = decoder.flush().unwrap().expect("a batch");

        assert!(U256Type::is_u256_field(&batch.schema().field(0).clone()));
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("FixedSizeBinary(32)");
        assert_eq!(col.value(0), &payload, "u256 round-trips through Confluent decode");
    }
}
