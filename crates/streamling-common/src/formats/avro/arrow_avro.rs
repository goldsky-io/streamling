//! arrow-avro-based decode support (DataFusion 54's `arrow-avro` crate).
//!
//! arrow-avro decodes avro directly into Arrow `RecordBatch`es, but its native avro→Arrow type
//! mapping differs from streamling's vendored mapping (`convert_avro_schema_to_arrow` +
//! `AvroArrowArrayReader`) in several ways — most importantly it hard-errors on avro `decimal`
//! with precision > 76 (`DECIMAL256_MAX_PRECISION`), which is exactly how streamling's blockchain
//! `u256`/`i256` values arrive on the wire (precision 77–100), and it maps nested high-precision
//! decimals, enums, maps, etc. to different Arrow types than streamling does.
//!
//! To make arrow-avro a faithful drop-in for the existing decode path, we:
//!   1. [`rewrite_writer_schema`] — recursively strip every high-precision `decimal` logicalType
//!      down to its underlying `bytes`/`fixed` (wire-identical) so arrow-avro can build a decoder.
//!   2. [`coerce_batch_to_target`] — recursively coerce arrow-avro's decoded batch so its column
//!      data types exactly match streamling's *target* Arrow schema (the one
//!      `convert_avro_schema_to_arrow` produces and the rest of the pipeline expects). This is
//!      what reinterprets the downgraded `Binary` columns back into `FixedSizeBinary(32)` u256/i256
//!      (top-level) or `Decimal128(p,0)` (nested), rebuilds `List`/`Struct` columns to match the
//!      target field names/types, and casts anything else (e.g. enum dictionaries → `Utf8`).
//!
//! Schemas using avro named-type references (`AvroSchema::Ref`) are NOT supported — streamling's
//! `convert_avro_schema_to_arrow` itself `todo!()`s on them, so there's no target to coerce to.

use crate::types::i256::I256Type;
use crate::types::u256::U256Type;
use apache_avro::Schema as ApacheAvroSchema;
use arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray, ListArray,
    PrimitiveArray, StructArray, new_null_array,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{Decimal128Type, Decimal256Type, i256};
use arrow::record_batch::RecordBatch;
use arrow_avro::reader::{Decoder, ReaderBuilder};
use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};
use arrow_schema::{DataType, Field, FieldRef, Fields, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use serde_json::Value as Json;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use super::convert_avro_schema_to_arrow;

/// Arrow's `Decimal256` max precision; avro decimals above this can't be decoded natively
/// by arrow-avro, so we strip the logicalType and reinterpret the raw bytes afterward.
const DECIMAL256_MAX_PRECISION: u64 = 76;

fn arrow_err(e: arrow_schema::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

// ---------------------------------------------------------------------------
// Schema rewrite: strip high-precision decimals so arrow-avro accepts the schema.
// ---------------------------------------------------------------------------

/// Recursively rewrite an avro schema (JSON) so arrow-avro can build a decoder for it: every
/// `decimal` logicalType with precision > 76 (which arrow-avro rejects) has its decimal
/// logicalType/precision/scale removed, leaving the plain underlying `bytes`/`fixed`. Avro
/// `bytes` and `decimal`-on-`bytes` share an identical wire encoding, so decoding the same bytes
/// with the downgraded schema is lossless; [`coerce_batch_to_target`] reinterprets the decoded
/// `Binary` columns afterward.
pub fn rewrite_writer_schema(writer_json: &str) -> Result<String> {
    let mut root: Json =
        serde_json::from_str(writer_json).map_err(|e| DataFusionError::External(Box::new(e)))?;
    strip_high_precision_decimals(&mut root);
    serde_json::to_string(&root).map_err(|e| DataFusionError::External(Box::new(e)))
}

fn strip_high_precision_decimals(node: &mut Json) {
    match node {
        Json::Object(map) => {
            let is_high_precision_decimal = map.get("logicalType").and_then(Json::as_str)
                == Some("decimal")
                && map
                    .get("precision")
                    .and_then(Json::as_u64)
                    .is_some_and(|p| p > DECIMAL256_MAX_PRECISION);
            if is_high_precision_decimal {
                // Drop the decimal logicalType; the underlying `type` (bytes/fixed) stays and is
                // wire-identical, so arrow-avro decodes the raw bytes.
                map.remove("logicalType");
                map.remove("precision");
                map.remove("scale");
            }
            // Recurse into the type holders an avro schema object can carry.
            if let Some(t) = map.get_mut("type") {
                strip_high_precision_decimals(t);
            }
            if let Some(items) = map.get_mut("items") {
                strip_high_precision_decimals(items);
            }
            if let Some(values) = map.get_mut("values") {
                strip_high_precision_decimals(values);
            }
            if let Some(Json::Array(fields)) = map.get_mut("fields") {
                for f in fields.iter_mut() {
                    if let Some(ft) = f.get_mut("type") {
                        strip_high_precision_decimals(ft);
                    }
                }
            }
        }
        Json::Array(variants) => {
            for v in variants.iter_mut() {
                strip_high_precision_decimals(v);
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Batch coercion: arrow-avro's batch -> streamling's target Arrow schema.
// ---------------------------------------------------------------------------

/// Coerce arrow-avro's decoded `batch` so its columns exactly match `target` (the schema
/// `convert_avro_schema_to_arrow` produced). Columns are matched to target fields by name and
/// coerced recursively; the returned batch carries `target` as its schema.
pub fn coerce_batch_to_target(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let src_schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for tf in target.fields() {
        match src_schema.index_of(tf.name()) {
            Ok(i) => columns.push(coerce_array(batch.column(i), tf)?),
            // Field absent from the decoded batch. With resolution enabled this cannot happen
            // (arrow-avro produces every target field); it only arises under `skip_schema_resolution`,
            // where we decode against the writer schema. Mirror the vendored reader: a nullable target
            // field becomes an all-null column; a required one is a genuine error.
            Err(_) if tf.is_nullable() => {
                columns.push(new_null_array(tf.data_type(), batch.num_rows()));
            }
            Err(_) => {
                return Err(DataFusionError::Internal(format!(
                    "decoded avro batch is missing required target field '{}' (have: {:?})",
                    tf.name(),
                    src_schema
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                )));
            }
        }
    }
    RecordBatch::try_new(target.clone(), columns).map_err(arrow_err)
}

/// Coerce a single decoded array to `target`'s data type, recursing into list/struct children.
fn coerce_array(src: &ArrayRef, target: &Field) -> Result<ArrayRef> {
    match target.data_type() {
        // u256 / i256: arrow-avro decoded the (downgraded) high-precision decimal as raw bytes.
        DataType::FixedSizeBinary(32) if U256Type::is_u256_metadata(target.metadata()) => {
            binary_to_fixed256(src, BigIntKind::U256)
        }
        DataType::FixedSizeBinary(32) if I256Type::is_i256_metadata(target.metadata()) => {
            binary_to_fixed256(src, BigIntKind::I256)
        }
        DataType::Decimal128(p, s) => binary_or_passthrough_decimal128(src, *p, *s),
        DataType::Decimal256(p, s) => binary_or_passthrough_decimal256(src, *p, *s),
        DataType::List(child) => coerce_list(src, child),
        DataType::Struct(fields) => coerce_struct(src, fields),
        // Anything else: identical types pass through, otherwise lean on arrow's cast kernel
        // (handles enum Dictionary→Utf8, timestamp tz adjustments, integer widening, etc.).
        tdt if src.data_type() == tdt => Ok(src.clone()),
        tdt => cast(src, tdt).map_err(arrow_err),
    }
}

fn coerce_list(src: &ArrayRef, target_child: &FieldRef) -> Result<ArrayRef> {
    let (offsets, values, nulls) = match src.data_type() {
        DataType::List(_) => {
            let la = src
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("List downcast");
            (
                la.offsets().clone(),
                la.values().clone(),
                la.nulls().cloned(),
            )
        }
        DataType::LargeList(_) => {
            let la = src
                .as_any()
                .downcast_ref::<arrow::array::LargeListArray>()
                .expect("LargeList downcast");
            // Narrow i64 offsets to i32 for the target `List` type.
            let off: Vec<i32> = la.offsets().iter().map(|&o| o as i32).collect();
            (
                OffsetBuffer::new(ScalarBuffer::from(off)),
                la.values().clone(),
                la.nulls().cloned(),
            )
        }
        other => {
            return Err(DataFusionError::Internal(format!(
                "arrow-avro coerce_list: expected List/LargeList, got {other:?}"
            )));
        }
    };
    let coerced_values = coerce_array(&values, target_child)?;
    let list = ListArray::try_new(target_child.clone(), offsets, coerced_values, nulls)
        .map_err(arrow_err)?;
    Ok(Arc::new(list))
}

fn coerce_struct(src: &ArrayRef, target_fields: &Fields) -> Result<ArrayRef> {
    let sa = src.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "arrow-avro coerce_struct: expected Struct, got {:?}",
            src.data_type()
        ))
    })?;
    let mut children: Vec<ArrayRef> = Vec::with_capacity(target_fields.len());
    for tf in target_fields {
        match sa.column_by_name(tf.name()) {
            Some(col) => children.push(coerce_array(col, tf)?),
            // Field absent from the decoded struct. Mirror `coerce_batch_to_target`'s top-level
            // handling (and the vendored reader): a nullable target field becomes an all-null
            // column; a required one is a genuine error.
            None if tf.is_nullable() => {
                children.push(new_null_array(tf.data_type(), sa.len()));
            }
            None => {
                return Err(DataFusionError::Internal(format!(
                    "arrow-avro coerce_struct: missing required nested field '{}'",
                    tf.name()
                )));
            }
        }
    }
    let struct_arr = StructArray::try_new(target_fields.clone(), children, sa.nulls().cloned())
        .map_err(arrow_err)?;
    Ok(Arc::new(struct_arr))
}

// ---------------------------------------------------------------------------
// Leaf conversions: raw avro decimal bytes -> streamling number types.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BigIntKind {
    U256,
    I256,
}

fn as_binary_iter(src: &ArrayRef) -> Result<Vec<Option<Vec<u8>>>> {
    if let Some(b) = src.as_any().downcast_ref::<BinaryArray>() {
        Ok((0..b.len())
            .map(|i| (!b.is_null(i)).then(|| b.value(i).to_vec()))
            .collect())
    } else if let Some(b) = src.as_any().downcast_ref::<LargeBinaryArray>() {
        Ok((0..b.len())
            .map(|i| (!b.is_null(i)).then(|| b.value(i).to_vec()))
            .collect())
    } else {
        Err(DataFusionError::Internal(format!(
            "arrow-avro: expected Binary for high-precision decimal column, got {:?}",
            src.data_type()
        )))
    }
}

fn binary_to_fixed256(src: &ArrayRef, kind: BigIntKind) -> Result<ArrayRef> {
    let rows = as_binary_iter(src)?;
    let out: Vec<Option<[u8; 32]>> = rows
        .iter()
        .map(|b| match b {
            None => Ok(None),
            Some(bytes) => match kind {
                BigIntKind::U256 => u256_be_bytes(bytes).map(Some),
                BigIntKind::I256 => i256_be_bytes(bytes).map(Some),
            },
        })
        .collect::<Result<_>>()?;
    let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(out.into_iter(), 32)
        .map_err(arrow_err)?;
    Ok(Arc::new(arr))
}

fn binary_or_passthrough_decimal128(src: &ArrayRef, p: u8, s: i8) -> Result<ArrayRef> {
    let dt = DataType::Decimal128(p, s);
    match src.data_type() {
        DataType::Binary | DataType::LargeBinary => {
            let rows = as_binary_iter(src)?;
            let arr: PrimitiveArray<Decimal128Type> = rows
                .iter()
                .map(|b| b.as_ref().map(|bytes| be_bytes_to_i128(bytes)))
                .collect();
            Ok(Arc::new(arr.with_data_type(dt)))
        }
        DataType::Decimal128(_, _) => {
            let a = src
                .as_any()
                .downcast_ref::<PrimitiveArray<Decimal128Type>>()
                .expect("Decimal128 downcast")
                .clone()
                .with_data_type(dt);
            Ok(Arc::new(a))
        }
        _ => cast(src, &dt).map_err(arrow_err),
    }
}

fn binary_or_passthrough_decimal256(src: &ArrayRef, p: u8, s: i8) -> Result<ArrayRef> {
    let dt = DataType::Decimal256(p, s);
    match src.data_type() {
        DataType::Binary | DataType::LargeBinary => {
            let rows = as_binary_iter(src)?;
            let arr: PrimitiveArray<Decimal256Type> = rows
                .iter()
                .map(|b| b.as_ref().map(|bytes| be_bytes_to_i256(bytes)))
                .collect();
            Ok(Arc::new(arr.with_data_type(dt)))
        }
        DataType::Decimal256(_, _) => {
            let a = src
                .as_any()
                .downcast_ref::<PrimitiveArray<Decimal256Type>>()
                .expect("Decimal256 downcast")
                .clone()
                .with_data_type(dt);
            Ok(Arc::new(a))
        }
        _ => cast(src, &dt).map_err(arrow_err),
    }
}

/// Big-endian two's-complement avro decimal bytes → sign-extended `i128`. Mirrors the vendored
/// `arrow_array_reader::resolve_decimal`. For inputs longer than 16 bytes the low 16 bytes are
/// kept (the vendored reader only ever saw ≤16-byte nested values).
fn be_bytes_to_i128(bytes: &[u8]) -> i128 {
    let negative = !bytes.is_empty() && (bytes[0] & 0x80) != 0;
    let fill = if negative { 0xFFu8 } else { 0x00 };
    let mut ext = [fill; 16];
    let n = bytes.len().min(16);
    ext[16 - n..].copy_from_slice(&bytes[bytes.len() - n..]);
    i128::from_be_bytes(ext)
}

/// Big-endian two's-complement avro decimal bytes → sign-extended `i256`.
fn be_bytes_to_i256(bytes: &[u8]) -> i256 {
    let negative = !bytes.is_empty() && (bytes[0] & 0x80) != 0;
    let fill = if negative { 0xFFu8 } else { 0x00 };
    let mut ext = [fill; 32];
    let n = bytes.len().min(32);
    ext[32 - n..].copy_from_slice(&bytes[bytes.len() - n..]);
    i256::from_be_bytes(ext)
}

/// Big-endian two's-complement avro decimal bytes → 32-byte big-endian u256 (zero-extended).
/// Rejects negative values. (Extracted from `arrow_array_reader::resolve_u256`.)
pub fn u256_be_bytes(bytes: &[u8]) -> Result<[u8; 32]> {
    if !bytes.is_empty() && (bytes[0] & 0x80) != 0 {
        return Err(DataFusionError::Internal(
            "Failed to convert negative decimal to U256 - negative values not supported"
                .to_string(),
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

// ---------------------------------------------------------------------------
// ConfluentAvroDecoder: the reusable decode core the Kafka source plugs into.
// ---------------------------------------------------------------------------

/// A Confluent-framed avro → Arrow decoder built on arrow-avro, producing batches whose schema
/// matches streamling's vendored `convert_avro_schema_to_arrow` mapping.
///
/// Writer schemas are registered by their Confluent registry id (high-precision `decimal`
/// logicalTypes are stripped before registration so arrow-avro can decode them — see
/// [`rewrite_writer_schema`]). An optional reader schema drives arrow-avro's schema resolution and
/// supplies the target Arrow schema; when no reader schema is set, the target is derived from the
/// first registered writer schema.
///
/// A single arrow-avro `Decoder` cannot accumulate rows from more than one writer schema into one
/// batch (mixing writer ids silently drops rows). So when the incoming writer id changes, we
/// finalize the current arrow `Decoder` into a per-generation batch (coerced to the target schema)
/// and rebuild; [`flush`](Self::flush) concatenates all generations. Heterogeneous-writer batches
/// (the Confluent schema-evolution case) are therefore handled transparently.
pub struct ConfluentAvroDecoder {
    store: SchemaStore,
    reader_schema_json: Option<String>,
    target_schema: Option<SchemaRef>,
    /// When a writer schema's root is a `["null", record]`-style union (the Debezium/Confluent
    /// convention), arrow-avro can't build a decoder for the union root, so we register the
    /// unwrapped record and strip the leading union-branch varint from each body. This maps each
    /// union-rooted writer id to its record-branch index (what the wire prefix must equal).
    /// Keyed per writer id because a single subject can carry writer schemas with differing root
    /// framing (union-wrapped vs plain) across schema evolution; a global index would apply the
    /// last-registered framing to every id and corrupt the others.
    union_record_indices: HashMap<u32, i64>,
    /// The writer id the live `decoder` is currently accumulating (its "generation"). `None` when
    /// no decoder is live (start, or just after a flush). Invariant: `active_writer_id.is_some()`
    /// iff a decoder exists and is bound to that single writer schema.
    active_writer_id: Option<u32>,
    /// Finalized per-generation batches (each from a single writer schema, coerced to the target),
    /// concatenated by `flush`.
    pending: Vec<RecordBatch>,
    decoder: Option<Decoder>,
    /// The reader record's Avro full name (set by `with_reader_schema`), used to detect a writer
    /// whose top-level record name differs from the reader's.
    reader_full_name: Option<String>,
    /// Full names of registered writer records that differ from the reader's. These are injected as
    /// aliases on the reader schema before building the arrow-avro decoder: arrow-avro performs
    /// spec-strict record-name resolution and rejects a writer/reader name mismatch, whereas the
    /// vendored reader matched fields positionally and ignored the record name. Producers that
    /// rename the top-level record (e.g. a transform / schema-compat output named differently from
    /// the topic's writer schema) are common, so we preserve the old lenient behavior via aliases.
    ///
    /// LIMITATION: only the *top-level* record name is aliased. arrow-avro also name-checks every
    /// *nested* named type (`resolve_records`/`resolve_enums`/`resolve_fixed`) using that type's own
    /// aliases, so a differing NESTED record/enum/fixed name still errors. And the alias can't
    /// express a bare (namespace-less) writer name under a namespaced reader — arrow-avro re-qualifies
    /// a bare alias with the reader's namespace. Neither case occurs for today's namespace-less,
    /// top-level-renamed schemas. Tracked in STRM-6359.
    writer_aliases: BTreeSet<String>,
    /// Whether to drive arrow-avro's writer→reader schema *resolution* with the reader schema. When
    /// `false` (the pipeline set `skip_schema_resolution`), the decoder is built with only the writer
    /// schema store, so arrow-avro decodes each message against its own writer schema with no
    /// resolution — no field reordering, default-filling, or name-matching. The decoded batch is
    /// still coerced to `target_schema` (by field name) afterward. This mirrors the vendored path,
    /// where `skip_schema_resolution` fed the raw writer value straight to the converter.
    resolve_against_reader: bool,
}

/// Strip high-precision decimals, then if the root is a union, unwrap it to its record branch.
/// Returns the rewritten record-root JSON (for arrow-avro) and the record branch's union index
/// (so the wire union prefix can be stripped on decode), or `None` if the root wasn't a union.
fn prepare_arrow_avro_schema(json: &str) -> Result<(String, Option<i64>)> {
    let mut root: Json =
        serde_json::from_str(json).map_err(|e| DataFusionError::External(Box::new(e)))?;
    strip_high_precision_decimals(&mut root);
    if let Json::Array(variants) = &root {
        let idx = variants
            .iter()
            .position(|v| v.get("type").and_then(Json::as_str) == Some("record"))
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "top-level avro union has no record branch (unsupported)".into(),
                )
            })?;
        let record_json = serde_json::to_string(&variants[idx])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        return Ok((record_json, Some(idx as i64)));
    }
    let record_json =
        serde_json::to_string(&root).map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok((record_json, None))
}

/// The Avro full name (`namespace.name`, or bare `name`) of a record-root schema JSON. Returns
/// `None` if the JSON isn't an object with a string `name`. A `name` that already contains a dot is
/// itself a full name (the namespace attribute is ignored), matching arrow-avro's `make_full_name`.
fn record_full_name(record_json: &str) -> Option<String> {
    let v: Json = serde_json::from_str(record_json).ok()?;
    let name = v.get("name")?.as_str()?;
    if name.contains('.') {
        return Some(name.to_string());
    }
    match v.get("namespace").and_then(Json::as_str) {
        Some(ns) if !ns.is_empty() => Some(format!("{ns}.{name}")),
        _ => Some(name.to_string()),
    }
}

/// Decode a zigzag avro `long` from the front of `buf`; returns `(value, bytes_consumed)`.
fn read_avro_long(buf: &[u8]) -> Result<(i64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut i = 0usize;
    loop {
        let b = *buf
            .get(i)
            .ok_or_else(|| DataFusionError::Internal("truncated avro long".into()))?;
        value |= ((b & 0x7F) as u64) << shift;
        i += 1;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(DataFusionError::Internal("avro long overflow".into()));
        }
    }
    let decoded = ((value >> 1) as i64) ^ -((value & 1) as i64);
    Ok((decoded, i))
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
            target_schema: None,
            union_record_indices: HashMap::new(),
            active_writer_id: None,
            pending: Vec::new(),
            decoder: None,
            reader_full_name: None,
            writer_aliases: BTreeSet::new(),
            resolve_against_reader: true,
        }
    }

    /// Enable or disable arrow-avro writer→reader schema resolution (default: enabled). Pass `false`
    /// to honor a pipeline's `skip_schema_resolution`: the decoder then decodes each message against
    /// its writer schema with no resolution, and the batch is coerced to the target by field name.
    pub fn with_schema_resolution(mut self, enabled: bool) -> Self {
        self.resolve_against_reader = enabled;
        self.decoder = None;
        self
    }

    /// Set the reader schema (the topic's current schema): its rewritten form drives arrow-avro's
    /// schema resolution, and `convert_avro_schema_to_arrow` of it is the target the decoded
    /// batches are coerced to.
    pub fn with_reader_schema(mut self, reader: &ApacheAvroSchema) -> Result<Self> {
        let reader_json =
            serde_json::to_string(reader).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let (record_json, _) = prepare_arrow_avro_schema(&reader_json)?;
        self.reader_full_name = record_full_name(&record_json);
        self.reader_schema_json = Some(record_json);
        self.target_schema = Some(convert_avro_schema_to_arrow(reader.clone()));
        self.decoder = None;
        Ok(self)
    }

    /// Register a writer schema under its Confluent registry id (high-precision decimals stripped).
    /// If no target schema has been established yet, it is derived from this writer schema.
    pub fn register_writer_schema(&mut self, id: u32, writer_json: &str) -> Result<()> {
        let (record_json, union_idx) = prepare_arrow_avro_schema(writer_json)?;
        if self.target_schema.is_none() {
            let parsed = ApacheAvroSchema::parse_str(writer_json)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            self.target_schema = Some(convert_avro_schema_to_arrow(parsed));
        }
        // If this writer's top-level record name differs from the reader's, remember it so the
        // reader schema can claim it as an alias when the decoder is (re)built — otherwise
        // arrow-avro's spec-strict name resolution rejects the mismatch (see `writer_aliases`).
        if let (Some(reader_name), Some(writer_name)) =
            (&self.reader_full_name, record_full_name(&record_json))
            && &writer_name != reader_name
        {
            self.writer_aliases.insert(writer_name);
        }
        // Record this writer id's root framing so `decode` can strip the union-branch prefix from
        // its bodies. Keyed per id: a later registration of a differently-framed writer must not
        // change how earlier ids are decoded.
        match union_idx {
            Some(idx) => {
                self.union_record_indices.insert(id, idx);
            }
            None => {
                self.union_record_indices.remove(&id);
            }
        }
        self.store
            .set(Fingerprint::Id(id), AvroSchema::new(record_json))
            .map_err(arrow_err)?;
        // Do NOT drop the live decoder here: it may be mid-generation for a *different* writer id
        // with buffered rows that would be lost. `decode` rebuilds (from this updated store) when
        // the active writer id actually changes.
        Ok(())
    }

    /// Whether a writer schema with this id is already registered.
    pub fn has_writer_schema(&self, id: u32) -> bool {
        self.store.lookup(&Fingerprint::Id(id)).is_some()
    }

    /// The target Arrow schema decoded batches are coerced to (reader-derived, or first writer).
    pub fn target_schema(&self) -> Option<&SchemaRef> {
        self.target_schema.as_ref()
    }

    /// The reader schema JSON with any differing writer record names merged into its top-level
    /// `aliases`, so arrow-avro's record-name resolution accepts those writers. Returns the reader
    /// JSON unchanged when there are no such writers (the common same-name case).
    fn reader_json_with_aliases(&self) -> Result<Option<String>> {
        let Some(base) = &self.reader_schema_json else {
            return Ok(None);
        };
        if self.writer_aliases.is_empty() {
            return Ok(Some(base.clone()));
        }
        let mut v: Json =
            serde_json::from_str(base).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let Some(obj) = v.as_object_mut() else {
            return Ok(Some(base.clone()));
        };
        let mut aliases: Vec<Json> = obj
            .get("aliases")
            .and_then(Json::as_array)
            .cloned()
            .unwrap_or_default();
        let existing: BTreeSet<String> = aliases
            .iter()
            .filter_map(Json::as_str)
            .map(str::to_string)
            .collect();
        for name in &self.writer_aliases {
            if !existing.contains(name) {
                aliases.push(Json::String(name.clone()));
            }
        }
        obj.insert("aliases".to_string(), Json::Array(aliases));
        Ok(Some(
            serde_json::to_string(&v).map_err(|e| DataFusionError::External(Box::new(e)))?,
        ))
    }

    fn ensure_decoder(&mut self) -> Result<&mut Decoder> {
        if self.decoder.is_none() {
            let mut builder = ReaderBuilder::new().with_writer_schema_store(self.store.clone());
            // When resolution is disabled (`skip_schema_resolution`), don't set a reader schema:
            // arrow-avro then decodes each message against its own writer schema with no resolution.
            // The batch is still coerced to `target_schema` (by name) in `flush_inner`.
            if self.resolve_against_reader
                && let Some(js) = self.reader_json_with_aliases()?
            {
                builder = builder.with_reader_schema(AvroSchema::new(js));
            }
            self.decoder = Some(builder.build_decoder().map_err(arrow_err)?);
        }
        Ok(self
            .decoder
            .as_mut()
            .expect("decoder is Some: built just above when it was None"))
    }

    /// Decode one Confluent-framed message: `0x00` + 4-byte big-endian schema id + avro body.
    pub fn decode(&mut self, framed: &[u8]) -> Result<usize> {
        if framed.len() < 5 {
            return Err(DataFusionError::Internal(
                "Confluent frame shorter than 5 bytes".into(),
            ));
        }
        let id = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);

        // A single arrow-avro Decoder can't mix writer schemas in one batch. When the writer id
        // changes, finalize the current generation and rebuild for the new schema.
        if let Some(active) = self.active_writer_id
            && active != id
        {
            if let Some(b) = self.flush_inner()? {
                self.pending.push(b);
            }
            self.decoder = None;
        }
        self.active_writer_id = Some(id);

        // For union-rooted writer schemas, strip the leading union-branch varint so the body lines
        // up with the unwrapped record schema registered in the store. Looked up per writer id so
        // mixed union/plain framings in one subject each decode correctly.
        if let Some(&record_index) = self.union_record_indices.get(&id) {
            let body = &framed[5..];
            let (branch, consumed) = read_avro_long(body)?;
            if branch != record_index {
                return Err(DataFusionError::Internal(format!(
                    "top-level avro union branch {branch} is not the record branch {record_index} \
                     (top-level null / non-record values are unsupported)"
                )));
            }
            let mut reframed = Vec::with_capacity(framed.len() - consumed);
            reframed.extend_from_slice(&framed[..5]);
            reframed.extend_from_slice(&body[consumed..]);
            return self
                .ensure_decoder()?
                .decode(&reframed)
                .map_err(|e| DataFusionError::Internal(format!("arrow-avro decode failed: {e}")));
        }
        self.ensure_decoder()?
            .decode(framed)
            .map_err(|e| DataFusionError::Internal(format!("arrow-avro decode failed: {e}")))
    }

    /// Flush the live arrow `Decoder` (if any) into a target-coerced batch. Does not touch the
    /// `pending` generations or the active-id/decoder bookkeeping.
    fn flush_inner(&mut self) -> Result<Option<RecordBatch>> {
        if self.decoder.is_none() {
            return Ok(None);
        }
        let target = self.target_schema.clone().ok_or_else(|| {
            DataFusionError::Internal("ConfluentAvroDecoder: no schema set".into())
        })?;
        let batch = self
            .decoder
            .as_mut()
            .expect("decoder is Some: guarded by the is_none() early return above")
            .flush()
            .map_err(|e| DataFusionError::Internal(format!("arrow-avro flush failed: {e}")))?;
        match batch {
            Some(b) => Ok(Some(coerce_batch_to_target(&b, &target)?)),
            None => Ok(None),
        }
    }

    /// Flush all accumulated rows into a single `RecordBatch` coerced to the target Arrow schema,
    /// concatenating across writer-schema generations. Resets the decoder so the next batch starts
    /// fresh (registered schemas in the store are retained).
    pub fn flush(&mut self) -> Result<Option<RecordBatch>> {
        let target = self.target_schema.clone().ok_or_else(|| {
            DataFusionError::Internal("ConfluentAvroDecoder: no schema set".into())
        })?;
        if let Some(b) = self.flush_inner()? {
            self.pending.push(b);
        }
        self.decoder = None;
        self.active_writer_id = None;
        let batches = std::mem::take(&mut self.pending);
        match batches.len() {
            0 => Ok(None),
            1 => Ok(Some(
                batches
                    .into_iter()
                    .next()
                    .expect("batches.len() == 1 in this match arm"),
            )),
            _ => Ok(Some(concat_batches(&target, &batches).map_err(arrow_err)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::{Record, Value};
    use apache_avro::{Decimal, Schema as AvroWriterSchema, to_avro_datum};
    use arrow_avro::schema::{AvroSchema as ArrowAvroSchema, Fingerprint, SchemaStore};

    const DECIMAL_SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}]}"#;

    // record with an array of records carrying a nested high-precision decimal (the traces shape).
    const NESTED_SCHEMA: &str = r#"{"type":"record","name":"R","fields":[
        {"name":"top","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}},
        {"name":"xfers","type":["null",{"type":"array","items":["null",{"type":"record","name":"X","fields":[
            {"name":"who","type":["null","string"],"default":null},
            {"name":"amt","type":["null",{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}],"default":null}
        ]}]}],"default":null}
    ]}"#;

    #[test]
    fn rewrite_strips_high_precision_decimal_recursively() {
        let json = rewrite_writer_schema(NESTED_SCHEMA).unwrap();
        assert!(
            !json.contains("decimal"),
            "decimal logicalType not fully stripped: {json}"
        );
        // The nested bytes type survives.
        assert!(json.contains("bytes"));
    }

    #[test]
    fn end_to_end_u256_decode_via_arrow_avro() {
        let mut payload = [0u8; 32];
        payload[0] = 0x12;
        payload[1] = 0x34;
        payload[30] = 0xAB;
        payload[31] = 0xCD;

        let decimal_schema = AvroWriterSchema::parse_str(DECIMAL_SCHEMA).unwrap();
        let mut rec = Record::new(&decimal_schema).unwrap();
        rec.put("v", Value::Decimal(Decimal::from(payload.to_vec())));
        let body = to_avro_datum(&decimal_schema, rec).unwrap();

        let rewritten_json = rewrite_writer_schema(DECIMAL_SCHEMA).unwrap();
        let mut store = SchemaStore::new();
        let fp = store
            .register(ArrowAvroSchema::new(rewritten_json))
            .unwrap();
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

        let target = convert_avro_schema_to_arrow(decimal_schema);
        let batch = coerce_batch_to_target(&batch, &target).unwrap();

        let field = batch.schema().field(0).clone();
        assert!(
            U256Type::is_u256_field(&field),
            "field not tagged u256: {field:?}"
        );
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("FixedSizeBinary(32)");
        assert_eq!(col.value(0), &payload, "u256 bytes round-trip");
    }

    #[test]
    fn confluent_decoder_u256_end_to_end() {
        let mut payload = [0u8; 32];
        payload[0] = 0x12;
        payload[15] = 0x55;
        payload[31] = 0xBB;

        let decimal_schema = AvroWriterSchema::parse_str(DECIMAL_SCHEMA).unwrap();
        let mut rec = Record::new(&decimal_schema).unwrap();
        rec.put("v", Value::Decimal(Decimal::from(payload.to_vec())));
        let body = to_avro_datum(&decimal_schema, rec).unwrap();

        let schema_id: u32 = 42;
        let mut decoder = ConfluentAvroDecoder::new();
        decoder
            .register_writer_schema(schema_id, DECIMAL_SCHEMA)
            .unwrap();

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
        assert_eq!(
            col.value(0),
            &payload,
            "u256 round-trips through Confluent decode"
        );
    }

    #[test]
    fn nested_decimal_decodes_to_list_struct_decimal128() {
        // top-level u256 value + an array with one transfer carrying a small nested decimal.
        let mut top = [0u8; 32];
        top[31] = 0x07;

        let schema = AvroWriterSchema::parse_str(NESTED_SCHEMA).unwrap();
        let mut rec = Record::new(&schema).unwrap();
        rec.put("top", Value::Decimal(Decimal::from(top.to_vec())));
        // xfers = [ {who: "alice", amt: 1234} ]
        let inner = Value::Record(vec![
            (
                "who".to_string(),
                Value::Union(1, Box::new(Value::String("alice".into()))),
            ),
            (
                "amt".to_string(),
                Value::Union(1, Box::new(Value::Decimal(Decimal::from(vec![0x04, 0xD2])))), // 1234
            ),
        ]);
        rec.put(
            "xfers",
            Value::Union(
                1,
                Box::new(Value::Array(vec![Value::Union(1, Box::new(inner))])),
            ),
        );
        let body = to_avro_datum(&schema, rec).unwrap();

        let schema_id: u32 = 7;
        let mut decoder = ConfluentAvroDecoder::new();
        decoder
            .register_writer_schema(schema_id, NESTED_SCHEMA)
            .unwrap();
        let mut framed = vec![0x00];
        framed.extend_from_slice(&schema_id.to_be_bytes());
        framed.extend_from_slice(&body);
        decoder.decode(&framed).unwrap();
        let batch = decoder.flush().unwrap().expect("a batch");

        // top-level field is u256
        assert!(U256Type::is_u256_field(&batch.schema().field(0).clone()));

        // xfers is List<Struct{who: Utf8, amt: Decimal128(100,0)}>
        let xfers = batch.schema().field(1).clone();
        let DataType::List(elem) = xfers.data_type() else {
            panic!("xfers not a List: {:?}", xfers.data_type());
        };
        let DataType::Struct(fields) = elem.data_type() else {
            panic!("element not a Struct: {:?}", elem.data_type());
        };
        let amt = fields.iter().find(|f| f.name() == "amt").unwrap();
        assert_eq!(amt.data_type(), &DataType::Decimal128(100, 0));

        // verify the nested decimal value round-trips as 1234.
        let list = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let st = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let amt_col = st
            .column_by_name("amt")
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<Decimal128Type>>()
            .unwrap();
        assert_eq!(amt_col.value(0), 1234_i128);
    }

    // Backward-compatible schema evolution (mirrors e2e test_schema_evolution_new_field_with_default):
    // rows written with v1 {id,data} and v2 {id,data,version=default 1}, both resolved to the v2
    // reader, decoded by ONE ConfluentAvroDecoder into one batch.
    const EVOLVE_V1: &str = r#"{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"}]}"#;
    const EVOLVE_V2: &str = r#"{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":1}]}"#;

    fn confluent_frame(id: u32, body: &[u8]) -> Vec<u8> {
        let mut f = vec![0x00];
        f.extend_from_slice(&id.to_be_bytes());
        f.extend_from_slice(body);
        f
    }

    #[test]
    fn mixed_writer_schemas_resolve_to_reader_in_one_batch() {
        let v1 = AvroWriterSchema::parse_str(EVOLVE_V1).unwrap();
        let v2 = AvroWriterSchema::parse_str(EVOLVE_V2).unwrap();
        let reader = AvroWriterSchema::parse_str(EVOLVE_V2).unwrap();
        let (v1_id, v2_id): (u32, u32) = (1, 2);

        let mut decoder = ConfluentAvroDecoder::new()
            .with_reader_schema(&reader)
            .unwrap();
        decoder.register_writer_schema(v2_id, EVOLVE_V2).unwrap();
        decoder.register_writer_schema(v1_id, EVOLVE_V1).unwrap();

        // 3 rows with v1 (no `version`), then 3 rows with v2 (version=2).
        for i in 1..=3i64 {
            let mut rec = Record::new(&v1).unwrap();
            rec.put("id", Value::Long(i));
            rec.put("data", Value::String(format!("v1_{i}")));
            let body = to_avro_datum(&v1, rec).unwrap();
            decoder.decode(&confluent_frame(v1_id, &body)).unwrap();
        }
        for i in 4..=6i64 {
            let mut rec = Record::new(&v2).unwrap();
            rec.put("id", Value::Long(i));
            rec.put("data", Value::String(format!("v2_{i}")));
            rec.put("version", Value::Int(2));
            let body = to_avro_datum(&v2, rec).unwrap();
            decoder.decode(&confluent_frame(v2_id, &body)).unwrap();
        }

        let batch = decoder.flush().unwrap().expect("a batch");
        assert_eq!(batch.num_rows(), 6, "all 6 rows present");
        assert_eq!(batch.num_columns(), 3, "id, data, version");
        // every column must be the same length (regression: heterogeneous-writer desync)
        for c in batch.columns() {
            assert_eq!(c.len(), 6, "column length mismatch across writer schemas");
        }
    }

    // Regression: a producer whose top-level record name differs from the reader's. The vendored
    // reader matched fields positionally and ignored the record name; arrow-avro does spec-strict
    // name resolution and would otherwise fail with "Record name mismatch writer=..., reader=...".
    // We inject the writer name as a reader alias so resolution succeeds (and field-level schema
    // evolution still applies). Mirrors the production `arbitrum-one.raw.traces` failure.
    #[test]
    fn writer_record_name_differs_from_reader_decodes_via_alias() {
        // Same fields, different record names — a pure rename plus one added reader field (default).
        const WRITER: &str = r#"{"type":"record","name":"trace_arbitrums_after_evm_transfers","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"}]}"#;
        const READER: &str = r#"{"type":"record","name":"ArbitrumTransfer","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":7}]}"#;
        let writer = AvroWriterSchema::parse_str(WRITER).unwrap();
        let reader = AvroWriterSchema::parse_str(READER).unwrap();
        let (writer_id, reader_id): (u32, u32) = (10, 20);

        let mut decoder = ConfluentAvroDecoder::new()
            .with_reader_schema(&reader)
            .unwrap();
        // Pre-register the reader's own schema (the same-id fast path), then the differently-named
        // writer fetched on first sight — exactly what the Kafka source does.
        decoder.register_writer_schema(reader_id, READER).unwrap();
        decoder.register_writer_schema(writer_id, WRITER).unwrap();

        for i in 1..=3i64 {
            let mut rec = Record::new(&writer).unwrap();
            rec.put("id", Value::Long(i));
            rec.put("data", Value::String(format!("row_{i}")));
            let body = to_avro_datum(&writer, rec).unwrap();
            decoder
                .decode(&confluent_frame(writer_id, &body))
                .expect("decode must not fail on a writer/reader record-name mismatch");
        }

        let batch = decoder.flush().unwrap().expect("a batch");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3, "id, data, version(default)");
        let versions = batch
            .column(batch.schema().index_of("version").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("version is int32");
        // The reader-only field is filled from its default for every writer row.
        assert!(
            (0..3).all(|i| versions.value(i) == 7),
            "added reader field resolved to its default"
        );
    }

    // `with_schema_resolution(false)` (a pipeline's `skip_schema_resolution`) must decode each
    // message against its own writer schema with NO writer→reader resolution — so a reader-only
    // field is NOT default-filled (it comes through null after target coercion), unlike the default
    // resolving path. Mirrors the vendored `skip_schema_resolution` behavior.
    #[test]
    fn skip_schema_resolution_decodes_against_writer_and_skips_defaults() {
        // Reader has an extra nullable field with a NON-null default; the writer lacks it.
        const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"extra","type":["int","null"],"default":42}]}"#;
        const WRITER: &str =
            r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;
        let reader = AvroWriterSchema::parse_str(READER).unwrap();
        let writer = AvroWriterSchema::parse_str(WRITER).unwrap();
        let (reader_id, writer_id): (u32, u32) = (1, 2);

        let encode = |id_val: i64| {
            let mut rec = Record::new(&writer).unwrap();
            rec.put("id", Value::Long(id_val));
            confluent_frame(writer_id, &to_avro_datum(&writer, rec).unwrap())
        };
        let extra_of = |rb: &RecordBatch| -> Arc<arrow::array::Int32Array> {
            Arc::new(
                rb.column(rb.schema().index_of("extra").unwrap())
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .expect("extra is int32")
                    .clone(),
            )
        };

        // Resolution ON (default): the missing `extra` is filled from its reader default (42).
        let mut resolving = ConfluentAvroDecoder::new()
            .with_reader_schema(&reader)
            .unwrap();
        resolving.register_writer_schema(reader_id, READER).unwrap();
        resolving.register_writer_schema(writer_id, WRITER).unwrap();
        resolving.decode(&encode(1)).unwrap();
        let resolved = resolving.flush().unwrap().expect("batch");
        let re = extra_of(&resolved);
        assert!(
            !re.is_null(0) && re.value(0) == 42,
            "resolution fills the reader default"
        );

        // Resolution OFF (skip): decode against the writer schema; `extra` is absent → null, not
        // defaulted. `id` still decodes.
        let mut skipping = ConfluentAvroDecoder::new()
            .with_reader_schema(&reader)
            .unwrap()
            .with_schema_resolution(false);
        skipping.register_writer_schema(reader_id, READER).unwrap();
        skipping.register_writer_schema(writer_id, WRITER).unwrap();
        skipping.decode(&encode(7)).unwrap();
        let skipped = skipping.flush().unwrap().expect("batch");
        assert!(
            extra_of(&skipped).is_null(0),
            "skip_schema_resolution must not fill reader defaults"
        );
        let id = skipped
            .column(skipped.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("id is int64");
        assert_eq!(id.value(0), 7, "writer field still decodes under skip");
    }

    // Equivalence check (#4): the vendored `Resolver::resolve` coerced avro numerics with
    // `NumCast::from`, which returns `None` (→ silent NULL) on overflow. That fallback was only
    // reachable if the target arrow type were *narrower* than the avro value — but the avro→arrow
    // mapping is width-preserving (int→Int32, long→Int64, float→Float32, double→Float64) and Avro
    // only permits *widening* promotion, so it never triggered. arrow-avro decodes each primitive to
    // its natural width identically. This asserts the reachable domain: boundary values round-trip
    // exactly, with no overflow, panic, or width change.
    #[test]
    fn numeric_boundaries_decode_exactly() {
        const SCHEMA: &str = r#"{"type":"record","name":"N","fields":[
            {"name":"i","type":"int"},{"name":"l","type":"long"},
            {"name":"f","type":"float"},{"name":"d","type":"double"}]}"#;
        let schema = AvroWriterSchema::parse_str(SCHEMA).unwrap();
        let id = 1u32;
        let mut decoder = ConfluentAvroDecoder::new()
            .with_reader_schema(&schema)
            .unwrap();
        decoder.register_writer_schema(id, SCHEMA).unwrap();
        let mut rec = Record::new(&schema).unwrap();
        rec.put("i", Value::Int(i32::MIN));
        rec.put("l", Value::Long(i64::MAX));
        rec.put("f", Value::Float(f32::MIN));
        rec.put("d", Value::Double(f64::MAX));
        decoder
            .decode(&confluent_frame(id, &to_avro_datum(&schema, rec).unwrap()))
            .unwrap();
        let b = decoder.flush().unwrap().expect("batch");
        let col = |name: &str| b.column(b.schema().index_of(name).unwrap()).clone();
        assert_eq!(
            col("i")
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(0),
            i32::MIN
        );
        assert_eq!(
            col("l")
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            i64::MAX
        );
        assert_eq!(
            col("f")
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .unwrap()
                .value(0),
            f32::MIN
        );
        assert_eq!(
            col("d")
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .value(0),
            f64::MAX
        );
    }

    // Equivalence check (#5): the u256/i256/decimal byte reinterpretation must match the vendored
    // `resolve_u256`/`resolve_i256`/`resolve_decimal(_256)` two's-complement handling exactly (the
    // functions were extracted from them). Locks in the concrete byte→value contract.
    #[test]
    fn decimal_byte_reinterpretation_is_twos_complement() {
        // i128 sign extension (Decimal128 nested path).
        assert_eq!(be_bytes_to_i128(&[0x01]), 1);
        assert_eq!(be_bytes_to_i128(&[0xFF]), -1);
        assert_eq!(be_bytes_to_i128(&[0x80]), -128);
        assert_eq!(be_bytes_to_i128(&[0x01, 0x00]), 256);
        // i256 sign extension (Decimal256 path).
        assert_eq!(be_bytes_to_i256(&[0x01]), i256::from_i128(1));
        assert_eq!(be_bytes_to_i256(&[0xFF]), i256::from_i128(-1));
        // u256: big-endian zero-extension; negatives rejected.
        let one = u256_be_bytes(&[0x01]).unwrap();
        assert_eq!(one[31], 1);
        assert!(one[..31].iter().all(|&x| x == 0));
        assert!(
            u256_be_bytes(&[0x80]).is_err(),
            "negative decimal rejected for u256"
        );
        // i256 bytes: sign-extended fill.
        assert_eq!(i256_be_bytes(&[0xFF]).unwrap(), [0xFFu8; 32]);
        assert_eq!(i256_be_bytes(&[0x01]).unwrap()[31], 1);
        assert!(
            i256_be_bytes(&[0x01]).unwrap()[..31]
                .iter()
                .all(|&x| x == 0)
        );
    }

    // Debezium/Confluent union-root framing: the writer schema's root is a `["null", record]`
    // union, so each body carries a leading union-branch varint that must be stripped before the
    // unwrapped record decodes. Exercises `union_record_indices` + `read_avro_long` re-framing,
    // which is otherwise only hit through the live pipeline.
    #[test]
    fn union_root_record_strips_branch_prefix_and_decodes() {
        const REC: &str = r#"{"type":"record","name":"Envelope","fields":[{"name":"id","type":"long"},{"name":"name","type":["null","string"],"default":null}]}"#;
        let union_json = format!(r#"["null",{REC}]"#);
        let union_schema = AvroWriterSchema::parse_str(&union_json).unwrap();
        let reader = AvroWriterSchema::parse_str(&union_json).unwrap();

        let id = 5u32;
        let mut decoder = ConfluentAvroDecoder::new()
            .with_reader_schema(&reader)
            .unwrap();
        decoder.register_writer_schema(id, &union_json).unwrap();

        // Value::Union(1, record) — to_avro_datum writes the record-branch varint + the record body,
        // exactly the wire shape a Debezium producer emits.
        let record_val = Value::Record(vec![
            ("id".to_string(), Value::Long(99)),
            (
                "name".to_string(),
                Value::Union(1, Box::new(Value::String("hi".into()))),
            ),
        ]);
        let body = to_avro_datum(&union_schema, Value::Union(1, Box::new(record_val))).unwrap();
        decoder.decode(&confluent_frame(id, &body)).unwrap();
        let batch = decoder.flush().unwrap().expect("a batch");

        assert_eq!(batch.num_rows(), 1);
        let ids = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("id is int64");
        assert_eq!(
            ids.value(0),
            99,
            "union-root body decodes after prefix strip"
        );
    }

    // Regression (Bugbot): the union branch index must be tracked PER writer id, not globally.
    // A single subject can carry a union-rooted writer (id A) and a plain-rooted writer (id B).
    // Registering the plain writer last must NOT change how the union writer's messages are
    // decoded. A global index would have union-stripping skipped for id A after B registers,
    // corrupting its bodies.
    #[test]
    fn mixed_union_and_plain_framing_decode_per_writer_id() {
        const REC: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;
        let union_json = format!(r#"["null",{REC}]"#);
        let union_schema = AvroWriterSchema::parse_str(&union_json).unwrap();
        let plain_schema = AvroWriterSchema::parse_str(REC).unwrap();
        let reader = AvroWriterSchema::parse_str(REC).unwrap();
        let (union_id, plain_id): (u32, u32) = (1, 2);

        let mut decoder = ConfluentAvroDecoder::new()
            .with_reader_schema(&reader)
            .unwrap();
        // Register union-rooted first, then plain-rooted — the ordering that a global index botches.
        decoder
            .register_writer_schema(union_id, &union_json)
            .unwrap();
        decoder.register_writer_schema(plain_id, REC).unwrap();

        // union-framed message (needs prefix strip)
        let union_body = to_avro_datum(
            &union_schema,
            Value::Union(
                1,
                Box::new(Value::Record(vec![("id".to_string(), Value::Long(11))])),
            ),
        )
        .unwrap();
        decoder
            .decode(&confluent_frame(union_id, &union_body))
            .unwrap();
        // plain-framed message (must NOT be prefix-stripped)
        let plain_body = to_avro_datum(
            &plain_schema,
            Value::Record(vec![("id".to_string(), Value::Long(22))]),
        )
        .unwrap();
        decoder
            .decode(&confluent_frame(plain_id, &plain_body))
            .unwrap();

        let batch = decoder.flush().unwrap().expect("a batch");
        assert_eq!(batch.num_rows(), 2, "both writer generations present");
        let ids = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("id is int64");
        let mut vals: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
        vals.sort();
        assert_eq!(
            vals,
            vec![11, 22],
            "each writer id decoded with its own framing"
        );
    }

    // Regression (Bugbot): a nullable target field absent from a decoded *nested* struct must be
    // filled with nulls (mirroring `coerce_batch_to_target`'s top-level handling and the vendored
    // reader), not error. A missing *required* nested field is still an error.
    #[test]
    fn coerce_struct_fills_missing_nullable_nested_field_with_nulls() {
        use arrow::array::Int64Array;
        use arrow_schema::Schema;

        // Source struct `s` has only `a`; the target adds a nullable `b` and (later) a required `c`.
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let src_struct = StructArray::new(
            Fields::from(vec![Field::new("a", DataType::Int64, false)]),
            vec![a],
            None,
        );
        let src_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                src_struct.data_type().clone(),
                false,
            )])),
            vec![Arc::new(src_struct)],
        )
        .unwrap();

        // Nullable `b` absent from the source → filled with nulls, `a` preserved.
        let ok_fields = Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let ok_target = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(ok_fields),
            false,
        )]));
        let out = coerce_batch_to_target(&src_batch, &ok_target).unwrap();
        let s = out
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            s.column_by_name("b").unwrap().null_count(),
            3,
            "missing nullable nested field is all-null"
        );
        let a_out = s
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a_out.values(), &[1, 2, 3], "present nested field preserved");

        // A missing *required* nested field is still a hard error.
        let bad_fields = Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false),
        ]);
        let bad_target = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(bad_fields),
            false,
        )]));
        assert!(
            coerce_batch_to_target(&src_batch, &bad_target).is_err(),
            "missing required nested field must error"
        );
    }
}
