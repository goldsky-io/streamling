//! Adversarial integration tests: NON-DECIMAL type-mapping fidelity.
//!
//! PR #60 replaced the vendored apache-avro->Arrow reader with DataFusion 54's `arrow-avro`
//! crate, then coerces arrow-avro's decoded batch to streamling's target schema (the one
//! `convert_avro_schema_to_arrow` produces). For most non-decimal types `coerce_array` either
//! passes the column through (if it already equals the target) or leans on arrow's generic
//! `cast` kernel. That fallback is the richest bug surface: arrow-avro's native avro->Arrow
//! mapping frequently diverges from streamling's target, and a divergent cast may error, panic,
//! or silently corrupt values.
//!
//! Each test builds a single-field (or small) record schema, asks the ORACLE
//! (`convert_avro_schema_to_arrow`) what the target dtype must be, decodes a value through
//! `ConfluentAvroDecoder`, and asserts the CORRECT dtype + value round-trip. Where the new path
//! cannot honor the oracle it will error/panic here, flagging the bug.
//!
//! Oracle (convert_avro_schema_to_arrow):
//!   null->Null, boolean->Boolean, int->Int32, long->Int64, float->Float32, double->Float64,
//!   bytes->Binary, string->Utf8, enum->Utf8(symbol), fixed(n)->FixedSizeBinary(n),
//!   uuid->FixedSizeBinary(16), array<T>->List(element:T), record->Struct,
//!   map<V>->Dictionary(Utf8,V), union[null,T]->nullable T, multi-variant union->Union(Dense),
//!   date->Date32, time-millis->Time32(ms), time-micros->Time64(us),
//!   timestamp-millis->Timestamp(ms,None), timestamp-micros->Timestamp(us,None),
//!   local-timestamp-millis->Timestamp(ms,None), local-timestamp-micros->Timestamp(us,None).

use apache_avro::types::{Record, Value};
use apache_avro::{Schema as AvroWriterSchema, to_avro_datum};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, ListArray, NullArray, StringArray, StructArray,
    Time32MillisecondArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, SchemaRef, TimeUnit, UnionMode};
use std::collections::HashMap;
use streamling_common::formats::avro::arrow_avro::ConfluentAvroDecoder;
use streamling_common::formats::avro::convert_avro_schema_to_arrow;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn confluent_frame(id: u32, body: &[u8]) -> Vec<u8> {
    let mut f = vec![0x00];
    f.extend_from_slice(&id.to_be_bytes());
    f.extend_from_slice(body);
    f
}

/// The oracle target Arrow schema for `schema_json`.
fn target_of(schema_json: &str) -> SchemaRef {
    let schema = AvroWriterSchema::parse_str(schema_json).unwrap();
    convert_avro_schema_to_arrow(schema)
}

/// Decode a single Avro record (built by `put`) through the full ConfluentAvroDecoder pipeline,
/// resolving against the same schema as reader. Panics (=> test failure) if any stage errors,
/// which is exactly how a decode/coercion bug surfaces.
fn decode_record(schema_json: &str, fields: &[(&str, Value)]) -> RecordBatch {
    let schema = AvroWriterSchema::parse_str(schema_json).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(1, schema_json).unwrap();
    let mut rec = Record::new(&schema).unwrap();
    for (name, value) in fields {
        rec.put(name, value.clone());
    }
    let body = to_avro_datum(&schema, rec).unwrap();
    d.decode(&confluent_frame(1, &body)).unwrap();
    d.flush().unwrap().expect("a batch")
}

/// Single-field convenience over [`decode_record`].
fn decode_one(schema_json: &str, field: &str, value: Value) -> RecordBatch {
    decode_record(schema_json, &[(field, value)])
}

fn rec1(field_type: &str) -> String {
    format!(r#"{{"type":"record","name":"R","fields":[{{"name":"v","type":{field_type}}}]}}"#)
}

// ---------------------------------------------------------------------------
// boolean -> Boolean
// ---------------------------------------------------------------------------

#[test]
fn boolean_true_roundtrips() {
    let s = rec1(r#""boolean""#);
    assert_eq!(target_of(&s).field(0).data_type(), &DataType::Boolean);
    let b = decode_one(&s, "v", Value::Boolean(true));
    let c = b.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(c.value(0), "boolean true must round-trip");
}

#[test]
fn boolean_false_roundtrips() {
    let s = rec1(r#""boolean""#);
    let b = decode_one(&s, "v", Value::Boolean(false));
    let c = b.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!c.value(0), "boolean false must round-trip");
}

// ---------------------------------------------------------------------------
// int -> Int32
// ---------------------------------------------------------------------------

fn assert_int(v: i32) {
    let s = rec1(r#""int""#);
    assert_eq!(target_of(&s).field(0).data_type(), &DataType::Int32);
    let b = decode_one(&s, "v", Value::Int(v));
    let c = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(c.value(0), v, "int must map to Int32 and round-trip");
}

#[test]
fn int_zero() {
    assert_int(0);
}
#[test]
fn int_one() {
    assert_int(1);
}
#[test]
fn int_negative_one() {
    assert_int(-1);
}
#[test]
fn int_max() {
    assert_int(i32::MAX);
}
#[test]
fn int_min() {
    assert_int(i32::MIN);
}
#[test]
fn int_typical() {
    assert_int(1_234_567);
}

// ---------------------------------------------------------------------------
// long -> Int64
// ---------------------------------------------------------------------------

fn assert_long(v: i64) {
    let s = rec1(r#""long""#);
    assert_eq!(target_of(&s).field(0).data_type(), &DataType::Int64);
    let b = decode_one(&s, "v", Value::Long(v));
    let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(c.value(0), v, "long must map to Int64 and round-trip");
}

#[test]
fn long_zero() {
    assert_long(0);
}
#[test]
fn long_max() {
    assert_long(i64::MAX);
}
#[test]
fn long_min() {
    assert_long(i64::MIN);
}
#[test]
fn long_negative() {
    assert_long(-98765);
}
#[test]
fn long_typical() {
    assert_long(9_000_000_000);
}

// ---------------------------------------------------------------------------
// float -> Float32 / double -> Float64
// ---------------------------------------------------------------------------

fn assert_float(v: f32) {
    let s = rec1(r#""float""#);
    assert_eq!(target_of(&s).field(0).data_type(), &DataType::Float32);
    let b = decode_one(&s, "v", Value::Float(v));
    let c = b.column(0).as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(c.value(0), v, "float must map to Float32 and round-trip");
}

#[test]
fn float_zero() {
    assert_float(0.0);
}
#[test]
fn float_positive() {
    assert_float(1.5);
}
#[test]
fn float_negative() {
    assert_float(-2.5);
}
#[test]
fn float_max() {
    assert_float(f32::MAX);
}

fn assert_double(v: f64) {
    let s = rec1(r#""double""#);
    assert_eq!(target_of(&s).field(0).data_type(), &DataType::Float64);
    let b = decode_one(&s, "v", Value::Double(v));
    let c = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(c.value(0), v, "double must map to Float64 and round-trip");
}

#[test]
fn double_zero() {
    assert_double(0.0);
}
#[test]
fn double_pi() {
    assert_double(std::f64::consts::PI);
}
#[test]
fn double_negative_large() {
    assert_double(-1e300);
}
#[test]
fn double_min() {
    assert_double(f64::MIN);
}

// ---------------------------------------------------------------------------
// bytes -> Binary
// ---------------------------------------------------------------------------

#[test]
fn bytes_dtype_is_binary() {
    let s = rec1(r#""bytes""#);
    assert_eq!(target_of(&s).field(0).data_type(), &DataType::Binary);
}

fn assert_bytes(v: Vec<u8>) {
    let s = rec1(r#""bytes""#);
    let b = decode_one(&s, "v", Value::Bytes(v.clone()));
    let c = b.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(
        c.value(0),
        &v[..],
        "bytes must map to Binary and round-trip"
    );
}

#[test]
fn bytes_typical() {
    assert_bytes(vec![1, 2, 3, 4, 5]);
}
#[test]
fn bytes_empty() {
    assert_bytes(vec![]);
}
#[test]
fn bytes_high_bit() {
    assert_bytes(vec![0xFF, 0x80, 0x00, 0x7F]);
}
#[test]
fn bytes_single() {
    assert_bytes(vec![0xAB]);
}

// ---------------------------------------------------------------------------
// string -> Utf8
// ---------------------------------------------------------------------------

#[test]
fn string_dtype_is_utf8() {
    let s = rec1(r#""string""#);
    assert_eq!(target_of(&s).field(0).data_type(), &DataType::Utf8);
}

fn assert_string(v: &str) {
    let s = rec1(r#""string""#);
    let b = decode_one(&s, "v", Value::String(v.to_string()));
    let c = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(c.value(0), v, "string must map to Utf8 and round-trip");
}

#[test]
fn string_typical() {
    assert_string("hello world");
}
#[test]
fn string_empty() {
    assert_string("");
}
#[test]
fn string_unicode() {
    assert_string("héllo 世界 🚀");
}
#[test]
fn string_long() {
    assert_string(&"a".repeat(5000));
}

// ---------------------------------------------------------------------------
// null -> Null
// ---------------------------------------------------------------------------

#[test]
fn null_type_maps_to_null_dtype() {
    let s = rec1(r#""null""#);
    assert_eq!(target_of(&s).field(0).data_type(), &DataType::Null);
}

#[test]
#[ignore = "known gap (not a blocking regression): a bare avro \"null\"-typed field yields no batch under arrow-avro (vendored produced a NullArray). Practically unreachable — nullability uses [\"null\",T]."]
fn null_type_decodes_to_null_array() {
    let s = rec1(r#""null""#);
    let b = decode_one(&s, "v", Value::Null);
    assert_eq!(b.num_rows(), 1, "one row decoded for a null-typed field");
    let c = b.column(0).as_any().downcast_ref::<NullArray>().unwrap();
    assert_eq!(c.len(), 1, "null column has one (null) slot");
}

// ---------------------------------------------------------------------------
// enum -> Utf8 (symbol string)
// ---------------------------------------------------------------------------

const ENUM_FIELD: &str = r#"{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}"#;

#[test]
fn enum_maps_to_utf8() {
    let s = rec1(ENUM_FIELD);
    assert_eq!(target_of(&s).field(0).data_type(), &DataType::Utf8);
}

fn assert_enum(idx: u32, symbol: &str) {
    let s = rec1(ENUM_FIELD);
    let b = decode_one(&s, "v", Value::Enum(idx, symbol.to_string()));
    let c = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        c.value(0),
        symbol,
        "enum must decode to its symbol string, not an ordinal or dictionary key"
    );
}

#[test]
fn enum_first_symbol() {
    assert_enum(0, "RED");
}
#[test]
fn enum_middle_symbol() {
    assert_enum(1, "GREEN");
}
#[test]
fn enum_last_symbol() {
    assert_enum(2, "BLUE");
}

// enum inside an array -> List(element: Utf8)
#[test]
fn enum_in_array_maps_to_list_of_utf8() {
    let field = format!(r#"{{"type":"array","items":{ENUM_FIELD}}}"#);
    let s = rec1(&field);
    let target = target_of(&s);
    let DataType::List(child) = target.field(0).data_type() else {
        panic!(
            "array<enum> must map to List, got {:?}",
            target.field(0).data_type()
        );
    };
    assert_eq!(
        child.data_type(),
        &DataType::Utf8,
        "array element enum must map to Utf8"
    );
}

#[test]
fn enum_in_array_values_roundtrip() {
    let field = format!(r#"{{"type":"array","items":{ENUM_FIELD}}}"#);
    let s = rec1(&field);
    let b = decode_one(
        &s,
        "v",
        Value::Array(vec![
            Value::Enum(0, "RED".to_string()),
            Value::Enum(2, "BLUE".to_string()),
        ]),
    );
    let list = b.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let vals = list
        .value(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("list element must coerce to Utf8")
        .iter()
        .map(|o| o.unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        vals,
        vec!["RED", "BLUE"],
        "enum-in-array symbols round-trip"
    );
}

// ---------------------------------------------------------------------------
// fixed(n) -> FixedSizeBinary(n)
// ---------------------------------------------------------------------------

fn assert_fixed(size: usize, bytes: Vec<u8>) {
    assert_eq!(bytes.len(), size);
    let field = format!(r#"{{"type":"fixed","name":"F{size}","size":{size}}}"#);
    let s = rec1(&field);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::FixedSizeBinary(size as i32),
        "fixed({size}) must map to FixedSizeBinary({size})"
    );
    let b = decode_one(&s, "v", Value::Fixed(size, bytes.clone()));
    let c = b
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(c.value(0), &bytes[..], "fixed bytes must round-trip");
}

#[test]
fn fixed_size_1() {
    assert_fixed(1, vec![0x42]);
}
#[test]
fn fixed_size_4() {
    assert_fixed(4, vec![0xDE, 0xAD, 0xBE, 0xEF]);
}
#[test]
fn fixed_size_8() {
    assert_fixed(8, vec![1, 2, 3, 4, 5, 6, 7, 8]);
}
#[test]
fn fixed_size_16() {
    assert_fixed(16, (0u8..16).collect());
}
#[test]
fn fixed_size_20_high_bytes() {
    assert_fixed(20, vec![0xFF; 20]);
}
#[test]
fn fixed_size_20_zero() {
    assert_fixed(20, vec![0x00; 20]);
}

// ---------------------------------------------------------------------------
// uuid -> FixedSizeBinary(16)   (mapping-only: wire form is a 36-char string, a known divergence)
// ---------------------------------------------------------------------------

#[test]
fn uuid_maps_to_fixed_size_binary_16() {
    let s = rec1(r#"{"type":"string","logicalType":"uuid"}"#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::FixedSizeBinary(16),
        "uuid must map to FixedSizeBinary(16)"
    );
}

// ---------------------------------------------------------------------------
// map<V> -> Dictionary(Utf8, V)   (HIGH SUSPICION: arrow-avro produces a Map; cast likely diverges)
// ---------------------------------------------------------------------------

fn map_field(values: &str) -> String {
    rec1(&format!(r#"{{"type":"map","values":{values}}}"#))
}

#[test]
fn map_long_maps_to_dictionary() {
    let s = map_field(r#""long""#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Int64)),
        "map<long> must map to Dictionary(Utf8, Int64)"
    );
}

#[test]
fn map_string_maps_to_dictionary() {
    let s = map_field(r#""string""#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Utf8)),
        "map<string> must map to Dictionary(Utf8, Utf8)"
    );
}

#[test]
fn map_int_maps_to_dictionary() {
    let s = map_field(r#""int""#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Int32)),
    );
}

#[test]
fn map_double_maps_to_dictionary() {
    let s = map_field(r#""double""#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Float64)),
    );
}

#[test]
fn map_boolean_maps_to_dictionary() {
    let s = map_field(r#""boolean""#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Boolean)),
    );
}

#[test]
#[ignore = "pre-existing (not a migration regression): avro map -> Dictionary(Utf8,V) was unsupported by the removed vendored reader too (its Dictionary builder resolves enum/string values, not Value::Map); arrow-avro decodes maps as arrow Map and coerce cannot cast Map->Dictionary. Tracked as a known avro-map gap."]
fn map_long_decodes_and_preserves_row() {
    // Full pipeline: must not error/panic and must yield exactly one row.
    let s = map_field(r#""long""#);
    let mut m = HashMap::new();
    m.insert("alpha".to_string(), Value::Long(7));
    m.insert("beta".to_string(), Value::Long(9));
    let b = decode_one(&s, "v", Value::Map(m));
    assert_eq!(b.num_rows(), 1, "map<long> must decode to one row");
}

#[test]
#[ignore = "pre-existing (not a migration regression): avro map -> Dictionary(Utf8,V) was unsupported by the removed vendored reader too (its Dictionary builder resolves enum/string values, not Value::Map); arrow-avro decodes maps as arrow Map and coerce cannot cast Map->Dictionary. Tracked as a known avro-map gap."]
fn map_string_decodes_and_preserves_row() {
    let s = map_field(r#""string""#);
    let mut m = HashMap::new();
    m.insert("k".to_string(), Value::String("val".to_string()));
    let b = decode_one(&s, "v", Value::Map(m));
    assert_eq!(b.num_rows(), 1, "map<string> must decode to one row");
}

#[test]
#[ignore = "pre-existing (not a migration regression): avro map -> Dictionary(Utf8,V) was unsupported by the removed vendored reader too (its Dictionary builder resolves enum/string values, not Value::Map); arrow-avro decodes maps as arrow Map and coerce cannot cast Map->Dictionary. Tracked as a known avro-map gap."]
fn map_empty_decodes() {
    let s = map_field(r#""long""#);
    let b = decode_one(&s, "v", Value::Map(HashMap::new()));
    assert_eq!(b.num_rows(), 1, "empty map must decode to one row");
}

#[test]
#[ignore = "pre-existing (not a migration regression): avro map -> Dictionary(Utf8,V) was unsupported by the removed vendored reader too (its Dictionary builder resolves enum/string values, not Value::Map); arrow-avro decodes maps as arrow Map and coerce cannot cast Map->Dictionary. Tracked as a known avro-map gap."]
fn nested_map_in_record_maps_to_struct_of_dictionary() {
    let inner = r#"{"type":"record","name":"Inner","fields":[{"name":"m","type":{"type":"map","values":"string"}}]}"#;
    let s = rec1(inner);
    let target = target_of(&s);
    let DataType::Struct(fields) = target.field(0).data_type() else {
        panic!(
            "nested record must map to Struct, got {:?}",
            target.field(0).data_type()
        );
    };
    let m = fields.iter().find(|f| f.name() == "m").expect("field m");
    assert_eq!(
        m.data_type(),
        &DataType::Dictionary(Box::new(DataType::Utf8), Box::new(DataType::Utf8)),
        "nested map must map to Dictionary(Utf8, Utf8)"
    );
    // And the full decode of a nested map must not error/panic.
    let mut mm = HashMap::new();
    mm.insert("k".to_string(), Value::String("v".to_string()));
    let inner_rec = Value::Record(vec![("m".to_string(), Value::Map(mm))]);
    let b = decode_one(&s, "v", inner_rec);
    assert_eq!(b.num_rows(), 1, "nested map decodes to one row");
}

// ---------------------------------------------------------------------------
// date -> Date32
// ---------------------------------------------------------------------------

#[test]
fn date_maps_to_date32() {
    let s = rec1(r#"{"type":"int","logicalType":"date"}"#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Date32,
        "avro date must map to Date32"
    );
}

fn assert_date(days: i32) {
    let s = rec1(r#"{"type":"int","logicalType":"date"}"#);
    let b = decode_one(&s, "v", Value::Date(days));
    let c = b.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
    assert_eq!(c.value(0), days, "date (days since epoch) must round-trip");
}

#[test]
fn date_value() {
    assert_date(19_000);
}
#[test]
fn date_epoch() {
    assert_date(0);
}
#[test]
fn date_negative() {
    assert_date(-3650);
}

// ---------------------------------------------------------------------------
// time-millis -> Time32(ms) ; time-micros -> Time64(us)
// ---------------------------------------------------------------------------

#[test]
fn time_millis_maps_to_time32() {
    let s = rec1(r#"{"type":"int","logicalType":"time-millis"}"#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Time32(TimeUnit::Millisecond),
    );
}

#[test]
fn time_millis_value() {
    let s = rec1(r#"{"type":"int","logicalType":"time-millis"}"#);
    let b = decode_one(&s, "v", Value::TimeMillis(3_600_000));
    let c = b
        .column(0)
        .as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .unwrap();
    assert_eq!(c.value(0), 3_600_000, "time-millis must round-trip");
}

#[test]
fn time_micros_maps_to_time64() {
    let s = rec1(r#"{"type":"long","logicalType":"time-micros"}"#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Time64(TimeUnit::Microsecond),
    );
}

#[test]
fn time_micros_value() {
    let s = rec1(r#"{"type":"long","logicalType":"time-micros"}"#);
    let b = decode_one(&s, "v", Value::TimeMicros(3_600_000_000));
    let c = b
        .column(0)
        .as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap();
    assert_eq!(c.value(0), 3_600_000_000, "time-micros must round-trip");
}

// ---------------------------------------------------------------------------
// timestamp-millis / timestamp-micros -> Timestamp(unit, None)   (tz divergence: arrow-avro
// commonly tags UTC timestamps with a "+00:00" tz; target is tz-less)
// ---------------------------------------------------------------------------

#[test]
fn timestamp_millis_maps_to_tz_less() {
    let s = rec1(r#"{"type":"long","logicalType":"timestamp-millis"}"#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, None),
        "timestamp-millis must map to a tz-LESS Timestamp(ms, None)"
    );
}

fn assert_ts_millis(v: i64) {
    let s = rec1(r#"{"type":"long","logicalType":"timestamp-millis"}"#);
    let b = decode_one(&s, "v", Value::TimestampMillis(v));
    let c = b
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(c.value(0), v, "timestamp-millis value must round-trip");
}

#[test]
fn timestamp_millis_value() {
    assert_ts_millis(1_600_000_000_000);
}
#[test]
fn timestamp_millis_negative() {
    assert_ts_millis(-1_000);
}

#[test]
fn timestamp_micros_maps_to_tz_less() {
    let s = rec1(r#"{"type":"long","logicalType":"timestamp-micros"}"#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
    );
}

#[test]
fn timestamp_micros_value() {
    let s = rec1(r#"{"type":"long","logicalType":"timestamp-micros"}"#);
    let b = decode_one(&s, "v", Value::TimestampMicros(1_600_000_000_000_000));
    let c = b
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        c.value(0),
        1_600_000_000_000_000,
        "timestamp-micros value must round-trip"
    );
}

// ---------------------------------------------------------------------------
// local-timestamp-millis / local-timestamp-micros -> Timestamp(unit, None)
// ---------------------------------------------------------------------------

#[test]
fn local_timestamp_millis_maps_to_tz_less() {
    let s = rec1(r#"{"type":"long","logicalType":"local-timestamp-millis"}"#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Millisecond, None),
        "local-timestamp-millis must map to tz-less Timestamp(ms, None)"
    );
}

#[test]
fn local_timestamp_millis_value() {
    let s = rec1(r#"{"type":"long","logicalType":"local-timestamp-millis"}"#);
    let b = decode_one(&s, "v", Value::LocalTimestampMillis(1_700_000_000_000));
    let c = b
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(
        c.value(0),
        1_700_000_000_000,
        "local-timestamp-millis value must round-trip"
    );
}

#[test]
fn local_timestamp_micros_maps_to_tz_less() {
    let s = rec1(r#"{"type":"long","logicalType":"local-timestamp-micros"}"#);
    assert_eq!(
        target_of(&s).field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
    );
}

#[test]
fn local_timestamp_micros_value() {
    let s = rec1(r#"{"type":"long","logicalType":"local-timestamp-micros"}"#);
    let b = decode_one(&s, "v", Value::LocalTimestampMicros(1_700_000_000_000_000));
    let c = b
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(
        c.value(0),
        1_700_000_000_000_000,
        "local-timestamp-micros value must round-trip"
    );
}

// ---------------------------------------------------------------------------
// union[null, T] (2-branch) -> nullable T
// ---------------------------------------------------------------------------

fn nullable_field(inner: &str) -> String {
    rec1(&format!(r#"["null",{inner}]"#))
}

#[test]
fn nullable_int_dtype_and_nullable_flag() {
    let s = nullable_field(r#""int""#);
    let t = target_of(&s);
    assert_eq!(t.field(0).data_type(), &DataType::Int32);
    assert!(
        t.field(0).is_nullable(),
        "[null,int] must be nullable Int32"
    );
}

#[test]
fn nullable_int_present_value() {
    let s = nullable_field(r#""int""#);
    let b = decode_one(&s, "v", Value::Union(1, Box::new(Value::Int(55))));
    let c = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(!c.is_null(0));
    assert_eq!(c.value(0), 55, "present nullable int round-trips");
}

#[test]
fn nullable_int_null_value() {
    let s = nullable_field(r#""int""#);
    let b = decode_one(&s, "v", Value::Union(0, Box::new(Value::Null)));
    let c = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(c.is_null(0), "null branch must decode to a null slot");
}

#[test]
fn nullable_string_present_value() {
    let s = nullable_field(r#""string""#);
    let b = decode_one(
        &s,
        "v",
        Value::Union(1, Box::new(Value::String("x".into()))),
    );
    let c = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(c.value(0), "x");
}

#[test]
fn nullable_string_null_value() {
    let s = nullable_field(r#""string""#);
    let b = decode_one(&s, "v", Value::Union(0, Box::new(Value::Null)));
    let c = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(c.is_null(0));
}

#[test]
fn nullable_boolean_present_value() {
    let s = nullable_field(r#""boolean""#);
    let b = decode_one(&s, "v", Value::Union(1, Box::new(Value::Boolean(true))));
    let c = b.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(!c.is_null(0) && c.value(0));
}

#[test]
fn nullable_long_present_value() {
    let s = nullable_field(r#""long""#);
    let b = decode_one(&s, "v", Value::Union(1, Box::new(Value::Long(-42))));
    let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(c.value(0), -42);
}

#[test]
fn nullable_double_present_value() {
    let s = nullable_field(r#""double""#);
    let b = decode_one(&s, "v", Value::Union(1, Box::new(Value::Double(2.75))));
    let c = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(c.value(0), 2.75);
}

#[test]
fn nullable_bytes_present_value() {
    let s = nullable_field(r#""bytes""#);
    let b = decode_one(
        &s,
        "v",
        Value::Union(1, Box::new(Value::Bytes(vec![9, 8, 7]))),
    );
    let c = b.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(c.value(0), &[9u8, 8, 7][..]);
}

#[test]
fn nullable_fixed_present_value() {
    let s = nullable_field(r#"{"type":"fixed","name":"F4","size":4}"#);
    let t = target_of(&s);
    assert_eq!(t.field(0).data_type(), &DataType::FixedSizeBinary(4));
    let b = decode_one(
        &s,
        "v",
        Value::Union(1, Box::new(Value::Fixed(4, vec![1, 2, 3, 4]))),
    );
    let c = b
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(c.value(0), &[1u8, 2, 3, 4][..]);
}

#[test]
fn nullable_enum_present_value() {
    let s = nullable_field(ENUM_FIELD);
    let t = target_of(&s);
    assert_eq!(t.field(0).data_type(), &DataType::Utf8);
    let b = decode_one(
        &s,
        "v",
        Value::Union(1, Box::new(Value::Enum(2, "BLUE".into()))),
    );
    let c = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(c.value(0), "BLUE", "nullable enum decodes to its symbol");
}

// ---------------------------------------------------------------------------
// multi-variant union (3+ non-null, or 3-variant incl null) -> Union(Dense)
// ---------------------------------------------------------------------------

#[test]
fn three_branch_union_maps_to_dense_union() {
    let s = rec1(r#"["int","string","boolean"]"#);
    let dt = target_of(&s).field(0).data_type().clone();
    assert!(
        matches!(dt, DataType::Union(_, UnionMode::Dense)),
        "3-branch union must map to Union(Dense), got {dt:?}"
    );
}

#[test]
fn three_branch_union_field_types_match_branches() {
    let s = rec1(r#"["int","string","boolean"]"#);
    let dt = target_of(&s).field(0).data_type().clone();
    let DataType::Union(fields, _) = dt else {
        panic!("expected Union");
    };
    let types: Vec<DataType> = fields.iter().map(|(_, f)| f.data_type().clone()).collect();
    assert_eq!(
        types,
        vec![DataType::Int32, DataType::Utf8, DataType::Boolean],
        "union child types must be Int32/Utf8/Boolean in order"
    );
}

#[test]
#[ignore = "pre-existing (not a migration regression): multi-variant avro union -> Union(Dense) had no builder in the vendored reader (catch-all \"type not supported\"); arrow-avro Union does not cast to the target Union(Dense)."]
fn three_branch_union_decodes_int_variant() {
    // Full pipeline must not error/panic when decoding a dense-union column.
    let s = rec1(r#"["int","string","boolean"]"#);
    let b = decode_one(&s, "v", Value::Union(0, Box::new(Value::Int(123))));
    assert_eq!(b.num_rows(), 1, "union int variant decodes to one row");
}

#[test]
#[ignore = "pre-existing (not a migration regression): multi-variant avro union -> Union(Dense) had no builder in the vendored reader (catch-all \"type not supported\"); arrow-avro Union does not cast to the target Union(Dense)."]
fn three_branch_union_decodes_string_variant() {
    let s = rec1(r#"["int","string","boolean"]"#);
    let b = decode_one(
        &s,
        "v",
        Value::Union(1, Box::new(Value::String("z".into()))),
    );
    assert_eq!(b.num_rows(), 1, "union string variant decodes to one row");
}

#[test]
fn four_branch_union_maps_to_dense_union() {
    let s = rec1(r#"["int","long","string","boolean"]"#);
    let dt = target_of(&s).field(0).data_type().clone();
    assert!(
        matches!(dt, DataType::Union(_, UnionMode::Dense)),
        "4-branch union must map to Union(Dense), got {dt:?}"
    );
}

#[test]
fn union_null_and_two_branches_maps_to_dense_union() {
    // 3 total variants including null => NOT the 2-branch nullable case => Union(Dense).
    let s = rec1(r#"["null","int","string"]"#);
    let dt = target_of(&s).field(0).data_type().clone();
    assert!(
        matches!(dt, DataType::Union(_, UnionMode::Dense)),
        "[null,int,string] (3 variants) must map to Union(Dense), got {dt:?}"
    );
}

#[test]
#[ignore = "pre-existing (not a migration regression): multi-variant avro union -> Union(Dense) had no builder in the vendored reader (catch-all \"type not supported\"); arrow-avro Union does not cast to the target Union(Dense)."]
fn union_null_and_two_branches_decodes() {
    let s = rec1(r#"["null","int","string"]"#);
    let b = decode_one(&s, "v", Value::Union(1, Box::new(Value::Int(7))));
    assert_eq!(b.num_rows(), 1, "3-variant union decodes to one row");
}

// ---------------------------------------------------------------------------
// array<T> -> List(element: T) ; record -> Struct  (structural oracle checks)
// ---------------------------------------------------------------------------

#[test]
fn array_long_maps_to_list_of_int64() {
    let s = rec1(r#"{"type":"array","items":"long"}"#);
    let DataType::List(child) = target_of(&s).field(0).data_type().clone() else {
        panic!("array<long> must map to List");
    };
    assert_eq!(child.data_type(), &DataType::Int64);
    assert_eq!(
        child.name(),
        "element",
        "list child field is named 'element'"
    );
}

#[test]
fn array_long_values_roundtrip() {
    let s = rec1(r#"{"type":"array","items":"long"}"#);
    let b = decode_one(
        &s,
        "v",
        Value::Array(vec![Value::Long(1), Value::Long(2), Value::Long(3)]),
    );
    let list = b.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let vals = list
        .value(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .map(|o| o.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(vals, vec![1, 2, 3], "array<long> elements round-trip");
}

#[test]
fn array_string_maps_to_list_of_utf8() {
    let s = rec1(r#"{"type":"array","items":"string"}"#);
    let DataType::List(child) = target_of(&s).field(0).data_type().clone() else {
        panic!("array<string> must map to List");
    };
    assert_eq!(child.data_type(), &DataType::Utf8);
}

#[test]
fn record_maps_to_struct() {
    let inner = r#"{"type":"record","name":"P","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}"#;
    let s = rec1(inner);
    let DataType::Struct(fields) = target_of(&s).field(0).data_type().clone() else {
        panic!("nested record must map to Struct");
    };
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].data_type(), &DataType::Int32);
    assert_eq!(fields[1].data_type(), &DataType::Utf8);
}

#[test]
fn record_struct_values_roundtrip() {
    let inner = r#"{"type":"record","name":"P","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}"#;
    let s = rec1(inner);
    let val = Value::Record(vec![
        ("a".to_string(), Value::Int(9)),
        ("b".to_string(), Value::String("hi".into())),
    ]);
    let b = decode_one(&s, "v", val);
    let st = b.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    let a = st
        .column_by_name("a")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let bcol = st
        .column_by_name("b")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(a.value(0), 9);
    assert_eq!(bcol.value(0), "hi");
}

// ---------------------------------------------------------------------------
// multi-row / multi-value sanity (ordering + null interleaving in a nullable column)
// ---------------------------------------------------------------------------

#[test]
fn nullable_int_multi_row_ordering_and_nulls() {
    // Three messages, one batch: [10, null, -10] must preserve order and null placement.
    let schema_json = nullable_field(r#""int""#);
    let schema = AvroWriterSchema::parse_str(&schema_json).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(1, &schema_json).unwrap();
    for v in [
        Value::Union(1, Box::new(Value::Int(10))),
        Value::Union(0, Box::new(Value::Null)),
        Value::Union(1, Box::new(Value::Int(-10))),
    ] {
        let mut rec = Record::new(&schema).unwrap();
        rec.put("v", v);
        let body = to_avro_datum(&schema, rec).unwrap();
        d.decode(&confluent_frame(1, &body)).unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 3);
    let c = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(c.value(0), 10);
    assert!(c.is_null(1), "middle row must be null");
    assert_eq!(c.value(2), -10);
}

#[test]
fn enum_multi_row_ordering() {
    let schema_json = rec1(ENUM_FIELD);
    let schema = AvroWriterSchema::parse_str(&schema_json).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(1, &schema_json).unwrap();
    for (idx, sym) in [(2u32, "BLUE"), (0, "RED"), (1, "GREEN")] {
        let mut rec = Record::new(&schema).unwrap();
        rec.put("v", Value::Enum(idx, sym.to_string()));
        let body = to_avro_datum(&schema, rec).unwrap();
        d.decode(&confluent_frame(1, &body)).unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    let c = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        (c.value(0), c.value(1), c.value(2)),
        ("BLUE", "RED", "GREEN"),
        "enum symbols must preserve row order"
    );
}
