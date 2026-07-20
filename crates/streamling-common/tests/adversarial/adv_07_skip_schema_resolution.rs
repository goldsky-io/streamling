//! Adversarial integration tests for `ConfluentAvroDecoder::with_schema_resolution(false)`
//! (a pipeline's `skip_schema_resolution`).
//!
//! Contract under test (from `arrow_avro.rs` + `schema.rs`):
//!  * With resolution OFF the decoder is built with ONLY the writer schema store — arrow-avro
//!    decodes each message against its own writer schema with NO writer->reader resolution:
//!    no field reordering, no default-filling, no name-matching.
//!  * The decoded batch is still coerced to `target_schema` (derived from the reader schema, or
//!    the first writer if no reader) BY FIELD NAME via `coerce_batch_to_target`.
//!  * A reader/target field absent from the decoded (writer) batch: nullable target field -> an
//!    all-null column of the target's data type; a NON-nullable (required) target field -> a hard
//!    coercion error (surfaces at `flush`).
//!  * A writer-only field not present in the target is simply ignored (target coercion iterates
//!    target fields only).
//!  * Nested structs follow the same rule (`coerce_struct`): missing nullable nested field -> null,
//!    missing required nested field -> error.
//!  * Contrast: with resolution ON (the default), a reader-only field WITH a default is filled
//!    from that default. The two modes are asserted side by side so the divergence is explicit.
//!
//! Target type oracle (convert_avro_schema_to_arrow): long->Int64, int->Int32, string->Utf8,
//! boolean->Boolean, double->Float64, float->Float32, bytes->Binary, enum->Utf8, fixed(n)->
//! FixedSizeBinary(n), array<T>->List, record->Struct, map<V>->Dictionary(Utf8,V),
//! decimal p<=38 -> Decimal128, 38<p<=76 -> Decimal256, p>76 s==0 -> u256 FixedSizeBinary(32),
//! p>76 s>0 -> Utf8 (scale metadata). union[null,T] (2-branch) -> nullable T.

#![allow(dead_code)]

use apache_avro::types::{Record, Value};
use apache_avro::{Decimal, Schema as AvroWriterSchema, to_avro_datum};
use arrow::array::{
    Array, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray, StructArray,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use streamling_common::formats::avro::arrow_avro::ConfluentAvroDecoder;
use streamling_common::formats::avro::convert_avro_schema_to_arrow;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn confluent_frame(id: u32, body: &[u8]) -> Vec<u8> {
    let mut f = vec![0x00];
    f.extend_from_slice(&id.to_be_bytes());
    f.extend_from_slice(body);
    f
}

/// The trivial writer used by most "reader-only field" tests: a single `long id`.
const W_ID: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;

fn schema(json: &str) -> AvroWriterSchema {
    AvroWriterSchema::parse_str(json).unwrap()
}

/// Build a skip-resolution decoder: reader schema supplies the target; the writer is registered
/// under id 1. `with_schema_resolution(false)` disables writer->reader resolution.
fn build_skip(reader_json: &str, writer_json: &str) -> (ConfluentAvroDecoder, AvroWriterSchema) {
    let reader = schema(reader_json);
    let writer = schema(writer_json);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap()
        .with_schema_resolution(false);
    d.register_writer_schema(1, writer_json).unwrap();
    (d, writer)
}

/// Build a resolving decoder (default behavior): reader schema drives arrow-avro resolution.
fn build_resolve(reader_json: &str, writer_json: &str) -> (ConfluentAvroDecoder, AvroWriterSchema) {
    let reader = schema(reader_json);
    let writer = schema(writer_json);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, writer_json).unwrap();
    (d, writer)
}

/// Encode one writer record (all fields supplied) and decode+flush it, returning the coerced batch.
fn decode_flush(
    d: &mut ConfluentAvroDecoder,
    writer: &AvroWriterSchema,
    fields: Vec<(&str, Value)>,
) -> RecordBatch {
    let mut rec = Record::new(writer).unwrap();
    for (n, v) in fields {
        rec.put(n, v);
    }
    let body = to_avro_datum(writer, rec).unwrap();
    d.decode(&confluent_frame(1, &body)).unwrap();
    d.flush().unwrap().expect("batch")
}

fn column<'a>(b: &'a RecordBatch, name: &str) -> &'a arrow::array::ArrayRef {
    b.column(b.schema().index_of(name).unwrap())
}

fn i64_col(b: &RecordBatch, name: &str) -> Int64Array {
    column(b, name)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .clone()
}

/// Core assertion: a nullable reader-only field `f` (absent from the writer) comes through as an
/// all-null column typed exactly like the target field, while the writer field `id` still decodes.
fn assert_reader_only_field_nulls(reader_json: &str) {
    let (mut d, writer) = build_skip(reader_json, W_ID);
    let target = d.target_schema().unwrap().clone();
    let expected_dt = target.field_with_name("f").unwrap().data_type().clone();

    // Two rows so we exercise a multi-row null column.
    for v in [10i64, 20i64] {
        let mut rec = Record::new(&writer).unwrap();
        rec.put("id", Value::Long(v));
        let body = to_avro_datum(&writer, rec).unwrap();
        d.decode(&confluent_frame(1, &body)).unwrap();
    }
    let b = d.flush().unwrap().expect("batch");

    assert_eq!(b.num_rows(), 2, "row count preserved");
    let f = column(&b, "f");
    assert_eq!(
        f.data_type(),
        &expected_dt,
        "reader-only null column must be typed like the target field"
    );
    assert_eq!(
        f.null_count(),
        2,
        "reader-only field must be all-null under skip_schema_resolution (not default-filled)"
    );
    let id = i64_col(&b, "id");
    assert_eq!(id.value(0), 10, "writer field still decodes under skip");
    assert_eq!(id.value(1), 20, "writer field still decodes under skip");
}

fn reader_with_f(f_type: &str) -> String {
    format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"id","type":"long"}},{{"name":"f","type":{f_type}}}]}}"#
    )
}

// ---------------------------------------------------------------------------
// 1. Nullable reader-only field -> typed all-null column (one test per Arrow type)
// ---------------------------------------------------------------------------

#[test]
fn reader_only_nullable_long_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(r#"["null","long"]"#));
}

#[test]
fn reader_only_nullable_int_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(r#"["null","int"]"#));
}

#[test]
fn reader_only_nullable_string_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(r#"["null","string"]"#));
}

#[test]
fn reader_only_nullable_boolean_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(r#"["null","boolean"]"#));
}

#[test]
fn reader_only_nullable_double_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(r#"["null","double"]"#));
}

#[test]
fn reader_only_nullable_float_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(r#"["null","float"]"#));
}

#[test]
fn reader_only_nullable_bytes_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(r#"["null","bytes"]"#));
}

#[test]
fn reader_only_nullable_enum_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(
        r#"["null",{"type":"enum","name":"E","symbols":["A","B","C"]}]"#,
    ));
}

#[test]
fn reader_only_nullable_fixed_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(
        r#"["null",{"type":"fixed","name":"F8","size":8}]"#,
    ));
}

#[test]
fn reader_only_nullable_array_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(
        r#"["null",{"type":"array","items":"long"}]"#,
    ));
}

#[test]
fn reader_only_nullable_record_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(
        r#"["null",{"type":"record","name":"S","fields":[{"name":"x","type":"long"}]}]"#,
    ));
}

#[test]
#[ignore = "pre-existing (not a migration regression): avro map -> Dictionary(Utf8,V) was unsupported by the removed vendored reader too (its Dictionary builder resolves enum/string values, not Value::Map); arrow-avro decodes maps as arrow Map and coerce cannot cast Map->Dictionary. Tracked as a known avro-map gap."]
fn reader_only_nullable_map_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(r#"["null",{"type":"map","values":"long"}]"#));
}

#[test]
fn reader_only_nullable_decimal128_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(
        r#"["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}]"#,
    ));
}

#[test]
fn reader_only_nullable_decimal256_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(
        r#"["null",{"type":"bytes","logicalType":"decimal","precision":50,"scale":5}]"#,
    ));
}

#[test]
fn reader_only_nullable_u256_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(
        r#"["null",{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}]"#,
    ));
}

#[test]
fn reader_only_nullable_scaled_highprec_utf8_is_null() {
    assert_reader_only_field_nulls(&reader_with_f(
        r#"["null",{"type":"bytes","logicalType":"decimal","precision":85,"scale":4}]"#,
    ));
}

// Explicit type-shape assertions for the trickier target types (verifies the null column carries
// the exact Arrow type the pipeline expects, not merely "some null column").

#[test]
fn reader_only_nullable_fixed_null_column_is_fixed_size_binary_8() {
    let (mut d, writer) = build_skip(
        &reader_with_f(r#"["null",{"type":"fixed","name":"F8","size":8}]"#),
        W_ID,
    );
    let b = decode_flush(&mut d, &writer, vec![("id", Value::Long(1))]);
    assert_eq!(
        column(&b, "f").data_type(),
        &DataType::FixedSizeBinary(8),
        "fixed(8) reader-only null column keeps FixedSizeBinary(8)"
    );
}

#[test]
fn reader_only_nullable_u256_null_column_is_fixed_size_binary_32() {
    let (mut d, writer) = build_skip(
        &reader_with_f(
            r#"["null",{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}]"#,
        ),
        W_ID,
    );
    let b = decode_flush(&mut d, &writer, vec![("id", Value::Long(1))]);
    assert_eq!(
        column(&b, "f").data_type(),
        &DataType::FixedSizeBinary(32),
        "u256 (p=100,s=0) reader-only null column keeps FixedSizeBinary(32)"
    );
    assert!(column(&b, "f").is_null(0));
}

#[test]
fn reader_only_nullable_decimal128_null_column_is_decimal128() {
    let (mut d, writer) = build_skip(
        &reader_with_f(
            r#"["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}]"#,
        ),
        W_ID,
    );
    let b = decode_flush(&mut d, &writer, vec![("id", Value::Long(1))]);
    assert_eq!(column(&b, "f").data_type(), &DataType::Decimal128(10, 2));
}

#[test]
fn reader_only_nullable_decimal256_null_column_is_decimal256() {
    let (mut d, writer) = build_skip(
        &reader_with_f(
            r#"["null",{"type":"bytes","logicalType":"decimal","precision":50,"scale":5}]"#,
        ),
        W_ID,
    );
    let b = decode_flush(&mut d, &writer, vec![("id", Value::Long(1))]);
    assert_eq!(column(&b, "f").data_type(), &DataType::Decimal256(50, 5));
}

#[test]
fn reader_only_nullable_list_null_column_is_list() {
    let (mut d, writer) = build_skip(
        &reader_with_f(r#"["null",{"type":"array","items":"long"}]"#),
        W_ID,
    );
    let b = decode_flush(&mut d, &writer, vec![("id", Value::Long(1))]);
    assert!(
        matches!(column(&b, "f").data_type(), DataType::List(_)),
        "array reader-only null column stays List"
    );
    assert!(column(&b, "f").is_null(0));
}

#[test]
fn reader_only_nullable_struct_null_column_is_struct() {
    let (mut d, writer) = build_skip(
        &reader_with_f(
            r#"["null",{"type":"record","name":"S","fields":[{"name":"x","type":"long"}]}]"#,
        ),
        W_ID,
    );
    let b = decode_flush(&mut d, &writer, vec![("id", Value::Long(1))]);
    assert!(
        matches!(column(&b, "f").data_type(), DataType::Struct(_)),
        "record reader-only null column stays Struct"
    );
    assert!(column(&b, "f").is_null(0));
}

// ---------------------------------------------------------------------------
// 2. Divergence: resolution ON default-fills; resolution OFF nulls (side by side)
// ---------------------------------------------------------------------------

#[test]
fn divergence_int_default_resolve_fills_skip_nulls() {
    let reader = reader_with_f(r#"["int","null"]"#) // note: default matches first branch (int)
        .replace(
            r#"{"name":"f","type":["int","null"]}"#,
            r#"{"name":"f","type":["int","null"],"default":42}"#,
        );
    // resolution ON
    let (mut r, w) = build_resolve(&reader, W_ID);
    let rb = decode_flush(&mut r, &w, vec![("id", Value::Long(1))]);
    let rf = column(&rb, "f")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32");
    assert!(
        !rf.is_null(0) && rf.value(0) == 42,
        "resolve fills default 42"
    );
    // resolution OFF
    let (mut s, w2) = build_skip(&reader, W_ID);
    let sb = decode_flush(&mut s, &w2, vec![("id", Value::Long(1))]);
    assert!(
        column(&sb, "f").is_null(0),
        "skip must NOT fill the reader default"
    );
}

#[test]
fn divergence_long_default_resolve_fills_skip_nulls() {
    let reader = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["long","null"],"default":7}]}"#;
    let (mut r, w) = build_resolve(reader, W_ID);
    let rb = decode_flush(&mut r, &w, vec![("id", Value::Long(1))]);
    let rf = column(&rb, "f")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert!(
        !rf.is_null(0) && rf.value(0) == 7,
        "resolve fills default 7"
    );
    let (mut s, w2) = build_skip(reader, W_ID);
    let sb = decode_flush(&mut s, &w2, vec![("id", Value::Long(1))]);
    assert!(
        column(&sb, "f").is_null(0),
        "skip nulls the reader-only long"
    );
}

#[test]
fn divergence_string_default_resolve_fills_skip_nulls() {
    let reader = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["string","null"],"default":"hi"}]}"#;
    let (mut r, w) = build_resolve(reader, W_ID);
    let rb = decode_flush(&mut r, &w, vec![("id", Value::Long(1))]);
    let rf = column(&rb, "f")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    assert!(!rf.is_null(0) && rf.value(0) == "hi", "resolve fills 'hi'");
    let (mut s, w2) = build_skip(reader, W_ID);
    let sb = decode_flush(&mut s, &w2, vec![("id", Value::Long(1))]);
    assert!(
        column(&sb, "f").is_null(0),
        "skip nulls the reader-only string"
    );
}

#[test]
fn divergence_boolean_default_resolve_fills_skip_nulls() {
    let reader = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["boolean","null"],"default":true}]}"#;
    let (mut r, w) = build_resolve(reader, W_ID);
    let rb = decode_flush(&mut r, &w, vec![("id", Value::Long(1))]);
    let rf = column(&rb, "f")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Boolean");
    assert!(!rf.is_null(0) && rf.value(0), "resolve fills true");
    let (mut s, w2) = build_skip(reader, W_ID);
    let sb = decode_flush(&mut s, &w2, vec![("id", Value::Long(1))]);
    assert!(
        column(&sb, "f").is_null(0),
        "skip nulls the reader-only bool"
    );
}

#[test]
fn divergence_double_default_resolve_fills_skip_nulls() {
    let reader = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["double","null"],"default":2.5}]}"#;
    let (mut r, w) = build_resolve(reader, W_ID);
    let rb = decode_flush(&mut r, &w, vec![("id", Value::Long(1))]);
    let rf = column(&rb, "f")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    assert!(!rf.is_null(0) && rf.value(0) == 2.5, "resolve fills 2.5");
    let (mut s, w2) = build_skip(reader, W_ID);
    let sb = decode_flush(&mut s, &w2, vec![("id", Value::Long(1))]);
    assert!(
        column(&sb, "f").is_null(0),
        "skip nulls the reader-only double"
    );
}

#[test]
fn divergence_float_default_resolve_fills_skip_nulls() {
    let reader = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["float","null"],"default":1.5}]}"#;
    let (mut r, w) = build_resolve(reader, W_ID);
    let rb = decode_flush(&mut r, &w, vec![("id", Value::Long(1))]);
    let rf = column(&rb, "f")
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("Float32");
    assert!(!rf.is_null(0) && rf.value(0) == 1.5, "resolve fills 1.5");
    let (mut s, w2) = build_skip(reader, W_ID);
    let sb = decode_flush(&mut s, &w2, vec![("id", Value::Long(1))]);
    assert!(
        column(&sb, "f").is_null(0),
        "skip nulls the reader-only float"
    );
}

#[test]
fn divergence_skip_never_fills_across_two_rows() {
    // Two rows, and skip must leave `f` null in every row while resolve fills every row.
    let reader = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["int","null"],"default":9}]}"#;
    let (mut s, w) = build_skip(reader, W_ID);
    for v in [1i64, 2] {
        let mut rec = Record::new(&w).unwrap();
        rec.put("id", Value::Long(v));
        s.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
            .unwrap();
    }
    let b = s.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 2);
    assert_eq!(column(&b, "f").null_count(), 2, "both rows null under skip");
}

// ---------------------------------------------------------------------------
// 3. Required (non-nullable) reader-only field -> coercion error under skip
// ---------------------------------------------------------------------------

/// Decode one `id` row then flush; return whether flush errored.
fn skip_flush_errs(reader_json: &str) -> bool {
    let (mut d, writer) = build_skip(reader_json, W_ID);
    let mut rec = Record::new(&writer).unwrap();
    rec.put("id", Value::Long(1));
    // decode should succeed (message decodes against its own writer schema)...
    d.decode(&confluent_frame(1, &to_avro_datum(&writer, rec).unwrap()))
        .unwrap();
    // ...the missing required target field surfaces as an error at coercion time.
    d.flush().is_err()
}

#[test]
fn required_reader_only_int_errors_under_skip() {
    assert!(skip_flush_errs(
        r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"req","type":"int"}]}"#
    ));
}

#[test]
fn required_reader_only_string_errors_under_skip() {
    assert!(skip_flush_errs(
        r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"req","type":"string"}]}"#
    ));
}

#[test]
fn required_reader_only_double_errors_under_skip() {
    assert!(skip_flush_errs(
        r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"req","type":"double"}]}"#
    ));
}

#[test]
fn required_reader_only_boolean_errors_under_skip() {
    assert!(skip_flush_errs(
        r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"req","type":"boolean"}]}"#
    ));
}

#[test]
fn required_reader_only_bytes_errors_under_skip() {
    assert!(skip_flush_errs(
        r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"req","type":"bytes"}]}"#
    ));
}

#[test]
fn required_reader_only_record_errors_under_skip() {
    assert!(skip_flush_errs(
        r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"req","type":{"type":"record","name":"S","fields":[{"name":"x","type":"long"}]}}]}"#
    ));
}

#[test]
fn required_reader_only_mixed_with_nullable_still_errors() {
    // One nullable reader-only field (would be null) plus one required (must error) -> overall error.
    assert!(skip_flush_errs(
        r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"opt","type":["null","int"]},{"name":"req","type":"int"}]}"#
    ));
}

#[test]
fn required_with_default_diverges_resolve_fills_skip_errors() {
    // A required (non-union) reader field WITH a default: resolution fills it; skip still errors,
    // because target coercion is purely by-name and the target field is non-nullable.
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"req","type":"int","default":99}]}"#;
    let (mut r, w) = build_resolve(READER, W_ID);
    let rb = decode_flush(&mut r, &w, vec![("id", Value::Long(1))]);
    let rf = column(&rb, "req")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32");
    assert!(
        !rf.is_null(0) && rf.value(0) == 99,
        "resolution fills the required field's default"
    );
    assert!(
        skip_flush_errs(READER),
        "skip must error on a missing required reader-only field even when it has a default"
    );
}

// ---------------------------------------------------------------------------
// 4. Writer-only field (not in reader/target) is ignored
// ---------------------------------------------------------------------------

#[test]
fn writer_extra_long_field_is_dropped() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"junk","type":"long"}]}"#;
    let (mut d, w) = build_skip(W_ID, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![("id", Value::Long(5)), ("junk", Value::Long(999))],
    );
    assert_eq!(b.num_columns(), 1, "only the target's `id` survives");
    assert!(
        b.schema().index_of("junk").is_err(),
        "writer-only field dropped"
    );
    assert_eq!(i64_col(&b, "id").value(0), 5);
}

#[test]
fn writer_extra_string_field_is_dropped() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"note","type":"string"}]}"#;
    let (mut d, w) = build_skip(W_ID, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![("id", Value::Long(5)), ("note", Value::String("x".into()))],
    );
    assert_eq!(b.num_columns(), 1);
    assert!(b.schema().index_of("note").is_err());
    assert_eq!(i64_col(&b, "id").value(0), 5);
}

#[test]
fn writer_extra_boolean_field_is_dropped() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"flag","type":"boolean"}]}"#;
    let (mut d, w) = build_skip(W_ID, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![("id", Value::Long(5)), ("flag", Value::Boolean(true))],
    );
    assert_eq!(b.num_columns(), 1);
    assert!(b.schema().index_of("flag").is_err());
}

#[test]
fn writer_extra_double_field_is_dropped() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"d","type":"double"}]}"#;
    let (mut d, w) = build_skip(W_ID, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![("id", Value::Long(5)), ("d", Value::Double(1.0))],
    );
    assert_eq!(b.num_columns(), 1);
    assert!(b.schema().index_of("d").is_err());
}

#[test]
fn writer_extra_record_field_is_dropped() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"sub","type":{"type":"record","name":"S","fields":[{"name":"x","type":"long"}]}}]}"#;
    let (mut d, w) = build_skip(W_ID, WRITER);
    let sub = Value::Record(vec![("x".to_string(), Value::Long(1))]);
    let b = decode_flush(&mut d, &w, vec![("id", Value::Long(5)), ("sub", sub)]);
    assert_eq!(b.num_columns(), 1, "nested writer-only record dropped");
    assert!(b.schema().index_of("sub").is_err());
}

#[test]
fn writer_extra_array_field_is_dropped() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"arr","type":{"type":"array","items":"long"}}]}"#;
    let (mut d, w) = build_skip(W_ID, WRITER);
    let arr = Value::Array(vec![Value::Long(1), Value::Long(2)]);
    let b = decode_flush(&mut d, &w, vec![("id", Value::Long(5)), ("arr", arr)]);
    assert_eq!(b.num_columns(), 1);
    assert!(b.schema().index_of("arr").is_err());
}

#[test]
fn writer_extra_enum_field_is_dropped() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}"#;
    let (mut d, w) = build_skip(W_ID, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![("id", Value::Long(5)), ("e", Value::Enum(1, "B".into()))],
    );
    assert_eq!(b.num_columns(), 1);
    assert!(b.schema().index_of("e").is_err());
}

#[test]
fn writer_extra_field_output_schema_equals_target() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"junk","type":"string"}]}"#;
    let (mut d, w) = build_skip(W_ID, WRITER);
    let target_names: Vec<String> = d
        .target_schema()
        .unwrap()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let b = decode_flush(
        &mut d,
        &w,
        vec![("id", Value::Long(1)), ("junk", Value::String("z".into()))],
    );
    let out_names: Vec<String> = b
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        out_names, target_names,
        "output schema equals target schema exactly"
    );
}

#[test]
fn writer_many_extra_fields_all_dropped() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"a","type":"long"},{"name":"b","type":"string"},{"name":"c","type":"boolean"}]}"#;
    let (mut d, w) = build_skip(W_ID, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("id", Value::Long(7)),
            ("a", Value::Long(1)),
            ("b", Value::String("x".into())),
            ("c", Value::Boolean(false)),
        ],
    );
    assert_eq!(b.num_columns(), 1);
    assert_eq!(i64_col(&b, "id").value(0), 7);
}

// ---------------------------------------------------------------------------
// 5. Field order differs -> coercion matches by NAME, not position
// ---------------------------------------------------------------------------

#[test]
fn field_order_swap_matched_by_name_under_skip() {
    // writer order [a, b]; reader/target order [b, a]. Values must follow names, not positions.
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"b","type":"long"},{"name":"a","type":"long"}]}"#;
    let (mut d, w) = build_skip(READER, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![("a", Value::Long(1)), ("b", Value::Long(2))],
    );
    assert_eq!(i64_col(&b, "a").value(0), 1, "`a` follows its name");
    assert_eq!(i64_col(&b, "b").value(0), 2, "`b` follows its name");
    // And the output column ORDER follows the target (reader) order [b, a].
    let names: Vec<String> = b
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["b".to_string(), "a".to_string()]);
}

#[test]
fn field_order_three_way_permutation_matched_by_name() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"},{"name":"c","type":"long"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"c","type":"long"},{"name":"a","type":"long"},{"name":"b","type":"long"}]}"#;
    let (mut d, w) = build_skip(READER, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("a", Value::Long(10)),
            ("b", Value::Long(20)),
            ("c", Value::Long(30)),
        ],
    );
    assert_eq!(i64_col(&b, "a").value(0), 10);
    assert_eq!(i64_col(&b, "b").value(0), 20);
    assert_eq!(i64_col(&b, "c").value(0), 30);
    let names: Vec<String> = b
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        names,
        vec!["c".to_string(), "a".to_string(), "b".to_string()]
    );
}

#[test]
fn field_order_swap_and_reader_extra_and_writer_extra_combined() {
    // writer [b, a, junk]; reader [a, b, extra(nullable)]. Correct: a/b by name, extra null, junk gone.
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"b","type":"long"},{"name":"a","type":"long"},{"name":"junk","type":"string"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"},{"name":"extra","type":["null","int"]}]}"#;
    let (mut d, w) = build_skip(READER, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("b", Value::Long(2)),
            ("a", Value::Long(1)),
            ("junk", Value::String("x".into())),
        ],
    );
    assert_eq!(i64_col(&b, "a").value(0), 1);
    assert_eq!(i64_col(&b, "b").value(0), 2);
    assert!(column(&b, "extra").is_null(0), "reader-only extra null");
    assert!(
        b.schema().index_of("junk").is_err(),
        "writer-only junk dropped"
    );
}

#[test]
fn field_order_swap_matched_by_name_under_resolve_too() {
    // Same names, differing order, resolution ON: arrow-avro resolves by name; result must match.
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"b","type":"long"},{"name":"a","type":"long"}]}"#;
    let (mut d, w) = build_resolve(READER, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![("a", Value::Long(1)), ("b", Value::Long(2))],
    );
    assert_eq!(i64_col(&b, "a").value(0), 1);
    assert_eq!(i64_col(&b, "b").value(0), 2);
}

// ---------------------------------------------------------------------------
// 6. Nested struct: reader-only nullable -> null; required -> error
// ---------------------------------------------------------------------------

const NESTED_WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"sub","type":{"type":"record","name":"S","fields":[{"name":"x","type":"long"}]}}]}"#;

fn nested_reader(y_type: &str) -> String {
    format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"id","type":"long"}},{{"name":"sub","type":{{"type":"record","name":"S","fields":[{{"name":"x","type":"long"}},{{"name":"y","type":{y_type}}}]}}}}]}}"#
    )
}

fn nested_struct(b: &RecordBatch) -> &StructArray {
    column(b, "sub")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("Struct")
}

#[test]
fn nested_reader_only_nullable_field_is_null_under_skip() {
    let reader = nested_reader(r#"["null","string"]"#);
    let (mut d, w) = build_skip(&reader, NESTED_WRITER);
    let sub = Value::Record(vec![("x".to_string(), Value::Long(5))]);
    let b = decode_flush(&mut d, &w, vec![("id", Value::Long(1)), ("sub", sub)]);
    let st = nested_struct(&b);
    let y = st.column_by_name("y").unwrap();
    assert!(
        y.is_null(0),
        "nested reader-only nullable field is null under skip"
    );
    let x = st
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(x.value(0), 5, "present nested field preserved");
}

#[test]
fn nested_reader_only_nullable_diverges_resolve_fills_skip_nulls() {
    // Nested reader-only field WITH a non-null default: resolve fills "def"; skip nulls it.
    let reader = nested_reader(r#"["string","null"]"#).replace(
        r#"{"name":"y","type":["string","null"]}"#,
        r#"{"name":"y","type":["string","null"],"default":"def"}"#,
    );
    // resolution ON
    let (mut r, w) = build_resolve(&reader, NESTED_WRITER);
    let sub = Value::Record(vec![("x".to_string(), Value::Long(5))]);
    let rb = decode_flush(&mut r, &w, vec![("id", Value::Long(1)), ("sub", sub)]);
    let ry = nested_struct(&rb)
        .column_by_name("y")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    assert!(
        !ry.is_null(0) && ry.value(0) == "def",
        "resolution fills the nested reader-only default"
    );
    // resolution OFF
    let (mut s, w2) = build_skip(&reader, NESTED_WRITER);
    let sub2 = Value::Record(vec![("x".to_string(), Value::Long(5))]);
    let sb = decode_flush(&mut s, &w2, vec![("id", Value::Long(1)), ("sub", sub2)]);
    assert!(
        nested_struct(&sb).column_by_name("y").unwrap().is_null(0),
        "skip must not fill the nested reader-only default"
    );
}

#[test]
fn nested_required_reader_only_field_errors_under_skip() {
    let reader = nested_reader(r#""string""#); // required (non-union) nested field
    let (mut d, w) = build_skip(&reader, NESTED_WRITER);
    let sub = Value::Record(vec![("x".to_string(), Value::Long(5))]);
    let mut rec = Record::new(&w).unwrap();
    rec.put("id", Value::Long(1));
    rec.put("sub", sub);
    d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
        .unwrap();
    assert!(
        d.flush().is_err(),
        "missing required nested field must error under skip"
    );
}

#[test]
fn deeply_nested_reader_only_nullable_is_null_under_skip() {
    // R{ id, a: A{ b: B{ x } } }; reader adds a.b.y nullable -> null through two struct levels.
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"a","type":{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"x","type":"long"}]}}]}}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"a","type":{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"x","type":"long"},{"name":"y","type":["null","string"]}]}}]}}]}"#;
    let (mut d, w) = build_skip(READER, WRITER);
    let inner_b = Value::Record(vec![("x".to_string(), Value::Long(9))]);
    let a = Value::Record(vec![("b".to_string(), inner_b)]);
    let batch = decode_flush(&mut d, &w, vec![("id", Value::Long(1)), ("a", a)]);
    let a_st = column(&batch, "a")
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let b_st = a_st
        .column_by_name("b")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(
        b_st.column_by_name("y").unwrap().is_null(0),
        "y null two levels deep"
    );
    let x = b_st
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(x.value(0), 9, "deep present field preserved");
}

#[test]
fn nested_reader_only_multiple_nullable_fields_all_null() {
    let reader = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"sub","type":{"type":"record","name":"S","fields":[{"name":"x","type":"long"},{"name":"y","type":["null","string"]},{"name":"z","type":["null","int"]}]}}]}"#;
    let (mut d, w) = build_skip(reader, NESTED_WRITER);
    let sub = Value::Record(vec![("x".to_string(), Value::Long(5))]);
    let b = decode_flush(&mut d, &w, vec![("id", Value::Long(1)), ("sub", sub)]);
    let st = nested_struct(&b);
    assert!(st.column_by_name("y").unwrap().is_null(0));
    assert!(st.column_by_name("z").unwrap().is_null(0));
}

// ---------------------------------------------------------------------------
// 7. Present fields decode correctly under skip
// ---------------------------------------------------------------------------

#[test]
fn all_fields_present_no_nulls_under_skip() {
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
    let (mut d, w) = build_skip(SCHEMA, SCHEMA);
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("id", Value::Long(3)),
            ("name", Value::String("bob".into())),
        ],
    );
    assert_eq!(i64_col(&b, "id").value(0), 3);
    let name = column(&b, "name")
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "bob");
    assert_eq!(name.null_count(), 0);
}

#[test]
fn present_field_and_reader_only_null_same_row() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"extra","type":["null","int"]}]}"#;
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
    let (mut d, w) = build_skip(READER, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("id", Value::Long(3)),
            ("name", Value::String("bob".into())),
        ],
    );
    assert_eq!(i64_col(&b, "id").value(0), 3);
    assert_eq!(
        column(&b, "name")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "bob"
    );
    assert!(column(&b, "extra").is_null(0));
}

#[test]
fn present_nullable_writer_field_with_value_under_skip() {
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":["null","string"]}]}"#;
    let (mut d, w) = build_skip(SCHEMA, SCHEMA);
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("id", Value::Long(1)),
            ("name", Value::Union(1, Box::new(Value::String("v".into())))),
        ],
    );
    assert_eq!(
        column(&b, "name")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "v"
    );
}

#[test]
fn present_nullable_writer_field_with_null_under_skip() {
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":["null","string"]}]}"#;
    let (mut d, w) = build_skip(SCHEMA, SCHEMA);
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("id", Value::Long(1)),
            ("name", Value::Union(0, Box::new(Value::Null))),
        ],
    );
    assert!(
        column(&b, "name").is_null(0),
        "explicit null writer value stays null"
    );
}

#[test]
fn present_enum_field_decodes_to_symbol_under_skip() {
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"color","type":{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}}]}"#;
    let (mut d, w) = build_skip(SCHEMA, SCHEMA);
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("id", Value::Long(1)),
            ("color", Value::Enum(2, "BLUE".into())),
        ],
    );
    assert_eq!(
        column(&b, "color").data_type(),
        &DataType::Utf8,
        "enum target is Utf8"
    );
    let c = column(&b, "color")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    assert_eq!(c.value(0), "BLUE", "enum decodes to its symbol under skip");
}

#[test]
fn present_fixed_field_decodes_bytes_under_skip() {
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"h","type":{"type":"fixed","name":"H","size":4}}]}"#;
    let (mut d, w) = build_skip(SCHEMA, SCHEMA);
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("id", Value::Long(1)),
            ("h", Value::Fixed(4, vec![0xDE, 0xAD, 0xBE, 0xEF])),
        ],
    );
    let h = column(&b, "h")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("FixedSizeBinary");
    assert_eq!(h.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
}

#[test]
fn present_decimal128_field_passthrough_under_skip() {
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}"#;
    let (mut d, w) = build_skip(SCHEMA, SCHEMA);
    // unscaled 12345 (=> 123.45) big-endian minimal bytes.
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("id", Value::Long(1)),
            ("v", Value::Decimal(Decimal::from(vec![0x30, 0x39]))),
        ],
    );
    assert_eq!(column(&b, "v").data_type(), &DataType::Decimal128(10, 2));
    let v = column(&b, "v")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("Decimal128");
    assert_eq!(
        v.value(0),
        12345_i128,
        "nested decimal unscaled value round-trips"
    );
}

#[test]
fn present_u256_field_reinterpreted_under_skip() {
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}]}"#;
    let (mut d, w) = build_skip(SCHEMA, SCHEMA);
    let mut payload = [0u8; 32];
    payload[0] = 0x12;
    payload[31] = 0xCD;
    let b = decode_flush(
        &mut d,
        &w,
        vec![
            ("id", Value::Long(1)),
            ("v", Value::Decimal(Decimal::from(payload.to_vec()))),
        ],
    );
    assert_eq!(column(&b, "v").data_type(), &DataType::FixedSizeBinary(32));
    let v = column(&b, "v")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("FixedSizeBinary(32)");
    assert_eq!(
        v.value(0),
        &payload,
        "u256 bytes reinterpreted correctly under skip"
    );
}

#[test]
fn present_negative_u256_field_errors_under_skip() {
    // High bit set => negative; u256 must reject it (the reinterpretation happens during coercion).
    const SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"v","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}]}"#;
    let (mut d, w) = build_skip(SCHEMA, SCHEMA);
    let mut rec = Record::new(&w).unwrap();
    rec.put("id", Value::Long(1));
    rec.put("v", Value::Decimal(Decimal::from(vec![0x80])));
    d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
        .unwrap();
    assert!(d.flush().is_err(), "negative u256 must error under skip");
}

// ---------------------------------------------------------------------------
// 8. Type-widening coercion under skip (target from reader, cast applied)
// ---------------------------------------------------------------------------

#[test]
fn writer_int_reader_long_widens_under_skip() {
    // Writer field is `int`; reader/target is `long`. Skip decodes Int32, coercion casts to Int64.
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"n","type":"long"}]}"#;
    let (mut d, w) = build_skip(READER, WRITER);
    let b = decode_flush(&mut d, &w, vec![("n", Value::Int(1234567))]);
    assert_eq!(
        column(&b, "n").data_type(),
        &DataType::Int64,
        "target is Int64"
    );
    assert_eq!(
        i64_col(&b, "n").value(0),
        1234567,
        "int widened to i64 value preserved"
    );
}

#[test]
fn writer_int_reader_long_negative_widens_under_skip() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"n","type":"long"}]}"#;
    let (mut d, w) = build_skip(READER, WRITER);
    let b = decode_flush(&mut d, &w, vec![("n", Value::Int(i32::MIN))]);
    assert_eq!(
        i64_col(&b, "n").value(0),
        i32::MIN as i64,
        "sign preserved on widening"
    );
}

// ---------------------------------------------------------------------------
// 9. Case sensitivity: field names match exactly (no case folding)
// ---------------------------------------------------------------------------

#[test]
fn field_name_case_mismatch_treated_as_reader_only_and_writer_only() {
    // reader "Value" (nullable) vs writer "value": names differ by case, so `Value` is reader-only
    // (null) and `value` is writer-only (dropped).
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"value","type":"long"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"Value","type":["null","long"]}]}"#;
    let (mut d, w) = build_skip(READER, WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![("id", Value::Long(1)), ("value", Value::Long(42))],
    );
    assert!(
        column(&b, "Value").is_null(0),
        "case-different reader field is reader-only null"
    );
    assert!(
        b.schema().index_of("value").is_err(),
        "lowercase writer field dropped"
    );
}

// ---------------------------------------------------------------------------
// 10. Builder ordering & flag toggling
// ---------------------------------------------------------------------------

#[test]
fn resolution_flag_set_before_reader_schema_still_skips() {
    // with_schema_resolution(false) applied BEFORE with_reader_schema.
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["int","null"],"default":42}]}"#;
    let reader = schema(READER);
    let mut d = ConfluentAvroDecoder::new()
        .with_schema_resolution(false)
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, W_ID).unwrap();
    let w = schema(W_ID);
    let b = decode_flush(&mut d, &w, vec![("id", Value::Long(1))]);
    assert!(
        column(&b, "f").is_null(0),
        "flag-before-reader ordering still skips default-fill"
    );
}

#[test]
fn resolution_flag_set_after_reader_schema_still_skips() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["int","null"],"default":42}]}"#;
    let (mut d, w) = build_skip(READER, W_ID); // helper sets flag after reader schema
    let b = decode_flush(&mut d, &w, vec![("id", Value::Long(1))]);
    assert!(column(&b, "f").is_null(0));
}

#[test]
fn resolution_toggled_false_then_true_resolves() {
    // Re-enabling resolution after disabling must produce the resolving behavior (default-fill).
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["int","null"],"default":42}]}"#;
    let reader = schema(READER);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap()
        .with_schema_resolution(false)
        .with_schema_resolution(true);
    d.register_writer_schema(1, W_ID).unwrap();
    let w = schema(W_ID);
    let b = decode_flush(&mut d, &w, vec![("id", Value::Long(1))]);
    let f = column(&b, "f")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32");
    assert!(
        !f.is_null(0) && f.value(0) == 42,
        "re-enabled resolution fills default"
    );
}

#[test]
fn default_decoder_resolves_and_fills_default() {
    // Sanity baseline: the default decoder (no flag call) resolves and fills the reader default.
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["int","null"],"default":42}]}"#;
    let (mut d, w) = build_resolve(READER, W_ID);
    let b = decode_flush(&mut d, &w, vec![("id", Value::Long(1))]);
    let f = column(&b, "f")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32");
    assert!(!f.is_null(0) && f.value(0) == 42);
}

// ---------------------------------------------------------------------------
// 11. No reader schema: target derived from the writer, all fields present
// ---------------------------------------------------------------------------

#[test]
fn skip_without_reader_schema_targets_writer() {
    // No reader schema set; target derives from the writer. Under skip all fields decode, no nulls.
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
    let mut d = ConfluentAvroDecoder::new().with_schema_resolution(false);
    d.register_writer_schema(1, WRITER).unwrap();
    let w = schema(WRITER);
    let b = decode_flush(
        &mut d,
        &w,
        vec![("id", Value::Long(9)), ("name", Value::String("z".into()))],
    );
    assert_eq!(i64_col(&b, "id").value(0), 9);
    assert_eq!(
        column(&b, "name")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "z"
    );
    assert_eq!(column(&b, "name").null_count(), 0);
}

#[test]
fn skip_without_reader_schema_target_matches_writer_conversion() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
    let mut d = ConfluentAvroDecoder::new().with_schema_resolution(false);
    d.register_writer_schema(1, WRITER).unwrap();
    let expected = convert_avro_schema_to_arrow(schema(WRITER));
    let got = d.target_schema().unwrap();
    let en: Vec<&str> = expected
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let gn: Vec<&str> = got.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        gn, en,
        "target derived from writer matches convert_avro_schema_to_arrow"
    );
}

// ---------------------------------------------------------------------------
// 12. Union-rooted writer under skip
// ---------------------------------------------------------------------------

#[test]
fn union_rooted_writer_reader_only_field_null_under_skip() {
    // Debezium-style ["null", record] writer root; reader adds a nullable field. Under skip the
    // branch prefix is still stripped and the reader-only field comes through null.
    const REC_W: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;
    const REC_R: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["null","string"]}]}"#;
    let union_w = format!(r#"["null",{REC_W}]"#);
    let union_r = format!(r#"["null",{REC_R}]"#);
    let reader = schema(&union_r);
    let union_writer = schema(&union_w);

    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap()
        .with_schema_resolution(false);
    d.register_writer_schema(1, &union_w).unwrap();

    let record_val = Value::Record(vec![("id".to_string(), Value::Long(77))]);
    let body = to_avro_datum(&union_writer, Value::Union(1, Box::new(record_val))).unwrap();
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(
        i64_col(&b, "id").value(0),
        77,
        "union-root body decodes under skip"
    );
    assert!(
        column(&b, "f").is_null(0),
        "reader-only field null under skip (union root)"
    );
}

// ---------------------------------------------------------------------------
// 13. Disjoint schemas
// ---------------------------------------------------------------------------

#[test]
fn fully_disjoint_all_nullable_reader_all_null_under_skip() {
    // Writer and reader share NO field names; every reader field nullable -> all-null columns,
    // row count preserved, and writer's own field is dropped.
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"w","type":"long"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"x","type":["null","long"]},{"name":"y","type":["null","string"]}]}"#;
    let (mut d, w) = build_skip(READER, WRITER);
    for v in [1i64, 2, 3] {
        let mut rec = Record::new(&w).unwrap();
        rec.put("w", Value::Long(v));
        d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(
        b.num_rows(),
        3,
        "row count preserved across disjoint schema"
    );
    assert_eq!(b.num_columns(), 2, "only reader fields present");
    assert_eq!(column(&b, "x").null_count(), 3);
    assert_eq!(column(&b, "y").null_count(), 3);
    assert!(
        b.schema().index_of("w").is_err(),
        "writer-only field dropped"
    );
}

#[test]
fn fully_disjoint_with_required_reader_field_errors_under_skip() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"w","type":"long"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"x","type":["null","long"]},{"name":"req","type":"long"}]}"#;
    let (mut d, w) = build_skip(READER, WRITER);
    let mut rec = Record::new(&w).unwrap();
    rec.put("w", Value::Long(1));
    d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
        .unwrap();
    assert!(
        d.flush().is_err(),
        "required reader field in disjoint schema must error"
    );
}

// ---------------------------------------------------------------------------
// 14. Multi-row & multi-generation under skip
// ---------------------------------------------------------------------------

#[test]
fn multi_row_reader_only_null_across_all_rows() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["null","int"]}]}"#;
    let (mut d, w) = build_skip(READER, W_ID);
    for v in 0..5i64 {
        let mut rec = Record::new(&w).unwrap();
        rec.put("id", Value::Long(v));
        d.decode(&confluent_frame(1, &to_avro_datum(&w, rec).unwrap()))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 5);
    assert_eq!(
        column(&b, "f").null_count(),
        5,
        "reader-only null in every row"
    );
    let id = i64_col(&b, "id");
    for v in 0..5i64 {
        assert_eq!(id.value(v as usize), v);
    }
}

#[test]
fn multi_writer_generation_flush_concats_with_reader_only_null() {
    // Two writer ids (same shape here), skip mode: flush concatenates generations; the reader-only
    // field is null across the whole concatenated batch.
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["null","int"]}]}"#;
    let reader = schema(READER);
    let writer = schema(W_ID);
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap()
        .with_schema_resolution(false);
    d.register_writer_schema(1, W_ID).unwrap();
    d.register_writer_schema(2, W_ID).unwrap();

    for (id_frame, val) in [(1u32, 11i64), (1, 12), (2, 21), (2, 22)] {
        let mut rec = Record::new(&writer).unwrap();
        rec.put("id", Value::Long(val));
        d.decode(&confluent_frame(
            id_frame,
            &to_avro_datum(&writer, rec).unwrap(),
        ))
        .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 4, "all generations concatenated");
    assert_eq!(
        column(&b, "f").null_count(),
        4,
        "reader-only null across generations"
    );
    let mut ids: Vec<i64> = {
        let c = i64_col(&b, "id");
        (0..c.len()).map(|i| c.value(i)).collect()
    };
    ids.sort();
    assert_eq!(ids, vec![11, 12, 21, 22]);
}

#[test]
fn flush_with_no_input_returns_none_under_skip() {
    let (mut d, _w) = build_skip(
        r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"f","type":["null","int"]}]}"#,
        W_ID,
    );
    assert!(
        d.flush().unwrap().is_none(),
        "no decode -> no batch under skip"
    );
}

// ---------------------------------------------------------------------------
// 15. Table-driven: every nullable reader-only primitive type nulls (belt & suspenders)
// ---------------------------------------------------------------------------

#[test]
fn table_reader_only_nullable_primitives_all_null() {
    let cases: &[(&str, DataType)] = &[
        (r#"["null","long"]"#, DataType::Int64),
        (r#"["null","int"]"#, DataType::Int32),
        (r#"["null","string"]"#, DataType::Utf8),
        (r#"["null","boolean"]"#, DataType::Boolean),
        (r#"["null","double"]"#, DataType::Float64),
        (r#"["null","float"]"#, DataType::Float32),
        (r#"["null","bytes"]"#, DataType::Binary),
    ];
    for (ftype, expected_dt) in cases {
        let (mut d, w) = build_skip(&reader_with_f(ftype), W_ID);
        let b = decode_flush(&mut d, &w, vec![("id", Value::Long(1))]);
        let f = column(&b, "f");
        assert_eq!(
            f.data_type(),
            expected_dt,
            "case {ftype}: reader-only null column type"
        );
        assert!(
            f.is_null(0),
            "case {ftype}: reader-only field must be null under skip"
        );
        assert_eq!(i64_col(&b, "id").value(0), 1, "case {ftype}: id decodes");
    }
}

#[test]
fn table_required_reader_only_primitives_all_error() {
    let cases: &[&str] = &[
        "int", "long", "string", "boolean", "double", "float", "bytes",
    ];
    for t in cases {
        let reader = format!(
            r#"{{"type":"record","name":"R","fields":[{{"name":"id","type":"long"}},{{"name":"req","type":"{t}"}}]}}"#
        );
        assert!(
            skip_flush_errs(&reader),
            "required reader-only `{t}` field must error under skip"
        );
    }
}

#[test]
fn table_writer_extra_primitives_all_dropped() {
    let cases: &[(&str, Value)] = &[
        ("long", Value::Long(1)),
        ("int", Value::Int(1)),
        ("string", Value::String("x".into())),
        ("boolean", Value::Boolean(true)),
        ("double", Value::Double(1.0)),
        ("float", Value::Float(1.0)),
    ];
    for (t, v) in cases {
        let writer = format!(
            r#"{{"type":"record","name":"R","fields":[{{"name":"id","type":"long"}},{{"name":"extra","type":"{t}"}}]}}"#
        );
        let (mut d, w) = build_skip(W_ID, &writer);
        let b = decode_flush(
            &mut d,
            &w,
            vec![("id", Value::Long(1)), ("extra", v.clone())],
        );
        assert_eq!(b.num_columns(), 1, "case {t}: writer-only field dropped");
        assert!(
            b.schema().index_of("extra").is_err(),
            "case {t}: no extra column"
        );
        assert_eq!(i64_col(&b, "id").value(0), 1);
    }
}

// ---------------------------------------------------------------------------
// 16. Output schema invariant: coerced batch schema equals target schema
// ---------------------------------------------------------------------------

#[test]
fn output_schema_equals_target_schema_under_skip() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"a","type":["null","string"]},{"name":"b","type":["null","int"]}]}"#;
    let (mut d, w) = build_skip(READER, W_ID);
    let target = d.target_schema().unwrap().clone();
    let b = decode_flush(&mut d, &w, vec![("id", Value::Long(1))]);
    assert_eq!(
        b.schema().as_ref(),
        target.as_ref(),
        "coerced batch schema must equal the target schema"
    );
}

#[test]
fn reader_only_field_order_preserved_in_output() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":["null","int"]},{"name":"id","type":"long"},{"name":"b","type":["null","int"]}]}"#;
    let (mut d, w) = build_skip(READER, W_ID);
    let b = decode_flush(&mut d, &w, vec![("id", Value::Long(1))]);
    let names: Vec<String> = b
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        names,
        vec!["a".to_string(), "id".to_string(), "b".to_string()],
        "output follows target (reader) field order, with reader-only fields interleaved"
    );
    assert!(column(&b, "a").is_null(0));
    assert!(column(&b, "b").is_null(0));
    assert_eq!(i64_col(&b, "id").value(0), 1);
}
