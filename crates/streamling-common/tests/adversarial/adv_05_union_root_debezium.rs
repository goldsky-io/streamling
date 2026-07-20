//! Adversarial integration tests for UNION-ROOT (Debezium/Confluent) framing and the
//! per-writer-id union-branch index in `ConfluentAvroDecoder`.
//!
//! Focus area: writer schema whose root is a union like `["null", record]`. arrow-avro cannot
//! build a decoder for a union root, so the decoder registers the unwrapped record and strips the
//! leading union-branch varint from each body. These tests exercise:
//!   * branch order permutations ([null,record] index 1, [record,null] index 0, 3+ branches)
//!   * frames that select the null / a non-record branch (must error cleanly)
//!   * malformed / wrong union-branch varints (mismatch, negative, truncated, overflow, empty)
//!   * the per-writer-id index regression: a union-rooted id and a plain-rooted id decoded
//!     interleaved into one batch must each be framed correctly (a global index would strip a
//!     varint off the plain id's bodies and corrupt them)
//!   * union-root records carrying nullable / decimal / u256 / list / struct fields
//!
//! Everything here uses only the public API + listed deps and asserts values derived from the
//! target oracle (`convert_avro_schema_to_arrow`) and the vendored decode contract.

use apache_avro::Decimal;
use apache_avro::types::{Record, Value};
use apache_avro::{Schema as AvroWriterSchema, to_avro_datum};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Decimal256Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
};
use arrow::datatypes::i256;
use arrow_schema::DataType;
use streamling_common::formats::avro::arrow_avro::{AVRO_DECIMAL_SCALE_META, ConfluentAvroDecoder};
use streamling_common::formats::avro::convert_avro_schema_to_arrow;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Confluent frame: 0x00 magic + 4-byte big-endian schema id + avro body.
fn confluent_frame(id: u32, body: &[u8]) -> Vec<u8> {
    let mut f = vec![0x00];
    f.extend_from_slice(&id.to_be_bytes());
    f.extend_from_slice(body);
    f
}

/// Encode an avro `long` (the union-branch index) as a zigzag varint, exactly as it appears on the
/// wire ahead of the record body in a union-rooted message.
fn zigzag_long(n: i64) -> Vec<u8> {
    let mut v = ((n << 1) ^ (n >> 63)) as u64;
    let mut out = Vec::new();
    loop {
        let mut byte = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
    out
}

/// Encode a record body against `rec_json` (the unwrapped record schema).
fn encode_record(rec_json: &str, fields: Vec<(&str, Value)>) -> Vec<u8> {
    let rs = AvroWriterSchema::parse_str(rec_json).unwrap();
    let mut rec = Record::new(&rs).unwrap();
    for (k, v) in fields {
        rec.put(k, v);
    }
    to_avro_datum(&rs, rec).unwrap()
}

/// Full union-message body: zigzag(branch) followed by the record body.
fn union_body(rec_json: &str, branch: i64, fields: Vec<(&str, Value)>) -> Vec<u8> {
    let mut body = zigzag_long(branch);
    body.extend_from_slice(&encode_record(rec_json, fields));
    body
}

/// Decode a single union-rooted message and return the flushed batch.
fn decode_union_one(
    union_json: &str,
    rec_json: &str,
    branch: i64,
    fields: Vec<(&str, Value)>,
) -> arrow::record_batch::RecordBatch {
    let id = 7u32;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, union_json).unwrap();
    let body = union_body(rec_json, branch, fields);
    d.decode(&confluent_frame(id, &body)).unwrap();
    d.flush().unwrap().expect("a batch")
}

// Common record schemas used throughout.
const R_ID_DATA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"}]}"#;
const R_ID: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;

fn i64_col(b: &arrow::record_batch::RecordBatch, name: &str) -> Int64Array {
    let i = b.schema().index_of(name).unwrap();
    b.column(i)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone()
}
fn str_col(b: &arrow::record_batch::RecordBatch, name: &str) -> StringArray {
    let i = b.schema().index_of(name).unwrap();
    b.column(i)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone()
}

// ===========================================================================
// A. Branch-order permutations that MUST decode (record at various indices)
// ===========================================================================

#[test]
fn null_then_record_index1_decodes() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        1,
        vec![
            ("id", Value::Long(42)),
            ("data", Value::String("hi".into())),
        ],
    );
    assert_eq!(b.num_rows(), 1);
    assert_eq!(i64_col(&b, "id").value(0), 42);
    assert_eq!(str_col(&b, "data").value(0), "hi");
}

#[test]
fn record_then_null_index0_decodes() {
    let union = format!("[{R_ID_DATA},\"null\"]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        0,
        vec![
            ("id", Value::Long(7)),
            ("data", Value::String("bye".into())),
        ],
    );
    assert_eq!(i64_col(&b, "id").value(0), 7);
    assert_eq!(str_col(&b, "data").value(0), "bye");
}

#[test]
fn null_int_record_index2_decodes() {
    let union = format!("[\"null\",\"int\",{R_ID_DATA}]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        2,
        vec![("id", Value::Long(3)), ("data", Value::String("x".into()))],
    );
    assert_eq!(i64_col(&b, "id").value(0), 3);
}

#[test]
fn null_record_string_index1_decodes() {
    let union = format!("[\"null\",{R_ID_DATA},\"string\"]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        1,
        vec![("id", Value::Long(9)), ("data", Value::String("y".into()))],
    );
    assert_eq!(i64_col(&b, "id").value(0), 9);
}

#[test]
fn record_null_string_index0_decodes() {
    let union = format!("[{R_ID_DATA},\"null\",\"string\"]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        0,
        vec![("id", Value::Long(11)), ("data", Value::String("z".into()))],
    );
    assert_eq!(i64_col(&b, "id").value(0), 11);
}

#[test]
fn int_record_index1_decodes() {
    let union = format!("[\"int\",{R_ID_DATA}]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        1,
        vec![("id", Value::Long(1)), ("data", Value::String("a".into()))],
    );
    assert_eq!(i64_col(&b, "id").value(0), 1);
}

#[test]
fn record_int_index0_decodes() {
    let union = format!("[{R_ID_DATA},\"int\"]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        0,
        vec![("id", Value::Long(2)), ("data", Value::String("b".into()))],
    );
    assert_eq!(i64_col(&b, "id").value(0), 2);
}

#[test]
fn null_int_string_record_index3_decodes() {
    let union = format!("[\"null\",\"int\",\"string\",{R_ID_DATA}]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        3,
        vec![("id", Value::Long(30)), ("data", Value::String("q".into()))],
    );
    assert_eq!(i64_col(&b, "id").value(0), 30);
}

#[test]
fn null_boolean_int_long_record_index4_decodes() {
    let union = format!("[\"null\",\"boolean\",\"int\",\"long\",{R_ID_DATA}]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        4,
        vec![("id", Value::Long(44)), ("data", Value::String("w".into()))],
    );
    assert_eq!(i64_col(&b, "id").value(0), 44);
}

#[test]
fn record_first_in_five_branch_index0_decodes() {
    let union = format!("[{R_ID_DATA},\"null\",\"int\",\"long\",\"string\"]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        0,
        vec![
            ("id", Value::Long(500)),
            ("data", Value::String("five".into())),
        ],
    );
    assert_eq!(i64_col(&b, "id").value(0), 500);
}

#[test]
fn single_field_record_null_record_decodes() {
    let union = format!("[\"null\",{R_ID}]");
    let b = decode_union_one(&union, R_ID, 1, vec![("id", Value::Long(-1))]);
    assert_eq!(i64_col(&b, "id").value(0), -1);
}

// ===========================================================================
// B. Varint-alignment adversarial cases: exactly ONE branch varint stripped
// ===========================================================================

#[test]
fn strips_exactly_one_varint_first_field_looks_like_branch() {
    // id=1 encodes to 0x02, the same byte as the branch varint for index 1. If the decoder stripped
    // too much, `id` would be corrupted.
    let union = format!("[\"null\",{R_ID_DATA}]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        1,
        vec![("id", Value::Long(1)), ("data", Value::String("k".into()))],
    );
    assert_eq!(i64_col(&b, "id").value(0), 1, "exactly one varint stripped");
    assert_eq!(str_col(&b, "data").value(0), "k");
}

#[test]
fn record_body_starting_with_zero_byte_decodes() {
    // id=0 encodes to 0x00; ensure a leading 0x00 in the record body is not mistaken for framing.
    let union = format!("[\"null\",{R_ID_DATA}]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        1,
        vec![
            ("id", Value::Long(0)),
            ("data", Value::String("zero".into())),
        ],
    );
    assert_eq!(i64_col(&b, "id").value(0), 0);
    assert_eq!(str_col(&b, "data").value(0), "zero");
}

#[test]
fn record_first_index0_body_starting_with_zero() {
    // [record,null]: branch varint is 0x00; record's first field also 0x00 -> two 0x00 bytes.
    let union = format!("[{R_ID_DATA},\"null\"]");
    let b = decode_union_one(
        &union,
        R_ID_DATA,
        0,
        vec![("id", Value::Long(0)), ("data", Value::String("dz".into()))],
    );
    assert_eq!(i64_col(&b, "id").value(0), 0);
    assert_eq!(str_col(&b, "data").value(0), "dz");
}

// ===========================================================================
// C. Frames selecting the NULL or a non-record branch MUST error cleanly
// ===========================================================================

fn decode_err(union_json: &str, id: u32, body: &[u8]) -> bool {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, union_json).unwrap();
    d.decode(&confluent_frame(id, body)).is_err()
}

#[test]
fn null_branch_selected_null_first_errors() {
    // [null,record]: null is index 0, record index 1. A frame with branch 0 must error.
    let union = format!("[\"null\",{R_ID_DATA}]");
    let body = zigzag_long(0); // selects null; no record body follows
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn null_branch_selected_record_first_errors() {
    // [record,null]: null is index 1, record index 0. Branch 1 must error.
    let union = format!("[{R_ID_DATA},\"null\"]");
    let body = zigzag_long(1);
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn int_branch_selected_three_branch_errors() {
    // [null,int,record] record index 2; selecting int (index 1) must error.
    let union = format!("[\"null\",\"int\",{R_ID_DATA}]");
    let mut body = zigzag_long(1);
    body.extend_from_slice(&zigzag_long(123)); // an int payload, never reached
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn string_branch_selected_errors() {
    let union = format!("[\"null\",\"string\",{R_ID_DATA}]");
    let mut body = zigzag_long(1);
    body.extend_from_slice(&zigzag_long(2)); // string len prefix
    body.extend_from_slice(b"ab");
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn record_last_null_branch_errors() {
    let union = format!("[\"null\",\"int\",\"string\",{R_ID_DATA}]");
    let body = zigzag_long(0); // null branch
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn record_first_int_branch_errors() {
    let union = format!("[{R_ID_DATA},\"int\"]");
    let mut body = zigzag_long(1); // int branch
    body.extend_from_slice(&zigzag_long(9));
    assert!(decode_err(&union, 1, &body));
}

// ===========================================================================
// D. Malformed / wrong union-branch varints
// ===========================================================================

#[test]
fn wrong_branch_index_too_high_errors() {
    let union = format!("[\"null\",{R_ID_DATA}]"); // record index 1
    let body = zigzag_long(5); // branch 5 != 1
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn wrong_branch_index_multibyte_varint_errors() {
    // zigzag(64) = 128 -> two-byte varint [0x80,0x01]; must be decoded then rejected (64 != 1).
    let union = format!("[\"null\",{R_ID_DATA}]");
    let body = zigzag_long(64);
    assert_eq!(body, vec![0x80, 0x01], "sanity: multibyte varint");
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn negative_branch_index_errors() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let body = zigzag_long(-1);
    assert_eq!(body, vec![0x01]);
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn large_negative_branch_index_errors() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let body = zigzag_long(-100);
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn truncated_varint_continuation_bit_errors() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let body = [0x80u8]; // continuation bit set, no following byte
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn overflow_varint_errors() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let body = [0x80u8; 12]; // never terminates within 64 bits
    assert!(decode_err(&union, 1, &body));
}

#[test]
fn empty_body_union_errors() {
    // Frame is exactly the 5-byte header; the union path has no varint to read.
    let union = format!("[\"null\",{R_ID_DATA}]");
    assert!(decode_err(&union, 1, &[]));
}

#[test]
fn correct_branch_but_truncated_record_body_does_not_panic() {
    // Correct branch varint, then a record body that is truncated (missing the string field).
    // Must surface as an error (from decode or flush), never a panic.
    let union = format!("[\"null\",{R_ID_DATA}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union).unwrap();
    let mut body = zigzag_long(1);
    body.extend_from_slice(&zigzag_long(5)); // id only; `data` missing
    let r = d.decode(&confluent_frame(1, &body)).and_then(|_| d.flush());
    assert!(r.is_err(), "truncated record must error, not panic");
}

// ===========================================================================
// E. Register-time errors: top-level union with no record branch
// ===========================================================================

#[test]
fn union_without_record_branch_register_errors() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(
        d.register_writer_schema(1, r#"["null","string"]"#).is_err(),
        "union with no record branch is unsupported"
    );
}

#[test]
fn union_null_int_no_record_register_errors() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(d.register_writer_schema(1, r#"["null","int"]"#).is_err());
}

#[test]
fn union_single_null_register_errors() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(d.register_writer_schema(1, r#"["null"]"#).is_err());
}

#[test]
fn union_of_primitives_register_errors() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(
        d.register_writer_schema(1, r#"["int","long","string"]"#)
            .is_err()
    );
}

// ===========================================================================
// F. General framing errors (checked before the union logic)
// ===========================================================================

#[test]
fn frame_shorter_than_five_bytes_errors() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union).unwrap();
    assert!(d.decode(&[0x00, 0x00, 0x00]).is_err());
}

#[test]
fn frame_bad_magic_byte_errors() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union).unwrap();
    // Valid union body but wrong magic byte in front.
    let mut frame = vec![0x01, 0x00, 0x00, 0x00, 0x01];
    frame.extend_from_slice(&union_body(
        R_ID_DATA,
        1,
        vec![("id", Value::Long(1)), ("data", Value::String("m".into()))],
    ));
    assert!(d.decode(&frame).is_err());
}

#[test]
fn decode_unregistered_union_id_errors() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union).unwrap();
    let body = union_body(
        R_ID_DATA,
        1,
        vec![("id", Value::Long(1)), ("data", Value::String("u".into()))],
    );
    // id 999 is not registered; the union-strip map won't touch it and arrow-avro has no schema.
    assert!(d.decode(&confluent_frame(999, &body)).is_err());
}

#[test]
fn has_writer_schema_reflects_registration() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(55, &union).unwrap();
    assert!(d.has_writer_schema(55));
    assert!(!d.has_writer_schema(56));
}

// ===========================================================================
// G. Per-writer-id union-branch index (the global-index regression)
// ===========================================================================

#[test]
fn interleaved_union_and_plain_ids_all_rows_decode() {
    // idA union-rooted [null,R], idB plain-rooted R (same fields -> same target). A global union
    // index would strip a varint off idB's plain bodies and corrupt them.
    let union = format!("[\"null\",{R_ID_DATA}]");
    let id_u = 100u32;
    let id_p = 200u32;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id_u, &union).unwrap(); // first -> establishes target from union
    d.register_writer_schema(id_p, R_ID_DATA).unwrap();

    // Interleave: union, plain, union, plain
    let u1 = union_body(
        R_ID_DATA,
        1,
        vec![("id", Value::Long(1)), ("data", Value::String("a".into()))],
    );
    let p2 = encode_record(
        R_ID_DATA,
        vec![("id", Value::Long(2)), ("data", Value::String("b".into()))],
    );
    let u3 = union_body(
        R_ID_DATA,
        1,
        vec![("id", Value::Long(3)), ("data", Value::String("c".into()))],
    );
    let p4 = encode_record(
        R_ID_DATA,
        vec![("id", Value::Long(4)), ("data", Value::String("d".into()))],
    );
    d.decode(&confluent_frame(id_u, &u1)).unwrap();
    d.decode(&confluent_frame(id_p, &p2)).unwrap();
    d.decode(&confluent_frame(id_u, &u3)).unwrap();
    d.decode(&confluent_frame(id_p, &p4)).unwrap();

    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 4, "all interleaved rows present");
    let ids = i64_col(&b, "id");
    let data = str_col(&b, "data");
    let got: Vec<(i64, String)> = (0..4)
        .map(|i| (ids.value(i), data.value(i).to_string()))
        .collect();
    assert_eq!(
        got,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (3, "c".to_string()),
            (4, "d".to_string())
        ],
        "per-id framing keeps union and plain bodies correct and ordered"
    );
}

#[test]
fn plain_id_body_not_varint_stripped() {
    // A single plain-rooted id whose first field byte (id=1 -> 0x02) resembles a branch varint.
    // With a per-id map (plain id absent), no stripping happens and id decodes as 1.
    let union = format!("[\"null\",{R_ID_DATA}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union).unwrap(); // target from union
    d.register_writer_schema(2, R_ID_DATA).unwrap(); // plain id
    let body = encode_record(
        R_ID_DATA,
        vec![
            ("id", Value::Long(1)),
            ("data", Value::String("plain".into())),
        ],
    );
    d.decode(&confluent_frame(2, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(i64_col(&b, "id").value(0), 1);
    assert_eq!(str_col(&b, "data").value(0), "plain");
}

#[test]
fn two_union_ids_different_branch_indices_interleaved() {
    // idA [null,R] index 1, idB [R,null] index 0 -> different per-id indices, same target.
    let union_a = format!("[\"null\",{R_ID_DATA}]");
    let union_b = format!("[{R_ID_DATA},\"null\"]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union_a).unwrap();
    d.register_writer_schema(2, &union_b).unwrap();
    let a = union_body(
        R_ID_DATA,
        1,
        vec![("id", Value::Long(10)), ("data", Value::String("A".into()))],
    );
    let b = union_body(
        R_ID_DATA,
        0,
        vec![("id", Value::Long(20)), ("data", Value::String("B".into()))],
    );
    d.decode(&confluent_frame(1, &a)).unwrap();
    d.decode(&confluent_frame(2, &b)).unwrap();
    let batch = d.flush().unwrap().expect("batch");
    assert_eq!(batch.num_rows(), 2);
    let ids = i64_col(&batch, "id");
    assert_eq!((ids.value(0), ids.value(1)), (10, 20));
}

#[test]
fn register_plain_after_union_keeps_union_framing() {
    // Register union id first, then a plain id; decoding the union id must still strip its varint.
    let union = format!("[\"null\",{R_ID_DATA}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union).unwrap();
    d.register_writer_schema(2, R_ID_DATA).unwrap();
    let body = union_body(
        R_ID_DATA,
        1,
        vec![("id", Value::Long(77)), ("data", Value::String("s".into()))],
    );
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(i64_col(&b, "id").value(0), 77);
}

#[test]
fn register_union_after_plain_keeps_plain_framing() {
    // Register plain id first (establishes target), then union id; plain id must NOT be stripped.
    let union = format!("[\"null\",{R_ID_DATA}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(2, R_ID_DATA).unwrap();
    d.register_writer_schema(1, &union).unwrap();
    let body = encode_record(
        R_ID_DATA,
        vec![("id", Value::Long(88)), ("data", Value::String("p".into()))],
    );
    d.decode(&confluent_frame(2, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(i64_col(&b, "id").value(0), 88);
    assert_eq!(str_col(&b, "data").value(0), "p");
}

#[test]
fn reregistering_union_id_as_plain_switches_framing() {
    // A single id re-registered from union-rooted to plain-rooted must drop its union index so
    // subsequent bodies are decoded plainly (schema-evolution: framing changed across versions).
    let union = format!("[\"null\",{R_ID}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union).unwrap();
    // Now the same id becomes plain-rooted.
    d.register_writer_schema(1, R_ID).unwrap();
    let body = encode_record(R_ID, vec![("id", Value::Long(321))]);
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(i64_col(&b, "id").value(0), 321);
}

#[test]
fn reregistering_plain_id_as_union_switches_framing() {
    let union = format!("[\"null\",{R_ID}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, R_ID).unwrap();
    d.register_writer_schema(1, &union).unwrap();
    let body = union_body(R_ID, 1, vec![("id", Value::Long(654))]);
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(i64_col(&b, "id").value(0), 654);
}

#[test]
fn many_rows_same_union_id_one_batch() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let id = 5u32;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, &union).unwrap();
    for i in 0..25i64 {
        let body = union_body(
            R_ID_DATA,
            1,
            vec![
                ("id", Value::Long(i)),
                ("data", Value::String(format!("r{i}"))),
            ],
        );
        d.decode(&confluent_frame(id, &body)).unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 25);
    let ids = i64_col(&b, "id");
    assert!((0..25).all(|i| ids.value(i as usize) == i));
}

#[test]
fn three_union_ids_all_interleaved() {
    // Three union-rooted ids with distinct branch orders, all same target, interleaved.
    let u0 = format!("[{R_ID},\"null\"]"); // index 0
    let u1 = format!("[\"null\",{R_ID}]"); // index 1
    let u2 = format!("[\"null\",\"int\",{R_ID}]"); // index 2
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(10, &u1).unwrap();
    d.register_writer_schema(20, &u0).unwrap();
    d.register_writer_schema(30, &u2).unwrap();
    d.decode(&confluent_frame(
        10,
        &union_body(R_ID, 1, vec![("id", Value::Long(1))]),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        20,
        &union_body(R_ID, 0, vec![("id", Value::Long(2))]),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        30,
        &union_body(R_ID, 2, vec![("id", Value::Long(3))]),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        10,
        &union_body(R_ID, 1, vec![("id", Value::Long(4))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 4);
    let ids = i64_col(&b, "id");
    assert_eq!(
        (0..4).map(|i| ids.value(i)).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
}

// ===========================================================================
// H. Union-root records carrying nullable fields
// ===========================================================================

const R_NULLABLE: &str = r#"{"type":"record","name":"R","fields":[
    {"name":"id","type":"long"},
    {"name":"opt","type":["null","string"],"default":null}
]}"#;

#[test]
fn union_record_nullable_field_present() {
    let union = format!("[\"null\",{R_NULLABLE}]");
    let b = decode_union_one(
        &union,
        R_NULLABLE,
        1,
        vec![
            ("id", Value::Long(1)),
            (
                "opt",
                Value::Union(1, Box::new(Value::String("here".into()))),
            ),
        ],
    );
    let opt = str_col(&b, "opt");
    assert!(!opt.is_null(0));
    assert_eq!(opt.value(0), "here");
}

#[test]
fn union_record_nullable_field_null() {
    let union = format!("[\"null\",{R_NULLABLE}]");
    let b = decode_union_one(
        &union,
        R_NULLABLE,
        1,
        vec![
            ("id", Value::Long(2)),
            ("opt", Value::Union(0, Box::new(Value::Null))),
        ],
    );
    assert!(str_col(&b, "opt").is_null(0), "null union branch -> null");
}

#[test]
fn union_record_nullable_is_nullable_in_target() {
    let union = format!("[\"null\",{R_NULLABLE}]");
    let reader = AvroWriterSchema::parse_str(&union).unwrap();
    let target = convert_avro_schema_to_arrow(reader);
    let f = target.field(target.index_of("opt").unwrap());
    assert!(f.is_nullable());
    assert_eq!(f.data_type(), &DataType::Utf8);
}

// ===========================================================================
// I. Union-root records carrying decimal / u256 / scaled-string fields
// ===========================================================================

fn decimal_record(logical: &str) -> String {
    format!(
        r#"{{"type":"record","name":"R","fields":[{{"name":"v","type":{logical}}}]}}"#,
        logical = logical
    )
}

#[test]
fn union_record_decimal128_field() {
    let rec =
        decimal_record(r#"{"type":"bytes","logicalType":"decimal","precision":10,"scale":0}"#);
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(
        &union,
        &rec,
        1,
        vec![("v", Value::Decimal(Decimal::from(vec![0x04, 0xD2])))], // 1234
    );
    let col = b
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("Decimal128");
    assert_eq!(col.value(0), 1234_i128);
}

#[test]
fn union_record_decimal128_scaled_field() {
    let rec =
        decimal_record(r#"{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}"#);
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(
        &union,
        &rec,
        1,
        vec![("v", Value::Decimal(Decimal::from(vec![0x30, 0x39])))], // unscaled 12345
    );
    let col = b
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(col.value(0), 12345_i128);
    assert_eq!(col.data_type(), &DataType::Decimal128(10, 2));
}

#[test]
fn union_record_decimal256_field() {
    let rec =
        decimal_record(r#"{"type":"bytes","logicalType":"decimal","precision":40,"scale":0}"#);
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(
        &union,
        &rec,
        1,
        vec![("v", Value::Decimal(Decimal::from(vec![0x04, 0xD2])))],
    );
    let col = b
        .column(0)
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .expect("Decimal256");
    assert_eq!(col.value(0), i256::from_i128(1234));
    assert_eq!(col.data_type(), &DataType::Decimal256(40, 0));
}

#[test]
fn union_record_u256_field_positive() {
    let rec =
        decimal_record(r#"{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}"#);
    let union = format!("[\"null\",{rec}]");
    let mut payload = [0u8; 32];
    payload[0] = 0x12; // high bit clear -> non-negative
    payload[31] = 0xCD;
    let b = decode_union_one(
        &union,
        &rec,
        1,
        vec![("v", Value::Decimal(Decimal::from(payload.to_vec())))],
    );
    assert_eq!(
        b.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(32)
    );
    let col = b
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("FixedSizeBinary(32)");
    assert_eq!(col.value(0), &payload);
}

#[test]
fn union_record_u256_small_value_zero_extended() {
    let rec =
        decimal_record(r#"{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}"#);
    let union = format!("[\"null\",{rec}]");
    // avro-minimal encoding of 7: single byte 0x07.
    let b = decode_union_one(
        &union,
        &rec,
        1,
        vec![("v", Value::Decimal(Decimal::from(vec![0x07])))],
    );
    let col = b
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let mut expect = [0u8; 32];
    expect[31] = 0x07;
    assert_eq!(col.value(0), &expect, "u256 zero-extended big-endian");
}

#[test]
fn union_record_u256_negative_input_errors() {
    // precision>76, scale 0 with a negative unscaled value must error (u256 can't be negative).
    let rec =
        decimal_record(r#"{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}"#);
    let union = format!("[\"null\",{rec}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union).unwrap();
    // 0xFF encodes -1 (high bit set) -> negative.
    let body = union_body(
        &rec,
        1,
        vec![("v", Value::Decimal(Decimal::from(vec![0xFF])))],
    );
    let r = d.decode(&confluent_frame(1, &body)).and_then(|_| d.flush());
    assert!(r.is_err(), "negative u256 input must error");
}

#[test]
fn union_record_high_precision_scaled_decimal_to_string() {
    // precision>76 with scale>0 -> Utf8 carrying scale metadata; value formatted as decimal string.
    let rec =
        decimal_record(r#"{"type":"bytes","logicalType":"decimal","precision":77,"scale":2}"#);
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(
        &union,
        &rec,
        1,
        vec![("v", Value::Decimal(Decimal::from(vec![0x30, 0x39])))], // unscaled 12345, scale 2
    );
    // 12345 with scale 2 -> "123.45"
    let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(col.value(0), "123.45");
    let f = b.schema().field(0).clone();
    assert!(f.metadata().contains_key(AVRO_DECIMAL_SCALE_META));
}

#[test]
fn union_record_high_precision_scaled_trailing_zeros_trimmed() {
    let rec =
        decimal_record(r#"{"type":"bytes","logicalType":"decimal","precision":77,"scale":2}"#);
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(
        &union,
        &rec,
        1,
        vec![("v", Value::Decimal(Decimal::from(vec![0x64])))], // unscaled 100, scale 2 -> 1.00 -> "1"
    );
    let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(col.value(0), "1", "trailing fractional zeros trimmed");
}

#[test]
fn union_record_scaled_decimal_negative_value() {
    let rec =
        decimal_record(r#"{"type":"bytes","logicalType":"decimal","precision":77,"scale":2}"#);
    let union = format!("[\"null\",{rec}]");
    // -12345 two's complement big-endian: 0xCF 0xC7
    let b = decode_union_one(
        &union,
        &rec,
        1,
        vec![("v", Value::Decimal(Decimal::from(vec![0xCF, 0xC7])))],
    );
    let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(col.value(0), "-123.45");
}

// ===========================================================================
// J. Union-root records carrying primitive / composite field types
// ===========================================================================

#[test]
fn union_record_int_field() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(&union, rec, 1, vec![("n", Value::Int(-2_000_000_000))]);
    let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(col.value(0), -2_000_000_000);
}

#[test]
fn union_record_boolean_field() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"b","type":"boolean"}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(&union, rec, 1, vec![("b", Value::Boolean(true))]);
    let col = b.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(col.value(0));
}

#[test]
fn union_record_float_field() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"f","type":"float"}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(&union, rec, 1, vec![("f", Value::Float(1.5))]);
    let col = b.column(0).as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(col.value(0), 1.5);
}

#[test]
fn union_record_double_field() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"d","type":"double"}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(&union, rec, 1, vec![("d", Value::Double(2.25))]);
    let col = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(col.value(0), 2.25);
}

#[test]
fn union_record_bytes_field() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"raw","type":"bytes"}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(
        &union,
        rec,
        1,
        vec![("raw", Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]))],
    );
    let col = b.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(col.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
}

#[test]
fn union_record_string_field_unicode() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(&union, rec, 1, vec![("s", Value::String("héllo✓".into()))]);
    assert_eq!(str_col(&b, "s").value(0), "héllo✓");
}

#[test]
fn union_record_string_field_empty() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(&union, rec, 1, vec![("s", Value::String("".into()))]);
    assert_eq!(str_col(&b, "s").value(0), "");
}

#[test]
fn union_record_fixed_field() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"h","type":{"type":"fixed","name":"F4","size":4}}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(
        &union,
        rec,
        1,
        vec![("h", Value::Fixed(4, vec![1, 2, 3, 4]))],
    );
    assert_eq!(
        b.schema().field(0).data_type(),
        &DataType::FixedSizeBinary(4)
    );
    let col = b
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(col.value(0), &[1, 2, 3, 4]);
}

#[test]
fn union_record_enum_field() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"c","type":{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(&union, rec, 1, vec![("c", Value::Enum(1, "GREEN".into()))]);
    assert_eq!(b.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(str_col(&b, "c").value(0), "GREEN");
}

#[test]
fn union_record_array_of_long_field() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"xs","type":{"type":"array","items":"long"}}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(
        &union,
        rec,
        1,
        vec![(
            "xs",
            Value::Array(vec![Value::Long(10), Value::Long(20), Value::Long(30)]),
        )],
    );
    let list = b.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    let vals = list
        .value(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone();
    assert_eq!(
        (0..vals.len()).map(|i| vals.value(i)).collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
}

#[test]
fn union_record_array_empty() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"xs","type":{"type":"array","items":"long"}}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(&union, rec, 1, vec![("xs", Value::Array(vec![]))]);
    let list = b.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value(0).len(), 0);
}

#[test]
fn union_record_nested_struct_field() {
    let rec = r#"{"type":"record","name":"R","fields":[{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"x","type":"int"},{"name":"y","type":"string"}]}}]}"#;
    let union = format!("[\"null\",{rec}]");
    let b = decode_union_one(
        &union,
        rec,
        1,
        vec![(
            "inner",
            Value::Record(vec![
                ("x".to_string(), Value::Int(9)),
                ("y".to_string(), Value::String("nested".into())),
            ]),
        )],
    );
    let st = b.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    let x = st
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .clone();
    let y = st
        .column_by_name("y")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    assert_eq!(x.value(0), 9);
    assert_eq!(y.value(0), "nested");
}

// ===========================================================================
// K. Boundary long values through the union path
// ===========================================================================

#[test]
fn union_record_long_max() {
    let union = format!("[\"null\",{R_ID}]");
    let b = decode_union_one(&union, R_ID, 1, vec![("id", Value::Long(i64::MAX))]);
    assert_eq!(i64_col(&b, "id").value(0), i64::MAX);
}

#[test]
fn union_record_long_min() {
    let union = format!("[\"null\",{R_ID}]");
    let b = decode_union_one(&union, R_ID, 1, vec![("id", Value::Long(i64::MIN))]);
    assert_eq!(i64_col(&b, "id").value(0), i64::MIN);
}

#[test]
fn union_record_long_zero() {
    let union = format!("[\"null\",{R_ID}]");
    let b = decode_union_one(&union, R_ID, 1, vec![("id", Value::Long(0))]);
    assert_eq!(i64_col(&b, "id").value(0), 0);
}

#[test]
fn union_record_long_negative_one() {
    let union = format!("[\"null\",{R_ID}]");
    let b = decode_union_one(&union, R_ID, 1, vec![("id", Value::Long(-1))]);
    assert_eq!(i64_col(&b, "id").value(0), -1);
}

// ===========================================================================
// L. Reader-schema-driven union decoding (resolution ON)
// ===========================================================================

#[test]
fn union_reader_schema_decodes_with_resolution() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let reader = AvroWriterSchema::parse_str(&union).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, &union).unwrap();
    let body = union_body(
        R_ID_DATA,
        1,
        vec![
            ("id", Value::Long(99)),
            ("data", Value::String("rr".into())),
        ],
    );
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(i64_col(&b, "id").value(0), 99);
    assert_eq!(str_col(&b, "data").value(0), "rr");
}

#[test]
fn union_reader_target_matches_record_schema() {
    // The target derived from a union reader is the unwrapped record's arrow schema.
    let union = format!("[\"null\",{R_ID_DATA}]");
    let reader = AvroWriterSchema::parse_str(&union).unwrap();
    let d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    let target = d.target_schema().expect("target set").clone();
    assert_eq!(target.fields().len(), 2);
    assert_eq!(target.field(0).name(), "id");
    assert_eq!(target.field(0).data_type(), &DataType::Int64);
    assert_eq!(target.field(1).name(), "data");
    assert_eq!(target.field(1).data_type(), &DataType::Utf8);
}

#[test]
fn union_reader_with_added_default_field_resolves() {
    // Writer is union-rooted with {id,data}; reader adds a defaulted `version`.
    let writer_union = format!("[\"null\",{R_ID_DATA}]");
    const READER_REC: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":7}]}"#;
    let reader_union = format!("[\"null\",{READER_REC}]");
    let reader = AvroWriterSchema::parse_str(&reader_union).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, &writer_union).unwrap();
    let body = union_body(
        R_ID_DATA,
        1,
        vec![("id", Value::Long(5)), ("data", Value::String("v".into()))],
    );
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    let ver = b
        .column(b.schema().index_of("version").unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .clone();
    assert_eq!(ver.value(0), 7, "reader-only field filled from default");
}

#[test]
fn union_reader_plain_writer_mixed() {
    // Reader union-rooted, writer plain-rooted with the same record -> decodes without stripping.
    let reader_union = format!("[\"null\",{R_ID_DATA}]");
    let reader = AvroWriterSchema::parse_str(&reader_union).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .unwrap();
    d.register_writer_schema(1, R_ID_DATA).unwrap(); // plain writer
    let body = encode_record(
        R_ID_DATA,
        vec![
            ("id", Value::Long(8)),
            ("data", Value::String("plainw".into())),
        ],
    );
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(i64_col(&b, "id").value(0), 8);
    assert_eq!(str_col(&b, "data").value(0), "plainw");
}

// ===========================================================================
// M. Table-driven: many branch-order permutations decode with correct index
// ===========================================================================

#[test]
fn table_branch_order_permutations_decode() {
    // (union JSON built around R_ID, record branch index, id value)
    let cases: Vec<(String, i64, i64)> = vec![
        (format!("[\"null\",{R_ID}]"), 1, 100),
        (format!("[{R_ID},\"null\"]"), 0, 101),
        (format!("[\"int\",{R_ID}]"), 1, 102),
        (format!("[{R_ID},\"int\"]"), 0, 103),
        (format!("[\"null\",\"int\",{R_ID}]"), 2, 104),
        (format!("[\"null\",{R_ID},\"int\"]"), 1, 105),
        (format!("[{R_ID},\"null\",\"int\"]"), 0, 106),
        (format!("[\"null\",\"int\",\"string\",{R_ID}]"), 3, 107),
        (
            format!("[\"null\",\"boolean\",\"long\",\"string\",{R_ID}]"),
            4,
            108,
        ),
        (
            format!("[{R_ID},\"null\",\"int\",\"long\",\"string\"]"),
            0,
            109,
        ),
        (format!("[\"long\",\"string\",{R_ID}]"), 2, 110),
        (format!("[\"bytes\",{R_ID}]"), 1, 111),
    ];
    for (union, idx, id_val) in cases {
        let b = decode_union_one(&union, R_ID, idx, vec![("id", Value::Long(id_val))]);
        assert_eq!(
            i64_col(&b, "id").value(0),
            id_val,
            "branch index {idx} in union {union} must decode id {id_val}"
        );
    }
}

#[test]
fn table_wrong_branch_indices_error() {
    // For [null,record] (record index 1), every non-1 branch selector must error.
    let union = format!("[\"null\",{R_ID}]");
    for bad in [-100i64, -2, -1, 0, 2, 3, 10, 64, 1000] {
        let body = zigzag_long(bad);
        assert!(
            decode_err(&union, 1, &body),
            "branch {bad} != record index 1 must error"
        );
    }
}

#[test]
fn table_wrong_branch_indices_error_record_first() {
    // For [record,null] (record index 0), every non-0 branch selector must error.
    let union = format!("[{R_ID},\"null\"]");
    for bad in [-5i64, -1, 1, 2, 5, 64] {
        let body = zigzag_long(bad);
        assert!(
            decode_err(&union, 1, &body),
            "branch {bad} != record index 0 must error"
        );
    }
}

#[test]
fn table_malformed_bodies_error() {
    let union = format!("[\"null\",{R_ID}]");
    let malformed: Vec<Vec<u8>> = vec![
        vec![],           // empty body
        vec![0x80],       // truncated varint
        vec![0x80, 0x80], // still truncated
        vec![0x80; 12],   // overflow
        vec![0x00],       // selects null branch (index 0)
        vec![0x01],       // negative branch (-1)
    ];
    for (i, body) in malformed.iter().enumerate() {
        assert!(
            decode_err(&union, 1, body),
            "malformed body #{i} ({body:?}) must error"
        );
    }
}

// ===========================================================================
// N. Interleaving with a flush in between (generations across writer ids)
// ===========================================================================

#[test]
fn union_then_plain_flush_between_two_batches() {
    let union = format!("[\"null\",{R_ID_DATA}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union).unwrap();
    d.register_writer_schema(2, R_ID_DATA).unwrap();

    let u = union_body(
        R_ID_DATA,
        1,
        vec![("id", Value::Long(1)), ("data", Value::String("u".into()))],
    );
    d.decode(&confluent_frame(1, &u)).unwrap();
    let b1 = d.flush().unwrap().expect("batch 1");
    assert_eq!(i64_col(&b1, "id").value(0), 1);

    let p = encode_record(
        R_ID_DATA,
        vec![("id", Value::Long(2)), ("data", Value::String("p".into()))],
    );
    d.decode(&confluent_frame(2, &p)).unwrap();
    let b2 = d.flush().unwrap().expect("batch 2");
    assert_eq!(i64_col(&b2, "id").value(0), 2);
    assert_eq!(str_col(&b2, "data").value(0), "p");
}

#[test]
fn flush_with_no_decode_returns_none() {
    let union = format!("[\"null\",{R_ID}]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &union).unwrap();
    assert!(d.flush().unwrap().is_none());
}

#[test]
fn alternating_union_ids_produce_all_rows() {
    // Two union ids alternating many times -> many generations, all concatenated.
    let u_a = format!("[\"null\",{R_ID}]");
    let u_b = format!("[{R_ID},\"null\"]");
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, &u_a).unwrap();
    d.register_writer_schema(2, &u_b).unwrap();
    let mut expected = Vec::new();
    for i in 0..10i64 {
        if i % 2 == 0 {
            d.decode(&confluent_frame(
                1,
                &union_body(R_ID, 1, vec![("id", Value::Long(i))]),
            ))
            .unwrap();
        } else {
            d.decode(&confluent_frame(
                2,
                &union_body(R_ID, 0, vec![("id", Value::Long(i))]),
            ))
            .unwrap();
        }
        expected.push(i);
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 10);
    let ids = i64_col(&b, "id");
    assert_eq!((0..10).map(|i| ids.value(i)).collect::<Vec<_>>(), expected);
}

// ===========================================================================
// O. Nullable + decimal combined in a union-root record (Debezium-shaped)
// ===========================================================================

const R_DEBEZIUM: &str = r#"{"type":"record","name":"Value","fields":[
    {"name":"id","type":"long"},
    {"name":"amount","type":["null",{"type":"bytes","logicalType":"decimal","precision":20,"scale":4}],"default":null},
    {"name":"note","type":["null","string"],"default":null}
]}"#;

#[test]
fn debezium_union_record_all_fields_present() {
    let union = format!("[\"null\",{R_DEBEZIUM}]");
    let b = decode_union_one(
        &union,
        R_DEBEZIUM,
        1,
        vec![
            ("id", Value::Long(1000)),
            (
                "amount",
                Value::Union(1, Box::new(Value::Decimal(Decimal::from(vec![0x04, 0xD2])))),
            ),
            (
                "note",
                Value::Union(1, Box::new(Value::String("ok".into()))),
            ),
        ],
    );
    assert_eq!(i64_col(&b, "id").value(0), 1000);
    let amt = b
        .column(b.schema().index_of("amount").unwrap())
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .clone();
    assert!(!amt.is_null(0));
    assert_eq!(amt.value(0), 1234_i128);
    assert_eq!(str_col(&b, "note").value(0), "ok");
}

#[test]
fn debezium_union_record_nullable_fields_null() {
    let union = format!("[\"null\",{R_DEBEZIUM}]");
    let b = decode_union_one(
        &union,
        R_DEBEZIUM,
        1,
        vec![
            ("id", Value::Long(2000)),
            ("amount", Value::Union(0, Box::new(Value::Null))),
            ("note", Value::Union(0, Box::new(Value::Null))),
        ],
    );
    assert_eq!(i64_col(&b, "id").value(0), 2000);
    let amt = b
        .column(b.schema().index_of("amount").unwrap())
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .clone();
    assert!(amt.is_null(0));
    assert!(str_col(&b, "note").is_null(0));
}

#[test]
fn debezium_union_record_decimal_target_type() {
    let union = format!("[\"null\",{R_DEBEZIUM}]");
    let target = convert_avro_schema_to_arrow(AvroWriterSchema::parse_str(&union).unwrap());
    let amt = target.field(target.index_of("amount").unwrap());
    assert_eq!(amt.data_type(), &DataType::Decimal128(20, 4));
    assert!(amt.is_nullable());
}

#[test]
fn debezium_multiple_rows_mixed_nulls() {
    let union = format!("[\"null\",{R_DEBEZIUM}]");
    let id = 3u32;
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(id, &union).unwrap();
    // row 0: amount present, note null; row 1: amount null, note present
    let r0 = union_body(
        R_DEBEZIUM,
        1,
        vec![
            ("id", Value::Long(1)),
            (
                "amount",
                Value::Union(1, Box::new(Value::Decimal(Decimal::from(vec![0x0A])))),
            ),
            ("note", Value::Union(0, Box::new(Value::Null))),
        ],
    );
    let r1 = union_body(
        R_DEBEZIUM,
        1,
        vec![
            ("id", Value::Long(2)),
            ("amount", Value::Union(0, Box::new(Value::Null))),
            (
                "note",
                Value::Union(1, Box::new(Value::String("hi".into()))),
            ),
        ],
    );
    d.decode(&confluent_frame(id, &r0)).unwrap();
    d.decode(&confluent_frame(id, &r1)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 2);
    let amt = b
        .column(b.schema().index_of("amount").unwrap())
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .clone();
    assert_eq!(amt.value(0), 10_i128);
    assert!(amt.is_null(1));
    let note = str_col(&b, "note");
    assert!(note.is_null(0));
    assert_eq!(note.value(1), "hi");
}

// ===========================================================================
// P. Plain-root control cases (no union): must decode without any stripping
// ===========================================================================

#[test]
fn plain_root_control_decodes() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    let body = encode_record(
        R_ID_DATA,
        vec![
            ("id", Value::Long(7)),
            ("data", Value::String("plain".into())),
        ],
    );
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(i64_col(&b, "id").value(0), 7);
    assert_eq!(str_col(&b, "data").value(0), "plain");
}

#[test]
fn plain_root_id_with_first_field_zero() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, R_ID).unwrap();
    let body = encode_record(R_ID, vec![("id", Value::Long(0))]);
    d.decode(&confluent_frame(1, &body)).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(i64_col(&b, "id").value(0), 0);
}
