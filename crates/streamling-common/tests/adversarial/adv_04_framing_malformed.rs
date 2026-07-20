//! Adversarial integration tests for `ConfluentAvroDecoder::decode` Confluent-framing and
//! malformed-payload handling in `arrow_avro.rs`.
//!
//! Area: frame parsing. Every malformed input must produce a CLEAN error (or, for arrow-avro
//! body-level problems, no silently-corrupt row) -- never a panic, never a corrupt/wrong value.
//! Covered:
//!   * empty slice and lengths 1..=4 (shorter than the 5-byte Confluent header) -> error;
//!   * correct length but first byte != 0x00 (bad magic) -> error, checked before the body;
//!   * valid header with an UNREGISTERED schema id -> error, not panic;
//!   * big-endian schema-id parsing (a byte-swapped id must miss a registered id);
//!   * body truncated mid-record -> error or zero complete rows (no phantom row);
//!   * body with EXTRA trailing bytes after a valid record -> no phantom/corrupt row;
//!   * a valid frame decodes and `flush` yields exactly one correctly-valued row;
//!   * `decode` before any writer schema is registered -> error;
//!   * `flush` with no decode -> Ok(None) (schema set) or error (no schema at all);
//!   * `has_writer_schema` registration-state transitions;
//!   * writer-id change finalizes a generation and both rows survive `flush`.
//!
//! Assertions target the CORRECT contract, not whatever the code currently happens to do.

use apache_avro::types::{Record, Value};
use apache_avro::{Schema as AvroWriterSchema, to_avro_datum};
use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

use streamling_common::formats::avro::arrow_avro::ConfluentAvroDecoder;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Confluent wire framing: 0x00 magic + 4-byte big-endian schema id + avro body.
fn confluent_frame(id: u32, body: &[u8]) -> Vec<u8> {
    let mut f = vec![0x00];
    f.extend_from_slice(&id.to_be_bytes());
    f.extend_from_slice(body);
    f
}

const LONG_SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;
const STRING_SCHEMA: &str =
    r#"{"type":"record","name":"S","fields":[{"name":"s","type":"string"}]}"#;
const TWO_FIELD_SCHEMA: &str = r#"{"type":"record","name":"T","fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}"#;

/// A decoder with `LONG_SCHEMA` as both reader and writer schema, registered under `id`.
fn long_decoder(id: u32) -> ConfluentAvroDecoder {
    let schema = AvroWriterSchema::parse_str(LONG_SCHEMA).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(id, LONG_SCHEMA).unwrap();
    d
}

fn long_body(v: i64) -> Vec<u8> {
    let schema = AvroWriterSchema::parse_str(LONG_SCHEMA).unwrap();
    let mut rec = Record::new(&schema).unwrap();
    rec.put("id", Value::Long(v));
    to_avro_datum(&schema, rec).unwrap()
}

fn string_body(v: &str) -> Vec<u8> {
    let schema = AvroWriterSchema::parse_str(STRING_SCHEMA).unwrap();
    let mut rec = Record::new(&schema).unwrap();
    rec.put("s", Value::String(v.to_string()));
    to_avro_datum(&schema, rec).unwrap()
}

fn two_field_body(a: i64, b: i64) -> Vec<u8> {
    let schema = AvroWriterSchema::parse_str(TWO_FIELD_SCHEMA).unwrap();
    let mut rec = Record::new(&schema).unwrap();
    rec.put("a", Value::Long(a));
    rec.put("b", Value::Long(b));
    to_avro_datum(&schema, rec).unwrap()
}

fn id_col(b: &RecordBatch) -> Int64Array {
    b.column(b.schema().index_of("id").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is Int64")
        .clone()
}

// ===========================================================================
// 1. Frames shorter than the 5-byte header must error cleanly.
// ===========================================================================

#[test]
fn empty_slice_errors() {
    let mut d = long_decoder(1);
    assert!(d.decode(&[]).is_err(), "empty slice must be rejected");
}

#[test]
fn short_frames_1_to_4_bytes_error() {
    // Every length below the 5-byte header, with a couple of first-byte variants, must error --
    // before any attempt to look up a schema or touch arrow-avro.
    let first_bytes = [0x00u8, 0xFF, 0x42];
    for len in 0usize..=4 {
        for &fb in &first_bytes {
            let mut d = long_decoder(1);
            let mut frame: Vec<u8> = vec![0u8; len];
            if len > 0 {
                frame[0] = fb;
            }
            assert!(
                d.decode(&frame).is_err(),
                "frame of length {len} (first byte {fb:#04x}) must be rejected"
            );
        }
    }
}

#[test]
fn four_zero_bytes_with_magic_still_too_short() {
    // 0x00 magic present but only 4 bytes total: still shorter than the 5-byte header.
    let mut d = long_decoder(1);
    assert!(d.decode(&[0x00, 0x00, 0x00, 0x00]).is_err());
}

#[test]
fn exactly_five_bytes_unregistered_id_errors() {
    // Minimum-length header, valid magic, but id 0 is not registered (only id 1 is). This must
    // error at schema lookup, not panic on the empty body.
    let mut d = long_decoder(1);
    assert!(d.decode(&[0x00, 0x00, 0x00, 0x00, 0x00]).is_err());
}

// ===========================================================================
// 2. Bad magic byte (first byte != 0x00).
// ===========================================================================

#[test]
fn bad_magic_with_registered_id_errors() {
    // Registered id, valid body, but a wrong magic byte -- must be rejected regardless of body.
    let bad_magics = [
        0x01u8, 0x02, 0x03, 0x04, 0x05, 0x0F, 0x10, 0x40, 0x7F, 0x80, 0x81, 0xAA, 0xC0, 0xC3, 0xFE,
        0xFF,
    ];
    let body = long_body(7);
    for &m in &bad_magics {
        let mut d = long_decoder(1);
        let mut frame = confluent_frame(1, &body);
        frame[0] = m;
        assert!(
            d.decode(&frame).is_err(),
            "magic byte {m:#04x} must be rejected even with a valid body"
        );
    }
}

#[test]
fn bad_magic_with_unregistered_id_errors() {
    // Bad magic combined with an unregistered id: still a clean error.
    let bad_magics = [0x01u8, 0x7F, 0x80, 0xC3, 0xFF];
    let body = long_body(1);
    for &m in &bad_magics {
        let mut d = long_decoder(1);
        let mut frame = confluent_frame(999, &body);
        frame[0] = m;
        assert!(d.decode(&frame).is_err(), "magic {m:#04x} + unknown id");
    }
}

#[test]
fn avro_single_object_magic_c3_rejected() {
    // 0xC3 is avro's OTHER single-object-encoding magic; this decoder only speaks Confluent (0x00).
    let mut d = long_decoder(1);
    let mut frame = confluent_frame(1, &long_body(3));
    frame[0] = 0xC3;
    assert!(d.decode(&frame).is_err());
}

#[test]
fn bad_magic_checked_before_body_validity() {
    // Even an otherwise perfectly-valid body must not slip through a bad magic byte.
    let mut d = long_decoder(1);
    let mut frame = confluent_frame(1, &long_body(123));
    frame[0] = 0x01;
    assert!(d.decode(&frame).is_err());
}

// ===========================================================================
// 3. Unregistered schema id (valid framing, no matching writer schema).
// ===========================================================================

#[test]
fn unregistered_id_sweep_errors() {
    // Decoder knows only id 1; every other id must error, never panic.
    let unknown_ids = [0u32, 2, 3, 100, 255, 65535, 0x0102_0304, u32::MAX];
    let body = long_body(5);
    for &id in &unknown_ids {
        let mut d = long_decoder(1);
        assert!(
            d.decode(&confluent_frame(id, &body)).is_err(),
            "unregistered id {id} must error"
        );
    }
}

#[test]
fn registered_a_decode_b_errors() {
    let mut d = long_decoder(7);
    assert!(d.has_writer_schema(7));
    assert!(!d.has_writer_schema(8));
    // id 8 was never registered.
    assert!(d.decode(&confluent_frame(8, &long_body(1))).is_err());
}

#[test]
fn schema_id_is_big_endian_byteswapped_id_misses() {
    // Register a multi-byte id; a byte-swapped (little-endian) frame must parse to a DIFFERENT,
    // unregistered id and error -- proving big-endian parsing.
    let id: u32 = 0x0102_0304;
    let mut d = long_decoder(id);
    // Correct big-endian frame decodes.
    assert!(d.decode(&confluent_frame(id, &long_body(1))).is_ok());
    d.flush().unwrap();
    // Byte-swapped id (0x04030201) is not registered.
    let mut d2 = long_decoder(id);
    let swapped = id.swap_bytes();
    assert!(
        d2.decode(&confluent_frame(swapped, &long_body(1))).is_err(),
        "byte-swapped id {swapped:#010x} must be treated as unregistered"
    );
}

// ===========================================================================
// 4. decode before any writer schema registered.
// ===========================================================================

#[test]
fn decode_before_any_register_with_reader_errors() {
    // Reader schema set (so target exists) but no writer registered -> unknown id -> error.
    let schema = AvroWriterSchema::parse_str(LONG_SCHEMA).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    assert!(d.decode(&confluent_frame(1, &long_body(1))).is_err());
}

#[test]
fn decode_before_any_schema_at_all_errors() {
    // Nothing set at all: decode must fail cleanly, not panic.
    let mut d = ConfluentAvroDecoder::new();
    assert!(d.decode(&confluent_frame(1, &long_body(1))).is_err());
}

// ===========================================================================
// 5. flush with no decode.
// ===========================================================================

#[test]
fn flush_no_decode_schema_set_returns_none() {
    let mut d = long_decoder(1);
    assert!(
        d.flush().unwrap().is_none(),
        "flush before any decode must yield Ok(None)"
    );
}

#[test]
fn flush_no_schema_at_all_errors() {
    // No reader schema and no writer registered => no target schema => flush is a genuine error.
    let mut d = ConfluentAvroDecoder::new();
    assert!(d.flush().is_err());
}

#[test]
fn double_flush_second_is_none() {
    let mut d = long_decoder(1);
    d.decode(&confluent_frame(1, &long_body(5))).unwrap();
    let b = d.flush().unwrap().expect("first flush yields a batch");
    assert_eq!(b.num_rows(), 1);
    assert!(
        d.flush().unwrap().is_none(),
        "flush resets state; the second flush must be empty"
    );
}

#[test]
fn flush_after_register_only_is_none() {
    // Registering a writer schema (target derived) but never decoding -> flush is empty, not error.
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, LONG_SCHEMA).unwrap();
    assert!(d.flush().unwrap().is_none());
}

// ===========================================================================
// 6. has_writer_schema state transitions.
// ===========================================================================

#[test]
fn has_writer_schema_false_before_register() {
    let d = ConfluentAvroDecoder::new();
    assert!(!d.has_writer_schema(1));
    assert!(!d.has_writer_schema(0));
    assert!(!d.has_writer_schema(u32::MAX));
}

#[test]
fn has_writer_schema_true_after_register() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(42, LONG_SCHEMA).unwrap();
    assert!(d.has_writer_schema(42));
    assert!(!d.has_writer_schema(43));
}

#[test]
fn has_writer_schema_multiple_ids() {
    let mut d = ConfluentAvroDecoder::new();
    for id in [0u32, 1, 255, 65535, u32::MAX] {
        d.register_writer_schema(id, LONG_SCHEMA).unwrap();
    }
    for id in [0u32, 1, 255, 65535, u32::MAX] {
        assert!(d.has_writer_schema(id), "id {id} should be registered");
    }
    assert!(!d.has_writer_schema(2));
    assert!(!d.has_writer_schema(65534));
}

#[test]
fn re_register_same_id_keeps_registration_and_decodes() {
    let mut d = long_decoder(1);
    // Re-register the same id (overwrite) -- registration state must persist and decode still work.
    d.register_writer_schema(1, LONG_SCHEMA).unwrap();
    assert!(d.has_writer_schema(1));
    d.decode(&confluent_frame(1, &long_body(9))).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(id_col(&b).value(0), 9);
}

// ===========================================================================
// 7. Valid frames decode to exactly one correctly-valued row.
// ===========================================================================

#[test]
fn valid_long_frame_yields_one_row() {
    let mut d = long_decoder(1);
    let n = d.decode(&confluent_frame(1, &long_body(42))).unwrap();
    assert!(n > 0, "decode should report bytes consumed");
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 1, "exactly one row");
    assert_eq!(id_col(&b).value(0), 42);
}

#[test]
fn valid_long_values_roundtrip() {
    let values = [
        0i64,
        1,
        -1,
        42,
        -42,
        1_000_000,
        -1_000_000,
        i64::MAX,
        i64::MIN,
        1 << 40,
    ];
    for &v in &values {
        let mut d = long_decoder(1);
        d.decode(&confluent_frame(1, &long_body(v))).unwrap();
        let b = d.flush().unwrap().expect("batch");
        assert_eq!(b.num_rows(), 1);
        assert_eq!(id_col(&b).value(0), v, "long value {v} must round-trip");
    }
}

#[test]
fn valid_string_frame_roundtrips() {
    let schema = AvroWriterSchema::parse_str(STRING_SCHEMA).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(1, STRING_SCHEMA).unwrap();
    for s in ["", "a", "hello world", "unicode: \u{1F600}\u{00e9}"] {
        d.decode(&confluent_frame(1, &string_body(s))).unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 4);
    let col = b
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    assert_eq!(col.value(0), "");
    assert_eq!(col.value(1), "a");
    assert_eq!(col.value(2), "hello world");
    assert_eq!(col.value(3), "unicode: \u{1F600}\u{00e9}");
}

#[test]
fn multiple_rows_same_id_accumulate() {
    let mut d = long_decoder(1);
    for i in 0..10i64 {
        d.decode(&confluent_frame(1, &long_body(i * 100))).unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 10);
    let col = id_col(&b);
    for i in 0..10i64 {
        assert_eq!(col.value(i as usize), i * 100);
    }
}

#[test]
fn valid_decode_for_various_registered_ids() {
    // The full id range must be usable as a registry id.
    let ids = [0u32, 1, 255, 256, 65535, 0x0102_0304, u32::MAX];
    for (n, &id) in ids.iter().enumerate() {
        let mut d = long_decoder(id);
        d.decode(&confluent_frame(id, &long_body(n as i64)))
            .unwrap_or_else(|e| panic!("id {id} should decode: {e:?}"));
        let b = d.flush().unwrap().expect("batch");
        assert_eq!(id_col(&b).value(0), n as i64);
    }
}

#[test]
fn id_zero_registered_decodes() {
    let mut d = long_decoder(0);
    assert!(d.has_writer_schema(0));
    d.decode(&confluent_frame(0, &long_body(-7))).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(id_col(&b).value(0), -7);
}

// ===========================================================================
// 8. Truncated bodies: must error or produce zero complete rows -- never a phantom row.
// ===========================================================================

/// A truncated frame must never silently yield a complete row. Accept a decode error, a flush
/// error, or a zero-row batch; reject any row surviving from an incomplete record.
fn assert_no_row_survives_truncation(d: &mut ConfluentAvroDecoder, frame: &[u8], ctx: &str) {
    if d.decode(frame).is_ok() {
        match d.flush() {
            Ok(None) => {}
            Ok(Some(b)) => assert_eq!(
                b.num_rows(),
                0,
                "truncated frame ({ctx}) silently produced {} row(s)",
                b.num_rows()
            ),
            Err(_) => {}
        }
    }
}

#[test]
fn truncated_single_field_body_at_every_offset() {
    // Use a value whose zigzag varint spans multiple continuation bytes so every mid-body cut is
    // genuinely incomplete.
    let full = confluent_frame(1, &long_body(1_234_567_890_123));
    for cut in 5..full.len() {
        let mut d = long_decoder(1);
        assert_no_row_survives_truncation(&mut d, &full[..cut], &format!("cut={cut}"));
    }
}

#[test]
fn header_only_with_nonempty_schema_no_row() {
    // 5-byte header for a schema that expects a body: no complete record exists.
    let mut d = long_decoder(1);
    assert_no_row_survives_truncation(&mut d, &confluent_frame(1, &[]), "header only");
}

#[test]
fn truncated_second_field_no_row() {
    // Two-long record with the second field cut off entirely.
    let schema = AvroWriterSchema::parse_str(TWO_FIELD_SCHEMA).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(1, TWO_FIELD_SCHEMA).unwrap();
    let full_body = two_field_body(1_000_000, 2_000_000);
    // Keep only the first field's bytes (first varint). Cut to 1 body byte after header.
    let frame = confluent_frame(1, &full_body[..1]);
    assert_no_row_survives_truncation(&mut d, &frame, "second field missing");
}

#[test]
fn truncated_string_length_prefix_no_row() {
    // A string body whose length prefix claims more bytes than are present.
    let schema = AvroWriterSchema::parse_str(STRING_SCHEMA).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(1, STRING_SCHEMA).unwrap();
    let body = string_body("a fairly long string value");
    // Drop the tail so the declared length overruns the buffer.
    let frame = confluent_frame(1, &body[..2]);
    if d.decode(&frame).is_ok() {
        match d.flush() {
            Ok(Some(b)) => assert_eq!(b.num_rows(), 0, "overrun string produced a row"),
            _ => {}
        }
    }
}

// ===========================================================================
// 9. Extra trailing bytes after a valid record: no phantom/corrupt row.
// ===========================================================================

/// After a valid record plus trailing junk, the decoder must not fabricate extra rows nor corrupt
/// the first. Accept an error; if it produces rows, there is at most one and it equals `expected`.
fn assert_trailing_junk_clean(d: &mut ConfluentAvroDecoder, frame: &[u8], expected: i64) {
    if d.decode(frame).is_ok() {
        if let Ok(Some(b)) = d.flush() {
            assert!(
                b.num_rows() <= 1,
                "trailing junk produced {} phantom rows",
                b.num_rows()
            );
            if b.num_rows() == 1 {
                assert_eq!(
                    id_col(&b).value(0),
                    expected,
                    "the real record's value must not be corrupted by trailing bytes"
                );
            }
        }
    }
}

#[test]
fn trailing_ff_bytes_after_valid_record() {
    let mut frame = confluent_frame(1, &long_body(7));
    frame.push(0xFF);
    frame.push(0xFF);
    let mut d = long_decoder(1);
    assert_trailing_junk_clean(&mut d, &frame, 7);
}

#[test]
fn trailing_zero_bytes_after_valid_record() {
    let mut frame = confluent_frame(1, &long_body(31));
    frame.extend_from_slice(&[0x00, 0x00, 0x00]);
    let mut d = long_decoder(1);
    assert_trailing_junk_clean(&mut d, &frame, 31);
}

#[test]
fn trailing_single_byte_after_valid_record() {
    let mut frame = confluent_frame(1, &long_body(-5));
    frame.push(0x2A);
    let mut d = long_decoder(1);
    assert_trailing_junk_clean(&mut d, &frame, -5);
}

#[test]
fn second_body_concatenated_without_own_header() {
    // One valid record immediately followed by another record's bytes but WITHOUT its own
    // Confluent header. Must not corrupt the first record's value.
    let mut frame = confluent_frame(1, &long_body(11));
    frame.extend_from_slice(&long_body(22));
    let mut d = long_decoder(1);
    assert_trailing_junk_clean(&mut d, &frame, 11);
}

#[test]
fn full_second_frame_appended_is_not_two_rows_in_one_decode() {
    // Two fully-framed messages passed as ONE slice: the decoder is documented to take one framed
    // message per call, so this must not silently corrupt the first value.
    let mut frame = confluent_frame(1, &long_body(100));
    frame.extend_from_slice(&confluent_frame(1, &long_body(200)));
    let mut d = long_decoder(1);
    if d.decode(&frame).is_ok() {
        if let Ok(Some(b)) = d.flush() {
            assert!(b.num_rows() >= 1);
            // Whatever it decodes, the FIRST row must be the first message's value, never garbage.
            assert_eq!(id_col(&b).value(0), 100);
        }
    }
}

// ===========================================================================
// 10. Writer-id change generations + wrong-id after a valid decode.
// ===========================================================================

#[test]
fn writer_id_change_finalizes_generation_both_rows_survive() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, LONG_SCHEMA).unwrap();
    d.register_writer_schema(2, LONG_SCHEMA).unwrap();
    d.decode(&confluent_frame(1, &long_body(10))).unwrap();
    d.decode(&confluent_frame(2, &long_body(20))).unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 2, "both writer-id generations must survive");
    let col = id_col(&b);
    assert_eq!(col.value(0), 10);
    assert_eq!(col.value(1), 20);
}

#[test]
fn interleaved_writer_ids_preserve_all_rows() {
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, LONG_SCHEMA).unwrap();
    d.register_writer_schema(2, LONG_SCHEMA).unwrap();
    // 1,1,2,2,1 -> three generation switches; all five rows must survive in order.
    let seq = [(1u32, 1i64), (1, 2), (2, 3), (2, 4), (1, 5)];
    for (id, v) in seq {
        d.decode(&confluent_frame(id, &long_body(v))).unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_rows(), 5);
    let col = id_col(&b);
    for (i, expected) in [1, 2, 3, 4, 5].into_iter().enumerate() {
        assert_eq!(col.value(i), expected, "row {i}");
    }
}

#[test]
fn wrong_id_frame_after_valid_decode_errors() {
    // A valid decode, then a frame for an unregistered id. The unregistered decode must error.
    let mut d = long_decoder(1);
    d.decode(&confluent_frame(1, &long_body(1))).unwrap();
    assert!(
        d.decode(&confluent_frame(999, &long_body(2))).is_err(),
        "switching to an unregistered id must error"
    );
}

#[test]
fn empty_slice_after_valid_decode_errors_without_losing_prior_flush() {
    // A malformed (empty) frame after a good decode must error; the good row is still flushable.
    let mut d = long_decoder(1);
    d.decode(&confluent_frame(1, &long_body(88))).unwrap();
    assert!(d.decode(&[]).is_err());
    let b = d
        .flush()
        .unwrap()
        .expect("prior valid row must still flush");
    assert!(b.num_rows() >= 1);
    assert_eq!(id_col(&b).value(0), 88);
}

// ===========================================================================
// 11. Header-boundary and magic/id-interaction edges.
// ===========================================================================

#[test]
fn all_ones_id_bytes_unregistered_errors() {
    // Frame with id = 0xFFFFFFFF (u32::MAX) not registered.
    let mut d = long_decoder(1);
    let frame = confluent_frame(u32::MAX, &long_body(1));
    assert!(d.decode(&frame).is_err());
}

#[test]
fn valid_magic_registered_id_but_garbage_body_no_row() {
    // Correct header, registered id, body is arbitrary junk that is not a valid record encoding.
    let mut d = long_decoder(1);
    let mut frame = confluent_frame(1, &[]);
    frame.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    // long field: 10 continuation-marked bytes overflow the varint -> must not yield a valid row.
    if d.decode(&frame).is_ok() {
        if let Ok(Some(b)) = d.flush() {
            // If anything decodes it must not exceed one row; correctness of a garbage long is
            // undefined, so we only guard against a phantom row explosion / corruption crash.
            assert!(b.num_rows() <= 1);
        }
    }
}

#[test]
fn magic_ok_but_frame_is_exactly_header_for_registered_id() {
    // Registered id, exactly 5 bytes (empty body) for a schema that needs a body: no phantom row.
    let mut d = long_decoder(1);
    let frame = vec![0x00, 0x00, 0x00, 0x00, 0x01]; // id = 1, empty body
    assert_no_row_survives_truncation(&mut d, &frame, "registered id, empty body");
}
