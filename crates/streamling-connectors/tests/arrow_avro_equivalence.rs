//! Equivalence test: the arrow-avro decode path (`ConfluentAvroDecoder` + recursive
//! `coerce_batch_to_target`) must produce a `RecordBatch` byte-for-byte identical to streamling's
//! current vendored path (`AvroToArrowConverter` / `AvroArrowArrayReader`) on the real blockchain
//! `traces` schema + payload.
//!
//! Run: `cargo test -p streamling-connectors --test arrow_avro_equivalence -- --nocapture`

use apache_avro::Schema as AvroSchema;
use streamling_core::formats::ToArrowConverter;
use streamling_core::formats::avro::arrow_avro::ConfluentAvroDecoder;
use streamling_core::formats::avro::{AvroToArrowConverter, convert_avro_schema_to_arrow};

fn manifest_path(rel: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)
}

fn load_schema(filename: &str) -> AvroSchema {
    let json = std::fs::read_to_string(manifest_path(&format!("testdata/avro-schema/{filename}")))
        .unwrap();
    AvroSchema::parse_str(&json).unwrap()
}

fn load_schema_json(filename: &str) -> String {
    std::fs::read_to_string(manifest_path(&format!("testdata/avro-schema/{filename}"))).unwrap()
}

fn load_payload(filename: &str) -> Vec<u8> {
    std::fs::read(manifest_path(&format!("testdata/avro-payload/{filename}"))).unwrap()
}

/// These `.bin` fixtures were captured with a trailing `0x0a` newline that is NOT part of the avro
/// datum (a real Kafka message carries exactly the avro bytes). `from_avro_datum` reads one datum
/// and ignores the trailing byte; arrow-avro's streaming decoder correctly rejects trailing junk.
/// To compare apples-to-apples we feed arrow-avro exactly the message a broker would deliver, so
/// here we compute the true datum boundary with apache_avro and trim the fixture to it.
fn trim_to_datum_boundary(framed: &[u8], writer_schema: &AvroSchema) -> Vec<u8> {
    let body = &framed[5..];
    let mut cursor = std::io::Cursor::new(body);
    apache_avro::from_avro_datum(writer_schema, &mut cursor, None).expect("decode to find boundary");
    let consumed = cursor.position() as usize;
    framed[..5 + consumed].to_vec()
}

/// Decode a single Confluent-framed payload through streamling's CURRENT path.
fn streamling_decode(framed: &[u8], writer_schema: &AvroSchema) -> arrow::record_batch::RecordBatch {
    let payload_schema = convert_avro_schema_to_arrow(writer_schema.clone());
    let body = &framed[5..];
    let mut cursor = std::io::Cursor::new(body);
    let value = apache_avro::from_avro_datum(writer_schema, &mut cursor, None)
        .expect("decode avro body with writer schema");
    let mut converter = AvroToArrowConverter::new(payload_schema, writer_schema.clone(), None);
    converter.buffer(value);
    converter.convert_to_batch().expect("convert_to_batch")
}

/// Decode the same payload through the arrow-avro path.
fn arrow_avro_decode(framed: &[u8], writer_json: &str) -> arrow::record_batch::RecordBatch {
    let schema_id = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
    let mut decoder = ConfluentAvroDecoder::new();
    decoder
        .register_writer_schema(schema_id, writer_json)
        .expect("register writer schema");
    decoder.decode(framed).expect("arrow-avro decode");
    decoder.flush().expect("arrow-avro flush").expect("a batch")
}

#[test]
fn arrow_avro_matches_vendored_path_on_real_traces() {
    // v1 (inlined nested records, no avro Refs) is the schema shape the current arrow path
    // supports. v2/v5 use named-type Refs which `convert_avro_schema_to_arrow` itself `todo!()`s on.
    for (schema_file, payload_file) in [
        ("traces-value-v1-1271.json", "arbitrum-one.raw.traces-1271.bin"),
        ("traces-value-v1-1271.json", "arbitrum-one.raw.traces-1271-p0-o366.bin"),
    ] {
        println!("\n=== {schema_file} / {payload_file} ===");
        let writer_schema = load_schema(schema_file);
        let writer_json = load_schema_json(schema_file);
        let framed = load_payload(payload_file);
        assert_eq!(framed[0], 0x00, "expected Confluent magic byte");
        let framed = trim_to_datum_boundary(&framed, &writer_schema);

        let expected = streamling_decode(&framed, &writer_schema);
        let actual = arrow_avro_decode(&framed, &writer_json);

        assert_eq!(expected.num_rows(), actual.num_rows(), "row count differs");
        assert_eq!(
            expected.num_columns(),
            actual.num_columns(),
            "column count differs"
        );

        // Compare each column's field type, metadata, and data, surfacing the first mismatch.
        let actual_schema = actual.schema();
        for (i, ef) in expected.schema().fields().iter().enumerate() {
            let af = actual_schema.field(i);
            assert_eq!(
                ef.data_type(),
                af.data_type(),
                "col {i} ({}) type differs: expected {:?}, got {:?}",
                ef.name(),
                ef.data_type(),
                af.data_type()
            );
            assert_eq!(
                ef.metadata(),
                af.metadata(),
                "col {i} ({}) metadata differs",
                ef.name()
            );
            assert_eq!(
                expected.column(i),
                actual.column(i),
                "col {i} ({}) data differs",
                ef.name()
            );
        }
        println!(
            "  OK: {} rows, {} cols match",
            actual.num_rows(),
            actual.num_columns()
        );
    }
}
