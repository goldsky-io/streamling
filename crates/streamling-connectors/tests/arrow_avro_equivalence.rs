//! Real-payload regression test for the arrow-avro decode path (`ConfluentAvroDecoder` +
//! recursive `coerce_batch_to_target`): decoding the real blockchain `traces` schema + payload
//! must produce a `RecordBatch` whose schema matches streamling's `convert_avro_schema_to_arrow`
//! mapping (top-level wide decimal → `decimal_arb` + nested `List<Struct{…Decimal128(100,0)}>`)
//! and whose `decimal_arb` column round-trips.
//!
//! (This test previously cross-checked against the vendored `AvroToArrowConverter` /
//! `AvroArrowArrayReader` path; that path has been removed, so it now asserts against the
//! target schema + decoded values directly.)
//!
//! Run: `cargo test -p streamling-connectors --test arrow_avro_equivalence -- --nocapture`

use apache_avro::Schema as AvroSchema;
use arrow::array::{Array, LargeBinaryArray};
use arrow_schema::DataType;
use streamling_core::formats::avro::arrow_avro::ConfluentAvroDecoder;
use streamling_core::formats::avro::convert_avro_schema_to_arrow;
use streamling_core::types::decimal_arb::{DecimalArbType, DecimalArbValue};

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
/// datum (a real Kafka message carries exactly the avro bytes). arrow-avro's streaming decoder
/// rejects trailing junk, so we trim the fixture to the true datum boundary (found via apache_avro).
fn trim_to_datum_boundary(framed: &[u8], writer_schema: &AvroSchema) -> Vec<u8> {
    let body = &framed[5..];
    let mut cursor = std::io::Cursor::new(body);
    apache_avro::from_avro_datum(writer_schema, &mut cursor, None)
        .expect("decode to find boundary");
    let consumed = cursor.position() as usize;
    framed[..5 + consumed].to_vec()
}

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
fn arrow_avro_decodes_real_traces_to_target_schema() {
    // v1 (inlined nested records, no avro Refs) is the schema shape the arrow decode path supports.
    // v2/v5 use named-type Refs which `convert_avro_schema_to_arrow` itself `todo!()`s on.
    let schema_file = "traces-value-v1-1271.json";
    let writer_schema = load_schema(schema_file);
    let writer_json = load_schema_json(schema_file);
    let target = convert_avro_schema_to_arrow(writer_schema.clone());

    for payload_file in [
        "arbitrum-one.raw.traces-1271.bin",
        "arbitrum-one.raw.traces-1271-p0-o366.bin",
    ] {
        println!("\n=== {schema_file} / {payload_file} ===");
        let framed = load_payload(payload_file);
        assert_eq!(framed[0], 0x00, "expected Confluent magic byte");
        let framed = trim_to_datum_boundary(&framed, &writer_schema);

        let batch = arrow_avro_decode(&framed, &writer_json);
        let schema = batch.schema();

        // The decoded batch must carry exactly streamling's target Arrow schema, recursively
        // (field types, nullability, and decimal_arb extension metadata).
        assert_eq!(
            schema.fields(),
            target.fields(),
            "decoded batch schema does not match convert_avro_schema_to_arrow"
        );
        assert_eq!(batch.num_rows(), 1, "expected one decoded row");

        // Top-level `value` (avro decimal precision 100, scale 0) → streamling.decimal_arb.
        let value_idx = target.index_of("value").unwrap();
        let value_field = schema.field(value_idx).clone();
        assert!(
            DecimalArbType::is_decimal_arb_field(&value_field),
            "`value` field is not tagged decimal_arb: {value_field:?}"
        );
        let (_p, scale) =
            DecimalArbType::precision_scale_from_field(&value_field).expect("decimal_arb metadata");
        let value_col = batch
            .column(value_idx)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("decimal_arb column is LargeBinary");
        assert!(
            !value_col.is_null(0),
            "`value` should be non-null in this row"
        );
        // The canonical payload decodes to a valid decimal_arb value at the declared scale.
        DecimalArbValue::from_canonical_bytes_at_scale(value_col.value(0), scale)
            .expect("`value` decodes as decimal_arb");

        // Nested high-precision decimals inside the array-of-records become Decimal128(100,0)
        // (matching streamling's top-level-only u256 fixup — nested decimals fall through).
        let xfers_idx = target.index_of("after_evm_transfers").unwrap();
        let DataType::List(elem) = schema.field(xfers_idx).data_type() else {
            panic!("after_evm_transfers is not a List");
        };
        let DataType::Struct(fields) = elem.data_type() else {
            panic!("after_evm_transfers element is not a Struct");
        };
        let nested_value = fields.iter().find(|f| f.name() == "value").unwrap();
        assert_eq!(
            nested_value.data_type(),
            &DataType::Decimal128(100, 0),
            "nested transfer `value` should be Decimal128(100,0)"
        );

        println!(
            "  OK: {} cols, schema + u256 + nested types verified",
            batch.num_columns()
        );
    }
}
