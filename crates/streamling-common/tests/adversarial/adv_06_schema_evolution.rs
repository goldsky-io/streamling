//! Adversarial integration tests for schema evolution / mixed writers / generations in
//! `arrow_avro.rs` (`ConfluentAvroDecoder`).
//!
//! Area: multiple writer schemas resolved to ONE reader schema, accumulated across arrow-avro
//! "generations" and concatenated by `flush`. A single arrow-avro `Decoder` cannot mix writer
//! schemas in one batch, so `ConfluentAvroDecoder` finalizes a per-generation batch every time the
//! incoming writer id changes and concatenates them on `flush`. The chief risk is ROW LOSS / COLUMN
//! DESYNC when writer ids interleave, plus incorrect writer->reader resolution (added-field
//! defaults, removed fields, reordered fields, numeric promotion).
//!
//! Contract asserted (derived from the code under test + Avro resolution spec + the vendored
//! reader), NOT whatever the code currently happens to do:
//!   * every decoded row survives `flush`; all columns have equal length == num_rows;
//!   * a reader field the writer lacks is filled from its reader DEFAULT (resolution on);
//!   * a writer-only field (absent from reader) is dropped;
//!   * reordered writer fields resolve to the reader by NAME;
//!   * Avro numeric promotion int->long / float->double yields the reader's (wider) type & value;
//!   * row ORDER is preserved across generations (pending pushed in close order, current last);
//!   * `flush` with zero decodes is Ok(None); registering an id but never decoding adds no rows;
//!   * a reader-added REQUIRED field with no default is an error for an old writer (spec).

use apache_avro::types::{Record, Value};
use apache_avro::{Schema as AvroWriterSchema, to_avro_datum};
use arrow::array::{Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;

use streamling_common::formats::avro::arrow_avro::ConfluentAvroDecoder;
use streamling_common::formats::avro::convert_avro_schema_to_arrow;

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

fn parse(json: &str) -> AvroWriterSchema {
    AvroWriterSchema::parse_str(json).expect("schema parses")
}

/// Encode one record body against `schema`, setting the given named fields.
fn body(schema: &AvroWriterSchema, fields: &[(&str, Value)]) -> Vec<u8> {
    let mut rec = Record::new(schema).expect("schema is a record");
    for (name, value) in fields {
        rec.put(name, value.clone());
    }
    to_avro_datum(schema, rec).expect("encode avro datum")
}

fn decoder_for(reader_json: &str) -> ConfluentAvroDecoder {
    let reader = parse(reader_json);
    ConfluentAvroDecoder::new()
        .with_reader_schema(&reader)
        .expect("reader schema accepted")
}

/// Assert num_rows == expected AND every column has exactly that length (the desync regression).
fn assert_uniform(b: &RecordBatch, expected_rows: usize) {
    assert_eq!(b.num_rows(), expected_rows, "num_rows mismatch");
    for (i, c) in b.columns().iter().enumerate() {
        assert_eq!(
            c.len(),
            expected_rows,
            "column {i} length != num_rows (writer-generation desync)"
        );
    }
}

fn get_i64(b: &RecordBatch, name: &str, row: usize) -> i64 {
    b.column(b.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .value(row)
}

fn get_i32(b: &RecordBatch, name: &str, row: usize) -> i32 {
    b.column(b.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32")
        .value(row)
}

fn get_f32(b: &RecordBatch, name: &str, row: usize) -> f32 {
    b.column(b.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("Float32")
        .value(row)
}

fn get_f64(b: &RecordBatch, name: &str, row: usize) -> f64 {
    b.column(b.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64")
        .value(row)
}

fn get_str(b: &RecordBatch, name: &str, row: usize) -> String {
    b.column(b.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8")
        .value(row)
        .to_string()
}

fn is_null(b: &RecordBatch, name: &str, row: usize) -> bool {
    b.column(b.schema().index_of(name).unwrap()).is_null(row)
}

// Shared schemas (record name held constant = "R" so writer/reader name resolution succeeds).
const R_ID: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}"#;
const R_ID_DATA: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"}]}"#;
const R_ID_DATA_VER1: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":1}]}"#;
const R_ID_DATA_VER7: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":7}]}"#;
const R_DATA_ID: &str = r#"{"type":"record","name":"R","fields":[{"name":"data","type":"string"},{"name":"id","type":"long"}]}"#;
const R_ID_N_INT: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"n","type":"int"}]}"#;
const R_ID_N_LONG: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"n","type":"long"}]}"#;
const R_ID_X_FLOAT: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"x","type":"float"}]}"#;
const R_ID_X_DOUBLE: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"x","type":"double"}]}"#;
const R_SEQ: &str = r#"{"type":"record","name":"R","fields":[{"name":"seq","type":"long"}]}"#;

// ===========================================================================
// GROUP A: added field carrying a default (resolution fills it)
// ===========================================================================

#[test]
fn a01_added_field_v1_row_gets_default_one() {
    let v1 = parse(R_ID_DATA);
    let mut d = decoder_for(R_ID_DATA_VER1);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(10)), ("data", Value::String("a".into()))],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 1);
    assert_eq!(get_i32(&b, "version", 0), 1, "reader default fills v1 row");
    assert_eq!(get_i64(&b, "id", 0), 10);
    assert_eq!(get_str(&b, "data", 0), "a");
}

#[test]
fn a02_added_field_v2_row_keeps_actual() {
    let v2 = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA_VER1);
    d.register_writer_schema(2, R_ID_DATA_VER1).unwrap();
    d.decode(&confluent_frame(
        2,
        &body(
            &v2,
            &[
                ("id", Value::Long(20)),
                ("data", Value::String("b".into())),
                ("version", Value::Int(99)),
            ],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i32(&b, "version", 0), 99, "actual value, not default");
}

#[test]
fn a03_added_field_default_seven() {
    let v1 = parse(R_ID_DATA);
    let mut d = decoder_for(R_ID_DATA_VER7);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(1)), ("data", Value::String("x".into()))],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i32(&b, "version", 0), 7);
}

#[test]
fn a04_mixed_v1_v2_one_batch_rowcount() {
    let v1 = parse(R_ID_DATA);
    let v2 = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA_VER1);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_ID_DATA_VER1).unwrap();
    for i in 0..3 {
        d.decode(&confluent_frame(
            1,
            &body(
                &v1,
                &[("id", Value::Long(i)), ("data", Value::String("v1".into()))],
            ),
        ))
        .unwrap();
    }
    for i in 3..6 {
        d.decode(&confluent_frame(
            2,
            &body(
                &v2,
                &[
                    ("id", Value::Long(i)),
                    ("data", Value::String("v2".into())),
                    ("version", Value::Int(2)),
                ],
            ),
        ))
        .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 6);
    assert_eq!(b.num_columns(), 3);
}

#[test]
fn a05_mixed_v1_v2_default_only_on_v1_rows() {
    let v1 = parse(R_ID_DATA);
    let v2 = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA_VER1);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_ID_DATA_VER1).unwrap();
    // rows: v1(id0), v1(id1), v2(id2,version=55)
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(0)), ("data", Value::String("p".into()))],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(1)), ("data", Value::String("q".into()))],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        2,
        &body(
            &v2,
            &[
                ("id", Value::Long(2)),
                ("data", Value::String("r".into())),
                ("version", Value::Int(55)),
            ],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 3);
    // rows in decode order: two defaults (1), then actual (55)
    let versions: Vec<i32> = (0..3).map(|i| get_i32(&b, "version", i)).collect();
    assert_eq!(versions, vec![1, 1, 55]);
}

#[test]
fn a06_v1_only_batch_all_default() {
    let v1 = parse(R_ID_DATA);
    let mut d = decoder_for(R_ID_DATA_VER1);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    for i in 0..4 {
        d.decode(&confluent_frame(
            1,
            &body(
                &v1,
                &[("id", Value::Long(i)), ("data", Value::String("z".into()))],
            ),
        ))
        .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 4);
    assert!((0..4).all(|i| get_i32(&b, "version", i) == 1));
}

#[test]
fn a07_added_string_field_default_value() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"tag","type":"string","default":"hello"}]}"#;
    let v1 = parse(R_ID);
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, R_ID).unwrap();
    d.decode(&confluent_frame(1, &body(&v1, &[("id", Value::Long(1))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_str(&b, "tag", 0), "hello");
}

#[test]
fn a08_added_field_zero_default() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"cnt","type":"int","default":0}]}"#;
    let v1 = parse(R_ID);
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, R_ID).unwrap();
    d.decode(&confluent_frame(1, &body(&v1, &[("id", Value::Long(1))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i32(&b, "cnt", 0), 0);
}

#[test]
fn a09_added_field_negative_default() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"delta","type":"int","default":-5}]}"#;
    let v1 = parse(R_ID);
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, R_ID).unwrap();
    d.decode(&confluent_frame(1, &body(&v1, &[("id", Value::Long(1))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i32(&b, "delta", 0), -5);
}

#[test]
fn a10_two_added_fields_both_defaults() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"a","type":"int","default":3},{"name":"b","type":"string","default":"z"}]}"#;
    let v1 = parse(R_ID);
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, R_ID).unwrap();
    d.decode(&confluent_frame(1, &body(&v1, &[("id", Value::Long(1))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i32(&b, "a", 0), 3);
    assert_eq!(get_str(&b, "b", 0), "z");
}

#[test]
fn a11_two_v1_generations_flush_defaults() {
    // Two separate flushes: each starts fresh; defaults still fill.
    let v1 = parse(R_ID_DATA);
    let mut d = decoder_for(R_ID_DATA_VER1);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(1)), ("data", Value::String("a".into()))],
        ),
    ))
    .unwrap();
    let b1 = d.flush().unwrap().expect("batch1");
    assert_eq!(get_i32(&b1, "version", 0), 1);
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(2)), ("data", Value::String("b".into()))],
        ),
    ))
    .unwrap();
    let b2 = d.flush().unwrap().expect("batch2");
    assert_uniform(&b2, 1);
    assert_eq!(get_i64(&b2, "id", 0), 2);
    assert_eq!(get_i32(&b2, "version", 0), 1);
}

#[test]
fn a12_added_nullable_union_field_default_null() {
    // reader adds a nullable [null,int] field defaulting to null; v1 writer lacks it.
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"maybe","type":["null","int"],"default":null}]}"#;
    let v1 = parse(R_ID);
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, R_ID).unwrap();
    d.decode(&confluent_frame(1, &body(&v1, &[("id", Value::Long(1))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert!(is_null(&b, "maybe", 0), "null default fills as null");
}

#[test]
fn a13_added_nullable_union_field_nonnull_default() {
    // reader adds [int,null] field with a non-null default; v1 writer lacks it -> default 42.
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"maybe","type":["int","null"],"default":42}]}"#;
    let v1 = parse(R_ID);
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, R_ID).unwrap();
    d.decode(&confluent_frame(1, &body(&v1, &[("id", Value::Long(1))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert!(!is_null(&b, "maybe", 0));
    assert_eq!(get_i32(&b, "maybe", 0), 42);
}

// ===========================================================================
// GROUP B: removed field (writer has it, reader does not -> dropped)
// ===========================================================================

#[test]
fn b01_removed_field_dropped_from_output_schema() {
    // writer has version, reader (R_ID_DATA) does not.
    let w = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA);
    d.register_writer_schema(1, R_ID_DATA_VER1).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w,
            &[
                ("id", Value::Long(1)),
                ("data", Value::String("a".into())),
                ("version", Value::Int(9)),
            ],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_columns(), 2, "writer-only field dropped");
    assert!(
        b.schema().index_of("version").is_err(),
        "version not in output"
    );
}

#[test]
fn b02_removed_field_values_preserved() {
    let w = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA);
    d.register_writer_schema(1, R_ID_DATA_VER1).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w,
            &[
                ("id", Value::Long(77)),
                ("data", Value::String("keep".into())),
                ("version", Value::Int(9)),
            ],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "id", 0), 77);
    assert_eq!(get_str(&b, "data", 0), "keep");
}

#[test]
fn b03_removed_field_rowcount_and_uniform() {
    let w = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA);
    d.register_writer_schema(1, R_ID_DATA_VER1).unwrap();
    for i in 0..5 {
        d.decode(&confluent_frame(
            1,
            &body(
                &w,
                &[
                    ("id", Value::Long(i)),
                    ("data", Value::String("d".into())),
                    ("version", Value::Int(1)),
                ],
            ),
        ))
        .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 5);
}

#[test]
fn b04_removed_field_mixed_with_matching_writer() {
    // reader R_ID_DATA; writer1 R_ID_DATA (matching), writer2 R_ID_DATA_VER1 (extra version dropped)
    let w1 = parse(R_ID_DATA);
    let w2 = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_ID_DATA_VER1).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w1,
            &[("id", Value::Long(1)), ("data", Value::String("a".into()))],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        2,
        &body(
            &w2,
            &[
                ("id", Value::Long(2)),
                ("data", Value::String("b".into())),
                ("version", Value::Int(5)),
            ],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 2);
    assert_eq!(b.num_columns(), 2);
    assert_eq!(get_i64(&b, "id", 0), 1);
    assert_eq!(get_i64(&b, "id", 1), 2);
}

#[test]
fn b05_removed_multiple_fields() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"a","type":"int"},{"name":"b","type":"int"},{"name":"c","type":"string"}]}"#;
    let w = parse(WRITER);
    let mut d = decoder_for(R_ID);
    d.register_writer_schema(1, WRITER).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w,
            &[
                ("id", Value::Long(3)),
                ("a", Value::Int(1)),
                ("b", Value::Int(2)),
                ("c", Value::String("x".into())),
            ],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.num_columns(), 1, "only id survives");
    assert_eq!(get_i64(&b, "id", 0), 3);
}

// ===========================================================================
// GROUP C: reordered fields (same names, different order -> resolve by name)
// ===========================================================================

#[test]
fn c01_reordered_two_fields_values() {
    let w = parse(R_DATA_ID); // writer order: data, id
    let mut d = decoder_for(R_ID_DATA); // reader order: id, data
    d.register_writer_schema(1, R_DATA_ID).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w,
            &[
                ("data", Value::String("hi".into())),
                ("id", Value::Long(88)),
            ],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "id", 0), 88, "resolved by name, not position");
    assert_eq!(get_str(&b, "data", 0), "hi");
}

#[test]
fn c02_reordered_output_in_reader_order() {
    let w = parse(R_DATA_ID);
    let mut d = decoder_for(R_ID_DATA);
    d.register_writer_schema(1, R_DATA_ID).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w,
            &[("data", Value::String("hi".into())), ("id", Value::Long(1))],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(b.schema().field(0).name(), "id", "reader field order");
    assert_eq!(b.schema().field(1).name(), "data");
}

#[test]
fn c03_reordered_three_fields_values() {
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"c","type":"string"},{"name":"a","type":"long"},{"name":"b","type":"int"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"int"},{"name":"c","type":"string"}]}"#;
    let w = parse(WRITER);
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, WRITER).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w,
            &[
                ("c", Value::String("cc".into())),
                ("a", Value::Long(7)),
                ("b", Value::Int(9)),
            ],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "a", 0), 7);
    assert_eq!(get_i32(&b, "b", 0), 9);
    assert_eq!(get_str(&b, "c", 0), "cc");
}

#[test]
fn c04_reordered_mixed_with_inorder_writer() {
    let w_order = parse(R_ID_DATA); // in-order
    let w_reord = parse(R_DATA_ID); // reordered
    let mut d = decoder_for(R_ID_DATA);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_DATA_ID).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w_order,
            &[("id", Value::Long(1)), ("data", Value::String("a".into()))],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        2,
        &body(
            &w_reord,
            &[("data", Value::String("b".into())), ("id", Value::Long(2))],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 2);
    assert_eq!(get_i64(&b, "id", 0), 1);
    assert_eq!(get_i64(&b, "id", 1), 2);
    assert_eq!(get_str(&b, "data", 0), "a");
    assert_eq!(get_str(&b, "data", 1), "b");
}

#[test]
fn c05_reordered_and_added_field_combined() {
    // writer reordered + missing version -> resolved by name and default-filled.
    let w = parse(R_DATA_ID);
    let mut d = decoder_for(R_ID_DATA_VER7);
    d.register_writer_schema(1, R_DATA_ID).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w,
            &[("data", Value::String("x".into())), ("id", Value::Long(5))],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "id", 0), 5);
    assert_eq!(get_str(&b, "data", 0), "x");
    assert_eq!(get_i32(&b, "version", 0), 7);
}

// ===========================================================================
// GROUP D: type promotion int -> long
// ===========================================================================

#[test]
fn d01_promote_int_to_long_basic() {
    let w = parse(R_ID_N_INT);
    let mut d = decoder_for(R_ID_N_LONG);
    d.register_writer_schema(1, R_ID_N_INT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("n", Value::Int(1234))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "n", 0), 1234, "int promoted to long");
}

#[test]
fn d02_promote_int_to_long_column_is_int64() {
    let w = parse(R_ID_N_INT);
    let mut d = decoder_for(R_ID_N_LONG);
    d.register_writer_schema(1, R_ID_N_INT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("n", Value::Int(5))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert!(
        b.column(b.schema().index_of("n").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .is_some(),
        "promoted column must be Int64 (reader type)"
    );
}

#[test]
fn d03_promote_int_to_long_zero() {
    let w = parse(R_ID_N_INT);
    let mut d = decoder_for(R_ID_N_LONG);
    d.register_writer_schema(1, R_ID_N_INT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("n", Value::Int(0))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "n", 0), 0);
}

#[test]
fn d04_promote_int_to_long_negative() {
    let w = parse(R_ID_N_INT);
    let mut d = decoder_for(R_ID_N_LONG);
    d.register_writer_schema(1, R_ID_N_INT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("n", Value::Int(-42))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "n", 0), -42);
}

#[test]
fn d05_promote_int_to_long_i32_max() {
    let w = parse(R_ID_N_INT);
    let mut d = decoder_for(R_ID_N_LONG);
    d.register_writer_schema(1, R_ID_N_INT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("n", Value::Int(i32::MAX))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "n", 0), i32::MAX as i64);
}

#[test]
fn d06_promote_int_to_long_i32_min() {
    let w = parse(R_ID_N_INT);
    let mut d = decoder_for(R_ID_N_LONG);
    d.register_writer_schema(1, R_ID_N_INT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("n", Value::Int(i32::MIN))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "n", 0), i32::MIN as i64);
}

#[test]
fn d07_mixed_int_and_long_writers_rowcount() {
    let wi = parse(R_ID_N_INT);
    let wl = parse(R_ID_N_LONG);
    let mut d = decoder_for(R_ID_N_LONG);
    d.register_writer_schema(1, R_ID_N_INT).unwrap();
    d.register_writer_schema(2, R_ID_N_LONG).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&wi, &[("id", Value::Long(1)), ("n", Value::Int(7))]),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        2,
        &body(
            &wl,
            &[("id", Value::Long(2)), ("n", Value::Long(8_000_000_000))],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 2);
    assert_eq!(get_i64(&b, "n", 0), 7, "promoted int");
    assert_eq!(get_i64(&b, "n", 1), 8_000_000_000, "native long beyond i32");
}

#[test]
fn d08_mixed_int_long_columns_uniform_many() {
    let wi = parse(R_ID_N_INT);
    let wl = parse(R_ID_N_LONG);
    let mut d = decoder_for(R_ID_N_LONG);
    d.register_writer_schema(1, R_ID_N_INT).unwrap();
    d.register_writer_schema(2, R_ID_N_LONG).unwrap();
    for i in 0..6 {
        if i % 2 == 0 {
            d.decode(&confluent_frame(
                1,
                &body(&wi, &[("id", Value::Long(i)), ("n", Value::Int(i as i32))]),
            ))
            .unwrap();
        } else {
            d.decode(&confluent_frame(
                2,
                &body(&wl, &[("id", Value::Long(i)), ("n", Value::Long(i))]),
            ))
            .unwrap();
        }
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 6);
    for i in 0..6 {
        assert_eq!(get_i64(&b, "n", i), i as i64);
    }
}

// ===========================================================================
// GROUP E: type promotion float -> double
// ===========================================================================

#[test]
fn e01_promote_float_to_double_basic() {
    let w = parse(R_ID_X_FLOAT);
    let mut d = decoder_for(R_ID_X_DOUBLE);
    d.register_writer_schema(1, R_ID_X_FLOAT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("x", Value::Float(1.5))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(
        get_f64(&b, "x", 0),
        1.5f64,
        "float widened to double exactly"
    );
}

#[test]
fn e02_promote_float_to_double_column_is_f64() {
    let w = parse(R_ID_X_FLOAT);
    let mut d = decoder_for(R_ID_X_DOUBLE);
    d.register_writer_schema(1, R_ID_X_FLOAT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("x", Value::Float(2.0))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert!(
        b.column(b.schema().index_of("x").unwrap())
            .as_any()
            .downcast_ref::<Float64Array>()
            .is_some(),
        "promoted column must be Float64"
    );
}

#[test]
fn e03_promote_float_to_double_zero() {
    let w = parse(R_ID_X_FLOAT);
    let mut d = decoder_for(R_ID_X_DOUBLE);
    d.register_writer_schema(1, R_ID_X_FLOAT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("x", Value::Float(0.0))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_f64(&b, "x", 0), 0.0f64);
}

#[test]
fn e04_promote_float_to_double_negative() {
    let w = parse(R_ID_X_FLOAT);
    let mut d = decoder_for(R_ID_X_DOUBLE);
    d.register_writer_schema(1, R_ID_X_FLOAT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("x", Value::Float(-2.25))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_f64(&b, "x", 0), -2.25f64);
}

#[test]
fn e05_mixed_float_double_writers() {
    let wf = parse(R_ID_X_FLOAT);
    let wd = parse(R_ID_X_DOUBLE);
    let mut d = decoder_for(R_ID_X_DOUBLE);
    d.register_writer_schema(1, R_ID_X_FLOAT).unwrap();
    d.register_writer_schema(2, R_ID_X_DOUBLE).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&wf, &[("id", Value::Long(1)), ("x", Value::Float(1.5))]),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        2,
        &body(&wd, &[("id", Value::Long(2)), ("x", Value::Double(3.5))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 2);
    assert_eq!(get_f64(&b, "x", 0), 1.5);
    assert_eq!(get_f64(&b, "x", 1), 3.5);
}

#[test]
fn e06_no_promotion_float_stays_float() {
    // Sanity: float writer + float reader -> Float32 preserved (no accidental widening).
    let w = parse(R_ID_X_FLOAT);
    let mut d = decoder_for(R_ID_X_FLOAT);
    d.register_writer_schema(1, R_ID_X_FLOAT).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("x", Value::Float(1.25))]),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_f32(&b, "x", 0), 1.25f32);
}

// ===========================================================================
// GROUP F: MANY (5+) distinct writer ids interleaved row-by-row (desync regression)
// ===========================================================================

#[test]
fn f01_five_ids_same_schema_interleaved_rowcount() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    for id in 1..=5u32 {
        d.register_writer_schema(id, R_SEQ).unwrap();
    }
    // 10 rows, cycling id 1..5 so EVERY consecutive row switches generation.
    for i in 0..10i64 {
        let id = (i as u32 % 5) + 1;
        d.decode(&confluent_frame(id, &body(&w, &[("seq", Value::Long(i))])))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 10);
}

#[test]
fn f02_five_ids_interleaved_values_in_order() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    for id in 1..=5u32 {
        d.register_writer_schema(id, R_SEQ).unwrap();
    }
    for i in 0..10i64 {
        let id = (i as u32 % 5) + 1;
        d.decode(&confluent_frame(id, &body(&w, &[("seq", Value::Long(i))])))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    // Row order must be preserved across generations.
    for i in 0..10 {
        assert_eq!(
            get_i64(&b, "seq", i),
            i as i64,
            "row order preserved across generations"
        );
    }
}

#[test]
fn f03_ten_ids_row_by_row_rowcount() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    for id in 1..=10u32 {
        d.register_writer_schema(id, R_SEQ).unwrap();
    }
    for i in 0..10i64 {
        d.decode(&confluent_frame(
            (i as u32) + 1,
            &body(&w, &[("seq", Value::Long(i * 100))]),
        ))
        .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 10);
    for i in 0..10 {
        assert_eq!(get_i64(&b, "seq", i), (i as i64) * 100);
    }
}

#[test]
fn f04_five_ids_with_default_all_filled() {
    // reader adds version; 5 v1 writer ids interleaved -> every row default-filled, uniform.
    let v1 = parse(R_ID_DATA);
    let mut d = decoder_for(R_ID_DATA_VER7);
    for id in 1..=5u32 {
        d.register_writer_schema(id, R_ID_DATA).unwrap();
    }
    for i in 0..15i64 {
        let id = (i as u32 % 5) + 1;
        d.decode(&confluent_frame(
            id,
            &body(
                &v1,
                &[("id", Value::Long(i)), ("data", Value::String("d".into()))],
            ),
        ))
        .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 15);
    assert!(
        (0..15).all(|i| get_i32(&b, "version", i) == 7),
        "all default-filled"
    );
    for i in 0..15 {
        assert_eq!(get_i64(&b, "id", i), i as i64);
    }
}

#[test]
fn f05_seven_ids_alternating_rowcount() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    for id in 1..=7u32 {
        d.register_writer_schema(id, R_SEQ).unwrap();
    }
    for i in 0..21i64 {
        let id = (i as u32 % 7) + 1;
        d.decode(&confluent_frame(id, &body(&w, &[("seq", Value::Long(i))])))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 21);
}

#[test]
fn f06_five_distinct_schemas_interleaved_rowcount() {
    // 5 genuinely different writer schemas, all resolving to one reader {id:long,data:string,version:int}.
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":5}]}"#;
    const W1: &str = R_ID_DATA; // no version -> default
    const W2: &str = R_ID_DATA_VER1; // has version
    const W3: &str = R_DATA_ID; // reordered, no version -> default
    const W4: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":1},{"name":"junk","type":"string","default":"j"}]}"#; // junk dropped
    const W5: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"int"},{"name":"data","type":"string"},{"name":"version","type":"int","default":1}]}"#; // id int -> promote

    let (w1, w2, w3, w4, w5) = (parse(W1), parse(W2), parse(W3), parse(W4), parse(W5));
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, W1).unwrap();
    d.register_writer_schema(2, W2).unwrap();
    d.register_writer_schema(3, W3).unwrap();
    d.register_writer_schema(4, W4).unwrap();
    d.register_writer_schema(5, W5).unwrap();

    d.decode(&confluent_frame(
        1,
        &body(
            &w1,
            &[("id", Value::Long(0)), ("data", Value::String("a".into()))],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        2,
        &body(
            &w2,
            &[
                ("id", Value::Long(1)),
                ("data", Value::String("b".into())),
                ("version", Value::Int(11)),
            ],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        3,
        &body(
            &w3,
            &[("data", Value::String("c".into())), ("id", Value::Long(2))],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        4,
        &body(
            &w4,
            &[
                ("id", Value::Long(3)),
                ("data", Value::String("d".into())),
                ("version", Value::Int(22)),
                ("junk", Value::String("k".into())),
            ],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        5,
        &body(
            &w5,
            &[
                ("id", Value::Int(4)),
                ("data", Value::String("e".into())),
                ("version", Value::Int(33)),
            ],
        ),
    ))
    .unwrap();

    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 5);
    assert_eq!(b.num_columns(), 3, "junk dropped, output = reader fields");
    for i in 0..5 {
        assert_eq!(get_i64(&b, "id", i), i as i64);
    }
    let versions: Vec<i32> = (0..5).map(|i| get_i32(&b, "version", i)).collect();
    assert_eq!(
        versions,
        vec![5, 11, 5, 22, 33],
        "defaults where absent, actual otherwise"
    );
}

#[test]
fn f07_five_distinct_schemas_columns_uniform() {
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":5}]}"#;
    const W4: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":1},{"name":"junk","type":"string","default":"j"}]}"#;
    const W5: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"int"},{"name":"data","type":"string"},{"name":"version","type":"int","default":1}]}"#;
    let (w1, w2, w3, w4, w5) = (
        parse(R_ID_DATA),
        parse(R_ID_DATA_VER1),
        parse(R_DATA_ID),
        parse(W4),
        parse(W5),
    );
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_ID_DATA_VER1).unwrap();
    d.register_writer_schema(3, R_DATA_ID).unwrap();
    d.register_writer_schema(4, W4).unwrap();
    d.register_writer_schema(5, W5).unwrap();
    // interleave 20 rows across the 5 schemas
    for i in 0..20i64 {
        match i % 5 {
            0 => d
                .decode(&confluent_frame(
                    1,
                    &body(
                        &w1,
                        &[("id", Value::Long(i)), ("data", Value::String("a".into()))],
                    ),
                ))
                .unwrap(),
            1 => d
                .decode(&confluent_frame(
                    2,
                    &body(
                        &w2,
                        &[
                            ("id", Value::Long(i)),
                            ("data", Value::String("b".into())),
                            ("version", Value::Int(2)),
                        ],
                    ),
                ))
                .unwrap(),
            2 => d
                .decode(&confluent_frame(
                    3,
                    &body(
                        &w3,
                        &[("data", Value::String("c".into())), ("id", Value::Long(i))],
                    ),
                ))
                .unwrap(),
            3 => d
                .decode(&confluent_frame(
                    4,
                    &body(
                        &w4,
                        &[
                            ("id", Value::Long(i)),
                            ("data", Value::String("d".into())),
                            ("version", Value::Int(4)),
                            ("junk", Value::String("k".into())),
                        ],
                    ),
                ))
                .unwrap(),
            _ => d
                .decode(&confluent_frame(
                    5,
                    &body(
                        &w5,
                        &[
                            ("id", Value::Int(i as i32)),
                            ("data", Value::String("e".into())),
                            ("version", Value::Int(5)),
                        ],
                    ),
                ))
                .unwrap(),
        };
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 20);
    assert_eq!(b.num_columns(), 3);
}

#[test]
fn f08_large_interleave_fifty_rows() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    for id in 1..=5u32 {
        d.register_writer_schema(id, R_SEQ).unwrap();
    }
    for i in 0..50i64 {
        let id = (i as u32 % 5) + 1;
        d.decode(&confluent_frame(id, &body(&w, &[("seq", Value::Long(i))])))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 50);
    for i in 0..50 {
        assert_eq!(get_i64(&b, "seq", i), i as i64);
    }
}

#[test]
fn f09_two_ids_twenty_rows_uniform() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    d.register_writer_schema(1, R_SEQ).unwrap();
    d.register_writer_schema(2, R_SEQ).unwrap();
    for i in 0..20i64 {
        let id = (i as u32 % 2) + 1;
        d.decode(&confluent_frame(id, &body(&w, &[("seq", Value::Long(i))])))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 20);
}

#[test]
fn f10_stress_100_rows_five_ids_no_reader() {
    // No reader schema: target derived from first writer; all same schema, 100 rows across 5 ids.
    let w = parse(R_SEQ);
    let mut d = ConfluentAvroDecoder::new();
    for id in 1..=5u32 {
        d.register_writer_schema(id, R_SEQ).unwrap();
    }
    for i in 0..100i64 {
        let id = (i as u32 % 5) + 1;
        d.decode(&confluent_frame(id, &body(&w, &[("seq", Value::Long(i))])))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 100);
    for i in 0..100 {
        assert_eq!(get_i64(&b, "seq", i), i as i64);
    }
}

// ===========================================================================
// GROUP G: a writer id that appears, disappears, reappears
// ===========================================================================

#[test]
fn g01_writer_id_reappears_rowcount() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    d.register_writer_schema(1, R_SEQ).unwrap();
    d.register_writer_schema(2, R_SEQ).unwrap();
    // A, A, B, A  -> id1 appears, id2 interposes, id1 reappears
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(0))])))
        .unwrap();
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(1))])))
        .unwrap();
    d.decode(&confluent_frame(2, &body(&w, &[("seq", Value::Long(2))])))
        .unwrap();
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(3))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 4);
    for i in 0..4 {
        assert_eq!(get_i64(&b, "seq", i), i as i64, "order across reappearance");
    }
}

#[test]
fn g02_abab_pattern() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    d.register_writer_schema(1, R_SEQ).unwrap();
    d.register_writer_schema(2, R_SEQ).unwrap();
    for i in 0..8i64 {
        let id = (i as u32 % 2) + 1;
        d.decode(&confluent_frame(id, &body(&w, &[("seq", Value::Long(i))])))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 8);
    for i in 0..8 {
        assert_eq!(get_i64(&b, "seq", i), i as i64);
    }
}

#[test]
fn g03_abcabc_pattern() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    for id in 1..=3u32 {
        d.register_writer_schema(id, R_SEQ).unwrap();
    }
    for i in 0..9i64 {
        let id = (i as u32 % 3) + 1;
        d.decode(&confluent_frame(id, &body(&w, &[("seq", Value::Long(i))])))
            .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 9);
}

#[test]
fn g04_reappear_after_many_others() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    for id in 1..=4u32 {
        d.register_writer_schema(id, R_SEQ).unwrap();
    }
    // 1, then 2,3,4, then 1 again
    let order = [1u32, 2, 3, 4, 1];
    for (i, id) in order.iter().enumerate() {
        d.decode(&confluent_frame(
            *id,
            &body(&w, &[("seq", Value::Long(i as i64))]),
        ))
        .unwrap();
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 5);
    for i in 0..5 {
        assert_eq!(get_i64(&b, "seq", i), i as i64);
    }
}

#[test]
fn g05_reappear_with_evolving_defaults() {
    // v1 (no version) reappears around a v2 row; defaults must stay correct per generation.
    let v1 = parse(R_ID_DATA);
    let v2 = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA_VER7);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_ID_DATA_VER1).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(0)), ("data", Value::String("a".into()))],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        2,
        &body(
            &v2,
            &[
                ("id", Value::Long(1)),
                ("data", Value::String("b".into())),
                ("version", Value::Int(50)),
            ],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(2)), ("data", Value::String("c".into()))],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 3);
    let versions: Vec<i32> = (0..3).map(|i| get_i32(&b, "version", i)).collect();
    assert_eq!(versions, vec![7, 50, 7]);
}

// ===========================================================================
// GROUP H: empty batch / register-then-never-decode / flush semantics
// ===========================================================================

#[test]
fn h01_flush_zero_decodes_is_none() {
    let mut d = decoder_for(R_ID);
    d.register_writer_schema(1, R_ID).unwrap();
    assert!(
        d.flush().unwrap().is_none(),
        "flush with zero decodes -> None"
    );
}

#[test]
fn h02_flush_twice_second_is_none() {
    let w = parse(R_ID);
    let mut d = decoder_for(R_ID);
    d.register_writer_schema(1, R_ID).unwrap();
    d.decode(&confluent_frame(1, &body(&w, &[("id", Value::Long(1))])))
        .unwrap();
    assert!(d.flush().unwrap().is_some());
    assert!(
        d.flush().unwrap().is_none(),
        "second flush has no residual rows"
    );
}

#[test]
fn h03_register_but_never_decode_adds_no_rows() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    d.register_writer_schema(1, R_SEQ).unwrap();
    d.register_writer_schema(2, R_SEQ).unwrap(); // never decoded
    d.register_writer_schema(3, R_SEQ).unwrap(); // never decoded
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(42))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 1);
    assert_eq!(get_i64(&b, "seq", 0), 42);
}

#[test]
fn h04_register_many_decode_one() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    for id in 1..=20u32 {
        d.register_writer_schema(id, R_SEQ).unwrap();
    }
    d.decode(&confluent_frame(7, &body(&w, &[("seq", Value::Long(7))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 1);
    assert_eq!(get_i64(&b, "seq", 0), 7);
}

#[test]
fn h05_has_writer_schema_true_after_register() {
    let mut d = decoder_for(R_ID);
    d.register_writer_schema(11, R_ID).unwrap();
    assert!(d.has_writer_schema(11));
}

#[test]
fn h06_has_writer_schema_false_for_unregistered() {
    let d = decoder_for(R_ID);
    assert!(!d.has_writer_schema(999));
}

#[test]
fn h07_flush_resets_between_batches() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    d.register_writer_schema(1, R_SEQ).unwrap();
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(1))])))
        .unwrap();
    let b1 = d.flush().unwrap().expect("b1");
    assert_uniform(&b1, 1);
    // Fresh batch: only the new row appears.
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(2))])))
        .unwrap();
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(3))])))
        .unwrap();
    let b2 = d.flush().unwrap().expect("b2");
    assert_uniform(&b2, 2);
    assert_eq!(get_i64(&b2, "seq", 0), 2);
    assert_eq!(get_i64(&b2, "seq", 1), 3);
}

#[test]
fn h08_flush_no_schema_at_all_errors() {
    // brand-new decoder, no reader and no writer registered -> flush has no target schema.
    let mut d = ConfluentAvroDecoder::new();
    assert!(
        d.flush().is_err(),
        "flush with no schema must error, not panic"
    );
}

#[test]
fn h09_registered_ids_persist_across_flush() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    d.register_writer_schema(1, R_SEQ).unwrap();
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(1))])))
        .unwrap();
    let _ = d.flush().unwrap();
    assert!(
        d.has_writer_schema(1),
        "registered schema retained after flush"
    );
    // and still decodes without re-registration
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(2))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "seq", 0), 2);
}

// ===========================================================================
// GROUP I: mid-stream registration must not drop buffered rows
// ===========================================================================

#[test]
fn i01_register_midstream_preserves_buffered_rows() {
    // Decode a few rows for id1, then register a NEW id2 mid-stream (comment: must not drop the
    // live decoder / buffered rows), then flush -> all id1 rows survive.
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    d.register_writer_schema(1, R_SEQ).unwrap();
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(0))])))
        .unwrap();
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(1))])))
        .unwrap();
    // new id registered while a generation for id1 is live and buffered
    d.register_writer_schema(2, R_SEQ).unwrap();
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(2))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 3);
    for i in 0..3 {
        assert_eq!(
            get_i64(&b, "seq", i),
            i as i64,
            "buffered rows survive mid-stream register"
        );
    }
}

#[test]
fn i02_register_midstream_then_switch() {
    let w = parse(R_SEQ);
    let mut d = decoder_for(R_SEQ);
    d.register_writer_schema(1, R_SEQ).unwrap();
    d.decode(&confluent_frame(1, &body(&w, &[("seq", Value::Long(0))])))
        .unwrap();
    d.register_writer_schema(2, R_SEQ).unwrap();
    d.decode(&confluent_frame(2, &body(&w, &[("seq", Value::Long(1))])))
        .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 2);
    assert_eq!(get_i64(&b, "seq", 0), 0);
    assert_eq!(get_i64(&b, "seq", 1), 1);
}

#[test]
#[ignore = "by design (not a regression): re-registering a different schema under an existing Confluent id is rejected (ids are immutable); the Kafka source never does this (has_writer_schema guard)."]
fn i03_re_register_same_id_new_schema() {
    // Re-registering the same id with a broader schema (adds a field) should take effect for the
    // next generation. Reader has version(default 9); v1 then re-registered-as-v2 under same id.
    let v1 = parse(R_ID_DATA);
    let v2 = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA_VER7);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(0)), ("data", Value::String("a".into()))],
        ),
    ))
    .unwrap();
    // flush the v1 generation so the id "changes" semantics are clean, then re-register id1 as v2.
    let b1 = d.flush().unwrap().expect("b1");
    assert_eq!(get_i32(&b1, "version", 0), 7, "v1 default-filled");
    d.register_writer_schema(1, R_ID_DATA_VER1).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &v2,
            &[
                ("id", Value::Long(1)),
                ("data", Value::String("b".into())),
                ("version", Value::Int(3)),
            ],
        ),
    ))
    .unwrap();
    let b2 = d.flush().unwrap().expect("b2");
    assert_eq!(
        get_i32(&b2, "version", 0),
        3,
        "re-registered schema now carries actual version"
    );
}

// ===========================================================================
// GROUP J: target-schema oracle checks (convert_avro_schema_to_arrow)
// ===========================================================================

#[test]
fn j01_reader_target_includes_added_field() {
    let reader = parse(R_ID_DATA_VER1);
    let target = convert_avro_schema_to_arrow(reader);
    assert_eq!(target.fields().len(), 3);
    assert!(target.index_of("version").is_ok());
}

#[test]
fn j02_removed_field_target_excludes_it() {
    let reader = parse(R_ID_DATA);
    let target = convert_avro_schema_to_arrow(reader);
    assert_eq!(target.fields().len(), 2);
    assert!(target.index_of("version").is_err());
}

#[test]
fn j03_promotion_target_is_int64() {
    use arrow::datatypes::DataType;
    let reader = parse(R_ID_N_LONG);
    let target = convert_avro_schema_to_arrow(reader);
    assert_eq!(
        target.field(target.index_of("n").unwrap()).data_type(),
        &DataType::Int64
    );
}

#[test]
fn j04_reorder_target_in_reader_order() {
    let reader = parse(R_ID_DATA); // id, data
    let target = convert_avro_schema_to_arrow(reader);
    assert_eq!(target.field(0).name(), "id");
    assert_eq!(target.field(1).name(), "data");
}

#[test]
fn j05_decoder_target_schema_present_after_reader() {
    let d = decoder_for(R_ID_DATA_VER1);
    let t = d.target_schema().expect("target set from reader");
    assert_eq!(t.fields().len(), 3);
}

#[test]
fn j06_no_reader_first_writer_sets_target() {
    let mut d = ConfluentAvroDecoder::new();
    assert!(d.target_schema().is_none(), "no schema yet");
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    let t = d.target_schema().expect("target from first writer");
    assert_eq!(t.fields().len(), 2);
}

// ===========================================================================
// GROUP K: no-reader path -- coercion to first-writer target by name
// ===========================================================================

#[test]
fn k01_no_reader_reordered_writer_coerces_by_name() {
    // First writer sets target {id,data}; a reordered second writer decodes then coerces by name.
    let w1 = parse(R_ID_DATA);
    let w2 = parse(R_DATA_ID);
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_DATA_ID).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w1,
            &[("id", Value::Long(1)), ("data", Value::String("a".into()))],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        2,
        &body(
            &w2,
            &[("data", Value::String("b".into())), ("id", Value::Long(2))],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 2);
    // target field order (from first writer): id, data
    assert_eq!(b.schema().field(0).name(), "id");
    assert_eq!(get_i64(&b, "id", 0), 1);
    assert_eq!(get_i64(&b, "id", 1), 2, "reordered writer coerced by name");
    assert_eq!(get_str(&b, "data", 1), "b");
}

#[test]
fn k02_no_reader_single_writer_roundtrip() {
    let w = parse(R_ID_DATA);
    let mut d = ConfluentAvroDecoder::new();
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &w,
            &[("id", Value::Long(9)), ("data", Value::String("q".into()))],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i64(&b, "id", 0), 9);
    assert_eq!(get_str(&b, "data", 0), "q");
}

// ===========================================================================
// GROUP L: three-generation evolution v1 -> v2 -> v3
// ===========================================================================

#[test]
fn l01_three_versions_added_fields_rowcount() {
    const V3: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":1},{"name":"extra","type":"string","default":"e"}]}"#;
    let v1 = parse(R_ID_DATA);
    let v2 = parse(R_ID_DATA_VER1);
    let v3 = parse(V3);
    let mut d = decoder_for(V3);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_ID_DATA_VER1).unwrap();
    d.register_writer_schema(3, V3).unwrap();
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(0)), ("data", Value::String("a".into()))],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        2,
        &body(
            &v2,
            &[
                ("id", Value::Long(1)),
                ("data", Value::String("b".into())),
                ("version", Value::Int(2)),
            ],
        ),
    ))
    .unwrap();
    d.decode(&confluent_frame(
        3,
        &body(
            &v3,
            &[
                ("id", Value::Long(2)),
                ("data", Value::String("c".into())),
                ("version", Value::Int(3)),
                ("extra", Value::String("z".into())),
            ],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 3);
    assert_eq!(b.num_columns(), 4);
}

#[test]
fn l02_three_versions_default_and_extra_values() {
    const V3: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"data","type":"string"},{"name":"version","type":"int","default":1},{"name":"extra","type":"string","default":"e"}]}"#;
    let v1 = parse(R_ID_DATA);
    let v3 = parse(V3);
    let mut d = decoder_for(V3);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(3, V3).unwrap();
    // v1 row: version + extra both defaulted
    d.decode(&confluent_frame(
        1,
        &body(
            &v1,
            &[("id", Value::Long(0)), ("data", Value::String("a".into()))],
        ),
    ))
    .unwrap();
    // v3 row: both actual
    d.decode(&confluent_frame(
        3,
        &body(
            &v3,
            &[
                ("id", Value::Long(1)),
                ("data", Value::String("b".into())),
                ("version", Value::Int(9)),
                ("extra", Value::String("zz".into())),
            ],
        ),
    ))
    .unwrap();
    let b = d.flush().unwrap().expect("batch");
    assert_eq!(get_i32(&b, "version", 0), 1);
    assert_eq!(get_str(&b, "extra", 0), "e");
    assert_eq!(get_i32(&b, "version", 1), 9);
    assert_eq!(get_str(&b, "extra", 1), "zz");
}

// ===========================================================================
// GROUP M: malformed / adversarial resolution edges
// ===========================================================================

#[test]
fn m01_decode_unregistered_id_errors() {
    let w = parse(R_ID);
    let mut d = decoder_for(R_ID);
    d.register_writer_schema(1, R_ID).unwrap();
    let frame = confluent_frame(999, &body(&w, &[("id", Value::Long(1))]));
    assert!(
        d.decode(&frame).is_err(),
        "unknown writer id must error, not panic"
    );
}

#[test]
fn m02_frame_too_short_errors() {
    let mut d = decoder_for(R_ID);
    d.register_writer_schema(1, R_ID).unwrap();
    assert!(d.decode(&[0x00, 0x00, 0x00]).is_err());
}

#[test]
fn m03_bad_magic_byte_errors() {
    let w = parse(R_ID);
    let mut d = decoder_for(R_ID);
    d.register_writer_schema(1, R_ID).unwrap();
    let mut frame = confluent_frame(1, &body(&w, &[("id", Value::Long(1))]));
    frame[0] = 0x01; // corrupt magic
    assert!(d.decode(&frame).is_err());
}

#[test]
fn m04_reader_added_required_field_no_default_errors() {
    // Avro spec: a reader field with NO default that the writer lacks is a resolution error.
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"req","type":"int"}]}"#;
    let w = parse(R_ID);
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, R_ID).unwrap();
    let frame = confluent_frame(1, &body(&w, &[("id", Value::Long(1))]));
    // Either decode or flush must fail; it must NOT silently produce a garbage/zero value.
    let ok = d.decode(&frame).is_ok() && d.flush().is_ok();
    assert!(
        !ok,
        "reader-only required field w/o default must not silently succeed"
    );
}

#[test]
fn m05_incompatible_type_change_errors() {
    // writer field `n` is a string; reader expects long — not a valid promotion.
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"n","type":"string"}]}"#;
    let w = parse(WRITER);
    let mut d = decoder_for(R_ID_N_LONG);
    d.register_writer_schema(1, WRITER).unwrap();
    let frame = confluent_frame(
        1,
        &body(
            &w,
            &[("id", Value::Long(1)), ("n", Value::String("x".into()))],
        ),
    );
    let ok = d.decode(&frame).is_ok() && d.flush().is_ok();
    assert!(
        !ok,
        "incompatible string->long change must not silently succeed"
    );
}

#[test]
fn m06_narrowing_long_to_int_reader_rejected_or_no_silent_truncation() {
    // Reader `n` is int, writer `n` is long — Avro does NOT allow long->int promotion.
    const WRITER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"n","type":"long"}]}"#;
    const READER: &str = r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"n","type":"int"}]}"#;
    let w = parse(WRITER);
    let mut d = decoder_for(READER);
    d.register_writer_schema(1, WRITER).unwrap();
    let frame = confluent_frame(
        1,
        &body(&w, &[("id", Value::Long(1)), ("n", Value::Long(1))]),
    );
    let ok = d.decode(&frame).is_ok() && d.flush().is_ok();
    assert!(
        !ok,
        "long->int narrowing is not a valid Avro promotion; must not silently succeed"
    );
}

// ===========================================================================
// GROUP N: interleave with per-row default/removed variation (desync stress)
// ===========================================================================

#[test]
fn n01_alternating_v1_v2_row_by_row_rowcount() {
    let v1 = parse(R_ID_DATA);
    let v2 = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA_VER1);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_ID_DATA_VER1).unwrap();
    for i in 0..10i64 {
        if i % 2 == 0 {
            d.decode(&confluent_frame(
                1,
                &body(
                    &v1,
                    &[("id", Value::Long(i)), ("data", Value::String("a".into()))],
                ),
            ))
            .unwrap();
        } else {
            d.decode(&confluent_frame(
                2,
                &body(
                    &v2,
                    &[
                        ("id", Value::Long(i)),
                        ("data", Value::String("b".into())),
                        ("version", Value::Int(i as i32)),
                    ],
                ),
            ))
            .unwrap();
        }
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 10);
}

#[test]
fn n02_alternating_v1_v2_version_values() {
    let v1 = parse(R_ID_DATA);
    let v2 = parse(R_ID_DATA_VER1);
    let mut d = decoder_for(R_ID_DATA_VER1); // default version = 1
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_ID_DATA_VER1).unwrap();
    for i in 0..6i64 {
        if i % 2 == 0 {
            d.decode(&confluent_frame(
                1,
                &body(
                    &v1,
                    &[("id", Value::Long(i)), ("data", Value::String("a".into()))],
                ),
            ))
            .unwrap();
        } else {
            d.decode(&confluent_frame(
                2,
                &body(
                    &v2,
                    &[
                        ("id", Value::Long(i)),
                        ("data", Value::String("b".into())),
                        ("version", Value::Int(100 + i as i32)),
                    ],
                ),
            ))
            .unwrap();
        }
    }
    let b = d.flush().unwrap().expect("batch");
    // even rows: default 1; odd rows: 100+i
    let expected: Vec<i32> = (0..6)
        .map(|i| if i % 2 == 0 { 1 } else { 100 + i })
        .collect();
    let actual: Vec<i32> = (0..6).map(|i| get_i32(&b, "version", i as usize)).collect();
    assert_eq!(actual, expected);
}

#[test]
fn n03_alternating_reordered_and_inorder_rowcount() {
    let w1 = parse(R_ID_DATA);
    let w2 = parse(R_DATA_ID);
    let mut d = decoder_for(R_ID_DATA);
    d.register_writer_schema(1, R_ID_DATA).unwrap();
    d.register_writer_schema(2, R_DATA_ID).unwrap();
    for i in 0..12i64 {
        if i % 2 == 0 {
            d.decode(&confluent_frame(
                1,
                &body(
                    &w1,
                    &[("id", Value::Long(i)), ("data", Value::String("a".into()))],
                ),
            ))
            .unwrap();
        } else {
            d.decode(&confluent_frame(
                2,
                &body(
                    &w2,
                    &[("data", Value::String("b".into())), ("id", Value::Long(i))],
                ),
            ))
            .unwrap();
        }
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 12);
    for i in 0..12 {
        assert_eq!(get_i64(&b, "id", i), i as i64);
    }
}

#[test]
fn n04_alternating_int_long_promotion_values() {
    let wi = parse(R_ID_N_INT);
    let wl = parse(R_ID_N_LONG);
    let mut d = decoder_for(R_ID_N_LONG);
    d.register_writer_schema(1, R_ID_N_INT).unwrap();
    d.register_writer_schema(2, R_ID_N_LONG).unwrap();
    for i in 0..8i64 {
        if i % 2 == 0 {
            d.decode(&confluent_frame(
                1,
                &body(&wi, &[("id", Value::Long(i)), ("n", Value::Int(i as i32))]),
            ))
            .unwrap();
        } else {
            d.decode(&confluent_frame(
                2,
                &body(&wl, &[("id", Value::Long(i)), ("n", Value::Long(i * 1000))]),
            ))
            .unwrap();
        }
    }
    let b = d.flush().unwrap().expect("batch");
    assert_uniform(&b, 8);
    for i in 0..8 {
        let expect = if i % 2 == 0 {
            i as i64
        } else {
            i as i64 * 1000
        };
        assert_eq!(get_i64(&b, "n", i), expect);
    }
}

// ===========================================================================
// GROUP O: table-driven scale-out (added-field default values across many defaults)
// ===========================================================================

#[test]
fn o01_added_int_default_matrix() {
    // Each case: reader adds `k` with a distinct default; v1 writer lacks it -> that default fills.
    let defaults: [i32; 12] = [
        0,
        1,
        -1,
        2,
        -2,
        7,
        100,
        -100,
        i32::MAX,
        i32::MIN,
        12345,
        -98765,
    ];
    let v1 = parse(R_ID);
    for (case_i, def) in defaults.iter().enumerate() {
        let reader = format!(
            r#"{{"type":"record","name":"R","fields":[{{"name":"id","type":"long"}},{{"name":"k","type":"int","default":{def}}}]}}"#
        );
        let mut d = decoder_for(&reader);
        d.register_writer_schema(1, R_ID).unwrap();
        d.decode(&confluent_frame(
            1,
            &body(&v1, &[("id", Value::Long(case_i as i64))]),
        ))
        .unwrap();
        let b = d.flush().unwrap().expect("batch");
        assert_eq!(
            get_i32(&b, "k", 0),
            *def,
            "case {case_i}: default {def} must fill"
        );
        assert_eq!(get_i64(&b, "id", 0), case_i as i64);
    }
}

#[test]
fn o02_added_string_default_matrix() {
    let defaults = [
        "",
        "a",
        "hello world",
        "unicode-\u{00e9}",
        "123",
        "  spaced  ",
    ];
    let v1 = parse(R_ID);
    for (case_i, def) in defaults.iter().enumerate() {
        // JSON-escape via serde_json to build the schema safely.
        let reader = format!(
            r#"{{"type":"record","name":"R","fields":[{{"name":"id","type":"long"}},{{"name":"s","type":"string","default":{}}}]}}"#,
            serde_json::to_string(def).unwrap()
        );
        let mut d = decoder_for(&reader);
        d.register_writer_schema(1, R_ID).unwrap();
        d.decode(&confluent_frame(
            1,
            &body(&v1, &[("id", Value::Long(case_i as i64))]),
        ))
        .unwrap();
        let b = d.flush().unwrap().expect("batch");
        assert_eq!(
            get_str(&b, "s", 0),
            *def,
            "case {case_i}: string default must fill"
        );
    }
}

#[test]
fn o03_promotion_int_value_matrix() {
    let vals: [i32; 10] = [0, 1, -1, 42, -42, 1000, -1000, i32::MAX, i32::MIN, 65536];
    let w = parse(R_ID_N_INT);
    for (case_i, v) in vals.iter().enumerate() {
        let mut d = decoder_for(R_ID_N_LONG);
        d.register_writer_schema(1, R_ID_N_INT).unwrap();
        d.decode(&confluent_frame(
            1,
            &body(
                &w,
                &[("id", Value::Long(case_i as i64)), ("n", Value::Int(*v))],
            ),
        ))
        .unwrap();
        let b = d.flush().unwrap().expect("batch");
        assert_eq!(
            get_i64(&b, "n", 0),
            *v as i64,
            "case {case_i}: int {v} -> long"
        );
    }
}

#[test]
fn o04_interleave_id_count_matrix() {
    // For k in 2..=8 distinct ids interleaved over 3k rows, every row must survive with uniform cols.
    let w = parse(R_SEQ);
    for k in 2..=8u32 {
        let mut d = decoder_for(R_SEQ);
        for id in 1..=k {
            d.register_writer_schema(id, R_SEQ).unwrap();
        }
        let total = (3 * k) as i64;
        for i in 0..total {
            let id = (i as u32 % k) + 1;
            d.decode(&confluent_frame(id, &body(&w, &[("seq", Value::Long(i))])))
                .unwrap();
        }
        let b = d.flush().unwrap().expect("batch");
        assert_uniform(&b, total as usize);
        for i in 0..total as usize {
            assert_eq!(
                get_i64(&b, "seq", i),
                i as i64,
                "k={k}: row {i} order preserved"
            );
        }
    }
}
