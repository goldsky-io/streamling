//! Adversarial integration tests for arrow-avro nested-container coercion:
//! `coerce_list` / `coerce_struct` recursion, arrays of nested decimals, top-level u256 alongside
//! `List<Struct<..>>`, empty arrays, NULL elements, nullable-list-is-null, missing nested fields,
//! deeply nested `List<Struct<List<..>>>`, struct field reordering, and multi-row varying lengths.
//!
//! Every assertion is derived from the TARGET-type oracle (`convert_avro_schema_to_arrow`) and the
//! vendored byte-reinterpretation contract, NOT from whatever the new code happens to do — so a
//! regression fails the assertion. End-to-end cases drive the real `ConfluentAvroDecoder` decode
//! path (arrow-avro -> coerce); structural cases drive `coerce_batch_to_target` on hand-built
//! arrays for precise control over shapes arrow-avro won't easily emit.
#![allow(unused_imports, dead_code)]

use apache_avro::Decimal;
use apache_avro::types::{Record, Value};
use apache_avro::{Schema as AvroWriterSchema, to_avro_datum};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeListArray, ListArray, StringArray,
    StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use std::sync::Arc;
use streamling_common::formats::avro::arrow_avro::{ConfluentAvroDecoder, coerce_batch_to_target};
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

/// Decode a sequence of top-level record `Value`s against `schema_json` (writer == reader, so
/// resolution is on) and return the single flushed, coerced batch.
fn decode_rows(schema_json: &str, rows: Vec<Value>) -> RecordBatch {
    let schema = AvroWriterSchema::parse_str(schema_json).unwrap();
    let id = 1u32;
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(id, schema_json).unwrap();
    for r in rows {
        let body = to_avro_datum(&schema, r).unwrap();
        d.decode(&confluent_frame(id, &body)).unwrap();
    }
    d.flush().unwrap().expect("a batch")
}

fn decode_one(schema_json: &str, row: Value) -> RecordBatch {
    decode_rows(schema_json, vec![row])
}

fn col_list(batch: &RecordBatch, name: &str) -> ListArray {
    batch
        .column(batch.schema().index_of(name).unwrap())
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("column is a List")
        .clone()
}

/// The target-oracle Arrow schema for a writer/reader schema JSON.
fn target_of(schema_json: &str) -> SchemaRef {
    convert_avro_schema_to_arrow(AvroWriterSchema::parse_str(schema_json).unwrap())
}

// ---------------------------------------------------------------------------
// Schemas used across cases
// ---------------------------------------------------------------------------

const INT_ARRAY: &str = r#"{"type":"record","name":"R","fields":[{"name":"arr","type":{"type":"array","items":"int"}}]}"#;
const LONG_ARRAY: &str = r#"{"type":"record","name":"R","fields":[{"name":"arr","type":{"type":"array","items":"long"}}]}"#;
const STRING_ARRAY: &str = r#"{"type":"record","name":"R","fields":[{"name":"arr","type":{"type":"array","items":"string"}}]}"#;
const BOOL_ARRAY: &str = r#"{"type":"record","name":"R","fields":[{"name":"arr","type":{"type":"array","items":"boolean"}}]}"#;
const DOUBLE_ARRAY: &str = r#"{"type":"record","name":"R","fields":[{"name":"arr","type":{"type":"array","items":"double"}}]}"#;
const FLOAT_ARRAY: &str = r#"{"type":"record","name":"R","fields":[{"name":"arr","type":{"type":"array","items":"float"}}]}"#;
const BYTES_ARRAY: &str = r#"{"type":"record","name":"R","fields":[{"name":"arr","type":{"type":"array","items":"bytes"}}]}"#;
const NULLABLE_ELEM_ARRAY: &str = r#"{"type":"record","name":"R","fields":[{"name":"arr","type":{"type":"array","items":["null","int"]}}]}"#;
const NULLABLE_LIST_FIELD: &str = r#"{"type":"record","name":"R","fields":[{"name":"arr","type":["null",{"type":"array","items":"int"}],"default":null}]}"#;

// traces shape: array<record{who:["null",string], amt:["null",decimal(100,0)]}>
const TRACES: &str = r#"{"type":"record","name":"R","fields":[
  {"name":"xfers","type":{"type":"array","items":{"type":"record","name":"X","fields":[
    {"name":"who","type":["null","string"],"default":null},
    {"name":"amt","type":["null",{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}],"default":null}
  ]}}}
]}"#;

// top-level u256 alongside a list<struct> carrying a nested (Decimal128) decimal.
const U256_TRACES: &str = r#"{"type":"record","name":"R","fields":[
  {"name":"top","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}},
  {"name":"xfers","type":{"type":"array","items":{"type":"record","name":"X","fields":[
    {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":100,"scale":0}}
  ]}}}
]}"#;

// deeply nested: List<Struct<List<int>>>
const DEEP: &str = r#"{"type":"record","name":"R","fields":[
  {"name":"outer","type":{"type":"array","items":{"type":"record","name":"S","fields":[
    {"name":"inner","type":{"type":"array","items":"int"}}
  ]}}}
]}"#;

// nested record-in-record with a nullable field.
const STRUCT_IN_STRUCT: &str = r#"{"type":"record","name":"R","fields":[
  {"name":"s","type":{"type":"record","name":"S","fields":[
    {"name":"a","type":"long"},
    {"name":"b","type":["null","string"],"default":null}
  ]}}
]}"#;

// nested small-scale decimal (Decimal128 with scale, nested inside a struct in a list).
const TRACES_SCALED: &str = r#"{"type":"record","name":"R","fields":[
  {"name":"xfers","type":{"type":"array","items":{"type":"record","name":"X","fields":[
    {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":20,"scale":5}}
  ]}}}
]}"#;

// ---------------------------------------------------------------------------
// Row builders
// ---------------------------------------------------------------------------

fn int_arr_row(v: &[i32]) -> Value {
    Value::Record(vec![(
        "arr".into(),
        Value::Array(v.iter().map(|&x| Value::Int(x)).collect()),
    )])
}

fn nullable_elem_row(v: &[Option<i32>]) -> Value {
    Value::Record(vec![(
        "arr".into(),
        Value::Array(
            v.iter()
                .map(|o| match o {
                    Some(x) => Value::Union(1, Box::new(Value::Int(*x))),
                    None => Value::Union(0, Box::new(Value::Null)),
                })
                .collect(),
        ),
    )])
}

fn xfer(who: Option<&str>, amt: Option<&[u8]>) -> Value {
    Value::Record(vec![
        (
            "who".into(),
            match who {
                Some(s) => Value::Union(1, Box::new(Value::String(s.to_string()))),
                None => Value::Union(0, Box::new(Value::Null)),
            },
        ),
        (
            "amt".into(),
            match amt {
                Some(b) => Value::Union(1, Box::new(Value::Decimal(Decimal::from(b.to_vec())))),
                None => Value::Union(0, Box::new(Value::Null)),
            },
        ),
    ])
}

fn xfers_row(items: Vec<Value>) -> Value {
    Value::Record(vec![("xfers".into(), Value::Array(items))])
}

// ===========================================================================
// SECTION A: arrays of primitives (element type + values round-trip)
// ===========================================================================

#[test]
fn a01_array_of_ints_single_row() {
    let b = decode_one(INT_ARRAY, int_arr_row(&[1, 2, 3]));
    let list = col_list(&b, "arr");
    assert_eq!(list.len(), 1);
    assert_eq!(list.value_offsets(), &[0, 3]);
    let row = list.value(0);
    let ints = row.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(
        (0..3).map(|i| ints.value(i)).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[test]
fn a02_array_of_ints_target_type_is_list_int32() {
    let t = target_of(INT_ARRAY);
    match t.field(0).data_type() {
        DataType::List(elem) => assert_eq!(elem.data_type(), &DataType::Int32),
        other => panic!("expected List<Int32>, got {other:?}"),
    }
}

#[test]
fn a03_array_of_longs() {
    let row = Value::Record(vec![(
        "arr".into(),
        Value::Array(vec![
            Value::Long(i64::MIN),
            Value::Long(0),
            Value::Long(i64::MAX),
        ]),
    )]);
    let b = decode_one(LONG_ARRAY, row);
    let row0 = col_list(&b, "arr").value(0);
    let a = row0.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(
        (0..3).map(|i| a.value(i)).collect::<Vec<_>>(),
        vec![i64::MIN, 0, i64::MAX]
    );
}

#[test]
fn a04_array_of_strings() {
    let row = Value::Record(vec![(
        "arr".into(),
        Value::Array(vec![
            Value::String("alpha".into()),
            Value::String("".into()),
            Value::String("gamma".into()),
        ]),
    )]);
    let b = decode_one(STRING_ARRAY, row);
    let row0 = col_list(&b, "arr").value(0);
    let a = row0.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(a.value(0), "alpha");
    assert_eq!(a.value(1), "");
    assert_eq!(a.value(2), "gamma");
}

#[test]
fn a05_array_of_booleans() {
    let row = Value::Record(vec![(
        "arr".into(),
        Value::Array(vec![
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(true),
        ]),
    )]);
    let b = decode_one(BOOL_ARRAY, row);
    let row0 = col_list(&b, "arr").value(0);
    let a = row0.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(a.value(0) && !a.value(1) && a.value(2));
}

#[test]
fn a06_array_of_doubles() {
    let row = Value::Record(vec![(
        "arr".into(),
        Value::Array(vec![
            Value::Double(1.5),
            Value::Double(-2.25),
            Value::Double(0.0),
        ]),
    )]);
    let b = decode_one(DOUBLE_ARRAY, row);
    let row0 = col_list(&b, "arr").value(0);
    let a = row0.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(a.value(0), 1.5);
    assert_eq!(a.value(1), -2.25);
    assert_eq!(a.value(2), 0.0);
}

#[test]
fn a07_array_of_floats() {
    let row = Value::Record(vec![(
        "arr".into(),
        Value::Array(vec![Value::Float(3.5), Value::Float(f32::MIN)]),
    )]);
    let b = decode_one(FLOAT_ARRAY, row);
    let row0 = col_list(&b, "arr").value(0);
    let a = row0.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!(a.value(0), 3.5);
    assert_eq!(a.value(1), f32::MIN);
}

#[test]
fn a08_array_of_bytes() {
    let row = Value::Record(vec![(
        "arr".into(),
        Value::Array(vec![
            Value::Bytes(vec![0xDE, 0xAD]),
            Value::Bytes(vec![]),
            Value::Bytes(vec![0xBE, 0xEF, 0x00]),
        ]),
    )]);
    let b = decode_one(BYTES_ARRAY, row);
    let row0 = col_list(&b, "arr").value(0);
    let a = row0.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(a.value(0), &[0xDE, 0xAD]);
    assert_eq!(a.value(1), &[] as &[u8]);
    assert_eq!(a.value(2), &[0xBE, 0xEF, 0x00]);
}

// ===========================================================================
// SECTION B: empty arrays, offsets, multi-row lengths
// ===========================================================================

#[test]
fn b01_empty_array_single_row() {
    let b = decode_one(INT_ARRAY, int_arr_row(&[]));
    let list = col_list(&b, "arr");
    assert_eq!(list.len(), 1);
    assert_eq!(list.value_offsets(), &[0, 0]);
    assert_eq!(list.value_length(0), 0);
    assert!(!list.is_null(0), "empty list is present, not null");
}

#[test]
fn b02_all_empty_rows() {
    let b = decode_rows(
        INT_ARRAY,
        vec![int_arr_row(&[]), int_arr_row(&[]), int_arr_row(&[])],
    );
    let list = col_list(&b, "arr");
    assert_eq!(list.len(), 3);
    assert_eq!(list.value_offsets(), &[0, 0, 0, 0]);
    assert_eq!(list.values().len(), 0, "no elements across all-empty rows");
}

#[test]
fn b03_multi_row_varying_lengths_offsets() {
    let b = decode_rows(
        INT_ARRAY,
        vec![
            int_arr_row(&[1, 2, 3]),
            int_arr_row(&[]),
            int_arr_row(&[4, 5]),
        ],
    );
    let list = col_list(&b, "arr");
    assert_eq!(list.len(), 3);
    assert_eq!(list.value_offsets(), &[0, 3, 3, 5]);
    assert_eq!(list.value_length(0), 3);
    assert_eq!(list.value_length(1), 0);
    assert_eq!(list.value_length(2), 2);
    let vals = list.values();
    let ints = vals.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(
        (0..5).map(|i| ints.value(i)).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
}

#[test]
fn b04_single_element_rows() {
    let b = decode_rows(
        INT_ARRAY,
        vec![int_arr_row(&[7]), int_arr_row(&[8]), int_arr_row(&[9])],
    );
    let list = col_list(&b, "arr");
    assert_eq!(list.value_offsets(), &[0, 1, 2, 3]);
    for (r, &want) in [7, 8, 9].iter().enumerate() {
        let row = list.value(r);
        assert_eq!(
            row.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            want
        );
    }
}

// Table-driven: many length patterns; each entry is one adversarial case.
#[test]
fn b05_varying_length_patterns_table() {
    let patterns: Vec<Vec<Vec<i32>>> = vec![
        vec![vec![1, 2, 3]],
        vec![vec![]],
        vec![vec![1], vec![2, 3]],
        vec![vec![], vec![], vec![]],
        vec![vec![1, 2, 3], vec![], vec![4, 5]],
        vec![vec![], vec![9]],
        vec![vec![0], vec![], vec![], vec![1, 2]],
        vec![vec![10, 11, 12, 13, 14]],
        vec![vec![-1, -2], vec![-3]],
        vec![vec![i32::MIN, i32::MAX]],
        vec![vec![], vec![1], vec![2, 3], vec![4, 5, 6]],
        vec![vec![100]; 10],
        vec![vec![1, 2], vec![], vec![3], vec![], vec![4, 5, 6, 7]],
    ];
    for (pi, pat) in patterns.iter().enumerate() {
        let rows: Vec<Value> = pat.iter().map(|v| int_arr_row(v)).collect();
        let b = decode_rows(INT_ARRAY, rows);
        let list = col_list(&b, "arr");
        assert_eq!(list.len(), pat.len(), "pattern {pi}: row count");
        let mut exp_off = vec![0i32];
        for v in pat {
            exp_off.push(exp_off.last().unwrap() + v.len() as i32);
        }
        assert_eq!(
            list.value_offsets(),
            exp_off.as_slice(),
            "pattern {pi}: offsets"
        );
        for (ri, v) in pat.iter().enumerate() {
            let row = list.value(ri);
            let ints = row.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(ints.len(), v.len(), "pattern {pi} row {ri}: len");
            for (i, &x) in v.iter().enumerate() {
                assert_eq!(ints.value(i), x, "pattern {pi} row {ri} idx {i}");
            }
        }
    }
}

// ===========================================================================
// SECTION C: NULL elements inside arrays (items ["null", int])
// ===========================================================================

#[test]
fn c01_null_element_middle() {
    let b = decode_one(
        NULLABLE_ELEM_ARRAY,
        nullable_elem_row(&[Some(1), None, Some(3)]),
    );
    let row = col_list(&b, "arr").value(0);
    let a = row.as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(!a.is_null(0) && a.value(0) == 1);
    assert!(a.is_null(1), "middle element is null");
    assert!(!a.is_null(2) && a.value(2) == 3);
}

#[test]
fn c02_all_null_elements() {
    let b = decode_one(NULLABLE_ELEM_ARRAY, nullable_elem_row(&[None, None, None]));
    let row = col_list(&b, "arr").value(0);
    let a = row.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(a.len(), 3);
    assert_eq!(a.null_count(), 3);
}

#[test]
fn c03_target_nullable_element_field() {
    // items ["null", int] -> List child field is nullable Int32.
    let t = target_of(NULLABLE_ELEM_ARRAY);
    match t.field(0).data_type() {
        DataType::List(elem) => {
            assert_eq!(elem.data_type(), &DataType::Int32);
            assert!(elem.is_nullable(), "element field must be nullable");
        }
        other => panic!("expected List, got {other:?}"),
    }
}

// Table-driven null-position patterns.
#[test]
fn c04_null_position_patterns_table() {
    let patterns: Vec<Vec<Option<i32>>> = vec![
        vec![None],
        vec![Some(0)],
        vec![None, Some(1)],
        vec![Some(1), None],
        vec![None, None, Some(5)],
        vec![Some(5), None, None],
        vec![Some(1), None, Some(3), None, Some(5)],
        vec![None, Some(2), None, Some(4), None],
        vec![],
        vec![Some(-1), None, Some(i32::MIN)],
    ];
    for (pi, pat) in patterns.iter().enumerate() {
        let b = decode_one(NULLABLE_ELEM_ARRAY, nullable_elem_row(pat));
        let row = col_list(&b, "arr").value(0);
        let a = row.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.len(), pat.len(), "pattern {pi}: len");
        for (i, o) in pat.iter().enumerate() {
            match o {
                Some(x) => {
                    assert!(!a.is_null(i), "pattern {pi} idx {i} should be present");
                    assert_eq!(a.value(i), *x, "pattern {pi} idx {i} value");
                }
                None => assert!(a.is_null(i), "pattern {pi} idx {i} should be null"),
            }
        }
    }
}

// ===========================================================================
// SECTION D: nullable LIST field that is itself null
// ===========================================================================

#[test]
fn d01_nullable_list_field_is_null() {
    let row = Value::Record(vec![("arr".into(), Value::Union(0, Box::new(Value::Null)))]);
    let b = decode_one(NULLABLE_LIST_FIELD, row);
    let list = col_list(&b, "arr");
    assert_eq!(list.len(), 1);
    assert!(list.is_null(0), "null list slot");
}

#[test]
fn d02_nullable_list_field_present() {
    let row = Value::Record(vec![(
        "arr".into(),
        Value::Union(
            1,
            Box::new(Value::Array(vec![Value::Int(5), Value::Int(6)])),
        ),
    )]);
    let b = decode_one(NULLABLE_LIST_FIELD, row);
    let list = col_list(&b, "arr");
    assert!(!list.is_null(0));
    let row0 = list.value(0);
    let a = row0.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!((0..2).map(|i| a.value(i)).collect::<Vec<_>>(), vec![5, 6]);
}

#[test]
fn d03_nullable_list_field_mixed_rows() {
    let present = |v: &[i32]| {
        Value::Record(vec![(
            "arr".into(),
            Value::Union(
                1,
                Box::new(Value::Array(v.iter().map(|&x| Value::Int(x)).collect())),
            ),
        )])
    };
    let null_row = Value::Record(vec![("arr".into(), Value::Union(0, Box::new(Value::Null)))]);
    let b = decode_rows(
        NULLABLE_LIST_FIELD,
        vec![present(&[10, 20]), null_row, present(&[])],
    );
    let list = col_list(&b, "arr");
    assert_eq!(list.len(), 3);
    assert!(!list.is_null(0));
    assert!(list.is_null(1), "row 1 list is null");
    assert!(!list.is_null(2), "empty list is present, not null");
    assert_eq!(list.value_length(2), 0);
    let r0 = list.value(0);
    let a = r0.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!((0..2).map(|i| a.value(i)).collect::<Vec<_>>(), vec![10, 20]);
}

#[test]
fn d04_nullable_list_field_target_is_nullable() {
    let t = target_of(NULLABLE_LIST_FIELD);
    assert!(
        t.field(0).is_nullable(),
        "union[null,array] -> nullable List"
    );
    assert!(matches!(t.field(0).data_type(), DataType::List(_)));
}

// ===========================================================================
// SECTION E: List<Struct{..}> traces shape (nested Decimal128 + nullable string)
// ===========================================================================

fn traces_list_struct(b: &RecordBatch) -> (ListArray, StructArray) {
    let list = col_list(b, "xfers");
    let values = list.values().clone();
    let st = values
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone();
    (list, st)
}

#[test]
fn e01_traces_target_type() {
    let t = target_of(TRACES);
    let DataType::List(elem) = t.field(0).data_type() else {
        panic!("xfers not a List");
    };
    let DataType::Struct(fields) = elem.data_type() else {
        panic!("element not a Struct");
    };
    let amt = fields.iter().find(|f| f.name() == "amt").unwrap();
    // nested high-precision decimal maps to Decimal128(p,s), NOT u256 (top-level only).
    assert_eq!(amt.data_type(), &DataType::Decimal128(100, 0));
    let who = fields.iter().find(|f| f.name() == "who").unwrap();
    assert_eq!(who.data_type(), &DataType::Utf8);
}

#[test]
fn e02_traces_single_transfer_values() {
    let b = decode_one(
        TRACES,
        xfers_row(vec![xfer(Some("alice"), Some(&[0x04, 0xD2]))]),
    );
    let (list, st) = traces_list_struct(&b);
    assert_eq!(list.value_offsets(), &[0, 1]);
    let who = st
        .column_by_name("who")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(who.value(0), "alice");
    assert_eq!(amt.value(0), 1234_i128);
}

#[test]
fn e03_traces_empty_array() {
    let b = decode_one(TRACES, xfers_row(vec![]));
    let list = col_list(&b, "xfers");
    assert_eq!(list.value_offsets(), &[0, 0]);
    assert_eq!(list.value_length(0), 0);
}

#[test]
fn e04_traces_null_nested_fields() {
    let b = decode_one(TRACES, xfers_row(vec![xfer(None, None)]));
    let (_list, st) = traces_list_struct(&b);
    let who = st.column_by_name("who").unwrap();
    let amt = st.column_by_name("amt").unwrap();
    assert!(who.is_null(0), "null who");
    assert!(amt.is_null(0), "null amt");
}

#[test]
fn e05_traces_multi_transfer_offsets_and_values() {
    let b = decode_one(
        TRACES,
        xfers_row(vec![
            xfer(Some("a"), Some(&[0x01])),
            xfer(Some("b"), Some(&[0x02])),
            xfer(None, Some(&[0x7F])),
        ]),
    );
    let (list, st) = traces_list_struct(&b);
    assert_eq!(list.value_offsets(), &[0, 3]);
    let who = st
        .column_by_name("who")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(who.value(0), "a");
    assert_eq!(who.value(1), "b");
    assert!(who.is_null(2));
    assert_eq!(amt.value(0), 1);
    assert_eq!(amt.value(1), 2);
    assert_eq!(amt.value(2), 127);
}

#[test]
fn e06_traces_multi_row_varying_lengths() {
    let b = decode_rows(
        TRACES,
        vec![
            xfers_row(vec![xfer(Some("a"), Some(&[0x01]))]),
            xfers_row(vec![]),
            xfers_row(vec![
                xfer(Some("b"), Some(&[0x02])),
                xfer(Some("c"), Some(&[0x03])),
            ]),
        ],
    );
    let list = col_list(&b, "xfers");
    assert_eq!(list.len(), 3);
    assert_eq!(list.value_offsets(), &[0, 1, 1, 3]);
    let st = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        (0..3).map(|i| amt.value(i)).collect::<Vec<_>>(),
        vec![1_i128, 2, 3]
    );
    assert_eq!(st.len(), 3, "struct values length equals total elements");
}

#[test]
fn e07_traces_negative_nested_decimal() {
    // two's-complement 0xFF -> -1 through the nested Decimal128 path.
    let b = decode_one(TRACES, xfers_row(vec![xfer(Some("neg"), Some(&[0xFF]))]));
    let (_l, st) = traces_list_struct(&b);
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amt.value(0), -1_i128);
}

#[test]
fn e08_traces_scaled_nested_decimal_type() {
    // Decimal(20,5) nested -> Decimal128(20,5); unscaled integer preserved.
    let t = target_of(TRACES_SCALED);
    let DataType::List(elem) = t.field(0).data_type() else {
        panic!("not a list");
    };
    let DataType::Struct(fields) = elem.data_type() else {
        panic!("not a struct");
    };
    let amt = fields.iter().find(|f| f.name() == "amt").unwrap();
    assert_eq!(amt.data_type(), &DataType::Decimal128(20, 5));

    let row = Value::Record(vec![(
        "xfers".into(),
        Value::Array(vec![Value::Record(vec![(
            "amt".into(),
            Value::Decimal(Decimal::from(vec![0x30, 0x39])), // 12345 unscaled
        )])]),
    )]);
    let b = decode_one(TRACES_SCALED, row);
    let st = col_list(&b, "xfers")
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone();
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amt.value(0), 12345_i128, "unscaled integer preserved");
}

// ===========================================================================
// SECTION F: deeply nested List<Struct<List<int>>>
// ===========================================================================

#[test]
fn f01_deep_target_type() {
    let t = target_of(DEEP);
    let DataType::List(elem) = t.field(0).data_type() else {
        panic!("outer not list");
    };
    let DataType::Struct(fields) = elem.data_type() else {
        panic!("elem not struct");
    };
    let inner = fields.iter().find(|f| f.name() == "inner").unwrap();
    let DataType::List(inner_elem) = inner.data_type() else {
        panic!("inner not list");
    };
    assert_eq!(inner_elem.data_type(), &DataType::Int32);
}

#[test]
fn f02_deep_values() {
    let row = Value::Record(vec![(
        "outer".into(),
        Value::Array(vec![
            Value::Record(vec![(
                "inner".into(),
                Value::Array(vec![Value::Int(1), Value::Int(2)]),
            )]),
            Value::Record(vec![("inner".into(), Value::Array(vec![Value::Int(3)]))]),
        ]),
    )]);
    let b = decode_one(DEEP, row);
    let outer = col_list(&b, "outer");
    assert_eq!(outer.value_offsets(), &[0, 2]);
    let structs = outer.value(0);
    let st = structs.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(st.len(), 2);
    let inner = st
        .column_by_name("inner")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(inner.value_offsets(), &[0, 2, 3]);
    let r0 = inner.value(0);
    let a0 = r0.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!((0..2).map(|i| a0.value(i)).collect::<Vec<_>>(), vec![1, 2]);
    let r1 = inner.value(1);
    let a1 = r1.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(a1.value(0), 3);
}

#[test]
fn f03_deep_empty_inner_lists() {
    let row = Value::Record(vec![(
        "outer".into(),
        Value::Array(vec![
            Value::Record(vec![("inner".into(), Value::Array(vec![]))]),
            Value::Record(vec![("inner".into(), Value::Array(vec![Value::Int(9)]))]),
        ]),
    )]);
    let b = decode_one(DEEP, row);
    let outer = col_list(&b, "outer");
    let st = outer.value(0);
    let st = st.as_any().downcast_ref::<StructArray>().unwrap();
    let inner = st
        .column_by_name("inner")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(inner.value_length(0), 0, "first inner empty");
    assert_eq!(inner.value_length(1), 1);
}

#[test]
fn f04_deep_empty_outer() {
    let row = Value::Record(vec![("outer".into(), Value::Array(vec![]))]);
    let b = decode_one(DEEP, row);
    let outer = col_list(&b, "outer");
    assert_eq!(outer.value_length(0), 0);
    assert_eq!(outer.values().len(), 0);
}

// ===========================================================================
// SECTION G: nested struct-in-struct
// ===========================================================================

#[test]
fn g01_struct_in_struct_values() {
    let row = Value::Record(vec![(
        "s".into(),
        Value::Record(vec![
            ("a".into(), Value::Long(77)),
            (
                "b".into(),
                Value::Union(1, Box::new(Value::String("hi".into()))),
            ),
        ]),
    )]);
    let b = decode_one(STRUCT_IN_STRUCT, row);
    let s = b.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    let a = s
        .column_by_name("a")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let bb = s
        .column_by_name("b")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(a.value(0), 77);
    assert_eq!(bb.value(0), "hi");
}

#[test]
fn g02_struct_in_struct_nested_null() {
    let row = Value::Record(vec![(
        "s".into(),
        Value::Record(vec![
            ("a".into(), Value::Long(5)),
            ("b".into(), Value::Union(0, Box::new(Value::Null))),
        ]),
    )]);
    let b = decode_one(STRUCT_IN_STRUCT, row);
    let s = b.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    assert!(s.column_by_name("b").unwrap().is_null(0), "nested b null");
    let a = s
        .column_by_name("a")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(a.value(0), 5);
}

#[test]
fn g03_struct_in_struct_target_type() {
    let t = target_of(STRUCT_IN_STRUCT);
    let DataType::Struct(fields) = t.field(0).data_type() else {
        panic!("not a struct");
    };
    assert_eq!(
        fields.iter().find(|f| f.name() == "a").unwrap().data_type(),
        &DataType::Int64
    );
    let bf = fields.iter().find(|f| f.name() == "b").unwrap();
    assert_eq!(bf.data_type(), &DataType::Utf8);
    assert!(bf.is_nullable());
}

// ===========================================================================
// SECTION H: top-level u256 alongside List<Struct<Decimal128>>
// ===========================================================================

#[test]
fn h01_top_u256_plus_nested_decimal128() {
    let mut top = [0u8; 32];
    top[0] = 0x12;
    top[31] = 0x34;
    let row = Value::Record(vec![
        ("top".into(), Value::Decimal(Decimal::from(top.to_vec()))),
        (
            "xfers".into(),
            Value::Array(vec![Value::Record(vec![(
                "amt".into(),
                Value::Decimal(Decimal::from(vec![0x04, 0xD2])),
            )])]),
        ),
    ]);
    let b = decode_one(U256_TRACES, row);

    // top-level field is FixedSizeBinary(32) carrying the exact 32-byte payload.
    let top_col = b
        .column(b.schema().index_of("top").unwrap())
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("top is FixedSizeBinary(32)");
    assert_eq!(top_col.value_length(), 32);
    assert_eq!(top_col.value(0), &top);

    // nested amt is Decimal128(100,0) = 1234.
    let st = col_list(&b, "xfers")
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone();
    let amt = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amt.value(0), 1234_i128);
}

#[test]
fn h02_top_u256_target_is_fixedsizebinary32() {
    let t = target_of(U256_TRACES);
    assert_eq!(
        t.field(t.index_of("top").unwrap()).data_type(),
        &DataType::FixedSizeBinary(32)
    );
    // nested amt still Decimal128(100,0), not u256.
    let DataType::List(elem) = t.field(t.index_of("xfers").unwrap()).data_type() else {
        panic!("xfers not list");
    };
    let DataType::Struct(fields) = elem.data_type() else {
        panic!("elem not struct");
    };
    assert_eq!(
        fields
            .iter()
            .find(|f| f.name() == "amt")
            .unwrap()
            .data_type(),
        &DataType::Decimal128(100, 0)
    );
}

// ===========================================================================
// SECTION I: direct coerce_batch_to_target on hand-built arrays
//            (shapes/edge cases arrow-avro won't easily emit)
// ===========================================================================

/// Build a one-column batch whose single column is `arr` with `dt`.
fn one_col_batch(name: &str, dt: DataType, col: ArrayRef, nullable: bool) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(name, dt, nullable)])),
        vec![col],
    )
    .unwrap()
}

#[test]
fn i01_coerce_struct_reorders_children_by_name() {
    // Source struct order [b, a]; target order [a, b]. Must match by name, not position.
    let bcol: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
    let acol: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
    let src_struct = StructArray::new(
        Fields::from(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int64, false),
        ]),
        vec![bcol, acol],
        None,
    );
    let src = one_col_batch(
        "s",
        src_struct.data_type().clone(),
        Arc::new(src_struct),
        false,
    );

    let target = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ])),
        false,
    )]));
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let s = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let a = s.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let bb = s.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        a.values(),
        &[1, 2],
        "field 'a' resolved by name to position 0"
    );
    assert_eq!(bb.value(0), "x");
    assert_eq!(bb.value(1), "y");
}

#[test]
fn i02_coerce_struct_missing_nullable_filled_null() {
    let acol: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let src_struct = StructArray::new(
        Fields::from(vec![Field::new("a", DataType::Int64, false)]),
        vec![acol],
        None,
    );
    let src = one_col_batch(
        "s",
        src_struct.data_type().clone(),
        Arc::new(src_struct),
        false,
    );
    let target = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ])),
        false,
    )]));
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let s = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(s.column_by_name("b").unwrap().null_count(), 3);
    assert_eq!(
        s.column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2, 3]
    );
}

#[test]
fn i03_coerce_struct_missing_required_errors() {
    let acol: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
    let src_struct = StructArray::new(
        Fields::from(vec![Field::new("a", DataType::Int64, false)]),
        vec![acol],
        None,
    );
    let src = one_col_batch(
        "s",
        src_struct.data_type().clone(),
        Arc::new(src_struct),
        false,
    );
    let target = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false), // required, absent
        ])),
        false,
    )]));
    assert!(
        coerce_batch_to_target(&src, &target).is_err(),
        "missing required nested field must error"
    );
}

#[test]
fn i04_coerce_struct_extra_source_field_ignored() {
    // Source has extra field 'z' not present in target — must be dropped, not error.
    let acol: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
    let zcol: ArrayRef = Arc::new(StringArray::from(vec!["junk"]));
    let src_struct = StructArray::new(
        Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("z", DataType::Utf8, true),
        ]),
        vec![acol, zcol],
        None,
    );
    let src = one_col_batch(
        "s",
        src_struct.data_type().clone(),
        Arc::new(src_struct),
        false,
    );
    let target = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, false)])),
        false,
    )]));
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let s = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(s.columns().len(), 1, "extra source field dropped");
    assert!(s.column_by_name("z").is_none());
}

#[test]
fn i05_coerce_struct_preserves_struct_level_null() {
    let acol: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
    let nulls = NullBuffer::from(vec![true, false]); // row 1 is a null struct
    let src_struct = StructArray::new(
        Fields::from(vec![Field::new("a", DataType::Int64, false)]),
        vec![acol],
        Some(nulls),
    );
    let src = one_col_batch(
        "s",
        src_struct.data_type().clone(),
        Arc::new(src_struct),
        true,
    );
    let target = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ])),
        true,
    )]));
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let s = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(!s.is_null(0));
    assert!(s.is_null(1), "struct-level null preserved through coercion");
}

#[test]
fn i06_coerce_list_binary_child_to_decimal128_bytes_table() {
    // Nested Binary -> Decimal128 via be_bytes_to_i128 (two's-complement, low-16-bytes).
    let cases: Vec<(&[u8], i128)> = vec![
        (b"\x00", 0),
        (b"\x01", 1),
        (b"\x7f", 127),
        (b"\x80", -128),
        (b"\xff", -1),
        (b"\x04\xd2", 1234),
        (b"\x01\x00", 256),
        (b"\xff\x00", -256),
        (b"\x00\x80", 128),
        (b"\x7f\xff\xff\xff", 2147483647),
        (b"\x80\x00\x00\x00", -2147483648),
        (b"\xff\xff\xff\xff", -1),
        (b"\x00\x00\x00\x00\x00\x00\x00\x01", 1),
        (
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01",
            1,
        ),
    ];
    for (ci, (bytes, expect)) in cases.iter().enumerate() {
        let bin: ArrayRef = Arc::new(BinaryArray::from(vec![Some(*bytes)]));
        let src = one_col_batch("v", DataType::Binary, bin, true);
        let target = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(38, 0),
            true,
        )]));
        let out = coerce_batch_to_target(&src, &target).unwrap();
        let d = out
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(d.value(0), *expect, "case {ci}: bytes {bytes:?}");
    }
}

#[test]
fn i07_coerce_list_binary_child_null_preserved() {
    let bin: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&b"\x01"[..]),
        None,
        Some(&b"\x02"[..]),
    ]));
    let src = one_col_batch("v", DataType::Binary, bin, true);
    let target = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::Decimal128(10, 0),
        true,
    )]));
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let d = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(d.value(0), 1);
    assert!(d.is_null(1), "null binary -> null decimal");
    assert_eq!(d.value(2), 2);
}

#[test]
fn i08_coerce_list_in_struct_binary_to_decimal128() {
    // Struct{amt: Binary} rows inside a List -> Struct{amt: Decimal128}. Offsets preserved.
    let amt: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(&b"\x01"[..]),
        Some(&b"\x02"[..]),
        Some(&b"\x03"[..]),
    ]));
    let src_struct = StructArray::new(
        Fields::from(vec![Field::new("amt", DataType::Binary, true)]),
        vec![amt],
        None,
    );
    let offsets = OffsetBuffer::new(vec![0i32, 2, 3].into());
    let src_list = ListArray::new(
        Arc::new(Field::new("element", src_struct.data_type().clone(), true)),
        offsets,
        Arc::new(src_struct),
        None,
    );
    let src = one_col_batch(
        "xfers",
        src_list.data_type().clone(),
        Arc::new(src_list),
        false,
    );

    let target = Arc::new(Schema::new(vec![Field::new(
        "xfers",
        DataType::List(Arc::new(Field::new(
            "element",
            DataType::Struct(Fields::from(vec![Field::new(
                "amt",
                DataType::Decimal128(100, 0),
                true,
            )])),
            true,
        ))),
        false,
    )]));
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let list = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.value_offsets(), &[0, 2, 3], "list offsets preserved");
    let st = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let d = st
        .column_by_name("amt")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        (0..3).map(|i| d.value(i)).collect::<Vec<_>>(),
        vec![1_i128, 2, 3]
    );
}

#[test]
fn i09_coerce_largelist_narrows_to_list() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let offsets = OffsetBuffer::new(vec![0i64, 2, 4].into());
    let ll = LargeListArray::new(
        Arc::new(Field::new("element", DataType::Int32, false)),
        offsets,
        values,
        None,
    );
    let src = one_col_batch("arr", ll.data_type().clone(), Arc::new(ll), false);
    let target = Arc::new(Schema::new(vec![Field::new(
        "arr",
        DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
        false,
    )]));
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let list = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(
        list.value_offsets(),
        &[0, 2, 4],
        "i64 offsets narrowed to i32"
    );
    let r1 = list.value(1);
    let a = r1.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!((0..2).map(|i| a.value(i)).collect::<Vec<_>>(), vec![3, 4]);
}

#[test]
fn i10_coerce_list_wrong_source_type_errors() {
    // Target wants List but source is a plain Int32 column -> coerce_list must error, not panic.
    let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let src = one_col_batch("arr", DataType::Int32, col, false);
    let target = Arc::new(Schema::new(vec![Field::new(
        "arr",
        DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
        false,
    )]));
    assert!(coerce_batch_to_target(&src, &target).is_err());
}

#[test]
fn i11_coerce_struct_wrong_source_type_errors() {
    let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let src = one_col_batch("s", DataType::Int32, col, false);
    let target = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, false)])),
        false,
    )]));
    assert!(coerce_batch_to_target(&src, &target).is_err());
}

#[test]
fn i12_coerce_empty_list_preserved() {
    let values: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
    let offsets = OffsetBuffer::new(vec![0i32, 0, 0].into());
    let list = ListArray::new(
        Arc::new(Field::new("element", DataType::Int32, false)),
        offsets,
        values,
        None,
    );
    let src = one_col_batch("arr", list.data_type().clone(), Arc::new(list), false);
    let target = Arc::new(Schema::new(vec![Field::new(
        "arr",
        DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
        false,
    )]));
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let l = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(l.value_offsets(), &[0, 0, 0]);
    assert_eq!(l.values().len(), 0);
}

#[test]
fn i13_coerce_list_with_null_list_entries_preserved() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let offsets = OffsetBuffer::new(vec![0i32, 2, 2, 3].into());
    let nulls = NullBuffer::from(vec![true, false, true]); // row 1 is a null list
    let list = ListArray::new(
        Arc::new(Field::new("element", DataType::Int32, false)),
        offsets,
        values,
        Some(nulls),
    );
    let src = one_col_batch("arr", list.data_type().clone(), Arc::new(list), true);
    let target = Arc::new(Schema::new(vec![Field::new(
        "arr",
        DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
        true,
    )]));
    let out = coerce_batch_to_target(&src, &target).unwrap();
    let l = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();
    assert!(!l.is_null(0));
    assert!(l.is_null(1), "null list entry preserved");
    assert!(!l.is_null(2));
    assert_eq!(l.value_offsets(), &[0, 2, 2, 3]);
}

// ===========================================================================
// SECTION J: malformed framing / consistency guards
// ===========================================================================

#[test]
fn j01_frame_too_short_errors() {
    let schema = AvroWriterSchema::parse_str(INT_ARRAY).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(1, INT_ARRAY).unwrap();
    assert!(
        d.decode(&[0x00, 0x00, 0x00]).is_err(),
        "frame < 5 bytes must error"
    );
}

#[test]
fn j02_bad_magic_byte_errors() {
    let schema = AvroWriterSchema::parse_str(INT_ARRAY).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(1, INT_ARRAY).unwrap();
    let mut framed = vec![0x01]; // wrong magic
    framed.extend_from_slice(&1u32.to_be_bytes());
    framed.push(0x00);
    assert!(d.decode(&framed).is_err(), "non-zero magic must error");
}

#[test]
fn j03_flush_without_data_is_none() {
    let schema = AvroWriterSchema::parse_str(INT_ARRAY).unwrap();
    let mut d = ConfluentAvroDecoder::new()
        .with_reader_schema(&schema)
        .unwrap();
    d.register_writer_schema(1, INT_ARRAY).unwrap();
    assert!(d.flush().unwrap().is_none(), "no rows decoded -> no batch");
}

#[test]
fn j04_column_lengths_consistent_across_container_columns() {
    // A record mixing a scalar, a list, and a list<struct> — all columns must have equal length.
    const MIXED: &str = r#"{"type":"record","name":"R","fields":[
      {"name":"id","type":"long"},
      {"name":"nums","type":{"type":"array","items":"int"}},
      {"name":"xfers","type":{"type":"array","items":{"type":"record","name":"X","fields":[
        {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":0}}
      ]}}}
    ]}"#;
    let rows: Vec<Value> = (0..4i64)
        .map(|i| {
            Value::Record(vec![
                ("id".into(), Value::Long(i)),
                (
                    "nums".into(),
                    Value::Array((0..i).map(|x| Value::Int(x as i32)).collect()),
                ),
                (
                    "xfers".into(),
                    Value::Array(
                        (0..i)
                            .map(|x| {
                                Value::Record(vec![(
                                    "amt".into(),
                                    Value::Decimal(Decimal::from(vec![x as u8 + 1])),
                                )])
                            })
                            .collect(),
                    ),
                ),
            ])
        })
        .collect();
    let b = decode_rows(MIXED, rows);
    assert_eq!(b.num_rows(), 4);
    for c in b.columns() {
        assert_eq!(c.len(), 4, "every column has 4 rows");
    }
    let nums = col_list(&b, "nums");
    assert_eq!(
        nums.value_offsets(),
        &[0, 0, 1, 3, 6],
        "cumulative 0+0+1+2+3"
    );
    let xfers = col_list(&b, "xfers");
    assert_eq!(xfers.value_offsets(), &[0, 0, 1, 3, 6]);
}
