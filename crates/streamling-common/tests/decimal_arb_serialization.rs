//! Independent, adversarial serialization checks for PR37 revision 6e6072c.
use apache_avro::types::Value as AvroValue;
use arrow::{
    array::*,
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, Int32Type, Schema, SchemaRef},
    ipc::writer::FileWriter,
};
use datafusion::common::Result;
use num_bigint::BigInt;
use serde_json::{Value, json};
use std::{str::FromStr, sync::Arc};
use streamling_common::{
    formats::{
        FromArrowConverter, ToArrowConverter,
        avro::{serialize, to_avro},
        ipc::{FromArrowToIpcConverter, FromIpcToArrowConverter},
        json::{FromArrowToJsonConverter, JsonToArrowConverter},
    },
    types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue},
};

fn decimal(precision: u32, scale: u32, texts: &[Option<&str>]) -> (Field, ArrayRef) {
    let field = DecimalArbType::field("amount", precision, scale, true).unwrap();
    let mut b =
        DecimalArbArrayBuilder::with_capacity(texts.len(), "amount", precision, scale).unwrap();
    for text in texts {
        if let Some(text) = text {
            b.append_str(text).unwrap();
        } else {
            b.append_null();
        }
    }
    let (raw, _, _) = b.finish().into_inner();
    (field, Arc::new(raw))
}
fn batch(field: Field, array: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap()
}
fn shaped(kind: &str) -> RecordBatch {
    let (f, a) = decimal(
        100,
        8,
        &[
            Some("0"),
            None,
            Some("-12.34000001"),
            Some("1000"),
            Some("1234567890123456789012345678901234567890.00000001"),
            Some("-0.00000001"),
        ],
    );
    let field = Arc::new(f);
    let nulls = Some(NullBuffer::from(vec![true, false, true]));
    let array: ArrayRef = match kind {
        "leaf" => a,
        "struct" => Arc::new(StructArray::new(
            vec![field].into(),
            vec![a],
            Some(NullBuffer::from(vec![true, true, false, true, true, true])),
        )),
        "list" => Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0, 2, 4, 6].into()),
            a,
            nulls,
        )),
        "large_list" => Arc::new(LargeListArray::new(
            field,
            OffsetBuffer::new(vec![0_i64, 2, 4, 6].into()),
            a,
            nulls,
        )),
        "fixed_list" => Arc::new(FixedSizeListArray::new(field, 2, a, nulls)),
        "map" => {
            let key = Arc::new(Field::new("keys", DataType::Utf8, false));
            let entries = StructArray::new(
                vec![key, field].into(),
                vec![
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
                    a,
                ],
                None,
            );
            let ef = Arc::new(Field::new("entries", entries.data_type().clone(), false));
            Arc::new(MapArray::new(
                ef,
                OffsetBuffer::new(vec![0, 2, 4, 6].into()),
                entries,
                nulls,
                false,
            ))
        }
        "list_struct" => {
            let st = StructArray::new(vec![field].into(), vec![a], None);
            let sf = Arc::new(Field::new("item", st.data_type().clone(), true));
            Arc::new(ListArray::new(
                sf,
                OffsetBuffer::new(vec![0, 2, 4, 6].into()),
                Arc::new(st),
                nulls,
            ))
        }
        "list_view" => Arc::new(ListViewArray::new(
            field,
            ScalarBuffer::from(vec![4, 0, 2]),
            ScalarBuffer::from(vec![2, 2, 2]),
            a,
            nulls,
        )),
        "large_list_view" => Arc::new(LargeListViewArray::new(
            field,
            ScalarBuffer::from(vec![4_i64, 0, 2]),
            ScalarBuffer::from(vec![2_i64, 2, 2]),
            a,
            nulls,
        )),
        "dictionary_struct" => {
            let st = StructArray::new(vec![field].into(), vec![a], None);
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(4), Some(0), None, Some(2), Some(4), Some(1)]),
                    Arc::new(st),
                )
                .unwrap(),
            )
        }
        _ => panic!("bad kind"),
    };
    let field = if kind == "leaf" {
        DecimalArbType::field("value", 100, 8, true).unwrap()
    } else {
        Field::new("value", array.data_type().clone(), true)
    };
    batch(field, array)
}
fn native_ipc(input: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut w = FileWriter::try_new(&mut buf, input.schema().as_ref()).unwrap();
    w.write(input).unwrap();
    w.finish().unwrap();
    buf
}
fn read_ipc(bytes: Vec<u8>, target: SchemaRef) -> Result<RecordBatch> {
    let mut r = FromIpcToArrowConverter::new(target);
    r.buffer(bytes);
    r.convert_to_batch()
}
fn bridge_ipc(input: &RecordBatch, target: SchemaRef) -> Result<RecordBatch> {
    let bytes = FromArrowToIpcConverter::new()
        .convert_from_batch(input)?
        .pop()
        .unwrap();
    read_ipc(bytes, target)
}
fn json_rows(input: &RecordBatch) -> Result<Vec<Value>> {
    FromArrowToJsonConverter::new()
        .convert_from_batch(input)?
        .iter()
        .map(|v| Ok(serde_json::from_slice(v).unwrap()))
        .collect()
}
fn read_json(rows: &[Value], target: SchemaRef) -> Result<RecordBatch> {
    let mut r = JsonToArrowConverter::new(target, true, None);
    for row in rows {
        r.buffer(row.to_string());
    }
    r.convert_to_batch()
}
fn logical_value(field: &Field, array: &ArrayRef, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    if DecimalArbType::is_decimal_arb_field(field) {
        let (_, s) = DecimalArbType::precision_scale_from_field(field).unwrap();
        let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        return json!(
            DecimalArbValue::from_canonical_bytes_at_scale(arr.value(row), s)
                .unwrap()
                .to_canonical_string()
        );
    }
    let list_value = |child: &Field, values: ArrayRef| {
        Value::Array(
            (0..values.len())
                .map(|i| logical_value(child, &values, i))
                .collect(),
        )
    };
    match field.data_type() {
        DataType::Struct(fields) => {
            let st = array.as_any().downcast_ref::<StructArray>().unwrap();
            Value::Object(
                fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.name().clone(), logical_value(f, st.column(i), row)))
                    .collect(),
            )
        }
        DataType::List(f) => list_value(
            f,
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(row),
        ),
        DataType::LargeList(f) => list_value(
            f,
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .value(row),
        ),
        DataType::FixedSizeList(f, _) => list_value(
            f,
            array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap()
                .value(row),
        ),
        DataType::ListView(f) => list_value(
            f,
            array
                .as_any()
                .downcast_ref::<ListViewArray>()
                .unwrap()
                .value(row),
        ),
        DataType::LargeListView(f) => list_value(
            f,
            array
                .as_any()
                .downcast_ref::<LargeListViewArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Map(f, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            let st = map.value(row);
            let DataType::Struct(fields) = f.data_type() else {
                panic!()
            };
            Value::Object(
                (0..st.len())
                    .map(|i| {
                        (
                            st.column(0)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(i)
                                .to_owned(),
                            logical_value(&fields[1], st.column(1), i),
                        )
                    })
                    .collect(),
            )
        }
        DataType::Dictionary(_, dt) => {
            let d = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap();
            logical_value(
                &Field::new("value", dt.as_ref().clone(), true),
                d.values(),
                d.key(row).unwrap(),
            )
        }
        _ => panic!("unsupported logical oracle {field:?}"),
    }
}
fn expected_json(b: &RecordBatch) -> Vec<Value> {
    let s = b.schema();
    (0..b.num_rows())
        .map(|i| {
            Value::Object(
                s.fields()
                    .iter()
                    .enumerate()
                    .map(|(j, f)| (f.name().clone(), logical_value(f, b.column(j), i)))
                    .collect(),
            )
        })
        .collect()
}

#[test]
fn json_supported_containers_slices_nulls_matrix() {
    for kind in [
        "leaf",
        "struct",
        "list",
        "large_list",
        "fixed_list",
        "map",
        "list_struct",
    ] {
        let original = shaped(kind);
        for (start, len) in [
            (0, original.num_rows()),
            (1, 1),
            (original.num_rows() - 1, 1),
        ] {
            let input = original.slice(start, len);
            let expected = expected_json(&input);
            let rows = json_rows(&input).unwrap();
            assert_eq!(rows, expected, "JSON output {kind} slice{start}:{len}");
            let restored = read_json(&rows, input.schema()).unwrap();
            assert_eq!(
                expected_json(&restored),
                expected,
                "JSON roundtrip {kind} slice{start}:{len}"
            );
        }
    }
}
#[test]
fn ipc_supported_containers_slices_nulls_matrix() {
    let mut failures = Vec::new();
    for kind in [
        "leaf",
        "struct",
        "list",
        "large_list",
        "fixed_list",
        "map",
        "list_struct",
    ] {
        let original = shaped(kind);
        for (start, len) in [
            (0, original.num_rows()),
            (1, 1),
            (original.num_rows() - 1, 1),
        ] {
            let input = original.slice(start, len);
            let expected = expected_json(&input);
            match bridge_ipc(&input, input.schema()) {
                Ok(restored) => match json_rows(&restored) {
                    Ok(actual) if actual == expected => {}
                    Ok(actual) => failures.push(format!(
                        "{kind} slice{start}:{len}: actual{actual:?}, expected{expected:?}"
                    )),
                    Err(e) => failures.push(format!(
                        "{kind} slice{start}:{len}: {}",
                        e.to_string().lines().next().unwrap_or_default()
                    )),
                },
                Err(e) => failures.push(format!(
                    "{kind} slice{start}:{len}: {}",
                    e.to_string().lines().next().unwrap_or_default()
                )),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
#[test]
fn json_view_dictionary_decimal_output() {
    let mut failures = Vec::new();
    for kind in ["list_view", "large_list_view", "dictionary_struct"] {
        let input = shaped(kind);
        let actual = json_rows(&input);
        let expected = expected_json(&input);
        if actual.as_ref().ok() != Some(&expected) {
            failures.push(format!("{kind}: actual{actual:?} expected{expected:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
fn nested_decimal(precision: u32, scale: u32, text: &str) -> RecordBatch {
    let (f, a) = decimal(precision, scale, &[Some(text)]);
    let st = StructArray::new(vec![Arc::new(f)].into(), vec![a], None);
    batch(
        Field::new("nested", st.data_type().clone(), false),
        Arc::new(st),
    )
}
fn avro_first_decimal(v: &AvroValue) -> BigInt {
    match v {
        AvroValue::Record(fields) => avro_first_decimal(&fields[0].1),
        AvroValue::Union(_, v) => avro_first_decimal(v),
        AvroValue::Array(values) => avro_first_decimal(&values[0]),
        AvroValue::Decimal(d) => BigInt::from_signed_bytes_be(&Vec::<u8>::try_from(d).unwrap()),
        _ => panic!("{v:?}"),
    }
}
#[test]
fn ipc_nested_echo_to_avro_preserves_1000() {
    let input = nested_decimal(80, 0, "1000");
    let output = bridge_ipc(&input, input.schema()).unwrap();
    let schema = to_avro("Test", output.schema().fields());
    let actual = avro_first_decimal(&serialize(&schema, &output)[0]);
    assert_eq!(actual, BigInt::from(1000));
}
#[test]
fn native_ipc_nested_scale_change_preserves_12_34() {
    let input = nested_decimal(80, 2, "12.34");
    let target = nested_decimal(80, 4, "12.34").schema();
    let output = read_ipc(native_ipc(&input), target).unwrap();
    assert_eq!(
        expected_json(&output),
        vec![json!({"nested":{"amount":"12.3400"}})]
    );
}
#[test]
fn scalar_ipc_string_encodings_parse_exactly() {
    let target = Arc::new(Schema::new(vec![
        DecimalArbType::field("amount", 100, 8, true).unwrap(),
    ]));
    let values = [Some("-12.34000001"), None, Some("1000")];
    let dictionary: DictionaryArray<Int32Type> = values.into_iter().collect();
    for array in [
        Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
        Arc::new(LargeStringArray::from(values.to_vec())),
        Arc::new(StringViewArray::from(values.to_vec())),
        Arc::new(dictionary),
    ] {
        let input = batch(Field::new("amount", array.data_type().clone(), true), array);
        let output = read_ipc(native_ipc(&input), target.clone()).unwrap();
        assert_eq!(
            json_rows(&output).unwrap(),
            vec![
                json!({"amount":"-12.34000001"}),
                json!({"amount":null}),
                json!({"amount":"1000.00000000"})
            ]
        );
    }
}
#[test]
fn native_ipc_narrow_precision_does_not_accept_out_of_range() {
    let (f, a) = decimal(80, 0, &[Some("1000")]);
    let input = batch(f, a);
    let target = Arc::new(Schema::new(vec![
        DecimalArbType::field("amount", 2, 0, true).unwrap(),
    ]));
    assert!(
        read_ipc(native_ipc(&input), target).is_err(),
        "1000 does not fit decimal(2,0), but identical-scale native IPC bypasses validation"
    );
}

#[test]
fn ipc_zero_width_fixed_lists_preserve_row_count() {
    let (field, values) = decimal(80, 0, &[]);
    let array =
        FixedSizeListArray::try_new_with_length(Arc::new(field), 0, values, None, 3).unwrap();
    let input = batch(
        Field::new("items", array.data_type().clone(), false),
        Arc::new(array),
    );
    assert_eq!(input.num_rows(), 3);
    let output = bridge_ipc(&input, input.schema()).unwrap();
    assert_eq!(
        output.num_rows(),
        3,
        "Three valid empty-list rows must not disappear at the text bridge"
    );
}

#[test]
fn json_zero_width_fixed_lists_emit_rows() {
    let (field, values) = decimal(80, 0, &[]);
    let array =
        FixedSizeListArray::try_new_with_length(Arc::new(field), 0, values, None, 3).unwrap();
    let input = batch(
        Field::new("items", array.data_type().clone(), false),
        Arc::new(array),
    );
    let outputs = FromArrowToJsonConverter::new()
        .convert_from_batch(&input)
        .unwrap();
    assert_eq!(outputs, vec![br#"{"items":[]}"#.to_vec(); 3]);
}

fn decimal_text_from_unscaled(coefficient: &BigInt, scale: usize) -> String {
    let text = coefficient.to_string();
    let (negative, digits) = if let Some(rest) = text.strip_prefix('-') {
        (true, rest)
    } else {
        (false, text.as_str())
    };
    let unsigned = if scale == 0 {
        digits.to_string()
    } else if digits.len() > scale {
        format!(
            "{}.{}",
            &digits[..digits.len() - scale],
            &digits[digits.len() - scale..]
        )
    } else {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    };
    if negative {
        format!("-{unsigned}")
    } else {
        unsigned
    }
}

#[test]
fn avro_twos_complement_precision_scale_boundary_grid() {
    use streamling_common::formats::avro::arrow_avro::ConfluentAvroDecoder;
    let mut coefficients = vec![BigInt::from(0)];
    for bits in [
        1_usize, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 129, 255, 256, 257, 330,
    ] {
        let pivot = BigInt::from(1_u8) << bits;
        for delta in [-1_i32, 0, 1] {
            let value = &pivot + BigInt::from(delta);
            coefficients.push(value.clone());
            coefficients.push(-value);
        }
    }
    let mut checked = 0;
    for scale in [0_usize, 1, 18, 38, 76, 100] {
        for fixed in [false, true] {
            let typ = if fixed {
                json!({"type":"fixed","name":"F","size":42,"logicalType":"decimal","precision":100,"scale":scale})
            } else {
                json!({"type":"bytes","logicalType":"decimal","precision":100,"scale":scale})
            };
            let schema_json=json!({"type":"record","name":"Test","fields":[{"name":"amount","type":["null",typ]}]}).to_string();
            let avro = apache_avro::Schema::parse_str(&schema_json).unwrap();
            let mut decoder = ConfluentAvroDecoder::new();
            decoder.register_writer_schema(1, &schema_json).unwrap();
            for coefficient in &coefficients {
                let mut bytes = coefficient.to_signed_bytes_be();
                if fixed {
                    let fill = if coefficient < &BigInt::from(0) {
                        0xff
                    } else {
                        0
                    };
                    let mut padded = vec![fill; 42 - bytes.len()];
                    padded.append(&mut bytes);
                    bytes = padded;
                }
                let value = AvroValue::Record(vec![(
                    "amount".into(),
                    AvroValue::Union(
                        1,
                        Box::new(AvroValue::Decimal(apache_avro::Decimal::from(bytes))),
                    ),
                )]);
                let mut framed = vec![0, 0, 0, 0, 1];
                framed.extend(apache_avro::to_avro_datum(&avro, value).unwrap());
                decoder.decode(&framed).unwrap();
            }
            let output = decoder.flush().unwrap().unwrap();
            let raw = output
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            for (i, coefficient) in coefficients.iter().enumerate() {
                let actual =
                    DecimalArbValue::from_canonical_bytes_at_scale(raw.value(i), scale as u32)
                        .unwrap();
                let expected =
                    DecimalArbValue::from_str(&decimal_text_from_unscaled(coefficient, scale))
                        .unwrap();
                assert_eq!(
                    actual, expected,
                    "fixed={fixed} scale={scale} coefficient={coefficient}"
                );
                checked += 1;
            }
            // Serialize the decoded batch back to real Avro bytes and re-read,
            // so both sign-magnitude↔two's-complement directions are exercised.
            let writer = to_avro("RoundTrip", output.schema().fields());
            let mut second = ConfluentAvroDecoder::new();
            second
                .register_writer_schema(2, &serde_json::to_string(&writer).unwrap())
                .unwrap();
            for value in serialize(&writer, &output) {
                let mut framed = vec![0, 0, 0, 0, 2];
                framed.extend(apache_avro::to_avro_datum(&writer, value).unwrap());
                second.decode(&framed).unwrap();
            }
            let final_batch = second.flush().unwrap().unwrap();
            assert_eq!(output.columns(), final_batch.columns());
        }
    }
    eprintln!(
        "verified {checked} exact signed coefficient/scale/fixed-boundary cases in both Avro directions"
    );
}
