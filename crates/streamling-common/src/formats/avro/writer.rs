//! Copied from the Arroyo project, which is licensed under the Apache License 2.0.
//! https://github.com/ArroyoSystems/arroyo/blob/master/crates/arroyo-formats/src/avro/ser.rs

use apache_avro::Schema;
use apache_avro::types::{Record, Value};
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::types::{
    Decimal128Type, Float16Type, Float32Type, Float64Type, Int8Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt32Type, UInt64Type,
};
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{Decimal256Type, i256};
use num_bigint::{BigInt, Sign};
use regex::Regex;
use serde_json::json;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

trait SerializeTarget {
    fn add(&mut self, i: usize, name: &str, value: Value);
    fn is_some(&self, i: usize) -> bool;
}

impl SerializeTarget for Vec<Option<Record<'_>>> {
    fn add(&mut self, i: usize, name: &str, value: Value) {
        if let Some(r) = &mut self[i] {
            r.put(name, value);
        }
    }

    fn is_some(&self, i: usize) -> bool {
        self[i].is_some()
    }
}

impl SerializeTarget for Vec<Value> {
    fn add(&mut self, _: usize, _: &str, value: Value) {
        self.push(value);
    }

    fn is_some(&self, _: usize) -> bool {
        true
    }
}

/// Build the Avro record-schema JSON for a struct's `fields`, preserving nested
/// `logicalType` attributes (decimal, date, timestamp, …).
///
/// This must be assembled as JSON directly: Avro's Parsing Canonical Form
/// (`Schema::canonical_form`) *strips* `logicalType`, so round-tripping a nested
/// struct schema through it silently demotes nested decimals to plain `bytes`
/// (the F7 cause — the value encoder then emits `Decimal` against a `Bytes`
/// schema and fails).
fn record_schema_json(name: &str, fields: &Fields) -> serde_json::value::Value {
    let avro_fields: Vec<_> = fields.iter().map(|f| field_to_avro(name, f)).collect();
    json!({
        "type": "record",
        "name": name,
        "fields": avro_fields,
    })
}

/// Computes an avro schema from an arrow schema
pub fn to_avro(name: &str, fields: &Fields) -> Schema {
    // TODO: make it a Result
    Schema::parse_str(&record_schema_json(name, fields).to_string()).unwrap()
}

fn field_to_avro(name: &str, field: &Field) -> serde_json::value::Value {
    let next_name = format!("{}_{}", name, &field.name());
    // T060: decimal_arb fields are LargeBinary at the DataType level but
    // carry extension metadata that promotes them to Avro's `decimal`
    // logical type with the user-declared (precision, scale). Detect and
    // route here before falling through to the generic LargeBinary →
    // bytes mapping, which would otherwise lose numeric semantics.
    let mut schema = if let Some((precision, scale)) =
        crate::types::decimal_arb::DecimalArbType::precision_scale_from_field(field)
    {
        json!({
            "type": "bytes",
            "logicalType": "decimal",
            "scale": scale,
            "precision": precision,
        })
    } else {
        arrow_to_avro(&next_name, field.data_type())
    };

    if field.is_nullable() {
        schema = json!({
            "type": ["null", schema]
        })
    }

    json!({
        "name": sanitize_field(field.name()),
        "type": schema
    })
}

fn arrow_to_avro(name: &str, dt: &DataType) -> serde_json::value::Value {
    let typ = match dt {
        DataType::Null => unreachable!("null fields are not supported"),
        DataType::Boolean => "boolean",
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            "int"
        }
        // TODO: not all values of u64 can be represented as a long in avro
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "long",
        DataType::Float16 | DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Timestamp(t, tz) => {
            let logical = match (t, tz) {
                (TimeUnit::Microsecond | TimeUnit::Nanosecond, None) => "timestamp-micros",
                (TimeUnit::Microsecond | TimeUnit::Nanosecond, Some(_)) => "local-timestamp-micros",
                (TimeUnit::Millisecond | TimeUnit::Second, None) => "timestamp-millis",
                (TimeUnit::Millisecond | TimeUnit::Second, Some(_)) => "local-timestamp-millis",
            };

            return json!({
                "type": "long",
                "logicalType": logical
            });
        }
        DataType::Date32 | DataType::Date64 => {
            return json!({
                "type": "int",
                "logicalType": "date"
            });
        }
        DataType::Time64(_) | DataType::Time32(_) => {
            todo!("time is not supported")
        }
        DataType::Duration(_) => todo!("duration is not supported"),
        DataType::Interval(_) => todo!("interval is not supported"),
        DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => "bytes",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
        DataType::List(t) | DataType::FixedSizeList(t, _) | DataType::LargeList(t) => {
            return json!({
                "type": "array",
                "items": field_to_avro("item", t),
            });
        }
        DataType::Struct(fields) => {
            // Build the nested record JSON directly — NOT via canonical_form,
            // which strips nested logicalType (decimal/date/timestamp) and breaks
            // nested decimal encoding (F7).
            return record_schema_json(name, fields);
        }
        DataType::Union(_, _) => unimplemented!("unions are not supported"),
        DataType::Dictionary(_, _) => unimplemented!("dictionaries are not supported"),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => {
            return json!({
                "type": "bytes",
                "logicalType": "decimal",
                "scale": scale,
                "precision": precision,
            });
        }
        DataType::Decimal256(precision, scale) => {
            return json!({
                "type": "bytes",
                "logicalType": "decimal",
                "scale": scale,
                "precision": precision,
            });
        }
        DataType::Map(_, _) => unimplemented!("maps are not supported"),
        DataType::RunEndEncoded(_, _) => unimplemented!("run end encoded is not supported"),
        DataType::BinaryView => unimplemented!("binary view is not supported"),
        // Utf8View handled above alongside Utf8/LargeUtf8
        DataType::ListView(_) => unimplemented!("list view is not supported"),
        DataType::LargeListView(_) => unimplemented!("large list view is not suported"),
    };

    json!({
        "type": typ
    })
}

fn get_field_schema<'a>(schema: &'a Schema, name: &str, nullable: bool) -> &'a Schema {
    let Schema::Record(record_schema) = schema else {
        panic!("invalid avro schema -- struct field {name} should correspond to record schema");
    };

    // For lists the name is empty, but the schema argument is already the item schema
    if name.is_empty() {
        return schema;
    }

    let record_field_number = record_schema.lookup.get(name).unwrap();
    let schema = &record_schema.fields[*record_field_number].schema;

    if nullable {
        let Schema::Union(__union_schema) = schema else {
            panic!(
                "invalid avro schema -- struct field {name} is nullable and should be represented by a union"
            );
        };
        __union_schema.variants().get(1).unwrap_or_else(|| {
            panic!("invalid avro schema -- struct field {name} should be a union with two variants")
        })
    } else {
        schema
    }
}

pub fn from_nanos(ts: u128) -> SystemTime {
    UNIX_EPOCH
        + Duration::from_secs((ts / 1_000_000_000) as u64)
        + Duration::from_nanos((ts % 1_000_000_000) as u64)
}

pub fn to_micros(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
}

fn sanitize_field(s: &str) -> String {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"[^a-zA-Z0-9_.]").unwrap());

    re.replace_all(s, "_").replace('.', "__")
}

/// Convert canonical decimal_arb bytes (`[sign][big-endian unsigned magnitude]`,
/// per `contracts/arrow-extension-type.md` §3) into Avro's signed two's-complement
/// big-endian representation of the unscaled integer (`value × 10^scale`),
/// which the Avro `decimal` logical type expects under §9.
///
/// Canonical bytes already store the value at the column's declared scale,
/// so the BigInt decoded here is exactly the unscaled magnitude that Avro
/// expects — no further rescaling is required at write time.
fn decimal_arb_canonical_to_avro_bytes(canonical: &[u8]) -> Vec<u8> {
    // Empty payload guards against malformed cells; treat as zero so we
    // never panic on a row-by-row encode. Validity bitmap is the
    // authoritative null source per §4.
    if canonical.is_empty() {
        return vec![0x00];
    }
    let sign_byte = canonical[0];
    let magnitude = &canonical[1..];
    let bigint = match (sign_byte, magnitude.is_empty()) {
        (0xFF, false) => BigInt::from_bytes_be(Sign::Minus, magnitude),
        (_, true) => BigInt::from(0),
        // Treat any non-0xFF sign byte as non-negative; canonical encoder
        // only ever emits 0x00 or 0xFF.
        (_, false) => BigInt::from_bytes_be(Sign::Plus, magnitude),
    };
    bigint.to_signed_bytes_be()
}

#[allow(clippy::redundant_closure_call)]
fn serialize_column<T: SerializeTarget>(
    schema: &Schema,
    values: &mut T,
    name: &str,
    column: &ArrayRef,
    nullable: bool,
    field: Option<&Field>,
) {
    // T060: decimal_arb columns store canonical bytes in a LargeBinary
    // array but must surface as Avro's `decimal` logical type (§9). We
    // detect via the field's extension metadata before falling through
    // to the generic LargeBinary → Bytes mapping below.
    if let Some(f) = field
        && crate::types::decimal_arb::DecimalArbType::precision_scale_from_field(f).is_some()
    {
        let array = column.as_binary::<i64>();
        for (i, v) in array.iter().enumerate() {
            if !values.is_some(i) {
                continue;
            }
            let avro_value = v.map(|bytes| {
                let avro_bytes = decimal_arb_canonical_to_avro_bytes(bytes);
                Value::Decimal(apache_avro::Decimal::from(avro_bytes))
            });
            if nullable {
                values.add(
                    i,
                    name,
                    Value::Union(
                        avro_value.is_some() as u32,
                        Box::new(avro_value.unwrap_or(Value::Null)),
                    ),
                );
            } else {
                values.add(
                    i,
                    name,
                    avro_value.expect("non-nullable decimal_arb column has null cell"),
                );
            }
        }
        return;
    }

    macro_rules! write_arrow_value {
        ($as_call:path, $value_variant:path, $converter:expr) => {{
            $as_call(column).iter().enumerate().for_each(|(i, v)| {
                if values.is_some(i) {
                    if nullable {
                        values.add(
                            i,
                            name,
                            Value::Union(
                                v.is_some() as u32,
                                Box::new(
                                    v.map(|v| $value_variant($converter(v)))
                                        .unwrap_or(Value::Null),
                                ),
                            ),
                        );
                    } else {
                        values.add(
                            i,
                            name,
                            $value_variant($converter(v.expect("cannot be none"))),
                        );
                    }
                }
            })
        }};
    }

    macro_rules! write_primitive {
        ($primitive_type:ty, $rust_type:ty, $value_variant:path) => {
            write_arrow_value!(
                ArrayRef::as_primitive::<$primitive_type>,
                $value_variant,
                |v| Into::<$rust_type>::into(v)
            )
        };
    }

    match column.data_type() {
        DataType::Utf8 => {
            write_arrow_value!(ArrayRef::as_string::<i32>, Value::String, |v: &str| v
                .into())
        }
        DataType::LargeUtf8 => {
            write_arrow_value!(ArrayRef::as_string::<i64>, Value::String, |v: &str| v
                .into())
        }
        DataType::Utf8View => {
            write_arrow_value!(ArrayRef::as_string_view, Value::String, |v: &str| v.into())
        }
        DataType::Boolean => write_arrow_value!(ArrayRef::as_boolean, Value::Boolean, |v| v),

        DataType::Int8 => write_primitive!(Int8Type, i32, Value::Int),
        DataType::Int32 => write_primitive!(Int32Type, i32, Value::Int),
        DataType::Int64 => write_primitive!(Int64Type, i64, Value::Long),

        DataType::UInt8 => write_primitive!(UInt8Type, i32, Value::Int),
        DataType::UInt32 => write_primitive!(UInt32Type, i64, Value::Long),
        DataType::UInt64 => {
            write_arrow_value!(ArrayRef::as_primitive::<UInt64Type>, Value::Long, |v| v
                as i64)
        }

        DataType::Float16 => write_primitive!(Float16Type, f32, Value::Float),
        DataType::Float32 => write_primitive!(Float32Type, f32, Value::Float),
        DataType::Float64 => write_primitive!(Float64Type, f64, Value::Double),

        DataType::Decimal128(_, _) => {
            write_arrow_value!(
                ArrayRef::as_primitive::<Decimal128Type>,
                Value::Decimal,
                |v: i128| { v.to_be_bytes().into() }
            );
        }

        DataType::Decimal256(_, _) => {
            write_arrow_value!(
                ArrayRef::as_primitive::<Decimal256Type>,
                Value::Decimal,
                |v: i256| { v.to_be_bytes().into() }
            );
        }

        DataType::Timestamp(TimeUnit::Second, _) => write_arrow_value!(
            ArrayRef::as_primitive::<TimestampSecondType>,
            Value::TimestampMillis,
            |v| v * 1_000
        ),

        DataType::Timestamp(TimeUnit::Millisecond, _) => write_arrow_value!(
            ArrayRef::as_primitive::<TimestampMillisecondType>,
            Value::TimestampMillis,
            |v| v
        ),

        DataType::Timestamp(TimeUnit::Microsecond, _) => write_arrow_value!(
            ArrayRef::as_primitive::<TimestampMicrosecondType>,
            Value::TimestampMicros,
            |v| v
        ),

        DataType::Timestamp(TimeUnit::Nanosecond, _) => write_arrow_value!(
            ArrayRef::as_primitive::<TimestampNanosecondType>,
            Value::TimestampMicros,
            |v| to_micros(from_nanos(v as u128)) as i64
        ),

        DataType::Date32 => {
            write_arrow_value!(ArrayRef::as_primitive::<Int32Type>, Value::Date, |v| v)
        }
        DataType::Date64 => write_arrow_value!(
            ArrayRef::as_primitive::<Int64Type>,
            Value::Date,
            |v| (v / 86400000) as i32
        ),

        DataType::Binary => {
            write_arrow_value!(ArrayRef::as_binary::<i32>, Value::Bytes, |v: &[u8]| v
                .to_vec())
        }

        DataType::LargeBinary => {
            write_arrow_value!(ArrayRef::as_binary::<i64>, Value::Bytes, |v: &[u8]| v
                .to_vec())
        }

        DataType::FixedSizeBinary(_) => {
            write_arrow_value!(
                ArrayRef::as_fixed_size_binary,
                Value::Bytes,
                |v: &[u8]| v.to_vec()
            )
        }

        DataType::List(item) => {
            let schema = get_field_schema(schema, name, nullable);
            let Schema::Array(item_schema) = schema else {
                panic!(
                    "invalid avro schema -- list field {name} should correspond to array schema but is {schema:?}"
                );
            };

            let item_values: Vec<Option<Vec<Value>>> = if let Some(nulls) = column.nulls() {
                nulls
                    .iter()
                    .map(|null| null.then(std::vec::Vec::new))
                    .collect()
            } else {
                (0..column.len()).map(|_| Some(vec![])).collect()
            };

            for ((i, mut v), column) in item_values
                .into_iter()
                .enumerate()
                .zip(column.as_list::<i32>().iter())
            {
                if let Some(v) = &mut v {
                    serialize_column(
                        &item_schema.items,
                        v,
                        "",
                        &column.expect("unmasked null in list"),
                        item.is_nullable(),
                        Some(item.as_ref()),
                    )
                }

                if nullable {
                    values.add(
                        i,
                        name,
                        Value::Union(
                            v.is_some() as u32,
                            Box::new(v.map(Value::Array).unwrap_or_else(|| Value::Null)),
                        ),
                    );
                } else {
                    values.add(
                        i,
                        name,
                        Value::Array(v.expect("null found in non-nullable list column")),
                    );
                }
            }
        }

        DataType::Struct(fields) => {
            let schema = get_field_schema(schema, name, nullable);
            if nullable {
                let mut struct_values: Vec<_> = if let Some(nulls) = column.nulls() {
                    nulls
                        .iter()
                        .map(|null| null.then(|| Record::new(schema).unwrap()))
                        .collect()
                } else {
                    (0..column.len())
                        .map(|_| Some(Record::new(schema).unwrap()))
                        .collect()
                };

                for (field, column) in fields.iter().zip(column.as_struct().columns()) {
                    let name = sanitize_field(field.name());

                    serialize_column(
                        schema,
                        &mut struct_values,
                        &name,
                        column,
                        field.is_nullable(),
                        Some(field.as_ref()),
                    );
                }

                for (i, struct_v) in struct_values.into_iter().enumerate() {
                    values.add(
                        i,
                        name,
                        Value::Union(
                            struct_v.is_some() as u32,
                            Box::new(if let Some(struct_v) = struct_v {
                                struct_v.into()
                            } else {
                                Value::Null
                            }),
                        ),
                    );
                }
            } else {
                let mut struct_values = (0..column.len())
                    .map(|_| Some(Record::new(schema).unwrap()))
                    .collect::<Vec<_>>();

                for (field, column) in fields.iter().zip(column.as_struct().columns()) {
                    let name = sanitize_field(field.name());

                    serialize_column(
                        schema,
                        &mut struct_values,
                        &name,
                        column,
                        field.is_nullable(),
                        Some(field.as_ref()),
                    );
                }

                for (i, struct_v) in struct_values.into_iter().enumerate() {
                    values.add(i, name, Into::<Value>::into(struct_v.expect("not null")));
                }
            }
        }

        _ => unimplemented!("unsupported data type: {}", column.data_type()),
    };
}

pub fn serialize(schema: &Schema, batch: &RecordBatch) -> Vec<Value> {
    let mut values = (0..batch.num_rows())
        .map(|_| Some(Record::new(schema).unwrap()))
        .collect::<Vec<_>>();

    for i in 0..batch.num_columns() {
        let column = batch.column(i);
        let field = &batch.schema().fields[i];

        let name = sanitize_field(field.name());
        serialize_column(
            schema,
            &mut values,
            &name,
            column,
            field.is_nullable(),
            Some(field.as_ref()),
        );
    }

    values.into_iter().flatten().map(|r| r.into()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::array::builder::{
        Int64Builder, ListBuilder, StringBuilder, StructBuilder,
    };
    use std::sync::Arc;

    /// A decimal nested inside a struct must keep its `decimal` logicalType in the
    /// generated Avro schema. Regression guard for F7: the struct path used
    /// `Schema::canonical_form()`, which strips logicalType and demoted nested
    /// decimals to plain `bytes` (the value encoder then emitted `Decimal` against
    /// a `Bytes` schema and failed). Covers both decimal_arb and standard Decimal128.
    #[test]
    fn nested_struct_decimal_keeps_logical_type_in_schema() {
        use crate::types::decimal_arb::DecimalArbType;

        let inner = vec![
            DecimalArbType::field("amt", 20, 0, false).unwrap(),
            Field::new("d128", DataType::Decimal128(10, 2), false),
        ];
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("inner", DataType::Struct(inner.into()), false),
        ]);

        let avro = to_avro("R", &arrow_schema.fields);
        let json = serde_json::to_string(&avro).unwrap();

        // Both nested decimals must surface their decimal logicalType, not bytes.
        let decimal_count = json.matches("\"logicalType\":\"decimal\"").count();
        assert_eq!(
            decimal_count, 2,
            "both nested decimals must keep decimal logicalType; schema was: {json}"
        );
    }

    #[test]
    fn test_writing() {
        use apache_avro::types::Value::*;

        let address_fields = vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ];

        let second_address_fields = vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ];

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("favorite_number", DataType::Int32, false),
            Field::new("favorite_color", DataType::Utf8, true),
            Field::new("favorite_decimal", DataType::Decimal128(10, 5), false),
            Field::new(
                "address",
                DataType::Struct(address_fields.clone().into()),
                false,
            ),
            Field::new(
                "second_address",
                DataType::Struct(second_address_fields.clone().into()),
                true,
            ),
            Field::new(
                "numbers",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]));

        let names = vec!["Alyssa", "Ben", "Charlie"];
        let favorite_numbers = vec![256, 7, 0];
        let favorite_colors = vec![None, Some("red"), None];
        let favorite_decimals = vec![100, 110, -3099];

        let mut address_builder = StructBuilder::from_fields(address_fields, 3);
        let mut second_address_builder = StructBuilder::from_fields(second_address_fields, 3);

        address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("123 Elm St");
        address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("Springfield");
        address_builder.append(true);
        second_address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("321 Pine St");
        second_address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("Sacramento");
        second_address_builder
            .field_builder::<StringBuilder>(2)
            .unwrap()
            .append_null();
        second_address_builder.append(true);

        address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("456 Oak St");
        address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("Boston");
        address_builder.append(true);
        second_address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("645 Glen Ave");
        second_address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("Santa Cruz");
        second_address_builder
            .field_builder::<StringBuilder>(2)
            .unwrap()
            .append_value("Ben");
        second_address_builder.append(true);

        address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("789 Pine St");
        address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("Calgary");
        address_builder.append(true);
        second_address_builder
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_null();
        second_address_builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_null();
        second_address_builder
            .field_builder::<StringBuilder>(2)
            .unwrap()
            .append_null();
        second_address_builder.append(false);

        let mut numbers = ListBuilder::new(Int64Builder::new());
        numbers.append_value([Some(1), Some(2), Some(3)]);
        numbers.append_null();
        numbers.append_value([Some(4), Some(5)]);

        let avro_schema = to_avro("User", &arrow_schema.fields);

        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(datafusion::arrow::array::StringArray::from(names)),
                Arc::new(datafusion::arrow::array::Int32Array::from(favorite_numbers)),
                Arc::new(datafusion::arrow::array::StringArray::from(favorite_colors)),
                Arc::new(
                    datafusion::arrow::array::Decimal128Array::from(favorite_decimals)
                        .with_precision_and_scale(10, 5)
                        .unwrap(),
                ),
                Arc::new(address_builder.finish()),
                Arc::new(second_address_builder.finish()),
                Arc::new(numbers.finish()),
            ],
        )
        .unwrap();

        let result: Vec<apache_avro::types::Value> = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![
                Record(vec![
                    ("name".to_string(), String("Alyssa".to_string())),
                    ("favorite_number".to_string(), Int(256)),
                    ("favorite_color".to_string(), Union(0, Box::new(Null))),
                    (
                        "favorite_decimal".to_string(),
                        Decimal(apache_avro::Decimal::from(100i128.to_be_bytes()))
                    ),
                    (
                        "address".to_string(),
                        Record(vec![
                            ("street".to_string(), String("123 Elm St".to_string())),
                            ("city".to_string(), String("Springfield".to_string())),
                        ])
                    ),
                    (
                        "second_address".to_string(),
                        Union(
                            1,
                            Box::new(Record(vec![
                                ("street".to_string(), String("321 Pine St".to_string())),
                                ("city".to_string(), String("Sacramento".to_string())),
                                ("name".to_string(), Union(0, Box::new(Null))),
                            ]))
                        )
                    ),
                    (
                        "numbers".to_string(),
                        Union(
                            1,
                            Box::new(Array(vec![
                                Union(1, Box::new(Long(1))),
                                Union(1, Box::new(Long(2))),
                                Union(1, Box::new(Long(3)))
                            ]))
                        )
                    )
                ]),
                Record(vec![
                    ("name".to_string(), String("Ben".to_string())),
                    ("favorite_number".to_string(), Int(7)),
                    (
                        "favorite_color".to_string(),
                        Union(1, Box::new(String("red".to_string())))
                    ),
                    (
                        "favorite_decimal".to_string(),
                        Decimal(apache_avro::Decimal::from(110i128.to_be_bytes()))
                    ),
                    (
                        "address".to_string(),
                        Record(vec![
                            ("street".to_string(), String("456 Oak St".to_string())),
                            ("city".to_string(), String("Boston".to_string())),
                        ])
                    ),
                    (
                        "second_address".to_string(),
                        Union(
                            1,
                            Box::new(Record(vec![
                                ("street".to_string(), String("645 Glen Ave".to_string())),
                                ("city".to_string(), String("Santa Cruz".to_string())),
                                (
                                    "name".to_string(),
                                    Union(1, Box::new(String("Ben".to_string())))
                                ),
                            ]))
                        )
                    ),
                    ("numbers".to_string(), Union(0, Box::new(Null)))
                ]),
                Record(vec![
                    ("name".to_string(), String("Charlie".to_string())),
                    ("favorite_number".to_string(), Int(0)),
                    ("favorite_color".to_string(), Union(0, Box::new(Null))),
                    (
                        "favorite_decimal".to_string(),
                        Decimal(apache_avro::Decimal::from((-3099i128).to_be_bytes()))
                    ),
                    (
                        "address".to_string(),
                        Record(vec![
                            ("street".to_string(), String("789 Pine St".to_string())),
                            ("city".to_string(), String("Calgary".to_string())),
                        ])
                    ),
                    ("second_address".to_string(), Union(0, Box::new(Null))),
                    (
                        "numbers".to_string(),
                        Union(
                            1,
                            Box::new(Array(vec![
                                Union(1, Box::new(Long(4))),
                                Union(1, Box::new(Long(5)))
                            ]))
                        )
                    )
                ]),
            ]
        )
    }

    #[test]
    fn test_timestamp_millisecond_serialization() {
        use apache_avro::types::Value::*;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]));

        // 1710720000 seconds = 2024-03-18T00:00:00Z
        // In milliseconds: 1710720000 * 1000 = 1_710_720_000_000
        let ts_millis: i64 = 1_710_720_000_000;

        let mut builder = datafusion::arrow::array::builder::TimestampMillisecondBuilder::new();
        builder.append_value(ts_millis);

        let batch =
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(builder.finish())]).unwrap();

        let avro_schema = to_avro("Test", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![Record(vec![("ts".to_string(), TimestampMillis(ts_millis))])]
        );
    }

    #[test]
    fn test_timestamp_second_serialization() {
        use apache_avro::types::Value::*;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        )]));

        // 1710720000 seconds = 2024-03-18T00:00:00Z
        let ts_seconds: i64 = 1_710_720_000;

        let mut builder =
            datafusion::arrow::array::builder::TimestampSecondBuilder::new().with_timezone("UTC");
        builder.append_value(ts_seconds);

        let batch =
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(builder.finish())]).unwrap();

        let avro_schema = to_avro("Test", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        // Seconds are converted to millis for Avro's timestamp-millis logical type
        assert_eq!(
            result,
            vec![Record(vec![(
                "ts".to_string(),
                TimestampMillis(ts_seconds * 1_000)
            )])]
        );
    }

    #[test]
    fn test_timestamp_microsecond_serialization() {
        use apache_avro::types::Value::*;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));

        // 1710720000 seconds = 2024-03-18T00:00:00Z
        // In microseconds: 1710720000 * 1_000_000 = 1_710_720_000_000_000
        let ts_micros: i64 = 1_710_720_000_000_000;

        let mut builder = datafusion::arrow::array::builder::TimestampMicrosecondBuilder::new();
        builder.append_value(ts_micros);

        let batch =
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(builder.finish())]).unwrap();

        let avro_schema = to_avro("Test", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![Record(vec![("ts".to_string(), TimestampMicros(ts_micros))])]
        );
    }

    /// End-to-end test: build a RecordBatch matching the Stellar ledger
    /// `ledger_closed_at` schema — `Timestamp(Second, None)` — and verify
    /// it serializes correctly through the Avro writer as `TimestampMillis`.
    #[test]
    fn test_stellar_timestamp_second_no_tz_end_to_end() {
        use apache_avro::types::Value::*;

        // Mimics the Stellar ledger schema's timestamp column
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("ledger_sequence", DataType::UInt32, false),
            Field::new(
                "ledger_closed_at",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
        ]));

        // Ledger 53000000 closed at 2024-03-18T00:00:00Z (Unix 1710720000)
        let ts_seconds: i64 = 1_710_720_000;

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::UInt32Array::from(vec![
                    53_000_000,
                ])),
                {
                    let mut b = datafusion::arrow::array::builder::TimestampSecondBuilder::new();
                    b.append_value(ts_seconds);
                    Arc::new(b.finish())
                },
            ],
        )
        .unwrap();

        let avro_schema = to_avro("StellarLedger", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![Record(vec![
                ("ledger_sequence".to_string(), Long(53_000_000)),
                (
                    "ledger_closed_at".to_string(),
                    TimestampMillis(ts_seconds * 1_000)
                ),
            ])]
        );
    }

    #[test]
    fn test_nested_records() {
        use apache_avro::types::Value::*;

        // Inner struct: { street: Utf8, city: Utf8 }
        let inner_fields = vec![
            Field::new("street", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
        ];

        let inner_item_field = Arc::new(Field::new(
            "inner_item",
            DataType::Struct(inner_fields.clone().into()),
            false,
        ));
        // Outer struct: { meta: Utf8, inner: List<Struct(inner_fields)> }
        let outer_fields = vec![
            Field::new("meta", DataType::Utf8, false),
            Field::new("inner", DataType::List(inner_item_field.clone()), false),
        ];

        let outer_item_field = Arc::new(Field::new(
            "outer_item",
            DataType::Struct(outer_fields.clone().into()),
            false,
        ));
        let root_fields = vec![Field::new(
            "outer",
            DataType::List(outer_item_field.clone()),
            false,
        )];

        let street_builder = StringBuilder::new();
        let city_builder = StringBuilder::new();
        let inner_struct_builder = StructBuilder::new(
            inner_fields.clone(),
            vec![Box::new(street_builder), Box::new(city_builder)],
        );

        let meta_builder = StringBuilder::new();
        let inner_list_builder =
            ListBuilder::new(inner_struct_builder).with_field(inner_item_field.clone());
        let outer_struct_builder = StructBuilder::new(
            outer_fields.clone(),
            vec![Box::new(meta_builder), Box::new(inner_list_builder)],
        );

        let mut outer_list_builder =
            ListBuilder::new(outer_struct_builder).with_field(outer_item_field.clone());

        for (meta_value, street_value, city_value) in
            [("m1", "s1", "c1"), ("m2", "s2", "c2"), ("m3", "s3", "c3")]
        {
            let outer_values = outer_list_builder.values();

            // Fill the OUTER struct's fields
            // meta
            {
                let meta = outer_values.field_builder::<StringBuilder>(0).unwrap();
                meta.append_value(meta_value);
            }

            // inner: List<Struct{street, city}> (for this OUTER struct)
            {
                let inner_list = outer_values
                    .field_builder::<ListBuilder<StructBuilder>>(1)
                    .unwrap();
                let inner_values = inner_list.values(); // &mut StructBuilder (inner struct)

                // inner[0]
                {
                    {
                        let street = inner_values.field_builder::<StringBuilder>(0).unwrap();
                        street.append_value(street_value);
                    }

                    {
                        let city = inner_values.field_builder::<StringBuilder>(1).unwrap();
                        city.append_value(city_value);
                    }

                    inner_values.append(true); // finish one inner struct
                }

                inner_list.append(true); // finish the inner list for this OUTER struct
            }

            outer_values.append(true); // finish the OUTER struct
            outer_list_builder.append(true); // finish one element in root.outer
        }

        let outer_array: ArrayRef = Arc::new(outer_list_builder.finish());
        let arrow_schema = Arc::new(Schema::new(root_fields));

        let avro_schema = to_avro("Outer", &arrow_schema.fields);
        println!("Avro schema: {}", avro_schema.canonical_form());

        let batch = RecordBatch::try_new(arrow_schema, vec![outer_array]).unwrap();

        let result: Vec<apache_avro::types::Value> = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![
                Record(vec![(
                    "outer".to_string(),
                    Array(vec![Record(vec![
                        ("meta".to_string(), String("m1".to_string())),
                        (
                            "inner".to_string(),
                            Array(vec![Record(vec![
                                ("street".to_string(), String("s1".to_string())),
                                ("city".to_string(), String("c1".to_string())),
                            ])]),
                        ),
                    ])]),
                )]),
                Record(vec![(
                    "outer".to_string(),
                    Array(vec![Record(vec![
                        ("meta".to_string(), String("m2".to_string())),
                        (
                            "inner".to_string(),
                            Array(vec![Record(vec![
                                ("street".to_string(), String("s2".to_string())),
                                ("city".to_string(), String("c2".to_string())),
                            ])]),
                        ),
                    ])]),
                )]),
                Record(vec![(
                    "outer".to_string(),
                    Array(vec![Record(vec![
                        ("meta".to_string(), String("m3".to_string())),
                        (
                            "inner".to_string(),
                            Array(vec![Record(vec![
                                ("street".to_string(), String("s3".to_string())),
                                ("city".to_string(), String("c3".to_string())),
                            ])]),
                        ),
                    ])]),
                )]),
            ]
        );
    }

    /// Regression test for the `unimplemented!("unsupported data type:
    /// FixedSizeBinary(32)")` panic. A Kafka avro decimal with precision > 76
    /// is read in as a U256, backed by `FixedSizeBinary(32)`; serializing it
    /// back out must not panic and should emit the raw bytes.
    #[test]
    fn test_fixed_size_binary_serialization() {
        use apache_avro::types::Value::*;
        use datafusion::arrow::array::FixedSizeBinaryArray;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "balance",
            DataType::FixedSizeBinary(32),
            false,
        )]));

        // Two 32-byte big-endian values, as a U256 column would hold.
        let row0 = [0u8; 32];
        let mut row1 = [0u8; 32];
        row1[30] = 0x01;
        row1[31] = 0x00; // == 256

        let array = FixedSizeBinaryArray::try_from_iter(vec![row0, row1].into_iter()).unwrap();

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(array)]).unwrap();
        let avro_schema = to_avro("U256Record", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![
                Record(vec![("balance".to_string(), Bytes(row0.to_vec()))]),
                Record(vec![("balance".to_string(), Bytes(row1.to_vec()))]),
            ]
        );
    }

    #[test]
    fn test_fixed_size_binary_nullable_serialization() {
        use apache_avro::types::Value::*;
        use datafusion::arrow::array::FixedSizeBinaryArray;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "maybe_hash",
            DataType::FixedSizeBinary(4),
            true,
        )]));

        let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(vec![1u8, 2, 3, 4]), None, Some(vec![5u8, 6, 7, 8])].into_iter(),
            4,
        )
        .unwrap();

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(array)]).unwrap();
        let avro_schema = to_avro("MaybeHash", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![
                Record(vec![(
                    "maybe_hash".to_string(),
                    Union(1, Box::new(Bytes(vec![1, 2, 3, 4])))
                )]),
                Record(vec![("maybe_hash".to_string(), Union(0, Box::new(Null)))]),
                Record(vec![(
                    "maybe_hash".to_string(),
                    Union(1, Box::new(Bytes(vec![5, 6, 7, 8])))
                )]),
            ]
        );
    }

    #[test]
    fn test_large_binary_serialization() {
        use apache_avro::types::Value::*;
        use datafusion::arrow::array::LargeBinaryArray;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::LargeBinary,
            true,
        )]));

        let array =
            LargeBinaryArray::from(vec![Some(b"hello".as_ref()), None, Some(b"world".as_ref())]);

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(array)]).unwrap();
        let avro_schema = to_avro("Payload", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![
                Record(vec![(
                    "payload".to_string(),
                    Union(1, Box::new(Bytes(b"hello".to_vec())))
                )]),
                Record(vec![("payload".to_string(), Union(0, Box::new(Null)))]),
                Record(vec![(
                    "payload".to_string(),
                    Union(1, Box::new(Bytes(b"world".to_vec())))
                )]),
            ]
        );
    }

    /// Regression test for the `unimplemented!("unsupported data type:
    /// LargeUtf8")` panic. EVM datasets declare unbounded string columns
    /// (log `data`, transaction calldata `input`) as `LargeUtf8` to avoid
    /// i32 string-offset overflow; serializing them to a Kafka avro sink
    /// must emit plain avro strings, exactly like `Utf8`.
    #[test]
    fn test_large_utf8_serialization() {
        use apache_avro::types::Value::*;
        use datafusion::arrow::array::LargeStringArray;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::LargeUtf8,
            false,
        )]));

        let array = LargeStringArray::from(vec!["0xdeadbeef", ""]);

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(array)]).unwrap();
        let avro_schema = to_avro("LogData", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![
                Record(vec![("data".to_string(), String("0xdeadbeef".to_string()))]),
                Record(vec![("data".to_string(), String("".to_string()))]),
            ]
        );
    }

    #[test]
    fn test_large_utf8_nullable_serialization() {
        use apache_avro::types::Value::*;
        use datafusion::arrow::array::LargeStringArray;

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "input",
            DataType::LargeUtf8,
            true,
        )]));

        let array = LargeStringArray::from(vec![Some("0x00"), None, Some("0xffff")]);

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(array)]).unwrap();
        let avro_schema = to_avro("TxInput", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        assert_eq!(
            result,
            vec![
                Record(vec![(
                    "input".to_string(),
                    Union(1, Box::new(String("0x00".to_string())))
                )]),
                Record(vec![("input".to_string(), Union(0, Box::new(Null)))]),
                Record(vec![(
                    "input".to_string(),
                    Union(1, Box::new(String("0xffff".to_string())))
                )]),
            ]
        );
    }

    // ------- T060: decimal_arb Avro schema generation -------

    #[test]
    fn decimal_arb_field_emits_avro_decimal_logical_type() {
        // A `decimal_arb(100, 18)` field on the Arrow side must surface as
        // an Avro `bytes` logical-type `decimal` with the declared
        // precision and scale. The previous behavior (LargeBinary → plain
        // "bytes") would have lost numeric semantics on the consumer side.
        let field =
            crate::types::decimal_arb::DecimalArbType::field("amount", 100, 18, false).unwrap();
        let avro_field = field_to_avro("payload", &field);
        // The schema is: { "name": "amount", "type": { ... decimal logical ... } }
        let type_field = avro_field.get("type").unwrap();
        assert_eq!(type_field.get("type").unwrap(), "bytes");
        assert_eq!(type_field.get("logicalType").unwrap(), "decimal");
        assert_eq!(type_field.get("precision").unwrap(), 100);
        assert_eq!(type_field.get("scale").unwrap(), 18);
    }

    #[test]
    fn decimal_arb_nullable_field_wraps_in_union() {
        // Nullable Avro fields wrap the schema in a union with "null".
        // The decimal logical type → outer union gets the inner schema
        // under the second variant (whose 'type' contains the bytes/
        // logicalType/precision/scale shape directly, NOT under a "type"
        // key).
        let field =
            crate::types::decimal_arb::DecimalArbType::field("amount", 80, 30, true).unwrap();
        let avro_field = field_to_avro("payload", &field);
        let nested_type = avro_field.get("type").unwrap();
        // {"type": ["null", { ... decimal ... }]}
        let outer = nested_type.get("type").unwrap();
        let arr = outer.as_array().expect("nullable type is union array");
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], "null");
        assert_eq!(arr[1].get("logicalType").unwrap(), "decimal");
        assert_eq!(arr[1].get("precision").unwrap(), 80);
        assert_eq!(arr[1].get("scale").unwrap(), 30);
    }

    #[test]
    fn plain_large_binary_field_still_maps_to_bytes() {
        // Regression guard: LargeBinary without decimal_arb metadata stays
        // as plain Avro `bytes` (no logical type promotion). The shape is
        // {"type": "bytes"} (the avro schema for primitive types is itself
        // wrapped in an object by arrow_to_avro).
        let field = Field::new("blob", DataType::LargeBinary, false);
        let avro_field = field_to_avro("payload", &field);
        let nested_type = avro_field.get("type").unwrap();
        // No logicalType key — stays as plain bytes.
        assert!(nested_type.get("logicalType").is_none());
        assert_eq!(nested_type.get("type").unwrap(), "bytes");
    }

    // ------- T060: decimal_arb Avro value serialization -------

    use std::str::FromStr;

    /// Decode an Avro `Value::Decimal` payload back into a canonical
    /// decimal_arb byte payload at `scale`. Mirrors
    /// `arrow_array_reader::resolve_decimal_arb_canonical_bytes` so these
    /// writer tests stay self-contained while still exercising the
    /// symmetric encoder/decoder contract from
    /// `contracts/arrow-extension-type.md` §3 + §9.
    fn avro_decimal_value_to_canonical_bytes(v: &Value, scale: u32) -> Vec<u8> {
        use crate::types::decimal_arb::DecimalArbValue;
        let inner = if let Value::Union(_, b) = v { b } else { v };
        match inner {
            Value::Decimal(d) => {
                let bytes = <Vec<u8>>::try_from(d).expect("decimal -> bytes");
                let bigint = BigInt::from_signed_bytes_be(&bytes);
                let value = DecimalArbValue::from_bigint_and_scale(bigint, scale as i64);
                value.to_canonical_bytes_at_scale(scale)
            }
            other => panic!("expected Value::Decimal, got {:?}", other),
        }
    }

    /// Build a single-column `RecordBatch` of `decimal_arb(precision, scale)`
    /// from a list of canonical decimal text values (or NULL).
    fn build_decimal_arb_batch(
        precision: u32,
        scale: u32,
        nullable: bool,
        values: &[Option<&str>],
    ) -> (Arc<Schema>, RecordBatch) {
        use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType};

        let mut builder =
            DecimalArbArrayBuilder::with_capacity(values.len(), "amount", precision, scale)
                .expect("builder");
        for v in values {
            match v {
                Some(s) => builder.append_str(s).expect("append"),
                None => builder.append_null(),
            }
        }
        let (raw, _, _) = builder.finish().into_inner();
        let field = DecimalArbType::field("amount", precision, scale, nullable).expect("field");
        let arrow_schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(raw)]).expect("batch");
        (arrow_schema, batch)
    }

    #[test]
    fn decimal_arb_round_trips_positive_value() {
        // precision/scale chosen to exercise the > Decimal256 (76) regime.
        let (arrow_schema, batch) = build_decimal_arb_batch(80, 4, false, &[Some("123.4500")]);

        let avro_schema = to_avro("Test", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        let Value::Record(fields) = &result[0] else {
            panic!("expected Record, got {:?}", result[0]);
        };
        let (_, decimal_value) = &fields[0];
        let canonical = avro_decimal_value_to_canonical_bytes(decimal_value, 4);

        // Expected canonical bytes for 123.45 at scale=4 → unscaled 1234500.
        let expected = crate::types::decimal_arb::DecimalArbValue::from_str("123.4500")
            .unwrap()
            .to_canonical_bytes_at_scale(4);
        assert_eq!(canonical, expected);
        // Sign byte should be 0x00 (non-negative) and magnitude non-empty.
        assert_eq!(canonical[0], 0x00);
        assert!(canonical.len() > 1);
    }

    #[test]
    fn decimal_arb_round_trips_negative_value() {
        let (arrow_schema, batch) = build_decimal_arb_batch(80, 6, false, &[Some("-0.000123")]);

        let avro_schema = to_avro("Test", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        let Value::Record(fields) = &result[0] else {
            panic!("expected Record");
        };
        let (_, decimal_value) = &fields[0];
        let canonical = avro_decimal_value_to_canonical_bytes(decimal_value, 6);

        let expected = crate::types::decimal_arb::DecimalArbValue::from_str("-0.000123")
            .unwrap()
            .to_canonical_bytes_at_scale(6);
        assert_eq!(canonical, expected);
        // Negative-value canonical bytes start with the sign byte 0xFF.
        assert_eq!(canonical[0], 0xFF);
    }

    #[test]
    fn decimal_arb_round_trips_null_value() {
        let (arrow_schema, batch) =
            build_decimal_arb_batch(80, 4, true, &[Some("1.0000"), None, Some("-2.5000")]);

        let avro_schema = to_avro("Test", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        // Row 0: positive
        let Value::Record(fields0) = &result[0] else {
            panic!("expected Record");
        };
        let Value::Union(branch_idx, payload) = &fields0[0].1 else {
            panic!("nullable decimal_arb should be encoded as a Union");
        };
        assert_eq!(
            *branch_idx, 1,
            "non-null cell takes the second union branch"
        );
        assert!(matches!(payload.as_ref(), Value::Decimal(_)));

        // Row 1: NULL
        let Value::Record(fields1) = &result[1] else {
            panic!("expected Record");
        };
        let Value::Union(branch_idx, payload) = &fields1[0].1 else {
            panic!("nullable decimal_arb should be encoded as a Union");
        };
        assert_eq!(*branch_idx, 0, "null cell takes the first union branch");
        assert!(matches!(payload.as_ref(), Value::Null));

        // Row 2: negative — round-trips
        let Value::Record(fields2) = &result[2] else {
            panic!("expected Record");
        };
        let canonical2 = avro_decimal_value_to_canonical_bytes(&fields2[0].1, 4);
        let expected2 = crate::types::decimal_arb::DecimalArbValue::from_str("-2.5000")
            .unwrap()
            .to_canonical_bytes_at_scale(4);
        assert_eq!(canonical2, expected2);
    }

    #[test]
    fn decimal_arb_round_trips_wide_precision_value() {
        // A 100-digit integer value — well outside Decimal128/Decimal256
        // range — proves we don't depend on a fixed-width integer path.
        let big = "1".to_string() + &"0".repeat(99); // 1e99 (100-digit integer)
        let (arrow_schema, batch) = build_decimal_arb_batch(120, 0, false, &[Some(&big)]);

        let avro_schema = to_avro("Test", &arrow_schema.fields);
        let result = serialize(&avro_schema, &batch);

        let Value::Record(fields) = &result[0] else {
            panic!("expected Record");
        };
        let canonical = avro_decimal_value_to_canonical_bytes(&fields[0].1, 0);
        let expected = crate::types::decimal_arb::DecimalArbValue::from_str(&big)
            .unwrap()
            .to_canonical_bytes_at_scale(0);
        assert_eq!(canonical, expected);
    }
}
