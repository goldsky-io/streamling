//! Copied from the Arroyo project, which is licensed under the Apache License 2.0.
//! https://github.com/ArroyoSystems/arroyo/blob/master/crates/arroyo-formats/src/avro/ser.rs

use crate::formats::decimal_arb_text::{plain_layout_field, to_plain_layout};
use apache_avro::Schema;
use apache_avro::types::{Record, Value};
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use datafusion::arrow::array::cast::AsArray;
use datafusion::arrow::array::types::{
    Decimal32Type, Decimal64Type, Decimal128Type, Float16Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{Decimal256Type, i256};
use num_bigint::{BigInt, Sign};
use regex::Regex;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
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

/// The record names already taken in the schema being built: Avro needs
/// every named type in a schema to have its own full name.
#[derive(Default)]
struct RecordNames(HashSet<String>);

impl RecordNames {
    /// `preferred` if no record has it yet, else `qualified`, else
    /// `qualified` with the first free numeric suffix. Both are first made
    /// valid Avro names (see [`record_name`]). Unqualified names inherit the
    /// enclosing record's namespace, which is part of their identity but
    /// need not be added to the emitted name.
    fn claim(&mut self, preferred: &str, qualified: &str, namespace: Option<&str>) -> String {
        let fullname = |name: &str| match namespace {
            Some(namespace) if !name.contains('.') => format!("{namespace}.{name}"),
            _ => name.to_string(),
        };
        let preferred = record_name(preferred);
        let qualified = record_name(qualified);
        if self.0.insert(fullname(&preferred)) {
            return preferred;
        }
        if self.0.insert(fullname(&qualified)) {
            return qualified;
        }
        (2..)
            .map(|n| format!("{qualified}_{n}"))
            .find(|candidate| self.0.insert(fullname(candidate)))
            .expect("an unbounded range always yields a free name")
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
///
/// The record is named `preferred`, or `qualified` on a collision (see
/// [`RecordNames::claim`]). `column` is the path of the struct this record
/// stands for, in the form error messages use (see [`field_to_avro`]); empty
/// for the root record.
fn record_schema_json(
    preferred: &str,
    qualified: &str,
    namespace: Option<&str>,
    column: &str,
    fields: &Fields,
    names: &mut RecordNames,
) -> Result<serde_json::value::Value, String> {
    let name = names.claim(preferred, qualified, namespace);
    let namespace = name
        .rsplit_once('.')
        .map(|(namespace, _)| namespace)
        .or(namespace);
    let avro_fields = fields
        .iter()
        .map(|f| {
            let child = if column.is_empty() {
                f.name().clone()
            } else {
                format!("{column}.{}", f.name())
            };
            field_to_avro(&name, &name, namespace, &child, f, names)
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(json!({
        "type": "record",
        "name": name,
        "fields": avro_fields,
    }))
}

/// Computes an avro schema from an arrow schema, or explains which column
/// has no Avro encoding.
pub fn try_to_avro(name: &str, fields: &Fields) -> crate::error::Result<Schema> {
    let json = record_schema_json(name, name, None, "", fields, &mut RecordNames::default())
        .map_err(|e| crate::streamling_user_err!("cannot encode as Avro: {}", e))?;
    Schema::parse_str(&json.to_string())
        .map_err(|e| crate::streamling_err!("invalid generated Avro schema: {}", e))
}

/// `prefix` / `qualified_prefix`: the names of the record (or list) the field
/// sits in — the first as it is preferred, the second unique to the field's
/// path (see [`RecordNames`]).
///
/// `column` names the field in errors, from the top-level column down: a
/// struct child as `col.child`, a list's items as `col[]`, a map's values as
/// `col{}`. The Arrow field names below the column (`item`, `entries`,
/// `value`) mean nothing to the user who has to drop or cast the column.
fn field_to_avro(
    prefix: &str,
    qualified_prefix: &str,
    namespace: Option<&str>,
    column: &str,
    field: &Field,
    names: &mut RecordNames,
) -> Result<serde_json::value::Value, String> {
    // A list view or a dictionary-/run-end-encoded column is written as its
    // plain layout (see `serialize_column`), so it is declared as one.
    if let Some(plain) = plain_layout_field(field) {
        return field_to_avro(prefix, qualified_prefix, namespace, column, &plain, names);
    }
    let next_name = format!("{}_{}", prefix, &field.name());
    let qualified_next_name = format!("{}_{}", qualified_prefix, &field.name());
    // decimal_arb fields are LargeBinary at the DataType level but
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
        arrow_to_avro(
            &next_name,
            &qualified_next_name,
            namespace,
            column,
            field,
            names,
        )?
    };

    if field.is_nullable() {
        schema = json!({
            "type": ["null", schema]
        })
    }

    Ok(json!({
        "name": sanitize_field(field.name()),
        "type": schema
    }))
}

fn arrow_to_avro(
    preferred: &str,
    qualified: &str,
    namespace: Option<&str>,
    column: &str,
    field: &Field,
    names: &mut RecordNames,
) -> Result<serde_json::value::Value, String> {
    let unsupported = |what: &str| {
        Err(format!(
            "column '{column}' is {what}, which has no Avro encoding"
        ))
    };
    let typ = match field.data_type() {
        DataType::Null => return unsupported("an untyped null"),
        DataType::Boolean => "boolean",
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            "int"
        }
        // TODO: not all values of u64 can be represented as a long in avro
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "long",
        DataType::Float16 | DataType::Float32 => "float",
        DataType::Float64 => "double",
        DataType::Timestamp(t, _) => {
            // Every timestamp is written as an offset from the epoch, which
            // is Avro's `timestamp-*` (an instant), time zone or not.
            let logical = match t {
                TimeUnit::Microsecond | TimeUnit::Nanosecond => "timestamp-micros",
                TimeUnit::Millisecond | TimeUnit::Second => "timestamp-millis",
            };

            return Ok(json!({
                "type": "long",
                "logicalType": logical
            }));
        }
        DataType::Date32 | DataType::Date64 => {
            return Ok(json!({
                "type": "int",
                "logicalType": "date"
            }));
        }
        DataType::Time64(_) | DataType::Time32(_) => return unsupported("a time"),
        DataType::Duration(_) => return unsupported("a duration"),
        DataType::Interval(_) => return unsupported("an interval"),
        DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => "bytes",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
        DataType::List(t) | DataType::FixedSizeList(t, _) | DataType::LargeList(t) => {
            // The item record prefers the `item` prefix; on a collision it
            // takes the list's own path.
            return Ok(json!({
                "type": "array",
                "items": field_to_avro("item", qualified, namespace, &format!("{column}[]"), t, names)?,
            }));
        }
        DataType::Map(entries, _) => {
            // Avro maps are keyed by strings.
            let DataType::Struct(kv) = entries.data_type() else {
                return unsupported("a map without key/value entries");
            };
            let (Some(key), Some(value)) = (kv.first(), kv.get(1)) else {
                return unsupported("a map without key/value entries");
            };
            if !matches!(
                key.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return unsupported("a map with non-string keys (Avro map keys are strings)");
            }
            // Arrow requires map keys to be non-null, and every producer here
            // (DataFusion's map functions, the JSON / Avro / Parquet readers)
            // declares the key field non-nullable. Arrow's `MapArray` does not
            // itself check key nulls, so a nullable key field is the one way a
            // null key could reach the row encoder, which has no Avro value
            // for it; refuse it up front.
            if key.is_nullable() {
                return unsupported(
                    "a map whose key field is nullable (Avro map keys are never null)",
                );
            }
            return Ok(json!({
                "type": "map",
                "values": field_to_avro(
                    qualified,
                    qualified,
                    namespace,
                    &format!("{column}{{}}"),
                    value,
                    names
                )?,
            }));
        }
        DataType::Struct(fields) => {
            // Build the nested record JSON directly — NOT via canonical_form,
            // which strips nested logicalType (decimal/date/timestamp) and breaks
            // nested decimal encoding (F7).
            return record_schema_json(preferred, qualified, namespace, column, fields, names);
        }
        DataType::Union(_, _) => return unsupported("a union"),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            return Ok(json!({
                "type": "bytes",
                "logicalType": "decimal",
                "scale": scale,
                "precision": precision,
            }));
        }
        DataType::BinaryView => return unsupported("a binary view"),
        // `field_to_avro` unwraps these to their plain layout first.
        DataType::Dictionary(_, _)
        | DataType::RunEndEncoded(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => return unsupported("an encoded layout"),
    };

    Ok(json!({
        "type": typ
    }))
}

fn get_field_schema<'a>(schema: &'a Schema, name: &str, nullable: bool) -> &'a Schema {
    // For lists the name is empty, but the schema argument is already the item
    // schema — a `["null", item]` union when the item is nullable, which is
    // unwrapped below like a nullable record field.
    let schema = if name.is_empty() {
        schema
    } else {
        let Schema::Record(record_schema) = schema else {
            panic!("invalid avro schema -- struct field {name} should correspond to record schema");
        };
        let record_field_number = record_schema.lookup.get(name).unwrap();
        &record_schema.fields[*record_field_number].schema
    };

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
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[^a-zA-Z0-9_.]").unwrap());
    RE.replace_all(s, "_").replace('.', "__")
}

/// `s` as a valid Avro full name: dot-separated segments, each starting with
/// a letter or underscore and holding only letters, digits and underscores.
///
/// Record names are built from the topic and from Arrow field names, neither
/// of which Avro's grammar constrains (`evm-blocks`, `my-field`), and
/// apache-avro panics while parsing a schema whose record name breaks it. A
/// name that already conforms is returned unchanged, so the schema of every
/// pipeline that could encode before is the same; one that did not has its
/// offending characters replaced by `_`, and a segment that is empty or
/// starts with a digit gets a leading `_`. Dots are kept: a dotted topic
/// names a namespace.
fn record_name(s: &str) -> String {
    s.split('.')
        .map(|segment| {
            let segment = segment.replace(|c: char| !c.is_ascii_alphanumeric() && c != '_', "_");
            match segment.as_bytes().first() {
                Some(b) if b.is_ascii_alphabetic() || *b == b'_' => segment,
                _ => format!("_{segment}"),
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Convert canonical decimal_arb bytes (`[sign][big-endian unsigned magnitude]`)
/// into Avro's signed two's-complement big-endian representation of the unscaled
/// integer (`value × 10^scale`), which the Avro `decimal` logical type expects.
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
    field: &Field,
    column_path: &str,
) -> crate::error::Result<()> {
    // A list view or a dictionary-/run-end-encoded column is written as its
    // plain layout, which is what `field_to_avro` declared for it.
    if let Some((plain, plain_column)) = to_plain_layout(field, column)
        .map_err(|e| crate::streamling_err!("cannot unwrap column '{column_path}': {e}"))?
    {
        return serialize_column(schema, values, name, &plain_column, &plain, column_path);
    }
    let nullable = field.is_nullable();

    // decimal_arb columns store canonical bytes in a LargeBinary
    // array but must surface as Avro's `decimal` logical type. We
    // detect via the field's extension metadata before falling through
    // to the generic LargeBinary → Bytes mapping below.
    if crate::types::decimal_arb::DecimalArbType::precision_scale_from_field(field).is_some() {
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
        return Ok(());
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
        DataType::Int16 => write_primitive!(Int16Type, i32, Value::Int),
        DataType::Int32 => write_primitive!(Int32Type, i32, Value::Int),
        DataType::Int64 => write_primitive!(Int64Type, i64, Value::Long),

        DataType::UInt8 => write_primitive!(UInt8Type, i32, Value::Int),
        DataType::UInt16 => write_primitive!(UInt16Type, i32, Value::Int),
        DataType::UInt32 => write_primitive!(UInt32Type, i64, Value::Long),
        DataType::UInt64 => {
            write_arrow_value!(ArrayRef::as_primitive::<UInt64Type>, Value::Long, |v| v
                as i64)
        }

        DataType::Float16 => write_primitive!(Float16Type, f32, Value::Float),
        DataType::Float32 => write_primitive!(Float32Type, f32, Value::Float),
        DataType::Float64 => write_primitive!(Float64Type, f64, Value::Double),

        DataType::Decimal32(_, _) => {
            write_arrow_value!(
                ArrayRef::as_primitive::<Decimal32Type>,
                Value::Decimal,
                |v: i32| { v.to_be_bytes().into() }
            );
        }

        DataType::Decimal64(_, _) => {
            write_arrow_value!(
                ArrayRef::as_primitive::<Decimal64Type>,
                Value::Decimal,
                |v: i64| { v.to_be_bytes().into() }
            );
        }

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

        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            let schema = get_field_schema(schema, name, nullable);
            let Schema::Array(item_schema) = schema else {
                panic!(
                    "invalid avro schema -- list field {name} should correspond to array schema but is {schema:?}"
                );
            };

            // Each row's items; a null row has none to write.
            let rows: Box<dyn Iterator<Item = Option<ArrayRef>> + '_> = match column.data_type() {
                DataType::List(_) => Box::new(column.as_list::<i32>().iter()),
                DataType::LargeList(_) => Box::new(column.as_list::<i64>().iter()),
                _ => Box::new(column.as_fixed_size_list().iter()),
            };
            let item_path = format!("{column_path}[]");
            for (i, row) in rows.enumerate() {
                // A slot under a null record holds nothing to write — and may
                // hold a null the record's validity masks, even in a
                // non-nullable child.
                if !values.is_some(i) {
                    continue;
                }
                let v = if let Some(items) = row {
                    let mut v = Vec::with_capacity(items.len());
                    serialize_column(&item_schema.items, &mut v, "", &items, item, &item_path)?;
                    Some(v)
                } else {
                    None
                };

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

        DataType::Map(entries_field, _) => {
            let schema = get_field_schema(schema, name, nullable);
            let Schema::Map(map_schema) = schema else {
                panic!(
                    "invalid avro schema -- map field {name} should correspond to map schema but is {schema:?}"
                );
            };
            let DataType::Struct(kv) = entries_field.data_type() else {
                panic!("map field {name} has no key/value entries");
            };
            let value_field = &kv[1];

            let map = column.as_map();
            let value_path = format!("{column_path}{{}}");
            for i in 0..map.len() {
                if !values.is_some(i) {
                    continue;
                }
                let v = if map.is_valid(i) {
                    let entries = map.value(i);
                    // `arrow_to_avro` admits string keys only.
                    let keys = datafusion::arrow::compute::cast(entries.column(0), &DataType::Utf8)
                        .map_err(|e| {
                            crate::streamling_err!(
                                "cannot cast map keys for column '{column_path}': {e}"
                            )
                        })?;
                    let keys = keys.as_string::<i32>();
                    let mut items = Vec::with_capacity(entries.len());
                    serialize_column(
                        &map_schema.types,
                        &mut items,
                        "",
                        entries.column(1),
                        value_field,
                        &value_path,
                    )?;
                    let mut v = HashMap::with_capacity(entries.len());
                    for (key, item) in keys.iter().zip(items) {
                        // `field_to_avro` admits a non-nullable key field
                        // only, and `StructArray` validation rejects nulls
                        // in one.
                        v.insert(
                            key.expect("non-nullable map key field holds a null")
                                .to_string(),
                            item,
                        );
                    }
                    Some(v)
                } else {
                    None
                };

                if nullable {
                    values.add(
                        i,
                        name,
                        Value::Union(
                            v.is_some() as u32,
                            Box::new(v.map(Value::Map).unwrap_or(Value::Null)),
                        ),
                    );
                } else {
                    values.add(
                        i,
                        name,
                        Value::Map(v.expect("null found in non-nullable map column")),
                    );
                }
            }
        }

        DataType::Struct(fields) => {
            let schema = get_field_schema(schema, name, nullable);
            // No record is built for a slot under a null parent record (or a
            // null struct), so the children skip it: a non-nullable child may
            // hold a null there that the parent's validity masks.
            let mut struct_values = Vec::with_capacity(column.len());
            for i in 0..column.len() {
                let record = if (!nullable || column.is_valid(i)) && values.is_some(i) {
                    Some(Record::new(schema).ok_or_else(|| {
                        crate::streamling_err!(
                            "invalid Avro record schema for column '{column_path}'"
                        )
                    })?)
                } else {
                    None
                };
                struct_values.push(record);
            }
            for (field, column) in fields.iter().zip(column.as_struct().columns()) {
                let name = sanitize_field(field.name());
                serialize_column(
                    schema,
                    &mut struct_values,
                    &name,
                    column,
                    field,
                    &format!("{column_path}.{}", field.name()),
                )?;
            }
            for (i, struct_v) in struct_values.into_iter().enumerate() {
                if nullable {
                    values.add(
                        i,
                        name,
                        Value::Union(
                            struct_v.is_some() as u32,
                            Box::new(struct_v.map_or(Value::Null, Into::into)),
                        ),
                    );
                } else if let Some(struct_v) = struct_v {
                    // `None` only under a null parent record, where `add` is a no-op.
                    values.add(i, name, struct_v.into());
                }
            }
        }

        _ => unimplemented!("unsupported data type: {}", column.data_type()),
    };
    Ok(())
}

/// Serialize a batch as Avro records, returning conversion errors with the
/// offending column path when an encoded layout or map key cannot be cast.
pub fn serialize(schema: &Schema, batch: &RecordBatch) -> crate::error::Result<Vec<Value>> {
    let mut values = Vec::with_capacity(batch.num_rows());
    for _ in 0..batch.num_rows() {
        values.push(Some(Record::new(schema).ok_or_else(|| {
            crate::streamling_err!("invalid Avro record schema")
        })?));
    }

    for i in 0..batch.num_columns() {
        let column = batch.column(i);
        let field = &batch.schema().fields[i];

        let name = sanitize_field(field.name());
        serialize_column(schema, &mut values, &name, column, field, field.name())?;
    }

    Ok(values.into_iter().flatten().map(|r| r.into()).collect())
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

    #[test]
    fn serialize_reports_nested_dictionary_union_cast_error() {
        use arrow_schema::UnionFields;
        use datafusion::arrow::array::{
            DictionaryArray, Int8Array, Int32Array, StructArray, UnionArray,
        };

        // This is a valid dictionary, but Arrow rejects casting to a Union
        // before dispatching to its dictionary-unpacking kernel.
        let union = UnionArray::try_new(
            UnionFields::from_fields(vec![Field::new("number", DataType::Int32, false)]),
            vec![0_i8].into(),
            None,
            vec![Arc::new(Int32Array::from(vec![7]))],
        )
        .unwrap();
        let dictionary = Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), Arc::new(union))
                .unwrap(),
        ) as ArrayRef;
        let choice = Arc::new(Field::new("choice", dictionary.data_type().clone(), false));
        let payload = StructArray::try_new(vec![choice].into(), vec![dictionary], None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            payload.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(payload)]).unwrap();
        // Schema generation deliberately refuses Arrow unions; a manual Avro
        // schema lets this test exercise the public serialization error boundary.
        let avro_schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"Root","fields":[
                {"name":"payload","type":{"type":"record","name":"Payload","fields":[
                    {"name":"choice","type":["null","int"]}
                ]}}
            ]}"#,
        )
        .unwrap();

        let error = serialize(&avro_schema, &batch).unwrap_err().to_string();
        assert!(
            error.contains("cannot unwrap column 'payload.choice'"),
            "{error}"
        );
        assert!(error.contains("Casting from Dictionary"), "{error}");
        assert!(error.contains("not supported"), "{error}");
    }

    /// Zero's canonical payload is a lone sign byte; on the Avro wire it must
    /// be the one-byte `0x00`, never an empty payload (Java's
    /// `new BigInteger(new byte[0])` throws). num-bigint's
    /// `to_signed_bytes_be` yields `[0]` for zero, so the encoder already
    /// does — pinned here.
    #[test]
    fn decimal_arb_zero_encodes_as_one_avro_byte() {
        use crate::types::decimal_arb::DecimalArbArrayBuilder;
        let mut b = DecimalArbArrayBuilder::with_capacity(3, "v", 10, 2).unwrap();
        for s in ["0", "0.00", "-0.00"] {
            b.append_str(s).unwrap();
        }
        let (raw, _, _) = b.finish().into_inner();
        for i in 0..3 {
            assert_eq!(
                decimal_arb_canonical_to_avro_bytes(raw.value(i)),
                vec![0x00],
                "row {i}"
            );
        }
        let mut b = DecimalArbArrayBuilder::with_capacity(2, "v", 10, 0).unwrap();
        b.append_str("1").unwrap();
        b.append_str("-1").unwrap();
        let (raw, _, _) = b.finish().into_inner();
        assert_eq!(
            decimal_arb_canonical_to_avro_bytes(raw.value(0)),
            vec![0x01]
        );
        assert_eq!(
            decimal_arb_canonical_to_avro_bytes(raw.value(1)),
            vec![0xff]
        );
    }

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

        let avro = try_to_avro("R", &arrow_schema.fields).unwrap();
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

        let avro_schema = try_to_avro("User", &arrow_schema.fields).unwrap();

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

        let result: Vec<apache_avro::types::Value> = serialize(&avro_schema, &batch).unwrap();

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

        let avro_schema = try_to_avro("Test", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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

        let avro_schema = try_to_avro("Test", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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

        let avro_schema = try_to_avro("Test", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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

        let avro_schema = try_to_avro("StellarLedger", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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

        let avro_schema = try_to_avro("Outer", &arrow_schema.fields).unwrap();
        println!("Avro schema: {}", avro_schema.canonical_form());

        let batch = RecordBatch::try_new(arrow_schema, vec![outer_array]).unwrap();

        let result: Vec<apache_avro::types::Value> = serialize(&avro_schema, &batch).unwrap();

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
        let avro_schema = try_to_avro("U256Record", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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
        let avro_schema = try_to_avro("MaybeHash", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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
        let avro_schema = try_to_avro("Payload", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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
        let avro_schema = try_to_avro("LogData", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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
        let avro_schema = try_to_avro("TxInput", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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

    // ------- decimal_arb Avro schema generation -------

    #[test]
    fn decimal_arb_field_emits_avro_decimal_logical_type() {
        // decimal_arb(100, 18) emits a decimal logical type with its declared scale.
        let field =
            crate::types::decimal_arb::DecimalArbType::field("amount", 100, 18, false).unwrap();
        let schema = try_to_avro("payload", &vec![field].into()).unwrap();
        let json = serde_json::to_value(schema).unwrap();
        let type_field = &json["fields"][0]["type"];
        assert_eq!(type_field.get("type").unwrap(), "bytes");
        assert_eq!(type_field.get("logicalType").unwrap(), "decimal");
        assert_eq!(type_field.get("precision").unwrap(), 100);
        assert_eq!(type_field.get("scale").unwrap(), 18);
    }

    #[test]
    fn decimal_arb_nullable_field_wraps_in_union() {
        // Nullable Avro fields wrap the decimal schema in a union with "null".
        let field =
            crate::types::decimal_arb::DecimalArbType::field("amount", 80, 30, true).unwrap();
        let schema = try_to_avro("payload", &vec![field].into()).unwrap();
        let json = serde_json::to_value(schema).unwrap();
        let arr = json["fields"][0]["type"]
            .as_array()
            .expect("nullable type is union array");
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], "null");
        assert_eq!(arr[1].get("logicalType").unwrap(), "decimal");
        assert_eq!(arr[1].get("precision").unwrap(), 80);
        assert_eq!(arr[1].get("scale").unwrap(), 30);
    }

    #[test]
    fn plain_large_binary_field_still_maps_to_bytes() {
        // LargeBinary without decimal_arb metadata stays plain Avro bytes.
        let field = Field::new("blob", DataType::LargeBinary, false);
        let schema = try_to_avro("payload", &vec![field].into()).unwrap();
        let json = serde_json::to_value(schema).unwrap();
        assert_eq!(json["fields"][0]["type"], "bytes");
    }

    // ------- decimal_arb Avro value serialization -------

    use std::str::FromStr;

    /// Decode an Avro `Value::Decimal` payload back into a canonical
    /// decimal_arb byte payload at `scale`. Mirrors
    /// `arrow_array_reader::resolve_decimal_arb_canonical_bytes` so these
    /// writer tests stay self-contained while still exercising the
    /// symmetric encoder/decoder contract.
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

        let avro_schema = try_to_avro("Test", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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

        let avro_schema = try_to_avro("Test", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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

        let avro_schema = try_to_avro("Test", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

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

        let avro_schema = try_to_avro("Test", &arrow_schema.fields).unwrap();
        let result = serialize(&avro_schema, &batch).unwrap();

        let Value::Record(fields) = &result[0] else {
            panic!("expected Record");
        };
        let canonical = avro_decimal_value_to_canonical_bytes(&fields[0].1, 0);
        let expected = crate::types::decimal_arb::DecimalArbValue::from_str(&big)
            .unwrap()
            .to_canonical_bytes_at_scale(0);
        assert_eq!(canonical, expected);
    }

    /// 256-bit integer leaves nested in `List<Struct<..>>` / `List<..>` (the
    /// plugin call-trace shape) are encoded as Avro decimals carrying their
    /// exact values, and the datum validates against the generated schema.
    #[test]
    fn nested_wide_int_leaves_encode_as_their_values() {
        use crate::types::decimal_arb_nested::fixtures::{
            I256_MAX, I256_MIN, U256_MAX, wide_int_traces_batch,
        };

        let (_, seen) = round_trip_decimals(&wide_int_traces_batch());
        let expected: Vec<Option<String>> = [
            Some("1"),
            Some("1000000000000000000"),
            Some("-1"),
            Some("0"),
            Some(U256_MAX),
            None,
            Some(I256_MIN),
            Some(I256_MAX),
        ]
        .iter()
        .map(|v| v.map(str::to_string))
        .collect();
        assert_eq!(seen, expected);
    }

    /// Every decimal (or null) below `v`, in document order; map entries in
    /// key order.
    fn collect_decimals(v: &Value, out: &mut Vec<Option<String>>) {
        match v {
            Value::Decimal(d) => {
                let bytes = <Vec<u8>>::try_from(d).expect("decimal -> bytes");
                out.push(Some(BigInt::from_signed_bytes_be(&bytes).to_string()));
            }
            Value::Null => out.push(None),
            Value::Union(_, inner) => collect_decimals(inner, out),
            Value::Array(items) => items.iter().for_each(|i| collect_decimals(i, out)),
            Value::Record(fields) => fields.iter().for_each(|(_, f)| collect_decimals(f, out)),
            Value::Map(entries) => {
                let mut keys: Vec<_> = entries.keys().collect();
                keys.sort();
                keys.into_iter()
                    .for_each(|k| collect_decimals(&entries[k], out));
            }
            _ => {}
        }
    }

    /// Encode every row of `batch` with the generated schema, decode it back,
    /// and collect the decimals it holds.
    fn round_trip_decimals(batch: &RecordBatch) -> (apache_avro::Schema, Vec<Option<String>>) {
        let avro_schema = try_to_avro("R", &batch.schema().fields).unwrap();
        let mut seen = Vec::new();
        for value in serialize(&avro_schema, batch).unwrap() {
            let datum = apache_avro::to_avro_datum(&avro_schema, value).expect("valid datum");
            let decoded =
                apache_avro::from_avro_datum(&avro_schema, &mut datum.as_slice(), None).unwrap();
            collect_decimals(&decoded, &mut seen);
        }
        (avro_schema, seen)
    }

    fn hinted_leaf(name: &str, kind: crate::types::decimal_arb::NativeIntKind) -> Arc<Field> {
        use crate::types::decimal_arb::DecimalArbType;
        Arc::new(
            DecimalArbType::with_native_int_kind(
                DecimalArbType::field(name, 78, 0, true).unwrap(),
                kind,
            )
            .unwrap(),
        )
    }

    fn arb_leaves(values: &[Option<&str>]) -> ArrayRef {
        use crate::types::decimal_arb::DecimalArbArrayBuilder;
        let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), "v", 78, 0).unwrap();
        for v in values {
            match v {
                Some(s) => b.append_str(s).unwrap(),
                None => b.append_null(),
            }
        }
        let (raw, _, _) = b.finish().into_inner();
        Arc::new(raw)
    }

    /// `List<nullable Struct<value: leaf>>` with the given leaves, one list
    /// per row (`offsets`), and a null struct where `struct_valid` says so.
    fn list_of_structs(
        leaf: Arc<Field>,
        leaves: ArrayRef,
        struct_valid: Vec<bool>,
        offsets: Vec<i32>,
    ) -> (Arc<Field>, ArrayRef) {
        use datafusion::arrow::array::{ListArray, StructArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
        let item = Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::clone(&leaf)].into()),
            true,
        ));
        let structs = StructArray::try_new(
            vec![leaf].into(),
            vec![leaves],
            Some(NullBuffer::from(struct_valid)),
        )
        .unwrap();
        let list = ListArray::try_new(
            Arc::clone(&item),
            OffsetBuffer::new(offsets.into()),
            Arc::new(structs),
            None,
        )
        .unwrap();
        (item, Arc::new(list))
    }

    /// Nested lists of structs get distinct record names and preserve exact leaves.
    #[test]
    fn several_lists_of_structs_get_distinct_record_names_and_round_trip() {
        use crate::types::decimal_arb::NativeIntKind;
        use crate::types::decimal_arb_nested::fixtures::{I256_MAX, I256_MIN, U256_MAX};
        use datafusion::arrow::array::{ListArray, StructArray};
        use datafusion::arrow::buffer::OffsetBuffer;

        let (trace_item, traces) = list_of_structs(
            hinted_leaf("value", NativeIntKind::U256),
            arb_leaves(&[
                Some("1"),
                Some("1000000000000000000"),
                Some("7"),
                Some(U256_MAX),
            ]),
            vec![true, true, false, true],
            vec![0, 2, 4],
        );
        let (tx_item, transactions) = list_of_structs(
            hinted_leaf("value", NativeIntKind::I256),
            arb_leaves(&[Some(I256_MIN), Some(I256_MAX)]),
            vec![true, true],
            vec![0, 1, 2],
        );
        // blocks: List<Struct<inner: List<Struct<value>>>>, one block per row.
        let (inner_item, inner) = list_of_structs(
            hinted_leaf("value", NativeIntKind::U256),
            arb_leaves(&[Some("42"), None]),
            vec![true, true],
            vec![0, 1, 2],
        );
        let inner_field = Arc::new(Field::new("inner", DataType::List(inner_item), true));
        let block = Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::clone(&inner_field)].into()),
            true,
        ));
        let blocks = ListArray::try_new(
            Arc::clone(&block),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(StructArray::try_new(vec![inner_field].into(), vec![inner], None).unwrap()),
            None,
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("traces", DataType::List(trace_item), false),
            Field::new("transactions", DataType::List(tx_item), true),
            Field::new("blocks", DataType::List(block), false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![traces, transactions, Arc::new(blocks)],
        )
        .unwrap();

        let (avro_schema, seen) = round_trip_decimals(&batch);
        let json = serde_json::to_string(&avro_schema).unwrap();
        // The first list keeps the name it always had; the others are named
        // after their own path.
        for name in [
            "\"item_item\"",
            "\"R_transactions_item\"",
            "\"R_blocks_item\"",
            "\"R_blocks_item_inner_item\"",
        ] {
            assert!(json.contains(name), "{name} missing from {json}");
        }
        let expected: Vec<Option<String>> = [
            // row 0
            Some("1"),
            Some("1000000000000000000"),
            Some(I256_MIN),
            Some("42"),
            // row 1: the null struct, then 2^256 - 1
            None,
            Some(U256_MAX),
            Some(I256_MAX),
            None,
        ]
        .iter()
        .map(|v| v.map(str::to_string))
        .collect();
        assert_eq!(seen, expected);
    }

    /// A schema with a single list of structs is unchanged, so a subject
    /// registered from it before still matches what the sink writes.
    #[test]
    fn single_list_of_structs_keeps_its_record_name() {
        let a = Arc::new(Field::new("a", DataType::Int64, true));
        let item = Arc::new(Field::new("item", DataType::Struct(vec![a].into()), true));
        let fields: Fields = vec![Field::new("items", DataType::List(item), false)].into();
        let json = serde_json::to_value(try_to_avro("R", &fields).unwrap()).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "type": "record",
                "name": "R",
                "fields": [{
                    "name": "items",
                    "type": {"type": "array", "items": ["null", {
                        "type": "record",
                        "name": "item_item",
                        "fields": [{"name": "a", "type": ["null", "long"]}],
                    }]},
                }],
            })
        );
    }

    #[test]
    fn record_names_preserve_distinct_inherited_namespaces() {
        use apache_avro::schema_compatibility::SchemaCompatibility;

        let item = Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Field::new("a", DataType::Int64, false)].into()),
            false,
        ));
        let fields: Fields = ["a.x", "b.x"]
            .into_iter()
            .map(|name| {
                Field::new(
                    name,
                    DataType::Struct(
                        vec![Field::new("l", DataType::List(Arc::clone(&item)), false)].into(),
                    ),
                    false,
                )
            })
            .collect();
        // These two `item_item` definitions have different fullnames. This
        // is the schema existing sinks registered before de-duplication.
        let old = apache_avro::Schema::parse_str(
            &json!({
                "type": "record", "name": "R", "fields": [
                    {"name": "a__x", "type": {
                        "type": "record", "name": "R_a.x", "fields": [
                            {"name": "l", "type": {"type": "array", "items": {
                                "type": "record", "name": "item_item", "fields": [
                                    {"name": "a", "type": "long"}
                                ]
                            }}}
                        ]
                    }},
                    {"name": "b__x", "type": {
                        "type": "record", "name": "R_b.x", "fields": [
                            {"name": "l", "type": {"type": "array", "items": {
                                "type": "record", "name": "item_item", "fields": [
                                    {"name": "a", "type": "long"}
                                ]
                            }}}
                        ]
                    }}
                ]
            })
            .to_string(),
        )
        .unwrap();
        let new = try_to_avro("R", &fields).unwrap();
        SchemaCompatibility::can_read(&old, &new).expect("existing schemas stay compatible");
        assert_eq!(
            serde_json::to_value(new).unwrap(),
            serde_json::to_value(old).unwrap()
        );
    }

    #[test]
    fn record_names_disambiguate_inherited_and_explicit_fullnames() {
        use datafusion::arrow::array::{Int32Array, ListArray, StructArray};
        use datafusion::arrow::buffer::OffsetBuffer;

        let v = Arc::new(Field::new("v", DataType::Int32, false));
        let item = Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::clone(&v)].into()),
            false,
        ));
        let xs = ListArray::try_new(
            item,
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(
                StructArray::try_new(
                    vec![v].into(),
                    vec![Arc::new(Int32Array::from(vec![1]))],
                    None,
                )
                .unwrap(),
            ),
            None,
        )
        .unwrap();
        let other = StructArray::try_new(
            vec![Field::new("w", DataType::Int32, false)].into(),
            vec![Arc::new(Int32Array::from(vec![2]))],
            None,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("xs", xs.data_type().clone(), false),
            Field::new("item", other.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(xs), Arc::new(other)]).unwrap();
        // The list's unqualified `item_item` inherits namespace `a`, so it
        // clashes with the second struct's explicit name `a.item_item`.
        let avro_schema = try_to_avro("a.item", &batch.schema().fields).unwrap();
        let value = serialize(&avro_schema, &batch).unwrap().pop().unwrap();
        let encoded = apache_avro::to_avro_datum(&avro_schema, value.clone())
            .expect("record fullnames must be unique");
        let decoded =
            apache_avro::from_avro_datum(&avro_schema, &mut encoded.as_slice(), None).unwrap();
        assert_eq!(decoded, value);
    }

    /// Large / fixed-size lists, string-keyed maps, and run-end- or
    /// dictionary-encoded columns all have an Avro encoding; each used to
    /// panic, either building the schema or writing the first batch.
    #[test]
    fn other_container_layouts_encode_nested_leaves() {
        use crate::types::decimal_arb::NativeIntKind;
        use crate::types::decimal_arb_nested::fixtures::U256_MAX;
        use datafusion::arrow::array::{
            DictionaryArray, FixedSizeListArray, Int32Array, LargeListArray, MapArray, RunArray,
            StringArray, StructArray, make_array,
        };
        use datafusion::arrow::buffer::OffsetBuffer;
        use datafusion::arrow::datatypes::Int32Type;

        let leaf = || hinted_leaf("item", NativeIntKind::U256);
        let large = LargeListArray::try_new(
            leaf(),
            OffsetBuffer::new(vec![0_i64, 1, 2].into()),
            arb_leaves(&[Some("1"), Some(U256_MAX)]),
            None,
        )
        .unwrap();
        let fixed =
            FixedSizeListArray::try_new(leaf(), 1, arb_leaves(&[Some("2"), None]), None).unwrap();

        let key = Arc::new(Field::new("key", DataType::Utf8, false));
        let value = hinted_leaf("value", NativeIntKind::U256);
        let entry = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![Arc::clone(&key), Arc::clone(&value)].into()),
            false,
        ));
        let map = MapArray::try_new(
            Arc::clone(&entry),
            OffsetBuffer::new(vec![0, 2, 2].into()),
            StructArray::try_new(
                vec![key, value].into(),
                vec![
                    Arc::new(StringArray::from(vec!["b", "a"])),
                    arb_leaves(&[Some("1000000000000000000"), Some("3")]),
                ],
                None,
            )
            .unwrap(),
            None,
            false,
        )
        .unwrap();

        let ree = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![2]),
            arb_leaves(&[Some("5")]).as_ref(),
        )
        .unwrap();
        let DataType::RunEndEncoded(run_ends, _) = ree.data_type().clone() else {
            unreachable!()
        };
        let ree_type =
            DataType::RunEndEncoded(run_ends, hinted_leaf("values", NativeIntKind::U256));
        let ree = make_array(
            ree.to_data()
                .into_builder()
                .data_type(ree_type.clone())
                .build()
                .unwrap(),
        );
        let dict: DictionaryArray<Int32Type> = vec!["x", "y"].into_iter().collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("large", large.data_type().clone(), false),
            Field::new("fixed", fixed.data_type().clone(), false),
            Field::new("map", DataType::Map(entry, false), true),
            Field::new("ree", ree_type, true),
            Field::new("dict", dict.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(large),
                Arc::new(fixed),
                Arc::new(map),
                ree,
                Arc::new(dict),
            ],
        )
        .unwrap();

        let (avro_schema, seen) = round_trip_decimals(&batch);
        let expected: Vec<Option<String>> = [
            // row 0: large, fixed, map {a: 3, b: 10^18}, ree
            Some("1"),
            Some("2"),
            Some("3"),
            Some("1000000000000000000"),
            Some("5"),
            // row 1: large, fixed (null leaf), empty map, ree
            Some(U256_MAX),
            None,
            Some("5"),
        ]
        .iter()
        .map(|v| v.map(str::to_string))
        .collect();
        assert_eq!(seen, expected);
        // The dictionary column is written as its string values, nullable
        // since a dictionary's values may hold nulls its keys do not show.
        let rows = serialize(&avro_schema, &batch).unwrap();
        let Value::Record(fields) = &rows[1] else {
            panic!("{:?}", rows[1])
        };
        assert_eq!(
            fields[4],
            (
                "dict".to_string(),
                Value::Union(1, Box::new(Value::String("y".into())))
            )
        );
    }

    /// Unsupported columns are reported by their user-facing path, including
    /// nested list, map and struct leaves.
    #[test]
    fn columns_without_an_avro_encoding_are_reported() {
        use datafusion::arrow::datatypes::{UnionFields, UnionMode};
        let bad_entry = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        ));
        let durations = Field::new(
            "durations",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Duration(TimeUnit::Millisecond),
                true,
            ))),
            true,
        );
        let entry = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Time64(TimeUnit::Microsecond), true),
                ]
                .into(),
            ),
            false,
        ));
        let by_id = Field::new("by_id", DataType::Map(entry, false), true);
        let s = Field::new(
            "s",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new(
                        "inner",
                        DataType::Struct(
                            vec![Field::new("when", DataType::Time32(TimeUnit::Second), true)]
                                .into(),
                        ),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        );
        for (field, path) in [
            (
                Field::new("m", DataType::Map(bad_entry, false), true),
                "'m'",
            ),
            (
                Field::new(
                    "u",
                    DataType::Union(
                        UnionFields::try_new(vec![0], vec![Field::new("a", DataType::Int32, true)])
                            .unwrap(),
                        UnionMode::Dense,
                    ),
                    true,
                ),
                "'u'",
            ),
            (
                Field::new("t", DataType::Time64(TimeUnit::Microsecond), true),
                "'t'",
            ),
            (durations, "'durations[]'"),
            (by_id, "'by_id{}'"),
            (s, "'s.inner.when'"),
        ] {
            let err = try_to_avro("R", &vec![field].into())
                .unwrap_err()
                .to_string();
            assert!(err.contains(path), "{err}");
            assert!(!err.contains("'item'") && !err.contains("'value'"), "{err}");
        }
    }

    /// Arrow's `MapArray` does not check its keys for nulls, so a map whose
    /// key field is declared nullable can carry a null key. The row encoder
    /// has no Avro value for one; the schema is refused before any row is
    /// written instead of the sink panicking on the first such row.
    #[test]
    fn map_with_a_nullable_key_field_is_refused_before_any_row_is_written() {
        use datafusion::arrow::array::{Int32Array, MapArray, StringArray, StructArray};
        use datafusion::arrow::buffer::OffsetBuffer;

        let kv: Fields = vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]
        .into();
        let entry = Arc::new(Field::new("entries", DataType::Struct(kv.clone()), false));
        // Arrow builds the map with a nullable key field, null key and all.
        let map = MapArray::try_new(
            Arc::clone(&entry),
            OffsetBuffer::new(vec![0, 2].into()),
            StructArray::try_new(
                kv,
                vec![
                    Arc::new(StringArray::from(vec![Some("a"), None])),
                    Arc::new(Int32Array::from(vec![1, 2])),
                ],
                None,
            )
            .unwrap(),
            None,
            false,
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(entry, false),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(map)]).unwrap();
        let err = try_to_avro("R", &batch.schema().fields)
            .unwrap_err()
            .to_string();
        assert!(err.contains("'m'"), "{err}");
        assert!(err.contains("key field is nullable"), "{err}");
    }

    /// Record names come from the topic and from Arrow field names, which
    /// Avro's name grammar does not constrain; apache-avro panicked while
    /// parsing a schema whose record name broke it. Such names are made
    /// valid, and conforming ones (dotted topics included) stay as they are.
    #[test]
    fn record_names_are_made_valid_avro_names() {
        use datafusion::arrow::array::{Int64Array, StructArray};

        assert_eq!(record_name("eth.mainnet.blocks"), "eth.mainnet.blocks");
        assert_eq!(record_name("R_my_field"), "R_my_field");
        assert_eq!(record_name("evm-blocks"), "evm_blocks");
        assert_eq!(record_name("R_my-field"), "R_my_field");
        assert_eq!(record_name("1inch.events"), "_1inch.events");
        assert_eq!(record_name("a..b"), "a._.b");

        let a = Arc::new(Field::new("a", DataType::Int64, true));
        let s = Arc::new(Field::new(
            "my-struct",
            DataType::Struct(vec![Arc::clone(&a)].into()),
            true,
        ));
        let schema = Arc::new(Schema::new(vec![Arc::clone(&s)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(
                StructArray::try_new(
                    vec![a].into(),
                    vec![Arc::new(Int64Array::from(vec![Some(7)]))],
                    None,
                )
                .unwrap(),
            )],
        )
        .unwrap();

        let avro_schema = try_to_avro("evm-blocks.v2", &schema.fields).unwrap();
        let json = serde_json::to_string(&avro_schema).unwrap();
        assert!(json.contains("evm_blocks"), "{json}");
        assert!(json.contains("v2_my_struct"), "{json}");
        assert!(!json.contains("my-struct"), "{json}");
        for value in serialize(&avro_schema, &batch).unwrap() {
            let datum = apache_avro::to_avro_datum(&avro_schema, value).expect("valid datum");
            let decoded =
                apache_avro::from_avro_datum(&avro_schema, &mut datum.as_slice(), None).unwrap();
            assert_eq!(
                decoded,
                Value::Record(vec![(
                    "my_struct".to_string(),
                    Value::Union(
                        1,
                        Box::new(Value::Record(vec![(
                            "a".to_string(),
                            Value::Union(1, Box::new(Value::Long(7))),
                        )])),
                    ),
                )])
            );
        }
    }

    /// Time-zone-aware timestamps are epoch offsets declared as Avro `timestamp-*`.
    #[test]
    fn timezone_aware_timestamps_encode_as_avro_instants() {
        use datafusion::arrow::array::{TimestampMicrosecondArray, TimestampMillisecondArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "at_us",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                false,
            ),
            Field::new(
                "at_ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1_700_000_000_000_000_i64])
                        .with_timezone("+00:00"),
                ),
                Arc::new(
                    TimestampMillisecondArray::from(vec![Some(1_700_000_000_000_i64)])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .unwrap();

        let avro_schema = try_to_avro("R", &schema.fields).unwrap();
        let json = serde_json::to_string(&avro_schema).unwrap();
        assert!(
            json.contains(r#""logicalType":"timestamp-micros""#),
            "{json}"
        );
        assert!(
            json.contains(r#""logicalType":"timestamp-millis""#),
            "{json}"
        );
        assert!(!json.contains("local-timestamp"), "{json}");
        for value in serialize(&avro_schema, &batch).unwrap() {
            let datum = apache_avro::to_avro_datum(&avro_schema, value).expect("valid datum");
            let decoded =
                apache_avro::from_avro_datum(&avro_schema, &mut datum.as_slice(), None).unwrap();
            assert_eq!(
                decoded,
                Value::Record(vec![
                    (
                        "at_us".to_string(),
                        Value::TimestampMicros(1_700_000_000_000_000),
                    ),
                    (
                        "at_ms".to_string(),
                        Value::Union(1, Box::new(Value::TimestampMillis(1_700_000_000_000))),
                    ),
                ])
            );
        }
    }

    /// A run-end-encoded column reports no nulls of its own: they sit in its
    /// values. Declared non-nullable, it is still written as a nullable
    /// column of its plain layout, so a null value encodes instead of
    /// panicking.
    #[test]
    fn run_end_encoded_column_with_null_values_encodes_as_nullable() {
        use datafusion::arrow::array::{Int32Array, RunArray};
        use datafusion::arrow::datatypes::Int32Type;

        let ree = RunArray::<Int32Type>::try_new(
            &Int32Array::from(vec![1, 3]),
            &Int32Array::from(vec![Some(4), None]),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "r",
            ree.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ree)]).unwrap();
        let avro_schema = try_to_avro("R", &schema.fields).unwrap();
        let json = serde_json::to_string(&avro_schema).unwrap();
        assert!(json.contains(r#"["null","int"]"#), "{json}");
        let rows = serialize(&avro_schema, &batch).unwrap();
        let r = |i: usize| match &rows[i] {
            Value::Record(fields) => fields[0].1.clone(),
            other => panic!("{other:?}"),
        };
        assert_eq!(r(0), Value::Union(1, Box::new(Value::Int(4))));
        assert_eq!(r(1), Value::Union(0, Box::new(Value::Null)));
        assert_eq!(r(2), Value::Union(0, Box::new(Value::Null)));
        for value in rows {
            apache_avro::to_avro_datum(&avro_schema, value).expect("valid datum");
        }
    }

    /// Arrow counts only key nulls toward a dictionary's validity, so a
    /// non-nullable dictionary column may still point at a null value. It is
    /// declared and written nullable, so that value reads back as Avro null.
    #[test]
    fn dictionary_column_with_null_values_encodes_as_nullable() {
        use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType};
        use datafusion::arrow::array::{DictionaryArray, Int32Array, StringArray};
        use datafusion::arrow::datatypes::Int32Type;

        let strings = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0]),
            Arc::new(StringArray::from(vec![None::<&str>])),
        )
        .unwrap();
        let mut builder = DecimalArbArrayBuilder::with_capacity(1, "d", 78, 0).unwrap();
        builder.append_null();
        let (raw, _, _) = builder.finish().into_inner();
        let decimals =
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), Arc::new(raw))
                .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", strings.data_type().clone(), false),
            Field::new("d", decimals.data_type().clone(), false).with_metadata(
                DecimalArbType::field("d", 78, 0, false)
                    .unwrap()
                    .metadata()
                    .clone(),
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(strings), Arc::new(decimals)],
        )
        .unwrap();
        let avro_schema = try_to_avro("R", &schema.fields).unwrap();
        for value in serialize(&avro_schema, &batch).unwrap() {
            let datum = apache_avro::to_avro_datum(&avro_schema, value).expect("valid datum");
            let decoded =
                apache_avro::from_avro_datum(&avro_schema, &mut datum.as_slice(), None).unwrap();
            let null = Value::Union(0, Box::new(Value::Null));
            assert_eq!(
                decoded,
                Value::Record(vec![("s".into(), null.clone()), ("d".into(), null)])
            );
        }
    }

    /// Arrow lets a non-nullable child of a nullable struct hold a null in a
    /// slot the struct's own validity masks. The list, map and struct
    /// encoders used to read that slot (and panic on the null); like the
    /// primitive encoder they now skip every slot under a null record.
    #[test]
    fn children_masked_by_a_null_struct_are_skipped() {
        use datafusion::arrow::array::{Int64Array, ListArray, MapArray, StringArray, StructArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};

        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let tags = ListArray::try_new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0, 0, 1].into()),
            Arc::new(StringArray::from(vec!["x"])),
            Some(NullBuffer::from(vec![false, true])),
        )
        .unwrap();
        let entry = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ]
                .into(),
            ),
            false,
        ));
        let DataType::Struct(kv) = entry.data_type().clone() else {
            unreachable!()
        };
        let by_name = MapArray::try_new(
            Arc::clone(&entry),
            OffsetBuffer::new(vec![0, 0, 1].into()),
            StructArray::try_new(
                kv,
                vec![
                    Arc::new(StringArray::from(vec!["k"])),
                    Arc::new(Int64Array::from(vec![1])),
                ],
                None,
            )
            .unwrap(),
            Some(NullBuffer::from(vec![false, true])),
            false,
        )
        .unwrap();
        // A non-nullable struct child whose own non-nullable child is null
        // under the outer struct's null row.
        let x = Arc::new(Field::new("x", DataType::Int64, false));
        let inner = StructArray::try_new(
            vec![Arc::clone(&x)].into(),
            vec![Arc::new(Int64Array::from(vec![None, Some(3)]))],
            Some(NullBuffer::from(vec![false, true])),
        )
        .unwrap();
        let children: Fields = vec![
            Field::new("tags", tags.data_type().clone(), false),
            Field::new("by_name", by_name.data_type().clone(), false),
            Field::new("inner", inner.data_type().clone(), false),
            Field::new("n", DataType::Int64, false),
        ]
        .into();
        let s = StructArray::try_new(
            children,
            vec![
                Arc::new(tags),
                Arc::new(by_name),
                Arc::new(inner),
                Arc::new(Int64Array::from(vec![None, Some(7)])),
            ],
            Some(NullBuffer::from(vec![false, true])),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            s.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(s)]).unwrap();

        let avro_schema = try_to_avro("R", &schema.fields).unwrap();
        let rows = serialize(&avro_schema, &batch).unwrap();
        assert_eq!(
            rows[0],
            Value::Record(vec![(
                "s".to_string(),
                Value::Union(0, Box::new(Value::Null))
            )])
        );
        let Value::Record(fields) = &rows[1] else {
            panic!("{:?}", rows[1])
        };
        let Value::Union(1, record) = &fields[0].1 else {
            panic!("{:?}", fields[0])
        };
        let Value::Record(children) = record.as_ref() else {
            panic!("{record:?}")
        };
        assert_eq!(
            children[0],
            (
                "tags".to_string(),
                Value::Array(vec![Value::Union(1, Box::new(Value::String("x".into())))]),
            )
        );
        assert_eq!(
            children[1],
            (
                "by_name".to_string(),
                Value::Map([("k".to_string(), Value::Union(1, Box::new(Value::Long(1))))].into()),
            )
        );
        assert_eq!(
            children[2],
            (
                "inner".to_string(),
                Value::Record(vec![("x".to_string(), Value::Long(3))]),
            )
        );
        assert_eq!(children[3], ("n".to_string(), Value::Long(7)));
        for value in rows {
            apache_avro::to_avro_datum(&avro_schema, value).expect("valid datum");
        }
    }
}
