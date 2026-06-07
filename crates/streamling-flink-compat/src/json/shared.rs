use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};

#[derive(Debug, Clone, Copy)]
pub(crate) enum JsonConstructorNullBehavior {
    NullOnNull,
    AbsentOnNull,
}

impl JsonConstructorNullBehavior {
    pub(crate) fn include_nulls(self) -> bool {
        matches!(self, Self::NullOnNull)
    }
}

pub(crate) fn json_quote(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len() + 2);
    escaped.push('"');

    for ch in input.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '/' => escaped.push_str("\\/"),
            '\u{0008}' => escaped.push_str("\\b"),
            '\u{000C}' => escaped.push_str("\\f"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ if ch.is_ascii() => escaped.push(ch),
            _ => escaped.push_str(format!("\\u{:04x}", ch as u32).as_str()),
        }
    }

    escaped.push('"');
    escaped
}

pub(crate) fn canonicalize_json_scalar(
    value: &ScalarValue,
    allow_duplicates: bool,
) -> DataFusionResult<ScalarValue> {
    match value {
        ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None) => Ok(ScalarValue::Utf8(None)),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => match canonicalize_json_text(v, allow_duplicates)? {
            Some(text) => Ok(ScalarValue::Utf8(Some(text))),
            None => Ok(ScalarValue::Utf8(None)),
        },
        other => Err(DataFusionError::Execution(format!(
            "JSON-based function expects Utf8 input, received scalar of type {other:?}"
        ))),
    }
}

pub(crate) fn canonicalize_json_text(
    text: &str,
    _allow_duplicates: bool,
) -> DataFusionResult<Option<String>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    serde_json::from_str::<serde_json::Value>(trimmed)
        .map(|value| Some(value.to_string()))
        .map_err(|err| {
            DataFusionError::Execution(format!(
                "Invalid JSON string in JSON function: `{text}` ({err})"
            ))
        })
}

pub(crate) fn parse_json_structure_literal(text: &str) -> Option<JsonValue> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        serde_json::from_str::<serde_json::Value>(trimmed).ok()
    } else {
        None
    }
}

pub(crate) fn build_json_array_row(
    values: &[ScalarValue],
    null_behavior: JsonConstructorNullBehavior,
) -> DataFusionResult<String> {
    let mut json_values = Vec::with_capacity(values.len());
    for value in values {
        if value.is_null() {
            if null_behavior.include_nulls() {
                json_values.push(JsonValue::Null);
            }
            continue;
        }
        json_values.push(scalar_to_json_value(value)?);
    }
    Ok(JsonValue::Array(json_values).to_string())
}

pub(crate) fn build_json_object_row(
    keys: &[ScalarValue],
    values: &[ScalarValue],
    null_behavior: JsonConstructorNullBehavior,
) -> DataFusionResult<String> {
    if keys.len() != values.len() {
        return Err(DataFusionError::Execution(
            "JSON_OBJECT expects matching key/value pairs".to_string(),
        ));
    }

    let mut object = JsonMap::new();
    for (key_scalar, value_scalar) in keys.iter().zip(values.iter()) {
        let key = scalar_to_json_key(key_scalar)?;
        if value_scalar.is_null() {
            if null_behavior.include_nulls() {
                object.insert(key, JsonValue::Null);
            }
            continue;
        }
        object.insert(key, scalar_to_json_value(value_scalar)?);
    }

    Ok(JsonValue::Object(object).to_string())
}

pub(crate) fn scalar_to_json_key(value: &ScalarValue) -> DataFusionResult<String> {
    use ScalarValue::*;

    const KEY_NULL_ERR: &str = "JSON_OBJECT keys must not be NULL";
    const KEY_FINITE_ERR: &str = "JSON_OBJECT keys must be finite numbers";

    match value {
        Null => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        Boolean(Some(v)) => Ok(v.to_string()),
        Boolean(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        Int8(Some(v)) => Ok(v.to_string()),
        Int8(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        Int16(Some(v)) => Ok(v.to_string()),
        Int16(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        Int32(Some(v)) => Ok(v.to_string()),
        Int32(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        Int64(Some(v)) => Ok(v.to_string()),
        Int64(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        UInt8(Some(v)) => Ok(v.to_string()),
        UInt8(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        UInt16(Some(v)) => Ok(v.to_string()),
        UInt16(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        UInt32(Some(v)) => Ok(v.to_string()),
        UInt32(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        UInt64(Some(v)) => Ok(v.to_string()),
        UInt64(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        Float32(Some(v)) => match float_to_json_value(*v as f64) {
            JsonValue::Number(n) => Ok(n.to_string()),
            _ => Err(DataFusionError::Execution(KEY_FINITE_ERR.to_string())),
        },
        Float32(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        Float64(Some(v)) => match float_to_json_value(*v) {
            JsonValue::Number(n) => Ok(n.to_string()),
            _ => Err(DataFusionError::Execution(KEY_FINITE_ERR.to_string())),
        },
        Float64(None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        Decimal128(Some(_), _, _) | Decimal256(Some(_), _, _) => Ok(value.to_string()),
        Decimal128(None, _, _) | Decimal256(None, _, _) => {
            Err(DataFusionError::Execution(KEY_NULL_ERR.to_string()))
        }
        Utf8(Some(v)) | Utf8View(Some(v)) | LargeUtf8(Some(v)) => Ok(v.clone()),
        Utf8(None) | Utf8View(None) | LargeUtf8(None) => {
            Err(DataFusionError::Execution(KEY_NULL_ERR.to_string()))
        }
        Dictionary(_, value) => scalar_to_json_key(value),
        Binary(Some(bytes)) | LargeBinary(Some(bytes)) | BinaryView(Some(bytes)) => {
            Ok(BASE64_STANDARD.encode(bytes))
        }
        Binary(None) | LargeBinary(None) | BinaryView(None) => {
            Err(DataFusionError::Execution(KEY_NULL_ERR.to_string()))
        }
        FixedSizeBinary(_, Some(bytes)) => Ok(BASE64_STANDARD.encode(bytes)),
        FixedSizeBinary(_, None) => Err(DataFusionError::Execution(KEY_NULL_ERR.to_string())),
        other => Ok(other.to_string()),
    }
}

pub(crate) fn scalar_to_json_value(value: &ScalarValue) -> DataFusionResult<JsonValue> {
    use ScalarValue::*;

    Ok(match value {
        Null => JsonValue::Null,
        Boolean(Some(v)) => JsonValue::Bool(*v),
        Boolean(None) => JsonValue::Null,
        Int8(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        Int8(None) => JsonValue::Null,
        Int16(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        Int16(None) => JsonValue::Null,
        Int32(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        Int32(None) => JsonValue::Null,
        Int64(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        Int64(None) => JsonValue::Null,
        UInt8(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        UInt8(None) => JsonValue::Null,
        UInt16(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        UInt16(None) => JsonValue::Null,
        UInt32(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        UInt32(None) => JsonValue::Null,
        UInt64(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        UInt64(None) => JsonValue::Null,
        Float16(Some(v)) => float_to_json_value(f32::from(*v) as f64),
        Float16(None) => JsonValue::Null,
        Float32(Some(v)) => float_to_json_value(*v as f64),
        Float32(None) => JsonValue::Null,
        Float64(Some(v)) => float_to_json_value(*v),
        Float64(None) => JsonValue::Null,
        Decimal128(Some(_), _, _) | Decimal256(Some(_), _, _) => {
            JsonValue::String(value.to_string())
        }
        Decimal128(None, _, _) | Decimal256(None, _, _) => JsonValue::Null,
        Utf8(Some(v)) | Utf8View(Some(v)) | LargeUtf8(Some(v)) => {
            if let Some(json) = parse_json_structure_literal(v) {
                json
            } else {
                JsonValue::String(v.clone())
            }
        }
        Utf8(None) | Utf8View(None) | LargeUtf8(None) => JsonValue::Null,
        Binary(Some(bytes)) | LargeBinary(Some(bytes)) | BinaryView(Some(bytes)) => {
            JsonValue::String(BASE64_STANDARD.encode(bytes))
        }
        Binary(None) | LargeBinary(None) | BinaryView(None) => JsonValue::Null,
        FixedSizeBinary(_, Some(bytes)) => JsonValue::String(BASE64_STANDARD.encode(bytes)),
        FixedSizeBinary(_, None) => JsonValue::Null,
        List(list) => list_array_to_json(list.as_ref())?,
        LargeList(list) => large_list_array_to_json(list.as_ref())?,
        FixedSizeList(list) => fixed_size_list_array_to_json(list.as_ref())?,
        Struct(struct_array) => struct_array_to_json(struct_array.as_ref())?,
        Map(map_array) => map_array_to_json(map_array.as_ref())?,
        Dictionary(_, value) => scalar_to_json_value(value)?,
        TimestampSecond(Some(v), tz)
        | TimestampMillisecond(Some(v), tz)
        | TimestampMicrosecond(Some(v), tz)
        | TimestampNanosecond(Some(v), tz) => JsonValue::String(match tz {
            Some(tz) => format!("{} @{}", v, tz),
            None => v.to_string(),
        }),
        TimestampSecond(None, _)
        | TimestampMillisecond(None, _)
        | TimestampMicrosecond(None, _)
        | TimestampNanosecond(None, _) => JsonValue::Null,
        Date32(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        Date32(None) => JsonValue::Null,
        Date64(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        Date64(None) => JsonValue::Null,
        DurationSecond(Some(v))
        | DurationMillisecond(Some(v))
        | DurationMicrosecond(Some(v))
        | DurationNanosecond(Some(v)) => JsonValue::Number(JsonNumber::from(*v)),
        DurationSecond(None)
        | DurationMillisecond(None)
        | DurationMicrosecond(None)
        | DurationNanosecond(None) => JsonValue::Null,
        Union(Some((_type, value)), _, _) => scalar_to_json_value(value)?,
        Union(None, _, _) => JsonValue::Null,
        other => JsonValue::String(other.to_string()),
    })
}

fn list_array_to_json(list: &ListArray) -> DataFusionResult<JsonValue> {
    if list.is_empty() {
        return Ok(JsonValue::Array(Vec::new()));
    }
    if list.is_null(0) {
        return Ok(JsonValue::Null);
    }
    let values = list.value(0);
    Ok(JsonValue::Array(array_to_json_elements(&values)?))
}

fn large_list_array_to_json(list: &LargeListArray) -> DataFusionResult<JsonValue> {
    if list.is_empty() {
        return Ok(JsonValue::Array(Vec::new()));
    }
    if list.is_null(0) {
        return Ok(JsonValue::Null);
    }
    let values = list.value(0);
    Ok(JsonValue::Array(array_to_json_elements(&values)?))
}

fn fixed_size_list_array_to_json(list: &FixedSizeListArray) -> DataFusionResult<JsonValue> {
    if list.is_null(0) {
        return Ok(JsonValue::Null);
    }
    let values = list.values();
    let value_length = list.value_length() as usize;
    let mut elements = Vec::with_capacity(value_length);
    for i in 0..value_length {
        let scalar = ScalarValue::try_from_array(values.as_ref(), i)?;
        elements.push(scalar_to_json_value(&scalar)?);
    }
    Ok(JsonValue::Array(elements))
}

fn struct_array_to_json(struct_array: &StructArray) -> DataFusionResult<JsonValue> {
    if struct_array.is_null(0) {
        return Ok(JsonValue::Null);
    }
    let mut object = JsonMap::new();
    for (idx, field) in struct_array.columns().iter().enumerate() {
        let field_name = struct_array.fields()[idx].name().clone();
        let scalar = ScalarValue::try_from_array(field.as_ref(), 0)?;
        object.insert(field_name, scalar_to_json_value(&scalar)?);
    }
    Ok(JsonValue::Object(object))
}

fn map_array_to_json(map_array: &MapArray) -> DataFusionResult<JsonValue> {
    if map_array.is_null(0) {
        return Ok(JsonValue::Null);
    }

    let values = map_array.value(0);
    let struct_array = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("Map entries not represented as struct".to_string())
        })?;

    let key_array = struct_array.column(0);
    let value_array = struct_array.column(1);
    let mut object = JsonMap::new();
    for i in 0..struct_array.len() {
        let key_scalar = ScalarValue::try_from_array(key_array.as_ref(), i)?;
        let key = scalar_to_json_key(&key_scalar)?;
        let value_scalar = ScalarValue::try_from_array(value_array.as_ref(), i)?;
        object.insert(key, scalar_to_json_value(&value_scalar)?);
    }
    Ok(JsonValue::Object(object))
}

fn array_to_json_elements(array: &ArrayRef) -> DataFusionResult<Vec<JsonValue>> {
    let mut elements = Vec::with_capacity(array.len());
    for i in 0..array.len() {
        let scalar = ScalarValue::try_from_array(array.as_ref(), i)?;
        elements.push(scalar_to_json_value(&scalar)?);
    }
    Ok(elements)
}

fn float_to_json_value(value: f64) -> JsonValue {
    if let Some(number) = JsonNumber::from_f64(value) {
        JsonValue::Number(number)
    } else {
        JsonValue::Null
    }
}

pub(crate) fn parse_utf8_literal(value: &ColumnarValue, context: &str) -> DataFusionResult<String> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => Ok(v.clone()),
        ColumnarValue::Scalar(ScalarValue::Utf8View(Some(v))) => Ok(v.to_string()),
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(None)) => Err(DataFusionError::Execution(
            format!("JSON_QUERY {context} expects a non-null string literal"),
        )),
        _ => Err(DataFusionError::Execution(format!(
            "JSON_QUERY {context} expects a string literal"
        ))),
    }
}

pub(crate) fn normalise_keyword(text: &str) -> String {
    text.split_whitespace()
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.to_ascii_uppercase())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Utility used by many JSON UDFs to normalise a set of columnar arguments into
/// aligned arrays and determine whether all inputs were scalar.
pub(crate) fn prepare_row_evaluation(
    args: &ScalarFunctionArgs,
) -> DataFusionResult<(bool, Vec<ArrayRef>, usize)> {
    let all_scalar = args
        .args
        .iter()
        .all(|value| matches!(value, ColumnarValue::Scalar(_)));
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays.first().map(|array| array.len()).unwrap_or(1);
    Ok((all_scalar, arrays, row_count))
}

/// Finalise a row-wise evaluation, collapsing back to a scalar when every input
/// row was scalar.
pub(crate) fn finalize_row_evaluation(
    all_scalar: bool,
    array: ArrayRef,
) -> DataFusionResult<ColumnarValue> {
    if all_scalar {
        let scalar = ScalarValue::try_from_array(array.as_ref(), 0)?;
        Ok(ColumnarValue::Scalar(scalar))
    } else {
        Ok(ColumnarValue::Array(array))
    }
}
