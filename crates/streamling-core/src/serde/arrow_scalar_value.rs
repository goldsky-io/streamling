use datafusion::common::ScalarValue;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
struct SerializableScalarValue {
    value: Value,
    data_type: String,
}

pub fn serialize<S>(values: &[ScalarValue], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let serializable_values: Vec<SerializableScalarValue> = values
        .iter()
        .map(|sv| SerializableScalarValue {
            value: scalar_to_json_value(sv),
            data_type: sv.data_type().to_string(),
        })
        .collect();

    serializable_values.serialize(serializer)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<ScalarValue>, D::Error>
where
    D: Deserializer<'de>,
{
    let serializable_values: Vec<SerializableScalarValue> = Vec::deserialize(deserializer)?;

    serializable_values
        .into_iter()
        .map(|sv| json_value_to_scalar(&sv.value, &sv.data_type).map_err(serde::de::Error::custom))
        .collect()
}

fn scalar_to_json_value(scalar: &ScalarValue) -> Value {
    match scalar {
        ScalarValue::Utf8(Some(s)) => Value::String(s.clone()),
        ScalarValue::Utf8(None) => Value::Null,
        ScalarValue::LargeUtf8(Some(s)) => Value::String(s.clone()),
        ScalarValue::LargeUtf8(None) => Value::Null,
        ScalarValue::Int8(Some(i)) => Value::Number((*i).into()),
        ScalarValue::Int8(None) => Value::Null,
        ScalarValue::Int16(Some(i)) => Value::Number((*i).into()),
        ScalarValue::Int16(None) => Value::Null,
        ScalarValue::Int32(Some(i)) => Value::Number((*i).into()),
        ScalarValue::Int32(None) => Value::Null,
        ScalarValue::Int64(Some(i)) => Value::Number((*i).into()),
        ScalarValue::Int64(None) => Value::Null,
        ScalarValue::UInt8(Some(i)) => Value::Number((*i).into()),
        ScalarValue::UInt8(None) => Value::Null,
        ScalarValue::UInt16(Some(i)) => Value::Number((*i).into()),
        ScalarValue::UInt16(None) => Value::Null,
        ScalarValue::UInt32(Some(i)) => Value::Number((*i).into()),
        ScalarValue::UInt32(None) => Value::Null,
        ScalarValue::UInt64(Some(i)) => Value::Number((*i).into()),
        ScalarValue::UInt64(None) => Value::Null,
        ScalarValue::Float32(Some(f)) => serde_json::Number::from_f64(*f as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ScalarValue::Float32(None) => Value::Null,
        ScalarValue::Float64(Some(f)) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ScalarValue::Float64(None) => Value::Null,
        ScalarValue::Boolean(Some(b)) => Value::Bool(*b),
        ScalarValue::Boolean(None) => Value::Null,
        _ => Value::String(scalar.to_string()), // Fallback for unsupported types
    }
}

fn json_value_to_scalar(value: &Value, type_str: &str) -> Result<ScalarValue, String> {
    if value.is_null() {
        return match type_str {
            "Utf8" => Ok(ScalarValue::Utf8(None)),
            "LargeUtf8" => Ok(ScalarValue::LargeUtf8(None)),
            "Int8" => Ok(ScalarValue::Int8(None)),
            "Int16" => Ok(ScalarValue::Int16(None)),
            "Int32" => Ok(ScalarValue::Int32(None)),
            "Int64" => Ok(ScalarValue::Int64(None)),
            "UInt8" => Ok(ScalarValue::UInt8(None)),
            "UInt16" => Ok(ScalarValue::UInt16(None)),
            "UInt32" => Ok(ScalarValue::UInt32(None)),
            "UInt64" => Ok(ScalarValue::UInt64(None)),
            "Float32" => Ok(ScalarValue::Float32(None)),
            "Float64" => Ok(ScalarValue::Float64(None)),
            "Boolean" => Ok(ScalarValue::Boolean(None)),
            _ => Err(format!("Unsupported null type: {}", type_str)),
        };
    }

    match type_str {
        "Utf8" => value
            .as_str()
            .map(|s| ScalarValue::Utf8(Some(s.to_string())))
            .ok_or_else(|| "Expected string for Utf8".to_string()),
        "LargeUtf8" => value
            .as_str()
            .map(|s| ScalarValue::LargeUtf8(Some(s.to_string())))
            .ok_or_else(|| "Expected string for LargeUtf8".to_string()),
        "Int8" => value
            .as_i64()
            .and_then(|i| i.try_into().ok())
            .map(|i: i8| ScalarValue::Int8(Some(i)))
            .ok_or_else(|| "Expected number for Int8".to_string()),
        "Int16" => value
            .as_i64()
            .and_then(|i| i.try_into().ok())
            .map(|i: i16| ScalarValue::Int16(Some(i)))
            .ok_or_else(|| "Expected number for Int16".to_string()),
        "Int32" => value
            .as_i64()
            .and_then(|i| i.try_into().ok())
            .map(|i: i32| ScalarValue::Int32(Some(i)))
            .ok_or_else(|| "Expected number for Int32".to_string()),
        "Int64" => value
            .as_i64()
            .map(|i| ScalarValue::Int64(Some(i)))
            .ok_or_else(|| "Expected number for Int64".to_string()),
        "UInt8" => value
            .as_u64()
            .and_then(|i| i.try_into().ok())
            .map(|i: u8| ScalarValue::UInt8(Some(i)))
            .ok_or_else(|| "Expected number for UInt8".to_string()),
        "UInt16" => value
            .as_u64()
            .and_then(|i| i.try_into().ok())
            .map(|i: u16| ScalarValue::UInt16(Some(i)))
            .ok_or_else(|| "Expected number for UInt16".to_string()),
        "UInt32" => value
            .as_u64()
            .and_then(|i| i.try_into().ok())
            .map(|i: u32| ScalarValue::UInt32(Some(i)))
            .ok_or_else(|| "Expected number for UInt32".to_string()),
        "UInt64" => value
            .as_u64()
            .map(|i| ScalarValue::UInt64(Some(i)))
            .ok_or_else(|| "Expected number for UInt64".to_string()),
        "Float32" => value
            .as_f64()
            .map(|f| ScalarValue::Float32(Some(f as f32)))
            .ok_or_else(|| "Expected number for Float32".to_string()),
        "Float64" => value
            .as_f64()
            .map(|f| ScalarValue::Float64(Some(f)))
            .ok_or_else(|| "Expected number for Float64".to_string()),
        "Boolean" => value
            .as_bool()
            .map(|b| ScalarValue::Boolean(Some(b)))
            .ok_or_else(|| "Expected boolean for Boolean".to_string()),
        _ => Err(format!("Unsupported scalar value type: {}", type_str)),
    }
}
