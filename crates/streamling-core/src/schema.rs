//! Shared helpers for building Arrow schemas from user-provided topology config.

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use heck::ToUpperCamelCase;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::streamling_user_err;

/// Default precision and scale for `decimal` types declared without parameters.
/// Mirrors the Postgres `numeric` mapping in [`crate::utils::pg`].
const DEFAULT_DECIMAL_PRECISION: u8 = 38;
const DEFAULT_DECIMAL_SCALE: i8 = 9;

/// Convert a `column name -> Arrow type string` map into an Arrow [`SchemaRef`].
///
/// Type strings are matched case-insensitively. `"string"` maps to `Utf8`;
/// `decimal128`/`decimal256` (alias `decimal` -> 128-bit) accept an optional
/// `(precision[, scale])` suffix and otherwise default to `(38, 9)`; every other value is
/// upper-camel-cased (e.g. `"int64"` -> `Int64`) and parsed via [`DataType::from_str`].
/// All fields are nullable.
pub fn arrow_schema_from_type_map(schema_map: &BTreeMap<String, String>) -> Result<SchemaRef> {
    let fields = schema_map
        .iter()
        .map(|(name, type_str)| Ok(Field::new(name, parse_data_type(type_str)?, true)))
        .collect::<Result<Vec<Field>>>()?;

    Ok(Arc::new(Schema::new(fields)))
}

fn parse_data_type(type_str: &str) -> Result<DataType> {
    if let Some(decimal) = parse_decimal_type(type_str)? {
        return Ok(decimal);
    }

    let normalized = if type_str.eq_ignore_ascii_case("string") {
        "Utf8".to_string()
    } else {
        type_str.to_upper_camel_case()
    };
    DataType::from_str(&normalized).map_err(|_| {
        DataFusionError::from(streamling_user_err!(
            "unsupported data type '{}' in schema; supported types: \
             int8, int16, int32, int64, uint8, uint16, uint32, uint64, \
             float16, float32, float64, string/utf8, binary, boolean, \
             date32, date64, timestamp, time32, time64, duration, interval, \
             decimal128(precision, scale), decimal256(precision, scale), null",
            type_str
        ))
    })
}

/// Parse `decimal128`/`decimal256` (alias `decimal` -> 128-bit) with an optional
/// `(precision[, scale])` suffix. Returns `Ok(None)` when `type_str` is not a decimal. A
/// bare name defaults to `(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE)`; a precision
/// without a scale defaults the scale to 0.
fn parse_decimal_type(type_str: &str) -> Result<Option<DataType>> {
    let lower = type_str.trim().to_ascii_lowercase();
    let (is_256, params) = if let Some(rest) = lower.strip_prefix("decimal256") {
        (true, rest)
    } else if let Some(rest) = lower.strip_prefix("decimal128") {
        (false, rest)
    } else if let Some(rest) = lower.strip_prefix("decimal") {
        (false, rest)
    } else {
        return Ok(None);
    };

    let (precision, scale) = match params.trim() {
        "" => (DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE),
        params => {
            let inner = params
                .strip_prefix('(')
                .and_then(|p| p.strip_suffix(')'))
                .ok_or_else(|| {
                    streamling_user_err!(
                        "invalid decimal type '{}': expected 'decimal128(precision, scale)'",
                        type_str
                    )
                })?;
            let mut parts = inner.split(',');
            let precision = parts
                .next()
                .unwrap_or_default()
                .trim()
                .parse::<u8>()
                .map_err(|_| {
                    streamling_user_err!("invalid precision in decimal type '{}'", type_str)
                })?;
            let scale = match parts.next() {
                Some(scale) => scale.trim().parse::<i8>().map_err(|_| {
                    streamling_user_err!("invalid scale in decimal type '{}'", type_str)
                })?,
                None => 0,
            };
            if parts.next().is_some() {
                return Err(streamling_user_err!(
                    "invalid decimal type '{}': expected at most precision and scale",
                    type_str
                )
                .into());
            }
            (precision, scale)
        }
    };

    Ok(Some(if is_256 {
        DataType::Decimal256(precision, scale)
    } else {
        DataType::Decimal128(precision, scale)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_nullable_fields() {
        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());
        schema_map.insert("value".to_string(), "decimal128".to_string());

        let schema = arrow_schema_from_type_map(&schema_map).unwrap();

        assert_eq!(schema.fields().len(), 3);
        // BTreeMap iterates in key order: id, name
        let id = schema.field(0);
        assert_eq!(id.name(), "id");
        assert_eq!(id.data_type(), &DataType::Int64);
        assert!(id.is_nullable());

        let name = schema.field(1);
        assert_eq!(name.name(), "name");
        assert_eq!(name.data_type(), &DataType::Utf8);
        assert!(name.is_nullable());

        // Bare `decimal128` falls back to the default precision and scale.
        let value = schema.field(2);
        assert_eq!(value.name(), "value");
        assert_eq!(
            value.data_type(),
            &DataType::Decimal128(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE)
        );
        assert!(value.is_nullable());

        assert!(
            schema
                .fields()
                .iter()
                .all(|f| f.name() != crate::data::COLUMN_NAME_OP),
            "_gs_op must not be appended by the shared helper"
        );
    }

    #[test]
    fn parses_decimal_precision_and_scale() {
        let mut schema_map = BTreeMap::new();
        schema_map.insert("a".to_string(), "decimal128(20, 4)".to_string());
        schema_map.insert("b".to_string(), "decimal256(50, 8)".to_string());
        schema_map.insert("c".to_string(), "decimal(10)".to_string());

        let schema = arrow_schema_from_type_map(&schema_map).unwrap();

        assert_eq!(schema.field(0).data_type(), &DataType::Decimal128(20, 4));
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal256(50, 8));
        // A precision without a scale defaults the scale to 0.
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(10, 0));
    }

    /// Confirms a decimal declared in the schema actually decodes from a JSON payload.
    #[test]
    fn json_decode_decimal_field() {
        use crate::formats::ToArrowConverter;
        use crate::formats::json::JsonToArrowConverter;
        use datafusion::arrow::array::Decimal128Array;

        let mut schema_map = BTreeMap::new();
        schema_map.insert("amount".to_string(), "decimal128(10, 2)".to_string());

        let schema = arrow_schema_from_type_map(&schema_map).unwrap();
        let mut converter = JsonToArrowConverter::new(schema, true, None);
        converter.buffer(r#"{"amount":123.45}"#.to_string());

        let batch = converter.convert_to_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let amount = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        // 123.45 at scale 2 -> unscaled 12345.
        assert_eq!(amount.value(0), 12345);
    }

    #[test]
    fn rejects_malformed_decimal() {
        let mut schema_map = BTreeMap::new();
        schema_map.insert("x".to_string(), "decimal128(1, 2, 3)".to_string());

        let err = arrow_schema_from_type_map(&schema_map).unwrap_err();
        assert!(err.to_string().contains("decimal"));
    }

    #[test]
    fn rejects_unknown_type() {
        let mut schema_map = BTreeMap::new();
        schema_map.insert("x".to_string(), "not_a_type".to_string());

        let err = arrow_schema_from_type_map(&schema_map).unwrap_err();
        assert!(err.to_string().contains("unsupported data type"));
    }

    /// Exercises the exact path the Kafka JSON source uses: config map -> Arrow schema ->
    /// schema-driven JSON decode of one object per message.
    #[test]
    fn json_decode_uses_map_derived_schema() {
        use crate::formats::ToArrowConverter;
        use crate::formats::json::JsonToArrowConverter;
        use datafusion::arrow::array::{Array, Int64Array, StringArray};

        let mut schema_map = BTreeMap::new();
        schema_map.insert("id".to_string(), "int64".to_string());
        schema_map.insert("name".to_string(), "string".to_string());

        let schema = arrow_schema_from_type_map(&schema_map).unwrap();
        let mut converter = JsonToArrowConverter::new(schema, true, None);
        converter.buffer(r#"{"id":1,"name":"foo"}"#.to_string());
        converter.buffer(r#"{"id":2,"name":null}"#.to_string());

        let batch = converter.convert_to_batch().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.value(0), 1);
        assert_eq!(id.value(1), 2);

        let name = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name.value(0), "foo");
        assert!(name.is_null(1));
    }
}
