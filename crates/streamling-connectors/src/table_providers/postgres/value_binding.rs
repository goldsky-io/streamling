use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use std::sync::Arc;
use streamling_core::streamling_err;

use crate::table_providers::postgres::type_mapping::get_postgres_type_info;

/// Helper to determine if a DataType should be bound as string
/// This uses the type mapping to centralize the decision
fn should_bind_as_string(data_type: &DataType) -> bool {
    // Create a temporary field to query the type mapping
    // Note: This won't have metadata for U256/I256, but those are transformed to Utf8 anyway
    let field = arrow_schema::Field::new("temp", data_type.clone(), false);
    get_postgres_type_info(&field).string_cast_sql.is_some()
}

/// Helper module for binding Arrow array values to PostgreSQL queries with proper types
/// Based on patterns from datafusion-table-providers
/// Bind a value from an Arrow array to a sqlx PostgreSQL query
/// Handles type conversion and null values properly
pub fn bind_arrow_value_to_query<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    array: &Arc<dyn datafusion::arrow::array::Array>,
    index: usize,
    data_type: &DataType,
) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
    if array.is_null(index) {
        // Bind null with the appropriate type based on Arrow data type
        // Use the type mapping to determine if we should bind as string or native type
        let bind_as_string = should_bind_as_string(data_type);

        // Special case for binary types which bind as Vec<u8>
        if matches!(
            data_type,
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
        ) {
            return Ok(q.bind::<Option<Vec<u8>>>(None));
        }

        // For other types, use the binding strategy from type mapping
        if bind_as_string {
            return Ok(q.bind::<Option<String>>(None));
        }

        // Bind as native types based on DataType
        let q = match data_type {
            DataType::Boolean => q.bind::<Option<bool>>(None),
            DataType::Int8 => q.bind::<Option<i16>>(None), // PostgreSQL SMALLINT
            DataType::Int16 => q.bind::<Option<i16>>(None),
            DataType::Int32 => q.bind::<Option<i32>>(None),
            DataType::Int64 => q.bind::<Option<i64>>(None),
            DataType::UInt8 => q.bind::<Option<i32>>(None), // PostgreSQL INTEGER
            DataType::UInt16 => q.bind::<Option<i32>>(None), // PostgreSQL INTEGER
            DataType::UInt32 => q.bind::<Option<i64>>(None), // PostgreSQL BIGINT
            DataType::Float32 => q.bind::<Option<f32>>(None),
            DataType::Float64 => q.bind::<Option<f64>>(None),
            _ => q.bind::<Option<String>>(None), // Fallback to string
        };
        return Ok(q);
    }

    // Bind non-null value with proper type conversion
    let q = match data_type {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            q.bind(arr.value(index) as i16) // PostgreSQL SMALLINT
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            q.bind(arr.value(index)) // PostgreSQL BIGINT
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            q.bind(arr.value(index) as i32) // PostgreSQL INTEGER
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            q.bind(arr.value(index) as i32) // PostgreSQL INTEGER
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            q.bind(arr.value(index) as i64) // PostgreSQL BIGINT
        }
        DataType::UInt64 => {
            // UInt64 needs special handling - convert to string for NUMERIC binding
            // This is determined by type mapping (string_cast_sql is Some)
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            q.bind(arr.value(index).to_string())
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            q.bind(arr.value(index))
        }
        DataType::Date32 => {
            // Convert to PostgreSQL date string format YYYY-MM-DD
            let date =
                crate::table_providers::postgres::schema_extraction::extract_value_from_array(
                    array, index,
                )?
                .ok_or_else(|| streamling_err!("invalid date32 value"))?;
            q.bind(date)
        }
        DataType::Date64 => {
            // Convert to PostgreSQL date string format YYYY-MM-DD
            let date =
                crate::table_providers::postgres::schema_extraction::extract_value_from_array(
                    array, index,
                )?
                .ok_or_else(|| streamling_err!("invalid date64 value"))?;
            q.bind(date)
        }
        DataType::Time32(_) | DataType::Time64(_) => {
            // Extract time as string for PostgreSQL TIME
            let time =
                crate::table_providers::postgres::schema_extraction::extract_value_from_array(
                    array, index,
                )?
                .ok_or_else(|| streamling_err!("invalid time value"))?;
            q.bind(time)
        }
        DataType::Timestamp(_, _) => {
            // Extract timestamp as string for PostgreSQL TIMESTAMP
            let timestamp =
                crate::table_providers::postgres::schema_extraction::extract_value_from_array(
                    array, index,
                )?
                .ok_or_else(|| streamling_err!("invalid timestamp value"))?;
            q.bind(timestamp)
        }
        DataType::Decimal128(_precision, scale) => {
            // Decimal128 binds as string (determined by type mapping)
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let value = arr.value(index);
            // Convert decimal to properly formatted string with scale
            let scaled = value.to_string();
            // Format with proper decimal places
            let formatted = format_decimal_string(&scaled, *scale as usize);
            q.bind(formatted)
        }
        DataType::Decimal256(_precision, scale) => {
            // Decimal256 binds as string (determined by type mapping)
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let value = arr.value(index);
            // Convert decimal256 to string with proper scale
            let scaled = value.to_string();
            let formatted = format_decimal_string(&scaled, *scale as usize);
            q.bind(formatted)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            // String types - already stringified (including U256/I256/nested JSON)
            // Utf8View uses StringViewArray, but extract_value_from_array handles it correctly
            let value =
                crate::table_providers::postgres::schema_extraction::extract_value_from_array(
                    array, index,
                )?
                .ok_or_else(|| streamling_err!("expected non-null string value"))?;
            q.bind(value)
        }
        DataType::Binary | DataType::LargeBinary => {
            let bytes = match data_type {
                DataType::Binary => {
                    let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                    arr.value(index).to_vec()
                }
                DataType::LargeBinary => {
                    let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                    arr.value(index).to_vec()
                }
                _ => unreachable!(),
            };
            q.bind(bytes) // PostgreSQL BYTEA
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            q.bind(arr.value(index).to_vec()) // PostgreSQL BYTEA
        }
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => {
            // Nested types should already be converted to JSON strings
            // They bind as string and will be cast to JSONB in SQL (determined by type mapping)
            let value =
                crate::table_providers::postgres::schema_extraction::extract_value_from_array(
                    array, index,
                )?
                .ok_or_else(|| streamling_err!("expected non-null JSON string value"))?;
            q.bind(value) // Will be cast to JSONB in SQL
        }
        _ => {
            // For other types, extract as string as fallback
            let value =
                crate::table_providers::postgres::schema_extraction::extract_value_from_array(
                    array, index,
                )?
                .ok_or_else(|| {
                    streamling_err!(
                        "unsupported Arrow type for PostgreSQL binding: {:?}",
                        data_type
                    )
                })?;
            q.bind(value)
        }
    };

    Ok(q)
}

/// Format a decimal string with the specified scale (number of decimal places)
fn format_decimal_string(value: &str, scale: usize) -> String {
    if scale == 0 {
        return value.to_string();
    }

    // If value already has decimal point, ensure it has the right number of decimal places
    if let Some(dot_pos) = value.find('.') {
        let integer_part = &value[..dot_pos];
        let decimal_part = &value[dot_pos + 1..];

        if decimal_part.len() == scale {
            value.to_string()
        } else if decimal_part.len() < scale {
            // Pad with zeros
            format!(
                "{}.{}{}",
                integer_part,
                decimal_part,
                "0".repeat(scale - decimal_part.len())
            )
        } else {
            // Truncate (shouldn't happen, but handle it)
            format!("{}.{}", integer_part, &decimal_part[..scale])
        }
    } else {
        // No decimal point, add it with zeros
        format!("{}.{}", value, "0".repeat(scale))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray,
        UInt64Array,
    };
    use std::sync::Arc;

    // Test helper to verify binding doesn't panic
    fn test_binding_doesnt_panic(array: Arc<dyn Array>, index: usize, data_type: &DataType) {
        // Create a dummy query - we can't easily test the actual binding without a real connection
        // but we can at least verify the function doesn't panic
        let query = sqlx::query("SELECT $1");
        let result = bind_arrow_value_to_query(query, &array, index, data_type);
        assert!(result.is_ok(), "Binding should succeed for {:?}", data_type);
    }

    #[test]
    fn test_boolean_binding() {
        let array: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![true, false]));
        test_binding_doesnt_panic(array.clone(), 0, &DataType::Boolean);
        test_binding_doesnt_panic(array, 1, &DataType::Boolean);
    }

    #[test]
    fn test_int_binding() {
        let array: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1, -2, 3]));
        test_binding_doesnt_panic(array.clone(), 0, &DataType::Int64);
        test_binding_doesnt_panic(array.clone(), 1, &DataType::Int64);
        test_binding_doesnt_panic(array, 2, &DataType::Int64);
    }

    #[test]
    fn test_uint_binding() {
        let array: Arc<dyn Array> =
            Arc::new(UInt64Array::from(vec![1, 2, 18446744073709551615u64]));
        test_binding_doesnt_panic(array.clone(), 0, &DataType::UInt64);
        test_binding_doesnt_panic(array, 2, &DataType::UInt64);
    }

    #[test]
    fn test_float_binding() {
        let array: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.5, -2.5]));
        test_binding_doesnt_panic(array.clone(), 0, &DataType::Float64);
        test_binding_doesnt_panic(array, 1, &DataType::Float64);
    }

    #[test]
    fn test_string_binding() {
        let array: Arc<dyn Array> = Arc::new(StringArray::from(vec!["hello", "world"]));
        test_binding_doesnt_panic(array.clone(), 0, &DataType::Utf8);
        test_binding_doesnt_panic(array, 1, &DataType::Utf8);
    }

    #[test]
    fn test_binary_binding() {
        let array: Arc<dyn Array> = Arc::new(BinaryArray::from(vec![
            &[0x41u8, 0x42][..],
            &[0x01u8, 0x02][..],
        ]));
        test_binding_doesnt_panic(array.clone(), 0, &DataType::Binary);
        test_binding_doesnt_panic(array, 1, &DataType::Binary);
    }

    #[test]
    fn test_null_binding() {
        let array: Arc<dyn Array> = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        test_binding_doesnt_panic(array.clone(), 0, &DataType::Int64);
        test_binding_doesnt_panic(array.clone(), 1, &DataType::Int64); // null
        test_binding_doesnt_panic(array, 2, &DataType::Int64);
    }

    #[test]
    fn test_decimal_binding() {
        let array: Arc<dyn Array> = Arc::new(
            Decimal128Array::from(vec![12345i128])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        test_binding_doesnt_panic(array, 0, &DataType::Decimal128(10, 2));
    }

    #[test]
    fn test_format_decimal_string() {
        use super::format_decimal_string;

        // Test with scale 0
        assert_eq!(format_decimal_string("123", 0), "123");

        // Test adding decimal point
        assert_eq!(format_decimal_string("123", 2), "123.00");

        // Test padding decimals
        assert_eq!(format_decimal_string("123.4", 3), "123.400");

        // Test truncation (shouldn't happen but handled)
        assert_eq!(format_decimal_string("123.456", 2), "123.45");

        // Test already correct scale
        assert_eq!(format_decimal_string("123.45", 2), "123.45");
    }
}
