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
            // Decimal128 binds as string (determined by type mapping). The array
            // value is the UNSCALED integer; place the point `scale` from the right.
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let unscaled = arr.value(index).to_string();
            let formatted = unscaled_to_numeric_string(&unscaled, *scale as usize);
            q.bind(formatted)
        }
        DataType::Decimal256(_precision, scale) => {
            // Decimal256 binds as string (determined by type mapping). The array
            // value is the UNSCALED integer; place the point `scale` from the right.
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let unscaled = arr.value(index).to_string();
            let formatted = unscaled_to_numeric_string(&unscaled, *scale as usize);
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

/// Render a base-10 **unscaled integer** — the raw value of an Arrow
/// `Decimal128`/`Decimal256` (e.g. `"12345"` for `123.45` at scale 2) — as the
/// decimal string Postgres `NUMERIC` expects, with the point placed `scale`
/// digits from the right.
///
/// The previous implementation *appended* `scale` trailing zeros (treating the
/// unscaled integer as if it were already the integer part), which inflated the
/// magnitude by 10^scale: it both wrote wrong values and overflowed otherwise
/// wide-enough NUMERIC columns for high-scale / all-fractional decimals (F3).
fn unscaled_to_numeric_string(unscaled: &str, scale: usize) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }
    let (sign, digits) = match unscaled.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", unscaled),
    };
    let body = if digits.len() > scale {
        // Has integer digits: split `scale` from the right.
        let (int_part, frac_part) = digits.split_at(digits.len() - scale);
        format!("{int_part}.{frac_part}")
    } else {
        // Magnitude < 1: pad with leading zeros after "0.".
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    };
    // Never emit "-0.000…" for a zero magnitude.
    if sign == "-" && digits.bytes().all(|b| b == b'0') {
        body
    } else {
        format!("{sign}{body}")
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
    fn test_unscaled_to_numeric_string() {
        use super::unscaled_to_numeric_string;

        // scale 0: unchanged
        assert_eq!(unscaled_to_numeric_string("123", 0), "123");
        // point placed `scale` from the right
        assert_eq!(unscaled_to_numeric_string("12345", 2), "123.45");
        // exactly `scale` digits -> "0.<digits>"
        assert_eq!(unscaled_to_numeric_string("45", 2), "0.45");
        // all-fractional (scale == precision), the F3 dec128(10,10) shape
        assert_eq!(unscaled_to_numeric_string("1234567890", 10), "0.1234567890");
        // magnitude < 10^scale needs leading zero padding
        assert_eq!(unscaled_to_numeric_string("5", 3), "0.005");
        // zero
        assert_eq!(unscaled_to_numeric_string("0", 2), "0.00");
        assert_eq!(unscaled_to_numeric_string("0", 0), "0");

        // negatives
        assert_eq!(unscaled_to_numeric_string("-12345", 2), "-123.45");
        assert_eq!(
            unscaled_to_numeric_string("-1234567890", 10),
            "-0.1234567890"
        );
        assert_eq!(unscaled_to_numeric_string("-5", 3), "-0.005");

        // F3 dec256(60,30) high-scale shape: 60-digit unscaled -> 30 integer +
        // 30 fractional digits (fits NUMERIC(80,30); the old code produced 60
        // integer digits and overflowed).
        let big = "1".repeat(60);
        assert_eq!(
            unscaled_to_numeric_string(&big, 30),
            format!("{}.{}", "1".repeat(30), "1".repeat(30))
        );
    }
}
