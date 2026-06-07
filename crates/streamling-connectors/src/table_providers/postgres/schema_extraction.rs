use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use std::sync::Arc;
use streamling_core::streamling_err;

use crate::table_providers::util::timestamp_format;

/// Simple hex encoding for binary data (for PostgreSQL BYTEA)
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Extract a value from an Arrow array at the given index
/// Returns None for null values, Some(String) for non-null values
pub fn extract_value_from_array(array: &Arc<dyn Array>, index: usize) -> Result<Option<String>> {
    if array.is_null(index) {
        return Ok(None);
    }

    let value = match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let val = arr.value(index);
            if val.is_finite() {
                val.to_string()
            } else if val.is_nan() {
                "NaN".to_string()
            } else if val.is_infinite() && val.is_sign_positive() {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = arr.value(index);
            if val.is_finite() {
                val.to_string()
            } else if val.is_nan() {
                "NaN".to_string()
            } else if val.is_infinite() && val.is_sign_positive() {
                "Infinity".to_string()
            } else {
                "-Infinity".to_string()
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            arr.value(index).to_string()
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            arr.value(index).to_string()
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            // Convert binary to hex string for PostgreSQL BYTEA
            format!("\\x{}", hex_encode(arr.value(index)))
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            format!("\\x{}", hex_encode(arr.value(index)))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            format!("\\x{}", hex_encode(arr.value(index)))
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            // Date32 represents days since Unix epoch (1970-01-01)
            timestamp_format::format_date_from_days(arr.value(index))
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            // Date64 represents milliseconds since Unix epoch
            let timestamp_seconds = arr.value(index) / 1000;
            // Extract just the date part
            timestamp_format::format_timestamp(timestamp_seconds, 0)[..10].to_string()
        }
        DataType::Timestamp(unit, _tz) => {
            // Convert Arrow timestamp to PostgreSQL timestamp string format
            // Arrow timestamps are stored as integers representing time since Unix epoch
            let (timestamp_seconds, nanos) = match unit {
                datafusion::arrow::datatypes::TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    (arr.value(index), 0)
                }
                datafusion::arrow::datatypes::TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let ms = arr.value(index);
                    (ms / 1000, ((ms % 1000) * 1_000_000) as u32)
                }
                datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let us = arr.value(index);
                    (us / 1_000_000, ((us % 1_000_000) * 1000) as u32)
                }
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    let ns = arr.value(index);
                    (ns / 1_000_000_000, (ns % 1_000_000_000) as u32)
                }
            };

            timestamp_format::format_timestamp(timestamp_seconds, nanos)
        }
        DataType::Decimal128(_precision, _scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let val = arr.value(index);
            // Convert decimal128 to string representation
            // Need to scale properly based on scale parameter
            format!("{}", val) // Placeholder - should scale properly
        }
        DataType::Decimal256(_precision, _scale) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let val = arr.value(index);
            format!("{}", val) // Placeholder
        }
        _ => {
            return Err(streamling_err!(
                "unsupported Arrow type for PostgreSQL extraction: {:?}",
                array.data_type()
            )
            .into());
        }
    };

    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Float64Array,
        Int64Array, StringArray, TimestampSecondArray, UInt64Array,
    };
    use std::sync::Arc;

    #[test]
    fn test_boolean_extraction() {
        let array: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![true, false, true]));
        assert_eq!(
            extract_value_from_array(&array, 0).unwrap(),
            Some("true".to_string())
        );
        assert_eq!(
            extract_value_from_array(&array, 1).unwrap(),
            Some("false".to_string())
        );
    }

    #[test]
    fn test_int_extraction() {
        let array: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1, -2, 3]));
        assert_eq!(
            extract_value_from_array(&array, 0).unwrap(),
            Some("1".to_string())
        );
        assert_eq!(
            extract_value_from_array(&array, 1).unwrap(),
            Some("-2".to_string())
        );
    }

    #[test]
    fn test_uint_extraction() {
        let array: Arc<dyn Array> =
            Arc::new(UInt64Array::from(vec![1, 2, 18446744073709551615u64]));
        assert_eq!(
            extract_value_from_array(&array, 0).unwrap(),
            Some("1".to_string())
        );
        assert_eq!(
            extract_value_from_array(&array, 2).unwrap(),
            Some("18446744073709551615".to_string())
        );
    }

    #[test]
    fn test_float_extraction() {
        let array: Arc<dyn Array> = Arc::new(Float64Array::from(vec![
            1.5,
            -2.5,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ]));
        assert_eq!(
            extract_value_from_array(&array, 0).unwrap(),
            Some("1.5".to_string())
        );
        assert_eq!(
            extract_value_from_array(&array, 1).unwrap(),
            Some("-2.5".to_string())
        );
        assert_eq!(
            extract_value_from_array(&array, 2).unwrap(),
            Some("NaN".to_string())
        );
        assert_eq!(
            extract_value_from_array(&array, 3).unwrap(),
            Some("Infinity".to_string())
        );
        assert_eq!(
            extract_value_from_array(&array, 4).unwrap(),
            Some("-Infinity".to_string())
        );
    }

    #[test]
    fn test_string_extraction() {
        let array: Arc<dyn Array> = Arc::new(StringArray::from(vec!["hello", "world", "test"]));
        assert_eq!(
            extract_value_from_array(&array, 0).unwrap(),
            Some("hello".to_string())
        );
        assert_eq!(
            extract_value_from_array(&array, 1).unwrap(),
            Some("world".to_string())
        );
    }

    #[test]
    fn test_binary_extraction() {
        let array: Arc<dyn Array> = Arc::new(BinaryArray::from(vec![
            &[0x41u8, 0x42, 0x43][..],
            &[0x01u8, 0x02][..],
        ]));
        let result = extract_value_from_array(&array, 0).unwrap();
        assert!(result.is_some());
        assert!(result.unwrap().starts_with("\\x"));
    }

    #[test]
    fn test_fixed_size_binary_extraction() {
        use arrow::buffer::Buffer;
        let array: Arc<dyn Array> = Arc::new(FixedSizeBinaryArray::new(
            3,
            Buffer::from(vec![0x41, 0x42, 0x43, 0x44, 0x45, 0x46]),
            None,
        ));
        let result = extract_value_from_array(&array, 0).unwrap();
        assert!(result.is_some());
        assert!(result.unwrap().starts_with("\\x"));
    }

    #[test]
    fn test_timestamp_extraction() {
        // Test timestamp second (epoch + 1000 seconds = 1970-01-01 00:16:40)
        let array: Arc<dyn Array> = Arc::new(TimestampSecondArray::from(vec![1000, 2000]));
        let result = extract_value_from_array(&array, 0).unwrap();
        assert_eq!(result, Some("1970-01-01 00:16:40".to_string()));

        // Test timestamp nanosecond (with fractional seconds)
        // Use a value that has fractional seconds: 1736067304123456789 nanoseconds
        let ns_array: Arc<dyn Array> = Arc::new(
            datafusion::arrow::array::TimestampNanosecondArray::from(vec![1736067304123456789i64]),
        );
        let ns_result = extract_value_from_array(&ns_array, 0).unwrap();
        assert!(ns_result.is_some());
        let timestamp_str = ns_result.unwrap();
        // Verify it's a valid timestamp format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.ffffff
        assert!(timestamp_str.len() >= 19); // At least "YYYY-MM-DD HH:MM:SS"
        assert!(timestamp_str.chars().nth(4) == Some('-'));
        assert!(timestamp_str.chars().nth(7) == Some('-'));
        assert!(timestamp_str.chars().nth(10) == Some(' '));
        // This value has fractional seconds, so verify it includes them
        assert!(timestamp_str.contains('.'));
    }

    #[test]
    fn test_null_extraction() {
        let array: Arc<dyn Array> = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        assert_eq!(
            extract_value_from_array(&array, 0).unwrap(),
            Some("1".to_string())
        );
        assert_eq!(extract_value_from_array(&array, 1).unwrap(), None);
        assert_eq!(
            extract_value_from_array(&array, 2).unwrap(),
            Some("3".to_string())
        );
    }

    #[test]
    fn test_decimal_extraction() {
        let array: Arc<dyn Array> = Arc::new(
            Decimal128Array::from(vec![12345i128])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let result = extract_value_from_array(&array, 0).unwrap();
        assert!(result.is_some());
    }

    #[test]
    #[should_panic(expected = "Trying to access an element")]
    fn test_invalid_index() {
        let array: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1, 2]));
        // Index 2 is out of bounds - should panic
        let _ = extract_value_from_array(&array, 2).unwrap();
    }
}
