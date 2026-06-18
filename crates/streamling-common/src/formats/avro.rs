mod arrow_array_reader;
pub mod arrow_avro;
mod schema;
mod writer;

pub use crate::formats::avro::schema::convert_avro_schema_to_arrow;
pub use crate::formats::avro::schema::post_process_avro_schema_for_reading;
pub use crate::formats::avro::schema::post_process_avro_schema_for_writing;
pub use crate::formats::avro::writer::{serialize, to_avro};
use crate::formats::{FromArrowConverter, ToArrowConverter};
use apache_avro::schema::Schema as AvroSchema;
use apache_avro::types::Value;
use arrow_array_reader::AvroArrowArrayReader;
use arrow_schema::{Schema, SchemaRef};
use datafusion::arrow::array::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use std::sync::Arc;

use crate::{streamling_err, streamling_user_err};

// Maximum precision we allow in schema definitions (100 digits)
const MAX_SCHEMA_PRECISION: usize = 100;

#[derive(Clone)]
pub struct AvroToArrowConverter {
    schema: SchemaRef,
    avro_schema: AvroSchema,
    projection: Option<Vec<usize>>,
    projected_fields: Option<Vec<String>>,
    values: Vec<Value>,
    size: usize,
}

impl AvroToArrowConverter {
    pub fn new(
        schema: Arc<Schema>,
        avro_schema: AvroSchema,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let projected_fields = projection
            .clone()
            .map(|p| p.iter().map(|i| schema.field(*i).name().clone()).collect());

        AvroToArrowConverter {
            schema,
            avro_schema,
            projection,
            projected_fields,
            values: Vec::new(),
            size: 0,
        }
    }

    fn clear(&mut self) {
        self.values.clear();
        self.size = 0;
    }
}

impl ToArrowConverter<Value> for AvroToArrowConverter {
    fn buffer(&mut self, value: Value) {
        self.values.push(value);
        self.size += 1;
    }

    fn convert_to_batch(&mut self) -> Result<RecordBatch> {
        if self.size == 0 {
            let schema = match self.projection {
                Some(ref projection) => {
                    let fields: Vec<_> = projection
                        .iter()
                        .map(|i| self.schema.field(*i).clone())
                        .collect();
                    Arc::new(Schema::new(fields))
                }
                None => self.schema.clone(),
            };
            return Ok(RecordBatch::new_empty(schema));
        }
        let mut reader = AvroArrowArrayReader::new(
            self.schema.clone(),
            post_process_avro_schema_for_reading(self.avro_schema.clone()),
            self.projected_fields.clone(),
        );

        let values_as_tuples: Result<Vec<&Vec<(String, Value)>>> = self
            .values
            .iter()
            .map(|v| match v {
                Value::Record(r) => Ok(r),
                Value::Union(_pos, val) => match val.as_ref() {
                    Value::Record(r) => Ok(r),
                    _ => Err(streamling_user_err!(
                        "expected Avro record inside union, got {:?}",
                        val
                    )
                    .into()),
                },
                _ => Err(streamling_user_err!("expected Avro record, got {:?}", v).into()),
            })
            .collect();

        let record_batch = reader.read_batch(values_as_tuples?).ok_or_else(|| {
            DataFusionError::from(streamling_err!(
                "Avro reader returned None for non-empty batch ({} values)",
                self.size
            ))
        })??;

        self.clear();

        Ok(record_batch)
    }
}

#[derive(Clone)]
pub struct FromArrowToAvroConverter {
    schema: SchemaRef,
    topic: String, // Using topic as a record name
}

impl FromArrowToAvroConverter {
    pub fn new(schema: SchemaRef, topic: String) -> Self {
        FromArrowToAvroConverter { schema, topic }
    }
}

impl FromArrowConverter<Value> for FromArrowToAvroConverter {
    fn convert_from_batch(&self, batch: &RecordBatch) -> Result<Vec<Value>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let avro_schema = to_avro(&self.topic, &self.schema.fields);
        let payloads = serialize(&avro_schema, batch);
        Ok(payloads)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::Decimal;
    use apache_avro::types::Value;
    use arrow_schema::{DataType, Field};
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::i256;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_avro_to_arrow_converter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Decimal256(76, 18), false), // Updated to use Decimal256 for high precision
        ]));

        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "int"},
                    {"name": "b", "type": "string"},
                    {"name": "c", "type": {"type": "bytes", "logicalType": "decimal", "precision": 76, "scale": 18}}
                ]
            }
        "#,
        ).unwrap();

        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema, None);

        let data = vec![
            Value::Record(vec![
                ("a".to_string(), Value::Int(1)),
                ("b".to_string(), Value::String("Streamling".to_string())),
                (
                    "c".to_string(),
                    Value::Decimal(Decimal::from(vec![
                        5, 45, 35, 104, 51, 168, 97, 115, 93, 112, 0, 0,
                    ])),
                ),
            ]),
            Value::Record(vec![
                ("a".to_string(), Value::Int(2)),
                ("b".to_string(), Value::Null),
                (
                    "c".to_string(),
                    Value::Decimal(Decimal::from(vec![
                        5, 45, 150, 140, 131, 193, 132, 221, 190, 168, 0, 0,
                    ])),
                ),
            ]),
        ];

        for value in data {
            converter.buffer(value);
        }

        let batch = converter.convert_to_batch().unwrap();

        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 2);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let decimal_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal256Array>() // Updated to use Decimal256Array
            .unwrap();

        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        assert_eq!(name_array.value(0), "Streamling");
        assert_eq!(name_array.value(1), ""); // FIXME: should this be null?

        // These are the underlying BigInts (now i256)
        // Note: These expected values may need to be updated based on the actual conversion
        assert_eq!(
            decimal_array.value(0),
            i256::from_i128(1601993916000000000000000000_i128)
        );
        assert_eq!(
            decimal_array.value(1),
            i256::from_i128(1602537658000000000000000000_i128)
        );
    }

    #[test]
    fn test_avro_local_timestamp_decodes_without_panic() {
        use datafusion::arrow::datatypes::TimeUnit;
        // Local (timezone-naive) timestamps must decode through the value resolver
        // into tz-less Arrow timestamps, not hit the `unreachable!()` arm (the bug
        // a bare schema mapping would have relocated to per-row decode time).
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test_local_ts",
                "fields": [
                    {"name": "ts_ms", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
                    {"name": "ts_us", "type": {"type": "long", "logicalType": "local-timestamp-micros"}}
                ]
            }
        "#,
        )
        .unwrap();

        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema, None);
        converter.buffer(Value::Record(vec![
            (
                "ts_ms".to_string(),
                Value::LocalTimestampMillis(1_700_000_000_000),
            ),
            (
                "ts_us".to_string(),
                Value::LocalTimestampMicros(1_700_000_000_000_000),
            ),
        ]));

        let batch = converter.convert_to_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let ms = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let us = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ms.value(0), 1_700_000_000_000);
        assert_eq!(us.value(0), 1_700_000_000_000_000);
    }

    #[test]
    fn test_decimal_precision_boundary() {
        // Test that precision <= 38 uses Decimal128
        let schema_128 = Arc::new(Schema::new(vec![Field::new(
            "low_precision",
            DataType::Decimal128(38, 10),
            false,
        )]));

        let avro_schema_128 = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "low_precision", "type": {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 10}}
                ]
            }
        "#,
        ).unwrap();

        // Test that precision > 38 uses Decimal256
        let schema_256 = Arc::new(Schema::new(vec![Field::new(
            "high_precision",
            DataType::Decimal256(39, 10),
            false,
        )]));

        let avro_schema_256 = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "high_precision", "type": {"type": "bytes", "logicalType": "decimal", "precision": 39, "scale": 10}}
                ]
            }
        "#,
        ).unwrap();

        // Both should work without panicking
        let _converter_128 = AvroToArrowConverter::new(schema_128, avro_schema_128, None);
        let _converter_256 = AvroToArrowConverter::new(schema_256, avro_schema_256, None);
    }

    #[test]
    #[should_panic(
        expected = "Decimal precision 101 exceeds maximum supported precision of 100 for schema definition"
    )]
    fn test_decimal_precision_too_high() {
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "invalid", "type": {"type": "bytes", "logicalType": "decimal", "precision": 101, "scale": 10}}
                ]
            }
        "#,
        ).unwrap();

        // This should panic due to precision > 100 (schema limit)
        let _processed = post_process_avro_schema_for_reading(avro_schema);
    }

    #[test]
    fn test_decimal_precision_allowed_in_schema() {
        // Test that precision between 76 and 100 is allowed in schema definition
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "high_precision", "type": {"type": "bytes", "logicalType": "decimal", "precision": 85, "scale": 10}}
                ]
            }
        "#,
        ).unwrap();

        // This should work (schema allows up to 100)
        let _processed = post_process_avro_schema_for_reading(avro_schema);
    }

    #[test]
    fn test_high_precision_decimal_conversion_attempt() {
        // Test that we allow schemas with precision > 76 and attempt conversion
        // The conversion should succeed if actual data fits, even with high declared precision
        let schema = Arc::new(Schema::new(vec![Field::new(
            "high_precision",
            DataType::Decimal256(85, 10),
            false,
        )]));

        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "high_precision", "type": {"type": "bytes", "logicalType": "decimal", "precision": 85, "scale": 10}}
                ]
            }
        "#,
        ).unwrap();

        // This should work - schema allows precision 85, and conversion will be attempted
        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema, None);

        // Small decimal value that fits in Decimal256 despite high declared precision
        let data = vec![Value::Record(vec![(
            "high_precision".to_string(),
            Value::Decimal(Decimal::from(vec![0, 0, 0, 0, 1, 2, 3, 4])), // Small value
        )])];

        for value in data {
            converter.buffer(value);
        }

        // Should succeed - the actual data fits in Decimal256 even though precision is declared as 85
        let result = converter.convert_to_batch();
        assert!(
            result.is_ok(),
            "Should successfully convert small decimal values even with high declared precision"
        );
    }

    #[test]
    fn test_oversized_decimal_data_handling() {
        // Test that oversized decimal data (>32 bytes) that cannot be reduced by padding removal fails appropriately
        let schema = Arc::new(Schema::new(vec![Field::new(
            "oversized_decimal",
            DataType::Decimal256(50, 10),
            false,
        )]));

        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "oversized_decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 50, "scale": 10}}
                ]
            }
        "#,
        ).unwrap();

        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema.clone(), None);

        // Create decimal data that can be reduced by padding removal (should succeed)
        let mut padded_bytes = vec![0x00; 10]; // 10 bytes of leading zeros (padding)
        padded_bytes.extend_from_slice(&[0x01, 0x23, 0x45, 0x67]); // 4 bytes of actual data
        // Total: 14 bytes, padding removal should make this work

        let data_good = vec![Value::Record(vec![(
            "oversized_decimal".to_string(),
            Value::Decimal(Decimal::from(padded_bytes)),
        )])];

        for value in data_good {
            converter.buffer(value);
        }

        // Should succeed by removing padding
        let result = converter.convert_to_batch();
        assert!(
            result.is_ok(),
            "Should succeed by removing leading padding from decimal data"
        );

        // Now test with truly oversized data that cannot be reduced
        let mut converter2 = AvroToArrowConverter::new(schema.clone(), avro_schema, None);

        // Create 50 bytes of non-padding data (cannot be reduced)
        let large_bytes: Vec<u8> = (1u8..=50u8).collect(); // 50 bytes of varying non-zero data

        let data_bad = vec![Value::Record(vec![(
            "oversized_decimal".to_string(),
            Value::Decimal(Decimal::from(large_bytes)),
        )])];

        for value in data_bad {
            converter2.buffer(value);
        }

        // Should fail because data is too large and cannot be reduced without truncation
        let result = converter2.convert_to_batch();
        assert!(
            result.is_err(),
            "Should fail when decimal data is too large to fit without truncation"
        );
    }

    #[test]
    fn test_decimal_100_precision_value_verification() {
        // Test decimal(100,0) with a known value that fits in uint256
        let schema = Arc::new(Schema::new(vec![Field::new(
            "decimal_100",
            DataType::Decimal256(100, 0),
            false,
        )]));

        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "decimal_100", "type": {"type": "bytes", "logicalType": "decimal", "precision": 100, "scale": 0}}
                ]
            }
        "#,
        ).unwrap();

        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema, None);

        // Use the bigger number: 123456789012345678901234567890 (30 digits)
        // This number in hex is: 0x18EE90FF6C373E0EE4E3F0AD2
        // That's about 13 bytes, so we'll pad it to 32 bytes

        // Create decimal bytes with some padding + the actual number
        let mut decimal_bytes = vec![0x00; 6]; // 6 bytes of leading zeros (padding)

        // Add the hex representation of 123456789012345678901234567890
        // 0x18EE90FF6C373E0EE4E3F0AD2 = 13 bytes
        decimal_bytes.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 8 more padding bytes
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 8 more padding bytes
            0x00, 0x00, 0x00, 0x01, 0x8E, 0xE9, 0x0F, 0xF6, // Start of actual number
            0xC3, 0x73, 0xE0, 0xEE, 0x4E, 0x3F, 0x0A, 0xD2, // End of actual number
        ]);

        // Total: 38 bytes, first 6 should be removed as padding
        // Resulting 32 bytes should represent 123456789012345678901234567890

        let data = vec![Value::Record(vec![(
            "decimal_100".to_string(),
            Value::Decimal(Decimal::from(decimal_bytes)),
        )])];

        for value in data {
            converter.buffer(value);
        }

        let result = converter.convert_to_batch();
        assert!(
            result.is_ok(),
            "Should succeed converting decimal(100,0) value that fits in uint256"
        );

        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1, "Should have one row");
        assert_eq!(batch.num_columns(), 1, "Should have one column");

        // Verify the actual converted value
        let decimal_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();
        let converted_value = decimal_array.value(0);

        // Expected: i256 representing 123456789012345678901234567890
        // The hex is: 0x18EE90FF6C373E0EE4E3F0AD2 (13 bytes)
        // In a 32-byte representation with leading zeros:
        let expected_bytes = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 8 bytes of zeros
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 8 bytes of zeros
            0x00, 0x00, 0x00, 0x01, 0x8E, 0xE9, 0x0F, 0xF6, // Start of number
            0xC3, 0x73, 0xE0, 0xEE, 0x4E, 0x3F, 0x0A, 0xD2, // End of number
        ];
        let expected_value = i256::from_be_bytes(expected_bytes);

        assert_eq!(
            converted_value, expected_value,
            "Converted decimal(100,0) value should match expected hex representation"
        );

        // Print both decimal and hex representations for verification
        println!("Successfully converted decimal(100,0):");
        println!("  Decimal: {}", converted_value);
        println!("  Expected: 123456789012345678901234567890");

        // Convert i256 to hex string manually
        let converted_bytes = converted_value.to_be_bytes();
        let hex_string = converted_bytes
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        println!("  Hex: 0x{}", hex_string);
        println!("  Expected hex: 0x18EE90FF6C373E0EE4E3F0AD2");
    }

    #[test]
    fn test_decimal_100_precision_large_uint256_value() {
        // Test decimal(100,0) with a much larger value that uses more uint256 capacity
        let schema = Arc::new(Schema::new(vec![Field::new(
            "large_decimal_100",
            DataType::Decimal256(100, 0),
            false,
        )]));

        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "large_decimal_100", "type": {"type": "bytes", "logicalType": "decimal", "precision": 100, "scale": 0}}
                ]
            }
        "#,
        ).unwrap();

        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema, None);

        // Create a large decimal that uses significant portion of uint256 space
        // Let's create a number with about 70 decimal digits
        // We'll construct this as a 32-byte big-endian number
        let mut decimal_bytes = vec![0x00; 5]; // 5 bytes of leading zeros (padding to test truncation)

        // Add 32 bytes representing a very large number
        // Use a pattern that creates a large but predictable value
        decimal_bytes.extend_from_slice(&[
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, // 8 bytes
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // 8 bytes
            0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11, // 8 bytes
            0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, // 8 bytes
        ]); // Total: 32 bytes of large number data

        // Total: 37 bytes, first 5 should be removed as padding

        let data = vec![Value::Record(vec![(
            "large_decimal_100".to_string(),
            Value::Decimal(Decimal::from(decimal_bytes)),
        )])];

        for value in data {
            converter.buffer(value);
        }

        let result = converter.convert_to_batch();
        assert!(
            result.is_ok(),
            "Should succeed converting large decimal(100,0) value"
        );

        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1, "Should have one row");
        assert_eq!(batch.num_columns(), 1, "Should have one column");

        // Verify the actual converted value matches our expected bytes
        let decimal_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();
        let converted_value = decimal_array.value(0);

        // Expected: i256 from our 32-byte pattern
        let expected_bytes = [
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11, 0x99, 0x88, 0x77, 0x66,
            0x55, 0x44, 0x33, 0x22,
        ];
        let expected_value = i256::from_be_bytes(expected_bytes);

        assert_eq!(
            converted_value, expected_value,
            "Converted large decimal(100,0) value should match expected byte pattern"
        );

        // Verify it's a positive number (since first bit is 0 in 0x12)
        assert!(
            converted_value > i256::from(0),
            "Should be a positive number"
        );
    }

    #[test]
    fn test_from_arrow_to_avro_converter_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let converter = FromArrowToAvroConverter::new(schema.clone(), "test_topic".to_string());

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), None, Some("Charlie")]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        let result = converter.convert_from_batch(&batch);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values.len(), 3);

        let expected_names = [Some("Alice"), None, Some("Charlie")];
        for (i, value) in values.iter().enumerate() {
            if let Value::Record(fields) = value {
                assert_eq!(fields.len(), 2);
                let field_map: HashMap<_, _> =
                    fields.iter().map(|(k, v)| (k.as_str(), v)).collect();

                assert!(field_map.contains_key("id"));
                assert!(field_map.contains_key("name"));

                assert_eq!(field_map["id"], &Value::Int((i + 1) as i32));

                match expected_names[i] {
                    Some(name) => assert_eq!(
                        field_map["name"],
                        &Value::Union(1, Box::new(Value::String(name.to_string())))
                    ),
                    None => assert_eq!(field_map["name"], &Value::Union(0, Box::new(Value::Null))),
                }
            } else {
                panic!("Expected Value::Record");
            }
        }
    }

    #[test]
    fn test_from_arrow_to_avro_converter_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let converter = FromArrowToAvroConverter::new(schema.clone(), "test_topic".to_string());

        let id_array = Int32Array::from(Vec::<i32>::new());
        let value_array = Float64Array::from(Vec::<Option<f64>>::new());

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(value_array)],
        )
        .unwrap();

        let result = converter.convert_from_batch(&batch);
        assert!(result.is_ok(), "Empty batch conversion should succeed");

        let values = result.unwrap();
        assert_eq!(values.len(), 0, "Empty batch should return empty values");
    }

    #[test]
    fn test_from_arrow_to_avro_converter_with_decimals() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Decimal256(10, 2), false),
            Field::new("quantity", DataType::Int32, false),
        ]));

        let converter = FromArrowToAvroConverter::new(schema.clone(), "test_topic".to_string());

        let price_data = vec![
            i256::from_i128(12345), // Represents 123.45 with scale 2
            i256::from_i128(67890), // Represents 678.90 with scale 2
        ];
        let price_array = Decimal256Array::from(price_data.clone())
            .with_precision_and_scale(10, 2)
            .unwrap();
        let quantity_array = Int32Array::from(vec![5, 10]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(price_array), Arc::new(quantity_array)],
        )
        .unwrap();

        let result = converter.convert_from_batch(&batch);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values.len(), 2);

        let expected_quantities = [5, 10];
        for (i, value) in values.iter().enumerate() {
            if let Value::Record(fields) = value {
                assert_eq!(fields.len(), 2);
                let field_map: HashMap<_, _> =
                    fields.iter().map(|(k, v)| (k.as_str(), v)).collect();

                assert!(field_map.contains_key("price"));
                assert!(field_map.contains_key("quantity"));

                assert_eq!(field_map["quantity"], &Value::Int(expected_quantities[i]));

                match field_map["price"] {
                    Value::Decimal(decimal_val) => {
                        let decimal_bytes: Vec<u8> = decimal_val.clone().try_into().unwrap();
                        assert!(!decimal_bytes.is_empty());

                        // Convert bytes back to i256 for comparison
                        let mut padded_bytes = [0u8; 32];
                        let start_idx = 32 - decimal_bytes.len();
                        padded_bytes[start_idx..].copy_from_slice(&decimal_bytes);
                        let actual_value = i256::from_be_bytes(padded_bytes);

                        assert_eq!(actual_value, price_data[i]);
                    }
                    _ => panic!("Expected Decimal value for price field"),
                }
            } else {
                panic!("Expected Value::Record");
            }
        }
    }

    #[test]
    fn test_from_arrow_to_avro_converter_multiple_data_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, false),
            Field::new("created_at", DataType::Int64, true), // timestamp as i64
        ]));

        let converter = FromArrowToAvroConverter::new(schema.clone(), "test_topic".to_string());

        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec![Some("Test User"), None]);
        let score_array = Float64Array::from(vec![Some(95.5), Some(87.2)]);
        let active_array = BooleanArray::from(vec![true, false]);
        let created_at_array = Int64Array::from(vec![Some(1640995200), None]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(score_array),
                Arc::new(active_array),
                Arc::new(created_at_array),
            ],
        )
        .unwrap();

        let result = converter.convert_from_batch(&batch);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values.len(), 2);

        let expected_data = [
            (1i32, Some("Test User"), 95.5, true, Some(1640995200i64)),
            (2i32, None, 87.2, false, None),
        ];

        for (i, value) in values.iter().enumerate() {
            if let Value::Record(fields) = value {
                assert_eq!(fields.len(), 5);
                let field_map: HashMap<_, _> =
                    fields.iter().map(|(k, v)| (k.as_str(), v)).collect();

                ["id", "name", "score", "active", "created_at"]
                    .iter()
                    .for_each(|field| assert!(field_map.contains_key(*field)));

                let (exp_id, exp_name, exp_score, exp_active, exp_created_at) = expected_data[i];

                assert_eq!(field_map["id"], &Value::Int(exp_id));
                assert_eq!(
                    field_map["score"],
                    &Value::Union(1, Box::new(Value::Double(exp_score)))
                );
                assert_eq!(field_map["active"], &Value::Boolean(exp_active));

                match exp_name {
                    Some(name) => assert_eq!(
                        field_map["name"],
                        &Value::Union(1, Box::new(Value::String(name.to_string())))
                    ),
                    None => assert_eq!(field_map["name"], &Value::Union(0, Box::new(Value::Null))),
                }

                match exp_created_at {
                    Some(timestamp) => assert_eq!(
                        field_map["created_at"],
                        &Value::Union(1, Box::new(Value::Long(timestamp)))
                    ),
                    None => assert_eq!(
                        field_map["created_at"],
                        &Value::Union(0, Box::new(Value::Null))
                    ),
                }
            } else {
                panic!("Expected Value::Record");
            }
        }
    }

    #[test]
    fn test_nullable_decimal128_with_nulls() {
        // Test Decimal128 with nullable field containing null values
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Decimal128(18, 2), true), // Nullable decimal
        ]));

        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}]}
                ]
            }
        "#,
        )
        .unwrap();

        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema, None);

        let data = vec![
            Value::Record(vec![
                ("id".to_string(), Value::Int(1)),
                (
                    "amount".to_string(),
                    Value::Union(
                        1,
                        Box::new(Value::Decimal(Decimal::from(vec![
                            0, 0, 0, 0, 0, 0, 0, 100,
                        ]))),
                    ),
                ),
            ]),
            Value::Record(vec![
                ("id".to_string(), Value::Int(2)),
                ("amount".to_string(), Value::Union(0, Box::new(Value::Null))), // Null value
            ]),
            Value::Record(vec![
                ("id".to_string(), Value::Int(3)),
                (
                    "amount".to_string(),
                    Value::Union(
                        1,
                        Box::new(Value::Decimal(Decimal::from(vec![
                            0, 0, 0, 0, 0, 0, 0, 200,
                        ]))),
                    ),
                ),
            ]),
        ];

        for value in data {
            converter.buffer(value);
        }

        let batch = converter.convert_to_batch().unwrap();

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let decimal_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        // Check IDs
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);

        // Check decimal values
        assert!(!decimal_array.is_null(0), "First row should not be null");
        assert_eq!(decimal_array.value(0), 100);

        assert!(decimal_array.is_null(1), "Second row should be null");

        assert!(!decimal_array.is_null(2), "Third row should not be null");
        assert_eq!(decimal_array.value(2), 200);
    }

    #[test]
    fn test_nullable_decimal256_with_nulls() {
        // Test Decimal256 with nullable field containing null values
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("balance", DataType::Decimal256(76, 18), true), // Nullable high-precision decimal
        ]));

        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "balance", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 76, "scale": 18}]}
                ]
            }
        "#,
        )
        .unwrap();

        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema, None);

        let data = vec![
            Value::Record(vec![
                ("id".to_string(), Value::Int(1)),
                (
                    "balance".to_string(),
                    Value::Union(
                        1,
                        Box::new(Value::Decimal(Decimal::from(vec![
                            5, 45, 35, 104, 51, 168, 97, 115, 93, 112, 0, 0,
                        ]))),
                    ),
                ),
            ]),
            Value::Record(vec![
                ("id".to_string(), Value::Int(2)),
                (
                    "balance".to_string(),
                    Value::Union(0, Box::new(Value::Null)),
                ), // Null value
            ]),
            Value::Record(vec![
                ("id".to_string(), Value::Int(3)),
                (
                    "balance".to_string(),
                    Value::Union(0, Box::new(Value::Null)),
                ), // Another null
            ]),
            Value::Record(vec![
                ("id".to_string(), Value::Int(4)),
                (
                    "balance".to_string(),
                    Value::Union(
                        1,
                        Box::new(Value::Decimal(Decimal::from(vec![
                            5, 45, 150, 140, 131, 193, 132, 221, 190, 168, 0, 0,
                        ]))),
                    ),
                ),
            ]),
        ];

        for value in data {
            converter.buffer(value);
        }

        let batch = converter.convert_to_batch().unwrap();

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 4);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let decimal_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();

        // Check IDs
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);
        assert_eq!(id_array.value(3), 4);

        // Check decimal values and nulls
        assert!(!decimal_array.is_null(0), "First row should not be null");
        assert_eq!(
            decimal_array.value(0),
            i256::from_i128(1601993916000000000000000000_i128)
        );

        assert!(decimal_array.is_null(1), "Second row should be null");
        assert!(decimal_array.is_null(2), "Third row should be null");

        assert!(!decimal_array.is_null(3), "Fourth row should not be null");
        assert_eq!(
            decimal_array.value(3),
            i256::from_i128(1602537658000000000000000000_i128)
        );
    }

    #[test]
    #[should_panic(expected = "Unexpected null value for non-nullable decimal field")]
    fn test_non_nullable_decimal128_with_null_panics() {
        // Test that non-nullable Decimal128 field with null value causes an error
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Decimal128(18, 2), false), // Non-nullable decimal
        ]));

        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}}
                ]
            }
        "#,
        )
        .unwrap();

        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema, None);

        // Try to insert a null value in a non-nullable field
        let data = vec![Value::Record(vec![
            ("id".to_string(), Value::Int(1)),
            ("amount".to_string(), Value::Null), // This should cause an error
        ])];

        for value in data {
            converter.buffer(value);
        }

        // This should panic
        converter.convert_to_batch().unwrap();
    }

    #[test]
    #[should_panic(expected = "Unexpected null value for non-nullable decimal field")]
    fn test_non_nullable_decimal256_with_null_panics() {
        // Test that non-nullable Decimal256 field with null value causes an error
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("balance", DataType::Decimal256(76, 18), false), // Non-nullable decimal
        ]));

        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "balance", "type": {"type": "bytes", "logicalType": "decimal", "precision": 76, "scale": 18}}
                ]
            }
        "#,
        )
        .unwrap();

        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema, None);

        // Try to insert a null value in a non-nullable field
        let data = vec![Value::Record(vec![
            ("id".to_string(), Value::Int(1)),
            ("balance".to_string(), Value::Null), // This should cause an error
        ])];

        for value in data {
            converter.buffer(value);
        }

        // This should panic
        converter.convert_to_batch().unwrap();
    }

    #[test]
    fn test_nullable_decimal128_all_nulls() {
        // Test Decimal128 with all null values
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Decimal128(18, 2), true),
        ]));

        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}]}
                ]
            }
        "#,
        )
        .unwrap();

        let mut converter = AvroToArrowConverter::new(schema.clone(), avro_schema, None);

        let data = vec![
            Value::Record(vec![
                ("id".to_string(), Value::Int(1)),
                ("amount".to_string(), Value::Union(0, Box::new(Value::Null))),
            ]),
            Value::Record(vec![
                ("id".to_string(), Value::Int(2)),
                ("amount".to_string(), Value::Union(0, Box::new(Value::Null))),
            ]),
        ];

        for value in data {
            converter.buffer(value);
        }

        let batch = converter.convert_to_batch().unwrap();

        let decimal_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        assert!(decimal_array.is_null(0), "First row should be null");
        assert!(decimal_array.is_null(1), "Second row should be null");
        assert_eq!(decimal_array.null_count(), 2);
    }

    #[test]
    fn test_large_decimal_to_string_conversion() {
        // The schema will be automatically converted to use Utf8 for this unsupported decimal type
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "unsupported_decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 77, "scale": 5}}
                ]
            }
        "#,
        )
        .unwrap();

        let arrow_schema = convert_avro_schema_to_arrow(avro_schema.clone());

        // Verify the schema conversion resulted in Utf8 type
        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(arrow_schema.field(1).name(), "unsupported_decimal");
        assert_eq!(
            arrow_schema.field(1).data_type(),
            &DataType::Utf8,
            "Decimal with precision > 76 and scale > 0 should fall back to Utf8"
        );

        let mut converter = AvroToArrowConverter::new(arrow_schema.clone(), avro_schema, None);

        // Using small decimal values for easier verification
        let data = vec![
            Value::Record(vec![
                ("id".to_string(), Value::Int(1)),
                (
                    "unsupported_decimal".to_string(),
                    // Represents 123.45000 with scale 5 (integer value: 12,345,000 = 0xBC5EA8)
                    Value::Decimal(Decimal::from(vec![0, 0, 0, 0, 0, 0, 0xBC, 0x5E, 0xA8])),
                ),
            ]),
            Value::Record(vec![
                ("id".to_string(), Value::Int(2)),
                (
                    "unsupported_decimal".to_string(),
                    // Represents 987.65000 with scale 5 (integer value: 98,765,000 = 0x05E308C8)
                    Value::Decimal(Decimal::from(vec![0, 0, 0, 0, 0x05, 0xE3, 0x08, 0xC8])),
                ),
            ]),
        ];

        for value in data {
            converter.buffer(value);
        }

        let batch = converter.convert_to_batch().unwrap();

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 2);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        let string_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Column should be StringArray since decimal precision > 76 with scale > 0");

        let value_0 = string_array.value(0);
        let value_1 = string_array.value(1);

        assert_eq!(value_0, "123.45");
        assert_eq!(value_1, "987.65");
    }

    // ===========================================
    // Schema Resolution Tests
    // ===========================================

    #[test]
    fn test_schema_resolution_same_schema() {
        // When writer and reader schemas are identical, resolve should be a no-op
        let schema_str = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"}
                ]
            }
        "#;
        let schema = AvroSchema::parse_str(schema_str).unwrap();

        let value = Value::Record(vec![
            ("id".to_string(), Value::Int(42)),
            ("name".to_string(), Value::String("test".to_string())),
        ]);

        let resolved = value.resolve(&schema).unwrap();

        if let Value::Record(fields) = resolved {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0], ("id".to_string(), Value::Int(42)));
            assert_eq!(
                fields[1],
                ("name".to_string(), Value::String("test".to_string()))
            );
        } else {
            panic!("Expected Record value");
        }
    }

    #[test]
    fn test_schema_resolution_new_field_with_default() {
        // Reader schema has a new field with a default value (backward compatibility)
        let _writer_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"}
                ]
            }
        "#,
        )
        .unwrap();

        let reader_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "version", "type": "int", "default": 1}
                ]
            }
        "#,
        )
        .unwrap();

        // Value written with writer schema (no "version" field)
        let value = Value::Record(vec![("id".to_string(), Value::Int(42))]);

        // Resolve to reader schema - should add default value for "version"
        let resolved = value.resolve(&reader_schema).unwrap();

        if let Value::Record(fields) = resolved {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0], ("id".to_string(), Value::Int(42)));
            assert_eq!(fields[1], ("version".to_string(), Value::Int(1))); // default value
        } else {
            panic!("Expected Record value");
        }
    }

    #[test]
    fn test_schema_resolution_new_nullable_field_with_null_default() {
        // Reader schema has a new nullable field with null default
        let reader_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "optional_field", "type": ["null", "string"], "default": null}
                ]
            }
        "#,
        )
        .unwrap();

        // Value without the optional field
        let value = Value::Record(vec![("id".to_string(), Value::Int(42))]);

        let resolved = value.resolve(&reader_schema).unwrap();

        if let Value::Record(fields) = resolved {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0], ("id".to_string(), Value::Int(42)));
            assert_eq!(
                fields[1],
                (
                    "optional_field".to_string(),
                    Value::Union(0, Box::new(Value::Null))
                )
            );
        } else {
            panic!("Expected Record value");
        }
    }

    #[test]
    fn test_schema_resolution_type_promotion_int_to_long() {
        // Reader expects long, writer wrote int (type promotion)
        let reader_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "count", "type": "long"}
                ]
            }
        "#,
        )
        .unwrap();

        // Value written as int
        let value = Value::Record(vec![("count".to_string(), Value::Int(100))]);

        let resolved = value.resolve(&reader_schema).unwrap();

        if let Value::Record(fields) = resolved {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0], ("count".to_string(), Value::Long(100)));
        } else {
            panic!("Expected Record value");
        }
    }

    #[test]
    fn test_schema_resolution_type_promotion_float_to_double() {
        // Reader expects double, writer wrote float (type promotion)
        let reader_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "value", "type": "double"}
                ]
            }
        "#,
        )
        .unwrap();

        // Value written as float
        let value = Value::Record(vec![("value".to_string(), Value::Float(2.5))]);

        let resolved = value.resolve(&reader_schema).unwrap();

        if let Value::Record(fields) = resolved {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].0, "value");
            if let Value::Double(d) = fields[0].1 {
                assert!((d - 2.5f64).abs() < 0.001);
            } else {
                panic!("Expected Double value");
            }
        } else {
            panic!("Expected Record value");
        }
    }

    #[test]
    fn test_schema_resolution_extra_field_in_writer_ignored() {
        // Writer has extra field that reader doesn't know about (forward compatibility)
        let reader_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"}
                ]
            }
        "#,
        )
        .unwrap();

        // Value with extra field "extra_data"
        let value = Value::Record(vec![
            ("id".to_string(), Value::Int(42)),
            (
                "extra_data".to_string(),
                Value::String("ignored".to_string()),
            ),
        ]);

        let resolved = value.resolve(&reader_schema).unwrap();

        if let Value::Record(fields) = resolved {
            assert_eq!(fields.len(), 1); // extra field should be dropped
            assert_eq!(fields[0], ("id".to_string(), Value::Int(42)));
        } else {
            panic!("Expected Record value");
        }
    }

    #[test]
    fn test_schema_resolution_missing_required_field_fails() {
        // Reader has required field without default that writer didn't provide
        let reader_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "required_field", "type": "string"}
                ]
            }
        "#,
        )
        .unwrap();

        // Value missing "required_field" with no default
        let value = Value::Record(vec![("id".to_string(), Value::Int(42))]);

        let result = value.resolve(&reader_schema);
        assert!(
            result.is_err(),
            "Should fail when required field without default is missing"
        );
    }

    #[test]
    fn test_schema_resolution_nested_record() {
        // Test schema resolution with nested records
        let reader_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "outer",
                "fields": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "inner",
                        "type": {
                            "type": "record",
                            "name": "inner_record",
                            "fields": [
                                {"name": "value", "type": "string"},
                                {"name": "count", "type": "int", "default": 0}
                            ]
                        }
                    }
                ]
            }
        "#,
        )
        .unwrap();

        // Inner record missing "count" field
        let value = Value::Record(vec![
            ("id".to_string(), Value::Int(1)),
            (
                "inner".to_string(),
                Value::Record(vec![(
                    "value".to_string(),
                    Value::String("nested".to_string()),
                )]),
            ),
        ]);

        let resolved = value.resolve(&reader_schema).unwrap();

        if let Value::Record(outer_fields) = resolved {
            assert_eq!(outer_fields.len(), 2);
            if let Value::Record(inner_fields) = &outer_fields[1].1 {
                assert_eq!(inner_fields.len(), 2);
                assert_eq!(
                    inner_fields[0],
                    ("value".to_string(), Value::String("nested".to_string()))
                );
                assert_eq!(inner_fields[1], ("count".to_string(), Value::Int(0))); // default
            } else {
                panic!("Expected nested Record");
            }
        } else {
            panic!("Expected Record value");
        }
    }

    #[test]
    fn test_schema_resolution_with_arrow_conversion() {
        // End-to-end test: resolve then convert to Arrow
        let _writer_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"}
                ]
            }
        "#,
        )
        .unwrap();

        let reader_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "version", "type": "int", "default": 1}
                ]
            }
        "#,
        )
        .unwrap();

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("version", DataType::Int32, false),
        ]));

        let mut converter =
            AvroToArrowConverter::new(arrow_schema.clone(), reader_schema.clone(), None);

        // Values written with writer schema
        let values = vec![
            Value::Record(vec![("id".to_string(), Value::Int(1))]),
            Value::Record(vec![("id".to_string(), Value::Int(2))]),
            Value::Record(vec![("id".to_string(), Value::Int(3))]),
        ];

        // Resolve each value to reader schema, then buffer
        for value in values {
            let resolved = value.resolve(&reader_schema).unwrap();
            converter.buffer(resolved);
        }

        let batch = converter.convert_to_batch().unwrap();

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let version_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Check IDs
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);

        // Check version (should all be default value 1)
        assert_eq!(version_array.value(0), 1);
        assert_eq!(version_array.value(1), 1);
        assert_eq!(version_array.value(2), 1);
    }
}
