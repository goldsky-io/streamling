pub mod arrow_avro;
mod schema;
mod writer;

use crate::formats::FromArrowConverter;
pub use crate::formats::avro::schema::convert_avro_schema_to_arrow;
pub use crate::formats::avro::schema::post_process_avro_schema_for_reading;
pub use crate::formats::avro::schema::post_process_avro_schema_for_writing;
pub use crate::formats::avro::writer::{serialize, to_avro};
use apache_avro::types::Value;
use arrow_schema::SchemaRef;
use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result;

// Maximum precision we allow in schema definitions (100 digits). Used by `schema.rs`.
const MAX_SCHEMA_PRECISION: usize = 100;

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
    use apache_avro::Schema as AvroSchema;
    use apache_avro::types::Value;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::i256;
    use std::collections::HashMap;
    use std::sync::Arc;

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
}
