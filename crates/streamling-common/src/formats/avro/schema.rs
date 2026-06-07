use crate::formats::avro::MAX_SCHEMA_PRECISION;
use crate::types::u256::U256Type;
use apache_avro::schema::{
    DecimalSchema, RecordField, RecordSchema, Schema as AvroSchema, UnionSchema,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_datasource_avro::avro_to_arrow::to_arrow_schema;
use serde_json::json;
use std::sync::Arc;

// Helper function to find decimal schema, potentially nested in unions
fn find_decimal_schema(schema: &AvroSchema) -> Option<&DecimalSchema> {
    match schema {
        AvroSchema::Decimal(decimal_schema) => Some(decimal_schema),
        AvroSchema::Union(union) => {
            // Look for decimal in union variants
            for variant in union.variants() {
                if let Some(decimal) = find_decimal_schema(variant) {
                    return Some(decimal);
                }
            }
            None
        }
        _ => None,
    }
}

pub fn convert_avro_schema_to_arrow(root_avro_schema: AvroSchema) -> SchemaRef {
    let updated_avro_schema = post_process_avro_schema_for_reading(root_avro_schema);

    let arrow_schema = to_arrow_schema(&updated_avro_schema).unwrap();

    // Update decimal fields to use appropriate Arrow decimal type or extension types for u256/i256
    let updated_fields: Vec<Arc<Field>> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            // Check if this field corresponds to a decimal in the Avro schema
            // If so, convert to the right Arrow decimal type based on precision.
            if let AvroSchema::Record(record) = &updated_avro_schema
                && let Some(avro_field) = record.fields.get(i)
                && let Some(decimal_schema) = find_decimal_schema(&avro_field.schema)
            {
                return match (decimal_schema.precision, decimal_schema.scale) {
                    // Assuming it's UInt256 based on blockchain context, but ideally we have a schema override
                    // in the kafka source config that will let us make a better decision. When that happens we can
                    // default to I256Type instead and maybe introduce 512 byte types.
                    (p, 0) if p > 76 => {
                        // Unsigned 256-bit integer - use U256 type with metadata
                        Arc::new(
                            Field::new(field.name(), U256Type::new(), field.is_nullable())
                                .with_metadata(U256Type::metadata()),
                        )
                    }
                    (p, s) if p > 38 && p <= 76 => {
                        // Use Decimal256 for high precision decimals (up to 76 digits)
                        Arc::new(Field::new(
                            field.name(),
                            DataType::Decimal256(p as u8, s as i8),
                            field.is_nullable(),
                        ))
                    }
                    (p, s) if p <= 38 => {
                        // Use Decimal128 for standard precision
                        Arc::new(Field::new(
                            field.name(),
                            DataType::Decimal128(p as u8, s as i8),
                            field.is_nullable(),
                        ))
                    }
                    _ => Arc::new(Field::new(
                        field.name(),
                        DataType::Utf8,
                        field.is_nullable(),
                    )),
                };
            }
            field.clone()
        })
        .collect();

    SchemaRef::new(Schema::new(updated_fields))
}

pub fn post_process_avro_schema_for_reading(root_avro_schema: AvroSchema) -> AvroSchema {
    // potentially extract the record schema from the union
    let root_avro_schema = match root_avro_schema {
        // we expect to see a record in a union
        AvroSchema::Union(union) => {
            let mut record_schema = None;
            for schema in union.variants() {
                if let AvroSchema::Record(record) = schema {
                    record_schema = Some(record);
                    break;
                }
            }
            AvroSchema::Record(record_schema.unwrap().clone())
        }
        _ => root_avro_schema,
    };

    // patch the decimal schema to have appropriate precision and scale limits based on the decimal type
    match root_avro_schema {
        AvroSchema::Record(record) => {
            let fields: Vec<RecordField> = record
                .fields
                .iter()
                .map(|field| {
                    let new_field_schema = match field.schema.clone() {
                        AvroSchema::Decimal(decimal) => {
                            // Validate precision doesn't exceed our schema limit
                            if decimal.precision > MAX_SCHEMA_PRECISION {
                                panic!(
                                    "Decimal precision {} exceeds maximum supported precision of {} for schema definition",
                                    decimal.precision, MAX_SCHEMA_PRECISION
                                );
                            }

                            // Use original precision and scale if within schema limits
                            AvroSchema::Decimal(DecimalSchema {
                                precision: decimal.precision,
                                scale: if decimal.scale > MAX_SCHEMA_PRECISION {
                                    MAX_SCHEMA_PRECISION
                                } else {
                                    decimal.scale
                                },
                                inner: decimal.inner,
                            })
                        }
                        _ => field.schema.clone(),
                    };

                    // copy everything, but replace the field schema
                    RecordField {
                        name: field.name.clone(),
                        doc: field.doc.clone(),
                        default: field.default.clone(),
                        aliases: field.aliases.clone(),
                        schema: new_field_schema,
                        order: field.order.clone(),
                        position: field.position,
                        custom_attributes: field.custom_attributes.clone(),
                    }
                })
                .collect();
            // copy everything, but replace the fields
            AvroSchema::Record(RecordSchema {
                name: record.name.clone(),
                aliases: record.aliases.clone(),
                doc: record.doc.clone(),
                fields,
                lookup: record.lookup.clone(),
                attributes: record.attributes.clone(),
            })
        }
        _ => root_avro_schema,
    }
}

pub fn post_process_avro_schema_for_writing(
    root_avro_schema: AvroSchema,
    primary_key: Option<String>,
) -> AvroSchema {
    // potentially wrap the record schema in a union
    match root_avro_schema {
        AvroSchema::Record(mut record) => {
            // If primary key is provided, encode it in the doc field as JSON
            // Format: {"primaryKeys": ["col1", "col2"]}
            if let Some(pk) = primary_key {
                let keys: Vec<&str> = pk.split(',').map(|s| s.trim()).collect();
                let doc_json = json!({
                    "primaryKeys": keys
                });
                record.doc = Some(doc_json.to_string());
            }

            AvroSchema::Union(
                UnionSchema::new(vec![AvroSchema::Null, AvroSchema::Record(record)]).unwrap(),
            )
        }
        _ => root_avro_schema,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::schema::Name;
    use arrow_schema::DataType;

    #[test]
    fn test_convert_avro_schema_basic_types() {
        // Test basic Avro types conversion to Arrow
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "int_field", "type": "int"},
                    {"name": "long_field", "type": "long"},
                    {"name": "string_field", "type": "string"},
                    {"name": "boolean_field", "type": "boolean"}
                ]
            }
        "#,
        )
        .unwrap();

        let arrow_schema = convert_avro_schema_to_arrow(avro_schema);

        assert_eq!(arrow_schema.fields().len(), 4);
        assert_eq!(arrow_schema.field(0).name(), "int_field");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(arrow_schema.field(1).name(), "long_field");
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Int64);
        assert_eq!(arrow_schema.field(2).name(), "string_field");
        assert_eq!(arrow_schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field(3).name(), "boolean_field");
        assert_eq!(arrow_schema.field(3).data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_convert_avro_schema_decimal128() {
        // Test decimal with precision <= 38 converts to Decimal128
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "small_decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                    {"name": "max_decimal128", "type": {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 10}}
                ]
            }
        "#,
        )
        .unwrap();

        let arrow_schema = convert_avro_schema_to_arrow(avro_schema);

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "small_decimal");
        assert_eq!(
            arrow_schema.field(0).data_type(),
            &DataType::Decimal128(10, 2)
        );
        assert_eq!(arrow_schema.field(1).name(), "max_decimal128");
        assert_eq!(
            arrow_schema.field(1).data_type(),
            &DataType::Decimal128(38, 10)
        );
    }

    #[test]
    fn test_convert_avro_schema_decimal256() {
        // Test decimal with 38 < precision <= 76 converts to Decimal256
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "min_decimal256", "type": {"type": "bytes", "logicalType": "decimal", "precision": 39, "scale": 5}},
                    {"name": "mid_decimal256", "type": {"type": "bytes", "logicalType": "decimal", "precision": 50, "scale": 10}},
                    {"name": "max_decimal256", "type": {"type": "bytes", "logicalType": "decimal", "precision": 76, "scale": 18}}
                ]
            }
        "#,
        )
        .unwrap();

        let arrow_schema = convert_avro_schema_to_arrow(avro_schema);

        assert_eq!(arrow_schema.fields().len(), 3);
        assert_eq!(arrow_schema.field(0).name(), "min_decimal256");
        assert_eq!(
            arrow_schema.field(0).data_type(),
            &DataType::Decimal256(39, 5)
        );
        assert_eq!(arrow_schema.field(1).name(), "mid_decimal256");
        assert_eq!(
            arrow_schema.field(1).data_type(),
            &DataType::Decimal256(50, 10)
        );
        assert_eq!(arrow_schema.field(2).name(), "max_decimal256");
        assert_eq!(
            arrow_schema.field(2).data_type(),
            &DataType::Decimal256(76, 18)
        );
    }

    #[test]
    fn test_convert_avro_schema_u256() {
        // Test decimal with precision > 76 and scale = 0 converts to U256Type
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "u256_field", "type": {"type": "bytes", "logicalType": "decimal", "precision": 77, "scale": 0}},
                    {"name": "large_u256", "type": {"type": "bytes", "logicalType": "decimal", "precision": 100, "scale": 0}}
                ]
            }
        "#,
        )
        .unwrap();

        let arrow_schema = convert_avro_schema_to_arrow(avro_schema);

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "u256_field");
        assert_eq!(arrow_schema.field(0).data_type(), &U256Type::new());
        assert_eq!(arrow_schema.field(0).metadata(), &U256Type::metadata());
        assert_eq!(arrow_schema.field(1).name(), "large_u256");
        assert_eq!(arrow_schema.field(1).data_type(), &U256Type::new());
    }

    #[test]
    fn test_convert_avro_schema_decimal_with_scale_to_string() {
        // Test decimal with precision > 76 and scale > 0 falls back to Utf8
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "unsupported_decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 77, "scale": 1}}
                ]
            }
        "#,
        )
        .unwrap();

        let arrow_schema = convert_avro_schema_to_arrow(avro_schema);

        assert_eq!(arrow_schema.fields().len(), 1);
        assert_eq!(arrow_schema.field(0).name(), "unsupported_decimal");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_convert_avro_schema_nullable_fields() {
        // Test nullable fields (union with null)
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "nullable_string", "type": ["null", "string"]},
                    {"name": "nullable_decimal", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 5}]}
                ]
            }
        "#,
        )
        .unwrap();

        let arrow_schema = convert_avro_schema_to_arrow(avro_schema);

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "nullable_string");
        assert!(arrow_schema.field(0).is_nullable());
        assert_eq!(arrow_schema.field(1).name(), "nullable_decimal");
        assert!(arrow_schema.field(1).is_nullable());
        assert_eq!(
            arrow_schema.field(1).data_type(),
            &DataType::Decimal128(20, 5)
        );
    }

    #[test]
    fn test_convert_avro_schema_mixed_fields() {
        // Test schema with mix of regular and decimal fields
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 2}},
                    {"name": "balance", "type": {"type": "bytes", "logicalType": "decimal", "precision": 76, "scale": 18}},
                    {"name": "large_int", "type": {"type": "bytes", "logicalType": "decimal", "precision": 100, "scale": 0}},
                    {"name": "active", "type": "boolean"}
                ]
            }
        "#,
        )
        .unwrap();

        let arrow_schema = convert_avro_schema_to_arrow(avro_schema);

        assert_eq!(arrow_schema.fields().len(), 6);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            arrow_schema.field(2).data_type(),
            &DataType::Decimal128(18, 2)
        );
        assert_eq!(
            arrow_schema.field(3).data_type(),
            &DataType::Decimal256(76, 18)
        );
        assert_eq!(arrow_schema.field(4).data_type(), &U256Type::new());
        assert_eq!(arrow_schema.field(5).data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_convert_avro_schema_from_union() {
        // Test that union schemas are properly extracted
        let avro_schema = AvroSchema::parse_str(
            r#"
            ["null", {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "value", "type": "int"}
                ]
            }]
        "#,
        )
        .unwrap();

        let arrow_schema = convert_avro_schema_to_arrow(avro_schema);

        assert_eq!(arrow_schema.fields().len(), 1);
        assert_eq!(arrow_schema.field(0).name(), "value");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
    }

    #[test]
    #[should_panic(expected = "Decimal precision 101 exceeds maximum supported precision of 100")]
    fn test_convert_avro_schema_precision_exceeds_max() {
        // Test that precision > MAX_SCHEMA_PRECISION (100) panics
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "too_large", "type": {"type": "bytes", "logicalType": "decimal", "precision": 101, "scale": 0}}
                ]
            }
        "#,
        )
        .unwrap();

        // This should panic during post-processing
        let _arrow_schema = convert_avro_schema_to_arrow(avro_schema);
    }

    #[test]
    fn test_convert_avro_schema_scale_clamping() {
        // Test that scale > MAX_SCHEMA_PRECISION gets clamped to MAX_SCHEMA_PRECISION
        let decimal_schema = DecimalSchema {
            precision: 50,
            scale: 150, // Exceeds MAX_SCHEMA_PRECISION (100)
            inner: Box::new(AvroSchema::Bytes),
        };

        let record = RecordSchema {
            name: Name::new("test").unwrap(),
            aliases: None,
            doc: None,
            fields: vec![RecordField {
                name: "clamped_scale".to_string(),
                doc: None,
                aliases: None,
                default: None,
                schema: AvroSchema::Decimal(decimal_schema),
                order: apache_avro::schema::RecordFieldOrder::Ascending,
                position: 0,
                custom_attributes: std::collections::BTreeMap::new(),
            }],
            lookup: std::collections::BTreeMap::new(),
            attributes: std::collections::BTreeMap::new(),
        };

        let processed = post_process_avro_schema_for_reading(AvroSchema::Record(record));

        // Verify scale was clamped
        if let AvroSchema::Record(record) = processed {
            if let AvroSchema::Decimal(decimal) = &record.fields[0].schema {
                assert_eq!(decimal.precision, 50);
                assert_eq!(decimal.scale, MAX_SCHEMA_PRECISION);
            } else {
                panic!("Expected decimal schema");
            }
        } else {
            panic!("Expected record schema");
        }
    }
}
