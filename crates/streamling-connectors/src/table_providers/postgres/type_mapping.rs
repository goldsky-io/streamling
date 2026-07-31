use arrow_schema::Field;
use datafusion::arrow::datatypes::DataType;
use streamling_core::types::decimal_arb::DecimalArbType;
// Feature 002 (Retire U256/I256): U256/I256 imports removed.

/// PostgreSQL type information for an Arrow field
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresTypeInfo {
    /// PostgreSQL column type for CREATE TABLE (e.g., "NUMERIC(20,0)")
    pub column_type: String,
    /// If Some, bind as String and use this SQL cast expression (e.g., "numeric(20,0)").
    /// If None, bind as native Rust type.
    pub string_cast_sql: Option<String>,
}

/// Get PostgreSQL type information for an Arrow field
/// This is the single source of truth for Arrow → PostgreSQL type mapping
pub fn get_postgres_type_info(field: &Field) -> PostgresTypeInfo {
    // Feature 002 (Retire U256/I256): FSB(32)+U256/I256-metadata fields
    // no longer arrive here after the Phase 3 routing flip. Wide integers
    // flow through the decimal_arb branch below.

    // decimal_arb (LargeBinary + extension metadata) becomes NUMERIC(precision, scale).
    // Scale-aligned canonical bytes are pre-projected to canonical decimal strings
    // by `build_projection_for_postgres`, so the bind path sees Utf8 here.
    if let Some((precision, scale)) = DecimalArbType::precision_scale_from_field(field) {
        return PostgresTypeInfo {
            column_type: format!("NUMERIC({}, {})", precision, scale),
            string_cast_sql: Some(format!("numeric({},{})", precision, scale)),
        };
    }

    match field.data_type() {
        // UInt64 that becomes NUMERIC(20,0) - bind as string with cast
        DataType::UInt64 => PostgresTypeInfo {
            column_type: "NUMERIC(20,0)".to_string(),
            string_cast_sql: Some("numeric(20,0)".to_string()),
        },
        // Decimal128 that becomes NUMERIC(precision, scale) - bind as string with cast
        DataType::Decimal128(precision, scale) => PostgresTypeInfo {
            column_type: format!("NUMERIC({}, {})", precision, scale),
            string_cast_sql: Some(format!("numeric({},{})", precision, scale)),
        },
        // Decimal256 that becomes NUMERIC(precision, scale) - bind as string with cast
        DataType::Decimal256(precision, scale) => PostgresTypeInfo {
            column_type: format!("NUMERIC({}, {})", precision, scale),
            string_cast_sql: Some(format!("numeric({},{})", precision, scale)),
        },
        // Nested types that become JSONB - bind as string with cast
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _) => PostgresTypeInfo {
            column_type: "JSONB".to_string(),
            string_cast_sql: Some("jsonb".to_string()),
        },
        // Integer types - bind as native types
        DataType::Int8 => PostgresTypeInfo {
            column_type: "SMALLINT".to_string(),
            string_cast_sql: None,
        },
        DataType::Int16 => PostgresTypeInfo {
            column_type: "SMALLINT".to_string(),
            string_cast_sql: None,
        },
        DataType::Int32 => PostgresTypeInfo {
            column_type: "INTEGER".to_string(),
            string_cast_sql: None,
        },
        DataType::Int64 => PostgresTypeInfo {
            column_type: "BIGINT".to_string(),
            string_cast_sql: None,
        },
        // Unsigned integer types (except UInt64) - bind as native types
        DataType::UInt8 => PostgresTypeInfo {
            column_type: "INTEGER".to_string(),
            string_cast_sql: None,
        },
        DataType::UInt16 => PostgresTypeInfo {
            column_type: "INTEGER".to_string(),
            string_cast_sql: None,
        },
        DataType::UInt32 => PostgresTypeInfo {
            column_type: "BIGINT".to_string(),
            string_cast_sql: None,
        },
        // Boolean - bind as native type
        DataType::Boolean => PostgresTypeInfo {
            column_type: "BOOLEAN".to_string(),
            string_cast_sql: None,
        },
        // Float types - bind as native types
        DataType::Float16 | DataType::Float32 => PostgresTypeInfo {
            column_type: "REAL".to_string(),
            string_cast_sql: None,
        },
        DataType::Float64 => PostgresTypeInfo {
            column_type: "DOUBLE PRECISION".to_string(),
            string_cast_sql: None,
        },
        // String types - bind as string, no cast needed
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => PostgresTypeInfo {
            column_type: "TEXT".to_string(),
            string_cast_sql: None,
        },
        // Binary types - bind as Vec<u8>, no cast needed
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            PostgresTypeInfo {
                column_type: "BYTEA".to_string(),
                string_cast_sql: None,
            }
        }
        // Date types - bind as string with cast
        DataType::Date32 | DataType::Date64 => PostgresTypeInfo {
            column_type: "DATE".to_string(),
            string_cast_sql: Some("date".to_string()),
        },
        // Timestamp types - bind as string with cast
        DataType::Timestamp(_, _) => PostgresTypeInfo {
            column_type: "TIMESTAMP".to_string(),
            string_cast_sql: Some("timestamp".to_string()),
        },
        // Time types - bind as string with cast
        DataType::Time32(_) | DataType::Time64(_) => PostgresTypeInfo {
            column_type: "TIME".to_string(),
            string_cast_sql: Some("time".to_string()),
        },
        // Unknown types - default to TEXT, bind as string
        _ => {
            tracing::warn!(
                "Unmapped Arrow type {:?} for column '{}', defaulting to TEXT",
                field.data_type(),
                field.name()
            );
            PostgresTypeInfo {
                column_type: "TEXT".to_string(),
                string_cast_sql: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    #[test]
    fn test_uint64_mapping() {
        let field = Field::new("block_slot", DataType::UInt64, false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "NUMERIC(20,0)");
        assert_eq!(info.string_cast_sql, Some("numeric(20,0)".to_string()));
    }

    #[test]
    fn test_decimal128_mapping() {
        let field = Field::new("amount", DataType::Decimal128(10, 2), false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "NUMERIC(10, 2)");
        assert_eq!(info.string_cast_sql, Some("numeric(10,2)".to_string()));
    }

    #[test]
    fn test_decimal256_mapping() {
        let field = Field::new("value", DataType::Decimal256(30, 6), false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "NUMERIC(30, 6)");
        assert_eq!(info.string_cast_sql, Some("numeric(30,6)".to_string()));
    }

    // Feature 002: U256/I256 mapping tests deleted with the retired types.
    // Wide-int columns now route via the decimal_arb mapping test below.

    #[test]
    fn test_decimal_arb_mapping_to_numeric() {
        let field = DecimalArbType::field("amount", 100, 18, false).unwrap();
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "NUMERIC(100, 18)");
        assert_eq!(info.string_cast_sql, Some("numeric(100,18)".to_string()));
    }

    #[test]
    fn test_plain_large_binary_is_not_decimal_arb() {
        // Without the extension metadata, LargeBinary stays BYTEA.
        let field = Field::new("blob", DataType::LargeBinary, false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "BYTEA");
        assert_eq!(info.string_cast_sql, None);
    }

    #[test]
    fn test_int64_mapping() {
        let field = Field::new("id", DataType::Int64, false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "BIGINT");
        assert_eq!(info.string_cast_sql, None);
    }

    #[test]
    fn test_nested_types_mapping() {
        let empty_fields: Vec<Field> = vec![];
        let field = Field::new("struct", DataType::Struct(empty_fields.into()), false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "JSONB");
        assert_eq!(info.string_cast_sql, Some("jsonb".to_string()));
    }

    #[test]
    fn test_date_mapping() {
        let field = Field::new("date", DataType::Date32, false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "DATE");
        assert_eq!(info.string_cast_sql, Some("date".to_string()));
    }

    #[test]
    fn test_timestamp_mapping() {
        let field = Field::new(
            "ts",
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Second, None),
            false,
        );
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "TIMESTAMP");
        assert_eq!(info.string_cast_sql, Some("timestamp".to_string()));
    }

    #[test]
    fn test_string_mapping() {
        let field = Field::new("text", DataType::Utf8, false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "TEXT");
        assert_eq!(info.string_cast_sql, None);
    }

    #[test]
    fn test_utf8view_mapping() {
        let field = Field::new("text_view", DataType::Utf8View, false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "TEXT");
        assert_eq!(info.string_cast_sql, None);
    }

    #[test]
    fn test_boolean_mapping() {
        let field = Field::new("flag", DataType::Boolean, false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "BOOLEAN");
        assert_eq!(info.string_cast_sql, None);
    }

    #[test]
    fn test_float_mapping() {
        let field = Field::new("price", DataType::Float64, false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "DOUBLE PRECISION");
        assert_eq!(info.string_cast_sql, None);
    }

    #[test]
    fn test_binary_mapping() {
        let field = Field::new("bin", DataType::Binary, false);
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "BYTEA");
        assert_eq!(info.string_cast_sql, None);
    }

    #[test]
    fn test_unknown_type_defaults_to_text() {
        let field = Field::new(
            "unknown",
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::DayTime),
            false,
        );
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "TEXT");
        assert_eq!(info.string_cast_sql, None);
    }
}
