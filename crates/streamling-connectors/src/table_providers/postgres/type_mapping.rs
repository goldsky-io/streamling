use arrow_schema::Field;

pub use streamling_core::utils::pg::PostgresTypeInfo;

/// Get PostgreSQL type information for an Arrow field.
///
/// Delegates to [`streamling_core::utils::pg::get_postgres_type_info`], which
/// the Postgres sink's DDL pass also uses. These were two copies of the same
/// match, and they drifted: this one grew the legacy wide-int arm and the
/// other did not, so a `streamling.u256` column got `BYTEA` in CREATE TABLE
/// and a `numeric(78,0)` cast on insert, failing every insert with 42804.
/// One function now serves both paths so they cannot disagree again.
pub fn get_postgres_type_info(field: &Field) -> PostgresTypeInfo {
    streamling_core::utils::pg::get_postgres_type_info(field)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::arrow::datatypes::DataType;
    use streamling_core::types::decimal_arb::DecimalArbType;

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

    // U256/I256 mapping tests were deleted with the retired types.
    // Wide-int columns now route via the decimal_arb mapping test below.

    #[test]
    fn test_decimal_arb_mapping_to_numeric() {
        let field = DecimalArbType::field("amount", 100, 18, false).unwrap();
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "NUMERIC(100, 18)");
        assert_eq!(info.string_cast_sql, Some("numeric(100,18)".to_string()));
    }

    #[test]
    fn test_legacy_wide_int_maps_to_numeric() {
        // A retired plugin `streamling.u256` column is a NUMERIC(78, 0), not
        // the BYTEA a bare FixedSizeBinary(32) would be.
        let field = Field::new("balance", DataType::FixedSizeBinary(32), true).with_metadata(
            std::collections::HashMap::from([(
                "ARROW:extension:name".to_string(),
                "streamling.u256".to_string(),
            )]),
        );
        let info = get_postgres_type_info(&field);
        assert_eq!(info.column_type, "NUMERIC(78, 0)");
        assert_eq!(info.string_cast_sql, Some("numeric(78,0)".to_string()));
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
