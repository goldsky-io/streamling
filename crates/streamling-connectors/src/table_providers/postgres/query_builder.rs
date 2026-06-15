use arrow_schema::{Field, SchemaRef};
use std::collections::BTreeMap;
use streamling_core::data::COLUMN_NAME_OP;
use streamling_core::error::Result;
use streamling_core::streamling_user_bail;

use crate::table_providers::postgres::type_mapping::get_postgres_type_info;

pub const ALLOWED_UPDATE_WHERE_OPS: &[&str] = &["=", ">", ">=", "<", "<=", "!=", "<>"];

/// Validate that all `update_where` columns exist in the schema and all operators
/// are in the allowed list. Call this at pipeline startup to fail fast on bad config.
pub fn validate_update_where(
    update_where: &BTreeMap<String, String>,
    schema: &SchemaRef,
    sink_name: &str,
) -> Result<()> {
    let schema_fields: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    for (col, op) in update_where {
        if !schema_fields.contains(col) {
            streamling_user_bail!(
                "update_where validation failed for sink '{}': column '{}' not found in schema. Available columns: {:?}",
                sink_name,
                col,
                schema_fields
            );
        }
        let op = op.trim();
        if !ALLOWED_UPDATE_WHERE_OPS.contains(&op) {
            streamling_user_bail!(
                "update_where validation failed for sink '{}': invalid operator '{}' for column '{}'. Allowed operators: {:?}",
                sink_name,
                op,
                col,
                ALLOWED_UPDATE_WHERE_OPS
            );
        }
    }
    Ok(())
}

/// Build INSERT ... ON CONFLICT UPDATE SQL query
pub struct PostgresQueryBuilder;

impl PostgresQueryBuilder {
    /// Determine if a field needs a SQL cast based on its original Arrow type
    /// Fields that were converted to Utf8 for insertion but need casting back to their PostgreSQL type
    /// Returns Some(String) with the cast type, or None if no cast is needed
    fn field_needs_sql_cast(field: &Field) -> Option<String> {
        get_postgres_type_info(field).string_cast_sql
    }

    /// Build a map of column names to their SQL cast expressions
    /// Based on the original schema before transformation
    pub fn build_cast_map(
        original_schema: &SchemaRef,
        column_names: &[String],
    ) -> std::collections::HashMap<String, Option<String>> {
        original_schema
            .fields()
            .iter()
            .filter(|f| f.name() != COLUMN_NAME_OP && column_names.contains(&f.name().to_string()))
            .map(|f| {
                let cast_type = Self::field_needs_sql_cast(f);
                (f.name().to_string(), cast_type)
            })
            .collect()
    }
    /// Build a WHERE clause from an update_where map.
    /// Each entry (column, operator) becomes: EXCLUDED."column" op "table"."column"
    /// Multiple entries are ANDed. Invalid operators are skipped with a warning.
    fn build_update_where_clause(
        update_where: Option<&BTreeMap<String, String>>,
        target_table: &str,
    ) -> String {
        let conditions: Vec<String> = update_where
            .into_iter()
            .flatten()
            .filter_map(|(col, op)| {
                let op = op.trim();
                if ALLOWED_UPDATE_WHERE_OPS.contains(&op) {
                    Some(format!(
                        r#"EXCLUDED."{}" {} "{}"."{}""#,
                        col, op, target_table, col
                    ))
                } else {
                    tracing::warn!(
                        "Ignoring invalid update_where operator '{}' for column '{}' (allowed: {:?})",
                        op, col, ALLOWED_UPDATE_WHERE_OPS
                    );
                    None
                }
            })
            .collect();

        if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        }
    }

    /// Build INSERT ... ON CONFLICT UPDATE query for batch insert
    ///
    /// Returns (query_string, num_placeholders_per_row)
    /// on_conflict: "update" for DO UPDATE SET, "nothing" for DO NOTHING
    /// update_where: optional map of column->operator conditions ANDed into a WHERE clause
    ///   on DO UPDATE SET (e.g. {"updated_at": ">"} becomes WHERE EXCLUDED."updated_at" > "table"."updated_at")
    pub fn build_upsert_query(
        schema: &str,
        table: &str,
        column_names: &[String],
        primary_key_columns: &[String],
        on_conflict: &str,
        update_where: Option<&BTreeMap<String, String>>,
        target_table: &str,
    ) -> (String, usize) {
        let quoted_columns: Vec<String> =
            column_names.iter().map(|c| format!(r#""{}""#, c)).collect();

        let quoted_pk_columns: Vec<String> = primary_key_columns
            .iter()
            .map(|c| format!(r#""{}""#, c))
            .collect();

        // Columns to update (all except primary keys)
        let update_columns: Vec<String> = column_names
            .iter()
            .filter(|c| !primary_key_columns.contains(c))
            .map(|c| format!(r#""{}" = EXCLUDED."{}""#, c, c))
            .collect();

        let num_placeholders_per_row = column_names.len();

        let query = if primary_key_columns.is_empty() {
            // Simple INSERT if no primary key (VALUES placeholder will be filled in)
            format!(
                r#"INSERT INTO "{}"."{}" ({}) VALUES {}"#,
                schema,
                table,
                quoted_columns.join(", "),
                "$VALUES_PLACEHOLDER"
            )
        } else {
            // INSERT ... ON CONFLICT. With no non-PK columns there is nothing to
            // update, so an `update` conflict must degrade to DO NOTHING —
            // otherwise the SET clause is empty and Postgres rejects the query
            // with "syntax error at end of input".
            if on_conflict == "nothing" || update_columns.is_empty() {
                format!(
                    r#"INSERT INTO "{}"."{}" ({}) VALUES {}
                    ON CONFLICT ({}) DO NOTHING"#,
                    schema,
                    table,
                    quoted_columns.join(", "),
                    // VALUES placeholder will be filled in when building query
                    "$VALUES_PLACEHOLDER",
                    quoted_pk_columns.join(", ")
                )
            } else {
                let where_clause = Self::build_update_where_clause(update_where, target_table);
                format!(
                    r#"INSERT INTO "{}"."{}" ({}) VALUES {}
                    ON CONFLICT ({}) DO UPDATE SET {}{}"#,
                    schema,
                    table,
                    quoted_columns.join(", "),
                    "$VALUES_PLACEHOLDER",
                    quoted_pk_columns.join(", "),
                    update_columns.join(", "),
                    where_clause
                )
            }
        };

        (query, num_placeholders_per_row)
    }

    /// Build VALUES clause with placeholders for a batch of rows
    /// If cast_map is provided, adds SQL casts for columns that need them (e.g., ::numeric(78,0), ::jsonb)
    pub fn build_values_clause(
        num_rows: usize,
        num_placeholders_per_row: usize,
        column_names: &[String],
        cast_map: &std::collections::HashMap<String, Option<String>>,
    ) -> String {
        let mut values = Vec::new();
        let mut placeholder_num = 1;

        for _ in 0..num_rows {
            let row_placeholders: Vec<String> = (0..num_placeholders_per_row)
                .map(|idx| {
                    let needs_cast = column_names
                        .get(idx)
                        .and_then(|name| cast_map.get(name))
                        .as_ref()
                        .and_then(|opt| opt.as_ref());
                    let ph = if let Some(cast_type) = needs_cast {
                        format!("${}::{}", placeholder_num, cast_type)
                    } else {
                        format!("${}", placeholder_num)
                    };
                    placeholder_num += 1;
                    ph
                })
                .collect();
            values.push(format!("({})", row_placeholders.join(", ")));
        }

        values.join(", ")
    }

    /// Build complete INSERT query with VALUES clause
    /// If original_schema is provided, adds SQL casts for columns that need them
    /// on_conflict: "update" for DO UPDATE SET, "nothing" for DO NOTHING
    /// update_where: optional map of column->operator conditions for conditional update
    /// target_table: the destination table name (used in WHERE clause column references)
    /// If checkpoint_epoch is provided, adds _gs_checkpoint_epoch column
    pub fn build_complete_upsert_query(
        schema: &str,
        table: &str,
        column_names: &[String],
        primary_key_columns: &[String],
        num_rows: usize,
        original_schema: Option<&SchemaRef>,
        on_conflict: &str,
        update_where: Option<&BTreeMap<String, String>>,
        target_table: &str,
        checkpoint_epoch: Option<u64>,
    ) -> String {
        // If checkpoint_epoch is provided, add _gs_checkpoint_epoch to column list
        let mut column_names_with_epoch = column_names.to_vec();
        if checkpoint_epoch.is_some() {
            column_names_with_epoch.push("_gs_checkpoint_epoch".to_string());
        }

        let (query_template, num_placeholders_per_row) = Self::build_upsert_query(
            schema,
            table,
            &column_names_with_epoch,
            primary_key_columns,
            on_conflict,
            update_where,
            target_table,
        );

        // Build cast map from original schema if provided
        let cast_map = if let Some(schema_ref) = original_schema {
            Self::build_cast_map(schema_ref, column_names)
        } else {
            std::collections::HashMap::new()
        };

        let values_clause = Self::build_values_clause(
            num_rows,
            num_placeholders_per_row,
            &column_names_with_epoch,
            &cast_map,
        );

        query_template.replace("$VALUES_PLACEHOLDER", &values_clause)
    }

    /// Build DELETE query for rows matching primary key values
    /// Returns (query_string, num_placeholders_per_row)
    pub fn build_delete_query(
        schema: &str,
        table: &str,
        primary_key_columns: &[String],
        num_rows: usize,
    ) -> String {
        if primary_key_columns.is_empty() {
            // Without primary key, we can't safely delete - this shouldn't happen
            // but we'll return an empty query
            return String::new();
        }

        let quoted_pk_columns: Vec<String> = primary_key_columns
            .iter()
            .map(|c| format!(r#""{}""#, c))
            .collect();

        // Build WHERE clause conditions: (pk1, pk2, ...) IN (($1, $2, ...), ($3, $4, ...), ...)
        let mut placeholder_num = 1;
        let mut values = Vec::new();

        for _ in 0..num_rows {
            let row_placeholders: Vec<String> = (0..primary_key_columns.len())
                .map(|_| {
                    let ph = format!("${}", placeholder_num);
                    placeholder_num += 1;
                    ph
                })
                .collect();
            values.push(format!("({})", row_placeholders.join(", ")));
        }

        let pk_tuple = format!("({})", quoted_pk_columns.join(", "));
        let values_clause = values.join(", ");

        format!(
            r#"DELETE FROM "{}"."{}" WHERE {} IN ({})"#,
            schema, table, pk_tuple, values_clause
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    #[test]
    fn test_build_upsert_query_with_primary_key() {
        let columns = vec!["id".to_string(), "name".to_string(), "value".to_string()];
        let pk_columns = vec!["id".to_string()];

        let (query, placeholders) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            "update",
            None,
            "test",
        );

        assert_eq!(placeholders, 3);
        assert!(query.contains("INSERT INTO"));
        assert!(query.contains(r#""id", "name", "value""#));
        assert!(query.contains("ON CONFLICT"));
        assert!(query.contains(r#""id""#));
        assert!(query.contains(r#""name" = EXCLUDED."name""#));
        assert!(query.contains(r#""value" = EXCLUDED."value""#));
    }

    #[test]
    fn test_build_upsert_query_pk_only_columns_falls_back_to_do_nothing() {
        // When every column is part of the primary key there is nothing to
        // update, so `on_conflict: update` must not emit an empty `DO UPDATE SET`
        // (which is invalid SQL — `syntax error at end of input`). It must fall
        // back to `DO NOTHING`.
        let columns = vec!["id".to_string()];
        let pk_columns = vec!["id".to_string()];

        let (query, placeholders) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            "update",
            None,
            "test",
        );

        assert_eq!(placeholders, 1);
        assert!(query.contains("ON CONFLICT"));
        assert!(query.contains("DO NOTHING"), "query was: {query}");
        assert!(!query.contains("DO UPDATE SET"), "query was: {query}");
    }

    #[test]
    fn test_build_upsert_query_without_primary_key() {
        let columns = vec!["name".to_string(), "value".to_string()];
        let pk_columns = vec![];

        let (query, placeholders) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            "update",
            None,
            "test",
        );

        assert_eq!(placeholders, 2);
        assert!(query.contains("INSERT INTO"));
        assert!(!query.contains("ON CONFLICT"));
    }

    #[test]
    fn test_build_values_clause() {
        let columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let cast_map = std::collections::HashMap::new();
        let clause = PostgresQueryBuilder::build_values_clause(2, 3, &columns, &cast_map);
        assert_eq!(clause, "($1, $2, $3), ($4, $5, $6)");
    }

    #[test]
    fn test_build_values_clause_with_casts() {
        let columns = vec![
            "id".to_string(),
            "value".to_string(),
            "metadata".to_string(),
        ];
        let mut cast_map = std::collections::HashMap::new();
        cast_map.insert("value".to_string(), Some("numeric(78,0)".to_string()));
        cast_map.insert("metadata".to_string(), Some("jsonb".to_string()));
        let clause = PostgresQueryBuilder::build_values_clause(1, 3, &columns, &cast_map);
        assert_eq!(clause, "($1, $2::numeric(78,0), $3::jsonb)");
    }

    #[test]
    fn test_build_complete_upsert_query() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let pk_columns = vec!["id".to_string()];

        let query = PostgresQueryBuilder::build_complete_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            2,
            None,
            "update",
            None,
            "test",
            None,
        );

        assert!(query.contains("($1, $2), ($3, $4)"));
        assert!(query.contains("ON CONFLICT"));
        assert!(query.contains("DO UPDATE SET"));
    }

    #[test]
    fn test_build_cast_map_uint64() {
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("block_slot", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let column_names = vec![
            "id".to_string(),
            "block_slot".to_string(),
            "name".to_string(),
        ];
        let cast_map = PostgresQueryBuilder::build_cast_map(&schema, &column_names);

        // block_slot (UInt64) should get numeric(20,0) cast
        assert_eq!(
            cast_map.get("block_slot"),
            Some(&Some("numeric(20,0)".to_string()))
        );
        // id (Int64) should not need a cast
        assert_eq!(cast_map.get("id"), Some(&None));
        // name (Utf8) should not need a cast
        assert_eq!(cast_map.get("name"), Some(&None));
    }

    #[test]
    fn test_build_cast_map_decimal128() {
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(10, 2), false),
            Field::new("price", DataType::Decimal128(20, 4), false),
        ]));

        let column_names = vec!["id".to_string(), "amount".to_string(), "price".to_string()];
        let cast_map = PostgresQueryBuilder::build_cast_map(&schema, &column_names);

        // amount (Decimal128(10,2)) should get numeric(10,2) cast
        assert_eq!(
            cast_map.get("amount"),
            Some(&Some("numeric(10,2)".to_string()))
        );
        // price (Decimal128(20,4)) should get numeric(20,4) cast
        assert_eq!(
            cast_map.get("price"),
            Some(&Some("numeric(20,4)".to_string()))
        );
        // id (Int64) should not need a cast
        assert_eq!(cast_map.get("id"), Some(&None));
    }

    #[test]
    fn test_build_cast_map_decimal256() {
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Decimal256(30, 6), false),
        ]));

        let column_names = vec!["id".to_string(), "value".to_string()];
        let cast_map = PostgresQueryBuilder::build_cast_map(&schema, &column_names);

        // value (Decimal256(30,6)) should get numeric(30,6) cast
        assert_eq!(
            cast_map.get("value"),
            Some(&Some("numeric(30,6)".to_string()))
        );
        // id (Int64) should not need a cast
        assert_eq!(cast_map.get("id"), Some(&None));
    }

    // Feature 002 (Retire U256/I256): U256/I256 cast_map test deleted with
    // the retired types. Wide-int columns now route via decimal_arb +
    // native_int_kind; the cast_map for decimal_arb is covered by
    // test_build_cast_map_decimal256 (the decimal_arb branch is identical).

    #[test]
    fn test_build_complete_upsert_query_with_numeric_casts() {
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("block_slot", DataType::UInt64, false),
            Field::new("amount", DataType::Decimal128(10, 2), false),
        ]));

        let columns = vec![
            "id".to_string(),
            "block_slot".to_string(),
            "amount".to_string(),
        ];
        let pk_columns = vec!["id".to_string()];

        let query = PostgresQueryBuilder::build_complete_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            1,
            Some(&schema),
            "update",
            None,
            "test",
            None,
        );

        // Verify that block_slot gets numeric(20,0) cast
        assert!(query.contains("$2::numeric(20,0)"));
        // Verify that amount gets numeric(10,2) cast
        assert!(query.contains("$3::numeric(10,2)"));
        // Verify that id (Int64) doesn't get a cast
        assert!(query.contains("$1,"));
        assert!(!query.contains("$1::"));
    }

    #[test]
    fn test_build_values_clause_with_uint64_and_decimal() {
        let columns = vec![
            "id".to_string(),
            "block_slot".to_string(),
            "amount".to_string(),
        ];
        let mut cast_map = std::collections::HashMap::new();
        cast_map.insert("block_slot".to_string(), Some("numeric(20,0)".to_string()));
        cast_map.insert("amount".to_string(), Some("numeric(10,2)".to_string()));

        let clause = PostgresQueryBuilder::build_values_clause(2, 3, &columns, &cast_map);
        // First row: id (no cast), block_slot (cast), amount (cast)
        // Second row: id (no cast), block_slot (cast), amount (cast)
        assert_eq!(
            clause,
            "($1, $2::numeric(20,0), $3::numeric(10,2)), ($4, $5::numeric(20,0), $6::numeric(10,2))"
        );
    }

    #[test]
    fn test_build_complete_upsert_query_with_timestamp_cast() {
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "block_timestamp",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Second, None),
                false,
            ),
        ]));

        let columns = vec!["id".to_string(), "block_timestamp".to_string()];
        let pk_columns = vec!["id".to_string()];

        let query = PostgresQueryBuilder::build_complete_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            1,
            Some(&schema),
            "update",
            None,
            "test",
            None,
        );

        // Verify that block_timestamp gets timestamp cast
        assert!(query.contains("$2::timestamp"));
        // Verify that id (Int64) doesn't get a cast
        assert!(query.contains("$1,"));
        assert!(!query.contains("$1::"));
    }

    #[test]
    fn test_build_upsert_query_with_on_conflict_nothing() {
        let columns = vec!["id".to_string(), "name".to_string(), "value".to_string()];
        let pk_columns = vec!["id".to_string()];

        let (query, placeholders) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            "nothing",
            None,
            "test",
        );

        assert_eq!(placeholders, 3);
        assert!(query.contains("INSERT INTO"));
        assert!(query.contains(r#""id", "name", "value""#));
        assert!(query.contains("ON CONFLICT"));
        assert!(query.contains(r#""id""#));
        assert!(query.contains("DO NOTHING"));
        assert!(!query.contains("DO UPDATE"));
        assert!(!query.contains("EXCLUDED"));
    }

    #[test]
    fn test_build_complete_upsert_query_with_on_conflict_nothing() {
        let columns = vec!["id".to_string(), "name".to_string(), "value".to_string()];
        let pk_columns = vec!["id".to_string()];

        let query = PostgresQueryBuilder::build_complete_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            2,
            None,
            "nothing",
            None,
            "test",
            None,
        );

        assert!(query.contains("INSERT INTO"));
        assert!(query.contains(r#""public"."test""#));
        assert!(query.contains(r#""id", "name", "value""#));
        assert!(query.contains("ON CONFLICT"));
        assert!(query.contains(r#"("id")"#));
        assert!(query.contains("DO NOTHING"));
        assert!(!query.contains("DO UPDATE"));
        assert!(!query.contains("EXCLUDED"));
        // Verify VALUES clause with 2 rows
        assert!(query.contains("($1, $2, $3), ($4, $5, $6)"));
    }

    #[test]
    fn test_build_upsert_query_with_composite_primary_key_and_nothing() {
        let columns = vec![
            "id".to_string(),
            "tenant_id".to_string(),
            "name".to_string(),
            "value".to_string(),
        ];
        let pk_columns = vec!["id".to_string(), "tenant_id".to_string()];

        let (query, placeholders) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            "nothing",
            None,
            "test",
        );

        assert_eq!(placeholders, 4);
        assert!(query.contains("INSERT INTO"));
        assert!(query.contains("ON CONFLICT"));
        assert!(query.contains(r#""id", "tenant_id""#));
        assert!(query.contains("DO NOTHING"));
        assert!(!query.contains("DO UPDATE"));
        assert!(!query.contains("EXCLUDED"));
    }

    #[test]
    fn test_build_upsert_query_with_composite_primary_key_and_update() {
        let columns = vec![
            "id".to_string(),
            "tenant_id".to_string(),
            "name".to_string(),
            "value".to_string(),
        ];
        let pk_columns = vec!["id".to_string(), "tenant_id".to_string()];

        let (query, placeholders) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            "update",
            None,
            "test",
        );

        assert_eq!(placeholders, 4);
        assert!(query.contains("INSERT INTO"));
        assert!(query.contains("ON CONFLICT"));
        assert!(query.contains(r#""id", "tenant_id""#));
        assert!(query.contains("DO UPDATE SET"));
        assert!(query.contains(r#""name" = EXCLUDED."name""#));
        assert!(query.contains(r#""value" = EXCLUDED."value""#));
        // Verify primary key columns are not in UPDATE SET
        assert!(!query.contains(r#""id" = EXCLUDED."id""#));
        assert!(!query.contains(r#""tenant_id" = EXCLUDED."tenant_id""#));
    }

    #[test]
    fn test_build_complete_upsert_query_with_multiple_rows() {
        // Test both strategies with multiple rows to ensure VALUES clause generation works correctly
        let columns = vec!["id".to_string(), "name".to_string(), "status".to_string()];
        let pk_columns = vec!["id".to_string()];

        // Test "nothing" strategy with 3 rows
        let query_nothing = PostgresQueryBuilder::build_complete_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            3,
            None,
            "nothing",
            None,
            "test",
            None,
        );

        assert!(query_nothing.contains("DO NOTHING"));
        assert!(!query_nothing.contains("DO UPDATE"));
        assert!(query_nothing.contains("($1, $2, $3), ($4, $5, $6), ($7, $8, $9)"));

        // Test "update" strategy with 3 rows
        let query_update = PostgresQueryBuilder::build_complete_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            3,
            None,
            "update",
            None,
            "test",
            None,
        );

        assert!(query_update.contains("DO UPDATE SET"));
        assert!(query_update.contains(r#""name" = EXCLUDED."name""#));
        assert!(query_update.contains(r#""status" = EXCLUDED."status""#));
        assert!(!query_update.contains("DO NOTHING"));
        assert!(query_update.contains("($1, $2, $3), ($4, $5, $6), ($7, $8, $9)"));
    }

    #[test]
    fn test_build_complete_upsert_query_with_checkpoint_epoch() {
        let columns = vec!["id".to_string(), "name".to_string(), "value".to_string()];
        let pk_columns = vec!["id".to_string()];

        let query = PostgresQueryBuilder::build_complete_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            2,
            None,
            "update",
            None,
            "test",
            Some(42),
        );

        // Verify _gs_checkpoint_epoch is included in column list
        assert!(query.contains(r#""id", "name", "value", "_gs_checkpoint_epoch""#));
        // Verify ON CONFLICT is on original PK only (not including checkpoint epoch)
        assert!(query.contains(r#"ON CONFLICT ("id")"#));
        // Verify UPDATE SET includes all non-PK columns (including checkpoint epoch)
        assert!(query.contains(r#""name" = EXCLUDED."name""#));
        assert!(query.contains(r#""value" = EXCLUDED."value""#));
        assert!(query.contains(r#""_gs_checkpoint_epoch" = EXCLUDED."_gs_checkpoint_epoch""#));
        // Verify VALUES has 4 placeholders per row (3 columns + 1 epoch)
        assert!(query.contains("($1, $2, $3, $4), ($5, $6, $7, $8)"));
    }

    #[test]
    fn test_update_where_single_condition() {
        let columns = vec![
            "id".to_string(),
            "value".to_string(),
            "updated_at".to_string(),
        ];
        let pk_columns = vec!["id".to_string()];
        let update_where = BTreeMap::from([("updated_at".to_string(), ">".to_string())]);

        let (query, placeholders) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            "update",
            Some(&update_where),
            "my_table",
        );

        assert_eq!(placeholders, 3);
        assert!(query.contains("DO UPDATE SET"));
        assert!(query.contains(r#"WHERE EXCLUDED."updated_at" > "my_table"."updated_at""#));
    }

    #[test]
    fn test_update_where_multiple_conditions() {
        let columns = vec![
            "id".to_string(),
            "value".to_string(),
            "updated_at".to_string(),
            "version".to_string(),
        ];
        let pk_columns = vec!["id".to_string()];
        let update_where = BTreeMap::from([
            ("updated_at".to_string(), ">=".to_string()),
            ("version".to_string(), ">".to_string()),
        ]);

        let (query, _) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            "update",
            Some(&update_where),
            "events",
        );

        assert!(query.contains("DO UPDATE SET"));
        assert!(query.contains(r#"EXCLUDED."updated_at" >= "events"."updated_at""#));
        assert!(query.contains(r#"EXCLUDED."version" > "events"."version""#));
        assert!(query.contains(" AND "));
    }

    #[test]
    fn test_update_where_complete_query() {
        let columns = vec![
            "id".to_string(),
            "value".to_string(),
            "updated_at".to_string(),
        ];
        let pk_columns = vec!["id".to_string()];
        let update_where = BTreeMap::from([("updated_at".to_string(), ">".to_string())]);

        let query = PostgresQueryBuilder::build_complete_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            2,
            None,
            "update",
            Some(&update_where),
            "my_table",
            None,
        );

        assert!(query.contains("DO UPDATE SET"));
        assert!(query.contains(r#""value" = EXCLUDED."value""#));
        assert!(query.contains(r#""updated_at" = EXCLUDED."updated_at""#));
        assert!(query.contains(r#"WHERE EXCLUDED."updated_at" > "my_table"."updated_at""#));
        assert!(query.contains("($1, $2, $3), ($4, $5, $6)"));
    }

    #[test]
    fn test_update_where_equals_operator() {
        let columns = vec!["id".to_string(), "value".to_string(), "status".to_string()];
        let pk_columns = vec!["id".to_string()];
        let update_where = BTreeMap::from([("status".to_string(), "=".to_string())]);

        let (query, _) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            "update",
            Some(&update_where),
            "my_table",
        );

        assert!(query.contains("DO UPDATE SET"));
        assert!(query.contains(r#"WHERE EXCLUDED."status" = "my_table"."status""#));
    }

    #[test]
    fn test_update_where_ignored_for_nothing_strategy() {
        let columns = vec!["id".to_string(), "value".to_string()];
        let pk_columns = vec!["id".to_string()];
        let update_where = BTreeMap::from([("value".to_string(), ">".to_string())]);

        let (query, _) = PostgresQueryBuilder::build_upsert_query(
            "public",
            "test",
            &columns,
            &pk_columns,
            "nothing",
            Some(&update_where),
            "test",
        );

        assert!(query.contains("DO NOTHING"));
        assert!(!query.contains("WHERE"));
    }

    #[test]
    fn test_validate_update_where_valid() {
        use arrow_schema::Schema;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
        ]));

        let uw = BTreeMap::from([
            ("updated_at".to_string(), ">".to_string()),
            ("version".to_string(), ">=".to_string()),
        ]);

        let result = validate_update_where(&uw, &schema, "my_sink");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_update_where_missing_column() {
        use arrow_schema::Schema;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let uw = BTreeMap::from([("nonexistent".to_string(), ">".to_string())]);

        let result = validate_update_where(&uw, &schema, "my_sink");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("column 'nonexistent' not found in schema"),
            "expected column-not-found error, got: {err}"
        );
        assert!(
            err.contains("my_sink"),
            "expected sink name in error, got: {err}"
        );
    }

    #[test]
    fn test_validate_update_where_invalid_operator() {
        use arrow_schema::Schema;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let uw = BTreeMap::from([("value".to_string(), "LIKE".to_string())]);

        let result = validate_update_where(&uw, &schema, "my_sink");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("invalid operator 'LIKE'"),
            "expected invalid-operator error, got: {err}"
        );
        assert!(
            err.contains("my_sink"),
            "expected sink name in error, got: {err}"
        );
    }

    #[test]
    fn test_validate_update_where_all_allowed_operators() {
        use arrow_schema::Schema;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("col", DataType::Utf8, false),
        ]));

        for op in ALLOWED_UPDATE_WHERE_OPS {
            let uw = BTreeMap::from([("col".to_string(), op.to_string())]);
            let result = validate_update_where(&uw, &schema, "sink");
            assert!(result.is_ok(), "operator '{}' should be allowed", op);
        }
    }
}
