use crate::error::{Result, ResultExt};
use crate::topology::{AggregateColumn, AggregateFunction, GroupByColumn};
use crate::utils::pg::{
    PostgresConnection, arrow_field_to_postgres_type, create_schema_and_table_if_needed,
    postgres_type_to_arrow_type,
};
use crate::{streamling_bail, streamling_user_bail};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Column, DFSchema, DFSchemaRef, ScalarValue};
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::utils::exprlist_to_fields;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::Arc;
use streamling_config::PostgresSinkConfig;
use tracing::debug;

const GLOBAL_AGGREGATION_SENTINEL_KEY: &str = "key";

#[derive(Debug)]
pub struct PostgresAggregator {
    group_expr: Vec<Expr>,
    aggr_expr: Vec<Expr>,
    agg_schema: DFSchemaRef,
    input_schema: DFSchemaRef,
    source_table: String,
    source_table_pk: String,
    target_table: String,
    pg_schema: String,
    type_overrides: IndexMap<String, String>,
}

impl PostgresAggregator {
    pub fn try_new(
        group_by: &IndexMap<String, GroupByColumn>,
        aggregate: &IndexMap<String, AggregateColumn>,
        input_schema: DFSchemaRef,
        source_table: String,
        source_table_pk: String,
        target_table: String,
        pg_schema: String,
    ) -> Result<Self> {
        if aggregate.is_empty() {
            streamling_user_bail!("at least one aggregate column is required");
        }

        debug!(
            "Creating PostgresAggregator with {} group_by columns and {} aggregate columns",
            group_by.len(),
            aggregate.len()
        );

        let mut group_expr = Vec::new();
        let mut aggr_expr = Vec::new();
        let mut type_overrides = IndexMap::new();

        for (col_name, col_def) in group_by {
            let source_field = col_def.from.as_ref().unwrap_or(col_name);

            if let Some(pg_type) = &col_def.pg_type {
                type_overrides.insert(col_name.clone(), pg_type.clone());
            }

            group_expr.push(Expr::Column(Column::new_unqualified(source_field)).alias(col_name));
        }

        for (col_name, col_def) in aggregate {
            let source_field = col_def.from.as_ref().unwrap_or(col_name);

            if let Some(pg_type) = &col_def.pg_type {
                type_overrides.insert(col_name.clone(), pg_type.clone());
            }

            // For COUNT without a source, use a literal (COUNT(*))
            let col_expr = if col_def.function == AggregateFunction::Count && col_def.from.is_none()
            {
                Expr::Literal(ScalarValue::Int64(Some(1)), None)
            } else {
                Expr::Column(Column::new_unqualified(source_field))
            };

            let agg_fn = match &col_def.function {
                AggregateFunction::Sum => sum(col_expr),
                AggregateFunction::Count => count(col_expr),
                AggregateFunction::Avg => avg(col_expr),
                AggregateFunction::Min => min(col_expr),
                AggregateFunction::Max => max(col_expr),
            };
            aggr_expr.push(agg_fn.alias(col_name));
        }

        // Create a dummy LogicalPlan for exprlist_to_fields
        let dummy_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: input_schema.clone(),
        });

        let mut qualified_fields = exprlist_to_fields(&group_expr, &dummy_plan)?;

        qualified_fields.extend(exprlist_to_fields(&aggr_expr, &dummy_plan)?);

        let agg_schema = Arc::new(DFSchema::new_with_metadata(
            qualified_fields,
            HashMap::new(),
        )?);

        Ok(PostgresAggregator {
            group_expr,
            aggr_expr,
            agg_schema,
            input_schema,
            source_table,
            source_table_pk,
            target_table,
            pg_schema,
            type_overrides,
        })
    }

    fn extract_column_name_from_args(args: &[Expr]) -> Result<String> {
        if args.is_empty() {
            return Ok("*".to_string()); // COUNT(*)
        }
        match &args[0] {
            Expr::Column(col) => Ok(col.name.clone()),
            #[allow(deprecated)]
            Expr::Wildcard { .. } => Ok("*".to_string()),
            Expr::Literal(_, _) => Ok("*".to_string()), // COUNT(*) represented as Literal
            _ => streamling_user_bail!("unsupported aggregate argument: {:?}", args[0]),
        }
    }

    fn quoted(name: &str) -> String {
        format!("\"{}\"", name)
    }

    fn qualified_name(&self, name: &str) -> String {
        format!("\"{}\".\"{}\"", self.pg_schema, name)
    }

    /// Generates the PostgreSQL function SQL for the trigger.
    ///
    /// Returns the CREATE OR REPLACE FUNCTION statement that will be executed by the trigger.
    pub fn generate_function_sql(&self) -> Result<String> {
        let group_cols: Vec<(String, String)> = if self.group_expr.is_empty() {
            vec![(
                GLOBAL_AGGREGATION_SENTINEL_KEY.to_string(),
                "'value'".to_string(),
            )]
        } else {
            self.group_expr
                .iter()
                .map(|expr| match expr {
                    Expr::Alias(alias) => match alias.expr.as_ref() {
                        Expr::Column(col) => {
                            Ok((alias.name.clone(), format!("new_table.\"{}\"", col.name)))
                        }
                        _ => streamling_user_bail!("unsupported group expr: {:?}", expr),
                    },
                    _ => streamling_user_bail!("unsupported group expr: {:?}", expr),
                })
                .collect::<Result<Vec<_>>>()?
        };

        let mut agg_columns = Vec::new();

        for expr in &self.aggr_expr {
            let (agg, alias_name) = match expr {
                Expr::AggregateFunction(agg) => (agg, None),
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::AggregateFunction(agg) => (agg, Some(alias.name.as_str())),
                    _ => {
                        streamling_bail!(
                            "expected AggregateFunction in Alias, got: {:?}",
                            alias.expr
                        );
                    }
                },
                _ => {
                    streamling_bail!("expected AggregateFunction or Alias, got: {:?}", expr);
                }
            };

            let func_name = agg.func.name();
            let col_name = Self::extract_column_name_from_args(&agg.params.args)?;

            // Use alias name if provided, otherwise generate clean column name
            let agg_field_name = if let Some(alias) = alias_name {
                alias.to_string()
            } else if col_name == "*" {
                func_name.to_string()
            } else {
                format!("{}_{}", func_name, col_name)
            };

            let qualified_target = self.qualified_name(&self.target_table);
            let quoted_agg_field = Self::quoted(&agg_field_name);

            match func_name {
                "count" => {
                    agg_columns.push((
                        agg_field_name.clone(),
                        // Conditional count based on _gs_op: +1 for inserts, 0 for updates, -1 for deletes
                        "SUM(CASE WHEN new_table.\"_gs_op\" = 'd' THEN -1 WHEN new_table.\"_gs_op\" = 'u' THEN 0 ELSE 1 END)".to_string(),
                        format!("{}.{} + EXCLUDED.{}", qualified_target, quoted_agg_field, quoted_agg_field),
                    ));
                }
                "sum" => {
                    let quoted_source_col = Self::quoted(&col_name);
                    agg_columns.push((
                        agg_field_name.clone(),
                        // Conditional sum based on _gs_op: subtract for deletes, add for inserts/updates
                        format!(
                            "SUM(CASE WHEN new_table.\"_gs_op\" = 'd' THEN -new_table.{} ELSE new_table.{} END)",
                            quoted_source_col, quoted_source_col
                        ),
                        format!("{}.{} + EXCLUDED.{}", qualified_target, quoted_agg_field, quoted_agg_field),
                    ));
                }
                "avg" => {
                    let quoted_source_col = Self::quoted(&col_name);
                    // AVG splits into sum and count, both use conditional logic
                    let sum_field = format!("{}_sum", agg_field_name);
                    let count_field = format!("{}_count", agg_field_name);
                    let quoted_sum_field = Self::quoted(&sum_field);
                    let quoted_count_field = Self::quoted(&count_field);
                    agg_columns.push((
                        sum_field,
                        format!(
                            "SUM(CASE WHEN new_table.\"_gs_op\" = 'd' THEN -new_table.{} ELSE new_table.{} END)",
                            quoted_source_col, quoted_source_col
                        ),
                        format!("{}.{} + EXCLUDED.{}", qualified_target, quoted_sum_field, quoted_sum_field),
                    ));
                    agg_columns.push((
                        count_field,
                        // Conditional count based on _gs_op: +1 for inserts, 0 for updates, -1 for deletes
                        "SUM(CASE WHEN new_table.\"_gs_op\" = 'd' THEN -1 WHEN new_table.\"_gs_op\" = 'u' THEN 0 ELSE 1 END)".to_string(),
                        format!("{}.{} + EXCLUDED.{}", qualified_target, quoted_count_field, quoted_count_field),
                    ));
                }
                "min" => {
                    // MIN/MAX do not support deletes correctly in append-only mode.
                    // Deletes cannot retract values without scanning all historical
                    // operations in the landing table.
                    let quoted_source_col = Self::quoted(&col_name);
                    agg_columns.push((
                        agg_field_name.clone(),
                        format!("MIN(new_table.{})", quoted_source_col),
                        format!(
                            "LEAST({}.{}, EXCLUDED.{})",
                            qualified_target, quoted_agg_field, quoted_agg_field
                        ),
                    ));
                }
                "max" => {
                    let quoted_source_col = Self::quoted(&col_name);
                    agg_columns.push((
                        agg_field_name.clone(),
                        format!("MAX(new_table.{})", quoted_source_col),
                        format!(
                            "GREATEST({}.{}, EXCLUDED.{})",
                            qualified_target, quoted_agg_field, quoted_agg_field
                        ),
                    ));
                }
                _ => {
                    streamling_user_bail!("unsupported aggregate function: {}", func_name);
                }
            }
        }

        let all_columns: Vec<String> = group_cols
            .iter()
            .map(|(name, _)| Self::quoted(name))
            .chain(agg_columns.iter().map(|(name, _, _)| Self::quoted(name)))
            .collect();

        let select_expressions: Vec<String> =
            group_cols
                .iter()
                .map(|(name, expr)| format!("{} AS {}", expr, Self::quoted(name)))
                .chain(agg_columns.iter().map(|(name, select_expr, _)| {
                    format!("{} AS {}", select_expr, Self::quoted(name))
                }))
                .collect();

        let group_by_clause = if self.group_expr.is_empty() {
            String::new()
        } else {
            let group_columns: Vec<String> = self
                .group_expr
                .iter()
                .filter_map(|expr| match expr {
                    Expr::Alias(alias) => match alias.expr.as_ref() {
                        Expr::Column(col) => Some(format!("new_table.{}", Self::quoted(&col.name))),
                        _ => None,
                    },
                    _ => None,
                })
                .collect();
            if group_columns.is_empty() {
                String::new()
            } else {
                format!("GROUP BY {}", group_columns.join(", "))
            }
        };

        let conflict_columns: Vec<String> = group_cols
            .iter()
            .map(|(name, _)| Self::quoted(name))
            .collect();

        let update_clauses: Vec<String> = agg_columns
            .iter()
            .map(|(name, _, update)| format!("{} = {}", Self::quoted(name), update))
            .collect();

        let function_name = format!("{}_to_{}_fn", self.source_table, self.target_table);
        let qualified_function_name = self.qualified_name(&function_name);
        let qualified_target_table = self.qualified_name(&self.target_table);

        Ok(format!(
            r#"CREATE OR REPLACE FUNCTION {function_name}()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO {target_table} ({columns})
  SELECT {select_list}
  FROM new_table
  {group_by}
  ON CONFLICT ({conflict_cols})
  DO UPDATE SET {updates};
  RETURN NULL;
END;
$$;"#,
            function_name = qualified_function_name,
            target_table = qualified_target_table,
            columns = all_columns.join(", "),
            select_list = select_expressions.join(", "),
            group_by = group_by_clause,
            conflict_cols = conflict_columns.join(", "),
            updates = update_clauses.join(", "),
        ))
    }

    /// Generates the PostgreSQL trigger statement.
    ///
    /// Returns the DROP and CREATE TRIGGER statements that invoke the function.
    /// Uses separate statements because CREATE OR REPLACE TRIGGER was only added in Postgres 14.
    pub fn generate_trigger_statement_sql(&self) -> Result<String> {
        let function_name = format!("{}_to_{}_fn", self.source_table, self.target_table);
        let trigger_name = format!("{}_to_{}_trigger", self.source_table, self.target_table);
        let qualified_source_table = self.qualified_name(&self.source_table);
        let qualified_function_name = self.qualified_name(&function_name);

        Ok(format!(
            r#"DROP TRIGGER IF EXISTS "{trigger_name}" ON {source_table};
CREATE TRIGGER "{trigger_name}"
AFTER INSERT ON {source_table}
REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE FUNCTION {function_name}();"#,
            trigger_name = trigger_name,
            source_table = qualified_source_table,
            function_name = qualified_function_name,
        ))
    }

    /// Builds an Arrow Schema for the aggregation target table.
    ///
    /// This schema differs from `agg_schema` (DataFusion's logical output schema) because:
    /// 1. Global aggregations need a sentinel 'key' column for PRIMARY KEY constraint
    /// 2. AVG is split into separate _sum and _count columns for incremental updates
    /// 3. MIN/MAX results must be nullable (can be NULL when no input rows exist)
    ///
    /// This schema includes:
    /// - Group columns (or a sentinel 'key' column for global aggregations)
    /// - Aggregate result columns (count, sum, avg_sum, avg_count, min, max)
    fn build_target_schema(&self) -> Result<SchemaRef> {
        let mut fields = Vec::new();

        if self.group_expr.is_empty() {
            fields.push(Field::new(
                GLOBAL_AGGREGATION_SENTINEL_KEY,
                DataType::Utf8,
                false,
            ));
        } else {
            for expr in &self.group_expr {
                match expr {
                    Expr::Alias(alias) => {
                        let output_name = &alias.name;
                        let field = self
                            .agg_schema
                            .field_with_unqualified_name(output_name)
                            .streamling_with_context(|| {
                                format!("column '{}' not found in schema", output_name)
                            })?;

                        let data_type =
                            if let Some(override_type) = self.type_overrides.get(output_name) {
                                postgres_type_to_arrow_type(override_type)?
                            } else {
                                field.data_type().clone()
                            };

                        fields.push(Field::new(
                            output_name.clone(),
                            data_type,
                            field.is_nullable(),
                        ));
                    }
                    _ => {
                        streamling_user_bail!("unsupported group expr: {:?}", expr);
                    }
                }
            }
        }

        // Generate aggregate columns
        for expr in &self.aggr_expr {
            let (agg, alias_name) = match expr {
                Expr::AggregateFunction(agg) => (agg, None),
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::AggregateFunction(agg) => (agg, Some(alias.name.as_str())),
                    _ => {
                        streamling_bail!(
                            "expected AggregateFunction in Alias, got: {:?}",
                            alias.expr
                        );
                    }
                },
                _ => {
                    streamling_bail!("expected AggregateFunction or Alias, got: {:?}", expr);
                }
            };

            let func_name = agg.func.name();
            let col_name = Self::extract_column_name_from_args(&agg.params.args)?;

            let agg_field_name = if let Some(alias) = alias_name {
                alias.to_string()
            } else if col_name == "*" {
                func_name.to_string()
            } else {
                format!("{}_{}", func_name, col_name)
            };

            match func_name {
                "count" => {
                    // COUNT always returns BIGINT, not nullable
                    let data_type =
                        if let Some(override_type) = self.type_overrides.get(&agg_field_name) {
                            postgres_type_to_arrow_type(override_type)?
                        } else {
                            DataType::Int64
                        };
                    fields.push(Field::new(agg_field_name, data_type, false));
                }
                "sum" => {
                    // SUM type depends on input column type or override, not nullable
                    let data_type =
                        if let Some(override_type) = self.type_overrides.get(&agg_field_name) {
                            postgres_type_to_arrow_type(override_type)?
                        } else {
                            let field = self
                                .input_schema
                                .field_with_unqualified_name(&col_name)
                                .streamling_with_context(|| {
                                    format!("column '{}' not found in input schema", col_name)
                                })?;
                            field.data_type().clone()
                        };
                    fields.push(Field::new(agg_field_name, data_type, false));
                }
                "avg" => {
                    // AVG splits into sum and count, both not nullable
                    let sum_data_type =
                        if let Some(override_type) = self.type_overrides.get(&agg_field_name) {
                            postgres_type_to_arrow_type(override_type)?
                        } else {
                            let field = self
                                .input_schema
                                .field_with_unqualified_name(&col_name)
                                .streamling_with_context(|| {
                                    format!("column '{}' not found in input schema", col_name)
                                })?;
                            field.data_type().clone()
                        };
                    fields.push(Field::new(
                        format!("{}_sum", agg_field_name),
                        sum_data_type,
                        false,
                    ));
                    fields.push(Field::new(
                        format!("{}_count", agg_field_name),
                        DataType::Int64,
                        false,
                    ));
                }
                "min" | "max" => {
                    // MIN/MAX type matches input column type or override, nullable
                    let data_type =
                        if let Some(override_type) = self.type_overrides.get(&agg_field_name) {
                            postgres_type_to_arrow_type(override_type)?
                        } else {
                            let field = self
                                .input_schema
                                .field_with_unqualified_name(&col_name)
                                .streamling_with_context(|| {
                                    format!("column '{}' not found in input schema", col_name)
                                })?;
                            field.data_type().clone()
                        };
                    fields.push(Field::new(agg_field_name, data_type, true));
                }
                _ => {
                    streamling_user_bail!("unsupported aggregate function: {}", func_name);
                }
            }
        }

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Generates a PostgreSQL CREATE TABLE statement for the aggregation target table.
    ///
    /// Creates a table schema that matches the output of the trigger function, including:
    /// - Group columns (or a sentinel 'key' column for global aggregations)
    /// - Aggregate result columns (count, sum, avg_sum, avg_count, min, max)
    /// - Primary key constraint on group columns
    pub fn generate_target_table_sql(&self) -> Result<String> {
        let schema = self.build_target_schema()?;
        let mut column_defs = Vec::new();
        let mut primary_key_cols = Vec::new();

        // Determine primary key columns (group columns)
        let pk_count = if self.group_expr.is_empty() {
            1 // sentinel key column
        } else {
            self.group_expr.len()
        };

        for (idx, field) in schema.fields().iter().enumerate() {
            let pg_type = if let Some(override_type) = self.type_overrides.get(field.name()) {
                override_type.to_uppercase()
            } else {
                arrow_field_to_postgres_type(field)
            };
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };

            // Add DEFAULT 0 for aggregate columns (not for group/key columns)
            let default = if idx >= pk_count && !field.is_nullable() {
                " DEFAULT 0"
            } else {
                ""
            };

            column_defs.push(format!(
                r#""{}" {}{}{}"#,
                field.name(),
                pg_type,
                nullable,
                default
            ));

            if idx < pk_count {
                primary_key_cols.push(Self::quoted(field.name()));
            }
        }

        Ok(format!(
            r#"CREATE TABLE IF NOT EXISTS "{}"."{}" ({}, PRIMARY KEY ({}))"#,
            self.pg_schema,
            self.target_table,
            column_defs.join(", "),
            primary_key_cols.join(", ")
        ))
    }

    pub async fn create_trigger_and_tables(&self, config: &PostgresSinkConfig) -> Result<()> {
        let connection = PostgresConnection::new(config)
            .await
            .streamling_context("failed to create PostgreSQL connection")?;

        // Create source ("landing") table (and schema), if needed
        // Use append-only mode for landing table (composite PK with _gs_op)
        create_schema_and_table_if_needed(
            connection.pool(),
            &self.pg_schema,
            &self.source_table,
            self.input_schema.inner(),
            Some(&self.source_table_pk),
            true,
            true, // checkpoint_truncation (enabled for landing table)
        )
        .await?;

        // Create target ("agg") table (and schema), if needed
        // Uses generate_target_table_sql directly to preserve type overrides
        let target_table_sql = self.generate_target_table_sql()?;
        debug!("Creating PostgreSQL target table: {}", target_table_sql);

        sqlx::query(&target_table_sql)
            .execute(connection.pool())
            .await
            .streamling_with_context(|| {
                format!(
                    "failed to create target table '{}.{}'",
                    self.pg_schema, self.target_table
                )
            })?;

        // Create trigger function
        let function_sql = self.generate_function_sql()?;
        debug!("Creating PostgreSQL function: {}", function_sql);

        sqlx::query(&function_sql)
            .execute(connection.pool())
            .await
            .streamling_with_context(|| "failed to create PostgreSQL function")?;

        // Create trigger (DROP IF EXISTS + CREATE)
        // CREATE OR REPLACE TRIGGER was only added in Postgres 14
        let trigger_sql = self.generate_trigger_statement_sql()?;
        debug!("Creating PostgreSQL trigger: {}", trigger_sql);

        // These statements should be executed separately
        for statement in trigger_sql.split(';').filter(|s| !s.trim().is_empty()) {
            sqlx::query(statement)
                .execute(connection.pool())
                .await
                .streamling_with_context(|| "failed to execute trigger statement")?;
        }

        // Not needed anymore
        connection.pool().close().await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;
    use std::sync::Arc;

    async fn create_test_context() -> SessionContext {
        let ctx = SessionContext::new();

        // Register a test table
        let schema = Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
            Field::new("user_id", DataType::Int64, true),
        ]);

        ctx.register_table(
            "polymarket_transaction",
            Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(
                schema,
            ))),
        )
        .unwrap();

        ctx
    }

    fn assert_aliased_function(expr: &Expr, expected_func_name: &str) {
        match expr {
            Expr::Alias(alias) => match alias.expr.as_ref() {
                Expr::AggregateFunction(agg) => {
                    assert_eq!(agg.func.name(), expected_func_name);
                }
                _ => panic!(
                    "Expected AggregateFunction expression inside Alias, got: {:?}",
                    &alias.expr
                ),
            },
            _ => panic!("Expected Alias expression, got: {:?}", expr),
        }
    }

    #[tokio::test]
    async fn test_count_aggregation_with_group_by() {
        use crate::topology::{AggregateColumn, AggregateFunction, GroupByColumn};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let mut group_by = IndexMap::new();
        group_by.insert(
            "market".to_string(),
            GroupByColumn {
                from: None,
                pg_type: None,
            },
        );

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "count".to_string(),
            AggregateColumn {
                from: None,
                function: AggregateFunction::Count,
                pg_type: None,
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "polymarket_transaction".to_string(),
            "id".to_string(),
            "polymarket_transaction_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        assert_eq!(aggregator.group_expr.len(), 1);
        assert!(matches!(&aggregator.group_expr[0], Expr::Alias(a) if a.name == "market"));

        assert_eq!(aggregator.aggr_expr.len(), 1);
        assert_aliased_function(&aggregator.aggr_expr[0], "count");

        let schema = &aggregator.agg_schema;
        assert_eq!(schema.fields().len(), 2);

        let market_field = schema.field_with_unqualified_name("market").unwrap();
        assert_eq!(market_field.name(), "market");
        assert_eq!(market_field.data_type(), &DataType::Utf8);

        // Count aggregate field - check by index since it may have generated name
        let count_field = &schema.fields()[1];
        assert_eq!(count_field.data_type(), &DataType::Int64);

        let function_sql = aggregator.generate_function_sql().unwrap();
        let trigger_sql = aggregator.generate_trigger_statement_sql().unwrap();
        debug!("Generated function SQL:\n{}", function_sql);
        debug!("Generated trigger SQL:\n{}", trigger_sql);

        assert!(function_sql.contains(
            r#"CREATE OR REPLACE FUNCTION "public"."polymarket_transaction_to_polymarket_transaction_agg_fn"()"#
        ));
        assert!(function_sql.contains("RETURNS TRIGGER"));
        assert!(
            function_sql.contains(
                r#"INSERT INTO "public"."polymarket_transaction_agg" ("market", "count")"#
            )
        );
        assert!(function_sql.contains(r#"SELECT new_table."market" AS "market", SUM(CASE WHEN new_table."_gs_op" = 'd' THEN -1 WHEN new_table."_gs_op" = 'u' THEN 0 ELSE 1 END) AS "count""#));
        assert!(function_sql.contains("FROM new_table"));
        assert!(function_sql.contains(r#"GROUP BY new_table."market""#));
        assert!(function_sql.contains(r#"ON CONFLICT ("market")"#));
        assert!(function_sql.contains(
            r#"DO UPDATE SET "count" = "public"."polymarket_transaction_agg"."count" + EXCLUDED."count""#
        ));
        assert!(function_sql.contains("RETURN NULL"));

        assert!(trigger_sql.contains(
            r#"DROP TRIGGER IF EXISTS "polymarket_transaction_to_polymarket_transaction_agg_trigger""#
        ));
        assert!(trigger_sql.contains(
            r#"CREATE TRIGGER "polymarket_transaction_to_polymarket_transaction_agg_trigger""#
        ));
        assert!(trigger_sql.contains(r#"AFTER INSERT ON "public"."polymarket_transaction""#));
        assert!(trigger_sql.contains("REFERENCING NEW TABLE AS new_table"));
        assert!(trigger_sql.contains("FOR EACH STATEMENT"));
    }

    #[tokio::test]
    async fn test_multiple_aggregations() {
        use crate::topology::{AggregateColumn, AggregateFunction, GroupByColumn};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let mut group_by = IndexMap::new();
        group_by.insert(
            "market".to_string(),
            GroupByColumn {
                from: None,
                pg_type: None,
            },
        );

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "count".to_string(),
            AggregateColumn {
                from: None,
                function: AggregateFunction::Count,
                pg_type: None,
            },
        );
        aggregate.insert(
            "avg_amount".to_string(),
            AggregateColumn {
                from: Some("amount".to_string()),
                function: AggregateFunction::Avg,
                pg_type: None,
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "polymarket_transaction".to_string(),
            "id".to_string(),
            "polymarket_transaction_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        assert_eq!(aggregator.group_expr.len(), 1);
        assert!(matches!(&aggregator.group_expr[0], Expr::Alias(a) if a.name == "market"));

        assert_eq!(aggregator.aggr_expr.len(), 2);

        assert_aliased_function(&aggregator.aggr_expr[0], "count");
        assert_aliased_function(&aggregator.aggr_expr[1], "avg");

        let schema = &aggregator.agg_schema;
        assert_eq!(schema.fields().len(), 3); // market + count + average

        let market_field = schema.field_with_unqualified_name("market").unwrap();
        assert_eq!(market_field.name(), "market");
        assert_eq!(market_field.data_type(), &DataType::Utf8);

        // Aggregate fields by index
        let count_field = &schema.fields()[1];
        assert_eq!(count_field.data_type(), &DataType::Int64);

        let average_field = &schema.fields()[2];
        // Avg returns Float64
        assert_eq!(average_field.data_type(), &DataType::Float64);

        let function_sql = aggregator.generate_function_sql().unwrap();
        let trigger_sql = aggregator.generate_trigger_statement_sql().unwrap();
        debug!("Generated function SQL:\n{}", function_sql);
        debug!("Generated trigger SQL:\n{}", trigger_sql);

        assert!(function_sql.contains(
            r#"CREATE OR REPLACE FUNCTION "public"."polymarket_transaction_to_polymarket_transaction_agg_fn"()"#
        ));
        assert!(function_sql.contains(r#"INSERT INTO "public"."polymarket_transaction_agg" ("market", "count", "avg_amount_sum", "avg_amount_count")"#));
        assert!(function_sql.contains(r#"SELECT new_table."market" AS "market", SUM(CASE WHEN new_table."_gs_op" = 'd' THEN -1 WHEN new_table."_gs_op" = 'u' THEN 0 ELSE 1 END) AS "count", SUM(CASE WHEN new_table."_gs_op" = 'd' THEN -new_table."amount" ELSE new_table."amount" END) AS "avg_amount_sum", SUM(CASE WHEN new_table."_gs_op" = 'd' THEN -1 WHEN new_table."_gs_op" = 'u' THEN 0 ELSE 1 END) AS "avg_amount_count""#));
        assert!(function_sql.contains("FROM new_table"));
        assert!(function_sql.contains(r#"GROUP BY new_table."market""#));
        assert!(function_sql.contains(r#"ON CONFLICT ("market")"#));
        assert!(function_sql.contains(
            r#""count" = "public"."polymarket_transaction_agg"."count" + EXCLUDED."count""#
        ));
        assert!(function_sql.contains(
            r#""avg_amount_sum" = "public"."polymarket_transaction_agg"."avg_amount_sum" + EXCLUDED."avg_amount_sum""#
        ));
        assert!(function_sql.contains(r#""avg_amount_count" = "public"."polymarket_transaction_agg"."avg_amount_count" + EXCLUDED."avg_amount_count""#));
        assert!(function_sql.contains("RETURN NULL"));

        assert!(trigger_sql.contains("REFERENCING NEW TABLE AS new_table"));
        assert!(trigger_sql.contains("FOR EACH STATEMENT"));
    }

    #[tokio::test]
    async fn test_multiple_group_by_columns() {
        use crate::topology::{AggregateColumn, AggregateFunction, GroupByColumn};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("user_id", DataType::Int64, true),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let mut group_by = IndexMap::new();
        group_by.insert(
            "market".to_string(),
            GroupByColumn {
                from: None,
                pg_type: None,
            },
        );
        group_by.insert(
            "user_id".to_string(),
            GroupByColumn {
                from: None,
                pg_type: None,
            },
        );

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "count".to_string(),
            AggregateColumn {
                from: None,
                function: AggregateFunction::Count,
                pg_type: None,
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "polymarket_transaction".to_string(),
            "id".to_string(),
            "polymarket_transaction_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        assert_eq!(aggregator.group_expr.len(), 2);
        assert!(matches!(&aggregator.group_expr[0], Expr::Alias(a) if a.name == "market"));
        assert!(matches!(&aggregator.group_expr[1], Expr::Alias(a) if a.name == "user_id"));

        assert_eq!(aggregator.aggr_expr.len(), 1);
        assert_aliased_function(&aggregator.aggr_expr[0], "count");

        let schema = &aggregator.agg_schema;
        assert_eq!(schema.fields().len(), 3); // market + user_id + count

        let market_field = schema.field_with_unqualified_name("market").unwrap();
        assert_eq!(market_field.name(), "market");
        assert_eq!(market_field.data_type(), &DataType::Utf8);

        let user_id_field = schema.field_with_unqualified_name("user_id").unwrap();
        assert_eq!(user_id_field.name(), "user_id");
        assert_eq!(user_id_field.data_type(), &DataType::Int64);

        let count_field = &schema.fields()[2];
        assert_eq!(count_field.data_type(), &DataType::Int64);

        let function_sql = aggregator.generate_function_sql().unwrap();
        let trigger_sql = aggregator.generate_trigger_statement_sql().unwrap();
        debug!("Generated function SQL:\n{}", function_sql);
        debug!("Generated trigger SQL:\n{}", trigger_sql);

        assert!(function_sql.contains(
            r#"CREATE OR REPLACE FUNCTION "public"."polymarket_transaction_to_polymarket_transaction_agg_fn"()"#
        ));
        assert!(function_sql.contains(
            r#"INSERT INTO "public"."polymarket_transaction_agg" ("market", "user_id", "count")"#,
        ));
        assert!(function_sql.contains(
            r#"SELECT new_table."market" AS "market", new_table."user_id" AS "user_id", SUM(CASE WHEN new_table."_gs_op" = 'd' THEN -1 WHEN new_table."_gs_op" = 'u' THEN 0 ELSE 1 END) AS "count""#
        ));
        assert!(function_sql.contains("FROM new_table"));
        assert!(function_sql.contains(r#"GROUP BY new_table."market", new_table."user_id""#));
        assert!(function_sql.contains(r#"ON CONFLICT ("market", "user_id")"#));
        assert!(function_sql.contains(
            r#"DO UPDATE SET "count" = "public"."polymarket_transaction_agg"."count" + EXCLUDED."count""#
        ));
        assert!(function_sql.contains("RETURN NULL"));

        assert!(trigger_sql.contains("REFERENCING NEW TABLE AS new_table"));
        assert!(trigger_sql.contains("FOR EACH STATEMENT"));
    }

    #[tokio::test]
    async fn test_aggregation_without_group_by() {
        use crate::topology::{AggregateColumn, AggregateFunction};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            true,
        )]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let group_by = IndexMap::new();

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "total_count".to_string(),
            AggregateColumn {
                from: None,
                function: AggregateFunction::Count,
                pg_type: None,
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "polymarket_transaction".to_string(),
            "id".to_string(),
            "polymarket_transaction_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        assert_eq!(aggregator.group_expr.len(), 0);
        assert_eq!(aggregator.aggr_expr.len(), 1);
        assert_aliased_function(&aggregator.aggr_expr[0], "count");

        let schema = &aggregator.agg_schema;
        assert_eq!(schema.fields().len(), 1); // Only count field

        let count_field = &schema.fields()[0];
        assert_eq!(count_field.data_type(), &DataType::Int64);

        // Test trigger generation without group by (uses sentinel key)
        let function_sql = aggregator.generate_function_sql().unwrap();
        let trigger_sql = aggregator.generate_trigger_statement_sql().unwrap();
        debug!("Generated function SQL:\n{}", function_sql);
        debug!("Generated trigger SQL:\n{}", trigger_sql);

        assert!(function_sql.contains(
            r#"CREATE OR REPLACE FUNCTION "public"."polymarket_transaction_to_polymarket_transaction_agg_fn"()"#
        ));
        assert!(function_sql.contains(
            r#"INSERT INTO "public"."polymarket_transaction_agg" ("key", "total_count")"#,
        ));
        assert!(function_sql.contains(r#"SELECT 'value' AS "key", SUM(CASE WHEN new_table."_gs_op" = 'd' THEN -1 WHEN new_table."_gs_op" = 'u' THEN 0 ELSE 1 END) AS "total_count""#));
        assert!(function_sql.contains("FROM new_table"));
        assert!(!function_sql.contains("GROUP BY")); // No GROUP BY for global aggregation
        assert!(function_sql.contains(r#"ON CONFLICT ("key")"#));
        assert!(function_sql.contains(
            r#"DO UPDATE SET "total_count" = "public"."polymarket_transaction_agg"."total_count" + EXCLUDED."total_count""#
        ));
        assert!(function_sql.contains("RETURN NULL"));

        assert!(trigger_sql.contains("REFERENCING NEW TABLE AS new_table"));
        assert!(trigger_sql.contains("FOR EACH STATEMENT"));
    }

    #[tokio::test]
    async fn test_min_max_aggregations() {
        use crate::topology::{AggregateColumn, AggregateFunction, GroupByColumn};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let mut group_by = IndexMap::new();
        group_by.insert(
            "market".to_string(),
            GroupByColumn {
                from: None,
                pg_type: None,
            },
        );

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "min_amount".to_string(),
            AggregateColumn {
                from: Some("amount".to_string()),
                function: AggregateFunction::Min,
                pg_type: None,
            },
        );
        aggregate.insert(
            "max_amount".to_string(),
            AggregateColumn {
                from: Some("amount".to_string()),
                function: AggregateFunction::Max,
                pg_type: None,
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "polymarket_transaction".to_string(),
            "id".to_string(),
            "polymarket_transaction_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        assert_eq!(aggregator.group_expr.len(), 1);
        assert!(matches!(&aggregator.group_expr[0], Expr::Alias(a) if a.name == "market"));

        assert_eq!(aggregator.aggr_expr.len(), 2);
        assert_aliased_function(&aggregator.aggr_expr[0], "min");
        assert_aliased_function(&aggregator.aggr_expr[1], "max");

        let schema = &aggregator.agg_schema;
        assert_eq!(schema.fields().len(), 3); // market + min_amount + max_amount

        let market_field = schema.field_with_unqualified_name("market").unwrap();
        assert_eq!(market_field.name(), "market");
        assert_eq!(market_field.data_type(), &DataType::Utf8);

        let min_field = &schema.fields()[1];
        assert_eq!(min_field.data_type(), &DataType::Int64);

        let max_field = &schema.fields()[2];
        assert_eq!(max_field.data_type(), &DataType::Int64);

        let function_sql = aggregator.generate_function_sql().unwrap();
        let trigger_sql = aggregator.generate_trigger_statement_sql().unwrap();
        debug!("Generated function SQL:\n{}", function_sql);
        debug!("Generated trigger SQL:\n{}", trigger_sql);

        assert!(function_sql.contains(
            r#"CREATE OR REPLACE FUNCTION "public"."polymarket_transaction_to_polymarket_transaction_agg_fn"()"#
        ));
        assert!(function_sql.contains(
            r#"INSERT INTO "public"."polymarket_transaction_agg" ("market", "min_amount", "max_amount")"#
        ));
        assert!(function_sql.contains(r#"SELECT new_table."market" AS "market", MIN(new_table."amount") AS "min_amount", MAX(new_table."amount") AS "max_amount""#));
        assert!(function_sql.contains("FROM new_table"));
        assert!(function_sql.contains(r#"GROUP BY new_table."market""#));
        assert!(function_sql.contains(r#"ON CONFLICT ("market")"#));
        assert!(function_sql.contains(
            r#""min_amount" = LEAST("public"."polymarket_transaction_agg"."min_amount", EXCLUDED."min_amount")"#
        ));
        assert!(function_sql.contains(
            r#""max_amount" = GREATEST("public"."polymarket_transaction_agg"."max_amount", EXCLUDED."max_amount")"#
        ));
        assert!(function_sql.contains("RETURN NULL"));

        assert!(trigger_sql.contains("REFERENCING NEW TABLE AS new_table"));
        assert!(trigger_sql.contains("FOR EACH STATEMENT"));
    }

    #[tokio::test]
    async fn test_generate_target_table_with_group_by() {
        use crate::topology::{AggregateColumn, AggregateFunction, GroupByColumn};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let mut group_by = IndexMap::new();
        group_by.insert(
            "market".to_string(),
            GroupByColumn {
                from: None,
                pg_type: None,
            },
        );

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "sum_amount".to_string(),
            AggregateColumn {
                from: Some("amount".to_string()),
                function: AggregateFunction::Sum,
                pg_type: None,
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "test_table".to_string(),
            "id".to_string(),
            "test_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        let table_sql = aggregator.generate_target_table_sql().unwrap();
        debug!("Generated table SQL:\n{}", table_sql);

        assert!(table_sql.contains(r#"CREATE TABLE IF NOT EXISTS "public"."test_agg""#));
        assert!(table_sql.contains(r#""market" TEXT NOT NULL"#));
        assert!(table_sql.contains(r#""sum_amount" BIGINT NOT NULL DEFAULT 0"#));
        assert!(table_sql.contains(r#"PRIMARY KEY ("market")"#));
    }

    #[tokio::test]
    async fn test_generate_target_table_multiple_aggregations() {
        use crate::topology::{AggregateColumn, AggregateFunction, GroupByColumn};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let mut group_by = IndexMap::new();
        group_by.insert(
            "market".to_string(),
            GroupByColumn {
                from: None,
                pg_type: None,
            },
        );

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "count".to_string(),
            AggregateColumn {
                from: None,
                function: AggregateFunction::Count,
                pg_type: None,
            },
        );
        aggregate.insert(
            "avg_amount".to_string(),
            AggregateColumn {
                from: Some("amount".to_string()),
                function: AggregateFunction::Avg,
                pg_type: None,
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "test_table".to_string(),
            "id".to_string(),
            "test_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        let table_sql = aggregator.generate_target_table_sql().unwrap();
        debug!("Generated table SQL:\n{}", table_sql);

        assert!(table_sql.contains(r#"CREATE TABLE IF NOT EXISTS "public"."test_agg""#));
        assert!(table_sql.contains(r#""market" TEXT NOT NULL"#));
        assert!(table_sql.contains(r#""count" BIGINT NOT NULL DEFAULT 0"#));
        assert!(table_sql.contains(r#""avg_amount_sum" BIGINT NOT NULL DEFAULT 0"#));
        assert!(table_sql.contains(r#""avg_amount_count" BIGINT NOT NULL DEFAULT 0"#));
        assert!(table_sql.contains(r#"PRIMARY KEY ("market")"#));
    }

    #[tokio::test]
    async fn test_generate_target_table_without_group_by() {
        use crate::topology::{AggregateColumn, AggregateFunction};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Int64,
            true,
        )]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let group_by = IndexMap::new();

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "count".to_string(),
            AggregateColumn {
                from: None,
                function: AggregateFunction::Count,
                pg_type: None,
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "test_table".to_string(),
            "id".to_string(),
            "test_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        let table_sql = aggregator.generate_target_table_sql().unwrap();
        debug!("Generated table SQL:\n{}", table_sql);

        assert!(table_sql.contains(r#"CREATE TABLE IF NOT EXISTS "public"."test_agg""#));
        assert!(table_sql.contains(r#""key" TEXT NOT NULL"#));
        assert!(table_sql.contains(r#""count" BIGINT NOT NULL DEFAULT 0"#));
        assert!(table_sql.contains(r#"PRIMARY KEY ("key")"#));
    }

    #[tokio::test]
    async fn test_generate_target_table_multiple_group_by() {
        use crate::topology::{AggregateColumn, AggregateFunction, GroupByColumn};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("user_id", DataType::Int64, true),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let mut group_by = IndexMap::new();
        group_by.insert(
            "market".to_string(),
            GroupByColumn {
                from: None,
                pg_type: None,
            },
        );
        group_by.insert(
            "user_id".to_string(),
            GroupByColumn {
                from: None,
                pg_type: None,
            },
        );

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "count".to_string(),
            AggregateColumn {
                from: None,
                function: AggregateFunction::Count,
                pg_type: None,
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "test_table".to_string(),
            "id".to_string(),
            "test_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        let table_sql = aggregator.generate_target_table_sql().unwrap();
        debug!("Generated table SQL:\n{}", table_sql);

        assert!(table_sql.contains(r#"CREATE TABLE IF NOT EXISTS "public"."test_agg""#));
        assert!(table_sql.contains(r#""market" TEXT NOT NULL"#));
        assert!(table_sql.contains(r#""user_id" BIGINT"#));
        assert!(table_sql.contains(r#""count" BIGINT NOT NULL DEFAULT 0"#));
        assert!(table_sql.contains(r#"PRIMARY KEY ("market", "user_id")"#));
    }

    #[tokio::test]
    async fn test_invalid_source_field_fails() {
        use crate::topology::{AggregateColumn, AggregateFunction};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let group_by = IndexMap::new();

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "sum_amount".to_string(),
            AggregateColumn {
                from: Some("nonexistent".to_string()),
                function: AggregateFunction::Sum,
                pg_type: None,
            },
        );

        let result = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "test_table".to_string(),
            "id".to_string(),
            "test_agg".to_string(),
            "public".to_string(),
        );

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("No field named"));
    }

    #[tokio::test]
    async fn test_invalid_postgres_type_fails() {
        use crate::topology::{AggregateColumn, AggregateFunction};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let group_by = IndexMap::new();

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "sum_amount".to_string(),
            AggregateColumn {
                from: Some("amount".to_string()),
                function: AggregateFunction::Sum,
                pg_type: Some("invalid_type".to_string()),
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "test_table".to_string(),
            "id".to_string(),
            "test_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        let result = aggregator.build_target_schema();

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("unsupported PostgreSQL type"));
    }

    #[tokio::test]
    async fn test_type_override_in_schema() {
        use crate::topology::{AggregateColumn, AggregateFunction, GroupByColumn};

        let _ = create_test_context().await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("market", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let mut group_by = IndexMap::new();
        group_by.insert(
            "market".to_string(),
            GroupByColumn {
                from: None,
                pg_type: Some("varchar(100)".to_string()),
            },
        );

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "sum_amount".to_string(),
            AggregateColumn {
                from: Some("amount".to_string()),
                function: AggregateFunction::Sum,
                pg_type: Some("numeric(30,5)".to_string()),
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "test_table".to_string(),
            "id".to_string(),
            "test_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        let table_sql = aggregator.generate_target_table_sql().unwrap();
        assert!(table_sql.contains("VARCHAR(100)"));
        assert!(table_sql.contains("NUMERIC(30,5)"));
    }

    #[tokio::test]
    async fn test_empty_aggregate_config_fails() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "market",
            DataType::Utf8,
            false,
        )]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let group_by = IndexMap::new();
        let aggregate = IndexMap::new();

        let result = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "test_table".to_string(),
            "id".to_string(),
            "test_agg".to_string(),
            "public".to_string(),
        );

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("at least one aggregate column is required"));
    }

    #[tokio::test]
    async fn test_group_by_column_renaming_via_from() {
        use crate::topology::{AggregateColumn, AggregateFunction, GroupByColumn};

        let _ = tracing_subscriber::fmt::try_init();

        let schema = Arc::new(Schema::new(vec![
            Field::new("market_name", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
        ]));
        let df_schema = Arc::new(DFSchema::try_from((*schema).clone()).unwrap());

        let mut group_by = IndexMap::new();
        group_by.insert(
            "market".to_string(),
            GroupByColumn {
                from: Some("market_name".to_string()),
                pg_type: None,
            },
        );

        let mut aggregate = IndexMap::new();
        aggregate.insert(
            "count".to_string(),
            AggregateColumn {
                from: None,
                function: AggregateFunction::Count,
                pg_type: None,
            },
        );

        let aggregator = PostgresAggregator::try_new(
            &group_by,
            &aggregate,
            df_schema,
            "test_table".to_string(),
            "id".to_string(),
            "test_agg".to_string(),
            "public".to_string(),
        )
        .unwrap();

        let function_sql = aggregator.generate_function_sql().unwrap();
        debug!("Generated function SQL:\n{}", function_sql);

        // Output column should use the configured name "market", not the source "market_name"
        assert!(function_sql.contains(r#"("market", "count")"#));
        // SELECT should reference source column but alias to output name
        assert!(function_sql.contains(r#"new_table."market_name" AS "market""#));
        // GROUP BY should use the source column name
        assert!(function_sql.contains(r#"GROUP BY new_table."market_name""#));
        // ON CONFLICT should use the output column name
        assert!(function_sql.contains(r#"ON CONFLICT ("market")"#));

        let table_sql = aggregator.generate_target_table_sql().unwrap();
        debug!("Generated table SQL:\n{}", table_sql);

        // Target table should use the output name
        assert!(table_sql.contains(r#""market" TEXT NOT NULL"#));
        assert!(table_sql.contains(r#"PRIMARY KEY ("market")"#));
    }
}
