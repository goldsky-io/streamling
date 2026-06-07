use datafusion::logical_expr::sqlparser::ast::{SetExpr, Statement, TableFactor};
use datafusion::logical_expr::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use std::collections::HashMap;
use std::collections::HashSet;

fn extract_from_setexpr(expr: &SetExpr, tables: &mut Vec<String>) -> Result<(), ParserError> {
    match expr {
        SetExpr::Select(select) if select.from.len() == 1 => {
            let table_with_joins = select
                .from
                .first()
                .expect("expected at least one FROM <table>");
            if !table_with_joins.joins.is_empty() {
                return Err(ParserError::ParserError(
                    "JOINs are not supported for table reference extraction".into(),
                ));
            }
            match &table_with_joins.relation {
                TableFactor::Table { name, .. } => {
                    let table_name = name.to_string();
                    tables.push(table_name);
                    Ok(())
                }
                TableFactor::Derived { subquery, .. } => {
                    // Resolve the base table of the subquery
                    extract_from_setexpr(&subquery.body, tables)
                }
                _ => Err(ParserError::ParserError(
                    "Only tables with from <table_name> is supported".into(),
                )),
            }
        }
        SetExpr::Select(_) => Err(ParserError::ParserError(
            "Expected single query with FROM".into(),
        )),
        SetExpr::SetOperation { left, right, .. } => {
            extract_from_setexpr(left, tables)?;
            extract_from_setexpr(right, tables)?;
            Ok(())
        }
        _ => Err(ParserError::ParserError(
            "Only SELECT query supported".into(),
        )),
    }
}

fn extract_from_setexpr_with_ctes(
    expr: &SetExpr,
    cte_base_table_by_name: &HashMap<String, String>,
    tables: &mut Vec<String>,
) -> Result<(), ParserError> {
    match expr {
        SetExpr::Select(select) if select.from.len() == 1 => {
            let table_with_joins = select
                .from
                .first()
                .expect("expected at least one FROM <table>");
            if !table_with_joins.joins.is_empty() {
                return Err(ParserError::ParserError(
                    "JOINs are not supported for table reference extraction".into(),
                ));
            }
            match &table_with_joins.relation {
                TableFactor::Table { name, .. } => {
                    let name_str = name.to_string();
                    if let Some(base) = cte_base_table_by_name.get(&name_str) {
                        tables.push(base.clone());
                    } else {
                        tables.push(name_str);
                    }
                    Ok(())
                }
                TableFactor::Derived { subquery, .. } => {
                    // Resolve the base table of the subquery
                    extract_from_setexpr_with_ctes(&subquery.body, cte_base_table_by_name, tables)
                }
                _ => Err(ParserError::ParserError(
                    "Only tables with from <table_name> is supported".into(),
                )),
            }
        }
        SetExpr::Select(_) => Err(ParserError::ParserError(
            "Expected single query with FROM".into(),
        )),
        SetExpr::SetOperation { left, right, .. } => {
            extract_from_setexpr_with_ctes(left, cte_base_table_by_name, tables)?;
            extract_from_setexpr_with_ctes(right, cte_base_table_by_name, tables)?;
            Ok(())
        }
        _ => Err(ParserError::ParserError(
            "Only SELECT query supported".into(),
        )),
    }
}

pub fn extract_table_references_from_sql(sql: &str) -> Result<Vec<String>, ParserError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;

    if statements.len() != 1 {
        return Err(ParserError::ParserError(
            "Only single SQL statement supported".into(),
        ));
    }

    let statement = &statements[0];
    let mut tables = Vec::new();

    match statement {
        Statement::Query(query) => {
            // Resolve CTEs (non-recursive) to their underlying base tables
            let mut cte_base_table_by_name: HashMap<String, String> = HashMap::new();
            if let Some(with) = &query.with {
                if with.recursive {
                    return Err(ParserError::ParserError(
                        "Recursive CTEs are not supported".into(),
                    ));
                }
                for cte in &with.cte_tables {
                    // Each CTE can reference previously defined CTEs, so resolve in order
                    let mut cte_tables = Vec::new();
                    extract_from_setexpr_with_ctes(
                        &cte.query.body,
                        &cte_base_table_by_name,
                        &mut cte_tables,
                    )?;
                    // Deduplicate to find unique base tables (CTEs can have UNION ALL with same table)
                    let unique_cte_tables: HashSet<String> = cte_tables.into_iter().collect();
                    if unique_cte_tables.len() == 1 {
                        let base = unique_cte_tables.into_iter().next().unwrap();
                        let cte_name = cte.alias.name.to_string();
                        cte_base_table_by_name.insert(cte_name, base);
                    } else {
                        return Err(ParserError::ParserError(
                            "CTEs must reference exactly one base table".into(),
                        ));
                    }
                }
            }
            if cte_base_table_by_name.is_empty() {
                extract_from_setexpr(&query.body, &mut tables)?;
            } else {
                extract_from_setexpr_with_ctes(&query.body, &cte_base_table_by_name, &mut tables)?;
            }
            Ok(tables)
        }
        _ => Err(ParserError::ParserError(
            "Only SELECT query supported".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select_query() {
        let sql = "SELECT * FROM users";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["users"]);
    }

    #[test]
    fn test_select_with_columns() {
        let sql = "SELECT id, name FROM customers";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["customers"]);
    }

    #[test]
    fn test_select_with_where_clause() {
        let sql = "SELECT * FROM orders WHERE status = 'active'";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["orders"]);
    }

    #[test]
    fn test_select_with_quoted_table_name() {
        let sql = "SELECT * FROM \"my_table\"";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["\"my_table\""]);
    }

    #[test]
    fn test_select_with_schema_qualified_table() {
        let sql = "SELECT * FROM schema.table_name";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["schema.table_name"]);
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let sql = "select * from Products";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["Products"]);
    }

    #[test]
    fn test_multiple_statements_error() {
        let sql = "SELECT * FROM users; SELECT * FROM orders;";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Only single SQL statement supported")
        );
    }

    #[test]
    fn test_non_select_statement_error() {
        let sql = "INSERT INTO users (name) VALUES ('John')";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Only SELECT query supported")
        );
    }

    #[test]
    fn test_update_statement_error() {
        let sql = "UPDATE users SET name = 'Jane' WHERE id = 1";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Only SELECT query supported")
        );
    }

    #[test]
    fn test_delete_statement_error() {
        let sql = "DELETE FROM users WHERE id = 1";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Only SELECT query supported")
        );
    }

    #[test]
    fn test_multiple_tables_in_from_error() {
        let sql = "SELECT * FROM users, orders";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Expected single query with FROM")
        );
    }

    #[test]
    fn test_join_query_error() {
        // JOINs are not supported for table reference extraction.
        let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_subquery_ok() {
        let sql = "SELECT * FROM (SELECT * FROM users) AS subquery";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["users"]);
    }

    #[test]
    fn test_union_query_same_table_ok() {
        let sql = "SELECT * FROM users UNION ALL SELECT * FROM users";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        // Should return both references to enable scan sharing
        assert_eq!(result.unwrap(), vec!["users", "users"]);
    }

    #[test]
    fn test_cte_with_union_all_ethereum_query() {
        let sql = r#"
        WITH ledger AS (
            SELECT
                block_number,
                id,
                address AS contract_address,
                event_params[2] AS owner_address,
                COALESCE(TRY_CAST(event_params[3] AS DECIMAL(100, 0)), 0) AS `value`
            FROM
                ethereum.decoded_logs
            WHERE
                LOWER(raw_log.topics) LIKE '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef%'
                AND SPLIT_INDEX(raw_log.topics, ',', 3) IS NULL
 
            UNION ALL
 
            SELECT
                block_number,
                id,
                address AS contract_address,
                event_params[1] AS owner_address,
                -COALESCE(TRY_CAST(event_params[3] AS DECIMAL(100, 0)), 0) AS `value`
            FROM
                ethereum.decoded_logs
            WHERE
                LOWER(raw_log.topics) LIKE '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef%'
                AND SPLIT_INDEX(raw_log.topics, ',', 3) IS NULL
        )
 
        SELECT
            MD5(contract_address || owner_address) AS id,
            contract_address,
            owner_address,
            GREATEST(0, SUM(`value`)) AS balance,
            MIN(`block_number`) AS first_updated,
            MAX(`block_number`) AS last_updated
        FROM
            ledger
        WHERE
            `owner_address` <> '0x0000000000000000000000000000000000000000'
        GROUP BY
            contract_address,
            owner_address
        "#;
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["ethereum.decoded_logs"]);
    }

    #[test]
    fn test_cte_extracts_accurate_table_name() {
        let sql = r#"
        WITH transactions_unnested AS (
        SELECT unnest(array_enumerate(transactions)) AS tx, slot, blockhash, "blockTime"
        FROM raw_blocks
      ),
      transaction_accounts_base AS (
        SELECT
          b.slot as block_slot,
          b.blockhash as block_hash,
          b."blockTime" as block_time,
          tx.index as tx_index,
          tx.value.transaction.signatures[1] AS tx_signature,
          tx.value.meta.err AS tx_err,
          tx.value.meta.fee AS tx_fee,
          _gs_solana_get_accounts(
            tx.value.transaction.message."accountKeys",
            tx.value.meta."loadedAddresses".readonly,
            tx.value.meta."loadedAddresses".writable,
            CAST(tx.value.transaction.message.header."numRequiredSignatures" AS TINYINT UNSIGNED),
            CAST(tx.value.transaction.message.header."numReadonlySignedAccounts" AS TINYINT UNSIGNED),
            CAST(tx.value.transaction.message.header."numReadonlyUnsignedAccounts" AS TINYINT UNSIGNED),
            'transaction'
          ) AS tx_accounts,
          tx.value.transaction.message.instructions AS tx_instructions,
          tx.value.meta."innerInstructions" AS tx_inner_instructions
        FROM transactions_unnested AS b
      )
      SELECT * from transaction_accounts_base
        "#;
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["raw_blocks"]);
    }

    #[test]
    fn test_union_query_different_tables() {
        let sql = "SELECT * FROM users UNION SELECT * FROM customers";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        let tables = result.unwrap();
        // Order matters now that we preserve duplicates, but UNION (not ALL) won't have duplicates
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"customers".to_string()));
    }

    #[test]
    fn test_invalid_sql_syntax_error() {
        let sql = "SELECT * FROM";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_sql_error() {
        let sql = "";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_whitespace_handling() {
        let sql = "  SELECT   *   FROM   users  ";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["users"]);
    }

    #[test]
    fn test_newlines_in_sql() {
        let sql = "SELECT *\nFROM users\nWHERE id > 0";
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["users"]);
    }

    #[test]
    fn test_cte_simple_select_ok() {
        let sql = r#"
            WITH recent_orders AS (
                SELECT * FROM orders
            )
            SELECT * FROM recent_orders
        "#;
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["orders"]);
    }

    #[test]
    fn test_cte_nested_cte_ok() {
        let sql = r#"
        WITH first_cte AS (
            SELECT id, value FROM raw_blocks
        ),
        second_cte AS (
            SELECT id, value FROM first_cte
        ),
        third_cte AS (
            SELECT id, value
            FROM (
                SELECT id, value FROM second_cte
            ) a

            UNION ALL

            SELECT id, value
            FROM (
                SELECT id, value
                FROM (
                    SELECT id, value FROM second_cte
                ) inner_a
            ) outer_a
        )
        SELECT id, value FROM third_cte
        "#;
        let result = extract_table_references_from_sql(sql);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec!["raw_blocks"]);
    }
}
