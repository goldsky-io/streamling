use datafusion::common::ScalarValue;

#[derive(Debug, Clone)]
pub struct ClickHousePaginationConfig {
    pub sorting_keys: Vec<String>,
    pub page_size: usize,
}

#[derive(Debug, Clone)]
pub struct ClickHouseQueryBuilder {
    query: String,
    table_name: String,
    columns: Vec<String>,
    where_clause: Option<String>,
    pagination_config: Option<ClickHousePaginationConfig>,
    current_keyset: Option<Vec<ScalarValue>>, // Current pagination state
    is_start_at: bool, // True if this keyset is from start_at (use >=), false if pagination boundary (use >)
    block_range_upper_bound: Option<ScalarValue>, // Upper bound (exclusive) on first sorting key for block range pagination
}

impl ClickHouseQueryBuilder {
    const VIRTUAL_GS_OP_FIELD: &str = "CASE WHEN is_deleted=0 THEN 'i' ELSE 'd' END AS _gs_op";

    // Helper function to format ScalarValue for SQL with proper quoting
    fn format_scalar_for_sql(value: &ScalarValue) -> String {
        match value {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
            }
            _ => value.to_string(), // For numbers, dates, etc., use default string representation
        }
    }

    // Unwind tuple comparison for better performance
    // Converts (a,b,c) > (1,2,3) into (a > 1) OR (a = 1 AND b > 2) OR (a = 1 AND b = 2 AND c > 3)
    fn build_keyset_conditions(
        sorting_keys: &[String],
        keyset: &[ScalarValue],
        operator: &str,
    ) -> String {
        let mut conditions = Vec::new();
        let len = sorting_keys.len().min(keyset.len());

        for i in 0..len {
            let mut condition_parts = Vec::new();

            // Add equality conditions for all preceding keys
            for j in 0..i {
                condition_parts.push(format!(
                    "{} = {}",
                    sorting_keys[j],
                    Self::format_scalar_for_sql(&keyset[j])
                ));
            }

            // Add the comparison condition for the current key
            condition_parts.push(format!(
                "{} {} {}",
                sorting_keys[i],
                operator,
                Self::format_scalar_for_sql(&keyset[i])
            ));

            conditions.push(format!("({})", condition_parts.join(" AND ")));
        }

        conditions.join(" OR ")
    }

    pub fn of(
        table_name: String,
        columns: Vec<String>,
        where_clause: Option<String>,
        config: Option<ClickHousePaginationConfig>,
    ) -> Self {
        // We use an empty string since rebuild_query() will generate the actual query
        ClickHouseQueryBuilder {
            query: String::new(),
            table_name,
            columns,
            where_clause,
            pagination_config: config,
            current_keyset: None,
            is_start_at: false,
            block_range_upper_bound: None,
        }
    }

    pub fn start_at_page(&mut self, args: Vec<ScalarValue>) -> &mut Self {
        // Store the start_at keyset so it survives query rebuilding
        self.current_keyset = Some(args);
        self.is_start_at = true;
        self
    }

    // Convenient method to update pagination state and rebuild query
    pub fn update_keyset(&mut self, keyset: Vec<ScalarValue>) -> &mut Self {
        self.current_keyset = Some(keyset);
        self.is_start_at = false; // This is a pagination boundary, not start_at
        self.rebuild_query();
        self
    }

    #[allow(dead_code)]
    pub fn clear_keyset(&mut self) -> &mut Self {
        self.current_keyset = None;
        self.is_start_at = false;
        self
    }

    pub fn set_block_range_upper_bound(&mut self, value: Option<ScalarValue>) -> &mut Self {
        self.block_range_upper_bound = value;
        self
    }

    // Rebuild the query with current pagination state
    fn rebuild_query(&mut self) {
        // Build the CTE with SELECT * FROM table WHERE ... ORDER BY ...
        let mut cte_query = format!("SELECT * FROM {}", self.table_name);

        // Add original where clause if it exists
        // put in parentheses to ensure proper precedence
        if let Some(ref where_clause) = self.where_clause {
            cte_query = format!("{} WHERE ({})", cte_query, where_clause);
        }

        // Track whether we already have a WHERE clause
        let has_where = self.where_clause.is_some();
        let mut added_conditions = has_where;

        // Add block range upper bound on the first sorting key
        if let (Some(pagination_config), Some(upper_bound)) =
            (&self.pagination_config, &self.block_range_upper_bound)
            && let Some(first_key) = pagination_config.sorting_keys.first()
        {
            let bound_clause = format!(
                "{} < {}",
                first_key,
                Self::format_scalar_for_sql(upper_bound)
            );
            let connector = if added_conditions { "AND" } else { "WHERE" };
            cte_query = format!("{} {} ({})", cte_query, connector, bound_clause);
            added_conditions = true;
        }

        // Add pagination clause if we have a keyset
        if let (Some(pagination_config), Some(keyset)) =
            (&self.pagination_config, &self.current_keyset)
        {
            let operator = if self.is_start_at { ">=" } else { ">" };
            let conditions =
                Self::build_keyset_conditions(&pagination_config.sorting_keys, keyset, operator);
            if !conditions.is_empty() {
                let connector = if added_conditions { "AND" } else { "WHERE" };
                cte_query = format!("{} {} ({})", cte_query, connector, conditions);
            }
        }

        // Add ORDER BY clause to CTE
        if let Some(pagination_config) = &self.pagination_config {
            let sorting_keys_str = pagination_config.sorting_keys.join(",");
            if !sorting_keys_str.trim().is_empty() {
                cte_query = format!("{} ORDER BY {}", cte_query, sorting_keys_str);
            }
        }

        // Build the final SELECT statement that selects columns from the CTE
        // Remove _gs_op if present since we create our own virtual column
        let select_columns: Vec<String> = self
            .columns
            .iter()
            .filter(|col| *col != "_gs_op")
            .cloned()
            .collect();

        let final_select = format!(
            "SELECT {},\n{}",
            select_columns.join(",\n"),
            Self::VIRTUAL_GS_OP_FIELD
        );

        // Combine CTE and final SELECT
        let query = format!("WITH t AS (\n  {}\n)\n{} FROM t", cte_query, final_select);

        self.query = query;
    }

    // Get current query for execution
    // Always rebuilds query to ensure it reflects current state (keyset, etc.)
    pub fn get_query(&mut self) -> &str {
        self.rebuild_query();
        self.query.as_str()
    }

    // Get pagination config (for accessing page_size)
    pub fn pagination_config(&self) -> Option<&ClickHousePaginationConfig> {
        self.pagination_config.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_query_without_where() {
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string(), "name".to_string()],
            None,
            None,
        );
        let query = builder.get_query();
        // Query will be built with CTE structure
        assert!(query.contains("WITH t AS"));
        assert!(query.contains("SELECT * FROM test_table"));
        assert!(query.contains("id"));
        assert!(query.contains("name"));
    }

    #[test]
    fn test_basic_query_with_where() {
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string(), "name".to_string()],
            Some("id > 10".to_string()),
            None,
        );
        let query = builder.get_query();
        // Query will be built with CTE structure
        assert!(query.contains("WITH t AS"));
        assert!(query.contains("SELECT * FROM test_table"));
        assert!(query.contains("WHERE (id > 10)"));
        assert!(query.contains("id"));
        assert!(query.contains("name"));
    }

    #[test]
    fn test_query_with_order_by() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["id".to_string(), "timestamp".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string(), "name".to_string()],
            None,
            Some(pagination_config),
        );
        let query = builder.get_query();

        // Should have CTE structure
        assert!(query.contains("WITH t AS"));
        assert!(query.contains("SELECT * FROM test_table"));
        assert!(query.contains("ORDER BY id,timestamp"));
        assert!(query.contains("SELECT id,\nname"));
        assert!(query.contains("_gs_op"));
        assert!(query.contains("FROM t"));
    }

    #[test]
    fn test_query_with_where_and_order_by() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string(), "name".to_string()],
            Some("status = 'active'".to_string()),
            Some(pagination_config),
        );
        let query = builder.get_query();

        assert!(query.contains("WITH t AS"));
        assert!(query.contains("SELECT * FROM test_table"));
        assert!(query.contains("WHERE (status = 'active')"));
        assert!(query.contains("ORDER BY id"));
    }

    #[test]
    fn test_query_with_start_at_keyset() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string(), "name".to_string()],
            None,
            Some(pagination_config),
        );
        builder.start_at_page(vec![ScalarValue::Int64(Some(100))]);
        let query = builder.get_query();

        // Should use >= for start_at
        assert!(query.contains("id >= 100"));
        assert!(query.contains("ORDER BY id"));
    }

    #[test]
    fn test_query_with_pagination_keyset() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string(), "name".to_string()],
            None,
            Some(pagination_config),
        );
        builder.update_keyset(vec![ScalarValue::Int64(Some(100))]);
        let query = builder.get_query();

        // Should use > for pagination boundary
        assert!(query.contains("id > 100"));
    }

    #[test]
    fn test_query_with_multiple_sorting_keys() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string(), "block_number".to_string()],
            None,
            Some(pagination_config),
        );
        builder.start_at_page(vec![
            ScalarValue::Int64(Some(1000)),
            ScalarValue::Int64(Some(50)),
        ]);
        let query = builder.get_query();

        // Should have proper tuple comparison unwinding
        assert!(query.contains("block_number >= 1000"));
        assert!(query.contains("block_number = 1000 AND id >= 50"));
        assert!(query.contains("ORDER BY block_number,id"));
    }

    #[test]
    fn test_query_with_string_keyset() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["address".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["address".to_string(), "value".to_string()],
            None,
            Some(pagination_config),
        );
        builder.start_at_page(vec![ScalarValue::Utf8(Some("0x1234".to_string()))]);
        let query = builder.get_query();

        // Should properly quote strings
        assert!(query.contains("address >= '0x1234'"));
    }

    #[test]
    fn test_query_with_string_escaping() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["name".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["name".to_string()],
            None,
            Some(pagination_config),
        );
        builder.start_at_page(vec![ScalarValue::Utf8(Some("O'Reilly".to_string()))]);
        let query = builder.get_query();

        // Should escape single quotes
        assert!(query.contains("name >= 'O\\'Reilly'"));
    }

    #[test]
    fn test_query_with_where_and_keyset() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string(), "name".to_string()],
            Some("status = 'active'".to_string()),
            Some(pagination_config),
        );
        builder.start_at_page(vec![ScalarValue::Int64(Some(100))]);
        let query = builder.get_query();

        // Should combine WHERE and keyset with AND
        assert!(query.contains("WHERE (status = 'active')"));
        // The keyset condition is wrapped in parentheses: AND ((id >= 100))
        assert!(query.contains("id >= 100"));
        assert!(query.contains("AND"));
    }

    #[test]
    fn test_query_column_formatting() {
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec![
                "id".to_string(),
                "user_name".to_string(),
                "created_at".to_string(),
            ],
            None,
            None,
        );
        let query = builder.get_query();

        // Columns should not be backtick-quoted in final SELECT
        assert!(query.contains("id"));
        assert!(query.contains("user_name"));
        assert!(query.contains("created_at"));
    }

    #[test]
    fn test_query_includes_gs_op() {
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string()],
            None,
            None,
        );
        let query = builder.get_query();

        // Should include _gs_op virtual field
        assert!(query.contains("_gs_op"));
        assert!(query.contains("CASE WHEN is_deleted=0 THEN 'i' ELSE 'd' END AS _gs_op"));
    }

    #[test]
    fn test_query_filters_out_gs_op_from_columns() {
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string(), "_gs_op".to_string(), "name".to_string()],
            None,
            None,
        );
        let query = builder.get_query();

        // Should include id and name columns
        assert!(query.contains("id"));
        assert!(query.contains("name"));
        // Should include the virtual _gs_op field
        assert!(query.contains("CASE WHEN is_deleted=0 THEN 'i' ELSE 'd' END AS _gs_op"));
        // Should not include _gs_op as a regular column (it should only appear once as the virtual field)
        // Count occurrences - should only appear once (as the virtual field)
        let gs_op_count = query.matches("_gs_op").count();
        assert_eq!(
            gs_op_count, 1,
            "_gs_op should only appear once as the virtual field"
        );
    }

    #[test]
    fn test_query_star_columns() {
        let mut builder =
            ClickHouseQueryBuilder::of("test_table".to_string(), vec!["*".to_string()], None, None);
        let query = builder.get_query();

        // CTE should use SELECT * FROM table
        assert!(query.contains("SELECT * FROM test_table"));
        // Final SELECT should include the * column (without backticks) in the column list
        // The query structure should be: SELECT *, _gs_op FROM t
        let final_select_start = query
            .find("SELECT *")
            .expect("Final SELECT should contain *");
        // Verify it's in the final SELECT, not the CTE
        assert!(query[final_select_start..].contains("FROM t"));
        // Verify * is not backticked in the final SELECT
        assert!(!query.contains("SELECT `*`"));
    }

    #[test]
    fn test_query_cte_structure() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "matic_raw_logs".to_string(),
            vec![
                "id".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
            ],
            Some("address = '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e'".to_string()),
            Some(pagination_config),
        );
        builder.start_at_page(vec![ScalarValue::Int64(Some(1))]);
        let query = builder.get_query();

        // Verify CTE structure
        assert!(query.starts_with("WITH t AS"));
        assert!(query.contains("SELECT * FROM matic_raw_logs"));
        assert!(query.contains("WHERE (address = '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e')"));
        assert!(query.contains("ORDER BY id"));
        assert!(query.contains("SELECT id,\nblock_number,\nblock_hash"));
        assert!(query.contains("FROM t"));
    }

    #[test]
    fn test_pagination_flow() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "transactions".to_string(),
            vec![
                "block_number".to_string(),
                "id".to_string(),
                "hash".to_string(),
            ],
            Some("status = 'confirmed'".to_string()),
            Some(pagination_config),
        );

        // First page: no keyset, should return all matching rows
        let first_page_query = builder.get_query().to_string();
        assert!(first_page_query.contains("SELECT * FROM transactions"));
        assert!(first_page_query.contains("WHERE (status = 'confirmed')"));
        assert!(first_page_query.contains("ORDER BY block_number,id"));
        assert!(!first_page_query.contains("block_number >"));
        assert!(!first_page_query.contains("block_number >="));

        // Simulate reading first page and getting last row: block_number=1000, id=50
        builder.update_keyset(vec![
            ScalarValue::Int64(Some(1000)),
            ScalarValue::Int64(Some(50)),
        ]);
        let second_page_query = builder.get_query().to_string();

        // Second page: should use > (not >=) for pagination boundary
        assert!(second_page_query.contains("WHERE (status = 'confirmed')"));
        assert!(second_page_query.contains("AND"));
        // Should have proper tuple comparison unwinding
        assert!(second_page_query.contains("block_number > 1000"));
        assert!(second_page_query.contains("block_number = 1000 AND id > 50"));
        assert!(second_page_query.contains("ORDER BY block_number,id"));

        // Simulate reading second page and getting last row: block_number=1000, id=150
        builder.update_keyset(vec![
            ScalarValue::Int64(Some(1000)),
            ScalarValue::Int64(Some(150)),
        ]);
        let third_page_query = builder.get_query().to_string();

        // Third page: should continue with > operator
        assert!(third_page_query.contains("block_number > 1000"));
        assert!(third_page_query.contains("block_number = 1000 AND id > 150"));

        // Verify all queries have proper CTE structure
        assert!(first_page_query.starts_with("WITH t AS"));
        assert!(second_page_query.starts_with("WITH t AS"));
        assert!(third_page_query.starts_with("WITH t AS"));
    }

    #[test]
    fn test_block_range_upper_bound_only() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["block_number".to_string(), "id".to_string()],
            None,
            Some(pagination_config),
        );
        builder.set_block_range_upper_bound(Some(ScalarValue::Int64(Some(1_000_000))));
        let query = builder.get_query();

        assert!(query.contains("block_number < 1000000"));
        assert!(query.contains("ORDER BY block_number,id"));
    }

    #[test]
    fn test_block_range_with_where_clause() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["block_number".to_string(), "data".to_string()],
            Some("address = '0x1234'".to_string()),
            Some(pagination_config),
        );
        builder.set_block_range_upper_bound(Some(ScalarValue::Int64(Some(2_000_000))));
        let query = builder.get_query();

        assert!(query.contains("WHERE (address = '0x1234')"));
        assert!(query.contains("AND (block_number < 2000000)"));
    }

    #[test]
    fn test_block_range_with_keyset() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["block_number".to_string(), "id".to_string()],
            None,
            Some(pagination_config),
        );
        builder.set_block_range_upper_bound(Some(ScalarValue::Int64(Some(1_000_000))));
        builder.update_keyset(vec![
            ScalarValue::Int64(Some(500_000)),
            ScalarValue::Int64(Some(42)),
        ]);
        let query = builder.get_query();

        // Both block range upper bound and keyset conditions should be present
        assert!(query.contains("block_number < 1000000"));
        assert!(query.contains("block_number > 500000"));
        assert!(query.contains("block_number = 500000 AND id > 42"));
    }

    #[test]
    fn test_block_range_with_where_and_keyset() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["block_number".to_string(), "id".to_string()],
            Some("address = '0xdead'".to_string()),
            Some(pagination_config),
        );
        builder.set_block_range_upper_bound(Some(ScalarValue::Int64(Some(3_000_000))));
        builder.start_at_page(vec![
            ScalarValue::Int64(Some(1_000_000)),
            ScalarValue::Int64(Some(0)),
        ]);
        let query = builder.get_query();

        // All three conditions: filter, block range, and keyset
        assert!(query.contains("WHERE (address = '0xdead')"));
        assert!(query.contains("block_number < 3000000"));
        assert!(query.contains("block_number >= 1000000"));
        assert!(query.contains("block_number = 1000000 AND id >= 0"));
    }

    #[test]
    fn test_clear_keyset() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["block_number".to_string()],
            None,
            Some(pagination_config),
        );

        builder.update_keyset(vec![ScalarValue::Int64(Some(500))]);
        let query_with_keyset = builder.get_query().to_string();
        assert!(query_with_keyset.contains("block_number > 500"));

        builder.clear_keyset();
        let query_without_keyset = builder.get_query().to_string();
        assert!(!query_without_keyset.contains("block_number >"));
        assert!(!query_without_keyset.contains("block_number >="));
    }

    #[test]
    fn test_block_range_pagination_flow() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "traces".to_string(),
            vec![
                "block_number".to_string(),
                "id".to_string(),
                "data".to_string(),
            ],
            Some("error = '0x01'".to_string()),
            Some(pagination_config),
        );

        // Range 1: [0, 1M) - first page, no keyset
        builder.set_block_range_upper_bound(Some(ScalarValue::Int64(Some(1_000_000))));
        let q1 = builder.get_query().to_string();
        assert!(q1.contains("block_number < 1000000"));
        assert!(!q1.contains("block_number >"));

        // Within range 1: keyset pagination after first page
        builder.update_keyset(vec![
            ScalarValue::Int64(Some(500_000)),
            ScalarValue::Int64(Some(99)),
        ]);
        let q2 = builder.get_query().to_string();
        assert!(q2.contains("block_number < 1000000"));
        assert!(q2.contains("block_number > 500000"));

        // Advance to range 2: [1M, 2M) - start_at new range, new upper bound
        builder.set_block_range_upper_bound(Some(ScalarValue::Int64(Some(2_000_000))));
        builder.start_at_page(vec![ScalarValue::Int64(Some(1_000_000))]);
        let q3 = builder.get_query().to_string();
        assert!(q3.contains("block_number < 2000000"));
        assert!(q3.contains("block_number >= 1000000"));
        assert!(!q3.contains("block_number > 500000"));

        // All queries maintain CTE structure
        assert!(q1.starts_with("WITH t AS"));
        assert!(q2.starts_with("WITH t AS"));
        assert!(q3.starts_with("WITH t AS"));
    }

    #[test]
    fn test_timeout_retry_resets_keyset() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "traces".to_string(),
            vec!["block_number".to_string(), "id".to_string()],
            None,
            Some(pagination_config),
        );

        // Scanning range [0, 1000) — keyset advanced to block_number=800
        builder.set_block_range_upper_bound(Some(ScalarValue::Int64(Some(1000))));
        builder.update_keyset(vec![
            ScalarValue::Int64(Some(800)),
            ScalarValue::Int64(Some(42)),
        ]);
        let q1 = builder.get_query().to_string();
        assert!(q1.contains("block_number < 1000"));
        assert!(q1.contains("block_number > 800"));

        // Timeout! Shrink range to [0, 500) and reset keyset to range_start=0.
        // Without the reset, the query would have block_number > 800 AND block_number < 500.
        builder.set_block_range_upper_bound(Some(ScalarValue::Int64(Some(500))));
        builder.start_at_page(vec![ScalarValue::Int64(Some(0))]);
        let q2 = builder.get_query().to_string();
        assert!(
            q2.contains("block_number < 500"),
            "upper bound should be 500, got: {}",
            q2
        );
        assert!(
            q2.contains("block_number >= 0"),
            "should restart from range_start, got: {}",
            q2
        );
        assert!(
            !q2.contains("block_number > 800"),
            "stale keyset must be cleared, got: {}",
            q2
        );
    }

    #[test]
    fn test_empty_keyset_does_not_produce_empty_condition() {
        // Regression test: when current_keyset is Some(vec![]), build_keyset_conditions
        // returns "" and the query must not emit AND () or WHERE ().
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "matic_raw_logs".to_string(),
            vec!["block_number".to_string(), "id".to_string()],
            Some("address IN ('0x1234')".to_string()),
            Some(pagination_config),
        );
        builder.set_block_range_upper_bound(Some(ScalarValue::Int64(Some(1_000_000))));
        // Simulate an empty keyset being set (e.g. checkpoint with no args)
        builder.update_keyset(vec![]);
        let query = builder.get_query();

        assert!(
            !query.contains("AND ()"),
            "query must not contain 'AND ()': {query}"
        );
        assert!(
            !query.contains("WHERE ()"),
            "query must not contain 'WHERE ()': {query}"
        );
        // The filter and block range upper bound should still be present
        assert!(query.contains("WHERE (address IN ('0x1234'))"));
        assert!(query.contains("AND (block_number < 1000000)"));
    }

    #[test]
    fn test_recovery_uses_enlarged_block_range() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "traces".to_string(),
            vec!["block_number".to_string(), "id".to_string()],
            None,
            Some(pagination_config),
        );

        // Simulate: block_range was halved to 500 after timeout, range [0, 500) exhausted.
        // Advancing to next range: recover block_range to 1000 first, THEN set upper bound.
        let mut block_range: i128 = 500;
        let default_block_range: i128 = 1000;
        let next_start: i128 = 500;

        // Recovery happens before setting upper bound
        if block_range < default_block_range {
            block_range = (block_range * 2).min(default_block_range);
        }
        assert_eq!(block_range, 1000);

        builder.set_block_range_upper_bound(Some(ScalarValue::Int64(Some(
            (next_start + block_range) as i64,
        ))));
        builder.start_at_page(vec![ScalarValue::Int64(Some(next_start as i64))]);
        let query = builder.get_query().to_string();

        // The range should be [500, 1500), not [500, 1000) which would skip [1000, 1500)
        assert!(
            query.contains("block_number < 1500"),
            "upper bound should use recovered block_range, got: {}",
            query
        );
        assert!(query.contains("block_number >= 500"));
    }
}
