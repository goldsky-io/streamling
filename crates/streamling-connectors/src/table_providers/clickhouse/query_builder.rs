use datafusion::common::ScalarValue;

use super::in_key::cursor_column;

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
    current_keyset: Option<Vec<ScalarValue>>, // `>=` lower bound on the sorting key
    sort_key_range_upper_bound: Option<ScalarValue>, // Upper bound (exclusive) on first sorting key for sort key range pagination
    /// In-key pagination within one first-key value; see `set_in_key_after`.
    in_key_after: Option<Vec<ScalarValue>>,
    /// Name of the ReplacingMergeTree `is_deleted` flag column, parsed from
    /// `engine_full`. When `Some`, `_gs_op` is derived from it (so a custom-named
    /// flag works); when `None` there is no engine-level deletion concept and
    /// every row is classified 'i'. See `gs_op_field`.
    is_deleted_column: Option<String>,
}

impl ClickHouseQueryBuilder {
    // Helper function to format ScalarValue for SQL with proper quoting
    fn format_scalar_for_sql(value: &ScalarValue) -> String {
        match value {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => format!(
                "'{}'",
                s.replace('\\', "\\\\")
                    .replace('\'', "\\'")
                    .replace('\0', "\\0")
            ),
            _ => value.to_string(), // For numbers, dates, etc., use default string representation
        }
    }

    /// `(k1, ..) > after` in `NULLS FIRST` order, unwound like
    /// `build_keyset_conditions`. NULL sorts before every value, so `k > NULL`
    /// is `k IS NOT NULL` and `k = NULL` is `k IS NULL`.
    fn in_key_seek(keys: &[String], after: &[ScalarValue]) -> String {
        let eq = |key: &String, value: &ScalarValue| match value.is_null() {
            true => format!("{} IS NULL", key),
            false => format!("{} = {}", key, Self::format_scalar_for_sql(value)),
        };
        let gt = |key: &String, value: &ScalarValue| match value.is_null() {
            true => format!("{} IS NOT NULL", key),
            false => format!("{} > {}", key, Self::format_scalar_for_sql(value)),
        };
        (0..keys.len().min(after.len()))
            .map(|i| {
                let mut parts: Vec<String> = (0..i).map(|j| eq(&keys[j], &after[j])).collect();
                parts.push(gt(&keys[i], &after[i]));
                format!("({})", parts.join(" AND "))
            })
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    /// Hidden CTE alias of sorting key `i` (0 = the first key) on an in-key page.
    fn in_key_alias(i: usize) -> String {
        format!("_gs_key_{}", i)
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
            sort_key_range_upper_bound: None,
            in_key_after: None,
            is_deleted_column: None,
        }
    }

    pub fn start_at_page(&mut self, args: Vec<ScalarValue>) -> &mut Self {
        // Store the keyset; it is applied as a `>=` lower bound on the sorting key.
        self.current_keyset = Some(args);
        self
    }

    pub fn set_sort_key_range_upper_bound(&mut self, value: Option<ScalarValue>) -> &mut Self {
        self.sort_key_range_upper_bound = value;
        self
    }

    /// Enter (`Some`) or leave (`None`) in-key pagination within the one
    /// first-key value the range bounds select. `Some(after)` orders the page by
    /// the full sorting key, keeps only rows whose remaining sort-key tuple is
    /// strictly greater than a non-empty `after`, and appends one cursor column
    /// per remaining key (`in_key::cursor_column`).
    ///
    /// Order and seek use the raw sorting keys, aliased inside the CTE where
    /// MATERIALIZED columns and key expressions resolve. The output names can
    /// be projected aliases (a hybrid source selects `CAST(k AS T) AS k`) whose
    /// order differs from the raw column the seek compares. The full key keeps
    /// ClickHouse reading in order; `NULLS FIRST` puts NULL keys before any
    /// cursor, so the seek cannot skip them.
    pub fn set_in_key_after(&mut self, after: Option<Vec<ScalarValue>>) -> &mut Self {
        self.in_key_after = after;
        self
    }

    /// Set the ReplacingMergeTree `is_deleted` flag column name (parsed from
    /// `engine_full`). Drives the virtual `_gs_op` field; see `gs_op_field`.
    pub fn set_is_deleted_column(&mut self, col: Option<String>) -> &mut Self {
        self.is_deleted_column = col;
        self
    }

    /// The virtual `_gs_op` column: 'i' for a live row, 'd' for a tombstone.
    /// For a ReplacingMergeTree with an `is_deleted` flag, derive it from that
    /// flag using the column name parsed from `engine_full` (not a hardcode, so a
    /// custom-named flag column works). With no `is_deleted` column there is no
    /// engine-level deletion concept, so every row is classified 'i'.
    fn gs_op_field(&self) -> String {
        match &self.is_deleted_column {
            Some(col) => format!("CASE WHEN {}=0 THEN 'i' ELSE 'd' END AS _gs_op", col),
            None => "'i' AS _gs_op".to_string(),
        }
    }

    // Rebuild the query with current pagination state
    fn rebuild_query(&mut self) {
        // Sorting keys aliased for an in-key page; none for a range page.
        let in_key_keys: &[String] = match (&self.pagination_config, &self.in_key_after) {
            (Some(pagination_config), Some(_)) => &pagination_config.sorting_keys,
            _ => &[],
        };
        let aliases: Vec<String> = (0..in_key_keys.len()).map(Self::in_key_alias).collect();

        let mut cte_select = "*".to_string();
        for (key, alias) in in_key_keys.iter().zip(&aliases) {
            cte_select = format!("{}, {} AS {}", cte_select, key, alias);
        }

        // The user filter goes first, in parentheses to keep its precedence.
        let mut predicates: Vec<String> = Vec::new();
        if let Some(ref where_clause) = self.where_clause {
            predicates.push(format!("({})", where_clause));
        }
        if let Some(pagination_config) = &self.pagination_config {
            if let (Some(upper_bound), Some(first_key)) = (
                &self.sort_key_range_upper_bound,
                pagination_config.sorting_keys.first(),
            ) {
                predicates.push(format!(
                    "({} < {})",
                    first_key,
                    Self::format_scalar_for_sql(upper_bound)
                ));
            }
            // The keyset is always a `>=` lower bound (set via start_at_page).
            if let Some(keyset) = &self.current_keyset {
                let conditions =
                    Self::build_keyset_conditions(&pagination_config.sorting_keys, keyset, ">=");
                if !conditions.is_empty() {
                    predicates.push(format!("({})", conditions));
                }
            }
            if let Some(after) = &self.in_key_after {
                let conditions = Self::in_key_seek(&pagination_config.sorting_keys[1..], after);
                if !conditions.is_empty() {
                    predicates.push(format!("({})", conditions));
                }
            }
        }

        let mut cte_query = format!("SELECT {} FROM {}", cte_select, self.table_name);
        if !predicates.is_empty() {
            cte_query = format!("{} WHERE {}", cte_query, predicates.join(" AND "));
        }

        // NB: range pages carry deliberately NO `ORDER BY`. Pagination is driven
        // by disjoint half-open `block_number` ranges (the WHERE bounds above), so
        // determinism comes from the predicate, not row order. An `ORDER BY` on
        // the sorting key would force read-in-order on the main table and make
        // ClickHouse skip a matching projection (read-in-order is not supported on
        // projections). Only an in-key page (one first-key value) is ordered, so
        // its LIMIT cuts at a sort-key tuple boundary.

        // Remove _gs_op if present since we create our own virtual column; a `*`
        // must not re-select the hidden aliases.
        let mut select_columns: Vec<String> = self
            .columns
            .iter()
            .filter(|col| *col != "_gs_op")
            .map(|col| match col.as_str() {
                "*" if !aliases.is_empty() => format!("* EXCEPT ({})", aliases.join(", ")),
                _ => col.clone(),
            })
            .collect();
        select_columns.push(self.gs_op_field());
        for (i, alias) in aliases.iter().skip(1).enumerate() {
            select_columns.push(format!("toString({}) AS {}", alias, cursor_column(i)));
        }

        let mut query = format!(
            "WITH t AS (\n  {}\n)\nSELECT {} FROM t",
            cte_query,
            select_columns.join(",\n")
        );
        if !aliases.is_empty() {
            let order: Vec<String> = aliases
                .iter()
                .map(|alias| format!("{} NULLS FIRST", alias))
                .collect();
            query = format!("{}\nORDER BY {}", query, order.join(", "));
        }

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

    // The user-supplied filter, used to build the matching count probe.
    pub fn where_clause(&self) -> Option<&str> {
        self.where_clause.as_deref()
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
        assert!(
            !query.contains("ORDER BY"),
            "CTE must not emit ORDER BY: {query}"
        );
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
        assert!(
            !query.contains("ORDER BY"),
            "CTE must not emit ORDER BY: {query}"
        );
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
        assert!(
            !query.contains("ORDER BY"),
            "CTE must not emit ORDER BY: {query}"
        );
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
        assert!(
            !query.contains("ORDER BY"),
            "CTE must not emit ORDER BY: {query}"
        );
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
    fn test_query_includes_gs_op_default_is_constant_insert() {
        // No is_deleted column: no engine-level deletion concept, so _gs_op is a
        // constant 'i' (always present so the output schema contract holds).
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string()],
            None,
            None,
        );
        let query = builder.get_query();
        assert!(query.contains("_gs_op"));
        assert!(query.contains("'i' AS _gs_op"));
    }

    #[test]
    fn test_query_gs_op_from_is_deleted_flag() {
        // A ReplacingMergeTree is_deleted flag (custom name) drives _gs_op, so a
        // non-standard flag column works instead of a hardcode.
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string()],
            None,
            None,
        );
        builder.set_is_deleted_column(Some("deleted_flag".to_string()));
        let query = builder.get_query();
        assert!(query.contains("CASE WHEN deleted_flag=0 THEN 'i' ELSE 'd' END AS _gs_op"));
    }

    #[test]
    fn test_query_filters_out_gs_op_from_columns() {
        let mut builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["id".to_string(), "_gs_op".to_string(), "name".to_string()],
            None,
            None,
        );
        builder.set_is_deleted_column(Some("is_deleted".to_string()));
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
        assert!(
            !query.contains("ORDER BY"),
            "CTE must not emit ORDER BY: {query}"
        );
        assert!(query.contains("SELECT id,\nblock_number,\nblock_hash"));
        assert!(query.contains("FROM t"));
    }

    #[test]
    fn test_sort_key_range_upper_bound_only() {
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
        builder.set_sort_key_range_upper_bound(Some(ScalarValue::Int64(Some(1_000_000))));
        let query = builder.get_query();

        assert!(query.contains("block_number < 1000000"));
        assert!(
            !query.contains("ORDER BY"),
            "CTE must not emit ORDER BY: {query}"
        );
    }

    #[test]
    fn test_sort_key_range_with_where_clause() {
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
        builder.set_sort_key_range_upper_bound(Some(ScalarValue::Int64(Some(2_000_000))));
        let query = builder.get_query();

        assert!(query.contains("WHERE (address = '0x1234')"));
        assert!(query.contains("AND (block_number < 2000000)"));
    }

    #[test]
    fn test_sort_key_range_with_where_and_keyset() {
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
        builder.set_sort_key_range_upper_bound(Some(ScalarValue::Int64(Some(3_000_000))));
        builder.start_at_page(vec![
            ScalarValue::Int64(Some(1_000_000)),
            ScalarValue::Int64(Some(0)),
        ]);
        let query = builder.get_query();

        // All three conditions: filter, sort key range, and keyset
        assert!(query.contains("WHERE (address = '0xdead')"));
        assert!(query.contains("block_number < 3000000"));
        assert!(query.contains("block_number >= 1000000"));
        assert!(query.contains("block_number = 1000000 AND id >= 0"));
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
        builder.set_sort_key_range_upper_bound(Some(ScalarValue::Int64(Some(1_000_000))));
        // Simulate an empty keyset being set (e.g. checkpoint with no args)
        builder.start_at_page(vec![]);
        let query = builder.get_query();

        assert!(
            !query.contains("AND ()"),
            "query must not contain 'AND ()': {query}"
        );
        assert!(
            !query.contains("WHERE ()"),
            "query must not contain 'WHERE ()': {query}"
        );
        // The filter and sort key range upper bound should still be present
        assert!(query.contains("WHERE (address IN ('0x1234'))"));
        assert!(query.contains("AND (block_number < 1000000)"));
    }

    #[test]
    fn test_in_key_page_orders_by_full_key_and_seeks_past_cursor() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec![
                "block_number".to_string(),
                "id".to_string(),
                "log_index".to_string(),
            ],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "t_src".to_string(),
            vec!["block_number".to_string(), "id".to_string()],
            None,
            Some(pagination_config),
        );
        builder.set_sort_key_range_upper_bound(Some(ScalarValue::Int64(Some(8))));
        builder.start_at_page(vec![ScalarValue::Int64(Some(7))]);
        builder.set_in_key_after(Some(vec![
            ScalarValue::Utf8(Some("a'b".to_string())),
            ScalarValue::Int64(Some(3)),
        ]));
        let query = builder.get_query().to_string();

        assert!(query.contains("block_number >= 7"), "{query}");
        assert!(query.contains("block_number < 8"), "{query}");
        assert!(
            query.contains(
                "SELECT *, block_number AS _gs_key_0, id AS _gs_key_1, log_index AS _gs_key_2 FROM t_src"
            ),
            "{query}"
        );
        assert!(
            query.contains("(id > 'a\\'b') OR (id = 'a\\'b' AND log_index > 3)"),
            "{query}"
        );
        assert!(
            query.contains(
                "toString(_gs_key_1) AS _gs_cursor_1,\ntoString(_gs_key_2) AS _gs_cursor_2 FROM t"
            ),
            "{query}"
        );
        assert!(
            query.ends_with(
                "FROM t\nORDER BY _gs_key_0 NULLS FIRST, _gs_key_1 NULLS FIRST, _gs_key_2 NULLS FIRST"
            ),
            "{query}"
        );

        // A NULL cursor value sorts first: seek past it with IS [NOT] NULL.
        builder.set_in_key_after(Some(vec![
            ScalarValue::Utf8(None),
            ScalarValue::Utf8(Some("3".to_string())),
        ]));
        let query = builder.get_query().to_string();
        assert!(
            query.contains("(id IS NOT NULL) OR (id IS NULL AND log_index > '3')"),
            "{query}"
        );

        // First in-key page: ordered, no tuple seek.
        builder.set_in_key_after(Some(vec![]));
        let query = builder.get_query().to_string();
        assert!(!query.contains("id >"), "{query}");
        assert!(query.contains("ORDER BY _gs_key_0 NULLS FIRST"), "{query}");

        // Range pages stay unordered and select no hidden keys.
        builder.set_in_key_after(None);
        let query = builder.get_query().to_string();
        assert!(!query.contains("ORDER BY"), "{query}");
        assert!(!query.contains("_gs_key"), "{query}");
    }

    #[test]
    fn test_in_key_page_star_does_not_reselect_hidden_keys() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "t_src".to_string(),
            vec!["*".to_string()],
            None,
            Some(pagination_config),
        );
        builder.set_in_key_after(Some(vec![]));
        let query = builder.get_query();
        assert!(
            query.contains("SELECT * EXCEPT (_gs_key_0, _gs_key_1),\n'i' AS _gs_op"),
            "{query}"
        );
    }

    #[test]
    fn test_string_literal_escapes_nul() {
        assert_eq!(
            ClickHouseQueryBuilder::format_scalar_for_sql(&ScalarValue::Utf8(Some(
                "a\0b'c\\".to_string()
            ))),
            "'a\\0b\\'c\\\\'"
        );
    }

    #[test]
    fn test_in_key_seek_after_keyset_without_upper_bound_has_one_where() {
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "t_src".to_string(),
            vec!["block_number".to_string(), "id".to_string()],
            None,
            Some(pagination_config),
        );
        builder.start_at_page(vec![ScalarValue::Int64(Some(7))]);
        builder.set_in_key_after(Some(vec![ScalarValue::Utf8(Some("a".to_string()))]));
        let query = builder.get_query();
        assert_eq!(query.matches("WHERE").count(), 1, "{query}");
    }

    #[test]
    fn test_in_key_page_orders_on_raw_keys_not_output_aliases() {
        // A hybrid source selects `CAST(`id` AS String) AS `id``. Ordering by the
        // output name sorts the cast value while the seek compares the raw column,
        // and a NULL key must sort before the cursor so the seek cannot skip it.
        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let mut builder = ClickHouseQueryBuilder::of(
            "t_src".to_string(),
            vec![
                "`block_number`".to_string(),
                "CAST(`id` AS String) AS `id`".to_string(),
            ],
            None,
            Some(pagination_config),
        );
        builder.set_sort_key_range_upper_bound(Some(ScalarValue::Int64(Some(8))));
        builder.start_at_page(vec![ScalarValue::Int64(Some(7))]);
        builder.set_in_key_after(Some(vec![]));
        let query = builder.get_query();
        assert!(
            query.ends_with("ORDER BY _gs_key_0 NULLS FIRST, _gs_key_1 NULLS FIRST"),
            "{query}"
        );
    }

    #[test]
    fn test_recovery_uses_enlarged_sort_key_range() {
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

        // Simulate: sort_key_range was halved to 500 after timeout, range [0, 500) exhausted.
        // Advancing to next range: recover sort_key_range to 1000 first, THEN set upper bound.
        let mut sort_key_range: i128 = 500;
        let default_sort_key_range: i128 = 1000;
        let next_start: i128 = 500;

        // Recovery happens before setting upper bound
        if sort_key_range < default_sort_key_range {
            sort_key_range = (sort_key_range * 2).min(default_sort_key_range);
        }
        assert_eq!(sort_key_range, 1000);

        builder.set_sort_key_range_upper_bound(Some(ScalarValue::Int64(Some(
            (next_start + sort_key_range) as i64,
        ))));
        builder.start_at_page(vec![ScalarValue::Int64(Some(next_start as i64))]);
        let query = builder.get_query().to_string();

        // The range should be [500, 1500), not [500, 1000) which would skip [1000, 1500)
        assert!(
            query.contains("block_number < 1500"),
            "upper bound should use recovered sort_key_range, got: {}",
            query
        );
        assert!(query.contains("block_number >= 500"));
    }
}
