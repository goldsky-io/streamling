use datafusion::common::ScalarValue;
use std::borrow::Cow;

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
            where_clause: where_clause.map(|w| rewrite_prefix_likes(&w).into_owned()),
            pagination_config: config,
            current_keyset: None,
            sort_key_range_upper_bound: None,
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
        // Build the CTE with SELECT * FROM table WHERE ... (no ORDER BY; see below)
        let mut cte_query = format!("SELECT * FROM {}", self.table_name);

        // Add original where clause if it exists
        // put in parentheses to ensure proper precedence
        if let Some(ref where_clause) = self.where_clause {
            cte_query = format!("{} WHERE ({})", cte_query, where_clause);
        }

        // Track whether we already have a WHERE clause
        let has_where = self.where_clause.is_some();
        let mut added_conditions = has_where;

        // Add sort key range upper bound on the first sorting key
        if let (Some(pagination_config), Some(upper_bound)) =
            (&self.pagination_config, &self.sort_key_range_upper_bound)
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
            // The keyset is always a `>=` lower bound (set via start_at_page).
            let operator = ">=";
            let conditions =
                Self::build_keyset_conditions(&pagination_config.sorting_keys, keyset, operator);
            if !conditions.is_empty() {
                let connector = if added_conditions { "AND" } else { "WHERE" };
                cte_query = format!("{} {} ({})", cte_query, connector, conditions);
            }
        }

        // NB: deliberately NO `ORDER BY` here. Pagination is driven by disjoint
        // half-open `block_number` ranges (the keyset/sort-key-range WHERE bounds
        // above), so determinism comes from the predicate, not row order. An
        // `ORDER BY` on the sorting key would force read-in-order on the main
        // table and make ClickHouse skip a matching projection (read-in-order is
        // not supported on projections), so it is intentionally omitted.

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
            self.gs_op_field()
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

    // The user-supplied filter, used to build the matching count probe.
    pub fn where_clause(&self) -> Option<&str> {
        self.where_clause.as_deref()
    }
}

/// Rewrite every `<column> LIKE '<prefix>%'` in a pushed-down filter to the
/// equivalent `(substring(<column>, 1, <prefix bytes>) = '<prefix>')`.
///
/// With a pure-prefix LIKE in the filter, ClickHouse stopped pruning by the
/// filter's own `block_number` bounds and archive reads timed out; the
/// `substring` form prunes.
///
/// Only a bare column operand and a literal with no wildcard but the trailing
/// `%` and no escapes is rewritten; anything else is left untouched.
fn rewrite_prefix_likes(filter: &str) -> Cow<'_, str> {
    let Some(tokens) = lex(filter) else {
        return Cow::Borrowed(filter);
    };
    let mut out = String::new();
    let mut copied_to = 0;
    for (k, tok) in tokens.iter().enumerate() {
        if !tok.is_word(filter, "LIKE") {
            continue;
        }
        let Some(prefix) = tokens.get(k + 1).and_then(|t| pure_prefix(filter, t)) else {
            continue;
        };
        if !tokens
            .get(k + 2)
            .is_none_or(|t| t.is_punct(b')') || t.is_punct(b',') || t.is_logical(filter))
        {
            continue;
        }
        let Some(first) = column_start(filter, &tokens, k) else {
            continue;
        };
        if first > 0 {
            let before = &tokens[first - 1];
            if !(before.is_punct(b'(')
                || before.is_punct(b',')
                || before.is_logical(filter)
                || before.is_word(filter, "NOT"))
            {
                continue;
            }
        }
        let start = tokens[first].start;
        out.push_str(&filter[copied_to..start]);
        out.push_str(&format!(
            "(substring({}, 1, {}) = '{}')",
            &filter[start..tokens[k - 1].end],
            prefix.len(),
            prefix
        ));
        copied_to = tokens[k + 1].end;
    }
    if copied_to == 0 {
        return Cow::Borrowed(filter);
    }
    out.push_str(&filter[copied_to..]);
    Cow::Owned(out)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TokenKind {
    Word,
    QuotedIdent,
    Str,
    Punct(u8),
}

#[derive(Debug)]
struct Token {
    kind: TokenKind,
    start: usize,
    end: usize,
}

impl Token {
    fn is_word(&self, src: &str, word: &str) -> bool {
        self.kind == TokenKind::Word && src[self.start..self.end].eq_ignore_ascii_case(word)
    }

    fn is_punct(&self, c: u8) -> bool {
        self.kind == TokenKind::Punct(c)
    }

    fn is_logical(&self, src: &str) -> bool {
        self.is_word(src, "AND") || self.is_word(src, "OR")
    }

    fn is_column_part(&self, src: &str) -> bool {
        match self.kind {
            TokenKind::QuotedIdent => true,
            TokenKind::Word => {
                !src.as_bytes()[self.start].is_ascii_digit()
                    && !self.is_logical(src)
                    && !self.is_word(src, "NOT")
            }
            _ => false,
        }
    }
}

/// Index of the first token of the `a.b.c` column right before `tokens[like]`.
fn column_start(src: &str, tokens: &[Token], like: usize) -> Option<usize> {
    let mut first = like.checked_sub(1)?;
    if !tokens[first].is_column_part(src) {
        return None;
    }
    while first >= 2 && tokens[first - 1].is_punct(b'.') && tokens[first - 2].is_column_part(src) {
        first -= 2;
    }
    Some(first)
}

/// The `<prefix>` of a `'<prefix>%'` literal, if that is all the literal holds.
fn pure_prefix<'a>(src: &'a str, tok: &Token) -> Option<&'a str> {
    if tok.kind != TokenKind::Str {
        return None;
    }
    let prefix = src[tok.start + 1..tok.end - 1].strip_suffix('%')?;
    (!prefix.is_empty() && !prefix.contains(['%', '_', '\\', '\''])).then_some(prefix)
}

/// ClickHouse-flavoured lexer: just enough to tell code from string literals,
/// quoted identifiers and comments. `None` on an unterminated quote or comment.
fn lex(src: &str) -> Option<Vec<Token>> {
    let b = src.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let start = i;
        let kind = match c {
            _ if c.is_ascii_whitespace() => {
                i += 1;
                continue;
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                i = b[i..]
                    .iter()
                    .position(|&x| x == b'\n')
                    .map_or(b.len(), |p| i + p);
                continue;
            }
            b'#' => {
                i = b[i..]
                    .iter()
                    .position(|&x| x == b'\n')
                    .map_or(b.len(), |p| i + p);
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2 + src[i + 2..].find("*/")? + 2;
                continue;
            }
            b'\'' | b'"' | b'`' => {
                i += 1;
                loop {
                    match *b.get(i)? {
                        b'\\' => i += 2,
                        q if q == c && b.get(i + 1) == Some(&c) => i += 2,
                        q if q == c => break,
                        _ => i += 1,
                    }
                }
                i += 1;
                if c == b'\'' {
                    TokenKind::Str
                } else {
                    TokenKind::QuotedIdent
                }
            }
            _ if c.is_ascii_alphanumeric() || c == b'_' || !c.is_ascii() => {
                while i < b.len()
                    && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || !b[i].is_ascii())
                {
                    i += 1;
                }
                TokenKind::Word
            }
            _ => {
                i += 1;
                TokenKind::Punct(c)
            }
        };
        tokens.push(Token {
            kind,
            start,
            end: i,
        });
    }
    Some(tokens)
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

    const T0: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    #[test]
    fn prefix_like_is_rewritten_to_substring_equality() {
        let filter = format!(
            "block_number >= 10156394 AND block_number <= 10176394 \
             AND address = '0x9216f67a276b4bf1d883c4ec24095c2bc53c2ef4' AND topics LIKE '{T0}%'"
        );
        assert_eq!(
            rewrite_prefix_likes(&filter),
            format!(
                "block_number >= 10156394 AND block_number <= 10176394 \
                 AND address = '0x9216f67a276b4bf1d883c4ec24095c2bc53c2ef4' \
                 AND (substring(topics, 1, 66) = '{T0}')"
            )
        );
    }

    #[test]
    fn prefix_like_rewrite_handles_operand_and_boundary_shapes() {
        for (filter, expected) in [
            (
                "(topics like '0xa%' OR topics LIKE '0xbc%')",
                "((substring(topics, 1, 3) = '0xa') OR (substring(topics, 1, 4) = '0xbc'))",
            ),
            (
                "NOT t.topics LIKE '0xa%'",
                "NOT (substring(t.topics, 1, 3) = '0xa')",
            ),
            (
                "`my col` LIKE 'ab%' AND x = 1",
                "(substring(`my col`, 1, 2) = 'ab') AND x = 1",
            ),
            // substring() counts bytes, so the length is the UTF-8 byte length.
            ("name LIKE 'hé%'", "(substring(name, 1, 3) = 'hé')"),
            (
                "if(a LIKE 'x%', 1, 0) = 1",
                "if((substring(a, 1, 1) = 'x'), 1, 0) = 1",
            ),
        ] {
            assert_eq!(rewrite_prefix_likes(filter), expected, "filter: {filter}");
        }
    }

    #[test]
    fn non_prefix_or_ambiguous_likes_are_left_untouched() {
        for filter in [
            // not a pure prefix
            format!("topics LIKE '{T0},%,%'"),
            "topics LIKE '0x_bc%'".to_string(),
            "topics LIKE '%'".to_string(),
            "topics LIKE '0xa\\%%'".to_string(),
            "topics LIKE '0xa'".to_string(),
            "topics LIKE '0xa%' ESCAPE '!'".to_string(),
            // other operators
            "topics NOT LIKE '0xa%'".to_string(),
            "topics ILIKE '0xa%'".to_string(),
            // operand or surroundings bind differently than a bare comparison
            "lower(topics) LIKE '0xa%'".to_string(),
            "'a' || topics LIKE '0xa%'".to_string(),
            "topics LIKE '0xa%' = 1".to_string(),
            // LIKE inside a literal or comment is not code
            "note = 'topics LIKE ''0xa%''' AND x = 1".to_string(),
            "x = 1 -- topics LIKE '0xa%'".to_string(),
            // unterminated literal
            "topics LIKE '0xa%".to_string(),
        ] {
            assert_eq!(rewrite_prefix_likes(&filter), filter.as_str());
        }
    }

    #[test]
    fn page_and_count_queries_use_the_rewritten_filter() {
        let mut builder = ClickHouseQueryBuilder::of(
            "eth_sepolia_raw_logs".to_string(),
            vec!["id".to_string()],
            Some(format!("topics LIKE '{T0}%'")),
            None,
        );
        let rewritten = format!("(substring(topics, 1, 66) = '{T0}')");
        assert_eq!(builder.where_clause(), Some(rewritten.as_str()));
        assert!(
            builder
                .get_query()
                .contains(&format!("WHERE ({rewritten})"))
        );
    }
}
