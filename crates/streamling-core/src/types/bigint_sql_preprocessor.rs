//! Pre-process SQL for bigint (casts + binary ops)
//!
//! This is used to rewrite SQL to use UDFs for bigint operations instead of binary operators.
//! It also rewrites any DECIMAL casts over 76 digits to UINT256 or VARCHAR.
//!
//! This is used in the session manager to pre-process SQL before creating a logical plan.
//! It is also used in the sql_parse module to pre-process SQL before creating a logical plan.

use crate::error::{Result, ResultExt};
use crate::streamling_user_err;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::sqlparser::ast::{
    DataType as SqlDataType, Expr as SqlExpr, SelectItem, SetExpr, Statement, TableFactor,
};
use datafusion::logical_expr::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use regex::Regex;
use std::collections::HashSet;

// ---------------- Shared helpers ----------------

fn parse_single_statement(sql: &str) -> Option<Statement> {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).ok()?;
    if stmts.len() == 1 {
        Some(stmts.remove(0))
    } else {
        None
    }
}

fn clone_strip_nested(expr: &SqlExpr) -> SqlExpr {
    match expr {
        SqlExpr::Nested(inner) => clone_strip_nested(inner),
        _ => expr.clone(),
    }
}

fn extract_from_setexpr(
    expr: &datafusion::logical_expr::sqlparser::ast::SetExpr,
    tables: &mut Vec<String>,
) -> std::result::Result<(), ParserError> {
    match expr {
        SetExpr::Select(select) if select.from.len() == 1 => {
            let table_with_joins = select
                .from
                .first()
                .expect("expected at least one FROM <table>");
            if !table_with_joins.joins.is_empty() {
                return Err(ParserError::ParserError(
                    "JOIN queries not supported".into(),
                ));
            }
            match &table_with_joins.relation {
                TableFactor::Table { name, .. } => {
                    let table_name = name.to_string();
                    tables.push(table_name);
                    Ok(())
                }
                TableFactor::Derived { subquery, .. } => {
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
    expr: &datafusion::logical_expr::sqlparser::ast::SetExpr,
    cte_base_table_by_name: &std::collections::HashMap<String, String>,
    tables: &mut Vec<String>,
) -> std::result::Result<(), ParserError> {
    match expr {
        SetExpr::Select(select) if select.from.len() == 1 => {
            let table_with_joins = select
                .from
                .first()
                .expect("expected at least one FROM <table>");
            if !table_with_joins.joins.is_empty() {
                return Err(ParserError::ParserError(
                    "JOIN queries not supported".into(),
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

fn extract_table_references_from_stmt(
    stmt: &Statement,
) -> std::result::Result<Vec<String>, ParserError> {
    let mut tables = Vec::new();
    match stmt {
        Statement::Query(query) => {
            // Resolve CTEs (non-recursive) to their underlying base tables
            let mut cte_base_table_by_name: std::collections::HashMap<String, String> =
                std::collections::HashMap::new();
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
                    let unique_cte_tables: std::collections::HashSet<String> =
                        cte_tables.into_iter().collect();
                    if unique_cte_tables.len() == 1 {
                        let base = unique_cte_tables.into_iter().next().unwrap();
                        let cte_name = cte.alias.name.to_string();
                        cte_base_table_by_name.insert(cte_name, base);
                    } else if unique_cte_tables.len() > 1 {
                        // For UNION ALL with multiple tables, use the first table as the base
                        // This is a limitation but allows the preprocessor to work
                        let base = unique_cte_tables.into_iter().next().unwrap();
                        let cte_name = cte.alias.name.to_string();
                        cte_base_table_by_name.insert(cte_name, base);
                    }
                    // If no tables found, skip this CTE
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

// ---------------- Public API ----------------

pub async fn preprocess_bigint_binary_ops_with_schema(
    ctx: &SessionContext,
    sql: &str,
) -> Result<String> {
    let mut stmt = parse_single_statement(sql)
        .ok_or_else(|| streamling_user_err!("failed to parse SQL statement: {}", sql))?;

    let tables = extract_table_references_from_stmt(&stmt).streamling_with_context(|| {
        format!("failed to extract table references from SQL: {}", sql)
    })?;

    if tables.is_empty() {
        return Err(streamling_user_err!(
            "no table references found in SQL statement: {}",
            sql
        ));
    }

    let (schema_name, table_name) =
        crate::session::SessionManager::extract_schema_and_table_names(&tables[0]);

    let catalog = ctx
        .catalog(crate::session::DEFAULT_CATALOG_NAME)
        .ok_or_else(|| {
            streamling_user_err!(
                "catalog '{}' not found for SQL: {}",
                crate::session::DEFAULT_CATALOG_NAME,
                sql
            )
        })?;

    let schema = catalog.schema(schema_name).ok_or_else(|| {
        streamling_user_err!("schema '{}' not found for SQL: {}", schema_name, sql)
    })?;

    let maybe_table = schema.table(table_name).await.streamling_with_context(|| {
        format!("failed to look up table '{}.{}'", schema_name, table_name)
    })?;

    let table_provider = maybe_table.ok_or_else(|| {
        streamling_user_err!(
            "table '{}.{}' not found for SQL: {}",
            schema_name,
            table_name,
            sql
        )
    })?;

    let arrow_schema = table_provider.schema();
    let mut decimal_arb_cols: HashSet<String> = HashSet::new();
    for field in arrow_schema.fields() {
        if crate::types::decimal_arb::DecimalArbType::is_decimal_arb_field(field) {
            decimal_arb_cols.insert(field.name().to_string());
        }
    }

    // Walk the SQL AST and apply the decimal_arb CAST-to-string rewrite.
    // DataFusion has no native cast from LargeBinary
    // (decimal_arb storage) to Utf8View, so this rewrite lowers
    // `CAST(decimal_arb_col AS TEXT|VARCHAR|STRING|UTF8|CHAR)` to
    // `decimal_arb_to_string(decimal_arb_col)` before the plan is built.
    //
    // After feature 002 (Retire U256/I256), binary-op rewriting for
    // wide integers happens at the LogicalPlan level via
    // `DecimalArbExprPlanner` — no SQL-string rewriting needed.

    fn rewrite_setexpr(
        expr: &mut datafusion::logical_expr::sqlparser::ast::SetExpr,
        decimal_arb_cols: &HashSet<String>,
    ) {
        match expr {
            SetExpr::Select(select) => {
                for item in select.projection.iter_mut() {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            rewrite_expr_for_decimal_arb_cast(expr, decimal_arb_cols)
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            rewrite_expr_for_decimal_arb_cast(expr, decimal_arb_cols)
                        }
                        _ => {}
                    }
                }
                if let Some(selection) = select.selection.as_mut() {
                    rewrite_expr_for_decimal_arb_cast(selection, decimal_arb_cols);
                }
                if let Some(having) = select.having.as_mut() {
                    rewrite_expr_for_decimal_arb_cast(having, decimal_arb_cols);
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                rewrite_setexpr(left.as_mut(), decimal_arb_cols);
                rewrite_setexpr(right.as_mut(), decimal_arb_cols);
            }
            _ => {}
        }
    }

    if let Statement::Query(query) = &mut stmt {
        // Process CTEs (their projections may yield decimal_arb columns
        // referenced by the main query, but the CAST-to-string rewrite
        // only requires the source-table column set — CTE column tracking
        // is no longer needed once BigIntKind binary-op rewriting is gone).
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                rewrite_setexpr(&mut cte.query.body, &decimal_arb_cols);
            }
        }
        rewrite_setexpr(&mut query.body, &decimal_arb_cols);
    }

    Ok(stmt.to_string())
}

/// Recursively walk a SQL expression tree and rewrite any
/// `CAST(decimal_arb_col AS TEXT/VARCHAR/STRING/CHAR)` (case-insensitive)
/// to `decimal_arb_to_string(decimal_arb_col)`. DataFusion has no native
/// cast from `LargeBinary` to `Utf8View`, so this rewrite is the only
/// safe lowering for the natural SQL form.
///
/// Only the immediate inner-expression case is handled (i.e. `CAST(col AS
/// TEXT)` where `col` is a decimal_arb column). More complex inner
/// expressions (e.g. `CAST(col_a + col_b AS TEXT)`) fall through; users
/// can wrap with `decimal_arb_to_string(...)` explicitly for those.
fn rewrite_expr_for_decimal_arb_cast(e: &mut SqlExpr, decimal_arb_cols: &HashSet<String>) {
    match e {
        SqlExpr::Cast {
            expr, data_type, ..
        } => {
            // Match against explicit sqlparser DataType variants rather than
            // substringing the stringified type — `contains("char")` would
            // false-positive on e.g. `Array(VARCHAR)` and rewrite a cast
            // whose target is a collection.
            let is_text_target = matches!(
                data_type,
                SqlDataType::Text
                    | SqlDataType::Varchar(_)
                    | SqlDataType::CharacterVarying(_)
                    | SqlDataType::CharVarying(_)
                    | SqlDataType::Char(_)
                    | SqlDataType::Character(_)
                    | SqlDataType::String(_)
            );
            if is_text_target {
                let stripped = clone_strip_nested(expr);
                if let SqlExpr::Identifier(ident) = &stripped
                    && decimal_arb_cols.contains(&ident.value)
                {
                    // Rewrite the whole Cast node to decimal_arb_to_string(col)
                    *e = build_decimal_arb_to_string_call(stripped);
                    return;
                }
                if let SqlExpr::CompoundIdentifier(idents) = &stripped
                    && let Some(last) = idents.last()
                    && decimal_arb_cols.contains(&last.value)
                {
                    *e = build_decimal_arb_to_string_call(stripped);
                    return;
                }
            }
            // Recurse into the inner expression even if we didn't rewrite.
            rewrite_expr_for_decimal_arb_cast(expr.as_mut(), decimal_arb_cols);
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            rewrite_expr_for_decimal_arb_cast(left.as_mut(), decimal_arb_cols);
            rewrite_expr_for_decimal_arb_cast(right.as_mut(), decimal_arb_cols);
        }
        SqlExpr::UnaryOp { expr, .. } => {
            rewrite_expr_for_decimal_arb_cast(expr.as_mut(), decimal_arb_cols);
        }
        SqlExpr::Nested(inner) => {
            rewrite_expr_for_decimal_arb_cast(inner.as_mut(), decimal_arb_cols);
        }
        SqlExpr::Function(func) => {
            if let datafusion::logical_expr::sqlparser::ast::FunctionArguments::List(arglist) =
                &mut func.args
            {
                for arg in arglist.args.iter_mut() {
                    if let datafusion::logical_expr::sqlparser::ast::FunctionArg::Unnamed(
                        datafusion::logical_expr::sqlparser::ast::FunctionArgExpr::Expr(e_inner),
                    ) = arg
                    {
                        rewrite_expr_for_decimal_arb_cast(e_inner, decimal_arb_cols);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Construct an AST node for the call `decimal_arb_to_string(inner)`.
fn build_decimal_arb_to_string_call(inner: SqlExpr) -> SqlExpr {
    use datafusion::logical_expr::sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments,
        ObjectName, ObjectNamePart,
    };
    // Build via sqlparser's own pretty-printed form to avoid hand-constructing
    // every span; fall back to a Function expression if parse fails.
    let call_sql = format!("SELECT decimal_arb_to_string({})", inner);
    if let Some(Statement::Query(q)) = parse_single_statement(&call_sql)
        && let SetExpr::Select(select) = q.body.as_ref()
        && let Some(SelectItem::UnnamedExpr(expr)) = select.projection.first()
    {
        return expr.clone();
    }
    // Fallback: build minimally. Should never fire — kept for safety.
    SqlExpr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(
            datafusion::logical_expr::sqlparser::ast::Ident::new("decimal_arb_to_string"),
        )]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

pub fn preprocess_bigint_decimal_casts(sql: &str) -> String {
    // First, normalize TRY_CAST DECIMAL via regex (AST may not have TryCast variant)
    lazy_static::lazy_static! {
        static ref DECIMAL_TRY_RE: Regex = Regex::new(
            r"(?i)TRY_CAST\s*\(\s*(.+?)\s+AS\s+DECIMAL\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)\s*\)"
        ).unwrap();
    }
    let sql = DECIMAL_TRY_RE
        .replace_all(sql, |caps: &regex::Captures| {
            let expr = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            // Parse precision as u32 — decimal_arb supports declared
            // precision well beyond u8::MAX. Scale parses as u32 too because
            // negative scale isn't representable for decimal_arb.
            let precision: u32 = caps
                .get(2)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0);
            let scale: i32 = caps
                .get(3)
                .and_then(|m| m.as_str().parse().ok())
                .unwrap_or(0);
            if precision > 76 && scale >= 0 {
                // Feature 002 (Retire U256/I256): all wide-precision CASTs
                // route through the decimal_arb cast UDF. The legacy
                // `to_u256` fast path for (p ≤ 78, 0) is retired alongside
                // the U256/I256 types — those values flow through
                // decimal_arb end-to-end now.
                format!(
                    "to_decimal_arb_from_string(TRY_CAST({} AS VARCHAR), {}, {})",
                    expr, precision, scale
                )
            } else {
                caps.get(0).map(|m| m.as_str()).unwrap_or("").to_string()
            }
        })
        .to_string();

    // Now parse AST and handle CAST(... AS DECIMAL(p,s))
    let Some(mut stmt) = parse_single_statement(&sql) else {
        return sql;
    };

    fn parse_cast_varchar(inner: &SqlExpr) -> Option<SqlExpr> {
        let dialect = GenericDialect {};
        let inner_sql = inner.to_string();
        let cast_sql = format!("SELECT CAST({} AS VARCHAR)", inner_sql);
        let mut stmts = Parser::parse_sql(&dialect, cast_sql.as_str()).ok()?;
        if stmts.len() != 1 {
            return None;
        }
        if let Statement::Query(query) = stmts.remove(0)
            && let SetExpr::Select(select) = query.body.as_ref()
            && let Some(item) = select.projection.first()
        {
            return match item {
                SelectItem::UnnamedExpr(e) => Some(e.clone()),
                SelectItem::ExprWithAlias { expr, .. } => Some(expr.clone()),
                _ => None,
            };
        }
        None
    }

    /// Build `to_decimal_arb_from_string(CAST({inner} AS VARCHAR), {precision}, {scale})`
    /// as an `SqlExpr`. Falls back to the inner cast-to-varchar (lossy) if
    /// the function-call shape can't be parsed for some reason.
    fn parse_to_decimal_arb_from_string(
        inner: &SqlExpr,
        precision: u64,
        scale: u64,
    ) -> Option<SqlExpr> {
        let dialect = GenericDialect {};
        let inner_sql = inner.to_string();
        let call_sql = format!(
            "SELECT to_decimal_arb_from_string(CAST({} AS VARCHAR), {}, {})",
            inner_sql, precision, scale
        );
        let mut stmts = Parser::parse_sql(&dialect, call_sql.as_str()).ok()?;
        if stmts.len() != 1 {
            return None;
        }
        if let Statement::Query(query) = stmts.remove(0)
            && let SetExpr::Select(select) = query.body.as_ref()
            && let Some(item) = select.projection.first()
        {
            return match item {
                SelectItem::UnnamedExpr(e) => Some(e.clone()),
                SelectItem::ExprWithAlias { expr, .. } => Some(expr.clone()),
                _ => None,
            };
        }
        None
    }

    fn rewrite_expr(expr: &mut SqlExpr) {
        match expr {
            SqlExpr::Cast {
                expr: inner,
                data_type,
                kind: _,
                format: _,
                array: _,
            } => {
                // Attempt to parse DECIMAL(p,s) from data_type.to_string()
                let dt = data_type.to_string();
                let dt_lower = dt.to_lowercase();
                // naive parse: decimal(p[, s])
                if let Some(start) = dt_lower.find("decimal(")
                    && dt_lower.ends_with(')')
                {
                    // extract inside parens
                    let inside = &dt_lower[start + "decimal(".len()..dt_lower.len() - 1];
                    let parts: Vec<&str> = inside.split(',').map(|s| s.trim()).collect();
                    let (p, s) = match parts.len() {
                        1 => (parts[0].parse::<u64>().unwrap_or(0), 0i64),
                        2 => (
                            parts[0].parse::<u64>().unwrap_or(0),
                            parts[1].parse::<i64>().unwrap_or(-1),
                        ),
                        _ => (0, -1),
                    };
                    if p > 76 && s >= 0 {
                        // Feature 002 (Retire U256/I256): all wide-precision
                        // CASTs route through the decimal_arb cast UDF. The
                        // legacy `to_u256` fast path for (p ≤ 78, 0) is
                        // retired alongside the U256/I256 types — those
                        // values now flow through decimal_arb end-to-end.
                        if let Some(call) = parse_to_decimal_arb_from_string(inner, p, s as u64) {
                            *expr = call;
                            return;
                        } else if let Some(cast_varchar) = parse_cast_varchar(inner) {
                            // Defensive fallback — should not fire in practice.
                            *expr = cast_varchar;
                            return;
                        }
                    }
                }
                // Recurse into inner if not rewritten
                rewrite_expr(inner);
            }
            SqlExpr::UnaryOp { expr, .. } => rewrite_expr(expr),
            SqlExpr::Nested(inner) => rewrite_expr(inner),
            SqlExpr::Function(func) => {
                // Recurse into function args
                if let datafusion::logical_expr::sqlparser::ast::FunctionArguments::List(arglist) =
                    &mut func.args
                {
                    for arg in arglist.args.iter_mut() {
                        if let datafusion::logical_expr::sqlparser::ast::FunctionArg::Unnamed(
                            datafusion::logical_expr::sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) = arg
                        {
                            rewrite_expr(e);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if let Statement::Query(query) = &mut stmt
        && let SetExpr::Select(select) = query.body.as_mut()
    {
        for item in select.projection.iter_mut() {
            match item {
                SelectItem::UnnamedExpr(e) => rewrite_expr(e),
                SelectItem::ExprWithAlias { expr, .. } => rewrite_expr(expr),
                _ => {}
            }
        }
        if let Some(selection) = select.selection.as_mut() {
            rewrite_expr(selection);
        }
        if let Some(having) = select.having.as_mut() {
            rewrite_expr(having);
        }
    }

    stmt.to_string()
}

/// Combined preprocessor: first applies DECIMAL cast rewrite, then bigint binary-op rewrite.
pub async fn preprocess_bigint_sql(ctx: &SessionContext, sql: &str) -> Result<String> {
    let cast_rewritten = preprocess_bigint_decimal_casts(sql);
    let rewritten = preprocess_bigint_binary_ops_with_schema(ctx, &cast_rewritten).await?;
    Ok(rewritten)
}

#[cfg(test)]
mod tests {
    use super::{preprocess_bigint_binary_ops_with_schema, preprocess_bigint_decimal_casts};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Arc;

    #[test]
    fn test_preprocess_decimal_78_routes_to_decimal_arb() {
        // Feature 002 (Retire U256/I256): CAST AS DECIMAL(78, 0) now routes
        // through the decimal_arb cast UDF. The legacy `to_u256` fast path
        // is retired alongside the U256/I256 extension types.
        let sql = "SELECT CAST(balance AS DECIMAL(78, 0)) FROM accounts";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_decimal_arb_from_string(CAST(balance AS VARCHAR), 78, 0) FROM accounts"
        );
    }

    #[test]
    fn test_preprocess_decimal_77_routes_to_decimal_arb() {
        // Feature 002 (Retire U256/I256): see test_preprocess_decimal_78.
        let sql = "SELECT CAST(value AS DECIMAL(77, 0)) FROM data";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_decimal_arb_from_string(CAST(value AS VARCHAR), 77, 0) FROM data"
        );
    }

    #[test]
    fn test_preprocess_decimal_100_to_decimal_arb() {
        // T070 / FR-018: previously fell back to lossy `CAST(... AS VARCHAR)`;
        // now routes to the lossless decimal_arb cast UDF.
        let sql = "SELECT CAST(large_num AS DECIMAL(100, 0)) FROM data";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_decimal_arb_from_string(CAST(large_num AS VARCHAR), 100, 0) FROM data"
        );
    }

    #[test]
    fn test_erc_20_transform_sql() {
        let sql = r#"WITH transfers AS (
            SELECT *,
                   _gs_log_decode('[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]',`topics`,`data`) AS decoded
            FROM matic_raw_logs__1_0_0__go6d6vq
            WHERE topics LIKE '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef%'
              AND SPLIT_INDEX(topics, ',', 3) IS NULL
        )
        SELECT id,
               block_number,
               block_timestamp,
               block_hash,
               transaction_hash,
               transaction_index,
               log_index,
               address,
               LOWER(decoded.event_params[1]) AS sender,
               LOWER(decoded.event_params[2]) AS recipient,
               COALESCE(TRY_CAST(decoded.event_params[3] AS DECIMAL(78)), 0) AS amount
        FROM transfers
        WHERE decoded IS NOT NULL"#;
        let result = preprocess_bigint_decimal_casts(sql);
        assert!(!result.contains("COALESCE(TRY_CAST(decoded.event_params[3] AS DECIMAL(78)), 0)"));
    }

    #[test]
    fn test_preprocess_try_cast_78_routes_to_decimal_arb() {
        // Feature 002: TRY_CAST AS DECIMAL(78, 0) routes through decimal_arb.
        let sql = "SELECT TRY_CAST(balance AS DECIMAL(78, 0)) FROM accounts";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_decimal_arb_from_string(TRY_CAST(balance AS VARCHAR), 78, 0) FROM accounts"
        );
    }

    #[test]
    fn test_preprocess_try_cast_100() {
        // T070 / FR-018: TRY_CAST routes through the same lossless path.
        let sql = "SELECT TRY_CAST(balance AS DECIMAL(100, 0)) FROM accounts";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_decimal_arb_from_string(TRY_CAST(balance AS VARCHAR), 100, 0) FROM accounts"
        );
    }

    #[test]
    fn test_preprocess_decimal_76_unchanged() {
        let sql = "SELECT CAST(value AS DECIMAL(76,0)) FROM data";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn test_preprocess_decimal_with_scale_routes_to_decimal_arb() {
        // T070 / FR-018: previously this case was left untouched (and would
        // fail at DataFusion's CAST resolution because Decimal128 caps at
        // 38). It now routes through the decimal_arb cast UDF.
        let sql = "SELECT CAST(price AS DECIMAL(78,2)) FROM products";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_decimal_arb_from_string(CAST(price AS VARCHAR), 78, 2) FROM products"
        );
    }

    #[test]
    fn test_preprocess_multiple_casts() {
        // Feature 002: both 78 and 100 route through decimal_arb (the u256
        // fast path is retired alongside the U256/I256 types).
        let sql = "SELECT CAST(a AS DECIMAL(78, 0)), CAST(b AS DECIMAL(100, 0)) FROM t";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_decimal_arb_from_string(CAST(a AS VARCHAR), 78, 0), \
             to_decimal_arb_from_string(CAST(b AS VARCHAR), 100, 0) FROM t"
        );
    }

    #[test]
    fn test_preprocess_case_insensitive() {
        let sql = "SELECT cast(balance as decimal(78, 0)) FROM accounts";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_decimal_arb_from_string(CAST(balance AS VARCHAR), 78, 0) FROM accounts"
        );
    }

    // Helper functions for test setup
    fn setup_session_context() -> SessionContext {
        let cfg = SessionConfig::new()
            .set_str(
                "datafusion.catalog.default_catalog",
                crate::session::DEFAULT_CATALOG_NAME,
            )
            .set_str(
                "datafusion.catalog.default_schema",
                crate::session::DEFAULT_SCHEMA_NAME,
            );
        SessionContext::new_with_config(cfg)
    }

    /// Register a MemTable whose named fields are decimal_arb(78, 0).
    /// `kind` controls the optional native_int_kind hint per field; pass
    /// `None` for plain decimal_arb.
    fn register_decimal_arb_table(
        ctx: &SessionContext,
        table_name: &str,
        fields: Vec<(&str, Option<crate::types::decimal_arb::NativeIntKind>)>,
    ) {
        let schema_fields: Vec<Field> = fields
            .into_iter()
            .map(|(name, kind_opt)| {
                let f =
                    crate::types::decimal_arb::DecimalArbType::field(name, 78, 0, false).unwrap();
                match kind_opt {
                    Some(k) => {
                        crate::types::decimal_arb::DecimalArbType::with_native_int_kind(f, k)
                            .unwrap()
                    }
                    None => f,
                }
            })
            .collect();
        let schema = Arc::new(Schema::new(schema_fields));
        let table = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
        ctx.register_table(table_name, Arc::new(table)).unwrap();
    }

    // ---------------- Feature 002 (Retire U256/I256) — decimal_arb CAST AS TEXT ----------------
    //
    // After feature 002, wide-integer columns (Avro decimal(p, 0) with
    // p > 76) arrive in streamling SQL as decimal_arb. DataFusion has no
    // native cast from `LargeBinary` (decimal_arb storage) to `Utf8View`,
    // so `CAST(decimal_arb_col AS TEXT)` would fail with "Unsupported
    // CAST from LargeBinary to Utf8View". The preprocessor lowers all four
    // text-cast keyword spellings to `decimal_arb_to_string(col)`.
    //
    // This closes the wide-int text-cast regression *via the decimal_arb path*. The legacy
    // u256/i256 path is retired as part of the same feature; once those
    // types are deleted in Phase 8 there is no remaining FSB(32)-based
    // wide-int route.

    #[tokio::test]
    async fn test_cast_decimal_arb_as_text() {
        let ctx = setup_session_context();
        register_decimal_arb_table(&ctx, "t", vec![("gas_used", None)]);
        let sql = "SELECT CAST(gas_used AS TEXT) AS gas_used FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert!(
            rewritten.contains("decimal_arb_to_string(gas_used)"),
            "rewrite must wrap inner expression in decimal_arb_to_string, got: {}",
            rewritten
        );
        assert!(
            !rewritten.to_lowercase().contains("cast(gas_used as text"),
            "rewrite must NOT leave a raw CAST AS TEXT in the output, got: {}",
            rewritten
        );
    }

    #[tokio::test]
    async fn test_cast_decimal_arb_as_varchar() {
        let ctx = setup_session_context();
        register_decimal_arb_table(&ctx, "t", vec![("amount", None)]);
        let sql = "SELECT CAST(amount AS VARCHAR) AS amount_text FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert!(
            rewritten.contains("decimal_arb_to_string(amount)"),
            "VARCHAR cast must lower to decimal_arb_to_string: {}",
            rewritten
        );
    }

    #[tokio::test]
    async fn test_cast_decimal_arb_as_string() {
        let ctx = setup_session_context();
        register_decimal_arb_table(&ctx, "t", vec![("balance", None)]);
        let sql = "SELECT CAST(balance AS STRING) FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert!(
            rewritten.contains("decimal_arb_to_string(balance)"),
            "STRING cast must lower to decimal_arb_to_string: {}",
            rewritten
        );
    }

    #[tokio::test]
    async fn test_cast_decimal_arb_case_insensitive() {
        let ctx = setup_session_context();
        register_decimal_arb_table(&ctx, "t", vec![("v", None)]);
        for variant in &[
            "SELECT CAST(v AS text) FROM t",
            "SELECT cast(v AS TEXT) FROM t",
            "SELECT CAST(v as Text) FROM t",
            "SELECT cast(v as varchar) FROM t",
        ] {
            let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, variant)
                .await
                .unwrap();
            assert!(
                rewritten.contains("decimal_arb_to_string(v)"),
                "case-insensitive variant {:?} must lower to decimal_arb_to_string: {}",
                variant,
                rewritten
            );
        }
    }

    /// The canonical CAST-AS-TEXT YAML reproduction, expressed as a SQL
    /// transform: `SELECT * EXCEPT col, CAST(col AS TEXT) AS col FROM t`
    /// where `col` is a decimal_arb column (post-feature-002 routing).
    #[tokio::test]
    async fn test_select_except_cast_as_text() {
        let ctx = setup_session_context();
        register_decimal_arb_table(
            &ctx,
            "traces",
            vec![(
                "gas_used",
                Some(crate::types::decimal_arb::NativeIntKind::U256),
            )],
        );
        let sql = "SELECT * EXCEPT (gas_used), CAST(gas_used AS TEXT) AS gas_used FROM traces";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert!(
            rewritten.contains("decimal_arb_to_string(gas_used)"),
            "wide-int text-cast YAML pattern must lower the cast: {}",
            rewritten
        );
        assert!(
            !rewritten.to_lowercase().contains("cast(gas_used as text"),
            "wide-int text-cast fix must NOT leave the raw cast: {}",
            rewritten
        );
    }

    /// Non-decimal_arb columns are not rewritten — verifies the
    /// preprocessor doesn't over-apply.
    #[tokio::test]
    async fn test_cast_int_as_text_is_left_alone() {
        let ctx = setup_session_context();
        // Register a plain-Int64 column to verify no rewrite fires.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
        ctx.register_table("u", Arc::new(table)).unwrap();
        let sql = "SELECT CAST(id AS TEXT) AS id_text FROM u";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert!(
            !rewritten.contains("decimal_arb_to_string"),
            "Int64 column CAST AS TEXT must not be rewritten: {}",
            rewritten
        );
    }
}
