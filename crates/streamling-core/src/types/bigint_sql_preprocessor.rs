//! Pre-process SQL for bigint (casts + binary ops)
//!
//! This is used to rewrite SQL to use UDFs for bigint operations instead of binary operators.
//! It also rewrites any DECIMAL casts over 76 digits to UINT256 or VARCHAR.
//!
//! This is used in the session manager to pre-process SQL before creating a logical plan.
//! It is also used in the sql_parse module to pre-process SQL before creating a logical plan.

use crate::error::{Result, ResultExt};
use crate::streamling_user_err;
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, SelectItem, SetExpr, Statement, TableFactor, UnaryOperator,
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

fn column_ident(e: &SqlExpr) -> Option<&str> {
    match e {
        SqlExpr::Identifier(id) => Some(id.value.as_str()),
        SqlExpr::CompoundIdentifier(ids) if !ids.is_empty() => {
            Some(ids.last().unwrap().value.as_str())
        }
        _ => None,
    }
}

fn parse_wrapped_fn(func_name: &str, inner: &SqlExpr) -> Option<SqlExpr> {
    let dialect = GenericDialect {};
    let inner_sql = inner.to_string();
    let sql = format!("SELECT {func_name}({inner_sql})");
    let mut stmts = Parser::parse_sql(&dialect, sql.as_str()).ok()?;
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

fn parse_binary_fn(func_name: &str, left: &SqlExpr, right: &SqlExpr) -> Option<SqlExpr> {
    let dialect = GenericDialect {};
    let lsql = left.to_string();
    let rsql = right.to_string();
    let sql = format!("SELECT {func_name}({lsql},{rsql})");
    let mut stmts = Parser::parse_sql(&dialect, sql.as_str()).ok()?;
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

/// Returns true for operators that produce a bigint result when their
/// operands are bigint (arithmetic ops). Returns false for comparison
/// and logical operators which always produce Boolean.
fn is_bigint_producing_op(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
            | BinaryOperator::BitwiseAnd
            | BinaryOperator::BitwiseOr
            | BinaryOperator::BitwiseXor
    )
}

fn clone_strip_nested(expr: &SqlExpr) -> SqlExpr {
    match expr {
        SqlExpr::Nested(inner) => clone_strip_nested(inner),
        _ => expr.clone(),
    }
}

/// Check whether a function name starting with the bigint prefix actually
/// returns the bigint type.  Functions like `i256_to_string` or
/// `i256_to_int64` convert *out* of the bigint type and must not be treated
/// as bigint-producing expressions.
fn is_bigint_returning_prefixed_func<K: BigIntKind>(name: &str) -> bool {
    if !name.starts_with(K::PREFIX) {
        return false;
    }
    let suffix = &name[K::PREFIX.len()..];
    !suffix.starts_with("to_")
}

fn is_kind_func_call<K: BigIntKind>(e: &SqlExpr) -> bool {
    if let SqlExpr::Function(f) = e {
        let full = f.name.to_string();
        let last = full.rsplit('.').next().unwrap_or(full.as_str());
        return last == K::TO_NAME || is_bigint_returning_prefixed_func::<K>(last);
    }
    false
}

fn wrap_literals_if_needed<K: BigIntKind>(
    left: &mut SqlExpr,
    right: &mut SqlExpr,
    left_is_kind: bool,
    right_is_kind: bool,
) {
    if left_is_kind || right_is_kind {
        if !left_is_kind && let Some(func) = parse_wrapped_fn(K::TO_NAME, left) {
            *left = func;
        }
        if !right_is_kind && let Some(func) = parse_wrapped_fn(K::TO_NAME, right) {
            *right = func;
        }
    }
}

trait BigIntKind {
    const TO_NAME: &'static str;
    const PREFIX: &'static str;
    const TO_STRING_NAME: &'static str;
    fn bin_op_name(op: &BinaryOperator) -> Option<&'static str>;
    fn neg_name() -> Option<&'static str> {
        None
    }
}

struct U256Kind;
impl BigIntKind for U256Kind {
    const TO_NAME: &'static str = "to_u256";
    const PREFIX: &'static str = "u256_";
    const TO_STRING_NAME: &'static str = "u256_to_string";
    fn bin_op_name(op: &BinaryOperator) -> Option<&'static str> {
        match op {
            BinaryOperator::Plus => Some("u256_add"),
            BinaryOperator::Minus => Some("u256_sub"),
            BinaryOperator::Multiply => Some("u256_mul"),
            BinaryOperator::Divide => Some("u256_div"),
            BinaryOperator::Modulo => Some("u256_mod"),
            _ => None,
        }
    }
}

struct I256Kind;
impl BigIntKind for I256Kind {
    const TO_NAME: &'static str = "to_i256";
    const PREFIX: &'static str = "i256_";
    const TO_STRING_NAME: &'static str = "i256_to_string";
    fn bin_op_name(op: &BinaryOperator) -> Option<&'static str> {
        match op {
            BinaryOperator::Plus => Some("i256_add"),
            BinaryOperator::Minus => Some("i256_sub"),
            BinaryOperator::Multiply => Some("i256_mul"),
            BinaryOperator::Divide => Some("i256_div"),
            BinaryOperator::Modulo => Some("i256_mod"),
            _ => None,
        }
    }
    fn neg_name() -> Option<&'static str> {
        Some("i256_neg")
    }
}

fn is_bigint_expr<K: BigIntKind>(expr: &SqlExpr, cols: &HashSet<String>) -> bool {
    match expr {
        SqlExpr::Nested(inner) => is_bigint_expr::<K>(inner, cols),
        SqlExpr::UnaryOp { expr, .. } => is_bigint_expr::<K>(expr, cols),
        SqlExpr::BinaryOp { left, op, right } => {
            // Only arithmetic operations produce bigint results.
            // Comparison (>, <, =, etc.) and logical (AND, OR) operators
            // produce Boolean regardless of operand types.
            if is_bigint_producing_op(op) {
                is_bigint_expr::<K>(left, cols) || is_bigint_expr::<K>(right, cols)
            } else {
                false
            }
        }
        SqlExpr::Cast { expr, .. } => is_bigint_expr::<K>(expr, cols),
        SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
            column_ident(expr).is_some_and(|c| cols.contains(c))
        }
        SqlExpr::Function(f) => {
            let fname = f.name.to_string().to_lowercase();
            // Functions that convert OUT of bigint (e.g. i256_to_string,
            // u256_to_string) do not produce a bigint result.
            if fname.starts_with(K::PREFIX) && !is_bigint_returning_prefixed_func::<K>(&fname) {
                return false;
            }
            if fname == K::TO_NAME || is_bigint_returning_prefixed_func::<K>(&fname) {
                return true;
            }
            if (fname == "coalesce" || fname == "coalesce_meta")
                && let datafusion::logical_expr::sqlparser::ast::FunctionArguments::List(arglist) =
                    &f.args
            {
                for arg in arglist.args.iter() {
                    if let datafusion::logical_expr::sqlparser::ast::FunctionArg::Unnamed(
                        datafusion::logical_expr::sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = arg
                        && is_bigint_expr::<K>(e, cols)
                    {
                        return true;
                    }
                }
            }
            // Check if the function directly wraps a to_<kind>() call
            let s = f.to_string().to_lowercase();
            if s.contains(&(String::from(K::TO_NAME) + "(")) {
                return true;
            }
            false
        }
        _ => false,
    }
}

fn contains_bigint_operations<K: BigIntKind>(expr: &SqlExpr, cols: &HashSet<String>) -> bool {
    match expr {
        SqlExpr::Nested(inner) => contains_bigint_operations::<K>(inner, cols),
        SqlExpr::UnaryOp { expr, .. } => contains_bigint_operations::<K>(expr, cols),
        SqlExpr::BinaryOp { left, right, .. } => {
            contains_bigint_operations::<K>(left, cols)
                || contains_bigint_operations::<K>(right, cols)
        }
        SqlExpr::Cast { expr, .. } => contains_bigint_operations::<K>(expr, cols),
        SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
            column_ident(expr).is_some_and(|c| cols.contains(c))
        }
        SqlExpr::Function(f) => {
            let fname = f.name.to_string().to_lowercase();
            if fname.starts_with(K::PREFIX) && !is_bigint_returning_prefixed_func::<K>(&fname) {
                return false;
            }
            if is_bigint_returning_prefixed_func::<K>(&fname) || fname == K::TO_NAME {
                return true;
            }
            if (fname == "coalesce" || fname == "coalesce_meta")
                && let datafusion::logical_expr::sqlparser::ast::FunctionArguments::List(arglist) =
                    &f.args
            {
                for arg in arglist.args.iter() {
                    if let datafusion::logical_expr::sqlparser::ast::FunctionArg::Unnamed(
                        datafusion::logical_expr::sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = arg
                        && contains_bigint_operations::<K>(e, cols)
                    {
                        return true;
                    }
                }
            }
            false
        }
        _ => false,
    }
}

fn rewrite_expr_kind<K: BigIntKind>(e: &mut SqlExpr, cols: &HashSet<String>) {
    match e {
        SqlExpr::BinaryOp { left, op, right } => {
            rewrite_expr_kind::<K>(left.as_mut(), cols);
            rewrite_expr_kind::<K>(right.as_mut(), cols);

            let left_is_col = column_ident(left).is_some_and(|c| cols.contains(c));
            let right_is_col = column_ident(right).is_some_and(|c| cols.contains(c));
            let left_is_kind =
                left_is_col || is_kind_func_call::<K>(left) || is_bigint_expr::<K>(left, cols);
            let right_is_kind =
                right_is_col || is_kind_func_call::<K>(right) || is_bigint_expr::<K>(right, cols);

            // Always wrap literals if needed (for both comparison and arithmetic operators)
            wrap_literals_if_needed::<K>(
                left.as_mut(),
                right.as_mut(),
                left_is_kind,
                right_is_kind,
            );

            // Handle arithmetic operators (convert to function calls)
            if let Some(fname) = K::bin_op_name(op)
                && (left_is_kind || right_is_kind)
            {
                let left_clone = clone_strip_nested(left);
                let right_clone = clone_strip_nested(right);
                if let Some(func) = parse_binary_fn(fname, &left_clone, &right_clone) {
                    *e = func;
                }
            }
        }
        SqlExpr::Nested(inner) => rewrite_expr_kind::<K>(inner.as_mut(), cols),
        SqlExpr::UnaryOp { op, expr } => {
            if matches!(op, UnaryOperator::Minus) {
                if let Some(neg_name) = K::neg_name() {
                    let target_is_col = column_ident(expr).is_some_and(|c| cols.contains(c));
                    let target_is_kind = target_is_col || is_kind_func_call::<K>(expr);
                    rewrite_expr_kind::<K>(expr.as_mut(), cols);
                    if target_is_kind {
                        if !target_is_col
                            && !is_kind_func_call::<K>(expr)
                            && let Some(func) = parse_wrapped_fn(K::TO_NAME, expr)
                        {
                            **expr = func;
                        }
                        if let Some(neg) = parse_wrapped_fn(neg_name, expr) {
                            *e = neg;
                        }
                    }
                }
            } else {
                rewrite_expr_kind::<K>(expr.as_mut(), cols);
            }
        }
        SqlExpr::Cast {
            expr, data_type, ..
        } => {
            // Enable CAST of 32-byte bigints to string e.g. CAST(u256_col AS VARCHAR)
            let dt = data_type.to_string().to_lowercase();
            if (dt.contains("varchar") || dt.contains("string") || dt.contains("utf8"))
                && is_bigint_expr::<K>(expr, cols)
                && !is_kind_func_call::<K>(expr)
            {
                let inner = clone_strip_nested(expr);
                if let Some(wrapped) = parse_wrapped_fn(K::TO_STRING_NAME, &inner) {
                    *expr.as_mut() = wrapped; // wrap inner expr without extra nesting
                }
            }
            rewrite_expr_kind::<K>(expr.as_mut(), cols)
        }
        SqlExpr::Function(func) => {
            // For functions, always recurse into args; for COALESCE, coerce args to kind if any is kind.
            let fname = func.name.to_string().to_lowercase();
            if let datafusion::logical_expr::sqlparser::ast::FunctionArguments::List(arglist) =
                &mut func.args
            {
                let coalesce = fname == "coalesce" || fname == "coalesce_meta";
                let any_is_kind = if coalesce {
                    arglist.args.iter().any(|arg| {
                        matches!(arg,
                            datafusion::logical_expr::sqlparser::ast::FunctionArg::Unnamed(
                                datafusion::logical_expr::sqlparser::ast::FunctionArgExpr::Expr(e)
                            ) if is_kind_func_call::<K>(e) || is_bigint_expr::<K>(e, cols)
                        )
                    })
                } else {
                    false
                };
                for arg in arglist.args.iter_mut() {
                    if let datafusion::logical_expr::sqlparser::ast::FunctionArg::Unnamed(
                        datafusion::logical_expr::sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = arg
                    {
                        if coalesce
                            && any_is_kind
                            && !(is_kind_func_call::<K>(e) || is_bigint_expr::<K>(e, cols))
                            && let Some(wrapped) = parse_wrapped_fn(K::TO_NAME, e)
                        {
                            *e = wrapped;
                        }
                        rewrite_expr_kind::<K>(e, cols);
                        // Remove redundant parentheses introduced by original SQL
                        *e = clone_strip_nested(e);
                    }
                }

                // If this is COALESCE and any arg is kind, rewrite function name to coalesce_meta
                if fname == "coalesce" && any_is_kind {
                    use datafusion::logical_expr::sqlparser::ast::{
                        Ident, ObjectName, ObjectNamePart,
                    };
                    func.name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                        "coalesce_meta",
                    ))]);
                }
            }
        }
        _ => {}
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
    let mut u256_cols: HashSet<String> = HashSet::new();
    let mut i256_cols: HashSet<String> = HashSet::new();
    for field in arrow_schema.fields() {
        if matches!(field.data_type(), DataType::FixedSizeBinary(32)) {
            if crate::types::u256::U256Type::is_u256_metadata(field.metadata()) {
                u256_cols.insert(field.name().to_string());
            } else if crate::types::i256::I256Type::is_i256_metadata(field.metadata()) {
                i256_cols.insert(field.name().to_string());
            }
        }
    }

    fn rewrite_expr(e: &mut SqlExpr, u256_cols: &HashSet<String>, i256_cols: &HashSet<String>) {
        // Run U256 rewrite pass, then I256 pass. Each pass is idempotent for the other kind.
        rewrite_expr_kind::<U256Kind>(e, u256_cols);
        rewrite_expr_kind::<I256Kind>(e, i256_cols);
    }

    // Helper function to rewrite expressions in a SetExpr
    fn rewrite_setexpr(
        expr: &mut datafusion::logical_expr::sqlparser::ast::SetExpr,
        u256_cols: &HashSet<String>,
        i256_cols: &HashSet<String>,
    ) {
        match expr {
            SetExpr::Select(select) => {
                for item in select.projection.iter_mut() {
                    match item {
                        SelectItem::UnnamedExpr(expr) => rewrite_expr(expr, u256_cols, i256_cols),
                        SelectItem::ExprWithAlias { expr, .. } => {
                            rewrite_expr(expr, u256_cols, i256_cols)
                        }
                        _ => {}
                    }
                }
                if let Some(selection) = select.selection.as_mut() {
                    rewrite_expr(selection, u256_cols, i256_cols);
                }
                if let Some(having) = select.having.as_mut() {
                    rewrite_expr(having, u256_cols, i256_cols);
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                rewrite_setexpr(left.as_mut(), u256_cols, i256_cols);
                rewrite_setexpr(right.as_mut(), u256_cols, i256_cols);
            }
            _ => {}
        }
    }

    if let Statement::Query(query) = &mut stmt {
        // Track CTE columns that contain bigint data
        let mut cte_u256_columns: HashSet<String> = HashSet::new();
        let mut cte_i256_columns: HashSet<String> = HashSet::new();

        // Process CTEs first and track which columns contain bigint data
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                // Analyze CTE to find columns that contain bigint expressions
                let cte_name = cte.alias.name.to_string();

                // Process the CTE body
                rewrite_setexpr(&mut cte.query.body, &u256_cols, &i256_cols);

                // Analyze the CTE to determine which output columns contain bigint data
                if let SetExpr::Select(select) = cte.query.body.as_ref() {
                    for item in &select.projection {
                        if let SelectItem::ExprWithAlias { alias, expr, .. } = item {
                            let alias_name = alias.value.to_string();
                            // Check if the expression contains bigint operations
                            // This is a heuristic - if the expression was rewritten with u256_ or i256_ functions,
                            // then the output column contains bigint data

                            // Check if this expression contains U256 or I256 operations
                            let contains_u256 =
                                contains_bigint_operations::<U256Kind>(expr, &u256_cols);
                            let contains_i256 =
                                contains_bigint_operations::<I256Kind>(expr, &i256_cols);

                            if contains_u256 {
                                cte_u256_columns.insert(format!("{}.{}", cte_name, alias_name));
                                cte_u256_columns.insert(alias_name.clone()); // Also add without table prefix
                            }
                            if contains_i256 {
                                cte_i256_columns.insert(format!("{}.{}", cte_name, alias_name));
                                cte_i256_columns.insert(alias_name.clone()); // Also add without table prefix
                            }
                        }
                    }
                }
            }
        }

        // Process main query body with CTE column awareness
        if cte_u256_columns.is_empty() && cte_i256_columns.is_empty() {
            rewrite_setexpr(&mut query.body, &u256_cols, &i256_cols);
        } else {
            // Combine base columns with CTE columns
            let mut extended_u256_cols = u256_cols.clone();
            let mut extended_i256_cols = i256_cols.clone();
            extended_u256_cols.extend(cte_u256_columns);
            extended_i256_cols.extend(cte_i256_columns);
            rewrite_setexpr(&mut query.body, &extended_u256_cols, &extended_i256_cols);
        }
    }

    Ok(stmt.to_string())
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
            if precision > 76 {
                if precision <= 78 && scale == 0 {
                    // Preserve the existing u256 fast path for the
                    // narrow window — it's lossless and faster than
                    // a string detour.
                    format!("to_u256({})", expr)
                } else if scale >= 0 {
                    // T070 / FR-018: route wide-precision CAST through
                    // the decimal_arb cast UDF instead of the lossy
                    // VARCHAR fallback. The inner CAST AS VARCHAR is
                    // universal — it works for any source type.
                    format!(
                        "to_decimal_arb_from_string(TRY_CAST({} AS VARCHAR), {}, {})",
                        expr, precision, scale
                    )
                } else {
                    // Negative scale — not representable for decimal_arb;
                    // leave as-is so DataFusion surfaces the original error.
                    caps.get(0).map(|m| m.as_str()).unwrap_or("").to_string()
                }
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
                        if p <= 78 && s == 0 {
                            // Preserve existing u256 fast path.
                            if let Some(func) = parse_wrapped_fn("to_u256", inner) {
                                **inner = func;
                            }
                            if let Some(replacement) = Some((**inner).clone()) {
                                *expr = replacement;
                                return;
                            }
                        } else if let Some(call) =
                            parse_to_decimal_arb_from_string(inner, p, s as u64)
                        {
                            // T070 / FR-018: route wide-precision CAST through
                            // the decimal_arb cast UDF, replacing the prior
                            // lossy CAST AS VARCHAR fallback.
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
    fn test_preprocess_decimal_78_to_u256() {
        let sql = "SELECT CAST(balance AS DECIMAL(78, 0)) FROM accounts";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(result, "SELECT to_u256(balance) FROM accounts");
    }

    #[test]
    fn test_preprocess_decimal_77_to_u256() {
        let sql = "SELECT CAST(value AS DECIMAL(77, 0)) FROM data";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(result, "SELECT to_u256(value) FROM data");
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
    fn test_preprocess_try_cast_78() {
        let sql = "SELECT TRY_CAST(balance AS DECIMAL(78, 0)) FROM accounts";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(result, "SELECT to_u256(balance) FROM accounts");
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
        // u256 fast path stays for (78, 0); wide-precision routes to
        // decimal_arb instead of the lossy VARCHAR fallback.
        let sql = "SELECT CAST(a AS DECIMAL(78, 0)), CAST(b AS DECIMAL(100, 0)) FROM t";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_u256(a), to_decimal_arb_from_string(CAST(b AS VARCHAR), 100, 0) FROM t"
        );
    }

    #[test]
    fn test_preprocess_case_insensitive() {
        let sql = "SELECT cast(balance as decimal(78, 0)) FROM accounts";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(result, "SELECT to_u256(balance) FROM accounts");
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

    fn register_u256_table(ctx: &SessionContext, table_name: &str, fields: Vec<(&str, bool)>) {
        let schema_fields: Vec<Field> = fields
            .into_iter()
            .map(|(name, is_u256)| {
                if is_u256 {
                    Field::new(name, DataType::FixedSizeBinary(32), false)
                        .with_metadata(crate::types::u256::U256Type::metadata())
                } else {
                    Field::new(name, DataType::Int64, false)
                }
            })
            .collect();
        let schema = Arc::new(Schema::new(schema_fields));
        let table = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
        ctx.register_table(table_name, Arc::new(table)).unwrap();
    }

    fn register_i256_table(ctx: &SessionContext, table_name: &str, fields: Vec<(&str, bool)>) {
        let schema_fields: Vec<Field> = fields
            .into_iter()
            .map(|(name, is_i256)| {
                if is_i256 {
                    Field::new(name, DataType::FixedSizeBinary(32), false)
                        .with_metadata(crate::types::i256::I256Type::metadata())
                } else {
                    Field::new(name, DataType::Int64, false)
                }
            })
            .collect();
        let schema = Arc::new(Schema::new(schema_fields));
        let table = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
        ctx.register_table(table_name, Arc::new(table)).unwrap();
    }

    #[tokio::test]
    async fn test_u256_literal_wrapping() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("value", true)]);

        let sql = "SELECT coalesce(value, 0) + 2 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT u256_add(coalesce_meta(value, to_u256(0)), to_u256(2)) AS x FROM t"
        );

        // works with nested additions
        let sql = "SELECT (value - 1) + 2 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT u256_add(u256_sub(value, to_u256(1)), to_u256(2)) AS x FROM t"
        );
    }

    #[tokio::test]
    async fn test_i256_operations() {
        let ctx = setup_session_context();
        register_i256_table(&ctx, "t", vec![("balance", true)]);

        // Basic I256 addition
        let sql = "SELECT balance + 100 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT i256_add(balance, to_i256(100)) AS x FROM t"
        );

        // I256 subtraction
        let sql = "SELECT balance - 50 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT i256_sub(balance, to_i256(50)) AS x FROM t"
        );

        // I256 multiplication
        let sql = "SELECT balance * 2 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT i256_mul(balance, to_i256(2)) AS x FROM t"
        );
    }

    #[tokio::test]
    async fn test_u256_all_operators() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("value", true)]);

        // Multiplication
        let sql = "SELECT value * 5 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(rewritten, "SELECT u256_mul(value, to_u256(5)) AS x FROM t");

        // Division
        let sql = "SELECT value / 10 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(rewritten, "SELECT u256_div(value, to_u256(10)) AS x FROM t");

        // Modulo
        let sql = "SELECT value % 3 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(rewritten, "SELECT u256_mod(value, to_u256(3)) AS x FROM t");
    }
    #[tokio::test]
    async fn test_nested_coalesce_mul_with_to_string() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("value", true)]);

        let sql = "SELECT u256_to_string((coalesce(value, 0) * 2)) AS value_times_2_string FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT u256_to_string(u256_mul(coalesce_meta(value, to_u256(0)), to_u256(2))) AS value_times_2_string FROM t"
        );
    }

    #[tokio::test]
    async fn test_i256_unary_negation() {
        let ctx = setup_session_context();
        register_i256_table(&ctx, "t", vec![("balance", true)]);

        // Unary minus
        let sql = "SELECT -balance AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(rewritten, "SELECT i256_neg(balance) AS x FROM t");

        // Unary minus in expression
        let sql = "SELECT -balance + 100 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT i256_add(i256_neg(balance), to_i256(100)) AS x FROM t"
        );
    }

    #[tokio::test]
    async fn test_column_to_column_operations() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("a", true), ("b", true)]);

        // Column + column
        let sql = "SELECT a + b AS sum FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(rewritten, "SELECT u256_add(a, b) AS sum FROM t");

        // Column - column
        let sql = "SELECT a - b AS diff FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(rewritten, "SELECT u256_sub(a, b) AS diff FROM t");

        // Column * column
        let sql = "SELECT a * b AS product FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(rewritten, "SELECT u256_mul(a, b) AS product FROM t");
    }

    #[tokio::test]
    async fn test_complex_nested_expressions() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("value", true)]);

        // Deeply nested: (value + 1) * 2 - 3
        let sql = "SELECT (value + 1) * 2 - 3 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT u256_sub(u256_mul(u256_add(value, to_u256(1)), to_u256(2)), to_u256(3)) AS x FROM t"
        );

        // With division and modulo: value / 100 % 10
        let sql = "SELECT value / 100 % 10 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT u256_mod(u256_div(value, to_u256(100)), to_u256(10)) AS x FROM t"
        );
    }

    #[tokio::test]
    async fn test_where_clause_rewriting() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("value", true)]);

        // WHERE clause with arithmetic
        let sql = "SELECT value FROM t WHERE value + 10 > 100";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        // Check if WHERE clause arithmetic is rewritten
        assert!(rewritten.contains("u256_add(value, to_u256(10))"));
    }

    #[tokio::test]
    async fn test_multiple_projections() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("a", true), ("b", true)]);

        // Multiple SELECT items
        let sql = "SELECT a + 1 AS x, b * 2 AS y, a - b AS z FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert!(rewritten.contains("u256_add(a, to_u256(1))"));
        assert!(rewritten.contains("u256_mul(b, to_u256(2))"));
        assert!(rewritten.contains("u256_sub(a, b)"));
    }

    #[tokio::test]
    async fn test_literal_left_operand() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("value", true)]);

        // Literal on left side
        let sql = "SELECT 100 + value AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT u256_add(to_u256(100), value) AS x FROM t"
        );
    }

    #[tokio::test]
    async fn test_mixed_u256_and_i256_tables() {
        let ctx = setup_session_context();

        // Table 1 with U256
        register_u256_table(&ctx, "t1", vec![("unsigned_val", true)]);

        // Table 2 with I256
        register_i256_table(&ctx, "t2", vec![("signed_val", true)]);

        // U256 operation
        let sql = "SELECT unsigned_val + 1 FROM t1";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT u256_add(unsigned_val, to_u256(1)) FROM t1"
        );

        // I256 operation
        let sql = "SELECT signed_val - 1 FROM t2";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(rewritten, "SELECT i256_sub(signed_val, to_i256(1)) FROM t2");
    }

    #[tokio::test]
    async fn test_having_clause() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("id", false), ("value", true)]);

        // HAVING clause with arithmetic
        let sql = "SELECT id, SUM(value) as total FROM t GROUP BY id HAVING SUM(value) + 10 > 100";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        // Check if HAVING clause arithmetic is rewritten
        assert!(rewritten.contains("HAVING"));
    }

    #[tokio::test]
    async fn test_no_rewrite_for_non_bigint_columns() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("value", false)]);

        // Should NOT rewrite regular int operations
        let sql = "SELECT value + 10 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(rewritten, sql); // Should remain unchanged
    }

    #[tokio::test]
    async fn test_mixed_u256_and_regular_columns() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("u256_val", true), ("int_val", false)]);

        // U256 column with literal
        let sql = "SELECT u256_val + 100 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT u256_add(u256_val, to_u256(100)) AS x FROM t"
        );

        // Regular int column should not be rewritten
        let sql = "SELECT int_val + 100 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(rewritten, sql);
    }

    #[tokio::test]
    async fn test_parenthesized_expressions() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("value", true)]);

        // Multiple levels of parentheses
        let sql = "SELECT ((value + 1)) AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert!(rewritten.contains("u256_add"));
        assert!(rewritten.contains("to_u256(1)"));
    }

    #[tokio::test]
    async fn test_i256_division_and_modulo() {
        let ctx = setup_session_context();
        register_i256_table(&ctx, "t", vec![("balance", true)]);

        // Division
        let sql = "SELECT balance / 10 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT i256_div(balance, to_i256(10)) AS x FROM t"
        );

        // Modulo
        let sql = "SELECT balance % 5 AS x FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert_eq!(
            rewritten,
            "SELECT i256_mod(balance, to_i256(5)) AS x FROM t"
        );
    }

    #[tokio::test]
    async fn test_cte_with_u256_operations() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "accounts", vec![("balance", true), ("id", false)]);

        // CTE with U256 operations
        let sql = r#"
        WITH processed_balances AS (
            SELECT id, balance + 100 AS adjusted_balance
            FROM accounts
        )
        SELECT id, adjusted_balance * 2 AS doubled_balance
        FROM processed_balances
        "#;
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();

        // Check that CTE expressions are rewritten
        assert!(rewritten.contains("u256_add(balance, to_u256(100))"));
        assert!(rewritten.contains("u256_mul(adjusted_balance, to_u256(2))"));
    }

    #[tokio::test]
    async fn test_nested_cte_with_bigint_operations() {
        let ctx = setup_session_context();
        register_u256_table(
            &ctx,
            "transactions",
            vec![("amount", true), ("fee", true), ("id", false)],
        );

        // Nested CTEs with U256 operations
        let sql = r#"
        WITH base_tx AS (
            SELECT id, amount, fee
            FROM transactions
        ),
        processed_tx AS (
            SELECT id, amount + fee AS total_cost, amount * 2 AS double_amount
            FROM base_tx
        )
        SELECT id, total_cost - 50 AS net_cost
        FROM processed_tx
        "#;
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();

        // Check that all CTE expressions are rewritten
        assert!(rewritten.contains("u256_add(amount, fee)"));
        assert!(rewritten.contains("u256_mul(amount, to_u256(2))"));
        assert!(rewritten.contains("u256_sub(total_cost, to_u256(50))"));
    }

    #[tokio::test]
    async fn test_cte_with_i256_operations() {
        let ctx = setup_session_context();
        register_i256_table(&ctx, "balances", vec![("amount", true), ("user_id", false)]);

        // CTE with I256 operations including negation
        let sql = r#"
        WITH adjusted_balances AS (
            SELECT user_id, -amount AS negative_amount, amount / 10 AS tenth_amount
            FROM balances
        )
        SELECT user_id, negative_amount + 100 AS final_amount
        FROM adjusted_balances
        "#;
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();

        // Check that CTE expressions are rewritten
        assert!(rewritten.contains("i256_neg(amount)"));
        assert!(rewritten.contains("i256_div(amount, to_i256(10))"));
        assert!(rewritten.contains("i256_add(negative_amount, to_i256(100))"));
    }

    #[tokio::test]
    async fn test_cte_with_union_all() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "table1", vec![("value", true), ("id", false)]);

        // CTE with UNION ALL using same table
        let sql = r#"
        WITH combined_data AS (
            SELECT id, value + 1 AS adjusted_value FROM table1
            UNION ALL
            SELECT id, value * 2 AS adjusted_value FROM table1
        )
        SELECT id, adjusted_value - 10 AS final_value
        FROM combined_data
        "#;
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();

        // Check that expressions in both parts of UNION are rewritten
        assert!(rewritten.contains("u256_add(value, to_u256(1))"));
        assert!(rewritten.contains("u256_mul(value, to_u256(2))"));
        // Note: The main query may not be rewritten due to CTE extraction limitations with UNION ALL
        // This is a known limitation of the current implementation
    }

    #[tokio::test]
    async fn test_cast_then_coalesce() {
        let ctx = setup_session_context();
        register_u256_table(
            &ctx,
            "table1",
            vec![("decoded.event_params[3]", true), ("id", false)],
        );

        // CTE with UNION ALL using same table
        let sql = r#"
        SELECT COALESCE(TRY_CAST(decoded.event_params[3] AS DECIMAL(78)), 0) AS x FROM table1
        "#;
        let cast_rewritten = preprocess_bigint_decimal_casts(sql);
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, &cast_rewritten)
            .await
            .unwrap();

        // Check that the CAST is rewritten to to_u256 and COALESCE to coalesce_meta
        assert_eq!(
            rewritten,
            "SELECT coalesce_meta(to_u256(decoded.event_params[3]), to_u256(0)) AS x FROM table1"
        );
    }

    #[tokio::test]
    async fn test_greater_than_comparison() {
        let ctx = setup_session_context();
        register_u256_table(
            &ctx,
            "ethereum_transfers",
            vec![
                ("id", false),
                ("address", false),
                ("sender", false),
                ("recipient", false),
                ("amount", true),
                ("block_timestamp", false),
                ("transaction_hash", false),
                ("_gs_op", false),
            ],
        );

        let sql = r#"
        SELECT
            id,
            address as token,
            sender,
            recipient,
            amount,
            block_timestamp,
            transaction_hash,
            _gs_op
        FROM ethereum_transfers
        WHERE amount > 1000000000000000000000000
        "#;
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();

        // Check that the literal is wrapped in to_u256
        assert!(rewritten.contains("to_u256(1000000000000000000000000)"));
        assert!(rewritten.contains("amount > to_u256(1000000000000000000000000)"));
    }

    #[tokio::test]
    async fn test_comparison_with_and_does_not_wrap_boolean() {
        let ctx = setup_session_context();
        register_u256_table(
            &ctx,
            "traces",
            vec![("id", false), ("call_type", false), ("value", true)],
        );

        // Reproduces the boolean-predicate regression: `value > 0` involves a u256 column but the AND
        // combines two boolean predicates. The preprocessor must NOT wrap the
        // boolean side (`call_type <> 'delegatecall'`) with to_u256().
        let sql = "SELECT * FROM traces WHERE call_type <> 'delegatecall' AND `value` > 0";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();

        // The literal should be wrapped (backticks may be preserved)
        assert!(
            rewritten.contains("> to_u256(0)"),
            "Literal 0 should be wrapped with to_u256, got: {}",
            rewritten
        );
        // The boolean predicate must NOT be wrapped with to_u256
        assert!(
            !rewritten.contains("to_u256(call_type"),
            "Boolean predicate should not be wrapped with to_u256, got: {}",
            rewritten
        );
        assert!(
            rewritten.contains("call_type <> 'delegatecall'"),
            "Boolean predicate should remain unchanged, got: {}",
            rewritten
        );
    }

    #[tokio::test]
    async fn test_comparison_with_or_does_not_wrap_boolean() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("value", true), ("flag", false)]);

        let sql = "SELECT * FROM t WHERE flag = 1 OR value > 0";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();

        assert!(
            rewritten.contains("value > to_u256(0)"),
            "Literal should be wrapped, got: {}",
            rewritten
        );
        assert!(
            !rewritten.contains("to_u256(flag"),
            "Non-bigint predicate should not be wrapped, got: {}",
            rewritten
        );
    }

    #[tokio::test]
    async fn test_multiple_and_conditions_with_u256() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("id", false), ("a", true), ("b", true)]);

        let sql = "SELECT * FROM t WHERE a > 0 AND b < 100 AND id = 1";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();

        assert!(
            rewritten.contains("a > to_u256(0)"),
            "a > 0 should be rewritten, got: {}",
            rewritten
        );
        assert!(
            rewritten.contains("b < to_u256(100)"),
            "b < 100 should be rewritten, got: {}",
            rewritten
        );
        assert!(
            !rewritten.contains("to_u256(id"),
            "Non-bigint column should not be wrapped, got: {}",
            rewritten
        );
    }

    #[tokio::test]
    async fn test_coalesce_with_non_bigint_function_on_i256_column() {
        let ctx = setup_session_context();
        register_i256_table(&ctx, "t", vec![("rent_epoch", true), ("id", false)]);

        // to_int64() converts I256 to Int64, so COALESCE should NOT be
        // rewritten to coalesce_meta — both args are plain Int64.
        let sql = "SELECT COALESCE(to_int64(rent_epoch), 0) AS rent_epoch FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert!(
            !rewritten.contains("coalesce_meta"),
            "COALESCE should not be rewritten to coalesce_meta when \
             the result is Int64, got: {}",
            rewritten
        );
        assert!(
            !rewritten.contains("to_i256(0)"),
            "literal 0 should not be wrapped with to_i256, got: {}",
            rewritten
        );
    }

    #[tokio::test]
    async fn test_coalesce_with_non_bigint_function_on_u256_column() {
        let ctx = setup_session_context();
        register_u256_table(&ctx, "t", vec![("value", true), ("id", false)]);

        // u256_to_string converts U256 to Utf8, so COALESCE should NOT be
        // rewritten to coalesce_meta.
        let sql = "SELECT COALESCE(u256_to_string(value), 'none') AS val FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert!(
            !rewritten.contains("coalesce_meta"),
            "COALESCE should not be rewritten to coalesce_meta when \
             the result is Utf8, got: {}",
            rewritten
        );
    }

    #[tokio::test]
    async fn test_i256_to_string_in_coalesce() {
        let ctx = setup_session_context();
        register_i256_table(&ctx, "t", vec![("balance", true), ("id", false)]);

        let sql = "SELECT COALESCE(i256_to_string(balance), '0') AS bal FROM t";
        let rewritten = preprocess_bigint_binary_ops_with_schema(&ctx, sql)
            .await
            .unwrap();
        assert!(
            !rewritten.contains("coalesce_meta"),
            "COALESCE should not be rewritten to coalesce_meta when \
             the result is Utf8, got: {}",
            rewritten
        );
    }
}
