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
    BinaryOperator, CastKind, DataType as SqlDataType, ExactNumberInfo, Expr as SqlExpr, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, Query, Select, SelectItem, SetExpr, Statement,
    TableAlias, TableFactor, UnaryOperator, Value, Visit, Visitor, visit_expressions_mut,
};
use datafusion::logical_expr::sqlparser::parser::ParserError;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use regex::Regex;
use std::collections::HashSet;
use std::ops::ControlFlow;

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
    // Joined tables contribute their decimal_arb columns too; one that
    // cannot be resolved here is left for DataFusion to report.
    for table in tables.iter().skip(1) {
        let (schema_name, table_name) =
            crate::session::SessionManager::extract_schema_and_table_names(table);
        let Some(schema) = catalog.schema(schema_name) else {
            continue;
        };
        let Ok(Some(provider)) = schema.table(table_name).await else {
            continue;
        };
        for field in provider.schema().fields() {
            if crate::types::decimal_arb::DecimalArbType::is_decimal_arb_field(field) {
                decimal_arb_cols.insert(field.name().to_string());
            }
        }
    }

    // Walk the SQL AST and apply the decimal_arb CAST-to-string rewrite.
    // DataFusion has no native cast from LargeBinary
    // (decimal_arb storage) to Utf8View, so this rewrite lowers
    // `CAST(decimal_arb_col AS TEXT|VARCHAR|STRING|UTF8|CHAR)` to
    // `decimal_arb_to_string(decimal_arb_col)` before the plan is built.
    //
    // Binary-op rewriting for wide integers happens at the
    // LogicalPlan level via
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

    let names = DecimalArbNames::collect(&stmt, decimal_arb_cols);
    quote_inexact_literals_near_decimal_arb(&mut stmt, &names);

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

/// The digits of a bare numeric literal — optionally signed, possibly
/// parenthesised — exactly as written.
fn number_literal_text(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Nested(inner) => number_literal_text(inner),
        SqlExpr::Value(v) => match &v.value {
            Value::Number(n, _) => Some(n.to_string()),
            _ => None,
        },
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => number_literal_text(expr).map(|t| format!("-{t}")),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => number_literal_text(expr),
        _ => None,
    }
}

// ---------------- Exact numeric literals next to decimal_arb ----------------
//
// DataFusion types a numeric literal that fits neither i64 nor u64, or that
// has a fractional part or an exponent, as Float64 — its digits are gone
// before any decimal_arb rule runs (`1000000000000000000000000` plans as
// 999999999999999983222784, and `amount > 1000000000000000000000000` then
// fails coercion outright). Where such a literal is an operand of a
// decimal_arb expression the preprocessor quotes it: the decimal_arb planner
// and rewrite parse string literals exactly. An operand that is not
// decimal_arb is never touched, so other columns keep DataFusion's typing.

/// Would DataFusion plan this numeric token as Float64?
fn is_inexact_number(text: &str) -> bool {
    if text.contains(['.', 'e', 'E']) {
        return true;
    }
    if text.starts_with('-') {
        text.parse::<i64>().is_err()
    } else {
        text.parse::<i64>().is_err() && text.parse::<u64>().is_err()
    }
}

/// Replace a bare numeric literal DataFusion would plan as Float64 with a
/// string literal carrying the same digits.
fn quote_inexact_literal(expr: &mut SqlExpr) {
    if let Some(text) = number_literal_text(expr)
        && is_inexact_number(&text)
    {
        *expr = SqlExpr::Value(Value::SingleQuotedString(text).into());
    }
}

fn is_arithmetic(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
    )
}

fn is_comparison(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Spaceship
    )
}

fn function_name(func: &Function) -> Option<String> {
    func.name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.to_ascii_lowercase())
}

fn function_args(func: &Function) -> Vec<&SqlExpr> {
    match &func.args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn function_args_mut(func: &mut Function) -> Vec<&mut SqlExpr> {
    match &mut func.args {
        FunctionArguments::List(list) => list
            .args
            .iter_mut()
            .filter_map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// UDFs that produce decimal_arb whatever their arguments.
const DECIMAL_ARB_CONSTRUCTORS: &[&str] = &[
    "to_decimal_arb_from_string",
    "try_to_decimal_arb_from_string",
    "to_decimal_arb_from_int",
    "to_decimal_arb_from_decimal128",
    "to_decimal_arb_from_decimal256",
    "legacy_wide_int_to_decimal_arb",
    "decimal_arb_add",
    "decimal_arb_sub",
    "decimal_arb_mul",
    "decimal_arb_div",
    "decimal_arb_mod",
    "decimal_arb_neg",
    "decimal_arb_abs",
    "decimal_arb_rescale",
    "decimal_arb_with_meta",
    "decimal_arb_restamp",
    "decimal_arb_greatest",
    "decimal_arb_least",
    "decimal_arb_array_min",
    "decimal_arb_array_max",
];

/// Functions whose result is decimal_arb when a decimal_arb argument goes in.
const DECIMAL_ARB_PRESERVING: &[&str] = &[
    "coalesce",
    "nvl",
    "nvl2",
    "ifnull",
    "nullif",
    "greatest",
    "least",
    "abs",
    "sum",
    "min",
    "max",
    "avg",
    "first_value",
    "last_value",
    "lag",
    "lead",
    "nth_value",
    "any_value",
    "array_element",
    "array_extract",
    "list_element",
    "list_extract",
    "array_min",
    "array_max",
];

/// Functions whose arguments are compared or combined with each other, so a
/// numeric literal among them meets the decimal_arb argument.
const DECIMAL_ARB_ARGUMENT_FUNCTIONS: &[&str] = &[
    "coalesce",
    "nvl",
    "nvl2",
    "ifnull",
    "nullif",
    "greatest",
    "least",
    "decimal_arb_greatest",
    "decimal_arb_least",
    "make_array",
    "make_list",
    "array_has",
    "array_has_any",
    "array_has_all",
    "array_position",
    "array_positions",
    "array_remove",
    "array_remove_n",
    "array_remove_all",
    "array_replace",
    "array_replace_n",
    "array_replace_all",
    "array_append",
    "array_prepend",
    "list_append",
    "list_prepend",
];

/// Column names known to hold decimal_arb: the referenced tables' columns
/// plus every projection alias (in CTEs and derived tables too) whose
/// expression is decimal_arb-valued.
struct DecimalArbNames(HashSet<String>);

impl DecimalArbNames {
    fn collect(stmt: &Statement, seed: HashSet<String>) -> Self {
        let mut names = Self(seed);
        // Aliases chain (`WITH a AS (SELECT v AS x …), b AS (SELECT x AS y
        // FROM a)`), so collect until nothing new appears.
        loop {
            let before = names.0.len();
            let mut collector = AliasCollector { names: &mut names };
            let _ = stmt.visit(&mut collector);
            if names.0.len() == before {
                break;
            }
        }
        names
    }

    /// Is `expr` decimal_arb-valued, as far as names and shapes can tell?
    fn is_decimal(&self, expr: &SqlExpr) -> bool {
        match expr {
            SqlExpr::Identifier(ident) => self.0.contains(&ident.value),
            SqlExpr::CompoundIdentifier(parts) => {
                parts.last().is_some_and(|p| self.0.contains(&p.value))
            }
            SqlExpr::Nested(inner) => self.is_decimal(inner),
            SqlExpr::UnaryOp {
                op: UnaryOperator::Minus | UnaryOperator::Plus,
                expr,
            } => self.is_decimal(expr),
            SqlExpr::BinaryOp { left, op, right } if is_arithmetic(op) => {
                self.is_decimal(left) || self.is_decimal(right)
            }
            SqlExpr::Cast { data_type, .. } => is_wide_decimal_type(data_type),
            SqlExpr::Case {
                conditions,
                else_result,
                ..
            } => {
                conditions.iter().any(|c| self.is_decimal(&c.result))
                    || else_result.as_deref().is_some_and(|e| self.is_decimal(e))
            }
            SqlExpr::Function(func) => match function_name(func) {
                Some(name) if DECIMAL_ARB_CONSTRUCTORS.contains(&name.as_str()) => true,
                Some(name) if DECIMAL_ARB_PRESERVING.contains(&name.as_str()) => {
                    function_args(func).into_iter().any(|a| self.is_decimal(a))
                }
                _ => false,
            },
            SqlExpr::Array(array) => array.elem.iter().any(|e| self.is_decimal(e)),
            SqlExpr::Subquery(query) => first_projection(query).is_some_and(|e| self.is_decimal(e)),
            _ => false,
        }
    }

    /// Record the output columns of `select` that are decimal_arb-valued:
    /// explicit aliases, and positional aliases from `alias(c1, c2, …)`.
    fn record_select(&mut self, select: &Select, positional: Option<&TableAlias>) {
        for (i, item) in select.projection.iter().enumerate() {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) => expr,
                SelectItem::ExprWithAlias { expr, alias } => {
                    if self.is_decimal(expr) {
                        self.0.insert(alias.value.clone());
                    }
                    expr
                }
                _ => continue,
            };
            if let Some(alias) = positional
                && let Some(column) = alias.columns.get(i)
                && self.is_decimal(expr)
            {
                self.0.insert(column.name.value.clone());
            }
        }
    }
}

/// `CAST(… AS DECIMAL(p[, s]))` beyond DataFusion's native precision routes
/// to decimal_arb.
fn is_wide_decimal_type(data_type: &SqlDataType) -> bool {
    let info = match data_type {
        SqlDataType::Decimal(info)
        | SqlDataType::Numeric(info)
        | SqlDataType::Dec(info)
        | SqlDataType::BigNumeric(info)
        | SqlDataType::BigDecimal(info) => info,
        _ => return false,
    };
    match info {
        ExactNumberInfo::Precision(p) | ExactNumberInfo::PrecisionAndScale(p, _) => *p > 76,
        ExactNumberInfo::None => false,
    }
}

/// The left-most `SELECT` of a query body (through set operations).
fn leftmost_select(body: &SetExpr) -> Option<&Select> {
    match body {
        SetExpr::Select(select) => Some(select),
        SetExpr::SetOperation { left, .. } => leftmost_select(left),
        SetExpr::Query(query) => leftmost_select(&query.body),
        _ => None,
    }
}

fn first_projection(query: &Query) -> Option<&SqlExpr> {
    match leftmost_select(&query.body)?.projection.first()? {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => Some(expr),
        _ => None,
    }
}

struct AliasCollector<'a> {
    names: &'a mut DecimalArbNames,
}

impl Visitor for AliasCollector<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                if let Some(select) = leftmost_select(&cte.query.body) {
                    self.names.record_select(select, Some(&cte.alias));
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } = table_factor
            && let Some(select) = leftmost_select(&subquery.body)
        {
            self.names.record_select(select, Some(alias));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
        self.names.record_select(select, None);
        ControlFlow::Continue(())
    }
}

/// Quote every Float64-typed numeric literal that is compared, combined or
/// listed together with a decimal_arb expression, anywhere in `stmt`.
fn quote_inexact_literals_near_decimal_arb(stmt: &mut Statement, names: &DecimalArbNames) {
    let quote_all = |exprs: Vec<&mut SqlExpr>| {
        for e in exprs {
            quote_inexact_literal(e);
        }
    };
    let _ = visit_expressions_mut(stmt, |expr: &mut SqlExpr| {
        match expr {
            SqlExpr::BinaryOp { left, op, right } if is_arithmetic(op) || is_comparison(op) => {
                if names.is_decimal(left) {
                    quote_inexact_literal(right);
                } else if names.is_decimal(right) {
                    quote_inexact_literal(left);
                }
            }
            SqlExpr::Between {
                expr, low, high, ..
            } => {
                if names.is_decimal(expr) || names.is_decimal(low) || names.is_decimal(high) {
                    quote_all(vec![expr, low, high]);
                }
            }
            SqlExpr::InList { expr, list, .. } => {
                if names.is_decimal(expr) || list.iter().any(|e| names.is_decimal(e)) {
                    quote_inexact_literal(expr);
                    quote_all(list.iter_mut().collect());
                }
            }
            SqlExpr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(operand) = operand
                    && (names.is_decimal(operand)
                        || conditions.iter().any(|c| names.is_decimal(&c.condition)))
                {
                    quote_inexact_literal(operand);
                    quote_all(conditions.iter_mut().map(|c| &mut c.condition).collect());
                }
                if conditions.iter().any(|c| names.is_decimal(&c.result))
                    || else_result.as_deref().is_some_and(|e| names.is_decimal(e))
                {
                    quote_all(conditions.iter_mut().map(|c| &mut c.result).collect());
                    if let Some(else_result) = else_result {
                        quote_inexact_literal(else_result);
                    }
                }
            }
            SqlExpr::Array(array) => {
                if array.elem.iter().any(|e| names.is_decimal(e)) {
                    quote_all(array.elem.iter_mut().collect());
                }
            }
            SqlExpr::Function(func) => {
                if function_name(func)
                    .is_some_and(|name| DECIMAL_ARB_ARGUMENT_FUNCTIONS.contains(&name.as_str()))
                    && function_args(func).into_iter().any(|a| names.is_decimal(a))
                {
                    quote_all(function_args_mut(func));
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
}

pub fn preprocess_bigint_decimal_casts(sql: &str) -> String {
    // First, normalize TRY_CAST DECIMAL via regex (AST may not have TryCast variant)
    lazy_static::lazy_static! {
        static ref DECIMAL_TRY_RE: Regex = Regex::new(
            r"(?i)TRY_CAST\s*\(\s*(.+?)\s+AS\s+DECIMAL\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)\s*\)"
        ).unwrap();
        /// An unquoted SQL numeric literal (optionally signed, fractional,
        /// exponent), as it appears inside `TRY_CAST(<literal> AS …)`.
        static ref NUMERIC_LITERAL_RE: Regex =
            Regex::new(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$").unwrap();
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
                // All wide-precision CASTs route through the decimal_arb
                // cast UDFs. TRY_CAST is
                // contractually non-throwing, so it takes the `try_` variant,
                // which yields NULL for a value that does not parse or does
                // not fit the declared type instead of failing the query.
                // A bare numeric literal is quoted rather than cast to
                // VARCHAR: planned as Float64 it would lose digits first.
                let text = if NUMERIC_LITERAL_RE.is_match(expr.trim()) {
                    format!("'{}'", expr.trim())
                } else {
                    format!("TRY_CAST({expr} AS VARCHAR)")
                };
                format!(
                    "try_to_decimal_arb_from_string({}, {}, {})",
                    text, precision, scale
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

    /// The source text of a numeric literal, looking through parentheses and
    /// a leading sign: `18446744073709551617`, `(1e30)`, `-5`.
    /// Build `to_decimal_arb_from_string(<text>, {precision}, {scale})` as an
    /// `SqlExpr`, where `<text>` is `CAST({inner} AS VARCHAR)` — or, for a bare
    /// numeric literal, the literal's own digits as a string. Falls back to the
    /// inner cast-to-varchar (lossy) if the function-call shape can't be parsed
    /// for some reason.
    fn parse_to_decimal_arb_from_string(
        inner: &SqlExpr,
        precision: u64,
        scale: u64,
        non_throwing: bool,
    ) -> Option<SqlExpr> {
        let dialect = GenericDialect {};
        // TRY_CAST must yield NULL for a value that does not convert.
        let function = if non_throwing {
            "try_to_decimal_arb_from_string"
        } else {
            "to_decimal_arb_from_string"
        };
        // An unquoted wide literal (`CAST(18446744073709551617 AS DECIMAL(77,0))`)
        // has no SQL type of its own: DataFusion plans it as Float64 before the
        // cast ever runs, so `CAST(... AS VARCHAR)` saw an approximation and the
        // exact digits were gone. Quoting the token keeps them.
        let inner_sql = match number_literal_text(inner) {
            Some(text) => format!("'{text}'"),
            None => format!("CAST({inner} AS VARCHAR)"),
        };
        let call_sql = format!(
            "SELECT {}({}, {}, {})",
            function, inner_sql, precision, scale
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
                kind,
                format: _,
                array: _,
            } => {
                let non_throwing = matches!(kind, CastKind::TryCast | CastKind::SafeCast);
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
                        // All wide-precision CASTs route through the
                        // decimal_arb cast UDF. The
                        // legacy `to_u256` fast path for (p ≤ 78, 0) is
                        // retired alongside the U256/I256 types — those
                        // values now flow through decimal_arb end-to-end.
                        if let Some(call) =
                            parse_to_decimal_arb_from_string(inner, p, s as u64, non_throwing)
                        {
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
        // CAST AS DECIMAL(78, 0) routes through the decimal_arb cast
        // UDF. The legacy `to_u256` fast path
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
        // See test_preprocess_decimal_78.
        let sql = "SELECT CAST(value AS DECIMAL(77, 0)) FROM data";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_decimal_arb_from_string(CAST(value AS VARCHAR), 77, 0) FROM data"
        );
    }

    #[test]
    fn test_preprocess_decimal_100_to_decimal_arb() {
        // Previously fell back to lossy `CAST(... AS VARCHAR)`; now
        // routes to the lossless decimal_arb cast UDF.
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
        // TRY_CAST AS DECIMAL(78, 0) routes through decimal_arb.
        let sql = "SELECT TRY_CAST(balance AS DECIMAL(78, 0)) FROM accounts";
        let result = preprocess_bigint_decimal_casts(sql);
        // TRY_CAST is non-throwing, so it takes the `try_` constructor.
        assert_eq!(
            result,
            "SELECT try_to_decimal_arb_from_string(TRY_CAST(balance AS VARCHAR), 78, 0) FROM accounts"
        );
    }

    #[test]
    fn test_preprocess_try_cast_quotes_numeric_literal() {
        // An unquoted wide literal must not pass through Float64 on its way
        // to the constructor.
        let sql = "SELECT TRY_CAST(18446744073709551617 AS DECIMAL(77, 0)) FROM t";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT try_to_decimal_arb_from_string('18446744073709551617', 77, 0) FROM t"
        );
        let sql = "SELECT CAST(18446744073709551617 AS DECIMAL(77, 0)) FROM t";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT to_decimal_arb_from_string('18446744073709551617', 77, 0) FROM t"
        );
    }

    #[test]
    fn test_preprocess_try_cast_100() {
        // TRY_CAST routes through the same lossless path.
        let sql = "SELECT TRY_CAST(balance AS DECIMAL(100, 0)) FROM accounts";
        let result = preprocess_bigint_decimal_casts(sql);
        assert_eq!(
            result,
            "SELECT try_to_decimal_arb_from_string(TRY_CAST(balance AS VARCHAR), 100, 0) FROM accounts"
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
        // Previously this case was left untouched (and would
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
        // Both 78 and 100 route through decimal_arb (the u256
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

    // ---------------- Exact numeric literals next to decimal_arb ----------------

    #[tokio::test]
    async fn test_wide_and_fractional_literals_next_to_decimal_arb_are_quoted() {
        let ctx = setup_session_context();
        register_decimal_arb_table(&ctx, "transfers", vec![("amount", None), ("fee", None)]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("price", DataType::Float64, true),
        ]));
        ctx.register_table(
            "meta",
            Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap()),
        )
        .unwrap();

        let cases = [
            (
                "SELECT * FROM transfers WHERE amount > 1000000000000000000000000",
                "SELECT * FROM transfers WHERE amount > '1000000000000000000000000'",
            ),
            (
                "SELECT * FROM transfers WHERE 1000000000000000000000000 < transfers.amount",
                "SELECT * FROM transfers WHERE '1000000000000000000000000' < transfers.amount",
            ),
            (
                "SELECT amount * 1.5, amount + (-0.25) FROM transfers",
                "SELECT amount * '1.5', amount + '-0.25' FROM transfers",
            ),
            (
                "SELECT * FROM transfers WHERE amount BETWEEN 0.5 AND 1e24 OR amount IN (1, 2.5)",
                "SELECT * FROM transfers WHERE amount BETWEEN '0.5' AND '1e24' OR amount IN (1, '2.5')",
            ),
            (
                "SELECT coalesce(amount, 0.5), greatest(fee, 1.5, 2), CASE amount WHEN 1.5 THEN 1 ELSE 0.5 END FROM transfers",
                "SELECT coalesce(amount, '0.5'), greatest(fee, '1.5', 2), CASE amount WHEN '1.5' THEN 1 ELSE 0.5 END FROM transfers",
            ),
            (
                "SELECT CASE WHEN id = 1 THEN amount ELSE 0.5 END FROM transfers",
                "SELECT CASE WHEN id = 1 THEN amount ELSE '0.5' END FROM transfers",
            ),
            // Aliases in CTEs and derived tables are tracked, positionally too.
            (
                "WITH c AS (SELECT amount AS a FROM transfers), d(b) AS (SELECT a FROM c) SELECT * FROM d WHERE b > 18446744073709551616",
                "WITH c AS (SELECT amount AS a FROM transfers), d (b) AS (SELECT a FROM c) SELECT * FROM d WHERE b > '18446744073709551616'",
            ),
            (
                "SELECT * FROM (SELECT amount + fee AS total FROM transfers) s WHERE s.total >= 0.001",
                "SELECT * FROM (SELECT amount + fee AS total FROM transfers) s WHERE s.total >= '0.001'",
            ),
            (
                "SELECT * FROM transfers WHERE sum(amount) > 1.5 AND (SELECT max(amount) FROM transfers) > 1e30",
                "SELECT * FROM transfers WHERE sum(amount) > '1.5' AND (SELECT max(amount) FROM transfers) > '1e30'",
            ),
            // Integer literals keep DataFusion's exact integer typing.
            (
                "SELECT * FROM transfers WHERE amount > 9223372036854775808 AND amount < -5",
                "SELECT * FROM transfers WHERE amount > 9223372036854775808 AND amount < -5",
            ),
            // Nothing changes for other columns.
            (
                "SELECT * FROM meta WHERE price > 1.5 AND id * 0.5 > 1e24 AND coalesce(price, 0.5) IN (1.5, 2)",
                "SELECT * FROM meta WHERE price > 1.5 AND id * 0.5 > 1e24 AND coalesce(price, 0.5) IN (1.5, 2)",
            ),
            (
                "SELECT * FROM transfers WHERE CAST(amount AS TEXT) = '1.5' AND length(CAST(amount AS TEXT)) > 1.5",
                "SELECT * FROM transfers WHERE decimal_arb_to_string(amount) = '1.5' AND length(decimal_arb_to_string(amount)) > 1.5",
            ),
        ];
        for (input, expected) in cases {
            let result = preprocess_bigint_binary_ops_with_schema(&ctx, input)
                .await
                .unwrap();
            assert_eq!(result, expected, "input: {input}");
        }
    }

    // ---------------- decimal_arb CAST AS TEXT ----------------
    //
    // Wide-integer columns (Avro decimal(p, 0) with p > 76) arrive in
    // streamling SQL as decimal_arb. DataFusion has no
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
