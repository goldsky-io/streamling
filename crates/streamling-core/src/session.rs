use crate::dynamic_table::DynamicTableRegistry;
use crate::functions::StreamlingFunctions;
use crate::operators::planner::StreamlingQueryPlanner;
use crate::optimizer::StreamlingPhysicalOptimizerRules;
use crate::types::bigint_sql_preprocessor::preprocess_bigint_sql;
use crate::{streamling_err, streamling_user_err};
use datafusion::catalog::memory::MemorySchemaProvider;
use datafusion::catalog::{SchemaProvider, Session, TableProvider};
use datafusion::common::{config::ConfigExtension, extensions_options};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{FunctionRegistry, SessionState, SessionStateBuilder};
use datafusion::logical_expr::lit;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, col};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;
use streamling_common::functions::decimal_arb_aggregates::{
    DecimalArbAvgUdaf, DecimalArbExtremeUdaf, DecimalArbSumUdaf,
};
use streamling_common::functions::decimal_arb_coercion::DecimalArbExprPlanner;
use streamling_common::functions::decimal_arb_predicate_optimizer::DecimalArbExprRewrite;
use streamling_common::functions::decimal_arb_sort_optimizer::DecimalArbSortRewriteRule;
use streamling_flink_compat::{register_json_functions, register_string_aliases};

pub static DEFAULT_CATALOG_NAME: &str = "default";
pub static DEFAULT_SCHEMA_NAME: &str = "default";

/// SessionManager is a thin wrapper around a DataFusion SessionContext that provides
/// catalog DDL methods and other functionality.
/// A clone of a SessionManager is a shallow copy of a SessionContext with the same session id and state.
#[derive(Clone)]
pub struct SessionManager {
    ctx: SessionContext,
}

#[derive(Clone, Copy)]
pub enum SubqueryHandling {
    Recurse,
    Return,
}

extensions_options! {
    pub struct StreamlingConfig {
        pub internal_buffer_size: usize, default = 10
    }
}

impl StreamlingConfig {
    pub fn new(internal_buffer_size: usize) -> Self {
        Self {
            internal_buffer_size,
        }
    }
}

impl ConfigExtension for StreamlingConfig {
    const PREFIX: &'static str = "streamling";
}

/// Helper function to get StreamlingConfig from SessionState
pub fn get_streamling_config(session_state: &SessionState) -> Result<&StreamlingConfig> {
    session_state
        .config()
        .options()
        .extensions
        .get::<StreamlingConfig>()
        .ok_or_else(|| streamling_err!("StreamlingConfig not found in session state").into())
}

/// Helper function to get StreamlingConfig from a Session trait object
pub fn get_streamling_config_from_session(session: &dyn Session) -> Result<&StreamlingConfig> {
    let session_state = session
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| {
            DataFusionError::from(streamling_err!(
                "cannot downcast Session to SessionState: unsupported Session type"
            ))
        })?;
    get_streamling_config(session_state)
}

impl SessionManager {
    pub fn new(
        batch_size: u64,
        internal_buffer_size: u32,
        dynamic_table_registry: DynamicTableRegistry,
    ) -> Result<Self> {
        let config = SessionConfig::new()
            .set_bool("datafusion.catalog.information_schema", true)
            .set_bool("datafusion.catalog.create_default_catalog_and_schema", true)
            // this increases e2e latency and can keep intermediate results in memory indefinitely, so we disable it
            .set_bool("datafusion.execution.coalesce_batches", false)
            // DataFusion 54's ExtractLeafExpressions rule rewrites plans by
            // pulling leaf expressions (struct/map field access) into
            // extraction projections aliased `__datafusion_extracted_N`, with
            // recovery projections meant to hide the extra columns again. Our
            // user-defined extension nodes (Wrapper/Rebatch/MultiSink/...)
            // capture their schema when built, and the rule's alias counter is
            // not stable across re-optimization, so with the rule enabled
            // plans that contain such extension nodes fail in three ways:
            //   - "Extension planner for Wrapper ... created an ExecutionPlan
            //     with mismatched schema" (the logical schema carries one set
            //     of `__datafusion_extracted_N` names, physical planning
            //     regenerates different ones) — a hard planning error,
            //   - extraction columns leaking past the recovery projection
            //     into sinks ("number of columns(N+1) must match number of
            //     fields(N)" when building the sink batch),
            //   - `arrow-schema` index-out-of-bounds panic in `Schema::field`
            //     (the leaked phantom column reaching an unchecked index).
            // Disable the rule to keep pre-54 planning semantics for
            // streaming plans built around extension nodes.
            .set_bool(
                "datafusion.optimizer.enable_leaf_expression_pushdown",
                false,
            )
            .set_u64("datafusion.execution.batch_size", batch_size)
            .set_str(
                "datafusion.catalog.default_catalog",
                crate::session::DEFAULT_CATALOG_NAME,
            )
            .set_str(
                "datafusion.catalog.default_schema",
                crate::session::DEFAULT_SCHEMA_NAME,
            )
            // Convert Utf8View/BinaryView to LargeUtf8/LargeBinary for compatibility with sinks like ClickHouse
            // This is used when we convert to arrow ipc for export as well as any other output conversion
            // directly using datafusion.
            .set_bool("datafusion.optimizer.expand_views_at_output", true)
            // Recurse into subdirectories when listing files. DataFusion defaults this
            // to `true` (ignore subdirs), which makes the file source skip nested
            // folders; Used by the file source
            .set_bool(
                "datafusion.execution.listing_table_ignore_subdirectory",
                false,
            )
            .with_option_extension(StreamlingConfig::new(internal_buffer_size as usize));

        let runtime = Arc::new(RuntimeEnv::default());

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .with_physical_optimizer_rules(StreamlingPhysicalOptimizerRules::rules())
            // T046: rewrite ORDER BY decimal_arb_col -> ORDER BY
            // decimal_arb_to_sort_key(...) so DataFusion's bytewise
            // sort over the canonical encoding produces correct
            // numeric ordering across signs (FR-005).
            .with_optimizer_rule(Arc::new(DecimalArbSortRewriteRule::new()))
            .build();

        let mut ctx = SessionContext::new_with_state(state);

        register_json_functions(&ctx)?;
        register_string_aliases(&ctx)?;

        for udf in StreamlingFunctions::functions(dynamic_table_registry) {
            // This can override a built-in function with the same name
            ctx.register_udf(udf);
        }

        // T047: register decimal_arb aggregate UDAFs (override built-in
        // sum/min/max/avg for decimal_arb input columns) and the
        // ExprPlanner (auto-bind native +/-/*/`/`%/=/!=/</<=/>/`>=` to
        // decimal_arb_<op> ScalarUDFs when both operands are decimal_arb).
        // The T007 spike confirmed register_udaf overrides the built-in
        // for that name; the T005 spike confirmed register_expr_planner
        // wires the planner into SQL frontend planning.
        ctx.register_udaf(DecimalArbSumUdaf::into_udaf());
        ctx.register_udaf(DecimalArbExtremeUdaf::min_udaf());
        ctx.register_udaf(DecimalArbExtremeUdaf::max_udaf());
        ctx.register_udaf(DecimalArbAvgUdaf::into_udaf());
        ctx.register_expr_planner(Arc::new(DecimalArbExprPlanner::new()))?;
        // Rewrite `BETWEEN` / `IN` over decimal_arb into the decimal_arb
        // comparison UDFs (F1b). Registered as a FunctionRewrite so it runs in
        // the analyzer *before* TypeCoercion — which would otherwise fail to
        // reconcile LargeBinary against the Int64 bounds before any optimizer
        // rule could rewrite the Between/InList node.
        ctx.register_function_rewrite(Arc::new(DecimalArbExprRewrite::new()))?;

        crate::plugin::udf::register_plugin_udfs(&ctx);

        Ok(SessionManager { ctx })
    }

    /// Registers a table with the given name and provider in the default catalog and the default OR custom schema.
    /// table_name can be in the format "schema.table" or just "table".
    pub fn register_table(
        &self,
        full_table_name: &str,
        table_provider: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let (schema_name, table_name) = Self::extract_schema_and_table_names(full_table_name);

        let schema = self.register_or_get_schema(schema_name)?;

        schema.register_table(table_name.to_string(), table_provider)
    }

    /// Creates a logical plan given the provided SQL statement IF it's supported by Streamling.
    pub async fn create_supported_logical_plan(
        &self,
        sql: String,
    ) -> Result<(LogicalPlan, String)> {
        // Pre-process SQL for bigint (casts + binary ops)
        let processed_sql_bigint = preprocess_bigint_sql(&self.ctx, &sql).await?;
        let logical_plan = self
            .ctx
            .state()
            .create_logical_plan(&processed_sql_bigint)
            .await?;

        // Ensure _gs_op transparently propagates through SQL projections
        let logical_plan = Self::append_gs_op_to_projection_if_missing(logical_plan);

        Self::validate_plan_and_extract_source_name(&logical_plan, SubqueryHandling::Recurse)
            .map(|name| (logical_plan, name))
    }

    /// Same as create_supported_logical_plan, but without validation. It doesn't return the source name.
    pub async fn create_logical_plan(&self, sql: String) -> Result<LogicalPlan> {
        // Pre-process SQL for bigint (casts + binary ops)
        let processed_sql_bigint = preprocess_bigint_sql(&self.ctx, &sql).await?;
        self.ctx
            .state()
            .create_logical_plan(&processed_sql_bigint)
            .await
    }

    pub fn validate_plan_and_extract_source_name(
        plan: &LogicalPlan,
        subquery_handling: SubqueryHandling,
    ) -> Result<String> {
        match plan {
            // TODO: add support for more plan types
            LogicalPlan::TableScan(scan) => Ok(scan.table_name.to_string()),
            LogicalPlan::Projection(projection) => Self::validate_plan_and_extract_source_name(
                projection.input.as_ref(),
                subquery_handling,
            ),
            LogicalPlan::Filter(filter) => Self::validate_plan_and_extract_source_name(
                filter.input.as_ref(),
                subquery_handling,
            ),
            LogicalPlan::SubqueryAlias(subquery) => match subquery_handling {
                SubqueryHandling::Recurse => Self::validate_plan_and_extract_source_name(
                    subquery.input.as_ref(),
                    subquery_handling,
                ),
                // This can be used to extract the transform name (not the underlying source name)
                SubqueryHandling::Return => Ok(String::from(subquery.alias.table())),
            },
            LogicalPlan::Unnest(unnest) => Self::validate_plan_and_extract_source_name(
                unnest.input.as_ref(),
                subquery_handling,
            ),
            LogicalPlan::Union(union) => {
                // XXX(jeffling): currently we only support one input,
                // but we should support multiple inputs
                Self::validate_plan_and_extract_source_name(
                    union.inputs[0].as_ref(),
                    subquery_handling,
                )
            }
            LogicalPlan::Extension(extension) => {
                let name = extension.node.name();
                match name {
                    "WrappingNode" => Self::validate_plan_and_extract_source_name(
                        extension.node.inputs()[0],
                        subquery_handling,
                    ),
                    "Checkpointable" => Self::validate_plan_and_extract_source_name(
                        extension.node.inputs()[0],
                        subquery_handling,
                    ),
                    "Plugin" => Self::validate_plan_and_extract_source_name(
                        extension.node.inputs()[0],
                        subquery_handling,
                    ),
                    _ => Err(streamling_user_err!("unsupported extension: {}", name).into()),
                }
            }
            _ => Err(streamling_user_err!("unsupported plan: {}", plan.display()).into()),
        }
    }

    pub fn new_df(&self, plan: LogicalPlan) -> DataFrame {
        DataFrame::new(self.ctx.state(), plan)
    }

    /// Recursively rewrite a plan to transparently propagate `_gs_op` through
    /// projections and across unions. For projections that omit `_gs_op` while
    /// the child still has it, append it. For unions, ensure all inputs carry
    /// `_gs_op` consistently by projecting it into inputs that dropped it.
    fn append_gs_op_to_projection_if_missing(plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Projection(proj) => {
                // First, rewrite the child
                let rewritten_child =
                    Self::append_gs_op_to_projection_if_missing(proj.input.as_ref().clone());

                let child_has_gs_op = rewritten_child
                    .schema()
                    .field_with_unqualified_name(crate::data::COLUMN_NAME_OP)
                    .is_ok();

                // Try to rebuild the same projection over the rewritten child
                let mut exprs = proj.expr.clone();
                let base_projection = LogicalPlanBuilder::from(rewritten_child.clone())
                    .project(exprs.clone())
                    .and_then(|b| b.build());

                match base_projection {
                    Ok(built) => {
                        // If child has `_gs_op` but this projection still doesn't, append it
                        let this_has_gs_op = built
                            .schema()
                            .field_with_unqualified_name(crate::data::COLUMN_NAME_OP)
                            .is_ok();
                        if child_has_gs_op && !this_has_gs_op {
                            exprs.push(col(crate::data::COLUMN_NAME_OP));
                            LogicalPlanBuilder::from(rewritten_child)
                                .project(exprs)
                                .and_then(|b| b.build())
                                .unwrap_or(built)
                        } else {
                            built
                        }
                    }
                    Err(_) => {
                        // Fallback to original plan on any rebuild error
                        LogicalPlan::Projection(proj)
                    }
                }
            }
            LogicalPlan::Union(union) => {
                // Recursively rewrite all inputs first
                let mut new_inputs: Vec<LogicalPlan> = union
                    .inputs
                    .iter()
                    .map(|p| Self::append_gs_op_to_projection_if_missing(p.as_ref().clone()))
                    .collect();

                // If any input has `_gs_op`, align all inputs to include it
                let any_has_gs_op = new_inputs.iter().any(|p| {
                    p.schema()
                        .field_with_unqualified_name(crate::data::COLUMN_NAME_OP)
                        .is_ok()
                });

                if any_has_gs_op {
                    for input in &mut new_inputs {
                        let has_gs_op = input
                            .schema()
                            .field_with_unqualified_name(crate::data::COLUMN_NAME_OP)
                            .is_ok();
                        if !has_gs_op {
                            // Project all existing fields plus a default `_gs_op` column.
                            // Use a default of 'i' (Insert) to keep semantics consistent
                            // with stream ingestion defaults when op is unknown.
                            let mut exprs = input
                                .schema()
                                .fields()
                                .iter()
                                .map(|f| col(f.name()))
                                .collect::<Vec<_>>();
                            exprs.push(
                                lit(ScalarValue::Utf8(Some(
                                    crate::data::RowKind::Insert.to_str(),
                                )))
                                .alias(crate::data::COLUMN_NAME_OP),
                            );

                            if let Ok(built) = LogicalPlanBuilder::from(input.clone())
                                .project(exprs)
                                .and_then(|b| b.build())
                            {
                                *input = built;
                            }
                        }
                    }
                }

                // Rebuild the Union with the possibly-updated inputs by chaining unions
                if new_inputs.is_empty() {
                    return LogicalPlan::Union(union);
                }
                let mut iter = new_inputs.into_iter();
                let first = iter.next().unwrap();
                let mut builder = LogicalPlanBuilder::from(first);
                for next in iter {
                    match builder.union(next) {
                        Ok(b) => builder = b,
                        Err(_) => return LogicalPlan::Union(union),
                    }
                }
                builder.build().unwrap_or(LogicalPlan::Union(union))
            }
            // Single-input nodes we care less about for `_gs_op` logic. Just recurse.
            LogicalPlan::Filter(filter) => {
                let rewritten_child =
                    Self::append_gs_op_to_projection_if_missing(filter.input.as_ref().clone());
                LogicalPlanBuilder::from(rewritten_child)
                    .filter(filter.predicate.clone())
                    .and_then(|b| b.build())
                    .unwrap_or(LogicalPlan::Filter(filter))
            }
            // For other node types (including SubqueryAlias/Unnest), keep as-is.
            // Default: return the plan unchanged
            other => other,
        }
    }

    pub fn session_state(&self) -> datafusion::execution::SessionState {
        self.ctx.state().clone()
    }

    pub fn session_context(&self) -> SessionContext {
        self.ctx.clone()
    }

    /// Registers a schema with the given name in the default catalog (or returns it if it already exists).
    fn register_or_get_schema(&self, schema_name: &str) -> Result<Arc<dyn SchemaProvider>> {
        let catalog = self.ctx.catalog(DEFAULT_CATALOG_NAME).ok_or_else(|| {
            DataFusionError::from(streamling_err!(
                "default catalog '{}' is not available",
                DEFAULT_CATALOG_NAME
            ))
        })?;

        if let Some(schema) = catalog.schema(schema_name) {
            return Ok(schema);
        }

        catalog
            .register_schema(schema_name, Arc::new(MemorySchemaProvider::new()))
            .map_err(|err| {
                DataFusionError::from(streamling_err!(
                    "failed to register schema '{}': {}",
                    schema_name,
                    err
                ))
            })?;

        catalog.schema(schema_name).ok_or_else(|| {
            DataFusionError::from(streamling_err!(
                "schema '{}' was not found after registration",
                schema_name
            ))
        })
    }

    /// Extracts the schema and table names from a fully qualified table name.
    pub fn extract_schema_and_table_names(full_table_name: &str) -> (&str, &str) {
        match full_table_name.find(".") {
            Some(idx) => (&full_table_name[..idx], &full_table_name[idx + 1..]),
            None => (DEFAULT_SCHEMA_NAME, full_table_name),
        }
    }
}

// TODO: more test coverage
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    /// Build a `t(id, amt decimal_arb(100,0), alt decimal_arb(100,0), flag)`
    /// MemTable: amt = [111, 222], alt = [999, 888], flag = [1, 0].
    fn register_decimal_arb_case_table(sm: &SessionManager) {
        use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue};
        use arrow::array::StringArray;
        use datafusion::datasource::MemTable;
        use std::str::FromStr;

        let col = |vals: [&str; 2], name: &str| {
            let mut b = DecimalArbArrayBuilder::with_capacity(2, name, 100, 0).unwrap();
            for v in vals {
                b.append_value(&DecimalArbValue::from_str(v).unwrap())
                    .unwrap();
            }
            b.finish().into_inner().0
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            DecimalArbType::field("amt", 100, 0, true).unwrap(),
            DecimalArbType::field("alt", 100, 0, true).unwrap(),
            Field::new("flag", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(col(["111", "222"], "amt")),
                Arc::new(col(["999", "888"], "alt")),
                Arc::new(Int64Array::from(vec![1, 0])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        sm.register_table("t", Arc::new(table)).unwrap();
    }

    /// `CASE` over `decimal_arb` branches plans, executes, and selects the right
    /// values through the full session — the decimal_arb `ExprPlanner` only
    /// rewrites binary ops, so this confirms CASE rides DataFusion's native
    /// coercion of the underlying `LargeBinary` without erroring. (Mirrors the
    /// "does not kill the stream" u256 CASE test retired in feature 002.)
    #[tokio::test]
    async fn case_over_decimal_arb_plans_and_selects_correct_values() {
        use crate::types::decimal_arb::DecimalArbValue;
        use arrow::array::{Array, LargeBinaryArray};

        let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap();
        register_decimal_arb_case_table(&sm);

        let sql = "SELECT id, CASE WHEN flag = 1 THEN amt ELSE alt END AS chosen FROM t";
        let plan = sm
            .create_logical_plan(sql.to_string())
            .await
            .expect("CASE over decimal_arb should plan");
        let batches = sm
            .new_df(plan)
            .collect()
            .await
            .expect("CASE over decimal_arb should execute");

        // row 0: flag=1 -> amt=111 ; row 1: flag=0 -> alt=888
        let col = batches[0]
            .column_by_name("chosen")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .expect("decimal_arb storage is LargeBinary");
        let v0 = DecimalArbValue::from_canonical_bytes_at_scale(col.value(0), 0).unwrap();
        let v1 = DecimalArbValue::from_canonical_bytes_at_scale(col.value(1), 0).unwrap();
        assert_eq!(v0.to_canonical_string(), "111");
        assert_eq!(v1.to_canonical_string(), "888");
    }

    /// F2 FIXED: `CASE` over `decimal_arb` used to come back as a bare
    /// `LargeBinary` (extension metadata dropped), so a Postgres/ClickHouse sink
    /// treated it as raw BYTEA instead of `NUMERIC(p, s)`. The
    /// `DecimalArbExprRewrite` FunctionRewrite now re-stamps the metadata, so the
    /// `chosen` output is a proper decimal_arb field again.
    #[tokio::test]
    async fn case_over_decimal_arb_should_preserve_metadata() {
        use crate::types::decimal_arb::DecimalArbType;

        let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap();
        register_decimal_arb_case_table(&sm);

        let sql = "SELECT id, CASE WHEN flag = 1 THEN amt ELSE alt END AS chosen FROM t";
        let plan = sm.create_logical_plan(sql.to_string()).await.unwrap();
        let batches = sm.new_df(plan).collect().await.unwrap();

        let field = batches[0]
            .schema()
            .field_with_name("chosen")
            .unwrap()
            .clone();
        assert!(
            DecimalArbType::is_decimal_arb_field(&field),
            "CASE output lost decimal_arb metadata: {field:?}"
        );
    }

    /// Register a single-column `t(amt decimal_arb(100,0))` MemTable from a list
    /// of textual decimal values (each canonicalized via `DecimalArbValue`).
    fn register_decimal_arb_values(sm: &SessionManager, table: &str, values: &[&str]) {
        use crate::types::decimal_arb::{DecimalArbArrayBuilder, DecimalArbType, DecimalArbValue};
        use datafusion::datasource::MemTable;
        use std::str::FromStr;

        let mut b = DecimalArbArrayBuilder::with_capacity(values.len(), "amt", 100, 0).unwrap();
        for v in values {
            b.append_value(&DecimalArbValue::from_str(v).unwrap())
                .unwrap();
        }
        let amt = b.finish().into_inner().0;
        let schema = Arc::new(Schema::new(vec![
            DecimalArbType::field("amt", 100, 0, true).unwrap(),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(amt)]).unwrap();
        let table_provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        sm.register_table(table, Arc::new(table_provider)).unwrap();
    }

    /// `GROUP BY` over `decimal_arb` groups by the canonical bytes. This verifies
    /// the equality assumption end-to-end: numerically-equal values written in
    /// different textual forms (`5` / `5.0` / `05`) collapse into ONE group, and
    /// `+0` / `-0` are the same group — i.e. grouping is numerically correct, not
    /// a silent byte-representation split.
    #[tokio::test]
    async fn group_by_decimal_arb_groups_numerically_equal_values() {
        use crate::types::decimal_arb::DecimalArbValue;
        use arrow::array::{Array, LargeBinaryArray};
        use std::collections::HashMap;

        let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap();
        register_decimal_arb_values(&sm, "t", &["5", "5.0", "05", "-3", "-3", "0", "-0"]);

        let sql = "SELECT amt, COUNT(*) AS n FROM t GROUP BY amt";
        let plan = sm
            .create_logical_plan(sql.to_string())
            .await
            .expect("GROUP BY decimal_arb should plan");
        let batches = sm
            .new_df(plan)
            .collect()
            .await
            .expect("GROUP BY decimal_arb should execute");

        let mut counts: HashMap<String, i64> = HashMap::new();
        for batch in &batches {
            let amt = batch
                .column_by_name("amt")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            let n = batch
                .column_by_name("n")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let key = DecimalArbValue::from_canonical_bytes_at_scale(amt.value(i), 0)
                    .unwrap()
                    .to_canonical_string();
                *counts.entry(key).or_default() += n.value(i);
            }
        }

        assert_eq!(
            counts.len(),
            3,
            "exactly three distinct numeric groups expected: {counts:?}"
        );
        assert_eq!(
            counts.get("5"),
            Some(&3),
            "5 / 5.0 / 05 must collapse to one group of 3: {counts:?}"
        );
        assert_eq!(counts.get("-3"), Some(&2), "{counts:?}");
        assert_eq!(
            counts.get("0"),
            Some(&2),
            "+0 and -0 must be the same group: {counts:?}"
        );
    }

    /// `SELECT DISTINCT` over `decimal_arb` dedupes by numeric value, not textual
    /// form — the same byte-equality guarantee as GROUP BY.
    #[tokio::test]
    async fn distinct_decimal_arb_dedupes_numerically_equal_values() {
        use crate::types::decimal_arb::DecimalArbValue;
        use arrow::array::{Array, LargeBinaryArray};
        use std::collections::BTreeSet;

        let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap();
        register_decimal_arb_values(&sm, "t", &["5", "5.0", "-3", "-3", "0", "-0", "7"]);

        let sql = "SELECT DISTINCT amt FROM t";
        let plan = sm
            .create_logical_plan(sql.to_string())
            .await
            .expect("DISTINCT decimal_arb should plan");
        let batches = sm
            .new_df(plan)
            .collect()
            .await
            .expect("DISTINCT decimal_arb should execute");

        let mut distinct: BTreeSet<String> = BTreeSet::new();
        for batch in &batches {
            let amt = batch
                .column_by_name("amt")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                distinct.insert(
                    DecimalArbValue::from_canonical_bytes_at_scale(amt.value(i), 0)
                        .unwrap()
                        .to_canonical_string(),
                );
            }
        }

        let expected: BTreeSet<String> = ["5", "-3", "0", "7"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            distinct, expected,
            "DISTINCT must dedupe numerically-equal decimal_arb values"
        );
    }

    #[test]
    fn test_extract_schema_and_table_names() {
        assert_eq!(
            SessionManager::extract_schema_and_table_names("schema_a.table_b"),
            ("schema_a", "table_b")
        );
        assert_eq!(
            SessionManager::extract_schema_and_table_names("table_c"),
            (DEFAULT_SCHEMA_NAME, "table_c")
        );
    }

    /// Regression test (code-reviewer-pro: SUM/MIN/MAX/AVG UDAFs are
    /// registered under built-in names, so a pipeline calling SUM on a
    /// plain Int64/Float64 column must still resolve via the built-in
    /// aggregate — not error with a coerce-to-LargeBinary type mismatch.
    #[tokio::test]
    async fn builtin_aggregates_still_work_for_non_decimal_arb_columns() {
        let registry = DynamicTableRegistry::new();
        let sm = SessionManager::new(1024, 64, registry).unwrap();
        let ctx = sm.session_context();

        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("f", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5])),
                Arc::new(Float64Array::from(vec![1.5_f64, 2.5, 3.5, 4.5, 5.5])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();

        for (sql, expected_col, expected_val) in [
            ("SELECT SUM(i) AS s FROM t", "s", "15"),
            ("SELECT MIN(i) AS m FROM t", "m", "1"),
            ("SELECT MAX(i) AS m FROM t", "m", "5"),
            ("SELECT AVG(f) AS a FROM t", "a", "3.5"),
            // The `mean` alias for AVG is registered by DataFusion's built-in
            // and must be preserved through the wrapper's `aliases` delegation.
            ("SELECT MEAN(f) AS a FROM t", "a", "3.5"),
        ] {
            let df = ctx
                .sql(sql)
                .await
                .unwrap_or_else(|e| panic!("plan failed for `{sql}`: {e}"));
            let batches = df
                .collect()
                .await
                .unwrap_or_else(|e| panic!("collect failed for `{sql}`: {e}"));
            assert_eq!(
                batches.len(),
                1,
                "expected one output batch for `{sql}`, got {}",
                batches.len()
            );
            assert_eq!(batches[0].num_rows(), 1, "expected one row for `{sql}`");
            let col = batches[0]
                .column_by_name(expected_col)
                .unwrap_or_else(|| panic!("missing output column `{expected_col}` for `{sql}`"));
            let formatter = arrow::util::display::ArrayFormatter::try_new(
                col,
                &arrow::util::display::FormatOptions::default(),
            )
            .unwrap();
            assert_eq!(
                formatter.value(0).to_string(),
                expected_val,
                "wrong result for `{sql}`"
            );
        }
    }
}
