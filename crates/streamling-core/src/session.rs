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
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::lit;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, col};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;
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
            .with_option_extension(StreamlingConfig::new(internal_buffer_size as usize));

        let runtime = Arc::new(RuntimeEnv::default());

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_query_planner(Arc::new(StreamlingQueryPlanner::new()))
            .with_physical_optimizer_rules(StreamlingPhysicalOptimizerRules::rules())
            .build();

        let ctx = SessionContext::new_with_state(state);

        register_json_functions(&ctx)?;
        register_string_aliases(&ctx)?;

        for udf in StreamlingFunctions::functions(dynamic_table_registry) {
            // This can override a built-in function with the same name
            ctx.register_udf(udf);
        }

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

    // Test infrastructure for end-to-end CASE-over-u256 behavior. Exercises the
    // full session (preprocessor + registered UDFs), so it catches both the
    // type-coercion failure and the field-metadata loss that a bare
    // SessionContext (used in the preprocessor's string-only tests) cannot.
    use crate::types::u256::{U256, U256Type, u256_to_bytes};
    use datafusion::arrow::array::{
        Array, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;

    fn u256_col(vals: &[Option<u64>]) -> FixedSizeBinaryArray {
        let iter = vals
            .iter()
            .map(|v| v.map(|x| u256_to_bytes(&U256::from(x)).to_vec()));
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(iter, 32).unwrap()
    }

    /// A `txs` table with two u256 columns (`gas_price`, nullable
    /// `effective_gas_price`) and a non-u256 `flag` for CASE conditions.
    fn u256_session() -> SessionManager {
        let sm = SessionManager::new(8192, 10, DynamicTableRegistry::new()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("gas_price", U256Type::new(), false).with_metadata(U256Type::metadata()),
            Field::new("effective_gas_price", U256Type::new(), true)
                .with_metadata(U256Type::metadata()),
            Field::new("flag", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(u256_col(&[Some(10), Some(20)])),
                Arc::new(u256_col(&[None, Some(99)])),
                Arc::new(Int64Array::from(vec![1, 0])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        sm.register_table("txs", Arc::new(table)).unwrap();
        sm
    }

    async fn collect_sql(sm: &SessionManager, sql: &str) -> Result<Vec<RecordBatch>> {
        let plan = sm.create_logical_plan(sql.to_string()).await?;
        sm.new_df(plan).collect().await
    }

    #[tokio::test]
    async fn test_case_with_u256_and_literal_branch_does_not_kill_stream() {
        // A literal branch (`ELSE 0`) alongside a u256 branch must not fail
        // type-coercion (FixedSizeBinary(32) vs Int64). Before the fix this
        // errored with "Failed to coerce then/else ... in CASE WHEN expression",
        // which terminates the stream.
        let sm = u256_session();
        let sql = "SELECT id, u256_to_string(CASE WHEN flag = 1 THEN gas_price ELSE 0 END) AS chosen FROM txs";
        let batches = collect_sql(&sm, sql)
            .await
            .expect("CASE with literal branch over u256 should plan and execute");
        let batch = &batches[0];
        let chosen = batch
            .column(batch.schema().index_of("chosen").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("chosen should be a string");
        // row 0: flag=1 -> gas_price=10 ; row 1: flag=0 -> 0
        assert_eq!(chosen.value(0), "10");
        assert_eq!(chosen.value(1), "0");
    }

    #[tokio::test]
    async fn test_case_over_u256_preserves_metadata_for_numeric_sink() {
        // A CASE whose branches are u256 must yield an output field that still
        // carries the streamling.u256 extension metadata, otherwise the Postgres
        // sink falls back to BYTEA instead of numeric(78,0).
        let sm = u256_session();
        let sql = "SELECT id, CASE WHEN flag = 1 THEN gas_price ELSE effective_gas_price END AS chosen FROM txs";
        let batches = collect_sql(&sm, sql)
            .await
            .expect("CASE over u256 should execute");
        let field = batches[0]
            .schema()
            .field_with_name("chosen")
            .unwrap()
            .clone();
        assert_eq!(field.data_type(), &DataType::FixedSizeBinary(32));
        assert!(
            U256Type::is_u256_metadata(field.metadata()),
            "CASE output lost u256 metadata: {:?}",
            field.metadata()
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
}
