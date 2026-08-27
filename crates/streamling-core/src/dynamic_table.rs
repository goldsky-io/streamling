mod in_memory;
mod key_set;
mod postgres;

use arrow_schema::DataType;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, StringArray, StringViewArray,
};
use datafusion::arrow::compute::filter as arrow_filter;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{ColumnarValue, Expr, LogicalPlan};
use datafusion::physical_expr::PhysicalExpr;
use enum_display::EnumDisplay;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use streamling_config::app_config::{DynamicTableBackendConfig, DynamicTableBackendType};

use crate::error::Result as StreamlingResult;
use crate::{streamling_err, streamling_user_err};

use crate::dynamic_table::postgres::PostgresDynamicTableBackendFactory;
use crate::session::SessionManager;
use crate::side_output::SourceSideOutput;
use crate::topology::{DynamicTableTransform, PipelineTopology, Transform};
pub use in_memory::InMemoryDynamicTableBackend;
pub use postgres::PostgresDynamicTableBackend;

#[derive(Debug, PartialEq, Eq, EnumDisplay)]
pub enum DynamicTableBackendError {
    Initialization(String),
    Connection(String),
    Query(String),
    StringArrayExpected,
}

impl std::error::Error for DynamicTableBackendError {}

impl From<DynamicTableBackendError> for crate::error::StreamlingError {
    fn from(err: DynamicTableBackendError) -> Self {
        let retriable = matches!(err, DynamicTableBackendError::Connection(_));
        crate::error::StreamlingError::from_parts(anyhow::Error::new(err), retriable, true)
    }
}

#[async_trait]
pub trait DynamicTableBackend: Debug + Sync + Send {
    async fn append(&self, values: ArrayRef) -> Result<(), DynamicTableBackendError>;
    async fn contains(&self, values: ArrayRef) -> Result<ArrayRef, DynamicTableBackendError>;
}

pub struct DynamicTableBackendFactory {
    config: DynamicTableBackendConfig,
}

impl DynamicTableBackendFactory {
    pub fn new(config: DynamicTableBackendConfig) -> Self {
        Self { config }
    }
}

impl DynamicTableBackendFactory {
    pub async fn create(
        &self,
        backend_type: DynamicTableBackendType,
        backend_entity_name: String,
        schema: Option<String>,
        column: Option<String>,
        time_column: Option<String>,
        cache_refresh_debounce_ms: u64,
    ) -> Result<Arc<dyn DynamicTableBackend>, DynamicTableBackendError> {
        let max_batch_size = self.config.max_batch_size.unwrap_or(1000);
        match backend_type {
            DynamicTableBackendType::InMemory => Ok(Arc::new(InMemoryDynamicTableBackend::new(
                backend_entity_name,
                schema,
                column,
                time_column,
                max_batch_size,
                // InMemory has no freshness-check cache; the debounce window does not apply.
            ))),
            DynamicTableBackendType::Postgres => {
                let postgres_config = self.config.postgres.clone().ok_or_else(|| {
                    DynamicTableBackendError::Initialization(
                        "Postgres backend config is required".to_string(),
                    )
                })?;

                // TODO: store the factory somewhere to avoid recreating it each time
                // Note: new() doesn't connect - connection happens lazily on first use
                let factory = PostgresDynamicTableBackendFactory::new(postgres_config)?;

                let backend = factory
                    .create_backend(
                        backend_entity_name,
                        schema,
                        column,
                        time_column,
                        max_batch_size,
                        cache_refresh_debounce_ms,
                    )
                    .await?;
                Ok(Arc::new(backend))
            }
        }
    }
}

/// A thread-safe registry for dynamic table backends that can be updated at runtime
#[derive(Debug, Clone)]
pub struct DynamicTableRegistry {
    backends: Arc<RwLock<HashMap<String, Arc<dyn DynamicTableBackend>>>>,
}

impl DynamicTableRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            backends: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new backend with the given table name
    pub fn register(
        &self,
        name: String,
        backend: Arc<dyn DynamicTableBackend>,
    ) -> StreamlingResult<()> {
        let mut backends = self
            .backends
            .write()
            .map_err(|_| streamling_err!("Failed to acquire write lock on backends registry"))?;

        if backends.contains_key(&name) {
            return Err(streamling_err!("Backend '{}' is already registered", name));
        }

        backends.insert(name, backend);
        Ok(())
    }

    /// Get a backend by name
    pub fn get(&self, name: &str) -> StreamlingResult<Option<Arc<dyn DynamicTableBackend>>> {
        let backends = self
            .backends
            .read()
            .map_err(|_| streamling_err!("Failed to acquire read lock on backends registry"))?;
        Ok(backends.get(name).cloned())
    }
}

impl Default for DynamicTableRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct DynamicTableSideOutput {
    backend: Arc<dyn DynamicTableBackend>,
    projection_expr: Arc<dyn PhysicalExpr>,
    filter_expr: Option<Arc<dyn PhysicalExpr>>,
}

impl DynamicTableSideOutput {
    pub fn new(
        backend: Arc<dyn DynamicTableBackend>,
        projection_logical_exprs: Vec<Expr>,
        filter_logical_expr: Option<Expr>,
        session_state: &SessionState,
        df_schema: &DFSchema,
    ) -> Result<Self> {
        if projection_logical_exprs.is_empty() {
            return Err(streamling_user_err!(
                "at least one projection expression is required for dynamic table side output"
            )
            .into());
        }

        // Dynamic table backends only support a single string column
        if projection_logical_exprs.len() > 1 {
            return Err(streamling_user_err!(
                "only one projection expression is supported for dynamic table side output (got {})",
                projection_logical_exprs.len()
            )
            .into());
        }

        let projection_expr =
            session_state.create_physical_expr(projection_logical_exprs[0].clone(), df_schema)?;

        let filter_expr = if let Some(filter) = filter_logical_expr {
            Some(session_state.create_physical_expr(filter, df_schema)?)
        } else {
            None
        };

        Ok(Self {
            backend,
            projection_expr,
            filter_expr,
        })
    }
}

#[async_trait]
impl SourceSideOutput for DynamicTableSideOutput {
    /// - If a filter expression is provided, it is applied first to filter the input batch.
    /// - Then the projection expression is applied to extract the values to be stored in the dynamic table.
    /// - The projection expression must result in a single string column.
    /// - The resulting values are appended to the dynamic table backend.
    async fn process(&self, batch: &RecordBatch) -> Result<()> {
        let mut current_batch = batch.clone();
        let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();

        if let Some(filter) = &self.filter_expr {
            let value = filter.evaluate(batch)?;
            let mask = match value {
                ColumnarValue::Array(a) => a
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::from(streamling_err!(
                            "dynamic table filter expression must return a boolean array, got {:?}",
                            a.data_type()
                        ))
                    })?,
                ColumnarValue::Scalar(s) => match s {
                    ScalarValue::Boolean(Some(b)) => BooleanArray::from(vec![b; batch.num_rows()]),
                    ScalarValue::Boolean(None) => BooleanArray::from(vec![false; batch.num_rows()]),
                    _ => {
                        return Err(streamling_err!(
                            "dynamic table filter scalar must be boolean, got {:?}",
                            s.data_type()
                        )
                        .into());
                    }
                },
            };
            let mut filtered = Vec::with_capacity(arrays.len());
            for arr in arrays.into_iter() {
                filtered.push(arrow_filter(&arr, &mask)?);
            }
            arrays = filtered;
            // Recreate the batch with filtered arrays if filtering was applied
            current_batch = RecordBatch::try_new(batch.schema(), arrays)?;
        }

        let value = self.projection_expr.evaluate(&current_batch)?;
        let value = match value {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(batch.num_rows())?,
        };

        match value.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
            other => {
                return Err(streamling_user_err!(
                    "dynamic table side output expects a string column, got {:?}",
                    other
                )
                .into());
            }
        }
        self.backend.append(value).await.map_err(|e| {
            DataFusionError::from(streamling_err!("dynamic table append failed: {e}"))
        })?;
        Ok(())
    }
}

/// Extract expressions from a SQL query. Only Projection and Filter are supported.
/// Returns a tuple of (projection expressions, optional filter expression)
pub fn extract_expressions_from_sql_query(plan: &LogicalPlan) -> Result<(Vec<Expr>, Option<Expr>)> {
    match plan {
        LogicalPlan::Projection(projection) => {
            let exprs = projection.expr.clone();
            // Unwrap common wrapper nodes to find an optional Filter or base scan
            let mut current = projection.input.as_ref();
            let filter_expr: Option<Expr> = loop {
                match current {
                    LogicalPlan::Filter(filter) => break Some(filter.predicate.clone()),
                    LogicalPlan::TableScan(_) => break None,
                    LogicalPlan::SubqueryAlias(alias) => {
                        current = alias.input.as_ref();
                    }
                    // Treat Extension as a valid base (e.g., custom table provider)
                    LogicalPlan::Extension(_) => break None,
                    _ => {
                        return Err(streamling_user_err!(
                            "only Projection and Filter SQL queries are supported for dynamic tables"
                        )
                        .into());
                    }
                }
            };
            Ok((exprs, filter_expr))
        }
        LogicalPlan::Filter(filter) => {
            let filter_expr = filter.predicate.clone();
            Ok((vec![], Some(filter_expr)))
        }
        // Allow top-level wrappers by delegating to their inputs
        LogicalPlan::SubqueryAlias(alias) => {
            extract_expressions_from_sql_query(alias.input.as_ref())
        }
        _ => Err(streamling_user_err!(
            "only Projection and Filter SQL queries are supported for dynamic tables"
        )
        .into()),
    }
}

/// Validates that any dynamic table UDFs used in the given SQL transform plan
/// are properly defined in the pipeline topology and reference the same source as the SQL transform.
pub async fn validate_dynamic_table_usage(
    sql_transform_source: String,
    sql_transform_plan: &LogicalPlan,
    pipeline_topology: &PipelineTopology,
    session_manager: &SessionManager,
) -> Result<()> {
    let dynamic_table_names = find_dynamic_table_udfs(sql_transform_plan)?;
    if dynamic_table_names.is_empty() {
        return Ok(());
    }

    // For each dynamic table UDF used, validate its configuration
    for table_name in dynamic_table_names {
        let dynamic_table_transform = pipeline_topology.transforms.get(&table_name);

        if dynamic_table_transform.is_none() {
            return Err(streamling_user_err!(
                "dynamic table '{}' is used in SQL transform but not defined in pipeline topology",
                table_name
            )
            .into());
        }

        // Check if the dynamic table has SQL defined
        if let Some(Transform::dynamic_table(DynamicTableTransform { sql: Some(sql), .. })) =
            dynamic_table_transform
        {
            let (_, dynamic_table_source) = session_manager
                .create_supported_logical_plan(sql.clone())
                .await
                .map_err(|e| {
                    DataFusionError::from(streamling_user_err!(
                        "failed to parse SQL for dynamic table '{}': {}",
                        table_name,
                        e
                    ))
                })?;

            if dynamic_table_source != sql_transform_source {
                return Err(streamling_user_err!(
                    "dynamic table '{}' uses source '{}' but SQL transform uses source '{}'; \
                     dynamic tables can only be used with SQL transforms that reference the same source",
                    table_name, dynamic_table_source, sql_transform_source
                ).into());
            }
        }
        // If no SQL is defined, we don't need to validate the source match
    }

    Ok(())
}

/// Recursively traverses the logical plan to find all dynamic table UDF calls
/// Returns a vector of table names used in dynamic_table_check UDF calls
pub fn find_dynamic_table_udfs(plan: &LogicalPlan) -> Result<Vec<String>> {
    let mut table_names = Vec::new();
    find_dynamic_table_udfs_recursive(plan, &mut table_names)?;
    Ok(table_names)
}

/// Recursive helper function to traverse the logical plan and collect dynamic table names
fn find_dynamic_table_udfs_recursive(
    plan: &LogicalPlan,
    table_names: &mut Vec<String>,
) -> Result<()> {
    // Check expressions in the current plan node
    for expr in plan.expressions() {
        find_dynamic_table_udfs_in_expr(&expr, table_names)?;
    }

    // Recursively check child plans
    for child in plan.inputs() {
        find_dynamic_table_udfs_recursive(child, table_names)?;
    }

    Ok(())
}

/// Recursively searches for dynamic_table_check UDF calls in expressions
fn find_dynamic_table_udfs_in_expr(expr: &Expr, table_names: &mut Vec<String>) -> Result<()> {
    match expr {
        Expr::ScalarFunction(scalar_func) => {
            // Check if this is a dynamic_table_check UDF
            if scalar_func.func.name() == "dynamic_table_check" && !scalar_func.args.is_empty() {
                // Extract the table name from the first argument
                if let Expr::Literal(ScalarValue::Utf8(Some(table_name)), ..) = &scalar_func.args[0]
                {
                    table_names.push(table_name.clone());
                }
            }

            // Recursively check arguments
            for arg in &scalar_func.args {
                find_dynamic_table_udfs_in_expr(arg, table_names)?;
            }
        }
        // Handle other expression types that can contain nested expressions
        Expr::BinaryExpr(binary) => {
            find_dynamic_table_udfs_in_expr(&binary.left, table_names)?;
            find_dynamic_table_udfs_in_expr(&binary.right, table_names)?;
        }
        Expr::Case(case) => {
            if let Some(expr) = &case.expr {
                find_dynamic_table_udfs_in_expr(expr, table_names)?;
            }
            for when_then in &case.when_then_expr {
                find_dynamic_table_udfs_in_expr(&when_then.0, table_names)?;
                find_dynamic_table_udfs_in_expr(&when_then.1, table_names)?;
            }
            if let Some(else_expr) = &case.else_expr {
                find_dynamic_table_udfs_in_expr(else_expr, table_names)?;
            }
        }
        Expr::Cast(cast) => {
            find_dynamic_table_udfs_in_expr(&cast.expr, table_names)?;
        }
        Expr::Not(not_expr) => {
            find_dynamic_table_udfs_in_expr(not_expr, table_names)?;
        }
        Expr::IsNull(is_null_expr) => {
            find_dynamic_table_udfs_in_expr(is_null_expr, table_names)?;
        }
        Expr::IsNotNull(is_not_null_expr) => {
            find_dynamic_table_udfs_in_expr(is_not_null_expr, table_names)?;
        }
        Expr::Negative(neg_expr) => {
            find_dynamic_table_udfs_in_expr(neg_expr, table_names)?;
        }
        Expr::InList(in_list) => {
            find_dynamic_table_udfs_in_expr(&in_list.expr, table_names)?;
            for list_expr in &in_list.list {
                find_dynamic_table_udfs_in_expr(list_expr, table_names)?;
            }
        }
        Expr::Between(between) => {
            find_dynamic_table_udfs_in_expr(&between.expr, table_names)?;
            find_dynamic_table_udfs_in_expr(&between.low, table_names)?;
            find_dynamic_table_udfs_in_expr(&between.high, table_names)?;
        }
        // For other expression types that don't contain nested expressions, do nothing
        _ => {}
    }
    Ok(())
}

/// Extract string values from an ArrayRef, supporting both StringArray and StringViewArray
pub fn extract_string_values(
    values: ArrayRef,
) -> std::result::Result<Vec<String>, DynamicTableBackendError> {
    // Try StringArray first
    if let Some(string_array) = values.as_any().downcast_ref::<StringArray>() {
        let values: Vec<String> = (0..string_array.len())
            .filter_map(|i| {
                if !string_array.is_null(i) {
                    Some(string_array.value(i).to_string())
                } else {
                    None
                }
            })
            .collect();
        return Ok(values);
    }

    // Try StringViewArray
    if let Some(string_view_array) = values.as_any().downcast_ref::<StringViewArray>() {
        let values: Vec<String> = (0..string_view_array.len())
            .filter_map(|i| {
                if !string_view_array.is_null(i) {
                    Some(string_view_array.value(i).to_string())
                } else {
                    None
                }
            })
            .collect();
        return Ok(values);
    }

    Err(DynamicTableBackendError::StringArrayExpected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::side_output::SourceSideOutput;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::DFSchema;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::{LogicalPlanBuilder, ScalarUDF, col, lit};
    use std::collections::HashMap;

    fn create_test_session_manager() -> SessionManager {
        SessionManager::new(1000, 10, DynamicTableRegistry::new())
            .expect("test session manager initialisation failed")
    }

    fn create_test_topology() -> PipelineTopology {
        let mut sources = HashMap::new();
        sources.insert(
            "test_source".to_string(),
            crate::topology::Source::kafka(crate::topology::KafkaSource {
                topic: "test_topic".to_string(),
                starting_offsets: None,
                include_metadata: None,
                filter: None,
                primary_key: None,
                telemetry: None,
                batch_size: None,
                batch_flush_interval: None,
                data_format: None,
                schema: None,
                validate_writer_schema_ordering: None,
                schema_id_overrides: None,
                skip_schema_resolution: None,
                skip_schema_resolution_for_reader_schema_ids: None,
            }),
        );

        let mut transforms = HashMap::new();
        transforms.insert(
            "test_dynamic_table".to_string(),
            Transform::dynamic_table(crate::topology::DynamicTableTransform {
                backend_type: streamling_config::DynamicTableBackendType::InMemory,
                backend_entity_name: "test_table".to_string(),
                sql: Some("SELECT * FROM test_source".to_string()),
                schema: None,
                column: None,
                time_column: None,
                cache_refresh_debounce_ms: None,
                telemetry: None,
            }),
        );

        transforms.insert(
            "test_dynamic_table_no_sql".to_string(),
            Transform::dynamic_table(crate::topology::DynamicTableTransform {
                backend_type: streamling_config::DynamicTableBackendType::InMemory,
                backend_entity_name: "test_table_no_sql".to_string(),
                sql: None,
                schema: None,
                column: None,
                time_column: None,
                cache_refresh_debounce_ms: None,
                telemetry: None,
            }),
        );

        transforms.insert(
            "different_source_table".to_string(),
            Transform::dynamic_table(crate::topology::DynamicTableTransform {
                backend_type: streamling_config::DynamicTableBackendType::InMemory,
                backend_entity_name: "different_table".to_string(),
                sql: Some("SELECT * FROM different_source".to_string()),
                schema: None,
                column: None,
                time_column: None,
                cache_refresh_debounce_ms: None,
                telemetry: None,
            }),
        );

        PipelineTopology {
            sources,
            transforms,
            sinks: HashMap::new(),
        }
    }

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_dt_func() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(
            crate::functions::dynamic_table::DynamicTableCheckFunc::new(DynamicTableRegistry::new()),
        ))
    }

    fn create_plan_with_dynamic_table_udf(table_name: &str) -> Result<LogicalPlan> {
        let schema = create_test_schema();
        let empty_table = Arc::new(EmptyTable::new(schema));

        // Create a plan with a dynamic_table_check UDF call
        LogicalPlanBuilder::scan("test_table", provider_as_source(empty_table), None)?
            .filter(col("id").eq(Expr::ScalarFunction(ScalarFunction {
                func: create_dt_func(),
                args: vec![lit(table_name), col("name")],
            })))?
            .build()
    }

    fn create_plan_without_dynamic_table_udf() -> Result<LogicalPlan> {
        let schema = create_test_schema();
        let empty_table = Arc::new(EmptyTable::new(schema));

        LogicalPlanBuilder::scan("test_table", provider_as_source(empty_table), None)?
            .filter(col("id").eq(lit(1)))?
            .build()
    }

    #[tokio::test]
    async fn test_validate_dynamic_table_usage_no_udfs() {
        let session_manager = create_test_session_manager();
        let topology = create_test_topology();
        let plan = create_plan_without_dynamic_table_udf().unwrap();

        let result = validate_dynamic_table_usage(
            "test_source".to_string(),
            &plan,
            &topology,
            &session_manager,
        )
        .await;

        assert!(
            result.is_ok(),
            "Should succeed when no dynamic table UDFs are used"
        );
    }

    #[tokio::test]
    async fn test_validate_dynamic_table_usage_valid_table() {
        let session_manager = create_test_session_manager();
        let topology = create_test_topology();

        // Register the test_source table in the session manager
        let schema = create_test_schema();
        let empty_table = Arc::new(EmptyTable::new(schema));
        session_manager
            .register_table("test_source", empty_table)
            .unwrap();

        let plan = create_plan_with_dynamic_table_udf("test_dynamic_table").unwrap();

        let result = validate_dynamic_table_usage(
            "test_source".to_string(),
            &plan,
            &topology,
            &session_manager,
        )
        .await;

        assert!(
            result.is_ok(),
            "Should succeed when dynamic table uses same source"
        );
    }

    #[tokio::test]
    async fn test_validate_dynamic_table_usage_undefined_table() {
        let session_manager = create_test_session_manager();
        let topology = create_test_topology();
        let plan = create_plan_with_dynamic_table_udf("undefined_table").unwrap();

        let result = validate_dynamic_table_usage(
            "test_source".to_string(),
            &plan,
            &topology,
            &session_manager,
        )
        .await;

        assert!(
            result.is_err(),
            "Should fail when dynamic table is not defined in topology"
        );
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("undefined_table"),
            "Error should mention the undefined table"
        );
        assert!(
            error_msg.contains("not defined in pipeline topology"),
            "Error should mention topology"
        );
    }

    #[tokio::test]
    async fn test_validate_dynamic_table_usage_different_source() {
        let session_manager = create_test_session_manager();
        let topology = create_test_topology();

        // Register both sources
        let schema = create_test_schema();
        let empty_table1 = Arc::new(EmptyTable::new(schema.clone()));
        let empty_table2 = Arc::new(EmptyTable::new(schema));
        session_manager
            .register_table("test_source", empty_table1)
            .unwrap();
        session_manager
            .register_table("different_source", empty_table2)
            .unwrap();

        let plan = create_plan_with_dynamic_table_udf("different_source_table").unwrap();

        let result = validate_dynamic_table_usage(
            "test_source".to_string(),
            &plan,
            &topology,
            &session_manager,
        )
        .await;

        assert!(
            result.is_err(),
            "Should fail when dynamic table uses different source"
        );
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("different_source"),
            "Error should mention the different source"
        );
        assert!(
            error_msg.contains("test_source"),
            "Error should mention the expected source"
        );
    }

    #[tokio::test]
    async fn test_validate_dynamic_table_usage_no_sql_defined() {
        let session_manager = create_test_session_manager();
        let topology = create_test_topology();
        let plan = create_plan_with_dynamic_table_udf("test_dynamic_table_no_sql").unwrap();

        let result = validate_dynamic_table_usage(
            "test_source".to_string(),
            &plan,
            &topology,
            &session_manager,
        )
        .await;

        assert!(
            result.is_ok(),
            "Should succeed when dynamic table has no SQL defined"
        );
    }

    #[test]
    fn test_find_dynamic_table_udfs_no_udfs() {
        let plan = create_plan_without_dynamic_table_udf().unwrap();
        let result = find_dynamic_table_udfs(&plan).unwrap();
        assert!(result.is_empty(), "Should find no dynamic table UDFs");
    }

    #[test]
    fn test_find_dynamic_table_udfs_with_udfs() {
        let plan = create_plan_with_dynamic_table_udf("test_table").unwrap();
        let result = find_dynamic_table_udfs(&plan).unwrap();
        assert_eq!(result.len(), 1, "Should find one dynamic table UDF");
        assert_eq!(result[0], "test_table", "Should extract correct table name");
    }

    #[test]
    fn test_find_dynamic_table_udfs_in_expr_scalar_function() {
        let mut table_names = Vec::new();

        // Create a scalar function expression with dynamic_table_check
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: create_dt_func(),
            args: vec![lit("test_table"), col("value")],
        });

        let result = find_dynamic_table_udfs_in_expr(&expr, &mut table_names);
        assert!(
            result.is_ok(),
            "Should successfully process scalar function"
        );
        assert_eq!(table_names.len(), 1, "Should find one table name");
        assert_eq!(
            table_names[0], "test_table",
            "Should extract correct table name"
        );
    }

    #[test]
    fn test_find_dynamic_table_udfs_in_expr_binary_expr() {
        let mut table_names = Vec::new();

        // Create a binary expression that contains a dynamic_table_check UDF
        let left_expr = Expr::ScalarFunction(ScalarFunction {
            func: create_dt_func(),
            args: vec![lit("left_table"), col("value")],
        });

        let right_expr = col("id");
        let binary_expr = left_expr.eq(right_expr);

        let result = find_dynamic_table_udfs_in_expr(&binary_expr, &mut table_names);
        assert!(
            result.is_ok(),
            "Should successfully process binary expression"
        );
        assert_eq!(
            table_names.len(),
            1,
            "Should find one table name in binary expression"
        );
        assert_eq!(
            table_names[0], "left_table",
            "Should extract table name from left side"
        );
    }

    #[test]
    fn test_find_dynamic_table_udfs_in_expr_nested_expressions() {
        let mut table_names = Vec::new();

        // Create nested expressions with multiple dynamic_table_check UDFs
        let udf1 = Expr::ScalarFunction(ScalarFunction {
            func: create_dt_func(),
            args: vec![lit("table1"), col("value1")],
        });

        let udf2 = Expr::ScalarFunction(ScalarFunction {
            func: create_dt_func(),
            args: vec![lit("table2"), col("value2")],
        });

        let combined_expr = udf1.and(udf2);

        let result = find_dynamic_table_udfs_in_expr(&combined_expr, &mut table_names);
        assert!(
            result.is_ok(),
            "Should successfully process nested expressions"
        );
        assert_eq!(
            table_names.len(),
            2,
            "Should find two table names in nested expressions"
        );
        assert!(
            table_names.contains(&"table1".to_string()),
            "Should find table1"
        );
        assert!(
            table_names.contains(&"table2".to_string()),
            "Should find table2"
        );
    }

    #[tokio::test]
    async fn test_extract_expressions_from_sql_query_simple_select() {
        let session_manager = create_test_session_manager();

        // Register a test table
        let schema = create_test_schema();
        let empty_table = Arc::new(EmptyTable::new(schema));
        session_manager
            .register_table("test_table", empty_table)
            .unwrap();

        let sql = "SELECT * FROM test_table";
        let plan = session_manager
            .create_logical_plan(String::from(sql))
            .await
            .unwrap();
        let result = extract_expressions_from_sql_query(&plan);

        assert!(result.is_ok(), "Should successfully parse simple SELECT *");
        let (projection_exprs, filter_expr) = result.unwrap();

        // For SELECT *, we expect wildcard expressions for each column
        assert!(
            !projection_exprs.is_empty(),
            "Should have projection expressions for SELECT *"
        );
        assert!(filter_expr.is_none(), "Should have no filter expression");
    }

    #[tokio::test]
    async fn test_extract_expressions_from_sql_query_projection() {
        let session_manager = create_test_session_manager();

        // Register a test table
        let schema = create_test_schema();
        let empty_table = Arc::new(EmptyTable::new(schema));
        session_manager
            .register_table("test_table", empty_table)
            .unwrap();

        let sql = "SELECT id, name FROM test_table";
        let plan = session_manager
            .create_logical_plan(String::from(sql))
            .await
            .unwrap();
        let result = extract_expressions_from_sql_query(&plan);

        assert!(result.is_ok(), "Should successfully parse projection query");
        let (projection_exprs, filter_expr) = result.unwrap();

        assert_eq!(
            projection_exprs.len(),
            2,
            "Should have 2 projection expressions"
        );
        assert!(filter_expr.is_none(), "Should have no filter expression");

        // Verify the expressions are column references
        match &projection_exprs[0] {
            Expr::Column(_) => (),
            _ => panic!("First expression should be a column reference"),
        }
        match &projection_exprs[1] {
            Expr::Column(_) => (),
            _ => panic!("Second expression should be a column reference"),
        }
    }

    #[tokio::test]
    async fn test_extract_expressions_from_sql_query_filter_only() {
        let session_manager = create_test_session_manager();

        // Register a test table
        let schema = create_test_schema();
        let empty_table = Arc::new(EmptyTable::new(schema));
        session_manager
            .register_table("test_table", empty_table)
            .unwrap();

        let sql = "SELECT * FROM test_table WHERE id > 10";
        let plan = session_manager
            .create_logical_plan(String::from(sql))
            .await
            .unwrap();
        let result = extract_expressions_from_sql_query(&plan);

        assert!(
            result.is_ok(),
            "Should successfully parse query with WHERE clause"
        );
        let (projection_exprs, filter_expr) = result.unwrap();

        assert!(
            !projection_exprs.is_empty(),
            "Should have projection expressions"
        );
        assert!(filter_expr.is_some(), "Should have filter expression");

        // Verify the filter expression is a binary expression
        let filter = filter_expr.unwrap();
        match filter {
            Expr::BinaryExpr(_) => (),
            _ => panic!("Filter expression should be a binary expression"),
        }
    }

    #[tokio::test]
    async fn test_extract_expressions_from_sql_query_projection_and_filter() {
        let session_manager = create_test_session_manager();

        // Register a test table
        let schema = create_test_schema();
        let empty_table = Arc::new(EmptyTable::new(schema));
        session_manager
            .register_table("test_table", empty_table)
            .unwrap();

        let sql = "SELECT id, name FROM test_table WHERE id > 10";
        let plan = session_manager
            .create_logical_plan(String::from(sql))
            .await
            .unwrap();
        let result = extract_expressions_from_sql_query(&plan);

        assert!(
            result.is_ok(),
            "Should successfully parse projection with WHERE clause"
        );
        let (projection_exprs, filter_expr) = result.unwrap();

        assert_eq!(
            projection_exprs.len(),
            2,
            "Should have 2 projection expressions"
        );
        assert!(filter_expr.is_some(), "Should have filter expression");

        // Verify the projection expressions are column references
        match &projection_exprs[0] {
            Expr::Column(_) => (),
            _ => panic!("First expression should be a column reference"),
        }
        match &projection_exprs[1] {
            Expr::Column(_) => (),
            _ => panic!("Second expression should be a column reference"),
        }

        // Verify the filter expression is a binary expression
        let filter = filter_expr.unwrap();
        match filter {
            Expr::BinaryExpr(_) => (),
            _ => panic!("Filter expression should be a binary expression"),
        }
    }

    #[tokio::test]
    async fn test_extract_expressions_from_sql_query_unsupported_query() {
        let session_manager = create_test_session_manager();

        // Register a test table
        let schema = create_test_schema();
        let empty_table = Arc::new(EmptyTable::new(schema));
        session_manager
            .register_table("test_table", empty_table)
            .unwrap();

        // Test an unsupported query type (GROUP BY)
        let sql = "SELECT COUNT(*) FROM test_table GROUP BY id";
        let plan = session_manager
            .create_logical_plan(String::from(sql))
            .await
            .unwrap();
        let result = extract_expressions_from_sql_query(&plan);

        assert!(result.is_err(), "Should fail for unsupported query types");
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("only Projection and Filter SQL queries are supported"),
            "Error should mention unsupported query type, got: {error_msg}"
        );
    }

    #[tokio::test]
    async fn test_dynamic_table_side_output_basic() {
        let backend = Arc::new(InMemoryDynamicTableBackend::new(
            "test_table".to_string(),
            None,
            None,
            None,
            1000, // max_batch_size
        ));
        let session_manager = create_test_session_manager();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));

        let df_schema = DFSchema::try_from(schema.clone()).unwrap();

        // Create a projection expression for the "name" column
        let projection_exprs = vec![col("name")];

        let side_output = DynamicTableSideOutput::new(
            backend.clone(),
            projection_exprs,
            None, // No filter
            &session_manager.session_state(),
            &df_schema,
        )
        .unwrap();

        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let id_array = datafusion::arrow::array::Int64Array::from(vec![1, 2, 3]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(name_array), Arc::new(id_array)]).unwrap();

        let result = side_output.process(&batch).await;
        assert!(result.is_ok(), "Processing should succeed");

        // Verify that the backend received the data
        let test_values = StringArray::from(vec!["Alice"]);
        let contains_result = backend.contains(Arc::new(test_values)).await.unwrap();
        let boolean_array = contains_result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(boolean_array.value(0), "Backend should contain 'Alice'");
    }

    #[tokio::test]
    async fn test_dynamic_table_side_output_with_filter() {
        let backend = Arc::new(InMemoryDynamicTableBackend::new(
            "test_table".to_string(),
            None,
            None,
            None,
            1000, // max_batch_size
        ));
        let session_manager = create_test_session_manager();

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]));

        let df_schema = DFSchema::try_from(schema.clone()).unwrap();

        // Create projection and filter expressions
        let projection_exprs = vec![col("name")];
        let filter_expr = Some(col("id").gt(lit(1))); // Filter where id > 1

        let side_output = DynamicTableSideOutput::new(
            backend.clone(),
            projection_exprs,
            filter_expr,
            &session_manager.session_state(),
            &df_schema,
        )
        .unwrap();

        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let id_array = datafusion::arrow::array::Int64Array::from(vec![1, 2, 3]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(name_array), Arc::new(id_array)]).unwrap();

        let result = side_output.process(&batch).await;
        assert!(result.is_ok(), "Processing with filter should succeed");

        // Verify that only filtered data was added (Bob and Charlie, not Alice)
        let test_values_alice = StringArray::from(vec!["Alice"]);
        let contains_alice = backend.contains(Arc::new(test_values_alice)).await.unwrap();
        let boolean_array_alice = contains_alice
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(
            !boolean_array_alice.value(0),
            "Backend should not contain 'Alice' due to filter"
        );

        let test_values_bob = StringArray::from(vec!["Bob"]);
        let contains_bob = backend.contains(Arc::new(test_values_bob)).await.unwrap();
        let boolean_array_bob = contains_bob
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(boolean_array_bob.value(0), "Backend should contain 'Bob'");
    }
}
