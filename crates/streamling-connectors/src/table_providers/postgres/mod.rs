mod batch_processor;
mod columns;
mod execution;
mod projection;
pub mod query_builder;
mod schema_extraction;
mod type_mapping;
mod value_binding;

// Re-export main types
pub use query_builder::PostgresQueryBuilder;

// Main implementations
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use batch_processor::{BatchProcessorContext, process_batch};
use columns::ColumnInfo;
use datafusion::catalog::Session;
use datafusion::common::{Result, not_impl_err};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use projection::build_projection_for_postgres;
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use streamling_config::PostgresSinkConfig;
use streamling_core::data::COLUMN_NAME_OP;
use streamling_core::error::ResultExt;
use streamling_core::node_context::get_node_context;
use streamling_core::operators::wrapping::WrappingDataSink;
use streamling_core::telemetry::recorder::get_metrics_recorder;
use streamling_core::topology::Telemetry;
use streamling_core::utils::parse_primary_key_columns;
use streamling_core::utils::pg::{PostgresConnection, create_schema_and_table_if_needed};
use tracing::{debug, info};

/// TableProvider for PostgreSQL sink
#[derive(Debug)]
pub struct PostgresSinkTableProvider {
    metric_metadata_id: String,
    schema: SchemaRef,
    config: PostgresSinkConfig,
    table: String,
    schema_name: String,
    num_records_before_stop: Option<u64>,
    source_name: String,
    reference_name: String,
    primary_key: Option<String>,
    on_conflict: String,
    update_where: Option<BTreeMap<String, String>>,
    append_only_mode: bool,
    checkpoint_truncation: bool,
    parallelism: usize,
    telemetry: Option<Telemetry>,
    write_batch_size: u32,
}

impl PostgresSinkTableProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metric_metadata_id: String,
        schema: SchemaRef,
        config: PostgresSinkConfig,
        table: String,
        schema_name: String,
        batch_size: Option<u32>,
        num_records_before_stop: Option<u64>,
        source_name: String,
        primary_key: Option<String>,
        on_conflict: String,
        update_where: Option<BTreeMap<String, String>>,
        append_only_mode: bool,
        checkpoint_truncation: bool,
        reference_name: String,
        parallelism: Option<usize>,
        telemetry: Option<Telemetry>,
    ) -> Self {
        let batch_size = batch_size.unwrap_or(config.batch_size);
        let parallelism = parallelism.unwrap_or(1).max(1);
        let write_batch_size = batch_size.div_ceil(parallelism as u32).max(1);

        Self {
            metric_metadata_id,
            schema,
            config,
            table,
            schema_name,
            num_records_before_stop,
            source_name,
            reference_name,
            primary_key,
            on_conflict,
            update_where,
            append_only_mode,
            checkpoint_truncation,
            parallelism,
            telemetry,
            write_batch_size,
        }
    }
}

#[async_trait]
impl TableProvider for PostgresSinkTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Reading is not implemented for PostgresSinkTableProvider")
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let adjusted_input = build_projection_for_postgres(state, input.clone(), &input_schema)?;

        let postgres_sink = Arc::new(PostgresSinkExec::new(
            self.config.clone(),
            adjusted_input.schema(), // Transformed schema
            input_schema.clone(),    // Original schema for type detection
            self.schema_name.clone(),
            self.table.clone(),
            self.num_records_before_stop,
            self.source_name.clone(),
            self.primary_key.clone(),
            self.metric_metadata_id.clone(),
            self.on_conflict.clone(),
            self.update_where.clone(),
            self.append_only_mode,
            self.checkpoint_truncation,
            self.reference_name.clone(),
            self.parallelism,
            self.write_batch_size,
        ));

        let wrapper_sink = Arc::new(WrappingDataSink::new(
            postgres_sink,
            self.metric_metadata_id.clone(),
            self.primary_key.clone(),
            self.telemetry.as_ref(),
        ));

        Ok(Arc::new(DataSinkExec::new(
            adjusted_input,
            wrapper_sink,
            None,
        )))
    }
}

/// DataSink implementation for PostgreSQL
#[derive(Debug)]
pub struct PostgresSinkExec {
    config: PostgresSinkConfig,
    schema: SchemaRef, // Transformed schema (with Utf8 for U256/I256/nested types)
    original_schema: SchemaRef, // Original schema (for determining SQL casts)
    schema_name: String,
    table: String,
    num_records_before_stop: Option<u64>,
    source_name: String,
    reference_name: String,
    primary_key: Option<String>,
    metric_metadata_id: String,
    on_conflict: String,
    update_where: Option<BTreeMap<String, String>>,
    append_only_mode: bool,
    checkpoint_truncation: bool,
    parallelism: usize,
    write_batch_size: u32,
}

impl PostgresSinkExec {
    pub fn new(
        config: PostgresSinkConfig,
        schema: SchemaRef,          // Transformed schema
        original_schema: SchemaRef, // Original schema for type detection
        schema_name: String,
        table: String,
        num_records_before_stop: Option<u64>,
        source_name: String,
        primary_key: Option<String>,
        metric_metadata_id: String,
        on_conflict: String,
        update_where: Option<BTreeMap<String, String>>,
        append_only_mode: bool,
        checkpoint_truncation: bool,
        reference_name: String,
        parallelism: usize,
        write_batch_size: u32,
    ) -> Self {
        Self {
            config,
            schema,
            original_schema,
            schema_name,
            table,
            num_records_before_stop,
            source_name,
            reference_name,
            primary_key,
            metric_metadata_id,
            on_conflict,
            update_where,
            append_only_mode,
            checkpoint_truncation,
            parallelism,
            write_batch_size,
        }
    }
}

#[async_trait]
impl DataSink for PostgresSinkExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let node_label = get_node_context(&self.reference_name)
            .map(|ctx| ctx.format())
            .unwrap_or_else(|| format!("postgres sink '{}'", self.reference_name));

        debug!(
            "[{}] starting write_all for table '{}.{}' (parallelism={})",
            node_label, self.schema_name, self.table, self.parallelism
        );

        // Create connection pool with enough connections for parallel tasks
        let connection = PostgresConnection::new_with_parallelism(&self.config, self.parallelism)
            .await
            .streamling_with_context(|| format!("{}: failed to create connection", node_label))?;

        // Create schema and table if needed (use original schema for correct column types)
        create_schema_and_table_if_needed(
            connection.pool(),
            &self.schema_name,
            &self.table,
            &self.original_schema,
            self.primary_key.as_ref(),
            self.append_only_mode,
            self.checkpoint_truncation,
        )
        .await
        .streamling_with_context(|| {
            format!(
                "{}: failed to create schema/table '{}.{}'",
                node_label, self.schema_name, self.table
            )
        })?;

        // Get primary key columns
        let mut primary_key_columns: Vec<String> = if let Some(pk_str) = &self.primary_key {
            parse_primary_key_columns(pk_str)
                .iter()
                .map(|s| s.to_string())
                .collect()
        } else {
            Vec::new()
        };

        // For append-only mode, add _gs_op to primary key for composite key
        if self.append_only_mode {
            primary_key_columns.push(COLUMN_NAME_OP.to_string());
        }

        // Get column names and indices (conditionally filter out _gs_op based on append_only_mode)
        let column_info = ColumnInfo::from_schema(&self.schema, self.append_only_mode);

        // Get primary key indices in the schema (before filtering)
        let primary_key_indices: Vec<usize> = primary_key_columns
            .iter()
            .filter_map(|pk_col| self.schema.index_of(pk_col).ok())
            .collect();

        // Create batch processor context
        let processor_context = BatchProcessorContext {
            pool: Arc::new(tokio::sync::Mutex::new((connection.pool().clone(), 0u64))),
            config: self.config.clone(),
            schema_name: self.schema_name.clone(),
            table: self.table.clone(),
            primary_key_columns: primary_key_columns.clone(),
            primary_key_indices,
            column_names: column_info.names.clone(),
            column_indices: column_info.indices.clone(),
            source_name: self.source_name.clone(),
            node_label: node_label.clone(),
            records_processed: Arc::new(Mutex::new(0u64)),
            num_records_before_stop: self.num_records_before_stop,
            metrics_recorder: get_metrics_recorder().clone(),
            metric_metadata_id: self.metric_metadata_id.clone(),
            original_schema: self.original_schema.clone(),
            on_conflict: self.on_conflict.clone(),
            update_where: self.update_where.clone(),
            append_only_mode: self.append_only_mode,
            checkpoint_truncation: self.checkpoint_truncation,
            current_checkpoint_epoch: Arc::new(Mutex::new(0)),
            parallelism: self.parallelism,
            write_batch_size: self.write_batch_size,
        };

        // Process each batch directly — accumulation is handled by RebatchExec
        let mut data = data;
        let mut row_count: usize = 0;
        while let Some(batch_result) = data.next().await {
            let batch = batch_result?;
            let num_rows = batch.num_rows();
            let should_continue = process_batch(&processor_context, batch).await?;
            row_count += num_rows;
            if !should_continue {
                break;
            }
        }

        info!(
            "[{}] write_all completed with {} rows (table '{}.{}')",
            node_label, row_count, self.schema_name, self.table
        );

        Ok(row_count as u64)
    }
}

impl DisplayAs for PostgresSinkExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PostgresSink")
    }
}
