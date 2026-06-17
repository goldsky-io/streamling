use arrow_schema::{ArrowError, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};
use streamling_core::error::{ResultExt, StreamlingError};
use streamling_core::functions::byte_reverse::ReverseBytes32Func;
use streamling_core::streamling_err;
use streamling_core::types::{i256::I256Type, u256::U256Type};
use streamling_core::utils::parse_primary_key_columns;

use crate::util::parallel::parallel_execute;

use async_stream;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use futures::Stream;
use futures::StreamExt;
use futures::executor::block_on;
use reqwest;
use streamling_core::checkpoints::channels::{send, subscribe_with_id, unsubscribe};
use streamling_core::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
    extract_checkpoint_messages, now_ms, process_checkpoint_acks,
};
use streamling_core::utils::batch::enrich_batch_with_metadata;

use bytes::Bytes;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CaseExpr, binary, col, lit};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::{Error, Response};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::pin::Pin;
use std::time::{Duration, Instant};
pub use streamling_config::{
    ClickHouseCompression, ClickHouseConfig, ClickHouseSinkConfig, ClickHouseSourceConfig,
    GzipCompressionLevel,
};
use streamling_core::data::{COLUMN_NAME_OP, RowKind};
use streamling_core::node_context::get_node_context;
use streamling_core::operators::wrapping::WrappingDataSink;
use streamling_core::retry::retry_forever_with_backoff_async;
use streamling_core::telemetry::provider::get_reference_name_from_metric_key;
use streamling_core::telemetry::recorder::get_metrics_recorder;
use streamling_core::topology::Telemetry;
use streamling_state::{StateBackendError, StateKey, StateOperatorBackend};
use tokio::sync::watch;
use tracing::{debug, error, info, trace, warn};

// Matches ClickHouse exception markers in HTTP response bodies.
// Covers DB::Exception, DB::ErrnoException, DB::NetException, DB::ParsingException,
// and Poco::Exception — all of which can appear in 200 OK bodies on certain failures.
static CLICKHOUSE_ERROR_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"Code: \d+\. \w+::\w*Exception:").unwrap());

mod query_builder;
use query_builder::{ClickHousePaginationConfig, ClickHouseQueryBuilder};

fn scalar_to_i128(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(*v as i128),
        ScalarValue::Int16(Some(v)) => Some(*v as i128),
        ScalarValue::Int32(Some(v)) => Some(*v as i128),
        ScalarValue::Int64(Some(v)) => Some(*v as i128),
        ScalarValue::UInt8(Some(v)) => Some(*v as i128),
        ScalarValue::UInt16(Some(v)) => Some(*v as i128),
        ScalarValue::UInt32(Some(v)) => Some(*v as i128),
        ScalarValue::UInt64(Some(v)) => Some(*v as i128),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => v.parse::<i128>().ok(),
        _ => None,
    }
}

fn i128_to_scalar_like(value: i128, template: &ScalarValue) -> ScalarValue {
    match template {
        ScalarValue::Int8(_) => ScalarValue::Int8(Some(value as i8)),
        ScalarValue::Int16(_) => ScalarValue::Int16(Some(value as i16)),
        ScalarValue::Int32(_) => ScalarValue::Int32(Some(value as i32)),
        ScalarValue::Int64(_) => ScalarValue::Int64(Some(value as i64)),
        ScalarValue::UInt8(_) => ScalarValue::UInt8(Some(value as u8)),
        ScalarValue::UInt16(_) => ScalarValue::UInt16(Some(value as u16)),
        ScalarValue::UInt32(_) => ScalarValue::UInt32(Some(value as u32)),
        ScalarValue::UInt64(_) => ScalarValue::UInt64(Some(value as u64)),
        other => panic!(
            "unsupported scalar type for block range arithmetic: {:?}",
            other
        ),
    }
}

fn is_timeout_error(error: &DataFusionError) -> bool {
    let error_msg = error.to_string().to_lowercase();
    error_msg.contains("timeout") || error_msg.contains("timed out")
}

#[derive(Clone, Debug)]
pub struct ClickHouseTableProvider {
    reference_name: String,
    pub schema: SchemaRef,
    client: ClickHouseClient,
    source_params: Option<SourceParams>,
    sink_params: Option<SinkParams>,
    metric_metadata_id: String,
}

#[derive(Clone, Debug)]
struct SourceParams {
    query_builder: ClickHouseQueryBuilder,
    sorting_keys: Vec<String>,
    initial_split_args: Vec<ScalarValue>,
    state_store: Arc<ClickHouseSourceStateStore>,
    datafusion_buffer_size: usize,
    block_range: i64,
    table_name: String,
    has_persisted_split: bool,
}

#[derive(Clone, Debug)]
struct SinkParams {
    table_name: String,
    source_name: String,
    reference_name: String,
    write_batch_size: u32,
    num_records_before_stop: Option<u64>,
    primary_keys: Vec<String>,
    parallelism: usize,
    append_only_mode: bool,
    version_column_name: Option<String>,
    schema_override: Option<std::collections::HashMap<String, String>>,
    telemetry: Option<Telemetry>,
}

impl ClickHouseTableProvider {
    const DEFAULT_BLOCK_RANGE: i64 = 1_000_000;
    const DEFAULT_PAGE_SIZE: usize = 10_000_000;
    const MIN_BLOCK_RANGE: i64 = 100;
    const SOURCE_QUERY_TIMEOUT_SECS: u64 = 60;

    pub fn new_source(
        reference_name: String,
        metric_metadata_id: String,
        table_name: &str,
        config: ClickHouseSourceConfig,
        start_at: Option<Vec<ScalarValue>>,
        filter: Option<String>,
        columns: Option<Vec<String>>,
        state_backend: Arc<dyn StateOperatorBackend<ClickHouseSourceSplit>>,
        datafusion_buffer_size: usize,
    ) -> Result<Self, DataFusionError> {
        let database_name = config.connection.database.clone();
        let page_size = config.page_size.unwrap_or(Self::DEFAULT_PAGE_SIZE);
        let block_range_config = config.block_range;
        let client = ClickHouseClient::new(config.connection.clone());

        let columns_copy = columns.clone();
        let schema = client
            .fetch_schema(table_name, columns)
            .streamling_with_context(|| {
                format!(
                    "failed to fetch schema from ClickHouse for table '{}'",
                    table_name
                )
            })?;
        let sorting_keys = client
            .fetch_sorting_keys(&database_name, table_name)
            .streamling_with_context(|| {
                format!(
                    "failed to fetch sorting keys from ClickHouse for table {}.{}",
                    database_name, table_name
                )
            })?;
        if sorting_keys.is_empty() {
            return Err(streamling_err!(
                "no sorting keys found for ClickHouse table {}.{}",
                database_name,
                table_name
            )
            .into());
        }

        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: sorting_keys.clone(),
            page_size,
        };

        // Require a wide numeric first sorting key for block range pagination.
        // Narrow types (Int8, UInt8, Int16, UInt16) would silently overflow in
        // i128_to_scalar_like when computing block range boundaries.
        let first_key_name = sorting_keys.first().unwrap();
        let first_key_type = schema
            .field_with_name(first_key_name)
            .ok()
            .map(|f| f.data_type().clone());

        let is_wide_integer = matches!(
            first_key_type.as_ref(),
            Some(arrow::datatypes::DataType::Int32)
                | Some(arrow::datatypes::DataType::Int64)
                | Some(arrow::datatypes::DataType::UInt32)
                | Some(arrow::datatypes::DataType::UInt64)
        );

        if !is_wide_integer {
            return Err(streamling_err!(
                "ClickHouse source requires a 32-bit or wider integer first sorting key for block range \
                 pagination, but '{}' on table {}.{} has type {:?}",
                first_key_name, database_name, table_name, first_key_type
            )
            .into());
        }

        let block_range = block_range_config
            .map(|br| br.max(Self::MIN_BLOCK_RANGE))
            .unwrap_or(Self::DEFAULT_BLOCK_RANGE);

        info!(
            "[{}] block range pagination configured (block_range={}, page_size={})",
            reference_name, block_range, page_size
        );

        let mut query_builder = ClickHouseQueryBuilder::of(
            table_name.to_string(),
            columns_copy.unwrap_or(vec!["*".to_string()]),
            filter.clone(),
            Some(pagination_config),
        );
        let state_store = Arc::new(ClickHouseSourceStateStore {
            reference_name: reference_name.clone(),
            state_backend,
        });
        let (initial_split_args, has_persisted_split) = match start_at {
            Some(start_at) => {
                info!(
                    "Starting ClickHouseTableProvider with user-provided start_at: {:?}",
                    start_at
                );
                query_builder.start_at_page(start_at.clone());
                (start_at, false)
            }
            None => {
                if let Some(split) = block_on(state_store.load_split()) {
                    info!(
                        "Starting ClickHouseTableProvider with saved split: {:?}",
                        split
                    );
                    // Only set the keyset if args are non-empty. An empty args slice
                    // means the checkpoint was saved before any data was processed
                    // (the finalizer raced ahead of the first batch). Calling
                    // start_at_page(vec![]) would set current_keyset = Some(vec![]),
                    // which produces `AND ()` in the next query.
                    if !split.args.is_empty() {
                        query_builder.start_at_page(split.args.clone());
                    }
                    (split.args, true)
                } else {
                    info!("Starting ClickHouseTableProvider from the beginning (no saved split)");
                    (Vec::new(), false)
                }
            }
        };

        let source_params = SourceParams {
            query_builder: query_builder.clone(),
            sorting_keys,
            initial_split_args,
            state_store,
            datafusion_buffer_size,
            block_range,
            table_name: table_name.to_string(),
            has_persisted_split,
        };
        Ok(ClickHouseTableProvider {
            reference_name: reference_name.clone(),
            schema,
            client,
            source_params: Some(source_params),
            sink_params: None,
            metric_metadata_id,
        })
    }

    /// Creates a projection expression for a single field, applying schema override if needed
    fn create_field_projection(
        field: &arrow::datatypes::Field,
        field_idx: usize,
        input_schema: &SchemaRef,
        datetime_columns: &std::collections::HashSet<String>,
        schema_override: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<(Arc<dyn PhysicalExpr>, String)> {
        // Check if this column needs DateTime conversion
        if datetime_columns.contains(field.name()) {
            // Validate and create DateTime conversion
            if let Some(overrides) = schema_override
                && let Some(override_type) = overrides.get(field.name())
            {
                schema_overrides::validate_conversion(field, override_type)?;
            }
            let converted_expr =
                schema_overrides::create_datetime_conversion(field, field_idx, input_schema)?;
            info!(
                "Applying DateTime conversion for column '{}' ({:?} -> Timestamp)",
                field.name(),
                field.data_type()
            );
            Ok((converted_expr, field.name().clone()))
        } else {
            // No conversion needed - use column as-is
            let col_expr = col(field.name(), input_schema).streamling_with_context(|| {
                format!("failed to create column expression for '{}'", field.name())
            })?;
            Ok((col_expr, field.name().clone()))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_sink(
        metric_metadata_id: String,
        table_name: &str,
        config: ClickHouseSinkConfig,
        batch_size: u32,
        num_records_before_stop: Option<u64>,
        primary_key: String,
        source_name: String,
        parallelism: Option<usize>,
        append_only_mode: Option<bool>,
        version_column_name: Option<String>,
        schema_override: Option<std::collections::HashMap<String, String>>,
        compression_override: Option<ClickHouseCompression>,
        compression_level_override: Option<GzipCompressionLevel>,
        reference_name: String,
        telemetry: Option<Telemetry>,
    ) -> Result<Self, DataFusionError> {
        let compression = compression_override.unwrap_or(config.compression);
        let compression_level = compression_level_override.unwrap_or(config.compression_level);
        let client =
            ClickHouseClient::with_compression(config.clone(), compression, compression_level);

        let primary_keys: Vec<String> = parse_primary_key_columns(&primary_key)
            .iter()
            .map(|s| s.to_string())
            .collect();

        if primary_keys.is_empty() {
            return Err(streamling_err!("primary key is required for ClickHouse sink").into());
        }

        let empty_schema = Arc::new(arrow::datatypes::Schema::empty());

        let parallelism = parallelism.unwrap_or(1).max(1);
        let write_batch_size = batch_size.div_ceil(parallelism as u32).max(1);

        let sink_params = SinkParams {
            table_name: table_name.to_string(),
            source_name,
            reference_name: reference_name.clone(),
            write_batch_size,
            num_records_before_stop,
            primary_keys,
            parallelism,
            append_only_mode: append_only_mode.unwrap_or(true),
            version_column_name,
            schema_override,
            telemetry,
        };
        Ok(ClickHouseTableProvider {
            reference_name: reference_name.clone(),
            schema: empty_schema,
            client,
            source_params: None,
            sink_params: Some(sink_params),
            metric_metadata_id,
        })
    }

    /// Get the primary key extracted from the ClickHouse sorting keys, if available
    pub fn get_extracted_primary_key(&self) -> Option<String> {
        self.source_params.as_ref().and_then(|params| {
            if params.sorting_keys.is_empty() {
                None
            } else {
                Some(params.sorting_keys.join(","))
            }
        })
    }

    /// True iff this provider was constructed as a source, had no
    /// user-supplied `start_at`, AND the source state backend had a
    /// persisted split at construction time. This is the signal the
    /// hybrid restore protocol uses to decide whether to recover to this
    /// phase: a user-supplied `start_at` masks any saved split (the
    /// provider will scan from `start_at`, not from the cursor), so the
    /// phase has effectively been reset and should not be treated as
    /// "has resumable progress" for hybrid recovery purposes. Sink-only
    /// providers and sources with no saved split also return false.
    pub fn has_persisted_source_state(&self) -> bool {
        self.source_params
            .as_ref()
            .is_some_and(|params| params.has_persisted_split)
    }
}

#[async_trait]
impl TableProvider for ClickHouseTableProvider {
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
        let source_params = self
            .source_params
            .as_ref()
            .ok_or_else(|| streamling_err!("ClickHouseTableProvider is not a source"))?;

        let clickhouse_source_exec = Arc::new(ClickHouseSourceExec {
            cached_properties: PlanProperties::new(
                EquivalenceProperties::new(self.schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            provider: (*self).clone(),
            split: ClickHouseSourceSplit {
                sorting_keys: source_params.sorting_keys.clone(),
                args: source_params.initial_split_args.clone(),
            },
        });
        Ok(clickhouse_source_exec)
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sink_params = self
            .sink_params
            .as_ref()
            .ok_or_else(|| streamling_err!("ClickHouseTableProvider is not a sink"))?;

        let input_schema = input.schema();

        // Get DateTime columns from schema override (if any)
        let datetime_columns = if let Some(ref overrides) = sink_params.schema_override {
            schema_overrides::get_datetime_columns(overrides)
        } else {
            std::collections::HashSet::new()
        };

        const IS_DELETED: &str = "is_deleted";
        let append_only_mode = sink_params.append_only_mode;

        let projection_exprs: Result<Vec<(Arc<dyn PhysicalExpr>, String)>> = if append_only_mode {
            // append_only_mode=true: all columns except _gs_op and is_deleted, plus computed is_deleted
            input_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| f.name() != COLUMN_NAME_OP && f.name() != IS_DELETED)
                .map(|(idx, f)| {
                    Self::create_field_projection(
                        f,
                        idx,
                        &input_schema,
                        &datetime_columns,
                        sink_params.schema_override.as_ref(),
                    )
                })
                .chain(std::iter::once({
                    // Add IS_DELETED: CASE WHEN _gs_op = 'd' THEN 1 ELSE 0 END
                    let op_col =
                        col(COLUMN_NAME_OP, &input_schema).streamling_with_context(|| {
                            format!(
                                "failed to find required column '{}' in input schema",
                                COLUMN_NAME_OP
                            )
                        })?;
                    let when_expr = binary(
                        op_col,
                        Operator::Eq,
                        lit(ScalarValue::Utf8(Some("d".to_string()))),
                        &input_schema,
                    )?;
                    let case_expr = CaseExpr::try_new(
                        None,
                        vec![(when_expr, lit(ScalarValue::UInt8(Some(1))))],
                        Some(lit(ScalarValue::UInt8(Some(0)))),
                    )?;
                    Ok((
                        Arc::new(case_expr) as Arc<dyn PhysicalExpr>,
                        IS_DELETED.to_string(),
                    ))
                }))
                .collect()
        } else {
            // append_only_mode=false: keep all columns including _gs_op (no is_deleted)
            // _gs_op is kept in the batch so write_all can split inserts vs deletes;
            // it will be stripped before the actual INSERT to ClickHouse
            input_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| f.name() != IS_DELETED)
                .map(|(idx, f)| {
                    if f.name() == COLUMN_NAME_OP {
                        // Pass _gs_op through as-is
                        let col_expr =
                            col(COLUMN_NAME_OP, &input_schema).streamling_with_context(|| {
                                format!(
                                    "failed to create column expression for '{}'",
                                    COLUMN_NAME_OP
                                )
                            })?;
                        Ok((col_expr, COLUMN_NAME_OP.to_string()))
                    } else {
                        Self::create_field_projection(
                            f,
                            idx,
                            &input_schema,
                            &datetime_columns,
                            sink_params.schema_override.as_ref(),
                        )
                    }
                })
                .collect()
        };

        let projection_exec = Arc::new(ProjectionExec::try_new(projection_exprs?, input)?);

        let input_schema = projection_exec.schema();
        let schema = Arc::new(ClickHouseClient::normalize_schema_for_clickhouse(
            input_schema.as_ref(),
        ));

        let clickhouse_sink = Arc::new(ClickHouseSinkExec {
            client: self.client.clone(),
            table_name: sink_params.table_name.clone(),
            write_batch_size: sink_params.write_batch_size,
            num_records_before_stop: sink_params.num_records_before_stop,
            source_name: sink_params.source_name.clone(),
            reference_name: sink_params.reference_name.clone(),
            schema,
            primary_keys: Arc::new(sink_params.primary_keys.clone()),
            metric_metadata_id: self.metric_metadata_id.clone(),
            parallelism: sink_params.parallelism,
            append_only_mode: sink_params.append_only_mode,
            version_column_name: sink_params.version_column_name.clone(),
            schema_override: sink_params.schema_override.clone(),
        });
        let wrapper_sink = Arc::new(WrappingDataSink::new(
            clickhouse_sink,
            self.metric_metadata_id.clone(),
            Some(sink_params.primary_keys.join(",")),
            sink_params.telemetry.as_ref(),
        ));
        Ok(Arc::new(DataSinkExec::new(
            projection_exec,
            wrapper_sink,
            None,
        )))
    }
}

#[derive(Debug)]
pub struct ClickHouseSinkExec {
    client: ClickHouseClient,
    table_name: String,
    write_batch_size: u32,
    num_records_before_stop: Option<u64>,
    source_name: String,
    reference_name: String,
    schema: SchemaRef,
    primary_keys: Arc<Vec<String>>,
    metric_metadata_id: String,
    parallelism: usize,
    append_only_mode: bool,
    version_column_name: Option<String>,
    schema_override: Option<std::collections::HashMap<String, String>>,
}

#[async_trait]
impl DataSink for ClickHouseSinkExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let node_label = get_node_context(&self.reference_name)
            .map(|ctx| ctx.format())
            .unwrap_or_else(|| format!("clickhouse sink '{}'", self.reference_name));

        debug!(
            "[{}] starting write_all for table '{}'",
            node_label, self.table_name
        );
        // Use sink schema (normalized) for table creation
        self.client
            .create_table_if_not_exists(
                &self.table_name,
                &self.schema,
                (*self.primary_keys).clone(),
                self.append_only_mode,
                self.version_column_name.as_deref(),
                self.schema_override.as_ref(),
            )
            .await
            .streamling_with_context(|| {
                format!(
                    "{}: failed to create table '{}'",
                    node_label, self.table_name
                )
            })?;

        let client = self.client.clone();
        let table_name = self.table_name.clone();
        let num_records_before_stop = self.num_records_before_stop;
        let normalized_schema = self.schema.clone();
        let primary_keys = self.primary_keys.clone();
        let parallelism = self.parallelism.max(1);
        let write_batch_size = self.write_batch_size;
        let append_only_mode = self.append_only_mode;
        let source_name = self.source_name.clone();
        let metric_metadata_id = self.metric_metadata_id.clone();
        let metrics_recorder = get_metrics_recorder().clone();
        let mut row_count: usize = 0;
        let mut records_processed: u64 = 0;
        let mut data = data;

        while let Some(result) = data.next().await {
            let batch = result?;

            // Capture arrival time at the start for accurate marker arrival latency
            let arrival_time_ms = now_ms();
            let checkpoint_messages = extract_checkpoint_messages(batch.schema().metadata());
            trace!(
                "Extracted checkpoint messages from batch metadata: {:?}",
                checkpoint_messages
            );

            // Start timing ack latency from when checkpoint messages are received
            // (includes any INSERT/processing time before ack is sent)
            let ack_start = Instant::now();

            let sink_id = get_reference_name_from_metric_key(&metric_metadata_id);

            let num_rows = batch.num_rows();
            if num_rows == 0 {
                process_checkpoint_acks(
                    checkpoint_messages,
                    arrival_time_ms,
                    ack_start,
                    &metrics_recorder,
                    &metric_metadata_id,
                    &sink_id,
                );
                continue;
            }

            let start_at = Instant::now();

            let normalized_batch = match ClickHouseClient::normalize_batch_for_clickhouse(
                &batch,
                &normalized_schema,
            ) {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        "{}: failed to normalize batch for table '{}': {}",
                        node_label, table_name, e
                    );
                    return Err(e);
                }
            };
            drop(batch);

            // Deduplication is handled by WrappingDataSink before batches reach this sink.

            if append_only_mode {
                // append_only_mode=true: INSERT all rows directly
                parallel_execute(&normalized_batch, parallelism, write_batch_size as usize, {
                    let client_closure = client.clone();
                    let table_closure = table_name.clone();
                    let schema_closure = normalized_schema.clone();
                    let node_label_closure = node_label.clone();
                    move |slice: RecordBatch| {
                        let client = client_closure.clone();
                        let table_name = table_closure.clone();
                        let schema = schema_closure.clone();
                        let node_label = node_label_closure.clone();
                        async move {
                            let operation_name =
                                format!("{}: INSERT into '{}'", node_label, table_name);
                            retry_forever_with_backoff_async(
                                || async {
                                    client
                                        .send_arrow_batch(&table_name, &slice, &schema)
                                        .await
                                        .streamling_context("failed to send Arrow batch")
                                },
                                &operation_name,
                            )
                            .await;
                        }
                    }
                })
                .await;
            } else {
                // append_only_mode=false: split rows by _gs_op into inserts vs deletes
                use datafusion::arrow::array::StringArray;
                use std::str::FromStr;

                let op_column =
                    normalized_batch
                        .column_by_name(COLUMN_NAME_OP)
                        .ok_or_else(|| {
                            DataFusionError::from(streamling_core::streamling_err!(
                                "missing required column '{}' in ClickHouse sink batch",
                                COLUMN_NAME_OP
                            ))
                        })?;
                let op_array = op_column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::from(streamling_core::streamling_err!(
                            "column '{}' must be StringArray, got {:?}",
                            COLUMN_NAME_OP,
                            op_column.data_type()
                        ))
                    })?;

                let mut insert_indices = Vec::new();
                let mut delete_indices = Vec::new();

                for (idx, op) in op_array.iter().enumerate() {
                    if let Some(op_str) = op {
                        let row_kind = RowKind::from_str(op_str).unwrap_or(RowKind::Insert);
                        match row_kind {
                            RowKind::Delete => delete_indices.push(idx as u32),
                            RowKind::Insert | RowKind::Update => insert_indices.push(idx as u32),
                        }
                    } else {
                        insert_indices.push(idx as u32);
                    }
                }

                // Process inserts/updates: strip _gs_op, then send via Arrow IPC
                if !insert_indices.is_empty() {
                    let indices_array = arrow::array::UInt32Array::from(insert_indices);
                    // Use streamling_core::utils::arrow::safe_take_record_batch to
                    // recover from the documented arrow take_bytes overflow panic on
                    // deeply nested schemas. Native take_record_batch panics with
                    // Option::expect("overflow") inside take_bytes for batches whose
                    // Utf8/Binary columns' cumulative byte offsets exceed i32::MAX;
                    // the panic crosses the extern "C" plugin boundary and crashes
                    // the process with exit 132/133 if not caught here.
                    let insert_batch = streamling_core::utils::arrow::safe_take_record_batch(
                        &normalized_batch,
                        &indices_array,
                    )?;
                    let insert_batch = ClickHouseClient::strip_gs_op_column(&insert_batch)?;
                    // Build schema without _gs_op for INSERTs
                    let insert_schema =
                        Arc::new(ClickHouseClient::normalize_schema_for_clickhouse(
                            insert_batch.schema().as_ref(),
                        ));

                    parallel_execute(&insert_batch, parallelism, write_batch_size as usize, {
                        let client_closure = client.clone();
                        let table_closure = table_name.clone();
                        let schema_closure = insert_schema;
                        let node_label_closure = node_label.clone();
                        move |slice: RecordBatch| {
                            let client = client_closure.clone();
                            let table_name = table_closure.clone();
                            let schema = schema_closure.clone();
                            let node_label = node_label_closure.clone();
                            async move {
                                let operation_name =
                                    format!("{}: INSERT into '{}'", node_label, table_name);
                                retry_forever_with_backoff_async(
                                    || async {
                                        client
                                            .send_arrow_batch(&table_name, &slice, &schema)
                                            .await
                                            .streamling_context("failed to send Arrow batch")
                                    },
                                    &operation_name,
                                )
                                .await;
                            }
                        }
                    })
                    .await;
                }

                // Process deletes: extract PK columns and issue ALTER TABLE DELETE
                if !delete_indices.is_empty() && primary_keys.is_empty() {
                    warn!(
                        "{}: dropping {} delete rows for table '{}' because no primary keys are configured",
                        node_label,
                        delete_indices.len(),
                        table_name
                    );
                }
                if !delete_indices.is_empty() && !primary_keys.is_empty() {
                    let indices_array = arrow::array::UInt32Array::from(delete_indices);
                    // See safe_take_record_batch comment on the insert path above.
                    let delete_batch = streamling_core::utils::arrow::safe_take_record_batch(
                        &normalized_batch,
                        &indices_array,
                    )?;

                    let operation_name = format!("{}: DELETE from '{}'", node_label, table_name);
                    let client_for_delete = client.clone();
                    let table_for_delete = table_name.clone();
                    let pks = primary_keys.clone();
                    retry_forever_with_backoff_async(
                        || {
                            let client = client_for_delete.clone();
                            let table = table_for_delete.clone();
                            let pks = pks.clone();
                            let batch = delete_batch.clone();
                            async move { client.delete_by_primary_keys(&table, &pks, &batch).await }
                        },
                        &operation_name,
                    )
                    .await;
                }
            }

            process_checkpoint_acks(
                checkpoint_messages,
                arrival_time_ms,
                ack_start,
                &metrics_recorder,
                &metric_metadata_id,
                &sink_id,
            );

            let num_rows = num_rows as u64;
            metrics_recorder.record_output_rows_count(num_rows, &metric_metadata_id);
            metrics_recorder.record_elapsed_compute(start_at.elapsed(), &metric_metadata_id);

            row_count += num_rows as usize;

            debug!(
                "[{}] wrote batch of {} rows to table '{}'",
                node_label, num_rows, table_name
            );

            if let Some(limit) = num_records_before_stop {
                records_processed += num_rows;

                tracing::info!(
                    "[{}] records processed: {}, just added: {} (table '{}')",
                    node_label,
                    records_processed,
                    num_rows,
                    table_name
                );

                if records_processed >= limit {
                    // Notify the coordinator (and sources) that the sink has received the expected rows
                    let _ = send(
                        CHECKPOINT_COORDINATOR_CHANNEL,
                        CheckpointMessage::SourceComplete(source_name),
                    );
                    break;
                }
            }
        }

        info!(
            "[{}] write_all completed with {} rows (table '{}')",
            node_label, row_count, self.table_name
        );

        Ok(row_count as u64)
    }
}

impl DisplayAs for ClickHouseSinkExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ClickHouseSink")
    }
}

#[derive(Debug)]
pub struct ClickHouseSourceExec {
    cached_properties: PlanProperties,
    provider: ClickHouseTableProvider,
    split: ClickHouseSourceSplit,
}

impl DisplayAs for ClickHouseSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ClickHouseSourceExec")
    }
}

impl ExecutionPlan for ClickHouseSourceExec {
    fn name(&self) -> &str {
        "ClickHouseSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cached_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.provider.schema.clone();

        let source_params = self
            .provider
            .source_params
            .as_ref()
            .ok_or_else(|| streamling_err!("ClickHouseSourceExec is not a source"))?;

        let mut builder = RecordBatchReceiverStreamBuilder::new(
            schema.clone(),
            source_params.datafusion_buffer_size,
        );
        let tx = builder.tx();
        let (checkpoint_receiver, checkpoint_subscriber_id) =
            subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);

        let (shutdown_tx, mut shutdown_rx) = watch::channel(());

        let reference_name = self.provider.reference_name.clone();
        debug!(
            "ClickHouseSourceExec: subscribed to checkpoint channel for {}",
            reference_name
        );
        let mut query_builder = source_params.query_builder.clone();
        let client = self.provider.client.clone();
        let page_size = source_params
            .query_builder
            .pagination_config()
            .expect("pagination config must be set")
            .page_size;
        let sorting_keys = self.split.sorting_keys.clone();
        let default_block_range = source_params.block_range;
        let table_name_for_exec = source_params.table_name.clone();
        let first_sorting_key_name = sorting_keys
            .first()
            .cloned()
            .expect("sorting keys must not be empty");
        let split = Arc::new(Mutex::new(ClickHouseSourceSplit {
            sorting_keys: self.split.sorting_keys.clone(),
            args: self.split.args.clone(),
        }));
        let state_store = source_params.state_store.clone();
        let empty_batch_schema = schema.clone();

        // Shared checkpoint buffer for metadata propagation
        let checkpoint_buffer = Arc::new(Mutex::new(Vec::<CheckpointMessage>::new()));
        let checkpoint_buffer_for_checkpointing = checkpoint_buffer.clone();
        let checkpoint_buffer_for_data = checkpoint_buffer.clone();

        builder.spawn(async move {
            let checkpointing_task = tokio::spawn({
                let reference_name = reference_name.clone();
                let split_for_checkpointing = split.clone();
                let state_store_for_checkpointing = state_store.clone();
                debug!("Starting checkpointing task for {}", reference_name);
                async move {
                    loop {
                        // Clone receiver for each blocking operation as spawn_blocking requires 'static lifetime
                        // or for the closure to own its data. Receiver is Clone.
                        let receiver_for_blocking_call = checkpoint_receiver.clone();
                        let recv_future = tokio::task::spawn_blocking(move || {
                            // Use a timeout on the blocking recv operation to check for shutdown periodically
                            match receiver_for_blocking_call.recv_timeout(std::time::Duration::from_millis(500)) {
                                Ok(msg) => Ok(msg),
                                Err(crossbeam::channel::RecvTimeoutError::Timeout) => Err(None), // Timeout, just check shutdown
                                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => Err(Some(crossbeam::channel::RecvError)), // Actual error
                            }
                        });

                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => {
                                trace!("Checkpointing task for {} received shutdown signal", reference_name);
                                break;
                            }
                            msg_result_from_blocking = recv_future => {
                                match msg_result_from_blocking {
                                    Ok(Ok(CheckpointMessage::Marker { epoch, created_at_ms })) => {
                                        debug!("Buffering checkpoint marker with epoch {}", epoch.0);
                                        checkpoint_buffer_for_checkpointing.lock().unwrap().push(CheckpointMessage::Marker { epoch, created_at_ms });
                                    }
                                    Ok(Ok(CheckpointMessage::Finalizer(epoch))) => {
                                        debug!("Buffering checkpoint finalizer with epoch {}", epoch.0);
                                        checkpoint_buffer_for_checkpointing.lock().unwrap().push(CheckpointMessage::Finalizer(epoch));

                                        // Save split state for restart recovery
                                        let split_clone = {
                                            let split_guard = split_for_checkpointing.lock().unwrap();
                                            split_guard.clone()
                                        };
                                        let result = state_store_for_checkpointing.save_split(split_clone).await;
                                        trace!("ClickHouseSourceExec: saved split result: {:?} during checkpointing cycle", result);
                                    }
                                    Ok(Ok(_)) => {
                                    }
                                    Ok(Err(_recv_error)) => { // This is crossbeam::channel::RecvError, signifies disconnection
                                    }
                                    Err(join_error) => { // This is tokio::task::JoinError, signifies a panic in spawn_blocking
                                        error!("ClickHouseSourceExec: spawn_blocking for recv panicked: {:?}", join_error);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                trace!("Checkpointing task completed for {}", reference_name);
                }
            });

            debug!("Starting ClickHouse execution with continuous pagination");

            struct BlockRangeState {
                block_range: i128,
                default_block_range: i128,
                max_key: i128,
                range_start: i128,
                template: ScalarValue,
            }

            let max_val = client
                .fetch_max_sorting_key(&table_name_for_exec, &first_sorting_key_name)
                .await
                .streamling_with_context(|| format!(
                    "failed to fetch max sorting key for '{}'",
                    table_name_for_exec
                ))
                .map_err(DataFusionError::from)?;
            // When the table is empty, SELECT max(first_key) returns NULL. In that case
            // scalar_to_i128 returns None; treat this as "no rows to scan" by defaulting to 0,
            // so the pagination loop will immediately terminate (next_start > max_key).
            let max_key = scalar_to_i128(&max_val).unwrap_or(0);

            let initial_keyset = {
                let guard = split.lock().unwrap();
                if !guard.args.is_empty() { Some(guard.args.clone()) } else { None }
            };
            let range_start = initial_keyset.as_ref()
                .and_then(|ks| ks.first().and_then(scalar_to_i128))
                .unwrap_or(0);
            let template = i128_to_scalar_like(0, &max_val);

            let default_block_range = default_block_range as i128;
            let upper = i128_to_scalar_like(range_start + default_block_range, &template);
            query_builder.set_block_range_upper_bound(Some(upper));

            info!(
                "[{}] block range pagination enabled (block_range={}, max_key={}, start={})",
                reference_name, default_block_range, max_key, range_start
            );
            let mut block_state = BlockRangeState {
                block_range: default_block_range,
                default_block_range,
                max_key,
                range_start,
                template,
            };

            let mut has_more_data = true;
            let mut page_count = 0;
            let mut query_retry_attempts: u32 = 0;
            let mut query_retry_backoff_ms: u64 = 100;

            let source_query_timeout = Duration::from_secs(ClickHouseTableProvider::SOURCE_QUERY_TIMEOUT_SECS);
            let recovery_threshold = source_query_timeout / 4;

            while has_more_data {
                page_count += 1;
                let current_query = query_builder.get_query();
                trace!("ClickHouseSourceExec: executing page {} with query: {}", page_count, current_query);

                let page_start = std::time::Instant::now();
                let run_result = tokio::time::timeout(
                    source_query_timeout,
                    Self::run(client.clone(), current_query, Some(page_size)),
                ).await;

                let run_result = match run_result {
                    Ok(inner) => inner,
                    Err(_elapsed) => {
                        Err(DataFusionError::from(streamling_core::streamling_err!(
                            "ClickHouse query timed out after {}s",
                            ClickHouseTableProvider::SOURCE_QUERY_TIMEOUT_SECS
                        )))
                    }
                };

                match run_result {
                    Ok(mut stream) => {
                        query_retry_attempts = 0;
                        query_retry_backoff_ms = 100;
                        let mut batch_count = 0;
                        let mut total_rows_in_page = 0;
                        let mut last_batch_keyset: Option<Vec<ScalarValue>> = None;

                        while let Some(item_result) = stream.next().await {
                            match item_result {
                                Ok(batch) => {
                                    batch_count += 1;
                                    total_rows_in_page += batch.num_rows();

                                    let args = match Self::extract_keyset_from_batch(&batch, &sorting_keys) {
                                        Ok(keyset_values) => keyset_values,
                                        Err(e) => {
                                            error!("Failed to extract keyset from batch: {}", e);
                                            let df_error: DataFusionError = e.into();
                                            if tx.send(Err(df_error)).await.is_err() {
                                                warn!("ClickHouseSourceExec: receiver dropped");
                                            }
                                            has_more_data = false;
                                            break;
                                        }
                                    };

                                    last_batch_keyset = Some(args.clone());

                                    let enriched_batch = {
                                        let mut buffer = checkpoint_buffer_for_data.lock().unwrap();
                                        if !buffer.is_empty() {
                                            debug!("Attaching {} buffered checkpoint messages to batch", buffer.len());
                                            let mut metadata = batch.schema().metadata().clone();
                                            enrich_batch_metadata_with_checkpoints(&mut metadata, &buffer);
                                            let enriched = enrich_batch_with_metadata(batch, metadata)
                                                .expect("Failed to enrich batch with checkpoint metadata");
                                            buffer.clear();
                                            enriched
                                        } else {
                                            batch
                                        }
                                    };

                                    if tx.send(Ok(enriched_batch)).await.is_err() {
                                        warn!("ClickHouseSourceExec: receiver dropped during batch {} of page {}", batch_count, page_count);
                                        has_more_data = false;
                                        break;
                                    } else {
                                        if let Ok(mut split_guard) = split.lock() {
                                            split_guard.update_args(args.clone());
                                        }
                                        trace!("ClickHouseSourceExec: batch {} completed successfully", batch_count);
                                    }
                                }
                                Err(e) => {
                                    error!("Error processing ClickHouse batch: {}", e);
                                    if tx.send(Err(DataFusionError::ArrowError(Box::new(e), None))).await.is_err() {
                                        warn!("ClickHouseSourceExec: receiver dropped while sending error");
                                    }
                                    has_more_data = false;
                                    break;
                                }
                            }
                        }

                        trace!("ClickHouseSourceExec: page {} summary - total_rows: {}, page_size: {}, batch_count: {}", page_count, total_rows_in_page, page_size, batch_count);

                        // Emit an empty batch on empty scans so downstream operators
                        // still receive a batch and buffered checkpoint messages can flow.
                        if total_rows_in_page == 0 {
                            let empty_batch = {
                                let batch = RecordBatch::new_empty(empty_batch_schema.clone());
                                let mut buffer = checkpoint_buffer_for_data.lock().unwrap();
                                if !buffer.is_empty() {
                                    debug!("Attaching {} buffered checkpoint messages to empty batch", buffer.len());
                                    let mut metadata = batch.schema().metadata().clone();
                                    enrich_batch_metadata_with_checkpoints(&mut metadata, &buffer);
                                    let enriched = enrich_batch_with_metadata(batch, metadata)
                                        .expect("Failed to enrich empty batch with checkpoint metadata");
                                    buffer.clear();
                                    enriched
                                } else {
                                    batch
                                }
                            };
                            info!("[{}] empty scan on block range [{}, {})",
                                reference_name, block_state.range_start, block_state.range_start + block_state.block_range);
                            if tx.send(Ok(empty_batch)).await.is_err() {
                                warn!("ClickHouseSourceExec: receiver dropped while sending empty batch");
                                has_more_data = false;
                                continue;
                            }
                        }

                        let page_exhausted = total_rows_in_page < page_size;

                        if page_exhausted {
                            let next_start = block_state.range_start + block_state.block_range;
                            if next_start > block_state.max_key {
                                has_more_data = false;
                                info!("[{}] scanned past max_key {} — done", reference_name, block_state.max_key);
                            } else {
                                block_state.range_start = next_start;

                                let page_elapsed = page_start.elapsed();
                                if block_state.block_range < block_state.default_block_range
                                    && page_elapsed < recovery_threshold
                                {
                                    block_state.block_range = (block_state.block_range * 2).min(block_state.default_block_range);
                                    info!("[{}] recovering block range to {} (page took {:?})", reference_name, block_state.block_range, page_elapsed);
                                }

                                let upper = i128_to_scalar_like(next_start + block_state.block_range, &block_state.template);
                                query_builder.set_block_range_upper_bound(Some(upper));
                                let start_val = i128_to_scalar_like(next_start, &block_state.template);
                                query_builder.start_at_page(vec![start_val]);

                                info!("[{}] advancing to next block range [{}, {})", reference_name, next_start, next_start + block_state.block_range);
                            }
                        } else {
                            // More rows in this range — continue keyset pagination
                            if let Some(keyset) = last_batch_keyset {
                                query_builder.update_keyset(keyset);
                            } else {
                                has_more_data = false;
                            }
                        }

                        trace!("ClickHouseSourceExec: done reading page {} (batches: {}, has_more: {})", page_count, batch_count, has_more_data);
                    }
                    Err(df_error) => {
                        if is_timeout_error(&df_error) {
                            let old_br = block_state.block_range;
                            block_state.block_range = (block_state.block_range / 2).max(ClickHouseTableProvider::MIN_BLOCK_RANGE as i128);
                            warn!(
                                "[{}] timeout on page {}; reducing block range {} -> {}",
                                reference_name, page_count, old_br, block_state.block_range
                            );
                            let upper = i128_to_scalar_like(block_state.range_start + block_state.block_range, &block_state.template);
                            query_builder.set_block_range_upper_bound(Some(upper));
                            // Reset keyset to retry from the start of the current block range,
                            // otherwise the stale keyset can contradict the new upper bound.
                            let start_val = i128_to_scalar_like(block_state.range_start, &block_state.template);
                            query_builder.start_at_page(vec![start_val]);

                            page_count -= 1;
                            tokio::time::sleep(Duration::from_millis(1_000)).await;
                            continue;
                        }

                        query_retry_attempts += 1;
                        if query_retry_attempts > 5 {
                            error!(
                                "ClickHouseSourceExec: page {} query failed (attempt {}): {}. Retrying with backoff...",
                                page_count, query_retry_attempts, df_error
                            );
                        } else {
                            warn!(
                                "ClickHouseSourceExec: page {} query failed (attempt {}): {}. Retrying with backoff...",
                                page_count, query_retry_attempts, df_error
                            );
                        }
                        page_count -= 1;
                        let jitter = (query_retry_attempts as u64 % 100) * 7;
                        let sleep_ms = std::cmp::min(30_000u64, query_retry_backoff_ms + jitter);
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                        query_retry_backoff_ms = std::cmp::min(query_retry_backoff_ms.saturating_mul(2), 30_000);
                        continue;
                    }
                }
            }

            info!("ClickHouseSourceExec: finished continuous pagination (processed {} pages)", page_count);

            // Signal the checkpointing task to shut down
            if shutdown_tx.send(()).is_err() {
                warn!("Failed to send shutdown signal to checkpointing task for {}", reference_name);
            }
            // Wait for the checkpointing task to complete
            if let Err(e) = checkpointing_task.await {
                error!("Checkpointing task for {} failed: {:?}", reference_name, e);
            }
            debug!("Checkpointing task completed for {}", reference_name);

            // Unsubscribe from checkpoint channel before dropping the receiver
            // to avoid SendError when the coordinator tries to send to this channel
            unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, checkpoint_subscriber_id);
            debug!("ClickHouseSourceExec: unsubscribed from checkpoint channel for {}", reference_name);

            Ok(())
        });

        info!("ClickHouseSourceExec built successfully");
        Ok(builder.build())
    }
}

impl ClickHouseSourceExec {
    pub async fn run(
        client: ClickHouseClient,
        query: &str,
        limit: Option<usize>,
    ) -> Result<
        Pin<Box<dyn Stream<Item = arrow::error::Result<RecordBatch>> + Send>>,
        DataFusionError,
    > {
        let query = format!(
            "{} {} FORMAT Arrow",
            query,
            limit.map_or("".to_string(), |l| format!("LIMIT {}", l))
        );

        trace!("Opening new page with query: {}", query);
        let response_result = client
            .send_query(reqwest::Method::GET, query.as_str())
            .await;
        let response_bytes = client
            .process_http_response(response_result, query.as_str(), "run")
            .await?;

        let record_batch_stream = async_stream::stream! {
            let reader = match client.create_arrow_reader(response_bytes, query.as_str()) {
                Ok(r) => r,
                Err(e) => {
                    yield Err(ArrowError::ExternalError(e.into()));
                    return;
                }
            };
            for batch_result in reader {
                yield batch_result;
            }
        };

        trace!("Created async record batch stream");
        Ok(Box::pin(record_batch_stream))
    }

    pub fn extract_keyset_from_batch(
        batch: &RecordBatch,
        sorting_keys: &[String],
    ) -> streamling_core::error::Result<Vec<ScalarValue>> {
        // If there are no sorting keys, return empty vector (no pagination possible)
        if sorting_keys.is_empty() {
            return Ok(Vec::new());
        }

        let last_row_idx = batch.num_rows() - 1;
        let mut keyset_values = Vec::new();

        for key in sorting_keys {
            let column = batch.column_by_name(key.as_str()).ok_or_else(|| {
                streamling_err!("sorting key '{}' not found in batch columns", key)
            })?;

            let scalar_value = ScalarValue::try_from_array(column, last_row_idx)
                .streamling_with_context(|| {
                    format!("failed to extract value for sorting key '{}'", key)
                })?;

            keyset_values.push(scalar_value);
        }

        Ok(keyset_values)
    }
}

#[derive(Clone, Debug)]
pub struct ClickHouseClient {
    creds: ClickHouseConfig,
    http_client: reqwest::Client,
    compression: ClickHouseCompression,
    compression_level: GzipCompressionLevel,
}

impl ClickHouseClient {
    const DATABASE_HEADER: &str = "X-ClickHouse-Database";
    const ARROW_STRING_AS_STRING: &str = "output_format_arrow_string_as_string";
    const DEFAULT_TIMEOUT_SECS: u64 = 300; // 5 minutes

    pub fn new(creds: ClickHouseConfig) -> Self {
        let compression = creds.compression;
        let compression_level = creds.compression_level;
        Self::with_compression(creds, compression, compression_level)
    }

    pub fn with_compression(
        creds: ClickHouseConfig,
        compression: ClickHouseCompression,
        compression_level: GzipCompressionLevel,
    ) -> Self {
        // Configure HTTP client with optimized connection pooling settings
        // These settings improve connection reuse and reduce latency for high-throughput workloads
        let http_client = reqwest::Client::builder()
            // Keep idle connections in the pool for 90 seconds (default is also 90s, but explicit)
            .pool_idle_timeout(Duration::from_secs(90))
            // Limit max idle connections per host to prevent unbounded resource growth
            // while still allowing good parallelism for batch inserts
            .pool_max_idle_per_host(32)
            // Enable TCP keepalive to maintain connections through load balancers/proxies
            // that might close idle connections prematurely
            .tcp_keepalive(Duration::from_secs(60))
            // Enable TCP nodelay to reduce latency for small requests
            .tcp_nodelay(true)
            .build()
            .expect("Failed to build HTTP client for ClickHouse");

        ClickHouseClient {
            creds,
            http_client,
            compression,
            compression_level,
        }
    }

    pub async fn send_query(
        &self,
        method: reqwest::Method,
        query: &str,
    ) -> Result<Response, Error> {
        let mut request = self
            .http_client
            .request(method.clone(), &self.creds.url)
            .basic_auth(&self.creds.user, Some(&self.creds.password))
            .header(Self::DATABASE_HEADER, &self.creds.database)
            .query(&[(Self::ARROW_STRING_AS_STRING, "1")])
            .timeout(Duration::from_secs(Self::DEFAULT_TIMEOUT_SECS));

        request = match method {
            reqwest::Method::POST => request.body(query.to_string()),
            reqwest::Method::GET => request.query(&[("query", query)]),
            _ => request,
        };
        // Avoid logging full request (may include Authorization headers)
        trace!(
            "ClickHouse: sending {:?} request to {}",
            method, self.creds.url
        );
        request.send().await
    }

    async fn process_http_response(
        &self,
        result: Result<Response, Error>,
        query: &str,
        operation_name: &str,
    ) -> streamling_core::error::Result<Vec<u8>> {
        let response = match result {
            Ok(resp) => resp,
            Err(e) => {
                error!(
                    "ClickHouse {} request (construction/network) failed for query '{}': {}",
                    operation_name, query, e
                );
                return Err(StreamlingError::retriable_with_cause(
                    format!(
                        "ClickHouse {} request (construction/network) failed for query '{}'",
                        operation_name, query
                    ),
                    e,
                ));
            }
        };

        let status = response.status();
        let is_transient =
            status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS;

        let response_bytes = response
            .bytes()
            .await
            .streamling_with_context(|| {
                format!(
                    "failed to read ClickHouse response bytes for {} query",
                    operation_name
                )
            })
            .map_err(|e| e.mark_retriable())?;

        if !status.is_success() {
            let error_body = String::from_utf8_lossy(response_bytes.as_ref());
            error!(
                "ClickHouse {} request failed for query '{}'. Status: {}. Body: {}",
                operation_name, query, status, error_body
            );
            let err = streamling_err!(
                "ClickHouse {} request failed. Status: {}. Body: {}",
                operation_name,
                status,
                error_body
            );
            return Err(if is_transient {
                err.mark_retriable()
            } else {
                err
            });
        }

        Ok(response_bytes.to_vec())
    }

    /// Validates a ClickHouse HTTP response for DDL statements (CREATE, ALTER).
    ///
    /// ClickHouse can return HTTP 200 with an error message in the body for some
    /// DDL failures (e.g. nullable column in primary key on CREATE, or missing
    /// column on ALTER ... DELETE). Status code alone is therefore not sufficient
    /// to determine success — we also scan the body for an exception marker.
    async fn check_ddl_response(
        response: Response,
        operation: &str,
        table_name: &str,
    ) -> streamling_core::error::Result<()> {
        let status = response.status();
        // A mid-body read failure is a textbook transient (TCP reset, etc.) — mark
        // retriable so the operator retries. CREATE TABLE IF NOT EXISTS is idempotent
        // and ALTER TABLE DELETE WHERE pk IN (...) is idempotent against a static PK
        // set, so re-issuing on a flaky network is safe.
        let body_bytes = response
            .bytes()
            .await
            .streamling_with_context(|| {
                format!(
                    "failed to read ClickHouse {} response body for table '{}'",
                    operation, table_name
                )
            })
            .map_err(|e| e.mark_retriable())?;
        let body = String::from_utf8_lossy(&body_bytes);

        if !status.is_success() {
            return Err(streamling_err!(
                "ClickHouse {} failed for table '{}'. Status: {}. Body: {}",
                operation,
                table_name,
                status,
                body
            ));
        }

        if CLICKHOUSE_ERROR_RE.is_match(&body) {
            return Err(streamling_err!(
                "ClickHouse {} failed for table '{}': {}",
                operation,
                table_name,
                body.trim()
            ));
        }

        Ok(())
    }

    fn create_arrow_reader(
        &self,
        response_bytes: Vec<u8>,
        query: &str,
    ) -> streamling_core::error::Result<FileReader<Cursor<Vec<u8>>>> {
        let cursor = Cursor::new(response_bytes);
        FileReader::try_new(cursor, None).streamling_with_context(|| {
            format!(
                "failed to create Arrow IPC FileReader for query '{}'",
                query
            )
        })
    }

    pub fn fetch_schema(
        &self,
        table_name: &str,
        columns: Option<Vec<String>>,
    ) -> streamling_core::error::Result<SchemaRef> {
        let mut query_builder = ClickHouseQueryBuilder::of(
            table_name.to_string(),
            columns.unwrap_or(vec!["*".to_string()]),
            None,
            None,
        );

        let query = format!("{} LIMIT 1 FORMAT Arrow", query_builder.get_query());
        let response_bytes = block_on(async {
            let response_result = self.send_query(reqwest::Method::GET, query.as_str()).await;
            self.process_http_response(response_result, query.as_str(), "schema")
                .await
        })?;

        let reader = self.create_arrow_reader(response_bytes, &query)?;
        debug!(
            "ClickHouse schema query returned Arrow schema: {:?}",
            reader.schema()
        );
        Ok(reader.schema())
    }

    pub fn fetch_sorting_keys(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> streamling_core::error::Result<Vec<String>> {
        let query = format!(
            "SELECT sorting_key FROM system.tables WHERE database = '{}' AND name = '{}' FORMAT Arrow",
            database_name, table_name
        );
        let response_bytes = block_on(async {
            let response_result = self.send_query(reqwest::Method::GET, query.as_str()).await;
            self.process_http_response(response_result, query.as_str(), "sorting_keys")
                .await
        })?;

        let mut reader = self.create_arrow_reader(response_bytes, &query)?;
        // read values of the first row
        let batch = reader
            .next()
            .ok_or_else(|| streamling_err!("no rows returned for sorting keys query"))??;
        let sorting_keys = ScalarValue::try_from_array(&batch.column(0), 0)
            .streamling_context("failed to extract sorting keys from query result")?;
        let sorting_keys = sorting_keys
            .to_string()
            .split(",")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<String>>();
        Ok(sorting_keys)
    }

    pub async fn fetch_max_sorting_key(
        &self,
        table_name: &str,
        key_name: &str,
    ) -> streamling_core::error::Result<ScalarValue> {
        let query = format!("SELECT max({}) FROM {} FORMAT Arrow", key_name, table_name);
        let response_result = self.send_query(reqwest::Method::GET, query.as_str()).await;
        let response_bytes = self
            .process_http_response(response_result, query.as_str(), "max_sorting_key")
            .await?;

        let mut reader = self.create_arrow_reader(response_bytes, &query)?;
        let batch = reader
            .next()
            .ok_or_else(|| streamling_err!("no rows returned for max sorting key query"))??;
        let max_val = ScalarValue::try_from_array(&batch.column(0), 0)
            .streamling_context("failed to extract max sorting key value")?;
        Ok(max_val)
    }

    /// Normalize schema for ClickHouse Arrow IPC: convert view types to standard types
    fn normalize_schema_for_clickhouse(
        schema: &arrow::datatypes::Schema,
    ) -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType, Field};

        let normalized_fields: Vec<arrow::datatypes::FieldRef> = schema
            .fields()
            .iter()
            .map(|field| {
                let normalized_type = match field.data_type() {
                    DataType::Utf8View => DataType::Utf8,
                    DataType::BinaryView => DataType::Binary,
                    other => other.clone(),
                };

                if normalized_type != *field.data_type() {
                    Arc::new(
                        Field::new(field.name(), normalized_type, field.is_nullable())
                            .with_metadata(field.metadata().clone()),
                    )
                } else {
                    field.clone()
                }
            })
            .collect();

        arrow::datatypes::Schema::new_with_metadata(normalized_fields, schema.metadata().clone())
    }

    /// Convert a RecordBatch to use the provided normalized schema
    /// TODO: Move this to a projection expr instead so it can be folded?
    fn normalize_batch_for_clickhouse(
        batch: &RecordBatch,
        normalized_schema: &SchemaRef,
    ) -> Result<RecordBatch, DataFusionError> {
        use arrow::compute::cast;
        use arrow::datatypes::DataType;

        let original_schema = batch.schema();

        let normalized_columns: Result<Vec<_>, DataFusionError> = batch
            .columns()
            .iter()
            .zip(original_schema.fields().iter())
            .zip(normalized_schema.fields().iter())
            .map(|((column, original_field), normalized_field)| {
                match (original_field.data_type(), normalized_field.data_type()) {
                    (DataType::Utf8View, DataType::Utf8) => {
                        Ok(cast(column.as_ref(), &DataType::Utf8)
                            .streamling_context("failed to cast Utf8View to Utf8")?)
                    }
                    (DataType::BinaryView, DataType::Binary) => {
                        Ok(cast(column.as_ref(), &DataType::Binary)
                            .streamling_context("failed to cast BinaryView to Binary")?)
                    }
                    // Perform endian conversion for U256/I256. We store as big endian and clickhouse needs little.
                    (DataType::FixedSizeBinary(32), DataType::FixedSizeBinary(32))
                        if U256Type::is_u256_metadata(original_field.metadata())
                            || I256Type::is_i256_metadata(original_field.metadata()) =>
                    {
                        // Call the reverse_bytes32 UDF directly using invoke_with_args
                        let func = ReverseBytes32Func::new();
                        let args = ScalarFunctionArgs {
                            args: vec![ColumnarValue::Array(column.clone())],
                            arg_fields: vec![original_field.clone()],
                            number_rows: batch.num_rows(),
                            return_field: ScalarUDFImpl::return_field_from_args(
                                &func,
                                ReturnFieldArgs {
                                    arg_fields: std::slice::from_ref(original_field),
                                    scalar_arguments: &[None],
                                },
                            )?,
                        };
                        let result = ScalarUDFImpl::invoke_with_args(&func, args)?;
                        match result {
                            ColumnarValue::Array(arr) => Ok(arr),
                            ColumnarValue::Scalar(s) => Ok(s.to_array()?),
                        }
                    }
                    _ => Ok(column.clone()),
                }
            })
            .collect();

        Ok(
            RecordBatch::try_new(Arc::clone(normalized_schema), normalized_columns?)
                .streamling_context("failed to create normalized batch")?,
        )
    }

    /// Mirror of `normalize_batch_for_clickhouse` for the read side.
    ///
    /// ClickHouse stores `UInt256` / `Int256` as little-endian and emits them as
    /// `FixedSizeBinary(32)` without the streamling u256/i256 extension metadata.
    /// Streamling's internal convention is big-endian + extension metadata, so a
    /// raw passthrough corrupts every value (a logical 1 reads as 2^248, which
    /// overflows U256 on multiplication). For each column whose target field is
    /// u256/i256 but whose incoming field lacks that metadata, reverse the bytes
    /// and adopt the target field on that column only.
    ///
    /// Crucially, untouched columns keep their original Field rather than
    /// being rewritten against `target_schema`. The hybrid layer deliberately
    /// tolerates List inner-field-name differences (e.g. ClickHouse's `item`
    /// vs Kafka/Avro's `element`); forcing target_schema onto every column
    /// would reject those List columns at `RecordBatch::try_new`.
    pub(crate) fn normalize_batch_from_clickhouse(
        batch: &RecordBatch,
        target_schema: &SchemaRef,
    ) -> Result<RecordBatch, DataFusionError> {
        use arrow::datatypes::DataType;

        if batch.num_columns() != target_schema.fields().len() {
            return Ok(batch.clone());
        }

        let original_schema = batch.schema();
        let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
        let mut new_fields: Vec<FieldRef> = Vec::with_capacity(batch.num_columns());
        let mut changed = false;

        for ((column, original_field), target_field) in batch
            .columns()
            .iter()
            .zip(original_schema.fields().iter())
            .zip(target_schema.fields().iter())
        {
            let needs_reverse = matches!(
                (original_field.data_type(), target_field.data_type()),
                (DataType::FixedSizeBinary(32), DataType::FixedSizeBinary(32))
            ) && (U256Type::is_u256_metadata(target_field.metadata())
                || I256Type::is_i256_metadata(target_field.metadata()))
                && !U256Type::is_u256_metadata(original_field.metadata())
                && !I256Type::is_i256_metadata(original_field.metadata());

            if !needs_reverse {
                new_columns.push(column.clone());
                new_fields.push(original_field.clone());
                continue;
            }

            let func = ReverseBytes32Func::new();
            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(column.clone())],
                arg_fields: vec![original_field.clone()],
                number_rows: batch.num_rows(),
                return_field: ScalarUDFImpl::return_field_from_args(
                    &func,
                    ReturnFieldArgs {
                        arg_fields: std::slice::from_ref(original_field),
                        scalar_arguments: &[None],
                    },
                )?,
            };
            let result = ScalarUDFImpl::invoke_with_args(&func, args)?;
            let arr = match result {
                ColumnarValue::Array(arr) => arr,
                ColumnarValue::Scalar(s) => s.to_array()?,
            };
            new_columns.push(arr);
            new_fields.push(target_field.clone());
            changed = true;
        }

        if !changed {
            return Ok(batch.clone());
        }

        let new_schema = Arc::new(Schema::new_with_metadata(
            new_fields,
            original_schema.metadata().clone(),
        ));
        Ok(RecordBatch::try_new(new_schema, new_columns)
            .streamling_context("failed to create normalized batch from clickhouse")?)
    }

    /// Send normalized Arrow batches to ClickHouse
    pub async fn send_arrow_batch(
        &self,
        table_name: &str,
        batch: &RecordBatch,
        schema: &SchemaRef,
    ) -> streamling_core::error::Result<()> {
        // Offload Arrow IPC encoding (and gzip compression when enabled) to a
        // blocking thread to avoid stalling the async runtime. Compressing
        // inline with encoding avoids an extra allocation/copy of the
        // uncompressed buffer.
        let schema_for_encode = schema.clone();
        let batch_for_encode = batch.clone();
        let compression = self.compression;
        let compression_level = self.compression_level;
        let buffer: Vec<u8> = tokio::task::spawn_blocking(move || match compression {
            ClickHouseCompression::None => {
                let mut buf = Vec::new();
                let mut writer = FileWriter::try_new(&mut buf, &schema_for_encode)
                    .streamling_context("failed to create Arrow IPC FileWriter")?;
                writer
                    .write(&batch_for_encode)
                    .streamling_context("failed to write batch to Arrow IPC")?;
                writer
                    .finish()
                    .streamling_context("failed to finish Arrow IPC write")?;
                Ok::<_, StreamlingError>(buf)
            }
            ClickHouseCompression::Gzip => {
                let mut encoder = flate2::write::GzEncoder::new(
                    Vec::new(),
                    flate2::Compression::new(compression_level.as_u32()),
                );
                {
                    let mut writer = FileWriter::try_new(&mut encoder, &schema_for_encode)
                        .streamling_context("failed to create Arrow IPC FileWriter")?;
                    writer
                        .write(&batch_for_encode)
                        .streamling_context("failed to write batch to Arrow IPC")?;
                    writer
                        .finish()
                        .streamling_context("failed to finish Arrow IPC write")?;
                }
                encoder
                    .finish()
                    .streamling_context("failed to finish gzip encoding")
            }
        })
        .await
        .streamling_context("failed to join Arrow IPC encode task")??;

        let body = Bytes::from(buffer);
        let columns = schema
            .fields()
            .iter()
            .map(|f| format!("`{}`", f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("INSERT INTO {} ({}) FORMAT Arrow", table_name, columns);

        trace!("ClickHouse INSERT for table {}", table_name);

        let mut request = self
            .http_client
            .post(&self.creds.url)
            .basic_auth(&self.creds.user, Some(&self.creds.password))
            .header(Self::DATABASE_HEADER, &self.creds.database)
            .query(&[
                ("query", query.as_str()),
                // Let ClickHouse fill missing columns with their DEFAULT values
                // instead of rejecting the INSERT.
                ("input_format_arrow_allow_missing_columns", "1"),
            ])
            .timeout(std::time::Duration::from_secs(Self::DEFAULT_TIMEOUT_SECS))
            .body(body);

        if matches!(self.compression, ClickHouseCompression::Gzip) {
            request = request.header(reqwest::header::CONTENT_ENCODING, "gzip");
        }

        let resp = request.send().await.streamling_with_context(|| {
            format!("ClickHouse INSERT network error for table {}", table_name)
        })?;

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let error_body = resp
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(streamling_err!(
                "ClickHouse INSERT failed for table '{}'. Status: {}. Body: {}",
                table_name,
                status,
                error_body
            ))
        }
    }

    /// Execute `ALTER TABLE DELETE` for rows matching the given primary key values.
    /// Batches deletes into chunks to avoid exceeding ClickHouse's `max_query_size`.
    pub async fn delete_by_primary_keys(
        &self,
        table_name: &str,
        primary_keys: &[String],
        delete_batch: &RecordBatch,
    ) -> streamling_core::error::Result<()> {
        if delete_batch.num_rows() == 0 {
            return Ok(());
        }

        // Build all value tuples first
        let mut value_tuples = Vec::with_capacity(delete_batch.num_rows());
        for row_idx in 0..delete_batch.num_rows() {
            let mut values = Vec::with_capacity(primary_keys.len());
            for pk in primary_keys {
                let col = delete_batch.column_by_name(pk).ok_or_else(|| {
                    streamling_err!("primary key column '{}' not found in delete batch", pk)
                })?;
                let scalar =
                    ScalarValue::try_from_array(col, row_idx).streamling_with_context(|| {
                        format!(
                            "failed to extract primary key value for column '{}' at row {}",
                            pk, row_idx
                        )
                    })?;
                values.push(Self::scalar_to_clickhouse_literal(&scalar));
            }
            if primary_keys.len() == 1 {
                value_tuples.push(values[0].clone());
            } else {
                value_tuples.push(format!("({})", values.join(", ")));
            }
        }

        let pk_clause = if primary_keys.len() == 1 {
            format!("`{}`", primary_keys[0])
        } else {
            format!(
                "({})",
                primary_keys
                    .iter()
                    .map(|pk| format!("`{}`", pk))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        // Chunk deletes to stay well under ClickHouse's max_query_size (256KB default).
        // 1000 rows per chunk is conservative and safe for most PK sizes.
        const DELETE_CHUNK_SIZE: usize = 1000;

        for chunk in value_tuples.chunks(DELETE_CHUNK_SIZE) {
            let where_clause = format!("{} IN ({})", pk_clause, chunk.join(", "));
            let query = format!("ALTER TABLE {} DELETE WHERE {}", table_name, where_clause);

            debug!(
                "ClickHouse DELETE for table {} ({} rows in this chunk, {} total)",
                table_name,
                chunk.len(),
                delete_batch.num_rows()
            );

            let response = self
                .send_query(reqwest::Method::POST, &query)
                .await
                .streamling_with_context(|| {
                    format!(
                        "ClickHouse ALTER TABLE DELETE failed for table '{}'",
                        table_name
                    )
                })?;

            Self::check_ddl_response(response, "ALTER TABLE DELETE", table_name).await?;
        }

        Ok(())
    }

    /// Convert a ScalarValue to a ClickHouse SQL literal string.
    fn scalar_to_clickhouse_literal(scalar: &ScalarValue) -> String {
        if scalar.is_null() {
            return "NULL".to_string();
        }
        match scalar {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
            }
            ScalarValue::Int8(Some(v)) => v.to_string(),
            ScalarValue::Int16(Some(v)) => v.to_string(),
            ScalarValue::Int32(Some(v)) => v.to_string(),
            ScalarValue::Int64(Some(v)) => v.to_string(),
            ScalarValue::UInt8(Some(v)) => v.to_string(),
            ScalarValue::UInt16(Some(v)) => v.to_string(),
            ScalarValue::UInt32(Some(v)) => v.to_string(),
            ScalarValue::UInt64(Some(v)) => v.to_string(),
            ScalarValue::Float32(Some(v)) => v.to_string(),
            ScalarValue::Float64(Some(v)) => v.to_string(),
            ScalarValue::Boolean(Some(v)) => if *v { "1" } else { "0" }.to_string(),
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                let s = ScalarValue::Decimal128(Some(*v), *precision, *scale).to_string();
                // Strip any quotes that Display might add
                s.trim_matches('\'').to_string()
            }
            ScalarValue::Decimal256(Some(v), precision, scale) => {
                let s = ScalarValue::Decimal256(Some(*v), *precision, *scale).to_string();
                s.trim_matches('\'').to_string()
            }
            ScalarValue::Date32(Some(v)) => {
                let s = ScalarValue::Date32(Some(*v)).to_string();
                let s = s.trim_matches('\'');
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
            }
            ScalarValue::Date64(Some(v)) => {
                let s = ScalarValue::Date64(Some(*v)).to_string();
                let s = s.trim_matches('\'');
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
            }
            ScalarValue::TimestampSecond(Some(v), tz) => {
                let s = ScalarValue::TimestampSecond(Some(*v), tz.clone()).to_string();
                let s = s.trim_matches('\'');
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
            }
            ScalarValue::TimestampMillisecond(Some(v), tz) => {
                let s = ScalarValue::TimestampMillisecond(Some(*v), tz.clone()).to_string();
                let s = s.trim_matches('\'');
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
            }
            ScalarValue::TimestampMicrosecond(Some(v), tz) => {
                let s = ScalarValue::TimestampMicrosecond(Some(*v), tz.clone()).to_string();
                let s = s.trim_matches('\'');
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
            }
            ScalarValue::TimestampNanosecond(Some(v), tz) => {
                let s = ScalarValue::TimestampNanosecond(Some(*v), tz.clone()).to_string();
                let s = s.trim_matches('\'');
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
            }
            _ => {
                // Fallback: use Display with quoting for unhandled types
                let s = scalar.to_string();
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
            }
        }
    }

    /// Strip the _gs_op column from a RecordBatch and return the filtered batch.
    fn strip_gs_op_column(
        batch: &RecordBatch,
    ) -> std::result::Result<RecordBatch, DataFusionError> {
        let schema = batch.schema();
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            if field.name() != COLUMN_NAME_OP {
                fields.push(field.clone());
                columns.push(batch.column(idx).clone());
            }
        }
        let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        ));
        RecordBatch::try_new(new_schema, columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    pub fn arrow_field_to_clickhouse(field: &arrow::datatypes::Field) -> String {
        let ch_type = match field.data_type() {
            arrow::datatypes::DataType::Null => "Nullable(Nothing)".to_string(),
            arrow::datatypes::DataType::Boolean => "UInt8".to_string(),
            arrow::datatypes::DataType::Int8 => "Int8".to_string(),
            arrow::datatypes::DataType::Int16 => "Int16".to_string(),
            arrow::datatypes::DataType::Int32 => "Int32".to_string(),
            arrow::datatypes::DataType::Int64 => "Int64".to_string(),
            arrow::datatypes::DataType::UInt8 => "UInt8".to_string(),
            arrow::datatypes::DataType::UInt16 => "UInt16".to_string(),
            arrow::datatypes::DataType::UInt32 => "UInt32".to_string(),
            arrow::datatypes::DataType::UInt64 => "UInt64".to_string(),
            arrow::datatypes::DataType::Float16 => "Float32".to_string(),
            arrow::datatypes::DataType::Float32 => "Float32".to_string(),
            arrow::datatypes::DataType::Float64 => "Float64".to_string(),
            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                "String".to_string()
            }
            arrow::datatypes::DataType::Binary | arrow::datatypes::DataType::LargeBinary => {
                "String".to_string()
            }
            arrow::datatypes::DataType::FixedSizeBinary(size) => {
                // Check metadata to detect U256/I256 types
                if *size == 32 {
                    if U256Type::is_u256_metadata(field.metadata()) {
                        return "UInt256".to_string();
                    } else if I256Type::is_i256_metadata(field.metadata()) {
                        return "Int256".to_string();
                    }
                }
                // Regular fixed binary - use FixedString
                format!("FixedString({})", size)
            }
            arrow::datatypes::DataType::Date32 => "Date".to_string(),
            arrow::datatypes::DataType::Date64 => "DateTime".to_string(),
            arrow::datatypes::DataType::Time32(_) => "DateTime".to_string(),
            arrow::datatypes::DataType::Time64(_) => "DateTime64(9)".to_string(),
            arrow::datatypes::DataType::Timestamp(unit, _) => match unit {
                arrow::datatypes::TimeUnit::Second => "DateTime".to_string(),
                arrow::datatypes::TimeUnit::Millisecond => "DateTime64(3)".to_string(),
                arrow::datatypes::TimeUnit::Microsecond => "DateTime64(6)".to_string(),
                arrow::datatypes::TimeUnit::Nanosecond => "DateTime64(9)".to_string(),
            },
            arrow::datatypes::DataType::Duration(_) => "Int64".to_string(),
            arrow::datatypes::DataType::Interval(_) => "String".to_string(),
            arrow::datatypes::DataType::Decimal128(precision, scale) => {
                if *precision <= 76 {
                    format!("Decimal({}, {})", precision, scale)
                } else {
                    warn!(
                        "Decimal128 with precision {} exceeds ClickHouse max (76), using String",
                        precision
                    );
                    "String".to_string()
                }
            }
            arrow::datatypes::DataType::Decimal256(precision, scale) => {
                if *precision <= 76 {
                    format!("Decimal({}, {})", precision, scale)
                } else {
                    warn!(
                        "Decimal256 with precision {} exceeds ClickHouse max (76), using String",
                        precision
                    );
                    "String".to_string()
                }
            }
            arrow::datatypes::DataType::List(inner_field)
            | arrow::datatypes::DataType::LargeList(inner_field) => {
                let inner_type = Self::arrow_field_to_clickhouse(inner_field);
                format!("Array({})", inner_type)
            }
            arrow::datatypes::DataType::FixedSizeList(inner_field, _) => {
                let inner_type = Self::arrow_field_to_clickhouse(inner_field);
                format!("Array({})", inner_type)
            }
            arrow::datatypes::DataType::Struct(fields) => {
                let field_defs: Vec<String> = fields
                    .iter()
                    .map(|f| {
                        let ch_type = Self::arrow_field_to_clickhouse(f);
                        format!("{} {}", f.name(), ch_type)
                    })
                    .collect();
                format!("Tuple({})", field_defs.join(", "))
            }
            arrow::datatypes::DataType::Union(_, _) => {
                warn!("Union types are not directly supported in ClickHouse, defaulting to String");
                "String".to_string()
            }
            arrow::datatypes::DataType::Dictionary(_, value_type) => {
                // For dictionaries, we need to create a temporary field for the value type
                let temp_field =
                    arrow::datatypes::Field::new("value", value_type.as_ref().clone(), true);
                Self::arrow_field_to_clickhouse(&temp_field)
            }
            arrow::datatypes::DataType::Map(map_field, _) => {
                if let arrow::datatypes::DataType::Struct(fields) = map_field.data_type()
                    && fields.len() == 2
                {
                    let key_type = Self::arrow_field_to_clickhouse(&fields[0]);
                    let value_type = Self::arrow_field_to_clickhouse(&fields[1]);
                    return format!("Map({}, {})", key_type, value_type);
                }
                warn!("Complex Map type cannot be directly converted, using String");
                "String".to_string()
            }
            arrow::datatypes::DataType::RunEndEncoded(_, inner_field) => {
                Self::arrow_field_to_clickhouse(inner_field)
            }
            arrow::datatypes::DataType::Utf8View | arrow::datatypes::DataType::BinaryView => {
                "String".to_string()
            }
            arrow::datatypes::DataType::ListView(_)
            | arrow::datatypes::DataType::LargeListView(_) => {
                warn!(
                    "ListView types are not directly supported in ClickHouse, defaulting to String"
                );
                "String".to_string()
            }
        };
        // ClickHouse doesn't support Nullable for Array, Tuple, and Map types
        let is_non_nullable_type = ch_type.starts_with("Array(")
            || ch_type.starts_with("Tuple(")
            || ch_type.starts_with("Map(");
        if field.is_nullable() && !is_non_nullable_type {
            format!("Nullable({})", ch_type)
        } else {
            ch_type
        }
    }

    pub async fn create_table_if_not_exists(
        &self,
        table_name: &str,
        schema: &SchemaRef,
        primary_keys: Vec<String>,
        append_only_mode: bool,
        version_column_name: Option<&str>,
        schema_override: Option<&std::collections::HashMap<String, String>>,
    ) -> streamling_core::error::Result<()> {
        let create_table_query = self.build_create_table_query(
            table_name,
            schema,
            primary_keys,
            append_only_mode,
            version_column_name,
            schema_override,
        )?;

        debug!(
            "Creating ClickHouse table with query: {}",
            create_table_query
        );

        let response = self
            .send_query(reqwest::Method::POST, &create_table_query)
            .await
            .streamling_with_context(|| {
                format!("failed to create ClickHouse table '{}'", table_name)
            })?;

        Self::check_ddl_response(response, "CREATE TABLE", table_name).await?;

        info!("Successfully created ClickHouse table '{}'", table_name);
        Ok(())
    }

    fn build_create_table_query(
        &self,
        table_name: &str,
        schema: &SchemaRef,
        primary_keys: Vec<String>,
        append_only_mode: bool,
        version_column_name: Option<&str>,
        schema_override: Option<&std::collections::HashMap<String, String>>,
    ) -> streamling_core::error::Result<String> {
        let has_custom_version_column = version_column_name.is_some();
        let version_column_name = version_column_name.unwrap_or("insert_time");

        let column_defs: Vec<String> = schema
            .fields()
            .iter()
            .filter(|field| {
                // In non-append-only mode, _gs_op is in the schema for downstream use
                // but should not be created as a ClickHouse column
                if !append_only_mode && field.name() == COLUMN_NAME_OP {
                    return false;
                }
                true
            })
            .map(|field| {
                let clickhouse_type = if let Some(overrides) = schema_override {
                    if let Some(override_type) = overrides.get(field.name()) {
                        info!(
                            "Applying schema override for column '{}': {}",
                            field.name(),
                            override_type
                        );
                        override_type.clone()
                    } else {
                        Self::arrow_field_to_clickhouse(field)
                    }
                } else {
                    Self::arrow_field_to_clickhouse(field)
                };

                format!("`{}` {}", field.name(), clickhouse_type)
            })
            .collect();

        let primary_key_clause = primary_keys.join(", ");

        if append_only_mode
            && has_custom_version_column
            && !schema
                .fields()
                .iter()
                .any(|field| field.name() == version_column_name)
        {
            return Err(streamling_err!(
                "ClickHouse sink version_column_name '{}' was not found in the input schema for table '{}'",
                version_column_name,
                table_name
            ));
        }

        if !append_only_mode && has_custom_version_column {
            warn!(
                "version_column_name '{}' is configured for table '{}' but has no effect in non-append-only mode",
                version_column_name, table_name
            );
        }

        let create_table_query = if append_only_mode {
            let mut append_only_columns = column_defs;
            if !has_custom_version_column {
                append_only_columns.push("`insert_time` DateTime DEFAULT now()".to_string());
            }

            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                {}
            ) ENGINE = ReplacingMergeTree({}, is_deleted)
            ORDER BY ({})",
                table_name,
                append_only_columns.join(",\n                "),
                version_column_name,
                primary_key_clause
            )
        } else {
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                {}
            ) ENGINE = ReplacingMergeTree()
            PRIMARY KEY ({})
            ORDER BY ({})",
                table_name,
                column_defs.join(",\n                "),
                primary_key_clause,
                primary_key_clause
            )
        };

        Ok(create_table_query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    #[test]
    fn test_schema_normalization() {
        // Create a schema with view types
        let schema = Schema::new(vec![
            Field::new("utf8_view_col", DataType::Utf8View, true),
            Field::new("binary_view_col", DataType::BinaryView, true),
            Field::new("regular_col", DataType::Int32, false),
        ]);

        // Normalize the schema
        let normalized = ClickHouseClient::normalize_schema_for_clickhouse(&schema);

        // Check that view types are converted
        assert_eq!(
            normalized.field(0).data_type(),
            &DataType::Utf8,
            "Utf8View should be converted to Utf8"
        );
        assert_eq!(
            normalized.field(1).data_type(),
            &DataType::Binary,
            "BinaryView should be converted to Binary"
        );
        assert_eq!(
            normalized.field(2).data_type(),
            &DataType::Int32,
            "Regular types should remain unchanged"
        );

        // Check that field names are preserved
        assert_eq!(normalized.field(0).name(), "utf8_view_col");
        assert_eq!(normalized.field(1).name(), "binary_view_col");
        assert_eq!(normalized.field(2).name(), "regular_col");
    }

    #[test]
    fn test_build_create_table_query_uses_custom_version_column() {
        let client = create_test_client("http://localhost:8123");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "insert_timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("is_deleted", DataType::UInt8, false),
        ]));

        let query = client
            .build_create_table_query(
                "test_output",
                &schema,
                vec!["id".to_string()],
                true,
                Some("insert_timestamp"),
                None,
            )
            .expect("query should build");

        assert!(
            query.contains("ENGINE = ReplacingMergeTree(insert_timestamp, is_deleted)"),
            "query should use the configured version column: {query}"
        );
        assert!(
            !query.contains("`insert_time` DateTime DEFAULT now()"),
            "query should not add the default insert_time column when a custom version column is configured: {query}"
        );
    }

    #[test]
    fn test_build_create_table_query_explicit_insert_time_version_column() {
        // Explicitly setting version_column_name to "insert_time" should be treated as
        // a custom column, not the default — so it must exist in schema and not be duplicated.
        let client = create_test_client("http://localhost:8123");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("insert_time", DataType::UInt64, false),
            Field::new("is_deleted", DataType::UInt8, false),
        ]));

        let query = client
            .build_create_table_query(
                "test_output",
                &schema,
                vec!["id".to_string()],
                true,
                Some("insert_time"),
                None,
            )
            .expect("query should build when insert_time is explicitly set and exists in schema");

        assert!(
            query.contains("ENGINE = ReplacingMergeTree(insert_time, is_deleted)"),
            "query should use insert_time as the version column: {query}"
        );
        // The schema already has insert_time — the auto-generated default column must not be added
        assert_eq!(
            query.matches("`insert_time`").count(),
            1,
            "insert_time should appear exactly once (no duplicate column): {query}"
        );
    }

    #[test]
    fn test_build_create_table_query_rejects_missing_custom_version_column() {
        let client = create_test_client("http://localhost:8123");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("is_deleted", DataType::UInt8, false),
        ]));

        let error = client
            .build_create_table_query(
                "test_output",
                &schema,
                vec!["id".to_string()],
                true,
                Some("insert_timestamp"),
                None,
            )
            .expect_err("query should fail when the version column is missing");

        assert!(
            error
                .to_string()
                .contains("version_column_name 'insert_timestamp' was not found"),
            "error should explain the missing configured version column: {error}"
        );
    }

    #[test]
    fn test_u256_i256_to_clickhouse() {
        // Test U256 with metadata
        let u256_field = Field::new("u256_col", DataType::FixedSizeBinary(32), false)
            .with_metadata(U256Type::metadata());
        let clickhouse_type = ClickHouseClient::arrow_field_to_clickhouse(&u256_field);
        assert_eq!(
            clickhouse_type, "UInt256",
            "U256 with metadata should be converted to UInt256"
        );

        // Test I256 with metadata
        let i256_field = Field::new("i256_col", DataType::FixedSizeBinary(32), false)
            .with_metadata(I256Type::metadata());
        let clickhouse_type = ClickHouseClient::arrow_field_to_clickhouse(&i256_field);
        assert_eq!(
            clickhouse_type, "Int256",
            "I256 with metadata should be converted to Int256"
        );

        // Test regular FixedSizeBinary(32) without metadata
        let regular_field = Field::new("fixed_col", DataType::FixedSizeBinary(32), false);
        let clickhouse_type = ClickHouseClient::arrow_field_to_clickhouse(&regular_field);
        assert_eq!(
            clickhouse_type, "FixedString(32)",
            "FixedSizeBinary(32) without metadata should be converted to FixedString(32)"
        );
    }

    #[test]
    fn test_normalize_batch_from_clickhouse_reverses_le_u256() {
        // ClickHouse emits UInt256 as FixedSizeBinary(32) without u256 metadata
        // and in little-endian. Without reversal, a logical 1 reads as 2^248 and
        // downstream u256_mul overflows. This test simulates one such column
        // arriving from ClickHouse and asserts the bytes come out big-endian
        // with the u256 extension metadata attached.
        use arrow::array::{Array, FixedSizeBinaryArray, Int64Array};
        use streamling_core::types::u256::{U256, bytes_to_u256, u256_to_bytes};

        // ClickHouse-side: FixedSizeBinary(32) without u256 metadata.
        let incoming_field = Arc::new(Field::new("balance", DataType::FixedSizeBinary(32), false));
        let id_field = Arc::new(Field::new("id", DataType::Int64, false));
        let incoming_schema = Arc::new(Schema::new(vec![
            id_field.as_ref().clone(),
            incoming_field.as_ref().clone(),
        ]));

        // LE-encoded payload: logical value 1 stored as [0x01, 0x00, ..., 0x00].
        let mut le_one = [0u8; 32];
        le_one[0] = 1;
        let mut le_two = [0u8; 32];
        le_two[0] = 2;
        let incoming_balance = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(le_one.to_vec()), Some(le_two.to_vec())].into_iter(),
            32,
        )
        .unwrap();
        let incoming_id = Int64Array::from(vec![1, 2]);
        let batch = RecordBatch::try_new(
            incoming_schema,
            vec![Arc::new(incoming_id), Arc::new(incoming_balance)],
        )
        .unwrap();

        // Target schema: streamling u256 with extension metadata.
        let target_balance = Arc::new(
            Field::new("balance", DataType::FixedSizeBinary(32), false)
                .with_metadata(U256Type::metadata()),
        );
        let target_schema = Arc::new(Schema::new(vec![
            id_field.as_ref().clone(),
            target_balance.as_ref().clone(),
        ]));

        let normalized =
            ClickHouseClient::normalize_batch_from_clickhouse(&batch, &target_schema).unwrap();

        // Output schema carries the u256 metadata.
        assert!(U256Type::is_u256_field(
            normalized.schema().field_with_name("balance").unwrap()
        ));

        // Output bytes are big-endian: a logical 1 lives at byte 31.
        let out = normalized
            .column_by_name("balance")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let mut row0 = [0u8; 32];
        row0.copy_from_slice(out.value(0));
        assert_eq!(bytes_to_u256(&row0), U256::from(1u64));
        let mut row1 = [0u8; 32];
        row1.copy_from_slice(out.value(1));
        assert_eq!(bytes_to_u256(&row1), U256::from(2u64));

        // Sanity: a real big-endian payload round-trips through u256_to_bytes
        // to the same bytes the normalizer produced.
        let expected = u256_to_bytes(&U256::from(1u64));
        assert_eq!(out.value(0), &expected[..]);
    }

    #[test]
    fn test_normalize_batch_from_clickhouse_skips_metadata_tagged_batch() {
        // Kafka/Avro batches arrive already-tagged with the u256 extension
        // metadata and already in big-endian. Reversing them would corrupt
        // every value. The normalizer must skip when the incoming field already
        // carries the metadata.
        use arrow::array::FixedSizeBinaryArray;
        use streamling_core::types::u256::{U256, bytes_to_u256, u256_to_bytes};

        let already_tagged = Arc::new(
            Field::new("balance", DataType::FixedSizeBinary(32), false)
                .with_metadata(U256Type::metadata()),
        );
        let schema = Arc::new(Schema::new(vec![already_tagged.as_ref().clone()]));
        let be_one = u256_to_bytes(&U256::from(1u64));
        let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(be_one.to_vec())].into_iter(),
            32,
        )
        .unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();

        let normalized =
            ClickHouseClient::normalize_batch_from_clickhouse(&batch, &schema).unwrap();
        let out = normalized
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let mut row = [0u8; 32];
        row.copy_from_slice(out.value(0));
        assert_eq!(bytes_to_u256(&row), U256::from(1u64));
    }

    #[test]
    fn test_normalize_batch_from_clickhouse_noop_without_bigint_fields() {
        // Target schemas without any u256/i256 field must pass batches through
        // verbatim — no column should be transformed.
        use arrow::array::Int64Array;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let arr = Int64Array::from(vec![1i64, 2, 3]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();

        let normalized =
            ClickHouseClient::normalize_batch_from_clickhouse(&batch, &schema).unwrap();
        let out = normalized
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(out.value(0), 1);
        assert_eq!(out.value(2), 3);
    }

    #[test]
    fn test_normalize_batch_from_clickhouse_preserves_list_field_names() {
        // Regression for a prod failure: the hybrid layer's compatibility check
        // deliberately tolerates differing List inner-field names ("item" from
        // ClickHouse vs "element" from Kafka/Avro). An earlier version of this
        // normalizer rebuilt every column against target_schema verbatim, which
        // forced an exact List-inner-field match and blew up with
        //   "expected List(Field name: element ...) but found List(Field name: item ...)".
        // The normalizer must only touch fields it actually transforms (u256/i256
        // byte reversal). Untouched columns keep their original Field.
        use arrow::array::{Array, FixedSizeBinaryArray, ListBuilder, StringBuilder};

        let incoming_list_field = Arc::new(Field::new("item", DataType::Utf8, false));
        let target_list_field = Arc::new(Field::new("element", DataType::Utf8, false));

        let incoming_schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(incoming_list_field.clone()),
            false,
        )]));
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(target_list_field.clone()),
            false,
        )]));

        let mut builder = ListBuilder::new(StringBuilder::new()).with_field(incoming_list_field);
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.append(true);
        let list_arr = builder.finish();

        let batch = RecordBatch::try_new(incoming_schema, vec![Arc::new(list_arr)]).unwrap();

        let normalized =
            ClickHouseClient::normalize_batch_from_clickhouse(&batch, &target_schema).unwrap();
        assert_eq!(normalized.num_rows(), 1);
        assert_eq!(normalized.num_columns(), 1);

        // Sanity: u256 metadata transform still fires even when other columns
        // have mismatched List inner names. Add a u256 column to the same batch
        // to prove both paths coexist.
        let incoming_with_u256_schema = Arc::new(Schema::new(vec![
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
            Field::new("balance", DataType::FixedSizeBinary(32), false),
        ]));
        let target_with_u256_schema = Arc::new(Schema::new(vec![
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("element", DataType::Utf8, false))),
                false,
            ),
            Field::new("balance", DataType::FixedSizeBinary(32), false)
                .with_metadata(U256Type::metadata()),
        ]));

        let mut tags_builder = ListBuilder::new(StringBuilder::new())
            .with_field(Arc::new(Field::new("item", DataType::Utf8, false)));
        tags_builder.values().append_value("a");
        tags_builder.append(true);
        let tags_arr = tags_builder.finish();

        let mut le_one = [0u8; 32];
        le_one[0] = 1;
        let balance_arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(le_one.to_vec())].into_iter(),
            32,
        )
        .unwrap();

        let mixed_batch = RecordBatch::try_new(
            incoming_with_u256_schema,
            vec![Arc::new(tags_arr), Arc::new(balance_arr)],
        )
        .unwrap();

        let normalized = ClickHouseClient::normalize_batch_from_clickhouse(
            &mixed_batch,
            &target_with_u256_schema,
        )
        .unwrap();

        // u256 column carries the metadata now.
        assert!(U256Type::is_u256_field(
            normalized.schema().field_with_name("balance").unwrap()
        ));

        // u256 bytes were reversed (LE -> BE).
        use streamling_core::types::u256::{U256, bytes_to_u256};
        let out = normalized
            .column_by_name("balance")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let mut row = [0u8; 32];
        row.copy_from_slice(out.value(0));
        assert_eq!(bytes_to_u256(&row), U256::from(1u64));
    }

    #[test]
    fn test_arrow_to_clickhouse_type_conversion() {
        // Helper to create a basic field for testing
        let field_bool = Field::new("test", DataType::Boolean, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_bool),
            "UInt8"
        );

        let field_int8 = Field::new("test", DataType::Int8, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_int8),
            "Int8"
        );

        let field_int16 = Field::new("test", DataType::Int16, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_int16),
            "Int16"
        );

        let field_int32 = Field::new("test", DataType::Int32, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_int32),
            "Int32"
        );

        let field_int64 = Field::new("test", DataType::Int64, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_int64),
            "Int64"
        );

        let field_uint8 = Field::new("test", DataType::UInt8, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_uint8),
            "UInt8"
        );

        let field_uint16 = Field::new("test", DataType::UInt16, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_uint16),
            "UInt16"
        );

        let field_uint32 = Field::new("test", DataType::UInt32, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_uint32),
            "UInt32"
        );
        let field_uint64 = Field::new("test", DataType::UInt64, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_uint64),
            "UInt64"
        );

        let field_float32 = Field::new("test", DataType::Float32, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_float32),
            "Float32"
        );

        let field_float64 = Field::new("test", DataType::Float64, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_float64),
            "Float64"
        );

        let field_utf8 = Field::new("test", DataType::Utf8, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_utf8),
            "String"
        );

        let field_large_utf8 = Field::new("test", DataType::LargeUtf8, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_large_utf8),
            "String"
        );

        let field_binary = Field::new("test", DataType::Binary, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_binary),
            "String"
        );

        let field_large_binary = Field::new("test", DataType::LargeBinary, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_large_binary),
            "String"
        );

        let field_date32 = Field::new("test", DataType::Date32, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_date32),
            "Date"
        );

        let field_date64 = Field::new("test", DataType::Date64, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_date64),
            "DateTime"
        );

        let field_ts_sec = Field::new("test", DataType::Timestamp(TimeUnit::Second, None), false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_ts_sec),
            "DateTime"
        );

        let field_ts_ms = Field::new(
            "test",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        );
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_ts_ms),
            "DateTime64(3)"
        );

        let field_ts_us = Field::new(
            "test",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        );
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_ts_us),
            "DateTime64(6)"
        );

        let field_ts_ns = Field::new(
            "test",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        );
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_ts_ns),
            "DateTime64(9)"
        );

        let field_dec128 = Field::new("test", DataType::Decimal128(18, 4), false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_dec128),
            "Decimal(18, 4)"
        );

        let field_dec256 = Field::new("test", DataType::Decimal256(38, 10), false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_dec256),
            "Decimal(38, 10)"
        );

        let list_field = Field::new("item", DataType::Int32, false);
        let field_list = Field::new("test", DataType::List(Arc::new(list_field.clone())), false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_list),
            "Array(Int32)"
        );

        let field_large_list = Field::new(
            "test",
            DataType::LargeList(Arc::new(list_field.clone())),
            false,
        );
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_large_list),
            "Array(Int32)"
        );

        let struct_fields = vec![
            Arc::new(Field::new("field1", DataType::Int32, false)),
            Arc::new(Field::new("field2", DataType::Utf8, false)),
        ];
        let field_struct = Field::new("test", DataType::Struct(struct_fields.into()), false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_struct),
            "Tuple(field1 Int32, field2 String)"
        );

        let field_dict = Field::new(
            "test",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        );
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_dict),
            "Nullable(String)"
        );

        let field_null = Field::new("test", DataType::Null, false);
        assert_eq!(
            ClickHouseClient::arrow_field_to_clickhouse(&field_null),
            "Nullable(Nothing)"
        );
    }

    #[tokio::test]
    async fn test_create_table_query_generation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));

        for field in schema.fields() {
            let ch_type = ClickHouseClient::arrow_field_to_clickhouse(field);
            assert!(!ch_type.is_empty());
        }
    }

    #[tokio::test]
    async fn test_create_table_returns_error_on_http_failure() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/")
            .match_query(mockito::Matcher::Any)
            .with_status(400)
            .with_body(
                "Code: 44. DB::Exception: Sorting key column 'id' is nullable. (ILLEGAL_COLUMN)",
            )
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("is_deleted", DataType::UInt8, false),
        ]));
        let result = client
            .create_table_if_not_exists(
                "test_table",
                &schema,
                vec!["id".to_string()],
                false,
                None,
                None,
            )
            .await;

        assert!(result.is_err(), "should return Err on HTTP 400");
        assert!(
            result.unwrap_err().to_string().contains("ILLEGAL_COLUMN"),
            "error message should contain the ClickHouse error"
        );
    }

    #[tokio::test]
    async fn test_create_table_returns_error_on_200_with_error_body() {
        // ClickHouse can return 200 OK with an error in the body for some DDL errors
        // (e.g., nullable column used in primary key).
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(
                "Code: 44. DB::Exception: Sorting key column 'id' is nullable. (ILLEGAL_COLUMN)",
            )
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("is_deleted", DataType::UInt8, false),
        ]));
        let result = client
            .create_table_if_not_exists(
                "test_table",
                &schema,
                vec!["id".to_string()],
                false,
                None,
                None,
            )
            .await;

        assert!(
            result.is_err(),
            "should return Err when 200 response body contains a ClickHouse error"
        );
        assert!(
            result.unwrap_err().to_string().contains("ILLEGAL_COLUMN"),
            "error message should contain the ClickHouse error"
        );
    }

    #[tokio::test]
    async fn test_create_table_returns_error_on_200_with_prefixed_error_body() {
        // Lock in that the error scan tolerates leading whitespace / prefix bytes
        // before the ClickHouse exception marker — protects against future regressions
        // where someone mistakenly anchors the regex with `^`.
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(
                "Garbage prefix\nCode: 44. DB::Exception: Sorting key column 'id' is nullable. (ILLEGAL_COLUMN)\n",
            )
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("is_deleted", DataType::UInt8, false),
        ]));
        let result = client
            .create_table_if_not_exists(
                "test_table",
                &schema,
                vec!["id".to_string()],
                false,
                None,
                None,
            )
            .await;

        assert!(
            result.is_err(),
            "should detect the error marker even when not at the start of the body"
        );
        assert!(
            result.unwrap_err().to_string().contains("ILLEGAL_COLUMN"),
            "error message should contain the ClickHouse error"
        );
    }

    #[tokio::test]
    async fn test_create_table_returns_error_on_200_with_non_db_exception() {
        // Cover other ClickHouse exception classes (Poco::Exception, DB::ErrnoException, etc.)
        // that the broader regex must catch.
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body("Code: 210. DB::NetException: Connection refused. (NETWORK_ERROR)")
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("is_deleted", DataType::UInt8, false),
        ]));
        let result = client
            .create_table_if_not_exists(
                "test_table",
                &schema,
                vec!["id".to_string()],
                false,
                None,
                None,
            )
            .await;

        assert!(
            result.is_err(),
            "should detect DB::NetException in 200 OK body"
        );
        assert!(
            result.unwrap_err().to_string().contains("NETWORK_ERROR"),
            "error message should contain the ClickHouse error"
        );
    }

    #[tokio::test]
    async fn test_create_table_succeeds_on_200_with_empty_body() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body("")
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("is_deleted", DataType::UInt8, false),
        ]));
        let result = client
            .create_table_if_not_exists(
                "test_table",
                &schema,
                vec!["id".to_string()],
                false,
                None,
                None,
            )
            .await;

        assert!(result.is_ok(), "should succeed on 200 with empty body");
    }

    #[test]
    fn test_scalar_to_clickhouse_literal() {
        assert_eq!(
            ClickHouseClient::scalar_to_clickhouse_literal(&ScalarValue::Int64(Some(42))),
            "42"
        );
        assert_eq!(
            ClickHouseClient::scalar_to_clickhouse_literal(&ScalarValue::Utf8(Some(
                "hello".to_string()
            ))),
            "'hello'"
        );
        assert_eq!(
            ClickHouseClient::scalar_to_clickhouse_literal(&ScalarValue::Utf8(Some(
                "it's".to_string()
            ))),
            "'it\\'s'"
        );
        assert_eq!(
            ClickHouseClient::scalar_to_clickhouse_literal(&ScalarValue::UInt64(Some(100))),
            "100"
        );
        assert_eq!(
            ClickHouseClient::scalar_to_clickhouse_literal(&ScalarValue::Boolean(Some(true))),
            "1"
        );
        assert_eq!(
            ClickHouseClient::scalar_to_clickhouse_literal(&ScalarValue::Boolean(Some(false))),
            "0"
        );
        // Backslash escaping
        assert_eq!(
            ClickHouseClient::scalar_to_clickhouse_literal(&ScalarValue::Utf8(Some(
                "test\\value".to_string()
            ))),
            "'test\\\\value'"
        );
        // Backslash + quote combo (the SQL injection vector)
        assert_eq!(
            ClickHouseClient::scalar_to_clickhouse_literal(&ScalarValue::Utf8(Some(
                "test\\'inject".to_string()
            ))),
            "'test\\\\\\'inject'"
        );
    }

    #[test]
    fn test_strip_gs_op_column() {
        use datafusion::arrow::array::{Int64Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_gs_op", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("c"), Some("c"), Some("d")])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap();

        let stripped = ClickHouseClient::strip_gs_op_column(&batch).unwrap();

        assert_eq!(stripped.num_columns(), 2);
        assert_eq!(stripped.schema().field(0).name(), "id");
        assert_eq!(stripped.schema().field(1).name(), "value");
        assert_eq!(stripped.num_rows(), 3);
        // _gs_op column should be gone
        assert!(stripped.column_by_name("_gs_op").is_none());
    }

    #[test]
    fn test_strip_gs_op_column_no_op() {
        // When there's no _gs_op column, the batch should be returned as-is
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1, 2])),
                Arc::new(arrow::array::StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap();

        let stripped = ClickHouseClient::strip_gs_op_column(&batch).unwrap();
        assert_eq!(stripped.num_columns(), 2);
        assert_eq!(stripped.num_rows(), 2);
    }

    #[test]
    fn test_schema_override_datetime_detection() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert("timestamp".to_string(), "DateTime".to_string());
        overrides.insert("created_at".to_string(), "DateTime64(3)".to_string());
        overrides.insert(
            "modified_at".to_string(),
            "DateTime64(6) CODEC(Delta, ZSTD)".to_string(),
        );
        overrides.insert("user_id".to_string(), "UInt64".to_string());

        let datetime_columns = schema_overrides::get_datetime_columns(&overrides);

        assert!(datetime_columns.contains("timestamp"));
        assert!(datetime_columns.contains("created_at"));
        assert!(datetime_columns.contains("modified_at"));
        assert!(
            !datetime_columns.contains("user_id"),
            "Non-DateTime columns should not be included"
        );
        assert_eq!(datetime_columns.len(), 3);
    }

    #[test]
    fn test_schema_override_requires_conversion() {
        assert!(schema_overrides::requires_conversion("DateTime"));
        assert!(schema_overrides::requires_conversion("datetime"));
        assert!(schema_overrides::requires_conversion("DateTime64(3)"));
        assert!(schema_overrides::requires_conversion(
            "DateTime CODEC(Delta, ZSTD)"
        ));
        assert!(schema_overrides::requires_conversion(
            "DateTime64(6) CODEC(Delta, ZSTD)"
        ));

        assert!(!schema_overrides::requires_conversion("UInt64"));
        assert!(!schema_overrides::requires_conversion("String"));
        assert!(!schema_overrides::requires_conversion("FixedString(32)"));
        assert!(!schema_overrides::requires_conversion("Int64"));
    }

    #[test]
    fn test_schema_override_validation_datetime() {
        // Valid conversions
        let field_int64 = Field::new("ts", DataType::Int64, false);
        assert!(schema_overrides::validate_conversion(&field_int64, "DateTime64(3)").is_ok());

        let field_uint64 = Field::new("ts", DataType::UInt64, false);
        assert!(schema_overrides::validate_conversion(&field_uint64, "DateTime").is_ok());

        let field_int32 = Field::new("ts", DataType::Int32, false);
        assert!(schema_overrides::validate_conversion(&field_int32, "DateTime").is_ok());

        let field_uint32 = Field::new("ts", DataType::UInt32, false);
        assert!(schema_overrides::validate_conversion(&field_uint32, "DateTime").is_ok());

        let field_timestamp = Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false);
        assert!(schema_overrides::validate_conversion(&field_timestamp, "DateTime64(3)").is_ok());

        // Invalid conversions
        let field_string = Field::new("ts", DataType::Utf8, false);
        assert!(schema_overrides::validate_conversion(&field_string, "DateTime").is_err());

        let field_float = Field::new("ts", DataType::Float64, false);
        assert!(schema_overrides::validate_conversion(&field_float, "DateTime").is_err());

        // Non-DateTime overrides should always be valid (DDL-only)
        let field_int64_non_dt = Field::new("id", DataType::Int64, false);
        assert!(schema_overrides::validate_conversion(&field_int64_non_dt, "UInt64").is_ok());

        let field_string_non_dt = Field::new("hash", DataType::Utf8, false);
        assert!(
            schema_overrides::validate_conversion(&field_string_non_dt, "FixedString(32)").is_ok()
        );
    }

    #[test]
    fn test_schema_override_datetime_conversion_creation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts_int64", DataType::Int64, false),
            Field::new("ts_uint64", DataType::UInt64, false),
        ]));

        // Test Int64 -> Timestamp conversion
        let field_int64 = &schema.fields()[0];
        let conversion = schema_overrides::create_datetime_conversion(field_int64, 0, &schema);
        assert!(
            conversion.is_ok(),
            "Int64 should be convertible to DateTime"
        );

        // Test UInt64 -> Timestamp conversion
        let field_uint64 = &schema.fields()[1];
        let conversion = schema_overrides::create_datetime_conversion(field_uint64, 1, &schema);
        assert!(
            conversion.is_ok(),
            "UInt64 should be convertible to DateTime"
        );

        // Test invalid type
        let schema_invalid = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let field_string = &schema_invalid.fields()[0];
        let conversion =
            schema_overrides::create_datetime_conversion(field_string, 0, &schema_invalid);
        assert!(
            conversion.is_err(),
            "String should not be convertible to DateTime"
        );
    }

    fn create_test_client(url: &str) -> ClickHouseClient {
        create_test_client_with_compression(url, ClickHouseCompression::None)
    }

    fn create_test_client_with_compression(
        url: &str,
        compression: ClickHouseCompression,
    ) -> ClickHouseClient {
        let config = ClickHouseConfig {
            url: url.to_string(),
            database: "test_db".to_string(),
            user: "default".to_string(),
            password: "".to_string(),
            compression,
            compression_level: GzipCompressionLevel::default(),
        };
        ClickHouseClient::new(config)
    }

    fn create_test_arrow_ipc_bytes(rows: &[i64]) -> Vec<u8> {
        use arrow::array::Int64Array;
        use arrow::ipc::writer::FileWriter;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(rows.to_vec()))],
        )
        .unwrap();

        let mut buf = Vec::new();
        let mut writer = FileWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        buf
    }

    #[tokio::test]
    async fn test_source_run_returns_error_on_server_error() {
        let mut server = mockito::Server::new_async().await;

        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Any)
            .with_status(500)
            .with_body("DB::Exception: timeout")
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = ClickHouseSourceExec::run(client, "SELECT id FROM test_table", None).await;

        assert!(
            result.is_err(),
            "run() should return Err on HTTP 500 — this error triggers the retry loop in execute()"
        );
    }

    #[tokio::test]
    async fn test_source_run_returns_error_on_network_failure() {
        let client = create_test_client("http://127.0.0.1:1");
        let result = ClickHouseSourceExec::run(client, "SELECT id FROM test_table", None).await;

        assert!(
            result.is_err(),
            "run() should return Err on connection refused — this error triggers the retry loop"
        );
    }

    #[tokio::test]
    async fn test_source_run_succeeds_with_valid_arrow_response() {
        let mut server = mockito::Server::new_async().await;

        let arrow_bytes = create_test_arrow_ipc_bytes(&[1, 2, 3]);
        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(arrow_bytes)
            .create_async()
            .await;

        let client = create_test_client(&server.url());
        let result = ClickHouseSourceExec::run(client, "SELECT id FROM test_table", None).await;
        assert!(result.is_ok(), "run() should succeed with valid Arrow IPC");

        let mut stream = result.unwrap();
        let mut row_count = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.expect("batch should be valid");
            row_count += batch.num_rows();
        }
        assert_eq!(row_count, 3, "should have received 3 rows");
    }

    #[tokio::test]
    async fn test_scan_seeds_exec_split_with_initial_start_args() {
        use datafusion::execution::context::SessionContext;
        use streamling_state::StateOperatorBackendFactory;
        use streamling_state::in_memory::InMemoryStateOperatorBackendFactory;

        let initial_split_args = vec![
            ScalarValue::Int64(Some(44_608_123)),
            ScalarValue::Utf8(Some("log_abc_194".to_string())),
        ];

        let pagination_config = ClickHousePaginationConfig {
            sorting_keys: vec!["block_number".to_string(), "id".to_string()],
            page_size: 1000,
        };
        let query_builder = ClickHouseQueryBuilder::of(
            "test_table".to_string(),
            vec!["block_number".to_string(), "id".to_string()],
            None,
            Some(pagination_config),
        );

        let state_backend_factory = InMemoryStateOperatorBackendFactory::new()
            .expect("failed to create in-memory state backend factory");
        let state_store = Arc::new(ClickHouseSourceStateStore {
            reference_name: "test_source".to_string(),
            state_backend: state_backend_factory.create::<ClickHouseSourceSplit>("test_ns"),
        });

        let provider = ClickHouseTableProvider {
            reference_name: "test_source".to_string(),
            schema: Arc::new(Schema::new(vec![
                Field::new("block_number", DataType::Int64, false),
                Field::new("id", DataType::Utf8, false),
            ])),
            client: ClickHouseClient::new(ClickHouseConfig {
                url: "http://localhost:8123".to_string(),
                database: "default".to_string(),
                user: "default".to_string(),
                password: "".to_string(),
                compression: ClickHouseCompression::None,
                compression_level: GzipCompressionLevel::default(),
            }),
            source_params: Some(SourceParams {
                query_builder,
                sorting_keys: vec!["block_number".to_string(), "id".to_string()],
                initial_split_args: initial_split_args.clone(),
                state_store,
                datafusion_buffer_size: 16,
                block_range: 1_000_000,
                table_name: "test_table".to_string(),
                has_persisted_split: false,
            }),
            sink_params: None,
            metric_metadata_id: "test_metric".to_string(),
        };

        let session = SessionContext::new();
        let session_state = session.state();
        let plan = provider
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan should succeed");

        let source_exec = plan
            .as_any()
            .downcast_ref::<ClickHouseSourceExec>()
            .expect("scan should return ClickHouseSourceExec");

        assert_eq!(
            source_exec.split.args, initial_split_args,
            "scan should seed execution split with initial start args"
        );
    }

    #[tokio::test]
    async fn test_source_retry_loop_retries_on_failure_then_succeeds() {
        let mut server = mockito::Server::new_async().await;

        let arrow_bytes = create_test_arrow_ipc_bytes(&[10, 20, 30]);

        let _fail_mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Any)
            .with_status(500)
            .with_body("DB::Exception: timeout")
            .expect_at_most(3)
            .create_async()
            .await;

        let _success_mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Any)
            .with_status(200)
            .with_body(arrow_bytes)
            .create_async()
            .await;

        let client = create_test_client(&server.url());

        // Simulate the retry loop from execute() — retry on Err, break on Ok
        let mut query_retry_attempts: u32 = 0;
        let mut query_retry_backoff_ms: u64 = 100;
        let mut succeeded = false;
        let max_test_attempts = 10;

        for _ in 0..max_test_attempts {
            match ClickHouseSourceExec::run(client.clone(), "SELECT id FROM test_table", None).await
            {
                Ok(mut stream) => {
                    query_retry_attempts = 0;
                    query_retry_backoff_ms = 100;

                    let mut row_count = 0;
                    while let Some(batch_result) = stream.next().await {
                        let batch = batch_result.expect("batch should be valid");
                        row_count += batch.num_rows();
                    }
                    assert_eq!(row_count, 3, "should have received 3 rows on success");
                    succeeded = true;
                    break;
                }
                Err(_) => {
                    query_retry_attempts += 1;
                    let jitter = (query_retry_attempts as u64 % 100) * 7;
                    let sleep_ms = std::cmp::min(30_000u64, query_retry_backoff_ms + jitter);
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                    query_retry_backoff_ms =
                        std::cmp::min(query_retry_backoff_ms.saturating_mul(2), 30_000);
                    continue;
                }
            }
        }

        assert!(
            succeeded,
            "retry loop should eventually succeed after transient failures"
        );
        assert_eq!(
            query_retry_attempts, 0,
            "retry state should be reset after success"
        );
        assert_eq!(
            query_retry_backoff_ms, 100,
            "backoff should be reset after success"
        );
    }

    #[test]
    fn test_source_retry_backoff_increases_exponentially() {
        let mut backoff_ms: u64 = 100;
        let expected = [100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 30000];

        for (attempt, &expected_backoff) in expected.iter().enumerate() {
            assert_eq!(
                backoff_ms, expected_backoff,
                "backoff at attempt {} should be {}ms",
                attempt, expected_backoff
            );
            backoff_ms = std::cmp::min(backoff_ms.saturating_mul(2), 30_000);
        }

        // Verify cap is maintained
        for _ in 0..5 {
            backoff_ms = std::cmp::min(backoff_ms.saturating_mul(2), 30_000);
            assert_eq!(backoff_ms, 30_000, "backoff should stay capped at 30s");
        }
    }

    #[test]
    fn test_source_retry_jitter_calculation() {
        for attempt in 1u32..=10 {
            let jitter = (attempt as u64 % 100) * 7;
            assert!(jitter < 700, "jitter should be bounded");

            let backoff_ms: u64 = 100;
            let sleep_ms = std::cmp::min(30_000u64, backoff_ms + jitter);
            assert!(
                sleep_ms >= backoff_ms,
                "sleep should be at least the base backoff"
            );
            assert!(sleep_ms <= 30_000, "sleep should not exceed 30s cap");
        }
    }

    #[test]
    fn test_source_retry_state_resets_on_success() {
        // Simulate accumulated retry state after several failures
        let mut attempts: u32 = 0;
        let mut backoff_ms: u64 = 100;

        for _ in 0..5 {
            attempts += 1;
            backoff_ms = std::cmp::min(backoff_ms.saturating_mul(2), 30_000);
        }
        assert_eq!(attempts, 5);
        assert_eq!(backoff_ms, 3200);

        // Simulate success: reset state (mirrors the Ok branch in execute())
        attempts = 0;
        backoff_ms = 100;

        assert_eq!(attempts, 0, "attempts should reset to 0 on success");
        assert_eq!(backoff_ms, 100, "backoff should reset to 100ms on success");
    }

    #[test]
    fn test_scalar_to_i128() {
        assert_eq!(scalar_to_i128(&ScalarValue::Int8(Some(42))), Some(42));
        assert_eq!(scalar_to_i128(&ScalarValue::Int16(Some(1000))), Some(1000));
        assert_eq!(
            scalar_to_i128(&ScalarValue::Int32(Some(100_000))),
            Some(100_000)
        );
        assert_eq!(
            scalar_to_i128(&ScalarValue::Int64(Some(1_000_000))),
            Some(1_000_000)
        );
        assert_eq!(
            scalar_to_i128(&ScalarValue::UInt32(Some(50_000))),
            Some(50_000)
        );
        assert_eq!(scalar_to_i128(&ScalarValue::UInt64(Some(999))), Some(999));
        assert_eq!(
            scalar_to_i128(&ScalarValue::Utf8(Some("5".to_string()))),
            Some(5)
        );
        assert_eq!(
            scalar_to_i128(&ScalarValue::Utf8(Some("hello".to_string()))),
            None
        );
        assert_eq!(scalar_to_i128(&ScalarValue::Int64(None)), None);
    }

    #[test]
    fn test_scalar_to_i128_large_uint64() {
        let large = u64::MAX;
        assert_eq!(
            scalar_to_i128(&ScalarValue::UInt64(Some(large))),
            Some(large as i128)
        );
        let above_i64_max = (i64::MAX as u64) + 1;
        assert_eq!(
            scalar_to_i128(&ScalarValue::UInt64(Some(above_i64_max))),
            Some(above_i64_max as i128)
        );
    }

    #[test]
    fn test_i128_to_scalar_like() {
        let template_i64 = ScalarValue::Int64(Some(0));
        assert_eq!(
            i128_to_scalar_like(42, &template_i64),
            ScalarValue::Int64(Some(42))
        );

        let template_u64 = ScalarValue::UInt64(Some(0));
        assert_eq!(
            i128_to_scalar_like(100, &template_u64),
            ScalarValue::UInt64(Some(100))
        );

        let template_i32 = ScalarValue::Int32(Some(0));
        assert_eq!(
            i128_to_scalar_like(500, &template_i32),
            ScalarValue::Int32(Some(500))
        );
    }

    #[test]
    fn test_i128_to_scalar_like_large_uint64() {
        let large: i128 = (i64::MAX as i128) + 1_000_000;
        let template = ScalarValue::UInt64(Some(0));
        let result = i128_to_scalar_like(large, &template);
        assert_eq!(result, ScalarValue::UInt64(Some(large as u64)));
    }

    #[test]
    fn test_is_timeout_error() {
        let timeout_err = DataFusionError::Execution("request timeout".to_string());
        assert!(is_timeout_error(&timeout_err));

        let timeout_err2 = DataFusionError::Execution("connection timed out".to_string());
        assert!(is_timeout_error(&timeout_err2));

        let ch_timeout = DataFusionError::Execution("DB::Exception: timeout".to_string());
        assert!(is_timeout_error(&ch_timeout));

        let other_err = DataFusionError::Execution("connection refused".to_string());
        assert!(!is_timeout_error(&other_err));
    }

    /// With `compression = Gzip`, `send_arrow_batch` must set
    /// `Content-Encoding: gzip` on the outbound request. The mock only matches
    /// when that header is present, so the request would otherwise fall
    /// through to mockito's default 501 and the call would error out.
    #[tokio::test]
    async fn test_send_arrow_batch_sets_gzip_content_encoding() {
        use arrow::array::Int64Array;

        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .match_query(mockito::Matcher::Any)
            .match_header("content-encoding", "gzip")
            .with_status(200)
            .with_body("")
            .expect(1)
            .create_async()
            .await;

        let client =
            create_test_client_with_compression(&server.url(), ClickHouseCompression::Gzip);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        client
            .send_arrow_batch("test_table", &batch, &schema)
            .await
            .expect("send_arrow_batch should succeed when mock matches the gzip header");

        mock.assert_async().await;
    }

    /// With `compression = None`, `send_arrow_batch` must NOT set a
    /// `Content-Encoding` header. `Matcher::Missing` rejects the request if
    /// the header is present, which would surface as a 501 and an error.
    #[tokio::test]
    async fn test_send_arrow_batch_omits_content_encoding_when_compression_off() {
        use arrow::array::Int64Array;

        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .match_query(mockito::Matcher::Any)
            .match_header("content-encoding", mockito::Matcher::Missing)
            .with_status(200)
            .with_body("")
            .expect(1)
            .create_async()
            .await;

        let client = create_test_client(&server.url()); // defaults to compression: None
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        client
            .send_arrow_batch("test_table", &batch, &schema)
            .await
            .expect("send_arrow_batch should succeed without any content-encoding header");

        mock.assert_async().await;
    }

    /// Benchmark: encode representative Arrow batches at several sizes, then
    /// gzip each at three compression levels. Prints raw size, compressed size,
    /// ratio, and wall-clock time per (size, level). Run with:
    ///   cargo test --release -p streamling-connectors \
    ///     bench_gzip_arrow_batch -- --ignored --nocapture
    #[test]
    #[ignore = "benchmark; run with --ignored --nocapture"]
    fn bench_gzip_arrow_batch() {
        use arrow::array::{Float64Array, Int64Array, StringArray};
        use arrow::ipc::writer::FileWriter as IpcFileWriter;
        use std::io::Write;
        use std::time::Instant;

        // Row width: 8 + 8 + 8 + 66 ≈ 90 bytes/row.
        // Hash column is hex-encoded fixed-length strings, simulating blockchain workloads.
        let row_counts = [50_usize, 500, 5_000, 60_000];

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("block_number", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("hash", DataType::Utf8, false),
        ]));

        let levels = [
            ("fast (1)", flate2::Compression::fast()),
            ("default (6)", flate2::Compression::default()),
            ("best (9)", flate2::Compression::best()),
        ];

        for &rows in &row_counts {
            let ids: Vec<i64> = (0..rows as i64).collect();
            let blocks: Vec<i64> = (0..rows as i64).map(|i| 18_000_000 + i / 10).collect();
            let amounts: Vec<f64> = (0..rows).map(|i| (i as f64) * 1.0001).collect();
            let hashes: Vec<String> = (0..rows)
                .map(|i| format!("0x{:064x}", i.wrapping_mul(0x9e3779b97f4a7c15)))
                .collect();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(blocks)),
                    Arc::new(Float64Array::from(amounts)),
                    Arc::new(StringArray::from(hashes)),
                ],
            )
            .unwrap();

            // Encode once outside the timed compression loop so we measure just gzip.
            let mut arrow_buf = Vec::new();
            let encode_start = Instant::now();
            {
                let mut writer = IpcFileWriter::try_new(&mut arrow_buf, &schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }
            let encode_elapsed = encode_start.elapsed();
            let raw_size = arrow_buf.len();

            println!();
            println!(
                "=== {} rows | raw {:.2} KiB | encode {:?} ===",
                rows,
                raw_size as f64 / 1024.0,
                encode_elapsed,
            );

            for (label, level) in levels {
                // Warm up + measure best of 5 to dampen noise on the small cases.
                let mut best = std::time::Duration::MAX;
                let mut compressed_size = 0usize;
                for _ in 0..5 {
                    let start = Instant::now();
                    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), level);
                    encoder.write_all(&arrow_buf).unwrap();
                    let compressed = encoder.finish().unwrap();
                    let elapsed = start.elapsed();
                    if elapsed < best {
                        best = elapsed;
                    }
                    compressed_size = compressed.len();
                }
                let ratio = compressed_size as f64 / raw_size as f64;
                let mb_per_s = (raw_size as f64 / 1_048_576.0) / best.as_secs_f64();
                println!(
                    "  {:<12} {:>9.2} KiB  ratio {:.3}  time {:>9.2?}  ({:>7.1} MB/s)",
                    label,
                    compressed_size as f64 / 1024.0,
                    ratio,
                    best,
                    mb_per_s,
                );
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClickHouseSourceSplit {
    pub sorting_keys: Vec<String>,
    #[serde(with = "streamling_core::serde::arrow_scalar_value")]
    pub args: Vec<ScalarValue>,
}

impl ClickHouseSourceSplit {
    pub fn update_args(&mut self, args: Vec<ScalarValue>) {
        self.args = args;
    }
}

#[derive(Debug)]
struct ClickHouseSourceStateStore {
    reference_name: String,
    state_backend: Arc<dyn StateOperatorBackend<ClickHouseSourceSplit>>,
}

impl ClickHouseSourceStateStore {
    const VERSION: &str = "v1";

    pub async fn save_split(&self, split: ClickHouseSourceSplit) -> Result<(), StateBackendError> {
        debug!(
            "Saving ClickHouseSourceSplit for {}: {:?}",
            self.reference_name, split
        );
        self.state_backend.put(self.state_key(), split).await
    }

    pub async fn load_split(&self) -> Option<ClickHouseSourceSplit> {
        debug!("Loading ClickHouseSourceSplit for {}", self.reference_name);
        self.state_backend.get(self.state_key()).await.unwrap()
    }

    fn state_key(&self) -> StateKey {
        StateKey::from(format!(
            "clickhouse_source:{}:{}",
            self.reference_name,
            Self::VERSION
        ))
    }
}

/// Schema override conversion logic
mod schema_overrides {
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion::error::Result;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{CastExpr, Column};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use streamling_core::streamling_err;

    /// Determines if a schema override requires data type conversion
    /// Currently only DateTime/DateTime64 overrides trigger conversion
    pub fn requires_conversion(override_type: &str) -> bool {
        override_type.to_lowercase().contains("datetime")
    }

    /// Identifies columns that need DateTime conversion
    pub fn get_datetime_columns(schema_overrides: &HashMap<String, String>) -> HashSet<String> {
        schema_overrides
            .iter()
            .filter(|(_, override_type)| requires_conversion(override_type))
            .map(|(col_name, _)| col_name.clone())
            .collect()
    }

    /// Creates a CAST expression to convert a column to Timestamp for DateTime override
    pub fn create_datetime_conversion(
        field: &Field,
        col_idx: usize,
        _schema: &arrow::datatypes::SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let source_type = field.data_type();

        // Determine target timestamp type based on source
        let target_type = match source_type {
            // Int64 is treated as seconds (Unix timestamp)
            DataType::Int64 | DataType::UInt64 => DataType::Timestamp(TimeUnit::Second, None),
            // Int32 also treated as seconds
            DataType::Int32 | DataType::UInt32 => DataType::Timestamp(TimeUnit::Second, None),
            // Already a timestamp - pass through (may need precision adjustment)
            DataType::Timestamp(_, _) => source_type.clone(),
            _ => {
                return Err(streamling_err!(
                    "cannot convert column '{}' of type {:?} to DateTime. \
                     Only Int32, Int64, UInt32, UInt64, and Timestamp types are supported",
                    field.name(),
                    source_type
                )
                .into());
            }
        };

        let col_expr = Arc::new(Column::new(field.name(), col_idx));
        let cast_expr = CastExpr::new(col_expr, target_type, None);

        Ok(Arc::new(cast_expr))
    }

    /// Validates that a conversion is possible
    pub fn validate_conversion(field: &Field, override_type: &str) -> Result<()> {
        if requires_conversion(override_type) {
            // For DateTime conversions, validate source type
            match field.data_type() {
                DataType::Int64
                | DataType::UInt64
                | DataType::Int32
                | DataType::UInt32
                | DataType::Timestamp(_, _) => Ok(()),
                _ => Err(streamling_err!(
                    "cannot apply DateTime override to column '{}' of type {:?}. \
                     Only Int32, Int64, UInt32, UInt64, and Timestamp types can be converted to DateTime",
                    field.name(),
                    field.data_type()
                )
                .into()),
            }
        } else {
            // Non-DateTime overrides are DDL-only (ClickHouse handles the conversion)
            Ok(())
        }
    }
}
