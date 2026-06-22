//! File source backed by DataFusion's file readers.
//!
//! Two modes share schema inference, object-store registration, and the
//! `_gs_op = 'i'` change-operation contract:
//!
//! - **Bounded** ([`build_bounded_file_source_provider`]) wraps DataFusion's
//!   [`ListingTable`]: it lists the matching files once at startup, reads them to
//!   EOF (inferring the schema and Hive-style partition columns), and the job
//!   terminates on its own.
//! - **Continuous** ([`FileSourceTableProvider`]) keeps watching the path: a poll
//!   loop lists the prefix every `poll_interval`, ingests files whose
//!   `last_modified` exceeds a persisted [`FileWatermark`], and never
//!   self-terminates.
//!
//! Because file reads are append-only, a constant `_gs_op = 'i'` column is
//! synthesized when the inferred schema lacks it. Files that already carry
//! `_gs_op` pass through unwrapped, but the column must be a non-nullable Utf8
//! column to match what downstream consumers expect.

use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{Column, DataFusionError, ScalarValue};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
};
use datafusion::datasource::physical_plan::{
    CsvSource, FileGroup, FileScanConfigBuilder, FileSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{TableProvider, TableType, ViewTable, provider_as_source};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, lit};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    project_schema,
};
use futures::StreamExt;
use serde_derive::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{debug, info};

use streamling_core::data::{COLUMN_NAME_OP, RowKind};
use streamling_core::error::Result;
use streamling_core::session::SessionManager;
use streamling_core::topology::FileSourceFormat;
use streamling_core::{streamling_user_bail, streamling_user_err};
use streamling_state::{StateKey, StateOperatorBackend};

/// Maps the source's format enum to a DataFusion [`FileFormat`]. Shared by the
/// bounded and continuous builders.
fn file_format_for(format: FileSourceFormat) -> Arc<dyn FileFormat> {
    match format {
        FileSourceFormat::Parquet => Arc::new(ParquetFormat::default()),
        FileSourceFormat::Csv => Arc::new(CsvFormat::default()),
        FileSourceFormat::Json => Arc::new(JsonFormat::default()),
        FileSourceFormat::Avro => Arc::new(AvroFormat),
    }
}

/// The DataFusion [`FileSource`] (per-format reader) the continuous source feeds
/// to its runtime `FileScanConfig`.
fn file_source_for(
    format: FileSourceFormat,
    file_format: &Arc<dyn FileFormat>,
) -> Arc<dyn FileSource> {
    match format {
        // CsvSource::default() has incorrect defaults, passing proper ones from CsvFormat::default()
        FileSourceFormat::Csv => {
            const DEFAULT_HAS_HEADER: bool = true;
            let default_options = CsvFormat::default().options().clone();
            Arc::new(CsvSource::new(
                default_options.has_header.unwrap_or(DEFAULT_HAS_HEADER),
                default_options.delimiter,
                default_options.quote,
            ))
        }
        _ => file_format.file_source(),
    }
}

/// Registers the object store for a remote path scheme on the session. Local
/// paths use the default object store; remote schemes need one registered.
///
/// Each cloud builder's `from_env()` folds in credentials/region/endpoint from
/// the environment (it lowercases keys before parsing, which the generic
/// `parse_url` path does not — so `from_env` is required for `AWS_*`).
fn register_object_store_for_url(
    table_url: &ListingTableUrl,
    path: &str,
    session_manager: &SessionManager,
) -> Result<()> {
    match table_url.scheme() {
        "file" => {}
        "s3" | "s3a" => {
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_url(table_url.as_str())
                .build()
                .map_err(|e| {
                    streamling_user_err!(
                        "failed to create S3 object store for path '{}': {}",
                        path,
                        e
                    )
                })?;
            session_manager
                .session_context()
                .register_object_store(table_url.object_store().as_ref(), Arc::new(store));
        }
        "gs" => {
            let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_url(table_url.as_str())
                .build()
                .map_err(|e| {
                    streamling_user_err!(
                        "failed to create GCS object store for path '{}': {}",
                        path,
                        e
                    )
                })?;
            session_manager
                .session_context()
                .register_object_store(table_url.object_store().as_ref(), Arc::new(store));
        }
        other => {
            streamling_user_bail!(
                "file source path '{}' uses unsupported scheme '{}'. \
                 Supported schemes: local paths, s3://, gs://",
                path,
                other
            );
        }
    }
    Ok(())
}

/// Builds the runtime provider for a **bounded** `file` source backed by
/// DataFusion's [`ListingTable`]. The schema is inferred from the files at
/// startup. Because file reads are append-only, a constant `_gs_op = 'i'` column
/// is appended (via a [`ViewTable`]) when the inferred schema lacks it, so the
/// source composes with sinks that require the change-operation column. Files
/// that already carry `_gs_op` pass through unwrapped, but the column must be a
/// non-nullable Utf8 column to match what downstream consumers expect.
///
/// Supported path schemes: local paths, `s3://`, and `gs://`. Hive-style
/// partition columns in the path (e.g. `…/dt=2024-01-01/`) are inferred and added
/// to the schema.
pub async fn build_bounded_file_source_provider(
    reference_name: &str,
    path: &str,
    format: FileSourceFormat,
    session_manager: &SessionManager,
) -> Result<Arc<dyn TableProvider>> {
    let table_url = ListingTableUrl::parse(path)?;
    register_object_store_for_url(&table_url, path, session_manager)?;

    let file_format = file_format_for(format);

    let state = session_manager.session_state();
    let listing_options = ListingOptions::new(file_format);
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        // Surface Hive-style partition columns (e.g. `dt=2024-01-01/`) in the
        // schema, matching DataFusion's dynamic-file behavior. A no-op for flat
        // directories.
        .infer_partitions_from_path(&state)
        .await?
        .infer_schema(&state)
        .await?;
    let listing_table = Arc::new(ListingTable::try_new(config)?);

    let schema = listing_table.schema();

    // Schema inference over a path that matches no files yields an empty schema
    // and a silent zero-row source; fail fast instead.
    if schema.fields().is_empty() {
        streamling_user_bail!(
            "file source '{}': no files matching format {:?} found at path '{}'. \
             Check the path and that the files use the format's extension.",
            reference_name,
            format,
            path
        );
    }

    if let Ok(op_field) = schema.field_with_name(COLUMN_NAME_OP) {
        // Downstream consumers (RowKind::extract_row_kinds_from_batch) require a
        // non-nullable Utf8 op column; reject anything else rather than panicking
        // later in a sink.
        if op_field.data_type() != &DataType::Utf8 || op_field.is_nullable() {
            streamling_user_bail!(
                "file source '{}': column '{}' must be a non-nullable Utf8 column, \
                 found type {:?} (nullable: {})",
                reference_name,
                COLUMN_NAME_OP,
                op_field.data_type(),
                op_field.is_nullable()
            );
        }
        return Ok(listing_table);
    }

    // Reference each column by its exact name. `col(name)` would parse the name as a
    // SQL identifier and lowercase it (so `fieldName` -> `fieldname`), breaking case-sensitive
    // columns; `Column::new_unqualified` stores the name verbatim.
    let mut projection: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| Expr::Column(Column::new_unqualified(f.name())))
        .collect();
    projection.push(lit(ScalarValue::Utf8(Some(RowKind::Insert.to_str()))).alias(COLUMN_NAME_OP));
    let plan = LogicalPlanBuilder::scan(
        reference_name,
        provider_as_source(listing_table as Arc<dyn TableProvider>),
        None,
    )?
    .project(projection)?
    .build()?;

    Ok(Arc::new(ViewTable::new(plan, None)))
}

/// Persisted discovery position for the continuous file source: the maximum
/// object `last_modified` (epoch milliseconds) already ingested. Files with a
/// greater `last_modified` are (re)processed on the next poll.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct FileWatermark {
    pub last_modified_ms: i64,
}

fn watermark_state_key(reference_name: &str) -> StateKey {
    StateKey::from(format!("{reference_name}:watermark"))
}

/// Whether a listed object should be ingested: it must carry the format's
/// extension and be newer than the watermark.
fn should_process(
    location_extension: Option<&str>,
    last_modified_ms: i64,
    file_extension: &str,
    watermark_ms: i64,
) -> bool {
    location_extension == Some(file_extension) && last_modified_ms > watermark_ms
}

/// Continuous (unbounded) file source. Polls `table_url` every `poll_interval`,
/// ingesting files whose `last_modified` exceeds the persisted [`FileWatermark`],
/// and emits their rows with a synthesized `_gs_op = 'i'` (unless the files
/// already carry a valid `_gs_op`). Never self-terminates.
pub struct FileSourceTableProvider {
    reference_name: String,
    table_url: ListingTableUrl,
    file_source: Arc<dyn FileSource>,
    file_extension: String,
    /// Columns read from the files (includes `_gs_op` only if the files provide it).
    file_schema: SchemaRef,
    /// Published schema: `file_schema` plus a synthesized `_gs_op` when absent.
    full_schema: SchemaRef,
    /// Whether `_gs_op` must be synthesized (false when the files already have it).
    append_op: bool,
    poll_interval: Duration,
    state_backend: Arc<dyn StateOperatorBackend<FileWatermark>>,
    num_records_before_stop: Option<u64>,
    internal_buffer_size: u32,
    shutdown_rx: watch::Receiver<bool>,
    // Kept alive so the shutdown channel stays open for `shutdown()`.
    shutdown_tx: watch::Sender<bool>,
}

impl FileSourceTableProvider {
    #[allow(clippy::too_many_arguments)]
    pub async fn try_new(
        reference_name: &str,
        path: &str,
        format: FileSourceFormat,
        poll_interval: Duration,
        session_manager: &SessionManager,
        state_backend: Arc<dyn StateOperatorBackend<FileWatermark>>,
        num_records_before_stop: Option<u64>,
        internal_buffer_size: u32,
    ) -> Result<Arc<Self>> {
        let table_url = ListingTableUrl::parse(path)?;
        register_object_store_for_url(&table_url, path, session_manager)?;

        let file_format = file_format_for(format);
        let state = session_manager.session_state();

        // Continuous mode does not surface Hive partition columns (the poll loop
        // builds flat file groups); bail rather than silently dropping them.
        // `infer_partitions_from_path` only inspects the directory structure.
        let partition_probe = ListingTableConfig::new(table_url.clone())
            .with_listing_options(ListingOptions::new(file_format.clone()))
            .infer_partitions_from_path(&state)
            .await?;
        let partition_cols = partition_probe
            .options
            .map(|options| options.table_partition_cols)
            .unwrap_or_default();
        if !partition_cols.is_empty() {
            let names: Vec<&str> = partition_cols
                .iter()
                .map(|(name, _)| name.as_str())
                .collect();
            streamling_user_bail!(
                "file source '{}': continuous mode does not support Hive partition \
                 columns (found {:?} under path '{}'). Use bounded mode or a \
                 non-partitioned path.",
                reference_name,
                names,
                path
            );
        }

        let config = ListingTableConfig::new(table_url.clone())
            .with_listing_options(ListingOptions::new(file_format.clone()))
            .infer_schema(&state)
            .await?;
        let file_schema = ListingTable::try_new(config)?.schema();

        if file_schema.fields().is_empty() {
            streamling_user_bail!(
                "file source '{}': no files matching format {:?} found at path '{}'. \
                 Check the path and that the files use the format's extension.",
                reference_name,
                format,
                path
            );
        }

        // Append a synthesized `_gs_op` unless the files already carry a valid one
        // (mirrors the bounded source's ViewTable projection vs. passthrough).
        let (full_schema, append_op) = match file_schema.field_with_name(COLUMN_NAME_OP) {
            Ok(op_field) => {
                if op_field.data_type() != &DataType::Utf8 || op_field.is_nullable() {
                    streamling_user_bail!(
                        "file source '{}': column '{}' must be a non-nullable Utf8 column, \
                         found type {:?} (nullable: {})",
                        reference_name,
                        COLUMN_NAME_OP,
                        op_field.data_type(),
                        op_field.is_nullable()
                    );
                }
                (file_schema.clone(), false)
            }
            Err(_) => {
                let mut fields = file_schema.fields().to_vec();
                fields.push(Arc::new(Field::new(COLUMN_NAME_OP, DataType::Utf8, false)));
                (Arc::new(Schema::new(fields)), true)
            }
        };

        let file_extension = file_format.get_ext();
        let file_source = file_source_for(format, &file_format);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Ok(Arc::new(Self {
            reference_name: reference_name.to_string(),
            table_url,
            file_source,
            file_extension,
            file_schema,
            full_schema,
            append_op,
            poll_interval,
            state_backend,
            num_records_before_stop,
            internal_buffer_size,
            shutdown_rx,
            shutdown_tx,
        }))
    }

    /// Signals the poll loop to stop. Parity with the Kafka source; the exec also
    /// exits on SIGTERM, so this is for programmatic shutdown.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl Debug for FileSourceTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FileSourceTableProvider({})", self.reference_name)
    }
}

#[async_trait]
impl TableProvider for FileSourceTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FileSourceExec::new(
            self.reference_name.clone(),
            self.table_url.clone(),
            self.file_source.clone(),
            self.file_extension.clone(),
            self.file_schema.clone(),
            self.full_schema.clone(),
            self.append_op,
            projection.cloned(),
            self.poll_interval,
            self.state_backend.clone(),
            self.num_records_before_stop,
            self.internal_buffer_size,
            self.shutdown_rx.clone(),
        )))
    }
}

struct FileSourceExec {
    reference_name: String,
    table_url: ListingTableUrl,
    file_source: Arc<dyn FileSource>,
    file_extension: String,
    file_schema: SchemaRef,
    full_schema: SchemaRef,
    output_schema: SchemaRef,
    append_op: bool,
    projection: Option<Vec<usize>>,
    poll_interval: Duration,
    state_backend: Arc<dyn StateOperatorBackend<FileWatermark>>,
    num_records_before_stop: Option<u64>,
    internal_buffer_size: u32,
    shutdown_rx: watch::Receiver<bool>,
    cached_properties: PlanProperties,
}

impl FileSourceExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        reference_name: String,
        table_url: ListingTableUrl,
        file_source: Arc<dyn FileSource>,
        file_extension: String,
        file_schema: SchemaRef,
        full_schema: SchemaRef,
        append_op: bool,
        projection: Option<Vec<usize>>,
        poll_interval: Duration,
        state_backend: Arc<dyn StateOperatorBackend<FileWatermark>>,
        num_records_before_stop: Option<u64>,
        internal_buffer_size: u32,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        let output_schema = project_schema(&full_schema, projection.as_ref()).unwrap();
        let cached_properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );
        Self {
            reference_name,
            table_url,
            file_source,
            file_extension,
            file_schema,
            full_schema,
            output_schema,
            append_op,
            projection,
            poll_interval,
            state_backend,
            num_records_before_stop,
            internal_buffer_size,
            shutdown_rx,
            cached_properties,
        }
    }
}

impl Debug for FileSourceExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FileSourceExec")
    }
}

impl DisplayAs for FileSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "FileSourceExec")
    }
}

impl ExecutionPlan for FileSourceExec {
    fn name(&self) -> &'static str {
        "FileSourceExec"
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut builder = RecordBatchReceiverStreamBuilder::new(
            self.output_schema.clone(),
            self.internal_buffer_size as usize,
        );
        let tx = builder.tx();

        let reference_name = self.reference_name.clone();
        let table_url = self.table_url.clone();
        let file_source = self.file_source.clone();
        let file_extension = self.file_extension.clone();
        let file_schema = self.file_schema.clone();
        let full_schema = self.full_schema.clone();
        let append_op = self.append_op;
        let projection = self.projection.clone();
        let poll_interval = self.poll_interval;
        let state_backend = self.state_backend.clone();
        let state_key = watermark_state_key(&self.reference_name);
        let num_records_before_stop = self.num_records_before_stop;
        let mut shutdown_rx = self.shutdown_rx.clone();

        builder.spawn(async move {
            let object_store_url = table_url.object_store();
            let object_store = context.runtime_env().object_store(&object_store_url)?;

            let mut watermark = state_backend
                .get(state_key.clone())
                .await
                .map_err(to_df_err)?
                .unwrap_or(FileWatermark {
                    last_modified_ms: i64::MIN,
                });
            let mut total_emitted: u64 = 0;

            debug!("Current watermark: {:?}", watermark);

            // Caught once and held across iterations so a SIGTERM that arrives
            // while sleeping is not missed (a freshly created handler would not
            // observe a signal delivered before it existed).
            #[cfg(unix)]
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();

            loop {
                let mut new_files: Vec<PartitionedFile> = Vec::new();
                let mut max_seen = watermark.last_modified_ms;
                let mut listing = object_store.list(Some(table_url.prefix()));
                while let Some(meta) = listing.next().await {
                    let meta = meta?;
                    let last_modified_ms = meta.last_modified.timestamp_millis();
                    if should_process(
                        meta.location.extension(),
                        last_modified_ms,
                        &file_extension,
                        watermark.last_modified_ms,
                    ) {
                        max_seen = max_seen.max(last_modified_ms);
                        new_files.push(PartitionedFile::from(meta));
                    } else {
                        debug!(
                            "Skipping file {}, last modified at {}",
                            meta.location, meta.last_modified
                        );
                    }
                }

                if !new_files.is_empty() {
                    let config = FileScanConfigBuilder::new(
                        object_store_url.clone(),
                        file_schema.clone(),
                        file_source.clone(),
                    )
                    .with_file_group(FileGroup::new(new_files))
                    .build();

                    let mut stream =
                        DataSourceExec::from_data_source(config).execute(0, context.clone())?;
                    while let Some(batch) = stream.next().await {
                        let batch = build_output_batch(
                            batch?,
                            append_op,
                            &full_schema,
                            projection.as_ref(),
                        )?;
                        let rows = batch.num_rows() as u64;
                        if tx.send(Ok(batch)).await.is_err() {
                            return Ok(());
                        }
                        total_emitted += rows;
                        if let Some(limit) = num_records_before_stop
                            && total_emitted >= limit
                        {
                            return Ok(());
                        }
                    }

                    watermark.last_modified_ms = max_seen;
                    state_backend
                        .put(state_key.clone(), watermark)
                        .await
                        .map_err(to_df_err)?;
                } else {
                    debug!("No new files to process");
                }

                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return Ok(());
                        }
                    }
                    _ = sleep(poll_interval) => {}
                    // SIGTERM (unix) / Ctrl-C (otherwise): exit the poll loop so
                    // the stream ends and downstream sinks complete. The wait is
                    // bound to a `let` so neither cfg branch sits in a
                    // (feature-gated) trailing-expression position.
                    _ = async {
                        #[cfg(unix)]
                        let _: () = match sigterm.as_mut() {
                            Some(stream) => {
                                stream.recv().await;
                            }
                            None => std::future::pending::<()>().await,
                        };
                        #[cfg(not(unix))]
                        let _: () = {
                            let _ = tokio::signal::ctrl_c().await;
                        };
                    } => {
                        info!("[{}] file source received termination signal", reference_name);
                        return Ok(());
                    }
                }
            }
        });

        Ok(builder.build())
    }
}

/// Converts a discovered file batch into the source's output: appends the
/// constant `_gs_op = 'i'` column when the files don't provide one, then applies
/// the requested projection.
fn build_output_batch(
    batch: RecordBatch,
    append_op: bool,
    full_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<RecordBatch> {
    let full = if append_op {
        let op_value = RowKind::Insert.to_str();
        let op: ArrayRef = Arc::new(StringArray::from(vec![op_value.as_str(); batch.num_rows()]));
        let mut columns = batch.columns().to_vec();
        columns.push(op);
        RecordBatch::try_new(full_schema.clone(), columns)?
    } else {
        batch
    };

    match projection {
        Some(indices) => Ok(full.project(indices)?),
        None => Ok(full),
    }
}

fn to_df_err<E>(e: E) -> DataFusionError
where
    E: std::error::Error + Send + Sync + 'static,
{
    DataFusionError::External(Box::new(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_watermark_serde_round_trip() {
        let watermark = FileWatermark {
            last_modified_ms: 1_700_000_000_123,
        };
        let json = serde_json::to_string(&watermark).unwrap();
        let restored: FileWatermark = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.last_modified_ms, watermark.last_modified_ms);
    }

    #[test]
    fn should_process_requires_matching_extension() {
        assert!(should_process(Some("parquet"), 10, "parquet", 5));
        assert!(!should_process(Some("csv"), 10, "parquet", 5));
        assert!(!should_process(None, 10, "parquet", 5));
    }

    #[test]
    fn should_process_requires_newer_than_watermark() {
        assert!(should_process(Some("parquet"), 10, "parquet", 5));
        assert!(!should_process(Some("parquet"), 5, "parquet", 5));
        assert!(!should_process(Some("parquet"), 4, "parquet", 5));
    }

    /// Regression for the NUL-delimiter bug: building the runtime `FileScanConfig`
    /// with `CsvFormat::file_source()` (`CsvSource::default`) splits on a NUL byte,
    /// so every line collapses to a single field and the read fails with
    /// "incorrect number of fields". `file_source_for` must wire the comma
    /// delimiter and header-skipping instead.
    #[tokio::test]
    async fn continuous_csv_reads_comma_delimited_rows() {
        use datafusion::arrow::array::Array;
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;

        let dir = std::env::temp_dir().join(format!("streamling_csv_src_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("1.csv"), "id,name\n1,alice\n2,bob\n3,carol").unwrap();

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let file_format = file_format_for(FileSourceFormat::Csv);
        let file_source = file_source_for(FileSourceFormat::Csv, &file_format);

        let url = ListingTableUrl::parse(format!("{}/", dir.to_str().unwrap())).unwrap();
        let object_store_url = url.object_store();

        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let object_store = task_ctx
            .runtime_env()
            .object_store(&object_store_url)
            .unwrap();
        let files: Vec<PartitionedFile> = object_store
            .list(Some(url.prefix()))
            .map(|meta| PartitionedFile::from(meta.unwrap()))
            .collect()
            .await;

        let config = FileScanConfigBuilder::new(object_store_url, file_schema, file_source)
            .with_file_group(FileGroup::new(files))
            .build();
        let batches = collect(DataSourceExec::from_data_source(config), task_ctx)
            .await
            .unwrap();
        let _ = std::fs::remove_dir_all(&dir);

        let names: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column_by_name("name").unwrap();
                let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                (0..array.len())
                    .map(|i| array.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        // Header skipped (3 rows, not 4) and fields split on the comma.
        assert_eq!(names, vec!["alice", "bob", "carol"]);
    }
}
