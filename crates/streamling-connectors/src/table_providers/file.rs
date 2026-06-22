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
use std::collections::BTreeMap;
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

use streamling_core::checkpoints::channels::subscribe;
use streamling_core::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointEpoch, CheckpointMessage,
    enrich_batch_metadata_with_checkpoints,
};
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
        let output_schema = self.output_schema.clone();
        let append_op = self.append_op;
        let projection = self.projection.clone();
        let poll_interval = self.poll_interval;
        let state_backend = self.state_backend.clone();
        let state_key = watermark_state_key(&self.reference_name);
        let num_records_before_stop = self.num_records_before_stop;
        let mut shutdown_rx = self.shutdown_rx.clone();

        // Subscribe synchronously, before the stream is returned, so no
        // coordinator message is missed; the receiver is drained each poll.
        let checkpoint_receiver = subscribe(CHECKPOINT_COORDINATOR_CHANNEL);

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
            let mut checkpoint_buffer: Vec<CheckpointMessage> = Vec::new();
            // Watermark snapshot per in-flight checkpoint epoch (taken at marker
            // time), persisted only when that epoch is finalized.
            let mut pending_watermarks: BTreeMap<CheckpointEpoch, FileWatermark> = BTreeMap::new();

            debug!("Current watermark: {:?}", watermark);

            // Caught once and held across iterations so a SIGTERM that arrives
            // while sleeping is not missed (a freshly created handler would not
            // observe a signal delivered before it existed).
            #[cfg(unix)]
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();

            loop {
                // Drain coordinator messages that arrived during the sleep. Each
                // is recorded for commit-on-finalize (snapshot watermark on
                // Marker, persist on Finalizer) and buffered to ride downstream on
                // the next emitted batch (a data batch, or the idle heartbeat).
                // The watermark snapshotted here is the pre-this-poll value, so a
                // restart re-reads at most this poll's files. Mirrors Kafka.
                let mut source_complete = drain_checkpoints(
                    &checkpoint_receiver,
                    watermark,
                    &mut pending_watermarks,
                    &state_backend,
                    &state_key,
                    &mut checkpoint_buffer,
                )
                .await?;

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
                        // Drain again before every batch so markers arriving mid-read
                        // ride the next batch (within one batch interval) instead of
                        // waiting for the whole file group, which can take a while.
                        source_complete |= drain_checkpoints(
                            &checkpoint_receiver,
                            watermark,
                            &mut pending_watermarks,
                            &state_backend,
                            &state_key,
                            &mut checkpoint_buffer,
                        )
                        .await?;

                        let batch = enrich_with_checkpoints(
                            build_output_batch(
                                batch?,
                                append_op,
                                &full_schema,
                                projection.as_ref(),
                            )?,
                            &checkpoint_buffer,
                        );
                        checkpoint_buffer.clear();

                        let rows = batch.num_rows() as u64;
                        if tx.send(Ok(batch)).await.is_err() {
                            return Ok(());
                        }
                        total_emitted += rows;
                        if source_complete
                            || num_records_before_stop.is_some_and(|limit| total_emitted >= limit)
                        {
                            return Ok(());
                        }
                    }

                    // Advance the in-memory watermark so files aren't re-read
                    // within this run. Durable persistence happens only on a
                    // checkpoint finalize (see the drain above), mirroring the
                    // Kafka source's commit-on-finalize.
                    watermark.last_modified_ms = max_seen;
                } else {
                    debug!("No new files to process");
                    // Heartbeat: on an idle poll (no new files), emit an empty batch
                    // — carrying any drained checkpoint messages — so downstream
                    // operators keep advancing (checkpoints, liveness, time-based
                    // logic) while the source has no data to forward.
                    let heartbeat = enrich_with_checkpoints(
                        RecordBatch::new_empty(output_schema.clone()),
                        &checkpoint_buffer,
                    );
                    checkpoint_buffer.clear();
                    if tx.send(Ok(heartbeat)).await.is_err() {
                        return Ok(());
                    }
                }

                if source_complete {
                    debug!(
                        "[{}] file source received SourceComplete; shutting down",
                        reference_name
                    );
                    return Ok(());
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

/// Attaches pending checkpoint-coordinator messages to a batch's schema metadata
/// so the checkpoint barrier propagates downstream (mirrors the Kafka source).
/// A no-op when there are no messages.
fn enrich_with_checkpoints(batch: RecordBatch, messages: &[CheckpointMessage]) -> RecordBatch {
    if messages.is_empty() {
        return batch;
    }
    let mut metadata = batch.schema().metadata().clone();
    enrich_batch_metadata_with_checkpoints(&mut metadata, messages);
    let schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().clone(),
        metadata,
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap_or(batch)
}

/// Handles one drained checkpoint message, mirroring the Kafka source's
/// commit-on-finalize: a `Marker` snapshots the current watermark for its epoch,
/// and a `Finalizer` durably persists the matching snapshot (the value captured
/// at marker time, not the now-advanced watermark) and drops it. Returns `true`
/// for `SourceComplete` so the caller can shut down. Other messages are no-ops
/// here (they still propagate downstream via batch metadata).
async fn handle_checkpoint_message(
    message: &CheckpointMessage,
    watermark: FileWatermark,
    pending_watermarks: &mut BTreeMap<CheckpointEpoch, FileWatermark>,
    state_backend: &Arc<dyn StateOperatorBackend<FileWatermark>>,
    state_key: &StateKey,
) -> DataFusionResult<bool> {
    match message {
        CheckpointMessage::Marker { epoch, .. } => {
            pending_watermarks.insert(epoch.clone(), watermark);
        }
        CheckpointMessage::Finalizer(epoch) => {
            if let Some(snapshot) = pending_watermarks.remove(epoch) {
                state_backend
                    .put(state_key.clone(), snapshot)
                    .await
                    .map_err(to_df_err)?;
                debug!(
                    "Persisted watermark {:?} on finalize of epoch {:?}",
                    snapshot, epoch
                );
            }
        }
        CheckpointMessage::SourceComplete(_) => return Ok(true),
        CheckpointMessage::Ack { .. } => {}
    }
    Ok(false)
}

/// Drains all currently-available coordinator messages, recording each (snapshot
/// on `Marker`, persist on `Finalizer`) and buffering it for downstream
/// enrichment. Returns whether a `SourceComplete` was seen. Called both between
/// polls and before every emitted batch, so markers propagate within one batch
/// interval even while a large file group reads for a while.
async fn drain_checkpoints(
    receiver: &crossbeam::channel::Receiver<CheckpointMessage>,
    watermark: FileWatermark,
    pending_watermarks: &mut BTreeMap<CheckpointEpoch, FileWatermark>,
    state_backend: &Arc<dyn StateOperatorBackend<FileWatermark>>,
    state_key: &StateKey,
    buffer: &mut Vec<CheckpointMessage>,
) -> DataFusionResult<bool> {
    let mut source_complete = false;
    while let Ok(message) = receiver.try_recv() {
        source_complete |= handle_checkpoint_message(
            &message,
            watermark,
            pending_watermarks,
            state_backend,
            state_key,
        )
        .await?;
        buffer.push(message);
    }
    Ok(source_complete)
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

    /// The continuous source must emit an empty `RecordBatch` on every poll — even
    /// when no new files were discovered — so downstream operators keep advancing
    /// while the source is idle.
    #[tokio::test]
    async fn continuous_source_emits_empty_heartbeat_when_idle() {
        use std::time::Duration;
        use streamling_core::dynamic_table::DynamicTableRegistry;
        use streamling_state::StateOperatorBackendFactory;
        use streamling_state::in_memory::InMemoryStateOperatorBackendFactory;
        use tokio::time::timeout;

        let dir = std::env::temp_dir().join(format!("streamling_heartbeat_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("1.csv"), "id,name\n1,alice\n2,bob\n3,carol").unwrap();

        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new()).unwrap();
        let state_backend = InMemoryStateOperatorBackendFactory::new()
            .unwrap()
            .create::<FileWatermark>("heartbeat_test");

        let provider = FileSourceTableProvider::try_new(
            "heartbeat_src",
            &format!("{}/", dir.to_str().unwrap()),
            FileSourceFormat::Csv,
            Duration::from_millis(100),
            &session_manager,
            state_backend,
            None,
            10,
        )
        .await
        .unwrap();

        let plan = provider
            .scan(&session_manager.session_state(), None, &[], None)
            .await
            .unwrap();
        let mut stream = plan
            .execute(0, session_manager.session_context().task_ctx())
            .unwrap();

        // The first poll reads the file once (3 rows); each subsequent idle poll
        // emits an empty heartbeat. Collecting several items spans multiple polls.
        let mut data_rows = 0usize;
        let mut empty_batches = 0usize;
        for _ in 0..5 {
            let batch = timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("stream should yield within timeout")
                .expect("stream should not end")
                .unwrap();
            if batch.num_rows() == 0 {
                empty_batches += 1;
            } else {
                data_rows += batch.num_rows();
            }
        }
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(data_rows, 3, "the file's 3 rows are read exactly once");
        assert!(
            empty_batches >= 2,
            "idle polls must emit empty heartbeat batches; got {empty_batches}"
        );
    }

    /// Checkpoint messages drained from the coordinator are attached to a batch's
    /// schema metadata so the barrier propagates downstream (recoverable via
    /// `extract_checkpoint_messages`); an empty set is a no-op.
    #[test]
    fn enrich_with_checkpoints_attaches_marker_metadata() {
        use streamling_core::checkpoints::checkpoint_management::{
            CheckpointEpoch, extract_checkpoint_messages,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));

        let enriched = enrich_with_checkpoints(
            RecordBatch::new_empty(schema.clone()),
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(3),
                created_at_ms: 100,
            }],
        );
        let extracted = extract_checkpoint_messages(enriched.schema().metadata());
        assert!(
            matches!(
                extracted.as_slice(),
                [CheckpointMessage::Marker { epoch, .. }] if epoch.0 == 3
            ),
            "marker should be recoverable from batch metadata; got {extracted:?}"
        );

        let untouched = enrich_with_checkpoints(RecordBatch::new_empty(schema), &[]);
        assert!(extract_checkpoint_messages(untouched.schema().metadata()).is_empty());
    }

    /// The watermark is persisted only on a checkpoint finalize, and the value
    /// persisted is the snapshot captured at marker time (not the now-advanced
    /// watermark) — mirroring the Kafka source's commit-on-finalize.
    #[tokio::test]
    async fn persists_watermark_only_on_finalize() {
        use streamling_state::StateOperatorBackendFactory;
        use streamling_state::in_memory::InMemoryStateOperatorBackendFactory;

        let state_backend = InMemoryStateOperatorBackendFactory::new()
            .unwrap()
            .create::<FileWatermark>("ns");
        let key = watermark_state_key("src");
        let mut pending: BTreeMap<CheckpointEpoch, FileWatermark> = BTreeMap::new();

        // A marker snapshots the current watermark but persists nothing.
        let is_complete = handle_checkpoint_message(
            &CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                created_at_ms: 0,
            },
            FileWatermark {
                last_modified_ms: 42,
            },
            &mut pending,
            &state_backend,
            &key,
        )
        .await
        .unwrap();
        assert!(!is_complete);
        assert!(
            state_backend.get(key.clone()).await.unwrap().is_none(),
            "a marker must not persist the watermark"
        );

        // Finalize persists the marker-time snapshot (42), not the now-advanced
        // watermark (999) passed in here.
        handle_checkpoint_message(
            &CheckpointMessage::Finalizer(CheckpointEpoch(1)),
            FileWatermark {
                last_modified_ms: 999,
            },
            &mut pending,
            &state_backend,
            &key,
        )
        .await
        .unwrap();
        assert_eq!(
            state_backend
                .get(key.clone())
                .await
                .unwrap()
                .unwrap()
                .last_modified_ms,
            42,
            "finalize must persist the snapshot captured at marker time"
        );
        assert!(pending.is_empty());

        // A finalize for an unknown epoch changes nothing.
        handle_checkpoint_message(
            &CheckpointMessage::Finalizer(CheckpointEpoch(7)),
            FileWatermark {
                last_modified_ms: 1234,
            },
            &mut pending,
            &state_backend,
            &key,
        )
        .await
        .unwrap();
        assert_eq!(
            state_backend
                .get(key.clone())
                .await
                .unwrap()
                .unwrap()
                .last_modified_ms,
            42,
            "an unknown-epoch finalize must not change persisted state"
        );

        // SourceComplete signals shutdown.
        let is_complete = handle_checkpoint_message(
            &CheckpointMessage::SourceComplete("src".to_string()),
            FileWatermark {
                last_modified_ms: 0,
            },
            &mut pending,
            &state_backend,
            &key,
        )
        .await
        .unwrap();
        assert!(is_complete);
    }
}
