//! The table provider and execution plan for both read modes.

use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, internal_err};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
};
use datafusion::datasource::physical_plan::{FileGroup, FileSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    project_schema,
};
use tokio::sync::watch;
use tracing::{debug, info};

use streamling_core::checkpoints::checkpoint_management::CheckpointControl;
use streamling_core::data::COLUMN_NAME_OP;
use streamling_core::error::Result;
use streamling_core::session::SessionManager;
use streamling_core::topology::FileSourceFormat;
use streamling_core::{streamling_user_bail, streamling_user_err};
use streamling_state::StateOperatorBackend;

use super::bounded::{
    BoundedPlan, BoundedProgress, RangeProgress, bounded_progress_state_key, read_bounded_partition,
};
use super::checkpoint::CheckpointInbox;
use super::continuous::{
    ContinuousPlan, ContinuousWork, FileWatermark, WatermarkProgress, read_continuous_partition,
    watermark_state_key,
};
use super::discovery::{FileDiscovery, infer_partition_columns};
use super::progress::ProgressTracker;
use super::reader::PartitionReader;
use super::{ScanProjection, pushable_filters, to_df_err};

/// How a [`FileSourceTableProvider`] reads its path. Each mode owns the state
/// backend for its own progress format, so a mode can only ever read and write
/// its own state.
pub enum FileSourceReadMode {
    /// Read the files present at scan time once, then end.
    Bounded {
        /// Output partitions the files are split across; defaults to the
        /// session's target partitions.
        parallelism: Option<usize>,
        state_backend: Arc<dyn StateOperatorBackend<BoundedProgress>>,
        /// Mints and awaits the terminal checkpoint that closes the source;
        /// without it the partitions end their streams without one.
        checkpoint_control: Option<CheckpointControl>,
    },
    /// Poll for new files every `poll_interval`, forever.
    Continuous {
        poll_interval: Duration,
        /// Output partitions the newly discovered files are shared across.
        parallelism: usize,
        state_backend: Arc<dyn StateOperatorBackend<FileWatermark>>,
    },
}

/// The `file` source in either read mode; see the module docs.
pub struct FileSourceTableProvider {
    reference_name: String,
    pub(super) discovery: FileDiscovery,
    file_source: Arc<dyn FileSource>,
    /// Published schema: file columns, partition columns, then a synthesized
    /// `_gs_op` when absent. (The file schema + partition fields are baked into
    /// `file_source`'s `TableSchema` at construction, so they aren't stored
    /// separately.)
    full_schema: SchemaRef,
    /// Whether `_gs_op` must be synthesized (false when the files already have it).
    append_op: bool,
    mode: FileSourceReadMode,
    num_records_before_stop: Option<u64>,
    internal_buffer_size: u32,
    shutdown_rx: watch::Receiver<bool>,
    // Kept alive so the shutdown channel stays open for `shutdown()`.
    shutdown_tx: watch::Sender<bool>,
}

impl FileSourceTableProvider {
    pub async fn try_new(
        reference_name: &str,
        path: &str,
        format: FileSourceFormat,
        mode: FileSourceReadMode,
        session_manager: &SessionManager,
        num_records_before_stop: Option<u64>,
        internal_buffer_size: u32,
    ) -> Result<Arc<Self>> {
        let table_url = ListingTableUrl::parse(path)?;
        register_object_store_for_url(&table_url, path, session_manager)?;

        let file_format = file_format_for(format);
        let file_extension = file_format.get_ext();

        let state = session_manager.session_state();

        // Infer the data (file) schema from file contents. This is safe for any
        // nesting; it does not validate a partition structure (unlike DataFusion's
        // `infer_partitions_from_path`, which rejects plain nested subfolders).
        //
        // DataFusion 54 errors out of `infer_schema` when the path matches no files
        // ("No files found at ... Cannot infer schema from an empty location") rather
        // than returning an empty schema (df49). Translate that into a clear
        // fail-fast message (the `is_empty()` check below is the df49-era fallback).
        let config = ListingTableConfig::new(table_url.clone())
            .with_listing_options(ListingOptions::new(file_format.clone()));
        let config = match config.infer_schema(&state).await {
            Ok(config) => config,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("No files found") || msg.contains("empty location") {
                    streamling_user_bail!(
                        "file source '{}': no files matching format {:?} found at path '{}'. \
                         Check the path and that the files use the format's extension.",
                        reference_name,
                        format,
                        path
                    );
                }
                return Err(e.into());
            }
        };
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

        // Detect Hive partition columns from a sample of discovered paths: only
        // `key=value` directory segments count, so plain nested subfolders yield no
        // partition columns.
        let object_store = state.runtime_env().object_store(table_url.object_store())?;
        let partition_cols =
            infer_partition_columns(&table_url, &file_extension, object_store.as_ref()).await;
        let partition_fields: Vec<Field> = partition_cols
            .iter()
            .map(|(name, datatype)| Field::new(name, datatype.clone(), false))
            .collect();

        // Output schema: data columns, then partition columns, then a synthesized
        // `_gs_op` unless the data already carries a valid one.
        let mut output_fields: Vec<FieldRef> = file_schema.fields().iter().cloned().collect();
        output_fields.extend(partition_fields.iter().map(|field| Arc::new(field.clone())));
        let append_op = match file_schema.field_with_name(COLUMN_NAME_OP) {
            Ok(op_field) => {
                // Downstream consumers (RowKind::extract_row_kinds_from_batch)
                // require a non-nullable Utf8 op column; reject anything else
                // rather than panicking later in a sink.
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
                false
            }
            Err(_) => {
                output_fields.push(Arc::new(Field::new(COLUMN_NAME_OP, DataType::Utf8, false)));
                true
            }
        };
        let full_schema: SchemaRef = Arc::new(Schema::new(output_fields));

        // df54: the FileSource carries the full TableSchema (file columns +
        // partition columns), which is how the runtime FileScanConfig learns the
        // partition layout (the builder no longer takes partition cols directly).
        let table_partition_cols: Vec<FieldRef> = partition_fields
            .iter()
            .map(|f| Arc::new(f.clone()))
            .collect();
        let table_schema = TableSchema::new(file_schema.clone(), table_partition_cols);
        let file_source = file_source_for(&file_format, table_schema);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        Ok(Arc::new(Self {
            reference_name: reference_name.to_string(),
            discovery: FileDiscovery {
                table_url,
                file_extension,
                partition_cols,
            },
            file_source,
            full_schema,
            append_op,
            mode,
            num_records_before_stop,
            internal_buffer_size,
            shutdown_rx,
            shutdown_tx,
        }))
    }

    /// Signals the read loops to stop. Parity with the Kafka source; the exec also
    /// exits on SIGTERM, so this is for programmatic shutdown.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Lists the path, drops the files persisted progress already covers, and
    /// queues the rest for the partitions to share. This happens at scan time
    /// because the partition count is fixed in `PlanProperties` at planning time
    /// (`ListingTable` lists at scan time for the same reason).
    async fn plan_bounded(
        &self,
        state: &dyn Session,
        file_source: &Arc<dyn FileSource>,
        parallelism: Option<usize>,
        state_backend: &Arc<dyn StateOperatorBackend<BoundedProgress>>,
        checkpoint_control: &Option<CheckpointControl>,
    ) -> DataFusionResult<ExecMode> {
        let state_key = bounded_progress_state_key(&self.reference_name);
        let persisted = state_backend
            .get(state_key.clone())
            .await
            .map_err(to_df_err)?
            .unwrap_or_default();

        let object_store = state
            .runtime_env()
            .object_store(self.discovery.table_url.object_store())?;
        let listed = self
            .discovery
            .list(object_store.as_ref(), |meta| {
                self.discovery.has_extension(meta)
            })
            .await?;
        let mut listing: Vec<String> = listed
            .iter()
            .map(|file| file.object_meta.location.as_ref().to_string())
            .collect();
        listing.sort_unstable();
        let remaining: Vec<PartitionedFile> = listed
            .into_iter()
            .filter(|file| !persisted.covers(file.object_meta.location.as_ref()))
            .collect();
        let remaining_count = remaining.len();

        let target_partitions = parallelism
            .unwrap_or_else(|| state.config().target_partitions())
            .max(1);
        // Mirrors `ListingTable` feeding `DataSourceExec`: one output partition per
        // group `split_files` returns (possibly fewer than `target_partitions`),
        // with every group's files flattened into one queue that the partitions
        // share and the reader may reorder (parquet does, by statistics, for
        // TopK). A fully covered listing (a rerun of a finished job) still plans
        // one partition, which finds the queue empty and ends.
        let groups = FileGroup::new(remaining).split_files(target_partitions);
        let partitions = groups.len().max(1);
        let queue: VecDeque<PartitionedFile> = file_source
            .reorder_files(groups.into_iter().flat_map(FileGroup::into_inner).collect())
            .into();
        info!(
            "File source '{}': {} of {} listed file(s) left to read across {} partition(s)",
            self.reference_name,
            remaining_count,
            listing.len(),
            partitions
        );

        let tracker = Arc::new(ProgressTracker::new(
            state_backend.clone(),
            state_key,
            RangeProgress::new(listing, persisted),
            partitions,
        ));
        Ok(ExecMode::Bounded {
            partitions,
            queue: Arc::new(Mutex::new(queue)),
            tracker,
            checkpoint_control: checkpoint_control.clone(),
        })
    }

    /// Loads the persisted watermark and sets up the files the partitions share.
    /// This happens at scan time, like `plan_bounded`, so every partition of the
    /// plan shares one progress.
    async fn plan_continuous(
        &self,
        state: &dyn Session,
        poll_interval: Duration,
        parallelism: usize,
        state_backend: &Arc<dyn StateOperatorBackend<FileWatermark>>,
    ) -> DataFusionResult<ExecMode> {
        let state_key = watermark_state_key(&self.reference_name);
        let watermark = state_backend
            .get(state_key.clone())
            .await
            .map_err(to_df_err)?
            .unwrap_or(FileWatermark {
                last_modified_ms: i64::MIN,
                ..Default::default()
            });
        debug!("Current watermark: {:?}", watermark);

        let object_store = state
            .runtime_env()
            .object_store(self.discovery.table_url.object_store())?;
        let partitions = parallelism.max(1);
        Ok(ExecMode::Continuous {
            partitions,
            work: Arc::new(ContinuousWork::new(
                self.discovery.clone(),
                object_store,
                poll_interval,
            )),
            tracker: Arc::new(ProgressTracker::new(
                state_backend.clone(),
                state_key,
                WatermarkProgress::new(watermark),
                partitions,
            )),
        })
    }

    /// The reader with the scan's filters pushed in for pruning — row groups, for
    /// parquet; other formats ignore them. A filter that can't become a physical
    /// expression is skipped: DataFusion re-applies every filter above the scan.
    fn file_source_with_filters(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> DataFusionResult<Arc<dyn FileSource>> {
        let table_schema = self.file_source.table_schema().table_schema();
        let df_schema = DFSchema::try_from(table_schema.as_ref().clone())?;
        let predicates: Vec<_> = pushable_filters(filters, table_schema)
            .into_iter()
            .filter_map(|filter| state.create_physical_expr(filter, &df_schema).ok())
            .collect();
        if predicates.is_empty() {
            return Ok(self.file_source.clone());
        }
        let pushdown = self
            .file_source
            .try_pushdown_filters(predicates, state.config_options())?;
        Ok(pushdown
            .updated_node
            .unwrap_or_else(|| self.file_source.clone()))
    }
}

impl Debug for FileSourceTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FileSourceTableProvider({})", self.reference_name)
    }
}

#[async_trait]
impl TableProvider for FileSourceTableProvider {
    fn schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Every filter is `Inexact`: DataFusion keeps its own filter above the
    /// scan, so correctness never depends on what the reader prunes.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let file_source = self.file_source_with_filters(state, filters)?;
        let mode = match &self.mode {
            FileSourceReadMode::Bounded {
                parallelism,
                state_backend,
                checkpoint_control,
            } => {
                self.plan_bounded(
                    state,
                    &file_source,
                    *parallelism,
                    state_backend,
                    checkpoint_control,
                )
                .await?
            }
            FileSourceReadMode::Continuous {
                poll_interval,
                parallelism,
                state_backend,
            } => {
                self.plan_continuous(state, *poll_interval, *parallelism, state_backend)
                    .await?
            }
        };
        Ok(Arc::new(FileSourceExec::try_new(
            self,
            file_source,
            projection,
            limit,
            mode,
        )?))
    }
}

/// A scan's mode-specific plan.
pub(super) enum ExecMode {
    Bounded {
        partitions: usize,
        /// Unopened files every partition takes its next file from, as
        /// DataFusion's `SharedWorkSource` is for `DataSourceExec`'s partitions.
        queue: Arc<Mutex<VecDeque<PartitionedFile>>>,
        tracker: Arc<ProgressTracker<RangeProgress>>,
        checkpoint_control: Option<CheckpointControl>,
    },
    Continuous {
        partitions: usize,
        /// Newly discovered files every partition takes its next round from.
        work: Arc<ContinuousWork>,
        tracker: Arc<ProgressTracker<WatermarkProgress>>,
    },
}

pub(super) struct FileSourceExec {
    reference_name: String,
    discovery: FileDiscovery,
    file_source: Arc<dyn FileSource>,
    output_schema: SchemaRef,
    projection: ScanProjection,
    limit: Option<usize>,
    pub(super) mode: ExecMode,
    num_records_before_stop: Option<u64>,
    /// Rows emitted across every partition, checked against
    /// `num_records_before_stop`.
    records_emitted: Arc<AtomicU64>,
    internal_buffer_size: u32,
    shutdown_rx: watch::Receiver<bool>,
    cached_properties: Arc<PlanProperties>,
}

impl FileSourceExec {
    fn try_new(
        provider: &FileSourceTableProvider,
        file_source: Arc<dyn FileSource>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        mode: ExecMode,
    ) -> DataFusionResult<Self> {
        let output_schema = project_schema(&provider.full_schema, projection)?;
        let table_columns = file_source.table_schema().table_schema().fields().len();
        let (partitions, boundedness) = match &mode {
            ExecMode::Bounded { partitions, .. } => (*partitions, Boundedness::Bounded),
            ExecMode::Continuous { partitions, .. } => (
                *partitions,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            ),
        };
        let cached_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            boundedness,
        ));
        Ok(Self {
            reference_name: provider.reference_name.clone(),
            discovery: provider.discovery.clone(),
            file_source,
            output_schema,
            projection: ScanProjection::new(projection, table_columns, provider.append_op),
            limit,
            mode,
            num_records_before_stop: provider.num_records_before_stop,
            records_emitted: Arc::new(AtomicU64::new(0)),
            internal_buffer_size: provider.internal_buffer_size,
            shutdown_rx: provider.shutdown_rx.clone(),
            cached_properties,
        })
    }
}

impl ExecMode {
    fn name(&self) -> &'static str {
        match self {
            ExecMode::Bounded { .. } => "bounded",
            ExecMode::Continuous { .. } => "continuous",
        }
    }
}

impl Debug for FileSourceExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FileSourceExec: mode={}", self.mode.name())
    }
}

impl DisplayAs for FileSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "FileSourceExec: mode={}, partitions={}",
            self.mode.name(),
            self.properties().output_partitioning().partition_count()
        )
    }
}

impl ExecutionPlan for FileSourceExec {
    fn name(&self) -> &'static str {
        "FileSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Subscribe synchronously, before the stream is returned, so no
        // coordinator message is missed.
        self.execute_with_inbox(partition, context, CheckpointInbox::subscribe())
    }
}

impl FileSourceExec {
    /// [`ExecutionPlan::execute`] with the coordinator subscription passed in, so
    /// a test can drive one partition from a private channel.
    pub(super) fn execute_with_inbox(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        inbox: CheckpointInbox,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let partitions = self.properties().output_partitioning().partition_count();
        if partition >= partitions {
            return internal_err!(
                "FileSourceExec has {} partition(s), asked to execute partition {}",
                partitions,
                partition
            );
        }
        let mut builder = RecordBatchReceiverStreamBuilder::new(
            self.output_schema.clone(),
            self.internal_buffer_size as usize,
        );
        let tx = builder.tx();

        let reader = PartitionReader {
            reference_name: self.reference_name.clone(),
            object_store_url: self.discovery.table_url.object_store(),
            file_source: self.file_source.clone(),
            projection: self.projection.clone(),
            output_schema: self.output_schema.clone(),
            limit: self.limit,
            num_records_before_stop: self.num_records_before_stop,
            records_emitted: self.records_emitted.clone(),
            shutdown_rx: self.shutdown_rx.clone(),
            context: context.clone(),
        };

        match &self.mode {
            ExecMode::Bounded {
                queue,
                tracker,
                checkpoint_control,
                ..
            } => {
                let plan = BoundedPlan {
                    partition,
                    queue: queue.clone(),
                    tracker: tracker.clone(),
                };
                builder.spawn(read_bounded_partition(
                    plan,
                    reader,
                    inbox,
                    checkpoint_control.clone(),
                    tx,
                ));
            }
            ExecMode::Continuous { work, tracker, .. } => {
                let plan = ContinuousPlan {
                    partition,
                    // Roughly `target_partitions` files are read at once, however
                    // many partitions share them.
                    round_size: (context.session_config().target_partitions() / partitions).max(1),
                    work: work.clone(),
                    tracker: tracker.clone(),
                };
                builder.spawn(read_continuous_partition(plan, reader, inbox, tx));
            }
        }

        Ok(builder.build())
    }
}

/// Maps the source's format enum to a DataFusion [`FileFormat`].
fn file_format_for(format: FileSourceFormat) -> Arc<dyn FileFormat> {
    match format {
        FileSourceFormat::Parquet => Arc::new(ParquetFormat::default()),
        FileSourceFormat::Csv => Arc::new(CsvFormat::default()),
        FileSourceFormat::Json => Arc::new(JsonFormat::default()),
        FileSourceFormat::Avro => Arc::new(AvroFormat),
    }
}

/// The DataFusion [`FileSource`] (per-format reader) the source feeds to each
/// round's `FileScanConfig`.
///
/// df54: `FileSource` now carries the `TableSchema` (file schema + partition
/// columns), and `FileFormat::file_source` builds a per-format reader already
/// configured from the format's own options (e.g. `CsvFormat`'s
/// has_header/delimiter/quote), so no per-format special-casing is needed.
fn file_source_for(
    file_format: &Arc<dyn FileFormat>,
    table_schema: TableSchema,
) -> Arc<dyn FileSource> {
    file_format.file_source(table_schema)
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
