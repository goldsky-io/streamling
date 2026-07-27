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
//!   loop lists the prefix (or HEADs the path when it names a single object)
//!   every `poll_interval`, ingests files whose `last_modified` exceeds a
//!   persisted [`FileWatermark`], and never self-terminates.
//!
//! Because file reads are append-only, a constant `_gs_op = 'i'` column is
//! synthesized when the inferred schema lacks it. Files that already carry
//! `_gs_op` pass through unwrapped, but the column must be a non-nullable Utf8
//! column to match what downstream consumers expect.

use std::collections::{BTreeMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{Column, DataFusionError, ScalarValue};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::helpers::parse_partitions_for_path;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, FileSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::datasource::{TableProvider, TableType, ViewTable, provider_as_source};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, TableProviderFilterPushDown, lit};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
    coalesce_partitions::CoalescePartitionsExec,
    execution_plan::{Boundedness, EmissionType},
    project_schema,
};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
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

/// Merges the bounded scan's parallel file groups back into a single output
/// partition, delegating projection/filter/limit pushdown to the inner
/// [`ListingTable`] so splitting the files costs nothing at plan time.
#[derive(Debug)]
struct CoalescedFileScan {
    inner: Arc<ListingTable>,
}

#[async_trait]
impl TableProvider for CoalescedFileScan {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        if plan.output_partitioning().partition_count() > 1 {
            return Ok(Arc::new(CoalescePartitionsExec::new(plan)));
        }
        Ok(plan)
    }
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
/// partition columns in the path (e.g. `…/dt=2024-01-01/`) are surfaced in the
/// schema, and nested subfolders are read recursively (the session sets
/// `listing_table_ignore_subdirectory = false`).
pub async fn build_bounded_file_source_provider(
    reference_name: &str,
    path: &str,
    format: FileSourceFormat,
    session_manager: &SessionManager,
) -> Result<Arc<dyn TableProvider>> {
    let table_url = ListingTableUrl::parse(path)?;
    register_object_store_for_url(&table_url, path, session_manager)?;

    let file_format = file_format_for(format);
    let file_extension = file_format.get_ext();

    let state = session_manager.session_state();
    // Detect Hive partition columns ourselves (only `key=value` directories) rather
    // than via `infer_partitions_from_path`, which treats every parent directory as
    // a partition level and so errors on plain nested subfolders.
    let object_store = state.runtime_env().object_store(table_url.object_store())?;
    let partition_cols =
        infer_partition_columns(&table_url, &file_extension, object_store.as_ref()).await;
    // `ListingOptions::new` defaults to one target partition, which reads every
    // file serially on a single core. Split the files across the session's target
    // partitions instead; `CoalescedFileScan` merges them back into one output
    // partition. Set explicitly rather than via `with_session_config_options`,
    // which would also turn on `collect_stat` (a footer fetch per file at startup).
    let listing_options = ListingOptions::new(file_format)
        .with_table_partition_cols(partition_cols)
        .with_target_partitions(state.config().target_partitions());
    // Partition columns are detected ourselves above (`infer_partition_columns`),
    // not via DataFusion's `infer_partitions_from_path` (which treats every parent
    // directory as a partition level and errors on plain nested subfolders).
    let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);

    // DataFusion 54 errors out of `infer_schema` when the path matches no files
    // ("No files found at ... Cannot infer schema from an empty location") rather
    // than returning an empty schema. Translate that into a clear fail-fast message
    // (older DataFusion returned an empty schema, handled by the `is_empty()` check
    // below, which is kept as a defensive fallback).
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
    let provider: Arc<dyn TableProvider> = Arc::new(CoalescedFileScan {
        inner: Arc::new(ListingTable::try_new(config)?),
    });

    let schema = provider.schema();

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
        return Ok(provider);
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
    let plan = LogicalPlanBuilder::scan(reference_name, provider_as_source(provider), None)?
        .project(projection)?
        .build()?;

    Ok(Arc::new(ViewTable::new(plan, None)))
}

/// Persisted discovery position for the continuous file source.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct FileWatermark {
    /// Maximum object `last_modified` (epoch milliseconds) already ingested.
    pub last_modified_ms: i64,
    /// Paths of files already ingested whose `last_modified` equals
    /// `last_modified_ms`. New files that share that boundary timestamp (common
    /// with second-granularity object-store mtimes) are still picked up, while the
    /// ones already done are not reprocessed. Bounded by the number of files
    /// sharing the latest timestamp; it resets whenever a newer file advances the
    /// watermark.
    #[serde(default)]
    pub boundary_paths: Vec<String>,
}

fn watermark_state_key(reference_name: &str) -> StateKey {
    StateKey::from(format!("{reference_name}:watermark"))
}

/// The candidate objects for one poll.
///
/// A non-collection URL names one exact object, which a prefix listing never
/// returns (object_store matches prefixes on whole path segments), so it is
/// HEADed instead. A `NotFound` falls back to a prefix listing, mirroring
/// DataFusion's `ListingTableUrl::list_prefixed_files`: a remote directory URL
/// without a trailing slash is also non-collection, and schema inference
/// resolves it through that same fallback, so a source configured that way would
/// otherwise start and then tear down on its first poll. Other errors ride the
/// stream, keeping one error path in the caller.
async fn list_poll_candidates(
    object_store: &dyn ObjectStore,
    table_url: &ListingTableUrl,
) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
    if table_url.is_collection() {
        return object_store.list(Some(table_url.prefix()));
    }
    match object_store.head(table_url.prefix()).await {
        Err(object_store::Error::NotFound { .. }) => object_store.list(Some(table_url.prefix())),
        result => futures::stream::iter([result]).boxed(),
    }
}

/// Whether a listed object should be ingested: it must carry the format's
/// extension and either be newer than the watermark, or share the watermark's
/// timestamp without having been ingested already. The boundary case keeps new
/// files that land at the exact watermark second (common with second-granularity
/// object-store mtimes) from being missed.
fn should_process(
    location_extension: Option<&str>,
    file_extension: &str,
    last_modified_ms: i64,
    watermark_ms: i64,
    already_ingested_at_boundary: bool,
) -> bool {
    if location_extension != Some(file_extension) {
        return false;
    }
    last_modified_ms > watermark_ms
        || (last_modified_ms == watermark_ms && !already_ingested_at_boundary)
}

/// Advances the watermark after a poll's files are ingested. `new_boundary` are
/// the paths ingested at `max_seen`. When `max_seen` matches the current second (a
/// boundary), the boundary set is unioned so future files at that timestamp are
/// still picked up; when it advances, the watermark and boundary set reset.
fn advance_watermark(watermark: &mut FileWatermark, max_seen: i64, new_boundary: Vec<String>) {
    if max_seen == watermark.last_modified_ms {
        let mut combined: HashSet<String> = std::mem::take(&mut watermark.boundary_paths)
            .into_iter()
            .collect();
        combined.extend(new_boundary);
        watermark.boundary_paths = combined.into_iter().collect();
    } else {
        watermark.last_modified_ms = max_seen;
        watermark.boundary_paths = new_boundary;
    }
}

/// Number of discovered files sampled to detect the Hive partition layout.
const PARTITION_SAMPLE_SIZE: usize = 10;

/// Detects Hive partition column names from a sample of file paths: the leading
/// run of `key=value` parent directory segments, required to be consistent across
/// the sample. Plain (non `key=value`) directories yield no partition columns, so
/// arbitrary nested subfolders are read without spurious partition columns.
///
/// This replaces DataFusion's `infer_partitions_from_path`, which treats every
/// parent directory as a partition level and so rejects plain nested subfolders.
fn detect_partition_columns(
    table_url: &ListingTableUrl,
    sample_paths: &[object_store::path::Path],
) -> Vec<String> {
    let per_file: Vec<Vec<String>> = sample_paths
        .iter()
        .filter_map(|path| {
            let segments: Vec<&str> = table_url.strip_prefix(path)?.collect();
            // Parent directories only (drop the filename), then the leading run of
            // `key=value` segments.
            let parents = segments
                .split_last()
                .map(|(_, parents)| parents)
                .unwrap_or(&[]);
            let keys = parents
                .iter()
                .take_while(|segment| segment.contains('='))
                .map(|segment| segment.split('=').next().unwrap_or(segment).to_string())
                .collect();
            Some(keys)
        })
        .collect();

    match per_file.split_first() {
        Some((first, rest)) if rest.iter().all(|keys| keys == first) => first.clone(),
        _ => Vec::new(),
    }
}

/// Lists a sample of files under the prefix and detects the Hive partition
/// columns, typed as DataFusion types inferred partitions. Empty for flat /
/// plain-subfolder layouts. Shared by the bounded and continuous builders.
async fn infer_partition_columns(
    table_url: &ListingTableUrl,
    file_extension: &str,
    object_store: &dyn object_store::ObjectStore,
) -> Vec<(String, DataType)> {
    let sample: Vec<object_store::path::Path> = object_store
        .list(Some(table_url.prefix()))
        .filter_map(|meta| {
            let extension = file_extension.to_string();
            async move {
                meta.ok()
                    .filter(|object| object.location.extension() == Some(extension.as_str()))
                    .map(|object| object.location)
            }
        })
        .take(PARTITION_SAMPLE_SIZE)
        .collect()
        .await;
    detect_partition_columns(table_url, &sample)
        .into_iter()
        .map(|name| {
            (
                name,
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            )
        })
        .collect()
}

/// Parses Hive partition values for a file path against the configured partition
/// columns (typed as DataFusion's `infer_partitions_from_path` produced them).
/// `Ok(None)` means the file is not under the expected `key=value` layout and
/// should be skipped; `Ok(Some(vec![]))` is returned for flat / plain-subfolder
/// layouts that have no partition columns.
fn partition_values_for(
    table_url: &ListingTableUrl,
    file_path: &object_store::path::Path,
    partition_cols: &[(String, DataType)],
) -> DataFusionResult<Option<Vec<ScalarValue>>> {
    if partition_cols.is_empty() {
        return Ok(Some(vec![]));
    }
    match parse_partitions_for_path(
        table_url,
        file_path,
        partition_cols.iter().map(|(name, _)| name.as_str()),
    ) {
        None => Ok(None),
        Some(values) => values
            .into_iter()
            .zip(partition_cols)
            .map(|(value, (_, datatype))| ScalarValue::try_from_string(value.to_string(), datatype))
            .collect::<DataFusionResult<Vec<_>>>()
            .map(Some),
    }
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
    /// Hive partition columns inferred from the path (empty for flat layouts);
    /// used to parse per-file partition values during discovery.
    partition_cols: Vec<(String, DataType)>,
    /// Published schema: file columns plus a synthesized `_gs_op` when absent.
    /// (The file schema + partition fields are baked into `file_source`'s
    /// `TableSchema` at construction, so they aren't stored separately.)
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
        let file_extension = file_format.get_ext();

        let state = session_manager.session_state();

        // Infer the data (file) schema from file contents. This is safe for any
        // nesting; it does not validate a partition structure (unlike DataFusion's
        // `infer_partitions_from_path`, which rejects plain nested subfolders).
        //
        // DataFusion 54 errors out of `infer_schema` when the path matches no files
        // ("No files found at ... Cannot infer schema from an empty location") rather
        // than returning an empty schema (df49). Translate that into the same clear
        // fail-fast message the bounded path uses (the `is_empty()` check below is the
        // df49-era fallback).
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
        // `_gs_op` unless the data already carries a valid one (mirrors the bounded
        // source's ViewTable projection vs. passthrough).
        let mut output_fields: Vec<FieldRef> = file_schema.fields().iter().cloned().collect();
        output_fields.extend(partition_fields.iter().map(|field| Arc::new(field.clone())));
        let append_op = match file_schema.field_with_name(COLUMN_NAME_OP) {
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
            table_url,
            file_source,
            file_extension,
            partition_cols,
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
            self.partition_cols.clone(),
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
    partition_cols: Vec<(String, DataType)>,
    full_schema: SchemaRef,
    output_schema: SchemaRef,
    append_op: bool,
    projection: Option<Vec<usize>>,
    poll_interval: Duration,
    state_backend: Arc<dyn StateOperatorBackend<FileWatermark>>,
    num_records_before_stop: Option<u64>,
    internal_buffer_size: u32,
    shutdown_rx: watch::Receiver<bool>,
    cached_properties: Arc<PlanProperties>,
}

impl FileSourceExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        reference_name: String,
        table_url: ListingTableUrl,
        file_source: Arc<dyn FileSource>,
        file_extension: String,
        partition_cols: Vec<(String, DataType)>,
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
        let cached_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Self {
            reference_name,
            table_url,
            file_source,
            file_extension,
            partition_cols,
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
        let partition_cols = self.partition_cols.clone();
        // partition columns + file schema are now baked into `file_source`'s
        // TableSchema (df54), so the execute loop no longer needs them directly.
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
                    boundary_paths: Vec::new(),
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
                    &watermark,
                    &mut pending_watermarks,
                    &state_backend,
                    &state_key,
                    &mut checkpoint_buffer,
                )
                .await?;

                // Files already ingested at the current watermark second, so new
                // files sharing that timestamp are still picked up without
                // reprocessing the done ones.
                let boundary: HashSet<String> = watermark.boundary_paths.iter().cloned().collect();
                let mut new_files: Vec<PartitionedFile> = Vec::new();
                let mut max_seen = watermark.last_modified_ms;
                // A HEADed single object flows through the same watermark and
                // extension checks as a listed one, so it is ingested only once.
                let mut listing = list_poll_candidates(object_store.as_ref(), &table_url).await;
                while let Some(meta) = listing.next().await {
                    let meta = meta?;
                    let last_modified_ms = meta.last_modified.timestamp_millis();
                    let already_ingested_at_boundary = last_modified_ms
                        == watermark.last_modified_ms
                        && boundary.contains(meta.location.as_ref());
                    if !should_process(
                        meta.location.extension(),
                        &file_extension,
                        last_modified_ms,
                        watermark.last_modified_ms,
                        already_ingested_at_boundary,
                    ) {
                        debug!(
                            "Skipping file {}, last modified at {}",
                            meta.location, meta.last_modified
                        );
                        continue;
                    }
                    // Parse Hive partition values from the path (empty for flat /
                    // plain-subfolder layouts). Files outside the partition layout
                    // are skipped.
                    let partition_values =
                        match partition_values_for(&table_url, &meta.location, &partition_cols)? {
                            Some(values) => values,
                            None => {
                                debug!("Skipping file outside partition layout: {}", meta.location);
                                continue;
                            }
                        };
                    max_seen = max_seen.max(last_modified_ms);
                    new_files.push(PartitionedFile {
                        object_meta: meta,
                        partition_values,
                        range: None,
                        statistics: None,
                        ordering: None,
                        extensions: Default::default(),
                        metadata_size_hint: None,
                        table_reference: None,
                    });
                }

                if !new_files.is_empty() {
                    // The new boundary set: paths ingested this poll at the newest
                    // timestamp. Computed before `new_files` is moved below.
                    let new_boundary: Vec<String> = new_files
                        .iter()
                        .filter(|file| {
                            file.object_meta.last_modified.timestamp_millis() == max_seen
                        })
                        .map(|file| file.object_meta.location.as_ref().to_string())
                        .collect();

                    // df54: partition columns are carried by the FileSource's
                    // TableSchema (set at construction), so the builder takes just
                    // (object_store_url, file_source) and no longer accepts the
                    // file schema or partition cols directly.
                    //
                    // Spread the poll's files over the session's target partitions so
                    // they are read concurrently, then merge them back into the single
                    // stream this loop emits from: the watermark, the checkpoint drain,
                    // and the record limit all assume one emission point.
                    let file_groups = FileGroup::new(new_files)
                        .split_files(context.session_config().target_partitions());
                    let config =
                        FileScanConfigBuilder::new(object_store_url.clone(), file_source.clone())
                            .with_file_groups(file_groups)
                            .build();

                    let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
                    let mut stream = if scan.output_partitioning().partition_count() > 1 {
                        CoalescePartitionsExec::new(scan).execute(0, context.clone())?
                    } else {
                        scan.execute(0, context.clone())?
                    };
                    while let Some(batch) = stream.next().await {
                        // Drain again before every batch so markers arriving mid-read
                        // ride the next batch (within one batch interval) instead of
                        // waiting for the whole file group, which can take a while.
                        source_complete |= drain_checkpoints(
                            &checkpoint_receiver,
                            &watermark,
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

                    // Advance the in-memory watermark so files aren't re-read within
                    // this run. Durable persistence happens only on a checkpoint
                    // finalize (see the drain above), mirroring the Kafka source's
                    // commit-on-finalize.
                    advance_watermark(&mut watermark, max_seen, new_boundary);
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
    watermark: &FileWatermark,
    pending_watermarks: &mut BTreeMap<CheckpointEpoch, FileWatermark>,
    state_backend: &Arc<dyn StateOperatorBackend<FileWatermark>>,
    state_key: &StateKey,
) -> DataFusionResult<bool> {
    match message {
        CheckpointMessage::Marker { epoch, .. } => {
            pending_watermarks.insert(epoch.clone(), watermark.clone());
        }
        CheckpointMessage::Finalizer(epoch) => {
            if let Some(snapshot) = pending_watermarks.remove(epoch) {
                debug!(
                    "Persisting watermark {:?} on finalize of epoch {:?}",
                    snapshot, epoch
                );
                state_backend
                    .put(state_key.clone(), snapshot)
                    .await
                    .map_err(to_df_err)?;
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
    watermark: &FileWatermark,
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
            boundary_paths: vec!["a/f1.csv".to_string(), "a/f2.csv".to_string()],
        };
        let json = serde_json::to_string(&watermark).unwrap();
        let restored: FileWatermark = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.last_modified_ms, watermark.last_modified_ms);
        assert_eq!(restored.boundary_paths, watermark.boundary_paths);
    }

    #[test]
    fn should_process_requires_matching_extension() {
        assert!(should_process(Some("parquet"), "parquet", 10, 5, false));
        assert!(!should_process(Some("csv"), "parquet", 10, 5, false));
        assert!(!should_process(None, "parquet", 10, 5, false));
    }

    #[test]
    fn should_process_handles_watermark_boundary() {
        // Strictly newer than the watermark.
        assert!(should_process(Some("parquet"), "parquet", 10, 5, false));
        // Below the watermark — skipped.
        assert!(!should_process(Some("parquet"), "parquet", 4, 5, false));
        // At the watermark, not yet ingested — picked up (the boundary catch).
        assert!(should_process(Some("parquet"), "parquet", 5, 5, false));
        // At the watermark, already ingested — skipped.
        assert!(!should_process(Some("parquet"), "parquet", 5, 5, true));
    }

    #[test]
    fn advance_watermark_unions_at_boundary_and_resets_on_advance() {
        let mut watermark = FileWatermark {
            last_modified_ms: 100,
            boundary_paths: vec!["a.csv".to_string()],
        };

        // Same second: the boundary set accumulates (so a third same-second file
        // could still be ingested next poll), watermark unchanged.
        advance_watermark(&mut watermark, 100, vec!["b.csv".to_string()]);
        assert_eq!(watermark.last_modified_ms, 100);
        let mut got = watermark.boundary_paths.clone();
        got.sort();
        assert_eq!(got, vec!["a.csv".to_string(), "b.csv".to_string()]);

        // Newer second: watermark advances and the boundary set resets.
        advance_watermark(&mut watermark, 200, vec!["c.csv".to_string()]);
        assert_eq!(watermark.last_modified_ms, 200);
        assert_eq!(watermark.boundary_paths, vec!["c.csv".to_string()]);
    }

    #[test]
    fn detect_partition_columns_handles_hive_and_plain() {
        let url = ListingTableUrl::parse("file:///t/").unwrap();

        let hive = vec![
            object_store::path::Path::from("t/dt=2024-01-01/a.parquet"),
            object_store::path::Path::from("t/dt=2024-01-02/b.parquet"),
        ];
        assert_eq!(
            detect_partition_columns(&url, &hive),
            vec!["dt".to_string()]
        );

        let multi = vec![
            object_store::path::Path::from("t/dt=2024-01-01/region=us/a.parquet"),
            object_store::path::Path::from("t/dt=2024-01-02/region=eu/b.parquet"),
        ];
        assert_eq!(
            detect_partition_columns(&url, &multi),
            vec!["dt".to_string(), "region".to_string()]
        );

        // Plain nested subfolders (no `key=value`) → no partition columns, even at
        // mixed depths.
        let plain = vec![
            object_store::path::Path::from("t/a/x.parquet"),
            object_store::path::Path::from("t/b/c/y.parquet"),
        ];
        assert!(detect_partition_columns(&url, &plain).is_empty());

        // Flat layout → no partition columns.
        let flat = vec![object_store::path::Path::from("t/x.parquet")];
        assert!(detect_partition_columns(&url, &flat).is_empty());
    }

    #[test]
    fn partition_values_for_parses_hive_layout() {
        let table_url = ListingTableUrl::parse("file:///tablepath/").unwrap();
        let cols = vec![
            ("dt".to_string(), DataType::Utf8),
            ("region".to_string(), DataType::Utf8),
        ];

        // A file under a `dt=…/region=…/` layout yields its partition values.
        let path = object_store::path::Path::from("tablepath/dt=2024-01-01/region=us/f.parquet");
        let values = partition_values_for(&table_url, &path, &cols)
            .unwrap()
            .unwrap();
        assert_eq!(
            values,
            vec![ScalarValue::from("2024-01-01"), ScalarValue::from("us")]
        );

        // No partition columns → empty values (flat / plain-subfolder layout).
        assert_eq!(
            partition_values_for(&table_url, &path, &[]).unwrap(),
            Some(vec![])
        );

        // A file not under the expected partition layout is skipped.
        let flat = object_store::path::Path::from("tablepath/f.parquet");
        assert_eq!(
            partition_values_for(&table_url, &flat, &cols).unwrap(),
            None
        );
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

    /// The bounded source splits its files across the session's target partitions
    /// but must expose exactly one output partition: `DataSinkExec` reads only
    /// input partition 0, and streamling's optimizer never inserts a coalesce, so
    /// a multi-partition source feeding a sink directly would drop rows.
    #[tokio::test]
    async fn bounded_source_coalesces_file_groups_into_one_partition() {
        use streamling_core::dynamic_table::DynamicTableRegistry;

        let dir = std::env::temp_dir().join(format!("streamling_bounded_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for file in 0..3 {
            std::fs::write(
                dir.join(format!("{file}.csv")),
                format!("id,name\n{file}0,alice\n{file}1,bob\n{file}2,carol"),
            )
            .unwrap();
        }

        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new()).unwrap();
        let provider = build_bounded_file_source_provider(
            "bounded_src",
            &format!("{}/", dir.to_str().unwrap()),
            FileSourceFormat::Csv,
            &session_manager,
        )
        .await
        .unwrap();

        let state = session_manager.session_state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        assert_eq!(
            plan.output_partitioning().partition_count(),
            1,
            "the bounded source must expose a single output partition"
        );

        let mut stream = plan
            .execute(0, session_manager.session_context().task_ctx())
            .unwrap();
        let mut rows = 0usize;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows();
        }
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(rows, 9, "every file group's rows must reach partition 0");
    }

    /// A poll's files are split across the session's target partitions and read
    /// concurrently, but the source still emits from one stream: every row must
    /// arrive exactly once (the watermark's boundary set covers the files sharing
    /// the poll's newest timestamp), and idle polls must still heartbeat.
    #[tokio::test]
    async fn continuous_source_reads_split_file_groups_exactly_once() {
        use datafusion::arrow::array::Array;
        use std::time::Duration;
        use streamling_core::dynamic_table::DynamicTableRegistry;
        use streamling_state::StateOperatorBackendFactory;
        use streamling_state::in_memory::InMemoryStateOperatorBackendFactory;
        use tokio::time::timeout;

        let dir = std::env::temp_dir().join(format!("streamling_split_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Non-numeric ids keep the inferred column Utf8, so the assertion below can
        // name the exact rows rather than just count them.
        for file in ["a", "b", "c"] {
            std::fs::write(
                dir.join(format!("{file}.csv")),
                format!("id,name\n{file}0,alice\n{file}1,bob\n{file}2,carol"),
            )
            .unwrap();
        }

        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new()).unwrap();
        let state_backend = InMemoryStateOperatorBackendFactory::new()
            .unwrap()
            .create::<FileWatermark>("split_test");

        let provider = FileSourceTableProvider::try_new(
            "split_src",
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
        assert_eq!(
            plan.output_partitioning().partition_count(),
            1,
            "concurrent file groups must still surface as one output partition"
        );

        let mut stream = plan
            .execute(0, session_manager.session_context().task_ctx())
            .unwrap();
        let mut ids: Vec<String> = Vec::new();
        let mut empty_batches = 0usize;
        for _ in 0..8 {
            let batch = timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("stream should yield within timeout")
                .expect("stream should not end")
                .unwrap();
            if batch.num_rows() == 0 {
                empty_batches += 1;
                continue;
            }
            let column = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            ids.extend((0..column.len()).map(|row| column.value(row).to_string()));
        }
        let _ = std::fs::remove_dir_all(&dir);

        ids.sort();
        assert_eq!(
            ids,
            vec!["a0", "a1", "a2", "b0", "b1", "b2", "c0", "c1", "c2"],
            "every row from every file group must arrive exactly once"
        );
        assert!(
            empty_batches >= 2,
            "polls after ingest must emit empty heartbeat batches; got {empty_batches}"
        );
    }

    /// A remote directory URL without a trailing slash is not a collection, so
    /// the poll HEADs it and gets NotFound. Schema inference resolves such a URL
    /// by falling back to a prefix listing, so the poll loop must agree —
    /// otherwise the source starts and dies on its first poll.
    #[tokio::test]
    async fn poll_candidates_fall_back_to_listing_when_head_misses() {
        use futures::TryStreamExt;
        use object_store::memory::InMemory;
        use object_store::path::Path;

        let store = InMemory::new();
        store
            .put(&Path::from("data/1.csv"), "id\n1".into())
            .await
            .unwrap();

        let table_url = ListingTableUrl::parse("memory:///data").unwrap();
        assert!(!table_url.is_collection(), "no trailing slash");

        let found: Vec<ObjectMeta> = list_poll_candidates(&store, &table_url)
            .await
            .try_collect()
            .await
            .unwrap();
        let locations: Vec<&str> = found.iter().map(|meta| meta.location.as_ref()).collect();
        assert_eq!(locations, vec!["data/1.csv"]);
    }

    /// A path naming one exact object (no trailing slash) must be ingested by
    /// the continuous source. The poll loop's prefix listing never returns a
    /// key equal to the prefix (object_store prefixes match whole path
    /// segments), so this exercises the HEAD branch — without it the source
    /// heartbeats forever with zero rows.
    #[tokio::test]
    async fn continuous_source_ingests_single_file_path() {
        use std::time::Duration;
        use streamling_core::dynamic_table::DynamicTableRegistry;
        use streamling_state::StateOperatorBackendFactory;
        use streamling_state::in_memory::InMemoryStateOperatorBackendFactory;
        use tokio::time::timeout;

        let dir =
            std::env::temp_dir().join(format!("streamling_single_file_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("1.csv");
        std::fs::write(&file, "id,name\n1,alice\n2,bob\n3,carol").unwrap();

        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new()).unwrap();
        let state_backend = InMemoryStateOperatorBackendFactory::new()
            .unwrap()
            .create::<FileWatermark>("single_file_test");

        let provider = FileSourceTableProvider::try_new(
            "single_file_src",
            file.to_str().unwrap(),
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

        // First poll HEADs and reads the file (3 rows); the watermark then
        // filters the unchanged object, so later polls are idle heartbeats.
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

        assert_eq!(data_rows, 3, "the object's 3 rows are read exactly once");
        assert!(
            empty_batches >= 2,
            "polls after ingest must emit empty heartbeat batches; got {empty_batches}"
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
            &FileWatermark {
                last_modified_ms: 42,
                boundary_paths: vec!["a/x.csv".to_string()],
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
            &FileWatermark {
                last_modified_ms: 999,
                boundary_paths: Vec::new(),
            },
            &mut pending,
            &state_backend,
            &key,
        )
        .await
        .unwrap();
        let persisted = state_backend.get(key.clone()).await.unwrap().unwrap();
        assert_eq!(
            persisted.last_modified_ms, 42,
            "finalize must persist the snapshot captured at marker time"
        );
        assert_eq!(
            persisted.boundary_paths,
            vec!["a/x.csv".to_string()],
            "finalize must persist the boundary set from the marker-time snapshot"
        );
        assert!(pending.is_empty());

        // A finalize for an unknown epoch changes nothing.
        handle_checkpoint_message(
            &CheckpointMessage::Finalizer(CheckpointEpoch(7)),
            &FileWatermark {
                last_modified_ms: 1234,
                boundary_paths: Vec::new(),
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
            &FileWatermark {
                last_modified_ms: 0,
                boundary_paths: Vec::new(),
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
