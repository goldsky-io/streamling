//! Applies a table's changelog to its backend. One apply is one transaction
//! holding the table's writes, its window's evictions and its aggregates'
//! updates, so a reader sees all of it or none. Applies of one store take
//! turns, in the order the input's partitions deliver them.

use super::aggregate::{self, TableAggregate};
use super::backend::{Mark, NamespaceSpec, TableBackend, TableTransaction, Version};
use super::horizon::Horizons;
use super::window::{WALL_CLOCK_COLUMN, WALL_CLOCK_TIME_ZONE, Window, WindowConfig};
use super::{JournalTrim, Marks, OnSchemaChange, Table, VersionRetention};
use crate::checkpoints::checkpoint_management::CheckpointEpoch;
use crate::data::COLUMN_NAME_OP;
use crate::error::{Result, ResultExt};
use crate::retry::retry_if_retriable_until_cancelled;
use crate::telemetry::recorder::get_metrics_recorder;
use crate::{streamling_user_bail, streamling_user_err};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, TimestampNanosecondArray};
use arrow::compute::{and, concat_batches, filter, filter_record_batch, not};
use arrow::datatypes::{Fields, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;

const OP_DELETE: &str = "d";
const UPSERT_OPS: [&str; 4] = ["i", "c", "r", "u"];

/// The table's own namespace in its store.
const TABLE_NAMESPACE: &str = "table";
/// Meta keys: the table's fingerprint, its aggregates' fingerprints by name,
/// and the window's watermarks.
const FINGERPRINT_META: &str = "fingerprint";
const AGGREGATES_META: &str = "aggregates";
const WATERMARKS_META: &str = "watermarks";
/// Rows one `evict_below` call deletes at most.
const EVICTION_CHUNK_ROWS: usize = 8192;
/// Rows of one `scan` page.
const SCAN_PAGE_ROWS: usize = 8192;
/// Minimum time between reads of the backend's stats for the gauges.
const GAUGE_INTERVAL: Duration = Duration::from_secs(1);

const LIVE_ROWS_GAUGE: &str = "table_live_rows";
const MEMORY_BYTES_GAUGE: &str = "table_memory_bytes";
const VERSIONS_GAUGE: &str = "table_versions";
const RETIRED_BYTES_GAUGE: &str = "table_retired_bytes";

/// A stable description of a namespace's shape: a stored row reads back the
/// same only while it's unchanged.
pub(super) fn fingerprint(schema: &SchemaRef, key: &[String]) -> String {
    let fields: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            let metadata: BTreeMap<_, _> = field.metadata().iter().collect();
            format!(
                "{}:{}:{}:{:?}",
                field.name(),
                field.data_type(),
                field.is_nullable(),
                metadata
            )
        })
        .collect();
    format!("{}|key={}", fields.join(";"), key.join(","))
}

#[derive(Clone, Copy)]
enum RowOp {
    Upsert,
    Delete,
}

/// A batch of the table's SQL output, ready to write.
struct Changes {
    /// Every row, in the stored schema. Watermarks rise to its values.
    rows: RecordBatch,
    /// The last row of each key whose last op is an upsert, in order.
    upserts: RecordBatch,
    /// The key columns of each key whose last op is a delete.
    deletes: RecordBatch,
}

/// What applies share. Locked for a whole apply, so applies take turns.
struct ApplyState {
    started: bool,
    horizons: Horizons,
    /// The mark of the last commit.
    last_mark: Mark,
    /// When the last GC pass ran.
    last_pass: Instant,
    /// When the gauges were last recorded, if ever.
    last_gauges: Option<Instant>,
}

/// Applies a table's SQL output to its backend, with its window and
/// aggregates. See the module docs.
pub struct TableStore {
    name: String,
    metric_metadata_id: String,
    backend: Arc<dyn TableBackend>,
    table: Arc<Table>,
    /// The input schema without `_gs_op`, plus [`WALL_CLOCK_COLUMN`] when the
    /// window reads it.
    schema: SchemaRef,
    op_index: usize,
    /// Input columns stored in `schema`, in order.
    value_indices: Vec<usize>,
    /// Primary key columns' positions in the input.
    key_indices: Vec<usize>,
    /// Primary key columns' positions in `schema`.
    stored_key_indices: Vec<usize>,
    primary_key: Vec<String>,
    /// Encodes keys to find each key's last row in a batch.
    converter: RowConverter,
    window: Option<Window>,
    retention: VersionRetention,
    journal_trim: JournalTrim,
    on_schema_change: OnSchemaChange,
    /// Added before data flows.
    aggregates: RwLock<Vec<Arc<TableAggregate>>>,
    state: tokio::sync::Mutex<ApplyState>,
}

impl TableStore {
    /// `input_schema` is the table's SQL output schema. It must contain
    /// `_gs_op` and every `primary_key` column.
    pub fn try_new(
        name: &str,
        input_schema: &SchemaRef,
        primary_key: &[String],
        metric_metadata_id: String,
        window: Option<WindowConfig>,
        retention: VersionRetention,
        journal_trim: JournalTrim,
        on_schema_change: OnSchemaChange,
        backend: Arc<dyn TableBackend>,
    ) -> Result<Self> {
        let op_index = input_schema.index_of(COLUMN_NAME_OP).map_err(|_| {
            streamling_user_err!(
                "table '{}': the SQL output must contain the '{}' column",
                name,
                COLUMN_NAME_OP
            )
        })?;
        if primary_key.iter().any(|column| column == COLUMN_NAME_OP) {
            streamling_user_bail!(
                "table '{}': the primary key cannot be '{}'",
                name,
                COLUMN_NAME_OP
            );
        }
        let key_indices = primary_key
            .iter()
            .map(|column| {
                input_schema.index_of(column).map_err(|_| {
                    streamling_user_err!(
                        "table '{}': primary key column '{}' is not in the SQL output",
                        name,
                        column
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let converter = RowConverter::new(
            key_indices
                .iter()
                .map(|&index| SortField::new(input_schema.field(index).data_type().clone()))
                .collect(),
        )
        .map_err(|e| {
            streamling_user_err!("table '{}': unsupported primary key type: {}", name, e)
        })?;
        let value_indices: Vec<usize> = (0..input_schema.fields().len())
            .filter(|&index| index != op_index)
            .collect();
        let value_fields: Fields = value_indices
            .iter()
            .map(|&index| Arc::clone(&input_schema.fields()[index]))
            .collect();
        let window = window
            .map(|config| Window::try_new(name, &config, &value_fields))
            .transpose()?;
        let schema = match &window {
            Some(window) => Arc::clone(window.schema()),
            None => Arc::new(Schema::new(value_fields)),
        };
        let stored_key_indices = key_indices
            .iter()
            .map(|&index| index - usize::from(index > op_index))
            .collect();
        let hidden = window
            .as_ref()
            .filter(|window| window.reads_wall_clock())
            .map(|_| WALL_CLOCK_COLUMN);
        let table = Arc::new(Table::try_new(
            name,
            TABLE_NAMESPACE,
            Arc::clone(&backend),
            Arc::clone(&schema),
            primary_key,
            hidden,
            retention,
        )?);
        Ok(Self {
            name: name.to_string(),
            metric_metadata_id,
            backend,
            table,
            schema,
            op_index,
            value_indices,
            key_indices,
            stored_key_indices,
            primary_key: primary_key.to_vec(),
            converter,
            window,
            retention,
            journal_trim,
            on_schema_change,
            aggregates: RwLock::new(Vec::new()),
            state: tokio::sync::Mutex::new(ApplyState {
                started: false,
                horizons: Horizons::new(),
                last_mark: Mark::default(),
                last_pass: Instant::now(),
                last_gauges: None,
            }),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The table's reads.
    pub fn table(&self) -> Arc<Table> {
        Arc::clone(&self.table)
    }

    pub(super) fn backend(&self) -> &Arc<dyn TableBackend> {
        &self.backend
    }

    pub(super) fn retention(&self) -> VersionRetention {
        self.retention
    }

    /// The schema of the stored rows, which aggregates read.
    pub(super) fn stored_schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Called before data flows, so the aggregate sees every change.
    pub(super) fn add_aggregate(&self, aggregate: Arc<TableAggregate>) {
        self.aggregates.write().push(aggregate);
    }

    fn aggregates(&self) -> Vec<Arc<TableAggregate>> {
        self.aggregates.read().clone()
    }

    fn table_namespace(&self) -> NamespaceSpec {
        NamespaceSpec {
            name: TABLE_NAMESPACE.to_string(),
            schema: Arc::clone(&self.schema),
            key: self.primary_key.clone(),
            versioned: true,
            range_column: self
                .window
                .as_ref()
                .and_then(|window| window.range_column().map(str::to_string)),
        }
    }

    /// Opens the store before data flows: checks that what it stored has
    /// the table's shape (or drops it, with `on_schema_change: reset`),
    /// creates the namespaces, rebuilds the aggregates that are new or
    /// changed, and restores the watermarks. Idempotent.
    pub async fn start(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        self.start_locked(&mut state).await
    }

    async fn start_locked(&self, state: &mut ApplyState) -> Result<()> {
        if state.started {
            return Ok(());
        }
        let stored = retry(&self.name, "open", || self.backend.open()).await?;
        let fingerprint = fingerprint(&self.schema, &self.primary_key);
        let mut stored_aggregates: BTreeMap<String, String> = match stored.meta.get(AGGREGATES_META)
        {
            Some(bytes) => serde_json::from_slice(bytes).streamling_with_context(|| {
                format!("table '{}': invalid stored aggregates", self.name)
            })?,
            None => BTreeMap::new(),
        };
        let reset = stored
            .meta
            .get(FINGERPRINT_META)
            .is_some_and(|stored| stored.as_slice() != fingerprint.as_bytes());
        if reset {
            if self.on_schema_change == OnSchemaChange::Fail {
                streamling_user_bail!(
                    "table '{}': the stored table has another schema or primary key than the table's SQL output; set on_schema_change: reset to drop it and start empty",
                    self.name
                );
            }
            warn!(
                "table '{}': the stored table has another schema or primary key than the table's SQL output, so on_schema_change: reset drops it and its aggregates",
                self.name
            );
            let namespaces = std::iter::once(TABLE_NAMESPACE.to_string()).chain(
                stored_aggregates
                    .keys()
                    .flat_map(|name| aggregate::namespaces(name)),
            );
            for namespace in namespaces {
                retry(&self.name, "drop namespace", || {
                    self.backend.drop_namespace(&namespace)
                })
                .await?;
            }
            stored_aggregates.clear();
        }
        let aggregates = self.aggregates();
        let fingerprints: BTreeMap<String, String> = aggregates
            .iter()
            .map(|aggregate| (aggregate.name().to_string(), aggregate.fingerprint()))
            .collect();

        let table_namespace = self.table_namespace();
        retry(&self.name, "create namespace", || {
            self.backend.create_namespace(&table_namespace)
        })
        .await?;
        for name in stored_aggregates.keys() {
            if !fingerprints.contains_key(name) {
                for namespace in aggregate::namespaces(name) {
                    retry(&self.name, "drop namespace", || {
                        self.backend.drop_namespace(&namespace)
                    })
                    .await?;
                }
            }
        }
        let mut rebuilt = Vec::new();
        for aggregate in &aggregates {
            let unchanged =
                stored_aggregates.get(aggregate.name()) == fingerprints.get(aggregate.name());
            for spec in aggregate.namespace_specs() {
                if !unchanged {
                    retry(&self.name, "drop namespace", || {
                        self.backend.drop_namespace(&spec.name)
                    })
                    .await?;
                }
                retry(&self.name, "create namespace", || {
                    self.backend.create_namespace(&spec)
                })
                .await?;
            }
            if !unchanged {
                rebuilt.push(Arc::clone(aggregate));
            }
        }
        let fingerprints = serde_json::to_vec(&fingerprints)
            .streamling_context("failed to encode the aggregates' fingerprints")?;
        let version = retry(&self.name, "startup", || {
            self.backfill(&rebuilt, fingerprint.as_bytes(), &fingerprints)
        })
        .await?;
        state.last_mark = Mark::after(version);
        // No reader survives a restart, and every stored version predates it.
        self.finalize(state.last_mark).await?;
        if let (Some(window), Some(watermarks), false) =
            (&self.window, stored.meta.get(WATERMARKS_META), reset)
        {
            window.restore_watermarks(watermarks)?;
        }
        state.started = true;
        Ok(())
    }

    /// Records the fingerprints, and rebuilds `aggregates` from the table's
    /// rows, in one transaction.
    async fn backfill(
        &self,
        aggregates: &[Arc<TableAggregate>],
        fingerprint: &[u8],
        aggregate_fingerprints: &[u8],
    ) -> Result<Version> {
        let mut transaction = self.backend.begin().await?;
        transaction
            .set_meta(FINGERPRINT_META, fingerprint.to_vec())
            .await?;
        transaction
            .set_meta(AGGREGATES_META, aggregate_fingerprints.to_vec())
            .await?;
        if !aggregates.is_empty() {
            let none = RecordBatch::new_empty(Arc::clone(&self.schema));
            let mut cursor = None;
            loop {
                let (page, next) = transaction
                    .scan(TABLE_NAMESPACE, cursor, SCAN_PAGE_ROWS)
                    .await?;
                for aggregate in aggregates {
                    aggregate.apply(transaction.as_mut(), &none, &page).await?;
                }
                match next {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
        }
        let version = transaction.version();
        transaction.commit().await?;
        Ok(version)
    }

    /// Applies `batch`, the table's SQL output, in row order: an upsert
    /// replaces its key's row, and a delete removes it. `epochs` are the
    /// checkpoint markers the batch carried. Returns the marks the batch's
    /// readers see the table and its aggregates' outputs at: the end of this
    /// apply. Returns once those versions are durable when `epochs` isn't
    /// empty.
    pub async fn apply(&self, batch: &RecordBatch, epochs: &[CheckpointEpoch]) -> Result<Marks> {
        let mut state = self.state.lock().await;
        self.start_locked(&mut state).await?;
        if let Some(horizon) = state.horizons.due() {
            self.finalize(horizon).await?;
        }
        let changes = self.normalize(batch)?;
        let pass_due = self.window.as_ref().is_some_and(|window| {
            (window.reads_watermark() && changes.rows.num_rows() > 0)
                || state.last_pass.elapsed() >= window.gc_interval()
        });
        let pass = match &self.window {
            Some(window) if pass_due => {
                state.last_pass = Instant::now();
                // Before the transaction, so a retry still stores them.
                let watermarks_moved =
                    window.reads_watermark() && window.advance_watermarks(&changes.rows)?;
                Some((window, watermarks_moved))
            }
            _ => None,
        };
        if changes.rows.num_rows() > 0 || pass.is_some() {
            let version = retry(&self.name, "apply", || self.transact(&changes, pass)).await?;
            state.last_mark = Mark::after(version);
        }
        let mark = state.last_mark;
        state.horizons.record(epochs, mark);
        if !epochs.is_empty() {
            retry(&self.name, "checkpoint", || self.backend.checkpoint(mark)).await?;
        }
        let gauges_due = state
            .last_gauges
            .is_none_or(|last| last.elapsed() >= GAUGE_INTERVAL);
        if gauges_due {
            state.last_gauges = Some(Instant::now());
        }
        drop(state);
        if gauges_due {
            self.record_gauges().await;
        }
        Ok(std::iter::once(self.name.clone())
            .chain(
                self.aggregates()
                    .iter()
                    .map(|aggregate| aggregate.name().to_string()),
            )
            .map(|name| (name, mark))
            .collect())
    }

    async fn finalize(&self, horizon: Mark) -> Result<()> {
        let trim = self.journal_trim == JournalTrim::OnFinalize;
        retry(&self.name, "finalize", || {
            self.backend.finalize(horizon, trim)
        })
        .await
    }

    /// Writes `changes`, runs `pass`, a GC pass of the window that stores the
    /// watermarks if they moved, and updates the aggregates, in one
    /// transaction. Returns its version.
    async fn transact(&self, changes: &Changes, pass: Option<(&Window, bool)>) -> Result<Version> {
        let aggregates = self.aggregates();
        let mut transaction = self.backend.begin().await?;
        let version = transaction.version();
        let mut removed = Vec::new();
        if changes.upserts.num_rows() + changes.deletes.num_rows() > 0 {
            removed.push(
                transaction
                    .write(TABLE_NAMESPACE, &changes.upserts, &changes.deletes)
                    .await?,
            );
        }
        if let Some((window, watermarks_moved)) = pass {
            if watermarks_moved {
                transaction
                    .set_meta(WATERMARKS_META, window.encode_watermarks()?)
                    .await?;
            }
            removed.extend(self.evict(transaction.as_mut(), window).await?);
        }
        if !aggregates.is_empty() {
            let removed = concat_batches(&self.schema, &removed)?;
            for aggregate in &aggregates {
                aggregate
                    .apply(transaction.as_mut(), &removed, &changes.upserts)
                    .await?;
            }
        }
        transaction.commit().await?;
        Ok(version)
    }

    /// Removes the rows the window keeps out, and returns them: a range
    /// window's through `evict_below`, any other's by evaluating it over
    /// every row.
    async fn evict(
        &self,
        transaction: &mut dyn TableTransaction,
        window: &Window,
    ) -> Result<Vec<RecordBatch>> {
        let compiled = window.compile()?;
        let mut evicted = Vec::new();
        if let (Some((bound, inclusive)), Some(column)) =
            (&compiled.eviction, window.range_column())
        {
            loop {
                let rows = transaction
                    .evict_below(
                        TABLE_NAMESPACE,
                        column,
                        bound,
                        *inclusive,
                        EVICTION_CHUNK_ROWS,
                    )
                    .await?;
                let done = rows.num_rows() < EVICTION_CHUNK_ROWS;
                evicted.push(rows);
                if done {
                    return Ok(evicted);
                }
            }
        }
        let mut cursor = None;
        loop {
            let (page, next) = transaction
                .scan(TABLE_NAMESPACE, cursor, SCAN_PAGE_ROWS)
                .await?;
            let outside = window.outside(&compiled, &page)?;
            if outside.has_true() {
                let deletes = self.keys(&filter_record_batch(&page, &outside)?)?;
                let none = RecordBatch::new_empty(Arc::clone(&self.schema));
                evicted.push(transaction.write(TABLE_NAMESPACE, &none, &deletes).await?);
            }
            match next {
                Some(next) => cursor = Some(next),
                None => return Ok(evicted),
            }
        }
    }

    /// The key columns of `rows`, in the stored schema.
    fn keys(&self, rows: &RecordBatch) -> Result<RecordBatch> {
        Ok(rows.project(&self.stored_key_indices)?)
    }

    /// Parses `batch`'s ops and keeps each key's last row.
    fn normalize(&self, batch: &RecordBatch) -> Result<Changes> {
        let ops = self.parse_ops(batch.column(self.op_index))?;
        let key_columns: Vec<ArrayRef> = self
            .key_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index)))
            .collect();
        if let Some(null_column) = key_columns
            .iter()
            .position(|column| column.null_count() > 0)
        {
            streamling_user_bail!(
                "table '{}': primary key column '{}' contains a null value",
                self.name,
                batch.schema().field(self.key_indices[null_column]).name()
            );
        }
        let mut columns: Vec<ArrayRef> = self
            .value_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index)))
            .collect();
        if self
            .window
            .as_ref()
            .is_some_and(|window| window.reads_wall_clock())
        {
            columns.push(wall_clock(batch.num_rows()));
        }
        let rows = RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .streamling_with_context(|| {
                format!(
                    "table '{}': batch does not match the table schema",
                    self.name
                )
            })?;
        let keys = self
            .converter
            .convert_columns(&key_columns)
            .streamling_context("failed to encode the primary key")?;
        // Each key's last row.
        let mut last: HashMap<&[u8], usize> = HashMap::with_capacity(rows.num_rows());
        for (row, key) in keys.iter().enumerate() {
            last.insert(key.data(), row);
        }
        let is_last: BooleanArray = keys
            .iter()
            .enumerate()
            .map(|(row, key)| Some(last[key.data()] == row))
            .collect();
        let is_upsert: BooleanArray = ops
            .iter()
            .map(|op| Some(matches!(op, RowOp::Upsert)))
            .collect();
        let upserts = and(&is_last, &is_upsert)?;
        let deletes = and(&is_last, &not(&is_upsert)?)?;
        Ok(Changes {
            upserts: if upserts.true_count() == rows.num_rows() {
                rows.clone()
            } else {
                filter_record_batch(&rows, &upserts)?
            },
            deletes: RecordBatch::try_new(
                Arc::new(self.schema.project(&self.stored_key_indices)?),
                self.stored_key_indices
                    .iter()
                    .map(|&index| filter(rows.column(index), &deletes))
                    .collect::<std::result::Result<_, _>>()?,
            )?,
            rows,
        })
    }

    fn parse_ops(&self, column: &ArrayRef) -> Result<Vec<RowOp>> {
        let ops = column.as_string_opt::<i32>().ok_or_else(|| {
            streamling_user_err!(
                "table '{}': column '{}' must be Utf8, got {}",
                self.name,
                COLUMN_NAME_OP,
                column.data_type()
            )
        })?;
        ops.iter()
            .map(|op| match op {
                Some(op) if op.eq_ignore_ascii_case(OP_DELETE) => Ok(RowOp::Delete),
                Some(op)
                    if UPSERT_OPS
                        .iter()
                        .any(|upsert| op.eq_ignore_ascii_case(upsert)) =>
                {
                    Ok(RowOp::Upsert)
                }
                other => Err(streamling_user_err!(
                    "table '{}': unrecognized '{}' value {:?}",
                    self.name,
                    COLUMN_NAME_OP,
                    other
                )),
            })
            .collect()
    }

    /// Records the gauges of the table and of its aggregates' outputs.
    async fn record_gauges(&self) {
        let stats = match self.backend.stats().await {
            Ok(stats) => stats,
            Err(err) => {
                warn!("table '{}': failed to read its stats: {}", self.name, err);
                return;
            }
        };
        let aggregates = self.aggregates();
        let recorder = get_metrics_recorder();
        for stats in stats {
            let metric_metadata_id = if stats.namespace == TABLE_NAMESPACE {
                self.metric_metadata_id.as_str()
            } else if let Some(aggregate) = aggregates
                .iter()
                .find(|aggregate| aggregate::namespaces(aggregate.name())[0] == stats.namespace)
            {
                aggregate.metric_metadata_id()
            } else {
                continue;
            };
            let gauges = [
                (LIVE_ROWS_GAUGE, Some(stats.live_rows)),
                (VERSIONS_GAUGE, Some(stats.journal_rows)),
                (MEMORY_BYTES_GAUGE, stats.memory_bytes),
                (RETIRED_BYTES_GAUGE, stats.retired_bytes),
            ];
            for (gauge, value) in gauges {
                if let Some(value) = value {
                    recorder.record_gauge(gauge, value, metric_metadata_id);
                }
            }
        }
    }
}

#[cfg(test)]
impl TableStore {
    /// Makes the next apply run a GC pass.
    pub(crate) async fn make_gc_pass_due(&self) {
        let window = self.window.as_ref().expect("a windowed table");
        self.state.lock().await.last_pass = Instant::now()
            .checked_sub(window.gc_interval())
            .expect("an instant a GC interval ago");
    }

    /// Finalizes `epoch` the way the coordinator's finalizer does on the
    /// next apply.
    pub(crate) async fn finalize_epoch(&self, epoch: u64) -> Result<()> {
        let horizon = self.state.lock().await.horizons.collect(epoch);
        match horizon {
            Some(horizon) => self.finalize(horizon).await,
            None => Ok(()),
        }
    }

    pub(crate) fn range_column(&self) -> Option<&str> {
        self.window.as_ref().and_then(Window::range_column)
    }

    /// The table namespace's stats.
    pub(crate) async fn table_stats(&self) -> super::backend::NamespaceStats {
        self.backend
            .stats()
            .await
            .expect("stats")
            .into_iter()
            .find(|stats| stats.namespace == TABLE_NAMESPACE)
            .expect("the table's namespace")
    }
}

impl fmt::Debug for TableStore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TableStore")
            .field("name", &self.name)
            .field("backend", &self.backend)
            .finish()
    }
}

/// Retries a backend call while it fails with a retriable error, until
/// shutdown.
async fn retry<T, F>(table: &str, operation: &str, call: impl FnMut() -> F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    retry_if_retriable_until_cancelled(
        call,
        &format!("table '{table}' {operation}"),
        &mut crate::shutdown::subscribe(),
    )
    .await
}

/// A [`WALL_CLOCK_COLUMN`] array holding the current time.
fn wall_clock(rows: usize) -> ArrayRef {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    Arc::new(TimestampNanosecondArray::from_value(now, rows).with_timezone(WALL_CLOCK_TIME_ZONE))
}
