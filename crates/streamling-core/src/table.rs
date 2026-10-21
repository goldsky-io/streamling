//! The `table` transform: the latest row per primary key, maintained from a
//! changelog stream. The table is applied by its upstream's side output.

pub mod aggregate;
pub mod driver;
pub mod read;
pub mod side_output;
mod versions;
mod watermark;

use aggregate::TableAggregate;
use versions::{ChangeRef, Versions};
pub use versions::{Mark, Marks, VersionRetention};
use watermark::{WATERMARK_NAME, WatermarkFunc, Watermarks};

use crate::checkpoints::checkpoint_management::CheckpointEpoch;

use crate::data::COLUMN_NAME_OP;
use crate::error::{Result, ResultExt};
use crate::telemetry::recorder::get_metrics_recorder;
use crate::{streamling_err, streamling_user_bail, streamling_user_err};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, RecordBatch, TimestampNanosecondArray, UInt64Array,
    new_null_array,
};
use arrow::compute::{
    cast, filter, filter_record_batch, interleave, interleave_record_batch, not,
    prep_null_mask_filter,
};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use arrow::row::{RowConverter, Rows, SortField};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion::logical_expr::{Accumulator, Expr, ExprSchemable, ScalarUDF, col};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;

const OP_DELETE: &str = "d";
const UPSERT_OPS: [&str; 4] = ["i", "c", "r", "u"];

const LIVE_ROWS_GAUGE: &str = "table_live_rows";
const MEMORY_BYTES_GAUGE: &str = "table_memory_bytes";
const VERSIONS_GAUGE: &str = "table_versions";
const RETIRED_BYTES_GAUGE: &str = "table_retired_bytes";

/// Floor of a cleaning step's budget, so tiny batches still merge old chunks
/// into chunks of a useful size.
const MIN_CLEANING_ROWS: usize = 1024;

/// The column a window reads for the wall-clock time a row was applied.
pub const WALL_CLOCK_COLUMN: &str = "__timestamp";
const WALL_CLOCK_TIME_ZONE: &str = "UTC";

#[derive(Clone, Copy)]
enum RowOp {
    Upsert,
    Delete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RowLocation {
    chunk: usize,
    row: usize,
}

/// A live key's row and its newest change in the version history.
#[derive(Clone, Copy)]
struct IndexEntry {
    location: RowLocation,
    newest_change: Option<ChangeRef>,
}

struct Chunk {
    batch: RecordBatch,
    /// Rows of `batch` the index points to.
    live_rows: usize,
    /// History entries that point into `batch`, which keep it readable after
    /// the cleaner drops the chunk.
    held: usize,
    /// Bounds of the window's columns over all of `batch`'s rows, in
    /// `Window::columns` order. Empty without a window.
    statistics: Vec<ColumnStatistics>,
}

#[derive(Debug, PartialEq)]
struct ColumnStatistics {
    min: ScalarValue,
    max: ScalarValue,
    null_count: usize,
}

struct TableState {
    /// Oldest first. A chunk's id is its position plus `first_chunk_id`, so
    /// ids stay valid while the cleaner drops chunks from the front.
    chunks: VecDeque<Chunk>,
    first_chunk_id: usize,
    /// The front chunk's next row to clean.
    cleaning_cursor: usize,
    /// Primary key, encoded by the table's `RowConverter`, to its live row.
    index: HashMap<Box<[u8]>, IndexEntry>,
    total_rows: usize,
    chunk_bytes: usize,
    key_bytes: usize,
    versions: Versions,
    /// Chunks the cleaner dropped that history entries still point into, by
    /// id, with how many do.
    retired: HashMap<usize, (RecordBatch, usize)>,
    retired_bytes: usize,
}

impl TableState {
    fn new(retention: &VersionRetention) -> Self {
        Self {
            chunks: VecDeque::new(),
            first_chunk_id: 0,
            cleaning_cursor: 0,
            index: HashMap::new(),
            total_rows: 0,
            chunk_bytes: 0,
            key_bytes: 0,
            versions: Versions::new(retention),
            retired: HashMap::new(),
            retired_bytes: 0,
        }
    }

    fn chunk(&self, id: usize) -> &Chunk {
        &self.chunks[id - self.first_chunk_id]
    }

    fn chunk_mut(&mut self, id: usize) -> &mut Chunk {
        &mut self.chunks[id - self.first_chunk_id]
    }

    /// The rows of a live or retired chunk.
    fn batch(&self, id: usize) -> &RecordBatch {
        match id.checked_sub(self.first_chunk_id) {
            Some(offset) => &self.chunks[offset].batch,
            None => &self.retired[&id].0,
        }
    }

    /// How many history entries point into a live or retired chunk.
    fn held_mut(&mut self, id: usize) -> &mut usize {
        match id.checked_sub(self.first_chunk_id) {
            Some(offset) => &mut self.chunks[offset].held,
            None => {
                &mut self
                    .retired
                    .get_mut(&id)
                    .expect("a held chunk is live or retired")
                    .1
            }
        }
    }

    fn hold(&mut self, location: RowLocation) {
        *self.held_mut(location.chunk) += 1;
    }

    /// Frees a retired chunk once no history entry points into it.
    fn unhold(&mut self, location: RowLocation) {
        let held = self.held_mut(location.chunk);
        *held -= 1;
        if *held == 0
            && let Some((batch, _)) = self.retired.remove(&location.chunk)
        {
            self.retired_bytes -= batch.get_array_memory_size();
        }
    }

    /// Records a change of a key whose live row was `before` and whose
    /// newest change was `previous`. Returns the new change, if recorded.
    fn record(
        &mut self,
        previous: Option<ChangeRef>,
        before: Option<RowLocation>,
    ) -> Option<ChangeRef> {
        let recorded = self.versions.record(previous, before);
        if recorded.is_some()
            && let Some(before) = before
        {
            self.hold(before);
        }
        recorded
    }

    /// Removes `key`'s live row, and returns it.
    fn remove(&mut self, key: &[u8]) -> Option<RowLocation> {
        let (key, entry) = self.index.remove_entry(key)?;
        self.key_bytes -= key.len();
        if let Some(change) = self.record(entry.newest_change, Some(entry.location)) {
            self.versions.mark_removed(key, change);
        }
        self.release(entry.location);
        Some(entry.location)
    }

    /// Drops the history up to the finalized `epoch`.
    fn collect(&mut self, epoch: u64) {
        for location in self.versions.collect(epoch) {
            self.unhold(location);
        }
    }

    fn next_chunk_id(&self) -> usize {
        self.first_chunk_id + self.chunks.len()
    }

    fn push_chunk(
        &mut self,
        batch: RecordBatch,
        live_rows: usize,
        statistics: Vec<ColumnStatistics>,
    ) {
        self.total_rows += batch.num_rows();
        self.chunk_bytes += batch.get_array_memory_size();
        self.chunks.push_back(Chunk {
            batch,
            live_rows,
            held: 0,
            statistics,
        });
    }

    /// Drops the front chunk, retiring it while history entries point into
    /// it.
    fn pop_front_chunk(&mut self) {
        if let Some(chunk) = self.chunks.pop_front() {
            let bytes = chunk.batch.get_array_memory_size();
            self.total_rows -= chunk.batch.num_rows();
            self.chunk_bytes -= bytes;
            if chunk.held > 0 {
                self.retired_bytes += bytes;
                self.retired
                    .insert(self.first_chunk_id, (chunk.batch, chunk.held));
            }
            self.first_chunk_id += 1;
            self.cleaning_cursor = 0;
        }
    }

    /// The row at `location` is no longer the live row of its key.
    fn release(&mut self, location: RowLocation) {
        self.chunk_mut(location.chunk).live_rows -= 1;
    }

    /// The stored rows at `locations`, in order.
    fn rows_at(&self, schema: &SchemaRef, locations: &[RowLocation]) -> Result<RecordBatch> {
        if locations.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(schema)));
        }
        let mut sources: Vec<&RecordBatch> = Vec::new();
        // Chunk number to its position in `sources`.
        let mut chunk_sources: HashMap<usize, usize> = HashMap::new();
        let mut indices = Vec::with_capacity(locations.len());
        for location in locations {
            let source = *chunk_sources.entry(location.chunk).or_insert_with(|| {
                sources.push(self.batch(location.chunk));
                sources.len() - 1
            });
            indices.push((source, location.row));
        }
        interleave_record_batch(&sources, &indices)
            .streamling_context("failed to read the stored rows")
    }
}

/// What an apply changes in the live rows, for the table's aggregates.
struct LiveRowChanges {
    /// Rows of earlier chunks that stopped being live.
    removed: Vec<RowLocation>,
    /// Whether each row of the new chunk is live.
    added: Vec<bool>,
}

impl LiveRowChanges {
    /// The row at `location` is no longer live. A row of the new chunk was
    /// never live before the apply, so it's neither removed nor added.
    fn release(&mut self, location: RowLocation, new_chunk: usize) {
        if location.chunk == new_chunk {
            self.added[location.row] = false;
        } else {
            self.removed.push(location);
        }
    }
}

/// `(chunk, rows, keys)`: rows of a chunk a GC pass found outside the window,
/// and their primary keys.
type OutsideRows = (usize, Vec<usize>, Rows);

/// A table's `window`.
pub struct WindowConfig<'a> {
    /// SQL over the table's columns, [`WALL_CLOCK_COLUMN`] and
    /// `watermark(column)`. The table keeps the rows it is true for.
    pub predicate: &'a str,
    pub session_state: &'a SessionState,
    /// Minimum time between GC passes.
    pub gc_interval: Duration,
}

/// A planned [`WindowConfig`].
struct Window {
    /// Type-coerced but not simplified, so each GC pass folds `now()` and
    /// `watermark()` anew.
    predicate: Expr,
    /// True for the rows outside the window: `NOT predicate`, or a NULL in
    /// any column the predicate reads. Pruned against chunk statistics.
    outside: Expr,
    /// The stored schema, which `predicate` reads.
    schema: DFSchemaRef,
    /// The columns `predicate` reads, by name and position in `schema`.
    columns: Vec<(String, usize)>,
    reads_wall_clock: bool,
    /// The columns `watermark()` reads, by name and position in `columns`.
    watermark_columns: Vec<(String, usize)>,
    watermarks: Watermarks,
    /// Knows `watermark()`, unlike the pipeline's session.
    session_state: SessionState,
    gc_interval: Duration,
    /// When the last GC pass started. Locked while a pass runs.
    last_pass: Mutex<Instant>,
}

/// A [`Window`] with `now()` folded to the current time and `watermark()` to
/// the current watermarks.
struct CompiledWindow {
    predicate: Arc<dyn PhysicalExpr>,
    /// Rules out the chunks without a row outside the window.
    outside: PruningPredicate,
}

impl Window {
    /// Plans `config.predicate` over `fields`, the stored columns, which get
    /// [`WALL_CLOCK_COLUMN`] appended when the predicate reads it.
    fn try_new(table: &str, config: &WindowConfig, fields: &Fields) -> Result<Self> {
        if fields.find(WALL_CLOCK_COLUMN).is_some() {
            streamling_user_bail!(
                "table '{}': the SQL output cannot contain '{}', which a window reserves for the wall-clock time",
                table,
                WALL_CLOCK_COLUMN
            );
        }
        let invalid = |err: DataFusionError| {
            streamling_user_err!(
                "table '{}': invalid window '{}': {}",
                table,
                config.predicate,
                err
            )
        };
        let wall_clock = Field::new(
            WALL_CLOCK_COLUMN,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(WALL_CLOCK_TIME_ZONE.into())),
            false,
        );
        let with_wall_clock = DFSchema::try_from(Schema::new(
            fields
                .iter()
                .cloned()
                .chain([Arc::new(wall_clock)])
                .collect::<Fields>(),
        ))?;
        let watermarks = Watermarks::default();
        let mut session_state = config.session_state.clone();
        session_state.register_udf(Arc::new(ScalarUDF::from(WatermarkFunc::new(Arc::clone(
            &watermarks,
        )))))?;
        let predicate = session_state
            .create_logical_expr(config.predicate, &with_wall_clock)
            .map_err(invalid)?;
        let mut column_names: Vec<String> = predicate
            .column_refs()
            .into_iter()
            .map(|column| column.name.clone())
            .collect();
        column_names.sort();
        let reads_wall_clock = column_names.iter().any(|name| name == WALL_CLOCK_COLUMN);
        let schema = Arc::new(if reads_wall_clock {
            with_wall_clock
        } else {
            DFSchema::try_from(Schema::new(fields.clone()))?
        });
        let predicate = ExprSimplifier::new(Self::simplify_context(&session_state, &schema))
            .coerce(predicate, &schema)
            .map_err(invalid)?;
        let data_type = predicate.get_type(&schema).map_err(invalid)?;
        if data_type != DataType::Boolean {
            streamling_user_bail!(
                "table '{}': the window must be a boolean expression, got {}",
                table,
                data_type
            );
        }
        let outside = column_names
            .iter()
            .fold(Expr::Not(Box::new(predicate.clone())), |outside, name| {
                outside.or(col(Column::new_unqualified(name)).is_null())
            });
        let columns: Vec<(String, usize)> = column_names
            .into_iter()
            .map(|name| {
                let index = schema.inner().index_of(&name)?;
                Ok((name, index))
            })
            .collect::<Result<_>>()?;
        let mut watermark_columns = Vec::new();
        predicate
            .apply(|expr| {
                if let Expr::ScalarFunction(function) = expr
                    && function.name() == WATERMARK_NAME
                {
                    let [Expr::Column(column)] = function.args.as_slice() else {
                        return Err(DataFusionError::Plan(format!(
                            "{WATERMARK_NAME}() takes one column of the table, got {expr}"
                        )));
                    };
                    let position = columns
                        .iter()
                        .position(|(name, _)| *name == column.name)
                        .expect("the predicate's columns include watermark()'s");
                    watermark_columns.push((column.name.clone(), position));
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .map_err(invalid)?;
        for (name, position) in &watermark_columns {
            let data_type = schema.field(columns[*position].1).data_type();
            watermarks
                .lock()
                .insert(name.clone(), ScalarValue::try_from(data_type)?);
        }
        let window = Self {
            predicate,
            outside,
            schema,
            columns,
            reads_wall_clock,
            watermark_columns,
            watermarks,
            session_state,
            gc_interval: config.gc_interval,
            last_pass: Mutex::new(Instant::now()),
        };
        if !window.prunable().map_err(invalid)? {
            warn!(
                "table '{}': window '{}' can't skip chunks by their min/max, so each GC pass evaluates every stored row. Compare a column, not a function of it, with an expression of now() or watermark(), such as `ts >= watermark(ts) - interval '1 hour'`",
                table, config.predicate
            );
        }
        Ok(window)
    }

    fn simplify_context(session_state: &SessionState, schema: &DFSchemaRef) -> SimplifyContext {
        SimplifyContext::builder()
            .with_schema(Arc::clone(schema))
            .with_config_options(Arc::clone(session_state.config_options()))
            .with_current_time()
            .build()
    }

    /// Simplifying folds `now()`, and pushes `outside`'s `NOT` into the
    /// comparisons, which `PruningPredicate` can't see through otherwise.
    fn compile(&self) -> datafusion::common::Result<CompiledWindow> {
        let simplifier =
            ExprSimplifier::new(Self::simplify_context(&self.session_state, &self.schema));
        let predicate = simplifier.simplify(self.predicate.clone())?;
        let outside = simplifier.simplify(self.outside.clone())?;
        Ok(CompiledWindow {
            predicate: self
                .session_state
                .create_physical_expr(predicate, &self.schema)?,
            outside: PruningPredicate::try_new(
                self.session_state
                    .create_physical_expr(outside, &self.schema)?,
                Arc::clone(self.schema.inner()),
            )?,
        })
    }

    /// Whether chunk statistics can rule chunks out. A NULL watermark folds
    /// the comparisons to NULL, which rules nothing out, so the check
    /// substitutes zeros. Only called before the table is in use.
    fn prunable(&self) -> datafusion::common::Result<bool> {
        let watermarks = self.watermarks.lock().clone();
        for (name, value) in &watermarks {
            if let Ok(zero) = ScalarValue::new_zero(&value.data_type()) {
                self.watermarks.lock().insert(name.clone(), zero);
            }
        }
        let prunable = self
            .compile()
            .map(|compiled| !compiled.outside.always_true());
        *self.watermarks.lock() = watermarks;
        prunable
    }

    fn reads_watermark(&self) -> bool {
        !self.watermark_columns.is_empty()
    }

    /// Raises each watermark to its column's max in a new chunk's
    /// `statistics`.
    fn advance_watermarks(&self, statistics: &[ColumnStatistics]) {
        let mut watermarks = self.watermarks.lock();
        for (name, position) in &self.watermark_columns {
            let max = &statistics[*position].max;
            let watermark = watermarks
                .get_mut(name)
                .expect("every watermark column has a watermark");
            // A null max is less than any value, and equal to a null watermark.
            if *max > *watermark {
                *watermark = max.clone();
            }
        }
    }

    /// The bounds of `columns` in `batch`, a chunk in the stored schema.
    fn statistics(&self, batch: &RecordBatch) -> datafusion::common::Result<Vec<ColumnStatistics>> {
        self.columns
            .iter()
            .map(|(_, index)| {
                let values = [Arc::clone(batch.column(*index))];
                let mut min = MinAccumulator::try_new(values[0].data_type())?;
                min.update_batch(&values)?;
                let mut max = MaxAccumulator::try_new(values[0].data_type())?;
                max.update_batch(&values)?;
                Ok(ColumnStatistics {
                    min: min.evaluate()?,
                    max: max.evaluate()?,
                    null_count: values[0].null_count(),
                })
            })
            .collect()
    }
}

/// The statistics of the chunks a GC pass prunes, one container per chunk.
struct ChunkStatistics<'a> {
    window: &'a Window,
    chunks: &'a [&'a Chunk],
}

impl ChunkStatistics<'_> {
    fn values(
        &self,
        column: &Column,
        value: impl Fn(&ColumnStatistics) -> ScalarValue,
    ) -> Option<ArrayRef> {
        let position = self
            .window
            .columns
            .iter()
            .position(|(name, _)| *name == column.name)?;
        // Unknown on error, which keeps every chunk.
        ScalarValue::iter_to_array(
            self.chunks
                .iter()
                .map(|chunk| value(&chunk.statistics[position])),
        )
        .ok()
    }
}

impl PruningStatistics for ChunkStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.values(column, |statistics| statistics.min.clone())
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.values(column, |statistics| statistics.max.clone())
    }

    fn num_containers(&self) -> usize {
        self.chunks.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.values(column, |statistics| {
            ScalarValue::UInt64(Some(statistics.null_count as u64))
        })
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        Some(Arc::new(UInt64Array::from_iter_values(
            self.chunks
                .iter()
                .map(|chunk| chunk.batch.num_rows() as u64),
        )))
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

/// A [`WALL_CLOCK_COLUMN`] array holding the current time.
fn wall_clock(rows: usize) -> ArrayRef {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64;
    Arc::new(TimestampNanosecondArray::from_value(now, rows).with_timezone(WALL_CLOCK_TIME_ZONE))
}

/// The `in_memory` backend of a `table` transform.
///
/// Storage is append-only, because Arrow arrays can't be edited in place:
///
/// - A chunk is one applied batch, stored without `_gs_op`.
/// - The index maps each primary key to its live row, the key's latest
///   version. Every other stored row (an older version, a deleted key's row,
///   a delete row) is dead.
/// - Each chunk counts its live rows, so a fully dead chunk is dropped
///   without being read. Chunk ids keep growing, so dropping chunks from the
///   front leaves the other ids valid.
///
/// [`Self::apply`] stores the batch as a new chunk, then goes through its
/// rows in order. An upsert points the index at its row, and the key's
/// previous row becomes dead. A delete removes the key from the index, and
/// its row becomes dead. So the last row of a key in a batch wins. A lookup
/// reads the row the index points to.
///
/// Every apply then cleans a little. While dead rows exceed `max_dead_ratio`
/// of the live ones, the live rows of the oldest chunks, which are mostly
/// dead, are copied into one new chunk, the index is pointed at the copies
/// and the old chunks are dropped. A step visits a budget of rows
/// proportional to the rows just written (`clean` explains the factor), so
/// the work is spread over writes, and a step that runs out mid-chunk
/// resumes there next time. Storage stays at about `1 + max_dead_ratio`
/// times the live rows.
///
/// For example, with a `max_dead_ratio` of 0.25:
///
/// | Step              | Chunks                         | Index          | Stored / live / dead |
/// |-------------------|--------------------------------|----------------|----------------------|
/// | apply `A1 B2 C3`  | `c0:[A1 B2 C3]`                | A→c0 B→c0 C→c0 | 3 / 3 / 0            |
/// | apply `A10 -B`    | `c0:[A1 B2 C3]` `c1:[A10 -B]`  | A→c1 C→c0      | 5 / 2 / 3            |
/// | clean             | `c2:[C3 A10]`                  | A→c2 C→c2      | 2 / 2 / 0            |
///
/// With a window, a GC pass removes the keys whose live row the window's
/// predicate isn't true for, and the cleaner reclaims their rows like any
/// other dead rows. An apply ends with a pass when the window's GC interval
/// has passed since the last one. A window that reads a watermark also runs
/// one inside every apply that writes rows, under its write lock, so no
/// lookup sees a row the batch pushed out of the window. Each chunk keeps the min, max and null count of the
/// columns the predicate reads, so a pass skips the chunks they show to be
/// wholly inside the window. A predicate that reads [`WALL_CLOCK_COLUMN`]
/// makes each chunk store the time its rows were applied in that column.
///
/// Each [`TableAggregate`] reading the table gets every change to the live
/// rows, from applies and GC passes, before the write lock is released.
///
/// With [`VersionRetention::PerBatch`], every change of a key (an upsert,
/// a delete or an eviction) is recorded in a version history, in apply order
/// across the input's partitions, with the key's live row before it, and so
/// is every change to an aggregate's output. Each finished apply returns the
/// history's end as its [`Mark`], for the table and its aggregates' outputs
/// in one lock window. A lookup at a batch's mark undoes every change made
/// after it, whatever its partition, so it sees the table and its aggregates
/// as they were when the batch was applied. The dead rows such
/// lookups need stay readable, even after the cleaner drops their chunk,
/// until a finalized checkpoint collects them. Each change links to its
/// key's previous change. A key's index entry points to its newest change,
/// so a lookup of a live key probes the index once; a key without a live row
/// keeps its newest change in a side map of the history until it's
/// collected.
///
/// Nothing is persisted, so the table starts empty after a restart.
pub struct InMemoryTable {
    name: String,
    metric_metadata_id: String,
    /// The input schema without `_gs_op`, plus [`WALL_CLOCK_COLUMN`] when the
    /// window reads it.
    schema: SchemaRef,
    op_index: usize,
    /// Input columns stored in `schema`, in order.
    value_indices: Vec<usize>,
    key_indices: Vec<usize>,
    /// Primary key columns' positions in `schema`.
    stored_key_indices: Vec<usize>,
    /// The primary key columns, in order.
    key_fields: Fields,
    primary_key: String,
    converter: RowConverter,
    /// Cleaning runs while dead rows exceed this fraction of the live rows.
    max_dead_ratio: f64,
    window: Option<Window>,
    retention: VersionRetention,
    /// Locked before `state`.
    aggregates: RwLock<Vec<Arc<TableAggregate>>>,
    /// Locked before the `state` of each aggregate's output.
    state: RwLock<TableState>,
}

impl InMemoryTable {
    /// `input_schema` is the table's SQL output schema. It must contain
    /// `_gs_op` and every `primary_key` column. `max_dead_ratio` must be a
    /// positive number.
    pub fn try_new(
        name: &str,
        input_schema: &SchemaRef,
        primary_key: &[String],
        metric_metadata_id: String,
        max_dead_ratio: f64,
        window: Option<WindowConfig>,
        retention: VersionRetention,
    ) -> Result<Self> {
        if !(max_dead_ratio.is_finite() && max_dead_ratio > 0.0) {
            streamling_user_bail!(
                "table '{}': max_dead_ratio must be a positive number, got {}",
                name,
                max_dead_ratio
            );
        }
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
        let key_fields: Fields = key_indices
            .iter()
            .map(|&index| Arc::clone(&input_schema.fields()[index]))
            .collect();
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
            Some(window) => Arc::clone(window.schema.inner()),
            None => Arc::new(Schema::new(value_fields)),
        };
        let stored_key_indices = key_indices
            .iter()
            .map(|&index| index - usize::from(index > op_index))
            .collect();
        Ok(Self {
            name: name.to_string(),
            metric_metadata_id,
            schema,
            op_index,
            value_indices,
            key_indices,
            stored_key_indices,
            key_fields,
            primary_key: primary_key.join(","),
            converter,
            max_dead_ratio,
            window,
            state: RwLock::new(TableState::new(&retention)),
            retention,
            aggregates: RwLock::new(Vec::new()),
        })
    }

    /// Applies `batch` (the table's SQL output) in row order: an upsert
    /// replaces its key's row, and a delete removes it. `epochs` are the
    /// checkpoint markers the batch carried. Returns the marks the batch's
    /// readers see the table and its aggregates' outputs at: the end of this
    /// apply.
    pub fn apply(&self, batch: &RecordBatch, epochs: &[CheckpointEpoch]) -> Result<Marks> {
        self.begin();
        self.write(batch)?;
        // After the batch, so a pass evicts its rows too.
        self.collect_garbage_if_due()?;
        let marks = self.finish(epochs);
        self.record_gauges();
        Ok(marks)
    }

    /// Starts an apply on the table and on its aggregates' outputs,
    /// collecting the history a finalized checkpoint freed.
    fn begin(&self) {
        let aggregates = self.aggregates.read();
        let mut state = self.state.write();
        if let Some(epoch) = state.versions.latest_finalized() {
            state.collect(epoch);
        }
        for aggregate in aggregates.iter() {
            aggregate.begin();
        }
    }

    /// Finishes an apply of a batch that carried the markers of `epochs`,
    /// on the table and on its aggregates' outputs, in one lock window, so
    /// the batch's readers see them at the same moment.
    fn finish(&self, epochs: &[CheckpointEpoch]) -> Marks {
        let aggregates = self.aggregates.read();
        let mut state = self.state.write();
        let mut marks: Marks = state
            .versions
            .finish(epochs)
            .map(|mark| (self.name.clone(), mark))
            .into_iter()
            .collect();
        for aggregate in aggregates.iter() {
            marks.extend(aggregate.finish(epochs));
        }
        marks
    }

    /// Applies `batch`, recording its changes in the version history.
    fn write(&self, batch: &RecordBatch) -> Result<()> {
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
        let keys = self
            .converter
            .convert_columns(&key_columns)
            .streamling_context("failed to encode the primary key")?;
        let mut columns: Vec<ArrayRef> = self
            .value_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index)))
            .collect();
        if self.reads_wall_clock() {
            columns.push(wall_clock(batch.num_rows()));
        }
        let values = RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .streamling_with_context(|| {
                format!(
                    "table '{}': batch does not match the table schema",
                    self.name
                )
            })?;
        let written_rows = values.num_rows();
        let statistics = if written_rows > 0 {
            self.chunk_statistics(&values)?
        } else {
            Vec::new()
        };
        if let Some(window) = &self.window
            && window.reads_watermark()
            && written_rows > 0
        {
            window.advance_watermarks(&statistics);
        }

        let aggregates = self.aggregates.read();
        let mut changes = (!aggregates.is_empty()).then(|| LiveRowChanges {
            removed: Vec::new(),
            added: vec![false; written_rows],
        });
        let mut guard = self.state.write();
        let state = &mut *guard;
        let new_chunk = state.next_chunk_id();
        // Stored before the loop, so a row the batch replaces in itself is
        // released like any other.
        if written_rows > 0 {
            state.push_chunk(values, 0, statistics);
        }
        for (row, op) in ops.into_iter().enumerate() {
            let key = keys.row(row);
            match op {
                RowOp::Upsert => {
                    let location = RowLocation {
                        chunk: new_chunk,
                        row,
                    };
                    let replaced = match state.index.get_mut(key.as_ref()) {
                        Some(entry) => {
                            let replaced = std::mem::replace(&mut entry.location, location);
                            // `TableState::record`, inlined: `entry` borrows the index.
                            entry.newest_change =
                                state.versions.record(entry.newest_change, Some(replaced));
                            if entry.newest_change.is_some() {
                                state.hold(replaced);
                            }
                            Some(replaced)
                        }
                        None => {
                            state.key_bytes += key.as_ref().len();
                            let previous = state.versions.take_removed(key.as_ref());
                            let newest_change = state.record(previous, None);
                            state.index.insert(
                                key.as_ref().into(),
                                IndexEntry {
                                    location,
                                    newest_change,
                                },
                            );
                            None
                        }
                    };
                    if let Some(replaced) = replaced {
                        state.release(replaced);
                        if let Some(changes) = &mut changes {
                            changes.release(replaced, new_chunk);
                        }
                    }
                    state.chunk_mut(new_chunk).live_rows += 1;
                    if let Some(changes) = &mut changes {
                        changes.added[row] = true;
                    }
                }
                RowOp::Delete => {
                    if let Some(stored) = state.remove(key.as_ref())
                        && let Some(changes) = &mut changes
                    {
                        changes.release(stored, new_chunk);
                    }
                }
            }
        }
        // The batch may have moved a watermark, or be older than it already.
        // Evicting before the lock is released, and before the aggregates
        // update, means no lookup sees the rows in between.
        if let Some(window) = &self.window
            && window.reads_watermark()
            && written_rows > 0
        {
            let compiled = window.compile()?;
            let chunks = self.chunks_to_evaluate(state, window, &compiled)?;
            let outside_rows = self.outside_rows(&compiled, chunks)?;
            for location in self.evict(state, &outside_rows) {
                if let Some(changes) = &mut changes {
                    changes.release(location, new_chunk);
                }
            }
        }
        // Before cleaning, which may drop the chunks of the removed rows.
        if let Some(changes) = changes {
            let removed = state.rows_at(&self.schema, &changes.removed)?;
            let added = if written_rows > 0 {
                filter_record_batch(state.batch(new_chunk), &BooleanArray::from(changes.added))?
            } else {
                RecordBatch::new_empty(Arc::clone(&self.schema))
            };
            for aggregate in aggregates.iter() {
                aggregate.update(&removed, &added)?;
            }
        }

        self.clean(state, written_rows)
    }

    pub fn retention(&self) -> &VersionRetention {
        &self.retention
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Called before data flows, so the aggregate sees every change.
    fn add_aggregate(&self, aggregate: Arc<TableAggregate>) {
        self.aggregates.write().push(aggregate);
    }

    /// The primary key's field, if `key` names the table's single-column
    /// primary key.
    pub fn lookup_key_field(&self, key: &str) -> Result<&FieldRef> {
        let field = self.single_key()?;
        if field.name() != key {
            streamling_user_bail!(
                "table '{}': lookups must use the primary key '{}', got '{}'",
                self.name,
                field.name(),
                key
            );
        }
        Ok(field)
    }

    /// The field [`Self::lookup`] returns for `column`: nullable, because a
    /// key may have no row.
    pub fn lookup_field(&self, column: &str) -> Result<FieldRef> {
        let field = self.schema.field(self.column_index(column)?);
        Ok(Arc::new(field.clone().with_nullable(true)))
    }

    /// For each of `keys`, `column` of the stored row with that primary key,
    /// or null when no row has it, as of `mark`, or the latest apply without
    /// one. `keys` is cast to the primary key's type.
    pub fn lookup(&self, keys: &ArrayRef, column: &str, mark: Option<Mark>) -> Result<ArrayRef> {
        let column_index = self.column_index(column)?;
        // A safe cast: a value that can't be converted becomes null and misses.
        let keys = cast(keys, self.single_key()?.data_type())
            .streamling_context("failed to cast the lookup keys to the primary key type")?;
        // The converter that encoded the index's keys in `apply`, so an equal
        // key yields equal bytes. A null encodes to bytes the index never holds,
        // because `apply` rejects null keys.
        let encoded = self
            .converter
            .convert_columns(&[keys])
            .streamling_context("failed to encode the lookup keys")?;
        let missing = new_null_array(self.schema.field(column_index).data_type(), 1);

        let state = self.state.read();
        state
            .versions
            .ensure_readable(mark)
            .map_err(|e| streamling_err!("table '{}': {}", self.name, e))?;
        // Source 0 is a single null for keys with no row. Each chunk a hit
        // reads from is added once, so the cost follows hits, not chunk count.
        let mut sources: Vec<&dyn Array> = vec![missing.as_ref()];
        // Chunk number to its position in `sources`.
        let mut chunk_sources: HashMap<usize, usize> = HashMap::new();
        // One `(source, row)` per key, in input order. `interleave` copies
        // `sources[source][row]` for each into the output.
        let mut gather_indices = Vec::with_capacity(encoded.num_rows());
        for key in encoded.iter() {
            let (current, newest_change) = match state.index.get(key.as_ref()) {
                Some(entry) => (Some(entry.location), entry.newest_change),
                None if mark.is_some() => {
                    (None, state.versions.removed_newest_change(key.as_ref()))
                }
                None => (None, None),
            };
            let Some(location) = state.versions.resolve(mark, current, newest_change) else {
                gather_indices.push((0, 0));
                continue;
            };
            let source = *chunk_sources.entry(location.chunk).or_insert_with(|| {
                sources.push(state.batch(location.chunk).column(column_index).as_ref());
                sources.len() - 1
            });
            gather_indices.push((source, location.row));
        }
        interleave(&sources, &gather_indices)
            .streamling_context("failed to read the looked-up rows")
    }

    fn single_key(&self) -> Result<&FieldRef> {
        match self.key_fields.as_ref() {
            [key] => Ok(key),
            _ => Err(streamling_user_err!(
                "table '{}': lookups need a single-column primary key, the table's is '{}'",
                self.name,
                self.primary_key
            )),
        }
    }

    /// The stored column a lookup reads. A window's wall clock is GC
    /// bookkeeping, not part of the SQL output, so lookups can't read it.
    fn column_index(&self, column: &str) -> Result<usize> {
        self.schema
            .index_of(column)
            .ok()
            .filter(|_| !(column == WALL_CLOCK_COLUMN && self.reads_wall_clock()))
            .ok_or_else(|| streamling_user_err!("table '{}' has no column '{}'", self.name, column))
    }

    fn reads_wall_clock(&self) -> bool {
        self.window
            .as_ref()
            .is_some_and(|window| window.reads_wall_clock)
    }

    fn chunk_statistics(&self, batch: &RecordBatch) -> Result<Vec<ColumnStatistics>> {
        match &self.window {
            Some(window) => Ok(window.statistics(batch)?),
            None => Ok(Vec::new()),
        }
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

    /// Runs a GC pass when one is due and no other apply is running one.
    fn collect_garbage_if_due(&self) -> Result<()> {
        let Some(window) = &self.window else {
            return Ok(());
        };
        let Some(mut last_pass) = window.last_pass.try_lock() else {
            return Ok(());
        };
        if last_pass.elapsed() < window.gc_interval {
            return Ok(());
        }
        *last_pass = Instant::now();
        self.evict_outside(window)
    }

    /// Removes the keys whose live row the window's predicate isn't true
    /// for. The predicate runs on a snapshot of the chunks, so lookups
    /// aren't blocked meanwhile. A row an apply moves or replaces in the
    /// meantime is no longer where the index points, so it stays until the
    /// next pass.
    fn evict_outside(&self, window: &Window) -> Result<()> {
        let compiled = window.compile()?;
        let chunks = self.chunks_to_evaluate(&self.state.read(), window, &compiled)?;
        let outside_rows = self.outside_rows(&compiled, chunks)?;
        let aggregates = self.aggregates.read();
        let mut state = self.state.write();
        let evicted = self.evict(&mut state, &outside_rows);
        if !aggregates.is_empty() && !evicted.is_empty() {
            let removed = state.rows_at(&self.schema, &evicted)?;
            let added = RecordBatch::new_empty(Arc::clone(&self.schema));
            for aggregate in aggregates.iter() {
                aggregate.update(&removed, &added)?;
            }
        }
        Ok(())
    }

    /// The rows of `chunks` outside the window, live or not.
    fn outside_rows(
        &self,
        compiled: &CompiledWindow,
        chunks: Vec<(usize, RecordBatch)>,
    ) -> Result<Vec<OutsideRows>> {
        let mut outside_rows = Vec::new();
        for (chunk, batch) in chunks {
            let inside = compiled
                .predicate
                .evaluate(&batch)?
                .into_array(batch.num_rows())?;
            let inside = inside.as_boolean_opt().ok_or_else(|| {
                streamling_err!(
                    "table '{}': the window must return booleans, got {}",
                    self.name,
                    inside.data_type()
                )
            })?;
            // A null is outside the window. `prep_null_mask_filter` panics
            // without a null buffer.
            let outside = if inside.null_count() > 0 {
                not(&prep_null_mask_filter(inside))?
            } else {
                not(inside)?
            };
            if !outside.has_true() {
                continue;
            }
            let key_columns = self
                .stored_key_indices
                .iter()
                .map(|&index| filter(batch.column(index), &outside))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let keys = self
                .converter
                .convert_columns(&key_columns)
                .streamling_context("failed to encode the stored keys")?;
            let rows: Vec<usize> = outside.values().set_indices().collect();
            outside_rows.push((chunk, rows, keys));
        }
        Ok(outside_rows)
    }

    /// Removes the keys whose live row is among `outside_rows`, and returns
    /// those rows.
    fn evict(&self, state: &mut TableState, outside_rows: &[OutsideRows]) -> Vec<RowLocation> {
        let mut evicted = Vec::new();
        for (chunk, rows, keys) in outside_rows {
            for (&row, key) in rows.iter().zip(keys.iter()) {
                let location = RowLocation { chunk: *chunk, row };
                if state
                    .index
                    .get(key.as_ref())
                    .is_some_and(|entry| entry.location == location)
                {
                    state.remove(key.as_ref());
                    evicted.push(location);
                }
            }
        }
        evicted
    }

    /// The chunks with a live row that their statistics don't show to be
    /// wholly inside the window, by id. Their order of time doesn't matter,
    /// but the more chunks cover narrow ranges, the more are skipped.
    fn chunks_to_evaluate(
        &self,
        state: &TableState,
        window: &Window,
        compiled: &CompiledWindow,
    ) -> Result<Vec<(usize, RecordBatch)>> {
        let (ids, chunks): (Vec<usize>, Vec<&Chunk>) = (state.first_chunk_id..)
            .zip(&state.chunks)
            .filter(|(_, chunk)| chunk.live_rows > 0)
            .unzip();
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        let may_hold_outside_rows = compiled.outside.prune(&ChunkStatistics {
            window,
            chunks: &chunks,
        })?;
        Ok(ids
            .into_iter()
            .zip(chunks)
            .zip(may_hold_outside_rows)
            .filter(|(_, may_hold_outside_rows)| *may_hold_outside_rows)
            .map(|((id, chunk), _)| (id, chunk.batch.clone()))
            .collect())
    }

    /// While dead rows exceed `max_dead_ratio` of the live ones, moves the
    /// live rows of the oldest chunks into one new chunk, visiting at most a
    /// budget of rows. A chunk without live rows is dropped unvisited.
    ///
    /// Visiting k = 1 + 1/max_dead_ratio rows per written row reclaims dead
    /// rows as fast as writes create them once they make up 1/k of storage,
    /// which is max_dead_ratio of the live rows; the oldest chunks, cleaned
    /// first, hold at least their share of dead rows.
    ///
    /// A chunk keeps its input's buffers alive. If upstream hands over slices
    /// of larger buffers, memory exceeds what the row counts suggest. The
    /// upgrade path is to trigger on `get_array_memory_size` instead.
    ///
    /// An error leaves the index pointing at dropped chunks. It fails the
    /// pipeline through the side output, and an in_memory table starts empty
    /// after a restart.
    fn clean(&self, state: &mut TableState, written_rows: usize) -> Result<()> {
        let rows_per_write = 1.0 + 1.0 / self.max_dead_ratio;
        let budget =
            ((written_rows as f64 * rows_per_write).ceil() as usize).max(MIN_CLEANING_ROWS);
        let target = state.next_chunk_id();
        // Moved rows as `(position in sources, row)`, in their order in `target`.
        let mut sources: Vec<RecordBatch> = Vec::new();
        let mut moved: Vec<(usize, usize)> = Vec::new();
        let mut visited = 0;
        // `moved` rows count toward storage: `target` will hold them.
        while visited < budget
            && self.too_many_dead_rows(state.total_rows + moved.len(), state.index.len())
        {
            let Some(front) = state.chunks.front() else {
                break;
            };
            if front.live_rows == 0 {
                state.pop_front_chunk();
                continue;
            }
            let front_id = state.first_chunk_id;
            let batch = front.batch.clone();
            let start = state.cleaning_cursor;
            let end = batch.num_rows().min(start.saturating_add(budget - visited));
            let key_columns: Vec<ArrayRef> = self
                .stored_key_indices
                .iter()
                .map(|&index| batch.column(index).slice(start, end - start))
                .collect();
            let keys = self
                .converter
                .convert_columns(&key_columns)
                .streamling_context("failed to encode the stored keys")?;
            let source = sources.len();
            let mut moved_from_front = 0;
            for (offset, key) in keys.iter().enumerate() {
                let row = start + offset;
                // A row is live if the index still points to it.
                if let Some(entry) = state.index.get_mut(key.as_ref())
                    && entry.location
                        == (RowLocation {
                            chunk: front_id,
                            row,
                        })
                {
                    entry.location = RowLocation {
                        chunk: target,
                        row: moved.len(),
                    };
                    moved.push((source, row));
                    moved_from_front += 1;
                }
            }
            visited += end - start;
            let finished = end == batch.num_rows();
            sources.push(batch);
            state.chunk_mut(front_id).live_rows -= moved_from_front;
            // The rest of a chunk without live rows is dead.
            if finished || state.chunk(front_id).live_rows == 0 {
                state.pop_front_chunk();
            } else {
                state.cleaning_cursor = end;
            }
        }
        if !moved.is_empty() {
            let sources: Vec<&RecordBatch> = sources.iter().collect();
            let batch = interleave_record_batch(&sources, &moved)
                .streamling_context("failed to move the live rows")?;
            let statistics = self.chunk_statistics(&batch)?;
            state.push_chunk(batch, moved.len(), statistics);
        }
        Ok(())
    }

    fn too_many_dead_rows(&self, total_rows: usize, live_rows: usize) -> bool {
        total_rows.saturating_sub(live_rows) as f64 > self.max_dead_ratio * live_rows as f64
    }

    /// Records the gauges of the table and of its aggregates' outputs.
    fn record_gauges(&self) {
        let aggregates = self.aggregates.read();
        {
            let state = self.state.read();
            let recorder = get_metrics_recorder();
            for (gauge, value) in [
                (LIVE_ROWS_GAUGE, state.index.len()),
                (MEMORY_BYTES_GAUGE, state.chunk_bytes + state.key_bytes),
                (VERSIONS_GAUGE, state.versions.count()),
                (RETIRED_BYTES_GAUGE, state.retired_bytes),
            ] {
                recorder.record_gauge(gauge, value as u64, &self.metric_metadata_id);
            }
        }
        for aggregate in aggregates.iter() {
            aggregate.record_gauges();
        }
    }
}

/// The pipeline's tables by name
#[derive(Clone, Default)]
pub struct TableRegistry(Arc<RwLock<HashMap<String, Arc<InMemoryTable>>>>);

impl TableRegistry {
    pub fn register(&self, table: Arc<InMemoryTable>) {
        self.0.write().insert(table.name.clone(), table);
    }

    pub fn get(&self, name: &str) -> Option<Arc<InMemoryTable>> {
        self.0.read().get(name).cloned()
    }
}

impl fmt::Debug for TableRegistry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_set().entries(self.0.read().keys()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, LargeBinaryArray, StringArray, StringViewArray};
    use arrow::datatypes::{DataType, Field, Int64Type};
    use datafusion::prelude::SessionContext;
    use streamling_common::types::decimal_arb::DecimalArbType;

    const TABLE: &str = "tokens";
    const METRIC_KEY: &str = "app::tokens";
    const MAX_DEAD_RATIO: f64 = 0.25;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
        ]))
    }

    fn batch(rows: &[(i64, &str, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
            ],
        )
        .unwrap()
    }

    fn table() -> InMemoryTable {
        InMemoryTable::try_new(
            TABLE,
            &schema(),
            &["id".to_string()],
            METRIC_KEY.to_string(),
            MAX_DEAD_RATIO,
            None,
            VersionRetention::Latest,
        )
        .unwrap()
    }

    /// Live contents as `(id, name)`, sorted by id.
    fn contents(table: &InMemoryTable) -> Vec<(i64, String)> {
        let state = table.state.read();
        let gather: Vec<(usize, usize)> = state
            .index
            .values()
            .map(|entry| {
                (
                    entry.location.chunk - state.first_chunk_id,
                    entry.location.row,
                )
            })
            .collect();
        if gather.is_empty() {
            return Vec::new();
        }
        let chunks: Vec<&RecordBatch> = state.chunks.iter().map(|chunk| &chunk.batch).collect();
        let live = interleave_record_batch(&chunks, &gather).unwrap();
        let ids = live
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let names = live.column_by_name("name").unwrap().as_string::<i32>();
        let mut contents: Vec<(i64, String)> = (0..live.num_rows())
            .map(|i| (ids.value(i), names.value(i).to_string()))
            .collect();
        contents.sort();
        contents
    }

    /// Every chunk's live-row count matches the index.
    fn assert_live_rows_match_the_index(table: &InMemoryTable) {
        let state = table.state.read();
        assert_eq!(
            state
                .chunks
                .iter()
                .map(|chunk| chunk.live_rows)
                .sum::<usize>(),
            state.index.len()
        );
    }

    #[test]
    fn upserts_replace_and_deletes_remove_by_primary_key() {
        let table = table();
        table
            .apply(&batch(&[(1, "a", "i"), (2, "b", "c"), (3, "c", "r")]), &[])
            .unwrap();
        assert_eq!(
            contents(&table),
            vec![
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string())
            ]
        );
        table
            .apply(
                &batch(&[(1, "a2", "u"), (2, "b2", "C"), (3, "c2", "i")]),
                &[],
            )
            .unwrap();
        assert_eq!(
            contents(&table),
            vec![
                (1, "a2".to_string()),
                (2, "b2".to_string()),
                (3, "c2".to_string())
            ]
        );
        table
            .apply(&batch(&[(1, "ignored", "d"), (2, "ignored", "D")]), &[])
            .unwrap();
        assert_eq!(contents(&table), vec![(3, "c2".to_string())]);
        assert_live_rows_match_the_index(&table);
    }

    #[test]
    fn delete_of_an_unknown_key_changes_nothing() {
        let table = table();
        table.apply(&batch(&[(9, "incoming", "d")]), &[]).unwrap();
        assert!(contents(&table).is_empty());
    }

    #[test]
    fn later_rows_of_a_batch_win() {
        let table = table();
        table
            .apply(
                &batch(&[(1, "a", "i"), (1, "b", "u"), (2, "c", "i"), (2, "c", "d")]),
                &[],
            )
            .unwrap();
        assert_eq!(contents(&table), vec![(1, "b".to_string())]);
        assert_live_rows_match_the_index(&table);
    }

    #[test]
    fn composite_primary_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chain", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        let table = InMemoryTable::try_new(
            TABLE,
            &schema,
            &["chain".to_string(), "address".to_string()],
            METRIC_KEY.to_string(),
            MAX_DEAD_RATIO,
            None,
            VersionRetention::Latest,
        )
        .unwrap();
        let input = |rows: &[(&str, &str, &str)]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
                ],
            )
            .unwrap()
        };

        table
            .apply(&input(&[("eth", "0x1", "i"), ("sol", "0x1", "i")]), &[])
            .unwrap();
        assert_eq!(table.state.read().index.len(), 2);
        table
            .apply(&input(&[("eth", "0x1", "u"), ("eth", "0x2", "i")]), &[])
            .unwrap();
        assert_eq!(table.state.read().index.len(), 3);
        assert_live_rows_match_the_index(&table);
    }

    #[test]
    fn field_metadata_survives_into_storage() {
        let amount = DecimalArbType::field("amount", 78, 0, true).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            amount.clone(),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        let table = InMemoryTable::try_new(
            TABLE,
            &schema,
            &["id".to_string()],
            METRIC_KEY.to_string(),
            MAX_DEAD_RATIO,
            None,
            VersionRetention::Latest,
        )
        .unwrap();
        let input = |op: &str| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(LargeBinaryArray::from_iter_values([b"\x01".as_slice()])),
                    Arc::new(StringArray::from(vec![op])),
                ],
            )
            .unwrap()
        };
        table.apply(&input("i"), &[]).unwrap();
        assert_eq!(
            table.schema.field_with_name("amount").unwrap().metadata(),
            amount.metadata()
        );
        assert_eq!(
            table.lookup_field("amount").unwrap().metadata(),
            amount.metadata()
        );
    }

    #[test]
    fn cleaning_keeps_contents_and_remaps_the_index() {
        let table = table();
        table
            .apply(&batch(&[(1, "a", "i"), (2, "b", "i"), (3, "c", "i")]), &[])
            .unwrap();
        // Each batch leaves 2 dead rows, more than a quarter of the 3 live ones,
        // so every chunk holding a dead row is cleaned.
        table
            .apply(&batch(&[(1, "a2", "u"), (2, "b2", "u")]), &[])
            .unwrap();
        table
            .apply(&batch(&[(1, "a3", "u"), (3, "c3", "u")]), &[])
            .unwrap();

        {
            let state = table.state.read();
            assert_eq!(state.first_chunk_id, 3);
            assert_eq!(state.total_rows, 3);
            assert_eq!(
                state.chunk_bytes,
                state
                    .chunks
                    .iter()
                    .map(|chunk| chunk.batch.get_array_memory_size())
                    .sum::<usize>()
            );
            assert_eq!(
                state.key_bytes,
                state.index.keys().map(|key| key.len()).sum::<usize>()
            );
        }
        assert_eq!(
            contents(&table),
            vec![
                (1, "a3".to_string()),
                (2, "b2".to_string()),
                (3, "c3".to_string())
            ]
        );
        // Key 2 was moved by the cleaner; the delete releases it at its new place.
        table.apply(&batch(&[(2, "x", "d")]), &[]).unwrap();
        assert_eq!(
            contents(&table),
            vec![(1, "a3".to_string()), (3, "c3".to_string())]
        );
        assert_live_rows_match_the_index(&table);
    }

    #[test]
    fn null_primary_key_is_rejected() {
        let input = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["i"])),
            ],
        )
        .unwrap();
        let err = table().apply(&input, &[]).unwrap_err().to_string();
        assert!(
            err.contains("primary key column 'id' contains a null value"),
            "{err}"
        );
    }

    #[test]
    fn unrecognized_or_null_op_is_rejected() {
        for op in [Some("x"), None] {
            let input = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec!["a"])),
                    Arc::new(StringArray::from(vec![op])),
                ],
            )
            .unwrap();
            let err = table().apply(&input, &[]).unwrap_err().to_string();
            assert!(err.contains("unrecognized '_gs_op' value"), "{err}");
        }

        let large_utf8 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new(COLUMN_NAME_OP, DataType::LargeUtf8, true),
        ]));
        let input = RecordBatch::try_new(
            large_utf8,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(arrow::array::LargeStringArray::from(vec!["i"])),
            ],
        )
        .unwrap();
        let err = table().apply(&input, &[]).unwrap_err().to_string();
        assert!(err.contains("column '_gs_op' must be Utf8"), "{err}");
    }

    #[test]
    fn batch_with_a_different_schema_is_rejected() {
        let other = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Int64, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
        ]));
        let input = RecordBatch::try_new(
            other,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["i"])),
            ],
        )
        .unwrap();
        let err = table().apply(&input, &[]).unwrap_err().to_string();
        assert!(err.contains("does not match the table schema"), "{err}");
    }

    fn lookup_names(table: &InMemoryTable, ids: &[Option<i64>]) -> Vec<Option<String>> {
        let keys: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        table
            .lookup(&keys, "name", None)
            .unwrap()
            .as_string::<i32>()
            .iter()
            .map(|name| name.map(str::to_string))
            .collect()
    }

    #[test]
    fn lookup_reads_the_live_row_per_key() {
        let table = table();
        assert_eq!(lookup_names(&table, &[Some(1)]), [None], "empty table");
        table
            .apply(&batch(&[(1, "a", "i"), (2, "b", "i"), (3, "c", "i")]), &[])
            .unwrap();
        table
            .apply(&batch(&[(1, "a2", "u"), (2, "x", "d")]), &[])
            .unwrap();
        assert_eq!(
            lookup_names(&table, &[Some(1), Some(2), Some(3), Some(9), None, Some(1)]),
            [
                Some("a2".to_string()),
                None,
                Some("c".to_string()),
                None,
                None,
                Some("a2".to_string())
            ]
        );
    }

    #[test]
    fn lookup_follows_cleaning() {
        let table = table();
        table
            .apply(&batch(&[(1, "a", "i"), (2, "b", "i")]), &[])
            .unwrap();
        table.apply(&batch(&[(1, "a2", "u")]), &[]).unwrap();
        table
            .apply(&batch(&[(1, "a3", "u"), (2, "b2", "u")]), &[])
            .unwrap();
        assert_eq!(table.state.read().first_chunk_id, 3);
        assert_eq!(
            lookup_names(&table, &[Some(2), Some(1)]),
            [Some("b2".to_string()), Some("a3".to_string())]
        );
    }

    #[test]
    fn cleaning_keeps_dead_rows_within_the_ratio() {
        const KEYS: i64 = 4096;
        const BATCH: i64 = 512;
        const UPDATE_BATCHES: i64 = 64;
        // Coprime with KEYS, so updates sweep the keys in a scattered order.
        const STRIDE: i64 = 2053;
        let table = table();
        for start in (0..KEYS).step_by(BATCH as usize) {
            let inserts: Vec<(i64, &str, &str)> =
                (start..start + BATCH).map(|id| (id, "a", "i")).collect();
            table.apply(&batch(&inserts), &[]).unwrap();
        }
        for round in 0..UPDATE_BATCHES {
            let updates: Vec<(i64, &str, &str)> = (0..BATCH)
                .map(|i| (((round * BATCH + i) * STRIDE) % KEYS, "b", "u"))
                .collect();
            table.apply(&batch(&updates), &[]).unwrap();
            let state = table.state.read();
            let live = state.index.len();
            let dead = state.total_rows - live;
            assert!(
                dead as f64 <= MAX_DEAD_RATIO * live as f64 + BATCH as f64,
                "round {round}: {dead} dead rows for {live} live ones"
            );
        }
    }

    #[test]
    fn hot_key_updates_drop_dead_chunks() {
        const UPDATES: usize = 1000;
        let table = table();
        for _ in 0..UPDATES {
            table.apply(&batch(&[(1, "a", "u")]), &[]).unwrap();
            let state = table.state.read();
            assert_eq!((state.chunks.len(), state.total_rows), (1, 1));
        }
    }

    #[test]
    fn cleaning_resumes_mid_chunk() {
        const KEYS: i64 = 3000;
        const UPDATE_BATCH: i64 = 100;
        // 800 dead rows exceed a quarter of the 3000 live ones only after the
        // last batch, whose cleaning budget is MIN_CLEANING_ROWS.
        const UPDATE_BATCHES: i64 = 8;
        let table = table();
        let names: Vec<String> = (0..KEYS).map(|id| format!("v{id}")).collect();
        let inserts: Vec<(i64, &str, &str)> = (0..KEYS)
            .map(|id| (id, names[id as usize].as_str(), "i"))
            .collect();
        table.apply(&batch(&inserts), &[]).unwrap();
        for start in (0..UPDATE_BATCH * UPDATE_BATCHES).step_by(UPDATE_BATCH as usize) {
            let updates: Vec<(i64, &str, &str)> = (start..start + UPDATE_BATCH)
                .map(|id| (id, "u", "u"))
                .collect();
            table.apply(&batch(&updates), &[]).unwrap();
        }

        {
            let state = table.state.read();
            assert_eq!(
                state.first_chunk_id, 0,
                "the insert chunk is partly cleaned"
            );
            assert_eq!(state.cleaning_cursor, MIN_CLEANING_ROWS);
        }
        // 900 was moved, 2900 is past the cursor, 5 was updated.
        assert_eq!(
            lookup_names(&table, &[Some(900), Some(2900), Some(5)]),
            [
                Some("v900".to_string()),
                Some("v2900".to_string()),
                Some("u".to_string())
            ]
        );
        table
            .apply(&batch(&[(900, "x", "d"), (2900, "x", "d")]), &[])
            .unwrap();
        assert_eq!(lookup_names(&table, &[Some(900), Some(2900)]), [None, None]);
        assert_live_rows_match_the_index(&table);
    }

    #[test]
    fn rejects_a_max_dead_ratio_that_is_not_a_positive_number() {
        for ratio in [0.0, -0.5, f64::NAN, f64::INFINITY] {
            let err = InMemoryTable::try_new(
                TABLE,
                &schema(),
                &["id".to_string()],
                METRIC_KEY.to_string(),
                ratio,
                None,
                VersionRetention::Latest,
            )
            .err()
            .expect("the ratio must be rejected")
            .to_string();
            assert!(
                err.contains("max_dead_ratio must be a positive number"),
                "{ratio}: {err}"
            );
        }
    }

    #[test]
    fn lookup_casts_keys_to_the_primary_key_type() {
        let table = table();
        table.apply(&batch(&[(7, "seven", "i")]), &[]).unwrap();
        let narrower: ArrayRef = Arc::new(Int32Array::from(vec![7]));
        let text: ArrayRef = Arc::new(StringViewArray::from(vec!["7"]));
        for keys in [narrower, text] {
            let names = table.lookup(&keys, "name", None).unwrap();
            assert_eq!(
                names.as_string::<i32>().value(0),
                "seven",
                "{}",
                keys.data_type()
            );
        }
    }

    #[test]
    fn lookup_rejects_non_key_fields_unknown_columns_and_composite_keys() {
        let table = table();
        assert_eq!(
            table.lookup_key_field("id").unwrap().data_type(),
            &DataType::Int64
        );
        let err = table.lookup_key_field("name").unwrap_err().to_string();
        assert!(
            err.contains("lookups must use the primary key 'id', got 'name'"),
            "{err}"
        );
        for column in ["missing", COLUMN_NAME_OP] {
            let err = table.lookup_field(column).unwrap_err().to_string();
            assert!(
                err.contains(&format!("table 'tokens' has no column '{column}'")),
                "{err}"
            );
        }
        assert!(table.lookup_field("name").unwrap().is_nullable());

        let composite = InMemoryTable::try_new(
            TABLE,
            &schema(),
            &["id".to_string(), "name".to_string()],
            METRIC_KEY.to_string(),
            MAX_DEAD_RATIO,
            None,
            VersionRetention::Latest,
        )
        .unwrap();
        let err = composite.lookup_key_field("id").unwrap_err().to_string();
        assert!(
            err.contains("lookups need a single-column primary key"),
            "{err}"
        );
    }

    #[test]
    fn sql_output_without_op_column_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let err = InMemoryTable::try_new(
            TABLE,
            &schema,
            &["id".to_string()],
            METRIC_KEY.to_string(),
            MAX_DEAD_RATIO,
            None,
            VersionRetention::Latest,
        )
        .err()
        .expect("a schema without _gs_op must be rejected")
        .to_string();
        assert!(err.contains("must contain the '_gs_op' column"), "{err}");
    }

    #[test]
    fn primary_key_naming_the_op_column_is_rejected() {
        let err = InMemoryTable::try_new(
            TABLE,
            &schema(),
            &[COLUMN_NAME_OP.to_string()],
            METRIC_KEY.to_string(),
            MAX_DEAD_RATIO,
            None,
            VersionRetention::Latest,
        )
        .err()
        .expect("a primary key on _gs_op must be rejected")
        .to_string();
        assert!(err.contains("primary key cannot be '_gs_op'"), "{err}");
    }

    const HOUR_NANOS: i64 = 3_600_000_000_000;
    const KEEP_LAST_HOUR: &str = "ts >= now() - interval '1 hour'";
    /// Long enough that applies don't run GC passes unless a test makes one
    /// due.
    const GC_INTERVAL: Duration = Duration::from_secs(60);

    /// `schema()` plus `ts`, the rows' event time.
    fn timed_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(WALL_CLOCK_TIME_ZONE.into())),
                true,
            ),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
        ]))
    }

    /// Upserts of `(id, name, ts)`.
    fn timed_batch(rows: &[(i64, &str, Option<i64>)]) -> RecordBatch {
        timed_batch_of(rows, "i")
    }

    /// `op`s of `(id, name, ts)`.
    fn timed_batch_of(rows: &[(i64, &str, Option<i64>)], op: &str) -> RecordBatch {
        RecordBatch::try_new(
            timed_schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(
                    TimestampNanosecondArray::from_iter(rows.iter().map(|row| row.2))
                        .with_timezone(WALL_CLOCK_TIME_ZONE),
                ),
                Arc::new(StringArray::from(vec![op; rows.len()])),
            ],
        )
        .unwrap()
    }

    fn windowed_table(input_schema: &SchemaRef, predicate: &str) -> Result<InMemoryTable> {
        windowed_table_with(input_schema, predicate, VersionRetention::Latest)
    }

    fn windowed_table_with(
        input_schema: &SchemaRef,
        predicate: &str,
        retention: VersionRetention,
    ) -> Result<InMemoryTable> {
        InMemoryTable::try_new(
            TABLE,
            input_schema,
            &["id".to_string()],
            METRIC_KEY.to_string(),
            MAX_DEAD_RATIO,
            Some(WindowConfig {
                predicate,
                session_state: &SessionContext::new().state(),
                gc_interval: GC_INTERVAL,
            }),
            retention,
        )
    }

    fn evict_outside(table: &InMemoryTable) {
        table
            .evict_outside(table.window.as_ref().expect("a windowed table"))
            .unwrap();
    }

    fn now_nanos() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }

    #[test]
    fn window_evicts_the_live_rows_it_is_not_true_for() {
        let table = windowed_table(&timed_schema(), KEEP_LAST_HOUR).unwrap();
        assert!(
            table.schema.field_with_name(WALL_CLOCK_COLUMN).is_err(),
            "a window that doesn't read the wall clock doesn't store it"
        );
        let now = now_nanos();
        let old = now - 2 * HOUR_NANOS;
        table
            .apply(
                &timed_batch(&[
                    (1, "old", Some(old)),
                    (2, "new", Some(now)),
                    (3, "null", None),
                    (4, "old", Some(old)),
                    (5, "new", Some(now)),
                ]),
                &[],
            )
            .unwrap();
        // Key 4's live row is now inside the window, key 5's outside it.
        table
            .apply(
                &timed_batch(&[(4, "new", Some(now)), (5, "old", Some(old))]),
                &[],
            )
            .unwrap();

        evict_outside(&table);

        assert_eq!(
            lookup_names(&table, &[Some(1), Some(2), Some(3), Some(4), Some(5)]),
            [
                None,
                Some("new".to_string()),
                None,
                Some("new".to_string()),
                None
            ]
        );
        assert_live_rows_match_the_index(&table);
        table.apply(&timed_batch(&[]), &[]).unwrap();
        assert_eq!(
            table.state.read().total_rows,
            2,
            "the cleaner reclaims them"
        );
    }

    #[test]
    fn wall_clock_window_reads_the_apply_time() {
        let keep_last_hour = format!("{WALL_CLOCK_COLUMN} >= now() - interval '1 hour'");
        let keep_older = format!("{WALL_CLOCK_COLUMN} < now() - interval '1 hour'");
        for (predicate, expected) in [(keep_last_hour, Some("a".to_string())), (keep_older, None)] {
            let table = windowed_table(&schema(), &predicate).unwrap();
            table.apply(&batch(&[(1, "a", "i")]), &[]).unwrap();
            evict_outside(&table);
            assert_eq!(lookup_names(&table, &[Some(1)]), [expected], "{predicate}");
        }
    }

    #[test]
    fn apply_runs_a_gc_pass_once_it_is_due() {
        let table = windowed_table(&timed_schema(), KEEP_LAST_HOUR).unwrap();
        let old = now_nanos() - 2 * HOUR_NANOS;
        table
            .apply(&timed_batch(&[(1, "old", Some(old))]), &[])
            .unwrap();
        table.apply(&timed_batch(&[]), &[]).unwrap();
        assert_eq!(lookup_names(&table, &[Some(1)]), [Some("old".to_string())]);

        let window = table.window.as_ref().unwrap();
        *window.last_pass.lock() = Instant::now().checked_sub(GC_INTERVAL).unwrap();
        table.apply(&timed_batch(&[]), &[]).unwrap();
        assert_eq!(lookup_names(&table, &[Some(1)]), [None]);
    }

    fn chunks_to_evaluate(table: &InMemoryTable) -> Vec<usize> {
        let window = table.window.as_ref().expect("a windowed table");
        table
            .chunks_to_evaluate(&table.state.read(), window, &window.compile().unwrap())
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    #[test]
    fn gc_pass_evaluates_only_the_chunks_that_may_hold_rows_outside_the_window() {
        let table = windowed_table(&timed_schema(), KEEP_LAST_HOUR).unwrap();
        let now = now_nanos();
        let old = now - 2 * HOUR_NANOS;
        // Chunks 0 to 3: inside, straddling, outside, inside but with a null.
        table
            .apply(
                &timed_batch(&[(1, "a", Some(now)), (2, "b", Some(now))]),
                &[],
            )
            .unwrap();
        table
            .apply(
                &timed_batch(&[(3, "c", Some(old)), (4, "d", Some(now))]),
                &[],
            )
            .unwrap();
        table
            .apply(&timed_batch(&[(5, "e", Some(old))]), &[])
            .unwrap();
        table
            .apply(&timed_batch(&[(6, "f", Some(now)), (7, "g", None)]), &[])
            .unwrap();
        assert_eq!(chunks_to_evaluate(&table), [1, 2, 3]);

        evict_outside(&table);

        assert_eq!(
            lookup_names(&table, &(1..=7).map(Some).collect::<Vec<_>>()),
            [
                Some("a".to_string()),
                Some("b".to_string()),
                None,
                Some("d".to_string()),
                None,
                Some("f".to_string()),
                None
            ]
        );
        assert_eq!(
            chunks_to_evaluate(&table),
            [1, 3],
            "a chunk without live rows isn't evaluated, whatever its statistics"
        );
    }

    #[test]
    fn unprunable_window_evaluates_every_chunk() {
        let unprunable = "date_trunc('second', ts) >= now() - interval '1 hour'";
        for (predicate, expected) in [(KEEP_LAST_HOUR, vec![]), (unprunable, vec![0, 1])] {
            let table = windowed_table(&timed_schema(), predicate).unwrap();
            let now = now_nanos();
            table
                .apply(&timed_batch(&[(1, "a", Some(now))]), &[])
                .unwrap();
            table
                .apply(&timed_batch(&[(2, "b", Some(now))]), &[])
                .unwrap();
            assert_eq!(chunks_to_evaluate(&table), expected, "{predicate}");
        }
    }

    #[test]
    fn cleaned_chunks_get_statistics() {
        let table = windowed_table(&timed_schema(), KEEP_LAST_HOUR).unwrap();
        let now = now_nanos();
        table
            .apply(
                &timed_batch(&[
                    (1, "a", Some(now - 4)),
                    (2, "b", Some(now - 3)),
                    (3, "c", Some(now - 2)),
                    (4, "d", Some(now - 1)),
                ]),
                &[],
            )
            .unwrap();
        // Dead rows exceed the ratio, so chunk 0's live rows move to chunk 2.
        table
            .apply(
                &timed_batch(&[(1, "a", Some(now)), (2, "b", Some(now))]),
                &[],
            )
            .unwrap();

        let state = table.state.read();
        assert_eq!((state.first_chunk_id, state.chunks.len()), (1, 2));
        let window = table.window.as_ref().unwrap();
        for chunk in &state.chunks {
            assert_eq!(chunk.statistics.len(), 1);
            assert_eq!(chunk.statistics, window.statistics(&chunk.batch).unwrap());
        }
        assert_eq!(
            state.chunks[1].statistics[0].min,
            ScalarValue::TimestampNanosecond(Some(now - 2), Some(WALL_CLOCK_TIME_ZONE.into()))
        );
    }

    #[test]
    fn lookups_cannot_read_the_window_wall_clock() {
        let windowed = windowed_table(
            &schema(),
            &format!("{WALL_CLOCK_COLUMN} >= now() - interval '1 hour'"),
        )
        .unwrap();
        let err = windowed
            .lookup_field(WALL_CLOCK_COLUMN)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("table 'tokens' has no column '__timestamp'"),
            "{err}"
        );

        let with_wall_clock = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(WALL_CLOCK_COLUMN, DataType::Int64, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
        ]));
        let unwindowed = InMemoryTable::try_new(
            TABLE,
            &with_wall_clock,
            &["id".to_string()],
            METRIC_KEY.to_string(),
            MAX_DEAD_RATIO,
            None,
            VersionRetention::Latest,
        )
        .unwrap();
        assert!(
            unwindowed.lookup_field(WALL_CLOCK_COLUMN).is_ok(),
            "without a window, '__timestamp' is an ordinary column"
        );
    }

    const KEEP_LAST_HOUR_OF_DATA: &str = "ts >= watermark(ts) - interval '1 hour'";
    const MINUTE_NANOS: i64 = 60_000_000_000;

    /// The README's walkthrough: every apply that writes rows runs a GC pass,
    /// whatever the GC interval.
    #[test]
    fn watermark_window_keeps_the_hour_before_the_largest_stored_time() {
        let table = windowed_table(&timed_schema(), KEEP_LAST_HOUR_OF_DATA).unwrap();
        let at = |hour: i64, minute: i64| Some(hour * HOUR_NANOS + minute * MINUTE_NANOS);
        let ids = [Some(1), Some(2), Some(3), Some(4), Some(5)];

        table
            .apply(
                &timed_batch(&[(1, "a", at(10, 0)), (2, "b", at(10, 20))]),
                &[],
            )
            .unwrap();
        assert_eq!(
            lookup_names(&table, &ids),
            [
                Some("a".to_string()),
                Some("b".to_string()),
                None,
                None,
                None
            ]
        );

        table
            .apply(&timed_batch(&[(3, "c", at(11, 10))]), &[])
            .unwrap();
        assert_eq!(
            lookup_names(&table, &ids),
            [
                None,
                Some("b".to_string()),
                Some("c".to_string()),
                None,
                None
            ],
            "the watermark moved to 11:10, so 10:00 is outside"
        );

        table
            .apply(&timed_batch(&[(4, "late", at(9, 30))]), &[])
            .unwrap();
        table
            .apply(&timed_batch(&[(2, "b", at(10, 5))]), &[])
            .unwrap();
        assert_eq!(
            lookup_names(&table, &ids),
            [None, None, Some("c".to_string()), None, None],
            "a late row and an update to before 10:10 are evicted in their own apply"
        );

        table
            .apply(&timed_batch_of(&[(3, "c", at(11, 10))], "d"), &[])
            .unwrap();
        table
            .apply(&timed_batch(&[(5, "e", at(10, 0))]), &[])
            .unwrap();
        assert_eq!(
            lookup_names(&table, &ids),
            [None, None, None, None, None],
            "deleting the latest row doesn't move the watermark back"
        );
        assert_live_rows_match_the_index(&table);
    }

    #[test]
    fn watermark_reads_integer_columns() {
        const HOUR_SECS: i64 = 3600;
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("block_timestamp", DataType::Int64, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
        ]));
        let table = windowed_table(
            &input_schema,
            "block_timestamp >= watermark(block_timestamp) - 3600",
        )
        .unwrap();
        let input = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["old", "new"])),
                Arc::new(Int64Array::from(vec![HOUR_SECS, 3 * HOUR_SECS])),
                Arc::new(StringArray::from(vec!["i", "i"])),
            ],
        )
        .unwrap();
        table.apply(&input, &[]).unwrap();
        assert_eq!(
            lookup_names(&table, &[Some(1), Some(2)]),
            [None, Some("new".to_string())]
        );
    }

    #[test]
    fn least_with_now_guards_against_future_timestamps() {
        const DECADE_NANOS: i64 = 10 * 365 * 24 * HOUR_NANOS;
        let guarded = "ts >= least(watermark(ts), now()) - interval '1 hour'";
        for (predicate, expected) in [
            (KEEP_LAST_HOUR_OF_DATA, None),
            (guarded, Some("now".to_string())),
        ] {
            let table = windowed_table(&timed_schema(), predicate).unwrap();
            let now = now_nanos();
            table
                .apply(
                    &timed_batch(&[
                        (1, "now", Some(now)),
                        (2, "future", Some(now + DECADE_NANOS)),
                    ]),
                    &[],
                )
                .unwrap();
            assert_eq!(lookup_names(&table, &[Some(1)]), [expected], "{predicate}");
        }
    }

    #[test]
    fn watermark_window_prunes_chunks() {
        let table = windowed_table(&timed_schema(), KEEP_LAST_HOUR_OF_DATA).unwrap();
        assert!(table.window.as_ref().unwrap().prunable().unwrap());
        table
            .apply(&timed_batch(&[(1, "a", Some(0))]), &[])
            .unwrap();
        table
            .apply(&timed_batch(&[(2, "b", Some(MINUTE_NANOS))]), &[])
            .unwrap();
        assert_eq!(chunks_to_evaluate(&table), Vec::<usize>::new());
    }

    #[test]
    fn watermark_takes_one_column() {
        for predicate in [
            "ts >= watermark(date_trunc('hour', ts))",
            "ts >= watermark()",
            "ts >= watermark(ts, ts)",
        ] {
            let err = windowed_table(&timed_schema(), predicate)
                .err()
                .expect("the window must be rejected")
                .to_string();
            assert!(err.contains("invalid window"), "{predicate}: {err}");
        }
    }

    #[test]
    fn invalid_windows_are_rejected() {
        let with_wall_clock = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(WALL_CLOCK_COLUMN, DataType::Int64, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
        ]));
        for (input_schema, predicate, expected) in [
            (
                timed_schema(),
                "missing >= now()",
                "invalid window 'missing >= now()'",
            ),
            (
                timed_schema(),
                "_gs_op = 'i'",
                "invalid window '_gs_op = 'i''",
            ),
            (
                timed_schema(),
                "ts",
                "the window must be a boolean expression, got Timestamp",
            ),
            (
                with_wall_clock,
                "id > 0",
                "the SQL output cannot contain '__timestamp'",
            ),
        ] {
            let err = windowed_table(&input_schema, predicate)
                .err()
                .expect("the window must be rejected")
                .to_string();
            assert!(err.contains(expected), "{predicate}: {err}");
        }
    }

    const STREAM: &str = "app::src";

    fn versioned_table() -> InMemoryTable {
        InMemoryTable::try_new(
            TABLE,
            &schema(),
            &["id".to_string()],
            METRIC_KEY.to_string(),
            MAX_DEAD_RATIO,
            None,
            VersionRetention::PerBatch {
                stream: STREAM.to_string(),
            },
        )
        .unwrap()
    }

    /// Applies `batch`, which carried the markers of `epochs`, and returns
    /// the mark its readers see the table at.
    fn apply_marked(table: &InMemoryTable, batch: &RecordBatch, epochs: &[u64]) -> Option<Mark> {
        let epochs: Vec<CheckpointEpoch> = epochs.iter().copied().map(CheckpointEpoch).collect();
        table_mark(table.apply(batch, &epochs).unwrap())
    }

    fn table_mark(marks: Marks) -> Option<Mark> {
        marks
            .into_iter()
            .find(|(table, _)| table == TABLE)
            .map(|(_, mark)| mark)
    }

    fn lookup_names_at(
        table: &InMemoryTable,
        ids: &[i64],
        mark: Option<Mark>,
    ) -> Result<Vec<Option<String>>> {
        let keys: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        Ok(table
            .lookup(&keys, "name", mark)?
            .as_string::<i32>()
            .iter()
            .map(|name| name.map(str::to_string))
            .collect())
    }

    fn names(values: &[Option<&str>]) -> Vec<Option<String>> {
        values.iter().map(|name| name.map(str::to_string)).collect()
    }

    #[test]
    fn reads_at_a_mark_do_not_see_later_applies() {
        let table = versioned_table();
        let first = apply_marked(
            &table,
            &batch(&[(1, "a", "i"), (2, "b", "i"), (3, "c", "i")]),
            &[],
        );
        let second = apply_marked(&table, &batch(&[(1, "a2", "u"), (2, "b2", "u")]), &[]);
        let third = apply_marked(&table, &batch(&[(1, "x", "d"), (3, "c3", "u")]), &[]);
        assert!(
            table.state.read().first_chunk_id > 0,
            "the cleaner dropped chunks the history still points into"
        );

        for (mark, expected) in [
            (first, [Some("a"), Some("b"), Some("c")]),
            (second, [Some("a2"), Some("b2"), Some("c")]),
            (third, [None, Some("b2"), Some("c3")]),
            (None, [None, Some("b2"), Some("c3")]),
        ] {
            assert_eq!(
                lookup_names_at(&table, &[1, 2, 3], mark).unwrap(),
                names(&expected),
                "{mark:?}"
            );
        }
    }

    #[test]
    fn latest_only_tables_keep_no_versions() {
        let table = table();
        let marks = table
            .apply(&batch(&[(1, "a", "i")]), &[CheckpointEpoch(1)])
            .unwrap();
        assert!(marks.is_empty());
        table.apply(&batch(&[(1, "b", "u")]), &[]).unwrap();
        assert_eq!(
            lookup_names_at(&table, &[1], None).unwrap(),
            names(&[Some("b")])
        );
        assert_eq!(table.state.read().versions.count(), 0);
    }

    fn collected(table: &InMemoryTable, mark: Option<Mark>) -> bool {
        match lookup_names_at(table, &[1], mark) {
            Ok(_) => false,
            Err(err) => {
                assert!(err.is_internal(), "a bug, not bad input: {err}");
                assert!(err.to_string().contains("already collected"), "{err}");
                true
            }
        }
    }

    /// Six batches, each changing key 1, from two partitions taking turns.
    /// Epoch 1's marker rides batches 1 and 2, epoch 2's batches 4 and 5.
    /// Returns the table and the batches' marks.
    fn table_with_markers() -> (InMemoryTable, Vec<Option<Mark>>) {
        let table = versioned_table();
        let marks = [&[][..], &[1], &[1], &[], &[2], &[2]]
            .iter()
            .map(|epochs| apply_marked(&table, &batch(&[(1, "v", "u")]), epochs))
            .collect();
        (table, marks)
    }

    /// Epoch 1's cut is batch 1's mark, the earlier of its two marker
    /// batches, so batch 2's change stays for a reader at batch 1.
    #[test]
    fn a_finalized_epoch_collects_up_to_its_earliest_marker_batch() {
        let (table, marks) = table_with_markers();
        table.state.write().collect(1);
        assert_eq!(
            marks
                .iter()
                .map(|mark| collected(&table, *mark))
                .collect::<Vec<_>>(),
            [true, false, false, false, false, false],
            "a read at the cut is exact"
        );
        assert_eq!(table.state.read().versions.count(), 4);

        // A repeated finalizer changes nothing.
        table.state.write().collect(1);
        assert_eq!(table.state.read().versions.count(), 4);
    }

    #[test]
    fn a_lookup_of_no_keys_at_a_collected_mark_errors() {
        let (table, marks) = table_with_markers();
        table.state.write().collect(1);
        assert!(collected(&table, marks[0]));
        let err = lookup_names_at(&table, &[], marks[0]).unwrap_err();
        assert!(err.to_string().contains("already collected"), "{err}");
    }

    #[test]
    fn a_later_finalized_epoch_covers_a_skipped_one() {
        let (table, marks) = table_with_markers();
        // Epoch 1's finalizer never came. Epoch 2's cut is batch 4's mark.
        table.state.write().collect(2);
        assert_eq!(
            marks
                .iter()
                .map(|mark| collected(&table, *mark))
                .collect::<Vec<_>>(),
            [true, true, true, true, false, false]
        );
        assert_eq!(table.state.read().versions.count(), 1);
    }

    #[test]
    fn history_holds_dropped_chunks_until_collected() {
        let table = versioned_table();
        let first = apply_marked(
            &table,
            &batch(&[(1, "a", "i"), (2, "b", "i"), (3, "c", "i")]),
            &[],
        );
        let second = apply_marked(&table, &batch(&[(1, "a2", "u"), (2, "b2", "u")]), &[1]);
        assert!(
            table.state.read().retired_bytes > 0,
            "the cleaner dropped the first chunk, which a read at the first batch needs"
        );
        assert_eq!(
            lookup_names_at(&table, &[1, 2, 3], first).unwrap(),
            names(&[Some("a"), Some("b"), Some("c")])
        );

        table.state.write().collect(1);

        assert_eq!(table.state.read().retired_bytes, 0);
        assert_eq!(table.state.read().versions.count(), 0);
        assert_eq!(
            lookup_names_at(&table, &[1, 2, 3], second).unwrap(),
            names(&[Some("a2"), Some("b2"), Some("c")])
        );
    }

    #[test]
    fn a_retired_chunk_stays_until_its_last_change_is_collected() {
        const KEYS: i64 = 10;
        let table = versioned_table();
        let rows: Vec<(i64, &str, &str)> = (1..=KEYS).map(|id| (id, "old", "i")).collect();
        apply_marked(&table, &batch(&rows), &[]);
        // Within the dead-row ratio, so the first chunk stays live.
        let second = apply_marked(&table, &batch(&[(1, "new", "u")]), &[1]);
        let rows: Vec<(i64, &str, &str)> = (2..=KEYS).map(|id| (id, "new", "u")).collect();
        apply_marked(&table, &batch(&rows), &[2]);
        assert!(
            table.state.read().first_chunk_id > 0,
            "the cleaner dropped the first chunk, held by the second and third batches' changes"
        );

        table.state.write().collect(1);
        assert!(table.state.read().retired_bytes > 0);
        assert_eq!(
            lookup_names_at(&table, &[1, 2], second).unwrap(),
            names(&[Some("new"), Some("old")])
        );

        table.state.write().collect(2);
        assert_eq!(table.state.read().retired_bytes, 0);
    }

    #[test]
    fn index_entries_take_24_bytes() {
        const INDEX_ENTRY_BYTES: usize = 24;
        const CHANGE_LINK_BYTES: usize = 8;
        assert_eq!(size_of::<Option<ChangeRef>>(), CHANGE_LINK_BYTES);
        assert_eq!(size_of::<IndexEntry>(), INDEX_ENTRY_BYTES);
    }

    #[test]
    fn interval_window_evictions_are_versioned() {
        let table = windowed_table_with(
            &timed_schema(),
            KEEP_LAST_HOUR,
            VersionRetention::PerBatch {
                stream: STREAM.to_string(),
            },
        )
        .unwrap();
        let old = now_nanos() - 2 * HOUR_NANOS;
        let first = apply_marked(&table, &timed_batch(&[(1, "old", Some(old))]), &[]);
        let window = table.window.as_ref().unwrap();
        *window.last_pass.lock() = Instant::now().checked_sub(GC_INTERVAL).unwrap();
        let second = apply_marked(&table, &timed_batch(&[]), &[]);

        assert_eq!(
            lookup_names_at(&table, &[1], first).unwrap(),
            names(&[Some("old")])
        );
        assert_eq!(
            lookup_names_at(&table, &[1], second).unwrap(),
            names(&[None])
        );
    }

    /// Partition 1 applies between partition 0's write and the interval GC
    /// pass at the end of the same apply: a reader of partition 1's batch
    /// still sees the row the pass evicts, a reader of partition 0's doesn't.
    #[test]
    fn an_interval_gc_pass_is_inside_its_applys_moment() {
        let table = windowed_table_with(
            &timed_schema(),
            KEEP_LAST_HOUR,
            VersionRetention::PerBatch {
                stream: STREAM.to_string(),
            },
        )
        .unwrap();
        let window = table.window.as_ref().unwrap();
        let old = now_nanos() - 2 * HOUR_NANOS;
        apply_marked(&table, &timed_batch(&[(1, "old", Some(old))]), &[]);

        table.begin();
        table.write(&timed_batch(&[])).unwrap();
        let other_partition =
            apply_marked(&table, &timed_batch(&[(2, "new", Some(now_nanos()))]), &[]);
        *window.last_pass.lock() = Instant::now().checked_sub(GC_INTERVAL).unwrap();
        table.collect_garbage_if_due().unwrap();
        let own = table_mark(table.finish(&[]));

        assert_eq!(
            lookup_names_at(&table, &[1, 2], other_partition).unwrap(),
            names(&[Some("old"), Some("new")])
        );
        assert_eq!(
            lookup_names_at(&table, &[1, 2], own).unwrap(),
            names(&[None, Some("new")])
        );
    }

    /// `#[serial]`: the coordinator channel is process-wide.
    #[test]
    #[serial_test::serial]
    fn a_finalizer_from_the_coordinator_collects_on_the_next_apply() {
        // Far above the epochs other tests record.
        const EPOCH: u64 = 1_000_000;
        let table = versioned_table();
        let first = apply_marked(&table, &batch(&[(1, "a", "i")]), &[]);
        let second = apply_marked(&table, &batch(&[(1, "b", "u")]), &[EPOCH]);
        crate::checkpoints::channels::send(
            crate::checkpoints::checkpoint_management::CHECKPOINT_COORDINATOR_CHANNEL,
            crate::checkpoints::checkpoint_management::CheckpointMessage::Finalizer(
                CheckpointEpoch(EPOCH),
            ),
        )
        .unwrap();
        assert!(!collected(&table, first));

        apply_marked(&table, &batch(&[]), &[]);

        assert!(collected(&table, first));
        assert!(!collected(&table, second));
    }
}
