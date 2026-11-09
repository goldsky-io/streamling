//! The `in_memory` table backend.
//!
//! Each namespace's storage is append-only, because Arrow arrays can't be
//! edited in place:
//!
//! - A chunk is the upserts of one write.
//! - The index maps each key to its live row, the key's latest version.
//!   Every other stored row (an older version, a deleted key's row) is dead.
//! - Each chunk counts its live rows, so a fully dead chunk is dropped
//!   without being read. Chunk ids keep growing, so dropping chunks from the
//!   front leaves the other ids valid.
//!
//! Every commit then cleans a little. While dead rows exceed `max_dead_ratio`
//! of the live ones, the live rows of the oldest chunks, which are mostly
//! dead, are copied into one new chunk, the index is pointed at the copies
//! and the old chunks are dropped. A step visits a budget of rows
//! proportional to the rows just written (`clean` explains the factor), so
//! the work is spread over writes, and a step that runs out mid-chunk resumes
//! there next time. Storage stays at about `1 + max_dead_ratio` times the
//! live rows.
//!
//! For example, with a `max_dead_ratio` of 0.25:
//!
//! | Step                 | Chunks                     | Index          | Stored / live / dead |
//! |----------------------|----------------------------|----------------|----------------------|
//! | write `A1 B2 C3`     | `c0:[A1 B2 C3]`            | A→c0 B→c0 C→c0 | 3 / 3 / 0            |
//! | write `A10`, del `B` | `c0:[A1 B2 C3]` `c1:[A10]` | A→c1 C→c0      | 4 / 2 / 2            |
//! | clean                | `c2:[C3 A10]`              | A→c2 C→c2      | 2 / 2 / 0            |
//!
//! A versioned namespace journals each key a transaction changes with the
//! key's live row before it. The dead rows the journal points to stay
//! readable, even after the cleaner drops their chunk, until `finalize`
//! trims the entries. A range column's min, max and null count are kept per
//! chunk, so `evict_below` skips the chunks wholly above its bound.
//!
//! A transaction holds the store's write lock from `begin` until it commits
//! or drops, so a lookup never sees a partial apply. It records what it
//! changes, so dropping it rolls the changes back. Nothing is persisted, so
//! the store starts empty after a restart.

mod journal;

use super::backend::{
    LookupRows, Mark, NamespaceSpec, NamespaceStats, ScanCursor, StoredState, TableBackend,
    TableTransaction, Version,
};
use crate::error::{Result, ResultExt};
use crate::{streamling_bail, streamling_err, streamling_user_bail, streamling_user_err};
use arrow::array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, Scalar, new_null_array};
use arrow::compute::kernels::boolean::or_kleene;
use arrow::compute::kernels::cmp::{lt, lt_eq};
use arrow::compute::{filter, interleave, interleave_record_batch, is_null};
use arrow::row::{RowConverter, Rows, SortField};
use async_trait::async_trait;
use datafusion::common::ScalarValue;
use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion::logical_expr::Accumulator;
use journal::{ChangeRef, Journal};
use parking_lot::RwLock;
use parking_lot::lock_api::ArcRwLockWriteGuard;
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

/// Floor of a cleaning step's budget, so tiny writes still merge old chunks
/// into chunks of a useful size.
const MIN_CLEANING_ROWS: usize = 1024;
/// Bytes of a scan cursor: the chunk id, then the row, as little-endian u64s.
const CURSOR_BYTES: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RowLocation {
    chunk: usize,
    row: usize,
}

/// A live key's row and its newest journal entry.
#[derive(Clone, Copy, Debug)]
struct IndexEntry {
    location: RowLocation,
    newest_change: Option<ChangeRef>,
}

struct Chunk {
    batch: RecordBatch,
    /// Rows of `batch` the index points to.
    live_rows: usize,
    /// Journal entries that point into `batch`, which keep it readable after
    /// the cleaner drops the chunk.
    held: usize,
    /// Bounds of the range column over all of `batch`'s rows.
    statistics: Option<ColumnStatistics>,
}

#[derive(Debug, PartialEq)]
struct ColumnStatistics {
    min: ScalarValue,
    max: ScalarValue,
    null_count: usize,
}

impl ColumnStatistics {
    fn of(values: &ArrayRef) -> Result<Self> {
        let values = [Arc::clone(values)];
        let mut min = MinAccumulator::try_new(values[0].data_type())?;
        min.update_batch(&values)?;
        let mut max = MaxAccumulator::try_new(values[0].data_type())?;
        max.update_batch(&values)?;
        Ok(Self {
            min: min.evaluate()?,
            max: max.evaluate()?,
            null_count: values[0].null_count(),
        })
    }

    /// Whether a row may be null, or below `bound` (or equal when
    /// `inclusive`). Unknown when the bound doesn't compare.
    fn may_hold_below(&self, bound: &ScalarValue, inclusive: bool) -> bool {
        self.null_count > 0
            || match self.min.partial_cmp(bound) {
                Some(Ordering::Less) => true,
                Some(Ordering::Equal) => inclusive,
                Some(Ordering::Greater) => false,
                None => true,
            }
    }
}

/// The rows of one namespace.
struct Namespace {
    spec: NamespaceSpec,
    key_indices: Vec<usize>,
    /// Encodes keys, so equal keys yield equal bytes.
    converter: RowConverter,
    range_index: Option<usize>,
    /// Oldest first. A chunk's id is its position plus `first_chunk_id`, so
    /// ids stay valid while the cleaner drops chunks from the front.
    chunks: VecDeque<Chunk>,
    first_chunk_id: usize,
    /// The front chunk's next row to clean.
    cleaning_cursor: usize,
    /// Encoded key to its live row.
    index: HashMap<Box<[u8]>, IndexEntry>,
    total_rows: usize,
    chunk_bytes: usize,
    key_bytes: usize,
    /// `None` for an unversioned namespace.
    journal: Option<Journal>,
    /// Chunks the cleaner dropped that journal entries still point into, by
    /// id, with how many do.
    retired: HashMap<usize, (RecordBatch, usize)>,
    retired_bytes: usize,
}

impl Namespace {
    fn try_new(table: &str, spec: &NamespaceSpec) -> Result<Self> {
        let key_indices = spec
            .key
            .iter()
            .map(|column| {
                spec.schema.index_of(column).map_err(|_| {
                    streamling_err!(
                        "table '{}': key column '{}' is not in namespace '{}'",
                        table,
                        column,
                        spec.name
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let converter = RowConverter::new(
            key_indices
                .iter()
                .map(|&index| SortField::new(spec.schema.field(index).data_type().clone()))
                .collect(),
        )
        .map_err(|e| {
            streamling_user_err!("table '{}': unsupported primary key type: {}", table, e)
        })?;
        let range_index = spec
            .range_column
            .as_ref()
            .map(|column| spec.schema.index_of(column))
            .transpose()?;
        Ok(Self {
            spec: spec.clone(),
            key_indices,
            converter,
            range_index,
            chunks: VecDeque::new(),
            first_chunk_id: 0,
            cleaning_cursor: 0,
            index: HashMap::new(),
            total_rows: 0,
            chunk_bytes: 0,
            key_bytes: 0,
            journal: spec.versioned.then(Journal::default),
            retired: HashMap::new(),
            retired_bytes: 0,
        })
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

    /// How many journal entries point into a live or retired chunk.
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

    /// Frees a retired chunk once no journal entry points into it.
    fn unhold(&mut self, location: RowLocation) {
        let held = self.held_mut(location.chunk);
        *held -= 1;
        if *held == 0
            && let Some((batch, _)) = self.retired.remove(&location.chunk)
        {
            self.retired_bytes -= batch.get_array_memory_size();
        }
    }

    /// The row at `location` is no longer the live row of its key.
    fn release(&mut self, location: RowLocation) {
        self.chunk_mut(location.chunk).live_rows -= 1;
    }

    fn next_chunk_id(&self) -> usize {
        self.first_chunk_id + self.chunks.len()
    }

    fn push_chunk(&mut self, batch: RecordBatch, live_rows: usize) -> Result<()> {
        let statistics = self
            .range_index
            .map(|index| ColumnStatistics::of(batch.column(index)))
            .transpose()?;
        self.total_rows += batch.num_rows();
        self.chunk_bytes += batch.get_array_memory_size();
        self.chunks.push_back(Chunk {
            batch,
            live_rows,
            held: 0,
            statistics,
        });
        Ok(())
    }

    /// Drops the front chunk, retiring it while journal entries point into
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

    /// Drops a chunk a rolled-back transaction appended, which nothing
    /// holds.
    fn pop_back_chunk(&mut self) {
        if let Some(chunk) = self.chunks.pop_back() {
            self.total_rows -= chunk.batch.num_rows();
            self.chunk_bytes -= chunk.batch.get_array_memory_size();
        }
    }

    /// The rows at `locations`, in order.
    fn rows_at(&self, locations: &[RowLocation]) -> Result<RecordBatch> {
        if locations.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.spec.schema)));
        }
        let mut sources: Vec<&RecordBatch> = Vec::new();
        // Chunk id to its position in `sources`.
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

    /// The column at `column_index` of the rows at `locations`, null where
    /// there is none.
    fn gather(&self, column_index: usize, locations: &[Option<RowLocation>]) -> Result<ArrayRef> {
        let missing = new_null_array(self.spec.schema.field(column_index).data_type(), 1);
        // Source 0 is a single null for keys with no row. Each chunk a hit
        // reads from is added once, so the cost follows hits, not chunk count.
        let mut sources: Vec<&dyn Array> = vec![missing.as_ref()];
        let mut chunk_sources: HashMap<usize, usize> = HashMap::new();
        let indices: Vec<(usize, usize)> = locations
            .iter()
            .map(|location| match location {
                Some(location) => {
                    let source = *chunk_sources.entry(location.chunk).or_insert_with(|| {
                        sources.push(self.batch(location.chunk).column(column_index).as_ref());
                        sources.len() - 1
                    });
                    (source, location.row)
                }
                None => (0, 0),
            })
            .collect();
        interleave(&sources, &indices).streamling_context("failed to read the looked-up rows")
    }

    /// `keys`, the key columns, encoded like the index's keys.
    fn encode_keys(&self, table: &str, keys: &[ArrayRef]) -> Result<Rows> {
        if keys.len() != self.key_indices.len() {
            streamling_bail!(
                "table '{}': namespace '{}' has {} key columns, got {}",
                table,
                self.spec.name,
                self.key_indices.len(),
                keys.len()
            );
        }
        self.converter
            .convert_columns(keys)
            .streamling_context("failed to encode the keys")
    }

    fn stored_keys(&self, batch: &RecordBatch) -> Vec<ArrayRef> {
        self.key_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index)))
            .collect()
    }

    /// The location of each of `keys`' rows as of `mark`, or the latest
    /// commit without one.
    fn locate(&self, keys: &Rows, mark: Option<Mark>) -> Vec<Option<RowLocation>> {
        keys.iter()
            .map(|key| {
                let entry = self.index.get(key.as_ref());
                let current = entry.map(|entry| entry.location);
                match (&self.journal, mark) {
                    (Some(journal), Some(mark)) => {
                        let newest_change = match entry {
                            Some(entry) => entry.newest_change,
                            None => journal.removed_newest_change(key.as_ref()),
                        };
                        journal.resolve(mark, current, newest_change)
                    }
                    _ => current,
                }
            })
            .collect()
    }

    fn lookup(
        &self,
        table: &str,
        keys: &RecordBatch,
        projection: &[usize],
        mark: Option<Mark>,
    ) -> Result<LookupRows> {
        let encoded = self.encode_keys(table, keys.columns())?;
        let locations = self.locate(&encoded, mark);
        let columns = projection
            .iter()
            .map(|&index| self.gather(index, &locations))
            .collect::<Result<Vec<_>>>()?;
        // A missing key's row is all nulls.
        let fields: Vec<_> = projection
            .iter()
            .map(|&index| self.spec.schema.field(index).clone().with_nullable(true))
            .collect();
        let rows = RecordBatch::try_new_with_options(
            Arc::new(arrow::datatypes::Schema::new(fields)),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(locations.len())),
        )?;
        Ok(LookupRows {
            rows,
            found: locations
                .iter()
                .map(|location| Some(location.is_some()))
                .collect(),
        })
    }

    /// The chunks with a live row that may hold rows whose column at
    /// `column_index` is null or below `bound` (or equal when `inclusive`),
    /// by id. The range column's statistics rule the others out.
    fn chunks_below(
        &self,
        column_index: usize,
        bound: &ScalarValue,
        inclusive: bool,
    ) -> Vec<usize> {
        let prunes = self.range_index == Some(column_index);
        (self.first_chunk_id..)
            .zip(&self.chunks)
            .filter(|(_, chunk)| {
                chunk.live_rows > 0
                    && (!prunes
                        || chunk
                            .statistics
                            .as_ref()
                            .is_none_or(|statistics| statistics.may_hold_below(bound, inclusive)))
            })
            .map(|(id, _)| id)
            .collect()
    }

    fn too_many_dead_rows(&self, max_dead_ratio: f64, total_rows: usize) -> bool {
        let live_rows = self.index.len();
        total_rows.saturating_sub(live_rows) as f64 > max_dead_ratio * live_rows as f64
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
    /// pipeline, and an in_memory table starts empty after a restart.
    fn clean(&mut self, max_dead_ratio: f64, written_rows: usize) -> Result<()> {
        let rows_per_write = 1.0 + 1.0 / max_dead_ratio;
        let budget =
            ((written_rows as f64 * rows_per_write).ceil() as usize).max(MIN_CLEANING_ROWS);
        let target = self.next_chunk_id();
        // Moved rows as `(position in sources, row)`, in their order in `target`.
        let mut sources: Vec<RecordBatch> = Vec::new();
        let mut moved: Vec<(usize, usize)> = Vec::new();
        let mut visited = 0;
        // `moved` rows count toward storage: `target` will hold them.
        while visited < budget
            && self.too_many_dead_rows(max_dead_ratio, self.total_rows + moved.len())
        {
            let Some(front) = self.chunks.front() else {
                break;
            };
            if front.live_rows == 0 {
                self.pop_front_chunk();
                continue;
            }
            let front_id = self.first_chunk_id;
            let batch = front.batch.clone();
            let start = self.cleaning_cursor;
            let end = batch.num_rows().min(start.saturating_add(budget - visited));
            let key_columns: Vec<ArrayRef> = self
                .key_indices
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
                if let Some(entry) = self.index.get_mut(key.as_ref())
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
            self.chunk_mut(front_id).live_rows -= moved_from_front;
            // The rest of a chunk without live rows is dead.
            if finished || self.chunk(front_id).live_rows == 0 {
                self.pop_front_chunk();
            } else {
                self.cleaning_cursor = end;
            }
        }
        if !moved.is_empty() {
            let sources: Vec<&RecordBatch> = sources.iter().collect();
            let batch = interleave_record_batch(&sources, &moved)
                .streamling_context("failed to move the live rows")?;
            let live_rows = moved.len();
            self.push_chunk(batch, live_rows)?;
        }
        Ok(())
    }

    fn stats(&self) -> NamespaceStats {
        NamespaceStats {
            namespace: self.spec.name.clone(),
            live_rows: self.index.len() as u64,
            journal_rows: self.journal.as_ref().map_or(0, Journal::count) as u64,
            memory_bytes: Some((self.chunk_bytes + self.key_bytes) as u64),
            retired_bytes: Some(self.retired_bytes as u64),
        }
    }
}

struct Store {
    next_version: Version,
    floor: Mark,
    meta: HashMap<String, Vec<u8>>,
    namespaces: HashMap<String, Namespace>,
}

/// See the module docs.
pub struct InMemoryBackend {
    /// The table's name, for errors.
    table: String,
    /// Cleaning runs while dead rows exceed this fraction of the live rows.
    max_dead_ratio: f64,
    store: Arc<RwLock<Store>>,
}

impl InMemoryBackend {
    /// `max_dead_ratio` must be a positive number.
    pub fn try_new(table: &str, max_dead_ratio: f64) -> Result<Self> {
        if !(max_dead_ratio.is_finite() && max_dead_ratio > 0.0) {
            streamling_user_bail!(
                "table '{}': max_dead_ratio must be a positive number, got {}",
                table,
                max_dead_ratio
            );
        }
        Ok(Self {
            table: table.to_string(),
            max_dead_ratio,
            store: Arc::new(RwLock::new(Store {
                next_version: 0,
                floor: Mark::default(),
                meta: HashMap::new(),
                namespaces: HashMap::new(),
            })),
        })
    }

    fn unknown_namespace(&self, namespace: &str) -> crate::error::StreamlingError {
        streamling_err!("table '{}': no namespace '{}'", self.table, namespace)
    }
}

impl fmt::Debug for InMemoryBackend {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("InMemoryBackend")
            .field("table", &self.table)
            .finish()
    }
}

#[async_trait]
impl TableBackend for InMemoryBackend {
    async fn open(&self) -> Result<StoredState> {
        let store = self.store.read();
        Ok(StoredState {
            next_version: store.next_version,
            floor: store.floor,
            meta: store.meta.clone(),
        })
    }

    async fn create_namespace(&self, spec: &NamespaceSpec) -> Result<()> {
        let mut store = self.store.write();
        if let Some(existing) = store.namespaces.get(&spec.name) {
            if existing.spec.schema.fields() != spec.schema.fields()
                || existing.spec.key != spec.key
            {
                streamling_bail!(
                    "table '{}': namespace '{}' exists with another schema or key",
                    self.table,
                    spec.name
                );
            }
            return Ok(());
        }
        let namespace = Namespace::try_new(&self.table, spec)?;
        store.namespaces.insert(spec.name.clone(), namespace);
        Ok(())
    }

    async fn drop_namespace(&self, name: &str) -> Result<()> {
        self.store.write().namespaces.remove(name);
        Ok(())
    }

    async fn begin(&self) -> Result<Box<dyn TableTransaction>> {
        let store = self.store.write_arc();
        Ok(Box::new(InMemoryTransaction {
            table: self.table.clone(),
            max_dead_ratio: self.max_dead_ratio,
            version: store.next_version,
            store,
            undo: HashMap::new(),
            meta_undo: HashMap::new(),
            committed: false,
        }))
    }

    async fn lookup(
        &self,
        namespace: &str,
        keys: &RecordBatch,
        projection: &[usize],
        mark: Option<Mark>,
    ) -> Result<LookupRows> {
        let store = self.store.read();
        if let Some(mark) = mark
            && mark < store.floor
        {
            streamling_bail!(
                "table '{}': no table version at mark {}: it was already collected",
                self.table,
                mark.0
            );
        }
        store
            .namespaces
            .get(namespace)
            .ok_or_else(|| self.unknown_namespace(namespace))?
            .lookup(&self.table, keys, projection, mark)
    }

    /// Nothing survives a crash.
    async fn checkpoint(&self, _mark: Mark) -> Result<()> {
        Ok(())
    }

    async fn finalize(&self, horizon: Mark, trim: bool) -> Result<()> {
        if !trim {
            return Ok(());
        }
        let mut store = self.store.write();
        store.floor = store.floor.max(horizon);
        for namespace in store.namespaces.values_mut() {
            let Some(journal) = &mut namespace.journal else {
                continue;
            };
            for location in journal.trim(horizon) {
                namespace.unhold(location);
            }
        }
        Ok(())
    }

    async fn stats(&self) -> Result<Vec<NamespaceStats>> {
        Ok(self
            .store
            .read()
            .namespaces
            .values()
            .map(Namespace::stats)
            .collect())
    }
}

/// What a transaction changed in a namespace, to roll it back.
struct NamespaceUndo {
    /// Chunks from this id on were appended by the transaction.
    first_new_chunk: usize,
    journal_end: u64,
    /// Each key's index entry before the transaction first changed it.
    index: HashMap<Box<[u8]>, Option<IndexEntry>>,
    /// Each key's newest journal entry of `removed` before the transaction
    /// first changed it.
    removed: HashMap<Box<[u8]>, Option<ChangeRef>>,
    /// The cleaning budget at commit follows them.
    written_rows: usize,
}

impl NamespaceUndo {
    fn new(namespace: &Namespace) -> Self {
        Self {
            first_new_chunk: namespace.next_chunk_id(),
            journal_end: namespace.journal.as_ref().map_or(0, Journal::end),
            index: HashMap::new(),
            removed: HashMap::new(),
            written_rows: 0,
        }
    }

    fn rollback(self, namespace: &mut Namespace) {
        if let Some(journal) = &mut namespace.journal {
            let freed = journal.truncate(self.journal_end);
            for (key, newest_change) in self.removed {
                journal.set_removed(key, newest_change);
            }
            for location in freed {
                namespace.unhold(location);
            }
        }
        for (key, before) in self.index {
            if namespace.index.remove(&key).is_some() {
                namespace.key_bytes -= key.len();
            }
            if let Some(before) = before {
                // Every change of a key released the row it had before.
                namespace.chunk_mut(before.location.chunk).live_rows += 1;
                namespace.key_bytes += key.len();
                namespace.index.insert(key, before);
            }
        }
        while namespace.next_chunk_id() > self.first_new_chunk {
            namespace.pop_back_chunk();
        }
    }
}

struct InMemoryTransaction {
    table: String,
    max_dead_ratio: f64,
    version: Version,
    store: ArcRwLockWriteGuard<parking_lot::RawRwLock, Store>,
    undo: HashMap<String, NamespaceUndo>,
    /// Each meta key's value before the transaction first set it.
    meta_undo: HashMap<String, Option<Vec<u8>>>,
    committed: bool,
}

impl InMemoryTransaction {
    /// The namespace, and its undo log.
    fn namespace(&mut self, name: &str) -> Result<(&mut Namespace, &mut NamespaceUndo)> {
        let namespace =
            self.store.namespaces.get_mut(name).ok_or_else(|| {
                streamling_err!("table '{}': no namespace '{}'", self.table, name)
            })?;
        let undo = self
            .undo
            .entry(name.to_string())
            .or_insert_with(|| NamespaceUndo::new(namespace));
        Ok((namespace, undo))
    }

    /// Removes `key`'s live row, and returns it.
    fn remove(
        version: Version,
        namespace: &mut Namespace,
        undo: &mut NamespaceUndo,
        key: &[u8],
    ) -> Option<RowLocation> {
        let (key, entry) = namespace.index.remove_entry(key)?;
        undo.index.entry(key.clone()).or_insert(Some(entry));
        namespace.key_bytes -= key.len();
        if let Some(journal) = &mut namespace.journal {
            let newest_change = if journal.changed_in(version, entry.newest_change) {
                entry.newest_change
            } else {
                Some(journal.record(version, entry.newest_change, Some(entry.location)))
            };
            let before = journal.set_removed(key.clone(), newest_change);
            undo.removed.entry(key).or_insert(before);
            if newest_change != entry.newest_change {
                namespace.hold(entry.location);
            }
        }
        namespace.release(entry.location);
        Some(entry.location)
    }

    /// Points `key`'s index entry at `location`, and returns the row it
    /// replaces.
    fn upsert(
        version: Version,
        namespace: &mut Namespace,
        undo: &mut NamespaceUndo,
        key: &[u8],
        location: RowLocation,
    ) -> Option<RowLocation> {
        match namespace.index.get_mut(key) {
            Some(entry) => {
                undo.index.entry(key.into()).or_insert(Some(*entry));
                let replaced = std::mem::replace(&mut entry.location, location);
                let journaled = match &mut namespace.journal {
                    Some(journal) if !journal.changed_in(version, entry.newest_change) => {
                        entry.newest_change =
                            Some(journal.record(version, entry.newest_change, Some(replaced)));
                        true
                    }
                    _ => false,
                };
                if journaled {
                    namespace.hold(replaced);
                }
                namespace.release(replaced);
                Some(replaced)
            }
            None => {
                undo.index.entry(key.into()).or_insert(None);
                namespace.key_bytes += key.len();
                let newest_change = match &mut namespace.journal {
                    Some(journal) => {
                        let previous = journal.take_removed(key);
                        undo.removed.entry(key.into()).or_insert(previous);
                        if journal.changed_in(version, previous) {
                            previous
                        } else {
                            Some(journal.record(version, previous, None))
                        }
                    }
                    None => None,
                };
                namespace.index.insert(
                    key.into(),
                    IndexEntry {
                        location,
                        newest_change,
                    },
                );
                None
            }
        }
    }
}

#[async_trait]
impl TableTransaction for InMemoryTransaction {
    fn version(&self) -> Version {
        self.version
    }

    async fn get(&mut self, namespace: &str, keys: &RecordBatch) -> Result<LookupRows> {
        let table = self.table.clone();
        let (namespace, _) = self.namespace(namespace)?;
        let projection: Vec<usize> = (0..namespace.spec.schema.fields().len()).collect();
        namespace.lookup(&table, keys, &projection, None)
    }

    async fn write(
        &mut self,
        namespace: &str,
        upserts: &RecordBatch,
        deletes: &RecordBatch,
    ) -> Result<RecordBatch> {
        let (table, version) = (self.table.clone(), self.version);
        let (namespace, undo) = self.namespace(namespace)?;
        let upserts = RecordBatch::try_new(
            Arc::clone(&namespace.spec.schema),
            upserts.columns().to_vec(),
        )
        .streamling_with_context(|| {
            format!(
                "table '{}': rows don't match namespace '{}'",
                table, namespace.spec.name
            )
        })?;
        let upserted_keys = namespace.encode_keys(&table, &namespace.stored_keys(&upserts))?;
        let deleted_keys = namespace.encode_keys(&table, deletes.columns())?;
        let mut removed = Vec::new();
        let new_chunk = namespace.next_chunk_id();
        if upserts.num_rows() > 0 {
            namespace.push_chunk(upserts.clone(), 0)?;
        }
        for (row, key) in upserted_keys.iter().enumerate() {
            let location = RowLocation {
                chunk: new_chunk,
                row,
            };
            removed.extend(Self::upsert(
                version,
                namespace,
                undo,
                key.as_ref(),
                location,
            ));
            namespace.chunk_mut(new_chunk).live_rows += 1;
        }
        for key in deleted_keys.iter() {
            removed.extend(Self::remove(version, namespace, undo, key.as_ref()));
        }
        undo.written_rows += upserts.num_rows() + deletes.num_rows();
        namespace.rows_at(&removed)
    }

    async fn evict_below(
        &mut self,
        namespace: &str,
        column: &str,
        bound: &ScalarValue,
        inclusive: bool,
        limit: usize,
    ) -> Result<RecordBatch> {
        let version = self.version;
        let (namespace, undo) = self.namespace(namespace)?;
        let column_index = namespace.spec.schema.index_of(column)?;
        let candidates: Vec<(usize, RecordBatch)> = namespace
            .chunks_below(column_index, bound, inclusive)
            .into_iter()
            .map(|id| (id, namespace.chunk(id).batch.clone()))
            .collect();
        let bound_array = Scalar::new(bound.to_array()?);
        let mut evicted = Vec::new();
        'chunks: for (chunk, batch) in candidates {
            let values = batch.column(column_index);
            let below = if inclusive {
                lt_eq(values, &bound_array)?
            } else {
                lt(values, &bound_array)?
            };
            let outside = or_kleene(&below, &is_null(values)?)?;
            if !outside.has_true() {
                continue;
            }
            let key_columns = namespace
                .key_indices
                .iter()
                .map(|&index| filter(batch.column(index), &outside))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let keys = namespace
                .converter
                .convert_columns(&key_columns)
                .streamling_context("failed to encode the stored keys")?;
            for (row, key) in outside.values().set_indices().zip(keys.iter()) {
                let location = RowLocation { chunk, row };
                if namespace
                    .index
                    .get(key.as_ref())
                    .is_some_and(|entry| entry.location == location)
                {
                    Self::remove(version, namespace, undo, key.as_ref());
                    evicted.push(location);
                    if evicted.len() == limit {
                        break 'chunks;
                    }
                }
            }
        }
        undo.written_rows += evicted.len();
        namespace.rows_at(&evicted)
    }

    async fn scan(
        &mut self,
        namespace: &str,
        cursor: Option<ScanCursor>,
        limit: usize,
    ) -> Result<(RecordBatch, Option<ScanCursor>)> {
        let table = self.table.clone();
        let (namespace, _) = self.namespace(namespace)?;
        let (mut chunk, mut row) = match cursor {
            Some(ScanCursor(bytes)) => {
                let bytes: [u8; CURSOR_BYTES] = bytes
                    .try_into()
                    .map_err(|_| streamling_err!("table '{}': invalid scan cursor", table))?;
                let (chunk, row) = bytes.split_at(CURSOR_BYTES / 2);
                (
                    u64::from_le_bytes(chunk.try_into().expect("half a cursor")) as usize,
                    u64::from_le_bytes(row.try_into().expect("half a cursor")) as usize,
                )
            }
            None => (namespace.first_chunk_id, 0),
        };
        let mut live = Vec::new();
        while chunk < namespace.next_chunk_id() && live.len() < limit {
            let batch = &namespace.chunk(chunk).batch;
            if namespace.chunk(chunk).live_rows == 0 || row >= batch.num_rows() {
                (chunk, row) = (chunk + 1, 0);
                continue;
            }
            let end = batch.num_rows().min(row + limit - live.len());
            let key_columns: Vec<ArrayRef> = namespace
                .key_indices
                .iter()
                .map(|&index| batch.column(index).slice(row, end - row))
                .collect();
            let keys = namespace
                .converter
                .convert_columns(&key_columns)
                .streamling_context("failed to encode the stored keys")?;
            for (offset, key) in keys.iter().enumerate() {
                let location = RowLocation {
                    chunk,
                    row: row + offset,
                };
                if namespace
                    .index
                    .get(key.as_ref())
                    .is_some_and(|entry| entry.location == location)
                {
                    live.push(location);
                }
            }
            row = end;
        }
        let next = (chunk < namespace.next_chunk_id()).then(|| {
            let mut bytes = (chunk as u64).to_le_bytes().to_vec();
            bytes.extend((row as u64).to_le_bytes());
            ScanCursor(bytes)
        });
        Ok((namespace.rows_at(&live)?, next))
    }

    async fn set_meta(&mut self, key: &str, value: Vec<u8>) -> Result<()> {
        let before = self.store.meta.insert(key.to_string(), value);
        self.meta_undo.entry(key.to_string()).or_insert(before);
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.committed = true;
        self.store.next_version = self.version + 1;
        let max_dead_ratio = self.max_dead_ratio;
        for (name, undo) in std::mem::take(&mut self.undo) {
            if let Some(namespace) = self.store.namespaces.get_mut(&name) {
                namespace.clean(max_dead_ratio, undo.written_rows)?;
            }
        }
        Ok(())
    }
}

impl Drop for InMemoryTransaction {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        for (name, undo) in std::mem::take(&mut self.undo) {
            if let Some(namespace) = self.store.namespaces.get_mut(&name) {
                undo.rollback(namespace);
            }
        }
        for (key, value) in std::mem::take(&mut self.meta_undo) {
            match value {
                Some(value) => self.store.meta.insert(key, value),
                None => self.store.meta.remove(&key),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::conformance::{self, BackendFactory};
    use arrow::array::{AsArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    const TABLE: &str = "tokens";
    const NAMESPACE: &str = "rows";
    const MAX_DEAD_RATIO: f64 = 0.25;

    struct Factory;

    #[async_trait]
    impl BackendFactory for Factory {
        async fn create(&self, store: &str) -> Arc<dyn TableBackend> {
            Arc::new(InMemoryBackend::try_new(store, MAX_DEAD_RATIO).unwrap())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn satisfies_the_backend_contract() {
        conformance::run(&Factory).await;
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, true),
        ]))
    }

    async fn backend(versioned: bool) -> InMemoryBackend {
        backend_cleaning_at(versioned, MAX_DEAD_RATIO).await
    }

    async fn backend_cleaning_at(versioned: bool, max_dead_ratio: f64) -> InMemoryBackend {
        let backend = InMemoryBackend::try_new(TABLE, max_dead_ratio).unwrap();
        backend
            .create_namespace(&NamespaceSpec {
                name: NAMESPACE.to_string(),
                schema: schema(),
                key: vec!["id".to_string()],
                versioned,
                range_column: Some("ts".to_string()),
            })
            .await
            .unwrap();
        backend
    }

    fn rows(rows: &[(i64, &str, Option<i64>)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.2))),
            ],
        )
        .unwrap()
    }

    fn keys(ids: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap()
    }

    /// Commits a transaction upserting `upserts` (each `(id, name)`) and
    /// deleting `deletes`, and returns its mark.
    async fn write(backend: &InMemoryBackend, upserts: &[(i64, &str)], deletes: &[i64]) -> Mark {
        let upserts: Vec<(i64, &str, Option<i64>)> =
            upserts.iter().map(|&(id, name)| (id, name, None)).collect();
        let mut transaction = backend.begin().await.unwrap();
        transaction
            .write(NAMESPACE, &rows(&upserts), &keys(deletes))
            .await
            .unwrap();
        let mark = Mark::after(transaction.version());
        transaction.commit().await.unwrap();
        mark
    }

    async fn names(
        backend: &InMemoryBackend,
        ids: &[i64],
        mark: Option<Mark>,
    ) -> Vec<Option<String>> {
        let found = backend
            .lookup(NAMESPACE, &keys(ids), &[1], mark)
            .await
            .unwrap();
        found
            .rows
            .column(0)
            .as_string::<i32>()
            .iter()
            .map(|name| name.map(str::to_string))
            .collect()
    }

    fn some(values: &[&str]) -> Vec<Option<String>> {
        values.iter().map(|name| Some(name.to_string())).collect()
    }

    /// Reads the namespace's state.
    fn inspect<T>(backend: &InMemoryBackend, read: impl FnOnce(&Namespace) -> T) -> T {
        read(&backend.store.read().namespaces[NAMESPACE])
    }

    /// Every counter matches what it counts.
    fn assert_consistent(backend: &InMemoryBackend) {
        inspect(backend, |namespace| {
            assert_eq!(
                namespace
                    .chunks
                    .iter()
                    .map(|chunk| chunk.live_rows)
                    .sum::<usize>(),
                namespace.index.len(),
                "live rows"
            );
            assert_eq!(
                namespace
                    .chunks
                    .iter()
                    .map(|chunk| chunk.batch.num_rows())
                    .sum::<usize>(),
                namespace.total_rows,
                "total rows"
            );
            assert_eq!(
                namespace
                    .chunks
                    .iter()
                    .map(|chunk| chunk.batch.get_array_memory_size())
                    .sum::<usize>(),
                namespace.chunk_bytes,
                "chunk bytes"
            );
            assert_eq!(
                namespace.index.keys().map(|key| key.len()).sum::<usize>(),
                namespace.key_bytes,
                "key bytes"
            );
            let mut held: HashMap<usize, usize> = HashMap::new();
            if let Some(journal) = &namespace.journal {
                for before in journal.befores() {
                    *held.entry(before.chunk).or_default() += 1;
                }
            }
            for (id, chunk) in (namespace.first_chunk_id..).zip(&namespace.chunks) {
                assert_eq!(
                    chunk.held,
                    held.remove(&id).unwrap_or(0),
                    "chunk {id} holds"
                );
            }
            for (id, (_, count)) in &namespace.retired {
                assert_eq!(
                    *count,
                    held.remove(id).unwrap_or(0),
                    "retired chunk {id} holds"
                );
            }
            assert!(held.is_empty(), "every held chunk is live or retired");
        });
    }

    #[test]
    fn rejects_a_max_dead_ratio_that_is_not_a_positive_number() {
        for ratio in [0.0, -0.5, f64::NAN, f64::INFINITY] {
            let err = InMemoryBackend::try_new(TABLE, ratio)
                .expect_err("the ratio must be rejected")
                .to_string();
            assert!(
                err.contains("max_dead_ratio must be a positive number"),
                "{ratio}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn cleaning_keeps_contents_and_remaps_the_index() {
        let backend = backend(false).await;
        write(&backend, &[(1, "a"), (2, "b"), (3, "c")], &[]).await;
        // Each write leaves 2 dead rows, more than a quarter of the 3 live
        // ones, so every chunk holding a dead row is cleaned.
        write(&backend, &[(1, "a2"), (2, "b2")], &[]).await;
        write(&backend, &[(1, "a3"), (3, "c3")], &[]).await;

        inspect(&backend, |namespace| {
            assert_eq!(namespace.first_chunk_id, 3);
            assert_eq!(namespace.total_rows, 3);
        });
        assert_eq!(
            names(&backend, &[1, 2, 3], None).await,
            some(&["a3", "b2", "c3"])
        );
        // Key 2 was moved by the cleaner; the delete releases it at its new place.
        write(&backend, &[], &[2]).await;
        assert_eq!(
            names(&backend, &[1, 2, 3], None).await,
            [Some("a3".to_string()), None, Some("c3".to_string())]
        );
        assert_consistent(&backend);
    }

    #[tokio::test]
    async fn lookup_follows_cleaning() {
        let backend = backend(false).await;
        write(&backend, &[(1, "a"), (2, "b")], &[]).await;
        write(&backend, &[(1, "a2")], &[]).await;
        write(&backend, &[(1, "a3"), (2, "b2")], &[]).await;
        inspect(&backend, |namespace| {
            assert_eq!(namespace.first_chunk_id, 3)
        });
        assert_eq!(names(&backend, &[2, 1], None).await, some(&["b2", "a3"]));
    }

    #[tokio::test]
    async fn cleaning_keeps_dead_rows_within_the_ratio() {
        const KEYS: i64 = 4096;
        const BATCH: i64 = 512;
        const UPDATE_BATCHES: i64 = 64;
        // Coprime with KEYS, so updates sweep the keys in a scattered order.
        const STRIDE: i64 = 2053;
        let backend = backend(false).await;
        for start in (0..KEYS).step_by(BATCH as usize) {
            let inserts: Vec<(i64, &str)> = (start..start + BATCH).map(|id| (id, "a")).collect();
            write(&backend, &inserts, &[]).await;
        }
        for round in 0..UPDATE_BATCHES {
            let updates: Vec<(i64, &str)> = (0..BATCH)
                .map(|i| (((round * BATCH + i) * STRIDE) % KEYS, "b"))
                .collect();
            write(&backend, &updates, &[]).await;
            inspect(&backend, |namespace| {
                let live = namespace.index.len();
                let dead = namespace.total_rows - live;
                assert!(
                    dead as f64 <= MAX_DEAD_RATIO * live as f64 + BATCH as f64,
                    "round {round}: {dead} dead rows for {live} live ones"
                );
            });
        }
    }

    #[tokio::test]
    async fn hot_key_updates_drop_dead_chunks() {
        const UPDATES: usize = 1000;
        let backend = backend(false).await;
        for _ in 0..UPDATES {
            write(&backend, &[(1, "a")], &[]).await;
            inspect(&backend, |namespace| {
                assert_eq!((namespace.chunks.len(), namespace.total_rows), (1, 1));
            });
        }
    }

    #[tokio::test]
    async fn cleaning_resumes_mid_chunk() {
        const KEYS: i64 = 3000;
        const UPDATE_BATCH: i64 = 100;
        // 800 dead rows exceed a quarter of the 3000 live ones only after the
        // last batch, whose cleaning budget is MIN_CLEANING_ROWS.
        const UPDATE_BATCHES: i64 = 8;
        let backend = backend(false).await;
        let names_by_id: Vec<String> = (0..KEYS).map(|id| format!("v{id}")).collect();
        let inserts: Vec<(i64, &str)> = (0..KEYS)
            .map(|id| (id, names_by_id[id as usize].as_str()))
            .collect();
        write(&backend, &inserts, &[]).await;
        for start in (0..UPDATE_BATCH * UPDATE_BATCHES).step_by(UPDATE_BATCH as usize) {
            let updates: Vec<(i64, &str)> =
                (start..start + UPDATE_BATCH).map(|id| (id, "u")).collect();
            write(&backend, &updates, &[]).await;
        }

        inspect(&backend, |namespace| {
            assert_eq!(
                namespace.first_chunk_id, 0,
                "the insert chunk is partly cleaned"
            );
            assert_eq!(namespace.cleaning_cursor, MIN_CLEANING_ROWS);
        });
        // 900 was moved, 2900 is past the cursor, 5 was updated.
        assert_eq!(
            names(&backend, &[900, 2900, 5], None).await,
            some(&["v900", "v2900", "u"])
        );
        write(&backend, &[], &[900, 2900]).await;
        assert_eq!(names(&backend, &[900, 2900], None).await, [None, None]);
        assert_consistent(&backend);
    }

    #[tokio::test]
    async fn the_journal_holds_dropped_chunks_until_trimmed() {
        let backend = backend(true).await;
        let first = write(&backend, &[(1, "a"), (2, "b"), (3, "c")], &[]).await;
        let second = write(&backend, &[(1, "a2"), (2, "b2")], &[]).await;
        inspect(&backend, |namespace| {
            assert!(
                namespace.retired_bytes > 0,
                "the cleaner dropped the first chunk, which a read at the first write needs"
            );
        });
        assert_eq!(
            names(&backend, &[1, 2, 3], Some(first)).await,
            some(&["a", "b", "c"])
        );
        assert_consistent(&backend);

        backend.finalize(second, true).await.unwrap();

        inspect(&backend, |namespace| {
            assert_eq!(namespace.retired_bytes, 0);
            assert_eq!(namespace.journal.as_ref().unwrap().count(), 0);
        });
        assert_eq!(
            names(&backend, &[1, 2, 3], Some(second)).await,
            some(&["a2", "b2", "c"])
        );
        assert_consistent(&backend);
    }

    #[tokio::test]
    async fn a_retired_chunk_stays_until_its_last_entry_is_trimmed() {
        const KEYS: i64 = 10;
        let backend = backend(true).await;
        let inserts: Vec<(i64, &str)> = (1..=KEYS).map(|id| (id, "old")).collect();
        write(&backend, &inserts, &[]).await;
        // Within the dead-row ratio, so the first chunk stays live.
        let second = write(&backend, &[(1, "new")], &[]).await;
        let updates: Vec<(i64, &str)> = (2..=KEYS).map(|id| (id, "new")).collect();
        let third = write(&backend, &updates, &[]).await;
        inspect(&backend, |namespace| {
            assert!(
                namespace.first_chunk_id > 0,
                "the cleaner dropped the first chunk, held by the second and third writes' entries"
            );
        });

        backend.finalize(second, true).await.unwrap();
        inspect(&backend, |namespace| assert!(namespace.retired_bytes > 0));
        assert_eq!(
            names(&backend, &[1, 2], Some(second)).await,
            some(&["new", "old"])
        );

        backend.finalize(third, true).await.unwrap();
        inspect(&backend, |namespace| assert_eq!(namespace.retired_bytes, 0));
        assert_consistent(&backend);
    }

    #[tokio::test]
    async fn a_rolled_back_transaction_leaves_every_counter_as_it_was() {
        let backend = backend(true).await;
        write(&backend, &[(1, "a"), (2, "b"), (3, "c")], &[]).await;
        write(&backend, &[(1, "a2")], &[3]).await;
        let mut transaction = backend.begin().await.unwrap();
        transaction
            .write(
                NAMESPACE,
                &rows(&[(1, "x", Some(1)), (4, "d", None)]),
                &keys(&[2]),
            )
            .await
            .unwrap();
        transaction
            .write(
                NAMESPACE,
                &rows(&[(2, "y", None), (3, "z", None)]),
                &keys(&[1, 4]),
            )
            .await
            .unwrap();
        transaction
            .evict_below(NAMESPACE, "ts", &ScalarValue::Int64(Some(0)), false, 10)
            .await
            .unwrap();
        drop(transaction);

        assert_consistent(&backend);
        assert_eq!(
            names(&backend, &[1, 2, 3, 4], None).await,
            [Some("a2".to_string()), Some("b".to_string()), None, None]
        );
        let mark = write(&backend, &[(3, "c2")], &[]).await;
        assert_eq!(
            names(&backend, &[1, 2, 3], Some(mark)).await,
            some(&["a2", "b", "c2"])
        );
        assert_consistent(&backend);
    }

    #[tokio::test]
    async fn eviction_skips_the_chunks_wholly_above_the_bound() {
        // High enough that no chunk is cleaned.
        const NO_CLEANING_RATIO: f64 = 100.0;
        let backend = backend_cleaning_at(false, NO_CLEANING_RATIO).await;
        // Chunks 0 to 3: above, straddling, below, above but with a null.
        for rows_of_chunk in [
            rows(&[(1, "a", Some(10)), (2, "b", Some(20))]),
            rows(&[(3, "c", Some(1)), (4, "d", Some(20))]),
            rows(&[(5, "e", Some(1))]),
            rows(&[(6, "f", Some(20)), (7, "g", None)]),
        ] {
            let mut transaction = backend.begin().await.unwrap();
            transaction
                .write(NAMESPACE, &rows_of_chunk, &keys(&[]))
                .await
                .unwrap();
            transaction.commit().await.unwrap();
        }
        let bound = ScalarValue::Int64(Some(5));
        let chunks_below = |backend: &InMemoryBackend| {
            inspect(backend, |namespace| {
                namespace.chunks_below(2, &bound, false)
            })
        };
        assert_eq!(chunks_below(&backend), [1, 2, 3]);

        let mut transaction = backend.begin().await.unwrap();
        let evicted = transaction
            .evict_below(NAMESPACE, "ts", &bound, false, 10)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let mut evicted_ids = evicted
            .column(0)
            .as_primitive::<arrow::datatypes::Int64Type>()
            .values()
            .to_vec();
        evicted_ids.sort();
        assert_eq!(evicted_ids, [3, 5, 7]);
        assert_eq!(
            chunks_below(&backend),
            [1, 3],
            "a chunk without live rows isn't evaluated, whatever its statistics"
        );
    }

    #[tokio::test]
    async fn cleaned_chunks_get_statistics() {
        let backend = backend(false).await;
        let mut transaction = backend.begin().await.unwrap();
        transaction
            .write(
                NAMESPACE,
                &rows(&[
                    (1, "a", Some(1)),
                    (2, "b", Some(2)),
                    (3, "c", Some(3)),
                    (4, "d", Some(4)),
                ]),
                &keys(&[]),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        // Dead rows exceed the ratio, so chunk 0's live rows move to chunk 2.
        write(&backend, &[(1, "a"), (2, "b")], &[]).await;

        inspect(&backend, |namespace| {
            assert_eq!((namespace.first_chunk_id, namespace.chunks.len()), (1, 2));
            for chunk in &namespace.chunks {
                assert_eq!(
                    chunk.statistics.as_ref(),
                    Some(&ColumnStatistics::of(chunk.batch.column(2)).unwrap())
                );
            }
            assert_eq!(
                namespace.chunks[1].statistics.as_ref().unwrap().min,
                ScalarValue::Int64(Some(3))
            );
        });
    }

    #[test]
    fn index_entries_take_24_bytes() {
        const INDEX_ENTRY_BYTES: usize = 24;
        const CHANGE_LINK_BYTES: usize = 8;
        assert_eq!(size_of::<Option<ChangeRef>>(), CHANGE_LINK_BYTES);
        assert_eq!(size_of::<IndexEntry>(), INDEX_ENTRY_BYTES);
    }
}
