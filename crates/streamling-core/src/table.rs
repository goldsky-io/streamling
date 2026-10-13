//! The `table` transform: the latest row per primary key, maintained from a
//! changelog stream. The table is applied by its upstream's side output.

pub mod driver;
pub mod side_output;

use crate::data::COLUMN_NAME_OP;
use crate::error::{Result, ResultExt};
use crate::telemetry::recorder::get_metrics_recorder;
use crate::{streamling_user_bail, streamling_user_err};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, new_null_array};
use arrow::compute::{cast, interleave, interleave_record_batch};
use arrow::datatypes::{FieldRef, Fields, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

const OP_DELETE: &str = "d";
const UPSERT_OPS: [&str; 4] = ["i", "c", "r", "u"];

const LIVE_ROWS_GAUGE: &str = "table_live_rows";
const MEMORY_BYTES_GAUGE: &str = "table_memory_bytes";

/// Floor of a cleaning step's budget, so tiny batches still merge old chunks
/// into chunks of a useful size.
const MIN_CLEANING_ROWS: usize = 1024;

#[derive(Clone, Copy)]
enum RowOp {
    Upsert,
    Delete,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct RowLocation {
    chunk: usize,
    row: usize,
}

struct Chunk {
    batch: RecordBatch,
    /// Rows of `batch` the index points to.
    live_rows: usize,
}

#[derive(Default)]
struct TableState {
    /// Oldest first. A chunk's id is its position plus `first_chunk_id`, so
    /// ids stay valid while the cleaner drops chunks from the front.
    chunks: VecDeque<Chunk>,
    first_chunk_id: usize,
    /// The front chunk's next row to clean.
    cleaning_cursor: usize,
    /// Primary key, encoded by the table's `RowConverter`, to its live row.
    index: HashMap<Box<[u8]>, RowLocation>,
    total_rows: usize,
    chunk_bytes: usize,
    key_bytes: usize,
}

impl TableState {
    fn chunk(&self, id: usize) -> &Chunk {
        &self.chunks[id - self.first_chunk_id]
    }

    fn chunk_mut(&mut self, id: usize) -> &mut Chunk {
        &mut self.chunks[id - self.first_chunk_id]
    }

    fn next_chunk_id(&self) -> usize {
        self.first_chunk_id + self.chunks.len()
    }

    fn push_chunk(&mut self, batch: RecordBatch, live_rows: usize) {
        self.total_rows += batch.num_rows();
        self.chunk_bytes += batch.get_array_memory_size();
        self.chunks.push_back(Chunk { batch, live_rows });
    }

    fn pop_front_chunk(&mut self) {
        if let Some(chunk) = self.chunks.pop_front() {
            self.total_rows -= chunk.batch.num_rows();
            self.chunk_bytes -= chunk.batch.get_array_memory_size();
            self.first_chunk_id += 1;
            self.cleaning_cursor = 0;
        }
    }

    /// The row at `location` is no longer the live row of its key.
    fn release(&mut self, location: RowLocation) {
        self.chunk_mut(location.chunk).live_rows -= 1;
    }
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
/// Nothing is persisted, so the table starts empty after a restart.
pub struct InMemoryTable {
    name: String,
    metric_metadata_id: String,
    /// The input schema without `_gs_op`.
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
        let schema = Arc::new(Schema::new(
            value_indices
                .iter()
                .map(|&index| Arc::clone(&input_schema.fields()[index]))
                .collect::<Vec<_>>(),
        ));
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
            state: RwLock::new(TableState::default()),
        })
    }

    /// Applies `batch` (the table's SQL output) in row order: an upsert
    /// replaces its key's row, and a delete removes it.
    pub fn apply(&self, batch: &RecordBatch) -> Result<()> {
        let ops = self.parse_ops(batch.column(self.op_index))?;
        let key_columns: Vec<ArrayRef> = self
            .key_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index)))
            .collect();
        if let Some(position) = key_columns
            .iter()
            .position(|column| column.null_count() > 0)
        {
            streamling_user_bail!(
                "table '{}': primary key column '{}' contains a null value",
                self.name,
                batch.schema().field(self.key_indices[position]).name()
            );
        }
        let keys = self
            .converter
            .convert_columns(&key_columns)
            .streamling_context("failed to encode the primary key")?;
        let values = RecordBatch::try_new(
            Arc::clone(&self.schema),
            self.value_indices
                .iter()
                .map(|&index| Arc::clone(batch.column(index)))
                .collect(),
        )
        .streamling_with_context(|| {
            format!(
                "table '{}': batch does not match the table schema",
                self.name
            )
        })?;

        let written_rows = values.num_rows();
        let mut guard = self.state.write();
        let state = &mut *guard;
        let new_chunk = state.next_chunk_id();
        // Stored before the loop, so a row the batch replaces in itself is
        // released like any other.
        if written_rows > 0 {
            state.push_chunk(values, 0);
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
                        Some(existing) => Some(std::mem::replace(existing, location)),
                        None => {
                            state.key_bytes += key.as_ref().len();
                            state.index.insert(key.as_ref().into(), location);
                            None
                        }
                    };
                    if let Some(replaced) = replaced {
                        state.release(replaced);
                    }
                    state.chunk_mut(new_chunk).live_rows += 1;
                }
                RowOp::Delete => {
                    if let Some(stored) = state.index.remove(key.as_ref()) {
                        state.key_bytes -= key.as_ref().len();
                        state.release(stored);
                    }
                }
            }
        }

        self.clean(state, written_rows)?;
        let live_rows = state.index.len();
        let memory_bytes = state.chunk_bytes + state.key_bytes;
        drop(guard);
        self.record_gauges(live_rows, memory_bytes);
        Ok(())
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
    /// or null when no row has it. `keys` is cast to the primary key's type.
    pub fn lookup(&self, keys: &ArrayRef, column: &str) -> Result<ArrayRef> {
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
        // Source 0 is a single null for keys with no row. Each chunk a hit
        // reads from is added once, so the cost follows hits, not chunk count.
        let mut sources: Vec<&dyn Array> = vec![missing.as_ref()];
        // Chunk number to its position in `sources`.
        let mut chunk_sources: HashMap<usize, usize> = HashMap::new();
        // One `(source, row)` per key, in input order. `interleave` copies
        // `sources[source][row]` for each into the output.
        let mut gather_indices = Vec::with_capacity(encoded.num_rows());
        for key in encoded.iter() {
            let Some(location) = state.index.get(key.as_ref()) else {
                gather_indices.push((0, 0));
                continue;
            };
            let source = *chunk_sources.entry(location.chunk).or_insert_with(|| {
                sources.push(
                    state
                        .chunk(location.chunk)
                        .batch
                        .column(column_index)
                        .as_ref(),
                );
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

    fn column_index(&self, column: &str) -> Result<usize> {
        self.schema
            .index_of(column)
            .map_err(|_| streamling_user_err!("table '{}' has no column '{}'", self.name, column))
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
                if let Some(location) = state.index.get_mut(key.as_ref())
                    && *location
                        == (RowLocation {
                            chunk: front_id,
                            row,
                        })
                {
                    *location = RowLocation {
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
            state.push_chunk(batch, moved.len());
        }
        Ok(())
    }

    fn too_many_dead_rows(&self, total_rows: usize, live_rows: usize) -> bool {
        total_rows.saturating_sub(live_rows) as f64 > self.max_dead_ratio * live_rows as f64
    }

    fn record_gauges(&self, live_rows: usize, memory_bytes: usize) {
        let recorder = get_metrics_recorder();
        recorder.record_gauge(LIVE_ROWS_GAUGE, live_rows as u64, &self.metric_metadata_id);
        recorder.record_gauge(
            MEMORY_BYTES_GAUGE,
            memory_bytes as u64,
            &self.metric_metadata_id,
        );
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
        )
        .unwrap()
    }

    /// Live contents as `(id, name)`, sorted by id.
    fn contents(table: &InMemoryTable) -> Vec<(i64, String)> {
        let state = table.state.read();
        let gather: Vec<(usize, usize)> = state
            .index
            .values()
            .map(|location| (location.chunk - state.first_chunk_id, location.row))
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
            .apply(&batch(&[(1, "a", "i"), (2, "b", "c"), (3, "c", "r")]))
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
            .apply(&batch(&[(1, "a2", "u"), (2, "b2", "C"), (3, "c2", "i")]))
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
            .apply(&batch(&[(1, "ignored", "d"), (2, "ignored", "D")]))
            .unwrap();
        assert_eq!(contents(&table), vec![(3, "c2".to_string())]);
        assert_live_rows_match_the_index(&table);
    }

    #[test]
    fn delete_of_an_unknown_key_changes_nothing() {
        let table = table();
        table.apply(&batch(&[(9, "incoming", "d")])).unwrap();
        assert!(contents(&table).is_empty());
    }

    #[test]
    fn later_rows_of_a_batch_win() {
        let table = table();
        table
            .apply(&batch(&[
                (1, "a", "i"),
                (1, "b", "u"),
                (2, "c", "i"),
                (2, "c", "d"),
            ]))
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
            .apply(&input(&[("eth", "0x1", "i"), ("sol", "0x1", "i")]))
            .unwrap();
        assert_eq!(table.state.read().index.len(), 2);
        table
            .apply(&input(&[("eth", "0x1", "u"), ("eth", "0x2", "i")]))
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
        table.apply(&input("i")).unwrap();
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
            .apply(&batch(&[(1, "a", "i"), (2, "b", "i"), (3, "c", "i")]))
            .unwrap();
        // Each batch leaves 2 dead rows, more than a quarter of the 3 live ones,
        // so every chunk holding a dead row is cleaned.
        table
            .apply(&batch(&[(1, "a2", "u"), (2, "b2", "u")]))
            .unwrap();
        table
            .apply(&batch(&[(1, "a3", "u"), (3, "c3", "u")]))
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
        table.apply(&batch(&[(2, "x", "d")])).unwrap();
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
        let err = table().apply(&input).unwrap_err().to_string();
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
            let err = table().apply(&input).unwrap_err().to_string();
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
        let err = table().apply(&input).unwrap_err().to_string();
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
        let err = table().apply(&input).unwrap_err().to_string();
        assert!(err.contains("does not match the table schema"), "{err}");
    }

    fn lookup_names(table: &InMemoryTable, ids: &[Option<i64>]) -> Vec<Option<String>> {
        let keys: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        table
            .lookup(&keys, "name")
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
            .apply(&batch(&[(1, "a", "i"), (2, "b", "i"), (3, "c", "i")]))
            .unwrap();
        table
            .apply(&batch(&[(1, "a2", "u"), (2, "x", "d")]))
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
            .apply(&batch(&[(1, "a", "i"), (2, "b", "i")]))
            .unwrap();
        table.apply(&batch(&[(1, "a2", "u")])).unwrap();
        table
            .apply(&batch(&[(1, "a3", "u"), (2, "b2", "u")]))
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
            table.apply(&batch(&inserts)).unwrap();
        }
        for round in 0..UPDATE_BATCHES {
            let updates: Vec<(i64, &str, &str)> = (0..BATCH)
                .map(|i| (((round * BATCH + i) * STRIDE) % KEYS, "b", "u"))
                .collect();
            table.apply(&batch(&updates)).unwrap();
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
            table.apply(&batch(&[(1, "a", "u")])).unwrap();
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
        table.apply(&batch(&inserts)).unwrap();
        for start in (0..UPDATE_BATCH * UPDATE_BATCHES).step_by(UPDATE_BATCH as usize) {
            let updates: Vec<(i64, &str, &str)> = (start..start + UPDATE_BATCH)
                .map(|id| (id, "u", "u"))
                .collect();
            table.apply(&batch(&updates)).unwrap();
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
            .apply(&batch(&[(900, "x", "d"), (2900, "x", "d")]))
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
        table.apply(&batch(&[(7, "seven", "i")])).unwrap();
        let narrower: ArrayRef = Arc::new(Int32Array::from(vec![7]));
        let text: ArrayRef = Arc::new(StringViewArray::from(vec!["7"]));
        for keys in [narrower, text] {
            let names = table.lookup(&keys, "name").unwrap();
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
        )
        .err()
        .expect("a primary key on _gs_op must be rejected")
        .to_string();
        assert!(err.contains("primary key cannot be '_gs_op'"), "{err}");
    }
}
