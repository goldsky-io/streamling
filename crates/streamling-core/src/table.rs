//! The `table` transform: the latest row per primary key, maintained from a
//! changelog stream.

pub mod apply;
pub mod driver;

use crate::data::COLUMN_NAME_OP;
use crate::error::{Result, ResultExt};
use crate::telemetry::recorder::get_metrics_recorder;
use crate::utils::dedup::deduplicate_record_batch;
use crate::{streamling_user_bail, streamling_user_err};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, StringArray, new_null_array};
use arrow::compute::{cast, interleave, interleave_record_batch};
use arrow::datatypes::{FieldRef, Fields, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

const OP_INSERT: &str = "i";
const OP_UPDATE: &str = "u";
const OP_DELETE: &str = "d";
const UPSERT_OPS: [&str; 4] = ["i", "c", "r", "u"];

const LIVE_ROWS_GAUGE: &str = "table_live_rows";
const MEMORY_BYTES_GAUGE: &str = "table_memory_bytes";

#[derive(Clone, Copy)]
enum RowOp {
    Upsert,
    Delete,
}

#[derive(Clone, Copy)]
struct RowLocation {
    chunk: usize,
    row: usize,
}

#[derive(Default)]
struct TableState {
    /// Rows superseded by a later write stay in their chunk until compaction.
    chunks: Vec<RecordBatch>,
    /// Primary key, encoded by the table's `RowConverter`, to its live row.
    index: HashMap<Box<[u8]>, RowLocation>,
    total_rows: usize,
    chunk_bytes: usize,
    key_bytes: usize,
}

/// The `in_memory` backend of a `table` transform.
pub struct InMemoryTable {
    name: String,
    metric_metadata_id: String,
    /// The input schema without `_gs_op`.
    schema: SchemaRef,
    op_index: usize,
    /// Input columns stored in `schema`, in order.
    value_indices: Vec<usize>,
    key_indices: Vec<usize>,
    /// The primary key columns, in order.
    key_fields: Fields,
    primary_key: String,
    converter: RowConverter,
    state: RwLock<TableState>,
}

impl InMemoryTable {
    /// `input_schema` is the table's SQL output schema. It must contain
    /// `_gs_op` and every `primary_key` column.
    pub fn try_new(
        name: &str,
        input_schema: &SchemaRef,
        primary_key: &[String],
        metric_metadata_id: String,
    ) -> Result<Self> {
        let op_index = input_schema.index_of(COLUMN_NAME_OP).map_err(|_| {
            streamling_user_err!(
                "table '{}': the SQL output must contain the '{}' column",
                name,
                COLUMN_NAME_OP
            )
        })?;
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
        Ok(Self {
            name: name.to_string(),
            metric_metadata_id,
            schema,
            op_index,
            value_indices,
            key_indices,
            key_fields,
            primary_key: primary_key.join(","),
            converter,
            state: RwLock::new(TableState::default()),
        })
    }

    /// Applies `batch` (the table's SQL output) by primary key and returns the
    /// table's changelog: one row per applied row, in `batch`'s schema so the
    /// batch's checkpoint markers carry over.
    pub fn apply(&self, batch: &RecordBatch, deduplicate: bool) -> Result<RecordBatch> {
        let input = if deduplicate {
            deduplicate_record_batch(batch, &self.primary_key)?
        } else {
            batch.clone()
        };
        let ops = self.parse_ops(input.column(self.op_index))?;
        let key_columns: Vec<ArrayRef> = self
            .key_indices
            .iter()
            .map(|&index| Arc::clone(input.column(index)))
            .collect();
        if let Some(position) = key_columns
            .iter()
            .position(|column| column.null_count() > 0)
        {
            streamling_user_bail!(
                "table '{}': primary key column '{}' contains a null value",
                self.name,
                input.schema().field(self.key_indices[position]).name()
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
                .map(|&index| Arc::clone(input.column(index)))
                .collect(),
        )
        .streamling_with_context(|| {
            format!(
                "table '{}': batch does not match the table schema",
                self.name
            )
        })?;

        let mut guard = self.state.write();
        let state = &mut *guard;
        let new_chunk = state.chunks.len();
        let mut changelog_ops = Vec::with_capacity(ops.len());
        // `interleave` indices for each changelog row's values. Source 0 is
        // `values`; deletes of stored keys add the chunks they read from.
        let mut gather = Vec::with_capacity(ops.len());
        let mut stored_sources: HashMap<usize, usize> = HashMap::new();
        let mut reads_stored_rows = false;
        for (row, op) in ops.into_iter().enumerate() {
            let key = keys.row(row);
            match op {
                RowOp::Upsert => {
                    let location = RowLocation {
                        chunk: new_chunk,
                        row,
                    };
                    if let Some(existing) = state.index.get_mut(key.as_ref()) {
                        *existing = location;
                        changelog_ops.push(OP_UPDATE);
                    } else {
                        state.key_bytes += key.as_ref().len();
                        state.index.insert(key.as_ref().into(), location);
                        changelog_ops.push(OP_INSERT);
                    }
                    gather.push((0, row));
                }
                RowOp::Delete => {
                    changelog_ops.push(OP_DELETE);
                    let Some(stored) = state.index.remove(key.as_ref()) else {
                        gather.push((0, row));
                        continue;
                    };
                    state.key_bytes -= key.as_ref().len();
                    reads_stored_rows = true;
                    if stored.chunk == new_chunk {
                        gather.push((0, stored.row));
                    } else {
                        let next_source = stored_sources.len() + 1;
                        let source = *stored_sources.entry(stored.chunk).or_insert(next_source);
                        gather.push((source, stored.row));
                    }
                }
            }
        }

        let changelog_values = if !reads_stored_rows {
            values.clone()
        } else {
            let mut sources = vec![&values; stored_sources.len() + 1];
            for (&chunk, &source) in &stored_sources {
                sources[source] = &state.chunks[chunk];
            }
            interleave_record_batch(&sources, &gather)
                .streamling_context("failed to read stored rows")?
        };

        if values.num_rows() > 0 {
            state.total_rows += values.num_rows();
            state.chunk_bytes += values.get_array_memory_size();
            state.chunks.push(values);
        }
        compact_if_needed(state)?;
        let live_rows = state.index.len();
        let memory_bytes = state.chunk_bytes + state.key_bytes;
        drop(guard);
        self.record_gauges(live_rows, memory_bytes);

        let mut columns = changelog_values.columns().to_vec();
        columns.insert(self.op_index, Arc::new(StringArray::from(changelog_ops)));
        RecordBatch::try_new(batch.schema(), columns).streamling_with_context(|| {
            format!("table '{}': failed to build the changelog", self.name)
        })
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
                sources.push(state.chunks[location.chunk].column(column_index).as_ref());
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

/// Rewrites the live rows into one chunk once dead rows outnumber them, which
/// keeps storage under twice the live rows at amortized O(1) per write.
///
/// A chunk keeps its input's buffers alive. If upstream hands over slices of
/// larger buffers, memory exceeds what the row counts suggest. The upgrade
/// path is to trigger on `get_array_memory_size` instead.
fn compact_if_needed(state: &mut TableState) -> Result<()> {
    let live_rows = state.index.len();
    if state.total_rows - live_rows <= live_rows {
        return Ok(());
    }
    let mut locations: Vec<&mut RowLocation> = state.index.values_mut().collect();
    let gather: Vec<(usize, usize)> = locations
        .iter()
        .map(|location| (location.chunk, location.row))
        .collect();
    let compacted = if gather.is_empty() {
        None
    } else {
        let chunks: Vec<&RecordBatch> = state.chunks.iter().collect();
        Some(
            interleave_record_batch(&chunks, &gather)
                .streamling_context("failed to compact the table")?,
        )
    };
    for (row, location) in locations.iter_mut().enumerate() {
        **location = RowLocation { chunk: 0, row };
    }
    state.chunks = compacted.into_iter().collect();
    state.total_rows = live_rows;
    state.chunk_bytes = state
        .chunks
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::RowKind;
    use arrow::array::{Int32Array, Int64Array, LargeBinaryArray, StringViewArray};
    use arrow::datatypes::{DataType, Field, Int64Type};
    use std::str::FromStr;
    use streamling_common::types::decimal_arb::DecimalArbType;

    const TABLE: &str = "tokens";
    const METRIC_KEY: &str = "app::tokens";

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
        )
        .unwrap()
    }

    fn row(id: i64, name: &str, op: &str) -> (i64, String, String) {
        (id, name.to_string(), op.to_string())
    }

    /// Changelog rows as `(id, name, _gs_op)`.
    fn rows(changelog: &RecordBatch) -> Vec<(i64, String, String)> {
        let ids = changelog
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let names = changelog.column_by_name("name").unwrap().as_string::<i32>();
        let ops = changelog
            .column_by_name(COLUMN_NAME_OP)
            .unwrap()
            .as_string::<i32>();
        (0..changelog.num_rows())
            .map(|i| row(ids.value(i), names.value(i), ops.value(i)))
            .collect()
    }

    /// Live contents as `(id, name)`, sorted by id.
    fn contents(table: &InMemoryTable) -> Vec<(i64, String)> {
        let state = table.state.read();
        let gather: Vec<(usize, usize)> = state
            .index
            .values()
            .map(|location| (location.chunk, location.row))
            .collect();
        if gather.is_empty() {
            return Vec::new();
        }
        let chunks: Vec<&RecordBatch> = state.chunks.iter().collect();
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

    #[test]
    fn upserts_and_deletes_map_to_changelog_ops() {
        let table = table();
        let inserted = table
            .apply(&batch(&[(1, "a", "i"), (2, "b", "c"), (3, "c", "r")]), true)
            .unwrap();
        assert_eq!(
            rows(&inserted),
            vec![row(1, "a", "i"), row(2, "b", "i"), row(3, "c", "i")]
        );
        let updated = table
            .apply(
                &batch(&[(1, "a2", "u"), (2, "b2", "C"), (3, "c2", "i")]),
                true,
            )
            .unwrap();
        assert_eq!(
            rows(&updated),
            vec![row(1, "a2", "u"), row(2, "b2", "u"), row(3, "c2", "u")]
        );
        // A delete of a stored key carries the stored values, not the incoming ones.
        let deleted = table
            .apply(&batch(&[(1, "ignored", "d"), (2, "ignored", "D")]), true)
            .unwrap();
        assert_eq!(rows(&deleted), vec![row(1, "a2", "d"), row(2, "b2", "d")]);
        assert_eq!(contents(&table), vec![(3, "c2".to_string())]);
    }

    #[test]
    fn emitted_ops_parse_as_row_kinds() {
        // The Kafka and print sinks parse `_gs_op` with `RowKind::from_str`,
        // which rejects Debezium's `c` and `r`.
        let table = table();
        let mut ops = Vec::new();
        for input in [
            batch(&[(1, "a", "c")]),
            batch(&[(1, "b", "r")]),
            batch(&[(1, "b", "d")]),
        ] {
            ops.extend(
                rows(&table.apply(&input, true).unwrap())
                    .into_iter()
                    .map(|(_, _, op)| op),
            );
        }
        assert_eq!(ops, ["i", "u", "d"]);
        for op in &ops {
            RowKind::from_str(op).unwrap();
        }
    }

    #[test]
    fn delete_of_an_unknown_key_is_forwarded_with_incoming_values() {
        let table = table();
        let changelog = table.apply(&batch(&[(9, "incoming", "d")]), true).unwrap();
        assert_eq!(rows(&changelog), vec![row(9, "incoming", "d")]);
        assert!(contents(&table).is_empty());
    }

    #[test]
    fn delete_after_insert_in_the_same_batch_carries_the_inserted_values() {
        let table = table();
        let changelog = table
            .apply(&batch(&[(1, "inserted", "i"), (1, "incoming", "d")]), false)
            .unwrap();
        assert_eq!(
            rows(&changelog),
            vec![row(1, "inserted", "i"), row(1, "inserted", "d")]
        );
        assert!(contents(&table).is_empty());
    }

    #[test]
    fn deduplicate_changes_the_changelog_but_not_the_final_state() {
        let input = batch(&[(1, "a", "i"), (1, "b", "u"), (2, "c", "i"), (2, "c", "d")]);

        let deduplicated = table();
        assert_eq!(
            rows(&deduplicated.apply(&input, true).unwrap()),
            vec![row(1, "b", "i"), row(2, "c", "d")]
        );

        let raw = table();
        assert_eq!(
            rows(&raw.apply(&input, false).unwrap()),
            vec![
                row(1, "a", "i"),
                row(1, "b", "u"),
                row(2, "c", "i"),
                row(2, "c", "d")
            ]
        );

        assert_eq!(contents(&raw), vec![(1, "b".to_string())]);
        assert_eq!(contents(&deduplicated), contents(&raw));
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
        let ops = |changelog: RecordBatch| -> Vec<String> {
            changelog
                .column_by_name(COLUMN_NAME_OP)
                .unwrap()
                .as_string::<i32>()
                .iter()
                .map(|op| op.unwrap().to_string())
                .collect()
        };

        let first = table
            .apply(&input(&[("eth", "0x1", "i"), ("sol", "0x1", "i")]), true)
            .unwrap();
        assert_eq!(ops(first), ["i", "i"]);
        let second = table
            .apply(&input(&[("eth", "0x1", "u"), ("eth", "0x2", "i")]), true)
            .unwrap();
        assert_eq!(ops(second), ["u", "i"]);
        assert_eq!(table.state.read().index.len(), 3);
    }

    #[test]
    fn field_metadata_survives_into_storage_and_changelog() {
        let amount = DecimalArbType::field("amount", 78, 0, true).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            amount.clone(),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        let table =
            InMemoryTable::try_new(TABLE, &schema, &["id".to_string()], METRIC_KEY.to_string())
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
        table.apply(&input("i"), true).unwrap();
        // The delete's values are gathered from storage.
        let changelog = table.apply(&input("d"), true).unwrap();
        assert_eq!(
            changelog
                .schema()
                .field_with_name("amount")
                .unwrap()
                .metadata(),
            amount.metadata()
        );
        assert_eq!(
            table.schema.field_with_name("amount").unwrap().metadata(),
            amount.metadata()
        );
    }

    #[test]
    fn compaction_keeps_contents_and_remaps_the_index() {
        let table = table();
        table
            .apply(&batch(&[(1, "a", "i"), (2, "b", "i"), (3, "c", "i")]), true)
            .unwrap();
        table
            .apply(&batch(&[(1, "a2", "u"), (2, "b2", "u")]), true)
            .unwrap();
        assert_eq!(
            table.state.read().chunks.len(),
            2,
            "2 dead rows do not outnumber 3 live ones"
        );
        table
            .apply(&batch(&[(1, "a3", "u"), (3, "c3", "u")]), true)
            .unwrap();

        {
            let state = table.state.read();
            assert_eq!(state.chunks.len(), 1, "4 dead rows outnumber 3 live ones");
            assert_eq!(state.total_rows, 3);
            assert_eq!(state.chunk_bytes, state.chunks[0].get_array_memory_size());
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
        // The delete reads the stored row through the remapped index.
        let deleted = table.apply(&batch(&[(2, "x", "d")]), true).unwrap();
        assert_eq!(rows(&deleted), vec![row(2, "b2", "d")]);
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
        let err = table().apply(&input, true).unwrap_err().to_string();
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
            let err = table().apply(&input, true).unwrap_err().to_string();
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
        let err = table().apply(&input, true).unwrap_err().to_string();
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
        let err = table().apply(&input, true).unwrap_err().to_string();
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
            .apply(&batch(&[(1, "a", "i"), (2, "b", "i"), (3, "c", "i")]), true)
            .unwrap();
        table
            .apply(&batch(&[(1, "a2", "u"), (2, "x", "d")]), true)
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
    fn lookup_follows_compaction() {
        let table = table();
        table
            .apply(&batch(&[(1, "a", "i"), (2, "b", "i")]), true)
            .unwrap();
        table.apply(&batch(&[(1, "a2", "u")]), true).unwrap();
        table
            .apply(&batch(&[(1, "a3", "u"), (2, "b2", "u")]), true)
            .unwrap();
        assert_eq!(
            table.state.read().chunks.len(),
            1,
            "3 dead rows outnumber 2 live ones"
        );
        assert_eq!(
            lookup_names(&table, &[Some(2), Some(1)]),
            [Some("b2".to_string()), Some("a3".to_string())]
        );
    }

    #[test]
    fn lookup_casts_keys_to_the_primary_key_type() {
        let table = table();
        table.apply(&batch(&[(7, "seven", "i")]), true).unwrap();
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
        let err =
            InMemoryTable::try_new(TABLE, &schema, &["id".to_string()], METRIC_KEY.to_string())
                .err()
                .expect("a schema without _gs_op must be rejected")
                .to_string();
        assert!(err.contains("must contain the '_gs_op' column"), "{err}");
    }
}
