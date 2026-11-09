//! The `table` transform: the latest row per primary key, maintained from a
//! changelog stream. The table is applied by its upstream's side output.
//!
//! A table's rows live in a [`TableBackend`]. The host ([`TableStore`])
//! parses the changelog, runs the window and maintains the table's
//! aggregates; the backend stores keyed rows and reads them as of a mark.

pub mod aggregate;
mod apply;
pub mod backend;
#[cfg(test)]
mod conformance;
pub mod driver;
mod horizon;
pub mod in_memory;
pub mod plugin;
pub mod read;
pub mod side_output;
mod watermark;
mod window;

pub use apply::TableStore;
use apply::fingerprint;
pub use backend::Mark;
use backend::TableBackend;
pub use window::{WALL_CLOCK_COLUMN, WindowConfig};

use crate::error::{Result, ResultExt};
use crate::retry::retry_if_retriable_until_cancelled;
use crate::{streamling_bail, streamling_err, streamling_user_bail, streamling_user_err};
use arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions, StructArray, UInt32Array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::{cast, take, take_record_batch};
use arrow::datatypes::{Field, FieldRef, Fields, Schema, SchemaRef};
use futures::FutureExt;
use futures::future::BoxFuture;
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use streamling_common::utils::parse_primary_key_columns;
use tokio::runtime::{Handle, RuntimeFlavor};

/// Which version lookups read.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VersionRetention {
    /// As of the reader's mark, so a node downstream of the node whose side
    /// output applies the table reads it as of its own batch.
    #[default]
    Journal,
    /// The latest commit, whatever the reader's mark.
    Latest,
}

/// What a store that kept a table across a restart does when the table's
/// columns or primary key changed.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnSchemaChange {
    /// Startup fails.
    #[default]
    Fail,
    /// The stored table and its aggregates are dropped, and the table starts
    /// empty.
    Reset,
}

/// When a table's journal is trimmed.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JournalTrim {
    /// Below each finalized checkpoint's horizon.
    #[default]
    OnFinalize,
    /// Never: every version stays readable.
    Never,
}

/// The mark of each table an apply changed, the table and its aggregates'
/// outputs, by name.
pub type Marks = Vec<(String, Mark)>;

/// Reads of one table, or of one aggregate's output: a namespace of a
/// [`TableBackend`].
pub struct Table {
    name: String,
    namespace: String,
    backend: Arc<dyn TableBackend>,
    /// The namespace's schema.
    schema: SchemaRef,
    /// The primary key columns, in order.
    key_fields: Fields,
    primary_key: String,
    /// The columns lookups read, by position in `schema`.
    readable: Vec<usize>,
    retention: VersionRetention,
}

impl Table {
    /// `key` names columns of `schema`, which lookups read but for
    /// `hidden`.
    fn try_new(
        name: &str,
        namespace: &str,
        backend: Arc<dyn TableBackend>,
        schema: SchemaRef,
        key: &[String],
        hidden: Option<&str>,
        retention: VersionRetention,
    ) -> Result<Self> {
        let key_fields = key
            .iter()
            .map(|column| Ok(Arc::new(schema.field_with_name(column)?.clone())))
            .collect::<Result<Fields>>()?;
        let readable = (0..schema.fields().len())
            .filter(|&index| Some(schema.field(index).name().as_str()) != hidden)
            .collect();
        Ok(Self {
            name: name.to_string(),
            namespace: namespace.to_string(),
            backend,
            schema,
            key_fields,
            primary_key: key.join(","),
            readable,
            retention,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn retention(&self) -> VersionRetention {
        self.retention
    }

    /// The primary key's fields, if `key` names the table's whole primary
    /// key, comma-separated like the table's `primary_key`.
    pub fn lookup_key_fields(&self, key: &str) -> Result<&Fields> {
        if !parse_primary_key_columns(key)
            .into_iter()
            .eq(self.key_fields.iter().map(|field| field.name().as_str()))
        {
            streamling_user_bail!(
                "table '{}': lookups must use the primary key '{}', got '{}'",
                self.name,
                self.primary_key,
                key
            );
        }
        Ok(&self.key_fields)
    }

    /// The field [`Self::lookup`] returns for `column`: nullable, because a
    /// key may have no row.
    pub fn lookup_field(&self, column: &str) -> Result<FieldRef> {
        let field = self.schema.field(self.column_index(column)?);
        Ok(Arc::new(field.clone().with_nullable(true)))
    }

    /// The field [`Self::lookup_rows`] returns: a nullable struct of every
    /// column [`Self::lookup`] can read.
    pub fn lookup_row_field(&self) -> FieldRef {
        Arc::new(Field::new_struct(&self.name, self.readable_fields(), true))
    }

    /// For each row of `keys`, one array per primary key column, `column` of
    /// the stored row with that primary key, or null when no row has it, as
    /// of `mark`, or the latest apply without one.
    pub fn lookup(&self, keys: &[ArrayRef], column: &str, mark: Option<Mark>) -> Result<ArrayRef> {
        let (rows, _) = self.read(keys, &[self.column_index(column)?], mark)?;
        Ok(Arc::clone(rows.column(0)))
    }

    /// Like [`Self::lookup`], for every column [`Self::lookup`] can read at
    /// once, as a struct that is null when no row has the key.
    pub fn lookup_rows(&self, keys: &[ArrayRef], mark: Option<Mark>) -> Result<StructArray> {
        let (rows, found) = self.read(keys, &self.readable, mark)?;
        StructArray::try_new(
            self.readable_fields(),
            rows.columns().to_vec(),
            Some(NullBuffer::from(found.values().clone())),
        )
        .streamling_context("failed to read the looked-up rows")
    }

    /// For each row of `keys`, whether the table has a row with that primary
    /// key, as of `mark`, or the latest apply without one.
    pub fn lookup_exists(&self, keys: &[ArrayRef], mark: Option<Mark>) -> Result<BooleanArray> {
        Ok(self.read(keys, &[], mark)?.1)
    }

    /// The `projection` columns of each key's row, and whether it has one.
    ///
    /// The keys are cast to the key's types, a safe cast: a value that can't
    /// be converted becomes null. A key with a null in any column misses
    /// without reaching the backend, which never sees null keys.
    fn read(
        &self,
        keys: &[ArrayRef],
        projection: &[usize],
        mark: Option<Mark>,
    ) -> Result<(RecordBatch, BooleanArray)> {
        if keys.len() != self.key_fields.len() {
            streamling_bail!(
                "table '{}': the primary key '{}' has {} columns, got {} lookup key columns",
                self.name,
                self.primary_key,
                self.key_fields.len(),
                keys.len()
            );
        }
        let rows = keys.first().map_or(0, |keys| keys.len());
        let keys = keys
            .iter()
            .zip(self.key_fields.iter())
            .map(|(keys, field)| {
                cast(keys, field.data_type())
                    .streamling_context("failed to cast the lookup keys to the primary key type")
            })
            .collect::<Result<Vec<_>>>()?;
        // A value that can't be cast is null.
        let key_fields: Vec<_> = self
            .key_fields
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect();
        let keys = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(key_fields)),
            keys,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        let valid = keys.columns().iter().fold(None, |valid, keys| {
            NullBuffer::union(valid.as_ref(), keys.logical_nulls().as_ref())
        });
        let Some(valid) = valid.filter(|valid| valid.null_count() > 0) else {
            let found = self.backend_read(&keys, projection, mark)?;
            return Ok((found.rows, found.found));
        };
        // Each key's position among the valid ones, null for the others.
        let positions: UInt32Array = valid
            .iter()
            .scan(0u32, |next, is_valid| {
                Some(is_valid.then(|| {
                    *next += 1;
                    *next - 1
                }))
            })
            .collect();
        let valid_rows: UInt32Array = valid.valid_indices().map(|index| index as u32).collect();
        let found = self.backend_read(&take_record_batch(&keys, &valid_rows)?, projection, mark)?;
        let rows = RecordBatch::try_new_with_options(
            found.rows.schema(),
            found
                .rows
                .columns()
                .iter()
                .map(|column| take(column, &positions, None))
                .collect::<std::result::Result<_, _>>()?,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        let found = take(&found.found, &positions, None)?;
        let found = found
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("take keeps the type");
        Ok((
            rows,
            found
                .iter()
                .map(|found| Some(found == Some(true)))
                .collect(),
        ))
    }

    fn backend_read(
        &self,
        keys: &RecordBatch,
        projection: &[usize],
        mark: Option<Mark>,
    ) -> Result<backend::LookupRows> {
        bridge(&format!("table '{}' lookup", self.name), || {
            self.backend.lookup(&self.namespace, keys, projection, mark)
        })
    }

    fn readable_fields(&self) -> Fields {
        self.readable
            .iter()
            .map(|&index| Arc::clone(&self.schema.fields()[index]))
            .collect()
    }

    /// The position of a column lookups read.
    fn column_index(&self, column: &str) -> Result<usize> {
        self.readable
            .iter()
            .copied()
            .find(|&index| self.schema.field(index).name() == column)
            .ok_or_else(|| streamling_user_err!("table '{}' has no column '{}'", self.name, column))
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("namespace", &self.namespace)
            .finish()
    }
}

/// Runs a backend read from synchronous code: lookups run inside scalar UDFs.
/// A read that completes on its first poll, as `in_memory` reads do, never
/// blocks. Otherwise the thread blocks on it, which needs a multi-threaded
/// runtime. Retriable errors are retried with backoff.
fn bridge<'a, T>(
    operation_name: &str,
    mut operation: impl FnMut() -> BoxFuture<'a, Result<T>>,
) -> Result<T> {
    let mut first = operation();
    let failed = match (&mut first).now_or_never() {
        None => None,
        Some(Err(err)) if err.is_retriable() => Some(err),
        Some(result) => return result,
    };
    let handle = Handle::try_current()
        .map_err(|_| streamling_err!("{}: table reads need a Tokio runtime", operation_name))?;
    if handle.runtime_flavor() == RuntimeFlavor::CurrentThread {
        streamling_bail!(
            "{}: a table backend that reads asynchronously needs a multi-threaded Tokio runtime",
            operation_name
        );
    }
    tokio::task::block_in_place(|| {
        handle.block_on(async {
            let result = match failed {
                Some(err) => Err(err),
                None => first.await,
            };
            match result {
                Err(err) if err.is_retriable() => {
                    retry_if_retriable_until_cancelled(
                        operation,
                        operation_name,
                        &mut crate::shutdown::subscribe(),
                    )
                    .await
                }
                result => result,
            }
        })
    })
}

/// The pipeline's tables by name, and the stores that apply them.
#[derive(Clone, Default)]
pub struct TableRegistry(Arc<RwLock<Registry>>);

#[derive(Default)]
struct Registry {
    tables: HashMap<String, Arc<Table>>,
    stores: HashMap<String, Arc<TableStore>>,
}

impl TableRegistry {
    pub fn register(&self, table: Arc<Table>) {
        self.0.write().tables.insert(table.name.clone(), table);
    }

    /// Registers `store` and its table.
    pub fn register_store(&self, store: Arc<TableStore>) {
        let mut registry = self.0.write();
        let table = store.table();
        registry.tables.insert(table.name.clone(), table);
        registry.stores.insert(store.name().to_string(), store);
    }

    pub fn get(&self, name: &str) -> Option<Arc<Table>> {
        self.0.read().tables.get(name).cloned()
    }

    pub fn store(&self, name: &str) -> Option<Arc<TableStore>> {
        self.0.read().stores.get(name).cloned()
    }
}

impl fmt::Debug for TableRegistry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_set().entries(self.0.read().tables.keys()).finish()
    }
}

/// Tables on `in_memory` backends, for tests.
#[cfg(test)]
pub(crate) mod testing {
    use super::in_memory::InMemoryBackend;
    use super::*;

    pub(crate) const MAX_DEAD_RATIO: f64 = 0.25;

    /// Table `name` on a new `in_memory` backend, its journal trimmed on
    /// finalization.
    pub(crate) fn store(
        name: &str,
        input_schema: &SchemaRef,
        primary_key: &[&str],
        window: Option<WindowConfig>,
        retention: VersionRetention,
    ) -> Result<TableStore> {
        store_trimmed(
            name,
            input_schema,
            primary_key,
            window,
            retention,
            JournalTrim::OnFinalize,
        )
    }

    pub(crate) fn store_trimmed(
        name: &str,
        input_schema: &SchemaRef,
        primary_key: &[&str],
        window: Option<WindowConfig>,
        retention: VersionRetention,
        journal_trim: JournalTrim,
    ) -> Result<TableStore> {
        let backend = Arc::new(InMemoryBackend::try_new(name, MAX_DEAD_RATIO)?);
        store_on(
            name,
            input_schema,
            primary_key,
            window,
            retention,
            journal_trim,
            OnSchemaChange::Fail,
            backend,
        )
    }

    /// Table `name` on `backend`, which may hold what another store wrote,
    /// as after a restart.
    pub(crate) fn store_on(
        name: &str,
        input_schema: &SchemaRef,
        primary_key: &[&str],
        window: Option<WindowConfig>,
        retention: VersionRetention,
        journal_trim: JournalTrim,
        on_schema_change: OnSchemaChange,
        backend: Arc<dyn TableBackend>,
    ) -> Result<TableStore> {
        let primary_key: Vec<String> = primary_key
            .iter()
            .map(|column| column.to_string())
            .collect();
        TableStore::try_new(
            name,
            input_schema,
            &primary_key,
            format!("app::{name}"),
            window,
            retention,
            journal_trim,
            on_schema_change,
            backend,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::backend::{
        LookupRows, NamespaceSpec, NamespaceStats, StoredState, TableTransaction,
    };
    use super::in_memory::InMemoryBackend;
    use super::testing::{MAX_DEAD_RATIO, store, store_on, store_trimmed};
    use super::*;
    use crate::checkpoints::checkpoint_management::CheckpointEpoch;
    use crate::data::COLUMN_NAME_OP;
    use arrow::array::{
        AsArray, Int32Array, Int64Array, LargeBinaryArray, StringArray, StringViewArray,
        TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Int64Type, TimeUnit};
    use async_trait::async_trait;
    use datafusion::prelude::SessionContext;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use streamling_common::types::decimal_arb::DecimalArbType;
    use window::WALL_CLOCK_TIME_ZONE;

    const TABLE: &str = "tokens";

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

    async fn table_with(retention: VersionRetention) -> TableStore {
        let store = store(TABLE, &schema(), &["id"], None, retention).unwrap();
        store.start().await.unwrap();
        store
    }

    async fn table() -> TableStore {
        table_with(VersionRetention::Journal).await
    }

    /// Applies `batch`, which carried the markers of `epochs`, and returns
    /// the mark its readers see the table at.
    async fn apply_marked(store: &TableStore, batch: &RecordBatch, epochs: &[u64]) -> Mark {
        let epochs: Vec<CheckpointEpoch> = epochs.iter().copied().map(CheckpointEpoch).collect();
        table_mark(store.apply(batch, &epochs).await.unwrap())
    }

    async fn apply(store: &TableStore, batch: &RecordBatch) -> Mark {
        apply_marked(store, batch, &[]).await
    }

    fn table_mark(marks: Marks) -> Mark {
        marks
            .into_iter()
            .find(|(table, _)| table == TABLE)
            .map(|(_, mark)| mark)
            .expect("the table's mark")
    }

    fn lookup_names_at(
        store: &TableStore,
        ids: &[i64],
        mark: Option<Mark>,
    ) -> Result<Vec<Option<String>>> {
        let keys: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        Ok(store
            .table()
            .lookup(&[keys], "name", mark)?
            .as_string::<i32>()
            .iter()
            .map(|name| name.map(str::to_string))
            .collect())
    }

    fn lookup_names(store: &TableStore, ids: &[Option<i64>]) -> Vec<Option<String>> {
        let keys: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        store
            .table()
            .lookup(&[keys], "name", None)
            .unwrap()
            .as_string::<i32>()
            .iter()
            .map(|name| name.map(str::to_string))
            .collect()
    }

    fn names(values: &[Option<&str>]) -> Vec<Option<String>> {
        values.iter().map(|name| name.map(str::to_string)).collect()
    }

    #[tokio::test]
    async fn upserts_replace_and_deletes_remove_by_primary_key() {
        let table = table().await;
        apply(
            &table,
            &batch(&[(1, "a", "i"), (2, "b", "c"), (3, "c", "r")]),
        )
        .await;
        assert_eq!(
            lookup_names(&table, &[Some(1), Some(2), Some(3)]),
            names(&[Some("a"), Some("b"), Some("c")])
        );
        apply(
            &table,
            &batch(&[(1, "a2", "u"), (2, "b2", "C"), (3, "c2", "i")]),
        )
        .await;
        assert_eq!(
            lookup_names(&table, &[Some(1), Some(2), Some(3)]),
            names(&[Some("a2"), Some("b2"), Some("c2")])
        );
        apply(&table, &batch(&[(1, "ignored", "d"), (2, "ignored", "D")])).await;
        assert_eq!(
            lookup_names(&table, &[Some(1), Some(2), Some(3)]),
            names(&[None, None, Some("c2")])
        );
        assert_eq!(table.table_stats().await.live_rows, 1);
    }

    #[tokio::test]
    async fn delete_of_an_unknown_key_changes_nothing() {
        let table = table().await;
        apply(&table, &batch(&[(9, "incoming", "d")])).await;
        assert_eq!(lookup_names(&table, &[Some(9)]), [None]);
        assert_eq!(table.table_stats().await.live_rows, 0);
    }

    #[tokio::test]
    async fn later_rows_of_a_batch_win() {
        let table = table().await;
        apply(
            &table,
            &batch(&[(1, "a", "i"), (1, "b", "u"), (2, "c", "i"), (2, "c", "d")]),
        )
        .await;
        assert_eq!(
            lookup_names(&table, &[Some(1), Some(2)]),
            names(&[Some("b"), None])
        );
        assert_eq!(table.table_stats().await.live_rows, 1);
    }

    #[tokio::test]
    async fn composite_primary_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("chain", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        let table = store(
            TABLE,
            &schema,
            &["chain", "address"],
            None,
            VersionRetention::Journal,
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

        apply(&table, &input(&[("eth", "0x1", "i"), ("sol", "0x1", "i")])).await;
        assert_eq!(table.table_stats().await.live_rows, 2);
        apply(&table, &input(&[("eth", "0x1", "u"), ("eth", "0x2", "i")])).await;
        assert_eq!(table.table_stats().await.live_rows, 3);
    }

    #[tokio::test]
    async fn field_metadata_survives_into_storage() {
        let amount = DecimalArbType::field("amount", 78, 0, true).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            amount.clone(),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        let table = store(TABLE, &schema, &["id"], None, VersionRetention::Journal).unwrap();
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(LargeBinaryArray::from_iter_values([b"\x01".as_slice()])),
                Arc::new(StringArray::from(vec!["i"])),
            ],
        )
        .unwrap();
        apply(&table, &input).await;
        assert_eq!(
            table
                .stored_schema()
                .field_with_name("amount")
                .unwrap()
                .metadata(),
            amount.metadata()
        );
        assert_eq!(
            table.table().lookup_field("amount").unwrap().metadata(),
            amount.metadata()
        );
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let found = table.table().lookup(&[keys], "amount", None).unwrap();
        assert_eq!(found.as_binary::<i64>().value(0), b"\x01");
    }

    #[tokio::test]
    async fn null_primary_key_is_rejected() {
        let input = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["i"])),
            ],
        )
        .unwrap();
        let err = table()
            .await
            .apply(&input, &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("primary key column 'id' contains a null value"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn unrecognized_or_null_op_is_rejected() {
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
            let err = table()
                .await
                .apply(&input, &[])
                .await
                .unwrap_err()
                .to_string();
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
        let err = table()
            .await
            .apply(&input, &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("column '_gs_op' must be Utf8"), "{err}");
    }

    #[tokio::test]
    async fn batch_with_a_different_schema_is_rejected() {
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
        let err = table()
            .await
            .apply(&input, &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not match the table schema"), "{err}");
    }

    #[tokio::test]
    async fn lookup_reads_the_live_row_per_key() {
        let table = table().await;
        assert_eq!(lookup_names(&table, &[Some(1)]), [None], "empty table");
        apply(
            &table,
            &batch(&[(1, "a", "i"), (2, "b", "i"), (3, "c", "i")]),
        )
        .await;
        apply(&table, &batch(&[(1, "a2", "u"), (2, "x", "d")])).await;
        assert_eq!(
            lookup_names(&table, &[Some(1), Some(2), Some(3), Some(9), None, Some(1)]),
            names(&[Some("a2"), None, Some("c"), None, None, Some("a2")])
        );
    }

    #[tokio::test]
    async fn lookup_casts_keys_to_the_primary_key_type() {
        let table = table().await;
        apply(&table, &batch(&[(7, "seven", "i")])).await;
        let narrower: ArrayRef = Arc::new(Int32Array::from(vec![7]));
        let text: ArrayRef = Arc::new(StringViewArray::from(vec!["7"]));
        for keys in [narrower, text] {
            let names = table
                .table()
                .lookup(&[Arc::clone(&keys)], "name", None)
                .unwrap();
            assert_eq!(
                names.as_string::<i32>().value(0),
                "seven",
                "{}",
                keys.data_type()
            );
        }
        let uncastable: ArrayRef = Arc::new(StringViewArray::from(vec!["seven"]));
        assert!(
            !table
                .table()
                .lookup_exists(&[uncastable], None)
                .unwrap()
                .value(0),
            "a value that can't be cast misses"
        );
    }

    #[tokio::test]
    async fn lookups_must_use_the_whole_primary_key() {
        let table = table().await;
        let reads = table.table();
        assert_eq!(
            reads.lookup_key_fields(" id ").unwrap().as_ref(),
            schema().fields()[..1].as_ref(),
            "key names are trimmed like the table's primary_key"
        );
        let err = reads.lookup_key_fields("name").unwrap_err().to_string();
        assert!(
            err.contains("lookups must use the primary key 'id', got 'name'"),
            "{err}"
        );
        for column in ["missing", COLUMN_NAME_OP] {
            let err = reads.lookup_field(column).unwrap_err().to_string();
            assert!(
                err.contains(&format!("table 'tokens' has no column '{column}'")),
                "{err}"
            );
        }
        assert!(reads.lookup_field("name").unwrap().is_nullable());

        let composite = balances().await.table();
        assert_eq!(
            composite.lookup_key_fields("chain, address").unwrap().len(),
            2
        );
        for key in ["chain", "address,chain", "chain,address,amount"] {
            let err = composite.lookup_key_fields(key).unwrap_err().to_string();
            assert!(
                err.contains(&format!(
                    "lookups must use the primary key 'chain,address', got '{key}'"
                )),
                "{err}"
            );
        }
    }

    fn balances_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("chain", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false),
        ]))
    }

    /// A table keyed by `(chain, address)` holding `(eth, 0x1) → 10` and
    /// `(sol, 0x1) → 20`.
    async fn balances() -> TableStore {
        let table = store(
            TABLE,
            &balances_schema(),
            &["chain", "address"],
            None,
            VersionRetention::Journal,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            balances_schema(),
            vec![
                Arc::new(StringArray::from(vec!["eth", "sol"])),
                Arc::new(StringArray::from(vec!["0x1", "0x1"])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["i", "i"])),
            ],
        )
        .unwrap();
        apply(&table, &batch).await;
        table
    }

    #[tokio::test]
    async fn composite_key_lookups_match_every_key_column() {
        let table = balances().await.table();
        let chains: ArrayRef = Arc::new(StringArray::from(vec![
            Some("eth"),
            Some("sol"),
            Some("eth"),
            None,
            Some("eth"),
        ]));
        let addresses: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("0x1"),
            Some("0x1"),
            Some("0x2"),
            Some("0x1"),
            None,
        ]));
        let keys = [chains, addresses];
        let amounts = table.lookup(&keys, "amount", None).unwrap();
        assert_eq!(
            amounts
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>(),
            [Some(10), Some(20), None, None, None],
            "a NULL in any key column misses"
        );
        assert_eq!(
            table
                .lookup_exists(&keys, None)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            [
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false)
            ]
        );
        let rows = table.lookup_rows(&keys, None).unwrap();
        assert_eq!(
            (0..rows.len())
                .map(|row| rows.is_valid(row))
                .collect::<Vec<_>>(),
            [true, true, false, false, false]
        );

        let err = table
            .lookup(&keys[..1], "amount", None)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("has 2 columns, got 1 lookup key columns"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn row_lookups_read_every_column_once_per_key() {
        let table = balances().await.table();
        let keys: [ArrayRef; 2] = [
            Arc::new(StringArray::from(vec!["sol", "eth"])),
            Arc::new(StringArray::from(vec!["0x1", "0x2"])),
        ];
        let rows = table.lookup_rows(&keys, None).unwrap();
        let DataType::Struct(fields) = table.lookup_row_field().data_type().clone() else {
            panic!("a struct");
        };
        assert_eq!(rows.fields(), &fields);
        assert_eq!(
            fields
                .iter()
                .map(|field| (field.name().as_str(), field.data_type().clone()))
                .collect::<Vec<_>>(),
            [
                ("chain", DataType::Utf8),
                ("address", DataType::Utf8),
                ("amount", DataType::Int64)
            ]
        );
        assert!(table.lookup_row_field().is_nullable());
        assert!(rows.is_valid(0) && rows.is_null(1));
        assert_eq!(
            rows.column_by_name("amount")
                .unwrap()
                .as_primitive::<Int64Type>()
                .value(0),
            20
        );
    }

    #[test]
    fn sql_output_without_op_column_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let err = store(TABLE, &schema, &["id"], None, VersionRetention::Journal)
            .expect_err("a schema without _gs_op must be rejected")
            .to_string();
        assert!(err.contains("must contain the '_gs_op' column"), "{err}");
    }

    #[test]
    fn primary_key_naming_the_op_column_is_rejected() {
        let err = store(
            TABLE,
            &schema(),
            &[COLUMN_NAME_OP],
            None,
            VersionRetention::Journal,
        )
        .expect_err("a primary key on _gs_op must be rejected")
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

    fn windowed(input_schema: &SchemaRef, predicate: &str) -> Result<TableStore> {
        windowed_with(input_schema, predicate, VersionRetention::Journal)
    }

    fn windowed_with(
        input_schema: &SchemaRef,
        predicate: &str,
        retention: VersionRetention,
    ) -> Result<TableStore> {
        store(
            TABLE,
            input_schema,
            &["id"],
            Some(WindowConfig {
                predicate,
                session_state: &SessionContext::new().state(),
                gc_interval: GC_INTERVAL,
            }),
            retention,
        )
    }

    /// Runs a GC pass in an apply of nothing.
    async fn evict_outside(table: &TableStore, input_schema: &SchemaRef) -> Mark {
        table.make_gc_pass_due().await;
        apply(table, &RecordBatch::new_empty(Arc::clone(input_schema))).await
    }

    fn now_nanos() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }

    #[tokio::test]
    async fn window_evicts_the_live_rows_it_is_not_true_for() {
        let table = windowed(&timed_schema(), KEEP_LAST_HOUR).unwrap();
        assert!(
            table
                .stored_schema()
                .field_with_name(WALL_CLOCK_COLUMN)
                .is_err(),
            "a window that doesn't read the wall clock doesn't store it"
        );
        let now = now_nanos();
        let old = now - 2 * HOUR_NANOS;
        apply(
            &table,
            &timed_batch(&[
                (1, "old", Some(old)),
                (2, "new", Some(now)),
                (3, "null", None),
                (4, "old", Some(old)),
                (5, "new", Some(now)),
            ]),
        )
        .await;
        // Key 4's live row is now inside the window, key 5's outside it.
        apply(
            &table,
            &timed_batch(&[(4, "new", Some(now)), (5, "old", Some(old))]),
        )
        .await;

        evict_outside(&table, &timed_schema()).await;

        assert_eq!(
            lookup_names(&table, &[Some(1), Some(2), Some(3), Some(4), Some(5)]),
            names(&[None, Some("new"), None, Some("new"), None])
        );
        assert_eq!(table.table_stats().await.live_rows, 2);
    }

    #[tokio::test]
    async fn wall_clock_window_reads_the_apply_time() {
        let keep_last_hour = format!("{WALL_CLOCK_COLUMN} >= now() - interval '1 hour'");
        let keep_older = format!("{WALL_CLOCK_COLUMN} < now() - interval '1 hour'");
        for (predicate, expected) in [(keep_last_hour, Some("a")), (keep_older, None)] {
            let table = windowed(&schema(), &predicate).unwrap();
            apply(&table, &batch(&[(1, "a", "i")])).await;
            evict_outside(&table, &schema()).await;
            assert_eq!(
                lookup_names(&table, &[Some(1)]),
                names(&[expected]),
                "{predicate}"
            );
        }
    }

    #[tokio::test]
    async fn apply_runs_a_gc_pass_once_it_is_due() {
        let table = windowed(&timed_schema(), KEEP_LAST_HOUR).unwrap();
        let old = now_nanos() - 2 * HOUR_NANOS;
        apply(&table, &timed_batch(&[(1, "old", Some(old))])).await;
        apply(&table, &timed_batch(&[])).await;
        assert_eq!(lookup_names(&table, &[Some(1)]), names(&[Some("old")]));

        evict_outside(&table, &timed_schema()).await;
        assert_eq!(lookup_names(&table, &[Some(1)]), [None]);
    }

    /// The README's windows that skip stored rows wholly inside them, and
    /// two that don't.
    #[test]
    fn range_windows_compare_a_column_or_a_cast_of_one() {
        let epoch_seconds = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("block_timestamp", DataType::Int64, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
        ]));
        let wall_clock = format!("{WALL_CLOCK_COLUMN} >= now() - interval '1 hour'");
        for (input_schema, predicate, range_column) in [
            (timed_schema(), KEEP_LAST_HOUR, Some("ts")),
            (timed_schema(), KEEP_LAST_HOUR_OF_DATA, Some("ts")),
            (
                timed_schema(),
                "ts >= least(watermark(ts), now()) - interval '1 hour'",
                Some("ts"),
            ),
            (schema(), wall_clock.as_str(), Some(WALL_CLOCK_COLUMN)),
            (
                Arc::clone(&epoch_seconds),
                "block_timestamp >= watermark(block_timestamp) - 3600",
                Some("block_timestamp"),
            ),
            (
                Arc::clone(&epoch_seconds),
                "block_timestamp >= extract(epoch from now() - interval '1 hour')",
                Some("block_timestamp"),
            ),
            (
                timed_schema(),
                "date_trunc('second', ts) >= now() - interval '1 hour'",
                None,
            ),
            (
                Arc::clone(&epoch_seconds),
                "block_timestamp >= 0 AND id > 0",
                None,
            ),
        ] {
            let table = windowed(&input_schema, predicate).unwrap();
            assert_eq!(table.range_column(), range_column, "{predicate}");
        }
    }

    /// An integer column compared as a float evicts exactly the rows the
    /// comparison is false for, whichever way the bound rounds.
    #[tokio::test]
    async fn range_windows_over_a_cast_keep_the_boundary_rows_the_predicate_keeps() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
        ]));
        let input = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(Int64Array::from(vec![7, 8, 9, 10])),
                Arc::new(StringArray::from(vec!["7", "8", "9", "10"])),
                Arc::new(Int64Array::from(vec![7, 8, 9, 10])),
                Arc::new(StringArray::from(vec!["i"; 4])),
            ],
        )
        .unwrap();
        for (range, unprunable) in [
            (
                "CAST(n AS DOUBLE) >= watermark(n) - 2.5",
                "CAST(n AS DOUBLE) + 0 >= watermark(n) - 2.5",
            ),
            (
                "CAST(n AS DOUBLE) > watermark(n) - 2.5",
                "CAST(n AS DOUBLE) + 0 > watermark(n) - 2.5",
            ),
            (
                "CAST(n AS DOUBLE) >= watermark(n) - 2.0",
                "CAST(n AS DOUBLE) + 0 >= watermark(n) - 2.0",
            ),
            (
                "CAST(n AS DOUBLE) > watermark(n) - 2.0",
                "CAST(n AS DOUBLE) + 0 > watermark(n) - 2.0",
            ),
        ] {
            let mut kept = Vec::new();
            for predicate in [range, unprunable] {
                let table = windowed(&input_schema, predicate).unwrap();
                assert_eq!(
                    table.range_column(),
                    (predicate == range).then_some("n"),
                    "{predicate}"
                );
                apply(&table, &input).await;
                kept.push(lookup_names(&table, &[Some(7), Some(8), Some(9), Some(10)]));
            }
            assert_eq!(kept[0], kept[1], "{range}");
        }
    }

    #[tokio::test]
    async fn lookups_cannot_read_the_window_wall_clock() {
        let windowed = windowed(
            &schema(),
            &format!("{WALL_CLOCK_COLUMN} >= now() - interval '1 hour'"),
        )
        .unwrap()
        .table();
        let err = windowed
            .lookup_field(WALL_CLOCK_COLUMN)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("table 'tokens' has no column '__timestamp'"),
            "{err}"
        );
        let DataType::Struct(row_fields) = windowed.lookup_row_field().data_type().clone() else {
            panic!("a struct");
        };
        assert_eq!(
            row_fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "name"]
        );

        let with_wall_clock = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(WALL_CLOCK_COLUMN, DataType::Int64, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
        ]));
        let unwindowed = store(
            TABLE,
            &with_wall_clock,
            &["id"],
            None,
            VersionRetention::Journal,
        )
        .unwrap()
        .table();
        assert!(
            unwindowed.lookup_field(WALL_CLOCK_COLUMN).is_ok(),
            "without a window, '__timestamp' is an ordinary column"
        );
    }

    const KEEP_LAST_HOUR_OF_DATA: &str = "ts >= watermark(ts) - interval '1 hour'";
    const MINUTE_NANOS: i64 = 60_000_000_000;

    /// The README's walkthrough: every apply that writes rows runs a GC pass,
    /// whatever the GC interval.
    #[tokio::test]
    async fn watermark_window_keeps_the_hour_before_the_largest_stored_time() {
        let table = windowed(&timed_schema(), KEEP_LAST_HOUR_OF_DATA).unwrap();
        let at = |hour: i64, minute: i64| Some(hour * HOUR_NANOS + minute * MINUTE_NANOS);
        let ids = [Some(1), Some(2), Some(3), Some(4), Some(5)];

        apply(
            &table,
            &timed_batch(&[(1, "a", at(10, 0)), (2, "b", at(10, 20))]),
        )
        .await;
        assert_eq!(
            lookup_names(&table, &ids),
            names(&[Some("a"), Some("b"), None, None, None])
        );

        apply(&table, &timed_batch(&[(3, "c", at(11, 10))])).await;
        assert_eq!(
            lookup_names(&table, &ids),
            names(&[None, Some("b"), Some("c"), None, None]),
            "the watermark moved to 11:10, so 10:00 is outside"
        );

        apply(&table, &timed_batch(&[(4, "late", at(9, 30))])).await;
        apply(&table, &timed_batch(&[(2, "b", at(10, 5))])).await;
        assert_eq!(
            lookup_names(&table, &ids),
            names(&[None, None, Some("c"), None, None]),
            "a late row and an update to before 10:10 are evicted in their own apply"
        );

        apply(&table, &timed_batch_of(&[(3, "c", at(11, 10))], "d")).await;
        apply(&table, &timed_batch(&[(5, "e", at(10, 0))])).await;
        assert_eq!(
            lookup_names(&table, &ids),
            names(&[None, None, None, None, None]),
            "deleting the latest row doesn't move the watermark back"
        );
    }

    #[tokio::test]
    async fn watermark_reads_integer_columns() {
        const HOUR_SECS: i64 = 3600;
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("block_timestamp", DataType::Int64, true),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, true),
        ]));
        let table = windowed(
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
        apply(&table, &input).await;
        assert_eq!(
            lookup_names(&table, &[Some(1), Some(2)]),
            names(&[None, Some("new")])
        );
    }

    #[tokio::test]
    async fn least_with_now_guards_against_future_timestamps() {
        const DECADE_NANOS: i64 = 10 * 365 * 24 * HOUR_NANOS;
        let guarded = "ts >= least(watermark(ts), now()) - interval '1 hour'";
        for (predicate, expected) in [(KEEP_LAST_HOUR_OF_DATA, None), (guarded, Some("now"))] {
            let table = windowed(&timed_schema(), predicate).unwrap();
            let now = now_nanos();
            apply(
                &table,
                &timed_batch(&[
                    (1, "now", Some(now)),
                    (2, "future", Some(now + DECADE_NANOS)),
                ]),
            )
            .await;
            assert_eq!(
                lookup_names(&table, &[Some(1)]),
                names(&[expected]),
                "{predicate}"
            );
        }
    }

    #[test]
    fn watermark_takes_one_column() {
        for predicate in [
            "ts >= watermark(date_trunc('hour', ts))",
            "ts >= watermark()",
            "ts >= watermark(ts, ts)",
        ] {
            let err = windowed(&timed_schema(), predicate)
                .expect_err("the window must be rejected")
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
            let err = windowed(&input_schema, predicate)
                .expect_err("the window must be rejected")
                .to_string();
            assert!(err.contains(expected), "{predicate}: {err}");
        }
    }

    #[tokio::test]
    async fn reads_at_a_mark_do_not_see_later_applies() {
        let table = table().await;
        let first = apply(
            &table,
            &batch(&[(1, "a", "i"), (2, "b", "i"), (3, "c", "i")]),
        )
        .await;
        let second = apply(&table, &batch(&[(1, "a2", "u"), (2, "b2", "u")])).await;
        let third = apply(&table, &batch(&[(1, "x", "d"), (3, "c3", "u")])).await;

        for (mark, expected) in [
            (Some(first), [Some("a"), Some("b"), Some("c")]),
            (Some(second), [Some("a2"), Some("b2"), Some("c")]),
            (Some(third), [None, Some("b2"), Some("c3")]),
            (None, [None, Some("b2"), Some("c3")]),
        ] {
            assert_eq!(
                lookup_names_at(&table, &[1, 2, 3], mark).unwrap(),
                names(&expected),
                "{mark:?}"
            );
        }
    }

    #[tokio::test]
    async fn row_and_exists_reads_at_a_mark_do_not_see_later_applies() {
        let table = table().await;
        let first = apply(&table, &batch(&[(1, "a", "i"), (2, "b", "i")])).await;
        let second = apply(&table, &batch(&[(1, "x", "d"), (2, "b2", "u")])).await;
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let reads = table.table();

        for (mark, expected) in [
            (Some(first), [Some("a"), Some("b")]),
            (Some(second), [None, Some("b2")]),
            (None, [None, Some("b2")]),
        ] {
            let rows = reads.lookup_rows(&[Arc::clone(&keys)], mark).unwrap();
            let row_names: Vec<Option<String>> = (0..rows.len())
                .map(|row| {
                    rows.is_valid(row).then(|| {
                        rows.column_by_name("name")
                            .unwrap()
                            .as_string::<i32>()
                            .value(row)
                            .to_string()
                    })
                })
                .collect();
            assert_eq!(row_names, names(&expected), "{mark:?}");
            assert_eq!(
                reads
                    .lookup_exists(&[Arc::clone(&keys)], mark)
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected.map(|name| Some(name.is_some())),
                "{mark:?}"
            );
        }
    }

    /// `latest` only changes which version lookups read: applies journal and
    /// return marks either way.
    #[tokio::test]
    async fn latest_tables_still_journal_and_get_marks() {
        let table = table_with(VersionRetention::Latest).await;
        let first = apply_marked(&table, &batch(&[(1, "a", "i")]), &[1]).await;
        let second = apply(&table, &batch(&[(1, "b", "u")])).await;
        assert!(second > first);
        assert_eq!(table.table().retention(), VersionRetention::Latest);
        assert_eq!(
            lookup_names_at(&table, &[1], None).unwrap(),
            names(&[Some("b")])
        );
        assert_eq!(table.table_stats().await.journal_rows, 2);
    }

    #[tokio::test]
    async fn an_empty_batch_without_a_gc_pass_commits_nothing() {
        let table = table().await;
        let mark = apply(&table, &batch(&[(1, "a", "i")])).await;
        assert_eq!(apply(&table, &batch(&[])).await, mark);
        assert_eq!(
            apply_marked(&table, &batch(&[]), &[1]).await,
            mark,
            "a marker on an empty batch is recorded at the last mark"
        );
    }

    fn collected(table: &TableStore, mark: Mark) -> bool {
        match lookup_names_at(table, &[1], Some(mark)) {
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
    async fn table_with_markers(journal_trim: JournalTrim) -> (TableStore, Vec<Mark>) {
        let table = store_trimmed(
            TABLE,
            &schema(),
            &["id"],
            None,
            VersionRetention::Journal,
            journal_trim,
        )
        .unwrap();
        let mut marks = Vec::new();
        for epochs in [&[][..], &[1], &[1], &[], &[2], &[2]] {
            marks.push(apply_marked(&table, &batch(&[(1, "v", "u")]), epochs).await);
        }
        (table, marks)
    }

    /// Epoch 1's horizon is batch 1's mark, the earlier of its two marker
    /// batches, so batch 2's change stays for a reader at batch 1.
    #[tokio::test]
    async fn a_finalized_epoch_collects_up_to_its_earliest_marker_batch() {
        let (table, marks) = table_with_markers(JournalTrim::OnFinalize).await;
        table.finalize_epoch(1).await.unwrap();
        assert_eq!(
            marks
                .iter()
                .map(|mark| collected(&table, *mark))
                .collect::<Vec<_>>(),
            [true, false, false, false, false, false],
            "a read at the horizon is exact"
        );
        assert_eq!(table.table_stats().await.journal_rows, 4);

        // A repeated finalizer changes nothing.
        table.finalize_epoch(1).await.unwrap();
        assert_eq!(table.table_stats().await.journal_rows, 4);
    }

    #[tokio::test]
    async fn a_lookup_of_no_keys_at_a_collected_mark_errors() {
        let (table, marks) = table_with_markers(JournalTrim::OnFinalize).await;
        table.finalize_epoch(1).await.unwrap();
        assert!(collected(&table, marks[0]));
        let err = lookup_names_at(&table, &[], Some(marks[0])).unwrap_err();
        assert!(err.to_string().contains("already collected"), "{err}");
    }

    #[tokio::test]
    async fn a_later_finalized_epoch_covers_a_skipped_one() {
        let (table, marks) = table_with_markers(JournalTrim::OnFinalize).await;
        // Epoch 1's finalizer never came. Epoch 2's horizon is batch 4's mark.
        table.finalize_epoch(2).await.unwrap();
        assert_eq!(
            marks
                .iter()
                .map(|mark| collected(&table, *mark))
                .collect::<Vec<_>>(),
            [true, true, true, true, false, false]
        );
        assert_eq!(table.table_stats().await.journal_rows, 1);
    }

    #[tokio::test]
    async fn journal_trim_never_keeps_every_version() {
        let (table, marks) = table_with_markers(JournalTrim::Never).await;
        table.finalize_epoch(2).await.unwrap();
        assert!(
            marks.iter().all(|mark| !collected(&table, *mark)),
            "no version is collected"
        );
        assert_eq!(table.table_stats().await.journal_rows, 6);
    }

    #[tokio::test]
    async fn interval_window_evictions_are_versioned() {
        let table =
            windowed_with(&timed_schema(), KEEP_LAST_HOUR, VersionRetention::Journal).unwrap();
        let old = now_nanos() - 2 * HOUR_NANOS;
        let first = apply(&table, &timed_batch(&[(1, "old", Some(old))])).await;
        let second = evict_outside(&table, &timed_schema()).await;

        assert_eq!(
            lookup_names_at(&table, &[1], Some(first)).unwrap(),
            names(&[Some("old")])
        );
        assert_eq!(
            lookup_names_at(&table, &[1], Some(second)).unwrap(),
            names(&[None])
        );
    }

    /// `#[serial]`: the coordinator channel is process-wide.
    #[tokio::test]
    #[serial_test::serial]
    async fn a_finalizer_from_the_coordinator_collects_on_the_next_apply() {
        // Far above the epochs other tests record.
        const EPOCH: u64 = 1_000_000;
        let table = table().await;
        let first = apply(&table, &batch(&[(1, "a", "i")])).await;
        let second = apply_marked(&table, &batch(&[(1, "b", "u")]), &[EPOCH]).await;
        crate::checkpoints::channels::send(
            crate::checkpoints::checkpoint_management::CHECKPOINT_COORDINATOR_CHANNEL,
            crate::checkpoints::checkpoint_management::CheckpointMessage::Finalizer(
                CheckpointEpoch(EPOCH),
            ),
        )
        .unwrap();
        assert!(!collected(&table, first));

        apply(&table, &batch(&[])).await;

        assert!(collected(&table, first));
        assert!(!collected(&table, second));
    }

    /// An `in_memory` backend that records the checkpoints and
    /// finalizations it's asked for.
    #[derive(Debug)]
    struct Recorder {
        backend: InMemoryBackend,
        calls: parking_lot::Mutex<Vec<String>>,
    }

    impl Recorder {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                backend: InMemoryBackend::try_new(TABLE, MAX_DEAD_RATIO).unwrap(),
                calls: parking_lot::Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().clone()
        }
    }

    #[async_trait]
    impl TableBackend for Recorder {
        async fn open(&self) -> Result<StoredState> {
            self.backend.open().await
        }

        async fn create_namespace(&self, spec: &NamespaceSpec) -> Result<()> {
            self.backend.create_namespace(spec).await
        }

        async fn drop_namespace(&self, name: &str) -> Result<()> {
            self.backend.drop_namespace(name).await
        }

        async fn begin(&self) -> Result<Box<dyn TableTransaction>> {
            self.backend.begin().await
        }

        async fn lookup(
            &self,
            namespace: &str,
            keys: &RecordBatch,
            projection: &[usize],
            mark: Option<Mark>,
        ) -> Result<LookupRows> {
            self.backend.lookup(namespace, keys, projection, mark).await
        }

        async fn checkpoint(&self, mark: Mark) -> Result<()> {
            self.calls.lock().push(format!("checkpoint {}", mark.0));
            self.backend.checkpoint(mark).await
        }

        async fn finalize(&self, horizon: Mark, trim: bool) -> Result<()> {
            self.calls
                .lock()
                .push(format!("finalize {} {trim}", horizon.0));
            self.backend.finalize(horizon, trim).await
        }

        async fn stats(&self) -> Result<Vec<NamespaceStats>> {
            self.backend.stats().await
        }
    }

    fn recorded(journal_trim: JournalTrim) -> (TableStore, Arc<Recorder>) {
        let recorder = Recorder::new();
        let table = store_on(
            TABLE,
            &schema(),
            &["id"],
            None,
            VersionRetention::Journal,
            journal_trim,
            OnSchemaChange::Fail,
            Arc::clone(&recorder) as Arc<dyn TableBackend>,
        )
        .unwrap();
        (table, recorder)
    }

    #[tokio::test]
    async fn a_batch_carrying_markers_returns_once_its_versions_are_checkpointed() {
        let (table, recorder) = recorded(JournalTrim::OnFinalize);
        apply(&table, &batch(&[(1, "a", "i")])).await;
        assert!(
            !recorder
                .calls()
                .iter()
                .any(|call| call.starts_with("checkpoint")),
            "no markers, no checkpoint"
        );
        let mark = apply_marked(&table, &batch(&[(1, "b", "u")]), &[1]).await;
        assert_eq!(
            recorder.calls().last(),
            Some(&format!("checkpoint {}", mark.0))
        );
        let idle = apply_marked(&table, &batch(&[]), &[2]).await;
        assert_eq!(idle, mark);
        assert_eq!(
            recorder.calls().last(),
            Some(&format!("checkpoint {}", mark.0)),
            "an idle batch's markers checkpoint the last mark"
        );
    }

    #[tokio::test]
    async fn journal_trim_never_finalizes_every_cut_without_trimming() {
        for (journal_trim, trim) in [(JournalTrim::OnFinalize, true), (JournalTrim::Never, false)] {
            let (table, recorder) = recorded(journal_trim);
            let first = apply(&table, &batch(&[(1, "a", "i")])).await;
            apply_marked(&table, &batch(&[(1, "b", "u")]), &[1]).await;
            table.finalize_epoch(1).await.unwrap();
            let finalized: Vec<String> = recorder
                .calls()
                .into_iter()
                .filter(|call| call.starts_with("finalize"))
                .collect();
            assert_eq!(
                finalized,
                [
                    format!("finalize {} {trim}", first.0 - 1),
                    format!("finalize {} {trim}", first.0 + 1),
                ],
                "at startup, then at the epoch's horizon"
            );
        }
    }

    /// A store that kept its rows, as a durable backend does across a
    /// restart.
    fn restarted(
        backend: &Arc<dyn TableBackend>,
        input_schema: &SchemaRef,
        predicate: &str,
    ) -> TableStore {
        store_on(
            TABLE,
            input_schema,
            &["id"],
            Some(WindowConfig {
                predicate,
                session_state: &SessionContext::new().state(),
                gc_interval: GC_INTERVAL,
            }),
            VersionRetention::Journal,
            JournalTrim::OnFinalize,
            OnSchemaChange::Fail,
            Arc::clone(backend),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_restart_restores_the_watermarks() {
        let backend: Arc<dyn TableBackend> =
            Arc::new(InMemoryBackend::try_new(TABLE, MAX_DEAD_RATIO).unwrap());
        let at = |hour: i64| Some(hour * HOUR_NANOS);
        let before = restarted(&backend, &timed_schema(), KEEP_LAST_HOUR_OF_DATA);
        apply(&before, &timed_batch(&[(1, "a", at(11))])).await;
        drop(before);

        let after = restarted(&backend, &timed_schema(), KEEP_LAST_HOUR_OF_DATA);
        apply(&after, &timed_batch(&[(2, "late", at(9))])).await;
        assert_eq!(
            lookup_names(&after, &[Some(1), Some(2)]),
            names(&[Some("a"), None]),
            "the watermark is still 11:00, so 9:00 is outside"
        );
    }

    /// The table on `backend`, with `schema()` instead of the windowed
    /// tables' columns.
    fn changed(backend: &Arc<dyn TableBackend>, on_schema_change: OnSchemaChange) -> TableStore {
        store_on(
            TABLE,
            &schema(),
            &["id"],
            None,
            VersionRetention::Journal,
            JournalTrim::OnFinalize,
            on_schema_change,
            Arc::clone(backend),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_restart_with_another_schema_fails_or_resets() {
        let backend: Arc<dyn TableBackend> =
            Arc::new(InMemoryBackend::try_new(TABLE, MAX_DEAD_RATIO).unwrap());
        let before = restarted(&backend, &timed_schema(), KEEP_LAST_HOUR);
        apply(&before, &timed_batch(&[(1, "a", Some(now_nanos()))])).await;
        drop(before);
        let after = restarted(&backend, &timed_schema(), KEEP_LAST_HOUR);
        after.start().await.expect("the same shape restarts");
        drop(after);

        let err = changed(&backend, OnSchemaChange::Fail)
            .start()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("the stored table has another schema or primary key")
                && err.contains("on_schema_change: reset"),
            "{err}"
        );

        let reset = changed(&backend, OnSchemaChange::Reset);
        reset.start().await.expect("reset drops the stored table");
        assert_eq!(lookup_names(&reset, &[Some(1)]), [None], "it starts empty");
        apply(&reset, &batch(&[(1, "b", "i")])).await;
        assert_eq!(lookup_names(&reset, &[Some(1)]), names(&[Some("b")]));
    }

    /// An `in_memory` backend whose next commits fail retriably, rolling
    /// their transactions back.
    #[derive(Debug)]
    struct Flaky {
        backend: Arc<dyn TableBackend>,
        failing_commits: Arc<std::sync::atomic::AtomicUsize>,
    }

    struct FlakyTransaction {
        transaction: Box<dyn TableTransaction>,
        failing_commits: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl TableBackend for Flaky {
        async fn open(&self) -> Result<StoredState> {
            self.backend.open().await
        }

        async fn create_namespace(&self, spec: &NamespaceSpec) -> Result<()> {
            self.backend.create_namespace(spec).await
        }

        async fn drop_namespace(&self, name: &str) -> Result<()> {
            self.backend.drop_namespace(name).await
        }

        async fn begin(&self) -> Result<Box<dyn TableTransaction>> {
            Ok(Box::new(FlakyTransaction {
                transaction: self.backend.begin().await?,
                failing_commits: Arc::clone(&self.failing_commits),
            }))
        }

        async fn lookup(
            &self,
            namespace: &str,
            keys: &RecordBatch,
            projection: &[usize],
            mark: Option<Mark>,
        ) -> Result<LookupRows> {
            self.backend.lookup(namespace, keys, projection, mark).await
        }

        async fn checkpoint(&self, mark: Mark) -> Result<()> {
            self.backend.checkpoint(mark).await
        }

        async fn finalize(&self, horizon: Mark, trim: bool) -> Result<()> {
            self.backend.finalize(horizon, trim).await
        }

        async fn stats(&self) -> Result<Vec<NamespaceStats>> {
            self.backend.stats().await
        }
    }

    #[async_trait]
    impl TableTransaction for FlakyTransaction {
        fn version(&self) -> backend::Version {
            self.transaction.version()
        }

        async fn get(&mut self, namespace: &str, keys: &RecordBatch) -> Result<LookupRows> {
            self.transaction.get(namespace, keys).await
        }

        async fn write(
            &mut self,
            namespace: &str,
            upserts: &RecordBatch,
            deletes: &RecordBatch,
        ) -> Result<RecordBatch> {
            self.transaction.write(namespace, upserts, deletes).await
        }

        async fn evict_below(
            &mut self,
            namespace: &str,
            column: &str,
            bound: &datafusion::common::ScalarValue,
            inclusive: bool,
            limit: usize,
        ) -> Result<RecordBatch> {
            self.transaction
                .evict_below(namespace, column, bound, inclusive, limit)
                .await
        }

        async fn scan(
            &mut self,
            namespace: &str,
            cursor: Option<backend::ScanCursor>,
            limit: usize,
        ) -> Result<(RecordBatch, Option<backend::ScanCursor>)> {
            self.transaction.scan(namespace, cursor, limit).await
        }

        async fn set_meta(&mut self, key: &str, value: Vec<u8>) -> Result<()> {
            self.transaction.set_meta(key, value).await
        }

        async fn commit(self: Box<Self>) -> Result<()> {
            use std::sync::atomic::Ordering;
            if self
                .failing_commits
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
            {
                return Err(crate::error::StreamlingError::from_parts(
                    anyhow::anyhow!("connection reset"),
                    true,
                    false,
                ));
            }
            self.transaction.commit().await
        }
    }

    /// The apply's transaction is dropped and re-run whole, so what it stores
    /// in meta, such as a moved watermark, survives the retry.
    #[tokio::test]
    async fn a_retriable_failure_reruns_the_whole_apply() {
        let backend: Arc<dyn TableBackend> =
            Arc::new(InMemoryBackend::try_new(TABLE, MAX_DEAD_RATIO).unwrap());
        let failing_commits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let flaky: Arc<dyn TableBackend> = Arc::new(Flaky {
            backend: Arc::clone(&backend),
            failing_commits: Arc::clone(&failing_commits),
        });
        let at = |hour: i64| Some(hour * HOUR_NANOS);
        let before = restarted(&flaky, &timed_schema(), KEEP_LAST_HOUR_OF_DATA);
        before.start().await.unwrap();
        failing_commits.store(1, std::sync::atomic::Ordering::SeqCst);
        apply(&before, &timed_batch(&[(1, "a", at(11))])).await;
        assert_eq!(
            failing_commits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the first commit failed"
        );
        assert_eq!(lookup_names(&before, &[Some(1)]), names(&[Some("a")]));
        drop(before);

        let after = restarted(&backend, &timed_schema(), KEEP_LAST_HOUR_OF_DATA);
        apply(&after, &timed_batch(&[(2, "late", at(9))])).await;
        assert_eq!(
            lookup_names(&after, &[Some(2)]),
            [None],
            "the retry stored the watermark the failed attempt moved"
        );
    }
}
