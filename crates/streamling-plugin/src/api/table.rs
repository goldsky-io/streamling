//! Table backends: the storage a `table` transform runs on, as a versioned
//! keyed store. The host parses the changelog, runs the window and maintains
//! the table's aggregates; a backend stores rows by key in namespaces,
//! journals each transaction's changes, and reads them as of a mark.
//!
//! Implement [`TableBackendPlugin`] and register it with
//! `register_plugin_table_backend!`.

use crate::ffi::PluginMetricsRecorder;
use arrow::array::{BooleanArray, RecordBatch};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::ScalarValue;
use std::collections::HashMap;
use std::fmt;

/// One committed apply. Versions strictly increase for a store's lifetime,
/// across restarts too for a durable backend.
pub type Version = u64;

/// A moment a reader sees a store at: a read at mark `M` sees every version
/// below `M` and nothing later. The mark of the apply at version `v` is
/// `v + 1`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Mark(pub u64);

impl Mark {
    /// The mark that sees `version` and every version before it.
    pub fn after(version: Version) -> Self {
        Self(version + 1)
    }
}

/// What a store persisted, as `open` finds it. Empty for a new or volatile
/// store.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StoredState {
    pub next_version: Version,
    /// The lowest readable mark.
    pub floor: Mark,
    pub meta: HashMap<String, Vec<u8>>,
}

/// A keyed set of rows in a store.
#[derive(Clone, Debug, PartialEq)]
pub struct NamespaceSpec {
    pub name: String,
    pub schema: SchemaRef,
    /// Key columns, in order. Never null.
    pub key: Vec<String>,
    /// Whether changes are journaled, so the namespace reads as of a mark.
    pub versioned: bool,
    /// The column a range window evicts by, which a backend may index.
    pub range_column: Option<String>,
}

/// Where a paged scan continues. Opaque and backend-defined.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanCursor(pub Vec<u8>);

/// One row per looked-up key, in input order: the requested columns of the
/// key's row, all null where `found` is false.
#[derive(Clone, Debug)]
pub struct LookupRows {
    pub rows: RecordBatch,
    pub found: BooleanArray,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceStats {
    pub namespace: String,
    pub live_rows: u64,
    pub journal_rows: u64,
    pub memory_bytes: Option<u64>,
    pub retired_bytes: Option<u64>,
}

/// Which table a backend instance stores.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableBackendContext {
    pub application_id: String,
    /// The table's name in the pipeline.
    pub table: String,
}

/// A failed backend call. A retriable one, such as a lost connection, is
/// retried with backoff; anything else fails the pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableBackendError {
    pub message: String,
    pub retriable: bool,
}

impl TableBackendError {
    pub fn retriable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retriable: true,
        }
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retriable: false,
        }
    }
}

impl fmt::Display for TableBackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TableBackendError {}

pub type TableBackendResult<T> = Result<T, TableBackendError>;

/// A table backend a plugin provides. One instance stores one table: its
/// namespace and its aggregates'. The host never begins two transactions on
/// one instance at once.
///
/// A retriable failure in a transaction must leave the store as if the
/// transaction never began, or, after an unknown commit outcome, committed:
/// the host re-runs the same apply either way.
#[async_trait]
pub trait TableBackendPlugin: Send + Sync + 'static {
    /// `options` come from the app config's `table.backends.<id>`. Fails
    /// naming any missing option. Must not connect: `open` does.
    fn create(
        context: TableBackendContext,
        options: HashMap<String, String>,
        metrics: PluginMetricsRecorder,
    ) -> TableBackendResult<Self>
    where
        Self: Sized;

    /// Called before data flows. A durable backend waits here, retriably,
    /// until no other process writes the store.
    async fn open(&self) -> TableBackendResult<StoredState>;

    /// Idempotent. Fails if the namespace exists with another schema or key.
    async fn create_namespace(&self, spec: &NamespaceSpec) -> TableBackendResult<()>;

    /// Idempotent.
    async fn drop_namespace(&self, name: &str) -> TableBackendResult<()>;

    async fn begin(&self) -> TableBackendResult<Box<dyn TableTransactionPlugin>>;

    /// For each row of `keys` (the key columns, never null), the
    /// `projection` columns of the key's row as of `mark`, or the latest
    /// commit without one. Fails below the floor, checked atomically with
    /// the read.
    async fn lookup(
        &self,
        namespace: &str,
        keys: &RecordBatch,
        projection: &[usize],
        mark: Option<Mark>,
    ) -> TableBackendResult<LookupRows>;

    /// Makes every version below `mark` survive a crash before returning.
    /// Idempotent. Never runs inside a transaction.
    async fn checkpoint(&self, mark: Mark) -> TableBackendResult<()>;

    /// Every version below `horizon` is finalized. With `trim`, drops their
    /// journal entries and raises the floor to `horizon`. Without it, the floor
    /// stays and every read at or above it keeps its answer. Idempotent.
    /// Never runs inside a transaction.
    async fn finalize(&self, horizon: Mark, trim: bool) -> TableBackendResult<()>;

    async fn stats(&self) -> TableBackendResult<Vec<NamespaceStats>>;
}

/// One apply's writes, visible all at once on `commit`. Dropping it without
/// committing rolls it back.
#[async_trait]
pub trait TableTransactionPlugin: Send {
    fn version(&self) -> Version;

    /// The keys' rows, including this transaction's own writes.
    async fn get(&mut self, namespace: &str, keys: &RecordBatch) -> TableBackendResult<LookupRows>;

    /// Upserts `upserts` and deletes the keys in `deletes`. The keys are
    /// unique within the call. Returns the replaced or deleted rows of the
    /// keys that had one.
    async fn write(
        &mut self,
        namespace: &str,
        upserts: &RecordBatch,
        deletes: &RecordBatch,
    ) -> TableBackendResult<RecordBatch>;

    /// Deletes up to `limit` rows whose `column` is below `bound` (or equal
    /// when `inclusive`) or null, and returns them. `bound` has the column's
    /// type.
    async fn evict_below(
        &mut self,
        namespace: &str,
        column: &str,
        bound: &ScalarValue,
        inclusive: bool,
        limit: usize,
    ) -> TableBackendResult<RecordBatch>;

    /// Up to `limit` rows from `cursor`, and the cursor to continue from
    /// (`None` at the end). The pages visit each row once.
    async fn scan(
        &mut self,
        namespace: &str,
        cursor: Option<ScanCursor>,
        limit: usize,
    ) -> TableBackendResult<(RecordBatch, Option<ScanCursor>)>;

    async fn set_meta(&mut self, key: &str, value: Vec<u8>) -> TableBackendResult<()>;

    async fn commit(self: Box<Self>) -> TableBackendResult<()>;
}
