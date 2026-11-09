//! The storage a `table` runs on: a versioned keyed store. The host parses
//! changelogs, runs windows and maintains aggregates; a backend stores rows by
//! key in namespaces, journals each transaction's changes, and reads as of a
//! mark. See the conformance suite for the contract every backend satisfies.
//!
//! The data types are the plugin SDK's, so a plugin's backend passes them
//! through unchanged.

use crate::error::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::common::ScalarValue;
use std::fmt;

pub use streamling_plugin::api::table::{
    LookupRows, Mark, NamespaceSpec, NamespaceStats, ScanCursor, StoredState, Version,
};

/// A table's storage. Errors carry a `retriable` flag: a retriable failure
/// in a transaction leaves the store as if it never began, or, after an
/// unknown commit outcome, committed.
#[async_trait]
pub trait TableBackend: Send + Sync + fmt::Debug {
    /// Called before data flows.
    async fn open(&self) -> Result<StoredState>;

    /// Idempotent. Fails if the namespace exists with another schema or key.
    async fn create_namespace(&self, spec: &NamespaceSpec) -> Result<()>;

    /// Idempotent.
    async fn drop_namespace(&self, name: &str) -> Result<()>;

    /// Starts the next apply. The host never begins two transactions on one
    /// store at once.
    async fn begin(&self) -> Result<Box<dyn TableTransaction>>;

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
    ) -> Result<LookupRows>;

    /// Makes every version below `mark` survive a crash before returning.
    /// Idempotent. Never runs inside a transaction.
    async fn checkpoint(&self, mark: Mark) -> Result<()>;

    /// Every version below `horizon` is finalized. With `trim`, drops their
    /// journal entries and raises the floor to `horizon`. Without it, the floor
    /// stays and every read at or above it keeps its answer. Idempotent.
    /// Never runs inside a transaction.
    async fn finalize(&self, horizon: Mark, trim: bool) -> Result<()>;

    async fn stats(&self) -> Result<Vec<NamespaceStats>>;
}

/// One apply's writes, visible all at once on `commit`. Dropping it without
/// committing rolls it back.
#[async_trait]
pub trait TableTransaction: Send {
    fn version(&self) -> Version;

    /// The keys' rows, including this transaction's own writes.
    async fn get(&mut self, namespace: &str, keys: &RecordBatch) -> Result<LookupRows>;

    /// Upserts `upserts` and deletes the keys in `deletes`. The keys are
    /// unique within the call. Returns the replaced or deleted rows of the
    /// keys that had one.
    async fn write(
        &mut self,
        namespace: &str,
        upserts: &RecordBatch,
        deletes: &RecordBatch,
    ) -> Result<RecordBatch>;

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
    ) -> Result<RecordBatch>;

    /// Up to `limit` rows from `cursor`, and the cursor to continue from
    /// (`None` at the end). The pages visit each row once.
    async fn scan(
        &mut self,
        namespace: &str,
        cursor: Option<ScanCursor>,
        limit: usize,
    ) -> Result<(RecordBatch, Option<ScanCursor>)>;

    async fn set_meta(&mut self, key: &str, value: Vec<u8>) -> Result<()>;

    async fn commit(self: Box<Self>) -> Result<()>;
}
