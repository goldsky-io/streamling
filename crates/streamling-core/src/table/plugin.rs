//! Table backends from plugin libraries: [`FfiTableBackend`] serves the host's
//! [`TableBackend`] from a plugin's FFI object, and the registry holds the
//! backends the loaded libraries provide, by id.

use super::backend::{
    LookupRows, Mark, NamespaceSpec, NamespaceStats, ScanCursor, StoredState, TableBackend,
    TableTransaction, Version,
};
use crate::error::{Result, StreamlingError};
use crate::plugin::telemetry::process_plugin_metrics;
use crate::shutdown::ComponentScope;
use crate::streamling_user_err;
use crate::telemetry::recorder::get_metrics_recorder;
use abi_stable::external_types::crossbeam_channel;
use abi_stable::std_types::{RResult, RString, RVec};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::common::ScalarValue;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;
use streamling_plugin::PluginOptions;
use streamling_plugin::api::table::TableBackendError;
use streamling_plugin::ffi::{PluginMetricsChannel, PluginMetricsRecorder};
use streamling_plugin::table_backend::{
    FfiLookupRows, FfiNamespaceSpec, PluginTableBackendContext, PluginTableBackendDescriptor,
    PluginTableError, TableBackendFfiObj, TableTransactionFfiObj, scalar_to_ffi,
};
use tracing::info;

lazy_static! {
    static ref TABLE_BACKEND_DESCRIPTORS: RwLock<HashMap<String, PluginTableBackendDescriptor>> =
        RwLock::new(HashMap::new());
}

/// Registers the table backends a loaded plugin library provides.
pub fn store_plugin_table_backends(descriptors: RVec<PluginTableBackendDescriptor>) {
    let mut registry = TABLE_BACKEND_DESCRIPTORS
        .write()
        .expect("table backend descriptors write lock");
    for descriptor in descriptors {
        info!("Registered plugin table backend: {}", descriptor.id);
        registry.insert(descriptor.id.to_string(), descriptor);
    }
}

/// Creates the plugin table backend `id` for `table`. Its metrics are
/// forwarded under `metric_metadata_id` by a task of `scope`.
pub fn create_plugin_table_backend(
    id: &str,
    application_id: &str,
    table: &str,
    options: HashMap<String, String>,
    metric_metadata_id: String,
    channel_capacity: usize,
    scope: &ComponentScope,
) -> Result<FfiTableBackend> {
    let registry = TABLE_BACKEND_DESCRIPTORS
        .read()
        .expect("table backend descriptors read lock");
    let Some(descriptor) = registry.get(id) else {
        let mut loaded: Vec<&str> = registry.keys().map(String::as_str).collect();
        loaded.sort();
        return Err(streamling_user_err!(
            "table '{}': no table backend '{}'; check that its plugin library is in plugin.path. Loaded table backends: [{}]",
            table,
            id,
            loaded.join(", ")
        ));
    };
    let metrics = PluginMetricsChannel::new(crossbeam_channel::bounded(channel_capacity));
    let recorder = PluginMetricsRecorder::new(metrics.sender.clone());
    // Exits when the backend drops its recorder and the channel disconnects.
    scope.spawn(process_plugin_metrics(
        metrics.receiver,
        get_metrics_recorder(),
        metric_metadata_id,
        table.to_string(),
        scope.stage_token().clone(),
    ));
    let backend = match (descriptor.create)(
        PluginTableBackendContext {
            application_id: application_id.into(),
            table: table.into(),
        },
        PluginOptions::new(options),
        crate::plugin::host_async_runtime(),
        recorder,
    ) {
        RResult::ROk(backend) => backend,
        RResult::RErr(error) => {
            return Err(streamling_user_err!(
                "table '{}': table backend '{}' failed to start: {}",
                table,
                id,
                error.message
            ));
        }
    };
    Ok(FfiTableBackend {
        table: table.to_string(),
        backend,
    })
}

/// A plugin's table backend.
pub struct FfiTableBackend {
    /// The table's name, for errors.
    table: String,
    backend: TableBackendFfiObj,
}

impl fmt::Debug for FfiTableBackend {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FfiTableBackend")
            .field("table", &self.table)
            .finish()
    }
}

/// A plugin's error, keeping its `retriable` flag.
fn plugin_error(table: &str, error: PluginTableError) -> StreamlingError {
    StreamlingError::from_parts(
        anyhow::anyhow!("table '{}': {}", table, error.message),
        error.retriable,
        true,
    )
}

/// An error decoding what a plugin returned, which a retry won't fix.
fn decode_error(table: &str, error: TableBackendError) -> StreamlingError {
    plugin_error(table, error.into())
}

fn result<T>(table: &str, result: RResult<T, PluginTableError>) -> Result<T> {
    result
        .into_result()
        .map_err(|error| plugin_error(table, error))
}

fn lookup_rows(table: &str, rows: FfiLookupRows) -> Result<LookupRows> {
    LookupRows::try_from(rows).map_err(|error| decode_error(table, error))
}

#[async_trait]
impl TableBackend for FfiTableBackend {
    async fn open(&self) -> Result<StoredState> {
        Ok(result(&self.table, self.backend.open().await)?.into())
    }

    async fn create_namespace(&self, spec: &NamespaceSpec) -> Result<()> {
        result(
            &self.table,
            self.backend
                .create_namespace(FfiNamespaceSpec::from(spec))
                .await,
        )
    }

    async fn drop_namespace(&self, name: &str) -> Result<()> {
        result(&self.table, self.backend.drop_namespace(name.into()).await)
    }

    async fn begin(&self) -> Result<Box<dyn TableTransaction>> {
        let transaction = result(&self.table, self.backend.begin().await)?;
        Ok(Box::new(FfiTableTransaction {
            table: self.table.clone(),
            version: transaction.version(),
            transaction,
        }))
    }

    async fn lookup(
        &self,
        namespace: &str,
        keys: &RecordBatch,
        projection: &[usize],
        mark: Option<Mark>,
    ) -> Result<LookupRows> {
        let rows = self
            .backend
            .lookup(
                namespace.into(),
                keys.clone().into(),
                projection.iter().map(|&index| index as u64).collect(),
                mark.map(|mark| mark.0).into(),
            )
            .await;
        lookup_rows(&self.table, result(&self.table, rows)?)
    }

    async fn checkpoint(&self, mark: Mark) -> Result<()> {
        result(&self.table, self.backend.checkpoint(mark.0).await)
    }

    async fn finalize(&self, horizon: Mark, trim: bool) -> Result<()> {
        result(&self.table, self.backend.finalize(horizon.0, trim).await)
    }

    async fn stats(&self) -> Result<Vec<NamespaceStats>> {
        Ok(result(&self.table, self.backend.stats().await)?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

struct FfiTableTransaction {
    table: String,
    version: Version,
    transaction: TableTransactionFfiObj,
}

#[async_trait]
impl TableTransaction for FfiTableTransaction {
    fn version(&self) -> Version {
        self.version
    }

    async fn get(&mut self, namespace: &str, keys: &RecordBatch) -> Result<LookupRows> {
        let rows = self
            .transaction
            .get(namespace.into(), keys.clone().into())
            .await;
        lookup_rows(&self.table, result(&self.table, rows)?)
    }

    async fn write(
        &mut self,
        namespace: &str,
        upserts: &RecordBatch,
        deletes: &RecordBatch,
    ) -> Result<RecordBatch> {
        let removed = self
            .transaction
            .write(
                namespace.into(),
                upserts.clone().into(),
                deletes.clone().into(),
            )
            .await;
        Ok(result(&self.table, removed)?.into())
    }

    async fn evict_below(
        &mut self,
        namespace: &str,
        column: &str,
        bound: &ScalarValue,
        inclusive: bool,
        limit: usize,
    ) -> Result<RecordBatch> {
        let bound = scalar_to_ffi(bound).map_err(|error| decode_error(&self.table, error))?;
        let evicted = self
            .transaction
            .evict_below(
                namespace.into(),
                column.into(),
                bound,
                inclusive,
                limit as u64,
            )
            .await;
        Ok(result(&self.table, evicted)?.into())
    }

    async fn scan(
        &mut self,
        namespace: &str,
        cursor: Option<ScanCursor>,
        limit: usize,
    ) -> Result<(RecordBatch, Option<ScanCursor>)> {
        let page = self
            .transaction
            .scan(
                namespace.into(),
                cursor.map(|ScanCursor(cursor)| RVec::from(cursor)).into(),
                limit as u64,
            )
            .await;
        let page = result(&self.table, page)?;
        Ok((
            page.rows.into(),
            page.next
                .into_option()
                .map(|cursor| ScanCursor(cursor.into())),
        ))
    }

    async fn set_meta(&mut self, key: &str, value: Vec<u8>) -> Result<()> {
        result(
            &self.table,
            self.transaction
                .set_meta(RString::from(key), value.into())
                .await,
        )
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        let table = self.table;
        result(&table, self.transaction.commit().await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::test_plugins::{self, FAIL_OPEN, PANIC_STATS, TABLE_BACKEND};
    use crate::table::conformance::{self, BackendFactory};
    use std::sync::Arc;

    const APPLICATION: &str = "app";
    const CHANNEL_CAPACITY: usize = 64;

    fn create(table: &str, options: &[(&str, &str)]) -> Result<FfiTableBackend> {
        test_plugins::install();
        create_plugin_table_backend(
            TABLE_BACKEND,
            APPLICATION,
            table,
            options
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            format!("{APPLICATION}::{table}"),
            CHANNEL_CAPACITY,
            &ComponentScope::detached("test"),
        )
    }

    struct Factory;

    #[async_trait]
    impl BackendFactory for Factory {
        async fn create(&self, store: &str) -> Arc<dyn TableBackend> {
            Arc::new(create(store, &[]).unwrap())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_plugin_backend_satisfies_the_backend_contract_through_ffi() {
        conformance::run(&Factory).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn plugin_errors_keep_their_retriable_flag() {
        for (failure, retriable) in [("retriable", true), ("fatal", false)] {
            let err = create("errors", &[(FAIL_OPEN, failure)])
                .unwrap()
                .open()
                .await
                .unwrap_err();
            assert_eq!(err.is_retriable(), retriable, "{err}");
            assert!(err.to_string().contains("table 'errors'"), "{err}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_panicking_call_fails_without_a_retry() {
        let err = create("panics", &[(PANIC_STATS, "")])
            .unwrap()
            .stats()
            .await
            .unwrap_err();
        assert!(!err.is_retriable(), "{err}");
        assert!(err.to_string().contains("stats blew up"), "{err}");
    }

    #[test]
    fn an_unknown_backend_lists_the_loaded_ones() {
        test_plugins::install();
        let err = create_plugin_table_backend(
            "missing",
            APPLICATION,
            "t",
            HashMap::new(),
            "app::t".to_string(),
            CHANNEL_CAPACITY,
            &ComponentScope::detached("test"),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("no table backend 'missing'") && err.contains(TABLE_BACKEND),
            "{err}"
        );
    }

    /// `(id, ts)` rows of op `op`.
    fn rows(rows: &[(i64, i64)], op: &str) -> RecordBatch {
        use arrow::array::{Int64Array, StringArray};
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(StringArray::from(vec![op; rows.len()])),
            ],
        )
        .unwrap()
    }

    fn schema() -> arrow::datatypes::SchemaRef {
        use arrow::datatypes::{DataType, Field, Schema};
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new(crate::data::COLUMN_NAME_OP, DataType::Utf8, false),
        ]))
    }

    /// A windowed table on the plugin's backend.
    fn plugin_table(name: &str) -> crate::table::TableStore {
        use crate::table::testing::store_on;
        use crate::table::{JournalTrim, OnSchemaChange, VersionRetention, WindowConfig};
        use datafusion::prelude::SessionContext;
        const GC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
        store_on(
            name,
            &schema(),
            &["id"],
            Some(WindowConfig {
                predicate: "ts >= watermark(ts) - 10",
                session_state: &SessionContext::new().state(),
                gc_interval: GC_INTERVAL,
            }),
            VersionRetention::Journal,
            JournalTrim::OnFinalize,
            OnSchemaChange::Fail,
            Arc::new(create(name, &[]).unwrap()),
        )
        .unwrap()
    }

    fn timestamps(
        table: &crate::table::TableStore,
        ids: &[i64],
        mark: Option<Mark>,
    ) -> Result<Vec<Option<i64>>> {
        use arrow::array::{ArrayRef, AsArray, Int64Array};
        use arrow::datatypes::Int64Type;
        let keys: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
        Ok(table
            .table()
            .lookup(&[keys], "ts", mark)?
            .as_primitive::<Int64Type>()
            .iter()
            .collect())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_table_runs_on_a_plugin_backend() {
        let table = plugin_table("plugin_backed");
        let mark_of = |marks: crate::table::Marks| marks[0].1;
        let first = mark_of(
            table
                .apply(&rows(&[(1, 100), (2, 105)], "i"), &[])
                .await
                .unwrap(),
        );
        let second = mark_of(table.apply(&rows(&[(3, 112)], "i"), &[]).await.unwrap());
        assert_eq!(
            timestamps(&table, &[1, 2, 3], Some(first)).unwrap(),
            [Some(100), Some(105), None]
        );
        assert_eq!(
            timestamps(&table, &[1, 2, 3], Some(second)).unwrap(),
            [None, Some(105), Some(112)],
            "the watermark moved to 112, so the range window evicted 100"
        );
        table.apply(&rows(&[(2, 0)], "d"), &[]).await.unwrap();
        assert_eq!(
            timestamps(&table, &[1, 2, 3], None).unwrap(),
            [None, None, Some(112)]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_plugin_lookup_needs_a_multi_threaded_runtime() {
        let table = plugin_table("plugin_current_thread");
        table.start().await.unwrap();
        let err = timestamps(&table, &[1], None).unwrap_err().to_string();
        assert!(err.contains("multi-threaded Tokio runtime"), "{err}");
    }
}
