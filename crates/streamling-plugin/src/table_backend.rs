//! Table backends across the FFI boundary: the objects the host calls, and
//! the adapters that serve them from a [`TableBackendPlugin`].
//!
//! Every call returns a future the host awaits. The adapters run each call
//! on a runtime the plugin library owns: the runtime the host passes wraps
//! the host's Tokio, whose threads have no reactor for the library's own copy
//! of Tokio.

use crate::api::table::{
    LookupRows, Mark, NamespaceSpec, NamespaceStats, ScanCursor, StoredState, TableBackendContext,
    TableBackendError, TableBackendPlugin, TableBackendResult, TableTransactionPlugin,
};
use crate::r#async::{DirectTokioProxy, PluginAsyncRuntimeObj};
use crate::ffi::{
    PluginMetricsRecorder, PluginOptions, SafeArrowArray, SafeArrowColumn, SafeArrowSchema,
};
use crate::panic_payload_to_string;
use abi_stable::sabi_trait::TD_Opaque;
use abi_stable::std_types::{RBox, RErr, ROk, ROption, RResult, RString, RVec, Tuple2};
use abi_stable::{StableAbi, sabi_trait};
use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch};
use async_ffi::{FfiFuture, FutureExt};
use datafusion::common::ScalarValue;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use tokio::runtime::Handle;

/// A failed call. See [`TableBackendError`].
#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct PluginTableError {
    pub message: RString,
    pub retriable: bool,
}

impl From<TableBackendError> for PluginTableError {
    fn from(error: TableBackendError) -> Self {
        Self {
            message: error.message.into(),
            retriable: error.retriable,
        }
    }
}

impl From<PluginTableError> for TableBackendError {
    fn from(error: PluginTableError) -> Self {
        Self {
            message: error.message.into(),
            retriable: error.retriable,
        }
    }
}

pub type TableResult<T> = RResult<T, PluginTableError>;

#[repr(C)]
#[derive(StableAbi, Clone, Debug)]
pub struct PluginTableBackendContext {
    pub application_id: RString,
    pub table: RString,
}

#[repr(C)]
#[derive(StableAbi)]
pub struct FfiStoredState {
    pub next_version: u64,
    pub floor: u64,
    pub meta: RVec<Tuple2<RString, RVec<u8>>>,
}

impl From<StoredState> for FfiStoredState {
    fn from(state: StoredState) -> Self {
        Self {
            next_version: state.next_version,
            floor: state.floor.0,
            meta: state
                .meta
                .into_iter()
                .map(|(key, value)| Tuple2(key.into(), value.into()))
                .collect(),
        }
    }
}

impl From<FfiStoredState> for StoredState {
    fn from(state: FfiStoredState) -> Self {
        Self {
            next_version: state.next_version,
            floor: Mark(state.floor),
            meta: state
                .meta
                .into_iter()
                .map(|Tuple2(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}

#[repr(C)]
#[derive(StableAbi)]
pub struct FfiNamespaceSpec {
    pub name: RString,
    pub schema: SafeArrowSchema,
    pub key: RVec<RString>,
    pub versioned: bool,
    pub range_column: ROption<RString>,
}

impl From<&NamespaceSpec> for FfiNamespaceSpec {
    fn from(spec: &NamespaceSpec) -> Self {
        Self {
            name: spec.name.as_str().into(),
            schema: Arc::clone(&spec.schema).into(),
            key: spec
                .key
                .iter()
                .map(|column| column.as_str().into())
                .collect(),
            versioned: spec.versioned,
            range_column: spec.range_column.as_deref().map(RString::from).into(),
        }
    }
}

impl From<FfiNamespaceSpec> for NamespaceSpec {
    fn from(spec: FfiNamespaceSpec) -> Self {
        Self {
            name: spec.name.into(),
            schema: spec.schema.into(),
            key: spec.key.into_iter().map(Into::into).collect(),
            versioned: spec.versioned,
            range_column: spec.range_column.map(Into::into).into(),
        }
    }
}

#[repr(C)]
#[derive(StableAbi)]
pub struct FfiLookupRows {
    pub rows: SafeArrowArray,
    pub found: SafeArrowColumn,
}

impl From<LookupRows> for FfiLookupRows {
    fn from(rows: LookupRows) -> Self {
        Self {
            rows: rows.rows.into(),
            found: (Arc::new(rows.found) as ArrayRef).into(),
        }
    }
}

impl TryFrom<FfiLookupRows> for LookupRows {
    type Error = TableBackendError;

    fn try_from(rows: FfiLookupRows) -> TableBackendResult<Self> {
        let found = ArrayRef::from(rows.found);
        let found = found
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                TableBackendError::fatal(format!(
                    "a lookup's found mask must be Boolean, got {}",
                    found.data_type()
                ))
            })?
            .clone();
        Ok(Self {
            rows: rows.rows.into(),
            found,
        })
    }
}

#[repr(C)]
#[derive(StableAbi)]
pub struct FfiNamespaceStats {
    pub namespace: RString,
    pub live_rows: u64,
    pub journal_rows: u64,
    pub memory_bytes: ROption<u64>,
    pub retired_bytes: ROption<u64>,
}

impl From<NamespaceStats> for FfiNamespaceStats {
    fn from(stats: NamespaceStats) -> Self {
        Self {
            namespace: stats.namespace.into(),
            live_rows: stats.live_rows,
            journal_rows: stats.journal_rows,
            memory_bytes: stats.memory_bytes.into(),
            retired_bytes: stats.retired_bytes.into(),
        }
    }
}

impl From<FfiNamespaceStats> for NamespaceStats {
    fn from(stats: FfiNamespaceStats) -> Self {
        Self {
            namespace: stats.namespace.into(),
            live_rows: stats.live_rows,
            journal_rows: stats.journal_rows,
            memory_bytes: stats.memory_bytes.into(),
            retired_bytes: stats.retired_bytes.into(),
        }
    }
}

#[repr(C)]
#[derive(StableAbi)]
pub struct FfiScanPage {
    pub rows: SafeArrowArray,
    pub next: ROption<RVec<u8>>,
}

/// A scalar as a one-row column.
pub fn scalar_to_ffi(value: &ScalarValue) -> TableBackendResult<SafeArrowColumn> {
    value
        .to_array()
        .map(SafeArrowColumn::from)
        .map_err(|e| TableBackendError::fatal(format!("failed to encode {value}: {e}")))
}

pub fn scalar_from_ffi(value: SafeArrowColumn) -> TableBackendResult<ScalarValue> {
    ScalarValue::try_from_array(&ArrayRef::from(value), 0)
        .map_err(|e| TableBackendError::fatal(format!("failed to decode a scalar: {e}")))
}

/// [`TableBackendPlugin`] across the FFI boundary.
#[sabi_trait]
pub trait TableBackendFfi: Send + Sync {
    fn open(&self) -> FfiFuture<TableResult<FfiStoredState>>;
    fn create_namespace(&self, spec: FfiNamespaceSpec) -> FfiFuture<TableResult<()>>;
    fn drop_namespace(&self, name: RString) -> FfiFuture<TableResult<()>>;
    fn begin(&self) -> FfiFuture<TableResult<TableTransactionFfiObj>>;
    fn lookup(
        &self,
        namespace: RString,
        keys: SafeArrowArray,
        projection: RVec<u64>,
        mark: ROption<u64>,
    ) -> FfiFuture<TableResult<FfiLookupRows>>;
    fn checkpoint(&self, mark: u64) -> FfiFuture<TableResult<()>>;
    fn finalize(&self, horizon: u64, trim: bool) -> FfiFuture<TableResult<()>>;
    fn stats(&self) -> FfiFuture<TableResult<RVec<FfiNamespaceStats>>>;
}

pub type TableBackendFfiObj = TableBackendFfi_TO<'static, RBox<()>>;

pub use transaction_ffi::*;

// What `#[sabi_trait]` generates for the by-value `commit` casts a pointer to
// its own type.
#[allow(clippy::unnecessary_cast)]
mod transaction_ffi {
    use super::*;

    /// [`TableTransactionPlugin`] across the FFI boundary. Dropping it without
    /// committing rolls it back.
    #[sabi_trait]
    pub trait TableTransactionFfi: Send {
        fn version(&self) -> u64;
        fn get(
            &mut self,
            namespace: RString,
            keys: SafeArrowArray,
        ) -> FfiFuture<TableResult<FfiLookupRows>>;
        fn write(
            &mut self,
            namespace: RString,
            upserts: SafeArrowArray,
            deletes: SafeArrowArray,
        ) -> FfiFuture<TableResult<SafeArrowArray>>;
        /// `bound` is a one-row column.
        fn evict_below(
            &mut self,
            namespace: RString,
            column: RString,
            bound: SafeArrowColumn,
            inclusive: bool,
            limit: u64,
        ) -> FfiFuture<TableResult<SafeArrowArray>>;
        fn scan(
            &mut self,
            namespace: RString,
            cursor: ROption<RVec<u8>>,
            limit: u64,
        ) -> FfiFuture<TableResult<FfiScanPage>>;
        fn set_meta(&mut self, key: RString, value: RVec<u8>) -> FfiFuture<TableResult<()>>;
        fn commit(self) -> FfiFuture<TableResult<()>>;
    }

    pub type TableTransactionFfiObj = TableTransactionFfi_TO<'static, RBox<()>>;
}

/// A table backend a plugin library provides, under its id.
#[repr(C)]
#[derive(StableAbi)]
pub struct PluginTableBackendDescriptor {
    pub id: RString,
    /// One instance per table. `options` come from the app config's
    /// `table.backends.<id>`.
    pub create: extern "C" fn(
        context: PluginTableBackendContext,
        options: PluginOptions,
        runtime: PluginAsyncRuntimeObj,
        metrics: PluginMetricsRecorder,
    ) -> TableResult<TableBackendFfiObj>,
}

/// Creates `T` for a table and serves it across the FFI boundary. Called by
/// the code `register_plugin_table_backend!` generates.
pub fn create_table_backend<T: TableBackendPlugin>(
    context: PluginTableBackendContext,
    options: PluginOptions,
    metrics: PluginMetricsRecorder,
) -> TableResult<TableBackendFfiObj> {
    let context = TableBackendContext {
        application_id: context.application_id.into(),
        table: context.table.into(),
    };
    // Rationale: as for the other plugin factories, a panic is only turned
    // into an error; no state of the failed backend is observed after it.
    match std::panic::catch_unwind(AssertUnwindSafe(|| {
        T::create(context, options.as_rust(), metrics)
    })) {
        Ok(Ok(backend)) => ROk(TableBackendFfi_TO::from_value(
            PluginTableBackend {
                inner: Arc::new(backend),
                runtime: DirectTokioProxy::new().handle(),
            },
            TD_Opaque,
        )),
        Ok(Err(error)) => RErr(error.into()),
        Err(panic) => RErr(PluginTableError {
            message: format!(
                "table backend panicked while being created: {}",
                panic_payload_to_string(panic)
            )
            .into(),
            retriable: false,
        }),
    }
}

/// Runs `call` on the plugin's runtime. A panic in it is a failure the host
/// doesn't retry.
fn run<T, F>(runtime: &Handle, call: F) -> FfiFuture<TableResult<T>>
where
    T: Send + 'static,
    F: Future<Output = TableBackendResult<T>> + Send + 'static,
{
    let task = runtime.spawn(call);
    async move {
        match task.await {
            Ok(result) => result.map_err(PluginTableError::from).into(),
            Err(error) => RErr(PluginTableError {
                message: format!("table backend call failed: {error}").into(),
                retriable: false,
            }),
        }
    }
    .into_ffi()
}

struct PluginTableBackend<T> {
    inner: Arc<T>,
    runtime: Handle,
}

impl<T: TableBackendPlugin> TableBackendFfi for PluginTableBackend<T> {
    fn open(&self) -> FfiFuture<TableResult<FfiStoredState>> {
        let inner = Arc::clone(&self.inner);
        run(
            &self.runtime,
            async move { inner.open().await.map(Into::into) },
        )
    }

    fn create_namespace(&self, spec: FfiNamespaceSpec) -> FfiFuture<TableResult<()>> {
        let inner = Arc::clone(&self.inner);
        run(&self.runtime, async move {
            inner.create_namespace(&NamespaceSpec::from(spec)).await
        })
    }

    fn drop_namespace(&self, name: RString) -> FfiFuture<TableResult<()>> {
        let inner = Arc::clone(&self.inner);
        run(
            &self.runtime,
            async move { inner.drop_namespace(&name).await },
        )
    }

    fn begin(&self) -> FfiFuture<TableResult<TableTransactionFfiObj>> {
        let inner = Arc::clone(&self.inner);
        let runtime = self.runtime.clone();
        run(&self.runtime, async move {
            let transaction = inner.begin().await?;
            Ok(TableTransactionFfi_TO::from_value(
                PluginTableTransaction {
                    version: transaction.version(),
                    inner: Arc::new(tokio::sync::Mutex::new(Some(transaction))),
                    runtime,
                },
                TD_Opaque,
            ))
        })
    }

    fn lookup(
        &self,
        namespace: RString,
        keys: SafeArrowArray,
        projection: RVec<u64>,
        mark: ROption<u64>,
    ) -> FfiFuture<TableResult<FfiLookupRows>> {
        let inner = Arc::clone(&self.inner);
        run(&self.runtime, async move {
            let projection: Vec<usize> = projection.iter().map(|&index| index as usize).collect();
            inner
                .lookup(
                    &namespace,
                    &RecordBatch::from(keys),
                    &projection,
                    mark.into_option().map(Mark),
                )
                .await
                .map(Into::into)
        })
    }

    fn checkpoint(&self, mark: u64) -> FfiFuture<TableResult<()>> {
        let inner = Arc::clone(&self.inner);
        run(
            &self.runtime,
            async move { inner.checkpoint(Mark(mark)).await },
        )
    }

    fn finalize(&self, horizon: u64, trim: bool) -> FfiFuture<TableResult<()>> {
        let inner = Arc::clone(&self.inner);
        run(&self.runtime, async move {
            inner.finalize(Mark(horizon), trim).await
        })
    }

    fn stats(&self) -> FfiFuture<TableResult<RVec<FfiNamespaceStats>>> {
        let inner = Arc::clone(&self.inner);
        run(&self.runtime, async move {
            Ok(inner.stats().await?.into_iter().map(Into::into).collect())
        })
    }
}

/// The transaction, shared with the call running on the plugin's runtime.
/// `None` once committed.
type SharedTransaction = Arc<tokio::sync::Mutex<Option<Box<dyn TableTransactionPlugin>>>>;

struct PluginTableTransaction {
    version: u64,
    inner: SharedTransaction,
    runtime: Handle,
}

impl PluginTableTransaction {
    /// Runs `call` on the transaction, on the plugin's runtime.
    fn run<T, F>(&self, call: F) -> FfiFuture<TableResult<T>>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(
                &'a mut Box<dyn TableTransactionPlugin>,
            ) -> futures::future::BoxFuture<'a, TableBackendResult<T>>
            + Send
            + 'static,
    {
        let inner = Arc::clone(&self.inner);
        run(&self.runtime, async move {
            let mut transaction = inner.lock().await;
            let transaction = transaction
                .as_mut()
                .ok_or_else(|| TableBackendError::fatal("the transaction already committed"))?;
            call(transaction).await
        })
    }
}

impl TableTransactionFfi for PluginTableTransaction {
    fn version(&self) -> u64 {
        self.version
    }

    fn get(
        &mut self,
        namespace: RString,
        keys: SafeArrowArray,
    ) -> FfiFuture<TableResult<FfiLookupRows>> {
        let keys = RecordBatch::from(keys);
        self.run(move |transaction| {
            Box::pin(async move { transaction.get(&namespace, &keys).await.map(Into::into) })
        })
    }

    fn write(
        &mut self,
        namespace: RString,
        upserts: SafeArrowArray,
        deletes: SafeArrowArray,
    ) -> FfiFuture<TableResult<SafeArrowArray>> {
        let (upserts, deletes) = (RecordBatch::from(upserts), RecordBatch::from(deletes));
        self.run(move |transaction| {
            Box::pin(async move {
                transaction
                    .write(&namespace, &upserts, &deletes)
                    .await
                    .map(Into::into)
            })
        })
    }

    fn evict_below(
        &mut self,
        namespace: RString,
        column: RString,
        bound: SafeArrowColumn,
        inclusive: bool,
        limit: u64,
    ) -> FfiFuture<TableResult<SafeArrowArray>> {
        let bound = scalar_from_ffi(bound);
        self.run(move |transaction| {
            Box::pin(async move {
                transaction
                    .evict_below(&namespace, &column, &bound?, inclusive, limit as usize)
                    .await
                    .map(Into::into)
            })
        })
    }

    fn scan(
        &mut self,
        namespace: RString,
        cursor: ROption<RVec<u8>>,
        limit: u64,
    ) -> FfiFuture<TableResult<FfiScanPage>> {
        let cursor = cursor.into_option().map(|cursor| ScanCursor(cursor.into()));
        self.run(move |transaction| {
            Box::pin(async move {
                let (rows, next) = transaction.scan(&namespace, cursor, limit as usize).await?;
                Ok(FfiScanPage {
                    rows: rows.into(),
                    next: next.map(|ScanCursor(cursor)| RVec::from(cursor)).into(),
                })
            })
        })
    }

    fn set_meta(&mut self, key: RString, value: RVec<u8>) -> FfiFuture<TableResult<()>> {
        self.run(move |transaction| {
            Box::pin(async move { transaction.set_meta(&key, value.into()).await })
        })
    }

    fn commit(self) -> FfiFuture<TableResult<()>> {
        let inner = Arc::clone(&self.inner);
        run(&self.runtime, async move {
            let transaction = inner
                .lock()
                .await
                .take()
                .ok_or_else(|| TableBackendError::fatal("the transaction already committed"))?;
            transaction.commit().await
        })
    }
}
