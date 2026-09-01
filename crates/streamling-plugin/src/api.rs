#![allow(dead_code)]
//! This module defines the API that can be used for implementing plugins.
//! NOTE: this API is NOT FFI-safe and is intended for use in the plugin AFTER the FFI layer.
//! See `plugin_interface::ffi` for the FFI-safe types and traits.
//!
//! # Shutdown and durability lifecycle (what a plugin must implement)
//!
//! Plugins do NOT observe process signals and there is no separate shutdown
//! API to implement. SIGTERM is handled once, by the host; what a plugin sees
//! is the same message protocol it already handles in steady state, delivered
//! in a guaranteed order. Handling that protocol correctly IS handling
//! shutdown:
//!
//! ```text
//! steady state:   NextBatch* → CheckpointMarker → [Finalizer] → NextBatch* → …
//! shutdown:       NextBatch* → CheckpointMarker → Finalizer → Terminate
//!                 (the last marker is the TERMINAL checkpoint — it looks
//!                  exactly like every other marker on purpose)
//! ```
//!
//! The rules, per hook:
//!
//! - **`process_checkpoint_marker` is the durability point — the only one.**
//!   For a sink, returning `Ok` acks the epoch, which tells the host that
//!   everything received up to this marker survives a crash. Flush buffered
//!   data durably BEFORE returning. Do not defer durability to `terminate()`:
//!   on graceful shutdown the terminal marker arrives after the last batch,
//!   so a plugin that flushes on markers never has unflushed acked data at
//!   exit. A plugin does not need to know (and is not told) whether a marker
//!   is terminal — treat every marker identically.
//! - **`process_checkpoint_finalizer` must be idempotent, non-blocking, and
//!   must never wait for a specific epoch** (see the method docs). Use it for
//!   commit-on-finalize bookkeeping only.
//! - **`terminate` / `is_running`** (`SupportsGracefulShutdown`): `terminate`
//!   flips your running flag and releases what is WORTH releasing (see
//!   below); the dispatcher then exits its loop. Data flushing should already
//!   have happened on the last marker — a best-effort flush here is cheap
//!   insurance, not the contract.
//! - **Never retry forever.** Any network call inside a handler needs a
//!   bounded retry budget or an `is_running()` check between attempts. The
//!   host bounds you regardless — after shutdown it abandons sends to a
//!   plugin that stops consuming (≈5s grace) and a watchdog hard-exits the
//!   process at the shutdown budget — but a plugin that wedges forfeits its
//!   own drain window.
//!
//! ## Does resource cleanup matter if the process is exiting anyway?
//!
//! Split resources in two:
//!
//! - **Local resources (memory, threads, file handles): no.** Process exit
//!   reclaims them; spending shutdown-budget seconds on them competes with
//!   the flush that actually matters. Do nothing.
//! - **Resources with REMOTE state: yes, close them if it is fast.** A
//!   connection pool with server-side sessions, a consumer-group membership,
//!   a lease, or an open transaction outlives the process on the remote end
//!   until it times out — which slows down the replacement pod (rebalance
//!   delays, held locks, connection-count pressure). A graceful close on
//!   `terminate()` releases them immediately. If closing is slow or flaky,
//!   skip it: the watchdog treats a slow `terminate()` the same as a hang.
//!
//! Priority order inside the shutdown budget: durability (marker acks) →
//! fast remote releases → exit. Anything slower than a second or two in
//! `terminate()` is usually a bug.

use crate::{PluginLabel, PluginStateBackendConfig};
use abi_stable::traits::IntoReprRust;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::io;
use std::sync::{Arc, RwLock};
use streamling_config::StateBackendConfig;
use streamling_state::{
    StateBackendError, StateBackendFactories, StateKey, StateOperatorBackend,
    StateOperatorBackendFactory,
};

pub static STREAMLING_COLUMN_NAME_OP: &str = "_gs_op";

pub struct PluginStateBackendFactory {
    factories: StateBackendFactories,
    application_namespace: String,
    plugin_reference_name: String,
}

impl PluginStateBackendFactory {
    pub fn new(config: PluginStateBackendConfig) -> Self {
        let state_backend_config: StateBackendConfig =
            serde_json::from_str(&config.serialized_config)
                .expect("Failed to deserialize StateBackendConfig");

        let factories = StateBackendFactories::new(state_backend_config)
            .expect("Failed to create State Backend Factory");

        PluginStateBackendFactory {
            factories,
            application_namespace: config.application_namespace.into_rust(),
            plugin_reference_name: config.plugin_reference_name.into_rust(),
        }
    }

    pub fn create<V>(&self) -> Arc<PluginStateBackend<V>>
    where
        V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Unpin + Clone + Debug + 'static,
    {
        let inner = self.factories.create(&self.application_namespace);
        Arc::new(PluginStateBackend::new(
            inner,
            self.plugin_reference_name.clone(),
        ))
    }
}

/// State backend for plugins.
/// - `get()` / `put(value)` use the default key: `{reference_name}`
/// - `get_kv(key)` / `put_kv(key, value)` use key: `{prefix}:{key}` (prefix defaults to reference_name)
/// - `set_prefix(None)` resets to default (reference_name)
/// - `set_prefix(Some("custom"))` sets prefix to "custom"
/// - `set_prefix(Some(""))` removes prefix (global state)
pub struct PluginStateBackend<V>
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + Debug + 'static,
{
    inner: Arc<dyn StateOperatorBackend<V>>,
    reference_name: String,
    kv_prefix: RwLock<Option<String>>,
}

impl<V> PluginStateBackend<V>
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + Debug + 'static,
{
    fn new(inner: Arc<dyn StateOperatorBackend<V>>, reference_name: String) -> Self {
        Self {
            inner,
            reference_name,
            kv_prefix: RwLock::new(None),
        }
    }

    fn default_key(&self) -> StateKey {
        StateKey(self.reference_name.clone())
    }

    fn build_kv_key(&self, key: &str) -> StateKey {
        let prefix = self.kv_prefix.read().unwrap();
        match prefix.as_ref() {
            None => StateKey(format!("{}:{}", self.reference_name, key)),
            Some(p) if p.is_empty() => StateKey(key.to_string()),
            Some(p) => StateKey(format!("{}:{}", p, key)),
        }
    }

    /// Set the prefix for `_kv` methods.
    /// - `None` -> reset to default, keys become `{reference_name}:{key}`
    /// - `Some("custom")` -> keys become `custom:{key}`
    /// - `Some("")` -> keys become `{key}` (global state, no prefix)
    pub fn set_prefix(&self, prefix: Option<&str>) {
        let mut p = self.kv_prefix.write().unwrap();
        *p = prefix.map(|s| s.to_string());
    }

    pub async fn get(&self) -> Result<Option<V>, StateBackendError> {
        self.inner.get(self.default_key()).await
    }

    pub async fn put(&self, value: V) -> Result<(), StateBackendError> {
        self.inner.put(self.default_key(), value).await
    }

    pub async fn remove(&self) -> Result<(), StateBackendError> {
        self.inner.remove(self.default_key()).await
    }

    pub async fn get_kv(&self, key: &str) -> Result<Option<V>, StateBackendError> {
        self.inner.get(self.build_kv_key(key)).await
    }

    pub async fn put_kv(&self, key: &str, value: V) -> Result<(), StateBackendError> {
        self.inner.put(self.build_kv_key(key), value).await
    }

    pub async fn remove_kv(&self, key: &str) -> Result<(), StateBackendError> {
        self.inner.remove(self.build_kv_key(key)).await
    }

    /// Clear the state for the current reference_name (removes the default key)
    pub async fn clear(&self) -> Result<(), StateBackendError> {
        self.inner.remove(self.default_key()).await
    }
}

impl<V> Debug for PluginStateBackend<V>
where
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + Debug + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = self.kv_prefix.read().unwrap();
        f.debug_struct("PluginStateBackend")
            .field("reference_name", &self.reference_name)
            .field("kv_prefix", &prefix)
            .finish()
    }
}

#[derive(Debug)]
pub enum PluginError {
    ArrowError(ArrowError),
    IoError(io::Error),
    Internal(String),
    Execution(String),
    State(StateBackendError),
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArrowError(e) => write!(f, "{e}"),
            Self::IoError(e) => write!(f, "{e}"),
            Self::Internal(msg) => f.write_str(msg),
            Self::Execution(msg) => f.write_str(msg),
            Self::State(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PluginError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ArrowError(e) => Some(e),
            Self::IoError(e) => Some(e),
            Self::State(e) => Some(e),
            Self::Internal(_) | Self::Execution(_) => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct CheckpointEpoch(pub u64);

#[async_trait]
pub trait SupportsGracefulShutdown {
    /// Returns true if the plugin is still running.
    fn is_running(&self) -> bool;

    /// Attempts to gracefully shut down the plugin.
    async fn terminate(&self) -> Result<(), PluginError>;
}

/// Optional, non-FFI trait for the plugins to implement preprocessor support.
/// Preprocessors transform the raw topology config string before parsing.
#[async_trait]
pub trait PreprocessorPlugin: Send + Sync {
    async fn preprocess_topology(&self, config: String) -> Result<String, PluginError>;
}

/// Optional, non-FFI trait for the plugins to implement source support.
#[async_trait]
pub trait SourcePlugin: SupportsGracefulShutdown + Send + Sync {
    async fn initialize(&self) -> Result<(), PluginError>;
    fn output_schema(&self) -> Result<SchemaRef, PluginError>;
    /// Identity labels for this plugin instance. Typically derived from options at
    /// construction time (e.g. `chain_slug`, `network`, `topic`). Returned labels flow
    /// through `PluginResult::labels` into the metrics subsystem as Prometheus labels.
    /// Default: no labels.
    fn labels(&self) -> Vec<PluginLabel> {
        Vec::new()
    }
    /// Return an empty batch to indicate missing data if needed.
    async fn generate_batch(&self) -> Result<RecordBatch, PluginError>;
    /// Returning a successful result indicates that the checkpoint marker was processed
    /// successfully, and the mark should be propagated downstream.
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
    /// Called when `epoch` finalized end-to-end (every live sink acked it).
    /// Contract: MUST be idempotent, MUST NOT block, and MUST NEVER wait for
    /// one specific epoch's Finalizer — at shutdown the host drops in-flight
    /// timer epochs without ever sending their Finalizers, and only a later
    /// terminal epoch (covering their work) finalizes. An implementation that
    /// gates on an exact epoch will stall the shutdown drain.
    async fn process_checkpoint_finalizer(&self, epoch: CheckpointEpoch)
    -> Result<(), PluginError>;
}

/// Optional, non-FFI trait for the plugins to implement transform support.
#[async_trait]
pub trait TransformPlugin: SupportsGracefulShutdown + Send + Sync {
    async fn initialize(&self) -> Result<(), PluginError>;
    fn output_schema(&self) -> Result<SchemaRef, PluginError>;
    /// See `SourcePlugin::labels`.
    fn labels(&self) -> Vec<PluginLabel> {
        Vec::new()
    }
    /// Return an empty batch to indicate missing data if needed.
    async fn process_batch(&self, data: RecordBatch) -> Result<RecordBatch, PluginError>;
    /// Returning a successful result indicates that the checkpoint marker was processed
    /// successfully, and the mark should be propagated downstream.
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
    /// See `SourcePlugin::process_checkpoint_finalizer` for the consumer
    /// contract (idempotent, non-blocking, never wait for a specific epoch).
    async fn process_checkpoint_finalizer(&self, epoch: CheckpointEpoch)
    -> Result<(), PluginError>;
}

/// Optional, non-FFI trait for the plugins to implement sink support.
#[async_trait]
pub trait SinkPlugin: SupportsGracefulShutdown + Send + Sync {
    async fn initialize(&self) -> Result<(), PluginError>;
    /// See `SourcePlugin::labels`.
    fn labels(&self) -> Vec<PluginLabel> {
        Vec::new()
    }
    async fn process_batch(&self, data: RecordBatch) -> Result<(), PluginError>;
    /// Returning a successful result indicates that the checkpoint marker was processed
    /// successfully, and an acknowledgment should be sent back to the source.
    /// For a sink, "processed successfully" means DURABLY flushed: the ack
    /// tells the source everything up to this marker survives a crash, so
    /// buffered-but-unpublished data must be flushed before returning `Ok`.
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError>;
    /// See `SourcePlugin::process_checkpoint_finalizer` for the consumer
    /// contract (idempotent, non-blocking, never wait for a specific epoch).
    async fn process_checkpoint_finalizer(&self, epoch: CheckpointEpoch)
    -> Result<(), PluginError>;
}

/// Trait for plugins to implement side output support.
/// Side outputs observe data from all sources without modifying the pipeline.
/// Unlike other plugin types, side outputs use direct FFI invocation (no channels).
/// One instance is created per source via `new(source_name, schema, options, metrics_recorder)`.
pub trait SideOutputPlugin: Send + Sync {
    fn process_batch(&self, batch: &RecordBatch) -> Result<(), String>;
    fn shutdown(&self);
}
