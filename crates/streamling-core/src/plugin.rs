pub mod diagnostics;
pub mod operator;
mod preprocessor;
pub mod side_output;
pub mod table_provider;
mod telemetry;
pub mod udf;

pub use preprocessor::build_plugin_preprocessors;

use crate::app_config::AppConfig;
use crate::data::COLUMN_NAME_OP;
use crate::error::Result;
use crate::telemetry::provider::metric_key;
use crate::telemetry::recorder::merge_metadata_tags;
use crate::{streamling_err, streamling_user_bail, streamling_user_err};
use abi_stable::StableAbi;
use abi_stable::derive_macro_reexports::{NonExhaustive, TD_Opaque};
use abi_stable::external_types::crossbeam_channel;
use abi_stable::library::lib_header_from_path;
use abi_stable::std_types::{RDuration, RErr, RNone, ROk, RResult, RSome};
use abi_stable::traits::{IntoReprC, IntoReprRust};
use arrow_schema::SchemaRef;
use async_ffi::{FfiFuture, FutureExt as AsyncFfiFutureExt};
use futures::FutureExt;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use streamling_plugin::r#async::{
    PluginAsyncRuntime, PluginAsyncRuntime_TO, PluginAsyncRuntimeObj,
};
pub(crate) use streamling_plugin::ffi::IDLE_POLL_INTERVAL;
use streamling_plugin::ffi::PluginMetricsChannel;
pub use streamling_plugin::{
    PluginChannel, PluginChannelCaps, PluginChannels, PluginLabel, PluginLogging, PluginModuleRef,
    PluginMsg, PluginOptions, PluginStateBackendConfig,
};
use tokio::runtime::Handle;
use tracing::{error, info, warn};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PluginId {
    namespace: Option<String>,
    id: String,
}

impl fmt::Display for PluginId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.namespace {
            Some(namespace) => write!(f, "{}.{}", namespace, self.id),
            None => write!(f, "{}", self.id),
        }
    }
}

impl From<String> for PluginId {
    fn from(value: String) -> Self {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() == 1 {
            PluginId {
                namespace: None,
                id: parts[0].to_string(),
            }
        } else {
            PluginId {
                namespace: Some(parts[0].to_string()),
                id: parts[1..].join("."),
            }
        }
    }
}

impl From<PluginId> for String {
    fn from(plugin_id: PluginId) -> Self {
        plugin_id.to_string()
    }
}

// Registry to store initialized plugin modules
lazy_static! {
    static ref PLUGIN_MODULE_REGISTRY: RwLock<HashMap<PluginId, Arc<PluginModuleRef>>> =
        RwLock::new(HashMap::new());
    static ref PLUGIN_DEFAULT_CAPS: RwLock<HashMap<PluginId, PluginChannelCaps>> =
        RwLock::new(HashMap::new());
    static ref PLUGIN_INSTANCE_REGISTRY: RwLock<HashMap<String, PluginChannels>> =
        RwLock::new(HashMap::new());
}

fn register_plugin_instance(instance_key: &str, channels: PluginChannels) {
    // Keep a clone of the metrics receiver for the shutdown watchdog: at
    // watchdog time the async metrics forwarder may be long past its last
    // poll, and the crossbeam receiver clone lets the dump drain the
    // dispatcher's final liveness markers without a runtime.
    diagnostics::register_diag_receiver(instance_key, channels.metrics.receiver.clone());
    let mut reg = PLUGIN_INSTANCE_REGISTRY.write().unwrap();
    reg.insert(instance_key.to_string(), channels);
}

/// `send_budget` bounds the TOTAL time spent signalling Terminate across all
/// plugins: each send gets an equal slice of it (floored at 100ms so a healthy
/// plugin always gets a real chance, capped at the legacy 5s). `None` keeps
/// the legacy 5s-per-plugin bound — fine for one plugin, but N wedged plugins
/// serialize to N×5s, which overruns the shutdown budget from N=5; shutdown
/// paths with a deadline should pass their remaining budget instead.
pub fn terminate_all_plugins(send_budget: Option<Duration>) -> Result<()> {
    info!("Terminating all plugins");
    let mut reg = PLUGIN_INSTANCE_REGISTRY.write().unwrap();
    if reg.is_empty() {
        return Ok(());
    }
    let ids: Vec<(String, PluginChannels)> = reg.drain().collect();
    drop(reg);
    terminate_plugins(ids, send_budget)
}

/// Non-blocking, panic-safe variant of [`terminate_all_plugins`] for contexts
/// that must never park — the global panic hook in particular. A panic while a
/// plugin's input channel is full would otherwise block the panicking thread
/// on the bounded send (up to the timeout, per plugin), and a wedged panic
/// hook leaves a process that neither crashes nor exits. Best-effort: a plugin whose channel is
/// full simply doesn't get the Terminate — process death is the backstop.
/// Uses `eprintln!` rather than `tracing`/`Result` so it can run safely inside
/// the hook.
pub fn terminate_all_plugins_nonblocking() {
    let mut reg = match PLUGIN_INSTANCE_REGISTRY.write() {
        Ok(guard) => guard,
        // Poisoned by the very panic we're hooking: take the inner value —
        // unwrapping here would double-panic and abort before any output.
        Err(poisoned) => poisoned.into_inner(),
    };
    if reg.is_empty() {
        return;
    }
    let ids: Vec<(String, PluginChannels)> = reg.drain().collect();
    drop(reg);
    for (plugin_id, channels) in ids {
        if let Err(e) = channels
            .input
            .sender
            .try_send(NonExhaustive::new(PluginMsg::Terminate))
        {
            eprintln!(
                "panic hook: could not signal Terminate to plugin {} (channel full or closed): {:?}",
                plugin_id, e
            );
        }
    }
}

#[repr(transparent)]
#[derive(StableAbi, Clone)]
struct PluginTokioWrapper {
    #[sabi(unsafe_opaque_field)]
    inner: Handle,
}

/// This async runtime is created on the host side and passed to the plugin,
/// allowing it to spawn futures, sleep, and block.
impl PluginAsyncRuntime for PluginTokioWrapper {
    fn spawn(&self, fut: FfiFuture<()>) -> FfiFuture<()> {
        // The sanctioned bridge itself: this IS the spawn surface handed to
        // legacy (shared-runtime) plugin libraries, so it cannot route through
        // a scope without moving plugin tasks onto host drain stages.
        #[allow(clippy::disallowed_methods)]
        let handle = self.inner.spawn(fut);
        async move {
            // This await is the ONLY observer of the task's JoinError. Mapping
            // it away silently turned a panicked plugin task (e.g. a source's
            // generate loop) into an unexplained wedge — the dispatcher keeps
            // looping while the work it awaited never happened.
            if let Err(e) = handle.await {
                error!("Plugin task panicked or was cancelled: {e}");
            }
        }
        .into_ffi()
    }

    fn sleep(&self, dur: RDuration) -> FfiFuture<()> {
        async move { tokio::time::sleep(dur.into_rust()).await }.into_ffi()
    }

    fn timeout(&self, dur: RDuration, fut: FfiFuture<()>) -> FfiFuture<RResult<(), ()>> {
        async move {
            match tokio::time::timeout(dur.into_rust(), fut).await {
                Ok(_) => ROk(()),
                Err(_) => RErr(()),
            }
        }
        .into_ffi()
    }

    fn block_on(&self, fut: FfiFuture<()>) {
        self.inner.block_on(fut);
    }

    fn yield_now(&self) -> FfiFuture<()> {
        async move {
            tokio::task::yield_now().await;
        }
        .into_ffi()
    }
}

pub fn load_and_initialize_plugins(app_config: &AppConfig) -> Result<()> {
    if let Some(ref path) = app_config.plugin.path {
        if path.is_empty() {
            return Ok(());
        }

        let plugin_path = Path::new(path);

        if !plugin_path.exists() {
            streamling_user_bail!("Plugin path '{}' does not exist", path);
        }

        if plugin_path.is_file() {
            // If the path is a file, just load that specific plugin file
            load_and_initialize_plugin(path, app_config)?;
        } else if plugin_path.is_dir() {
            // If the path is a directory, load all plugin files in the directory
            for entry in std::fs::read_dir(plugin_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    load_and_initialize_plugin(path.to_str().unwrap(), app_config)?
                }
            }
        } else {
            streamling_user_bail!("Plugin path '{}' is neither a file nor a directory", path);
        }
    } else {
        info!("No plugin path specified in the configuration, skipping plugin loading.");
    }

    Ok(())
}

/// Load the root module from a plugin library, tolerating libraries built
/// against an older SDK whose `PluginModule` has fewer (suffix) fields.
///
/// abi_stable's layout check is one-directional: a library with MORE module
/// fields than the host expects passes, but one with FEWER is rejected with a
/// `FieldCountMismatch` even when the missing fields sit after
/// `last_prefix_field` — the runtime `Option` accessors never get a chance.
/// So on a primary-check failure, re-validate the library against the frozen
/// four-field twin (`streamling_plugin::compat`); if that passes, the shared
/// prefix is proven intact and loading the primary ref with the layout check
/// skipped is sound — every suffix accessor past the library's own recorded
/// field count returns `None` via abi_stable's runtime field guard, which is
/// exactly the degraded-but-bounded path the call sites already handle.
fn load_plugin_module(plugin_path: &Path) -> Result<PluginModuleRef> {
    let header = lib_header_from_path(plugin_path)
        .map_err(|e| streamling_err!("Unable to read plugin library {:?}: {}", plugin_path, e))?;

    match header.init_root_module::<PluginModuleRef>() {
        Ok(module) => Ok(module),
        Err(primary_err) => {
            header
                .init_root_module::<streamling_plugin::compat::PluginModuleRef>()
                .map_err(|compat_err| {
                    streamling_err!(
                        "Unable to load plugin from {:?}: not a compatible plugin module \
                         (current-ABI check: {}; frozen-ABI check: {})",
                        plugin_path,
                        primary_err,
                        compat_err
                    )
                })?;

            info!(
                "Plugin library predates the current module ABI; loading in \
                 compatibility mode (newer capabilities report as absent): {:?}",
                plugin_path
            );

            // SAFETY: the compat probe above validated the library's module
            // against the frozen four-field layout, which is a prefix of the
            // primary layout; suffix-field access is guarded at runtime by
            // the library's own recorded field count.
            unsafe { header.init_root_module_with_unchecked_layout::<PluginModuleRef>() }.map_err(
                |e| {
                    streamling_err!(
                        "Unable to load plugin from {:?} in compatibility mode: {}",
                        plugin_path,
                        e
                    )
                },
            )
        }
    }
}

pub fn load_and_initialize_plugin(path: &str, app_config: &AppConfig) -> Result<()> {
    let plugin_path = Path::new(path);
    info!("Loading plugin from: {:?}", plugin_path);

    let plugin_module = Arc::new(load_plugin_module(plugin_path)?);

    let logging_config = create_logging(app_config);
    let init_fn = plugin_module.init();
    let init_result = init_fn(logging_config);

    let plugin_runtime_configuration = init_result
        .into_rust()
        .map_err(|e| streamling_err!("Plugin initialization failed: {:?}", e))?;

    let mut module_registry = PLUGIN_MODULE_REGISTRY.write().unwrap();
    let mut caps_registry = PLUGIN_DEFAULT_CAPS.write().unwrap();

    // Register the plugin module for each plugin ID it provides
    for plugin_id in plugin_runtime_configuration.plugin_ids.into_iter() {
        let plugin_id_string = plugin_id.into_rust();
        let plugin_id_key: PluginId = plugin_id_string.clone().into();
        module_registry.insert(plugin_id_key.clone(), plugin_module.clone());

        // If init provided default caps for this id, store them
        if let Some(caps) = plugin_runtime_configuration
            .default_channel_caps
            .get(plugin_id_string.as_str())
        {
            caps_registry.insert(plugin_id_key, *caps);
        }
    }

    // Hand the library an out-of-band shutdown signal. The accessor returns
    // None for libraries built against an older SDK (the field sits after
    // `last_prefix_field`), in which case the SDK falls back to its finite
    // defaults — degraded, never unbounded.
    match plugin_module.set_shutdown_signal() {
        Some(set_signal) => {
            set_signal(create_shutdown_signal());
            info!("Installed shutdown signal for plugin: {:?}", path);
        }
        None => info!(
            "Plugin library predates the shutdown signal; SDK defaults apply: {:?}",
            path
        ),
    }

    // Load UDF descriptors if the plugin provides them
    if let Some(udf_descriptors_fn) = plugin_module.udf_descriptors() {
        match udf_descriptors_fn() {
            ROk(descriptors) => {
                info!(
                    "Loaded {} UDF(s) from plugin: {:?}",
                    descriptors.len(),
                    path
                );
                udf::store_plugin_udfs(descriptors);
            }
            RErr(e) => {
                streamling_user_bail!("Failed to load UDF descriptors from {:?}: {:?}", path, e);
            }
        }
    }

    // Load side output descriptors if the plugin provides them
    if let Some(side_output_descriptors_fn) = plugin_module.side_output_descriptors() {
        match side_output_descriptors_fn() {
            ROk(descriptors) => {
                info!(
                    "Loaded {} side output(s) from plugin: {:?}",
                    descriptors.len(),
                    path
                );
                side_output::store_plugin_side_outputs(descriptors);
            }
            RErr(e) => {
                streamling_user_bail!(
                    "Failed to load side output descriptors from {:?}: {:?}",
                    path,
                    e
                );
            }
        }
    }

    Ok(())
}

fn find_plugin(plugin_id: &PluginId) -> Option<Arc<PluginModuleRef>> {
    let module_registry = PLUGIN_MODULE_REGISTRY.read().unwrap();
    module_registry.get(plugin_id).cloned()
}

/// Returns all loaded plugin ids in a stable order.
fn registered_plugin_ids() -> Vec<String> {
    let module_registry = PLUGIN_MODULE_REGISTRY
        .read()
        .expect("plugin module registry lock poisoned");
    let mut ids: Vec<String> = module_registry.keys().map(|id| id.to_string()).collect();
    ids.sort();
    ids
}

/// Resolves a plugin module and includes the loaded ids in any error.
fn require_plugin(plugin_id: &PluginId) -> Result<Arc<PluginModuleRef>> {
    find_plugin(plugin_id).ok_or_else(|| {
        streamling_user_err!(
            "plugin '{}' is not available; check that the plugin type is correct and that the plugin bundle is installed. Registered plugin ids: [{}]",
            plugin_id,
            registered_plugin_ids().join(", ")
        )
    })
}

fn create_plugin_async_runtime(handle: Handle) -> PluginAsyncRuntimeObj {
    // `TD_Opaque` chooses `RBox<()>` for the erased-pointer parameter
    PluginAsyncRuntime_TO::from_value(PluginTokioWrapper { inner: handle }, TD_Opaque)
}

/// Host-side shutdown signal handed to each plugin library at load time via
/// `PluginModule::set_shutdown_signal`. Bridges the process-global shutdown
/// watch and drain budget across the FFI boundary — the plugin's own copy of
/// those statics can never observe the host's (separate dylib statics), so
/// the handle must be passed explicitly.
#[derive(Clone)]
struct HostShutdownSignal;

impl streamling_plugin::shutdown::ShutdownSignal for HostShutdownSignal {
    fn is_shutting_down(&self) -> bool {
        *crate::shutdown::subscribe().borrow()
    }

    fn cancelled(&self) -> FfiFuture<()> {
        async {
            let mut rx = crate::shutdown::subscribe();
            loop {
                if *rx.borrow_and_update() {
                    return;
                }
                if rx.changed().await.is_err() {
                    // The sender is a process-global static that never drops
                    // in production; park rather than resolve spuriously.
                    std::future::pending::<()>().await;
                }
            }
        }
        .into_ffi()
    }

    fn remaining_budget_ms(&self) -> u64 {
        u64::try_from(crate::shutdown::remaining_budget().as_millis()).unwrap_or(u64::MAX)
    }

    fn request_shutdown(&self) {
        crate::shutdown::request_shutdown();
    }
}

fn create_shutdown_signal() -> streamling_plugin::shutdown::ShutdownSignalObj {
    streamling_plugin::shutdown::ShutdownSignal_TO::from_value(HostShutdownSignal, TD_Opaque)
}

fn create_plugin_state_backend_config(
    app_config: &AppConfig,
    reference_name: &str,
) -> PluginStateBackendConfig {
    let serialized_state_backend_config = serde_json::to_string(&app_config.state_backend)
        .expect("Failed to serialize state backend config");

    PluginStateBackendConfig::new(
        app_config.state_backend_namespace().to_string(),
        reference_name.to_string(),
        serialized_state_backend_config,
    )
}

fn create_channels_with_caps(input: usize, output: usize, metrics: usize) -> PluginChannels {
    PluginChannels {
        input: PluginChannel::new(crossbeam_channel::bounded(input)),
        output: PluginChannel::new(crossbeam_channel::bounded(output)),
        metrics: PluginMetricsChannel::new(crossbeam_channel::bounded(metrics)),
    }
}

/// Default capacity for the plugin→host metrics channel.
/// Distinct from `plugin.channel_capacity` (data-plane backpressure, default 50).
/// Plugins emit with non-blocking `try_send`; a full channel drops the sample
/// (`Encountered error dispatching metrics`) rather than applying backpressure.
/// A `0` metrics cap used to fall back to that 50 and dropped samples on
/// high-rate plugins; this default is the metrics fallback instead.
pub const DEFAULT_PLUGIN_METRICS_CHANNEL_CAPACITY: usize = 4096;

/// Resolve per-plugin channel sizes. A `0` cap means "use the default":
/// input/output fall back to the data-plane default, metrics fall back to
/// the metrics default so a tight output buffer (e.g. Solana's 1) does not
/// also shrink the metrics channel.
fn resolve_channel_caps(
    data_default: usize,
    metrics_default: usize,
    caps: Option<PluginChannelCaps>,
) -> (usize, usize, usize) {
    let Some(caps) = caps else {
        return (data_default, data_default, metrics_default);
    };
    let to_sz = |v: u32, fallback: usize| {
        if v == 0 { fallback } else { v as usize }
    };
    (
        to_sz(caps.input, data_default),
        to_sz(caps.output, data_default),
        to_sz(caps.metrics, metrics_default),
    )
}

fn create_channels_for_plugin(app_config: &AppConfig, plugin_type: &PluginId) -> PluginChannels {
    let default = app_config.plugin.channel_capacity as usize;
    let caps_registry = PLUGIN_DEFAULT_CAPS.read().unwrap();
    let (input, output, metrics) = resolve_channel_caps(
        default,
        DEFAULT_PLUGIN_METRICS_CHANNEL_CAPACITY,
        caps_registry.get(plugin_type).copied(),
    );
    create_channels_with_caps(input, output, metrics)
}

/// Slice for bounded sends into plugin input channels: short enough that a
/// shutdown flip is observed promptly, long enough not to spin.
const PLUGIN_SEND_SLICE: Duration = Duration::from_millis(100);
/// How long a full plugin input channel may keep refusing a message AFTER
/// process shutdown was requested before the host abandons the send.
const PLUGIN_SHUTDOWN_SEND_GRACE: Duration = Duration::from_secs(5);
/// Bound for the sync [`send_to_plugin_blocking`] variant (used for `Init`
/// on a freshly created, near-empty channel — hitting this bound at all
/// means the dispatcher never started consuming).
const PLUGIN_BLOCKING_SEND_BOUND: Duration = Duration::from_secs(5);

/// Shutdown-aware replacement for a blocking crossbeam `send().unwrap()` into
/// a plugin's input channel.
///
/// A full input channel in steady state is ordinary backpressure — a slow
/// plugin is SUPPOSED to slow the pipeline — so before shutdown this waits
/// indefinitely, in short bounded slices instead of one indefinite park. Once
/// shutdown is requested, a plugin that still refuses the message after
/// [`PLUGIN_SHUTDOWN_SEND_GRACE`] is treated as wedged and the send is
/// abandoned with an error, so the caller's drain can wind down instead of
/// holding a runtime worker hostage until the watchdog hard-exits. A
/// disconnected channel (dispatcher already exited) errors immediately — the
/// `.unwrap()`s this replaces panicked the host there.
///
/// On a multi-thread runtime each blocking slice runs under `block_in_place`
/// so sibling tasks migrate off the worker; on a current-thread runtime
/// (unit tests) it blocks directly, bounded by the slice.
pub async fn send_to_plugin<T>(
    sender: &crossbeam_channel::RSender<T>,
    msg: T,
    plugin_id: &str,
) -> Result<()> {
    let rx = crate::shutdown::subscribe();
    send_to_plugin_with(sender, msg, plugin_id, move || *rx.borrow(), {
        PLUGIN_SHUTDOWN_SEND_GRACE
    })
    .await
}

/// [`send_to_plugin`] with the shutdown signal and grace injected, so tests
/// can exercise the abandon path without flipping the process-wide watch
/// (which would contaminate every later test in the binary).
async fn send_to_plugin_with<T>(
    sender: &crossbeam_channel::RSender<T>,
    msg: T,
    plugin_id: &str,
    is_shutting_down: impl Fn() -> bool,
    shutdown_grace: Duration,
) -> Result<()> {
    use crossbeam::channel::{SendTimeoutError, TrySendError};

    // Fast path: room in the channel (the common case).
    let mut msg = match sender.try_send(msg) {
        Ok(()) => return Ok(()),
        Err(TrySendError::Full(m)) => m,
        Err(TrySendError::Disconnected(_)) => return Err(plugin_channel_closed(plugin_id)),
    };

    let block_in_place_ok = Handle::try_current()
        .map(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
        .unwrap_or(false);
    let mut grace_deadline: Option<std::time::Instant> = None;
    loop {
        let res = if block_in_place_ok {
            tokio::task::block_in_place(|| sender.send_timeout(msg, PLUGIN_SEND_SLICE))
        } else {
            sender.send_timeout(msg, PLUGIN_SEND_SLICE)
        };
        msg = match res {
            Ok(()) => return Ok(()),
            Err(SendTimeoutError::Timeout(m)) => m,
            Err(SendTimeoutError::Disconnected(_)) => {
                return Err(plugin_channel_closed(plugin_id));
            }
        };
        if is_shutting_down() {
            let deadline =
                *grace_deadline.get_or_insert_with(|| std::time::Instant::now() + shutdown_grace);
            if std::time::Instant::now() >= deadline {
                return Err(streamling_err!(
                    "plugin '{}' input channel still full {:?} after shutdown was requested; \
                     abandoning the send so the drain can proceed (dispatcher presumed wedged)",
                    plugin_id,
                    shutdown_grace
                ));
            }
        }
        // Yield between slices so a current-thread runtime can run the tasks
        // that would drain this channel.
        tokio::task::yield_now().await;
    }
}

/// Sync-context variant of [`send_to_plugin`] for the two `Init` sends that
/// happen inside DataFusion's non-async `execute()`. `Init` targets a fresh,
/// near-empty channel, so the fixed [`PLUGIN_BLOCKING_SEND_BOUND`] is
/// generous — reaching it means the dispatcher never started consuming, and
/// erroring beats the panic (`unwrap`) and the unbounded park this replaces.
pub fn send_to_plugin_blocking<T>(
    sender: &crossbeam_channel::RSender<T>,
    msg: T,
    plugin_id: &str,
) -> Result<()> {
    use crossbeam::channel::SendTimeoutError;

    match sender.send_timeout(msg, PLUGIN_BLOCKING_SEND_BOUND) {
        Ok(()) => Ok(()),
        Err(SendTimeoutError::Timeout(_)) => Err(streamling_err!(
            "plugin '{}' did not accept a message within {:?} of pipeline start \
             (dispatcher not consuming)",
            plugin_id,
            PLUGIN_BLOCKING_SEND_BOUND
        )),
        Err(SendTimeoutError::Disconnected(_)) => Err(plugin_channel_closed(plugin_id)),
    }
}

fn plugin_channel_closed(plugin_id: &str) -> crate::error::StreamlingError {
    streamling_err!(
        "plugin '{}' input channel is closed (dispatcher exited); cannot deliver message",
        plugin_id
    )
}

fn create_logging(app_config: &AppConfig) -> PluginLogging {
    if app_config.log_format == "json" {
        PluginLogging::Json
    } else {
        PluginLogging::Plain
    }
}

pub type ExecutionFuture =
    Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send + 'static>>;

pub struct InitializedPlugin {
    pub plugin_id: String,
    pub execution_future: ExecutionFuture,
    pub channels: PluginChannels,
    /// Expected to be defined for sources and transforms, but not for sinks.
    pub output_schema: Option<SchemaRef>,
}

impl InitializedPlugin {
    pub fn new(
        plugin_id: String,
        execution_future: ExecutionFuture,
        channels: PluginChannels,
        output_schema: Option<SchemaRef>,
    ) -> Result<Self> {
        if let Some(schema) = &output_schema
            && schema.field_with_name(COLUMN_NAME_OP).is_err()
        {
            streamling_user_bail!("Output schema must contain the column '{}'", COLUMN_NAME_OP);
        }

        Ok(InitializedPlugin {
            plugin_id,
            execution_future,
            channels,
            output_schema,
        })
    }
}

/// Convert plugin-returned labels into the `(String, String)` shape expected by
/// `merge_metadata_tags`. The FFI types are `RString`; the registry holds owned `String`s.
fn collect_labels(labels: abi_stable::std_types::RVec<PluginLabel>) -> Vec<(String, String)> {
    labels
        .into_iter()
        .map(|l| (l.key.to_string(), l.value.to_string()))
        .collect()
}

pub fn create_source_plugin(
    app_config: &AppConfig,
    reference_name: String,
    plugin_type: String,
    options: HashMap<String, String>,
) -> Result<InitializedPlugin> {
    let plugin_type: PluginId = plugin_type.into();
    let plugin_module = require_plugin(&plugin_type)?;
    let plugin_async_runtime = create_plugin_async_runtime(Handle::current());
    let plugin_state_backend_config =
        create_plugin_state_backend_config(app_config, &reference_name);
    let plugin_channels = create_channels_for_plugin(app_config, &plugin_type);

    let create_fn = plugin_module.create();
    let create_result = create_fn(
        plugin_type.to_string().into_c(),
        RNone,
        PluginOptions::new(options),
        plugin_async_runtime,
        plugin_state_backend_config,
        plugin_channels.clone(),
    );

    let result = create_result
        .into_rust()
        .map_err(|e| streamling_err!("Plugin creation failed: {:?}", e))?;
    // Validate schema before registering so a missing/invalid output schema does
    // not leave a half-initialized entry in the plugin instance registry.
    let output_schema = result.output_schema.into_option().ok_or_else(|| {
        streamling_err!(
            "source plugin '{}' must provide an output schema (plugin invariant)",
            plugin_type
        )
    })?;
    let labels = collect_labels(result.labels);
    let mapped_future = result
        .execution_future
        .map(|r| r.into_rust().map_err(|msg| msg.into_string()));
    let initialized = InitializedPlugin::new(
        plugin_type.to_string(),
        Box::pin(mapped_future),
        plugin_channels.clone(),
        Some(output_schema.into()),
    )?;
    register_plugin_instance(reference_name.as_str(), plugin_channels);
    merge_metadata_tags(
        &metric_key(&app_config.application_id, &reference_name),
        labels,
    );
    Ok(initialized)
}

pub fn create_transform_plugin(
    app_config: &AppConfig,
    reference_name: String,
    plugin_type: String,
    options: HashMap<String, String>,
    input_schema: SchemaRef,
) -> Result<InitializedPlugin> {
    let plugin_type: PluginId = plugin_type.into();
    let plugin_module = require_plugin(&plugin_type)?;
    let plugin_async_runtime = create_plugin_async_runtime(Handle::current());
    let plugin_state_backend_config =
        create_plugin_state_backend_config(app_config, &reference_name);
    let plugin_channels = create_channels_for_plugin(app_config, &plugin_type);

    let create_fn = plugin_module.create();
    let create_result = create_fn(
        plugin_type.to_string().into_c(),
        RSome(input_schema.into()),
        PluginOptions::new(options),
        plugin_async_runtime,
        plugin_state_backend_config,
        plugin_channels.clone(),
    );

    let result = create_result
        .into_rust()
        .map_err(|e| streamling_err!("Plugin creation failed: {:?}", e))?;
    // Validate schema before registering so a missing/invalid output schema does
    // not leave a half-initialized entry in the plugin instance registry.
    let output_schema = result.output_schema.into_option().ok_or_else(|| {
        streamling_err!(
            "transform plugin '{}' must provide an output schema (plugin invariant)",
            plugin_type
        )
    })?;
    let labels = collect_labels(result.labels);
    let mapped_future = result
        .execution_future
        .map(|r| r.into_rust().map_err(|msg| msg.into_string()));
    let initialized = InitializedPlugin::new(
        plugin_type.to_string(),
        Box::pin(mapped_future),
        plugin_channels.clone(),
        Some(output_schema.into()),
    )?;
    register_plugin_instance(reference_name.as_str(), plugin_channels);
    merge_metadata_tags(
        &metric_key(&app_config.application_id, &reference_name),
        labels,
    );
    Ok(initialized)
}

pub fn create_sink_plugin(
    app_config: &AppConfig,
    reference_name: String,
    plugin_type: String,
    options: HashMap<String, String>,
    input_schema: SchemaRef,
) -> Result<InitializedPlugin> {
    let plugin_type: PluginId = plugin_type.into();
    let plugin_module = require_plugin(&plugin_type)?;
    let plugin_async_runtime = create_plugin_async_runtime(Handle::current());
    let plugin_state_backend_config =
        create_plugin_state_backend_config(app_config, &reference_name);
    let plugin_channels = create_channels_for_plugin(app_config, &plugin_type);

    let create_fn = plugin_module.create();
    let create_result = create_fn(
        plugin_type.to_string().into_c(),
        RSome(input_schema.into()),
        PluginOptions::new(options),
        plugin_async_runtime,
        plugin_state_backend_config,
        plugin_channels.clone(),
    );

    let result = create_result
        .into_rust()
        .map_err(|e| streamling_err!("Plugin creation failed: {:?}", e))?;
    // Match source/transform: only register after InitializedPlugin::new succeeds
    // so a future sink-schema invariant cannot leave a half-initialized registry entry.
    let labels = collect_labels(result.labels);
    let mapped_future = result
        .execution_future
        .map(|r| r.into_rust().map_err(|msg| msg.into_string()));
    let initialized = InitializedPlugin::new(
        plugin_type.to_string(),
        Box::pin(mapped_future),
        plugin_channels.clone(),
        None,
    )?;
    register_plugin_instance(reference_name.as_str(), plugin_channels);
    merge_metadata_tags(
        &metric_key(&app_config.application_id, &reference_name),
        labels,
    );
    Ok(initialized)
}

pub fn create_preprocessor_plugin(
    app_config: &AppConfig,
    reference_name: String,
    plugin_type: String,
    options: HashMap<String, String>,
) -> Result<InitializedPlugin> {
    let plugin_type: PluginId = plugin_type.into();
    let plugin_module = require_plugin(&plugin_type)?;
    let plugin_async_runtime = create_plugin_async_runtime(Handle::current());
    let plugin_state_backend_config =
        create_plugin_state_backend_config(app_config, &reference_name);
    let plugin_channels = create_channels_with_caps(1, 1, 1);

    let create_fn = plugin_module.create();
    let create_result = create_fn(
        plugin_type.to_string().into_c(),
        RNone,
        PluginOptions::new(options),
        plugin_async_runtime,
        plugin_state_backend_config,
        plugin_channels.clone(),
    );

    // Preprocessors are short-lived helpers: they are not registered in
    // PLUGIN_INSTANCE_REGISTRY and do not start the process-wide shutdown watcher
    // (unlike source/transform/sink). FFI create failures are internal/platform.
    // Construct the struct directly (no InitializedPlugin::new): preprocessors have
    // no output schema, so the op-column check in `new` does not apply and init
    // remains infallible after a successful FFI create.
    let result = create_result
        .into_rust()
        .map_err(|e| streamling_err!("Plugin creation failed: {:?}", e))?;
    let mapped_future = result
        .execution_future
        .map(|r| r.into_rust().map_err(|msg| msg.into_string()));
    Ok(InitializedPlugin {
        plugin_id: plugin_type.to_string(),
        execution_future: Box::pin(mapped_future),
        channels: plugin_channels,
        output_schema: None,
    })
}

pub fn terminate_plugins(
    plugins: Vec<(String, PluginChannels)>,
    send_budget: Option<Duration>,
) -> Result<()> {
    // Bound the send so a plugin whose input channel is full (a wedged
    // dispatcher) cannot park this call — and thus the whole shutdown path —
    // forever on a blocking crossbeam send. A failure to signal one plugin must
    // not stop us from terminating the rest, so warn and continue rather than
    // bailing; the host awaits the dispatchers afterwards and the shutdown
    // watchdog is the final backstop.
    const TERMINATE_SEND_TIMEOUT_MAX: Duration = Duration::from_secs(5);
    const TERMINATE_SEND_TIMEOUT_MIN: Duration = Duration::from_millis(100);
    let per_plugin = match send_budget {
        Some(total) => (total / plugins.len().max(1) as u32)
            .clamp(TERMINATE_SEND_TIMEOUT_MIN, TERMINATE_SEND_TIMEOUT_MAX),
        None => TERMINATE_SEND_TIMEOUT_MAX,
    };
    for (plugin_id, channels) in plugins {
        info!("Terminating plugin {}", plugin_id);

        if let Err(e) = channels
            .input
            .sender
            .send_timeout(NonExhaustive::new(PluginMsg::Terminate), per_plugin)
        {
            warn!(
                "Failed to send termination message to plugin {} within {:?}: {}. \
                 Continuing to terminate remaining plugins.",
                plugin_id, per_plugin, e
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §1.2 facade: a disconnected input channel (dispatcher exited) must be
    /// a typed error, not the panic the old `send().unwrap()` produced.
    #[tokio::test]
    async fn send_to_plugin_errors_on_disconnected_channel() {
        let (rtx, rrx) = crossbeam_channel::bounded::<u32>(1);
        drop(rrx);
        let err = send_to_plugin_with(&rtx, 7u32, "p", || false, Duration::from_secs(1))
            .await
            .expect_err("disconnected channel must error");
        assert!(err.to_string().contains("closed"), "got: {err}");
    }

    /// §1.2 facade: a full channel AFTER shutdown is requested is abandoned
    /// within the grace bound instead of parking forever.
    #[tokio::test]
    async fn send_to_plugin_abandons_full_channel_after_shutdown_grace() {
        let (rtx, _rrx) = crossbeam_channel::bounded::<u32>(1);
        rtx.try_send(1).unwrap(); // fill the single slot; nothing ever drains it

        let start = std::time::Instant::now();
        let err = send_to_plugin_with(&rtx, 2u32, "p", || true, Duration::from_millis(300))
            .await
            .expect_err("wedged channel must be abandoned");
        assert!(err.to_string().contains("abandoning"), "got: {err}");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "must abandon within the grace bound, took {:?}",
            start.elapsed()
        );
    }

    /// §1.2 facade: pre-shutdown a full channel is plain backpressure — the
    /// send must succeed once the dispatcher drains a slot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_to_plugin_waits_out_backpressure_before_shutdown() {
        let (rtx, rrx) = crossbeam_channel::bounded::<u32>(1);
        rtx.try_send(1).unwrap();

        let drainer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(250));
            let v = rrx.recv().unwrap();
            (v, rrx)
        });
        send_to_plugin_with(&rtx, 2u32, "p", || false, Duration::from_millis(100))
            .await
            .expect("send must succeed once a slot frees up");
        let (first, rrx) = drainer.join().unwrap();
        assert_eq!(first, 1);
        assert_eq!(rrx.recv().unwrap(), 2);
    }

    /// Regression: the panic hook calls
    /// into plugin termination; with a plugin input channel already full, a
    /// blocking (or even bounded-timeout) send parks the panicking thread and
    /// the process neither crashes nor exits. The non-blocking variant must
    /// return promptly no matter the channel state.
    #[test]
    fn nonblocking_terminate_returns_promptly_with_full_channel() {
        let channels = create_channels_with_caps(1, 1, 1);
        channels
            .input
            .sender
            .try_send(NonExhaustive::new(PluginMsg::Init))
            .expect("fills the single-slot input channel");
        register_plugin_instance("nonblocking-terminate-test", channels);

        let start = std::time::Instant::now();
        terminate_all_plugins_nonblocking();
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "must not block on a full channel, took {:?}",
            start.elapsed()
        );

        // The registry was drained even though the send could not go through.
        assert!(PLUGIN_INSTANCE_REGISTRY.read().unwrap().is_empty());
    }

    use crate::error::StreamlingError;
    use arrow_schema::Schema;

    /// Unused name that will not be present in the empty default plugin registry.
    const UNKNOWN_PLUGIN: &str = "definitely_not_a_real_plugin";

    fn assert_unknown_plugin_user_error(err: StreamlingError) {
        assert!(
            !err.is_internal(),
            "missing plugin must be a user-facing error"
        );
        let msg = err.to_string();
        assert!(
            msg.contains(UNKNOWN_PLUGIN),
            "error should name the plugin: {msg}"
        );
        assert!(
            msg.contains("is not available"),
            "error should say plugin is not available: {msg}"
        );
        assert!(
            msg.contains(&format!(
                "Registered plugin ids: [{}]",
                registered_plugin_ids().join(", ")
            )),
            "error should name what is actually loaded: {msg}"
        );
    }

    /// Regression: unknown plugins used to `panic!("Plugin {} not found!")`.
    /// They must now return a structured user error (never a panic).
    #[test]
    fn create_source_plugin_unknown_is_user_error_not_panic() {
        let app_config = AppConfig::load().expect("embedded config must load");
        match create_source_plugin(
            &app_config,
            "ref".to_string(),
            UNKNOWN_PLUGIN.to_string(),
            HashMap::new(),
        ) {
            Ok(_) => panic!("expected Err for unknown plugin"),
            Err(err) => assert_unknown_plugin_user_error(err),
        }
    }

    #[test]
    fn create_transform_plugin_unknown_is_user_error_not_panic() {
        let app_config = AppConfig::load().expect("embedded config must load");
        let empty_schema = Arc::new(Schema::empty());
        match create_transform_plugin(
            &app_config,
            "ref".to_string(),
            UNKNOWN_PLUGIN.to_string(),
            HashMap::new(),
            empty_schema,
        ) {
            Ok(_) => panic!("expected Err for unknown plugin"),
            Err(err) => assert_unknown_plugin_user_error(err),
        }
    }

    #[test]
    fn create_sink_plugin_unknown_is_user_error_not_panic() {
        let app_config = AppConfig::load().expect("embedded config must load");
        let empty_schema = Arc::new(Schema::empty());
        match create_sink_plugin(
            &app_config,
            "ref".to_string(),
            UNKNOWN_PLUGIN.to_string(),
            HashMap::new(),
            empty_schema,
        ) {
            Ok(_) => panic!("expected Err for unknown plugin"),
            Err(err) => assert_unknown_plugin_user_error(err),
        }
    }

    #[test]
    fn create_preprocessor_plugin_unknown_is_user_error_not_panic() {
        let app_config = AppConfig::load().expect("embedded config must load");
        match create_preprocessor_plugin(
            &app_config,
            "ref".to_string(),
            UNKNOWN_PLUGIN.to_string(),
            HashMap::new(),
        ) {
            Ok(_) => panic!("expected Err for unknown plugin"),
            Err(err) => assert_unknown_plugin_user_error(err),
        }
    }

    /// Regression: a `0` metrics cap (every plugin today) used the data-plane
    /// default of 50, so high-rate plugins overflowed the metrics channel.
    #[test]
    fn resolve_channel_caps_uses_metrics_default_when_unset() {
        assert_eq!(
            resolve_channel_caps(50, 4096, None),
            (50, 50, 4096),
            "no plugin caps: data channels stay at 50, metrics use 4096"
        );
        assert_eq!(
            resolve_channel_caps(
                50,
                4096,
                Some(PluginChannelCaps {
                    input: 0,
                    output: 1,
                    metrics: 0,
                }),
            ),
            (50, 1, 4096),
            "Solana-style output=1 must not shrink the metrics channel to 50"
        );
        assert_eq!(
            resolve_channel_caps(
                50,
                4096,
                Some(PluginChannelCaps {
                    input: 8,
                    output: 8,
                    metrics: 16,
                }),
            ),
            (8, 8, 16),
            "an explicit metrics cap must win"
        );
    }
}
