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
use crate::{streamling_bail, streamling_err, streamling_user_bail, streamling_user_err};
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
use std::sync::OnceLock;
use std::sync::{Arc, RwLock};
use streamling_plugin::r#async::{
    PluginAsyncRuntime, PluginAsyncRuntime_TO, PluginAsyncRuntimeObj,
};
use streamling_plugin::ffi::PluginMetricsChannel;
pub use streamling_plugin::{
    PluginChannel, PluginChannelCaps, PluginChannels, PluginLabel, PluginLogging, PluginModuleRef,
    PluginMsg, PluginOptions, PluginStateBackendConfig,
};
use tokio::runtime::Handle;
use tracing::info;

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

static SHUTDOWN_WATCH_STARTED: OnceLock<()> = OnceLock::new();

fn register_plugin_instance(instance_key: &str, channels: PluginChannels) {
    let mut reg = PLUGIN_INSTANCE_REGISTRY.write().unwrap();
    reg.insert(instance_key.to_string(), channels);
}

pub fn terminate_all_plugins() -> Result<()> {
    info!("Terminating all plugins");
    let mut reg = PLUGIN_INSTANCE_REGISTRY.write().unwrap();
    if reg.is_empty() {
        return Ok(());
    }
    let ids: Vec<(String, PluginChannels)> = reg.drain().collect();
    drop(reg);
    terminate_plugins(ids)
}

fn start_shutdown_watcher_once() {
    if SHUTDOWN_WATCH_STARTED.set(()).is_ok() {
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{SignalKind, signal};
                let mut sigterm =
                    signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
                let mut sigint =
                    signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
                tokio::select! {
                    _ = sigterm.recv() => {},
                    _ = sigint.recv() => {},
                    _ = tokio::signal::ctrl_c() => {},
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            let _ = self::terminate_all_plugins();
        });
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
        self.inner.spawn(fut).map(|_| ()).into_ffi()
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

pub fn load_and_initialize_plugin(path: &str, app_config: &AppConfig) -> Result<()> {
    let plugin_path = Path::new(path);
    info!("Loading plugin from: {:?}", plugin_path);

    let plugin_module = Arc::new(
        lib_header_from_path(plugin_path)
            .and_then(|x| x.init_root_module::<PluginModuleRef>())
            .expect("Unable to load plugin from path"),
    );

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

fn create_plugin_async_runtime(handle: Handle) -> PluginAsyncRuntimeObj {
    // `TD_Opaque` chooses `RBox<()>` for the erased-pointer parameter
    PluginAsyncRuntime_TO::from_value(PluginTokioWrapper { inner: handle }, TD_Opaque)
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

fn create_channels_for_plugin(app_config: &AppConfig, plugin_type: &PluginId) -> PluginChannels {
    let default = app_config.plugin.channel_capacity as usize;
    let caps_registry = PLUGIN_DEFAULT_CAPS.read().unwrap();
    if let Some(caps) = caps_registry.get(plugin_type) {
        let to_sz = |v: u32| if v == 0 { default } else { v as usize };
        return create_channels_with_caps(
            to_sz(caps.input),
            to_sz(caps.output),
            to_sz(caps.metrics),
        );
    }
    create_channels_with_caps(default, default, default)
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
    let plugin_module = find_plugin(&plugin_type).ok_or_else(|| {
        streamling_user_err!(
            "plugin '{}' is not available; check that the plugin type is correct and that the plugin bundle is installed",
            plugin_type
        )
    })?;
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
    start_shutdown_watcher_once();
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
    let plugin_module = find_plugin(&plugin_type).ok_or_else(|| {
        streamling_user_err!(
            "plugin '{}' is not available; check that the plugin type is correct and that the plugin bundle is installed",
            plugin_type
        )
    })?;
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
    start_shutdown_watcher_once();
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
    let plugin_module = find_plugin(&plugin_type).ok_or_else(|| {
        streamling_user_err!(
            "plugin '{}' is not available; check that the plugin type is correct and that the plugin bundle is installed",
            plugin_type
        )
    })?;
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
    start_shutdown_watcher_once();
    register_plugin_instance(reference_name.as_str(), plugin_channels.clone());
    merge_metadata_tags(
        &metric_key(&app_config.application_id, &reference_name),
        collect_labels(result.labels),
    );
    let mapped_future = result
        .execution_future
        .map(|r| r.into_rust().map_err(|msg| msg.into_string()));
    InitializedPlugin::new(
        plugin_type.to_string(),
        Box::pin(mapped_future),
        plugin_channels,
        None,
    )
}

pub fn create_preprocessor_plugin(
    app_config: &AppConfig,
    reference_name: String,
    plugin_type: String,
    options: HashMap<String, String>,
) -> Result<InitializedPlugin> {
    let plugin_type: PluginId = plugin_type.into();
    let plugin_module = find_plugin(&plugin_type).ok_or_else(|| {
        streamling_user_err!(
            "plugin '{}' is not available; check that the plugin type is correct and that the plugin bundle is installed",
            plugin_type
        )
    })?;
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

    create_result
        .into_rust()
        .map(|result| {
            let mapped_future = result
                .execution_future
                .map(|r| r.into_rust().map_err(|msg| msg.into_string()));
            InitializedPlugin {
                plugin_id: plugin_type.to_string(),
                execution_future: Box::pin(mapped_future),
                channels: plugin_channels,
                output_schema: None,
            }
        })
        .map_err(|e| streamling_err!("Plugin creation failed: {:?}", e))
}

pub fn terminate_plugins(plugins: Vec<(String, PluginChannels)>) -> Result<()> {
    for (plugin_id, channels) in plugins {
        info!("Terminating plugin {}", plugin_id);

        if let Err(e) = channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Terminate))
        {
            streamling_bail!("Failed to send termination message: {}", e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;
    use crate::error::StreamlingError;

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
}
