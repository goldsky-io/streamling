#![allow(non_local_definitions)]

pub mod api;
pub mod r#async;
mod dispatch;
pub mod ffi;
pub mod shutdown;

use crate::api::PluginStateBackendFactory;
pub use crate::api::{
    CheckpointEpoch, InputPlacement, PartitionCount, PartitionedSinkPlugin,
    PartitionedSourcePlugin, PartitionedTransformPlugin, PluginError, PluginStateBackend,
    PreprocessorPlugin, SideOutputPlugin, SinkDescription, SinkPlugin, SourceDescription,
    SourcePlugin, TransformDescription, TransformPlugin,
};
use crate::r#async::PluginAsyncRuntimeObj;
pub use crate::dispatch::{
    PreprocessorPluginDispatcher, SinkPluginDispatcher, SourcePluginDispatcher,
    TransformPluginDispatcher,
};
use crate::ffi::PluginMetricsRecorder;
pub use crate::ffi::SafeArrowSchema;
pub use crate::ffi::{
    PluginChannel, PluginChannels, PluginCheckpointEpoch, PluginLogging, PluginMsg, PluginOptions,
    SafeArrowColumn, SafeUdfArg,
};
use abi_stable::derive_macro_reexports::NonExhaustive;
use abi_stable::std_types::{RHashMap, RNone, ROption, RResult, RSome, RString, RVec};
use abi_stable::traits::IntoReprC;
use abi_stable::{
    StableAbi, declare_root_module_statics,
    library::{LibraryError, RootModule},
    package_version_strings,
    sabi_types::VersionStrings,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, SchemaRef};
use async_ffi::{FfiFuture, FutureExt};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, TypeSignature,
};
use futures::FutureExt as _;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, OnceLock};
pub use streamling_plugin_derive::*;
pub use streamling_state::{StateKey, StateOperatorBackend};
use tracing::{error, info};

/// A single identity label for a plugin instance. Plugins use this to declare *what they
/// are* — typically derived from their options at `create` time (e.g. a Kafka plugin
/// declaring its topic, an Ethereum plugin declaring its chain slug). The metrics
/// subsystem is the first consumer: labels are attached to every metric the plugin's
/// source/transform/sink emits. Other subsystems that need plugin identity (future admin
/// UIs, log-line decoration, tracing) can read the same field without a new FFI surface.
#[repr(C)]
#[derive(StableAbi, Debug, Clone)]
pub struct PluginLabel {
    pub key: RString,
    pub value: RString,
}

impl PluginLabel {
    /// Construct a single identity label. Both `key` and `value` accept any `Into<RString>`
    /// (e.g. `&str`, `String`), so plugins can pass literals or owned values from their
    /// options without manual FFI conversion.
    ///
    /// ```ignore
    /// PluginLabel::new("chain_slug", options.chain.clone())
    /// ```
    pub fn new(key: impl Into<RString>, value: impl Into<RString>) -> Self {
        PluginLabel {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[repr(C)]
#[derive(StableAbi)]
pub struct PluginResult {
    /// Future that resolves when the plugin has finished execution.
    ///
    /// ROk(()) => graceful termination
    /// RErr(message) => error termination
    pub execution_future: FfiFuture<RResult<(), RString>>,
    /// Expected to be defined for sources and transforms, but not for sinks.
    pub output_schema: ROption<SafeArrowSchema>,
    /// Identity labels the plugin declares for this instance. The plugin decides the set
    /// from its options; consumers (starting with the metrics subsystem) read the same
    /// field. Empty for plugins that don't opt in. See `PluginLabel`.
    pub labels: RVec<PluginLabel>,
}

#[repr(C)]
#[derive(StableAbi, Debug, Clone, Copy)]
pub struct PluginChannelCaps {
    pub input: u32,
    pub output: u32,
    pub metrics: u32,
}

#[repr(C)]
#[derive(StableAbi, Debug)]
pub struct PluginRuntimeConfiguration {
    pub plugin_ids: RVec<RString>,
    // Optional, per-plugin default channel capacities that the host can honor
    pub default_channel_caps: RHashMap<RString, PluginChannelCaps>,
}

impl PluginResult {
    pub fn new(
        execution_future: FfiFuture<RResult<(), RString>>,
        output_schema: ROption<SafeArrowSchema>,
    ) -> Self {
        PluginResult {
            execution_future,
            output_schema,
            labels: RVec::new(),
        }
    }

    /// Attach plugin-declared identity labels to this result. Typically chained onto
    /// `PluginResult::new`; the host merges these labels into metric metadata so every
    /// metric the plugin instance emits carries them as Prometheus labels.
    ///
    /// ```ignore
    /// PluginResult::new(execution_future, output_schema).with_labels(vec![
    ///     PluginLabel::new("chain_slug", options.chain.clone()),
    ///     PluginLabel::new("topic", options.topic.clone()),
    /// ])
    /// ```
    pub fn with_labels(mut self, labels: Vec<PluginLabel>) -> Self {
        self.labels = labels.into();
        self
    }
}

/// Name of one partition instance of a node: `{reference_name}[{index}]`.
/// Keys the instance's own state and, on the host, its registry entries.
pub fn partition_instance_name(reference_name: &str, partition_index: u32) -> String {
    format!("{reference_name}[{partition_index}]")
}

/// Which partition instance of a partitioned plugin node is being created.
/// `create_partitioned` is called once per `partition_index` in
/// `0..partition_count`, each call with its own channels.
#[repr(C)]
#[derive(StableAbi, Debug, Clone, PartialEq, Eq)]
pub struct PluginInstanceContext {
    /// The node's reference name (unique within the pipeline topology).
    pub reference_name: RString,
    pub partition_index: u32,
    pub partition_count: u32,
}

/// How the host must place a partitioned transform's or sink's input rows
/// before routing physical partition `i` to plugin instance `i`.
///
/// Non-exhaustive across the FFI boundary (like `PluginMsg`), so an older host
/// can still load a library that adds a variant later. The reverse is
/// rejected: a host with more variants fails the layout check of a library
/// built before them, so adding a variant needs a new frozen twin in
/// [`compat`] carrying a frozen copy of this enum. Unlike `PluginMsg` it has
/// no Rust `#[non_exhaustive]`: this type appears in `extern "C"` signatures,
/// and that attribute makes rustc flag every hand-written module function
/// using it as not FFI-safe.
#[repr(u8)]
#[derive(StableAbi, Debug, Clone, PartialEq, Eq)]
#[sabi(kind(WithNonExhaustive(
    size = [usize;8],
    traits(Debug, Clone, PartialEq),
    assert_nonexhaustive(PluginInputPlacement),
)))]
pub enum PluginInputPlacement {
    /// All rows of a primary key land on one instance. The key is the node's
    /// configured (or inherited) primary key.
    ByPrimaryKey,
    /// All rows sharing these columns' values land on one instance.
    ByColumns { columns: RVec<RString> },
    /// Any instance will do.
    RoundRobin,
}

/// Planning-time constraints on how many partitions a plugin node can run
/// with. The host resolves the actual count before creating any instance.
#[repr(C)]
#[derive(StableAbi, Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginPartitionCount {
    pub minimum: u32,
    pub maximum: ROption<u32>,
    /// Width to use when the topology does not set `parallelism` on a source
    /// (e.g. the source's native shard count). Ignored for transforms and
    /// sinks, which inherit their input's width.
    pub preferred: ROption<u32>,
}

/// What a partition-capable plugin reports about itself at planning time,
/// without constructing a running instance.
#[repr(C)]
#[derive(StableAbi, Debug)]
pub struct PartitionedPluginDescription {
    /// Defined for sources and transforms, not for sinks.
    pub output_schema: ROption<SafeArrowSchema>,
    pub labels: RVec<PluginLabel>,
    /// `RNone` for sources, which have no input.
    pub input_placement: ROption<PluginInputPlacement_NE>,
    pub partition_count: PluginPartitionCount,
}

#[repr(u8)]
#[derive(StableAbi, Debug)]
pub enum PluginInitializationError {
    NotImplemented,
    Configuration(RString),
    Execution(RString),
}

#[repr(C)]
#[derive(StableAbi, Debug)]
pub struct PluginStateBackendConfig {
    pub application_namespace: RString,
    /// The reference name of the plugin instance (unique within the pipeline topology).
    pub plugin_reference_name: RString,
    /// Serialized (JSON) configuration for the state backend. This is a workaround for
    /// the fact that the state backend configuration is not FFI-safe. We could either change the
    /// existing state backend configuration to be FFI-safe (which leaks FFI types into the general API),
    /// or we can create a separate configuration structure that is FFI-safe (and a conversion logic
    /// between the two).
    pub serialized_config: RString,
}

impl PluginStateBackendConfig {
    pub fn new(
        application_namespace: String,
        plugin_reference_name: String,
        serialized_config: String,
    ) -> Self {
        PluginStateBackendConfig {
            application_namespace: application_namespace.into_c(),
            plugin_reference_name: plugin_reference_name.into_c(),
            serialized_config: serialized_config.into_c(),
        }
    }
}

/// Conversion trait that lets `register_plugin_source!` accept constructors
/// returning either `Self` (infallible) or `Result<Self, PluginInitializationError>`
/// (fallible). Existing plugins that return `Self` continue to work unchanged.
pub trait IntoSourcePluginResult {
    fn into_source_result(self) -> Result<Arc<dyn SourcePlugin>, PluginInitializationError>;
}

impl<T: SourcePlugin + 'static> IntoSourcePluginResult for T {
    fn into_source_result(self) -> Result<Arc<dyn SourcePlugin>, PluginInitializationError> {
        Ok(Arc::new(self))
    }
}

impl<T: SourcePlugin + 'static> IntoSourcePluginResult for Result<T, PluginInitializationError> {
    fn into_source_result(self) -> Result<Arc<dyn SourcePlugin>, PluginInitializationError> {
        self.map(|s| Arc::new(s) as Arc<dyn SourcePlugin>)
    }
}

/// Conversion trait that lets `register_plugin_transform!` accept constructors
/// returning either `Self` (infallible) or `Result<Self, PluginInitializationError>`
/// (fallible). Existing plugins that return `Self` continue to work unchanged.
pub trait IntoTransformPluginResult {
    fn into_transform_result(self) -> Result<Arc<dyn TransformPlugin>, PluginInitializationError>;
}

impl<T: TransformPlugin + 'static> IntoTransformPluginResult for T {
    fn into_transform_result(self) -> Result<Arc<dyn TransformPlugin>, PluginInitializationError> {
        Ok(Arc::new(self))
    }
}

impl<T: TransformPlugin + 'static> IntoTransformPluginResult
    for Result<T, PluginInitializationError>
{
    fn into_transform_result(self) -> Result<Arc<dyn TransformPlugin>, PluginInitializationError> {
        self.map(|t| Arc::new(t) as Arc<dyn TransformPlugin>)
    }
}

/// Conversion trait that lets `register_plugin_sink!` accept constructors
/// returning either `Self` (infallible) or `Result<Self, PluginInitializationError>`
/// (fallible). Existing plugins that return `Self` continue to work unchanged.
pub trait IntoSinkPluginResult {
    fn into_sink_result(self) -> Result<Arc<dyn SinkPlugin>, PluginInitializationError>;
}

impl<T: SinkPlugin + 'static> IntoSinkPluginResult for T {
    fn into_sink_result(self) -> Result<Arc<dyn SinkPlugin>, PluginInitializationError> {
        Ok(Arc::new(self))
    }
}

impl<T: SinkPlugin + 'static> IntoSinkPluginResult for Result<T, PluginInitializationError> {
    fn into_sink_result(self) -> Result<Arc<dyn SinkPlugin>, PluginInitializationError> {
        self.map(|s| Arc::new(s) as Arc<dyn SinkPlugin>)
    }
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Ok(s) = payload.downcast::<String>() {
        *s
    } else {
        "unknown panic in plugin code".to_string()
    }
}

/// Drives a plugin dispatcher on the plugin async runtime and reports its outcome
/// through the returned FFI execution future.
///
/// A dispatcher error (e.g. a sink whose backend is unreachable) must resolve the
/// execution future with `RErr` so the host fails the pipeline and drains it —
/// never `panic!`: unwinding across the `extern "C"` poll boundary of an
/// `async_ffi` future aborts the process before the host's signal handling or
/// shutdown watchdog can react. Panics from plugin code are caught and reported
/// the same way; as with the factory `catch_unwind` above, the payload is only
/// converted to a string and no plugin state is observed after a panic.
fn spawn_dispatcher_worker<Fut>(
    id: RString,
    runtime: &PluginAsyncRuntimeObj,
    start: Fut,
) -> FfiFuture<RResult<(), RString>>
where
    Fut: Future<Output = Result<(), PluginError>> + Send + 'static,
{
    let worker_error: Arc<OnceLock<String>> = Arc::new(OnceLock::new());
    let worker_error_writer = worker_error.clone();

    let worker = async move {
        match AssertUnwindSafe(start).catch_unwind().await {
            Ok(Ok(())) => (),
            Ok(Err(e)) => {
                let msg = format!("Plugin error {id}: {e:?}");
                error!("{msg}");
                let _ = worker_error_writer.set(msg);
            }
            Err(panic_payload) => {
                let msg = format!(
                    "Plugin panic {id}: {}",
                    panic_payload_to_string(panic_payload)
                );
                error!("{msg}");
                let _ = worker_error_writer.set(msg);
            }
        }
    }
    .into_ffi();

    let spawned = runtime.spawn(worker);

    async move {
        spawned.await;
        match worker_error.get() {
            Some(err) => RResult::RErr(RString::from(err.as_str())),
            None => RResult::ROk(()),
        }
    }
    .into_ffi()
}

pub fn source_generator<F>(
    id: RString,
    factory: F,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError>
where
    F: FnOnce(
        PluginAsyncRuntimeObj,
        PluginStateBackendFactory,
        PluginMetricsRecorder,
        HashMap<String, String>,
    ) -> Result<Arc<dyn SourcePlugin>, PluginInitializationError>,
{
    start_source(
        id,
        factory,
        options,
        runtime,
        PluginStateBackendFactory::new(state_backend_config),
        message_channels,
    )
}

fn start_source<F>(
    id: RString,
    factory: F,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_factory: PluginStateBackendFactory,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError>
where
    F: FnOnce(
        PluginAsyncRuntimeObj,
        PluginStateBackendFactory,
        PluginMetricsRecorder,
        HashMap<String, String>,
    ) -> Result<Arc<dyn SourcePlugin>, PluginInitializationError>,
{
    info!("Creating {} with options: {:?}", id, options);

    let metrics_recorder = PluginMetricsRecorder::new(message_channels.metrics.sender.clone());
    // Rationale: Plugin factories are not necessarily `UnwindSafe`; we only convert panics into
    // `PluginInitializationError` and never observe partial plugin state after a panic.
    let source = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        factory(
            runtime.clone(),
            state_backend_factory,
            metrics_recorder,
            options.as_rust(),
        )
    })) {
        Ok(Ok(source)) => source,
        Ok(Err(e)) => return Err(e).into_c(),
        Err(panic_payload) => {
            return Err(PluginInitializationError::Configuration(RString::from(
                panic_payload_to_string(panic_payload),
            )))
            .into_c();
        }
    };
    let labels = source.labels();
    let output_schema = match source.output_schema() {
        Ok(schema) => schema,
        Err(e) => {
            return RResult::RErr(PluginInitializationError::Configuration(RString::from(
                e.to_string(),
            )));
        }
    };
    let dispatcher = SourcePluginDispatcher::new(message_channels, source);

    let rt = runtime.clone();
    let dispatcher_future =
        spawn_dispatcher_worker(id, &runtime, async move { dispatcher.start(rt).await });

    Ok(PluginResult::new(dispatcher_future, RSome(output_schema.into())).with_labels(labels))
        .into_c()
}

pub fn transform_generator<F>(
    id: RString,
    factory: F,
    input_schema: SafeArrowSchema,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError>
where
    F: FnOnce(
        SchemaRef,
        PluginAsyncRuntimeObj,
        PluginStateBackendFactory,
        PluginMetricsRecorder,
        HashMap<String, String>,
    ) -> Result<Arc<dyn TransformPlugin>, PluginInitializationError>,
{
    start_transform(
        id,
        factory,
        input_schema,
        options,
        runtime,
        PluginStateBackendFactory::new(state_backend_config),
        message_channels,
    )
}

fn start_transform<F>(
    id: RString,
    factory: F,
    input_schema: SafeArrowSchema,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_factory: PluginStateBackendFactory,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError>
where
    F: FnOnce(
        SchemaRef,
        PluginAsyncRuntimeObj,
        PluginStateBackendFactory,
        PluginMetricsRecorder,
        HashMap<String, String>,
    ) -> Result<Arc<dyn TransformPlugin>, PluginInitializationError>,
{
    info!("Creating {} with options: {:?}", id, options);

    let metrics_recorder = PluginMetricsRecorder::new(message_channels.metrics.sender.clone());

    // Rationale: See `transform_generator` — panics become initialization errors; no use-after-panic.
    let transform = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        factory(
            input_schema.into(),
            runtime.clone(),
            state_backend_factory,
            metrics_recorder,
            options.as_rust(),
        )
    })) {
        Ok(Ok(transform)) => transform,
        Ok(Err(e)) => return Err(e).into_c(),
        Err(panic_payload) => {
            return Err(PluginInitializationError::Configuration(RString::from(
                panic_payload_to_string(panic_payload),
            )))
            .into_c();
        }
    };
    let labels = transform.labels();
    let output_schema = match transform.output_schema() {
        Ok(schema) => schema,
        Err(e) => {
            return RResult::RErr(PluginInitializationError::Configuration(RString::from(
                e.to_string(),
            )));
        }
    };

    let dispatcher = TransformPluginDispatcher::new(message_channels, transform);

    let rt = runtime.clone();
    let dispatcher_future =
        spawn_dispatcher_worker(id, &runtime, async move { dispatcher.start(rt).await });

    Ok(PluginResult::new(dispatcher_future, RSome(output_schema.into())).with_labels(labels))
        .into_c()
}

pub fn sink_generator<F>(
    id: RString,
    factory: F,
    input_schema: SafeArrowSchema,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError>
where
    F: FnOnce(
        SchemaRef,
        PluginAsyncRuntimeObj,
        PluginStateBackendFactory,
        PluginMetricsRecorder,
        HashMap<String, String>,
    ) -> Result<Arc<dyn SinkPlugin>, PluginInitializationError>,
{
    start_sink(
        id,
        factory,
        input_schema,
        options,
        runtime,
        PluginStateBackendFactory::new(state_backend_config),
        message_channels,
    )
}

fn start_sink<F>(
    id: RString,
    factory: F,
    input_schema: SafeArrowSchema,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_factory: PluginStateBackendFactory,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError>
where
    F: FnOnce(
        SchemaRef,
        PluginAsyncRuntimeObj,
        PluginStateBackendFactory,
        PluginMetricsRecorder,
        HashMap<String, String>,
    ) -> Result<Arc<dyn SinkPlugin>, PluginInitializationError>,
{
    info!("Creating {} with options: {:?}", id, options);

    let metrics_recorder = PluginMetricsRecorder::new(message_channels.metrics.sender.clone());
    // Rationale: See `sink_generator` — panics become initialization errors; no use-after-panic.
    let sink = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        factory(
            input_schema.into(),
            runtime.clone(),
            state_backend_factory,
            metrics_recorder,
            options.as_rust(),
        )
    })) {
        Ok(Ok(sink)) => sink,
        Ok(Err(e)) => return Err(e).into_c(),
        Err(panic_payload) => {
            return Err(PluginInitializationError::Configuration(RString::from(
                panic_payload_to_string(panic_payload),
            )))
            .into_c();
        }
    };
    let labels = sink.labels();

    let rt = runtime.clone();
    let dispatcher_future = spawn_dispatcher_worker(id, &runtime, async move {
        let dispatcher = SinkPluginDispatcher::new(message_channels, sink);
        dispatcher.start(rt).await
    });

    Ok(PluginResult::new(dispatcher_future, RNone).with_labels(labels)).into_c()
}

pub fn preprocessor_generator<F>(
    id: RString,
    factory: F,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError>
where
    F: FnOnce(
        HashMap<String, String>,
    ) -> Result<Arc<dyn PreprocessorPlugin>, PluginInitializationError>,
{
    info!("Creating preprocessor {} with options: {:?}", id, options);

    let preprocessor = match factory(options.as_rust()) {
        Ok(p) => p,
        Err(e) => return Err(e).into_c(),
    };
    let dispatcher = PreprocessorPluginDispatcher::new(message_channels, preprocessor);

    let dispatcher_future =
        spawn_dispatcher_worker(id, &runtime, async move { dispatcher.start().await });

    Ok(PluginResult::new(dispatcher_future, RNone)).into_c()
}

impl From<PartitionCount> for PluginPartitionCount {
    fn from(value: PartitionCount) -> Self {
        PluginPartitionCount {
            minimum: value.minimum,
            maximum: value.maximum.into_c(),
            preferred: value.preferred.into_c(),
        }
    }
}

impl From<InputPlacement> for PluginInputPlacement_NE {
    fn from(value: InputPlacement) -> Self {
        NonExhaustive::new(match value {
            InputPlacement::ByPrimaryKey => PluginInputPlacement::ByPrimaryKey,
            InputPlacement::ByColumns(columns) => PluginInputPlacement::ByColumns {
                columns: columns.into_iter().map(RString::from).collect(),
            },
            InputPlacement::RoundRobin => PluginInputPlacement::RoundRobin,
        })
    }
}

type DescribeResult = RResult<ROption<PartitionedPluginDescription>, PluginInitializationError>;

/// Runs a plugin's `describe`, turning a panic into an initialization error
/// like the create factories do.
fn describe_catching_panics<F>(describe: F) -> DescribeResult
where
    F: FnOnce() -> Result<PartitionedPluginDescription, PluginInitializationError>,
{
    // Rationale: see `source_generator` — panics become initialization errors
    // and no plugin state is observed after one.
    match std::panic::catch_unwind(AssertUnwindSafe(describe)) {
        Ok(Ok(description)) => RResult::ROk(RSome(description)),
        Ok(Err(e)) => RResult::RErr(e),
        Err(panic_payload) => RResult::RErr(PluginInitializationError::Configuration(
            RString::from(panic_payload_to_string(panic_payload)),
        )),
    }
}

fn required_input_schema(
    input_schema: ROption<SafeArrowSchema>,
) -> Result<SchemaRef, PluginInitializationError> {
    input_schema.into_option().map(Into::into).ok_or_else(|| {
        PluginInitializationError::Configuration(RString::from(
            "an input schema is required for transforms and sinks",
        ))
    })
}

/// `describe_partitioned` for a plugin registered with
/// `register_partitioned_plugin_source!`.
pub fn describe_partitioned_source<T: PartitionedSourcePlugin>(
    options: PluginOptions,
) -> DescribeResult {
    describe_catching_panics(|| {
        let description = T::describe(&options.as_rust())?;
        Ok(PartitionedPluginDescription {
            output_schema: RSome(description.output_schema.into()),
            labels: description.labels.into(),
            input_placement: RNone,
            partition_count: description.partition_count.into(),
        })
    })
}

/// `describe_partitioned` for a plugin registered with
/// `register_partitioned_plugin_transform!`.
pub fn describe_partitioned_transform<T: PartitionedTransformPlugin>(
    input_schema: ROption<SafeArrowSchema>,
    options: PluginOptions,
) -> DescribeResult {
    describe_catching_panics(|| {
        let description = T::describe(required_input_schema(input_schema)?, &options.as_rust())?;
        Ok(PartitionedPluginDescription {
            output_schema: RSome(description.output_schema.into()),
            labels: description.labels.into(),
            input_placement: RSome(description.input_placement.into()),
            partition_count: description.partition_count.into(),
        })
    })
}

/// `describe_partitioned` for a plugin registered with
/// `register_partitioned_plugin_sink!`.
pub fn describe_partitioned_sink<T: PartitionedSinkPlugin>(
    input_schema: ROption<SafeArrowSchema>,
    options: PluginOptions,
) -> DescribeResult {
    describe_catching_panics(|| {
        let description = T::describe(required_input_schema(input_schema)?, &options.as_rust())?;
        Ok(PartitionedPluginDescription {
            output_schema: RNone,
            labels: description.labels.into(),
            input_placement: RSome(description.input_placement.into()),
            partition_count: description.partition_count.into(),
        })
    })
}

/// `create_partitioned` for a plugin registered with
/// `register_partitioned_plugin_source!`.
pub fn create_partitioned_source<T: PartitionedSourcePlugin>(
    id: RString,
    options: PluginOptions,
    context: PluginInstanceContext,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    let state_backend_factory =
        PluginStateBackendFactory::for_partition(state_backend_config, &context);
    start_source(
        id,
        |rt, state, metrics, opts| {
            T::create(context, rt, state, metrics, opts).map(|s| Arc::new(s) as _)
        },
        options,
        runtime,
        state_backend_factory,
        message_channels,
    )
}

/// `create_partitioned` for a plugin registered with
/// `register_partitioned_plugin_transform!`.
pub fn create_partitioned_transform<T: PartitionedTransformPlugin>(
    id: RString,
    input_schema: ROption<SafeArrowSchema>,
    options: PluginOptions,
    context: PluginInstanceContext,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    let input_schema = match required_input_schema(input_schema) {
        Ok(schema) => schema,
        Err(e) => return RResult::RErr(e),
    };
    let state_backend_factory =
        PluginStateBackendFactory::for_partition(state_backend_config, &context);
    start_transform(
        id,
        |schema, rt, state, metrics, opts| {
            T::create(context, schema, rt, state, metrics, opts).map(|t| Arc::new(t) as _)
        },
        input_schema.into(),
        options,
        runtime,
        state_backend_factory,
        message_channels,
    )
}

/// `create_partitioned` for a plugin registered with
/// `register_partitioned_plugin_sink!`.
pub fn create_partitioned_sink<T: PartitionedSinkPlugin>(
    id: RString,
    input_schema: ROption<SafeArrowSchema>,
    options: PluginOptions,
    context: PluginInstanceContext,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    let input_schema = match required_input_schema(input_schema) {
        Ok(schema) => schema,
        Err(e) => return RResult::RErr(e),
    };
    let state_backend_factory =
        PluginStateBackendFactory::for_partition(state_backend_config, &context);
    start_sink(
        id,
        |schema, rt, state, metrics, opts| {
            T::create(context, schema, rt, state, metrics, opts).map(|s| Arc::new(s) as _)
        },
        input_schema.into(),
        options,
        runtime,
        state_backend_factory,
        message_channels,
    )
}

/// The context a partition-aware plugin runs with when a host that predates
/// partitioning creates it through the single-stream `create`: the only
/// partition of its node. Fails when the plugin cannot run that narrow.
fn single_stream_context(
    state_backend_config: &PluginStateBackendConfig,
    described: DescribeResult,
) -> Result<PluginInstanceContext, PluginInitializationError> {
    let minimum = match described {
        RResult::ROk(description) => description.map_or(1, |d| d.partition_count.minimum),
        RResult::RErr(e) => return Err(e),
    };
    if minimum > 1 {
        return Err(PluginInitializationError::Configuration(RString::from(
            format!(
                "plugin requires at least {minimum} partitions, but the host runs it as a \
                 single stream (it predates partitioned plugins)"
            ),
        )));
    }
    Ok(PluginInstanceContext {
        reference_name: state_backend_config.plugin_reference_name.clone(),
        partition_index: 0,
        partition_count: 1,
    })
}

/// `create` for a plugin registered with `register_partitioned_plugin_source!`.
/// Only a host that predates partitioning calls it.
pub fn create_partitioned_source_single_stream<T: PartitionedSourcePlugin>(
    id: RString,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    let described = describe_partitioned_source::<T>(options.clone());
    match single_stream_context(&state_backend_config, described) {
        Ok(context) => create_partitioned_source::<T>(
            id,
            options,
            context,
            runtime,
            state_backend_config,
            message_channels,
        ),
        Err(e) => RResult::RErr(e),
    }
}

/// `create` for a plugin registered with
/// `register_partitioned_plugin_transform!`. Only a host that predates
/// partitioning calls it.
pub fn create_partitioned_transform_single_stream<T: PartitionedTransformPlugin>(
    id: RString,
    input_schema: ROption<SafeArrowSchema>,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    let input_schema: Option<SchemaRef> = input_schema.into_option().map(Into::into);
    let described = describe_partitioned_transform::<T>(
        input_schema.clone().map(Into::into).into_c(),
        options.clone(),
    );
    match single_stream_context(&state_backend_config, described) {
        Ok(context) => create_partitioned_transform::<T>(
            id,
            input_schema.map(Into::into).into_c(),
            options,
            context,
            runtime,
            state_backend_config,
            message_channels,
        ),
        Err(e) => RResult::RErr(e),
    }
}

/// `create` for a plugin registered with `register_partitioned_plugin_sink!`.
/// Only a host that predates partitioning calls it.
pub fn create_partitioned_sink_single_stream<T: PartitionedSinkPlugin>(
    id: RString,
    input_schema: ROption<SafeArrowSchema>,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    let input_schema: Option<SchemaRef> = input_schema.into_option().map(Into::into);
    let described = describe_partitioned_sink::<T>(
        input_schema.clone().map(Into::into).into_c(),
        options.clone(),
    );
    match single_stream_context(&state_backend_config, described) {
        Ok(context) => create_partitioned_sink::<T>(
            id,
            input_schema.map(Into::into).into_c(),
            options,
            context,
            runtime,
            state_backend_config,
            message_channels,
        ),
        Err(e) => RResult::RErr(e),
    }
}

/// Descriptor for a single UDF provided by a plugin.
#[repr(C)]
#[derive(StableAbi)]
pub struct PluginUdfDescriptor {
    pub name: RString,
    pub aliases: RVec<RString>,
    pub type_signatures: RVec<RVec<SafeArrowSchema>>,
    pub return_type: SafeArrowSchema,
    pub deterministic: bool,
    pub invoke: extern "C" fn(
        args: RVec<SafeUdfArg>,
        number_rows: usize,
    ) -> RResult<SafeArrowColumn, RString>,
}

/// Invokes a `ScalarUDFImpl` with FFI-marshaled arguments and returns the FFI-marshaled result.
///
/// This is the shared implementation behind every plugin UDF's `extern "C"` invoke function.
/// [`SafeUdfArg`] carries scalar/array semantics across the FFI boundary so plugin UDFs see the
/// same `ColumnarValue` shapes as non-plugin UDFs (no unnecessary array broadcast for literals).
pub fn invoke_plugin_udf(
    instance: &dyn ScalarUDFImpl,
    args: RVec<SafeUdfArg>,
    number_rows: usize,
) -> RResult<SafeArrowColumn, RString> {
    let columnar_args: Vec<ColumnarValue> = args
        .into_iter()
        .map(|arg| {
            let array = ArrayRef::from(arg.column);
            if arg.is_scalar {
                match ScalarValue::try_from_array(array.as_ref(), 0) {
                    Ok(s) => ColumnarValue::Scalar(s),
                    Err(_) => ColumnarValue::Array(array),
                }
            } else {
                ColumnarValue::Array(array)
            }
        })
        .collect();

    let arg_fields: Vec<Arc<Field>> = columnar_args
        .iter()
        .map(|cv| match cv {
            ColumnarValue::Array(a) => Arc::new(Field::new("_", a.data_type().clone(), true)),
            ColumnarValue::Scalar(s) => Arc::new(Field::new("_", s.data_type(), true)),
        })
        .collect();

    let scalar_storage: Vec<Option<ScalarValue>> = columnar_args
        .iter()
        .map(|cv| match cv {
            ColumnarValue::Scalar(s) => Some(s.clone()),
            ColumnarValue::Array(_) => None,
        })
        .collect();
    let scalar_argument_refs: Vec<Option<&ScalarValue>> =
        scalar_storage.iter().map(|opt| opt.as_ref()).collect();

    let return_field = match instance.return_type(&[]) {
        Ok(dt) => Arc::new(Field::new("result", dt, true)),
        Err(_) => {
            let fallback_args = ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_argument_refs,
            };
            match instance.return_field_from_args(fallback_args) {
                Ok(field) => field,
                Err(e) => return RResult::RErr(RString::from(e.to_string())),
            }
        }
    };

    let scalar_args = ScalarFunctionArgs {
        args: columnar_args,
        arg_fields,
        number_rows,
        return_field,
        config_options: std::sync::Arc::new(datafusion::config::ConfigOptions::default()),
    };

    match instance.invoke_with_args(scalar_args) {
        Ok(ColumnarValue::Array(arr)) => RResult::ROk(SafeArrowColumn::from(arr)),
        Ok(ColumnarValue::Scalar(s)) => match s.to_array_of_size(number_rows.max(1)) {
            Ok(arr) => RResult::ROk(SafeArrowColumn::from(arr)),
            Err(e) => RResult::RErr(RString::from(e.to_string())),
        },
        Err(e) => RResult::RErr(RString::from(e.to_string())),
    }
}

/// Descriptor for a side output provided by a plugin.
/// Side outputs use direct FFI invocation (no channels) and are auto-registered on all sources.
/// One instance is created per source — the macro manages a HashMap<source_name, instance>.
#[repr(C)]
#[derive(StableAbi, Clone)]
pub struct PluginSideOutputDescriptor {
    pub id: RString,
    pub initialize: extern "C" fn(
        source_name: RString,
        schema: SafeArrowSchema,
        options: PluginOptions,
        metrics_recorder: PluginMetricsRecorder,
    ) -> RResult<(), RString>,
    pub process_batch:
        extern "C" fn(source_name: RString, data: ffi::SafeArrowArray) -> RResult<(), RString>,
    pub shutdown: extern "C" fn() -> RResult<(), RString>,
}

/// Builds a `PluginUdfDescriptor` from a `ScalarUDFImpl` instance and its `extern "C"` invoke
/// function pointer.
pub fn build_plugin_udf_descriptor(
    instance: &dyn ScalarUDFImpl,
    invoke: extern "C" fn(
        args: RVec<SafeUdfArg>,
        number_rows: usize,
    ) -> RResult<SafeArrowColumn, RString>,
) -> Result<PluginUdfDescriptor, PluginInitializationError> {
    let sig = instance.signature();
    let type_signatures: RVec<RVec<SafeArrowSchema>> = match &sig.type_signature {
        TypeSignature::Exact(types) => {
            let converted: RVec<SafeArrowSchema> = types
                .iter()
                .map(|dt| SafeArrowSchema::from(dt.clone()))
                .collect();
            RVec::from(vec![converted])
        }
        TypeSignature::OneOf(variants) => {
            let mut converted = Vec::with_capacity(variants.len());
            for variant in variants {
                match variant {
                    TypeSignature::Exact(types) => {
                        converted.push(
                            types
                                .iter()
                                .map(|dt| SafeArrowSchema::from(dt.clone()))
                                .collect(),
                        );
                    }
                    other => {
                        return Err(PluginInitializationError::Configuration(RString::from(
                            format!(
                                "Plugin UDFs only support Exact type signatures within OneOf, got: {:?}",
                                other
                            ),
                        )));
                    }
                }
            }
            RVec::from(converted)
        }
        other => {
            return Err(PluginInitializationError::Configuration(RString::from(
                format!(
                    "Plugin UDFs only support Exact and OneOf type signatures, got: {:?}",
                    other
                ),
            )));
        }
    };
    let return_type = match instance.return_type(&[]) {
        Ok(dt) => dt,
        Err(_) => {
            let fallback_args = ReturnFieldArgs {
                arg_fields: &[],
                scalar_arguments: &[],
            };
            instance
                .return_field_from_args(fallback_args)
                .map_err(|e| {
                    PluginInitializationError::Configuration(RString::from(format!(
                        "UDF must implement either return_type or return_field_from_args: {e}"
                    )))
                })?
                .data_type()
                .clone()
        }
    };
    let deterministic = sig.volatility == datafusion::logical_expr::Volatility::Immutable;
    let aliases: RVec<RString> = instance
        .aliases()
        .iter()
        .map(|a| RString::from(a.as_str()))
        .collect();
    Ok(PluginUdfDescriptor {
        name: RString::from(instance.name()),
        aliases,
        type_signatures,
        return_type: SafeArrowSchema::from(return_type),
        deterministic,
        invoke,
    })
}

#[cfg(test)]
mod safe_udf_arg_tests {
    use super::*;
    use arrow::array::StringArray;
    use std::sync::Arc;

    #[test]
    fn scalar_arg_round_trips_to_columnar_scalar() {
        let arr = Arc::new(StringArray::from(vec!["url"])) as ArrayRef;
        let ffi_arg = SafeUdfArg {
            column: SafeArrowColumn::from(arr),
            is_scalar: true,
        };

        let array = ArrayRef::from(ffi_arg.column);
        assert!(ffi_arg.is_scalar);
        let sv = ScalarValue::try_from_array(array.as_ref(), 0).unwrap();
        assert_eq!(sv, ScalarValue::Utf8(Some("url".to_string())));
    }

    #[test]
    fn array_arg_round_trips_to_columnar_array() {
        let arr = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let ffi_arg = SafeUdfArg {
            column: SafeArrowColumn::from(arr.clone()),
            is_scalar: false,
        };

        let restored = ArrayRef::from(ffi_arg.column);
        assert_eq!(restored.len(), 3);
    }
}

#[cfg(test)]
mod dispatcher_worker_tests {
    use super::*;
    use crate::r#async::DirectTokioProxy;

    // A dispatcher error must resolve the execution future with RErr — never
    // panic. A panic here unwinds through the extern "C" poll of the FFI
    // future and aborts the whole pipeline process, leaving it deaf to
    // SIGTERM (the sink-precheck failure mode this guards against).
    #[tokio::test]
    async fn dispatcher_error_resolves_execution_future_with_rerr() {
        let runtime = DirectTokioProxy::new().into_async_runtime_obj();
        let execution_future =
            spawn_dispatcher_worker(RString::from("tinybird"), &runtime, async {
                Err(PluginError::Internal(
                    "failed to check if datasource exists".to_string(),
                ))
            });
        match execution_future.await {
            RResult::RErr(msg) => {
                assert!(msg.as_str().contains("Plugin error tinybird"), "{msg}");
                assert!(
                    msg.as_str()
                        .contains("failed to check if datasource exists"),
                    "{msg}"
                );
            }
            RResult::ROk(()) => panic!("dispatcher error must surface as RErr"),
        }
    }

    #[tokio::test]
    async fn dispatcher_panic_resolves_execution_future_with_rerr() {
        let runtime = DirectTokioProxy::new().into_async_runtime_obj();
        let execution_future = spawn_dispatcher_worker(RString::from("panicky"), &runtime, async {
            panic!("boom in plugin code");
        });
        match execution_future.await {
            RResult::RErr(msg) => {
                assert!(msg.as_str().contains("Plugin panic panicky"), "{msg}");
                assert!(msg.as_str().contains("boom in plugin code"), "{msg}");
            }
            RResult::ROk(()) => panic!("plugin panic must surface as RErr"),
        }
    }

    #[tokio::test]
    async fn dispatcher_success_resolves_execution_future_with_rok() {
        let runtime = DirectTokioProxy::new().into_async_runtime_obj();
        let execution_future =
            spawn_dispatcher_worker(RString::from("clean"), &runtime, async { Ok(()) });
        assert!(matches!(execution_future.await, RResult::ROk(())));
    }
}

#[cfg(test)]
mod partitioned_generator_tests {
    use super::*;
    use crate::api::{
        InputPlacement, PartitionCount, PartitionedSinkPlugin, PartitionedSourcePlugin,
        PartitionedTransformPlugin, SinkDescription, SourceDescription, SupportsGracefulShutdown,
        TransformDescription,
    };
    use crate::r#async::DirectTokioProxy;
    use abi_stable::derive_macro_reexports::NonExhaustive;
    use abi_stable::external_types::crossbeam_channel;
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Schema};
    use async_trait::async_trait;
    use ffi::PluginMetricsChannel;

    const MINIMUM_PARTITIONS_OPTION: &str = "minimum_partitions";
    const PARTITION_LABEL: &str = "partition";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(crate::api::STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
        ]))
    }

    fn partition_count(options: &HashMap<String, String>) -> PartitionCount {
        PartitionCount {
            minimum: options
                .get(MINIMUM_PARTITIONS_OPTION)
                .map_or(1, |m| m.parse().unwrap()),
            maximum: Some(8),
            preferred: Some(4),
        }
    }

    /// Reports the partition it was created for as a label, so tests can see
    /// the context that reached the plugin.
    struct PartitionReporter {
        context: PluginInstanceContext,
    }

    impl PartitionReporter {
        fn partition_labels(&self) -> Vec<PluginLabel> {
            vec![PluginLabel::new(
                PARTITION_LABEL,
                format!(
                    "{}:{}/{}",
                    self.context.reference_name,
                    self.context.partition_index,
                    self.context.partition_count
                ),
            )]
        }
    }

    #[async_trait]
    impl SupportsGracefulShutdown for PartitionReporter {
        fn is_running(&self) -> bool {
            true
        }
        async fn terminate(&self) -> Result<(), PluginError> {
            Ok(())
        }
    }

    #[async_trait]
    impl SourcePlugin for PartitionReporter {
        async fn initialize(&self) -> Result<(), PluginError> {
            Ok(())
        }
        fn output_schema(&self) -> Result<SchemaRef, PluginError> {
            Ok(schema())
        }
        fn labels(&self) -> Vec<PluginLabel> {
            self.partition_labels()
        }
        async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
            Ok(RecordBatch::new_empty(schema()))
        }
        async fn process_checkpoint_marker(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
            Ok(())
        }
        async fn process_checkpoint_finalizer(
            &self,
            _: CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Ok(())
        }
    }

    impl PartitionedSourcePlugin for PartitionReporter {
        fn describe(
            options: &HashMap<String, String>,
        ) -> Result<SourceDescription, PluginInitializationError> {
            Ok(SourceDescription {
                output_schema: schema(),
                labels: vec![PluginLabel::new("topic", "blocks")],
                partition_count: partition_count(options),
            })
        }

        fn create(
            context: PluginInstanceContext,
            _: PluginAsyncRuntimeObj,
            _: PluginStateBackendFactory,
            _: PluginMetricsRecorder,
            _: HashMap<String, String>,
        ) -> Result<Self, PluginInitializationError> {
            Ok(PartitionReporter { context })
        }
    }

    struct KeyedTransform;

    #[async_trait]
    impl SupportsGracefulShutdown for KeyedTransform {
        fn is_running(&self) -> bool {
            true
        }
        async fn terminate(&self) -> Result<(), PluginError> {
            Ok(())
        }
    }

    #[async_trait]
    impl TransformPlugin for KeyedTransform {
        async fn initialize(&self) -> Result<(), PluginError> {
            Ok(())
        }
        fn output_schema(&self) -> Result<SchemaRef, PluginError> {
            Ok(schema())
        }
        async fn process_batch(&self, data: RecordBatch) -> Result<RecordBatch, PluginError> {
            Ok(data)
        }
        async fn process_checkpoint_marker(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
            Ok(())
        }
        async fn process_checkpoint_finalizer(
            &self,
            _: CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Ok(())
        }
    }

    impl PartitionedTransformPlugin for KeyedTransform {
        fn describe(
            input_schema: SchemaRef,
            options: &HashMap<String, String>,
        ) -> Result<TransformDescription, PluginInitializationError> {
            Ok(TransformDescription {
                output_schema: input_schema,
                labels: Vec::new(),
                input_placement: InputPlacement::ByColumns(vec!["id".to_string()]),
                partition_count: partition_count(options),
            })
        }

        fn create(
            _: PluginInstanceContext,
            _: SchemaRef,
            _: PluginAsyncRuntimeObj,
            _: PluginStateBackendFactory,
            _: PluginMetricsRecorder,
            _: HashMap<String, String>,
        ) -> Result<Self, PluginInitializationError> {
            Ok(KeyedTransform)
        }
    }

    struct PanickingSink;

    #[async_trait]
    impl SupportsGracefulShutdown for PanickingSink {
        fn is_running(&self) -> bool {
            true
        }
        async fn terminate(&self) -> Result<(), PluginError> {
            Ok(())
        }
    }

    #[async_trait]
    impl SinkPlugin for PanickingSink {
        async fn initialize(&self) -> Result<(), PluginError> {
            Ok(())
        }
        async fn process_batch(&self, _: RecordBatch) -> Result<(), PluginError> {
            Ok(())
        }
        async fn process_checkpoint_marker(&self, _: CheckpointEpoch) -> Result<(), PluginError> {
            Ok(())
        }
        async fn process_checkpoint_finalizer(
            &self,
            _: CheckpointEpoch,
        ) -> Result<(), PluginError> {
            Ok(())
        }
    }

    impl PartitionedSinkPlugin for PanickingSink {
        fn describe(
            _: SchemaRef,
            _: &HashMap<String, String>,
        ) -> Result<SinkDescription, PluginInitializationError> {
            panic!("describe blew up");
        }

        fn create(
            _: PluginInstanceContext,
            _: SchemaRef,
            _: PluginAsyncRuntimeObj,
            _: PluginStateBackendFactory,
            _: PluginMetricsRecorder,
            _: HashMap<String, String>,
        ) -> Result<Self, PluginInitializationError> {
            Ok(PanickingSink)
        }
    }

    fn options(entries: &[(&str, &str)]) -> PluginOptions {
        PluginOptions::new(
            entries
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn state_backend_config() -> PluginStateBackendConfig {
        PluginStateBackendConfig::new(
            "app".to_string(),
            "blocks".to_string(),
            r#"{"backend_type":"InMemory","postgres":null,"sqlite":null}"#.to_string(),
        )
    }

    fn channels() -> PluginChannels {
        PluginChannels {
            input: PluginChannel::new(crossbeam_channel::bounded(8)),
            output: PluginChannel::new(crossbeam_channel::bounded(8)),
            metrics: PluginMetricsChannel::new(crossbeam_channel::bounded(64)),
        }
    }

    fn label_value(result: &PluginResult, key: &str) -> Option<String> {
        result
            .labels
            .iter()
            .find(|l| l.key.as_str() == key)
            .map(|l| l.value.to_string())
    }

    /// Starts the instance's dispatcher lifecycle and waits for it to exit.
    async fn terminate(result: PluginResult, channels: &PluginChannels) {
        channels
            .input
            .sender
            .send(NonExhaustive::new(PluginMsg::Terminate))
            .unwrap();
        assert!(matches!(result.execution_future.await, RResult::ROk(())));
    }

    #[test]
    fn transform_description_carries_placement_and_constraints() {
        let description = describe_partitioned_transform::<KeyedTransform>(
            RSome(schema().into()),
            options(&[(MINIMUM_PARTITIONS_OPTION, "2")]),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            description.input_placement.unwrap().into_enum().unwrap(),
            PluginInputPlacement::ByColumns {
                columns: vec![RString::from("id")].into()
            }
        );
        assert_eq!(
            description.partition_count,
            PluginPartitionCount {
                minimum: 2,
                maximum: RSome(8),
                preferred: RSome(4),
            }
        );
        let output_schema: SchemaRef = description.output_schema.unwrap().into();
        assert_eq!(output_schema.fields(), schema().fields());
    }

    #[test]
    fn source_description_has_no_input_placement() {
        let description = describe_partitioned_source::<PartitionReporter>(options(&[]))
            .unwrap()
            .unwrap();

        assert!(description.input_placement.is_none());
        assert_eq!(description.labels.len(), 1);
        assert_eq!(description.labels[0].key.as_str(), "topic");
    }

    #[test]
    fn describe_panic_is_an_initialization_error() {
        let result =
            describe_partitioned_sink::<PanickingSink>(RSome(schema().into()), options(&[]));
        match result {
            RResult::RErr(PluginInitializationError::Configuration(message)) => {
                assert!(message.contains("describe blew up"), "{message}")
            }
            other => panic!("expected a configuration error, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partition_instance_receives_its_context() {
        let channels = channels();
        let context = PluginInstanceContext {
            reference_name: "blocks".into(),
            partition_index: 1,
            partition_count: 3,
        };

        let result = create_partitioned_source::<PartitionReporter>(
            "test.source".into(),
            options(&[]),
            context,
            DirectTokioProxy::new().into_async_runtime_obj(),
            state_backend_config(),
            channels.clone(),
        )
        .unwrap();

        assert_eq!(
            label_value(&result, PARTITION_LABEL).as_deref(),
            Some("blocks:1/3")
        );
        assert!(result.output_schema.is_some());
        terminate(result, &channels).await;
    }

    /// A host that predates partitioning calls `create`; a partition-aware
    /// plugin then runs as the only partition of its node.
    #[tokio::test(flavor = "multi_thread")]
    async fn single_stream_create_runs_partition_zero_of_one() {
        let channels = channels();

        let result = create_partitioned_source_single_stream::<PartitionReporter>(
            "test.source".into(),
            options(&[]),
            DirectTokioProxy::new().into_async_runtime_obj(),
            state_backend_config(),
            channels.clone(),
        )
        .unwrap();

        assert_eq!(
            label_value(&result, PARTITION_LABEL).as_deref(),
            Some("blocks:0/1")
        );
        terminate(result, &channels).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn single_stream_create_rejects_plugins_needing_more_partitions() {
        let result = create_partitioned_source_single_stream::<PartitionReporter>(
            "test.source".into(),
            options(&[(MINIMUM_PARTITIONS_OPTION, "2")]),
            DirectTokioProxy::new().into_async_runtime_obj(),
            state_backend_config(),
            channels(),
        );

        match result {
            RResult::RErr(PluginInitializationError::Configuration(message)) => {
                assert!(message.contains("at least 2 partitions"), "{message}")
            }
            RResult::RErr(other) => panic!("expected a configuration error, got {other:?}"),
            RResult::ROk(_) => panic!("a plugin needing 2 partitions must not run as one"),
        }
    }
}

// New functions can be added to the end of the struct
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = PluginModuleRef)))]
pub struct PluginModule {
    /// Function to initialize the plugin, called only once per plugin module.
    pub init: extern "C" fn(
        logging: PluginLogging,
    ) -> RResult<PluginRuntimeConfiguration, PluginInitializationError>,

    // WARNING: do not move last_prefix_field. From abi_stable docs:
    // To ensure that libraries stay abi compatible, the first minor version of the library
    // must use the #[sabi(last_prefix_field)] attribute on some field, and every minor version
    // after that must add fields at the end (never moving that attribute). Changing the field
    // that #[sabi(last_prefix_field)] is applied to is a breaking change.
    #[sabi(last_prefix_field)]
    /// Function to create a plugin instance, e.g. a source, transform, or sink.
    pub create: extern "C" fn(
        plugin_id: RString,
        input_schema: ROption<SafeArrowSchema>,
        options: PluginOptions,
        runtime: PluginAsyncRuntimeObj,
        state_backend_config: PluginStateBackendConfig,
        message_channels: PluginChannels,
    ) -> RResult<PluginResult, PluginInitializationError>,

    /// Returns UDF descriptors provided by this plugin. Can return an empty vector.
    pub udf_descriptors:
        extern "C" fn() -> RResult<RVec<PluginUdfDescriptor>, PluginInitializationError>,

    /// Returns side output descriptors provided by this plugin. Can return an empty vector.
    pub side_output_descriptors:
        extern "C" fn() -> RResult<RVec<PluginSideOutputDescriptor>, PluginInitializationError>,

    /// Hands the plugin an out-of-band shutdown signal, called once by the
    /// host right after load, before any `create`. Appended after
    /// `last_prefix_field`; the suffix accessor reports it as absent (`None`)
    /// for a library built before this field existed, and the host then skips
    /// the call — the SDK falls back to finite defaults
    /// (`shutdown::FALLBACK_BUDGET`).
    ///
    /// NOTE: abi_stable's load-time layout check rejects a library whose
    /// module has FEWER fields than the host expects, even for suffix fields
    /// (the tolerance is one-directional; only extra fields on the library
    /// side pass). Loaders must therefore fall back to probing with the
    /// frozen twins in [`compat`] before trusting the suffix accessors —
    /// see `compat` for the contract.
    pub set_shutdown_signal: extern "C" fn(crate::shutdown::ShutdownSignalObj),

    /// Describes a partition-capable plugin without constructing an instance:
    /// validates the options and reports schema, labels, input placement, and
    /// partition-count constraints. Must not open channels, register
    /// checkpoint participants, or reserve durable resources; read-only
    /// external schema discovery is allowed.
    ///
    /// Returns `RNone` for a plugin id that only supports single-stream
    /// execution through `create`. Absent (`None` accessor) for libraries
    /// built before partitioning, whose plugins are all single-stream.
    pub describe_partitioned:
        extern "C" fn(
            plugin_id: RString,
            input_schema: ROption<SafeArrowSchema>,
            options: PluginOptions,
        )
            -> RResult<ROption<PartitionedPluginDescription>, PluginInitializationError>,

    /// Creates one partition instance of a plugin whose
    /// `describe_partitioned` returned `RSome`. Called exactly once per
    /// partition index, each call with its own channels and a
    /// partition-scoped state backend.
    pub create_partitioned: extern "C" fn(
        plugin_id: RString,
        input_schema: ROption<SafeArrowSchema>,
        options: PluginOptions,
        context: PluginInstanceContext,
        runtime: PluginAsyncRuntimeObj,
        state_backend_config: PluginStateBackendConfig,
        message_channels: PluginChannels,
    ) -> RResult<PluginResult, PluginInitializationError>,
}

impl RootModule for PluginModuleRef {
    declare_root_module_statics! {PluginModuleRef}
    const BASE_NAME: &'static str = "streamling_plugin";
    const NAME: &'static str = "streamling_plugin";
    const VERSION_STRINGS: VersionStrings = package_version_strings!();

    fn initialization(self) -> Result<Self, LibraryError> {
        Ok(self)
    }
}

/// Compatibility probes for plugin libraries built against an SDK older than
/// the newest module field.
///
/// abi_stable's layout check only tolerates a field-count difference in one
/// direction: a library may have MORE module fields than the host expects
/// (they are ignored), but a host expecting more fields than the library has
/// is rejected with a `FieldCountMismatch` — even when the extra fields sit
/// after `last_prefix_field`. The runtime suffix accessors (which return
/// `Option` guarded by the library's own recorded field count) never get a
/// chance to run.
///
/// Each submodule carries a byte-identical twin of an older
/// [`PluginModule`] shape under the same type name (the layout check compares
/// type names), so its layout matches what libraries built against that SDK
/// embed. A loader that fails the primary layout check validates the library
/// against these frozen shapes, newest to oldest; success proves every field
/// the library actually has is intact, after which obtaining the primary
/// [`super::PluginModuleRef`] with the layout check skipped is sound: every
/// accessor past the library's field count returns `None` via abi_stable's
/// runtime field guard.
///
/// Do not add fields to a twin, ever — they are fossils, not live types. A
/// new module field gets a new twin with the shape it supersedes.
pub mod compat {
    /// Five-field shape of SDK 0.2.2 and 0.2.3: everything up to
    /// `set_shutdown_signal`, before partitioned plugins existed.
    pub mod pre_partitioning {
        use crate::*;

        /// Frozen five-field twin of [`crate::PluginModule`]. See the
        /// [`crate::compat`] docs.
        #[repr(C)]
        #[derive(StableAbi)]
        #[sabi(kind(Prefix(prefix_ref = PluginModuleRef)))]
        pub struct PluginModule {
            /// See [`crate::PluginModule::init`].
            pub init:
                extern "C" fn(
                    logging: PluginLogging,
                )
                    -> RResult<PluginRuntimeConfiguration, PluginInitializationError>,

            // Same prefix boundary as the live type; moving it would desync
            // the two layouts and break the probe.
            #[sabi(last_prefix_field)]
            /// See [`crate::PluginModule::create`].
            pub create: extern "C" fn(
                plugin_id: RString,
                input_schema: ROption<SafeArrowSchema>,
                options: PluginOptions,
                runtime: PluginAsyncRuntimeObj,
                state_backend_config: PluginStateBackendConfig,
                message_channels: PluginChannels,
            )
                -> RResult<PluginResult, PluginInitializationError>,

            /// See [`crate::PluginModule::udf_descriptors`].
            pub udf_descriptors:
                extern "C" fn() -> RResult<RVec<PluginUdfDescriptor>, PluginInitializationError>,

            /// See [`crate::PluginModule::side_output_descriptors`].
            pub side_output_descriptors: extern "C" fn() -> RResult<
                RVec<PluginSideOutputDescriptor>,
                PluginInitializationError,
            >,

            /// See [`crate::PluginModule::set_shutdown_signal`].
            pub set_shutdown_signal: extern "C" fn(crate::shutdown::ShutdownSignalObj),
        }

        impl RootModule for PluginModuleRef {
            declare_root_module_statics! {PluginModuleRef}
            const BASE_NAME: &'static str = "streamling_plugin";
            const NAME: &'static str = "streamling_plugin";
            const VERSION_STRINGS: VersionStrings = package_version_strings!();

            fn initialization(self) -> Result<Self, LibraryError> {
                Ok(self)
            }
        }
    }

    /// Four-field shape of SDK 0.2.1, before `set_shutdown_signal`.
    pub mod pre_shutdown_signal {
        use crate::*;

        /// Frozen four-field twin of [`crate::PluginModule`]. See the
        /// [`crate::compat`] docs.
        #[repr(C)]
        #[derive(StableAbi)]
        #[sabi(kind(Prefix(prefix_ref = PluginModuleRef)))]
        pub struct PluginModule {
            /// See [`crate::PluginModule::init`].
            pub init:
                extern "C" fn(
                    logging: PluginLogging,
                )
                    -> RResult<PluginRuntimeConfiguration, PluginInitializationError>,

            // Same prefix boundary as the live type; moving it would desync
            // the two layouts and break the probe.
            #[sabi(last_prefix_field)]
            /// See [`crate::PluginModule::create`].
            pub create: extern "C" fn(
                plugin_id: RString,
                input_schema: ROption<SafeArrowSchema>,
                options: PluginOptions,
                runtime: PluginAsyncRuntimeObj,
                state_backend_config: PluginStateBackendConfig,
                message_channels: PluginChannels,
            )
                -> RResult<PluginResult, PluginInitializationError>,

            /// See [`crate::PluginModule::udf_descriptors`].
            pub udf_descriptors:
                extern "C" fn() -> RResult<RVec<PluginUdfDescriptor>, PluginInitializationError>,

            /// See [`crate::PluginModule::side_output_descriptors`].
            pub side_output_descriptors: extern "C" fn() -> RResult<
                RVec<PluginSideOutputDescriptor>,
                PluginInitializationError,
            >,
        }

        impl RootModule for PluginModuleRef {
            declare_root_module_statics! {PluginModuleRef}
            const BASE_NAME: &'static str = "streamling_plugin";
            const NAME: &'static str = "streamling_plugin";
            const VERSION_STRINGS: VersionStrings = package_version_strings!();

            fn initialization(self) -> Result<Self, LibraryError> {
                Ok(self)
            }
        }
    }
}

#[cfg(test)]
mod compat_layout_tests {
    use super::*;
    use abi_stable::StableAbi;
    use abi_stable::abi_stability::abi_checking::check_layout_compatibility;

    use abi_stable::type_layout::TypeLayout;

    fn frozen_twins() -> [(&'static str, &'static TypeLayout); 2] {
        [
            (
                "pre_partitioning",
                <compat::pre_partitioning::PluginModuleRef as StableAbi>::LAYOUT,
            ),
            (
                "pre_shutdown_signal",
                <compat::pre_shutdown_signal::PluginModuleRef as StableAbi>::LAYOUT,
            ),
        ]
    }

    /// Each frozen twin must stay a loadable prefix of the live module type:
    /// a host expecting the twin's fields accepts a library exporting the
    /// live (larger) module. This is the direction the compatibility probe
    /// relies on; it breaks if the live type's prefix drifts or if a field is
    /// ever added to a twin.
    #[test]
    fn frozen_twins_accept_live_module() {
        for (name, twin) in frozen_twins() {
            check_layout_compatibility(twin, <PluginModuleRef as StableAbi>::LAYOUT)
                .unwrap_or_else(|e| {
                    panic!("frozen twin {name} no longer a prefix of the live module: {e}")
                });
        }
    }

    /// Documents the asymmetry that makes the loader fallback necessary: the
    /// live module (more fields) does NOT accept a library with fewer fields,
    /// even though the missing fields are suffix fields. If a future
    /// abi_stable upgrade makes this pass, the compatibility probe (and this
    /// test) can be retired.
    #[test]
    fn live_module_still_rejects_smaller_libraries() {
        for (name, twin) in frozen_twins() {
            assert!(
                check_layout_compatibility(<PluginModuleRef as StableAbi>::LAYOUT, twin).is_err(),
                "abi_stable now tolerates missing suffix fields ({name}); the compat probe is obsolete"
            );
        }
    }

    /// The same asymmetry between the twins is why the loader probes newest
    /// to oldest: a four-field library fails the five-field probe and needs
    /// its own.
    #[test]
    fn pre_partitioning_twin_rejects_pre_shutdown_signal_library() {
        assert!(
            check_layout_compatibility(
                <compat::pre_partitioning::PluginModuleRef as StableAbi>::LAYOUT,
                <compat::pre_shutdown_signal::PluginModuleRef as StableAbi>::LAYOUT,
            )
            .is_err()
        );
    }
}
