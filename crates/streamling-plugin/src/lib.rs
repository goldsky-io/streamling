#![allow(non_local_definitions)]

pub mod api;
pub mod r#async;
mod dispatch;
pub mod ffi;

use crate::api::PluginStateBackendFactory;
pub use crate::api::{
    CheckpointEpoch, PluginError, PluginStateBackend, PreprocessorPlugin, SideOutputPlugin,
    SinkPlugin, SourcePlugin, TransformPlugin,
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
use std::collections::HashMap;
use std::sync::Arc;
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
        "unknown panic during plugin creation".to_string()
    }
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
    info!("Creating {} with options: {:?}", id, options);

    let state_backend_factory = PluginStateBackendFactory::new(state_backend_config);
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
    let worker = async move {
        match dispatcher.start(rt).await {
            Ok(()) => (),
            Err(e) => {
                error!("Plugin error {}: {:?}", id, e);
                panic!("Plugin error {}: {:?}", id, e);
            }
        }
    }
    .into_ffi();

    let spawned = runtime.spawn(worker);

    let dispatcher_future = async move {
        spawned.await;
        RResult::ROk(())
    }
    .into_ffi();

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
    info!("Creating {} with options: {:?}", id, options);

    let state_backend_factory = PluginStateBackendFactory::new(state_backend_config);
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
    let worker = async move {
        match dispatcher.start(rt).await {
            Ok(()) => (),
            Err(e) => {
                error!("Plugin error {}: {:?}", id, e);
                panic!("Plugin error {}: {:?}", id, e);
            }
        }
    }
    .into_ffi();

    let spawned = runtime.spawn(worker);

    let dispatcher_future = async move {
        spawned.await;
        RResult::ROk(())
    }
    .into_ffi();

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
    info!("Creating {} with options: {:?}", id, options);

    let state_backend_factory = PluginStateBackendFactory::new(state_backend_config);
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
    let worker = async move {
        let dispatcher = SinkPluginDispatcher::new(message_channels, sink);
        match dispatcher.start(rt).await {
            Ok(()) => (),
            Err(e) => {
                error!("Plugin error {}: {:?}", id, e);
                panic!("Plugin error {}: {:?}", id, e);
            }
        }
    }
    .into_ffi();

    let spawned = runtime.spawn(worker);

    let dispatcher_future = async move {
        spawned.await;
        RResult::ROk(())
    }
    .into_ffi();

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

    let worker_error: Arc<std::sync::OnceLock<String>> = Arc::new(std::sync::OnceLock::new());
    let worker_error_writer = worker_error.clone();

    let worker = async move {
        if let Err(e) = dispatcher.start().await {
            error!("Preprocessor plugin error {}: {:?}", id, e);
            let _ = worker_error_writer.set(format!("{e}"));
        }
    }
    .into_ffi();

    let spawned = runtime.spawn(worker);

    let dispatcher_future = async move {
        spawned.await;
        match worker_error.get() {
            Some(err) => RResult::RErr(RString::from(err.clone())),
            None => RResult::ROk(()),
        }
    }
    .into_ffi();

    Ok(PluginResult::new(dispatcher_future, RNone)).into_c()
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
