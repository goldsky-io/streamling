mod sink;

use crate::sink::PrintSink;
use abi_stable::std_types::{RHashMap, RNone, ROption, RResult, RString, RVec};
use abi_stable::traits::IntoReprC;
use abi_stable::{export_root_module, prefix_type::PrefixTypeTrait};
use async_ffi::FutureExt;
use streamling_plugin::api::PluginStateBackendFactory;
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::SafeArrowSchema;
use streamling_plugin::{
    PluginChannels, PluginInitializationError, PluginLogging, PluginModule, PluginModuleRef,
    PluginOptions, PluginResult, PluginRuntimeConfiguration, PluginSideOutputDescriptor,
    PluginStateBackendConfig, PluginUdfDescriptor,
};
use tracing::info;

extern "C" fn init(
    logging: PluginLogging,
) -> RResult<PluginRuntimeConfiguration, PluginInitializationError> {
    logging.initialize_logging();

    let plugin_ids = vec![
        "low_level.print_sink".to_string().into_c()
    ].into_c();

    Ok(PluginRuntimeConfiguration {
        plugin_ids,
        default_channel_caps: RHashMap::new(),
    }).into_c()
}

extern "C" fn create(
    plugin_id: RString,
    input_schema: ROption<SafeArrowSchema>,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    match plugin_id.as_str() {
        "low_level.print_sink" => create_sink(
            input_schema.expect("Input schema must be defined for sinks"),
            options,
            runtime,
            state_backend_config,
            message_channels,
        ),
        _ => Err(PluginInitializationError::NotImplemented).into_c(),
    }
}

fn create_sink(
    input_schema: SafeArrowSchema,
    options: PluginOptions,
    runtime: PluginAsyncRuntimeObj,
    state_backend_config: PluginStateBackendConfig,
    message_channels: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    info!("Creating print sink with options: {:?}", options);

    let _state_backend_factory = PluginStateBackendFactory::new(state_backend_config);

    let print_sink = PrintSink::new(input_schema.into(), options.as_rust(), message_channels);

    let rt = runtime.clone();
    let worker = async move {
        if let Err(e) = print_sink.start(rt).await {
            eprintln!("Error starting PrintSink: {}", e);
            panic!("Error starting PrintSink: {}", e);
        }
    }
    .into_ffi();

    let spawned = runtime.spawn(worker);

    let dispatcher_future = async move {
        spawned.await;
        RResult::ROk(())
    }
    .into_ffi();

    Ok(PluginResult::new(dispatcher_future, RNone)).into_c()
}

extern "C" fn udf_descriptors() -> RResult<RVec<PluginUdfDescriptor>, PluginInitializationError> {
    RResult::ROk(RVec::new())
}

extern "C" fn side_output_descriptors(
) -> RResult<RVec<PluginSideOutputDescriptor>, PluginInitializationError> {
    RResult::ROk(RVec::new())
}

#[export_root_module]
pub fn get_module() -> PluginModuleRef {
    PluginModule {
        init,
        create,
        udf_descriptors,
        side_output_descriptors,
    }
    .leak_into_prefix()
}
