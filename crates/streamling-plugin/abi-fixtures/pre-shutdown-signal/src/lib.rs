//! A plugin library as built against SDK 0.2.1: its module has the four
//! fields that predate `set_shutdown_signal`.

use abi_stable::export_root_module;
use abi_stable::prefix_type::PrefixTypeTrait;
use abi_stable::std_types::{RHashMap, ROption, RResult, RString, RVec};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::compat::pre_shutdown_signal::{PluginModule, PluginModuleRef};
use streamling_plugin::ffi::SafeArrowSchema;
use streamling_plugin::{
    PluginChannels, PluginInitializationError, PluginLogging, PluginOptions, PluginResult,
    PluginRuntimeConfiguration, PluginSideOutputDescriptor, PluginStateBackendConfig,
    PluginUdfDescriptor,
};

extern "C" fn init(
    _: PluginLogging,
) -> RResult<PluginRuntimeConfiguration, PluginInitializationError> {
    RResult::ROk(PluginRuntimeConfiguration {
        plugin_ids: RVec::from(vec![RString::from("abi_fixture.pre_shutdown_signal")]),
        default_channel_caps: RHashMap::new(),
    })
}

/// Reports, instead of creating anything, that a library of this age never
/// receives the shutdown signal.
extern "C" fn create(
    _: RString,
    _: ROption<SafeArrowSchema>,
    _: PluginOptions,
    _: PluginAsyncRuntimeObj,
    _: PluginStateBackendConfig,
    _: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    RResult::RErr(PluginInitializationError::Configuration(RString::from(
        "shutdown_signal_installed=false",
    )))
}

extern "C" fn udf_descriptors() -> RResult<RVec<PluginUdfDescriptor>, PluginInitializationError> {
    RResult::ROk(RVec::new())
}

extern "C" fn side_output_descriptors()
-> RResult<RVec<PluginSideOutputDescriptor>, PluginInitializationError> {
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
