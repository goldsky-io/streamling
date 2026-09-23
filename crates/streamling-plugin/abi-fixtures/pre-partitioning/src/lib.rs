//! A plugin library as built against SDK 0.2.2/0.2.3: its module has the five
//! fields that predate partitioned plugins.

use abi_stable::export_root_module;
use abi_stable::prefix_type::PrefixTypeTrait;
use abi_stable::std_types::{RHashMap, ROption, RResult, RString, RVec};
use std::sync::atomic::{AtomicBool, Ordering};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::compat::pre_partitioning::{PluginModule, PluginModuleRef};
use streamling_plugin::ffi::SafeArrowSchema;
use streamling_plugin::shutdown::{ShutdownSignalObj, install_shutdown_signal};
use streamling_plugin::{
    PluginChannels, PluginInitializationError, PluginLogging, PluginOptions, PluginResult,
    PluginRuntimeConfiguration, PluginSideOutputDescriptor, PluginStateBackendConfig,
    PluginUdfDescriptor,
};

static SHUTDOWN_SIGNAL_INSTALLED: AtomicBool = AtomicBool::new(false);

extern "C" fn init(
    _: PluginLogging,
) -> RResult<PluginRuntimeConfiguration, PluginInitializationError> {
    RResult::ROk(PluginRuntimeConfiguration {
        plugin_ids: RVec::from(vec![RString::from("abi_fixture.pre_partitioning")]),
        default_channel_caps: RHashMap::new(),
    })
}

/// Reports, instead of creating anything, whether the host handed this
/// library its shutdown signal.
extern "C" fn create(
    _: RString,
    _: ROption<SafeArrowSchema>,
    _: PluginOptions,
    _: PluginAsyncRuntimeObj,
    _: PluginStateBackendConfig,
    _: PluginChannels,
) -> RResult<PluginResult, PluginInitializationError> {
    RResult::RErr(PluginInitializationError::Configuration(RString::from(
        format!(
            "shutdown_signal_installed={}",
            SHUTDOWN_SIGNAL_INSTALLED.load(Ordering::SeqCst)
        ),
    )))
}

extern "C" fn udf_descriptors() -> RResult<RVec<PluginUdfDescriptor>, PluginInitializationError> {
    RResult::ROk(RVec::new())
}

extern "C" fn side_output_descriptors()
-> RResult<RVec<PluginSideOutputDescriptor>, PluginInitializationError> {
    RResult::ROk(RVec::new())
}

extern "C" fn set_shutdown_signal(signal: ShutdownSignalObj) {
    install_shutdown_signal(signal);
    SHUTDOWN_SIGNAL_INSTALLED.store(true, Ordering::SeqCst);
}

#[export_root_module]
pub fn get_module() -> PluginModuleRef {
    PluginModule {
        init,
        create,
        udf_descriptors,
        side_output_descriptors,
        set_shutdown_signal,
    }
    .leak_into_prefix()
}
