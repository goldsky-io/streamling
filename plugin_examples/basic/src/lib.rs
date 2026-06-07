mod sink;
mod source;
mod transform;

use crate::sink::PrintSink;
use crate::source::RandomSource;
use crate::transform::FilterTransform;
use streamling_plugin::{
    init_plugin_with_async_runtime, register_plugin_sink, register_plugin_source,
    register_plugin_transform,
};

// using namespace and name; this would be "basic_plugin.random_source" in the topology
register_plugin_source!("basic_plugin", "random_source", RandomSource);
register_plugin_transform!("basic_plugin", "filter_transform", FilterTransform);
// using name only
register_plugin_sink!("print_sink", PrintSink);
init_plugin_with_async_runtime!();
