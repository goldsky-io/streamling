mod partitioned;
mod sink;
mod source;
mod transform;

use crate::partitioned::{PartitionedFileSink, PartitionedSource, PartitionedTransform};
use crate::sink::PrintSink;
use crate::source::RandomSource;
use crate::transform::FilterTransform;
use streamling_plugin::{
    init_plugin_with_async_runtime, register_partitioned_plugin_sink,
    register_partitioned_plugin_source, register_partitioned_plugin_transform,
    register_plugin_sink, register_plugin_source, register_plugin_transform,
};

// using namespace and name; this would be "basic_plugin.random_source" in the topology
register_plugin_source!("basic_plugin", "random_source", RandomSource);
register_plugin_transform!("basic_plugin", "filter_transform", FilterTransform);
// using name only
register_plugin_sink!("print_sink", PrintSink);
// partition-aware: the host runs one instance per physical stream
register_partitioned_plugin_source!("basic_plugin", "partitioned_source", PartitionedSource);
register_partitioned_plugin_transform!("basic_plugin", "partitioned_transform", PartitionedTransform);
register_partitioned_plugin_sink!("basic_plugin", "partitioned_file_sink", PartitionedFileSink);
init_plugin_with_async_runtime!();
