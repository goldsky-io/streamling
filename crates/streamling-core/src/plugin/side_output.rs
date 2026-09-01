use crate::error::Result;
use crate::plugin::telemetry::process_plugin_metrics;
use crate::side_output::{SourceSideOutput, SupportsSideOutputs};
use crate::streamling_user_bail;
use crate::telemetry::provider::metric_key;
use crate::telemetry::recorder::get_metrics_recorder;
use abi_stable::external_types::crossbeam_channel;
use abi_stable::std_types::{RResult, RString};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};
use streamling_plugin::ffi::{
    PluginMetricsChannel, PluginMetricsRecorder, SafeArrowArray, SafeArrowSchema,
};
use streamling_plugin::{PluginOptions, PluginSideOutputDescriptor};
use tracing::{info, warn};

lazy_static! {
    static ref PLUGIN_SIDE_OUTPUT_DESCRIPTORS: RwLock<Vec<StoredSideOutputDescriptor>> =
        RwLock::new(Vec::new());
}

struct StoredSideOutputDescriptor {
    id: String,
    initialize: extern "C" fn(
        RString,
        SafeArrowSchema,
        PluginOptions,
        PluginMetricsRecorder,
    ) -> RResult<(), RString>,
    process_batch: extern "C" fn(RString, SafeArrowArray) -> RResult<(), RString>,
    shutdown: extern "C" fn() -> RResult<(), RString>,
}

pub fn store_plugin_side_outputs(
    descriptors: abi_stable::std_types::RVec<PluginSideOutputDescriptor>,
) {
    let mut registry = PLUGIN_SIDE_OUTPUT_DESCRIPTORS
        .write()
        .expect("side output descriptors write lock");
    for descriptor in descriptors {
        let id = descriptor.id.to_string();
        info!("Registered plugin side output: {}", id);
        registry.push(StoredSideOutputDescriptor {
            id,
            initialize: descriptor.initialize,
            process_batch: descriptor.process_batch,
            shutdown: descriptor.shutdown,
        });
    }
}

pub struct PluginSideOutputAdapter {
    source_name: String,
    process_batch_fn: extern "C" fn(RString, SafeArrowArray) -> RResult<(), RString>,
}

impl fmt::Debug for PluginSideOutputAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginSideOutputAdapter")
            .field("source_name", &self.source_name)
            .finish()
    }
}

#[async_trait]
impl SourceSideOutput for PluginSideOutputAdapter {
    async fn process(&self, batch: &RecordBatch) -> datafusion::common::Result<()> {
        let batch_data: SafeArrowArray = batch.clone().into();
        match (self.process_batch_fn)(RString::from(self.source_name.as_str()), batch_data) {
            RResult::ROk(()) => Ok(()),
            RResult::RErr(msg) => Err(DataFusionError::Execution(format!(
                "Side output plugin error: {}",
                msg
            ))),
        }
    }
}

pub fn register_plugin_side_outputs(
    enabled_ids: &[String],
    side_output_operators: &HashMap<String, Arc<dyn SupportsSideOutputs>>,
    source_schemas: &HashMap<String, SchemaRef>,
    options: &HashMap<String, HashMap<String, String>>,
    application_id: &str,
    channel_capacity: usize,
) -> Result<()> {
    let registry = PLUGIN_SIDE_OUTPUT_DESCRIPTORS
        .read()
        .expect("side output descriptors read lock");
    for descriptor in registry.iter() {
        if !enabled_ids.contains(&descriptor.id) {
            continue;
        }
        let plugin_options = options.get(&descriptor.id).cloned().unwrap_or_default();
        for (source_name, schema) in source_schemas {
            // Create a per-source metrics channel
            let metrics_channel =
                PluginMetricsChannel::new(crossbeam_channel::bounded(channel_capacity));
            let metrics_recorder = PluginMetricsRecorder::new(metrics_channel.sender.clone());

            // Spawn metrics processing task with source-specific metric_metadata_id
            let metric_metadata_id = metric_key(application_id, source_name);
            let host_metrics_recorder = get_metrics_recorder();
            tokio::spawn(process_plugin_metrics(
                metrics_channel.receiver,
                host_metrics_recorder,
                metric_metadata_id,
            ));

            let init_result = (descriptor.initialize)(
                RString::from(source_name.as_str()),
                SafeArrowSchema::from(schema.clone()),
                PluginOptions::new(plugin_options.clone()),
                metrics_recorder,
            );
            if let RResult::RErr(msg) = init_result {
                streamling_user_bail!(
                    "Side output '{}' failed to initialize for source '{}': {}",
                    descriptor.id,
                    source_name,
                    msg
                );
            }

            let adapter = Arc::new(PluginSideOutputAdapter {
                source_name: source_name.clone(),
                process_batch_fn: descriptor.process_batch,
            });
            if let Some(handle) = side_output_operators.get(source_name) {
                handle.add_side_output(adapter);
            }
        }
        info!(
            "Side output '{}' registered on {} source(s)",
            descriptor.id,
            source_schemas.len()
        );
    }
    Ok(())
}

pub fn shutdown_plugin_side_outputs() {
    let registry = PLUGIN_SIDE_OUTPUT_DESCRIPTORS
        .read()
        .expect("side output descriptors read lock");
    for descriptor in registry.iter() {
        if let RResult::RErr(msg) = (descriptor.shutdown)() {
            warn!("Side output '{}' shutdown error: {}", descriptor.id, msg);
        }
    }
}
