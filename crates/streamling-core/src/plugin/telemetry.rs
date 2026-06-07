use crate::telemetry::recorder::MetricsRecorder;
use abi_stable::external_types::crossbeam_channel::RReceiver;
use abi_stable::std_types::{RHashMap, RString};
use crossbeam::channel::TryRecvError;
use std::sync::Arc;
use std::time::Duration;
use streamling_plugin::ffi::PluginMetric;
use streamling_plugin::ffi::PluginMetric_NE;
use tracing::{error, info, warn};

pub fn record_plugin_metric(
    metric: PluginMetric,
    metric_metadata_id: String,
    metrics_recorder: Arc<MetricsRecorder>,
) {
    match metric {
        PluginMetric::Count { name, value, tags } => {
            if name.as_str() == "output_rows" {
                // Route through record_output_rows_count so output_rows_delta (billing) is emitted.
                // Tags from the plugin are intentionally ignored here — the dispatcher always sends
                // empty tags (ffi.rs PluginMetricsRecorder::record_count), and
                // record_output_rows_count uses the canonical metadata tags from the registry.
                metrics_recorder.record_output_rows_count(value, metric_metadata_id.as_str());
            } else {
                let tags = convert_rhash_map_to_vec(&tags);
                metrics_recorder.record_count_w_tags(
                    name.as_str(),
                    value,
                    tags,
                    metric_metadata_id.as_str(),
                );
            }
        }
        PluginMetric::Gauge { name, value, tags } => {
            let tags: Vec<(&str, &str)> = convert_rhash_map_to_vec(&tags);
            metrics_recorder.record_gauge_w_tags(
                name.as_str(),
                value,
                tags,
                metric_metadata_id.as_str(),
            );
        }
        PluginMetric::Time {
            name,
            duration_ms,
            tags,
        } => {
            let tags: Vec<(&str, &str)> = convert_rhash_map_to_vec(&tags);
            metrics_recorder.record_time_w_tags(
                name.as_str(),
                Duration::from_millis(duration_ms),
                tags,
                metric_metadata_id.as_str(),
            );
        }
        _ => {
            warn!("Unknown PluginMetric seen")
        }
    };
}

pub async fn process_plugin_metrics(
    metrics_receiver: RReceiver<PluginMetric_NE>,
    metrics_recorder: Arc<MetricsRecorder>,
    metric_metadata_id: String,
) {
    info!(
        "Started plugin metrics processing task with metric_metadata_id: {}",
        metric_metadata_id
    );
    loop {
        let metrics_recorder = metrics_recorder.clone();
        match metrics_receiver.try_recv() {
            Ok(metric) => match metric.into_enum() {
                Ok(metric_enum) => {
                    record_plugin_metric(metric_enum, metric_metadata_id.clone(), metrics_recorder);
                }
                Err(e) => {
                    error!(
                        "Failed to convert metric to enum - this indicates memory corruption or FFI boundary issues: {:?}",
                        e
                    );
                    continue;
                }
            },
            Err(TryRecvError::Empty) => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(TryRecvError::Disconnected) => {
                warn!("Plugin metrics channel disconnected, stopping metrics processing task");
                break;
            }
        }
    }
    info!(
        "Plugin metrics processing task completed for metric_metadata_id: {}",
        metric_metadata_id
    );
}

fn convert_rhash_map_to_vec(tags: &RHashMap<RString, RString>) -> Vec<(&str, &str)> {
    let tags: Vec<(&str, &str)> = tags
        .iter()
        .map(|kv| (kv.0.as_str(), kv.1.as_str()))
        .collect();
    tags
}
