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

/// `diagnostics_key` names the instance in shutdown diagnostics; metrics are
/// recorded under the node's `metric_metadata_id`.
///
/// `cancel` is the owning scope's token. The channel never disconnects on its
/// own — the host and the plugin each hold both ends for the process's whole
/// life — so without observing the token this loop runs forever and its scope
/// can only ever "blow its drain budget slice" during shutdown. On
/// cancellation the loop drains whatever is already queued (it only exits from
/// the Empty arm), then stops; the scope's drain stage runs after the plugin
/// dispatcher's flush, so everything the dispatcher emitted has been consumed
/// by then.
pub async fn process_plugin_metrics(
    metrics_receiver: RReceiver<PluginMetric_NE>,
    metrics_recorder: Arc<MetricsRecorder>,
    metric_metadata_id: String,
    diagnostics_key: String,
    cancel: tokio_util::sync::CancellationToken,
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
                    // Dispatcher liveness markers feed the shutdown
                    // diagnostics maps instead of telemetry.
                    if crate::plugin::diagnostics::intercept_metric(&diagnostics_key, &metric_enum)
                    {
                        continue;
                    }
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
                if cancel.is_cancelled() {
                    // Queue drained and the scope is winding down.
                    break;
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use abi_stable::external_types::crossbeam_channel;
    use abi_stable::nonexhaustive_enum::NonExhaustive;

    // The forwarder must exit on scope cancellation, draining what is already
    // queued first. Its channel never disconnects (both ends outlive it), so
    // before this contract the task ran forever and its scope could only blow
    // its drain slice — the shape of a field failure where a wedged sink's
    // forwarders were the last thing pinning the drain.
    #[tokio::test]
    // Test harness plumbing: the bounded channel cannot block (capacity 8, one
    // message) and the raw spawn is the subject under test's own lifetime,
    // joined below — no drain ladder exists in a unit test to track it.
    #[allow(clippy::disallowed_methods)]
    async fn metrics_forwarder_drains_queue_then_exits_on_cancellation() {
        let (tx, rx) = crossbeam_channel::bounded::<PluginMetric_NE>(8);
        let cancel = tokio_util::sync::CancellationToken::new();

        tx.try_send(NonExhaustive::new(PluginMetric::Count {
            name: RString::from("some_metric"),
            value: 1,
            tags: RHashMap::new(),
        }))
        .unwrap();

        let handle = tokio::spawn(process_plugin_metrics(
            rx,
            crate::telemetry::recorder::get_metrics_recorder(),
            "test-forwarder".to_string(),
            "test-forwarder".to_string(),
            cancel.clone(),
        ));

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("forwarder must exit promptly after cancellation")
            .expect("forwarder task must not panic");
        // The sender is still alive, proving exit came from the token, not a
        // channel disconnect.
        drop(tx);
    }

    // A node's partition instances share its metric key but must be told
    // apart in shutdown diagnostics, which name the dispatcher that wedged.
    #[tokio::test]
    #[allow(clippy::disallowed_methods)]
    async fn liveness_is_attributed_to_the_instance_not_the_node() {
        let (tx, rx) = crossbeam_channel::bounded::<PluginMetric_NE>(8);
        let cancel = tokio_util::sync::CancellationToken::new();
        tx.try_send(NonExhaustive::new(PluginMetric::Count {
            name: RString::from(crate::plugin::diagnostics::HEARTBEAT_METRIC),
            value: 1,
            tags: RHashMap::new(),
        }))
        .unwrap();

        let handle = tokio::spawn(process_plugin_metrics(
            rx,
            crate::telemetry::recorder::get_metrics_recorder(),
            "app::liveness_node".to_string(),
            "liveness_node[1]".to_string(),
            cancel.clone(),
        ));
        cancel.cancel();
        handle.await.unwrap();

        let pending = ["liveness_node[1]".to_string()].into_iter().collect();
        let described = crate::plugin::diagnostics::describe_pending(&pending);
        assert!(
            described.contains("liveness_node[1]: last heard"),
            "{described}"
        );
        drop(tx);
    }

    // Forwarders watch the STAGE token, not the root-child token: the scope
    // tokens are children of the controller root, so they fire the moment
    // shutdown is REQUESTED — but a plugin sink's terminal ack arrives after
    // that request (the terminal marker rides the last batch and the sink
    // flushes before acking). A forwarder that exits at request time silently
    // drops that ack and the terminal epoch can never finalize. It must keep
    // serving until its OWN drain stage begins.
    #[tokio::test]
    #[allow(clippy::disallowed_methods)]
    async fn metrics_forwarder_survives_shutdown_request_until_its_stage_drains() {
        use crate::shutdown::{DrainStage, ShutdownController};

        let controller = ShutdownController::new(Duration::from_secs(5));
        let scope = controller.scope_at("forwarder-under-test", DrainStage::PostPlugin);
        let (tx, rx) = crossbeam_channel::bounded::<PluginMetric_NE>(8);

        let handle = scope.spawn(process_plugin_metrics(
            rx,
            crate::telemetry::recorder::get_metrics_recorder(),
            "test-stage-forwarder".to_string(),
            "test-stage-forwarder".to_string(),
            scope.stage_token().clone(),
        ));

        // Shutdown REQUESTED (local tokens only; the global watch is one-way
        // and must not be flipped from a unit test). The root-child token has
        // fired — the forwarder must still be serving.
        controller.cancel_local();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !handle.is_finished(),
            "forwarder must keep serving between the shutdown request and its stage drain"
        );

        // Its stage drains — now it must wind down (drain() itself awaits the
        // tracker; a wedged forwarder would surface as the overrun warn and a
        // still-unfinished handle).
        controller.drain(DrainStage::PostPlugin, None).await;
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("forwarder must exit once its stage drain begins")
            .expect("forwarder task must not panic");
        drop(tx);
    }
}
