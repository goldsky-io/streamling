use streamling::topology::PipelineTopology;
use streamling_config::LiveDataInspectConfig;
use streamling_core::admin_api;
use streamling_core::operators::inspect::LiveDataInspect;
use streamling_core::telemetry::provider::metric_key;
use tracing::{error, info};

/// Starts the admin API server and returns a handle to the spawned task
pub fn start_admin_api_server(
    port: u16,
    live_data_inspect: &'static LiveDataInspect,
) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    info!("Starting Admin API server on port {}", port);

    // Process-lifetime task: the admin server serves until the process exits
    // and holds no data needing a drain. Tracking it in a scope would only
    // make the DataPath drain wait on a server that never stops.
    #[allow(clippy::disallowed_methods)]
    tokio::spawn(async move {
        match admin_api::start_admin_server(port, live_data_inspect).await {
            Ok(()) => Ok(()),
            Err(e) => {
                error!("Admin API server error: {}", e);
                Err(e)
            }
        }
    })
}

/// Initializes live data inspection by collecting topology node keys and creating a LiveDataInspect instance
pub fn initialize_live_data_inspect(
    application_id: &str,
    live_data_inspect_config: LiveDataInspectConfig,
    pipeline_topology: &PipelineTopology,
) -> &'static LiveDataInspect {
    let mut topology_node_keys = Vec::new();

    // Need to use metric key as it is what gets passed along to the reference_name attr in WrappingExec
    topology_node_keys.extend(
        pipeline_topology
            .sources
            .keys()
            .cloned()
            .map(|key| metric_key(application_id, key.as_str())),
    );
    topology_node_keys.extend(
        pipeline_topology
            .transforms
            .keys()
            .cloned()
            .map(|key| metric_key(application_id, key.as_str())),
    );

    // Skipping sink as it can be inspected in the sink itself
    // Also, WrappingExec is measuring input stream to the sink not output
    LiveDataInspect::create_instance(live_data_inspect_config, topology_node_keys)
}
