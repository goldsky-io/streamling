use crate::operators::inspect::LiveDataInspect;
use crate::telemetry::provider::get_reference_name_from_metric_key;
use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::OnceLock;
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, info, warn};

/// State for the admin API server
#[derive(Clone)]
pub struct AdminApiState {
    /// Reference to the LiveDataInspect singleton
    live_data_inspect: &'static LiveDataInspect,
}

impl AdminApiState {
    pub fn new(live_data_inspect: &'static LiveDataInspect) -> Self {
        Self { live_data_inspect }
    }
}

/// Query parameters for the live-data endpoint
#[derive(Deserialize)]
pub struct LiveDataQuery {
    /// Optional comma-separated list of topology node keys to filter
    #[serde(default)]
    topology_node_keys: Option<String>,
}

/// Handler for the /admin/live-data SSE endpoint
async fn live_data_handler(
    State(state): State<AdminApiState>,
    Query(params): Query<LiveDataQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    info!("New SSE connection established for /admin/live-data");

    // Parse the topology node keys filter if provided
    let topology_node_keys_to_filter: Option<HashSet<String>> = params
        .topology_node_keys
        .map(|names| names.split(',').map(|s| s.trim().to_string()).collect());

    if let Some(ref nodes) = topology_node_keys_to_filter {
        debug!("Filtering live data for topology nodes: {:?}", nodes);
    } else {
        debug!("No filter specified, streaming all topology nodes");
    }

    // Get all stored data first and send as initial events
    let all_keys = state.live_data_inspect.get_all_topology_node_keys();
    let mut initial_events = Vec::new();

    for key in all_keys {
        if let Some(ref filter) = topology_node_keys_to_filter
            && !filter.contains(&get_reference_name_from_metric_key(&key))
        {
            continue;
        }

        if let Some(stored_data) = state.live_data_inspect.get_stored_data(&key) {
            for data in stored_data {
                initial_events.push((get_reference_name_from_metric_key(&key), data));
            }
        }
    }

    debug!("Sending {} initial stored events", initial_events.len());

    // Subscribe to the broadcast channel for new events
    let rx = state.live_data_inspect.subscribe();
    let broadcast_stream = BroadcastStream::new(rx);

    // Create the SSE stream
    let stream = async_stream::stream! {
        // First, send all stored data as initial events
        for (topology_node_key, data) in initial_events {
            let topology_node_key = get_reference_name_from_metric_key(&topology_node_key);
            let event_data = format!("{{\"topology_node_key\":\"{}\",\"data\":{}}}", topology_node_key, data);
            let event = Event::default().data(event_data);
            yield Ok::<_, Infallible>(event);
        }

        // Then stream new events from the broadcast channel
        let mut stream = broadcast_stream;
        while let Some(result) = stream.next().await {
            match result {
                Ok(live_event) => {
                    // Apply filter if specified
                    let topology_node_key = get_reference_name_from_metric_key(&live_event.topology_node_key);
                    if let Some(ref filter) = topology_node_keys_to_filter
                        && !filter.contains(&topology_node_key) {
                            continue;
                        }

                    let event_data = format!(
                        "{{\"topology_node_key\":\"{}\",\"data\":{}}}",
                        topology_node_key, live_event.data
                    );

                    let event = Event::default().data(event_data);
                    yield Ok(event);
                }
                Err(e) => {
                    warn!("Broadcast stream error: {:?}, ending SSE stream", e);
                    break;
                }
            }
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

/// Terminal pipeline error surfaced through the admin API in preview
/// tolerant mode: the process stays alive after a fatal component failure
/// purely to serve the pre-crash live-data buffers plus this record.
#[derive(Serialize, Clone, Debug)]
pub struct PipelineErrorRecord {
    /// Topology node the error was attributed to (`StreamlingError::node`),
    /// matching the `topology_node_key` values used by /admin/live-data.
    pub node: Option<String>,
    pub message: String,
    pub internal: bool,
    pub retriable: bool,
}

static PIPELINE_ERROR: OnceLock<PipelineErrorRecord> = OnceLock::new();

/// Record the terminal pipeline error (first write wins).
pub fn record_pipeline_error(record: PipelineErrorRecord) {
    let _ = PIPELINE_ERROR.set(record);
}

/// Handler for /admin/error: 204 while the pipeline is healthy, the terminal
/// error as JSON once one has been recorded.
async fn error_handler() -> axum::response::Response {
    match PIPELINE_ERROR.get() {
        Some(record) => axum::Json(record.clone()).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

/// Create and configure the admin API router
pub fn create_admin_router(live_data_inspect: &'static LiveDataInspect) -> Router {
    let state = AdminApiState::new(live_data_inspect);

    Router::new()
        .route("/admin/live-data", get(live_data_handler))
        .route("/admin/error", get(error_handler))
        .with_state(state)
}

/// Start the admin API server on the specified port
pub async fn start_admin_server(
    port: u16,
    live_data_inspect: &'static LiveDataInspect,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = create_admin_router(live_data_inspect);

    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    info!("Admin API server listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
