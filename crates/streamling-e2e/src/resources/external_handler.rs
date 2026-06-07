//! External handler server resource for testing handler transforms.
//!
//! This module provides an HTTP server that can receive requests from external handler
//! transforms and respond with modified data, similar to the test webhook in the unit tests.

use crate::{E2eError, Result};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use serde::{Deserialize, Serialize};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tracing::info;

/// A captured handler request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapturedHandlerRequest {
    /// The request body as string
    pub body: String,
    /// The endpoint that was called
    pub endpoint: String,
}

/// Shared state for the handler server
#[derive(Clone)]
struct HandlerState {
    requests: Arc<Mutex<Vec<CapturedHandlerRequest>>>,
    capture_file: Option<PathBuf>,
}

impl HandlerState {
    fn new(capture_file: Option<PathBuf>) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            capture_file,
        }
    }

    fn add_request(&self, endpoint: &str, body: String) {
        let request = CapturedHandlerRequest {
            body: body.clone(),
            endpoint: endpoint.to_string(),
        };

        let mut requests = self.requests.lock().unwrap();
        requests.push(request.clone());

        // Also write to file if configured
        if let Some(ref path) = self.capture_file {
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                use std::io::Write;
                let _ = writeln!(
                    file,
                    "{}",
                    serde_json::to_string(&request).unwrap_or_default()
                );
            }
        }
    }

    fn get_requests(&self) -> Vec<CapturedHandlerRequest> {
        let requests = self.requests.lock().unwrap();
        requests.clone()
    }

    fn request_count(&self) -> usize {
        let requests = self.requests.lock().unwrap();
        requests.len()
    }
}

/// External handler server resource for testing handler transforms
pub struct ExternalHandlerResource {
    /// The base URL of the handler server (e.g., "http://127.0.0.1:8080")
    pub url: String,
    /// The port the server is listening on
    pub port: u16,
    /// Shared state containing captured requests
    state: HandlerState,
    /// Path to the capture file (if any)
    pub capture_file: Option<PathBuf>,
    /// Shutdown signal sender
    #[allow(dead_code)]
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl ExternalHandlerResource {
    /// Start a new external handler server on an available port
    pub async fn new() -> Result<Self> {
        Self::with_capture_file(None).await
    }

    /// Start a new external handler server with request capture to a file
    pub async fn with_capture_file(capture_file: Option<PathBuf>) -> Result<Self> {
        // Find an available port
        let listener = TcpListener::bind("127.0.0.1:0").map_err(E2eError::Io)?;
        let port = listener.local_addr().map_err(E2eError::Io)?.port();
        drop(listener); // Release the port so axum can bind to it

        let state = HandlerState::new(capture_file.clone());
        let state_clone = state.clone();

        // Create the router with all handler endpoints
        let app = Router::new()
            // Single row handlers (one request per row)
            .route("/handler_slim", post(handle_slim_single))
            .route("/handler_slim_envelope", post(handle_slim_single_envelope))
            // Batch handlers (one request for all rows)
            .route("/handler_slim_batch", post(handle_slim_batch))
            .route(
                "/handler_slim_batch_envelope",
                post(handle_slim_batch_envelope),
            )
            // Generic passthrough handler (captures but doesn't modify)
            .route("/handler_passthrough", post(handle_passthrough))
            .with_state(state_clone);

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Bind and start the server
        let addr = format!("127.0.0.1:{}", port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(E2eError::Io)?;

        info!("Starting external handler server at: {}", addr);

        // Spawn the server
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .ok();
        });

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        Ok(Self {
            url: format!("http://127.0.0.1:{}", port),
            port,
            state,
            capture_file,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// Get the URL for the slim single-row handler
    pub fn slim_handler_url(&self) -> String {
        format!("{}/handler_slim", self.url)
    }

    /// Get the URL for the slim single-row handler with envelope
    pub fn slim_handler_envelope_url(&self) -> String {
        format!("{}/handler_slim_envelope", self.url)
    }

    /// Get the URL for the slim batch handler
    pub fn slim_batch_handler_url(&self) -> String {
        format!("{}/handler_slim_batch", self.url)
    }

    /// Get the URL for the slim batch handler with envelope
    pub fn slim_batch_handler_envelope_url(&self) -> String {
        format!("{}/handler_slim_batch_envelope", self.url)
    }

    /// Get the URL for the passthrough handler
    pub fn passthrough_handler_url(&self) -> String {
        format!("{}/handler_passthrough", self.url)
    }

    /// Get the number of requests received
    pub fn request_count(&self) -> usize {
        self.state.request_count()
    }

    /// Get all captured requests
    pub fn get_requests(&self) -> Vec<CapturedHandlerRequest> {
        self.state.get_requests()
    }

    /// Read captured requests from the file (if configured)
    pub fn read_captured_file(&self) -> Result<Vec<CapturedHandlerRequest>> {
        match &self.capture_file {
            Some(path) => {
                let content = std::fs::read_to_string(path).map_err(E2eError::Io)?;
                let requests: Vec<CapturedHandlerRequest> = content
                    .lines()
                    .filter_map(|line| serde_json::from_str(line).ok())
                    .collect();
                Ok(requests)
            }
            None => Ok(Vec::new()),
        }
    }
}

// ============================================================================
// Handler Implementations
// ============================================================================

/// Envelope format version 0 - flat JSON with _gs_op field
#[derive(Debug, Deserialize, Serialize)]
struct EnvelopeV0 {
    id: String,
    data: String,
    #[serde(rename = "_gs_op")]
    gs_op: String,
}

/// Metadata for envelope version 1
#[derive(Debug, Deserialize, Serialize)]
struct EnvelopeMetadata {
    op: String,
}

/// Inner data for envelope version 1
#[derive(Debug, Deserialize, Serialize)]
struct EnvelopeV1Data {
    id: String,
    data: String,
    #[serde(rename = "_gs_op")]
    gs_op: String,
}

/// Envelope format version 1 - wrapped with metadata.op
#[derive(Debug, Deserialize, Serialize)]
struct EnvelopeV1 {
    metadata: EnvelopeMetadata,
    data: EnvelopeV1Data,
}

/// Handle single row requests without envelope (payload_version=0)
/// Modifies the data field by adding "updated-" prefix
async fn handle_slim_single(
    State(state): State<HandlerState>,
    body: Bytes,
) -> (StatusCode, String) {
    let body_str = String::from_utf8_lossy(&body).to_string();
    state.add_request("handler_slim", body_str.clone());

    // Parse and modify the data
    match serde_json::from_slice::<EnvelopeV0>(&body) {
        Ok(mut req) => {
            req.data = format!("updated-{}", req.data);
            let response = serde_json::to_string(&req).unwrap_or_default();
            (StatusCode::OK, response)
        }
        Err(e) => {
            tracing::error!("Failed to parse request: {} - body: {}", e, body_str);
            (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e))
        }
    }
}

/// Handle single row requests with envelope (payload_version=1)
async fn handle_slim_single_envelope(
    State(state): State<HandlerState>,
    body: Bytes,
) -> (StatusCode, String) {
    let body_str = String::from_utf8_lossy(&body).to_string();
    state.add_request("handler_slim_envelope", body_str.clone());

    match serde_json::from_slice::<EnvelopeV1>(&body) {
        Ok(mut req) => {
            req.data.data = format!("updated-{}", req.data.data);
            let response = serde_json::to_string(&req).unwrap_or_default();
            (StatusCode::OK, response)
        }
        Err(e) => {
            tracing::error!(
                "Failed to parse envelope request: {} - body: {}",
                e,
                body_str
            );
            (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e))
        }
    }
}

/// Handle batch requests without envelope (payload_version=0)
async fn handle_slim_batch(State(state): State<HandlerState>, body: Bytes) -> (StatusCode, String) {
    let body_str = String::from_utf8_lossy(&body).to_string();
    state.add_request("handler_slim_batch", body_str.clone());

    match serde_json::from_slice::<Vec<EnvelopeV0>>(&body) {
        Ok(rows) => {
            let updated: Vec<EnvelopeV0> = rows
                .into_iter()
                .map(|mut r| {
                    r.data = format!("updated-{}", r.data);
                    r
                })
                .collect();
            let response = serde_json::to_string(&updated).unwrap_or_default();
            (StatusCode::OK, response)
        }
        Err(e) => {
            tracing::error!("Failed to parse batch request: {} - body: {}", e, body_str);
            (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e))
        }
    }
}

/// Handle batch requests with envelope (payload_version=1)
async fn handle_slim_batch_envelope(
    State(state): State<HandlerState>,
    body: Bytes,
) -> (StatusCode, String) {
    let body_str = String::from_utf8_lossy(&body).to_string();
    state.add_request("handler_slim_batch_envelope", body_str.clone());

    match serde_json::from_slice::<Vec<EnvelopeV1>>(&body) {
        Ok(rows) => {
            let updated: Vec<EnvelopeV1> = rows
                .into_iter()
                .map(|mut r| {
                    r.data.data = format!("updated-{}", r.data.data);
                    r
                })
                .collect();
            let response = serde_json::to_string(&updated).unwrap_or_default();
            (StatusCode::OK, response)
        }
        Err(e) => {
            tracing::error!(
                "Failed to parse batch envelope request: {} - body: {}",
                e,
                body_str
            );
            (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e))
        }
    }
}

/// Passthrough handler - captures request but returns it unchanged
async fn handle_passthrough(
    State(state): State<HandlerState>,
    body: Bytes,
) -> (StatusCode, String) {
    let body_str = String::from_utf8_lossy(&body).to_string();
    state.add_request("handler_passthrough", body_str.clone());
    (StatusCode::OK, body_str)
}
