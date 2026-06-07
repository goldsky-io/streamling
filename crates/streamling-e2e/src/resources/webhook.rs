//! Webhook server resource for capturing HTTP requests from streamling.
//!
//! This module provides a simple HTTP server that can be used to test webhook sinks.
//! The server captures all incoming requests and stores them for verification.

use crate::{E2eError, Result};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tracing::info;

/// A captured webhook request
#[derive(Debug, Clone)]
pub struct CapturedRequest {
    /// The request body as bytes
    pub body: Vec<u8>,
}

/// Shared state for the webhook server
#[derive(Clone)]
struct WebhookState {
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    response_plan: Arc<Mutex<Vec<StatusCode>>>,
    next_response_index: Arc<Mutex<usize>>,
}

impl WebhookState {
    fn with_response_plan(response_plan: Vec<StatusCode>) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            response_plan: Arc::new(Mutex::new(response_plan)),
            next_response_index: Arc::new(Mutex::new(0)),
        }
    }

    fn add_request(&self, body: Vec<u8>) {
        let mut requests = self.requests.lock().unwrap();
        requests.push(CapturedRequest { body });
    }

    fn get_requests(&self) -> Vec<CapturedRequest> {
        let requests = self.requests.lock().unwrap();
        requests.clone()
    }

    fn request_count(&self) -> usize {
        let requests = self.requests.lock().unwrap();
        requests.len()
    }

    fn next_status(&self) -> StatusCode {
        let response_plan = self.response_plan.lock().unwrap();
        let mut next_response_index = self.next_response_index.lock().unwrap();
        let status = response_plan
            .get(*next_response_index)
            .copied()
            .unwrap_or(StatusCode::OK);
        *next_response_index += 1;
        status
    }
}

/// Webhook server resource for capturing HTTP requests
pub struct WebhookResource {
    /// The base URL of the webhook server (e.g., "http://127.0.0.1:8080")
    pub url: String,
    /// The port the server is listening on
    pub port: u16,
    /// Shared state containing captured requests
    state: WebhookState,
    /// Shutdown signal sender
    #[allow(dead_code)]
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl WebhookResource {
    /// Start a new webhook server on an available port
    pub async fn new() -> Result<Self> {
        Self::new_with_response_plan(Vec::new()).await
    }

    /// Start a new webhook server with a scripted sequence of response statuses.
    pub async fn new_with_response_plan(response_plan: Vec<StatusCode>) -> Result<Self> {
        // Find an available port
        let listener = TcpListener::bind("127.0.0.1:0").map_err(E2eError::Io)?;
        let port = listener.local_addr().map_err(E2eError::Io)?.port();
        drop(listener); // Release the port so axum can bind to it

        let state = WebhookState::with_response_plan(response_plan);
        let state_clone = state.clone();

        // Create the router
        let app = Router::new()
            .route("/webhook", post(handle_webhook))
            .route("/webhook/{path}", post(handle_webhook))
            .with_state(state_clone);

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // Bind and start the server
        let addr = format!("127.0.0.1:{}", port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(E2eError::Io)?;

        info!("Starting webhook server at: {}", addr);

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
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// Get the URL for the webhook endpoint
    pub fn webhook_url(&self) -> String {
        format!("{}/webhook", self.url)
    }

    /// Get the number of requests received
    pub fn request_count(&self) -> usize {
        self.state.request_count()
    }

    /// Get all captured requests
    pub fn get_requests(&self) -> Vec<CapturedRequest> {
        self.state.get_requests()
    }

    /// Get all captured request bodies as strings (for JSON verification)
    pub fn get_request_bodies_as_string(&self) -> Vec<String> {
        self.state
            .get_requests()
            .into_iter()
            .filter_map(|r| String::from_utf8(r.body).ok())
            .collect()
    }

    /// Parse all captured request bodies as JSON
    pub fn get_request_bodies_as_json(&self) -> Vec<serde_json::Value> {
        self.get_request_bodies_as_string()
            .into_iter()
            .filter_map(|s| serde_json::from_str(&s).ok())
            .collect()
    }

    /// Wait for at least N requests with a timeout
    pub async fn wait_for_requests(&self, count: usize, timeout: std::time::Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if self.request_count() >= count {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        false
    }
}

/// Handler for webhook requests - captures the body and returns 200 OK
async fn handle_webhook(State(state): State<WebhookState>, body: Bytes) -> StatusCode {
    state.add_request(body.to_vec());
    state.next_status()
}
