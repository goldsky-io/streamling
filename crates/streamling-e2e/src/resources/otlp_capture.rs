//! OTLP/HTTP-JSON metrics receiver for tests that check exported series exactly.
//!
//! Prometheus' OTLP receiver drops delta-temporality sums, so the billing
//! `*_delta` series never reach it. This resource receives the raw OTLP export
//! instead: every data point, with its aggregation temporality, as streamling
//! sent it.

use crate::{E2eError, Result};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Router;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// OTLP `AggregationTemporality`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Temporality {
    Delta,
    Cumulative,
}

/// One exported sum data point.
#[derive(Debug, Clone)]
pub struct SumPoint {
    pub metric: String,
    pub temporality: Temporality,
    pub monotonic: bool,
    pub value: u64,
    pub attributes: Vec<(String, String)>,
    /// Index of the export request that carried this point.
    pub export: usize,
}

impl SumPoint {
    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

pub struct OtlpCaptureResource {
    pub endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    _shutdown_tx: oneshot::Sender<()>,
}

impl OtlpCaptureResource {
    pub async fn new() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(E2eError::Io)?;
        let port = listener.local_addr().map_err(E2eError::Io)?.port();
        let requests: Arc<Mutex<Vec<Value>>> = Arc::default();
        let app = Router::new().fallback(capture).with_state(requests.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok(Self {
            endpoint: format!("http://127.0.0.1:{port}/v1/metrics"),
            requests,
            _shutdown_tx: shutdown_tx,
        })
    }

    /// Environment that points a streamling run's metrics at this receiver.
    pub fn env(&self, application_id: &str) -> Vec<(String, String)> {
        [
            ("STREAMLING__APPLICATION_ID", application_id),
            (
                "STREAMLING__OPEN_TELEMETRY_METRICS__INGESTION_ENDPOINT",
                self.endpoint.as_str(),
            ),
            (
                "STREAMLING__OPEN_TELEMETRY_METRICS__ENDPOINT_PROTOCOL",
                "http/json",
            ),
            (
                "STREAMLING__OPEN_TELEMETRY_METRICS__BATCH_INTERVAL_SECS",
                "1",
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    /// Every integer sum data point received so far, in export order.
    pub fn sum_points(&self) -> Vec<SumPoint> {
        let requests = self.requests.lock().unwrap();
        let mut points = Vec::new();
        for (export, request) in requests.iter().enumerate() {
            for metric in json_items(request, &["resourceMetrics"])
                .iter()
                .flat_map(|rm| json_items(rm, &["scopeMetrics"]))
                .flat_map(|sm| json_items(&sm, &["metrics"]))
            {
                let Some(sum) = metric.get("sum") else {
                    continue;
                };
                let name = metric["name"].as_str().unwrap_or_default().to_string();
                let temporality = match sum["aggregationTemporality"].as_i64() {
                    Some(1) => Temporality::Delta,
                    Some(2) => Temporality::Cumulative,
                    other => panic!("metric {name}: unexpected aggregationTemporality {other:?}"),
                };
                let monotonic = sum["isMonotonic"].as_bool().unwrap_or(false);
                for point in json_items(sum, &["dataPoints"]) {
                    points.push(SumPoint {
                        metric: name.clone(),
                        temporality,
                        monotonic,
                        value: json_u64(&point["asInt"]),
                        attributes: json_items(&point, &["attributes"])
                            .iter()
                            .map(|kv| {
                                (
                                    kv["key"].as_str().unwrap_or_default().to_string(),
                                    kv["value"]["stringValue"]
                                        .as_str()
                                        .unwrap_or_default()
                                        .to_string(),
                                )
                            })
                            .collect(),
                        export,
                    });
                }
            }
        }
        points
    }

    /// The points of `metric` whose `id` attribute is `node_id`.
    pub fn node_points(&self, metric: &str, node_id: &str) -> Vec<SumPoint> {
        self.sum_points()
            .into_iter()
            .filter(|p| p.metric == metric && p.attribute("id") == Some(node_id))
            .collect()
    }
}

async fn capture(State(requests): State<Arc<Mutex<Vec<Value>>>>, body: Bytes) -> StatusCode {
    match serde_json::from_slice::<Value>(&body) {
        Ok(request) => {
            requests.lock().unwrap().push(request);
            StatusCode::OK
        }
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

fn json_items(value: &Value, path: &[&str]) -> Vec<Value> {
    path.iter()
        .try_fold(value, |v, key| v.get(*key))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

/// OTLP/JSON encodes 64-bit integers as strings.
fn json_u64(value: &Value) -> u64 {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_default()
}
