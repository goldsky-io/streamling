//! Prometheus resource for querying metrics in e2e tests.

use crate::{E2eError, Result};
use serde::Deserialize;
use tracing::{debug, warn};

/// Resource for querying Prometheus metrics
pub struct PrometheusResource {
    /// Base URL for Prometheus (e.g., http://localhost:30090)
    pub url: String,
    /// Query endpoint (e.g., http://localhost:30090/api/v1/query)
    pub query_endpoint: String,
    /// Metrics ingestion endpoint for OTLP (e.g., http://localhost:30090/api/v1/otlp/v1/metrics)
    pub ingestion_endpoint: String,
}

impl PrometheusResource {
    /// Create a new Prometheus resource from a base URL
    pub fn new(base_url: &str) -> Self {
        let url = base_url.trim_end_matches('/').to_string();
        Self {
            query_endpoint: format!("{}/api/v1/query", url),
            ingestion_endpoint: format!("{}/api/v1/otlp/v1/metrics", url),
            url,
        }
    }

    /// Query Prometheus for a metric value
    pub async fn query(&self, query: &str) -> Result<Option<f64>> {
        let encoded = urlencoding::encode(query);
        let full_url = format!("{}?query={}", self.query_endpoint, encoded);

        debug!("Prometheus query: {} -> {}", query, full_url);

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| E2eError::Prometheus(format!("Failed to build client: {}", e)))?;

        let resp =
            client.get(&full_url).send().await.map_err(|e| {
                E2eError::Prometheus(format!("Query failed: {} url={}", e, full_url))
            })?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| E2eError::Prometheus(format!("Failed to read response: {}", e)))?;

        if !status.is_success() {
            return Err(E2eError::Prometheus(format!(
                "Query failed with status {}: {}",
                status, body
            )));
        }

        // Parse the Prometheus response
        let response: PrometheusResponse = serde_json::from_str(&body).map_err(|e| {
            E2eError::Prometheus(format!("Failed to parse response: {} body={}", e, body))
        })?;

        if response.status != "success" {
            return Err(E2eError::Prometheus(format!(
                "Query returned error status: {:?}",
                response
            )));
        }

        // Extract the value from the result
        if let Some(result) = response.data.result.first() {
            if let Some(value) = result.value.get(1) {
                if let Some(s) = value.as_str() {
                    return Ok(s.parse::<f64>().ok());
                }
            }
        }

        Ok(None)
    }

    /// Query for a metric and return as u64
    pub async fn query_count(&self, query: &str) -> Result<Option<u64>> {
        self.query(query).await.map(|v| v.map(|f| f as u64))
    }

    /// Build a query for output rows total metric
    pub fn output_rows_query(node_id: &str, instance_id: Option<&str>) -> String {
        Self::build_metric_query("streamling_output_rows_total", node_id, instance_id)
    }

    /// Build a query for input rows total metric
    pub fn input_rows_query(node_id: &str, instance_id: Option<&str>) -> String {
        Self::build_metric_query("streamling_input_rows_total", node_id, instance_id)
    }

    /// Build a query for elapsed compute metric
    pub fn elapsed_compute_query(node_id: &str, instance_id: Option<&str>) -> String {
        Self::build_metric_query(
            "streamling_elapsed_compute_milliseconds_sum",
            node_id,
            instance_id,
        )
    }

    /// Build a query for a checkpoint coordinator metric (counter total).
    /// Coordinator metrics use `id="checkpoint_coordinator"`.
    pub fn checkpoint_coordinator_query(metric_name: &str, instance_id: Option<&str>) -> String {
        Self::build_metric_query(metric_name, "checkpoint_coordinator", instance_id)
    }

    /// Build a query for a checkpoint histogram metric (sum) by node id.
    pub fn checkpoint_histogram_query(
        metric_name: &str,
        node_id: &str,
        instance_id: Option<&str>,
    ) -> String {
        Self::build_metric_query(&format!("{}_sum", metric_name), node_id, instance_id)
    }

    /// Build a metric query with labels
    /// Note: instance_id maps to the `instance` label in Prometheus (from OTLP resource attribute)
    fn build_metric_query(metric_name: &str, node_id: &str, instance_id: Option<&str>) -> String {
        let mut labels = format!("id=\"{}\"", node_id);
        if let Some(instance) = instance_id {
            labels.push_str(&format!(",instance=\"{}\"", instance));
        }
        format!("{}{{{}}}", metric_name, labels)
    }

    /// Wait for a metric to reach at least a certain value
    /// Returns the actual value when the threshold is reached
    pub async fn wait_for_metric_at_least(
        &self,
        query: &str,
        min_value: u64,
        timeout_secs: u64,
        poll_interval_ms: u64,
    ) -> Result<u64> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);
        let poll_interval = std::time::Duration::from_millis(poll_interval_ms);

        loop {
            if start.elapsed() >= timeout {
                return Err(E2eError::Prometheus(format!(
                    "Timeout waiting for metric {} to reach {}",
                    query, min_value
                )));
            }

            if let Some(count) = self.query_count(query).await? {
                if count >= min_value {
                    return Ok(count);
                }
                debug!(
                    "Metric {} = {}, waiting for at least {}",
                    query, count, min_value
                );
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Assert that a metric reaches at least a certain value
    pub async fn assert_metric_at_least(
        &self,
        query: &str,
        expected: u64,
        timeout_secs: u64,
    ) -> Result<()> {
        match self
            .wait_for_metric_at_least(query, expected, timeout_secs, 500)
            .await
        {
            Ok(actual) => {
                debug!(
                    "Metric {} reached {} (expected at least {})",
                    query, actual, expected
                );
                Ok(())
            }
            Err(e) => {
                // Try one more query to get the actual value for the error message
                let actual = self.query_count(query).await.ok().flatten();
                warn!(
                    "Metric assertion failed: {} expected at least {}, got {:?}",
                    query, expected, actual
                );
                Err(e)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct PrometheusResponse {
    status: String,
    data: PrometheusData,
}

#[derive(Debug, Deserialize)]
struct PrometheusData {
    #[serde(default)]
    result: Vec<PrometheusResult>,
}

#[derive(Debug, Deserialize)]
struct PrometheusResult {
    #[serde(default)]
    value: Vec<serde_json::Value>,
}
