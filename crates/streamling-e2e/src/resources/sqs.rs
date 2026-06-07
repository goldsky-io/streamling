//! SQS resource manager for creating and cleaning up isolated SQS queues per test.
//!
//! Uses ElasticMQ (or any SQS-compatible endpoint) as the SQS backend for testing.

use crate::{E2eError, Result};
use aws_config::BehaviorVersion;
use aws_sdk_sqs::Client as SqsClient;
use std::time::Duration;
use tracing::info;

/// SQS resource manager
pub struct SqsResource {
    /// SQS-compatible endpoint URL (e.g. ElasticMQ)
    pub endpoint_url: String,
    /// AWS region (default: us-east-1)
    pub region: String,
    /// Name of the isolated SQS queue
    pub queue_name: String,
    /// URL of the created SQS queue
    pub queue_url: String,
    /// SQS client
    client: SqsClient,
}

impl SqsResource {
    /// Create a new SQS resource with an isolated queue
    pub async fn new(endpoint_url: &str, queue_name: &str) -> Result<Self> {
        let region = "us-east-1".to_string();

        let client = Self::create_client(endpoint_url, &region).await;

        // Create the queue
        let create_result = client
            .create_queue()
            .queue_name(queue_name)
            .send()
            .await
            .map_err(|e| {
                E2eError::Sqs(format!(
                    "Failed to create SQS queue '{}': {}",
                    queue_name, e
                ))
            })?;

        let mut queue_url = create_result
            .queue_url()
            .ok_or_else(|| E2eError::Sqs("Queue URL not returned after creation".to_string()))?
            .to_string();

        // ElasticMQ may return URLs with its internal port (9324); normalize to use
        // the endpoint_url so clients connecting via NodePort get the correct URL.
        if let Some(path_start) = queue_url
            .find("://")
            .and_then(|i| queue_url[i + 3..].find('/').map(|j| i + 3 + j))
        {
            let path = &queue_url[path_start..];
            let base = endpoint_url.trim_end_matches('/');
            queue_url = format!("{}{}", base, path);
        }
        info!("Created SQS queue: {} (url: {})", queue_name, queue_url);

        Ok(Self {
            endpoint_url: endpoint_url.to_string(),
            region,
            queue_name: queue_name.to_string(),
            queue_url,
            client,
        })
    }

    /// Create an SQS client for the SQS-compatible endpoint
    async fn create_client(endpoint_url: &str, region: &str) -> SqsClient {
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(endpoint_url)
            .region(aws_types::region::Region::new(region.to_string()))
            .load()
            .await;

        SqsClient::new(&sdk_config)
    }

    /// Send a single message to the queue
    pub async fn send_message(&self, body: &str) -> Result<()> {
        self.client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(body)
            .send()
            .await
            .map_err(|e| E2eError::Sqs(format!("Failed to send message: {}", e)))?;

        Ok(())
    }

    /// Receive messages from the queue
    ///
    /// Returns a list of message bodies. Uses long polling with a wait time of up to 5 seconds.
    pub async fn receive_messages(&self, max_messages: i32) -> Result<Vec<String>> {
        let result = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(max_messages.min(10)) // SQS max is 10 per call
            .wait_time_seconds(5)
            .send()
            .await
            .map_err(|e| E2eError::Sqs(format!("Failed to receive messages: {}", e)))?;

        let messages = result
            .messages()
            .iter()
            .filter_map(|msg| msg.body().map(|b| b.to_string()))
            .collect();

        Ok(messages)
    }

    /// Receive all available messages from the queue, polling until no more messages arrive.
    ///
    /// This is useful for verification in e2e tests where we want to read all messages
    /// that have been written to the queue.
    pub async fn receive_all_messages(
        &self,
        max_messages: usize,
        max_wait: Duration,
    ) -> Result<Vec<String>> {
        let mut all_messages = Vec::new();
        let start = std::time::Instant::now();

        while all_messages.len() < max_messages && start.elapsed() < max_wait {
            let batch_size = (max_messages - all_messages.len()).min(10) as i32;
            let result = self
                .client
                .receive_message()
                .queue_url(&self.queue_url)
                .max_number_of_messages(batch_size)
                .wait_time_seconds(2)
                .send()
                .await
                .map_err(|e| E2eError::Sqs(format!("Failed to receive messages: {}", e)))?;

            let messages: Vec<String> = result
                .messages()
                .iter()
                .filter_map(|msg| msg.body().map(|b| b.to_string()))
                .collect();

            if messages.is_empty() {
                // No more messages available
                break;
            }

            // Delete received messages so they don't show up again
            for msg in result.messages() {
                if let Some(receipt_handle) = msg.receipt_handle() {
                    let _ = self
                        .client
                        .delete_message()
                        .queue_url(&self.queue_url)
                        .receipt_handle(receipt_handle)
                        .send()
                        .await;
                }
            }

            all_messages.extend(messages);
        }

        info!(
            "Received {} messages from SQS queue {}",
            all_messages.len(),
            self.queue_name
        );

        Ok(all_messages)
    }

    /// Get the approximate number of messages in the queue
    pub async fn get_message_count(&self) -> Result<i64> {
        let result = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
            .map_err(|e| E2eError::Sqs(format!("Failed to get queue attributes: {}", e)))?;

        let count = result
            .attributes()
            .and_then(|attrs| {
                attrs
                    .get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
                    .and_then(|v| v.parse::<i64>().ok())
            })
            .unwrap_or(0);

        Ok(count)
    }

    /// Delete the queue (can be called explicitly if needed)
    #[allow(dead_code)]
    pub async fn cleanup(&self) -> Result<()> {
        self.client
            .delete_queue()
            .queue_url(&self.queue_url)
            .send()
            .await
            .map_err(|e| E2eError::Sqs(format!("Failed to delete SQS queue: {}", e)))?;

        info!("Deleted SQS queue: {}", self.queue_name);
        Ok(())
    }
}

impl Drop for SqsResource {
    fn drop(&mut self) {
        // Best-effort cleanup
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let queue_url = self.queue_url.clone();
            let endpoint_url = self.endpoint_url.clone();
            let region = self.region.clone();
            let queue_name = self.queue_name.clone();

            handle.spawn(async move {
                let client = Self::create_client(&endpoint_url, &region).await;

                if let Err(e) = client.delete_queue().queue_url(&queue_url).send().await {
                    tracing::warn!("Failed to delete SQS queue {}: {}", queue_name, e);
                } else {
                    info!("Deleted SQS queue: {}", queue_name);
                }
            });
        }
    }
}
