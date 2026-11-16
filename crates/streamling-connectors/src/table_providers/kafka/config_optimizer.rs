use reqwest_middleware::ClientBuilder;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use std::time::Duration;
use streamling_config::KafkaConfig;
use streamling_core::error::ResultExt;
use tracing::debug;

const AZ_ENV: &str = "AVAILABILITY_ZONE";
const IMDS_BASE_URL: &str = "http://169.254.169.254";

pub struct KafkaConfigOptimizer {
    kafka_config: KafkaConfig,
}

impl KafkaConfigOptimizer {
    pub fn new(kafka_config: &KafkaConfig) -> Self {
        Self {
            kafka_config: kafka_config.clone(),
        }
    }

    pub fn optimized_consumer_config(&self) -> Vec<(String, String)> {
        if self
            .kafka_config
            .brokers
            .to_lowercase()
            .contains("warpstream")
        {
            // https://docs.warpstream.com/warpstream/byoc/configure-kafka-client/tuning-for-performance#librdkafka
            // however, these are slightly adjusted based on Strick values
            let config = vec![
                ("topic.metadata.refresh.interval.ms", "60000"),
                ("fetch.max.bytes", "52428800"), // default is 52428800
                ("max.partition.fetch.bytes", "5242880"), // default is 1048576
            ];

            let mut config = self.to_string_vec(config);

            if let Some(az) = detect_az() {
                debug!("Detected AZ: {}", az);
                config.push(("client.id".to_string(), format!("warpstream_az={}", az)));
            } else {
                debug!("Failed to detect AZ");
            }

            config
        } else {
            vec![]
        }
    }

    pub fn optimized_producer_config(&self) -> Vec<(String, String)> {
        if self
            .kafka_config
            .brokers
            .to_lowercase()
            .contains("warpstream")
        {
            // https://docs.warpstream.com/warpstream/byoc/configure-kafka-client/tuning-for-performance#librdkafka
            let config = vec![
                ("topic.metadata.refresh.interval.ms", "60000"),
                ("queue.buffering.max.kbytes", "1048576"),
                ("queue.buffering.max.messages", "1000000"),
                ("message.max.bytes", "64000000"),
                ("batch.size", "16000000"),
                ("batch.num.messages", "100000"),
                ("linger.ms", "200"), // a bit higher than the default
                ("sticky.partitioning.linger.ms", "25"),
                ("enable.idempotence", "false"),
                ("max.in.flight.requests.per.connection", "1000000"),
                ("partitioner", "consistent_random"),
                ("compression.type", "lz4"),
            ];

            let mut config = self.to_string_vec(config);

            if let Some(az) = detect_az() {
                debug!("Detected AZ: {}", az);
                config.push(("client.id".to_string(), format!("warpstream_az={}", az)));
            } else {
                debug!("Failed to detect AZ");
            }

            config
        } else {
            // Default producer config for non-WarpStream Kafka brokers.
            // Keep queue small so QueueFull throttles production and flush
            // completes in bounded time (~queue_kbytes / drain_rate).
            self.to_string_vec(vec![
                ("message.max.bytes", "10485760"),
                ("batch.size", "1048576"),
                ("batch.num.messages", "10000"),
                ("queue.buffering.max.messages", "10000"),
                ("queue.buffering.max.kbytes", "131072"),
                ("linger.ms", "200"),
                ("compression.type", "lz4"),
            ])
        }
    }

    fn to_string_vec(&self, config: Vec<(&str, &str)>) -> Vec<(String, String)> {
        config
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect()
    }
}

fn detect_az() -> Option<String> {
    let env_az = std::env::var(AZ_ENV).ok();
    tokio::task::block_in_place(move || {
        tokio::runtime::Handle::current().block_on(resolve_az(env_az, IMDS_BASE_URL))
    })
}

async fn resolve_az(env_az: Option<String>, imds_base_url: &str) -> Option<String> {
    if let Some(az) = env_az
        .map(|az| az.trim().to_string())
        .filter(|az| !az.is_empty())
    {
        return Some(az);
    }
    match load_imds_az(imds_base_url).await {
        Ok(az) => Some(az),
        Err(e) => {
            debug!("IMDS AZ lookup failed: {e}");
            None
        }
    }
}

async fn load_imds_az(base_url: &str) -> streamling_core::error::Result<String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .streamling_context("failed to build HTTP client")?;

    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    let client = ClientBuilder::new(client)
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build();

    // IMDSv2: First get a session token
    let token = client
        .put(format!("{base_url}/latest/api/token"))
        .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
        .send()
        .await
        .streamling_context("IMDS token request failed")?
        .error_for_status()
        .streamling_context("IMDS token request returned error status")?
        .text()
        .await
        .streamling_context("failed to read IMDS token")?;

    // IMDSv2: Use the token to fetch the AZ
    let az = client
        .get(format!(
            "{base_url}/latest/meta-data/placement/availability-zone/"
        ))
        .header("X-aws-ec2-metadata-token", &token)
        .send()
        .await
        .streamling_context("IMDS AZ request failed")?
        .error_for_status()
        .streamling_context("IMDS AZ request returned error status")?
        .text()
        .await
        .streamling_context("failed to read AZ")?;

    Ok(az)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn env_az_wins_without_calling_imds() {
        let mut server = mockito::Server::new_async().await;
        let token = server
            .mock("PUT", "/latest/api/token")
            .expect(0)
            .create_async()
            .await;

        let az = resolve_az(Some("us-west-2a".to_string()), &server.url()).await;

        assert_eq!(az.as_deref(), Some("us-west-2a"));
        token.assert_async().await;
    }

    #[tokio::test]
    async fn missing_or_empty_env_falls_back_to_imds() {
        let mut server = mockito::Server::new_async().await;
        let _token = server
            .mock("PUT", "/latest/api/token")
            .with_body("tok")
            .expect(2)
            .create_async()
            .await;
        let _az = server
            .mock("GET", "/latest/meta-data/placement/availability-zone/")
            .match_header("X-aws-ec2-metadata-token", "tok")
            .with_body("us-west-2b")
            .expect(2)
            .create_async()
            .await;

        assert_eq!(
            resolve_az(None, &server.url()).await.as_deref(),
            Some("us-west-2b")
        );
        assert_eq!(
            resolve_az(Some(" ".to_string()), &server.url())
                .await
                .as_deref(),
            Some("us-west-2b")
        );
    }

    #[tokio::test]
    async fn missing_env_and_failing_imds_yields_no_az() {
        let mut server = mockito::Server::new_async().await;
        // 404 is non-transient, so the retry middleware does not back off.
        let _token = server
            .mock("PUT", "/latest/api/token")
            .with_status(404)
            .create_async()
            .await;

        assert_eq!(resolve_az(None, &server.url()).await, None);
    }
}
