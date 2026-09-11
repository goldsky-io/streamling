use reqwest_middleware::ClientBuilder;
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use std::time::Duration;
use streamling_config::KafkaConfig;
use streamling_core::error::ResultExt;
use tracing::{debug, info};

const IMDS_TOKEN_ENDPOINT: &str = "http://169.254.169.254/latest/api/token";
const AZ_ENDPOINT: &str = "http://169.254.169.254/latest/meta-data/placement/availability-zone/";

/// Look up an already-assembled config value, for logging what actually took
/// effect.
fn get<'a>(config: &'a [(String, String)], key: &str) -> Option<&'a str> {
    config
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

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

            if let Some(backoff) = &self.kafka_config.fetch_queue_backoff_ms {
                config.push(("fetch.queue.backoff.ms".to_string(), backoff.clone()));
            }

            if let Some(client_id) = self.warpstream_client_id() {
                config.push(("client.id".to_string(), client_id));
            }

            info!(
                "WarpStream consumer tuning: fetch.queue.backoff.ms={:?}, client.id={:?}",
                get(&config, "fetch.queue.backoff.ms"),
                get(&config, "client.id"),
            );

            config
        } else {
            let mut config = vec![];

            if let Some(backoff) = &self.kafka_config.fetch_queue_backoff_ms {
                config.push(("fetch.queue.backoff.ms".to_string(), backoff.clone()));
            }

            config
        }
    }

    /// Assemble the WarpStream `client.id`, which WarpStream parses as a
    /// comma-separated flag list. The auto-detected AZ comes first (when
    /// detection succeeds) and any configured extra flags are appended, so a
    /// flag override never costs us AZ-aware fetching.
    fn warpstream_client_id(&self) -> Option<String> {
        let az = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { self.load_az().await })
        });

        let mut parts = Vec::new();
        match az {
            Ok(az) => {
                debug!("Detected AZ: {}", az);
                parts.push(format!("warpstream_az={}", az));
            }
            Err(_) => debug!("Failed to detect AZ"),
        }

        if let Some(flags) = &self.kafka_config.warpstream_client_id_flags {
            parts.extend(
                flags
                    .split(',')
                    .map(str::trim)
                    .filter(|f| !f.is_empty())
                    .map(str::to_string),
            );
        }

        (!parts.is_empty()).then(|| parts.join(","))
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

            if let Some(client_id) = self.warpstream_client_id() {
                config.push(("client.id".to_string(), client_id));
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

    async fn load_az(&self) -> streamling_core::error::Result<String> {
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
            .put(IMDS_TOKEN_ENDPOINT)
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
            .get(AZ_ENDPOINT)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(brokers: &str) -> KafkaConfig {
        KafkaConfig {
            brokers: brokers.to_string(),
            security_protocol: "plaintext".to_string(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            schema_registry_url: None,
            schema_registry_username: None,
            schema_registry_password: None,
            consumer_group_id: None,
            client_id: None,
            lag_report_interval_ms: None,
            fetch_queue_backoff_ms: None,
            warpstream_client_id_flags: None,
        }
    }

    #[test]
    fn fetch_queue_backoff_is_unset_by_default() {
        let optimizer = KafkaConfigOptimizer::new(&config("localhost:9092"));
        let consumer = optimizer.optimized_consumer_config();
        assert_eq!(get(&consumer, "fetch.queue.backoff.ms"), None);
    }

    #[test]
    fn fetch_queue_backoff_applies_to_non_warpstream_brokers() {
        let mut cfg = config("localhost:9092");
        cfg.fetch_queue_backoff_ms = Some("100".to_string());

        let optimizer = KafkaConfigOptimizer::new(&cfg);
        let consumer = optimizer.optimized_consumer_config();
        assert_eq!(get(&consumer, "fetch.queue.backoff.ms"), Some("100"));
    }

    #[test]
    fn client_id_flags_are_split_trimmed_and_joined() {
        let mut cfg = config("localhost:9092");
        cfg.warpstream_client_id_flags =
            Some(" warpstream_disable_fetch_auto_tune=true , , other=1 ".to_string());

        let optimizer = KafkaConfigOptimizer::new(&cfg);
        // AZ detection is skipped here; only the configured flags remain.
        let flags: Vec<&str> = cfg
            .warpstream_client_id_flags
            .as_ref()
            .unwrap()
            .split(',')
            .map(str::trim)
            .filter(|f| !f.is_empty())
            .collect();
        assert_eq!(
            flags,
            vec!["warpstream_disable_fetch_auto_tune=true", "other=1"]
        );
        // Non-WarpStream brokers never emit a client.id.
        assert_eq!(
            get(&optimizer.optimized_consumer_config(), "client.id"),
            None
        );
    }
}
