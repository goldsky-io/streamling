//! Kafka resource manager for creating isolated topics per test.

use crate::{E2eError, Result};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use schema_registry_converter::async_impl::avro::{AvroDecoder, AvroEncoder};
use schema_registry_converter::async_impl::schema_registry::{post_schema, SrSettings};
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedSchema,
};
use serde::Serialize;
use std::time::Duration;
use tracing::info;

/// Kafka resource manager
pub struct KafkaResource {
    /// Kafka broker address
    pub broker: String,
    /// Schema registry URL
    pub schema_registry_url: String,
    /// Name of the isolated topic
    pub topic: String,
    /// Admin client for topic management (kept for potential future use)
    #[allow(dead_code)]
    admin_client: AdminClient<DefaultClientContext>,
    /// Producer for sending messages
    producer: FutureProducer,
    /// Schema registry settings
    sr_settings: SrSettings,
}

impl KafkaResource {
    /// Create a new Kafka resource with an isolated topic
    pub async fn new(broker: &str, schema_registry_url: &str, topic: &str) -> Result<Self> {
        // Create admin client
        let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("socket.timeout.ms", "10000")
            .create()
            .map_err(|e| E2eError::Kafka(e.to_string()))?;

        // Create the topic
        let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(30)));

        admin_client
            .create_topics(&[new_topic], &opts)
            .await
            .map_err(|e| E2eError::Kafka(e.to_string()))?;

        info!("Created Kafka topic: {}", topic);

        // Create producer
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("message.timeout.ms", "30000")
            .create()
            .map_err(|e| E2eError::Kafka(e.to_string()))?;

        // Schema registry settings
        let sr_settings = SrSettings::new(schema_registry_url.to_string());

        Ok(Self {
            broker: broker.to_string(),
            schema_registry_url: schema_registry_url.to_string(),
            topic: topic.to_string(),
            admin_client,
            producer,
            sr_settings,
        })
    }

    /// Register an Avro schema for the topic
    pub async fn register_schema(&self, schema: &str) -> Result<u32> {
        let subject_strategy = SubjectNameStrategy::TopicNameStrategy(self.topic.clone(), false);

        let supplied_schema = SuppliedSchema {
            name: None,
            schema_type: SchemaType::Avro,
            schema: schema.to_string(),
            references: vec![],
        };

        let result = post_schema(
            &self.sr_settings,
            subject_strategy.get_subject().unwrap(),
            supplied_schema,
        )
        .await
        .map_err(|e| E2eError::Kafka(e.to_string()))?;

        info!(
            "Registered schema for topic {}: id={}",
            self.topic, result.id
        );
        Ok(result.id)
    }

    /// Produce JSON records to the topic (without schema registry)
    pub async fn produce_json_records<T: Serialize>(&self, records: &[T]) -> Result<()> {
        for record in records {
            let payload = serde_json::to_vec(record).map_err(|e| E2eError::Kafka(e.to_string()))?;

            let kafka_record = FutureRecord::to(&self.topic).payload(&payload).key("");

            self.producer
                .send(kafka_record, Timeout::After(Duration::from_secs(15)))
                .await
                .map_err(|(e, _)| E2eError::Kafka(e.to_string()))?;
        }

        info!(
            "Produced {} JSON records to topic {}",
            records.len(),
            self.topic
        );
        Ok(())
    }

    /// Produce Avro records to the topic (with schema registry)
    /// Uses dbz.op='c' (create/insert) header by default
    pub async fn produce_avro_records<T: Serialize>(&self, records: &[T]) -> Result<()> {
        self.produce_avro_records_with_op(records, "c").await
    }

    /// Produce Avro records with a specific debezium operation type
    /// op: "c" = create/insert, "u" = update, "d" = delete
    pub async fn produce_avro_records_with_op<T: Serialize>(
        &self,
        records: &[T],
        op: &str,
    ) -> Result<()> {
        let encoder = AvroEncoder::new(self.sr_settings.clone());
        let subject_strategy = SubjectNameStrategy::TopicNameStrategy(self.topic.clone(), false);

        for record in records {
            let payload = encoder
                .encode_struct(record, &subject_strategy)
                .await
                .map_err(|e| E2eError::Kafka(e.to_string()))?;

            let headers = OwnedHeaders::new().insert(Header {
                key: "dbz.op",
                value: Some(op),
            });

            let kafka_record = FutureRecord::to(&self.topic)
                .payload(&payload)
                .key("")
                .headers(headers);

            self.producer
                .send(kafka_record, Timeout::After(Duration::from_secs(15)))
                .await
                .map_err(|(e, _)| E2eError::Kafka(e.to_string()))?;
        }

        info!(
            "Produced {} Avro records to topic {} (op={})",
            records.len(),
            self.topic,
            op
        );
        Ok(())
    }

    /// Bulk-produce `count` Avro records for benchmark preloading.
    ///
    /// Pre-encodes a pool of `distinct` payloads once via `generate`, then
    /// fire-and-forget produces `count` records by cycling the pool with a
    /// `dbz.op="c"` header (so the source synthesizes `_gs_op='i'`). Unlike
    /// [`Self::produce_avro_records`], deliveries are not awaited per record — a
    /// single `flush` at the end waits for all in-flight messages. That is the
    /// difference between seconds and hours when loading millions of rows.
    ///
    /// The schema must already be registered (see [`Self::register_schema`]).
    /// Returns the average encoded payload size in bytes, for byte-throughput
    /// reporting.
    ///
    /// Panics if `distinct` is zero (there would be no payloads to cycle).
    pub async fn produce_avro_bulk<T, F>(
        &self,
        count: u64,
        distinct: u32,
        generate: F,
    ) -> Result<u64>
    where
        T: Serialize,
        F: Fn(u64) -> T,
    {
        assert!(distinct > 0, "produce_avro_bulk requires distinct > 0");

        let encoder = AvroEncoder::new(self.sr_settings.clone());
        let subject_strategy = SubjectNameStrategy::TopicNameStrategy(self.topic.clone(), false);

        // Non-empty by the assert above, so indexing and division are safe.
        let mut pool: Vec<Vec<u8>> = Vec::with_capacity(distinct as usize);
        for i in 0..distinct as u64 {
            let payload = encoder
                .encode_struct(generate(i), &subject_strategy)
                .await
                .map_err(|e| E2eError::Kafka(e.to_string()))?;
            pool.push(payload);
        }
        let avg_payload_bytes = (pool.iter().map(|p| p.len()).sum::<usize>() / pool.len()) as u64;

        for i in 0..count {
            let payload = &pool[(i as usize) % pool.len()];
            let mut record = FutureRecord::to(&self.topic)
                .payload(payload.as_slice())
                .key("")
                .headers(OwnedHeaders::new().insert(Header {
                    key: "dbz.op",
                    value: Some("c"),
                }));

            // Fire-and-forget: enqueue without awaiting delivery, backing off
            // only when the local producer queue fills up.
            loop {
                match self.producer.send_result(record) {
                    Ok(_delivery) => break,
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), rejected)) => {
                        record = rejected;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err((e, _)) => return Err(E2eError::Kafka(e.to_string())),
                }
            }
        }

        self.producer
            .flush(Timeout::After(Duration::from_secs(120)))
            .map_err(|e| E2eError::Kafka(e.to_string()))?;

        info!(
            "Bulk-produced {} Avro records to topic {} (avg {} bytes/payload)",
            count, self.topic, avg_payload_bytes
        );
        Ok(avg_payload_bytes)
    }

    /// Produce a single Avro record holding a `decimal` logical-type column.
    ///
    /// `schema_str` must be a record schema with two fields: an `id` long
    /// and a `decimal(precision, scale)` field named by `decimal_field`. The
    /// unscaled decimal value is supplied as a base-10 string (`unscaled_value`).
    ///
    /// Uses the schema-registry wire format: `[magic(0)] [schema_id BE u32] [avro_datum]`.
    pub async fn produce_decimal_record(
        &self,
        schema_str: &str,
        id: i64,
        decimal_field: &str,
        unscaled_value: &str,
    ) -> Result<()> {
        use apache_avro::types::{Record, Value};
        use apache_avro::{Decimal, Schema};
        use num_bigint::BigInt;
        use rdkafka::message::{Header, OwnedHeaders};
        use std::str::FromStr;

        // Register schema (idempotent) to fetch the registry id we need.
        let schema_id = self.register_schema(schema_str).await?;

        let schema = Schema::parse_str(schema_str)
            .map_err(|e| E2eError::Kafka(format!("invalid schema: {}", e)))?;
        let mut record = Record::new(&schema)
            .ok_or_else(|| E2eError::Kafka("schema is not a record".to_string()))?;
        let unscaled = BigInt::from_str(unscaled_value).map_err(|e| {
            E2eError::Kafka(format!(
                "invalid unscaled bigint '{}': {}",
                unscaled_value, e
            ))
        })?;
        let bytes = unscaled.to_signed_bytes_be();
        record.put("id", id);
        record.put(decimal_field, Value::Decimal(Decimal::from(bytes)));

        let datum = apache_avro::to_avro_datum(&schema, record)
            .map_err(|e| E2eError::Kafka(format!("avro encode failed: {}", e)))?;
        let mut payload = vec![0u8];
        payload.extend_from_slice(&schema_id.to_be_bytes());
        payload.extend_from_slice(&datum);

        let headers = OwnedHeaders::new().insert(Header {
            key: "dbz.op",
            value: Some("c"),
        });
        let kafka_record = FutureRecord::to(&self.topic)
            .payload(&payload)
            .key("")
            .headers(headers);
        self.producer
            .send(kafka_record, Timeout::After(Duration::from_secs(15)))
            .await
            .map_err(|(e, _)| E2eError::Kafka(e.to_string()))?;
        Ok(())
    }

    /// Produce raw bytes to the topic
    pub async fn produce_raw(&self, records: &[Vec<u8>]) -> Result<()> {
        for payload in records {
            let kafka_record = FutureRecord::to(&self.topic)
                .payload(payload.as_slice())
                .key("");

            self.producer
                .send(kafka_record, Timeout::After(Duration::from_secs(15)))
                .await
                .map_err(|(e, _)| E2eError::Kafka(e.to_string()))?;
        }

        info!(
            "Produced {} raw records to topic {}",
            records.len(),
            self.topic
        );
        Ok(())
    }

    /// Inspect messages in an existing topic without creating it
    /// Returns (messages, highest_offset) where messages are (offset, key, id_string) tuples
    pub async fn inspect_topic_messages(
        broker: &str,
        schema_registry_url: &str,
        topic: &str,
        max_messages: usize,
        max_show: usize,
    ) -> Result<(Vec<(i64, String, String)>, Option<i64>)> {
        use apache_avro::types::Value;
        use rdkafka::Offset;

        // Create consumer for inspection
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("group.id", format!("inspect-{}", uuid::Uuid::new_v4()))
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|e| E2eError::Kafka(e.to_string()))?;

        // Check if topic exists
        let topic_exists = match consumer.fetch_metadata(Some(topic), Duration::from_secs(5)) {
            Ok(metadata) => {
                !metadata.topics().is_empty() && !metadata.topics()[0].partitions().is_empty()
            }
            Err(_) => false,
        };

        if !topic_exists {
            return Ok((vec![], None));
        }

        consumer
            .subscribe(&[topic])
            .map_err(|e| E2eError::Kafka(e.to_string()))?;

        // Poll to trigger partition assignment
        let assignment_timeout = Duration::from_secs(10);
        let assignment_start = std::time::Instant::now();
        let mut assignment = None;
        let mut poll_count = 0;

        while assignment_start.elapsed() < assignment_timeout {
            if let Ok(assigned) = consumer.assignment() {
                if assigned.count() > 0 {
                    assignment = Some(assigned);
                    break;
                }
            }

            match tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await {
                Ok(Ok(msg)) => {
                    if let Ok(assigned) = consumer.assignment() {
                        if assigned.count() > 0 {
                            assignment = Some(assigned);
                            break;
                        }
                    }
                    drop(msg);
                }
                Ok(Err(e)) => {
                    let err_str = e.to_string();
                    if err_str.contains("Partition EOF") {
                        if let Ok(assigned) = consumer.assignment() {
                            if assigned.count() > 0 {
                                assignment = Some(assigned);
                                break;
                            }
                        }
                    }
                }
                Err(_) => {}
            }

            poll_count += 1;
            if poll_count % 5 == 0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        let mut results = Vec::new();
        let mut highest_offset: Option<i64> = None;

        if let Some(assignment_list) = assignment {
            // Seek to beginning
            for tp in assignment_list.elements() {
                let _ = consumer.seek(
                    tp.topic(),
                    tp.partition(),
                    Offset::Beginning,
                    Duration::from_secs(5),
                );
            }

            // Create Avro decoder
            let sr_settings = SrSettings::new(schema_registry_url.to_string());
            let decoder = AvroDecoder::new(sr_settings);

            // Consume messages
            let timeout = Duration::from_secs(5);
            let start = std::time::Instant::now();
            let mut message_count = 0;

            while message_count < max_messages && start.elapsed() < timeout {
                match tokio::time::timeout(Duration::from_millis(1000), consumer.recv()).await {
                    Ok(Ok(msg)) => {
                        message_count += 1;
                        let offset = msg.offset();

                        // Track highest offset
                        highest_offset =
                            Some(highest_offset.map(|h| h.max(offset)).unwrap_or(offset));

                        let key = msg
                            .key()
                            .map(|k| String::from_utf8_lossy(k).to_string())
                            .unwrap_or_default();

                        // Decode Avro message to extract ID
                        let id_str = match msg.payload() {
                            Some(payload) => match decoder.decode(Some(payload)).await {
                                Ok(decoded_result) => {
                                    // Unwrap Union if present
                                    let value = match &decoded_result.value {
                                        Value::Union(_, inner) => inner.as_ref(),
                                        other => other,
                                    };

                                    match value {
                                        Value::Record(fields) => {
                                            let parts: Vec<String> = fields
                                                .iter()
                                                .map(|(name, fv)| {
                                                    let val_str = match fv {
                                                        Value::Int(i) => i.to_string(),
                                                        Value::Long(l) => l.to_string(),
                                                        Value::String(s) => s.clone(),
                                                        Value::Union(_, v) => format!("{:?}", v),
                                                        _ => format!("{:?}", fv),
                                                    };
                                                    format!("{}={}", name, val_str)
                                                })
                                                .collect();
                                            if parts.is_empty() {
                                                format!("[{} bytes]", payload.len())
                                            } else {
                                                parts.join(",")
                                            }
                                        }
                                        _ => format!("[{} bytes]", payload.len()),
                                    }
                                }
                                Err(_) => format!("[{} bytes]", payload.len()),
                            },
                            None => "[0 bytes]".to_string(),
                        };

                        if message_count <= max_show {
                            results.push((offset, key, id_str));
                        }
                    }
                    Ok(Err(e)) => {
                        if e.to_string().contains("Partition EOF") {
                            break;
                        }
                        break;
                    }
                    Err(_) => break,
                }
            }
        }

        Ok((results, highest_offset))
    }

    /// Delete the topic (can be called explicitly if needed)
    #[allow(dead_code)]
    pub async fn cleanup(&self) -> Result<()> {
        let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(30)));

        self.admin_client
            .delete_topics(&[&self.topic], &opts)
            .await
            .map_err(|e| E2eError::Kafka(e.to_string()))?;

        info!("Deleted Kafka topic: {}", self.topic);
        Ok(())
    }
}

impl Drop for KafkaResource {
    fn drop(&mut self) {
        let topic = self.topic.clone();
        let broker = self.broker.clone();

        let delete = async move {
            let admin_client: std::result::Result<AdminClient<DefaultClientContext>, _> =
                ClientConfig::new()
                    .set("bootstrap.servers", &broker)
                    .set("socket.timeout.ms", "5000")
                    .create();

            if let Ok(client) = admin_client {
                let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(10)));
                if let Err(e) = client.delete_topics(&[&topic], &opts).await {
                    tracing::warn!("Failed to delete Kafka topic {}: {}", topic, e);
                } else {
                    info!("Deleted Kafka topic: {}", topic);
                }
            }
        };

        // Run cleanup on a dedicated thread with its own runtime so it works regardless of the
        // caller's runtime flavor (current-thread or multi-thread) and doesn't race shutdown.
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime")
                .block_on(delete);
        })
        .join()
        .ok();
    }
}
