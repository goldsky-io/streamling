mod config_optimizer;
mod metadata;
mod schema_registry;

use streamling_config::{KafkaCompression, KafkaConfig};
use streamling_core::checkpoints::channels::{send, subscribe_with_id, unsubscribe};
use streamling_core::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointEpoch, CheckpointMessage,
    enrich_batch_metadata_with_checkpoints, extract_checkpoint_messages, now_ms,
    report_marker_at_sink, send_checkpoint_ack,
};
use streamling_core::data::{COLUMN_NAME_OP, RowKind};
use streamling_core::error::{ResultExt, StreamlingError};
use streamling_core::{
    streamling_err, streamling_retriable_err, streamling_user_bail, streamling_user_err,
};

use crate::table_providers::kafka::config_optimizer::KafkaConfigOptimizer;
use crate::table_providers::kafka::metadata::KafkaMetadata;
use crate::table_providers::kafka::schema_registry::{
    SubjectVersionsClient, is_writer_schema_ahead, patch_schema_id_with_overrides,
    retry_registry_call,
};
use apache_avro::Schema as AvroSchema;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, not_impl_err};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
    execution_plan::{Boundedness, EmissionType},
    filter::FilterExec,
    project_schema,
    projection::ProjectionExec,
};
use streamling_core::formats::avro::arrow_avro::ConfluentAvroDecoder;
use streamling_core::formats::avro::{
    FromArrowToAvroConverter, convert_avro_schema_to_arrow, post_process_avro_schema_for_writing,
    to_avro,
};
use streamling_core::formats::json::{FromArrowToJsonConverter, JsonToArrowConverter};
use streamling_core::formats::{FromArrowConverter, ToArrowConverter};
use streamling_core::operators::filter::StreamingFilterExec;
use streamling_core::operators::projection::StreamingProjectionExec;
use streamling_core::schema::arrow_schema_from_type_map;
use streamling_core::session::SessionManager;
use streamling_core::topology::SchemaIdOverride;
use streamling_state::StateKey;
use streamling_state::StateOperatorBackend;

use crate::util::lag::LagResult;
use apache_avro::types::Value::Union;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::sink::DataSink;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::metrics::MetricsSet;
use futures::StreamExt;
use once_cell::sync::OnceCell;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Header, Headers, OwnedHeaders, ToBytes};
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use rdkafka::util::Timeout;
use rdkafka::{
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList as KafkaTopicPartitionList,
};
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::schema_registry::{
    SrSettings, get_schema_by_id, get_schema_by_subject, post_schema,
};
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedSchema,
};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration as StdDuration, Instant as StdInstant, SystemTime, UNIX_EPOCH};
use streamling_core::operators::parallel_sink::ParallelSinkExec;
use streamling_core::operators::wrapping::WrappingDataSink;
use streamling_core::telemetry::provider::get_reference_name_from_metric_key;
use streamling_core::telemetry::recorder::{MetricsRecorder, get_metrics_recorder};
use streamling_core::topology::Telemetry;
use tokio::sync::watch;
use tokio::time::{Duration, Instant, sleep, sleep_until};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

static KAFKA_HEADER_OPERATION: &str = "dbz.op";

/// Producer context that captures delivery errors via rdkafka's callback mechanism.
/// Used with `ThreadedProducer` -- the internal poll thread invokes `delivery()`
/// for every produced message, and we record the first error so the pipeline
/// can fail on the next `check_error()` call.
struct KafkaProducerContext {
    first_error: Arc<std::sync::Mutex<Option<String>>>,
}

impl KafkaProducerContext {
    fn new() -> Self {
        Self {
            first_error: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    fn check_error(&self) -> streamling_core::error::Result<()> {
        let mut guard = self.first_error.lock().unwrap();
        if let Some(err) = guard.take() {
            Err(streamling_err!("{}", err))
        } else {
            Ok(())
        }
    }
}

impl ClientContext for KafkaProducerContext {}

impl rdkafka::producer::ProducerContext for KafkaProducerContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        result: &rdkafka::producer::DeliveryResult<'_>,
        _opaque: Self::DeliveryOpaque,
    ) {
        if let Err((e, msg)) = result {
            let topic = msg.topic();
            error!("Kafka delivery failed for topic {}: {}", topic, e);
            let mut guard = self.first_error.lock().unwrap();
            if guard.is_none() {
                *guard = Some(format!("Kafka delivery failed for topic {}: {}", topic, e));
            }
        }
    }
}

type KafkaThreadedProducer = ThreadedProducer<KafkaProducerContext>;

static CONSUMER_SEEK_TIMEOUT_SEC: u64 = 60;
static CONSUMER_ASSIGNMENT_TIMEOUT_SEC: u64 = 60;
static DEFAULT_CONSUMER_FETCH_TIMEOUT_SEC: u64 = 60;
static DEFAULT_STALL_WATCHDOG_TIMEOUT_SEC: u64 = 60;
static DEFAULT_LAG_REPORT_INTERVAL_MS: u64 = 30_000;
static WATCHDOG_LOG_INTERVAL: Duration = Duration::from_secs(60);

static DEFAULT_NUM_PARTITIONS: i32 = 4;

/// Wrapper around StreamConsumer that safely handles drop.
///
/// Dropping rdkafka consumers synchronously on a tokio worker thread can cause
/// deadlocks because rd_kafka_destroy() waits for internal rdkafka threads to
/// finish, but those threads may also be waiting for cleanup coordination.
///
/// This wrapper moves the drop operation to a blocking thread to prevent deadlock.
struct SafeKafkaConsumer {
    consumer: Option<Arc<StreamConsumer>>,
}

impl SafeKafkaConsumer {
    fn new(consumer: StreamConsumer) -> Self {
        tracing::debug!("SafeKafkaConsumer: created");
        Self {
            consumer: Some(Arc::new(consumer)),
        }
    }

    /// Explicitly forget the consumer without dropping.
    /// Use this when you've already called unsubscribe/unassign and want to
    /// avoid the cleanup overhead entirely.
    fn forget(mut self) {
        if let Some(consumer) = self.consumer.take() {
            tracing::info!("SafeKafkaConsumer: forget() called, skipping drop");
            std::mem::forget(consumer);
        }
    }
}

impl std::ops::Deref for SafeKafkaConsumer {
    type Target = Arc<StreamConsumer>;
    fn deref(&self) -> &Self::Target {
        self.consumer.as_ref().expect("consumer already taken")
    }
}

impl Drop for SafeKafkaConsumer {
    fn drop(&mut self) {
        if let Some(consumer) = self.consumer.take() {
            // Move the drop to a blocking thread to prevent deadlock with rdkafka threads
            tracing::warn!(
                "SafeKafkaConsumer: drop() triggered (early exit path), \
                 moving rd_kafka_destroy to spawn_blocking to prevent deadlock"
            );
            tokio::task::spawn_blocking(move || {
                tracing::info!("SafeKafkaConsumer: spawn_blocking starting rd_kafka_destroy");
                drop(consumer);
                tracing::info!("SafeKafkaConsumer: spawn_blocking rd_kafka_destroy completed");
            });
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct TopicPartitionOffset {
    pub offset: i64,
    pub updated_at: u64,
}

#[derive(Debug, Clone)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
    pub offset: TopicPartitionOffset,
}

impl TopicPartition {
    fn state_key(&self, reference_name: String) -> String {
        format!("{}:{}:{}", reference_name, self.topic, self.partition)
    }
}

pub struct TopicPartitionList {
    pub topic_partitions: Vec<TopicPartition>,
}

impl From<KafkaTopicPartitionList> for TopicPartitionList {
    fn from(kafka_topic_partition_list: KafkaTopicPartitionList) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let topic_partitions = kafka_topic_partition_list
            .elements()
            .iter()
            .map(|topic_partition| TopicPartition {
                topic: topic_partition.topic().to_string(),
                partition: topic_partition.partition(),
                offset: TopicPartitionOffset {
                    offset: topic_partition.offset().to_raw().unwrap(),
                    updated_at: now,
                },
            })
            .collect();

        TopicPartitionList { topic_partitions }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum KafkaFormat {
    Avro,
    Json,
}

/// Holds the appropriate converter based on the Kafka format.
/// This enum ensures type-safe access to converters without needing Option::unwrap().
enum KafkaSinkConverter<'a> {
    Avro {
        converter: FromArrowToAvroConverter,
        encoder: AvroEncoder<'a>,
    },
    Json {
        converter: FromArrowToJsonConverter,
    },
}

impl FromStr for KafkaFormat {
    type Err = StreamlingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "avro" => Ok(KafkaFormat::Avro),
            "json" => Ok(KafkaFormat::Json),
            _ => Err(streamling_user_err!("unsupported Kafka format: {}", s)),
        }
    }
}

/// Format-specific state needed to decode Kafka source payloads, resolved at source
/// construction. Grouping it behind an enum keeps the Avro-only state (schema registry
/// client, resolution settings) off the JSON path, where it is meaningless.
#[derive(Clone)]
pub(crate) enum KafkaSourceDecoding {
    Avro(Box<AvroSourceDecoding>),
    Json,
}

#[derive(Clone)]
pub(crate) struct AvroSourceDecoding {
    avro_schema: AvroSchema,
    reader_schema_id: u32,
    versions_client: Arc<SubjectVersionsClient>,
    validate_writer_schema_ordering: bool,
    schema_id_overrides: HashMap<u32, u32>,
    skip_schema_resolution: bool,
}

/// Per-stream payload decoder, built from [`KafkaSourceDecoding`] when the consumer task
/// starts. Mirrors the sink-side `KafkaSinkConverter`: each variant owns exactly the state
/// its format needs. The Avro state is boxed to keep the variant sizes balanced.
enum KafkaSourceConverter {
    Avro(Box<AvroStreamConverter>),
    Json(Box<JsonToArrowConverter>),
}

/// Confluent-framed Avro decode via arrow-avro. Writer schemas are registered by their
/// registry id on demand (fetched the first time an id is seen); the reader schema is
/// pre-registered under its own id so the common same-id case decodes without a fetch.
///
/// `ConfluentAvroDecoder` handles mid-batch writer-schema changes internally (it finalizes the
/// current generation and concatenates all generations on `flush`), so this converter just
/// decodes each message and flushes once per source batch.
struct AvroStreamConverter {
    decoder: ConfluentAvroDecoder,
    sr_settings: SrSettings,
    versions_client: Arc<SubjectVersionsClient>,
    subject: String,
    reader_schema_id: u32,
    skip_schema_resolution: bool,
    validate_writer_schema_ordering: bool,
    schema_id_overrides: HashMap<u32, u32>,
    topic: String,
    /// Full (unprojected) payload schema the decoder emits into; used for the empty-batch case.
    payload_schema: SchemaRef,
    payload_projection: Option<Vec<usize>>,
}

impl AvroStreamConverter {
    async fn decode_and_buffer(&mut self, message: &BorrowedMessage<'_>) -> Result<()> {
        let payload = message.payload();
        let patched = patch_schema_id_with_overrides(payload, &self.schema_id_overrides);
        let frame = patched.as_deref().or(payload).ok_or_else(|| {
            streamling_err!(
                "Kafka message has empty payload\n  topic: {}\n  partition: {}\n  offset: {}",
                message.topic(),
                message.partition(),
                message.offset()
            )
        })?;
        // Confluent wire format: 0x00 magic + 4-byte big-endian schema id.
        if frame.len() < 5 || frame[0] != 0x00 {
            return Err(datafusion::error::DataFusionError::from(streamling_err!(
                "payload is not Confluent-framed avro (len {}, first byte {:?})\n  topic: {}\n  partition: {}\n  offset: {}",
                frame.len(),
                frame.first(),
                message.topic(),
                message.partition(),
                message.offset()
            )));
        }
        let writer_schema_id = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]);

        // Fetch + register the writer schema on first sight. The decoder handles mid-batch
        // writer-schema changes internally (it finalizes the current generation and
        // concatenates on flush), so heterogeneous-writer batches (schema evolution) are safe.
        if !self.decoder.has_writer_schema(writer_schema_id) {
            let registered = retry_registry_call(
                format!("schema-registry get_schema_by_id(id={writer_schema_id})"),
                || get_schema_by_id(writer_schema_id, &self.sr_settings),
            )
            .await?;
            self.decoder
                .register_writer_schema(writer_schema_id, &registered.schema)
                .streamling_with_context(|| {
                    format!(
                        "failed to register writer schema id {writer_schema_id} (topic: {})",
                        self.topic
                    )
                })?;
        }

        // Preserve the writer-ahead fail-fast: when validation is on and the writer schema is
        // newer than the reader for this subject, crash so the pod restarts and refetches the
        // latest schema.
        if !self.skip_schema_resolution
            && self.validate_writer_schema_ordering
            && writer_schema_id != self.reader_schema_id
            && match is_writer_schema_ahead(
                &self.versions_client,
                &self.subject,
                writer_schema_id,
                self.reader_schema_id,
            )
            .await
            {
                Ok(ahead) => ahead,
                Err(e) => {
                    error!(topic = %self.topic, "writer-schema ordering check failed: {e}");
                    std::process::exit(1);
                }
            }
        {
            error!(
                topic = %self.topic,
                "writer_schema_id={writer_schema_id} is newer than reader_schema_id={}. Restarting to fetch the newest schema",
                self.reader_schema_id
            );
            std::process::exit(1);
        }

        self.decoder.decode(frame).streamling_with_context(|| {
            format!(
                "Avro decoding failed\n  topic: {}\n  partition: {}\n  offset: {}\n  patched: {}",
                message.topic(),
                message.partition(),
                message.offset(),
                patched.is_some()
            )
        })?;
        Ok(())
    }

    /// Flush the decoder into one batch for this interval (it concatenates across any
    /// writer-schema generations internally), then apply the payload column projection.
    fn convert_to_batch(&mut self) -> Result<RecordBatch> {
        let payload_batch_full = self
            .decoder
            .flush()
            .streamling_with_context(|| format!("arrow-avro flush failed (topic: {})", self.topic))?
            .unwrap_or_else(|| RecordBatch::new_empty(self.payload_schema.clone()));
        match &self.payload_projection {
            Some(projection) => payload_batch_full
                .project(projection)
                .map_err(datafusion::error::DataFusionError::from),
            None => Ok(payload_batch_full),
        }
    }
}

impl KafkaSourceConverter {
    /// Decode a single Kafka message payload and buffer it for the next batch.
    async fn decode_and_buffer(&mut self, message: &BorrowedMessage<'_>) -> Result<()> {
        match self {
            KafkaSourceConverter::Avro(avro) => avro.decode_and_buffer(message).await,
            KafkaSourceConverter::Json(converter) => {
                let json = json_payload_to_string(
                    message.payload().unwrap_or_default(),
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )?;
                converter.buffer(json);
                Ok(())
            }
        }
    }

    fn convert_to_batch(&mut self) -> Result<RecordBatch> {
        match self {
            KafkaSourceConverter::Avro(avro) => avro.convert_to_batch(),
            KafkaSourceConverter::Json(converter) => converter.convert_to_batch(),
        }
    }
}

/// Decode a Kafka JSON payload into a UTF-8 string, failing with message coordinates for
/// empty payloads and invalid UTF-8.
///
/// The Kafka source does not support tombstone records (null/empty payloads, e.g. CDC
/// deletes-as-tombstones) in any format — the Avro decoder rejects them too. An empty payload
/// is a hard error that terminates the source; represent deletes with a non-empty payload plus
/// a `dbz.op=d` header instead.
fn json_payload_to_string(
    payload: &[u8],
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<String> {
    if payload.is_empty() {
        return Err(streamling_user_err!(
            "JSON source received an empty payload; tombstone records are not supported\n  topic: {}\n  partition: {}\n  offset: {}",
            topic,
            partition,
            offset
        )
        .into());
    }

    String::from_utf8(payload.to_vec()).map_err(|e| {
        streamling_user_err!(
            "Kafka JSON payload is not valid UTF-8\n  topic: {}\n  partition: {}\n  offset: {}\n  error: {}",
            topic,
            partition,
            offset,
            e
        )
        .into()
    })
}

struct KafkaCommon {}

impl KafkaCommon {
    /// Fetches the Avro schema from the schema registry.
    /// Returns a tuple of (AvroSchema, schema_id).
    /// Requires schema_registry_url to be configured.
    fn fetch_avro_schema(
        config: &KafkaConfig,
        topic: &str,
    ) -> streamling_core::error::Result<(AvroSchema, u32)> {
        let schema_registry_settings = config.get_schema_registry_settings().ok_or_else(|| {
            streamling_user_err!("schema_registry_url is required for Avro format")
        })?;

        let avro_schema_wrapper = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                get_schema_by_subject(
                    &schema_registry_settings,
                    &SubjectNameStrategy::TopicNameStrategy(topic.to_owned(), false),
                )
                .await
                .streamling_with_context(|| {
                    format!("failed to fetch Avro schema from Schema Registry for topic {topic}")
                })
            })
        })?;

        debug!("Fetched schema: {:?}", avro_schema_wrapper);

        let schema_id = avro_schema_wrapper.id;
        Ok((
            AvroSchema::parse_str(avro_schema_wrapper.schema.as_str()).streamling_with_context(
                || format!("failed to parse Avro schema for topic {topic}"),
            )?,
            schema_id,
        ))
    }

    pub fn build_client(config: &KafkaConfig) -> ClientConfig {
        let mut builder = ClientConfig::new();

        builder
            .set("bootstrap.servers", &config.brokers)
            .set("security.protocol", &config.security_protocol)
            .set("socket.connection.setup.timeout.ms", "5000"); // dev only to fail fast

        if let Some(client_id) = &config.client_id {
            debug!("Using client id: {}", client_id);
            builder.set("client.id", client_id);
        }

        if config.security_protocol.to_lowercase().contains("sasl") {
            builder
                .set(
                    "sasl.mechanism",
                    config
                        .sasl_mechanism
                        .as_ref()
                        .expect("Missing sasl.mechanism"),
                )
                .set(
                    "sasl.username",
                    config
                        .sasl_username
                        .as_ref()
                        .expect("Missing sasl.username"),
                )
                .set(
                    "sasl.password",
                    config
                        .sasl_password
                        .as_ref()
                        .expect("Missing sasl.password"),
                );
        }

        builder
    }
}

#[allow(dead_code)]
struct KafkaSourceExec {
    reference_name: String,
    metric_metadata_id: String,
    full_schema: SchemaRef,
    payload_schema: SchemaRef,
    full_schema_projected: SchemaRef,
    payload_projection: Option<Vec<usize>>,
    decoding: KafkaSourceDecoding,
    should_enrich_op_column: bool,
    kafka_config: KafkaConfig,
    topic: String,
    start_at: Option<String>,
    filter: Option<String>,
    cached_properties: Arc<PlanProperties>,
    record_batch_interval_ms: u64,
    record_batch_size: u32,
    internal_buffer_size: u32,
    include_metadata: bool,
    state_backend: Arc<dyn StateOperatorBackend<TopicPartitionOffset>>,
    shutdown_rx: watch::Receiver<bool>,
    num_records_before_stop: Option<u64>,
    /// Number of concurrent consumer instances; one `execute` call per instance.
    parallelism: usize,
    /// Resolved once at construction so every instance joins the *same* consumer
    /// group. Computing it per `create_consumer` call would mint a fresh
    /// `df-consumer-{uuid}` per instance when no group is configured, and each
    /// instance would then read the whole topic instead of a disjoint slice.
    group_id: String,
    lag_group_id: String,
}

impl Debug for KafkaSourceExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("KafkaSourceExec") // TODO
    }
}

impl KafkaSourceExec {
    fn consumer_fetch_timeout_sec() -> u64 {
        static FETCH_TIMEOUT_SEC: OnceCell<u64> = OnceCell::new();
        *FETCH_TIMEOUT_SEC.get_or_init(|| {
            std::env::var("STREAMLING__KAFKA_SOURCE__CONSUMER_FETCH_TIMEOUT_SEC")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_CONSUMER_FETCH_TIMEOUT_SEC)
        })
    }

    fn stall_watchdog_timeout_sec() -> u64 {
        static STALL_WATCHDOG_TIMEOUT_SEC: OnceCell<u64> = OnceCell::new();
        *STALL_WATCHDOG_TIMEOUT_SEC.get_or_init(|| {
            std::env::var("STREAMLING__KAFKA_SOURCE__STALL_WATCHDOG_TIMEOUT_SEC")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_STALL_WATCHDOG_TIMEOUT_SEC)
        })
    }

    fn new(
        reference_name: String,
        projections: Option<&Vec<usize>>,
        full_schema: SchemaRef,
        payload_schema: SchemaRef,
        config: KafkaConfig,
        topic: String,
        start_at: Option<String>,
        filter: Option<String>,
        decoding: KafkaSourceDecoding,
        should_enrich_op_column: bool,
        record_batch_interval_ms: u64,
        record_batch_size: u32,
        internal_buffer_size: u32,
        include_metadata: bool,
        state_backend: Arc<dyn StateOperatorBackend<TopicPartitionOffset>>,
        shutdown_rx: watch::Receiver<bool>,
        num_records_before_stop: Option<u64>,
        metric_metadata_id: String,
        parallelism: usize,
    ) -> Self {
        let full_schema_projected = project_schema(&full_schema, projections).unwrap();
        let cached_properties =
            Self::compute_properties(full_schema_projected.clone(), parallelism);
        let group_id = Self::resolve_group_id(&config, &reference_name, false);
        let lag_group_id = Self::resolve_group_id(&config, &reference_name, true);

        let payload_projection = projections.cloned().map(|vec| {
            // Filter to only include indices valid for the payload schema.
            // Metadata columns (partition, offset, timestamp) are handled separately
            // after payload conversion, so they should not be included in the payload projection.
            let max_valid_index = payload_schema.fields.len();
            vec.into_iter().filter(|&i| i < max_valid_index).collect()
        });

        Self {
            reference_name,
            metric_metadata_id,
            full_schema,
            payload_schema,
            full_schema_projected,
            payload_projection,
            decoding,
            should_enrich_op_column,
            kafka_config: config,
            topic,
            start_at,
            filter,
            cached_properties: Arc::new(cached_properties),
            record_batch_interval_ms,
            record_batch_size,
            internal_buffer_size,
            include_metadata,
            state_backend,
            shutdown_rx,
            num_records_before_stop,
            parallelism,
            group_id,
            lag_group_id,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    ///
    /// Deliberately `UnknownPartitioning`, never `Hash`: Kafka places records by
    /// murmur2 over the message key, which has nothing to do with the hash a
    /// downstream keyed sink needs. Declaring `Hash` here would let the planner
    /// elide the sink-edge exchange that actually does the keying.
    fn compute_properties(schema: SchemaRef, parallelism: usize) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(parallelism.max(1)),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        )
    }

    /// Each source gets its own consumer group so sources reading different
    /// topics never fight over assignments; all instances of *one* source share
    /// that group, which is what makes the broker split the topic's partitions
    /// between them. The lag consumer needs a separate group so it doesn't
    /// compete for partitions with the main consumers.
    fn resolve_group_id(
        config: &KafkaConfig,
        reference_name: &str,
        is_lag_consumer: bool,
    ) -> String {
        match &config.consumer_group_id {
            Some(base_id) if is_lag_consumer => format!("{}-{}-lag", base_id, reference_name),
            Some(base_id) => format!("{}-{}", base_id, reference_name),
            None if is_lag_consumer => format!("df-consumer-{}-lag", Uuid::new_v4()),
            None => format!("df-consumer-{}", Uuid::new_v4()),
        }
    }

    fn create_consumer(
        config: &KafkaConfig,
        starting_offsets: &Option<String>,
        reference_name: &str,
        group_id: &str,
        is_lag_consumer: bool,
    ) -> StreamConsumer {
        let mut builder = KafkaCommon::build_client(config);

        debug!(
            "[{}] Using consumer group id: {} (lag_consumer: {})",
            reference_name, group_id, is_lag_consumer
        );

        builder
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            // .set("debug", "all") // enable for debugging
            .set(
                "auto.offset.reset",
                starting_offsets.as_deref().unwrap_or("earliest"),
            );

        let kafka_config_optimizer = KafkaConfigOptimizer::new(config);
        kafka_config_optimizer
            .optimized_consumer_config()
            .iter()
            .for_each(|(key, value)| {
                builder.set(key, value);
            });

        builder.create().expect("Failed to create client")
    }

    fn extract_op_from_headers(headers: Option<&BorrowedHeaders>) -> Option<&str> {
        headers.and_then(|headers| {
            // no API to get a header by value >_<
            for i in 0..headers.count() {
                let header = headers.get_as::<str>(i).unwrap();
                if header.key == KAFKA_HEADER_OPERATION {
                    return Some(header.value.unwrap());
                } else {
                    continue;
                }
            }
            None
        })
    }

    async fn wait_for_assignment(
        consumer: &StreamConsumer,
    ) -> streamling_core::error::Result<KafkaTopicPartitionList> {
        let timeout = Duration::from_secs(CONSUMER_ASSIGNMENT_TIMEOUT_SEC);
        let tpl = tokio::time::timeout(timeout, async {
            loop {
                if let Ok(tpl) = consumer.assignment()
                    && !tpl.elements().is_empty()
                {
                    return tpl;
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| {
            streamling_retriable_err!(
                "partition assignment timed out after {}s",
                CONSUMER_ASSIGNMENT_TIMEOUT_SEC
            )
        })?;
        Ok(tpl)
    }

    /// Triggers the consumer poll loop until we observe either:
    /// - a partition assignment, or
    /// - the first message.
    ///
    /// If no message arrives within the fetch timeout, we keep retrying
    /// so idle topics do not fail source startup.
    async fn wait_for_initial_assignment_or_message(
        consumer: &StreamConsumer,
        topic: &str,
    ) -> streamling_core::error::Result<()> {
        let fetch_timeout_sec = Self::consumer_fetch_timeout_sec();
        loop {
            if let Ok(tpl) = consumer.assignment()
                && !tpl.elements().is_empty()
            {
                return Ok(());
            }

            match tokio::time::timeout(
                Duration::from_secs(fetch_timeout_sec),
                consumer.stream().next(),
            )
            .await
            {
                Ok(Some(Ok(_message))) => return Ok(()),
                Ok(Some(Err(e))) => {
                    return Err(streamling_err!(
                        "fetching initial message from topic '{}': {:?}",
                        topic,
                        e
                    ));
                }
                Ok(None) => {
                    return Err(streamling_err!(
                        "Kafka stream ended unexpectedly (topic: {})",
                        topic
                    ));
                }
                Err(_) => {
                    debug!(
                        "initial message fetch timed out after {}s (topic: {}), no new records yet; retrying",
                        fetch_timeout_sec, topic
                    );
                }
            }
        }
    }

    async fn find_offsets_in_state_backend(
        state_backend: Arc<dyn StateOperatorBackend<TopicPartitionOffset>>,
        reference_name: String,
        topic_partition_list: KafkaTopicPartitionList,
    ) -> KafkaTopicPartitionList {
        let mut state_topic_partition_list = KafkaTopicPartitionList::new();

        for topic_partition in TopicPartitionList::from(topic_partition_list).topic_partitions {
            let state_key = StateKey::from(topic_partition.state_key(reference_name.clone()));
            let topic_partition_state = state_backend.get(state_key).await.unwrap();

            if let Some(state) = topic_partition_state {
                state_topic_partition_list
                    .add_partition_offset(
                        &topic_partition.topic,
                        topic_partition.partition,
                        Offset::Offset(state.offset),
                    )
                    .unwrap();
            }
        }

        state_topic_partition_list
    }
}

fn is_stalled_with_lag(
    has_received_data: bool,
    elapsed_since_last_input: Duration,
    stall_timeout: Duration,
    max_observed_lag: Option<i64>,
) -> bool {
    has_received_data
        && elapsed_since_last_input > stall_timeout
        && max_observed_lag.is_some_and(|lag| lag != 0)
}

fn is_stalled_lag_unavailable(
    has_received_data: bool,
    elapsed_since_last_input: Duration,
    lag_unavailable_for: Option<Duration>,
    stall_timeout: Duration,
) -> bool {
    has_received_data
        && elapsed_since_last_input > stall_timeout
        && lag_unavailable_for.is_some_and(|elapsed| elapsed > stall_timeout)
}

struct KafkaSourceWatchdogState {
    has_received_data: bool,
    last_input_row_at: Instant,
    max_observed_lag: Option<i64>,
    lag_unavailable_since: Option<Instant>,
    stall_timeout: Duration,
    stall_check_count: u64,
    last_stall_log_at: Option<Instant>,
    lag_rx: watch::Receiver<Option<i64>>,
}

impl KafkaSourceWatchdogState {
    fn new(stall_timeout: Duration, lag_rx: watch::Receiver<Option<i64>>) -> Self {
        Self {
            has_received_data: false,
            last_input_row_at: Instant::now(),
            max_observed_lag: None,
            lag_unavailable_since: None,
            stall_timeout,
            stall_check_count: 0,
            last_stall_log_at: None,
            lag_rx,
        }
    }

    fn on_record(&mut self) {
        self.has_received_data = true;
        self.last_input_row_at = Instant::now();
        self.stall_check_count = 0;
        self.last_stall_log_at = None;
    }

    fn refresh_lag(&mut self) {
        if !self.has_received_data {
            return;
        }

        self.max_observed_lag = *self.lag_rx.borrow();

        if self.max_observed_lag.is_some() {
            self.lag_unavailable_since = None;
        } else {
            self.lag_unavailable_since.get_or_insert_with(Instant::now);
        }
    }

    fn is_stalled(&self, elapsed_since_last_input: Duration) -> bool {
        is_stalled_with_lag(
            self.has_received_data,
            elapsed_since_last_input,
            self.stall_timeout,
            self.max_observed_lag,
        ) || is_stalled_lag_unavailable(
            self.has_received_data,
            elapsed_since_last_input,
            self.lag_unavailable_since.map(|ts| ts.elapsed()),
            self.stall_timeout,
        )
    }

    fn stall_message(&self, topic: &str, elapsed_since_last_input: Duration) -> String {
        if is_stalled_with_lag(
            self.has_received_data,
            elapsed_since_last_input,
            self.stall_timeout,
            self.max_observed_lag,
        ) {
            format!(
                "Kafka source stalled for {:?} (timeout: {:?}) with positive lag {:?} (topic: {})",
                elapsed_since_last_input, self.stall_timeout, self.max_observed_lag, topic
            )
        } else {
            format!(
                "Kafka source stalled for {:?} (timeout: {:?}) and watchdog lag was unavailable for {:?} (topic: {})",
                elapsed_since_last_input,
                self.stall_timeout,
                self.lag_unavailable_since.map(|ts| ts.elapsed()),
                topic
            )
        }
    }

    fn check_and_alert(&mut self, topic: &str) {
        let elapsed = self.last_input_row_at.elapsed();
        if !self.is_stalled(elapsed) {
            return;
        }
        self.stall_check_count += 1;
        let should_log = match self.last_stall_log_at {
            None => true,
            Some(last) => last.elapsed() >= WATCHDOG_LOG_INTERVAL,
        };
        if should_log {
            self.last_stall_log_at = Some(Instant::now());
            warn!(
                "{} (occurrence: {})",
                self.stall_message(topic, elapsed),
                self.stall_check_count
            );
        }
    }
}

async fn calculate_lag_task(
    reference_name: String,
    metric_metadata_id: String,
    kafka_topic_partition_list: KafkaTopicPartitionList,
    consumer: StreamConsumer,
    state_backend: Arc<dyn StateOperatorBackend<TopicPartitionOffset>>,
    metrics_recorder: Arc<MetricsRecorder>,
    lag_report_interval_ms: Option<u64>,
    max_lag_tx: watch::Sender<Option<i64>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(
        lag_report_interval_ms.unwrap_or(DEFAULT_LAG_REPORT_INTERVAL_MS),
    ));

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("Lag task received shutdown signal for {}", reference_name);
                    break;
                }
            },
            _ = interval.tick() => {
                trace!("Calculating lag for reference_name: {}", reference_name);
                let mut lag_results = Vec::new();
        let topic_partition_list = TopicPartitionList::from(kafka_topic_partition_list.clone());

        let mut max_lag: Option<i64> = None;

        for topic_partition in topic_partition_list.topic_partitions {
            // Add yield point for better cancellation responsiveness
            tokio::task::yield_now().await;

            trace!(
                "Calculating lag for reference_name: {}, topic: {}, partition: {}",
                reference_name, topic_partition.topic, topic_partition.partition
            );
            let (_, high_watermark) = match consumer.fetch_watermarks(
                &topic_partition.topic,
                topic_partition.partition,
                Duration::from_secs(5),
            ) {
                Ok(watermarks) => watermarks,
                Err(e) => {
                    error!(
                        "Failed to fetch watermarks for topic: {}, partition {}: {:?}; lag metric will not be reported",
                        topic_partition.topic, topic_partition.partition, e
                    );
                    continue;
                }
            };

            let state_key = StateKey::from(topic_partition.state_key(reference_name.to_string()));
            let tail_at = match state_backend.get(state_key).await {
                Ok(Some(offset_state)) => offset_state.offset,
                Ok(None) => {
                    trace!(
                        "No offset found in state backend for topic: {}, partition {}; fallback to 0",
                        topic_partition.topic, topic_partition.partition
                    );
                    0
                }
                Err(_) => {
                    error!(
                        "Failed to fetch offsets from state backend for topic: {}, partition {}, lag metric will not be reported",
                        topic_partition.topic, topic_partition.partition,
                    );
                    continue;
                }
            };

            let partition_lag = (high_watermark - tail_at).max(0);
            max_lag = Some(max_lag.unwrap_or(0).max(partition_lag));

            lag_results.push(LagResult {
                metric_metadata_id: metric_metadata_id.to_string(),
                unit: "messages".to_string(),
                head_at_provider: "kafka_consumer".to_string(),
                head_at: high_watermark as u64,
                tail_at: tail_at as u64,
                tags: vec![(
                    String::from("partition"),
                    topic_partition.partition.to_string(),
                )],
            });
        }

        let _ = max_lag_tx.send(max_lag);

        lag_results.iter().for_each(|lag_result| {
            let tags = lag_result
                .tags
                .iter()
                .map(|tuple| (tuple.0.as_str(), tuple.1.as_str()))
                .collect();
            let metric_name = &format!("{}_{}_lag", lag_result.head_at_provider, lag_result.unit);
            debug!("Recording lag result with metric_name: {}", metric_name);
            metrics_recorder.record_gauge_w_tags(
                metric_name,
                lag_result.head_at - lag_result.tail_at,
                tags,
                lag_result.metric_metadata_id.as_str(),
            );
        });

            }
        }
    }

    info!("Lag task shutting down for {}", reference_name);
}

impl DisplayAs for KafkaSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        // One consumer instance per partition, so this is the source's
        // `parallelism` and the width the rest of the plan inherits.
        write!(
            f,
            "KafkaSourceExec: partitions={}",
            self.cached_properties
                .output_partitioning()
                .partition_count()
        )
    }
}

impl ExecutionPlan for KafkaSourceExec {
    fn name(&self) -> &'static str {
        "KafkaSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cached_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut builder = RecordBatchReceiverStreamBuilder::new(
            self.full_schema_projected.clone(),
            self.internal_buffer_size as usize,
        );
        let tx = builder.tx();

        let state_backend = self.state_backend.clone();

        // One consumer instance per output partition, all in the same group: the
        // broker hands each a disjoint slice of the topic's Kafka partitions.
        // Checkpoint state is keyed by (topic, kafka partition) and seeks are
        // scoped to this instance's assignment, so instances never collide.
        let consumer = SafeKafkaConsumer::new(Self::create_consumer(
            &self.kafka_config,
            &self.start_at,
            &self.reference_name,
            &self.group_id,
            false,
        ));
        consumer
            .subscribe(&[&*self.topic])
            .streamling_with_context(|| format!("failed to subscribe to topic '{}'", self.topic))?;

        let mut consumer_offsets: BTreeMap<CheckpointEpoch, KafkaTopicPartitionList> =
            BTreeMap::new();
        // local cache for the state backend
        let mut committed_offsets: Option<KafkaTopicPartitionList> = None;

        let mut converter = match &self.decoding {
            KafkaSourceDecoding::Avro(avro) => {
                // arrow-avro decode: the reader schema drives arrow-avro's schema resolution and
                // supplies the target Arrow schema (== self.payload_schema). Writer schemas are
                // registered by their Confluent registry id on demand; the reader schema is
                // pre-registered under its own id so the common same-id case decodes without a fetch.
                let sr_settings = self
                    .kafka_config
                    .get_schema_registry_settings()
                    .expect("schema_registry_url is required for Avro format");
                let reader_schema_json = serde_json::to_string(&avro.avro_schema)
                    .expect("reader avro schema serializes to JSON");
                let mut decoder = ConfluentAvroDecoder::new()
                    .with_reader_schema(&avro.avro_schema)
                    .map_err(|e| streamling_err!("failed to set arrow-avro reader schema: {e}"))?
                    // Honor `skip_schema_resolution`: when set, decode each message against its own
                    // writer schema with no writer→reader resolution (matching the vendored path),
                    // then coerce the batch to the target schema by field name.
                    .with_schema_resolution(!avro.skip_schema_resolution);
                decoder
                    .register_writer_schema(avro.reader_schema_id, &reader_schema_json)
                    .map_err(|e| {
                        streamling_err!("failed to register reader schema in arrow-avro store: {e}")
                    })?;
                KafkaSourceConverter::Avro(Box::new(AvroStreamConverter {
                    decoder,
                    sr_settings,
                    versions_client: avro.versions_client.clone(),
                    // Subject convention: TopicNameStrategy with is_key=false yields "{topic}-value".
                    // See SubjectNameStrategy::get_subject in schema_registry_converter.
                    subject: format!("{}-value", self.topic),
                    reader_schema_id: avro.reader_schema_id,
                    skip_schema_resolution: avro.skip_schema_resolution,
                    validate_writer_schema_ordering: avro.validate_writer_schema_ordering,
                    schema_id_overrides: avro.schema_id_overrides.clone(),
                    topic: self.topic.clone(),
                    payload_schema: self.payload_schema.clone(),
                    payload_projection: self.payload_projection.clone(),
                }))
            }
            KafkaSourceDecoding::Json => {
                // Apply the same column projection Avro does, so the decoded batch lines up
                // with `full_schema_projected`. The schema-driven JSON decoder reads fields
                // by name and ignores any payload keys not in the (projected) schema.
                let projected_payload_schema =
                    project_schema(&self.payload_schema, self.payload_projection.as_ref())?;
                KafkaSourceConverter::Json(Box::new(JsonToArrowConverter::new(
                    projected_payload_schema,
                    true,
                    None,
                )))
            }
        };

        let full_schema = self.full_schema_projected.clone();

        let batch_interval = Duration::from_millis(self.record_batch_interval_ms);
        let batch_size_limit = self.record_batch_size as u64;

        let reference_name = self.reference_name.clone();
        // Each consumer instance is its own subscriber. Unsubscribing on exit
        // matters once there are several: `send` errors out on a dropped
        // receiver of a source that has not signalled completion, so one
        // instance shutting down early would fail the coordinator's broadcast to
        // the instances still running.
        let (receiver, checkpoint_subscriber_id) =
            subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);

        let mut metadata = if self.include_metadata {
            Some(KafkaMetadata::default())
        } else {
            None
        };

        let should_enrich_op_column = self.should_enrich_op_column;
        // Append the enriched op column to the data only when the pushed-down
        // scan projection kept it: a transform that recomputes `_gs_op` (or
        // never reads it) lets the engine prune it from the scan schema, and
        // appending the array anyway makes every batch one column wider than
        // the schema ("number of columns(N+1) must match number of fields(N)").
        let append_op_column = self.should_enrich_op_column
            && self
                .full_schema_projected
                .field_with_name(COLUMN_NAME_OP)
                .is_ok();
        let start_at = self.start_at.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let num_records_before_stop = self.num_records_before_stop;
        let metric_metadata_id = self.metric_metadata_id.clone();
        let kafka_lag_reporter_interval = self.kafka_config.lag_report_interval_ms;
        let metrics_recorder = get_metrics_recorder().clone();
        // Lag is a per-source metric, and every instance would report the same
        // topic-wide numbers, so only instance 0 runs the reporter.
        let lag_consumer = (partition == 0).then(|| {
            Self::create_consumer(
                &self.kafka_config,
                &self.start_at,
                &self.reference_name,
                &self.lag_group_id,
                true,
            )
        });
        let topic = self.topic.clone();
        let stall_watchdog_timeout = Duration::from_secs(Self::stall_watchdog_timeout_sec());
        builder.spawn(async move {
            let (max_lag_tx, max_lag_rx) = watch::channel(None);
            let mut watchdog = KafkaSourceWatchdogState::new(stall_watchdog_timeout, max_lag_rx);
            // first, wait for the consumer to be assigned partitions and then check the offsets
            // in the state backend

            // We have to call "poll" on a consumer to get the assignment
            // Unfortunately, the high-level StreamConsumer doesn't expose this method
            // So, instead, we trigger it indirectly by calling "next" on the stream
            // We'll seek to the correct position afterward based on state backend or config
            Self::wait_for_initial_assignment_or_message(&consumer, &topic).await?;

            // A blocking call to wait for the assignment to finish
            let kafka_topic_partition_list = Self::wait_for_assignment(&consumer).await?;
            if let Some(lag_consumer) = lag_consumer {
                tokio::spawn(calculate_lag_task(
                    reference_name.clone(),
                    metric_metadata_id.clone(),
                    kafka_topic_partition_list.clone(),
                    lag_consumer,
                    state_backend.clone(),
                    metrics_recorder.clone(),
                    kafka_lag_reporter_interval,
                    max_lag_tx,
                    shutdown_rx.clone(),
                ));
            }
            let kafka_topic_partition_list_to_seek = Self::find_offsets_in_state_backend(
                state_backend.clone(),
                reference_name.clone(),
                kafka_topic_partition_list,
            ).await;

            if kafka_topic_partition_list_to_seek.count() > 0 {
                // One info! summary so the multi-partition case doesn't dump
                // N info! lines on startup; per-partition targets stay at
                // debug! since this fires once per pipeline lifetime.
                info!(
                    "Seeking {} partitions of topic '{}' from persisted state",
                    kafka_topic_partition_list_to_seek.count(),
                    topic
                );
                for topic_partition in kafka_topic_partition_list_to_seek.elements() {
                    debug!(
                        "Seeking to topic: {}, partition: {}, offset: {}",
                        topic_partition.topic(),
                        topic_partition.partition(),
                        topic_partition.offset().to_raw().unwrap()
                    );
                }

                let seek_result = consumer
                    // batch seek for performance
                    .seek_partitions(kafka_topic_partition_list_to_seek.clone(), Timeout::After(Duration::from_secs(CONSUMER_SEEK_TIMEOUT_SEC)))
                    .streamling_with_context(|| format!("failed to seek partitions from state (topic: {})", topic))?;

                trace!("Seek result: {:?}", seek_result);

                for topic_partition in seek_result.elements() {
                    if topic_partition.error().is_err() {
                        error!("Failed to seek to partition: {:?}", topic_partition.error());
                        return Err(streamling_err!("{}", topic_partition.error().unwrap_err()).into());
                    }
                }

                // if the seek was successful, we assume the offsets from the state were committed
                committed_offsets = Some(kafka_topic_partition_list_to_seek);
            } else {
                // No offsets found in the state backend, so we need to seek to the configured
                // starting position (earliest/latest) for all assigned partitions
                let target_offset = match start_at.as_deref() {
                    Some("latest") => Offset::End,
                    _ => Offset::Beginning, // "earliest" or default
                };

                debug!(
                    "No state found, seeking all partitions to {:?}",
                    target_offset
                );

                // Get the current assignment and set all partitions to the target offset
                let mut assignment = consumer
                    .assignment()
                    .streamling_with_context(|| format!("failed to get consumer assignment (topic: {})", topic))?;

                assignment
                    .set_all_offsets(target_offset)
                    .streamling_with_context(|| format!("failed to set offsets to {:?} (topic: {})", target_offset, topic))?;

                let seek_result = consumer
                    .seek_partitions(assignment, Timeout::After(Duration::from_secs(CONSUMER_SEEK_TIMEOUT_SEC)))
                    .streamling_with_context(|| format!("failed to seek partitions to {:?} (topic: {})", target_offset, topic))?;

                let mut seek_count = 0usize;
                for topic_partition in seek_result.elements() {
                    if topic_partition.error().is_err() {
                        error!("Failed to seek to partition: {:?}", topic_partition.error());
                        return Err(streamling_err!("{}", topic_partition.error().unwrap_err()).into());
                    }
                    seek_count += 1;
                    // Per-partition seek confirmation, kept at debug! since
                    // this fires once per pipeline lifetime.
                    debug!(
                        "Seeked to topic: {}, partition: {}, offset: {:?}",
                        topic_partition.topic(),
                        topic_partition.partition(),
                        topic_partition.offset()
                    );
                }
                debug!(
                    "Seeked {} partitions of topic '{}' to start_at={:?}",
                    seek_count, topic, target_offset
                );
            }

            // now, enter the main loop and start processing messages
            let metrics_recorder = get_metrics_recorder().clone();

            // Collection to store checkpoint messages for batch enrichment
            let mut checkpoint_messages_buffer: Vec<CheckpointMessage> = Vec::new();
            // Messages drained from the receiver inside the inner select (e.g.
            // the SourceComplete probe below) that still need full dispatch.
            // We never drop a CheckpointMessage from the receiver: anything
            // we pull off the channel that isn't acted on immediately lands
            // here and is replayed through the main dispatch after the next
            // batch is built.
            //
            // NOTE: today this buffer only fills in test/job mode, because the
            // sole producer (the SourceComplete probe below) is gated on
            // `num_records_before_stop`. Production deployments don't set
            // that env, so this vec stays empty and the main dispatch is the
            // only path consuming the channel. Keep the indirection regardless
            // so that any future intra-select probe can route through here
            // without re-introducing the marker-loss bug.
            let mut pending_checkpoint_messages: Vec<CheckpointMessage> = Vec::new();

            // Create interval timer for checking SourceComplete (only in test mode)
            let mut source_complete_interval = num_records_before_stop
                .map(|_| tokio::time::interval(Duration::from_millis(5)));

            'outer: loop {
                watchdog.refresh_lag();
                watchdog.check_and_alert(&topic);

                let outer_loop_start_at = Instant::now();
                let deadline = Instant::now() + batch_interval;
                // SIGTERM on Unix; Windows has no SIGTERM, so the shutdown branch
                // below falls back to Ctrl-C there.
                #[cfg(unix)]
                let mut sigterm = {
                    use tokio::signal::unix::{SignalKind, signal};
                    signal(SignalKind::terminate())?
                };

                let mut row_kinds = Vec::new();
                let mut batch_row_count = 0u64;

                loop {
                    tokio::select! {
                        // Check for shutdown signal
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                info!("Kafka consumer received shutdown signal");
                                break 'outer;
                            }
                        },
                        // Check for SourceComplete messages (only in test mode with num_records_before_stop).
                        //
                        // This probe shares the checkpoint-coordinator receiver with the main
                        // marker/finalizer dispatch below, so we MUST NOT drop any non-SourceComplete
                        // messages we happen to drain here — otherwise Markers/Finalizers that arrive
                        // while the kafka consumer is idle (notably: just after a hybrid
                        // bounded→unbounded transition, before kafka has emitted its first batch)
                        // get silently consumed and the coordinator stalls forever waiting for an
                        // ack that the sink will never see. Anything we pull off the channel that
                        // isn't a SourceComplete is forwarded to `pending_checkpoint_messages` and
                        // replayed through the main dispatch loop.
                        _ = async {
                            if let Some(ref mut interval) = source_complete_interval {
                                interval.tick().await;
                            } else {
                                futures::future::pending::<()>().await;
                            }
                        } => {
                            loop {
                                match receiver.try_recv() {
                                    Ok(CheckpointMessage::SourceComplete(name)) => {
                                        debug!("Received SourceComplete as part of num_records_before_stop for {}, shutting down", name);
                                        break 'outer;
                                    }
                                    Ok(msg) => {
                                        pending_checkpoint_messages.push(msg);
                                    }
                                    Err(_) => break,
                                }
                            }
                        },
                        // Await the next record
                        messge = consumer.recv() => {
                            match messge {
                                Ok(message) => {
                                    if should_enrich_op_column {
                                        let op = Self::extract_op_from_headers(message.headers());
                                        let row_kind = op.map(RowKind::from_dbz_op).unwrap_or(RowKind::Insert);
                                        metrics_recorder.record_count_w_tags(
                                            "kafka_consumer.row_kind_count",
                                            1,
                                            vec!(("kind", row_kind.to_str().as_ref())),
                                            metric_metadata_id.as_str()
                                        );
                                        if append_op_column {
                                            row_kinds.push(row_kind.to_str());
                                        }
                                    }

                                    if let Some(meta) = &mut metadata {
                                        meta.extract_metadata(&message);
                                    }

                                    converter.decode_and_buffer(&message).await?;
                                    watchdog.on_record();
                                    batch_row_count += 1;
                                    metrics_recorder.record_count("input_rows", 1, metric_metadata_id.as_str());
                                    if batch_row_count >= batch_size_limit {
                                        break;
                                    }
                                }
                                Err(err) => {
                                    error!("Error reading from Kafka {:?}", err);
                                    // TODO: retries?
                                }
                            }
                        },
                        _ = sleep_until(deadline) => {
                            break;
                        },
                        // shutdown hook: SIGTERM on Unix, Ctrl-C on Windows
                        _ = async {
                            #[cfg(unix)]
                            { let _ = sigterm.recv().await; }
                            #[cfg(not(unix))]
                            { let _ = tokio::signal::ctrl_c().await; }
                        } => {
                            // TODO: flush, cleanup, etc.
                            info!("Received shutdown signal, shutting down");
                            break 'outer; // exit the outer loop, which terminates the task
                        }
                    }
                }

                let payload_batch = converter.convert_to_batch()?;

                let mut record_columns = payload_batch.columns().to_vec();
                if append_op_column {
                    record_columns.push(Arc::new(StringArray::from(row_kinds.clone())));
                    row_kinds.clear();
                }

                if let Some(meta) = &mut metadata {
                    meta.copy_records_into(&mut record_columns);
                    meta.clear();
                }

                // Enrich schema metadata with checkpoint messages
                let mut batch_metadata = HashMap::new();
                enrich_batch_metadata_with_checkpoints(&mut batch_metadata, &checkpoint_messages_buffer);
                let enriched_schema = Arc::new(Schema::new_with_metadata(
                    full_schema.fields.clone(),
                    batch_metadata,
                ));

                let record_batch = RecordBatch::try_new(enriched_schema, record_columns)?;
                // Clear checkpoint messages buffer after enriching the batch
                checkpoint_messages_buffer.clear();

                // XXX: looks like we get lots of empty record batches
                if record_batch.num_rows() != 0 {
                    debug!("Record batch size: {:?}", record_batch.num_rows());
                }

                // Always check for checkpoint messages (non-blocking).
                //
                // Messages can come from two places: any that the inner SourceComplete
                // probe consumed but didn't itself act on land in
                // `pending_checkpoint_messages`; the rest we drain off the receiver here.
                // Both feed into the same dispatch below so Markers/Finalizers/SourceComplete
                // are handled identically regardless of which path pulled them off the channel.
                let mut messages_to_process: Vec<CheckpointMessage> =
                    std::mem::take(&mut pending_checkpoint_messages);
                loop {
                    match receiver.try_recv() {
                        Ok(msg) => messages_to_process.push(msg),
                        Err(crossbeam::channel::TryRecvError::Empty) => {
                            // No more messages available, exit the loop
                            debug!("Checkpoint loop: channel empty, exiting");
                            break;
                        }
                        Err(crossbeam::channel::TryRecvError::Disconnected) => {
                            warn!("Checkpoint channel disconnected unexpectedly");
                            break;
                        }
                    }
                }

                for message in messages_to_process {
                    match message {
                        CheckpointMessage::Marker { epoch, created_at_ms } => {
                            debug!("Received epoch marker: {:?}", epoch);
                            checkpoint_messages_buffer.push(CheckpointMessage::Marker { epoch: epoch.clone(), created_at_ms });

                            match consumer.position() {
                                Ok(position) => {
                                    debug!("Current position: {:?}", position);
                                    consumer_offsets.insert(epoch.clone(), position);
                                }
                                Err(e) => {
                                    error!("Failed to get current position: {}", e);
                                }
                            }
                        }
                        CheckpointMessage::Finalizer(epoch) => {
                            debug!("Received epoch finalizer: {:?}", epoch);
                            checkpoint_messages_buffer.push(CheckpointMessage::Finalizer(epoch.clone()));

                            if let Some(position) = consumer_offsets.get(&epoch) {

                                if committed_offsets.is_some() && position == committed_offsets.as_ref().unwrap() {
                                    debug!("Current position already committed, so skipping commit");
                                } else {
                                    // Filter out invalid offsets
                                    // This is needed in order to make commit progress on other partitions that are valid
                                    // A few known reasons for the offsets to be "invalid":
                                    // - A newly assigned partition after a rebalance
                                    // - No new messages in the topic after the seek
                                    let mut filtered_position = KafkaTopicPartitionList::new();
                                    for tp in position.elements().iter().filter(|tp| tp.offset() != Offset::Invalid) {
                                        filtered_position
                                            .add_partition_offset(tp.topic(), tp.partition(), tp.offset())
                                            .streamling_with_context(|| format!(
                                                "failed to add partition offset (topic: {}, partition: {}, offset: {:?})",
                                                tp.topic(), tp.partition(), tp.offset()
                                            ))?;
                                    }
                                    let position = &filtered_position;

                                    if !position.elements().is_empty() {
                                        debug!("Committing position: {:?}", position);
                                        // TODO: better rebalancing handling can be implemented
                                        // Currently, every time a group rebalances it's possible to see the following error:
                                        //     Consumer commit error: NoOffset (Local: No offset stored)
                                        // This is OK because the next checkpoint epoch will commit the newly assigned partitions anyway
                                        match consumer.commit(position, CommitMode::Sync) {
                                            Ok(_) => {
                                                // Saving the position to the state backend.
                                                // Per-partition writes are emitted at trace! level so a
                                                // topic with many partitions doesn't dump N debug lines
                                                // on every checkpoint finalize.
                                                let topic_partition_list = TopicPartitionList::from(position.clone());
                                                let partition_count = topic_partition_list.topic_partitions.len();
                                                for topic_partition in topic_partition_list.topic_partitions {
                                                    trace!("Saving position to state backend: {:?}", topic_partition);
                                                    let tp_topic = topic_partition.topic.clone();
                                                    let tp_partition = topic_partition.partition;
                                                    let tp_offset_debug = format!("{:?}", topic_partition.offset);
                                                    state_backend
                                                        .put(topic_partition.state_key(reference_name.clone()).into(), topic_partition.offset)
                                                        .await
                                                        .map_err(|e|
                                                            streamling_err!("saving offset to state backend (topic: {}, partition: {}, offset: {}): {:?}",
                                                                tp_topic, tp_partition, tp_offset_debug, e
                                                            )
                                                        )?;
                                                }
                                                debug!(
                                                    "Saved {} partition offset(s) to state backend for epoch {}",
                                                    partition_count, epoch.0
                                                );

                                                committed_offsets = Some(position.clone());
                                            }
                                            Err(e) => {
                                                error!("Failed to commit position {:?}: {}", position, e);
                                            }
                                        }
                                    } else {
                                        debug!("No valid offsets to commit");
                                    }
                                }

                                consumer_offsets.remove(&epoch);
                            } else {
                                error!("No position found for epoch: {:?}", epoch);
                            }
                        }
                        CheckpointMessage::SourceComplete(name) => {
                            // TODO add verification that the name matches
                            debug!("Received SourceComplete for {}, shutting down", name.clone());
                            checkpoint_messages_buffer.push(CheckpointMessage::SourceComplete(name));

                            break 'outer;
                        }
                        msg => {
                            checkpoint_messages_buffer.push(msg);
                        }
                    }
                }

                match tx.send(Ok(record_batch)).await {
                    Ok(_) => {}
                    Err(e) => {
                        // this could simply mean shutdown
                        warn!("Error sending record batch: {:?}", e);
                        break 'outer;
                    }
                }
                metrics_recorder.record_elapsed_compute(outer_loop_start_at.elapsed(), metric_metadata_id.as_str());
            }

            info!("Shutting down Kafka consumer: unsubscribing and unassigning");
            // Drop this instance's subscription before its receiver, or the
            // coordinator's next broadcast fails for every instance still running.
            unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, checkpoint_subscriber_id);
            consumer.unsubscribe();
            consumer.unassign().expect("Failed to unassign consumer");
            // Always forget after explicit cleanup to avoid redundant drop overhead.
            // SafeKafkaConsumer's Drop impl handles early returns (errors) safely via spawn_blocking.
            consumer.forget();

            Ok(())
        });

        Ok(builder.build())
    }
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }
}

#[derive(Clone)]
pub struct KafkaSourceTableProvider {
    reference_name: String,
    metric_metadata_id: String,
    config: KafkaConfig,
    topic: String,
    start_at: Option<String>, // TODO: use enum
    filter: Option<String>,
    full_schema: SchemaRef,
    payload_schema: SchemaRef,
    decoding: KafkaSourceDecoding,
    should_enrich_op_column: bool,
    record_batch_interval_ms: u64,
    record_batch_size: u32,
    internal_buffer_size: u32,
    include_metadata: bool,
    state_backend: Arc<dyn StateOperatorBackend<TopicPartitionOffset>>,
    session_manager: SessionManager,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    num_records_before_stop: Option<u64>,
    extracted_primary_key: Option<String>,
    parallelism: usize,
}

impl Debug for KafkaSourceTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaSourceTableProvider")
            .field("reference_name", &self.reference_name)
            .field("config", &self.config)
            .field("topic", &self.topic)
            .field("start_at", &self.start_at)
            .field("filter", &self.filter)
            .field("full_schema", &self.full_schema)
            .field("payload_schema", &self.payload_schema)
            .field("should_enrich_op_column", &self.should_enrich_op_column)
            .field("record_batch_interval_ms", &self.record_batch_interval_ms)
            .field("record_batch_size", &self.record_batch_size)
            .field("internal_buffer_size", &self.internal_buffer_size)
            .field("include_metadata", &self.include_metadata)
            .field("num_records_before_stop", &self.num_records_before_stop)
            .finish()
    }
}

fn build_schema_id_overrides_map(
    topic: &str,
    overrides: Vec<SchemaIdOverride>,
) -> Result<HashMap<u32, u32>> {
    let mut map = HashMap::with_capacity(overrides.len());
    for SchemaIdOverride { from, to } in overrides {
        if map.insert(from, to).is_some() {
            streamling_user_bail!(
                "schema_id_overrides for topic '{}' contains duplicate `from` value: {}",
                topic,
                from
            );
        }
    }
    Ok(map)
}

impl KafkaSourceTableProvider {
    pub fn new(
        reference_name: String,
        metric_metadata_id: String,
        config: KafkaConfig,
        topic: String,
        start_at: Option<String>,
        filter: Option<String>,
        record_batch_interval_ms: u64,
        record_batch_size: u32,
        internal_buffer_size: u32,
        include_metadata: bool,
        state_backend: Arc<dyn StateOperatorBackend<TopicPartitionOffset>>,
        session_manager: SessionManager,
        num_records_before_stop: Option<u64>,
        validate_writer_schema_ordering: bool,
        schema_id_overrides: Vec<SchemaIdOverride>,
        skip_schema_resolution_unconditional: bool,
        skip_schema_resolution_for_reader_schema_ids: Vec<u32>,
        format: KafkaFormat,
        json_schema: Option<BTreeMap<String, String>>,
        parallelism: usize,
    ) -> Result<Self> {
        let (payload_schema, decoding, extracted_primary_key) = match format {
            KafkaFormat::Avro => {
                if json_schema.is_some() {
                    return Err(streamling_user_err!(
                        "`schema` is only supported with data_format: json"
                    )
                    .into());
                }

                let (avro_schema, reader_schema_id) =
                    KafkaCommon::fetch_avro_schema(&config, &topic)?;
                let payload_schema = convert_avro_schema_to_arrow(avro_schema.clone());

                let schema_id_overrides =
                    build_schema_id_overrides_map(&topic, schema_id_overrides)?;
                let skip_schema_resolution = skip_schema_resolution_unconditional
                    || skip_schema_resolution_for_reader_schema_ids.contains(&reader_schema_id);

                let extracted_primary_key = Self::extract_primary_key(avro_schema.clone())
                    .map_err(|e| {
                        warn!("Could not extract primary key from Avro schema: {}", e);
                        e
                    })
                    .ok();
                if let Some(ref pk) = extracted_primary_key {
                    debug!("Extracted primary key from Avro schema: {}", pk);
                } else {
                    debug!("No primary key found in Avro schema");
                }

                let versions_client = Arc::new(SubjectVersionsClient::new(
                    config.get_schema_registry_settings().ok_or_else(|| {
                        streamling_user_err!("schema_registry_url is required for Avro format")
                    })?,
                ));

                let decoding = KafkaSourceDecoding::Avro(Box::new(AvroSourceDecoding {
                    avro_schema,
                    reader_schema_id,
                    versions_client,
                    validate_writer_schema_ordering,
                    schema_id_overrides,
                    skip_schema_resolution,
                }));

                (payload_schema, decoding, extracted_primary_key)
            }
            KafkaFormat::Json => {
                let schema_map = json_schema.ok_or_else(|| {
                    streamling_user_err!("`schema` is required when data_format is json")
                })?;
                let payload_schema = arrow_schema_from_type_map(&schema_map)?;
                // JSON payloads carry no embedded primary key; it comes from the source config.
                (payload_schema, KafkaSourceDecoding::Json, None)
            }
        };

        let mut should_enrich_op_column = false;
        let mut enriched_schema_fields = payload_schema.fields.to_vec();
        if !enriched_schema_fields
            .iter()
            .any(|f| f.name() == COLUMN_NAME_OP)
        {
            enriched_schema_fields.push(Arc::new(Field::new(
                COLUMN_NAME_OP,
                DataType::Utf8,
                false,
            )));
            should_enrich_op_column = true;
        }

        // Add metadata columns if requested
        if include_metadata {
            KafkaMetadata::enrich_avro_fields(&mut enriched_schema_fields);
        }

        let full_schema = Arc::new(Schema::new(enriched_schema_fields));

        debug!("Full schema: {:?}", full_schema);

        if let Some(ref filter_expr) = filter {
            Self::validate_filter_against_schema(filter_expr, &full_schema, &session_manager)?;
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config_for_metadata = config.clone();
        let topic_for_metadata = topic.clone();

        Ok(KafkaSourceTableProvider {
            reference_name,
            metric_metadata_id,
            config,
            topic,
            start_at,
            filter,
            full_schema,
            payload_schema,
            decoding,
            should_enrich_op_column,
            record_batch_interval_ms,
            record_batch_size,
            internal_buffer_size,
            include_metadata,
            state_backend,
            session_manager,
            shutdown_tx,
            shutdown_rx,
            num_records_before_stop,
            extracted_primary_key,
            parallelism: Self::effective_parallelism(
                &config_for_metadata,
                &topic_for_metadata,
                parallelism,
            ),
        })
    }

    /// Clamps `parallelism` to the topic's partition count.
    ///
    /// Instances beyond that count get an empty broker assignment, and
    /// `wait_for_assignment` fails the source after its timeout rather than
    /// idling — so an over-provisioned `parallelism` would take the pipeline
    /// down. Clamp and say so instead.
    fn effective_parallelism(config: &KafkaConfig, topic: &str, requested: usize) -> usize {
        let requested = requested.max(1);
        if requested == 1 {
            return 1;
        }
        let partitions = KafkaCommon::build_client(config)
            .create::<rdkafka::consumer::BaseConsumer>()
            .ok()
            .and_then(|client| {
                client
                    .fetch_metadata(Some(topic), Duration::from_secs(60))
                    .ok()
            })
            .and_then(|metadata| metadata.topics().first().map(|t| t.partitions().len()));

        match partitions {
            Some(partitions) if partitions > 0 && partitions < requested => {
                warn!(
                    "source parallelism {} exceeds the {} partition(s) of topic '{}'; \
                     running {} consumer instance(s) instead",
                    requested, partitions, topic, partitions
                );
                partitions
            }
            _ => requested,
        }
    }

    /// Get the payload schema (data columns only, without _gs_op or __kafka_* metadata)
    pub fn payload_schema(&self) -> SchemaRef {
        self.payload_schema.clone()
    }

    /// Get the primary key extracted from the Avro schema, if available
    pub fn get_extracted_primary_key(&self) -> Option<String> {
        self.extracted_primary_key.clone()
    }

    /// Trigger a clean shutdown of all Kafka consumer tasks
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Seed the Kafka state backend with offsets from an external source (e.g., ClickHouse).
    /// This is used during hybrid source handoff to ensure the Kafka consumer starts
    /// from the correct position after the bounded source completes.
    pub async fn seed_offsets(&self, offsets: &HashMap<i32, u32>) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        for (partition, offset) in offsets {
            let state_key = StateKey::from(format!(
                "{}:{}:{}",
                self.reference_name, self.topic, partition
            ));
            let offset_state = TopicPartitionOffset {
                offset: *offset as i64,
                updated_at: now,
            };
            self.state_backend
                .put(state_key, offset_state)
                .await
                .map_err(|e| {
                    streamling_err!("seeding offset for partition {}: {:?}", partition, e)
                })?;
            // Per-partition seed line, kept at debug! since this fires once
            // per hybrid bounded→unbounded transition (not per checkpoint).
            debug!(
                "Seeded Kafka offset for topic {} partition {} to offset {}",
                self.topic, partition, offset
            );
        }
        debug!(
            "Seeded {} Kafka partition offset(s) for topic '{}'",
            offsets.len(),
            self.topic
        );

        Ok(())
    }

    /// Check whether any persisted partition offset exists for this source.
    /// Probes partition 0 as a heuristic — any Kafka topic has at least partition 0.
    /// Returns false on missing state or backend errors (best-effort probe).
    pub async fn has_any_persisted_offset(&self) -> bool {
        let state_key = StateKey::from(format!("{}:{}:0", self.reference_name, self.topic));
        matches!(self.state_backend.get(state_key).await, Ok(Some(_)))
    }

    /// Check whether persisted partition offsets exist for ALL specified partitions.
    /// Returns false on any missing offset or backend error (best-effort).
    pub async fn has_all_persisted_offsets(&self, partitions: &[i32]) -> bool {
        for partition in partitions {
            let state_key = StateKey::from(format!(
                "{}:{}:{}",
                self.reference_name, self.topic, partition
            ));
            if !matches!(self.state_backend.get(state_key).await, Ok(Some(_))) {
                return false;
            }
        }
        true
    }

    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        reference_name: String,
        full_schema: SchemaRef,
        payload_schema: SchemaRef,
        config: KafkaConfig,
        topic: String,
        start_at: Option<String>,
        filter: Option<String>,
        decoding: KafkaSourceDecoding,
        should_enrich_op_column: bool,
        record_batch_interval_ms: u64,
        record_batch_size: u32,
        internal_buffer_size: u32,
        include_metadata: bool,
        state_backend: Arc<dyn StateOperatorBackend<TopicPartitionOffset>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The topology-level source `filter:` is defined against the source's
        // full payload schema. A scan projection pushed down by the engine may
        // prune columns the filter references, so when a filter is present the
        // source is built UNPROJECTED, the filter planned against the full
        // schema, and the projection re-applied on top of the filter.
        let exec_projections = if filter.is_some() { None } else { projections };
        let kafka_source_exec = Arc::new(KafkaSourceExec::new(
            reference_name.clone(),
            exec_projections,
            full_schema,
            payload_schema,
            config,
            topic,
            start_at,
            filter.clone(),
            decoding,
            should_enrich_op_column,
            record_batch_interval_ms,
            record_batch_size,
            internal_buffer_size,
            include_metadata,
            state_backend,
            self.shutdown_rx.clone(),
            self.num_records_before_stop,
            self.metric_metadata_id.clone(),
            self.parallelism,
        ));

        if let Some(filter_expr) = &filter {
            let unprojected_schema = kafka_source_exec.schema();
            let filter_predicate = Self::prepare_filter_expression(
                filter_expr,
                &unprojected_schema,
                &self.session_manager,
            )
            .streamling_with_context(|| format!("invalid filter for source '{reference_name}'"))?;
            // Use StreamingFilterExec to preserve batch metadata (including checkpoint markers)
            let original_filter = FilterExec::try_new(filter_predicate, kafka_source_exec)?;
            let filtered: Arc<dyn ExecutionPlan> =
                Arc::new(StreamingFilterExec::from_original(original_filter)?);
            match projections {
                Some(indices) => {
                    // Re-apply the pushed-down projection above the filter so the
                    // scan still returns the projected schema. StreamingProjectionExec
                    // preserves batch metadata (checkpoint markers), like the filter.
                    let exprs = indices
                        .iter()
                        .map(|&i| {
                            let field = unprojected_schema.field(i);
                            Ok((
                                physical_col(field.name(), &unprojected_schema)?,
                                field.name().clone(),
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let projection = ProjectionExec::try_new(exprs, filtered)?;
                    Ok(Arc::new(StreamingProjectionExec::from_original(
                        projection,
                    )?))
                }
                None => Ok(filtered),
            }
        } else {
            Ok(kafka_source_exec)
        }
    }

    fn validate_filter_against_schema(
        filter_expr: &str,
        schema: &SchemaRef,
        session_manager: &SessionManager,
    ) -> Result<()> {
        let session_state = session_manager.session_state();
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;

        let logical_expr = session_state.create_logical_expr(filter_expr, &df_schema)?;
        Self::validate_filter_columns(&logical_expr, schema)
    }

    fn prepare_filter_expression(
        filter_expr: &str,
        schema: &SchemaRef,
        session_manager: &SessionManager,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let session_state = session_manager.session_state();
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;

        let logical_expr = session_state.create_logical_expr(filter_expr, &df_schema)?;
        Self::validate_filter_columns(&logical_expr, schema)?;

        session_state.create_physical_expr(logical_expr, &df_schema)
    }

    fn validate_filter_columns(expr: &Expr, schema: &SchemaRef) -> Result<()> {
        expr.apply(|e| {
            if let Expr::Column(col) = e {
                let field_exists = schema.fields().iter().any(|f| f.name() == &col.name);

                if !field_exists {
                    streamling_user_bail!(
                        "column '{}' in the source filter does not exist in the source schema",
                        col.name
                    );
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(())
    }

    /// Extracts primary key information from the doc string (typically populated by the data producers).
    /// Expecting the following JSON object format:
    /// {
    ///   "primaryKeys": ["column1", "column2"]
    /// }
    fn extract_primary_key(avro_schema: AvroSchema) -> streamling_core::error::Result<String> {
        let record_schema = match avro_schema {
            AvroSchema::Union(union) => {
                // Look for record in union variants
                let mut record_opt = None;
                for variant in union.variants() {
                    if let AvroSchema::Record(record) = variant {
                        record_opt = Some(record);
                        break;
                    }
                }
                record_opt
                    .ok_or_else(|| {
                        streamling_user_err!(
                            "expected Avro record schema in union for extracting primary keys"
                        )
                    })?
                    .clone()
            }
            AvroSchema::Record(record) => record,
            _ => {
                return Err(streamling_user_err!(
                    "expected Avro record schema for extracting primary keys"
                ));
            }
        };

        // Either expecting top-level "doc" or "connect.doc" attribute to be present
        let doc = record_schema
            .doc
            .or_else(|| {
                record_schema
                    .attributes
                    .get("connect.doc")
                    .and_then(|attr| attr.as_str().map(|s| s.to_string()))
            })
            .ok_or_else(|| {
                streamling_user_err!(
                    "doc string not found in Avro schema for extracting primary keys"
                )
            })?;

        let parsed: Value = serde_json::from_str(&doc)
            .streamling_context("failed to parse doc string for primary keys")?;
        if let Some(primary_keys) = parsed.get("primaryKeys")
            && let Some(keys_array) = primary_keys.as_array()
        {
            let keys: Vec<String> = keys_array
                .iter()
                .filter_map(|k| k.as_str().map(|s| s.to_string()))
                .collect();
            return Ok(keys.join(","));
        }

        Err(streamling_user_err!("primary keys not found in doc string"))
    }
}

#[async_trait]
impl TableProvider for KafkaSourceTableProvider {
    fn schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.create_physical_plan(
            projection,
            self.reference_name.clone(),
            self.full_schema.clone(),
            self.payload_schema.clone(),
            self.config.clone(),
            self.topic.clone(),
            self.start_at.clone(),
            self.filter.clone(),
            self.decoding.clone(),
            self.should_enrich_op_column,
            self.record_batch_interval_ms,
            self.record_batch_size,
            self.internal_buffer_size,
            self.include_metadata,
            self.state_backend.clone(),
        )
        .await
    }
}

pub struct KafkaSink {
    _reference_name: String,
    schema: SchemaRef,
    config: KafkaConfig,
    topic: String,
    topic_partitions: i32,
    format: KafkaFormat,
    subject_name_strategy: SubjectNameStrategy,
    num_records_before_stop: Option<u64>, // for integration tests only!
    /// Global `num_records_before_stop` progress across the concurrent
    /// per-partition `write_all` streams (`ParallelSinkExec`).
    rows_received: AtomicU64,
    source_name: String,
    metric_metadata_id: String,
    producer: OnceCell<KafkaThreadedProducer>,
    primary_key: Option<String>,
    /// Maximum number of messages to batch before sending (maps to Kafka's batch.num.messages)
    batch_size: Option<u32>,
    /// Time interval to wait for batching messages in ms (maps to Kafka's linger.ms)
    batch_flush_interval_ms: Option<u64>,
    /// Maximum Kafka protocol request message size in bytes (maps to message.max.bytes)
    message_max_bytes: Option<u32>,
    /// Producer compression codec (maps to librdkafka's compression.type)
    compression: KafkaCompression,
}

impl KafkaSink {
    pub fn new(
        reference_name: String,
        schema: SchemaRef,
        config: KafkaConfig,
        topic: String,
        topic_partitions: Option<i32>,
        format: KafkaFormat,
        num_records_before_stop: Option<u64>,
        source_name: String,
        primary_key: Option<String>,
        batch_size: Option<u32>,
        batch_flush_interval_ms: Option<u64>,
        message_max_bytes: Option<u32>,
        compression: KafkaCompression,
    ) -> Self {
        let subject_name_strategy = SubjectNameStrategy::TopicNameStrategy(topic.to_owned(), false);
        let topic_partitions = topic_partitions.unwrap_or(DEFAULT_NUM_PARTITIONS);

        Self {
            _reference_name: reference_name.clone(),
            schema,
            config,
            topic,
            topic_partitions,
            format,
            subject_name_strategy,
            num_records_before_stop,
            rows_received: AtomicU64::new(0),
            source_name,
            metric_metadata_id: reference_name,
            producer: OnceCell::new(),
            primary_key,
            batch_size,
            batch_flush_interval_ms,
            message_max_bytes,
            compression,
        }
    }

    /// Flush all buffered messages to Kafka, blocking until complete.
    /// Runs via `block_in_place` to avoid stalling other Tokio tasks.
    fn flush_producer(producer: &KafkaThreadedProducer, topic: &str) {
        let flush_start = StdInstant::now();
        loop {
            match producer.flush(Timeout::After(StdDuration::from_secs(10))) {
                Ok(()) => {
                    debug!(
                        "Kafka producer flush complete for topic: {} (took {:?})",
                        topic,
                        flush_start.elapsed()
                    );
                    break;
                }
                Err(e) => {
                    warn!(
                        "Kafka producer flush still in progress for topic: {} ({:?} elapsed, outq_len: {}): {}",
                        topic,
                        flush_start.elapsed(),
                        producer.in_flight_count(),
                        e
                    );
                }
            }
        }
    }

    /// Assemble the librdkafka producer configuration.
    fn build_producer_config(
        config: &KafkaConfig,
        batch_size: Option<u32>,
        batch_flush_interval_ms: Option<u64>,
        message_max_bytes: Option<u32>,
        compression: KafkaCompression,
    ) -> ClientConfig {
        let mut builder = KafkaCommon::build_client(config);

        builder
            .set("message.timeout.ms", "600000")
            .set("acks", "all");

        let kafka_config_optimizer = KafkaConfigOptimizer::new(config);
        for (key, value) in kafka_config_optimizer.optimized_producer_config() {
            builder.set(key, value);
        }

        if let Some(batch_num_messages) = batch_size {
            builder.set("batch.num.messages", batch_num_messages.to_string());
        }
        if let Some(linger_ms) = batch_flush_interval_ms {
            builder.set("linger.ms", linger_ms.to_string());
        }
        if let Some(max_bytes) = message_max_bytes {
            builder.set("message.max.bytes", max_bytes.to_string());
        }
        // Applied last so the per-sink setting wins over the optimizer default.
        builder.set("compression.type", compression.as_str());

        builder
    }

    /// One producer per sink, shared by every concurrent write stream.
    ///
    /// The sink used to fan out to N producers and route rows between them by
    /// `hash(key) % N` — a hash exchange hand-rolled inside the sink. The
    /// `parallelism` knob now drives a real `StreamingRepartitionExec` upstream,
    /// which gives each key its own write stream, so the internal routing is
    /// gone. `ThreadedProducer` is `Sync` and batches internally, so the streams
    /// share it.
    fn create_producer(&self) -> streamling_core::error::Result<KafkaThreadedProducer> {
        info!(
            "Creating Kafka producer for topic: {} (message.timeout.ms=600000, acks=all, batch_size={:?}, batch_flush_interval_ms={:?}, message_max_bytes={:?}, compression={})",
            self.topic,
            self.batch_size,
            self.batch_flush_interval_ms,
            self.message_max_bytes,
            self.compression.as_str(),
        );

        Self::build_producer_config(
            &self.config,
            self.batch_size,
            self.batch_flush_interval_ms,
            self.message_max_bytes,
            self.compression,
        )
        .create_with_context(KafkaProducerContext::new())
        .streamling_context("failed to create Kafka producer")
    }

    async fn send_batch_to_kafka_as_avro(
        &self,
        batch: &RecordBatch,
        producer: &KafkaThreadedProducer,
        converter: &FromArrowToAvroConverter,
        encoder: &AvroEncoder<'_>,
        metrics_recorder: Arc<MetricsRecorder>,
    ) -> streamling_core::error::Result<()> {
        let num_rows = batch.num_rows();

        debug!(
            "Sending batch with {} rows to Kafka topic: {}",
            num_rows, self.topic,
        );

        let serialized_values = converter.convert_from_batch(batch)?;

        let row_kinds = RowKind::extract_row_kinds_from_batch(batch);

        let keys = if let Some(ref pk) = self.primary_key {
            Some(self.extract_keys_from_batch(batch, pk)?)
        } else {
            None
        };

        let mut encoded_payloads = Vec::new();
        let mut total_bytes: usize = 0;
        for value in serialized_values.into_iter() {
            let value = Union(1, Box::new(value));
            let encode_start_at = Instant::now();
            let encoded_payload = encoder
                .encode_value(value, &self.subject_name_strategy)
                .await
                .streamling_context("failed to encode Avro message")?;
            metrics_recorder.record_time(
                "kafka_producer.msg_encode_latency",
                encode_start_at.elapsed(),
                self.metric_metadata_id.as_str(),
            );
            let bytes = encoded_payload.to_bytes().to_vec();
            total_bytes += bytes.len();
            encoded_payloads.push(bytes);
        }
        let avg_msg_bytes = if num_rows > 0 {
            total_bytes / num_rows
        } else {
            0
        };
        debug!(
            "Encoded {} rows for topic: {} ({} bytes total, {} bytes/msg avg)",
            num_rows, self.topic, total_bytes, avg_msg_bytes,
        );

        self.send_payloads_to_producer(encoded_payloads, &row_kinds, keys.as_ref(), producer)
    }

    /// Extract keys from batch based on primary key column names
    fn extract_keys_from_batch(
        &self,
        batch: &RecordBatch,
        primary_key: &str,
    ) -> streamling_core::error::Result<Vec<String>> {
        use datafusion::common::ScalarValue;

        let key_columns: Vec<&str> = primary_key.split(',').map(|s| s.trim()).collect();
        let num_rows = batch.num_rows();
        let mut keys = Vec::with_capacity(num_rows);

        let mut key_arrays = Vec::new();
        for col_name in &key_columns {
            let column = batch.column_by_name(col_name).ok_or_else(|| {
                streamling_user_err!("primary key column '{}' not found in batch", col_name)
            })?;
            key_arrays.push(column);
        }

        for row_idx in 0..num_rows {
            let mut key_parts = Vec::new();
            for column in &key_arrays {
                let scalar_value = ScalarValue::try_from_array(column, row_idx)?;
                key_parts.push(scalar_value.to_string());
            }
            keys.push(key_parts.join(":"));
        }

        Ok(keys)
    }

    fn send_batch_to_kafka_as_json(
        &self,
        batch: &RecordBatch,
        producer: &KafkaThreadedProducer,
        converter: &FromArrowToJsonConverter,
    ) -> streamling_core::error::Result<()> {
        let num_rows = batch.num_rows();

        debug!(
            "Sending batch with {} rows as JSON to Kafka topic: {}",
            num_rows, self.topic,
        );

        let serialized_values = converter.convert_from_batch(batch)?;

        let row_kinds = RowKind::extract_row_kinds_from_batch(batch);

        let keys = if let Some(ref pk) = self.primary_key {
            Some(self.extract_keys_from_batch(batch, pk)?)
        } else {
            None
        };

        self.send_payloads_to_producer(serialized_values, &row_kinds, keys.as_ref(), producer)
    }

    /// Send pre-serialized payloads to Kafka producers with key-based partitioning,
    /// operation headers, and backpressure handling.
    /// Uses `block_in_place` since rdkafka's `send` is a blocking call.
    fn send_payloads_to_producer(
        &self,
        payloads: Vec<Vec<u8>>,
        row_kinds: &[RowKind],
        keys: Option<&Vec<String>>,
        producer: &KafkaThreadedProducer,
    ) -> streamling_core::error::Result<()> {
        let topic = &self.topic;
        tokio::task::block_in_place(|| {
            for (i, encoded_bytes) in payloads.into_iter().enumerate() {
                let key = keys.map(|k| &k[i]);
                producer.context().check_error()?;

                let op = row_kinds[i].to_dbz_op();
                let headers = OwnedHeaders::new().insert(Header {
                    key: KAFKA_HEADER_OPERATION,
                    value: Some(op.as_str()),
                });

                let mut record = BaseRecord::to(topic)
                    .payload(&encoded_bytes)
                    .headers(headers);

                if let Some(k) = key {
                    record = record.key(k.as_str());
                }

                loop {
                    match producer.send(record) {
                        Ok(()) => break,
                        Err((
                            rdkafka::error::KafkaError::MessageProduction(
                                rdkafka::types::RDKafkaErrorCode::QueueFull,
                            ),
                            returned_record,
                        )) => {
                            debug!(
                                "rdkafka queue full for topic: {}, polling and retrying",
                                topic
                            );
                            producer.poll(StdDuration::from_millis(100));
                            record = returned_record;
                        }
                        Err((e, _)) => {
                            return Err(streamling_err!(
                                "Kafka send failed for topic {}: {}",
                                topic,
                                e
                            ));
                        }
                    }
                }
            }
            Ok(())
        })
    }

    fn create_avro_encoder(&self) -> AvroEncoder<'_> {
        AvroEncoder::new(
            self.config
                .get_schema_registry_settings()
                .expect("schema_registry_url is required for Avro format"),
        )
    }

    fn create_avro_converter(&self) -> FromArrowToAvroConverter {
        FromArrowToAvroConverter::new(self.schema.clone(), self.topic.clone())
    }

    fn create_json_converter(&self) -> FromArrowToJsonConverter {
        FromArrowToJsonConverter::new()
    }

    async fn ensure_topic_and_schema_exist(&self) -> streamling_core::error::Result<()> {
        let builder = KafkaCommon::build_client(&self.config);

        let admin_client: AdminClient<_> = builder
            .create()
            .streamling_context("failed to create Kafka admin client")?;

        let topic_exists = tokio::task::block_in_place(|| {
            match admin_client
                .inner()
                .fetch_metadata(Some(&self.topic), Duration::from_secs(10))
            {
                Ok(metadata) => metadata
                    .topics()
                    .first()
                    .map(|t| !t.partitions().is_empty())
                    .unwrap_or(false),
                Err(e) => {
                    warn!(
                        "Failed to fetch metadata for topic {}: {}, will attempt creation",
                        self.topic, e
                    );
                    false
                }
            }
        });

        if topic_exists {
            debug!("Topic {} already exists, skipping creation", self.topic);
        } else {
            let mut new_topic = NewTopic::new(
                self.topic.as_str(),
                self.topic_partitions,
                TopicReplication::Fixed(-1),
            )
            .set("retention.ms", "-1");

            if self.primary_key.is_some() {
                new_topic = new_topic.set("cleanup.policy", "compact");
            }

            let admin_opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

            match admin_client.create_topics(&[new_topic], &admin_opts).await {
                Ok(results) => {
                    for result in results {
                        match result {
                            Ok(topic_name) => {
                                info!(
                                    "Successfully created topic {} with {} partitions",
                                    topic_name, self.topic_partitions
                                );
                            }
                            Err((topic_name, error)) => {
                                if error.to_string().contains("already exists") {
                                    debug!("Topic {} already exists", topic_name);
                                } else {
                                    return Err(streamling_err!(
                                        "creating topic '{}': {}",
                                        topic_name,
                                        error
                                    ));
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(streamling_err!("creating topic '{}': {}", self.topic, e));
                }
            }
        }

        // Ensure the schema exists in the schema registry
        // Only need to do it after the initial topic creation
        if self.format == KafkaFormat::Avro {
            let schema_registry_settings =
                self.config.get_schema_registry_settings().ok_or_else(|| {
                    streamling_user_err!("schema_registry_url is required for Avro format")
                })?;
            let existing_avro_schema = KafkaCommon::fetch_avro_schema(&self.config, &self.topic);

            if existing_avro_schema.is_ok() {
                debug!(
                    "Schema already exists in schema registry for topic: {}",
                    self.topic
                );
                return Ok(());
            }

            let avro_schema = to_avro(&self.topic, &self.schema.fields);
            let avro_schema =
                post_process_avro_schema_for_writing(avro_schema, self.primary_key.clone());

            let subject = self
                .subject_name_strategy
                .get_subject()
                .streamling_with_context(|| {
                    format!("failed to get subject name for topic '{}'", self.topic)
                })?;
            let schema = SuppliedSchema {
                name: None,
                schema_type: SchemaType::Avro,
                schema: serde_json::to_string(&avro_schema).streamling_with_context(|| {
                    format!("failed to serialize Avro schema for topic '{}'", self.topic)
                })?,
                references: vec![],
            };

            let registered_schema =
                post_schema(&schema_registry_settings, subject.clone(), schema).await;

            let registered_schema = registered_schema.streamling_with_context(|| {
                format!(
                    "failed to register schema with Schema Registry (topic: {}, subject: {})",
                    self.topic, subject
                )
            })?;

            debug!("Registered schema with id: {}", registered_schema.id);
        }

        Ok(())
    }
}

#[async_trait]
impl DataSink for KafkaSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut row_count = 0;

        self.ensure_topic_and_schema_exist().await?;

        let producer: &KafkaThreadedProducer =
            self.producer.get_or_try_init(|| self.create_producer())?;
        let metrics_recorder = get_metrics_recorder().clone();

        let sink_converter = match self.format {
            KafkaFormat::Avro => KafkaSinkConverter::Avro {
                converter: self.create_avro_converter(),
                encoder: self.create_avro_encoder(),
            },
            KafkaFormat::Json => KafkaSinkConverter::Json {
                converter: self.create_json_converter(),
            },
        };

        while let Some(batch) = data.next().await.transpose()? {
            let arrival_time_ms = now_ms();

            if batch.num_rows() != 0 {
                let start_at = Instant::now();
                match &sink_converter {
                    KafkaSinkConverter::Avro { converter, encoder } => {
                        self.send_batch_to_kafka_as_avro(
                            &batch,
                            producer,
                            converter,
                            encoder,
                            metrics_recorder.clone(),
                        )
                        .await?;
                    }
                    KafkaSinkConverter::Json { converter } => {
                        self.send_batch_to_kafka_as_json(&batch, producer, converter)?;
                    }
                }
                metrics_recorder.record_time(
                    "kafka_producer.batch_send_latency",
                    start_at.elapsed(),
                    self.metric_metadata_id.as_str(),
                );
                metrics_recorder
                    .record_elapsed_compute(start_at.elapsed(), self.metric_metadata_id.as_str());
                metrics_recorder.record_output_rows_count(
                    batch.num_rows() as u64,
                    self.metric_metadata_id.as_str(),
                )
            }

            row_count += batch.num_rows();
            let total_received = self
                .rows_received
                .fetch_add(batch.num_rows() as u64, Ordering::SeqCst)
                + batch.num_rows() as u64;

            let checkpoint_messages = extract_checkpoint_messages(batch.schema().metadata());
            trace!(
                "Extracted checkpoint messages from batch metadata: {:?}",
                checkpoint_messages
            );

            for message in checkpoint_messages {
                if let CheckpointMessage::Marker {
                    epoch,
                    created_at_ms,
                } = message
                {
                    let arrival_latency_ms = arrival_time_ms.saturating_sub(created_at_ms);
                    metrics_recorder.record_time(
                        "checkpoint_marker_arrival",
                        StdDuration::from_millis(arrival_latency_ms),
                        self.metric_metadata_id.as_str(),
                    );

                    let ack_start = StdInstant::now();

                    debug!(
                        "Received marker for epoch {}, flushing Kafka producer for topic: {}",
                        epoch.0, self.topic,
                    );
                    tokio::task::block_in_place(|| {
                        Self::flush_producer(producer, &self.topic);
                    });

                    producer.context().check_error().map_err(|e| {
                        datafusion::error::DataFusionError::from(
                            streamling_core::streamling_err!("Kafka producer error: {}", e)
                                .mark_retriable(),
                        )
                    })?;

                    let sink_id = get_reference_name_from_metric_key(&self.metric_metadata_id);
                    if report_marker_at_sink(&sink_id, epoch.clone()) {
                        send_checkpoint_ack(epoch, &sink_id);
                    }
                    metrics_recorder.record_time(
                        "checkpoint_sink_flush",
                        ack_start.elapsed(),
                        self.metric_metadata_id.as_str(),
                    );
                }
            }

            // Compare against the global received count so the stop threshold
            // stays global across the concurrent per-partition streams.
            if let Some(num_records_before_stop) = self.num_records_before_stop
                && total_received >= num_records_before_stop
            {
                let _ = send(
                    CHECKPOINT_COORDINATOR_CHANNEL,
                    CheckpointMessage::SourceComplete(self.source_name.clone()),
                );
                break;
            }
        }

        tokio::task::block_in_place(|| {
            Self::flush_producer(producer, &self.topic);
        });
        {
            producer.context().check_error().map_err(|e| {
                datafusion::error::DataFusionError::from(
                    streamling_core::streamling_err!("Kafka producer error: {}", e)
                        .mark_retriable(),
                )
            })?;
        }

        Ok(row_count as u64)
    }
}

impl Debug for KafkaSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaSink").finish()
    }
}

impl DisplayAs for KafkaSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "KafkaSink")
            }
        }
    }
}

#[derive(Debug)]
pub struct KafkaSinkTableProvider {
    metric_metadata_id: String,
    schema: SchemaRef,
    config: KafkaConfig,
    topic: String,
    topic_partitions: Option<i32>,
    format: KafkaFormat,
    num_records_before_stop: Option<u64>,
    source_name: String,
    primary_key: Option<String>,
    /// Maximum number of messages to batch before sending (maps to Kafka's batch.num.messages)
    batch_size: Option<u32>,
    /// Time interval to wait for batching messages in ms (maps to Kafka's linger.ms)
    batch_flush_interval_ms: Option<u64>,
    /// Maximum Kafka protocol request message size in bytes (maps to message.max.bytes)
    message_max_bytes: Option<u32>,
    /// Producer compression codec (maps to librdkafka's compression.type)
    compression: KafkaCompression,
    telemetry: Option<Telemetry>,
}

impl KafkaSinkTableProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metric_metadata_id: String,
        schema: SchemaRef,
        config: KafkaConfig,
        topic: String,
        topic_partitions: Option<i32>,
        format: KafkaFormat,
        num_records_before_stop: Option<u64>,
        source_name: String,
        primary_key: Option<String>,
        batch_size: Option<u32>,
        batch_flush_interval_ms: Option<u64>,
        message_max_bytes: Option<u32>,
        compression: KafkaCompression,
        telemetry: Option<Telemetry>,
    ) -> Self {
        Self {
            metric_metadata_id,
            schema,
            config,
            topic,
            topic_partitions,
            format,
            num_records_before_stop,
            source_name,
            primary_key,
            batch_size,
            batch_flush_interval_ms,
            message_max_bytes,
            compression,
            telemetry,
        }
    }
}

#[async_trait]
impl TableProvider for KafkaSinkTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Reading is not implemented for KafkaSinkTableProvider")
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let kafka_sink = Arc::new(KafkaSink::new(
            self.metric_metadata_id.clone(),
            self.schema.clone(),
            self.config.clone(),
            self.topic.clone(),
            self.topic_partitions,
            self.format.clone(),
            self.num_records_before_stop,
            self.source_name.clone(),
            self.primary_key.clone(),
            self.batch_size,
            self.batch_flush_interval_ms,
            self.message_max_bytes,
            self.compression,
        ));
        let metric_metadata_id = self.metric_metadata_id.clone();
        let wrapper_data_sink = Arc::new(WrappingDataSink::new(
            kafka_sink,
            metric_metadata_id.clone(),
            self.primary_key.clone(),
            self.telemetry.as_ref(),
        ));
        Ok(Arc::new(ParallelSinkExec::new(
            input,
            wrapper_data_sink,
            get_reference_name_from_metric_key(&metric_metadata_id),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A topology-level source `filter:` is defined against the source's full
    /// payload schema. When the engine pushes a scan projection that prunes a
    /// column the filter references, the filter must still plan against the
    /// full schema (and the projection applied afterwards) — otherwise a
    /// perfectly valid filter becomes a fatal "invalid filter for source"
    /// error at startup.
    #[tokio::test]
    async fn source_filter_survives_scan_projection_pushdown() {
        use datafusion::catalog::TableProvider;
        use streamling_core::dynamic_table::DynamicTableRegistry;
        use streamling_state::StateOperatorBackendFactory;
        use streamling_state::in_memory::InMemoryStateOperatorBackendFactory;

        let session_manager =
            SessionManager::new(8192, 10, DynamicTableRegistry::new(), 1).unwrap();
        let state_backend = InMemoryStateOperatorBackendFactory::new()
            .unwrap()
            .create::<TopicPartitionOffset>("test-kafka-filter");

        let mut json_schema = BTreeMap::new();
        json_schema.insert("a".to_string(), "string".to_string());
        json_schema.insert("b".to_string(), "int64".to_string());
        json_schema.insert("c".to_string(), "string".to_string());

        let provider = KafkaSourceTableProvider::new(
            "src".to_string(),
            "src-metrics".to_string(),
            test_kafka_config(),
            "test-topic".to_string(),
            None,
            Some("c = 'x'".to_string()),
            1000,
            10,
            10,
            false,
            state_backend,
            session_manager.clone(),
            None,
            false,
            vec![],
            true,
            vec![],
            KafkaFormat::Json,
            Some(json_schema),
            1,
        )
        .unwrap();

        // The engine pushes a projection that keeps only column `a` (index 0)
        // — pruning `c`, which the source filter references.
        let state = session_manager.session_state();
        let plan = provider
            .scan(&state, Some(&vec![0]), &[], None)
            .await
            .expect(
                "source filter must plan against the full payload schema, \
                 not the projected scan schema",
            );

        // The scan contract still holds: output schema is the projected one.
        let schema = plan.schema();
        let fields: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(fields, vec!["a"], "scan must project to the pushed columns");
    }

    #[test]
    fn json_payload_to_string_rejects_empty_payload() {
        let err = json_payload_to_string(b"", "orders", 3, 42).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("empty payload"), "{msg}");
        assert!(msg.contains("orders"), "should carry the topic: {msg}");
        assert!(msg.contains("42"), "should carry the offset: {msg}");
    }

    #[test]
    fn json_payload_to_string_rejects_invalid_utf8() {
        let err = json_payload_to_string(&[0xff, 0xfe], "orders", 0, 0).unwrap_err();
        assert!(err.to_string().contains("not valid UTF-8"));
    }

    #[test]
    fn json_payload_to_string_passes_valid_payload() {
        let json = json_payload_to_string(br#"{"id":1}"#, "orders", 0, 0).unwrap();
        assert_eq!(json, r#"{"id":1}"#);
    }

    fn test_kafka_config() -> KafkaConfig {
        KafkaConfig {
            brokers: "localhost:9092".to_string(),
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
        }
    }

    #[test]
    fn build_producer_config_sets_requested_compression() {
        for compression in [
            KafkaCompression::None,
            KafkaCompression::Gzip,
            KafkaCompression::Lz4,
        ] {
            let cfg = KafkaSink::build_producer_config(
                &test_kafka_config(),
                None,
                None,
                None,
                compression,
            );
            assert_eq!(
                cfg.get("compression.type"),
                Some(compression.as_str()),
                "compression {compression:?} should reach the producer config"
            );
        }
    }

    #[test]
    fn build_producer_config_compression_overrides_optimizer_default() {
        // The optimizer seeds compression.type=lz4; an explicit gzip is applied
        // afterwards and must win.
        let cfg = KafkaSink::build_producer_config(
            &test_kafka_config(),
            None,
            None,
            None,
            KafkaCompression::Gzip,
        );
        assert_eq!(cfg.get("compression.type"), Some("gzip"));
    }

    #[test]
    fn build_producer_config_threads_optional_tunables() {
        let cfg = KafkaSink::build_producer_config(
            &test_kafka_config(),
            Some(5000),
            Some(250),
            Some(20_000_000),
            KafkaCompression::Lz4,
        );
        assert_eq!(cfg.get("batch.num.messages"), Some("5000"));
        assert_eq!(cfg.get("linger.ms"), Some("250"));
        assert_eq!(cfg.get("message.max.bytes"), Some("20000000"));
        // Invariants that must always hold for the sink producer.
        assert_eq!(cfg.get("acks"), Some("all"));
        assert_eq!(cfg.get("message.timeout.ms"), Some("600000"));
    }

    #[test]
    fn build_schema_id_overrides_map_empty_returns_empty_map() {
        let map = build_schema_id_overrides_map("topic", vec![]).unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn build_schema_id_overrides_map_collects_unique_entries() {
        let map = build_schema_id_overrides_map(
            "topic",
            vec![
                SchemaIdOverride { from: 1, to: 10 },
                SchemaIdOverride { from: 2, to: 20 },
            ],
        )
        .unwrap();
        assert_eq!(map.get(&1), Some(&10));
        assert_eq!(map.get(&2), Some(&20));
    }

    #[test]
    fn build_schema_id_overrides_map_rejects_duplicate_from() {
        let err = build_schema_id_overrides_map(
            "my_topic",
            vec![
                SchemaIdOverride { from: 1, to: 10 },
                SchemaIdOverride { from: 1, to: 20 },
            ],
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("duplicate"),
            "error should mention duplicate: {msg}"
        );
        assert!(
            msg.contains("my_topic"),
            "error should mention topic: {msg}"
        );
        assert!(
            msg.contains('1'),
            "error should mention the duplicate id: {msg}"
        );
    }

    #[test]
    fn test_stall_watchdog_requires_previous_data() {
        assert!(!is_stalled_with_lag(
            false,
            Duration::from_secs(600),
            Duration::from_secs(60),
            Some(100)
        ));
    }

    #[test]
    fn test_stall_watchdog_ignores_non_positive_lag() {
        assert!(!is_stalled_with_lag(
            true,
            Duration::from_secs(600),
            Duration::from_secs(60),
            Some(0)
        ));
        assert!(!is_stalled_with_lag(
            true,
            Duration::from_secs(600),
            Duration::from_secs(60),
            None
        ));
    }

    #[test]
    fn test_stall_watchdog_fires_after_timeout_with_positive_lag() {
        assert!(is_stalled_with_lag(
            true,
            Duration::from_secs(301),
            Duration::from_secs(300),
            Some(42)
        ));
    }

    #[test]
    fn test_unavailable_lag_watchdog_only_fires_after_timeout() {
        assert!(!is_stalled_lag_unavailable(
            true,
            Duration::from_secs(301),
            Some(Duration::from_secs(300)),
            Duration::from_secs(300)
        ));
        assert!(is_stalled_lag_unavailable(
            true,
            Duration::from_secs(301),
            Some(Duration::from_secs(301)),
            Duration::from_secs(300)
        ));
        assert!(!is_stalled_lag_unavailable(
            true,
            Duration::from_secs(301),
            None,
            Duration::from_secs(300)
        ));
    }

    #[test]
    fn test_watchdog_reads_lag_from_channel() {
        let (tx, rx) = watch::channel(None);
        let mut watchdog = KafkaSourceWatchdogState::new(Duration::from_secs(60), rx);

        watchdog.on_record();

        // Before any lag is published, watchdog sees None
        watchdog.refresh_lag();
        assert_eq!(watchdog.max_observed_lag, None);
        assert!(watchdog.lag_unavailable_since.is_some());

        // Lag task publishes positive lag
        tx.send(Some(500)).unwrap();
        watchdog.refresh_lag();
        assert_eq!(watchdog.max_observed_lag, Some(500));
        assert!(watchdog.lag_unavailable_since.is_none());

        // Lag task publishes caught-up
        tx.send(Some(0)).unwrap();
        watchdog.refresh_lag();
        assert_eq!(watchdog.max_observed_lag, Some(0));
        assert!(watchdog.lag_unavailable_since.is_none());

        // Lag task publishes None (watermarks temporarily unavailable)
        tx.send(None).unwrap();
        watchdog.refresh_lag();
        assert_eq!(watchdog.max_observed_lag, None);
        assert!(watchdog.lag_unavailable_since.is_some());
    }

    #[test]
    fn test_watchdog_skips_refresh_before_data() {
        let (tx, rx) = watch::channel(None);
        let mut watchdog = KafkaSourceWatchdogState::new(Duration::from_secs(60), rx);

        tx.send(Some(100)).unwrap();
        watchdog.refresh_lag();

        // Should not read from channel before first record
        assert_eq!(watchdog.max_observed_lag, None);
    }

    mod filter_validation {
        use super::*;
        use arrow_schema::{DataType, Field, Schema};
        use streamling_core::dynamic_table::DynamicTableRegistry;
        use streamling_core::session::SessionManager;

        fn test_session_manager() -> SessionManager {
            SessionManager::new(100, 10, DynamicTableRegistry::new(), 1)
                .expect("test session manager initialisation failed")
        }

        fn test_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("block", DataType::Int64, false),
                Field::new("data", DataType::Utf8, true),
            ]))
        }

        #[tokio::test]
        async fn valid_numeric_filter() {
            let schema = test_schema();
            let session_manager = test_session_manager();
            let result = KafkaSourceTableProvider::validate_filter_against_schema(
                "block > 60",
                &schema,
                &session_manager,
            );
            assert!(
                result.is_ok(),
                "Expected valid filter to pass: {:?}",
                result
            );
        }

        #[tokio::test]
        async fn valid_string_filter() {
            let schema = test_schema();
            let session_manager = test_session_manager();
            let result = KafkaSourceTableProvider::validate_filter_against_schema(
                "data = 'target'",
                &schema,
                &session_manager,
            );
            assert!(
                result.is_ok(),
                "Expected valid filter to pass: {:?}",
                result
            );
        }

        #[tokio::test]
        async fn valid_compound_filter() {
            let schema = test_schema();
            let session_manager = test_session_manager();
            let result = KafkaSourceTableProvider::validate_filter_against_schema(
                "block > 10 AND id < 100",
                &schema,
                &session_manager,
            );
            assert!(
                result.is_ok(),
                "Expected valid filter to pass: {:?}",
                result
            );
        }

        #[tokio::test]
        async fn nonexistent_column_fails() {
            let schema = test_schema();
            let session_manager = test_session_manager();
            let result = KafkaSourceTableProvider::validate_filter_against_schema(
                "nonexistent > 5",
                &schema,
                &session_manager,
            );
            assert!(result.is_err(), "Expected error for nonexistent column");
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("nonexistent"),
                "Error should mention the bad column name, got: {}",
                err_msg
            );
        }

        #[tokio::test]
        async fn syntax_error_fails() {
            let schema = test_schema();
            let session_manager = test_session_manager();
            let result = KafkaSourceTableProvider::validate_filter_against_schema(
                "block >>>",
                &schema,
                &session_manager,
            );
            assert!(result.is_err(), "Expected error for syntax error");
        }

        #[tokio::test]
        async fn validate_filter_columns_with_existing_columns() {
            let schema = test_schema();
            let session_manager = test_session_manager();
            let session_state = session_manager.session_state();
            let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
            let expr = session_state
                .create_logical_expr("block > 60", &df_schema)
                .unwrap();
            let result = KafkaSourceTableProvider::validate_filter_columns(&expr, &schema);
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn validate_filter_columns_with_missing_column() {
            let full_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("block", DataType::Int64, false),
                Field::new("data", DataType::Utf8, true),
                Field::new("extra", DataType::Int64, true),
            ]));
            let session_manager = test_session_manager();
            let session_state = session_manager.session_state();
            let df_schema = DFSchema::try_from(full_schema.as_ref().clone()).unwrap();
            let expr = session_state
                .create_logical_expr("extra > 5", &df_schema)
                .unwrap();

            let restricted_schema = test_schema();
            let result =
                KafkaSourceTableProvider::validate_filter_columns(&expr, &restricted_schema);
            assert!(result.is_err());
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("extra"),
                "Error should mention 'extra', got: {}",
                err_msg
            );
        }
    }
}

/// Real-payload regression check for the arrow-avro decode path (`ConfluentAvroDecoder` +
/// recursive `coerce_batch_to_target`): decoding the real blockchain `traces` schema + payload
/// must produce a `RecordBatch` whose schema matches streamling's `convert_avro_schema_to_arrow`
/// mapping (top-level u256 + nested `List<Struct{…Decimal128(100,0)}>`) and whose u256 column
/// round-trips. Lives inline per repo convention (no standalone test files in library crates).
#[cfg(test)]
mod arrow_avro_real_payload_tests {
    use apache_avro::Schema as AvroSchema;
    use arrow::array::{Array, FixedSizeBinaryArray};
    use arrow_schema::DataType;
    use streamling_core::formats::avro::arrow_avro::ConfluentAvroDecoder;
    use streamling_core::formats::avro::convert_avro_schema_to_arrow;
    use streamling_core::types::u256::U256Type;

    fn manifest_path(rel: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(rel)
    }

    fn load_schema(filename: &str) -> AvroSchema {
        let json =
            std::fs::read_to_string(manifest_path(&format!("testdata/avro-schema/{filename}")))
                .unwrap();
        AvroSchema::parse_str(&json).unwrap()
    }

    fn load_schema_json(filename: &str) -> String {
        std::fs::read_to_string(manifest_path(&format!("testdata/avro-schema/{filename}"))).unwrap()
    }

    fn load_payload(filename: &str) -> Vec<u8> {
        std::fs::read(manifest_path(&format!("testdata/avro-payload/{filename}"))).unwrap()
    }

    /// These `.bin` fixtures were captured with a trailing `0x0a` newline that is NOT part of the
    /// avro datum (a real Kafka message carries exactly the avro bytes). arrow-avro's streaming
    /// decoder rejects trailing junk, so we trim the fixture to the true datum boundary (found via
    /// apache_avro).
    fn trim_to_datum_boundary(framed: &[u8], writer_schema: &AvroSchema) -> Vec<u8> {
        let body = &framed[5..];
        let mut cursor = std::io::Cursor::new(body);
        apache_avro::from_avro_datum(writer_schema, &mut cursor, None)
            .expect("decode to find boundary");
        let consumed = cursor.position() as usize;
        framed[..5 + consumed].to_vec()
    }

    fn arrow_avro_decode(framed: &[u8], writer_json: &str) -> arrow::record_batch::RecordBatch {
        let schema_id = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
        let mut decoder = ConfluentAvroDecoder::new();
        decoder
            .register_writer_schema(schema_id, writer_json)
            .expect("register writer schema");
        decoder.decode(framed).expect("arrow-avro decode");
        decoder.flush().expect("arrow-avro flush").expect("a batch")
    }

    #[test]
    fn arrow_avro_decodes_real_traces_to_target_schema() {
        // v1 (inlined nested records, no avro Refs) is the schema shape the arrow decode path
        // supports. v2/v5 use named-type Refs which `convert_avro_schema_to_arrow` itself
        // `todo!()`s on.
        let schema_file = "traces-value-v1-1271.json";
        let writer_schema = load_schema(schema_file);
        let writer_json = load_schema_json(schema_file);
        let target = convert_avro_schema_to_arrow(writer_schema.clone());

        for payload_file in [
            "arbitrum-one.raw.traces-1271.bin",
            "arbitrum-one.raw.traces-1271-p0-o366.bin",
        ] {
            println!("\n=== {schema_file} / {payload_file} ===");
            let framed = load_payload(payload_file);
            assert_eq!(framed[0], 0x00, "expected Confluent magic byte");
            let framed = trim_to_datum_boundary(&framed, &writer_schema);

            let batch = arrow_avro_decode(&framed, &writer_json);
            let schema = batch.schema();

            // The decoded batch must carry exactly streamling's target Arrow schema, recursively
            // (field types, nullability, and u256/i256 extension metadata).
            assert_eq!(
                schema.fields(),
                target.fields(),
                "decoded batch schema does not match convert_avro_schema_to_arrow"
            );
            assert_eq!(batch.num_rows(), 1, "expected one decoded row");

            // Top-level `value` (avro decimal precision 100, scale 0) → u256 FixedSizeBinary(32).
            let value_idx = target.index_of("value").unwrap();
            let value_field = schema.field(value_idx).clone();
            assert!(
                U256Type::is_u256_field(&value_field),
                "`value` field is not tagged u256: {value_field:?}"
            );
            let value_col = batch
                .column(value_idx)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("u256 column is FixedSizeBinary(32)");
            assert!(
                !value_col.is_null(0),
                "`value` should be non-null in this row"
            );
            assert_eq!(value_col.value(0).len(), 32, "u256 is 32 bytes");

            // Nested high-precision decimals inside the array-of-records become Decimal128(100,0)
            // (matching streamling's top-level-only u256 fixup — nested decimals fall through).
            let xfers_idx = target.index_of("after_evm_transfers").unwrap();
            let DataType::List(elem) = schema.field(xfers_idx).data_type() else {
                panic!("after_evm_transfers is not a List");
            };
            let DataType::Struct(fields) = elem.data_type() else {
                panic!("after_evm_transfers element is not a Struct");
            };
            let nested_value = fields.iter().find(|f| f.name() == "value").unwrap();
            assert_eq!(
                nested_value.data_type(),
                &DataType::Decimal128(100, 0),
                "nested transfer `value` should be Decimal128(100,0)"
            );

            println!(
                "  OK: {} cols, schema + u256 + nested types verified",
                batch.num_columns()
            );
        }
    }
}
