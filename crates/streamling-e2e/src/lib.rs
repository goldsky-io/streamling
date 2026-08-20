//! End-to-end test framework for streamling.
//!
//! This crate provides utilities for running e2e tests against a k3s cluster
//! with PostgreSQL, Kafka (Redpanda), ClickHouse, and Prometheus.
//!
//! ## Design Principles
//!
//! - **No streamling dependencies**: This crate only depends on client libraries.
//!   Streamling is executed as an external binary.
//! - **Resource isolation**: Each test creates unique databases/topics via UUIDs.
//! - **Automatic cleanup**: Resources are cleaned up when `TestContext` is dropped.
//!
//! ## Example
//!
//! ```rust,ignore
//! use streamling_e2e::TestContext;
//!
//! #[tokio::test]
//! async fn test_kafka_to_postgres() {
//!     let ctx = TestContext::new().await.unwrap();
//!     
//!     // Seed data
//!     ctx.produce_json_records(&ctx.kafka_topic, records).await.unwrap();
//!     
//!     // Run streamling
//!     let status = ctx.run_streamling(&pipeline_yaml).await.unwrap();
//!     assert!(status.success());
//!     
//!     // Verify results
//!     let count = ctx.postgres.count("SELECT COUNT(*) FROM output").await.unwrap();
//!     assert_eq!(count, 100);
//! }
//! ```

pub mod inspect;
pub mod resources;
pub mod streamling;

use resources::{
    ClickHouseResource, KafkaResource, MySqlResource, PostgresResource, PrometheusResource,
    SqsResource,
};
use std::env;
use std::path::PathBuf;
use std::process::ExitStatus;
use tempfile::TempDir;
use thiserror::Error;
use tracing::info;
use uuid::Uuid;

// Re-export commonly used types
pub use resources::{PrintSinkOutput, PrintSinkRow};
pub use streamling::StreamlingOutput;

/// Errors that can occur during e2e testing
#[derive(Error, Debug)]
pub enum E2eError {
    #[error("Environment variable not set: {0}")]
    EnvVarNotSet(String),

    #[error("PostgreSQL error: {0}")]
    Postgres(#[from] sqlx::Error),

    #[error("Kafka error: {0}")]
    Kafka(String),

    #[error("ClickHouse error: {0}")]
    ClickHouse(String),

    #[error("MySQL error: {0}")]
    Mysql(String),

    #[error("Prometheus error: {0}")]
    Prometheus(String),

    #[error("SQS error: {0}")]
    Sqs(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("Streamling execution failed: {0}")]
    StreamlingFailed(String),
}

pub type Result<T> = std::result::Result<T, E2eError>;

/// Configuration for the e2e test environment
#[derive(Debug, Clone)]
pub struct E2eConfig {
    pub postgres_url: String,
    pub kafka_broker: String,
    pub schema_registry_url: String,
    pub clickhouse_url: String,
    pub mysql_url: Option<String>,
    pub prometheus_url: Option<String>,
    pub sqs_url: Option<String>,
    pub streamling_bin: Option<PathBuf>,
}

impl E2eConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            postgres_url: env::var("E2E_POSTGRES_URL")
                .map_err(|_| E2eError::EnvVarNotSet("E2E_POSTGRES_URL".to_string()))?,
            kafka_broker: env::var("E2E_KAFKA_BROKER")
                .map_err(|_| E2eError::EnvVarNotSet("E2E_KAFKA_BROKER".to_string()))?,
            schema_registry_url: env::var("E2E_SCHEMA_REGISTRY_URL")
                .map_err(|_| E2eError::EnvVarNotSet("E2E_SCHEMA_REGISTRY_URL".to_string()))?,
            clickhouse_url: env::var("E2E_CLICKHOUSE_URL")
                .map_err(|_| E2eError::EnvVarNotSet("E2E_CLICKHOUSE_URL".to_string()))?,
            mysql_url: env::var("E2E_MYSQL_URL").ok(),
            prometheus_url: env::var("E2E_PROMETHEUS_URL").ok(),
            sqs_url: env::var("E2E_SQS_URL").ok(),
            streamling_bin: env::var("E2E_STREAMLING_BIN").ok().map(PathBuf::from),
        })
    }
}

/// Test context providing isolated resources for a single test
pub struct TestContext {
    /// Unique identifier for this test run
    pub test_id: String,

    /// Configuration
    pub config: E2eConfig,

    /// PostgreSQL resource manager
    pub postgres: PostgresResource,

    /// Kafka resource manager
    pub kafka: KafkaResource,

    /// ClickHouse resource manager (optional, only if clickhouse tests are needed)
    pub clickhouse: Option<ClickHouseResource>,

    /// MySQL resource manager (optional, only if mysql tests are needed)
    pub mysql: Option<MySqlResource>,

    /// Prometheus resource manager (optional, only if metrics tests are needed)
    pub prometheus: Option<PrometheusResource>,

    /// SQS resource manager (optional, only if SQS tests are needed)
    pub sqs: Option<SqsResource>,

    /// When true, [`Self::build_env_vars`] forwards `STREAMLING__PLUGIN__PATH`
    /// to the streamling subprocess if it is set in the current environment
    /// and points at an existing file.
    pub use_plugin: bool,

    /// Temporary directory for pipeline files
    pub temp_dir: TempDir,

    /// Name of the isolated PostgreSQL database
    pub pg_database: String,

    /// Name of the isolated Kafka topic
    pub kafka_topic: String,
}

impl TestContext {
    /// Create a new test context with isolated resources
    pub async fn new() -> Result<Self> {
        Self::with_options(TestContextOptions::default()).await
    }

    /// Create a new test context with custom options
    pub async fn with_options(options: TestContextOptions) -> Result<Self> {
        let config = E2eConfig::from_env()?;
        let test_id = Uuid::new_v4().to_string();
        let short_id = &test_id[..8];

        info!("Creating test context with id: {}", test_id);

        // Create temporary directory
        let temp_dir = TempDir::new()?;

        // Create isolated PostgreSQL database
        let pg_database = format!("test_{}", short_id);
        let postgres = PostgresResource::new(&config.postgres_url, &pg_database).await?;
        info!("Created PostgreSQL database: {}", pg_database);

        // Create isolated Kafka topic
        let kafka_topic = format!("test_{}_topic", short_id);
        let kafka = KafkaResource::new(
            &config.kafka_broker,
            &config.schema_registry_url,
            &kafka_topic,
        )
        .await?;
        info!("Created Kafka topic: {}", kafka_topic);

        // Create ClickHouse database if needed
        let clickhouse = if options.with_clickhouse {
            let ch_database = format!("test_{}", short_id);
            Some(ClickHouseResource::new(&config.clickhouse_url, &ch_database).await?)
        } else {
            None
        };

        // Create MySQL database if needed
        let mysql = if options.with_mysql {
            let mysql_url = config.mysql_url.as_deref().ok_or_else(|| {
                E2eError::Mysql("E2E_MYSQL_URL must be set when with_mysql is enabled".into())
            })?;
            let mysql_database = format!("test_{}", short_id);
            Some(
                MySqlResource::new(mysql_url, &mysql_database)
                    .await
                    .map_err(|e| {
                        E2eError::Mysql(format!("failed to create MySQL resource: {}", e))
                    })?,
            )
        } else {
            None
        };

        // Create Prometheus resource if URL is configured and needed
        let prometheus = if options.with_prometheus {
            config
                .prometheus_url
                .as_ref()
                .map(|url| PrometheusResource::new(url))
        } else {
            None
        };

        // Create SQS resource if SQS URL is configured and needed
        let sqs = if options.with_sqs {
            if let Some(url) = &config.sqs_url {
                let sqs_queue = format!("test_{}_queue", short_id);
                Some(SqsResource::new(url, &sqs_queue).await?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            test_id,
            config,
            postgres,
            kafka,
            clickhouse,
            mysql,
            prometheus,
            sqs,
            use_plugin: options.with_plugin,
            temp_dir,
            pg_database,
            kafka_topic,
        })
    }

    /// Get the PostgreSQL connection string for the isolated database
    pub fn pg_connection_string(&self) -> String {
        self.postgres.connection_string()
    }

    /// Run streamling with the given pipeline YAML content
    pub async fn run_streamling(&self, pipeline_yaml: &str) -> Result<ExitStatus> {
        let pipeline_path = self.temp_dir.path().join("pipeline.yaml");
        std::fs::write(&pipeline_path, pipeline_yaml)?;

        streamling::run_streamling(
            &pipeline_path,
            self.config.streamling_bin.as_deref(),
            &self.build_env_vars(),
        )
        .await
    }

    /// Run streamling with a pipeline file path
    pub async fn run_streamling_file(&self, pipeline_path: &std::path::Path) -> Result<ExitStatus> {
        streamling::run_streamling(
            pipeline_path,
            self.config.streamling_bin.as_deref(),
            &self.build_env_vars(),
        )
        .await
    }

    /// Build environment variables for streamling execution
    pub fn build_env_vars(&self) -> Vec<(String, String)> {
        let mut env_vars = vec![
            (
                "STREAMLING__KAFKA_SOURCE__BROKERS".to_string(),
                self.config.kafka_broker.clone(),
            ),
            (
                "STREAMLING__KAFKA_SOURCE__SCHEMA_REGISTRY_URL".to_string(),
                self.config.schema_registry_url.clone(),
            ),
            // Unique consumer group ID per test to avoid offset conflicts
            (
                "STREAMLING__KAFKA_SOURCE__CONSUMER_GROUP_ID".to_string(),
                format!("e2e-test-{}", self.test_id),
            ),
            (
                "STREAMLING__POSTGRES_SINK__HOST".to_string(),
                self.postgres.host.clone(),
            ),
            (
                "STREAMLING__POSTGRES_SINK__PORT".to_string(),
                self.postgres.port.to_string(),
            ),
            (
                "STREAMLING__POSTGRES_SINK__USER".to_string(),
                self.postgres.user.clone(),
            ),
            (
                "STREAMLING__POSTGRES_SINK__PASS".to_string(),
                self.postgres.password.clone(),
            ),
            (
                "STREAMLING__POSTGRES_SINK__DB".to_string(),
                self.pg_database.clone(),
            ),
            // Use in-memory state backend for tests (don't persist checkpoints)
            (
                "STREAMLING__STATE_BACKEND__BACKEND_TYPE".to_string(),
                "InMemory".to_string(),
            ),
            // Kafka sink configuration
            (
                "STREAMLING__KAFKA_SINK__BROKERS".to_string(),
                self.config.kafka_broker.clone(),
            ),
            (
                "STREAMLING__KAFKA_SINK__SCHEMA_REGISTRY_URL".to_string(),
                self.config.schema_registry_url.clone(),
            ),
            // Avoid port collisions when e2e tests run streamling in parallel.
            // Port 0 lets the OS pick an ephemeral free port for the admin API.
            ("STREAMLING__ADMIN_API_PORT".to_string(), "0".to_string()),
        ];

        // Add ClickHouse source and sink configuration if clickhouse is enabled
        if let Some(clickhouse) = &self.clickhouse {
            // ClickHouse source configuration
            env_vars.push((
                "STREAMLING__CLICKHOUSE_SOURCE__URL".to_string(),
                self.config.clickhouse_url.clone(),
            ));
            env_vars.push((
                "STREAMLING__CLICKHOUSE_SOURCE__USER".to_string(),
                "default".to_string(),
            ));
            env_vars.push((
                "STREAMLING__CLICKHOUSE_SOURCE__PASSWORD".to_string(),
                String::new(),
            ));
            env_vars.push((
                "STREAMLING__CLICKHOUSE_SOURCE__DATABASE".to_string(),
                clickhouse.database.clone(),
            ));
            // ClickHouse sink configuration
            env_vars.push((
                "STREAMLING__CLICKHOUSE_SINK__URL".to_string(),
                self.config.clickhouse_url.clone(),
            ));
            env_vars.push((
                "STREAMLING__CLICKHOUSE_SINK__USER".to_string(),
                "default".to_string(),
            ));
            env_vars.push((
                "STREAMLING__CLICKHOUSE_SINK__PASSWORD".to_string(),
                String::new(),
            ));
            env_vars.push((
                "STREAMLING__CLICKHOUSE_SINK__DATABASE".to_string(),
                clickhouse.database.clone(),
            ));
        }

        // Add AWS/SQS configuration for ElasticMQ if SQS is enabled
        if self.sqs.is_some() {
            if let Some(sqs_url) = &self.config.sqs_url {
                // Default credential chain fallback (for SqsResource in test harness)
                env_vars.push(("AWS_ACCESS_KEY_ID".to_string(), "test".to_string()));
                env_vars.push(("AWS_SECRET_ACCESS_KEY".to_string(), "test".to_string()));
                env_vars.push(("AWS_DEFAULT_REGION".to_string(), "us-east-1".to_string()));
                env_vars.push(("AWS_ENDPOINT_URL".to_string(), sqs_url.clone()));
            }
        }

        if self.use_plugin {
            if let Ok(plugin_path) = env::var("STREAMLING__PLUGIN__PATH") {
                if !plugin_path.is_empty() && PathBuf::from(&plugin_path).exists() {
                    env_vars.push(("STREAMLING__PLUGIN__PATH".to_string(), plugin_path));
                }
            }
        }

        // Add Prometheus/OpenTelemetry metrics configuration if enabled
        if let Some(prometheus) = &self.prometheus {
            // Set application ID to test_id for metrics isolation
            env_vars.push((
                "STREAMLING__APPLICATION_ID".to_string(),
                self.test_id.clone(),
            ));
            env_vars.push((
                "STREAMLING__OPEN_TELEMETRY_METRICS__INGESTION_ENDPOINT".to_string(),
                prometheus.ingestion_endpoint.clone(),
            ));
            env_vars.push((
                "STREAMLING__OPEN_TELEMETRY_METRICS__ENDPOINT_PROTOCOL".to_string(),
                "http/protobuf".to_string(),
            ));
            env_vars.push((
                "STREAMLING__OPEN_TELEMETRY_METRICS__BATCH_INTERVAL_SECS".to_string(),
                "1".to_string(),
            ));
        }

        env_vars
    }

    /// Create an additional SQS queue for use in tests
    pub async fn create_sqs_queue(&self, queue_suffix: &str) -> Result<SqsResource> {
        let queue_name = format!("test_{}_{}", &self.test_id[..8], queue_suffix);
        let sqs_url = self
            .config
            .sqs_url
            .as_ref()
            .ok_or_else(|| E2eError::EnvVarNotSet("E2E_SQS_URL".to_string()))?;
        SqsResource::new(sqs_url, &queue_name).await
    }

    /// Create an additional isolated PostgreSQL database, for tests that need a
    /// second destination on the same cluster (e.g. two postgres sinks resolving
    /// their own per-sink connections). Dropped when the returned resource is.
    pub async fn create_postgres_database(&self, suffix: &str) -> Result<PostgresResource> {
        let database = format!("test_{}_{}", &self.test_id[..8], suffix);
        PostgresResource::new(&self.config.postgres_url, &database).await
    }

    /// Create an additional Kafka topic for use in tests (e.g., for sink output)
    pub async fn create_kafka_topic(&self, topic_suffix: &str) -> Result<KafkaResource> {
        let topic = format!("test_{}_{}", &self.test_id[..8], topic_suffix);
        KafkaResource::new(
            &self.config.kafka_broker,
            &self.config.schema_registry_url,
            &topic,
        )
        .await
    }

    /// Consume messages from a Kafka topic and return them as decoded values
    pub async fn consume_kafka_messages(
        &self,
        topic: &str,
        max_messages: usize,
    ) -> Result<Vec<(i64, String, String)>> {
        let (messages, _) = KafkaResource::inspect_topic_messages(
            &self.config.kafka_broker,
            &self.config.schema_registry_url,
            topic,
            max_messages,
            max_messages,
        )
        .await?;
        Ok(messages)
    }

    /// Run streamling with pipeline YAML and stop after N records
    ///
    /// This is the most common pattern for e2e tests - run until a specific
    /// number of records have been processed.
    pub async fn run_pipeline(&self, pipeline_yaml: &str, record_limit: u64) -> Result<ExitStatus> {
        self.run_pipeline_with_opts(
            pipeline_yaml,
            PipelineOpts::new().record_limit(record_limit),
        )
        .await
    }

    /// Run streamling with pipeline YAML and custom options
    pub async fn run_pipeline_with_opts(
        &self,
        pipeline_yaml: &str,
        opts: PipelineOpts,
    ) -> Result<ExitStatus> {
        let pipeline_path = self.temp_dir.path().join("pipeline.yaml");
        std::fs::write(&pipeline_path, pipeline_yaml)?;

        let mut env_vars = self.build_env_vars();

        if let Some(limit) = opts.record_limit {
            env_vars.push((
                "STREAMLING__NUM_RECORDS_BEFORE_STOP".to_string(),
                limit.to_string(),
            ));
        }

        env_vars.extend(opts.extra_env);

        let timeout = opts.timeout.unwrap_or(std::time::Duration::from_secs(30));

        tokio::time::timeout(
            timeout,
            streamling::run_streamling(
                &pipeline_path,
                self.config.streamling_bin.as_deref(),
                &env_vars,
            ),
        )
        .await
        .map_err(|_| {
            E2eError::StreamlingFailed(format!("Pipeline execution timed out after {:?}", timeout))
        })?
    }

    /// Run streamling with pipeline YAML, send SIGTERM after `signal_after`,
    /// and require the process to exit within `exit_deadline` of the signal.
    ///
    /// This is the graceful-shutdown chaos harness: it asserts the process
    /// observes SIGTERM, drains, and exits well inside the k8s grace period
    /// instead of hanging until SIGKILL.
    #[cfg(unix)]
    pub async fn run_pipeline_with_sigterm(
        &self,
        pipeline_yaml: &str,
        opts: PipelineOpts,
        signal_after: std::time::Duration,
        exit_deadline: std::time::Duration,
    ) -> Result<ExitStatus> {
        let pipeline_path = self.temp_dir.path().join("pipeline.yaml");
        std::fs::write(&pipeline_path, pipeline_yaml)?;

        let mut env_vars = self.build_env_vars();
        if let Some(limit) = opts.record_limit {
            env_vars.push((
                "STREAMLING__NUM_RECORDS_BEFORE_STOP".to_string(),
                limit.to_string(),
            ));
        }
        env_vars.extend(opts.extra_env);

        streamling::run_streamling_with_sigterm(
            &pipeline_path,
            self.config.streamling_bin.as_deref(),
            &env_vars,
            signal_after,
            exit_deadline,
        )
        .await
    }

    /// Run streamling with pipeline YAML and capture stdout/stderr for inspection
    ///
    /// This is useful for tests that need to inspect the print sink output.
    /// Returns a `PrintSinkOutput` which can be used to verify the pipeline output.
    pub async fn run_pipeline_with_capture(
        &self,
        pipeline_yaml: &str,
        opts: PipelineOpts,
    ) -> Result<PrintSinkOutput> {
        let pipeline_path = self.temp_dir.path().join("pipeline.yaml");
        std::fs::write(&pipeline_path, pipeline_yaml)?;

        let mut env_vars = self.build_env_vars();

        if let Some(limit) = opts.record_limit {
            env_vars.push((
                "STREAMLING__NUM_RECORDS_BEFORE_STOP".to_string(),
                limit.to_string(),
            ));
        }

        env_vars.extend(opts.extra_env);

        let timeout = opts.timeout.unwrap_or(std::time::Duration::from_secs(30));

        let output = tokio::time::timeout(
            timeout,
            streamling::run_streamling_with_capture(
                &pipeline_path,
                self.config.streamling_bin.as_deref(),
                &env_vars,
            ),
        )
        .await
        .map_err(|_| {
            E2eError::StreamlingFailed(format!("Pipeline execution timed out after {:?}", timeout))
        })??;

        // Parse print sink output from stderr (tracing logs are written to stderr)
        let parsed = PrintSinkOutput::parse(&output.stderr);
        tracing::debug!("Parsed {} rows from print sink output", parsed.len());

        Ok(parsed)
    }

    /// Run streamling with pipeline YAML and return raw output (stdout, stderr, status)
    ///
    /// This is useful for tests that need to inspect error messages or other raw output
    /// that doesn't fit the PrintSinkOutput format. Unlike other run methods, this
    /// returns the output even if the pipeline fails, allowing tests to inspect
    /// error messages.
    pub async fn run_pipeline_raw(
        &self,
        pipeline_yaml: &str,
        opts: PipelineOpts,
    ) -> Result<StreamlingOutput> {
        let pipeline_path = self.temp_dir.path().join("pipeline.yaml");
        std::fs::write(&pipeline_path, pipeline_yaml)?;

        let mut env_vars = self.build_env_vars();

        if let Some(limit) = opts.record_limit {
            env_vars.push((
                "STREAMLING__NUM_RECORDS_BEFORE_STOP".to_string(),
                limit.to_string(),
            ));
        }

        env_vars.extend(opts.extra_env);

        let timeout = opts.timeout.unwrap_or(std::time::Duration::from_secs(30));

        tokio::time::timeout(
            timeout,
            streamling::run_streamling_raw(
                &pipeline_path,
                self.config.streamling_bin.as_deref(),
                &env_vars,
                &opts.extra_args,
            ),
        )
        .await
        .map_err(|_| {
            E2eError::StreamlingFailed(format!("Pipeline execution timed out after {:?}", timeout))
        })?
    }
}

/// Options for running a pipeline
#[derive(Debug, Clone)]
pub struct PipelineOpts {
    /// Stop after processing this many records
    pub record_limit: Option<u64>,
    /// Additional environment variables
    pub extra_env: Vec<(String, String)>,
    /// Timeout for pipeline execution (default: 30 seconds)
    pub timeout: Option<std::time::Duration>,
    /// Extra CLI arguments passed to the streamling binary
    pub extra_args: Vec<String>,
}

impl Default for PipelineOpts {
    fn default() -> Self {
        Self {
            record_limit: None,
            extra_env: Vec::new(),
            timeout: Some(std::time::Duration::from_secs(30)),
            extra_args: Vec::new(),
        }
    }
}

impl PipelineOpts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_limit(mut self, limit: u64) -> Self {
        self.record_limit = Some(limit);
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_env.push((key.into(), value.into()));
        self
    }

    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn no_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.extra_args.push(arg.into());
        self
    }
}

/// Options for creating a test context
#[derive(Debug, Clone, Default)]
pub struct TestContextOptions {
    /// Whether to create a ClickHouse database
    pub with_clickhouse: bool,
    /// Whether to create a MySQL database
    pub with_mysql: bool,
    /// Whether to enable Prometheus metrics
    pub with_prometheus: bool,
    /// Whether to create an SQS queue (requires ElasticMQ or SQS-compatible endpoint)
    pub with_sqs: bool,
    /// Whether to forward `STREAMLING__PLUGIN__PATH` to the streamling
    /// subprocess (for tests that require plugin-provided sources, sinks, or UDFs).
    pub with_plugin: bool,
}

impl TestContextOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_clickhouse(mut self) -> Self {
        self.with_clickhouse = true;
        self
    }

    pub fn with_mysql(mut self) -> Self {
        self.with_mysql = true;
        self
    }

    pub fn with_prometheus(mut self) -> Self {
        self.with_prometheus = true;
        self
    }

    pub fn with_sqs(mut self) -> Self {
        self.with_sqs = true;
        self
    }

    pub fn with_plugin(mut self) -> Self {
        self.with_plugin = true;
        self
    }
}

/// Initialize tracing for tests
pub fn init_tracing() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let _ = tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .try_init();
}
