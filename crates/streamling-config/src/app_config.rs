use anyhow::Context;
use config::{Config, ConfigError, Environment, File, FileFormat};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use serde::{Deserialize as SerdeDeserialize, Deserializer, de::Error as DeError};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fmt::Formatter;
use std::time::Duration;

fn default_sslmode() -> String {
    "require".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StateBackendType {
    InMemory,
    Postgres,
    Sqlite,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PostgresStateBackendConfig {
    pub host: String,
    pub port: u16,
    pub db: String,
    pub user: String,
    pub password: String,
    #[serde(default = "default_sslmode")]
    pub sslmode: String,
    pub max_connections: Option<u32>,
    pub state_schema_name: Option<String>,
    pub state_table_name: Option<String>,
}

impl std::fmt::Debug for PostgresStateBackendConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let masked_password = if self.password.len() > 5 {
            format!("*****{}", &self.password[self.password.len() - 5..])
        } else {
            "*****".to_string()
        };
        f.debug_struct("PostgresStateBackendConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("db", &self.db)
            .field("user", &self.user)
            .field("password", &masked_password)
            .field("sslmode", &self.sslmode)
            .field("max_connections", &self.max_connections)
            .field("state_schema_name", &self.state_schema_name)
            .field("state_table_name", &self.state_table_name)
            .finish()
    }
}

impl PostgresStateBackendConfig {
    pub fn connection_url(&self) -> String {
        postgres_connection_url(
            self.host.clone(),
            self.port,
            self.db.clone(),
            self.user.clone(),
            self.password.clone(),
            self.sslmode.clone(),
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SqliteStateBackendConfig {
    pub database_path: String,
    pub max_connections: Option<u32>,
    pub state_table_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StateBackendConfig {
    pub backend_type: StateBackendType,
    pub postgres: Option<PostgresStateBackendConfig>,
    pub sqlite: Option<SqliteStateBackendConfig>,
}

impl StateBackendConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        match self.backend_type {
            StateBackendType::Postgres => {
                if self.postgres.is_none() {
                    return Err(ConfigError::Message(
                        "Postgres backend type requires postgres configuration".to_string(),
                    ));
                }
            }
            StateBackendType::Sqlite => {
                if self.sqlite.is_none() {
                    return Err(ConfigError::Message(
                        "Sqlite backend type requires sqlite configuration".to_string(),
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

impl Default for StateBackendConfig {
    fn default() -> Self {
        Self {
            backend_type: StateBackendType::InMemory,
            postgres: None,
            sqlite: None,
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub enum DynamicTableBackendType {
    InMemory,
    Postgres,
}

impl Default for DynamicTableBackendType {
    fn default() -> Self {
        Self::Postgres
    }
}

impl<'de> SerdeDeserialize<'de> for DynamicTableBackendType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        // Normalize to case-insensitive, remove non-alphanumeric to allow "in_memory", "In-Memory", etc.
        let normalized = s
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect::<String>()
            .to_lowercase();

        match normalized.as_str() {
            "inmemory" => Ok(DynamicTableBackendType::InMemory),
            "postgres" => Ok(DynamicTableBackendType::Postgres),
            _ => Err(DeError::custom(format!(
                "invalid dynamic table backend_type '{}' (expected 'Postgres' or 'InMemory')",
                s
            ))),
        }
    }
}

// TODO: perhaps this can be merged with PostgresStateBackendConfig
#[derive(Serialize, Deserialize, Clone)]
pub struct PostgresDynamicTableBackendConfig {
    pub host: String,
    pub port: u16,
    pub db: String,
    pub user: String,
    pub password: String,
    #[serde(default = "default_sslmode")]
    pub sslmode: String,
    pub max_connections: Option<u32>,
    pub dt_schema_name: Option<String>,
    /// Enables incremental in-memory caching for dynamic tables that explicitly set `time_column`.
    #[serde(default)]
    pub cache_enabled: bool,
}

impl std::fmt::Debug for PostgresDynamicTableBackendConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let masked_password = if self.password.len() > 5 {
            format!("*****{}", &self.password[self.password.len() - 5..])
        } else {
            "*****".to_string()
        };
        f.debug_struct("PostgresDynamicTableBackendConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("db", &self.db)
            .field("user", &self.user)
            .field("password", &masked_password)
            .field("sslmode", &self.sslmode)
            .field("max_connections", &self.max_connections)
            .field("dt_schema_name", &self.dt_schema_name)
            .field("cache_enabled", &self.cache_enabled)
            .finish()
    }
}

impl PostgresDynamicTableBackendConfig {
    pub fn connection_url(&self) -> String {
        postgres_connection_url(
            self.host.clone(),
            self.port,
            self.db.clone(),
            self.user.clone(),
            self.password.clone(),
            self.sslmode.clone(),
        )
    }
}

pub fn postgres_connection_url(
    host: String,
    port: u16,
    db: String,
    user: String,
    password: String,
    sslmode: String,
) -> String {
    let encoded_user = utf8_percent_encode(user.as_str(), NON_ALPHANUMERIC);
    let encoded_password = utf8_percent_encode(password.as_str(), NON_ALPHANUMERIC);

    let base_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        encoded_user, encoded_password, host, port, db
    );

    if !sslmode.is_empty() {
        format!("{}?sslmode={}", base_url, sslmode)
    } else {
        base_url
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DynamicTableBackendConfig {
    pub postgres: Option<PostgresDynamicTableBackendConfig>,
    /// Maximum number of entries to process in a single query. If a batch exceeds this,
    /// it will be split into multiple concurrent queries. If None, defaults to 1000.
    pub max_batch_size: Option<usize>,
}

#[derive(Clone, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    /// Schema registry URL. Required for Avro format, optional for JSON format.
    pub schema_registry_url: Option<String>,
    pub schema_registry_username: Option<String>,
    pub schema_registry_password: Option<String>,
    pub consumer_group_id: Option<String>, // Source only property
    pub client_id: Option<String>,
    pub lag_report_interval_ms: Option<u64>,
}

impl std::fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mask = |opt: &Option<String>| -> Option<String> {
            opt.as_ref().map(|s| {
                if s.len() > 5 {
                    format!("*****{}", &s[s.len() - 5..])
                } else {
                    "*****".to_string()
                }
            })
        };
        f.debug_struct("KafkaConfig")
            .field("brokers", &self.brokers)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password", &mask(&self.sasl_password))
            .field("schema_registry_url", &self.schema_registry_url)
            .field("schema_registry_username", &self.schema_registry_username)
            .field(
                "schema_registry_password",
                &mask(&self.schema_registry_password),
            )
            .field("consumer_group_id", &self.consumer_group_id)
            .field("client_id", &self.client_id)
            .finish()
    }
}

impl KafkaConfig {
    /// Returns schema registry settings if a schema registry URL is configured.
    /// Returns None if no schema registry URL is set (e.g., when using JSON format).
    pub fn get_schema_registry_settings(&self) -> Option<SrSettings> {
        let url = self.schema_registry_url.as_ref()?;
        let mut builder = SrSettings::new_builder(url.clone());

        if let (Some(username), Some(password)) = (
            &self.schema_registry_username,
            &self.schema_registry_password,
        ) {
            builder.set_basic_authorization(username, Some(password.as_str()));
        }

        Some(
            builder
                .build()
                .expect("failed to build schema registry settings from KafkaConfig"),
        )
    }
}

/// Compression codec applied by the Kafka sink's producer (librdkafka
/// `compression.type`). Defaults to `lz4`, which is the historical built-in
/// behavior. Parsing is case-insensitive so env-var overrides arriving as
/// strings work.
///
/// Only the codecs that librdkafka always links are exposed: `none`, `gzip`,
/// and `lz4`. `gzip` (or `none`) is needed for brokers that don't accept lz4.
/// `snappy`/`zstd` are intentionally omitted for now — `zstd` in particular
/// requires libzstd to be linked at build time — and can be added once that is
/// verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KafkaCompression {
    None,
    Gzip,
    #[default]
    Lz4,
}

impl KafkaCompression {
    /// The librdkafka `compression.type` value for this codec.
    pub fn as_str(self) -> &'static str {
        match self {
            KafkaCompression::None => "none",
            KafkaCompression::Gzip => "gzip",
            KafkaCompression::Lz4 => "lz4",
        }
    }
}

impl<'de> SerdeDeserialize<'de> for KafkaCompression {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.trim().to_lowercase().as_str() {
            "none" => Ok(KafkaCompression::None),
            "gzip" => Ok(KafkaCompression::Gzip),
            "lz4" => Ok(KafkaCompression::Lz4),
            other => Err(DeError::custom(format!(
                "unknown Kafka compression `{}`; expected `none`, `gzip`, or `lz4`",
                other
            ))),
        }
    }
}

/// Wire-format compression to apply to outbound HTTP request bodies sent to
/// ClickHouse. Accepted values: `"none"`, `"gzip"`, `"zstd"` (default), or
/// `"lz4"`. Parsing is case-insensitive so env-var overrides arriving as
/// strings work. The gzip compression *level* is configured separately via
/// `compression_level`; `zstd` uses its default level (3) and `lz4` (frame
/// format) has no level knob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ClickHouseCompression {
    None,
    #[default]
    Zstd,
    Gzip,
    Lz4,
}

impl<'de> SerdeDeserialize<'de> for ClickHouseCompression {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.trim().to_lowercase().as_str() {
            "none" => Ok(ClickHouseCompression::None),
            "zstd" => Ok(ClickHouseCompression::Zstd),
            "gzip" => Ok(ClickHouseCompression::Gzip),
            "lz4" => Ok(ClickHouseCompression::Lz4),
            other => Err(DeError::custom(format!(
                "unknown ClickHouse compression `{}`; expected `none`, `gzip`, `zstd`, or `lz4`",
                other
            ))),
        }
    }
}

/// gzip compression level, 0–9. flate2's `Compression::default()` is 6, which
/// is also our default. 0 means no compression (gzip framing only), 1 is
/// fastest, 9 is best ratio.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GzipCompressionLevel(u32);

impl GzipCompressionLevel {
    pub const MAX: u32 = 9;

    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl Default for GzipCompressionLevel {
    fn default() -> Self {
        Self(6)
    }
}

impl<'de> SerdeDeserialize<'de> for GzipCompressionLevel {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let level = u32::deserialize(deserializer)?;
        if level > Self::MAX {
            return Err(DeError::custom(format!(
                "compression_level must be 0–{}, got {}",
                Self::MAX,
                level
            )));
        }
        Ok(Self(level))
    }
}

#[derive(Clone, Deserialize)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
    /// Wire compression for INSERTs. Accepts `"none"`, `"gzip"`, `"zstd"`
    /// (default), or `"lz4"`. Set here for the global default; optionally
    /// override per sink via the pipeline YAML.
    #[serde(default)]
    pub compression: ClickHouseCompression,
    /// gzip compression level, 0–9. Defaults to 6 (`flate2::Compression::default()`).
    /// Applies to `"gzip"` only; ignored for `"none"`, `"zstd"` (uses its
    /// default level 3), and `"lz4"` (frame format has no level knob).
    #[serde(default)]
    pub compression_level: GzipCompressionLevel,
}

#[derive(Clone, Deserialize)]
pub struct ClickHouseSourceConfig {
    #[serde(flatten)]
    pub connection: ClickHouseConfig,
    pub page_size: Option<usize>,
    /// Range of the first sorting key (e.g. block_number) to scan per query batch.
    /// Limits scan width to prevent timeouts on large tables. Automatically halved on timeout.
    /// Only applies when the first sorting key is a numeric type.
    /// Default: 1,000,000
    #[serde(alias = "block_range")]
    pub sort_key_range: Option<i64>,
}

/// Global defaults for every ClickHouse sink in the pipeline. Connection fields
/// are flattened, so `clickhouse_sink.url` / `STREAMLING__CLICKHOUSE_SINK__URL`
/// keep working unchanged.
///
/// `deny_unknown_fields` is deliberately absent: serde rejects it alongside
/// `flatten`, since the flattened struct is what consumes the "unknown" keys.
#[derive(Clone, Deserialize)]
pub struct ClickHouseSinkConfig {
    #[serde(flatten)]
    pub connection: ClickHouseConfig,
    /// Rows per INSERT for sinks that omit `batch_size` in the pipeline YAML.
    /// The old fallback (the global `record_batch_size`, 1000) caps row-heavy
    /// backfills well below what ClickHouse will accept; 100k measured ~12x
    /// higher throughput on transaction-shaped data (STRM-6530). Override with
    /// `STREAMLING__CLICKHOUSE_SINK__BATCH_SIZE`.
    pub batch_size: u32,
    /// Flush interval for sinks that omit `batch_flush_interval`, as a
    /// humantime duration (`"1s"`, `"500ms"`). Bounds tail latency for
    /// low-volume pipelines that would never fill a `batch_size` batch.
    /// Override with `STREAMLING__CLICKHOUSE_SINK__BATCH_FLUSH_INTERVAL`.
    pub batch_flush_interval: String,
}

impl ClickHouseSinkConfig {
    /// Parses `batch_flush_interval` into a `Duration`.
    ///
    /// Called during `AppConfig` load so a malformed value (typically a typo in
    /// `STREAMLING__CLICKHOUSE_SINK__BATCH_FLUSH_INTERVAL`) fails startup with a
    /// clear message, instead of surviving until the first ClickHouse sink is
    /// planned.
    pub fn parsed_batch_flush_interval(&self) -> anyhow::Result<Duration> {
        humantime::parse_duration(&self.batch_flush_interval).with_context(|| {
            format!(
                "clickhouse_sink.batch_flush_interval must be a duration like \"1s\" or \"500ms\", got '{}'",
                self.batch_flush_interval
            )
        })
    }
}

impl std::fmt::Debug for ClickHouseSinkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSinkConfig")
            .field("connection", &self.connection)
            .field("batch_size", &self.batch_size)
            .field("batch_flush_interval", &self.batch_flush_interval)
            .finish()
    }
}

impl std::fmt::Debug for ClickHouseConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let masked_password = if self.password.len() > 5 {
            format!("*****{}", &self.password[self.password.len() - 5..])
        } else {
            "*****".to_string()
        };
        f.debug_struct("ClickHouseConfig")
            .field("url", &self.url)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &masked_password)
            .field("compression", &self.compression)
            .field("compression_level", &self.compression_level)
            .finish()
    }
}

impl std::fmt::Debug for ClickHouseSourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSourceConfig")
            .field("connection", &self.connection)
            .field("page_size", &self.page_size)
            .finish()
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct PrintSinkConfig {
    pub sample_every: u32,
    pub num_records_before_stop: Option<u64>,
}

#[derive(Deserialize, Clone)]
pub struct PostgresSinkConfig {
    pub host: String,
    pub port: String,
    pub user: String,
    pub pass: String,
    pub db: String,
    pub sslmode: String,
    pub batch_flush_interval: String, // milliseconds
    pub batch_size: u32,
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    #[serde(default = "default_pool_acquire_timeout_secs")]
    pub pool_acquire_timeout_secs: u64,
    #[serde(default = "default_pool_idle_timeout_secs")]
    pub pool_idle_timeout_secs: u64,
    #[serde(default = "default_pool_max_lifetime_secs")]
    pub pool_max_lifetime_secs: u64,
}

fn default_statement_timeout_secs() -> u64 {
    60
}
fn default_pool_acquire_timeout_secs() -> u64 {
    30
}
fn default_pool_idle_timeout_secs() -> u64 {
    600
}
fn default_pool_max_lifetime_secs() -> u64 {
    1800
}

impl std::fmt::Debug for PostgresSinkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let masked_pass = if self.pass.len() > 5 {
            format!("*****{}", &self.pass[self.pass.len() - 5..])
        } else {
            "*****".to_string()
        };
        f.debug_struct("PostgresSinkConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("pass", &masked_pass)
            .field("db", &self.db)
            .field("sslmode", &self.sslmode)
            .field("batch_flush_interval", &self.batch_flush_interval)
            .field("batch_size", &self.batch_size)
            .field("statement_timeout_secs", &self.statement_timeout_secs)
            .field("pool_acquire_timeout_secs", &self.pool_acquire_timeout_secs)
            .field("pool_idle_timeout_secs", &self.pool_idle_timeout_secs)
            .field("pool_max_lifetime_secs", &self.pool_max_lifetime_secs)
            .finish()
    }
}

impl PostgresSinkConfig {
    pub fn get_secure_config(&self) -> HashMap<String, String> {
        HashMap::from([
            ("host".to_string(), self.host.clone()),
            ("port".to_string(), self.port.clone()),
            ("user".to_string(), self.user.clone()),
            ("pass".to_string(), self.pass.clone()),
            ("db".to_string(), self.db.clone()),
            (
                "batch_flush_interval".to_string(),
                self.batch_flush_interval.clone(),
            ),
            ("batch_size".to_string(), self.batch_size.to_string()),
            ("sslmode".to_string(), self.sslmode.to_string()),
        ])
    }

    /// Client-side bound for a single statement execution.
    ///
    /// `statement_timeout_secs` is enforced by the server and cannot fire when
    /// the connection is dead (e.g. a middlebox silently dropped the flow), so
    /// sinks additionally bound the await client-side. Derived as 2x the
    /// server-side timeout so the server-side timeout fires first whenever the
    /// server is reachable (the 2x headroom also covers wire upload time,
    /// which the server-side timeout does not measure).
    ///
    /// `statement_timeout_secs = 0` means "no timeout" and is honored here
    /// too (`None`): a statement legitimately slower than any fixed bound
    /// would otherwise be killed and retried forever. Users who disable the
    /// server timeout opt out of the client bound as well.
    pub fn client_statement_timeout(&self) -> Option<std::time::Duration> {
        match self.statement_timeout_secs {
            0 => None,
            s => Some(std::time::Duration::from_secs(s.saturating_mul(2))),
        }
    }
}

impl Default for PostgresSinkConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: "5432".to_string(),
            user: "postgres".to_string(),
            pass: "password".to_string(),
            db: "postgres".to_string(),
            sslmode: "disable".to_string(),
            batch_flush_interval: "1000".to_string(),
            batch_size: 1000,
            statement_timeout_secs: default_statement_timeout_secs(),
            pool_acquire_timeout_secs: default_pool_acquire_timeout_secs(),
            pool_idle_timeout_secs: default_pool_idle_timeout_secs(),
            pool_max_lifetime_secs: default_pool_max_lifetime_secs(),
        }
    }
}

/// Normalizes a topology node name to the key the config crate produces for a
/// `STREAMLING__<SINK_TYPE>_SINK_CONNECTIONS__<NODE_NAME>__<FIELD>` env var.
///
/// The config crate lowercases env var names and splits them on `__`, so a node
/// name has to survive both: non-alphanumeric characters become `_` and runs of
/// `_` collapse into one. `my-sink`, `my_sink` and `my__sink` therefore all
/// resolve to `my_sink`. streamling-agent applies the same normalization when it
/// writes the env vars, and rejects a pipeline whose sink names collide once
/// normalized, so one key never stands for two different destinations.
pub fn normalize_sink_key(node_name: &str) -> String {
    let mut key = String::with_capacity(node_name.len());
    for c in node_name.chars() {
        if c.is_alphanumeric() {
            key.push(c.to_ascii_lowercase());
        } else if !key.is_empty() && !key.ends_with('_') {
            key.push('_');
        }
    }
    // A leading or trailing `_` would make the config crate split the env name
    // into an empty segment, so the key could never be looked up.
    if key.ends_with('_') {
        key.pop();
    }
    key
}

/// Connection overrides for one `postgres` / `postgres_aggregate` sink node.
///
/// A streamling process has a single global `postgres_sink` connection, so two
/// postgres sinks in one pipeline used to share one destination: whichever
/// secret the cloud agent flattened into `STREAMLING__POSTGRES_SINK__*` last
/// won, and the other sink silently wrote to the same database (STRM-6516).
/// Each sink's own credentials now arrive under
/// `STREAMLING__POSTGRES_SINK_CONNECTIONS__<SINK_NAME>__*`; fields left unset
/// fall back to the global block, and a sink with no entry at all keeps the
/// global connection verbatim.
#[derive(Deserialize, Clone, Default)]
pub struct PostgresSinkConnection {
    pub host: Option<String>,
    pub port: Option<String>,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub db: Option<String>,
    pub sslmode: Option<String>,
}

impl std::fmt::Debug for PostgresSinkConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSinkConnection")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("pass", &self.pass.as_ref().map(|_| "*****"))
            .field("db", &self.db)
            .field("sslmode", &self.sslmode)
            .finish()
    }
}

impl PostgresSinkConnection {
    fn apply_to(&self, config: &mut PostgresSinkConfig) {
        if let Some(host) = &self.host {
            config.host = host.clone();
        }
        if let Some(port) = &self.port {
            config.port = port.clone();
        }
        if let Some(user) = &self.user {
            config.user = user.clone();
        }
        if let Some(pass) = &self.pass {
            config.pass = pass.clone();
        }
        if let Some(db) = &self.db {
            config.db = db.clone();
        }
        if let Some(sslmode) = &self.sslmode {
            config.sslmode = sslmode.clone();
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExternalHttpHandlerConfig {
    pub trigger_max_count: u32,
    pub operator_timeout_sec: u32,
    pub buffer_size: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WasmScriptConfig {
    /// Optional override for the embedded WASM runtime. When unset, the runtime
    /// compiled into the binary is used.
    #[serde(default)]
    pub runtime_wasm_file_path: Option<String>,
    /// Number of WASM plugin instances in the pool for concurrent processing.
    /// Higher values allow more concurrent batch processing but use more memory.
    /// Default is 4.
    #[serde(default = "default_wasm_parallelism")]
    pub parallelism: usize,
    /// Minimum number of rows to accumulate before processing.
    /// Smaller batches are combined until this threshold is reached.
    /// Set to 0 to disable accumulation and process each batch immediately.
    /// Default is 0 (disabled).
    #[serde(default)]
    pub batch_size: usize,
}

fn default_wasm_parallelism() -> usize {
    4
}

#[derive(Debug, Deserialize, Clone)]
pub struct PluginConfig {
    pub path: Option<String>,
    #[serde(default = "default_plugin_channel_capacity")]
    pub channel_capacity: u32,
    #[serde(default, deserialize_with = "deserialize_string_list")]
    pub preprocessor_ids: Vec<String>,
    #[serde(default)]
    pub preprocessor_options: HashMap<String, HashMap<String, String>>,
    #[serde(default, deserialize_with = "deserialize_string_list")]
    pub side_output_ids: Vec<String>,
    #[serde(default)]
    pub side_output_options: HashMap<String, HashMap<String, String>>,
}

fn default_plugin_channel_capacity() -> u32 {
    50
}

/// Accepts either a YAML/JSON list of strings or a single comma-separated string.
/// Env vars are always strings, so a comma separator is the natural encoding for a list
/// (e.g. `STREAMLING__PLUGIN__PREPROCESSOR_IDS="a,b,c"`).
fn deserialize_string_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(SerdeDeserialize)]
    #[serde(untagged)]
    enum StringOrList {
        List(Vec<String>),
        String(String),
    }

    match StringOrList::deserialize(deserializer)? {
        StringOrList::List(list) => Ok(list),
        StringOrList::String(s) => Ok(s
            .split(',')
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
            .collect()),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenTelemetryMetricsConfig {
    pub ingestion_endpoint: String,
    pub endpoint_protocol: String,
    pub batch_interval_secs: u32,
    pub global_tags: String,
    #[serde(default)]
    pub metric_deny_list: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveDataInspectConfig {
    pub refresh_in_secs: u32,
    pub records_per_topology_node: u32,
}

/// A map of named secrets whose values are masked in `Debug` output.
#[derive(Deserialize, Clone, Default)]
pub struct SecretMap(HashMap<String, String>);

impl std::fmt::Debug for SecretMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let masked: HashMap<&str, &str> = self.0.keys().map(|k| (k.as_str(), "*****")).collect();
        f.debug_tuple("SecretMap").field(&masked).finish()
    }
}

impl std::ops::Deref for SecretMap {
    type Target = HashMap<String, String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub application_id: String,
    pub log_format: String,
    pub pipeline_definition_location: String,
    pub record_batch_interval_ms: u64,
    pub record_batch_size: u32,
    pub internal_buffer_size: u32,
    // Note: only used for integration tests
    // Some supported sinks will stop polling after this many records have been processed
    pub num_records_before_stop: Option<u64>,
    pub checkpoint_interval_sec: u64,
    #[serde(default)]
    pub enforce_primary_keys: bool,
    pub state_backend: StateBackendConfig,
    pub dynamic_table_backend: DynamicTableBackendConfig,

    pub external_http_handler: ExternalHttpHandlerConfig,
    pub wasm_script: WasmScriptConfig,
    pub plugin: PluginConfig,
    pub kafka_source: KafkaConfig,
    pub kafka_sink: KafkaConfig,
    pub clickhouse_source: ClickHouseSourceConfig,
    pub clickhouse_sink: ClickHouseSinkConfig,
    pub print_sink: PrintSinkConfig,
    pub postgres_sink: PostgresSinkConfig,
    pub open_telemetry_metrics: OpenTelemetryMetricsConfig,
    pub live_data_inspect_enabled: bool,
    pub live_data_inspect: LiveDataInspectConfig,
    pub admin_api_port: u16,
    /// Preview-only "tolerant mode": on a fatal pipeline error, keep the
    /// process and the admin API alive (serving the pre-crash live-data
    /// buffers plus the node-attributed error) instead of exiting. The
    /// process no longer processes data — it is a terminal
    /// "failed but inspectable" state, bounded by the preview TTL. Never
    /// set this on production pipelines: it deliberately trades fail-fast
    /// restarts for inspectability.
    #[serde(default)]
    pub preview_tolerant_mode: bool,
    /// HTTP header names for named secrets injected by the cloud deployment system.
    ///
    /// Populated from `STREAMLING__HTTP_SECRET_HEADER__<NAME>` environment variables (the config
    /// crate lowercases key names, so `STREAMLING__HTTP_SECRET_HEADER__MY_TOKEN` → `http_secret_header["my_token"]`).
    /// Referenced in pipeline YAML via `secret_name` on webhook/handler nodes.
    #[serde(default)]
    pub http_secret_header: SecretMap,
    /// HTTP header values for named secrets injected by the cloud deployment system.
    ///
    /// Populated from `STREAMLING__HTTP_SECRET_VALUE__<NAME>` environment variables.
    #[serde(default)]
    pub http_secret_value: SecretMap,
    /// Per-sink Postgres connections, keyed by normalized sink node name
    /// (see [`normalize_sink_key`]). Populated from
    /// `STREAMLING__POSTGRES_SINK_CONNECTIONS__<SINK_NAME>__*` env vars; read
    /// via [`AppConfig::postgres_sink_for`].
    #[serde(default)]
    pub postgres_sink_connections: HashMap<String, PostgresSinkConnection>,
    #[serde(default)]
    pub test_settings: TestSettings,
    /// When true, hybrid sources terminate after all bounded phases complete
    /// instead of transitioning to the unbounded source. Defaults to false.
    /// Set via STREAMLING__JOB_MODE env var by streamling-agent when `job: true`.
    #[serde(default)]
    pub job_mode: bool,
}

impl AppConfig {
    /// By default, the application ID is used as the namespace for the state backend.
    /// This can be changed in the future, but be aware that this is a breaking change, and
    /// it would affect existing state.
    pub fn state_backend_namespace(&self) -> &str {
        self.application_id.as_str()
    }

    /// Connection for the `postgres` / `postgres_aggregate` sink node named
    /// `sink_name`: the global `postgres_sink` block with that sink's own
    /// overrides applied. Sinks without an override keep the global connection,
    /// which is the single-destination behavior every pipeline had before
    /// STRM-6516.
    pub fn postgres_sink_for(&self, sink_name: &str) -> PostgresSinkConfig {
        let mut config = self.postgres_sink.clone();
        if let Some(connection) = self
            .postgres_sink_connections
            .get(&normalize_sink_key(sink_name))
        {
            connection.apply_to(&mut config);
        }
        config
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct TestSettings {
    pub telemetry_assertions_enabled: bool,
    pub prometheus_max_attempts: u32,
    pub prometheus_sleep_ms: u64,
}

impl Default for TestSettings {
    fn default() -> Self {
        Self {
            telemetry_assertions_enabled: true,
            prometheus_max_attempts: 30,
            prometheus_sleep_ms: 500,
        }
    }
}

/// Default configuration compiled into the binary so streamling runs with no
/// external config file present. Precedence (low -> high):
///   embedded defaults < optional external file < STREAMLING__* env vars.
const EMBEDDED_DEFAULT_CONFIG: &str = include_str!("../default_config.yaml");

impl AppConfig {
    /// Load using only the embedded defaults plus environment overrides.
    /// No external file is consulted.
    pub fn load() -> anyhow::Result<Self> {
        Self::build(None)
    }

    /// Load embedded defaults, then layer an optional external file on top
    /// (ignored if absent), then environment overrides.
    ///
    /// `config_path` is passed to `File::with_name`, which resolves it relative
    /// to the current working directory and auto-appends a known extension
    /// (.yaml/.yml/.json). A missing file is not an error.
    pub fn load_from_path(config_path: &str) -> anyhow::Result<Self> {
        Self::build(Some(config_path))
    }

    fn build(external_path: Option<&str>) -> anyhow::Result<Self> {
        let mut builder =
            Config::builder().add_source(File::from_str(EMBEDDED_DEFAULT_CONFIG, FileFormat::Yaml));
        if let Some(path) = external_path {
            builder = builder.add_source(File::with_name(path).required(false));
        }
        let config = builder
            .add_source(Environment::with_prefix("streamling").separator("__"))
            .build()
            .context("failed to build configuration")?;
        let app_config: AppConfig = config
            .try_deserialize()
            .context("failed to deserialize configuration")?;
        app_config
            .state_backend
            .validate()
            .context("invalid state backend configuration")?;
        app_config.clickhouse_sink.parsed_batch_flush_interval()?;
        Ok(app_config.apply_env_overrides())
    }

    fn apply_env_overrides(mut self) -> Self {
        if let Ok(value) = env::var("STREAMLING_ENABLE_TELEMETRY_ASSERTIONS")
            && let Some(flag) = parse_env_bool(&value)
        {
            self.test_settings.telemetry_assertions_enabled = flag;
        }
        if let Ok(value) = env::var("STREAMLING_TEST_PROMETHEUS_MAX_ATTEMPTS")
            && let Ok(parsed) = value.parse::<u32>()
        {
            self.test_settings.prometheus_max_attempts = parsed;
        }
        if let Ok(value) = env::var("STREAMLING_TEST_PROMETHEUS_SLEEP_MS")
            && let Ok(parsed) = value.parse::<u64>()
        {
            self.test_settings.prometheus_sleep_ms = parsed;
        }
        self
    }
}

fn parse_env_bool(value: &str) -> Option<bool> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::Config;

    // Serialize env-var mutation across every test that touches the process environment.
    static ENV_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Verifies that the config crate can populate a HashMap<String, String> field from values
    /// nested under a key — which is the mechanism used to populate AppConfig::http_secret_header and
    /// AppConfig::http_secret_value from STREAMLING__HTTP_SECRET_HEADER__* / STREAMLING__HTTP_SECRET_VALUE__*
    /// environment variables.
    #[test]
    fn test_secret_hashmap_populated_from_config_values() {
        #[derive(serde_derive::Deserialize)]
        struct TestConfig {
            #[serde(default)]
            http_secret_header: HashMap<String, String>,
            #[serde(default)]
            http_secret_value: HashMap<String, String>,
        }

        let config = Config::builder()
            .set_override("http_secret_header.my_token", "Authorization")
            .unwrap()
            .set_override("http_secret_value.my_token", "Bearer abc123")
            .unwrap()
            .build()
            .unwrap();

        let test: TestConfig = config.try_deserialize().unwrap();
        assert_eq!(
            test.http_secret_header.get("my_token"),
            Some(&"Authorization".to_string())
        );
        assert_eq!(
            test.http_secret_value.get("my_token"),
            Some(&"Bearer abc123".to_string())
        );
    }

    /// The embedded defaults, with no external file and no env overrides — a
    /// deterministic starting point for connection-resolution tests.
    fn base_app_config() -> AppConfig {
        Config::builder()
            .add_source(File::from_str(EMBEDDED_DEFAULT_CONFIG, FileFormat::Yaml))
            .build()
            .expect("embedded default config must build")
            .try_deserialize()
            .expect("embedded default config must deserialize into AppConfig")
    }

    #[test]
    fn normalize_sink_key_matches_config_crate_key_shape() {
        // The config crate lowercases and splits on `__`, so every rendering of
        // one sink name has to land on the same key.
        assert_eq!(normalize_sink_key("postgres_prod_txs"), "postgres_prod_txs");
        assert_eq!(normalize_sink_key("Postgres-Prod-Txs"), "postgres_prod_txs");
        assert_eq!(
            normalize_sink_key("postgres__prod__txs"),
            "postgres_prod_txs"
        );
        assert_eq!(normalize_sink_key("pg.prod"), "pg_prod");
        // No empty leading or trailing segment: the config crate would split the
        // env name around it and the key would be unreachable.
        assert_eq!(normalize_sink_key("-pg-prod-"), "pg_prod");
    }

    /// The whole point of the per-sink maps: two postgres sinks in one pipeline
    /// resolve to two different databases instead of sharing whichever secret
    /// the cloud agent flattened last (STRM-6516).
    #[test]
    fn postgres_sink_connections_resolve_per_sink() {
        let mut config = base_app_config();
        config.postgres_sink_connections.insert(
            "postgres_prod_txs".to_string(),
            PostgresSinkConnection {
                host: Some("prod.example.com".to_string()),
                db: Some("prod".to_string()),
                ..Default::default()
            },
        );
        config.postgres_sink_connections.insert(
            "postgres_dev_txs".to_string(),
            PostgresSinkConnection {
                host: Some("dev.example.com".to_string()),
                db: Some("dev".to_string()),
                pass: Some("dev-pass".to_string()),
                ..Default::default()
            },
        );

        let prod = config.postgres_sink_for("postgres_prod_txs");
        let dev = config.postgres_sink_for("postgres_dev_txs");

        assert_eq!(prod.host, "prod.example.com");
        assert_eq!(prod.db, "prod");
        assert_eq!(dev.host, "dev.example.com");
        assert_eq!(dev.db, "dev");
        // Unset fields fall back to the global block, set ones win.
        assert_eq!(prod.pass, config.postgres_sink.pass);
        assert_eq!(dev.pass, "dev-pass");
        // Non-connection knobs are never per-sink.
        assert_eq!(dev.batch_size, config.postgres_sink.batch_size);
    }

    /// A sink with no override keeps the global connection — the behavior of
    /// every pipeline deployed by an agent that does not publish per-sink keys.
    #[test]
    fn sink_without_connection_override_falls_back_to_global() {
        let config = base_app_config();

        let resolved = config.postgres_sink_for("some_sink");
        assert_eq!(resolved.host, config.postgres_sink.host);
        assert_eq!(resolved.db, config.postgres_sink.db);
    }

    /// End-to-end env path: the agent writes
    /// `STREAMLING__POSTGRES_SINK_CONNECTIONS__<SINK>__<FIELD>`, so the config
    /// crate must nest a map of structs two levels deep from those env vars.
    #[test]
    fn postgres_sink_connections_populated_from_env_vars() {
        let _guard = env_guard();

        let vars = [
            (
                "STREAMLING__POSTGRES_SINK_CONNECTIONS__POSTGRES_DEV_TXS__HOST",
                "dev.example.com",
            ),
            (
                "STREAMLING__POSTGRES_SINK_CONNECTIONS__POSTGRES_DEV_TXS__DB",
                "dev",
            ),
            (
                "STREAMLING__POSTGRES_SINK_CONNECTIONS__POSTGRES_PROD_TXS__HOST",
                "prod.example.com",
            ),
        ];
        let previous: Vec<_> = vars
            .iter()
            .map(|(name, _)| (*name, std::env::var(name).ok()))
            .collect();

        // SAFETY: we hold ENV_LOCK, serializing all env-var mutation in this test module.
        unsafe {
            for (name, value) in vars {
                std::env::set_var(name, value);
            }
        }

        let result = std::panic::catch_unwind(|| {
            #[derive(serde_derive::Deserialize)]
            struct TestConfig {
                #[serde(default)]
                postgres_sink_connections: HashMap<String, PostgresSinkConnection>,
            }
            Config::builder()
                .add_source(Environment::with_prefix("streamling").separator("__"))
                .build()
                .unwrap()
                .try_deserialize::<TestConfig>()
                .unwrap()
                .postgres_sink_connections
        });

        // SAFETY: see above.
        unsafe {
            for (name, value) in previous {
                match value {
                    Some(v) => std::env::set_var(name, v),
                    None => std::env::remove_var(name),
                }
            }
        }

        let connections = result.expect("test body panicked");
        let dev = connections
            .get("postgres_dev_txs")
            .expect("dev sink connection must be populated from env");
        assert_eq!(dev.host.as_deref(), Some("dev.example.com"));
        assert_eq!(dev.db.as_deref(), Some("dev"));
        assert_eq!(dev.user, None);
        let prod = connections
            .get("postgres_prod_txs")
            .expect("prod sink connection must be populated from env");
        assert_eq!(prod.host.as_deref(), Some("prod.example.com"));
        assert_eq!(prod.db, None);
    }

    /// The embedded defaults are what every pipeline that omits `batch_size`
    /// actually runs with, so pin them: silently reverting to the old
    /// `record_batch_size` fallback (1000) would quietly re-cap backfills.
    #[test]
    fn clickhouse_sink_batch_defaults_come_from_embedded_config() {
        let config = base_app_config();

        assert_eq!(config.clickhouse_sink.batch_size, 100_000);
        assert_eq!(config.clickhouse_sink.batch_flush_interval, "1s");
        assert!(
            humantime::parse_duration(&config.clickhouse_sink.batch_flush_interval).is_ok(),
            "the shipped default must parse as a humantime duration, or every \
             ClickHouse pipeline fails to plan"
        );
        // Flattening the connection must not shadow the connection fields.
        assert_eq!(config.clickhouse_sink.connection.database, "default");
    }

    /// `batch_size` sits behind `#[serde(flatten)]`, and env vars arrive as
    /// strings — the combination is exactly where config-rs coercion tends to
    /// break, so exercise the documented override end to end.
    #[test]
    fn clickhouse_sink_batch_defaults_are_env_overridable() {
        let _guard = env_guard();

        let vars = [
            ("STREAMLING__CLICKHOUSE_SINK__BATCH_SIZE", "250000"),
            ("STREAMLING__CLICKHOUSE_SINK__BATCH_FLUSH_INTERVAL", "500ms"),
        ];
        let previous: Vec<_> = vars
            .iter()
            .map(|(name, _)| (*name, std::env::var(name).ok()))
            .collect();

        // SAFETY: we hold ENV_LOCK, serializing all env-var mutation in this test module.
        unsafe {
            for (name, value) in vars {
                std::env::set_var(name, value);
            }
        }

        let result = std::panic::catch_unwind(|| {
            AppConfig::load().expect("embedded defaults plus env overrides must load")
        });

        // SAFETY: see above.
        unsafe {
            for (name, value) in previous {
                match value {
                    Some(v) => std::env::set_var(name, v),
                    None => std::env::remove_var(name),
                }
            }
        }

        let config = result.expect("test body panicked");
        assert_eq!(config.clickhouse_sink.batch_size, 250_000);
        assert_eq!(config.clickhouse_sink.batch_flush_interval, "500ms");
    }

    /// A typo in the interval env var must fail startup, not survive until the
    /// first ClickHouse sink is planned (which for a job-mode backfill can be
    /// minutes of source setup later).
    #[test]
    fn clickhouse_sink_rejects_unparseable_batch_flush_interval_at_load() {
        let _guard = env_guard();

        let name = "STREAMLING__CLICKHOUSE_SINK__BATCH_FLUSH_INTERVAL";
        let previous = std::env::var(name).ok();

        // SAFETY: we hold ENV_LOCK, serializing all env-var mutation in this test module.
        unsafe {
            std::env::set_var(name, "1 fortnight");
        }

        let result = std::panic::catch_unwind(AppConfig::load);

        // SAFETY: see above.
        unsafe {
            match previous {
                Some(v) => std::env::set_var(name, v),
                None => std::env::remove_var(name),
            }
        }

        let err = result
            .expect("test body panicked")
            .expect_err("an unparseable batch_flush_interval must fail the load");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("batch_flush_interval") && rendered.contains("1 fortnight"),
            "the error must name the field and the offending value, got: {rendered}"
        );
    }

    /// Verifies the full path: STREAMLING__HTTP_SECRET_HEADER__* and STREAMLING__HTTP_SECRET_VALUE__* env
    /// vars are picked up by the Environment source and land in the respective HashMaps under
    /// lowercased, normalized keys.
    #[test]
    fn test_secret_hashmap_populated_from_env_vars() {
        #[derive(serde_derive::Deserialize)]
        struct TestConfig {
            #[serde(default)]
            http_secret_header: HashMap<String, String>,
            #[serde(default)]
            http_secret_value: HashMap<String, String>,
        }

        // Serialize env-var tests to prevent concurrent mutation of the process environment.
        let _guard = env_guard();

        // Save existing values so cleanup is correct even if they were already set.
        let prev_header = std::env::var("STREAMLING__HTTP_SECRET_HEADER__MY_TOKEN").ok();
        let prev_value = std::env::var("STREAMLING__HTTP_SECRET_VALUE__MY_TOKEN").ok();

        // SAFETY: we hold ENV_LOCK, serializing all env-var mutation in this test module.
        unsafe {
            std::env::set_var("STREAMLING__HTTP_SECRET_HEADER__MY_TOKEN", "Authorization");
            std::env::set_var("STREAMLING__HTTP_SECRET_VALUE__MY_TOKEN", "Bearer abc123");
        }

        let result = std::panic::catch_unwind(|| {
            let config = Config::builder()
                .add_source(Environment::with_prefix("streamling").separator("__"))
                .build()
                .unwrap();
            config.try_deserialize::<TestConfig>().unwrap()
        });

        // Restore previous state regardless of success or failure.
        // SAFETY: see above.
        unsafe {
            match prev_header {
                Some(v) => std::env::set_var("STREAMLING__HTTP_SECRET_HEADER__MY_TOKEN", v),
                None => std::env::remove_var("STREAMLING__HTTP_SECRET_HEADER__MY_TOKEN"),
            }
            match prev_value {
                Some(v) => std::env::set_var("STREAMLING__HTTP_SECRET_VALUE__MY_TOKEN", v),
                None => std::env::remove_var("STREAMLING__HTTP_SECRET_VALUE__MY_TOKEN"),
            }
        }

        let test = result.expect("test body panicked");
        assert_eq!(
            test.http_secret_header.get("my_token"),
            Some(&"Authorization".to_string())
        );
        assert_eq!(
            test.http_secret_value.get("my_token"),
            Some(&"Bearer abc123".to_string())
        );
    }

    #[test]
    fn deserialize_string_list_accepts_yaml_list() {
        let yaml = "preprocessor_ids:\n  - a\n  - b\n  - c\nchannel_capacity: 10\n";
        let plugin: PluginConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(plugin.preprocessor_ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn deserialize_string_list_accepts_comma_separated_string() {
        let yaml = "preprocessor_ids: \"a, b ,c\"\nchannel_capacity: 10\n";
        let plugin: PluginConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(plugin.preprocessor_ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn deserialize_string_list_treats_empty_string_as_empty_list() {
        let yaml = "preprocessor_ids: \"\"\nchannel_capacity: 10\n";
        let plugin: PluginConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(plugin.preprocessor_ids.is_empty());
    }

    fn clickhouse_yaml(extra: &str) -> String {
        format!(
            r#"
url: "http://localhost:8123"
database: "default"
user: "default"
password: ""
{}
"#,
            extra
        )
    }

    #[test]
    fn clickhouse_compression_accepts_all_codecs() {
        for (s, expected) in [
            ("none", ClickHouseCompression::None),
            ("gzip", ClickHouseCompression::Gzip),
            ("zstd", ClickHouseCompression::Zstd),
            ("lz4", ClickHouseCompression::Lz4),
        ] {
            let cfg: ClickHouseConfig =
                serde_yaml::from_str(&clickhouse_yaml(&format!(r#"compression: "{}""#, s)))
                    .unwrap();
            assert_eq!(cfg.compression, expected, "input: {s:?}");
        }
    }

    #[test]
    fn clickhouse_compression_is_case_insensitive_for_env_var_compat() {
        // Env-var overrides arrive as strings; accept common casings/whitespace.
        for (s, expected) in [
            ("none", ClickHouseCompression::None),
            ("NONE", ClickHouseCompression::None),
            (" None ", ClickHouseCompression::None),
            ("gzip", ClickHouseCompression::Gzip),
            ("GZIP", ClickHouseCompression::Gzip),
            ("zstd", ClickHouseCompression::Zstd),
            ("ZSTD", ClickHouseCompression::Zstd),
            ("lz4", ClickHouseCompression::Lz4),
            ("LZ4", ClickHouseCompression::Lz4),
        ] {
            let cfg: ClickHouseConfig =
                serde_yaml::from_str(&clickhouse_yaml(&format!(r#"compression: "{}""#, s)))
                    .unwrap();
            assert_eq!(cfg.compression, expected, "input: {s:?}");
        }
    }

    #[test]
    fn clickhouse_compression_defaults_to_zstd_when_omitted() {
        let cfg: ClickHouseConfig = serde_yaml::from_str(&clickhouse_yaml("")).unwrap();
        assert_eq!(cfg.compression, ClickHouseCompression::Zstd);
    }

    #[test]
    fn clickhouse_compression_rejects_unknown_method() {
        let err =
            serde_yaml::from_str::<ClickHouseConfig>(&clickhouse_yaml(r#"compression: "brotli""#))
                .unwrap_err();
        assert!(
            err.to_string()
                .contains("expected `none`, `gzip`, `zstd`, or `lz4`"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn clickhouse_compression_rejects_bool() {
        // Only the strings `"none"` / `"gzip"` / `"zstd"` / `"lz4"` are
        // accepted. serde_yaml coerces bool to its string form on the string
        // visitor, so the failure path is the same "unknown compression"
        // branch — the point is that the bool form is not a valid codec.
        for bad in ["compression: false", "compression: true"] {
            let err = serde_yaml::from_str::<ClickHouseConfig>(&clickhouse_yaml(bad)).unwrap_err();
            assert!(
                err.to_string()
                    .contains("expected `none`, `gzip`, `zstd`, or `lz4`"),
                "unexpected error for {bad}: {err}"
            );
        }
    }

    #[test]
    fn kafka_compression_parses_known_codecs() {
        // Covers all three codecs plus case/whitespace normalization, since
        // env-var overrides arrive as strings.
        for (s, expected) in [
            ("none", KafkaCompression::None),
            ("gzip", KafkaCompression::Gzip),
            ("lz4", KafkaCompression::Lz4),
            ("NONE", KafkaCompression::None),
            (" Gzip ", KafkaCompression::Gzip),
            ("LZ4", KafkaCompression::Lz4),
        ] {
            let value: KafkaCompression =
                serde_yaml::from_str(&format!(r#""{}""#, s)).expect("should parse");
            assert_eq!(value, expected, "input: {s:?}");
        }
    }

    #[test]
    fn kafka_compression_defaults_to_lz4() {
        // Preserves the historical hardcoded producer default.
        assert_eq!(KafkaCompression::default(), KafkaCompression::Lz4);
    }

    #[test]
    fn kafka_compression_rejects_unknown_codec() {
        // snappy/zstd are intentionally not exposed yet, so they must be rejected
        // rather than silently misconfiguring the producer.
        for bad in ["snappy", "zstd", "brotli"] {
            let err = serde_yaml::from_str::<KafkaCompression>(&format!(r#""{}""#, bad))
                .expect_err("should reject");
            assert!(
                err.to_string()
                    .contains("expected `none`, `gzip`, or `lz4`"),
                "unexpected error for {bad}: {err}"
            );
        }
    }

    #[test]
    fn kafka_compression_as_str_matches_librdkafka_values() {
        assert_eq!(KafkaCompression::None.as_str(), "none");
        assert_eq!(KafkaCompression::Gzip.as_str(), "gzip");
        assert_eq!(KafkaCompression::Lz4.as_str(), "lz4");
    }

    #[test]
    fn clickhouse_compression_level_defaults_to_6() {
        let cfg: ClickHouseConfig = serde_yaml::from_str(&clickhouse_yaml("")).unwrap();
        assert_eq!(cfg.compression_level.as_u32(), 6);
    }

    #[test]
    fn clickhouse_compression_level_accepts_valid_range() {
        for level in 0..=9 {
            let cfg: ClickHouseConfig =
                serde_yaml::from_str(&clickhouse_yaml(&format!("compression_level: {}", level)))
                    .unwrap();
            assert_eq!(cfg.compression_level.as_u32(), level);
        }
    }

    #[test]
    fn clickhouse_compression_level_rejects_out_of_range() {
        let err =
            serde_yaml::from_str::<ClickHouseConfig>(&clickhouse_yaml("compression_level: 10"))
                .unwrap_err();
        assert!(
            err.to_string().contains("compression_level must be"),
            "unexpected error: {err}"
        );
    }

    /// The embedded baseline must let the binary boot with no external file on disk.
    #[test]
    fn embedded_config_loads_without_external_file() {
        let _guard = env_guard();

        let config = AppConfig::load().expect("embedded config must load");
        assert_eq!(config.application_id, "local_app_v1");

        // A missing external override file is ignored, not an error.
        let config = AppConfig::load_from_path("definitely-not-a-real-config-file")
            .expect("missing external file must be ignored");
        assert_eq!(config.application_id, "local_app_v1");
    }

    /// An external file layers on top of the embedded baseline (file > embedded).
    #[test]
    fn external_file_overrides_embedded() {
        let _guard = env_guard();

        let base = std::env::temp_dir().join("streamling_test_override_embedded");
        let yaml_path = base.with_extension("yaml");
        std::fs::write(&yaml_path, "application_id: \"from_file\"\n").unwrap();

        let config = AppConfig::load_from_path(base.to_str().unwrap())
            .expect("config with external override must load");
        std::fs::remove_file(&yaml_path).ok();

        // Overridden key wins; unspecified keys still come from the embedded baseline.
        assert_eq!(config.application_id, "from_file");
        assert_eq!(config.checkpoint_interval_sec, 5);
    }

    /// Environment variables win over both the external file and the embedded baseline
    /// (env > file > embedded).
    #[test]
    fn env_var_overrides_file_and_embedded() {
        let _guard = env_guard();

        let base = env::temp_dir().join("streamling_test_env_over_file");
        let yaml_path = base.with_extension("yaml");
        std::fs::write(&yaml_path, "application_id: \"from_file\"\n").unwrap();

        let prev = env::var("STREAMLING__APPLICATION_ID").ok();
        // SAFETY: we hold ENV_LOCK, serializing env mutation across the test module.
        unsafe {
            env::set_var("STREAMLING__APPLICATION_ID", "from_env");
        }

        let path = base.to_str().unwrap().to_string();
        let result = std::panic::catch_unwind(move || {
            AppConfig::load_from_path(&path).expect("config with env + file override must load")
        });

        // Restore the environment regardless of the outcome.
        // SAFETY: see above.
        unsafe {
            match prev {
                Some(v) => env::set_var("STREAMLING__APPLICATION_ID", v),
                None => env::remove_var("STREAMLING__APPLICATION_ID"),
            }
        }
        std::fs::remove_file(&yaml_path).ok();

        let config = result.expect("test body panicked");
        assert_eq!(config.application_id, "from_env");
    }

    #[test]
    fn test_client_statement_timeout_is_double_the_server_timeout() {
        let config = PostgresSinkConfig {
            statement_timeout_secs: 60,
            ..Default::default()
        };
        assert_eq!(
            config.client_statement_timeout(),
            Some(std::time::Duration::from_secs(120))
        );
    }

    #[test]
    fn test_client_statement_timeout_disabled_when_server_timeout_disabled() {
        let config = PostgresSinkConfig {
            statement_timeout_secs: 0,
            ..Default::default()
        };
        assert_eq!(config.client_statement_timeout(), None);
    }

    #[test]
    fn test_client_statement_timeout_saturates_on_huge_values() {
        let config = PostgresSinkConfig {
            statement_timeout_secs: u64::MAX,
            ..Default::default()
        };
        assert_eq!(
            config.client_statement_timeout(),
            Some(std::time::Duration::from_secs(u64::MAX))
        );
    }
}
