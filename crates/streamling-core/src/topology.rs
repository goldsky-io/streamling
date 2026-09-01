use std::collections::{BTreeMap, HashMap};

use crate::error::ResultExt;
use config::Config;
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::de;
use serde::{Deserialize, Deserializer};
use streamling_config::{
    ClickHouseCompression, DynamicTableBackendType, GzipCompressionLevel, KafkaCompression,
};
use tracing::log::info;

// ============================================================================
// Event-time freshness configuration
// ============================================================================

/// Unit annotation for integer event-time columns. Arrow `Timestamp(_)` columns
/// are self-describing and do not require this annotation.
///
/// Note: nanoseconds is intentionally omitted — integer columns rarely carry
/// nanosecond precision in practice. Use an Arrow `Timestamp(Nanosecond)`
/// column instead, which the `EventTimeReader` handles natively.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum EventTimeUnit {
    /// Seconds since the unix epoch. Typical for blockchain
    /// `block_timestamp` / `blockTime` columns.
    Seconds,
    /// Milliseconds since the unix epoch. Match for JavaScript
    /// `Date.now()` and many HTTP-layer timestamps.
    Milliseconds,
    /// Microseconds since the unix epoch. Uncommon; normally you'll
    /// get this precision from an Arrow `Timestamp(Microsecond)` column
    /// rather than an integer.
    Microseconds,
}

/// Per-node event-time column specification.
///
/// Drives the `streamling_event_time_watermark_milliseconds` gauge and
/// `streamling_event_time_lag_milliseconds` histogram emissions. Valid at
/// sources, transforms, and sinks — each node with this configured emits
/// its own series, letting operators observe how freshness evolves across
/// pipeline stages (e.g. source watermark vs sink watermark reveals
/// transform+batch-flush delay).
#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct EventTimeConfig {
    /// Name of the node's output column carrying the event timestamp.
    pub column: String,
    /// Required for `Int64`/`UInt64` columns; ignored for Arrow `Timestamp(_)` columns.
    pub unit: Option<EventTimeUnit>,
}

/// Per-node telemetry configuration.
///
/// Groups observability-only fields that shape what metrics a node emits
/// without affecting its runtime behavior. Today: `event_time` (drives
/// `streamling_event_time_*` series) and `labels` (attaches identity tags
/// to every metric the node emits).
#[derive(Deserialize, Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Telemetry {
    pub event_time: Option<EventTimeConfig>,
    /// Author-declared identity labels. Each key/value attaches to every
    /// metric the node emits (input/output counters, elapsed-compute,
    /// checkpoint metrics, `streamling_event_time_*`). Merged into
    /// `PipelineMetricMetadata.additional_tags` at pipeline construction
    /// time and overlaid by plugin-declared labels if a plugin declares
    /// the same key (see `merge_metadata_tags`).
    pub labels: Option<BTreeMap<String, String>>,
}

impl Telemetry {
    /// Convenience accessor for the nested `event_time` config.
    pub fn event_time(&self) -> Option<&EventTimeConfig> {
        self.event_time.as_ref()
    }

    /// Convenience accessor for author-declared labels.
    pub fn labels(&self) -> Option<&BTreeMap<String, String>> {
        self.labels.as_ref()
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct HybridBoundedSource {
    pub source_type: String,
    pub table_name: String,
    pub columns: Option<String>,
    pub filter: Option<String>,
    pub start_at: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct HybridUnboundedSource {
    pub source_type: String,
    pub topic: String,
    pub filter: Option<String>,
    pub start_at: Option<String>,
    /// Mirrors `KafkaSource::validate_writer_schema_ordering` for the hybrid path.
    /// Defaults to true when omitted.
    pub validate_writer_schema_ordering: Option<bool>,
    /// Mirrors `KafkaSource::schema_id_overrides` for the hybrid path.
    pub schema_id_overrides: Option<Vec<SchemaIdOverride>>,
    /// Mirrors `KafkaSource::skip_schema_resolution` for the hybrid path.
    pub skip_schema_resolution: Option<bool>,
    /// Mirrors `KafkaSource::skip_schema_resolution_for_reader_schema_ids` for the hybrid path.
    pub skip_schema_resolution_for_reader_schema_ids: Option<Vec<u32>>,
}

/// Maps a writer schema ID embedded in a Confluent wire-format payload to a replacement ID.
///
/// Used as an escape hatch when an upstream writer schema is incompatible with the reader in
/// ways the registry's compatibility check did not flag (e.g. renamed/restructured fields
/// sharing a name across versions). Applied at decode time before Avro parsing — the bytes
/// at positions 1..5 of a payload whose embedded id matches `from` are rewritten to `to`,
/// causing the decoder to fetch and use schema `to` instead.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SchemaIdOverride {
    pub from: u32,
    pub to: u32,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct HybridOffsetTable {
    pub topic_name: String,
    pub table_name: Option<String>,
    pub topic_column: Option<String>,
    pub partition_column: Option<String>,
    pub offset_column: Option<String>,
}

impl HybridOffsetTable {
    pub fn new(topic_name: String) -> Self {
        Self {
            topic_name,
            table_name: None,
            topic_column: None,
            partition_column: None,
            offset_column: None,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct KafkaSource {
    pub topic: String,
    /// Number of concurrent consumer instances. All instances share one consumer
    /// group, so the broker assigns each a disjoint slice of the topic's
    /// partitions. Defaults to 1. Values above the topic's partition count leave
    /// the surplus instances idle.
    pub parallelism: Option<usize>,
    pub starting_offsets: Option<String>,
    pub include_metadata: Option<bool>,
    pub filter: Option<String>,
    pub primary_key: Option<String>,
    pub telemetry: Option<Telemetry>,
    pub batch_size: Option<u32>,
    pub batch_flush_interval: Option<String>,
    /// Message payload format: "avro" (default) or "json". Avro decodes via the Schema
    /// Registry; JSON decodes each payload as a UTF-8 JSON object using `schema`.
    ///
    /// The Kafka source does not support tombstone records (null/empty payloads, e.g. CDC
    /// deletes-as-tombstones) in any format — they fail the source. Represent deletes with a
    /// non-empty payload plus a `dbz.op=d` header instead.
    pub data_format: Option<String>,
    /// Input schema for JSON payloads: column name -> Arrow type string (e.g. `id: int64`,
    /// `name: string`). Required when `data_format` is "json"; rejected for Avro.
    pub schema: Option<BTreeMap<String, String>>,
    /// When true (default), the consumer compares writer/reader schema versions per subject
    /// and fails fast if the writer schema is newer than the reader's, so the pod restarts
    /// and refetches the latest schema. Set to false to skip the check
    pub validate_writer_schema_ordering: Option<bool>,
    /// Rewrite a Confluent wire-format payload's schema ID before Avro decoding.
    ///
    /// Each entry maps a writer schema ID (`from`) to a replacement ID (`to`). When a payload's
    /// embedded schema ID matches a `from`, the bytes 1..5 of the payload are patched to `to`
    /// so the decoder fetches and uses schema `to` instead. Useful as an escape hatch when an
    /// upstream writer schema is incompatible with the reader in ways the registry's
    /// compatibility check did not flag (e.g. renamed/restructured fields sharing a name across
    /// versions). Duplicate `from` values are rejected at source construction.
    pub schema_id_overrides: Option<Vec<SchemaIdOverride>>,
    /// When true, bypass Avro schema resolution unconditionally for this source — regardless of
    /// the reader schema ID selected at startup. Use only when you have manually verified that
    /// every writer schema this source will see is "close enough" to the reader for direct use.
    ///
    /// Trade-off: loses the safety net that `skip_schema_resolution_for_reader_schema_ids`
    /// provides. Prefer the per-reader-id list unless you specifically need always-skip
    /// behavior across reader-schema upgrades.
    pub skip_schema_resolution: Option<bool>,
    /// Bypass Avro schema resolution when the source's reader schema ID (selected at startup)
    /// is in this list. The decoded payload value is forwarded as-is without resolving against
    /// the reader schema. Use only when writer/reader schemas are structurally identical but
    /// Avro resolution rejects them (e.g. semantically equivalent docstring/metadata changes).
    ///
    /// Listing reader schema IDs explicitly preserves a safety net: if the registry returns a
    /// newer reader schema not in this list, skip stops applying — forcing re-verification of
    /// compatibility after schema upgrades.
    ///
    /// Combined with `skip_schema_resolution`: skip applies if either condition is true.
    pub skip_schema_resolution_for_reader_schema_ids: Option<Vec<u32>>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ClickhouseSource {
    pub table_name: String,
    pub filter: Option<String>,
    pub start_at: Option<String>,
    pub columns: Option<String>,
    pub primary_key: Option<String>,
    pub telemetry: Option<Telemetry>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct HybridSource {
    pub bounded_sources: Vec<HybridBoundedSource>,
    pub unbounded_source: HybridUnboundedSource,
    pub offset_table: Option<HybridOffsetTable>,
    pub primary_key: Option<String>,
    pub telemetry: Option<Telemetry>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PluginSource {
    pub r#type: String,
    pub options: Option<HashMap<String, serde_yaml::Value>>,
    pub primary_key: Option<String>,
    pub telemetry: Option<Telemetry>,
}

impl PluginSource {
    /// Must list exactly this struct's fields (see `merge_plugin_options`).
    /// Anything listed here is invisible to the plugin as an option;
    /// anything missing leaks a typed field into the plugin's options map.
    const TYPED_FIELDS: &'static [&'static str] = &["type", "primary_key", "telemetry"];
}

/// Source that reads files from `path` in the given `format`. `path` may be a
/// local path or a remote object store URL (`s3://`, `gs://`); remote
/// credentials come from the environment. The `mode` selects between a
/// continuous poll-for-new-files read (the default) and a bounded one-shot read.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct FileSource {
    pub path: String,
    pub format: FileSourceFormat,
    #[serde(default)]
    pub mode: FileSourceMode,
    /// Number of concurrent scan partitions the discovered files are split
    /// across. Bounded mode only; defaults to the session's target partitions.
    /// A continuous file source is single-stream (one watermark cursor) and
    /// rejects any value above 1.
    pub parallelism: Option<usize>,
    pub primary_key: Option<String>,
    pub telemetry: Option<Telemetry>,
}

/// Default discovery interval for a `Continuous` file source when none is given.
pub const DEFAULT_FILE_POLL_INTERVAL: &str = "5s";

fn default_file_poll_interval() -> String {
    DEFAULT_FILE_POLL_INTERVAL.to_string()
}

/// How the file source reads `path`.
///
/// - `Continuous` (the default) keeps polling `path` every `poll_interval`,
///   ingesting files whose `last_modified` exceeds a persisted watermark; it
///   never self-terminates and so is not allowed under `job_mode`. When the mode
///   is omitted entirely (or given without `poll_interval`), `poll_interval`
///   defaults to [`DEFAULT_FILE_POLL_INTERVAL`].
/// - `Bounded` lists the matching files once via DataFusion's `ListingTable`,
///   reads them to completion, and lets the job terminate.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum FileSourceMode {
    Bounded,
    Continuous {
        /// Discovery interval as a humantime string (e.g. `5s`, `500ms`),
        /// parsed when the source is built. Defaults to
        /// [`DEFAULT_FILE_POLL_INTERVAL`].
        #[serde(default = "default_file_poll_interval")]
        poll_interval: String,
    },
}

impl Default for FileSourceMode {
    fn default() -> Self {
        FileSourceMode::Continuous {
            poll_interval: default_file_poll_interval(),
        }
    }
}

/// Named `FileSourceFormat` to avoid colliding with DataFusion's `FileFormat`
/// trait. A closed enum so unknown formats fail at config-parse time.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileSourceFormat {
    Parquet,
    Csv,
    /// Newline-delimited JSON (one object per line / JSONL) — NOT a top-level
    /// JSON array. `ndjson` and `jsonl` are accepted as aliases.
    #[serde(alias = "ndjson", alias = "jsonl")]
    Json,
    Avro,
}

// ============================================================================
// Type-based deserialization helpers
// ============================================================================

/// Merge flattened plugin options with nested options for plugin types only.
///
/// Plugins can declare their options either at the top level of the node
/// (flat) or under an `options:` block (nested). This helper unifies both
/// into a single `options:` mapping for `serde_yaml` to bind to the
/// plugin's typed `options: HashMap<String, Value>` field.
///
/// `typed_fields` are the fields typed on THAT node struct (e.g.
/// `telemetry`, `primary_key`) and must not leak into the plugin's options
/// map. They are stripped from BOTH the flat-top-level sweep AND any
/// nested `options:` block — plugin authors sometimes place typed fields
/// inside `options:` by mistake, and silent leakage would make
/// `telemetry.labels` or `telemetry.event_time` invisible to the host
/// while the plugin received a stray options entry it doesn't understand.
///
/// The list is per node struct on purpose: a single shared list once
/// stripped `batch_size`/`batch_flush_interval` from plugin SOURCES too —
/// where no typed field exists to bind them — so a source option by either
/// name silently vanished before reaching the plugin. Only exclude a key
/// for a node when the node struct actually has that typed field.
fn merge_plugin_options(inner_mapping: &mut serde_yaml::Mapping, typed_fields: &[&str]) {
    const OPTIONS_FIELD: &str = "options";

    let excluded = |key: &str| key == OPTIONS_FIELD || typed_fields.contains(&key);

    let mut merged_options = serde_yaml::Mapping::new();

    // First, collect nested options (if any), filtering out excluded keys
    // so typed-field misplacements (e.g. `options: { telemetry: ... }`)
    // bind to the typed field after merge, not to the plugin's options map.
    if let Some(serde_yaml::Value::Mapping(options_mapping)) =
        inner_mapping.get(serde_yaml::Value::String(OPTIONS_FIELD.to_string()))
    {
        for (k, v) in options_mapping.iter() {
            if let Some(key_str) = k.as_str()
                && !excluded(key_str)
            {
                merged_options.insert(serde_yaml::Value::String(key_str.to_string()), v.clone());
            }
        }
    }

    // Then, collect flattened top-level fields of any type (these will overwrite nested options)
    for (k, v) in inner_mapping.iter() {
        if let Some(key_str) = k.as_str()
            && !excluded(key_str)
        {
            merged_options.insert(serde_yaml::Value::String(key_str.to_string()), v.clone());
        }
    }

    // Always replace the `options` field, even if the merged map is empty.
    // An unconditional write is required so that a user writing
    // `options: { labels: { ... } }` (where every nested key is excluded)
    // ends up with no `options` leak — the stale original mapping would
    // otherwise remain because the typed-fields sweep wouldn't touch it.
    // When `merged_options` is empty, we remove the key outright so the
    // typed `Option<HashMap<_, _>>` binds to `None` rather than `Some({})`.
    if merged_options.is_empty() {
        inner_mapping.remove(serde_yaml::Value::String(OPTIONS_FIELD.to_string()));
    } else {
        inner_mapping.insert(
            serde_yaml::Value::String(OPTIONS_FIELD.to_string()),
            serde_yaml::Value::Mapping(merged_options),
        );
    }
}

/// Macro that defines an enum and its type-based deserializer in one place
/// This ensures types are only registered once - in the enum definition
///
/// The category for error messages is derived from the enum name in lowercase
/// (e.g., Source → "source", Transform → "transform")
///
/// The plugin variant is always named `plugin` and uses Plugin<EnumName> as the struct
/// (e.g., for Source enum, expects PluginSource struct)
macro_rules! define_typed_enum {
    (
        $(#[$enum_meta:meta])*
        pub enum $enum_name:ident {
            $( $variant:ident ( $struct:ident ) , )*
        }
    ) => {
        // Use paste! to create Plugin<EnumName> identifier
        paste::paste! {
            $(#[$enum_meta])*
            pub enum $enum_name {
                $( $variant($struct), )*
                plugin([<Plugin $enum_name>]),
            }

            impl<'de> Deserialize<'de> for $enum_name {
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    let mut value = serde_yaml::Value::deserialize(deserializer)?;

                    // Extract the type field to determine which variant to use
                    let type_field = value
                        .get("type")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| de::Error::missing_field("type"))?;

                    // Category name is the lowercase version of the enum name
                    let category = stringify!($enum_name).to_lowercase();

                    // Match type string to variants
                    match type_field {
                        $(
                            stringify!($variant) => {
                                // Built-in type: remove the type field before deserializing
                                if let serde_yaml::Value::Mapping(ref mut map) = value {
                                    map.remove(&serde_yaml::Value::String("type".to_string()));
                                }
                                serde_yaml::from_value::<$struct>(value)
                                    .map($enum_name::$variant)
                                    .map_err(|e| {
                                        de::Error::custom(format!(
                                            "Failed to deserialize '{}' {}: {}",
                                            stringify!($variant), category, e
                                        ))
                                    })
                            }
                        )*
                        _ => {
                            // Plugin type: merge flattened options and keep the type field.
                            // The exclusion list is the node struct's own typed
                            // fields — per node, not shared (see merge_plugin_options).
                            if let serde_yaml::Value::Mapping(ref mut map) = value {
                                merge_plugin_options(map, [<Plugin $enum_name>]::TYPED_FIELDS);
                            }
                            serde_yaml::from_value::<[<Plugin $enum_name>]>(value)
                                .map($enum_name::plugin)
                                .map_err(|e| {
                                    de::Error::custom(format!(
                                        "Failed to deserialize plugin {}: {}",
                                        category, e
                                    ))
                                })
                        }
                    }
                }
            }
        }
    };
}

define_typed_enum!(
    #[allow(non_camel_case_types)]
    #[derive(Debug, Clone)]
    pub enum Source {
        kafka(KafkaSource),
        clickhouse(ClickhouseSource),
        hybrid(HybridSource),
        file(FileSource),
    }
);

impl Source {
    /// Per-source telemetry configuration, if any. Carries `event_time`
    /// (source freshness metrics) and `labels` (per-source identity tags
    /// on every metric this source emits).
    pub fn telemetry(&self) -> Option<&Telemetry> {
        match self {
            Source::kafka(s) => s.telemetry.as_ref(),
            Source::clickhouse(s) => s.telemetry.as_ref(),
            Source::hybrid(s) => s.telemetry.as_ref(),
            Source::file(s) => s.telemetry.as_ref(),
            Source::plugin(s) => s.telemetry.as_ref(),
        }
    }

    /// Requested number of concurrent instances for this source, if the source
    /// type supports more than one. Sources not listed here are structurally
    /// single-stream and have no field to set.
    pub fn parallelism(&self) -> Option<usize> {
        match self {
            Source::kafka(s) => s.parallelism,
            Source::file(s) => s.parallelism,
            Source::clickhouse(_) | Source::hybrid(_) | Source::plugin(_) => None,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct DynamicTableTransform {
    pub backend_type: DynamicTableBackendType,
    pub backend_entity_name: String,
    pub sql: Option<String>,
    pub schema: Option<String>,
    pub column: Option<String>,
    pub time_column: Option<String>,
    pub telemetry: Option<Telemetry>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct SqlTransform {
    pub primary_key: String,
    pub sql: String,
    /// Width of this transform's output: the rows are hash-partitioned by
    /// `primary_key` into this many streams, letting a narrow source feed wider
    /// downstream compute. Defaults to the input's width.
    pub parallelism: Option<usize>,
    pub telemetry: Option<Telemetry>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct HandlerTransform {
    pub primary_key: String,
    pub from: String,
    pub url: String,
    pub headers: Option<BTreeMap<String, String>>,
    pub secret_name: Option<String>,
    pub one_row_per_request: Option<bool>,
    pub payload_version: Option<u32>,
    pub schema_override: Option<BTreeMap<String, Option<String>>>,
    /// Number of concurrent request streams, hash-partitioned by `primary_key`
    /// so a key is never in flight against the endpoint twice at once.
    /// Each stream runs its own HTTP client, so in-flight requests multiply by
    /// this. Defaults to the input's width.
    pub parallelism: Option<usize>,
    pub telemetry: Option<Telemetry>,
    pub batch_size: Option<u32>,
    pub batch_flush_interval: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ScriptTransform {
    pub primary_key: String,
    pub from: String,
    pub language: String,
    pub script: String,
    pub schema: Option<BTreeMap<String, String>>,
    /// Number of concurrent WASM execution streams, hash-partitioned by
    /// `primary_key`. Each stream owns one WASM instance. Defaults to the input
    /// width.
    pub parallelism: Option<usize>,
    /// Rows accumulated per execution stream before invoking WASM.
    pub batch_size: Option<usize>,
    pub telemetry: Option<Telemetry>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct PluginTransform {
    pub r#type: String,
    pub from: String,
    pub options: Option<HashMap<String, serde_yaml::Value>>,
    pub primary_key: Option<String>,
    pub telemetry: Option<Telemetry>,
    pub batch_size: Option<u32>,
    pub batch_flush_interval: Option<String>,
}

impl PluginTransform {
    /// See `PluginSource::TYPED_FIELDS`.
    const TYPED_FIELDS: &'static [&'static str] = &[
        "type",
        "from",
        "primary_key",
        "telemetry",
        "batch_size",
        "batch_flush_interval",
    ];
}

define_typed_enum!(
    #[allow(non_camel_case_types)]
    #[derive(Debug, Clone)]
    pub enum Transform {
        dynamic_table(DynamicTableTransform),
        sql(SqlTransform),
        handler(HandlerTransform),
        script(ScriptTransform),
    }
);

impl Transform {
    /// Per-transform telemetry configuration, if any. Useful for measuring
    /// end-to-end lag across pipeline stages — configure at source, transform,
    /// and sink to see how much delay each stage adds.
    pub fn telemetry(&self) -> Option<&Telemetry> {
        match self {
            Transform::dynamic_table(t) => t.telemetry.as_ref(),
            Transform::sql(t) => t.telemetry.as_ref(),
            Transform::handler(t) => t.telemetry.as_ref(),
            Transform::script(t) => t.telemetry.as_ref(),
            Transform::plugin(t) => t.telemetry.as_ref(),
        }
    }

    /// Requested output width for this transform.
    ///
    /// `plugin` and `dynamic_table` are `SinglePartition` operators.
    pub fn parallelism(&self) -> Option<usize> {
        match self {
            Transform::sql(t) => t.parallelism,
            Transform::handler(t) => t.parallelism,
            Transform::script(t) => t.parallelism,
            Transform::dynamic_table(_) | Transform::plugin(_) => None,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct WebhookSink {
    pub from: String,
    pub url: String,
    pub headers: Option<BTreeMap<String, String>>,
    pub secret_name: Option<String>,
    pub one_row_per_request: Option<bool>,
    pub payload_version: Option<u32>,
    pub skip_on_error: Option<bool>,
    pub primary_key: Option<String>,
    /// Number of concurrent write streams, keyed by `primary_key` so a key is
    /// never delivered by two streams at once — the payload carries a per-row
    /// op, so a receiver applying upserts and deletes depends on that ordering.
    /// In-flight HTTP requests against the endpoint multiply by this. Defaults
    /// to the input's width.
    pub parallelism: Option<usize>,
    pub telemetry: Option<Telemetry>,
    pub batch_size: Option<u32>,
    pub batch_flush_interval: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PrintSink {
    pub from: String,
    /// Number of concurrent write streams. Rows are dealt out round-robin
    /// rather than by key: this sink neither dedupes nor depends on ordering,
    /// so it needs no primary key to parallelize. Output from the streams
    /// interleaves.
    pub parallelism: Option<usize>,
    pub sample_every: Option<u32>,
    pub num_records_before_stop: Option<u64>,
    pub primary_key: Option<String>,
    pub telemetry: Option<Telemetry>,
    pub batch_size: Option<u32>,
    pub batch_flush_interval: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct BlackholeSink {
    pub from: String,
    /// Number of concurrent write streams. Rows are dealt out round-robin
    /// rather than by key: this sink discards everything, so it needs no
    /// primary key to parallelize.
    pub parallelism: Option<usize>,
    pub primary_key: Option<String>,
    pub telemetry: Option<Telemetry>,
    pub batch_size: Option<u32>,
    pub batch_flush_interval: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct MemorySink {
    pub from: String,
    /// Number of concurrent write streams. Rows are dealt out round-robin
    /// rather than by key: this sink appends into a shared store and neither
    /// dedupes nor depends on ordering, so it needs no primary key to
    /// parallelize. Batch order in the store becomes nondeterministic.
    pub parallelism: Option<usize>,
    pub exclude_gs_op: Option<bool>,
    pub primary_key: Option<String>,
    pub telemetry: Option<Telemetry>,
    pub batch_size: Option<u32>,
    pub batch_flush_interval: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PostgresSink {
    pub from: String,
    pub table: String,
    pub schema: String,
    pub batch_flush_interval: Option<String>,
    /// Rows accumulated per write stream before a write is issued, so a sink
    /// with `parallelism: N` buffers up to `N * batch_size` rows.
    pub batch_size: Option<u32>,
    pub primary_key: Option<String>,
    #[serde(default = "default_on_conflict")]
    pub on_conflict: String,
    pub update_where: Option<std::collections::BTreeMap<String, String>>,
    /// Number of concurrent write streams into the table, keyed by
    /// `primary_key` so a key is never written by two streams at once.
    /// Also sizes the connection pool. Defaults to 1.
    pub parallelism: Option<usize>,
    /// When true (default), each batch is collapsed to the latest row per
    /// `primary_key` before it is written.
    pub deduplicate: Option<bool>,
    pub telemetry: Option<Telemetry>,
}

fn default_on_conflict() -> String {
    "update".to_string()
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AggregateFunction {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct GroupByColumn {
    pub from: Option<String>,
    #[serde(rename = "type")]
    pub pg_type: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct AggregateColumn {
    pub from: Option<String>,
    #[serde(rename = "fn")]
    pub function: AggregateFunction,
    #[serde(rename = "type")]
    pub pg_type: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct PostgresAggregateSink {
    pub from: String,
    /// Number of concurrent write streams into the landing table, keyed by
    /// `primary_key` so a key is never written by two streams at once.
    ///
    /// Note this parallelizes the *insert*, not the aggregation: the aggregate
    /// table is maintained by a Postgres trigger, and concurrent inserts whose
    /// rows fall in the same `group_by` bucket contend on that row.
    pub parallelism: Option<usize>,
    pub schema: String,
    pub landing_table: String,
    pub agg_table: String,
    #[serde(default)]
    pub group_by: IndexMap<String, GroupByColumn>,
    pub aggregate: IndexMap<String, AggregateColumn>,
    pub batch_flush_interval: Option<String>,
    pub batch_size: Option<u32>,
    pub primary_key: Option<String>,
    /// When true (default), each batch is collapsed to the latest row per
    /// `primary_key` before it is written.
    pub deduplicate: Option<bool>,
    pub telemetry: Option<Telemetry>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct KafkaSink {
    pub from: String,
    pub topic: String,
    pub data_format: String,
    pub topic_partitions: Option<i32>,
    pub primary_key: Option<String>,
    /// Maximum number of messages to batch before sending (maps to Kafka's batch.num.messages)
    pub batch_size: Option<u32>,
    /// Time interval to wait for batching messages (maps to Kafka's linger.ms)
    pub batch_flush_interval: Option<String>,
    /// Maximum Kafka protocol request message size in bytes (maps to message.max.bytes).
    /// Also controls the maximum individual message size.
    pub message_max_bytes: Option<u32>,
    /// Number of parallel Kafka producers. Each producer has independent connections
    /// and queues, multiplying broker throughput. Defaults to 1.
    pub parallelism: Option<usize>,
    /// Producer compression codec (`none`, `gzip`, or `lz4`). Defaults to `lz4`.
    /// Set `gzip` or `none` for brokers that reject lz4.
    #[serde(default)]
    pub compression: KafkaCompression,
    /// When true (default), each batch is collapsed to the latest row per
    /// `primary_key` before it is written.
    pub deduplicate: Option<bool>,
    pub telemetry: Option<Telemetry>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct ClickhouseSink {
    pub from: String,
    pub table: String,
    pub batch_flush_interval: Option<String>,
    /// Rows accumulated per write stream before an INSERT is issued, so a sink
    /// with `parallelism: N` buffers up to `N * batch_size` rows.
    pub batch_size: Option<u32>,
    pub primary_key: String,
    pub version_column_name: Option<String>,
    /// Number of concurrent write streams into the table, keyed by
    /// `primary_key` so a key is never written by two streams at once.
    /// Defaults to 1.
    pub parallelism: Option<usize>,
    /// When true (default), uses ReplacingMergeTree(insert_time, is_deleted) with
    /// automatic is_deleted/insert_time columns derived from _gs_op.
    /// When false, uses plain ReplacingMergeTree() with INSERT for upserts and
    /// ALTER TABLE DELETE for deletes.
    pub append_only_mode: Option<bool>,
    /// When true (default), each batch is collapsed to the latest row per
    /// `primary_key` before it is written.
    pub deduplicate: Option<bool>,
    /// Optional schema overrides for type conversions
    /// Maps column name -> ClickHouse type (e.g., "timestamp" -> "DateTime64(3)")
    #[serde(default)]
    #[serde(alias = "schema_overrides")]
    pub schema_override: Option<HashMap<String, String>>,
    /// Wire compression for INSERTs. When set, overrides the global default
    /// from `app_config.clickhouse_sink.compression`. Accepted values:
    /// `"none"` or `"gzip"`.
    #[serde(default)]
    pub compression: Option<ClickHouseCompression>,
    /// gzip compression level (0–9). When set, overrides the global default
    /// from `app_config.clickhouse_sink.compression_level`. Ignored unless
    /// `compression` resolves to `gzip`.
    #[serde(default)]
    pub compression_level: Option<GzipCompressionLevel>,
    pub telemetry: Option<Telemetry>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct PluginSink {
    pub from: String,
    pub r#type: String,
    pub options: Option<HashMap<String, serde_yaml::Value>>,
    pub primary_key: Option<String>,
    pub telemetry: Option<Telemetry>,
    pub batch_size: Option<u32>,
    pub batch_flush_interval: Option<String>,
}

impl PluginSink {
    /// See `PluginSource::TYPED_FIELDS`.
    const TYPED_FIELDS: &'static [&'static str] = &[
        "type",
        "from",
        "primary_key",
        "telemetry",
        "batch_size",
        "batch_flush_interval",
    ];
}

define_typed_enum!(
    #[allow(non_camel_case_types)]
    #[derive(Debug, Clone)]
    pub enum Sink {
        webhook(WebhookSink),
        print(PrintSink),
        blackhole(BlackholeSink),
        memory(MemorySink),
        postgres(PostgresSink),
        postgres_aggregate(PostgresAggregateSink),
        kafka(KafkaSink),
        clickhouse(ClickhouseSink),
    }
);

impl Sink {
    /// Per-sink telemetry configuration, if any. Measuring at the sink
    /// exposes end-to-end pipeline lag including transform and batch-flush
    /// delays.
    pub fn telemetry(&self) -> Option<&Telemetry> {
        match self {
            Sink::webhook(s) => s.telemetry.as_ref(),
            Sink::print(s) => s.telemetry.as_ref(),
            Sink::blackhole(s) => s.telemetry.as_ref(),
            Sink::memory(s) => s.telemetry.as_ref(),
            Sink::postgres(s) => s.telemetry.as_ref(),
            Sink::postgres_aggregate(s) => s.telemetry.as_ref(),
            Sink::kafka(s) => s.telemetry.as_ref(),
            Sink::clickhouse(s) => s.telemetry.as_ref(),
            Sink::plugin(s) => s.telemetry.as_ref(),
        }
    }

    /// Requested number of concurrent write streams for this sink.
    pub fn parallelism(&self) -> Option<usize> {
        match self {
            Sink::postgres(s) => s.parallelism,
            Sink::kafka(s) => s.parallelism,
            Sink::clickhouse(s) => s.parallelism,
            Sink::postgres_aggregate(s) => s.parallelism,
            Sink::print(s) => s.parallelism,
            Sink::blackhole(s) => s.parallelism,
            Sink::memory(s) => s.parallelism,
            Sink::webhook(s) => s.parallelism,
            Sink::plugin(_) => None,
        }
    }
}

/// Label keys that YAML `telemetry.labels` maps on sources/transforms/sinks
/// are rejected at config load because they would collide with a tag that
/// `PipelineMetricMetadata::to_tags()` always writes. Enforced by
/// [`PipelineTopology::validate_labels`].
///
/// OTEL global tags (`project_id`, `image_tag`, etc.) are deliberately not
/// reserved here — they are deployment-specific (set by the operator via
/// `STREAMLING__OPEN_TELEMETRY__METRICS__GLOBAL_TAGS`) and overlay user labels
/// at metric-record time anyway, so a YAML/global collision is silently
/// no-op'd, not corrupting.
pub(crate) const RESERVED_LABEL_KEYS: &[&str] = &[
    "id",
    "topology_node_type",
    "operator_type",
    "service_instance_id",
];

/// Allowed shape for a YAML `telemetry.labels` key. Matches Prometheus
/// label-name rules: starts with a letter or underscore, then letters,
/// digits, or underscores. Keys outside this grammar (`my dataset`,
/// `tier!`, `2024-q1`) are silently dropped or cause errors at the
/// metric backend, so we fail fast at config load with a targeted error
/// instead.
pub(crate) static LABEL_KEY_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").expect("label key regex compiles"));

/// Maximum number of labels allowed on a single node's `telemetry.labels`
/// map. Each label becomes a Prometheus label dimension on every metric the
/// node emits; Prometheus storage cost is proportional to the product of
/// dimension cardinalities, so an unbounded map is a direct operational
/// footgun. 20 is comfortably above the handful of identity labels authors
/// actually need (dataset, chain, tier, team, destination, ...) while low
/// enough that an accidental 1000-entry map fails at config load instead of
/// ballooning Prometheus in production.
pub(crate) const MAX_LABELS_PER_NODE: usize = 20;

/// Maximum length of a YAML `telemetry.labels` value, in bytes. Prometheus
/// has no hard spec limit on label values, but very long values inflate
/// scrape payloads and flag dashboards as "this is a high-cardinality
/// freeform string, not an identity dimension." 256 is the same cap common
/// APM/observability stacks apply to span attribute values.
pub(crate) const MAX_LABEL_VALUE_LEN: usize = 256;

/// Per-type label keys reserved for the given source variant. Overriding
/// these via YAML `telemetry.labels` would silently replace the real
/// identity tag (Kafka topic, ClickHouse table name, etc.) on every
/// emitted metric — dashboards filtering on the physical identifier
/// would find zero series with no diagnostic. Hybrid reserves both
/// `table` and `topic` because its bounded phase emits `table` and its
/// unbounded phase emits `topic`, and parent labels propagate to both.
fn source_per_type_reserved_keys(source: &Source) -> &'static [&'static str] {
    match source {
        Source::kafka(_) => &["topic"],
        Source::clickhouse(_) => &["table"],
        Source::hybrid(_) => &["table", "topic"],
        Source::file(_) => &["path"],
        Source::plugin(_) => &["type"],
    }
}

/// Per-type label keys reserved for the given transform variant. See
/// [`source_per_type_reserved_keys`] for rationale.
fn transform_per_type_reserved_keys(transform: &Transform) -> &'static [&'static str] {
    match transform {
        Transform::script(_) => &["language"],
        Transform::plugin(_) => &["type"],
        Transform::sql(_) | Transform::handler(_) | Transform::dynamic_table(_) => &[],
    }
}

/// Per-type label keys reserved for the given sink variant. See
/// [`source_per_type_reserved_keys`] for rationale.
fn sink_per_type_reserved_keys(sink: &Sink) -> &'static [&'static str] {
    match sink {
        Sink::webhook(_) => &["url"],
        Sink::postgres(_) | Sink::postgres_aggregate(_) | Sink::clickhouse(_) => &["table"],
        Sink::kafka(_) => &["topic"],
        Sink::plugin(_) => &["type"],
        Sink::print(_) | Sink::blackhole(_) | Sink::memory(_) => &[],
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct PipelineTopology {
    pub sources: HashMap<String, Source>,
    pub transforms: HashMap<String, Transform>,
    pub sinks: HashMap<String, Sink>,
}

impl PipelineTopology {
    pub fn load_from_string(config_str: &str) -> crate::error::Result<Self> {
        // Deserialize directly into the final structure
        // Plugin option merging happens in the custom deserializers
        let config: PipelineTopology = Config::builder()
            .add_source(config::File::from_str(config_str, config::FileFormat::Yaml))
            .build()
            .streamling_context("failed to build config")?
            .try_deserialize()
            .streamling_with_context(|| format!("failed to deserialize config:\n{}", config_str))?;
        config.validate_labels()?;
        info!("PipelineTopology: {:?}", config.redacted_for_logging());
        Ok(config)
    }

    /// Reject any YAML `telemetry.labels` entry that (a) collides with a
    /// built-in metric tag, (b) would silently override the per-type
    /// identity tag the host seeds (e.g. `topic` on a Kafka source),
    /// (c) is shaped in a way the metric backend cannot accept, (d)
    /// exceeds the per-node count cap, or (e) has a value containing
    /// control characters or exceeds the length cap. Hard error at config
    /// load rather than runtime warning — silent overrides and silently-
    /// dropped labels both lead to metrics that dashboards cannot find.
    ///
    /// `pub(crate)` because `load_from_string` is the single production
    /// entry point that calls this. Callers constructing `PipelineTopology`
    /// by struct-literal (e.g. in-crate test helpers) are responsible for
    /// supplying label-clean input or invoking this method themselves.
    pub(crate) fn validate_labels(&self) -> crate::error::Result<()> {
        for (name, source) in &self.sources {
            Self::check_labels(
                "source",
                name,
                source.telemetry().and_then(Telemetry::labels),
                source_per_type_reserved_keys(source),
            )?;
        }
        for (name, transform) in &self.transforms {
            Self::check_labels(
                "transform",
                name,
                transform.telemetry().and_then(Telemetry::labels),
                transform_per_type_reserved_keys(transform),
            )?;
        }
        for (name, sink) in &self.sinks {
            Self::check_labels(
                "sink",
                name,
                sink.telemetry().and_then(Telemetry::labels),
                sink_per_type_reserved_keys(sink),
            )?;
        }
        Ok(())
    }

    fn check_labels(
        kind: &str,
        ref_name: &str,
        labels: Option<&BTreeMap<String, String>>,
        per_type_reserved: &'static [&'static str],
    ) -> crate::error::Result<()> {
        let Some(map) = labels else {
            return Ok(());
        };
        if map.len() > MAX_LABELS_PER_NODE {
            crate::streamling_user_bail!(
                "{} '{}' declares {} labels; the per-node cap is {}. \
                 Each label attaches to every metric the node emits, so \
                 many labels multiply Prometheus storage cost. If you need \
                 more than {} dimensions, declare the truly identifying \
                 ones here and keep the rest out of metric labels.",
                kind,
                ref_name,
                map.len(),
                MAX_LABELS_PER_NODE,
                MAX_LABELS_PER_NODE
            );
        }
        for (key, value) in map {
            if RESERVED_LABEL_KEYS.contains(&key.as_str()) {
                crate::streamling_user_bail!(
                    "{} '{}' declares reserved label key '{}'. \
                     Reserved keys (which collide with built-in metric tags): {}",
                    kind,
                    ref_name,
                    key,
                    RESERVED_LABEL_KEYS.join(", ")
                );
            }
            if per_type_reserved.contains(&key.as_str()) {
                crate::streamling_user_bail!(
                    "{} '{}' declares label key '{}' which is reserved for \
                     this {} kind (the host seeds it from the node's \
                     configuration). Overriding would silently replace the \
                     real identity on every emitted metric. \
                     Reserved for this kind: {}",
                    kind,
                    ref_name,
                    key,
                    kind,
                    per_type_reserved.join(", ")
                );
            }
            if key.starts_with("__") {
                crate::streamling_user_bail!(
                    "{} '{}' declares label key '{}' with a '__' prefix. \
                     Keys starting with '__' are reserved by Prometheus for \
                     internal use (e.g. '__name__') and would shadow \
                     built-in label dimensions on scrape.",
                    kind,
                    ref_name,
                    key
                );
            }
            if !LABEL_KEY_PATTERN.is_match(key) {
                crate::streamling_user_bail!(
                    "{} '{}' declares label key '{}' with an invalid character. \
                     Label keys must match {} (Prometheus naming rule): \
                     start with a letter or underscore, followed by letters, \
                     digits, or underscores.",
                    kind,
                    ref_name,
                    key,
                    LABEL_KEY_PATTERN.as_str()
                );
            }
            if value.len() > MAX_LABEL_VALUE_LEN {
                crate::streamling_user_bail!(
                    "{} '{}' declares label '{}' with a {}-byte value; the \
                     per-label value cap is {} bytes.",
                    kind,
                    ref_name,
                    key,
                    value.len(),
                    MAX_LABEL_VALUE_LEN
                );
            }
            if let Some(bad) = value.chars().find(|c| c.is_control() && *c != '\t') {
                crate::streamling_user_bail!(
                    "{} '{}' declares label '{}' with a value containing a \
                     control character (U+{:04X}). Label values are embedded \
                     in Prometheus text-format scrape output; a raw newline \
                     or null byte can break the scrape parser or inject \
                     fake metric lines. Strip control characters before \
                     declaring the label.",
                    kind,
                    ref_name,
                    key,
                    bad as u32
                );
            }
        }
        Ok(())
    }

    pub fn redacted_for_logging(&self) -> Self {
        let mut redacted = self.clone();
        // Redact headers in transforms
        for (_name, transform) in redacted.transforms.iter_mut() {
            if let Transform::handler(handler) = transform
                && let Some(h) = handler.headers.as_mut()
            {
                for (_k, v) in h.iter_mut() {
                    *v = "[REDACTED]".to_string();
                }
            }
        }
        // Redact headers and credentials in sinks
        for (_name, sink) in redacted.sinks.iter_mut() {
            if let Sink::webhook(webhook) = sink
                && let Some(h) = webhook.headers.as_mut()
            {
                for (_k, v) in h.iter_mut() {
                    *v = "[REDACTED]".to_string();
                }
            }
        }
        redacted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kafka_sink_yaml(extra: &str) -> String {
        format!(
            r#"
from: src
topic: out
data_format: avro
{extra}
"#
        )
    }

    #[test]
    fn kafka_sink_parses_compression() {
        let sink: KafkaSink = serde_yaml::from_str(&kafka_sink_yaml("compression: gzip")).unwrap();
        assert_eq!(sink.compression, KafkaCompression::Gzip);
    }

    #[test]
    fn kafka_sink_compression_defaults_to_lz4_when_omitted() {
        // Preserves the historical producer default when YAML omits the field.
        let sink: KafkaSink = serde_yaml::from_str(&kafka_sink_yaml("")).unwrap();
        assert_eq!(sink.compression, KafkaCompression::Lz4);
    }

    #[test]
    fn test_plugin_options_nested_format() {
        // Test backwards compatible nested format
        let yaml = r#"
sources:
  test_source:
    type: test_plugin
    options:
      key1: value1
      key2: value2
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Source::plugin(plugin) = topology.sources.get("test_source").unwrap() {
            let opts = plugin.options.as_ref().unwrap();
            assert_eq!(opts.get("key1").and_then(|v| v.as_str()), Some("value1"));
            assert_eq!(opts.get("key2").and_then(|v| v.as_str()), Some("value2"));
        } else {
            panic!("Expected plugin source");
        }
    }

    #[test]
    fn test_plugin_options_flattened_format() {
        // Test new flattened format
        let yaml = r#"
sources:
  test_source:
    type: test_plugin
    key1: value1
    key2: value2
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Source::plugin(plugin) = topology.sources.get("test_source").unwrap() {
            let opts = plugin.options.as_ref().unwrap();
            assert_eq!(opts.get("key1").and_then(|v| v.as_str()), Some("value1"));
            assert_eq!(opts.get("key2").and_then(|v| v.as_str()), Some("value2"));
        } else {
            panic!("Expected plugin source");
        }
    }

    #[test]
    fn test_plugin_source_primary_key_preserved() {
        // Test that primary_key is not moved into options
        let yaml = r#"
sources:
  test_source:
    type: solana_source
    primary_key: id
    options:
      start_block: '12345'
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Source::plugin(plugin) = topology.sources.get("test_source").unwrap() {
            // primary_key should be at the top level, not in options
            assert_eq!(plugin.primary_key, Some("id".to_string()));
            let opts = plugin.options.as_ref().unwrap();
            assert!(
                opts.get("primary_key").is_none(),
                "primary_key should NOT be in options"
            );
            assert_eq!(
                opts.get("start_block").and_then(|v| v.as_str()),
                Some("12345")
            );
        } else {
            panic!("Expected plugin source");
        }
    }

    #[test]
    fn test_plugin_options_mixed_format() {
        // Test mixing both formats (flattened takes precedence if there's a conflict)
        let yaml = r#"
sources:
  test_source:
    type: test_plugin
    key1: value1_flattened
    key2: value2
    options:
      key1: value1_nested
      key3: value3
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Source::plugin(plugin) = topology.sources.get("test_source").unwrap() {
            let opts = plugin.options.as_ref().unwrap();
            // Flattened takes precedence over nested
            assert_eq!(
                opts.get("key1").and_then(|v| v.as_str()),
                Some("value1_flattened")
            );
            assert_eq!(opts.get("key2").and_then(|v| v.as_str()), Some("value2"));
            assert_eq!(opts.get("key3").and_then(|v| v.as_str()), Some("value3"));
        } else {
            panic!("Expected plugin source");
        }
    }

    #[test]
    fn test_plugin_transform_options() {
        // Test transform plugin with flattened options
        let yaml = r#"
sources: {}
transforms:
  test_transform:
    type: test_transform_plugin
    from: some_source
    transform_type: filter
    plugin_batch_size: "100"
    batch_size: 200
    batch_flush_interval: "500ms"
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Transform::plugin(plugin) = topology.transforms.get("test_transform").unwrap() {
            let opts = plugin.options.as_ref().unwrap();
            assert_eq!(
                opts.get("transform_type").and_then(|v| v.as_str()),
                Some("filter")
            );
            // Plugin-specific options are collected in the options map
            assert_eq!(
                opts.get("plugin_batch_size").and_then(|v| v.as_str()),
                Some("100")
            );
            // Generic batch config is parsed as explicit fields
            assert_eq!(plugin.batch_size, Some(200));
            assert_eq!(plugin.batch_flush_interval.as_deref(), Some("500ms"));
        } else {
            panic!("Expected plugin transform");
        }
    }

    #[test]
    fn test_plugin_sink_options() {
        // Test sink plugin with flattened options
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  test_sink:
    type: test_sink_plugin
    from: some_source
    table_name: my_table
    plugin_batch_size: "1000"
    batch_size: 500
    batch_flush_interval: "1s"
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Sink::plugin(plugin) = topology.sinks.get("test_sink").unwrap() {
            let opts = plugin.options.as_ref().unwrap();
            assert_eq!(
                opts.get("table_name").and_then(|v| v.as_str()),
                Some("my_table")
            );
            // Plugin-specific options are collected in the options map
            assert_eq!(
                opts.get("plugin_batch_size").and_then(|v| v.as_str()),
                Some("1000")
            );
            // Generic batch config is parsed as explicit fields
            assert_eq!(plugin.batch_size, Some(500));
            assert_eq!(plugin.batch_flush_interval.as_deref(), Some("1s"));
        } else {
            panic!("Expected plugin sink");
        }
    }

    // PluginSource has NO typed batch fields, so `batch_size` /
    // `batch_flush_interval` on a plugin source are ordinary plugin options
    // and must reach the plugin. A shared exclusion list once stripped them
    // here too — the option silently vanished (nothing typed existed to bind
    // it) while sibling keys passed, which is invisible until the plugin
    // misbehaves in the field.
    #[test]
    fn test_plugin_source_keeps_batch_options() {
        let yaml = r#"
sources:
  bounded_source:
    type: test_source_plugin
    options:
      start_block: "100"
      end_block: "200"
      batch_size: "1000"
    batch_flush_interval: "500ms"
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Source::plugin(plugin) = topology.sources.get("bounded_source").unwrap() {
            let opts = plugin.options.as_ref().unwrap();
            assert_eq!(
                opts.get("batch_size").and_then(|v| v.as_str()),
                Some("1000"),
                "nested batch_size must reach the plugin's options"
            );
            assert_eq!(
                opts.get("batch_flush_interval").and_then(|v| v.as_str()),
                Some("500ms"),
                "flattened batch_flush_interval must reach the plugin's options"
            );
            assert_eq!(
                opts.get("start_block").and_then(|v| v.as_str()),
                Some("100")
            );
        } else {
            panic!("Expected plugin source");
        }
    }

    #[test]
    fn test_kafka_source_missing_topic_error() {
        // Test that missing required field in kafka source gives clear error message
        let yaml = r#"
sources:
  my_source:
    type: kafka
    # missing required 'topic' field
transforms: {}
sinks: {}
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Error should mention 'kafka' not 'plugin'
        assert!(
            err_msg.contains("kafka"),
            "Error message should mention 'kafka': {}",
            err_msg
        );
    }

    #[test]
    fn test_clickhouse_sink_missing_table_error() {
        // Test that missing required field in clickhouse sink gives clear error message
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  my_sink:
    type: clickhouse
    from: some_source
    # missing required 'table' field
    primary_key: id
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Error should mention 'clickhouse' not 'plugin'
        assert!(
            err_msg.contains("clickhouse"),
            "Error message should mention 'clickhouse': {}",
            err_msg
        );
    }

    #[test]
    fn test_clickhouse_sink_supports_version_column_name() {
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  my_sink:
    type: clickhouse
    from: some_source
    table: test_output
    primary_key: id
    version_column_name: insert_timestamp
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let Sink::clickhouse(clickhouse) = topology.sinks.get("my_sink").unwrap() else {
            panic!("Expected clickhouse sink");
        };

        assert_eq!(
            clickhouse.version_column_name.as_deref(),
            Some("insert_timestamp")
        );
    }

    #[test]
    fn test_sql_transform_missing_primary_key_error() {
        // Test that missing required field in sql transform gives clear error message
        let yaml = r#"
sources: {}
transforms:
  my_transform:
    type: sql
    # missing required 'primary_key' field
    sql: "SELECT * FROM foo"
sinks: {}
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Error should mention 'sql' not 'plugin'
        assert!(
            err_msg.contains("sql"),
            "Error message should mention 'sql': {}",
            err_msg
        );
    }

    #[test]
    fn test_webhook_sink_missing_url_error() {
        // Test that missing required field in webhook sink gives clear error message
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  my_sink:
    type: webhook
    from: some_source
    # missing required 'url' field
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Error should mention 'webhook' not 'plugin'
        assert!(
            err_msg.contains("webhook"),
            "Error message should mention 'webhook': {}",
            err_msg
        );
    }

    #[test]
    fn test_kafka_source_unknown_field_error() {
        // Test that unknown/misspelled fields are rejected
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: test_topic
    topik: typo_topic  # typo - should be rejected
transforms: {}
sinks: {}
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Error should mention 'unknown field'
        assert!(
            err_msg.contains("unknown field") || err_msg.contains("topik"),
            "Error message should mention unknown field 'topik': {}",
            err_msg
        );
    }

    #[test]
    fn test_plugin_options_complex_types() {
        // Test that complex types (maps, arrays, numbers, booleans) are supported
        let yaml = r#"
sources:
  test_source:
    type: test_plugin
    string_field: simple_string
    number_field: 8080
    boolean_field: true
    array_field:
      - item1
      - item2
    map_field:
      nested_key: nested_value
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Source::plugin(plugin) = topology.sources.get("test_source").unwrap() {
            let opts = plugin.options.as_ref().unwrap();
            assert_eq!(
                opts.get("string_field").and_then(|v| v.as_str()),
                Some("simple_string")
            );
            assert_eq!(
                opts.get("number_field").and_then(|v| v.as_u64()),
                Some(8080)
            );
            assert_eq!(
                opts.get("boolean_field").and_then(|v| v.as_bool()),
                Some(true)
            );
            assert!(
                opts.get("array_field")
                    .and_then(|v| v.as_sequence())
                    .is_some()
            );
            assert!(opts.get("map_field").and_then(|v| v.as_mapping()).is_some());
        } else {
            panic!("Expected plugin source");
        }
    }

    #[test]
    fn test_plugin_validation_with_mock_struct() {
        // This test demonstrates how Phase 2 validation works with a mock plugin struct
        // (similar to how SolanaBlocksSource would work)

        #[derive(Deserialize, Debug)]
        #[serde(deny_unknown_fields)]
        struct MockSolanaSource {
            network: String,
            rpc_url: String,
            commitment: Option<String>,
        }

        // Phase 1: Parse topology with flattened format (should succeed)
        let yaml = r#"
sources:
  solana_source:
    type: solana
    network: mainnet
    rpc_url: https://api.mainnet-beta.solana.com
    commitment: confirmed
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();

        // Phase 2: Plugin deserializes from options (should succeed)
        if let Source::plugin(plugin) = topology.sources.get("solana_source").unwrap() {
            let options_value = serde_yaml::Value::Mapping(
                plugin
                    .options
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (serde_yaml::Value::String(k.clone()), v.clone()))
                    .collect(),
            );
            let result = serde_yaml::from_value::<MockSolanaSource>(options_value);
            assert!(
                result.is_ok(),
                "Valid fields should deserialize successfully"
            );
            let source = result.unwrap();
            assert_eq!(source.network, "mainnet");
            assert_eq!(source.rpc_url, "https://api.mainnet-beta.solana.com");
            assert_eq!(source.commitment, Some("confirmed".to_string()));
        }

        // Test with typo in field name - should be caught in Phase 2
        let yaml_with_typo = r#"
sources:
  solana_source:
    type: solana
    netwrok: mainnet  # Typo: should be 'network'
    rpc_url: https://api.mainnet-beta.solana.com
transforms: {}
sinks: {}
"#;
        let topology_with_typo = PipelineTopology::load_from_string(yaml_with_typo).unwrap();

        if let Source::plugin(plugin) = topology_with_typo.sources.get("solana_source").unwrap() {
            let options_value = serde_yaml::Value::Mapping(
                plugin
                    .options
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (serde_yaml::Value::String(k.clone()), v.clone()))
                    .collect(),
            );
            let result = serde_yaml::from_value::<MockSolanaSource>(options_value);
            assert!(result.is_err(), "Typo should be caught by plugin struct");
            let err_msg = result.unwrap_err().to_string();
            assert!(
                err_msg.contains("unknown field") && err_msg.contains("netwrok"),
                "Error should mention unknown field 'netwrok': {}",
                err_msg
            );
        }
    }

    // Note: Plugin validation is delegated to the actual plugin structs (e.g., SolanaBlocksSource).
    // The topology parser merges ALL top-level fields (of any type) into the options map.
    // - Field name typos (e.g., `netwrok: mainnet`) will be merged and caught by the plugin struct
    // - Field type mismatches (e.g., `port: "8080"` instead of `port: 8080`) are caught by the plugin struct
    // - The plugin struct should use #[serde(deny_unknown_fields)] for strict field name validation

    #[test]
    fn test_transform_unknown_field_error() {
        // Test that unknown fields in transforms are rejected
        let yaml = r#"
sources: {}
transforms:
  my_transform:
    type: sql
    primary_key: id
    sql: "SELECT * FROM foo"
    invalid_field: should_fail
sinks: {}
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown field") || err_msg.contains("invalid_field"),
            "Error message should mention unknown field: {}",
            err_msg
        );
    }

    #[test]
    fn test_sink_unknown_field_error() {
        // Test that unknown fields in sinks are rejected
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  my_sink:
    type: print
    from: some_source
    wrong_field: bad_value
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown field") || err_msg.contains("wrong_field"),
            "Error message should mention unknown field: {}",
            err_msg
        );
    }

    #[test]
    fn test_type_field_removed_from_deserialization() {
        // Test that the 'type' field is properly removed and doesn't cause errors
        let yaml = r#"
sources:
  test_source:
    type: kafka
    topic: test_topic
transforms:
  test_transform:
    type: sql
    primary_key: id
    sql: "SELECT * FROM test_source"
sinks:
  test_sink:
    type: print
    from: test_transform
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_ok(), "Should successfully parse with type field");
        let topology = result.unwrap();

        // Verify the source was parsed correctly
        assert!(topology.sources.contains_key("test_source"));
        if let Source::kafka(kafka) = topology.sources.get("test_source").unwrap() {
            assert_eq!(kafka.topic, "test_topic");
        } else {
            panic!("Expected kafka source");
        }

        // Verify the transform was parsed correctly
        assert!(topology.transforms.contains_key("test_transform"));
        if let Transform::sql(sql) = topology.transforms.get("test_transform").unwrap() {
            assert_eq!(sql.primary_key, "id");
            assert_eq!(sql.sql, "SELECT * FROM test_source");
        } else {
            panic!("Expected sql transform");
        }

        // Verify the sink was parsed correctly
        assert!(topology.sinks.contains_key("test_sink"));
        if let Sink::print(print) = topology.sinks.get("test_sink").unwrap() {
            assert_eq!(print.from, "test_transform");
        } else {
            panic!("Expected print sink");
        }
    }

    #[test]
    fn test_builtin_type_misspelled_field_error() {
        // Test that misspelled fields in built-in types give clear errors (not "unknown field `options`")
        let yaml = r#"
sources:
  test_source:
    type: kafka
    topik: test_topic
transforms: {}
sinks: {}
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err(), "Should fail with misspelled field");
        let err_msg = result.unwrap_err().to_string();
        // Error should mention the misspelled field 'topik', not 'options'
        assert!(
            err_msg.contains("topik") && !err_msg.contains("options"),
            "Error message should mention 'topik' and not 'options': {}",
            err_msg
        );
    }

    #[test]
    fn test_webhook_sink_secret_name_parsed() {
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  my_webhook:
    type: webhook
    from: some_source
    url: https://example.com/hook
    secret_name: my-webhook-token
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Sink::webhook(webhook) = topology.sinks.get("my_webhook").unwrap() {
            assert_eq!(webhook.secret_name, Some("my-webhook-token".to_string()));
            assert!(webhook.headers.is_none());
        } else {
            panic!("Expected webhook sink");
        }
    }

    #[test]
    fn test_webhook_sink_secret_name_and_headers_parsed() {
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  my_webhook:
    type: webhook
    from: some_source
    url: https://example.com/hook
    secret_name: my-token
    headers:
      X-Custom-Header: custom-value
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Sink::webhook(webhook) = topology.sinks.get("my_webhook").unwrap() {
            assert_eq!(webhook.secret_name, Some("my-token".to_string()));
            // headers are present alongside secret_name
            assert!(webhook.headers.is_some());
        } else {
            panic!("Expected webhook sink");
        }
    }

    #[test]
    fn test_handler_transform_secret_name_parsed() {
        let yaml = r#"
sources: {}
transforms:
  my_handler:
    type: handler
    from: some_source
    url: https://example.com/transform
    primary_key: id
    secret_name: handler-secret
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Transform::handler(handler) = topology.transforms.get("my_handler").unwrap() {
            assert_eq!(handler.secret_name, Some("handler-secret".to_string()));
        } else {
            panic!("Expected handler transform");
        }
    }

    #[test]
    fn test_webhook_sink_without_secret_name() {
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  my_webhook:
    type: webhook
    from: some_source
    url: https://example.com/hook
    headers:
      Authorization: Bearer plaintext-token
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        if let Sink::webhook(webhook) = topology.sinks.get("my_webhook").unwrap() {
            // without secret_name, the plaintext headers field is used as-is
            assert!(webhook.secret_name.is_none());
            assert!(webhook.headers.is_some());
        } else {
            panic!("Expected webhook sink");
        }
    }

    // ------------------------------------------------------------------------
    // telemetry / event_time configuration tests
    // ------------------------------------------------------------------------

    fn source_event_time(source: &Source) -> Option<&EventTimeConfig> {
        source.telemetry().and_then(Telemetry::event_time)
    }

    #[test]
    fn test_kafka_source_with_event_time_seconds() {
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    telemetry:
      event_time:
        column: block_timestamp
        unit: seconds
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        match source {
            Source::kafka(kafka) => assert_eq!(kafka.topic, "blocks"),
            other => panic!("expected kafka source, got {:?}", other),
        }
        let event_time = source_event_time(source).expect("event_time should be present");
        assert_eq!(event_time.column, "block_timestamp");
        assert_eq!(event_time.unit, Some(EventTimeUnit::Seconds));
    }

    #[test]
    fn test_clickhouse_source_with_event_time_no_unit() {
        let yaml = r#"
sources:
  my_source:
    type: clickhouse
    table_name: blocks
    telemetry:
      event_time:
        column: block_timestamp
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        match source {
            Source::clickhouse(ch) => assert_eq!(ch.table_name, "blocks"),
            other => panic!("expected clickhouse source, got {:?}", other),
        }
        let event_time = source_event_time(source).unwrap();
        assert_eq!(event_time.column, "block_timestamp");
        assert_eq!(event_time.unit, None);
    }

    #[test]
    fn test_file_source_parses_path_and_format() {
        let yaml = r#"
sources:
  events:
    type: file
    path: s3://my-bucket/events/
    format: parquet
    primary_key: id
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("events").unwrap();
        match source {
            Source::file(file) => {
                assert_eq!(file.path, "s3://my-bucket/events/");
                assert_eq!(file.format, FileSourceFormat::Parquet);
                assert_eq!(file.primary_key.as_deref(), Some("id"));
                // Continuous is the default when `mode` is omitted.
                assert_eq!(
                    file.mode,
                    FileSourceMode::Continuous {
                        poll_interval: DEFAULT_FILE_POLL_INTERVAL.to_string()
                    }
                );
            }
            other => panic!("expected file source, got {:?}", other),
        }
    }

    #[test]
    fn test_file_source_continuous_poll_interval_defaults() {
        // `mode: continuous` without `poll_interval` falls back to the default.
        let yaml = r#"
sources:
  events:
    type: file
    path: /tmp/events
    format: parquet
    mode:
      type: continuous
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        match topology.sources.get("events").unwrap() {
            Source::file(file) => assert_eq!(
                file.mode,
                FileSourceMode::Continuous {
                    poll_interval: DEFAULT_FILE_POLL_INTERVAL.to_string()
                }
            ),
            other => panic!("expected file source, got {:?}", other),
        }
    }

    #[test]
    fn test_file_source_parses_bounded_mode() {
        let yaml = r#"
sources:
  events:
    type: file
    path: /tmp/events
    format: parquet
    mode:
      type: bounded
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        match topology.sources.get("events").unwrap() {
            Source::file(file) => assert_eq!(file.mode, FileSourceMode::Bounded),
            other => panic!("expected file source, got {:?}", other),
        }
    }

    #[test]
    fn test_file_source_parses_continuous_mode() {
        let yaml = r#"
sources:
  events:
    type: file
    path: /tmp/events
    format: parquet
    mode:
      type: continuous
      poll_interval: 5s
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        match topology.sources.get("events").unwrap() {
            Source::file(file) => assert_eq!(
                file.mode,
                FileSourceMode::Continuous {
                    poll_interval: "5s".to_string()
                }
            ),
            other => panic!("expected file source, got {:?}", other),
        }
    }

    #[test]
    fn test_file_source_json_accepts_ndjson_aliases() {
        for alias in ["json", "ndjson", "jsonl"] {
            let yaml = format!(
                r#"
sources:
  events:
    type: file
    path: /tmp/events
    format: {alias}
transforms: {{}}
sinks: {{}}
"#
            );
            let topology = PipelineTopology::load_from_string(&yaml).unwrap();
            match topology.sources.get("events").unwrap() {
                Source::file(file) => assert_eq!(file.format, FileSourceFormat::Json),
                other => panic!("expected file source for alias '{alias}', got {other:?}"),
            }
        }
    }

    #[test]
    fn test_file_source_rejects_unknown_format() {
        let yaml = r#"
sources:
  events:
    type: file
    path: /tmp/events
    format: orc
transforms: {}
sinks: {}
"#;
        assert!(
            PipelineTopology::load_from_string(yaml).is_err(),
            "unknown file format should be rejected at parse time"
        );
    }

    #[test]
    fn test_plugin_source_telemetry_does_not_leak_into_options() {
        let yaml = r#"
sources:
  my_source:
    type: ethereum_source
    start_block: "12345"
    telemetry:
      event_time:
        column: block_timestamp
        unit: seconds
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        match source {
            Source::plugin(plugin) => {
                let opts = plugin.options.as_ref().unwrap();
                assert!(
                    opts.get("telemetry").is_none(),
                    "telemetry must NOT be in plugin options: {:?}",
                    opts
                );
                assert_eq!(
                    opts.get("start_block").and_then(|v| v.as_str()),
                    Some("12345")
                );
            }
            other => panic!("expected plugin source, got {:?}", other),
        }
        assert_eq!(source_event_time(source).unwrap().column, "block_timestamp");
    }

    #[test]
    fn test_source_without_telemetry_returns_none() {
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        assert!(source.telemetry().is_none());
    }

    #[test]
    fn test_event_time_unknown_field_rejected() {
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    telemetry:
      event_time:
        column: block_timestamp
        foo: bar
transforms: {}
sinks: {}
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown field") || err_msg.contains("foo"),
            "expected unknown-field error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_telemetry_unknown_field_rejected() {
        // The Telemetry wrapper itself also rejects unknown fields so typos
        // (e.g. `telemetry.labls: ...`) surface at config load time rather
        // than silently being ignored.
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    telemetry:
      event_time:
        column: block_timestamp
      bogus: value
transforms: {}
sinks: {}
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown field") || err_msg.contains("bogus"),
            "expected unknown-field error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_event_time_unit_nanoseconds_rejected() {
        // Integer-column unit enum intentionally excludes nanoseconds; users
        // wanting nanosecond precision should use Arrow Timestamp(Nanosecond).
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    telemetry:
      event_time:
        column: block_timestamp
        unit: nanoseconds
transforms: {}
sinks: {}
"#;
        let result = PipelineTopology::load_from_string(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_telemetry_does_not_collide_with_kafka_optional_fields() {
        // Regression: existing fields like include_metadata, filter, primary_key
        // still parse correctly when telemetry is also configured.
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    include_metadata: true
    filter: "number > 5"
    primary_key: id
    telemetry:
      event_time:
        column: block_timestamp
        unit: seconds
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        match source {
            Source::kafka(kafka) => {
                assert_eq!(kafka.topic, "blocks");
                assert_eq!(kafka.include_metadata, Some(true));
                assert_eq!(kafka.filter.as_deref(), Some("number > 5"));
                assert_eq!(kafka.primary_key.as_deref(), Some("id"));
            }
            other => panic!("expected kafka source, got {:?}", other),
        }
        assert!(source_event_time(source).is_some());
    }

    // ------------------------------------------------------------------------
    // telemetry.labels deserialization tests
    // ------------------------------------------------------------------------

    fn source_labels(source: &Source) -> Option<&BTreeMap<String, String>> {
        source.telemetry().and_then(Telemetry::labels)
    }

    fn transform_labels(transform: &Transform) -> Option<&BTreeMap<String, String>> {
        transform.telemetry().and_then(Telemetry::labels)
    }

    fn sink_labels(sink: &Sink) -> Option<&BTreeMap<String, String>> {
        sink.telemetry().and_then(Telemetry::labels)
    }

    #[test]
    fn test_kafka_source_with_telemetry_labels() {
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        tier: critical
        team: indexing-core
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        let labels = source_labels(source).expect("labels should be present");
        assert_eq!(labels.get("tier"), Some(&"critical".to_string()));
        assert_eq!(labels.get("team"), Some(&"indexing-core".to_string()));
    }

    #[test]
    fn test_clickhouse_source_with_telemetry_labels() {
        let yaml = r#"
sources:
  my_source:
    type: clickhouse
    table_name: blocks
    telemetry:
      labels:
        chain_slug: ethereum
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        let labels = source_labels(source).unwrap();
        assert_eq!(labels.get("chain_slug"), Some(&"ethereum".to_string()));
    }

    #[test]
    fn test_hybrid_source_with_telemetry_labels() {
        let yaml = r#"
sources:
  my_source:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: blocks_historic
    unbounded_source:
      source_type: kafka
      topic: blocks_live
    telemetry:
      labels:
        dataset: v2.evm.blocks
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        let labels = source_labels(source).unwrap();
        assert_eq!(labels.get("dataset"), Some(&"v2.evm.blocks".to_string()));
    }

    #[test]
    fn test_plugin_source_with_telemetry_labels() {
        let yaml = r#"
sources:
  my_source:
    type: ethereum_source
    start_block: "100"
    telemetry:
      labels:
        chain_slug: ethereum
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        let labels = source_labels(source).unwrap();
        assert_eq!(labels.get("chain_slug"), Some(&"ethereum".to_string()));
    }

    #[test]
    fn test_sql_transform_with_telemetry_labels() {
        let yaml = r#"
sources: {}
transforms:
  my_transform:
    type: sql
    primary_key: id
    sql: "SELECT * FROM foo"
    telemetry:
      labels:
        stage: enrichment
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let transform = topology.transforms.get("my_transform").unwrap();
        let labels = transform_labels(transform).unwrap();
        assert_eq!(labels.get("stage"), Some(&"enrichment".to_string()));
    }

    #[test]
    fn test_webhook_sink_with_telemetry_labels() {
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  my_sink:
    type: webhook
    from: foo
    url: http://example.com
    telemetry:
      labels:
        destination: customer-a
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let sink = topology.sinks.get("my_sink").unwrap();
        let labels = sink_labels(sink).unwrap();
        assert_eq!(labels.get("destination"), Some(&"customer-a".to_string()));
    }

    #[test]
    fn test_telemetry_event_time_and_labels_together() {
        // Both fields present in the same telemetry block — the common case.
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    telemetry:
      event_time:
        column: block_timestamp
        unit: seconds
      labels:
        tier: critical
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        assert_eq!(source_event_time(source).unwrap().column, "block_timestamp");
        assert_eq!(
            source_labels(source).unwrap().get("tier"),
            Some(&"critical".to_string())
        );
    }

    #[test]
    fn test_telemetry_labels_only_no_event_time() {
        // `labels` without `event_time` — should deserialize cleanly.
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        tier: critical
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        assert!(source_event_time(source).is_none());
        assert_eq!(
            source_labels(source).unwrap().get("tier"),
            Some(&"critical".to_string())
        );
    }

    #[test]
    fn test_telemetry_empty_labels_map() {
        // `labels: {}` — technically valid; merges to nothing.
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    telemetry:
      labels: {}
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        let labels = source_labels(source).unwrap();
        assert!(labels.is_empty());
    }

    #[test]
    fn test_nested_options_telemetry_misplaced_stripped() {
        // User placed `telemetry:` inside the plugin's `options:` block
        // alongside a legitimate plugin option. The nested-sweep must
        // strip `telemetry` so it doesn't leak into the plugin's options
        // map. `labels` itself is NOT excluded — it's a legitimate
        // plugin option when it appears inside `options:`.
        let yaml = r#"
sources:
  my_source:
    type: ethereum_source
    options:
      start_block: "100"
      telemetry:
        event_time:
          column: block_timestamp
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        match source {
            Source::plugin(plugin) => {
                let opts = plugin.options.as_ref().unwrap();
                assert!(
                    opts.get("telemetry").is_none(),
                    "telemetry must NOT be in plugin options: {:?}",
                    opts
                );
                assert_eq!(
                    opts.get("start_block").and_then(|v| v.as_str()),
                    Some("100")
                );
            }
            other => panic!("expected plugin source, got {:?}", other),
        }
    }

    #[test]
    fn test_nested_options_all_excluded_keys_do_not_leak() {
        // Regression for the Cursor Bugbot-reported all-excluded guard
        // issue: when `options:` contains only excluded keys (e.g. only
        // `telemetry`), the previous `if !merged_options.is_empty()`
        // guard skipped the replacement, leaving the original stale
        // mapping intact. Result: `telemetry` ended up in the plugin's
        // options map despite being on the EXCLUDED list.
        let yaml = r#"
sources:
  my_source:
    type: ethereum_source
    options:
      telemetry:
        event_time:
          column: block_timestamp
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        match source {
            Source::plugin(plugin) => {
                // Either the options field binds to None (entire `options`
                // removed because everything in it was excluded) or to
                // `Some(empty map)` — both prove the telemetry key did
                // not leak.
                if let Some(opts) = plugin.options.as_ref() {
                    assert!(
                        opts.get("telemetry").is_none(),
                        "telemetry must NOT be in plugin options: {:?}",
                        opts
                    );
                }
            }
            other => panic!("expected plugin source, got {:?}", other),
        }
    }

    // ------------------------------------------------------------------------
    // telemetry.labels validation tests (reserved keys + Prometheus grammar)
    // ------------------------------------------------------------------------

    #[test]
    fn test_labels_valid_keys_accepted() {
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        tier: critical
        chain_slug: ethereum
        _private: yes
        team_1: indexing
transforms: {}
sinks: {}
"#;
        PipelineTopology::load_from_string(yaml).unwrap();
    }

    #[test]
    fn test_labels_reserved_key_id_rejected_on_source() {
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        id: oops
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml)
            .expect_err("reserved key `id` should be rejected");
        let msg = err.to_string();
        assert!(msg.contains("source"), "message: {msg}");
        assert!(msg.contains("my_source"), "message: {msg}");
        assert!(msg.contains("'id'"), "message: {msg}");
        assert!(msg.contains("Reserved keys"), "message: {msg}");
    }

    #[test]
    fn test_labels_reserved_key_topology_node_type_rejected() {
        let yaml = r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        topology_node_type: x
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("topology_node_type"));
    }

    #[test]
    fn test_labels_reserved_key_operator_type_rejected() {
        let yaml = r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        operator_type: kafka
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("operator_type"));
    }

    #[test]
    fn test_labels_reserved_key_service_instance_id_rejected() {
        let yaml = r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        service_instance_id: abc
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("service_instance_id"));
    }

    #[test]
    fn test_labels_reserved_key_rejected_on_transform() {
        let yaml = r#"
sources: {}
transforms:
  my_transform:
    type: sql
    primary_key: id
    sql: "SELECT 1"
    telemetry:
      labels:
        id: oops
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        let msg = err.to_string();
        assert!(msg.contains("transform"), "message: {msg}");
        assert!(msg.contains("my_transform"), "message: {msg}");
    }

    #[test]
    fn test_labels_reserved_key_rejected_on_sink() {
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  my_sink:
    type: blackhole
    from: x
    telemetry:
      labels:
        id: oops
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        let msg = err.to_string();
        assert!(msg.contains("sink"), "message: {msg}");
        assert!(msg.contains("my_sink"), "message: {msg}");
    }

    #[test]
    fn test_labels_otel_global_tag_keys_not_reserved() {
        // `project_id` and `image_tag` are intentionally NOT reserved
        // (see `RESERVED_LABEL_KEYS` doc-comment). They overlay at
        // metric-record time, so a user label with the same key is
        // silently no-op'd rather than corrupting.
        let yaml = r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        project_id: override
        image_tag: latest
transforms: {}
sinks: {}
"#;
        PipelineTopology::load_from_string(yaml).unwrap();
    }

    #[test]
    fn test_labels_per_type_key_allowed_on_non_matching_kind() {
        // Per-type reservation is scoped to the node kind that actually
        // emits the tag: `topic` is reserved on Kafka sources, but on a
        // ClickHouse source `topic` is a perfectly valid identity label
        // (it might mean "source topic we ingested from upstream").
        let yaml = r#"
sources:
  s1:
    type: clickhouse
    table_name: blocks
    telemetry:
      labels:
        topic: upstream-kafka-topic
transforms: {}
sinks: {}
"#;
        PipelineTopology::load_from_string(yaml).unwrap();
    }

    #[test]
    fn test_labels_per_type_key_rejected_on_matching_source() {
        // `topic` on a Kafka source is reserved — overriding silently
        // replaces the real topic on every metric.
        let yaml = r#"
sources:
  s1:
    type: kafka
    topic: real_topic
    telemetry:
      labels:
        topic: override
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        let msg = err.to_string();
        assert!(
            msg.contains("reserved for this source kind"),
            "message: {msg}"
        );
        assert!(msg.contains("'topic'"), "message: {msg}");
    }

    #[test]
    fn test_labels_per_type_key_rejected_on_matching_sink() {
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  s:
    type: kafka
    from: x
    topic: real_topic
    data_format: json
    telemetry:
      labels:
        topic: override
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("reserved for this sink kind"));
    }

    #[test]
    fn test_labels_per_type_key_rejected_on_hybrid_source_topic() {
        // Hybrid reserves BOTH `table` and `topic` because its bounded
        // phase emits `table` and unbounded emits `topic`, and parent
        // labels propagate to both.
        let yaml = r#"
sources:
  s1:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: blocks_historic
    unbounded_source:
      source_type: kafka
      topic: blocks_live
    telemetry:
      labels:
        topic: override
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("reserved for this source kind"));
    }

    #[test]
    fn test_labels_per_type_key_rejected_on_hybrid_source_table() {
        let yaml = r#"
sources:
  s1:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: blocks_historic
    unbounded_source:
      source_type: kafka
      topic: blocks_live
    telemetry:
      labels:
        table: override
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("reserved for this source kind"));
    }

    #[test]
    fn test_labels_per_type_key_rejected_on_plugin_source() {
        let yaml = r#"
sources:
  s1:
    type: ethereum_source
    telemetry:
      labels:
        type: override
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("reserved for this source kind"));
    }

    #[test]
    fn test_labels_per_type_key_rejected_on_script_transform() {
        let yaml = r#"
sources: {}
transforms:
  t:
    type: script
    primary_key: id
    from: x
    language: wasm
    script: ""
    telemetry:
      labels:
        language: override
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("reserved for this transform kind"));
    }

    #[test]
    fn test_labels_per_type_key_rejected_on_webhook_sink() {
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  s:
    type: webhook
    from: x
    url: http://example.com
    telemetry:
      labels:
        url: override
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("reserved for this sink kind"));
    }

    #[test]
    fn test_labels_underscore_prefix_rejected() {
        let yaml = r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        __name__: spoof
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        let msg = err.to_string();
        assert!(msg.contains("'__' prefix"), "message: {msg}");
        assert!(msg.contains("'__name__'"), "message: {msg}");
    }

    #[test]
    fn test_labels_count_cap_enforced() {
        let mut entries = String::new();
        for i in 0..21 {
            entries.push_str(&format!("        k{i}: v\n"));
        }
        let yaml = format!(
            r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
{entries}transforms: {{}}
sinks: {{}}
"#
        );
        let err = PipelineTopology::load_from_string(&yaml).expect_err("should reject");
        let msg = err.to_string();
        assert!(msg.contains("21 labels"), "message: {msg}");
        assert!(msg.contains("cap is 20"), "message: {msg}");
    }

    #[test]
    fn test_labels_count_cap_at_limit_accepted() {
        let mut entries = String::new();
        for i in 0..20 {
            entries.push_str(&format!("        k{i}: v\n"));
        }
        let yaml = format!(
            r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
{entries}transforms: {{}}
sinks: {{}}
"#
        );
        PipelineTopology::load_from_string(&yaml).unwrap();
    }

    #[test]
    fn test_labels_value_newline_rejected() {
        let yaml = "
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        env: \"prod\\nmalicious_metric{x=\\\"y\\\"} 999\"
transforms: {}
sinks: {}
";
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        let msg = err.to_string();
        assert!(msg.contains("control character"), "message: {msg}");
    }

    #[test]
    fn test_labels_value_null_byte_rejected() {
        let yaml = "
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        env: \"prod\\u0000injection\"
transforms: {}
sinks: {}
";
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("control character"));
    }

    #[test]
    fn test_labels_value_tab_accepted() {
        // Tab is a control character per Unicode but is benign in Prometheus
        // label values — used occasionally for visual alignment in generated
        // label values. We allow it explicitly.
        let yaml = "
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        env: \"prod\\tqa\"
transforms: {}
sinks: {}
";
        PipelineTopology::load_from_string(yaml).unwrap();
    }

    #[test]
    fn test_labels_value_length_cap_enforced() {
        let long = "x".repeat(257);
        let yaml = format!(
            r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        env: "{long}"
transforms: {{}}
sinks: {{}}
"#
        );
        let err = PipelineTopology::load_from_string(&yaml).expect_err("should reject");
        let msg = err.to_string();
        assert!(msg.contains("257-byte value"), "message: {msg}");
        assert!(msg.contains("cap is 256"), "message: {msg}");
    }

    #[test]
    fn test_labels_value_length_at_cap_accepted() {
        let at_cap = "x".repeat(256);
        let yaml = format!(
            r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        env: "{at_cap}"
transforms: {{}}
sinks: {{}}
"#
        );
        PipelineTopology::load_from_string(&yaml).unwrap();
    }

    #[test]
    fn test_labels_invalid_grammar_space_rejected() {
        let yaml = r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        "my dataset": bad
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        let msg = err.to_string();
        assert!(msg.contains("invalid character"), "message: {msg}");
        assert!(msg.contains("my dataset"), "message: {msg}");
    }

    #[test]
    fn test_labels_invalid_grammar_leading_digit_rejected() {
        let yaml = r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        "2024-q1": bad
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("invalid character"));
    }

    #[test]
    fn test_labels_invalid_grammar_punctuation_rejected() {
        let yaml = r#"
sources:
  s1:
    type: kafka
    topic: blocks
    telemetry:
      labels:
        "tier!": bad
transforms: {}
sinks: {}
"#;
        let err = PipelineTopology::load_from_string(yaml).expect_err("should reject");
        assert!(err.to_string().contains("invalid character"));
    }

    #[test]
    fn test_plugin_source_telemetry_labels_does_not_leak_into_options() {
        // Regression for the `EXCLUDED_FIELDS` sweep: plugin variants don't
        // declare `deny_unknown_fields`, so the options-merge path is the
        // sole guardrail against `telemetry` leaking into the plugin's
        // options map.
        let yaml = r#"
sources:
  my_source:
    type: ethereum_source
    start_block: "100"
    telemetry:
      labels:
        chain_slug: ethereum
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let source = topology.sources.get("my_source").unwrap();
        match source {
            Source::plugin(plugin) => {
                let opts = plugin.options.as_ref().unwrap();
                assert!(
                    opts.get("telemetry").is_none(),
                    "telemetry must NOT be in plugin options: {:?}",
                    opts
                );
                assert!(
                    opts.get("labels").is_none(),
                    "labels must NOT be in plugin options: {:?}",
                    opts
                );
            }
            other => panic!("expected plugin source, got {:?}", other),
        }
        assert_eq!(
            source_labels(source).unwrap().get("chain_slug"),
            Some(&"ethereum".to_string())
        );
    }

    #[test]
    fn kafka_source_deserializes_schema_id_overrides_and_skip_list() {
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: my_topic
    schema_id_overrides:
      - from: 10
        to: 20
      - from: 11
        to: 20
    skip_schema_resolution_for_reader_schema_ids: [30, 31]
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let Source::kafka(kafka) = topology.sources.get("my_source").unwrap() else {
            panic!("Expected kafka source");
        };
        assert_eq!(
            kafka.schema_id_overrides.as_deref(),
            Some(
                &[
                    SchemaIdOverride { from: 10, to: 20 },
                    SchemaIdOverride { from: 11, to: 20 },
                ][..]
            )
        );
        assert_eq!(
            kafka
                .skip_schema_resolution_for_reader_schema_ids
                .as_deref(),
            Some(&[30u32, 31u32][..])
        );
    }

    #[test]
    fn kafka_source_omitting_new_options_yields_none() {
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: my_topic
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let Source::kafka(kafka) = topology.sources.get("my_source").unwrap() else {
            panic!("Expected kafka source");
        };
        assert!(kafka.schema_id_overrides.is_none());
        assert!(kafka.skip_schema_resolution.is_none());
        assert!(kafka.skip_schema_resolution_for_reader_schema_ids.is_none());
    }

    #[test]
    fn kafka_source_deserializes_skip_schema_resolution_boolean() {
        let yaml = r#"
sources:
  my_source:
    type: kafka
    topic: my_topic
    skip_schema_resolution: true
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let Source::kafka(kafka) = topology.sources.get("my_source").unwrap() else {
            panic!("Expected kafka source");
        };
        assert_eq!(kafka.skip_schema_resolution, Some(true));
    }

    #[test]
    fn hybrid_unbounded_source_deserializes_schema_id_overrides_and_skip_list() {
        let yaml = r#"
sources:
  my_source:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: my_table
    unbounded_source:
      source_type: kafka
      topic: my_topic
      schema_id_overrides:
        - from: 1
          to: 2
      skip_schema_resolution_for_reader_schema_ids: [99]
transforms: {}
sinks: {}
"#;
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let Source::hybrid(hybrid) = topology.sources.get("my_source").unwrap() else {
            panic!("Expected hybrid source");
        };
        assert_eq!(
            hybrid.unbounded_source.schema_id_overrides.as_deref(),
            Some(&[SchemaIdOverride { from: 1, to: 2 }][..])
        );
        assert_eq!(
            hybrid
                .unbounded_source
                .skip_schema_resolution_for_reader_schema_ids
                .as_deref(),
            Some(&[99u32][..])
        );
    }
}
