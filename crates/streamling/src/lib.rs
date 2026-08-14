use arrow_schema::SchemaRef;
use datafusion::arrow::datatypes::ArrowNativeType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::TableProvider;
use datafusion::datasource::{ViewTable, provider_as_source};
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, dml::InsertOp};
use datafusion::physical_plan::{collect, displayable};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use streamling_config::AppConfig;
use streamling_connectors::table_providers::blackhole::BlackholeTableProvider;
use streamling_connectors::table_providers::clickhouse::ClickHouseTableProvider;
use streamling_connectors::table_providers::file::{
    FileSourceTableProvider, build_bounded_file_source_provider,
};
use streamling_connectors::table_providers::http::HttpTableProvider;
use streamling_connectors::table_providers::hybrid::HybridTableProvider;
use streamling_connectors::table_providers::kafka::{KafkaFormat, KafkaSourceTableProvider};
use streamling_connectors::table_providers::memory::MemoryTableProvider;
use streamling_connectors::table_providers::postgres::PostgresSinkTableProvider;
use streamling_connectors::table_providers::postgres::query_builder::validate_update_where;
use streamling_connectors::table_providers::print::PrintTableProvider;
use streamling_core::checkpoints::checkpoint_management::CheckpointCoordinator;
use streamling_core::error::{Result, ResultExt};
use streamling_core::node_context::{NodeContext, TopologyNodeType, init_node_registry};
use streamling_core::operators::broadcast::{MultiSinkEntry, MultiSinkLogicalNode};
use streamling_core::operators::checkpointable::CheckpointableNode;
use streamling_core::operators::external_handlers::{ExternalHandlerConfig, ExternalHandlerNode};
use streamling_core::operators::wasm_runner::WasmRunnerNode;
use streamling_core::session::{SessionManager, SubqueryHandling};
use streamling_core::topology::PipelineTopology;
use streamling_core::{streamling_bail, streamling_err, streamling_user_bail, streamling_user_err};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
// Re-export for convenience
pub use streamling_config as app_config;
use streamling_connectors::kafka::KafkaSinkTableProvider;
pub use streamling_core::topology;

// Re-export modules that tests need access to
pub use streamling_connectors::table_providers;
use streamling_core::dynamic_table::{DynamicTableBackendFactory, validate_dynamic_table_usage};
use streamling_core::dynamic_table::{
    DynamicTableRegistry, DynamicTableSideOutput, extract_expressions_from_sql_query,
};
pub use streamling_core::operators;
use streamling_core::operators::pg_aggregation::PostgresAggregator;
use streamling_core::operators::rebatch::{RebatchConfig, RebatchNode};
use streamling_core::operators::repartition::{Placement, RepartitionNode};
use streamling_core::operators::scan_sharing::SharedSourceRegistry;
use streamling_core::operators::wrapping::{WrappingNode, WrappingSourceTableProvider};

use streamling_core::telemetry::PipelineMetricMetadata;
use streamling_core::telemetry::provider::{
    metric_key, metric_key_hybrid_src_bounded, metric_key_hybrid_src_unbounded, metric_tags,
};

pub mod error_format;
mod topology_sort;
pub mod validate;
use streamling_core::plugin::operator::PluginNode;
use streamling_core::plugin::side_output::{
    register_plugin_side_outputs, shutdown_plugin_side_outputs,
};
use streamling_core::plugin::table_provider::{PluginSinkProvider, PluginSourceProvider};
use streamling_core::plugin::{
    ExecutionFuture, InitializedPlugin, create_sink_plugin, create_source_plugin,
    create_transform_plugin, terminate_all_plugins,
};
use streamling_core::side_output::SupportsSideOutputs;
use streamling_core::sql_parse::extract_table_references_from_sql;
use streamling_core::telemetry::recorder::{get_metrics_recorder, initialize_metrics_recorder};
use streamling_state::{StateBackendFactories, StateOperatorBackendFactory};

/// Represents the source of a primary key definition
#[derive(Debug, Clone, PartialEq)]
pub enum PrimaryKeySource {
    /// Primary key was explicitly defined in the topology configuration
    TopologyDefined,
    /// Primary key was inferred from schema metadata (e.g., Avro doc, ClickHouse sorting_key)
    SchemaInferred,
    /// Primary key was propagated from an upstream node
    Propagated,
}

/// Metadata about primary keys for a pipeline node
#[derive(Debug, Clone)]
pub struct PrimaryKeyMetadata {
    /// Ordered list of column names that form the primary key
    pub columns: Vec<String>,
    /// Where this primary key definition came from
    pub source: PrimaryKeySource,
    /// The reference name of the node that defined/modified this primary key
    pub reference_name: String,
}

impl PrimaryKeyMetadata {
    pub fn new(columns: Vec<String>, source: PrimaryKeySource, reference_name: String) -> Self {
        Self {
            columns,
            source,
            reference_name,
        }
    }

    pub fn from_str(csv: &str, source: PrimaryKeySource, reference_name: String) -> Self {
        let columns: Vec<String> = csv
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Self::new(columns, source, reference_name)
    }

    pub fn to_str(&self) -> String {
        self.columns.join(",")
    }

    pub fn validate_against_schema(&self, schema: &SchemaRef) -> Result<()> {
        let schema_fields: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        let missing_columns: Vec<&String> = self
            .columns
            .iter()
            .filter(|col| !schema_fields.contains(col))
            .collect();

        if !missing_columns.is_empty() {
            streamling_user_bail!(
                "Primary key validation failed for node '{}': columns {:?} not found in schema. Available columns: {:?}",
                self.reference_name,
                missing_columns,
                schema_fields
            );
        }

        Ok(())
    }
}

/// Registry for tracking primary key metadata across the pipeline
#[derive(Debug, Clone)]
pub struct PrimaryKeyRegistry {
    registry: Arc<RwLock<HashMap<String, PrimaryKeyMetadata>>>,
    enforce_primary_keys: bool,
}

impl PrimaryKeyRegistry {
    pub fn new(enforce_primary_keys: bool) -> Self {
        Self {
            registry: Arc::new(RwLock::new(HashMap::new())),
            enforce_primary_keys,
        }
    }

    /// Register primary key metadata for a node
    pub fn register(&self, reference_name: String, metadata: PrimaryKeyMetadata) {
        let mut registry = self
            .registry
            .write()
            .expect("Failed to acquire write lock on PrimaryKeyRegistry");
        registry.insert(reference_name, metadata);
    }

    /// Get primary key metadata for a node
    pub fn get(&self, reference_name: &str) -> Option<PrimaryKeyMetadata> {
        let registry = self
            .registry
            .read()
            .expect("Failed to acquire read lock on PrimaryKeyRegistry");
        registry.get(reference_name).cloned()
    }

    /// Propagate primary key from one node to another
    /// Returns the propagated metadata
    pub fn propagate(
        &self,
        from_reference_name: &str,
        to_reference_name: String,
    ) -> Result<Option<PrimaryKeyMetadata>> {
        match self.get(from_reference_name) {
            Some(source_metadata) => {
                let propagated_metadata = PrimaryKeyMetadata::new(
                    source_metadata.columns.clone(),
                    PrimaryKeySource::Propagated,
                    to_reference_name.clone(),
                );

                self.register(to_reference_name, propagated_metadata.clone());
                Ok(Some(propagated_metadata))
            }
            None => {
                if self.enforce_primary_keys {
                    streamling_user_bail!(
                        "Cannot propagate primary key to '{}': source node '{}' \
                        has no primary key registered",
                        to_reference_name,
                        from_reference_name
                    );
                } else {
                    warn!(
                        "Cannot propagate primary key to '{}': source node '{}' \
                        has no primary key registered. Continuing without primary key. \
                        Set 'enforce_primary_keys: true' for strict validation.",
                        to_reference_name, from_reference_name
                    );
                    Ok(None)
                }
            }
        }
    }

    pub fn track_primary_key_for_source(
        &self,
        primary_key: &Option<String>,
        reference_name: String,
        schema: &SchemaRef,
        primary_key_source: PrimaryKeySource,
    ) -> Result<Option<PrimaryKeyMetadata>> {
        if let Some(pk_string) = primary_key {
            let pk_metadata =
                PrimaryKeyMetadata::from_str(pk_string, primary_key_source, reference_name.clone());

            pk_metadata.validate_against_schema(schema)?;

            self.register(reference_name.clone(), pk_metadata.clone());
            Ok(Some(pk_metadata))
        } else if self.enforce_primary_keys {
            streamling_user_bail!(
                "Can't determine primary key for source '{}'! \
                You may need to define it explicitly in the topology configuration.",
                reference_name
            );
        } else {
            warn!(
                "Primary key not defined for source '{}'. \
                This may cause issues with stateful operations. \
                Consider defining it explicitly or set 'enforce_primary_keys: true' for strict validation.",
                reference_name
            );
            Ok(None)
        }
    }

    pub fn track_primary_key_for_transform_or_sink(
        &self,
        primary_key: &Option<String>,
        source_name: String,
        reference_name: String,
        schema: &SchemaRef,
    ) -> Result<Option<PrimaryKeyMetadata>> {
        let pk_metadata_opt = if let Some(pk_string) = primary_key {
            Some(PrimaryKeyMetadata::from_str(
                pk_string,
                PrimaryKeySource::TopologyDefined,
                reference_name.clone(),
            ))
        } else {
            self.propagate(source_name.as_str(), reference_name.clone())?
        };

        if let Some(pk_metadata) = &pk_metadata_opt {
            pk_metadata.validate_against_schema(schema)?;
            self.register(reference_name.clone(), pk_metadata.clone());
        }

        Ok(pk_metadata_opt)
    }

    /// Resolve the primary key for a sink that cannot operate without one and require
    /// the resolved key to be non-empty.
    ///
    /// Kafka is the motivating case (STRM-6281): without a usable key a Kafka sink either
    /// silently drops to round-robin partitioning with no log compaction, or — when an
    /// upstream node supplies an empty key string, as the abstract-dataset preprocessor
    /// does (`primary_key: ""`) for datasets that have no primary key in the CMS —
    /// crashloops at runtime with `primary key column '' not found in batch`. Validating
    /// here, *after* the source's Avro-schema discovery and after primary-key propagation,
    /// turns that runtime crashloop into an up-front failure while still accepting a key
    /// supplied by the sink config, an upstream node, or the source's Avro schema.
    ///
    /// The failure is raised as an **internal (platform) error**, not a user error: every
    /// dataset is expected to declare a primary key (in its schema or the CMS), so a
    /// missing one is a platform/dataset invariant violation rather than a customer
    /// misconfiguration. This makes `--validate` report `success: false` and tags the
    /// log as `error.internal = true` so it routes to the platform team.
    pub fn require_primary_key_for_sink(
        &self,
        sink_kind: &str,
        primary_key: &Option<String>,
        source_name: String,
        reference_name: String,
        schema: &SchemaRef,
    ) -> Result<PrimaryKeyMetadata> {
        let pk_metadata_opt = self.track_primary_key_for_transform_or_sink(
            primary_key,
            source_name,
            reference_name.clone(),
            schema,
        )?;

        match pk_metadata_opt {
            Some(pk) if !pk.columns.is_empty() => Ok(pk),
            _ => streamling_bail!(
                "{} sink '{}' requires a primary key, but none could be resolved. \
                 Every dataset is expected to declare a primary key (in the sink config, \
                 an upstream node, the source's Avro schema, or — for abstract datasets — \
                 the CMS), so this indicates a platform/dataset configuration issue rather \
                 than a user error.",
                sink_kind,
                reference_name
            ),
        }
    }
}

impl Default for PrimaryKeyRegistry {
    fn default() -> Self {
        Self::new(false)
    }
}

/// Normalizes a secret name to the key used in `AppConfig::secret`.
///
/// The config crate lowercases env var names when building the config, so
/// `STREAMLING__SECRET__MY_TOKEN` ends up as `secret["my_token"]`. This function applies the
/// same normalization to user-supplied `secret_name` values so the lookup matches.
fn normalize_secret_name(secret_name: &str) -> String {
    secret_name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Resolves a secret by name and merges it into the provided headers.
///
/// The header name and value are read from `secret_headers` and `secret_values` respectively,
/// both of which are populated by the config crate from `STREAMLING__HTTP_SECRET_HEADER__<NAME>`
/// and `STREAMLING__HTTP_SECRET_VALUE__<NAME>` environment variables.
///
/// Returns an error if:
/// - the secret header name or value is not found in the respective maps, or
/// - the caller also provided an explicit header with the same name (ambiguous configuration).
fn merge_secret_into_headers(
    node_name: &str,
    secret_name: &str,
    headers: Option<BTreeMap<String, String>>,
    secret_headers: &HashMap<String, String>,
    secret_values: &HashMap<String, String>,
) -> Result<Option<BTreeMap<String, String>>> {
    let key = normalize_secret_name(secret_name);
    let upper = secret_name
        .to_uppercase()
        .replace(|c: char| !c.is_alphanumeric(), "_");

    let header_name = secret_headers.get(&key).cloned().ok_or_else(|| {
        streamling_user_err!(
            "'{}': secret '{}' not found: set the STREAMLING__HTTP_SECRET_HEADER__{} environment variable",
            node_name,
            secret_name,
            upper
        )
    })?;

    let header_value = secret_values.get(&key).cloned().ok_or_else(|| {
        streamling_user_err!(
            "'{}': secret '{}' not found: set the STREAMLING__HTTP_SECRET_VALUE__{} environment variable",
            node_name,
            secret_name,
            upper
        )
    })?;

    let mut merged = headers.unwrap_or_default();
    if merged.keys().any(|k| k.eq_ignore_ascii_case(&header_name)) {
        return Err(streamling_user_err!(
            "'{}': cannot set both 'secret_name' and an explicit '{}' header — remove one",
            node_name,
            header_name
        ));
    }
    merged.insert(header_name, header_value);
    Ok(Some(merged))
}

/// During dry-run validation, secret-backed headers are not resolved because the validation process
/// may not have access to the env vars that are mounted for the runtime pipeline pod.
fn secret_name_to_resolve(secret_name: Option<&str>, dry_run: bool) -> Option<&str> {
    if dry_run { None } else { secret_name }
}

/// Parse an optional human-readable duration string (e.g. "1s", "500ms") into `Option<Duration>`.
fn parse_batch_flush_interval(
    interval: &Option<String>,
    context: &str,
) -> Result<Option<Duration>> {
    interval
        .as_ref()
        .map(|s| {
            humantime::parse_duration(s).streamling_with_context(|| {
                format!("{}: invalid batch_flush_interval '{}'", context, s)
            })
        })
        .transpose()
}

fn wrap_with_rebatch(
    plan: LogicalPlan,
    batch_size: Option<usize>,
    batch_flush_interval: Option<Duration>,
    name: String,
) -> LogicalPlan {
    if let Some(bs) = batch_size {
        LogicalPlan::Extension(Extension {
            node: Arc::new(RebatchNode::new(plan, bs, batch_flush_interval, name)),
        })
    } else if let Some(interval) = batch_flush_interval {
        LogicalPlan::Extension(Extension {
            node: Arc::new(RebatchNode::new(plan, usize::MAX, Some(interval), name)),
        })
    } else {
        plan
    }
}

pub struct Streamling {
    pub app_config: AppConfig,
    pub pipeline_topology: PipelineTopology,
    side_output_operators: Arc<RwLock<HashMap<String, Arc<dyn SupportsSideOutputs>>>>,
}

/// Per-sink record in the fan-in mapping. Carries the sink's reference name,
/// its `TableProvider`, and the effective `RebatchConfig` that drives the
/// sink-local `RebatchExec` inserted downstream of the broadcast (or upstream
/// of a single sink). Each entry is independent — there is no cross-sink
/// consistency requirement, because each sink gets its own rebatcher.
struct SinkEntry {
    name: String,
    provider: Arc<dyn TableProvider>,
    rebatch_config: RebatchConfig,
    /// How this sink's rows must be spread across its write streams. Keyed for
    /// order-sensitive sinks (upsert/delete, keep-last dedup), round-robin for
    /// sinks that neither dedupe nor depend on ordering, single for sinks that
    /// cannot write from more than one stream at all.
    placement: Placement,
    /// Number of concurrent write streams requested by the sink's `parallelism`.
    parallelism: Option<usize>,
}

impl SinkEntry {
    fn new(
        name: String,
        provider: Arc<dyn TableProvider>,
        rebatch_config: RebatchConfig,
        placement: Placement,
        parallelism: Option<usize>,
    ) -> Self {
        Self {
            name,
            provider,
            rebatch_config,
            placement,
            parallelism,
        }
    }
}

/// Validates every node's `parallelism` and returns the highest value declared.
///
/// There is deliberately no upper bound: the session's `target_partitions` is
/// raised to whatever the topology asks for, so the same pipeline definition
/// behaves the same on any machine. Node types that are structurally single-stream
/// are rejected outright rather than silently clamped, so the config stays honest
/// about what it will get.
fn validate_parallelism(topology: &PipelineTopology) -> Result<usize> {
    let mut max_declared = 1;

    let mut check = |kind: &str, name: &str, parallelism: Option<usize>| -> Result<()> {
        let Some(parallelism) = parallelism else {
            return Ok(());
        };
        if parallelism == 0 {
            streamling_user_bail!("{kind} '{name}': parallelism must be at least 1");
        }
        max_declared = max_declared.max(parallelism);
        Ok(())
    };

    for (name, source) in &topology.sources {
        check("source", name, source.parallelism())?;
        if let topology::Source::file(file) = source
            && file.parallelism.is_some_and(|p| p > 1)
            && matches!(file.mode, topology::FileSourceMode::Continuous { .. })
        {
            streamling_user_bail!(
                "source '{name}': a continuous file source is single-stream (one \
                 watermark cursor and one checkpoint drain point) and cannot run \
                 with parallelism > 1; use mode: bounded to read in parallel"
            );
        }
    }
    for (name, transform) in &topology.transforms {
        check("transform", name, transform.parallelism())?;
    }
    for (name, sink) in &topology.sinks {
        check("sink", name, sink.parallelism())?;
    }

    Ok(max_declared)
}

/// Decides the shared partitioning for a fan-out group of sinks.
///
/// Every sink in the group reads the same broadcast, so one exchange has to
/// serve all of them and it can only be keyed one way:
///
/// - all key-sensitive sinks agree on a key → exchange on it, and the group runs
///   as wide as the input (or as wide as the widest declared `parallelism`);
/// - no key-sensitive sinks → any placement works, so the group only gets an
///   exchange if one of them asked to be wider than its input;
/// - the sinks disagree → no single placement is correct for all of them, so the
///   group runs on one stream, which is what it did before it could be parallel.
///
/// A sink that cannot be parallelized at all short-circuits this: `MultiSinkExec`
/// spawns one `write_all` per input partition per sink, so the only way to hold
/// that sink to one write stream is to narrow the whole group.
fn wrap_multi_sink_with_repartition(
    plan: LogicalPlan,
    sinks: &[SinkEntry],
    group_name: &str,
) -> LogicalPlan {
    let single_stream_sinks: Vec<&str> = sinks
        .iter()
        .filter(|entry| matches!(entry.placement, Placement::Single))
        .map(|entry| entry.name.as_str())
        .collect();
    if !single_stream_sinks.is_empty() {
        warn!(
            "sinks [{}] share one input, but [{}] cannot write from more than one stream; \
             running the whole group on a single stream, since they all read one exchange",
            group_name,
            single_stream_sinks.join(", ")
        );
        return wrap_with_repartition(plan, &Placement::Single, None, group_name.to_string());
    }

    let mut key_sets: Vec<&Vec<String>> = sinks
        .iter()
        .filter_map(|entry| match &entry.placement {
            Placement::ByKey(columns) if !columns.is_empty() => Some(columns),
            _ => None,
        })
        .collect();
    key_sets.sort();
    key_sets.dedup();

    let parallelism = sinks.iter().filter_map(|entry| entry.parallelism).max();

    match key_sets.as_slice() {
        // Round-robin serves every sink here, since none of them cares which
        // stream a row lands on.
        [] => wrap_with_repartition(
            plan,
            &Placement::RoundRobin,
            parallelism,
            group_name.to_string(),
        ),
        [keys] => wrap_with_repartition(
            plan,
            &Placement::ByKey((*keys).clone()),
            parallelism,
            group_name.to_string(),
        ),
        _ => {
            warn!(
                "sinks [{}] share one input but declare different primary keys ({}); \
                 running them on a single stream, since one exchange cannot key for all of them",
                group_name,
                key_sets
                    .iter()
                    .map(|keys| keys.join("+"))
                    .collect::<Vec<_>>()
                    .join(" vs ")
            );
            wrap_with_repartition(
                plan,
                &Placement::RoundRobin,
                Some(1),
                group_name.to_string(),
            )
        }
    }
}

/// Inserts a transform's exchange at its *input* — directly above the scan of
/// the upstream node it reads.
fn wrap_transform_input_with_repartition(
    sql_plan: LogicalPlan,
    source_name: &str,
    placement: &Placement,
    parallelism: usize,
    name: &str,
) -> Result<LogicalPlan> {
    let mut wrapped = 0;
    let plan = sql_plan
        .transform_up(|node| {
            let reads_upstream = matches!(
                &node,
                LogicalPlan::TableScan(scan) if scan.table_name.table() == source_name
            );
            if !reads_upstream {
                return Ok(Transformed::no(node));
            }
            wrapped += 1;
            Ok(Transformed::yes(wrap_with_repartition(
                node,
                placement,
                Some(parallelism),
                name.to_string(),
            )))
        })
        .map(|transformed| transformed.data)?;

    if wrapped == 0 {
        streamling_user_bail!(
            "{name}: cannot apply parallelism {parallelism}, the transform's SQL \
             has no scan of its source '{source_name}'"
        );
    }
    Ok(plan)
}

fn pk_columns(pk_metadata: &Option<PrimaryKeyMetadata>) -> Vec<String> {
    pk_metadata
        .as_ref()
        .map(|pk| pk.columns.clone())
        .unwrap_or_default()
}

/// Wraps `plan` in a sink-edge hash exchange when the sink needs one.
///
/// A sink with a primary key needs all rows of a key on one write stream; a sink
/// asking for N streams needs the exchange to produce them. With neither, the
/// plan is returned untouched and the sink simply inherits its input's width.
/// The planner elides the node when the input already satisfies the placement.
fn wrap_with_repartition(
    plan: LogicalPlan,
    placement: &Placement,
    parallelism: Option<usize>,
    name: String,
) -> LogicalPlan {
    // Nothing to place and no width to hit: the sink just inherits its input.
    let needs_node = match placement {
        Placement::ByKey(columns) => !columns.is_empty(),
        // The node *is* the coalesce for a single-stream sink, so it always has
        // to be emitted — inheriting the input's width is exactly what it exists
        // to prevent.
        Placement::Single => true,
        Placement::RoundRobin => false,
    };
    if !needs_node && parallelism.is_none() {
        return plan;
    }
    LogicalPlan::Extension(Extension {
        node: Arc::new(RepartitionNode::new(
            plan,
            placement.clone(),
            parallelism,
            name,
        )),
    })
}

type SourceToSinkMapping = HashMap<String, (LogicalPlan, Vec<SinkEntry>)>;

/// Merge author-declared YAML `telemetry.labels` into the already-seeded per-type
/// `additional_tags` map. Reserved keys (global and per-type) are already rejected at
/// config load by `PipelineTopology::validate_labels`, so the only collisions possible
/// here are between YAML labels and per-type tags not shared with this node kind (e.g.
/// a user declaring `topic` on a ClickHouse source); YAML wins in those cases and that
/// is intentional. Called from `build_pipeline_metric_metadata`.
fn merge_labels(tags: &mut BTreeMap<String, String>, labels: Option<&BTreeMap<String, String>>) {
    if let Some(labels) = labels {
        for (k, v) in labels {
            tags.insert(k.clone(), v.clone());
        }
    }
}

impl Streamling {
    pub fn new(app_config: AppConfig, pipeline_topology: PipelineTopology) -> Self {
        Streamling {
            app_config,
            pipeline_topology,
            side_output_operators: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a per-table side-output operator.
    fn register_operator_for_side_outputs<T>(&self, table_name: &str, operator: Arc<T>)
    where
        T: SupportsSideOutputs + 'static,
    {
        let mut caps = self
            .side_output_operators
            .write()
            .expect("side outputs lock");
        caps.insert(
            table_name.to_string(),
            operator as Arc<dyn SupportsSideOutputs>,
        );
    }

    /// Lookup the side-output handle for a table.
    fn get_side_output_operator(&self, table_name: &str) -> Option<Arc<dyn SupportsSideOutputs>> {
        self.side_output_operators
            .read()
            .ok()
            .and_then(|m| m.get(table_name).cloned())
    }

    pub async fn start(&self) -> Result<()> {
        self.start_with(false).await
    }

    pub async fn start_with(&self, dry_run: bool) -> Result<()> {
        info!("Starting Streamling...");

        let app_config = self.app_config.clone();
        debug!("Application config: {:?}", app_config);
        let application_id = app_config.application_id.clone();

        let pipeline_topology = self.pipeline_topology.clone();
        info!(
            "Pipeline topology: {:?}",
            pipeline_topology.redacted_for_logging()
        );

        // Analyze topology to determine which sources and transforms have multiple consumers
        let node_consumers = Self::find_source_consumers(&pipeline_topology);
        debug!("Node consumer analysis: {:?}", node_consumers);

        let state_backend_factory =
            StateBackendFactories::new(app_config.clone().state_backend.clone())
                .map_err(|e| streamling_err!("failed to create state backend factory: {:?}", e))?;

        let dynamic_table_backend_factory =
            DynamicTableBackendFactory::new(app_config.dynamic_table_backend.clone());

        let dynamic_table_registry = DynamicTableRegistry::new();

        let max_declared_parallelism = validate_parallelism(&pipeline_topology)?;

        let session_manager = SessionManager::new(
            app_config.record_batch_size as u64,
            app_config.internal_buffer_size,
            dynamic_table_registry.clone(),
            max_declared_parallelism,
        )?;

        let pk_registry = PrimaryKeyRegistry::new(app_config.enforce_primary_keys);

        // Unified registry of initialized plugins keyed by a stable id.
        let mut plugins: BTreeMap<String, InitializedPlugin> = BTreeMap::new();

        let mut checkpoint_coordinator = CheckpointCoordinator::new();
        // Control handle shared with bounded sources (to begin the terminal
        // checkpoint) and sink futures (to signal completion). Valid before the
        // coordinator is started since it shares the coordinator's state.
        let checkpoint_control = checkpoint_coordinator.control();
        // Process-wide shutdown signal (streamling_core::shutdown). A single
        // SIGTERM/SIGINT handler flips it (installed below); every source
        // observes it and drains front-to-back, and deep call sites (sink
        // retry loops) subscribe to it directly.
        let shutdown_rx = streamling_core::shutdown::subscribe();
        let mut checkpoint_sink_names: Vec<String> = Vec::new();

        let mut pipeline_plans: HashMap<String, LogicalPlan> = HashMap::new();

        let node_contexts = Self::build_node_contexts(&pipeline_topology);
        let metric_metadata_mapping = Self::build_pipeline_metric_metadata(
            &node_contexts,
            &pipeline_topology,
            &application_id,
        );
        initialize_metrics_recorder(metric_metadata_mapping);

        let scan_sharing_registry = SharedSourceRegistry::new();

        let mut sink_futures = Vec::new();
        let mut sources_to_sinks: SourceToSinkMapping = SourceToSinkMapping::new();
        let mut hybrid_providers: Vec<(
            Arc<HybridTableProvider>,
            Arc<WrappingSourceTableProvider>,
        )> = Vec::new();

        let pipeline_topology_clone = pipeline_topology.clone();
        for (reference_name, source) in &pipeline_topology_clone.sources {
            // MultiSink already accounted for in consumer count
            let consumer_count = node_consumers.get(reference_name).copied().unwrap_or(0);
            let scan_sharing = if consumer_count > 1 {
                let metric_name = metric_key(&application_id, reference_name.as_str());
                info!(
                    "Source '{}' (metric: '{}') has {} consumers - enabling scan sharing",
                    reference_name, metric_name, consumer_count
                );
                scan_sharing_registry.set_expected_consumers(metric_name, consumer_count);
                Some(scan_sharing_registry.clone())
            } else {
                None
            };

            match source {
                topology::Source::kafka(kafka) => {
                    let ctx = node_contexts
                        .get(reference_name)
                        .expect("node context must exist");
                    let topic = &kafka.topic;
                    let starting_offsets = &kafka.starting_offsets;
                    let include_metadata = kafka.include_metadata;
                    let filter = &kafka.filter;
                    let primary_key_opt = &kafka.primary_key;

                    let record_batch_interval_ms =
                        parse_batch_flush_interval(&kafka.batch_flush_interval, reference_name)?
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(app_config.record_batch_interval_ms);
                    let record_batch_size =
                        kafka.batch_size.unwrap_or(app_config.record_batch_size);
                    let data_format: KafkaFormat =
                        kafka.data_format.as_deref().unwrap_or("avro").parse()?;
                    let kafka_source_provider = Arc::new(
                        KafkaSourceTableProvider::new(
                            reference_name.clone(),
                            metric_key(&application_id, reference_name.as_str()),
                            app_config.kafka_source.clone(),
                            topic.clone(),
                            starting_offsets.clone(),
                            filter.clone(),
                            record_batch_interval_ms,
                            record_batch_size,
                            app_config.internal_buffer_size,
                            include_metadata.unwrap_or(false),
                            state_backend_factory.create(app_config.state_backend_namespace()),
                            session_manager.clone(),
                            app_config.num_records_before_stop,
                            kafka.validate_writer_schema_ordering.unwrap_or(true),
                            kafka.schema_id_overrides.clone().unwrap_or_default(),
                            kafka.skip_schema_resolution.unwrap_or(false),
                            kafka
                                .skip_schema_resolution_for_reader_schema_ids
                                .clone()
                                .unwrap_or_default(),
                            data_format,
                            kafka.schema.clone(),
                            kafka.parallelism.unwrap_or(1),
                        )
                        // Convert via `StreamlingError::from` (not `streamling_with_context`)
                        // so a user-facing schema error (e.g. unsupported JSON dtype) is
                        // recovered from the `DataFusionError::External` wrapper and stays
                        // user-facing. Otherwise `--validate` would misreport it as internal.
                        .map_err(|e| {
                            streamling_core::error::StreamlingError::from(e)
                                .context(format!("{}: failed to create Kafka source", ctx.format()))
                        })?,
                    );
                    let extracted_pk = kafka_source_provider.get_extracted_primary_key();

                    let provider_with_telemetry = Arc::new(WrappingSourceTableProvider::new(
                        kafka_source_provider,
                        metric_key(&application_id, reference_name.as_str()),
                        scan_sharing.clone(),
                        kafka.telemetry.as_ref(),
                    ));

                    self.register_operator_for_side_outputs(
                        reference_name.as_str(),
                        provider_with_telemetry.clone(),
                    );

                    session_manager
                        .register_table(reference_name.as_str(), provider_with_telemetry.clone())?;

                    let logical_plan = LogicalPlanBuilder::scan(
                        reference_name.clone(),
                        provider_as_source(provider_with_telemetry.clone()),
                        None,
                    )?
                    .build()?;

                    // Try to get extracted primary key from the schema if not defined in topology
                    let effective_primary_key = if primary_key_opt.is_none() {
                        extracted_pk
                    } else {
                        primary_key_opt.clone()
                    };

                    let primary_key_source = if primary_key_opt.is_some() {
                        PrimaryKeySource::TopologyDefined
                    } else {
                        PrimaryKeySource::SchemaInferred
                    };

                    pk_registry.track_primary_key_for_source(
                        &effective_primary_key,
                        reference_name.clone(),
                        logical_plan.schema().inner(),
                        primary_key_source,
                    )?;

                    if let Some(_existing_plan) =
                        pipeline_plans.insert(reference_name.clone(), logical_plan)
                    {
                        streamling_user_bail!("{}: duplicate node name", ctx.format());
                    }
                }
                topology::Source::clickhouse(clickhouse) => {
                    let ctx = node_contexts
                        .get(reference_name)
                        .expect("node context must exist");
                    let table_name = &clickhouse.table_name;
                    let filter = &clickhouse.filter;
                    let start_at = &clickhouse.start_at;
                    let columns = &clickhouse.columns;
                    let primary_key_opt = &clickhouse.primary_key;

                    let start_at = start_at
                        .clone()
                        .map(|start_at| start_at.split(',').map(ScalarValue::from).collect());
                    let columns = columns
                        .clone()
                        .map(|columns| columns.split(",").map(|s| s.to_string()).collect());
                    let clickhouse_source_provider = Arc::new(ClickHouseTableProvider::new_source(
                        reference_name.clone(),
                        metric_key(&application_id, reference_name.as_str()),
                        table_name.as_str(),
                        app_config.clickhouse_source.clone(),
                        start_at,
                        filter.clone(),
                        columns,
                        state_backend_factory.create(app_config.state_backend_namespace()),
                        app_config.internal_buffer_size.as_usize(),
                        app_config.record_batch_size as usize,
                    )?);
                    let extracted_pk = clickhouse_source_provider.get_extracted_primary_key();

                    let provider_with_telemetry = Arc::new(WrappingSourceTableProvider::new(
                        clickhouse_source_provider,
                        metric_key(&application_id, reference_name.as_str()),
                        scan_sharing.clone(),
                        clickhouse.telemetry.as_ref(),
                    ));

                    self.register_operator_for_side_outputs(
                        reference_name.as_str(),
                        provider_with_telemetry.clone(),
                    );

                    session_manager
                        .register_table(reference_name.as_str(), provider_with_telemetry.clone())?;

                    let logical_plan = LogicalPlanBuilder::scan(
                        reference_name.clone(),
                        provider_as_source(provider_with_telemetry.clone()),
                        None,
                    )?
                    .build()?;

                    // Try to get extracted primary key from the sorting keys if not defined in topology
                    let effective_primary_key = if primary_key_opt.is_none() {
                        extracted_pk
                    } else {
                        primary_key_opt.clone()
                    };

                    let primary_key_source = if primary_key_opt.is_some() {
                        PrimaryKeySource::TopologyDefined
                    } else {
                        PrimaryKeySource::SchemaInferred
                    };

                    pk_registry.track_primary_key_for_source(
                        &effective_primary_key,
                        reference_name.clone(),
                        logical_plan.schema().inner(),
                        primary_key_source,
                    )?;

                    if let Some(_existing_plan) =
                        pipeline_plans.insert(reference_name.clone(), logical_plan)
                    {
                        streamling_user_bail!("{}: duplicate node name", ctx.format());
                    }
                }
                topology::Source::hybrid(hybrid) => {
                    let ctx = node_contexts
                        .get(reference_name)
                        .expect("node context must exist");
                    let bounded_sources = &hybrid.bounded_sources;
                    let unbounded_source = &hybrid.unbounded_source;
                    let offset_table = &hybrid.offset_table;
                    let primary_key_opt = &hybrid.primary_key;

                    let hybrid_source_provider = Arc::new(
                        HybridTableProvider::new_from_topology(
                            reference_name.clone(),
                            bounded_sources.clone(),
                            unbounded_source.clone(),
                            offset_table.clone(),
                            &app_config,
                            &state_backend_factory,
                            session_manager.clone(),
                            // Per-phase event-time config flows directly to the
                            // inner WrappingSourceTableProviders (one per bounded
                            // phase + one for unbounded), each carrying its own
                            // `metric_key_hybrid_src_*` suffix. R9 falls out.
                            hybrid.telemetry.as_ref(),
                        )?
                        // In job mode this source emits the terminal checkpoint
                        // when its bounded phases complete; in streaming mode it
                        // does so on shutdown. Give it the control handle (to gate
                        // teardown on that epoch finalizing) and the shutdown
                        // signal (to drain rather than drop on SIGTERM).
                        .with_checkpoint_control(checkpoint_control.clone())
                        .with_shutdown(shutdown_rx.clone()),
                    );

                    let provider_with_telemetry = Arc::new(WrappingSourceTableProvider::new(
                        hybrid_source_provider.clone(),
                        metric_key(&application_id, reference_name.as_str()),
                        scan_sharing.clone(),
                        // Outer wrapping passes None — telemetry is handled
                        // by the per-phase inner providers above; emitting
                        // here would double-count.
                        None,
                    ));
                    hybrid_providers
                        .push((hybrid_source_provider, provider_with_telemetry.clone()));

                    self.register_operator_for_side_outputs(
                        reference_name.as_str(),
                        provider_with_telemetry.clone(),
                    );

                    session_manager
                        .register_table(reference_name.as_str(), provider_with_telemetry.clone())?;

                    let logical_plan = LogicalPlanBuilder::scan(
                        reference_name.clone(),
                        provider_as_source(provider_with_telemetry.clone()),
                        None,
                    )?
                    .build()?;

                    pk_registry.track_primary_key_for_source(
                        primary_key_opt,
                        reference_name.clone(),
                        logical_plan.schema().inner(),
                        PrimaryKeySource::TopologyDefined,
                    )?;

                    if let Some(_existing_plan) =
                        pipeline_plans.insert(reference_name.clone(), logical_plan)
                    {
                        streamling_user_bail!("{}: duplicate node name", ctx.format());
                    }
                }
                topology::Source::file(file) => {
                    let ctx = node_contexts
                        .get(reference_name)
                        .expect("node context must exist");

                    let provider: Arc<dyn TableProvider> = match &file.mode {
                        topology::FileSourceMode::Bounded => build_bounded_file_source_provider(
                            reference_name,
                            &file.path,
                            file.format,
                            &session_manager,
                            file.parallelism,
                        )
                        .await
                        .map_err(|e| {
                            e.context(format!("{}: failed to create file source", ctx.format()))
                        })?,
                        topology::FileSourceMode::Continuous { poll_interval } => {
                            let interval =
                                humantime::parse_duration(poll_interval).map_err(|e| {
                                    streamling_user_err!(
                                        "{}: invalid poll_interval '{}': {}",
                                        ctx.format(),
                                        poll_interval,
                                        e
                                    )
                                })?;
                            FileSourceTableProvider::try_new(
                                reference_name,
                                &file.path,
                                file.format,
                                interval,
                                &session_manager,
                                state_backend_factory.create(app_config.state_backend_namespace()),
                                app_config.num_records_before_stop,
                                app_config.internal_buffer_size,
                            )
                            .await
                            .map_err(|e| {
                                e.context(format!("{}: failed to create file source", ctx.format()))
                            })?
                        }
                    };

                    let provider_with_telemetry = Arc::new(WrappingSourceTableProvider::new(
                        provider,
                        metric_key(&application_id, reference_name.as_str()),
                        scan_sharing.clone(),
                        file.telemetry.as_ref(),
                    ));

                    self.register_operator_for_side_outputs(
                        reference_name.as_str(),
                        provider_with_telemetry.clone(),
                    );

                    session_manager
                        .register_table(reference_name.as_str(), provider_with_telemetry.clone())?;

                    let logical_plan = LogicalPlanBuilder::scan(
                        reference_name.clone(),
                        provider_as_source(provider_with_telemetry.clone()),
                        None,
                    )?
                    .build()?;

                    pk_registry.track_primary_key_for_source(
                        &file.primary_key,
                        reference_name.clone(),
                        logical_plan.schema().inner(),
                        PrimaryKeySource::TopologyDefined,
                    )?;

                    if let Some(_existing_plan) =
                        pipeline_plans.insert(reference_name.clone(), logical_plan)
                    {
                        streamling_user_bail!("{}: duplicate node name", ctx.format());
                    }
                }
                topology::Source::plugin(plugin) => {
                    let ctx = node_contexts
                        .get(reference_name)
                        .expect("node context must exist");
                    let opts = plugin.options.clone().unwrap_or_default();
                    let primary_key_opt = &plugin.primary_key;

                    // Keying by reference name (no cross-source sharing)
                    if !plugins.contains_key(reference_name) {
                        let created = create_source_plugin(
                            &app_config,
                            reference_name.clone(),
                            plugin.r#type.clone(),
                            Self::convert_plugin_options(opts.clone()),
                        )
                        .map_err(|e| {
                            e.context(format!("{}: failed to initialize plugin", ctx.format()))
                        })?;
                        plugins.insert(reference_name.clone(), created);
                    }
                    let created = plugins.get(reference_name).expect("plugin must exist");
                    let channels = created.channels.clone();
                    let output_schema = created
                        .output_schema
                        .clone()
                        .expect("Source plugin must have output schema");
                    let plugin_source_provider: Arc<PluginSourceProvider> =
                        Arc::new(PluginSourceProvider::new(
                            output_schema,
                            Arc::new(channels),
                            app_config.internal_buffer_size,
                            metric_key(&application_id, reference_name.as_str()),
                        ));

                    let provider_with_telemetry = Arc::new(WrappingSourceTableProvider::new(
                        plugin_source_provider,
                        metric_key(&application_id, reference_name.as_str()),
                        scan_sharing.clone(),
                        plugin.telemetry.as_ref(),
                    ));

                    self.register_operator_for_side_outputs(
                        reference_name.as_str(),
                        provider_with_telemetry.clone(),
                    );

                    session_manager
                        .register_table(reference_name.as_str(), provider_with_telemetry.clone())?;

                    let logical_plan = LogicalPlanBuilder::scan(
                        reference_name.clone(),
                        provider_as_source(provider_with_telemetry.clone()),
                        None,
                    )?
                    .build()?;

                    pk_registry.track_primary_key_for_source(
                        primary_key_opt,
                        reference_name.clone(),
                        logical_plan.schema().inner(),
                        PrimaryKeySource::TopologyDefined,
                    )?;

                    if let Some(_existing_plan) =
                        pipeline_plans.insert(reference_name.clone(), logical_plan)
                    {
                        streamling_user_bail!("{}: duplicate node name", ctx.format());
                    }
                }
            }
        }

        // Register plugin side outputs on all sources
        {
            let operators = self
                .side_output_operators
                .read()
                .expect("side outputs lock");
            let source_schemas: HashMap<String, SchemaRef> = operators
                .keys()
                .filter_map(|name| {
                    pipeline_plans
                        .get(name)
                        .map(|plan| (name.clone(), plan.schema().inner().clone()))
                })
                .collect();
            register_plugin_side_outputs(
                &app_config.plugin.side_output_ids,
                &operators,
                &source_schemas,
                &app_config.plugin.side_output_options,
                &application_id,
                app_config.plugin.channel_capacity as usize,
            )?;

            // Forward side outputs to hybrid inner sources so they see pre-filter
            // data during the Kafka phase.
            for (hybrid, outer_wrapping) in &hybrid_providers {
                for side_output in outer_wrapping.get_side_outputs() {
                    hybrid.add_side_output_to_inner_sources(side_output);
                }
            }
        }

        // Process transforms in topological dependency order to avoid race conditions
        let transform_order =
            topology_sort::sort_transforms(&pipeline_topology.transforms, &pipeline_plans);

        debug!("Transform order: {:?}", transform_order);

        for reference_name in transform_order {
            let transform = pipeline_topology
                .transforms
                .get(&reference_name)
                .unwrap()
                .clone();
            let transform_telemetry = transform.telemetry().cloned();

            // Check if this transform has multiple consumers and needs scan sharing
            let consumer_count = node_consumers.get(&reference_name).copied().unwrap_or(0);
            let enable_scan_sharing = consumer_count > 1;

            if enable_scan_sharing {
                let metric_name = metric_key(&application_id, reference_name.as_str());
                info!(
                    "Transform '{}' (metric: '{}') has {} consumers - enabling scan sharing",
                    reference_name, metric_name, consumer_count
                );
                scan_sharing_registry.set_expected_consumers(metric_name, consumer_count);
            }

            match transform {
                topology::Transform::dynamic_table(dt) => {
                    let ctx = node_contexts
                        .get(&reference_name)
                        .expect("node context must exist");
                    let backend_type = dt.backend_type;
                    let backend_entity_name = &dt.backend_entity_name;
                    let sql = &dt.sql;
                    let schema = dt.schema.clone();
                    let column = dt.column.clone();
                    let time_column = dt.time_column.clone();
                    let dynamic_table_backend = dynamic_table_backend_factory
                        .create(
                            backend_type,
                            backend_entity_name.clone(),
                            schema,
                            column,
                            time_column,
                        )
                        .await
                        .map_err(|e| {
                            streamling_err!(
                                "{}: failed to create dynamic table backend: {:?}",
                                ctx.format(),
                                e
                            )
                        })?;

                    dynamic_table_registry
                        .register(reference_name.clone(), dynamic_table_backend.clone())
                        .map_err(|e| {
                            streamling_err!(
                                "{}: failed to register dynamic table backend: {}",
                                ctx.format(),
                                e
                            )
                        })?;

                    if let Some(sql) = sql {
                        let plan = session_manager.create_logical_plan(sql.clone()).await?;

                        let source_name = SessionManager::validate_plan_and_extract_source_name(
                            &plan,
                            SubqueryHandling::Return,
                        )?;

                        let (projection, filter) = extract_expressions_from_sql_query(&plan)?;

                        let schema = pipeline_plans.get(&source_name).unwrap().schema();
                        // Recreating the same schema with field_qualifiers of the source_name
                        // It's needed in order to support transforms
                        // By default, transforms are just SubqueryAlias nodes and they return the schema
                        // of the underlying source (with field qualifiers of that source)
                        let schema = Arc::new(DFSchema::try_from_qualified_schema(
                            source_name.clone(),
                            schema.inner().as_ref(),
                        )?);

                        let side_output = DynamicTableSideOutput::new(
                            dynamic_table_backend,
                            projection,
                            filter,
                            &session_manager.session_state(),
                            &schema,
                        )?;

                        if let Some(handle) = self.get_side_output_operator(&source_name) {
                            handle.add_side_output(Arc::new(side_output));
                        } else {
                            warn!(
                                "Source table provider for {} does not support side outputs",
                                source_name
                            );
                        }
                    }
                }
                topology::Transform::sql(sql_transform) => {
                    let ctx = node_contexts
                        .get(&reference_name)
                        .expect("node context must exist");
                    let sql = &sql_transform.sql;
                    let (sql_plan, source_name) = session_manager
                        .create_supported_logical_plan(sql.clone())
                        .await
                        .streamling_with_context(|| {
                            format!(
                                "{}: failed to parse SQL\n--- SQL ---\n{}\n-----------",
                                ctx.format(),
                                sql.trim()
                            )
                        })?;

                    validate_dynamic_table_usage(
                        source_name.clone(),
                        &sql_plan,
                        &pipeline_topology,
                        &session_manager,
                    )
                    .await?;

                    let pk_metadata_opt = pk_registry.track_primary_key_for_transform_or_sink(
                        &Some(sql_transform.primary_key),
                        source_name.clone(),
                        reference_name.clone(),
                        sql_plan.schema().inner(),
                    )?;

                    let pk_columns = pk_metadata_opt
                        .map(|pk| pk.columns.clone())
                        .unwrap_or_default();

                    // An explicit `parallelism` widens the transform itself: the
                    // exchange goes under its SQL, so the filter/projection above
                    // run at the requested width. Keyed by the transform's
                    // primary key, so per-key ordering survives the widening.
                    let sql_plan = match sql_transform.parallelism {
                        Some(parallelism) => wrap_transform_input_with_repartition(
                            sql_plan,
                            &source_name,
                            &Placement::ByKey(pk_columns.clone()),
                            parallelism,
                            &reference_name,
                        )?,
                        None => sql_plan,
                    };

                    let logical_plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(CheckpointableNode::new(
                            sql_plan,
                            app_config.internal_buffer_size,
                            reference_name.clone(),
                        )),
                    });

                    let wrapping_node = Arc::new(WrappingNode::new_with_non_null_cols(
                        logical_plan,
                        metric_key(&application_id, reference_name.as_str()),
                        enable_scan_sharing,
                        pk_columns,
                        transform_telemetry.clone(),
                    ));

                    self.register_operator_for_side_outputs(
                        reference_name.as_str(),
                        wrapping_node.clone(),
                    );

                    let logical_plan_with_telemetry = LogicalPlan::Extension(Extension {
                        node: wrapping_node,
                    });

                    let view = ViewTable::new(logical_plan_with_telemetry.clone(), None);
                    session_manager.register_table(reference_name.as_str(), Arc::new(view))?;

                    if let Some(_existing_plan) =
                        pipeline_plans.insert(reference_name.clone(), logical_plan_with_telemetry)
                    {
                        streamling_user_bail!("{}: duplicate node name", ctx.format());
                    }
                }
                topology::Transform::handler(handler) => {
                    let ctx = node_contexts
                        .get(&reference_name)
                        .expect("node context must exist");
                    let from = &handler.from;
                    let url = &handler.url;
                    let one_row_per_request = handler.one_row_per_request;
                    let payload_version = handler.payload_version;
                    let schema_override = &handler.schema_override;
                    let config = app_config.external_http_handler.clone();
                    let source_plan = pipeline_plans
                        .get(from.as_str())
                        .ok_or_else(|| {
                            streamling_user_err!("{}: source '{}' not found", ctx.format(), from)
                        })?
                        .clone();

                    let headers = if let Some(secret_name) =
                        secret_name_to_resolve(handler.secret_name.as_deref(), dry_run)
                    {
                        merge_secret_into_headers(
                            &reference_name,
                            secret_name,
                            handler.headers.clone(),
                            &app_config.http_secret_header,
                            &app_config.http_secret_value,
                        )?
                    } else {
                        handler.headers.clone()
                    };

                    let handler_config = ExternalHandlerConfig {
                        url: url.clone(),
                        headers,
                        one_row_per_request,
                        payload_version,
                        trigger_max_count: config.trigger_max_count,
                        operator_timeout_sec: config.operator_timeout_sec,
                        schema_override: schema_override.clone(),
                        buffer_size: config.buffer_size,
                        metric_metadata_id: metric_key(&application_id, reference_name.as_str()),
                    };

                    let batch_size = handler.batch_size.map(|s| s as usize);
                    let batch_flush_interval =
                        parse_batch_flush_interval(&handler.batch_flush_interval, &ctx.format())?;
                    let handler_input = wrap_with_rebatch(
                        source_plan,
                        batch_size,
                        batch_flush_interval,
                        reference_name.clone(),
                    );

                    let logical_plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(ExternalHandlerNode::new(handler_input, handler_config)),
                    });

                    let pk_metadata_opt = pk_registry.track_primary_key_for_transform_or_sink(
                        &Some(handler.primary_key),
                        from.clone(),
                        reference_name.clone(),
                        logical_plan.schema().inner(),
                    )?;

                    let pk_columns = pk_metadata_opt
                        .map(|pk| pk.columns.clone())
                        .unwrap_or_default();

                    let wrapping_node = Arc::new(WrappingNode::new_with_non_null_cols(
                        logical_plan,
                        metric_key(&application_id, reference_name.as_str()),
                        enable_scan_sharing,
                        pk_columns,
                        transform_telemetry.clone(),
                    ));

                    self.register_operator_for_side_outputs(
                        reference_name.as_str(),
                        wrapping_node.clone(),
                    );

                    let logical_plan_with_telemetry = LogicalPlan::Extension(Extension {
                        node: wrapping_node,
                    });

                    // also register this as a view in case another transform (e.g. SQL) refers to it
                    let view = ViewTable::new(logical_plan_with_telemetry.clone(), None);
                    session_manager.register_table(reference_name.as_str(), Arc::new(view))?;

                    if let Some(_existing_plan) =
                        pipeline_plans.insert(reference_name.clone(), logical_plan_with_telemetry)
                    {
                        streamling_user_bail!("{}: duplicate node name", ctx.format());
                    }
                }
                topology::Transform::script(script_transform) => {
                    let ctx = node_contexts
                        .get(&reference_name)
                        .expect("node context must exist");
                    let from = &script_transform.from;
                    let language = &script_transform.language;
                    let script = &script_transform.script;
                    let schema = &script_transform.schema;
                    // Use topology-level parallelism/batch_size if specified, otherwise fall back to app_config
                    let parallelism = script_transform
                        .parallelism
                        .unwrap_or(app_config.wasm_script.parallelism);
                    let batch_size = script_transform
                        .batch_size
                        .unwrap_or(app_config.wasm_script.batch_size);
                    let source_plan = pipeline_plans
                        .get(from.as_str())
                        .ok_or_else(|| {
                            streamling_user_err!("{}: source '{}' not found", ctx.format(), from)
                        })?
                        .clone();
                    let wasm_node = WasmRunnerNode::with_options(
                        source_plan,
                        language.clone(),
                        script.clone(),
                        app_config.wasm_script.runtime_wasm_file_path.clone(),
                        app_config.internal_buffer_size,
                        schema.clone(),
                        parallelism,
                        batch_size,
                    )
                    // Convert via `StreamlingError::from` (not `streamling_with_context`)
                    // so a user-facing schema error (e.g. unsupported dtype) is recovered
                    // from the `DataFusionError::External` wrapper and stays user-facing.
                    // Otherwise `--validate` would misreport it as an internal failure.
                    .map_err(|e| {
                        streamling_core::error::StreamlingError::from(e).context(format!(
                            "{}: failed to build script transform",
                            ctx.format()
                        ))
                    })?;

                    let logical_plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(wasm_node),
                    });

                    let pk_metadata_opt = pk_registry.track_primary_key_for_transform_or_sink(
                        &Some(script_transform.primary_key),
                        from.clone(),
                        reference_name.clone(),
                        logical_plan.schema().inner(),
                    )?;

                    let pk_columns = pk_metadata_opt
                        .map(|pk| pk.columns.clone())
                        .unwrap_or_default();

                    let wrapping_node = Arc::new(WrappingNode::new_with_non_null_cols(
                        logical_plan,
                        metric_key(&application_id, reference_name.as_str()),
                        enable_scan_sharing,
                        pk_columns,
                        transform_telemetry.clone(),
                    ));

                    self.register_operator_for_side_outputs(
                        reference_name.as_str(),
                        wrapping_node.clone(),
                    );

                    let logical_plan_with_telemetry = LogicalPlan::Extension(Extension {
                        node: wrapping_node,
                    });

                    // also register this as a view in case another transform (e.g. SQL) refers to it
                    let view = ViewTable::new(logical_plan_with_telemetry.clone(), None);
                    session_manager.register_table(reference_name.as_str(), Arc::new(view))?;

                    if let Some(_existing_plan) =
                        pipeline_plans.insert(reference_name.clone(), logical_plan_with_telemetry)
                    {
                        streamling_user_bail!("{}: duplicate node name", ctx.format());
                    }
                }
                topology::Transform::plugin(plugin_transform) => {
                    let ctx = node_contexts
                        .get(&reference_name)
                        .expect("node context must exist");
                    let r#type = &plugin_transform.r#type;
                    let from = &plugin_transform.from;
                    let options = &plugin_transform.options;
                    let primary_key_opt = &plugin_transform.primary_key;
                    let batch_size = plugin_transform.batch_size.map(|s| s as usize);
                    let batch_flush_interval = parse_batch_flush_interval(
                        &plugin_transform.batch_flush_interval,
                        &ctx.format(),
                    )?;
                    let source_plan = pipeline_plans
                        .get(from.as_str())
                        .ok_or_else(|| {
                            streamling_user_err!("{}: source '{}' not found", ctx.format(), from)
                        })?
                        .clone();

                    let transform_input = wrap_with_rebatch(
                        source_plan.clone(),
                        batch_size,
                        batch_flush_interval,
                        reference_name.clone(),
                    );

                    let initialized_plugin = create_transform_plugin(
                        &app_config,
                        reference_name.clone(), // name
                        r#type.clone(),         // plugin_type
                        Self::convert_plugin_options(options.clone().unwrap_or_default()),
                        transform_input.schema().inner().clone(),
                    )
                    .map_err(|e| {
                        e.context(format!("{}: failed to initialize plugin", ctx.format()))
                    })?;
                    let output_schema = Arc::new(DFSchema::try_from(
                        initialized_plugin
                            .output_schema
                            .as_ref()
                            .expect("Transform plugin must have output schema")
                            .as_ref()
                            .clone(),
                    )?);
                    let logical_plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(PluginNode::new(
                            transform_input,
                            output_schema,
                            Arc::new(initialized_plugin.channels.clone()),
                            app_config.internal_buffer_size,
                            metric_key(&application_id, reference_name.as_str()),
                        )),
                    });

                    let pk_metadata_opt = pk_registry.track_primary_key_for_transform_or_sink(
                        primary_key_opt,
                        from.clone(),
                        reference_name.clone(),
                        logical_plan.schema().inner(),
                    )?;

                    let pk_columns = pk_metadata_opt
                        .map(|pk| pk.columns.clone())
                        .unwrap_or_default();

                    let wrapping_node = Arc::new(WrappingNode::new_with_non_null_cols(
                        logical_plan,
                        metric_key(&application_id, reference_name.as_str()),
                        enable_scan_sharing,
                        pk_columns,
                        transform_telemetry.clone(),
                    ));

                    self.register_operator_for_side_outputs(
                        reference_name.as_str(),
                        wrapping_node.clone(),
                    );

                    let logical_plan_with_telemetry = LogicalPlan::Extension(Extension {
                        node: wrapping_node,
                    });

                    // Register transform plugin under its reference name
                    plugins.insert(reference_name.clone(), initialized_plugin);

                    // also register this as a view in case another transform (e.g. SQL) refers to it
                    let view = ViewTable::new(logical_plan_with_telemetry.clone(), None);
                    session_manager.register_table(reference_name.as_str(), Arc::new(view))?;

                    if let Some(_existing_plan) =
                        pipeline_plans.insert(reference_name.clone(), logical_plan_with_telemetry)
                    {
                        streamling_user_bail!("{}: duplicate node name", ctx.format());
                    }
                }
            }
        }

        for (reference_name, sink) in pipeline_topology.sinks {
            let sink_telemetry = sink.telemetry().cloned();
            match sink {
                topology::Sink::webhook(webhook) => {
                    let from = &webhook.from;
                    let url = &webhook.url;
                    let one_row_per_request = webhook.one_row_per_request;
                    let payload_version = webhook.payload_version;
                    let skip_on_error = webhook.skip_on_error;
                    let primary_key_opt = &webhook.primary_key;
                    let (source_plan, source_schema) =
                        Self::find_plan_and_schema(&pipeline_plans, from.as_str())?;

                    let headers = if let Some(secret_name) =
                        secret_name_to_resolve(webhook.secret_name.as_deref(), dry_run)
                    {
                        merge_secret_into_headers(
                            &reference_name,
                            secret_name,
                            webhook.headers.clone(),
                            &app_config.http_secret_header,
                            &app_config.http_secret_value,
                        )?
                    } else {
                        webhook.headers.clone()
                    };

                    pk_registry.track_primary_key_for_transform_or_sink(
                        primary_key_opt,
                        from.clone(),
                        reference_name.clone(),
                        &source_schema,
                    )?;

                    let batch_flush_interval =
                        parse_batch_flush_interval(&webhook.batch_flush_interval, &reference_name)?;
                    let http_sink_provider = Arc::new(HttpTableProvider::new(
                        app_config.external_http_handler.clone(),
                        url.clone(),
                        headers,
                        one_row_per_request,
                        payload_version,
                        skip_on_error,
                        source_schema.clone(),
                        app_config.num_records_before_stop,
                        from.clone(),
                        metric_key(&application_id, reference_name.as_str()),
                        sink_telemetry.clone(),
                    ));
                    session_manager
                        .register_table(reference_name.as_str(), http_sink_provider.clone())?;
                    Self::update_source_to_sink_mapping(
                        &mut sources_to_sinks,
                        from.clone(),
                        source_plan,
                        SinkEntry::new(
                            reference_name.clone(),
                            http_sink_provider,
                            RebatchConfig::new(webhook.batch_size, batch_flush_interval),
                            // Concurrent `write_all` is untested for this sink, and
                            // N streams would also multiply the in-flight requests.
                            Placement::Single,
                            None,
                        ),
                    );
                    checkpoint_sink_names.push(reference_name.clone());
                }
                topology::Sink::print(print_sink) => {
                    let from = &print_sink.from;
                    let sample_every = print_sink.sample_every;
                    let num_records_before_stop = print_sink.num_records_before_stop;
                    let primary_key_opt = &print_sink.primary_key;
                    let (source_plan, source_schema) =
                        Self::find_plan_and_schema(&pipeline_plans, from.as_str())?;

                    pk_registry.track_primary_key_for_transform_or_sink(
                        primary_key_opt,
                        from.clone(),
                        reference_name.clone(),
                        &source_schema,
                    )?;

                    let batch_flush_interval = parse_batch_flush_interval(
                        &print_sink.batch_flush_interval,
                        &reference_name,
                    )?;
                    let print_sink_provider = Arc::new(PrintTableProvider::new(
                        sample_every.unwrap_or(app_config.print_sink.sample_every),
                        num_records_before_stop.or(app_config.num_records_before_stop),
                        source_schema.clone(),
                        from.clone(),
                        metric_key(&application_id, reference_name.as_str()),
                        sink_telemetry.clone(),
                    ));
                    session_manager
                        .register_table(reference_name.as_str(), print_sink_provider.clone())?;
                    Self::update_source_to_sink_mapping(
                        &mut sources_to_sinks,
                        from.clone(),
                        source_plan,
                        SinkEntry::new(
                            reference_name.clone(),
                            print_sink_provider,
                            RebatchConfig::new(print_sink.batch_size, batch_flush_interval),
                            Placement::RoundRobin,
                            print_sink.parallelism,
                        ),
                    );
                    checkpoint_sink_names.push(reference_name.clone());
                }
                topology::Sink::blackhole(blackhole) => {
                    let from = &blackhole.from;
                    let primary_key_opt = &blackhole.primary_key;
                    let (source_plan, source_schema) =
                        Self::find_plan_and_schema(&pipeline_plans, from.as_str())?;

                    pk_registry.track_primary_key_for_transform_or_sink(
                        primary_key_opt,
                        from.clone(),
                        reference_name.clone(),
                        &source_schema,
                    )?;

                    let batch_flush_interval = parse_batch_flush_interval(
                        &blackhole.batch_flush_interval,
                        &reference_name,
                    )?;
                    let blackhole_sink_provider = Arc::new(BlackholeTableProvider::new(
                        source_schema.clone(),
                        app_config.num_records_before_stop,
                        from.clone(),
                        metric_key(&application_id, reference_name.as_str()),
                        sink_telemetry.clone(),
                    ));
                    session_manager
                        .register_table(reference_name.as_str(), blackhole_sink_provider.clone())?;
                    Self::update_source_to_sink_mapping(
                        &mut sources_to_sinks,
                        from.clone(),
                        source_plan,
                        SinkEntry::new(
                            reference_name.clone(),
                            blackhole_sink_provider,
                            RebatchConfig::new(blackhole.batch_size, batch_flush_interval),
                            Placement::RoundRobin,
                            blackhole.parallelism,
                        ),
                    );
                    checkpoint_sink_names.push(reference_name.clone());
                }
                topology::Sink::postgres(postgres) => {
                    let from = &postgres.from;
                    let table = &postgres.table;
                    let schema = &postgres.schema;
                    let batch_flush_interval = &postgres.batch_flush_interval;
                    let batch_size = postgres.batch_size;
                    let primary_key_opt = &postgres.primary_key;
                    let on_conflict = &postgres.on_conflict;
                    let update_where = &postgres.update_where;
                    let (source_plan, source_schema) =
                        Self::find_plan_and_schema(&pipeline_plans, from.as_str())?;

                    let pk_metadata_opt = pk_registry.track_primary_key_for_transform_or_sink(
                        primary_key_opt,
                        from.clone(),
                        reference_name.clone(),
                        &source_schema,
                    )?;

                    if let Some(uw) = update_where {
                        validate_update_where(uw, &source_schema, &reference_name)?;
                    }

                    // Per-sink connection: two postgres sinks in one pipeline can
                    // target two different databases (STRM-6516). Falls back to the
                    // global postgres_sink block when no per-sink keys are set.
                    let postgres_config = app_config.postgres_sink_for(&reference_name);
                    info!(
                        "postgres sink '{}' resolved connection {:?}",
                        reference_name, postgres_config
                    );

                    let postgres_sink_provider = Arc::new(PostgresSinkTableProvider::new(
                        metric_key(&application_id, reference_name.as_str()),
                        source_schema.clone(),
                        postgres_config.clone(),
                        table.clone(),
                        schema.clone(),
                        app_config.num_records_before_stop,
                        from.clone(),
                        pk_metadata_opt.as_ref().map(|pk| pk.to_str()),
                        on_conflict.clone(),
                        update_where.clone(),
                        false, // append_only_mode (normal Postgres sink)
                        false, // checkpoint_truncation (disabled for normal sink)
                        postgres.deduplicate,
                        reference_name.clone(),
                        postgres.parallelism,
                        sink_telemetry.clone(),
                    ));

                    session_manager
                        .register_table(reference_name.as_str(), postgres_sink_provider.clone())?;
                    let raw_batch_flush_interval =
                        parse_batch_flush_interval(batch_flush_interval, &reference_name)?;
                    // Apply Postgres-sink app_config defaults so the rebatcher
                    // always has a concrete batch_size/interval regardless of
                    // whether the topology supplied them.
                    let effective_batch_size =
                        Some(batch_size.unwrap_or(postgres_config.batch_size));
                    let effective_batch_flush_interval = Some(match raw_batch_flush_interval {
                        Some(d) => d,
                        None => humantime::parse_duration(&postgres_config.batch_flush_interval)
                            .streamling_with_context(|| {
                                format!(
                                    "invalid postgres batch_flush_interval '{}' resolved for sink '{}'",
                                    postgres_config.batch_flush_interval, reference_name
                                )
                            })?,
                    });
                    Self::update_source_to_sink_mapping(
                        &mut sources_to_sinks,
                        from.clone(),
                        source_plan,
                        SinkEntry::new(
                            reference_name.clone(),
                            postgres_sink_provider,
                            RebatchConfig::new(
                                effective_batch_size,
                                effective_batch_flush_interval,
                            ),
                            Placement::ByKey(pk_columns(&pk_metadata_opt)),
                            postgres.parallelism,
                        ),
                    );
                    checkpoint_sink_names.push(reference_name.clone());
                }
                // Same as Postgres, but includes creating triggers for aggregations
                topology::Sink::postgres_aggregate(postgres) => {
                    let ctx = node_contexts
                        .get(&reference_name)
                        .expect("node context must exist");
                    let from = &postgres.from;
                    let landing_table = &postgres.landing_table;
                    let schema = &postgres.schema;
                    let batch_flush_interval = &postgres.batch_flush_interval;
                    let batch_size = postgres.batch_size;
                    let primary_key_opt = &postgres.primary_key;
                    let (source_plan, source_schema) =
                        Self::find_plan_and_schema(&pipeline_plans, from.as_str())?;

                    let pk_metadata_opt = pk_registry.track_primary_key_for_transform_or_sink(
                        primary_key_opt,
                        from.clone(),
                        reference_name.clone(),
                        &source_schema,
                    )?;

                    let primary_key =
                        pk_metadata_opt
                            .as_ref()
                            .map(|pk| pk.to_str())
                            .ok_or_else(|| {
                                streamling_user_err!(
                                    "{}: primary key is required for Postgres aggregation sink",
                                    ctx.format()
                                )
                            })?;

                    let df_source_schema = DFSchema::try_from(source_schema.clone())
                        .streamling_with_context(|| {
                            format!(
                                "{}: failed to convert schema to DFSchema for sink",
                                ctx.format()
                            )
                        })?;

                    let pg_aggregator = PostgresAggregator::try_new(
                        &postgres.group_by,
                        &postgres.aggregate,
                        Arc::new(df_source_schema),
                        landing_table.clone(),
                        primary_key.clone(),
                        postgres.agg_table.clone(),
                        schema.clone(),
                    )
                    .streamling_with_context(|| {
                        format!("{}: failed to create PostgresAggregator", ctx.format())
                    })?;

                    // Per-sink connection, as for the plain postgres sink above.
                    let postgres_config = app_config.postgres_sink_for(&reference_name);
                    info!(
                        "postgres_aggregate sink '{}' resolved connection {:?}",
                        reference_name, postgres_config
                    );

                    if dry_run {
                        // Secrets are not resolved during dry-run (see
                        // secret_name_to_resolve), so there are no DB credentials
                        // to connect with. Still generate the DDL so aggregation
                        // config errors fail validation.
                        pg_aggregator.generate_target_table_sql()?;
                        pg_aggregator.generate_function_sql()?;
                        pg_aggregator.generate_trigger_statement_sql()?;
                    } else {
                        pg_aggregator
                            .create_trigger_and_tables(&postgres_config)
                            .await?;
                    }

                    let postgres_sink_provider = Arc::new(PostgresSinkTableProvider::new(
                        metric_key(&application_id, reference_name.as_str()),
                        source_schema.clone(),
                        postgres_config.clone(),
                        landing_table.clone(),
                        schema.clone(),
                        app_config.num_records_before_stop,
                        from.clone(),
                        Some(primary_key),
                        "update".to_string(),
                        None, // update_where (not applicable for aggregation sink)
                        true, // append_only_mode (aggregation sink)
                        true, // checkpoint_truncation (enabled for aggregation sink)
                        postgres.deduplicate,
                        reference_name.clone(),
                        postgres.parallelism,
                        sink_telemetry.clone(),
                    ));

                    session_manager
                        .register_table(reference_name.as_str(), postgres_sink_provider.clone())?;
                    let raw_batch_flush_interval =
                        parse_batch_flush_interval(batch_flush_interval, &ctx.format())?;
                    // Apply Postgres-sink app_config defaults so the rebatcher
                    // always has a concrete batch_size/interval regardless of
                    // whether the topology supplied them.
                    let effective_batch_size =
                        Some(batch_size.unwrap_or(postgres_config.batch_size));
                    let effective_batch_flush_interval = Some(match raw_batch_flush_interval {
                        Some(d) => d,
                        None => humantime::parse_duration(&postgres_config.batch_flush_interval)
                            .streamling_with_context(|| {
                                format!(
                                    "invalid postgres batch_flush_interval '{}' resolved for sink '{}'",
                                    postgres_config.batch_flush_interval, reference_name
                                )
                            })?,
                    });
                    Self::update_source_to_sink_mapping(
                        &mut sources_to_sinks,
                        from.clone(),
                        source_plan,
                        SinkEntry::new(
                            reference_name.clone(),
                            postgres_sink_provider,
                            RebatchConfig::new(
                                effective_batch_size,
                                effective_batch_flush_interval,
                            ),
                            Placement::ByKey(pk_columns(&pk_metadata_opt)),
                            postgres.parallelism,
                        ),
                    );
                    checkpoint_sink_names.push(reference_name.clone());
                }
                topology::Sink::memory(memory_sink) => {
                    let from = &memory_sink.from;
                    let exclude_gs_op = memory_sink.exclude_gs_op;
                    let primary_key_opt = &memory_sink.primary_key;
                    let (source_plan, source_schema) =
                        Self::find_plan_and_schema(&pipeline_plans, from.as_str())?;

                    pk_registry.track_primary_key_for_transform_or_sink(
                        primary_key_opt,
                        from.clone(),
                        reference_name.clone(),
                        &source_schema,
                    )?;

                    let batch_flush_interval = parse_batch_flush_interval(
                        &memory_sink.batch_flush_interval,
                        &reference_name,
                    )?;
                    let memory_sink_provider = Arc::new(MemoryTableProvider::new_with_options(
                        source_schema.clone(),
                        app_config.num_records_before_stop,
                        from.clone(),
                        reference_name.clone(), // reference_name for registry lookups
                        exclude_gs_op.unwrap_or(false),
                        metric_key(&application_id, reference_name.as_str()), // metric id
                        sink_telemetry.clone(),
                    ));
                    session_manager
                        .register_table(reference_name.as_str(), memory_sink_provider.clone())?;
                    Self::update_source_to_sink_mapping(
                        &mut sources_to_sinks,
                        from.clone(),
                        source_plan,
                        SinkEntry::new(
                            reference_name.clone(),
                            memory_sink_provider,
                            RebatchConfig::new(memory_sink.batch_size, batch_flush_interval),
                            Placement::RoundRobin,
                            memory_sink.parallelism,
                        ),
                    );
                    checkpoint_sink_names.push(reference_name.clone());
                }
                topology::Sink::kafka(kafka_sink) => {
                    let ctx = node_contexts
                        .get(&reference_name)
                        .expect("node context must exist");
                    let from = &kafka_sink.from;
                    let topic = &kafka_sink.topic;
                    let data_format = &kafka_sink.data_format;
                    let topic_partitions = kafka_sink.topic_partitions;
                    let primary_key_opt = &kafka_sink.primary_key;
                    let batch_size = kafka_sink.batch_size;
                    let batch_flush_interval = parse_batch_flush_interval(
                        &kafka_sink.batch_flush_interval,
                        &ctx.format(),
                    )?;
                    let batch_flush_interval_ms =
                        batch_flush_interval.map(|d| d.as_millis() as u64);
                    let (source_plan, source_schema) =
                        Self::find_plan_and_schema(&pipeline_plans, from.as_str())?;

                    // STRM-6281: a Kafka sink without a usable primary key crashloops at
                    // runtime (`primary key column '' not found in batch`) or silently
                    // drops to round-robin partitioning with no compaction. Require a
                    // non-empty resolved key here — after Avro-schema discovery and
                    // primary-key propagation — so a key from the sink config, an upstream
                    // node, or the source's Avro schema all satisfy it.
                    let pk_metadata = pk_registry.require_primary_key_for_sink(
                        "kafka",
                        primary_key_opt,
                        from.clone(),
                        reference_name.clone(),
                        &source_schema,
                    )?;

                    let kafka_sink_provider = Arc::new(KafkaSinkTableProvider::new(
                        metric_key(&application_id, reference_name.as_str()),
                        source_schema,
                        app_config.kafka_sink.clone(),
                        topic.clone(),
                        topic_partitions,
                        data_format.parse()?,
                        app_config.num_records_before_stop,
                        from.clone(),
                        Some(pk_metadata.to_str()),
                        batch_size,
                        batch_flush_interval_ms,
                        kafka_sink.message_max_bytes,
                        kafka_sink.compression,
                        kafka_sink.deduplicate,
                        sink_telemetry.clone(),
                    ));
                    session_manager
                        .register_table(reference_name.as_str(), kafka_sink_provider.clone())?;
                    // Kafka opts out of upstream rebatching: batch_size /
                    // batch_flush_interval already feed into the Kafka producer
                    // (batch.num.messages / linger.ms), which handles its own
                    // accumulation. Inserting a RebatchExec ahead of it would
                    // double-buffer the stream.
                    Self::update_source_to_sink_mapping(
                        &mut sources_to_sinks,
                        from.clone(),
                        source_plan,
                        SinkEntry::new(
                            reference_name.clone(),
                            kafka_sink_provider,
                            RebatchConfig::default(),
                            Placement::ByKey(pk_metadata.columns.clone()),
                            kafka_sink.parallelism,
                        ),
                    );
                    checkpoint_sink_names.push(reference_name.clone());
                }
                topology::Sink::clickhouse(clickhouse_sink) => {
                    let ctx = node_contexts
                        .get(&reference_name)
                        .expect("node context must exist");
                    let from = &clickhouse_sink.from;
                    let table = &clickhouse_sink.table;
                    let batch_flush_interval = &clickhouse_sink.batch_flush_interval;
                    let batch_size = clickhouse_sink.batch_size;
                    let (source_plan, source_schema) =
                        Self::find_plan_and_schema(&pipeline_plans, from.as_str())?;

                    let pk_metadata_opt = pk_registry.track_primary_key_for_transform_or_sink(
                        &Some(clickhouse_sink.primary_key),
                        from.clone(),
                        reference_name.clone(),
                        &source_schema,
                    )?;

                    let raw_batch_flush_interval =
                        parse_batch_flush_interval(batch_flush_interval, &ctx.format())?;
                    // Apply ClickHouse-sink app_config defaults so the rebatcher
                    // always has a concrete batch_size/interval regardless of
                    // whether the topology supplied them.
                    let clickhouse_config = &app_config.clickhouse_sink;
                    let effective_batch_size = batch_size.unwrap_or(clickhouse_config.batch_size);
                    let effective_batch_flush_interval = Some(match raw_batch_flush_interval {
                        Some(d) => d,
                        // Already validated at AppConfig load, so this only
                        // fires if the config was constructed some other way.
                        None => clickhouse_config
                            .parsed_batch_flush_interval()
                            .map_err(|e| streamling_user_err!("{}: {e:#}", ctx.format()))?,
                    });
                    let clickhouse_sink_provider = Arc::new(ClickHouseTableProvider::new_sink(
                        metric_key(&application_id, reference_name.as_str()),
                        table.as_str(),
                        clickhouse_config.connection.clone(),
                        app_config.num_records_before_stop,
                        pk_metadata_opt
                            .as_ref()
                            .map(|pk| pk.to_str())
                            .unwrap_or_default(),
                        from.clone(),
                        clickhouse_sink.append_only_mode,
                        clickhouse_sink.deduplicate,
                        clickhouse_sink.version_column_name.clone(),
                        clickhouse_sink.schema_override.clone(),
                        clickhouse_sink.compression,
                        clickhouse_sink.compression_level,
                        reference_name.clone(),
                        sink_telemetry.clone(),
                    )?);
                    session_manager.register_table(
                        reference_name.as_str(),
                        clickhouse_sink_provider.clone(),
                    )?;
                    Self::update_source_to_sink_mapping(
                        &mut sources_to_sinks,
                        from.clone(),
                        source_plan,
                        SinkEntry::new(
                            reference_name.clone(),
                            clickhouse_sink_provider,
                            RebatchConfig::new(
                                Some(effective_batch_size),
                                effective_batch_flush_interval,
                            ),
                            Placement::ByKey(pk_columns(&pk_metadata_opt)),
                            clickhouse_sink.parallelism,
                        ),
                    );
                    checkpoint_sink_names.push(reference_name.clone());
                }
                topology::Sink::plugin(plugin_sink) => {
                    let ctx = node_contexts
                        .get(&reference_name)
                        .expect("node context must exist");
                    let from = &plugin_sink.from;
                    let r#type = &plugin_sink.r#type;
                    let options = &plugin_sink.options;
                    let primary_key_opt = &plugin_sink.primary_key;
                    let (source_plan, source_schema) =
                        Self::find_plan_and_schema(&pipeline_plans, from.as_str())?;

                    pk_registry.track_primary_key_for_transform_or_sink(
                        primary_key_opt,
                        from.clone(),
                        reference_name.clone(),
                        &source_schema,
                    )?;

                    let mut plugin_opts = options.clone().unwrap_or_default();
                    if let Some(pk) = primary_key_opt {
                        plugin_opts.insert(
                            "primary_key".to_string(),
                            serde_yaml::Value::String(pk.clone()),
                        );
                    }

                    let initialized_plugin = create_sink_plugin(
                        &app_config,
                        reference_name.clone(), // name
                        r#type.clone(),         // plugin_type
                        Self::convert_plugin_options(plugin_opts),
                        source_schema.clone(),
                    )
                    .map_err(|e| {
                        e.context(format!("{}: failed to initialize plugin", ctx.format()))
                    })?;
                    let batch_flush_interval = parse_batch_flush_interval(
                        &plugin_sink.batch_flush_interval,
                        &ctx.format(),
                    )?;
                    let plugin_sink_provider = Arc::new(PluginSinkProvider::new(
                        source_schema.clone(),
                        Arc::new(initialized_plugin.channels.clone()),
                        app_config.num_records_before_stop,
                        metric_key(&application_id, reference_name.as_str()),
                        sink_telemetry.clone(),
                    ));

                    session_manager
                        .register_table(reference_name.as_str(), plugin_sink_provider.clone())?;
                    Self::update_source_to_sink_mapping(
                        &mut sources_to_sinks,
                        from.clone(),
                        source_plan,
                        SinkEntry::new(
                            reference_name.clone(),
                            plugin_sink_provider,
                            RebatchConfig::new(plugin_sink.batch_size, batch_flush_interval),
                            // A plugin acks epochs from inside the plugin, on a
                            // channel poll decoupled from the marker that triggered
                            // it, so the per-stream ack gate cannot see its markers.
                            // The plugin ABI has no partition dimension either.
                            Placement::Single,
                            None,
                        ),
                    );
                    checkpoint_sink_names.push(reference_name.clone());

                    // Register sink plugin under its reference name for lifecycle management
                    plugins.insert(reference_name.clone(), initialized_plugin);
                }
            }
        }

        init_node_registry(node_contexts);

        // Seed each non-sink node's elapsed_compute series only now: plugin
        // construction above merged plugin-declared identity labels
        // (`merge_metadata_tags`), so the seeded samples land on the final
        // label set instead of an orphan pre-merge series.
        get_metrics_recorder().seed_elapsed_compute_series();

        let mut dry_run_plans: Vec<(String, LogicalPlan)> = Vec::new();

        for (_, (source_plan, mut sinks)) in sources_to_sinks.into_iter() {
            let future_name = sinks
                .iter()
                .map(|e| e.name.as_str())
                .collect::<Vec<&str>>()
                .join(", ");
            // Captured for the sink-completion signal below: one entry per
            // sink driven by this future (a fan-out future drives several).
            let sink_names: Vec<String> = sinks.iter().map(|e| e.name.clone()).collect();
            let sink_plan = if sinks.len() > 1 {
                // Fan-out: each sink gets its own RebatchExec injected by
                // the MultiSinkExtensionPlanner, so the raw source_plan
                // flows into MultiSinkLogicalNode without any upstream
                // wrapping. Per-sink configs live on MultiSinkEntry.
                //
                // The one thing that *must* be decided upstream is the
                // partitioning: the sinks share one input, so they share one
                // exchange, and it can only be keyed one way.
                let partitioned_plan =
                    wrap_multi_sink_with_repartition(source_plan, &sinks, future_name.as_str());
                let entries: Vec<MultiSinkEntry> = sinks
                    .into_iter()
                    .map(|e| MultiSinkEntry {
                        name: e.name,
                        rebatch_config: e.rebatch_config,
                    })
                    .collect();
                LogicalPlan::Extension(Extension {
                    node: Arc::new(MultiSinkLogicalNode::new(partitioned_plan, entries)),
                })
            } else {
                // Single sink: wrap once at the logical level with this
                // sink's config, then insert_into.
                let entry = sinks.remove(0);
                // The exchange goes *below* the rebatcher: rebatching after
                // the split keeps each write stream's batches whole, where
                // splitting a rebatched batch would re-fragment it.
                let partitioned_plan = wrap_with_repartition(
                    source_plan,
                    &entry.placement,
                    entry.parallelism,
                    entry.name.clone(),
                );
                let rebatched_plan = wrap_with_rebatch(
                    partitioned_plan,
                    entry.rebatch_config.batch_size.map(|s| s as usize),
                    entry.rebatch_config.batch_flush_interval,
                    entry.name.clone(),
                );
                LogicalPlanBuilder::insert_into(
                    rebatched_plan,
                    entry.name.as_str(),
                    provider_as_source(entry.provider.clone()),
                    InsertOp::Append,
                )
                .map_err(|e| {
                    streamling_core::error::StreamlingError::from(e).context(format!(
                        "failed to build insert plan for sink [{}]",
                        entry.name
                    ))
                })?
                .build()
                .map_err(|e| {
                    streamling_core::error::StreamlingError::from(e).context(format!(
                        "failed to build insert plan for sink [{}]",
                        entry.name
                    ))
                })?
            };

            if !dry_run {
                let session_manager = session_manager.clone();
                let checkpoint_control = checkpoint_control.clone();
                let sink_future = async move {
                    // `DataFrame::collect`, split so the planned physical
                    // plan can be logged before execution starts.
                    let df = session_manager.new_df(sink_plan);
                    let task_ctx = Arc::new(df.task_ctx());
                    let result = match df.create_physical_plan().await {
                        Ok(plan) => {
                            info!(
                                "Pipeline physical plan:\n{}",
                                displayable(plan.as_ref()).indent(true)
                            );
                            collect(plan, task_ctx).await
                        }
                        Err(err) => Err(err),
                    };
                    match &result {
                        Ok(_) => {
                            // The sink has SUCCESSFULLY drained its input and
                            // will not ack any further checkpoint epochs. Tell
                            // the coordinator so it drops these sinks from the
                            // expected-ack set and can finalize in-flight
                            // epochs the remaining live sinks already acked,
                            // instead of blocking on a sink that is gone (the
                            // multi-source completion case).
                            for sink_name in &sink_names {
                                checkpoint_control.sink_completed(sink_name);
                            }
                        }
                        Err(err) => {
                            // A FAILED sink must NOT be deregistered: its
                            // missing acks are the coordinator's only signal
                            // that the epochs it touched are not durable.
                            // Removing it would let those epochs (including
                            // the terminal one) finalize as if the data had
                            // been written. The pipeline is failing anyway —
                            // let the epochs stall and the error propagate.
                            error!(
                                "Sink future [{}] completed with error: {}",
                                future_name, err
                            );
                        }
                    }
                    result
                };
                sink_futures.push(sink_future);
            } else {
                dry_run_plans.push((future_name, sink_plan));
            }
        }

        // During dry_run, create physical plans to catch type coercion
        // and other SQL errors that only manifest at physical planning time.
        if dry_run {
            for (name, plan) in dry_run_plans {
                session_manager
                    .new_df(plan)
                    .create_physical_plan()
                    .await
                    .streamling_with_context(|| {
                        format!(
                            "SQL validation failed for sink [{}]: \
                             the query plan contains type errors that would crash at runtime",
                            name
                        )
                    })?;
            }
        }

        // Start checkpoint coordinator only if not in dry_run mode
        if !dry_run {
            checkpoint_coordinator.start(
                self.app_config.checkpoint_interval_sec,
                checkpoint_sink_names,
            );
        }

        // One top-level shutdown trigger. On SIGTERM/SIGINT it flips the shared
        // signal (sources drain front-to-back) and arms the watchdog. This
        // replaces the per-component signal handlers (notably the plugin
        // watcher that used to kill plugins first, inverting the drain order).
        if !dry_run {
            tokio::spawn(async move {
                Self::wait_for_shutdown_signal().await;
                info!("Shutdown signal received; draining pipeline front-to-back");
                streamling_core::shutdown::request_shutdown();
                Self::arm_shutdown_watchdog(Self::shutdown_budget());
            });
        }

        // Plugin dispatchers run as detached tasks; their execution futures are
        // join wrappers. We keep them in a set so that AFTER the sinks drain we
        // can send Terminate and AWAIT the dispatchers finishing their flush,
        // rather than dropping them and letting the runtime cancel them
        // mid-flush at process exit (the job-mode tail-loss bug).
        let mut plugin_set: futures::stream::FuturesUnordered<ExecutionFuture> =
            plugins.into_values().map(|p| p.execution_future).collect();

        // Drive to completion. The terminal condition is "all sinks drained"
        // (every source ended its stream, via bounded completion or shutdown).
        // A plugin dispatcher exiting on its own is NOT terminal unless it
        // errored — otherwise we would drop in-flight sink work and lose the
        // tail, exactly the previous behaviour.
        // The sinks are driven individually (not try_join_all): a failing sink
        // must not CANCEL its siblings mid-flush — dropping their futures would
        // abort in-flight writes and re-widen the very tail-loss window this
        // change closes. Instead, the first failure triggers the same graceful
        // drain as SIGTERM (sources stop, streams end, the remaining sinks
        // finish flushing — all bounded by the watchdog), and the error is
        // propagated once every sink has wound down. No epoch a failed sink
        // touched is ever acked, so at-least-once is preserved either way.
        let app_result: Result<()> = {
            use futures::StreamExt as _;
            let mut sink_set: futures::stream::FuturesUnordered<_> =
                sink_futures.into_iter().collect();
            let mut first_error: Option<streamling_core::error::StreamlingError> = None;
            let fail_drain = |err: streamling_core::error::StreamlingError,
                              first_error: &mut Option<
                streamling_core::error::StreamlingError,
            >| {
                if first_error.is_none() {
                    if !dry_run {
                        warn!(
                            "Pipeline component failed; draining remaining sinks before exit: {}",
                            err
                        );
                        streamling_core::shutdown::request_shutdown();
                        Self::arm_shutdown_watchdog(Self::shutdown_budget());
                    }
                    *first_error = Some(err);
                }
            };
            while !sink_set.is_empty() {
                tokio::select! {
                    Some(sink_res) = sink_set.next() => {
                        if let Err(e) = sink_res {
                            debug!("A sink future completed with error");
                            fail_drain(e.into(), &mut first_error);
                        }
                    }
                    Some(plugin_res) = plugin_set.next() => {
                        match plugin_res {
                            Ok(()) => {
                                debug!("A plugin future completed; continuing to drain sinks");
                            }
                            Err(msg) => {
                                fail_drain(
                                    streamling_err!("Plugin error: {}", msg),
                                    &mut first_error,
                                );
                            }
                        }
                    }
                }
            }
            match first_error {
                None => Ok(()),
                Some(e) => Err(e),
            }
        };

        // Teardown. Everything below is bounded by a single deadline shared
        // with the watchdog: arming is idempotent and returns the deadline of
        // the FIRST arming (SIGTERM time, if one arrived), so after a long
        // drain the bounded waits below correctly see less time remaining
        // instead of overrunning into the hard exit. A 2s margin keeps these
        // waits expiring before the watchdog fires. Dry runs never arm the
        // watchdog (there is nothing to drain).
        let deadline = if dry_run {
            std::time::Instant::now() + Self::shutdown_budget()
        } else {
            let watchdog_deadline = Self::arm_shutdown_watchdog(Self::shutdown_budget());
            watchdog_deadline
                .checked_sub(Duration::from_secs(2))
                .unwrap_or(watchdog_deadline)
        };
        let remaining = || deadline.saturating_duration_since(std::time::Instant::now());
        if !dry_run {
            // The sinks drained. Before tearing anything down, wait (bounded)
            // for the terminal checkpoint to finalize so the tail is durably
            // checkpointed and its Finalizer reaches components that commit on
            // it. No-op in the streaming case where no terminal checkpoint was
            // begun, and skipped on error (none will arrive).
            if app_result.is_ok()
                && timeout(remaining(), checkpoint_control.await_terminal_finalized())
                    .await
                    .is_err()
            {
                warn!(
                    "Terminal checkpoint did not finalize within budget; proceeding with shutdown"
                );
            }
            // Ensure sources observe shutdown even on a clean job-mode completion
            // (so any lingering helper tasks — lag reporters — wind down).
            streamling_core::shutdown::request_shutdown();
        }

        // Issue SourceComplete to plugin sources so the checkpoint channels are
        // fully cleaned up when the plugins terminate.
        {
            use streamling_core::checkpoints::channels::send as checkpoint_send;
            use streamling_core::checkpoints::checkpoint_management::CheckpointMessage;

            for (source_name, source) in &pipeline_topology.sources {
                if let topology::Source::plugin(_) = source {
                    let _ = checkpoint_send(
                        source_name.as_str(),
                        CheckpointMessage::SourceComplete(source_name.clone()),
                    );
                }
            }
        }

        shutdown_plugin_side_outputs();

        // Terminate plugins, then AWAIT their dispatchers finishing their drain
        // (bounded) so the last buffered batches are flushed durably before the
        // runtime is dropped. This is the fix for the plugin/pubsub tail loss.
        terminate_all_plugins()?;
        if !plugin_set.is_empty() {
            use futures::StreamExt as _;
            let drain = async {
                while let Some(res) = plugin_set.next().await {
                    if let Err(msg) = res {
                        warn!("Plugin exited with error during drain: {}", msg);
                    }
                }
            };
            match timeout(remaining(), drain).await {
                Ok(()) => info!("All plugin dispatchers drained cleanly"),
                Err(_) => warn!(
                    "Plugin drain exceeded the shutdown budget; {} dispatcher(s) may not have flushed",
                    plugin_set.len()
                ),
            }
        }

        // Stop the checkpoint coordinator only if it was started (not dry_run).
        if !dry_run {
            match timeout(remaining(), checkpoint_coordinator.stop()).await {
                Ok(_) => {}
                Err(_) => {
                    warn!("Checkpoint coordinator stop timed out; continuing anyway");
                }
            }
        }

        app_result?;
        Ok(())
    }

    /// The total time budget for graceful shutdown. Delegates to the shared
    /// definition in `streamling_core::shutdown` so every consumer (this run
    /// loop's watchdog, the hybrid source's terminal-finalize wait) reads the
    /// same value and they can never drift.
    fn shutdown_budget() -> Duration {
        streamling_core::shutdown::shutdown_budget()
    }

    /// Arm a last-resort watchdog: a plain OS thread that hard-exits the process
    /// after `budget`, so a wedged FFI/rdkafka teardown or a plugin that never
    /// finishes its drain can never hold the process past the k8s grace period.
    ///
    /// Idempotent — the first arming wins and later calls are no-ops. Always
    /// returns the deadline of the FIRST arming so callers slice their bounded
    /// waits against the watchdog's real deadline (e.g. teardown after a long
    /// SIGTERM-triggered drain must not assume a fresh budget).
    ///
    /// The forced exit is non-zero: this path only fires when teardown failed
    /// to complete in time, and it must surface to k8s/alerting as an abnormal
    /// exit, not a clean completion.
    fn arm_shutdown_watchdog(budget: Duration) -> std::time::Instant {
        use std::sync::OnceLock;
        static WATCHDOG_DEADLINE: OnceLock<std::time::Instant> = OnceLock::new();
        *WATCHDOG_DEADLINE.get_or_init(|| {
            let deadline = std::time::Instant::now() + budget;
            let spawn_result = std::thread::Builder::new()
                .name("shutdown-watchdog".to_string())
                .spawn(move || {
                    std::thread::sleep(budget);
                    // A plain OS thread's exit cannot be blocked by a wedged
                    // tokio worker or a hung FFI call, so this always fires.
                    eprintln!(
                        "[streamling] shutdown budget of {:?} exceeded; forcing process exit",
                        budget
                    );
                    std::process::exit(1);
                });
            if let Err(e) = spawn_result {
                // Without the watchdog nothing guarantees the process exits
                // before the grace period; make that loudly visible instead of
                // failing silently (behaviour then degrades to pre-watchdog).
                eprintln!(
                    "[streamling] FAILED to spawn shutdown watchdog thread: {}; \
                     a wedged teardown can no longer be force-exited",
                    e
                );
            }
            deadline
        })
    }

    /// Await SIGTERM / SIGINT / Ctrl-C — the single shutdown trigger for the
    /// whole process, replacing the per-component signal handlers.
    async fn wait_for_shutdown_signal() {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let handlers = (|| {
                Ok::<_, std::io::Error>((
                    signal(SignalKind::terminate())?,
                    signal(SignalKind::interrupt())?,
                ))
            })();
            let (mut sigterm, mut sigint) = match handlers {
                Ok(h) => h,
                Err(e) => {
                    // Installation failed: the caller treats this function
                    // returning as "signal received" and starts a drain, so
                    // returning here would trigger a SPURIOUS shutdown at
                    // startup. Pend forever instead — the process keeps
                    // running and degrades to the OS default SIGTERM
                    // disposition (immediate kill, no graceful drain), which
                    // is the pre-existing behaviour without a handler.
                    error!(
                        "Failed to install SIGTERM/SIGINT handlers ({}); \
                         graceful shutdown on signal is DISABLED for this run",
                        e
                    );
                    futures::future::pending::<()>().await;
                    unreachable!("pending() never resolves");
                }
            };
            tokio::select! {
                _ = sigterm.recv() => info!("Received SIGTERM"),
                _ = sigint.recv() => info!("Received SIGINT"),
                _ = tokio::signal::ctrl_c() => info!("Received Ctrl-C"),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            info!("Received Ctrl-C");
        }
    }

    fn find_plan_and_schema(
        pipeline_plans: &HashMap<String, LogicalPlan>,
        name: &str,
    ) -> Result<(LogicalPlan, SchemaRef)> {
        let plan = pipeline_plans
            .get(name)
            .ok_or_else(|| {
                streamling_user_err!("failed to construct topology, plan '{}' not found", name)
            })?
            .clone();
        let schema = plan.schema().inner().clone();
        Ok((plan, schema))
    }

    fn update_source_to_sink_mapping(
        sources_to_sinks: &mut SourceToSinkMapping,
        source_name: String,
        source_plan: LogicalPlan,
        sink_entry: SinkEntry,
    ) {
        if let Some((_, sinks)) = sources_to_sinks.get_mut(source_name.as_str()) {
            sinks.push(sink_entry);
        } else {
            sources_to_sinks.insert(source_name, (source_plan, vec![sink_entry]));
        }
    }

    /// Analyze the topology to determine which sources and transforms have multiple consumers
    /// Returns a HashMap mapping reference names (sources and transforms) to their consumer count
    fn find_source_consumers(pipeline_topology: &PipelineTopology) -> HashMap<String, usize> {
        let mut node_consumers: HashMap<String, usize> = HashMap::new();

        // Initialize all sources with 0 consumers
        for source_name in pipeline_topology.sources.keys() {
            node_consumers.insert(source_name.clone(), 0);
        }

        // Initialize all transforms with 0 consumers
        for transform_name in pipeline_topology.transforms.keys() {
            node_consumers.insert(transform_name.clone(), 0);
        }

        // Count consumers from transforms
        for transform in pipeline_topology.transforms.values() {
            match transform {
                topology::Transform::sql(sql_transform) => {
                    // Extract table references from SQL
                    if let Ok(table_names) = extract_table_references_from_sql(&sql_transform.sql) {
                        for table_name in table_names {
                            if let Some(count) = node_consumers.get_mut(&table_name) {
                                *count += 1;
                            }
                        }
                    }
                }
                topology::Transform::handler(handler) => {
                    if let Some(count) = node_consumers.get_mut(&handler.from) {
                        *count += 1;
                    }
                }
                topology::Transform::script(script) => {
                    if let Some(count) = node_consumers.get_mut(&script.from) {
                        *count += 1;
                    }
                }
                topology::Transform::plugin(plugin) => {
                    if let Some(count) = node_consumers.get_mut(&plugin.from) {
                        *count += 1;
                    }
                }
                topology::Transform::dynamic_table(_) => {
                    // Dynamic tables don't directly consume from sources in the same way
                }
            }
        }

        // Count sink consumers (MultiSink groups count as 1)
        let mut sink_sources = HashSet::new();
        for sink in pipeline_topology.sinks.values() {
            let from = match sink {
                topology::Sink::webhook(webhook) => &webhook.from,
                topology::Sink::print(print) => &print.from,
                topology::Sink::blackhole(blackhole) => &blackhole.from,
                topology::Sink::postgres(postgres) => &postgres.from,
                topology::Sink::postgres_aggregate(postgres) => &postgres.from,
                topology::Sink::memory(memory) => &memory.from,
                topology::Sink::kafka(kafka) => &kafka.from,
                topology::Sink::clickhouse(clickhouse) => &clickhouse.from,
                topology::Sink::plugin(plugin) => &plugin.from,
            };
            sink_sources.insert(from.clone());
        }

        for from in sink_sources {
            if let Some(count) = node_consumers.get_mut(&from) {
                *count += 1;
            }
        }

        node_consumers
    }

    fn build_node_contexts(pipeline_topology: &PipelineTopology) -> HashMap<String, NodeContext> {
        let mut contexts = HashMap::new();

        for (reference_name, source) in &pipeline_topology.sources {
            let operator_type = match source {
                topology::Source::kafka(_) => "kafka",
                topology::Source::clickhouse(_) => "clickhouse",
                topology::Source::hybrid(_) => "hybrid",
                topology::Source::file(_) => "file",
                topology::Source::plugin(_) => "plugin",
            };
            contexts.insert(
                reference_name.clone(),
                NodeContext::new(TopologyNodeType::Source, operator_type, reference_name),
            );
        }

        for (reference_name, transform) in &pipeline_topology.transforms {
            let operator_type = match transform {
                topology::Transform::dynamic_table(_) => "dynamic_table",
                topology::Transform::sql(_) => "sql",
                topology::Transform::handler(_) => "handler",
                topology::Transform::script(_) => "script",
                topology::Transform::plugin(_) => "plugin",
            };
            contexts.insert(
                reference_name.clone(),
                NodeContext::new(TopologyNodeType::Transform, operator_type, reference_name),
            );
        }

        for (reference_name, sink) in &pipeline_topology.sinks {
            let operator_type = match sink {
                topology::Sink::webhook(_) => "webhook",
                topology::Sink::print(_) => "print",
                topology::Sink::blackhole(_) => "blackhole",
                topology::Sink::postgres(_) => "postgres",
                topology::Sink::postgres_aggregate(_) => "postgres_aggregate",
                topology::Sink::memory(_) => "memory",
                topology::Sink::kafka(_) => "kafka",
                topology::Sink::clickhouse(_) => "clickhouse",
                topology::Sink::plugin(_) => "plugin",
            };
            contexts.insert(
                reference_name.clone(),
                NodeContext::new(TopologyNodeType::Sink, operator_type, reference_name),
            );
        }

        contexts
    }

    fn build_pipeline_metric_metadata(
        node_contexts: &HashMap<String, NodeContext>,
        pipeline_topology: &PipelineTopology,
        application_id: &str,
    ) -> HashMap<String, PipelineMetricMetadata> {
        let mut metadata_map: HashMap<String, PipelineMetricMetadata> = HashMap::new();
        let mut dependency_map: HashMap<String, Vec<String>> = HashMap::new();
        // mapping from plain reference name to composite metadata key
        let mut ref_to_key: HashMap<String, String> = HashMap::new();

        for (reference_name, source) in &pipeline_topology.sources {
            let node_context = node_contexts
                .get(reference_name)
                .expect("node context must exist")
                .clone();
            let mut additional_tags = match source {
                topology::Source::kafka(kafka) => metric_tags([("topic", kafka.topic.as_str())]),
                topology::Source::clickhouse(clickhouse) => {
                    metric_tags([("table", clickhouse.table_name.as_str())])
                }
                topology::Source::hybrid(_) => metric_tags([]),
                topology::Source::file(file) => metric_tags([("path", file.path.as_str())]),
                topology::Source::plugin(plugin) => metric_tags([("type", plugin.r#type.as_str())]),
            };
            merge_labels(
                &mut additional_tags,
                source.telemetry().and_then(topology::Telemetry::labels),
            );

            let metadata = PipelineMetricMetadata {
                node_context,
                additional_tags,
                children_metadata_ids: Vec::new(),
                service_instance_id: application_id.to_string(),
            };
            let key = metric_key(application_id, reference_name.as_str());
            ref_to_key.insert(reference_name.clone(), key.clone());
            metadata_map.insert(key, metadata);
            dependency_map.insert(reference_name.clone(), Vec::new());
            if let topology::Source::hybrid(hybrid) = source {
                // Hybrid phase-child metadata entries inherit the parent
                // hybrid source's YAML labels. Operators declare labels on
                // the logical hybrid source, not on the synthetic
                // bounded/unbounded phase metadata — but both phases emit
                // their own `streamling_event_time_*` and row-count series,
                // so the labels must follow for dashboards to find them.
                let parent_labels = hybrid
                    .telemetry
                    .as_ref()
                    .and_then(topology::Telemetry::labels);
                let bounded_sources = &hybrid.bounded_sources;
                let unbounded_source = &hybrid.unbounded_source;
                bounded_sources.iter().enumerate().for_each(|(idx, b_src)| {
                    let id = format!("{}_bounded_{}", reference_name, idx);
                    let mut bounded_tags =
                        BTreeMap::from([(String::from("table"), b_src.table_name.to_string())]);
                    merge_labels(&mut bounded_tags, parent_labels);
                    let bounded_src_metadata = PipelineMetricMetadata {
                        node_context: NodeContext::new(
                            TopologyNodeType::Source,
                            &b_src.source_type,
                            &id,
                        ),
                        service_instance_id: application_id.to_string(),
                        additional_tags: bounded_tags,
                        children_metadata_ids: vec![],
                    };
                    let metric_key =
                        metric_key_hybrid_src_bounded(application_id, reference_name.as_str(), idx);
                    metadata_map.insert(metric_key, bounded_src_metadata);
                });
                let id = format!("{}_unbounded", reference_name);
                let mut unbounded_tags =
                    BTreeMap::from([(String::from("topic"), unbounded_source.topic.to_string())]);
                merge_labels(&mut unbounded_tags, parent_labels);
                let unbounded_src_metadata = PipelineMetricMetadata {
                    node_context: NodeContext::new(
                        TopologyNodeType::Source,
                        &unbounded_source.source_type,
                        &id,
                    ),
                    service_instance_id: application_id.to_string(),
                    additional_tags: unbounded_tags,
                    children_metadata_ids: vec![],
                };
                let metric_key =
                    metric_key_hybrid_src_unbounded(application_id, reference_name.as_str());
                metadata_map.insert(metric_key, unbounded_src_metadata);
            }
        }

        for (reference_name, transform) in &pipeline_topology.transforms {
            let node_context = node_contexts
                .get(reference_name)
                .expect("node context must exist")
                .clone();
            let (mut additional_tags, upstream_table_refs) = match transform {
                topology::Transform::sql(sql_transform) => {
                    let table_names = match extract_table_references_from_sql(&sql_transform.sql) {
                        Ok(names) => names,
                        Err(e) => {
                            warn!(
                                "Cannot extract upstream references from sql query: {}. This may impact telemetry for this transform and children. Error: {}",
                                sql_transform.sql, e
                            );
                            continue;
                        }
                    };
                    (metric_tags([]), table_names)
                }
                topology::Transform::handler(handler) => {
                    (metric_tags([]), vec![handler.from.clone()])
                }
                topology::Transform::script(script) => (
                    metric_tags([("language", script.language.as_str())]),
                    vec![script.from.clone()],
                ),
                topology::Transform::plugin(plugin) => (
                    metric_tags([("type", plugin.r#type.as_str())]),
                    vec![plugin.from.clone()],
                ),
                topology::Transform::dynamic_table(_) => (BTreeMap::new(), vec![]),
            };
            merge_labels(
                &mut additional_tags,
                transform.telemetry().and_then(topology::Telemetry::labels),
            );

            let metadata = PipelineMetricMetadata {
                node_context,
                additional_tags,
                children_metadata_ids: Vec::new(),
                service_instance_id: application_id.to_string(),
            };
            let key = metric_key(application_id, reference_name.as_str());
            ref_to_key.insert(reference_name.clone(), key.clone());
            metadata_map.insert(key, metadata);
            upstream_table_refs.iter().for_each(|table_name| {
                dependency_map
                    .entry(table_name.into())
                    .or_default()
                    .push(reference_name.clone());
            });
        }

        for (reference_name, sink) in &pipeline_topology.sinks {
            let node_context = node_contexts
                .get(reference_name)
                .expect("node context must exist")
                .clone();
            let (mut additional_tags, from) = match sink {
                topology::Sink::webhook(webhook) => (
                    metric_tags([("url", webhook.url.as_str())]),
                    webhook.from.clone(),
                ),
                topology::Sink::print(print) => (metric_tags([]), print.from.clone()),
                topology::Sink::blackhole(blackhole) => (metric_tags([]), blackhole.from.clone()),
                topology::Sink::postgres(postgres) => (
                    metric_tags([("table", postgres.table.as_str())]),
                    postgres.from.clone(),
                ),
                topology::Sink::postgres_aggregate(postgres) => (
                    metric_tags([("table", postgres.landing_table.as_str())]),
                    postgres.from.clone(),
                ),
                topology::Sink::memory(memory) => (metric_tags([]), memory.from.clone()),
                topology::Sink::kafka(kafka) => (
                    metric_tags([("topic", kafka.topic.as_str())]),
                    kafka.from.clone(),
                ),
                topology::Sink::clickhouse(clickhouse) => (
                    metric_tags([("table", clickhouse.table.as_str())]),
                    clickhouse.from.clone(),
                ),
                topology::Sink::plugin(plugin) => (
                    metric_tags([("type", plugin.r#type.as_str())]),
                    plugin.from.clone(),
                ),
            };
            merge_labels(
                &mut additional_tags,
                sink.telemetry().and_then(topology::Telemetry::labels),
            );

            let metadata = PipelineMetricMetadata {
                node_context,
                additional_tags,
                children_metadata_ids: Vec::new(),
                service_instance_id: application_id.to_string(),
            };
            let key = metric_key(application_id, reference_name.as_str());
            ref_to_key.insert(reference_name.clone(), key.clone());
            metadata_map.insert(key, metadata);
            dependency_map
                .entry(from)
                .or_default()
                .push(reference_name.clone());
        }

        for (parent_ref_name, children_names) in dependency_map {
            let mut children_metadata = Vec::new();
            for child_ref_name in children_names {
                if let Some(child_key) = ref_to_key.get(&child_ref_name) {
                    children_metadata.push(child_key.clone());
                }
            }
            if let Some(parent_key) = ref_to_key.get(&parent_ref_name)
                && let Some(parent_metric_metadata) = metadata_map.get_mut(parent_key)
            {
                parent_metric_metadata.children_metadata_ids = children_metadata;
            }
        }

        metadata_map
    }

    /// Helper function to convert HashMap<String, serde_yaml::Value> to HashMap<String, String>
    /// by serializing each value to a YAML string representation
    fn convert_plugin_options(
        options: HashMap<String, serde_yaml::Value>,
    ) -> HashMap<String, String> {
        options
            .into_iter()
            .map(|(k, v)| {
                let value_str = match v {
                    serde_yaml::Value::String(s) => s,
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Null => "null".to_string(),
                    _ => serde_yaml::to_string(&v)
                        .unwrap_or_else(|_| panic!("Failed to serialize plugin option: {}", k)),
                };
                (k, value_str)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink_entry(name: &str, placement: Placement, parallelism: Option<usize>) -> SinkEntry {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]));
        let provider =
            Arc::new(datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap());
        SinkEntry::new(
            name.to_string(),
            provider,
            RebatchConfig::default(),
            placement,
            parallelism,
        )
    }

    fn by_key(columns: &[&str]) -> Placement {
        Placement::ByKey(columns.iter().map(|c| c.to_string()).collect())
    }

    /// A transform's `parallelism` has to widen the transform's own work, so the
    /// exchange belongs under its SQL. Placed above, the filter would keep
    /// running at the source's width and only downstream nodes would widen —
    /// which is the knob doing nothing for the case it exists for.
    ///
    /// Also pins that `PushDownFilter` does not slide the filter back under the
    /// exchange: `RepartitionNode`'s default `prevent_predicate_push_down_columns`
    /// covers every column.
    #[tokio::test]
    async fn transform_parallelism_widens_the_transform_itself() {
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::physical_plan::displayable;
        use streamling_core::dynamic_table::DynamicTableRegistry;
        use streamling_core::session::SessionManager;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("vid", DataType::Int64, false),
            Field::new("_gs_op", DataType::Utf8, false),
        ]));
        // Three input partitions, like a `parallelism: 3` kafka source.
        let source =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![], vec![], vec![]])
                .unwrap();

        let session_manager = SessionManager::new(100, 10, DynamicTableRegistry::new(), 4).unwrap();
        session_manager
            .session_context()
            .register_table("blocks", Arc::new(source))
            .unwrap();

        let (sql_plan, source_name) = session_manager
            .create_supported_logical_plan("select * from blocks where vid = 100".to_string())
            .await
            .unwrap();

        let sql_plan = wrap_transform_input_with_repartition(
            sql_plan,
            &source_name,
            &by_key(&["id"]),
            4,
            "filter_blocks",
        )
        .unwrap();
        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(CheckpointableNode::new(
                sql_plan,
                10,
                "filter_blocks".to_string(),
            )),
        });

        let physical_plan = session_manager
            .new_df(logical_plan)
            .create_physical_plan()
            .await
            .unwrap();
        let rendered = displayable(physical_plan.as_ref()).indent(true).to_string();

        // The exchange sits directly above the scan, and everything above it —
        // the filter and the checkpoint wrapper — runs at the widened width.
        for expected in [
            "CheckpointableExec (for FilterExec), partitions=4",
            "FilterExec: vid@1 = 100, partitions=4",
            "StreamingRepartitionExec: partitions=4, keys=[id@0]",
        ] {
            assert!(
                rendered.contains(expected),
                "expected {expected:?} in plan:\n{rendered}"
            );
        }
    }

    fn empty_plan() -> LogicalPlan {
        LogicalPlanBuilder::empty(true).build().unwrap()
    }

    fn repartition_node(plan: &LogicalPlan) -> Option<&RepartitionNode> {
        match plan {
            LogicalPlan::Extension(extension) => {
                extension.node.as_any().downcast_ref::<RepartitionNode>()
            }
            _ => None,
        }
    }

    /// Sinks that agree on a key can all be served by one exchange, so the group
    /// stays as wide as the widest `parallelism` any of them asked for.
    #[test]
    fn multi_sink_group_with_one_key_gets_one_exchange() {
        let sinks = vec![
            sink_entry("a", by_key(&["id"]), Some(4)),
            sink_entry("b", by_key(&["id"]), None),
        ];

        let plan = wrap_multi_sink_with_repartition(empty_plan(), &sinks, "a, b");

        let node = repartition_node(&plan).expect("expected a RepartitionNode");
        assert_eq!(node.placement, Placement::ByKey(vec!["id".to_string()]));
        assert_eq!(node.target_parallelism, Some(4));
    }

    /// Nothing to key by and nothing to widen to: the group inherits its input's
    /// width with no exchange at all.
    #[test]
    fn keyless_multi_sink_group_is_left_alone() {
        let sinks = vec![
            sink_entry("a", Placement::RoundRobin, None),
            sink_entry("b", Placement::RoundRobin, None),
        ];

        let plan = wrap_multi_sink_with_repartition(empty_plan(), &sinks, "a, b");

        assert!(
            repartition_node(&plan).is_none(),
            "a keyless fan-out needs no exchange"
        );
    }

    /// A sink that cannot be parallelized has to get its node even with nothing
    /// to key by and no width to hit — the node *is* what narrows its input.
    #[test]
    fn single_sink_gets_a_coalescing_node() {
        let plan = wrap_with_repartition(
            empty_plan(),
            &Placement::Single,
            None,
            "webhook_sink".to_string(),
        );

        let node = repartition_node(&plan).expect("expected a RepartitionNode");
        assert_eq!(node.placement, Placement::Single);
    }

    /// `MultiSinkExec` fans one `write_all` per input partition per sink, so a
    /// sink that cannot be parallelized narrows the whole group — there is no
    /// way to keep the others wide.
    #[test]
    fn a_single_sink_forces_the_whole_group_to_one_stream() {
        let sinks = vec![
            sink_entry("warehouse", by_key(&["id"]), None),
            sink_entry("webhook", Placement::Single, None),
        ];

        let plan = wrap_multi_sink_with_repartition(empty_plan(), &sinks, "warehouse, webhook");

        let node = repartition_node(&plan).expect("expected a RepartitionNode");
        assert_eq!(node.placement, Placement::Single);
    }

    /// The sinks share one input, so one exchange has to serve all of them. When
    /// they disagree on the key, no placement is correct for every sink and the
    /// group falls back to a single stream.
    #[test]
    fn multi_sink_group_with_conflicting_keys_falls_back_to_one_stream() {
        let sinks = vec![
            sink_entry("a", by_key(&["id"]), None),
            sink_entry("b", by_key(&["account"]), None),
        ];

        let plan = wrap_multi_sink_with_repartition(empty_plan(), &sinks, "a, b");

        let node = repartition_node(&plan).expect("expected a RepartitionNode");
        assert_eq!(node.target_parallelism, Some(1));
        assert_eq!(
            node.placement,
            Placement::RoundRobin,
            "a single stream needs no key to hash by"
        );
    }

    /// A keyless sink alongside keyed ones is not a conflict: it does not care
    /// which stream a row lands on, so the keyed sinks' placement wins.
    #[test]
    fn a_keyless_sink_does_not_conflict_with_a_keyed_one() {
        let sinks = vec![
            sink_entry("printer", Placement::RoundRobin, None),
            sink_entry("warehouse", by_key(&["id"]), None),
        ];

        let plan = wrap_multi_sink_with_repartition(empty_plan(), &sinks, "printer, warehouse");

        let node = repartition_node(&plan).expect("expected a RepartitionNode");
        assert_eq!(node.placement, Placement::ByKey(vec!["id".to_string()]));
    }

    /// Print and blackhole neither dedupe nor depend on ordering, so a group of
    /// them widens round-robin — no primary key required.
    #[test]
    fn keyless_multi_sink_group_widens_round_robin() {
        let sinks = vec![
            sink_entry("printer", Placement::RoundRobin, Some(4)),
            sink_entry("void", Placement::RoundRobin, None),
        ];

        let plan = wrap_multi_sink_with_repartition(empty_plan(), &sinks, "printer, void");

        let node = repartition_node(&plan).expect("expected a RepartitionNode");
        assert_eq!(node.placement, Placement::RoundRobin);
        assert_eq!(node.target_parallelism, Some(4));
    }

    #[test]
    fn test_normalize_secret_name_hyphens_and_dots() {
        assert_eq!(
            normalize_secret_name("my-webhook.token"),
            "my_webhook_token"
        );
    }

    #[test]
    fn test_normalize_secret_name_uppercase_becomes_lowercase() {
        assert_eq!(normalize_secret_name("MY_TOKEN"), "my_token");
    }

    #[test]
    fn test_normalize_secret_name_mixed() {
        assert_eq!(normalize_secret_name("my-token"), "my_token");
    }

    fn make_secret_maps(
        name: &str,
        header_name: &str,
        header_value: &str,
    ) -> (HashMap<String, String>, HashMap<String, String>) {
        let mut headers = HashMap::new();
        headers.insert(name.to_string(), header_name.to_string());
        let mut values = HashMap::new();
        values.insert(name.to_string(), header_value.to_string());
        (headers, values)
    }

    #[test]
    fn test_merge_secret_into_headers_injects_configured_header() {
        let (secret_headers, secret_values) =
            make_secret_maps("my_token", "Authorization", "Bearer abc123");
        let result = merge_secret_into_headers(
            "my_webhook",
            "my-token",
            None,
            &secret_headers,
            &secret_values,
        )
        .unwrap();
        let headers = result.unwrap();
        assert_eq!(
            headers.get("Authorization").map(String::as_str),
            Some("Bearer abc123")
        );
    }

    #[test]
    fn test_merge_secret_into_headers_injects_arbitrary_header() {
        let (secret_headers, secret_values) =
            make_secret_maps("my_token", "X-Api-Key", "secret-key-123");
        let result = merge_secret_into_headers(
            "my_webhook",
            "my-token",
            None,
            &secret_headers,
            &secret_values,
        )
        .unwrap();
        let headers = result.unwrap();
        assert_eq!(
            headers.get("X-Api-Key").map(String::as_str),
            Some("secret-key-123")
        );
    }

    #[test]
    fn test_merge_secret_into_headers_preserves_other_headers() {
        let (secret_headers, secret_values) =
            make_secret_maps("my_token", "Authorization", "Bearer abc123");
        let mut existing = BTreeMap::new();
        existing.insert("X-Custom".to_string(), "value".to_string());
        let result = merge_secret_into_headers(
            "my_webhook",
            "my-token",
            Some(existing),
            &secret_headers,
            &secret_values,
        )
        .unwrap();
        let headers = result.unwrap();
        assert_eq!(
            headers.get("Authorization").map(String::as_str),
            Some("Bearer abc123")
        );
        assert_eq!(headers.get("X-Custom").map(String::as_str), Some("value"));
    }

    #[test]
    fn test_merge_secret_into_headers_errors_on_header_conflict() {
        let (secret_headers, secret_values) =
            make_secret_maps("my_token", "Authorization", "Bearer abc123");
        let mut conflicting = BTreeMap::new();
        conflicting.insert("Authorization".to_string(), "Bearer explicit".to_string());
        let result = merge_secret_into_headers(
            "my_webhook",
            "my-token",
            Some(conflicting),
            &secret_headers,
            &secret_values,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("my_webhook") && err.contains("Authorization"));
    }

    #[test]
    fn test_merge_secret_into_headers_errors_on_case_insensitive_conflict() {
        let (secret_headers, secret_values) =
            make_secret_maps("my_token", "Authorization", "Bearer abc123");
        let mut conflicting = BTreeMap::new();
        conflicting.insert("authorization".to_string(), "Bearer explicit".to_string());
        let result = merge_secret_into_headers(
            "my_webhook",
            "my-token",
            Some(conflicting),
            &secret_headers,
            &secret_values,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_merge_secret_into_headers_missing_secret() {
        let secret_headers = HashMap::new();
        let secret_values = HashMap::new();
        let result = merge_secret_into_headers(
            "my_webhook",
            "missing-secret",
            None,
            &secret_headers,
            &secret_values,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("missing-secret"));
        assert!(err.contains("STREAMLING__HTTP_SECRET_HEADER__"));
    }

    #[test]
    fn test_merge_secret_into_headers_missing_value() {
        let mut secret_headers = HashMap::new();
        secret_headers.insert("my_token".to_string(), "Authorization".to_string());
        let secret_values = HashMap::new();
        let result = merge_secret_into_headers(
            "my_webhook",
            "my-token",
            None,
            &secret_headers,
            &secret_values,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("my-token"));
        assert!(err.contains("STREAMLING__HTTP_SECRET_VALUE__"));
    }

    #[test]
    fn test_secret_name_to_resolve_skips_during_dry_run() {
        assert_eq!(secret_name_to_resolve(Some("missing-secret"), true), None);
        assert_eq!(
            secret_name_to_resolve(Some("my-secret"), false),
            Some("my-secret")
        );
    }

    // ------------------------------------------------------------------------
    // build_pipeline_metric_metadata: YAML labels seeded into additional_tags
    // ------------------------------------------------------------------------

    fn build_metadata_from_yaml(yaml: &str) -> HashMap<String, PipelineMetricMetadata> {
        let topology = PipelineTopology::load_from_string(yaml).unwrap();
        let node_contexts = Streamling::build_node_contexts(&topology);
        Streamling::build_pipeline_metric_metadata(&node_contexts, &topology, "test_app")
    }

    #[test]
    fn test_kafka_source_yaml_labels_merged_into_metadata() {
        let yaml = r#"
sources:
  blocks:
    type: kafka
    topic: v2.evm.blocks
    telemetry:
      labels:
        tier: critical
        dataset: v2.evm.blocks
transforms: {}
sinks: {}
"#;
        let map = build_metadata_from_yaml(yaml);
        let meta = map.get(&metric_key("test_app", "blocks")).unwrap();
        assert_eq!(
            meta.additional_tags.get("topic"),
            Some(&"v2.evm.blocks".to_string()),
            "per-type tag `topic` should still be seeded"
        );
        assert_eq!(
            meta.additional_tags.get("tier"),
            Some(&"critical".to_string())
        );
        assert_eq!(
            meta.additional_tags.get("dataset"),
            Some(&"v2.evm.blocks".to_string())
        );
    }

    #[test]
    fn test_yaml_label_preserves_per_type_tag() {
        // Per-type tags (`topic` on Kafka, `table` on ClickHouse, etc.)
        // are reserved at config load — see
        // `source_per_type_reserved_keys` in topology.rs. YAML labels
        // can add new dimensions but cannot override the identity tag
        // the host seeds from node config. Regression for the older v1
        // "YAML wins silently" behavior that could make dashboards go
        // dark without a diagnostic.
        let yaml = r#"
sources:
  blocks:
    type: kafka
    topic: v2.evm.blocks
    telemetry:
      labels:
        dataset: v2.evm.blocks
transforms: {}
sinks: {}
"#;
        let map = build_metadata_from_yaml(yaml);
        let meta = map.get(&metric_key("test_app", "blocks")).unwrap();
        assert_eq!(
            meta.additional_tags.get("topic"),
            Some(&"v2.evm.blocks".to_string()),
            "real topic must remain intact"
        );
        assert_eq!(
            meta.additional_tags.get("dataset"),
            Some(&"v2.evm.blocks".to_string())
        );
    }

    #[test]
    fn test_transform_yaml_labels_merged() {
        let yaml = r#"
sources: {}
transforms:
  enriched:
    type: sql
    primary_key: id
    sql: "SELECT * FROM foo"
    telemetry:
      labels:
        stage: enrichment
sinks: {}
"#;
        let map = build_metadata_from_yaml(yaml);
        let meta = map.get(&metric_key("test_app", "enriched")).unwrap();
        assert_eq!(
            meta.additional_tags.get("stage"),
            Some(&"enrichment".to_string())
        );
    }

    #[test]
    fn test_sink_yaml_labels_merged() {
        let yaml = r#"
sources: {}
transforms: {}
sinks:
  archive:
    type: blackhole
    from: x
    telemetry:
      labels:
        destination: cold-storage
"#;
        let map = build_metadata_from_yaml(yaml);
        let meta = map.get(&metric_key("test_app", "archive")).unwrap();
        assert_eq!(
            meta.additional_tags.get("destination"),
            Some(&"cold-storage".to_string())
        );
    }

    #[test]
    fn test_hybrid_source_labels_propagate_to_phase_children() {
        let yaml = r#"
sources:
  blocks:
    type: hybrid
    bounded_sources:
      - source_type: clickhouse
        table_name: blocks_historic
      - source_type: clickhouse
        table_name: blocks_recent
    unbounded_source:
      source_type: kafka
      topic: blocks_live
    telemetry:
      labels:
        dataset: v2.evm.blocks
        tier: critical
transforms: {}
sinks: {}
"#;
        let map = build_metadata_from_yaml(yaml);

        // Parent source metadata
        let parent = map.get(&metric_key("test_app", "blocks")).unwrap();
        assert_eq!(
            parent.additional_tags.get("dataset"),
            Some(&"v2.evm.blocks".to_string())
        );
        assert_eq!(
            parent.additional_tags.get("tier"),
            Some(&"critical".to_string())
        );

        // Bounded phase 0
        let bounded0 = map
            .get(&metric_key_hybrid_src_bounded("test_app", "blocks", 0))
            .expect("bounded_0 metadata must exist");
        assert_eq!(
            bounded0.additional_tags.get("dataset"),
            Some(&"v2.evm.blocks".to_string())
        );
        assert_eq!(
            bounded0.additional_tags.get("table"),
            Some(&"blocks_historic".to_string()),
            "phase-child keeps its own per-type tag"
        );

        // Bounded phase 1
        let bounded1 = map
            .get(&metric_key_hybrid_src_bounded("test_app", "blocks", 1))
            .expect("bounded_1 metadata must exist");
        assert_eq!(
            bounded1.additional_tags.get("dataset"),
            Some(&"v2.evm.blocks".to_string())
        );
        assert_eq!(
            bounded1.additional_tags.get("table"),
            Some(&"blocks_recent".to_string())
        );

        // Unbounded phase
        let unbounded = map
            .get(&metric_key_hybrid_src_unbounded("test_app", "blocks"))
            .expect("unbounded metadata must exist");
        assert_eq!(
            unbounded.additional_tags.get("dataset"),
            Some(&"v2.evm.blocks".to_string())
        );
        assert_eq!(
            unbounded.additional_tags.get("topic"),
            Some(&"blocks_live".to_string())
        );
    }

    #[test]
    fn test_node_without_telemetry_labels_has_only_per_type_tag() {
        let yaml = r#"
sources:
  blocks:
    type: kafka
    topic: v2.evm.blocks
transforms: {}
sinks: {}
"#;
        let map = build_metadata_from_yaml(yaml);
        let meta = map.get(&metric_key("test_app", "blocks")).unwrap();
        assert_eq!(meta.additional_tags.len(), 1);
        assert_eq!(
            meta.additional_tags.get("topic"),
            Some(&"v2.evm.blocks".to_string())
        );
    }

    #[test]
    fn test_plugin_transform_metric_metadata_keyed_by_composite_key() {
        let yaml = r#"
sources:
  blocks:
    type: kafka
    topic: v2.evm.blocks
transforms:
  token_metadata:
    type: test_transform_plugin
    from: blocks
sinks: {}
"#;
        let map = build_metadata_from_yaml(yaml);
        assert!(
            map.contains_key(&metric_key("test_app", "token_metadata")),
            "plugin transform metadata must be keyed by metric_key(app_id, reference_name)"
        );
        assert!(
            !map.contains_key("token_metadata"),
            "bare reference name is not a registry key; consumers must look up by the composite key"
        );
    }

    // ------------------------------------------------------------------
    // STRM-6281: Kafka sinks require a resolvable, non-empty primary key
    // ------------------------------------------------------------------

    fn pk_test_schema() -> SchemaRef {
        use arrow_schema::{DataType, Field, Schema};
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }

    /// Reproduces STRM-6281: the abstract-dataset preprocessor emits `primary_key: ""`
    /// for datasets with no primary key in the CMS. That empty string used to reach the
    /// Kafka sink as `Some("")` and crashloop at runtime with
    /// `primary key column '' not found in batch`. It must now fail validation up front.
    #[test]
    fn test_require_primary_key_for_sink_rejects_empty_string() {
        let registry = PrimaryKeyRegistry::new(false);
        let err = registry
            .require_primary_key_for_sink(
                "kafka",
                &Some(String::new()),
                "upstream".to_string(),
                "my_kafka_sink".to_string(),
                &pk_test_schema(),
            )
            .expect_err("empty-string primary key must be rejected for a Kafka sink");
        let msg = err.to_string();
        assert!(
            msg.contains("kafka sink 'my_kafka_sink'") && msg.contains("requires a primary key"),
            "unexpected error message: {msg}"
        );
        // Attributed as a platform error: every dataset is expected to have a primary
        // key, so a missing one is a platform/dataset invariant violation, not a user
        // misconfiguration. `internal == true` is this codebase's "platform error".
        assert!(
            err.is_internal(),
            "missing Kafka sink primary key must be a platform (internal) error"
        );
    }

    /// A whitespace-only / comma-only key string also resolves to zero columns and must be
    /// rejected (it would otherwise reach the sink as a key with no usable columns).
    #[test]
    fn test_require_primary_key_for_sink_rejects_blank_columns() {
        let registry = PrimaryKeyRegistry::new(false);
        let err = registry
            .require_primary_key_for_sink(
                "kafka",
                &Some("  ,  ".to_string()),
                "upstream".to_string(),
                "my_kafka_sink".to_string(),
                &pk_test_schema(),
            )
            .expect_err("blank primary key must be rejected for a Kafka sink");
        assert!(err.to_string().contains("requires a primary key"));
    }

    /// No primary key on the sink and nothing registered upstream to propagate from.
    #[test]
    fn test_require_primary_key_for_sink_rejects_missing_with_no_upstream() {
        let registry = PrimaryKeyRegistry::new(false);
        let err = registry
            .require_primary_key_for_sink(
                "kafka",
                &None,
                "upstream".to_string(),
                "my_kafka_sink".to_string(),
                &pk_test_schema(),
            )
            .expect_err("missing primary key must be rejected for a Kafka sink");
        assert!(err.to_string().contains("requires a primary key"));
    }

    /// An empty key string that propagates down from an upstream node (e.g. a transform
    /// generated for an abstract dataset with no CMS primary key) must also be rejected.
    #[test]
    fn test_require_primary_key_for_sink_rejects_propagated_empty() {
        let registry = PrimaryKeyRegistry::new(false);
        registry.register(
            "abstract_transform".to_string(),
            PrimaryKeyMetadata::new(
                Vec::new(),
                PrimaryKeySource::TopologyDefined,
                "abstract_transform".to_string(),
            ),
        );
        let err = registry
            .require_primary_key_for_sink(
                "kafka",
                &None,
                "abstract_transform".to_string(),
                "my_kafka_sink".to_string(),
                &pk_test_schema(),
            )
            .expect_err("propagated empty primary key must be rejected for a Kafka sink");
        assert!(err.to_string().contains("requires a primary key"));
    }

    /// An explicit primary key on the sink config is accepted.
    #[test]
    fn test_require_primary_key_for_sink_accepts_explicit() {
        let registry = PrimaryKeyRegistry::new(false);
        let pk = registry
            .require_primary_key_for_sink(
                "kafka",
                &Some("id".to_string()),
                "upstream".to_string(),
                "my_kafka_sink".to_string(),
                &pk_test_schema(),
            )
            .expect("explicit primary key should be accepted");
        assert_eq!(pk.columns, vec!["id".to_string()]);
    }

    /// Addresses the runtime-discovery concern: a key the source discovered from its Avro
    /// schema (`SchemaInferred`) and registered must propagate to a Kafka sink that omits
    /// `primary_key`, satisfying the requirement without an explicit config entry.
    #[test]
    fn test_require_primary_key_for_sink_accepts_propagated_from_source() {
        let registry = PrimaryKeyRegistry::new(false);
        registry.register(
            "kafka_source".to_string(),
            PrimaryKeyMetadata::new(
                vec!["id".to_string()],
                PrimaryKeySource::SchemaInferred,
                "kafka_source".to_string(),
            ),
        );
        let pk = registry
            .require_primary_key_for_sink(
                "kafka",
                &None,
                "kafka_source".to_string(),
                "my_kafka_sink".to_string(),
                &pk_test_schema(),
            )
            .expect("propagated primary key should be accepted");
        assert_eq!(pk.columns, vec!["id".to_string()]);
        assert_eq!(pk.source, PrimaryKeySource::Propagated);
    }
}
