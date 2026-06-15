use arrow_schema::SchemaRef;
use datafusion::arrow::datatypes::ArrowNativeType;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::TableProvider;
use datafusion::datasource::{ViewTable, provider_as_source};
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, dml::InsertOp};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use streamling_config::AppConfig;
use streamling_connectors::table_providers::blackhole::BlackholeTableProvider;
use streamling_connectors::table_providers::clickhouse::ClickHouseTableProvider;
use streamling_connectors::table_providers::http::HttpTableProvider;
use streamling_connectors::table_providers::hybrid::HybridTableProvider;
use streamling_connectors::table_providers::kafka::KafkaSourceTableProvider;
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
use streamling_core::{streamling_err, streamling_user_bail, streamling_user_err};
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
use streamling_core::telemetry::recorder::initialize_metrics_recorder;
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

/// Walk a sink's resolved schema and reject any decimal_arb column the
/// connector cannot carry. Surfaces every Reject error at once (not just
/// the first), prefixed with the sink's reference name so the user knows
/// which YAML block to fix.
fn validate_sink_decimal_arb(
    schema: &arrow_schema::Schema,
    kind: streamling_common::types::decimal_arb_capability::ConnectorKind,
    directives: Option<&[streamling_config::ColumnDirective]>,
    sink_name: &str,
) -> Result<()> {
    use streamling_common::types::decimal_arb_capability::{
        ColumnDirectiveView, validate_pipeline_decimal_arb,
    };
    let views: Vec<ColumnDirectiveView<'_>> = directives
        .into_iter()
        .flatten()
        .map(|d| ColumnDirectiveView {
            name: d.name.as_str(),
            coerce_to_string: d.coerces_to_string(),
        })
        .collect();
    if let Err(errs) = validate_pipeline_decimal_arb(schema, kind, &views) {
        let joined = errs
            .into_inner()
            .into_iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("\n  ");
        streamling_user_bail!("sink '{}': {}", sink_name, joined);
    }
    Ok(())
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
}

impl SinkEntry {
    fn new(name: String, provider: Arc<dyn TableProvider>, rebatch_config: RebatchConfig) -> Self {
        Self {
            name,
            provider,
            rebatch_config,
        }
    }
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

        let session_manager = SessionManager::new(
            app_config.record_batch_size as u64,
            app_config.internal_buffer_size,
            dynamic_table_registry.clone(),
        )?;

        let pk_registry = PrimaryKeyRegistry::new(app_config.enforce_primary_keys);

        // Unified registry of initialized plugins keyed by a stable id.
        let mut plugins: BTreeMap<String, InitializedPlugin> = BTreeMap::new();

        let mut checkpoint_coordinator = CheckpointCoordinator::new();
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
                        )
                        .streamling_with_context(|| {
                            format!("{}: failed to create Kafka source", ctx.format())
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

                    let hybrid_source_provider = Arc::new(HybridTableProvider::new_from_topology(
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
                    )?);

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

                    let logical_plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(CheckpointableNode::new(
                            sql_plan,
                            app_config.internal_buffer_size,
                            reference_name.clone(),
                        )),
                    });

                    let pk_metadata_opt = pk_registry.track_primary_key_for_transform_or_sink(
                        &Some(sql_transform.primary_key),
                        source_name.clone(),
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
                    );

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
                            reference_name.clone(),
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

                    validate_sink_decimal_arb(
                        &source_schema,
                        streamling_common::types::decimal_arb_capability::ConnectorKind::Postgres,
                        None,
                        &reference_name,
                    )?;

                    let postgres_sink_provider = Arc::new(PostgresSinkTableProvider::new(
                        metric_key(&application_id, reference_name.as_str()),
                        source_schema.clone(),
                        app_config.postgres_sink.clone(),
                        table.clone(),
                        schema.clone(),
                        batch_size,
                        app_config.num_records_before_stop,
                        from.clone(),
                        pk_metadata_opt.map(|pk| pk.to_str()),
                        on_conflict.clone(),
                        update_where.clone(),
                        false, // append_only_mode (normal Postgres sink)
                        false, // checkpoint_truncation (disabled for normal sink)
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
                        Some(batch_size.unwrap_or(app_config.postgres_sink.batch_size));
                    let effective_batch_flush_interval = Some(match raw_batch_flush_interval {
                        Some(d) => d,
                        None => humantime::parse_duration(
                            &app_config.postgres_sink.batch_flush_interval,
                        )
                        .streamling_with_context(|| {
                            format!(
                                "invalid app_config.postgres_sink.batch_flush_interval '{}'",
                                app_config.postgres_sink.batch_flush_interval
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

                    let primary_key = pk_metadata_opt.map(|pk| pk.to_str()).ok_or_else(|| {
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

                    pg_aggregator
                        .create_trigger_and_tables(&app_config.postgres_sink)
                        .await?;

                    validate_sink_decimal_arb(
                        &source_schema,
                        streamling_common::types::decimal_arb_capability::ConnectorKind::Postgres,
                        None,
                        &reference_name,
                    )?;

                    let postgres_sink_provider = Arc::new(PostgresSinkTableProvider::new(
                        metric_key(&application_id, reference_name.as_str()),
                        source_schema.clone(),
                        app_config.postgres_sink.clone(),
                        landing_table.clone(),
                        schema.clone(),
                        batch_size,
                        app_config.num_records_before_stop,
                        from.clone(),
                        Some(primary_key),
                        "update".to_string(),
                        None, // update_where (not applicable for aggregation sink)
                        true, // append_only_mode (aggregation sink)
                        true, // checkpoint_truncation (enabled for aggregation sink)
                        reference_name.clone(),
                        None, // parallelism (default for aggregation sink)
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
                        Some(batch_size.unwrap_or(app_config.postgres_sink.batch_size));
                    let effective_batch_flush_interval = Some(match raw_batch_flush_interval {
                        Some(d) => d,
                        None => humantime::parse_duration(
                            &app_config.postgres_sink.batch_flush_interval,
                        )
                        .streamling_with_context(|| {
                            format!(
                                "invalid app_config.postgres_sink.batch_flush_interval '{}'",
                                app_config.postgres_sink.batch_flush_interval
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

                    let pk_metadata_opt = pk_registry.track_primary_key_for_transform_or_sink(
                        primary_key_opt,
                        from.clone(),
                        reference_name.clone(),
                        &source_schema,
                    )?;

                    let kafka_kind = match data_format.to_lowercase().as_str() {
                        "avro" => streamling_common::types::decimal_arb_capability::ConnectorKind::KafkaAvro { declared_bytes: None },
                        "json" => streamling_common::types::decimal_arb_capability::ConnectorKind::KafkaJson,
                        _ => streamling_common::types::decimal_arb_capability::ConnectorKind::KafkaJson,
                    };
                    validate_sink_decimal_arb(&source_schema, kafka_kind, None, &reference_name)?;

                    let kafka_sink_provider = Arc::new(KafkaSinkTableProvider::new(
                        metric_key(&application_id, reference_name.as_str()),
                        source_schema,
                        app_config.kafka_sink.clone(),
                        topic.clone(),
                        topic_partitions,
                        data_format.parse()?,
                        app_config.num_records_before_stop,
                        from.clone(),
                        pk_metadata_opt.map(|pk| pk.to_str()),
                        batch_size,
                        batch_flush_interval_ms,
                        kafka_sink.message_max_bytes,
                        kafka_sink.parallelism,
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
                    let parallelism = clickhouse_sink.parallelism;
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
                    let effective_batch_size = batch_size.unwrap_or(app_config.record_batch_size);
                    let effective_batch_flush_interval =
                        Some(raw_batch_flush_interval.unwrap_or_else(|| {
                            Duration::from_millis(app_config.record_batch_interval_ms)
                        }));
                    validate_sink_decimal_arb(
                        &source_schema,
                        streamling_common::types::decimal_arb_capability::ConnectorKind::ClickHouse,
                        app_config.clickhouse_sink.columns.as_deref(),
                        &reference_name,
                    )?;
                    let clickhouse_sink_provider = Arc::new(ClickHouseTableProvider::new_sink(
                        metric_key(&application_id, reference_name.as_str()),
                        table.as_str(),
                        app_config.clickhouse_sink.clone(),
                        effective_batch_size,
                        app_config.num_records_before_stop,
                        pk_metadata_opt.map(|pk| pk.to_str()).unwrap_or_default(),
                        from.clone(),
                        parallelism,
                        clickhouse_sink.append_only_mode,
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

                    validate_sink_decimal_arb(
                        &source_schema,
                        streamling_common::types::decimal_arb_capability::ConnectorKind::Plugin,
                        None,
                        &reference_name,
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
                        ),
                    );
                    checkpoint_sink_names.push(reference_name.clone());

                    // Register sink plugin under its reference name for lifecycle management
                    plugins.insert(reference_name.clone(), initialized_plugin);
                }
            }
        }

        init_node_registry(node_contexts);

        let mut dry_run_plans: Vec<(String, LogicalPlan)> = Vec::new();

        sources_to_sinks
            .into_iter()
            .for_each(|(_, (source_plan, mut sinks))| {
                let future_name = sinks
                    .iter()
                    .map(|e| e.name.as_str())
                    .collect::<Vec<&str>>()
                    .join(", ");
                let sink_plan = if sinks.len() > 1 {
                    // Fan-out: each sink gets its own RebatchExec injected by
                    // the MultiSinkExtensionPlanner, so the raw source_plan
                    // flows into MultiSinkLogicalNode without any upstream
                    // wrapping. Per-sink configs live on MultiSinkEntry.
                    let entries: Vec<MultiSinkEntry> = sinks
                        .into_iter()
                        .map(|e| MultiSinkEntry {
                            name: e.name,
                            rebatch_config: e.rebatch_config,
                        })
                        .collect();
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(MultiSinkLogicalNode::new(source_plan, entries)),
                    })
                } else {
                    // Single sink: wrap once at the logical level with this
                    // sink's config, then insert_into.
                    let entry = sinks.remove(0);
                    let rebatched_plan = wrap_with_rebatch(
                        source_plan,
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
                    .unwrap()
                    .build()
                    .unwrap()
                };

                if !dry_run {
                    let session_manager = session_manager.clone();
                    let sink_future = async move {
                        let result = session_manager.new_df(sink_plan).collect().await;
                        if let Err(err) = &result {
                            error!(
                                "Sink future [{}] completed with error: {}",
                                future_name, err
                            );
                        }
                        result
                    };
                    sink_futures.push(sink_future);
                } else {
                    dry_run_plans.push((future_name, sink_plan));
                }
            });

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

        let app_result = if !plugins.is_empty() {
            let plugin_futures: Vec<ExecutionFuture> = plugins
                .into_values()
                .map(|plugin| plugin.execution_future)
                .collect();

            // We terminate the application in two cases:
            // 1. If ANY plugin future completes (assuming an error)
            // 2. If ANY sink future fails
            let result: Result<()> = tokio::select! {
                plugin_outcome = futures::future::select_all(plugin_futures) => {
                    let (res, _idx, _rest) = plugin_outcome;
                    match res {
                        Ok(()) => {
                            debug!("Terminating because a plugin future completed gracefully");
                            Ok(())
                        }
                        Err(msg) => {
                            debug!("Terminating because a plugin future completed with error: {}", msg);
                            Err(streamling_err!("Plugin error: {}", msg))
                        }
                    }
                },
                result = futures::future::try_join_all(sink_futures) => {
                    result
                        .map(|_| ())
                        .map_err(|e| {
                            debug!("Terminating because a sink future completed with error");
                            e.into()
                        })
                }
            };

            result
        } else {
            futures::future::try_join_all(sink_futures)
                .await
                .map(|_| ())
                .map_err(Into::into)
        };

        // Issue SourceComplete message to plugins. This is needed to fully clean up checkpoint channels when plugins
        // terminate.
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
        terminate_all_plugins()?;
        // Stop checkpoint coordinator only if it was started (not in dry_run mode)
        // Stop it before checking app_result to ensure cleanup happens even if there's an error
        if !dry_run {
            // Use timeout to prevent hanging if checkpoint coordinator tasks are stuck
            match timeout(Duration::from_secs(30), checkpoint_coordinator.stop()).await {
                Ok(_) => {}
                Err(_) => {
                    warn!(
                        "Checkpoint coordinator stop timed out after 5 seconds, continuing anyway"
                    );
                }
            }
        }

        app_result?;
        Ok(())
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

    // ----- validate_sink_decimal_arb -----

    fn arb_schema(precision: u32, scale: u32) -> arrow_schema::Schema {
        let f = streamling_common::types::decimal_arb::DecimalArbType::field(
            "amount", precision, scale, false,
        )
        .unwrap();
        arrow_schema::Schema::new(vec![f])
    }

    #[test]
    fn validate_sink_decimal_arb_postgres_native() {
        let schema = arb_schema(100, 18);
        validate_sink_decimal_arb(
            &schema,
            streamling_common::types::decimal_arb_capability::ConnectorKind::Postgres,
            None,
            "pg_sink",
        )
        .expect("Postgres at p=100 should be Native");
    }

    #[test]
    fn validate_sink_decimal_arb_clickhouse_rejects_without_directive() {
        let schema = arb_schema(100, 18);
        let err = validate_sink_decimal_arb(
            &schema,
            streamling_common::types::decimal_arb_capability::ConnectorKind::ClickHouse,
            None,
            "ch_sink",
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ch_sink"), "error names sink: {}", msg);
        assert!(msg.contains("amount"), "error names column: {}", msg);
        assert!(
            msg.contains("coerce_to: string"),
            "error suggests directive: {}",
            msg
        );
    }

    #[test]
    fn validate_sink_decimal_arb_clickhouse_passes_with_directive() {
        let schema = arb_schema(100, 18);
        let directives = vec![streamling_config::ColumnDirective {
            name: "amount".to_string(),
            coerce_to: Some(streamling_config::CoercionTarget::String),
        }];
        validate_sink_decimal_arb(
            &schema,
            streamling_common::types::decimal_arb_capability::ConnectorKind::ClickHouse,
            Some(&directives),
            "ch_sink",
        )
        .expect("ClickHouse with coerce_to: string should be OptInOnly (OK)");
    }

    #[test]
    fn validate_sink_decimal_arb_kafka_json_native() {
        let schema = arb_schema(200, 50);
        validate_sink_decimal_arb(
            &schema,
            streamling_common::types::decimal_arb_capability::ConnectorKind::KafkaJson,
            None,
            "kafka_sink",
        )
        .expect("Kafka JSON should accept any precision");
    }

    #[test]
    fn validate_sink_decimal_arb_plugin_rejects_by_default() {
        let schema = arb_schema(50, 10);
        let err = validate_sink_decimal_arb(
            &schema,
            streamling_common::types::decimal_arb_capability::ConnectorKind::Plugin,
            None,
            "my_plugin",
        )
        .unwrap_err();
        assert!(err.to_string().contains("my_plugin"));
    }

    #[test]
    fn validate_sink_decimal_arb_no_decimal_arb_columns_passes() {
        // Schema with only plain types — no decimal_arb fields, validator is a no-op.
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )]);
        validate_sink_decimal_arb(
            &schema,
            streamling_common::types::decimal_arb_capability::ConnectorKind::ClickHouse,
            None,
            "any_sink",
        )
        .expect("schema without decimal_arb columns is always OK");
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
}
