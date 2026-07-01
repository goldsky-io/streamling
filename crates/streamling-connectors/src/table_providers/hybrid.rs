use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::table_providers::clickhouse::{ClickHouseClient, ClickHouseTableProvider};
use crate::table_providers::kafka::KafkaSourceTableProvider;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use streamling_config::AppConfig;
use streamling_core::checkpoints::channels::{subscribe_with_id, unsubscribe};
use streamling_core::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
    extract_checkpoint_messages,
};
use streamling_core::data::COLUMN_NAME_OP;
use streamling_core::error::ResultExt;
use streamling_core::operators::wrapping::WrappingSourceTableProvider;
use streamling_core::session::SessionManager;
use streamling_core::side_output::{SourceSideOutput, SupportsSideOutputs};
use streamling_core::telemetry::provider::{
    metric_key, metric_key_hybrid_src_bounded, metric_key_hybrid_src_unbounded,
};
use streamling_core::topology::{
    HybridBoundedSource, HybridOffsetTable, HybridUnboundedSource, Telemetry,
};
use streamling_core::{streamling_err, streamling_user_bail};
use streamling_state::{StateKey, StateOperatorBackend, StateOperatorBackendFactory};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[async_trait]
pub trait OffsetProvider: Send + Sync + Debug {
    async fn get_offsets(&self) -> DataFusionResult<HashMap<i32, u32>>;
}

/// Probe whether a bounded phase source has its own persisted cursor state.
/// Unwraps the standard `WrappingSourceTableProvider` if present, then
/// dispatches to each supported concrete source type. Unknown types
/// (including most test mocks) report no state.
///
/// Assumes single-level wrapping: today every hybrid bounded source is
/// either bare or wrapped exactly once in `WrappingSourceTableProvider`.
/// If a future wrapper layer (e.g. filter or projection passthrough) is
/// inserted between Wrapping and the concrete source, this probe will
/// silently return false and recovery will regress to first-run. If that
/// happens, extend this function or expose a recursive unwrap helper on
/// `WrappingSourceTableProvider`.
fn bounded_has_persisted_state(source: &Arc<dyn TableProvider>) -> bool {
    let inner_arc = source
        .as_any()
        .downcast_ref::<WrappingSourceTableProvider>()
        .map(|w| w.get_inner());
    let inner: &dyn TableProvider = match &inner_arc {
        Some(arc) => arc.as_ref(),
        None => source.as_ref(),
    };

    if let Some(ch) = inner.as_any().downcast_ref::<ClickHouseTableProvider>() {
        return ch.has_persisted_source_state();
    }

    #[cfg(test)]
    if let Some(mock) = inner
        .as_any()
        .downcast_ref::<tests::MockStatefulBoundedProvider>()
    {
        return mock.has_persisted_state();
    }

    false
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HybridSourceState {
    pub current_phase: usize,
    pub completed_phases: Vec<bool>,
    pub unbounded_offsets: Option<HashMap<i32, u32>>,
}

/// Hybrid source state machine configuration.
///
/// The hybrid source progresses through the following states:
///
/// ```text
///  +-------------+
///  |   INITIAL   |
///  +------+------+
///         |
///         | (start)
///         v
///  +-------------+       (stream_ends)        +-------------+
///  | BOUNDED[0]  | --------------------------> | BOUNDED[1]  |
///  +-------------+                              +------+------+
///                                                      |
///                                                      | (stream_ends)
///                                                      v
///                                               +-------------+
///                                               |     ...     |
///                                               +------+------+
///                                                      |
///                                                      | (stream_ends)
///                                                      v
///                                               +-------------+       (stream_ends)        +-------------+
///                                               | BOUNDED[N]  | --------------------------> | UNBOUNDED   |
///                                               +-------------+                              +------+------+
///                                                                                                    |
///                                                                                                    | (stream_ends)
///                                                                                                    v
///                                                                                             +-------------+
///                                                                                             |  FINISHED   |
///                                                                                             +-------------+
/// ```
///
/// State Transitions:
/// - INITIAL -> BOUNDED[0]: Automatic on creation
/// - BOUNDED[i] -> BOUNDED[i+1]: When bounded source stream ends
/// - BOUNDED[N] -> UNBOUNDED: When last bounded source stream ends (optionally fetches offsets via offset_provider)
/// - UNBOUNDED -> FINISHED: When unbounded stream ends (typically runs indefinitely)
#[derive(Clone, Debug)]
pub struct HybridSourceConfig {
    pub bounded_sources: Vec<Arc<dyn TableProvider>>,
    pub unbounded_source: Arc<dyn TableProvider>,
    pub offset_provider: Option<Arc<dyn OffsetProvider>>,
    /// When true, terminate after all bounded phases complete instead of transitioning to unbounded.
    pub job_mode: bool,
}

#[derive(Clone)]
pub struct HybridTableProvider {
    pub config: HybridSourceConfig,
    schema: SchemaRef,
    state_backend: Arc<dyn StateOperatorBackend<HybridSourceState>>,
    pub state: Arc<RwLock<HybridSourceState>>,
    reference_name: String,
    session_manager: SessionManager,
}
impl Debug for HybridTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridTableProvider")
            .field("reference_name", &self.reference_name)
            .field("config", &self.config)
            .field("schema", &self.schema)
            .finish()
    }
}

impl HybridTableProvider {
    /// Shutdown the Kafka consumer if the unbounded source is Kafka
    pub fn shutdown(&self) {
        if let Some(kafka_provider) = self
            .config
            .unbounded_source
            .as_any()
            .downcast_ref::<KafkaSourceTableProvider>()
        {
            info!("Shutting down kafka table provider");
            kafka_provider.shutdown();
            info!("Successfully shutdown kafka table provider");
        }
    }

    /// Propagate a side output to all inner phase sources so it sees pre-filter data.
    pub fn add_side_output_to_inner_sources(&self, side_output: Arc<dyn SourceSideOutput>) {
        for source in &self.config.bounded_sources {
            if let Some(wrapping) = source
                .as_any()
                .downcast_ref::<WrappingSourceTableProvider>()
            {
                wrapping.add_side_output(side_output.clone());
            }
        }
        if let Some(wrapping) = self
            .config
            .unbounded_source
            .as_any()
            .downcast_ref::<WrappingSourceTableProvider>()
        {
            wrapping.add_side_output(side_output);
        }
    }
}

impl HybridTableProvider {
    pub fn new(
        reference_name: String,
        config: HybridSourceConfig,
        schema: SchemaRef,
        state_backend: Arc<dyn StateOperatorBackend<HybridSourceState>>,
        session_manager: SessionManager,
    ) -> DataFusionResult<Self> {
        Self::validate_schemas(&config.bounded_sources, &config.unbounded_source)?;

        let initial_state = HybridSourceState {
            completed_phases: vec![false; config.bounded_sources.len()],
            ..Default::default()
        };

        let provider = Self {
            config,
            schema,
            state_backend,
            state: Arc::new(RwLock::new(initial_state)),
            reference_name,
            session_manager,
        };

        Ok(provider)
    }

    pub fn new_from_topology(
        reference_name: String,
        bounded_sources: Vec<HybridBoundedSource>,
        unbounded_source: HybridUnboundedSource,
        offset_table: Option<HybridOffsetTable>,
        app_config: &AppConfig,
        state_backend_factory: &impl StateOperatorBackendFactory,
        session_manager: SessionManager,
        // The same `Telemetry` is forwarded to both inner phases (bounded
        // ClickHouse + unbounded Kafka). The user is responsible for
        // choosing a column name that exists in both schemas; if it doesn't
        // exist on one side, that phase's `EventTimeReader` will warn-once
        // per R5a and the other phase keeps emitting normally. Each inner
        // phase emits under its own `metric_key_hybrid_src_*` suffix (R9),
        // so bounded vs unbounded series are tag-distinguishable downstream.
        telemetry: Option<&Telemetry>,
    ) -> DataFusionResult<Self> {
        use crate::table_providers::clickhouse::ClickHouseTableProvider;
        use crate::table_providers::kafka::KafkaSourceTableProvider;
        use datafusion::common::ScalarValue;

        let mut bounded_table_providers = Vec::new();

        let application_id = app_config.application_id.clone();

        let unbounded_source_topic = unbounded_source.topic.clone();
        let unbounded_table_provider: Arc<dyn TableProvider> =
            match unbounded_source.source_type.as_str() {
                "kafka" => {
                    let kafka_table_provider = Arc::new(
                        KafkaSourceTableProvider::new(
                            reference_name.clone(),
                            metric_key(&application_id, &reference_name),
                            app_config.kafka_source.clone(),
                            unbounded_source.topic,
                            unbounded_source.start_at,
                            unbounded_source.filter,
                            app_config.record_batch_interval_ms,
                            app_config.record_batch_size,
                            app_config.internal_buffer_size,
                            false,
                            state_backend_factory.create(application_id.as_str()),
                            session_manager.clone(),
                            app_config.num_records_before_stop,
                            unbounded_source
                                .validate_writer_schema_ordering
                                .unwrap_or(true),
                            unbounded_source.schema_id_overrides.unwrap_or_default(),
                            unbounded_source.skip_schema_resolution.unwrap_or(false),
                            unbounded_source
                                .skip_schema_resolution_for_reader_schema_ids
                                .unwrap_or_default(),
                        )
                        .streamling_with_context(|| {
                            format!(
                                "hybrid source '{}': failed to create Kafka source",
                                reference_name
                            )
                        })?,
                    );
                    Arc::new(WrappingSourceTableProvider::new(
                        kafka_table_provider,
                        metric_key_hybrid_src_unbounded(&application_id, reference_name.as_str()),
                        None,
                        telemetry,
                    ))
                }
                _ => {
                    return Err(streamling_core::streamling_user_err!(
                        "unbounded source type '{}' is not supported for hybrid sources",
                        unbounded_source.source_type
                    )
                    .into());
                }
            };

        let schema_adapter = ClickHouseSchemaAdapter {
            client: ClickHouseClient::new(app_config.clickhouse_source.connection.clone()),
        };
        for (idx, bounded_source) in bounded_sources.into_iter().enumerate() {
            match bounded_source.source_type.as_str() {
                "clickhouse" => {
                    let start_at = bounded_source
                        .start_at
                        .map(|start_at| start_at.split(',').map(ScalarValue::from).collect());
                    // Unbounded source schema will be the source of truth for the hybrid source schema
                    // TODO: allow projection. Needs kafka changes
                    let unbounded_schema = unbounded_table_provider.schema();
                    let unbounded_columns: Vec<String> = unbounded_schema
                        .fields()
                        .iter()
                        .filter(|f| f.name() != COLUMN_NAME_OP)
                        .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                        .collect();
                    debug!(
                        "Hybrid source [{}] unbounded source columns ({}): {:?}",
                        reference_name,
                        unbounded_columns.len(),
                        unbounded_columns
                    );
                    let columns = Some(
                        schema_adapter
                            .get_columns(bounded_source.table_name.as_str(), &unbounded_schema)?,
                    );
                    let clickhouse_provider = Arc::new(ClickHouseTableProvider::new_source(
                        reference_name.clone(),
                        metric_key(&application_id, &reference_name),
                        bounded_source.table_name.as_str(),
                        app_config.clickhouse_source.clone(),
                        start_at,
                        bounded_source.filter,
                        columns,
                        state_backend_factory.create(application_id.as_str()),
                        app_config.internal_buffer_size as usize,
                        app_config.record_batch_size as usize,
                    )?);
                    debug!(
                        "Clickhouse schema for bounded source: {:?}",
                        clickhouse_provider.schema()
                    );

                    let provider_with_telemetry = Arc::new(WrappingSourceTableProvider::new(
                        clickhouse_provider,
                        metric_key_hybrid_src_bounded(
                            &application_id,
                            reference_name.as_str(),
                            idx,
                        ),
                        None, // We shouldn't need it here since the whole thing is nested under one wrapping provider.
                        telemetry,
                    ));
                    bounded_table_providers.push(provider_with_telemetry as Arc<dyn TableProvider>);
                }
                _ => {
                    return Err(streamling_core::streamling_user_err!(
                        "bounded source type '{}' is not supported for hybrid sources",
                        bounded_source.source_type
                    )
                    .into());
                }
            }
        }

        let offset_provider = {
            let client = ClickHouseClient::new(app_config.clickhouse_source.connection.clone());
            Some(Arc::new(ClickHouseOffsetProvider::new(
                client,
                offset_table.unwrap_or(HybridOffsetTable::new(unbounded_source_topic)),
            )) as Arc<dyn OffsetProvider>)
        };

        let schema = Self::validate_schemas(&bounded_table_providers, &unbounded_table_provider)?;

        let config = HybridSourceConfig {
            bounded_sources: bounded_table_providers,
            unbounded_source: unbounded_table_provider,
            offset_provider,
            job_mode: app_config.job_mode,
        };

        let state_backend =
            state_backend_factory.create::<HybridSourceState>(application_id.as_str());
        let provider = Self::new(
            reference_name,
            config,
            schema,
            state_backend,
            session_manager,
        )?;
        debug!("Created HybridTableProvider: {:?}", provider);

        Ok(provider)
    }

    fn validate_schemas(
        bounded_providers: &[Arc<dyn TableProvider>],
        unbounded_provider: &Arc<dyn TableProvider>,
    ) -> DataFusionResult<SchemaRef> {
        if bounded_providers.is_empty() {
            return Ok(unbounded_provider.schema());
        }

        let bounded_schema = bounded_providers[0].schema();
        let unbounded_schema = unbounded_provider.schema();

        for (i, provider) in bounded_providers.iter().enumerate() {
            if provider.schema() != bounded_schema {
                streamling_user_bail!("bounded source {} has incompatible schema", i);
            }
        }

        let get_fields = |schema: &SchemaRef| {
            schema
                .fields()
                .iter()
                .filter(|field| field.name() != COLUMN_NAME_OP)
                .map(|field| (field.name().clone(), field.data_type().clone()))
                .collect::<Vec<_>>()
        };

        let bounded_fields = get_fields(&bounded_schema);
        let unbounded_fields = get_fields(&unbounded_schema);

        let bounded_column_names: Vec<String> = bounded_fields
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let unbounded_column_names: Vec<String> = unbounded_fields
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        debug!(
            "Hybrid source schema validation - bounded source columns ({}): {:?}",
            bounded_column_names.len(),
            bounded_column_names
        );
        debug!(
            "Hybrid source schema validation - unbounded source columns ({}): {:?}",
            unbounded_column_names.len(),
            unbounded_column_names
        );

        for (col_name, col_type) in &unbounded_fields {
            match bounded_fields.iter().find(|(name, _)| name == col_name) {
                Some((_, bounded_type)) => {
                    if !Self::is_compatible_data_type(bounded_type, col_type) {
                        streamling_user_bail!(
                            "column '{}' type mismatch: bounded source has {:?}, unbounded source has {:?}",
                            col_name,
                            bounded_type,
                            col_type
                        );
                    }
                }
                None => {
                    streamling_user_bail!(
                        "unbounded source column '{}' not found in bounded source",
                        col_name
                    );
                }
            }
        }

        Ok(unbounded_schema)
    }

    fn is_compatible_data_type(
        bounded_type: &arrow_schema::DataType,
        unbounded_type: &arrow_schema::DataType,
    ) -> bool {
        use arrow_schema::DataType;

        if bounded_type == unbounded_type {
            return true;
        }

        match (bounded_type, unbounded_type) {
            (DataType::List(bounded_field), DataType::List(unbounded_field))
            | (DataType::LargeList(bounded_field), DataType::LargeList(unbounded_field)) => {
                // Safe to ignore nested field names (for example "item" vs "element"):
                // Arrow/DataFusion producers can label list children differently even when
                // the list semantics are identical. We still require matching child
                // nullability and recursively compatible element types.
                bounded_field.is_nullable() == unbounded_field.is_nullable()
                    && Self::is_compatible_data_type(
                        bounded_field.data_type(),
                        unbounded_field.data_type(),
                    )
            }
            (
                DataType::FixedSizeList(bounded_field, bounded_size),
                DataType::FixedSizeList(unbounded_field, unbounded_size),
            ) => {
                bounded_size == unbounded_size
                    && bounded_field.is_nullable() == unbounded_field.is_nullable()
                    && Self::is_compatible_data_type(
                        bounded_field.data_type(),
                        unbounded_field.data_type(),
                    )
            }
            _ => false,
        }
    }

    pub async fn load_state(&self) -> DataFusionResult<()> {
        match self
            .state_backend
            .get(StateKey(format!(
                "hybrid_source_{}_v1",
                self.reference_name
            )))
            .await
        {
            Ok(Some(saved_state)) => {
                let mut state = self.state.write().await;
                *state = saved_state;
                info!("Loaded hybrid source state: {:?}", *state);
                Ok(())
            }
            Ok(None) => {
                info!(
                    "Hybrid source '{}': no persisted hybrid state found. \
                     Probing per-phase state for fallback recovery.",
                    self.reference_name
                );
                if self.config.bounded_sources.is_empty() {
                    return Ok(());
                }
                self.try_recover_from_persisted_state().await
            }
            Err(e) => Err(streamling_err!("state backend error: {:?}", e).into()),
        }
    }

    /// Best-effort recovery when the hybrid state key is absent.
    ///
    /// Order, matching docs/hybrid-source-state.allium:
    ///   1. If unbounded offsets are persisted AND cover every configured
    ///      partition, recover directly to the unbounded phase.
    ///   2. Else, scan bounded_sources from highest index to lowest and
    ///      recover from the first phase whose source has its own persisted
    ///      state.
    ///   3. Else, leave state at default (first run / phase 0) and WARN so
    ///      operators can distinguish first run from FM1/FM6 in logs.
    async fn try_recover_from_persisted_state(&self) -> DataFusionResult<()> {
        if self.try_recover_to_unbounded().await? {
            return Ok(());
        }
        self.try_recover_to_highest_bounded_state().await
    }

    /// Ok(true) iff recovery to the unbounded phase succeeded. Ok(false)
    /// means this path did not apply (incomplete offsets, no offset
    /// provider, etc.) and the caller should try the next fallback.
    async fn try_recover_to_unbounded(&self) -> DataFusionResult<bool> {
        let kafka_inner = self
            .config
            .unbounded_source
            .as_any()
            .downcast_ref::<WrappingSourceTableProvider>()
            .map(|w| w.get_inner());

        let kafka_provider = kafka_inner
            .as_ref()
            .and_then(|inner| inner.as_any().downcast_ref::<KafkaSourceTableProvider>());

        // Cheap state-backend probe before touching the (potentially remote)
        // offset provider. Recovery to unbounded REQUIRES the kafka source to
        // already hold persisted offsets for every partition the offset
        // provider knows about — if no partition has any persisted offset at
        // all, `has_all_persisted_offsets` is guaranteed false and the
        // expensive `offset_provider.get_offsets()` call would only be used
        // to confirm a foregone "fall through". Skipping it here means cold
        // starts incur one ClickHouse offset query (the one in
        // `advance_to_next_phase`), not two.
        let kafka_has_any_state = match kafka_provider {
            Some(kp) => kp.has_any_persisted_offset().await,
            None => false,
        };
        if !kafka_has_any_state {
            return Ok(false);
        }

        if let Some(offset_provider) = &self.config.offset_provider {
            match offset_provider.get_offsets().await {
                Ok(offsets) if !offsets.is_empty() => {
                    if let Some(kafka_provider) = kafka_provider {
                        let partitions: Vec<i32> = offsets.keys().copied().collect();
                        if kafka_provider.has_all_persisted_offsets(&partitions).await {
                            warn!(
                                "Hybrid source '{}': all {} Kafka partition offsets exist. \
                                 Recovering directly to unbounded phase \
                                 (skipping bounded replay).",
                                self.reference_name,
                                partitions.len()
                            );
                            let mut state = self.state.write().await;
                            for i in 0..self.config.bounded_sources.len() {
                                state.completed_phases[i] = true;
                            }
                            state.current_phase = self.config.bounded_sources.len();
                            state.unbounded_offsets = Some(offsets);
                            drop(state);
                            self.save_state().await?;
                            return Ok(true);
                        }
                        warn!(
                            "Hybrid source '{}': only some Kafka partition offsets exist. \
                             Falling through to bounded-phase recovery.",
                            self.reference_name
                        );
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        "Hybrid source '{}': failed to query offset provider \
                         during recovery check: {:?}. Falling through to bounded-phase recovery.",
                        self.reference_name, e
                    );
                }
            }
        } else {
            // Kafka has persisted state but no offset provider is configured
            // to verify completeness. We can't safely fast-path to unbounded
            // here, so fall through to bounded-phase recovery and emit a
            // warning so operators can spot the configuration gap.
            warn!(
                "Hybrid source '{}': Kafka partition offsets exist but no offset \
                 provider is configured to verify completeness. Falling through to \
                 bounded-phase recovery.",
                self.reference_name
            );
        }
        Ok(false)
    }

    /// Scan bounded_sources from highest index to lowest. The first phase
    /// whose source reports its own persisted state is the recovery target.
    /// If none reports state, leave hybrid state at default (current_phase
    /// = 0) and WARN. Either way, hybrid state is persisted so that the
    /// next restart hits the happy path.
    ///
    /// Caveat: today's keying lets multiple ClickHouse bounded phases
    /// under the same hybrid reference_name overwrite each other's state
    /// (docs/hybrid-source-state.allium BoundedResumeContract /
    /// StateKeyUniqueness; migration tracked as an open question there).
    /// In such topologies "has persisted state" reflects only the last
    /// writer; this probe still returns the highest index that downcasts
    /// to a known source type AND reports state, which is the best signal
    /// available until per-phase keying lands.
    async fn try_recover_to_highest_bounded_state(&self) -> DataFusionResult<()> {
        for (idx, bounded_source) in self.config.bounded_sources.iter().enumerate().rev() {
            if !bounded_has_persisted_state(bounded_source) {
                continue;
            }

            warn!(
                "Hybrid source '{}': bounded phase {} has its own persisted cursor. \
                 Recovering to bounded phase {} (treating earlier phases as completed).",
                self.reference_name, idx, idx
            );

            let mut state = self.state.write().await;
            for i in 0..idx {
                state.completed_phases[i] = true;
            }
            state.current_phase = idx;
            drop(state);

            return self.save_state().await;
        }

        warn!(
            "Hybrid source '{}': no bounded phase has persisted state. \
             Starting from bounded phase 0 (true first run).",
            self.reference_name
        );
        self.save_state().await
    }

    async fn save_state(&self) -> DataFusionResult<()> {
        let state = self.state.read().await;
        info!("Saving hybrid source state: {:?}", *state);

        let key = StateKey(format!("hybrid_source_{}_v1", self.reference_name));
        let backoffs = [
            std::time::Duration::from_millis(100),
            std::time::Duration::from_millis(500),
        ];
        let max_attempts = backoffs.len() + 1;

        for attempt in 1..=max_attempts {
            match self.state_backend.put(key.clone(), state.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) if attempt < max_attempts => {
                    warn!(
                        "Hybrid source '{}' save_state attempt {}/{} failed: {:?}",
                        self.reference_name, attempt, max_attempts, e
                    );
                    tokio::time::sleep(backoffs[attempt - 1]).await;
                }
                Err(e) => {
                    error!(
                        "Hybrid source '{}' save_state failed after {} attempts: {:?}",
                        self.reference_name, max_attempts, e
                    );
                    return Err(streamling_err!("state backend error: {:?}", e).into());
                }
            }
        }
        unreachable!()
    }

    pub async fn get_current_source(&self) -> DataFusionResult<Arc<dyn TableProvider>> {
        let state = self.state.read().await;

        if state.current_phase < self.config.bounded_sources.len() {
            Ok(self.config.bounded_sources[state.current_phase].clone())
        } else {
            Ok(self.config.unbounded_source.clone())
        }
    }

    //used in testing only
    pub async fn get_current_inner_source(&self) -> DataFusionResult<Arc<dyn TableProvider>> {
        let current_src = self.get_current_source().await?;
        let wrapping_table_provider = current_src
            .as_any()
            .downcast_ref::<WrappingSourceTableProvider>()
            .ok_or_else(|| {
                streamling_err!(
                    "expected both clickhouse table provider and kafka table provider to be wrapped"
                )
            })?;
        Ok(wrapping_table_provider.get_inner())
    }

    //used in testing only
    pub async fn get_bounded_inner_source(
        &self,
        idx: usize,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let bounded_src = self.config.bounded_sources[idx].clone();
        let wrapping_table_provider = bounded_src
            .as_any()
            .downcast_ref::<WrappingSourceTableProvider>()
            .ok_or_else(|| {
                streamling_err!(
                    "expected both clickhouse table provider and kafka table provider to be wrapped"
                )
            })?;
        Ok(wrapping_table_provider.get_inner())
    }

    //used in testing only
    pub async fn get_unbounded_inner_source(&self) -> DataFusionResult<Arc<dyn TableProvider>> {
        let bounded_src = self.config.unbounded_source.clone();
        let wrapping_table_provider = bounded_src
            .as_any()
            .downcast_ref::<WrappingSourceTableProvider>()
            .ok_or_else(|| {
                streamling_err!(
                    "expected both clickhouse table provider and kafka table provider to be wrapped"
                )
            })?;
        Ok(wrapping_table_provider.get_inner())
    }

    pub async fn advance_to_next_phase(&self) -> DataFusionResult<()> {
        let mut state = self.state.write().await;
        let from_phase = state.current_phase;

        if state.current_phase < self.config.bounded_sources.len() {
            let current_phase = state.current_phase;
            state.completed_phases[current_phase] = true;
        }

        state.current_phase += 1;
        info!(
            "Advancing hybrid source '{}' from phase {} to {}",
            self.reference_name, from_phase, state.current_phase
        );

        if state.current_phase == self.config.bounded_sources.len()
            && !self.config.job_mode
            && let Some(offset_provider) = &self.config.offset_provider
        {
            // Only fetch/seed offsets when actually transitioning to the
            // unbounded phase. In job_mode the unbounded phase never runs, so
            // this ClickHouse max-offset query + Kafka seed is wasted work and a
            // stall/hang vector under load (it blocks advance_to_next_phase,
            // which the hybrid execute loop must return from before it can break
            // and end the source stream). Skip it; the bounded source's own
            // cursor is already persisted via its checkpoint Finalizer.
            let offsets = offset_provider.get_offsets().await?;
            state.unbounded_offsets = Some(offsets.clone());

            // Seed the Kafka source's state backend with the offsets from ClickHouse
            // so that the Kafka consumer will start from the correct position
            if let Some(wrapping_provider) = self
                .config
                .unbounded_source
                .as_any()
                .downcast_ref::<WrappingSourceTableProvider>()
                && let Some(kafka_provider) = wrapping_provider
                    .get_inner()
                    .as_any()
                    .downcast_ref::<KafkaSourceTableProvider>()
            {
                kafka_provider.seed_offsets(&offsets).await?;
                info!("Seeded Kafka offsets: {:?}", offsets);
            }
        }
        drop(state);
        self.save_state().await
    }

    pub async fn get_current_execution_stream(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let current_source = self.get_current_source().await?;
        let session_state = self.session_manager.session_state();
        let plan = current_source
            .scan(&session_state, projection.as_ref(), filters, limit)
            .await?;
        plan.execute(partition, context)
    }
}

#[async_trait]
impl TableProvider for HybridTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.load_state().await?;

        let current_source = self.get_current_source().await?;
        let inner_plan = current_source
            .scan(state, projection, filters, limit)
            .await?;

        Ok(Arc::new(HybridSourceExec::new(
            inner_plan,
            self.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
        )))
    }
}

#[derive(Debug)]
pub struct HybridSourceExec {
    inner: Arc<dyn ExecutionPlan>,
    provider: HybridTableProvider,
    cached_properties: PlanProperties,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl HybridSourceExec {
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        provider: HybridTableProvider,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        let cached_properties = PlanProperties::new(
            EquivalenceProperties::new(provider.schema.clone()),
            inner.properties().partitioning.clone(),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );

        Self {
            inner,
            provider,
            cached_properties,
            projection,
            filters,
            limit,
        }
    }
}

impl DisplayAs for HybridSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "HybridSourceExec[{}]", self.inner.name())
    }
}

#[async_trait]
impl ExecutionPlan for HybridSourceExec {
    fn name(&self) -> &str {
        "HybridSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.provider.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cached_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(streamling_err!("HybridSourceExec must have exactly one child").into());
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.provider.clone(),
            self.projection.clone(),
            self.filters.clone(),
            self.limit,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;

        let schema = self.schema();
        let builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 16);
        let tx = builder.tx();
        let provider = self.provider.clone();
        let context_clone = context.clone();
        let projection = self.projection.clone();
        let filters = self.filters.clone();
        let limit = self.limit;

        // Hybrid-level checkpoint subscription.
        //
        // Each inner source independently subscribes to
        // CHECKPOINT_COORDINATOR_CHANNEL for its own bookkeeping (clickhouse
        // saves split state on Finalizer, kafka snapshots offsets on Marker,
        // etc.) and attaches Markers/Finalizers to outgoing batches so sinks
        // can ack. Both subscriptions are scoped to the inner source's
        // `execute()` — so during the bounded→unbounded transition there is a
        // window where:
        //
        //   - the bounded source has unsubscribed (its pagination loop ended
        //     and its `checkpointing_task` was joined), and
        //   - the unbounded source has not yet subscribed (its `execute()`
        //     has not been called by the hybrid loop yet).
        //
        // Any Marker the coordinator broadcasts during that window is
        // delivered to zero source subscribers (the broadcast iterator just
        // skips when no senders exist for the channel id) and is therefore
        // never attached to a batch. The sink never sees that epoch, never
        // sends an Ack, and the coordinator stalls forever — exactly the
        // production stall observed when a bounded ClickHouse phase finishes
        // in a handful of ms.
        //
        // Adding a hybrid-level subscription that lives across all transitions
        // closes the gap: we accumulate any Markers/Finalizers that arrive
        // while no inner source is hooked up, then flush them either as a
        // synthetic empty batch (when an inner stream ends mid-transition) or
        // merged onto the next real batch the inner source produces. The
        // inner sources keep their own subscriptions — this is a strict
        // additive safety net, not a replacement.
        let (hybrid_marker_rx, hybrid_marker_sub_id) =
            subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);
        let pending_markers: Arc<Mutex<Vec<CheckpointMessage>>> = Arc::new(Mutex::new(Vec::new()));
        let (forwarder_shutdown_tx, mut forwarder_shutdown_rx) = tokio::sync::watch::channel(());

        // Forwarder: drains the hybrid subscription into `pending_markers`.
        // crossbeam recv is sync, so use spawn_blocking with a short timeout
        // to stay responsive to shutdown (same pattern as
        // `ClickHouseSourceExec::execute`'s `checkpointing_task`).
        let pending_for_drain = pending_markers.clone();
        let forwarder_handle = tokio::spawn(async move {
            loop {
                let rx = hybrid_marker_rx.clone();
                let recv = tokio::task::spawn_blocking(move || {
                    rx.recv_timeout(Duration::from_millis(250))
                });
                tokio::select! {
                    biased;
                    _ = forwarder_shutdown_rx.changed() => break,
                    res = recv => match res {
                        Ok(Ok(msg)) => {
                            if matches!(
                                msg,
                                CheckpointMessage::Marker { .. }
                                    | CheckpointMessage::Finalizer(_)
                            ) {
                                pending_for_drain.lock().unwrap().push(msg);
                            }
                            // Acks and SourceComplete are not relevant to
                            // the safety-net: sinks consume Acks from the
                            // channel directly, and SourceComplete is a
                            // source-side signal.
                        }
                        Ok(Err(crossbeam::channel::RecvTimeoutError::Timeout)) => {
                            // Expected: 250ms tick to stay responsive to shutdown.
                        }
                        Ok(Err(crossbeam::channel::RecvTimeoutError::Disconnected)) => {
                            // Unreachable in practice (CHECKPOINT_CHANNELS is a
                            // process-lifetime Lazy), but if it ever happens the
                            // safety-net cannot do anything more — exit cleanly
                            // instead of hot-spinning.
                            warn!(
                                "Hybrid checkpoint forwarder: channel disconnected; \
                                 exiting forwarder task"
                            );
                            break;
                        }
                        Err(join_err) => {
                            // spawn_blocking panicked. Same disposition as
                            // ClickHouseSourceExec's checkpointing_task: log and
                            // exit rather than busy-loop on the panic mode.
                            error!(
                                "Hybrid checkpoint forwarder: recv spawn_blocking \
                                 panicked: {:?}",
                                join_err
                            );
                            break;
                        }
                    }
                }
            }
        });

        let schema_for_synth = schema.clone();
        let schema_for_main = schema.clone();
        let pending_for_main = pending_markers.clone();

        let reference_name_for_spawn = self.provider.reference_name.clone();

        tokio::spawn(async move {
            // Every exit path from the loop falls through to the
            // post-loop teardown below — `break 'outer` is used uniformly
            // (instead of `return`) so the forwarder is signalled to
            // stop, joined, and the subscription is unsubscribed even
            // when downstream drops the receiver mid-stream.
            'outer: loop {
                // Capture the phase before streaming so we know what we were
                // executing regardless of any concurrent state changes.
                let executing_phase = provider.state.read().await.current_phase;
                let is_executing_unbounded =
                    executing_phase >= provider.config.bounded_sources.len();

                let current_source_result = provider.get_current_source().await;
                let _current_source = match current_source_result {
                    Ok(source) => source,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break 'outer;
                    }
                };

                let stream_result = provider
                    .get_current_execution_stream(
                        partition,
                        context_clone.clone(),
                        &projection,
                        &filters,
                        limit,
                    )
                    .await;

                let mut stream = match stream_result {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break 'outer;
                    }
                };

                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(batch) => {
                            // Bounded sources (ClickHouse) emit u256/i256 columns as
                            // FixedSizeBinary(32) without extension metadata and in
                            // little-endian. Reverse the bytes and adopt the target
                            // schema's metadata so downstream arithmetic UDFs (which
                            // assume big-endian + metadata) see correct values.
                            // No-op for sources that already match (e.g. Kafka/Avro).
                            let normalized = match ClickHouseClient::normalize_batch_from_clickhouse(
                                &batch,
                                &schema_for_main,
                            ) {
                                Ok(b) => b,
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    break 'outer;
                                }
                            };
                            let merged = merge_pending_markers(normalized, &pending_for_main);
                            if tx.send(Ok(merged)).await.is_err() {
                                break 'outer;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            break 'outer;
                        }
                    }
                }

                // Inner stream ended. Flush any markers we accumulated but
                // never got to attach (because the inner source had already
                // stopped producing batches by the time they arrived). This
                // must happen BEFORE `advance_to_next_phase` — once we drop
                // into the next iteration of the loop and the next inner
                // source's `execute()` runs, any marker that arrived in this
                // window is invisible to the data stream forever.
                flush_pending_to_synth_batch(
                    &pending_for_main,
                    &schema_for_synth,
                    &tx,
                    &reference_name_for_spawn,
                )
                .await;

                // Stream ended — use our local knowledge of what phase we were
                // executing to decide whether to advance or exit.
                if is_executing_unbounded {
                    warn!("Unbounded source stream ended, exiting hybrid source");
                    break 'outer;
                } else {
                    match provider.advance_to_next_phase().await {
                        Ok(()) => {
                            let state = provider.state.read().await;
                            let now_unbounded =
                                state.current_phase >= provider.config.bounded_sources.len();
                            drop(state);

                            if now_unbounded && provider.config.job_mode {
                                info!(
                                    "Job mode: all {} bounded phase(s) complete, terminating hybrid source",
                                    provider.config.bounded_sources.len()
                                );
                                provider.shutdown();
                                break 'outer;
                            }

                            info!("Advanced to next phase, continuing with new source");
                            continue;
                        }
                        Err(e) => {
                            error!(
                                "Hybrid source '{}': advance_to_next_phase failed; \
                                 hybrid state may not have persisted the phase advance. \
                                 The next restart will probe per-phase state for recovery: {:?}",
                                provider.reference_name, e
                            );
                            let _ = tx.send(Err(e)).await;
                            break 'outer;
                        }
                    }
                }
            }

            // Final flush: if the loop exited with markers still buffered
            // (e.g. downstream dropped mid-batch, unbounded stream ended,
            // job_mode termination), they would otherwise be lost when the
            // subscription is torn down. Best-effort: if `tx` is already
            // closed the send is silently dropped, which matches the
            // pipeline-shutting-down semantics of those exit paths.
            flush_pending_to_synth_batch(
                &pending_for_main,
                &schema_for_synth,
                &tx,
                &reference_name_for_spawn,
            )
            .await;

            // Tear down the hybrid subscription. If the loop panicked above
            // we won't reach this block — but in that case the spawned task's
            // unwind drops `forwarder_shutdown_tx`, which still wakes the
            // forwarder via its `changed().await` and lets it exit. The
            // subscription entry in CHECKPOINT_CHANNELS will leak until
            // process exit, which is the same panic-leak class as every
            // other source (clickhouse, kafka, plugin) — not a regression
            // introduced by this PR.
            let _ = forwarder_shutdown_tx.send(());
            let _ = forwarder_handle.await;
            unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, hybrid_marker_sub_id);
        });

        Ok(builder.build())
    }
}

/// Merge any markers buffered by the hybrid-level subscription into the
/// outgoing batch's metadata, deduping by epoch so we never attach the same
/// Marker/Finalizer twice when the inner source already wrote one to the
/// same batch.
///
/// Note: the dedup HashSets are scoped to a single call — they do not
/// survive across batches. If the inner source attaches a message M to a
/// later batch B+1 while the hybrid forwarder already attached M to B,
/// downstream operators see M twice across the two batches. This is
/// operationally safe under the contracts each consumer obeys:
///
/// - **Marker → sink Ack**: the coordinator's Ack handler is idempotent
///   per `(epoch, sink)` — see `checkpoint_management.rs`:
///   `EpochState::InProgress` (HashSet insert is a no-op for duplicates)
///   and `EpochState::Finalized` (duplicate Ack just debug-logs). Duplicate
///   Acks are wasted work, never wrong results.
/// - **Finalizer → postgres sink truncation**: idempotent — the truncation
///   is `DELETE … WHERE _gs_checkpoint_epoch <= $1`, so a second call
///   deletes zero rows.
/// - **Finalizer → plugin operator**: forwarded to user code as
///   `PluginMsg::CheckpointFinalizer { epoch }`. Plugins are expected to
///   treat duplicate finalizers as no-ops; this is part of the plugin
///   contract, not a streamling-side guarantee. If we ever ship a plugin
///   that performs non-idempotent work on finalize, this dedup window
///   needs to be widened (e.g. an across-batch attached-epochs set held by
///   the hybrid spawn).
///
/// The inner source's marker delivery remains the primary path; this
/// helper is a strict additive safety net. Lock is held only long
/// enough to drain.
fn merge_pending_markers(
    batch: RecordBatch,
    pending: &Arc<Mutex<Vec<CheckpointMessage>>>,
) -> RecordBatch {
    let drained: Vec<CheckpointMessage> = {
        let mut g = pending.lock().unwrap();
        if g.is_empty() {
            return batch;
        }
        std::mem::take(&mut *g)
    };

    let existing = extract_checkpoint_messages(batch.schema().metadata());
    let mut seen_marker = HashSet::new();
    let mut seen_finalizer = HashSet::new();
    let mut combined = Vec::with_capacity(existing.len() + drained.len());
    for msg in existing.into_iter().chain(drained.iter().cloned()) {
        let keep = match &msg {
            CheckpointMessage::Marker { epoch, .. } => seen_marker.insert(epoch.0),
            CheckpointMessage::Finalizer(epoch) => seen_finalizer.insert(epoch.0),
            _ => true,
        };
        if keep {
            combined.push(msg);
        }
    }

    let mut md = batch.schema().metadata().clone();
    enrich_batch_metadata_with_checkpoints(&mut md, &combined);
    let new_schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().clone(),
        md,
    ));
    match RecordBatch::try_new(new_schema, batch.columns().to_vec()) {
        Ok(merged) => merged,
        Err(e) => {
            // Re-queue the drained markers so the next merge or the final
            // synthetic flush gets another shot at attaching them. Without
            // this, a transient try_new failure would silently lose every
            // hybrid-buffered Marker for this batch. Note: a retry on the
            // very next batch will likely fail for the same reason (same
            // schema shape); the recovery point is typically the final
            // synthetic flush in `flush_pending_to_synth_batch`, which uses
            // `RecordBatch::new_empty` and cannot fail.
            warn!(
                "Hybrid source: failed to rebuild batch with merged markers \
                 ({} marker(s) requeued for next successful attach or final flush): {}",
                drained.len(),
                e
            );
            pending.lock().unwrap().extend(drained);
            batch
        }
    }
}

/// Flush any markers currently buffered in `pending` to the downstream
/// stream as a synthetic empty `RecordBatch` whose schema carries the
/// markers in its metadata. Used both on inner-stream-end (so a marker
/// that arrived after the inner source stopped producing batches still
/// reaches the sink before the next phase starts) and on final teardown
/// (so a marker buffered during exit paths isn't lost when we
/// unsubscribe).
///
/// `RecordBatch::new_empty` cannot fail and constructs one zero-length
/// array per field, satisfying the column-count invariant that
/// `try_new(schema, vec![])` violates. Send failures are best-effort:
/// downstream may already be gone on teardown paths, in which case the
/// pipeline is shutting down anyway.
async fn flush_pending_to_synth_batch(
    pending: &Arc<Mutex<Vec<CheckpointMessage>>>,
    schema_for_synth: &SchemaRef,
    tx: &tokio::sync::mpsc::Sender<DataFusionResult<RecordBatch>>,
    reference_name: &str,
) {
    let leftover: Vec<CheckpointMessage> = {
        let mut g = pending.lock().unwrap();
        std::mem::take(&mut *g)
    };
    if leftover.is_empty() {
        return;
    }
    debug!(
        "Hybrid source '{}': flushing {} unattached marker(s) on a synthetic empty batch",
        reference_name,
        leftover.len()
    );
    let mut md = HashMap::new();
    enrich_batch_metadata_with_checkpoints(&mut md, &leftover);
    let synth_schema = Arc::new(Schema::new_with_metadata(
        schema_for_synth.fields().clone(),
        md,
    ));
    let synth = RecordBatch::new_empty(synth_schema);
    let _ = tx.send(Ok(synth)).await;
}

struct ClickHouseSchemaAdapter {
    client: ClickHouseClient,
}
impl ClickHouseSchemaAdapter {
    // Returns a list of column expressions that match the target schema.
    // If a column type doesn't match, it will be cast to the target type.
    // Extra columns in the ClickHouse table (not in target schema) are automatically
    // discarded - the ClickHouse table can be a superset of the target schema.
    fn get_columns(
        &self,
        table_name: &str,
        target_schema: &SchemaRef,
    ) -> Result<Vec<String>, DataFusionError> {
        let table_schema = self
            .client
            .fetch_schema(table_name, None, None)
            .map_err(|e| {
                streamling_err!(
                    "failed to fetch schema from ClickHouse for table '{}': {}",
                    table_name,
                    e
                )
            })?;
        let table_fields: HashMap<&str, Arc<Field>> = table_schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.clone()))
            .collect();

        let all_clickhouse_columns: Vec<String> = table_schema
            .fields()
            .iter()
            .filter(|f| f.name() != COLUMN_NAME_OP)
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        debug!(
            "Hybrid source bounded source (ClickHouse table '{}') all columns ({}): {:?}",
            table_name,
            all_clickhouse_columns.len(),
            all_clickhouse_columns
        );

        let mut columns = Vec::with_capacity(target_schema.fields().len());
        for target_field in target_schema.fields() {
            // Skip _gs_op as it's a virtual column that ClickHouse query builder adds automatically
            if target_field.name() == COLUMN_NAME_OP {
                continue;
            }
            let clickhouse_expression = match table_fields.get(target_field.name().as_str()) {
                Some(table_field) => {
                    if table_field.data_type() == target_field.data_type() {
                        format!("`{}`", target_field.name())
                    } else {
                        Self::convert_field_type(table_field, target_field)
                    }
                }
                None => {
                    streamling_user_bail!(
                        "column '{}' not found in ClickHouse table '{}'",
                        target_field.name(),
                        table_name
                    );
                }
            };
            columns.push(clickhouse_expression);
        }
        debug!(
            "Hybrid source bounded source (ClickHouse table '{}') selected columns matching unbounded schema ({}): {:?}",
            table_name,
            columns.len(),
            columns
        );
        Ok(columns)
    }

    fn convert_field_type(table_field: &Field, target_field: &Field) -> String {
        let mut clickhouse_type = ClickHouseClient::arrow_field_to_clickhouse(target_field);
        let can_be_nullable = !clickhouse_type.starts_with("Array(")
            && !clickhouse_type.starts_with("Tuple(")
            && !clickhouse_type.starts_with("Map(");
        if table_field.is_nullable() && can_be_nullable && !clickhouse_type.starts_with("Nullable(")
        {
            clickhouse_type = format!("Nullable({})", clickhouse_type);
        }
        let column = match (table_field.data_type(), target_field.data_type()) {
            (DataType::Decimal256(_, _), DataType::Decimal128(_, scale)) => {
                format!("toDecimal128(`{}`, {})", table_field.name(), scale,)
            }
            (DataType::Decimal128(_, _), DataType::Decimal256(_, scale)) => {
                format!("toDecimal256(`{}`, {})", table_field.name(), scale,)
            }
            (_, DataType::Decimal256(_, _)) => format!("toDecimal256(`{}`, 0)", table_field.name()),
            (_, DataType::Decimal128(_, _)) => format!("toDecimal128(`{}`, 0)", table_field.name()),
            (_, _) => format!("`{}`", table_field.name()),
        };
        format!(
            "CAST({} AS {}) AS `{}`",
            column,
            clickhouse_type,
            target_field.name()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Schema};
    use std::sync::LazyLock;
    use streamling_config::StateBackendConfig;
    use streamling_core::dynamic_table::DynamicTableRegistry;
    use streamling_core::session::SessionManager;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    async fn create_state_backend(name: &str) -> Arc<dyn StateOperatorBackend<HybridSourceState>> {
        let state_config = StateBackendConfig {
            backend_type: streamling_config::StateBackendType::InMemory,
            postgres: None,
            sqlite: None,
        };
        let factory = streamling_state::StateBackendFactories::new(state_config)
            .expect("Failed to create state backend factory");
        factory.create::<HybridSourceState>(name)
    }

    #[derive(Debug)]
    struct MockTableProvider {
        schema: SchemaRef,
        name: String,
    }

    impl MockTableProvider {
        fn new(name: String) -> Self {
            Self {
                schema: create_test_schema(),
                name,
            }
        }
    }

    #[async_trait]
    impl TableProvider for MockTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn datafusion::catalog::Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::NotImplemented(format!(
                "Mock table provider {} scan not implemented",
                self.name
            )))
        }
    }

    /// A bounded-phase mock that reports `has_persisted_state` to the
    /// hybrid restore probe. Used to exercise `RestoreFromHighestBoundedState`.
    #[derive(Debug)]
    pub(super) struct MockStatefulBoundedProvider {
        schema: SchemaRef,
        name: String,
        has_state: bool,
    }

    impl MockStatefulBoundedProvider {
        fn new(name: String, has_state: bool) -> Self {
            Self {
                schema: create_test_schema(),
                name,
                has_state,
            }
        }

        pub(super) fn has_persisted_state(&self) -> bool {
            self.has_state
        }
    }

    #[async_trait]
    impl TableProvider for MockStatefulBoundedProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn datafusion::catalog::Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::NotImplemented(format!(
                "MockStatefulBoundedProvider {} scan not implemented",
                self.name
            )))
        }
    }

    #[derive(Debug)]
    struct MockOffsetProvider {
        offsets: HashMap<i32, u32>,
    }

    impl MockOffsetProvider {
        fn new(offsets: HashMap<i32, u32>) -> Self {
            Self { offsets }
        }
    }

    #[async_trait]
    impl OffsetProvider for MockOffsetProvider {
        async fn get_offsets(&self) -> DataFusionResult<HashMap<i32, u32>> {
            Ok(self.offsets.clone())
        }
    }

    static SESSION_MANAGER: LazyLock<SessionManager, fn() -> SessionManager> =
        LazyLock::new(|| {
            SessionManager::new(100, 10, DynamicTableRegistry::new())
                .expect("session manager initialisation failed")
        });

    #[test]
    fn convert_field_type_casts_numeric_to_target_type() {
        let source = Field::new("src_block", DataType::UInt64, false);
        let target = Field::new("block", DataType::Int64, false);
        let expr = ClickHouseSchemaAdapter::convert_field_type(&source, &target);
        assert_eq!(expr, "CAST(`src_block` AS Int64) AS `block`");
    }

    #[test]
    fn convert_field_type_casts_nullable_numeric() {
        let source = Field::new("src_block", DataType::UInt64, false);
        let target = Field::new("block", DataType::Int64, true);
        let expr = ClickHouseSchemaAdapter::convert_field_type(&source, &target);
        assert_eq!(expr, "CAST(`src_block` AS Nullable(Int64)) AS `block`");
    }

    #[test]
    fn convert_field_type_keeps_nullable_when_source_is_nullable() {
        let source = Field::new("max_fee_per_gas", DataType::UInt64, true);
        let target = Field::new("max_fee_per_gas", DataType::Int64, false);
        let expr = ClickHouseSchemaAdapter::convert_field_type(&source, &target);
        assert_eq!(
            expr,
            "CAST(`max_fee_per_gas` AS Nullable(Int64)) AS `max_fee_per_gas`"
        );
    }

    #[test]
    fn convert_field_type_handles_decimal128() {
        let source = Field::new("src_amount", DataType::UInt64, false);
        let target = Field::new("amount", DataType::Decimal128(10, 2), false);
        let expr = ClickHouseSchemaAdapter::convert_field_type(&source, &target);
        assert_eq!(
            expr,
            "CAST(toDecimal128(`src_amount`, 0) AS Decimal(10, 2)) AS `amount`"
        );
    }

    #[test]
    fn convert_field_type_handles_nullable_decimal128() {
        let source = Field::new("src_amount", DataType::UInt64, false);
        let target = Field::new("amount", DataType::Decimal128(10, 2), true);
        let expr = ClickHouseSchemaAdapter::convert_field_type(&source, &target);
        assert_eq!(
            expr,
            "CAST(toDecimal128(`src_amount`, 0) AS Nullable(Decimal(10, 2))) AS `amount`"
        );
    }

    #[test]
    fn convert_field_type_handles_decimal256() {
        let source = Field::new("src_amount", DataType::UInt64, false);
        let target = Field::new("amount_big", DataType::Decimal256(30, 4), false);
        let expr = ClickHouseSchemaAdapter::convert_field_type(&source, &target);
        assert_eq!(
            expr,
            "CAST(toDecimal256(`src_amount`, 0) AS Decimal(30, 4)) AS `amount_big`"
        );
    }

    #[test]
    fn convert_field_type_handles_nullable_decimal256() {
        let source = Field::new("src_amount", DataType::UInt64, false);
        let target = Field::new("amount_big", DataType::Decimal256(30, 4), true);
        let expr = ClickHouseSchemaAdapter::convert_field_type(&source, &target);
        assert_eq!(
            expr,
            "CAST(toDecimal256(`src_amount`, 0) AS Nullable(Decimal(30, 4))) AS `amount_big`"
        );
    }

    #[test]
    fn convert_field_type_handles_decimal256_with_max_precision() {
        let source = Field::new("src_amount", DataType::UInt64, false);
        let target = Field::new("amount_big", DataType::Decimal256(100, 0), false);
        let expr = ClickHouseSchemaAdapter::convert_field_type(&source, &target);
        assert_eq!(
            expr,
            "CAST(toDecimal256(`src_amount`, 0) AS String) AS `amount_big`"
        );
    }

    #[test]
    fn test_get_columns_skips_gs_op() {
        // Create a target schema that includes _gs_op (like Kafka source would)
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("block_number", DataType::Int64, false),
            Field::new(COLUMN_NAME_OP, DataType::Utf8, false), // This should be skipped
            Field::new("data", DataType::Utf8, false),
        ]));

        // Simulate the logic from get_columns() - iterate through target schema fields
        // and skip _gs_op
        let mut columns = Vec::with_capacity(target_schema.fields().len());
        for target_field in target_schema.fields() {
            // Skip _gs_op as it's a virtual column that ClickHouse query builder adds automatically
            if target_field.name() == COLUMN_NAME_OP {
                continue;
            }
            columns.push(target_field.name().to_string());
        }

        // Verify that _gs_op is NOT in the columns list
        assert!(
            !columns.contains(&COLUMN_NAME_OP.to_string()),
            "_gs_op should be skipped and not included in columns list"
        );

        // Verify that other columns ARE in the list
        assert_eq!(
            columns.len(),
            3,
            "Should have 3 columns (id, block_number, data)"
        );
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"block_number".to_string()));
        assert!(columns.contains(&"data".to_string()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_source_state_management() {
        let bounded_sources = vec![
            Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>,
            Arc::new(MockTableProvider::new("bounded2".to_string())) as Arc<dyn TableProvider>,
        ];
        let unbounded_source =
            Arc::new(MockTableProvider::new("unbounded".to_string())) as Arc<dyn TableProvider>;

        let mut test_offsets = HashMap::new();
        test_offsets.insert(0, 1000);
        test_offsets.insert(1, 2000);
        let offset_provider = Some(
            Arc::new(MockOffsetProvider::new(test_offsets.clone())) as Arc<dyn OffsetProvider>
        );

        let config = HybridSourceConfig {
            bounded_sources,
            unbounded_source,
            offset_provider,
            job_mode: false,
        };

        let state_backend = create_state_backend("test_hybrid_source_state_management").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        let current_source = hybrid_provider.get_current_source().await.unwrap();
        assert_eq!(
            current_source
                .as_any()
                .downcast_ref::<MockTableProvider>()
                .unwrap()
                .name,
            "bounded1"
        );

        hybrid_provider.advance_to_next_phase().await.unwrap();

        let current_source = hybrid_provider.get_current_source().await.unwrap();
        assert_eq!(
            current_source
                .as_any()
                .downcast_ref::<MockTableProvider>()
                .unwrap()
                .name,
            "bounded2"
        );

        hybrid_provider.advance_to_next_phase().await.unwrap();

        let current_source = hybrid_provider.get_current_source().await.unwrap();
        assert_eq!(
            current_source
                .as_any()
                .downcast_ref::<MockTableProvider>()
                .unwrap()
                .name,
            "unbounded"
        );

        let state = hybrid_provider.state.read().await;
        assert!(state.unbounded_offsets.is_some());
        let offsets = state.unbounded_offsets.as_ref().unwrap();
        assert_eq!(offsets.get(&0), Some(&1000));
        assert_eq!(offsets.get(&1), Some(&2000));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_source_in_memory_state() {
        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: None,
            job_mode: false,
        };

        let state_backend = create_state_backend("test_hybrid_source_in_memory_state").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        let initial_state = hybrid_provider.state.read().await;
        assert_eq!(initial_state.current_phase, 0);
        assert_eq!(initial_state.completed_phases, vec![false]);
        drop(initial_state);

        hybrid_provider.advance_to_next_phase().await.unwrap();

        let updated_state = hybrid_provider.state.read().await;
        assert_eq!(updated_state.current_phase, 1);
        assert_eq!(updated_state.completed_phases, vec![true]);
        let current_source = hybrid_provider.get_current_source().await.unwrap();
        assert_eq!(
            current_source
                .as_any()
                .downcast_ref::<MockTableProvider>()
                .unwrap()
                .name,
            "unbounded"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_source_without_offset_provider() {
        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: None,
            job_mode: false,
        };

        let state_backend =
            create_state_backend("test_hybrid_source_without_offset_provider").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        hybrid_provider.advance_to_next_phase().await.unwrap();
        let current_source = hybrid_provider.get_current_source().await.unwrap();
        assert_eq!(
            current_source
                .as_any()
                .downcast_ref::<MockTableProvider>()
                .unwrap()
                .name,
            "unbounded"
        );

        let state = hybrid_provider.state.read().await;
        assert!(state.unbounded_offsets.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_source_empty_bounded_sources() {
        let unbounded_source =
            Arc::new(MockTableProvider::new("unbounded".to_string())) as Arc<dyn TableProvider>;

        let config = HybridSourceConfig {
            bounded_sources: vec![],
            unbounded_source,
            offset_provider: None,
            job_mode: false,
        };

        let state_backend = create_state_backend("test_hybrid_source_empty_bounded_sources").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        let current_source = hybrid_provider.get_current_source().await.unwrap();
        assert_eq!(
            current_source
                .as_any()
                .downcast_ref::<MockTableProvider>()
                .unwrap()
                .name,
            "unbounded"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_source_configuration_and_integration() {
        let bounded_sources = vec![
            Arc::new(MockTableProvider::new("clickhouse_mock".to_string()))
                as Arc<dyn TableProvider>,
        ];
        let unbounded_source =
            Arc::new(MockTableProvider::new("kafka_mock".to_string())) as Arc<dyn TableProvider>;

        let config = HybridSourceConfig {
            bounded_sources,
            unbounded_source,
            offset_provider: None,
            job_mode: false,
        };

        let state_backend = create_state_backend("test_integration").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        let current_source = hybrid_provider.get_current_source().await.unwrap();
        assert_eq!(
            current_source
                .as_any()
                .downcast_ref::<MockTableProvider>()
                .unwrap()
                .name,
            "clickhouse_mock"
        );

        hybrid_provider.advance_to_next_phase().await.unwrap();

        let final_source = hybrid_provider.get_current_source().await.unwrap();
        assert_eq!(
            final_source
                .as_any()
                .downcast_ref::<MockTableProvider>()
                .unwrap()
                .name,
            "kafka_mock"
        );

        let state = hybrid_provider.state.read().await;
        assert_eq!(state.current_phase, 1, "Should be in unbounded phase");
        assert_eq!(
            state.completed_phases,
            vec![true],
            "Bounded phase should be marked complete"
        );
    }

    // Helper structs for schema validation tests
    #[derive(Debug)]
    struct BoundedSourceWithSchema {
        inner: Arc<MockTableProvider>,
        schema: SchemaRef,
    }

    #[async_trait]
    impl TableProvider for BoundedSourceWithSchema {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }
    }

    #[derive(Debug)]
    struct UnboundedSourceWithSchema {
        inner: Arc<MockTableProvider>,
        schema: SchemaRef,
    }

    #[async_trait]
    impl TableProvider for UnboundedSourceWithSchema {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_table_provider_schema_validation_success() {
        let bounded_schema = Arc::new(Schema::new(vec![
            Field::new("block", DataType::Int64, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("extra_field", DataType::Utf8, true),
        ]));

        let unbounded_schema = Arc::new(Schema::new(vec![
            Field::new("block", DataType::Int64, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));

        let bounded_source = Arc::new(MockTableProvider::new("bounded".to_string()));
        let unbounded_source = Arc::new(MockTableProvider::new("unbounded".to_string()));

        let bounded_source_with_schema = BoundedSourceWithSchema {
            inner: bounded_source,
            schema: bounded_schema.clone(),
        };
        let unbounded_source_with_schema = UnboundedSourceWithSchema {
            inner: unbounded_source,
            schema: unbounded_schema.clone(),
        };

        let config = HybridSourceConfig {
            bounded_sources: vec![Arc::new(bounded_source_with_schema)],
            unbounded_source: Arc::new(unbounded_source_with_schema),
            offset_provider: None,
            job_mode: false,
        };

        let state_backend = create_state_backend("test_schema_validation_success").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_schema_validation_success".to_string(),
            config,
            unbounded_schema.clone(),
            state_backend,
            SESSION_MANAGER.clone(),
        );

        assert!(
            hybrid_provider.is_ok(),
            "Should successfully create hybrid provider with compatible schemas"
        );

        let provider = hybrid_provider.unwrap();
        let schema = provider.schema();
        assert!(
            schema.field_with_name("block").is_ok(),
            "Should have block field"
        );
        assert!(schema.field_with_name("id").is_ok(), "Should have id field");
        assert!(
            schema.field_with_name("data").is_ok(),
            "Should have data field"
        );
        assert!(
            schema.field_with_name("timestamp").is_ok(),
            "Should have timestamp field"
        );
        assert!(
            schema.field_with_name("extra_field").is_err(),
            "Should not have extra_field as we use unbounded schema"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_table_provider_schema_validation_failure() {
        let bounded_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, false),
        ]));

        let unbounded_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("block", DataType::Int64, false),
        ]));

        let bounded_source = Arc::new(MockTableProvider::new("bounded".to_string()));
        let unbounded_source = Arc::new(MockTableProvider::new("unbounded".to_string()));

        let bounded_source_with_schema = BoundedSourceWithSchema {
            inner: bounded_source,
            schema: bounded_schema.clone(),
        };
        let unbounded_source_with_schema = UnboundedSourceWithSchema {
            inner: unbounded_source,
            schema: unbounded_schema.clone(),
        };

        let config = HybridSourceConfig {
            bounded_sources: vec![Arc::new(bounded_source_with_schema)],
            unbounded_source: Arc::new(unbounded_source_with_schema),
            offset_provider: None,
            job_mode: false,
        };

        let state_backend = create_state_backend("test_schema_validation_failure").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_schema_validation_failure".to_string(),
            config,
            bounded_schema,
            state_backend,
            SESSION_MANAGER.clone(),
        );

        assert!(
            hybrid_provider.is_err(),
            "Should fail to create hybrid provider with incompatible schemas"
        );

        let error = hybrid_provider.unwrap_err();
        let error_message = format!("{}", error);
        assert!(
            error_message.contains("not found in bounded source"),
            "Error should mention missing columns, got: {}",
            error_message
        );
        assert!(
            error_message.contains("timestamp") || error_message.contains("block"),
            "Error should mention the missing timestamp or block columns, got: {}",
            error_message
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_table_provider_schema_validation_list_item_name_compatibility() {
        let bounded_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "blob_versioned_hashes",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
        ]));

        let unbounded_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "blob_versioned_hashes",
                DataType::List(Arc::new(Field::new("element", DataType::Utf8, false))),
                false,
            ),
        ]));

        let bounded_source = Arc::new(MockTableProvider::new("bounded".to_string()));
        let unbounded_source = Arc::new(MockTableProvider::new("unbounded".to_string()));

        let bounded_source_with_schema = BoundedSourceWithSchema {
            inner: bounded_source,
            schema: bounded_schema.clone(),
        };
        let unbounded_source_with_schema = UnboundedSourceWithSchema {
            inner: unbounded_source,
            schema: unbounded_schema.clone(),
        };

        let config = HybridSourceConfig {
            bounded_sources: vec![Arc::new(bounded_source_with_schema)],
            unbounded_source: Arc::new(unbounded_source_with_schema),
            offset_provider: None,
            job_mode: false,
        };

        let state_backend =
            create_state_backend("test_schema_validation_list_item_name_compatibility").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_schema_validation_list_item_name_compatibility".to_string(),
            config,
            unbounded_schema,
            state_backend,
            SESSION_MANAGER.clone(),
        );

        assert!(
            hybrid_provider.is_ok(),
            "List element field name differences (item vs element) should be compatible"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_source_job_mode_config() {
        let bounded_sources = vec![
            Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>,
            Arc::new(MockTableProvider::new("bounded2".to_string())) as Arc<dyn TableProvider>,
        ];
        let unbounded_source =
            Arc::new(MockTableProvider::new("unbounded".to_string())) as Arc<dyn TableProvider>;

        let config = HybridSourceConfig {
            bounded_sources,
            unbounded_source,
            offset_provider: None,
            job_mode: true,
        };

        let state_backend = create_state_backend("test_hybrid_source_job_mode_config").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        // Advance through both bounded phases
        hybrid_provider.advance_to_next_phase().await.unwrap();
        let state = hybrid_provider.state.read().await;
        assert_eq!(state.current_phase, 1, "Should be in second bounded phase");
        assert!(
            !state.completed_phases.iter().all(|&c| c),
            "Not all phases complete yet"
        );
        drop(state);

        hybrid_provider.advance_to_next_phase().await.unwrap();
        let state = hybrid_provider.state.read().await;
        assert_eq!(
            state.current_phase,
            hybrid_provider.config.bounded_sources.len(),
            "Should be past all bounded phases"
        );
        assert!(hybrid_provider.config.job_mode, "job_mode should be true");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_source_non_job_mode_unchanged() {
        let bounded_sources = vec![
            Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
        ];
        let unbounded_source =
            Arc::new(MockTableProvider::new("unbounded".to_string())) as Arc<dyn TableProvider>;

        let config = HybridSourceConfig {
            bounded_sources,
            unbounded_source,
            offset_provider: None,
            job_mode: false,
        };

        let state_backend = create_state_backend("test_hybrid_source_non_job_mode_unchanged").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        // Advance past bounded phase
        hybrid_provider.advance_to_next_phase().await.unwrap();

        // With job_mode=false, the unbounded source should be reachable
        let current_source = hybrid_provider.get_current_source().await.unwrap();
        assert_eq!(
            current_source
                .as_any()
                .downcast_ref::<MockTableProvider>()
                .unwrap()
                .name,
            "unbounded",
            "With job_mode=false, unbounded source should be reachable"
        );

        let state = hybrid_provider.state.read().await;
        assert_eq!(
            state.current_phase,
            hybrid_provider.config.bounded_sources.len(),
            "Should be in unbounded phase"
        );
        assert!(!hybrid_provider.config.job_mode, "job_mode should be false");
    }

    // -- helpers for execute-path tests --

    /// A TableProvider whose scan() returns a real ExecutionPlan that produces
    /// a finite (empty) stream, allowing HybridSourceExec::execute() to be tested.
    #[derive(Debug)]
    struct FiniteMockTableProvider {
        schema: SchemaRef,
    }

    impl FiniteMockTableProvider {
        fn new() -> Self {
            Self {
                schema: create_test_schema(),
            }
        }
    }

    #[async_trait]
    impl TableProvider for FiniteMockTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        async fn scan(
            &self,
            _state: &dyn datafusion::catalog::Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(FiniteMockExec {
                schema: self.schema.clone(),
                properties: PlanProperties::new(
                    EquivalenceProperties::new(self.schema.clone()),
                    datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
                    datafusion::physical_plan::execution_plan::EmissionType::Final,
                    datafusion::physical_plan::execution_plan::Boundedness::Bounded,
                ),
            }))
        }
    }

    /// Minimal ExecutionPlan that yields an empty, immediately-terminating stream.
    #[derive(Debug)]
    struct FiniteMockExec {
        schema: SchemaRef,
        properties: PlanProperties,
    }

    impl DisplayAs for FiniteMockExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "FiniteMockExec")
        }
    }

    impl ExecutionPlan for FiniteMockExec {
        fn name(&self) -> &str {
            "FiniteMockExec"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn properties(&self) -> &PlanProperties {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            let schema = self.schema.clone();
            Ok(Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::empty(),
                ),
            ))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_source_execute_terminates_in_job_mode() {
        let bounded_sources: Vec<Arc<dyn TableProvider>> = vec![
            Arc::new(FiniteMockTableProvider::new()),
            Arc::new(FiniteMockTableProvider::new()),
        ];
        let unbounded_source: Arc<dyn TableProvider> = Arc::new(FiniteMockTableProvider::new());

        let config = HybridSourceConfig {
            bounded_sources,
            unbounded_source,
            offset_provider: None,
            job_mode: true,
        };

        let state_backend =
            create_state_backend("test_hybrid_source_execute_terminates_in_job_mode").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_execute_job_mode".to_string(),
            config,
            create_test_schema(),
            state_backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        let session_state = SESSION_MANAGER.session_state();
        let plan = hybrid_provider
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan should succeed");

        let context = Arc::new(TaskContext::default());
        let stream = plan.execute(0, context).expect("execute should succeed");

        // Collect the stream — it MUST terminate (not hang) because job_mode=true
        // causes break after bounded phases complete.
        let batches: Vec<_> = stream.collect().await;
        // Stream should have ended without error
        for batch in &batches {
            assert!(batch.is_ok(), "stream batch should not be an error");
        }

        // Verify state: both bounded phases should be complete
        let state = hybrid_provider.state.read().await;
        assert_eq!(
            state.current_phase,
            hybrid_provider.config.bounded_sources.len(),
            "Should have advanced past all bounded phases"
        );
        assert!(
            state.completed_phases.iter().all(|&c| c),
            "All bounded phases should be marked complete"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hybrid_source_execute_continues_to_unbounded_without_job_mode() {
        let bounded_sources: Vec<Arc<dyn TableProvider>> =
            vec![Arc::new(FiniteMockTableProvider::new())];
        let unbounded_source: Arc<dyn TableProvider> = Arc::new(FiniteMockTableProvider::new());

        let config = HybridSourceConfig {
            bounded_sources,
            unbounded_source,
            offset_provider: None,
            job_mode: false,
        };

        let state_backend =
            create_state_backend("test_hybrid_source_execute_continues_without_job_mode").await;
        let hybrid_provider = HybridTableProvider::new(
            "test_execute_no_job_mode".to_string(),
            config,
            create_test_schema(),
            state_backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        let session_state = SESSION_MANAGER.session_state();
        let plan = hybrid_provider
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan should succeed");

        let context = Arc::new(TaskContext::default());
        let stream = plan.execute(0, context).expect("execute should succeed");

        let batches: Vec<_> = stream.collect().await;
        for batch in &batches {
            assert!(batch.is_ok(), "stream batch should not be an error");
        }

        // With job_mode=false, it should have advanced through bounded AND unbounded
        let state = hybrid_provider.state.read().await;
        assert!(
            state.current_phase >= hybrid_provider.config.bounded_sources.len(),
            "Should have reached unbounded phase"
        );
    }

    use streamling_state::testing::{FailCondition, FailableStateBackend};

    // ========================================================================
    // FM6: save_state fails — in-memory phase advanced, durable state absent
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fm6_save_state_failure_leaves_inconsistent_state() {
        let state_config = StateBackendConfig {
            backend_type: streamling_config::StateBackendType::InMemory,
            postgres: None,
            sqlite: None,
        };
        let factory = streamling_state::StateBackendFactories::new(state_config)
            .expect("Failed to create state backend factory");
        let inner: Arc<dyn StateOperatorBackend<HybridSourceState>> =
            factory.create::<HybridSourceState>("fm6_test");
        let failable = Arc::new(
            FailableStateBackend::new(inner.clone())
                .with_put_condition(FailCondition::OnKeyPrefix("hybrid_source_".to_string())),
        );

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: Some(Arc::new(MockOffsetProvider::new(HashMap::from([
                (0, 1000),
                (1, 2000),
            ])))),
            job_mode: false,
        };

        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            failable as Arc<dyn StateOperatorBackend<HybridSourceState>>,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        let result = hybrid_provider.advance_to_next_phase().await;
        assert!(
            result.is_err(),
            "save_state should fail due to injected fault"
        );

        // FM6: in-memory phase advanced despite save failure
        let state = hybrid_provider.state.read().await;
        assert_eq!(
            state.current_phase, 1,
            "in-memory phase should have advanced"
        );
        drop(state);

        // Durable state is absent
        let persisted = inner
            .get(StateKey("hybrid_source_test_hybrid_v1".to_string()))
            .await
            .unwrap();
        assert!(
            persisted.is_none(),
            "hybrid state should NOT be persisted after save failure"
        );

        // FM6 consequence: load_state on a new provider silently falls back to phase 0
        let hybrid_provider2 = HybridTableProvider::new(
            "test_hybrid".to_string(),
            HybridSourceConfig {
                bounded_sources: vec![Arc::new(MockTableProvider::new("bounded1".to_string()))
                    as Arc<dyn TableProvider>],
                unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                    as Arc<dyn TableProvider>,
                offset_provider: None,
                job_mode: false,
            },
            create_test_schema(),
            inner.clone(),
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        hybrid_provider2.load_state().await.unwrap();
        let state2 = hybrid_provider2.state.read().await;
        assert_eq!(
            state2.current_phase, 0,
            "new provider should fall back to phase 0 (FM6 silent fallback)"
        );
    }

    // ========================================================================
    // FM1: hybrid state cleared, source silently regresses to phase 0
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fm1_cleared_hybrid_state_causes_silent_phase_regression() {
        let state_backend = create_state_backend("fm1_test").await;

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: None,
            job_mode: false,
        };

        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend.clone(),
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        // Advance through bounded to unbounded
        hybrid_provider.advance_to_next_phase().await.unwrap();
        let state = hybrid_provider.state.read().await;
        assert_eq!(state.current_phase, 1, "should be in unbounded phase");
        drop(state);

        // Verify state was persisted
        let persisted = state_backend
            .get(StateKey("hybrid_source_test_hybrid_v1".to_string()))
            .await
            .unwrap();
        assert!(persisted.is_some(), "hybrid state should be persisted");

        // Simulate FM1: clear hybrid phase state
        state_backend
            .remove(StateKey("hybrid_source_test_hybrid_v1".to_string()))
            .await
            .unwrap();

        // Create new provider (simulating restart) with same backend
        let config2 = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: None,
            job_mode: false,
        };

        let hybrid_provider2 = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config2,
            create_test_schema(),
            state_backend.clone(),
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        // load_state should succeed with silent fallback
        hybrid_provider2.load_state().await.unwrap();
        let state2 = hybrid_provider2.state.read().await;
        assert_eq!(
            state2.current_phase, 0,
            "FM1: should silently fall back to phase 0 despite having previously been in unbounded"
        );
        drop(state2);

        // The source can re-advance through bounded phases from scratch
        hybrid_provider2.advance_to_next_phase().await.unwrap();
        let state3 = hybrid_provider2.state.read().await;
        assert_eq!(state3.current_phase, 1, "should re-advance to unbounded");
    }

    // ========================================================================
    // Retry: transient failure on first attempt, retry succeeds
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_save_state_retry_recovers_from_transient_failure() {
        let state_config = StateBackendConfig {
            backend_type: streamling_config::StateBackendType::InMemory,
            postgres: None,
            sqlite: None,
        };
        let factory = streamling_state::StateBackendFactories::new(state_config)
            .expect("Failed to create state backend factory");
        let inner: Arc<dyn StateOperatorBackend<HybridSourceState>> =
            factory.create::<HybridSourceState>("retry_test");

        // Fail on the 1st put call (first save_state attempt), succeed on retries
        let failable = Arc::new(FailableStateBackend::new(inner.clone()).with_put_condition(
            FailCondition::FailOnNthCall(std::sync::atomic::AtomicUsize::new(0), 1),
        ));

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: None,
            job_mode: false,
        };

        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            failable as Arc<dyn StateOperatorBackend<HybridSourceState>>,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        // advance_to_next_phase should succeed because retry recovers
        let result = hybrid_provider.advance_to_next_phase().await;
        assert!(
            result.is_ok(),
            "should succeed after retry: {:?}",
            result.err()
        );

        // State should be persisted
        let persisted = inner
            .get(StateKey("hybrid_source_test_hybrid_v1".to_string()))
            .await
            .unwrap();
        assert!(
            persisted.is_some(),
            "hybrid state should be persisted after retry succeeds"
        );
        assert_eq!(persisted.unwrap().current_phase, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_save_state_all_retries_exhausted() {
        let state_config = StateBackendConfig {
            backend_type: streamling_config::StateBackendType::InMemory,
            postgres: None,
            sqlite: None,
        };
        let factory = streamling_state::StateBackendFactories::new(state_config)
            .expect("Failed to create state backend factory");
        let inner: Arc<dyn StateOperatorBackend<HybridSourceState>> =
            factory.create::<HybridSourceState>("retry_exhaust_test");

        // Always fail — all 3 attempts will fail
        let failable = Arc::new(
            FailableStateBackend::new(inner.clone()).with_put_condition(FailCondition::Always),
        );

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: None,
            job_mode: false,
        };

        let hybrid_provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            failable as Arc<dyn StateOperatorBackend<HybridSourceState>>,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        let result = hybrid_provider.advance_to_next_phase().await;
        assert!(
            result.is_err(),
            "should fail after all retry attempts exhausted"
        );

        // State should NOT be persisted
        let persisted = inner
            .get(StateKey("hybrid_source_test_hybrid_v1".to_string()))
            .await
            .unwrap();
        assert!(
            persisted.is_none(),
            "hybrid state should not be persisted when all retries fail"
        );
    }

    // ========================================================================
    // Recovery from Kafka offsets: fallback paths
    // ========================================================================

    #[derive(Debug)]
    struct FailingOffsetProvider;

    #[async_trait]
    impl OffsetProvider for FailingOffsetProvider {
        async fn get_offsets(&self) -> DataFusionResult<HashMap<i32, u32>> {
            Err(datafusion::error::DataFusionError::Execution(
                "injected offset provider failure".to_string(),
            ))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recovery_falls_back_when_offset_provider_fails() {
        let state_config = StateBackendConfig {
            backend_type: streamling_config::StateBackendType::InMemory,
            postgres: None,
            sqlite: None,
        };
        let factory = streamling_state::StateBackendFactories::new(state_config)
            .expect("Failed to create state backend factory");
        let backend: Arc<dyn StateOperatorBackend<HybridSourceState>> =
            factory.create::<HybridSourceState>("recovery_fail_test");

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: Some(Arc::new(FailingOffsetProvider)),
            job_mode: false,
        };

        let provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        provider.load_state().await.unwrap();
        let state = provider.state.read().await;
        assert_eq!(
            state.current_phase, 0,
            "should fall back to phase 0 when offset provider fails"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recovery_falls_back_when_offset_provider_returns_empty() {
        let state_config = StateBackendConfig {
            backend_type: streamling_config::StateBackendType::InMemory,
            postgres: None,
            sqlite: None,
        };
        let factory = streamling_state::StateBackendFactories::new(state_config)
            .expect("Failed to create state backend factory");
        let backend: Arc<dyn StateOperatorBackend<HybridSourceState>> =
            factory.create::<HybridSourceState>("recovery_empty_test");

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: Some(Arc::new(MockOffsetProvider::new(HashMap::new()))),
            job_mode: false,
        };

        let provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        provider.load_state().await.unwrap();
        let state = provider.state.read().await;
        assert_eq!(
            state.current_phase, 0,
            "should fall back to phase 0 when offset provider returns empty map"
        );
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn job_mode_advancing_past_bounded_skips_offset_provider() {
        // Regression: in job_mode, advancing to the unbounded transition must
        // NOT call offset_provider.get_offsets(). That ClickHouse max-offset
        // query + Kafka seed is for an unbounded phase job_mode never runs, and
        // blocking on it stalls advance_to_next_phase — which the hybrid execute
        // loop must return from before it can break and end the source stream
        // (the production job-mode termination hang).
        let state_config = StateBackendConfig {
            backend_type: streamling_config::StateBackendType::InMemory,
            postgres: None,
            sqlite: None,
        };
        let factory = streamling_state::StateBackendFactories::new(state_config)
            .expect("Failed to create state backend factory");
        let backend: Arc<dyn StateOperatorBackend<HybridSourceState>> =
            factory.create::<HybridSourceState>("job_mode_skip_offsets_test");

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            // Returns Err if ever called. job_mode must skip it, so advance
            // succeeds; without the skip, advance would propagate the error.
            offset_provider: Some(Arc::new(FailingOffsetProvider)),
            job_mode: true,
        };

        let provider = HybridTableProvider::new(
            "test_job_mode_skip_offsets".to_string(),
            config,
            create_test_schema(),
            backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        provider.load_state().await.unwrap();
        // Advance past the single bounded phase (0 -> 1 == bounded_sources.len()),
        // the unbounded transition. In job_mode it must skip the offset provider.
        let result = provider.advance_to_next_phase().await;
        assert!(
            result.is_ok(),
            "job_mode advance should skip the offset provider, got: {:?}",
            result
        );
        let state = provider.state.read().await;
        assert_eq!(
            state.current_phase, 1,
            "phase bookkeeping still advances; only the offset work is skipped"
        );
        assert!(
            state.unbounded_offsets.is_none(),
            "offsets must not be fetched in job_mode"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recovery_falls_back_when_no_offset_provider() {
        let state_config = StateBackendConfig {
            backend_type: streamling_config::StateBackendType::InMemory,
            postgres: None,
            sqlite: None,
        };
        let factory = streamling_state::StateBackendFactories::new(state_config)
            .expect("Failed to create state backend factory");
        let backend: Arc<dyn StateOperatorBackend<HybridSourceState>> =
            factory.create::<HybridSourceState>("recovery_no_provider_test");

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: None,
            job_mode: false,
        };

        let provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        provider.load_state().await.unwrap();
        let state = provider.state.read().await;
        assert_eq!(
            state.current_phase, 0,
            "should fall back to phase 0 when no offset provider"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recovery_falls_back_when_downcast_fails_with_offsets() {
        let state_config = StateBackendConfig {
            backend_type: streamling_config::StateBackendType::InMemory,
            postgres: None,
            sqlite: None,
        };
        let factory = streamling_state::StateBackendFactories::new(state_config)
            .expect("Failed to create state backend factory");
        let backend: Arc<dyn StateOperatorBackend<HybridSourceState>> =
            factory.create::<HybridSourceState>("recovery_downcast_test");

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockTableProvider::new("bounded1".to_string())) as Arc<dyn TableProvider>
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: Some(Arc::new(MockOffsetProvider::new(HashMap::from([
                (0, 1000),
                (1, 2000),
                (2, 3000),
            ])))),
            job_mode: false,
        };

        let provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            backend,
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        // Offset provider returns partitions, but MockTableProvider can't downcast to
        // KafkaSourceTableProvider, so recovery falls back gracefully to phase 0
        provider.load_state().await.unwrap();
        let state = provider.state.read().await;
        assert_eq!(
            state.current_phase, 0,
            "should fall back to phase 0 when downcast to KafkaSourceTableProvider fails"
        );
        assert!(
            state.unbounded_offsets.is_none(),
            "unbounded_offsets should remain None when recovery doesn't trigger"
        );
    }

    // ========================================================================
    // Bounded-phase recovery (RestoreFromHighestBoundedState from spec)
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recovery_picks_up_single_bounded_phase_with_cursor() {
        let state_backend = create_state_backend("recover_single_bounded").await;

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockStatefulBoundedProvider::new("b0".to_string(), true))
                    as Arc<dyn TableProvider>,
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: None,
            job_mode: false,
        };

        let provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend.clone(),
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        provider.load_state().await.unwrap();

        let state = provider.state.read().await;
        assert_eq!(
            state.current_phase, 0,
            "single-bounded recovery should land at phase 0"
        );
        assert_eq!(
            state.completed_phases,
            vec![false],
            "phase 0 itself is the recovery target and is NOT yet completed"
        );
        drop(state);

        // Recovery must persist hybrid state so the next restart hits the
        // happy path instead of probing again.
        let persisted = state_backend
            .get(StateKey("hybrid_source_test_hybrid_v1".to_string()))
            .await
            .unwrap();
        assert!(
            persisted.is_some(),
            "bounded recovery must persist hybrid state"
        );
        assert_eq!(persisted.unwrap().current_phase, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recovery_picks_highest_indexed_bounded_with_state() {
        // Topology: bounded[0] has no state, bounded[1] has state,
        // bounded[2] has no state. Highest-indexed-with-state is index 1.
        let state_backend = create_state_backend("recover_highest_bounded").await;

        let config = HybridSourceConfig {
            bounded_sources: vec![
                Arc::new(MockStatefulBoundedProvider::new("b0".to_string(), false))
                    as Arc<dyn TableProvider>,
                Arc::new(MockStatefulBoundedProvider::new("b1".to_string(), true))
                    as Arc<dyn TableProvider>,
                Arc::new(MockStatefulBoundedProvider::new("b2".to_string(), false))
                    as Arc<dyn TableProvider>,
            ],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: None,
            job_mode: false,
        };

        let provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend.clone(),
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        provider.load_state().await.unwrap();

        let state = provider.state.read().await;
        assert_eq!(
            state.current_phase, 1,
            "recovery should land at the highest bounded index with state (1)"
        );
        assert_eq!(
            state.completed_phases,
            vec![true, false, false],
            "phases below the recovery target are marked completed; the target itself is not"
        );
        drop(state);

        let persisted = state_backend
            .get(StateKey("hybrid_source_test_hybrid_v1".to_string()))
            .await
            .unwrap()
            .expect("bounded recovery must persist hybrid state");
        assert_eq!(persisted.current_phase, 1);
        assert_eq!(persisted.completed_phases, vec![true, false, false]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_first_run_persists_default_state_after_warn() {
        // Hybrid state missing, no bounded phase reports persisted state,
        // no offset provider, no Kafka recovery. Should land at phase 0
        // and persist default state so the next restart hits the happy path.
        let state_backend = create_state_backend("first_run").await;

        let config = HybridSourceConfig {
            bounded_sources: vec![Arc::new(MockStatefulBoundedProvider::new(
                "b0".to_string(),
                false,
            )) as Arc<dyn TableProvider>],
            unbounded_source: Arc::new(MockTableProvider::new("unbounded".to_string()))
                as Arc<dyn TableProvider>,
            offset_provider: None,
            job_mode: false,
        };

        let provider = HybridTableProvider::new(
            "test_hybrid".to_string(),
            config,
            create_test_schema(),
            state_backend.clone(),
            SESSION_MANAGER.clone(),
        )
        .unwrap();

        provider.load_state().await.unwrap();

        let state = provider.state.read().await;
        assert_eq!(state.current_phase, 0);
        assert_eq!(state.completed_phases, vec![false]);
        assert!(state.unbounded_offsets.is_none());
        drop(state);

        let persisted = state_backend
            .get(StateKey("hybrid_source_test_hybrid_v1".to_string()))
            .await
            .unwrap()
            .expect("first-run recovery must persist default state");
        assert_eq!(persisted.current_phase, 0);
    }
}

#[derive(Debug)]
pub struct ClickHouseOffsetProvider {
    client: ClickHouseClient,
    topic_name: String,
    offset_table: String,
    topic_column: String,
    partition_column: String,
    offset_column: String,
}

impl ClickHouseOffsetProvider {
    pub fn new(client: ClickHouseClient, hybrid_offset_table: HybridOffsetTable) -> Self {
        Self {
            client,
            topic_name: hybrid_offset_table.topic_name.replace(".", "_"),
            offset_table: hybrid_offset_table
                .table_name
                .unwrap_or("kafka_offsets".to_string()),
            topic_column: hybrid_offset_table
                .topic_column
                .unwrap_or("topic".to_string()),
            partition_column: hybrid_offset_table
                .partition_column
                .unwrap_or("partition".to_string()),
            offset_column: hybrid_offset_table
                .offset_column
                .unwrap_or("offset".to_string()),
        }
    }
}

#[async_trait]
impl OffsetProvider for ClickHouseOffsetProvider {
    async fn get_offsets(&self) -> DataFusionResult<HashMap<i32, u32>> {
        let query = format!(
            "SELECT {}, max({}) FROM {} WHERE {} = '{}' GROUP BY 1 FORMAT Arrow",
            self.partition_column,
            self.offset_column,
            self.offset_table,
            self.topic_column,
            self.topic_name
        );

        match self.client.send_query(reqwest::Method::GET, &query).await {
            Ok(response) => {
                let response_bytes = response
                    .bytes()
                    .await
                    .streamling_context("failed to read ClickHouse offset response")?;

                let cursor = std::io::Cursor::new(response_bytes.to_vec());
                let reader = datafusion::arrow::ipc::reader::FileReader::try_new(cursor, None)
                    .streamling_context(
                        "failed to read Arrow IPC from ClickHouse offset response",
                    )?;

                let mut offsets = HashMap::new();

                for batch_result in reader {
                    let batch = batch_result.streamling_context(
                        "failed to read batch from ClickHouse offset response",
                    )?;
                    let partition_array = batch.column(0);
                    let offset_array = batch.column(1);

                    for i in 0..batch.num_rows() {
                        if let (Some(partition), Some(offset)) = (
                            partition_array
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                                .map(|a| a.value(i)),
                            offset_array
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::UInt32Array>()
                                .map(|a| a.value(i)),
                        ) {
                            offsets.insert(partition, offset);
                        }
                    }
                }

                info!("Fetched {} Kafka offsets from ClickHouse", offsets.len());
                Ok(offsets)
            }
            Err(e) => {
                warn!("Failed to fetch offsets from ClickHouse: {}", e);
                Err(streamling_err!("failed to fetch offsets from ClickHouse: {}", e).into())
            }
        }
    }
}
