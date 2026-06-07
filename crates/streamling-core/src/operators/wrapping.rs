use crate::checkpoints::checkpoint_management::{
    CheckpointMessage, extract_checkpoint_messages, now_ms,
};
use crate::operators::filter::StreamingFilterExec;
use crate::operators::inspect::LiveDataInspect;
use crate::operators::scan_sharing::{BroadcastingExec, SharedSourceHandle, SharedSourceRegistry};
use crate::session::{get_streamling_config, get_streamling_config_from_session};
use crate::side_output::{SourceSideOutput, SupportsSideOutputs};
use crate::telemetry::EventTimeReader;
use crate::telemetry::recorder::{MetricsRecorder, get_metrics_recorder};
use crate::telemetry::types::RowCountMeasurementType;
use crate::topology::Telemetry;
use crate::utils::dedup::deduplicate_record_batch;
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::not_impl_err;
use datafusion::common::{DFSchemaRef, DataFusionError, Result};
use datafusion::config::ConfigOptions;
use datafusion::datasource::sink::DataSink;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{Distribution, OrderingRequirements};
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics, execute_input_stream,
};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use delegate::delegate;
use futures::StreamExt;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

/// Per-source event-time instrumentation state, propagated through every
/// `WrappingExec` reconstruction (`with_new_children`,
/// `wrap_with_side_outputs_before_filter`) so optimizer rewrites cannot
/// silently drop the freshness signal.
///
/// Cloning is shallow: the watermark `Arc<Mutex<_>>` is shared across all
/// clones — per-source state lives once, regardless of how many
/// physical-plan reconstructions DataFusion performs.
#[derive(Clone, Debug)]
pub struct EventTimeInstrumentation {
    reader: EventTimeReader,
    /// Running max event_time observed from this source, in unix
    /// milliseconds. `None` until the first non-null value arrives, which
    /// is the trigger for the first watermark gauge emission.
    watermark_state: Arc<Mutex<Option<u64>>>,
    /// Log-once gate for read errors (missing column, wrong type, missing
    /// unit). Shared across all clones so DataFusion optimizer rewrites of
    /// a single source don't each emit their own warning. A follow-up to
    /// move this to startup validation would let this path go cold entirely.
    read_error_logged: Arc<AtomicBool>,
    /// Log-once gate for the rare case where the `event_time_lag` histogram
    /// isn't registered for this metadata_id. Independent from
    /// `read_error_logged` so both distinct misconfigs remain observable.
    histogram_unresolved_logged: Arc<AtomicBool>,
}

impl EventTimeInstrumentation {
    pub fn new(reader: EventTimeReader) -> Self {
        Self {
            reader,
            watermark_state: Arc::new(Mutex::new(None)),
            read_error_logged: Arc::new(AtomicBool::new(false)),
            histogram_unresolved_logged: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Build instrumentation from the per-node `Telemetry` config, returning
    /// `None` when the node has no telemetry (or its `event_time`) configured.
    pub fn from_telemetry(telemetry: Option<&Telemetry>) -> Option<Self> {
        let event_time = telemetry?.event_time()?;
        Some(Self::new(EventTimeReader::from_config(event_time)))
    }
}

/// Per-batch emission of the event-time watermark gauge and lag histogram.
///
/// Caller guarantees `batch.num_rows() > 0` so we never burn lookups on
/// empty batches. On reader error (missing column, wrong type, missing
/// unit), emits a single `warn!` per source instance and skips the batch
/// — subsequent batches hitting the same misconfiguration stay silent so
/// warning-log-count health checks aren't swamped by per-batch noise.
/// Startup-time validation remains a future improvement that would let
/// this runtime warn path go cold entirely.
fn emit_event_time_metrics(
    instrumentation: &EventTimeInstrumentation,
    batch: &RecordBatch,
    metric_metadata_id: &str,
    metrics_recorder: &MetricsRecorder,
) {
    // Histogram: per-row lag emission via a pre-bound handle. Resolving once
    // per batch keeps the per-row cost down to OTEL's internal aggregation
    // rather than a streamling-core mutex acquisition per row.
    //
    // `resolve_histogram` returns None when the histogram isn't registered
    // (Unit 2 should have prevented this) or when the metadata_id is unknown
    // to the registry. In that case we skip lag emission but still update the
    // watermark — they use independent code paths and there's no reason to
    // lose the gauge when the histogram is unavailable.
    let bound = metrics_recorder.resolve_histogram("event_time_lag", metric_metadata_id);
    if bound.is_none()
        && !instrumentation
            .histogram_unresolved_logged
            .swap(true, Ordering::Relaxed)
    {
        warn!(
            "event_time_lag histogram could not be resolved for source '{}' \
             (metadata_id may be unregistered); skipping lag emission \
             (logging once per source).",
            metric_metadata_id
        );
    }

    // Single-pass iteration: compute watermark + emit histogram observations
    // inline via the reader callback. Avoids the per-batch
    // `Vec<Option<u64>>` allocation that a source+transform+sink topology
    // would pay three times per batch.
    let now = now_ms();
    let mut batch_max: Option<u64> = None;
    let read_result = instrumentation.reader.for_each_value(batch, |value| {
        if let Some(v) = value {
            if let Some(bound) = bound.as_ref() {
                // saturating_sub handles clock skew where event_time is slightly
                // ahead of wall clock (e.g. broker time vs. consumer time): the
                // histogram observes 0ms lag rather than underflowing.
                bound.record(now.saturating_sub(v));
            }
            batch_max = Some(batch_max.map_or(v, |m| m.max(v)));
        }
    });
    if let Err(e) = read_result {
        // The reader fails fast on column/type errors before any row is
        // yielded, so `batch_max` is guaranteed None here — nothing to
        // unwind. Warn-once and skip.
        if !instrumentation
            .read_error_logged
            .swap(true, Ordering::Relaxed)
        {
            warn!(
                "event-time instrumentation skipped for source '{}': {} \
                 (logging once per source; subsequent batches with the same error are silent)",
                metric_metadata_id, e
            );
        }
        return;
    }

    // Watermark gauge: re-emitted on every batch that contributed at least
    // one non-null value, even when the running max didn't advance. The
    // alternative — emit only on advancement — leaves Prometheus series
    // stale during late-arriving-data periods, triggering false-positive
    // "stuck source" alerts on the canonical PromQL expression.
    if let Some(max) = batch_max {
        let mut state = instrumentation
            .watermark_state
            .lock()
            .expect("event-time watermark mutex poisoned");
        let new_watermark = state.map_or(max, |existing| existing.max(max));
        *state = Some(new_watermark);
        drop(state);
        metrics_recorder.record_gauge_w_tags(
            "event_time_watermark",
            new_watermark,
            Vec::new(),
            metric_metadata_id,
        );
    }
}

/// WrappingSourceTableProvider wraps any source TableProvider and automatically adds additional
/// capabilities like telemetry and dynamic tables to its ExecutionPlan without requiring changes
/// to the underlying implementation
#[derive(Debug, Clone)]
pub struct WrappingSourceTableProvider {
    inner: Arc<dyn TableProvider>,
    reference_name: String,
    side_outputs: Arc<RwLock<Vec<Arc<dyn SourceSideOutput>>>>,
    scan_sharing_registry: Option<SharedSourceRegistry>,
    /// Per-source event-time freshness instrumentation, shared across every
    /// `WrappingExec` constructed for this source so the running watermark
    /// state is not duplicated.
    event_time_instrumentation: Option<EventTimeInstrumentation>,
}

impl WrappingSourceTableProvider {
    pub fn new(
        inner: Arc<dyn TableProvider>,
        reference_name: String,
        scan_sharing_registry: Option<SharedSourceRegistry>,
        telemetry: Option<&Telemetry>,
    ) -> Self {
        Self {
            inner,
            reference_name,
            side_outputs: Arc::new(RwLock::new(Vec::new())),
            scan_sharing_registry,
            event_time_instrumentation: EventTimeInstrumentation::from_telemetry(telemetry),
        }
    }

    pub fn get_inner(&self) -> Arc<dyn TableProvider> {
        self.inner.clone()
    }

    pub fn get_side_outputs(&self) -> Vec<Arc<dyn SourceSideOutput>> {
        self.side_outputs
            .read()
            .map(|g| g.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// When the inner plan is a `StreamingFilterExec`, place the `WrappingExec` (with side
    /// outputs like `BlockReporter`) around the unfiltered source, then re-apply the filter
    /// on top. This ensures side outputs observe **all** rows before filtering, giving
    /// accurate lag calculations even when a source-level filter is configured. Same
    /// rationale applies to event-time instrumentation: the `WrappingExec` sits below
    /// the source filter so freshness metrics see the pre-filter row stream.
    fn wrap_with_side_outputs_before_filter(
        inner_exec: Arc<dyn ExecutionPlan>,
        reference_name: &str,
        side_outputs: Vec<Arc<dyn SourceSideOutput>>,
        event_time_instrumentation: Option<EventTimeInstrumentation>,
    ) -> Arc<dyn ExecutionPlan> {
        if let Some(filter_exec) = inner_exec.as_any().downcast_ref::<StreamingFilterExec>() {
            let source_exec = filter_exec.input().clone();
            let wrapped_source: Arc<dyn ExecutionPlan> = Arc::new(WrappingExec::new(
                source_exec,
                reference_name.to_string(),
                side_outputs.clone(),
                Vec::new(),
                event_time_instrumentation.clone(),
            ));
            match Arc::new(filter_exec.clone()).with_new_children(vec![wrapped_source]) {
                Ok(plan) => plan,
                Err(e) => {
                    warn!(
                        "Failed to rebuild filter around WrappingExec for '{}': {}. \
                         Falling back to wrapping the filtered plan.",
                        reference_name, e
                    );
                    Arc::new(WrappingExec::new(
                        inner_exec,
                        reference_name.to_string(),
                        side_outputs,
                        Vec::new(),
                        event_time_instrumentation,
                    ))
                }
            }
        } else {
            Arc::new(WrappingExec::new(
                inner_exec,
                reference_name.to_string(),
                side_outputs,
                Vec::new(),
                event_time_instrumentation,
            ))
        }
    }
}

impl SupportsSideOutputs for WrappingSourceTableProvider {
    fn add_side_output(&self, side_output: Arc<dyn SourceSideOutput>) {
        if let Ok(mut w) = self.side_outputs.write() {
            w.push(side_output);
        } else {
            warn!("Failed to acquire write lock for side outputs");
        }
    }
}

#[async_trait]
impl TableProvider for WrappingSourceTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // If scan sharing is enabled, use lazy initialization to ensure inner.scan is called only once
        if let Some(registry) = &self.scan_sharing_registry {
            let schema = self.inner.schema();
            let reference_name = self.reference_name.clone();

            // Check if handle already exists (scan already happened)
            {
                let sources = registry.sources.read().unwrap();
                if let Some(handle) = sources.get(&reference_name) {
                    trace!(
                        "Reusing existing shared source handle for: {}",
                        reference_name
                    );
                    return Ok(Arc::new(BroadcastingExec::new(handle.clone())));
                }
            }

            // First scan - call inner.scan and create the handle
            debug!(
                "First scan for source: {}, calling inner.scan",
                reference_name
            );
            let inner_exec = self.inner.scan(state, projection, filters, limit).await?;

            let side_outputs: Vec<Arc<dyn SourceSideOutput>> = self
                .side_outputs
                .read()
                .map(|g| g.iter().cloned().collect())
                .unwrap_or_default();

            let wrapped_exec = Self::wrap_with_side_outputs_before_filter(
                inner_exec,
                &reference_name,
                side_outputs,
                self.event_time_instrumentation.clone(),
            );

            // Get internal_buffer_size from Session config
            let internal_buffer_size =
                get_streamling_config_from_session(state)?.internal_buffer_size;

            // Store in registry
            let handle = {
                let mut sources = registry.sources.write().unwrap();
                // Double-check in case another thread created it
                if let Some(existing_handle) = sources.get(&reference_name) {
                    trace!("Another thread created the handle first, using that");
                    existing_handle.clone()
                } else {
                    let expected_count =
                        SharedSourceRegistry::get_expected_consumers(&reference_name).unwrap_or(0);
                    let handle = Arc::new(SharedSourceHandle::new(
                        schema.clone(),
                        wrapped_exec,
                        internal_buffer_size,
                        expected_count,
                    ));
                    sources.insert(reference_name.clone(), handle.clone());
                    handle
                }
            };

            Ok(Arc::new(BroadcastingExec::new(handle)))
        } else {
            // No scan sharing - use the old behavior
            let inner_exec = self.inner.scan(state, projection, filters, limit).await?;

            let side_outputs: Vec<Arc<dyn SourceSideOutput>> = self
                .side_outputs
                .read()
                .map(|g| g.iter().cloned().collect())
                .unwrap_or_default();

            Ok(Self::wrap_with_side_outputs_before_filter(
                inner_exec,
                &self.reference_name,
                side_outputs,
                self.event_time_instrumentation.clone(),
            ))
        }
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        /*
        Sinks do not complete the `write_all` method (streaming-use-case)
        so it's difficult to get the actual number of records processed by using the composition pattern
         */
        not_impl_err!("WrappingSourceTableProvider not supported for sinks")
    }
}

/// Wrapper around `ExecutionPlan` that injects additional functionality like telemetry related
/// attributes and dynamic tables
#[derive(Debug)]
pub struct WrappingExec {
    inner: Arc<dyn ExecutionPlan>,
    reference_name: String,
    side_outputs: Vec<Arc<dyn SourceSideOutput>>,
    forced_non_null_columns: Vec<String>,
    adjusted_schema: SchemaRef,
    /// Per-source event-time instrumentation. `None` when the source has no
    /// `telemetry.event_time` configured. Cloned (shallowly) into every
    /// reconstruction so per-source watermark state survives DataFusion
    /// optimizer rewrites.
    event_time_instrumentation: Option<EventTimeInstrumentation>,
}

impl WrappingExec {
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        reference_name: String,
        side_outputs: Vec<Arc<dyn SourceSideOutput>>,
        forced_non_null_columns: Vec<String>,
        event_time_instrumentation: Option<EventTimeInstrumentation>,
    ) -> WrappingExec {
        trace!(
            "Running WrappingExec::new for inner_node_name: {:?}, reference_name: {}",
            inner.name(),
            reference_name
        );
        // Pre-compute adjusted schema with forced non-null columns
        let base_schema = inner.schema();
        let forced: HashSet<&str> = forced_non_null_columns.iter().map(|s| s.as_str()).collect();
        let adjusted_fields = base_schema
            .fields()
            .iter()
            .map(|f| {
                if forced.contains(f.name().as_str()) && f.is_nullable() {
                    Arc::new(
                        ArrowField::new(f.name(), f.data_type().clone(), false)
                            .with_metadata(f.metadata().clone()),
                    )
                } else {
                    f.clone()
                }
            })
            .collect::<Vec<_>>();
        let adjusted_schema: SchemaRef = Arc::new(ArrowSchema::new_with_metadata(
            adjusted_fields,
            base_schema.metadata().clone(),
        ));
        Self {
            inner,
            reference_name,
            side_outputs,
            forced_non_null_columns,
            adjusted_schema,
            event_time_instrumentation,
        }
    }
}

/// Used to intercept `execute()` calls and run additional logic like telemetry processing
impl ExecutionPlan for WrappingExec {
    delegate! {
        to self.inner {
            fn name(&self) -> &str;
            fn properties(&self) -> &PlanProperties;
            fn check_invariants(&self, check: InvariantLevel) -> Result<()>;
            fn required_input_distribution(&self) -> Vec<Distribution>;
            fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>>;
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn repartitioned(
                &self,
                target_partitions: usize,
                config: &ConfigOptions,
            ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
            fn metrics(&self) -> Option<MetricsSet>;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics>;
            fn supports_limit_pushdown(&self) -> bool;
            fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>>;
            fn cardinality_effect(&self) -> CardinalityEffect;
            fn try_swapping_with_projection(
                &self,
                projection: &ProjectionExec,
            ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
        }
    }

    fn schema(&self) -> SchemaRef {
        self.adjusted_schema.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_inner = self.inner.clone().with_new_children(children)?;
        // Clone the event-time instrumentation so the new exec shares the
        // SAME watermark state Arc — without this, DataFusion optimizer
        // rewrites would silently start a fresh watermark for every
        // reconstruction.
        Ok(Arc::new(WrappingExec::new(
            new_inner,
            self.reference_name.clone(),
            self.side_outputs.clone(),
            self.forced_non_null_columns.clone(),
            self.event_time_instrumentation.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let metric_metadata_id = self.reference_name.clone();
        let metrics = self.metrics();
        let metrics_recorder = get_metrics_recorder().clone();
        let live_data_inspect = LiveDataInspect::get_instance();

        trace!(
            "Running WrappingExec.execute with inner_plan_node_name: {}, partition: {}, metric_metadata_id: {} ",
            self.inner.name(),
            partition,
            metric_metadata_id
        );

        let mut data = execute_input_stream(
            Arc::clone(&self.inner),
            Arc::clone(&self.schema()),
            partition,
            Arc::clone(&context),
        )?;

        let side_outputs = self.side_outputs.clone();
        let event_time_instrumentation = self.event_time_instrumentation.clone();

        let schema = self.schema();

        let measured_stream = async_stream::stream! {
            loop {
                let batch_start = Instant::now();
                let batch_result = data.next().await;
                let batch_elapsed = batch_start.elapsed();

                let batch_result = match batch_result {
                    Some(r) => r,
                    None => break,
                };

                match batch_result {
                    Ok(batch) => {
                        // Record per-batch compute time
                        metrics_recorder.record_elapsed_compute(batch_elapsed, &metric_metadata_id);

                        // Process telemetry
                        metrics_recorder.record_execution_plan_metrics(
                            metric_metadata_id.as_str(),
                            batch.num_rows(),
                            RowCountMeasurementType::OutputRowCount,
                            metrics.clone(),
                        );

                        // Record checkpoint marker arrival time for transforms
                        let checkpoint_messages = extract_checkpoint_messages(batch.schema().metadata());
                        for message in &checkpoint_messages {
                            if let CheckpointMessage::Marker { created_at_ms, .. } = message {
                                let arrival_latency_ms = now_ms().saturating_sub(*created_at_ms);
                                metrics_recorder.record_time(
                                    "checkpoint_marker_arrival",
                                    Duration::from_millis(arrival_latency_ms),
                                    metric_metadata_id.as_str(),
                                );
                            }
                        }

                        // Event-time freshness instrumentation. Only sources
                        // configured with `telemetry.event_time` participate;
                        // everywhere else the Option is None and this block
                        // collapses to a no-op.
                        if let Some(instrumentation) = event_time_instrumentation.as_ref()
                            && batch.num_rows() > 0
                        {
                            emit_event_time_metrics(
                                instrumentation,
                                &batch,
                                &metric_metadata_id,
                                metrics_recorder.as_ref(),
                            );
                        }

                        // Process side outputs
                        for side_output in &side_outputs {
                            if let Err(e) = side_output.process(&batch).await {
                                debug!("WrappingExec [{}]: Side output error, stream will terminate: {}", metric_metadata_id, e);
                                yield Err(DataFusionError::from(crate::streamling_err!(
                                    "side output error for '{}': {e}",
                                    metric_metadata_id
                                )));
                                return;
                            }
                        }

                        live_data_inspect.process(metric_metadata_id.as_str(), &batch).await;

                        yield Ok(batch);
                    }
                    Err(e) => {
                        debug!("WrappingExec [{}]: Error from input stream, stream will terminate: {}", metric_metadata_id, e);
                        yield Err(e);
                        return;
                    }
                }
            }
        };

        let wrapped: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, measured_stream));

        Ok(wrapped)
    }
}

impl DisplayAs for WrappingExec {
    delegate! {
        to self.inner {
            fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result;
        }
    }
}

//for debugging
impl Drop for WrappingExec {
    fn drop(&mut self) {
        let inner_plan_node_name = self.inner.name();
        trace!(
            "Running drop for WrappingExec with inner_plan_node_name: {}",
            inner_plan_node_name
        );
    }
}

/// WrappingDataSink wraps a DataSink and adds functionality like telemetry (similar to `WrappingExec`)
#[derive(Debug)]
pub struct WrappingDataSink {
    inner: Arc<dyn DataSink>,
    reference_name: String,
    primary_key: Option<String>,
    /// Per-sink event-time freshness instrumentation. `None` when the sink
    /// has no `telemetry.event_time` configured. When `Some`, emits the
    /// watermark gauge and lag histogram observed at sink ingress — the
    /// freshness metric at this point captures end-to-end pipeline lag
    /// (source processing + all transforms + batch-flush behavior ahead of
    /// this point in the plan).
    event_time_instrumentation: Option<EventTimeInstrumentation>,
}

impl WrappingDataSink {
    pub fn new(
        inner: Arc<dyn DataSink>,
        reference_name: String,
        primary_key: Option<String>,
        telemetry: Option<&Telemetry>,
    ) -> WrappingDataSink {
        Self {
            inner,
            reference_name,
            primary_key,
            event_time_instrumentation: EventTimeInstrumentation::from_telemetry(telemetry),
        }
    }
}

#[async_trait]
impl DataSink for WrappingDataSink {
    delegate! {
        to self.inner {
            fn metrics(&self) -> Option<MetricsSet>;
            fn schema(&self) -> &SchemaRef;
        }
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let metric_metadata_id = self.reference_name.clone();
        let metrics = self.metrics();
        let metrics_recorder = get_metrics_recorder().clone();
        let event_time_instrumentation = self.event_time_instrumentation.clone();

        let schema = data.schema();
        let primary_key = self.primary_key.clone();

        let measured_stream = async_stream::stream! {
            while let Some(batch_result) = data.next().await {
                match batch_result {
                    Ok(batch) => {
                        metrics_recorder.record_execution_plan_metrics(
                            metric_metadata_id.as_str(),
                            batch.num_rows(),
                            RowCountMeasurementType::InputRowCount,
                            metrics.clone(),
                        );

                        // End-to-end freshness at sink ingress. Same code
                        // path as the source-side emission — see `WrappingExec`.
                        if let Some(instrumentation) = event_time_instrumentation.as_ref()
                            && batch.num_rows() > 0
                        {
                            emit_event_time_metrics(
                                instrumentation,
                                &batch,
                                &metric_metadata_id,
                                metrics_recorder.as_ref(),
                            );
                        }

                        let deduped_batch = match if let Some(pk) = &primary_key {
                            deduplicate_record_batch(&batch, pk).map_err(|e| {
                                DataFusionError::from(crate::streamling_err!(
                                    "deduplication failed: {}",
                                    e
                                ))
                            })
                        } else {
                            Ok(batch)
                        } {
                            Ok(b) => b,
                            Err(e) => {
                                debug!("WrappingDataSink [{}]: Dedup error, stream will terminate: {}", metric_metadata_id, e);
                                yield Err(e);
                                return;
                            }
                        };

                        yield Ok(deduped_batch);
                    }
                    Err(e) => {
                        debug!("WrappingDataSink [{}]: Error from input stream, sink will terminate: {}", metric_metadata_id, e);
                        yield Err(e);
                        return;
                    }
                }
            }
        };

        let wrapped: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, measured_stream));

        let output_count = self.inner.write_all(wrapped, context).await?;
        Ok(output_count)
    }
}

impl DisplayAs for WrappingDataSink {
    delegate! {
        to self.inner {
            fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result;
        }
    }
}

// Unfortunately, many things can't be stored in a UserDefinedLogicalNode directly
lazy_static::lazy_static! {
    // Registry to store side outputs for WrappingNode instances
    static ref SIDE_OUTPUTS_REGISTRY: Mutex<HashMap<String, Vec<Arc<dyn SourceSideOutput>>>> =
        Mutex::new(HashMap::new());

    // Registry to store scan sharing configuration for transforms
    static ref SCAN_SHARING_REGISTRY: Mutex<HashMap<String, Arc<SharedSourceHandle>>> =
        Mutex::new(HashMap::new());
}

#[derive(PartialEq, Eq, PartialOrd, Hash, Clone)]
pub struct WrappingNode {
    pub input: LogicalPlan,
    pub reference_name: String,
    pub enable_scan_sharing: bool,
    pub forced_non_null_columns: Vec<String>,
    pub telemetry: Option<Telemetry>,
}

impl SupportsSideOutputs for WrappingNode {
    fn add_side_output(&self, side_output: Arc<dyn SourceSideOutput>) {
        let reference_name = self.reference_name.clone();
        if let Ok(mut registry) = SIDE_OUTPUTS_REGISTRY.lock() {
            registry
                .entry(reference_name)
                .or_insert_with(Vec::new)
                .push(side_output);
        } else {
            warn!("Failed to acquire lock for side outputs registry");
        }
    }
}

impl WrappingNode {
    pub fn new(
        input: LogicalPlan,
        reference_name: String,
        enable_scan_sharing: bool,
        telemetry: Option<Telemetry>,
    ) -> Self {
        Self {
            input,
            reference_name,
            enable_scan_sharing,
            forced_non_null_columns: Vec::new(),
            telemetry,
        }
    }

    pub fn new_with_non_null_cols(
        input: LogicalPlan,
        reference_name: String,
        enable_scan_sharing: bool,
        forced_non_null_columns: Vec<String>,
        telemetry: Option<Telemetry>,
    ) -> Self {
        Self {
            input,
            reference_name,
            enable_scan_sharing,
            forced_non_null_columns,
            telemetry,
        }
    }

    fn get_side_outputs(&self) -> Vec<Arc<dyn SourceSideOutput>> {
        if let Ok(registry) = SIDE_OUTPUTS_REGISTRY.lock() {
            registry
                .get(&self.reference_name.clone())
                .cloned()
                .unwrap_or_default()
        } else {
            warn!("Failed to acquire lock for side outputs registry");
            Vec::new()
        }
    }
}

impl Debug for WrappingNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for WrappingNode {
    fn name(&self) -> &str {
        "WrappingNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Wrapper id: {}", self.reference_name)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");

        Ok(Self {
            input: inputs.swap_remove(0),
            reference_name: self.reference_name.clone(),
            enable_scan_sharing: self.enable_scan_sharing,
            forced_non_null_columns: self.forced_non_null_columns.clone(),
            telemetry: self.telemetry.clone(),
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

pub struct WrappingExtensionPlanner {}

#[async_trait]
impl ExtensionPlanner for WrappingExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(wrapping_node) = node.as_any().downcast_ref::<WrappingNode>() {
                let reference_name = wrapping_node.reference_name.clone();

                // Check if this transform should use scan sharing (handle already exists)
                {
                    let registry = SCAN_SHARING_REGISTRY.lock().unwrap();
                    if let Some(handle) = registry.get(&reference_name) {
                        debug!(
                            "Transform '{}' using scan sharing (reusing existing handle)",
                            reference_name
                        );
                        return Ok(Some(Arc::new(BroadcastingExec::new(handle.clone()))));
                    }
                }

                // Create the physical plan
                let input_physical = planner
                    .create_physical_plan(&wrapping_node.input, session_state)
                    .await?;

                let side_outputs = wrapping_node.get_side_outputs();

                // Validate that all forced non-null columns exist in the physical schema
                if !wrapping_node.forced_non_null_columns.is_empty() {
                    let physical_schema = input_physical.schema();
                    let field_names: HashSet<&str> = physical_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect();
                    for col in &wrapping_node.forced_non_null_columns {
                        if !field_names.contains(col.as_str()) {
                            return Err(crate::streamling_user_err!(
                                "primary key column '{}' not found in output schema for '{}'",
                                col,
                                reference_name
                            )
                            .into());
                        }
                    }
                }

                let exec = Arc::new(WrappingExec::new(
                    input_physical,
                    reference_name.clone(),
                    side_outputs,
                    wrapping_node.forced_non_null_columns.clone(),
                    EventTimeInstrumentation::from_telemetry(wrapping_node.telemetry.as_ref()),
                ));

                // Check if we should store this in the scan sharing registry for future reuse
                if wrapping_node.enable_scan_sharing {
                    let mut registry = SCAN_SHARING_REGISTRY.lock().unwrap();

                    // Double-check in case another thread created it first
                    return if let Some(existing_handle) = registry.get(&reference_name) {
                        debug!(
                            "Transform '{}' - another thread created the handle first",
                            reference_name
                        );
                        Ok(Some(Arc::new(BroadcastingExec::new(
                            existing_handle.clone(),
                        ))))
                    } else {
                        // Create and store the handle for this transform
                        let schema = exec.schema();
                        let internal_buffer_size =
                            get_streamling_config(session_state)?.internal_buffer_size;
                        let expected_count =
                            SharedSourceRegistry::get_expected_consumers(&reference_name)
                                .unwrap_or(0);
                        let handle = Arc::new(SharedSourceHandle::new(
                            schema,
                            exec.clone(),
                            internal_buffer_size,
                            expected_count,
                        ));
                        registry.insert(reference_name.clone(), handle.clone());

                        debug!(
                            "Transform '{}' has {} consumers - created shared handle",
                            reference_name, expected_count
                        );
                        Ok(Some(Arc::new(BroadcastingExec::new(handle))))
                    };
                }

                // No scan sharing needed - return the exec directly
                Some(exec)
            } else {
                None
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::filter::StreamingFilterExec;
    use arrow::array::{BooleanArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::prelude::SessionContext;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct CountingSideOutput {
        rows_seen: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SourceSideOutput for CountingSideOutput {
        async fn process(&self, batch: &RecordBatch) -> Result<()> {
            self.rows_seen
                .fetch_add(batch.num_rows(), Ordering::Relaxed);
            Ok(())
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::Int64, false),
            Field::new("block_timestamp", DataType::Int64, false),
            Field::new("active", DataType::Boolean, false),
        ]))
    }

    fn test_batch() -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![1000, 2000, 3000, 4000, 5000])),
                Arc::new(BooleanArray::from(vec![true, false, true, false, true])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_wrap_with_side_outputs_before_filter_reorders_plan() {
        let schema = test_schema();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![test_batch()]]).unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();

        let source_exec =
            futures::executor::block_on(mem_table.scan(&state, None, &[], None)).unwrap();

        let predicate: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new("active", 2));
        let filter = FilterExec::try_new(predicate, source_exec.clone()).unwrap();
        let streaming_filter =
            Arc::new(StreamingFilterExec::from_original(filter).unwrap()) as Arc<dyn ExecutionPlan>;

        let rows_seen = Arc::new(AtomicUsize::new(0));
        let side_output = Arc::new(CountingSideOutput {
            rows_seen: rows_seen.clone(),
        });

        let result = WrappingSourceTableProvider::wrap_with_side_outputs_before_filter(
            streaming_filter,
            "test_source",
            vec![side_output],
            None,
        );

        // The top-level plan should be a StreamingFilterExec (filter is on top)
        assert!(
            result
                .as_any()
                .downcast_ref::<StreamingFilterExec>()
                .is_some(),
            "Expected top-level plan to be StreamingFilterExec, got: {}",
            result.name()
        );

        // Its child should be WrappingExec (side outputs run before the filter)
        let filter_children = result.children();
        assert_eq!(filter_children.len(), 1);
        assert!(
            filter_children[0]
                .as_any()
                .downcast_ref::<WrappingExec>()
                .is_some(),
            "Expected filter's child to be WrappingExec, got: {}",
            filter_children[0].name()
        );
    }

    #[test]
    fn test_wrap_without_filter_wraps_directly() {
        let schema = test_schema();
        let mem_table = MemTable::try_new(schema, vec![vec![test_batch()]]).unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();

        let source_exec =
            futures::executor::block_on(mem_table.scan(&state, None, &[], None)).unwrap();

        let rows_seen = Arc::new(AtomicUsize::new(0));
        let side_output = Arc::new(CountingSideOutput {
            rows_seen: rows_seen.clone(),
        });

        let result = WrappingSourceTableProvider::wrap_with_side_outputs_before_filter(
            source_exec,
            "test_source",
            vec![side_output],
            None,
        );

        // Without a filter, the top-level plan should be WrappingExec directly
        assert!(
            result.as_any().downcast_ref::<WrappingExec>().is_some(),
            "Expected top-level plan to be WrappingExec, got: {}",
            result.name()
        );
    }

    #[tokio::test]
    async fn test_side_output_sees_all_rows_before_filter() {
        let schema = test_schema();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![test_batch()]]).unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();

        let source_exec = mem_table.scan(&state, None, &[], None).await.unwrap();

        let predicate: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new("active", 2));
        let filter = FilterExec::try_new(predicate, source_exec.clone()).unwrap();
        let streaming_filter =
            Arc::new(StreamingFilterExec::from_original(filter).unwrap()) as Arc<dyn ExecutionPlan>;

        let rows_seen = Arc::new(AtomicUsize::new(0));
        let side_output = Arc::new(CountingSideOutput {
            rows_seen: rows_seen.clone(),
        });

        let plan = WrappingSourceTableProvider::wrap_with_side_outputs_before_filter(
            streaming_filter,
            "test_source",
            vec![side_output],
            None,
        );

        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = plan.execute(0, task_ctx).unwrap();
        let mut filtered_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            filtered_rows += batch.num_rows();
        }

        // The filter keeps rows where active=true (rows 0, 2, 4) → 3 rows downstream
        assert_eq!(filtered_rows, 3, "Downstream should see 3 filtered rows");

        // Side output should have seen all 5 rows (before filter)
        assert_eq!(
            rows_seen.load(Ordering::Relaxed),
            5,
            "Side output should see all 5 rows before filtering"
        );
    }

    // ------------------------------------------------------------------
    // Event-time instrumentation propagation tests (Unit 4)
    // ------------------------------------------------------------------

    use crate::topology::{EventTimeConfig, EventTimeUnit, Telemetry};

    fn telemetry_event_time_seconds(column: &str) -> Telemetry {
        Telemetry {
            event_time: Some(EventTimeConfig {
                column: column.to_string(),
                unit: Some(EventTimeUnit::Seconds),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn from_telemetry_returns_some_when_event_time_set() {
        let tel = telemetry_event_time_seconds("block_timestamp");
        let inst = EventTimeInstrumentation::from_telemetry(Some(&tel))
            .expect("instrumentation should be built when event_time is set");
        assert_eq!(inst.reader.column_name(), "block_timestamp");
        // Watermark starts in its initial unobserved state.
        assert!(inst.watermark_state.lock().unwrap().is_none());
    }

    #[test]
    fn from_telemetry_returns_none_when_absent() {
        assert!(EventTimeInstrumentation::from_telemetry(None).is_none());
    }

    #[test]
    fn from_telemetry_returns_none_when_event_time_absent() {
        // Telemetry present but no event_time nested under it — no
        // instrumentation should be built.
        let tel = Telemetry::default();
        assert!(EventTimeInstrumentation::from_telemetry(Some(&tel)).is_none());
    }

    #[test]
    fn wrapping_node_carries_per_instance_telemetry() {
        // Telemetry lives on `WrappingNode` itself (no global registry).
        // Two nodes sharing a `reference_name` must still carry distinct
        // telemetry configs — the property that a process-global registry
        // keyed by `reference_name` could not provide.
        let ts_a = telemetry_event_time_seconds("block_timestamp");
        let ts_b = telemetry_event_time_seconds("observed_at");

        let schema = test_schema();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![test_batch()]]).unwrap();
        let ctx = SessionContext::new();
        let scan = futures::executor::block_on(mem_table.scan(&ctx.state(), None, &[], None))
            .expect("scan should succeed");
        let dummy_input = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                datafusion::common::DFSchema::try_from(scan.schema().as_ref().clone()).unwrap(),
            ),
        });

        let node_a = WrappingNode::new(
            dummy_input.clone(),
            "shared_ref".to_string(),
            false,
            Some(ts_a),
        );
        let node_b = WrappingNode::new(dummy_input, "shared_ref".to_string(), false, Some(ts_b));

        assert_eq!(
            node_a
                .telemetry
                .as_ref()
                .and_then(Telemetry::event_time)
                .map(|e| e.column.as_str()),
            Some("block_timestamp"),
        );
        assert_eq!(
            node_b
                .telemetry
                .as_ref()
                .and_then(Telemetry::event_time)
                .map(|e| e.column.as_str()),
            Some("observed_at"),
        );

        // `with_exprs_and_inputs` is DataFusion's node-rebuild hook. Dropping
        // the telemetry.clone() line there would silently lose the config
        // every time the optimizer reconstructs the node.
        let rebuilt = UserDefinedLogicalNodeCore::with_exprs_and_inputs(
            &node_a,
            vec![],
            vec![node_a.input.clone()],
        )
        .expect("rebuild should succeed");
        assert_eq!(
            rebuilt
                .telemetry
                .as_ref()
                .and_then(Telemetry::event_time)
                .map(|e| e.column.as_str()),
            Some("block_timestamp"),
            "telemetry must round-trip through with_exprs_and_inputs",
        );
    }

    #[test]
    fn with_new_children_propagates_event_time_state() {
        // Construct a WrappingExec with instrumentation, call with_new_children,
        // assert the resulting exec carries the same Arc<Mutex> for watermark
        // state (so optimizer reconstructions cannot start a fresh watermark).
        let schema = test_schema();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![test_batch()]]).unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();
        let source_exec =
            futures::executor::block_on(mem_table.scan(&state, None, &[], None)).unwrap();

        let inst = EventTimeInstrumentation::from_telemetry(Some(&telemetry_event_time_seconds(
            "block_timestamp",
        )))
        .unwrap();
        let original_watermark_arc = inst.watermark_state.clone();

        let original = Arc::new(WrappingExec::new(
            source_exec.clone(),
            "test_source".to_string(),
            Vec::new(),
            Vec::new(),
            Some(inst),
        ));

        let rebuilt = original.with_new_children(vec![source_exec]).unwrap();
        let rebuilt_wrapping = rebuilt
            .as_any()
            .downcast_ref::<WrappingExec>()
            .expect("with_new_children must return a WrappingExec");
        let rebuilt_inst = rebuilt_wrapping
            .event_time_instrumentation
            .as_ref()
            .expect("instrumentation must be propagated");

        assert!(
            Arc::ptr_eq(&rebuilt_inst.watermark_state, &original_watermark_arc),
            "watermark state Arc must be shared across reconstructions"
        );
    }

    #[test]
    fn wrap_with_side_outputs_propagates_event_time_through_filter() {
        // When a StreamingFilterExec is in play, the helper places a
        // WrappingExec under the filter and re-applies the filter on top.
        // Verify the inserted WrappingExec carries the instrumentation.
        let schema = test_schema();
        let mem_table = MemTable::try_new(schema.clone(), vec![vec![test_batch()]]).unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();
        let source_exec =
            futures::executor::block_on(mem_table.scan(&state, None, &[], None)).unwrap();

        let predicate: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new("active", 2));
        let filter = FilterExec::try_new(predicate, source_exec.clone()).unwrap();
        let streaming_filter =
            Arc::new(StreamingFilterExec::from_original(filter).unwrap()) as Arc<dyn ExecutionPlan>;

        let inst = EventTimeInstrumentation::from_telemetry(Some(&telemetry_event_time_seconds(
            "block_timestamp",
        )))
        .unwrap();
        let watermark_arc = inst.watermark_state.clone();

        let result = WrappingSourceTableProvider::wrap_with_side_outputs_before_filter(
            streaming_filter,
            "test_source",
            Vec::new(),
            Some(inst),
        );

        // result is StreamingFilterExec → child is WrappingExec with our instrumentation.
        let filter_children = result.children();
        let wrapping = filter_children[0]
            .as_any()
            .downcast_ref::<WrappingExec>()
            .expect("expected WrappingExec under filter");
        let inst = wrapping
            .event_time_instrumentation
            .as_ref()
            .expect("instrumentation must be carried through wrap helper");
        assert!(Arc::ptr_eq(&inst.watermark_state, &watermark_arc));
    }

    #[test]
    fn emit_event_time_metrics_no_op_on_empty_batch_path() {
        // emit_event_time_metrics is guarded behind `batch.num_rows() > 0` in
        // execute(); calling it with an empty values path (zero-row source)
        // must not panic. Exercise the no-rows guard by constructing an
        // empty batch and verifying the reader returns an empty vec rather
        // than failing.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_timestamp",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new()))])
                .unwrap();
        let inst = EventTimeInstrumentation::from_telemetry(Some(&telemetry_event_time_seconds(
            "block_timestamp",
        )))
        .unwrap();
        // Reader on its own returns Ok(empty vec); the execute() guard
        // prevents emit_event_time_metrics from even being called for empty
        // batches, but the underlying reader must also be safe.
        assert_eq!(inst.reader.read_batch(&batch).unwrap().len(), 0);
    }

    #[test]
    fn emit_event_time_metrics_misconfigured_column_gates_warning_after_first_batch() {
        // Misconfigured column name → `reader.read_batch` returns
        // `ColumnMissing`, which flips `read_error_logged` to `true` on the
        // first call so subsequent batches stay silent instead of burning a
        // warn-log per batch.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "some_other_column",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        let inst = EventTimeInstrumentation::from_telemetry(Some(&telemetry_event_time_seconds(
            "missing_column",
        )))
        .unwrap();
        assert!(!inst.read_error_logged.load(Ordering::Relaxed));

        // `get_metrics_recorder()` returns a no-op recorder fallback when
        // the global instance isn't initialized — adequate for exercising
        // the error-path gate. We don't assert on recorded metrics here,
        // only on the log-once boolean.
        let recorder = get_metrics_recorder();
        emit_event_time_metrics(&inst, &batch, "test_source", recorder.as_ref());
        assert!(
            inst.read_error_logged.load(Ordering::Relaxed),
            "first misconfigured batch must flip the log-once gate"
        );

        // A second call must be a no-op on the gate — the boolean stays
        // `true` and the warn-log path is skipped.
        emit_event_time_metrics(&inst, &batch, "test_source", recorder.as_ref());
        assert!(inst.read_error_logged.load(Ordering::Relaxed));
    }
}
