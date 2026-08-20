use crate::telemetry::types::{
    MetricData, RowCountMeasurementType, create_count_with_value, create_gauge_with_value,
    create_time_from_duration,
};
use crate::telemetry::{PipelineMetricMetadata, TopologyNodeType, get_global_metric_tags};
use datafusion::physical_plan::metrics::{Count, MetricValue, MetricsSet};
use once_cell::sync::Lazy;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::{KeyValue, global};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;
use tracing::{debug, trace, warn};

const SERVICE_NAME_PREFIX: &str = "streamling";

pub fn add_service_prefix(metric_name: &str) -> String {
    format!("{}_{}", SERVICE_NAME_PREFIX, metric_name)
}
static METRICS_RECORDER_INSTANCE: Lazy<Mutex<Option<Arc<MetricsRecorder>>>> =
    Lazy::new(|| Mutex::new(None));

static METRIC_DENY_PATTERNS: Lazy<Arc<RwLock<Vec<Regex>>>> =
    Lazy::new(|| Arc::new(RwLock::new(Vec::new())));

pub fn set_metric_deny_patterns(patterns: Vec<String>) {
    let mut compiled_patterns = Vec::new();

    for pattern in patterns {
        match Regex::new(&pattern) {
            Ok(regex) => compiled_patterns.push(regex),
            Err(e) => {
                warn!(
                    "Invalid regex pattern '{}' in metric deny list: {}. Pattern will be ignored.",
                    pattern, e
                );
            }
        }
    }

    let mut deny_patterns = METRIC_DENY_PATTERNS.write().unwrap();
    trace!("deny patterns: {:?}", deny_patterns);
    *deny_patterns = compiled_patterns;
}

fn is_metric_denied(metric_name: &str) -> bool {
    let deny_patterns = METRIC_DENY_PATTERNS.read().unwrap();
    deny_patterns.iter().any(|pattern| {
        let is_match = pattern.is_match(metric_name);
        trace!(
            "matching regex: {} on metric_name: {}, is_match: {}",
            pattern, metric_name, is_match
        );
        is_match
    })
}

/// The operator_type value identifying SQL transform nodes. Gates BOTH the
/// wall-clock suppression (via `is_sql_node`) and the subtree-delta export in
/// `record_execution_plan_metrics`; the two must never drift apart.
const SQL_OPERATOR_TYPE: &str = "sql";

/// Prefix for metric names sourced from a SQL transform's DataFusion subtree
/// (`subtree_delta_metric_values`). Namespacing them keeps operator-internal
/// metrics (a join's `build_time`, `StreamingUnnestExec`'s
/// `Count { name: "input_rows" }`, whatever names a DataFusion upgrade adds)
/// from ever colliding with streamling's own semantic series (`input_rows`,
/// `output_rows`, ...) — which are billing-relevant and populated by
/// `TelemetryStream` / the recorder itself.
const SUBTREE_METRIC_PREFIX: &str = "df_";

/// Millisecond bucket layout shared by every duration histogram (the
/// hand-registered `elapsed_compute` and any auto-created `Time` metric), so
/// all duration series bucket identically up to one hour.
const DURATION_MS_BOUNDARIES: [f64; 14] = [
    100.0,
    250.0,
    500.0,
    1_000.0,
    2_500.0,
    5_000.0,
    10_000.0,
    30_000.0,
    60_000.0,
    120_000.0,
    300_000.0,
    600_000.0,
    1_800_000.0,
    3_600_000.0,
];

/// Gauges represent absolute state (e.g. "lag = 0"), so zero is meaningful
/// and must be recorded. Additive metrics (counters) adding zero are no-ops.
fn should_skip_zero_value_metric(metric_value: &MetricValue) -> bool {
    metric_value.as_usize() == 0 && !matches!(metric_value, MetricValue::Gauge { .. })
}

/// Pre-bound histogram handle for hot-path emission.
///
/// Returned from [`MetricsRecorder::resolve_histogram`]. Holds a clone of the
/// pre-registered OTEL histogram together with the resolved attribute set
/// (per-source tags + global tags merged once at resolve time). Recording a
/// value goes straight to the OTEL histogram's internal aggregator and does
/// not touch any of the streamling-core `Mutex`es.
///
/// Designed for per-row event-time lag emission. Non-hot-path callers should
/// continue to use [`MetricsRecorder::record_time_w_tags`].
pub struct BoundHistogram {
    histogram: Histogram<u64>,
    attrs: Vec<KeyValue>,
}

impl BoundHistogram {
    /// Record a single observation. O(1) per call on the streamling-core side;
    /// any aggregation cost is internal to the OTEL histogram implementation.
    pub fn record(&self, value: u64) {
        self.histogram.record(value, &self.attrs);
    }
}

#[derive(Default)]
pub struct MetricsRecorder {
    service_instance_id: String,
    metric_metadata_registry: Mutex<HashMap<String, PipelineMetricMetadata>>,
    metric_metadata_tags_registry: Mutex<HashMap<String, HashMap<String, String>>>,
    count_registry: Mutex<HashMap<String, Counter<u64>>>,
    gauge_registry: Mutex<HashMap<String, Gauge<u64>>>,
    histogram_registry: Mutex<HashMap<String, Histogram<u64>>>,
    /// Per-node running state for turning DataFusion's *cumulative* metrics into
    /// per-batch deltas. Keyed by `metadata_id`, then by metric name (a SQL
    /// transform's subtree exposes many cumulative metrics — `elapsed_compute`,
    /// a join's `build_time` / `join_time`, operator-defined counters — and each
    /// must be deltaed independently). See `record_execution_plan_metrics`.
    ///
    /// Entries are never evicted: they live for the process lifetime, keyed by
    /// node id, matching `metric_metadata_registry`. Bound is distinct SQL
    /// node ids ever seen in this process (pipeline lifetime / redeploys),
    /// not currently-active nodes. Evict only if registry retirement is added.
    /// A Prometheus size-gauge on this map is out of scope: the bound is the
    /// same as the metadata registry, not a distinct leak to instrument.
    metric_accrual: Mutex<HashMap<String, NodeMetricAccrual>>,
    /// Node ids whose `elapsed_compute` series has already been seeded; makes
    /// [`MetricsRecorder::seed_elapsed_compute_series`] idempotent so a
    /// re-registered pipeline doesn't accumulate phantom 1ms samples.
    /// Same process-lifetime retention as `metric_accrual` (no per-pipeline
    /// teardown hook exists to prune it). A size gauge is out of scope for
    /// the same reason as `metric_accrual`.
    seeded_elapsed_compute: Mutex<HashSet<String>>,
}

/// Per-node running state for converting cumulative DataFusion metrics into
/// per-batch deltas. Counters and time metrics live in separate maps so a
/// `Count` and a `Time` sharing a name can never clobber each other's
/// last-seen state (which would trip the reset heuristic every batch).
#[derive(Default)]
struct NodeMetricAccrual {
    /// metric name -> last-seen cumulative counter value.
    counts: HashMap<String, u64>,
    /// metric name -> cumulative time accrual state.
    times: HashMap<String, TimeAccrual>,
}

/// Accrual state for one cumulative time metric. Whole milliseconds are
/// emitted as the growth of `accrued / 1ms` across a call, so the
/// sub-millisecond remainder is carried implicitly and high-throughput sub-ms
/// compute isn't truncated to zero.
#[derive(Default)]
struct TimeAccrual {
    /// Last-seen cumulative nanos (for reset detection).
    last: u64,
    /// Total accrued nanos across stream resets.
    accrued: u64,
}

/// Delta since the last-seen cumulative value. A re-executed stream resets
/// DataFusion's cumulative counters; a value below the last-seen total is
/// treated as a fresh delta rather than stalling at zero until it climbs back
/// past the previous run.
fn cumulative_delta(last: &mut u64, cumulative: u64) -> u64 {
    let delta = if cumulative >= *last {
        cumulative - *last
    } else {
        cumulative
    };
    *last = cumulative;
    delta
}

/// Per-batch delta (in whole milliseconds) for one cumulative time metric.
/// Emitting `accrued / 1ms - emitted_ms` carries the sub-millisecond remainder
/// implicitly, and [`cumulative_delta`] supplies the shared reset handling.
fn time_delta_millis(
    times: &mut HashMap<String, TimeAccrual>,
    name: &str,
    cumulative_nanos: u64,
) -> u64 {
    let acc = times.entry(name.to_string()).or_default();
    let emitted_before = acc.accrued / 1_000_000;
    acc.accrued = acc
        .accrued
        .saturating_add(cumulative_delta(&mut acc.last, cumulative_nanos));
    acc.accrued / 1_000_000 - emitted_before
}

impl MetricsRecorder {
    /// Seed each non-sink node's `elapsed_compute` series with a single 1ms
    /// sample so the series exists (and dashboards can find it) even for
    /// nodes that stall before their first batch or never accrue a whole
    /// millisecond of compute. Idempotent per node id, so re-registering a
    /// pipeline does not accumulate phantom samples per redeploy.
    ///
    /// Call AFTER plugin construction: plugin-declared identity labels are
    /// merged at that point (`merge_metadata_tags`), and seeding earlier
    /// would emit the sample on a pre-merge tag set — an orphan series that
    /// dashboards filtering on those labels would never match.
    ///
    /// Consumers must treat `elapsed_compute <= 1ms` as "no real compute
    /// recorded": the seed is indistinguishable from a node that truly did
    /// about 1ms of work (or stalled after seeding).
    pub fn seed_elapsed_compute_series(&self) {
        // The no-op fallback recorder has an empty histogram registry and the
        // ElapsedCompute record arm would panic on the missing instrument.
        if !self
            .histogram_registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key("elapsed_compute")
        {
            debug!("Skipping elapsed_compute seeding: histogram not registered (no-op recorder)");
            return;
        }
        let ids: Vec<String> = {
            let registry = self
                .metric_metadata_registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let seeded = self
                .seeded_elapsed_compute
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            registry
                .iter()
                .filter(|(id, meta)| {
                    meta.node_context.node_type != TopologyNodeType::Sink && !seeded.contains(*id)
                })
                .map(|(id, _)| id.clone())
                .collect()
        };
        for id in ids {
            self.record_elapsed_compute(Duration::from_millis(1), &id);
            self.seeded_elapsed_compute
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(id);
        }
    }

    pub fn record_count(&self, name: &str, value: u64, metadata_id: &str) {
        self.record_count_w_tags(name, value, vec![], metadata_id);
    }

    pub fn record_gauge(&self, name: &str, value: u64, metadata_id: &str) {
        self.record_gauge_w_tags(name, value, vec![], metadata_id);
    }

    pub fn record_time(&self, name: &str, duration: Duration, metadata_id: &str) {
        self.record_time_w_tags(name, duration, vec![], metadata_id);
    }

    pub fn record_output_rows_count(&self, value: u64, metadata_id: &str) {
        let tags_opt = self
            .metric_metadata_tags_registry
            .lock()
            .unwrap()
            .get(metadata_id)
            .cloned();
        if let Some(metric_metadata_tags) = tags_opt {
            // Record cumulative metric (existing behavior)
            let metric_value = MetricValue::OutputRows(create_count_with_value(value as usize));
            let data =
                MetricData::new_with_owned_tags(vec![metric_value], metric_metadata_tags.clone());
            self.record_metric_data(data);

            // Also record delta metric
            trace!(
                "Recording output_rows_delta: {} rows for metadata_id: {}",
                value, metadata_id
            );
            let delta_metric_value = MetricValue::Count {
                name: "output_rows_delta".into(),
                count: create_count_with_value(value as usize),
            };
            let delta_data =
                MetricData::new_with_owned_tags(vec![delta_metric_value], metric_metadata_tags);
            self.record_metric_data(delta_data);
        }
    }

    pub fn record_elapsed_compute(&self, duration: Duration, metadata_id: &str) {
        let tags_opt = self
            .metric_metadata_tags_registry
            .lock()
            .unwrap()
            .get(metadata_id)
            .cloned();
        if let Some(metric_metadata_tags) = tags_opt {
            let metric_value = MetricValue::ElapsedCompute(create_time_from_duration(duration));
            let data = MetricData::new_with_owned_tags(vec![metric_value], metric_metadata_tags);
            self.record_metric_data(data)
        }
    }

    /// Whether `metadata_id` is registered as a SQL transform (`operator_type == "sql"`).
    ///
    /// Takes the metadata-registry lock. The result is static for a node's
    /// lifetime, so per-batch paths must resolve it **once per stream** (see
    /// `WrappingExec::execute`) rather than on every batch. Used to suppress
    /// wall-clock `elapsed_compute` when the DataFusion subtree already
    /// supplies per-batch deltas.
    pub fn is_sql_node(&self, metadata_id: &str) -> bool {
        self.metric_metadata_registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(metadata_id)
            .is_some_and(|m| m.node_context.operator_type == SQL_OPERATOR_TYPE)
    }
    pub fn record_count_w_tags(
        &self,
        name: &str,
        value: u64,
        tags: Vec<(&str, &str)>,
        metadata_id: &str,
    ) {
        let metric_metadata_tags = self
            .metric_metadata_tags_registry
            .lock()
            .unwrap()
            .get(metadata_id)
            .cloned();
        if metric_metadata_tags.is_none() {
            warn!(
                "record_count_w_tags: metadata_id '{}' not found in registry, skipping metric '{}'",
                metadata_id, name
            );
            return;
        }
        let metric_metadata_tags = metric_metadata_tags.unwrap();
        let metric_value = MetricValue::Count {
            name: String::from(name).into(),
            count: create_count_with_value(value as usize),
        };
        let mut all_tags = metric_metadata_tags.clone();
        for (k, v) in tags {
            all_tags.insert(k.to_string(), v.to_string());
        }
        self.record_metric_data(MetricData::new_with_owned_tags(
            vec![metric_value],
            all_tags,
        ));
    }

    pub fn record_gauge_w_tags(
        &self,
        name: &str,
        value: u64,
        tags: Vec<(&str, &str)>,
        metadata_id: &str,
    ) {
        let metric_metadata_tags = self
            .metric_metadata_tags_registry
            .lock()
            .unwrap()
            .get(metadata_id)
            .cloned();
        if metric_metadata_tags.is_none() {
            warn!(
                "record_gauge_w_tags: metadata_id '{}' not found in registry, skipping metric '{}'",
                metadata_id, name
            );
            return;
        }
        let metric_metadata_tags = metric_metadata_tags.unwrap();
        let metric_value = MetricValue::Gauge {
            name: String::from(name).into(),
            gauge: create_gauge_with_value(value as usize),
        };
        let mut all_tags = metric_metadata_tags.clone();
        for (k, v) in tags {
            all_tags.insert(k.to_string(), v.to_string());
        }
        self.record_metric_data(MetricData::new_with_owned_tags(
            vec![metric_value],
            all_tags,
        ));
    }

    pub fn record_time_w_tags(
        &self,
        name: &str,
        duration: Duration,
        tags: Vec<(&str, &str)>,
        metadata_id: &str,
    ) {
        if let Some(metric_metadata_tags) = self
            .metric_metadata_tags_registry
            .lock()
            .unwrap()
            .get(metadata_id)
            .cloned()
        {
            let metric_value = MetricValue::Time {
                name: String::from(name).into(),
                time: create_time_from_duration(duration),
            };
            let mut all_tags = metric_metadata_tags.clone();
            for (k, v) in tags {
                all_tags.insert(k.to_string(), v.to_string());
            }
            self.record_metric_data(MetricData::new_with_owned_tags(
                vec![metric_value],
                all_tags,
            ));
        } else {
            warn!(
                "record_time_w_tags: metadata_id '{}' not found in registry, skipping metric '{}'",
                metadata_id, name
            );
        }
    }

    /// Resolve a [`BoundHistogram`] handle for hot-path emission.
    ///
    /// Performs the histogram-registry lookup, per-source tag lookup, and
    /// global-tag merge once — then returns a handle that records straight
    /// into the OTEL histogram with no further `MetricsRecorder` lock
    /// acquisitions. Use this for per-row emission inside tight loops; the
    /// non-hot-path APIs (`record_time_w_tags` etc.) re-resolve everything on
    /// every call.
    ///
    /// Returns `None` when:
    ///   - the histogram has not been pre-registered (caller forgot to add
    ///     it to `initialize_metrics_recorder`), or
    ///   - the `metadata_id` is not present in the per-source tag registry.
    pub fn resolve_histogram(&self, name: &str, metadata_id: &str) -> Option<BoundHistogram> {
        let histogram = self
            .histogram_registry
            .lock()
            .expect("histogram_registry mutex poisoned")
            .get(name)
            .cloned()?;
        let metric_metadata_tags = self
            .metric_metadata_tags_registry
            .lock()
            .expect("metric_metadata_tags_registry mutex poisoned")
            .get(metadata_id)
            .cloned()?;
        // Mirror the tag-merging logic in `record_metric_data` so the bound
        // handle carries the same effective attribute set as the standard
        // emission path: per-source tags first, then any global tags that
        // weren't already provided.
        let svc_id = metric_metadata_tags
            .get("service_instance_id")
            .cloned()
            .unwrap_or_else(|| self.service_instance_id.clone());
        let mut attrs: Vec<KeyValue> = metric_metadata_tags
            .iter()
            .map(|(k, v)| KeyValue::new::<String, String>(k.into(), v.into()))
            .collect();
        let mut existing: HashSet<String> = attrs.iter().map(|kv| kv.key.clone().into()).collect();
        for tag in get_global_metric_tags(&svc_id) {
            let key: String = tag.key.clone().into();
            if !existing.contains(&key) {
                existing.insert(key);
                attrs.push(tag);
            }
        }
        Some(BoundHistogram { histogram, attrs })
    }

    /// This method dispatches count recorded as part of `TelemetryExec` wrapping of `ExecutionPlan`s
    /// for sources, transforms the `batch_count` is recorded as `output_rows` metric
    /// for sinks the `batch_count` is recorded as `input_rows` metric since we wrap the input stream in `TelemetryDataSink`
    /// Datafusion's `ExecutionPlanMetricsSet` is also recorded but only for transforms
    /// for transforms, we also emit `batch_count` as children node's `input_row` count
    /// under the assumption that output for a transform node is input for it's downstream nodes
    pub fn record_execution_plan_metrics(
        &self,
        metadata_id: &str,
        batch_count: usize,
        measurement_type: RowCountMeasurementType,
        execution_plan_metrics: Option<&MetricsSet>,
    ) {
        let metric_metadata = self
            .metric_metadata_registry
            .lock()
            .unwrap()
            .get(metadata_id)
            .cloned();
        if metric_metadata.is_none() {
            return;
        }
        let metric_metadata_tags = self
            .metric_metadata_tags_registry
            .lock()
            .unwrap()
            .get(metadata_id)
            .cloned();
        if metric_metadata_tags.is_none() {
            return;
        }
        let metric_metadata = metric_metadata.unwrap();
        let metric_metadata_tags = metric_metadata_tags.unwrap();
        let mut all_metric_data: Vec<MetricData> = Vec::new();
        match measurement_type {
            RowCountMeasurementType::OutputRowCount => {
                let output_rows_count = Count::new();
                output_rows_count.add(batch_count);
                let output_rows_metric_value = MetricValue::OutputRows(output_rows_count.clone());
                all_metric_data.push(MetricData::new_with_owned_tags(
                    vec![output_rows_metric_value],
                    metric_metadata_tags.clone(),
                ));

                // Also emit delta variant
                trace!(
                    "Recording output_rows_delta from execution plan: {} rows for metadata_id: {}",
                    batch_count, metadata_id
                );
                let output_rows_delta_metric_value = MetricValue::Count {
                    name: "output_rows_delta".into(),
                    count: output_rows_count.clone(),
                };
                all_metric_data.push(MetricData::new_with_owned_tags(
                    vec![output_rows_delta_metric_value],
                    metric_metadata_tags.clone(),
                ));

                for child_metric_metadata_id in &metric_metadata.children_metadata_ids {
                    let child_metric_metadata = self
                        .metric_metadata_registry
                        .lock()
                        .unwrap()
                        .get(child_metric_metadata_id.as_str())
                        .cloned();
                    if child_metric_metadata.is_none() {
                        continue;
                    }
                    let child_metric_metadata = child_metric_metadata.unwrap();
                    if child_metric_metadata.node_context.node_type == TopologyNodeType::Sink {
                        continue;
                    };
                    if let Some(child_metric_metadata_tags) = self
                        .metric_metadata_tags_registry
                        .lock()
                        .unwrap()
                        .get(child_metric_metadata_id.as_str())
                        .cloned()
                    {
                        let metric_value = MetricValue::Count {
                            name: "input_rows".into(),
                            count: output_rows_count.clone(),
                        };
                        all_metric_data.push(MetricData::new_with_owned_tags(
                            vec![metric_value],
                            child_metric_metadata_tags,
                        ));
                    }
                }
            }
            RowCountMeasurementType::InputRowCount => {
                let input_rows_count = Count::new();
                input_rows_count.add(batch_count);
                let input_rows_metric_value = MetricValue::Count {
                    name: "input_rows".into(),
                    count: input_rows_count,
                };
                all_metric_data.push(MetricData::new_with_owned_tags(
                    vec![input_rows_metric_value],
                    metric_metadata_tags.clone(),
                ));
            }
        }
        // Also emit any ExecutionPlan metrics when present for sql transform only
        // as it is the only node that we reuse from DF; all others we should instrument
        // sources|sinks are implemented in this code base, so their metrics are emitted
        // either directly via metrics recorder or via TelemetryStream
        // todo: may need to add support for use cases like:
        // - Some sources can surface DF metrics once wrapped (e.g., scans with
        //   upstream projection/filter inside the plan):
        //     * ClickHouse source (DF-based scan)
        //     * Hybrid source per-phase child plans (bounded/unbounded)
        if let Some(metric_set) = execution_plan_metrics
            && metric_metadata.node_context.operator_type == SQL_OPERATOR_TYPE
        {
            // Every DataFusion metric here is the CUMULATIVE total of the
            // transform's whole physical subtree (aggregated by
            // `CheckpointableExec::metrics`), and this method runs once per
            // batch. Recording the raw snapshot every batch re-records an
            // ever-growing total, inflating every histogram/counter
            // super-linearly — the bug that originally only affected
            // `elapsed_compute` and then resurfaced for deeper operators'
            // metrics (e.g. a join's `build_time` / `join_time`) once the
            // subtree was folded in. Convert each to its per-batch DELTA,
            // batched into ONE MetricData so the tag set is cloned and the
            // attribute vector built once per batch, not once per metric.
            let delta_values = self.subtree_delta_metric_values(metadata_id, metric_set);
            if !delta_values.is_empty() {
                all_metric_data.push(MetricData::new_with_owned_tags(
                    delta_values,
                    metric_metadata_tags.clone(),
                ));
            }
        }
        self.record_metric_data_batch(all_metric_data);
    }

    /// Convert a SQL transform's cumulative subtree `MetricsSet` into per-batch
    /// DELTA metric values.
    ///
    /// DataFusion metrics are cumulative and this runs once per batch, so
    /// emitting raw snapshots would re-record an ever-growing total each batch
    /// and inflate every histogram/counter super-linearly. Metrics are first
    /// aggregated by name (a transform's work is spread across many operators,
    /// and multiple operators can expose the same metric name), then reduced to
    /// the delta since the previous batch so each cumulative series is deltaed
    /// exactly once.
    ///
    /// `OutputRows` is skipped (already counted via `TelemetryStream`). Counters
    /// and time metrics (`elapsed_compute`, `build_time`, `join_time`, …) are
    /// deltaed; gauges are absolute state and forwarded as-is. All generic
    /// subtree names are exported under [`SUBTREE_METRIC_PREFIX`] so
    /// operator-internal metrics can never collide with streamling's own
    /// semantic series.
    fn subtree_delta_metric_values(
        &self,
        metadata_id: &str,
        metric_set: &MetricsSet,
    ) -> Vec<MetricValue> {
        // Aggregate by name manually instead of via
        // `MetricsSet::aggregate_by_name`: that panics when two operators in
        // the subtree expose the same metric name under different
        // `MetricValue` variants (e.g. `BaselineMetrics`' typed
        // `OutputBatches` alongside an operator-defined
        // `Count { name: "output_batches" }`), which real plans do.
        let mut elapsed_compute_nanos: u64 = 0;
        let mut count_totals: HashMap<&str, u64> = HashMap::new();
        let mut time_totals: HashMap<&str, u64> = HashMap::new();
        let mut deltas = Vec::new();
        for metric in metric_set.iter() {
            match metric.value() {
                // Counted separately via TelemetryStream.
                MetricValue::OutputRows(_) => {}
                // Cumulative batch counter; not exported as telemetry
                // (matches pre-subtree-aggregation behavior, where it fell
                // into `record_metric_data`'s dropped-variants arm).
                MetricValue::OutputBatches(_) => {}
                MetricValue::ElapsedCompute(time) => {
                    elapsed_compute_nanos =
                        elapsed_compute_nanos.saturating_add(time.value() as u64);
                }
                MetricValue::Count { name, count } => {
                    *count_totals.entry(name.as_ref()).or_default() += count.value() as u64;
                }
                // A Time literally named "elapsed_compute" is the same
                // quantity as the typed variant; folding it in keeps one
                // accrual slot and one emitted series (a separate Time entry
                // would clobber the typed variant's last-seen state and trip
                // the reset heuristic every batch).
                MetricValue::Time { name, time } if name == "elapsed_compute" => {
                    elapsed_compute_nanos =
                        elapsed_compute_nanos.saturating_add(time.value() as u64);
                }
                MetricValue::Time { name, time } => {
                    *time_totals.entry(name.as_ref()).or_default() += time.value() as u64;
                }
                // Gauges are absolute state, not cumulative totals; the latest
                // snapshot is already correct, so forward it (under the
                // subtree prefix) without delta bookkeeping.
                MetricValue::Gauge { name, gauge } => deltas.push(MetricValue::Gauge {
                    name: format!("{SUBTREE_METRIC_PREFIX}{name}").into(),
                    gauge: gauge.clone(),
                }),
                // Remaining variants (spill counts, timestamps, custom) are
                // dropped by `record_metric_data`; don't clone them through
                // the per-batch path only to be discarded downstream.
                _ => {}
            }
        }

        // Numeric last-seen updates run under the single process-global
        // accrual guard; MetricValue construction (allocations) happens after
        // it drops so the critical section stays short.
        let mut elapsed_ms: u64 = 0;
        let mut count_deltas: Vec<(&str, u64)> = Vec::new();
        let mut time_deltas: Vec<(&str, u64)> = Vec::new();
        {
            let mut accruals = self
                .metric_accrual
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let node = accruals.entry(metadata_id.to_string()).or_default();

            // Only touch the accrual slot when the set actually carried compute:
            // an unconditional call would write `last = 0` for compute-less
            // snapshots, arming a full-cumulative re-emit if a compute-bearing
            // snapshot ever follows under the same id.
            if elapsed_compute_nanos > 0 {
                elapsed_ms =
                    time_delta_millis(&mut node.times, "elapsed_compute", elapsed_compute_nanos);
            }
            for (name, cumulative) in count_totals {
                let last = node.counts.entry(name.to_string()).or_insert(0);
                let delta = cumulative_delta(last, cumulative);
                if delta > 0 {
                    count_deltas.push((name, delta));
                }
            }
            for (name, cumulative_nanos) in time_totals {
                if cumulative_nanos == 0 {
                    continue;
                }
                let whole_ms = time_delta_millis(&mut node.times, name, cumulative_nanos);
                if whole_ms > 0 {
                    time_deltas.push((name, whole_ms));
                }
            }
        }

        if elapsed_ms > 0 {
            deltas.push(MetricValue::ElapsedCompute(create_time_from_duration(
                Duration::from_millis(elapsed_ms),
            )));
        }
        for (name, delta) in count_deltas {
            deltas.push(MetricValue::Count {
                name: format!("{SUBTREE_METRIC_PREFIX}{name}").into(),
                count: create_count_with_value(delta as usize),
            });
        }
        for (name, whole_ms) in time_deltas {
            deltas.push(MetricValue::Time {
                name: format!("{SUBTREE_METRIC_PREFIX}{name}").into(),
                time: create_time_from_duration(Duration::from_millis(whole_ms)),
            });
        }
        deltas
    }

    fn record_metric_data_batch(&self, all_metric_data: Vec<MetricData>) {
        for metric_data in all_metric_data {
            self.record_metric_data(metric_data);
        }
    }

    fn record_metric_data(&self, metric_data: MetricData) {
        self.update_registries(&metric_data.metric_values);
        let mut tags: Vec<_> = metric_data
            .tags
            .iter()
            .map(|(k, v)| KeyValue::new::<String, String>(k.into(), v.into()))
            .collect();
        // Determine service_instance_id from metric data if present; fallback to recorder's
        let svc_from_metric = metric_data.tags.get("service_instance_id").cloned();
        let svc_id = svc_from_metric.unwrap_or_else(|| self.service_instance_id.clone());
        // Merge in global tags for the chosen service instance, skipping duplicates
        let mut existing: HashSet<String> = tags.iter().map(|kv| kv.key.clone().into()).collect();
        let global_tags = get_global_metric_tags(&svc_id);
        for tag in global_tags {
            let key: String = tag.key.clone().into();
            if !existing.contains(&key) {
                existing.insert(key);
                tags.push(tag);
            }
        }
        let id = metric_data
            .tags
            .get("id")
            .map(|id| id.as_str())
            .unwrap_or("None");
        for metric_value in metric_data.metric_values {
            let metric_name = metric_value.name();
            trace!("id:{}, name: {}, value:{}", id, metric_name, metric_value,);
            if is_metric_denied(metric_name) {
                continue;
            }
            if should_skip_zero_value_metric(&metric_value) {
                continue;
            }
            match &metric_value {
                MetricValue::ElapsedCompute(ec) => {
                    let in_ms = ec.value() as u64 / 1_000_000;
                    if in_ms != 0 {
                        self.histogram_registry
                            .lock()
                            .unwrap()
                            .get(metric_name)
                            .expect("expect elapsed_compute histogram to be available")
                            .record(in_ms, &tags)
                    }
                }
                MetricValue::OutputRows(count) => {
                    self.count_registry
                        .lock()
                        .unwrap()
                        .get(metric_name)
                        .expect("expect output_rows counter to be available")
                        .add(count.value() as u64, &tags);
                }
                MetricValue::Count { name, count } => {
                    self.count_registry
                        .lock()
                        .unwrap()
                        .get(name.as_ref())
                        .unwrap_or_else(|| panic!("expect counter with name: {}", name))
                        .add(count.value() as u64, &tags);
                }
                MetricValue::Gauge { name, gauge } => {
                    self.gauge_registry
                        .lock()
                        .unwrap()
                        .get(name.as_ref())
                        .unwrap_or_else(|| panic!("expect gauge with name: {}", name))
                        .record(gauge.value() as u64, &tags);
                }
                MetricValue::Time { name, time } => {
                    let in_ms = time.value() as u64 / 1_000_000;
                    self.histogram_registry
                        .lock()
                        .unwrap()
                        .get(name.as_ref())
                        .unwrap_or_else(|| panic!("expect histogram with name: {}", name))
                        .record(in_ms, &tags)
                }
                MetricValue::SpillCount(_) => {}
                MetricValue::SpilledBytes(_) => {}
                MetricValue::SpilledRows(_) => {}
                MetricValue::CurrentMemoryUsage(_) => {}
                MetricValue::StartTimestamp(_) => {}
                MetricValue::EndTimestamp(_) => {}
                MetricValue::Custom { .. } => {}
                // Variants added in datafusion 54; not currently exported as telemetry.
                _ => {}
            };
        }
    }

    fn update_registries(&self, metric_values: &Vec<MetricValue>) {
        let meter = get_meter();
        for metric_value in metric_values {
            match metric_value {
                // All three arms defer instrument construction (and the
                // prefixed-name allocation) into `or_insert_with` so the hit
                // path — every batch after the first — does no work.
                MetricValue::Count { name, .. } => {
                    self.count_registry
                        .lock()
                        .unwrap()
                        .entry(name.to_string())
                        .or_insert_with(|| meter.u64_counter(add_service_prefix(name)).build());
                }
                MetricValue::Gauge { name, .. } => {
                    self.gauge_registry
                        .lock()
                        .unwrap()
                        .entry(name.to_string())
                        .or_insert_with(|| meter.u64_gauge(add_service_prefix(name)).build());
                }
                MetricValue::Time { name, .. } => {
                    self.histogram_registry
                        .lock()
                        .unwrap()
                        .entry(name.to_string())
                        // Time metrics are recorded in whole milliseconds, so
                        // auto-created histograms need the same unit and
                        // bucket layout as the hand-registered duration
                        // histograms — the OTel defaults top out at ~10s and
                        // would collapse long spans into +Inf.
                        .or_insert_with(|| {
                            meter
                                .u64_histogram(add_service_prefix(name))
                                .with_unit("ms")
                                .with_boundaries(DURATION_MS_BOUNDARIES.to_vec())
                                .build()
                        });
                }
                _ => {}
            }
        }
    }
}
pub fn initialize_metrics_recorder(
    metric_metadata_registry: HashMap<String, PipelineMetricMetadata>,
) {
    // Nothing to record without metadata (e.g. `--validate`/dry-run of a minimal topology).
    // Returning early avoids panicking on an empty registry during first initialization.
    if metric_metadata_registry.is_empty() {
        debug!("Skipping metrics recorder initialization: no metric metadata available");
        return;
    }

    let mut instance = METRICS_RECORDER_INSTANCE.lock().unwrap();
    if instance.is_none() {
        let meter = get_meter();
        let delta_meter = crate::telemetry::get_delta_meter();
        let mut count_registry: HashMap<String, Counter<u64>> = HashMap::new();
        let mut gauge_registry: HashMap<String, Gauge<u64>> = HashMap::new();
        let mut histogram_registry: HashMap<String, Histogram<u64>> = HashMap::new();

        let output_rows_name = add_service_prefix("output_rows");
        count_registry.insert(String::from("output_rows"), meter
            .u64_counter(output_rows_name)
            .with_description(
                "Indicates the number of rows produced as output from a specific query execution.",
            )
            .build());

        let output_rows_delta_name = add_service_prefix("output_rows_delta");
        debug!("Creating output_rows_delta counter with delta meter");
        count_registry.insert(String::from("output_rows_delta"), delta_meter
            .u64_counter(output_rows_delta_name)
            .with_description(
                "Indicates the number of rows produced as output from a specific query execution (Delta temporality).",
            )
            .build());

        let input_rows_name = add_service_prefix("input_rows");
        count_registry.insert(String::from("input_rows"), meter
            .u64_counter(input_rows_name)
            .with_description(
                "Indicates the number of rows produced as output from a specific query execution.",
            )
            .build());

        let elapsed_compute_name = add_service_prefix("elapsed_compute");
        histogram_registry.insert(
            String::from("elapsed_compute"),
            meter
                .u64_histogram(elapsed_compute_name)
                .with_description(
                    "Total time taken to execute the query execution plan on the data",
                )
                .with_unit("ms")
                .with_boundaries(DURATION_MS_BOUNDARIES.to_vec())
                .build(),
        );

        count_registry.insert(
            String::from("http_requests"),
            meter
                .u64_counter(add_service_prefix("http_requests"))
                .with_description(
                    "Number of outbound HTTP request attempts by handler or webhook node",
                )
                .build(),
        );
        count_registry.insert(
            String::from("http_retries"),
            meter
                .u64_counter(add_service_prefix("http_retries"))
                .with_description("Number of outbound HTTP request attempts that will be retried")
                .build(),
        );
        histogram_registry.insert(
            String::from("http_request_latency"),
            meter
                .u64_histogram(add_service_prefix("http_request_latency"))
                .with_description("Latency of each outbound HTTP request attempt")
                .with_unit("ms")
                .with_boundaries(vec![
                    10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0,
                    30_000.0, 60_000.0, 120_000.0, 300_000.0,
                ])
                .build(),
        );

        // Checkpoint metrics - Counters
        count_registry.insert(
            String::from("checkpoint_epochs_succeeded"),
            meter
                .u64_counter(add_service_prefix("checkpoint_epochs_succeeded"))
                .with_description("Number of checkpoint epochs that completed successfully")
                .build(),
        );
        count_registry.insert(
            String::from("checkpoint_epochs_failed"),
            meter
                .u64_counter(add_service_prefix("checkpoint_epochs_failed"))
                .with_description("Number of checkpoint epochs that failed due to timeout")
                .build(),
        );
        count_registry.insert(
            String::from("checkpoint_markers_sent"),
            meter
                .u64_counter(add_service_prefix("checkpoint_markers_sent"))
                .with_description("Number of checkpoint markers broadcast by coordinator")
                .build(),
        );
        count_registry.insert(
            String::from("checkpoint_acks_received"),
            meter
                .u64_counter(add_service_prefix("checkpoint_acks_received"))
                .with_description("Number of checkpoint acknowledgments received by coordinator")
                .build(),
        );
        count_registry.insert(
            String::from("checkpoint_finalizers_sent"),
            meter
                .u64_counter(add_service_prefix("checkpoint_finalizers_sent"))
                .with_description("Number of checkpoint finalizers broadcast by coordinator")
                .build(),
        );
        // Checkpoint metrics - Gauges
        gauge_registry.insert(
            String::from("checkpoint_epochs_in_flight"),
            meter
                .u64_gauge(add_service_prefix("checkpoint_epochs_in_flight"))
                .with_description("Number of checkpoint epochs currently in progress")
                .build(),
        );

        // Checkpoint metrics - Histograms
        // Note: OTel Prometheus exporter appends the unit as a suffix (e.g., "_milliseconds"),
        // so we use .with_unit("ms") and omit "_ms" from the metric name.
        //
        // Explicit boundaries in ms: 100ms to 1 hour.
        // Default OTel boundaries max out at 10s, which is too low for checkpoint metrics
        // that can take minutes or hours.
        let duration_boundaries_ms: Vec<f64> = DURATION_MS_BOUNDARIES.to_vec();

        histogram_registry.insert(
            String::from("checkpoint_epoch_duration"),
            meter
                .u64_histogram(add_service_prefix("checkpoint_epoch_duration"))
                .with_description("Time between consecutive epoch finalizations")
                .with_unit("ms")
                .with_boundaries(duration_boundaries_ms.clone())
                .build(),
        );
        histogram_registry.insert(
            String::from("checkpoint_marker_arrival"),
            meter
                .u64_histogram(add_service_prefix("checkpoint_marker_arrival"))
                .with_description("Time from checkpoint marker creation to arrival at a node")
                .with_unit("ms")
                .with_boundaries(duration_boundaries_ms.clone())
                .build(),
        );
        histogram_registry.insert(
            String::from("checkpoint_sink_flush"),
            meter
                .u64_histogram(add_service_prefix("checkpoint_sink_flush"))
                .with_description("Time from sink receiving checkpoint marker to completing flush and sending ack")
                .with_unit("ms")
                .with_boundaries(duration_boundaries_ms.clone())
                .build(),
        );
        histogram_registry.insert(
            String::from("checkpoint_finalization_wait"),
            meter
                .u64_histogram(add_service_prefix("checkpoint_finalization_wait"))
                .with_description(
                    "Time producer spends blocked waiting for previous epoch to finalize",
                )
                .with_unit("ms")
                .with_boundaries(duration_boundaries_ms.clone())
                .build(),
        );
        histogram_registry.insert(
            String::from("checkpoint_per_sink_ack_latency"),
            meter
                .u64_histogram(add_service_prefix("checkpoint_per_sink_ack_latency"))
                .with_description("Per-sink time from epoch creation to ack arrival")
                .with_unit("ms")
                .with_boundaries(duration_boundaries_ms.clone())
                .build(),
        );

        // Event-time freshness metrics — gauge of running max event_time per
        // source, plus per-row lag (t_emit - event_time). Reuses the
        // checkpoint duration boundaries; tune in a follow-up after
        // observing real-data distribution.
        gauge_registry.insert(
            String::from("event_time_watermark"),
            meter
                .u64_gauge(add_service_prefix("event_time_watermark"))
                .with_description("Running max event_time observed by source, unix milliseconds")
                .with_unit("ms")
                .build(),
        );
        histogram_registry.insert(
            String::from("event_time_lag"),
            meter
                .u64_histogram(add_service_prefix("event_time_lag"))
                .with_description("Per-row t_emit - event_time, milliseconds")
                .with_unit("ms")
                .with_boundaries(duration_boundaries_ms.clone())
                .build(),
        );
        let mut metric_metadata_tags_registry = HashMap::new();
        // caching tags for each metric metadata so that we don't have to compute them everytime metric is recorded
        for (metric_metadata_id, metric_metadata) in metric_metadata_registry.clone() {
            metric_metadata_tags_registry.insert(metric_metadata_id, metric_metadata.to_tags());
        }
        let service_instance_id = metric_metadata_registry
            .values()
            .next()
            .expect("expect at least one metric metadata to be available")
            .service_instance_id
            .clone();
        // Series seeding happens later, via `seed_elapsed_compute_series`,
        // once plugin-declared identity labels have been merged.
        let recorder = Arc::new(MetricsRecorder {
            service_instance_id,
            metric_metadata_registry: Mutex::new(metric_metadata_registry),
            metric_metadata_tags_registry: Mutex::new(metric_metadata_tags_registry),
            count_registry: Mutex::new(count_registry),
            gauge_registry: Mutex::new(gauge_registry),
            histogram_registry: Mutex::new(histogram_registry),
            ..Default::default()
        });
        *instance = Some(recorder);
    } else {
        debug!("MetricsRecorder already initialized; merging metric metadata registry.");
        // Merge new metadata into existing recorder so subsequent pipelines
        // are tracked. Series seeding for the new nodes happens later via
        // `seed_elapsed_compute_series` (idempotent per node id).
        if let Some(existing) = instance.as_ref() {
            let mut reg_lock = existing
                .metric_metadata_registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut tags_lock = existing
                .metric_metadata_tags_registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (id, meta) in metric_metadata_registry.into_iter() {
                reg_lock.insert(id.clone(), meta.clone());
                tags_lock.insert(id, meta.to_tags());
            }
        }
    }
}

fn get_meter() -> Meter {
    global::meter("execution_metrics")
}

/// Merge per-source identity labels into the running metric metadata for `metadata_id`.
///
/// This is the metrics subsystem's consumption path for plugin-declared identity. Called
/// at plugin-construction time so a plugin's `labels` (returned via `PluginResult::labels`)
/// become Prometheus labels on every metric that source records. A no-op if no metadata is
/// registered for this id (e.g., no-op recorder path in tests that never called
/// `initialize_metrics_recorder`).
///
/// Plugin-declared labels are validated with a subset of the rules applied to YAML
/// labels at config load. Shared rules (drop with WARN on violation): global reserved
/// keys (`id`, `topology_node_type`, `operator_type`, `service_instance_id`),
/// Prometheus-reserved `__`-prefixed keys, label-name grammar, value length, control
/// characters in values, and the per-node [`MAX_LABELS_PER_NODE`] cap counted across
/// both YAML-seeded and plugin-declared labels.
///
/// Intentionally *not* enforced here: per-type reserved keys (`topic` on Kafka,
/// `table` on ClickHouse, etc.). `merge_metadata_tags` is only called on plugin
/// variants of Source/Transform/Sink, where the host seeds `type=<plugin_type>` as
/// the sole per-type identity tag. A plugin declaring `topic` or `table` is adding a
/// semantically meaningful label about what it read from, not shadowing a host
/// identity the plugin doesn't own. Collisions with the host-seeded `type` tag still
/// fire the per-key collision WARN.
///
/// Plugin labels cannot go through the config-load validator because they are produced
/// by plugin code at runtime, not by YAML — so the applicable safety properties are
/// enforced here.
///
/// When a plugin-declared label collides with an existing label already in
/// `additional_tags` (typically seeded by `build_pipeline_metric_metadata` from YAML or
/// per-type node config), plugin wins — the plugin code is authoritative about its own
/// identity (e.g. `chain_slug`, `topic`) in ways YAML cannot override. A single WARN is
/// emitted per colliding key naming the node, the existing value, and the plugin value,
/// so silent overrides don't mask misconfigurations. Same-value agreement is not a
/// collision and emits no WARN.
///
/// Both registries must stay in sync: the recording hot-path reads tags exclusively from
/// the cached `metric_metadata_tags_registry` (built via `to_tags()`), so updating only
/// `additional_tags` on the metadata would leave plugin labels invisible to emitted
/// Prometheus metrics.
pub fn merge_metadata_tags(metadata_id: &str, tags: Vec<(String, String)>) {
    if tags.is_empty() {
        return;
    }
    let recorder = get_metrics_recorder();
    let mut registry = recorder
        .metric_metadata_registry
        .lock()
        .expect("metric_metadata_registry lock poisoned");
    let Some(metadata) = registry.get_mut(metadata_id) else {
        warn!(
            metadata_id,
            "merge_metadata_tags called but no metadata registered; plugin identity tags will not appear"
        );
        return;
    };
    for (k, v) in tags {
        if !validate_plugin_label(metadata_id, &k, &v) {
            continue;
        }
        let is_new_key = !metadata.additional_tags.contains_key(&k);
        if is_new_key && metadata.additional_tags.len() >= crate::topology::MAX_LABELS_PER_NODE {
            warn!(
                metadata_id,
                key = %k,
                cap = crate::topology::MAX_LABELS_PER_NODE,
                current_count = metadata.additional_tags.len(),
                "plugin-declared label would exceed per-node label cap; dropping"
            );
            continue;
        }
        if let Some(existing) = metadata.additional_tags.get(&k)
            && existing != &v
        {
            warn!(
                metadata_id,
                key = %k,
                existing_value = %existing,
                plugin_value = %v,
                "plugin-declared label overrides existing label for same key; plugin value wins"
            );
        }
        metadata.additional_tags.insert(k, v);
    }
    let refreshed_tags = metadata.to_tags();
    recorder
        .metric_metadata_tags_registry
        .lock()
        .expect("metric_metadata_tags_registry lock poisoned")
        .insert(metadata_id.to_string(), refreshed_tags);
}

/// Validate a single plugin-declared label key/value pair against the per-label safety
/// rules YAML labels undergo at config load. Returns `true` when the label is safe to
/// merge and `false` (with a WARN) when it should be dropped. Keep the per-label rules
/// in sync with `PipelineTopology::check_labels` in `topology.rs`. Per-type reserved-key
/// and per-node count-cap rules are documented on `merge_metadata_tags` and enforced at
/// the call site, not here.
fn validate_plugin_label(metadata_id: &str, key: &str, value: &str) -> bool {
    use crate::topology::{LABEL_KEY_PATTERN, MAX_LABEL_VALUE_LEN, RESERVED_LABEL_KEYS};
    if RESERVED_LABEL_KEYS.contains(&key) {
        warn!(
            metadata_id,
            key,
            "plugin-declared label key is reserved (collides with a built-in metric tag); dropping"
        );
        return false;
    }
    if key.starts_with("__") {
        warn!(
            metadata_id,
            key, "plugin-declared label key uses Prometheus-reserved '__' prefix; dropping"
        );
        return false;
    }
    if !LABEL_KEY_PATTERN.is_match(key) {
        warn!(
            metadata_id,
            key,
            "plugin-declared label key does not match Prometheus label-name grammar \
             (^[a-zA-Z_][a-zA-Z0-9_]*$); dropping"
        );
        return false;
    }
    if value.len() > MAX_LABEL_VALUE_LEN {
        warn!(
            metadata_id,
            key,
            value_len = value.len(),
            cap = MAX_LABEL_VALUE_LEN,
            "plugin-declared label value exceeds per-label length cap; dropping"
        );
        return false;
    }
    if let Some(bad) = value.chars().find(|c| c.is_control() && *c != '\t') {
        warn!(
            metadata_id,
            key,
            control_char = format!("U+{:04X}", bad as u32),
            "plugin-declared label value contains a control character; dropping"
        );
        return false;
    }
    true
}

pub fn get_metrics_recorder() -> Arc<MetricsRecorder> {
    let mut instance = METRICS_RECORDER_INSTANCE.lock().unwrap();
    if let Some(metrics_recorder) = instance.clone() {
        metrics_recorder.clone()
    } else {
        warn!(
            "get_metrics_recorder called before initialize_metrics_recorder; falling back to no-op implementation; no metrics will be recorded"
        );
        let recorder = Arc::new(MetricsRecorder {
            service_instance_id: "default-service-instance-id".to_string(),
            ..Default::default()
        });
        *instance = Some(recorder.clone());
        recorder
    }
}

/// A simplified metrics recorder for control-plane components (e.g., checkpoint coordinator).
///
/// Unlike the data-plane `MetricsRecorder`, this does not require pre-registration
/// or passing a `metadata_id` on every call. It maintains its own fixed tags
/// (component id) and delegates to the underlying recorder.
///
/// Usage:
/// ```ignore
/// let recorder = get_control_plane_metrics_recorder("checkpoint_coordinator");
/// recorder.record_count("checkpoint_epochs_succeeded", 1);
/// recorder.record_gauge("checkpoint_epochs_in_flight", 3);
/// recorder.record_time("checkpoint_epoch_duration", duration);
/// ```
pub struct ControlPlaneMetricsRecorder {
    inner: Arc<MetricsRecorder>,
    component_id: String,
}

impl ControlPlaneMetricsRecorder {
    fn new(inner: Arc<MetricsRecorder>, component_id: &str) -> Self {
        let mut tags = HashMap::new();
        tags.insert(String::from("id"), component_id.to_string());
        // Include service_instance_id from the inner recorder for proper metric attribution
        tags.insert(
            String::from("service_instance_id"),
            inner.service_instance_id.clone(),
        );

        // Register this component in the inner recorder's tags registry
        // so record_* calls can find the tags
        inner
            .metric_metadata_tags_registry
            .lock()
            .unwrap()
            .insert(component_id.to_string(), tags);

        Self {
            inner,
            component_id: component_id.to_string(),
        }
    }

    /// Record a counter metric.
    pub fn record_count(&self, name: &str, value: u64) {
        self.inner
            .record_count_w_tags(name, value, vec![], &self.component_id);
    }

    /// Record a gauge metric.
    pub fn record_gauge(&self, name: &str, value: u64) {
        self.inner
            .record_gauge_w_tags(name, value, vec![], &self.component_id);
    }

    /// Record a time/histogram metric.
    pub fn record_time(&self, name: &str, duration: Duration) {
        self.inner
            .record_time_w_tags(name, duration, vec![], &self.component_id);
    }

    /// Record a time/histogram metric with extra tags.
    pub fn record_time_w_tags(&self, name: &str, duration: Duration, tags: Vec<(&str, &str)>) {
        self.inner
            .record_time_w_tags(name, duration, tags, &self.component_id);
    }
}

static CONTROL_PLANE_RECORDERS: OnceLock<Mutex<HashMap<String, Arc<ControlPlaneMetricsRecorder>>>> =
    OnceLock::new();

fn get_control_plane_recorders() -> &'static Mutex<HashMap<String, Arc<ControlPlaneMetricsRecorder>>>
{
    CONTROL_PLANE_RECORDERS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Get or create a control-plane metrics recorder for the given component.
///
/// This is the preferred API for control-plane components like the checkpoint coordinator.
/// Unlike the data-plane `MetricsRecorder`, it does not require pre-registration or
/// passing a `metadata_id` on every call.
///
/// # Arguments
/// * `component_id` - A unique identifier for this component (e.g., "checkpoint_coordinator")
/// * `topology_node_type` - The type of node (e.g., "coordinator", "controller")
///
/// # Example
/// ```ignore
/// let recorder = get_control_plane_metrics_recorder("checkpoint_coordinator");
/// recorder.record_count("checkpoint_epochs_succeeded", 1);
/// ```
pub fn get_control_plane_metrics_recorder(component_id: &str) -> Arc<ControlPlaneMetricsRecorder> {
    let mut recorders = get_control_plane_recorders().lock().unwrap();
    if let Some(recorder) = recorders.get(component_id) {
        return recorder.clone();
    }

    let inner = get_metrics_recorder();
    let recorder = Arc::new(ControlPlaneMetricsRecorder::new(inner, component_id));
    recorders.insert(component_id.to_string(), recorder.clone());
    recorder
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::types::create_count_with_value;

    use crate::node_context::{NodeContext, TopologyNodeType};
    use crate::telemetry::PipelineMetricMetadata;

    /// Tests for `merge_metadata_tags`. These tests touch the global
    /// `METRICS_RECORDER_INSTANCE`; a mutex serializes them so state from one test cannot
    /// bleed into another under parallel execution.
    mod merge_metadata_tags_tests {
        use super::*;
        use std::sync::Mutex;

        static TEST_LOCK: Mutex<()> = Mutex::new(());

        fn reset_instance() {
            let mut instance = METRICS_RECORDER_INSTANCE.lock().unwrap();
            *instance = None;
        }

        fn seed_metadata(id: &str) {
            let mut map = HashMap::new();
            map.insert(
                id.to_string(),
                PipelineMetricMetadata {
                    node_context: NodeContext::new(TopologyNodeType::Source, "plugin", "blocks"),
                    service_instance_id: "test-instance".to_string(),
                    additional_tags: Default::default(),
                    children_metadata_ids: vec![],
                },
            );
            initialize_metrics_recorder(map);
        }

        fn additional_tags(id: &str) -> std::collections::BTreeMap<String, String> {
            let recorder = get_metrics_recorder();
            let registry = recorder.metric_metadata_registry.lock().unwrap();
            registry.get(id).unwrap().additional_tags.clone()
        }

        fn cached_tags(id: &str) -> HashMap<String, String> {
            let recorder = get_metrics_recorder();
            let registry = recorder.metric_metadata_tags_registry.lock().unwrap();
            registry.get(id).cloned().unwrap_or_default()
        }

        #[test]
        fn merges_tags_into_existing_metadata() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags(
                "app::blocks",
                vec![("chain_slug".into(), "ethereum".into())],
            );
            let tags = additional_tags("app::blocks");
            assert_eq!(tags.get("chain_slug"), Some(&"ethereum".to_string()));
        }

        #[test]
        fn empty_tags_is_noop() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags("app::blocks", vec![]);
            assert!(additional_tags("app::blocks").is_empty());
        }

        #[test]
        fn missing_metadata_is_noop() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::other");
            merge_metadata_tags("app::missing", vec![("chain".into(), "eth".into())]);
            assert!(additional_tags("app::other").is_empty());
        }

        /// Regression: `--validate`/dry-run of a minimal topology produced an empty
        /// metric metadata registry, and the first initialization used to
        /// `.expect()`-panic on `values().next()`. It must now be a no-op instead.
        #[test]
        fn empty_registry_does_not_panic() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            initialize_metrics_recorder(HashMap::new());
            let instance = METRICS_RECORDER_INSTANCE.lock().unwrap();
            assert!(
                instance.is_none(),
                "empty registry must not initialize the recorder"
            );
        }

        #[test]
        fn later_merge_overwrites_earlier_for_same_key() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags("app::blocks", vec![("chain".into(), "eth".into())]);
            merge_metadata_tags("app::blocks", vec![("chain".into(), "polygon".into())]);
            assert_eq!(
                additional_tags("app::blocks").get("chain"),
                Some(&"polygon".to_string())
            );
        }

        /// The metric-recording hot path reads from `metric_metadata_tags_registry`,
        /// not `metric_metadata_registry`. Labels must land in the cached registry too
        /// or they never appear on emitted Prometheus metrics.
        #[test]
        fn merge_refreshes_cached_tags_registry() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags(
                "app::blocks",
                vec![("chain_slug".into(), "ethereum".into())],
            );
            assert_eq!(
                cached_tags("app::blocks").get("chain_slug"),
                Some(&"ethereum".to_string()),
                "plugin-declared labels must appear in the cached tags registry used by record_*"
            );
        }

        // ------------------------------------------------------------------
        // Plugin-wins-with-WARN precedence tests.
        //
        // Simulate the YAML-first, plugin-second ordering that
        // `build_pipeline_metric_metadata` (YAML seeding) and
        // `create_*_plugin` (plugin merge) impose at startup by calling
        // `merge_metadata_tags` twice. The second call models the plugin's
        // `PluginResult::labels` arriving after YAML. WARN output is not
        // asserted here (no tracing-capture harness in this crate) — the
        // e2e test in streamling-e2e exercises the full flow.
        // ------------------------------------------------------------------

        #[test]
        fn no_collision_both_labels_preserved() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            // YAML seeds `tier`
            merge_metadata_tags("app::blocks", vec![("tier".into(), "critical".into())]);
            // Plugin adds `chain` (non-colliding)
            merge_metadata_tags("app::blocks", vec![("chain".into(), "ethereum".into())]);
            let tags = additional_tags("app::blocks");
            assert_eq!(tags.get("tier"), Some(&"critical".to_string()));
            assert_eq!(tags.get("chain"), Some(&"ethereum".to_string()));
        }

        #[test]
        fn agreement_same_value_overwrites_without_concern() {
            // Plugin and YAML agree on the same key+value. Final value is
            // the agreed value (same as YAML). Not treated as a real
            // collision in production; WARN logic filters via `existing != &v`.
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags("app::blocks", vec![("chain".into(), "ethereum".into())]);
            merge_metadata_tags("app::blocks", vec![("chain".into(), "ethereum".into())]);
            assert_eq!(
                additional_tags("app::blocks").get("chain"),
                Some(&"ethereum".to_string())
            );
        }

        #[test]
        fn collision_plugin_wins() {
            // YAML declared `chain=ethereum`; plugin declares `chain=polygon`.
            // Plugin wins. In production, WARN also fires — asserted via the
            // e2e test that runs with a real subscriber.
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags("app::blocks", vec![("chain".into(), "ethereum".into())]);
            merge_metadata_tags("app::blocks", vec![("chain".into(), "polygon".into())]);
            assert_eq!(
                additional_tags("app::blocks").get("chain"),
                Some(&"polygon".to_string()),
                "plugin value must win on collision"
            );
        }

        #[test]
        fn collision_refreshes_cached_tags_to_plugin_value() {
            // Ensures the hot-path cache stays in sync even when the
            // second merge is a collision. If the cache update short-
            // circuited on collision, the cached tags would still show the
            // YAML value, not the plugin value.
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags("app::blocks", vec![("chain".into(), "ethereum".into())]);
            merge_metadata_tags("app::blocks", vec![("chain".into(), "polygon".into())]);
            assert_eq!(
                cached_tags("app::blocks").get("chain"),
                Some(&"polygon".to_string())
            );
        }

        #[test]
        fn reserved_key_from_plugin_is_dropped() {
            // Plugins bypass the YAML config-load validator; this test
            // asserts the merge_metadata_tags runtime validator catches
            // reserved keys a malicious or buggy plugin might declare.
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags(
                "app::blocks",
                vec![
                    ("id".into(), "spoofed".into()),
                    ("chain".into(), "ethereum".into()),
                ],
            );
            let tags = additional_tags("app::blocks");
            assert!(!tags.contains_key("id"), "reserved `id` must be dropped");
            assert_eq!(tags.get("chain"), Some(&"ethereum".to_string()));
        }

        #[test]
        fn invalid_grammar_key_from_plugin_is_dropped() {
            // Plugin declares keys violating Prometheus label-name grammar:
            // space, hyphen, leading digit. Runtime validator must catch all
            // three the same way config-load validation does for YAML keys.
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags(
                "app::blocks",
                vec![
                    ("my key".into(), "v".into()),
                    ("chain-slug".into(), "v".into()),
                    ("2024q1".into(), "v".into()),
                    ("valid_key".into(), "v".into()),
                ],
            );
            let tags = additional_tags("app::blocks");
            assert!(!tags.contains_key("my key"));
            assert!(!tags.contains_key("chain-slug"));
            assert!(!tags.contains_key("2024q1"));
            assert_eq!(tags.get("valid_key"), Some(&"v".to_string()));
        }

        #[test]
        fn underscore_prefix_key_from_plugin_is_dropped() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags(
                "app::blocks",
                vec![
                    ("__name__".into(), "fake_metric".into()),
                    ("chain".into(), "ethereum".into()),
                ],
            );
            let tags = additional_tags("app::blocks");
            assert!(!tags.contains_key("__name__"));
            assert_eq!(tags.get("chain"), Some(&"ethereum".to_string()));
        }

        #[test]
        fn control_char_in_plugin_value_is_dropped() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags(
                "app::blocks",
                vec![
                    ("env".into(), "prod\nmalicious".into()),
                    ("chain".into(), "ethereum".into()),
                ],
            );
            let tags = additional_tags("app::blocks");
            assert!(!tags.contains_key("env"), "newline value must be dropped");
            assert_eq!(tags.get("chain"), Some(&"ethereum".to_string()));
        }

        #[test]
        fn oversized_plugin_value_is_dropped() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags(
                "app::blocks",
                vec![
                    ("k".into(), "x".repeat(257)),
                    ("chain".into(), "ethereum".into()),
                ],
            );
            let tags = additional_tags("app::blocks");
            assert!(!tags.contains_key("k"), "oversized value must be dropped");
            assert_eq!(tags.get("chain"), Some(&"ethereum".to_string()));
        }

        #[test]
        fn plugin_labels_respect_per_node_count_cap() {
            // Match the config-load cap of 20 labels per node on the
            // runtime path. Pre-seed additional_tags with 19 entries
            // (total capacity 1 more) and let the plugin try to add 5
            // new keys. Only the first should land; the rest drop with
            // WARN. Collisions (same-key overwrite) must NOT count
            // toward the cap since they don't grow the map.
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");

            // Seed 19 labels via the public API so we test what the
            // validator actually observes.
            let seed: Vec<(String, String)> =
                (0..19).map(|i| (format!("seed{i}"), "v".into())).collect();
            merge_metadata_tags("app::blocks", seed);
            assert_eq!(additional_tags("app::blocks").len(), 19);

            // Plugin tries to add 5 new keys plus an override of an
            // existing seed key. One new key fits (20 total); the other
            // four new keys are dropped. The collision overwrites and
            // does not consume a slot.
            merge_metadata_tags(
                "app::blocks",
                vec![
                    ("seed0".into(), "override".into()),
                    ("new1".into(), "v".into()),
                    ("new2".into(), "v".into()),
                    ("new3".into(), "v".into()),
                    ("new4".into(), "v".into()),
                    ("new5".into(), "v".into()),
                ],
            );
            let tags = additional_tags("app::blocks");
            assert_eq!(tags.len(), 20, "must not exceed per-node cap");
            assert_eq!(tags.get("seed0"), Some(&"override".to_string()));
            assert_eq!(tags.get("new1"), Some(&"v".to_string()));
            assert!(!tags.contains_key("new2"));
            assert!(!tags.contains_key("new3"));
            assert!(!tags.contains_key("new4"));
            assert!(!tags.contains_key("new5"));
        }

        #[test]
        fn tab_in_plugin_value_is_allowed() {
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags("app::blocks", vec![("env".into(), "prod\tqa".into())]);
            assert_eq!(
                additional_tags("app::blocks").get("env"),
                Some(&"prod\tqa".to_string())
            );
        }

        #[test]
        fn multi_key_merge_mixed_collision_and_new() {
            // Plugin inserts two keys in a single call: one collides with
            // an existing YAML label, one does not. Both must land.
            let _guard = TEST_LOCK.lock().unwrap();
            reset_instance();
            seed_metadata("app::blocks");
            merge_metadata_tags("app::blocks", vec![("chain".into(), "ethereum".into())]);
            merge_metadata_tags(
                "app::blocks",
                vec![
                    ("chain".into(), "polygon".into()),
                    ("tier".into(), "gold".into()),
                ],
            );
            let tags = additional_tags("app::blocks");
            assert_eq!(tags.get("chain"), Some(&"polygon".to_string()));
            assert_eq!(tags.get("tier"), Some(&"gold".to_string()));
        }
    }

    #[test]
    fn zero_gauge_is_not_skipped() {
        let gauge = MetricValue::Gauge {
            name: "lag".into(),
            gauge: create_gauge_with_value(0),
        };
        assert!(
            !should_skip_zero_value_metric(&gauge),
            "Gauge with value 0 must not be skipped — it reports absolute state"
        );
    }

    #[test]
    fn zero_counter_is_skipped() {
        let count = MetricValue::Count {
            name: "rows".into(),
            count: create_count_with_value(0),
        };
        assert!(
            should_skip_zero_value_metric(&count),
            "Counter adding 0 is a no-op and should be skipped"
        );
    }

    #[test]
    fn nonzero_counter_is_not_skipped() {
        let count = MetricValue::Count {
            name: "rows".into(),
            count: create_count_with_value(5),
        };
        assert!(
            !should_skip_zero_value_metric(&count),
            "Counter adding 5 must not be skipped"
        );
    }

    /// Filtering a batch must process every element. The old code used `return`
    /// inside a loop which aborted the entire batch on the first zero-value
    /// metric, silently dropping subsequent non-zero metrics.
    #[test]
    fn zero_value_in_batch_does_not_suppress_later_metrics() {
        let zero_count = MetricValue::Count {
            name: "rows".into(),
            count: create_count_with_value(0),
        };
        let nonzero_count = MetricValue::Count {
            name: "rows".into(),
            count: create_count_with_value(5),
        };
        let gauge_zero = MetricValue::Gauge {
            name: "lag".into(),
            gauge: create_gauge_with_value(0),
        };

        let batch = [zero_count, nonzero_count, gauge_zero];
        let recordable: Vec<_> = batch
            .iter()
            .filter(|m| !should_skip_zero_value_metric(m))
            .collect();

        assert_eq!(
            recordable.len(),
            2,
            "Expected the non-zero counter and zero gauge to survive filtering, got {:?}",
            recordable
        );
    }

    /// Regression tests for turning DataFusion's *cumulative* subtree metrics
    /// into per-batch deltas. `collect_subtree_metrics` folds every descendant
    /// operator's metrics into the set handed to `record_execution_plan_metrics`;
    /// once folded in, deeper operators' cumulative metrics (a join's
    /// `build_time` / `join_time`, operator-defined counters) must be deltaed
    /// too — not just `elapsed_compute` — or their histograms/counters inflate
    /// on every batch.
    mod subtree_delta_tests {
        use super::*;
        use datafusion::physical_plan::metrics::Metric;

        fn test_recorder() -> MetricsRecorder {
            MetricsRecorder::default()
        }

        fn time_metric(name: &'static str, nanos: u64) -> Arc<Metric> {
            Arc::new(Metric::new(
                MetricValue::Time {
                    name: name.into(),
                    time: create_time_from_duration(Duration::from_nanos(nanos)),
                },
                None,
            ))
        }

        fn elapsed_compute_metric(nanos: u64) -> Arc<Metric> {
            Arc::new(Metric::new(
                MetricValue::ElapsedCompute(create_time_from_duration(Duration::from_nanos(nanos))),
                None,
            ))
        }

        fn count_metric(name: &'static str, value: usize) -> Arc<Metric> {
            Arc::new(Metric::new(
                MetricValue::Count {
                    name: name.into(),
                    count: create_count_with_value(value),
                },
                None,
            ))
        }

        fn find_time_ms(values: &[MetricValue], name: &str) -> Option<u64> {
            values.iter().find_map(|v| match v {
                MetricValue::Time { name: n, time } if n == name => {
                    Some(time.value() as u64 / 1_000_000)
                }
                MetricValue::ElapsedCompute(time) if name == "elapsed_compute" => {
                    Some(time.value() as u64 / 1_000_000)
                }
                _ => None,
            })
        }

        fn find_count(values: &[MetricValue], name: &str) -> Option<u64> {
            values.iter().find_map(|v| match v {
                MetricValue::Count { name: n, count } if n == name => Some(count.value() as u64),
                _ => None,
            })
        }

        /// The core regression: a join's cumulative `build_time` must be emitted
        /// as the per-batch delta, not the ever-growing raw snapshot. Before the
        /// fix, the second batch re-emitted the full cumulative 8ms; now it emits
        /// only the 3ms that accrued since the previous batch.
        #[test]
        fn cumulative_time_metric_is_emitted_as_per_batch_delta() {
            let recorder = test_recorder();
            let id = "app::join_transform";

            let mut set1 = MetricsSet::new();
            set1.push(time_metric("build_time", 5_000_000));
            let d1 = recorder.subtree_delta_metric_values(id, &set1);
            assert_eq!(
                find_time_ms(&d1, "df_build_time"),
                Some(5),
                "first batch emits the full 5ms accrued so far"
            );

            let mut set2 = MetricsSet::new();
            set2.push(time_metric("build_time", 8_000_000));
            let d2 = recorder.subtree_delta_metric_values(id, &set2);
            assert_eq!(
                find_time_ms(&d2, "df_build_time"),
                Some(3),
                "second batch must emit only the 3ms delta, not the cumulative 8ms"
            );
        }

        /// Operator-defined counters (e.g. join `build_input_rows`) are also
        /// cumulative and must be deltaed the same way.
        #[test]
        fn cumulative_counter_is_emitted_as_per_batch_delta() {
            let recorder = test_recorder();
            let id = "app::join_transform";

            let mut set1 = MetricsSet::new();
            set1.push(count_metric("build_input_rows", 100));
            let d1 = recorder.subtree_delta_metric_values(id, &set1);
            assert_eq!(find_count(&d1, "df_build_input_rows"), Some(100));

            let mut set2 = MetricsSet::new();
            set2.push(count_metric("build_input_rows", 175));
            let d2 = recorder.subtree_delta_metric_values(id, &set2);
            assert_eq!(
                find_count(&d2, "df_build_input_rows"),
                Some(75),
                "counter must emit only the 75-row delta, not the cumulative 175"
            );
        }

        /// Multiple operators in the subtree can expose the same metric name
        /// (e.g. two joins each with `join_time`). They are aggregated by name
        /// first, then deltaed once — otherwise the shared accrual key would be
        /// overwritten mid-batch and corrupt the delta.
        #[test]
        fn duplicate_named_metrics_are_aggregated_before_delta() {
            let recorder = test_recorder();
            let id = "app::two_joins";

            let mut set1 = MetricsSet::new();
            set1.push(time_metric("join_time", 2_000_000));
            set1.push(time_metric("join_time", 3_000_000));
            let d1 = recorder.subtree_delta_metric_values(id, &set1);
            assert_eq!(
                find_time_ms(&d1, "df_join_time"),
                Some(5),
                "two operators' 2ms + 3ms aggregate to a single 5ms series"
            );

            let mut set2 = MetricsSet::new();
            set2.push(time_metric("join_time", 4_000_000));
            set2.push(time_metric("join_time", 5_000_000));
            let d2 = recorder.subtree_delta_metric_values(id, &set2);
            assert_eq!(
                find_time_ms(&d2, "df_join_time"),
                Some(4),
                "aggregate grew 5ms -> 9ms; only the 4ms delta is emitted"
            );
        }

        /// `elapsed_compute` keeps its existing per-batch delta behavior under
        /// the generalized path.
        #[test]
        fn elapsed_compute_still_deltaed() {
            let recorder = test_recorder();
            let id = "app::sql";

            let mut set1 = MetricsSet::new();
            set1.push(elapsed_compute_metric(10_000_000));
            let d1 = recorder.subtree_delta_metric_values(id, &set1);
            assert_eq!(find_time_ms(&d1, "elapsed_compute"), Some(10));

            let mut set2 = MetricsSet::new();
            set2.push(elapsed_compute_metric(14_000_000));
            let d2 = recorder.subtree_delta_metric_values(id, &set2);
            assert_eq!(
                find_time_ms(&d2, "elapsed_compute"),
                Some(4),
                "elapsed_compute emits only the 4ms delta"
            );
        }

        /// Distinct metric names are tracked independently, and a re-executed
        /// stream (cumulative counter resets below the last seen value) is
        /// treated as a fresh delta rather than stalling at zero.
        #[test]
        fn distinct_names_tracked_independently_and_handles_reset() {
            let recorder = test_recorder();
            let id = "app::join_transform";

            let mut set1 = MetricsSet::new();
            set1.push(time_metric("build_time", 6_000_000));
            set1.push(time_metric("join_time", 2_000_000));
            let d1 = recorder.subtree_delta_metric_values(id, &set1);
            assert_eq!(find_time_ms(&d1, "df_build_time"), Some(6));
            assert_eq!(find_time_ms(&d1, "df_join_time"), Some(2));

            // Stream re-executed: build_time resets to a smaller cumulative
            // value. The current value is taken as the delta.
            let mut set2 = MetricsSet::new();
            set2.push(time_metric("build_time", 1_000_000));
            let d2 = recorder.subtree_delta_metric_values(id, &set2);
            assert_eq!(
                find_time_ms(&d2, "df_build_time"),
                Some(1),
                "counter reset is treated as a fresh delta"
            );
        }

        /// Sub-millisecond per-batch time is carried forward rather than
        /// truncated to zero, so high-throughput compute isn't undercounted.
        #[test]
        fn sub_millisecond_time_delta_is_carried_forward() {
            let recorder = test_recorder();
            let id = "app::sql";

            // 0.6ms accrues: below 1ms, so nothing is emitted yet.
            let mut set1 = MetricsSet::new();
            set1.push(time_metric("build_time", 600_000));
            let d1 = recorder.subtree_delta_metric_values(id, &set1);
            assert_eq!(find_time_ms(&d1, "df_build_time"), None);

            // Another 0.6ms (cumulative 1.2ms): the carried remainder crosses
            // 1ms and a whole millisecond is emitted.
            let mut set2 = MetricsSet::new();
            set2.push(time_metric("build_time", 1_200_000));
            let d2 = recorder.subtree_delta_metric_values(id, &set2);
            assert_eq!(find_time_ms(&d2, "df_build_time"), Some(1));
        }

        /// Regression: a typed `OutputBatches` and a generic
        /// `Count { name: "output_batches" }` in the same subtree made
        /// `MetricsSet::aggregate_by_name` panic ("Mismatched metric types"),
        /// crashing the pipeline. The manual aggregation must handle the mix
        /// without panicking: the typed variant stays unexported, the generic
        /// count exports under the `df_` namespace.
        #[test]
        fn mixed_variant_same_name_metrics_do_not_panic() {
            let recorder = test_recorder();
            let id = "app::sql";

            let output_batches = Count::new();
            output_batches.add(2);
            let mut set1 = MetricsSet::new();
            set1.push(Arc::new(Metric::new(
                MetricValue::OutputBatches(output_batches),
                None,
            )));
            set1.push(count_metric("output_batches", 1));
            let d1 = recorder.subtree_delta_metric_values(id, &set1);
            assert_eq!(
                find_count(&d1, "output_batches"),
                None,
                "the typed variant stays unexported; the key point is no panic"
            );
            assert_eq!(find_count(&d1, "df_output_batches"), Some(1));
        }

        /// A `Count` and a `Time` sharing one name must keep independent
        /// accrual state: a shared last-seen slot would trip the reset
        /// heuristic every batch and re-emit full cumulative values.
        #[test]
        fn same_name_count_and_time_do_not_clobber_state() {
            let recorder = test_recorder();
            let id = "app::sql";

            let mut set1 = MetricsSet::new();
            set1.push(count_metric("spill_metric", 5));
            set1.push(time_metric("spill_metric", 200_000_000)); // 200ms
            let d1 = recorder.subtree_delta_metric_values(id, &set1);
            assert_eq!(find_count(&d1, "df_spill_metric"), Some(5));
            assert_eq!(find_time_ms(&d1, "df_spill_metric"), Some(200));

            // Unchanged cumulative values: both series must emit nothing. With
            // a shared slot the count (5 < 200ms-in-nanos) would re-emit 5 and
            // the time would re-emit 200ms, every batch, forever.
            let mut set2 = MetricsSet::new();
            set2.push(count_metric("spill_metric", 5));
            set2.push(time_metric("spill_metric", 200_000_000));
            let d2 = recorder.subtree_delta_metric_values(id, &set2);
            assert_eq!(find_count(&d2, "df_spill_metric"), None);
            assert_eq!(find_time_ms(&d2, "df_spill_metric"), None);
        }

        /// A `Time` literally named "elapsed_compute" folds into the typed
        /// `ElapsedCompute` series (one accrual slot, one emission); a
        /// separate slot would clobber the typed variant's last-seen state
        /// and trip the reset heuristic every batch.
        #[test]
        fn time_named_elapsed_compute_merges_with_typed_variant() {
            let recorder = test_recorder();
            let id = "app::sql";

            let mut set1 = MetricsSet::new();
            set1.push(elapsed_compute_metric(10_000_000)); // 10ms typed
            set1.push(time_metric("elapsed_compute", 2_000_000)); // 2ms named
            let d1 = recorder.subtree_delta_metric_values(id, &set1);
            assert_eq!(
                find_time_ms(&d1, "elapsed_compute"),
                Some(12),
                "typed and same-named Time merge into one 12ms delta"
            );

            // Unchanged cumulatives: nothing re-emits. A clobbered shared slot
            // would alternate phantom deltas here forever.
            let mut set2 = MetricsSet::new();
            set2.push(elapsed_compute_metric(10_000_000));
            set2.push(time_metric("elapsed_compute", 2_000_000));
            let d2 = recorder.subtree_delta_metric_values(id, &set2);
            assert_eq!(find_time_ms(&d2, "elapsed_compute"), None);
        }

        /// Operator-internal counters named after streamling's own semantic
        /// row-count series (e.g. `StreamingUnnestExec`'s `input_rows`) must
        /// never land on those series — they would double-count rows on
        /// billing-relevant series `TelemetryStream` already populates. The
        /// `df_` namespace guarantees this for every name, present and future.
        #[test]
        fn subtree_counters_are_namespaced_away_from_semantic_series() {
            let recorder = test_recorder();
            let id = "app::unnest_transform";

            let mut set = MetricsSet::new();
            set.push(count_metric("input_rows", 500));
            set.push(count_metric("custom_operator_count", 3));
            let d = recorder.subtree_delta_metric_values(id, &set);
            assert_eq!(
                find_count(&d, "input_rows"),
                None,
                "operator-internal input_rows must not reach the semantic series"
            );
            assert_eq!(
                find_count(&d, "df_input_rows"),
                Some(500),
                "it is exported under the df_ namespace instead"
            );
            assert_eq!(find_count(&d, "df_custom_operator_count"), Some(3));
        }

        /// Gauges report absolute state, not a cumulative total, so the latest
        /// snapshot is forwarded as-is (not deltaed).
        #[test]
        fn gauges_are_forwarded_as_absolute_state() {
            let recorder = test_recorder();
            let id = "app::sql";

            let mut set = MetricsSet::new();
            set.push(Arc::new(Metric::new(
                MetricValue::Gauge {
                    name: "mem_used".into(),
                    gauge: create_gauge_with_value(42),
                },
                None,
            )));
            let d = recorder.subtree_delta_metric_values(id, &set);
            let gauge = d.iter().find_map(|v| match v {
                MetricValue::Gauge { name, gauge } if name == "df_mem_used" => {
                    Some(gauge.value() as u64)
                }
                _ => None,
            });
            assert_eq!(gauge, Some(42), "gauge is forwarded unchanged, not deltaed");
        }
    }
}
