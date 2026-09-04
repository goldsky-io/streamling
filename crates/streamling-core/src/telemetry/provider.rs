use crate::telemetry::recorder::set_metric_deny_patterns;
use crate::telemetry::types::set_global_metric_tags;
use anyhow::{Context, Result, bail};
use once_cell::sync::OnceCell;
use opentelemetry::metrics::MeterProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{
    HttpExporterBuilder, MetricExporter, MetricExporterBuilder, Protocol, TonicExporterBuilder,
    WithExportConfig, WithTonicConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use reqwest::Url;
use std::collections::BTreeMap;
use std::time::Duration;
use streamling_config::OpenTelemetryMetricsConfig;
use tracing::{error, info, trace};

// Shared test provider to avoid clobbering the process-wide global provider when
// multiple tests run concurrently. Selected when test configs include
// `service_instance_id` in global tags.
static SHARED_TEST_METER: OnceCell<SdkMeterProvider> = OnceCell::new();
// Shared test delta provider for delta metrics
static SHARED_TEST_DELTA_METER: OnceCell<SdkMeterProvider> = OnceCell::new();
// Delta provider for production mode
static DELTA_METER_PROVIDER: OnceCell<SdkMeterProvider> = OnceCell::new();
// Tracks whether we are in test metrics mode (shared provider without resource instance id).
static TEST_METRICS_MODE: OnceCell<bool> = OnceCell::new();

pub fn init_telemetry_provider(
    application_id: &str,
    open_telemetry_metrics_config: &OpenTelemetryMetricsConfig,
) -> Result<SdkMeterProvider> {
    // Initialize metric deny patterns
    let deny_patterns = if open_telemetry_metrics_config
        .metric_deny_list
        .trim()
        .is_empty()
    {
        vec![]
    } else {
        open_telemetry_metrics_config
            .metric_deny_list
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };
    trace!("metric_deny_patterns: {:?}", deny_patterns);
    set_metric_deny_patterns(deny_patterns);

    // Use app_config value first, fallback to OTEL_EXPORTER_OTLP_METRICS_ENDPOINT if config is "none" or empty
    let metrics_ingestion_endpoint = if open_telemetry_metrics_config.ingestion_endpoint == "none"
        || open_telemetry_metrics_config
            .ingestion_endpoint
            .trim()
            .is_empty()
    {
        std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
            .unwrap_or_else(|_| open_telemetry_metrics_config.ingestion_endpoint.clone())
    } else {
        open_telemetry_metrics_config.ingestion_endpoint.clone()
    };

    Url::parse(&metrics_ingestion_endpoint)
        .context("failed to parse metrics ingestion endpoint URL")?;

    let protocol_str = if open_telemetry_metrics_config
        .endpoint_protocol
        .trim()
        .is_empty()
    {
        std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
            .unwrap_or_else(|_| open_telemetry_metrics_config.endpoint_protocol.clone())
    } else {
        open_telemetry_metrics_config.endpoint_protocol.clone()
    };

    let record_batch_interval_secs = open_telemetry_metrics_config.batch_interval_secs;
    let protocol = match protocol_str.as_str() {
        "HttpJson" | "http/json" => Protocol::HttpJson,
        "HttpBinary" | "http/protobuf" => Protocol::HttpBinary,
        "Grpc" | "grpc" => Protocol::Grpc,
        _ => {
            bail!(
                "invalid endpoint protocol '{}', expected one of: HttpJson, HttpBinary, Grpc (or their OTEL standard variants: http/json, http/protobuf, grpc)",
                protocol_str
            );
        }
    };

    let service_instance_id = application_id;
    let parsed_tags = parse_tags(&open_telemetry_metrics_config.global_tags)?;
    let test_mode = parsed_tags
        .iter()
        .any(|kv| kv.key.as_str() == "service_instance_id");

    if test_mode {
        if let Some(existing) = SHARED_TEST_METER.get() {
            set_global_metric_tags(service_instance_id, parsed_tags);
            let _ = TEST_METRICS_MODE.set(true);
            return Ok(existing.clone());
        }
        // Tests: do not set resource-level instance id; isolate via metric/global tags
        let resource = Resource::builder().with_service_name("streamling").build();

        // Build cumulative reader
        let reader = build_periodic_reader_exporter(
            &metrics_ingestion_endpoint,
            record_batch_interval_secs,
            protocol,
        )?;

        // Create cumulative provider with only cumulative reader
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource.clone())
            .build();

        // Build delta reader
        let delta_reader = build_delta_periodic_reader_exporter(
            &metrics_ingestion_endpoint,
            record_batch_interval_secs,
            protocol,
        )?;

        // Create separate delta provider with only delta reader
        let delta_provider = SdkMeterProvider::builder()
            .with_reader(delta_reader)
            .with_resource(resource.clone())
            .build();

        info!(
            "[TEST] Telemetry: endpoint={} protocol={:?} global_tags={:?}, with separate delta provider",
            metrics_ingestion_endpoint, protocol, parsed_tags
        );
        set_global_metric_tags(service_instance_id, parsed_tags);
        global::set_meter_provider(provider.clone());
        let _ = SHARED_TEST_METER.set(provider.clone());
        let _ = SHARED_TEST_DELTA_METER.set(delta_provider);
        let _ = TEST_METRICS_MODE.set(true);

        Ok(provider)
    } else {
        // Prod/default: use resource-level instance id
        let resource = Resource::builder()
            .with_service_name("streamling")
            .with_attribute(KeyValue::new(
                "service.instance.id",
                service_instance_id.to_string(),
            ))
            .build();

        // Build cumulative reader
        let reader = build_periodic_reader_exporter(
            &metrics_ingestion_endpoint,
            record_batch_interval_secs,
            protocol,
        )?;

        // Create cumulative provider with only cumulative reader
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource.clone())
            .build();

        // Build delta reader
        let delta_reader = build_delta_periodic_reader_exporter(
            &metrics_ingestion_endpoint,
            record_batch_interval_secs,
            protocol,
        )?;

        // Create separate delta provider with only delta reader
        let delta_provider = SdkMeterProvider::builder()
            .with_reader(delta_reader)
            .with_resource(resource.clone())
            .build();

        info!(
            "Telemetry: endpoint={} protocol={:?} global_tags={:?} service_instance_id={}, with separate delta provider",
            metrics_ingestion_endpoint, protocol, parsed_tags, service_instance_id
        );
        set_global_metric_tags(service_instance_id, parsed_tags);
        global::set_meter_provider(provider.clone());
        let _ = DELTA_METER_PROVIDER.set(delta_provider);
        let _ = TEST_METRICS_MODE.set(false);

        Ok(provider)
    }
}

/// Flush and shut down the delta meter provider. Must be called on process
/// exit: the delta provider is not the global provider, so it is not covered
/// by the cumulative provider's shutdown — without this, jobs that finish
/// before the PeriodicReader's first tick lose their output_rows_delta
/// (billing) counts entirely.
pub fn shutdown_delta_meter_provider() {
    if let Some(provider) = DELTA_METER_PROVIDER.get() {
        info!("Shutting down delta telemetry meter provider");
        if let Err(e) = provider.force_flush() {
            error!(
                "Failed to flush delta meter provider; billing counts since the last export tick may be lost: {e}"
            );
        }
        if let Err(e) = provider.shutdown() {
            error!("Failed to shut down delta meter provider: {e}");
        }
    }
}

pub fn shutdown_test_meter_providers() {
    info!("Shutting down telemetry meter providers");
    if let Some(provider) = SHARED_TEST_METER.get() {
        let _ = provider.shutdown();
    }
    if let Some(provider) = SHARED_TEST_DELTA_METER.get() {
        let _ = provider.shutdown();
    }
    if let Some(provider) = DELTA_METER_PROVIDER.get() {
        let _ = provider.shutdown();
    }
}

/// Thin wrapper that logs export failures at error level.
/// The OTel SDK's PeriodicReader only logs export results at debug, making
/// failures invisible in production. This surfaces them without enabling
/// debug for the entire SDK.
struct LoggingExporter {
    inner: MetricExporter,
}

impl PushMetricExporter for LoggingExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let result = self.inner.export(metrics).await;
        if let Err(ref e) = result {
            error!(error = %e, "OTel metric export failed");
        }
        result
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

fn build_grpc_channel(endpoint: &str) -> Result<tonic::transport::Channel> {
    let channel = tonic::transport::Channel::from_shared(endpoint.to_owned())
        .context("invalid gRPC endpoint URI")?
        .timeout(Duration::from_secs(10))
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(Duration::from_secs(15)))
        .connect_lazy();
    Ok(channel)
}

fn build_periodic_reader_exporter(
    metrics_ingestion_endpoint: &str,
    record_batch_interval_secs: u32,
    protocol: Protocol,
) -> Result<PeriodicReader<LoggingExporter>> {
    let inner = match protocol {
        Protocol::Grpc => {
            let channel = build_grpc_channel(metrics_ingestion_endpoint)?;
            MetricExporterBuilder::<TonicExporterBuilder>::default()
                .with_tonic()
                .with_channel(channel)
                .build()
                .context("failed to build gRPC metric exporter")?
        }
        Protocol::HttpJson | Protocol::HttpBinary => {
            MetricExporterBuilder::<HttpExporterBuilder>::default()
                .with_http()
                .with_protocol(protocol)
                .with_endpoint(metrics_ingestion_endpoint.to_owned())
                .build()
                .context("failed to build HTTP metric exporter")?
        }
    };
    let reader = PeriodicReader::builder(LoggingExporter { inner })
        .with_interval(Duration::from_secs(record_batch_interval_secs as u64))
        .build();
    Ok(reader)
}

fn build_delta_periodic_reader_exporter(
    metrics_ingestion_endpoint: &str,
    record_batch_interval_secs: u32,
    protocol: Protocol,
) -> Result<PeriodicReader<LoggingExporter>> {
    let inner = match protocol {
        Protocol::Grpc => {
            let channel = build_grpc_channel(metrics_ingestion_endpoint)?;
            MetricExporterBuilder::<TonicExporterBuilder>::default()
                .with_tonic()
                .with_channel(channel)
                .with_temporality(Temporality::Delta)
                .build()
                .context("failed to build gRPC delta metric exporter")?
        }
        Protocol::HttpJson | Protocol::HttpBinary => {
            MetricExporterBuilder::<HttpExporterBuilder>::default()
                .with_http()
                .with_protocol(protocol)
                .with_endpoint(metrics_ingestion_endpoint.to_owned())
                .with_temporality(Temporality::Delta)
                .build()
                .context("failed to build HTTP delta metric exporter")?
        }
    };
    let reader = PeriodicReader::builder(LoggingExporter { inner })
        .with_interval(Duration::from_secs(record_batch_interval_secs as u64))
        .build();
    Ok(reader)
}

pub fn is_test_metrics_mode() -> bool {
    TEST_METRICS_MODE.get().copied().unwrap_or(false)
}

fn parse_tags(tags_str: &str) -> Result<Vec<KeyValue>> {
    if tags_str.trim().is_empty() {
        return Ok(vec![]);
    }

    let mut key_values = Vec::new();

    for tag_pair in tags_str.split(',') {
        let tag_pair = tag_pair.trim();
        if tag_pair.is_empty() {
            continue;
        }

        let parts: Vec<&str> = tag_pair.split(':').collect();
        if parts.len() != 2 {
            bail!("invalid tag format '{}', expected 'key:value'", tag_pair);
        }

        let key = parts[0].trim();
        let value = parts[1].trim();

        if key.is_empty() || value.is_empty() {
            bail!("tag key and value cannot be empty in '{}'", tag_pair);
        }
        key_values.push(KeyValue::new(key.to_string(), value.to_string()));
    }
    trace!("Parsed tags: {:?}", key_values);

    Ok(key_values)
}

pub fn metric_tags<const N: usize>(tags: [(&str, &str); N]) -> BTreeMap<String, String> {
    let tags: Vec<(String, String)> = tags
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect::<Vec<(String, String)>>();
    BTreeMap::from_iter(tags)
}

pub fn metric_key(app_id: &str, id: &str) -> String {
    format!("{}::{}", app_id, id)
}

/// Recover the plain topology node name from a `metric_key`-form identifier.
///
/// Keys take three shapes:
///   - `<app_id>::<node>`                 (regular node)
///   - `<app_id>::<node>::unbounded`      (hybrid source, unbounded phase)
///   - `<app_id>::<node>::bounded::<idx>` (hybrid source, bounded phase)
///
/// The hybrid phase suffix is stripped first; otherwise a hybrid key would yield
/// `unbounded` or the partition index instead of the node, mis-stamping
/// `downstream_id` and breaking PromQL joins.
pub fn get_reference_name_from_metric_key(metric_key: &str) -> String {
    let base = strip_hybrid_phase_suffix(metric_key);
    base.rsplit("::").next().unwrap_or(base).to_string()
}

/// Strip a hybrid-source phase suffix (`::unbounded` or `::bounded::<idx>`),
/// leaving `<app_id>::<node>`; returns the key unchanged otherwise.
///
/// A phase suffix is only appended to a full `<app_id>::<node>` key, so the base
/// must still contain `::`. That guard keeps a regular node literally named
/// `unbounded`/`bounded` (key `<app_id>::unbounded`) from being stripped to the
/// app id. (Assumes `app_id` itself contains no `::`; otherwise a multi-segment
/// app id with a node named `unbounded` is not distinguishable from a hybrid
/// phase key.)
fn strip_hybrid_phase_suffix(metric_key: &str) -> &str {
    if let Some(base) = metric_key.strip_suffix("::unbounded") {
        // Phase suffix only when the base is still `<app_id>::<node>`; else this
        // is a regular node literally named `unbounded`.
        if base.contains("::") {
            return base;
        }
    }
    if let Some((base, idx)) = metric_key.rsplit_once("::bounded::") {
        // Bounded-phase suffix only when the base is `<app_id>::<node>` and the
        // trailing segment is a partition index, so a node named `bounded` is safe.
        if base.contains("::") && !idx.is_empty() && idx.bytes().all(|b| b.is_ascii_digit()) {
            return base;
        }
    }
    metric_key
}

pub fn metric_key_hybrid_src_unbounded(app_id: &str, id: &str) -> String {
    format!("{}::unbounded", metric_key(app_id, id))
}

pub fn metric_key_hybrid_src_bounded(app_id: &str, id: &str, idx: usize) -> String {
    format!("{}::bounded::{}", metric_key(app_id, id), idx)
}

pub fn get_delta_meter() -> opentelemetry::metrics::Meter {
    trace!("Getting delta meter from dedicated delta provider");
    if is_test_metrics_mode() {
        if let Some(delta_provider) = SHARED_TEST_DELTA_METER.get() {
            return delta_provider.meter("execution_metrics_delta");
        }
    } else if let Some(delta_provider) = DELTA_METER_PROVIDER.get() {
        return delta_provider.meter("execution_metrics_delta");
    }
    // Fallback to global meter if delta provider not initialized yet
    // This should rarely happen in practice
    trace!("Delta provider not found, falling back to global meter");
    global::meter("execution_metrics_delta")
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_sdk::metrics::InMemoryMetricExporterBuilder;

    /// Must recover the plain node name for every key shape, including hybrid
    /// phase keys. Taking only the last `::` segment would yield `unbounded` or
    /// the partition index for hybrid keys, breaking `downstream_id`/PromQL joins.
    #[test]
    fn reference_name_recovers_plain_node_for_all_key_shapes() {
        let app = "app-7f3c";

        // Regular node key: `<app>::<node>`.
        assert_eq!(
            get_reference_name_from_metric_key(&metric_key(app, "kafka_source")),
            "kafka_source"
        );

        // Hybrid unbounded phase: `<app>::<node>::unbounded`.
        assert_eq!(
            get_reference_name_from_metric_key(&metric_key_hybrid_src_unbounded(app, "evm_blocks")),
            "evm_blocks"
        );

        // Hybrid bounded phase: `<app>::<node>::bounded::<idx>` — must return the
        // node name, never the partition index.
        assert_eq!(
            get_reference_name_from_metric_key(&metric_key_hybrid_src_bounded(
                app,
                "evm_blocks",
                0
            )),
            "evm_blocks"
        );
        assert_eq!(
            get_reference_name_from_metric_key(&metric_key_hybrid_src_bounded(
                app,
                "evm_blocks",
                12
            )),
            "evm_blocks"
        );

        // Robust even when the app id itself contains `::` separators.
        assert_eq!(
            get_reference_name_from_metric_key("tenant::app::evm_blocks::bounded::3"),
            "evm_blocks"
        );
    }

    /// A regular node named like a phase marker (`unbounded`/`bounded`) must
    /// still recover its own name. Its key is `<app>::<node>`, so the suffix is
    /// only stripped when the base still contains `::`; otherwise the app id
    /// would leak in as the node name.
    #[test]
    fn reference_name_does_not_strip_regular_node_named_like_phase_marker() {
        let app = "app-7f3c";

        assert_eq!(
            get_reference_name_from_metric_key(&metric_key(app, "unbounded")),
            "unbounded"
        );
        assert_eq!(
            get_reference_name_from_metric_key(&metric_key(app, "bounded")),
            "bounded"
        );
    }

    /// Boundary of the single-`::`-free-`app_id` assumption. For `app::bounded::42`
    /// the base (`app`) has no `::`, so the guard reads `::bounded::` as node data
    /// rather than a phase suffix and leaves the key unchanged; reference-name
    /// extraction then yields the trailing `42`, not `bounded::42`. This is a known
    /// limitation (a real bounded key is `<app>::<node>::bounded::<idx>`); the test
    /// pins the behavior so a future refactor can't silently change it.
    #[test]
    fn strip_hybrid_phase_suffix_untouched_for_single_segment_app_bounded_key() {
        assert_eq!(
            strip_hybrid_phase_suffix("app::bounded::42"),
            "app::bounded::42"
        );
        assert_eq!(get_reference_name_from_metric_key("app::bounded::42"), "42");
    }

    /// Regression test for short-lived jobs losing billing counts: delta
    /// measurements recorded after the last PeriodicReader tick must still be
    /// exported when shutdown_delta_meter_provider() runs on process exit.
    #[test]
    fn shutdown_delta_meter_provider_flushes_pending_counts() {
        let exporter = InMemoryMetricExporterBuilder::new()
            .with_temporality(Temporality::Delta)
            .build();
        // Interval far longer than the test: nothing exports unless shutdown flushes.
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(Duration::from_secs(3600))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        DELTA_METER_PROVIDER
            .set(provider)
            .expect("DELTA_METER_PROVIDER already set; no other test may initialize it");

        get_delta_meter()
            .u64_counter("streamling_output_rows_delta")
            .build()
            .add(42, &[]);

        shutdown_delta_meter_provider();

        let exported = exporter.get_finished_metrics().unwrap();
        assert!(
            !exported.is_empty(),
            "delta counts recorded before exit were not exported on shutdown"
        );
    }
}
