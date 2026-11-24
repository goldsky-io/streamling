//! End-to-end throughput benchmark for streamling.
//!
//! Drives the real streamling binary through `Kafka (Avro/CDC) → SQL transform →
//! blackhole sink` against the shared k3s stack, measures steady-state
//! throughput, and compares it to a committed per-runner baseline (report-only).
//!
//! It reuses the `streamling-e2e` harness for resource isolation, binary
//! execution, and Prometheus metric queries.
//!
//! Summary:
//! - Preload `--records` Avro messages into an isolated topic **once**.
//! - For each scenario, run the pipeline `--warmup + --iterations` times, each
//!   iteration with a fresh consumer group (re-reads the topic from `earliest`)
//!   and a distinct metrics instance (OTel counters are cumulative per process).
//! - Headline metric: input throughput `records / wall_clock`. Robust secondary:
//!   `compute_us_per_input_record` from `streamling_elapsed_compute_milliseconds_sum`,
//!   which excludes process startup and Kafka I/O wait.

mod report;
mod scenario;

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use streamling_e2e::resources::PrometheusResource;
use streamling_e2e::{init_tracing, PipelineOpts, TestContext, TestContextOptions};
use tracing::info;

use report::{compare, print_report};
use scenario::{Scenario, SCENARIOS};

/// Size of the pre-encoded payload pool cycled during bulk load. A multiple of
/// 10 so `sel_key = index % 10` yields exactly 10% selectivity when cycled.
const DISTINCT_PAYLOADS: u32 = 1_000;
const DEFAULT_RECORDS: u64 = 5_000_000;
const DEFAULT_ITERATIONS: u32 = 5;
const DEFAULT_WARMUP: u32 = 1;
const DEFAULT_REGRESSION_THRESHOLD: f64 = 0.10;
/// Time to let the final OTel batch reach Prometheus after a run exits. Matches
/// the 3s flush window the e2e metrics tests use with a 1s batch interval.
const METRICS_FLUSH_WAIT: Duration = Duration::from_secs(3);
/// Upper bound on a single pipeline run; a hang (e.g. an empty topic) fails
/// loudly instead of blocking forever.
const RUN_TIMEOUT: Duration = Duration::from_secs(600);

/// A deterministic scenario with selectivity `1 / n` may reach its final
/// required output up to `n - 1` source rows before the end of the topic.
/// Preserve that small tail while still rejecting materially incomplete runs.
fn maximum_source_row_shortfall(selectivity: f64) -> u64 {
    debug_assert!(selectivity > 0.0 && selectivity <= 1.0);
    ((1.0 / selectivity).ceil() as u64).saturating_sub(1)
}

/// Avro schema for the benchmark payload. Field types map to Arrow as:
/// long→int64, string→utf8, double→float64, int→int32, and the `bytes` decimal
/// logical type → Decimal128(20, 4) (precision ≤ 38).
const BENCH_SCHEMA: &str = r#"{
    "type": "record",
    "name": "BenchRecord",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "user_id", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "country", "type": "string"},
        {"name": "device", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 4}},
        {"name": "ts", "type": "long"},
        {"name": "sel_key", "type": "int"}
    ]
}"#;

const COUNTRIES: &[&str] = &["US", "CA", "GB", "DE", "FR", "JP", "BR", "IN", "AU", "NL"];
const DEVICES: &[&str] = &["ios", "android", "web", "desktop"];

#[derive(Debug, Serialize)]
struct BenchRecord {
    id: i64,
    user_id: String,
    email: String,
    country: String,
    device: String,
    amount: f64,
    /// Avro decimal, encoded as the big-endian two's-complement bytes of the
    /// unscaled integer. `serde_bytes` forces Avro `bytes` (an int array would
    /// not resolve to the decimal logical type).
    #[serde(with = "serde_bytes")]
    price: Vec<u8>,
    ts: i64,
    /// Drives deterministic filter selectivity: `WHERE sel_key = 0` matches 10%.
    sel_key: i32,
}

fn generate_record(index: u64) -> BenchRecord {
    let user_num = index % 100_000;
    // Unscaled integer for the `price` decimal; the schema's scale of 4 makes
    // this ~0.0000..9999.9999. The generator is scale-agnostic — the bytes are
    // just a big-endian integer, and the schema owns the interpretation.
    let price_unscaled = (index % 100_000_000) as i128;
    BenchRecord {
        id: index as i64,
        user_id: format!("user_{}", user_num),
        email: format!("user_{}@example.com", user_num),
        country: COUNTRIES[(index % COUNTRIES.len() as u64) as usize].to_string(),
        device: DEVICES[(index % DEVICES.len() as u64) as usize].to_string(),
        amount: (index % 1_000) as f64 + 0.5,
        price: price_unscaled.to_be_bytes().to_vec(),
        ts: 1_700_000_000 + index as i64,
        sel_key: (index % 10) as i32,
    }
}

#[derive(Parser, Debug)]
#[command(about = "Streamling end-to-end throughput benchmark (report-only)")]
struct Cli {
    /// Scenario name to run; omit to run every scenario.
    #[arg(long)]
    scenario: Option<String>,

    /// Source records preloaded and consumed per iteration.
    #[arg(long, default_value_t = DEFAULT_RECORDS)]
    records: u64,

    /// Measured iterations (excludes warmup).
    #[arg(long, default_value_t = DEFAULT_ITERATIONS)]
    iterations: u32,

    /// Warmup iterations, discarded from the aggregate.
    #[arg(long, default_value_t = DEFAULT_WARMUP)]
    warmup: u32,

    /// Tokio worker threads for each Streamling child process. This does not
    /// resize the benchmark harness runtime.
    #[arg(long)]
    tokio_worker_threads: Option<NonZeroUsize>,

    /// Directory to write `<scenario>.json` result files (skipped if unset).
    #[arg(long)]
    out_dir: Option<PathBuf>,

    /// Directory holding `<scenario>.json` baselines. Defaults to
    /// `bench/baselines/<runner_label>`.
    #[arg(long)]
    baseline_dir: Option<PathBuf>,

    /// Regression flagged when input throughput drops (or compute time rises)
    /// by more than this fraction versus the baseline.
    #[arg(long, default_value_t = DEFAULT_REGRESSION_THRESHOLD)]
    regression_threshold: f64,

    /// Write current results as the new baseline instead of comparing.
    #[arg(long)]
    update_baseline: bool,
}

/// Raw measurements for one non-warmup Streamling process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchSample {
    pub instance: String,
    pub wall_clock_seconds: f64,
    pub throughput_input_records_per_sec: f64,
    pub throughput_output_records_per_sec: f64,
    pub throughput_mb_per_sec: f64,
    pub compute_us_per_input_record: f64,
    pub source_output_rows_observed: u64,
}

/// One benchmark result, serialized to JSON and used as the baseline schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResult {
    pub scenario: String,
    pub format: String,
    pub selectivity: f64,
    pub records: u64,
    pub payload_bytes: u64,
    pub iterations: u32,
    /// Explicit child runtime size, when supplied through the benchmark CLI.
    /// Defaults preserve compatibility with baselines written before this knob.
    #[serde(default)]
    pub tokio_worker_threads: Option<usize>,
    /// Per-process measurements used to compute the aggregate fields below.
    /// Older committed baselines contain only aggregates and deserialize to an
    /// empty sample list.
    #[serde(default)]
    pub samples: Vec<BenchSample>,
    pub wall_clock_seconds_median: f64,
    pub wall_clock_seconds_min: f64,
    pub input_rows: u64,
    pub throughput_input_records_per_sec_median: f64,
    pub throughput_output_records_per_sec_median: f64,
    pub throughput_mb_per_sec_median: f64,
    pub compute_us_per_input_record_median: f64,
    /// Rows the source emitted, from Prometheus — a sanity check that the source
    /// read the whole topic. Filtering happens in the downstream transform, so
    /// this should be ~records for every scenario, not the post-filter count.
    pub source_output_rows_observed: u64,
    pub runner_label: String,
    pub version: String,
    pub timestamp_unix_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let runner_label = std::env::var("BENCH_RUNNER_LABEL").unwrap_or_else(|_| "local".to_string());
    let version = std::env::var("BENCH_GIT_SHA").unwrap_or_else(|_| "dev".to_string());
    let baseline_dir = cli
        .baseline_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("bench/baselines/{}", runner_label)));

    let scenarios = select_scenarios(cli.scenario.as_deref())?;

    let ctx = TestContext::with_options(TestContextOptions::new().with_prometheus())
        .await
        .context(
            "failed to create test context (is the k3s stack up and are E2E_* env vars set?)",
        )?;
    if ctx.prometheus.is_none() {
        anyhow::bail!("Prometheus is not configured (E2E_PROMETHEUS_URL must be set)");
    }

    ctx.kafka
        .register_schema(BENCH_SCHEMA)
        .await
        .context("failed to register Avro schema")?;

    info!(
        "Preloading {} Avro records into topic {}",
        cli.records, ctx.kafka_topic
    );
    let payload_bytes = ctx
        .kafka
        .produce_avro_bulk(cli.records, DISTINCT_PAYLOADS, generate_record)
        .await
        .context("failed to preload Kafka topic")?;

    let mut any_regression = false;
    for scenario in scenarios {
        let result = run_scenario(&ctx, scenario, &cli, payload_bytes, &runner_label, &version)
            .await
            .with_context(|| format!("scenario '{}' failed", scenario.name))?;

        if let Some(dir) = &cli.out_dir {
            write_json(&dir.join(format!("{}.json", scenario.name)), &result)?;
        }

        let baseline_path = baseline_dir.join(format!("{}.json", scenario.name));
        if cli.update_baseline {
            write_json(&baseline_path, &result)?;
            info!("Wrote baseline {}", baseline_path.display());
            print_report(&result, &[]);
        } else {
            let baseline = load_baseline(&baseline_path)?;
            let comparison = match &baseline {
                Some(b) => compare(&result, b, cli.regression_threshold),
                None => Vec::new(),
            };
            any_regression |= comparison.iter().any(|c| c.regressed);
            print_report(&result, &comparison);
            if baseline.is_none() {
                println!(
                    "  (no baseline at {} — run `just bench-update-baseline` to seed it)",
                    baseline_path.display()
                );
            }
        }
    }

    // Report-only: a regression is surfaced but never fails the process.
    if any_regression {
        println!("\n⚠️  One or more metrics regressed beyond the threshold (report-only).");
    }
    Ok(())
}

fn select_scenarios(name: Option<&str>) -> Result<Vec<&'static Scenario>> {
    match name {
        None => Ok(SCENARIOS.iter().collect()),
        Some(n) => {
            let scenario = SCENARIOS.iter().find(|s| s.name == n).with_context(|| {
                let names: Vec<&str> = SCENARIOS.iter().map(|s| s.name).collect();
                format!("unknown scenario '{}'; available: {}", n, names.join(", "))
            })?;
            Ok(vec![scenario])
        }
    }
}

async fn run_scenario(
    ctx: &TestContext,
    scenario: &Scenario,
    cli: &Cli,
    payload_bytes: u64,
    runner_label: &str,
    version: &str,
) -> Result<BenchResult> {
    let prometheus = ctx.prometheus.as_ref().expect("prometheus checked in main");
    let stop = (scenario.selectivity * cli.records as f64).ceil() as u64;

    let pipeline_yaml = format!(
        r#"
sources:
  kafka_source:
    type: kafka
    topic: {topic}
    starting_offsets: earliest
    primary_key: id

transforms:
  projected:
    type: sql
    primary_key: id
    sql: "{sql}"

sinks:
  sink:
    type: blackhole
    from: projected
"#,
        topic = ctx.kafka_topic,
        sql = scenario.sql,
    );

    let short_id = &ctx.test_id[..8];
    let total_iters = cli.warmup + cli.iterations;
    let mut measured: Vec<(String, f64)> = Vec::new();

    for iter in 0..total_iters {
        // Distinct per iteration so (a) OTel counters don't accumulate across
        // runs and (b) a fresh consumer group re-reads the topic from earliest.
        let instance = format!("bench-{}-{}-{}", scenario.name, short_id, iter);
        let is_warmup = iter < cli.warmup;
        info!(
            "scenario={} iter={}/{} ({}) stop={} instance={}",
            scenario.name,
            iter + 1,
            total_iters,
            if is_warmup { "warmup" } else { "measured" },
            stop,
            instance,
        );

        let mut pipeline_opts = PipelineOpts::new()
            .record_limit(stop)
            .timeout(RUN_TIMEOUT)
            .env("STREAMLING__APPLICATION_ID", instance.clone())
            .env(
                "STREAMLING__KAFKA_SOURCE__CONSUMER_GROUP_ID",
                instance.clone(),
            );
        if let Some(worker_threads) = cli.tokio_worker_threads {
            pipeline_opts = pipeline_opts.env("TOKIO_WORKER_THREADS", worker_threads.to_string());
        }

        let start = Instant::now();
        ctx.run_pipeline_with_opts(&pipeline_yaml, pipeline_opts)
            .await
            .with_context(|| format!("pipeline run failed (iter {})", iter))?;
        let wall = start.elapsed().as_secs_f64();

        if !is_warmup {
            measured.push((instance, wall));
        }
    }

    tokio::time::sleep(METRICS_FLUSH_WAIT).await;

    let mut samples = Vec::with_capacity(measured.len());
    let mut source_rows_observed = 0u64;

    for (instance, wall) in &measured {
        let compute_query = format!(
            "sum(streamling_elapsed_compute_milliseconds_sum{{instance=\"{}\"}})",
            instance
        );
        let compute_ms = prometheus
            .query(&compute_query)
            .await?
            .with_context(|| format!("missing compute metric for measured instance {instance}"))?;
        let source_query = PrometheusResource::output_rows_query("kafka_source", Some(instance));
        let src_rows = prometheus
            .query_count(&source_query)
            .await?
            .with_context(|| {
                format!("missing source-row metric for measured instance {instance}")
            })?;
        let minimum_source_rows = cli
            .records
            .saturating_sub(maximum_source_row_shortfall(scenario.selectivity));
        if src_rows < minimum_source_rows {
            anyhow::bail!(
                "source-row sanity check failed for {instance}: observed {src_rows}, expected at least {minimum_source_rows}"
            );
        }
        source_rows_observed = source_rows_observed.max(src_rows);

        let input_tps = cli.records as f64 / wall;
        samples.push(BenchSample {
            instance: instance.clone(),
            wall_clock_seconds: *wall,
            throughput_input_records_per_sec: input_tps,
            throughput_output_records_per_sec: stop as f64 / wall,
            throughput_mb_per_sec: input_tps * payload_bytes as f64 / 1e6,
            compute_us_per_input_record: compute_ms * 1e3 / cli.records as f64,
            source_output_rows_observed: src_rows,
        });
    }

    let walls: Vec<f64> = samples
        .iter()
        .map(|sample| sample.wall_clock_seconds)
        .collect();
    let input_tps: Vec<f64> = samples
        .iter()
        .map(|sample| sample.throughput_input_records_per_sec)
        .collect();
    let output_tps: Vec<f64> = samples
        .iter()
        .map(|sample| sample.throughput_output_records_per_sec)
        .collect();
    let compute_us: Vec<f64> = samples
        .iter()
        .map(|sample| sample.compute_us_per_input_record)
        .collect();
    let input_tps_median = report::median(&input_tps);

    Ok(BenchResult {
        scenario: scenario.name.to_string(),
        format: "avro".to_string(),
        selectivity: scenario.selectivity,
        records: cli.records,
        payload_bytes,
        iterations: cli.iterations,
        tokio_worker_threads: cli.tokio_worker_threads.map(NonZeroUsize::get),
        samples,
        wall_clock_seconds_median: report::median(&walls),
        wall_clock_seconds_min: walls.iter().copied().fold(f64::INFINITY, f64::min),
        input_rows: cli.records,
        throughput_input_records_per_sec_median: input_tps_median,
        throughput_output_records_per_sec_median: report::median(&output_tps),
        throughput_mb_per_sec_median: input_tps_median * payload_bytes as f64 / 1e6,
        compute_us_per_input_record_median: report::median(&compute_us),
        source_output_rows_observed: source_rows_observed,
        runner_label: runner_label.to_string(),
        version: version.to_string(),
        timestamp_unix_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    })
}

fn load_baseline(path: &Path) -> Result<Option<BenchResult>> {
    if !path.exists() {
        return Ok(None);
    }
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read baseline {}", path.display()))?;
    let result = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse baseline {}", path.display()))?;
    Ok(Some(result))
}

fn write_json(path: &Path, result: &BenchResult) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(result)?;
    std::fs::write(path, json).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_baseline_defaults_new_measurement_fields() {
        let mut value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../bench/baselines/blacksmith-8vcpu/avro_cdc_projection.json"
        ))
        .expect("committed baseline must be valid JSON");
        let object = value
            .as_object_mut()
            .expect("committed baseline must be a JSON object");
        object.remove("tokio_worker_threads");
        object.remove("samples");

        let result: BenchResult =
            serde_json::from_value(value).expect("legacy baseline must remain readable");

        assert_eq!(result.tokio_worker_threads, None);
        assert!(result.samples.is_empty());
    }

    #[test]
    fn worker_thread_count_must_be_nonzero() {
        let parsed = Cli::try_parse_from(["streamling-bench", "--tokio-worker-threads", "0"]);
        assert!(parsed.is_err());
    }

    #[test]
    fn source_row_sanity_allows_only_the_final_filter_stride() {
        assert_eq!(maximum_source_row_shortfall(1.0), 0);
        assert_eq!(maximum_source_row_shortfall(0.1), 9);
    }
}
