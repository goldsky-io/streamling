use clap::Parser;
use std::fs::read_to_string;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use streamling::Streamling;
use streamling::app_config::AppConfig;
use streamling::error_format::{format_pretty_error, install_global_panic_hook};
use streamling::topology::PipelineTopology;
use streamling::validate::{CapturedLog, LogCaptureLayer, ValidationOutput};
use streamling_config::preprocessors::TopologyPreprocessor;
use streamling_core::error::{Result, ResultExt, StreamlingError};
use streamling_core::operators::inspect::LiveDataInspect;
use streamling_core::plugin::{
    build_plugin_preprocessors, load_and_initialize_plugins, terminate_all_plugins,
};
use tracing::info;
use tracing::log::warn;
use tracing_subscriber::EnvFilter;

mod initializations;
use initializations::{initialize_live_data_inspect, start_admin_api_server};
use streamling_common::logging::FlatJsonFormat;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Pipeline definition file; overrides pipeline_definition_location from config.
    pipeline_file: Option<String>,

    /// Config file base name or path (.yaml/.yml/.json auto-detected).
    #[arg(long, default_value = "config")]
    config: String,

    /// Build and validate the pipeline without running it (no data is processed).
    #[arg(long)]
    dry_run: bool,

    /// Validate the pipeline definition (implies --dry-run). Emits JSON results.
    #[arg(long)]
    validate: bool,
}

impl Cli {
    fn is_dry_run(&self) -> bool {
        self.dry_run || self.validate
    }
}

fn build_env_filter(default_level: &str) -> EnvFilter {
    let base = match std::env::var("RUST_LOG") {
        Ok(val) if !val.is_empty() => EnvFilter::from_default_env(),
        _ => EnvFilter::new(default_level),
    };
    base.add_directive("cranelift_codegen=off".parse().unwrap())
        .add_directive("wasmtime_cranelift=off".parse().unwrap())
        .add_directive("wasmtime=off".parse().unwrap())
        .add_directive("extism::plugin=off".parse().unwrap())
}

fn init_logging_standard(app_config: &AppConfig) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let failed = if app_config.log_format == "json" {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
                    .event_format(FlatJsonFormat),
            )
            .with(build_env_filter("info"))
            .try_init()
            .is_err()
    } else {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_thread_ids(true)
                    .with_thread_names(true),
            )
            .with(build_env_filter("info"))
            .try_init()
            .is_err()
    };
    if failed {
        eprintln!("Logger already initialized; skipping logging setup.");
    }
}

fn init_logging_capture(app_config: &AppConfig) -> Arc<Mutex<Vec<CapturedLog>>> {
    use tracing_subscriber::prelude::*;
    let capture_layer = LogCaptureLayer::new();
    let logs = capture_layer.logs();

    let base = tracing_subscriber::registry()
        .with(capture_layer)
        .with(build_env_filter("warn"));

    let init_result = if app_config.log_format == "json" {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
            .event_format(FlatJsonFormat);
        base.with(fmt_layer).try_init()
    } else {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_thread_ids(true)
            .with_thread_names(true);
        base.with(fmt_layer).try_init()
    };
    if init_result.is_err() {
        eprintln!("Logger already initialized; skipping logging setup.");
    }

    logs
}

async fn run_pipeline(
    pipeline_file: Option<String>,
    app_config: AppConfig,
    dry_run: bool,
) -> Result<()> {
    load_and_initialize_plugins(&app_config)?;

    let pipeline_definition_location =
        pipeline_file.unwrap_or_else(|| app_config.pipeline_definition_location.clone());
    let plugin_preprocessors = build_plugin_preprocessors(&app_config);
    let preprocessor = TopologyPreprocessor::new(plugin_preprocessors);
    let config_string = read_to_string(&pipeline_definition_location)
        .streamling_context("failed to read pipeline definition")?;
    streamling_core::topology_validation::validate_no_orphan_nodes(&config_string)?;
    let pipeline_topology = preprocessor
        .preprocess_topology(config_string)
        .await
        .streamling_context("failed to preprocess pipeline topology")?;
    let pipeline_topology = PipelineTopology::load_from_string(&pipeline_topology)?;
    streamling_core::topology_validation::validate_job_mode(
        app_config.job_mode,
        &pipeline_topology,
    )?;

    let telemetry_provider = if !dry_run {
        match streamling_core::telemetry::init_telemetry_provider(
            &app_config.application_id,
            &app_config.open_telemetry_metrics,
        ) {
            Ok(provider) => Some(provider),
            Err(error) => {
                warn!(
                    "Failed to initialize open telemetry metrics. Metrics will not be recorded and exported. Error: {:?}",
                    error
                );
                None
            }
        }
    } else {
        None
    };

    let live_data_inspect_enabled = !dry_run && app_config.live_data_inspect_enabled;
    let live_data_inspect = if live_data_inspect_enabled {
        Some(initialize_live_data_inspect(
            &app_config.application_id,
            app_config.live_data_inspect.clone(),
            &pipeline_topology,
        ))
    } else {
        None
    };

    let admin_api_handle = if let Some(live_data_inspect_instance) = live_data_inspect {
        Some(start_admin_api_server(
            app_config.admin_api_port,
            live_data_inspect_instance,
        ))
    } else {
        None
    };

    let streamling = Streamling::new(app_config, pipeline_topology);

    let result = streamling.start_with(dry_run).await;

    // Usually a no-op: the run loop already terminated and drained the
    // registry on its way out. Legacy per-plugin bound is fine here.
    terminate_all_plugins(None).unwrap();

    if let Some(handle) = admin_api_handle {
        info!("Shutting down Admin API server");
        handle.abort();
    }

    if live_data_inspect_enabled && !dry_run {
        let _ = LiveDataInspect::get_instance().shutdown().await;
    }

    if let Some(provider) = telemetry_provider {
        // Bounded: a dead/black-holed collector must not be able to stall
        // process exit on the final metric flush (
        // §6.1.4). force_flush is bounded per-export by the exporter's own
        // request timeouts; shutdown gets an explicit cap on top.
        let _ = provider.force_flush();
        let _ = provider.shutdown_with_timeout(std::time::Duration::from_secs(5));
    }
    // Independent of the cumulative provider above: flushes billing counts
    // even if a future code path initializes only the delta provider.
    streamling_core::telemetry::shutdown_delta_meter_provider();

    result
}

fn emit_validation_json(run_error: Option<&StreamlingError>, logs: &[CapturedLog]) -> ExitCode {
    let output = ValidationOutput::build(run_error, logs);
    let exit_code = if output.is_valid {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&output).expect("failed to serialize validation output")
    );
    exit_code
}

#[tokio::main]
async fn main() -> ExitCode {
    install_global_panic_hook();

    let cli = Cli::parse();
    let validate = cli.validate;
    let dry_run = cli.is_dry_run();

    let app_config = match AppConfig::load_from_path(&cli.config) {
        Ok(config) => config,
        Err(e) => {
            if validate {
                let se = StreamlingError::from(e);
                return emit_validation_json(Some(&se), &[]);
            }
            panic!("Failed to load config: {:#}", e);
        }
    };

    let captured_logs = if validate {
        Some(init_logging_capture(&app_config))
    } else {
        init_logging_standard(&app_config);
        None
    };

    let result = run_pipeline(cli.pipeline_file, app_config, dry_run).await;

    if let Err(ref e) = result {
        tracing::error!(
            target = "streamling",
            error.internal = e.is_internal(),
            error.retriable = e.is_retriable(),
            "{}",
            format_pretty_error(e),
        );
        if let Some(bt) = e.backtrace() {
            tracing::error!(target = "streamling", "backtrace:\n{:#}", bt);
        }
    }

    if validate {
        let logs_arc = captured_logs.expect("validate mode guarantees captured_logs is Some");
        let logs = logs_arc.lock().expect("captured logs mutex poisoned");
        let run_error = result.as_ref().err();
        return emit_validation_json(run_error, &logs);
    }

    if result.is_err() {
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use clap::{CommandFactory, Parser};

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn defaults_when_no_args() {
        let cli = Cli::try_parse_from(["streamling"]).unwrap();
        assert_eq!(cli.pipeline_file, None);
        assert_eq!(cli.config, "config");
        assert!(!cli.dry_run);
        assert!(!cli.validate);
        assert!(!cli.is_dry_run());
    }

    #[test]
    fn pipeline_file_is_captured_positionally() {
        let cli = Cli::try_parse_from(["streamling", "pipeline.yaml"]).unwrap();
        assert_eq!(cli.pipeline_file.as_deref(), Some("pipeline.yaml"));
    }

    #[test]
    fn config_flag_overrides_default() {
        let cli = Cli::try_parse_from(["streamling", "--config", "custom.yaml"]).unwrap();
        assert_eq!(cli.config, "custom.yaml");
    }

    #[test]
    fn dry_run_flag_enables_dry_run() {
        let cli = Cli::try_parse_from(["streamling", "--dry-run"]).unwrap();
        assert!(cli.dry_run);
        assert!(!cli.validate);
        assert!(cli.is_dry_run());
    }

    #[test]
    fn validate_implies_dry_run() {
        let cli = Cli::try_parse_from(["streamling", "--validate"]).unwrap();
        assert!(cli.validate);
        assert!(!cli.dry_run);
        assert!(cli.is_dry_run());
    }

    #[test]
    fn unknown_flag_is_rejected() {
        assert!(Cli::try_parse_from(["streamling", "--nope"]).is_err());
    }

    #[test]
    fn extra_positional_is_rejected() {
        assert!(Cli::try_parse_from(["streamling", "a.yaml", "b.yaml"]).is_err());
    }
}
