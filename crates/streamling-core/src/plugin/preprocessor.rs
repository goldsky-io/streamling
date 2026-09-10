use crate::app_config::AppConfig;
use abi_stable::derive_macro_reexports::NonExhaustive;
use abi_stable::traits::IntoReprC;
use streamling_config::preprocessors::{Preprocessor, PreprocessorError};
pub use streamling_plugin::PluginMsg;
use streamling_plugin::{PluginChannel, PluginChannels};
use tracing::warn;

use super::{create_preprocessor_plugin, registered_plugin_ids};

pub struct PluginPreprocessorAdapter {
    app_config: AppConfig,
    plugin_type: String,
}

impl PluginPreprocessorAdapter {
    pub fn new(app_config: AppConfig, plugin_type: String) -> Self {
        Self {
            app_config,
            plugin_type,
        }
    }
}

#[async_trait::async_trait]
impl Preprocessor for PluginPreprocessorAdapter {
    async fn preprocess_topology(
        &self,
        config: String,
    ) -> std::result::Result<String, PreprocessorError> {
        let reference_name = format!("preprocessor_{}", self.plugin_type);
        let plugin = create_preprocessor_plugin(
            &self.app_config,
            reference_name,
            self.plugin_type.clone(),
            self.app_config
                .plugin
                .preprocessor_options
                .get(&self.plugin_type)
                .cloned()
                .unwrap_or_default(),
        )
        .map_err(|e| PreprocessorError::PluginError(format!("{}", e)))?;

        // Spawn the execution future so the dispatcher starts running immediately.
        // Sanctioned: structured concurrency — `execution_handle` is awaited
        // at the end of this function (runs pre-pipeline, no controller yet).
        #[allow(clippy::disallowed_methods)]
        let execution_handle = tokio::spawn(plugin.execution_future);

        // Drop the channel halves we don't use. The host only sends on input and
        // receives on output. Keeping our copy of output.sender alive would prevent
        // recv() from returning RecvError if the plugin panics, causing a deadlock.
        let PluginChannels {
            input,
            output,
            metrics: _,
        } = plugin.channels;
        let PluginChannel {
            sender: input_sender,
            receiver: _,
        } = input;
        let PluginChannel {
            sender: _,
            receiver: output_receiver,
        } = output;

        // Send Topology config. Bounded: the channel is freshly created, so a
        // refused send means the dispatcher never started consuming — error
        // out instead of parking startup (§1.2).
        input_sender
            .send_timeout(
                NonExhaustive::new(PluginMsg::Topology {
                    config: config.into_c(),
                }),
                std::time::Duration::from_secs(5),
            )
            .map_err(|e| {
                PreprocessorError::PluginError(format!("Failed to send Topology: {}", e))
            })?;

        // Wait for the Topology response, bounded: a preprocessor that never
        // responds (bug, wedged API call inside the plugin) used to stall
        // startup forever — and no signal handling exists yet at this point,
        // so SIGTERM was unobservable.
        // Generous bound: dataset preprocessors legitimately make API calls.
        const TOPOLOGY_RESPONSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
        let response = output_receiver
            .recv_timeout(TOPOLOGY_RESPONSE_TIMEOUT)
            .map_err(|e| {
                PreprocessorError::PluginError(format!(
                    "Failed to receive topology response from preprocessor '{}' within {:?}: {}",
                    self.plugin_type, TOPOLOGY_RESPONSE_TIMEOUT, e
                ))
            })?;

        let result_config = match response.into_enum() {
            Ok(PluginMsg::Topology { config }) => config.into_string(),
            Ok(PluginMsg::Error { message }) => {
                return Err(PreprocessorError::PluginError(message.into_string()));
            }
            Ok(other) => {
                return Err(PreprocessorError::PluginError(format!(
                    "Expected Topology response, got: {:?}",
                    other
                )));
            }
            Err(_) => {
                return Err(PreprocessorError::PluginError(
                    "Malformed message wrapper".to_string(),
                ));
            }
        };

        // Send Terminate. Bounded: a preprocessor wedged mid-Topology with a
        // full input channel must not park startup forever (§1.2) — the
        // execution-future await below has its own error path.
        let _ = input_sender.send_timeout(
            NonExhaustive::new(PluginMsg::Terminate),
            std::time::Duration::from_secs(5),
        );

        // Await execution future
        execution_handle
            .await
            .map_err(|e| {
                PreprocessorError::PluginError(format!("Plugin execution panicked: {}", e))
            })?
            .map_err(|e| {
                PreprocessorError::PluginError(format!("Plugin execution failed: {}", e))
            })?;

        Ok(result_config)
    }
}

/// Builds registered preprocessors in configured order.
///
/// Unknown ids are skipped so shared configuration remains compatible with
/// older plugin bundles. The warning keeps the mismatch visible to callers.
pub fn build_plugin_preprocessors(app_config: &AppConfig) -> Vec<Box<dyn Preprocessor>> {
    let registered = registered_plugin_ids();
    let (available, skipped) =
        split_available_preprocessor_ids(&app_config.plugin.preprocessor_ids, &registered);

    for id in &skipped {
        warn!(
            preprocessor_id = %id,
            "Preprocessor '{}' is not registered by any loaded plugin; skipping it. Registered plugin ids: [{}]",
            id,
            registered.join(", ")
        );
    }

    available
        .into_iter()
        .map(|id| {
            Box::new(PluginPreprocessorAdapter::new(app_config.clone(), id))
                as Box<dyn Preprocessor>
        })
        .collect()
}

/// Partitions ids without changing their configured order.
fn split_available_preprocessor_ids(
    configured: &[String],
    registered: &[String],
) -> (Vec<String>, Vec<String>) {
    configured
        .iter()
        .cloned()
        .partition(|id| registered.contains(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    #[test]
    fn unregistered_ids_are_skipped_and_resolved_order_is_preserved() {
        let (available, skipped) = split_available_preprocessor_ids(
            &ids(&[
                "first_preprocessor",
                "new_optional_preprocessor",
                "last_preprocessor",
            ]),
            &ids(&["last_preprocessor", "first_preprocessor"]),
        );

        assert_eq!(
            available,
            ids(&["first_preprocessor", "last_preprocessor"]),
            "resolved ids must keep their configured relative order"
        );
        assert_eq!(skipped, ids(&["new_optional_preprocessor"]));
    }

    #[test]
    fn empty_config_builds_nothing() {
        let (available, skipped) = split_available_preprocessor_ids(&[], &ids(&["some_expander"]));
        assert!(available.is_empty());
        assert!(skipped.is_empty());
    }

    /// Regression: an id no loaded plugin provides used to be instantiated
    /// anyway and then fail the whole pipeline at startup.
    #[test]
    fn build_with_no_plugins_loaded_skips_every_configured_id() {
        let mut app_config = AppConfig::load().expect("embedded config must load");
        app_config.plugin.preprocessor_ids = ids(&["new_optional_preprocessor"]);

        assert!(build_plugin_preprocessors(&app_config).is_empty());
    }
}
