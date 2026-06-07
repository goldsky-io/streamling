use crate::app_config::AppConfig;
use abi_stable::derive_macro_reexports::NonExhaustive;
use abi_stable::traits::IntoReprC;
use streamling_config::preprocessors::{Preprocessor, PreprocessorError};
pub use streamling_plugin::PluginMsg;
use streamling_plugin::{PluginChannel, PluginChannels};

use super::create_preprocessor_plugin;

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

        // Send Topology config
        input_sender
            .send(NonExhaustive::new(PluginMsg::Topology {
                config: config.into_c(),
            }))
            .map_err(|e| {
                PreprocessorError::PluginError(format!("Failed to send Topology: {}", e))
            })?;

        // Wait for Topology response
        let response = output_receiver.recv().map_err(|e| {
            PreprocessorError::PluginError(format!("Failed to receive topology response: {}", e))
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

        // Send Terminate
        let _ = input_sender.send(NonExhaustive::new(PluginMsg::Terminate));

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

pub fn build_plugin_preprocessors(app_config: &AppConfig) -> Vec<Box<dyn Preprocessor>> {
    app_config
        .plugin
        .preprocessor_ids
        .iter()
        .map(|id| {
            Box::new(PluginPreprocessorAdapter::new(
                app_config.clone(),
                id.clone(),
            )) as Box<dyn Preprocessor>
        })
        .collect()
}
