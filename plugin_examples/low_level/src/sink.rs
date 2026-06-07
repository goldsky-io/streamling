use abi_stable::derive_macro_reexports::NonExhaustive;
use abi_stable::traits::IntoReprC;
use arrow::array::RecordBatch;
use arrow::util::pretty::print_batches;
use arrow_schema::SchemaRef;
use datafusion::common::DataFusionError;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use streamling_plugin::{PluginChannels, PluginMsg};
use tracing::{debug, error, info};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;

pub struct PrintSink {
    schema: SchemaRef,
    options: HashMap<String, String>,
    running: Arc<AtomicBool>,
    channels: PluginChannels,
}

impl PrintSink {
    pub fn new(
        schema: SchemaRef,
        options: HashMap<String, String>,
        channels: PluginChannels,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        PrintSink {
            schema,
            options,
            running,
            channels,
        }
    }

    pub async fn start(&self, runtime: PluginAsyncRuntimeObj) -> Result<(), DataFusionError> {
        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            if self.channels.input.receiver.is_empty() {
                runtime.sleep(Duration::from_millis(100).into_c()).await;
                continue;
            }
            match self.channels.input.receiver.recv().map(|m| m.into_enum()) {
                Ok(Ok(PluginMsg::Init)) => {
                    info!("Starting PrintSink with schema: {:?}", self.schema);
                }
                Ok(Ok(PluginMsg::NextBatch { data })) => {
                    let batch: RecordBatch = data.into();

                    if batch.num_rows() > 0 {
                        debug!("Received batch with {} rows", batch.num_rows());

                        // Demonstration of how to use the options
                        match self.options.get("mode") {
                            Some(mode) if mode == "batch" => {
                                print_batches(&[batch.clone()])?;
                            }
                            _ => {
                                for i in 0..batch.num_rows() {
                                    // TODO: this is not very useful, needs a proper converter / formatter
                                    let row = batch.slice(i, 1);
                                    let str = format!("{:?}", row);
                                    info!(str);
                                }
                            }
                        }
                    }
                }
                Ok(Ok(PluginMsg::CheckpointMarker { epoch })) => {
                    debug!("Received epoch marker: {}", epoch.0);

                    self.channels
                        .output
                        .sender
                        .send(NonExhaustive::new(PluginMsg::CheckpointAck { epoch }))
                        .expect("Failed to send checkpoint ack");
                }
                Ok(Ok(PluginMsg::CheckpointFinalizer { epoch })) => {
                    debug!("Received epoch finalizer: {:?}", epoch.0);
                }
                Ok(Ok(PluginMsg::Terminate)) => {
                    self.running.store(false, Ordering::SeqCst);
                }
                Err(e) => {
                    error!("Error receiving message from input channel: {:?}", e);
                    return Err(DataFusionError::Execution(format!(
                        "Error receiving message from input channel: {:?}",
                        e
                    )));
                }
                _ => {}
            }
        }

        Ok(())
    }
}
