use arrow::array::RecordBatch;
use arrow::util::pretty::print_batches;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant};
use abi_stable::std_types::RDuration;
use streamling_plugin::{CheckpointEpoch, PluginError, PluginStateBackend, SinkPlugin};
use streamling_plugin::api::{SupportsGracefulShutdown, PluginStateBackendFactory};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use tracing::{debug, info};
use streamling_plugin::ffi::PluginMetricsRecorder;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PrintSinkState {
    current_epoch: u64,
}

pub struct PrintSink {
    rt: PluginAsyncRuntimeObj,
    schema: SchemaRef,
    options: HashMap<String, String>,
    running: Arc<AtomicBool>,
    state_backend: Arc<PluginStateBackend<PrintSinkState>>,
    metrics_recorder: PluginMetricsRecorder,
}

impl PrintSink {
    pub fn new(
        schema: SchemaRef,
        rt: PluginAsyncRuntimeObj,
        state_backend_factory: PluginStateBackendFactory,
        metrics_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Self {
        let state_backend = state_backend_factory.create();
        let running = Arc::new(AtomicBool::new(true));

        PrintSink {
            rt,
            schema,
            options,
            running,
            state_backend,
            metrics_recorder,
        }
    }
}

#[async_trait]
impl SupportsGracefulShutdown for PrintSink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    async fn terminate(&self) -> Result<(), PluginError> {
        debug!("Terminating...");

        self.running.store(false, Ordering::SeqCst);

        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for PrintSink {
    async fn initialize(&self) -> Result<(), PluginError> {
        info!("Initializing PrintSink with schema: {:?}", self.schema);

        // Not really useful, just a demonstration of how to use the state backend
        let saved_state = self.state_backend.get().await.map_err(PluginError::State)?;
        info!("Saved state: {:?}", saved_state);

        Ok(())
    }

    async fn process_batch(&self, batch: RecordBatch) -> Result<(), PluginError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let start_at = Instant::now();

        debug!("Received batch with {} rows", batch.num_rows());
        
        // Demonstration of how to use the options
        match self.options.get("mode") {
            Some(mode) if mode == "batch" => {
                print_batches(&[batch]).map_err(|e| PluginError::ArrowError(e))?;
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
        //adding some artificial latency
        self.rt.sleep(RDuration::from_millis(50)).await;
        //emitting some make-up custom metrics
        self.metrics_recorder.record_count("sink_plugin_custom_count", 40);
        self.metrics_recorder.record_latency_w_tags("sink_plugin_custom_latency_w_tags", start_at.elapsed(), vec!(("tag1", "value2")));
        Ok(())
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        debug!("Received epoch marker: {}", epoch.0);

        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        debug!("Received epoch finalizer: {}", epoch.0);

        // Not really useful, just a demonstration of how to use the state backend
        self.state_backend
            .put(PrintSinkState {
                current_epoch: epoch.0,
            })
            .await
            .map_err(PluginError::State)?;

        Ok(())
    }
}
