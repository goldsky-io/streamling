use abi_stable::std_types::RDuration;
use arrow::array::{Int32Builder, RecordBatch, StringBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use streamling_plugin::api::{SupportsGracefulShutdown, PluginStateBackendFactory, STREAMLING_COLUMN_NAME_OP};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::{CheckpointEpoch, PluginError, PluginInitializationError, SourcePlugin};
use tracing::debug;
use streamling_plugin::ffi::PluginMetricsRecorder;

#[allow(dead_code)]
pub struct RandomSource {
    rt: PluginAsyncRuntimeObj,
    metrics_recorder: PluginMetricsRecorder,
    schema: SchemaRef,
    options: HashMap<String, String>,
    running: Arc<AtomicBool>,
    max_rows: usize,
    record_batch_size: usize,
    batch_sleep_ms: u64,
    row_count: AtomicUsize,
}

impl RandomSource {
    pub fn new(
        rt: PluginAsyncRuntimeObj,
        _state_backend_factory: PluginStateBackendFactory,
        metrics_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        // Static schema for demo purposes
        let schema = Arc::new(Schema::new(vec![
            Field::new("num_field", DataType::Int32, false),
            Field::new("alphanumeric_field", DataType::Utf8, false),
            Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
        ]));

        let max_rows = options
            .get("max_rows")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(100); // Default to 100 rows if not specified

        let record_batch_size = options
            .get("record_batch_size")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10); // Default to 10 rows per batch if not specified

        let batch_sleep_ms = options
            .get("batch_sleep_ms")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1000); // Default to 1000 ms if not specified

        let running = Arc::new(AtomicBool::new(true));

        Ok(RandomSource {
            rt,
            metrics_recorder,
            schema,
            options,
            running,
            max_rows,
            record_batch_size,
            batch_sleep_ms,
            row_count: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl SupportsGracefulShutdown for RandomSource {
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
impl SourcePlugin for RandomSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.schema.clone())
    }

    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let start_at = Instant::now();
        if self.batch_sleep_ms > 0 {
            // Sleep for the specified duration before generating the batch
            self.rt.sleep(RDuration::from_millis(self.batch_sleep_ms)).await;
        }

        let current_count = self.row_count.load(Ordering::Relaxed);
        if current_count >= self.max_rows {
            // No more rows to generate, return an empty batch
            debug!("Record batch size: 0");
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let batch_size = self.record_batch_size;

        let mut rng = thread_rng();

        let mut builder = Int32Builder::new();
        for _ in 0..batch_size {
            builder.append_value(rng.gen_range(0..1000));
        }
        let num_field = builder.finish();

        let mut str_builder = StringBuilder::new();
        for _ in 0..batch_size {
            let random_str: String = std::iter::repeat_with(|| rng.sample(Alphanumeric))
                .map(char::from)
                .take(8)
                .collect();
            str_builder.append_value(&random_str);
        }
        let alphanumeric_field = str_builder.finish();

        let mut op_builder = StringBuilder::new();
        // Always append one update and one delete to the end of the batch
        for i in 0..batch_size {
            if i < batch_size - 2 {
                op_builder.append_value("i");
            } else if i < batch_size - 1 {
                op_builder.append_value("u");
            } else {
                op_builder.append_value("d");
            }
        }
        let op_field = op_builder.finish();

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(num_field),
                Arc::new(alphanumeric_field),
                Arc::new(op_field),
            ],
        )
        .unwrap();
        
        debug!("Record batch size: {:?}", batch.num_rows());
        
        // Update row_count atomically
        self.row_count
            .fetch_add(batch.num_rows(), Ordering::Relaxed);
        //emitting some make-up custom metrics
        self.metrics_recorder.record_count("src_plugin_custom_count", 40);
        self.metrics_recorder.record_latency_w_tags("src_plugin_custom_latency_w_tags", start_at.elapsed(), vec!(("tag1", "value2")));
        Ok(batch)
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
        Ok(())
    }
}
