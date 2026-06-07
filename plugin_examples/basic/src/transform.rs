use arrow::array::{Array, BooleanArray, RecordBatch, StringArray};
use arrow::compute::{and, filter_record_batch};
use arrow::datatypes::DataType;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use streamling_plugin::api::{SupportsGracefulShutdown, PluginStateBackendFactory};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::{CheckpointEpoch, PluginError, TransformPlugin};
use tracing::{debug, info};
use streamling_plugin::ffi::PluginMetricsRecorder;

pub struct FilterTransform {
    schema: SchemaRef,
    options: HashMap<String, String>,
    running: Arc<AtomicBool>,
    metrics_recorder: PluginMetricsRecorder
}

impl FilterTransform {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state_backend_factory: PluginStateBackendFactory,
        metrics_recorder: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        FilterTransform { schema, options, running, metrics_recorder }
    }
}

#[async_trait]
impl SupportsGracefulShutdown for FilterTransform {
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
impl TransformPlugin for FilterTransform {
    async fn initialize(&self) -> Result<(), PluginError> {
        info!(
            "Initializing FilterTransform with schema: {:?}",
            self.schema
        );
        info!("Filter conditions: {:?}", self.options);
        Ok(())
    }

    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.schema.clone())
    }

    async fn process_batch(&self, batch: RecordBatch) -> Result<RecordBatch, PluginError> {
        if batch.num_rows() == 0 {
            return Ok(batch);
        }
        let start_at = Instant::now();

        debug!("Processing batch with {} rows", batch.num_rows());

        // If no filter conditions are specified, return the original batch
        if self.options.is_empty() {
            return Ok(batch);
        }

        // Start with all rows selected
        let mut mask = BooleanArray::from(vec![true; batch.num_rows()]);

        // Apply each filter condition
        for (column_name, expected_value) in &self.options {
            if let Ok(column_idx) = batch.schema().index_of(column_name) {
                let column = batch.column(column_idx);
                let column_data_type = column.data_type();

                // Only support string columns, ignore others
                if let DataType::Utf8 = column_data_type {
                    let string_array =
                        column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                PluginError::Execution(format!(
                                    "Failed to cast column {} to StringArray",
                                    column_name
                                ))
                            })?;

                    let mut filter_values = vec![false; batch.num_rows()];
                    for (i, value) in string_array.iter().enumerate() {
                        if let Some(value) = value {
                            filter_values[i] = value == expected_value;
                        }
                    }
                    let column_mask = BooleanArray::from(filter_values);

                    // Combine with the main mask using logical AND
                    mask = and(&mask, &column_mask).map_err(|e| PluginError::ArrowError(e))?;
                } else {
                    debug!(
                        "Ignoring non-string column {} of type {:?}",
                        column_name, column_data_type
                    );
                }
            }
        }

        // Apply the mask to filter the batch
        let filtered_batch =
            filter_record_batch(&batch, &mask).map_err(|e| PluginError::ArrowError(e))?;
        //emitting some make-up custom metrics
        self.metrics_recorder.record_count("transform_plugin_custom_count", 40);
        self.metrics_recorder.record_latency_w_tags("transform_plugin_custom_latency_w_tags", start_at.elapsed(), vec!(("tag1", "value2")));
        debug!("Filtered batch has {} rows", filtered_batch.num_rows());

        Ok(filtered_batch)
    }

    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        debug!("Received checkpoint marker: {}", epoch.0);
        Ok(())
    }

    async fn process_checkpoint_finalizer(
        &self,
        epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        debug!("Received checkpoint finalizer: {}", epoch.0);
        Ok(())
    }
}
