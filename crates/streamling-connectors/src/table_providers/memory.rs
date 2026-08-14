use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::common::not_impl_err;
use datafusion::datasource::sink::DataSink;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use streamling_core::checkpoints::channels::send;
use streamling_core::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointMessage, extract_checkpoint_messages, now_ms,
    process_checkpoint_acks,
};
use streamling_core::data::COLUMN_NAME_OP;
use streamling_core::operators::parallel_sink::ParallelSinkExec;
use streamling_core::operators::wrapping::WrappingDataSink;
use streamling_core::telemetry::provider::get_reference_name_from_metric_key;
use streamling_core::telemetry::recorder::get_metrics_recorder;
use streamling_core::topology::Telemetry;
use tracing::trace;

type MemorySinkStorage = Arc<Mutex<Vec<RecordBatch>>>;
type MemorySinkRegistry = Arc<Mutex<HashMap<String, MemorySinkStorage>>>;

static MEMORY_SINK_REGISTRY: OnceLock<MemorySinkRegistry> = OnceLock::new();

fn get_registry() -> &'static MemorySinkRegistry {
    MEMORY_SINK_REGISTRY.get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
}

// Test utility functions
pub fn get_memory_sink_results(sink_name: &str) -> Option<Vec<RecordBatch>> {
    let registry = get_registry().lock().unwrap();
    registry.get(sink_name).map(|storage| {
        let storage = storage.lock().unwrap();
        storage.clone()
    })
}

pub fn get_memory_sink_total_rows(sink_name: &str) -> usize {
    let registry = get_registry().lock().unwrap();
    registry.get(sink_name).map_or(0, |storage| {
        let storage = storage.lock().unwrap();
        storage.iter().map(|batch| batch.num_rows()).sum()
    })
}

pub fn clear_memory_sink_registry() {
    let mut registry = get_registry().lock().unwrap();
    registry.clear();
}

pub fn clear_memory_sink(sink_name: &str) {
    let registry = get_registry().lock().unwrap();
    if let Some(storage) = registry.get(sink_name) {
        let mut storage = storage.lock().unwrap();
        storage.clear();
    }
}

pub struct MemorySink {
    storage: MemorySinkStorage,
    num_records_before_stop: Option<u64>, // for integration tests only!
    /// Global `num_records_before_stop` progress across the concurrent
    /// per-partition `write_all` streams (`ParallelSinkExec`).
    rows_received: AtomicU64,
    source_name: String, // for SourceComplete message
    schema: SchemaRef,
    metric_metadata_id: String,
}

impl MemorySink {
    fn new(
        storage: MemorySinkStorage,
        num_records_before_stop: Option<u64>,
        source_name: String,
        schema: SchemaRef,
        metric_metadata_id: String,
    ) -> Self {
        Self {
            storage,
            num_records_before_stop,
            rows_received: AtomicU64::new(0),
            source_name,
            schema,
            metric_metadata_id,
        }
    }
}

#[async_trait]
impl DataSink for MemorySink {
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut row_count = 0;

        let metrics_recorder = get_metrics_recorder().clone();
        while let Some(batch) = data.next().await.transpose()? {
            let arrival_time_ms = now_ms();
            let row_count_in_batch = batch.num_rows();
            row_count += row_count_in_batch;
            let total_received = self
                .rows_received
                .fetch_add(row_count_in_batch as u64, Ordering::SeqCst)
                + row_count_in_batch as u64;

            // Store the batch in memory
            let ack_start = Instant::now();
            let checkpoint_messages = {
                let mut storage = self.storage.lock().unwrap();
                storage.push(batch.clone());
                metrics_recorder.record_output_rows_count(
                    row_count_in_batch as u64,
                    self.metric_metadata_id.as_str(),
                );
                extract_checkpoint_messages(batch.schema().metadata())
            };

            trace!(
                "Extracted checkpoint messages from batch metadata: {:?}",
                checkpoint_messages
            );

            let sink_id = get_reference_name_from_metric_key(&self.metric_metadata_id);
            process_checkpoint_acks(
                checkpoint_messages,
                arrival_time_ms,
                ack_start,
                &metrics_recorder,
                &self.metric_metadata_id,
                &sink_id,
            );

            // Compare against the global received count so the stop threshold
            // stays global across the concurrent per-partition streams.
            if let Some(num_records_before_stop) = self.num_records_before_stop
                && total_received >= num_records_before_stop
                && !(num_records_before_stop == 0 && total_received == 0)
            {
                // Notify the coordinator (and sources) that the sink has received the expected rows
                let _ = send(
                    CHECKPOINT_COORDINATOR_CHANNEL,
                    CheckpointMessage::SourceComplete(self.source_name.clone()),
                );
                break;
            }
        }
        // If running in test mode and the stream ended before reaching the limit,
        // notify the coordinator so sources waiting on completion can finish.
        if self.num_records_before_stop.is_some() {
            let _ = send(
                CHECKPOINT_COORDINATOR_CHANNEL,
                CheckpointMessage::SourceComplete(self.source_name.clone()),
            );
        }
        Ok(row_count as u64)
    }
}

impl Debug for MemorySink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemorySink").finish()
    }
}

impl DisplayAs for MemorySink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "MemorySink")
            }
        }
    }
}

#[derive(Debug)]
pub struct MemoryTableProvider {
    schema: SchemaRef,
    storage: MemorySinkStorage,
    num_records_before_stop: Option<u64>,
    source_name: String,
    exclude_gs_op: bool,
    metric_metadata_id: String,
    telemetry: Option<Telemetry>,
}

impl MemoryTableProvider {
    pub fn new(
        schema: SchemaRef,
        num_records_before_stop: Option<u64>,
        source_name: String,
        sink_name: String,
        metric_metadata_id: String,
        telemetry: Option<Telemetry>,
    ) -> Self {
        Self::new_with_options(
            schema,
            num_records_before_stop,
            source_name,
            sink_name,
            false,
            metric_metadata_id,
            telemetry,
        )
    }

    pub fn new_with_options(
        schema: SchemaRef,
        num_records_before_stop: Option<u64>,
        source_name: String,
        sink_name: String,
        exclude_gs_op: bool,
        metric_metadata_id: String,
        telemetry: Option<Telemetry>,
    ) -> Self {
        let storage = Arc::new(Mutex::new(Vec::new()));

        // Register in the global registry for test access
        {
            let mut registry = get_registry().lock().unwrap();
            registry.insert(sink_name, storage.clone());
        }

        Self {
            schema,
            storage,
            num_records_before_stop,
            source_name,
            exclude_gs_op,
            metric_metadata_id,
            telemetry,
        }
    }

    pub fn get_batches(&self) -> Vec<RecordBatch> {
        let storage = self.storage.lock().unwrap();
        storage.clone()
    }

    pub fn get_total_rows(&self) -> usize {
        let storage = self.storage.lock().unwrap();
        storage.iter().map(|batch| batch.num_rows()).sum()
    }
}

#[async_trait]
impl TableProvider for MemoryTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Reading is not implemented for MemoryTableProvider")
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Apply projection to exclude _gs_op if needed
        let final_input = if self.exclude_gs_op {
            let input_schema = input.schema();

            // Build projection: all columns except _gs_op
            let projection_exprs: Result<Vec<(Arc<dyn PhysicalExpr>, String)>> = input_schema
                .fields()
                .iter()
                .filter(|f| f.name() != COLUMN_NAME_OP)
                .map(|f| -> Result<(Arc<dyn PhysicalExpr>, String)> {
                    let col_expr = Arc::new(Column::new(f.name(), input_schema.index_of(f.name())?))
                        as Arc<dyn PhysicalExpr>;
                    Ok((col_expr, f.name().clone()))
                })
                .collect();

            let projection_exec = Arc::new(ProjectionExec::try_new(projection_exprs?, input)?);
            projection_exec as Arc<dyn ExecutionPlan>
        } else {
            input
        };

        let memory_sink = Arc::new(MemorySink::new(
            self.storage.clone(),
            self.num_records_before_stop,
            self.source_name.clone(),
            final_input.schema(),
            self.metric_metadata_id.clone(),
        ));
        let telemetry_data_sink = Arc::new(WrappingDataSink::new(
            memory_sink,
            self.metric_metadata_id.clone(),
            None,
            self.telemetry.as_ref(),
        ));
        Ok(Arc::new(ParallelSinkExec::new(
            final_input,
            telemetry_data_sink,
            get_reference_name_from_metric_key(&self.metric_metadata_id),
        )))
    }
}
