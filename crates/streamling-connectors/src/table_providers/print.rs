use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::common::not_impl_err;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::metrics::{Count, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use streamling_core::checkpoints::channels::send;
use streamling_core::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointMessage, extract_checkpoint_messages, now_ms,
    process_checkpoint_acks,
};
use streamling_core::data::RowKind;
use streamling_core::formats::FromArrowConverter;
use streamling_core::formats::json::FromArrowToJsonConverter;
use streamling_core::operators::wrapping::WrappingDataSink;
use streamling_core::telemetry::provider::get_reference_name_from_metric_key;
use streamling_core::telemetry::recorder::get_metrics_recorder;
use streamling_core::topology::Telemetry;
use tracing::{info, trace};

struct PrintSink {
    sample_every: u32,
    num_records_before_stop: Option<u64>,
    schema: SchemaRef,
    counter: AtomicU64,
    source_name: String,
    metric_metadata_id: String,
}

impl PrintSink {
    fn new(
        sample_every: u32,
        num_records_before_stop: Option<u64>,
        schema: SchemaRef,
        source_name: String,
        metric_metadata_id: String,
    ) -> Self {
        Self {
            sample_every,
            num_records_before_stop,
            schema,
            counter: AtomicU64::new(0),
            source_name,
            metric_metadata_id,
        }
    }
}

#[async_trait]
impl DataSink for PrintSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut row_count = 0;

        let metrics_recorder = get_metrics_recorder().clone();
        'outer: while let Some(batch) = data.next().await.transpose()? {
            // Capture timing before any work for accurate metrics
            let arrival_time_ms = now_ms();
            let ack_start = Instant::now();

            if batch.num_rows() > 0 {
                row_count += batch.num_rows();

                let row_kinds = RowKind::extract_row_kinds_from_batch(&batch);

                let converter = FromArrowToJsonConverter::new();
                let rows = converter.convert_from_batch(&batch)?;
                let num_of_printed_rows = Count::new();
                for (i, row) in rows.into_iter().enumerate() {
                    //fetch_add returns the previous value after incrementing so need to load to get value after
                    let counter = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
                    if counter.is_multiple_of(self.sample_every as u64) {
                        let row_kind = row_kinds.get(i).unwrap();
                        let json_row = String::from_utf8(row).unwrap();
                        info!(
                            "{} -> (sample_every={}) [{row_kind}] {json_row} ",
                            row_count, self.sample_every
                        );
                        num_of_printed_rows.add(1);
                    }
                }
                if let Some(stop_at) = self.num_records_before_stop
                    && row_count >= stop_at as usize
                    && !(stop_at == 0 && row_count == 0)
                {
                    info!("Stopping after {} records", row_count);
                    // Notify the coordinator (and sources) that the sink has received the expected rows
                    let _ = send(
                        CHECKPOINT_COORDINATOR_CHANNEL,
                        CheckpointMessage::SourceComplete(self.source_name.clone()),
                    );
                    //need to flush since we are breaking the outer loop
                    metrics_recorder.record_output_rows_count(
                        num_of_printed_rows.value() as u64,
                        self.metric_metadata_id.as_ref(),
                    );
                    break 'outer;
                }
                metrics_recorder.record_output_rows_count(
                    num_of_printed_rows.value() as u64,
                    self.metric_metadata_id.as_ref(),
                );
            }

            let checkpoint_messages = extract_checkpoint_messages(batch.schema().metadata());
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
        }

        Ok(row_count as u64)
    }
}

impl Debug for PrintSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrintSink")
            .field("sample_every", &self.sample_every)
            .field("num_records_before_stop", &self.num_records_before_stop)
            .finish()
    }
}

impl DisplayAs for PrintSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let sample_every = self.sample_every;
                write!(f, "PrintSink (sample_every={sample_every})")
            }
        }
    }
}

#[derive(Debug)]
pub struct PrintTableProvider {
    sample_every: u32,
    num_records_before_stop: Option<u64>,
    schema: SchemaRef,
    source_name: String,
    metric_metadata_id: String,
    telemetry: Option<Telemetry>,
}

impl PrintTableProvider {
    pub fn new(
        sample_every: u32,
        num_records_before_stop: Option<u64>,
        schema: SchemaRef,
        source_name: String,
        metric_metadata_id: String,
        telemetry: Option<Telemetry>,
    ) -> Self {
        Self {
            metric_metadata_id,
            sample_every,
            num_records_before_stop,
            schema,
            source_name,
            telemetry,
        }
    }
}

#[async_trait]
impl TableProvider for PrintTableProvider {
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
        not_impl_err!("Reading is not implemented for PrintTableProvider")
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sink = Arc::new(PrintSink::new(
            self.sample_every,
            self.num_records_before_stop,
            self.schema.clone(),
            self.source_name.clone(),
            self.metric_metadata_id.clone(),
        ));
        let with_telemetry = Arc::new(WrappingDataSink::new(
            sink,
            self.metric_metadata_id.clone(),
            None,
            self.telemetry.as_ref(),
        ));
        Ok(Arc::new(DataSinkExec::new(input, with_telemetry, None)))
    }
}
