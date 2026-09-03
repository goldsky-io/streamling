use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::common::not_impl_err;
use datafusion::datasource::sink::DataSink;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use std::fmt;
use std::fmt::Debug;
use streamling_core::checkpoints::channels::send;
use streamling_core::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointMessage, extract_checkpoint_messages, now_ms,
    process_checkpoint_acks,
};
use streamling_core::operators::parallel_sink::ParallelSinkExec;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use streamling_core::operators::wrapping::WrappingDataSink;
use streamling_core::telemetry::provider::get_reference_name_from_metric_key;
use streamling_core::telemetry::recorder::get_metrics_recorder;
use streamling_core::topology::Telemetry;

struct BlackholeSink {
    schema: SchemaRef,
    num_records_before_stop: Option<u64>, // for integration tests only!
    /// Global `num_records_before_stop` progress across the concurrent
    /// per-partition `write_all` streams (`ParallelSinkExec`).
    rows_received: AtomicU64,
    source_name: String,
    metric_metadata_id: String,
}

impl BlackholeSink {
    fn new(
        schema: SchemaRef,
        num_records_before_stop: Option<u64>,
        source_name: String,
        metric_metadata_id: String,
    ) -> Self {
        Self {
            schema,
            num_records_before_stop,
            rows_received: AtomicU64::new(0),
            source_name,
            metric_metadata_id,
        }
    }
}

#[async_trait]
impl DataSink for BlackholeSink {
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
        while let Some(batch) = data.next().await.transpose()? {
            let arrival_time_ms = now_ms();
            let start_at = Instant::now();
            let num_rows_in_batch = batch.num_rows();
            row_count += num_rows_in_batch;
            let total_received = self
                .rows_received
                .fetch_add(num_rows_in_batch as u64, Ordering::SeqCst)
                + num_rows_in_batch as u64;
            metrics_recorder.record_output_rows_count(
                num_rows_in_batch as u64,
                self.metric_metadata_id.as_str(),
            );
            metrics_recorder.record_elapsed_compute(start_at.elapsed(), &self.metric_metadata_id);

            let ack_start = Instant::now();
            let checkpoint_messages = extract_checkpoint_messages(batch.schema().metadata());
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

        Ok(row_count as u64)
    }
}

impl Debug for BlackholeSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlackholeSink").finish()
    }
}

impl DisplayAs for BlackholeSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "BlackholeSink")
            }
        }
    }
}

#[derive(Debug)]
pub struct BlackholeTableProvider {
    schema: SchemaRef,
    num_records_before_stop: Option<u64>,
    source_name: String,
    metric_metadata_id: String,
    telemetry: Option<Telemetry>,
}

impl BlackholeTableProvider {
    pub fn new(
        schema: SchemaRef,
        num_records_before_stop: Option<u64>,
        source_name: String,
        metric_metadata_id: String,
        telemetry: Option<Telemetry>,
    ) -> Self {
        Self {
            schema,
            num_records_before_stop,
            source_name,
            metric_metadata_id,
            telemetry,
        }
    }
}

#[async_trait]
impl TableProvider for BlackholeTableProvider {
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
        not_impl_err!("Reading is not implemented for BlackholeTableProvider")
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let blackhole_sink = Arc::new(BlackholeSink::new(
            self.schema.clone(),
            self.num_records_before_stop,
            self.source_name.clone(),
            self.metric_metadata_id.clone(),
        ));
        let telemetry_data_sink = Arc::new(WrappingDataSink::new(
            blackhole_sink,
            self.metric_metadata_id.clone(),
            None,
            self.telemetry.as_ref(),
        ));
        Ok(Arc::new(ParallelSinkExec::new(
            input,
            telemetry_data_sink,
            get_reference_name_from_metric_key(&self.metric_metadata_id),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;

    /// `num_records_before_stop` has to be global across the concurrent
    /// per-partition `write_all` calls `ParallelSinkExec` makes. Counted in a
    /// `write_all`-local, a sink with `parallelism: N` would need N times the
    /// rows before signalling completion — so a pipeline with a record limit
    /// would simply never stop.
    #[tokio::test]
    async fn stop_threshold_is_shared_across_concurrent_write_streams() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let sink = Arc::new(BlackholeSink::new(
            schema.clone(),
            Some(4),
            "src".to_string(),
            "app::sink".to_string(),
        ));

        // Two streams of two rows each: neither reaches 4 on its own.
        let context = SessionContext::new().task_ctx();
        for _ in 0..2 {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                    .unwrap();
            let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::iter(vec![Ok(batch)]),
            ));
            sink.write_all(stream, &context).await.unwrap();
        }

        assert_eq!(
            sink.rows_received.load(Ordering::SeqCst),
            4,
            "both write streams must count toward the same threshold"
        );
    }
}
