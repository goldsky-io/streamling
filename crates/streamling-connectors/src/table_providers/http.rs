use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::not_impl_err;
use datafusion::datasource::sink::DataSink;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::StreamExt;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use streamling_core::operators::parallel_sink::ParallelSinkExec;

use std::time::Instant;
use streamling_config::ExternalHttpHandlerConfig;
use streamling_core::checkpoints::channels::send;
use streamling_core::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointMessage, extract_checkpoint_messages, now_ms,
    process_checkpoint_acks,
};
use streamling_core::operators::external_handlers::{
    ExternalHandlerClient, ExternalHandlerExecutionOutcome,
};
use streamling_core::operators::wrapping::WrappingDataSink;
use streamling_core::telemetry::provider::get_reference_name_from_metric_key;
use streamling_core::telemetry::recorder::get_metrics_recorder;
use streamling_core::topology::Telemetry;
use tracing::trace;

#[allow(dead_code)]
struct HttpSink {
    url: String,
    client: ExternalHandlerClient,
    schema: SchemaRef,
    buffer_size: u32,
    num_records_before_stop: Option<u64>, // for integration tests only!
    /// Global `num_records_before_stop` progress across the concurrent
    /// per-partition `write_all` streams (`ParallelSinkExec`).
    rows_received: std::sync::atomic::AtomicU64,
    source_name: String,
    metric_metadata_id: String,
}

impl HttpSink {
    fn new(
        schema: SchemaRef,
        config: ExternalHttpHandlerConfig,
        url: String,
        headers: Option<BTreeMap<String, String>>,
        one_row_per_request: Option<bool>,
        payload_version: Option<u32>,
        skip_on_error: Option<bool>,
        num_records_before_stop: Option<u64>,
        source_name: String,
        metric_metadata_id: String,
    ) -> Self {
        let skip_on_error = skip_on_error.unwrap_or(false);
        let client = ExternalHandlerClient::new(
            schema.clone(),
            url.clone(),
            headers,
            one_row_per_request,
            payload_version,
            config.trigger_max_count,
            config.operator_timeout_sec,
            None,
            skip_on_error,
            false,
            Some(metric_metadata_id.clone()),
        );
        Self {
            url,
            client,
            schema,
            num_records_before_stop,
            rows_received: std::sync::atomic::AtomicU64::new(0),
            source_name,
            buffer_size: config.buffer_size,
            metric_metadata_id,
        }
    }
}

#[async_trait]
impl DataSink for HttpSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut row_count = 0;

        let metrics_recorder = get_metrics_recorder().clone();
        let buffer_size = self.buffer_size as usize;

        let mut buffered_data = data.ready_chunks(buffer_size);
        while let Some(batches) = buffered_data.next().await {
            let arrival_time_ms = now_ms();
            let ack_start = Instant::now();
            let mut all_checkpoint_messages = Vec::new();

            // Extract checkpoint messages from batches BEFORE sending them
            // This is critical because the HTTP client returns None for successful sends
            // (is_output_expected=false), so we'd lose checkpoint markers if we extract after
            for batch in batches.iter().flatten() {
                all_checkpoint_messages
                    .extend(extract_checkpoint_messages(batch.schema().metadata()));
            }

            let batches_futures =
                futures::future::join_all(batches.into_iter().map(|item| async {
                    let data: datafusion::common::Result<(Option<RecordBatch>, u64, u64)> =
                        match item {
                            Ok(batch) => {
                                let batch_count = batch.num_rows() as u64;
                                if batch_count > 0 {
                                    let start_at = Instant::now();
                                    let result =
                                        self.client.execute_with_stats(batch.clone()).await;
                                    metrics_recorder.record_elapsed_compute(
                                        start_at.elapsed(),
                                        self.metric_metadata_id.as_str(),
                                    );
                                    result.map(
                                        |ExternalHandlerExecutionOutcome {
                                             batch,
                                             delivered_rows,
                                             skipped_rows,
                                         }| {
                                            (batch, delivered_rows, skipped_rows)
                                        },
                                    )
                                } else {
                                    Ok((Some(batch), 0, 0))
                                }
                            }
                            Err(e) => Err(e),
                        };

                    data
                }));

            let batches = batches_futures.await;
            for item in batches {
                match item {
                    Ok((_batch_opt, delivered_rows, skipped_rows)) => {
                        row_count += delivered_rows + skipped_rows;
                        self.rows_received.fetch_add(
                            delivered_rows + skipped_rows,
                            std::sync::atomic::Ordering::SeqCst,
                        );
                        metrics_recorder.record_output_rows_count(
                            delivered_rows,
                            self.metric_metadata_id.as_str(),
                        );
                    }
                    Err(e) => return Err(e),
                }
            }

            trace!(
                "Extracted checkpoint messages from batch metadata: {:?}",
                all_checkpoint_messages
            );

            let sink_id = get_reference_name_from_metric_key(&self.metric_metadata_id);
            process_checkpoint_acks(
                all_checkpoint_messages,
                arrival_time_ms,
                ack_start,
                &metrics_recorder,
                &self.metric_metadata_id,
                &sink_id,
            );
            // Compare against the global received count so the stop threshold
            // stays global across the concurrent per-partition streams.
            let total_received = self.rows_received.load(std::sync::atomic::Ordering::SeqCst);
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

        // In tests, if the stream ends without reaching the limit, still notify completion
        if self.num_records_before_stop.is_some() {
            let _ = send(
                CHECKPOINT_COORDINATOR_CHANNEL,
                CheckpointMessage::SourceComplete(self.source_name.clone()),
            );
        }

        Ok(row_count)
    }
}

impl Debug for HttpSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpSink").field("url", &self.url).finish()
    }
}

impl DisplayAs for HttpSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let url = self.url.clone();
                write!(f, "HttpSink (url={url})")
            }
        }
    }
}

#[derive(Debug)]
pub struct HttpTableProvider {
    url: String,
    headers: Option<BTreeMap<String, String>>,
    one_row_per_request: Option<bool>,
    payload_version: Option<u32>,
    skip_on_error: Option<bool>,
    schema: SchemaRef,
    config: ExternalHttpHandlerConfig,
    num_records_before_stop: Option<u64>,
    source_name: String,
    metric_metadata_id: String,
    telemetry: Option<Telemetry>,
}

impl HttpTableProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: ExternalHttpHandlerConfig,
        url: String,
        headers: Option<BTreeMap<String, String>>,
        one_row_per_request: Option<bool>,
        payload_version: Option<u32>,
        skip_on_error: Option<bool>,
        schema: SchemaRef,
        num_records_before_stop: Option<u64>,
        source_name: String,
        metric_metadata_id: String,
        telemetry: Option<Telemetry>,
    ) -> Self {
        Self {
            config,
            url,
            headers,
            one_row_per_request,
            payload_version,
            skip_on_error,
            schema,
            num_records_before_stop,
            source_name,
            metric_metadata_id,
            telemetry,
        }
    }
}

#[async_trait]
impl TableProvider for HttpTableProvider {
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
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Reading is not implemented for HttpTableProvider")
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // TODO: check _insert_op
        let http_sink = Arc::new(HttpSink::new(
            self.schema(),
            self.config.clone(),
            self.url.clone(),
            self.headers.clone(),
            self.one_row_per_request,
            self.payload_version,
            self.skip_on_error,
            self.num_records_before_stop,
            self.source_name.clone(),
            self.metric_metadata_id.clone(),
        ));
        let telemetry_data_sink = Arc::new(WrappingDataSink::new(
            http_sink,
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
