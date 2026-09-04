//! A replacement for DataFusion's `DataSinkExec` that executes every input
//! partition and writes them through the sink concurrently — one `write_all`
//! call per partition, each on its own task. DataFusion's `DataSinkExec` pulls
//! only input partition 0, which would force a coalesce in front of every sink
//! and serialize the whole pipeline behind one stream.
//!
//! Concurrent writes mean there is no cross-partition ordering: rows carrying
//! the same primary key on different partitions reach the sink in
//! nondeterministic order. Order-sensitive streams (CDC upserts/deletes,
//! keep-last dedup) are kept correct by hash-partitioning on the primary key
//! upstream, so all rows of a key share one partition; `UNION ALL` fan-ins are
//! merged back to one partition by the `EnforceSinglePartition` optimizer rule.
//! `write_all` implementations must tolerate concurrent invocation: one-time
//! setup (DDL, plugin init) belongs behind a `OnceCell`, and
//! `num_records_before_stop` counting must use state shared on the sink
//! instance, not `write_all`-locals.
//!
//! Checkpoint markers fan out with the partitions, so each write stream sees its
//! own copy of an epoch. The sink must not ack until every stream has flushed
//! it, or the source commits offsets for data still in flight — hence the
//! per-sink ack gate registered here (see `MarkerAligner`).

use crate::checkpoints::checkpoint_management::{
    register_sink_streams, send_checkpoint_ack, sink_stream_done,
};
use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Statistics, internal_datafusion_err, internal_err};
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_input_stream,
};
use futures::StreamExt;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::task::JoinSet;

/// How many batches each write stream may run ahead of its `write_all`.
///
/// A sink stops polling its input for the whole duration of a write (an HTTP
/// INSERT, an `ALTER TABLE` mutation). With nothing draining behind it, that
/// stall backs up into the shared exchange upstream, whose router blocks on the
/// first full output and so stalls every *other* write stream too — the sink's
/// parallelism collapses to the pace of its slowest writer.
///
/// One batch is enough because it is denominated in the same unit as the stall
/// it covers: the rebatcher keeps accumulating behind the buffered batch, and
/// both the write and the accumulation scale with the sink's `batch_size`, so
/// "one batch ahead" stays "one write's worth of slack" at any tuning.
const WRITE_STREAM_PREFETCH: usize = 1;

/// Runs `input` on its own task, letting it produce up to `capacity` batches
/// ahead of the consumer.
///
/// Batch-level schema metadata (the checkpoint markers) is forwarded untouched:
/// `RecordBatchReceiverStream` yields exactly what it was sent, unlike
/// `CoalescePartitionsExec`, which rebuilds batches on the operator schema and
/// would drop them.
fn prefetch(mut input: SendableRecordBatchStream, capacity: usize) -> SendableRecordBatchStream {
    let mut builder = RecordBatchReceiverStreamBuilder::new(input.schema(), capacity);
    let tx = builder.tx();
    builder.spawn(async move {
        while let Some(batch) = input.next().await {
            if tx.send(batch).await.is_err() {
                // The consumer is gone; nothing left to feed.
                return Ok(());
            }
        }
        Ok(())
    });
    builder.build()
}

/// Executes all input partitions and writes each through `sink.write_all`
/// concurrently, returning a single-row batch with the total written count
/// (the same output contract as `DataSinkExec`).
pub struct ParallelSinkExec {
    input: Arc<dyn ExecutionPlan>,
    sink: Arc<dyn DataSink>,
    /// The sink's reference name. Must match the `sink_id` the sink itself acks
    /// with (`get_reference_name_from_metric_key(metric_metadata_id)`), or the
    /// ack gate registered here will not be found and the sink acks on the first
    /// stream to see the epoch.
    sink_id: String,
    count_schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl ParallelSinkExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, sink: Arc<dyn DataSink>, sink_id: String) -> Self {
        let count_schema = make_count_schema();
        let cache = Self::compute_properties(&input, count_schema.clone());
        Self {
            input,
            sink,
            sink_id,
            count_schema,
            cache: Arc::new(cache),
        }
    }

    /// Input execution plan (the sink's transform chain).
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// The wrapped sink.
    pub fn sink(&self) -> &dyn DataSink {
        self.sink.as_ref()
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }
}

fn make_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

impl Debug for ParallelSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ParallelSinkExec")
    }
}

impl DisplayAs for ParallelSinkExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                // The input's width is the number of concurrent `write_all`
                // calls, which is the one thing about this node a plan dump
                // cannot show anywhere else: its own output is always 1.
                write!(
                    f,
                    "ParallelSinkExec: partitions={}, sink=",
                    self.input.output_partitioning().partition_count()
                )?;
                self.sink.fmt_as(t, f)
            }
            DisplayFormatType::TreeRender => self.sink.fmt_as(t, f),
        }
    }
}

impl ExecutionPlan for ParallelSinkExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // The whole point: no `SinglePartition` requirement, every input
        // partition gets its own concurrent `write_all`.
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ParallelSinkExec::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.sink),
            self.sink_id.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "ParallelSinkExec has a single output partition, got request for partition {partition}"
            );
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        let streams: Vec<SendableRecordBatchStream> = (0..input_partitions)
            .map(|input_partition| {
                let data = execute_input_stream(
                    Arc::clone(&self.input),
                    Arc::clone(self.sink.schema()),
                    input_partition,
                    Arc::clone(&context),
                )?;
                Ok(prefetch(data, WRITE_STREAM_PREFETCH))
            })
            .collect::<Result<_>>()?;

        let sink = Arc::clone(&self.sink);
        let count_schema = Arc::clone(&self.count_schema);
        let sink_id = self.sink_id.clone();
        register_sink_streams(&sink_id, input_partitions);

        let stream = futures::stream::once(async move {
            // One spawned task per partition so each partition's pipeline
            // (scan, transforms, rebatch, write) runs on its own executor
            // thread. `JoinSet` aborts the remaining writes when the first
            // error propagates out.
            let mut writes = JoinSet::new();
            for data in streams {
                let sink = Arc::clone(&sink);
                let context = Arc::clone(&context);
                // Sanctioned: structured concurrency — every task is joined
                // via `join_next` below before this stream completes, and the
                // JoinSet's abort-on-drop is the intended first-error
                // behavior (cancel the sibling writes when one fails).
                #[allow(clippy::disallowed_methods)]
                writes.spawn(async move { sink.write_all(data, &context).await });
            }
            let mut total_count: u64 = 0;
            while let Some(write_result) = writes.join_next().await {
                // A finished stream will never report another epoch; releasing
                // its share here keeps a late epoch from waiting forever on it.
                // The epochs freed by a FAILED write are deliberately not acked —
                // acking them would let the source commit offsets for rows that
                // never landed, hence the `?`s before the ack loop.
                let freed_epochs = sink_stream_done(&sink_id);
                let written = write_result
                    .map_err(|e| internal_datafusion_err!("sink write task failed: {e}"))??;
                for epoch in freed_epochs {
                    send_checkpoint_ack(epoch, &sink_id);
                }
                total_count += written;
            }
            RecordBatch::try_new(
                make_count_schema(),
                vec![Arc::new(UInt64Array::from(vec![total_count]))],
            )
            .map_err(Into::into)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.sink.metrics()
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::channels::{subscribe_with_id, unsubscribe};
    use crate::checkpoints::checkpoint_management::{
        CHECKPOINT_COORDINATOR_CHANNEL, CheckpointEpoch, CheckpointMessage, report_marker_at_sink,
    };
    use arrow::array::Int32Array;
    use async_trait::async_trait;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    /// Counts every row it receives across concurrent `write_all` calls.
    #[derive(Debug)]
    struct CountingSink {
        schema: SchemaRef,
        rows_written: AtomicU64,
    }

    #[async_trait]
    impl DataSink for CountingSink {
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
            let mut count: u64 = 0;
            while let Some(batch) = data.next().await.transpose()? {
                count += batch.num_rows() as u64;
            }
            self.rows_written.fetch_add(count, Ordering::SeqCst);
            Ok(count)
        }
    }

    impl DisplayAs for CountingSink {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "CountingSink")
        }
    }

    /// The node's own output is always a single partition, so the write
    /// concurrency is invisible in a plan dump unless it says so itself.
    #[test]
    fn display_reports_the_number_of_write_streams() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        let input: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
            &[vec![batch.clone()], vec![batch]],
            schema.clone(),
            None,
        )
        .unwrap();
        let exec = ParallelSinkExec::new(
            input,
            Arc::new(CountingSink {
                schema,
                rows_written: AtomicU64::new(0),
            }),
            "test_sink".to_string(),
        );

        let rendered = datafusion::physical_plan::displayable(&exec)
            .indent(true)
            .to_string();
        assert!(
            rendered.contains("ParallelSinkExec: partitions=2, sink=CountingSink"),
            "unexpected plan display:\n{rendered}"
        );
    }

    /// The whole point of the prefetch: the input keeps running while the
    /// consumer sits idle, which is what a sink does for the length of a write.
    /// If the input were only driven on demand, a stalled writer would back up
    /// into the shared exchange and stall its sibling writers too.
    ///
    /// Checkpoint markers ride on batch schema metadata, so the buffer must
    /// forward batches untouched — losing them stalls checkpoint finalization.
    #[tokio::test]
    async fn prefetch_drives_the_input_while_the_consumer_is_idle() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let marked_schema = Arc::new(Schema::new_with_metadata(
            schema.fields().clone(),
            HashMap::from([("marker".to_string(), "epoch-1".to_string())]),
        ));
        let produced = Arc::new(AtomicU64::new(0));

        let counter = Arc::clone(&produced);
        let batch_schema = Arc::clone(&marked_schema);
        let input = futures::stream::iter(0..8).map(move |id| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(RecordBatch::try_new(
                Arc::clone(&batch_schema),
                vec![Arc::new(Int32Array::from(vec![id]))],
            )
            .unwrap())
        });
        let mut stream = prefetch(
            Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), input)),
            WRITE_STREAM_PREFETCH,
        );

        let first = stream.next().await.expect("a batch").unwrap();
        assert_eq!(
            first.schema().metadata().get("marker").map(String::as_str),
            Some("epoch-1"),
            "batch schema metadata must survive the buffer"
        );

        // The consumer takes nothing further; only the prefetch task can advance
        // the input from here.
        for _ in 0..100 {
            if produced.load(Ordering::SeqCst) > 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            produced.load(Ordering::SeqCst) > 1,
            "the input must run ahead of an idle consumer, produced only {}",
            produced.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn writes_every_input_partition() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch_for = |ids: Vec<i32>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))]).unwrap()
        };
        let input: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
            &[
                vec![batch_for(vec![1, 2, 3])],
                vec![batch_for(vec![4, 5])],
                vec![batch_for(vec![6])],
            ],
            schema.clone(),
            None,
        )
        .unwrap();
        assert_eq!(input.output_partitioning().partition_count(), 3);

        let sink = Arc::new(CountingSink {
            schema: schema.clone(),
            rows_written: AtomicU64::new(0),
        });
        let exec = ParallelSinkExec::new(input, sink.clone(), "test_sink".to_string());

        let task_context = SessionContext::new().task_ctx();
        let mut stream = exec.execute(0, task_context).unwrap();
        let count_batch = stream.next().await.unwrap().unwrap();

        let counts = count_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 6, "count batch must sum all partitions");
        assert_eq!(
            sink.rows_written.load(Ordering::SeqCst),
            6,
            "every partition's rows must reach the sink"
        );
    }

    const FREED_EPOCH: CheckpointEpoch = CheckpointEpoch(7);

    /// One stream flushes an epoch and stays open, the other fails: finishing the
    /// failed stream frees that epoch from the ack gate, and acking it there
    /// would let the source commit offsets for the rows the failed write lost.
    #[derive(Debug)]
    struct OneStreamFailsSink {
        schema: SchemaRef,
        sink_id: String,
        writes_started: AtomicU64,
        epoch_reported: AtomicBool,
    }

    #[async_trait]
    impl DataSink for OneStreamFailsSink {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn metrics(&self) -> Option<MetricsSet> {
            None
        }

        async fn write_all(
            &self,
            _data: SendableRecordBatchStream,
            _context: &Arc<TaskContext>,
        ) -> Result<u64> {
            if self.writes_started.fetch_add(1, Ordering::SeqCst) == 0 {
                report_marker_at_sink(&self.sink_id, FREED_EPOCH);
                self.epoch_reported.store(true, Ordering::SeqCst);
                // Never finish, so the failure below is the first join result.
                std::future::pending::<()>().await;
                unreachable!()
            }
            while !self.epoch_reported.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            internal_err!("write failed")
        }
    }

    impl DisplayAs for OneStreamFailsSink {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "OneStreamFailsSink")
        }
    }

    #[tokio::test]
    async fn a_failed_write_does_not_ack_the_epochs_it_frees() {
        let sink_id = "parallel_sink_failed_write";
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        let input: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
            &[vec![batch.clone()], vec![batch]],
            schema.clone(),
            None,
        )
        .unwrap();
        let exec = ParallelSinkExec::new(
            input,
            Arc::new(OneStreamFailsSink {
                schema,
                sink_id: sink_id.to_string(),
                writes_started: AtomicU64::new(0),
                epoch_reported: AtomicBool::new(false),
            }),
            sink_id.to_string(),
        );

        let (acks, subscriber_id) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);
        let mut stream = exec.execute(0, SessionContext::new().task_ctx()).unwrap();
        let result = stream.next().await.expect("a result");
        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, subscriber_id);

        assert!(result.is_err(), "the failed write must fail the plan");
        assert!(
            !acks.try_iter().any(|message| matches!(
                message,
                CheckpointMessage::Ack { sink_id: acked, .. } if acked == sink_id
            )),
            "a failed write must not ack the epochs its exit frees"
        );
    }
}
