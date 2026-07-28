//! A replacement for DataFusion's `DataSinkExec` that executes every input
//! partition and writes them through the sink concurrently — one `write_all`
//! call per partition, each on its own task. DataFusion's `DataSinkExec` pulls
//! only input partition 0, which would force a coalesce in front of every sink
//! and serialize the whole pipeline behind one stream.
//!
//! Concurrent writes mean there is no cross-partition ordering: rows carrying
//! the same primary key on different partitions reach the sink in
//! nondeterministic order. Today the only multi-partition input is the bounded
//! file source (append-only inserts, no checkpoint markers), where ordering is
//! immaterial; order-sensitive streams (CDC upserts/deletes, checkpoint
//! markers) all flow through single-partition sources, and `UNION ALL` fan-ins
//! are merged back to one partition by the `EnforceSinglePartition` optimizer
//! rule. `write_all` implementations must tolerate concurrent invocation:
//! one-time setup (DDL, plugin init) belongs behind a `OnceCell`, and
//! `num_records_before_stop` counting must use state shared on the sink
//! instance, not `write_all`-locals.

use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Statistics, internal_datafusion_err, internal_err};
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_input_stream,
};
use futures::StreamExt;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Executes all input partitions and writes each through `sink.write_all`
/// concurrently, returning a single-row batch with the total written count
/// (the same output contract as `DataSinkExec`).
pub struct ParallelSinkExec {
    input: Arc<dyn ExecutionPlan>,
    sink: Arc<dyn DataSink>,
    count_schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl ParallelSinkExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, sink: Arc<dyn DataSink>) -> Self {
        let count_schema = make_count_schema();
        let cache = Self::compute_properties(&input, count_schema.clone());
        Self {
            input,
            sink,
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
                write!(f, "ParallelSinkExec: sink=")?;
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
                execute_input_stream(
                    Arc::clone(&self.input),
                    Arc::clone(self.sink.schema()),
                    input_partition,
                    Arc::clone(&context),
                )
            })
            .collect::<Result<_>>()?;

        let sink = Arc::clone(&self.sink);
        let count_schema = Arc::clone(&self.count_schema);

        let stream = futures::stream::once(async move {
            // One spawned task per partition so each partition's pipeline
            // (scan, transforms, rebatch, write) runs on its own executor
            // thread. `JoinSet` aborts the remaining writes when the first
            // error propagates out.
            let mut writes = JoinSet::new();
            for data in streams {
                let sink = Arc::clone(&sink);
                let context = Arc::clone(&context);
                writes.spawn(async move { sink.write_all(data, &context).await });
            }
            let mut total_count: u64 = 0;
            while let Some(write_result) = writes.join_next().await {
                total_count += write_result
                    .map_err(|e| internal_datafusion_err!("sink write task failed: {e}"))??;
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
    use arrow::array::Int32Array;
    use async_trait::async_trait;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionContext;
    use std::sync::atomic::{AtomicU64, Ordering};

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
        let exec = ParallelSinkExec::new(input, sink.clone());

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
}
