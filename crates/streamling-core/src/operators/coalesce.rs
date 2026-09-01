//! A replacement for DataFusion's `CoalescePartitionsExec` that preserves
//! streamling's checkpoint markers. Stock `CoalescePartitionsExec`/`RepartitionExec`
//! rebuild batches with a metadata-less schema and would silently drop the
//! checkpoint markers that ride on RecordBatch schema metadata, stalling
//! checkpoint finalization.

use datafusion::common::{Statistics, internal_err};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};

use crate::checkpoints::checkpoint_management::MarkerAligner;
use crate::operators::spawn_aligning_forwarder;
use parking_lot::Mutex;
use std::fmt;
use std::sync::Arc;

/// Merges all input partitions into a single output partition, forwarding each
/// batch with its checkpoint-marker metadata intact.
#[derive(Debug)]
pub struct StreamingCoalesceExec {
    input: Arc<dyn ExecutionPlan>,
    channel_buffer_size: usize,
    cache: Arc<PlanProperties>,
}

impl StreamingCoalesceExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, channel_buffer_size: usize) -> Self {
        let cache = Self::compute_properties(&input);
        Self {
            input,
            channel_buffer_size,
            cache: Arc::new(cache),
        }
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }
}

impl DisplayAs for StreamingCoalesceExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "StreamingCoalesceExec: partitions={}",
                    self.properties().output_partitioning().partition_count()
                )
            }
        }
    }
}

impl ExecutionPlan for StreamingCoalesceExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingCoalesceExec::new(
            children[0].clone(),
            self.channel_buffer_size,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "StreamingCoalesceExec has a single output partition, got request for partition {partition}"
            );
        }

        let output_schema = self.schema();
        let mut builder =
            RecordBatchReceiverStreamBuilder::new(output_schema.clone(), self.channel_buffer_size);

        // Coalesce all input partitions into this single output partition, forwarding
        // each batch with its checkpoint-marker metadata intact. We merge here rather
        // than via a stock DataFusion `RepartitionExec`/`CoalescePartitionsExec`, which
        // rebuild batches with a metadata-less schema and would drop checkpoint markers,
        // stalling checkpoint finalization (e.g. for `UNION ALL`, which is multi-partition).
        //
        // The shared aligner collapses the N marker copies a multi-partition input
        // produces into one, so the downstream sink cannot ack an epoch while the
        // slower branches still have pre-marker data in flight.
        let input_partitions = self.input.output_partitioning().partition_count();
        let aligner = Arc::new(Mutex::new(MarkerAligner::new(input_partitions)));
        for input_partition in 0..input_partitions {
            spawn_aligning_forwarder(
                &mut builder,
                &self.input,
                input_partition,
                &output_schema,
                &context,
                format!("StreamingCoalesceExec over {}", self.input.name()),
                Arc::clone(&aligner),
            )?;
        }

        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;
    use crate::checkpoints::checkpoint_management::{
        CheckpointMessage, enrich_batch_metadata_with_checkpoints,
    };
    use arrow::array::Int32Array;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use std::collections::HashMap;

    /// A multi-partition source whose batches carry checkpoint markers, i.e. the
    /// shape `UNION ALL` of two streaming sources produces.
    #[derive(Debug)]
    pub(crate) struct MarkerSourceExec {
        partitions: Vec<Vec<RecordBatch>>,
        schema: SchemaRef,
        cache: Arc<PlanProperties>,
    }

    impl MarkerSourceExec {
        pub(crate) fn new(partitions: Vec<Vec<RecordBatch>>) -> Self {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let cache = PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(partitions.len()),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                partitions,
                schema,
                cache: Arc::new(cache),
            }
        }

        /// One batch of `ids`, carrying `messages` in its schema metadata.
        pub(crate) fn batch(ids: Vec<i32>, messages: &[CheckpointMessage]) -> RecordBatch {
            let fields = vec![Field::new("id", DataType::Int32, false)];
            let mut metadata: HashMap<String, String> = HashMap::new();
            if !messages.is_empty() {
                enrich_batch_metadata_with_checkpoints(&mut metadata, messages);
            }
            let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap()
        }
    }

    impl DisplayAs for MarkerSourceExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MarkerSourceExec")
        }
    }

    impl ExecutionPlan for MarkerSourceExec {
        fn name(&self) -> &'static str {
            "MarkerSourceExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.cache
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let batches = self.partitions[partition].clone();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::iter(batches.into_iter().map(Ok)),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::MarkerSourceExec;
    use super::*;
    use crate::checkpoints::checkpoint_management::{
        CheckpointEpoch, CheckpointMessage, extract_checkpoint_messages,
    };
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

    fn marker(epoch: u64) -> CheckpointMessage {
        CheckpointMessage::Marker {
            epoch: CheckpointEpoch(epoch),
            created_at_ms: 0,
        }
    }

    /// The hole this aligner closes: `UNION ALL` of two marker-emitting sources
    /// used to deliver two copies of the same epoch to the sink, and the
    /// coordinator finalizes on the *first* ack per sink — committing offsets
    /// while the slower branch still had pre-marker data in flight.
    #[tokio::test]
    async fn union_of_two_marker_emitting_branches_yields_one_marker() {
        let source = Arc::new(MarkerSourceExec::new(vec![
            vec![MarkerSourceExec::batch(vec![1, 2], &[marker(1)])],
            vec![MarkerSourceExec::batch(vec![3, 4], &[marker(1)])],
        ]));
        let coalesce = StreamingCoalesceExec::new(source, 10);

        let ctx = SessionContext::new();
        let mut stream = coalesce.execute(0, ctx.task_ctx()).unwrap();
        let (mut markers, mut rows) = (0, 0);
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            markers += extract_checkpoint_messages(batch.schema().metadata()).len();
            rows += batch.num_rows();
        }

        assert_eq!(
            markers, 1,
            "both branches' copies of epoch 1 must collapse to a single marker"
        );
        assert_eq!(rows, 4, "no rows may be dropped by the alignment");
    }

    /// A branch that ends without ever emitting the epoch must not stall it.
    #[tokio::test]
    async fn a_branch_that_ends_early_releases_the_marker() {
        let source = Arc::new(MarkerSourceExec::new(vec![
            vec![MarkerSourceExec::batch(vec![1], &[marker(2)])],
            vec![MarkerSourceExec::batch(vec![2], &[])],
        ]));
        let coalesce = StreamingCoalesceExec::new(source, 10);

        let ctx = SessionContext::new();
        let mut stream = coalesce.execute(0, ctx.task_ctx()).unwrap();
        let mut markers = 0;
        while let Some(batch) = stream.next().await {
            markers += extract_checkpoint_messages(batch.unwrap().schema().metadata()).len();
        }
        assert_eq!(
            markers, 1,
            "the epoch must be released when the other input ends"
        );
    }
}
