//! A replacement for DataFusion's `CoalescePartitionsExec` that preserves
//! streamling's checkpoint markers. Stock `CoalescePartitionsExec`/`RepartitionExec`
//! rebuild batches with a metadata-less schema and would silently drop the
//! checkpoint markers that ride on RecordBatch schema metadata, stalling
//! checkpoint finalization.

use arrow::record_batch::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::common::{Statistics, internal_err};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execute_input_stream,
};

use crate::checkpoints::checkpoint_management::{
    CheckpointMessage, MarkerAligner, enrich_batch_metadata_with_checkpoints,
    extract_checkpoint_messages, strip_checkpoint_messages,
};
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tracing::debug;

/// Returns `schema` with `messages` merged into its metadata.
pub(crate) fn schema_with_messages(
    schema: &SchemaRef,
    messages: &[CheckpointMessage],
) -> SchemaRef {
    let mut metadata: HashMap<String, String> = schema.metadata().clone();
    enrich_batch_metadata_with_checkpoints(&mut metadata, messages);
    Arc::new(Schema::new_with_metadata(schema.fields().clone(), metadata))
}

/// A zero-row batch whose only payload is `messages`.
///
/// Released markers travel on their own batch rather than riding the data batch
/// they arrived with, because alignment can release an epoch at a moment when no
/// data is being forwarded (e.g. when the last live input delivers its copy, or
/// when an input ends).
pub(crate) fn marker_only_batch(schema: &SchemaRef, messages: &[CheckpointMessage]) -> RecordBatch {
    RecordBatch::new_empty(schema_with_messages(schema, messages))
}

/// Spawns a task on `builder` that forwards one input partition into the
/// builder's output channel, re-attaching each batch's checkpoint-marker
/// metadata onto the output schema. `label` identifies the forwarding operator
/// in debug logs (e.g. a sql transform's reference name).
pub(crate) fn spawn_marker_preserving_forwarder(
    builder: &mut RecordBatchReceiverStreamBuilder,
    input: &Arc<dyn ExecutionPlan>,
    input_partition: usize,
    output_schema: &SchemaRef,
    context: &Arc<TaskContext>,
    label: String,
) -> Result<()> {
    let data = execute_input_stream(
        Arc::clone(input),
        Arc::clone(output_schema),
        input_partition,
        Arc::clone(context),
    )?;
    let tx = builder.tx();
    let output_schema = Arc::clone(output_schema);

    builder.spawn(async move {
        let mut stream = data;

        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => {
                    // Extract checkpoint messages from input batch metadata
                    let checkpoint_messages =
                        extract_checkpoint_messages(batch.schema().metadata());

                    // Create output batch with checkpoint messages preserved in schema metadata
                    // Preserve existing metadata from output_schema and merge with checkpoint messages
                    let output_batch = if !checkpoint_messages.is_empty() {
                        let mut metadata: HashMap<String, String> =
                            output_schema.metadata().clone();
                        enrich_batch_metadata_with_checkpoints(&mut metadata, &checkpoint_messages);
                        let enriched_schema = Arc::new(Schema::new_with_metadata(
                            output_schema.fields().clone(),
                            metadata,
                        ));
                        RecordBatch::try_new(enriched_schema, batch.columns().to_vec())
                            .unwrap_or(batch)
                    } else {
                        batch
                    };

                    // Forward the batch to the output stream
                    if tx.send(Ok(output_batch)).await.is_err() {
                        // The receiver was dropped, stop processing
                        break;
                    }
                }
                Err(e) => {
                    debug!(
                        "{} [partition {}]: error from input stream, stream will terminate: {}",
                        label, input_partition, e
                    );
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }

        Ok(())
    });

    Ok(())
}

/// Like [`spawn_marker_preserving_forwarder`], but for merge points: several
/// forwarders share one `aligner`, so a checkpoint marker is emitted downstream
/// only once every live input has delivered its copy.
///
/// Data is never held back — only the marker is delayed — so at-least-once
/// delivery is preserved and the downstream sink flushes at least all pre-marker
/// data before acking.
pub(crate) fn spawn_aligning_forwarder(
    builder: &mut RecordBatchReceiverStreamBuilder,
    input: &Arc<dyn ExecutionPlan>,
    input_partition: usize,
    output_schema: &SchemaRef,
    context: &Arc<TaskContext>,
    label: String,
    aligner: Arc<Mutex<MarkerAligner>>,
) -> Result<()> {
    let data = execute_input_stream(
        Arc::clone(input),
        Arc::clone(output_schema),
        input_partition,
        Arc::clone(context),
    )?;
    let tx = builder.tx();
    let output_schema = Arc::clone(output_schema);

    builder.spawn(async move {
        let mut stream = data;

        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => {
                    let messages = extract_checkpoint_messages(batch.schema().metadata());
                    if messages.is_empty() {
                        if tx.send(Ok(batch)).await.is_err() {
                            return Ok(());
                        }
                        continue;
                    }
                    // Forward the rows without their markers, then let the
                    // aligner decide whether this copy completes the epoch.
                    if tx
                        .send(Ok(strip_checkpoint_messages(&batch)))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    let released = aligner.lock().observe(messages);
                    if !released.is_empty()
                        && tx
                            .send(Ok(marker_only_batch(&output_schema, &released)))
                            .await
                            .is_err()
                    {
                        return Ok(());
                    }
                }
                Err(e) => {
                    debug!(
                        "{} [partition {}]: error from input stream, stream will terminate: {}",
                        label, input_partition, e
                    );
                    let _ = tx.send(Err(e)).await;
                    return Ok(());
                }
            }
        }

        // This input will never deliver another marker; release any epoch that
        // was only waiting on it.
        let released = aligner.lock().input_done();
        if !released.is_empty() {
            let _ = tx
                .send(Ok(marker_only_batch(&output_schema, &released)))
                .await;
        }

        Ok(())
    });

    Ok(())
}

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
    use arrow::array::Int32Array;
    use arrow_schema::{DataType, Field};
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
    use crate::checkpoints::checkpoint_management::CheckpointEpoch;
    use datafusion::prelude::SessionContext;

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
