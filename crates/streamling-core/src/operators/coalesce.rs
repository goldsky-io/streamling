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
    enrich_batch_metadata_with_checkpoints, extract_checkpoint_messages,
};
use futures::StreamExt;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tracing::debug;

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
                write!(f, "StreamingCoalesceExec")
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
        let input_partitions = self.input.output_partitioning().partition_count();
        for input_partition in 0..input_partitions {
            spawn_marker_preserving_forwarder(
                &mut builder,
                &self.input,
                input_partition,
                &output_schema,
                &context,
                format!("StreamingCoalesceExec over {}", self.input.name()),
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
