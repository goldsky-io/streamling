//! Physical operators, plus the checkpoint-marker plumbing they share.
//!
//! Streamling's checkpoint markers ride on RecordBatch *schema metadata*, which
//! stock DataFusion operators rebuild away.

pub mod broadcast;
pub mod checkpointable;
pub mod coalesce;
pub mod external_handlers;
pub mod filter;
pub mod inspect;
pub mod parallel_sink;
pub mod pg_aggregation;
pub mod planner;
pub mod projection;
pub mod rebatch;
pub mod repartition;
pub mod scan_sharing;
pub mod unnest;
pub mod wasm_runner;
pub mod wrapping;

use crate::checkpoints::checkpoint_management::{
    CheckpointMessage, MarkerAligner, enrich_batch_metadata_with_checkpoints,
    extract_checkpoint_messages, strip_checkpoint_messages,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{ExecutionPlan, execute_input_stream};
use futures::StreamExt;
use parking_lot::Mutex;
use std::collections::HashMap;
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
                        RecordBatch::try_new(
                            schema_with_messages(&output_schema, &checkpoint_messages),
                            batch.columns().to_vec(),
                        )
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
