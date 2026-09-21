//! A partition's subscription to the checkpoint coordinator.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::Result as DataFusionResult;
use tokio::sync::mpsc::Sender;

use streamling_core::checkpoints::channels::{SubscriberId, subscribe_with_id, unsubscribe};
use streamling_core::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointMessage, enrich_batch_metadata_with_checkpoints,
};

use super::reader::ReadPlan;

/// A partition's subscription to the checkpoint coordinator, plus the messages
/// drained but not yet sent downstream. Unsubscribes on drop, as each Kafka
/// consumer instance does, so an ended partition never leaves a dead receiver for
/// the coordinator's next broadcast to prune.
pub(super) struct CheckpointInbox {
    receiver: crossbeam::channel::Receiver<CheckpointMessage>,
    subscriber_id: SubscriberId,
    pub(super) buffer: Vec<CheckpointMessage>,
}

impl CheckpointInbox {
    pub(super) fn subscribe() -> Self {
        let (receiver, subscriber_id) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);
        Self {
            receiver,
            subscriber_id,
            buffer: Vec::new(),
        }
    }

    /// Drains every available coordinator message, recording each on the plan
    /// and buffering it to ride the next emitted batch downstream. Returns
    /// whether a `SourceComplete` was seen.
    pub(super) async fn drain<P: ReadPlan>(&mut self, plan: &mut P) -> DataFusionResult<bool> {
        let mut source_complete = false;
        while let Ok(message) = self.receiver.try_recv() {
            source_complete |= handle_checkpoint_message(&message, plan).await?;
            self.buffer.push(message);
        }
        Ok(source_complete)
    }

    /// Attaches the buffered messages to `batch` and clears the buffer.
    pub(super) fn attach(&mut self, batch: RecordBatch) -> RecordBatch {
        let batch = enrich_with_checkpoints(batch, &self.buffer);
        self.buffer.clear();
        batch
    }
}

impl Drop for CheckpointInbox {
    fn drop(&mut self) {
        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, self.subscriber_id);
    }
}

/// Handles one drained checkpoint message, mirroring the Kafka source's
/// commit-on-finalize: a `Marker` snapshots the plan's progress for its epoch,
/// and a `Finalizer` durably persists that snapshot. Returns `true` for
/// `SourceComplete` so the caller can shut down. Other messages are no-ops here
/// (they still propagate downstream via batch metadata).
pub(super) async fn handle_checkpoint_message<P: ReadPlan>(
    message: &CheckpointMessage,
    plan: &mut P,
) -> DataFusionResult<bool> {
    match message {
        CheckpointMessage::Marker { epoch, .. } => plan.snapshot(epoch),
        CheckpointMessage::Finalizer(epoch) => plan.persist(epoch).await?,
        CheckpointMessage::SourceComplete(_) => return Ok(true),
        CheckpointMessage::Ack { .. } => {}
    }
    Ok(false)
}

/// Attaches pending checkpoint-coordinator messages to a batch's schema metadata
/// so the checkpoint barrier propagates downstream (mirrors the Kafka source).
/// A no-op when there are no messages.
fn enrich_with_checkpoints(batch: RecordBatch, messages: &[CheckpointMessage]) -> RecordBatch {
    if messages.is_empty() {
        return batch;
    }
    let mut metadata = batch.schema().metadata().clone();
    enrich_batch_metadata_with_checkpoints(&mut metadata, messages);
    let schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().clone(),
        metadata,
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap_or(batch)
}

/// Sends `messages` downstream on an empty batch; a no-op without messages.
/// Returns false when downstream is gone.
pub(super) async fn send_checkpoint_messages(
    tx: &Sender<DataFusionResult<RecordBatch>>,
    schema: &SchemaRef,
    messages: Vec<CheckpointMessage>,
) -> bool {
    if messages.is_empty() {
        return true;
    }
    let batch = enrich_with_checkpoints(RecordBatch::new_empty(schema.clone()), &messages);
    tx.send(Ok(batch)).await.is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};
    use streamling_core::checkpoints::checkpoint_management::{
        CheckpointEpoch, extract_checkpoint_messages,
    };

    /// Checkpoint messages drained from the coordinator are attached to a batch's
    /// schema metadata so the barrier propagates downstream (recoverable via
    /// `extract_checkpoint_messages`); an empty set is a no-op.
    #[test]
    fn enrich_with_checkpoints_attaches_marker_metadata() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));

        let enriched = enrich_with_checkpoints(
            RecordBatch::new_empty(schema.clone()),
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(3),
                created_at_ms: 100,
            }],
        );
        let extracted = extract_checkpoint_messages(enriched.schema().metadata());
        assert!(
            matches!(
                extracted.as_slice(),
                [CheckpointMessage::Marker { epoch, .. }] if epoch.0 == 3
            ),
            "marker should be recoverable from batch metadata; got {extracted:?}"
        );

        let untouched = enrich_with_checkpoints(RecordBatch::new_empty(schema), &[]);
        assert!(extract_checkpoint_messages(untouched.schema().metadata()).is_empty());
    }
}
