//! Which versions finalized checkpoints cover. A `Finalizer(e)` means every
//! sink acked `e`, so every reader has processed each partition's batch that
//! carried marker `e`, and everything before it. Its horizon is the earliest mark
//! among those batches: no reader still needs a version below it.

use super::backend::Mark;
use crate::checkpoints::channels::{SubscriberId, subscribe_with_id, unsubscribe};
use crate::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointEpoch, CheckpointMessage,
};
use crossbeam::channel::Receiver;
use std::collections::BTreeMap;

/// The epochs the checkpoint coordinator finalized.
struct FinalizerInbox {
    receiver: Receiver<CheckpointMessage>,
    subscriber_id: SubscriberId,
}

impl FinalizerInbox {
    fn subscribe() -> Self {
        let (receiver, subscriber_id) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);
        Self {
            receiver,
            subscriber_id,
        }
    }

    /// The latest epoch finalized since the last call. Finalizers can repeat
    /// or be skipped, so only the latest matters.
    fn latest_finalized(&self) -> Option<u64> {
        self.receiver
            .try_iter()
            .filter_map(|message| match message {
                CheckpointMessage::Finalizer(CheckpointEpoch(epoch)) => Some(epoch),
                _ => None,
            })
            .max()
    }
}

impl Drop for FinalizerInbox {
    fn drop(&mut self) {
        unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, self.subscriber_id);
    }
}

/// A store's horizons, from the marks of the batches that carried checkpoint
/// markers.
pub(super) struct Horizons {
    finalizers: FinalizerInbox,
    /// Per epoch, the earliest mark among the batches that carried its
    /// marker.
    by_epoch: BTreeMap<u64, Mark>,
}

impl Horizons {
    pub(super) fn new() -> Self {
        Self {
            finalizers: FinalizerInbox::subscribe(),
            by_epoch: BTreeMap::new(),
        }
    }

    /// A batch that carried the markers of `epochs` was applied at `mark`.
    pub(super) fn record(&mut self, epochs: &[CheckpointEpoch], mark: Mark) {
        for CheckpointEpoch(epoch) in epochs {
            self.by_epoch
                .entry(*epoch)
                .and_modify(|horizon| *horizon = (*horizon).min(mark))
                .or_insert(mark);
        }
    }

    /// The horizon of the latest epoch finalized since the last call, if any.
    pub(super) fn due(&mut self) -> Option<Mark> {
        let epoch = self.finalizers.latest_finalized()?;
        self.collect(epoch)
    }

    /// The horizon of the newest epoch up to `epoch` that has one, forgetting
    /// epochs up to `epoch`. A partition without the marker has finished, so
    /// it has no reads in flight. A later epoch covers a skipped one.
    pub(super) fn collect(&mut self, epoch: u64) -> Option<Mark> {
        let later = self.by_epoch.split_off(&(epoch + 1));
        // Every live partition carried each finalized epoch's marker, so the
        // newest one here gives the latest safe horizon.
        std::mem::replace(&mut self.by_epoch, later)
            .pop_last()
            .map(|(_, horizon)| horizon)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(horizons: &mut Horizons, epochs: &[u64], mark: u64) {
        let epochs: Vec<CheckpointEpoch> = epochs.iter().copied().map(CheckpointEpoch).collect();
        horizons.record(&epochs, Mark(mark));
    }

    #[test]
    fn the_cut_is_the_earliest_mark_among_the_epochs_marker_batches() {
        let mut horizons = Horizons::new();
        record(&mut horizons, &[], 1);
        record(&mut horizons, &[1], 2);
        record(&mut horizons, &[], 3);
        record(&mut horizons, &[1], 4);
        assert_eq!(horizons.collect(1), Some(Mark(2)));
    }

    #[test]
    fn collect_is_idempotent_and_a_later_epoch_covers_a_skipped_one() {
        let mut horizons = Horizons::new();
        record(&mut horizons, &[1], 1);
        record(&mut horizons, &[2], 2);
        record(&mut horizons, &[3], 3);
        assert_eq!(horizons.collect(1), Some(Mark(1)));
        assert_eq!(horizons.collect(1), None);
        assert_eq!(horizons.collect(3), Some(Mark(3)), "epoch 2 was skipped");
    }

    #[test]
    fn a_finalizer_without_markers_up_to_its_epoch_has_no_cut() {
        let mut horizons = Horizons::new();
        record(&mut horizons, &[2], 1);
        assert_eq!(horizons.collect(1), None);
        assert_eq!(horizons.collect(2), Some(Mark(1)));
    }

    #[test]
    fn a_finalizer_for_an_epoch_without_markers_cuts_at_an_earlier_epochs_cut() {
        let mut horizons = Horizons::new();
        record(&mut horizons, &[], 1);
        record(&mut horizons, &[1], 2);
        record(&mut horizons, &[], 3);
        assert_eq!(horizons.collect(2), Some(Mark(2)));
    }
}
