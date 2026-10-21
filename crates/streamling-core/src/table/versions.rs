//! A table's version history: every change of a key since the last finalized
//! checkpoint, with the key's live row before it, so a reader can undo the
//! changes made after its moment. This is required so a reader can see the
//! table as it was when its batch finished.
//!
//! Applies from all partitions of the table's input take turns under the
//! table's lock, and the history logs their changes in that order. A change
//! links to the same key's previous change, so a key's history is a chain
//! from its newest change, which the table's index keeps for a live key and
//! `removed` keeps for a key without a live row.
//!
//! Each finished apply returns its mark: the log's end at that moment, which
//! its batch carries to the batch's readers. A reader at a mark sees the
//! table as it was then: it walks the key's chain back and undoes every
//! change at or after the mark, whatever partition made it.
//!
//! History is collected on finalized checkpoints: a `Finalizer(e)` means
//! every sink acked `e`, so every reader has processed each partition's
//! batch that carried marker `e`, and everything before it.

use super::RowLocation;
use crate::checkpoints::channels::{SubscriberId, subscribe_with_id, unsubscribe};
use crate::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointEpoch, CheckpointMessage,
};
use crate::error::Result;
use crate::streamling_bail;
use crossbeam::channel::Receiver;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::num::NonZeroU64;

/// Which versions a table keeps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VersionRetention {
    /// Only the latest, which every read gets.
    Latest,
    /// Every change since the last finalized checkpoint, so a node
    /// downstream of `stream`, the node whose side output applies the table,
    /// reads it as of its own batch.
    PerBatch { stream: String },
}

/// The moment a reader sees a table at: the end of its history when the
/// apply of the reader's batch finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mark(pub(super) u64);

/// The mark of each table an apply changed, the table and its aggregates'
/// outputs, by name. Empty for a table that keeps only the latest version.
pub type Marks = Vec<(String, Mark)>;

/// The epochs the checkpoint coordinator finalized, subscribed to while the
/// table keeps versions per batch.
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

/// A change's place in the log: its index among every change the table ever
/// recorded, so a ref below the log's `base` is collected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ChangeRef(
    /// The index plus one, so `Option<ChangeRef>` takes 8 bytes.
    NonZeroU64,
);

impl ChangeRef {
    fn new(index: u64) -> Self {
        Self(NonZeroU64::MIN.saturating_add(index))
    }

    fn index(self) -> u64 {
        self.0.get() - 1
    }
}

/// A change of one key.
struct Change {
    /// The key's live row before the change, if it had one.
    before: Option<RowLocation>,
    /// The key's previous change.
    previous: Option<ChangeRef>,
}

/// The changes of a table's keys, as of the last finalized checkpoint.
pub(super) struct Versions {
    /// `None` when the table keeps only the latest version.
    history: Option<History>,
}

struct History {
    /// The index of the front change.
    base: u64,
    /// In apply order, so collection pops from the front.
    changes: VecDeque<Change>,
    /// The newest change of each key that has no live row but still has
    /// history. Live keys keep theirs in the table's index.
    removed: HashMap<Box<[u8]>, ChangeRef>,
    /// Per epoch, the earliest mark among the batches that carried its
    /// marker.
    cuts: BTreeMap<u64, u64>,
    finalizers: FinalizerInbox,
}

impl History {
    fn end(&self) -> u64 {
        self.base + self.changes.len() as u64
    }
}

impl Versions {
    pub(super) fn new(retention: &VersionRetention) -> Self {
        Self {
            history: match retention {
                VersionRetention::Latest => None,
                VersionRetention::PerBatch { .. } => Some(History {
                    base: 0,
                    changes: VecDeque::new(),
                    removed: HashMap::new(),
                    cuts: BTreeMap::new(),
                    finalizers: FinalizerInbox::subscribe(),
                }),
            },
        }
    }

    /// The latest epoch finalized since the last call, for the caller to
    /// `collect`.
    pub(super) fn latest_finalized(&self) -> Option<u64> {
        self.history.as_ref()?.finalizers.latest_finalized()
    }

    /// Finishes applying a batch that carried the markers of `epochs`.
    /// Returns the mark its readers see the table at.
    pub(super) fn finish(&mut self, epochs: &[CheckpointEpoch]) -> Option<Mark> {
        let history = self.history.as_mut()?;
        let mark = history.end();
        for CheckpointEpoch(epoch) in epochs {
            history
                .cuts
                .entry(*epoch)
                .and_modify(|cut| *cut = (*cut).min(mark))
                .or_insert(mark);
        }
        Some(Mark(mark))
    }

    /// Records a change of a key whose live row was `before` and whose
    /// newest change was `previous`. Returns the new change, or `None` for
    /// a table that keeps no versions.
    pub(super) fn record(
        &mut self,
        previous: Option<ChangeRef>,
        before: Option<RowLocation>,
    ) -> Option<ChangeRef> {
        let history = self.history.as_mut()?;
        let index = history.end();
        history.changes.push_back(Change { before, previous });
        Some(ChangeRef::new(index))
    }

    /// The newest change of `key`, which is being inserted, from before it
    /// was removed.
    pub(super) fn take_removed(&mut self, key: &[u8]) -> Option<ChangeRef> {
        self.history.as_mut()?.removed.remove(key)
    }

    /// `key` just lost its live row in `newest_change`.
    pub(super) fn mark_removed(&mut self, key: Box<[u8]>, newest_change: ChangeRef) {
        if let Some(history) = &mut self.history {
            history.removed.insert(key, newest_change);
        }
    }

    /// The newest change of `key`, which has no live row.
    pub(super) fn removed_newest_change(&self, key: &[u8]) -> Option<ChangeRef> {
        self.history.as_ref()?.removed.get(key).copied()
    }

    /// Errors when the history a read at `mark` needs was collected.
    pub(super) fn ensure_readable(&self, mark: Option<Mark>) -> Result<()> {
        if let (Some(history), Some(Mark(mark))) = (&self.history, mark)
            && mark < history.base
        {
            streamling_bail!(
                "no table version at mark {}: it was already collected",
                mark
            );
        }
        Ok(())
    }

    /// The row a key had at `mark`, given its `current` live row and its
    /// `newest_change`: the row before the oldest change at or after the
    /// mark, or `current` if there's none.
    pub(super) fn resolve(
        &self,
        mark: Option<Mark>,
        current: Option<RowLocation>,
        newest_change: Option<ChangeRef>,
    ) -> Option<RowLocation> {
        let (Some(history), Some(Mark(mark))) = (&self.history, mark) else {
            return current;
        };
        let mut row = current;
        let mut next = newest_change;
        while let Some(reference) = next
            && reference.index() >= mark
        {
            let Some(change) = reference
                .index()
                .checked_sub(history.base)
                .and_then(|offset| usize::try_from(offset).ok())
                .and_then(|offset| history.changes.get(offset))
            else {
                break;
            };
            row = change.before;
            next = change.previous;
        }
        row
    }

    /// Drops the changes below the cut of the newest epoch up to `epoch`
    /// that has one, and forgets epochs up to `epoch`. A partition without
    /// the marker has finished, so it has no reads in flight. Returns the
    /// rows the dropped changes undid to, which no reader can ask for any
    /// more. Idempotent, and a later epoch covers a skipped one.
    pub(super) fn collect(&mut self, epoch: u64) -> Vec<RowLocation> {
        let Some(history) = &mut self.history else {
            return Vec::new();
        };
        let later = history.cuts.split_off(&(epoch + 1));
        // Every live partition carried each finalized epoch's marker, so the
        // newest one here gives the latest safe cut.
        let Some((_, cut)) = std::mem::replace(&mut history.cuts, later).pop_last() else {
            return Vec::new();
        };
        let count = usize::try_from(cut.saturating_sub(history.base))
            .unwrap_or(usize::MAX)
            .min(history.changes.len());
        let freed = history
            .changes
            .drain(..count)
            .filter_map(|change| change.before)
            .collect();
        history.base += count as u64;
        let base = history.base;
        history
            .removed
            .retain(|_, newest_change| newest_change.index() >= base);
        freed
    }

    /// How many history entries are held.
    pub(super) fn count(&self) -> usize {
        self.history
            .as_ref()
            .map_or(0, |history| history.changes.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"k";

    fn location(chunk: usize, row: usize) -> Option<RowLocation> {
        Some(RowLocation { chunk, row })
    }

    fn per_batch() -> Versions {
        Versions::new(&VersionRetention::PerBatch {
            stream: "stream".to_string(),
        })
    }

    fn sorted(mut rows: Vec<RowLocation>) -> Vec<(usize, usize)> {
        rows.sort_by_key(|row| (row.chunk, row.row));
        rows.into_iter().map(|row| (row.chunk, row.row)).collect()
    }

    /// Applies a batch that carried the markers of `epochs` and records
    /// `changes` as `(previous, before)`. Returns its mark and the new
    /// changes.
    fn apply(
        versions: &mut Versions,
        epochs: &[u64],
        changes: &[(Option<ChangeRef>, Option<RowLocation>)],
    ) -> (Option<Mark>, Vec<Option<ChangeRef>>) {
        let recorded = changes
            .iter()
            .map(|(previous, before)| versions.record(*previous, *before))
            .collect();
        let epochs: Vec<CheckpointEpoch> = epochs.iter().copied().map(CheckpointEpoch).collect();
        (versions.finish(&epochs), recorded)
    }

    /// Applies a batch with one change of a key.
    fn change(
        versions: &mut Versions,
        epochs: &[u64],
        previous: Option<ChangeRef>,
        before: Option<RowLocation>,
    ) -> (Option<Mark>, Option<ChangeRef>) {
        let (mark, recorded) = apply(versions, epochs, &[(previous, before)]);
        (mark, recorded[0])
    }

    fn read(
        versions: &Versions,
        mark: Option<Mark>,
        current: Option<RowLocation>,
        newest_change: Option<ChangeRef>,
    ) -> Option<RowLocation> {
        versions.ensure_readable(mark).unwrap();
        versions.resolve(mark, current, newest_change)
    }

    fn assert_collected(versions: &Versions, mark: Option<Mark>) {
        let error = versions.ensure_readable(mark).unwrap_err();
        assert!(error.is_internal());
        assert!(error.to_string().contains("already collected"), "{error}");
    }

    /// Applies a batch per entry of `epochs`, each changing the key with its
    /// row before in chunk `i`. Returns their marks and the newest change.
    fn chain(versions: &mut Versions, epochs: &[&[u64]]) -> (Vec<Option<Mark>>, Option<ChangeRef>) {
        let mut newest = None;
        let marks = epochs
            .iter()
            .enumerate()
            .map(|(chunk, epochs)| {
                let (mark, recorded) = change(versions, epochs, newest, location(chunk, 0));
                newest = recorded;
                mark
            })
            .collect();
        (marks, newest)
    }

    #[test]
    fn reads_undo_the_changes_after_their_mark() {
        let mut versions = per_batch();
        let (first_mark, first) = change(&mut versions, &[], None, None);
        let (other_mark, _) = change(&mut versions, &[], None, location(9, 0));
        let (newest_mark, newest) = change(&mut versions, &[], first, location(1, 0));

        let current = location(2, 0);
        assert_eq!(read(&versions, first_mark, current, newest), location(1, 0));
        assert_eq!(
            read(&versions, other_mark, current, newest),
            location(1, 0),
            "another key's change between them doesn't matter"
        );
        assert_eq!(read(&versions, newest_mark, current, newest), current);
        assert_eq!(read(&versions, None, current, newest), current);
    }

    #[test]
    fn several_changes_in_one_batch_undo_to_the_row_before_the_first() {
        let mut versions = per_batch();
        let (mark, _) = apply(&mut versions, &[], &[]);
        let first = versions.record(None, location(0, 0));
        let second = versions.record(first, location(1, 0));
        let newest = versions.record(second, location(1, 1));
        versions.finish(&[CheckpointEpoch(1)]);

        assert_eq!(read(&versions, mark, None, newest), location(0, 0));

        let freed = versions.collect(1);
        assert_eq!(versions.count(), 0);
        assert_eq!(sorted(freed), vec![(0, 0), (1, 0), (1, 1)]);
    }

    #[test]
    fn the_cut_is_the_earliest_mark_among_the_epochs_marker_batches() {
        let mut versions = per_batch();
        let (marks, newest) = chain(&mut versions, &[&[], &[1], &[], &[1]]);

        let freed = versions.collect(1);

        assert_eq!(sorted(freed), vec![(0, 0), (1, 0)], "below the second mark");
        assert_eq!(versions.count(), 2);
        assert_collected(&versions, marks[0]);
        let current = location(4, 0);
        assert_eq!(read(&versions, marks[1], current, newest), location(2, 0));
        assert_eq!(read(&versions, marks[2], current, newest), location(3, 0));
    }

    #[test]
    fn collect_is_idempotent_and_a_later_epoch_covers_a_skipped_one() {
        let mut versions = per_batch();
        let (marks, newest) = chain(&mut versions, &[&[1], &[2], &[3]]);

        versions.collect(1);
        assert_eq!(versions.count(), 2);
        assert!(versions.collect(1).is_empty());
        assert_eq!(versions.count(), 2);
        versions.collect(3);
        assert_eq!(versions.count(), 0);
        assert_collected(&versions, marks[1]);
        let current = location(4, 0);
        assert_eq!(read(&versions, marks[2], current, newest), current);
    }

    #[test]
    fn a_finalizer_without_markers_up_to_its_epoch_collects_nothing() {
        let mut versions = per_batch();
        chain(&mut versions, &[&[2]]);

        assert!(versions.collect(1).is_empty());
        assert_eq!(versions.count(), 1);
    }

    #[test]
    fn a_finalizer_for_an_epoch_without_markers_collects_up_to_an_earlier_epochs_cut() {
        let mut versions = per_batch();
        let (marks, newest) = chain(&mut versions, &[&[], &[1], &[]]);

        versions.collect(2);

        assert_eq!(versions.count(), 1);
        assert_collected(&versions, marks[0]);
        assert_eq!(
            read(&versions, marks[1], location(4, 0), newest),
            location(2, 0)
        );
    }

    #[test]
    fn latest_keeps_no_history() {
        let mut versions = Versions::new(&VersionRetention::Latest);
        assert_eq!(versions.latest_finalized(), None);
        assert_eq!(versions.record(None, location(1, 0)), None);
        assert_eq!(versions.finish(&[CheckpointEpoch(1)]), None);
        assert_eq!(versions.count(), 0);
        assert_eq!(
            versions.resolve(Some(Mark(0)), location(2, 0), None),
            location(2, 0)
        );
    }

    #[test]
    fn a_removed_and_reinserted_key_undoes_through_its_removal() {
        let mut versions = per_batch();
        let (insert_mark, insert) = change(&mut versions, &[], None, None);
        let (delete_mark, delete) = change(&mut versions, &[], insert, location(1, 0));
        let delete = delete.unwrap();
        versions.mark_removed(KEY.into(), delete);
        let previous = versions.take_removed(KEY);
        assert_eq!(previous, Some(delete));
        let reinsert = versions.record(previous, None);
        let reinsert_mark = versions.finish(&[]);

        let current = location(3, 0);
        assert_eq!(
            read(&versions, insert_mark, current, reinsert),
            location(1, 0)
        );
        assert_eq!(read(&versions, delete_mark, current, reinsert), None);
        assert_eq!(read(&versions, reinsert_mark, current, reinsert), current);
    }

    #[test]
    fn collect_forgets_removed_keys_whose_history_is_collected() {
        let mut versions = per_batch();
        let (_, delete) = change(&mut versions, &[1], None, location(0, 0));
        let delete = delete.unwrap();
        versions.mark_removed(KEY.into(), delete);
        assert_eq!(versions.removed_newest_change(KEY), Some(delete));

        versions.collect(1);
        assert_eq!(versions.removed_newest_change(KEY), None);
        assert_eq!(versions.count(), 0);
    }
}
