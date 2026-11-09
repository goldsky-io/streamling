//! A namespace's journal: for every key a version changed, the key's live
//! row before that version, so a reader at a mark can undo the versions at or
//! after it.
//!
//! An entry links to the same key's previous entry, so a key's journal is a
//! chain from its newest entry, which the namespace's index keeps for a live
//! key and `removed` keeps for a key without a live row. A read at mark `M`
//! walks the chain back while entries are at version `M` or later, and lands
//! on the row before the oldest of them.

use super::RowLocation;
use crate::table::backend::{Mark, Version};
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU64;

/// An entry's place: its index among every entry the journal ever recorded,
/// so a ref below `base` is trimmed.
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

/// The state of one key before one version.
struct Change {
    version: Version,
    /// The key's live row before the version, if it had one.
    before: Option<RowLocation>,
    /// The key's previous entry.
    previous: Option<ChangeRef>,
}

#[derive(Default)]
pub(super) struct Journal {
    /// The index of the front entry.
    base: u64,
    /// In version order, so trimming pops from the front.
    changes: VecDeque<Change>,
    /// The newest entry of each key that has no live row but still has a
    /// journal. Live keys keep theirs in the namespace's index.
    removed: HashMap<Box<[u8]>, ChangeRef>,
}

impl Journal {
    pub(super) fn end(&self) -> u64 {
        self.base + self.changes.len() as u64
    }

    fn change(&self, reference: ChangeRef) -> Option<&Change> {
        let offset = reference.index().checked_sub(self.base)?;
        self.changes.get(usize::try_from(offset).ok()?)
    }

    /// Whether `newest_change` is an entry of `version`: the key already
    /// changed in it, so its state before the version is journaled.
    pub(super) fn changed_in(&self, version: Version, newest_change: Option<ChangeRef>) -> bool {
        newest_change
            .and_then(|reference| self.change(reference))
            .is_some_and(|change| change.version == version)
    }

    /// Journals a key's live row `before` `version`, after its
    /// `previous` entry.
    pub(super) fn record(
        &mut self,
        version: Version,
        previous: Option<ChangeRef>,
        before: Option<RowLocation>,
    ) -> ChangeRef {
        let reference = ChangeRef::new(self.end());
        self.changes.push_back(Change {
            version,
            before,
            previous,
        });
        reference
    }

    /// Takes the newest entry of `key`, which is being inserted, from before
    /// it was removed.
    pub(super) fn take_removed(&mut self, key: &[u8]) -> Option<ChangeRef> {
        self.removed.remove(key)
    }

    /// Sets the newest entry of `key`, which has no live row, and returns the
    /// one it replaces.
    pub(super) fn set_removed(
        &mut self,
        key: Box<[u8]>,
        newest_change: Option<ChangeRef>,
    ) -> Option<ChangeRef> {
        match newest_change {
            Some(change) => self.removed.insert(key, change),
            None => self.removed.remove(&key),
        }
    }

    pub(super) fn removed_newest_change(&self, key: &[u8]) -> Option<ChangeRef> {
        self.removed.get(key).copied()
    }

    /// The row a key had at `mark`, given its `current` live row and its
    /// `newest_change`: the row before the oldest entry at or after the
    /// mark's version, or `current` if there's none.
    pub(super) fn resolve(
        &self,
        mark: Mark,
        current: Option<RowLocation>,
        newest_change: Option<ChangeRef>,
    ) -> Option<RowLocation> {
        let mut row = current;
        let mut next = newest_change;
        while let Some(change) = next.and_then(|reference| self.change(reference))
            && change.version >= mark.0
        {
            row = change.before;
            next = change.previous;
        }
        row
    }

    /// Drops the entries of versions below `horizon`, which no reader at or
    /// above it undoes. Returns the rows they held.
    pub(super) fn trim(&mut self, horizon: Mark) -> Vec<RowLocation> {
        let count = self
            .changes
            .iter()
            .take_while(|change| change.version < horizon.0)
            .count();
        let freed = self
            .changes
            .drain(..count)
            .filter_map(|change| change.before)
            .collect();
        self.base += count as u64;
        let base = self.base;
        self.removed
            .retain(|_, newest_change| newest_change.index() >= base);
        freed
    }

    /// Drops the entries from `end` on, which a rolled-back transaction
    /// recorded. Returns the rows they held.
    pub(super) fn truncate(&mut self, end: u64) -> Vec<RowLocation> {
        let keep = usize::try_from(end.saturating_sub(self.base))
            .unwrap_or(usize::MAX)
            .min(self.changes.len());
        self.changes
            .drain(keep..)
            .filter_map(|change| change.before)
            .collect()
    }

    pub(super) fn count(&self) -> usize {
        self.changes.len()
    }

    /// The rows the entries hold.
    #[cfg(test)]
    pub(super) fn befores(&self) -> impl Iterator<Item = RowLocation> + '_ {
        self.changes.iter().filter_map(|change| change.before)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"k";

    fn location(chunk: usize) -> Option<RowLocation> {
        Some(RowLocation { chunk, row: 0 })
    }

    #[test]
    fn reads_undo_the_versions_at_or_after_their_mark() {
        let mut journal = Journal::default();
        let first = journal.record(0, None, None);
        journal.record(1, None, location(9));
        let newest = journal.record(2, Some(first), location(1));
        let current = location(2);

        assert_eq!(
            journal.resolve(Mark(0), current, Some(newest)),
            None,
            "before the key's insert"
        );
        assert_eq!(journal.resolve(Mark(1), current, Some(newest)), location(1));
        assert_eq!(
            journal.resolve(Mark(2), current, Some(newest)),
            location(1),
            "another key's version between them doesn't matter"
        );
        assert_eq!(journal.resolve(Mark(3), current, Some(newest)), current);
    }

    #[test]
    fn trim_drops_the_versions_below_the_cut() {
        let mut journal = Journal::default();
        let first = journal.record(0, None, location(0));
        let second = journal.record(1, Some(first), location(1));
        journal.set_removed(KEY.into(), Some(first));

        assert_eq!(journal.trim(Mark(1)), [RowLocation { chunk: 0, row: 0 }]);
        assert_eq!(journal.count(), 1);
        assert_eq!(
            journal.removed_newest_change(KEY),
            None,
            "a removed key whose entries are trimmed is forgotten"
        );
        assert_eq!(
            journal.resolve(Mark(1), location(2), Some(second)),
            location(1),
            "the walk stops at a trimmed entry"
        );
        assert!(journal.trim(Mark(1)).is_empty(), "idempotent");
    }

    #[test]
    fn truncate_drops_a_rolled_back_versions_entries() {
        let mut journal = Journal::default();
        journal.record(0, None, location(0));
        let end = journal.end();
        let second = journal.record(1, None, location(1));
        assert!(journal.changed_in(1, Some(second)));

        assert_eq!(journal.truncate(end), [RowLocation { chunk: 1, row: 0 }]);
        assert_eq!(journal.end(), end);
        assert!(!journal.changed_in(1, Some(second)));
    }
}
