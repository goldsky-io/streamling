//! Checkpointed progress shared by one scan's partitions.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::Result as DataFusionResult;
use serde_derive::{Deserialize, Serialize};
use tracing::debug;

use streamling_core::checkpoints::checkpoint_management::CheckpointEpoch;
use streamling_state::{StateKey, StateOperatorBackend};

use super::to_df_err;

/// A file as progress records it. Ordered by `(last_modified_ms, path)`, the
/// order continuous discovery reads files in.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileKey {
    pub last_modified_ms: i64,
    pub path: String,
}

impl From<&PartitionedFile> for FileKey {
    fn from(file: &PartitionedFile) -> Self {
        Self {
            last_modified_ms: file.object_meta.last_modified.timestamp_millis(),
            path: file.object_meta.location.as_ref().to_string(),
        }
    }
}

/// A read mode's persisted progress, and how the files committed under a
/// finalized checkpoint fold into it.
pub(super) trait Progress: Send + 'static {
    type Persisted: serde::Serialize + serde::de::DeserializeOwned + Debug + Send + Sync + 'static;

    /// Folds `files` into the progress and returns the value to persist. Must be
    /// idempotent: after a failed `put`, a sibling partition's persist can pass
    /// the same files again before the job tears down.
    fn commit(&mut self, files: Vec<FileKey>) -> Self::Persisted;
}

/// Progress shared by one scan's partitions. The partitions take files from one
/// queue, so each one's emitted files are scattered across the read order; the
/// tracker keeps them per partition, in emission order, and commits the ones
/// each partition emitted before a finalized epoch's marker.
///
/// Persisting an epoch is consistent because a live partition's `Marker` for an
/// epoch precedes that epoch's `Finalizer` in its own queue, and the coordinator
/// sends the `Finalizer` only once every sink acked, which waits on every live
/// partition's marker copy (or its stream ending).
pub(super) struct ProgressTracker<P: Progress> {
    state_backend: Arc<dyn StateOperatorBackend<P::Persisted>>,
    state_key: StateKey,
    state: Mutex<TrackerState<P>>,
    /// Serializes persists so an older epoch's `put` can never land after a
    /// newer one's.
    persist_lock: tokio::sync::Mutex<()>,
}

struct TrackerState<P> {
    progress: P,
    persisted_epoch: Option<CheckpointEpoch>,
    /// Files each partition has fully emitted since the last persist, in
    /// emission order.
    emitted: Vec<Vec<FileKey>>,
    /// Whether each partition's stream has ended.
    ended: Vec<bool>,
    /// How many of each partition's `emitted` files preceded an epoch's `Marker`
    /// on its stream.
    snapshots: BTreeMap<CheckpointEpoch, HashMap<usize, usize>>,
}

impl<P: Progress> ProgressTracker<P> {
    pub(super) fn new(
        state_backend: Arc<dyn StateOperatorBackend<P::Persisted>>,
        state_key: StateKey,
        progress: P,
        partitions: usize,
    ) -> Self {
        Self {
            state_backend,
            state_key,
            state: Mutex::new(TrackerState {
                progress,
                persisted_epoch: None,
                emitted: vec![Vec::new(); partitions],
                ended: vec![false; partitions],
                snapshots: BTreeMap::new(),
            }),
            persist_lock: tokio::sync::Mutex::new(()),
        }
    }

    pub(super) fn advance(&self, partition: usize, file: FileKey) {
        self.state().emitted[partition].push(file);
    }

    pub(super) fn snapshot(&self, partition: usize, epoch: &CheckpointEpoch) {
        let mut state = self.state();
        let emitted = state.emitted[partition].len();
        state
            .snapshots
            .entry(epoch.clone())
            .or_default()
            .insert(partition, emitted);
    }

    /// Marks `partition`'s stream as ended, returning whether it was the last
    /// partition still reading.
    pub(super) fn finish(&self, partition: usize) -> bool {
        let mut state = self.state();
        state.ended[partition] = true;
        state.ended.iter().all(|ended| *ended)
    }

    pub(super) fn with_progress<R>(&self, access: impl FnOnce(&mut P) -> R) -> R {
        access(&mut self.state().progress)
    }

    /// Commits the files each partition emitted before `epoch`'s marker and
    /// `put`s the result once. Epochs at or below the last one persisted are
    /// no-ops, so each partition can handle its own copy of the same `Finalizer`.
    pub(super) async fn persist(&self, epoch: &CheckpointEpoch) -> DataFusionResult<()> {
        let _serialized = self.persist_lock.lock().await;
        let (persisted, counted) = {
            let mut guard = self.state();
            let state = &mut *guard;
            if state
                .persisted_epoch
                .as_ref()
                .is_some_and(|persisted| persisted >= epoch)
            {
                return Ok(());
            }
            let recorded = state.snapshots.get(epoch);
            let counted: Vec<usize> = (0..state.emitted.len())
                .map(
                    |partition| match recorded.and_then(|counts| counts.get(&partition)) {
                        Some(count) => *count,
                        // An ended stream that never carried the epoch's marker
                        // precedes it at every merge point, so all of its files count.
                        None if state.ended[partition] => state.emitted[partition].len(),
                        // Unreachable for a partition subscribed before the marker
                        // (see the type docs); counting nothing is safe.
                        None => 0,
                    },
                )
                .collect();
            let files = state
                .emitted
                .iter()
                .zip(&counted)
                .flat_map(|(files, count)| files[..*count].iter().cloned())
                .collect();
            (state.progress.commit(files), counted)
        };

        debug!(
            "Persisting file source progress {:?} on finalize of epoch {:?}",
            persisted, epoch
        );
        self.state_backend
            .put(self.state_key.clone(), persisted)
            .await
            .map_err(to_df_err)?;

        // Partitions may have emitted more during the `put`; only each one's
        // counted prefix is persisted now.
        let mut state = self.state();
        for (files, count) in state.emitted.iter_mut().zip(&counted) {
            files.drain(..*count);
        }
        state.snapshots.retain(|recorded, _| recorded > epoch);
        for counts in state.snapshots.values_mut() {
            for (partition, count) in counts.iter_mut() {
                *count = count.saturating_sub(counted[*partition]);
            }
        }
        state.persisted_epoch = Some(epoch.clone());
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn emitted(&self, partition: usize) -> Vec<FileKey> {
        self.state().emitted[partition].clone()
    }

    fn state(&self) -> MutexGuard<'_, TrackerState<P>> {
        self.state
            .lock()
            .expect("file source progress tracker mutex poisoned")
    }
}
