//! Immutable views of a table, one per position of the stream that applies
//! it, so a reader sees the table as of the batch it is processing however
//! far the table's input has run ahead.
//!
//! Each partition of the table's input applies its batches in order and
//! publishes a snapshot after each. A reader reads the snapshot of its
//! batch's position. Snapshots share structure with the table and each
//! other, so publishing one costs only what the batch changed.
//!
//! Snapshots are collected on finalized checkpoints: a `Finalizer(e)` means
//! every sink acked `e`, so every reader has processed each partition's
//! batch that carried marker `e`, and everything before it.

use super::RowLocation;
use crate::checkpoints::channels::{SubscriberId, subscribe_with_id, unsubscribe};
use crate::checkpoints::checkpoint_management::{
    CHECKPOINT_COORDINATOR_CHANNEL, CheckpointEpoch, CheckpointMessage,
};
use crate::error::Result;
use crate::{streamling_bail, streamling_err};
use arrow::array::RecordBatch;
use crossbeam::channel::Receiver;
use imbl::{HashMap as PersistentHashMap, Vector};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// Primary key, encoded by the table's `RowConverter`, to its live row.
pub(super) type Index = PersistentHashMap<Arc<[u8]>, RowLocation>;

const POSITION_SEPARATOR: char = ':';

/// Where a batch sits in the stream of the node that applies a table: its
/// partition, and its number among that partition's batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    pub partition: usize,
    pub seq: u64,
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{POSITION_SEPARATOR}{}", self.partition, self.seq)
    }
}

impl FromStr for Position {
    type Err = crate::error::StreamlingError;

    fn from_str(value: &str) -> Result<Self> {
        let invalid = || streamling_err!("invalid table position '{}'", value);
        let (partition, seq) = value.split_once(POSITION_SEPARATOR).ok_or_else(invalid)?;
        Ok(Self {
            partition: partition.parse().map_err(|_| invalid())?,
            seq: seq.parse().map_err(|_| invalid())?,
        })
    }
}

/// Which snapshots a table keeps.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotRetention {
    /// Only the latest, which every read gets.
    Latest,
    /// One per position of `stream`, the node whose side output applies the
    /// table, until a finalized checkpoint shows no reader needs it.
    PerPosition { stream: String },
}

/// The table's live rows as of one position.
pub(super) struct Snapshot {
    /// Chunk batches, from chunk `first_chunk_id` on.
    pub(super) batches: Vector<RecordBatch>,
    pub(super) first_chunk_id: usize,
    pub(super) index: Index,
}

impl Snapshot {
    pub(super) fn batch(&self, chunk: usize) -> &RecordBatch {
        &self.batches[chunk - self.first_chunk_id]
    }
}

/// The epochs the checkpoint coordinator finalized, subscribed to while the
/// table keeps snapshots per position.
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

/// The snapshots a table publishes.
pub(super) struct Snapshots {
    latest: Arc<Snapshot>,
    retention: Retention,
}

/// What a table keeps besides its latest snapshot.
enum Retention {
    /// Nothing: every read gets the latest snapshot.
    Latest,
    PerPosition {
        /// Per partition, by sequence number, oldest first.
        retained: HashMap<usize, VecDeque<(u64, Arc<Snapshot>)>>,
        /// Per epoch, the positions of the batches that carried its marker.
        markers: BTreeMap<u64, Vec<Position>>,
        finalizers: FinalizerInbox,
    },
}

impl Snapshots {
    pub(super) fn new(retention: &SnapshotRetention, empty: Snapshot) -> Self {
        Self {
            latest: Arc::new(empty),
            retention: match retention {
                SnapshotRetention::Latest => Retention::Latest,
                SnapshotRetention::PerPosition { .. } => Retention::PerPosition {
                    retained: HashMap::new(),
                    markers: BTreeMap::new(),
                    finalizers: FinalizerInbox::subscribe(),
                },
            },
        }
    }

    /// Makes `snapshot` the latest, and the one of `position` when the table
    /// keeps snapshots per position. Then collects what the finalizers that
    /// arrived since the last publish free.
    pub(super) fn publish(&mut self, snapshot: Snapshot, position: Option<Position>) -> Result<()> {
        let snapshot = Arc::new(snapshot);
        let finalized = match &mut self.retention {
            Retention::Latest => None,
            Retention::PerPosition {
                retained,
                finalizers,
                ..
            } => {
                let Some(position) = position else {
                    streamling_bail!(
                        "a table that keeps a snapshot per position got a batch without one"
                    );
                };
                let finalized = finalizers.latest_finalized();
                let partition = retained.entry(position.partition).or_default();
                if partition
                    .back()
                    .is_some_and(|(seq, _)| *seq >= position.seq)
                {
                    streamling_bail!(
                        "table position {} is not after the last one applied in its partition",
                        position
                    );
                }
                partition.push_back((position.seq, Arc::clone(&snapshot)));
                finalized
            }
        };
        if let Some(epoch) = finalized {
            self.collect(epoch);
        }
        self.latest = snapshot;
        Ok(())
    }

    /// The snapshot a read at `position` gets: the latest without a position
    /// or per-position snapshots.
    pub(super) fn at(&self, position: Option<Position>) -> Result<Arc<Snapshot>> {
        let (Some(position), Retention::PerPosition { retained, .. }) = (position, &self.retention)
        else {
            return Ok(Arc::clone(&self.latest));
        };
        let partition = retained.get(&position.partition);
        let found = partition.and_then(|partition| {
            partition
                .binary_search_by_key(&position.seq, |(seq, _)| *seq)
                .ok()
                .map(|found| Arc::clone(&partition[found].1))
        });
        found.ok_or_else(|| {
            let collected = partition
                .and_then(VecDeque::front)
                .is_some_and(|(oldest, _)| position.seq < *oldest);
            streamling_err!(
                "no table snapshot at position {}: it was {}",
                position,
                if collected {
                    "already collected"
                } else {
                    "never published"
                }
            )
        })
    }

    /// `position` is the batch that carried the marker of `epoch`.
    pub(super) fn record_marker(&mut self, epoch: &CheckpointEpoch, position: Position) {
        if let Retention::PerPosition { markers, .. } = &mut self.retention {
            markers.entry(epoch.0).or_default().push(position);
        }
    }

    /// Drops each partition's snapshots up to the last batch that carried a
    /// marker of `epoch` or an earlier one, always keeping the latest, and
    /// forgets those epochs. Idempotent, and a later epoch covers a skipped
    /// one.
    pub(super) fn collect(&mut self, epoch: u64) {
        let Retention::PerPosition {
            retained, markers, ..
        } = &mut self.retention
        else {
            return;
        };
        let later = markers.split_off(&(epoch + 1));
        let finalized = std::mem::replace(markers, later);
        let mut cuts: HashMap<usize, u64> = HashMap::new();
        for position in finalized.into_values().flatten() {
            let cut = cuts.entry(position.partition).or_insert(position.seq);
            *cut = (*cut).max(position.seq);
        }
        for (partition, cut) in cuts {
            if let Some(snapshots) = retained.get_mut(&partition) {
                while snapshots.len() > 1 && snapshots.front().is_some_and(|(seq, _)| *seq <= cut) {
                    snapshots.pop_front();
                }
            }
        }
    }

    /// How many snapshots are held.
    pub(super) fn count(&self) -> usize {
        match &self.retention {
            Retention::Latest => 1,
            Retention::PerPosition { retained, .. } => retained.values().map(VecDeque::len).sum(),
        }
    }

    /// The oldest chunk any held snapshot reads.
    pub(super) fn oldest_chunk_id(&self) -> usize {
        match &self.retention {
            Retention::Latest => self.latest.first_chunk_id,
            Retention::PerPosition { retained, .. } => retained
                .values()
                .filter_map(|snapshots| snapshots.front())
                .map(|(_, snapshot)| snapshot.first_chunk_id)
                .fold(self.latest.first_chunk_id, usize::min),
        }
    }
}
