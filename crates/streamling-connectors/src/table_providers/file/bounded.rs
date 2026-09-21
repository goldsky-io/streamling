//! Bounded mode: the shared file queue, per-file progress, and the terminal
//! checkpoint that closes the source.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::Result as DataFusionResult;
use serde_derive::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tracing::{info, warn};

use streamling_core::checkpoints::checkpoint_management::{
    CheckpointControl, CheckpointEpoch, CheckpointMessage, now_ms,
};
use streamling_core::telemetry::recorder::get_control_plane_metrics_recorder;
use streamling_state::StateKey;

use super::checkpoint::{CheckpointInbox, send_checkpoint_messages};
use super::progress::{FileKey, Progress, ProgressTracker};
use super::reader::{PartitionReader, ReadEnd, ReadPlan, RoundOutcome, read_rounds};

/// Metrics component the checkpoint counters are grouped under.
const CHECKPOINT_METRICS_COMPONENT: &str = "checkpoint_coordinator";

/// Persisted progress for the bounded file source: the files already emitted
/// under a finalized checkpoint, as path ranges coalesced over the listing. A gap
/// between ranges is a file that was in flight, or emitted after its partition's
/// last finalized marker, and is re-read on restart.
///
/// A file uploaded after the job starts that sorts inside a completed range is
/// never read. That is correct for bounded mode, whose file set is conceptually
/// snapshotted when the job starts.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct BoundedProgress {
    pub completed: Vec<PathRange>,
}

impl BoundedProgress {
    pub(super) fn covers(&self, path: &str) -> bool {
        self.completed.iter().any(|range| range.contains(path))
    }
}

/// An inclusive range of object paths, compared as strings — the order the
/// listing is sorted in.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct PathRange {
    pub first: String,
    pub last: String,
}

impl PathRange {
    fn contains(&self, path: &str) -> bool {
        self.first.as_str() <= path && path <= self.last.as_str()
    }
}

/// Separate from [`watermark_state_key`], so switching a source's mode can never
/// half-read one state format as the other.
pub(super) fn bounded_progress_state_key(reference_name: &str) -> StateKey {
    StateKey::from(format!("{reference_name}:bounded_progress"))
}

/// Sorts `ranges` and merges the ones that overlap or have no path of the sorted
/// `listing` between them, so persisted progress stays a handful of ranges no
/// matter how many runs and partitions produced it.
fn coalesce_ranges(mut ranges: Vec<PathRange>, listing: &[String]) -> Vec<PathRange> {
    ranges.sort_by(|a, b| a.first.cmp(&b.first));
    let mut coalesced: Vec<PathRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match coalesced.last_mut() {
            Some(previous)
                if range.first <= previous.last
                    || !lists_path_between(listing, &previous.last, &range.first) =>
            {
                if range.last > previous.last {
                    previous.last = range.last;
                }
            }
            _ => coalesced.push(range),
        }
    }
    coalesced
}

/// Whether the sorted `listing` holds a path strictly between `low` and `high`.
fn lists_path_between(listing: &[String], low: &str, high: &str) -> bool {
    let after_low = listing.partition_point(|path| path.as_str() <= low);
    listing
        .get(after_low)
        .is_some_and(|path| path.as_str() < high)
}

/// One bounded partition's reader of the scan's shared file queue. As with
/// DataFusion's `SharedWorkSource`, whichever partition finishes a file first
/// takes the next unopened one, so skewed file sizes don't leave partitions idle.
/// A round is one file, so a round boundary is a file boundary and progress needs
/// no row-to-file attribution.
pub(super) struct BoundedPlan {
    pub(super) partition: usize,
    pub(super) queue: Arc<Mutex<VecDeque<PartitionedFile>>>,
    pub(super) tracker: Arc<ProgressTracker<RangeProgress>>,
}

#[async_trait]
impl ReadPlan for BoundedPlan {
    async fn next_round(&mut self) -> DataFusionResult<RoundOutcome> {
        let next = self
            .queue
            .lock()
            .expect("bounded file queue mutex poisoned")
            .pop_front();
        Ok(match next {
            Some(file) => RoundOutcome::Files(vec![file]),
            None => RoundOutcome::Exhausted,
        })
    }

    fn advance(&mut self, round: &[PartitionedFile]) {
        for file in round {
            self.tracker.advance(self.partition, FileKey::from(file));
        }
    }

    fn snapshot(&mut self, epoch: &CheckpointEpoch) {
        self.tracker.snapshot(self.partition, epoch);
    }

    async fn persist(&mut self, epoch: &CheckpointEpoch) -> DataFusionResult<()> {
        self.tracker.persist(epoch).await
    }
}

/// Bounded progress: the completed files as path ranges, coalesced over the
/// listing.
pub(super) struct RangeProgress {
    /// Every path listed at scan time, sorted, so ranges adjacent in the
    /// listing coalesce.
    listing: Vec<String>,
    progress: BoundedProgress,
}

impl RangeProgress {
    pub(super) fn new(listing: Vec<String>, persisted: BoundedProgress) -> Self {
        Self {
            listing,
            progress: persisted,
        }
    }
}

impl Progress for RangeProgress {
    type Persisted = BoundedProgress;

    fn commit(&mut self, files: Vec<FileKey>) -> BoundedProgress {
        let mut completed = std::mem::take(&mut self.progress.completed);
        completed.extend(files.into_iter().map(|file| PathRange {
            first: file.path.clone(),
            last: file.path,
        }));
        self.progress.completed = coalesce_ranges(completed, &self.listing);
        self.progress.clone()
    }
}

/// Runs one bounded partition to the end of its stream. After the read loop,
/// checkpoint messages that arrived after the last batch are flushed, and the
/// source is closed with a terminal checkpoint round-trip as the hybrid source
/// does: the terminal `Marker` after all data, a wait for the epoch to finalize,
/// then the progress persist and a `Finalizer`.
pub(super) async fn read_bounded_partition(
    mut plan: BoundedPlan,
    mut reader: PartitionReader,
    mut inbox: CheckpointInbox,
    checkpoint_control: Option<CheckpointControl>,
    tx: Sender<DataFusionResult<RecordBatch>>,
) -> DataFusionResult<()> {
    let end = read_rounds(&mut plan, &mut reader, &mut inbox, &tx).await;
    let is_last = plan.tracker.finish(plan.partition);
    let end = end?;
    if end == ReadEnd::Disconnected {
        return Ok(());
    }
    // Drain once more so a marker that arrived after the last batch still
    // reaches the sinks.
    inbox.drain(&mut plan).await?;

    // A partition that exhausts while others still read must not mint the
    // terminal epoch: that stops periodic checkpoints pipeline-wide. Every
    // partition stopped by shutdown does (the first caller mints, the rest reuse
    // it), because the aligners wait for a terminal marker copy per live stream.
    let terminal = checkpoint_control
        .filter(|_| end == ReadEnd::Stopped || is_last)
        .map(|control| {
            let epoch = control.begin_terminal_checkpoint();
            inbox.buffer.push(CheckpointMessage::Marker {
                epoch: epoch.clone(),
                created_at_ms: now_ms(),
            });
            (control, epoch)
        });
    let messages = std::mem::take(&mut inbox.buffer);
    if !send_checkpoint_messages(&tx, &reader.output_schema, messages).await {
        return Ok(());
    }
    let Some((control, epoch)) = terminal else {
        return Ok(());
    };

    // The send above is buffered and the sinks pull concurrently, so awaiting
    // here does not block their consumption of the marker batch.
    if !control.await_terminal_finalized_on_completion().await {
        // At least one live sink never confirmed its writes. Persisting would
        // advance progress past unconfirmed files and a restart would skip them,
        // so persist nothing and withhold the Finalizer; the files replay.
        warn!(
            "File source '{}': terminal checkpoint epoch {} did not finalize (sinks that never \
             acked: {:?}); SKIPPING the progress persist and terminal Finalizer so the \
             unconfirmed files are re-read on restart",
            reader.reference_name,
            epoch.0,
            control.pending_terminal_ack_sinks()
        );
        get_control_plane_metrics_recorder(CHECKPOINT_METRICS_COMPONENT)
            .record_count("checkpoint_terminal_finalizer_skipped", 1);
        return Ok(());
    }
    plan.tracker.persist(&epoch).await?;
    info!(
        "File source '{}': terminal epoch {} finalized; progress persisted",
        reader.reference_name, epoch.0
    );
    send_checkpoint_messages(
        &tx,
        &reader.output_schema,
        vec![CheckpointMessage::Finalizer(epoch)],
    )
    .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_providers::file::provider::{ExecMode, FileSourceExec};
    use crate::table_providers::file::test_support::*;
    use datafusion::datasource::TableProvider;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
    use futures::StreamExt;

    fn tracker_of(plan: &Arc<dyn ExecutionPlan>) -> Arc<ProgressTracker<RangeProgress>> {
        let exec = plan
            .downcast_ref::<FileSourceExec>()
            .expect("a file source plan");
        match &exec.mode {
            ExecMode::Bounded { tracker, .. } => tracker.clone(),
            ExecMode::Continuous { .. } => panic!("expected a bounded plan"),
        }
    }

    fn range(first: &str, last: &str) -> PathRange {
        PathRange {
            first: first.to_string(),
            last: last.to_string(),
        }
    }

    fn file(path: &str) -> FileKey {
        FileKey {
            last_modified_ms: 0,
            path: path.to_string(),
        }
    }

    #[test]
    fn coalesce_ranges_merges_only_ranges_adjacent_in_the_listing() {
        let listing = |paths: &[&str]| {
            paths
                .iter()
                .map(|path| path.to_string())
                .collect::<Vec<_>>()
        };

        // `b` sits between the ranges, so they stay apart.
        assert_eq!(
            coalesce_ranges(
                vec![range("c", "c"), range("a", "a")],
                &listing(&["a", "b", "c"])
            ),
            vec![range("a", "a"), range("c", "c")]
        );
        // Nothing listed between them (`b` was deleted): they merge.
        assert_eq!(
            coalesce_ranges(
                vec![range("c", "d"), range("a", "a")],
                &listing(&["a", "c", "d"])
            ),
            vec![range("a", "d")]
        );
        // Overlapping ranges merge whatever the listing holds.
        assert_eq!(
            coalesce_ranges(
                vec![range("a", "c"), range("b", "b")],
                &listing(&["a", "b", "c"])
            ),
            vec![range("a", "c")]
        );
    }

    #[tokio::test]
    async fn bounded_progress_tracker_persists_the_files_emitted_before_each_marker() {
        let state_backend = bounded_backend("tracker");
        let key = bounded_progress_state_key("tracker_src");
        let listing = ["a", "b", "c", "d", "e", "f"].map(String::from).to_vec();
        let tracker = ProgressTracker::new(
            state_backend.clone(),
            key.clone(),
            RangeProgress::new(listing, BoundedProgress::default()),
            2,
        );
        let persisted = || async {
            state_backend
                .get(key.clone())
                .await
                .unwrap()
                .unwrap_or_default()
                .completed
        };

        // The partitions take files from one queue, so their files interleave.
        tracker.advance(0, file("a"));
        tracker.snapshot(0, &CheckpointEpoch(1));
        // Emitted after partition 0's marker copy: must not count toward epoch 1.
        tracker.advance(0, file("c"));
        tracker.advance(1, file("b"));
        tracker.snapshot(1, &CheckpointEpoch(1));
        assert!(persisted().await.is_empty(), "a marker persists nothing");

        tracker.persist(&CheckpointEpoch(1)).await.unwrap();
        assert_eq!(persisted().await, vec![range("a", "b")]);

        // Partition 1 finishes `e` while `d` is still in flight on partition 0.
        tracker.advance(1, file("e"));
        tracker.snapshot(0, &CheckpointEpoch(2));
        tracker.snapshot(1, &CheckpointEpoch(2));
        tracker.persist(&CheckpointEpoch(2)).await.unwrap();
        assert_eq!(
            persisted().await,
            vec![range("a", "c"), range("e", "e")],
            "the file in flight stays a gap"
        );

        // Partition 0 finishes `d` and its stream ends.
        tracker.advance(0, file("d"));
        assert!(!tracker.finish(0), "partition 1 is still reading");
        tracker.advance(1, file("f"));

        // Duplicate and older finalizers are no-ops, even though partition 0 has
        // since ended with `d` emitted.
        tracker.persist(&CheckpointEpoch(2)).await.unwrap();
        tracker.persist(&CheckpointEpoch(1)).await.unwrap();
        assert_eq!(persisted().await, vec![range("a", "c"), range("e", "e")]);

        // Partition 0 ended without seeing epoch 3's marker, so every file it
        // emitted counts, and the gap closes.
        tracker.snapshot(1, &CheckpointEpoch(3));
        tracker.persist(&CheckpointEpoch(3)).await.unwrap();
        assert_eq!(persisted().await, vec![range("a", "f")]);

        // Only the partition that takes the live count to zero is the last one.
        assert!(
            tracker.finish(1),
            "the last partition to finish is the last"
        );
    }

    /// The bounded source splits its files across the session's target partitions
    /// and keeps them as separate output partitions, which stay parallel through
    /// transforms and into `ParallelSinkExec`'s concurrent per-partition writes.
    #[tokio::test]
    async fn bounded_source_splits_files_across_partitions() {
        let dir = temp_dir_with("bounded", &csv_files(3));
        let (session_manager, provider) =
            bounded_provider(&dir, "bounded_src", None, bounded_backend("split"), None).await;

        let state = session_manager.session_state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let expected_partitions = state.config().target_partitions().min(3);
        assert_eq!(
            plan.output_partitioning().partition_count(),
            expected_partitions,
            "the bounded source must spread its files across the target partitions"
        );

        let batches = collect_partitions(&plan, &session_manager).await;
        let _ = std::fs::remove_dir_all(&dir);
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            rows, 9,
            "every row must arrive exactly once across partitions"
        );
    }

    /// On resume, files covered by persisted ranges are dropped before the split;
    /// a listing they fully cover plans one empty partition that just ends.
    #[tokio::test]
    async fn bounded_scan_skips_files_covered_by_persisted_progress() {
        let dir = temp_dir_with("bounded_resume", &csv_files(4));
        let state_backend = bounded_backend("resume");
        let (session_manager, provider) =
            bounded_provider(&dir, "resume_src", Some(4), state_backend.clone(), None).await;
        let prefix = provider.discovery.table_url.prefix().clone();
        let path_of = |file: &str| prefix.clone().join(file).as_ref().to_string();
        let key = bounded_progress_state_key("resume_src");

        state_backend
            .put(
                key.clone(),
                BoundedProgress {
                    completed: vec![range(&path_of("0.csv"), &path_of("1.csv"))],
                },
            )
            .await
            .unwrap();
        let plan = provider
            .scan(&session_manager.session_state(), None, &[], None)
            .await
            .unwrap();
        assert_eq!(
            plan.output_partitioning().partition_count(),
            2,
            "only the two uncovered files are planned"
        );
        let mut ids = int64_values(&collect_partitions(&plan, &session_manager).await, "id");
        ids.sort_unstable();
        assert_eq!(ids, vec![20, 21, 22, 30, 31, 32]);

        state_backend
            .put(
                key,
                BoundedProgress {
                    completed: vec![range(&path_of("0.csv"), &path_of("3.csv"))],
                },
            )
            .await
            .unwrap();
        let plan = provider
            .scan(&session_manager.session_state(), None, &[], None)
            .await
            .unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(plan.output_partitioning().partition_count(), 1);
        let rows: usize = collect_partitions(&plan, &session_manager)
            .await
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 0, "a fully covered listing reads nothing");
    }

    #[tokio::test]
    async fn bounded_progress_advances_only_after_a_file_is_fully_emitted() {
        let dir = temp_dir_with("bounded_advance", &csv_files(2));
        let rows =
            |batches: Vec<RecordBatch>| batches.iter().map(RecordBatch::num_rows).sum::<usize>();

        // A one-row record limit stops the partition right after the first file's
        // only batch, before that file's stream reports its end.
        let (session_manager, provider) = bounded_provider(
            &dir,
            "advance_src",
            Some(1),
            bounded_backend("advance"),
            Some(1),
        )
        .await;
        let plan = provider
            .scan(&session_manager.session_state(), None, &[], None)
            .await
            .unwrap();
        assert_eq!(rows(collect_partitions(&plan, &session_manager).await), 3);
        assert!(
            tracker_of(&plan).emitted(0).is_empty(),
            "a file whose stream never ended must not count as emitted"
        );

        let (session_manager, provider) = bounded_provider(
            &dir,
            "advance_src",
            Some(1),
            bounded_backend("advance"),
            None,
        )
        .await;
        let plan = provider
            .scan(&session_manager.session_state(), None, &[], Some(2))
            .await
            .unwrap();
        assert_eq!(rows(collect_partitions(&plan, &session_manager).await), 2);
        assert!(
            tracker_of(&plan).emitted(0).is_empty(),
            "a file the scan limit truncated must not count as emitted"
        );

        let plan = provider
            .scan(&session_manager.session_state(), None, &[], None)
            .await
            .unwrap();
        assert_eq!(rows(collect_partitions(&plan, &session_manager).await), 6);
        let prefix = provider.discovery.table_url.prefix().clone();
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            tracker_of(&plan)
                .emitted(0)
                .into_iter()
                .map(|file| file.path)
                .collect::<Vec<_>>(),
            vec![
                prefix.clone().join("0.csv").as_ref().to_string(),
                prefix.clone().join("1.csv").as_ref().to_string(),
            ],
            "reading to EOF records every file as emitted"
        );
    }

    /// Like DataFusion's `SharedWorkSource`, the partitions share one queue of
    /// unopened files: a partition that runs ahead takes the files the others
    /// haven't started, instead of idling while they read their own share.
    #[tokio::test]
    async fn bounded_partitions_share_one_file_queue() {
        let dir = temp_dir_with("bounded_queue", &csv_files(4));
        let (session_manager, provider) =
            bounded_provider(&dir, "queue_src", Some(2), bounded_backend("queue"), None).await;
        let plan = provider
            .scan(&session_manager.session_state(), None, &[], None)
            .await
            .unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 2);

        // Partition 0 runs to the end before partition 1 starts.
        let mut partition_rows = Vec::new();
        for partition in 0..2 {
            let mut stream = plan
                .execute(partition, session_manager.session_context().task_ctx())
                .unwrap();
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch.unwrap().num_rows();
            }
            partition_rows.push(rows);
        }
        let _ = std::fs::remove_dir_all(&dir);

        assert_eq!(
            partition_rows,
            vec![12, 0],
            "the partition that runs ahead reads every file; the other finds the queue empty"
        );
    }
}
