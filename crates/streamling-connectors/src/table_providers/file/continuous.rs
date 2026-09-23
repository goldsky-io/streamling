//! Continuous mode: watermark-driven polling for new files, shared by the scan's
//! partitions.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::Result as DataFusionResult;
use object_store::ObjectStore;
use serde_derive::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;

use streamling_core::checkpoints::checkpoint_management::CheckpointEpoch;
use streamling_state::StateKey;

use super::checkpoint::CheckpointInbox;
use super::discovery::FileDiscovery;
use super::progress::{FileKey, Progress, ProgressTracker};
use super::reader::{PartitionReader, ReadPlan, RoundOutcome, read_rounds};

/// Persisted discovery position for the continuous file source.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct FileWatermark {
    /// Maximum object `last_modified` (epoch milliseconds) already ingested.
    pub last_modified_ms: i64,
    /// Paths of files already ingested whose `last_modified` equals
    /// `last_modified_ms`. New files that share that boundary timestamp (common
    /// with second-granularity object-store mtimes) are still picked up, while the
    /// ones already done are not reprocessed. Bounded by the number of files
    /// sharing the latest timestamp; it resets whenever a newer file advances the
    /// watermark.
    #[serde(default)]
    pub boundary_paths: Vec<String>,
    /// Files committed after the first file still being read, which the
    /// watermark can't pass yet. A restart skips them instead of re-reading
    /// everything after that file; the list empties as the watermark catches up.
    #[serde(default)]
    pub committed_ahead: Vec<FileKey>,
}

impl FileWatermark {
    /// Whether the watermark accounts for a file: it is older than the watermark,
    /// or shares its timestamp and was already ingested. The boundary case keeps
    /// new files that land at the exact watermark second (common with
    /// second-granularity object-store mtimes) from being missed.
    fn covers(&self, last_modified_ms: i64, path: &str) -> bool {
        last_modified_ms < self.last_modified_ms
            || (last_modified_ms == self.last_modified_ms
                && self.boundary_paths.iter().any(|ingested| ingested == path))
    }
}

pub(super) fn watermark_state_key(reference_name: &str) -> StateKey {
    StateKey::from(format!("{reference_name}:watermark"))
}

/// Advances the watermark after a round's files are ingested. `new_boundary` are
/// the paths ingested at `max_seen`. When `max_seen` matches the current second (a
/// boundary), the boundary set is unioned so future files at that timestamp are
/// still picked up; when it advances, the watermark and boundary set reset.
fn advance_watermark(watermark: &mut FileWatermark, max_seen: i64, new_boundary: Vec<String>) {
    if max_seen == watermark.last_modified_ms {
        let mut combined: HashSet<String> = std::mem::take(&mut watermark.boundary_paths)
            .into_iter()
            .collect();
        combined.extend(new_boundary);
        watermark.boundary_paths = combined.into_iter().collect();
    } else {
        watermark.last_modified_ms = max_seen;
        watermark.boundary_paths = new_boundary;
    }
}

/// Continuous progress: a watermark over the files committed in
/// `(last_modified, path)` order. Partitions finish files out of that order, so a
/// committed file waits in `outstanding` until every file before it has
/// committed too.
pub(super) struct WatermarkProgress {
    committed: FileWatermark,
    /// Every discovered file the watermark doesn't cover yet (queued, being read,
    /// or emitted), and whether it has committed.
    outstanding: BTreeMap<FileKey, bool>,
    /// Whether this run has listed the path. Until then a file the last run left
    /// in flight isn't in `outstanding`, and folding the files committed past it
    /// would skip it for good.
    listed: bool,
}

impl WatermarkProgress {
    pub(super) fn new(mut persisted: FileWatermark) -> Self {
        let outstanding = std::mem::take(&mut persisted.committed_ahead)
            .into_iter()
            .map(|file| (file, true))
            .collect();
        Self {
            committed: persisted,
            outstanding,
            listed: false,
        }
    }

    /// Records a listing's files, returning the ones not discovered before, in
    /// read order.
    fn discover(&mut self, files: Vec<PartitionedFile>) -> Vec<PartitionedFile> {
        self.listed = true;
        let mut discovered: Vec<(FileKey, PartitionedFile)> = files
            .into_iter()
            .map(|file| (FileKey::from(&file), file))
            .filter(|(key, _)| {
                !self.committed.covers(key.last_modified_ms, &key.path)
                    && !self.outstanding.contains_key(key)
            })
            .collect();
        discovered.sort_by(|(a, _), (b, _)| a.cmp(b));
        discovered
            .into_iter()
            .map(|(key, file)| {
                self.outstanding.insert(key, false);
                file
            })
            .collect()
    }
}

impl Progress for WatermarkProgress {
    type Persisted = FileWatermark;

    fn commit(&mut self, files: Vec<FileKey>) -> FileWatermark {
        for file in files {
            // A file already folded into the watermark is no longer outstanding.
            if let Some(committed) = self.outstanding.get_mut(&file) {
                *committed = true;
            }
        }
        if self.listed {
            while let Some(entry) = self.outstanding.first_entry()
                && *entry.get()
            {
                let (file, _) = entry.remove_entry();
                advance_watermark(&mut self.committed, file.last_modified_ms, vec![file.path]);
            }
        }
        FileWatermark {
            committed_ahead: self
                .outstanding
                .iter()
                .filter(|(_, committed)| **committed)
                .map(|(file, _)| file.clone())
                .collect(),
            ..self.committed.clone()
        }
    }
}

/// The newly discovered files a continuous scan's partitions share, and when the
/// path is next listed.
pub(super) struct ContinuousWork {
    discovery: FileDiscovery,
    object_store: Arc<dyn ObjectStore>,
    poll_interval: Duration,
    /// Held across a listing, so partitions that find the queue empty meanwhile
    /// wait for its files instead of listing again.
    queue: tokio::sync::Mutex<WorkQueue>,
}

struct WorkQueue {
    files: VecDeque<PartitionedFile>,
    next_listing_at: Instant,
}

impl ContinuousWork {
    pub(super) fn new(
        discovery: FileDiscovery,
        object_store: Arc<dyn ObjectStore>,
        poll_interval: Duration,
    ) -> Self {
        Self {
            discovery,
            object_store,
            poll_interval,
            queue: tokio::sync::Mutex::new(WorkQueue {
                files: VecDeque::new(),
                next_listing_at: Instant::now(),
            }),
        }
    }

    /// Lists the path for files not discovered before, in read order.
    async fn list(
        &self,
        tracker: &ProgressTracker<WatermarkProgress>,
    ) -> DataFusionResult<Vec<PartitionedFile>> {
        // Only a prefilter: a finalize can advance the watermark during the
        // listing, so `discover` checks again under the tracker's lock. The
        // watermark never moves back, so nothing this snapshot covers is new.
        let watermark = tracker.with_progress(|progress| progress.committed.clone());
        let files = self
            .discovery
            .list(self.object_store.as_ref(), |meta| {
                self.discovery.has_extension(meta)
                    && !watermark.covers(
                        meta.last_modified.timestamp_millis(),
                        meta.location.as_ref(),
                    )
            })
            .await?;
        Ok(tracker.with_progress(|progress| progress.discover(files)))
    }
}

/// One continuous partition's reader of the scan's shared work: each round takes
/// up to `round_size` files from the queue, listing the path when the queue is
/// empty and a listing is due.
pub(super) struct ContinuousPlan {
    pub(super) partition: usize,
    pub(super) round_size: usize,
    pub(super) work: Arc<ContinuousWork>,
    pub(super) tracker: Arc<ProgressTracker<WatermarkProgress>>,
}

#[async_trait]
impl ReadPlan for ContinuousPlan {
    async fn next_round(&mut self) -> DataFusionResult<RoundOutcome> {
        let mut queue = self.work.queue.lock().await;
        if queue.files.is_empty() {
            if Instant::now() < queue.next_listing_at {
                return Ok(RoundOutcome::Idle {
                    until: queue.next_listing_at,
                });
            }
            let discovered = self.work.list(&self.tracker).await?;
            if discovered.is_empty() {
                queue.next_listing_at = Instant::now() + self.work.poll_interval;
                return Ok(RoundOutcome::Idle {
                    until: queue.next_listing_at,
                });
            }
            // A listing that found files is followed by another as soon as the
            // queue drains; only an empty one waits out the poll interval.
            queue.next_listing_at = Instant::now();
            queue.files.extend(discovered);
        }
        let round_size = self.round_size.min(queue.files.len());
        Ok(RoundOutcome::Files(
            queue.files.drain(..round_size).collect(),
        ))
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

/// Runs one continuous partition until its stream ends, then marks it ended so
/// the files it emitted count toward later epochs instead of holding the
/// watermark back for the rest of the run.
pub(super) async fn read_continuous_partition(
    mut plan: ContinuousPlan,
    mut reader: PartitionReader,
    mut inbox: CheckpointInbox,
    tx: Sender<DataFusionResult<RecordBatch>>,
) -> DataFusionResult<()> {
    let end = read_rounds(&mut plan, &mut reader, &mut inbox, &tx).await;
    plan.tracker.finish(plan.partition);
    end.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_providers::file::checkpoint::handle_checkpoint_message;
    use crate::table_providers::file::test_support::*;
    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use futures::{Stream, StreamExt};
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use streamling_core::checkpoints::checkpoint_management::CheckpointMessage;
    use streamling_state::in_memory::InMemoryStateOperatorBackendFactory;
    use streamling_state::{StateOperatorBackend, StateOperatorBackendFactory};
    use tokio::time::timeout;

    const POLL_INTERVAL: Duration = Duration::from_millis(300);

    fn memory_discovery() -> FileDiscovery {
        FileDiscovery {
            table_url: ListingTableUrl::parse("memory:///data/").unwrap(),
            file_extension: "csv".to_string(),
            partition_cols: Vec::new(),
        }
    }

    fn watermark_backend(namespace: &str) -> Arc<dyn StateOperatorBackend<FileWatermark>> {
        InMemoryStateOperatorBackendFactory::new()
            .unwrap()
            .create::<FileWatermark>(namespace)
    }

    fn initial_watermark() -> FileWatermark {
        FileWatermark {
            last_modified_ms: i64::MIN,
            ..Default::default()
        }
    }

    fn key(path: &str, last_modified_ms: i64) -> FileKey {
        FileKey {
            last_modified_ms,
            path: path.to_string(),
        }
    }

    fn file(path: &str, last_modified_ms: i64) -> PartitionedFile {
        let mut file = PartitionedFile::new(path, 1);
        file.object_meta.last_modified += Duration::from_millis(last_modified_ms as u64);
        file
    }

    fn paths(files: &[PartitionedFile]) -> Vec<String> {
        files
            .iter()
            .map(|file| file.object_meta.location.to_string())
            .collect()
    }

    async fn put_csv(store: &InMemory, name: &str) {
        store.put(&Path::from(name), "id\n1".into()).await.unwrap();
    }

    /// `partitions` plans sharing one scan's work and progress over `store`.
    fn shared_plans(
        store: Arc<InMemory>,
        partitions: usize,
        round_size: usize,
        state_backend: Arc<dyn StateOperatorBackend<FileWatermark>>,
    ) -> Vec<ContinuousPlan> {
        let work = Arc::new(ContinuousWork::new(
            memory_discovery(),
            store,
            POLL_INTERVAL,
        ));
        let tracker = Arc::new(ProgressTracker::new(
            state_backend,
            watermark_state_key("src"),
            WatermarkProgress::new(initial_watermark()),
            partitions,
        ));
        (0..partitions)
            .map(|partition| ContinuousPlan {
                partition,
                round_size,
                work: work.clone(),
                tracker: tracker.clone(),
            })
            .collect()
    }

    #[test]
    fn file_watermark_serde_round_trip() {
        let watermark = FileWatermark {
            last_modified_ms: 1_700_000_000_123,
            boundary_paths: vec!["a/f1.csv".to_string(), "a/f2.csv".to_string()],
            committed_ahead: vec![key("a/f3.csv", 1_700_000_000_456)],
        };
        let json = serde_json::to_string(&watermark).unwrap();
        let restored: FileWatermark = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, watermark);

        let persisted_before_committed_ahead: FileWatermark =
            serde_json::from_str(r#"{"last_modified_ms":5,"boundary_paths":["a/f1.csv"]}"#)
                .unwrap();
        assert!(persisted_before_committed_ahead.committed_ahead.is_empty());
    }

    #[test]
    fn watermark_covers_older_files_and_ingested_boundary_files() {
        let watermark = FileWatermark {
            last_modified_ms: 5,
            boundary_paths: vec!["done.csv".to_string()],
            committed_ahead: Vec::new(),
        };
        // Strictly newer than the watermark.
        assert!(!watermark.covers(10, "new.csv"));
        // Below the watermark — skipped.
        assert!(watermark.covers(4, "old.csv"));
        // At the watermark, not yet ingested — picked up (the boundary catch).
        assert!(!watermark.covers(5, "new.csv"));
        // At the watermark, already ingested — skipped.
        assert!(watermark.covers(5, "done.csv"));
    }

    #[test]
    fn advance_watermark_unions_at_boundary_and_resets_on_advance() {
        let mut watermark = FileWatermark {
            last_modified_ms: 100,
            boundary_paths: vec!["a.csv".to_string()],
            ..Default::default()
        };

        // Same second: the boundary set accumulates (so a third same-second file
        // could still be ingested next poll), watermark unchanged.
        advance_watermark(&mut watermark, 100, vec!["b.csv".to_string()]);
        assert_eq!(watermark.last_modified_ms, 100);
        let mut got = watermark.boundary_paths.clone();
        got.sort();
        assert_eq!(got, vec!["a.csv".to_string(), "b.csv".to_string()]);

        // Newer second: watermark advances and the boundary set resets.
        advance_watermark(&mut watermark, 200, vec!["c.csv".to_string()]);
        assert_eq!(watermark.last_modified_ms, 200);
        assert_eq!(watermark.boundary_paths, vec!["c.csv".to_string()]);
    }

    /// The watermark advances only over files committed in `(last_modified,
    /// path)` order; a file committed past one still being read is carried in
    /// `committed_ahead` instead.
    #[test]
    fn watermark_progress_folds_committed_files_in_read_order() {
        let mut progress = WatermarkProgress::new(initial_watermark());
        let discovered = progress.discover(vec![file("c", 30), file("a", 10), file("b", 20)]);
        assert_eq!(
            paths(&discovered),
            ["a", "b", "c"],
            "files are read in order"
        );
        assert!(
            progress
                .discover(vec![file("a", 10), file("c", 30)])
                .is_empty(),
            "a listing never queues a discovered file again"
        );

        let persisted = progress.commit(vec![key("c", 30)]);
        assert_eq!(
            persisted.last_modified_ms,
            i64::MIN,
            "`a` and `b` are still being read"
        );
        assert_eq!(persisted.committed_ahead, vec![key("c", 30)]);

        let persisted = progress.commit(vec![key("a", 10)]);
        assert_eq!(persisted.last_modified_ms, 10);
        assert_eq!(persisted.boundary_paths, vec!["a".to_string()]);
        assert_eq!(persisted.committed_ahead, vec![key("c", 30)]);

        assert_eq!(
            progress.commit(vec![key("b", 20), key("a", 10)]),
            FileWatermark {
                last_modified_ms: 30,
                boundary_paths: vec!["c".to_string()],
                committed_ahead: Vec::new(),
            },
            "closing the gap folds the file carried past it; a folded file is ignored"
        );
        assert!(
            progress
                .discover(vec![file("a", 10), file("b", 20), file("c", 30)])
                .is_empty(),
            "folded files stay covered"
        );
        assert_eq!(
            paths(&progress.discover(vec![file("d", 30)])),
            ["d"],
            "a new file at the watermark's timestamp is still read"
        );
    }

    /// A restart skips the files carried in `committed_ahead`, but folds past
    /// them only once the run's first listing has queued any file the last run
    /// left in flight.
    #[test]
    fn watermark_progress_resumes_past_carried_files_after_the_first_listing() {
        let persisted = FileWatermark {
            last_modified_ms: 10,
            boundary_paths: vec!["a".to_string()],
            committed_ahead: vec![key("c", 30)],
        };

        let mut progress = WatermarkProgress::new(persisted.clone());
        assert_eq!(
            progress.commit(Vec::new()),
            persisted,
            "`b` isn't known before the first listing, so nothing folds"
        );
        let discovered = progress.discover(vec![file("a", 10), file("b", 20), file("c", 30)]);
        assert_eq!(
            paths(&discovered),
            ["b"],
            "only the file left in flight is read again"
        );
        assert_eq!(progress.commit(Vec::new()), persisted);
        assert_eq!(
            progress.commit(vec![key("b", 20)]),
            FileWatermark {
                last_modified_ms: 30,
                boundary_paths: vec!["c".to_string()],
                committed_ahead: Vec::new(),
            }
        );

        // `b` was deleted before the restart: nothing holds the carried file back.
        let mut progress = WatermarkProgress::new(persisted);
        assert!(progress.discover(vec![file("c", 30)]).is_empty());
        assert_eq!(progress.commit(Vec::new()).last_modified_ms, 30);
    }

    /// The watermark is persisted only on a checkpoint finalize, and covers only
    /// the files emitted before that epoch's marker — mirroring the Kafka source's
    /// commit-on-finalize.
    #[tokio::test]
    async fn persists_watermark_only_on_finalize() {
        let store = Arc::new(InMemory::new());
        put_csv(&store, "data/a.csv").await;
        put_csv(&store, "data/b.csv").await;
        let state_backend = watermark_backend("finalize");
        let state_key = watermark_state_key("src");
        let persisted = || async { state_backend.get(state_key.clone()).await.unwrap() };
        let mut plan = shared_plans(store, 1, 1, state_backend.clone()).remove(0);

        let RoundOutcome::Files(first) = plan.next_round().await.unwrap() else {
            panic!("expected a round of files");
        };
        plan.advance(&first);
        let marker = CheckpointMessage::Marker {
            epoch: CheckpointEpoch(1),
            created_at_ms: 0,
        };
        assert!(!handle_checkpoint_message(&marker, &mut plan).await.unwrap());
        assert!(
            persisted().await.is_none(),
            "a marker must not persist the watermark"
        );

        let RoundOutcome::Files(second) = plan.next_round().await.unwrap() else {
            panic!("expected a round of files");
        };
        plan.advance(&second);
        handle_checkpoint_message(&CheckpointMessage::Finalizer(CheckpointEpoch(1)), &mut plan)
            .await
            .unwrap();
        let first = FileKey::from(&first[0]);
        let expected = FileWatermark {
            last_modified_ms: first.last_modified_ms,
            boundary_paths: vec![first.path],
            committed_ahead: Vec::new(),
        };
        assert_eq!(
            persisted().await,
            Some(expected.clone()),
            "finalize must persist only the files emitted before the marker"
        );

        // A finalize for an epoch no marker was recorded for commits nothing new.
        handle_checkpoint_message(&CheckpointMessage::Finalizer(CheckpointEpoch(7)), &mut plan)
            .await
            .unwrap();
        assert_eq!(persisted().await, Some(expected));

        // SourceComplete signals shutdown.
        let source_complete = CheckpointMessage::SourceComplete("src".to_string());
        assert!(
            handle_checkpoint_message(&source_complete, &mut plan)
                .await
                .unwrap()
        );
    }

    /// Partitions share one listing per poll interval: one listing's files feed
    /// every partition, and a partition that finds the queue empty before the
    /// next listing is due waits for it instead of listing again.
    #[tokio::test]
    async fn continuous_partitions_share_one_listing_per_poll_interval() {
        let store = Arc::new(InMemory::new());
        for name in ["data/a.csv", "data/b.csv", "data/c.csv"] {
            put_csv(&store, name).await;
        }
        let mut plans = shared_plans(store.clone(), 2, 2, watermark_backend("listing"));

        let RoundOutcome::Files(round) = plans[0].next_round().await.unwrap() else {
            panic!("partition 0 should take a round of the listing");
        };
        assert_eq!(round.len(), 2);
        let RoundOutcome::Files(round) = plans[1].next_round().await.unwrap() else {
            panic!("partition 1 should take the rest of the listing");
        };
        assert_eq!(paths(&round), ["data/c.csv"]);

        // The queue drained after a listing that found files, so partition 0
        // lists again at once; the files still being read aren't new.
        let RoundOutcome::Idle { until } = plans[0].next_round().await.unwrap() else {
            panic!("nothing new should be found");
        };

        put_csv(&store, "data/d.csv").await;
        let RoundOutcome::Idle {
            until: partition_1_until,
        } = plans[1].next_round().await.unwrap()
        else {
            panic!("partition 1 must wait for the next listing, not list itself");
        };
        assert_eq!(partition_1_until, until);

        tokio::time::sleep_until(until).await;
        let RoundOutcome::Files(round) = plans[1].next_round().await.unwrap() else {
            panic!("the next listing should find the new file");
        };
        assert_eq!(paths(&round), ["data/d.csv"]);
    }

    /// Pulls `items` batches from a continuous stream.
    async fn pull_continuous(
        stream: &mut (impl Stream<Item = DataFusionResult<RecordBatch>> + Unpin),
        items: usize,
    ) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        for _ in 0..items {
            let batch = timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("stream should yield within timeout")
                .expect("stream should not end")
                .unwrap();
            batches.push(batch);
        }
        batches
    }

    /// The continuous source must emit an empty `RecordBatch` on every poll — even
    /// when no new files were discovered — so downstream operators keep advancing
    /// while the source is idle.
    #[tokio::test]
    async fn continuous_source_emits_empty_heartbeat_when_idle() {
        let dir = temp_dir_with("heartbeat", &csv_files(1));
        let (session_manager, provider) =
            continuous_provider(&format!("{}/", dir.to_str().unwrap()), "heartbeat_src", 1).await;
        let plan = provider
            .scan(&session_manager.session_state(), None, &[], None)
            .await
            .unwrap();
        let mut stream = plan
            .execute(0, session_manager.session_context().task_ctx())
            .unwrap();

        // The first poll reads the file once (3 rows); each subsequent idle poll
        // emits an empty heartbeat. Collecting several items spans multiple polls.
        let batches = pull_continuous(&mut stream, 5).await;
        let _ = std::fs::remove_dir_all(&dir);

        let data_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let empty_batches = batches.iter().filter(|batch| batch.num_rows() == 0).count();
        assert_eq!(data_rows, 3, "the file's 3 rows are read exactly once");
        assert!(
            empty_batches >= 2,
            "idle polls must emit empty heartbeat batches; got {empty_batches}"
        );
    }

    /// A poll's files are split across the session's target partitions and read
    /// concurrently, but the source still emits from one stream: every row must
    /// arrive exactly once (the watermark's boundary set covers the files sharing
    /// the poll's newest timestamp), and idle polls must still heartbeat.
    #[tokio::test]
    async fn continuous_source_reads_split_file_groups_exactly_once() {
        // Non-numeric ids keep the inferred column Utf8, so the assertion below can
        // name the exact rows rather than just count them.
        let files: Vec<(String, String)> = ["a", "b", "c"]
            .iter()
            .map(|file| {
                (
                    format!("{file}.csv"),
                    format!("id,name\n{file}0,alice\n{file}1,bob\n{file}2,carol"),
                )
            })
            .collect();
        let dir = temp_dir_with("split", &files);
        let (session_manager, provider) =
            continuous_provider(&format!("{}/", dir.to_str().unwrap()), "split_src", 1).await;

        let plan = provider
            .scan(&session_manager.session_state(), None, &[], None)
            .await
            .unwrap();
        assert_eq!(
            plan.output_partitioning().partition_count(),
            1,
            "concurrent file groups must still surface as one output partition"
        );

        let mut stream = plan
            .execute(0, session_manager.session_context().task_ctx())
            .unwrap();
        let batches = pull_continuous(&mut stream, 8).await;
        let _ = std::fs::remove_dir_all(&dir);

        let mut ids: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone();
                (0..column.len()).map(move |row| column.value(row).to_string())
            })
            .collect();
        ids.sort();
        assert_eq!(
            ids,
            vec!["a0", "a1", "a2", "b0", "b1", "b2", "c0", "c1", "c2"],
            "every row from every file group must arrive exactly once"
        );
        let empty_batches = batches.iter().filter(|batch| batch.num_rows() == 0).count();
        assert!(
            empty_batches >= 2,
            "polls after ingest must emit empty heartbeat batches; got {empty_batches}"
        );
    }

    /// A continuous source with `parallelism` output partitions shares newly
    /// discovered files between them: every row arrives exactly once across the
    /// partitions, and idle partitions heartbeat.
    #[tokio::test]
    async fn continuous_partitions_read_every_file_exactly_once() {
        let dir = temp_dir_with("continuous_partitions", &csv_files(4));
        let (session_manager, provider) =
            continuous_provider(&format!("{}/", dir.to_str().unwrap()), "partitions_src", 2).await;
        let plan = provider
            .scan(&session_manager.session_state(), None, &[], None)
            .await
            .unwrap();
        assert_eq!(plan.output_partitioning().partition_count(), 2);

        let streams = (0..2).map(|partition| {
            plan.execute(partition, session_manager.session_context().task_ctx())
                .unwrap()
        });
        let batches = pull_continuous(&mut futures::stream::select_all(streams), 12).await;
        let _ = std::fs::remove_dir_all(&dir);

        let mut ids = int64_values(&batches, "id");
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![0, 1, 2, 10, 11, 12, 20, 21, 22, 30, 31, 32],
            "every row must arrive exactly once across the partitions"
        );
        let empty_batches = batches.iter().filter(|batch| batch.num_rows() == 0).count();
        assert!(
            empty_batches >= 2,
            "idle partitions must emit empty heartbeat batches; got {empty_batches}"
        );
    }

    /// A path naming one exact object (no trailing slash) must be ingested by
    /// the continuous source. A prefix listing never returns a key equal to the
    /// prefix (object_store prefixes match whole path segments), so this
    /// exercises the HEAD branch — without it the source heartbeats forever with
    /// zero rows.
    #[tokio::test]
    async fn continuous_source_ingests_single_file_path() {
        let dir = temp_dir_with("single_file", &csv_files(1));
        let file = dir.join("0.csv");
        let (session_manager, provider) =
            continuous_provider(file.to_str().unwrap(), "single_file_src", 1).await;
        let plan = provider
            .scan(&session_manager.session_state(), None, &[], None)
            .await
            .unwrap();
        let mut stream = plan
            .execute(0, session_manager.session_context().task_ctx())
            .unwrap();

        // First poll HEADs and reads the file (3 rows); the watermark then
        // filters the unchanged object, so later polls are idle heartbeats.
        let batches = pull_continuous(&mut stream, 5).await;
        let _ = std::fs::remove_dir_all(&dir);

        let data_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let empty_batches = batches.iter().filter(|batch| batch.num_rows() == 0).count();
        assert_eq!(data_rows, 3, "the object's 3 rows are read exactly once");
        assert!(
            empty_batches >= 2,
            "polls after ingest must emit empty heartbeat batches; got {empty_batches}"
        );
    }
}
