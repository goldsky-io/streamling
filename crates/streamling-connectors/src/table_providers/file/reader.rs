//! The per-partition read loop both modes share.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, FileSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, info};

use streamling_core::checkpoints::checkpoint_management::CheckpointEpoch;

use super::checkpoint::CheckpointInbox;
use super::{ScanProjection, build_output_batch};

/// What a read plan hands the read loop next.
pub(super) enum RoundOutcome {
    /// Read these files as one round, then call [`ReadPlan::advance`].
    Files(Vec<PartitionedFile>),
    /// No new files (continuous only): heartbeat, then poll again at `until`,
    /// when the partitions' next listing is due.
    Idle { until: Instant },
    /// Every file has been read (bounded only).
    Exhausted,
}

/// The mode-specific half of a partition's read loop: which files to read next,
/// and the progress checkpoints snapshot and persist.
#[async_trait]
pub(super) trait ReadPlan: Send {
    async fn next_round(&mut self) -> DataFusionResult<RoundOutcome>;
    /// Records a round as fully emitted.
    fn advance(&mut self, round: &[PartitionedFile]);
    /// Captures the current progress for `epoch` on its `Marker`.
    fn snapshot(&mut self, epoch: &CheckpointEpoch);
    /// Durably persists the progress captured for `epoch`, on its `Finalizer`.
    async fn persist(&mut self, epoch: &CheckpointEpoch) -> DataFusionResult<()>;
}

/// Why a partition's read loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadEnd {
    /// The plan ran out of files, or the scan limit was reached.
    Exhausted,
    /// Shutdown, a record limit, or `SourceComplete` stopped it at a batch
    /// boundary.
    Stopped,
    /// Downstream dropped the stream, so nothing more can reach a sink.
    Disconnected,
}

/// What one partition's read loop needs besides its plan.
pub(super) struct PartitionReader {
    pub(super) reference_name: String,
    pub(super) object_store_url: ObjectStoreUrl,
    pub(super) file_source: Arc<dyn FileSource>,
    pub(super) projection: ScanProjection,
    pub(super) output_schema: SchemaRef,
    pub(super) limit: Option<usize>,
    pub(super) num_records_before_stop: Option<u64>,
    pub(super) records_emitted: Arc<AtomicU64>,
    pub(super) shutdown_rx: watch::Receiver<bool>,
    pub(super) context: Arc<TaskContext>,
}

impl PartitionReader {
    /// Process shutdown (SIGTERM, or a record-limit sink), a provider shutdown,
    /// or this source's own record limit.
    fn stop_requested(&self, global_shutdown_rx: &watch::Receiver<bool>) -> bool {
        *global_shutdown_rx.borrow()
            || *self.shutdown_rx.borrow()
            || self
                .num_records_before_stop
                .is_some_and(|limit| self.records_emitted.load(Ordering::SeqCst) >= limit)
    }

    fn open_round(
        &self,
        files: Vec<PartitionedFile>,
        limit: Option<usize>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Spread the round's files over the session's target partitions so they
        // are read concurrently, then merge them back into the single stream this
        // loop emits from: progress, the checkpoint drain, and the record limit
        // all assume one emission point.
        let file_groups =
            FileGroup::new(files).split_files(self.context.session_config().target_partitions());
        let config =
            FileScanConfigBuilder::new(self.object_store_url.clone(), self.file_source.clone())
                .with_file_groups(file_groups)
                .with_projection_indices(self.projection.read.clone())?
                .with_limit(limit)
                .build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        if scan.output_partitioning().partition_count() > 1 {
            CoalescePartitionsExec::new(scan).execute(0, self.context.clone())
        } else {
            scan.execute(0, self.context.clone())
        }
    }
}

/// Drives one partition through its plan's rounds. Checkpoint messages are
/// drained before every batch, so a marker rides the next batch (within one
/// batch interval) instead of waiting for a whole round, which can take a while.
pub(super) async fn read_rounds<P: ReadPlan>(
    plan: &mut P,
    reader: &mut PartitionReader,
    inbox: &mut CheckpointInbox,
    tx: &Sender<DataFusionResult<RecordBatch>>,
) -> DataFusionResult<ReadEnd> {
    // Process-wide shutdown signal (the single top-level SIGTERM/SIGINT handler
    // flips it; record-limit sinks flip it too). Held across iterations so a flip
    // during the idle sleep is not missed.
    let mut global_shutdown_rx = streamling_core::shutdown::subscribe();
    let mut partition_emitted: usize = 0;

    loop {
        if inbox.drain(plan).await? || reader.stop_requested(&global_shutdown_rx) {
            return Ok(ReadEnd::Stopped);
        }

        let round = match plan.next_round().await? {
            RoundOutcome::Files(files) => files,
            RoundOutcome::Exhausted => return Ok(ReadEnd::Exhausted),
            RoundOutcome::Idle { until } => {
                debug!("No new files to process");
                // Heartbeat: on an idle poll, emit an empty batch — carrying any
                // drained checkpoint messages — so downstream operators keep
                // advancing.
                let heartbeat = inbox.attach(RecordBatch::new_empty(reader.output_schema.clone()));
                if tx.send(Ok(heartbeat)).await.is_err() {
                    return Ok(ReadEnd::Disconnected);
                }
                tokio::select! {
                    changed = reader.shutdown_rx.changed() => {
                        if changed.is_err() || *reader.shutdown_rx.borrow() {
                            return Ok(ReadEnd::Stopped);
                        }
                    }
                    _ = sleep_until(until) => {}
                    // Process shutdown (SIGTERM/SIGINT via the top-level handler,
                    // or a record-limit sink): end the stream so downstream sinks
                    // complete.
                    _ = global_shutdown_rx.wait_for(|stopping| *stopping) => {
                        info!("[{}] file source observed process shutdown; ending stream", reader.reference_name);
                        return Ok(ReadEnd::Stopped);
                    }
                }
                continue;
            }
        };

        let round_limit = reader
            .limit
            .map(|limit| limit.saturating_sub(partition_emitted));
        let mut stream = reader.open_round(round.clone(), round_limit)?;
        while let Some(batch) = stream.next().await {
            let source_complete = inbox.drain(plan).await?;
            let batch = inbox.attach(build_output_batch(
                batch?,
                &reader.projection.columns,
                &reader.output_schema,
            )?);
            let rows = batch.num_rows();
            if tx.send(Ok(batch)).await.is_err() {
                return Ok(ReadEnd::Disconnected);
            }
            partition_emitted += rows;
            reader
                .records_emitted
                .fetch_add(rows as u64, Ordering::SeqCst);
            if source_complete || reader.stop_requested(&global_shutdown_rx) {
                return Ok(ReadEnd::Stopped);
            }
        }

        // A round the scan limit cut short was not fully emitted, so it must not
        // advance progress. The limit is advisory: DataFusion's own stays above.
        if reader.limit.is_some_and(|limit| partition_emitted >= limit) {
            return Ok(ReadEnd::Exhausted);
        }
        plan.advance(&round);
    }
}
