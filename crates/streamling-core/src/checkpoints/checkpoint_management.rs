use crate::checkpoints::channels::{send, subscribe};
use crate::telemetry::recorder::{
    ControlPlaneMetricsRecorder, MetricsRecorder, get_control_plane_metrics_recorder,
};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use crossbeam::channel::RecvTimeoutError;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

pub const CHECKPOINT_COORDINATOR_CHANNEL: &str = "_checkpoint_coordinator";

/// The metadata key used to store checkpoint messages in batch schema metadata.
pub const CHECKPOINT_MESSAGES_KEY: &str = "checkpoint_messages";

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct CheckpointEpoch(pub u64);

/// The message types for the checkpoint coordinator
/// - Marker: A new checkpoint epoch marker. This is propagated through topology nodes.
///   Contains epoch and creation timestamp (ms since UNIX epoch) for propagation timing.
/// - Ack: An acknowledgment for a checkpoint epoch. Sinks MUST flush/commit their state before sending this.
/// - Finalizer: A message to finalize a checkpoint epoch. Should be used by sources/operators to flush/commit their state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CheckpointMessage {
    Marker {
        epoch: CheckpointEpoch,
        created_at_ms: u64,
    },
    Ack {
        epoch: CheckpointEpoch,
        sink_id: String,
    },
    Finalizer(CheckpointEpoch),
    SourceComplete(String), // Source name that has completed
}

/// Helper to get current time in milliseconds since UNIX epoch
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Send an ack for `epoch` on behalf of `sink_id`.
///
/// The coordinator subscriber can exit before a sink's last stream does, so a
/// failed send is logged rather than fatal — the pipeline is shutting down and
/// there is nothing left to finalize.
pub fn send_checkpoint_ack(epoch: CheckpointEpoch, sink_id: &str) {
    if let Err(e) = send(
        CHECKPOINT_COORDINATOR_CHANNEL,
        CheckpointMessage::Ack {
            epoch: epoch.clone(),
            sink_id: sink_id.to_string(),
        },
    ) {
        error!(
            "failed to send checkpoint ack for epoch {} from sink '{}': {}",
            epoch.0, sink_id, e
        );
    }
}

/// Process checkpoint messages from a batch: record arrival latency, send ack,
/// and record sink flush time. This is the standard pattern used by all sinks.
///
/// `arrival_time_ms` should be captured via `now_ms()` at the start of batch processing,
/// before any flush work begins, to accurately measure marker propagation time.
///
/// When the sink writes several partition streams concurrently the ack is gated
/// by [`report_marker_at_sink`], so the coordinator only sees the epoch once
/// every stream has flushed it.
pub fn process_checkpoint_acks(
    messages: Vec<CheckpointMessage>,
    arrival_time_ms: u64,
    ack_start: Instant,
    metrics_recorder: &MetricsRecorder,
    metric_metadata_id: &str,
    sink_id: &str,
) {
    for message in messages {
        if let CheckpointMessage::Marker {
            epoch,
            created_at_ms,
        } = message
        {
            let arrival_latency_ms = arrival_time_ms.saturating_sub(created_at_ms);
            metrics_recorder.record_time(
                "checkpoint_marker_arrival",
                Duration::from_millis(arrival_latency_ms),
                metric_metadata_id,
            );
            if report_marker_at_sink(sink_id, epoch.clone()) {
                send_checkpoint_ack(epoch, sink_id);
            }
            metrics_recorder.record_time(
                "checkpoint_sink_flush",
                ack_start.elapsed(),
                metric_metadata_id,
            );
        }
    }
}

/// Per-epoch bookkeeping for [`MarkerAligner`].
#[derive(Debug, Default)]
struct MarkerState {
    copies: usize,
    released: bool,
}

/// Aligns checkpoint markers arriving on several concurrent streams down to a
/// single copy.
///
/// Streamling's correctness invariant is that **every stream carries at most one
/// marker copy per epoch**: a sink acks an epoch as soon as it sees a marker, and
/// the coordinator finalizes on the first ack per sink name. Any place where N
/// streams merge into one — `StreamingCoalesceExec`, a `StreamingRepartitionExec`
/// output, a sink writing N partitions — must therefore hold a marker back until
/// every live input has delivered it, or the source commits offsets for data that
/// is still in flight on the slower streams.
///
/// Alignment only ever *delays* a marker; data is never blocked, so at-least-once
/// delivery is preserved.
#[derive(Debug)]
pub struct MarkerAligner {
    live_inputs: usize,
    /// Released epochs stay recorded so a late copy from a slower input is
    /// absorbed rather than releasing the same epoch a second time. Entries are
    /// pruned when the epoch is finalized.
    epochs: BTreeMap<CheckpointEpoch, MarkerState>,
    seen_finalizers: HashSet<u64>,
    seen_source_completions: HashSet<String>,
}

impl MarkerAligner {
    pub fn new(inputs: usize) -> Self {
        Self {
            live_inputs: inputs,
            epochs: BTreeMap::new(),
            seen_finalizers: HashSet::new(),
            seen_source_completions: HashSet::new(),
        }
    }

    /// Feed the messages observed on one input stream, returning the messages
    /// that may now be forwarded downstream.
    pub fn observe(&mut self, messages: Vec<CheckpointMessage>) -> Vec<CheckpointMessage> {
        let mut released = Vec::new();
        for message in messages {
            match message {
                CheckpointMessage::Marker {
                    ref epoch,
                    created_at_ms,
                } => {
                    let live = self.live_inputs;
                    let state = self.epochs.entry(epoch.clone()).or_default();
                    if state.released {
                        continue;
                    }
                    state.copies += 1;
                    if state.copies >= live.max(1) {
                        state.released = true;
                        released.push(CheckpointMessage::Marker {
                            epoch: epoch.clone(),
                            created_at_ms,
                        });
                    }
                }
                // A finalizer/completion is a broadcast notification rather than a
                // per-stream barrier: forwarding the first copy is both necessary
                // and sufficient.
                CheckpointMessage::Finalizer(ref epoch) => {
                    self.epochs.retain(|e, _| e.0 > epoch.0);
                    if self.seen_finalizers.insert(epoch.0) {
                        released.push(message);
                    }
                }
                CheckpointMessage::SourceComplete(ref source) => {
                    if self.seen_source_completions.insert(source.clone()) {
                        released.push(message);
                    }
                }
                CheckpointMessage::Ack { .. } => released.push(message),
            }
        }
        released
    }

    /// Record that one input stream has ended. Its share of every pending epoch
    /// counts as delivered, so epochs it would never contribute to are released.
    pub fn input_done(&mut self) -> Vec<CheckpointMessage> {
        self.live_inputs = self.live_inputs.saturating_sub(1);
        let live = self.live_inputs.max(1);
        let mut released = Vec::new();
        for (epoch, state) in self.epochs.iter_mut() {
            if !state.released && (state.copies >= live || self.live_inputs == 0) {
                state.released = true;
                released.push(CheckpointMessage::Marker {
                    epoch: epoch.clone(),
                    created_at_ms: now_ms(),
                });
            }
        }
        released
    }
}

/// Tracks, per sink, how many concurrent write streams must flush an epoch
/// before the sink acks it. See [`MarkerAligner`] for why this gate exists.
static SINK_ACK_GATES: once_cell::sync::Lazy<Mutex<HashMap<String, MarkerAligner>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

/// Declare that `sink_id` is written by `streams` concurrent partition streams.
///
/// Sinks that never register are treated as single-stream, which is exactly the
/// ungated behaviour they had before.
pub fn register_sink_streams(sink_id: &str, streams: usize) {
    if streams <= 1 {
        return;
    }
    SINK_ACK_GATES
        .lock()
        .insert(sink_id.to_string(), MarkerAligner::new(streams));
}

/// Report that one write stream of `sink_id` has flushed `epoch`. Returns true
/// when this was the last stream outstanding and the ack should be sent.
pub fn report_marker_at_sink(sink_id: &str, epoch: CheckpointEpoch) -> bool {
    let mut gates = SINK_ACK_GATES.lock();
    let Some(gate) = gates.get_mut(sink_id) else {
        return true;
    };
    !gate
        .observe(vec![CheckpointMessage::Marker {
            epoch,
            created_at_ms: now_ms(),
        }])
        .is_empty()
}

/// Report that one write stream of `sink_id` has finished, returning any epochs
/// that become ackable because the stream will never report them itself.
pub fn sink_stream_done(sink_id: &str) -> Vec<CheckpointEpoch> {
    let mut gates = SINK_ACK_GATES.lock();
    let Some(gate) = gates.get_mut(sink_id) else {
        return Vec::new();
    };
    gate.input_done()
        .into_iter()
        .filter_map(|m| match m {
            CheckpointMessage::Marker { epoch, .. } => Some(epoch),
            _ => None,
        })
        .collect()
}

#[derive(Debug)]
enum EpochState {
    Started {
        created_at: Instant,
    },
    InProgress {
        acked_sinks: HashSet<String>,
        created_at: Instant,
    },
    Finalized,
}

/// Default checkpoint timeout in seconds (5 minutes)
pub const DEFAULT_CHECKPOINT_TIMEOUT_SEC: u64 = 300;

/// Component ID used for checkpoint coordinator metrics
const CHECKPOINT_COORDINATOR_COMPONENT_ID: &str = "checkpoint_coordinator";

/// Get the control-plane metrics recorder for the checkpoint coordinator.
fn get_checkpoint_metrics_recorder() -> Arc<ControlPlaneMetricsRecorder> {
    get_control_plane_metrics_recorder(CHECKPOINT_COORDINATOR_COMPONENT_ID)
}

pub struct CheckpointCoordinator {
    epochs: Arc<Mutex<BTreeMap<CheckpointEpoch, EpochState>>>,
    /// Monotonically increasing epoch counter. Never reuses epoch numbers,
    /// even if previous epochs are removed due to timeout.
    next_epoch: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    timeout_sec: u64,
}

/// Record the current number of non-finalized epochs as a gauge metric.
/// Acquires the epochs lock internally, so callers must NOT hold it.
fn record_in_flight_gauge(
    epochs: &Arc<Mutex<BTreeMap<CheckpointEpoch, EpochState>>>,
    metrics_recorder: &ControlPlaneMetricsRecorder,
) {
    let epochs_guard = epochs.lock();
    let in_flight = epochs_guard
        .values()
        .filter(|s| !matches!(s, EpochState::Finalized))
        .count();
    metrics_recorder.record_gauge("checkpoint_epochs_in_flight", in_flight as u64);
}

/// The checkpoint coordinator is responsible for managing checkpoint epochs and acknowledgments
/// from the topology nodes. It will periodically send out checkpoint markers and finalize epochs
/// when all expected acknowledgments are received.
impl CheckpointCoordinator {
    pub fn new() -> Self {
        Self::with_timeout(DEFAULT_CHECKPOINT_TIMEOUT_SEC)
    }

    pub fn with_timeout(timeout_sec: u64) -> Self {
        Self {
            epochs: Arc::new(Mutex::new(BTreeMap::new())),
            next_epoch: Arc::new(AtomicU64::new(1)),
            running: Arc::new(AtomicBool::new(false)),
            handles: Vec::new(),
            timeout_sec,
        }
    }

    pub fn start(&mut self, sender_interval_sec: u64, expected_sinks: Vec<String>) {
        let num_of_expected_acks = expected_sinks.len() as u32;
        info!(
            "Starting checkpoint coordinator, interval: {}s, acks: {}, sinks: {:?}, timeout: {}s",
            sender_interval_sec, num_of_expected_acks, expected_sinks, self.timeout_sec
        );

        self.running.store(true, Ordering::SeqCst);

        let expected_sinks = Arc::new(expected_sinks);

        let receiver = subscribe(CHECKPOINT_COORDINATOR_CHANNEL);
        let epochs = Arc::clone(&self.epochs);
        let running = Arc::clone(&self.running);
        let expected_sinks_sub = Arc::clone(&expected_sinks);

        let subscriber_handle = tokio::spawn(async move {
            let metrics_recorder = get_checkpoint_metrics_recorder();

            while running.load(Ordering::SeqCst) {
                match receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(CheckpointMessage::Ack { epoch, sink_id }) => {
                        debug!(
                            "[CheckpointCoordinator] Received checkpoint ACK for epoch: {} from sink: {}",
                            epoch.0, sink_id
                        );

                        // Warn if this sink_id is not in the expected list
                        if !expected_sinks_sub.contains(&sink_id) {
                            warn!(
                                "Received ack from unexpected sink '{}' for epoch {} (expected: {:?})",
                                sink_id, epoch.0, *expected_sinks_sub
                            );
                            continue;
                        }

                        // Record ack received metric
                        metrics_recorder.record_count("checkpoint_acks_received", 1);

                        let mut finalized_epoch_duration = None;

                        // Only lock for the brief state modification
                        {
                            let mut epochs_guard = epochs.lock();
                            if let Some(state) = epochs_guard.get_mut(&epoch) {
                                match state {
                                    EpochState::Started { created_at } => {
                                        let ack_latency = created_at.elapsed();
                                        metrics_recorder.record_time_w_tags(
                                            "checkpoint_per_sink_ack_latency",
                                            ack_latency,
                                            vec![("sink_id", sink_id.as_str())],
                                        );

                                        let acked_sinks = HashSet::from([sink_id.clone()]);
                                        let ack_count = acked_sinks.len() as u32;
                                        info!(
                                            "Epoch {} ack {}/{} from '{}' (age {:?})",
                                            epoch.0,
                                            ack_count,
                                            num_of_expected_acks,
                                            sink_id,
                                            ack_latency
                                        );

                                        let is_complete = expected_sinks_sub
                                            .iter()
                                            .all(|s| acked_sinks.contains(s));
                                        if is_complete {
                                            finalized_epoch_duration = Some(ack_latency);
                                            *state = EpochState::Finalized;
                                        } else {
                                            *state = EpochState::InProgress {
                                                acked_sinks,
                                                created_at: *created_at,
                                            };
                                        }
                                    }
                                    EpochState::InProgress {
                                        acked_sinks,
                                        created_at,
                                    } => {
                                        let ack_latency = created_at.elapsed();
                                        metrics_recorder.record_time_w_tags(
                                            "checkpoint_per_sink_ack_latency",
                                            ack_latency,
                                            vec![("sink_id", sink_id.as_str())],
                                        );

                                        acked_sinks.insert(sink_id.clone());
                                        let ack_count = acked_sinks.len() as u32;
                                        info!(
                                            "Epoch {} ack {}/{} from '{}' (age {:?})",
                                            epoch.0,
                                            ack_count,
                                            num_of_expected_acks,
                                            sink_id,
                                            ack_latency
                                        );

                                        let is_complete = expected_sinks_sub
                                            .iter()
                                            .all(|s| acked_sinks.contains(s));
                                        if is_complete {
                                            finalized_epoch_duration = Some(created_at.elapsed());
                                            *state = EpochState::Finalized;
                                        }
                                    }
                                    EpochState::Finalized => {
                                        debug!(
                                            "Received ack from '{}' for finalized epoch: {}",
                                            sink_id, epoch.0
                                        );
                                    }
                                }
                            } else {
                                warn!(
                                    "Received ack from '{}' for unknown epoch: {} (not in epochs map)",
                                    sink_id, epoch.0
                                );
                            }
                        }

                        if let Some(epoch_duration) = finalized_epoch_duration {
                            metrics_recorder
                                .record_time("checkpoint_epoch_duration", epoch_duration);

                            metrics_recorder.record_count("checkpoint_epochs_succeeded", 1);
                            metrics_recorder.record_count("checkpoint_finalizers_sent", 1);

                            info!("Epoch finalized: {}", epoch.0);
                            send(
                                CHECKPOINT_COORDINATOR_CHANNEL,
                                CheckpointMessage::Finalizer(epoch),
                            )
                            .unwrap();

                            record_in_flight_gauge(&epochs, &metrics_recorder);
                        }
                    }
                    Ok(CheckpointMessage::SourceComplete(source_name)) => {
                        info!("Source completed: {}", source_name);
                        // SourceComplete messages are handled by the channel system directly during send calls.
                        // No additional processing needed here.
                    }
                    Ok(_) => {} // Ignore other messages
                    Err(RecvTimeoutError::Timeout) => {
                        // Short sleep to yield CPU
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(e) => {
                        error!("Error receiving message: {:?}", e);
                    }
                }
            }
        });
        self.handles.push(subscriber_handle);

        let epochs = Arc::clone(&self.epochs);
        let running = Arc::clone(&self.running);
        let next_epoch_counter = Arc::clone(&self.next_epoch);
        let expected_sinks_prod = Arc::clone(&expected_sinks);

        let producer_handle = tokio::spawn(async move {
            let metrics_recorder = get_checkpoint_metrics_recorder();

            while running.load(Ordering::SeqCst) {
                let sleep_start = Instant::now();
                debug!(
                    "Checkpoint producer starting interval sleep ({}s)",
                    sender_interval_sec
                );
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(sender_interval_sec)) => {
                        // Sleep completed normally
                    }
                    _ = async {
                        while running.load(Ordering::SeqCst) {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    } => {
                        // running flag changed to false, exit early
                        break;
                    }
                }

                let sleep_elapsed = sleep_start.elapsed();
                if sleep_elapsed > Duration::from_secs(sender_interval_sec + 5) {
                    warn!(
                        "Checkpoint producer sleep took {:?}, expected {}s",
                        sleep_elapsed, sender_interval_sec
                    );
                } else {
                    debug!("Checkpoint producer sleep took {:?}", sleep_elapsed);
                }

                if !running.load(Ordering::SeqCst) {
                    break;
                }

                // Wait for previous epoch to be finalized before creating a new one
                let finalization_start = Instant::now();
                let mut last_log = Instant::now();
                debug!("Checkpoint producer checking for previous epoch finalization");
                loop {
                    let (last_epoch_finalized, epoch_debug) = {
                        let epochs_guard = epochs.lock();
                        let entry = epochs_guard.iter().next_back();
                        let finalized = entry
                            .map(|(_, state)| matches!(state, EpochState::Finalized))
                            .unwrap_or(true); // No previous epoch means we can proceed
                        let debug_info = entry
                            .map(|(e, state)| format!("epoch={}, state={:?}", e.0, state))
                            .unwrap_or_else(|| "no epochs".to_string());
                        (finalized, debug_info)
                    };

                    if last_epoch_finalized {
                        break;
                    }

                    // Log epoch state every 10 seconds while waiting, including missing sinks
                    if last_log.elapsed() > Duration::from_secs(10) {
                        let missing_sinks: Vec<String> = {
                            let epochs_guard = epochs.lock();
                            if let Some((_, state)) = epochs_guard.iter().next_back() {
                                match state {
                                    EpochState::InProgress { acked_sinks, .. } => {
                                        expected_sinks_prod
                                            .iter()
                                            .filter(|s| !acked_sinks.contains(*s))
                                            .cloned()
                                            .collect()
                                    }
                                    EpochState::Started { .. } => expected_sinks_prod.to_vec(),
                                    _ => vec![],
                                }
                            } else {
                                vec![]
                            }
                        };
                        warn!(
                            "Checkpoint producer still waiting for finalization ({:?} elapsed): {} — missing sinks: {:?}",
                            finalization_start.elapsed(),
                            epoch_debug,
                            missing_sinks
                        );
                        last_log = Instant::now();
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;

                    if !running.load(Ordering::SeqCst) {
                        return;
                    }
                }

                let finalization_elapsed = finalization_start.elapsed();
                metrics_recorder.record_time("checkpoint_finalization_wait", finalization_elapsed);
                if finalization_elapsed > Duration::from_secs(5) {
                    warn!(
                        "Checkpoint producer waited {:?} for previous epoch to finalize",
                        finalization_elapsed
                    );
                } else {
                    debug!(
                        "Checkpoint producer finalization check took {:?}",
                        finalization_elapsed
                    );
                }

                let created_at = Instant::now();
                let created_at_ms = now_ms();
                let epoch_num = next_epoch_counter.fetch_add(1, Ordering::SeqCst);
                let new_epoch = CheckpointEpoch(epoch_num);
                {
                    let mut epochs_guard = epochs.lock();
                    // Previous epoch is finalized; clear it before inserting the new one
                    epochs_guard.clear();
                    epochs_guard.insert(new_epoch.clone(), EpochState::Started { created_at });
                }

                record_in_flight_gauge(&epochs, &metrics_recorder);

                info!("Sending checkpoint with epoch: {}", new_epoch.0);
                match send(
                    CHECKPOINT_COORDINATOR_CHANNEL,
                    CheckpointMessage::Marker {
                        epoch: new_epoch.clone(),
                        created_at_ms,
                    },
                ) {
                    Ok(successful_sends) => {
                        // Record markers sent metric
                        metrics_recorder.record_count("checkpoint_markers_sent", 1);

                        if successful_sends == 0 {
                            warn!(
                                "Checkpoint marker for epoch {} sent but no receivers were available",
                                new_epoch.0
                            );
                        } else {
                            debug!(
                                "Checkpoint marker for epoch {} sent to {} receiver(s)",
                                new_epoch.0, successful_sends
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to send checkpoint marker for epoch {}: {:?}. \
                            If this happened outside of a shutdown scenario, it may be an issue that cause some checkpoints to be missed.",
                            new_epoch.0, e
                        );
                        // Remove the epoch from tracking since we failed to send it
                        let mut epochs_guard = epochs.lock();
                        epochs_guard.remove(&new_epoch);
                    }
                }
            }
        });
        self.handles.push(producer_handle);

        // Timeout checker task - periodically checks for stalled epochs
        let epochs = Arc::clone(&self.epochs);
        let running = Arc::clone(&self.running);
        let timeout_duration = Duration::from_secs(self.timeout_sec);

        let timeout_handle = tokio::spawn(async move {
            let metrics_recorder = get_checkpoint_metrics_recorder();
            let check_interval = Duration::from_secs(30); // Check every 30 seconds
            let poll_interval = Duration::from_millis(500); // Poll for shutdown frequently

            while running.load(Ordering::SeqCst) {
                // Sleep in small increments to respond quickly to shutdown
                let mut elapsed = Duration::ZERO;
                while elapsed < check_interval && running.load(Ordering::SeqCst) {
                    tokio::time::sleep(poll_interval).await;
                    elapsed += poll_interval;
                }

                if !running.load(Ordering::SeqCst) {
                    break;
                }

                // Collect and count in-flight under a single lock to avoid a race
                // where the ack handler finalizes an epoch between these phases.
                let timed_out_count = {
                    let epochs_guard = epochs.lock();
                    let timed_out: Vec<CheckpointEpoch> = epochs_guard
                        .iter()
                        .filter_map(|(epoch, state)| match state {
                            EpochState::Started { created_at }
                            | EpochState::InProgress { created_at, .. } => {
                                if created_at.elapsed() > timeout_duration {
                                    Some(epoch.clone())
                                } else {
                                    None
                                }
                            }
                            EpochState::Finalized => None,
                        })
                        .collect();

                    for epoch in &timed_out {
                        warn!(
                            "Checkpoint epoch {} timed out after {:?}",
                            epoch.0, timeout_duration
                        );
                    }

                    let count = timed_out.len();

                    // Compute in-flight while we still hold the lock
                    let in_flight = epochs_guard
                        .values()
                        .filter(|s| !matches!(s, EpochState::Finalized))
                        .count();
                    metrics_recorder.record_gauge("checkpoint_epochs_in_flight", in_flight as u64);

                    count
                };

                if timed_out_count > 0 {
                    metrics_recorder
                        .record_count("checkpoint_epochs_failed", timed_out_count as u64);
                }
            }
        });
        self.handles.push(timeout_handle);
    }

    pub async fn stop(&mut self) {
        debug!("Stopping checkpoint coordinator");
        self.running.store(false, Ordering::SeqCst);

        for handle in std::mem::take(&mut self.handles) {
            let _ = handle.await;
        }

        info!("Checkpoint coordinator stopped");
    }
}

impl Default for CheckpointCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: use something more efficient than JSON, can be Rust-only
pub fn enrich_batch_metadata_with_checkpoints(
    batch_metadata: &mut HashMap<String, String>,
    messages: &[CheckpointMessage],
) {
    let serialized = serde_json::to_string(messages).unwrap_or_else(|_| "[]".to_string());
    batch_metadata.insert(CHECKPOINT_MESSAGES_KEY.to_string(), serialized);
}

pub fn extract_checkpoint_messages(
    batch_metadata: &HashMap<String, String>,
) -> Vec<CheckpointMessage> {
    if let Some(serialized) = batch_metadata.get(CHECKPOINT_MESSAGES_KEY) {
        serde_json::from_str(serialized).unwrap_or_else(|_| Vec::new())
    } else {
        Vec::new()
    }
}

/// Remove checkpoint messages metadata from a record batch, preserving all other metadata and data.
///
/// This is used by the batch accumulator to strip checkpoint markers from remainder slices
/// after a batch is split, preventing duplicate checkpoint acks.
pub fn strip_checkpoint_messages(batch: &RecordBatch) -> RecordBatch {
    let mut metadata = batch.schema().metadata().clone();
    if metadata.remove(CHECKPOINT_MESSAGES_KEY).is_some() {
        let schema = Arc::new(Schema::new_with_metadata(
            batch.schema().fields().clone(),
            metadata,
        ));
        RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap_or_else(|_| batch.clone())
    } else {
        batch.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(epoch: u64) -> CheckpointMessage {
        CheckpointMessage::Marker {
            epoch: CheckpointEpoch(epoch),
            created_at_ms: 0,
        }
    }

    fn epochs_of(messages: &[CheckpointMessage]) -> Vec<u64> {
        messages
            .iter()
            .filter_map(|m| match m {
                CheckpointMessage::Marker { epoch, .. } => Some(epoch.0),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn aligner_releases_a_marker_only_after_every_input_delivers_it() {
        let mut aligner = MarkerAligner::new(2);
        assert!(
            aligner.observe(vec![marker(1)]).is_empty(),
            "one of two inputs is not enough"
        );
        assert_eq!(
            epochs_of(&aligner.observe(vec![marker(1)])),
            vec![1],
            "the second copy completes the epoch"
        );
    }

    #[test]
    fn aligner_absorbs_a_late_copy_of_an_already_released_epoch() {
        let mut aligner = MarkerAligner::new(2);
        aligner.observe(vec![marker(1)]);
        assert_eq!(epochs_of(&aligner.observe(vec![marker(1)])), vec![1]);
        // A third copy (e.g. from an input that ended and was re-counted) must
        // not release the epoch a second time — that would break the
        // "at most one copy per stream per epoch" invariant the sink ack relies on.
        assert!(aligner.observe(vec![marker(1)]).is_empty());
    }

    #[test]
    fn aligner_treats_an_ended_input_as_having_delivered() {
        let mut aligner = MarkerAligner::new(2);
        assert!(aligner.observe(vec![marker(7)]).is_empty());
        assert_eq!(
            epochs_of(&aligner.input_done()),
            vec![7],
            "an input that ends can no longer withhold an epoch"
        );
    }

    #[test]
    fn aligner_releases_later_epochs_at_the_reduced_input_count() {
        let mut aligner = MarkerAligner::new(2);
        aligner.input_done();
        assert_eq!(
            epochs_of(&aligner.observe(vec![marker(3)])),
            vec![3],
            "with one input left a single copy completes the epoch"
        );
    }

    #[test]
    fn aligner_forwards_finalizers_and_completions_once() {
        let mut aligner = MarkerAligner::new(2);
        let first = aligner.observe(vec![CheckpointMessage::Finalizer(CheckpointEpoch(1))]);
        assert_eq!(first.len(), 1, "the first finalizer copy is forwarded");
        assert!(
            aligner
                .observe(vec![CheckpointMessage::Finalizer(CheckpointEpoch(1))])
                .is_empty(),
            "a finalizer is a broadcast, not a barrier: later copies are dropped"
        );

        let complete = CheckpointMessage::SourceComplete("src".to_string());
        assert_eq!(aligner.observe(vec![complete.clone()]).len(), 1);
        assert!(aligner.observe(vec![complete]).is_empty());
    }

    #[test]
    fn sink_gate_acks_once_every_stream_reported() {
        let sink = "gate_test_sink";
        register_sink_streams(sink, 2);
        assert!(
            !report_marker_at_sink(sink, CheckpointEpoch(1)),
            "the first of two write streams must not ack"
        );
        assert!(
            report_marker_at_sink(sink, CheckpointEpoch(1)),
            "the last write stream releases the ack"
        );
    }

    #[test]
    fn sink_gate_releases_when_a_write_stream_finishes() {
        let sink = "gate_test_sink_stream_done";
        register_sink_streams(sink, 2);
        assert!(!report_marker_at_sink(sink, CheckpointEpoch(4)));
        assert_eq!(
            sink_stream_done(sink),
            vec![CheckpointEpoch(4)],
            "a finished stream cannot report the epoch, so it is released"
        );
    }

    #[test]
    fn unregistered_sink_acks_immediately() {
        assert!(
            report_marker_at_sink("never_registered_sink", CheckpointEpoch(1)),
            "single-stream sinks keep their ungated behaviour"
        );
    }

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Fields};
    use tokio::time::sleep;

    fn create_test_batch_with_metadata(
        num_rows: usize,
        metadata: HashMap<String, String>,
    ) -> RecordBatch {
        let fields: Fields = vec![Field::new("id", DataType::Int32, false)].into();
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        let ids: Vec<i32> = (0..num_rows as i32).collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap()
    }

    #[test]
    fn test_strip_checkpoint_messages() {
        let mut metadata = HashMap::new();
        metadata.insert("custom_key".to_string(), "custom_value".to_string());
        enrich_batch_metadata_with_checkpoints(
            &mut metadata,
            &[CheckpointMessage::Marker {
                epoch: CheckpointEpoch(1),
                created_at_ms: 1000,
            }],
        );

        let batch = create_test_batch_with_metadata(10, metadata);

        // Verify checkpoint metadata exists before stripping
        assert!(!extract_checkpoint_messages(batch.schema().metadata()).is_empty());

        let stripped = strip_checkpoint_messages(&batch);

        // Checkpoint metadata should be removed
        assert!(extract_checkpoint_messages(stripped.schema().metadata()).is_empty());
        // Other metadata should be preserved
        assert_eq!(
            stripped.schema().metadata().get("custom_key"),
            Some(&"custom_value".to_string())
        );
        // Data should be preserved
        assert_eq!(stripped.num_rows(), 10);
    }

    #[test]
    fn test_strip_checkpoint_messages_no_op() {
        let mut metadata = HashMap::new();
        metadata.insert("custom_key".to_string(), "custom_value".to_string());

        let batch = create_test_batch_with_metadata(5, metadata);

        let stripped = strip_checkpoint_messages(&batch);

        // Should be unchanged
        assert_eq!(
            stripped.schema().metadata().get("custom_key"),
            Some(&"custom_value".to_string())
        );
        assert!(
            !stripped
                .schema()
                .metadata()
                .contains_key(CHECKPOINT_MESSAGES_KEY)
        );
        assert_eq!(stripped.num_rows(), 5);
    }

    #[tokio::test]
    async fn test_unexpected_ack_does_not_finalize_epoch() {
        let mut coordinator = CheckpointCoordinator::with_timeout(300);
        let epoch = CheckpointEpoch(42);

        {
            let mut epochs = coordinator.epochs.lock();
            epochs.insert(
                epoch.clone(),
                EpochState::Started {
                    created_at: Instant::now(),
                },
            );
        }

        coordinator.start(3600, vec!["expected_sink".to_string()]);

        send(
            CHECKPOINT_COORDINATOR_CHANNEL,
            CheckpointMessage::Ack {
                epoch: epoch.clone(),
                sink_id: "unexpected_sink".to_string(),
            },
        )
        .unwrap();

        sleep(Duration::from_millis(100)).await;

        {
            let epochs = coordinator.epochs.lock();
            assert!(matches!(
                epochs.get(&epoch),
                Some(EpochState::Started { .. })
            ));
        }

        send(
            CHECKPOINT_COORDINATOR_CHANNEL,
            CheckpointMessage::Ack {
                epoch: epoch.clone(),
                sink_id: "expected_sink".to_string(),
            },
        )
        .unwrap();

        sleep(Duration::from_millis(100)).await;

        {
            let epochs = coordinator.epochs.lock();
            assert!(matches!(epochs.get(&epoch), Some(EpochState::Finalized)));
        }

        coordinator.stop().await;
    }
}
