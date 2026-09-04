use crate::checkpoints::channels::{SubscriberId, send, subscribe_with_id, unsubscribe};
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
///
/// # Finalizer consumer contract (load-bearing — read before adding a connector)
///
/// Every Finalizer consumer MUST be idempotent and non-blocking, and MUST
/// NEVER gate progress on receiving the Finalizer for one specific epoch:
/// - Finalizers can be delivered more than once for the same epoch.
/// - Finalizers can be SKIPPED entirely: a timer epoch still in flight when a
///   terminal checkpoint begins is retained but may never collect its
///   remaining acks (a completed source can no longer carry its marker), and
///   a terminal epoch that never finalizes has its Finalizer withheld
///   deliberately.
/// - Commit/cleanup semantics must therefore be cumulative (offsets, current
///   state snapshots) or range-based (`<= N-1` truncation), so a later
///   Finalizer covers any skipped one.
///
/// A consumer that waits for an exact epoch's Finalizer will wedge terminal
/// checkpoints and hang shutdown.
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
            // Best-effort: during shutdown a source may have dropped its
            // channel receiver already; `send_checkpoint_ack` logs instead of
            // panicking so a failed broadcast can't kill the sink mid-drain.
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
    /// The sinks the coordinator waits on to ack an epoch before finalizing it.
    /// Mutable so a sink whose upstream source has completed can be dropped
    /// (via [`CheckpointControl::sink_completed`]) and stop blocking
    /// finalization. Populated in [`CheckpointCoordinator::start`]; shared with
    /// the subscriber/producer tasks and any [`CheckpointControl`] handle.
    expected_sinks: Arc<Mutex<HashSet<String>>>,
    /// Set once a terminal checkpoint has begun (a bounded source is completing
    /// or a shutdown was requested). Gates the timer producer so it stops
    /// issuing new epochs and never clears the terminal epoch out of the map.
    terminal_started: Arc<AtomicBool>,
    /// The terminal epoch, once begun. [`CheckpointControl::await_terminal_finalized`]
    /// resolves when this epoch reaches `Finalized`.
    terminal_epoch: Arc<Mutex<Option<CheckpointEpoch>>>,
    /// Woken whenever any epoch transitions to `Finalized`, so
    /// [`CheckpointControl::await_terminal_finalized`] wakes immediately
    /// instead of polling.
    finalized_notify: Arc<tokio::sync::Notify>,
    running: Arc<AtomicBool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    /// Subscriber id for the coordinator's own channel subscription, so
    /// [`CheckpointCoordinator::stop`] can unsubscribe and not leave a dead
    /// sender in the global channel map.
    subscriber_id: Option<SubscriberId>,
    timeout_sec: u64,
}

/// A cloneable handle for signalling checkpoint lifecycle events to the
/// coordinator from outside its subscriber task — e.g. the pipeline run loop
/// observing a sink future drain, or a bounded source completing. These are
/// control-plane signals, so they are handle methods rather than
/// `CheckpointMessage` variants: the on-the-wire protocol stays limited to
/// data-plane markers, acks, and finalizers.
#[derive(Clone)]
pub struct CheckpointControl {
    epochs: Arc<Mutex<BTreeMap<CheckpointEpoch, EpochState>>>,
    expected_sinks: Arc<Mutex<HashSet<String>>>,
    next_epoch: Arc<AtomicU64>,
    terminal_started: Arc<AtomicBool>,
    terminal_epoch: Arc<Mutex<Option<CheckpointEpoch>>>,
    finalized_notify: Arc<tokio::sync::Notify>,
}

/// Lock discipline for [`CheckpointControl`]'s fields:
/// `begin_terminal_checkpoint` is the only method allowed to hold `epochs`
/// and `terminal_epoch` simultaneously (acquired in that order). Every other
/// method takes ONE lock at a time, binding-and-dropping each guard before
/// acquiring the next — see `is_terminal_finalized` for the match-scrutinee
/// temporary that silently violates this. The subscriber/producer paths lock
/// `epochs` then `expected_sinks`, so no method here may hold
/// `expected_sinks` while acquiring `epochs`.
impl CheckpointControl {
    /// Mark a sink as drained: it will never ack again, so drop it from the
    /// expected-ack set and finalize any in-flight epoch the remaining live
    /// sinks have already acked. Without this, an epoch waiting on a completed
    /// sink — and any shutdown gated on finalization — blocks forever (the
    /// multi-source completion case: one branch finishes while others run on).
    pub fn sink_completed(&self, sink_id: &str) {
        // Snapshot the remaining set and release its lock before touching
        // `epochs`, so we never hold both locks at once. The subscriber/producer
        // paths lock epochs-then-expected; taking only one lock at a time here
        // keeps the lock order acyclic.
        let expected_snapshot: HashSet<String> = {
            let mut expected = self.expected_sinks.lock();
            if !expected.remove(sink_id) {
                // Unknown sink or already removed — nothing to do.
                return;
            }
            expected.clone()
        };
        // Deliberate relaxation: two sinks completing concurrently may each
        // snapshot before the other's removal lands, so a snapshot can be
        // stale-LARGER than the live set — `finalize_ready_epochs` then
        // conservatively declines to finalize, and the other call (whose
        // snapshot does reflect both removals) finishes the job. Snapshots are
        // never stale-smaller, so nothing finalizes early.
        info!(
            "Sink completed: {} (removed from expected ack set, {} live sink(s) remain)",
            sink_id,
            expected_snapshot.len()
        );

        let newly_finalized = finalize_ready_epochs(&self.epochs, &expected_snapshot);
        if newly_finalized.is_empty() {
            return;
        }

        // Single-sender guarantee: the transition to `Finalized` happens under
        // the epochs lock (here in `finalize_ready_epochs`, or in the
        // subscriber's ack arm), and only the path that performed the
        // transition broadcasts the Finalizer — the ack arm no-ops on an
        // already-`Finalized` epoch. Even if a duplicate ever slipped through,
        // every Finalizer consumer is idempotent (offset commits, state saves,
        // `DELETE <= N-1` truncation).
        let metrics_recorder = get_checkpoint_metrics_recorder();
        for epoch in newly_finalized {
            metrics_recorder.record_count("checkpoint_epochs_succeeded", 1);
            metrics_recorder.record_count("checkpoint_finalizers_sent", 1);
            info!("Epoch {} finalized after sink completion", epoch.0);
            let _ = send(
                CHECKPOINT_COORDINATOR_CHANNEL,
                CheckpointMessage::Finalizer(epoch),
            );
        }
        self.finalized_notify.notify_waiters();
        record_in_flight_gauge(&self.epochs, &metrics_recorder);
    }

    /// Begin the terminal checkpoint: the single final epoch a completing
    /// bounded source (or the shutdown path) emits inline after the last data,
    /// so the sinks flush and ack a checkpoint covering all of it before the
    /// pipeline tears down. The caller must emit the returned epoch as a
    /// `Marker` on a final (synthetic) batch so it flows to the sinks after the
    /// last data.
    ///
    /// Setting `terminal_started` first, then minting and inserting under the
    /// epochs lock, prevents the timer producer from clearing this epoch: the
    /// producer re-checks `terminal_started` under the same lock before it
    /// clears/inserts, so it can never race a fresh timer epoch over the
    /// terminal one. Idempotent: a second call returns the existing terminal
    /// epoch.
    ///
    /// Known trade-off (multi-source completion): the FIRST completing source
    /// stops the timer producer for the whole pipeline, so branches that are
    /// still producing get no further NEW periodic checkpoints — their tail's
    /// durability rides on the last in-flight timer epoch (retained below so
    /// it can still finalize off late acks) and then the shared terminal
    /// epoch. A crash in that window replays those branches from their last
    /// finalized epoch (at-least-once preserved; the window is bounded by job
    /// length). Keeping per-branch timer checkpoints alive would require
    /// per-branch epoch tracking in the coordinator — a larger redesign
    /// deliberately out of scope here.
    pub fn begin_terminal_checkpoint(&self) -> CheckpointEpoch {
        self.terminal_started.store(true, Ordering::SeqCst);

        // Lock order: `epochs` then `terminal_epoch` — the ONE place both are
        // held at once, which makes this order the pipeline-wide invariant.
        // Every other path must either take `terminal_epoch` alone and drop it
        // before touching `epochs` (see `await_terminal_finalized` /
        // `is_terminal_finalized`), or nest inside `epochs` like here.
        let mut epochs = self.epochs.lock();
        if let Some(existing) = self.terminal_epoch.lock().clone() {
            return existing;
        }
        // In-flight timer epochs are RETAINED alongside the terminal epoch
        // (they used to be dropped here). Their markers were broadcast before
        // terminal began and are still riding batches through the pipeline,
        // so acks for them keep arriving — the subscriber's ack arm handles
        // any epoch in this map, so a late-acked timer epoch still finalizes
        // and its Finalizer still commits durable state (Kafka offsets,
        // ClickHouse split state) for branches that are still producing.
        // Dropping them threw that last periodic checkpoint away and widened
        // the §5.4 replay window by up to one checkpoint interval per branch
        // for no benefit.
        //
        // An epoch whose remaining markers can no longer reach a sink (the
        // sink is fed only by the completed source) simply never finalizes:
        // nothing waits on timer epochs — the run loop awaits only the
        // terminal epoch — so the cost is a `checkpoint_epochs_failed` metric
        // from the timeout checker, and `stop()` clears the map at teardown.
        //
        // Skipped Finalizers remain safe for every audited consumer (all
        // fire-and-forget; none matches an exact epoch in a way that can
        // stall):
        // - Kafka source: exact-match offset lookup with a silent no-op on
        //   miss; commits are cumulative, so the next finalized epoch covers
        //   any skipped one (bounded replay, never a wait).
        // - ClickHouse source: persists its CURRENT split state on any
        //   Finalizer; a skipped epoch delays persistence to the next one.
        // - Postgres sink: range truncation (`DELETE <= N-1`).
        // - Plugin operators/sinks: Finalizers are forwarded to the plugin,
        //   whose contract requires idempotent, non-blocking handling.
        // Any FUTURE consumer must follow the same rule: never gate progress
        // on receiving the Finalizer for one specific epoch.
        let epoch_num = self.next_epoch.fetch_add(1, Ordering::SeqCst);
        let terminal = CheckpointEpoch(epoch_num);
        epochs.insert(
            terminal.clone(),
            EpochState::Started {
                created_at: Instant::now(),
            },
        );
        *self.terminal_epoch.lock() = Some(terminal.clone());
        info!("Begin terminal checkpoint: epoch {}", terminal.0);
        terminal
    }

    /// Resolve once the terminal checkpoint has finalized (all live sinks
    /// acked it). Returns immediately if no terminal checkpoint was begun.
    ///
    /// Ordering requirement: this is "wait for the terminal checkpoint that
    /// EXISTS", not "wait for one to appear" — a caller that races ahead of
    /// [`Self::begin_terminal_checkpoint`] falls through the `None` arm and
    /// skips the wait entirely. Callers must therefore hold the same
    /// `CheckpointControl` that minted the terminal epoch and only await
    /// AFTER `begin_terminal_checkpoint` returned (as the completion paths in
    /// the hybrid provider do), or gate on a signal that implies it (as the
    /// run loop does: it awaits only after every sink future drained, which a
    /// bounded source's stream-end — and thus its `begin` call — precedes).
    pub async fn await_terminal_finalized(&self) {
        loop {
            let terminal = self.terminal_epoch.lock().clone();
            match terminal {
                None => return,
                Some(epoch) => {
                    if matches!(self.epochs.lock().get(&epoch), Some(EpochState::Finalized)) {
                        return;
                    }
                }
            }
            // Wake immediately on a finalization (every transition to
            // `Finalized` calls `notify_waiters`), with a coarse fallback poll
            // so an edge lost between the state check above and waiter
            // registration can never wedge the wait. 200ms is deliberate:
            // rare enough to be free, and worst-case latency only applies to
            // the lost-edge race — the notify path is the fast path. Don't
            // tighten it into a busy-poll or widen it into drain latency.
            tokio::select! {
                _ = self.finalized_notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
    }

    /// Sink ids still expected to ack the terminal epoch, sorted. Empty when
    /// no terminal checkpoint was begun or it already finalized. Diagnostic
    /// only — the answer can be stale by the time the caller logs it, so it
    /// must not gate control flow.
    pub fn pending_terminal_ack_sinks(&self) -> Vec<String> {
        // One lock at a time (see the impl-level lock discipline): clone the
        // terminal epoch, then the expected set, then inspect epochs.
        let terminal = self.terminal_epoch.lock().clone();
        let Some(epoch) = terminal else {
            return Vec::new();
        };
        let expected: HashSet<String> = self.expected_sinks.lock().clone();
        let mut pending: Vec<String> = {
            let epochs = self.epochs.lock();
            match epochs.get(&epoch) {
                Some(EpochState::Finalized) => Vec::new(),
                Some(EpochState::InProgress { acked_sinks, .. }) => expected
                    .iter()
                    .filter(|s| !acked_sinks.contains(*s))
                    .cloned()
                    .collect(),
                // No acks yet (or the epoch is gone): every live sink is
                // still outstanding.
                Some(EpochState::Started { .. }) | None => expected.into_iter().collect(),
            }
        };
        pending.sort();
        pending
    }

    /// Returns true if a terminal checkpoint was begun and has finalized.
    pub fn is_terminal_finalized(&self) -> bool {
        // Bind (and drop) the terminal_epoch guard BEFORE locking epochs: a
        // match-scrutinee temporary lives to the end of the match, which would
        // acquire terminal_epoch -> epochs and invert
        // begin_terminal_checkpoint's epochs -> terminal_epoch order (silent
        // parking_lot deadlock). Same pattern as await_terminal_finalized.
        let terminal = self.terminal_epoch.lock().clone();
        match terminal {
            None => false,
            Some(epoch) => matches!(self.epochs.lock().get(&epoch), Some(EpochState::Finalized)),
        }
    }
}

/// Transition every non-finalized epoch that the given (already shrunk)
/// expected-sink set has fully acked into `Finalized`, returning the epochs
/// that newly finalized so the caller can broadcast their `Finalizer`.
fn finalize_ready_epochs(
    epochs: &Arc<Mutex<BTreeMap<CheckpointEpoch, EpochState>>>,
    expected: &HashSet<String>,
) -> Vec<CheckpointEpoch> {
    let mut newly_finalized = Vec::new();
    let mut epochs_guard = epochs.lock();
    for (epoch, state) in epochs_guard.iter_mut() {
        let is_complete = match state {
            // A Started epoch has no acks yet; it can only be considered
            // complete if no sinks remain to ack it at all.
            EpochState::Started { .. } => expected.is_empty(),
            EpochState::InProgress { acked_sinks, .. } => {
                expected.iter().all(|s| acked_sinks.contains(s))
            }
            EpochState::Finalized => false,
        };
        if is_complete {
            *state = EpochState::Finalized;
            newly_finalized.push(epoch.clone());
        }
    }
    newly_finalized
}

/// Collect the epochs a timeout sweep should remove: `Started`/`InProgress`
/// epochs older than `timeout`, excluding the terminal epoch (its lifecycle
/// is owned by the shutdown path's bounded wait, which must be able to
/// observe it until that wait expires).
///
/// Extracted as a free function so the removal decision is unit-testable
/// without driving the timeout task's sleep loop. The caller removes the
/// returned epochs from the map under the same lock it used to build the
/// snapshot.
fn timed_out_epochs_to_remove(
    epochs: &BTreeMap<CheckpointEpoch, EpochState>,
    timeout: Duration,
    terminal: Option<&CheckpointEpoch>,
) -> Vec<CheckpointEpoch> {
    epochs
        .iter()
        .filter_map(|(epoch, state)| match state {
            EpochState::Started { created_at } | EpochState::InProgress { created_at, .. }
                if created_at.elapsed() > timeout && Some(epoch) != terminal =>
            {
                Some(epoch.clone())
            }
            _ => None,
        })
        .collect()
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
            expected_sinks: Arc::new(Mutex::new(HashSet::new())),
            terminal_started: Arc::new(AtomicBool::new(false)),
            terminal_epoch: Arc::new(Mutex::new(None)),
            finalized_notify: Arc::new(tokio::sync::Notify::new()),
            running: Arc::new(AtomicBool::new(false)),
            handles: Vec::new(),
            subscriber_id: None,
            timeout_sec,
        }
    }

    /// Returns a cloneable control handle for signalling lifecycle events
    /// (sink completion, beginning the terminal checkpoint) from outside the
    /// coordinator's tasks. The handle shares the coordinator's state, so it is
    /// valid before or after [`CheckpointCoordinator::start`].
    pub fn control(&self) -> CheckpointControl {
        CheckpointControl {
            epochs: Arc::clone(&self.epochs),
            expected_sinks: Arc::clone(&self.expected_sinks),
            next_epoch: Arc::clone(&self.next_epoch),
            terminal_started: Arc::clone(&self.terminal_started),
            terminal_epoch: Arc::clone(&self.terminal_epoch),
            finalized_notify: Arc::clone(&self.finalized_notify),
        }
    }

    pub fn start(&mut self, sender_interval_sec: u64, expected_sinks: Vec<String>) {
        let num_of_expected_acks = expected_sinks.len() as u32;
        info!(
            "Starting checkpoint coordinator, interval: {}s, acks: {}, sinks: {:?}, timeout: {}s",
            sender_interval_sec, num_of_expected_acks, expected_sinks, self.timeout_sec
        );

        self.running.store(true, Ordering::SeqCst);

        // Populate the shared expected-ack set in place (rather than replacing
        // the Arc) so any control handle taken before `start` observes it.
        *self.expected_sinks.lock() = expected_sinks.into_iter().collect();
        let expected_sinks = Arc::clone(&self.expected_sinks);

        let (receiver, subscriber_id) = subscribe_with_id(CHECKPOINT_COORDINATOR_CHANNEL);
        self.subscriber_id = Some(subscriber_id);
        let epochs = Arc::clone(&self.epochs);
        let running = Arc::clone(&self.running);
        let expected_sinks_sub = Arc::clone(&expected_sinks);
        let finalized_notify_sub = Arc::clone(&self.finalized_notify);

        // Sanctioned: structured concurrency — the handle is stored on self
        // and joined by `stop()`, which the run loop awaits under the budget.
        #[allow(clippy::disallowed_methods)]
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
                        {
                            let expected = expected_sinks_sub.lock();
                            if !expected.contains(&sink_id) {
                                warn!(
                                    "Received ack from unexpected sink '{}' for epoch {} (expected: {:?})",
                                    sink_id, epoch.0, *expected
                                );
                                continue;
                            }
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
                                            .lock()
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
                                            .lock()
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
                            // Best-effort: a receiver dropped during shutdown
                            // must not panic the coordinator.
                            let _ = send(
                                CHECKPOINT_COORDINATOR_CHANNEL,
                                CheckpointMessage::Finalizer(epoch),
                            );
                            finalized_notify_sub.notify_waiters();

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
        let terminal_started_prod = Arc::clone(&self.terminal_started);

        // Sanctioned: structured concurrency — the handle is stored on self
        // and joined by `stop()`, which the run loop awaits under the budget.
        #[allow(clippy::disallowed_methods)]
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

                // A terminal checkpoint has begun (a bounded source completed or
                // shutdown was requested). Stop the timer producer: the terminal
                // epoch is the only one that should exist now, and the source
                // will not produce more data for a fresh timer marker to ride.
                if terminal_started_prod.load(Ordering::SeqCst) {
                    debug!("Terminal checkpoint started; stopping timer producer");
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
                                            .lock()
                                            .iter()
                                            .filter(|s| !acked_sinks.contains(*s))
                                            .cloned()
                                            .collect()
                                    }
                                    EpochState::Started { .. } => {
                                        expected_sinks_prod.lock().iter().cloned().collect()
                                    }
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
                // Mint and insert under the epochs lock, re-checking the terminal
                // flag inside that same critical section. `begin_terminal_checkpoint`
                // sets the flag and inserts its epoch under this same lock
                // (in-flight timer epochs are RETAINED, not cleared — late acks
                // still finalize them), so this re-check guarantees a fresh
                // timer epoch can never race in after the terminal epoch began.
                let new_epoch = {
                    let mut epochs_guard = epochs.lock();
                    if terminal_started_prod.load(Ordering::SeqCst) {
                        debug!("Terminal checkpoint started; stopping timer producer");
                        break;
                    }
                    let epoch_num = next_epoch_counter.fetch_add(1, Ordering::SeqCst);
                    let new_epoch = CheckpointEpoch(epoch_num);
                    // Previous epoch is finalized; clear it before inserting the new one
                    epochs_guard.clear();
                    epochs_guard.insert(new_epoch.clone(), EpochState::Started { created_at });
                    new_epoch
                };

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

        // Timeout checker task — periodically REMOVES stalled epochs so the
        // timer producer (which waits for the newest epoch to finalize before
        // minting the next one) can never wedge permanently on an epoch whose
        // Marker was lost. A Marker is lost when the broadcast reaches no
        // source subscriber — e.g. the producer's first interval fires while a
        // large plan (many sources, plugin UDF compilation) is still being
        // built, so no source `execute()` has subscribed yet. No batch ever
        // carries that epoch's Marker, no sink can ack it, and without removal
        // the producer waits on it forever while data continues to flow
        // uncheckpointed. Removing the epoch restores liveness: the next
        // minted epoch's Marker reaches the (by then live) subscribers and
        // finalizes normally. Removal is safe under the Finalizer consumer
        // contract at the top of this file: Finalizers can be skipped, and
        // commit/cleanup semantics are cumulative, so the offsets a removed
        // epoch covered stay uncommitted until the next finalized epoch covers
        // them (at-least-once preserved). The terminal epoch is exempt — its
        // lifecycle is owned by the shutdown path's bounded wait.
        let epochs = Arc::clone(&self.epochs);
        let running = Arc::clone(&self.running);
        let terminal_epoch_for_timeout = Arc::clone(&self.terminal_epoch);
        let timeout_duration = Duration::from_secs(self.timeout_sec);
        // Sweep at half the timeout, clamped to [1s, 30s] — 30s for the
        // production default, sub-second-free short cadences for tests that
        // construct the coordinator with a small timeout.
        let check_interval = Duration::from_secs((self.timeout_sec / 2).clamp(1, 30));

        // Sanctioned: structured concurrency — the handle is stored on self
        // and joined by `stop()`, which the run loop awaits under the budget.
        #[allow(clippy::disallowed_methods)]
        let timeout_handle = tokio::spawn(async move {
            let metrics_recorder = get_checkpoint_metrics_recorder();
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

                // Lock discipline: take `terminal_epoch` alone and drop it
                // before touching `epochs` (see the CheckpointControl lock
                // notes — only `begin_terminal_checkpoint` may hold both).
                let terminal: Option<CheckpointEpoch> =
                    { terminal_epoch_for_timeout.lock().clone() };

                // Collect, remove, and count in-flight under a single lock to
                // avoid a race where the ack handler finalizes an epoch
                // between these phases.
                let (removed, in_flight) = {
                    let mut epochs_guard = epochs.lock();
                    let timed_out = timed_out_epochs_to_remove(
                        &epochs_guard,
                        timeout_duration,
                        terminal.as_ref(),
                    );
                    for epoch in &timed_out {
                        epochs_guard.remove(epoch);
                    }
                    let in_flight = epochs_guard
                        .values()
                        .filter(|s| !matches!(s, EpochState::Finalized))
                        .count();
                    (timed_out, in_flight)
                };
                metrics_recorder.record_gauge("checkpoint_epochs_in_flight", in_flight as u64);

                for epoch in &removed {
                    warn!(
                        "Checkpoint epoch {} timed out after {:?}; removed so a fresh epoch can be \
                         minted (its Marker may never have reached a live source — late acks for it \
                         will be ignored, and its offsets stay uncommitted until the next finalized \
                         epoch covers them)",
                        epoch.0, timeout_duration
                    );
                }

                if !removed.is_empty() {
                    metrics_recorder.record_count("checkpoint_epochs_failed", removed.len() as u64);
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

        // A terminal epoch that never finalized is a broken drain contract —
        // its offsets were not committed and the drained tail replays on
        // restart. It is exempt from the timeout checker (by design), so
        // without this sweep it failed without ever touching
        // `checkpoint_epochs_failed`: on a fleet monitored through metrics, a
        // shutdown that lost its tail looked purely successful. Only the
        // terminal epoch is counted — ordinary in-flight timer epochs at
        // teardown are routine (fast-exit leaves them uncommitted on purpose;
        // the replayed tail covers them). The final flush in main() exports
        // this before the process exits. Lock discipline: bind and drop the
        // terminal_epoch guard BEFORE locking epochs (see
        // is_terminal_finalized for why).
        let terminal = self.terminal_epoch.lock().clone();
        if let Some(epoch) = terminal
            && !matches!(self.epochs.lock().get(&epoch), Some(EpochState::Finalized))
        {
            warn!(
                "Terminal checkpoint epoch {} never finalized before coordinator stop; recording it as failed (offsets stay uncommitted; the tail replays on restart)",
                epoch.0
            );
            get_checkpoint_metrics_recorder().record_count("checkpoint_epochs_failed", 1);
        }

        // The subscriber task has exited and dropped its receiver; remove the
        // now-dead sender from the global channel map so later sends don't fail
        // against it.
        if let Some(subscriber_id) = self.subscriber_id.take() {
            unsubscribe(CHECKPOINT_COORDINATOR_CHANNEL, subscriber_id);
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
    use serial_test::serial;
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
    #[serial]
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

    #[tokio::test]
    #[serial]
    async fn test_sink_completion_unblocks_finalization() {
        // Two sinks are expected. The epoch is acked by sink_b only, so it
        // stays in progress waiting on sink_a. When sink_a's branch finishes (a
        // bounded source completed), sink_a will never ack again. Marking it
        // complete must remove it from the expected set and let the epoch
        // finalize on the remaining live sink — otherwise the coordinator (and
        // any shutdown gated on finalization) blocks forever.
        let mut coordinator = CheckpointCoordinator::with_timeout(300);
        let epoch = CheckpointEpoch(1042);

        {
            let mut epochs = coordinator.epochs.lock();
            epochs.insert(
                epoch.clone(),
                EpochState::Started {
                    created_at: Instant::now(),
                },
            );
        }

        coordinator.start(
            3600,
            vec!["sink_a_dereg".to_string(), "sink_b_dereg".to_string()],
        );

        // Only sink_b acks; epoch is now InProgress waiting on sink_a.
        send(
            CHECKPOINT_COORDINATOR_CHANNEL,
            CheckpointMessage::Ack {
                epoch: epoch.clone(),
                sink_id: "sink_b_dereg".to_string(),
            },
        )
        .unwrap();

        sleep(Duration::from_millis(100)).await;

        {
            let epochs = coordinator.epochs.lock();
            assert!(
                matches!(epochs.get(&epoch), Some(EpochState::InProgress { .. })),
                "epoch should still be in progress, waiting on sink_a"
            );
        }

        // sink_a's branch finished and will never ack. Deregister it via the
        // control handle (no checkpoint message involved).
        coordinator.control().sink_completed("sink_a_dereg");

        sleep(Duration::from_millis(100)).await;

        {
            let epochs = coordinator.epochs.lock();
            assert!(
                matches!(epochs.get(&epoch), Some(EpochState::Finalized)),
                "epoch should finalize once the only outstanding sink completes"
            );
        }

        coordinator.stop().await;
    }

    #[tokio::test]
    #[serial]
    async fn test_terminal_checkpoint_blocks_until_acked() {
        // A bounded source, on completion, begins a terminal checkpoint and the
        // pipeline must not tear down until that epoch finalizes. The await must
        // block until the sink acks the terminal marker, then return.
        let mut coordinator = CheckpointCoordinator::with_timeout(300);
        coordinator.start(3600, vec!["sink_terminal".to_string()]);
        let control = coordinator.control();

        let terminal = control.begin_terminal_checkpoint();

        // No sink has acked the terminal marker yet, so the await must block.
        assert!(
            !control.is_terminal_finalized(),
            "terminal epoch must not be finalized before any sink acks"
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(150),
                control.await_terminal_finalized()
            )
            .await
            .is_err(),
            "await_terminal_finalized must block until the terminal epoch is acked"
        );

        // The sink processes the terminal marker, flushes, and acks.
        send(
            CHECKPOINT_COORDINATOR_CHANNEL,
            CheckpointMessage::Ack {
                epoch: terminal.clone(),
                sink_id: "sink_terminal".to_string(),
            },
        )
        .unwrap();

        // Now the terminal checkpoint finalizes and the await returns.
        tokio::time::timeout(Duration::from_secs(2), control.await_terminal_finalized())
            .await
            .expect("await_terminal_finalized must return once the terminal epoch finalizes");
        assert!(
            control.is_terminal_finalized(),
            "is_terminal_finalized must report true once the terminal epoch finalizes"
        );

        {
            let epochs = coordinator.epochs.lock();
            assert!(
                matches!(epochs.get(&terminal), Some(EpochState::Finalized)),
                "terminal epoch should be finalized after the sink acks"
            );
        }

        coordinator.stop().await;
    }

    #[test]
    #[serial]
    fn test_sink_completion_unblocks_finalization_synchronous() {
        // Direct-state variant of the multi-source case with no coordinator
        // tasks running: an InProgress epoch acked by sink_b, waiting on sink_a,
        // must finalize synchronously the moment sink_a is marked completed.
        // `#[serial]` because `sink_completed` broadcasts the Finalizer on the
        // global coordinator channel — running in parallel with a
        // channel-driving serial test would inject a spurious message into its
        // subscription.
        let coordinator = CheckpointCoordinator::with_timeout(300);
        let epoch = CheckpointEpoch(1042);

        {
            let mut expected = coordinator.expected_sinks.lock();
            expected.insert("sink_a_dereg".to_string());
            expected.insert("sink_b_dereg".to_string());
        }

        {
            let mut epochs = coordinator.epochs.lock();
            let mut acked_sinks = HashSet::new();
            acked_sinks.insert("sink_b_dereg".to_string());
            epochs.insert(
                epoch.clone(),
                EpochState::InProgress {
                    acked_sinks,
                    created_at: Instant::now(),
                },
            );
        }

        let control = coordinator.control();
        control.sink_completed("sink_a_dereg");

        let epochs = coordinator.epochs.lock();
        assert!(
            matches!(epochs.get(&epoch), Some(EpochState::Finalized)),
            "epoch should finalize synchronously once the only outstanding sink completes"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_failed_sink_keeps_epochs_stalled() {
        // The inverse of sink completion: a sink that FAILED is deliberately
        // NOT deregistered (the run loop only calls sink_completed on Ok), so
        // an epoch it never acked must stay un-finalized — its missing acks
        // are the coordinator's only signal that the data is not durable.
        // Offsets must not commit; the watchdog bounds the resulting stall.
        let mut coordinator = CheckpointCoordinator::with_timeout(300);
        coordinator.start(
            3600,
            vec!["sink_ok_stall".to_string(), "sink_failed_stall".to_string()],
        );
        let control = coordinator.control();

        let terminal = control.begin_terminal_checkpoint();

        // The healthy sink acks; the failed sink never does and is never
        // deregistered.
        send(
            CHECKPOINT_COORDINATOR_CHANNEL,
            CheckpointMessage::Ack {
                epoch: terminal.clone(),
                sink_id: "sink_ok_stall".to_string(),
            },
        )
        .unwrap();
        control.sink_completed("sink_ok_stall");

        // The terminal epoch must NOT finalize.
        assert!(
            tokio::time::timeout(
                Duration::from_millis(300),
                control.await_terminal_finalized()
            )
            .await
            .is_err(),
            "an epoch missing a failed sink's ack must stay un-finalized"
        );
        assert!(
            !control.is_terminal_finalized(),
            "failed sink's epoch must not report finalized"
        );

        coordinator.stop().await;
    }

    #[test]
    fn test_begin_terminal_checkpoint_is_idempotent() {
        // A second begin returns the same terminal epoch and does not mint a new
        // one (e.g. if both a bounded-completion path and the shutdown path fire).
        let coordinator = CheckpointCoordinator::with_timeout(300);
        let control = coordinator.control();

        let first = control.begin_terminal_checkpoint();
        let second = control.begin_terminal_checkpoint();
        assert_eq!(first, second, "terminal epoch must be stable across calls");

        let epochs = coordinator.epochs.lock();
        assert_eq!(
            epochs.len(),
            1,
            "only the single terminal epoch should exist"
        );
    }

    /// Regression: beginning the
    /// terminal checkpoint used to CLEAR in-flight timer epochs, throwing away
    /// the last periodic checkpoint of every still-running branch — late acks
    /// hit "unknown epoch" and the epoch's Finalizer (which commits Kafka
    /// offsets / ClickHouse split state) was never sent, widening the
    /// crash-replay window by up to one checkpoint interval per branch.
    /// In-flight epochs must survive terminal begin and still finalize.
    #[test]
    #[serial]
    fn test_terminal_begin_retains_in_flight_timer_epochs() {
        let coordinator = CheckpointCoordinator::with_timeout(300);
        let timer_epoch = CheckpointEpoch(2042);

        {
            let mut expected = coordinator.expected_sinks.lock();
            expected.insert("sink_a_retain".to_string());
            expected.insert("sink_b_retain".to_string());
        }
        {
            let mut epochs = coordinator.epochs.lock();
            let mut acked_sinks = HashSet::new();
            acked_sinks.insert("sink_b_retain".to_string());
            epochs.insert(
                timer_epoch.clone(),
                EpochState::InProgress {
                    acked_sinks,
                    created_at: Instant::now(),
                },
            );
        }

        let control = coordinator.control();
        let terminal = control.begin_terminal_checkpoint();

        {
            let epochs = coordinator.epochs.lock();
            assert!(
                epochs.contains_key(&timer_epoch),
                "in-flight timer epoch must survive terminal begin"
            );
            assert!(epochs.contains_key(&terminal), "terminal epoch must exist");
        }

        // The retained epoch still finalizes when its outstanding sink
        // completes (same for a late ack via the subscriber).
        control.sink_completed("sink_a_retain");
        let epochs = coordinator.epochs.lock();
        assert!(
            matches!(epochs.get(&timer_epoch), Some(EpochState::Finalized)),
            "retained timer epoch must still finalize after terminal begin"
        );
    }

    #[test]
    fn test_timed_out_epochs_to_remove_selects_only_stale_non_terminal() {
        let timeout = Duration::from_secs(60);
        let stale = Instant::now() - Duration::from_secs(120);
        let fresh = Instant::now();

        let mut epochs: BTreeMap<CheckpointEpoch, EpochState> = BTreeMap::new();
        epochs.insert(
            CheckpointEpoch(1),
            EpochState::Started { created_at: stale },
        );
        epochs.insert(
            CheckpointEpoch(2),
            EpochState::InProgress {
                acked_sinks: HashSet::from(["a".to_string()]),
                created_at: stale,
            },
        );
        epochs.insert(
            CheckpointEpoch(3),
            EpochState::Started { created_at: fresh },
        );
        epochs.insert(CheckpointEpoch(4), EpochState::Finalized);
        // Epoch 5 is stale but terminal — exempt: the shutdown path owns its
        // (bounded) wait and must be able to observe it until that expires.
        epochs.insert(
            CheckpointEpoch(5),
            EpochState::Started { created_at: stale },
        );

        let terminal = CheckpointEpoch(5);
        let removed = timed_out_epochs_to_remove(&epochs, timeout, Some(&terminal));
        assert_eq!(
            removed,
            vec![CheckpointEpoch(1), CheckpointEpoch(2)],
            "stale Started and InProgress epochs are removed; fresh, finalized, \
             and terminal epochs are kept"
        );

        let removed_no_terminal = timed_out_epochs_to_remove(&epochs, timeout, None);
        assert_eq!(
            removed_no_terminal,
            vec![CheckpointEpoch(1), CheckpointEpoch(2), CheckpointEpoch(5)],
            "without a terminal epoch, every stale epoch is removed"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_timeout_sweep_removes_stale_epoch_and_unwedges() {
        // An epoch whose Marker never reached any source (broadcast before the
        // sources subscribed) can never be acked. The timeout sweep must remove
        // it so the producer's finalization wait unblocks, and a later epoch
        // must still finalize normally through the regular ack path.
        let mut coordinator = CheckpointCoordinator::with_timeout(1);
        let stale_epoch = CheckpointEpoch(7);
        {
            let mut epochs = coordinator.epochs.lock();
            epochs.insert(
                stale_epoch.clone(),
                EpochState::Started {
                    created_at: Instant::now() - Duration::from_secs(5),
                },
            );
        }

        // Long producer interval: only the timeout sweep and the subscriber run.
        coordinator.start(3600, vec!["expected_sink".to_string()]);

        // Sweep cadence for timeout_sec=1 is 1s; give it two cycles.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            {
                let epochs = coordinator.epochs.lock();
                if !epochs.contains_key(&stale_epoch) {
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "stale epoch was not removed by the timeout sweep"
            );
            sleep(Duration::from_millis(100)).await;
        }

        // The pipeline is un-wedged: a later epoch finalizes via a normal ack.
        let next_epoch = CheckpointEpoch(8);
        {
            let mut epochs = coordinator.epochs.lock();
            epochs.insert(
                next_epoch.clone(),
                EpochState::Started {
                    created_at: Instant::now(),
                },
            );
        }
        send(
            CHECKPOINT_COORDINATOR_CHANNEL,
            CheckpointMessage::Ack {
                epoch: next_epoch.clone(),
                sink_id: "expected_sink".to_string(),
            },
        )
        .unwrap();
        sleep(Duration::from_millis(200)).await;
        {
            let epochs = coordinator.epochs.lock();
            assert!(
                matches!(epochs.get(&next_epoch), Some(EpochState::Finalized)),
                "the epoch after a removed one must finalize normally"
            );
        }

        coordinator.stop().await;
    }

    #[tokio::test]
    #[serial]
    async fn test_timeout_sweep_never_removes_terminal_epoch() {
        // The terminal epoch is exempt from timeout removal — the shutdown
        // path's bounded wait owns its lifecycle.
        let mut coordinator = CheckpointCoordinator::with_timeout(1);
        let control = coordinator.control();
        let terminal = control.begin_terminal_checkpoint();
        {
            // Age the terminal epoch far past the timeout.
            let mut epochs = coordinator.epochs.lock();
            epochs.insert(
                terminal.clone(),
                EpochState::Started {
                    created_at: Instant::now() - Duration::from_secs(60),
                },
            );
        }

        coordinator.start(3600, vec!["expected_sink".to_string()]);
        sleep(Duration::from_millis(2600)).await;

        {
            let epochs = coordinator.epochs.lock();
            assert!(
                epochs.contains_key(&terminal),
                "terminal epoch must survive timeout sweeps"
            );
        }

        coordinator.stop().await;
    }
}
