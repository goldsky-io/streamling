//! Proves the out-of-band shutdown signal reaches a plugin wedged inside its
//! checkpoint-marker flush — the one case the queued `Terminate` message can
//! never reach, because it sits in the FIFO *behind* the marker whose hook is
//! wedged.
//!
//! Lives in `tests/` (its own process) on purpose: the shutdown signal is a
//! one-way process-global, so flipping it here cannot contaminate the SDK's
//! unit tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use abi_stable::derive_macro_reexports::TD_Opaque;
use abi_stable::external_types::crossbeam_channel;
use abi_stable::nonexhaustive_enum::NonExhaustive;
use arrow::array::RecordBatch;
use async_ffi::{FfiFuture, FutureExt as _};
use async_trait::async_trait;
use streamling_plugin::api::SupportsGracefulShutdown;
use streamling_plugin::ffi::PluginMetricsChannel;
use streamling_plugin::shutdown::{self, ShutdownSignal, ShutdownSignal_TO, ShutdownSignalObj};
use streamling_plugin::{
    CheckpointEpoch, PluginChannel, PluginChannels, PluginCheckpointEpoch, PluginError, PluginMsg,
    SinkPlugin, SinkPluginDispatcher,
};

/// Host-side stand-in with the real adapter's exact shape: a latched flag and
/// a transition-only `cancelled` future.
#[derive(Clone)]
struct TestHostSignal {
    down: Arc<AtomicBool>,
}

impl ShutdownSignal for TestHostSignal {
    fn is_shutting_down(&self) -> bool {
        self.down.load(Ordering::SeqCst)
    }
    fn cancelled(&self) -> FfiFuture<()> {
        let down = self.down.clone();
        async move {
            while !down.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        .into_ffi()
    }
    fn remaining_budget_ms(&self) -> u64 {
        60_000
    }
    fn request_shutdown(&self) {
        self.down.store(true, Ordering::SeqCst);
    }
}

fn signal_obj(down: Arc<AtomicBool>) -> ShutdownSignalObj {
    ShutdownSignal_TO::from_value(TestHostSignal { down }, TD_Opaque)
}

#[derive(Debug)]
struct FlushError;
impl streamling_retry::RetryError for FlushError {}

/// A sink whose marker flush retries a permanently failing operation — the
/// EventBridge/Tinybird wedge shape, expressed through the SDK retry helper.
struct WedgedSink {
    running: AtomicBool,
    flush_attempts: Arc<AtomicU32>,
    terminated: Arc<AtomicBool>,
}

#[async_trait]
impl SupportsGracefulShutdown for WedgedSink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.terminated.store(true, Ordering::SeqCst);
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for WedgedSink {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }
    async fn process_batch(&self, _data: RecordBatch) -> Result<(), PluginError> {
        Ok(())
    }
    async fn process_checkpoint_marker(&self, _epoch: CheckpointEpoch) -> Result<(), PluginError> {
        let attempts = self.flush_attempts.clone();
        match shutdown::retry_until_cancelled(
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(FlushError)
                }
            },
            "wedged marker flush",
        )
        .await
        {
            streamling_retry::RetryOutcome::Completed(()) => Ok(()),
            streamling_retry::RetryOutcome::Cancelled(_) => Err(PluginError::Internal(
                "marker flush abandoned: shutdown requested".to_string(),
            )),
        }
    }
    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

fn channels() -> PluginChannels {
    PluginChannels {
        input: PluginChannel::new(crossbeam_channel::bounded(8)),
        output: PluginChannel::new(crossbeam_channel::bounded(8)),
        metrics: PluginMetricsChannel::new(crossbeam_channel::bounded(8)),
    }
}

/// The scenario end to end:
///
/// 1. Queue `Init`, `CheckpointMarker`, `Terminate` — the order the host
///    produces during a drain — and only then start the dispatcher.
/// 2. The marker hook wedges in a failing retry; assert it is actually
///    wedged (attempts grow, `terminate()` has NOT run — the queued
///    `Terminate` is stuck behind the flush, which is the whole bug class).
/// 3. Flip the out-of-band signal and assert the dispatcher finishes
///    promptly, with the retry loop giving up between attempts.
/// 4. Assert the abandoned flush surfaced as an error AND the input channel
///    was drained on the way out (Terminate consumed, `terminate()` ran
///    best-effort). The signal is still what ended the wedge — proven by
///    step 2's "not terminated while wedged" plus the prompt finish — but
///    the dispatcher must no longer exit leaving the queue full: that
///    stranded the host-side writer, made Terminate undeliverable, and rode
///    the watchdog in the field.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signal_cuts_a_wedged_marker_flush_that_terminate_cannot_reach() {
    let down = Arc::new(AtomicBool::new(false));
    shutdown::install_shutdown_signal(signal_obj(down.clone()));

    let flush_attempts = Arc::new(AtomicU32::new(0));
    let terminated = Arc::new(AtomicBool::new(false));
    let sink = Arc::new(WedgedSink {
        running: AtomicBool::new(true),
        flush_attempts: flush_attempts.clone(),
        terminated: terminated.clone(),
    });

    let chans = channels();
    let input_sender = chans.input.sender.clone();
    let input_receiver = chans.input.receiver.clone();
    let metrics_receiver = chans.metrics.receiver.clone();

    // The drain-time queue: everything is already enqueued when the wedge
    // starts, exactly like a host that sent the terminal marker and then
    // Terminate while the plugin was stuck.
    input_sender
        .send(NonExhaustive::new(PluginMsg::Init))
        .unwrap();
    input_sender
        .send(NonExhaustive::new(PluginMsg::CheckpointMarker {
            epoch: PluginCheckpointEpoch(1),
        }))
        .unwrap();
    input_sender
        .send(NonExhaustive::new(PluginMsg::Terminate))
        .unwrap();

    let dispatcher = SinkPluginDispatcher::new(chans, sink);
    let runtime = streamling_plugin::r#async::DirectTokioProxy::new().into_async_runtime_obj();
    let dispatcher_task = tokio::spawn(async move { dispatcher.start(runtime).await });

    // Wedged for real: the flush is retrying, and the queued Terminate has
    // NOT been processed (the dispatcher is stuck inside the marker hook).
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while flush_attempts.load(Ordering::SeqCst) < 3 {
        assert!(
            std::time::Instant::now() < deadline,
            "flush retry never started"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !terminated.load(Ordering::SeqCst),
        "Terminate must be stuck behind the wedged marker flush — if it ran, \
         the test no longer reproduces the head-of-line case"
    );

    // The out-of-band signal — the host flips this at SIGTERM without going
    // through the plugin's input queue.
    down.store(true, Ordering::SeqCst);

    let result = tokio::time::timeout(Duration::from_secs(5), dispatcher_task)
        .await
        .expect(
            "dispatcher must finish promptly once the signal fires — a \
                 hang here means the signal no longer reaches the retry loop",
        )
        .expect("dispatcher task must not panic");

    let err = result.expect_err("the abandoned flush must surface as an error");
    assert!(
        err.to_string().contains("shutdown"),
        "unexpected error: {err}"
    );

    // The failed dispatcher must drain its queue on the way out (the
    // drain-discard contract): an exit that leaves Terminate queued strands
    // the host-side writer on a full channel and the drain rides the
    // watchdog. The signal ending the wedge was already proven above —
    // `terminate()` had NOT run while wedged, and the finish was prompt.
    assert!(
        input_receiver.try_recv().is_err(),
        "the input channel must be fully drained after the failure"
    );
    assert!(
        terminated.load(Ordering::SeqCst),
        "terminate() must run (best effort) when the drained Terminate arrives"
    );

    // Attribution: the wedge left an UNBALANCED enter breadcrumb on the
    // metrics channel — an enter with no exit is exactly how the host's
    // watchdog dump names the plugin (and hook) that pinned the drain.
    let mut saw_enter = false;
    let mut saw_exit = false;
    while let Ok(metric) = metrics_receiver.try_recv() {
        if let Ok(streamling_plugin::ffi::PluginMetric::Count { name, .. }) = metric.into_enum() {
            if name.as_str() == streamling_plugin::ffi::DISPATCHER_HOOK_ENTER_METRIC {
                saw_enter = true;
            } else if name.as_str() == streamling_plugin::ffi::DISPATCHER_HOOK_EXIT_METRIC {
                saw_exit = true;
            }
        }
    }
    assert!(
        saw_enter,
        "the dispatcher must emit a hook-enter breadcrumb before the marker hook"
    );
    assert!(
        !saw_exit,
        "a wedged-then-abandoned hook must NOT emit the exit breadcrumb"
    );
}
