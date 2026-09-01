//! Out-of-band shutdown signal for plugins.
//!
//! # Why this exists
//!
//! `Terminate` is an ordinary message on the plugin's input channel, so a
//! plugin wedged inside a hook — retrying a dead endpoint during its
//! checkpoint-marker flush — has not dequeued it yet, and `is_running()` is
//! still `true` on exactly the occasion it needs to be `false`. The signal
//! installed here is set once at load time and flipped directly by the host,
//! so no amount of queued work can delay it.
//!
//! # The one rule
//!
//! **Read the signal inside hooks; never use it as a loop condition.** The
//! dispatcher's message loop exits on `is_running()`, which is safe precisely
//! because that flag flips only when `Terminate` is dequeued — with nothing
//! queued behind it. Wiring this signal into a loop guard lets the dispatcher
//! exit while a `CheckpointMarker` is still queued: the terminal marker is
//! abandoned, no ack is sent, offsets never commit, and the tail replays.
//! The retry helpers below consume the signal internally so there is no bare
//! boolean to reach for; [`is_shutting_down`] exists as an escape hatch for
//! checks *between units of work inside a hook*, nothing else.
//!
//! # Absence is normal
//!
//! A host older than this SDK never installs a signal. Everything here
//! degrades to a finite default — never to unbounded retry — so an old
//! engine paired with a new plugin image stays safe.

use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use abi_stable::sabi_trait;
use abi_stable::std_types::RBox;
use async_ffi::FfiFuture;
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

/// FFI-safe shutdown signal, implemented by the host and installed once per
/// plugin library via the `set_shutdown_signal` module hook.
#[sabi_trait]
pub trait ShutdownSignal: Send + Sync {
    /// Whether graceful shutdown has been requested. Latched: never reverts.
    fn is_shutting_down(&self) -> bool;

    /// Resolves once shutdown is requested. May only resolve on a transition,
    /// so callers must check [`is_shutting_down`](Self::is_shutting_down)
    /// first — the retry helpers do.
    fn cancelled(&self) -> FfiFuture<()>;

    /// Milliseconds of drain budget the host has left. The full budget until
    /// shutdown is requested, then counting down toward the hard exit.
    fn remaining_budget_ms(&self) -> u64;

    /// Ask the host to begin graceful shutdown of the whole pipeline — the
    /// same lever a bounded source pulls when its range completes.
    fn request_shutdown(&self);
}

pub type ShutdownSignalObj = ShutdownSignal_TO<'static, RBox<()>>;

/// When no host signal is installed (an engine older than this SDK): the
/// bounded-retry ceiling used during a drain. Finite on purpose — sized to
/// the engine's smallest real drain budget so the compatibility path can
/// never mean "retry forever".
const FALLBACK_BUDGET: Duration = Duration::from_secs(20);

/// Signal storage plus everything that must degrade gracefully without one.
/// Instance-scoped so tests can exercise install semantics without touching
/// the process-global cell (installing there would contaminate every later
/// test in the binary — the signal is one-way).
struct SignalCell {
    signal: OnceLock<ShutdownSignalObj>,
    tracker: TaskTracker,
}

impl SignalCell {
    fn new() -> Self {
        Self {
            signal: OnceLock::new(),
            tracker: TaskTracker::new(),
        }
    }

    fn install(&self, signal: ShutdownSignalObj) -> bool {
        let installed = self.signal.set(signal).is_ok();
        if installed {
            info!("Shutdown signal installed for this plugin library");
        } else {
            warn!("Shutdown signal installed twice; keeping the first");
        }
        installed
    }

    fn is_shutting_down(&self) -> bool {
        self.signal
            .get()
            .map(|s| s.is_shutting_down())
            .unwrap_or(false)
    }

    fn remaining_budget(&self) -> Duration {
        self.signal
            .get()
            .map(|s| Duration::from_millis(s.remaining_budget_ms()))
            .unwrap_or(FALLBACK_BUDGET)
    }

    async fn cancelled(&self) {
        match self.signal.get() {
            // Latched check first: the FFI future may only resolve on a
            // transition, and a signal that fired before this call would
            // otherwise never be observed.
            Some(s) => {
                if s.is_shutting_down() {
                    return;
                }
                s.cancelled().await;
            }
            // No signal, nothing will ever fire. Park; budget caps callers.
            None => std::future::pending().await,
        }
    }

    fn request_shutdown(&self) {
        match self.signal.get() {
            Some(s) => s.request_shutdown(),
            None => warn!(
                "request_shutdown ignored: no shutdown signal installed \
                 (host predates the shutdown-signal SDK)"
            ),
        }
    }
}

static CELL: std::sync::LazyLock<SignalCell> = std::sync::LazyLock::new(SignalCell::new);

/// Install the host's signal. Called once per library by the generated
/// `set_shutdown_signal` module hook; first install wins.
pub fn install_shutdown_signal(signal: ShutdownSignalObj) {
    CELL.install(signal);
}

/// Whether graceful shutdown has been requested. `false` when no signal is
/// installed. For checks between units of work inside a hook — never as a
/// dispatcher/loop exit condition (see the module docs for why).
pub fn is_shutting_down() -> bool {
    CELL.is_shutting_down()
}

/// The host's remaining drain budget, or a finite default when no signal is
/// installed. Re-query per attempt rather than capturing once: the value
/// counts down toward the host's hard exit.
pub fn remaining_budget() -> Duration {
    CELL.remaining_budget()
}

/// Resolves once shutdown is requested (immediately if it already was).
/// Never resolves when no signal is installed — pair with a budget.
pub async fn cancelled() {
    CELL.cancelled().await
}

/// Ask the host to gracefully shut down the pipeline. No-op (with a warning)
/// when no signal is installed.
pub fn request_shutdown() {
    CELL.request_shutdown()
}

/// Adapter wiring the installed signal into the shared retry policy.
///
/// `remaining_budget` is reported only once shutdown has been requested:
/// steady-state retries are governed by cancellation alone, while a drain in
/// progress additionally caps every backoff at the host's remaining time.
pub struct PluginCancelSignal;

impl streamling_retry::CancelSignal for PluginCancelSignal {
    fn is_cancelled(&self) -> bool {
        is_shutting_down()
    }

    fn cancelled(&self) -> streamling_retry::SignalFuture<'_> {
        Box::pin(cancelled())
    }

    fn remaining_budget(&self) -> Option<Duration> {
        is_shutting_down().then(remaining_budget)
    }
}

/// Retry `operation` with exponential backoff until it succeeds or shutdown
/// cancels the loop between attempts. The first attempt always runs — a sink
/// flushing its final batch during a drain must attempt the write, not
/// abandon it. See `streamling_retry::retry_until_cancelled` for the full
/// contract.
pub async fn retry_until_cancelled<Op, Fut, T, E>(
    operation: Op,
    operation_name: &str,
) -> streamling_retry::RetryOutcome<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: streamling_retry::RetryError,
{
    streamling_retry::retry_until_cancelled(operation, operation_name, &PluginCancelSignal).await
}

/// Like [`retry_until_cancelled`], but a non-retriable error returns
/// immediately and giving up surfaces the last error instead of an outcome.
pub async fn retry_retriable_until_cancelled<Op, Fut, T, E>(
    operation: Op,
    operation_name: &str,
) -> Result<T, E>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: streamling_retry::RetryError,
{
    streamling_retry::retry_retriable_until_cancelled(
        operation,
        operation_name,
        &PluginCancelSignal,
    )
    .await
}

/// Spawn a task the drain can wait on, onto the plugin library's runtime.
///
/// Prefer this over a bare `tokio::spawn`: bare tasks are invisible to the
/// [`drain_tracked`] wait at `Terminate`, so buffered work they hold is
/// silently abandoned at exit. Must be called from within the plugin
/// runtime's context (any hook body qualifies), like `tokio::spawn` itself.
///
/// Tracked means *awaited at drain*, not killable: a task that never finishes
/// still only times the drain out. Long-lived tasks should select on
/// [`cancelled`] so a drain can actually complete.
pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    CELL.tracker.spawn(future)
}

/// Wait (bounded) for tracked tasks to finish. Called by the dispatchers on
/// `Terminate`, after the plugin's own `terminate()` ran. Multiple calls are
/// fine — the tracker keeps accepting tasks after `close()`, and each wait
/// completes when the count reaches zero.
pub async fn drain_tracked() {
    let tracker = &CELL.tracker;
    if tracker.is_empty() {
        return;
    }
    tracker.close();
    // Never wait longer than the host has left, and leave headroom under the
    // fallback so an un-signalled drain still exits before a 30s pod grace.
    let budget = remaining_budget().min(Duration::from_secs(10));
    if tokio::time::timeout(budget, tracker.wait()).await.is_err() {
        warn!(
            "{} tracked plugin task(s) still running after the {:?} drain wait; \
             they will be abandoned at process exit",
            tracker.len(),
            budget
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use abi_stable::derive_macro_reexports::TD_Opaque;
    use async_ffi::FutureExt as _;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    /// Host-side stand-in: latched flag + transition-only cancelled future,
    /// the exact shape the real host adapter has.
    #[derive(Clone)]
    struct TestSignal {
        down: Arc<AtomicBool>,
        requested: Arc<AtomicU32>,
    }

    impl ShutdownSignal for TestSignal {
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
            1_500
        }
        fn request_shutdown(&self) {
            self.requested.fetch_add(1, Ordering::SeqCst);
            self.down.store(true, Ordering::SeqCst);
        }
    }

    fn obj(signal: TestSignal) -> ShutdownSignalObj {
        ShutdownSignal_TO::from_value(signal, TD_Opaque)
    }

    /// Uninstalled behaviour — exercised against a fresh cell, NOT the global
    /// (the global is one-way; installing there would contaminate the binary).
    #[tokio::test]
    async fn absent_signal_degrades_to_finite_defaults() {
        let cell = SignalCell::new();
        assert!(!cell.is_shutting_down());
        assert_eq!(cell.remaining_budget(), FALLBACK_BUDGET);
        let waited = tokio::time::timeout(Duration::from_millis(50), cell.cancelled()).await;
        assert!(waited.is_err(), "no signal => cancelled() must park");
        cell.request_shutdown(); // must not panic
    }

    #[tokio::test]
    async fn installed_signal_is_observed_through_the_ffi_object() {
        let cell = SignalCell::new();
        let down = Arc::new(AtomicBool::new(false));
        let requested = Arc::new(AtomicU32::new(0));
        assert!(cell.install(obj(TestSignal {
            down: down.clone(),
            requested: requested.clone(),
        })));

        assert!(!cell.is_shutting_down());
        assert_eq!(cell.remaining_budget(), Duration::from_millis(1_500));

        cell.request_shutdown();
        assert_eq!(requested.load(Ordering::SeqCst), 1);
        assert!(cell.is_shutting_down());

        // Already-latched signal: cancelled() must resolve promptly even
        // though the underlying future only observes transitions.
        tokio::time::timeout(Duration::from_secs(1), cell.cancelled())
            .await
            .expect("latched signal must resolve");
    }

    #[tokio::test]
    async fn second_install_keeps_the_first() {
        let cell = SignalCell::new();
        let first = Arc::new(AtomicBool::new(false));
        assert!(cell.install(obj(TestSignal {
            down: first.clone(),
            requested: Arc::new(AtomicU32::new(0)),
        })));
        assert!(!cell.install(obj(TestSignal {
            down: Arc::new(AtomicBool::new(true)),
            requested: Arc::new(AtomicU32::new(0)),
        })));
        assert!(
            !cell.is_shutting_down(),
            "the second (already-down) signal must have been discarded"
        );
    }
}
