//! Cancellable retry policy, shared by the engine and the plugin SDK.
//!
//! # Why this is its own crate
//!
//! Plugins are compiled as separate dynamic libraries and loaded over an
//! `abi_stable` FFI boundary. Two things therefore cannot be shared with them:
//!
//! - **Tokio types.** A `watch::Receiver` has no stable ABI, so a cancellation
//!   handle cannot simply be passed across the boundary.
//! - **Statics.** The loaded library gets its own copy of every static, so a
//!   process-global shutdown flag read inside a plugin is a *different* flag
//!   from the host's. Code doing that compiles and then silently never fires.
//!
//! What *can* be shared is the policy itself — pure logic with no state. This
//! crate holds it once, generic over where cancellation comes from
//! ([`CancelSignal`]) and over the error type being retried ([`RetryError`]).
//! The engine adapts its shutdown watch to `CancelSignal`; the plugin SDK
//! adapts the FFI-safe signal object it is handed at load time. One policy, two
//! adapters, and no plugin ends up hand-rolling a third copy.
//!
//! Keep the dependency list here to tokio and tracing. Pulling in anything
//! heavier defeats the purpose, because the plugin dylib pays for it.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tracing::{error, warn};

const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;

/// A future returned by [`CancelSignal`] methods.
///
/// Boxed rather than generic: these are only awaited around a backoff sleep of
/// 100ms or more, so an allocation per attempt is irrelevant, and boxing keeps
/// `CancelSignal` usable as `dyn` across the FFI adapter.
pub type SignalFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Where a retry loop learns that it should stop trying.
///
/// Implementors are latches: once cancelled, always cancelled. The retry loop
/// relies on that — it checks [`is_cancelled`](CancelSignal::is_cancelled)
/// before awaiting [`cancelled`](CancelSignal::cancelled), which would
/// otherwise miss a signal that fired earlier.
pub trait CancelSignal {
    /// Whether cancellation has already been requested.
    fn is_cancelled(&self) -> bool;

    /// Resolves once cancellation is requested.
    ///
    /// Must never resolve while [`is_cancelled`](Self::is_cancelled) is
    /// `false`; a future that resolves spuriously turns the backoff into a
    /// tight loop.
    fn cancelled(&self) -> SignalFuture<'_>;

    /// How much wall-clock time the caller still has, if the host imposes a
    /// deadline. `None` means unbounded.
    ///
    /// Queried fresh on every attempt rather than captured once, so the answer
    /// stays the host's rather than the component's. That is deliberate: a
    /// component that picks its own budget can pick one larger than the
    /// process has left, at which point its budget is decorative and the real
    /// outcome is a hard kill.
    fn remaining_budget(&self) -> Option<Duration> {
        None
    }

    /// Sleeps for `duration`.
    ///
    /// Overridable because a plugin is not necessarily running on a runtime
    /// with a timer it may touch directly; the SDK adapter routes this through
    /// the host-provided runtime instead.
    fn sleep(&self, duration: Duration) -> SignalFuture<'_> {
        Box::pin(tokio::time::sleep(duration))
    }
}

/// Log classification for a retried error.
///
/// Defaults suit a generic fallible operation: not internal, worth retrying.
/// Error types that can distinguish these should override, so the retry logs
/// carry the same structured fields as the rest of the pipeline.
pub trait RetryError: std::fmt::Debug {
    fn is_internal(&self) -> bool {
        false
    }
    fn is_retriable(&self) -> bool {
        true
    }
}

/// Why a retry loop stopped without succeeding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// Shutdown was requested.
    ShutdownRequested,
    /// The wall-clock budget the host granted ran out.
    BudgetExhausted,
}

/// Outcome of a cancellable retry loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome<T> {
    /// The operation eventually succeeded.
    Completed(T),
    /// The loop gave up between attempts.
    Cancelled(CancelReason),
}

/// Retry `operation` with exponential backoff until it succeeds or the loop is
/// cancelled between attempts.
///
/// **The first attempt always runs**, even when cancellation has already been
/// requested. Graceful drain means in-flight work still gets written: a sink
/// flushing its final batch during shutdown must attempt the write, not
/// abandon it — abandoning it is exactly the tail loss the drain exists to
/// prevent. What cancellation cuts short is the unbounded *re*-try loop against
/// a sick backend.
///
/// A single in-flight attempt is allowed to finish. Cancellation is only
/// observed during the backoff sleeps, so callers that need prompt cancellation
/// must give each attempt a bounded timeout of its own. This matters more than
/// it sounds: an attempt blocked forever inside a network call with no timeout
/// cannot be interrupted here, and remains a hang that only a process-level
/// watchdog resolves.
pub async fn retry_until_cancelled<Op, Fut, T, E, S>(
    mut operation: Op,
    operation_name: &str,
    signal: &S,
) -> RetryOutcome<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: RetryError,
    S: CancelSignal + ?Sized,
{
    let mut attempt: u32 = 0;
    let mut backoff_ms: u64 = INITIAL_BACKOFF_MS;

    loop {
        attempt = attempt.saturating_add(1);
        match operation().await {
            Ok(value) => {
                if attempt > 1 {
                    warn!("{} recovered after {} attempts", operation_name, attempt);
                }
                return RetryOutcome::Completed(value);
            }
            Err(err) => {
                // Escalate once this stops looking transient.
                if attempt > 5 {
                    error!(
                        error.internal = err.is_internal(),
                        error.retriable = err.is_retriable(),
                        "{} failed (attempt {}):\n{:?}\nRetrying...",
                        operation_name,
                        attempt,
                        err
                    );
                } else {
                    warn!(
                        error.internal = err.is_internal(),
                        error.retriable = err.is_retriable(),
                        "{} failed (attempt {}):\n{:?}\nRetrying...",
                        operation_name,
                        attempt,
                        err
                    );
                }
            }
        }

        // Checked before awaiting `cancelled()`, which may only resolve on a
        // future transition and would otherwise miss a signal already set.
        if signal.is_cancelled() {
            warn!(
                "{} cancelled by shutdown after {} attempt(s)",
                operation_name, attempt
            );
            return RetryOutcome::Cancelled(CancelReason::ShutdownRequested);
        }

        let jitter = (attempt as u64 % 100) * 7; // small deterministic jitter
        let mut sleep_ms = std::cmp::min(MAX_BACKOFF_MS, backoff_ms + jitter);

        // Never sleep past the host's deadline: waking after it has expired
        // wastes the remainder of the drain on an attempt that cannot land.
        if let Some(remaining) = signal.remaining_budget() {
            if remaining.is_zero() {
                warn!(
                    "{} abandoned after {} attempt(s): shutdown budget exhausted",
                    operation_name, attempt
                );
                return RetryOutcome::Cancelled(CancelReason::BudgetExhausted);
            }
            let remaining_ms = u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX);
            sleep_ms = std::cmp::min(sleep_ms, remaining_ms);
        }

        tokio::select! {
            _ = signal.sleep(Duration::from_millis(sleep_ms)) => {}
            _ = signal.cancelled() => {
                warn!(
                    "{} cancelled by shutdown after {} attempt(s)",
                    operation_name, attempt
                );
                return RetryOutcome::Cancelled(CancelReason::ShutdownRequested);
            }
        }
        backoff_ms = std::cmp::min(backoff_ms.saturating_mul(2), MAX_BACKOFF_MS);
    }
}

/// Retry only while the error says it is worth retrying.
///
/// Differs from [`retry_until_cancelled`] in two ways: a non-retriable error
/// returns immediately, and giving up surfaces the **last error** rather than an
/// outcome enum — so the caller sees why it stopped. Cancellation and budget
/// exhaustion both surface that last error.
///
/// The first-attempt rule from [`retry_until_cancelled`] applies here too.
///
/// Note the ordering: cancellation is checked *before* logging the failure, so a
/// drain that gives up immediately reports the cancellation rather than also
/// emitting a scary retry warning for an attempt it never intended to repeat.
pub async fn retry_retriable_until_cancelled<Op, Fut, T, E, S>(
    mut operation: Op,
    operation_name: &str,
    signal: &S,
) -> Result<T, E>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: RetryError,
    S: CancelSignal + ?Sized,
{
    let mut attempt: u32 = 0;
    let mut backoff_ms: u64 = INITIAL_BACKOFF_MS;

    loop {
        attempt = attempt.saturating_add(1);
        let err = match operation().await {
            Ok(value) => {
                if attempt > 1 {
                    warn!("{} recovered after {} attempts", operation_name, attempt);
                }
                return Ok(value);
            }
            Err(err) if !err.is_retriable() => return Err(err),
            Err(err) => err,
        };

        if signal.is_cancelled() {
            warn!(
                "{} cancelled by shutdown after {} attempt(s); surfacing the last error",
                operation_name, attempt
            );
            return Err(err);
        }

        if attempt > 5 {
            error!(
                error.internal = err.is_internal(),
                error.retriable = err.is_retriable(),
                "{} failed (attempt {}, retriable):\n{:?}\nRetrying...",
                operation_name,
                attempt,
                err
            );
        } else {
            warn!(
                error.internal = err.is_internal(),
                error.retriable = err.is_retriable(),
                "{} failed (attempt {}, retriable):\n{:?}\nRetrying...",
                operation_name,
                attempt,
                err
            );
        }

        let jitter = (attempt as u64 % 100) * 7;
        let mut sleep_ms = std::cmp::min(MAX_BACKOFF_MS, backoff_ms + jitter);

        if let Some(remaining) = signal.remaining_budget() {
            if remaining.is_zero() {
                warn!(
                    "{} abandoned after {} attempt(s): shutdown budget exhausted",
                    operation_name, attempt
                );
                return Err(err);
            }
            let remaining_ms = u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX);
            sleep_ms = std::cmp::min(sleep_ms, remaining_ms);
        }

        tokio::select! {
            _ = signal.sleep(Duration::from_millis(sleep_ms)) => {}
            _ = signal.cancelled() => {
                warn!(
                    "{} cancelled by shutdown after {} attempt(s); surfacing the last error",
                    operation_name, attempt
                );
                return Err(err);
            }
        }
        backoff_ms = std::cmp::min(backoff_ms.saturating_mul(2), MAX_BACKOFF_MS);
    }
}

/// Retry until success, with nothing able to cancel the loop.
///
/// Only for call sites with genuinely no shutdown handle in scope. Prefer
/// [`retry_until_cancelled`]: an uncancellable loop against a sick backend is
/// the hang this crate exists to bound, and it will burn the whole drain budget
/// and take a hard process kill instead of a graceful exit.
pub async fn retry_forever<Op, Fut, T, E>(operation: Op, operation_name: &str) -> T
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: RetryError,
{
    match retry_until_cancelled(operation, operation_name, &NeverCancelled).await {
        RetryOutcome::Completed(value) => value,
        // Unreachable: `NeverCancelled` never cancels and sets no budget. Park
        // rather than panic, so a future change to that type degrades into a
        // stalled task the drain can name instead of killing the process.
        RetryOutcome::Cancelled(_) => std::future::pending().await,
    }
}

/// A [`CancelSignal`] that never fires and imposes no budget.
///
/// For call sites with genuinely nothing to cancel against — startup paths that
/// run before a shutdown handle exists, and tests. Not a default: reaching for
/// this in a component that *does* have a signal available reintroduces the
/// unbounded retry this crate exists to bound.
#[derive(Debug, Clone, Copy, Default)]
pub struct NeverCancelled;

impl CancelSignal for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
    fn cancelled(&self) -> SignalFuture<'_> {
        Box::pin(std::future::pending())
    }
}

/// Adapts a `tokio::sync::watch` receiver carrying "shutdown requested".
///
/// Clones the receiver per wait rather than holding `&mut`, so the signal can
/// be shared behind `&self` like every other implementor.
#[derive(Debug, Clone)]
pub struct WatchSignal {
    rx: tokio::sync::watch::Receiver<bool>,
    // The deadline itself, not a Duration snapshot: remaining_budget() must
    // shrink as time passes, or the sleep cap and the budget-exhaustion check
    // both work from the value frozen at construction.
    deadline: Option<tokio::time::Instant>,
}

impl WatchSignal {
    pub fn new(rx: tokio::sync::watch::Receiver<bool>) -> Self {
        Self { rx, deadline: None }
    }

    /// Adds a wall-clock ceiling, re-evaluated against `deadline` on each
    /// attempt.
    pub fn with_deadline(mut self, deadline: tokio::time::Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }
}

impl CancelSignal for WatchSignal {
    fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    fn cancelled(&self) -> SignalFuture<'_> {
        let mut rx = self.rx.clone();
        Box::pin(async move {
            loop {
                if *rx.borrow_and_update() {
                    return;
                }
                if rx.changed().await.is_err() {
                    // Sender gone: nothing will ever set this. Park instead of
                    // returning, so the caller's select does not spin the
                    // backoff into a tight loop.
                    std::future::pending::<()>().await;
                }
            }
        })
    }

    fn remaining_budget(&self) -> Option<Duration> {
        self.deadline
            .map(|at| at.saturating_duration_since(tokio::time::Instant::now()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Debug)]
    struct TestError;
    impl RetryError for TestError {}

    fn watch(value: bool) -> (tokio::sync::watch::Sender<bool>, WatchSignal) {
        let (tx, rx) = tokio::sync::watch::channel(value);
        (tx, WatchSignal::new(rx))
    }

    /// The doubling schedule and its ceiling, which every caller's pacing
    /// depends on.
    #[test]
    fn backoff_doubles_and_caps() {
        let mut backoff = INITIAL_BACKOFF_MS;
        assert_eq!(backoff, 100);

        for expected in [200, 400, 800] {
            backoff = std::cmp::min(backoff.saturating_mul(2), MAX_BACKOFF_MS);
            assert_eq!(backoff, expected);
        }

        for _ in 0..10 {
            backoff = std::cmp::min(backoff.saturating_mul(2), MAX_BACKOFF_MS);
        }
        assert_eq!(backoff, MAX_BACKOFF_MS, "must reach the ceiling");

        backoff = std::cmp::min(backoff.saturating_mul(2), MAX_BACKOFF_MS);
        assert_eq!(backoff, MAX_BACKOFF_MS, "and stay there");
    }

    #[tokio::test(start_paused = true)]
    async fn succeeds_without_retrying() {
        let (_tx, signal) = watch(false);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let outcome = retry_until_cancelled(
            || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, TestError>(7)
                }
            },
            "op",
            &signal,
        )
        .await;
        assert_eq!(outcome, RetryOutcome::Completed(7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_then_succeeds() {
        let (_tx, signal) = watch(false);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let outcome = retry_until_cancelled(
            || {
                let c = c.clone();
                async move {
                    if c.fetch_add(1, Ordering::SeqCst) < 3 {
                        Err(TestError)
                    } else {
                        Ok(())
                    }
                }
            },
            "op",
            &signal,
        )
        .await;
        assert_eq!(outcome, RetryOutcome::Completed(()));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    /// The contract that makes graceful drain work: a sink flushing its last
    /// batch must still attempt the write even though shutdown already fired.
    #[tokio::test(start_paused = true)]
    async fn first_attempt_runs_even_when_already_cancelled() {
        let (_tx, signal) = watch(true);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let outcome = retry_until_cancelled(
            || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, TestError>(())
                }
            },
            "op",
            &signal,
        )
        .await;
        assert_eq!(outcome, RetryOutcome::Completed(()));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the in-flight attempt must not be abandoned"
        );
    }

    /// Already-cancelled plus a failing operation: exactly one attempt, then
    /// give up. Guards the borrow-check-before-await ordering — awaiting
    /// `cancelled()` first would hang here forever, because for a latch that
    /// already fired there is no further transition to observe.
    #[tokio::test(start_paused = true)]
    async fn cancels_after_one_attempt_when_already_cancelled() {
        let (_tx, signal) = watch(true);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let outcome = retry_until_cancelled(
            || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(TestError)
                }
            },
            "op",
            &signal,
        )
        .await;
        assert_eq!(
            outcome,
            RetryOutcome::Cancelled(CancelReason::ShutdownRequested)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// The pre-check earns its keep only against a signal whose `cancelled()`
    /// observes *transitions* rather than latched state — for such an impl a
    /// latch that fired before the future was created is never reported, so
    /// awaiting it first would hang forever. `WatchSignal` handles the latched
    /// case internally and so cannot exercise this; the FFI adapter is exactly
    /// the shape that can get it wrong, hence this stand-in.
    #[tokio::test(start_paused = true)]
    async fn cancelled_state_is_seen_even_if_the_future_never_resolves() {
        struct TransitionOnly;
        impl CancelSignal for TransitionOnly {
            fn is_cancelled(&self) -> bool {
                true
            }
            fn cancelled(&self) -> SignalFuture<'_> {
                // Fired before we were asked; nothing further to observe.
                Box::pin(std::future::pending())
            }
        }
        let outcome = tokio::time::timeout(
            Duration::from_secs(60),
            retry_until_cancelled(|| async { Err::<(), _>(TestError) }, "op", &TransitionOnly),
        )
        .await
        .expect("must not hang waiting on a signal that already fired");
        assert_eq!(
            outcome,
            RetryOutcome::Cancelled(CancelReason::ShutdownRequested)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancels_during_backoff() {
        let (tx, signal) = watch(false);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let task = tokio::spawn(async move {
            retry_until_cancelled(
                || {
                    let c = c.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>(TestError)
                    }
                },
                "op",
                &signal,
            )
            .await
        });
        // Let the first attempt fail and the loop park in its backoff sleep.
        tokio::time::sleep(Duration::from_millis(10)).await;
        tx.send(true).expect("receiver alive");
        let outcome = task.await.expect("join");
        assert_eq!(
            outcome,
            RetryOutcome::Cancelled(CancelReason::ShutdownRequested)
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "cancellation during backoff must not start another attempt"
        );
    }

    /// A budget already at zero stops the loop even though shutdown was never
    /// requested — the case where a component's own retry ceiling exceeds what
    /// the process has left.
    #[tokio::test(start_paused = true)]
    async fn exhausted_budget_stops_the_loop() {
        struct ZeroBudget;
        impl CancelSignal for ZeroBudget {
            fn is_cancelled(&self) -> bool {
                false
            }
            fn cancelled(&self) -> SignalFuture<'_> {
                Box::pin(std::future::pending())
            }
            fn remaining_budget(&self) -> Option<Duration> {
                Some(Duration::ZERO)
            }
        }
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let outcome = retry_until_cancelled(
            || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(TestError)
                }
            },
            "op",
            &ZeroBudget,
        )
        .await;
        assert_eq!(
            outcome,
            RetryOutcome::Cancelled(CancelReason::BudgetExhausted)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[derive(Debug)]
    struct FatalError;
    impl RetryError for FatalError {
        fn is_retriable(&self) -> bool {
            false
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retriable_family_fails_fast_on_non_retriable() {
        let (_tx, signal) = watch(false);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let err = retry_retriable_until_cancelled(
            || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(FatalError)
                }
            },
            "op",
            &signal,
        )
        .await
        .expect_err("non-retriable must not be retried");
        let _ = err;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// Giving up must hand back the real error, not a synthesized one — that is
    /// what lets the caller log why the flush was abandoned.
    #[tokio::test(start_paused = true)]
    async fn retriable_family_surfaces_last_error_when_cancelled() {
        let (_tx, signal) = watch(true);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let err = retry_retriable_until_cancelled(
            || {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(TestError)
                }
            },
            "op",
            &signal,
        )
        .await
        .expect_err("must surface the last retriable error");
        assert!(format!("{err:?}").contains("TestError"));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "first attempt runs, then gives up"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retriable_family_succeeds_after_retries() {
        let (_tx, signal) = watch(false);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let got = retry_retriable_until_cancelled(
            || {
                let c = c.clone();
                async move {
                    if c.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err(TestError)
                    } else {
                        Ok(42)
                    }
                }
            },
            "op",
            &signal,
        )
        .await
        .expect("must eventually succeed");
        assert_eq!(got, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_forever_returns_the_value() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let got = retry_forever(
            || {
                let c = c.clone();
                async move {
                    if c.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err(TestError)
                    } else {
                        Ok::<_, TestError>(9)
                    }
                }
            },
            "op",
        )
        .await;
        assert_eq!(got, 9);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    /// A dropped sender must park rather than resolve, otherwise the backoff
    /// degenerates into a tight retry loop.
    #[tokio::test(start_paused = true)]
    async fn dropped_sender_does_not_resolve_cancellation() {
        let (tx, signal) = watch(false);
        drop(tx);
        assert!(!signal.is_cancelled());
        let waited = tokio::time::timeout(Duration::from_secs(60), signal.cancelled()).await;
        assert!(
            waited.is_err(),
            "cancelled() must not resolve without a signal"
        );
    }

    /// remaining_budget() must count down as time passes, not return the
    /// duration snapshotted when with_deadline() was called.
    #[tokio::test(start_paused = true)]
    async fn watch_signal_budget_decays_toward_deadline() {
        let (_tx, signal) = watch(false);
        let signal =
            signal.with_deadline(tokio::time::Instant::now() + Duration::from_secs(10));
        assert_eq!(signal.remaining_budget(), Some(Duration::from_secs(10)));
        tokio::time::advance(Duration::from_secs(7)).await;
        assert_eq!(signal.remaining_budget(), Some(Duration::from_secs(3)));
        tokio::time::advance(Duration::from_secs(7)).await;
        assert_eq!(signal.remaining_budget(), Some(Duration::ZERO));
    }
}
