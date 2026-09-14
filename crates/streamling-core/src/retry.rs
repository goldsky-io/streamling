//! Retry helpers for the engine.
//!
//! The retry *policy* — backoff, jitter, the first-attempt-always rule, when to
//! give up — lives in `streamling-retry` so the plugin SDK can share one copy
//! rather than each plugin hand-rolling its own. This module is the engine's
//! adapter over it: it adapts the process shutdown watch to `CancelSignal`.
//! (The `StreamlingError` -> log-classification mapping lives in
//! `streamling-common` alongside the error type, as the orphan rule requires.)
//!
//! Prefer the `_until_cancelled` variants everywhere a shutdown handle is in
//! scope. An uncancellable retry against a sick backend burns the whole drain
//! budget and turns a graceful exit into a hard kill.

use crate::error::Result;
use streamling_retry::{
    CancelSignal, NeverCancelled, WatchSignal, retry_forever, retry_retriable_until_cancelled,
    retry_until_cancelled,
};

/// Outcome of a cancellable retry loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// The operation eventually succeeded.
    Completed,
    /// The shutdown signal fired before the operation succeeded; the loop gave
    /// up between attempts.
    Cancelled,
}

/// Adapts a shutdown watch receiver to the shared crate's cancellation trait.
///
/// Takes `&mut` purely to keep the call signature these helpers have always
/// had; the receiver is cloned internally, so nothing is consumed.
fn signal_from(shutdown: &mut tokio::sync::watch::Receiver<bool>) -> WatchSignal {
    WatchSignal::new(shutdown.clone())
}

/// Retry the provided operation indefinitely with exponential backoff.
#[deprecated(
    note = "uncancellable: burns the drain budget and forces a hard kill. Use retry_forever_with_backoff_until_cancelled."
)]
pub async fn retry_forever_with_backoff_async<Op, Fut>(operation: Op, operation_name: &str)
where
    Op: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<()>>,
{
    retry_forever(operation, operation_name).await
}

/// Like [`retry_forever_with_backoff_async`], but the loop gives up (returns
/// [`RetryOutcome::Cancelled`]) when the shutdown watch flips to `true`.
///
/// The FIRST attempt always runs, even when shutdown has already been
/// requested: graceful drain means in-flight work still gets written — a sink
/// flushing its final batches during shutdown must attempt the write, not
/// abandon it (abandoning it is exactly the tail loss the drain exists to
/// prevent). Cancellation is only checked during the backoff sleeps between
/// attempts, so what shutdown cuts short is the infinite RE-try loop against a
/// sick backend. A single in-flight attempt is allowed to finish (or hit its
/// own I/O timeout); callers relying on prompt cancellation must give each
/// attempt a bounded timeout of its own.
pub async fn retry_forever_with_backoff_until_cancelled<Op, Fut>(
    operation: Op,
    operation_name: &str,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> RetryOutcome
where
    Op: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<()>>,
{
    let signal = signal_from(shutdown);
    match retry_until_cancelled(operation, operation_name, &signal).await {
        streamling_retry::RetryOutcome::Completed(()) => RetryOutcome::Completed,
        // Budget exhaustion cannot arise here (this signal sets no deadline),
        // and either way it is a give-up from the caller's point of view.
        streamling_retry::RetryOutcome::Cancelled(_) => RetryOutcome::Cancelled,
    }
}

/// Retry the provided operation indefinitely with exponential backoff,
/// returning the successful value.
#[deprecated(
    note = "uncancellable: burns the drain budget and forces a hard kill. Use retry_forever_with_backoff_until_cancelled_returning."
)]
pub async fn retry_forever_with_backoff_async_returning<Op, Fut, T>(
    operation: Op,
    operation_name: &str,
) -> T
where
    Op: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<T>>,
{
    retry_forever(operation, operation_name).await
}

/// Like [`retry_forever_with_backoff_until_cancelled`], but returns the
/// successful value, or `None` if shutdown cut the loop short.
///
/// Same first-attempt rule: the first attempt always runs.
pub async fn retry_forever_with_backoff_until_cancelled_returning<Op, Fut, T>(
    operation: Op,
    operation_name: &str,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Option<T>
where
    Op: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<T>>,
{
    let signal = signal_from(shutdown);
    match retry_until_cancelled(operation, operation_name, &signal).await {
        streamling_retry::RetryOutcome::Completed(value) => Some(value),
        streamling_retry::RetryOutcome::Cancelled(_) => None,
    }
}

/// Retry the operation with exponential backoff for as long as the error is
/// retriable.
///
/// - If the operation succeeds, returns `Ok(value)`
/// - If the operation fails with a retriable error, retries with exponential backoff
/// - If the operation fails with a non-retriable error, returns the error immediately
///
/// This is useful for operations where some errors are transient (network issues)
/// while others are permanent (validation errors, configuration errors).
pub async fn retry_if_retriable<Op, Fut, T>(operation: Op, operation_name: &str) -> Result<T>
where
    Op: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<T>>,
{
    retry_retriable_until_cancelled(operation, operation_name, &NeverCancelled).await
}

/// Like [`retry_if_retriable`], but the retry loop gives up between attempts
/// once the shutdown watch flips to `true`, returning the last (retriable)
/// error instead of retrying it forever.
///
/// Same first-attempt rule as [`retry_forever_with_backoff_until_cancelled`]:
/// the first attempt always runs even when shutdown was already requested.
/// Non-retriable errors still fail immediately, exactly as in
/// [`retry_if_retriable`].
pub async fn retry_if_retriable_until_cancelled<Op, Fut, T>(
    operation: Op,
    operation_name: &str,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<T>
where
    Op: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<T>>,
{
    let signal = signal_from(shutdown);
    retry_retriable_until_cancelled(operation, operation_name, &signal).await
}

/// Re-exported so callers can build a bounded signal without depending on the
/// shared crate directly.
pub use streamling_retry::{CancelReason, SignalFuture};

/// Asserts at compile time that the engine's adapter really does satisfy the
/// shared trait (rather than silently going through a different impl).
const _: fn() = || {
    fn assert_signal<S: CancelSignal>() {}
    assert_signal::<WatchSignal>();
    assert_signal::<NeverCancelled>();
};

#[cfg(test)]
mod tests {
    #![allow(deprecated)] // several tests cover the deprecated wrappers on purpose

    use super::*;
    use crate::error::StreamlingError;
    use crate::streamling_err;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn test_immediate_success() {
        // Test that a successful operation on first attempt returns immediately
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let operation = || {
            let count = call_count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };

        retry_forever_with_backoff_async(operation, "test_operation").await;

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_then_success() {
        // Test that operation retries a few times before succeeding
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let operation = || {
            let count = call_count_clone.clone();
            async move {
                let c = count.fetch_add(1, Ordering::SeqCst) + 1;
                if c < 3 {
                    Err(streamling_err!("Attempt {} failed", c))
                } else {
                    Ok(())
                }
            }
        };

        retry_forever_with_backoff_async(operation, "test_operation").await;

        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_returning_immediate_success() {
        // Test that a successful operation on first attempt returns immediately
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let operation = || {
            let count = call_count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        };

        let result = retry_forever_with_backoff_async_returning(operation, "test_operation").await;

        assert_eq!(result, 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_returning_retry_then_success() {
        // Test that operation retries a few times before succeeding
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let operation = || {
            let count = call_count_clone.clone();
            async move {
                let c = count.fetch_add(1, Ordering::SeqCst) + 1;
                if c < 3 {
                    Err(streamling_err!("Attempt {} failed", c))
                } else {
                    Ok("success".to_string())
                }
            }
        };

        let result = retry_forever_with_backoff_async_returning(operation, "test_operation").await;

        assert_eq!(result, "success".to_string());
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_until_cancelled_completes_on_success() {
        let (_tx, mut rx) = tokio::sync::watch::channel(false);
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let operation = || {
            let count = cc.clone();
            async move {
                let c = count.fetch_add(1, Ordering::SeqCst) + 1;
                if c < 3 {
                    Err(streamling_err!("attempt {} failed", c))
                } else {
                    Ok(())
                }
            }
        };
        let outcome =
            retry_forever_with_backoff_until_cancelled(operation, "test_cancel", &mut rx).await;
        assert_eq!(outcome, RetryOutcome::Completed);
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_until_cancelled_stops_on_shutdown() {
        // Operation always fails; flipping the shutdown watch must break the
        // otherwise-infinite retry loop promptly (between attempts).
        let (tx, mut rx) = tokio::sync::watch::channel(false);
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let operation = move || {
            let count = cc.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(streamling_err!("always fails"))
            }
        };

        // Fire shutdown shortly after the loop starts.
        // Test task; not part of any pipeline drain.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = tx.send(true);
        });

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            retry_forever_with_backoff_until_cancelled(operation, "test_cancel", &mut rx),
        )
        .await
        .expect("cancellable retry must return, not hang");
        assert_eq!(outcome, RetryOutcome::Cancelled);
        assert!(call_count.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn test_until_cancelled_first_attempt_runs_even_when_already_shutdown() {
        // Graceful drain: work already in flight when shutdown is requested
        // still gets its first attempt (a sink flushing final batches must
        // write them, not abandon them). A successful first attempt completes.
        let (_tx, mut rx) = tokio::sync::watch::channel(true);
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let operation = move || {
            let count = cc.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };
        let outcome =
            retry_forever_with_backoff_until_cancelled(operation, "test_cancel", &mut rx).await;
        assert_eq!(outcome, RetryOutcome::Completed);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_until_cancelled_no_retries_when_already_shutdown() {
        // A FAILING first attempt is not retried once shutdown is requested:
        // cancellation cuts the re-try loop, not the initial attempt.
        let (_tx, mut rx) = tokio::sync::watch::channel(true);
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let operation = move || {
            let count = cc.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(streamling_err!("always fails"))
            }
        };
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            retry_forever_with_backoff_until_cancelled(operation, "test_cancel", &mut rx),
        )
        .await
        .expect("must return promptly, not keep retrying");
        assert_eq!(outcome, RetryOutcome::Cancelled);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_if_retriable_immediate_success() {
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let operation = || {
            let count = call_count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        };

        let result = retry_if_retriable(operation, "test_operation").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_if_retriable_non_retriable_fails_immediately() {
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let operation = || {
            let count = call_count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<i32, _>(streamling_err!("permanent failure"))
            }
        };

        let result = retry_if_retriable(operation, "test_operation").await;

        assert!(result.is_err());
        // Should only be called once since error is not retriable
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_if_retriable_retries_retriable_errors() {
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let operation = || {
            let count = call_count_clone.clone();
            async move {
                let c = count.fetch_add(1, Ordering::SeqCst) + 1;
                if c < 3 {
                    Err(StreamlingError::retriable(format!(
                        "transient failure {}",
                        c
                    )))
                } else {
                    Ok("recovered")
                }
            }
        };

        let result = retry_if_retriable(operation, "test_operation").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "recovered");
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_until_cancelled_returning_completes_on_success() {
        let (_tx, mut rx) = tokio::sync::watch::channel(false);
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let operation = || {
            let count = cc.clone();
            async move {
                let c = count.fetch_add(1, Ordering::SeqCst) + 1;
                if c < 3 {
                    Err(streamling_err!("attempt {} failed", c))
                } else {
                    Ok(42u32)
                }
            }
        };
        let value = retry_forever_with_backoff_until_cancelled_returning(
            operation,
            "test_cancel_returning",
            &mut rx,
        )
        .await;
        assert_eq!(value, Some(42));
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_until_cancelled_returning_stops_on_shutdown() {
        // Operation always fails; flipping the shutdown watch must break the
        // otherwise-infinite retry loop promptly (between attempts).
        let (tx, mut rx) = tokio::sync::watch::channel(false);
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let operation = move || {
            let count = cc.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<u32, _>(streamling_err!("always fails"))
            }
        };

        // Test task; not part of any pipeline drain.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = tx.send(true);
        });

        let value = tokio::time::timeout(
            Duration::from_secs(5),
            retry_forever_with_backoff_until_cancelled_returning(
                operation,
                "test_cancel_returning",
                &mut rx,
            ),
        )
        .await
        .expect("cancellable returning retry must return, not hang");
        assert_eq!(value, None);
        assert!(call_count.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn test_retry_if_retriable_until_cancelled_surfaces_last_error_on_shutdown() {
        // Shutdown already requested: the FIRST attempt still runs (drain
        // semantics), but a retriable failure is not retried — the last error
        // surfaces to the caller instead of looping forever.
        let (_tx, mut rx) = tokio::sync::watch::channel(true);
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let operation = move || {
            let count = cc.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<u32, _>(StreamlingError::retriable("transient failure".to_string()))
            }
        };

        let err = retry_if_retriable_until_cancelled(operation, "test_cancel", &mut rx)
            .await
            .expect_err("must surface the last retriable error");
        assert!(err.to_string().contains("transient failure"), "{err}");
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_if_retriable_until_cancelled_stops_promptly_mid_backoff() {
        let (tx, mut rx) = tokio::sync::watch::channel(false);
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let operation = move || {
            let count = cc.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<u32, _>(StreamlingError::retriable("transient failure".to_string()))
            }
        };

        // Test task; not part of any pipeline drain.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = tx.send(true);
        });

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            retry_if_retriable_until_cancelled(operation, "test_cancel", &mut rx),
        )
        .await
        .expect("cancellable retry must return, not hang");
        assert!(result.is_err());
        assert!(call_count.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn test_retry_if_retriable_until_cancelled_nonretriable_fails_fast() {
        // Non-retriable errors keep failing immediately, shutdown or not.
        let (_tx, mut rx) = tokio::sync::watch::channel(false);
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = call_count.clone();
        let operation = move || {
            let count = cc.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err::<u32, _>(streamling_err!("permanent failure"))
            }
        };

        let err = retry_if_retriable_until_cancelled(operation, "test_cancel", &mut rx)
            .await
            .expect_err("non-retriable error must fail immediately");
        assert!(err.to_string().contains("permanent failure"), "{err}");
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}
