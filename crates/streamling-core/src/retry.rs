use crate::error::Result;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, warn};

const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;

/// Retry the provided operation indefinitely with exponential backoff and small jitter.
/// The operation should return `Ok(())` on success and `Err` with a StreamlingError on failure.
pub async fn retry_forever_with_backoff_async<Op, Fut>(mut operation: Op, operation_name: &str)
where
    Op: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<()>>,
{
    let mut attempt: u32 = 0;
    let mut backoff_ms: u64 = INITIAL_BACKOFF_MS;

    loop {
        attempt = attempt.saturating_add(1);
        match operation().await {
            Ok(()) => {
                if attempt > 1 {
                    warn!("{} recovered after {} attempts", operation_name, attempt);
                }
                break;
            }
            Err(err) => {
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

        let jitter = (attempt as u64 % 100) * 7; // small deterministic jitter
        let sleep_ms = std::cmp::min(MAX_BACKOFF_MS, backoff_ms + jitter);
        sleep(Duration::from_millis(sleep_ms)).await;
        backoff_ms = std::cmp::min(backoff_ms.saturating_mul(2), MAX_BACKOFF_MS);
    }
}

/// Outcome of a cancellable retry loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryOutcome {
    /// The operation eventually succeeded.
    Completed,
    /// The shutdown signal fired before the operation succeeded; the loop gave
    /// up between attempts.
    Cancelled,
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
    mut operation: Op,
    operation_name: &str,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> RetryOutcome
where
    Op: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<()>>,
{
    let mut attempt: u32 = 0;
    let mut backoff_ms: u64 = INITIAL_BACKOFF_MS;

    loop {
        attempt = attempt.saturating_add(1);
        match operation().await {
            Ok(()) => {
                if attempt > 1 {
                    warn!("{} recovered after {} attempts", operation_name, attempt);
                }
                return RetryOutcome::Completed;
            }
            Err(err) => {
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

        // Between attempts: give up if shutdown has been requested. The
        // borrow check covers a watch that flipped before we got here (its
        // `changed()` would never fire again).
        if *shutdown.borrow() {
            warn!(
                "{} cancelled by shutdown after {} attempt(s)",
                operation_name, attempt
            );
            return RetryOutcome::Cancelled;
        }
        let jitter = (attempt as u64 % 100) * 7; // small deterministic jitter
        let sleep_ms = std::cmp::min(MAX_BACKOFF_MS, backoff_ms + jitter);
        tokio::select! {
            _ = sleep(Duration::from_millis(sleep_ms)) => {}
            res = shutdown.changed() => {
                if res.is_ok() && *shutdown.borrow() {
                    warn!(
                        "{} cancelled by shutdown after {} attempt(s)",
                        operation_name, attempt
                    );
                    return RetryOutcome::Cancelled;
                }
            }
        }
        backoff_ms = std::cmp::min(backoff_ms.saturating_mul(2), MAX_BACKOFF_MS);
    }
}

/// Retry the provided operation indefinitely with exponential backoff and small jitter.
/// Returns the successful value once the operation succeeds.
/// The operation should return `Ok(T)` on success and `Err` with a StreamlingError on failure.
pub async fn retry_forever_with_backoff_async_returning<Op, Fut, T>(
    mut operation: Op,
    operation_name: &str,
) -> T
where
    Op: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<T>>,
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
                return value;
            }
            Err(err) => {
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

        let jitter = (attempt as u64 % 100) * 7; // small deterministic jitter
        let sleep_ms = std::cmp::min(MAX_BACKOFF_MS, backoff_ms + jitter);
        sleep(Duration::from_millis(sleep_ms)).await;
        backoff_ms = std::cmp::min(backoff_ms.saturating_mul(2), MAX_BACKOFF_MS);
    }
}

/// Retry the operation only if errors are marked as retriable.
///
/// - If the operation succeeds, returns `Ok(value)`
/// - If the operation fails with a retriable error, retries with exponential backoff
/// - If the operation fails with a non-retriable error, returns the error immediately
///
/// This is useful for operations where some errors are transient (network issues)
/// while others are permanent (validation errors, configuration errors).
pub async fn retry_if_retriable<Op, Fut, T>(mut operation: Op, operation_name: &str) -> Result<T>
where
    Op: FnMut() -> Fut,
    Fut: core::future::Future<Output = Result<T>>,
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
                return Ok(value);
            }
            Err(err) => {
                if !err.is_retriable() {
                    // Non-retriable error: fail immediately
                    return Err(err);
                }

                // Retriable error: log and retry
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
            }
        }

        let jitter = (attempt as u64 % 100) * 7;
        let sleep_ms = std::cmp::min(MAX_BACKOFF_MS, backoff_ms + jitter);
        sleep(Duration::from_millis(sleep_ms)).await;
        backoff_ms = std::cmp::min(backoff_ms.saturating_mul(2), MAX_BACKOFF_MS);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StreamlingError;
    use crate::streamling_err;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

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

    #[test]
    fn test_backoff_calculation_logic() {
        // Test that backoff doubles each time and caps at MAX_BACKOFF_MS
        let mut backoff = INITIAL_BACKOFF_MS;
        assert_eq!(backoff, 100);

        backoff = std::cmp::min(backoff.saturating_mul(2), MAX_BACKOFF_MS);
        assert_eq!(backoff, 200);

        backoff = std::cmp::min(backoff.saturating_mul(2), MAX_BACKOFF_MS);
        assert_eq!(backoff, 400);

        backoff = std::cmp::min(backoff.saturating_mul(2), MAX_BACKOFF_MS);
        assert_eq!(backoff, 800);

        // Continue until we hit the cap
        for _ in 0..10 {
            backoff = std::cmp::min(backoff.saturating_mul(2), MAX_BACKOFF_MS);
        }
        assert_eq!(backoff, MAX_BACKOFF_MS);

        // Verify it stays at the cap
        backoff = std::cmp::min(backoff.saturating_mul(2), MAX_BACKOFF_MS);
        assert_eq!(backoff, MAX_BACKOFF_MS);
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
}
