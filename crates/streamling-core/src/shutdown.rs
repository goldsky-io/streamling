//! Process-wide graceful-shutdown signal.
//!
//! One `watch` channel for the whole process: the pipeline run loop's single
//! SIGTERM/SIGINT handler (or a clean job-mode completion) flips it, and every
//! component that can wait or retry indefinitely — sources, sink retry loops,
//! helper tasks — observes it and winds down instead of pinning the drain.
//!
//! Being global (like the checkpoint channel registry) means deep call sites
//! such as the sink retry helpers can subscribe without threading a receiver
//! through every constructor. The signal is one-way: once requested, shutdown
//! is never un-requested.

use once_cell::sync::Lazy;
use tokio::sync::watch;

static SHUTDOWN: Lazy<watch::Sender<bool>> = Lazy::new(|| watch::channel(false).0);

/// Request process-wide graceful shutdown. Idempotent.
pub fn request_shutdown() {
    let _ = SHUTDOWN.send(true);
}

/// Subscribe to the shutdown signal. The receiver observes the current value
/// immediately (`*rx.borrow()`) and wakes on `changed()` when it flips.
pub fn subscribe() -> watch::Receiver<bool> {
    SHUTDOWN.subscribe()
}

/// Whether shutdown has been requested.
pub fn is_requested() -> bool {
    *SHUTDOWN.subscribe().borrow()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscribe_observes_request() {
        // NOTE: the channel is process-global, so this test only asserts the
        // one-way transition (false -> true); it cannot assert the initial
        // state without racing other tests in the binary.
        let mut rx = subscribe();
        request_shutdown();
        assert!(is_requested());
        // A subscriber created before the request wakes and sees true.
        if !*rx.borrow() {
            rx.changed().await.expect("sender is static, never dropped");
        }
        assert!(*rx.borrow());
    }
}
