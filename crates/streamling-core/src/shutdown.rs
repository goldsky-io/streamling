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

// NOTE: deliberately no unit test calls `request_shutdown()` here. The signal
// is process-global and one-way — flipping it in a test would permanently
// contaminate every later test in the same binary that exercises production
// code paths which subscribe to it (e.g. the Postgres/ClickHouse sink retry
// loops). The end-to-end behaviour is covered by the SIGTERM drain e2e test
// (`crates/streamling-e2e/tests/shutdown_drain.rs`), which runs the signal
// path in a dedicated child process.
