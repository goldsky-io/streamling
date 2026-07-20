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

/// The total time budget for graceful shutdown, from
/// `STREAMLING__SHUTDOWN_BUDGET_SECS` (default 25s, sized to sit under the
/// k8s default 30s grace period). The single source of truth for every
/// consumer — the run loop's watchdog and any component slicing its own
/// bounded wait from the budget — so the values can never drift.
pub fn shutdown_budget() -> std::time::Duration {
    const DEFAULT_SHUTDOWN_BUDGET_SECS: u64 = 25;
    let secs = std::env::var("STREAMLING__SHUTDOWN_BUDGET_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|s| *s > 0)
        .unwrap_or(DEFAULT_SHUTDOWN_BUDGET_SECS);
    std::time::Duration::from_secs(secs)
}

// NOTE: deliberately no unit test calls `request_shutdown()` here. The signal
// is process-global and one-way — flipping it in a test would permanently
// contaminate every later test in the same binary that exercises production
// code paths which subscribe to it (e.g. the Postgres/ClickHouse sink retry
// loops). The end-to-end behaviour is covered by the SIGTERM drain e2e test
// (`crates/streamling-e2e/tests/shutdown_drain.rs`), which runs the signal
// path in a dedicated child process.
