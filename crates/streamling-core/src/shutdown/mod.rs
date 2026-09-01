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

pub mod controller;
pub use controller::{ComponentScope, DrainStage, ShutdownController};
// Re-exported so connector crates can name the token type in ported
// signatures without taking their own tokio-util dependency.
pub use tokio_util::sync::CancellationToken;

static SHUTDOWN: Lazy<watch::Sender<bool>> = Lazy::new(|| watch::channel(false).0);

/// When shutdown was first requested. Drives [`remaining_budget`], so bounded
/// waits taken late in the drain shrink by however much has already elapsed
/// instead of each restarting the full budget.
static REQUESTED_AT: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

/// Request process-wide graceful shutdown. Idempotent.
pub fn request_shutdown() {
    let _ = REQUESTED_AT.set(std::time::Instant::now());
    let _ = SHUTDOWN.send(true);
}

/// How much of the shutdown budget is left: the full budget until shutdown is
/// requested, then budget minus elapsed, saturating at zero. This mirrors the
/// watchdog's own clock (armed from the same request), so a component pacing a
/// wait by this value never outlives the hard exit.
pub fn remaining_budget() -> std::time::Duration {
    match REQUESTED_AT.get() {
        None => shutdown_budget(),
        Some(at) => shutdown_budget().saturating_sub(at.elapsed()),
    }
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
    use std::sync::OnceLock;
    static BUDGET: OnceLock<std::time::Duration> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        const DEFAULT_SHUTDOWN_BUDGET_SECS: u64 = 25;
        let secs = std::env::var("STREAMLING__SHUTDOWN_BUDGET_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|s| *s > 0)
            .unwrap_or(DEFAULT_SHUTDOWN_BUDGET_SECS);
        std::time::Duration::from_secs(secs)
    })
}

/// The bound on the plugin dispatcher drain during teardown (Terminate sends
/// plus awaiting the dispatchers' final flush), from
/// `STREAMLING__PLUGIN_DRAIN_BUDGET_SECS` (default 60s). Team decision on
/// Q2: external plugins' flush weight isn't
/// knowable from code, so the bound is a global, operator-tunable value
/// rather than per-plugin numbers. Call sites must additionally cap it by
/// the watchdog's remaining budget — with the default 25s overall budget the
/// remaining time is the effective bound, while long-grace deployments (the
/// agent's 110s pods) get a real 60s plugin phase with headroom left for the
/// post-plugin drain and coordinator stop.
pub fn plugin_drain_budget() -> std::time::Duration {
    use std::sync::OnceLock;
    static BUDGET: OnceLock<std::time::Duration> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        const DEFAULT_PLUGIN_DRAIN_BUDGET_SECS: u64 = 60;
        let secs = std::env::var("STREAMLING__PLUGIN_DRAIN_BUDGET_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|s| *s > 0)
            .unwrap_or(DEFAULT_PLUGIN_DRAIN_BUDGET_SECS);
        std::time::Duration::from_secs(secs)
    })
}

/// The bound on how long a completing source waits for the terminal checkpoint
/// to finalize. On timeout the terminal Finalizer is SKIPPED (never emitted
/// for an unconfirmed epoch); this bound only keeps a sink that never acks
/// from hanging the source task.
///
/// Derived from the run loop's shared shutdown budget ([`shutdown_budget`],
/// the same value the watchdog is armed with): `min(budget − 10, budget / 2)`,
/// so the wait always expires BEFORE the watchdog hard-exits the process AND
/// never eats more than half the budget. The half-budget cap matters at wide
/// grace: without it a sink that never acks (already failed, endpoint dead)
/// pins the drain for nearly the whole budget — e.g. 100s of a 110s budget —
/// leaving almost nothing for the rest of the drain ladder. Half the budget is
/// still generous for a healthy straggler (a real terminal ack lands in
/// sub-second time) while halving the worst case for a dead one. The
/// minimum-floor is itself capped at budget − 2s so a deliberately tiny budget
/// can never invert the invariant: for budgets ≤ 2s the timeout collapses to
/// zero, which expires immediately and takes the safe branch (Finalizer
/// skipped).
///
/// Example values: budget 25s (default) → 12s wait (half, floored down);
/// budget 60s → 30s; budget 110s (the agent's default for 120s-grace pods) →
/// 55s; budget 20s → 10s; budget 8s → 5s (the floor); budget 4s → 2s (floor
/// capped at budget − 2); budget 2s → 0s (expires immediately, Finalizer
/// skipped).
///
/// Shared by every source that emits a terminal checkpoint (hybrid bounded
/// completion, Kafka streaming shutdown) so their waits can never drift apart.
pub fn terminal_checkpoint_finalize_timeout() -> std::time::Duration {
    const MARGIN_SECS: u64 = 10;
    const MIN_TIMEOUT_SECS: u64 = 5;
    let budget = shutdown_budget().as_secs();
    let capped_floor = MIN_TIMEOUT_SECS.min(budget.saturating_sub(2));
    std::time::Duration::from_secs(
        budget
            .saturating_sub(MARGIN_SECS)
            .min(budget / 2)
            .max(capped_floor),
    )
}

// NOTE: deliberately no unit test calls `request_shutdown()` here. The signal
// is process-global and one-way — flipping it in a test would permanently
// contaminate every later test in the same binary that exercises production
// code paths which subscribe to it (e.g. the Postgres/ClickHouse sink retry
// loops). The end-to-end behaviour is covered by the SIGTERM drain e2e test
// (`crates/streamling-e2e/tests/shutdown_drain.rs`), which runs the signal
// path in a dedicated child process.
