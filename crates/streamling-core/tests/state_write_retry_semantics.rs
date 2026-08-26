//! Pins the two properties the Kafka source's offset-persistence retry relies
//! on, because they pull in opposite directions and a change to either would
//! be silent.
//!
//! The mid-flight checkpoint commit does not retry at its own call site: its
//! `?` propagates, the stream tears down, and the pipeline restarts. So a blip
//! long enough to fail one state-backend acquire — a failover, a connection
//! storm, a brief partition — would restart an otherwise healthy pipeline. The
//! commit therefore retries *inside*, and relies on:
//!
//! 1. **Steady state: retry.** Not shutting down, a transient failure must be
//!    ridden out rather than propagated. The caller's own 60s bound is what
//!    defines giving up ("a backend gone THAT long should restart the
//!    pipeline"); everything shorter should survive.
//! 2. **Draining: exactly one attempt.** The helper must attempt BEFORE it
//!    consults the signal, so a drain still tries to commit the tail — but
//!    must not then retry, because the drain budget is not there to be spent
//!    on a backend that is already down. The failure has to surface at once so
//!    the terminal caller can report the tail as uncommitted.
//!
//! If (2) regressed to "check the signal first", a drain would stop committing
//! its tail at all. If it regressed to "keep retrying", a dead backend would
//! eat the shutdown budget — the original wedge.
//!
//! Lives in `tests/` (its own process): the shutdown signal is a one-way
//! process-global, so flipping it here cannot contaminate the crate's other
//! tests. Same convention as the note at the foot of `src/shutdown/mod.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use streamling_core::retry::{RetryOutcome, retry_forever_with_backoff_until_cancelled};
use streamling_core::{shutdown, streamling_err};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_failures_are_ridden_out_but_a_drain_attempts_exactly_once() {
    // ---- 1. steady state: a transient failure must not propagate ----
    assert!(
        !*shutdown::subscribe().borrow(),
        "precondition: this test must start before any shutdown request"
    );

    let attempts = Arc::new(AtomicU32::new(0));
    let mut rx = shutdown::subscribe();
    let outcome = retry_forever_with_backoff_until_cancelled(
        || {
            let attempts = attempts.clone();
            async move {
                // Fail twice, then succeed — a backend that blipped and came
                // back, which is the case that must not restart a pipeline.
                if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(streamling_err!("transient backend failure"))
                } else {
                    Ok(())
                }
            }
        },
        "state-backend offset persistence (test)",
        &mut rx,
    )
    .await;

    assert!(
        matches!(outcome, RetryOutcome::Completed),
        "a transient state-backend failure must be ridden out, not propagated: propagating \
         is what restarts a healthy pipeline. Got {outcome:?}"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "expected two failures then a success"
    );

    // ---- 2. draining: attempt once, then give up immediately ----
    shutdown::request_shutdown();
    assert!(
        *shutdown::subscribe().borrow(),
        "shutdown must be observable before the drain half of this test"
    );

    let attempts = Arc::new(AtomicU32::new(0));
    let mut rx = shutdown::subscribe();
    let started = std::time::Instant::now();
    let outcome = retry_forever_with_backoff_until_cancelled(
        || {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(streamling_err!("backend is down"))
            }
        },
        "state-backend offset persistence (test, draining)",
        &mut rx,
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        matches!(outcome, RetryOutcome::Cancelled),
        "a failing write during a drain must give up, not retry. Got {outcome:?}"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "the drain must still ATTEMPT the commit exactly once — zero attempts means a \
         drain stops committing its tail at all; more than one means a dead backend eats \
         the shutdown budget, which is the wedge this bound exists to prevent"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "giving up during a drain must be immediate, not after a backoff sleep; took {elapsed:?}"
    );
}
