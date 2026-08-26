//! Pins the three properties the Kafka source's offset-persistence retry
//! relies on. They pull against each other, and a regression in any one of
//! them is silent.
//!
//! The mid-flight checkpoint commit does not retry at its call site: the `?`
//! propagates, the stream tears down, and the pipeline restarts. That is the
//! intended answer to a backend that is genuinely gone — the call passes a 60s
//! bound and says so. It is the wrong answer to a backend that blipped. So the
//! commit retries *inside* that bound, and depends on:
//!
//! 1. **Transient failure, running: retry.** A failover, a pool timeout, a
//!    brief partition must be ridden out rather than propagated. Propagating
//!    is what restarts a healthy pipeline.
//! 2. **Permanent failure, running: do not retry.** Schema, credentials, a
//!    value that will not serialize — waiting cannot fix these. Retrying would
//!    burn the caller's whole bound on every finalize and then report a config
//!    error as an outage.
//! 3. **Draining: attempt exactly once.** The helper must attempt BEFORE it
//!    consults the signal, so a drain still tries to commit its tail — but
//!    must not then retry, because the drain budget is not there to be spent
//!    on a backend that is already down.
//!
//! If (3) regressed to "check the signal first", a drain would stop committing
//! tails at all. If it regressed to "keep retrying", a dead backend would eat
//! the shutdown budget — the original wedge.
//!
//! Lives in `tests/` (its own process): the shutdown signal is a one-way
//! process-global, so flipping it here cannot contaminate the crate's other
//! tests. Same convention as the note at the foot of `src/shutdown/mod.rs`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use streamling_core::error::StreamlingError;
use streamling_core::retry::retry_if_retriable_until_cancelled;
use streamling_core::{shutdown, streamling_err};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_transient_not_permanent_and_attempts_once_while_draining() {
    assert!(
        !*shutdown::subscribe().borrow(),
        "precondition: this test must start before any shutdown request"
    );

    // ---- 1. transient failure while running: ridden out ----
    let attempts = Arc::new(AtomicU32::new(0));
    let mut rx = shutdown::subscribe();
    let result: Result<(), _> = retry_if_retriable_until_cancelled(
        || {
            let attempts = attempts.clone();
            async move {
                // Fails twice then succeeds: a backend that blipped and came
                // back, which must not restart a pipeline.
                if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(StreamlingError::retriable("transient backend failure"))
                } else {
                    Ok(())
                }
            }
        },
        "state-backend offset persistence (transient)",
        &mut rx,
    )
    .await;

    assert!(
        result.is_ok(),
        "a transient state-backend failure must be ridden out, not propagated: propagating \
         is what restarts a healthy pipeline. Got {result:?}"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "expected two failures then a success"
    );

    // ---- 2. permanent failure while running: surfaced at once ----
    let attempts = Arc::new(AtomicU32::new(0));
    let mut rx = shutdown::subscribe();
    let started = std::time::Instant::now();
    let result: Result<(), _> = retry_if_retriable_until_cancelled(
        || {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(streamling_err!("schema is wrong; waiting will not fix it"))
            }
        },
        "state-backend offset persistence (permanent)",
        &mut rx,
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        result.is_err(),
        "a permanent failure must surface, not loop"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "a permanent error must not be retried: retrying it burns the caller's entire bound \
         on every finalize and then misreports a config error as a backend outage"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "a permanent failure must surface immediately, not after backoff; took {elapsed:?}"
    );

    // ---- 3. draining: attempt once, then give up ----
    shutdown::request_shutdown();
    assert!(
        *shutdown::subscribe().borrow(),
        "shutdown must be observable before the drain half of this test"
    );

    let attempts = Arc::new(AtomicU32::new(0));
    let mut rx = shutdown::subscribe();
    let started = std::time::Instant::now();
    let result: Result<(), _> = retry_if_retriable_until_cancelled(
        || {
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                // Retriable on purpose: without the drain this would loop.
                Err::<(), _>(StreamlingError::retriable("backend is down"))
            }
        },
        "state-backend offset persistence (draining)",
        &mut rx,
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        result.is_err(),
        "a failing write during a drain must give up, not retry. Got {result:?}"
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "the drain must still ATTEMPT the commit exactly once — zero attempts means a drain \
         stops committing its tail at all; more than one means a dead backend eats the \
         shutdown budget, which is the wedge this bound exists to prevent"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "giving up during a drain must be immediate, not after a backoff sleep; took {elapsed:?}"
    );
}
