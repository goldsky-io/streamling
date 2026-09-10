//! `remaining_budget()` must measure time until the process is force-exited,
//! not time since shutdown was requested.
//!
//! Those coincide on the SIGTERM and component-failure paths, where the
//! request and the watchdog are armed on adjacent lines. They do not coincide
//! when a bounded plugin source finishes its range and pulls
//! `request_shutdown()` through the FFI: the watchdog is armed later, in
//! teardown, after the whole sink-drain phase. Measured from the request, the
//! budget could read zero while the drain had not even started — and every
//! component pacing a bounded wait by it would abandon work a live reader
//! would have accepted. On a bounded source that is data loss, because there
//! is no restart to replay it.
//!
//! Lives in `tests/` (its own process) on purpose: the shutdown signal and the
//! budget clock are one-way process-globals, so flipping them here cannot
//! contaminate the crate's other tests. This mirrors the note at the foot of
//! `src/shutdown/mod.rs`.

use std::time::{Duration, Instant};

use streamling_core::shutdown;

#[test]
fn budget_is_full_until_the_watchdog_is_armed_then_counts_down_to_it() {
    let budget = shutdown::shutdown_budget();
    assert!(
        budget > Duration::from_secs(1),
        "test needs a non-trivial budget, got {budget:?}"
    );

    // Before anything: the full budget.
    assert_eq!(shutdown::remaining_budget(), budget);

    // A bounded source completing pulls this lever without arming the
    // watchdog. The budget must not start draining -- there is no hard exit
    // to pace against yet.
    shutdown::request_shutdown();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        shutdown::remaining_budget(),
        budget,
        "the budget started counting down from the shutdown request while no watchdog was \
         armed. Any component pacing a bounded wait by this would give itself less time than \
         it actually has, and on a long sink drain it would reach zero before the drain even \
         began."
    );

    // Teardown arms the watchdog. From here the budget is time until that
    // fires, so it must be counting down.
    let deadline = Instant::now() + budget;
    shutdown::set_hard_exit_deadline(deadline);

    let immediately_after = shutdown::remaining_budget();
    assert!(
        immediately_after <= budget && immediately_after > budget - Duration::from_millis(500),
        "expected roughly the full budget just after arming, got {immediately_after:?}"
    );

    std::thread::sleep(Duration::from_millis(300));
    let later = shutdown::remaining_budget();
    assert!(
        later < immediately_after,
        "the budget must shrink once the watchdog is armed: {later:?} vs {immediately_after:?}"
    );

    // Idempotent: a second arming does not extend the deadline.
    shutdown::set_hard_exit_deadline(Instant::now() + budget * 10);
    let after_second_arm = shutdown::remaining_budget();
    assert!(
        after_second_arm <= later,
        "a later arming must not push the hard exit out: {after_second_arm:?} vs {later:?}"
    );
}
