//! Structured shutdown: a [`ShutdownController`] owns the process-wide drain
//! budget and an ordered list of [`ComponentScope`]s — one cancellation domain
//! plus task tracker per pipeline component. This is the sanctioned replacement for the
//! hand-rolled pattern audited in Part 4: raw
//! `tokio::spawn` + `shutdown::subscribe()` wiring per component, which
//! produced six variants of the same orphan-task/unbounded-drain bug.
//!
//! # Relationship to the global watch (`streamling_core::shutdown`)
//!
//! The global watch stays as a migration bridge. The two are kept coherent in
//! both directions:
//! - [`ShutdownController::request_shutdown`] flips the global watch AND
//!   cancels the root token.
//! - A bridge task (spawned in [`ShutdownController::new`]) cancels the root
//!   token when the global watch flips (e.g. the run loop's SIGTERM handler).
//!
//! Components migrate one at a time: each port replaces the component's
//! `shutdown::subscribe()` hand-wiring and raw `tokio::spawn`s with
//! `scope.cancelled()` / `scope.spawn()` in the same PR, so the two mechanisms
//! never coexist long-term inside one component.
//!
//! # Drain ladder
//!
//! [`ShutdownController::drain`] cancels scopes in registration order
//! (register front-to-back: sources → transforms → sinks) and waits for each
//! scope's tasks with a slice of the remaining budget. A scope that blows its
//! slice is logged and LEFT RUNNING — tasks are never aborted (an abort across
//! FFI/WASM can corrupt plugin state); the run-loop watchdog's `process::exit`
//! remains the backstop for leaked tasks.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use tokio_util::task::TaskTracker;
use tracing::{info, warn};

/// When in the run loop's PHASED teardown a scope is drained. The teardown
/// sequence is: terminal-finalize wait → [`DrainStage::DataPath`] drain →
/// plugin-source channel GC → plugin Terminate + dispatcher drain →
/// [`DrainStage::PostPlugin`] drain → coordinator stop. Tasks that serve the
/// plugin dispatchers while they flush (ack/metrics forwarders) MUST register
/// `PostPlugin`: a `DataPath` drain would cancel them mid-flush — either
/// losing acks (token-honoring) or warning spuriously every shutdown
/// (token-ignoring).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DrainStage {
    /// Data-path components (sources, helper tasks, sinks' helpers): drained
    /// in the main teardown drain, right after the terminal checkpoint
    /// finalizes. The default.
    #[default]
    DataPath,
    /// Outlives the plugin dispatcher drain; drained just before the
    /// coordinator stops.
    PostPlugin,
}

/// Owns the root cancellation token, the drain budget, and the ordered scopes.
pub struct ShutdownController {
    root: CancellationToken,
    budget: Duration,
    scopes: Mutex<Vec<Arc<ComponentScope>>>,
}

impl ShutdownController {
    /// `budget` is the total graceful-drain budget, normally
    /// [`super::shutdown_budget()`].
    pub fn new(budget: Duration) -> Arc<Self> {
        let controller = Arc::new(Self {
            root: CancellationToken::new(),
            budget,
            scopes: Mutex::new(Vec::new()),
        });
        // Bridge: a global request_shutdown() (SIGTERM handler, fail_drain)
        // must cancel scope tokens too, or ported components would never see
        // it. Holds only a weak ref so an abandoned controller is dropped.
        let weak = Arc::downgrade(&controller);
        let mut rx = super::subscribe();
        // Sanctioned: the bridge task is owned by the controller itself and
        // exits when the watch flips or its sender drops — nothing to drain.
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            if !*rx.borrow() && rx.changed().await.is_err() {
                return;
            }
            if let Some(c) = weak.upgrade() {
                c.root.cancel();
            }
        });
        controller
    }

    /// Register a component scope. Registration order IS drain order — callers
    /// register front-to-back (sources first, sinks last) so upstream stages
    /// stop producing before downstream stages are asked to finish flushing.
    pub fn scope(&self, name: impl Into<String>) -> Arc<ComponentScope> {
        self.scope_at(name, DrainStage::DataPath)
    }

    /// [`Self::scope`] with an explicit [`DrainStage`].
    pub fn scope_at(&self, name: impl Into<String>, stage: DrainStage) -> Arc<ComponentScope> {
        let scope = Arc::new(ComponentScope {
            name: name.into(),
            token: self.root.child_token(),
            stage_token: CancellationToken::new(),
            tracker: TaskTracker::new(),
            drain_share: 1.0,
            stage,
        });
        self.scopes.lock().push(Arc::clone(&scope));
        scope
    }

    /// Request process-wide graceful shutdown: flips the global watch (so
    /// unported components see it) and cancels every scope token. Idempotent.
    pub fn request_shutdown(&self) {
        super::request_shutdown();
        self.root.cancel();
    }

    pub fn is_shutdown_requested(&self) -> bool {
        self.root.is_cancelled()
    }

    /// Cancel only this controller's tokens without flipping the process-wide
    /// watch. Prod code wants [`Self::request_shutdown`]; this exists so unit
    /// tests can exercise cancellation without contaminating the global watch
    /// for every later test in the binary (see the note in `shutdown.rs`).
    pub fn cancel_local(&self) {
        self.root.cancel();
    }

    /// Run the drain ladder for one [`DrainStage`]: cancel each of that
    /// stage's scopes in registration order and wait (bounded) for its
    /// tracked tasks. The budget is `self.budget` unless the caller passes an
    /// explicit earlier `deadline` (the run loop's watchdog deadline minus
    /// margin). Never aborts a task. Scopes registered under other stages are
    /// untouched.
    pub async fn drain(&self, stage: DrainStage, deadline: Option<Instant>) {
        let deadline = deadline.unwrap_or_else(|| Instant::now() + self.budget);
        let scopes: Vec<Arc<ComponentScope>> = self
            .scopes
            .lock()
            .iter()
            .filter(|s| s.stage == stage)
            .cloned()
            .collect();
        let total_shares: f32 = scopes.iter().map(|s| s.drain_share).sum();
        let mut shares_left = total_shares.max(f32::EPSILON);

        // Below this, a scope never had a real chance to wind down: an
        // upstream scope consumed the stage budget before this one's slice
        // was computed. Overrunning a starved slice is not evidence of a
        // wedge, and labeling it like one misdirects triage toward a healthy
        // component while the real culprit is the scope that ate the budget.
        const STARVED_SLICE_FLOOR: Duration = Duration::from_millis(250);

        for scope in scopes {
            scope.token.cancel();
            scope.stage_token.cancel();
            scope.tracker.close();
            let remaining = deadline.saturating_duration_since(Instant::now());
            let slice = remaining.mul_f32((scope.drain_share / shares_left).min(1.0));
            shares_left = (shares_left - scope.drain_share).max(f32::EPSILON);
            if scope.tracker.is_empty() {
                // Wound down before its stage drain even began (tasks that
                // watch the root-child token exit at shutdown request). Still
                // emit the positive confirmation: a healthy drain must be
                // assertable from a line's PRESENCE, not from the absence of
                // an overrun warning.
                info!(scope = scope.name.as_str(), "component drained cleanly");
                continue;
            }
            match tokio::time::timeout(slice, scope.tracker.wait()).await {
                Ok(()) => info!(scope = scope.name.as_str(), "component drained cleanly"),
                Err(_) if slice < STARVED_SLICE_FLOOR => warn!(
                    scope = scope.name.as_str(),
                    tasks = scope.tracker.len(),
                    slice_ms = slice.as_millis() as u64,
                    "component was starved of drain budget (upstream scopes \
                     consumed it); its tasks may be healthy — this is not \
                     evidence of a wedge in this scope"
                ),
                Err(_) => warn!(
                    scope = scope.name.as_str(),
                    tasks = scope.tracker.len(),
                    slice_ms = slice.as_millis() as u64,
                    "component blew its drain budget slice; leaving its tasks \
                     to the shutdown watchdog (never aborting across FFI)"
                ),
            }
        }
    }
}

/// One pipeline component's cancellation domain: a child token of the
/// controller root plus a [`TaskTracker`]. `scope.spawn` is the only
/// sanctioned way to start a background task in connector code (enforced by
/// clippy `disallowed-methods`).
#[derive(Debug)]
pub struct ComponentScope {
    name: String,
    token: CancellationToken,
    /// Cancelled ONLY when this scope's own drain stage begins — unlike
    /// `token`, which is a child of the controller root and therefore fires
    /// the moment shutdown is REQUESTED. The distinction is load-bearing for
    /// tasks that must keep serving through the drain: a plugin sink's ack
    /// forwarder, for example, must forward the terminal ack that arrives
    /// AFTER the shutdown request (the terminal marker rides the last batch
    /// and the sink flushes before acking) — exiting at request time would
    /// silently drop it and the terminal epoch could never finalize. Such
    /// tasks watch this token; tasks that should stop producing at the
    /// request watch `token`.
    stage_token: CancellationToken,
    tracker: TaskTracker,
    drain_share: f32,
    stage: DrainStage,
}

impl ComponentScope {
    /// A scope attached to NO controller: its token never fires and nothing
    /// drains its tracker. For direct-construction paths outside a run loop —
    /// unit tests, ad-hoc tools — so component constructors can require a
    /// scope unconditionally. Semantically equivalent to the raw detached
    /// spawn it replaces, but keeps every spawn on the sanctioned API.
    pub fn detached(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            token: CancellationToken::new(),
            stage_token: CancellationToken::new(),
            tracker: TaskTracker::new(),
            drain_share: 1.0,
            stage: DrainStage::DataPath,
        })
    }

    /// Spawn a task tracked by this scope. The task should select on
    /// [`Self::cancelled`] and wind down promptly when it fires.
    pub fn spawn<F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tracker.spawn(fut)
    }

    /// Resolves when this scope (or the whole pipeline) is cancelled.
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.token.cancelled()
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// The scope's token, for call sites that need an owned/cloneable handle
    /// (e.g. moving into a spawned task).
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// The stage-local token: fires when this scope's drain stage begins, NOT
    /// when shutdown is requested (see the field doc). For tasks that must
    /// keep serving between the shutdown request and their own stage's drain
    /// — forwarders serving a plugin's post-request flush, most notably.
    pub fn stage_token(&self) -> &CancellationToken {
        &self.stage_token
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // NOTE: no test here calls `ShutdownController::request_shutdown()` — it
    // flips the process-global watch, which is one-way and would contaminate
    // every later test in this binary (see the note in `shutdown.rs`).
    // `cancel_local()` exists precisely so these tests can exercise the
    // cancellation and drain paths on per-instance tokens only. The
    // global-watch bridge and the SIGTERM path are covered by the
    // `shutdown_drain` e2e, which signals a dedicated child process.

    #[tokio::test]
    async fn scope_spawn_is_tracked_and_drain_waits_for_wind_down() {
        let controller = ShutdownController::new(Duration::from_secs(5));
        let scope = controller.scope("source");
        let finished = Arc::new(AtomicUsize::new(0));

        let f = Arc::clone(&finished);
        let token = scope.token().clone();
        scope.spawn(async move {
            token.cancelled().await;
            f.fetch_add(1, Ordering::SeqCst);
        });

        controller.cancel_local();
        controller.drain(DrainStage::DataPath, None).await;
        assert_eq!(
            finished.load(Ordering::SeqCst),
            1,
            "drain must wait for a task that winds down on cancellation"
        );
    }

    #[tokio::test]
    async fn drain_cancels_scopes_in_registration_order() {
        let controller = ShutdownController::new(Duration::from_secs(5));
        let source = controller.scope("source");
        let sink = controller.scope("sink");
        let order = Arc::new(Mutex::new(Vec::new()));

        for (name, scope) in [("source", &source), ("sink", &sink)] {
            let order = Arc::clone(&order);
            let token = scope.token().clone();
            scope.spawn(async move {
                token.cancelled().await;
                order.lock().push(name);
            });
        }

        controller.drain(DrainStage::DataPath, None).await;
        assert_eq!(
            *order.lock(),
            vec!["source", "sink"],
            "front-to-back: sources stop producing before sinks are drained"
        );
    }

    #[tokio::test]
    async fn drain_leaves_a_wedged_scope_running_instead_of_aborting() {
        let controller = ShutdownController::new(Duration::from_millis(200));
        let wedged = controller.scope("wedged-sink");
        let aborted = Arc::new(AtomicUsize::new(0));

        let a = Arc::clone(&aborted);
        wedged.spawn(async move {
            // Ignores cancellation — the orphan-task class under test.
            std::future::pending::<()>().await;
            a.fetch_add(1, Ordering::SeqCst);
        });

        controller.cancel_local();
        // Must return once the budget slice expires — not hang, not abort.
        controller.drain(DrainStage::DataPath, None).await;
        assert_eq!(wedged.tracker.len(), 1, "the wedged task is left running");
        assert_eq!(aborted.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancel_local_does_not_flip_the_global_watch() {
        let controller = ShutdownController::new(Duration::from_secs(1));
        let scope = controller.scope("only");
        controller.cancel_local();
        assert!(scope.is_cancelled());
        assert!(
            !*crate::shutdown::subscribe().borrow(),
            "cancel_local must stay instance-local (test isolation depends on it)"
        );
    }

    #[tokio::test]
    async fn data_path_drain_leaves_post_plugin_scopes_untouched() {
        let controller = ShutdownController::new(Duration::from_secs(5));
        let data = controller.scope("source");
        let post = controller.scope_at("plugin-forwarders", DrainStage::PostPlugin);

        let drained = Arc::new(AtomicUsize::new(0));
        let d = Arc::clone(&drained);
        let token = post.token().clone();
        post.spawn(async move {
            token.cancelled().await;
            d.fetch_add(1, Ordering::SeqCst);
        });

        controller.drain(DrainStage::DataPath, None).await;
        assert!(data.is_cancelled(), "DataPath scope drained");
        assert!(
            !post.is_cancelled(),
            "PostPlugin scope must survive the DataPath drain (ack forwarders \
             serve the plugin flush that happens between the stages)"
        );
        assert_eq!(drained.load(Ordering::SeqCst), 0);

        controller.drain(DrainStage::PostPlugin, None).await;
        assert!(post.is_cancelled());
        assert_eq!(drained.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn detached_scope_spawns_without_a_controller() {
        let scope = ComponentScope::detached("test-only");
        let done = Arc::new(AtomicUsize::new(0));
        let d = Arc::clone(&done);
        scope
            .spawn(async move {
                d.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .unwrap();
        assert_eq!(done.load(Ordering::SeqCst), 1);
        assert!(!scope.is_cancelled(), "no controller ever cancels it");
    }

    #[tokio::test]
    async fn empty_scopes_drain_immediately() {
        let controller = ShutdownController::new(Duration::from_secs(5));
        let _a = controller.scope("a");
        let _b = controller.scope("b");
        let started = std::time::Instant::now();
        controller.drain(DrainStage::DataPath, None).await;
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "no tracked tasks → no waiting"
        );
    }
}
