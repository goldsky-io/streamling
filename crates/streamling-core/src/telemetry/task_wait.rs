use crate::telemetry::node_time::NodeTime;
use crate::telemetry::recorder::MetricsRecorder;
use datafusion::execution::TaskContext;
use futures::{Stream, StreamExt};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Splits the timeline of an operator that runs its loop in a spawned task into
/// input wait (`starved`) and its own work (`busy`).
///
/// `WrappingExec` measures `starved` around its input poll. For an inline
/// operator that poll is the upstream wait. For an operator that decouples
/// through a spawned task and an output channel (HTTP handler, WASM, plugin,
/// sources), the wrapper only sees the channel, so the task's own work would
/// read as `starved`. Such an operator claims the meter its wrapper installed
/// in the `TaskContext` and brackets its awaits; the meter then reports the
/// node's `starved` and `busy` instead of the wrapper.
///
/// Time awaiting an output send is neither starved nor busy: that is the
/// edge's `blocked`, which the wrapper already owns.
pub struct TaskWaitMeter {
    recorder: Arc<MetricsRecorder>,
    metadata_id: String,
    /// Set by the first claim; from then on the meter owns `starved`/`busy`.
    timers: OnceLock<Timers>,
    /// Whether a live handle holds the meter. One timeline at a time.
    held: AtomicBool,
}

struct Timers {
    starved: Arc<NodeTime>,
    busy: Arc<NodeTime>,
}

impl fmt::Debug for TaskWaitMeter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskWaitMeter")
            .field("metadata_id", &self.metadata_id)
            .field("claimed", &self.is_claimed())
            .finish()
    }
}

/// Time accrued by a [`TaskWaitMeter`], open spans included.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TaskWaitTotals {
    pub starved: Duration,
    pub busy: Duration,
}

impl TaskWaitMeter {
    /// Wrapper side: a child context that carries a fresh meter for the node
    /// about to execute. The upstream node's wrapper replaces it with its own,
    /// so an operator only ever sees its own node's meter.
    pub fn install(
        context: &TaskContext,
        recorder: Arc<MetricsRecorder>,
        metadata_id: &str,
    ) -> (Arc<TaskContext>, Arc<TaskWaitMeter>) {
        let meter = Arc::new(TaskWaitMeter {
            recorder,
            metadata_id: metadata_id.to_string(),
            timers: OnceLock::new(),
            held: AtomicBool::new(false),
        });
        let child = TaskContext::new(
            context.task_id(),
            context.session_id(),
            context
                .session_config()
                .clone()
                .with_extension(Arc::clone(&meter)),
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        );
        (Arc::new(child), meter)
    }

    /// Operator side: take the meter, if a wrapper installed one and no live
    /// handle holds it. The busy clock runs from here until the handle drops.
    pub fn claim(context: &TaskContext) -> TaskWaitHandle {
        let meter = context
            .session_config()
            .get_extension::<TaskWaitMeter>()
            .filter(|meter| !meter.held.swap(true, Ordering::AcqRel));
        if let Some(meter) = &meter {
            meter.timers().busy.open();
        }
        TaskWaitHandle(meter)
    }

    /// Operator side: take over this node's `starved`/`busy` without timing
    /// anything yet. An operator that runs inner operators one after another
    /// (hybrid source phases) reserves at `execute`, and each inner operator
    /// claims the meter in turn.
    pub fn reserve(context: &TaskContext) {
        if let Some(meter) = context.session_config().get_extension::<TaskWaitMeter>() {
            meter.timers();
        }
    }

    /// Whether an operator took over this node's `starved` measurement.
    pub fn is_claimed(&self) -> bool {
        self.timers.get().is_some()
    }

    pub fn totals(&self) -> TaskWaitTotals {
        self.timers
            .get()
            .map_or_else(TaskWaitTotals::default, |timers| TaskWaitTotals {
                starved: timers.starved.total(),
                busy: timers.busy.total(),
            })
    }

    fn timers(&self) -> &Timers {
        self.timers.get_or_init(|| Timers {
            starved: NodeTime::wall(Arc::clone(&self.recorder), &self.metadata_id, "starved", ""),
            busy: NodeTime::wall(Arc::clone(&self.recorder), &self.metadata_id, "busy", ""),
        })
    }
}

/// The operator's view of its (possibly absent) [`TaskWaitMeter`]. Without a
/// meter every method is a plain pass-through. Dropping it stops the busy
/// clock and frees the meter for the next claim.
#[derive(Debug, Default)]
pub struct TaskWaitHandle(Option<Arc<TaskWaitMeter>>);

impl TaskWaitHandle {
    /// Await the next input item. Only a delivered item counts as `starved`;
    /// the wait that ends in end-of-stream is teardown, not starvation.
    pub async fn next<S: Stream + Unpin>(&self, input: &mut S) -> Option<S::Item> {
        let Some(meter) = &self.0 else {
            return input.next().await;
        };
        let timers = meter.timers();
        timers.busy.close();
        timers.starved.open();
        let item = input.next().await;
        if item.is_some() {
            timers.starved.close();
            timers.busy.open();
        } else {
            // The input is exhausted: the task has no more work to time.
            timers.starved.discard();
        }
        item
    }

    /// Await new input from an external system: `starved`.
    pub async fn wait_input<F: Future>(&self, wait: F) -> F::Output {
        let Some(meter) = &self.0 else {
            return wait.await;
        };
        let timers = meter.timers();
        timers.busy.close();
        timers.starved.open();
        let output = wait.await;
        timers.starved.close();
        timers.busy.open();
        output
    }

    /// Await an output send. Excluded from both `starved` and `busy`.
    pub async fn send<F: Future>(&self, send: F) -> F::Output {
        let Some(meter) = &self.0 else {
            return send.await;
        };
        let timers = meter.timers();
        timers.busy.close();
        let output = send.await;
        timers.busy.open();
        output
    }
}

impl Drop for TaskWaitHandle {
    fn drop(&mut self) {
        if let Some(meter) = &self.0 {
            meter.timers().busy.close();
            meter.held.store(false, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::recorder::get_metrics_recorder;
    use datafusion::prelude::SessionContext;

    fn install(ctx: &TaskContext) -> (Arc<TaskContext>, Arc<TaskWaitMeter>) {
        TaskWaitMeter::install(ctx, get_metrics_recorder(), "task_wait::test")
    }

    fn claimed_meter() -> (Arc<TaskWaitMeter>, TaskWaitHandle) {
        let ctx = SessionContext::new().task_ctx();
        let (child, meter) = install(&ctx);
        let handle = TaskWaitMeter::claim(&child);
        assert!(meter.is_claimed());
        (meter, handle)
    }

    #[test]
    fn claim_without_installed_meter_is_a_pass_through() {
        let ctx = SessionContext::new().task_ctx();
        assert!(TaskWaitMeter::claim(&ctx).0.is_none());
    }

    #[test]
    fn one_live_handle_at_a_time() {
        let ctx = SessionContext::new().task_ctx();
        let (child, _meter) = install(&ctx);
        let first = TaskWaitMeter::claim(&child);
        assert!(first.0.is_some());
        assert!(TaskWaitMeter::claim(&child).0.is_none());
        drop(first);
        assert!(
            TaskWaitMeter::claim(&child).0.is_some(),
            "a dropped handle frees the meter for the next phase"
        );
    }

    #[test]
    fn reserve_claims_the_node_without_timing() {
        let ctx = SessionContext::new().task_ctx();
        let (child, meter) = install(&ctx);
        TaskWaitMeter::reserve(&child);
        assert!(meter.is_claimed());
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(meter.totals(), TaskWaitTotals::default());
    }

    #[tokio::test]
    async fn splits_input_wait_work_and_send_wait() {
        let (meter, handle) = claimed_meter();
        let input = futures::stream::iter([1]).then(|i| async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            i
        });
        let mut input = std::pin::pin!(input);

        assert_eq!(handle.next(&mut input).await, Some(1));
        tokio::time::sleep(Duration::from_millis(30)).await; // work
        handle
            .send(tokio::time::sleep(Duration::from_millis(300)))
            .await;
        assert_eq!(handle.next(&mut input).await, None);

        // A loaded runner can overshoot a sleep, so assert against the 300ms
        // send: it must not leak into either state.
        let totals = meter.totals();
        assert!(totals.starved >= Duration::from_millis(30), "{totals:?}");
        assert!(totals.busy >= Duration::from_millis(30), "{totals:?}");
        assert!(
            totals.starved + totals.busy < Duration::from_millis(300),
            "the send wait is neither starved nor busy: {totals:?}"
        );
    }

    #[tokio::test]
    async fn wait_input_counts_as_starved() {
        let (meter, handle) = claimed_meter();
        handle
            .wait_input(tokio::time::sleep(Duration::from_millis(30)))
            .await;
        let totals = meter.totals();
        assert!(totals.starved >= Duration::from_millis(30), "{totals:?}");
        assert!(totals.busy < Duration::from_millis(30), "{totals:?}");
    }

    #[tokio::test]
    async fn end_of_input_stops_the_busy_clock() {
        let (meter, handle) = claimed_meter();
        let mut input = futures::stream::empty::<()>();
        assert_eq!(handle.next(&mut input).await, None);
        let at_eof = meter.totals();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            meter.totals(),
            at_eof,
            "a task past end of input is not working"
        );
    }

    #[tokio::test]
    async fn dropping_the_handle_stops_the_busy_clock() {
        let (meter, handle) = claimed_meter();
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(handle);
        let at_drop = meter.totals();
        assert!(at_drop.busy >= Duration::from_millis(20), "{at_drop:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(meter.totals(), at_drop);
    }

    #[tokio::test]
    async fn open_busy_span_is_reported_while_it_runs() {
        let (meter, _handle) = claimed_meter();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(meter.totals().busy >= Duration::from_millis(20));
    }
}
