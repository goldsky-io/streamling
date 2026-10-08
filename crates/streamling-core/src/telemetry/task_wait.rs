use crate::telemetry::MillisAccumulator;
use datafusion::execution::TaskContext;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// Splits the timeline of an operator that runs its loop in a spawned task into
/// input wait (`starved`) and its own work (`busy`).
///
/// `WrappingExec` measures `starved` around its input poll. For an inline
/// operator that poll is the upstream wait. For an operator that decouples
/// through a spawned task and an output channel (HTTP handler, WASM, plugin),
/// the wrapper only sees the channel, so the task's own work would read as
/// `starved`. Such an operator claims the meter its wrapper installed in the
/// `TaskContext` and brackets its awaits; the wrapper then reports the meter
/// instead of its channel wait.
///
/// Time awaiting an output send is neither starved nor busy: that is the
/// edge's `blocked`, which the wrapper already owns.
#[derive(Debug, Default)]
pub struct TaskWaitMeter {
    claimed: AtomicBool,
    timeline: Mutex<Timeline>,
}

#[derive(Debug, Default)]
struct Timeline {
    /// Start of the current busy span; `None` while the task is awaiting.
    busy_since: Option<Instant>,
    starved: MillisAccumulator,
    busy: MillisAccumulator,
}

/// Whole milliseconds drained from a [`TaskWaitMeter`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TaskWaitMillis {
    pub starved: u64,
    pub busy: u64,
}

impl TaskWaitMeter {
    /// Wrapper side: a child context that carries a fresh meter for the node
    /// about to execute. The upstream node's wrapper replaces it with its own,
    /// so an operator only ever sees its own node's meter.
    pub fn install(context: &TaskContext) -> (Arc<TaskContext>, Arc<TaskWaitMeter>) {
        let meter = Arc::new(TaskWaitMeter::default());
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

    /// Operator side: take ownership of the meter, if a wrapper installed one.
    /// Only the first claim wins, so one task timeline feeds one meter.
    pub fn claim(context: &TaskContext) -> TaskWaitHandle {
        let meter = context
            .session_config()
            .get_extension::<TaskWaitMeter>()
            .filter(|meter| !meter.claimed.swap(true, Ordering::AcqRel));
        if let Some(meter) = &meter {
            meter.timeline.lock().busy_since = Some(Instant::now());
        }
        TaskWaitHandle(meter)
    }

    /// Whether an operator took over this node's `starved` measurement.
    pub fn is_claimed(&self) -> bool {
        self.claimed.load(Ordering::Acquire)
    }

    /// Drain accrued whole milliseconds. The open busy span is banked first,
    /// so a long-running request is reported as it happens.
    pub fn take_whole_millis(&self) -> TaskWaitMillis {
        let mut timeline = self.timeline.lock();
        if let Some(since) = timeline.busy_since.as_mut() {
            let now = Instant::now();
            let span = now - *since;
            *since = now;
            timeline.busy.add(span);
        }
        TaskWaitMillis {
            starved: timeline.starved.take_whole_millis(),
            busy: timeline.busy.take_whole_millis(),
        }
    }

    fn begin_wait(&self) -> Instant {
        let now = Instant::now();
        let mut timeline = self.timeline.lock();
        if let Some(since) = timeline.busy_since.take() {
            timeline.busy.add(now - since);
        }
        now
    }

    fn end_wait(&self, started: Instant, starved: bool) {
        let now = Instant::now();
        let mut timeline = self.timeline.lock();
        if starved {
            timeline.starved.add(now - started);
        }
        timeline.busy_since = Some(now);
    }
}

/// The operator's view of its (possibly absent) [`TaskWaitMeter`]. Without a
/// meter every method is a plain pass-through.
#[derive(Debug, Default)]
pub struct TaskWaitHandle(Option<Arc<TaskWaitMeter>>);

impl TaskWaitHandle {
    /// Await the next input item. Only a delivered item counts as `starved`;
    /// the wait that ends in end-of-stream is teardown, not starvation.
    pub async fn next<S: Stream + Unpin>(&self, input: &mut S) -> Option<S::Item> {
        let Some(meter) = &self.0 else {
            return input.next().await;
        };
        let started = meter.begin_wait();
        let item = input.next().await;
        meter.end_wait(started, item.is_some());
        item
    }

    /// Await an output send. Excluded from both `starved` and `busy`.
    pub async fn send<F: Future>(&self, send: F) -> F::Output {
        let Some(meter) = &self.0 else {
            return send.await;
        };
        let started = meter.begin_wait();
        let output = send.await;
        meter.end_wait(started, false);
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use std::time::Duration;

    fn claimed_meter() -> (Arc<TaskWaitMeter>, TaskWaitHandle) {
        let ctx = SessionContext::new().task_ctx();
        let (child, meter) = TaskWaitMeter::install(&ctx);
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
    fn only_the_first_claim_wins() {
        let ctx = SessionContext::new().task_ctx();
        let (child, _meter) = TaskWaitMeter::install(&ctx);
        assert!(TaskWaitMeter::claim(&child).0.is_some());
        assert!(TaskWaitMeter::claim(&child).0.is_none());
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
            .send(tokio::time::sleep(Duration::from_millis(30)))
            .await;
        assert_eq!(handle.next(&mut input).await, None);

        let drained = meter.take_whole_millis();
        assert!(
            (30..60).contains(&drained.starved),
            "starved = input wait only: {drained:?}"
        );
        assert!(
            (30..60).contains(&drained.busy),
            "busy = work only, not the send wait: {drained:?}"
        );
    }

    #[tokio::test]
    async fn open_busy_span_is_reported_while_it_runs() {
        let (meter, _handle) = claimed_meter();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(meter.take_whole_millis().busy >= 20);
    }
}
