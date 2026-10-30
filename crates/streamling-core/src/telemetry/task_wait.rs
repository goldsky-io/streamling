use crate::telemetry::node_time::NodeTime;
use crate::telemetry::recorder::MetricsRecorder;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
/// Time awaiting an output send is the edge's `blocked`: the wrapper hands its
/// `blocked` series to the meter at install. A slow downstream holds the task
/// back only once the output channel is full. The wrapper pauses that series
/// while it works on a batch, which holds the task back too.
pub struct TaskWaitMeter {
    recorder: Arc<MetricsRecorder>,
    metadata_id: String,
    /// Set by the first claim; from then on the meter owns `starved`/`busy`.
    timers: OnceLock<Timers>,
    /// The latest claim. One timeline at a time: an older handle becomes a
    /// pass-through, so a finishing phase cannot disturb the next one.
    generation: AtomicU64,
    /// Set by [`Self::reserve`]: inner operators share this meter.
    reserved: AtomicBool,
    /// The edge's `blocked`, timed by output sends. `None` for a fan-out
    /// producer, whose broadcast times each edge.
    blocked: Option<Arc<NodeTime>>,
    /// A SQL transform's subtree polls, which run in its forwarder task.
    subtree: Mutex<SubtreePolls>,
}

/// DataFusion records an operator's compute when its poll ends, so a flush
/// during a poll would bank that compute as the wrapper's `starved`. The
/// forwarder marks each poll here; the wrapper binds its `starved` once it
/// exists, which may be mid-poll.
#[derive(Default)]
struct SubtreePolls {
    polling: bool,
    starved: Option<Arc<NodeTime>>,
}

struct Timers {
    starved: Arc<NodeTime>,
    busy: Arc<NodeTime>,
    /// Output sends. A reserved meter's inner operators send into the
    /// enclosing operator, not along the edge, so theirs time nothing.
    send: Option<Arc<NodeTime>>,
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
#[cfg(any(test, feature = "test-utils"))]
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
        blocked: Option<Arc<NodeTime>>,
    ) -> (Arc<TaskContext>, Arc<TaskWaitMeter>) {
        let meter = Arc::new(TaskWaitMeter {
            recorder,
            metadata_id: metadata_id.to_string(),
            timers: OnceLock::new(),
            generation: AtomicU64::new(0),
            reserved: AtomicBool::new(false),
            blocked,
            subtree: Mutex::default(),
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

    /// Operator side: take the meter, if a wrapper installed one. The busy
    /// clock runs from here until the handle drops or a later claim takes over.
    pub fn claim(context: &TaskContext) -> TaskWaitHandle {
        TaskWaitHandle(
            context
                .session_config()
                .get_extension::<TaskWaitMeter>()
                .map(|meter| {
                    let generation = meter.generation.fetch_add(1, Ordering::AcqRel) + 1;
                    let timers = meter.timers();
                    // The older claim's open input wait ends here, not when its
                    // now pass-through guard drops.
                    timers.starved.close();
                    timers.busy.open();
                    (meter, generation)
                }),
        )
    }

    /// Operator side: take over this node's `starved`/`busy` without timing
    /// anything yet. An operator that runs inner operators one after another
    /// (hybrid source phases) reserves at `execute`, and each inner operator
    /// claims the meter in turn. The returned handle times the operator's own
    /// work between them and its sends along the edge.
    pub fn reserve(context: &TaskContext) -> ReservedMeter {
        ReservedMeter(
            context
                .session_config()
                .get_extension::<TaskWaitMeter>()
                .map(|meter| {
                    meter.reserved.store(true, Ordering::Release);
                    meter.timers();
                    ReservedTimers {
                        busy: NodeTime::wall(
                            Arc::clone(&meter.recorder),
                            &meter.metadata_id,
                            "busy",
                            "",
                        ),
                        send: meter.blocked.clone(),
                    }
                }),
        )
    }

    /// Wrapper side: the meter an enclosing operator reserved for its inner
    /// operators. An inner operator's own wrapper shares it instead of
    /// installing a fresh one, so the inner work stays the enclosing node's.
    pub fn reserved(context: &TaskContext) -> Option<Arc<TaskWaitMeter>> {
        context
            .session_config()
            .get_extension::<TaskWaitMeter>()
            .filter(|meter| meter.reserved.load(Ordering::Acquire))
    }

    /// Wrapper side: `starved` excludes the subtree's compute, so it stops
    /// accruing while a subtree poll runs.
    pub fn time_subtree_polls(&self, starved: Arc<NodeTime>) {
        let mut subtree = self.subtree.lock();
        if subtree.polling {
            starved.poll_started();
        }
        subtree.starved = Some(starved);
    }

    /// Operator side: `input` with each poll marked as running the node's own
    /// subtree. A SQL transform's forwarder polls its subtree through this.
    pub fn mark_subtree_polls(
        context: &TaskContext,
        mut input: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let Some(meter) = context.session_config().get_extension::<TaskWaitMeter>() else {
            return input;
        };
        let schema = input.schema();
        let polls = futures::stream::poll_fn(move |cx| {
            meter.subtree_poll(true);
            let poll = input.poll_next_unpin(cx);
            meter.subtree_poll(false);
            poll
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, polls))
    }

    fn subtree_poll(&self, polling: bool) {
        let mut subtree = self.subtree.lock();
        subtree.polling = polling;
        if let Some(starved) = &subtree.starved {
            if polling {
                starved.poll_started();
            } else {
                starved.poll_ended();
            }
        }
    }

    /// Whether an operator took over this node's `starved` measurement.
    pub fn is_claimed(&self) -> bool {
        self.timers.get().is_some()
    }

    #[cfg(any(test, feature = "test-utils"))]
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
            send: self
                .blocked
                .clone()
                .filter(|_| !self.reserved.load(Ordering::Acquire)),
        })
    }
}

/// The operator's view of its (possibly absent) [`TaskWaitMeter`]. Without a
/// meter, or after a later claim, every method is a plain pass-through.
/// Dropping it stops the busy clock.
#[derive(Debug, Default)]
pub struct TaskWaitHandle(Option<(Arc<TaskWaitMeter>, u64)>);

impl TaskWaitHandle {
    /// Await the next input item. Only a wait that delivered data is
    /// `starved`: one that ends in end-of-stream or an error is teardown, and
    /// its unflushed part is dropped.
    pub async fn next<S>(&self, input: &mut S) -> Option<S::Item>
    where
        S: Stream + Unpin,
        S::Item: Delivery,
    {
        let mut wait = self.bracket(Bracket::Input);
        let item = input.next().await;
        match &item {
            // The input is exhausted: the task has no more work to time.
            None => wait.end = Bracket::Exhausted,
            Some(item) if !item.is_data() => wait.end = Bracket::Failed,
            Some(_) => {}
        }
        item
    }

    /// Await new input from an external system: `starved`. A cancelled wait
    /// (a losing `select!` branch) still counts up to its cancellation.
    pub async fn wait_input<F: Future>(&self, wait: F) -> F::Output {
        let _wait = self.bracket(Bracket::Input);
        wait.await
    }

    /// Await an output send: the edge's `blocked`.
    pub async fn send<F: Future>(&self, send: F) -> F::Output {
        let _wait = self.bracket(Bracket::Send);
        send.await
    }

    /// Await something that is neither input, work, nor a send along the
    /// edge (a wait on the sinks): in no state.
    pub async fn untimed<F: Future>(&self, wait: F) -> F::Output {
        let _wait = self.bracket(Bracket::Untimed);
        wait.await
    }

    /// The meter's timers while this handle holds the latest claim.
    fn timers(&self) -> Option<&Timers> {
        self.0
            .as_ref()
            .filter(|(meter, generation)| meter.generation.load(Ordering::Acquire) == *generation)
            .map(|(meter, _)| meter.timers())
    }

    fn bracket(&self, end: Bracket) -> WaitGuard<'_> {
        if let Some(timers) = self.timers() {
            timers.busy.close();
            match end {
                Bracket::Input => timers.starved.open(),
                Bracket::Send => {
                    if let Some(send) = &timers.send {
                        send.open();
                    }
                }
                Bracket::Exhausted | Bracket::Failed | Bracket::Untimed => {}
            }
        }
        WaitGuard { handle: self, end }
    }
}

/// What an input wait delivered: only data makes it `starved`.
pub trait Delivery {
    fn is_data(&self) -> bool;
}

impl<T, E> Delivery for Result<T, E> {
    fn is_data(&self) -> bool {
        self.is_ok()
    }
}

/// A chunk of items (`ready_chunks`) delivered data if any item is data.
impl<T: Delivery> Delivery for Vec<T> {
    fn is_data(&self) -> bool {
        self.iter().any(Delivery::is_data)
    }
}

/// An enclosing operator's view of the meter it reserved. Its own work runs
/// alongside the inner operators' tasks, so it is timed on its own `busy`
/// timeline. Without a meter every method is a plain pass-through.
#[derive(Debug, Default)]
pub struct ReservedMeter(Option<ReservedTimers>);

#[derive(Debug)]
struct ReservedTimers {
    busy: Arc<NodeTime>,
    send: Option<Arc<NodeTime>>,
}

impl ReservedMeter {
    /// The operator's own work: `busy`.
    pub async fn work<F: Future>(&self, work: F) -> F::Output {
        let _span = self.0.as_ref().map(|timers| Span::open(&timers.busy));
        work.await
    }

    /// A send along the edge: `blocked`.
    pub async fn send<F: Future>(&self, send: F) -> F::Output {
        let _span = self
            .0
            .as_ref()
            .and_then(|timers| timers.send.as_deref())
            .map(Span::open);
        send.await
    }
}

/// An open span, closed when the timed future completes or is dropped.
struct Span<'a>(&'a NodeTime);

impl<'a> Span<'a> {
    fn open(time: &'a NodeTime) -> Self {
        time.open();
        Span(time)
    }
}

impl Drop for Span<'_> {
    fn drop(&mut self) {
        self.0.close();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bracket {
    /// A wait for input: `starved`.
    Input,
    /// A wait for input that ended the input: teardown, and no work follows.
    Exhausted,
    /// A wait for input that ended in an error: teardown; handling it is work.
    Failed,
    /// An output send: the edge's `blocked`.
    Send,
    /// In no state.
    Untimed,
}

/// Ends a bracketed await when it completes or is cancelled. A later claim
/// that took over meanwhile owns the clocks, so the guard then does nothing.
struct WaitGuard<'a> {
    handle: &'a TaskWaitHandle,
    end: Bracket,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        let Some(timers) = self.handle.timers() else {
            return;
        };
        match self.end {
            Bracket::Input => timers.starved.close(),
            Bracket::Failed => timers.starved.discard(),
            Bracket::Exhausted => {
                timers.starved.discard();
                return;
            }
            Bracket::Send => {
                if let Some(send) = &timers.send {
                    send.close();
                }
            }
            Bracket::Untimed => {}
        }
        timers.busy.open();
    }
}

impl Drop for TaskWaitHandle {
    fn drop(&mut self) {
        if let Some(timers) = self.timers() {
            timers.busy.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::recorder::get_metrics_recorder;
    use datafusion::prelude::SessionContext;

    fn install(ctx: &TaskContext) -> (Arc<TaskContext>, Arc<TaskWaitMeter>) {
        TaskWaitMeter::install(ctx, get_metrics_recorder(), "task_wait::test", None)
    }

    fn claimed_meter() -> (Arc<TaskWaitMeter>, TaskWaitHandle) {
        let ctx = SessionContext::new().task_ctx();
        let (child, meter) = install(&ctx);
        let handle = TaskWaitMeter::claim(&child);
        assert!(meter.is_claimed());
        (meter, handle)
    }

    /// Operators below a wrapper look functions up by name
    /// (`information_schema.routines` lists them from the task context).
    #[test]
    fn the_child_context_keeps_the_parents_functions() {
        let ctx = SessionContext::new().task_ctx();
        let (child, _meter) = install(&ctx);
        assert!(!ctx.scalar_functions().is_empty());
        assert_eq!(
            child
                .scalar_functions()
                .keys()
                .collect::<std::collections::BTreeSet<_>>(),
            ctx.scalar_functions().keys().collect()
        );
        assert_eq!(
            child.higher_order_functions().len(),
            ctx.higher_order_functions().len()
        );
        assert_eq!(
            child.aggregate_functions().len(),
            ctx.aggregate_functions().len()
        );
        assert_eq!(child.window_functions().len(), ctx.window_functions().len());
    }

    #[test]
    fn claim_without_installed_meter_is_a_pass_through() {
        let ctx = SessionContext::new().task_ctx();
        assert!(TaskWaitMeter::claim(&ctx).0.is_none());
    }

    #[tokio::test]
    async fn a_later_claim_takes_over_the_timeline() {
        let ctx = SessionContext::new().task_ctx();
        let (child, meter) = install(&ctx);
        let first = TaskWaitMeter::claim(&child);
        let second = TaskWaitMeter::claim(&child);
        // The first phase finishes after the second started: its brackets and
        // drop must not stop the second phase's busy clock.
        first
            .send(tokio::time::sleep(Duration::from_millis(1)))
            .await;
        drop(first);
        let before = meter.totals().busy;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(meter.totals().busy >= before + Duration::from_millis(20));
        drop(second);
    }

    /// A wait an older claim started before a later claim must not touch the
    /// later claim's clocks when it ends.
    #[tokio::test]
    async fn an_older_claims_wait_ending_late_leaves_the_new_timeline_alone() {
        let ctx = SessionContext::new().task_ctx();
        let (child, meter) = install(&ctx);
        let first = TaskWaitMeter::claim(&child);
        let (release, input) = tokio::sync::oneshot::channel::<()>();
        let mut first_wait = std::pin::pin!(first.wait_input(input));
        assert!(futures::poll!(first_wait.as_mut()).is_pending());

        let second = TaskWaitMeter::claim(&child);
        let mut second_send =
            std::pin::pin!(second.send(tokio::time::sleep(Duration::from_millis(60))));
        assert!(futures::poll!(second_send.as_mut()).is_pending());
        release.send(()).unwrap();
        let _ = first_wait.await;
        let before_send_ends = meter.totals();
        second_send.await;

        let after = meter.totals();
        assert!(
            after.busy < before_send_ends.busy + Duration::from_millis(30),
            "the later claim's send must not read as busy: {before_send_ends:?} -> {after:?}"
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

    /// Only a reserved meter is shared with inner wrappers. A claimed one
    /// belongs to its node: the node's input wrapper installs its own.
    #[test]
    fn only_a_reserved_meter_is_shared() {
        let ctx = SessionContext::new().task_ctx();
        let (claimed, _meter) = install(&ctx);
        let _handle = TaskWaitMeter::claim(&claimed);
        assert!(TaskWaitMeter::reserved(&claimed).is_none());

        let (reserved, meter) = install(&ctx);
        TaskWaitMeter::reserve(&reserved);
        let shared = TaskWaitMeter::reserved(&reserved).expect("reserved meter");
        assert!(Arc::ptr_eq(&shared, &meter));
    }

    #[tokio::test]
    async fn splits_input_wait_work_and_send_wait() {
        let (meter, handle) = claimed_meter();
        let input = futures::stream::iter([Ok::<_, ()>(1)]).then(|i| async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            i
        });
        let mut input = std::pin::pin!(input);

        assert_eq!(handle.next(&mut input).await, Some(Ok(1)));
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

    /// The send wait is the edge's `blocked`, unless the meter is reserved:
    /// an inner operator sends into the enclosing one, not along the edge.
    #[tokio::test]
    async fn a_send_wait_is_blocked_unless_the_meter_is_reserved() {
        let ctx = SessionContext::new().task_ctx();
        let blocked =
            || NodeTime::wall(get_metrics_recorder(), "task_wait::test", "blocked", "down");

        let edge = blocked();
        let (child, _meter) = TaskWaitMeter::install(
            &ctx,
            get_metrics_recorder(),
            "task_wait::test",
            Some(Arc::clone(&edge)),
        );
        let handle = TaskWaitMeter::claim(&child);
        handle
            .send(tokio::time::sleep(Duration::from_millis(30)))
            .await;
        assert!(
            edge.total() >= Duration::from_millis(30),
            "{:?}",
            edge.total()
        );

        let edge = blocked();
        let (child, _meter) = TaskWaitMeter::install(
            &ctx,
            get_metrics_recorder(),
            "task_wait::test",
            Some(Arc::clone(&edge)),
        );
        let reserved = TaskWaitMeter::reserve(&child);
        let handle = TaskWaitMeter::claim(&child);
        handle
            .send(tokio::time::sleep(Duration::from_millis(30)))
            .await;
        assert_eq!(edge.total(), Duration::ZERO);

        // The reserving operator's own work and edge sends are its node's.
        reserved
            .work(tokio::time::sleep(Duration::from_millis(30)))
            .await;
        reserved
            .send(tokio::time::sleep(Duration::from_millis(30)))
            .await;
        assert!(
            edge.total() >= Duration::from_millis(30),
            "{:?}",
            edge.total()
        );
        let own_busy = reserved.0.as_ref().expect("a meter").busy.total();
        assert!(own_busy >= Duration::from_millis(30), "{own_busy:?}");
    }

    /// A wait on the sinks (terminal checkpoint finalize) is no send along
    /// the edge and no work of the node.
    #[tokio::test]
    async fn an_untimed_wait_is_in_no_state() {
        let ctx = SessionContext::new().task_ctx();
        let edge = NodeTime::wall(get_metrics_recorder(), "task_wait::test", "blocked", "down");
        let (child, meter) = TaskWaitMeter::install(
            &ctx,
            get_metrics_recorder(),
            "task_wait::test",
            Some(Arc::clone(&edge)),
        );
        let handle = TaskWaitMeter::claim(&child);
        handle
            .untimed(tokio::time::sleep(Duration::from_millis(100)))
            .await;
        let totals = meter.totals();
        assert_eq!(edge.total(), Duration::ZERO);
        assert!(totals.busy < Duration::from_millis(50), "{totals:?}");
        assert_eq!(totals.starved, Duration::ZERO);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            meter.totals().busy >= totals.busy + Duration::from_millis(20),
            "the busy clock restarts after the wait"
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
    async fn a_cancelled_input_wait_still_counts_as_starved() {
        let (meter, handle) = claimed_meter();
        tokio::select! {
            _ = handle.wait_input(futures::future::pending::<()>()) => unreachable!(),
            _ = tokio::time::sleep(Duration::from_millis(30)) => {}
        }
        let after_cancel = meter.totals();
        assert!(
            after_cancel.starved >= Duration::from_millis(30),
            "{after_cancel:?}"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            meter.totals().busy >= after_cancel.busy + Duration::from_millis(30),
            "the busy clock restarts after the cancelled wait"
        );
    }

    /// A wait that ends in an error is teardown, not starvation, as in
    /// `WakeTimed`; handling the error is the task's work.
    #[tokio::test]
    async fn a_wait_ending_in_an_error_is_not_starved() {
        let (meter, handle) = claimed_meter();
        let mut input = std::pin::pin!(futures::stream::once(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Err::<(), _>("upstream failed")
        }));
        assert!(handle.next(&mut input).await.unwrap().is_err());
        let at_error = meter.totals();
        // A flusher tick inside the wait may have banked part of it.
        assert!(
            at_error.starved < Duration::from_millis(100),
            "{at_error:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(meter.totals().busy >= at_error.busy + Duration::from_millis(20));
    }

    #[tokio::test]
    async fn end_of_input_stops_the_busy_clock() {
        let (meter, handle) = claimed_meter();
        let mut input = futures::stream::empty::<Result<(), ()>>();
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
