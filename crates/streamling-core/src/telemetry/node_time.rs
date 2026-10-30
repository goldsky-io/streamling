use crate::telemetry::recorder::MetricsRecorder;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet, Time};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

/// How often banked time and open spans are reported.
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// One `node_wait{state, downstream_id}` series of one node stream.
///
/// A span is reported as it accrues: a process-wide flusher banks every open
/// span each [`FLUSH_INTERVAL`], so a stall shows on the dashboard while it
/// lasts instead of as one spike when it ends (or never, if the pod dies
/// first). Only the flusher and `Drop` emit: a span's close just banks, so the
/// data path never takes the recorder's locks. Whole milliseconds are emitted
/// and the sub-ms remainder carries.
pub struct NodeTime {
    recorder: Arc<MetricsRecorder>,
    metadata_id: String,
    state: &'static str,
    downstream_id: String,
    measure: Measure,
    clock: Mutex<Clock>,
}

impl std::fmt::Debug for NodeTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeTime")
            .field("metadata_id", &self.metadata_id)
            .field("state", &self.state)
            .field("downstream_id", &self.downstream_id)
            .finish()
    }
}

#[derive(Debug)]
enum Measure {
    /// Wall time of open spans, minus the node's own work done inside them
    /// when `exclude` is set.
    Wall { exclude: Option<OwnWork> },
    /// The node's own work, read from its DataFusion compute counters.
    Work(OwnWork),
}

#[derive(Debug, Default)]
struct Clock {
    open_since: Option<Instant>,
    /// Nothing accrues from here until `resume`, whichever spans open or
    /// close meanwhile; with the work counter then.
    paused_at: Option<(Instant, u64)>,
    /// A poll that runs the node's own work started here, with the work
    /// counter then. Its compute is recorded only when it ends, so nothing
    /// past its start is banked before that.
    poll: Option<(Instant, u64)>,
    work_mark: u64,
    banked: Duration,
    /// Banked by flushes in the open span: work recorded later in the span
    /// can take it back.
    span_banked: Duration,
    emitted_ms: u64,
}

/// Cumulative `elapsed_compute` of one partition of a node's DataFusion subtree.
#[derive(Debug, Clone)]
pub struct OwnWork {
    /// This partition's compute timers, resolved once: they are shared
    /// counters, so reading them stays live.
    times: Arc<[Time]>,
}

impl OwnWork {
    /// Metrics without a partition are counted once, by partition 0. Exact
    /// when the subtree keeps its input partitioning; a repartition inside the
    /// subtree spreads one partition's work across others.
    pub fn new(metrics: MetricsSet, partition: usize) -> Self {
        let times = metrics
            .iter()
            .filter(|metric| metric.partition().unwrap_or(0) == partition)
            .filter_map(|metric| match metric.value() {
                MetricValue::ElapsedCompute(time) => Some(time.clone()),
                MetricValue::Time { name, time } if name == "elapsed_compute" => Some(time.clone()),
                _ => None,
            })
            .collect();
        Self { times }
    }

    pub(crate) fn nanos(&self) -> u64 {
        self.times
            .iter()
            .map(|time| time.value() as u64)
            .fold(0u64, u64::saturating_add)
    }
}

impl NodeTime {
    pub fn wall(
        recorder: Arc<MetricsRecorder>,
        metadata_id: &str,
        state: &'static str,
        downstream_id: &str,
    ) -> Arc<Self> {
        Self::register(
            recorder,
            metadata_id,
            state,
            downstream_id,
            Measure::Wall { exclude: None },
        )
    }

    /// Wall time of open spans minus the `work` done while they are open: the
    /// span of a poll that also runs the node's own operators.
    pub fn wall_excluding(
        recorder: Arc<MetricsRecorder>,
        metadata_id: &str,
        state: &'static str,
        work: OwnWork,
    ) -> Arc<Self> {
        Self::register(
            recorder,
            metadata_id,
            state,
            "",
            Measure::Wall {
                exclude: Some(work),
            },
        )
    }

    /// Accrues `work` continuously; no spans.
    pub fn work(
        recorder: Arc<MetricsRecorder>,
        metadata_id: &str,
        state: &'static str,
        work: OwnWork,
    ) -> Arc<Self> {
        let timer = Self::register(recorder, metadata_id, state, "", Measure::Work(work));
        timer.clock.lock().work_mark = timer.work_nanos();
        timer
    }

    fn register(
        recorder: Arc<MetricsRecorder>,
        metadata_id: &str,
        state: &'static str,
        downstream_id: &str,
        measure: Measure,
    ) -> Arc<Self> {
        let timer = Arc::new(Self {
            recorder,
            metadata_id: metadata_id.to_string(),
            state,
            downstream_id: downstream_id.to_string(),
            measure,
            clock: Mutex::new(Clock::default()),
        });
        register_live(&timer);
        timer
    }

    /// Start a span. No-op while one is open.
    pub fn open(&self) {
        let mut clock = self.clock.lock();
        if clock.open_since.is_none() {
            let (at, work) = clock
                .paused_at
                .unwrap_or_else(|| (Instant::now(), self.work_nanos()));
            clock.open_since = Some(at);
            clock.work_mark = work;
            clock.span_banked = Duration::ZERO;
        }
    }

    /// End the open span; the next flush reports it.
    pub fn close(&self) {
        FLUSHER.ensure_started(spawn_flusher);
        let mut clock = self.clock.lock();
        self.bank(&mut clock);
        clock.open_since = None;
    }

    /// Stop accruing until `resume`. An open span stays open: whether it is
    /// reported (`close`) or dropped (`discard`) is decided when it ends.
    /// Called from a waker, on whatever thread wakes the task.
    pub(crate) fn pause(&self) {
        let mut clock = self.clock.lock();
        if clock.paused_at.is_none() {
            clock.paused_at = Some((Instant::now(), self.work_nanos()));
        }
    }

    /// A poll that may run the node's own work starts. No-op unless own work
    /// is excluded.
    pub(crate) fn poll_started(&self) {
        if let Measure::Wall { exclude: Some(_) } = self.measure {
            self.clock.lock().poll = Some((Instant::now(), self.work_nanos()));
        }
    }

    pub(crate) fn poll_ended(&self) {
        self.clock.lock().poll = None;
    }

    /// Accrue again: neither the pause nor the work done during it is part
    /// of the open span.
    pub(crate) fn resume(&self) {
        let mut clock = self.clock.lock();
        if let Some((paused_at, paused_work)) = clock.paused_at.take() {
            if let Some(since) = clock.open_since {
                clock.open_since = Some(since + paused_at.elapsed());
            }
            clock.work_mark += self.work_nanos().saturating_sub(paused_work);
        }
    }

    /// End the open span without reporting its unflushed part: the wait ended
    /// in end-of-stream or an error, so it was teardown, not a wait for data.
    pub fn discard(&self) {
        self.clock.lock().open_since = None;
    }

    /// Everything accrued so far, the open span included. Not monotonic across
    /// [`Self::discard`], so readers keep a high-water mark.
    pub fn total(&self) -> Duration {
        let clock = self.clock.lock();
        let (now, work) = self.accrued_to(&clock);
        let (add, take_back) = self.unbanked(&clock, now, work);
        clock.banked + add - take_back
    }

    fn work_nanos(&self) -> u64 {
        match &self.measure {
            Measure::Wall {
                exclude: Some(work),
            }
            | Measure::Work(work) => work.nanos(),
            Measure::Wall { exclude: None } => 0,
        }
    }

    /// Unbanked time up to `now`, given the work counter read at `now`, and
    /// the part of this span's flushed time it takes back. DataFusion records
    /// compute when a poll ends, so a flush in the middle of a long poll
    /// banked that compute as wall time.
    fn unbanked(&self, clock: &Clock, now: Instant, work: u64) -> (Duration, Duration) {
        let work_done = Duration::from_nanos(work.saturating_sub(clock.work_mark));
        match &self.measure {
            Measure::Wall { .. } => {
                clock
                    .open_since
                    .map_or((Duration::ZERO, Duration::ZERO), |since| {
                        let wall = now - since;
                        (
                            wall.saturating_sub(work_done),
                            work_done.saturating_sub(wall).min(clock.span_banked),
                        )
                    })
            }
            Measure::Work(_) => (work_done, Duration::ZERO),
        }
    }

    /// Bank the unbanked time. The work counter is read under the clock lock:
    /// a read taken before it could be older than `work_mark` and rewind it.
    fn bank(&self, clock: &mut Clock) {
        let (now, work) = self.accrued_to(clock);
        if clock.open_since.is_some_and(|since| since > now) {
            // The span opened after a running poll started: nothing accrued.
            return;
        }
        let (add, take_back) = self.unbanked(clock, now, work);
        clock.banked = clock.banked + add - take_back;
        clock.span_banked = clock.span_banked + add - take_back;
        if clock.open_since.is_some() {
            clock.open_since = Some(now);
        }
        clock.work_mark = work;
    }

    /// The end of the time accrued so far, with the work counter then:
    /// nothing accrues while paused, nor past the start of a running poll.
    fn accrued_to(&self, clock: &Clock) -> (Instant, u64) {
        let end = clock
            .paused_at
            .unwrap_or_else(|| (Instant::now(), self.work_nanos()));
        match clock.poll {
            Some(poll) if poll.0 < end.0 => poll,
            _ => end,
        }
    }

    fn take_whole_millis(clock: &mut Clock) -> u64 {
        let total_ms = u64::try_from(clock.banked.as_millis()).unwrap_or(u64::MAX);
        // A take-back can drop `banked` below what the counter already shows;
        // later time then fills that gap before anything more is emitted.
        let ms = total_ms.saturating_sub(clock.emitted_ms);
        clock.emitted_ms = clock.emitted_ms.max(total_ms);
        ms
    }

    fn emit(&self, ms: u64) {
        if ms > 0 {
            self.recorder.record_count_w_tags(
                "node_wait",
                ms,
                vec![
                    ("state", self.state),
                    ("downstream_id", &self.downstream_id),
                ],
                &self.metadata_id,
            );
        }
    }
}

impl Flush for NodeTime {
    fn flush(&self) {
        let ms = {
            let mut clock = self.clock.lock();
            self.bank(&mut clock);
            Self::take_whole_millis(&mut clock)
        };
        self.emit(ms);
    }
}

impl Drop for NodeTime {
    /// Work already done is reported. An open wall span is not: it ends in
    /// cancellation, so it was teardown, like [`Self::discard`].
    fn drop(&mut self) {
        if let Measure::Work(_) = self.measure {
            let mut clock = self.clock.lock();
            self.bank(&mut clock);
        }
        let ms = Self::take_whole_millis(self.clock.get_mut());
        self.emit(ms);
    }
}

/// A series the background flusher reports.
trait Flush: Send + Sync {
    fn flush(&self);
}

/// A count banked on the data path and reported with `node_wait`: by the
/// flusher each [`FLUSH_INTERVAL`] and on `Drop`, so the data path never takes
/// the recorder's locks.
pub struct BankedCount {
    recorder: Arc<MetricsRecorder>,
    name: &'static str,
    metadata_id: String,
    pending: AtomicU64,
}

impl BankedCount {
    pub fn new(recorder: Arc<MetricsRecorder>, name: &'static str, metadata_id: &str) -> Arc<Self> {
        let count = Arc::new(Self {
            recorder,
            name,
            metadata_id: metadata_id.to_string(),
            pending: AtomicU64::new(0),
        });
        register_live(&count);
        count
    }

    pub fn add(&self, n: u64) {
        if n > 0 {
            FLUSHER.ensure_started(spawn_flusher);
            self.pending.fetch_add(n, Ordering::Relaxed);
        }
    }
}

impl Flush for BankedCount {
    fn flush(&self) {
        let n = self.pending.swap(0, Ordering::Relaxed);
        if n > 0 {
            self.recorder
                .record_count_w_tags(self.name, n, vec![], &self.metadata_id);
        }
    }
}

impl Drop for BankedCount {
    fn drop(&mut self) {
        self.flush();
    }
}

/// The live series, flushed by one background thread.
static LIVE: Mutex<Vec<Weak<dyn Flush>>> = Mutex::new(Vec::new());

static FLUSHER: Flusher = Flusher::new();

/// Starts the flusher thread. Every series registers at plan start, so a
/// start that fails then is retried from `close` and `add`: one atomic load once it
/// runs, and at most one spawn attempt per [`FLUSH_INTERVAL`] until it does.
struct Flusher {
    started: AtomicBool,
    /// Milliseconds since [`Flusher::clock`] before which no attempt is made.
    next_attempt_ms: AtomicU64,
}

impl Flusher {
    const fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            next_attempt_ms: AtomicU64::new(0),
        }
    }

    fn clock() -> u64 {
        static START: OnceLock<Instant> = OnceLock::new();
        u64::try_from(START.get_or_init(Instant::now).elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn ensure_started(&self, spawn: fn() -> std::io::Result<()>) {
        if self.started.load(Ordering::Acquire) {
            return;
        }
        let now = Self::clock();
        let next = self.next_attempt_ms.load(Ordering::Acquire);
        let retry_at = now.saturating_add(FLUSH_INTERVAL.as_millis() as u64);
        if now < next
            || self
                .next_attempt_ms
                .compare_exchange(next, retry_at, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return;
        }
        match spawn() {
            Ok(()) => self.started.store(true, Ordering::Release),
            Err(e) => tracing::warn!(
                "node-time flusher not started, retrying; until then node_wait and node_empty_batches report only when each stream ends: {e}"
            ),
        }
    }
}

fn spawn_flusher() -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("node-time-flush".into())
        .spawn(|| {
            loop {
                std::thread::sleep(FLUSH_INTERVAL);
                flush_live();
            }
        })
        .map(drop)
}

/// Track `series` for the flusher, starting the flusher if it is not running.
fn register_live<T: Flush + 'static>(series: &Arc<T>) {
    FLUSHER.ensure_started(spawn_flusher);
    let series: Weak<dyn Flush> = Arc::downgrade(series) as Weak<T>;
    let mut live = LIVE.lock();
    live.retain(|series| series.strong_count() > 0);
    live.push(series);
}

/// Holds the flusher off while the guard lives. Take it after the series
/// under test registered: registering needs the same lock.
#[cfg(test)]
pub(crate) fn hold_flusher() -> parking_lot::MutexGuard<'static, impl Sized> {
    LIVE.lock()
}

/// Report every live series: banked time and open spans, banked counts.
pub(crate) fn flush_live() {
    let live: Vec<Arc<dyn Flush>> = LIVE.lock().iter().filter_map(Weak::upgrade).collect();
    for series in live {
        series.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::recorder::get_metrics_recorder;
    use crate::telemetry::recorder::test_support::{
        TEST_LOCK, emitted_node_wait_ms, init_recorder_with_node, node_wait_ms,
    };
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};

    #[test]
    fn open_span_is_reported_before_it_closes() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let id = "node_time::open_span";
        init_recorder_with_node(id);
        let timer = NodeTime::wall(get_metrics_recorder(), id, "blocked", "down");

        timer.open();
        std::thread::sleep(FLUSH_INTERVAL * 2 + Duration::from_millis(300));
        // No manual flush: the background flusher reports the open span.
        let while_open = emitted_node_wait_ms(id, "blocked", Some("down"));
        assert!(
            while_open >= FLUSH_INTERVAL.as_millis() as u64,
            "a stalled wait must show before it ends, got {while_open}ms"
        );

        timer.close();
        let closed = node_wait_ms(id, "blocked", Some("down"));
        assert!(
            (2_300..3_000).contains(&closed),
            "closed span = its wall time, got {closed}ms"
        );
    }

    #[test]
    fn discard_drops_only_the_unflushed_part() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let id = "node_time::discard";
        init_recorder_with_node(id);
        let timer = NodeTime::wall(get_metrics_recorder(), id, "starved", "");

        let started = Instant::now();
        timer.open();
        std::thread::sleep(Duration::from_millis(20));
        timer.discard();
        // Only a flusher tick that landed inside the span banked part of it.
        let flushed = timer.total();
        assert!(flushed < started.elapsed(), "{flushed:?}");
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(timer.total(), flushed, "a discarded span stops accruing");
        drop(timer);
        assert_eq!(
            node_wait_ms(id, "starved", None),
            flushed.as_millis() as u64
        );
    }

    #[test]
    fn wall_excluding_subtracts_own_work_inside_the_span() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let id = "node_time::excluding";
        init_recorder_with_node(id);
        let plan_metrics = ExecutionPlanMetricsSet::new();
        let compute = MetricBuilder::new(&plan_metrics).elapsed_compute(0);
        let work = OwnWork::new(plan_metrics.clone_inner(), 0);
        let starved = NodeTime::wall_excluding(get_metrics_recorder(), id, "starved", work.clone());
        let busy = NodeTime::work(get_metrics_recorder(), id, "busy", work);

        let started = Instant::now();
        starved.open();
        std::thread::sleep(Duration::from_millis(40));
        compute.add_duration(Duration::from_millis(30));
        starved.close();
        let span = started.elapsed();
        // A loaded runner can overshoot the sleep, so bound by the real span.
        let starved_total = starved.total();
        assert!(
            starved_total >= Duration::from_millis(10)
                && starved_total + Duration::from_millis(30) <= span,
            "span {span:?} - 30ms compute, got {starved_total:?}"
        );
        assert_eq!(busy.total(), Duration::from_millis(30));
    }

    /// DataFusion records compute when a poll ends, so a flush in the middle
    /// of a long poll banks compute as wall time. The compute recorded later
    /// in the same span takes that back.
    #[test]
    fn work_recorded_after_a_flush_takes_back_the_flushed_part() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let id = "node_time::flush_mid_work";
        init_recorder_with_node(id);
        let plan_metrics = ExecutionPlanMetricsSet::new();
        let compute = MetricBuilder::new(&plan_metrics).elapsed_compute(0);
        let starved = NodeTime::wall_excluding(
            get_metrics_recorder(),
            id,
            "starved",
            OwnWork::new(plan_metrics.clone_inner(), 0),
        );

        let started = Instant::now();
        starved.open();
        std::thread::sleep(Duration::from_millis(20));
        starved.flush();
        std::thread::sleep(Duration::from_millis(20));
        // The whole span was one poll of compute.
        compute.add_duration(started.elapsed());
        starved.close();
        // Only the µs between recording the compute and the close remain.
        assert!(
            starved.total() < Duration::from_millis(1),
            "{:?}",
            starved.total()
        );

        // The take-back is limited to this span: earlier waits stay.
        starved.open();
        std::thread::sleep(Duration::from_millis(10));
        starved.close();
        let wait = starved.total();
        starved.open();
        compute.add_duration(Duration::from_secs(1));
        starved.close();
        assert_eq!(starved.total(), wait);
    }

    /// A pause drops its wall time from the span, so the work done during it
    /// must not be subtracted either.
    #[test]
    fn work_during_a_pause_is_not_subtracted() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let plan_metrics = ExecutionPlanMetricsSet::new();
        let compute = MetricBuilder::new(&plan_metrics).elapsed_compute(0);
        let starved = NodeTime::wall_excluding(
            get_metrics_recorder(),
            "node_time::pause_work",
            "starved",
            OwnWork::new(plan_metrics.clone_inner(), 0),
        );

        let started = Instant::now();
        starved.open();
        std::thread::sleep(Duration::from_millis(80));
        let waited = started.elapsed();
        starved.pause();
        compute.add_duration(Duration::from_millis(300));
        starved.resume();
        starved.close();
        let total = starved.total();
        assert!(
            total >= waited && total < waited + Duration::from_millis(20),
            "waited {waited:?}, got {total:?}"
        );
    }

    /// A failed flusher start is retried later, at most once per interval,
    /// and never again once it runs.
    #[test]
    fn a_failed_flusher_start_is_retried() {
        static ATTEMPTS: AtomicU64 = AtomicU64::new(0);
        fn fail() -> std::io::Result<()> {
            ATTEMPTS.fetch_add(1, Ordering::Relaxed);
            Err(std::io::Error::other("no threads"))
        }
        fn succeed() -> std::io::Result<()> {
            ATTEMPTS.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        let flusher = Flusher::new();

        flusher.ensure_started(fail);
        flusher.ensure_started(succeed);
        assert_eq!(
            ATTEMPTS.load(Ordering::Relaxed),
            1,
            "one attempt per interval"
        );

        std::thread::sleep(FLUSH_INTERVAL + Duration::from_millis(10));
        flusher.ensure_started(succeed);
        flusher.ensure_started(fail);
        assert_eq!(ATTEMPTS.load(Ordering::Relaxed), 2);
        std::thread::sleep(FLUSH_INTERVAL + Duration::from_millis(10));
        flusher.ensure_started(fail);
        assert_eq!(
            ATTEMPTS.load(Ordering::Relaxed),
            2,
            "a running flusher is not restarted"
        );
    }

    #[test]
    fn other_partitions_work_is_not_excluded() {
        let plan_metrics = ExecutionPlanMetricsSet::new();
        MetricBuilder::new(&plan_metrics)
            .elapsed_compute(1)
            .add_duration(Duration::from_millis(5));
        MetricBuilder::new(&plan_metrics)
            .elapsed_compute(0)
            .add_duration(Duration::from_millis(3));
        assert_eq!(
            OwnWork::new(plan_metrics.clone_inner(), 0).nanos(),
            3_000_000
        );
        assert_eq!(
            OwnWork::new(plan_metrics.clone_inner(), 1).nanos(),
            5_000_000
        );
    }
}
