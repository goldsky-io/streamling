use crate::telemetry::recorder::MetricsRecorder;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use parking_lot::Mutex;
use std::sync::{Arc, Once, Weak};
use std::time::{Duration, Instant};

/// How often open spans are reported while they are still running.
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// One `node_wait{state, downstream_id}` series of one node stream.
///
/// A span is reported as it accrues: a process-wide flusher banks every open
/// span each [`FLUSH_INTERVAL`], so a stall shows on the dashboard while it
/// lasts instead of as one spike when it ends (or never, if the pod dies
/// first). Whole milliseconds are emitted and the sub-ms remainder carries.
pub struct NodeTime {
    recorder: Arc<MetricsRecorder>,
    metadata_id: String,
    state: &'static str,
    downstream_id: String,
    measure: Measure,
    clock: Mutex<Clock>,
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
    metrics: MetricsSet,
    partition: usize,
}

impl OwnWork {
    pub fn new(metrics: MetricsSet, partition: usize) -> Self {
        Self { metrics, partition }
    }

    /// Metrics without a partition are counted once, by partition 0. Exact
    /// when the subtree keeps its input partitioning; a repartition inside the
    /// subtree spreads one partition's work across others.
    fn nanos(&self) -> u64 {
        self.metrics
            .iter()
            .filter(|metric| metric.partition().unwrap_or(0) == self.partition)
            .map(|metric| match metric.value() {
                MetricValue::ElapsedCompute(time) => time.value() as u64,
                MetricValue::Time { name, time } if name == "elapsed_compute" => {
                    time.value() as u64
                }
                _ => 0,
            })
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
        flusher().lock().push(Arc::downgrade(&timer));
        timer
    }

    /// Start a span. No-op while one is open.
    pub fn open(&self) {
        let mut clock = self.clock.lock();
        if clock.open_since.is_none() {
            clock.open_since = Some(Instant::now());
            clock.work_mark = self.work_nanos();
            clock.span_banked = Duration::ZERO;
        }
    }

    /// End the open span and report it.
    pub fn close(&self) {
        let ms = {
            let mut clock = self.clock.lock();
            self.bank(&mut clock);
            clock.open_since = None;
            Self::take_whole_millis(&mut clock)
        };
        self.emit(ms);
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
        let (add, take_back) = self.unbanked(&clock, Instant::now(), self.work_nanos());
        clock.banked + add - take_back
    }

    fn flush(&self) {
        let ms = {
            let mut clock = self.clock.lock();
            self.bank(&mut clock);
            Self::take_whole_millis(&mut clock)
        };
        self.emit(ms);
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
        let now = Instant::now();
        let work = self.work_nanos();
        let (add, take_back) = self.unbanked(clock, now, work);
        clock.banked = clock.banked + add - take_back;
        clock.span_banked = clock.span_banked + add - take_back;
        if clock.open_since.is_some() {
            clock.open_since = Some(now);
        }
        clock.work_mark = work;
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

/// The live timers, flushed by one background thread started on first use.
fn flusher() -> &'static Mutex<Vec<Weak<NodeTime>>> {
    static TIMERS: Mutex<Vec<Weak<NodeTime>>> = Mutex::new(Vec::new());
    static STARTED: Once = Once::new();
    STARTED.call_once(|| {
        std::thread::Builder::new()
            .name("node-time-flush".into())
            .spawn(|| {
                loop {
                    std::thread::sleep(FLUSH_INTERVAL);
                    let live: Vec<Arc<NodeTime>> = {
                        let mut timers = TIMERS.lock();
                        timers.retain(|timer| timer.strong_count() > 0);
                        timers.iter().filter_map(Weak::upgrade).collect()
                    };
                    for timer in live {
                        timer.flush();
                    }
                }
            })
            .expect("spawn node-time flusher");
    });
    &TIMERS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::recorder::get_metrics_recorder;
    use crate::telemetry::recorder::test_support::{
        TEST_LOCK, init_recorder_with_node, node_wait_ms,
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
        let while_open = node_wait_ms(id, "blocked", Some("down"));
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
