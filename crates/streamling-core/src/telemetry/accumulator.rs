use std::time::Duration;

/// Accumulates a duration and drains it as whole milliseconds, carrying the
/// sub-millisecond remainder forward so many small spans aren't each truncated
/// to zero (which would undercount at high throughput).
///
/// Shared by every `node_wait` emission: `blocked` (yield->resume suspension in
/// `WrappingExec`, and blocked-send time in `BroadcastStream`) and `starved`
/// (upstream input wait). Side-effect-free so the math is unit testable with
/// hand-fed durations.
#[derive(Debug, Default)]
pub struct MillisAccumulator {
    total: Duration,
}

impl MillisAccumulator {
    /// Add a span to the running total. A zero span is a no-op.
    pub fn add(&mut self, span: Duration) {
        self.total += span;
    }

    /// Drain the accrued time as whole milliseconds, retaining the
    /// sub-millisecond remainder. Returns 0 until at least 1ms has accrued.
    pub fn take_whole_millis(&mut self) -> u64 {
        // `as u64` truncation of the u128 `as_millis()` is safe: overflowing u64
        // ms takes ~584 million years, and the remainder is drained each call.
        let whole = self.total.as_millis() as u64;
        if whole > 0 {
            self.total -= Duration::from_millis(whole);
        }
        whole
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_accumulator_yields_zero() {
        let mut acc = MillisAccumulator::default();
        assert_eq!(acc.take_whole_millis(), 0);
    }

    #[test]
    fn accumulates_across_adds() {
        let mut acc = MillisAccumulator::default();
        acc.add(Duration::from_millis(5));
        acc.add(Duration::from_millis(7));
        assert_eq!(acc.take_whole_millis(), 12);
        // Fully drained.
        assert_eq!(acc.take_whole_millis(), 0);
    }

    #[test]
    fn retains_sub_millisecond_remainder() {
        let mut acc = MillisAccumulator::default();
        // 1.5ms: only 1 whole ms drains, 0.5ms is retained.
        acc.add(Duration::from_micros(1_500));
        assert_eq!(acc.take_whole_millis(), 1);
        assert_eq!(acc.take_whole_millis(), 0);
        // Another 0.5ms pushes the retained remainder over 1ms.
        acc.add(Duration::from_micros(500));
        assert_eq!(acc.take_whole_millis(), 1);
    }

    #[test]
    fn many_sub_millisecond_adds_do_not_vanish() {
        let mut acc = MillisAccumulator::default();
        // Each 0.4ms add truncates to 0ms in isolation.
        acc.add(Duration::from_micros(400));
        assert_eq!(acc.take_whole_millis(), 0);
        acc.add(Duration::from_micros(400));
        assert_eq!(acc.take_whole_millis(), 0);
        // The third crosses 1ms, so a whole millisecond is now emitted.
        acc.add(Duration::from_micros(400));
        assert_eq!(acc.take_whole_millis(), 1);
    }

    #[test]
    fn emits_whole_millis_and_keeps_fraction() {
        let mut acc = MillisAccumulator::default();
        acc.add(Duration::from_micros(3_700));
        assert_eq!(acc.take_whole_millis(), 3);
        // 0.7ms remainder carried; +0.4ms = 1.1ms -> 1 whole ms emitted.
        acc.add(Duration::from_micros(400));
        assert_eq!(acc.take_whole_millis(), 1);
    }
}
