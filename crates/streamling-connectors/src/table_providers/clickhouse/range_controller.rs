use std::time::Duration;

/// Adaptive sort-key-range pagination with an owned cursor.
///
/// The connector paginates by half-open ranges `[range_start, range_start + width)`
/// on the first sorting key, with no `ORDER BY`, so determinism comes from the
/// predicate (not row order) and a matching projection can be selected. This type
/// owns both the cursor (`range_start`) and the adaptive `width`, so the
/// advance-vs-reread decision — the part that determines whether data can be lost —
/// is unit-testable in isolation.
///
/// Invariant: a range is only advanced past on `on_complete` (a fully-read page).
/// `on_overflow` and `on_timeout` shrink `width` WITHOUT advancing, so the same
/// range is re-read. Emitted ranges therefore tile the key space with no gaps.
///
/// Two signals drive width:
/// - **rows** — the tripwire. A page is read with `LIMIT page_size + 1`; `page_size + 1`
///   rows means the range overflowed (`on_overflow`), else it was fully consumed.
/// - **elapsed** — the time guard. A page slower than `soft_time_budget` shrinks the
///   width even if the row count looked fine, catching the scan-bound case.
#[derive(Debug, Clone)]
pub struct RangeController {
    page_size: u64,
    width: i128,
    max_width: i128,
    soft_time_budget: Duration,
    range_start: i128,
    max_key: i128,
}

impl RangeController {
    /// Largest per-step growth multiplier. Bounds how fast width expands so a
    /// near-empty page can't explode the range in one step.
    const GROW_CEILING: f64 = 4.0;

    /// Smallest range width: one key. A range must cover at least one key, or it
    /// reads nothing, "completes", advances by zero, and the scan stalls forever.
    /// This floor is what guarantees forward progress. It is deliberately 1 (not
    /// higher): a larger floor could make a dense region unfittable into `page_size`
    /// and falsely report it as unsplittable.
    const MIN_WIDTH: i128 = 1;

    pub fn new(
        page_size: u64,
        range_start: i128,
        max_key: i128,
        initial_width: i128,
        max_width: i128,
        soft_time_budget: Duration,
    ) -> Self {
        Self {
            page_size,
            width: initial_width.clamp(Self::MIN_WIDTH, max_width),
            max_width,
            soft_time_budget,
            range_start,
            max_key,
        }
    }

    /// Start of the range to read next. This is the checkpoint cursor.
    pub fn range_start(&self) -> i128 {
        self.range_start
    }

    /// The half-open range to read next: `[range_start, range_start + width)`.
    pub fn current_range(&self) -> (i128, i128) {
        (self.range_start, self.range_start + self.width)
    }

    pub fn width(&self) -> i128 {
        self.width
    }

    /// True once the cursor has advanced past the last key — the scan is finished.
    pub fn is_done(&self) -> bool {
        self.range_start > self.max_key
    }

    /// True when width is at its floor and can't shrink further. If a page still
    /// overflows here, a single key holds more than `page_size` rows and the caller
    /// must surface it rather than lose data.
    pub fn at_min_width(&self) -> bool {
        self.width <= Self::MIN_WIDTH
    }

    /// A page was fully consumed (rows <= page_size). Advances the cursor past the
    /// completed range, then sizes the next range: grow toward `page_size` rows, or
    /// shrink if the page was slow.
    pub fn on_complete(&mut self, rows: usize, elapsed: Duration) {
        // Advance past the range just read, using the width that produced it. This
        // is the ONLY method that advances the cursor — overflow and timeout re-read.
        self.range_start += self.width;

        // Row-based multiplier: scale toward `page_size` rows per page, capped at
        // GROW_CEILING. A completed page has rows <= page_size, so this is >= 1.0
        // (never shrinks on rows alone — that's the tripwire's and time guard's job).
        let rows = rows.max(1) as f64;
        let row_mult = (self.page_size as f64 / rows).min(Self::GROW_CEILING);
        let mut candidate = self.width as f64 * row_mult;

        // Time guard: a page slower than the soft budget is transfer- or scan-bound,
        // so shrink proportionally regardless of row headroom. This stops an
        // empty-but-slow (sparse, no projection) page from growing into a timeout.
        if elapsed > self.soft_time_budget {
            let time_mult = self.soft_time_budget.as_secs_f64() / elapsed.as_secs_f64();
            candidate = candidate.min(self.width as f64 * time_mult);
        }

        self.width = (candidate as i128).clamp(Self::MIN_WIDTH, self.max_width);
    }

    /// A page overflowed the tripwire (rows == page_size + 1). Shrinks width so the
    /// SAME range is re-read. Does NOT advance the cursor. With a probed exact
    /// `count`, resizes in one step; otherwise halves.
    pub fn on_overflow(&mut self, probed_count: Option<u64>) {
        let candidate = match probed_count {
            Some(count) if count > 0 => {
                (self.width as f64 * (self.page_size as f64 / count as f64)) as i128
            }
            _ => self.width / 2,
        };
        self.width = candidate.clamp(Self::MIN_WIDTH, self.max_width);
    }

    /// A page timed out. Shrinks width and re-reads the SAME range. Does NOT advance
    /// the cursor, so no rows are skipped.
    pub fn on_timeout(&mut self) {
        self.on_overflow(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // page_size 1000, cursor 0, far-off max_key, width 1000, max 1_000_000, 30s
    fn controller() -> RangeController {
        RangeController::new(
            1000,
            0,
            1_000_000_000,
            1000,
            1_000_000,
            Duration::from_secs(30),
        )
    }

    // ---- width adaptation ----

    #[test]
    fn page_at_target_keeps_width_stable() {
        let mut c = controller();
        c.on_complete(1000, Duration::from_secs(1));
        assert_eq!(
            c.width(),
            1000,
            "a page exactly at page_size should hold width"
        );
    }

    #[test]
    fn sparse_page_grows_width_capped_at_ceiling() {
        let mut c = controller();
        c.on_complete(100, Duration::from_secs(1));
        assert_eq!(c.width(), 4000, "growth is capped at 4x per step");
    }

    #[test]
    fn empty_fast_page_grows_at_ceiling() {
        let mut c = controller();
        c.on_complete(0, Duration::from_secs(1));
        assert_eq!(c.width(), 4000, "an empty fast page grows at the ceiling");
    }

    #[test]
    fn empty_slow_page_does_not_grow() {
        let mut c = controller();
        // Empty but 60s against a 30s budget: scan-bound. Time guard overrides the
        // grow-on-empty and shrinks instead (width * 30/60 = 500).
        c.on_complete(0, Duration::from_secs(60));
        assert_eq!(
            c.width(),
            500,
            "an empty but slow page is scan-bound and must shrink"
        );
    }

    #[test]
    fn slow_page_shrinks_even_with_row_headroom() {
        let mut c = controller();
        c.on_complete(500, Duration::from_secs(60));
        assert_eq!(c.width(), 500, "the time guard wins over row-based growth");
    }

    #[test]
    fn overflow_with_probed_count_resizes_in_one_step() {
        let mut c = controller();
        c.on_overflow(Some(4000));
        assert_eq!(c.width(), 250, "a probed count resizes precisely");
    }

    #[test]
    fn overflow_without_count_halves() {
        let mut c = controller();
        c.on_overflow(None);
        assert_eq!(c.width(), 500, "no probe -> halve");
    }

    #[test]
    fn shrink_floors_at_one_to_guarantee_progress() {
        // Width must never reach 0, or an empty range would stall the scan. Even an
        // aggressive shrink (probed count far above page_size) floors at 1.
        let mut c =
            RangeController::new(1000, 0, 1_000_000, 100, 1_000_000, Duration::from_secs(30));
        c.on_overflow(Some(1_000_000));
        assert_eq!(
            c.width(),
            1,
            "shrink floors at 1 so the cursor always advances"
        );
        assert!(c.at_min_width(), "width 1 is the minimum");
    }

    #[test]
    fn growth_is_clamped_to_max_width() {
        let mut c = RangeController::new(
            1000,
            0,
            1_000_000_000,
            500_000,
            1_000_000,
            Duration::from_secs(30),
        );
        c.on_complete(0, Duration::from_secs(1));
        assert_eq!(c.width(), 1_000_000, "growth never exceeds max_width");
    }

    // ---- cursor / advance semantics (data-loss critical) ----

    #[test]
    fn complete_advances_cursor_by_the_width_just_used() {
        let mut c =
            RangeController::new(1000, 100, 1_000_000_000, 50, 4096, Duration::from_secs(30));
        assert_eq!(c.current_range(), (100, 150));
        c.on_complete(50, Duration::from_secs(1));
        assert_eq!(
            c.range_start(),
            150,
            "cursor advances by the width that was read"
        );
    }

    #[test]
    fn overflow_does_not_advance_cursor() {
        let mut c = RangeController::new(20, 100, 1_000_000_000, 80, 4096, Duration::from_secs(30));
        c.on_overflow(None);
        assert_eq!(
            c.range_start(),
            100,
            "overflow must re-read the same range, not advance"
        );
        assert_eq!(c.width(), 40, "overflow shrinks width");
    }

    #[test]
    fn timeout_does_not_advance_cursor_and_shrinks() {
        let mut c = RangeController::new(20, 100, 1_000_000_000, 80, 4096, Duration::from_secs(30));
        c.on_timeout();
        assert_eq!(c.range_start(), 100, "timeout must NOT advance the cursor");
        assert!(c.width() < 80, "timeout shrinks the width");
    }

    #[test]
    fn is_done_only_after_cursor_passes_max_key() {
        let mut c = RangeController::new(1000, 0, 100, 100, 4096, Duration::from_secs(30));
        assert!(!c.is_done());
        c.on_complete(100, Duration::from_secs(1)); // advance 0 -> 100
        assert!(
            !c.is_done(),
            "range_start == max_key is not done (max_key row still in range)"
        );
        c.on_complete(100, Duration::from_secs(1)); // advance past max_key
        assert!(c.is_done(), "range_start past max_key is done");
    }

    // Synthetic distribution: 1 row per key, except a dense burst in [300,400) at
    // 8 rows/key. With a small page_size this forces overflow + shrink inside the
    // burst and growth back out — without ever exceeding page_size for a single key
    // (so it stays coverable).
    fn rows_in_range(start: i128, end: i128) -> usize {
        let mut rows = 0usize;
        for k in start..end {
            rows += if (300..400).contains(&k) { 8 } else { 1 };
        }
        rows
    }

    /// CASE 1 (page size exceeds) + CASE 3 (scale down then back up): drive a full
    /// scan through a dense burst. Assert the emitted ranges tile the whole key
    /// space with no gaps and every row is emitted exactly once.
    #[test]
    fn no_data_loss_through_dense_burst_with_overflow_and_rescale() {
        let max_key = 1000i128;
        let page_size = 20u64;
        let mut c = RangeController::new(page_size, 0, max_key, 20, 4096, Duration::from_secs(30));

        let mut emitted: Vec<(i128, i128)> = Vec::new();
        let mut saw_overflow = false;
        let mut min_width_in_burst = i128::MAX;
        let mut max_width_after_burst = 0i128;
        let mut guard = 0;

        while !c.is_done() {
            guard += 1;
            assert!(guard < 1_000_000, "controller failed to terminate");
            let (start, end_raw) = c.current_range();
            let end = end_raw.min(max_key + 1);
            let rows = rows_in_range(start, end);

            if rows as u64 > page_size {
                assert!(
                    !c.at_min_width(),
                    "coverable density must never stick at min width"
                );
                saw_overflow = true;
                min_width_in_burst = min_width_in_burst.min(c.width());
                c.on_overflow(None);
            } else {
                emitted.push((start, end));
                c.on_complete(rows, Duration::from_millis(1));
                if start >= 400 {
                    max_width_after_burst = max_width_after_burst.max(c.width());
                }
            }
        }

        // No gaps, no overlaps: emitted ranges tile [0, max_key + 1) contiguously.
        assert_eq!(emitted.first().unwrap().0, 0, "scan must start at key 0");
        for w in emitted.windows(2) {
            assert_eq!(
                w[0].1, w[1].0,
                "gap or overlap between {:?} and {:?}",
                w[0], w[1]
            );
        }
        assert!(
            emitted.last().unwrap().1 > max_key,
            "scan must cover past max_key"
        );

        // Every row emitted exactly once.
        let total = rows_in_range(0, max_key + 1);
        let got: usize = emitted.iter().map(|(s, e)| rows_in_range(*s, *e)).sum();
        assert_eq!(
            got, total,
            "all rows must be emitted exactly once (no loss, no dup)"
        );

        // We actually scaled down in the burst and back up afterward.
        assert!(saw_overflow, "dense burst should have triggered overflow");
        assert!(
            max_width_after_burst > min_width_in_burst,
            "width should recover after the burst (down {} -> up {})",
            min_width_in_burst,
            max_width_after_burst
        );
    }

    /// CASE 2 (timeout): every 3rd page times out. Timeouts must re-read without
    /// advancing, so the scan still covers every key with no gaps.
    #[test]
    fn no_data_loss_with_injected_timeouts() {
        let max_key = 500i128;
        let page_size = 20u64;
        let mut c = RangeController::new(page_size, 0, max_key, 30, 4096, Duration::from_secs(30));

        let mut emitted: Vec<(i128, i128)> = Vec::new();
        let mut step = 0;
        let mut guard = 0;
        let mut saw_timeout = false;

        while !c.is_done() {
            guard += 1;
            assert!(guard < 1_000_000, "controller failed to terminate");
            let (start, end_raw) = c.current_range();
            let end = end_raw.min(max_key + 1);
            step += 1;

            // Inject a timeout on every 3rd page (while there's width to shrink).
            if step % 3 == 0 && c.width() > 1 {
                saw_timeout = true;
                c.on_timeout();
                continue;
            }

            let rows = rows_in_range(start, end);
            if rows as u64 > page_size {
                assert!(
                    !c.at_min_width(),
                    "coverable density must never stick at min width"
                );
                c.on_overflow(None);
            } else {
                emitted.push((start, end));
                c.on_complete(rows, Duration::from_millis(1));
            }
        }

        assert!(saw_timeout, "test should have injected timeouts");
        assert_eq!(emitted.first().unwrap().0, 0, "scan must start at key 0");
        for w in emitted.windows(2) {
            assert_eq!(
                w[0].1, w[1].0,
                "gap or overlap between {:?} and {:?}",
                w[0], w[1]
            );
        }
        assert!(
            emitted.last().unwrap().1 > max_key,
            "scan must cover past max_key"
        );
        let total = rows_in_range(0, max_key + 1);
        let got: usize = emitted.iter().map(|(s, e)| rows_in_range(*s, *e)).sum();
        assert_eq!(got, total, "no rows lost despite timeouts");
    }
}
