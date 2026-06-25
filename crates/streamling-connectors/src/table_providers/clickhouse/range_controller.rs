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
/// Three signals drive width:
/// - **rows** — the tripwire. A page is read with `LIMIT page_size + 1`; `page_size + 1`
///   rows means the range overflowed (`on_overflow`), else it was fully consumed.
/// - **elapsed** — the time guard. A page slower than `soft_time_budget` shrinks the
///   width even if the row count looked fine, catching the scan-bound case.
/// - **bytes** — the array guard. A page over `max_page_bytes` (`on_byte_overflow`)
///   would build a single Arrow column past the ~2 GiB `i32` offset limit, so it is
///   re-read smaller. `on_complete` also clamps growth by observed byte density, or a
///   byte-dense region would let the row multiplier snap width straight back into a
///   byte overflow and thrash forever.
#[derive(Debug, Clone)]
pub struct RangeController {
    page_size: u64,
    /// Upper bound on a page's total byte size. Guards Arrow's `i32` per-array
    /// limit (~2 GiB): the widest column is at most the page total, so a page
    /// kept under this never builds a column that overflows `concat_batches` or
    /// the IPC reader. Drives `on_byte_overflow` and the byte clamp in
    /// `on_complete`.
    max_page_bytes: u64,
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

    /// Fraction of `max_page_bytes` that byte sizing targets. A re-read covers a
    /// different (shrunk) sub-range whose byte density differs from the page that
    /// overflowed, so sizing to land at *exactly* the limit asymptotes — each read
    /// lands a hair over and shrinks by one (observed on `matic_raw_logs`: 6
    /// discarded ~1 GiB reads to converge). Targeting 90% leaves headroom for that
    /// density variance so the re-read lands under the limit in one step; even when
    /// variance exceeds it, convergence is geometric (each step shrinks >10%), not
    /// a unit creep. `on_complete`'s byte clamp uses the same target so a completed
    /// page can't creep back up to the limit and re-overflow.
    const BYTE_TARGET_RATIO: f64 = 0.9;

    /// Fraction of `page_size` that a probed one-step resize targets for rows.
    /// Symmetric with `BYTE_TARGET_RATIO`: sizing a shrunk range to land at
    /// *exactly* `page_size` rows leaves no margin, so a range whose true count
    /// sits just over the tripwire re-reads at almost the same width, hits
    /// `page_size + 1` again, and creeps down a row at a time. Targeting 90%
    /// guarantees each overflow shrinks the width meaningfully, at the cost of
    /// ~10% per-page headroom in a steady dense region.
    const ROW_TARGET_RATIO: f64 = 0.9;

    /// Smallest range width: one key. A range must cover at least one key, or it
    /// reads nothing, "completes", advances by zero, and the scan stalls forever.
    /// This floor is what guarantees forward progress. It is deliberately 1 (not
    /// higher): a larger floor could make a dense region unfittable into `page_size`
    /// and falsely report it as unsplittable.
    const MIN_WIDTH: i128 = 1;

    pub fn new(
        page_size: u64,
        max_page_bytes: u64,
        range_start: i128,
        max_key: i128,
        initial_width: i128,
        max_width: i128,
        soft_time_budget: Duration,
    ) -> Self {
        Self {
            page_size,
            max_page_bytes,
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

    /// A page was fully consumed (rows <= page_size, bytes <= max_page_bytes).
    /// Advances the cursor past the completed range, then sizes the next range:
    /// grow toward `page_size` rows, shrink if the page was slow, and — crucially
    /// — never grow past the byte density just observed. That byte clamp is what
    /// stops a byte-dense region from thrashing: without it the row multiplier
    /// (rows are few when bytes are large) would snap width straight back over
    /// `max_page_bytes`, re-trigger overflow, and re-read forever.
    pub fn on_complete(&mut self, rows: usize, elapsed: Duration, bytes: u64) {
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

        // Byte guard: cap growth so the next page stays under `max_page_bytes`,
        // using the byte density just observed (bytes per unit width). `self.width`
        // here is still the completing page's width, so bytes/width is the density
        // of the region just scanned. A byte-light page leaves headroom to grow;
        // a byte-heavy page pins width near its current value. Target
        // BYTE_TARGET_RATIO of the limit so a page completing right at the cap
        // can't let the next one creep over and re-overflow.
        if bytes > 0 && self.width > 0 {
            let bytes_per_width = bytes as f64 / self.width as f64;
            let byte_ceiling =
                (self.max_page_bytes as f64 * Self::BYTE_TARGET_RATIO) / bytes_per_width;
            candidate = candidate.min(byte_ceiling);
        }

        self.width = (candidate as i128).clamp(Self::MIN_WIDTH, self.max_width);
    }

    /// A page overflowed the row tripwire (rows == page_size + 1). Shrinks width so
    /// the SAME range is re-read. Does NOT advance the cursor. With a probed exact
    /// `count`, resizes in one step; otherwise halves.
    fn on_overflow(&mut self, probed_count: Option<u64>) {
        let candidate = match probed_count {
            Some(count) if count > 0 => {
                (self.width as f64 * (self.page_size as f64 / count as f64)) as i128
            }
            _ => self.width / 2,
        };
        self.width = candidate.clamp(Self::MIN_WIDTH, self.max_width);
    }

    /// A page overflowed the byte guard (bytes > max_page_bytes) while fitting by
    /// rows. Sizes the next width from the observed byte density, targeting
    /// `BYTE_TARGET_RATIO` of `max_page_bytes` (not the limit itself) so the re-read
    /// — which covers a different, shrunk sub-range with different density — lands
    /// under the limit in one step rather than asymptoting a unit at a time.
    /// `on_complete`'s byte clamp then keeps it there. Does NOT advance the cursor,
    /// so the same range is re-read.
    fn on_byte_overflow(&mut self, observed_bytes: u64) {
        let target = self.max_page_bytes as f64 * Self::BYTE_TARGET_RATIO;
        let candidate = if observed_bytes > 0 {
            (self.width as f64 * (target / observed_bytes as f64)) as i128
        } else {
            self.width / 2
        };
        self.width = candidate.clamp(Self::MIN_WIDTH, self.max_width);
    }

    /// A page overflowed the row tripwire and/or the byte guard, and the caller
    /// probed the exact `count` for the range just read. Size the next width to
    /// satisfy BOTH limits in one step using the *true* density (`count /
    /// width`) — not the LIMIT-capped row sample that biases `on_byte_overflow`.
    ///
    /// `observed_bytes` is the byte size of the rows actually materialised (≤
    /// `page_size + 1`). Divided by the smaller of `count` and `page_size + 1`
    /// it gives bytes-per-row; multiplied by the true row density it predicts
    /// the byte density of the *whole* range, so the byte limit is respected
    /// even though only a prefix of the rows was read.
    ///
    /// This is what makes a dense region converge in one re-read instead of
    /// shrinking geometrically over many discarded pages: a wide range whose
    /// first `page_size + 1` rows overflow still has its full `count` probed,
    /// so the next width lands under both limits immediately. A probe failure
    /// (`None`) falls back to the byte/row shrink so a transient count-query
    /// error cannot stall the scan. Does NOT advance the cursor — the same
    /// range is re-read.
    pub fn on_overflow_probed(&mut self, count: Option<u64>, observed_bytes: u64) {
        let Some(count) = count.filter(|&c| c > 0) else {
            if observed_bytes > self.max_page_bytes {
                self.on_byte_overflow(observed_bytes);
            } else {
                self.on_overflow(None);
            }
            return;
        };
        let count = count as f64;
        // Bytes-per-row from the sampled rows we actually materialised. The
        // sample is at most page_size + 1 rows even when the range holds more.
        let sampled_rows = count.min(self.page_size as f64 + 1.0);
        let bytes_per_row = if observed_bytes > 0 {
            observed_bytes as f64 / sampled_rows
        } else {
            0.0
        };
        // Row-safe width: scale so the next range holds ~ROW_TARGET_RATIO * page_size.
        let row_safe = self.width as f64 * (Self::ROW_TARGET_RATIO * self.page_size as f64 / count);
        // Byte-safe width: scale so the next range holds ~BYTE_TARGET_RATIO * max.
        // bytes_per_width = (count / width) * bytes_per_row = true row density × size.
        let byte_safe = if bytes_per_row > 0.0 && self.width > 0 {
            let bytes_per_width = (count / self.width as f64) * bytes_per_row;
            (Self::BYTE_TARGET_RATIO * self.max_page_bytes as f64) / bytes_per_width
        } else {
            row_safe
        };
        let candidate = row_safe.min(byte_safe);
        self.width = (candidate as i128).clamp(Self::MIN_WIDTH, self.max_width);
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
            u64::MAX,
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
        c.on_complete(1000, Duration::from_secs(1), 0);
        assert_eq!(
            c.width(),
            1000,
            "a page exactly at page_size should hold width"
        );
    }

    #[test]
    fn sparse_page_grows_width_capped_at_ceiling() {
        let mut c = controller();
        c.on_complete(100, Duration::from_secs(1), 0);
        assert_eq!(c.width(), 4000, "growth is capped at 4x per step");
    }

    #[test]
    fn empty_fast_page_grows_at_ceiling() {
        let mut c = controller();
        c.on_complete(0, Duration::from_secs(1), 0);
        assert_eq!(c.width(), 4000, "an empty fast page grows at the ceiling");
    }

    #[test]
    fn empty_slow_page_does_not_grow() {
        let mut c = controller();
        // Empty but 60s against a 30s budget: scan-bound. Time guard overrides the
        // grow-on-empty and shrinks instead (width * 30/60 = 500).
        c.on_complete(0, Duration::from_secs(60), 0);
        assert_eq!(
            c.width(),
            500,
            "an empty but slow page is scan-bound and must shrink"
        );
    }

    #[test]
    fn slow_page_shrinks_even_with_row_headroom() {
        let mut c = controller();
        c.on_complete(500, Duration::from_secs(60), 0);
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
        let mut c = RangeController::new(
            1000,
            u64::MAX,
            0,
            1_000_000,
            100,
            1_000_000,
            Duration::from_secs(30),
        );
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
            u64::MAX,
            0,
            1_000_000_000,
            500_000,
            1_000_000,
            Duration::from_secs(30),
        );
        c.on_complete(0, Duration::from_secs(1), 0);
        assert_eq!(c.width(), 1_000_000, "growth never exceeds max_width");
    }

    // ---- cursor / advance semantics (data-loss critical) ----

    #[test]
    fn complete_advances_cursor_by_the_width_just_used() {
        let mut c = RangeController::new(
            1000,
            u64::MAX,
            100,
            1_000_000_000,
            50,
            4096,
            Duration::from_secs(30),
        );
        assert_eq!(c.current_range(), (100, 150));
        c.on_complete(50, Duration::from_secs(1), 0);
        assert_eq!(
            c.range_start(),
            150,
            "cursor advances by the width that was read"
        );
    }

    #[test]
    fn overflow_does_not_advance_cursor() {
        let mut c = RangeController::new(
            20,
            u64::MAX,
            100,
            1_000_000_000,
            80,
            4096,
            Duration::from_secs(30),
        );
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
        let mut c = RangeController::new(
            20,
            u64::MAX,
            100,
            1_000_000_000,
            80,
            4096,
            Duration::from_secs(30),
        );
        c.on_timeout();
        assert_eq!(c.range_start(), 100, "timeout must NOT advance the cursor");
        assert!(c.width() < 80, "timeout shrinks the width");
    }

    #[test]
    fn is_done_only_after_cursor_passes_max_key() {
        let mut c =
            RangeController::new(1000, u64::MAX, 0, 100, 100, 4096, Duration::from_secs(30));
        assert!(!c.is_done());
        c.on_complete(100, Duration::from_secs(1), 0); // advance 0 -> 100
        assert!(
            !c.is_done(),
            "range_start == max_key is not done (max_key row still in range)"
        );
        c.on_complete(100, Duration::from_secs(1), 0); // advance past max_key
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
        let mut c = RangeController::new(
            page_size,
            u64::MAX,
            0,
            max_key,
            20,
            4096,
            Duration::from_secs(30),
        );

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
                c.on_complete(rows, Duration::from_millis(1), 0);
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
        let mut c = RangeController::new(
            page_size,
            u64::MAX,
            0,
            max_key,
            30,
            4096,
            Duration::from_secs(30),
        );

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
                c.on_complete(rows, Duration::from_millis(1), 0);
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

    // ---- byte tripwire (Arrow ~2 GiB per-array guard) ----

    /// A controller configured with a tiny byte limit, used to exercise the byte
    /// signals in isolation (page_size is set huge so rows never trip).
    fn byte_controller() -> RangeController {
        RangeController::new(
            100_000,
            1_000,
            0,
            1_000_000_000,
            1000,
            100_000,
            Duration::from_secs(30),
        )
    }

    #[test]
    fn byte_overflow_sizes_from_observed_density_in_one_step() {
        let mut c = byte_controller();
        // width 1000, observed 2500 bytes vs a 1000 limit, target 90% -> next
        // width 360, landing under the limit with headroom in a single re-read.
        c.on_byte_overflow(2500);
        assert_eq!(
            c.width(),
            360,
            "byte overflow resizes by (0.9*max)/observed ratio"
        );
        assert_eq!(
            c.range_start(),
            0,
            "byte overflow must NOT advance the cursor (re-read the same range)"
        );
    }

    #[test]
    fn byte_overflow_floors_at_one() {
        let mut c = byte_controller();
        // Observed bytes dwarf the limit: resize floors at MIN_WIDTH so the
        // cursor still advances one key at a time.
        c.on_byte_overflow(u64::MAX);
        assert_eq!(c.width(), 1);
        assert!(c.at_min_width());
    }

    #[test]
    fn on_complete_byte_clamp_prevents_thrash() {
        // Regression: a byte-dense but row-sparse region. Without the byte clamp
        // in on_complete, the row multiplier (page_size/rows, capped at 4x) would
        // snap width straight back past the byte limit every cycle, thrashing
        // between overflow and re-read forever. With the clamp, width stays put.
        let mut c = byte_controller();
        // width 1000: rows 100 (row-sparse), bytes 2000 (byte-dense -> overflow).
        // Target 90% of the 1000 limit -> shrinks to 450 (not 500).
        c.on_byte_overflow(2000);
        assert_eq!(c.width(), 450, "shrunk to 90% of the byte-safe width");
        // Re-read at 450: bytes ~1000 (at the limit, not over), rows ~50 -> complete.
        // The byte clamp caps growth at 90% of the limit, so width does NOT snap
        // back toward the row multiplier's 1800 target.
        c.on_complete(50, Duration::from_millis(1), 1000);
        assert!(
            c.width() <= 450,
            "on_complete must NOT grow width back into a byte overflow \
             (clamped to {}, would be ~1800 without it)",
            c.width()
        );
    }

    #[test]
    fn on_complete_grows_when_byte_headroom_allows() {
        // A byte-light page leaves headroom: growth is capped by the byte ceiling
        // (which is well above the row target), so the row multiplier wins and
        // width grows — the byte guard must not over-restrict sparse data.
        let mut c = byte_controller();
        c.on_complete(10, Duration::from_millis(1), 10);
        // row_mult = 100_000/10 capped at 4 -> 1000*4 = 4000; byte_ceiling =
        // 1000/(10/1000) = 100_000; min = 4000, clamped to max_width 100_000.
        assert_eq!(c.width(), 4000, "byte-light page grows normally");
    }

    // ---- probed one-step overflow sizing (true density) ----

    #[test]
    fn probed_overflow_converges_dense_range_in_one_step() {
        // The production pathology: a wide range whose first page_size+1 rows
        // overflow on bytes. The LIMIT-capped sample under-estimates density, so
        // the old byte sizer shrank geometrically over many re-reads. With a
        // probed count the true density sizes the next width under the byte limit
        // at once. byte_controller: page_size 100_000, max 1000 bytes, width 1000.
        //   count 50_000 (true); observed 4000 bytes for all 50_000 rows.
        //   bytes_per_row = 4000/50_000 = 0.08
        //   bytes_per_width = (50_000/1000)*0.08 = 4.0
        //   byte_safe = 0.9*1000/4 = 225; row_safe = 1000*0.9*100_000/50_000 = 1800
        //   -> 225 (byte binds), one step.
        let mut c = byte_controller();
        c.on_overflow_probed(Some(50_000), 4000);
        assert_eq!(c.width(), 225, "one-step resize to the byte-safe width");
        assert_eq!(
            c.range_start(),
            0,
            "probed overflow must NOT advance the cursor (re-read the same range)"
        );
    }

    #[test]
    fn probed_overflow_byte_only_matches_byte_sizer() {
        // count <= page_size (rows fit, bytes trip): reduces to on_byte_overflow.
        let mut a = byte_controller();
        let mut b = byte_controller();
        a.on_overflow_probed(Some(100), 2500);
        b.on_byte_overflow(2500);
        assert_eq!(a.width(), b.width());
        assert_eq!(a.width(), 360);
    }

    #[test]
    fn probed_overflow_row_only_targets_ninety_percent() {
        // No byte concern (observed 0): row constraint binds, sized to 90% of the
        // row-safe width (10% margin vs on_overflow's exact page_size target).
        let mut c = controller(); // page_size 1000, width 1000
        c.on_overflow_probed(Some(4000), 0);
        assert_eq!(c.width(), 225, "0.9 * (1000*1000/4000) = 225");
    }

    #[test]
    fn probed_overflow_none_falls_back_to_byte_or_halve() {
        // A failed/absent probe must not change behavior vs the un-probed path.
        let mut over = byte_controller(); // max_page_bytes 1000
        over.on_overflow_probed(None, 2500); // bytes over limit -> byte sizer
        assert_eq!(over.width(), 360);

        let mut under = controller(); // max_page_bytes u64::MAX, width 1000
        under.on_overflow_probed(None, 0); // bytes under limit -> halve
        assert_eq!(under.width(), 500);
    }

    #[test]
    fn probed_overflow_floors_at_one() {
        // A single key holding vast bytes/rows: even the true density can't fit,
        // so width floors at MIN_WIDTH and the caller surfaces it.
        let mut c = byte_controller();
        c.on_overflow_probed(Some(u64::MAX), u64::MAX);
        assert_eq!(c.width(), 1);
        assert!(c.at_min_width());
    }

    /// No-data-loss check for the probed path: drive a full scan through the dense
    /// burst, sizing overflows from the true count (as the driver now does). The
    /// emitted ranges must still tile the key space with no gaps or duplicates.
    #[test]
    fn no_data_loss_with_probed_overflow_sizing() {
        let max_key = 1000i128;
        let page_size = 20u64;
        let mut c = RangeController::new(
            page_size,
            u64::MAX,
            0,
            max_key,
            20,
            4096,
            Duration::from_secs(30),
        );

        let mut emitted: Vec<(i128, i128)> = Vec::new();
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
                // The driver probes the true count here.
                c.on_overflow_probed(Some(rows as u64), 0);
            } else {
                emitted.push((start, end));
                c.on_complete(rows, Duration::from_millis(1), 0);
            }
        }

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
        assert_eq!(
            got, total,
            "all rows emitted exactly once (no loss, no dup)"
        );
    }

    /// A full scan through a byte-dense burst. Models bytes-per-key (1 outside
    /// [300,400), 100 inside) against a 1000-byte limit with rows never
    /// overflowing. The controller must shrink inside the burst, recover after
    /// it, and tile the whole key space with no gaps or duplicates — proving the
    /// byte tripwire never loses data and converges instead of thrashing.
    fn bytes_in_range(start: i128, end: i128) -> u64 {
        (start..end)
            .map(|k| {
                if (300..400).contains(&k) {
                    100u64
                } else {
                    1u64
                }
            })
            .sum()
    }

    #[test]
    fn no_data_loss_through_byte_dense_burst() {
        let max_key = 1000i128;
        let page_size = 100_000u64;
        let max_page_bytes = 1000u64;
        let mut c = RangeController::new(
            page_size,
            max_page_bytes,
            0,
            max_key,
            500,
            100_000,
            Duration::from_secs(30),
        );

        let mut emitted: Vec<(i128, i128)> = Vec::new();
        let mut saw_byte_overflow = false;
        let mut min_width_in_burst = i128::MAX;
        let mut max_width_after_burst = 0i128;
        let mut guard = 0;

        while !c.is_done() {
            guard += 1;
            assert!(guard < 1_000_000, "controller failed to terminate");
            let (start, end_raw) = c.current_range();
            let end = end_raw.min(max_key + 1);
            let rows = (end - start) as usize; // 1 row/key
            let bytes = bytes_in_range(start, end);

            if bytes > max_page_bytes {
                assert!(
                    !c.at_min_width(),
                    "coverable byte density must never stick at min width"
                );
                saw_byte_overflow = true;
                if (300..400).contains(&start) || start >= 290 {
                    min_width_in_burst = min_width_in_burst.min(c.width());
                }
                c.on_byte_overflow(bytes);
            } else {
                emitted.push((start, end));
                c.on_complete(rows, Duration::from_millis(1), bytes);
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

        // Every key emitted exactly once.
        let total_keys = (max_key + 1) as usize;
        let got: usize = emitted.iter().map(|(s, e)| (*e - *s) as usize).sum();
        assert_eq!(
            got, total_keys,
            "all keys emitted exactly once (no loss, no dup)"
        );

        assert!(
            saw_byte_overflow,
            "byte-dense burst should trigger byte overflow"
        );
        assert!(
            max_width_after_burst > min_width_in_burst,
            "width should recover after the burst (down {} -> up {})",
            min_width_in_burst,
            max_width_after_burst
        );
    }
}
