use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, BooleanArray, BooleanBuilder, LargeStringArray, StringArray};
use arrow::compute::concat;
use datafusion::common::hash_utils::{RandomState, with_hashes};
use hashbrown::HashTable;

use super::bloom::BlockedBloom;

// Adaptive prefilter guard. Measured: the prefilter makes misses cheaper but
// makes hits ~1.8x more expensive (112ns -> 202ns), because a hit pays the
// filter probe and then still does the full exact probe. Break-even is around a
// ~19% hit rate. A first-seen/dedup table's hit rate RISES as it fills, so a
// filter that helps at deploy time silently becomes a tax later. These constants
// steer `filter_is_paying_off` so the filter turns itself off once hits dominate.
const FILTER_WARMUP_PROBES: u64 = 100_000;
// 15% < the ~19% break-even, so the guard leaves margin: it switches the filter
// off before hits get expensive enough to be a net regression.
const FILTER_MAX_HIT_PERCENT: u64 = 15;
const FILTER_DECAY_PROBES: u64 = 1_000_000;

/// Exact membership set over contiguous Arrow string keys.
///
/// Hash values only select candidate buckets; equality is decided by comparing
/// `&str` bytes against the stored haystack key. Collision-free for membership.
///
/// Cross-type probing (`LargeUtf8` haystack vs `Utf8` needle) is safe because
/// `datafusion_common::hash_utils::hash_array` is generic over `ArrayAccessor`
/// with `Item = &str` for both `StringArray` and `LargeStringArray`, so identical
/// string content hashes identically under the same `RandomState`. Do not hash
/// the haystack with anything other than `with_hashes` / that same state.
#[derive(Debug)]
pub(crate) struct ArrowKeySet {
    keys: LargeStringArray,
    /// Parallel to `keys`; needed by `HashTable` growth to rehash existing entries.
    hashes: Vec<u64>,
    state: RandomState,
    /// Index into `keys`.
    table: HashTable<u32>,
    /// Cache-resident prefilter that answers "definitely absent" without
    /// touching the (large) exact table. It uses the SAME u64 hashes as `table`
    /// and can never produce a hit on its own — a maybe is re-checked exactly.
    filter: BlockedBloom,
    /// Rolling probe/hit counts used to disable the prefilter if the workload
    /// turns hit-heavy. Relaxed ordering: these only steer a performance
    /// decision, never a result, so a torn or stale read is harmless.
    probes: AtomicU64,
    hits: AtomicU64,
}

impl ArrowKeySet {
    pub(crate) fn from_keys(keys: LargeStringArray) -> Result<Self, String> {
        // Default sizing: 10 bits/key, k=6, capacity = 2x keys (min 1024 so
        // tiny sets still work).
        let capacity_keys = (2 * keys.len()).max(1024);
        Self::from_keys_sized(keys, capacity_keys * 10, 6)
    }

    /// Build with an explicit filter sizing so benchmarks can compare.
    /// `filter_bits == 0` gives a disabled (zero-overhead) filter.
    pub(crate) fn from_keys_sized(
        keys: LargeStringArray,
        filter_bits: usize,
        k: u32,
    ) -> Result<Self, String> {
        // Deliberately no early return for empty keys. A shortcut that built a
        // DISABLED filter here would have no rebuild path, so a cache whose first
        // load found an empty table would never gain a prefilter as it filled up.
        // `hash_and_insert_range` is already a no-op when there is nothing to add.
        let capacity_keys = (2 * keys.len()).max(1024);
        let mut set = Self {
            hashes: vec![0; keys.len()],
            keys,
            state: RandomState::default(),
            table: HashTable::new(),
            filter: BlockedBloom::with_bits(filter_bits, k, capacity_keys),
            probes: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        };
        set.hash_and_insert_range(0)?;
        Ok(set)
    }

    pub(crate) fn extend_from(&mut self, extra: LargeStringArray) -> Result<(), String> {
        if extra.is_empty() {
            return Ok(());
        }

        let start = self.keys.len();
        // `concat` is append-only: existing indices stay valid, so incremental
        // refresh only needs to hash/insert the newly appended slice — no full rebuild.
        // ponytail: concat copies the whole key buffer, so a refresh is O(total bytes).
        // Fine while refreshes are rare (the read-mostly case this set exists for); if
        // they get frequent, keep a Vec of chunks and pack (chunk, row) into the index.
        let concatenated =
            concat(&[&self.keys as &dyn Array, &extra as &dyn Array]).map_err(|e| e.to_string())?;
        self.keys = concatenated
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .ok_or_else(|| "concat did not produce LargeStringArray".to_string())?
            .clone();

        self.hashes.resize(self.keys.len(), 0);
        self.hash_and_insert_range(start)?;

        // The filter's FPR decays toward 1 once the set outgrows its design
        // capacity. Rebuild at double the capacity and double the bits (keeping
        // bits/key constant), then re-insert every non-null key's hash from
        // `self.hashes` — no re-hashing needed.
        if !self.filter.is_disabled() && self.table.len() > self.filter.capacity_keys() {
            let new_capacity = self.filter.capacity_keys() * 2;
            let new_bits = self.filter.bytes() * 16; // 2x bits: bytes()*8 = bits, doubled
            let k = self.filter.k();
            self.filter = BlockedBloom::with_bits(new_bits, k, new_capacity);
            for idx in 0..self.keys.len() {
                if !self.keys.is_null(idx) {
                    self.filter.insert(self.hashes[idx]);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.table.len()
    }

    pub(crate) fn contains_array(&self, needles: &StringArray) -> Result<BooleanArray, String> {
        // Haystack is LargeUtf8, needle is Utf8 — hashes match for equal `&str`
        // content (see struct-level comment).
        //
        // Decide ONCE PER BATCH whether the prefilter pays, not per row — an
        // atomic load inside the inner loop would cost more than the filter
        // saves. Accumulate batch-local totals and fold them into the shared
        // counters once at the end (two atomics per batch, negligible).
        let use_filter = !self.filter.is_disabled() && self.filter_is_paying_off();

        with_hashes([needles as &dyn Array], &self.state, |hashes| {
            let mut builder = BooleanBuilder::with_capacity(needles.len());
            let mut batch_probes: u64 = 0;
            let mut batch_hits: u64 = 0;
            for (i, &hash) in hashes.iter().enumerate() {
                if needles.is_null(i) {
                    // `hash_array` only writes valid indices; null slots hold garbage.
                    builder.append_null();
                    continue;
                }
                batch_probes += 1;
                if use_filter {
                    // Prefilter: a miss means DEFINITELY absent — skip the exact
                    // table entirely. A hit still runs the exact probe below; the
                    // filter can never produce a `true` on its own.
                    if !self.filter.maybe_contains(hash) {
                        builder.append_value(false);
                        continue;
                    }
                }
                let needle = needles.value(i);
                let found = self
                    .table
                    .find(hash, |&idx| self.keys.value(idx as usize) == needle)
                    .is_some();
                // A hit is a needle present in the set — the filter only helps
                // when misses dominate, so watch the real hit rate, not whether
                // the exact probe ran (a filter false-positive is still a miss).
                if found {
                    batch_hits += 1;
                }
                builder.append_value(found);
            }
            if use_filter {
                self.probes.fetch_add(batch_probes, Ordering::Relaxed);
                self.hits.fetch_add(batch_hits, Ordering::Relaxed);
            }
            Ok(builder.finish())
        })
        .map_err(|e| e.to_string())
    }

    /// Whether consulting the prefilter is still a performance win on the
    /// recent workload.
    ///
    /// SAFETY: consulting the filter or not CANNOT change results — the filter
    /// only ever short-circuits a negative (a miss is DEFINITELY absent), so
    /// skipping it just means always running the exact probe. Because it is a
    /// pure performance switch, a torn or stale read of the counters is
    /// harmless: at worst the filter stays on (or off) one batch too long. That
    /// is why the atomics use Relaxed ordering and the periodic halving is
    /// racy.
    fn filter_is_paying_off(&self) -> bool {
        let probes = self.probes.load(Ordering::Relaxed);
        // Assume it pays until there is enough evidence to say otherwise.
        if probes < FILTER_WARMUP_PROBES {
            return true;
        }
        // Halve both counters periodically so this tracks the RECENT hit rate
        // rather than the lifetime average — a cache that starts cold and fills
        // up must be able to switch the filter off, and a workload that changes
        // back must be able to switch it on again.
        if probes >= FILTER_DECAY_PROBES {
            self.probes.store(probes / 2, Ordering::Relaxed);
            self.hits
                .store(self.hits.load(Ordering::Relaxed) / 2, Ordering::Relaxed);
        }
        let hits = self.hits.load(Ordering::Relaxed);
        hits.saturating_mul(100) / probes.max(1) < FILTER_MAX_HIT_PERCENT
    }

    fn hash_and_insert_range(&mut self, start: usize) -> Result<(), String> {
        let end = self.keys.len();
        if start == end {
            return Ok(());
        }

        let slice = self.keys.slice(start, end - start);
        let new_hashes = with_hashes([&slice as &dyn Array], &self.state, |h| {
            Ok::<_, datafusion::common::DataFusionError>(h.to_vec())
        })
        .map_err(|e| e.to_string())?;
        self.hashes[start..end].copy_from_slice(&new_hashes);

        // Destructure so `insert_unique` can take `&mut table` while the hasher
        // closure borrows `hashes`.
        let Self {
            keys,
            hashes,
            table,
            filter,
            ..
        } = self;

        for idx in start..end {
            if keys.is_null(idx) {
                continue;
            }
            let hash = hashes[idx];
            let key = keys.value(idx);
            // ponytail: u32 indices cap the set at ~4.3B keys; widen to u64 if that is ever near.
            let idx_u32 = u32::try_from(idx)
                .map_err(|_| format!("ArrowKeySet exceeded u32::MAX keys at index {idx}"))?;

            if table
                .find(hash, |&existing| keys.value(existing as usize) == key)
                .is_some()
            {
                continue;
            }
            table.insert_unique(hash, idx_u32, |&i| hashes[i as usize]);
            // Same hash, same loop, same null-skip as the table. Duplicates are
            // skipped (bit-set is idempotent, so this is exact).
            filter.insert(hash);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn large_keys<'a>(vals: impl IntoIterator<Item = Option<&'a str>>) -> LargeStringArray {
        LargeStringArray::from(vals.into_iter().collect::<Vec<_>>())
    }

    fn utf8_needles<'a>(vals: impl IntoIterator<Item = Option<&'a str>>) -> StringArray {
        StringArray::from(vals.into_iter().collect::<Vec<_>>())
    }

    #[test]
    fn empty_set_misses_and_preserves_nulls() {
        // Built the way production builds it when the first load finds no rows,
        // so this also covers probing through an enabled-but-empty prefilter.
        let set = ArrowKeySet::from_keys(LargeStringArray::from(Vec::<&str>::new()))
            .expect("empty build");
        let needles = utf8_needles([Some("a"), None, Some("b")]);
        let out = set.contains_array(&needles).expect("probe");
        assert_eq!(out.len(), 3);
        assert!(!out.value(0));
        assert!(out.is_null(1));
        assert!(!out.value(2));
    }

    #[test]
    fn exact_hit_and_miss() {
        let set = ArrowKeySet::from_keys(large_keys([Some("alpha"), Some("beta")])).expect("build");
        let needles = utf8_needles([Some("alpha"), Some("gamma"), Some("beta")]);
        let out = set.contains_array(&needles).expect("probe");
        assert!(out.value(0));
        assert!(!out.value(1));
        assert!(out.value(2));
    }

    #[test]
    fn utf8_and_large_utf8_hash_identically() {
        let values = ["one", "two", "three-longer-string", ""];
        let utf8 = StringArray::from(values.to_vec());
        let large = LargeStringArray::from(values.to_vec());
        let state = RandomState::default();

        let utf8_hashes = with_hashes([&utf8 as &dyn Array], &state, |h| {
            Ok::<_, datafusion::common::DataFusionError>(h.to_vec())
        })
        .expect("hash utf8");
        let large_hashes = with_hashes([&large as &dyn Array], &state, |h| {
            Ok::<_, datafusion::common::DataFusionError>(h.to_vec())
        })
        .expect("hash large");

        assert_eq!(utf8_hashes, large_hashes);
    }

    #[test]
    fn utf8view_hashes_differ_from_utf8() {
        // Guard rail, not an aspiration. `create_hashes` sends Utf8/LargeUtf8
        // through the generic `hash_array` (hashed as `&str`) but Utf8View through
        // `hash_generic_byte_view_array`, which hashes the raw u128 view when the
        // string is inline (<=12 bytes) and the raw `&[u8]` otherwise. Rust's
        // `Hash for str` appends 0xff; `Hash for [u8]` writes a length prefix. So
        // the same content hashes DIFFERENTLY across these types.
        //
        // Consequence: storing a StringViewArray haystack and probing it with
        // Utf8 needle hashes makes every lookup miss — silently, with no error.
        // If this test ever starts failing because upstream unified the paths,
        // that is good news; delete it. Until then, keep hashing keys as `&str`.
        let values = ["one", "a-string-longer-than-twelve-bytes"];
        let utf8 = StringArray::from(values.to_vec());
        let view = arrow::array::StringViewArray::from(values.to_vec());
        let state = RandomState::default();

        let utf8_hashes = with_hashes([&utf8 as &dyn Array], &state, |h| {
            Ok::<_, datafusion::common::DataFusionError>(h.to_vec())
        })
        .expect("hash utf8");
        let view_hashes = with_hashes([&view as &dyn Array], &state, |h| {
            Ok::<_, datafusion::common::DataFusionError>(h.to_vec())
        })
        .expect("hash utf8view");

        assert_ne!(
            utf8_hashes, view_hashes,
            "Utf8 and Utf8View hashed the same; re-check the cross-type invariant \
             before relying on it"
        );
    }

    #[test]
    fn extend_preserves_existing_and_adds_new() {
        let mut set = ArrowKeySet::from_keys(large_keys([Some("a"), Some("b")])).expect("build");
        set.extend_from(large_keys([Some("c")])).expect("extend");
        assert_eq!(set.len(), 3);

        let needles = utf8_needles([Some("a"), Some("b"), Some("c"), Some("d")]);
        let out = set.contains_array(&needles).expect("probe");
        assert!(out.value(0));
        assert!(out.value(1));
        assert!(out.value(2));
        assert!(!out.value(3));
    }

    #[test]
    fn duplicates_do_not_grow_distinct_len() {
        let set = ArrowKeySet::from_keys(large_keys([Some("a"), Some("a")])).expect("build");
        assert_eq!(set.len(), 1);
        let needles = utf8_needles([Some("a")]);
        assert!(set.contains_array(&needles).expect("probe").value(0));
    }

    #[test]
    fn bulk_membership_is_exact() {
        const N: usize = 100_000;
        let present: Vec<String> = (0..N).map(|i| format!("key-{i}")).collect();
        let absent: Vec<String> = (0..N).map(|i| format!("miss-{i}")).collect();

        let haystack =
            LargeStringArray::from(present.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let set = ArrowKeySet::from_keys(haystack).expect("build");
        assert_eq!(set.len(), N);

        let present_needles =
            StringArray::from(present.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let present_out = set.contains_array(&present_needles).expect("present probe");
        assert_eq!(present_out.len(), N);
        assert_eq!(present_out.null_count(), 0);
        assert!(present_out.iter().all(|v| v == Some(true)));

        let absent_needles =
            StringArray::from(absent.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let absent_out = set.contains_array(&absent_needles).expect("absent probe");
        assert_eq!(absent_out.len(), N);
        assert_eq!(absent_out.null_count(), 0);
        assert!(absent_out.iter().all(|v| v == Some(false)));
    }

    #[test]
    fn nulls_in_haystack_are_skipped() {
        let set = ArrowKeySet::from_keys(large_keys([Some("a"), None, Some("b")])).expect("build");
        // Null haystack rows are not members; distinct len ignores them.
        assert_eq!(set.len(), 2);

        let needles = utf8_needles([Some("a"), None, Some("b"), Some("")]);
        let out = set.contains_array(&needles).expect("probe");
        assert!(out.value(0));
        assert!(out.is_null(1));
        assert!(out.value(2));
        assert!(!out.value(3));
    }

    #[test]
    fn duplicate_needles_resolve_consistently() {
        let set = ArrowKeySet::from_keys(large_keys([Some("a"), Some("c")])).expect("build");
        let needles = utf8_needles([
            Some("a"),
            Some("b"),
            Some("a"),
            Some("c"),
            Some("b"),
            None,
            Some("a"),
        ]);
        let out = set.contains_array(&needles).expect("probe");
        assert_eq!(out.len(), 7);
        assert!(out.value(0)); // a hit
        assert!(!out.value(1)); // b miss
        assert!(out.value(2)); // a repeat
        assert!(out.value(3)); // c hit
        assert!(!out.value(4)); // b repeat
        assert!(out.is_null(5)); // null stays null
        assert!(out.value(6)); // a repeat
    }

    #[test]
    fn heavy_duplication_is_exact() {
        const SET_N: usize = 5_000;
        const NEEDLE_N: usize = 50_000;
        const CYCLE: usize = 200; // 100 present + 100 absent

        let present: Vec<String> = (0..SET_N).map(|i| format!("key{i}")).collect();
        let haystack =
            LargeStringArray::from(present.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let set = ArrowKeySet::from_keys(haystack).expect("build");

        // Cycle over 100 present keys and 100 absent keys; null every 7th row.
        let needles: Vec<Option<String>> = (0..NEEDLE_N)
            .map(|i| {
                if i % 7 == 0 {
                    return None;
                }
                let slot = i % CYCLE;
                if slot < 100 {
                    Some(format!("key{slot}"))
                } else {
                    Some(format!("miss{}", slot - 100))
                }
            })
            .collect();
        let needle_array =
            StringArray::from(needles.iter().map(|o| o.as_deref()).collect::<Vec<_>>());
        let out = set.contains_array(&needle_array).expect("probe");

        assert_eq!(out.len(), NEEDLE_N);
        for (i, expected) in needles.iter().enumerate() {
            match expected {
                None => assert!(out.is_null(i), "row {i} should be null"),
                Some(s) if s.starts_with("key") => {
                    assert!(!out.is_null(i), "row {i} should be non-null");
                    assert!(out.value(i), "row {i} present key should hit");
                }
                Some(_) => {
                    assert!(!out.is_null(i), "row {i} should be non-null");
                    assert!(!out.value(i), "row {i} absent key should miss");
                }
            }
        }
    }

    #[test]
    fn all_distinct_needles_still_exact() {
        const N: usize = 1_000;
        let present: Vec<String> = (0..N / 2).map(|i| format!("p{i}")).collect();
        let haystack =
            LargeStringArray::from(present.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let set = ArrowKeySet::from_keys(haystack).expect("build");

        // All distinct: first half present, second half absent.
        let needles: Vec<String> = (0..N)
            .map(|i| {
                if i < N / 2 {
                    format!("p{i}")
                } else {
                    format!("m{i}")
                }
            })
            .collect();
        let needle_array =
            StringArray::from(needles.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let out = set.contains_array(&needle_array).expect("probe");

        assert_eq!(out.len(), N);
        assert_eq!(out.null_count(), 0);
        for i in 0..N / 2 {
            assert!(out.value(i), "present row {i}");
        }
        for i in N / 2..N {
            assert!(!out.value(i), "absent row {i}");
        }
    }

    #[test]
    fn empty_initial_load_still_gets_a_filter() {
        // A cache whose first load found an empty table must still gain a working
        // prefilter as deltas arrive — the disabled-filter path has no rebuild.
        let mut set = ArrowKeySet::from_keys(LargeStringArray::from(Vec::<&str>::new()))
            .expect("empty build");
        assert!(
            !set.filter.is_disabled(),
            "empty initial load left the filter disabled"
        );

        set.extend_from(large_keys([Some("a"), Some("b")]))
            .expect("extend");
        let out = set
            .contains_array(&utf8_needles([Some("a"), Some("b"), Some("zzz")]))
            .expect("probe");
        assert!(out.value(0));
        assert!(out.value(1));
        assert!(!out.value(2));
    }

    #[test]
    fn filter_sizings_agree() {
        const N: usize = 5_000;
        let present: Vec<String> = (0..N).map(|i| format!("present-{i}")).collect();
        let keys = LargeStringArray::from(present.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        let disabled = ArrowKeySet::from_keys_sized(keys.clone(), 0, 0).expect("build disabled");
        let six_bits = ArrowKeySet::from_keys_sized(keys.clone(), N * 6, 4).expect("build 6b/k");
        let ten_bits = ArrowKeySet::from_keys_sized(keys.clone(), N * 10, 6).expect("build 10b/k");
        assert!(disabled.filter.is_disabled());
        assert!(!six_bits.filter.is_disabled());
        assert!(!ten_bits.filter.is_disabled());

        // Mix: every present key, absent keys with nulls every 7th row.
        let mut needles: Vec<Option<String>> = Vec::with_capacity(2 * N);
        needles.extend((0..N).map(|i| Some(format!("present-{i}"))));
        needles.extend((0..N).map(|i| {
            if i % 7 == 0 {
                None
            } else {
                Some(format!("absent-{i}"))
            }
        }));
        let needle_array = utf8_needles(needles.iter().map(|o| o.as_deref()));

        let out_disabled = disabled
            .contains_array(&needle_array)
            .expect("probe disabled");
        let out_six = six_bits.contains_array(&needle_array).expect("probe 6b/k");
        let out_ten = ten_bits.contains_array(&needle_array).expect("probe 10b/k");

        // Sanity: the disabled (exact) result has the expected shape.
        assert!(out_disabled.iter().take(N).all(|v| v == Some(true)));
        assert_eq!(
            out_disabled.null_count(),
            (0..N).filter(|i| i % 7 == 0).count()
        );
        assert!(
            out_disabled
                .iter()
                .skip(N)
                .filter(|v| *v == Some(true))
                .count()
                == 0
        );

        // The filter must never change an answer, at any sizing.
        assert_eq!(out_disabled, out_six);
        assert_eq!(out_disabled, out_ten);
    }

    #[test]
    fn rebuild_keeps_membership_exact() {
        // capacity_keys floor is 1024; start tiny and extend past it so the
        // filter rebuilds at least once.
        let mut set =
            ArrowKeySet::from_keys_sized(large_keys([Some("k0"), Some("k1")]), 1024 * 10, 6)
                .expect("build");
        assert!(!set.filter.is_disabled());
        let initial_capacity = set.filter.capacity_keys();
        assert_eq!(initial_capacity, 1024);

        let mut inserted: Vec<String> = vec!["k0".to_string(), "k1".to_string()];
        for chunk in 0..5 {
            let extra: Vec<String> = (0..500).map(|i| format!("k{chunk}-{i}")).collect();
            set.extend_from(large_keys(extra.iter().map(|s| Some(s.as_str()))))
                .expect("extend");
            inserted.extend(extra);
        }
        assert!(
            set.len() > initial_capacity,
            "extend must outgrow the initial filter capacity"
        );
        assert!(
            set.filter.capacity_keys() > initial_capacity,
            "extend must have rebuilt the filter"
        );

        // Every key ever inserted must still hit; known-absent keys must miss.
        let mut needles: Vec<Option<String>> = inserted.iter().map(|s| Some(s.clone())).collect();
        needles.extend((0..500).map(|i| Some(format!("absent-{i}"))));
        let out = set
            .contains_array(&utf8_needles(needles.iter().map(|o| o.as_deref())))
            .expect("probe");
        assert_eq!(out.len(), needles.len());
        for (i, n) in needles.iter().enumerate() {
            match n {
                Some(s) if s.starts_with("absent") => {
                    assert!(!out.value(i), "absent {s} must miss at row {i}")
                }
                Some(_) => assert!(out.value(i), "inserted key must hit at row {i}"),
                None => unreachable!("no nulls in this test"),
            }
        }
    }

    fn assert_equals(pattern: &str, out: &BooleanArray, expected_len: usize, is_hit: bool) {
        assert_eq!(out.len(), expected_len, "{pattern}: wrong length");
        for i in 0..expected_len {
            assert!(!out.is_null(i), "{pattern}: unexpected null at row {i}");
            assert_eq!(out.value(i), is_hit, "{pattern}: wrong answer at row {i}");
        }
    }

    #[test]
    fn filter_disables_itself_when_hits_dominate() {
        // All hits — exactly the workload where the filter is a tax. Probe well
        // past warmup so `filter_is_paying_off` must flip to false, and assert
        // every result stays exactly correct throughout (the whole point of the
        // guard is that flipping the switch never changes an answer).
        let present: Vec<String> = (0..64).map(|i| format!("present-{i}")).collect();
        let set = ArrowKeySet::from_keys(large_keys(present.iter().map(|s| Some(s.as_str()))))
            .expect("build");
        // Repeat the present keys so every needle is a hit.
        let needle_vals: Vec<String> = (0..1024).map(|i| format!("present-{}", i % 64)).collect();
        let needles = utf8_needles(needle_vals.iter().map(|s| Some(s.as_str())));

        let rounds = (FILTER_WARMUP_PROBES as usize / needles.len()) + 5;
        for _ in 0..rounds {
            let out = set.contains_array(&needles).expect("probe");
            assert_equals("hit-heavy", &out, needles.len(), true);
        }
        // Enough probes accumulated to cross warmup; the guard must have turned
        // the filter off (hit rate ~100% > 15%), yet answers are still exact.
        assert!(
            set.probes.load(Ordering::Relaxed) >= FILTER_WARMUP_PROBES,
            "test must have crossed warmup"
        );
        assert!(
            !set.filter_is_paying_off(),
            "hit-heavy workload must disable the filter"
        );
    }

    #[test]
    fn filter_stays_on_for_miss_heavy_workload() {
        // Mostly-absent needles — the workload the filter is built to win. Probe
        // past warmup; the filter must stay on and answers stay exactly correct.
        let present: Vec<String> = (0..64).map(|i| format!("present-{i}")).collect();
        let set = ArrowKeySet::from_keys(large_keys(present.iter().map(|s| Some(s.as_str()))))
            .expect("build");
        // Mostly absent, a sprinkle of present to make it a realistic mix.
        let mut raw: Vec<String> = (0..1024).map(|i| format!("absent-{i}")).collect();
        raw[0] = "present-0".to_string();
        raw[511] = "present-31".to_string();
        let needles = utf8_needles(raw.iter().map(|s| Some(s.as_str())));

        let rounds = (FILTER_WARMUP_PROBES as usize / needles.len()) + 5;
        for _ in 0..rounds {
            let out = set.contains_array(&needles).expect("probe");
            // Rows 0 and 511 are present (hits); the rest are absent misses.
            for i in 0..out.len() {
                let expect_hit = i == 0 || i == 511;
                assert!(!out.is_null(i), "miss-heavy: null at row {i}");
                assert_eq!(
                    out.value(i),
                    expect_hit,
                    "miss-heavy: wrong answer at row {i}"
                );
            }
        }
        assert!(
            set.probes.load(Ordering::Relaxed) >= FILTER_WARMUP_PROBES,
            "test must have crossed warmup"
        );
        // ~0.2% hits < 15%: the filter must still be considered worth keeping.
        assert!(
            set.filter_is_paying_off(),
            "miss-heavy workload must keep the filter on"
        );
    }

    #[test]
    fn hit_rate_guard_never_changes_results() {
        // A filter-enabled set and a filter-disabled set (`filter_bits = 0`)
        // MUST produce identical answers for every batch, including while the
        // enabled set crosses warmup and its guard flips the filter off.
        let present: Vec<String> = (0..64).map(|i| format!("present-{i}")).collect();
        let keys = large_keys(present.iter().map(|s| Some(s.as_str())));
        let enabled = ArrowKeySet::from_keys_sized(keys.clone(), 64 * 64, 6).expect("build");
        let disabled = ArrowKeySet::from_keys_sized(keys, 0, 0).expect("build");

        // Alternate hit-heavy and miss-heavy batches so the hit rate ends up
        // crossing warmup with a mixed signal; RandomState is deterministic per
        // batch, so answers are comparable across the two sets.
        let hit_vals: Vec<String> = (0..1024).map(|i| format!("present-{}", i % 64)).collect();
        let hit_needles = utf8_needles(hit_vals.iter().map(|s| Some(s.as_str())));
        let miss_vals: Vec<String> = (0..1024).map(|i| format!("absent-{i}")).collect();
        let miss_needles = utf8_needles(miss_vals.iter().map(|s| Some(s.as_str())));

        let rounds = (FILTER_WARMUP_PROBES as usize / 1024) + 5;
        for round in 0..rounds {
            // Alternate pure-hit and pure-miss rounds: a mixed signal that
            // crosses warmup (filter on at first) and then, with ~50% hits,
            // trips the guard off — exercising both sides of the switch.
            let needles = if round % 2 == 0 {
                &hit_needles
            } else {
                &miss_needles
            };
            let a = enabled.contains_array(needles).expect("enabled probe");
            let b = disabled.contains_array(needles).expect("disabled probe");
            assert_eq!(
                a, b,
                "enabled and disabled sets must agree at round {round}"
            );
        }
        assert!(
            enabled.probes.load(Ordering::Relaxed) >= FILTER_WARMUP_PROBES,
            "test must have crossed warmup"
        );
    }
}

#[cfg(test)]
mod bench {
    use super::*;
    use arrow::array::{DictionaryArray, StringDictionaryBuilder};
    use arrow::compute::take;
    use arrow::datatypes::Int32Type;
    use std::collections::HashSet;
    use std::hint::black_box;
    use std::time::Instant;

    const N_KEYS: usize = 1_400_000;
    const BATCH_SIZE: usize = 50;
    const N_BATCHES: usize = 20_000;
    const SEED: u64 = 0xC0FF_EE00_D15C_AFFE;

    fn next(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// `"0x"` + 40 lowercase hex chars from two SplitMix64 draws (+ 32 derived bits).
    fn gen_addr(state: &mut u64) -> String {
        let a = next(state);
        let b = next(state);
        format!(
            "0x{a:016x}{b:016x}{:08x}",
            a.wrapping_mul(0x9E37_79B9).wrapping_add(b) as u32
        )
    }

    /// Exact replica of the deleted `PostgresDynamicTableBackend::build_contains_result`
    /// (minus the per-row `trace!`).
    fn build_contains_result(
        string_array: &StringArray,
        existing_set: &HashSet<Box<str>>,
    ) -> BooleanArray {
        let mut builder = BooleanBuilder::with_capacity(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                builder.append_null();
            } else {
                let value = string_array.value(i);
                let contains_value = existing_set.contains(value);
                builder.append_value(contains_value);
            }
        }
        builder.finish()
    }

    fn checksum(arr: &BooleanArray) -> u64 {
        let mut n = 0u64;
        for i in 0..arr.len() {
            if arr.is_null(i) || arr.value(i) {
                n += 1;
            }
        }
        n
    }

    fn run_hashset(set: &HashSet<Box<str>>, batches: &[StringArray]) -> (std::time::Duration, u64) {
        let start = Instant::now();
        let mut sum = 0u64;
        for batch in batches {
            let out = build_contains_result(batch, set);
            sum += checksum(black_box(&out));
        }
        (start.elapsed(), sum)
    }

    fn run_arrow(set: &ArrowKeySet, batches: &[StringArray]) -> (std::time::Duration, u64) {
        let start = Instant::now();
        let mut sum = 0u64;
        for batch in batches {
            let out = set.contains_array(batch).expect("probe");
            sum += checksum(black_box(&out));
        }
        (start.elapsed(), sum)
    }

    /// Dictionary-encode a batch: K distinct values plus Int32 keys. Built OUTSIDE
    /// the timed section on purpose — the premise is that the data ARRIVES
    /// dictionary-encoded (parquet/avro sources produce it). If we had to encode
    /// it ourselves per batch we would just be rebuilding grouping by hand, which
    /// is exactly the software dedup that measured slower.
    fn to_dict(batch: &StringArray) -> DictionaryArray<Int32Type> {
        let mut b = StringDictionaryBuilder::<Int32Type>::new();
        for i in 0..batch.len() {
            if batch.is_null(i) {
                b.append_null();
            } else {
                b.append_value(batch.value(i));
            }
        }
        b.finish()
    }

    /// Probe only the K distinct dictionary values, then map answers onto the N
    /// rows with `take` — the encoding supplies the dedup for free.
    fn run_dict(
        set: &ArrowKeySet,
        dicts: &[DictionaryArray<Int32Type>],
    ) -> (std::time::Duration, u64) {
        let start = Instant::now();
        let mut sum = 0u64;
        for d in dicts {
            let values = d
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("dict values are Utf8");
            let vc = set.contains_array(values).expect("probe");
            let out = take(&vc, d.keys(), None).expect("take");
            let b = out
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("take of boolean is boolean");
            sum += checksum(black_box(b));
        }
        (start.elapsed(), sum)
    }

    /// Min of `reps` runs: the benchmark is memory-latency bound and this box is
    /// shared, so the minimum is the robust statistic — interference only ever
    /// makes a run slower.
    fn best_of<F: FnMut() -> (std::time::Duration, u64)>(
        reps: usize,
        mut f: F,
    ) -> (std::time::Duration, u64) {
        let mut best = std::time::Duration::MAX;
        let mut sum = 0u64;
        for _ in 0..reps {
            let (d, s) = f();
            sum = s;
            if d < best {
                best = d;
            }
        }
        (best, sum)
    }

    /// Fraction of needles the prefilter lets through. Called with all-absent
    /// needles, this IS the measured false-positive rate.
    fn filter_pass_rate(set: &ArrowKeySet, batches: &[StringArray]) -> f64 {
        let mut pass = 0usize;
        let mut total = 0usize;
        for b in batches.iter().take(2_000) {
            let (p, t) = with_hashes([b as &dyn Array], &set.state, |hashes| {
                let mut p = 0usize;
                let mut t = 0usize;
                for (i, &h) in hashes.iter().enumerate() {
                    if b.is_null(i) {
                        continue;
                    }
                    t += 1;
                    if set.filter.maybe_contains(h) {
                        p += 1;
                    }
                }
                Ok::<_, datafusion::common::DataFusionError>((p, t))
            })
            .expect("hash");
            pass += p;
            total += t;
        }
        pass as f64 / total.max(1) as f64
    }

    fn report(scenario: &str, impl_name: &str, elapsed: std::time::Duration) {
        let total_ms = elapsed.as_secs_f64() * 1_000.0;
        let per_batch_us = elapsed.as_secs_f64() * 1_000_000.0 / N_BATCHES as f64;
        let per_probe_ns =
            elapsed.as_secs_f64() * 1_000_000_000.0 / (N_BATCHES * BATCH_SIZE) as f64;
        println!(
            "scenario={scenario} impl={impl_name}  total={total_ms:.1}ms  per_batch={per_batch_us:.2}us  per_probe={per_probe_ns:.0}ns"
        );
    }

    fn report_ratio(scenario: &str, hashset: std::time::Duration, arrow: std::time::Duration) {
        let hs = hashset.as_secs_f64();
        let ar = arrow.as_secs_f64();
        if ar <= hs {
            println!(
                "scenario={scenario} arrow is {:.2}x faster",
                hs / ar.max(f64::EPSILON)
            );
        } else {
            println!(
                "scenario={scenario} arrow is {:.2}x slower",
                ar / hs.max(f64::EPSILON)
            );
        }
    }

    #[test]
    #[ignore = "perf benchmark, run explicitly with --release --ignored --nocapture"]
    fn probe_throughput_1_4m_keys_batches_of_50() {
        let mut state = SEED;

        // --- build 1.4M distinct EVM-address-shaped keys ---
        let mut dedup = HashSet::with_capacity(N_KEYS);
        let mut keys: Vec<String> = Vec::with_capacity(N_KEYS);
        while keys.len() < N_KEYS {
            let k = gen_addr(&mut state);
            if dedup.insert(k.clone()) {
                keys.push(k);
            }
        }
        drop(dedup);

        // --- build both structures from the identical key slice ---
        let hashset: HashSet<Box<str>> = keys.iter().map(|s| s.as_str().into()).collect();
        let arrow = ArrowKeySet::from_keys(LargeStringArray::from(
            keys.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        ))
        .expect("ArrowKeySet::from_keys");
        assert_eq!(hashset.len(), N_KEYS);
        assert_eq!(arrow.len(), N_KEYS);

        // membership oracle for miss generation (dropped before timing)
        let lookup: HashSet<&str> = keys.iter().map(|s| s.as_str()).collect();

        // --- pre-generate ALL needle batches ---
        let mut all_hit = Vec::with_capacity(N_BATCHES);
        for _ in 0..N_BATCHES {
            let batch: Vec<&str> = (0..BATCH_SIZE)
                .map(|_| {
                    let idx = (next(&mut state) as usize) % N_KEYS;
                    keys[idx].as_str()
                })
                .collect();
            all_hit.push(StringArray::from(batch));
        }

        let mut all_miss = Vec::with_capacity(N_BATCHES);
        for _ in 0..N_BATCHES {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            while batch.len() < BATCH_SIZE {
                let k = gen_addr(&mut state);
                if !lookup.contains(k.as_str()) {
                    batch.push(k);
                }
            }
            all_miss.push(StringArray::from(
                batch.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            ));
        }

        let mut mixed = Vec::with_capacity(N_BATCHES);
        for _ in 0..N_BATCHES {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            for i in 0..BATCH_SIZE {
                if i % 2 == 0 {
                    let idx = (next(&mut state) as usize) % N_KEYS;
                    batch.push(keys[idx].clone());
                } else {
                    loop {
                        let k = gen_addr(&mut state);
                        if !lookup.contains(k.as_str()) {
                            batch.push(k);
                            break;
                        }
                    }
                }
            }
            mixed.push(StringArray::from(
                batch.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            ));
        }
        // Duplicate-heavy: 50 rows carrying only 5 distinct (present) values. This is
        // the shape the batch-local dedup exists for; the other scenarios are
        // effectively all-distinct and measure only its overhead.
        let mut dup_heavy = Vec::with_capacity(N_BATCHES);
        for _ in 0..N_BATCHES {
            let distinct: Vec<&str> = (0..5)
                .map(|_| keys[(next(&mut state) as usize) % N_KEYS].as_str())
                .collect();
            let batch: Vec<&str> = (0..BATCH_SIZE)
                .map(|_| distinct[(next(&mut state) as usize) % distinct.len()])
                .collect();
            dup_heavy.push(StringArray::from(batch));
        }

        // 98% miss / 2% hit: the stated production shape.
        let mut miss98 = Vec::with_capacity(N_BATCHES);
        for _ in 0..N_BATCHES {
            let mut batch: Vec<String> = Vec::with_capacity(BATCH_SIZE);
            batch.push(keys[(next(&mut state) as usize) % N_KEYS].clone());
            while batch.len() < BATCH_SIZE {
                let k = gen_addr(&mut state);
                if !lookup.contains(k.as_str()) {
                    batch.push(k);
                }
            }
            miss98.push(StringArray::from(
                batch.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            ));
        }

        drop(lookup);

        // A tiny, fully cache-resident set built from the same key space. If probing it
        // is no faster than probing the 1.4M set, then memory latency is NOT what a miss
        // costs — and a Bloom/roaring prefilter (which can only accelerate misses) has no
        // headroom to capture.
        let small = ArrowKeySet::from_keys(LargeStringArray::from(
            keys[..2_000].iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        ))
        .expect("small set");

        // --- memory (outside timed section) ---
        let arrow_keys_bytes = arrow.keys.get_buffer_memory_size();
        let arrow_hashes_bytes = arrow.hashes.len() * 8;
        let arrow_table_bytes = arrow.table.capacity() * 4;
        println!(
            "memory arrow: keys_buffers={arrow_keys_bytes} hashes={arrow_hashes_bytes} table_capacity_u32s={} table_bytes={arrow_table_bytes} total={}",
            arrow.table.capacity(),
            arrow_keys_bytes + arrow_hashes_bytes + arrow_table_bytes
        );
        let box_str_size = std::mem::size_of::<Box<str>>();
        // ESTIMATE only — not measured. std HashSet<Box<str>> is a hashbrown RawTable:
        // the `Box<str>` fat pointer lives inline in a table slot (plus one control byte
        // per bucket), and each key's bytes are a separate heap allocation. Counting the
        // fat pointer per key *and* per slot would double-count it.
        let hashset_est = N_KEYS * (42 + 16) + hashset.capacity() * (box_str_size + 1);
        println!(
            "memory hashset ESTIMATE (not measured): N*(42key+16malloc) + capacity*(size_of::<Box<str>>()+1ctrl) = {hashset_est}  (N={N_KEYS}, Box<str>={box_str_size}, capacity={})",
            hashset.capacity()
        );

        let scenarios: [(&str, &[StringArray]); 5] = [
            ("all_hit", &all_hit),
            ("all_miss", &all_miss),
            ("mixed", &mixed),
            ("dup_heavy_5of50", &dup_heavy),
            ("miss98_1hit_of50", &miss98),
        ];

        // --- warm-up (untimed): one pass each scenario × structure ---
        for (_, batches) in scenarios {
            let _ = run_hashset(&hashset, &batches[..1.min(batches.len())]);
            let _ = run_arrow(&arrow, &batches[..1.min(batches.len())]);
        }

        // --- decisive check: same all_miss needles against 1.4M keys vs 2k keys ---
        const REPS_SMALL: usize = 3;
        let (big_miss, _) = best_of(REPS_SMALL, || run_arrow(&arrow, &all_miss));
        let (small_miss, _) = best_of(REPS_SMALL, || run_arrow(&small, &all_miss));
        report("all_miss_1.4M_keys", "arrow", big_miss);
        report("all_miss_2k_keys", "arrow", small_miss);
        println!(
            "MEMORY-BOUND CHECK: shrinking the set 700x changes a miss by {:.2}x  (near 1.00x => misses are NOT memory bound => a prefilter cannot help)",
            big_miss.as_secs_f64() / small_miss.as_secs_f64()
        );
        println!();

        // --- prefilter sizing sweep: does the L2 cliff exist? ---
        // Baselines from the already-built `arrow` set; every sizing must match them.
        let (_, baseline_miss) = best_of(1, || run_arrow(&arrow, &all_miss));
        let (_, baseline_98) = best_of(1, || run_arrow(&arrow, &miss98));
        let (_, baseline_hit) = best_of(1, || run_arrow(&arrow, &all_hit));
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let mk = |bits: usize, k: u32| {
            ArrowKeySet::from_keys_sized(LargeStringArray::from(key_refs.clone()), bits, k)
                .expect("sized build")
        };
        let sizings: [(&str, usize, u32); 4] = [
            ("nofilter", 0, 0),
            ("bloom_1.0MB_k4", 8_388_608, 4),
            ("bloom_1.75MB_k6", 14_680_064, 6),
            ("bloom_3.5MB_k6", 29_360_128, 6),
        ];
        println!("--- prefilter sizing sweep (L2=1MB/core, L3=32MB, exact table=7.34MB) ---");
        for (label, bits, k) in sizings {
            let set = mk(bits, k);
            let fpr = if bits == 0 {
                1.0
            } else {
                filter_pass_rate(&set, &all_miss)
            };
            let (d_miss, s_miss) = best_of(3, || run_arrow(&set, &all_miss));
            let (d_98, s_98) = best_of(3, || run_arrow(&set, &miss98));
            let (d_hit, s_hit) = best_of(3, || run_arrow(&set, &all_hit));
            // Every sizing must agree with the no-filter baseline, or the
            // prefilter is dropping real members (a false negative).
            assert_eq!(
                s_miss, baseline_miss,
                "filter {label} changed all_miss results"
            );
            assert_eq!(s_98, baseline_98, "filter {label} changed miss98 results");
            assert_eq!(
                s_hit, baseline_hit,
                "filter {label} changed all_hit results"
            );
            println!(
                "filter={label:16} bytes={:>9} measured_FPR={:>6.2}%",
                set.filter.bytes(),
                fpr * 100.0
            );
            report("  all_miss", label, d_miss);
            report("  miss98", label, d_98);
            report("  all_hit", label, d_hit);
        }
        println!();

        // --- dictionary-encoded probe vs plain row-by-row probe ---
        // `dup_heavy` has 5 distinct values per 50 rows, so the dictionary should
        // win big there; the all-distinct scenarios show what `take` costs when
        // there is no duplication to exploit.
        println!("--- dictionary probe (K distinct values + take) vs plain N-row probe ---");
        for (label, batches) in [
            ("dup_heavy_5of50", &dup_heavy),
            ("all_miss", &all_miss),
            ("miss98", &miss98),
        ] {
            let dicts: Vec<DictionaryArray<Int32Type>> = batches.iter().map(to_dict).collect();
            let distinct: usize =
                dicts.iter().map(|d| d.values().len()).sum::<usize>() / dicts.len();
            let (d_plain, s_plain) = best_of(3, || run_arrow(&arrow, batches));
            let (d_dict, s_dict) = best_of(3, || run_dict(&arrow, &dicts));
            assert_eq!(
                s_plain, s_dict,
                "dictionary path changed results for {label}"
            );
            report(label, "plain", d_plain);
            report(label, "dictionary", d_dict);
            let pl = d_plain.as_secs_f64();
            let dc = d_dict.as_secs_f64();
            if dc <= pl {
                println!(
                    "scenario={label} dictionary is {:.2}x faster (avg {distinct} distinct of {BATCH_SIZE} rows)",
                    pl / dc
                );
            } else {
                println!(
                    "scenario={label} dictionary is {:.2}x SLOWER (avg {distinct} distinct of {BATCH_SIZE} rows)",
                    dc / pl
                );
            }
        }
        println!();

        // --- timed runs: best of REPS, all three implementations ---
        const REPS: usize = 3;
        for (scenario, batches) in scenarios {
            let (hs_elapsed, hs_sum) = best_of(REPS, || run_hashset(&hashset, batches));
            let (ar_elapsed, ar_sum) = best_of(REPS, || run_arrow(&arrow, batches));
            assert_eq!(
                hs_sum, ar_sum,
                "checksum mismatch scenario={scenario}: hashset={hs_sum} arrow={ar_sum}"
            );
            report(scenario, "hashset", hs_elapsed);
            report(scenario, "arrow", ar_elapsed);
            report_ratio(scenario, hs_elapsed, ar_elapsed);
            println!();
        }
    }
}
