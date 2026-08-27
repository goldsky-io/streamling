use arrow::array::{Array, BooleanArray, BooleanBuilder, LargeStringArray, StringArray};
use arrow::compute::concat;
use datafusion::common::hash_utils::{RandomState, with_hashes};
use hashbrown::HashTable;

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
}

impl ArrowKeySet {
    pub(crate) fn from_keys(keys: LargeStringArray) -> Result<Self, String> {
        // Deliberately no early return for empty keys. A shortcut that early-returned
        // a degenerate empty set here would leave a cache whose first load found an
        // empty table unable to work as it fills up. `hash_and_insert_range` is already
        // a no-op when there is nothing to add, so building unconditionally is free.
        let mut set = Self {
            hashes: vec![0; keys.len()],
            keys,
            state: RandomState::default(),
            table: HashTable::new(),
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
        // NOTE: concat copies the whole key buffer, so a refresh is O(total bytes).
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
        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.table.len()
    }

    pub(crate) fn contains_array(&self, needles: &StringArray) -> Result<BooleanArray, String> {
        // Haystack is LargeUtf8, needle is Utf8 — hashes match for equal `&str`
        // content (see struct-level comment).
        with_hashes([needles as &dyn Array], &self.state, |hashes| {
            let mut builder = BooleanBuilder::with_capacity(needles.len());
            for (i, &hash) in hashes.iter().enumerate() {
                if needles.is_null(i) {
                    // `hash_array` only writes valid indices; null slots hold garbage.
                    builder.append_null();
                    continue;
                }
                let needle = needles.value(i);
                let found = self
                    .table
                    .find(hash, |&idx| self.keys.value(idx as usize) == needle)
                    .is_some();
                builder.append_value(found);
            }
            Ok(builder.finish())
        })
        .map_err(|e| e.to_string())
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
            ..
        } = self;

        for idx in start..end {
            if keys.is_null(idx) {
                continue;
            }
            let hash = hashes[idx];
            let key = keys.value(idx);
            // NOTE: u32 indices cap the set at ~4.3B keys; widen to u64 if that is ever near.
            let idx_u32 = u32::try_from(idx)
                .map_err(|_| format!("ArrowKeySet exceeded u32::MAX keys at index {idx}"))?;

            if table
                .find(hash, |&existing| keys.value(existing as usize) == key)
                .is_some()
            {
                continue;
            }
            table.insert_unique(hash, idx_u32, |&i| hashes[i as usize]);
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
    fn extend_from_makes_new_keys_immediately_visible() {
        // Property the append-path cache update relies on: after `extend_from`,
        // probes see the new keys with no intermediate step.
        let mut set = ArrowKeySet::from_keys(large_keys([Some("existing")])).expect("build");
        set.extend_from(large_keys([Some("just-appended")]))
            .expect("extend");
        let out = set
            .contains_array(&utf8_needles([
                Some("existing"),
                Some("just-appended"),
                Some("missing"),
            ]))
            .expect("probe");
        assert!(out.value(0));
        assert!(out.value(1));
        assert!(!out.value(2));
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
    fn empty_initial_load_then_extend_works() {
        // A cache whose first load found an empty table must still work as it
        // fills: `from_keys` must not early-return a degenerate empty set, because
        // a cache whose first load finds an empty table still has to work as it
        // fills.
        let mut set = ArrowKeySet::from_keys(LargeStringArray::from(Vec::<&str>::new()))
            .expect("empty build");

        set.extend_from(large_keys([Some("a"), Some("b")]))
            .expect("extend");
        let out = set
            .contains_array(&utf8_needles([Some("a"), Some("b"), Some("zzz")]))
            .expect("probe");
        assert!(out.value(0));
        assert!(out.value(1));
        assert!(!out.value(2));
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
