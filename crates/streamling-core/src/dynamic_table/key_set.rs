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
    pub(crate) fn new() -> Self {
        Self {
            keys: LargeStringArray::from_iter_values(std::iter::empty::<&str>()),
            hashes: Vec::new(),
            state: RandomState::default(),
            table: HashTable::new(),
        }
    }

    pub(crate) fn from_keys(keys: LargeStringArray) -> Result<Self, String> {
        if keys.is_empty() {
            return Ok(Self::new());
        }
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
        self.hash_and_insert_range(start)
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
        let set = ArrowKeySet::new();
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
}
