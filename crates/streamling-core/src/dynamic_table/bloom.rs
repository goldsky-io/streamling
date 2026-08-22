//! Blocked Bloom filter prefilter for [`ArrowKeySet`](super::key_set::ArrowKeySet).
//!
//! All `k` bits for a key live in one 64-byte block, so a probe touches exactly
//! one cache line. The exact table still decides every positive; the filter only
//! ever answers "definitely absent" cheaply.

/// Blocked Bloom filter: all k bits for a key live in one 64-byte block, so a
/// query touches exactly one cache line.
///
/// `with_bits(0, ..)` is DISABLED: no allocation, and `maybe_contains` always
/// returns `true` so every probe falls through to the exact table. This gives a
/// zero-overhead baseline through the same code path.
#[derive(Debug)]
pub(crate) struct BlockedBloom {
    blocks: Vec<[u64; 8]>, // 64 bytes = one cache line = 512 bits
    k: u32,
    capacity_keys: usize,
}

/// Odd 64-bit multiplier for the multiply-shift position extraction.
const POSITION_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

impl BlockedBloom {
    /// Round `bits` up to a whole number of 512-bit blocks. `bits == 0` means
    /// DISABLED: no allocation, and every probe falls through to the caller's
    /// exact structure.
    pub(crate) fn with_bits(bits: usize, k: u32, capacity_keys: usize) -> Self {
        let n_blocks = if bits == 0 { 0 } else { bits.div_ceil(512) };
        Self {
            blocks: vec![[0u64; 8]; n_blocks],
            k,
            capacity_keys,
        }
    }

    #[inline]
    pub(crate) fn is_disabled(&self) -> bool {
        self.blocks.is_empty()
    }

    #[inline]
    pub(crate) fn insert(&mut self, hash: u64) {
        if self.is_disabled() {
            return;
        }
        // Block index from the HIGH 32 bits via multiply-shift: no division,
        // and the result is uniform over [0, blocks.len()).
        let block_idx = (((hash >> 32) * self.blocks.len() as u64) >> 32) as usize;
        let block = &mut self.blocks[block_idx];

        // Kirsch-Mitzenmacher double hashing: b_i = h1 + i*h2, with h2 forced
        // odd so the step is a permutation of u32 (the k positions can never
        // fold back onto each other). Position index = high 9 bits of a 64-bit
        // multiply-shift of b_i; multiply-shift is a universal hash family, so
        // the 9-bit positions are well mixed for any input.
        let h1 = hash as u32;
        let h2 = (h1 ^ (h1 >> 15)).wrapping_mul(0x9E37_79B9) | 1;
        let mut acc = h1;
        for _ in 0..self.k {
            let pos = ((acc as u64).wrapping_mul(POSITION_MIX) >> 55) as usize;
            block[pos >> 6] |= 1u64 << (pos & 63);
            acc = acc.wrapping_add(h2);
        }
    }

    /// `false` means the key is DEFINITELY ABSENT (the only answer this filter
    /// is allowed to give); `true` means maybe present — the exact structure
    /// must be consulted.
    #[inline]
    pub(crate) fn maybe_contains(&self, hash: u64) -> bool {
        if self.is_disabled() {
            return true;
        }
        let block_idx = (((hash >> 32) * self.blocks.len() as u64) >> 32) as usize;
        let block = &self.blocks[block_idx];
        let h1 = hash as u32;
        let h2 = (h1 ^ (h1 >> 15)).wrapping_mul(0x9E37_79B9) | 1;
        let mut acc = h1;
        for _ in 0..self.k {
            let pos = ((acc as u64).wrapping_mul(POSITION_MIX) >> 55) as usize;
            if block[pos >> 6] & (1u64 << (pos & 63)) == 0 {
                return false;
            }
            acc = acc.wrapping_add(h2);
        }
        true
    }

    #[inline]
    pub(crate) fn bytes(&self) -> usize {
        self.blocks.len() * std::mem::size_of::<[u64; 8]>()
    }

    #[inline]
    pub(crate) fn capacity_keys(&self) -> usize {
        self.capacity_keys
    }

    /// Number of hash functions; needed to rebuild the filter at a larger
    /// sizing without changing its per-key false-positive behavior.
    #[inline]
    pub(crate) fn k(&self) -> u32 {
        self.k
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic SplitMix64 stream (bijective in state, so successive
    /// outputs are guaranteed distinct).
    fn splitmix64(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[test]
    fn disabled_filter_always_says_maybe() {
        let filter = BlockedBloom::with_bits(0, 6, 1024);
        assert!(filter.is_disabled());
        assert_eq!(filter.bytes(), 0);

        let mut state = 0x1234_5678_9ABC_DEF0;
        for _ in 0..10_000 {
            assert!(filter.maybe_contains(splitmix64(&mut state)));
        }

        // Insert is a no-op on a disabled filter.
        let mut f = BlockedBloom::with_bits(0, 6, 1024);
        f.insert(0xDEAD_BEEF_CAFE_F00D);
        assert!(f.maybe_contains(0xDEAD_BEEF_CAFE_F00D));
    }

    #[test]
    fn never_false_negative() {
        const N: usize = 200_000;
        let mut state = 0x1234_5678_9ABC_DEF0;
        let hashes: Vec<u64> = (0..N).map(|_| splitmix64(&mut state)).collect();

        let mut filter = BlockedBloom::with_bits(N * 10, 6, N * 2);
        assert!(!filter.is_disabled());
        for &h in &hashes {
            filter.insert(h);
        }

        for &h in &hashes {
            assert!(
                filter.maybe_contains(h),
                "false negative for inserted hash {h:#x}"
            );
        }
    }

    #[test]
    fn fpr_is_near_theory_at_10_bits() {
        const N: usize = 100_000;
        const PROBES: usize = 200_000;

        let mut state = 0xC0FF_EE00_D15C_AFFE;
        let mut filter = BlockedBloom::with_bits(N * 10, 6, N * 2);
        for _ in 0..N {
            filter.insert(splitmix64(&mut state));
        }

        // Different seed => a different SplitMix64 stream, so probe hashes do
        // not overlap the inserted set. Theory FPR ~1%; 5% is loose enough for
        // blocking effects but tight enough to catch broken bit mixing.
        let mut probe_state = 0x0BAD_5EED_4EAD_1BEE;
        let mut fps = 0usize;
        for _ in 0..PROBES {
            if filter.maybe_contains(splitmix64(&mut probe_state)) {
                fps += 1;
            }
        }
        let fpr = fps as f64 / PROBES as f64;
        assert!(
            fpr < 0.05,
            "FPR {fpr:.4} ({fps}/{PROBES}) far above theory ~1% — bit derivation is broken"
        );
    }

    #[test]
    fn fpr_is_usable_at_6_bits() {
        const N: usize = 100_000;
        const PROBES: usize = 200_000;

        let mut state = 0x0DD_BA11_F00D_5EED;
        let mut filter = BlockedBloom::with_bits(N * 6, 4, N * 2);
        for _ in 0..N {
            filter.insert(splitmix64(&mut state));
        }

        let mut probe_state = 0xFACE_B00C_DEED_FACE;
        let mut fps = 0usize;
        for _ in 0..PROBES {
            if filter.maybe_contains(splitmix64(&mut probe_state)) {
                fps += 1;
            }
        }
        let fpr = fps as f64 / PROBES as f64;
        assert!(
            fpr < 0.15,
            "FPR {fpr:.4} ({fps}/{PROBES}) unusable at 6 bits/key — bit derivation is broken"
        );
    }
}
