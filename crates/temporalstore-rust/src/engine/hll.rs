// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bounded approximate distinct count via a dense HyperLogLog sketch.
//!
//! Control State distinct/CHANGE counts are otherwise exact `BTreeSet` unions, whose memory
//! grows with cardinality — unbounded for high-cardinality long-window distincts. This sketch
//! gives a fixed-memory (2^P registers) approximate count, mergeable register-wise, so it can
//! back a bounded distinct mode (exact until a threshold, then sketch — the C++ DC→LDC shape).
//!
//! Fixed, version-stable hashing (FNV-1a + splitmix64 finalizer) so a persisted sketch stays
//! valid across builds. P=12 (4096 registers, ~1.6% standard error), matching the C++ CPC lgK.
//! Everything here is only reachable behind a config gate; the exact path is unchanged by default.

use serde::{Deserialize, Serialize};

use super::state::ShardState;

/// A bucket's exact distinct set is converted to a fixed-size HLL once it exceeds this many
/// distinct values, mirroring the C++ control_state DC→LDC upgrade threshold.
pub(crate) const DISTINCT_SKETCH_THRESHOLD: usize = 4096;

const P: u32 = 12;
const M: usize = 1 << P; // 4096 registers

/// Version-stable 64-bit hash: FNV-1a over the bytes, then a splitmix64 finalizer for avalanche.
fn hash64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325; // FNV offset basis
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3); // FNV prime
    }
    // splitmix64 finalizer
    let mut z = h.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Dense HyperLogLog. `registers[i]` holds the max observed rank (leftmost-1 position) for the
/// hashes that landed in register `i`.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub(crate) struct Hll {
    registers: Vec<u8>,
}

impl Default for Hll {
    fn default() -> Self {
        Self::new()
    }
}

impl Hll {
    pub(crate) fn new() -> Self {
        Self {
            registers: vec![0u8; M],
        }
    }

    /// Add one element (its raw bytes) to the sketch.
    pub(crate) fn add(&mut self, bytes: &[u8]) {
        let hash = hash64(bytes);
        let index = (hash >> (64 - P)) as usize; // top P bits pick the register
        // Rank = position of the leftmost 1-bit in the remaining (64-P) bits, +1. The guard bit
        // (1 << (P-1)) bounds the rank to <= 64-P+1 when the remainder is all zeros.
        let rank = (((hash << P) | (1u64 << (P - 1))).leading_zeros() as u8) + 1;
        if rank > self.registers[index] {
            self.registers[index] = rank;
        }
    }

    /// Merge another sketch into this one (register-wise max). Both must share P (they do).
    pub(crate) fn merge(&mut self, other: &Hll) {
        for (a, b) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *b > *a {
                *a = *b;
            }
        }
    }

    /// Estimated distinct cardinality.
    pub(crate) fn estimate(&self) -> u64 {
        let m = M as f64;
        // alpha_m bias constant for m >= 128.
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let sum: f64 = self
            .registers
            .iter()
            .map(|&r| 2f64.powi(-(r as i32)))
            .sum();
        let raw = alpha * m * m / sum;
        // Small-range correction via linear counting when many registers are still zero.
        if raw <= 2.5 * m {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count();
            if zeros != 0 {
                return (m * (m / zeros as f64).ln()).round() as u64;
            }
        }
        // 64-bit hashes make the large-range (2^32) correction unnecessary.
        raw.round() as u64
    }
}

/// Record a CHANGE value for `(key, bucket_ms)`. If the bucket is already sketched, the value
/// goes straight into its HLL. Otherwise it goes into the exact set, and — when the gate is on —
/// a set that grows past `DISTINCT_SKETCH_THRESHOLD` is folded into a new HLL and the exact set
/// is dropped, bounding memory while keeping small buckets exact. Read via `count_changes`.
pub(crate) fn record_change(shard: &mut ShardState, key: &str, bucket_ms: u64, value: Vec<u8>) {
    if let Some(sketch) = shard
        .control_state_change_sketch
        .get_mut(key)
        .and_then(|buckets| buckets.get_mut(&bucket_ms))
    {
        sketch.add(&value);
        return;
    }
    let enabled = shard.control_distinct_sketch;
    let need_convert = {
        let set = shard
            .control_state_changes
            .entry(key.to_string())
            .or_default()
            .entry(bucket_ms)
            .or_default();
        set.insert(value);
        enabled && set.len() > DISTINCT_SKETCH_THRESHOLD
    };
    if need_convert {
        if let Some(set) = shard
            .control_state_changes
            .get_mut(key)
            .and_then(|buckets| buckets.remove(&bucket_ms))
        {
            let mut sketch = Hll::new();
            for v in &set {
                sketch.add(v);
            }
            shard
                .control_state_change_sketch
                .entry(key.to_string())
                .or_default()
                .insert(bucket_ms, sketch);
        }
    }
}

/// Distinct count over `[start_ms, end_ms]`. Exact (BTreeSet union) when no bucket in the window
/// is sketched; otherwise everything (exact buckets folded in + sketched buckets merged) is
/// combined into one HLL and estimated.
pub(crate) fn count_changes(shard: &ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    let bounds = crate::engine::timestamp_range_bounds(start_ms, end_ms);
    let any_sketch = shard
        .control_state_change_sketch
        .get(key)
        .map(|buckets| buckets.range(bounds).next().is_some())
        .unwrap_or(false);
    if !any_sketch {
        let mut unique = std::collections::BTreeSet::new();
        if let Some(series) = shard.control_state_changes.get(key) {
            for (_, values) in series.range(bounds) {
                unique.extend(values.iter().cloned());
            }
        }
        return unique.len() as i64;
    }
    let mut merged = Hll::new();
    if let Some(series) = shard.control_state_changes.get(key) {
        for (_, values) in series.range(bounds) {
            for v in values {
                merged.add(v);
            }
        }
    }
    if let Some(sketches) = shard.control_state_change_sketch.get(key) {
        for (_, sketch) in sketches.range(bounds) {
            merged.merge(sketch);
        }
    }
    merged.estimate() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sketch_estimates_zero() {
        assert_eq!(Hll::new().estimate(), 0);
    }

    #[test]
    fn small_cardinality_is_near_exact() {
        let mut h = Hll::new();
        for i in 0..100u64 {
            h.add(format!("v{i}").as_bytes());
        }
        let est = h.estimate();
        // Linear counting makes small ranges very accurate.
        assert!((est as i64 - 100).abs() <= 5, "estimate {est} not near 100");
    }

    #[test]
    fn large_cardinality_within_tolerance() {
        let mut h = Hll::new();
        let n = 100_000u64;
        for i in 0..n {
            h.add(format!("item-{i}").as_bytes());
        }
        let est = h.estimate() as f64;
        let err = (est - n as f64).abs() / n as f64;
        // P=12 → ~1.6% standard error; allow a comfortable 5% bound for a single deterministic run.
        assert!(err < 0.05, "relative error {err:.4} exceeds 5% (est {est}, n {n})");
    }

    #[test]
    fn duplicates_do_not_inflate() {
        let mut h = Hll::new();
        for _ in 0..10_000 {
            h.add(b"same-value");
        }
        assert!(h.estimate() <= 3, "duplicate-only estimate should be ~1");
    }

    #[test]
    fn merge_equals_union() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0..60_000u64 {
            a.add(format!("k{i}").as_bytes());
        }
        for i in 40_000..100_000u64 {
            b.add(format!("k{i}").as_bytes()); // 20k overlap → union is 100k distinct
        }
        a.merge(&b);
        let est = a.estimate() as f64;
        let err = (est - 100_000.0).abs() / 100_000.0;
        assert!(err < 0.05, "merged union relative error {err:.4} exceeds 5% (est {est})");
    }
}
