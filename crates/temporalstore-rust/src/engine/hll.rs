// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bounded approximate distinct count via a dense HyperLogLog sketch.
//!
//! Control State distinct/CHANGE counts are otherwise exact `BTreeSet` unions, whose memory
//! grows with cardinality — unbounded for high-cardinality long-window distincts. This sketch
//! gives a fixed-memory (2^P registers) approximate count, mergeable register-wise, so it can
//! back a bounded distinct mode (exact until a threshold, then sketch — the DC→LDC shape).
//!
//! Fixed, version-stable hashing (FNV-1a + splitmix64 finalizer) so a persisted sketch stays
//! valid across builds. P=12 (4096 registers, ~1.6% standard error), matching the CPC lgK.
//! Everything here is only reachable behind a config gate; the exact path is unchanged by default.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::state::ShardState;

/// ONE DISTINCT-COUNT BUCKET, IN WHICHEVER REPRESENTATION IT CURRENTLY HOLDS.
///
/// # WHY THIS IS ONE TYPE AND NOT TWO MAPS
///
/// This was `control_state_changes: HashMap<String, BTreeMap<u64, BTreeSet<Vec<u8>>>>` beside
/// `control_state_change_sketch: HashMap<String, BTreeMap<u64, Hll>>` -- two maps keyed
/// identically, holding the two representations of the same thing. [`record_change`] kept them
/// MUTUALLY EXCLUSIVE per `(key, bucket)` by removing the bucket from the first as it inserted
/// into the second, so they were already one logical map; the exclusivity was maintained by a
/// remove/insert pair rather than by the type, and a bucket in both was representable.
///
/// It is not representable now. That is the whole of what this change buys, and it is worth being
/// precise about what it does NOT buy.
///
/// # IT IS NOT A BUG FIX, AND NOBODY SHOULD LATER DECIDE THAT IT WAS
///
/// A bucket sitting in BOTH maps would still have produced the right answer. [`count_changes`]
/// folds the exact values into an HLL and merges the sketches into the same HLL, and both an
/// `Hll` and a `BTreeSet` absorb a duplicate -- so the overlap state was wasteful and
/// unrepresentable-in-principle, not wrong. The merge removes a map and makes an invariant
/// unbreakable. It repairs nothing, and a future reader should not invent a defect to explain it.
///
/// # IT ALSO COSTS FEWER LOOKUPS, WHICH IS A CONSEQUENCE AND NOT THE REASON
///
/// The old `record_change` hashed the key up to three times on the conversion path -- a `get_mut`
/// on the sketch map, an `entry` on the exact map, a `get_mut` back on the exact map, an `entry`
/// on the sketch map. One `entry` reaches the bucket now and the conversion is a variant swap in
/// place.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub(crate) enum DistinctBucket {
    /// Every distinct value, while the bucket is small enough to keep them.
    Exact(BTreeSet<Vec<u8>>),
    /// A fixed-size sketch, once the exact set passed [`DISTINCT_SKETCH_THRESHOLD`] with the
    /// `control_distinct_sketch` gate on. The exact values are gone: this is the conversion, not
    /// a cache beside them.
    Sketch(Hll),
}

impl Default for DistinctBucket {
    fn default() -> Self {
        DistinctBucket::Exact(BTreeSet::new())
    }
}

impl DistinctBucket {
    /// How many distinct values this bucket stands for: exact when it is exact, estimated when it
    /// is sketched. For reporting, never for a decision -- a caller that needs the values
    /// themselves has to match on the variant, which is the point of the variant.
    pub(crate) fn distinct_hint(&self) -> u64 {
        match self {
            DistinctBucket::Exact(set) => set.len() as u64,
            DistinctBucket::Sketch(sketch) => sketch.estimate(),
        }
    }
}

/// A bucket's exact distinct set is converted to a fixed-size HLL once it exceeds this many
/// distinct values, mirroring the control_state DC→LDC upgrade threshold.
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
    let enabled = shard.control_distinct_sketch;
    // MINTS HERE AND IS ADOPTED AFTER, which is the order every other holder uses. The page
    // this write belongs to is not filed yet at this point, so there is no handle to borrow --
    // `adopt_object_key_allocations` runs once the bucket index holds one and re-keys this map
    // onto it. Asking for the handle here is what went 0 for 40,040 the first time a sharing
    // mechanism was wired before its source existed.
    let bucket = shard
        .control_state_changes
        .entry(super::state::ModelKey::from(key))
        .or_default()
        .entry(bucket_ms)
        .or_default();
    match bucket {
        // Already sketched: the value joins the sketch and the exact members are long gone.
        DistinctBucket::Sketch(sketch) => sketch.add(&value),
        DistinctBucket::Exact(set) => {
            set.insert(value);
            if enabled && set.len() > DISTINCT_SKETCH_THRESHOLD {
                // THE CONVERSION IS A VARIANT SWAP IN PLACE. It used to be a `remove` from one map
                // and an `insert` into another, which is what made a bucket in both representable
                // at all. There is no window here and no second map to be inconsistent with.
                let mut sketch = Hll::new();
                for v in set.iter() {
                    sketch.add(v);
                }
                *bucket = DistinctBucket::Sketch(sketch);
            }
        }
    }
}

/// Distinct count over `[start_ms, end_ms]`. Exact (BTreeSet union) when no bucket in the window
/// is sketched; otherwise everything (exact buckets folded in + sketched buckets merged) is
/// combined into one HLL and estimated.
pub(crate) fn count_changes(shard: &ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    let bounds = crate::engine::timestamp_range_bounds(start_ms, end_ms);
    let Some(series) = shard.control_state_changes.get(key) else {
        return 0;
    };
    // EXACT WHENEVER THE WHOLE WINDOW IS EXACT, which is the behaviour this had when the two
    // representations lived in two maps: one sketched bucket anywhere in the range makes the whole
    // answer an estimate, because an HLL cannot be unmerged back into members.
    let any_sketch = series
        .range(bounds)
        .any(|(_, bucket)| matches!(bucket, DistinctBucket::Sketch(_)));
    if !any_sketch {
        let mut unique = BTreeSet::new();
        for (_, bucket) in series.range(bounds) {
            if let DistinctBucket::Exact(values) = bucket {
                unique.extend(values.iter().cloned());
            }
        }
        return unique.len() as i64;
    }
    let mut merged = Hll::new();
    for (_, bucket) in series.range(bounds) {
        match bucket {
            DistinctBucket::Exact(values) => {
                for v in values {
                    merged.add(v);
                }
            }
            DistinctBucket::Sketch(sketch) => merged.merge(sketch),
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
