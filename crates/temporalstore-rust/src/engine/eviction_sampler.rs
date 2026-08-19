// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bounded victim selection for eviction.
//!
//! # Why this exists
//!
//! The straightforward way to pick eviction victims is to enumerate every bucket, sort by
//! recency and take the coldest `batch_limit`. That is what [`super::TemporalEngine`] does when
//! this sampler is off, and its cost grows with the size of the store rather than with the
//! number of victims wanted: building the candidate list walks every live page in the shard
//! (cloning two strings per page), and the sort is `O(buckets log buckets)`. Eviction runs on a
//! background loop, so a large shard pays that repeatedly to choose a handful of victims.
//!
//! This module bounds the work instead. Each pass scans at most
//! `samples * batch_limit * scan_turns` buckets starting from where the previous pass stopped,
//! and keeps what it saw in a candidate pool that survives across passes. Cold buckets
//! therefore accumulate in the pool over successive passes rather than being rediscovered from
//! scratch each time, and a pass costs the same whether the shard holds a thousand buckets or a
//! million.
//!
//! # What it gives up
//!
//! Sampling sees part of the store, so the victims are the coldest buckets *found*, not
//! provably the coldest that exist. That is the intended trade: eviction only needs victims
//! that are cold enough, and the pool plus the resuming cursor mean a genuinely cold bucket is
//! found within a bounded number of passes. Selection here is pure recency, which is also why
//! it does not need the per-bucket byte weights the full scan computes -- those are needed only
//! for the buckets actually chosen.
//!
//! # Staleness
//!
//! Pool entries can be several passes old, so a bucket in the pool may have been touched,
//! emptied, or dropped since it was seen. Every entry is therefore re-validated against current
//! state at selection time and its recency re-read; a stale entry is discarded rather than
//! evicted. Skipping that step would evict buckets that had since become hot.

use std::collections::HashMap;

/// Where the sampler reads bucket state from.
///
/// This is a trait rather than a slice so a caller backed by an ordered map can walk only the
/// scan window. Materializing every bucket to pick a handful of victims would reintroduce the
/// cost this module exists to remove.
pub(super) trait BucketSource {
    /// Total buckets available, used to tell "covered the store" from "ran out of budget".
    fn bucket_count(&self) -> usize;

    /// Visit buckets in ascending order from `cursor`, wrapping, until `budget` are seen or
    /// `visit` returns false.
    fn scan(
        &self,
        cursor: Option<u32>,
        budget: usize,
        visit: &mut dyn FnMut(&BucketSample) -> bool,
    ) -> ScanResult;

    /// Current state of one bucket, for re-validating a pool entry.
    fn lookup(&self, routing_bucket: u32) -> Option<BucketSample>;
}

/// Where a scan stopped.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct ScanResult {
    pub(super) scanned: usize,
    /// Bucket to resume at, or `None` if the whole store was covered.
    pub(super) next_cursor: Option<u32>,
    pub(super) wrapped: bool,
}

/// Slice-backed source, used by tests and by callers holding an ordered vector.
impl BucketSource for &[BucketSample] {
    fn bucket_count(&self) -> usize {
        self.len()
    }

    fn scan(
        &self,
        cursor: Option<u32>,
        budget: usize,
        visit: &mut dyn FnMut(&BucketSample) -> bool,
    ) -> ScanResult {
        if self.is_empty() || budget == 0 {
            return ScanResult::default();
        }
        let start = match cursor {
            Some(cursor) => self
                .iter()
                .position(|sample| sample.routing_bucket >= cursor)
                .unwrap_or(0),
            None => 0,
        };
        let mut scanned = 0usize;
        let mut wrapped = false;
        let mut index = start;
        loop {
            if !visit(&self[index]) {
                scanned += 1;
                index = (index + 1) % self.len();
                break;
            }
            scanned += 1;
            index += 1;
            if index >= self.len() {
                index = 0;
                wrapped = true;
            }
            if scanned >= budget || scanned >= self.len() {
                if scanned >= self.len() {
                    wrapped = true;
                }
                break;
            }
        }
        ScanResult {
            scanned,
            next_cursor: if wrapped && scanned >= self.len() {
                None
            } else {
                Some(self[index].routing_bucket)
            },
            wrapped,
        }
    }

    fn lookup(&self, routing_bucket: u32) -> Option<BucketSample> {
        self.iter()
            .find(|sample| sample.routing_bucket == routing_bucket)
            .copied()
    }
}

/// Tuning for a sampled pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EvictionSamplerConfig {
    /// Buckets to look at per wanted victim. Higher means better victims and more work.
    pub(crate) samples: usize,
    /// Upper bound on candidates carried between passes.
    pub(crate) pool_size: usize,
    /// Multiplier on the per-pass scan budget, bounding how far a pass may walk when most of
    /// what it sees is ineligible.
    pub(crate) scan_turns: usize,
}

impl Default for EvictionSamplerConfig {
    fn default() -> Self {
        Self {
            samples: 5,
            pool_size: 64,
            scan_turns: 4,
        }
    }
}

/// State carried between passes: where to resume, and the candidates seen so far.
#[derive(Debug, Default, Clone)]
pub(super) struct EvictionSamplerState {
    /// Bucket to resume the next scan at. `None` restarts from the beginning.
    pub(super) cursor: Option<u32>,
    /// Candidate buckets and the recency they carried when last observed.
    pub(super) pool: HashMap<u32, u64>,
}

/// What the sampler needs to know about a bucket. Deliberately not the bucket itself, so the
/// selection logic stays testable without building engine state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BucketSample {
    pub(super) routing_bucket: u32,
    /// Holds live objects in memory, so evicting it would actually free something.
    pub(super) eligible: bool,
    /// Wall-clock ms of the last access. Zero means never touched, which is coldest.
    pub(super) last_used_ms: u64,
}

/// Outcome of one pass, including the counters that make the bound observable.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct EvictionSampleOutcome {
    /// Chosen victims, coldest first.
    pub(super) victims: Vec<u32>,
    /// Buckets looked at this pass. This is the number the bound applies to.
    pub(super) scanned: usize,
    /// Candidates held after this pass.
    pub(super) pool_size_after: usize,
    /// Pool entries dropped at selection because they no longer qualified.
    pub(super) stale_dropped: usize,
    /// The scan reached the end of the bucket space and restarted.
    pub(super) wrapped: bool,
}

/// Select up to `batch_limit` victims, scanning a bounded slice of `ordered_buckets`.
///
/// `ordered_buckets` must be in ascending routing-bucket order, which is how the bucket index
/// stores them; the cursor is a position in that order.
pub(super) fn select_victims<S: BucketSource>(
    state: &mut EvictionSamplerState,
    config: EvictionSamplerConfig,
    batch_limit: usize,
    source: S,
) -> EvictionSampleOutcome {
    if batch_limit == 0 || source.bucket_count() == 0 {
        return EvictionSampleOutcome {
            pool_size_after: state.pool.len(),
            ..EvictionSampleOutcome::default()
        };
    }

    let samples = config.samples.max(1);
    let scan_turns = config.scan_turns.max(1);
    // Enough sampling to make a reasonable choice, and a hard ceiling so a pass cannot walk the
    // whole store when most buckets it meets are ineligible.
    let min_scan = samples.saturating_mul(batch_limit);
    let scan_budget = min_scan.saturating_mul(scan_turns).max(1);

    let mut seen = 0usize;
    let pool = &mut state.pool;
    let scan = source.scan(state.cursor, scan_budget, &mut |sample| {
        seen += 1;
        if sample.eligible {
            pool.insert(sample.routing_bucket, sample.last_used_ms);
        }
        // Stop early once there has been enough sampling AND there are enough candidates to
        // fill the batch. Without the pool condition a pass could return nothing on a store
        // where eligible buckets are sparse.
        !(seen >= min_scan && pool.len() >= batch_limit)
    });
    state.cursor = scan.next_cursor;

    // Re-validate: a pool entry may be several passes old. Re-read current recency and drop
    // anything that stopped qualifying, so a bucket that turned hot is not evicted on the
    // strength of a stale observation.
    let before = state.pool.len();
    let stale = state
        .pool
        .keys()
        .copied()
        .filter_map(|routing_bucket| match source.lookup(routing_bucket) {
            Some(sample) if sample.eligible => None,
            _ => Some(routing_bucket),
        })
        .collect::<Vec<_>>();
    for routing_bucket in stale {
        state.pool.remove(&routing_bucket);
    }
    let refreshed = state
        .pool
        .keys()
        .copied()
        .filter_map(|routing_bucket| {
            source
                .lookup(routing_bucket)
                .map(|sample| (routing_bucket, sample.last_used_ms))
        })
        .collect::<Vec<_>>();
    for (routing_bucket, last_used) in refreshed {
        state.pool.insert(routing_bucket, last_used);
    }
    let stale_dropped = before - state.pool.len();

    // Coldest first; ties broken on bucket id so the choice is deterministic.
    let mut ranked = state
        .pool
        .iter()
        .map(|(routing_bucket, last_used)| (*last_used, *routing_bucket))
        .collect::<Vec<_>>();
    ranked.sort_unstable();
    let victims = ranked
        .iter()
        .take(batch_limit)
        .map(|(_, routing_bucket)| *routing_bucket)
        .collect::<Vec<_>>();

    // Keep the pool bounded, discarding the warmest entries -- they are the least likely to be
    // wanted, and dropping them leaves room for colder ones found later.
    if state.pool.len() > config.pool_size {
        let excess = state.pool.len() - config.pool_size;
        for (_, routing_bucket) in ranked.iter().rev().take(excess) {
            state.pool.remove(routing_bucket);
        }
    }

    EvictionSampleOutcome {
        victims,
        scanned: scan.scanned,
        pool_size_after: state.pool.len(),
        stale_dropped,
        wrapped: scan.wrapped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buckets(count: u32) -> Vec<BucketSample> {
        (0..count)
            .map(|routing_bucket| BucketSample {
                routing_bucket,
                eligible: true,
                // Higher bucket id = more recently used, so bucket 0 is always coldest.
                last_used_ms: u64::from(routing_bucket) + 1,
            })
            .collect()
    }

    #[test]
    fn a_pass_scans_a_bounded_slice_not_the_whole_store() {
        // The point of the sampler: cost tracks batch_limit, not store size.
        let config = EvictionSamplerConfig {
            samples: 5,
            pool_size: 64,
            scan_turns: 4,
        };
        let small = buckets(100);
        let large = buckets(100_000);

        let mut state = EvictionSamplerState::default();
        let small_outcome = select_victims(&mut state, config, 4, small.as_slice());
        let mut state = EvictionSamplerState::default();
        let large_outcome = select_victims(&mut state, config, 4, large.as_slice());

        assert_eq!(
            small_outcome.scanned, large_outcome.scanned,
            "a 1000x larger store must not cost more to sample"
        );
        assert!(
            large_outcome.scanned <= config.samples * 4 * config.scan_turns,
            "scan must stay inside the budget"
        );
        assert_eq!(large_outcome.victims.len(), 4);
    }

    #[test]
    fn victims_are_the_coldest_candidates_seen_coldest_first() {
        let mut state = EvictionSamplerState::default();
        let outcome = select_victims(&mut state, EvictionSamplerConfig::default(), 3, buckets(50).as_slice());
        assert_eq!(outcome.victims, vec![0, 1, 2]);
    }

    #[test]
    fn the_cursor_resumes_so_successive_passes_cover_new_ground() {
        // A cursor that did not advance would resample the same head of the store forever, and
        // buckets past the first scan window would never be considered.
        let config = EvictionSamplerConfig {
            samples: 2,
            pool_size: 8,
            scan_turns: 1,
        };
        let all = buckets(100);
        let mut state = EvictionSamplerState::default();

        let first = select_victims(&mut state, config, 2, all.as_slice());
        let after_first = state.cursor;
        assert!(after_first.is_some(), "cursor must advance, not reset");
        assert!(after_first.unwrap() > 0);

        let second = select_victims(&mut state, config, 2, all.as_slice());
        assert_ne!(
            state.cursor, after_first,
            "the second pass must start where the first stopped"
        );
        assert_eq!(first.scanned, second.scanned);
    }

    #[test]
    fn the_cursor_wraps_and_restarts_once_the_store_is_covered() {
        let config = EvictionSamplerConfig {
            samples: 5,
            pool_size: 64,
            scan_turns: 4,
        };
        // 4 buckets, budget 5*2*4 = 40, so one pass covers everything and wraps.
        let mut state = EvictionSamplerState::default();
        let outcome = select_victims(&mut state, config, 2, buckets(4).as_slice());
        assert!(outcome.wrapped);
        assert_eq!(state.cursor, None, "a covered store restarts from the top");
    }

    #[test]
    fn candidates_survive_across_passes() {
        // The pool is what lets a small scan window still accumulate cold buckets.
        let config = EvictionSamplerConfig {
            samples: 1,
            pool_size: 64,
            scan_turns: 1,
        };
        let all = buckets(200);
        let mut state = EvictionSamplerState::default();

        let first = select_victims(&mut state, config, 2, all.as_slice());
        let second = select_victims(&mut state, config, 2, all.as_slice());
        assert!(
            second.pool_size_after > first.pool_size_after,
            "the second pass must build on the first, not start over"
        );
    }

    #[test]
    fn a_pool_entry_that_stopped_qualifying_is_dropped_not_evicted() {
        // Pool entries can be several passes old. Evicting on a stale observation would throw
        // out a bucket that had since been touched or emptied.
        let config = EvictionSamplerConfig::default();
        let mut all = buckets(20);
        let mut state = EvictionSamplerState::default();
        let first = select_victims(&mut state, config, 3, all.as_slice());
        assert_eq!(first.victims, vec![0, 1, 2]);

        // Bucket 0 is emptied and bucket 1 becomes the most recently used.
        all[0].eligible = false;
        all[1].last_used_ms = u64::MAX;

        let second = select_victims(&mut state, config, 3, all.as_slice());
        assert!(second.stale_dropped >= 1, "the emptied bucket must be dropped");
        assert!(
            !second.victims.contains(&0),
            "an ineligible bucket must not be evicted"
        );
        assert!(
            !second.victims.contains(&1),
            "a bucket that turned hot must not be evicted on a stale reading"
        );
    }

    #[test]
    fn the_pool_stays_bounded_and_keeps_the_coldest() {
        let config = EvictionSamplerConfig {
            samples: 50,
            pool_size: 10,
            scan_turns: 10,
        };
        let all = buckets(500);
        let mut state = EvictionSamplerState::default();
        for _ in 0..5 {
            select_victims(&mut state, config, 2, all.as_slice());
        }
        assert!(
            state.pool.len() <= config.pool_size,
            "pool grew past its bound: {}",
            state.pool.len()
        );
        assert!(
            state.pool.contains_key(&0),
            "trimming must discard the warmest, keeping the coldest candidate"
        );
    }

    #[test]
    fn ineligible_buckets_are_never_selected() {
        let mut all = buckets(30);
        for sample in all.iter_mut().take(5) {
            sample.eligible = false;
        }
        let mut state = EvictionSamplerState::default();
        let outcome = select_victims(&mut state, EvictionSamplerConfig::default(), 3, all.as_slice());
        assert_eq!(outcome.victims, vec![5, 6, 7]);
    }

    #[test]
    fn an_empty_store_or_zero_batch_selects_nothing() {
        let mut state = EvictionSamplerState::default();
        let empty: Vec<BucketSample> = Vec::new();
        assert!(select_victims(&mut state, EvictionSamplerConfig::default(), 3, empty.as_slice())
            .victims
            .is_empty());
        assert!(
            select_victims(&mut state, EvictionSamplerConfig::default(), 0, buckets(10).as_slice())
                .victims
                .is_empty()
        );
    }
}
