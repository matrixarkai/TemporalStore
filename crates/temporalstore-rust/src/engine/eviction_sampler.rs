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
//! found within a bounded number of passes.
//!
//! # The window is scan-ordered, and that is why a pass does not stop on pool SIZE
//!
//! The scan walks ascending routing buckets from the cursor, so the buckets a pass meets first
//! are the lowest-numbered ones -- a position in the key space, unrelated to how cold a bucket
//! is. A pass that stopped as soon as it held `batch_limit` CANDIDATES would therefore hand the
//! ranking a window chosen by bucket id: when the head of that window is in use, every candidate
//! in it is warm, the ranking faithfully returns the coldest of them, and eviction takes a bucket
//! that is being read while colder buckets sit one scan step past where the pass stopped.
//! Measured on a 4,000-key shard whose 160 lowest-numbered buckets were read just before the
//! round: all 16 victims came out of that band.
//!
//! So the stop reads the pool for candidates that are actually COLD -- not used since the
//! previous pass -- and keeps walking while it holds fewer of those than the batch wants. The
//! ceiling is unchanged and is the one `scan_turns` already sets: a pass still walks at most
//! `samples * batch_limit * scan_turns` buckets and then ranks whatever it has, so a store that
//! is warm everywhere still yields a full batch instead of stalling. The extra walking happens
//! only in the regime that needs it, which is the same reason `scan_turns` exists -- it was
//! already the headroom for a window that is mostly INELIGIBLE, and a window that is mostly IN
//! USE is the same problem with a different predicate. Selection here is pure recency, which is also why
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
    /// Wall-clock ms of the previous pass, which is the line between "cold" and "in use".
    ///
    /// A bucket is cold to this sampler when nothing has touched it since the last time victims
    /// were picked. That is a RELATIVE test and it has to be: recency carries no absolute scale,
    /// so the only thing a pass can say about a candidate is whether it has been used since the
    /// last one looked.
    ///
    /// Zero means no pass has run yet. The floor then falls back to "never touched at all",
    /// which is the one reading that is cold without a previous pass to compare against; a first
    /// pass on a shard whose buckets have all been written therefore finds no cold candidate,
    /// walks its whole budget, and ranks what it saw -- the widest window the bound allows, which
    /// is the right answer for the one pass that has no history.
    ///
    /// Ephemeral with the rest of this state: a reload starts the floor at zero again and the
    /// first pass after it pays one full budget.
    pub(super) last_pass_ms: u64,
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
    /// Candidates held that were NOT used since the previous pass. This is the quantity the scan
    /// keeps walking for, so it is the one that says whether a pass ranked a cold window or
    /// merely the window its cursor happened to land on.
    pub(super) cold_candidates: usize,
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
    now_ms: u64,
) -> EvictionSampleOutcome {
    if source.bucket_count() == 0 {
        return EvictionSampleOutcome {
            pool_size_after: state.pool.len(),
            ..EvictionSampleOutcome::default()
        };
    }
    // A zero limit means NO limit, the same thing it means to the exhaustive path, which reads
    // it as "do not truncate" and returns every eligible bucket. This read it as "take none" and
    // returned immediately, so the one value a caller uses to say "everything" produced opposite
    // answers depending on which path was running -- and which path runs is a default.
    //
    // Unlimited here is the whole bucket count: scan them all, keep them all. That makes the two
    // paths agree at the boundary rather than agreeing only in the middle.
    let batch_limit = if batch_limit == 0 {
        source.bucket_count()
    } else {
        batch_limit
    };

    let samples = config.samples.max(1);
    let scan_turns = config.scan_turns.max(1);
    // Enough sampling to make a reasonable choice, and a hard ceiling so a pass cannot walk the
    // whole store when most buckets it meets are ineligible.
    let min_scan = samples.saturating_mul(batch_limit);
    let scan_budget = min_scan.saturating_mul(scan_turns).max(1);

    // THE LINE BETWEEN COLD AND IN USE, and why it is `<` rather than `<=`: a bucket stamped at
    // exactly the previous pass's millisecond was touched no later than that pass, so it is cold.
    // `1` with no previous pass makes the test "never touched at all" -- see `last_pass_ms`.
    let cold_floor = if state.last_pass_ms == 0 {
        1
    } else {
        state.last_pass_ms
    };
    // Seeded from what the pool already carries, because candidates survive passes: a pass whose
    // pool came in holding enough cold buckets must still stop at `min_scan` exactly as before.
    // Bounded by `pool_size`, so this is a fixed cost and not a walk of the store.
    let mut cold_in_pool = state
        .pool
        .values()
        .filter(|last_used| **last_used < cold_floor)
        .count();

    let mut seen = 0usize;
    let pool = &mut state.pool;
    let scan = source.scan(state.cursor, scan_budget, &mut |sample| {
        seen += 1;
        if sample.eligible {
            let previous = pool.insert(sample.routing_bucket, sample.last_used_ms);
            // Maintained incrementally rather than recounted per visit: a recount is `pool_size`
            // comparisons on every one of `scan_budget` steps, and the whole point of this module
            // is that a pass costs what it was budgeted.
            let is_cold = sample.last_used_ms < cold_floor;
            match previous {
                None => {
                    if is_cold {
                        cold_in_pool += 1;
                    }
                }
                Some(previous_last_used) => {
                    let was_cold = previous_last_used < cold_floor;
                    if was_cold && !is_cold {
                        cold_in_pool -= 1;
                    } else if !was_cold && is_cold {
                        cold_in_pool += 1;
                    }
                }
            }
        }
        // Stop early once there has been enough sampling AND there are enough COLD candidates to
        // fill the batch.
        //
        // THIS TESTS COLDNESS, NOT POOL SIZE, AND THE DIFFERENCE IS A BUCKET IN USE EVICTED. The
        // scan is ordered by routing bucket, so `pool.len() >= batch_limit` stopped the pass on a
        // window picked by key-space position: where the head of the store is in use, that window
        // is entirely in use, and the ranking below then returns the coldest of a set of buckets
        // that are all being read. Counting only candidates not touched since the previous pass
        // makes the stop condition the thing eviction actually needs, and it can only ever
        // LENGTHEN a pass -- `cold_in_pool <= pool.len()` -- never shorten one.
        //
        // IT CANNOT RUN AWAY. `scan_budget` is the same ceiling as before, so a store with no
        // cold bucket anywhere walks `samples * batch_limit * scan_turns` and then ranks what it
        // has; it does not stall, and it does not scan the store.
        //
        // The sparse-eligibility case the pool condition was written for still holds, because a
        // candidate has to be eligible to be pooled at all.
        !(seen >= min_scan && cold_in_pool >= batch_limit)
    });
    state.cursor = scan.next_cursor;
    state.last_pass_ms = now_ms;

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
        // Re-read from the pool AFTER re-validation and the trim, so it describes what the
        // selection actually ranked rather than what the scan happened to meet.
        cold_candidates: state
            .pool
            .values()
            .filter(|last_used| **last_used < cold_floor)
            .count(),
        wrapped: scan.wrapped,
    }
}

#[cfg(test)]
mod tests {
    /// A zero batch limit means "no limit" here, because it means that to the path this one
    /// replaced.
    ///
    /// The exhaustive path reads a zero as "do not truncate" and hands back every eligible
    /// bucket. This one used to read the same zero as "take none" and return before scanning.
    /// One value, two opposite answers, and which one a caller got depended on a default it
    /// never set -- which is exactly how a caller asking for everything got nothing.
    #[test]
    fn a_zero_batch_limit_takes_everything_rather_than_nothing() {
        use super::*;

        struct AllEligible(usize);
        impl BucketSource for AllEligible {
            fn bucket_count(&self) -> usize {
                self.0
            }
            fn scan(
                &self,
                _cursor: Option<u32>,
                budget: usize,
                visit: &mut dyn FnMut(&BucketSample) -> bool,
            ) -> ScanResult {
                let mut scanned = 0usize;
                for routing_bucket in 0..self.0 as u32 {
                    if scanned >= budget {
                        break;
                    }
                    scanned += 1;
                    let sample = BucketSample {
                        routing_bucket,
                        eligible: true,
                        last_used_ms: routing_bucket as u64,
                    };
                    if !visit(&sample) {
                        break;
                    }
                }
                ScanResult {
                    scanned,
                    next_cursor: None,
                    wrapped: true,
                }
            }
            fn lookup(&self, routing_bucket: u32) -> Option<BucketSample> {
                (routing_bucket < self.0 as u32).then(|| BucketSample {
                    routing_bucket,
                    eligible: true,
                    last_used_ms: routing_bucket as u64,
                })
            }
        }

        let config = EvictionSamplerConfig::default();
        let mut state = EvictionSamplerState::default();
        let unlimited = select_victims(&mut state, config, 0, AllEligible(6), 0);
        assert_eq!(
            unlimited.victims.len(),
            6,
            "a zero limit must take every eligible bucket, not none"
        );

        // And a real limit still limits, or the fix would just have removed the bound.
        let mut state = EvictionSamplerState::default();
        let limited = select_victims(&mut state, config, 2, AllEligible(6), 0);
        assert_eq!(limited.victims.len(), 2, "a stated limit must still be a limit");
    }

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

    /// A WINDOW THAT IS ALL IN USE DOES NOT STOP THE PASS, AND THE CEILING STILL HOLDS.
    ///
    /// Three arms on one fixture, because "it scanned further" is also what a pass with no bound
    /// looks like, and "it stopped at the minimum" is also what a correct steady-state pass looks
    /// like. The fixture puts the in-use buckets at the HEAD of the scan order -- the position the
    /// cursor starts at -- so a stop keyed on pool SIZE fires inside that band.
    ///
    ///   A  band in use at the head -> the pass walks PAST the band and ranks cold buckets
    ///   B  the same, cold pool     -> the pass stops at `min_scan`, so the cost did not move
    ///   C  every bucket in use     -> the pass walks its ceiling and STILL returns a full batch
    ///
    /// Arm B is what makes A a change in the regime that needs it rather than a change
    /// everywhere; arm C is what makes it a longer pass rather than a stalled one.
    #[test]
    fn a_window_that_is_all_in_use_does_not_stop_the_pass_at_its_pool_size() {
        let config = EvictionSamplerConfig {
            samples: 5,
            pool_size: 1_000,
            scan_turns: 4,
        };
        const BATCH: usize = 4;
        let min_scan = config.samples * BATCH;
        let ceiling = min_scan * config.scan_turns;
        // The band is WIDER than `min_scan` and leaves a full batch of cold buckets inside the
        // ceiling. That is the only width that tells the two stop conditions apart: narrower and
        // the pass meets a cold bucket before `min_scan`, wider and there is no cold bucket left
        // to prefer.
        let band = min_scan + 4;
        assert!(
            band > min_scan && band + BATCH < ceiling,
            "a {band}-bucket band does not sit between the {min_scan}-bucket minimum scan and the \
             {ceiling}-bucket ceiling, so this fixture tests one of those instead"
        );

        // Recency 10 is "used since the last pass" against a floor of 5; recency 1 is not. Both
        // are non-zero, so no arm can pass because of the never-touched fallback.
        let in_use_then_cold: Vec<BucketSample> = (0..200u32)
            .map(|routing_bucket| BucketSample {
                routing_bucket,
                eligible: true,
                last_used_ms: if (routing_bucket as usize) < band { 10 } else { 1 },
            })
            .collect();

        // --- ARM A: the band at the head, nothing cold in the pool. ---
        let mut state = EvictionSamplerState {
            last_pass_ms: 5,
            ..EvictionSamplerState::default()
        };
        let past_the_band = select_victims(&mut state, config, BATCH, in_use_then_cold.as_slice(), 6);
        assert_eq!(
            past_the_band.scanned,
            band + BATCH,
            "the pass scanned {} buckets. It has to walk past the {band}-bucket band and stop the \
             moment it holds {BATCH} cold candidates, which is {} -- stopping at {min_scan} is \
             the pool-size condition firing inside the band",
            past_the_band.scanned,
            band + BATCH
        );
        assert!(
            past_the_band.scanned > min_scan && past_the_band.scanned <= ceiling,
            "the pass scanned {} buckets, outside ({min_scan}, {ceiling}]",
            past_the_band.scanned
        );
        assert_eq!(
            past_the_band.victims.len(),
            BATCH,
            "the pass returned {} victims against a limit of {BATCH}",
            past_the_band.victims.len()
        );
        let taken_in_use: Vec<u32> = past_the_band
            .victims
            .iter()
            .copied()
            .filter(|routing_bucket| (*routing_bucket as usize) < band)
            .collect();
        assert!(
            taken_in_use.is_empty(),
            "{taken_in_use:?} of the victims are inside the {band}-bucket band that was used \
             since the last pass, although {} cold candidates were pooled",
            past_the_band.cold_candidates
        );
        assert!(
            past_the_band.cold_candidates >= BATCH,
            "only {} cold candidates were pooled, so the ranking above had nothing to choose \
             between",
            past_the_band.cold_candidates
        );

        // --- ARM B: the same window, but the pool already holds cold candidates. ---
        //
        // The cost did not move in the steady state, and this is the arm that says so.
        let mut state = EvictionSamplerState {
            last_pass_ms: 5,
            pool: (190..190 + BATCH as u32)
                .map(|routing_bucket| (routing_bucket, 1u64))
                .collect(),
            ..EvictionSamplerState::default()
        };
        let cold_pool = select_victims(&mut state, config, BATCH, in_use_then_cold.as_slice(), 6);
        assert_eq!(
            cold_pool.scanned, min_scan,
            "a pass whose pool already held {BATCH} cold candidates scanned {} buckets instead of \
             stopping at {min_scan}, so this costs every pass and not only the one that needs it",
            cold_pool.scanned
        );

        // --- ARM C: every bucket in use. A longer pass, not a stalled one. ---
        let all_in_use: Vec<BucketSample> = (0..200u32)
            .map(|routing_bucket| BucketSample {
                routing_bucket,
                eligible: true,
                last_used_ms: 10,
            })
            .collect();
        let mut state = EvictionSamplerState {
            last_pass_ms: 5,
            ..EvictionSamplerState::default()
        };
        let everywhere = select_victims(&mut state, config, BATCH, all_in_use.as_slice(), 6);
        assert_eq!(
            everywhere.scanned, ceiling,
            "a store with no cold bucket scanned {} rather than its {ceiling} ceiling",
            everywhere.scanned
        );
        assert_eq!(
            everywhere.victims.len(),
            BATCH,
            "a store with no cold bucket returned {} victims; eviction has to relieve memory on a \
             shard that is in use everywhere, so the coldest of those is the answer",
            everywhere.victims.len()
        );
        assert_eq!(
            everywhere.cold_candidates, 0,
            "the all-in-use arm pooled {} cold candidates, so it is not the regime it is named for",
            everywhere.cold_candidates
        );

        // --- ARM D: no previous pass, and buckets nothing has ever touched. ---
        //
        // The floor falls back to "never touched at all" when there is no previous pass to
        // compare against, and that fallback is load-bearing: without it a first pass has no
        // cold candidate by definition and walks its whole ceiling on every shard, including one
        // whose buckets are plainly untouched.
        let never_touched: Vec<BucketSample> = (0..200u32)
            .map(|routing_bucket| BucketSample {
                routing_bucket,
                eligible: true,
                last_used_ms: 0,
            })
            .collect();
        let mut state = EvictionSamplerState::default();
        assert_eq!(state.last_pass_ms, 0, "arm D needs a sampler with no previous pass");
        let first_ever = select_victims(&mut state, config, BATCH, never_touched.as_slice(), 9);
        assert_eq!(
            first_ever.scanned, min_scan,
            "a first pass over buckets nothing has touched scanned {} rather than stopping at \
             {min_scan}; with no previous pass the floor has to read a never-touched bucket as \
             cold, or every first pass pays the {ceiling}-bucket ceiling",
            first_ever.scanned
        );
        assert_eq!(
            first_ever.cold_candidates, first_ever.pool_size_after,
            "{} of {} candidates were counted cold on a store nothing has touched",
            first_ever.cold_candidates, first_ever.pool_size_after
        );
    }

    /// The pass records the clock it was given, so the NEXT pass has a floor to compare against.
    ///
    /// Without this the floor stays at the never-touched fallback for ever: every pass then pays
    /// a full ceiling on a store whose buckets have all been written, which is the behaviour arm
    /// A above wants and arm B above refuses.
    #[test]
    fn a_pass_stamps_its_clock_so_the_next_one_has_a_floor() {
        let mut state = EvictionSamplerState::default();
        assert_eq!(state.last_pass_ms, 0, "a fresh sampler has no previous pass");
        select_victims(
            &mut state,
            EvictionSamplerConfig::default(),
            2,
            buckets(20).as_slice(),
            4_242,
        );
        assert_eq!(
            state.last_pass_ms, 4_242,
            "the pass did not record its clock, so the next pass cannot tell a bucket used since \
             then from one nothing has touched in hours"
        );
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
        let small_outcome = select_victims(&mut state, config, 4, small.as_slice(), 0);
        let mut state = EvictionSamplerState::default();
        let large_outcome = select_victims(&mut state, config, 4, large.as_slice(), 0);

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
        let outcome = select_victims(&mut state, EvictionSamplerConfig::default(), 3, buckets(50).as_slice(), 0);
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

        let first = select_victims(&mut state, config, 2, all.as_slice(), 0);
        let after_first = state.cursor;
        assert!(after_first.is_some(), "cursor must advance, not reset");
        assert!(after_first.unwrap() > 0);

        let second = select_victims(&mut state, config, 2, all.as_slice(), 0);
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
        let outcome = select_victims(&mut state, config, 2, buckets(4).as_slice(), 0);
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

        let first = select_victims(&mut state, config, 2, all.as_slice(), 0);
        let second = select_victims(&mut state, config, 2, all.as_slice(), 0);
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
        let first = select_victims(&mut state, config, 3, all.as_slice(), 0);
        assert_eq!(first.victims, vec![0, 1, 2]);

        // Bucket 0 is emptied and bucket 1 becomes the most recently used.
        all[0].eligible = false;
        all[1].last_used_ms = u64::MAX;

        let second = select_victims(&mut state, config, 3, all.as_slice(), 0);
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
            select_victims(&mut state, config, 2, all.as_slice(), 0);
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
        let outcome = select_victims(&mut state, EvictionSamplerConfig::default(), 3, all.as_slice(), 0);
        assert_eq!(outcome.victims, vec![5, 6, 7]);
    }

    /// An empty store yields no victims, whatever the limit.
    ///
    /// This used to assert a second thing: that a ZERO limit also yields nothing. It no longer
    /// does, and the change is deliberate. The exhaustive path this sampler replaced reads a
    /// zero as "do not truncate" and returns every eligible bucket, and so does every other
    /// bound in this engine -- the dump round, the index-GC round, the dump interval, the
    /// sweep threshold. The sampler was the one place where zero meant the opposite, and the
    /// caller that asked for everything got nothing.
    ///
    /// `a_zero_batch_limit_takes_everything_rather_than_nothing` now owns that half.
    #[test]
    fn an_empty_store_selects_nothing() {
        let mut state = EvictionSamplerState::default();
        let empty: Vec<BucketSample> = Vec::new();
        assert!(select_victims(&mut state, EvictionSamplerConfig::default(), 3, empty.as_slice(), 0)
            .victims
            .is_empty());
        // Zero limit, still nothing -- because there is nothing, not because the limit is zero.
        assert!(select_victims(&mut state, EvictionSamplerConfig::default(), 0, empty.as_slice(), 0)
            .victims
            .is_empty());
    }
}
