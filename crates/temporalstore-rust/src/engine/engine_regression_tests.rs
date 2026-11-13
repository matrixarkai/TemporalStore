// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::block_store::BlockStoreSlabState;
use crate::engine::golden::{
    native_api_golden_corpus_report, native_feature_sequence_golden_corpus_report,
};
use crate::types::{
    ContextAuditRef, ContextChildRef, ContextCompressionEvent,
    ContextExtractedEventIndexes, ContextSummary, ContextWire, FeatureFilter, FeatureFilterOp,
    ReplicatedCommand,
};
use crate::{ElementEntry, BlockStoreOptions, BlockStore};

/// Holds the one-entry-a-block gate OFF for as long as it lives, putting back whatever was there --
/// on a normal drop AND during unwinding.
///
/// # WHY A SHARED GUARD APPEARED WHEN THE GATE'S DEFAULT MOVED
///
/// Eleven tests in six modules went red when that default became ON, and not one of them mentions
/// the gate. They were driving the per-element projection THROUGH THE DEFAULT -- so the default was
/// load-bearing on coverage nobody had declared, and moving it silently changed what they measured.
/// They fail loudly, which is the good version of this problem; the twenty `remove_var` sites are
/// the silent version of the same thing, and six of those were arms that meant "off" and stopped
/// meaning it.
///
/// Each user of this guard states the path it drives and names where the COLLAPSED behaviour it is
/// not asserting is asserted instead, so holding the gate off here cannot quietly drop coverage of
/// what a deployment now runs.
///
/// A guard rather than a `set_var` and a matching `remove_var`: `Drop` runs while unwinding, so a
/// FAILING test cannot leak the gate into every test after it in the process. That contamination
/// has already happened once in this series, and it is ordered and silent rather than a flake.



/// THE OBJECT OF EVERY BLOCK IN ONE INDEX, KEYED BY THE POSITION THE BLOCK IS FILED AT.
///
/// # WHY THIS AND NOT ONLY [`blocks_with_identity`]
///
/// Half the walks that have to be converted cannot give up their own nesting. They need the
/// BUCKET as well as the block -- to read a tombstone row out of it, to count its object index, to
/// `retain` over it -- and a flat list of blocks does not carry the bucket. This composes with
/// whatever walk is already there: take it once, then ask it per block.
///
/// IT OWNS ITS KEYS AND BORROWS NOTHING, which is what lets a `retain` use it. `retain` holds the
/// bucket map mutably and the keys are drawn from a SIBLING field of the same index, so a map that
/// borrowed would be refused; the `Arc` clones cost one refcount bump each and the borrow ends
/// when this returns.
pub(super) struct BlockObjects {
    by_position: std::collections::HashMap<(u32, u64), std::sync::Arc<str>>,
}

impl BlockObjects {
    /// Taken in ONE pass over the index, from the same two sources [`blocks_with_identity`] reads.
    pub(super) fn of(index: &crate::engine::state::CoreIndex) -> Self {
        Self {
            by_position: blocks_with_identity(index)
                .into_iter()
                .map(|(routing_bucket, handle, object_key, _block)| {
                    ((routing_bucket, handle), object_key)
                })
                .collect(),
        }
    }

    /// The object the block at this position belongs to.
    ///
    /// A PANIC AND NOT AN `Option`, for [`blocks_with_identity`]'s reason: a block that nothing
    /// beside it names is a state the producer refuses, and a caller handed `None` would reach
    /// for a skip. Every caller here is walking the same index this was taken from, so the only
    /// way to miss is to ask at a position that does not exist.
    pub(super) fn at(&self, routing_bucket: u32, handle: u64) -> &std::sync::Arc<str> {
        self.by_position.get(&(routing_bucket, handle)).unwrap_or_else(|| {
            panic!(
                "no block is filed at bucket {routing_bucket} handle {handle} in the index this                  was taken from"
            )
        })
    }
}

/// EVERY BLOCK OF ONE CORE INDEX, WITH THE OBJECT THAT THE STRUCTURES BESIDE IT NAME.
///
/// # WHY THIS IS SHARED AND NOT A WALK PER TEST
///
/// `BlockIndex` carries no object key. Every test that walked a real store's buckets and asked a
/// block which object it belonged to now has to ask the bucket instead, and there are some forty
/// such walks across this module tree. Written out one by one they are forty chances to pick the
/// WRONG DOOR, and the wrong door fails silently rather than loudly:
///
///   * [`BlockKeysByPosition::live_identity`] answers from the object lookup alone, and
///     `insert_object_block_lookup` returns early on a deleted block -- so it names NO tombstone.
///     A walk that reports on deleted blocks and asks that door drops every one of them and comes
///     back with a smaller, entirely plausible number.
///   * [`BlockKeysByPosition::identity_of`] knows about both sources: the lookup for a live block,
///     and the bucket's own tombstone row for a deleted one. That is what this asks, so a caller
///     cannot get the tombstone case wrong by omitting to think about it.
///
/// # A LIVE BLOCK NAMED BY NOTHING IS A PANIC; AN UNNAMED TOMBSTONE IS A SKIP
///
/// THIS WAS A FLAT PANIC OVER BOTH STATES AND THAT WAS WRONG, AND THE SUITE IS WHAT SAID SO. The
/// paragraph here read "A BLOCK NAMED BY NOTHING IS A PANIC, NOT A SKIP ... Both are states a test
/// walking a store should fail on, loudly", naming the window between a decode and its rebuild and
/// a removal that named no element.
///
/// THE SECOND IS NOT A DEFECT, AND `identity_of`'s OWN DOC SAYS SO: a deleted block whose removal
/// named no element "is the one block in this store whose object nothing beside it states".
/// Production's `block_identities_in` skips it through a `filter_map`, with the note that in every
/// caller it means "this block is not the one I am matching".
///
/// `hash_read_union_divergence::an_index_entry_marked_deleted_loses_no_field_and_is_not_counted`
/// plants exactly that block -- it marks an entry deleted in place, with no tombstone row -- and
/// the flat panic fired while this walk was still BUILDING its list, before any caller's
/// `!block.deleted` filter could short-circuit. A test whose whole subject is that block could not
/// run.
///
/// SO THE TWO STATES ARE SPLIT, and the assertion says which is which:
///
///   * a LIVE block the lookup does not name PANICS with the handle. That is the decode-before-
///     rebuild window, which the decoder closed by filing as it decodes, so it is a defect.
///   * a DELETED block no tombstone row names is SKIPPED. Its object is genuinely unstated and
///     there is nothing for a walk to attribute it to.
///
/// A skip is still the dangerous direction, which is why only the second case gets one: a skip on
/// the first would be a count quietly short, the denominator mistake this campaign keeps
/// re-finding in a new costume.
///
/// # WHAT THIS IS NOT FOR
///
/// A test whose SUBJECT is a block filed under a bucket its key does not route to. This asks by
/// POSITION, and the lookup names such a block at the position it was filed under, not the one it
/// now sits in -- so this would panic on the very state that test is driving.
/// [`BlockKeysByPosition::identity_anywhere`] is the door for that, and
/// `bucket_fill`'s mis-filing walk uses it directly and says so.
pub(super) fn blocks_with_identity(
    index: &crate::engine::state::CoreIndex,
) -> Vec<(u32, u64, std::sync::Arc<str>, &crate::engine::state::BlockIndex)> {
    let keys = crate::engine::state::BlockKeysByPosition::from_parts(&index.objects, &index.object_block_lookup);
    let mut out = Vec::new();
    for bucket in index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            let Some(beside) = keys.identity_of(bucket, *handle, block) else {
                assert!(
                    block.deleted(),
                    "the LIVE block at handle {handle} in bucket {} is named by nothing beside \n                     it, and a live block is named by the object lookup unless the index is in \n                     the one window the decoder closed by filing as it decodes",
                    bucket.routing_bucket
                );
                continue;
            };
            out.push((
                bucket.routing_bucket,
                *handle,
                std::sync::Arc::clone(beside.object_key),
                block,
            ));
        }
    }
    out
}

fn wait_for_fresh_admission_second() {
    loop {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch");
        if elapsed.subsec_millis() < 100 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn assert_cache_latency_histograms_observed(stats: matrixcache::CacheStats) {
    assert!(stats.read_through_latency_samples > 0);
    assert!(stats.refill_latency_samples > 0);
    assert!(stats.writeback_latency_samples > 0);
    assert!(stats.eviction_latency_samples > 0);
    assert!(stats.compaction_latency_samples > 0);
    assert_eq!(
        stats.read_through_latency_samples,
        stats.read_through_latency_le_10us
            + stats.read_through_latency_le_100us
            + stats.read_through_latency_le_1ms
            + stats.read_through_latency_le_10ms
            + stats.read_through_latency_gt_10ms
    );
    assert_eq!(
        stats.refill_latency_samples,
        stats.refill_latency_le_10us
            + stats.refill_latency_le_100us
            + stats.refill_latency_le_1ms
            + stats.refill_latency_le_10ms
            + stats.refill_latency_gt_10ms
    );
    assert_eq!(
        stats.writeback_latency_samples,
        stats.writeback_latency_le_10us
            + stats.writeback_latency_le_100us
            + stats.writeback_latency_le_1ms
            + stats.writeback_latency_le_10ms
            + stats.writeback_latency_gt_10ms
    );
    assert_eq!(
        stats.eviction_latency_samples,
        stats.eviction_latency_le_10us
            + stats.eviction_latency_le_100us
            + stats.eviction_latency_le_1ms
            + stats.eviction_latency_le_10ms
            + stats.eviction_latency_gt_10ms
    );
    assert_eq!(
        stats.compaction_latency_samples,
        stats.compaction_latency_le_10us
            + stats.compaction_latency_le_100us
            + stats.compaction_latency_le_1ms
            + stats.compaction_latency_le_10ms
            + stats.compaction_latency_gt_10ms
    );
}


// The test-module list lives in its own file so that two changes which each add a module do
// not conflict: `.gitattributes` marks that file `merge=union`, which keeps both added lines.
// `include!` is textual, so every module below is declared in THIS module exactly as before --
// `#[path]` stays relative to this directory and `use super::*` in each test still resolves here.
include!("engine_regression_test_modules.rs");


/// The token-bucket arithmetic with explicit clocks -- the whole model is this pure function,
/// so this is the whole model under test: start full, drain to deny with an honest
/// retry-after, refill by elapsed time capped at capacity, tolerate a backwards clock, and
/// name the impossible take.
#[test]
fn bucket_take_arithmetic_with_explicit_clocks() {
    use crate::engine::execute_on_shard::bucket_take;

    // An absent bucket starts full: 10 capacity, take 3 -> 7 left.
    let (allowed, remaining, retry, state) = bucket_take(None, 1_000, 3.0, 10.0, 1.0);
    assert!(allowed);
    assert_eq!(7.0, remaining);
    assert_eq!(0, retry);
    assert_eq!((7.0, 1_000), state);

    // Drain past the level: denied, retry-after is the shortfall at the refill rate.
    let (allowed, remaining, retry, state) = bucket_take(Some(state), 1_000, 9.0, 10.0, 1.0);
    assert!(!allowed);
    assert_eq!(7.0, remaining, "a denied take consumes nothing");
    assert_eq!(2_000, retry, "2 tokens short at 1 token/sec");

    // 5 seconds later the refill covers it: 7 + 5 = 12, capped at 10, take 9 -> 1.
    let (allowed, remaining, _, state) = bucket_take(Some(state), 6_000, 9.0, 10.0, 1.0);
    assert!(allowed);
    assert_eq!(1.0, remaining);

    // A clock that moved backwards refills nothing and never panics.
    let (allowed, remaining, _, _) = bucket_take(Some(state), 5_000, 0.5, 10.0, 1.0);
    assert!(allowed);
    assert_eq!(0.5, remaining);

    // A take larger than capacity can never succeed: the sentinel says so.
    let (allowed, _, retry, _) = bucket_take(None, 1_000, 20.0, 10.0, 1.0);
    assert!(!allowed);
    assert_eq!(u64::MAX, retry);

    // Zero refill rate: a denied take also answers the sentinel rather than dividing by zero.
    let (allowed, _, retry, _) = bucket_take(Some((0.0, 1_000)), 2_000, 1.0, 10.0, 0.0);
    assert!(!allowed);
    assert_eq!(u64::MAX, retry);
}
#[path = "tests/a_row_append_publishes_only_what_it_added.rs"]
mod a_row_append_publishes_only_what_it_added;
#[path = "tests/the_holders_of_a_key_allocation.rs"]
mod the_holders_of_a_key_allocation;
#[path = "tests/what_a_high_cardinality_distinct_holds.rs"]
mod what_a_high_cardinality_distinct_holds;
#[path = "tests/an_ordered_zset_read_pays_for_its_answer.rs"]
mod an_ordered_zset_read_pays_for_its_answer;
