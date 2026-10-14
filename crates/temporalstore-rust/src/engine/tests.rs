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

/// Holds the one-entry-a-page gate OFF for as long as it lives, putting back whatever was there --
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



/// THE OBJECT OF EVERY PAGE IN ONE INDEX, KEYED BY THE POSITION THE PAGE IS FILED AT.
///
/// # WHY THIS AND NOT ONLY [`pages_with_identity`]
///
/// Half the walks that have to be converted cannot give up their own nesting. They need the
/// BUCKET as well as the page -- to read a tombstone row out of it, to count its object index, to
/// `retain` over it -- and a flat list of pages does not carry the bucket. This composes with
/// whatever walk is already there: take it once, then ask it per page.
///
/// IT OWNS ITS KEYS AND BORROWS NOTHING, which is what lets a `retain` use it. `retain` holds the
/// bucket map mutably and the keys are drawn from a SIBLING field of the same index, so a map that
/// borrowed would be refused; the `Arc` clones cost one refcount bump each and the borrow ends
/// when this returns.
pub(super) struct PageObjects {
    by_position: std::collections::HashMap<(u32, u64), std::sync::Arc<str>>,
}

impl PageObjects {
    /// Taken in ONE pass over the index, from the same two sources [`pages_with_identity`] reads.
    pub(super) fn of(index: &crate::engine::state::CoreIndex) -> Self {
        Self {
            by_position: pages_with_identity(index)
                .into_iter()
                .map(|(routing_bucket, handle, object_key, _page)| {
                    ((routing_bucket, handle), object_key)
                })
                .collect(),
        }
    }

    /// The object the page at this position belongs to.
    ///
    /// A PANIC AND NOT AN `Option`, for [`pages_with_identity`]'s reason: a page that nothing
    /// beside it names is a state the producer refuses, and a caller handed `None` would reach
    /// for a skip. Every caller here is walking the same index this was taken from, so the only
    /// way to miss is to ask at a position that does not exist.
    pub(super) fn at(&self, routing_bucket: u32, handle: u64) -> &std::sync::Arc<str> {
        self.by_position.get(&(routing_bucket, handle)).unwrap_or_else(|| {
            panic!(
                "no page is filed at bucket {routing_bucket} handle {handle} in the index this                  was taken from"
            )
        })
    }
}

/// EVERY PAGE OF ONE CORE INDEX, WITH THE OBJECT THAT THE STRUCTURES BESIDE IT NAME.
///
/// # WHY THIS IS SHARED AND NOT A WALK PER TEST
///
/// `BlockIndex` carries no object key. Every test that walked a real store's buckets and asked a
/// page which object it belonged to now has to ask the bucket instead, and there are some forty
/// such walks across this module tree. Written out one by one they are forty chances to pick the
/// WRONG DOOR, and the wrong door fails silently rather than loudly:
///
///   * [`PageKeysByPosition::live_identity`] answers from the object lookup alone, and
///     `insert_object_block_lookup` returns early on a deleted page -- so it names NO tombstone.
///     A walk that reports on deleted pages and asks that door drops every one of them and comes
///     back with a smaller, entirely plausible number.
///   * [`PageKeysByPosition::identity_of`] knows about both sources: the lookup for a live page,
///     and the bucket's own tombstone row for a deleted one. That is what this asks, so a caller
///     cannot get the tombstone case wrong by omitting to think about it.
///
/// # A LIVE PAGE NAMED BY NOTHING IS A PANIC; AN UNNAMED TOMBSTONE IS A SKIP
///
/// THIS WAS A FLAT PANIC OVER BOTH STATES AND THAT WAS WRONG, AND THE SUITE IS WHAT SAID SO. The
/// paragraph here read "A PAGE NAMED BY NOTHING IS A PANIC, NOT A SKIP ... Both are states a test
/// walking a store should fail on, loudly", naming the window between a decode and its rebuild and
/// a removal that named no element.
///
/// THE SECOND IS NOT A DEFECT, AND `identity_of`'s OWN DOC SAYS SO: a deleted page whose removal
/// named no element "is the one page in this store whose object nothing beside it states".
/// Production's `page_identities_in` skips it through a `filter_map`, with the note that in every
/// caller it means "this page is not the one I am matching".
///
/// `hash_read_union_divergence::an_index_entry_marked_deleted_loses_no_field_and_is_not_counted`
/// plants exactly that page -- it marks an entry deleted in place, with no tombstone row -- and
/// the flat panic fired while this walk was still BUILDING its list, before any caller's
/// `!page.deleted` filter could short-circuit. A test whose whole subject is that page could not
/// run.
///
/// SO THE TWO STATES ARE SPLIT, and the assertion says which is which:
///
///   * a LIVE page the lookup does not name PANICS with the handle. That is the decode-before-
///     rebuild window, which the decoder closed by filing as it decodes, so it is a defect.
///   * a DELETED page no tombstone row names is SKIPPED. Its object is genuinely unstated and
///     there is nothing for a walk to attribute it to.
///
/// A skip is still the dangerous direction, which is why only the second case gets one: a skip on
/// the first would be a count quietly short, the denominator mistake this campaign keeps
/// re-finding in a new costume.
///
/// # WHAT THIS IS NOT FOR
///
/// A test whose SUBJECT is a page filed under a bucket its key does not route to. This asks by
/// POSITION, and the lookup names such a page at the position it was filed under, not the one it
/// now sits in -- so this would panic on the very state that test is driving.
/// [`PageKeysByPosition::identity_anywhere`] is the door for that, and
/// `bucket_fill`'s mis-filing walk uses it directly and says so.
pub(super) fn pages_with_identity(
    index: &crate::engine::state::CoreIndex,
) -> Vec<(u32, u64, std::sync::Arc<str>, &crate::engine::state::BlockIndex)> {
    let keys = crate::engine::state::PageKeysByPosition::from_lookup(&index.object_block_lookup);
    let mut out = Vec::new();
    for bucket in index.bucket_map.values() {
        for (handle, page) in &bucket.block_index {
            let Some(beside) = keys.identity_of(bucket, *handle, page) else {
                assert!(
                    page.deleted,
                    "the LIVE page at handle {handle} in bucket {} is named by nothing beside \n                     it, and a live page is named by the object lookup unless the index is in \n                     the one window the decoder closed by filing as it decodes",
                    bucket.routing_bucket
                );
                continue;
            };
            out.push((
                bucket.routing_bucket,
                *handle,
                std::sync::Arc::clone(beside.object_key),
                page,
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


mod conformance;
mod concurrent_commit;
mod raft_apply_coalesce;
mod phase1_flat;
mod part1;
mod part2;
mod quota;
mod part3;
mod part4;
mod expiry_scale;
mod reclaim_dump;
mod claim_stamp_hold;
mod command_surface;
mod expiry_command_boundaries;
mod address_footprint;
mod address_word_footprint;
mod serving_read_guard;
mod poison_shard_guard;
mod restore_serving_gate;
mod page_and_block_counter_ratio;
mod manifest_restore_model_maps;
mod prune_crash_window;
mod install_on_load_anchor;
mod index_log_replay_reach;
mod round_scale;
mod round_stage_cost;
mod round_backlog_cost;
mod round_walk_scope;
mod bucket_filing_range;
mod inner_range;
mod shard_carried_range;
mod shard_carried_identity;
mod index_bytes_per_key;
mod sibling_handles;
mod level_two_value;
mod log_resident_is_derived;
mod fold_hash_map_completeness;
mod recorded_map_invariant;
mod set_install_census;
mod model_number_agreement;
mod model_slot_is_a_position;
mod dump_scale;
mod dump_release_scale;
mod restore_scale;
mod restore_large_scale;
mod promote_precondition;
mod alloc_class_scale;
mod evict_scale;
mod cache_invalidation_cost;
mod cache_read_cost;
mod warm_read_decomposition;
mod wal_reclaim_frame_boundary;
mod pages_per_bucket;
mod bucket_fill;
mod routing_range_default;
mod context_node_survives_reload;
mod durable_outranks_derived;
mod per_item_byte_budget;
mod flat_page_list;
mod range_free_bucket_id;
mod container_member_shadow;
mod element_ordinal_reuse;


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
mod page_entry_names;
mod page_entry_handles;

mod bucket_sequence_budget;
mod bucket_node_arms;
mod bucket_node_container_header;
mod bucket_flag_masks;
mod inline_arm_trade;
mod layout_arm_selection;
mod entry_object_identity;
mod bucket_tombstone_budget;
mod entry_count_versus_page_count;
mod page_entry_name_pointer;
mod bucket_mark_in_place_budget;
mod bucket_dirty_share_under_dumps;
mod model_map_container_cost;
mod container_page_ordinal;
mod page_entry_element_naming;
mod index_resident_truth;
mod carried_page_identity;
mod campaign_end_to_end;
mod length_answer_and_listing_agree;
mod set_listing_page_reads;
mod object_is_a_key_not_an_element;
mod container_page_element_key;
mod the_entry_cannot_name_its_object_by_slot;
mod the_entry_cannot_point_at_the_object_list;
mod resident_map_readers;
mod container_pages_are_batched;
mod view_rebuild_page_read_failures;
mod folded_page_membership;
mod container_tombstone_collection;
mod container_tombstone_entry;
mod tombstone_reload_path;
mod the_dump_door_reads_the_index_stamp;
mod hash_container_reload_authority;
mod decoded_index_packing;
mod two_level_resident_shape;
mod adopting_the_ported_index_log_record;
mod reconcile_allocation;
mod hash_read_union_divergence;
// `packed_element_scan_cost` WAS DECLARED HERE AND THE FILE IS DELETED.
//
// It priced two arms in nanoseconds at occupancies from one element to four hundred: ARM A, the
// per-element level of the resident lookup -- `ObjectBlockRefs::position`, a binary search whose
// comparison dereferenced an `Option<Arc<str>>` into a separate allocation and compared the name
// -- against ARM B, `select_container_element`'s packed linear scan. Its own words: "Arm A is the
// cost that goes; arm B is the cost that stays."
//
// ARM A IS GONE, so the trade it measured is settled rather than open. `ObjectBlockRefs`,
// `ComponentList` and `ComponentBlocks` are deleted and an object maps straight to its
// `BlockRefs`; there is no per-element level left to price and no `position` to call.
//
// IT COULD NOT BE REPAIRED MECHANICALLY, AND ITS OWN DOC SAID NOT TO TRY. The fixture built the
// per-element level BY HAND -- `occupancy` distinct `Some(name)` entries under one object, a state
// the producer could not reach even before the collapse, since `insert_object_block_lookup` wrote
// `component: None` as a literal. And on keeping arm B alone it is explicit: "A measurement that
// priced B alone would be pricing the status quo." Arm B is the shipped selector and both read
// doors already call it unconditionally, so nothing it measured about the scan has changed.
//
// WHAT IT ESTABLISHED, KEPT ON THE RECORD because it is the reasoning the collapse rests on: the
// recorded 3.0-17.8x that read as a refutation was a ratio over a different denominator -- a scan
// of one of 1,024 fixed routing buckets, whose length grows with the corpus -- while the scan it
// was being compared against walks the elements of ONE OBJECT, whose length is a property of the
// data shape and not of the corpus. The two numbers answer different questions and the first never
// bounded the second.
//
// THE ELEMENT-NAME BRANCH'S LAST CHANGE TO IT WAS A WIDTH RESTATEMENT, AND THAT IS WHY THIS
// DELETION IS A MERGE DECISION AND NOT ONLY THIS BRANCH'S. `ea2b26bb7` restated one pin inside
// `resident_bytes_per_object` -- `size_of::<BlockIndex>()` from 56 to 40, with a note that the
// arithmetic MULTIPLIES it so a stale width scales the whole column. That pin's subject survives;
// the arithmetic it fed does not, because the column priced the chain `ObjectBlockRefs ->
// ComponentList -> ComponentBlocks -> BlockRefs` per object and three of those four are deleted.
// The 40 is not lost with the file: it is a `const _` beside `BlockIndex` in `state.rs` and is
// re-asserted in `page_entry_handles`, `pages_per_bucket`, `per_item_byte_budget`,
// `page_entry_element_naming` and `the_entry_cannot_name_its_object_by_slot`.
mod set_listing_source;
mod projection_names_a_page;
mod authority_check_cost;
mod sweep_keeps_the_collapse;
mod ordinal_under_the_gate;
mod gated_removal;
mod replay_under_the_gate;
mod removal_the_index_can_find;
mod gated_listing_folds_the_pages;
mod the_slot_survives_a_reload;
mod the_handle_is_computed_at_the_bucket;
mod gated_corpus_across_a_store_boundary;
mod write_after_fold;
mod collapse_for_list;
mod zset_component_drops_the_score;
mod entry_width_at_each_step;
mod an_assigned_durable_id_defeats_ground_one_only;
mod where_the_characters_are_durable_per_kind;
mod what_an_assigned_id_costs_the_object_lookup;
mod what_the_core_index_puts_on_the_wire;
mod the_write_path_leaves_the_bucket_ttl_alone;
mod marking_an_object_dirty_reaches_its_pages;
mod one_record_per_object;
mod the_expiry_pair_shares_one_allocation;
mod one_key_two_kinds;
mod context_node_map_arrival;
mod the_third_door_stays_shut;
