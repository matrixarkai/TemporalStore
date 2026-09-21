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
use crate::{BlockAddress, BlockStoreOptions, BlockStore};

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
mod upsert_deltas;
mod part3;
mod part4;
mod expiry_scale;
mod reclaim_dump;
mod claim_stamp_hold;
mod command_surface;
mod expiry_command_boundaries;
mod address_footprint;
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
mod index_bytes_per_key;
mod dump_scale;
mod dump_release_scale;
mod restore_scale;
mod restore_large_scale;
mod promote_precondition;
mod alloc_class_scale;
mod evict_scale;
mod cache_invalidation_cost;
mod cache_read_cost;
mod wal_reclaim_frame_boundary;
mod per_item_byte_budget;


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
