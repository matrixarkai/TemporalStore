// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE CLEAR AFTER A DUMP ASKS THE BUCKETS THE MANIFEST NAMES, AND DOES NOT WALK THE SHARD TO DO IT.
//!
//! The clear reads one figure -- the derived dirty generation -- for the buckets a manifest names, and
//! it used to take that figure from `bucket_storage_summaries`, which materialises every live block
//! in the shard. For a dirty node the figure is the node's own generation plus the bucket's
//! dirty-object count, so `dirty_generations_of` reads those two and nothing else, and falls back to
//! the walk for any bucket it cannot answer that way.
//!
//! THREE CLAIMS, EACH WITH A CONTROL.
//!
//!   1. THE ANSWER IS THE WALK'S ANSWER, for dirty nodes with dirty objects and for dirty nodes
//!      with none. The control is that the walk is observably expensive here and that the figures
//!      are not all zero, so equality is not two empty maps agreeing.
//!   2. IT COSTS NO WALK. Counted in live-block entries visited, not timed: this box is shared and a
//!      stopwatch on it measures the other tenants.
//!   3. A BUCKET IT CANNOT ANSWER TAKES THE WALK, and gets the walk's answer. The control is the
//!      fallback counter moving by exactly one.
//!
//! The counters are process-wide, like those the neighbouring scale tests read, so this file is run
//! single-threaded as they are.
#![allow(clippy::all)]
use super::*;
use crate::engine::reports::StorageManagerCycleRequest;
use crate::engine::storage_reporting::{
    bucket_storage_summaries, dirty_bucket_claims, dirty_generations_of,
    DIRTY_GENERATION_FALLBACK_WALKS,
};

fn round_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn seed(engine: &TemporalEngine, count: usize) {
    let mut index = 0usize;
    while index < count {
        let end = (index + 500).min(count);
        let commands = (index..end)
            .map(|cursor| Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: vec![b'v'; 128],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
}

fn round_request() -> StorageManagerCycleRequest {
    StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: false,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        max_dump_buckets_per_round: 100_000,
        min_undumped_wal_records: 1,
        min_undumped_wal_bytes: 1,
        ..StorageManagerCycleRequest::default()
    }
}

fn fallbacks() -> u64 {
    DIRTY_GENERATION_FALLBACK_WALKS.load(std::sync::atomic::Ordering::Relaxed)
}

#[test]
fn a_dirty_buckets_generation_is_the_walks_answer_and_costs_no_walk() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed(&engine, 2_000);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let (start, end) = shard.routing_range();

    let named: Vec<u32> = dirty_bucket_claims(shard).into_keys().take(300).collect();
    // FLOOR: a handful of buckets cannot show a difference between asking a few and asking all.
    assert!(named.len() >= 200, "the fixture left only {} dirty buckets", named.len());

    // THE WALK'S ANSWER, and the control that the walk is the expensive thing being avoided.
    crate::engine::reset_live_block_scan_entries();
    let walked: std::collections::HashMap<u32, u64> = bucket_storage_summaries(shard, start, end)
        .into_iter()
        .map(|summary| (summary.routing_bucket, summary.dirty_generation))
        .collect();
    let walk_cost = crate::engine::live_block_scan_entries();
    assert!(
        walk_cost >= 1_500,
        "the walk visited only {walk_cost} live blocks, so avoiding it proves nothing"
    );

    // THE NAMED BUCKETS' ANSWER.
    crate::engine::reset_live_block_scan_entries();
    let before = fallbacks();
    let asked = dirty_generations_of(shard, &named, start, end);
    let ask_cost = crate::engine::live_block_scan_entries();
    let fell_back = fallbacks() - before;

    assert_eq!(ask_cost, 0, "asking {} dirty buckets visited {ask_cost} live blocks", named.len());
    assert_eq!(fell_back, 0, "a dirty node took the whole-shard fallback");
    assert_eq!(asked.len(), named.len());
    for routing_bucket in &named {
        assert_eq!(
            asked[routing_bucket], walked[routing_bucket],
            "bucket {routing_bucket}: the named answer differs from the walk's"
        );
    }
    // CONTROL: equality of two maps of zeros would pass. These carry the dirty-object count.
    assert!(
        named.iter().any(|routing_bucket| walked[routing_bucket] > 0),
        "every generation was zero, so the comparison above could not tell a wrong sum from a right one"
    );
}

#[test]
fn a_dirty_node_with_no_dirty_object_is_still_the_walks_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed(&engine, 1_200);
    // Settle: rounds until nothing is dirty, so the nodes below are clean going in.
    let mut settled = false;
    for _ in 0..12 {
        let report = engine.run_storage_manager_cycle(round_request());
        assert!(report.errors.is_empty(), "round errored: {:?}", report.errors);
        if report.plan.dirty_buckets.is_empty() {
            settled = true;
            break;
        }
    }
    assert!(settled, "twelve rounds did not dump the store");

    // Mark some clean nodes dirty DIRECTLY: dirty node, a live block, and no dirty object.
    let named: Vec<u32> = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1");
        let ids: Vec<u32> = shard.bucket_index.bucket_map.iter().map(|(id, _)| *id).take(50).collect();
        assert_eq!(ids.len(), 50, "the settled store holds fewer than 50 buckets");
        for id in &ids {
            let mut node = shard.bucket_index.bucket_map.get_mut(id).expect("node");
            assert!(!node.dirty(), "bucket {id} was dirty after settling");
            node.set_dirty(true);
            node.dirty_generation = node.dirty_generation.saturating_add(7);
        }
        ids
    };

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let (start, end) = shard.routing_range();
    let walked: std::collections::HashMap<u32, u64> = bucket_storage_summaries(shard, start, end)
        .into_iter()
        .map(|summary| (summary.routing_bucket, summary.dirty_generation))
        .collect();
    let before = fallbacks();
    let asked = dirty_generations_of(shard, &named, start, end);
    assert_eq!(fallbacks() - before, 0, "a dirty node took the whole-shard fallback");
    for routing_bucket in &named {
        assert_eq!(
            asked[routing_bucket], walked[routing_bucket],
            "bucket {routing_bucket}: dirty node, no dirty object"
        );
        // CONTROL: the generation moved by the 7 added above, so this is the node's own figure
        // and not a count of dirty objects, of which there are none.
        assert!(asked[routing_bucket] >= 7);
    }
}

#[test]
fn a_bucket_it_cannot_answer_takes_the_walk_and_gets_the_walks_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed(&engine, 1_200);
    let mut settled = false;
    for _ in 0..12 {
        let report = engine.run_storage_manager_cycle(round_request());
        assert!(report.errors.is_empty(), "round errored: {:?}", report.errors);
        if report.plan.dirty_buckets.is_empty() {
            settled = true;
            break;
        }
    }
    assert!(settled, "twelve rounds did not dump the store");

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let (start, end) = shard.routing_range();
    // A clean node, and a bucket with no node at all.
    let clean = *shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(id, _)| id)
        .next()
        .expect("a bucket");
    let absent = (0..u32::MAX)
        .find(|id| shard.bucket_index.bucket_map.get(id).is_none())
        .expect("an absent bucket id");

    for routing_bucket in [clean, absent] {
        let walked: std::collections::HashMap<u32, u64> =
            bucket_storage_summaries(shard, start, end)
                .into_iter()
                .map(|summary| (summary.routing_bucket, summary.dirty_generation))
                .collect();
        let before = fallbacks();
        let asked = dirty_generations_of(shard, &[routing_bucket], start, end);
        assert_eq!(
            fallbacks() - before,
            1,
            "bucket {routing_bucket} is not a dirty node, so exactly one fallback walk was due"
        );
        assert_eq!(asked, walked, "the fallback is the walk's whole answer");
    }
}
