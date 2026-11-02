// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHICH BUCKETS ARE DIRTY IS A SET THE MAP KEEPS, AND AN IDLE ROUND DOES NOT WALK THE STORE TO
//! FIND OUT.
//!
//! Before this, three questions in every round -- the plan's dirty filter, the dump ordering and
//! the WAL reclaim plan -- were each answered by visiting every bucket node and testing a flag, and
//! two of them copied every node's claims into a `HashMap` on the way. On a store with nothing to
//! do that is a cost of the bucket count, paid to learn there is no work. A list pushed to when a
//! bucket first goes dirty answers the same question without the walk.
//!
//! TWO CLAIMS, EACH WITH A CONTROL.
//!
//!   1. THE SET IS EXACT. After every door that can change a flag -- a guarded borrow, an insert, an
//!      entry, a removal, a sweep, a `retain`, a `clear` -- the set equals what a walk of the flags
//!      says, whether the set was HELD before the door or had been discarded. The control is the
//!      comparison itself: a copy of the set taken before a flag moves must differ from the set
//!      after, or the check could not see drift.
//!   2. A SETTLED STORE'S IDLE ROUND LOOKS AT NO BUCKET NODE. Counted, not timed: this box is shared
//!      and a stopwatch on it measures the other tenants. The control is the same store with its
//!      set thrown away, whose first read walks every node.
#![allow(clippy::all)]
use super::*;
use crate::engine::reports::StorageManagerCycleRequest;
use crate::engine::state::{BucketMap, BucketNode};

fn node(routing_bucket: u32, dirty: bool) -> BucketNode {
    let mut node = BucketNode {
        routing_bucket,
        ..BucketNode::default()
    };
    node.set_dirty(dirty);
    node
}

/// What the flags say, by walking them. The thing the set must always equal.
fn by_walk(map: &BucketMap) -> std::collections::BTreeSet<u32> {
    map.iter()
        .filter(|(_routing_bucket, bucket)| bucket.dirty())
        .map(|(routing_bucket, _bucket)| *routing_bucket)
        .collect()
}

fn assert_exact(map: &BucketMap, after: &str) {
    assert_eq!(
        *map.dirty_buckets(),
        by_walk(map),
        "the dirty set disagrees with the flags after {after}"
    );
}

/// A map of ten buckets, the even ones dirty, with the set HELD so the doors have to maintain it.
fn held_map() -> BucketMap {
    let mut map = BucketMap::default();
    for routing_bucket in 0..10u32 {
        map.insert(routing_bucket, node(routing_bucket, routing_bucket % 2 == 0));
    }
    assert_exact(&map, "the build");
    map
}

#[test]
fn the_dirty_set_equals_the_flags_after_every_door() {
    // Each door is run twice: once with the set held going in, which is what has to be maintained,
    // and once with it discarded, which has to be rebuilt. `hold` decides which.
    for hold in [true, false] {
        let begin = |map: &mut BucketMap| {
            if !hold {
                // `clear` of an empty map is not a way to discard: build a fresh one and sweep.
                for bucket in map.values_mut() {
                    let _ = bucket;
                }
            } else {
                let _ = map.dirty_buckets();
            }
        };

        // A guarded borrow that sets a flag, and one that clears it.
        let mut map = held_map();
        begin(&mut map);
        map.get_mut(&1).expect("bucket 1").set_dirty(true);
        assert_exact(&map, "a guarded set of a clean bucket");
        map.get_mut(&2).expect("bucket 2").set_dirty(false);
        assert_exact(&map, "a guarded clear of a dirty bucket");

        // Set and then clear inside one borrow leaves the bucket where it was.
        {
            let mut bucket = map.get_mut(&3).expect("bucket 3");
            bucket.set_dirty(true);
            bucket.set_dirty(false);
        }
        assert_exact(&map, "a borrow that set and cleared the same flag");

        // A borrow that changed a DIFFERENT flag must not move the bucket.
        map.get_mut(&5).expect("bucket 5").set_loading(true);
        assert_exact(&map, "a borrow that touched only another flag");

        // The three ways to file or drop a node.
        begin(&mut map);
        map.insert(20, node(20, true));
        assert_exact(&map, "an insert of a dirty node");
        map.insert(20, node(20, false));
        assert_exact(&map, "an insert that REPLACED a dirty node with a clean one");
        map.remove(&4);
        assert_exact(&map, "a removal of a dirty bucket");
        map.entry(30).or_insert_with(|| node(30, true));
        assert_exact(&map, "an entry that filed a dirty node");
        map.entry(30).or_insert_with(|| node(30, false)).set_dirty(false);
        assert_exact(&map, "an entry that found a dirty node and cleared it");

        // The doors that cannot say what they did: a sweep and a retain.
        begin(&mut map);
        for bucket in map.values_mut() {
            let dirty = bucket.routing_bucket % 3 == 0;
            bucket.set_dirty(dirty);
        }
        assert_exact(&map, "a values_mut sweep");
        map.retain(|routing_bucket, bucket| {
            bucket.set_dirty(routing_bucket % 2 == 1);
            routing_bucket % 5 != 0
        });
        assert_exact(&map, "a retain that also moved flags");

        map.clear();
        assert_exact(&map, "a clear");
        assert!(map.dirty_buckets().is_empty());
    }
}

/// THE CONTROL: the comparison above can see drift. A copy of the set taken before a flag moves
/// must stop agreeing with the flags afterwards; if it did not, `assert_exact` would pass on a set
/// that never changed.
#[test]
fn a_set_copied_before_a_flag_moves_stops_agreeing() {
    let mut map = held_map();
    let before = map.dirty_buckets().clone();
    map.get_mut(&1).expect("bucket 1").set_dirty(true);
    assert_ne!(
        before,
        by_walk(&map),
        "setting a flag did not change what a walk reports, so the check above is blind"
    );
    assert_eq!(*map.dirty_buckets(), by_walk(&map));
}

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

/// A round that dumps whatever is dirty (`min_undumped_wal_records: 1` is "never wait").
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

fn bucket_count(engine: &TemporalEngine) -> usize {
    engine
        .shards
        .read()
        .expect("engine lock poisoned")
        .get(&1)
        .map(|shard| shard.bucket_index.bucket_map.len())
        .unwrap_or(0)
}

#[test]
fn a_settled_stores_idle_round_looks_at_no_bucket_node() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed(&engine, 3_000);
    let buckets = bucket_count(&engine);
    // FLOOR: a store with a handful of buckets cannot show a difference between "all" and "none".
    assert!(buckets >= 1_000, "the fixture filed only {buckets} buckets");

    // Settle: rounds until nothing is dirty. A round that never dumps would loop here for ever, so
    // the bound is part of the claim.
    let mut settled = false;
    for _ in 0..12 {
        let report = engine.run_storage_manager_cycle(round_request());
        assert!(report.errors.is_empty(), "round errored: {:?}", report.errors);
        if report.plan.dirty_buckets.is_empty() {
            settled = true;
            break;
        }
    }
    assert!(settled, "twelve rounds did not dump the store, so no idle round is being measured");

    crate::engine::reset_bucket_node_scan_visits();
    let idle = engine.run_storage_manager_cycle(round_request());
    let visits = crate::engine::bucket_node_scan_visits();
    assert!(idle.errors.is_empty(), "idle round errored: {:?}", idle.errors);
    assert!(
        idle.plan.dirty_buckets.is_empty(),
        "the 'idle' round found {} dirty buckets",
        idle.plan.dirty_buckets.len()
    );
    println!("  idle round: {visits} bucket-node visits against {buckets} buckets");
    assert!(
        (visits as usize) * 10 < buckets,
        "an idle round on a settled store looked at {visits} of {buckets} bucket nodes: the dirty \
         question is being answered by a walk again"
    );
}

/// THE CONTROL: the same store with its set discarded pays one walk of every node on the first
/// read -- so the figure above is the set doing its job, not a counter that never moves.
#[test]
fn discarding_the_set_makes_the_next_read_walk_every_node() {
    let mut map = BucketMap::default();
    for routing_bucket in 0..2_000u32 {
        map.insert(routing_bucket, node(routing_bucket, false));
    }
    crate::engine::reset_bucket_node_scan_visits();
    let _ = map.dirty_buckets();
    assert_eq!(crate::engine::bucket_node_scan_visits(), 2_000);
    // Held now: a second read walks nothing.
    crate::engine::reset_bucket_node_scan_visits();
    let _ = map.dirty_buckets();
    assert_eq!(crate::engine::bucket_node_scan_visits(), 0);
    // A sweep discards it again.
    for bucket in map.values_mut() {
        let _ = bucket;
    }
    crate::engine::reset_bucket_node_scan_visits();
    let _ = map.dirty_buckets();
    assert_eq!(crate::engine::bucket_node_scan_visits(), 2_000);
}
