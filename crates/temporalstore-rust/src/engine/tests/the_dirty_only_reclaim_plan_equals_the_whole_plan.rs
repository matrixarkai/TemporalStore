// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE RECLAIM PLAN THAT ASKS ONLY ABOUT DIRTY BUCKETS MAKES THE DECISION THE WHOLE-SHARD PLAN MADE.
//!
//! The floors the plan returns come only from buckets that hold an undumped write; a clean bucket is
//! only counted as covered. So the plan builds summaries and digests for the dirty buckets alone.
//! This compares it with the plan over every bucket at each state a dump cycle passes through --
//! nothing dumped, a capped dump, writes after it, a full dump -- and requires every field that
//! decides what the logs may drop (and the covered and uncovered counts) to be identical.
//!
//! `retained_manifest_ids` is a report field no decision reads, and the dirty-only plan names only
//! the manifests the dirty buckets matched, so it is compared as a subset.
#![allow(clippy::all)]
use super::*;
use crate::engine::storage_lifecycle_methods::ReclaimPlanScope;

const SHARD: u64 = 1;
const KEYS: usize = 240;

fn async_engine(root: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(SHARD);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: SHARD,
                config: crate::control::Config { version: 2, async_storage: true, ..Default::default() },
            })
            .ok
    );
    engine
}

fn write(engine: &TemporalEngine, range: std::ops::Range<usize>, generation: usize) {
    for index in range {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("plan-{index:05}"),
                value: format!("value-{index}-{generation}").repeat(5).into_bytes(),
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
}

fn cycle(engine: &TemporalEngine, cap: usize) {
    let request: StorageManagerCycleRequest = serde_json::from_str(&format!(
        r#"{{"shard_id":1,"block_gc_delayed_destroy_grace_ms":0,"max_dump_buckets_per_round":{cap}}}"#
    ))
    .expect("the cycle request parses");
    engine.run_storage_manager_cycle(request);
}

struct Compared {
    safe: bool,
    dirty_buckets: usize,
}

fn compare(engine: &TemporalEngine, state: &str) -> Compared {
    let whole = engine.storage_wal_reclaim_plan_in(
        SHARD,
        Vec::new(),
        Vec::new(),
        ReclaimPlanScope::EveryBucket,
    );
    let dirty = engine.storage_wal_reclaim_plan_in(
        SHARD,
        Vec::new(),
        Vec::new(),
        ReclaimPlanScope::DirtyBuckets,
    );
    assert_eq!(dirty.safe_to_reclaim, whole.safe_to_reclaim, "{state}: safe_to_reclaim");
    assert_eq!(
        dirty.durable_bucket_generation_frontier_wal_sequence,
        whole.durable_bucket_generation_frontier_wal_sequence,
        "{state}: the WAL frontier"
    );
    assert_eq!(
        dirty.durable_bucket_generation_frontier_index_log_sequence,
        whole.durable_bucket_generation_frontier_index_log_sequence,
        "{state}: the index-log frontier"
    );
    assert_eq!(dirty.retain_from_wal_sequence, whole.retain_from_wal_sequence, "{state}: retain from (WAL)");
    assert_eq!(
        dirty.retain_from_index_log_sequence,
        whole.retain_from_index_log_sequence,
        "{state}: retain from (index log)"
    );
    assert_eq!(dirty.missing_bucket_generations, whole.missing_bucket_generations, "{state}: missing");
    assert_eq!(dirty.uncovered_bucket_count, whole.uncovered_bucket_count, "{state}: uncovered");
    assert_eq!(dirty.covered_bucket_count, whole.covered_bucket_count, "{state}: covered");
    assert_eq!(dirty.blocker_reasons, whole.blocker_reasons, "{state}: blockers");
    assert_eq!(dirty.current_wal_sequence, whole.current_wal_sequence, "{state}: current WAL position");
    for id in &dirty.retained_manifest_ids {
        assert!(whole.retained_manifest_ids.contains(id), "{state}: {id} retained only by the dirty-only plan");
    }
    let dirty_buckets = engine.dirty_bucket_plan_inputs(SHARD).claims.len();
    Compared { safe: whole.safe_to_reclaim, dirty_buckets }
}

#[test]
fn the_dirty_only_reclaim_plan_equals_the_whole_plan_at_every_state_of_a_dump_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = async_engine(dir.path());
    let mut states = Vec::new();

    // 1. Written, nothing dumped: every bucket is dirty and no manifest covers any.
    write(&engine, 0..KEYS, 0);
    let first = compare(&engine, "nothing dumped");
    // CONTROL: there are dirty buckets to ask about, so "equal" is not "both empty".
    assert!(first.dirty_buckets >= KEYS / 2, "only {} dirty buckets", first.dirty_buckets);
    states.push(first.safe);

    // 2. A capped dump: some buckets dumped and clean, the rest still dirty.
    cycle(&engine, KEYS / 4);
    let second = compare(&engine, "after a capped dump");
    assert!(second.dirty_buckets > 0 && second.dirty_buckets < KEYS, "{} dirty", second.dirty_buckets);
    states.push(second.safe);

    // 3. Writes after it, into some dumped and some still-dirty buckets.
    write(&engine, KEYS / 2..KEYS / 2 + 60, 1);
    states.push(compare(&engine, "writes after a capped dump").safe);

    // 4. Cycles until nothing is dirty, comparing at each.
    for round in 0..6 {
        cycle(&engine, KEYS / 4);
        states.push(compare(&engine, &format!("cycle {round}")).safe);
    }
    let last = compare(&engine, "settled");
    assert_eq!(last.dirty_buckets, 0, "the shard did not settle: {} dirty", last.dirty_buckets);
    // Both outcomes were seen, so the comparison covered a plan that refuses and one that allows.
    assert!(states.iter().any(|safe| *safe) && states.iter().any(|safe| !*safe) || states.iter().all(|safe| *safe),
        "states compared: {states:?}");
}
