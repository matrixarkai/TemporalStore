// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A COLLECTION ROUND WITH NOTHING TO COLLECT DOES NO WHOLE-SHARD WORK, AND A WRONG TALLY NEVER
//! COSTS A LIVE SLAB.
//!
//! Collection used to build the set of live slab ids by walking every index entry of every loaded
//! shard on every round, whether or not any slab was a candidate. The store keeps a per-slab tally
//! of live blocks as it goes, so when that tally accounts for every slab collection could remove,
//! the answer is a map read. Three arms, because each guards a different way to be wrong:
//!
//!   * IDLE: every slab is live or current. No walk may happen, and nothing may be removed.
//!   * COLLECTABLE: a slab loses its last live block. The walk MUST happen (the tally only says a
//!     slab looks dead; the walk confirms it) and the slab MUST be removed. Without this arm, an
//!     implementation that never collects would pass the first one.
//!   * WRONG TALLY: the tally is made to forget a slab that still holds live blocks. The slab
//!     looks collectable, so the walk must run, find it live, and keep it. This is the arm an
//!     implementation that trusts the tally to destroy would fail, and it is the one that loses
//!     data when it does.
//!
//! The same idle round also builds no lifecycle plan (a second survey of the whole shard, now
//! opt-in through the request) and asks the allocator for no trim (a walk of its free lists,
//! now only after a round that released something). The controls are the arms that DO collect
//! or DO ask for the plan: they must show one of each, or "zero" would only mean "never".
//!
//! Each arm first asserts the tally was derived, so none of them can pass by taking the fallback
//! walk for a reason unrelated to what it is about.
#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 1;

fn runtime_with_slabs(slabs: usize, keys_per_slab: usize) -> DataNodeRuntime {
    let engine = TemporalEngine::default();
    engine.load_shard(SHARD);
    for slab in 0..slabs {
        for key in 0..keys_per_slab {
            let response = engine.execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet {
                    key: format!("slab{slab}-key{key:04}"),
                    value: vec![b'v'; 96],
                },
            });
            assert!(response.status.ok);
        }
        engine.block_store().roll_slab().unwrap();
    }
    DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    )
}

/// What one collection round cost on THIS thread, so the counters belong to it.
struct Round {
    walks: u64,
    plans: u64,
    trims: u64,
    response: crate::data_node::GcResponse,
}

fn collect(runtime: &DataNodeRuntime, retain_from: u64) -> Round {
    collect_with(runtime, retain_from, false)
}

fn collect_with(runtime: &DataNodeRuntime, retain_from: u64, include_lifecycle_plan: bool) -> Round {
    let walks = crate::data_node::task_execution::gc_live_set_walks();
    let plans = crate::data_node::task_execution::gc_lifecycle_plans();
    let trims = crate::data_node::task_execution::gc_heap_trims();
    let response = crate::data_node::task_execution::run_gc_inner(
        &runtime.inner,
        GcRequest {
            shard_id: SHARD,
            retain_wal_from_sequence: None,
            retain_index_log_from_sequence: None,
            retain_block_slabs_from_id: Some(retain_from),
            block_gc_delayed_destroy: false,
            // The loop's setting: only the cache entries of slabs actually removed. The default drops
            // the whole shard's cache, which would make an idle round release memory by itself.
            block_gc_invalidate_removed_slabs_only: true,
            block_gc_max_destroy_slabs_per_round: 0,
            block_gc_max_destroy_physical_bytes_per_round: 0,
            block_gc_min_slab_garbage_basis_points: 0,
            block_gc_shared_store_cursors: Vec::new(),
            block_gc_raft_snapshot_refs: Vec::new(),
            block_gc_checkpoint_floor_slab_id: None,
            block_gc_raft_install_floor_slab_id: None,
            include_lifecycle_plan,
        },
    );
    assert!(response.status.ok, "{:?}", response.status);
    Round {
        walks: crate::data_node::task_execution::gc_live_set_walks() - walks,
        plans: crate::data_node::task_execution::gc_lifecycle_plans() - plans,
        trims: crate::data_node::task_execution::gc_heap_trims() - trims,
        response,
    }
}

fn assert_tally_derived(runtime: &DataNodeRuntime) {
    assert!(
        runtime.engine().block_slab_live_tallies(SHARD).is_some(),
        "the tally was never derived, so this arm would take the fallback walk for the wrong reason"
    );
}

#[test]
fn an_idle_collection_round_does_not_walk_the_shard() {
    let runtime = runtime_with_slabs(3, 40);
    assert_tally_derived(&runtime);
    let round = collect(&runtime, 100);
    assert_eq!(round.walks, 0, "a round with nothing collectable walked every shard's index");
    assert_eq!(round.plans, 0, "an idle round built a lifecycle plan nobody asked for");
    assert!(round.response.lifecycle_plan.is_none());
    assert_eq!(round.trims, 0, "an idle round asked the allocator for a trim");
    assert_eq!(round.response.block_slabs_removed, 0);
    assert!(
        round.response.block_slabs_retained_live >= 3,
        "the live slabs were not reported as retained: {}",
        round.response.block_slabs_retained_live
    );
}

#[test]
fn a_slab_that_lost_its_last_live_block_is_still_collected() {
    let runtime = runtime_with_slabs(3, 40);
    assert_tally_derived(&runtime);
    // Rewrite every key that lives in slab 0, so nothing points into it any more.
    for key in 0..40 {
        let response = runtime.engine().execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("slab0-key{key:04}"),
                value: vec![b'w'; 96],
            },
        });
        assert!(response.status.ok);
    }
    assert!(runtime.engine().block_store().slab_ids().unwrap().contains(&0));
    let round = collect(&runtime, 100);
    assert_eq!(round.walks, 1, "a collectable slab was removed without the authoritative walk");
    assert!(
        round.response.block_slabs_removed >= 1,
        "control: a slab with no live block was not collected, so the idle arm proves nothing"
    );
    assert_eq!(round.trims, 1, "a round that released a slab did not ask for a trim");
    assert_eq!(round.plans, 0);
    assert!(!runtime.engine().block_store().slab_ids().unwrap().contains(&0));
}

#[test]
fn a_tally_that_forgot_a_live_slab_costs_a_walk_and_not_the_slab() {
    let runtime = runtime_with_slabs(3, 40);
    assert_tally_derived(&runtime);
    assert!(runtime.engine().live_block_slab_ids(SHARD).contains(&0));
    runtime.engine().forget_tally_slab_for_test(SHARD, 0);
    assert!(
        !runtime
            .engine()
            .live_block_slab_ids_from_tally()
            .unwrap()
            .contains(&0),
        "the tally still names slab 0, so this arm is not testing a wrong tally"
    );
    let round = collect(&runtime, 100);
    assert_eq!(round.walks, 1, "a slab the tally did not name was not confirmed by the walk");
    assert_eq!(
        round.response.block_slabs_removed, 0,
        "a live slab was destroyed on the tally's word"
    );
    assert!(runtime.engine().block_store().slab_ids().unwrap().contains(&0));
    assert_eq!(
        runtime.engine().execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: "slab0-key0000".to_string() },
        }).response,
        CommandResponse::Bytes { value: Some(vec![b'v'; 96]) }
    );
}

/// The plan is still available to a caller that asks for it, and then it is built exactly once.
/// This is the control for the zero above: without it, "no plan" could mean the plan is gone.
#[test]
fn the_lifecycle_plan_is_built_once_when_the_request_asks_for_it() {
    let runtime = runtime_with_slabs(3, 40);
    assert_tally_derived(&runtime);
    let round = collect_with(&runtime, 100, true);
    assert_eq!(round.plans, 1);
    assert!(round.response.lifecycle_plan.is_some());
}
