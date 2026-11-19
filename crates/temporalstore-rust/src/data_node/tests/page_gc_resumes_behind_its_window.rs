// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A PERIODIC BLOCK-COLLECTION ROUND EXAMINES A BOUNDED WINDOW OF SLABS AND THE NEXT TICK RESUMES.
//!
//! Collection used to look at every collectable slab on every round, so the work of a round grew
//! with the number of stale slabs. The stage now examines at most a configured window of
//! candidates, remembers where it stopped, and is due again on the very next tick when candidates
//! remain. Three things have to hold together, and each alone is satisfied by a wrong stage:
//!
//!   * BOUNDED: no step examines more than the window. A stage that ignored the window would
//!     still finish, so the bound is asserted on every step, not on the total.
//!   * RESUMING: the same fixture is collected completely, across several steps, and the steps
//!     together remove exactly what one unbounded pass removes. A stage that stopped after the
//!     first window, or one that re-examined the same slabs, would remove less.
//!   * DENOMINATOR: the unbounded arm removes enough slabs, in one step, that "several steps of
//!     at most two" is a real split and not a store that never held more than the window.
#![allow(clippy::all)]
use super::*;
use crate::data_node::round_budget::{HeavySlot, PageCursor, ShardRoundState};
use std::time::{Duration, Instant};

const SHARD: u64 = 1;
const WINDOW: usize = 2;

/// Repeated writes over a small keyspace with a maintenance round after each batch leave behind
/// slabs no live block points at. Block collection is off while seeding so they accumulate.
fn seeded() -> DataNodeRuntime {
    const BATCH: usize = 400;
    const ROUNDS: usize = 6;
    const KEYSPACE: usize = 100;
    let engine = TemporalEngine::default();
    engine.load_shard(SHARD);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 4,
            max_background_queue_depth: 2,
        },
    );
    let seed_options = StorageManagerOptions {
        enable_block_gc: false,
        ..StorageManagerOptions::default()
    };
    let mut written = 0usize;
    for _ in 0..ROUNDS {
        let engine = runtime.engine();
        for index in 0..BATCH {
            engine.execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet {
                    key: format!("window-{:06}", (written + index) % KEYSPACE),
                    value: vec![b'v'; 96],
                },
            });
        }
        written += BATCH;
        runtime.run_storage_manager_once(SHARD, seed_options.clone());
    }
    runtime
}

#[derive(Debug)]
struct Step {
    examined: usize,
    removed: usize,
    complete: bool,
}

/// Tick until nothing is due, recording every page-collection step that ran.
fn drive(options: &StorageManagerOptions) -> (Vec<Step>, PageCursor) {
    let runtime = seeded();
    let period = Duration::from_secs(1);
    let mut state = ShardRoundState::new(Instant::now() - Duration::from_secs(60), period);
    let mut steps = Vec::new();
    for _ in 0..500 {
        let Some((slot, report)) =
            runtime.run_storage_manager_tick(SHARD, options, &mut state, period)
        else {
            break;
        };
        if slot == HeavySlot::PageGc {
            if let Some(gc) = report.gc_report.as_ref() {
                steps.push(Step {
                    examined: gc.block_gc_scan_examined_slabs,
                    removed: gc.block_slabs_removed,
                    complete: gc.block_gc_scan_complete,
                });
            }
        }
    }
    (steps, state.cursors.page)
}

#[test]
fn a_bounded_collection_resumes_and_removes_what_one_unbounded_pass_removes() {
    let unbounded_options = StorageManagerOptions {
        block_gc_max_scan_slabs_per_round: 0,
        // Only the scan window may differ between the arms.
        block_gc_max_destroy_slabs_per_round: 0,
        block_gc_max_destroy_physical_bytes_per_round: 0,
        ..StorageManagerOptions::default()
    };
    let windowed_options = StorageManagerOptions {
        block_gc_max_scan_slabs_per_round: WINDOW,
        ..unbounded_options.clone()
    };

    let (unbounded_steps, unbounded_cursor) = drive(&unbounded_options);
    let unbounded_removed: usize = unbounded_steps.iter().map(|step| step.removed).sum();
    eprintln!("  unbounded steps: {unbounded_steps:?}");
    assert_eq!(
        unbounded_steps.len(),
        1,
        "DENOMINATOR: one unbounded pass is one step: {unbounded_steps:?}"
    );
    assert!(
        unbounded_removed > WINDOW,
        "DENOMINATOR: only {unbounded_removed} slab(s) were collectable, so a window of {WINDOW} \
         could not be told from no window: {unbounded_steps:?}"
    );
    assert_eq!(unbounded_cursor, PageCursor::default());

    let (steps, cursor) = drive(&windowed_options);
    eprintln!("  windowed steps: {steps:?}");
    assert!(
        steps.len() >= 2,
        "a window of {WINDOW} over {unbounded_removed} collectable slabs must take several \
         steps: {steps:?}"
    );
    for step in &steps {
        assert!(
            step.examined <= WINDOW,
            "a step examined {} slabs against a window of {WINDOW}: {steps:?}",
            step.examined
        );
    }
    let (last, earlier) = steps.split_last().unwrap();
    assert!(last.complete, "the last step must finish the scan: {steps:?}");
    assert!(
        earlier.iter().all(|step| !step.complete),
        "a step that finished the scan must be the last one: {steps:?}"
    );
    let removed: usize = steps.iter().map(|step| step.removed).sum();
    assert_eq!(
        removed, unbounded_removed,
        "the steps together must remove what one unbounded pass removes: {steps:?}"
    );
    assert_eq!(cursor, PageCursor::default(), "a finished scan leaves no cursor behind");
}

/// A store whose slabs are all live has nothing to collect, and the stage must not even plan: the
/// gate is closed, so the round reports no collection at all. The control is the gate itself: the
/// same store must show a non-empty denominator and a closed gate, so a store with no slabs (which
/// is also "nothing to do") cannot pass for the right reason.
#[test]
fn a_store_that_is_all_live_closes_the_gate_and_the_stage_skips_collection() {
    let engine = TemporalEngine::default();
    engine.load_shard(SHARD);
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions { worker_threads: 0, max_queue_depth: 4, max_background_queue_depth: 2 },
    );
    for index in 0..400 {
        runtime.engine().execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet { key: format!("live-{index:06}"), value: vec![b'v'; 96] },
        });
    }
    let seed_options = StorageManagerOptions { enable_block_gc: false, ..StorageManagerOptions::default() };
    runtime.run_storage_manager_once(SHARD, seed_options);
    runtime.engine().publish_block_slab_live_bytes();
    let gate = runtime
        .engine()
        .block_store()
        .reclaim_gate(crate::block_store::DEFAULT_RECLAIM_UTILITY_THRESHOLD_BASIS_POINTS);
    assert!(gate.ready && gate.logical_bytes > 0, "CONTROL: the store must hold slabs: {gate:?}");
    assert!(!gate.due, "CONTROL: an all-live store must close the gate: {gate:?}");

    let period = Duration::from_secs(1);
    let mut state = ShardRoundState::new(Instant::now() - Duration::from_secs(60), period);
    for _ in 0..20 {
        let Some((slot, report)) =
            runtime.run_storage_manager_tick(SHARD, &StorageManagerOptions::default(), &mut state, period)
        else {
            break;
        };
        if slot == HeavySlot::PageGc {
            assert!(
                report.gc_report.is_none(),
                "a closed gate must skip block collection: {:?}",
                report.gc_report
            );
            return;
        }
    }
    panic!("the page-collection slot never ran");
}
