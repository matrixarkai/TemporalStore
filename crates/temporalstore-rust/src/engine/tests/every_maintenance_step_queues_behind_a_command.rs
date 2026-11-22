// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A MAINTENANCE STEP AND A COMMAND NEVER RUN ON ONE SHARD AT THE SAME TIME, AND THE LOCK ALONE SAYS SO.
//!
//! THE DISPATCH MODEL, as the source has it. There is no owner thread and no command queue. A client
//! command runs on the caller's thread and takes `shards.write()` for its whole body; every
//! maintenance step runs on whichever thread drives it (the periodic loop's thread, the tick
//! scheduler's, the on-demand cycle's worker) and reaches shard state only through `shards.read()` or
//! `shards.write()`. The engine-wide `RwLock` is the one thing that serialises a step against a
//! command, so the property worth pinning is: a step that touches a shard cannot make progress while
//! a command holds the lock, and cannot be overtaken by one while it holds it.
//!
//! WHAT THIS PINS. For each entry point a maintenance stage calls, the test holds the shard-table
//! WRITE guard on its own thread (a command in flight), runs the step on another thread, and requires
//! that the step has NOT finished after a settle period and DOES finish once the guard drops. A step
//! that finished while a command held the lock reached shard state around the lock, which is the
//! race the owner-thread design exists to exclude.
//!
//! THE NEGATIVE CONTROL. `a_step_that_skips_the_lock_is_seen_to_skip_it` runs the same harness on a
//! step that never touches `shards` and requires the harness to call it unserialised. Without that
//! control a harness that could never say "unserialised" would pass every step.
//!
//! WHAT IS NOT CLAIMED. This is serialisation, not boundedness: a step may still hold the guard for
//! as long as its work takes. The per-hold work bound is a separate guard per stage.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]

use super::*;

use std::sync::{mpsc, Arc};
use std::time::Duration;

const SETTLE: Duration = Duration::from_millis(400);
const PATIENCE: Duration = Duration::from_secs(30);

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn seed(engine: &TemporalEngine) {
    for index in 0..64u8 {
        let outcome = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{index}"),
                value: vec![b'a' + (index % 26); 40],
            },
        });
        assert!(outcome.status.ok, "{:?}", outcome.response);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    /// Did not finish while the guard was held, finished after it dropped.
    QueuedBehindTheGuard,
    /// Finished while the guard was held: it never asked the lock.
    RanAroundTheGuard,
    /// Never finished at all: a defect in the step or the harness, not a verdict.
    Hung,
}

/// Run `step` on its own thread while this thread holds the shard-table write guard.
fn verdict(engine: &Arc<TemporalEngine>, step: impl FnOnce(&TemporalEngine) + Send + 'static) -> Verdict {
    let (sender, receiver) = mpsc::channel();
    let held = engine.shards.write().expect("engine lock poisoned");
    let worker = {
        let engine = Arc::clone(engine);
        std::thread::spawn(move || {
            step(&engine);
            let _ = sender.send(());
        })
    };
    let finished_while_held = receiver.recv_timeout(SETTLE).is_ok();
    drop(held);
    let result = if finished_while_held {
        Verdict::RanAroundTheGuard
    } else if receiver.recv_timeout(PATIENCE).is_ok() {
        Verdict::QueuedBehindTheGuard
    } else {
        Verdict::Hung
    };
    let _ = worker.join();
    result
}

type Step = Box<dyn FnOnce(&TemporalEngine) + Send>;

/// Every shard-touching entry point the maintenance stages call, by name.
fn steps() -> Vec<(&'static str, Step)> {
    vec![
        (
            "expire sweep",
            Box::new(|e| {
                let _ = e.sweep_expired_records_with_request(ShardExpirySweepRequest {
                    shard_id: 1,
                    max_hot_buckets_per_round: 100,
                    max_cold_buckets_per_round: 5,
                    load_cold_buckets: false,
                });
            }),
        ),
        (
            "compaction",
            Box::new(|e| {
                let _ = e.compact_shard_blocks(1);
            }),
        ),
        (
            "evict round",
            Box::new(|e| {
                let _ = e.apply_storage_eviction(1, 1, 10, true, false);
            }),
        ),
        (
            "lifecycle plan",
            Box::new(|e| {
                let _ = e.storage_lifecycle_plan(StorageLifecycleRequest {
                    shard_id: 1,
                    ..Default::default()
                });
            }),
        ),
        (
            "dump manifest",
            Box::new(|e| {
                let _ = e.create_bucket_dump_manifest(1, [0u32]);
            }),
        ),
        (
            "live slab set, one shard",
            Box::new(|e| {
                let _ = e.live_block_slab_ids(1);
            }),
        ),
        (
            "live slab set, all shards",
            Box::new(|e| {
                let _ = e.live_block_slab_ids_all_shards();
            }),
        ),
        (
            "stale range prune",
            Box::new(|e| {
                let _ = e.discard_recorded_stale_ranges([1u64, 2, 3]);
            }),
        ),
    ]
}

#[test]
fn every_maintenance_step_queues_behind_a_command() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    let mut escaped = Vec::new();
    let mut checked = 0usize;
    for (name, step) in steps() {
        checked += 1;
        match verdict(&engine, step) {
            Verdict::QueuedBehindTheGuard => {}
            other => escaped.push(format!("{name}: {other:?}")),
        }
    }
    assert!(escaped.is_empty(), "steps not serialised by the shard-table lock: {escaped:?}");
    assert_eq!(8, checked, "the step table changed: update this count with it");
}

#[test]
fn a_command_queues_behind_a_step_that_holds_the_lock() {
    // The converse: while a maintenance step holds the guard, a command waits. The guard stands in
    // for the step because a real step's hold is too short to catch from outside.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    let verdict = verdict(&engine, |e| {
        let outcome = e.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: "late".into(), value: vec![1; 8] },
        });
        assert!(outcome.status.ok);
    });
    assert_eq!(Verdict::QueuedBehindTheGuard, verdict);
}

#[test]
fn a_step_that_skips_the_lock_is_seen_to_skip_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    // Touches the block store only, never `shards`: the shape of a step that reached shard-owned
    // data around the lock.
    let verdict = verdict(&engine, |e| {
        let _ = e.block_store().slab_ids();
    });
    assert_eq!(Verdict::RanAroundTheGuard, verdict);
}
