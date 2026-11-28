// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Maintenance and commands serialize on the shard they both touch. The fixture
//! holds one shard and observes same-shard waiting and unrelated-shard progress.
//! The lock-free step is a negative control for the observation harness.

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
    engine.load_shard(2);
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

/// Run the step on a worker while the test thread holds shard 1.
fn verdict(engine: &Arc<TemporalEngine>, step: impl FnOnce(&TemporalEngine) + Send + 'static) -> Verdict {
    let (sender, receiver) = mpsc::channel();
    let (worker, finished_while_held) = engine.shards.with_state_mut(1, |state| {
        assert!(state.is_some(), "the fixture must hold a loaded shard");
        let engine = Arc::clone(engine);
        let worker = std::thread::spawn(move || {
            step(&engine);
            let _ = sender.send(());
        });
        (worker, receiver.recv_timeout(SETTLE).is_ok())
    });
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
    assert!(escaped.is_empty(), "steps not serialised by the shard state lock: {escaped:?}");
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

#[test]
fn a_command_on_an_unrelated_shard_runs_while_shard_one_is_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    seed(&engine);
    let verdict = verdict(&engine, |e| {
        let outcome = e.execute(ExecuteRequest {
            shard_id: 2,
            command: Command::StringSet { key: "peer".into(), value: vec![2; 8] },
        });
        assert!(outcome.status.ok);
    });
    assert_eq!(Verdict::RanAroundTheGuard, verdict);
}
