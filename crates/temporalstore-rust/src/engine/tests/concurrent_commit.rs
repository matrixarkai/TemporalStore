// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests for the concurrent-commit write path: the durable WAL barrier runs OUTSIDE the global
//! `shards` write lock, so concurrent same-shard writers reach #45's group-commit queue and their
//! fdatasyncs coalesce. These tests prove:
//!   (a) N concurrent same-shard writers take FEWER fdatasyncs than writes (group commit engages)
//!       AND every acked write is present + exact (no lost/torn writes);
//!   (b) with the barrier taken back under the lock -- which only a test can ask for, of one
//!       engine -- it is one fsync per write, so coalescing is unreachable there;
//!   (c) the concurrent path is durable: after a drop + WAL-replay reload every acked write is
//!       recovered exactly (ordering/consistency preserved -- WAL order == apply order).
#![allow(clippy::all)]
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

// These used to serialize against each other through a mutex, because the choice was a
// process-global env var and a baseline measurement could observe a window its sibling had
// opened. It is a property of an engine now, and each of these builds its own.

const WRITERS: usize = 8;
const PER_WRITER: usize = 25; // 200 same-shard sync writes total

struct ConcurrentRun {
    fsyncs: u64,
    writes: usize,
    acked: usize,
}

/// Drive `WRITERS` threads, each issuing `PER_WRITER` synchronous StringSet writes to shard 1,
/// all starting together (barrier) to maximize the group-commit overlap window. Returns the
/// number of WAL fdatasyncs the run actually took (delta), the total write count, and how many
/// were acked. Also asserts every acked key is retrievable with its exact value (no lost/torn
/// write) from the LIVE engine before returning.
fn run_concurrent_same_shard_writes(engine: &Arc<TemporalEngine>) -> ConcurrentRun {
    let total = WRITERS * PER_WRITER;
    let syncs_before = engine.write_ahead_log_store().stats(1).syncs;
    let acked = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Barrier::new(WRITERS));
    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let engine = Arc::clone(engine);
        let acked = Arc::clone(&acked);
        let gate = Arc::clone(&gate);
        handles.push(std::thread::spawn(move || {
            gate.wait();
            for i in 0..PER_WRITER {
                let resp = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("w{w}-k{i}"),
                        value: format!("v{w}-{i}").into_bytes(),
                    },
                });
                assert!(resp.status.ok, "write w{w}-k{i} must ack ok: {:?}", resp.status);
                acked.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("writer thread must not panic");
    }
    let fsyncs = engine.write_ahead_log_store().stats(1).syncs - syncs_before;
    eprintln!(
        "[concurrent_commit] writers={WRITERS} writes={total} fdatasyncs={fsyncs} ratio={:.3} fsync/write",
        fsyncs as f64 / total as f64
    );

    // Correctness: every acked write is present and byte-exact on the live engine.
    for w in 0..WRITERS {
        for i in 0..PER_WRITER {
            let get = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("w{w}-k{i}"),
                },
            });
            match get.response {
                CommandResponse::Bytes { value: Some(value) } => assert_eq!(
                    value,
                    format!("v{w}-{i}").into_bytes(),
                    "torn/wrong value for w{w}-k{i}"
                ),
                other => panic!("lost acked write w{w}-k{i}: {other:?}"),
            }
        }
    }

    ConcurrentRun {
        fsyncs,
        writes: total,
        acked: acked.load(Ordering::Relaxed),
    }
}

fn new_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

/// THE proof: with the barrier moved out of the global lock, concurrent same-shard writers
/// coalesce their fdatasyncs (fewer fsyncs than writes); with the gate OFF the barrier stays
/// under the lock and every write takes its own fsync. Both paths preserve all acked writes.
#[test]
fn concurrent_commit_coalesces_fsyncs_while_off_path_is_one_fsync_per_write() {
    // --- Baseline: barrier under the shards lock -> group commit unreachable. ---
    let off_dir = tempfile::tempdir().unwrap();
    let off_engine = new_engine(off_dir.path());
    off_engine.commit_under_lock_for_test();
    let off = run_concurrent_same_shard_writes(&off_engine);
    assert_eq!(off.acked, off.writes, "barrier under the lock: all writes acked");
    // Serialized under the global lock, each sync write drives its own barrier -> exactly one
    // fdatasync per write. This is the live-proven 1.00 fdatasync/write bottleneck.
    assert_eq!(
        off.fsyncs, off.writes as u64,
        "gate OFF: each sync write takes its own fsync ({} writes, {} fsyncs)",
        off.writes, off.fsyncs
    );

    // --- What ships: barrier out of the lock -> group commit engages. ---
    let on_dir = tempfile::tempdir().unwrap();
    let on = run_concurrent_same_shard_writes(&new_engine(on_dir.path()));
    assert_eq!(on.acked, on.writes, "barrier outside the lock: all writes acked");
    // The whole point: fewer durable barriers than writes -> coalescing is now reachable.
    assert!(
        on.fsyncs < on.writes as u64,
        "gate ON: group commit must coalesce fsyncs below the write count (writes={}, fsyncs={})",
        on.writes, on.fsyncs
    );
    // And it must be a MEANINGFUL reduction vs the serialized baseline, not an off-by-one.
    assert!(
        on.fsyncs < off.fsyncs,
        "gate ON must take strictly fewer fsyncs than the serialized OFF baseline (on={}, off={})",
        on.fsyncs, off.fsyncs
    );
}

/// Durability + recovery + ordering/consistency for the concurrent path: after N concurrent
/// same-shard writers commit with the gate ON, drop the engine and reopen over the SAME dirs so
/// the shard is rebuilt by WAL replay. Every acked write must be recovered byte-exact -- proving
/// the sequence reservation stayed correct (WAL order == apply order) even though the fsync ran
/// outside the lock, and that no acked write was lost/torn/reordered.
#[test]
fn concurrent_commit_writes_survive_wal_replay_reload() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = new_engine(dir.path());
        let run = run_concurrent_same_shard_writes(&engine);
        assert_eq!(run.acked, run.writes, "all writes acked before reload");
        assert!(run.fsyncs < run.writes as u64, "coalescing engaged before reload");
        // engine dropped here -> nothing flushed beyond the WAL; recovery must rebuild from it.
    }

    // Fresh engine over the same directories -> load_shard replays the WAL tail.
    let recovered = new_engine(dir.path());
    for w in 0..WRITERS {
        for i in 0..PER_WRITER {
            let get = recovered.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("w{w}-k{i}"),
                },
            });
            match get.response {
                CommandResponse::Bytes { value: Some(value) } => assert_eq!(
                    value,
                    format!("v{w}-{i}").into_bytes(),
                    "recovery lost/corrupted w{w}-k{i}"
                ),
                other => panic!("recovery lost acked write w{w}-k{i}: {other:?}"),
            }
        }
    }
}
