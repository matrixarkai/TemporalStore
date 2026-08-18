// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests for the raft state-machine apply-path fsync coalescing (`TS_RAFT_APPLY_COALESCE`).
//! On the raft apply path the raft log is the durability + reconstruction source, so a batch of
//! committed entries (an AppendEntries batch on a follower, a recovery replay, or a pipelined
//! propose group) only needs ONE engine-WAL durability barrier instead of one per entry. These
//! tests prove:
//!   (a) with the gate ON, applying a batch of N committed commands via
//!       `execute_raft_apply_batch` takes exactly ONE fdatasync (coalesced) while every value is
//!       present + byte-exact on the live engine;
//!   (b) with the gate OFF, the same batch degrades to per-entry `execute_raft_apply` -> exactly
//!       N fdatasyncs (byte-identical legacy behavior);
//!   (c) the coalesced batch is durable: after a drop + WAL-replay reload every applied command is
//!       recovered byte-exact -- the coalesced barrier made the whole batch durable before the
//!       apply returned, so nothing acked-and-applied is lost.
#![allow(clippy::all)]
use super::*;
use std::sync::Mutex;

// The gate is a process-global env var; serialize the sub-tests that toggle it so an OFF baseline
// never observes an ON window from a sibling (and vice versa).
static RAFT_APPLY_ENV_LOCK: Mutex<()> = Mutex::new(());

const BATCH: usize = 64;

fn new_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn batch_requests() -> Vec<ExecuteRequest> {
    (0..BATCH)
        .map(|i| ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: format!("v{i}").into_bytes(),
            },
        })
        .collect()
}

fn assert_all_present(engine: &TemporalEngine) {
    for i in 0..BATCH {
        let get = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: format!("k{i}") },
        });
        match get.response {
            CommandResponse::Bytes { value: Some(value) } => {
                assert_eq!(value, format!("v{i}").into_bytes(), "wrong value for k{i}")
            }
            other => panic!("missing applied command k{i}: {other:?}"),
        }
    }
}

/// THE proof: a committed apply batch coalesces to ONE fdatasync with the gate ON, and pays one
/// fdatasync per entry with the gate OFF -- both applying every command correctly.
#[test]
fn raft_apply_batch_coalesces_to_one_fsync_while_off_is_one_per_entry() {
    let _serial = RAFT_APPLY_ENV_LOCK.lock().unwrap();

    // --- Baseline: gate OFF -> per-entry execute_raft_apply -> one fsync per committed entry. ---
    std::env::remove_var("TS_RAFT_APPLY_COALESCE");
    let off_dir = tempfile::tempdir().unwrap();
    let off = new_engine(off_dir.path());
    let before = off.write_ahead_log_store().stats(1).syncs;
    let off_responses = off.execute_raft_apply_batch(batch_requests());
    let off_fsyncs = off.write_ahead_log_store().stats(1).syncs - before;
    assert!(off_responses.iter().all(|r| r.status.ok), "gate OFF: all apply ok");
    assert_all_present(&off);
    assert_eq!(
        off_fsyncs, BATCH as u64,
        "gate OFF: each committed entry takes its own fsync ({BATCH} entries, {off_fsyncs} fsyncs)"
    );

    // --- Fix: gate ON -> one coalesced barrier for the whole batch. ---
    std::env::set_var("TS_RAFT_APPLY_COALESCE", "1");
    let on_dir = tempfile::tempdir().unwrap();
    let on = new_engine(on_dir.path());
    let before = on.write_ahead_log_store().stats(1).syncs;
    let on_responses = on.execute_raft_apply_batch(batch_requests());
    let on_fsyncs = on.write_ahead_log_store().stats(1).syncs - before;
    std::env::remove_var("TS_RAFT_APPLY_COALESCE");
    eprintln!(
        "[raft_apply_coalesce] batch={BATCH} off_fsyncs={off_fsyncs} on_fsyncs={on_fsyncs}"
    );
    assert!(on_responses.iter().all(|r| r.status.ok), "gate ON: all apply ok");
    assert_all_present(&on);
    // The whole point: the entire committed batch shares a single durable barrier.
    assert_eq!(
        on_fsyncs, 1,
        "gate ON: the whole committed batch must coalesce into ONE fdatasync (got {on_fsyncs})"
    );
}

/// Durability + recovery for the coalesced path: after applying a committed batch with the gate ON,
/// drop the engine and reopen over the SAME dirs so the shard rebuilds by WAL replay. Every applied
/// command must be recovered byte-exact -- proving the single coalesced barrier made the whole batch
/// durable before the apply returned (so `applied => engine-WAL-durable` holds).
#[test]
fn raft_apply_coalesced_batch_survives_wal_replay_reload() {
    let _serial = RAFT_APPLY_ENV_LOCK.lock().unwrap();
    std::env::set_var("TS_RAFT_APPLY_COALESCE", "1");

    let dir = tempfile::tempdir().unwrap();
    {
        let engine = new_engine(dir.path());
        let before = engine.write_ahead_log_store().stats(1).syncs;
        let responses = engine.execute_raft_apply_batch(batch_requests());
        let fsyncs = engine.write_ahead_log_store().stats(1).syncs - before;
        assert!(responses.iter().all(|r| r.status.ok), "all apply ok before reload");
        assert_eq!(fsyncs, 1, "coalesced to one barrier before reload (got {fsyncs})");
        // engine dropped here -> only the WAL is on disk; recovery must rebuild from it.
    }

    let recovered = new_engine(dir.path());
    std::env::remove_var("TS_RAFT_APPLY_COALESCE");
    assert_all_present(&recovered);
}
