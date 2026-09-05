// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests for phase-1 flat append (`LocalWriteAheadLogStore::flat_append`): the two per-write
//! O(store) costs that
//! run under the engine `shards` write lock -- the WAL append's full-file `last_wal_sequence_at`
//! rescan and the per-execute `promote_model_maps_to_bucket_index_authority` reconcile scan -- are
//! made O(1) so phase-1 stops aging O(n) with data size. These tests prove:
//!   (a) AGING: with the gate ON, neither scan grows with the write count (both counters stay a
//!       small constant over 5k writes), while with the gate OFF each fires once per command
//!       (O(writes)) -- and every write is still byte-exact;
//!   (b) DURABILITY/RECOVERY: with the gate ON, dropping the engine and reloading over the same
//!       dirs (WAL replay) recovers every acked write byte-exact (the fast-append length cache is
//!       cold on the fresh process, so the first append reconciles via the full scan);
//!   (c) COALESCING: with the gate ON plus the concurrent-commit path, group commit still engages
//!       (fewer fdatasyncs than writes) and all writes are correct.
#![allow(clippy::all)]
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

// The gates are process-global env vars; serialize the sub-tests that toggle them so an OFF-baseline
// measurement never observes a sibling's ON window (and vice versa). Other tests are unaffected: the
// gate only alters an internal fast path and the per-engine/per-store counters read here are
// isolated to the engine under test.

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

fn write_n_strings(engine: &TemporalEngine, n: usize) {
    for i in 0..n {
        let resp = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k{i}"),
                value: format!("v{i}").into_bytes(),
            },
        });
        assert!(resp.status.ok, "write k{i} must ack ok: {:?}", resp.status);
    }
}

fn assert_present_sampled(engine: &TemporalEngine, n: usize, step: usize) {
    let mut i = 0;
    while i < n {
        let get = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: format!("k{i}") },
        });
        match get.response {
            CommandResponse::Bytes { value: Some(value) } => {
                assert_eq!(value, format!("v{i}").into_bytes(), "wrong value for k{i}")
            }
            other => panic!("lost write k{i}: {other:?}"),
        }
        i += step;
    }
}

/// DIAGNOSTIC (ignored by default): time per-1000-write cost under the gate to see whether phase-1
/// is flat. Run with `cargo test ... phase1_flat_diag_timing -- --ignored --nocapture`.
#[test]
#[ignore]
fn phase1_flat_diag_timing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = new_engine(dir.path());
    let batch = 500usize;
    let batches = 8usize;
    let mut done = 0usize;
    for b in 0..batches {
        let t0 = std::time::Instant::now();
        for i in 0..batch {
            let k = done + i;
            let resp = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key: format!("k{k}"), value: b"v".to_vec() },
            });
            assert!(resp.status.ok);
        }
        done += batch;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        let raw = engine.write_ahead_log_store().raw_stats(1);
        let prom = engine.promote_scan_count();
        eprintln!(
            "[diag] batch {b} total_writes={done} batch_ms={ms:.1} per_write_ms={:.3} wal_scans={} stats_scans={} promote_scans={prom}",
            ms / batch as f64,
            raw.append_full_scans,
            raw.stats_full_scans
        );
    }
}

/// AGING: with flat append ON, the WAL append rescan count and the promote reconcile scan count
/// both stay a SMALL CONSTANT over 5k writes -- phase-1 is O(1) per write, not O(n). The OFF
/// baseline fires each scan once per command, so the counts track the write volume (O(writes)).
#[test]
fn phase1_flat_keeps_per_write_scans_flat_over_5k_writes() {

    // --- Gate ON: 5k writes, scans must stay flat (a small constant, not ~5000). ---
    let on_dir = tempfile::tempdir().unwrap();
    let on = new_engine(on_dir.path());
    const N: usize = 5000;
    write_n_strings(&on, N);
    // `raw_stats` reads the counters WITHOUT itself triggering a `stats()` scan.
    let on_raw = on.write_ahead_log_store().raw_stats(1);
    let on_wal_scans = on_raw.append_full_scans;
    let on_stats_scans = on_raw.stats_full_scans;
    let on_promote_scans = on.promote_scan_count();
    eprintln!(
        "[phase1_flat] gate ON  writes={N} wal_append_full_scans={on_wal_scans} stats_full_scans={on_stats_scans} promote_scans={on_promote_scans}"
    );
    // Read back a sample WHILE the gate is still on (so the reads also skip the O(store) promote
    // scan -- reading all N with the gate off would itself be O(N^2) and is not what we measure).
    assert_present_sampled(&on, N, 137);
    // A tiny constant covers the first warm-up append + the first reconcile (+ any load-time
    // reconcile). The point is NONE of the three per-write scans scale with N.
    assert!(
        on_wal_scans <= 8,
        "gate ON: WAL append rescans must stay O(1) over {N} writes, got {on_wal_scans}"
    );
    assert!(
        on_stats_scans <= 8,
        "gate ON: per-write index-anchor stats() rescans must stay O(1) over {N} writes, got {on_stats_scans}"
    );
    assert!(
        on_promote_scans <= 8,
        "gate ON: promote reconcile scans must stay O(1) over {N} writes, got {on_promote_scans}"
    );

    // --- Gate OFF baseline: each scan fires once per command -> counts track the write volume. ---
    let off_dir = tempfile::tempdir().unwrap();
    let off = new_engine(off_dir.path());
    off.write_ahead_log_store().rescan_on_every_append_for_test();
    // Smaller than N: with the gate off each of these writes pays the O(store) promote scan AND a
    // full-file WAL rescan, so the run is intentionally O(M^2) -- kept modest so the baseline is
    // quick while still proving the per-command scans track the write volume.
    const M: usize = 800;
    write_n_strings(&off, M);
    let off_raw = off.write_ahead_log_store().raw_stats(1);
    let off_wal_scans = off_raw.append_full_scans;
    let off_stats_scans = off_raw.stats_full_scans;
    let off_promote_scans = off.promote_scan_count();
    eprintln!(
        "[phase1_flat] gate OFF writes={M} wal_append_full_scans={off_wal_scans} stats_full_scans={off_stats_scans} promote_scans={off_promote_scans}"
    );
    assert_present_sampled(&off, M, 79);
    // Off, every append rescans, every anchor stats()-rescans, and every execute reconcile-scans
    // -> at least one of each per write.
    assert!(
        off_wal_scans >= M as u64,
        "gate OFF: WAL append should rescan once per append (>= {M}), got {off_wal_scans}"
    );
    assert!(
        off_stats_scans >= M as u64,
        "gate OFF: index-anchor should stats()-rescan once per write (>= {M}), got {off_stats_scans}"
    );
    assert!(
        off_promote_scans >= M as u64,
        "gate OFF: promote should scan once per execute (>= {M}), got {off_promote_scans}"
    );
    // The whole point: ON is dramatically fewer scans than OFF for MORE writes.
    assert!(
        on_wal_scans * 100 < off_wal_scans,
        "gate ON WAL rescans ({on_wal_scans}) must be far below OFF ({off_wal_scans})"
    );
    assert!(
        on_stats_scans * 100 < off_stats_scans,
        "gate ON anchor stats-scans ({on_stats_scans}) must be far below OFF ({off_stats_scans})"
    );
    assert!(
        on_promote_scans * 100 < off_promote_scans,
        "gate ON promote scans ({on_promote_scans}) must be far below OFF ({off_promote_scans})"
    );
}

/// DURABILITY + RECOVERY with the gate ON: write N, drop the engine (nothing flushed beyond the
/// WAL), reopen over the SAME dirs so the shard rebuilds by WAL replay. Every acked write must be
/// recovered byte-exact -- proving the fast-append cache never skipped a durable barrier and the
/// cold reload's first append correctly reconciles against the on-disk WAL via the full scan.
#[test]
fn phase1_flat_writes_survive_wal_replay_reload() {

    let dir = tempfile::tempdir().unwrap();
    const N: usize = 400;
    {
        let engine = new_engine(dir.path());
        write_n_strings(&engine, N);
        // engine dropped here -> recovery must rebuild purely from the WAL.
    }
    let recovered = new_engine(dir.path());
    // Read every recovered key while the gate is still on (reads skip the promote scan).
    assert_present_sampled(&recovered, N, 1);

    // And a fresh append after the cold reload must continue the sequence correctly (the reload's
    // first append reconciles via the full scan, then the fast path takes over).
    let resp = recovered.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "post-reload".into(),
            value: b"ok".to_vec(),
        },
    });
    assert!(resp.status.ok, "post-reload write must ack: {:?}", resp.status);
    let get = recovered.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet { key: "post-reload".into() },
    });
    assert!(
        matches!(get.response, CommandResponse::Bytes { value: Some(ref v) } if v == b"ok"),
        "post-reload readback failed: {:?}",
        get.response
    );
}

/// COALESCING: with the gate ON plus the concurrent-commit path, group commit still engages
/// (fewer fdatasyncs than writes) and every acked write is byte-exact. In this in-process unit
/// harness phase-1 is already cheap, so this confirms the phase-1 fix does not REGRESS coalescing;
/// the live proof that flat phase-1 lets many writers pile into one fsync window needs the live
/// re-test on the standalone shared backend.
#[test]
fn phase1_flat_composes_with_concurrent_commit_coalescing() {

    const WRITERS: usize = 8;
    const PER_WRITER: usize = 25;
    let total = WRITERS * PER_WRITER;
    let dir = tempfile::tempdir().unwrap();
    let engine = new_engine(dir.path());
    let syncs_before = engine.write_ahead_log_store().stats(1).syncs;
    let acked = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Barrier::new(WRITERS));
    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let engine = Arc::clone(&engine);
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
                assert!(resp.status.ok, "write w{w}-k{i} must ack: {:?}", resp.status);
                acked.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("writer thread must not panic");
    }
    let fsyncs = engine.write_ahead_log_store().stats(1).syncs - syncs_before;
    eprintln!(
        "[phase1_flat] concurrent writers={WRITERS} writes={total} fdatasyncs={fsyncs} ratio={:.3} fsync/write",
        fsyncs as f64 / total as f64
    );
    assert_eq!(acked.load(Ordering::Relaxed), total, "all writes acked");
    assert!(
        fsyncs < total as u64,
        "group commit must coalesce fsyncs below the write count (writes={total}, fsyncs={fsyncs})"
    );
    for w in 0..WRITERS {
        for i in 0..PER_WRITER {
            let get = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: format!("w{w}-k{i}") },
            });
            match get.response {
                CommandResponse::Bytes { value: Some(value) } => {
                    assert_eq!(value, format!("v{w}-{i}").into_bytes(), "wrong value w{w}-k{i}")
                }
                other => panic!("lost acked write w{w}-k{i}: {other:?}"),
            }
        }
    }
}
