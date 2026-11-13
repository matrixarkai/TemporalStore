// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE WAL-RECLAIM STAGE WAITS FOR THE UNDUMPED LOG TO REACH THE BYTE GATE, AND A CAPPED DUMP DOES
//! NOT RE-ARM IT.
//!
//! The shipped gate is bytes alone. The stage used to run for another reason as well --
//! `dirty_bucket_pressure` shipped at one, so a single dirty bucket made every round run the dump
//! decision, two lifecycle plans and a reclaim, each a walk of every live block. A log of a few
//! kilobytes is nowhere near 128 MiB, and nothing in the round needed to look at it.
//!
//! The second half is what keeps that honest: the log's own undumped counter is zeroed by ANY dump
//! manifest, so a dump capped at a few buckets would re-arm the whole gate and leave the rest of
//! the dirty set waiting another threshold of log. The gate reads the log above the oldest
//! undumped write instead.
#![allow(clippy::all)]
use super::*;

fn seeded(dir: &std::path::Path, keys: usize) -> DataNodeRuntime {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "byte-gate".to_string(),
        shard_uri: "local://byte-gate/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load shard 1: {:?}", response.status);
    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("gate-{index:06}"),
                value: vec![b'v'; 96],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 64,
            max_background_queue_depth: 8,
        },
    )
}

fn ran_wal_stage(report: &StorageManagerLoopReport) -> bool {
    report.executed_stages.iter().any(|stage| stage == "reclaim_wal")
}

#[test]
fn a_dirty_shard_below_the_byte_gate_does_not_run_the_wal_reclaim_stage() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path(), 200);
    let shipped = StorageManagerOptions::default();
    let (pressure, _) = runtime.storage_manager_pressure_snapshot(1, &shipped);
    // NON-VACUITY: the shard is dirty and its log is a real, nonzero size below the gate.
    assert!(pressure.dirty_bucket_count > 0, "the fixture left nothing dirty");
    assert!(
        pressure.wal_bytes > 0 && pressure.wal_bytes < shipped.min_undumped_wal_bytes,
        "wal_bytes {} is not a byte figure below the gate",
        pressure.wal_bytes
    );
    assert_ne!(
        pressure.wal_bytes, pressure.undumped_wal_records,
        "wal_bytes is the record count again"
    );
    let report = runtime.run_storage_manager_once(1, shipped);
    assert!(
        !ran_wal_stage(&report),
        "the stage ran on a dirty shard with {} undumped bytes against a {}-byte gate",
        pressure.wal_bytes,
        StorageManagerOptions::default().min_undumped_wal_bytes
    );
    let walked = report
        .stage_spans
        .iter()
        .find(|span| span.stage == "reclaim_wal")
        .expect("every round reports the stage")
        .walk
        .live_block_entries;
    assert_eq!(walked, 0, "a stage that did not run walked the shard");
}

/// The control: the same shard with the old trigger switched back on does run the stage, so the
/// test above is reading the trigger and not a stage that never runs.
#[test]
fn the_dirty_bucket_trigger_still_runs_the_stage_when_an_operator_sets_it() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path(), 200);
    let options = StorageManagerOptions {
        dirty_bucket_pressure: 1,
        ..StorageManagerOptions::default()
    };
    let report = runtime.run_storage_manager_once(1, options);
    assert!(ran_wal_stage(&report), "stages: {:?}", report.executed_stages);
}

#[test]
fn a_capped_dump_does_not_rearm_the_byte_gate() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path(), 400);
    let options = StorageManagerOptions {
        min_undumped_wal_bytes: 20_000,
        max_dump_buckets_per_round: 10,
        // Only this stage may dump: the index reclaim and memory reclaim stages take dumps of
        // their own, which would drain the dirty set and leave nothing for the cap to leave behind.
        enable_index_gc: false,
        enable_memory_reclaim: false,
        enable_block_compaction: false,
        enable_block_gc: false,
        enable_expire: false,
        ..StorageManagerOptions::default()
    };
    let (before, _) = runtime.storage_manager_pressure_snapshot(1, &options);
    assert!(
        before.wal_bytes >= options.min_undumped_wal_bytes,
        "the fixture's log {} is under the gate",
        before.wal_bytes
    );
    let first = runtime.run_storage_manager_once(1, options.clone());
    assert!(ran_wal_stage(&first), "the gate was crossed and the stage did not run");
    let (after, _) = runtime.storage_manager_pressure_snapshot(1, &options);
    // NON-VACUITY: ten buckets were dumped and more than ten were dirty, so some remain.
    assert!(
        after.dirty_bucket_count > 0,
        "the first round dumped every dirty bucket, so the cap was never the limit"
    );
    let second = runtime.run_storage_manager_once(1, options.clone());
    assert!(
        ran_wal_stage(&second),
        "{} dirty buckets remained after a capped dump and the byte gate stayed shut \
         (bytes now {})",
        after.dirty_bucket_count,
        after.wal_bytes
    );
}
