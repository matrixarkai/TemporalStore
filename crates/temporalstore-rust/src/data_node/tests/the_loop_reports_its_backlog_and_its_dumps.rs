// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LOOP REPORTS ITS BACKLOG AND ITS DUMPS WHERE A SCRAPE CAN READ THEM.
//!
//! The on-demand cycle report carries the undumped-log figure and the dump counts, and a running
//! server never runs a cycle, so nothing said how close the log was to the dump gate or what a dump
//! cost. The runtime's own counters now carry both, set by the loop's rounds.
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
        table_name: "loop-metrics".to_string(),
        shard_uri: "local://loop-metrics/1".to_string(),
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
                key: format!("metric-{index:05}"),
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

#[test]
fn a_round_records_the_backlog_it_read_and_a_dump_records_what_it_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path(), 300);
    // NON-VACUITY: nothing has been read or dumped yet.
    let before = runtime.stats();
    assert_eq!(before.storage_manager_loop_wal_bytes, 0);
    assert_eq!(before.storage_manager_dump_buckets, 0);

    // A one-byte gate, so the first round dumps.
    let options = StorageManagerOptions {
        min_undumped_wal_bytes: 1,
        ..StorageManagerOptions::default()
    };
    let (pressure, _) = runtime.storage_manager_pressure_snapshot(1, &options);
    runtime.run_storage_manager_once(1, options);
    let after = runtime.stats();

    // The backlog is what the round read at its start: the figure and the dirty buckets.
    assert_eq!(after.storage_manager_loop_wal_bytes, pressure.wal_bytes);
    assert!(after.storage_manager_loop_wal_bytes > 0, "the round read no backlog");
    assert_eq!(
        after.storage_manager_loop_dirty_buckets,
        pressure.dirty_bucket_count as u64
    );
    assert!(after.storage_manager_loop_dirty_buckets > 0);
    // And the dump it took is counted: its buckets, their bytes and the time inside the call.
    assert!(after.storage_manager_dump_buckets > 0, "no dumped bucket was counted");
    assert!(after.storage_manager_dump_logical_bytes > 0, "no dumped byte was counted");
    assert!(after.storage_manager_dump_us_total > 0, "the dump call was not timed");
    assert!(after.storage_manager_dump_us_max <= after.storage_manager_dump_us_total);
}
