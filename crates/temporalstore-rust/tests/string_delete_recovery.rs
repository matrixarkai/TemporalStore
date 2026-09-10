// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Does a deleted string survive a restart?
//!
//! `StringDelete` stages `stage_meta_outcome(.., "string", .., deleted: true)`, which carries no
//! address, and the `"string"` arm of `apply_outcome_item` opens by demanding one:
//!
//!     "string" => { let Some(address) = item.resolved_address() else { return false }; ..
//!
//! That is the same shape as the four kinds behind `wal_replay_outcome_refused` -- `hash`,
//! `list`, `set` and `zset` -- where a typed removal cannot be installed and the whole shard load
//! is refused. Nothing exercised it for strings: `StringDelete` appears only in lib tests, and no
//! test both deletes a string and reloads.
//!
//! **It fails**, and `StringDelete` is a fifth kind:
//!
//!     wal_replay_outcome_refused: WAL replay could not install a recorded string outcome at
//!     sequence 3 ... (address UNRESOLVED, component missing)
//!
//! A FIRST VERSION OF THIS TEST PASSED, and it was wrong. It called `unload_shard` before
//! reopening, which flushes the index, so the reopened engine read a base that already had the
//! delete and never replayed the tail -- the test passed without touching the path it is about.
//! `list_recovery` and `zset_recovery` drop the engine instead, which is why they reach it. That
//! one line was the difference between "the string path is fine" and "a string delete makes a
//! shard refuse to load".

use std::path::PathBuf;

use temporalstore_rust::engine::reports::StorageManagerCycleRequest;
use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, LoadShardRequest, TemporalEngine};

const SHARD_ID: u64 = 1;
const CACHE_BYTES: usize = 4096;

fn unique_root(name: &str) -> PathBuf {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-string-delete-recovery-{name}-{}", std::process::id()));
    root
}

fn new_engine(root: &PathBuf) -> TemporalEngine {
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        CACHE_BYTES,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    let loaded = engine.load_shard_with(LoadShardRequest {
        shard_id: SHARD_ID,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    assert!(loaded.status.ok, "the shard did not load: {:?}", loaded.status);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest { shard_id: SHARD_ID, command });
    assert!(response.status.ok, "command failed: {:?}", response.status);
    response.response
}

fn get(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    match run(engine, Command::StringGet { key: key.to_string() }) {
        CommandResponse::Bytes { value } => value,
        other => panic!("unexpected response: {other:?}"),
    }
}

#[test]
fn a_deleted_string_stays_deleted_across_a_restart() {
    let root = unique_root("basic");
    let _ = std::fs::remove_dir_all(&root);
    {
        let engine = new_engine(&root);
        run(&engine, Command::StringSet { key: "alpha".to_string(), value: b"v1".to_vec() });
        run(&engine, Command::StringSet { key: "bravo".to_string(), value: b"v2".to_vec() });
        run(&engine, Command::StringDelete { key: "alpha".to_string() });
        assert_eq!(None, get(&engine, "alpha"));
        assert_eq!(Some(b"v2".to_vec()), get(&engine, "bravo"));
        // Dropped, NOT unloaded -- exactly as list_recovery and zset_recovery do it. `unload_shard`
        // flushes the index, so the reopened engine reads a base that already has the delete and
        // never replays the tail: the test then passes without touching the path it is about.
    }

    // The load is where this fails if the removal outcome cannot be installed: `new_engine`
    // asserts the load status, so a refusal shows up here and not two steps later.
    let engine = new_engine(&root);
    assert_eq!(None, get(&engine, "alpha"), "the delete did not survive the restart");
    assert_eq!(
        Some(b"v2".to_vec()),
        get(&engine, "bravo"),
        "the untouched key did not survive the restart",
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// And does it stay deleted once a maintenance round has run over it?
///
/// The test above restarts straight after the delete, so the tombstone is still in the bucket
/// index when the shard reloads. A shipped server does not sit still: the storage manager runs a
/// round every 30 seconds, and a round dumps the shard, clears the dumped buckets' dirty state
/// and reclaims the WAL beneath the dump anchor. Measured, that round takes the tombstones out
/// of `deleted_object_index` -- 60 deleted objects, and after one cycle the durable tombstone
/// count is zero.
///
/// That is the right thing to do only if the dump that replaced them is durable and the WAL
/// records the tombstone was protecting are gone with it. If the tombstone is dropped while a
/// SET for the same key still sits in a replayable part of the log, the restart resurrects a
/// deleted object -- and every test that restarts BEFORE a maintenance round would still pass.
///
/// This is the sequence a shipped data node actually performs, since the maintenance cycle
/// became reachable by default.
#[test]
fn a_deleted_string_stays_deleted_across_a_restart_after_a_maintenance_round() {
    let root = unique_root("maintenance");
    let _ = std::fs::remove_dir_all(&root);
    {
        let engine = new_engine(&root);
        for index in 0..40 {
            run(
                &engine,
                Command::StringSet {
                    key: format!("key-{index:03}"),
                    value: format!("v{index}").into_bytes(),
                },
            );
        }
        for index in 0..20 {
            run(&engine, Command::StringDelete { key: format!("key-{index:03}") });
        }
        // A full round: dump, clear the dumped buckets, reclaim the log beneath the anchor.
        // Thresholds at zero so it fires here rather than waiting for pressure to build.
        engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: SHARD_ID,
            min_undumped_wal_records: 0,
            min_undumped_wal_bytes: 0,
            ..StorageManagerCycleRequest::default()
        });
        for index in 0..20 {
            assert_eq!(
                None,
                get(&engine, &format!("key-{index:03}")),
                "key-{index:03} came back while the engine was still up",
            );
        }
        // Dropped, not unloaded: an unload flushes the index, and the reopened engine would then
        // read a base that already holds the delete without replaying the tail.
    }

    let engine = new_engine(&root);
    for index in 0..20 {
        assert_eq!(
            None,
            get(&engine, &format!("key-{index:03}")),
            "key-{index:03} was resurrected by the restart after a maintenance round",
        );
    }
    for index in 20..40 {
        assert_eq!(
            Some(format!("v{index}").into_bytes()),
            get(&engine, &format!("key-{index:03}")),
            "key-{index:03} was never deleted and must survive",
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}
