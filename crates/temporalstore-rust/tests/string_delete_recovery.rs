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
//! **It passes.** Reading the arms predicted a fifth failing kind and the prediction was wrong,
//! which is why this file exists as a test rather than as a sentence in a report. Whatever saves
//! the string path does not save `list` and `zset`, whose equivalents fail on main today, and the
//! difference is worth knowing to whoever fixes those: something here already does the right
//! thing.
//!
//! Kept because the coverage was genuinely missing, not to prove a point. A string that is
//! deleted and then reloaded is an ordinary thing to want, and nothing checked it.

use std::path::PathBuf;

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
        engine.unload_shard(SHARD_ID);
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
