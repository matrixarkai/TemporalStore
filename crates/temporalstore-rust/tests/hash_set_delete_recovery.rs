// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Do a deleted hash field and a deleted set member survive a restart?
//!
//! `hash` and `set` are two of the five kinds whose typed removal goes through
//! `mark_bucket_index_page_deleted`, which stages `address: None, deleted: true`, while their arms
//! in `apply_outcome_item` open by demanding an address. `list`, `zset` and `string` are confirmed
//! to refuse the load because of it; these two share the arm shape and had no coverage at all.
//!
//! **Both fail**, which completes the set at five:
//!
//!     hash outcome at sequence 3 ... (address UNRESOLVED, component present, 5 chars)
//!     set  outcome at sequence 3 ... (address UNRESOLVED, component present, 10 chars)
//!
//! -- the hash field `alpha`, and the same member hex-encoded for the set. So EVERY typed removal
//! refuses the load: a deleted hash field, a removed set member, a popped list element, a rescored
//! zset member, a deleted string.
//!
//! `#[ignore]` only because a failing test trips the ratchet on every later pull request. Remove
//! both attributes when the deleted-branch lands; these are its verification for two of the five
//! kinds that have no other coverage.
//!
//! Dropped, NOT unloaded. `unload_shard` flushes the index, so the reopened engine reads a base
//! that already has the delete and never replays the tail -- a test written that way passes
//! without touching the path it is about, which is how the string case was briefly and wrongly
//! declared healthy.

use std::path::PathBuf;

use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, LoadShardRequest, TemporalEngine};

const SHARD_ID: u64 = 1;
const CACHE_BYTES: usize = 4096;

fn unique_root(name: &str) -> PathBuf {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-hash-set-delete-recovery-{name}-{}", std::process::id()));
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

fn hget(engine: &TemporalEngine, key: &str, field: &str) -> Option<Vec<u8>> {
    match run(engine, Command::HashGet { key: key.to_string(), field: field.to_string() }) {
        CommandResponse::Bytes { value } => value,
        other => panic!("unexpected response: {other:?}"),
    }
}

#[test]
#[ignore = "fails on main: a hash-field delete outcome cannot be installed; see the module comment"]
fn a_deleted_hash_field_stays_deleted_across_a_restart() {
    let root = unique_root("hash");
    let _ = std::fs::remove_dir_all(&root);
    {
        let engine = new_engine(&root);
        run(&engine, Command::HashSet {
            key: "h".to_string(), field: "alpha".to_string(), value: b"v1".to_vec() });
        run(&engine, Command::HashSet {
            key: "h".to_string(), field: "bravo".to_string(), value: b"v2".to_vec() });
        run(&engine, Command::HashDelete { key: "h".to_string(), field: "alpha".to_string() });
        assert_eq!(None, hget(&engine, "h", "alpha"));
    }
    let engine = new_engine(&root);
    assert_eq!(None, hget(&engine, "h", "alpha"), "the delete did not survive the restart");
    assert_eq!(Some(b"v2".to_vec()), hget(&engine, "h", "bravo"), "the sibling field was lost");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
#[ignore = "fails on main: a set-member removal outcome cannot be installed; see the module comment"]
fn a_removed_set_member_stays_removed_across_a_restart() {
    let root = unique_root("set");
    let _ = std::fs::remove_dir_all(&root);
    {
        let engine = new_engine(&root);
        run(&engine, Command::SetAdd { key: "s".to_string(), member: b"alpha".to_vec() });
        run(&engine, Command::SetAdd { key: "s".to_string(), member: b"bravo".to_vec() });
        run(&engine, Command::SetRemove { key: "s".to_string(), member: b"alpha".to_vec() });
    }
    let engine = new_engine(&root);
    match run(&engine, Command::SetMembers { key: "s".to_string() }) {
        CommandResponse::Members { members } => {
            assert!(!members.iter().any(|v| v.as_slice() == b"alpha"), "the removal did not survive");
            assert!(members.iter().any(|v| v.as_slice() == b"bravo"), "the sibling member was lost");
        }
        other => panic!("unexpected response: {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}
