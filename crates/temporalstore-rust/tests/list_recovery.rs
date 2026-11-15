// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A list's order must survive a restart.
//!
//! Elements are blocks named by a sortable sequence element name in the bucket index, so recovery
//! rebuilds the in-memory list purely from the index -- including elements pushed on the LEFT,
//! whose sequences are negative. This drives pushes on both ends, reopens the engine from disk,
//! and requires the exact order back; then proves pops persist too (a popped element must not
//! resurrect on the next restart).

use std::path::PathBuf;

use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, LoadShardRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;
const CACHE_BYTES: usize = 4096;

fn unique_root(name: &str) -> PathBuf {
    let pid = std::process::id();
    let mut root = std::env::temp_dir();
    root.push(format!("ts-list-recovery-{name}-{pid}"));
    root
}

fn new_engine(root: &PathBuf) -> TemporalEngine {
    for sub in ["cache", "blocks", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        CACHE_BYTES,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    );
    // `load_shard` builds this request and then drops the answer on the floor:
    //
    //     let _ = self.load_shard_with(request);
    //
    // So a load that fails is invisible until the first command comes back shard_not_loaded, and
    // that status says only that the shard is absent, never why. Both of these tests fail exactly
    // that way on main. Asking through load_shard_with makes the load report for itself.
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
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command,
    });
    assert!(response.status.ok, "command failed: {:?}", response.status);
    response.response
}

fn push(engine: &TemporalEngine, member: &str, left: bool) {
    run(
        engine,
        Command::ListPush {
            key: "jobs".to_string(),
            member: member.as_bytes().to_vec(),
            left,
        },
    );
}

fn range(engine: &TemporalEngine) -> Vec<String> {
    match run(
        engine,
        Command::ListRange {
            key: "jobs".to_string(),
            start: 0,
            stop: -1,
        },
    ) {
        CommandResponse::Members { members } => members
            .into_iter()
            .map(|member| String::from_utf8(member).expect("utf8"))
            .collect(),
        other => panic!("unexpected response: {other:?}"),
    }
}

#[test]
fn list_order_survives_restart_and_pops_stay_popped() {
    let root = unique_root("order");
    let _ = std::fs::remove_dir_all(&root);

    {
        let engine = new_engine(&root);
        push(&engine, "c", false);
        push(&engine, "b", true);
        push(&engine, "d", false);
        push(&engine, "a", true);
        assert_eq!(vec!["a", "b", "c", "d"], range(&engine));
    }

    {
        let engine = new_engine(&root);
        assert_eq!(
            vec!["a", "b", "c", "d"],
            range(&engine),
            "order must come back from the index alone, negative sequences included"
        );
        match run(
            &engine,
            Command::ListPop {
                key: "jobs".to_string(),
                left: true,
            },
        ) {
            CommandResponse::Bytes { value } => assert_eq!(Some(b"a".to_vec()), value),
            other => panic!("unexpected response: {other:?}"),
        }
    }

    {
        let engine = new_engine(&root);
        assert_eq!(
            vec!["b", "c", "d"],
            range(&engine),
            "a popped element must not resurrect on restart"
        );
        match run(
            &engine,
            Command::ListLen {
                key: "jobs".to_string(),
            },
        ) {
            CommandResponse::Integer { value } => assert_eq!(3, value),
            other => panic!("unexpected response: {other:?}"),
        }
    }

    let _ = std::fs::remove_dir_all(&root);
}

fn run_i(engine: &TemporalEngine, command: Command) -> i64 {
    match run(engine, command) {
        CommandResponse::Integer { value } => value,
        other => panic!("unexpected response: {other:?}"),
    }
}

/// LSET, LREM and LTRIM write only blocks and tombstones the list already knows (an overwrite is a
/// later block under the same element name; a removal is the pop tombstone), so a restart must
/// give back exactly the post-mutation list -- overwritten values not reverting, removed and
/// trimmed elements not resurrecting, and the list staying deleted once trimmed empty.
#[test]
fn list_mutators_survive_restart() {
    let root = unique_root("mutators");
    let _ = std::fs::remove_dir_all(&root);
    let key = || "jobs".to_string();
    {
        let engine = new_engine(&root);
        for member in ["c", "x", "d", "x", "e", "f"] {
            push(&engine, member, false);
        }
        push(&engine, "b", true);
        push(&engine, "a", true);
        assert_eq!(vec!["a", "b", "c", "x", "d", "x", "e", "f"], range(&engine));
        assert_eq!(1, run_i(&engine, Command::ListSet { key: key(), index: 0, member: b"A".to_vec() }));
        assert_eq!(1, run_i(&engine, Command::ListSet { key: key(), index: -1, member: b"F".to_vec() }));
        assert_eq!(0, run_i(&engine, Command::ListSet { key: key(), index: 8, member: b"no".to_vec() }));
        assert_eq!(0, run_i(&engine, Command::ListSet { key: "missing".into(), index: 0, member: b"no".to_vec() }));
        // one x from the tail goes, the head one stays
        assert_eq!(1, run_i(&engine, Command::ListRemove { key: key(), count: -1, member: b"x".to_vec() }));
        assert_eq!(vec!["A", "b", "c", "x", "d", "e", "F"], range(&engine));
        assert_eq!(1, run_i(&engine, Command::ListTrim { key: key(), start: 0, stop: -2 }));
        assert_eq!(vec!["A", "b", "c", "x", "d", "e"], range(&engine));
    }
    {
        let engine = new_engine(&root);
        assert_eq!(
            vec!["A", "b", "c", "x", "d", "e"],
            range(&engine),
            "overwrites, removals and trims must replay"
        );
        assert_eq!(1, run_i(&engine, Command::ListRemove { key: key(), count: 0, member: b"x".to_vec() }));
        assert_eq!(3, run_i(&engine, Command::ListTrim { key: key(), start: 1, stop: 2 }));
        assert_eq!(vec!["b", "c"], range(&engine));
    }
    {
        let engine = new_engine(&root);
        assert_eq!(vec!["b", "c"], range(&engine), "second restart agrees");
        assert_eq!(2, run_i(&engine, Command::ListTrim { key: key(), start: 5, stop: 9 }));
        assert_eq!(0, run_i(&engine, Command::ListLen { key: key() }));
    }
    {
        let engine = new_engine(&root);
        assert_eq!(0, run_i(&engine, Command::ListLen { key: key() }), "a list trimmed empty stays gone");
        assert!(range(&engine).is_empty());
        push(&engine, "n", true);
        assert_eq!(vec!["n"], range(&engine));
    }
    let _ = std::fs::remove_dir_all(&root);
}
