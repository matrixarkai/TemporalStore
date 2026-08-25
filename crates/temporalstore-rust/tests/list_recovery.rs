// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A list's order must survive a restart.
//!
//! Elements are pages named by a sortable sequence component in the bucket index, so recovery
//! rebuilds the in-memory list purely from the index -- including elements pushed on the LEFT,
//! whose sequences are negative. This drives pushes on both ends, reopens the engine from disk,
//! and requires the exact order back; then proves pops persist too (a popped element must not
//! resurrect on the next restart).

use std::path::PathBuf;

use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, TemporalEngine};

const SHARD_ID: u64 = 1;
const CACHE_BYTES: usize = 4096;

fn unique_root(name: &str) -> PathBuf {
    let pid = std::process::id();
    let mut root = std::env::temp_dir();
    root.push(format!("ts-list-recovery-{name}-{pid}"));
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
    engine.load_shard(SHARD_ID);
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
