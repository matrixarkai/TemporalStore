// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A sorted set's scores and order must survive a restart -- re-scores included.
//!
//! Each member's page is named in the bucket index by a component encoding (score, member), and
//! a re-score marks the OLD component deleted before publishing the new one. Recovery rebuilds
//! the map purely from live components, so a restart must see the moved score exactly once,
//! never the stale one -- that is the failure this pins: if the delete-then-insert ever loses
//! its delete, the member comes back twice at two scores.

use std::path::PathBuf;

use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, TemporalEngine};

const SHARD_ID: u64 = 1;
const CACHE_BYTES: usize = 4096;

fn unique_root(name: &str) -> PathBuf {
    let pid = std::process::id();
    let mut root = std::env::temp_dir();
    root.push(format!("ts-zset-recovery-{name}-{pid}"));
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

fn zadd(engine: &TemporalEngine, member: &str, score: f64) {
    run(
        engine,
        Command::ZSetAdd {
            key: "board".to_string(),
            member: member.as_bytes().to_vec(),
            score,
        },
    );
}

fn full_range(engine: &TemporalEngine) -> Vec<String> {
    match run(
        engine,
        Command::ZSetRange {
            key: "board".to_string(),
            start: 0,
            stop: -1,
            rev: false,
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
fn scores_and_order_survive_restart_including_a_rescore() {
    let root = unique_root("order");
    let _ = std::fs::remove_dir_all(&root);

    {
        let engine = new_engine(&root);
        zadd(&engine, "alice", 10.0);
        zadd(&engine, "bob", 20.0);
        zadd(&engine, "carol", -3.5);
        // The re-score that must not fork: alice moves past bob.
        zadd(&engine, "alice", 30.0);
        assert_eq!(
            vec!["carol", "-3.5", "bob", "20", "alice", "30"],
            full_range(&engine)
        );
    }

    {
        let engine = new_engine(&root);
        assert_eq!(
            vec!["carol", "-3.5", "bob", "20", "alice", "30"],
            full_range(&engine),
            "recovery must see the moved score exactly once -- a duplicated member here means \
             the re-score's delete was lost"
        );
        match run(
            &engine,
            Command::ZSetCard {
                key: "board".to_string(),
            },
        ) {
            CommandResponse::Integer { value } => assert_eq!(3, value),
            other => panic!("unexpected response: {other:?}"),
        }
        match run(
            &engine,
            Command::ZSetRemove {
                key: "board".to_string(),
                member: b"bob".to_vec(),
            },
        ) {
            CommandResponse::Integer { value } => assert_eq!(1, value),
            other => panic!("unexpected response: {other:?}"),
        }
    }

    {
        let engine = new_engine(&root);
        assert_eq!(
            vec!["carol", "-3.5", "alice", "30"],
            full_range(&engine),
            "a removed member must not resurrect on restart"
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}
