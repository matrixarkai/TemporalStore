// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SEQUENCE ROW IS A FEATURE POINT, SO A CACHED FEATURE QUERY MUST SEE IT.
//!
//! `FeatureQuery` serves its answer from a response cache keyed by the key and the window. A write
//! that adds a point to the series has to retire that answer. `FeatureAppend` does; this drives
//! `SequenceAdd`, which files into the same series, through the real execute path: read (and so
//! cache), add, read again.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::{FeaturePoint, SequenceFeatureRow};

const SHARD: ShardId = 1;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let outcome = engine.execute(ExecuteRequest { shard_id: SHARD, command });
    assert!(outcome.status.ok, "{:?}", outcome.response);
    outcome.response
}

fn row(timestamp_ms: u64) -> SequenceFeatureRow {
    SequenceFeatureRow { timestamp_ms, gid: timestamp_ms, action_type: 1, duration: 2, author_id: 3 }
}

fn add(engine: &TemporalEngine, key: &str, timestamp_ms: u64) {
    run(engine, Command::SequenceAdd { key: key.to_string(), rows: vec![row(timestamp_ms)] });
}

fn seen(engine: &TemporalEngine, key: &str) -> Vec<u64> {
    match run(
        engine,
        Command::FeatureQuery { key: key.to_string(), start_ms: 0, end_ms: u64::MAX, count: None },
    ) {
        CommandResponse::FeaturePoints { points } => {
            points.into_iter().map(|point| point.timestamp_ms).collect()
        }
        other => panic!("not points: {other:?}"),
    }
}

#[test]
fn a_cached_feature_query_sees_a_sequence_add() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    add(&engine, "s", 1_000);
    // Read twice: the second answer is the cached one.
    assert_eq!(vec![1_000], seen(&engine, "s"));
    assert_eq!(vec![1_000], seen(&engine, "s"));

    add(&engine, "s", 2_000);
    assert_eq!(vec![1_000, 2_000], seen(&engine, "s"), "the first add was not seen");
    assert_eq!(vec![1_000, 2_000], seen(&engine, "s"));

    add(&engine, "s", 3_000);
    assert_eq!(vec![1_000, 2_000, 3_000], seen(&engine, "s"), "the second add was not seen");

    // A feature append is the control: the sibling write that has always retired the answer.
    run(
        &engine,
        Command::FeatureAppend {
            key: "s".to_string(),
            points: vec![FeaturePoint { timestamp_ms: 4_000, value: b"1".to_vec() }],
        },
    );
    assert_eq!(vec![1_000, 2_000, 3_000, 4_000], seen(&engine, "s"), "control: feature append");
}

#[test]
fn a_cached_feature_query_sees_a_sequence_add_after_expiry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    run(
        &engine,
        Command::FeatureAppendWithTtl {
            key: "e".to_string(),
            points: vec![FeaturePoint { timestamp_ms: 10, value: b"1".to_vec() }],
            ttl_ms: 1,
        },
    );
    assert_eq!(vec![10], seen(&engine, "e"));
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert!(seen(&engine, "e").is_empty(), "the expired series still answered");
    add(&engine, "e", 5_000);
    assert_eq!(vec![5_000], seen(&engine, "e"), "an add after expiry was not seen");
    add(&engine, "e", 6_000);
    assert_eq!(vec![5_000, 6_000], seen(&engine, "e"));
}
