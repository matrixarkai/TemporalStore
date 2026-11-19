// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WHOLE-HASH READ ANSWERS ITS FIELDS IN ASCENDING NAME ORDER ON EVERY PATH.
//!
//! The hash is an ordered map keyed by field name, and a whole-hash read walks it in key order: bytewise
//! ascending. The read that takes the shared shard lock sorted its answer to say so, and the read that
//! takes the exclusive lock (a shard with a read rate limit, or a key whose deadline lapsed) answered in
//! the order the resident field map happens to hold them, which is arbitrary. The two paths gave two
//! different orders for one hash. Both now answer ascending.
//!
//! TWO ARMS.
//!   * `a_whole_hash_read_answers_ascending_on_the_shared_path` -- the default path.
//!   * `a_whole_hash_read_answers_ascending_on_the_exclusive_path` -- the same read on a shard with a
//!     read rate limit configured (which sends every read down the exclusive path), plus a hash whose
//!     fields were written in descending order, so insertion order and field order differ.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

fn engine_on(dir: &std::path::Path, read_limited: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    if read_limited {
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: crate::control::Config {
                        version: 2,
                        read_qps: Some(1_000_000_000),
                        ..Default::default()
                    },
                })
                .ok
        );
    }
    engine
}

fn fill(engine: &TemporalEngine) -> Vec<String> {
    // Descending, so the order written is the reverse of the order wanted, and enough fields that a
    // map that happened to hold them ascending would be a coincidence.
    let names: Vec<String> = (0..200).rev().map(|i| format!("f{i:04}")).collect();
    for name in &names {
        let outcome = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "h".to_string(),
                field: name.clone(),
                value: name.as_bytes().to_vec(),
            },
        });
        assert!(outcome.status.ok);
    }
    let mut ascending = names;
    ascending.sort();
    ascending
}

fn answer(engine: &TemporalEngine) -> Vec<(String, Vec<u8>)> {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGetAll { key: "h".to_string() },
        })
        .response
    {
        CommandResponse::HashEntries { entries } => entries,
        other => panic!("not entries: {other:?}"),
    }
}

fn check(engine: &TemporalEngine, ascending: &[String], label: &str) {
    for round in 0..2 {
        let entries = answer(engine);
        assert_eq!(
            ascending,
            entries.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>().as_slice(),
            "{label}, read {round}: the fields are not in ascending name order"
        );
        assert!(entries.iter().all(|(name, value)| name.as_bytes() == value.as_slice()));
    }
}

#[test]
fn a_whole_hash_read_answers_ascending_on_the_shared_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), false);
    let ascending = fill(&engine);
    check(&engine, &ascending, "shared path");
}

#[test]
fn a_whole_hash_read_answers_ascending_on_the_exclusive_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), true);
    let ascending = fill(&engine);
    check(&engine, &ascending, "exclusive path");
}
