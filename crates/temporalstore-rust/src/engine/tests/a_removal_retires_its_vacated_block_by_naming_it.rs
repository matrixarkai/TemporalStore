// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A REMOVAL RETIRES THE BLOCK AN ELEMENT VACATED BY NAMING IT, NOT BY WALKING THE OBJECT.
//!
//! Retiring a vacated block asked every ref of the object whether its block sat at the vacated
//! address, so a removal from a collection cost a pair of lookups per element it held. The ref is
//! now computed from the vacated address, and the walk remains only for a block that is not where
//! its identity says. This file removes members, pops, re-scores and removes by value at two
//! collection sizes and asserts that the walk never ran, that every answer is what a model says, and
//! that the same contents come back after a restart (a block retired wrongly would resurrect or lose
//! a member).
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::types::ZSetScoredMember;
use std::collections::BTreeSet;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn exec(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest { shard_id: 1, command });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn member(n: usize) -> Vec<u8> {
    format!("member-{n:05}").into_bytes()
}

fn set_members(engine: &TemporalEngine) -> BTreeSet<Vec<u8>> {
    match exec(engine, Command::SetMembers { key: "s".into() }) {
        CommandResponse::Members { members } => members.into_iter().collect(),
        other => panic!("members expected: {other:?}"),
    }
}

fn zset_members(engine: &TemporalEngine) -> Vec<Vec<u8>> {
    match exec(engine, Command::ZSetRange { key: "z".into(), start: 0, stop: -1, rev: false }) {
        CommandResponse::Members { members } => members.chunks(2).map(|p| p[0].clone()).collect(),
        other => panic!("members expected: {other:?}"),
    }
}

fn list_members(engine: &TemporalEngine) -> Vec<Vec<u8>> {
    match exec(engine, Command::ListRange { key: "l".into(), start: 0, stop: -1 }) {
        CommandResponse::Members { members } => members,
        other => panic!("members expected: {other:?}"),
    }
}

#[test]
fn removals_never_walk_the_object_and_leave_what_a_model_leaves_across_a_restart() {
    for size in [64usize, 2_048] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        for chunk in (0..size).collect::<Vec<_>>().chunks(256) {
            exec(&engine, Command::SetAddMany { key: "s".into(), members: chunk.iter().map(|n| member(*n)).collect() });
            exec(&engine, Command::ZSetAddMany {
                key: "z".into(),
                members: chunk.iter().map(|n| ZSetScoredMember { member: member(*n), score: *n as f64 }).collect(),
            });
            exec(&engine, Command::ListPushMany { key: "l".into(), members: chunk.iter().map(|n| member(*n)).collect(), left: false });
        }
        let _ = super::storage_bucket_internals::vacated_block_walks_for_test(true);

        let mut set_model: BTreeSet<Vec<u8>> = (0..size).map(member).collect();
        let mut zset_model: Vec<(i64, Vec<u8>)> = (0..size).map(|n| (n as i64, member(n))).collect();
        let mut list_model: Vec<Vec<u8>> = (0..size).map(member).collect();

        // Set and sorted-set removals of present and absent members.
        for n in (0..size).step_by(7) {
            assert_eq!(exec(&engine, Command::SetRemove { key: "s".into(), member: member(n) }), CommandResponse::Integer { value: 1 });
            set_model.remove(&member(n));
            assert_eq!(exec(&engine, Command::ZSetRemove { key: "z".into(), member: member(n) }), CommandResponse::Integer { value: 1 });
            zset_model.retain(|(_, m)| *m != member(n));
        }
        assert_eq!(exec(&engine, Command::SetRemove { key: "s".into(), member: b"absent".to_vec() }), CommandResponse::Integer { value: 0 });
        // Re-scores move a member's block; the old one is retired by the same path.
        for n in (1..size).step_by(11) {
            if n % 7 == 0 {
                continue;
            }
            exec(&engine, Command::ZSetAdd { key: "z".into(), member: member(n), score: 1_000_000.0 + n as f64 });
            for entry in zset_model.iter_mut() {
                if entry.1 == member(n) {
                    entry.0 = 1_000_000 + n as i64;
                }
            }
        }
        zset_model.sort();
        // Pops from both ends of the list, and a removal by value.
        for _ in 0..5 {
            exec(&engine, Command::ListPop { key: "l".into(), left: true });
            list_model.remove(0);
            exec(&engine, Command::ListPop { key: "l".into(), left: false });
            list_model.pop();
        }
        let victim = list_model[list_model.len() / 2].clone();
        exec(&engine, Command::ListRemove { key: "l".into(), count: 1, member: victim.clone() });
        let at = list_model.iter().position(|e| *e == victim).unwrap();
        list_model.remove(at);

        let walks = super::storage_bucket_internals::vacated_block_walks_for_test(false);
        assert_eq!(walks, 0, "size {size}: a removal walked the object's refs {walks} time(s)");

        let want_zset: Vec<Vec<u8>> = zset_model.iter().map(|(_, m)| m.clone()).collect();
        assert_eq!(set_members(&engine), set_model, "size {size}: the set after removals");
        assert_eq!(zset_members(&engine), want_zset, "size {size}: the sorted set after removals and re-scores");
        assert_eq!(list_members(&engine), list_model, "size {size}: the list after pops and a removal");

        // The same contents after a restart: a block retired wrongly would bring one back or lose one.
        engine.unload_shard(1);
        engine.load_shard(1);
        assert_eq!(set_members(&engine), set_model, "size {size}: the set after a restart");
        assert_eq!(zset_members(&engine), want_zset, "size {size}: the sorted set after a restart");
        assert_eq!(list_members(&engine), list_model, "size {size}: the list after a restart");
    }
}
