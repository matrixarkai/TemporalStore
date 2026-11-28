// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A REWRITE OR A REMOVAL ASKS ONCE WHETHER TWO ELEMENTS SHARE A BLOCK, NOT ONCE PER ELEMENT.
//!
//! Every rewrite and every removal asked whether another element of the object was still on the
//! block being left, by a pass over every element, so each cost the members the object held. For a
//! kind that writes one block per element the answer is "no" until a compaction fold puts two on
//! one, so the object is now checked once and remembered, and every event that can change which
//! block an element is on forgets it. This file holds the cost flat (a counter of the whole-object
//! passes must not grow with the writes or with the size) and holds the answer: after a compaction
//! a rewritten element must not hide the elements it shares a block with, live or after a restart.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::types::ZSetScoredMember;

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
    format!("member-{n:06}").into_bytes()
}

fn fill(engine: &TemporalEngine, count: usize) {
    for chunk in (0..count).collect::<Vec<_>>().chunks(256) {
        exec(
            engine,
            Command::ZSetAddMany {
                key: "z".into(),
                members: chunk
                    .iter()
                    .map(|n| ZSetScoredMember { member: member(*n), score: *n as f64 })
                    .collect(),
            },
        );
    }
}

fn pairs(engine: &TemporalEngine) -> Vec<(Vec<u8>, f64)> {
    match exec(engine, Command::ZSetRange { key: "z".into(), start: 0, stop: -1, rev: false }) {
        CommandResponse::Members { members } => members
            .chunks(2)
            .map(|p| (p[0].clone(), std::str::from_utf8(&p[1]).unwrap().parse().unwrap()))
            .collect(),
        other => panic!("members expected: {other:?}"),
    }
}

#[test]
fn the_whole_object_is_checked_once_whatever_the_writes_and_the_size() {
    for size in [64usize, 2_048] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        fill(&engine, size);
        let _ = super::recorded_map::element_scans_for_test(true);
        let mut model: std::collections::BTreeMap<Vec<u8>, f64> =
            (0..size).map(|n| (member(n), n as f64)).collect();
        // Re-scores and removals: each asks the question, none may re-run the pass.
        for n in (0..size).step_by(3).take(60) {
            exec(&engine, Command::ZSetAdd { key: "z".into(), member: member(n), score: 1_000_000.0 + n as f64 });
            model.insert(member(n), 1_000_000.0 + n as f64);
        }
        for n in (1..size).step_by(5).take(20) {
            exec(&engine, Command::ZSetRemove { key: "z".into(), member: member(n) });
            model.remove(&member(n));
        }
        for n in (2..size).step_by(7).take(20) {
            exec(&engine, Command::ZSetIncrBy { key: "z".into(), member: member(n), increment: 0.5 });
            // An increment of a member that was removed adds it back at the increment.
            *model.entry(member(n)).or_insert(0.0) += 0.5;
        }
        let scans = super::recorded_map::element_scans_for_test(false);
        assert!(scans <= 1, "size {size}: 100 rewrites and removals ran {scans} whole-object passes");
        let mut want: Vec<(Vec<u8>, f64)> = model.into_iter().collect();
        want.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
        assert_eq!(pairs(&engine), want, "size {size}");
    }
}

#[test]
fn a_rewrite_after_a_compaction_does_not_hide_the_elements_that_share_its_block() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    fill(&engine, 16);
    // Remove a few so a compaction has something to fold, and learn that the object is unshared.
    for n in [1usize, 5, 9] {
        exec(&engine, Command::ZSetRemove { key: "z".into(), member: member(n) });
    }
    engine.compact_shard_blocks(1).unwrap();
    let _ = super::recorded_map::element_scans_for_test(true);
    exec(&engine, Command::ZSetAdd { key: "z".into(), member: member(3), score: 500.0 });
    exec(&engine, Command::ZSetAdd { key: "z".into(), member: member(4), score: 501.0 });
    let mut want: std::collections::BTreeMap<Vec<u8>, f64> = (0..16)
        .filter(|n| ![1usize, 5, 9].contains(n))
        .map(|n| (member(n), n as f64))
        .collect();
    want.insert(member(3), 500.0);
    want.insert(member(4), 501.0);
    let mut want: Vec<(Vec<u8>, f64)> = want.into_iter().collect();
    want.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
    assert_eq!(pairs(&engine), want, "live, after a compaction and two rewrites");
    engine.unload_shard(1);
    engine.load_shard(1);
    assert_eq!(pairs(&engine), want, "after a restart");
}
