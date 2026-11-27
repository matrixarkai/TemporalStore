// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A RANGE OR A POP OF THE FIRST MEMBERS OF A SORTED SET DOES NOT SORT THE SET.
//!
//! `ZSetRange` and `ZSetPop` took the first (or last) few members by sorting every member of the
//! set, so ten members out of a hundred thousand cost a full sort and a buffer of the whole set.
//! They now keep the first `k` in a heap. What they must keep is
//! the answer: the same members, in the same `(score, member)` order, ties included, for every
//! window, both directions, negative indices, and a pop of any count. This file compares the engine
//! with a model that sorts, over sets of several sizes, from empty to larger than any window, so a window that is
//! most of the set and one that is a small part of it are both driven.
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
    format!("member-{n:05}").into_bytes()
}

/// Scores with many ties and some negatives, so the member bytes decide often.
fn score_of(n: usize) -> i64 {
    ((n * 7919) % 13) as i64 - 6
}

/// The model: every member with its score, sorted by (score, member), ascending.
fn model(count: usize) -> Vec<(Vec<u8>, i64)> {
    let mut all: Vec<(Vec<u8>, i64)> = (0..count).map(|n| (member(n), score_of(n))).collect();
    all.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    all
}

fn engine_with(dir: &std::path::Path, count: usize) -> TemporalEngine {
    let engine = engine_on(dir);
    for chunk in (0..count).collect::<Vec<_>>().chunks(256) {
        exec(
            &engine,
            Command::ZSetAddMany {
                key: "z".into(),
                members: chunk
                    .iter()
                    .map(|n| ZSetScoredMember { member: member(*n), score: score_of(*n) as f64 })
                    .collect(),
            },
        );
    }
    engine
}

/// The pairs an answer holds: member bytes and the score they carry.
fn pairs(response: CommandResponse) -> Vec<(Vec<u8>, f64)> {
    let CommandResponse::Members { members } = response else {
        panic!("members expected");
    };
    assert_eq!(members.len() % 2, 0, "member and score come in pairs");
    members
        .chunks(2)
        .map(|pair| {
            let score: f64 = std::str::from_utf8(&pair[1]).unwrap().parse().unwrap();
            (pair[0].clone(), score)
        })
        .collect()
}

fn expected(window: &[(Vec<u8>, i64)]) -> Vec<(Vec<u8>, f64)> {
    window.iter().map(|(m, s)| (m.clone(), *s as f64)).collect()
}

#[test]
fn a_range_of_the_first_members_answers_what_the_full_sort_answered() {
    for count in [0usize, 1, 7, 64, 500] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with(dir.path(), count);
        let ascending = model(count);
        let n = count as i64;
        let windows: Vec<(i64, i64)> = vec![
            (0, 0),
            (0, 9),
            (0, -1),
            (2, 5),
            (-3, -1),
            (-1, -1),
            (0, n.max(1) - 1),
            (n / 2, n / 2 + 3),
            (5, 2),
            (0, 1000),
            (-1000, 4),
        ];
        for rev in [false, true] {
            let mut ordered = ascending.clone();
            if rev {
                ordered.reverse();
            }
            for &(start, stop) in &windows {
                let resolve = |i: i64| if i < 0 { n + i } else { i };
                let from = resolve(start).max(0);
                let to = resolve(stop).min(n - 1);
                let want = if from > to || n == 0 {
                    Vec::new()
                } else {
                    expected(&ordered[from as usize..=to as usize])
                };
                let got = pairs(exec(&engine, Command::ZSetRange { key: "z".into(), start, stop, rev }));
                assert_eq!(got, want, "count {count}, window {start}..{stop}, rev {rev}");
            }
        }
    }
}

#[test]
fn a_pop_takes_the_first_or_last_members_in_order_and_leaves_the_rest() {
    for count in [0usize, 1, 7, 64, 500] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_with(dir.path(), count);
        let mut remaining = model(count);
        // (take from the low end?, how many): small counts take the heap path, the last takes it all.
        for (min, take) in [(true, 3usize), (false, 2), (true, 1), (false, 5), (true, 0), (true, usize::MAX)] {
            let want: Vec<(Vec<u8>, i64)> = {
                let k = take.min(remaining.len());
                if min {
                    remaining.drain(..k).collect()
                } else {
                    let at = remaining.len() - k;
                    let mut taken: Vec<(Vec<u8>, i64)> = remaining.drain(at..).collect();
                    taken.reverse();
                    taken
                }
            };
            let got = pairs(exec(
                &engine,
                Command::ZSetPop { key: "z".into(), min, count: take.min(u64::MAX as usize) as u64 },
            ));
            assert_eq!(got, expected(&want), "count {count}, min {min}, take {take}");
            let left = pairs(exec(&engine, Command::ZSetRange { key: "z".into(), start: 0, stop: -1, rev: false }));
            assert_eq!(left, expected(&remaining), "what is left after the pop, count {count}");
        }
    }
}
