// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN ORDERED ZSET READ SHOULD PAY FOR ITS ANSWER, NOT FOR THE SET.
//!
//! `ZSetRange` and `ZSetPop` ordered their members by cloning every member's bytes first and
//! sorting the copies, so a rank range of eight out of 4,096 paid 4,115 allocations and 324 KB to
//! return eight members, and popping ONE member copied every member in the set to decide which one
//! it was. The ordering only ever needed the bytes to COMPARE them, and a comparison needs no
//! allocation -- which `Command::ZSetRank` already knew: two owned tuples per member were removed
//! from it for being "exactly two allocations per member, for a comparison that needs none".
//! These two sites never got that fix.
//!
//! # WHAT THESE TESTS ARE FOR, GIVEN THE FIGURE LIVES ELSEWHERE
//!
//! The allocation figures come from `part4::what_the_score_ordered_zset_reads_cost`, which is
//! `#[ignore]`d AND behind `--features alloc-probe` -- so it is run by no CI job at all, and a
//! guard that runs nowhere cannot protect a change. These tests cannot count allocations without
//! that feature, so they pin the thing that would actually break: THE ANSWER. Moving the copy from
//! "every member" to "the window" is only sound if the window is still the right window, in the
//! right order, including the cases where the order is not the insertion order.
//!
//! # THE CASE A FUTURE CACHED VIEW WILL HAVE TO SURVIVE
//!
//! `a_score_update_reorders_an_ordered_read` is here for a change that has not happened yet. The
//! sanctioned upgrade for these reads is a resident score-ordered view rebuilt at load, and the
//! way such a view goes wrong is a score UPDATE on a member that is already present: the member
//! set does not change, so any staleness test based on presence or on length sees nothing, and the
//! view serves the old order forever. That is the same defect shape as a length-based staleness
//! test on a feature series that is appended to and trimmed in one command. Pinning it now means
//! the view cannot land without satisfying it.

use super::*;

const KEY: &str = "board";

fn engine() -> (TemporalEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    (engine, dir)
}

fn add(engine: &TemporalEngine, member: &str, score: f64) {
    let out = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ZSetAdd {
            key: KEY.to_string(),
            member: member.as_bytes().to_vec(),
            score,
        },
    });
    assert!(out.status.ok, "add {member}: {:?}", out.response);
}

/// The MEMBER sequence of an ordered read, which is what ordering is about. The response
/// interleaves member and score, and the score's rendering is a separate contract that these tests
/// deliberately do not pin -- asserting its text here would make an ordering test fail for a
/// formatting change.
fn members_of(response: CommandResponse) -> Vec<String> {
    match response {
        CommandResponse::Members { members } => members
            .chunks(2)
            .map(|pair| String::from_utf8_lossy(&pair[0]).to_string())
            .collect(),
        other => panic!("expected Members, got {other:?}"),
    }
}

fn range(engine: &TemporalEngine, start: i64, stop: i64, rev: bool) -> Vec<String> {
    members_of(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ZSetRange {
                    key: KEY.to_string(),
                    start,
                    stop,
                    rev,
                },
            })
            .response,
    )
}

fn pop(engine: &TemporalEngine, min: bool, count: u64) -> Vec<String> {
    members_of(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ZSetPop {
                    key: KEY.to_string(),
                    min,
                    count,
                },
            })
            .response,
    )
}

/// Five members whose score order is deliberately NOT their insertion order and NOT their
/// alphabetical order, so a read that accidentally returns either is caught.
fn seeded() -> (TemporalEngine, tempfile::TempDir) {
    let (engine, dir) = engine();
    add(&engine, "echo", 50.0);
    add(&engine, "alpha", 10.0);
    add(&engine, "delta", 40.0);
    add(&engine, "bravo", 20.0);
    add(&engine, "charlie", 30.0);
    (engine, dir)
}

#[test]
fn an_ordered_read_answers_by_score_whatever_the_window() {
    let (engine, _dir) = seeded();
    let all = ["alpha", "bravo", "charlie", "delta", "echo"];

    assert_eq!(range(&engine, 0, -1, false), all, "the whole set, by score");
    assert_eq!(
        range(&engine, 1, 3, false),
        ["bravo", "charlie", "delta"],
        "A WINDOW IN THE MIDDLE is the case the copy was moved for: the answer is three members \
         and the set is five, and the window is resolved against the length before it is sliced"
    );
    assert_eq!(
        range(&engine, -2, -1, false),
        ["delta", "echo"],
        "NEGATIVE INDICES COUNT FROM THE END, which is why the length must be known before the \
         window is -- and why the old helper took that as licence to copy every member first"
    );
    assert_eq!(
        range(&engine, 0, 1, true),
        ["echo", "delta"],
        "reversed, the window is taken AFTER the reversal, so index 0 is the highest score"
    );
    assert_eq!(
        range(&engine, 3, 1, false),
        Vec::<String>::new(),
        "an inverted window is empty, not a panic"
    );
    assert_eq!(
        range(&engine, 0, 99, false),
        all,
        "a window past the end is clamped to the end"
    );
}

#[test]
fn a_score_update_reorders_an_ordered_read() {
    let (engine, _dir) = seeded();
    assert_eq!(
        range(&engine, 0, -1, false),
        ["alpha", "bravo", "charlie", "delta", "echo"],
        "the order before the update"
    );

    // `alpha` is already present, so the MEMBER SET does not change -- only one member's score.
    add(&engine, "alpha", 99.0);

    assert_eq!(
        range(&engine, 0, -1, false),
        ["bravo", "charlie", "delta", "echo", "alpha"],
        "A SCORE UPDATE ON AN EXISTING MEMBER MUST REORDER THE READ. The member set is unchanged, \
         so any ordering derived from a cache whose staleness test looks at membership or at length \
         will serve the old order and never notice. If this is red, an ordered read is answering \
         from something that did not see the update"
    );
    assert_eq!(
        range(&engine, 0, 0, false),
        ["bravo"],
        "and the extreme moved too, which a window of one is the sharpest way to ask"
    );
}

#[test]
fn popping_takes_the_extreme_and_leaves_the_rest_in_order() {
    let (engine, _dir) = seeded();
    assert_eq!(
        pop(&engine, true, 1),
        ["alpha"],
        "POPPING ONE MEMBER used to copy every member in the set to decide which one it was"
    );
    assert_eq!(
        range(&engine, 0, -1, false),
        ["bravo", "charlie", "delta", "echo"],
        "the popped member is gone and the rest keep their order"
    );
    assert_eq!(
        pop(&engine, false, 2),
        ["echo", "delta"],
        "popping from the high end takes the two highest, highest first"
    );
    assert_eq!(
        range(&engine, 0, -1, false),
        ["bravo", "charlie"],
        "and both are gone"
    );
    assert_eq!(
        pop(&engine, true, 99),
        ["bravo", "charlie"],
        "a count past the size takes what there is rather than panicking"
    );
    assert_eq!(
        range(&engine, 0, -1, false),
        Vec::<String>::new(),
        "the set is empty now"
    );
}
