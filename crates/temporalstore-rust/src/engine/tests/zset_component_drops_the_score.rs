// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! GUARDS FOR "THE SCORE LEAVES THE ZSET COMPONENT", DRIVEN THROUGH `engine.execute` AND A REAL
//! COLD RELOAD.
//!
//! `BlockIndex` was 56 bytes with zero slack asserted; `component: Option<Arc<str>>` is 16 of
//! those 56. Set and list already collapse to one index entry a page; a zset could not, because
//! its component was not a name -- it was the score, `{biased:016x}` then the member in hex. This
//! change takes the score off the component (`zset_component` is `hex::encode(member)` alone
//! now), which makes zset collapsible like the others -- collapsing it is a separate, later step,
//! not this one.
//!
//! A HAND-BUILT `BlockIndex` FIXTURE MAKING ZERO ENGINE CALLS WOULD STAY GREEN UNDER ANY VERSION OF
//! THIS CUT, which is why every guard here goes through `engine.execute` and, for three of the
//! four, a real reload -- fresh `TemporalEngine` instances over the same on-disk directories,
//! never a resident-map peek standing in for one.
//!
//! # THE FOUR GUARDS
//!
//!   1. `n_members_at_distinct_scores_survive_a_cold_reload_in_score_order` -- the ordinary case.
//!   2. `a_removed_member_stays_removed_across_a_cold_reload` -- a loss guard is not a
//!      resurrection guard, so this is a second, separate test rather than one more assertion on
//!      the first.
//!   3. `a_rescore_leaves_exactly_one_member_across_a_cold_reload` -- `ZSetCard` and `ZSetScore`
//!      after `ZSetAdd m@1` then `ZSetAdd m@2`, through the collapse this change drives at the
//!      component level (see `container_tombstone_entry::a_rescore_sweeps_its_own_tombstone_...`
//!      for the index-entry mechanics; this guard is the user-visible shape).
//!   4. `a_zset_write_recovered_through_replay_alone_returns_its_score` -- **the guard that catches
//!      the data loss an earlier attempt at this change measured.** The score used to be
//!      recoverable from the component text on replay; it is not any more, so it has to ride the
//!      WAL outcome's own `value` slot (`RecordedKind::outcome_value`) instead. This guard writes a
//!      zset member and reloads WITHOUT an `unload_shard` -- `replay_under_the_gate`'s mechanism:
//!      an unload is what materializes the base index, so skipping it leaves the reload no choice
//!      but to replay the WAL, which is the only path that can lose the score this way.
//!
//! # THE DISCRIMINATING MUTATION, RECORDED HERE RATHER THAN LEFT ONLY IN THE PR BODY
//!
//! Make `ZSetKind::outcome_value` return `None` unconditionally. That must redden guard 4 ALONE:
//! guards 1-3 stay green because the index snapshot a normal `unload_shard` writes still carries
//! `zset_index_serde`'s copy of the score, and only guard 4 forces a path with no index snapshot
//! to fall back on. A `+1` on the score bits would redden everything and prove nothing about this
//! specific wire.

#![allow(clippy::all)]
use super::*;

fn engine_on(dir: &std::path::Path, cache: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join(cache),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, table: &str) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: table.to_string(),
        shard_uri: format!("local://{table}/1"),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "read failed: {response:?}");
    response.response
}

fn score_of(engine: &TemporalEngine, key: &str, member: &[u8]) -> Option<f64> {
    match read(
        engine,
        Command::ZSetScore {
            key: key.to_string(),
            member: member.to_vec(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).parse::<f64>().ok()
        }
        crate::types::CommandResponse::Bytes { value: None } => None,
        other => panic!("ZSetScore answered {other:?}"),
    }
}

fn card_of(engine: &TemporalEngine, key: &str) -> i64 {
    match read(
        engine,
        Command::ZSetCard {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("ZSetCard answered {other:?}"),
    }
}

fn member_bytes(index: usize) -> Vec<u8> {
    format!("zcd-member-{index:04}").into_bytes()
}

// =================================================================================================
// 1. PRESENT SURVIVES
// =================================================================================================

/// rust-internal: drives ZSetAdd through a real cold reload, no external surface
#[test]
fn n_members_at_distinct_scores_survive_a_cold_reload_in_score_order() {
    const N: usize = 12;
    let key = "zcd-present";
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let engine = engine_on(dir.path(), "cache-writer");
        load_on(&engine, "zcd-present-table");
        for index in 0..N {
            write(
                &engine,
                Command::ZSetAdd {
                    key: key.to_string(),
                    member: member_bytes(index),
                    score: index as f64 * 1.5,
                },
            );
        }
        for index in 0..N {
            assert_eq!(
                score_of(&engine, key, &member_bytes(index)),
                Some(index as f64 * 1.5),
                "member {index} did not read back its own score BEFORE the reload"
            );
        }
        engine.unload_shard(1);
    }

    let engine = engine_on(dir.path(), "cache-reloaded");
    load_on(&engine, "zcd-present-table");

    for index in 0..N {
        assert_eq!(
            score_of(&engine, key, &member_bytes(index)),
            Some(index as f64 * 1.5),
            "member {index} did not come back at its own score after a cold reload"
        );
    }

    let ranged = match read(
        &engine,
        Command::ZSetRangeByScore {
            key: key.to_string(),
            min: f64::MIN,
            max: f64::MAX,
            min_exclusive: false,
            max_exclusive: false,
            rev: false,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("ZSetRangeByScore answered {other:?}"),
    };
    assert_eq!(
        ranged.len(),
        2 * N,
        "ZSetRangeByScore answered {} interleaved entries, not the {} this fixture wrote",
        ranged.len(),
        2 * N
    );
    let scores: Vec<f64> = ranged
        .chunks(2)
        .map(|pair| String::from_utf8_lossy(&pair[1]).parse::<f64>().expect("a score string"))
        .collect();
    let mut sorted = scores.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(
        scores, sorted,
        "ZSetRangeByScore did not answer in score order after a cold reload: {scores:?}"
    );
    println!("[present] {N} members survived a cold reload, in score order");
}

// =================================================================================================
// 2. REMOVED STAYS REMOVED
// =================================================================================================

/// A LOSS GUARD IS NOT A RESURRECTION GUARD -- a separate test, not one more assertion on the
/// first, which the module doc explains.
///
/// rust-internal: drives ZSetAdd and ZSetRemove through a real cold reload, no external surface
#[test]
fn a_removed_member_stays_removed_across_a_cold_reload() {
    const N: usize = 10;
    let key = "zcd-removed";
    let removed_index = 4;
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let engine = engine_on(dir.path(), "cache-writer");
        load_on(&engine, "zcd-removed-table");
        for index in 0..N {
            write(
                &engine,
                Command::ZSetAdd {
                    key: key.to_string(),
                    member: member_bytes(index),
                    score: index as f64,
                },
            );
        }
        write(
            &engine,
            Command::ZSetRemove {
                key: key.to_string(),
                member: member_bytes(removed_index),
            },
        );
        assert_eq!(
            card_of(&engine, key),
            (N - 1) as i64,
            "DENOMINATOR: the removal did not take effect before the reload"
        );
        engine.unload_shard(1);
    }

    let engine = engine_on(dir.path(), "cache-reloaded");
    load_on(&engine, "zcd-removed-table");

    assert_eq!(
        card_of(&engine, key),
        (N - 1) as i64,
        "ZCARD is not N-1 after a cold reload -- either the removed member came back or an \
         unrelated one went missing"
    );
    assert_eq!(
        score_of(&engine, key, &member_bytes(removed_index)),
        None,
        "the removed member came back after a cold reload"
    );
    for index in 0..N {
        if index == removed_index {
            continue;
        }
        assert_eq!(
            score_of(&engine, key, &member_bytes(index)),
            Some(index as f64),
            "member {index} did not survive the cold reload at its own score"
        );
    }
    println!("[removed] member {removed_index} of {N} stayed removed, the rest kept their scores");
}

// =================================================================================================
// 3. A RESCORE LEAVES EXACTLY ONE MEMBER
// =================================================================================================

/// THE USER-VISIBLE SHAPE OF THE COLLAPSE THIS CHANGE DRIVES AT THE COMPONENT LEVEL.
///
/// `container_tombstone_entry::a_rescore_sweeps_its_own_tombstone_because_the_component_no_longer_
/// spells_the_score` drives the index-entry mechanics (one live entry, zero tombstones) in the
/// same process, without a reload. This guard asks the same question of a REAL cold reload, which
/// is the thing a hand-built index fixture cannot stand in for.
///
/// rust-internal: drives ZSetAdd twice at different scores through a real cold reload
#[test]
fn a_rescore_leaves_exactly_one_member_across_a_cold_reload() {
    let key = "zcd-rescore";
    let member = b"zcd-rescored-member".to_vec();
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let engine = engine_on(dir.path(), "cache-writer");
        load_on(&engine, "zcd-rescore-table");
        write(
            &engine,
            Command::ZSetAdd {
                key: key.to_string(),
                member: member.clone(),
                score: 1.0,
            },
        );
        write(
            &engine,
            Command::ZSetAdd {
                key: key.to_string(),
                member: member.clone(),
                score: 2.0,
            },
        );
        assert_eq!(
            (card_of(&engine, key), score_of(&engine, key, &member)),
            (1, Some(2.0)),
            "DENOMINATOR: the rescore did not already answer ZCARD==1 and ZSCORE==2 before the \
             reload"
        );
        engine.unload_shard(1);
    }

    let engine = engine_on(dir.path(), "cache-reloaded");
    load_on(&engine, "zcd-rescore-table");

    assert_eq!(
        card_of(&engine, key),
        1,
        "ZCARD is not 1 after a cold reload -- a rescore must not leave the member twice, once \
         per score"
    );
    assert_eq!(
        score_of(&engine, key, &member),
        Some(2.0),
        "ZSCORE is not 2.0 after a cold reload -- the rescore's own score did not survive"
    );
    println!("[rescore] ZCARD==1 and ZSCORE==2.0 survived a cold reload");
}

// =================================================================================================
// 4. REPLAY-ONLY RECOVERY -- THE GUARD THAT CATCHES THE DATA LOSS AN EARLIER ATTEMPT MEASURED
// =================================================================================================

/// THE SCORE RIDES THE WAL OUTCOME'S `value` SLOT NOW, NOT THE COMPONENT, AND THIS IS THE ONE
/// PATH THAT CAN ONLY REACH IT THAT WAY.
///
/// `replay_under_the_gate`'s mechanism, reused here rather than re-derived: an `unload_shard` is
/// what materializes the base index, so a fixture that never unloads leaves the reload no choice
/// but to replay the WAL through `apply_outcome_item` alone. That is the ONLY path with no page
/// read and no index snapshot available to fall back on for the score -- which is exactly why an
/// earlier, naive attempt at this change lost it here: the component used to carry
/// `{biased:016x}` and the replay arm decoded the score out of that text; with the score off the
/// component, a replay arm that still looked there would find none and either refuse the whole
/// shard load or silently install the wrong score. Neither happens if the score rode `value`
/// instead, which is what this guard is for.
///
/// rust-internal: drives one ZSetAdd through replay-only recovery, no external surface
#[test]
fn a_zset_write_recovered_through_replay_alone_returns_its_score() {
    let key = "zcd-replay";
    let member = b"zcd-replay-only-member".to_vec();
    let score = 3.0f64;
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let engine = engine_on(dir.path(), "cache-writer");
        load_on(&engine, "zcd-replay-table");
        write(
            &engine,
            Command::ZSetAdd {
                key: key.to_string(),
                member: member.clone(),
                score,
            },
        );
        assert_eq!(
            score_of(&engine, key, &member),
            Some(score),
            "DENOMINATOR: the write did not even answer its own score before any reload"
        );
        // DELIBERATELY NO `unload_shard`. The engine is dropped here with its index never
        // flushed, which is what forces the next load to replay rather than read a snapshot.
    }

    let engine = engine_on(dir.path(), "cache-reloaded");
    load_on(&engine, "zcd-replay-table");

    assert_eq!(
        score_of(&engine, key, &member),
        Some(score),
        "the member did not come back at its own score through replay-only recovery. If it came \
         back at NO score, or ABSENT, or the shard failed to load at all, the WAL outcome's `value` \
         slot is not carrying the score the way `RecordedKind::outcome_value` is supposed to."
    );
    assert_eq!(
        card_of(&engine, key),
        1,
        "ZCARD is not 1 after replay-only recovery"
    );
    println!("[replay-only] a zset write with no flushed index recovered its score through replay alone");
}
