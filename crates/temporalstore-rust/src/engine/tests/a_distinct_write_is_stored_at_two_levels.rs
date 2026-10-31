// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A distinct write is stored at its own precision and at one coarser level.
//!
//! WHY A DISTINCT WRITE FANS OUT WHERE A COUNTER WRITE DOES NOT. A counter over a wide window is
//! the SUM of the fine buckets inside it, so one stored level is enough. A distinct count is not a
//! sum: the number of distinct members in an hour is not the sum of the distinct members of its
//! sixty minutes, because a member appearing in two minutes is one member. So a wide window has to
//! MERGE every fine bucket it covers -- a day at minute precision is 1,440 merges, a week is
//! 10,080 -- and a frequency cap asking "how many distinct users in the last week" pays that on
//! every decision.
//!
//! Storing the member at a coarser level as it arrives turns that into one read per coarse bucket:
//! seven, for a week at day level, plus the two ragged edges at the named precision.
//!
//! WHY STORING IT TWICE CANNOT DOUBLE-COUNT. Set union is idempotent. A member recorded into both
//! its minute bucket and its hour bucket is still one distinct member in each, so no read can
//! count it twice however it combines the levels. The same fan-out on a counter would double every
//! amount, which is why only this family does it.
//!
//! WHAT THESE TESTS PIN. Three claims, and the third is the one that makes the other two safe:
//!
//!   * `a_distinct_member_is_readable_at_the_coarser_level` -- the fan-out happened. A member
//!     written at minute precision answers a question asked at hour precision. Remove the fan-out
//!     and this is 0 instead of 3.
//!   * `a_window_wider_than_the_coarse_level_is_answered_from_it` -- the read actually uses the
//!     coarse level for the aligned interior, and is still correct when the window is ragged and
//!     the edges have to come from the named precision.
//!   * `holding_a_member_at_two_levels_does_not_count_it_twice` -- the idempotence the whole design
//!     rests on, asserted rather than assumed. A read naming no precision covers BOTH levels at
//!     once, so if union were not idempotent this is where it would show, as 6 instead of 3.
#![allow(clippy::all)]
use super::*;

const ONE_MINUTE_MS: u64 = 60_000;
const ONE_HOUR_MS: u64 = 3_600_000;
/// Inside the first hour, and not on an hour boundary, so the bucket starts differ per level.
const WRITE_AT_MS: u64 = 1_800_000;
const MEMBERS: [&[u8]; 3] = [b"user-a", b"user-b", b"user-c"];

/// Three distinct members at minute precision, which fans out to minute and hour.
fn seed_minute_precision(engine: &TemporalEngine, key: &str) {
    for member in MEMBERS {
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateChangeAdd {
                key: key.to_string(),
                timestamp_ms: WRITE_AT_MS,
                value: member.to_vec(),
                precision_ms: Some(ONE_MINUTE_MS),
                ttl_ms: None,
            },
        });
    }
}

fn distinct(
    engine: &TemporalEngine,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    precision_ms: Option<u64>,
) -> i64 {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateQuery {
                key: key.to_string(),
                start_ms,
                end_ms,
                aggregator: "change".to_string(),
                precision_ms,
            },
        })
        .response
    {
        CommandResponse::Integer { value } => value,
        other => panic!("a distinct query answered {other:?}"),
    }
}

#[test]
fn a_distinct_member_is_readable_at_the_coarser_level() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    seed_minute_precision(&engine, "cap:distinct:a");

    // DENOMINATOR: the members are stored at all, at the precision they were written with.
    assert_eq!(
        3,
        distinct(&engine, "cap:distinct:a", 0, ONE_HOUR_MS - 1, Some(ONE_MINUTE_MS)),
        "DENOMINATOR: the write has to be readable at its own precision first"
    );

    // THE LADDER. The question is asked at HOUR precision; these members were written naming the
    // MINUTE. They are answerable only because the write stored them at the hour level too.
    assert_eq!(
        3,
        distinct(&engine, "cap:distinct:a", 0, ONE_HOUR_MS - 1, Some(ONE_HOUR_MS)),
        "a member written at minute precision has to be stored at the hour level as well"
    );
}

#[test]
fn a_window_wider_than_the_coarse_level_is_answered_from_it() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    seed_minute_precision(&engine, "cap:distinct:b");

    // An hour-aligned window: the whole of it is interior, so it is covered by the coarse (hour)
    // level with no edges at all.
    assert_eq!(
        3,
        distinct(&engine, "cap:distinct:b", 0, ONE_HOUR_MS - 1, Some(ONE_MINUTE_MS)),
        "an aligned window is covered by the coarse level"
    );

    // A RAGGED window: it starts and ends mid-hour, so the interior is covered coarsely and the
    // edges have to be read at the named precision. The write sits in the leading edge, before the
    // first whole hour, so a read that forgot its edges would answer 0 here.
    assert_eq!(
        3,
        distinct(
            &engine,
            "cap:distinct:b",
            WRITE_AT_MS - ONE_MINUTE_MS,
            ONE_HOUR_MS + ONE_MINUTE_MS,
            Some(ONE_MINUTE_MS)
        ),
        "a ragged window still has to read its edges at the named precision"
    );

    // And a window NARROWER than one coarse bucket, which no coarse bucket fits inside: the named
    // precision is the only level that can answer it without reaching outside the window.
    assert_eq!(
        3,
        distinct(
            &engine,
            "cap:distinct:b",
            WRITE_AT_MS - ONE_MINUTE_MS,
            WRITE_AT_MS + ONE_MINUTE_MS,
            Some(ONE_MINUTE_MS)
        ),
        "a window narrower than a coarse bucket is answered at the named precision"
    );
}

#[test]
fn holding_a_member_at_two_levels_does_not_count_it_twice() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    seed_minute_precision(&engine, "cap:distinct:c");

    // A read naming NO precision covers every level at once, so it sees each member in its minute
    // bucket AND in its hour bucket. Three members, held twice over, are still three.
    assert_eq!(
        3,
        distinct(&engine, "cap:distinct:c", 0, ONE_HOUR_MS - 1, None),
        "a member held at two levels is one member: union is idempotent, or the ladder is unsound"
    );
}
