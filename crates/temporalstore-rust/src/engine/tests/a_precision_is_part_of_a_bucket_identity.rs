// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A Control State bucket belongs to the precision it was written at, not only to an instant.
//!
//! WHAT WAS WRONG. A bucket was identified by its start alone: a write truncated its timestamp
//! down to a multiple of the precision it named and added its amount there. Nothing recorded WHICH
//! precision that was, so two writes naming different precisions shared a cell whenever the
//! coarser truncation agreed with the finer one -- and at the top of an hour, every precision from
//! a second upwards agrees. A per-minute write of 1 and a per-hour write of 5 at 01:00:00.000 both
//! reduced to 3_600_000 and became a single cell holding 6.
//!
//! WHY THAT IS NOT AN EXOTIC SHAPE. Serving a per-minute rate and a per-hour cap off one counter
//! is the ordinary way to use this model, and it needs no unusual call: a single
//! `ControlStateSetAndGetWithOptions` names a precision, writes at it and reads a window back, and
//! the read spanned every precision in that window. So the per-minute answer arrived with the
//! per-hour write added in, and a cap deciding on that answer admitted traffic it should have
//! refused. The error is silent, it is in the direction of over-admitting, and nothing in the
//! response says a second precision was involved.
//!
//! WHAT THE FORMAT DOES. The precision is part of the stored field's NAME -- a field is the
//! precision's code followed by the bucket's start -- so two precisions cannot occupy one field,
//! and a query names the precision it wants and reads only the fields bearing that code. The
//! bucket key here carries the same pair in the high bits of the `u64` the series already held.
//!
//! WHAT THESE TESTS PIN, AND HOW THEY FAIL. Three separate claims, because the first two can be
//! satisfied by changes that break the third:
//!
//!   * `two_precisions_on_one_key_do_not_share_a_bucket` -- the buckets are DISTINCT, and the
//!     instant each reports is still the bucket's start. Drop the precision from the key and the
//!     window holds one point of 6 instead of two of 5 and 1. Report the packed key instead of
//!     unpacking it and the instant comes back as a date in the year 10889, which is why this
//!     asserts the timestamps and not just the count of points.
//!   * `a_read_that_names_a_precision_answers_at_that_precision` -- the defect itself, at the one
//!     command that reaches it. Widen that read back to every precision and the per-minute answer
//!     returns 6.
//!   * `a_read_that_names_no_precision_still_answers_across_all_of_them` -- the compatibility
//!     obligation, and the reason this change needed no format migration. Buckets written before
//!     the precision existed carry the format's `DISABLE` code, so a caller that cannot name a
//!     precision -- which is every caller of `ControlStateCount`, and every caller that existed
//!     before this -- must still see everything in the window. Scope the untagged read to one code
//!     and this dies with 5 where it wants 6.
#![allow(clippy::all)]
use super::*;

/// 01:00:00.000. Every precision from a second upwards truncates to this instant, which is what
/// made the collision reachable: the buckets collide exactly when the truncations agree.
const TOP_OF_HOUR_MS: u64 = 3_600_000;
const ONE_MINUTE_MS: u64 = 60_000;
const ONE_HOUR_MS: u64 = 3_600_000;
/// A window wide enough to hold the hour, so neither read can miss a bucket for being outside it.
const WINDOW_END_MS: u64 = 7_200_000;

fn increment_at(engine: &TemporalEngine, key: &str, amount: i64, precision_ms: u64) {
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ControlStateIncrementWithOptions {
            key: key.to_string(),
            timestamp_ms: TOP_OF_HOUR_MS,
            amount,
            precision_ms: Some(precision_ms),
            ttl_ms: None,
        },
    });
}

fn count(engine: &TemporalEngine, key: &str) -> i64 {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateCount {
                key: key.to_string(),
                start_ms: 0,
                end_ms: WINDOW_END_MS,
            },
        })
        .response
    {
        CommandResponse::Integer { value } => value,
        other => panic!("ControlStateCount answered {other:?}"),
    }
}

#[test]
fn two_precisions_on_one_key_do_not_share_a_bucket() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    increment_at(&engine, "cap:u1", 5, ONE_HOUR_MS);
    increment_at(&engine, "cap:u1", 1, ONE_MINUTE_MS);

    // DENOMINATOR: both writes landed. Without this the two-point expectation below would also be
    // produced by the second write being dropped on the floor.
    assert_eq!(
        6,
        count(&engine, "cap:u1"),
        "DENOMINATOR: both amounts have to be stored before their SEPARATION means anything"
    );

    let CommandResponse::FeaturePoints { points } = engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ControlStateDetail {
                key: "cap:u1".to_string(),
                start_ms: 0,
                end_ms: WINDOW_END_MS,
                count: None,
            },
        })
        .response
    else {
        panic!("ControlStateDetail did not answer with points");
    };

    // TWO buckets, not one of 6. They share an instant and differ only in precision, which is the
    // whole claim.
    let mut amounts: Vec<Vec<u8>> = points.iter().map(|point| point.value.clone()).collect();
    amounts.sort();
    assert_eq!(
        vec![b"1".to_vec(), b"5".to_vec()],
        amounts,
        "the per-minute and per-hour writes have to be two buckets, not one cell holding their sum"
    );

    // And each still reports the instant it covers. A packed key reported raw is an instant tens of
    // thousands of years from now, and nothing else in this test would notice.
    for point in &points {
        assert_eq!(
            TOP_OF_HOUR_MS, point.timestamp_ms,
            "a bucket reports the START it names, with its precision code removed"
        );
    }
}

#[test]
fn a_read_that_names_a_precision_answers_at_that_precision() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let set_and_get = |amount: i64, precision_ms: u64| -> i64 {
        match engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateSetAndGetWithOptions {
                    family: ControlStateFamily::Counter,
                    key: "cap:u2".to_string(),
                    timestamp_ms: TOP_OF_HOUR_MS,
                    amount,
                    start_ms: 0,
                    end_ms: WINDOW_END_MS,
                    aggregator: "sum".to_string(),
                    precision_ms: Some(precision_ms),
                    ttl_ms: None,
                    uuid: None,
                },
            })
            .response
        {
            CommandResponse::Integer { value } => value,
            other => panic!("set-and-get answered {other:?}"),
        }
    };

    // The hourly cap's own traffic, read back at the hour: 5.
    assert_eq!(
        5,
        set_and_get(5, ONE_HOUR_MS),
        "DENOMINATOR: a read at one precision has to see that precision's own write"
    );

    // THE DEFECT. One request, naming the minute, writing 1. The answer is this minute's count.
    // It used to be 6, because the read spanned the hourly bucket sharing the instant -- so a
    // per-minute rate limit saw six times the traffic the minute actually carried.
    assert_eq!(
        1,
        set_and_get(1, ONE_MINUTE_MS),
        "a read naming the minute must not be answered with the hour's writes"
    );
}

#[test]
fn a_read_that_names_no_precision_still_answers_across_all_of_them() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    increment_at(&engine, "cap:u3", 5, ONE_HOUR_MS);
    increment_at(&engine, "cap:u3", 1, ONE_MINUTE_MS);

    // `ControlStateCount` carries no precision and never could, and neither could any caller
    // written before the precision was part of a bucket's identity. Every bucket in the window is
    // its answer -- including, on a store written by an older build, buckets filed under the
    // format's `DISABLE` code. This is what makes the change additive rather than a migration.
    assert_eq!(
        6,
        count(&engine, "cap:u3"),
        "a read that names no precision answers across every precision in the window"
    );
}
