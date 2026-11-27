// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A Control State detail read is bounded by configuration, not by whether the caller passed a
//! count.
//!
//! WHAT WAS WRONG. `ControlStateDetail` took `count.unwrap_or(usize::MAX)`. A caller that named no
//! count got EVERY bucket in the window materialised into one response -- on a key that has been
//! counting for a week at minute precision that is ten thousand points, decided by an argument
//! being omitted rather than by any configuration.
//!
//! ITS SIBLING ALREADY DID IT RIGHT, which is what this follows rather than inventing a bound.
//! `FeatureDetail`, in the same `match`, computes
//! `count.unwrap_or(feature_max_size).min(feature_max_size)`: a caller naming nothing gets the
//! configured cap, and a caller naming MORE than the cap still gets the cap. Every query in the
//! model this follows carries its own bound for the same reason.
//!
//! WHY `feature_max_size` CANNOT TRIM THIS SERIES, so the test means what it says. The cap is also
//! the trim bound for the FEATURE series, applied by `trim_timestamped_series` in the feature and
//! sequence write arms. The control-state counter series is a different map and no arm trims it, so
//! setting the cap low leaves all the buckets stored and changes only what a read returns. That is
//! what lets this test distinguish "the read is bounded" from "the data was discarded" -- and the
//! first assertion checks exactly that distinction.
//!
//! HOW EACH CLAIM FAILS. Restore `usize::MAX` and the unnamed read returns all ten points, so
//! `a_detail_read_with_no_count_is_bounded_by_the_configured_cap` dies. Drop the `.min()` and the
//! over-asking read returns ten, so `a_count_larger_than_the_cap_is_still_capped` dies. Hard-code
//! the cap instead of reading it and `a_named_count_under_the_cap_is_honoured` dies, because a
//! caller asking for less must still get less.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;
const KEY: &str = "detail:bounded";
/// Deliberately tiny, and far below the ten buckets written, so a bounded read and an unbounded
/// one cannot return the same number.
const CAP: usize = 3;
const BUCKETS: u64 = 10;

fn engine_with_cap() -> TemporalEngine {
    let engine = TemporalEngine::default();
    engine.load_shard(SHARD);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: SHARD,
                config: Config {
                    version: 2,
                    feature_max_size: CAP,
                    ..Config::default()
                },
            })
            .ok,
        "setting the cap failed"
    );
    for bucket in 0..BUCKETS {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::ControlStateIncrement {
                        key: KEY.to_string(),
                        timestamp_ms: (bucket + 1) * 1_000,
                        amount: 1,
                    },
                })
                .status
                .ok
        );
    }
    engine
}

fn detail(engine: &TemporalEngine, count: Option<usize>) -> usize {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::ControlStateDetail {
                key: KEY.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count,
            },
        })
        .response
    {
        CommandResponse::FeaturePoints { points } => points.len(),
        other => panic!("detail answered {other:?}"),
    }
}

fn total(engine: &TemporalEngine) -> i64 {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::ControlStateCount {
                key: KEY.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
            },
        })
        .response
    {
        CommandResponse::Integer { value } => value,
        other => panic!("count answered {other:?}"),
    }
}

#[test]
fn a_detail_read_with_no_count_is_bounded_by_the_configured_cap() {
    let engine = engine_with_cap();

    // DENOMINATOR, and the one that makes this a READ bound rather than a write trim: all ten
    // buckets are stored. The cap is the feature series' trim bound, and nothing trims this map --
    // if it did, a "bounded read" would be indistinguishable from discarded data.
    assert_eq!(
        BUCKETS as i64,
        total(&engine),
        "DENOMINATOR: every bucket has to still be stored, or this measures a trim"
    );

    assert_eq!(
        CAP,
        detail(&engine, None),
        "a detail read naming no count must take the configured cap, not every bucket in the window"
    );
}

#[test]
fn a_count_larger_than_the_cap_is_still_capped() {
    let engine = engine_with_cap();
    assert_eq!(
        CAP,
        detail(&engine, Some(BUCKETS as usize * 10)),
        "asking for more than the cap must still return the cap"
    );
}

#[test]
fn a_named_count_under_the_cap_is_honoured() {
    let engine = engine_with_cap();
    // The bound must not become a floor: a caller asking for less still gets less.
    assert_eq!(
        2,
        detail(&engine, Some(2)),
        "a count under the cap is the caller's business and must be honoured"
    );
}
