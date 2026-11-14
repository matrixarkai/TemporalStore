// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `FeatureAppendWithPolicy` and `FeatureReplace` answer to the same series ceiling as
//! `FeatureAppend`, and until now only the third of the three had a test that said so.
//!
//! WHAT WAS UNPINNED. `FEATURE_ADD_HARD_MAX_SIZE` is checked in `command_validation` by three
//! separate arms that each carry their own copy of the comparison.
//! `feature_append_rejects_hard_size_limit_before_mutation` pins the first. A source-wide search
//! for the refusal message (`size bigger than`) and for every test that builds these two commands
//! found no other test asserting it, so deleting or weakening either of the other two copies
//! would have gone unnoticed. Each copy is a separate edit target, which is why each needs its
//! own test rather than one test standing in for all three.
//!
//! WHAT THIS PINS, AND HOW EACH CLAIM FAILS. Per arm, two claims:
//!
//!   * a request past the ceiling is refused, by its message, and NOTHING is written. The seed
//!     is a point the request would have touched: for `FeatureReplace` the replaced range covers
//!     it, so a replace that ran would have removed it. Make the comparison in that arm
//!     unreachable and the request is accepted, so the test dies on `status.ok`.
//!   * a request exactly AT the ceiling is accepted. This is the control, and it sits exactly at
//!     the limit on purpose. A control well inside the ceiling would pass for an off-by-one that
//!     turns `>` into `>=`, and would also pass for a check that refuses everything. Exactly at
//!     the limit, both die here while the refusal tests stay green.
//!
//! The refusal request is `FEATURE_ADD_HARD_MAX_SIZE + 1` points on its own, so it is over the
//! ceiling whether or not the series already holding a point is counted. These tests pin that a
//! request that is oversized by itself is refused; they deliberately do not pin whether the
//! existing series length is added to it.
//!
//! The ceiling is a WRITE-path check: validation is skipped while the log is replayed. These
//! tests drive the live `execute` path, which is the only place the ceiling applies.
#![allow(clippy::all)]
use super::*;

const POLICY_KEY: &str = "policy-series";
const REPLACE_KEY: &str = "replace-series";

fn seed(engine: &TemporalEngine, key: &str) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: key.to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 1,
                value: b"seeded".to_vec(),
            }],
        },
    });
    assert!(
        response.status.ok,
        "the seed write has to land before the ceiling means anything: {:?}",
        response.status
    );
}

fn only_the_seed() -> CommandResponse {
    CommandResponse::FeaturePoints {
        points: vec![FeaturePoint {
            timestamp_ms: 1,
            value: b"seeded".to_vec(),
        }],
    }
}

fn series(engine: &TemporalEngine, key: &str) -> CommandResponse {
    engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: key.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(10),
            },
        })
        .response
}

fn points(count: usize) -> Vec<FeaturePoint> {
    (0..count)
        .map(|offset| FeaturePoint {
            timestamp_ms: 10 + offset as u64,
            value: b"x".to_vec(),
        })
        .collect()
}

fn assert_refused(ok: bool, code: &str, message: &str, key: &str) {
    assert_eq!(false, ok, "a request past the ceiling has to be refused");
    assert_eq!("invalid_argument", code);
    assert!(
        message.contains(&format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}")),
        "the refusal names the key and the ceiling, as the append command's does: {message}"
    );
}

fn assert_holds_points(engine: &TemporalEngine, key: &str) {
    match series(engine, key) {
        CommandResponse::FeaturePoints { points } => assert!(
            !points.is_empty(),
            "an accepted request at the ceiling has to leave the series readable, not empty"
        ),
        other => panic!("expected the series back after an accepted write, got {other:?}"),
    }
}

#[test]
fn a_policy_append_past_the_ceiling_is_refused_before_it_writes() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    seed(&engine, POLICY_KEY);

    // DENOMINATOR: the seed landed. Without it, "the series still holds one point" below would
    // also be satisfied by a seed that never wrote and a request that never wrote either.
    assert_eq!(
        only_the_seed(),
        series(&engine, POLICY_KEY),
        "DENOMINATOR: the seed has to be readable before the refusal means anything"
    );

    let oversized = points(FEATURE_ADD_HARD_MAX_SIZE + 1);
    assert_eq!(
        FEATURE_ADD_HARD_MAX_SIZE + 1,
        oversized.len(),
        "DENOMINATOR: the request is one past the ceiling by itself"
    );
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppendWithPolicy {
            key: POLICY_KEY.to_string(),
            points: oversized,
            policy: FeatureWritePolicy::Upsert,
            retry_token: None,
        },
    });
    assert_refused(
        response.status.ok,
        &response.status.code,
        &response.status.message,
        POLICY_KEY,
    );

    // Refused BEFORE the mutation: the series is untouched, not partially filled.
    assert_eq!(
        only_the_seed(),
        series(&engine, POLICY_KEY),
        "a refused request writes nothing, so the seeded point is still the only one"
    );
}

#[test]
fn a_policy_append_at_the_ceiling_is_accepted() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    // EXACTLY the ceiling, onto an empty key: not over it, so the check must let it through.
    let at_the_limit = points(FEATURE_ADD_HARD_MAX_SIZE);
    assert_eq!(
        FEATURE_ADD_HARD_MAX_SIZE,
        at_the_limit.len(),
        "DENOMINATOR: the request is exactly at the ceiling"
    );
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppendWithPolicy {
            key: POLICY_KEY.to_string(),
            points: at_the_limit,
            policy: FeatureWritePolicy::Upsert,
            retry_token: None,
        },
    });
    assert!(
        response.status.ok,
        "a request exactly at the ceiling is not over it: {:?}",
        response.status
    );
    assert_holds_points(&engine, POLICY_KEY);
}

#[test]
fn a_replace_past_the_ceiling_is_refused_before_it_writes() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    seed(&engine, REPLACE_KEY);

    assert_eq!(
        only_the_seed(),
        series(&engine, REPLACE_KEY),
        "DENOMINATOR: the seed has to be readable before the refusal means anything"
    );

    let oversized = points(FEATURE_ADD_HARD_MAX_SIZE + 1);
    assert_eq!(
        FEATURE_ADD_HARD_MAX_SIZE + 1,
        oversized.len(),
        "DENOMINATOR: the request is one past the ceiling by itself"
    );
    // The replaced range covers the seed (timestamp 1). A replace that ran would have removed it,
    // so "the seed is still there" below is a real statement about the mutation not happening.
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureReplace {
            key: REPLACE_KEY.to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            points: oversized,
        },
    });
    assert_refused(
        response.status.ok,
        &response.status.code,
        &response.status.message,
        REPLACE_KEY,
    );

    assert_eq!(
        only_the_seed(),
        series(&engine, REPLACE_KEY),
        "a refused replace removes nothing and writes nothing: the covered seed is still there"
    );
}

#[test]
fn a_replace_at_the_ceiling_is_accepted() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);

    let at_the_limit = points(FEATURE_ADD_HARD_MAX_SIZE);
    assert_eq!(
        FEATURE_ADD_HARD_MAX_SIZE,
        at_the_limit.len(),
        "DENOMINATOR: the request is exactly at the ceiling"
    );
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureReplace {
            key: REPLACE_KEY.to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            points: at_the_limit,
        },
    });
    assert!(
        response.status.ok,
        "a replace exactly at the ceiling is not over it: {:?}",
        response.status
    );
    assert_holds_points(&engine, REPLACE_KEY);
}
