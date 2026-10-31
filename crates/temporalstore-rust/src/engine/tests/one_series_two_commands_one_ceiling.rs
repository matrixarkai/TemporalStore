// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! `SequenceAdd` grows the same series the feature commands grow, so it answers to the same
//! ceiling.
//!
//! WHAT WAS WRONG. `FEATURE_ADD_HARD_MAX_SIZE` is checked for `FeatureAppend`,
//! `FeatureAppendWithPolicy` and `FeatureReplace`, and
//! `feature_append_rejects_hard_size_limit_before_mutation` pins it for the first of them. It was
//! NOT checked for `SequenceAdd` -- which appends into `features` exactly as those three do;
//! `execute_on_shard` reaches for `features_mut()` under both names. So the ceiling three commands
//! enforce could be reached around by sending rows instead of points, on the same key.
//!
//! WHY THAT IS WORSE THAN A MISSING LIMIT. Validation runs BEFORE execution, so a refused command
//! writes nothing. An ACCEPTED one serialises every row to JSON and appends a block per row, and
//! only then does `trim_timestamped_series` discard everything past `feature_max_size` (10,000 by
//! default). An unbounded batch therefore bought an unbounded amount of durable work for data that
//! did not survive the command that wrote it.
//!
//! WHAT THIS PINS, AND HOW IT FAILS. Three claims:
//!
//!   * the refusal happens at all -- remove the `SequenceAdd` arm from the size validation and the
//!     oversized batch is accepted, so this dies on `status.ok`.
//!   * the ceiling counts what the OTHER command already wrote. The batch here is exactly
//!     `FEATURE_ADD_HARD_MAX_SIZE` rows, which is not over the ceiling by itself: it is over only
//!     because one point was seeded through `FeatureAppend` first. A guard that counted just the
//!     batch would accept this, so the refusal is itself the proof that one series is being
//!     counted under both names.
//!   * nothing was written. The series still holds the seeded point and only that, so the refusal
//!     is before the mutation rather than after a partial one.
//!
//! The seeded write is asserted readable first, so none of the three can pass by the seed having
//! failed.
#![allow(clippy::all)]
use super::*;

const KEY: &str = "shared-series";

fn seed(engine: &TemporalEngine) {
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: KEY.to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 1,
                value: b"seeded".to_vec(),
            }],
        },
    });
}

fn only_the_seed(engine: &TemporalEngine) -> CommandResponse {
    engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: KEY.to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(10),
            },
        })
        .response
}

#[test]
fn a_sequence_add_answers_to_the_feature_ceiling_on_the_same_series() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    seed(&engine);

    // DENOMINATOR: the seed landed. Without this, "the series still holds one point" below would
    // also be satisfied by a seed that never wrote and a batch that never wrote either.
    assert_eq!(
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 1,
                value: b"seeded".to_vec(),
            }]
        },
        only_the_seed(&engine),
        "DENOMINATOR: the feature write has to land before its ceiling means anything"
    );

    // EXACTLY the ceiling's worth of rows. Not over it on its own -- over it only once the point
    // already in the series is counted, which is the whole claim.
    let rows = (0..FEATURE_ADD_HARD_MAX_SIZE)
        .map(|offset| SequenceFeatureRow {
            timestamp_ms: 10 + offset as u64,
            gid: offset as u64,
            action_type: 1,
            duration: 2,
            author_id: 3,
        })
        .collect::<Vec<_>>();
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: KEY.to_string(),
            rows,
        },
    });

    assert_eq!(
        false, response.status.ok,
        "a sequence batch that would carry the series past the ceiling has to be refused"
    );
    assert_eq!("invalid_argument", response.status.code);
    assert!(
        response
            .status
            .message
            .contains(&format!("{KEY} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}")),
        "the refusal names the key and the ceiling, as the feature commands' does: {}",
        response.status.message
    );

    // And it was refused BEFORE the mutation: the series is untouched, not partially filled.
    assert_eq!(
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 1,
                value: b"seeded".to_vec(),
            }]
        },
        only_the_seed(&engine),
        "a refused batch writes nothing, so the seeded point is still the only one"
    );
}

#[test]
fn a_sequence_add_inside_the_ceiling_is_still_accepted() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    seed(&engine);

    // The guard must not refuse ordinary traffic. Two rows onto a one-point series is nowhere near
    // the ceiling, and if this were refused the arm above would be rejecting everything rather
    // than rejecting what is oversized.
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: KEY.to_string(),
            rows: vec![
                SequenceFeatureRow {
                    timestamp_ms: 20,
                    gid: 1,
                    action_type: 1,
                    duration: 1,
                    author_id: 1,
                },
                SequenceFeatureRow {
                    timestamp_ms: 21,
                    gid: 2,
                    action_type: 1,
                    duration: 1,
                    author_id: 1,
                },
            ],
        },
    });
    assert_eq!(
        true, response.status.ok,
        "a batch well inside the ceiling is ordinary traffic: {}",
        response.status.message
    );
}
