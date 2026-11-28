// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A FEATURE AGGREGATE CAN TAKE A TYPED REDUCER, ITS PARAMETERS IN FIELDS RATHER THAN IN A NAME.
//!
//! `FeatureAggQuery` names its reduction with a string and has no place for a half-life, a
//! smoothing factor or a weight. `FeatureReduceQuery` carries a typed `FeatureReducer`, and the
//! shared reductions module does the arithmetic. The name-based query is untouched.
//!
//! The series every test reads: values 2, 4, a non-number, 4, 6 at 1000, 2000, 2500, 3000, 4000,
//! over the window 0..=4999 (5 seconds). The non-number is dropped by every numeric reduction, so
//! the numbers are 2, 4, 4, 6: sum 16, mean 4, squared deviations 4, 0, 0, 4.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

use super::*;
use crate::types::{FeatureReducer, FeatureRounding, MAX_REDUCER_WEIGHTS};

const END_MS: u64 = 4_999;

fn engine_with_series() -> (TemporalEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let points = [(1_000, "2"), (2_000, "4"), (2_500, "abc"), (3_000, "4"), (4_000, "6")]
        .into_iter()
        .map(|(timestamp_ms, value)| FeaturePoint {
            timestamp_ms,
            value: value.as_bytes().to_vec(),
        })
        .collect();
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "k".to_string(),
            points,
        },
    });
    assert!(outcome.status.ok, "fixture append failed: {:?}", outcome.response);
    (engine, dir)
}

fn typed_command(
    reducer: FeatureReducer,
    rounding: FeatureRounding,
    scale: u64,
    count: Option<usize>,
) -> Command {
    Command::FeatureReduceQuery {
        key: "k".to_string(),
        start_ms: 0,
        end_ms: END_MS,
        reducer,
        rounding,
        scale,
        count,
    }
}

fn typed(engine: &TemporalEngine, reducer: FeatureReducer, rounding: FeatureRounding, scale: u64) -> i64 {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: typed_command(reducer, rounding, scale, None),
        })
        .response
    {
        CommandResponse::Aggregate { value } => value,
        other => panic!("expected Aggregate, got {other:?}"),
    }
}

fn named(engine: &TemporalEngine, aggregator: &str) -> i64 {
    match engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAggQuery {
                key: "k".to_string(),
                start_ms: 0,
                end_ms: END_MS,
                aggregator: aggregator.to_string(),
                count: None,
            },
        })
        .response
    {
        CommandResponse::Aggregate { value } => value,
        other => panic!("expected Aggregate, got {other:?}"),
    }
}

/// The refusal a typed query gets, or `None` when it is accepted.
fn refusal(engine: &TemporalEngine, command: Command) -> Option<String> {
    let outcome = engine.execute(ExecuteRequest { shard_id: 1, command });
    (!outcome.status.ok).then(|| {
        assert_eq!(outcome.status.code, "invalid_argument");
        outcome.status.message
    })
}

use FeatureRounding::{Ceil, Floor, Nearest};

/// Each value below is worked by hand from the series in the module note, and none is a number the
/// name-based query can produce (it has no scale, no parameter and no fractional answer).
#[test]
fn every_typed_reducer_answers_its_hand_computed_value() {
    let (engine, _dir) = engine_with_series();
    let t = |reducer, rounding, scale| typed(&engine, reducer, rounding, scale);

    // mean 16 / 4 = 4, at three decimals.
    assert_eq!(t(FeatureReducer::Mean, Nearest, 1_000), 4_000);
    // population variance 8 / 4 = 2; sample variance 8 / 3 = 2.6667.
    assert_eq!(t(FeatureReducer::Variance { sample: false }, Nearest, 1_000), 2_000);
    assert_eq!(t(FeatureReducer::Variance { sample: true }, Floor, 1_000), 2_666);
    assert_eq!(t(FeatureReducer::Variance { sample: true }, Ceil, 1_000), 2_667);
    // sqrt(2) = 1.41421: nearest and ceiling differ at three decimals.
    assert_eq!(t(FeatureReducer::StdDev { sample: false }, Nearest, 1_000), 1_414);
    assert_eq!(t(FeatureReducer::StdDev { sample: false }, Ceil, 1_000), 1_415);
    // Weights 0.125, 0.25, 0.5, 1 at ages 3000, 2000, 1000, 0 (half-life 1000, as of 4000):
    // 2/8 + 4/4 + 4/2 + 6 = 9.25.
    let decayed = FeatureReducer::DecayedSum { half_life_ms: 1_000, as_of_ms: 4_000 };
    assert_eq!(t(decayed.clone(), Nearest, 100), 925);
    assert_eq!(t(decayed.clone(), Floor, 1), 9);
    assert_eq!(t(decayed, Ceil, 1), 10);
    // 2, then 0.5*4 + 0.5*2 = 3, then 0.5*4 + 0.5*3 = 3.5, then 0.5*6 + 0.5*3.5 = 4.75.
    assert_eq!(t(FeatureReducer::Ewma { alpha: 0.5 }, Nearest, 100), 475);
    // 16 over a five-second window.
    assert_eq!(t(FeatureReducer::Rate, Nearest, 1_000), 3_200);
    // Three of the four numbers are at least 4.
    assert_eq!(t(FeatureReducer::Ratio { at_least: 4 }, Nearest, 1_000_000), 750_000);
    // The Wilson lower bound for 3 of 4 at z = 1.96: centre 1.2302, margin 0.64083, divisor 1.9604,
    // so 0.30064. At z = 0 it is the plain share, 0.75.
    assert_eq!(t(FeatureReducer::Wilson { at_least: 4, z: 1.96 }, Nearest, 10_000), 3_006);
    assert_eq!(t(FeatureReducer::Wilson { at_least: 4, z: 1.96 }, Ceil, 10_000), 3_007);
    assert_eq!(t(FeatureReducer::Wilson { at_least: 4, z: 0.0 }, Nearest, 100), 75);
    // The newest three numbers, oldest first, are 4, 4, 6: 4*0.5 + 4*1 + 6*2 = 18.
    assert_eq!(t(FeatureReducer::Weighted { weights: vec![0.5, 1.0, 2.0] }, Nearest, 1), 18);
    // Five weights need five numbers and there are four, so there is no answer: 0.
    assert_eq!(t(FeatureReducer::Weighted { weights: vec![1.0; 5] }, Nearest, 1), 0);
}

#[test]
fn the_count_cap_and_an_empty_window_apply_to_a_typed_query() {
    let (engine, _dir) = engine_with_series();
    // The first two numbers only: (2 + 4) / 2.
    let capped = engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: typed_command(FeatureReducer::Mean, Nearest, 1_000, Some(2)),
        })
        .response;
    assert_eq!(capped, CommandResponse::Aggregate { value: 3_000 });
    // A window that holds nothing, and a key that does not exist, answer 0.
    let empty_window = Command::FeatureReduceQuery {
        key: "k".to_string(),
        start_ms: 10_000,
        end_ms: 20_000,
        reducer: FeatureReducer::Mean,
        rounding: Nearest,
        scale: 1_000,
        count: None,
    };
    let absent_key = Command::FeatureReduceQuery {
        key: "no-such-key".to_string(),
        start_ms: 0,
        end_ms: END_MS,
        reducer: FeatureReducer::Ewma { alpha: 0.5 },
        rounding: Nearest,
        scale: 1_000,
        count: None,
    };
    for command in [empty_window, absent_key] {
        assert_eq!(
            engine.execute(ExecuteRequest { shard_id: 1, command }).response,
            CommandResponse::Aggregate { value: 0 }
        );
    }
}

/// The name-based answers on the same series, pinned. These are what the query answered before
/// the typed form existed.
#[test]
fn the_name_based_aggregate_answers_what_it_always_did() {
    let (engine, _dir) = engine_with_series();
    assert_eq!(named(&engine, "sum"), 16);
    assert_eq!(named(&engine, "avg"), 4);
    assert_eq!(named(&engine, "average"), 4);
    assert_eq!(named(&engine, "min"), 2);
    assert_eq!(named(&engine, "max"), 6);
    assert_eq!(named(&engine, "first"), 2);
    assert_eq!(named(&engine, "last"), 6);
    // Count is the number of VALUES, the non-number included.
    assert_eq!(named(&engine, "count"), 5);
    assert_eq!(named(&engine, ""), 5);
    // An unknown name is lenient on this door: it counts.
    assert_eq!(named(&engine, "p99"), 5);
}

#[test]
fn a_bad_parameter_is_refused_naming_its_field() {
    let (engine, _dir) = engine_with_series();
    let refused = |reducer, scale| {
        refusal(&engine, typed_command(reducer, Nearest, scale, None))
            .expect("a bad parameter must be refused")
    };
    assert!(refused(FeatureReducer::Mean, 0).contains("scale"));
    assert!(refused(FeatureReducer::DecayedSum { half_life_ms: 0, as_of_ms: 1 }, 1)
        .contains("half_life_ms"));
    for alpha in [0.0, -0.5, 1.5, f64::NAN, f64::INFINITY] {
        assert!(refused(FeatureReducer::Ewma { alpha }, 1).contains("alpha"), "alpha {alpha}");
    }
    for z in [-1.0, f64::NAN, f64::INFINITY] {
        assert!(refused(FeatureReducer::Wilson { at_least: 1, z }, 1).contains("z must"), "z {z}");
    }
    assert!(refused(FeatureReducer::Weighted { weights: vec![] }, 1).contains("weights"));
    let bad_third = refused(FeatureReducer::Weighted { weights: vec![1.0, 2.0, f64::NAN] }, 1);
    assert!(bad_third.contains("weights[2]"), "{bad_third}");

    // CONTROLS: the edge of every accepted range is accepted, so the refusals above are not the
    // validator refusing everything.
    for reducer in [
        FeatureReducer::Ewma { alpha: 1.0 },
        FeatureReducer::Wilson { at_least: 1, z: 0.0 },
        FeatureReducer::DecayedSum { half_life_ms: 1, as_of_ms: 0 },
    ] {
        assert_eq!(refusal(&engine, typed_command(reducer, Nearest, 1, None)), None);
    }
}

/// The one new limit: at most `MAX_REDUCER_WEIGHTS` weights. One past it is refused, the limit
/// itself is accepted, and an accepted query with that many weights answers.
#[test]
fn the_weights_limit_refuses_one_past_and_accepts_the_limit() {
    let (engine, _dir) = engine_with_series();
    let with = |count: usize| {
        typed_command(FeatureReducer::Weighted { weights: vec![1.0; count] }, Nearest, 1, None)
    };
    let message = refusal(&engine, with(MAX_REDUCER_WEIGHTS + 1))
        .expect("one weight past the limit must be refused");
    assert!(
        message.contains("weights") && message.contains(&MAX_REDUCER_WEIGHTS.to_string()),
        "the refusal must name the field and the limit: {message}"
    );
    assert_eq!(refusal(&engine, with(MAX_REDUCER_WEIGHTS)), None);
    assert_eq!(MAX_REDUCER_WEIGHTS, 64, "the documented limit");
    // A series with MAX_REDUCER_WEIGHTS numbers answers the weighted sum of all of them.
    let points = (0..MAX_REDUCER_WEIGHTS as u64)
        .map(|offset| FeaturePoint { timestamp_ms: 10_000 + offset, value: b"1".to_vec() })
        .collect();
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend { key: "wide".to_string(), points },
    });
    assert!(outcome.status.ok);
    let answer = engine
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureReduceQuery {
                key: "wide".to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                reducer: FeatureReducer::Weighted { weights: vec![0.5; MAX_REDUCER_WEIGHTS] },
                rounding: Nearest,
                scale: 1,
                count: None,
            },
        })
        .response;
    assert_eq!(answer, CommandResponse::Aggregate { value: 32 });
}

/// The wire form: parameters are named fields, and `rounding`, `scale` and `count` may be left out.
#[test]
fn a_typed_query_reads_from_named_fields() {
    let text = r#"{"kind":"feature_reduce_query","key":"k","start_ms":0,"end_ms":4999,
        "reducer":{"reducer":"ewma","alpha":0.5},"scale":100}"#;
    let command: Command = serde_json::from_str(text).expect("the typed form must parse");
    assert_eq!(
        command,
        typed_command(FeatureReducer::Ewma { alpha: 0.5 }, Nearest, 100, None)
    );
    let (engine, _dir) = engine_with_series();
    assert_eq!(
        engine.execute(ExecuteRequest { shard_id: 1, command }).response,
        CommandResponse::Aggregate { value: 475 }
    );
    // A parameter that is not a number is a parse error, not a reducer with a default.
    let bad = r#"{"kind":"feature_reduce_query","key":"k","start_ms":0,"end_ms":1,
        "reducer":{"reducer":"ewma","alpha":"high"}}"#;
    assert!(serde_json::from_str::<Command>(bad).is_err());
}
