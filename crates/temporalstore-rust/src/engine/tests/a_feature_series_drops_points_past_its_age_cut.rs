// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A feature series with `feature_max_age_ms` set drops the points older than that before the
//! newest it holds, at the write that touches it; without the key nothing changes.
//!
//! The cut is measured from the newest point IN THE SERIES, not from a clock, so a replayed write
//! reaches the same series. It writes no bytes of its own: the points it drops go through the trim
//! the retained-points cap already uses.

#![allow(clippy::all)]
use super::*;
use crate::control::{Config, SetConfigRequest};
use std::collections::BTreeMap;

const KEY: &str = "series";

fn open(dir: &std::path::Path, age: Option<&str>) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    let mut extend_config = BTreeMap::new();
    if let Some(age) = age {
        extend_config.insert("feature_max_age_ms".to_string(), age.to_string());
    }
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config { version: 2, feature_max_size: 1_000, extend_config, ..Default::default() },
            })
            .ok
    );
    engine
}

fn append(engine: &TemporalEngine, stamps: &[u64]) {
    let points = stamps
        .iter()
        .map(|at| FeaturePoint { timestamp_ms: *at, value: format!("v{at}").into_bytes() })
        .collect();
    let response = engine.execute(ExecuteRequest { shard_id: 1, command: Command::FeatureAppend { key: KEY.to_string(), points } });
    assert!(response.status.ok, "{:?}", response.status);
}

fn stamps(engine: &TemporalEngine) -> Vec<u64> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQuery { key: KEY.to_string(), start_ms: 0, end_ms: u64::MAX, count: None },
    });
    match response.response {
        CommandResponse::FeaturePoints { points } => points.iter().map(|point| point.timestamp_ms).collect(),
        other => panic!("not points: {other:?}"),
    }
}

/// THE BEHAVIOUR: points older than the cut, before the newest, are gone after the write.
#[test]
fn a_write_drops_the_points_older_than_the_age_cut() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), Some("1000"));
    append(&engine, &[1_000, 1_500, 2_000]);
    assert_eq!(stamps(&engine), vec![1_000, 1_500, 2_000], "nothing is older than 1,000 ms before 2,000");
    append(&engine, &[3_500]);
    // The floor is 3,500 - 1,000 = 2,500: the three older points go, the newest stays.
    assert_eq!(stamps(&engine), vec![3_500]);
    append(&engine, &[3_000, 4_000, 4_400]);
    assert_eq!(stamps(&engine), vec![3_500, 4_000, 4_400], "3,000 is below the new floor 3,400 and is dropped");
}

/// THE DIFFERENTIAL: with no key, a zero, or a value that does not parse, the series is what it was
/// before the key existed.
#[test]
fn without_a_usable_age_the_series_keeps_every_point() {
    let writes: [&[u64]; 3] = [&[1_000, 2_000], &[10_000_000], &[500, 20_000_000]];
    let mut kept = Vec::new();
    for age in [None, Some("0"), Some("not a number")] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), age);
        for write in writes {
            append(&engine, write);
        }
        kept.push(stamps(&engine));
    }
    assert_eq!(kept[0], vec![500, 1_000, 2_000, 10_000_000, 20_000_000], "the unconfigured series dropped something");
    assert_eq!(kept[0], kept[1]);
    assert_eq!(kept[0], kept[2]);
}

/// The newest point is never dropped, whatever the age, and a series inside the cut is untouched.
#[test]
fn the_newest_point_always_stays() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), Some("1"));
    append(&engine, &[10, 20, 30]);
    assert_eq!(stamps(&engine), vec![30]);
}
