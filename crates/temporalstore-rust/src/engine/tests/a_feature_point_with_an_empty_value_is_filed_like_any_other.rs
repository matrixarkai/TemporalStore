// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A FEATURE POINT WITH AN EMPTY VALUE LEAVES THE SHARD INDEX WRITABLE, AND A DELETE AFTER IT FINDS
//! THE BLOCKS IT MARKS.
//!
//! The conformance oracle fails on seed 24 in four steps, the last two being a `FeatureAppend` that
//! carries a point with an empty value and a `FeatureDelete` of the same key; the no-panic harness
//! fails on seed 2 with a point of the same shape in its last steps. Both end in the same state: a
//! live block sits in `bucket_map` while the object lookup names nothing, so the delete's mark
//! `debug_assert!`s and the shard index refuses to serialise ("holds a block whose stored key would
//! name no object").
//!
//! THREE CASES, EACH SEPARABLE, AND A CONTROL.
//!
//!   1. an append with an empty-valued point: the index must still serialise;
//!   2. a delete after it: the mark must find the blocks it is told to mark;
//!   3. CONTROL: the same append with a non-empty value must pass both, so a failure above is the
//!      empty value and not the fixture.
#![allow(clippy::all)]
use super::*;

fn engine_in(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn append(engine: &TemporalEngine, points: Vec<FeaturePoint>) {
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "f0".to_string(),
            points,
        },
    });
}

fn point(timestamp_ms: u64, value: &[u8]) -> FeaturePoint {
    FeaturePoint {
        timestamp_ms,
        value: value.to_vec(),
    }
}

/// The shard index serialises, which is what every persist of it needs.
fn index_serialises(engine: &TemporalEngine) -> Result<(), String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    serde_json::to_value(shard).map(|_| ()).map_err(|err| err.to_string())
}

#[test]
fn an_append_with_an_empty_valued_point_leaves_the_index_writable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_in(dir.path());
    append(&engine, vec![point(31, &[]), point(54, &[119])]);
    if let Err(err) = index_serialises(&engine) {
        panic!("the index cannot be written after an append with an empty-valued point: {err}");
    }
}

#[test]
fn a_delete_after_an_empty_valued_append_finds_the_blocks_it_marks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_in(dir.path());
    append(&engine, vec![point(31, &[]), point(54, &[119])]);
    // The delete's mark asserts, in a debug build, that a live block is never resident while the
    // object lookup names nothing. Reaching the end of this call IS the assertion.
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureDelete {
            key: "f0".to_string(),
        },
    });
    if let Err(err) = index_serialises(&engine) {
        panic!("the index cannot be written after the delete: {err}");
    }
}

/// The four steps the oracle fails on at seed 24, exactly as the harness ran them. If the three
/// cases above pass and this fails, the trigger is one of the other two steps and not the empty
/// feature value.
#[test]
fn the_four_steps_the_oracle_fails_on_replay_exactly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_in(dir.path());
    let run = |command: Command| {
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
    };
    run(Command::ControlStateChangeAdd {
        key: "g3".to_string(),
        timestamp_ms: 33,
        value: Vec::new(),
        precision_ms: None,
        ttl_ms: None,
    });
    run(Command::CommonExpire {
        key: "h2".to_string(),
        ttl_ms: 1099511627776,
    });
    run(Command::FeatureAppend {
        key: "f0".to_string(),
        points: vec![point(31, &[]), point(54, &[119])],
    });
    run(Command::FeatureDelete {
        key: "f0".to_string(),
    });
    if let Err(err) = index_serialises(&engine) {
        panic!("the index cannot be written after the four oracle steps: {err}");
    }
}

/// THE QUESTION THAT SAYS WHETHER THIS IS A DEBUG-ONLY ASSERTION OR A DEFECT: in a shard that holds
/// OTHER objects, so the mark's assertion is silent (something still names a block), does deleting
/// a feature leave the index unwritable? The index writer's refusal is not a `debug_assert!`; it is
/// returned in release builds too.
#[test]
fn deleting_a_feature_beside_other_objects_leaves_the_index_writable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_in(dir.path());
    let run = |command: Command| {
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
    };
    // Something else that names a block, so the shard is not the empty case.
    run(Command::StringSet {
        key: "s1".to_string(),
        value: vec![b'v'; 64],
    });
    run(Command::FeatureAppend {
        key: "f0".to_string(),
        points: vec![point(31, &[7]), point(54, &[119])],
    });
    index_serialises(&engine).expect("before the delete the index must be writable");
    run(Command::FeatureDelete {
        key: "f0".to_string(),
    });
    if let Err(err) = index_serialises(&engine) {
        panic!("the index cannot be written after deleting a feature beside another object: {err}");
    }
}

#[test]
fn control_the_same_append_with_a_non_empty_value_passes_both() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_in(dir.path());
    append(&engine, vec![point(31, &[7]), point(54, &[119])]);
    index_serialises(&engine).expect("a non-empty append must leave the index writable");
    let _ = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureDelete {
            key: "f0".to_string(),
        },
    });
    index_serialises(&engine).expect("a delete after a non-empty append must leave it writable");
}
