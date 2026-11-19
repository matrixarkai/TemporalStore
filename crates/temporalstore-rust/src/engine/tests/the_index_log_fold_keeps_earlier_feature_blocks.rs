// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE FOLD OF THE INDEX LOG KEEPS THE BLOCKS OF AN EARLIER APPEND.
//!
//! A feature append is logged as an upsert record: one item per block it wrote. Folding such an item
//! drops the blocks it supersedes. A kind that converges on the OBJECT rather than on the block
//! superseded every block the object holds, and the feature kind did (`index_entry_names_a_block`
//! answers false for it), so a fold over a log holding two appends to one key kept only the second
//! append's blocks and rebuilt a series missing every point of the first.
//!
//! The live write never supersedes a feature block that way: it files the blocks it wrote and retires
//! the ones its replaced points left empty. The fold now does the same, an item superseding only the
//! block at its own address.
//!
//! The fold runs under `TS_WAL_LEGACY_RECOVERY` and through `load_index_checked`; the default
//! recovery loads the base and replays the log, which is why nothing had noticed.
//!
//! Each test compares the series a fold over the log rebuilds with the series the engine holds,
//! instant for instant and block for block.

#![allow(clippy::all)]
use super::*;
use crate::engine::storage_bucket_internals::block_physical_identity_key;
use crate::engine::timestamped_series::TimestampedSeries as _;
use std::collections::BTreeMap;

const KEY: &str = "k";

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn append(engine: &TemporalEngine, stamps: &[u64]) {
    let points = stamps
        .iter()
        .map(|at| FeaturePoint { timestamp_ms: *at, value: format!("v{at}").into_bytes() })
        .collect();
    let response =
        engine.execute(ExecuteRequest { shard_id: 1, command: Command::FeatureAppend { key: KEY.to_string(), points } });
    assert!(response.status.ok, "{:?}", response.status);
}

fn live(engine: &TemporalEngine) -> Vec<u64> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards
        .get(&1)
        .and_then(|shard| shard.features().get(KEY))
        .map(|series| series.points().map(|(at, _)| at).collect())
        .unwrap_or_default()
}

fn folded(engine: &TemporalEngine) -> Vec<u64> {
    let state = engine.load_index_checked(1, false).expect("the log folds").expect("the log holds a shard");
    state
        .features()
        .get(KEY)
        .map(|series| series.points().map(|(at, _)| at).collect())
        .unwrap_or_default()
}

fn live_blocks(engine: &TemporalEngine) -> BTreeMap<u64, String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards
        .get(&1)
        .and_then(|shard| shard.features().get(KEY))
        .map(|series| series.points().map(|(at, address)| (at, format!("{:?}", block_physical_identity_key(address)))).collect())
        .unwrap_or_default()
}

fn folded_blocks(engine: &TemporalEngine) -> BTreeMap<u64, String> {
    let state = engine.load_index_checked(1, false).expect("the log folds").expect("the log holds a shard");
    state
        .features()
        .get(KEY)
        .map(|series| series.points().map(|(at, address)| (at, format!("{:?}", block_physical_identity_key(address)))).collect())
        .unwrap_or_default()
}

#[test]
fn the_fold_keeps_the_points_of_an_earlier_append_after_a_second_one() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    append(&engine, &[1_300, 1_350, 1_540, 1_660]);
    assert_eq!(live(&engine), folded(&engine), "after the first append the fold and the engine agree");
    append(&engine, &[1_090, 1_460]);
    let held = live(&engine);
    assert_eq!(held, vec![1_090, 1_300, 1_350, 1_460, 1_540, 1_660], "the engine holds both appends");
    assert_eq!(folded(&engine), held, "after the second append the fold rebuilt another series than the engine holds");
}

/// A point replaced by a later append, and an append between two earlier ones: the fold holds the
/// series, and the blocks the points name, that the engine holds.
#[test]
fn a_replaced_point_and_an_interleaved_append_fold_to_the_series_the_engine_holds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    append(&engine, &[1_000, 2_000, 3_000]);
    append(&engine, &[2_000]);
    append(&engine, &[1_500, 2_500]);
    append(&engine, &[500, 3_500]);
    assert_eq!(live(&engine), vec![500, 1_000, 1_500, 2_000, 2_500, 3_000, 3_500]);
    assert_eq!(folded(&engine), live(&engine), "the points differ");
    assert_eq!(folded_blocks(&engine), live_blocks(&engine), "a point names another block in the fold");
}
