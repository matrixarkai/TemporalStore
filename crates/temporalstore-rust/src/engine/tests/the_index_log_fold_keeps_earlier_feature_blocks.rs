// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE FOLD OF THE INDEX LOG KEEPS THE BLOCKS OF AN EARLIER APPEND.
//!
//! A feature append is logged as an upsert record: one item per block it wrote. Folding such an item
//! drops the blocks it supersedes, and a kind that converges on the OBJECT rather than on the block
//! (`index_entry_names_a_block` answers false for the feature kind) supersedes every block the
//! object holds. So a fold over a log holding two appends to one key keeps only the second append's
//! block, and the series it rebuilds is missing every point of the first.
//!
//! The fold runs under `TS_WAL_LEGACY_RECOVERY` and through `load_index_checked`; the default
//! recovery loads the base and replays the log, which is why nothing had noticed.
//!
//! The test appends two blocks of points to one key and requires the series a fold over the log
//! rebuilds to be the series the engine holds, instant for instant.

#![allow(clippy::all)]
use super::*;
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

#[test]
fn the_fold_keeps_the_points_of_an_earlier_append_after_a_second_one() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    append(&engine, &[1_300, 1_350, 1_540, 1_660]);
    assert_eq!(live(&engine), folded(&engine), "after the first append the fold and the engine agree");
    append(&engine, &[1_090, 1_460]);
    let held = live(&engine);
    assert_eq!(held, vec![1_090, 1_300, 1_350, 1_460, 1_540, 1_660], "the engine holds both appends");
    let log: BTreeMap<usize, String> = engine
        .index_log_store()
        .read_delta_records(1, 0)
        .expect("records")
        .iter()
        .enumerate()
        .map(|(at, record)| (at, format!("seq {} upsert {} items {} key_states {}", record.sequence, record.upsert, record.items.len(), record.key_states.len())))
        .collect();
    assert_eq!(
        folded(&engine),
        held,
        "after the second append the fold rebuilt another series than the engine holds; the log is {log:?}"
    );
}
