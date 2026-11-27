// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A RECOVERY THAT FOLDS THE INDEX LOG REBUILDS, AFTER EVERY WRITE.
//!
//! The fold of the index log is the only reader of the per-key state a write that trims files. This is
//! the oracle for any change to that record: after each of 160 writes (all four writers, trims,
//! replaces, interleaved and range-replaced instants), over three seeds, the series the fold rebuilds
//! is the series the engine holds, instant for instant and block for block.
//!
//! It also covers the reader of the key-state edit: a blob that names only the instants that left a
//! series and the points that joined it is applied to the series a fold rebuilt, and nothing else.
//!
//! The fold runs under `TS_WAL_LEGACY_RECOVERY` and through `load_index_checked`; the default recovery
//! loads the base and replays the log.

#![allow(clippy::all)]
use super::a_feature_series_at_its_cap_files_only_what_changed::{open, run, step, Rng, KEY};
use super::*;
use crate::engine::storage_bucket_internals::block_physical_identity_key;
use crate::engine::timestamped_series::TimestampedSeries as _;
use std::collections::BTreeMap;

type Identity = String;

fn live_series(engine: &TemporalEngine) -> Option<BTreeMap<u64, Identity>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards.get(&1)?.features().get(KEY).map(|series| {
        series.points().map(|(at, address)| (at, format!("{:?}", block_physical_identity_key(address)))).collect()
    })
}

fn folded_series(engine: &TemporalEngine) -> Option<BTreeMap<u64, Identity>> {
    let state = engine.load_index_checked(1, false).expect("the log folds")?;
    state.features().get(KEY).map(|series| {
        series.points().map(|(at, address)| (at, format!("{:?}", block_physical_identity_key(address)))).collect()
    })
}

#[test]
fn the_fold_of_the_index_log_rebuilds_the_series_the_engine_holds_after_every_write() {
    const BOUND: usize = 40;
    let (mut writes, mut trims) = (0usize, 0usize);
    for seed in 1..=3u64 {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), BOUND);
        let mut rng = Rng(seed);
        let mut model = BTreeMap::new();
        let mut newest = 1_000 + 90 * 10;
        for n in 0..160usize {
            let before = model.len();
            let command = step(&mut rng, n, &mut newest, &mut model, BOUND);
            let label: String = format!("{command:?}").chars().take(100).collect();
            let full_command: String = format!("{command:?}");
            let records_before = engine.index_log_store().read_delta_records(1, 0).expect("records").len();
            let held_before: Vec<u64> = live_series(&engine).map(|m| m.keys().copied().collect()).unwrap_or_default();
            run(&engine, command);
            let records_after = engine.index_log_store().read_delta_records(1, 0).expect("records").len();
            let live = live_series(&engine);
            // The fold itself can panic on its own consistency checks; name the write when it does.
            let folded = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| folded_series(&engine))) {
                Ok(folded) => folded,
                Err(payload) => {
                    let message = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
                        .unwrap_or_default();
                    let records = engine.index_log_store().read_delta_records(1, 0).expect("records");
                    let summary: Vec<String> = records
                        .iter()
                        .map(|record| {
                            format!(
                                "seq {} upsert {} items {} deleted {} key_states {} features {}",
                                record.sequence,
                                record.upsert,
                                record.items.len(),
                                record.items.iter().filter(|item| item.deleted).count(),
                                record.key_states.len(),
                                record.key_states.iter().filter(|blob| blob.get("features").is_some()).count()
                            )
                        })
                        .collect();
                    panic!(
                        "THE FOLD ITSELF PANICKED at seed {seed}, write {n} ({}): {message}; records so far: {summary:?}; records this write added {}",
                        full_command.chars().take(300).collect::<String>(),
                        records_after - records_before
                    );
                }
            };
            if live != folded {
                let (l, f) = (live.clone().unwrap_or_default(), folded.clone().unwrap_or_default());
                let only_live: Vec<_> = l.keys().filter(|k| !f.contains_key(k)).collect();
                let only_folded: Vec<_> = f.keys().filter(|k| !l.contains_key(k)).collect();
                let other_block: Vec<_> = l.iter().filter(|(k, v)| f.get(k).map_or(false, |w| w != *v)).map(|(k, _)| k).collect();
                let records = engine.index_log_store().read_delta_records(1, 0).expect("records");
                let last: Vec<String> = records
                    .iter()
                    .rev()
                    .take(3)
                    .map(|record| {
                        format!(
                            "seq {} upsert {} items {} (deleted {}) key_states {} with features {}",
                            record.sequence,
                            record.upsert,
                            record.items.len(),
                            record.items.iter().filter(|item| item.deleted).count(),
                            record.key_states.len(),
                            record.key_states.iter().filter(|blob| blob.get("features").is_some()).count()
                        )
                    })
                    .collect();
                let (live_before, live_after) = (l.len(), f.len());
                eprintln!("ORACLE last records, newest first: {last:?}; live {live_before} fold {live_after}");
                eprintln!(
                    "ORACLE this write added {} record(s); the command was {}; the engine held before it {:?}",
                    records_after - records_before,
                    full_command.chars().take(1500).collect::<String>(),
                    held_before
                );
                eprintln!("ORACLE the engine holds {:?}; the fold holds {:?}", l.keys().collect::<Vec<_>>(), f.keys().collect::<Vec<_>>());
                panic!(
                    "seed {seed}, write {n} ({label}): live holds {} points, the fold {}; only in live {only_live:?}; only in fold {only_folded:?}; same instant, other block {other_block:?}",
                    l.len(),
                    f.len()
                );
            }
            writes += 1;
            if model.len() == BOUND && before == BOUND {
                trims += 1;
            }
        }
    }
    // THE CONTROL: the sequence reached the cap and trimmed, so the comparison covered trims.
    assert!(trims > 100, "only {trims} of {writes} writes were at the cap");
}

/// A key-state edit is applied to the series a fold rebuilt: the removed instants go, the added points
/// join, the rest stay.
#[test]
fn a_features_edit_removes_and_adds_points_in_the_series_a_fold_rebuilt() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), 1_000);
    for stamp in [100u64, 200, 300] {
        run(
            &engine,
            Command::FeatureAppend {
                key: KEY.to_string(),
                points: vec![FeaturePoint { timestamp_ms: stamp, value: format!("v{stamp}").into_bytes() }],
            },
        );
    }
    let mut state = engine.load_index_checked(1, false).expect("the log folds").expect("a shard");
    let (address_of_300, held): (serde_json::Value, Vec<u64>) = {
        let series = state.features().get(KEY).expect("the fold rebuilt the series");
        let address = series.points().find(|(at, _)| *at == 300).map(|(_, address)| address.clone()).unwrap();
        (serde_json::to_value(address).unwrap(), series.points().map(|(at, _)| at).collect())
    };
    assert_eq!(held, vec![100, 200, 300]);
    // Take 100 out, put 400 in at the address 300 holds, and say nothing about the rest.
    let blob = serde_json::json!({
        "key": KEY,
        "features_edit": { "removed": [100], "added": { "400": address_of_300 } },
    });
    crate::engine::apply_key_states(&mut state, &[blob]);
    let after: Vec<u64> = state.features().get(KEY).expect("the series is still there").points().map(|(at, _)| at).collect();
    assert_eq!(after, vec![200, 300, 400]);
    // An edit that empties the series takes the key with it.
    let blob = serde_json::json!({ "key": KEY, "features_edit": { "removed": [200, 300, 400] } });
    crate::engine::apply_key_states(&mut state, &[blob]);
    assert!(state.features().get(KEY).is_none(), "an emptied series leaves the key behind");
}
