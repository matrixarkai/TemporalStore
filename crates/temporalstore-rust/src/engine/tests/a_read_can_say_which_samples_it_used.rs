// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A READ THAT ASKS FOR IT SAYS WHICH SAMPLES IT USED AND HOW MANY CELLS EACH HELD, THROUGH THE
//! ENGINE.
//!
//! Compared with what the fixture stored, live and after a restart that reads every block cold,
//! for a one-key read, a stored-cell read and a merged read over two keys. A read that does not ask
//! answers what it always answered, byte for byte in its JSON.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::engine::cell_set::{
    encode_key, encode_value, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType, EntityValue,
    KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{RankedCells, WindowReduceRankSpec, WindowReduceSampleInfo, WindowReduceScore, WindowReduceWindow};

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn exec(engine: &TemporalEngine, command: Command) -> ExecuteResponse {
    engine.execute(ExecuteRequest { shard_id: 1, command })
}

fn key_spec() -> KeySpec {
    KeySpec {
        namespace: "ns".to_string(),
        entity: EntityType::U64,
        dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
    }
}

fn stored_key(entity: u64) -> String {
    encode_key(&key_spec(), &CellKey { entity: EntityValue::U64(entity), dims: vec![Some(DimValue::U16(3))] }).unwrap()
}

fn two_lanes() -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: key_spec(),
        labels: vec!["slot".to_string()],
        lanes: vec![
            LaneSpec { name: "shown".to_string(), reducer: LaneReducer::Sum },
            LaneSpec { name: "peak".to_string(), reducer: LaneReducer::Max },
        ],
    }
}

fn cell(member: u64, slot: u64, lanes: Vec<i64>) -> Cell {
    Cell { member: Member::U64(member), labels: vec![slot], lanes }
}

fn put(engine: &TemporalEngine, key: &str, timestamp_ms: u64, cells: &[Cell]) {
    let response = exec(
        engine,
        Command::FeatureAppend {
            key: key.to_string(),
            points: vec![FeaturePoint { timestamp_ms, value: encode_value(&two_lanes(), cells).unwrap() }],
        },
    );
    assert!(response.status.ok, "{:?}", response.status);
}

/// Four samples holding 3, 3, 2 and 2 cells.
fn seed(engine: &TemporalEngine, key: &str) {
    put(engine, key, 100, &[cell(1, 1, vec![10, 5]), cell(2, 2, vec![20, 7]), cell(3, 1, vec![100, 2])]);
    put(engine, key, 200, &[cell(1, 1, vec![10, 9]), cell(2, 3, vec![30, 3]), cell(4, 2, vec![8, 1])]);
    put(engine, key, 300, &[cell(1, 2, vec![10, 4]), cell(3, 1, vec![100, 6])]);
    put(engine, key, 400, &[cell(2, 2, vec![10, 8]), cell(4, 2, vec![2, 2])]);
}

fn spec(window: WindowReduceWindow, score: WindowReduceScore, sample_info: bool) -> WindowReduceRankSpec {
    WindowReduceRankSpec { sample_info, ..WindowReduceRankSpec::new(window, score, 10) }
}

fn shown() -> WindowReduceScore {
    WindowReduceScore::Lane { lane: "shown".to_string() }
}

fn ranked_of(response: ExecuteResponse) -> RankedCells {
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::WindowReduceRanking { ranked } => ranked,
        other => panic!("not a ranking: {other:?}"),
    }
}

fn rank(engine: &TemporalEngine, key: &str, spec: WindowReduceRankSpec) -> RankedCells {
    ranked_of(exec(engine, Command::WindowReduceRank { key: key.to_string(), spec }))
}

fn used(ranked: &RankedCells) -> Vec<(u64, usize)> {
    ranked.sample_info.iter().map(|s: &WindowReduceSampleInfo| (s.timestamp_ms, s.cells)).collect()
}

#[test]
fn a_read_that_asks_says_which_samples_it_used_and_how_many_cells_each_held_live_and_after_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(31);
    seed(&engine, &key);

    let all = WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX };
    let checks = |engine: &TemporalEngine| {
        // Every sample, oldest first, with the cells it holds.
        assert_eq!(used(&rank(engine, &key, spec(all.clone(), shown(), true))), vec![(100, 3), (200, 3), (300, 2), (400, 2)]);
        // The window is the read's: only the samples it names.
        let between = WindowReduceWindow::Between { start_ms: 200, end_ms: 300 };
        assert_eq!(used(&rank(engine, &key, spec(between, shown(), true))), vec![(200, 3), (300, 2)]);
        assert_eq!(
            used(&rank(engine, &key, spec(WindowReduceWindow::LastN { n: 2 }, shown(), true))),
            vec![(300, 2), (400, 2)]
        );
        // A window that names no sample says so, with an empty list.
        let none = WindowReduceWindow::Between { start_ms: 500, end_ms: 600 };
        assert!(rank(engine, &key, spec(none, shown(), true)).sample_info.is_empty());
        // Stored cells are one row each, and the summary is the same samples.
        let recent = rank(engine, &key, spec(all.clone(), WindowReduceScore::Recent, true));
        assert_eq!(used(&recent), vec![(100, 3), (200, 3), (300, 2), (400, 2)]);
        assert_eq!(recent.rows.len(), 10);
        // The cells are counted before the predicates apply: a predicate that keeps one member
        // leaves the sample sizes as they were.
        let mut filtered = spec(all.clone(), shown(), true);
        filtered.filters = vec![crate::types::FeatureFilter {
            field: "member".to_string(),
            op: crate::types::FeatureFilterOp::Equal,
            value: 1,
        }];
        let filtered = rank(engine, &key, filtered);
        assert_eq!(filtered.rows.len(), 1);
        assert_eq!(used(&filtered), vec![(100, 3), (200, 3), (300, 2), (400, 2)]);
    };
    checks(&engine);
    drop(engine);
    // A restart reads every block cold.
    checks(&engine_on(dir.path()));
}

#[test]
fn a_read_that_does_not_ask_answers_what_it_always_answered_and_its_json_has_no_new_field() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(32);
    seed(&engine, &key);
    let all = WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX };

    let plain = rank(&engine, &key, spec(all.clone(), shown(), false));
    assert!(plain.sample_info.is_empty());
    let text = serde_json::to_string(&plain).unwrap();
    assert!(!text.contains("sample_info"), "{text}");
    // Asking changes nothing about the rows, only adds the summary.
    let asked = rank(&engine, &key, spec(all.clone(), shown(), true));
    assert_eq!(RankedCells { sample_info: Vec::new(), ..asked.clone() }, plain);
    assert!(serde_json::to_string(&asked).unwrap().contains(r#""sample_info":[{"timestamp_ms":100,"cells":3}"#));
    // The request that asks is one more member of the spec's encoding, left out when it does not.
    let off = serde_json::to_string(&spec(all.clone(), shown(), false)).unwrap();
    let on = serde_json::to_string(&spec(all, shown(), true)).unwrap();
    assert!(!off.contains("sample_info"), "{off}");
    assert!(on.ends_with(r#","sample_info":true}"#), "{on}");
    assert_eq!(on.replace(r#","sample_info":true"#, ""), off);
}

#[test]
fn a_merged_read_says_the_samples_of_every_key_in_stamp_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let (a, b) = (stored_key(41), stored_key(42));
    seed(&engine, &a);
    put(&engine, &b, 150, &[cell(1, 1, vec![1, 1]), cell(5, 1, vec![7, 1])]);
    put(&engine, &b, 350, &[cell(5, 1, vec![9, 2])]);

    let merged = |window: WindowReduceWindow, asked: bool| {
        ranked_of(exec(
            &engine,
            Command::WindowReduceFanInRank { keys: vec![a.clone(), b.clone()], spec: spec(window, shown(), asked) },
        ))
    };
    let all = WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX };
    let info = merged(all.clone(), true);
    assert_eq!(used(&info), vec![(100, 3), (150, 2), (200, 3), (300, 2), (350, 1), (400, 2)]);
    assert_eq!(info.samples, 6);
    assert_eq!(info.keys, 2);
    // The window is applied to each key before the samples are listed.
    assert_eq!(used(&merged(WindowReduceWindow::LastN { n: 1 }, true)), vec![(350, 1), (400, 2)]);
    // Not asked: nothing listed, and the rows are the same.
    let quiet = merged(all, false);
    assert!(quiet.sample_info.is_empty());
    assert_eq!(RankedCells { sample_info: Vec::new(), ..info }, quiet);
}
