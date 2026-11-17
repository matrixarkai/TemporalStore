// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WINDOW OF THE NEWEST ROWS, THROUGH THE ENGINE.
//!
//! A rows window counts stored (stamp, member) cells instead of samples: the newest `n` of them,
//! whole samples from the newest back, with the sample that crosses `n` giving up only the cells it
//! needs (its largest members). Every answer here is compared with an independent fold over the
//! rows the fixture stored, live and after a restart that reads every block cold, and the bounds
//! are refused one past the limit and accepted at it.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::engine::cell_set::{
    encode_key, encode_value, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType, EntityValue,
    KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::engine::window_reduce_rank::{MAX_WINDOW_ROWS, MAX_WINDOW_SAMPLES};
use crate::types::{CellMemberId, RankedCells, WindowReduceRankSpec, WindowReduceScore, WindowReduceWindow};
use std::collections::BTreeMap;

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

fn header(lanes: &[(&str, LaneReducer)]) -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: key_spec(),
        labels: vec!["slot".to_string()],
        lanes: lanes.iter().map(|(n, r)| LaneSpec { name: n.to_string(), reducer: *r }).collect(),
    }
}

fn two_lanes() -> CellHeader {
    header(&[("shown", LaneReducer::Sum), ("peak", LaneReducer::Max)])
}

fn cell(member: u64, slot: u64, lanes: Vec<i64>) -> Cell {
    Cell { member: Member::U64(member), labels: vec![slot], lanes }
}

fn put(engine: &TemporalEngine, key: &str, header: &CellHeader, timestamp_ms: u64, cells: &[Cell]) {
    let response = exec(
        engine,
        Command::FeatureAppend {
            key: key.to_string(),
            points: vec![FeaturePoint { timestamp_ms, value: encode_value(header, cells).unwrap() }],
        },
    );
    assert!(response.status.ok, "{:?}", response.status);
}

/// Four samples under [shown (Sum), peak (Max)], ten stored rows in all.
fn seed(engine: &TemporalEngine, key: &str) {
    let h = two_lanes();
    put(engine, key, &h, 100, &[cell(1, 1, vec![10, 5]), cell(2, 2, vec![20, 7]), cell(3, 1, vec![100, 2])]);
    put(engine, key, &h, 200, &[cell(1, 1, vec![10, 9]), cell(2, 3, vec![30, 3]), cell(4, 2, vec![8, 1])]);
    put(engine, key, &h, 300, &[cell(1, 2, vec![10, 4]), cell(3, 1, vec![100, 6])]);
    put(engine, key, &h, 400, &[cell(2, 2, vec![10, 8]), cell(4, 2, vec![2, 2])]);
}

/// The rows `seed` stored, oldest first by (stamp, member): (stamp, member, shown, peak).
fn rows() -> Vec<(u64, u64, i64, i64)> {
    vec![
        (100, 1, 10, 5),
        (100, 2, 20, 7),
        (100, 3, 100, 2),
        (200, 1, 10, 9),
        (200, 2, 30, 3),
        (200, 4, 8, 1),
        (300, 1, 10, 4),
        (300, 3, 100, 6),
        (400, 2, 10, 8),
        (400, 4, 2, 2),
    ]
}

/// The independent statement of a rows window: the newest `n` of `rows`, folded per member (the
/// lane's own reduction: a sum of `shown`, a max of `peak`), best first, equal scores by member.
fn expected(n: usize, peak: bool) -> Vec<(u64, i64)> {
    let all = rows();
    let newest = &all[all.len().saturating_sub(n)..];
    let mut by_member: BTreeMap<u64, i64> = BTreeMap::new();
    for &(_, member, shown, top) in newest {
        let value = if peak { top } else { shown };
        by_member
            .entry(member)
            .and_modify(|held| *held = if peak { (*held).max(value) } else { *held + value })
            .or_insert(value);
    }
    let mut ranked: Vec<(u64, i64)> = by_member.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    ranked
}

fn rows_window(n: usize) -> WindowReduceWindow {
    WindowReduceWindow::NewestRows { n }
}

fn lane(name: &str) -> WindowReduceScore {
    WindowReduceScore::Lane { lane: name.to_string() }
}

fn spec(window: WindowReduceWindow, score: WindowReduceScore, top_k: usize) -> WindowReduceRankSpec {
    WindowReduceRankSpec::new(window, score, top_k)
}

fn rank(engine: &TemporalEngine, key: &str, spec: WindowReduceRankSpec) -> RankedCells {
    let response = exec(engine, Command::WindowReduceRank { key: key.to_string(), spec });
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::WindowReduceRanking { ranked } => ranked,
        other => panic!("not a ranking: {other:?}"),
    }
}

fn members(ranked: &RankedCells) -> Vec<u64> {
    ranked
        .rows
        .iter()
        .map(|row| match &row.member {
            CellMemberId::U64(v) => *v,
            CellMemberId::Bytes(_) => 99,
        })
        .collect()
}

fn pairs(ranked: &RankedCells) -> Vec<(u64, i64)> {
    members(ranked).into_iter().zip(ranked.rows.iter().map(|row| row.score as i64)).collect()
}

#[test]
fn a_rows_window_reads_the_newest_stored_rows_and_cuts_the_sample_that_crosses_its_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(11);
    seed(&engine, &key);

    // Hand-computed: the newest 3 rows are the sample at 400 whole and the largest member of the
    // sample at 300 (m3), the sample at 300 being cut to one cell.
    let three = rank(&engine, &key, spec(rows_window(3), lane("shown"), 10));
    assert_eq!(pairs(&three), vec![(3, 100), (2, 10), (4, 2)]);
    assert_eq!(three.samples, 2);
    // 5 rows reach into the sample at 200, which keeps only m4 (8).
    let five = rank(&engine, &key, spec(rows_window(5), lane("shown"), 10));
    assert_eq!(pairs(&five), vec![(3, 100), (1, 10), (2, 10), (4, 10)]);
    assert_eq!(five.samples, 3);
    // 4 rows end exactly at a sample: the sample at 300 is whole, and there are two samples.
    let four = rank(&engine, &key, spec(rows_window(4), lane("shown"), 10));
    assert_eq!(pairs(&four), vec![(3, 100), (1, 10), (2, 10), (4, 2)]);
    assert_eq!(four.samples, 2);
    // CONTROL: a window of the newest two SAMPLES holds the same four rows but is not the 3-row answer.
    let two_samples = rank(&engine, &key, spec(WindowReduceWindow::LastN { n: 2 }, lane("shown"), 10));
    assert_eq!(pairs(&two_samples), pairs(&four));
    assert_ne!(pairs(&two_samples), pairs(&three));

    // Every count against the independent fold, for both reductions, and past the rows the key holds.
    let check = |engine: &TemporalEngine| {
        for n in 1..=12usize {
            for peak in [false, true] {
                let got = pairs(&rank(engine, &key, spec(rows_window(n), lane(if peak { "peak" } else { "shown" }), 10)));
                assert_eq!(got, expected(n, peak), "n = {n}, peak = {peak}");
            }
        }
        // More rows than the key holds is the whole key.
        let all = rank(engine, &key, spec(rows_window(1000), lane("shown"), 10));
        assert_eq!((all.samples, all.refused.as_str()), (4, ""));
    };
    check(&engine);

    drop(engine);
    // A restart reads every block cold.
    let reloaded = engine_on(dir.path());
    check(&reloaded);
    assert_eq!(pairs(&rank(&reloaded, &key, spec(rows_window(3), lane("shown"), 10))), pairs(&three));
}

#[test]
fn a_recent_read_over_a_rows_window_returns_exactly_the_stored_cells_the_window_holds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(12);
    seed(&engine, &key);

    let stamped = |ranked: &RankedCells| -> Vec<(u64, u64)> {
        ranked.rows.iter().zip(members(ranked)).map(|(row, member)| (row.last_ms, member)).collect()
    };
    // The newest sample first, its cells by member; the sample at 300 is cut to its largest member.
    let recent = rank(&engine, &key, spec(rows_window(3), WindowReduceScore::Recent, 10));
    assert_eq!(stamped(&recent), vec![(400, 2), (400, 4), (300, 3)]);
    assert_eq!(recent.rows[2].lanes, vec![100, 6]);
    // The limit of the read caps the rows as for any read.
    assert_eq!(stamped(&rank(&engine, &key, spec(rows_window(3), WindowReduceScore::Recent, 2))), vec![(400, 2), (400, 4)]);
    // More rows than the key holds: all ten, newest first.
    let all = rank(&engine, &key, spec(rows_window(100), WindowReduceScore::Recent, 100));
    assert_eq!(
        stamped(&all),
        vec![(400, 2), (400, 4), (300, 1), (300, 3), (200, 1), (200, 2), (200, 4), (100, 1), (100, 2), (100, 3)]
    );
}

#[test]
fn a_rows_window_is_bounded_at_admission_and_by_the_samples_it_reaches_and_each_bound_is_exact() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(13);
    let h = header(&[("shown", LaneReducer::Sum)]);
    for ts in 1..=(MAX_WINDOW_SAMPLES as u64 + 1) {
        put(&engine, &key, &h, ts, &[cell(1, 0, vec![1])]);
    }

    // At admission: the row limit is served and one past it is refused, and so is a window of no rows.
    let status_of = |window: WindowReduceWindow| {
        exec(&engine, Command::WindowReduceRank { key: key.clone(), spec: spec(window, lane("shown"), 1) }).status
    };
    assert!(status_of(rows_window(MAX_WINDOW_ROWS)).ok);
    let over = status_of(rows_window(MAX_WINDOW_ROWS + 1));
    assert_eq!((over.ok, over.code.as_str()), (false, "invalid_argument"), "{over:?}");
    assert!(!status_of(rows_window(0)).ok);

    // By the samples the rows reach: 512 rows, one in each of the newest 512 samples, are served;
    // 513 would need a 513th sample and are refused after a bounded read, not cut short.
    let at = rank(&engine, &key, spec(rows_window(MAX_WINDOW_SAMPLES), lane("shown"), 1));
    assert_eq!((at.samples, at.rows[0].score as u64, at.refused.as_str()), (512, 512, ""));
    let over = rank(&engine, &key, spec(rows_window(MAX_WINDOW_SAMPLES + 1), lane("shown"), 1));
    assert!(over.refused.contains("samples in the window"), "{}", over.refused);
    assert!(over.rows.is_empty());
    // The largest count a request may ask for reaches the same refusal on this key.
    let widest = rank(&engine, &key, spec(rows_window(MAX_WINDOW_ROWS), lane("shown"), 1));
    assert!(widest.refused.contains("samples in the window"), "{}", widest.refused);

    // A key that holds fewer rows than the largest count is read whole.
    let short = stored_key(14);
    put(&engine, &short, &h, 5, &[cell(1, 0, vec![4])]);
    put(&engine, &short, &h, 6, &[cell(1, 0, vec![3])]);
    let whole = rank(&engine, &short, spec(rows_window(MAX_WINDOW_ROWS), lane("shown"), 1));
    assert_eq!((whole.samples, whole.rows[0].score as u64, whole.refused.as_str()), (2, 7, ""));
}

#[test]
fn a_rows_window_applies_to_each_key_of_a_merged_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let a = stored_key(21);
    let b = stored_key(22);
    seed(&engine, &a);
    let h = two_lanes();
    put(&engine, &b, &h, 150, &[cell(1, 1, vec![1, 1]), cell(5, 1, vec![7, 1])]);
    put(&engine, &b, &h, 350, &[cell(5, 1, vec![9, 2])]);

    let merged = |window: WindowReduceWindow| {
        let response = exec(
            &engine,
            Command::WindowReduceFanInRank { keys: vec![a.clone(), b.clone()], spec: spec(window, lane("shown"), 10) },
        );
        assert!(response.status.ok, "{:?}", response.status);
        match response.response {
            CommandResponse::WindowReduceRanking { ranked } => ranked,
            other => panic!("not a ranking: {other:?}"),
        }
    };
    // The newest 2 rows of EACH key: a's sample at 400 (m2 10, m4 2) and b's rows (150, m5) and (350, m5),
    // which fold to m5 16.
    let two = merged(rows_window(2));
    assert_eq!(pairs(&two), vec![(5, 16), (2, 10), (4, 2)]);
    assert_eq!(two.keys, 2);
    // The window is per key: asking for 1 row takes a's m4 (2) and b's m5 (9), not the one newest row of the union.
    let one = merged(rows_window(1));
    assert_eq!(pairs(&one), vec![(5, 9), (4, 2)]);
}

#[test]
fn the_rows_window_is_one_new_spelling_and_the_other_windows_keep_their_own() {
    let text = |window: &WindowReduceWindow| serde_json::to_string(window).unwrap();
    assert_eq!(text(&WindowReduceWindow::Between { start_ms: 1, end_ms: 2 }), r#"{"between":{"start_ms":1,"end_ms":2}}"#);
    assert_eq!(text(&WindowReduceWindow::Last { duration_ms: 3 }), r#"{"last":{"duration_ms":3}}"#);
    assert_eq!(text(&WindowReduceWindow::Since { start_ms: 4 }), r#"{"since":{"start_ms":4}}"#);
    assert_eq!(text(&WindowReduceWindow::LastN { n: 5 }), r#"{"last_n":{"n":5}}"#);
    assert_eq!(text(&rows_window(6)), r#"{"newest_rows":{"n":6}}"#);
    assert_eq!(serde_json::from_str::<WindowReduceWindow>(r#"{"newest_rows":{"n":6}}"#).unwrap(), rows_window(6));
}
