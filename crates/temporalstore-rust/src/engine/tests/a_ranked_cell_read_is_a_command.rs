// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE RANKED CELL READ AS A COMMAND, THROUGH THE ENGINE.
//!
//! The pure ranking is tested in `cell_rank`; these tests are about what only the engine path can
//! get wrong: that the snapshots it reads are the snapshots that were stored (live, and after a
//! restart that reads every block cold), that an absent or expired key ranks nothing, that a read
//! the key cannot serve is refused with its reason and a read that breaks a bound is refused at
//! admission, and that a header that gained a lane after older snapshots were written still reads
//! them.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::engine::cell_set::{
    encode_key, encode_value, Cell, CellHeader, CellKey, DimSpec, DimType, EntityType, EntityValue,
    DimValue, KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{
    CellMemberId, CellRankSpec, CellScore, CellWindow, FeatureFilter, FeatureFilterOp, RankedCells,
};

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
    encode_key(
        &key_spec(),
        &CellKey { entity: EntityValue::U64(entity), dims: vec![Some(DimValue::U16(3))] },
    )
    .unwrap()
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

/// Four snapshots under [shown (Sum), peak (Max)]. Reduced over all four:
///   m1 shown 30 peak 9    m2 shown 60 peak 8    m3 shown 200 peak 6    m4 shown 10 peak 2
fn seed(engine: &TemporalEngine, key: &str) {
    let h = two_lanes();
    put(engine, key, &h, 100, &[cell(1, 1, vec![10, 5]), cell(2, 2, vec![20, 7]), cell(3, 1, vec![100, 2])]);
    put(engine, key, &h, 200, &[cell(1, 1, vec![10, 9]), cell(2, 3, vec![30, 3]), cell(4, 2, vec![8, 1])]);
    put(engine, key, &h, 300, &[cell(1, 2, vec![10, 4]), cell(3, 1, vec![100, 6])]);
    put(engine, key, &h, 400, &[cell(2, 2, vec![10, 8]), cell(4, 2, vec![2, 2])]);
}

fn spec(window: CellWindow, score: CellScore, top_k: usize) -> CellRankSpec {
    CellRankSpec { window, filters: Vec::new(), score, top_k, project: Vec::new() }
}

const ALL: CellWindow = CellWindow::Between { start_ms: 0, end_ms: u64::MAX };

fn lane(name: &str) -> CellScore {
    CellScore::Lane { lane: name.to_string() }
}

fn rank(engine: &TemporalEngine, key: &str, spec: CellRankSpec) -> RankedCells {
    let response = exec(engine, Command::CellRankQuery { key: key.to_string(), spec });
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::CellRanking { ranked } => ranked,
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

#[test]
fn a_ranked_read_answers_from_what_was_stored_live_and_after_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(7);
    seed(&engine, &key);

    let by_sum = spec(ALL, lane("shown"), 10);
    let by_max = spec(ALL, lane("peak"), 10);
    let windowed = spec(CellWindow::Between { start_ms: 200, end_ms: 300 }, lane("shown"), 10);

    let live = rank(&engine, &key, by_sum.clone());
    // Sum over every snapshot: 200 60 30 10.
    assert_eq!(members(&live), vec![3, 2, 1, 4]);
    assert_eq!(live.rows.iter().map(|r| r.score as i64).collect::<Vec<_>>(), vec![200, 60, 30, 10]);
    assert_eq!(live.snapshots, 4);
    assert!(live.refused.is_empty());
    // Max, not sum: a sum of the peaks would put m1 and m2 level at 18.
    assert_eq!(members(&rank(&engine, &key, by_max.clone())), vec![1, 2, 3, 4]);
    // The window: snapshots 200 and 300 only. m1 20, m2 30, m3 100, m4 8.
    let window = rank(&engine, &key, windowed.clone());
    assert_eq!(members(&window), vec![3, 2, 1, 4]);
    assert_eq!(window.rows.iter().map(|r| r.score as i64).collect::<Vec<_>>(), vec![100, 30, 20, 8]);

    drop(engine);
    // A reload reads every block cold.
    let reloaded = engine_on(dir.path());
    assert_eq!(rank(&reloaded, &key, by_sum), live);
    assert_eq!(members(&rank(&reloaded, &key, by_max)), vec![1, 2, 3, 4]);
    assert_eq!(rank(&reloaded, &key, windowed), window);
}

#[test]
fn an_absent_key_and_an_expired_key_rank_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(1);

    let absent = rank(&engine, &key, spec(ALL, lane("shown"), 5));
    assert_eq!((absent.rows.len(), absent.refused.as_str()), (0, ""));

    seed(&engine, &key);
    assert_eq!(rank(&engine, &key, spec(ALL, lane("shown"), 5)).rows.len(), 4, "CONTROL: the key is live");
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
        crate::engine::set_expiry(shard, key.clone(), past);
    }
    let expired = rank(&engine, &key, spec(ALL, lane("shown"), 5));
    assert_eq!((expired.rows.len(), expired.refused.as_str()), (0, ""));
}

#[test]
fn a_read_the_key_cannot_serve_is_refused_with_its_reason() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(2);
    seed(&engine, &key);

    // A name the key does not declare: served as a refusal naming it, with no rows.
    let unknown_lane = rank(&engine, &key, spec(ALL, lane("views"), 5));
    assert!(unknown_lane.refused.contains("views"), "{}", unknown_lane.refused);
    assert!(unknown_lane.rows.is_empty());
    let mut unknown_filter = spec(ALL, lane("shown"), 5);
    unknown_filter.filters = vec![FeatureFilter { field: "gid".to_string(), op: FeatureFilterOp::Equal, value: 1 }];
    assert!(rank(&engine, &key, unknown_filter).refused.contains("gid"));

    // A feature key that holds something other than cells.
    let plain = "plain-feature";
    let appended = exec(
        &engine,
        Command::FeatureAppend {
            key: plain.to_string(),
            points: vec![FeaturePoint { timestamp_ms: 1, value: b"12".to_vec() }],
        },
    );
    assert!(appended.status.ok);
    assert!(!rank(&engine, plain, spec(ALL, lane("shown"), 5)).refused.is_empty());
}

#[test]
fn a_bound_is_refused_at_admission_and_accepted_at_exactly_the_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(3);
    seed(&engine, &key);

    let status_of = |query: CellRankSpec| {
        exec(&engine, Command::CellRankQuery { key: key.clone(), spec: query }).status
    };
    // top-k: 1000 is served, 1001 and 0 are refused before any snapshot is read.
    assert!(status_of(spec(ALL, lane("shown"), 1000)).ok);
    let over = status_of(spec(ALL, lane("shown"), 1001));
    assert_eq!((over.ok, over.code.as_str()), (false, "invalid_argument"), "{over:?}");
    assert!(!status_of(spec(ALL, lane("shown"), 0)).ok);
    // predicates: 8 served, 9 refused.
    let predicates = |n: usize| {
        let mut query = spec(ALL, lane("shown"), 5);
        query.filters = (0..n)
            .map(|_| FeatureFilter { field: "shown".to_string(), op: FeatureFilterOp::GreaterOrEqual, value: 0 })
            .collect();
        query
    };
    assert!(status_of(predicates(8)).ok);
    assert!(!status_of(predicates(9)).ok);
    // lanes in a weighted score: 8 is admitted (the key declares two lanes, so it is then refused by
    // name, as a served refusal), 9 is refused at admission.
    let weighted = |n: usize| {
        spec(
            ALL,
            CellScore::Weighted { lanes: (0..n).map(|i| format!("l{i}")).collect(), weights: vec![1.0; n] },
            5,
        )
    };
    assert!(status_of(weighted(8)).ok);
    assert!(!status_of(weighted(9)).ok);
    // window: LastN 512 is admitted, 513 refused.
    assert!(status_of(spec(CellWindow::LastN { n: 512 }, lane("shown"), 5)).ok);
    assert!(!status_of(spec(CellWindow::LastN { n: 513 }, lane("shown"), 5)).ok);
}

#[test]
fn a_window_over_the_bound_is_refused_after_a_bounded_read_and_one_at_the_bound_is_served() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(4);
    let h = header(&[("shown", LaneReducer::Sum)]);
    for ts in 1..=513u64 {
        put(&engine, &key, &h, ts, &[cell(1, 0, vec![1])]);
    }
    // 512 snapshots: the newest 512, summing to 512.
    let at = rank(&engine, &key, spec(CellWindow::Between { start_ms: 2, end_ms: u64::MAX }, lane("shown"), 1));
    assert_eq!((at.snapshots, at.rows[0].score as u64), (512, 512));
    // 513: refused, with rows left empty.
    let over = rank(&engine, &key, spec(ALL, lane("shown"), 1));
    assert!(over.refused.contains("snapshots in the window"), "{}", over.refused);
    assert!(over.rows.is_empty());
    // The newest 512 by count is served.
    assert_eq!(rank(&engine, &key, spec(CellWindow::LastN { n: 512 }, lane("shown"), 1)).rows[0].score as u64, 512);
}

#[test]
fn a_lane_added_later_reads_as_zero_in_the_snapshots_written_before_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(5);
    let before = header(&[("shown", LaneReducer::Sum)]);
    let after = header(&[("shown", LaneReducer::Sum), ("clicked", LaneReducer::Sum)]);
    put(&engine, &key, &before, 100, &[cell(1, 0, vec![10]), cell(2, 0, vec![20])]);
    put(&engine, &key, &after, 200, &[cell(1, 0, vec![10, 4])]);

    let ranked = rank(&engine, &key, spec(ALL, lane("clicked"), 5));
    // Only the later snapshot carries `clicked`: m1 4; m2 has none and reads 0.
    assert_eq!(members(&ranked), vec![1, 2]);
    assert_eq!(ranked.rows.iter().map(|r| r.lanes.clone()).collect::<Vec<_>>(), vec![vec![20, 4], vec![20, 0]]);
    assert_eq!(members(&rank(&engine, &key, spec(ALL, lane("shown"), 5))), vec![1, 2]);
}
