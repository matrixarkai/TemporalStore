// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A RANKED READ OVER SEVERAL CELL KEYS, THROUGH THE ENGINE.
//!
//! The merge itself is tested in `cell_fan_in` against an independent fold. These tests are about
//! what only the engine path can get wrong: that the snapshots it merges are the ones stored under
//! each named key (live, and after a restart that reads every block cold), that a key the shard
//! does not hold or one that expired contributes nothing and is not counted, that headers which
//! differ only by an added lane read under the wider one and any other difference is refused naming
//! the key, and that the bounds are refused at admission or while reading and accepted at exactly
//! the limit.
//!
//! The first test is also the documentation of the read: one user, a `channel` dimension in the
//! key, and the question "which items did this user respond to best across ALL channels?".
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::engine::cell_set::{
    encode_key, encode_value, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType,
    EntityValue, KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{CellMemberId, CellRankSpec, CellScore, CellWindow, RankedCells};

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
        namespace: "responses".to_string(),
        entity: EntityType::U64,
        dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
    }
}

/// The stored key of `entity` under `channel`.
fn stored_key(entity: u64, channel: u16) -> String {
    encode_key(
        &key_spec(),
        &CellKey { entity: EntityValue::U64(entity), dims: vec![Some(DimValue::U16(channel))] },
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

/// `shown` summed and `peak` kept at its maximum.
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

fn spec(window: CellWindow, score: CellScore, top_k: usize) -> CellRankSpec {
    CellRankSpec { window, filters: Vec::new(), score, top_k, project: Vec::new() }
}

const ALL: CellWindow = CellWindow::Between { start_ms: 0, end_ms: u64::MAX };

fn lane(name: &str) -> CellScore {
    CellScore::Lane { lane: name.to_string() }
}

fn fan_in_status(engine: &TemporalEngine, keys: &[String], query: CellRankSpec) -> ExecuteResponse {
    exec(engine, Command::CellFanInRankQuery { keys: keys.to_vec(), spec: query })
}

fn fan_in(engine: &TemporalEngine, keys: &[String], query: CellRankSpec) -> RankedCells {
    let response = fan_in_status(engine, keys, query);
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::CellRanking { ranked } => ranked,
        other => panic!("not a ranking: {other:?}"),
    }
}

fn rank(engine: &TemporalEngine, key: &str, query: CellRankSpec) -> RankedCells {
    let response = exec(engine, Command::CellRankQuery { key: key.to_string(), spec: query });
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

fn scores(ranked: &RankedCells) -> Vec<i64> {
    ranked.rows.iter().map(|row| row.score as i64).collect()
}

/// Three channels of user 7, six snapshots at distinct stamps, lanes [shown (Sum), peak (Max)]:
///   channel 1: 100 item 1 [10, 5], item 2 [20, 7]      400 item 3 [30, 2]
///   channel 2: 200 item 1 [10, 9], item 3 [40, 3]      500 item 4 [5, 1]
///   channel 3: 300 item 2 [25, 8], item 3 [10, 6]      600 item 1 [1, 2]
/// Over all three channels: item 1 shown 21 peak 9, item 2 shown 45 peak 8, item 3 shown 80 peak 6,
/// item 4 shown 5 peak 1. The same six snapshots go to user 99's channel 1, a single key.
fn seed(engine: &TemporalEngine) -> (Vec<String>, String) {
    let h = two_lanes();
    let channels = [
        (1u16, vec![(100u64, vec![cell(1, 1, vec![10, 5]), cell(2, 1, vec![20, 7])]), (400, vec![cell(3, 1, vec![30, 2])])]),
        (2, vec![(200, vec![cell(1, 1, vec![10, 9]), cell(3, 2, vec![40, 3])]), (500, vec![cell(4, 2, vec![5, 1])])]),
        (3, vec![(300, vec![cell(2, 3, vec![25, 8]), cell(3, 3, vec![10, 6])]), (600, vec![cell(1, 3, vec![1, 2])])]),
    ];
    let single = stored_key(99, 1);
    let mut keys = Vec::new();
    for (channel, snapshots) in &channels {
        let key = stored_key(7, *channel);
        for (ts, cells) in snapshots {
            put(engine, &key, &h, *ts, cells);
            put(engine, &single, &h, *ts, cells);
        }
        keys.push(key);
    }
    (keys, single)
}

#[test]
fn a_worked_example_of_a_ranked_read_across_the_channels_of_one_user() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let (keys, single) = seed(&engine);

    // ---- WHICH ITEMS DID THE USER RESPOND TO MOST, OVER ALL THREE CHANNELS? ---------------------
    // One read names the three keys. Each item's `shown` is summed over every snapshot of every
    // channel: 80, 45, 21, 5. No one channel's own top item list could say it: item 2 is second
    // in channel 1 and leads channel 3, and only the union puts it second overall.
    let by_shown = fan_in(&engine, &keys, spec(ALL, lane("shown"), 10));
    assert_eq!(members(&by_shown), vec![3, 2, 1, 4]);
    assert_eq!(scores(&by_shown), vec![80, 45, 21, 5]);
    assert_eq!((by_shown.snapshots, by_shown.keys), (6, 3));
    // Max, not sum: the peak is the declared verb of that lane, so item 1's 9 leads.
    assert_eq!(members(&fan_in(&engine, &keys, spec(ALL, lane("peak"), 10))), vec![1, 2, 3, 4]);
    // A row carries the merged lanes and the label of the newest cell: item 1's newest cell is in
    // channel 3 at 600, slot 3.
    let item_1 = by_shown.rows.iter().find(|row| row.member == CellMemberId::U64(1)).unwrap();
    assert_eq!((item_1.lanes.clone(), item_1.labels.clone(), item_1.last_ms), (vec![21, 9], vec![3], 600));

    // ---- THE ANSWER IS THE ONE A SINGLE KEY HOLDING EVERY SNAPSHOT GIVES -----------------------
    // User 99 has one key with the same six snapshots. Only the count of keys differs.
    let one_key = rank(&engine, &single, spec(ALL, lane("shown"), 10));
    assert_eq!(one_key.keys, 1);
    let mut merged = by_shown.clone();
    merged.keys = one_key.keys;
    assert_eq!(merged, one_key);

    // ---- THE WINDOW APPLIES TO EACH KEY --------------------------------------------------------
    // The newest snapshot of each channel: 400, 500 and 600. Item 3 30, item 4 5, item 1 1.
    let newest = fan_in(&engine, &keys, spec(CellWindow::LastN { n: 1 }, lane("shown"), 10));
    assert_eq!((members(&newest), scores(&newest), newest.snapshots), (vec![3, 4, 1], vec![30, 5, 1], 3));

    drop(engine);
    // A reload reads every block cold, and answers the same.
    let reloaded = engine_on(dir.path());
    assert_eq!(fan_in(&reloaded, &keys, spec(ALL, lane("shown"), 10)), by_shown);
}

#[test]
fn a_key_the_shard_does_not_hold_or_that_expired_contributes_nothing_and_is_not_counted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let (keys, _) = seed(&engine);
    let absent = stored_key(7, 40);

    // CONTROL: all three held. Channels 1 and 2 only: item 3 70, item 1 20, item 2 20, item 4 5 (the
    // tie between 1 and 2 breaks by member).
    assert_eq!(fan_in(&engine, &keys, spec(ALL, lane("shown"), 10)).keys, 3);
    let named = vec![keys[0].clone(), absent.clone(), keys[1].clone()];
    let partial = fan_in(&engine, &named, spec(ALL, lane("shown"), 10));
    assert_eq!((members(&partial), scores(&partial)), (vec![3, 1, 2, 4], vec![70, 20, 20, 5]));
    assert_eq!((partial.keys, partial.snapshots, partial.refused.as_str()), (2, 4, ""));

    // Every key absent: nothing to rank, not a refusal.
    let none = fan_in(&engine, &[absent.clone(), stored_key(7, 41)], spec(ALL, lane("shown"), 10));
    assert_eq!((none.rows.len(), none.keys, none.refused.as_str()), (0, 0, ""));

    // Channel 2 expires: it is dropped as a one-key read drops it, and the answer is channel 1's.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
        crate::engine::set_expiry(shard, keys[1].clone(), past);
    }
    let expired = fan_in(&engine, &named, spec(ALL, lane("shown"), 10));
    assert_eq!((members(&expired), scores(&expired), expired.keys), (vec![3, 2, 1], vec![30, 20, 10], 1));
}

#[test]
fn a_request_that_breaks_a_bound_is_refused_at_admission_and_the_limit_is_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let (keys, _) = seed(&engine);
    let query = || spec(ALL, lane("shown"), 5);
    let many = |n: usize| (0..n).map(|i| stored_key(1_000 + i as u64, 1)).collect::<Vec<_>>();

    // Keys: 64 are served (none of them held, so the answer is empty), 65 and none are refused.
    let at = fan_in_status(&engine, &many(64), query());
    assert!(at.status.ok, "{:?}", at.status);
    for refused in [many(65), Vec::new()] {
        let status = fan_in_status(&engine, &refused, query()).status;
        assert_eq!((status.ok, status.code.as_str()), (false, "invalid_argument"), "{status:?}");
        assert!(status.message.contains("keys in a fan-in read"), "{status:?}");
    }
    // A key named twice would count twice: refused, naming both positions.
    let twice = fan_in_status(&engine, &[keys[0].clone(), keys[1].clone(), keys[0].clone()], query()).status;
    assert!(!twice.ok && twice.message.contains("key 2 repeats key 0"), "{twice:?}");
    // What a one-key read is refused for is refused here too.
    assert!(!fan_in_status(&engine, &keys, spec(ALL, lane("shown"), 1001)).status.ok);
    assert!(fan_in_status(&engine, &keys, spec(ALL, lane("shown"), 1000)).status.ok);
    assert!(!fan_in_status(&engine, &keys, spec(CellWindow::LastN { n: 513 }, lane("shown"), 5)).status.ok);
    // A name the keys do not declare is a served refusal naming it.
    let unknown = fan_in(&engine, &keys, spec(ALL, lane("views"), 5));
    assert!(unknown.refused.contains("views") && unknown.rows.is_empty(), "{}", unknown.refused);
}

#[test]
fn keys_whose_headers_differ_only_by_an_added_lane_read_under_the_wider_and_any_other_difference_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let narrow = header(&[("shown", LaneReducer::Sum)]);
    let wide = header(&[("shown", LaneReducer::Sum), ("clicked", LaneReducer::Sum)]);
    let (a, b, c) = (stored_key(5, 1), stored_key(5, 2), stored_key(5, 3));
    put(&engine, &a, &narrow, 100, &[cell(1, 0, vec![10]), cell(2, 0, vec![20])]);
    put(&engine, &b, &wide, 200, &[cell(1, 0, vec![10, 4])]);

    // The narrow key reads the lane it lacks as zero, whichever key is named first.
    for keys in [vec![a.clone(), b.clone()], vec![b.clone(), a.clone()]] {
        let ranked = fan_in(&engine, &keys, spec(ALL, lane("clicked"), 5));
        assert_eq!(members(&ranked), vec![1, 2], "{keys:?}");
        assert_eq!(
            ranked.rows.iter().map(|r| r.lanes.clone()).collect::<Vec<_>>(),
            vec![vec![20, 4], vec![20, 0]]
        );
    }

    // A key whose `shown` lane merges by Max is a different spec, not a wider one.
    let other = header(&[("shown", LaneReducer::Max), ("clicked", LaneReducer::Sum)]);
    put(&engine, &c, &other, 300, &[cell(1, 0, vec![1, 1])]);
    let refused = fan_in(&engine, &[a, b, c], spec(ALL, lane("shown"), 5));
    assert!(refused.rows.is_empty());
    assert!(
        refused.refused.starts_with("key 2: ") && refused.refused.contains("reducer"),
        "{}",
        refused.refused
    );
}

fn put_many(engine: &TemporalEngine, key: &str, header: &CellHeader, snapshots: u64, cells: &[Cell]) {
    let points = (1..=snapshots)
        .map(|timestamp_ms| FeaturePoint { timestamp_ms, value: encode_value(header, cells).unwrap() })
        .collect();
    let response = exec(engine, Command::FeatureAppend { key: key.to_string(), points });
    assert!(response.status.ok, "{:?}", response.status);
}

#[test]
fn the_snapshot_ceiling_holds_while_reading_and_the_limit_is_served() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let h = header(&[("shown", LaneReducer::Sum)]);
    let one = [cell(1, 0, vec![1])];
    let (a, b, c) = (stored_key(6, 1), stored_key(6, 2), stored_key(6, 3));
    put_many(&engine, &a, &h, 512, &one);
    put_many(&engine, &b, &h, 512, &one);
    put_many(&engine, &c, &h, 1, &one);

    // Two keys of 512 (each at the per-key window bound) is 1024: served, every snapshot summed.
    let at = fan_in(&engine, &[a.clone(), b.clone()], spec(ALL, lane("shown"), 1));
    assert_eq!((at.snapshots, scores(&at), at.keys), (1024, vec![1024], 2));
    // A third key's one snapshot is 1025: refused whole, with no rows.
    let over = fan_in(&engine, &[a.clone(), b.clone(), c.clone()], spec(ALL, lane("shown"), 1));
    assert!(over.refused.contains("snapshots in a fan-in read"), "{}", over.refused);
    assert!(over.rows.is_empty());
    // The ceiling is on what the window names: the newest 100 of each key passes.
    let narrow = fan_in(&engine, &[a, b, c], spec(CellWindow::LastN { n: 100 }, lane("shown"), 1));
    assert_eq!((narrow.snapshots, scores(&narrow)), (201, vec![201]));
}

#[test]
fn the_cell_ceiling_holds_while_reading_and_the_limit_is_served() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let h = header(&[("shown", LaneReducer::Sum)]);
    let wide: Vec<Cell> = (0..4096u64).map(|m| cell(m, 0, vec![1])).collect();
    let (a, b) = (stored_key(8, 1), stored_key(8, 2));
    // 64 snapshots of 4 096 cells is exactly the 262 144 cells one fan-in read may visit.
    put_many(&engine, &a, &h, 64, &wide);
    put(&engine, &b, &h, 1, &[cell(0, 0, vec![1])]);

    let at = fan_in(&engine, &[a.clone()], spec(ALL, lane("shown"), 1));
    assert_eq!((at.snapshots, scores(&at)), (64, vec![64]), "member 0 is in every snapshot");
    // One cell more, from a second key, is refused whole.
    let over = fan_in(&engine, &[a, b], spec(ALL, lane("shown"), 1));
    assert!(over.refused.contains("cells in a fan-in read"), "{}", over.refused);
    assert!(over.rows.is_empty());
}

fn batch(engine: &TemporalEngine, queries: Vec<(String, CellRankSpec)>) -> ExecuteResponse {
    let queries = queries
        .into_iter()
        .map(|(key, spec)| crate::types::CellRankBatchItem { key, spec })
        .collect();
    exec(engine, Command::CellRankBatchQuery { queries })
}

fn rankings(response: ExecuteResponse) -> Vec<RankedCells> {
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::CellRankings { rankings } => rankings,
        other => panic!("not a batch of rankings: {other:?}"),
    }
}

#[test]
fn a_batch_of_reads_answers_each_as_the_single_read_would_in_request_order_and_one_failing_read_fails_no_other() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let (keys, _) = seed(&engine);
    let absent = stored_key(7, 40);

    // Different keys, different scores, a key the shard does not hold, a name the key does not
    // declare, and the first key again with another window.
    let queries = vec![
        (keys[0].clone(), spec(ALL, lane("shown"), 10)),
        (keys[1].clone(), spec(ALL, lane("peak"), 10)),
        (absent.clone(), spec(ALL, lane("shown"), 10)),
        (keys[2].clone(), spec(ALL, lane("views"), 10)),
        (keys[0].clone(), spec(CellWindow::LastN { n: 1 }, lane("shown"), 10)),
    ];
    let answers = rankings(batch(&engine, queries.clone()));
    assert_eq!(answers.len(), 5);
    // Each answer is exactly what the one-key read gives for that key and spec (a refused read is
    // served by the same path, so it compares too).
    for ((key, query), answer) in queries.iter().zip(&answers) {
        let single = exec(&engine, Command::CellRankQuery { key: key.clone(), spec: query.clone() });
        match single.response {
            CommandResponse::CellRanking { ranked } => assert_eq!(&ranked, answer),
            other => panic!("not a ranking: {other:?}"),
        }
    }
    // The values only this request can produce: channel 1 by shown is item 3 (30), 2 (20), 1 (10);
    // channel 2 by peak is item 1 (9), 3 (3), 4 (1).
    assert_eq!((members(&answers[0]), scores(&answers[0])), (vec![3, 2, 1], vec![30, 20, 10]));
    assert_eq!((members(&answers[1]), scores(&answers[1])), (vec![1, 3, 4], vec![9, 3, 1]));
    assert!(answers[2].rows.is_empty() && answers[2].refused.is_empty(), "an absent key answers empty");
    assert!(answers[3].refused.contains("views") && answers[3].rows.is_empty());
    assert_eq!((members(&answers[4]), answers[4].snapshots), (vec![3], 1));
}

#[test]
fn a_batch_is_refused_at_admission_for_its_size_and_for_any_read_a_single_read_would_be_refused_for() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let (keys, _) = seed(&engine);
    let read = |i: usize| (stored_key(2_000 + i as u64, 1), spec(ALL, lane("shown"), 5));
    let reads = |n: usize| (0..n).map(read).collect::<Vec<_>>();

    // 64 reads are served, 65 and none are refused whole.
    assert_eq!(rankings(batch(&engine, reads(64))).len(), 64);
    for refused in [reads(65), Vec::new()] {
        let status = batch(&engine, refused).status;
        assert_eq!((status.ok, status.code.as_str()), (false, "invalid_argument"), "{status:?}");
        assert!(status.message.contains("reads in a batch"), "{status:?}");
    }
    // One read over the top-k bound refuses the batch, naming its position, and the others are
    // not run.
    let mut mixed = vec![(keys[0].clone(), spec(ALL, lane("shown"), 5)), read(0)];
    mixed[1].1.top_k = 1001;
    let status = batch(&engine, mixed).status;
    assert!(!status.ok && status.message.starts_with("read 1: "), "{status:?}");
    // Window and predicate bounds are the single read's too.
    let over_window = vec![(keys[0].clone(), spec(CellWindow::LastN { n: 513 }, lane("shown"), 5))];
    assert!(!batch(&engine, over_window).status.ok);
}

#[test]
fn a_batch_read_drops_an_expired_key_and_answers_it_empty() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let (keys, _) = seed(&engine);
    let queries = vec![(keys[0].clone(), spec(ALL, lane("shown"), 5)), (keys[1].clone(), spec(ALL, lane("shown"), 5))];
    let live = rankings(batch(&engine, queries.clone()));
    assert_eq!((live[0].rows.len(), live[1].rows.len()), (3, 3), "CONTROL: both are live");
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
        crate::engine::set_expiry(shard, keys[1].clone(), past);
    }
    let after = rankings(batch(&engine, queries));
    assert_eq!(after[0], live[0]);
    assert!(after[1].rows.is_empty() && after[1].refused.is_empty());
}

#[test]
fn a_fan_in_read_is_a_read() {
    let command = Command::CellFanInRankQuery { keys: vec!["k".to_string()], spec: spec(ALL, lane("shown"), 1) };
    assert!(!crate::engine::is_write_command(&command));
    let batch = Command::CellRankBatchQuery { queries: Vec::new() };
    assert!(!crate::engine::is_write_command(&batch));
    assert!(crate::engine::command_object_keys(&batch).into_iter().next().is_none());
    // It writes no object, so no object key is held for it.
    assert!(crate::engine::command_object_keys(&command).into_iter().next().is_none());
}
