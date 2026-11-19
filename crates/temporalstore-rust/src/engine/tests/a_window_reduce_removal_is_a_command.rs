// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CELL REMOVAL AS A COMMAND, THROUGH THE ENGINE.
//!
//! The selection is tested as a pure function in `window_reduce_remove`; these tests are about what only the
//! engine path can get wrong: that the series after a removal is the series an independent fold of
//! the stored cells describes, that a sample the request did not touch keeps the very bytes it
//! had, that a restart (cold, and by replaying the log) reads the same bytes, that a request the
//! key cannot serve writes nothing, that every bound is exact, and that the log can carry the
//! command in every spelling of its selector.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::engine::cell_set::{
    decode_value, encode_key, encode_value, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue,
    EntityType, EntityValue, KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{
    CellMemberFilter, CellMemberId, WindowReduceRankSpec, WindowReduceRemoveSpec, WindowReduceScore, WindowReduceWindow, FeatureFilter,
    FeatureFilterOp, RankedCells,
};
use std::collections::BTreeSet;

fn engine_on(dir: &std::path::Path, config: Option<crate::control::Config>) -> TemporalEngine {
    engine_with_cache(dir, "cache", config)
}

/// A restart that must not be served from the previous process's cache names another one.
fn engine_with_cache(dir: &std::path::Path, cache: &str, config: Option<crate::control::Config>) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join(cache),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    if let Some(config) = config {
        assert!(engine.set_config(crate::control::SetConfigRequest { shard_id: 1, config }).ok);
    }
    engine
}

fn roomy() -> crate::control::Config {
    crate::control::Config { version: 2, feature_max_size: 5_000, ..Default::default() }
}

fn exec(engine: &TemporalEngine, command: Command) -> ExecuteResponse {
    engine.execute(ExecuteRequest { shard_id: 1, command })
}

fn key_spec() -> KeySpec {
    KeySpec {
        namespace: "shown".to_string(),
        entity: EntityType::U64,
        dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
    }
}

fn stored_key(user: u64) -> String {
    encode_key(&key_spec(), &CellKey { entity: EntityValue::U64(user), dims: vec![Some(DimValue::U16(3))] }).unwrap()
}

/// One label (`slot`) and four lanes.
fn header() -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: key_spec(),
        labels: vec!["slot".to_string()],
        lanes: [
            ("impressions", LaneReducer::Sum),
            ("clicks", LaneReducer::Sum),
            ("peak", LaneReducer::Max),
            ("seen", LaneReducer::Last),
        ]
        .into_iter()
        .map(|(name, reducer)| LaneSpec { name: name.to_string(), reducer })
        .collect(),
    }
}

fn cell(item: u64, slot: u64, lanes: [i64; 4]) -> Cell {
    Cell { member: Member::U64(item), labels: vec![slot], lanes: lanes.to_vec() }
}

fn point(timestamp_ms: u64, cells: &[Cell]) -> FeaturePoint {
    FeaturePoint { timestamp_ms, value: encode_value(&header(), cells).unwrap() }
}

fn append(engine: &TemporalEngine, key: &str, points: Vec<FeaturePoint>) {
    let response = exec(engine, Command::FeatureAppend { key: key.to_string(), points });
    assert!(response.status.ok, "{:?}", response.status);
}

fn series(engine: &TemporalEngine, key: &str) -> Vec<FeaturePoint> {
    match exec(
        engine,
        Command::FeatureQuery { key: key.to_string(), start_ms: 0, end_ms: u64::MAX, count: Some(1_000_000) },
    )
    .response
    {
        CommandResponse::FeaturePoints { points } => points,
        other => panic!("expected FeaturePoints, got {other:?}"),
    }
}

fn decoded(engine: &TemporalEngine, key: &str) -> Vec<(u64, Vec<Cell>)> {
    series(engine, key)
        .into_iter()
        .map(|p| (p.timestamp_ms, decode_value(&p.value).expect("a stored value is a cell sample").1))
        .collect()
}

fn remove(engine: &TemporalEngine, key: &str, spec: WindowReduceRemoveSpec) -> ExecuteResponse {
    exec(engine, Command::WindowReduceRemove { key: key.to_string(), spec })
}

/// (samples before, samples after, cells before, cells removed)
fn receipt(response: &ExecuteResponse) -> [u64; 4] {
    assert!(response.status.ok, "{:?}", response.status);
    match &response.response {
        CommandResponse::WindowReduceRemoveReceipt { samples_before, samples_after, cells_before, cells_removed } => {
            [*samples_before, *samples_after, *cells_before, *cells_removed]
        }
        other => panic!("a removal answers a receipt, got {other:?}"),
    }
}

/// Twelve samples, at 100, 200 ... 1200. Item `m` (1 to 6) is present at sample `i` unless
/// `(m + i)` is a multiple of 3, and its label is where it was placed that time, `(m + i) % 3`
/// (so 1 or 2), and every lane value is a function of `(m, i)`.
fn seed(engine: &TemporalEngine, key: &str) -> Vec<(u64, Vec<Cell>)> {
    let samples: Vec<(u64, Vec<Cell>)> = (1..=12u64)
        .map(|i| {
            let cells = (1..=6u64)
                .filter(|m| (m + i) % 3 != 0)
                .map(|m| {
                    cell(
                        m,
                        (m + i) % 3,
                        [(10 * m + i) as i64, ((m * i) % 5) as i64, ((m * 7 + i * 3) % 11) as i64, (i * 100 + m) as i64],
                    )
                })
                .collect();
            (i * 100, cells)
        })
        .collect();
    append(engine, key, samples.iter().map(|(ts, cells)| point(*ts, cells)).collect());
    samples
}

fn on(field: &str, op: FeatureFilterOp, value: u64) -> FeatureFilter {
    FeatureFilter { field: field.to_string(), op, value }
}

fn id(member: u64) -> CellMemberId {
    CellMemberId::U64(member)
}

fn member_of(cell: &Cell) -> u64 {
    match cell.member {
        Member::U64(v) => v,
        _ => unreachable!("the seed holds u64 members"),
    }
}

/// What an independent fold keeps: the samples with the cells `removed` does not name, an empty
/// sample dropped.
fn kept(stored: &[(u64, Vec<Cell>)], removed: impl Fn(u64, &Cell) -> bool) -> Vec<(u64, Vec<Cell>)> {
    stored
        .iter()
        .map(|(stamp, cells)| (*stamp, cells.iter().filter(|c| !removed(*stamp, c)).cloned().collect::<Vec<_>>()))
        .filter(|(_, cells)| !cells.is_empty())
        .collect()
}

#[test]
fn a_removal_leaves_exactly_the_cells_an_independent_fold_keeps() {
    type Gone = Box<dyn Fn(u64, &Cell) -> bool>;
    let slot_one: Gone = Box::new(|_, c| c.labels[0] == 1);
    let table: Vec<(&str, WindowReduceRemoveSpec, Gone)> = vec![
        (
            "a label",
            WindowReduceRemoveSpec { filters: vec![on("slot", FeatureFilterOp::Equal, 1)], ..Default::default() },
            slot_one,
        ),
        (
            "a time window, which empties five samples",
            WindowReduceRemoveSpec { start_ms: Some(300), end_ms: Some(700), ..Default::default() },
            Box::new(|stamp, _| (300..=700).contains(&stamp)),
        ),
        (
            "a member set",
            WindowReduceRemoveSpec {
                members: CellMemberFilter { include: Some(vec![id(2), id(5)]), exclude: vec![] },
                ..Default::default()
            },
            Box::new(|_, c| [2, 5].contains(&member_of(c))),
        ),
        (
            "every member but one",
            WindowReduceRemoveSpec { members: CellMemberFilter { include: None, exclude: vec![id(1)] }, ..Default::default() },
            Box::new(|_, c| member_of(c) != 1),
        ),
        (
            "a lane test",
            WindowReduceRemoveSpec { filters: vec![on("clicks", FeatureFilterOp::GreaterOrEqual, 3)], ..Default::default() },
            Box::new(|_, c| c.lanes[1] >= 3),
        ),
        (
            "the member itself",
            WindowReduceRemoveSpec { filters: vec![on("member", FeatureFilterOp::GreaterThan, 3)], ..Default::default() },
            Box::new(|_, c| member_of(c) > 3),
        ),
        (
            "window, members and a label together",
            WindowReduceRemoveSpec {
                start_ms: Some(200),
                end_ms: Some(900),
                members: CellMemberFilter { include: Some(vec![id(1), id(3), id(4), id(6)]), exclude: vec![id(4)] },
                filters: vec![on("slot", FeatureFilterOp::Equal, 2)],
            },
            Box::new(|stamp, c| (200..=900).contains(&stamp) && [1, 3, 6].contains(&member_of(c)) && c.labels[0] == 2),
        ),
    ];
    let mut emptied_somewhere = false;
    for (name, spec, gone) in table {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path(), Some(roomy()));
        let key = stored_key(1);
        let stored = seed(&engine, &key);
        let before = series(&engine, &key);

        let in_window = |stamp: u64| spec.start_ms.map_or(true, |s| stamp >= s) && spec.end_ms.map_or(true, |e| stamp <= e);
        let cells_before: usize = stored.iter().filter(|(s, _)| in_window(*s)).map(|(_, c)| c.len()).sum();
        let removed: usize = stored.iter().map(|(s, cells)| cells.iter().filter(|c| gone(*s, c)).count()).sum();
        let expected = kept(&stored, &gone);
        assert!(removed > 0, "{name}: DENOMINATOR: the request must remove something");
        assert!(expected.len() < stored.len() || name != "a time window, which empties five samples");

        let landed = receipt(&remove(&engine, &key, spec));
        assert_eq!(landed, [12, expected.len() as u64, cells_before as u64, removed as u64], "{name}");
        assert_eq!(decoded(&engine, &key), expected, "{name}: the series is not what the fold keeps");
        emptied_somewhere |= expected.len() < 12;

        // A sample that lost nothing keeps the very bytes it had.
        let after = series(&engine, &key);
        for point in &before {
            let untouched = stored.iter().find(|(s, _)| *s == point.timestamp_ms).map_or(false, |(s, cells)| cells.iter().all(|c| !gone(*s, c)));
            if untouched {
                assert!(after.contains(point), "{name}: the sample at {} was rewritten though nothing in it went", point.timestamp_ms);
            }
        }
    }
    assert!(emptied_somewhere, "DENOMINATOR: one request emptied samples, so the count after was exercised");
}

#[test]
fn a_ranked_read_after_a_removal_answers_what_a_fold_over_the_kept_cells_says() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), Some(roomy()));
    let key = stored_key(2);
    let stored = seed(&engine, &key);
    let gone = |_: u64, c: &Cell| c.labels[0] == 1;
    receipt(&remove(&engine, &key, WindowReduceRemoveSpec { filters: vec![on("slot", FeatureFilterOp::Equal, 1)], ..Default::default() }));

    // Impressions are a sum per member over the cells that remain.
    let mut sums: std::collections::BTreeMap<u64, i64> = Default::default();
    for (_, cells) in kept(&stored, gone) {
        for c in cells {
            *sums.entry(member_of(&c)).or_default() += c.lanes[0];
        }
    }
    let mut want: Vec<(u64, i64)> = sums.into_iter().collect();
    want.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let response = exec(
        &engine,
        Command::WindowReduceRank {
            key: key.clone(),
            spec: WindowReduceRankSpec::new(
                WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX },
                WindowReduceScore::Lane { lane: "impressions".to_string() },
                100,
            ),
        },
    );
    let ranked: RankedCells = match response.response {
        CommandResponse::WindowReduceRanking { ranked } => ranked,
        other => panic!("{other:?}"),
    };
    assert!(ranked.refused.is_empty(), "{}", ranked.refused);
    let got: Vec<(u64, i64)> = ranked
        .rows
        .iter()
        .map(|r| (match &r.member { CellMemberId::U64(v) => *v, _ => unreachable!() }, r.score as i64))
        .collect();
    assert_eq!(got, want);
    assert_eq!(ranked.samples, 12, "no sample of the seed held only slot 1");
}

#[test]
fn a_cold_reload_reads_the_removed_bytes_and_removing_again_changes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), Some(roomy()));
    let key = stored_key(3);
    seed(&engine, &key);
    let request = WindowReduceRemoveSpec {
        start_ms: Some(200),
        end_ms: Some(1000),
        filters: vec![on("slot", FeatureFilterOp::Equal, 2)],
        ..Default::default()
    };
    let first = receipt(&remove(&engine, &key, request.clone()));
    assert!(first[3] > 0);
    let removed = series(&engine, &key);
    engine.unload_shard(1);
    drop(engine);

    let reloaded = engine_with_cache(dir.path(), "cache-b", Some(roomy()));
    assert_eq!(series(&reloaded, &key), removed, "a cold reload served other bytes");
    let again = receipt(&remove(&reloaded, &key, request));
    assert_eq!(again[3], 0, "the cells were already gone");
    assert_eq!((again[0], again[1]), (first[1], first[1]));
    assert_eq!(series(&reloaded, &key), removed);
}

#[test]
fn replaying_the_log_reaches_the_bytes_the_live_engine_reached() {
    let dir = tempfile::tempdir().expect("tempdir");
    let async_config = crate::control::Config { version: 2, async_storage: true, feature_max_size: 5_000, ..Default::default() };
    let engine = engine_on(dir.path(), Some(async_config));
    let key = stored_key(4);
    seed(&engine, &key);
    let landed = receipt(&remove(
        &engine,
        &key,
        WindowReduceRemoveSpec {
            start_ms: Some(300),
            end_ms: Some(800),
            members: CellMemberFilter { include: Some(vec![id(1), id(2), id(3)]), exclude: vec![] },
            ..Default::default()
        },
    ));
    assert!(landed[3] > 0);
    let live = series(&engine, &key);
    assert_eq!(live.len() as u64, landed[1]);
    // Async storage defers the blocks: the log is what holds the write when the process ends.
    assert!(engine.write_ahead_log_store().stats(1).writes >= 2);
    drop(engine);

    let restarted = engine_with_cache(dir.path(), "cache-b", None);
    assert_eq!(series(&restarted, &key), live, "replay planned another removal than the live one");
}

#[test]
fn the_same_request_on_two_engines_leaves_the_same_bytes() {
    let request = WindowReduceRemoveSpec {
        start_ms: Some(100),
        end_ms: Some(1100),
        filters: vec![on("impressions", FeatureFilterOp::LessThan, 50)],
        ..Default::default()
    };
    let mut outcomes = Vec::new();
    for _ in 0..2 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path(), Some(roomy()));
        let key = stored_key(1);
        seed(&engine, &key);
        let landed = receipt(&remove(&engine, &key, request.clone()));
        outcomes.push((landed, series(&engine, &key)));
    }
    assert!(outcomes[0].0[3] > 0, "DENOMINATOR: the request removed something");
    assert_eq!(outcomes[0], outcomes[1]);
}

#[test]
fn a_request_that_matches_nothing_writes_nothing_and_says_so() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), Some(roomy()));
    let key = stored_key(5);
    seed(&engine, &key);
    let stored = series(&engine, &key);

    // A label no cell carries: every sample is read, none changes.
    let none = receipt(&remove(&engine, &key, WindowReduceRemoveSpec { filters: vec![on("slot", FeatureFilterOp::Equal, 9)], ..Default::default() }));
    assert_eq!(none[..2], [12, 12]);
    assert_eq!(none[3], 0);
    assert!(none[2] > 0, "CONTROL: the cells were read, so the zero is a finding and not an absence");
    assert_eq!(series(&engine, &key), stored);

    // A window past the newest sample holds nothing and reads nothing.
    let past = receipt(&remove(&engine, &key, WindowReduceRemoveSpec { start_ms: Some(5000), ..Default::default() }));
    assert_eq!(past, [12, 12, 0, 0]);
    assert_eq!(series(&engine, &key), stored);
}

#[test]
fn removing_every_cell_leaves_an_empty_key_that_still_reads_and_still_takes_a_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), Some(roomy()));
    let key = stored_key(6);
    let stored = seed(&engine, &key);
    let total: usize = stored.iter().map(|(_, c)| c.len()).sum();
    let landed = receipt(&remove(&engine, &key, WindowReduceRemoveSpec { start_ms: Some(0), ..Default::default() }));
    assert_eq!(landed, [12, 0, total as u64, total as u64]);
    assert!(series(&engine, &key).is_empty());

    let ranked = match exec(
        &engine,
        Command::WindowReduceRank {
            key: key.clone(),
            spec: WindowReduceRankSpec::new(WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX }, WindowReduceScore::Member, 10),
        },
    )
    .response
    {
        CommandResponse::WindowReduceRanking { ranked } => ranked,
        other => panic!("{other:?}"),
    };
    assert!(ranked.refused.is_empty() && ranked.rows.is_empty() && ranked.samples == 0, "{ranked:?}");

    append(&engine, &key, vec![point(50, &[cell(1, 1, [1, 1, 1, 1])])]);
    assert_eq!(decoded(&engine, &key).len(), 1);
}

#[test]
fn a_removal_that_restricts_nothing_deletes_the_key_in_one_record_whatever_its_size_and_a_missing_key_reports_zero() {
    // The deletion of a key is one log record, whatever the key held.
    for samples_hint in [1usize, 12] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path(), Some(roomy()));
        let key = stored_key(8);
        if samples_hint == 12 {
            seed(&engine, &key);
        } else {
            append(&engine, &key, vec![point(100, &[cell(1, 1, [1, 1, 1, 1])])]);
        }
        let held = series(&engine, &key).len() as u64;
        let writes = engine.write_ahead_log_store().stats(1).writes;
        let landed = receipt(&remove(&engine, &key, WindowReduceRemoveSpec::default()));
        // The cells are not counted, because none was read.
        assert_eq!(landed, [held, 0, 0, 0]);
        assert!(series(&engine, &key).is_empty());
        assert_eq!(engine.write_ahead_log_store().stats(1).writes, writes + 1, "one log record");
        // The key takes a write again as a new key.
        append(&engine, &key, vec![point(50, &[cell(1, 1, [1, 1, 1, 1])])]);
        assert_eq!(decoded(&engine, &key).len(), 1);
    }

    // A key that holds nothing reports zero samples, which is what a delete answers NO_DATA for.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), Some(roomy()));
    assert_eq!(receipt(&remove(&engine, &stored_key(9), WindowReduceRemoveSpec::default())), [0, 0, 0, 0]);
}

#[test]
fn a_request_the_key_cannot_serve_is_refused_and_writes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), Some(roomy()));
    let key = stored_key(7);
    seed(&engine, &key);
    let stored = series(&engine, &key);
    let writes = engine.write_ahead_log_store().stats(1).writes;

    let refused = |spec: WindowReduceRemoveSpec, needle: &str| {
        let response = remove(&engine, &key, spec);
        assert_eq!((response.status.ok, response.status.code.as_str()), (false, "invalid_argument"), "{:?}", response.status);
        assert!(response.status.message.contains(needle), "{:?} does not name `{needle}`", response.status.message);
        assert_eq!(series(&engine, &key), stored, "a refused removal changed the series");
        assert_eq!(engine.write_ahead_log_store().stats(1).writes, writes, "a refused removal reached the log");
    };
    refused(WindowReduceRemoveSpec { filters: vec![on("views", FeatureFilterOp::Equal, 1)], ..Default::default() }, "views");
    refused(
        WindowReduceRemoveSpec { members: CellMemberFilter { include: Some(vec![]), exclude: vec![] }, ..Default::default() },
        "names no member",
    );
    refused(WindowReduceRemoveSpec { start_ms: Some(9), end_ms: Some(8), ..Default::default() }, "after it ends");

    // A feature key that holds something other than cells.
    append(&engine, "plain", vec![FeaturePoint { timestamp_ms: 1, value: b"12".to_vec() }]);
    let response = remove(&engine, "plain", WindowReduceRemoveSpec { start_ms: Some(0), ..Default::default() });
    assert!(!response.status.ok && response.status.code == "invalid_argument", "{:?}", response.status);
}

#[test]
fn each_bound_is_accepted_at_exactly_the_limit_and_refused_one_past_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), Some(roomy()));
    let key = stored_key(8);
    let one = |ts: u64| point(ts, &[cell(1, 0, [1, 1, 1, 1]), cell(2, 0, [1, 1, 1, 1])]);
    append(&engine, &key, (1..=1025u64).map(one).collect());

    // 1025 samples inside the window are refused, naming the limit, before any is read.
    let over = remove(&engine, &key, WindowReduceRemoveSpec { start_ms: Some(1), end_ms: Some(1025), ..Default::default() });
    assert_eq!((over.status.ok, over.status.code.as_str()), (false, "invalid_argument"), "{:?}", over.status);
    assert!(over.status.message.contains("1024") && over.status.message.contains("narrow the window"), "{}", over.status.message);
    assert_eq!(series(&engine, &key).len(), 1025, "the refusal wrote nothing");
    // 1024 are removed, and the one outside the window stays.
    let at = receipt(&remove(&engine, &key, WindowReduceRemoveSpec { start_ms: Some(1), end_ms: Some(1024), ..Default::default() }));
    assert_eq!(at, [1025, 1, 2048, 2048]);
    assert_eq!(decoded(&engine, &key).iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![1025]);

    // 8 predicates are accepted and 9 refused; a member set holds 1024 a side and not 1025.
    let predicates = |n: usize| WindowReduceRemoveSpec {
        filters: (0..n).map(|i| on("slot", FeatureFilterOp::NotEqual, i as u64 + 5)).collect(),
        ..Default::default()
    };
    assert!(remove(&engine, &key, predicates(8)).status.ok);
    assert!(remove(&engine, &key, predicates(9)).status.message.contains("predicates per removal"));
    let side = |n: u64| (0..n).map(id).collect::<Vec<_>>();
    for include in [true, false] {
        let set = |n: u64| WindowReduceRemoveSpec {
            members: CellMemberFilter { include: include.then(|| side(n)), exclude: if include { vec![] } else { side(n) } },
            ..Default::default()
        };
        assert!(remove(&engine, &key, set(1024)).status.ok, "include={include}");
        assert!(!remove(&engine, &key, set(1025)).status.ok, "include={include}");
    }
}

#[test]
fn an_absent_key_and_an_expired_key_remove_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), Some(roomy()));
    let key = stored_key(9);
    let any = || WindowReduceRemoveSpec { start_ms: Some(0), ..Default::default() };
    assert_eq!(receipt(&remove(&engine, &key, any())), [0; 4]);
    assert!(series(&engine, &key).is_empty(), "removing from an absent key created a series");

    seed(&engine, &key);
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
        crate::engine::set_expiry(shard, key.clone(), past);
    }
    assert_eq!(receipt(&remove(&engine, &key, any())), [0; 4]);
}

#[test]
fn a_removal_is_a_write_the_log_can_carry_in_every_selector_spelling() {
    use crate::shared_store::{command_from_protobuf_bytes, command_protobuf_bytes};
    let ops = [
        FeatureFilterOp::Equal,
        FeatureFilterOp::NotEqual,
        FeatureFilterOp::GreaterThan,
        FeatureFilterOp::GreaterOrEqual,
        FeatureFilterOp::LessThan,
        FeatureFilterOp::LessOrEqual,
    ];
    let mut specs = vec![
        WindowReduceRemoveSpec { start_ms: Some(0), ..Default::default() },
        WindowReduceRemoveSpec { end_ms: Some(7), ..Default::default() },
        WindowReduceRemoveSpec { start_ms: Some(1), end_ms: Some(2), ..Default::default() },
        // An empty include set and an absent one are different statements and keep their difference.
        WindowReduceRemoveSpec { members: CellMemberFilter { include: Some(vec![]), exclude: vec![] }, ..Default::default() },
        WindowReduceRemoveSpec { members: CellMemberFilter { include: None, exclude: vec![id(4)] }, ..Default::default() },
        WindowReduceRemoveSpec {
            members: CellMemberFilter {
                include: Some(vec![id(1), CellMemberId::Bytes(vec![0, 255, 7])]),
                exclude: vec![CellMemberId::Bytes(vec![])],
            },
            ..Default::default()
        },
    ];
    specs.extend(ops.iter().map(|op| WindowReduceRemoveSpec { filters: vec![on("clicks", op.clone(), 9)], ..Default::default() }));
    let mut seen_bytes = BTreeSet::new();
    for spec in &specs {
        let command = Command::WindowReduceRemove { key: "k".to_string(), spec: spec.clone() };
        assert!(crate::engine::is_write_command(&command));
        let bytes = command_protobuf_bytes(&command).expect("a removal has a protobuf encoding");
        assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_err(), "the bytes are JSON");
        assert_eq!(command_from_protobuf_bytes(&bytes).unwrap(), command);
        seen_bytes.insert(bytes);
    }
    assert_eq!(seen_bytes.len(), specs.len(), "every spelling has its own bytes");
    // The wire form of the answer carries every count.
    let answer = crate::sdk::types_command_response_to_sdk(CommandResponse::WindowReduceRemoveReceipt {
        samples_before: 5,
        samples_after: 4,
        cells_before: 30,
        cells_removed: 7,
    });
    assert_eq!((answer.samples_before, answer.count, answer.cells_before, answer.cells_removed), (5, 4, 30, 7));
}
