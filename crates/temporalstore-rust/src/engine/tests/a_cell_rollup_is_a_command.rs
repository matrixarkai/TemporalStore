// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CELL ROLLUP AS A COMMAND, THROUGH THE ENGINE.
//!
//! The fold and the cap are tested as pure functions in `cell_rollup`; these tests are about what
//! only the engine path can get wrong: that the series after a rollup is the series the pure
//! function describes, that a ranked read over the rolled buckets answers what it answered over
//! the snapshots they replaced, that a restart (cold, and by replaying the log) reads the same
//! bytes, that two engines given the same request end with the same bytes, that a request the key
//! cannot serve writes nothing, and that an absent or expired key rolls nothing up.
//!
//! `a_worked_example_of_a_rollup` is also the example in `docs/CELLS_WORKED_EXAMPLE.md`.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::engine::cell_set::{
    decode_value, encode_key, encode_value, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue,
    EntityType, EntityValue, KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{
    CellCapSpec, CellMemberId, CellRankSpec, CellRollupSpec, CellScore, CellWindow, RankedCells,
};
use std::collections::BTreeSet;

const IMPRESSIONS: usize = 0;
const CLICKS: usize = 1;
const PEAK: usize = 2;
const SEEN: usize = 3;

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

/// The key family: an entity that is a user id and one required dimension, the channel.
fn key_spec() -> KeySpec {
    KeySpec {
        namespace: "shown".to_string(),
        entity: EntityType::U64,
        dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
    }
}

fn stored_key(user: u64) -> String {
    encode_key(
        &key_spec(),
        &CellKey { entity: EntityValue::U64(user), dims: vec![Some(DimValue::U16(3))] },
    )
    .unwrap()
}

/// One label (`slot`, where the item was placed) and four lanes, each merged a different way, so a
/// rollup that folded every lane alike could not produce the numbers asserted below.
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
        .map(|p| (p.timestamp_ms, decode_value(&p.value).expect("a stored value is a cell snapshot").1))
        .collect()
}

fn roll(older_than_ms: u64, bucket_ms: u64, cap: Option<CellCapSpec>) -> CellRollupSpec {
    CellRollupSpec { older_than_ms, bucket_ms, cap }
}

fn lane(name: &str) -> CellScore {
    CellScore::Lane { lane: name.to_string() }
}

fn rollup_response(engine: &TemporalEngine, key: &str, spec: CellRollupSpec) -> ExecuteResponse {
    exec(engine, Command::CellRollup { key: key.to_string(), spec })
}

/// (snapshots before, snapshots after, cells before, cells after, cells dropped)
fn receipt(response: &ExecuteResponse) -> [u64; 5] {
    assert!(response.status.ok, "{:?}", response.status);
    match &response.response {
        CommandResponse::CellRollupReceipt {
            snapshots_before,
            snapshots_after,
            cells_before,
            cells_after,
            cells_dropped,
        } => [*snapshots_before, *snapshots_after, *cells_before, *cells_after, *cells_dropped],
        other => panic!("a rollup answers a receipt, got {other:?}"),
    }
}

fn rank(engine: &TemporalEngine, key: &str, score: CellScore) -> RankedCells {
    let response = exec(
        engine,
        Command::CellRankQuery {
            key: key.to_string(),
            spec: CellRankSpec {
                window: CellWindow::Between { start_ms: 0, end_ms: u64::MAX },
                filters: Vec::new(),
                score,
                top_k: 100,
                project: Vec::new(),
            },
        },
    );
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::CellRanking { ranked } => {
            assert!(ranked.refused.is_empty(), "refused: {}", ranked.refused);
            ranked
        }
        other => panic!("not a ranking: {other:?}"),
    }
}

/// What a ranking says about each member, without the stamps a rollup moves.
fn answers(ranked: &RankedCells) -> Vec<(CellMemberId, u64, Vec<u64>, Vec<i64>)> {
    ranked
        .rows
        .iter()
        .map(|r| (r.member.clone(), r.score.to_bits(), r.labels.clone(), r.lanes.clone()))
        .collect()
}

/// Twelve snapshots, at 100, 200 ... 1200. Item `m` (1 to 6) is present at snapshot `i` unless
/// `(m + i)` is a multiple of 3, so members come and go; every lane value is a function of
/// `(m, i)` and the label is where the item was placed that time, `(m + i) % 3`.
fn seed(engine: &TemporalEngine, key: &str) -> Vec<(u64, Vec<Cell>)> {
    let snapshots: Vec<(u64, Vec<Cell>)> = (1..=12u64)
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
    append(engine, key, snapshots.iter().map(|(ts, cells)| point(*ts, cells)).collect());
    snapshots
}

#[test]
fn a_ranking_over_the_rolled_buckets_answers_what_it_answered_over_the_snapshots_they_replaced() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), None);
    let key = stored_key(7);
    let seeded = seed(&engine, &key);

    let scores = [
        lane("impressions"),
        lane("clicks"),
        lane("peak"),
        lane("seen"),
        CellScore::Ratio { numerator: "clicks".to_string(), denominator: "impressions".to_string() },
    ];
    let before: Vec<RankedCells> = scores.iter().map(|s| rank(&engine, &key, s.clone())).collect();
    assert!(before.iter().all(|b| b.snapshots == 12 && b.rows.len() == 6));

    // Cutoff 1000, buckets of 400: 100..300 fold into 0, 400..700 into 400, 800..900 into 800.
    // 1000, 1100 and 1200 are at or after the cutoff and stay.
    let response = rollup_response(&engine, &key, roll(1000, 400, None));
    let [snapshots_before, snapshots_after, cells_before, cells_after, dropped] = receipt(&response);

    let rolled: Vec<(u64, BTreeSet<u64>)> = [(0u64, 100u64..=300), (400, 400..=700), (800, 800..=900)]
        .into_iter()
        .map(|(start, stamps)| {
            let members: BTreeSet<u64> = seeded
                .iter()
                .filter(|(ts, _)| stamps.contains(ts))
                .flat_map(|(_, cells)| cells.iter().map(|c| match c.member { Member::U64(m) => m, _ => unreachable!() }))
                .collect();
            (start, members)
        })
        .collect();
    let below: u64 = seeded.iter().filter(|(ts, _)| *ts < 1000).map(|(_, cells)| cells.len() as u64).sum();
    let after: u64 = rolled.iter().map(|(_, members)| members.len() as u64).sum();
    assert_eq!([snapshots_before, snapshots_after], [12, 6]);
    assert_eq!([cells_before, cells_after, dropped], [below, after, 0]);
    assert!(below > after, "DENOMINATOR: members repeat across a bucket, so the rollup folded something");

    let rolled_series = decoded(&engine, &key);
    assert_eq!(rolled_series.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(), vec![0, 400, 800, 1000, 1100, 1200]);
    for ((start, members), (ts, cells)) in rolled.iter().zip(&rolled_series) {
        assert_eq!(ts, start);
        let stored: BTreeSet<u64> = cells.iter().map(|c| match c.member { Member::U64(m) => m, _ => unreachable!() }).collect();
        assert_eq!(&stored, members);
    }
    // The snapshots at or after the cutoff are the bytes that were written.
    assert_eq!(&rolled_series[3..], &seeded[9..]);

    for (score, before) in scores.iter().zip(&before) {
        let after = rank(&engine, &key, score.clone());
        assert_eq!(after.snapshots, 6);
        assert_eq!(answers(&after), answers(before), "{score:?} ranked another way after the rollup");
    }
}

#[test]
fn a_cold_reload_reads_the_rolled_bytes_and_rolling_again_changes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), None);
    let key = stored_key(8);
    seed(&engine, &key);
    let cap = Some(CellCapSpec { group_by: Some("slot".to_string()), score: lane("impressions"), keep: 1 });
    let request = roll(1000, 400, cap);
    let first = receipt(&rollup_response(&engine, &key, request.clone()));
    assert!(first[4] > 0, "DENOMINATOR: the cap dropped something for the second run to be idempotent about");
    let rolled = series(&engine, &key);
    engine.unload_shard(1);
    drop(engine);

    let reloaded = engine_with_cache(dir.path(), "cache-b", None);
    assert_eq!(series(&reloaded, &key), rolled, "a cold reload served other bytes");

    // The same request again: a rolled bucket is already at its start and already capped.
    let again = receipt(&rollup_response(&reloaded, &key, request));
    assert_eq!(again, [6, 6, again[2], again[2], 0]);
    assert_eq!(series(&reloaded, &key), rolled);
}

#[test]
fn replaying_the_log_reaches_the_bytes_the_live_engine_reached() {
    let dir = tempfile::tempdir().expect("tempdir");
    let async_config = crate::control::Config { version: 2, async_storage: true, feature_max_size: 5_000, ..Default::default() };
    let engine = engine_on(dir.path(), Some(async_config));
    let key = stored_key(9);
    seed(&engine, &key);
    let cap = Some(CellCapSpec { group_by: None, score: lane("impressions"), keep: 3 });
    let landed = receipt(&rollup_response(&engine, &key, roll(900, 300, cap)));
    assert!(landed[4] > 0 && landed[0] > landed[1]);
    let live = series(&engine, &key);
    assert_eq!(live.len() as u64, landed[1]);
    // Async storage defers the blocks: the log is what holds the write when the process ends.
    assert!(engine.write_ahead_log_store().stats(1).writes >= 2);
    drop(engine);

    let restarted = engine_with_cache(dir.path(), "cache-b", None);
    assert_eq!(series(&restarted, &key), live, "replay planned another rollup than the live one");
}

#[test]
fn the_same_request_on_two_engines_leaves_the_same_bytes() {
    let request = roll(
        1100,
        500,
        Some(CellCapSpec {
            group_by: Some("slot".to_string()),
            score: CellScore::Ratio { numerator: "clicks".to_string(), denominator: "impressions".to_string() },
            keep: 1,
        }),
    );
    let mut outcomes = Vec::new();
    for user in [1u64, 1] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path(), None);
        let key = stored_key(user);
        seed(&engine, &key);
        let landed = receipt(&rollup_response(&engine, &key, request.clone()));
        outcomes.push((landed, series(&engine, &key)));
    }
    assert!(outcomes[0].0[4] > 0 && outcomes[0].0[0] > outcomes[0].0[1], "DENOMINATOR: the request did something");
    assert_eq!(outcomes[0], outcomes[1]);
}

#[test]
fn a_request_the_key_cannot_serve_is_refused_and_writes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), None);
    let key = stored_key(10);
    seed(&engine, &key);
    let stored = series(&engine, &key);
    let writes = engine.write_ahead_log_store().stats(1).writes;

    let refused = |spec: CellRollupSpec, needle: &str| {
        let response = rollup_response(&engine, &key, spec);
        assert_eq!((response.status.ok, response.status.code.as_str()), (false, "invalid_argument"), "{:?}", response.status);
        assert!(response.status.message.contains(needle), "{:?} does not name `{needle}`", response.status.message);
        assert_eq!(series(&engine, &key), stored, "a refused rollup changed the series");
        assert_eq!(engine.write_ahead_log_store().stats(1).writes, writes, "a refused rollup reached the log");
    };
    let capped = |group_by: Option<&str>, score: CellScore, keep: usize| {
        roll(1000, 400, Some(CellCapSpec { group_by: group_by.map(str::to_string), score, keep }))
    };
    refused(roll(1000, 0, None), "bucket_ms must be at least 1");
    refused(capped(Some("gid"), lane("clicks"), 1), "gid");
    refused(capped(None, lane("views"), 1), "views");
    refused(capped(None, CellScore::Time, 1), "time");
    refused(capped(None, CellScore::Decay { lane: "clicks".to_string(), half_life_ms: 10 }, 1), "decay");
    refused(capped(None, lane("clicks"), 0), "at least 1");
    refused(capped(None, lane("clicks"), 4097), "over the limit");

    // A feature key that holds something other than cells.
    append_plain(&engine, "plain");
    let response = rollup_response(&engine, "plain", roll(10, 5, None));
    assert!(!response.status.ok && response.status.code == "invalid_argument", "{:?}", response.status);
}

fn append_plain(engine: &TemporalEngine, key: &str) {
    append(engine, key, vec![FeaturePoint { timestamp_ms: 1, value: b"12".to_vec() }]);
}

#[test]
fn each_bound_is_accepted_at_exactly_the_limit_and_refused_one_past_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), Some(roomy()));
    let key = stored_key(11);
    let one = |ts: u64| point(ts, &[cell(1, 0, [1, 1, 1, 1])]);

    // 1024 snapshots below the cutoff are rolled; 1025 are refused, naming the limit.
    append(&engine, &key, (1..=1025u64).map(one).collect());
    let over = rollup_response(&engine, &key, roll(1026, 1_000_000, None));
    assert_eq!((over.status.ok, over.status.code.as_str()), (false, "invalid_argument"), "{:?}", over.status);
    assert!(over.status.message.contains("1024"), "{}", over.status.message);
    assert_eq!(series(&engine, &key).len(), 1025, "the refusal wrote nothing");
    let at = receipt(&rollup_response(&engine, &key, roll(1025, 1_000_000, None)));
    assert_eq!([at[0], at[1]], [1025, 2], "1024 below the cutoff fold into one, the 1025th stays");

    // keep: 1 and 4096 are accepted; the engine key holds one cell, so nothing is dropped.
    for keep in [1usize, 4096] {
        let capped = roll(2000, 1_000_000, Some(CellCapSpec { group_by: None, score: lane("clicks"), keep }));
        assert_eq!(receipt(&rollup_response(&engine, &key, capped))[4], 0);
    }
    // bucket_ms: 1 is accepted.
    assert!(rollup_response(&engine, &key, roll(2000, 1, None)).status.ok);
}

#[test]
fn an_absent_key_and_an_expired_key_roll_nothing_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), None);
    let key = stored_key(12);
    assert_eq!(receipt(&rollup_response(&engine, &key, roll(1000, 100, None))), [0; 5]);
    assert!(series(&engine, &key).is_empty(), "rolling up an absent key created a series");

    seed(&engine, &key);
    assert_eq!(receipt(&rollup_response(&engine, &key, roll(0, 100, None)))[..2], [12, 12], "CONTROL: a cutoff of 0 rolls nothing");
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");
        let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
        crate::engine::set_expiry(shard, key.clone(), past);
    }
    assert_eq!(receipt(&rollup_response(&engine, &key, roll(10_000, 100, None))), [0; 5]);
}

#[test]
fn a_rollup_is_a_write_the_log_can_carry_in_every_score_spelling() {
    use crate::shared_store::{command_from_protobuf_bytes, command_protobuf_bytes};
    let scores = [
        lane("clicks"),
        CellScore::Ratio { numerator: "clicks".to_string(), denominator: "impressions".to_string() },
        CellScore::Wilson { successes: "clicks".to_string(), trials: "impressions".to_string(), z: 1.96 },
        CellScore::Member,
        CellScore::Time,
        CellScore::Weighted { lanes: vec!["clicks".to_string(), "peak".to_string()], weights: vec![0.5, -2.0] },
    ];
    let mut seen_bytes = BTreeSet::new();
    for score in scores {
        for group_by in [None, Some("slot".to_string())] {
            let command = Command::CellRollup {
                key: "k".to_string(),
                spec: roll(123, 45, Some(CellCapSpec { group_by, score: score.clone(), keep: 7 })),
            };
            assert!(crate::engine::is_write_command(&command));
            let bytes = command_protobuf_bytes(&command).expect("a rollup has a protobuf encoding");
            assert!(serde_json::from_slice::<serde_json::Value>(&bytes).is_err(), "the bytes are JSON");
            assert_eq!(command_from_protobuf_bytes(&bytes).unwrap(), command);
            seen_bytes.insert(bytes);
        }
    }
    assert_eq!(seen_bytes.len(), 12, "every score spelling and group spelling has its own bytes");
    let uncapped = Command::CellRollup { key: "k".to_string(), spec: roll(1, 2, None) };
    let bytes = command_protobuf_bytes(&uncapped).unwrap();
    assert_eq!(command_from_protobuf_bytes(&bytes).unwrap(), uncapped);
}

/// THE WORKED EXAMPLE. One user (the ENTITY), one channel (a DIMENSION), a label `slot` and the
/// lanes `impressions`, `clicks` and `peak`. Minute snapshots are rolled up per hour once they are
/// older than the cutoff, and each rolled hour keeps the best item of each slot by click rate.
///
///   minute 1  (60 000)   item 1: 10 shown, 1 click, peak 4     item 3: 100 shown, 1 click, peak 3
///   minute 2  (120 000)  item 1: 20 shown, 0 clicks, peak 9    item 2:   5 shown, 5 clicks, peak 2
///   minute 3  (180 000)  item 1: 30 shown, 2 clicks, peak 5    item 2:   5 shown, 0 clicks, peak 1
///   minute 61 (3 660 000), at or after the cutoff of 3 600 000, so it stays as it is.
///
/// Rolled into the hour that starts at 0, with slots item 1 and 2 in slot 1 and item 3 in slot 2:
///   item 1: 60 shown, 3 clicks, peak 9     item 2: 10 shown, 5 clicks, peak 2     item 3: 100, 1, 3
/// Capped to one item per slot by clicks / impressions: slot 1 holds item 1 at 3/60 = 0.05 and
/// item 2 at 5/10 = 0.5, so item 2 stays and item 1 goes. Slot 2 has only item 3.
#[test]
fn a_worked_example_of_a_rollup() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), None);
    let key = stored_key(42);
    append(
        &engine,
        &key,
        vec![
            point(60_000, &[cell(1, 1, [10, 1, 4, 1]), cell(3, 2, [100, 1, 3, 1])]),
            point(120_000, &[cell(1, 1, [20, 0, 9, 2]), cell(2, 1, [5, 5, 2, 2])]),
            point(180_000, &[cell(1, 1, [30, 2, 5, 3]), cell(2, 1, [5, 0, 1, 3])]),
            point(3_660_000, &[cell(1, 1, [7, 0, 0, 61])]),
        ],
    );
    let hour = 3_600_000;

    // Without a cap: the hour is three items and nothing is dropped.
    let uncapped = rollup_response(&engine, &key, roll(hour, hour, None));
    assert_eq!(receipt(&uncapped), [4, 2, 6, 3, 0]);
    let stored = decoded(&engine, &key);
    assert_eq!(stored.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(), vec![0, 3_660_000]);
    assert_eq!(
        stored[0].1,
        vec![
            cell(1, 1, [60, 3, 9, 3]),
            cell(2, 1, [10, 5, 2, 3]),
            cell(3, 2, [100, 1, 3, 1]),
        ]
    );

    // The cap, on the same hour: one item per slot by click rate. The hour is already rolled, so
    // only the cap acts on it.
    let capped = roll(
        hour,
        hour,
        Some(CellCapSpec {
            group_by: Some("slot".to_string()),
            score: CellScore::Ratio { numerator: "clicks".to_string(), denominator: "impressions".to_string() },
            keep: 1,
        }),
    );
    assert_eq!(receipt(&rollup_response(&engine, &key, capped)), [2, 2, 3, 2, 1]);
    assert_eq!(decoded(&engine, &key)[0].1, vec![cell(2, 1, [10, 5, 2, 3]), cell(3, 2, [100, 1, 3, 1])]);
}
