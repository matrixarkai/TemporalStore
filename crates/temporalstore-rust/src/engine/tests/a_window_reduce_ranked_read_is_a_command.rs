// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE RANKED CELL READ AS A COMMAND, THROUGH THE ENGINE.
//!
//! The pure ranking is tested in `window_reduce_rank`; these tests are about what only the engine path can
//! get wrong: that the samples it reads are the samples that were stored (live, and after a
//! restart that reads every block cold), that an absent or expired key ranks nothing, that a read
//! the key cannot serve is refused with its reason and a read that breaks a bound is refused at
//! admission, and that a header that gained a lane after older samples were written still reads
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
    WindowReduceAnchor, WindowReduceLaneQuota, CellMemberFilter, CellMemberId, WindowReduceQuota, WindowReduceRankSpec, WindowReduceScore, WindowReduceWindow,
    FeatureFilter, FeatureFilterOp, RankedCells,
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

/// Four samples under [shown (Sum), peak (Max)]. Reduced over all four:
///   m1 shown 30 peak 9    m2 shown 60 peak 8    m3 shown 200 peak 6    m4 shown 10 peak 2
fn seed(engine: &TemporalEngine, key: &str) {
    let h = two_lanes();
    put(engine, key, &h, 100, &[cell(1, 1, vec![10, 5]), cell(2, 2, vec![20, 7]), cell(3, 1, vec![100, 2])]);
    put(engine, key, &h, 200, &[cell(1, 1, vec![10, 9]), cell(2, 3, vec![30, 3]), cell(4, 2, vec![8, 1])]);
    put(engine, key, &h, 300, &[cell(1, 2, vec![10, 4]), cell(3, 1, vec![100, 6])]);
    put(engine, key, &h, 400, &[cell(2, 2, vec![10, 8]), cell(4, 2, vec![2, 2])]);
}

fn spec(window: WindowReduceWindow, score: WindowReduceScore, top_k: usize) -> WindowReduceRankSpec {
    WindowReduceRankSpec { window, filters: Vec::new(), score, top_k, project: Vec::new(), members: Default::default(), anchor: Default::default(), quota: Default::default(), sample_info: false }
}

const ALL: WindowReduceWindow = WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX };

fn lane(name: &str) -> WindowReduceScore {
    WindowReduceScore::Lane { lane: name.to_string() }
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

#[test]
fn a_ranked_read_answers_from_what_was_stored_live_and_after_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(7);
    seed(&engine, &key);

    let by_sum = spec(ALL, lane("shown"), 10);
    let by_max = spec(ALL, lane("peak"), 10);
    let windowed = spec(WindowReduceWindow::Between { start_ms: 200, end_ms: 300 }, lane("shown"), 10);

    let live = rank(&engine, &key, by_sum.clone());
    // Sum over every sample: 200 60 30 10.
    assert_eq!(members(&live), vec![3, 2, 1, 4]);
    assert_eq!(live.rows.iter().map(|r| r.score as i64).collect::<Vec<_>>(), vec![200, 60, 30, 10]);
    assert_eq!(live.samples, 4);
    assert!(live.refused.is_empty());
    // Max, not sum: a sum of the peaks would put m1 and m2 level at 18.
    assert_eq!(members(&rank(&engine, &key, by_max.clone())), vec![1, 2, 3, 4]);
    // The window: samples 200 and 300 only. m1 20, m2 30, m3 100, m4 8.
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
    engine.shards.with_state_mut(1, |state| {
        let shard = state.expect("shard 1 is loaded");
        let past = crate::engine::resolve_now_ms().saturating_sub(60_000);
        crate::engine::set_expiry(shard, key.clone(), past);
    });
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

    let status_of = |query: WindowReduceRankSpec| {
        exec(&engine, Command::WindowReduceRank { key: key.clone(), spec: query }).status
    };
    // top-k: 1000 is served, 1001 and 0 are refused before any sample is read.
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
            WindowReduceScore::Weighted { lanes: (0..n).map(|i| format!("l{i}")).collect(), weights: vec![1.0; n] },
            5,
        )
    };
    assert!(status_of(weighted(8)).ok);
    assert!(!status_of(weighted(9)).ok);
    // window: LastN 512 is admitted, 513 refused.
    assert!(status_of(spec(WindowReduceWindow::LastN { n: 512 }, lane("shown"), 5)).ok);
    assert!(!status_of(spec(WindowReduceWindow::LastN { n: 513 }, lane("shown"), 5)).ok);
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
    // 512 samples: the newest 512, summing to 512.
    let at = rank(&engine, &key, spec(WindowReduceWindow::Between { start_ms: 2, end_ms: u64::MAX }, lane("shown"), 1));
    assert_eq!((at.samples, at.rows[0].score as u64), (512, 512));
    // 513: refused, with rows left empty.
    let over = rank(&engine, &key, spec(ALL, lane("shown"), 1));
    assert!(over.refused.contains("samples in the window"), "{}", over.refused);
    assert!(over.rows.is_empty());
    // The newest 512 by count is served.
    assert_eq!(rank(&engine, &key, spec(WindowReduceWindow::LastN { n: 512 }, lane("shown"), 1)).rows[0].score as u64, 512);
}

#[test]
fn a_lane_added_later_reads_as_zero_in_the_samples_written_before_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(5);
    let before = header(&[("shown", LaneReducer::Sum)]);
    let after = header(&[("shown", LaneReducer::Sum), ("clicked", LaneReducer::Sum)]);
    put(&engine, &key, &before, 100, &[cell(1, 0, vec![10]), cell(2, 0, vec![20])]);
    put(&engine, &key, &after, 200, &[cell(1, 0, vec![10, 4])]);

    let ranked = rank(&engine, &key, spec(ALL, lane("clicked"), 5));
    // Only the later sample carries `clicked`: m1 4; m2 has none and reads 0.
    assert_eq!(members(&ranked), vec![1, 2]);
    assert_eq!(ranked.rows.iter().map(|r| r.lanes.clone()).collect::<Vec<_>>(), vec![vec![20, 4], vec![20, 0]]);
    assert_eq!(members(&rank(&engine, &key, spec(ALL, lane("shown"), 5))), vec![1, 2]);
}

// ---- the decay score, through the same path ------------------------------------------------------

fn decay(lane_name: &str, half_life_ms: u64) -> WindowReduceScore {
    WindowReduceScore::Decay { lane: lane_name.to_string(), half_life_ms }
}

fn scores(ranked: &RankedCells) -> Vec<f64> {
    ranked.rows.iter().map(|r| r.score).collect()
}

#[test]
fn a_decay_score_answers_its_hand_computed_value_live_and_after_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(8);
    seed(&engine, &key);

    // Half-life 100, as of the newest sample of the window (400). Weights: 400 -> 1, 300 -> 1/2,
    // 200 -> 1/4, 100 -> 1/8. The `shown` values the samples store:
    //   m1 10@100 10@200 10@300 = 8.75      m2 20@100 30@200 10@400 = 20
    //   m3 100@100 100@300      = 62.5      m4 8@200 2@400          = 4
    let all = rank(&engine, &key, spec(ALL, decay("shown", 100), 10));
    assert_eq!((members(&all), scores(&all)), (vec![3, 2, 1, 4], vec![62.5, 20.0, 8.75, 4.0]));
    // The window 100..=300 is as of 300: m3 100/4 + 100, m2 20/4 + 30/2, m1 10/4 + 10/2 + 10, m4 8/2.
    let early = rank(&engine, &key, spec(WindowReduceWindow::Between { start_ms: 100, end_ms: 300 }, decay("shown", 100), 10));
    assert_eq!((members(&early), scores(&early)), (vec![3, 2, 1, 4], vec![125.0, 20.0, 17.5, 4.0]));
    // `peak` is a Max lane; the decay reads what each sample stored: m2 7/8 + 3/4 + 8 = 9.625 leads
    // where the lane's own Max puts m1 (9) first.
    let peaks = rank(&engine, &key, spec(ALL, decay("peak", 100), 10));
    assert_eq!((members(&peaks), peaks.rows[0].score), (vec![2, 1, 3, 4], 9.625));
    assert_eq!(members(&rank(&engine, &key, spec(ALL, lane("peak"), 10))), vec![1, 2, 3, 4]);

    drop(engine);
    let reloaded = engine_on(dir.path());
    assert_eq!(rank(&reloaded, &key, spec(ALL, decay("shown", 100), 10)), all);
    // A name the key does not declare is a served refusal, as for every other score.
    assert!(rank(&reloaded, &key, spec(ALL, decay("views", 100), 10)).refused.contains("views"));
}

#[test]
fn a_half_life_is_refused_at_admission_past_its_limits_and_served_at_them() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(9);
    seed(&engine, &key);
    let status_of = |half_life_ms: u64| {
        exec(&engine, Command::WindowReduceRank { key: key.clone(), spec: spec(ALL, decay("shown", half_life_ms), 5) }).status
    };
    let longest = crate::engine::window_reduce_rank::MAX_HALF_LIFE_MS;
    assert_eq!(longest, 315_360_000_000);
    assert!(status_of(1).ok);
    assert!(status_of(longest).ok);
    for refused in [0, longest + 1, u64::MAX] {
        let status = status_of(refused);
        assert_eq!((status.ok, status.code.as_str()), (false, "invalid_argument"), "half-life {refused}: {status:?}");
    }
    // Served at the limits means ranked, not just admitted.
    let at_limit = rank(&engine, &key, spec(ALL, decay("shown", longest), 5));
    assert_eq!(members(&at_limit), vec![3, 2, 1, 4]);
}

#[test]
fn a_decay_total_past_the_integer_range_does_not_wrap_through_the_engine() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(10);
    let h = header(&[("shown", LaneReducer::Sum)]);
    put(&engine, &key, &h, 100, &[cell(1, 0, vec![i64::MAX]), cell(2, 0, vec![i64::MIN])]);
    put(&engine, &key, &h, 200, &[cell(1, 0, vec![i64::MAX])]);
    let max = i64::MAX as f64; // 2^63 once rounded
    // m1: MAX at weight 1/2 plus MAX at weight 1 is 1.5 * 2^63, past i64::MAX and never negative.
    // m2: MIN at weight 1/2 is -2^62.
    let ranked = rank(&engine, &key, spec(ALL, decay("shown", 100), 5));
    assert_eq!((members(&ranked), scores(&ranked)), (vec![1, 2], vec![1.5 * max, -0.5 * max]));
    assert_eq!(ranked.unscored, 0);
}

// ---- the optional controls on the command: a member set, an anchor, a quota --------------------

fn id(v: u64) -> CellMemberId {
    CellMemberId::U64(v)
}

fn with_members(include: Option<Vec<u64>>, exclude: Vec<u64>, base: WindowReduceRankSpec) -> WindowReduceRankSpec {
    WindowReduceRankSpec {
        members: CellMemberFilter {
            include: include.map(|set| set.into_iter().map(id).collect()),
            exclude: exclude.into_iter().map(id).collect(),
        },
        ..base
    }
}

fn status_of(engine: &TemporalEngine, key: &str, query: WindowReduceRankSpec) -> (bool, String) {
    let status = exec(engine, Command::WindowReduceRank { key: key.to_string(), spec: query }).status;
    (status.ok, status.code.clone())
}

/// The `shown` values `seed` stores: (member, shown). The fold below reads this table and nothing
/// the engine produced.
const SEEDED: [(u64, i64); 10] = [(1, 10), (2, 20), (3, 100), (1, 10), (2, 30), (4, 8), (1, 10), (3, 100), (2, 10), (4, 2)];

#[test]
fn a_member_set_changes_the_answer_exactly_as_an_independent_fold_says() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(11);
    seed(&engine, &key);

    // Every include set (none, and each non-empty subset of m1..m4) against every exclude set.
    let universe = [1u64, 2, 3, 4];
    let subsets: Vec<Vec<u64>> =
        (0..16u32).map(|bits| universe.iter().copied().filter(|m| (bits >> (m - 1)) & 1 == 1).collect()).collect();
    let (mut reads, mut narrowed) = (0, 0);
    for include in std::iter::once(None).chain(subsets.iter().skip(1).map(Some)) {
        for exclude in &subsets {
            // The fold: a table of shown totals over the seeded rows, a member kept when the sets say so.
            let mut totals: std::collections::BTreeMap<u64, i64> = Default::default();
            for (member, shown) in SEEDED.iter() {
                if include.map_or(true, |set| set.contains(member)) && !exclude.contains(member) {
                    *totals.entry(*member).or_insert(0) += shown;
                }
            }
            let mut want: Vec<(u64, i64)> = totals.into_iter().collect();
            want.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

            let query = with_members(include.cloned(), exclude.clone(), spec(ALL, lane("shown"), 10));
            let got = rank(&engine, &key, query);
            let got_pairs: Vec<(u64, i64)> =
                members(&got).into_iter().zip(got.rows.iter().map(|r| r.score as i64)).collect();
            assert_eq!(got_pairs, want, "include {include:?} exclude {exclude:?}");
            reads += 1;
            narrowed += usize::from(want.len() < 4);
        }
    }
    assert_eq!(reads, 16 * 16);
    assert!(narrowed > 150, "{narrowed} reads were narrowed");

    // ANDed with a predicate, which cuts before the reduction: peak > 4 keeps (100,m1) (100,m2)
    // (200,m1) (300,m3) (400,m2), so m1 20 and m2 30 among m1, m2.
    let mut query = with_members(Some(vec![1, 2]), vec![], spec(ALL, lane("shown"), 10));
    query.filters = vec![FeatureFilter { field: "peak".to_string(), op: FeatureFilterOp::GreaterThan, value: 4 }];
    let both = rank(&engine, &key, query.clone());
    assert_eq!((members(&both), scores(&both)), (vec![2, 1], vec![30.0, 20.0]));

    // And the same after a restart that reads every block cold.
    drop(engine);
    let reloaded = engine_on(dir.path());
    assert_eq!(rank(&reloaded, &key, query), both);
}

#[test]
fn the_member_set_is_bounded_at_admission_and_accepted_at_exactly_the_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(12);
    seed(&engine, &key);
    let many = |n: u64| (1..=n).collect::<Vec<u64>>();
    let plain = || spec(ALL, lane("shown"), 10);
    let outcome = |query: WindowReduceRankSpec| status_of(&engine, &key, query);
    let refused = (false, "invalid_argument".to_string());
    // 1024 a side is served; 1025 is refused before any sample is read.
    assert!(outcome(with_members(Some(many(1024)), vec![], plain())).0);
    assert_eq!(outcome(with_members(Some(many(1025)), vec![], plain())), refused);
    assert!(outcome(with_members(None, many(1024), plain())).0);
    assert_eq!(outcome(with_members(None, many(1025), plain())), refused);
    // A set that names nothing is refused rather than answered empty.
    assert_eq!(outcome(with_members(Some(vec![]), vec![], plain())), refused);
    // At the limit the set is a real set: 1024 ids holding m1..m4 keep all four.
    assert_eq!(members(&rank(&engine, &key, with_members(Some(many(1024)), vec![], plain()))), vec![3, 2, 1, 4]);
}

#[test]
fn an_anchored_window_reads_the_newest_sample_whatever_the_clock_says() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(13);
    seed(&engine, &key); // samples at 100 200 300 400
    let anchored = |duration_ms: u64| WindowReduceRankSpec {
        anchor: WindowReduceAnchor::Newest,
        ..spec(WindowReduceWindow::Last { duration_ms }, lane("shown"), 10)
    };
    // [301, 400]: the sample at 400 alone: m2 10, m4 2.
    let one = rank(&engine, &key, anchored(100));
    assert_eq!((members(&one), scores(&one), one.samples), (vec![2, 4], vec![10.0, 2.0], 1));
    // [101, 400]: 200, 300 and 400. m1 20, m2 30 + 10, m3 100, m4 8 + 2.
    let three = rank(&engine, &key, anchored(300));
    assert_eq!(
        (members(&three), scores(&three), three.samples),
        (vec![3, 2, 1, 4], vec![100.0, 40.0, 20.0, 10.0], 3)
    );
    // The start is inclusive: 301 reaches back to 100.
    assert_eq!(rank(&engine, &key, anchored(301)).samples, 4);
    // A duration of 0 holds nothing.
    assert_eq!(rank(&engine, &key, anchored(0)).samples, 0);
    // The request clock is the real clock, far past every stamp: the same window on it is empty.
    let on_the_clock = rank(&engine, &key, spec(WindowReduceWindow::Last { duration_ms: 100 }, lane("shown"), 10));
    assert_eq!((on_the_clock.samples, on_the_clock.rows.len()), (0, 0));
    // An anchor on any other window is refused at admission.
    for window in [ALL, WindowReduceWindow::Since { start_ms: 1 }, WindowReduceWindow::LastN { n: 2 }] {
        let status =
            status_of(&engine, &key, WindowReduceRankSpec { anchor: WindowReduceAnchor::Newest, ..spec(window, lane("shown"), 10) });
        assert_eq!(status, (false, "invalid_argument".to_string()));
    }
    // A key with no sample is an empty answer.
    let absent = rank(&engine, &stored_key(99), anchored(100));
    assert_eq!((absent.rows.len(), absent.refused.as_str()), (0, ""));

    drop(engine);
    let reloaded = engine_on(dir.path());
    assert_eq!(rank(&reloaded, &key, anchored(100)), one);
}

/// One sample of eight members ranked by `shown` 80 .. 10; `clicked` is non-zero for m3, m6, m8.
fn ladder_key(engine: &TemporalEngine, key: &str) {
    let h = header(&[("shown", LaneReducer::Sum), ("clicked", LaneReducer::Sum)]);
    let rows: [(u64, i64, i64); 8] =
        [(1, 80, 0), (2, 70, 0), (3, 60, 1), (4, 50, 0), (5, 40, 0), (6, 30, 1), (7, 20, 0), (8, 10, 1)];
    let cells: Vec<Cell> = rows.iter().map(|(m, shown, clicked)| cell(*m, 0, vec![*shown, *clicked])).collect();
    put(engine, key, &h, 100, &cells);
}

fn quota(lanes: &[(&str, usize)], others: usize, top_k: usize) -> WindowReduceRankSpec {
    WindowReduceRankSpec {
        quota: WindowReduceQuota {
            lanes: lanes.iter().map(|(lane, count)| WindowReduceLaneQuota { lane: lane.to_string(), count: *count }).collect(),
            others,
        },
        ..spec(ALL, lane("shown"), top_k)
    }
}

#[test]
fn a_quota_selects_exactly_as_a_fold_of_the_ranking_says() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(14);
    ladder_key(&engine, &key);
    let clicked: [(u64, i64); 8] = [(1, 0), (2, 0), (3, 1), (4, 0), (5, 0), (6, 1), (7, 0), (8, 1)];

    // The fold: walk the best `k` members; keep one that has `clicked` while the lane is short, else
    // up to `others`; stop when the lane is full. Written without the ranked read.
    let fold = |count: usize, others: usize, k: usize| -> Vec<u64> {
        let (mut short, mut spare, mut kept) = (count, others, Vec::new());
        for (member, value) in clicked.iter().take(k) {
            if short > 0 && *value != 0 {
                short -= 1;
                kept.push(*member);
            } else if spare > 0 {
                spare -= 1;
                kept.push(*member);
            }
            if short == 0 {
                break;
            }
        }
        kept
    };
    let mut compared = 0;
    for count in 1..=3 {
        for others in 0..=3 {
            for k in 1..=8 {
                let got = rank(&engine, &key, quota(&[("clicked", count)], others, k));
                assert_eq!(members(&got), fold(count, others, k), "count {count} others {others} k {k}");
                assert!(got.rows.len() <= k);
                compared += got.rows.len();
            }
        }
    }
    assert!(compared > 100);
    // The boundary the fold implies: the second `clicked` member is the sixth best.
    assert_eq!(members(&rank(&engine, &key, quota(&[("clicked", 2)], 0, 6))), vec![3, 6]);
    assert_eq!(members(&rank(&engine, &key, quota(&[("clicked", 2)], 0, 5))), vec![3]);
    // Without a quota the same read is the plain top-k.
    assert_eq!(members(&rank(&engine, &key, spec(ALL, lane("shown"), 4))), vec![1, 2, 3, 4]);

    drop(engine);
    let reloaded = engine_on(dir.path());
    assert_eq!(members(&rank(&reloaded, &key, quota(&[("clicked", 2)], 2, 8))), vec![1, 2, 3, 6]);
}

#[test]
fn a_quota_is_refused_at_admission_past_its_bounds_and_served_at_them() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(15);
    ladder_key(&engine, &key);
    let outcome = |query: WindowReduceRankSpec| status_of(&engine, &key, query);
    let refused = (false, "invalid_argument".to_string());
    // The reservation, counts plus others, is at most 1000.
    assert!(outcome(quota(&[("clicked", 1000)], 0, 8)).0);
    assert_eq!(outcome(quota(&[("clicked", 1001)], 0, 8)), refused);
    assert!(outcome(quota(&[("clicked", 999)], 1, 8)).0);
    assert_eq!(outcome(quota(&[("clicked", 999)], 2, 8)), refused);
    assert!(outcome(quota(&[], 1000, 8)).0);
    assert_eq!(outcome(quota(&[], 1001, 8)), refused);
    // A count of zero and a lane named twice.
    assert_eq!(outcome(quota(&[("clicked", 0)], 0, 8)), refused);
    assert_eq!(outcome(quota(&[("clicked", 1), ("clicked", 1)], 0, 8)), refused);
    // Lanes in a quota: 16 distinct names are admitted (and then refused by name as a served
    // refusal, the key declaring two lanes); 17 are refused at admission.
    let named = |n: usize| WindowReduceRankSpec {
        quota: WindowReduceQuota {
            lanes: (0..n).map(|i| WindowReduceLaneQuota { lane: format!("l{i}"), count: 1 }).collect(),
            others: 0,
        },
        ..spec(ALL, lane("shown"), 8)
    };
    assert!(outcome(named(16)).0);
    assert!(rank(&engine, &key, named(16)).refused.contains("l0"));
    assert_eq!(outcome(named(17)), refused);
    // A lane the key does not declare is a served refusal naming it, like every other name.
    assert!(rank(&engine, &key, quota(&[("views", 1)], 0, 8)).refused.contains("views"));
}

#[test]
fn a_query_that_uses_none_of_the_controls_is_encoded_and_answered_as_it_always_was() {
    // The encoding: none of the new keys appears, so a spec written by an older client and a spec
    // written today are the same bytes.
    let plain = spec(WindowReduceWindow::Last { duration_ms: 150 }, lane("shown"), 5);
    let json = serde_json::to_string(&plain).unwrap();
    assert_eq!(
        json,
        r#"{"window":{"last":{"duration_ms":150}},"filters":[],"score":{"lane":{"lane":"shown"}},"top_k":5,"project":[]}"#
    );
    // A spec that lacks the keys still reads, as the spec with the controls at rest.
    assert_eq!(serde_json::from_str::<WindowReduceRankSpec>(&json).unwrap(), plain);
    // And one that names them at rest is the same spec.
    let explicit = r#"{"window":{"last":{"duration_ms":150}},"filters":[],"score":{"lane":{"lane":"shown"}},"top_k":5,"project":[],"anchor":"request","members":{},"quota":{}}"#;
    assert_eq!(serde_json::from_str::<WindowReduceRankSpec>(explicit).unwrap(), plain);

    // With the controls set, their encoding is the one the docs show.
    let used = WindowReduceRankSpec {
        members: CellMemberFilter { include: Some(vec![id(1)]), exclude: vec![id(2)] },
        anchor: WindowReduceAnchor::Newest,
        quota: WindowReduceQuota { lanes: vec![WindowReduceLaneQuota { lane: "peak".to_string(), count: 2 }], others: 1 },
        ..plain.clone()
    };
    let used_json = serde_json::to_string(&used).unwrap();
    assert_eq!(
        used_json,
        r#"{"window":{"last":{"duration_ms":150}},"filters":[],"score":{"lane":{"lane":"shown"}},"top_k":5,"project":[],"members":{"include":[{"u64":1}],"exclude":[{"u64":2}]},"anchor":"newest","quota":{"lanes":[{"lane":"peak","count":2}],"others":1}}"#
    );
    assert_eq!(serde_json::from_str::<WindowReduceRankSpec>(&used_json).unwrap(), used);

    // The answer: the same through the engine whether the spec was built or read from its bytes.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(16);
    seed(&engine, &key);
    let all = spec(ALL, lane("shown"), 10);
    let from_bytes: WindowReduceRankSpec = serde_json::from_str(&serde_json::to_string(&all).unwrap()).unwrap();
    assert_eq!(rank(&engine, &key, all.clone()), rank(&engine, &key, from_bytes));
    assert_eq!(members(&rank(&engine, &key, all)), vec![3, 2, 1, 4]);
}

#[test]
fn a_recent_read_answers_the_stored_cells_newest_first_live_and_after_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(21);
    seed(&engine, &key);
    // The ten cells `seed` stored, newest sample first, and the `shown` each stored: a ranked
    // read reduces them to four members, so ten rows and a repeated member are this read's alone.
    let want_members = vec![2, 4, 1, 3, 1, 2, 4, 1, 2, 3];
    let want_stamps = vec![400, 400, 300, 300, 200, 200, 200, 100, 100, 100];
    let want_shown = vec![10, 2, 10, 100, 10, 30, 8, 10, 20, 100];
    let check = |engine: &TemporalEngine| {
        let ranked = rank(engine, &key, spec(ALL, WindowReduceScore::Recent, 100));
        assert!(ranked.refused.is_empty(), "{}", ranked.refused);
        assert_eq!(members(&ranked), want_members);
        assert_eq!(ranked.rows.iter().map(|r| r.last_ms).collect::<Vec<_>>(), want_stamps);
        assert_eq!(ranked.rows.iter().map(|r| r.lanes[0]).collect::<Vec<_>>(), want_shown);
        assert_eq!(ranked.samples, 4);
        // A limit cuts the newest rows; a lane test keeps the cells that stored that value.
        assert_eq!(members(&rank(engine, &key, spec(ALL, WindowReduceScore::Recent, 3))), vec![2, 4, 1]);
        let mut matched = spec(ALL, WindowReduceScore::Recent, 100);
        matched.filters = vec![FeatureFilter { field: "shown".into(), op: FeatureFilterOp::Equal, value: 100 }];
        let matched = rank(engine, &key, matched);
        assert_eq!((members(&matched), matched.rows.iter().map(|r| r.last_ms).collect::<Vec<_>>()), (vec![3, 3], vec![300, 100]));
        ranked
    };
    let live = check(&engine);
    drop(engine);
    // A reload reads every block cold and answers the same rows.
    assert_eq!(check(&engine_on(dir.path())), live);
}

#[test]
fn a_recent_read_is_bounded_like_the_ranked_read_and_refuses_a_quota() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    let key = stored_key(22);
    seed(&engine, &key);
    let refused = (false, "invalid_argument".to_string());
    let outcome = |query: WindowReduceRankSpec| status_of(&engine, &key, query);
    assert!(outcome(spec(ALL, WindowReduceScore::Recent, 1000)).0);
    assert_eq!(outcome(spec(ALL, WindowReduceScore::Recent, 1001)), refused);
    assert_eq!(outcome(spec(ALL, WindowReduceScore::Recent, 0)), refused);
    assert_eq!(outcome(WindowReduceRankSpec { score: WindowReduceScore::Recent, ..quota(&[], 1, 8) }), refused);
}
