// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests of the ranked read over several keys (`cell_fan_in`). The expected answers come from an
//! independent fold written in this file (a table per member, built by sorting every occurrence by
//! hand), so the assertions are not the code's own arithmetic.

use super::*;
use crate::engine::cell_set::{
    Cell, CellHeader, DimSpec, DimType, EntityType, KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{CellMemberId, CellRankSpec, CellScore, CellWindow, FeatureFilter, FeatureFilterOp, RankedCells};

type Snapshots = Vec<(u64, Vec<Cell>)>;

/// One label (`slot`) and five lanes with five different verbs, so a combine that used one verb
/// for every lane could not produce the numbers asserted below.
fn header() -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: KeySpec {
            namespace: "ns".to_string(),
            entity: EntityType::U64,
            dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
        },
        labels: vec!["slot".to_string()],
        lanes: [
            ("shown", LaneReducer::Sum),
            ("peak", LaneReducer::Max),
            ("low", LaneReducer::Min),
            ("first", LaneReducer::First),
            ("seen", LaneReducer::Last),
        ]
        .into_iter()
        .map(|(name, reducer)| LaneSpec { name: name.to_string(), reducer })
        .collect(),
    }
}

fn cell(member: u64, slot: u64, lanes: [i64; 5]) -> Cell {
    Cell { member: Member::U64(member), labels: vec![slot], lanes: lanes.to_vec() }
}

fn views(snaps: &Snapshots) -> Vec<SnapshotView<'_>> {
    snaps.iter().map(|(timestamp_ms, cells)| SnapshotView { timestamp_ms: *timestamp_ms, cells }).collect()
}

const ALL: CellWindow = CellWindow::Between { start_ms: 0, end_ms: u64::MAX };

fn spec(window: CellWindow, score: CellScore, top_k: usize) -> CellRankSpec {
    CellRankSpec { window, filters: Vec::new(), score, top_k, project: Vec::new() }
}

fn lane(name: &str) -> CellScore {
    CellScore::Lane { lane: name.to_string() }
}

fn plan(query: &CellRankSpec) -> Plan {
    Plan::compile(&header(), query).expect("the query is valid")
}

/// Runs the fan-in over `keys`.
fn fan_in(query: &CellRankSpec, keys: &[Snapshots], now_ms: u64) -> Result<RankedCells, String> {
    let owned: Vec<Vec<SnapshotView<'_>>> = keys.iter().map(views).collect();
    let slices: Vec<&[SnapshotView<'_>]> = owned.iter().map(Vec::as_slice).collect();
    plan(query).run_fan_in(now_ms, &slices)
}

fn members(ranked: &RankedCells) -> Vec<u64> {
    ranked
        .rows
        .iter()
        .map(|row| match &row.member {
            CellMemberId::U64(v) => *v,
            CellMemberId::Bytes(_) => unreachable!("these tests use u64 members"),
        })
        .collect()
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

// ---- the same answer as the single-key read ------------------------------------------------------

/// `keys` random keys, each with a random subset of the stamps `10, 20 .. 10 * stamps`, so keys
/// share stamps when `shared` and never do otherwise (key `i` then owns stamps `= i mod keys`).
fn random_keys(rng: &mut Rng, keys: usize, stamps: u64, shared: bool) -> Vec<Snapshots> {
    (0..keys)
        .map(|key| {
            let kept: Vec<u64> = (1..=stamps)
                .filter(|stamp| rng.next() % 3 != 0 && (shared || (*stamp as usize) % keys == key))
                .collect();
            kept.into_iter()
                .map(|stamp| {
                    let mut cells = Vec::new();
                    for member in 0..30u64 {
                        if rng.next() % 2 == 0 {
                            let lanes = [
                                (rng.next() % 40) as i64,
                                (rng.next() % 40) as i64,
                                (rng.next() % 40) as i64,
                                (rng.next() % 40) as i64,
                                (rng.next() % 40) as i64,
                            ];
                            cells.push(cell(member, rng.next() % 4, lanes));
                        }
                    }
                    (stamp * 10, cells)
                })
                .collect()
        })
        .collect()
}

#[test]
fn one_key_answers_what_the_single_key_read_answers() {
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let mut compared = 0usize;
    for _ in 0..40 {
        let keys = random_keys(&mut rng, 1, 9, true);
        for window in [ALL, CellWindow::LastN { n: 3 }, CellWindow::Since { start_ms: 40 }] {
            for score in [lane("shown"), lane("seen"), CellScore::Time] {
                let query = spec(window.clone(), score, 7);
                let single = plan(&query).run(0, &views(&keys[0])).unwrap();
                let merged = fan_in(&query, &keys, 0).unwrap();
                assert_eq!(merged, single);
                compared += merged.rows.len();
            }
        }
    }
    assert!(compared > 500, "the comparison covered {compared} rows, too few to mean anything");
}

#[test]
fn keys_with_distinct_stamps_answer_what_one_key_holding_all_their_snapshots_answers() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut compared = 0usize;
    for _ in 0..40 {
        let keys_count = 2 + (rng.next() % 4) as usize;
        let keys = random_keys(&mut rng, keys_count, 12, false);
        // The single key: every snapshot of every key, ascending by stamp. Stamps are distinct
        // across keys here, so the single key is well formed.
        let mut one: Snapshots = keys.iter().flatten().cloned().collect();
        one.sort_by_key(|(ts, _)| *ts);
        for score in [lane("shown"), lane("peak"), lane("low"), lane("first"), lane("seen"), CellScore::Time] {
            let query = spec(ALL, score, 1000);
            let single = plan(&query).run(0, &views(&one)).unwrap();
            let mut merged = fan_in(&query, &keys, 0).unwrap();
            // The one difference is what the keys count says: the number of keys that held a
            // snapshot, where the single key is one.
            assert_eq!(merged.keys, keys.iter().filter(|key| !key.is_empty()).count());
            assert_eq!(single.keys, 1);
            merged.keys = single.keys;
            assert_eq!(merged, single);
            compared += merged.rows.len();
        }
    }
    assert!(compared > 1000, "the comparison covered {compared} rows, too few to mean anything");
}

// ---- the same answer as an independent fold ------------------------------------------------------

struct Folded {
    member: u64,
    last_ms: u64,
    labels: Vec<u64>,
    lanes: Vec<i64>,
}

/// The oracle: every windowed occurrence of every key, sorted by (stamp, key position) by hand, then
/// folded into a table per member with the five verbs spelled out. `last_n` is per key.
fn oracle(keys: &[Snapshots], last_n: Option<usize>, slot_not: Option<u64>) -> Vec<Folded> {
    let mut occurrences: Vec<(u64, usize, &Cell)> = Vec::new();
    for (position, key) in keys.iter().enumerate() {
        let from = last_n.map_or(0, |n| key.len().saturating_sub(n));
        for (ts, cells) in &key[from..] {
            for c in cells {
                if slot_not != Some(c.labels[0]) {
                    occurrences.push((*ts, position, c));
                }
            }
        }
    }
    occurrences.sort_by_key(|(ts, position, _)| (*ts, *position));
    let mut table: std::collections::BTreeMap<u64, Folded> = Default::default();
    for (ts, _, c) in occurrences {
        let Member::U64(m) = c.member else { unreachable!() };
        match table.get_mut(&m) {
            None => {
                table.insert(m, Folded { member: m, last_ms: ts, labels: c.labels.clone(), lanes: c.lanes.clone() });
            }
            Some(f) => {
                f.lanes[0] += c.lanes[0];
                f.lanes[1] = f.lanes[1].max(c.lanes[1]);
                f.lanes[2] = f.lanes[2].min(c.lanes[2]);
                // lane 3 keeps the first value
                f.lanes[4] = c.lanes[4];
                f.labels = c.labels.clone();
                f.last_ms = ts;
            }
        }
    }
    table.into_values().collect()
}

#[test]
fn a_fan_in_answers_what_an_independent_fold_answers() {
    type Pick = Box<dyn Fn(&Folded) -> Option<f64>>;
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let mut compared = 0usize;
    for round in 0..50 {
        let keys_count = 1 + (rng.next() % 6) as usize;
        // Stamps are shared across keys, so ties between keys are the common case.
        let keys = random_keys(&mut rng, keys_count, 8, true);
        let (window, last_n) = if round % 2 == 0 { (ALL, None) } else { (CellWindow::LastN { n: 3 }, Some(3)) };
        let cases: Vec<(CellScore, Pick)> = vec![
            (lane("shown"), Box::new(|f| Some(f.lanes[0] as f64))),
            (lane("peak"), Box::new(|f| Some(f.lanes[1] as f64))),
            (lane("low"), Box::new(|f| Some(f.lanes[2] as f64))),
            (lane("first"), Box::new(|f| Some(f.lanes[3] as f64))),
            (lane("seen"), Box::new(|f| Some(f.lanes[4] as f64))),
            (CellScore::Time, Box::new(|f| Some(f.last_ms as f64))),
            (
                CellScore::Ratio { numerator: "shown".to_string(), denominator: "low".to_string() },
                Box::new(|f| (f.lanes[2] != 0).then(|| f.lanes[0] as f64 / f.lanes[2] as f64)),
            ),
        ];
        for (score, pick) in cases {
            for k in [4usize, 1000] {
                let mut query = spec(window.clone(), score.clone(), k);
                query.filters =
                    vec![FeatureFilter { field: "slot".to_string(), op: FeatureFilterOp::NotEqual, value: 3 }];
                let got = fan_in(&query, &keys, 0).unwrap();

                let folded = oracle(&keys, last_n, Some(3));
                let mut want: Vec<(&Folded, f64)> =
                    folded.iter().filter_map(|f| pick(f).map(|s| (f, s))).collect();
                want.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.member.cmp(&b.0.member)));
                want.truncate(k);
                let unscored = folded.iter().filter(|f| pick(f).is_none()).count() as u64;

                assert_eq!(got.unscored, unscored, "{score:?} k = {k}");
                assert_eq!(got.rows.len(), want.len(), "{score:?} k = {k}");
                for (row, (f, s)) in got.rows.iter().zip(&want) {
                    assert_eq!(row.member, CellMemberId::U64(f.member), "{score:?} k = {k}");
                    assert_eq!(row.score, *s, "{score:?} k = {k}");
                    assert_eq!(row.last_ms, f.last_ms, "{score:?} k = {k}");
                    assert_eq!(row.labels, f.labels, "{score:?} k = {k}");
                    assert_eq!(row.lanes, f.lanes, "{score:?} k = {k}");
                }
                compared += want.len();
            }
        }
    }
    assert!(compared > 1500, "the comparison covered {compared} rows, too few to mean anything");
}

// ---- what a caller's own merge gets wrong --------------------------------------------------------

#[test]
fn the_union_ranks_a_member_that_no_single_key_ranks_first() {
    // lanes: [shown, peak, low, first, seen]. Key 0 leads with member 1 (100), then member 7 (60);
    // key 1 leads with member 2 (100), then member 7 (60). Member 7's union is 120: the best.
    let key0: Snapshots = vec![(10, vec![cell(1, 0, [100, 0, 0, 0, 0]), cell(7, 0, [60, 0, 0, 0, 0])])];
    let key1: Snapshots = vec![(10, vec![cell(2, 0, [100, 0, 0, 0, 0]), cell(7, 0, [60, 0, 0, 0, 0])])];
    let query = spec(ALL, lane("shown"), 1);

    let each: Vec<Vec<u64>> =
        [&key0, &key1].iter().map(|k| members(&plan(&query).run(0, &views(k)).unwrap())).collect();
    assert_eq!(each, vec![vec![1], vec![2]], "member 7 is second in both keys, so no per-key top 1 holds it");

    let merged = fan_in(&query, &[key0, key1], 0).unwrap();
    assert_eq!(members(&merged), vec![7]);
    assert_eq!(merged.rows[0].score, 120.0);
}

// ---- ties, order and determinism -----------------------------------------------------------------

#[test]
fn a_tie_between_keys_at_one_stamp_resolves_by_key_position_and_sum_max_min_ignore_it() {
    // Two keys, one snapshot each, both at stamp 100, one member: lanes [shown, peak, low, first, seen].
    let a: Snapshots = vec![(100, vec![cell(1, 5, [10, 3, 9, 111, 211])])];
    let b: Snapshots = vec![(100, vec![cell(1, 6, [20, 8, 4, 122, 222])])];
    let query = spec(ALL, lane("shown"), 1);

    let ab = fan_in(&query, &[a.clone(), b.clone()], 0).unwrap();
    let ba = fan_in(&query, &[b, a], 0).unwrap();
    // Sum 30, Max 8, Min 4 whichever key is listed first.
    assert_eq!(ab.rows[0].lanes[..3], [30, 8, 4]);
    assert_eq!(ba.rows[0].lanes[..3], [30, 8, 4]);
    // First keeps the lowest position's value, Last the highest, and the label follows Last.
    assert_eq!((ab.rows[0].lanes[3], ab.rows[0].lanes[4], ab.rows[0].labels[0]), (111, 222, 6));
    assert_eq!((ba.rows[0].lanes[3], ba.rows[0].lanes[4], ba.rows[0].labels[0]), (122, 211, 5));
}

#[test]
fn the_same_request_twice_agrees_and_the_key_order_changes_nothing_when_stamps_are_distinct() {
    let mut rng = Rng(0xA076_1D64_78BD_642F);
    for _ in 0..30 {
        let keys_count = 2 + (rng.next() % 4) as usize;
        let keys = random_keys(&mut rng, keys_count, 12, false);
        let query = spec(ALL, lane("shown"), 25);
        let first = fan_in(&query, &keys, 0).unwrap();
        assert_eq!(fan_in(&query, &keys, 0).unwrap(), first);
        let mut reversed = keys.clone();
        reversed.reverse();
        assert_eq!(fan_in(&query, &reversed, 0).unwrap(), first, "distinct stamps leave no tie to break");
    }
}

#[test]
fn the_window_applies_to_each_key_and_the_answer_counts_what_it_chose() {
    let key = |stamps: &[u64]| -> Snapshots { stamps.iter().map(|ts| (*ts, vec![cell(1, 0, [1, 0, 0, 0, 0])])).collect() };
    let (a, b) = (key(&[10, 20, 30, 40, 50]), key(&[15, 25]));
    // The newest two of EACH key: 40, 50 and 15, 25. Not the newest two of the union (40, 50).
    let last_two = fan_in(&spec(CellWindow::LastN { n: 2 }, lane("shown"), 1), &[a.clone(), b.clone()], 0).unwrap();
    assert_eq!((last_two.snapshots, last_two.rows[0].score as i64, last_two.rows[0].last_ms), (4, 4, 50));
    // A time window is the same for every key: [20, 40] holds 20, 30, 40 and 25.
    let between = spec(CellWindow::Between { start_ms: 20, end_ms: 40 }, lane("shown"), 1);
    let ranked = fan_in(&between, &[a.clone(), b.clone()], 0).unwrap();
    assert_eq!((ranked.snapshots, ranked.rows[0].score as i64), (4, 4));
    // `Last` ends at the clock the caller passes.
    let last = spec(CellWindow::Last { duration_ms: 10 }, lane("shown"), 1);
    assert_eq!(fan_in(&last, &[a, b], 50).unwrap().snapshots, 2, "40 and 50 in the first key, nothing in the second");
}

#[test]
fn nothing_to_rank_is_an_empty_answer_and_a_malformed_key_is_refused_naming_its_position() {
    let query = spec(ALL, lane("shown"), 3);
    let empty = fan_in(&query, &[Vec::new(), Vec::new()], 0).unwrap();
    assert!(empty.rows.is_empty() && empty.snapshots == 0 && empty.refused.is_empty());
    assert_eq!(empty.keys, 0, "no key held a snapshot");
    // Of three keys, the one with nothing in the window is not counted as read.
    let one: Snapshots = vec![(10, vec![cell(1, 0, [1, 0, 0, 0, 0])])];
    let partly = fan_in(&query, &[Vec::new(), one.clone(), one], 0).unwrap();
    assert_eq!((partly.keys, partly.snapshots, partly.rows[0].score as i64), (2, 2, 2));
    let bad: Snapshots = vec![(20, vec![cell(1, 0, [1, 0, 0, 0, 0])]), (10, vec![cell(1, 0, [1, 0, 0, 0, 0])])];
    let error = fan_in(&query, &[Vec::new(), bad], 0).unwrap_err();
    assert!(error.starts_with("key 1: "), "{error}");
}

// ---- bounds: refused at limit + 1, accepted at exactly the limit ---------------------------------

fn one_member_keys(keys: usize, stamps_per_key: usize, cells_per_snapshot: usize) -> Vec<Snapshots> {
    (0..keys)
        .map(|_| {
            (1..=stamps_per_key as u64)
                .map(|ts| (ts, (0..cells_per_snapshot as u64).map(|m| cell(m, 0, [1, 0, 0, 0, 0])).collect()))
                .collect()
        })
        .collect()
}

#[test]
fn keys_in_a_fan_in_are_bounded_at_exactly_the_limit() {
    let query = spec(ALL, lane("shown"), 1);
    // At the limit all 64 keys merge, and the member's sum is 64.
    let at = fan_in(&query, &one_member_keys(MAX_FAN_IN_KEYS, 1, 1), 0).unwrap();
    assert_eq!((at.snapshots, at.rows[0].score as usize), (MAX_FAN_IN_KEYS, MAX_FAN_IN_KEYS));
    let over = fan_in(&query, &one_member_keys(MAX_FAN_IN_KEYS + 1, 1, 1), 0).unwrap_err();
    assert!(over.contains("keys in a fan-in read"), "{over}");
    let none = fan_in(&query, &[], 0).unwrap_err();
    assert!(none.contains("keys in a fan-in read") && none.contains("at least 1"), "{none}");
    assert_eq!(MAX_FAN_IN_KEYS, 64);
}

#[test]
fn snapshots_in_a_fan_in_are_bounded_at_exactly_the_limit() {
    let query = spec(ALL, lane("shown"), 1);
    // Two keys of 512 (each at the per-key window bound) is 1024: accepted, and all of it is summed.
    let at = fan_in(&query, &one_member_keys(2, 512, 1), 0).unwrap();
    assert_eq!((at.snapshots, at.rows[0].score as usize), (MAX_FAN_IN_SNAPSHOTS, MAX_FAN_IN_SNAPSHOTS));
    // One more snapshot in a third key is 1025: refused whole.
    let mut over = one_member_keys(2, 512, 1);
    over.push(one_member_keys(1, 1, 1).remove(0));
    let error = fan_in(&query, &over, 0).unwrap_err();
    assert!(error.contains("snapshots in a fan-in read"), "{error}");
    assert_eq!(MAX_FAN_IN_SNAPSHOTS, 1024);
    // The count is what the window CHOSE: the same 1025 snapshots under a narrower window pass.
    let narrow = fan_in(&spec(CellWindow::LastN { n: 100 }, lane("shown"), 1), &over, 0).unwrap();
    assert_eq!(narrow.snapshots, 201);
}

#[test]
fn cells_in_a_fan_in_are_bounded_at_exactly_the_limit() {
    let query = spec(ALL, lane("shown"), 1);
    // 64 keys of one snapshot of 4096 cells is exactly 262 144 cells: accepted, every cell summed.
    let at = fan_in(&query, &one_member_keys(MAX_FAN_IN_KEYS, 1, 4096), 0).unwrap();
    assert_eq!(MAX_FAN_IN_CELLS, MAX_FAN_IN_KEYS * 4096);
    assert_eq!(at.rows[0].score as usize, MAX_FAN_IN_KEYS, "member 0 is in every key's snapshot");
    // One cell more, in the last key, is refused whole.
    let mut over = one_member_keys(MAX_FAN_IN_KEYS, 1, 4096);
    over[MAX_FAN_IN_KEYS - 1][0].1.push(cell(4096, 0, [1, 0, 0, 0, 0]));
    let error = fan_in(&query, &over, 0).unwrap_err();
    assert!(error.contains("cells in a fan-in read"), "{error}");
    assert_eq!(MAX_FAN_IN_CELLS, 262_144);
}

#[test]
fn the_size_checks_accept_the_limit_and_refuse_one_past_it_by_themselves() {
    assert!(validate_key_count(1).is_ok() && validate_key_count(MAX_FAN_IN_KEYS).is_ok());
    assert!(validate_key_count(0).is_err() && validate_key_count(MAX_FAN_IN_KEYS + 1).is_err());
    assert!(validate_chosen(MAX_FAN_IN_SNAPSHOTS, MAX_FAN_IN_CELLS).is_ok());
    assert!(validate_chosen(MAX_FAN_IN_SNAPSHOTS + 1, 0).unwrap_err().contains("snapshots"));
    assert!(validate_chosen(0, MAX_FAN_IN_CELLS + 1).unwrap_err().contains("cells"));
}

#[test]
fn a_key_named_twice_is_refused_naming_both_positions_and_the_limit_is_accepted() {
    let names = |n: usize| (0..n).map(|i| format!("key-{i}")).collect::<Vec<_>>();
    assert!(validate_keys(&names(1)).is_ok() && validate_keys(&names(MAX_FAN_IN_KEYS)).is_ok());
    assert!(validate_keys(&names(MAX_FAN_IN_KEYS + 1)).unwrap_err().contains("keys in a fan-in read"));
    assert!(validate_keys(&[]).unwrap_err().contains("at least 1"));
    let mut twice = names(5);
    twice[3] = "key-1".to_string();
    assert_eq!(validate_keys(&twice).unwrap_err(), "key 3 repeats key 1: a key is read once per request");
}

#[test]
fn the_keys_count_is_a_wire_field_that_an_answer_without_it_reads_as_zero() {
    let ranked = RankedCells { keys: 3, snapshots: 5, ..RankedCells::default() };
    let json = serde_json::to_string(&ranked).unwrap();
    assert!(json.contains("\"keys\":3"), "{json}");
    assert_eq!(serde_json::from_str::<RankedCells>(&json).unwrap(), ranked);
    let without = r#"{"label_columns":[],"lane_columns":[],"snapshots":5,"unscored":0,"rows":[]}"#;
    assert_eq!(serde_json::from_str::<RankedCells>(without).unwrap().keys, 0);
}

// ---- allocations -----------------------------------------------------------------------------------

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    /// `keys` keys of 8 snapshots of `members` cells each, every member in every snapshot.
    fn dataset(keys: usize, members: u64) -> Vec<Snapshots> {
        (0..keys)
            .map(|key| {
                (1..=8u64)
                    .map(|ts| {
                        let cells = (0..members)
                            .map(|m| cell(m, m % 7, [((m * 31 + ts) % 1000) as i64, 3, 3, 3, (key as u64 + ts) as i64]))
                            .collect();
                        (ts, cells)
                    })
                    .collect()
            })
            .collect()
    }

    fn query(top_k: usize) -> CellRankSpec {
        CellRankSpec {
            window: CellWindow::LastN { n: 8 },
            filters: vec![FeatureFilter { field: "peak".to_string(), op: FeatureFilterOp::GreaterThan, value: 0 }],
            score: CellScore::Ratio { numerator: "shown".to_string(), denominator: "peak".to_string() },
            top_k,
            project: Vec::new(),
        }
    }

    fn allocations_of_a_fan_in(keys: usize, members: u64, top_k: usize) -> u64 {
        let data = dataset(keys, members);
        let owned: Vec<Vec<SnapshotView<'_>>> = data.iter().map(views).collect();
        let slices: Vec<&[SnapshotView<'_>]> = owned.iter().map(Vec::as_slice).collect();
        let plan = Plan::compile(&header(), &query(top_k)).unwrap();
        let probe = Probe::start();
        let ranked = plan.run_fan_in(100, &slices).unwrap();
        let spent = probe.stop().allocs;
        assert_eq!(ranked.rows.len(), top_k.min(members as usize));
        spent
    }

    /// What an obvious implementation does: a table entry per member over every key, then a sort.
    fn allocations_of_a_table_and_a_sort(keys: usize, members: u64) -> u64 {
        let data = dataset(keys, members);
        let probe = Probe::start();
        let mut table: std::collections::BTreeMap<&Member, Vec<i64>> = Default::default();
        for key in &data {
            for (_, cells) in key {
                for cell in cells {
                    let entry = table.entry(&cell.member).or_insert_with(|| vec![0, 0]);
                    entry[0] += cell.lanes[0];
                    entry[1] = entry[1].max(cell.lanes[1]);
                }
            }
        }
        let mut all: Vec<(&Member, f64)> = table.iter().map(|(m, v)| (*m, v[0] as f64 / v[1] as f64)).collect();
        all.sort_by(|a, b| b.1.total_cmp(&a.1));
        all.truncate(10);
        let spent = probe.stop().allocs;
        assert_eq!(all.len(), 10);
        spent
    }

    /// The fan-in allocates the same number of times over 100 members and over 2 000 (three keys of
    /// eight snapshots: 48 000 cells), because nothing it allocates is sized by the cell count.
    /// THE CONTROL: the table and sort an obvious implementation builds grows with the members, so
    /// the probe can see that dimension.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_fan_in_allocates_the_same_over_a_hundred_members_and_over_two_thousand() {
        let small = allocations_of_a_fan_in(3, 100, 10);
        let large = allocations_of_a_fan_in(3, 2_000, 10);
        let naive_small = allocations_of_a_table_and_a_sort(3, 100);
        let naive_large = allocations_of_a_table_and_a_sort(3, 2_000);
        println!("fan-in: {small} @100 members, {large} @2000; table+sort: {naive_small} @100, {naive_large} @2000");
        assert!(small > 0, "the probe saw no allocation at all, so it is not installed");
        assert_eq!(small, large, "the fan-in's allocations moved with the number of members");
        assert!(
            naive_large > naive_small + 1_000,
            "CONTROL: the table-and-sort baseline did not grow with the members ({naive_small} -> \
             {naive_large}), so this probe could not have seen a read that did"
        );
    }
}
