// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests of the ranked cell read (`cell_rank`). Every expected number below is worked out by hand
//! in a comment beside it, so the assertions are the arithmetic and not a copy of the code.

use super::*;
use crate::engine::cell_set::{
    decode_value, encode_value, DimSpec, DimType, EntityType, KeySpec, LaneReducer, LaneSpec,
    HEADER_VERSION, MAX_TOP_K,
};

type Snapshots = Vec<(u64, Vec<Cell>)>;

/// One label (`slot`) and four lanes. The reducers differ on purpose: a read that reduced every
/// lane the same way could not produce the numbers asserted below.
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
            ("clicked", LaneReducer::Sum),
            ("peak", LaneReducer::Max),
            ("seen", LaneReducer::Last),
        ]
        .into_iter()
        .map(|(name, reducer)| LaneSpec { name: name.to_string(), reducer })
        .collect(),
    }
}

fn cell(member: u64, slot: u64, lanes: [i64; 4]) -> Cell {
    Cell { member: Member::U64(member), labels: vec![slot], lanes: lanes.to_vec() }
}

/// The bytes member `zz`; `order` writes it as 99.
fn bytes_cell(slot: u64, lanes: [i64; 4]) -> Cell {
    Cell { member: Member::Bytes(b"zz".to_vec()), labels: vec![slot], lanes: lanes.to_vec() }
}

/// lanes are [shown, clicked, peak, seen]. Reduced over all four snapshots (Sum, Sum, Max, Last):
///   m1  shown  30 clicked  4 peak 9 seen 300 slot 2     m2  shown  60 clicked 30 peak 8 seen 400 slot 2
///   m3  shown 200 clicked 90 peak 6 seen 300 slot 1     m4  shown  10 clicked  8 peak 2 seen 400 slot 2
///   zz  shown   5 clicked  0 peak 1 seen 300 slot 4
fn data() -> Snapshots {
    vec![
        (100, vec![cell(1, 1, [10, 1, 5, 100]), cell(2, 2, [20, 10, 7, 100]), cell(3, 1, [100, 50, 2, 100])]),
        (200, vec![cell(1, 1, [10, 2, 9, 200]), cell(2, 3, [30, 15, 3, 200]), cell(4, 2, [8, 8, 1, 200])]),
        (300, vec![cell(1, 2, [10, 1, 4, 300]), cell(3, 1, [100, 40, 6, 300]), bytes_cell(4, [5, 0, 1, 300])]),
        (400, vec![cell(2, 2, [10, 5, 8, 400]), cell(4, 2, [2, 0, 2, 400])]),
    ]
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

fn filter(field: &str, op: FeatureFilterOp, value: u64) -> FeatureFilter {
    FeatureFilter { field: field.to_string(), op, value }
}

fn run_on(spec: &CellRankSpec, snaps: &Snapshots, now_ms: u64) -> Result<RankedCells, String> {
    Plan::compile(&header(), spec)?.run(now_ms, &views(snaps))
}

fn run(spec: &CellRankSpec) -> RankedCells {
    run_on(spec, &data(), 400).expect("the query is valid")
}

fn order(ranked: &RankedCells) -> Vec<u64> {
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

#[test]
fn each_score_orders_by_the_value_only_it_produces() {
    let wilson = CellScore::Wilson { successes: "clicked".to_string(), trials: "shown".to_string(), z: 1.96 };
    let weighted = CellScore::Weighted {
        lanes: vec!["shown".to_string(), "peak".to_string()],
        weights: vec![1.0, 50.0],
    };
    let ratio_score = CellScore::Ratio { numerator: "clicked".to_string(), denominator: "shown".to_string() };
    // (name, score, expected member order, expected first score)
    let table: Vec<(&str, CellScore, Vec<u64>, f64)> = vec![
        // Sum: 200 60 30 10 5.
        ("lane sum", lane("shown"), vec![3, 2, 1, 4, 99], 200.0),
        // Max: a sum would give m1 and m2 18 each and m3 8.
        ("lane max", lane("peak"), vec![1, 2, 3, 4, 99], 9.0),
        // Last: m2 and m4 tie at 400, then m1 m3 zz tie at 300; a sum would be 1000 for m1.
        ("lane last", lane("seen"), vec![2, 4, 1, 3, 99], 400.0),
        // clicked / shown: .8 .5 .45 .1333 0.
        ("ratio", ratio_score, vec![4, 2, 3, 1, 99], 0.8),
        // The same data under Wilson: m3 (90/200) overtakes m2 (30/60) on volume.
        ("wilson", wilson, vec![4, 3, 2, 1, 99], 0.490_157_5),
        // shown + 50 * peak: 500 480 460 110 55.
        ("weighted", weighted, vec![3, 1, 2, 4, 99], 500.0),
    ];
    for (name, score, expected, first_score) in table {
        let ranked = run(&spec(ALL, score, 5));
        assert_eq!(order(&ranked), expected, "{name}");
        assert!(
            (ranked.rows[0].score - first_score).abs() < 1e-4,
            "{name}: first score {} is not {first_score}",
            ranked.rows[0].score
        );
        assert_eq!(ranked.snapshots, 4, "{name}");
        assert_eq!(ranked.unscored, 0, "{name}");
    }

    // The member score and the time score read different things again.
    let mut by_member = spec(ALL, CellScore::Member, 5);
    by_member.filters = vec![filter("member", FeatureFilterOp::LessOrEqual, 4)];
    assert_eq!(order(&run(&by_member)), vec![4, 3, 2, 1]);
    // Newest stamp inside 100..=300: m1 m3 zz at 300, then m2 m4 at 200.
    let by_time = run(&spec(CellWindow::Between { start_ms: 100, end_ms: 300 }, CellScore::Time, 5));
    assert_eq!(order(&by_time), vec![1, 3, 99, 2, 4]);
    assert_eq!(by_time.rows.iter().map(|r| r.last_ms).collect::<Vec<_>>(), vec![300, 300, 300, 200, 200]);
}

#[test]
fn a_wilson_bound_is_the_hand_computed_number_and_rewards_volume() {
    // 8 of 10 at z = 1.96, by hand: p = .8, z^2 = 3.8416,
    //   centre = .8 + 3.8416 / 20                 = .99208
    //   margin = 1.96 * sqrt(.016 + 3.8416 / 400) = 1.96 * .1600125 = .313624
    //   lower  = (.99208 - .313624) / (1 + 3.8416 / 10)           = .678456 / 1.38416 = .49016
    let value = wilson_lower_bound(8, 10, 1.96).unwrap();
    assert!((value - 0.490_16).abs() < 1e-4, "{value}");

    // Two members with the SAME ratio and different volume: the ratio ties them (member order
    // decides), Wilson puts the one with more evidence first. 80 of 100 by hand:
    //   centre = .8 + 3.8416 / 200 = .819208; margin = 1.96 * sqrt(.0016 + 3.8416 / 40000) = 1.96 * .041183 = .080719
    //   lower  = (.819208 - .080719) / 1.038416 = .71118
    let snaps: Snapshots = vec![(100, vec![cell(1, 0, [10, 8, 0, 0]), cell(2, 0, [100, 80, 0, 0])])];
    let wilson = CellScore::Wilson { successes: "clicked".to_string(), trials: "shown".to_string(), z: 1.96 };
    let ratio_score = CellScore::Ratio { numerator: "clicked".to_string(), denominator: "shown".to_string() };
    let by_ratio = run_on(&spec(ALL, ratio_score, 5), &snaps, 100).unwrap();
    let by_wilson = run_on(&spec(ALL, wilson, 5), &snaps, 100).unwrap();
    assert_eq!(order(&by_ratio), vec![1, 2], "equal ratios tie and the smaller member leads");
    assert_eq!(order(&by_wilson), vec![2, 1]);
    assert!((by_wilson.rows[1].score - 0.490_16).abs() < 1e-4, "{}", by_wilson.rows[1].score);
    assert!((by_wilson.rows[0].score - 0.711_2).abs() < 1e-3, "{}", by_wilson.rows[0].score);
}

#[test]
fn an_undefined_score_is_counted_and_not_ranked() {
    // m1 has shown = 0: the ratio's denominator. m2 has clicked > shown: no Wilson bound.
    let snaps: Snapshots =
        vec![(100, vec![cell(1, 0, [0, 5, 0, 0]), cell(2, 0, [4, 9, 0, 0]), cell(3, 0, [10, 5, 0, 0])])];
    let ratio_score = CellScore::Ratio { numerator: "clicked".to_string(), denominator: "shown".to_string() };
    let ranked = run_on(&spec(ALL, ratio_score, 5), &snaps, 100).unwrap();
    assert_eq!((order(&ranked), ranked.unscored), (vec![2, 3], 1));
    let wilson = CellScore::Wilson { successes: "clicked".to_string(), trials: "shown".to_string(), z: 1.96 };
    let ranked = run_on(&spec(ALL, wilson, 5), &snaps, 100).unwrap();
    assert_eq!((order(&ranked), ranked.unscored), (vec![3], 2));
}

#[test]
fn each_window_kind_selects_the_snapshots_it_names() {
    let shown = |window| {
        let ranked = run(&spec(window, lane("shown"), 5));
        (order(&ranked), scores(&ranked), ranked.snapshots)
    };
    // Both ends are inclusive: snapshots 200 and 300.
    assert_eq!(
        shown(CellWindow::Between { start_ms: 200, end_ms: 300 }),
        (vec![3, 2, 1, 4, 99], vec![100, 30, 20, 8, 5], 2)
    );
    assert_eq!(shown(CellWindow::Between { start_ms: 100, end_ms: 100 }), (vec![3, 2, 1], vec![100, 20, 10], 1));
    assert_eq!(shown(CellWindow::Between { start_ms: 101, end_ms: 199 }), (vec![], vec![], 0));
    // now = 400: the last 150 ms is [250, 400], so the snapshots at 300 and 400.
    assert_eq!(shown(CellWindow::Last { duration_ms: 150 }), (vec![3, 1, 2, 99, 4], vec![100, 10, 10, 5, 2], 2));
    // A duration of 0 is the instant itself.
    assert_eq!(shown(CellWindow::Last { duration_ms: 0 }), (vec![2, 4], vec![10, 2], 1));
    assert_eq!(shown(CellWindow::Since { start_ms: 300 }), (vec![3, 1, 2, 99, 4], vec![100, 10, 10, 5, 2], 2));
    assert_eq!(shown(CellWindow::Since { start_ms: 401 }), (vec![], vec![], 0));
    assert_eq!(shown(CellWindow::LastN { n: 1 }), (vec![2, 4], vec![10, 2], 1));
    assert_eq!(shown(CellWindow::LastN { n: 3 }), (vec![3, 2, 1, 4, 99], vec![100, 40, 20, 10, 5], 3));
    // More snapshots asked for than exist is the whole key.
    assert_eq!(shown(CellWindow::LastN { n: 50 }).2, 4);
    // A `Last` window ends at the clock it is given: at 200 it covers [50, 200].
    let early = run_on(&spec(CellWindow::Last { duration_ms: 150 }, lane("shown"), 5), &data(), 200).unwrap();
    assert_eq!((early.snapshots, order(&early), scores(&early)), (2, vec![3, 2, 1, 4], vec![100, 50, 20, 8]));
}

#[test]
fn a_window_that_cannot_hold_a_snapshot_is_refused() {
    let backwards = spec(CellWindow::Between { start_ms: 5, end_ms: 4 }, lane("shown"), 1);
    assert!(Plan::compile(&header(), &backwards).err().unwrap().contains("after it ends"));
    let none = spec(CellWindow::LastN { n: 0 }, lane("shown"), 1);
    assert!(Plan::compile(&header(), &none).err().unwrap().contains("empty"));
}

#[test]
fn ties_break_by_member_and_a_smaller_k_is_a_prefix_of_a_larger_one() {
    // "seen" has m2 and m4 at 400 and m1, m3, zz at 300.
    let full = run(&spec(ALL, lane("seen"), 5));
    assert_eq!(order(&full), vec![2, 4, 1, 3, 99]);
    for k in 1..=5 {
        let cut = run(&spec(ALL, lane("seen"), k));
        assert_eq!(order(&cut), order(&full)[..k].to_vec(), "k = {k}");
        assert_eq!(cut.rows, full.rows[..k].to_vec(), "k = {k}");
    }
    // And a k beyond the members is every member once.
    assert_eq!(run(&spec(ALL, lane("seen"), 1000)).rows.len(), 5);
}

#[test]
fn a_row_carries_its_reduced_lanes_its_newest_labels_and_its_newest_stamp() {
    let ranked = run(&spec(ALL, lane("shown"), 1));
    assert_eq!(ranked.label_columns, vec!["slot".to_string()]);
    assert_eq!(ranked.lane_columns, ["shown", "clicked", "peak", "seen"].map(String::from).to_vec());
    let row = &ranked.rows[0];
    assert_eq!(row.member, CellMemberId::U64(3));
    assert_eq!(row.lanes, vec![200, 90, 6, 300]);
    assert_eq!(row.labels, vec![1]);
    assert_eq!(row.last_ms, 300);
    // m1's slot is the newest snapshot's (2 at stamp 300), not the first (1).
    let m1 = run(&spec(ALL, lane("shown"), 5))
        .rows
        .into_iter()
        .find(|r| r.member == CellMemberId::U64(1))
        .unwrap();
    assert_eq!(m1.labels, vec![2]);
}

#[test]
fn a_projection_returns_the_named_columns_only() {
    let mut query = spec(ALL, lane("shown"), 2);
    query.project = vec!["peak".to_string(), "slot".to_string()];
    let ranked = run(&query);
    assert_eq!(ranked.lane_columns, vec!["peak".to_string()]);
    assert_eq!(ranked.label_columns, vec!["slot".to_string()]);
    assert_eq!((ranked.rows[0].lanes.clone(), ranked.rows[0].labels.clone()), (vec![6], vec![1]));
    query.project = vec!["peak".to_string(), "peak".to_string()];
    assert!(run_on(&query, &data(), 400).unwrap_err().contains("twice"));
    query.project = vec!["member".to_string()];
    assert!(run_on(&query, &data(), 400).is_err());
}

#[test]
fn every_filter_operator_selects_what_it_names() {
    // Total shown per member among the cells whose `slot` passes, window = everything. The cells
    // by (stamp, member, slot, shown): (100,1,1,10) (100,2,2,20) (100,3,1,100) (200,1,1,10)
    // (200,2,3,30) (200,4,2,8) (300,1,2,10) (300,3,1,100) (300,zz,4,5) (400,2,2,10) (400,4,2,2).
    //   = 2   -> m1 10, m2 20+10, m4 8+2          -> 2 1 4 (m1 and m4 tie at 10)
    //   != 2  -> m1 20, m2 30, m3 200, zz 5       -> 3 2 1 zz
    //   > 2   -> m2 30, zz 5                      -> 2 zz
    //   >= 2  -> m1 10, m2 60, m4 10, zz 5        -> 2 1 4 zz
    //   < 2   -> m1 20, m3 200                    -> 3 1
    //   <= 2  -> m1 30, m2 30, m3 200, m4 10      -> 3 1 2 4
    let cases: [(FeatureFilterOp, Vec<u64>); 6] = [
        (FeatureFilterOp::Equal, vec![2, 1, 4]),
        (FeatureFilterOp::NotEqual, vec![3, 2, 1, 99]),
        (FeatureFilterOp::GreaterThan, vec![2, 99]),
        (FeatureFilterOp::GreaterOrEqual, vec![2, 1, 4, 99]),
        (FeatureFilterOp::LessThan, vec![3, 1]),
        (FeatureFilterOp::LessOrEqual, vec![3, 1, 2, 4]),
    ];
    for (op, expected) in cases {
        let mut query = spec(ALL, lane("shown"), 10);
        query.filters = vec![filter("slot", op.clone(), 2)];
        assert_eq!(order(&run(&query)), expected, "slot {op:?} 2");
    }
}

#[test]
fn a_predicate_cuts_before_the_reduction_not_after_it() {
    // peak > 4 keeps (100,m1) (100,m2) (200,m1) (300,m3) (400,m2): the shown sums are 20, 30, 100.
    // Reducing first and cutting the totals would have kept m1 at 30, m2 at 60 and m3 at 200.
    let mut query = spec(ALL, lane("shown"), 10);
    query.filters = vec![filter("peak", FeatureFilterOp::GreaterThan, 4)];
    let ranked = run(&query);
    assert_eq!((order(&ranked), scores(&ranked)), (vec![3, 2, 1], vec![100, 30, 20]));

    // Two predicates are ANDed: slot = 2 and shown >= 10 keeps (100,m2) (300,m1) (400,m2).
    let mut query = spec(ALL, lane("shown"), 10);
    query.filters = vec![
        filter("slot", FeatureFilterOp::Equal, 2),
        filter("shown", FeatureFilterOp::GreaterOrEqual, 10),
    ];
    let ranked = run(&query);
    assert_eq!((order(&ranked), scores(&ranked)), (vec![2, 1], vec![30, 10]));

    // A bytes member never satisfies a predicate on `member`.
    let mut query = spec(ALL, lane("shown"), 10);
    query.filters = vec![filter("member", FeatureFilterOp::GreaterOrEqual, 0)];
    assert_eq!(order(&run(&query)), vec![3, 2, 1, 4]);
}

#[test]
fn a_name_the_key_does_not_declare_is_refused_before_anything_is_read() {
    let header = header();
    let mut bad_filter = spec(ALL, lane("shown"), 1);
    bad_filter.filters = vec![filter("gid", FeatureFilterOp::Equal, 1)];
    let mut bad_projection = spec(ALL, lane("shown"), 1);
    bad_projection.project = vec!["slott".to_string()];
    let queries = [
        ("filter", bad_filter, "gid"),
        ("lane score", spec(ALL, lane("views"), 1), "views"),
        ("label as a lane", spec(ALL, lane("slot"), 1), "slot"),
        (
            "ratio denominator",
            spec(ALL, CellScore::Ratio { numerator: "shown".to_string(), denominator: "nope".to_string() }, 1),
            "nope",
        ),
        (
            "weighted lane",
            spec(ALL, CellScore::Weighted { lanes: vec!["nope".to_string()], weights: vec![1.0] }, 1),
            "nope",
        ),
        ("projection", bad_projection, "slott"),
    ];
    for (what, query, name) in queries {
        let error = Plan::compile(&header, &query).err().unwrap_or_else(|| panic!("{what} was accepted"));
        assert!(error.contains(name), "{what}: {error}");
    }
    // A name that is both a label and a lane is ambiguous, not silently one of them.
    let mut both = header.clone();
    both.labels.push("shown".to_string());
    assert!(Plan::compile(&both, &spec(ALL, lane("shown"), 1)).err().unwrap().contains("ambiguous"));
    // Score parameters are checked too.
    for z in [f64::NAN, -1.0, f64::INFINITY] {
        let wilson = CellScore::Wilson { successes: "clicked".to_string(), trials: "shown".to_string(), z };
        assert!(Plan::compile(&header, &spec(ALL, wilson, 1)).is_err(), "z = {z}");
    }
    let nan = CellScore::Weighted { lanes: vec!["shown".to_string()], weights: vec![f64::NAN] };
    assert!(Plan::compile(&header, &spec(ALL, nan, 1)).is_err());
    let short = CellScore::Weighted { lanes: vec!["shown".to_string(), "peak".to_string()], weights: vec![1.0] };
    assert!(Plan::compile(&header, &spec(ALL, short, 1)).err().unwrap().contains("weights"));
}

#[test]
fn the_member_score_refuses_a_bytes_member_rather_than_ranking_it_somewhere() {
    let error = run_on(&spec(ALL, CellScore::Member, 5), &data(), 400).unwrap_err();
    assert!(error.contains("u64 members"), "{error}");
}

#[test]
fn nothing_to_rank_is_an_empty_answer_not_an_error() {
    let query = spec(ALL, lane("shown"), 5);
    // A key with no snapshots.
    let none: Snapshots = Vec::new();
    let ranked = run_on(&query, &none, 400).unwrap();
    assert_eq!((ranked.rows.len(), ranked.snapshots, ranked.unscored), (0, 0, 0));
    assert_eq!(ranked.lane_columns.len(), 4, "the columns are still reported");
    // Snapshots holding no cells.
    let hollow: Snapshots = vec![(1, Vec::new()), (2, Vec::new())];
    let ranked = run_on(&query, &hollow, 400).unwrap();
    assert_eq!((ranked.rows.len(), ranked.snapshots), (0, 2));
    // A selection that keeps nothing.
    let mut nothing = spec(ALL, lane("shown"), 5);
    nothing.filters = vec![filter("shown", FeatureFilterOp::GreaterThan, 1_000_000)];
    assert_eq!(run(&nothing).rows.len(), 0);
}

#[test]
fn snapshots_that_cannot_be_merged_are_refused_not_guessed() {
    let query = spec(ALL, lane("shown"), 5);
    let unsorted: Snapshots = vec![(1, vec![cell(2, 0, [1, 0, 0, 0]), cell(1, 0, [1, 0, 0, 0])])];
    assert!(run_on(&query, &unsorted, 9).unwrap_err().contains("ascending by member"));
    let repeated: Snapshots = vec![(1, vec![cell(1, 0, [1, 0, 0, 0]), cell(1, 0, [1, 0, 0, 0])])];
    assert!(run_on(&query, &repeated, 9).is_err());
    let backwards: Snapshots = vec![(2, vec![cell(1, 0, [1, 0, 0, 0])]), (1, vec![cell(1, 0, [1, 0, 0, 0])])];
    assert!(run_on(&query, &backwards, 9).unwrap_err().contains("ascending stamp"));
    let narrow: Snapshots = vec![(1, vec![Cell { member: Member::U64(1), labels: vec![0], lanes: vec![1] }])];
    assert!(run_on(&query, &narrow, 9).unwrap_err().contains("header declares"));
}

#[test]
fn a_reloaded_snapshot_ranks_as_the_snapshot_it_was_stored_from() {
    let header = header();
    let live = data();
    // Stored and read back through the value codec, as a restart reads them.
    let reloaded: Snapshots = live
        .iter()
        .map(|(ts, cells)| {
            let (stored_header, decoded) = decode_value(&encode_value(&header, cells).unwrap()).unwrap();
            assert_eq!(stored_header, header);
            (*ts, decoded)
        })
        .collect();
    assert_eq!(reloaded, live);
    let query = spec(
        ALL,
        CellScore::Weighted { lanes: vec!["shown".to_string(), "peak".to_string()], weights: vec![1.0, 50.0] },
        5,
    );
    assert_eq!(run_on(&query, &reloaded, 400).unwrap(), run_on(&query, &live, 400).unwrap());
    assert_eq!(order(&run_on(&query, &reloaded, 400).unwrap()), vec![3, 1, 2, 4, 99]);
}

// ---- bounds: refused at limit + 1, accepted at exactly the limit --------------------------------

fn many_lane_header(lanes: usize) -> CellHeader {
    let mut h = header();
    h.lanes = (0..lanes).map(|i| LaneSpec { name: format!("l{i}"), reducer: LaneReducer::Sum }).collect();
    h
}

#[test]
fn top_k_is_bounded_at_exactly_the_limit() {
    assert!(Plan::compile(&header(), &spec(ALL, lane("shown"), MAX_TOP_K)).is_ok());
    let error = Plan::compile(&header(), &spec(ALL, lane("shown"), MAX_TOP_K + 1)).err().unwrap();
    assert!(error.contains("over the limit"), "{error}");
    assert!(Plan::compile(&header(), &spec(ALL, lane("shown"), 0)).is_err());
    assert_eq!(MAX_TOP_K, 1000);
}

#[test]
fn predicates_per_query_are_bounded_at_exactly_the_limit() {
    let predicates =
        |n: usize| (0..n).map(|_| filter("shown", FeatureFilterOp::GreaterOrEqual, 0)).collect::<Vec<_>>();
    let mut query = spec(ALL, lane("shown"), 1);
    query.filters = predicates(MAX_PREDICATES);
    assert!(Plan::compile(&header(), &query).is_ok());
    query.filters = predicates(MAX_PREDICATES + 1);
    let error = Plan::compile(&header(), &query).err().unwrap();
    assert!(error.contains("predicates per query"), "{error}");
    assert_eq!(MAX_PREDICATES, 8);
}

#[test]
fn lanes_in_a_weighted_score_are_bounded_at_exactly_the_limit() {
    let header = many_lane_header(MAX_WEIGHTED_LANES + 1);
    let weighted = |n: usize| CellScore::Weighted {
        lanes: (0..n).map(|i| format!("l{i}")).collect(),
        weights: vec![1.0; n],
    };
    assert!(Plan::compile(&header, &spec(ALL, weighted(MAX_WEIGHTED_LANES), 1)).is_ok());
    let error = Plan::compile(&header, &spec(ALL, weighted(MAX_WEIGHTED_LANES + 1), 1)).err().unwrap();
    assert!(error.contains("lanes in a weighted score"), "{error}");
    assert!(Plan::compile(&header, &spec(ALL, weighted(0), 1)).is_err());
    assert_eq!(MAX_WEIGHTED_LANES, 8);

    // At the limit the sum is really over all eight lanes: eight lanes of 1, weighted 1..=8, is 36.
    let cells = vec![Cell { member: Member::U64(1), labels: vec![0], lanes: vec![1; MAX_WEIGHTED_LANES + 1] }];
    let snaps: Snapshots = vec![(1, cells)];
    let score = CellScore::Weighted {
        lanes: (0..MAX_WEIGHTED_LANES).map(|i| format!("l{i}")).collect(),
        weights: (1..=MAX_WEIGHTED_LANES).map(|w| w as f64).collect(),
    };
    let plan = Plan::compile(&header, &spec(ALL, score, 1)).unwrap();
    assert_eq!(plan.run(1, &views(&snaps)).unwrap().rows[0].score, 36.0);
}

fn one_cell_per_stamp(count: usize) -> Snapshots {
    (1..=count as u64).map(|ts| (ts, vec![cell(1, 0, [1, 0, 0, 0])])).collect()
}

#[test]
fn snapshots_in_a_window_are_bounded_at_exactly_the_limit() {
    let query = spec(ALL, lane("shown"), 1);
    // At the limit: all MAX snapshots merge and the member's sum is MAX.
    let at = run_on(&query, &one_cell_per_stamp(MAX_WINDOW_SNAPSHOTS), 0).unwrap();
    assert_eq!((at.snapshots, at.rows[0].score as usize), (MAX_WINDOW_SNAPSHOTS, MAX_WINDOW_SNAPSHOTS));
    let error = run_on(&query, &one_cell_per_stamp(MAX_WINDOW_SNAPSHOTS + 1), 0).unwrap_err();
    assert!(error.contains("snapshots in the window"), "{error}");
    // The count is the WINDOW's, not the key's: a narrower window over the same key is fine.
    let narrow = spec(CellWindow::LastN { n: 10 }, lane("shown"), 1);
    assert_eq!(run_on(&narrow, &one_cell_per_stamp(MAX_WINDOW_SNAPSHOTS + 1), 0).unwrap().snapshots, 10);
    // LastN is bounded when the plan is compiled.
    let last_n = |n| spec(CellWindow::LastN { n }, lane("shown"), 1);
    assert!(Plan::compile(&header(), &last_n(MAX_WINDOW_SNAPSHOTS)).is_ok());
    assert!(Plan::compile(&header(), &last_n(MAX_WINDOW_SNAPSHOTS + 1)).is_err());
    assert_eq!(MAX_WINDOW_SNAPSHOTS, 512);
}

// ---- the same answer as an obvious implementation ------------------------------------------------

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[test]
fn the_merge_answers_what_a_per_member_table_answers() {
    type Oracle = Box<dyn Fn(&([i64; 4], u64)) -> Option<f64>>;
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut compared = 0usize;
    for _ in 0..60 {
        let count = 1 + rng.next() % 12;
        let snapshots: Snapshots = (1..=count)
            .map(|i| {
                let mut cells = Vec::new();
                for member in 0..40u64 {
                    if rng.next() % 2 == 0 {
                        let lanes =
                            [rng.next() % 50, rng.next() % 50, rng.next() % 50, rng.next() % 50].map(|v| v as i64);
                        cells.push(cell(member, rng.next() % 4, lanes));
                    }
                }
                (i * 10, cells)
            })
            .collect();
        // The oracle: a table per member, in snapshot order, with the `slot != 3` predicate applied.
        let mut table: std::collections::BTreeMap<u64, ([i64; 4], u64)> = Default::default();
        for (ts, cells) in &snapshots {
            for c in cells.iter().filter(|c| c.labels[0] != 3) {
                let Member::U64(m) = c.member else { unreachable!() };
                let entry = table.entry(m).or_insert(([0, 0, i64::MIN, 0], 0));
                entry.0[0] += c.lanes[0];
                entry.0[1] += c.lanes[1];
                entry.0[2] = entry.0[2].max(c.lanes[2]);
                entry.0[3] = c.lanes[3];
                entry.1 = *ts;
            }
        }
        let cases: Vec<(CellScore, Oracle)> = vec![
            (lane("shown"), Box::new(|(l, _)| Some(l[0] as f64))),
            (lane("peak"), Box::new(|(l, _)| Some(l[2] as f64))),
            (lane("seen"), Box::new(|(l, _)| Some(l[3] as f64))),
            (CellScore::Time, Box::new(|(_, ts)| Some(*ts as f64))),
            (
                CellScore::Ratio { numerator: "clicked".to_string(), denominator: "shown".to_string() },
                Box::new(|(l, _)| (l[0] != 0).then(|| l[1] as f64 / l[0] as f64)),
            ),
            (
                CellScore::Weighted { lanes: vec!["shown".to_string(), "seen".to_string()], weights: vec![0.5, 2.0] },
                Box::new(|(l, _)| Some(l[0] as f64 * 0.5 + l[3] as f64 * 2.0)),
            ),
        ];
        for (score, oracle) in cases {
            for k in [3usize, 1000] {
                let mut query = spec(ALL, score.clone(), k);
                query.filters = vec![filter("slot", FeatureFilterOp::NotEqual, 3)];
                let got = run_on(&query, &snapshots, 0).unwrap();
                let mut want: Vec<(u64, f64)> =
                    table.iter().filter_map(|(m, v)| oracle(v).map(|s| (*m, s))).collect();
                want.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                want.truncate(k);
                let got_pairs: Vec<(u64, f64)> = got
                    .rows
                    .iter()
                    .map(|r| match r.member {
                        CellMemberId::U64(m) => (m, r.score),
                        _ => unreachable!(),
                    })
                    .collect();
                assert_eq!(got_pairs, want, "{score:?} k = {k}");
                compared += got_pairs.len();
            }
        }
    }
    assert!(compared > 1000, "the comparison covered {compared} rows, too few to mean anything");
}

// The member set and the newest-snapshot anchor.
#[path = "cell_rank_options_tests.rs"]
mod options;

// The quota.
#[path = "cell_rank_quota_tests.rs"]
mod quota;

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    fn probe_header() -> CellHeader {
        let mut h = header();
        h.lanes = ["a", "b"]
            .iter()
            .map(|n| LaneSpec { name: n.to_string(), reducer: LaneReducer::Sum })
            .collect();
        h
    }

    /// `snapshots` snapshots of `members` cells each, every member in every snapshot.
    fn dataset(members: u64, snapshots: u64) -> Snapshots {
        (1..=snapshots)
            .map(|ts| {
                let cells = (0..members)
                    .map(|m| Cell {
                        member: Member::U64(m),
                        labels: vec![m % 7],
                        lanes: vec![((m * 31 + ts) % 1000) as i64, 3],
                    })
                    .collect();
                (ts, cells)
            })
            .collect()
    }

    fn query(top_k: usize) -> CellRankSpec {
        CellRankSpec {
            window: CellWindow::LastN { n: 8 },
            filters: vec![filter("b", FeatureFilterOp::GreaterThan, 0)],
            score: CellScore::Ratio { numerator: "a".to_string(), denominator: "b".to_string() },
            top_k,
            project: Vec::new(),
        }
    }

    fn allocations_of_a_ranked_read(members: u64, top_k: usize) -> u64 {
        let snaps = dataset(members, 8);
        let views = views(&snaps);
        let plan = Plan::compile(&probe_header(), &query(top_k)).unwrap();
        let probe = Probe::start();
        let ranked = plan.run(100, &views).unwrap();
        let spent = probe.stop().allocs;
        assert_eq!(ranked.rows.len(), top_k.min(members as usize));
        spent
    }

    /// What an obvious implementation would do: a table entry per member, then a sort.
    fn allocations_of_a_table_and_a_sort(members: u64) -> u64 {
        let snaps = dataset(members, 8);
        let probe = Probe::start();
        let mut table: std::collections::BTreeMap<&Member, Vec<i64>> = Default::default();
        for (_, cells) in &snaps {
            for cell in cells {
                let entry = table.entry(&cell.member).or_insert_with(|| vec![0, 0]);
                entry[0] += cell.lanes[0];
                entry[1] += cell.lanes[1];
            }
        }
        let mut all: Vec<(&Member, f64)> = table.iter().map(|(m, v)| (*m, v[0] as f64 / v[1] as f64)).collect();
        all.sort_by(|a, b| b.1.total_cmp(&a.1));
        all.truncate(10);
        let spent = probe.stop().allocs;
        assert_eq!(all.len(), 10);
        spent
    }

    /// The ranked read allocates the same number of times over 100 members and over 20 000 (160 000
    /// cells), because nothing it allocates is sized by the cell count. THE CONTROL: the table and
    /// sort an obvious implementation builds allocates in proportion to the members, so the probe
    /// can see that dimension.
    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_ranked_read_allocates_the_same_over_a_hundred_members_and_over_twenty_thousand() {
        let small = allocations_of_a_ranked_read(100, 10);
        let large = allocations_of_a_ranked_read(20_000, 10);
        let naive_small = allocations_of_a_table_and_a_sort(100);
        let naive_large = allocations_of_a_table_and_a_sort(20_000);
        println!("ranked: {small} @100 members, {large} @20000; table+sort: {naive_small} @100, {naive_large} @20000");
        assert!(small > 0, "the probe saw no allocation at all, so it is not installed");
        assert_eq!(small, large, "the ranked read's allocations moved with the number of members");
        assert!(
            naive_large > naive_small + 10_000,
            "CONTROL: the table-and-sort baseline did not grow with the members ({naive_small} -> \
             {naive_large}), so this probe could not have seen a read that did"
        );
        // k is what it is sized by: a ten times larger k costs more, by a bounded amount.
        let wide = allocations_of_a_ranked_read(20_000, 100);
        assert!(wide >= large, "{wide} {large}");
        assert!(wide <= large + 2 * 90 + 10, "k = 100 spent {wide}, k = 10 spent {large}");
    }
}
