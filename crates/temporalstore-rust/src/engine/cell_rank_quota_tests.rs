// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests of the quota of the ranked cell read: the members that must be present for some lanes,
//! chosen walking the ranking best first. See the module notes of `cell_rank_options`.

use super::*;
use crate::engine::cell_rank_options::{
    validate_options, validate_quota, LaneQuota, Quota, RankOptions, MAX_QUOTA, MAX_QUOTA_LANES,
};

fn quota_of(lanes: &[(&str, usize)], others: usize) -> Quota {
    Quota {
        lanes: lanes.iter().map(|(lane, count)| LaneQuota { lane: lane.to_string(), count: *count }).collect(),
        others,
    }
}

fn with_quota(quota: Quota) -> RankOptions {
    RankOptions { quota, ..RankOptions::default() }
}

/// One snapshot, eight members ranked by `shown` 80 .. 10, in member order. `clicked` is non-zero
/// for m3, m6 and m8; `peak` for m2 and m7; `seen` for nobody.
fn ladder() -> Snapshots {
    let rows: [(u64, [i64; 4]); 8] = [
        (1, [80, 0, 0, 0]),
        (2, [70, 0, 5, 0]),
        (3, [60, 1, 0, 0]),
        (4, [50, 0, 0, 0]),
        (5, [40, 0, 0, 0]),
        (6, [30, 1, 0, 0]),
        (7, [20, 0, 3, 0]),
        (8, [10, 1, 0, 0]),
    ];
    vec![(100, rows.iter().map(|(m, lanes)| cell(*m, 0, *lanes)).collect())]
}

fn pick(lanes: &[(&str, usize)], others: usize, k: usize, snaps: &Snapshots) -> RankedCells {
    let query = spec(ALL, lane("shown"), k);
    Plan::compile_with(&header(), &query, &with_quota(quota_of(lanes, others)))
        .and_then(|plan| plan.run(0, &views(snaps)))
        .expect("the quota is valid")
}

#[test]
fn a_quota_keeps_the_best_members_that_fit_a_short_lane_and_stops_when_every_lane_is_full() {
    let snaps = ladder();
    // Best first: 1 2 3 4 5 6 7 8. Only m3, m6 and m8 have `clicked`.
    assert_eq!(order(&pick(&[("clicked", 2)], 0, 8, &snaps)), vec![3, 6], "m3 then m6 fill the lane; m8 is not asked for");
    // Two lanes: m2 fits `peak` (done), m3 fits `clicked` (done), and the walk stops there.
    assert_eq!(order(&pick(&[("clicked", 1), ("peak", 1)], 0, 8, &snaps)), vec![2, 3]);
    // Three for `clicked` takes all of them.
    assert_eq!(order(&pick(&[("clicked", 3)], 0, 8, &snaps)), vec![3, 6, 8]);
    // A member that fits two short lanes counts for both (on the worked data m3 has clicked and peak,
    // so after it only one more `clicked` is wanted and m2 is that one).
    let ranked = pick(&[("clicked", 2), ("peak", 1)], 0, 5, &data());
    assert_eq!(order(&ranked), vec![3, 2]);
}

#[test]
fn members_that_fit_no_short_lane_are_kept_up_to_the_others_count_and_the_walk_ends_with_the_lanes() {
    let snaps = ladder();
    // m1 and m2 fit nothing and are the two `others`; m3 fills the lane; m4 and m5 are past the
    // others; m6 fills the lane (count 2) and ends the walk.
    assert_eq!(order(&pick(&[("clicked", 2)], 2, 8, &snaps)), vec![1, 2, 3, 6]);
    // The lanes end the walk even when `others` is not used up: m1, m2 kept, m3 fills the lane.
    assert_eq!(order(&pick(&[("clicked", 1)], 5, 8, &snaps)), vec![1, 2, 3]);
    // No lane at all: the first `others` members, which is what a top-k of that size would give.
    assert_eq!(order(&pick(&[], 3, 8, &snaps)), vec![1, 2, 3]);
    assert_eq!(order(&pick(&[], 2, 2, &snaps)), order(&pick(&[], 2, 8, &snaps)));
    // A lane nobody fits leaves it short, and with no `others` nothing else is kept: the answer is
    // empty although the ranking is not.
    let empty = pick(&[("seen", 1)], 0, 8, &snaps);
    assert_eq!((empty.rows.len(), empty.snapshots, empty.unscored), (0, 1, 0));
}

#[test]
fn a_quota_never_looks_past_the_top_k_and_never_returns_more_than_it() {
    let snaps = ladder();
    // m6, the second `clicked` member, is the sixth best: k = 6 reaches it and k = 5 does not.
    assert_eq!(order(&pick(&[("clicked", 2)], 0, 6, &snaps)), vec![3, 6]);
    assert_eq!(order(&pick(&[("clicked", 2)], 0, 5, &snaps)), vec![3]);
    // A quota that reserves more than k is cut to k: 4 rows, not 11.
    let wide = pick(&[("clicked", 3)], 8, 4, &snaps);
    assert_eq!(order(&wide), vec![1, 2, 3, 4]);
    for k in 1..=8 {
        assert!(pick(&[("clicked", 3), ("peak", 2)], 8, k, &snaps).rows.len() <= k, "k = {k}");
    }
}

#[test]
fn a_quota_only_drops_members_and_leaves_every_surviving_row_as_the_plain_read_has_it() {
    let snaps = ladder();
    let plain = run_on(&spec(ALL, lane("shown"), 8), &snaps, 0).unwrap();
    let picked = pick(&[("clicked", 2)], 2, 8, &snaps);
    let by_member = |ranked: &RankedCells, m: u64| ranked.rows.iter().find(|r| r.member == CellMemberId::U64(m)).cloned();
    for r in &picked.rows {
        let CellMemberId::U64(m) = r.member else { unreachable!() };
        assert_eq!(Some(r.clone()), by_member(&plain, m));
    }
    assert_eq!(picked.rows.len(), 4);
    assert_eq!((picked.snapshots, picked.unscored), (plain.snapshots, plain.unscored));
    // And no quota is the plain read, byte for byte.
    let none = Plan::compile_with(&header(), &spec(ALL, lane("shown"), 8), &with_quota(Quota::default())).unwrap();
    assert_eq!(none.run(0, &views(&snaps)).unwrap(), plain);
}

/// A literal port of the selection loop the quota follows: the lanes still short are the first
/// `end` entries of two parallel lists and a satisfied lane is swapped out of them.
fn scan(ranked: &RankedCells, lanes: &[(usize, usize)], others: usize, lane_position: &[usize]) -> Vec<u64> {
    let mut index: Vec<usize> = lanes.iter().map(|l| l.0).collect();
    let mut need: Vec<usize> = lanes.iter().map(|l| l.1).collect();
    let mut have: std::collections::HashMap<usize, std::collections::HashSet<u64>> = Default::default();
    let mut end = index.len();
    let mut left = others;
    let mut kept = Vec::new();
    for row in &ranked.rows {
        let CellMemberId::U64(m) = row.member else { unreachable!() };
        let (mut fits, mut returned) = (false, false);
        let mut i = 0;
        while i < end {
            let lane = index[i];
            if row.lanes[lane_position[lane]] != 0 {
                fits = true;
                if have.entry(lane).or_default().insert(m) {
                    returned = true;
                    if have[&lane].len() >= need[i] {
                        end -= 1;
                        index.swap(i, end);
                        need.swap(i, end);
                        continue;
                    }
                }
            }
            i += 1;
        }
        if returned {
            kept.push(m);
        } else if left > 0 && !fits {
            kept.push(m);
            left -= 1;
        }
        if !lanes.is_empty() && end == 0 {
            break;
        }
        if lanes.is_empty() && left == 0 {
            break;
        }
    }
    kept
}

#[test]
fn a_quota_answers_what_a_literal_scan_of_the_ranking_answers() {
    let mut rng = Rng(0x5EED_0123_4567_89AB);
    let (mut compared, mut dropped) = (0usize, 0usize);
    let names = ["shown", "clicked", "peak", "seen"];
    for _ in 0..200 {
        let snapshots: Snapshots = vec![(
            100,
            (0..30u64)
                .map(|m| {
                    let mut lanes = [0i64; 4];
                    lanes[0] = 1 + (rng.next() % 1000) as i64;
                    for l in lanes.iter_mut().skip(1) {
                        *l = if rng.next() % 3 == 0 { (rng.next() % 5) as i64 } else { 0 };
                    }
                    cell(m, 0, lanes)
                })
                .collect(),
        )];
        let k = 1 + (rng.next() % 30) as usize;
        let mut chosen: Vec<(usize, usize)> = Vec::new();
        for lane_index in 1..4usize {
            if rng.next() % 2 == 0 {
                chosen.push((lane_index, 1 + (rng.next() % 4) as usize));
            }
        }
        let others = (rng.next() % 4) as usize;
        let lanes: Vec<(&str, usize)> = chosen.iter().map(|(i, n)| (names[*i], *n)).collect();
        if lanes.is_empty() && others == 0 {
            continue;
        }

        let plain = run_on(&spec(ALL, lane("shown"), k), &snapshots, 0).unwrap();
        // The read reports every lane in header order, so a lane's column is its header position.
        let positions = [0usize, 1, 2, 3];
        let want = scan(&plain, &chosen, others, &positions);
        let got = pick(&lanes, others, k, &snapshots);
        let got_members: Vec<u64> = order(&got);
        assert_eq!(got_members, want, "k = {k} lanes {lanes:?} others {others}");
        compared += got_members.len();
        dropped += plain.rows.len() - got_members.len();
    }
    assert!(compared > 500, "{compared} rows compared");
    assert!(dropped > 500, "only {dropped} members were dropped: the quota cut too little to test");
}

#[test]
fn a_quota_is_bounded_at_exactly_its_limits() {
    let snaps = ladder();
    let compile = |quota: Quota| validate_quota(&quota);

    // The reservation is the counts plus `others`.
    assert!(compile(quota_of(&[("shown", MAX_QUOTA)], 0)).is_ok());
    let error = compile(quota_of(&[("shown", MAX_QUOTA + 1)], 0)).unwrap_err();
    assert!(error.contains("members a quota reserves") && error.contains("over the limit"), "{error}");
    assert!(compile(quota_of(&[("shown", MAX_QUOTA - 1)], 1)).is_ok());
    assert!(compile(quota_of(&[("shown", MAX_QUOTA - 1)], 2)).is_err());
    assert!(compile(quota_of(&[], MAX_QUOTA)).is_ok());
    assert!(compile(quota_of(&[], MAX_QUOTA + 1)).is_err());
    // A count that would overflow a sum is refused, not wrapped.
    assert!(compile(quota_of(&[("shown", usize::MAX), ("peak", 2)], 0)).is_err());
    assert!(compile(quota_of(&[("shown", 1)], usize::MAX)).is_err());
    assert_eq!(MAX_QUOTA, 1000);

    // Lanes in a quota, at exactly the limit, on a header that has that many.
    let lanes_header = |n: usize| {
        let mut h = header();
        h.lanes = (0..n)
            .map(|i| crate::engine::cell_set::LaneSpec { name: format!("l{i}"), reducer: crate::engine::cell_set::LaneReducer::Sum })
            .collect();
        h
    };
    let many = |n: usize| with_quota(Quota { lanes: (0..n).map(|i| LaneQuota { lane: format!("l{i}"), count: 1 }).collect(), others: 0 });
    let query = spec(ALL, lane("l0"), 1);
    assert!(Plan::compile_with(&lanes_header(MAX_QUOTA_LANES + 1), &query, &many(MAX_QUOTA_LANES)).is_ok());
    let error = Plan::compile_with(&lanes_header(MAX_QUOTA_LANES + 1), &query, &many(MAX_QUOTA_LANES + 1)).err().unwrap();
    assert!(error.contains("lanes in a quota") && error.contains("over the limit"), "{error}");
    assert_eq!(MAX_QUOTA_LANES, 16);

    // A count of zero asks for nothing and is refused; a lane named twice is refused.
    assert!(compile(quota_of(&[("shown", 0)], 0)).unwrap_err().contains("asks for no member"));
    assert!(compile(quota_of(&[("shown", 1), ("shown", 2)], 0)).unwrap_err().contains("twice"));
    // The lanes are named like any other: an unknown name, a label and `member` are refused when
    // the plan is compiled.
    for name in ["views", "slot", "member"] {
        let error = Plan::compile_with(&header(), &spec(ALL, lane("shown"), 1), &with_quota(quota_of(&[(name, 1)], 0)))
            .err()
            .unwrap();
        assert!(error.contains(name), "{name}: {error}");
    }
    // Admission runs the same check with no header.
    assert!(validate_options(&ALL, &with_quota(quota_of(&[("shown", 0)], 0))).is_err());
    assert!(validate_options(&ALL, &with_quota(quota_of(&[("shown", 1)], 0))).is_ok());
    // And at the limit the quota really selects: all 1000 asked for, the ranking has eight members.
    assert_eq!(pick(&[("shown", MAX_QUOTA)], 0, 8, &snaps).rows.len(), 8);
}
