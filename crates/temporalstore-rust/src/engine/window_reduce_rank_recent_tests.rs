// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests of the stored-cell read (`WindowReduceScore::Recent`): one row per stored cell, none reduced, the
//! newest sample first. The data is `data()` of the parent module; its eleven cells, newest
//! first, are
//!
//! ```text
//! 400: m2 m4        300: m1 m3 zz        200: m1 m2 m4        100: m1 m2 m3
//! ```
//!
//! (`zz` is the bytes member, which orders after every `u64` member). A reduced read of the same
//! data holds five members, so a row count or a repeated member below is a value only this path
//! can produce.

use super::*;

fn recent(window: WindowReduceWindow, top_k: usize) -> WindowReduceRankSpec {
    spec(window, WindowReduceScore::Recent, top_k)
}

fn stamps(ranked: &RankedCells) -> Vec<u64> {
    ranked.rows.iter().map(|row| row.last_ms).collect()
}

const NEWEST_FIRST_MEMBERS: [u64; 11] = [2, 4, 1, 3, 99, 1, 2, 4, 1, 2, 3];
const NEWEST_FIRST_STAMPS: [u64; 11] = [400, 400, 300, 300, 300, 200, 200, 200, 100, 100, 100];

#[test]
fn a_recent_read_answers_every_stored_cell_once_newest_sample_first_and_reduces_none() {
    let ranked = run(&recent(ALL, 100));
    assert_eq!(order(&ranked), NEWEST_FIRST_MEMBERS);
    assert_eq!(stamps(&ranked), NEWEST_FIRST_STAMPS);
    assert_eq!(ranked.samples, 4);
    assert_eq!(ranked.keys, 1);
    assert_eq!(ranked.unscored, 0);
    // The stored values, not the window's reduction: m1 appears at 100, 200 and 300 with the
    // `shown` it stored each time (10 + 10 + 10 reduces to 30 in a ranked read), and its label is
    // the one that sample carried.
    let m1: Vec<(u64, u64, Vec<i64>)> = ranked
        .rows
        .iter()
        .filter(|row| row.member == CellMemberId::U64(1))
        .map(|row| (row.last_ms, row.labels[0], row.lanes.clone()))
        .collect();
    assert_eq!(
        m1,
        vec![
            (300, 2, vec![10, 1, 4, 300]),
            (200, 1, vec![10, 2, 9, 200]),
            (100, 1, vec![10, 1, 5, 100]),
        ]
    );
    // Each row's score is its sample's stamp.
    assert_eq!(scores(&ranked), NEWEST_FIRST_STAMPS.map(|s| s as i64));
    assert_eq!(ranked.label_columns, vec!["slot".to_string()]);
    assert_eq!(ranked.lane_columns, vec!["shown", "clicked", "peak", "seen"]);
    // The same data ranked by a lane holds five rows: the two reads are not the same read.
    assert_eq!(run(&spec(ALL, lane("shown"), 100)).rows.len(), 5);
}

#[test]
fn rows_are_bounded_by_the_limit_and_the_walk_stops_in_the_middle_of_a_sample() {
    // One row short of every cell, exactly every cell, and one past it.
    assert_eq!(order(&run(&recent(ALL, 10))), NEWEST_FIRST_MEMBERS[..10].to_vec());
    assert_eq!(order(&run(&recent(ALL, 11))), NEWEST_FIRST_MEMBERS.to_vec());
    assert_eq!(order(&run(&recent(ALL, 12))), NEWEST_FIRST_MEMBERS.to_vec());
    // A limit of 1 is the newest cell: m2 of sample 400.
    let one = run(&recent(ALL, 1));
    assert_eq!((order(&one), stamps(&one)), (vec![2], vec![400]));
    // The limit is the ranked read's own: accepted at the top-k cap, refused one past it and at 0.
    assert!(validate_spec(&recent(ALL, MAX_TOP_K)).is_ok());
    assert!(validate_spec(&recent(ALL, MAX_TOP_K + 1)).unwrap_err().contains("top-k"));
    assert!(validate_spec(&recent(ALL, 0)).is_err());
}

#[test]
fn the_window_the_predicates_the_member_set_and_the_projection_apply_to_each_cell() {
    // A window of 150 to 350 holds samples 200 and 300.
    let windowed = run(&recent(WindowReduceWindow::Between { start_ms: 150, end_ms: 350 }, 100));
    assert_eq!(order(&windowed), vec![1, 3, 99, 1, 2, 4]);
    assert_eq!(windowed.samples, 2);

    // slot = 1 keeps m1@100, m3@100, m1@200 and m3@300, newest first.
    let mut slot1 = recent(ALL, 100);
    slot1.filters = vec![filter("slot", FeatureFilterOp::Equal, 1)];
    let ranked = run(&slot1);
    assert_eq!((order(&ranked), stamps(&ranked)), (vec![3, 1, 1, 3], vec![300, 200, 100, 100]));

    // A lane test is the match-lane read: the stored cells whose `shown` is 100 are m3 twice, and
    // a reduced read would see one m3 with 200.
    let mut matched = recent(ALL, 100);
    matched.filters = vec![filter("shown", FeatureFilterOp::Equal, 100)];
    let ranked = run(&matched);
    assert_eq!((order(&ranked), stamps(&ranked)), (vec![3, 3], vec![300, 100]));
    assert!(ranked.rows.iter().all(|row| row.lanes[0] == 100));

    // The include set keeps m2's three cells; the exclude set drops them.
    let mut only_m2 = recent(ALL, 100);
    only_m2.members.include = Some(vec![CellMemberId::U64(2)]);
    assert_eq!(stamps(&run(&only_m2)), vec![400, 200, 100]);
    let mut not_m2 = recent(ALL, 100);
    not_m2.members.exclude = vec![CellMemberId::U64(2)];
    assert_eq!(order(&run(&not_m2)), vec![4, 1, 3, 99, 1, 4, 1, 3]);

    // A projection names the columns that come back, in the order named.
    let mut projected = recent(ALL, 2);
    projected.project = vec!["peak".to_string(), "slot".to_string()];
    let ranked = run(&projected);
    assert_eq!(ranked.lane_columns, vec!["peak".to_string()]);
    assert_eq!(ranked.label_columns, vec!["slot".to_string()]);
    assert_eq!(ranked.rows.iter().map(|row| row.lanes.clone()).collect::<Vec<_>>(), vec![vec![8], vec![2]]);
}

#[test]
fn samples_sharing_a_stamp_come_back_with_the_later_one_in_the_slice_first() {
    // Samples of two keys at one stamp, as a merged read hands them over, earlier key first.
    let first = vec![cell(1, 1, [1, 0, 0, 0]), cell(5, 1, [2, 0, 0, 0])];
    let second = vec![cell(2, 1, [3, 0, 0, 0])];
    let slice = [
        SampleView { timestamp_ms: 50, cells: &first },
        SampleView { timestamp_ms: 100, cells: &first },
        SampleView { timestamp_ms: 100, cells: &second },
    ];
    let plan = Plan::compile(&header(), &recent(ALL, 100)).unwrap();
    let ranked = plan.run_ordered(&slice).unwrap();
    assert_eq!(order(&ranked), vec![2, 1, 5, 1, 5]);
    assert_eq!(stamps(&ranked), vec![100, 100, 100, 50, 50]);
}

fn quota_of(lane: &str, count: usize, others: usize) -> crate::types::WindowReduceQuota {
    crate::types::WindowReduceQuota {
        lanes: vec![crate::types::WindowReduceLaneQuota { lane: lane.to_string(), count }],
        others,
    }
}

#[test]
fn a_quota_on_stored_cells_walks_newest_first_and_stops_when_every_lane_is_full_or_the_limit_is_reached() {
    // `clicked` is non-zero in m2@400 (5), m1@300 (1), m3@300 (40), m1@200, m2@200, m2@100, m1@100, m3@100
    // and zero in m4@400 and zz@300. The walk is newest first, cells of a sample by member.
    let with = |lane: &str, count: usize, others: usize, top_k: usize| {
        let mut query = recent(ALL, top_k);
        query.quota = quota_of(lane, count, others);
        run(&query)
    };
    let rows = |ranked: &RankedCells| (order(ranked), stamps(ranked));
    // 2 members with clicks and 1 other: m2@400 fits and is new (1 of 2); m4@400 fits no short lane and
    // is the one other; m1@300 fits and is new (2 of 2), the lane is full and the walk ends.
    assert_eq!(rows(&with("clicked", 2, 1, 100)), (vec![2, 4, 1], vec![400, 400, 300]));
    // The limit is the cells WALKED: two cells are m2@400 and m4@400, and the lane is still short.
    assert_eq!(rows(&with("clicked", 2, 1, 2)), (vec![2, 4], vec![400, 400]));
    // A cell walked and not returned still counts: with no `others`, m4@400 is walked and dropped.
    assert_eq!(rows(&with("clicked", 2, 0, 2)), (vec![2], vec![400]));
    // 3 members: m2@400, m4@400 (the other), m1@300 and m3@300 (40, new, lane full) end the walk.
    assert_eq!(rows(&with("clicked", 3, 1, 100)), (vec![2, 4, 1, 3], vec![400, 400, 300, 300]));
    // A lane that can never fill walks every cell: `peak` is non-zero everywhere and holds 5 members, so a
    // count of 6 returns each member once (a repeated member fits the lane and is not new) and no other.
    let all = with("peak", 6, 3, 100);
    assert_eq!(rows(&all), (vec![2, 4, 1, 3, 99], vec![400, 400, 300, 300, 300]));
}

#[test]
fn a_quota_on_stored_cells_is_the_plain_read_when_the_walk_ends_with_it_and_it_names_a_lane() {
    // The same cells as the plain stored-cell read, never reduced: each row carries its own sample's lanes.
    let mut query = recent(ALL, 100);
    query.quota = quota_of("shown", 5, 0);
    let ranked = run(&query);
    // `shown` is non-zero in every cell; five distinct members fill the lane at zz@300.
    assert_eq!(order(&ranked), vec![2, 4, 1, 3, 99]);
    let m4 = ranked.rows.iter().find(|row| row.member == CellMemberId::U64(4)).unwrap();
    assert_eq!((m4.last_ms, m4.lanes.clone()), (400, vec![2, 0, 2, 400]));
    assert_eq!(ranked.samples, 4);
}

#[test]
fn a_recent_read_refuses_a_quota_with_no_lane_and_is_no_score_for_a_cap() {
    let mut quota = recent(ALL, 10);
    quota.quota = crate::types::WindowReduceQuota { lanes: Vec::new(), others: 1 };
    assert!(validate_spec(&quota).unwrap_err().contains("needs at least one lane"));
    assert!(Plan::compile(&header(), &quota).is_err());
    // A quota with a lane is accepted on the stored-cell read.
    let mut with_lane = recent(ALL, 10);
    with_lane.quota = quota_of("clicked", 2, 1);
    assert!(validate_spec(&with_lane).is_ok());
    // The same `others`-only quota on a reduced read is accepted: the refusal is about the stored-cell read.
    let mut reduced = spec(ALL, WindowReduceScore::Member, 10);
    reduced.quota = quota.quota.clone();
    assert!(validate_spec(&reduced).is_ok());
    // A rollup cap scores one folded cell by a header name; this is no such score.
    assert!(Scorer::compile(&header(), &WindowReduceScore::Recent).err().expect("refused").contains("not a score a cap can use"));
}

#[test]
fn a_cell_that_breaks_the_header_shape_or_the_member_order_is_refused_not_returned() {
    // Three lanes where the header declares four.
    let short = vec![(100u64, vec![Cell { member: Member::U64(1), labels: vec![1], lanes: vec![1, 2, 3] }])];
    let err = run_on(&recent(ALL, 10), &short, 100).unwrap_err();
    assert!(err.contains("3 lanes"), "{err}");
    // Members that do not ascend.
    let unordered = vec![(100u64, vec![cell(2, 1, [0; 4]), cell(1, 1, [0; 4])])];
    let err = run_on(&recent(ALL, 10), &unordered, 100).unwrap_err();
    assert!(err.contains("not strictly ascending"), "{err}");
}
