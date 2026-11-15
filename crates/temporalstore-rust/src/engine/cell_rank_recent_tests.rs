// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests of the stored-cell read (`CellScore::Recent`): one row per stored cell, none reduced, the
//! newest snapshot first. The data is `data()` of the parent module; its eleven cells, newest
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

fn recent(window: CellWindow, top_k: usize) -> CellRankSpec {
    spec(window, CellScore::Recent, top_k)
}

fn stamps(ranked: &RankedCells) -> Vec<u64> {
    ranked.rows.iter().map(|row| row.last_ms).collect()
}

const NEWEST_FIRST_MEMBERS: [u64; 11] = [2, 4, 1, 3, 99, 1, 2, 4, 1, 2, 3];
const NEWEST_FIRST_STAMPS: [u64; 11] = [400, 400, 300, 300, 300, 200, 200, 200, 100, 100, 100];

#[test]
fn a_recent_read_answers_every_stored_cell_once_newest_snapshot_first_and_reduces_none() {
    let ranked = run(&recent(ALL, 100));
    assert_eq!(order(&ranked), NEWEST_FIRST_MEMBERS);
    assert_eq!(stamps(&ranked), NEWEST_FIRST_STAMPS);
    assert_eq!(ranked.snapshots, 4);
    assert_eq!(ranked.keys, 1);
    assert_eq!(ranked.unscored, 0);
    // The stored values, not the window's reduction: m1 appears at 100, 200 and 300 with the
    // `shown` it stored each time (10 + 10 + 10 reduces to 30 in a ranked read), and its label is
    // the one that snapshot carried.
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
    // Each row's score is its snapshot's stamp.
    assert_eq!(scores(&ranked), NEWEST_FIRST_STAMPS.map(|s| s as i64));
    assert_eq!(ranked.label_columns, vec!["slot".to_string()]);
    assert_eq!(ranked.lane_columns, vec!["shown", "clicked", "peak", "seen"]);
    // The same data ranked by a lane holds five rows: the two reads are not the same read.
    assert_eq!(run(&spec(ALL, lane("shown"), 100)).rows.len(), 5);
}

#[test]
fn rows_are_bounded_by_the_limit_and_the_walk_stops_in_the_middle_of_a_snapshot() {
    // One row short of every cell, exactly every cell, and one past it.
    assert_eq!(order(&run(&recent(ALL, 10))), NEWEST_FIRST_MEMBERS[..10].to_vec());
    assert_eq!(order(&run(&recent(ALL, 11))), NEWEST_FIRST_MEMBERS.to_vec());
    assert_eq!(order(&run(&recent(ALL, 12))), NEWEST_FIRST_MEMBERS.to_vec());
    // A limit of 1 is the newest cell: m2 of snapshot 400.
    let one = run(&recent(ALL, 1));
    assert_eq!((order(&one), stamps(&one)), (vec![2], vec![400]));
    // The limit is the ranked read's own: accepted at the top-k cap, refused one past it and at 0.
    assert!(validate_spec(&recent(ALL, MAX_TOP_K)).is_ok());
    assert!(validate_spec(&recent(ALL, MAX_TOP_K + 1)).unwrap_err().contains("top-k"));
    assert!(validate_spec(&recent(ALL, 0)).is_err());
}

#[test]
fn the_window_the_predicates_the_member_set_and_the_projection_apply_to_each_cell() {
    // A window of 150 to 350 holds snapshots 200 and 300.
    let windowed = run(&recent(CellWindow::Between { start_ms: 150, end_ms: 350 }, 100));
    assert_eq!(order(&windowed), vec![1, 3, 99, 1, 2, 4]);
    assert_eq!(windowed.snapshots, 2);

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
fn snapshots_sharing_a_stamp_come_back_with_the_later_one_in_the_slice_first() {
    // Snapshots of two keys at one stamp, as a merged read hands them over, earlier key first.
    let first = vec![cell(1, 1, [1, 0, 0, 0]), cell(5, 1, [2, 0, 0, 0])];
    let second = vec![cell(2, 1, [3, 0, 0, 0])];
    let slice = [
        SnapshotView { timestamp_ms: 50, cells: &first },
        SnapshotView { timestamp_ms: 100, cells: &first },
        SnapshotView { timestamp_ms: 100, cells: &second },
    ];
    let plan = Plan::compile(&header(), &recent(ALL, 100)).unwrap();
    let ranked = plan.run_ordered(&slice).unwrap();
    assert_eq!(order(&ranked), vec![2, 1, 5, 1, 5]);
    assert_eq!(stamps(&ranked), vec![100, 100, 100, 50, 50]);
}

#[test]
fn a_recent_read_refuses_a_quota_and_is_no_score_for_a_cap() {
    let mut quota = recent(ALL, 10);
    quota.quota = crate::types::CellQuota { lanes: Vec::new(), others: 1 };
    assert!(validate_spec(&quota).unwrap_err().contains("takes no quota"));
    assert!(Plan::compile(&header(), &quota).is_err());
    // The same quota on a reduced read is accepted: the refusal is about the stored-cell read.
    let mut reduced = spec(ALL, CellScore::Member, 10);
    reduced.quota = quota.quota.clone();
    assert!(validate_spec(&reduced).is_ok());
    // A rollup cap scores one folded cell by a header name; this is no such score.
    assert!(Scorer::compile(&header(), &CellScore::Recent).err().expect("refused").contains("not a score a cap can use"));
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
