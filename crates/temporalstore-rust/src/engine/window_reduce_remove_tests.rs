// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Tests of the pure removal (`window_reduce_remove`). Every expectation is derived by a plain closure over
//! the stored cells (stamp, member, label, lane values) that shares no code with the selection, so
//! the assertions are the arithmetic and not a copy of the code.
//!
//! The data, as (stamp: member/label/shown), is
//!
//! ```text
//! 100: m1/1/10  m2/2/20  m3/1/100        200: m1/1/10  m2/3/30  m4/2/8
//! 300: m1/2/10  m3/1/100 zz/4/5          400: m2/2/10  m4/2/2
//! ```

use super::*;
use crate::engine::cell_set::{
    DimSpec, DimType, EntityType, KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{CellMemberFilter, CellMemberId, FeatureFilter, FeatureFilterOp};

fn header() -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: KeySpec {
            namespace: "ns".to_string(),
            entity: EntityType::U64,
            dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
        },
        labels: vec!["slot".to_string()],
        lanes: [("shown", LaneReducer::Sum), ("clicked", LaneReducer::Sum)]
            .into_iter()
            .map(|(name, reducer)| LaneSpec { name: name.to_string(), reducer })
            .collect(),
    }
}

fn cell(member: u64, slot: u64, shown: i64) -> Cell {
    Cell { member: Member::U64(member), labels: vec![slot], lanes: vec![shown, shown / 10] }
}

fn bytes_cell(slot: u64, shown: i64) -> Cell {
    Cell { member: Member::Bytes(b"zz".to_vec()), labels: vec![slot], lanes: vec![shown, shown / 10] }
}

fn data() -> Vec<(u64, Vec<Cell>)> {
    vec![
        (100, vec![cell(1, 1, 10), cell(2, 2, 20), cell(3, 1, 100)]),
        (200, vec![cell(1, 1, 10), cell(2, 3, 30), cell(4, 2, 8)]),
        (300, vec![cell(1, 2, 10), cell(3, 1, 100), bytes_cell(4, 5)]),
        (400, vec![cell(2, 2, 10), cell(4, 2, 2)]),
    ]
}

fn spec() -> WindowReduceRemoveSpec {
    WindowReduceRemoveSpec::default()
}

fn id(member: u64) -> CellMemberId {
    CellMemberId::U64(member)
}

fn on(field: &str, op: FeatureFilterOp, value: u64) -> FeatureFilter {
    FeatureFilter { field: field.to_string(), op, value }
}

/// The member as a number, the bytes member `zz` as 99.
fn number(cell: &Cell) -> u64 {
    match &cell.member {
        Member::U64(v) => *v,
        Member::Bytes(_) => 99,
    }
}

fn run(spec: &WindowReduceRemoveSpec) -> Vec<Outcome> {
    let selection = Selection::compile(&header(), spec).expect("the spec is valid");
    remove_cells(&selection, data())
}

/// (stamp, members kept, cells removed) per sample in the window.
fn shape(outcomes: &[Outcome]) -> Vec<(u64, Vec<u64>, usize)> {
    outcomes
        .iter()
        .map(|o| (o.timestamp_ms, o.kept.iter().map(number).collect(), o.removed))
        .collect()
}

#[test]
fn a_window_takes_every_cell_of_the_samples_inside_it_and_both_bounds_are_inclusive() {
    let both = WindowReduceRemoveSpec { start_ms: Some(200), end_ms: Some(300), ..spec() };
    // 100 and 400 are outside the window and are not reported at all.
    assert_eq!(shape(&run(&both)), vec![(200, vec![], 3), (300, vec![], 3)]);
    // One stamp past either end leaves that sample out.
    let inner = WindowReduceRemoveSpec { start_ms: Some(201), end_ms: Some(299), ..spec() };
    assert_eq!(shape(&run(&inner)), vec![]);
    let from = WindowReduceRemoveSpec { start_ms: Some(300), ..spec() };
    assert_eq!(shape(&run(&from)), vec![(300, vec![], 3), (400, vec![], 2)]);
    let until = WindowReduceRemoveSpec { end_ms: Some(100), ..spec() };
    assert_eq!(shape(&run(&until)), vec![(100, vec![], 3)]);
    // A window of one stamp is accepted, as start == end.
    let one = WindowReduceRemoveSpec { start_ms: Some(400), end_ms: Some(400), ..spec() };
    assert_eq!(shape(&run(&one)), vec![(400, vec![], 2)]);
}

#[test]
fn a_predicate_takes_the_cells_it_holds_on_and_keeps_the_rest_in_stored_order() {
    // slot = 1 holds on m1 and m3 at 100, m1 at 200 and m3 at 300.
    let slot = WindowReduceRemoveSpec { filters: vec![on("slot", FeatureFilterOp::Equal, 1)], ..spec() };
    assert_eq!(
        shape(&run(&slot)),
        vec![(100, vec![2], 2), (200, vec![2, 4], 1), (300, vec![1, 99], 1), (400, vec![2, 4], 0)]
    );
    // A lane: shown >= 30 holds on m3 at 100 and 300 and on m2 at 200.
    let big = WindowReduceRemoveSpec { filters: vec![on("shown", FeatureFilterOp::GreaterOrEqual, 30)], ..spec() };
    assert_eq!(
        shape(&run(&big)),
        vec![(100, vec![1, 2], 1), (200, vec![1, 4], 1), (300, vec![1, 99], 1), (400, vec![2, 4], 0)]
    );
    // `member` is a predicate target too; a bytes member has no number, so it never holds.
    let high = WindowReduceRemoveSpec { filters: vec![on("member", FeatureFilterOp::GreaterThan, 2)], ..spec() };
    assert_eq!(
        shape(&run(&high)),
        vec![(100, vec![1, 2], 1), (200, vec![1, 2], 1), (300, vec![1, 99], 1), (400, vec![2], 1)]
    );
    // Predicates are ANDed: slot = 1 and shown >= 100 holds on m3 at 100 and at 300, not on m1.
    let both = WindowReduceRemoveSpec {
        filters: vec![on("slot", FeatureFilterOp::Equal, 1), on("shown", FeatureFilterOp::GreaterOrEqual, 100)],
        ..spec()
    };
    assert_eq!(
        shape(&run(&both)),
        vec![(100, vec![1, 2], 1), (200, vec![1, 2, 4], 0), (300, vec![1, 99], 1), (400, vec![2, 4], 0)]
    );
}

#[test]
fn a_member_set_takes_the_members_it_names_and_spares_the_ones_it_excludes() {
    let include = WindowReduceRemoveSpec {
        members: CellMemberFilter { include: Some(vec![id(2), id(4)]), exclude: vec![] },
        ..spec()
    };
    assert_eq!(
        shape(&run(&include)),
        vec![(100, vec![1, 3], 1), (200, vec![1], 2), (300, vec![1, 3, 99], 0), (400, vec![], 2)]
    );
    // Only an exclude set: every member but the named one goes.
    let spare = WindowReduceRemoveSpec { members: CellMemberFilter { include: None, exclude: vec![id(1)] }, ..spec() };
    assert_eq!(
        shape(&run(&spare)),
        vec![(100, vec![1], 2), (200, vec![1], 2), (300, vec![1], 2), (400, vec![], 2)]
    );
    // A member in both sets is spared.
    let both = WindowReduceRemoveSpec {
        members: CellMemberFilter { include: Some(vec![id(1), id(2)]), exclude: vec![id(2)] },
        ..spec()
    };
    assert_eq!(
        shape(&run(&both)),
        vec![(100, vec![2, 3], 1), (200, vec![2, 4], 1), (300, vec![3, 99], 1), (400, vec![2, 4], 0)]
    );
    // A bytes member is named as bytes.
    let zz = WindowReduceRemoveSpec {
        members: CellMemberFilter { include: Some(vec![CellMemberId::Bytes(b"zz".to_vec())]), exclude: vec![] },
        ..spec()
    };
    assert_eq!(shape(&run(&zz))[2], (300, vec![1, 3], 1));
}

#[test]
fn the_window_the_members_and_the_predicates_are_anded() {
    let all = WindowReduceRemoveSpec {
        start_ms: Some(100),
        end_ms: Some(300),
        members: CellMemberFilter { include: Some(vec![id(1), id(3), id(4)]), exclude: vec![] },
        filters: vec![on("slot", FeatureFilterOp::Equal, 1)],
    };
    // Inside 100..=300, members 1, 3 or 4, slot 1: m1 and m3 at 100, m1 at 200, m3 at 300. The
    // sample at 400 is outside the window, so it is not reported though m4 is named.
    assert_eq!(shape(&run(&all)), vec![(100, vec![2], 2), (200, vec![2, 4], 1), (300, vec![1, 99], 1)]);
}

#[test]
fn the_order_and_repetition_of_a_member_set_change_nothing() {
    let sorted = WindowReduceRemoveSpec { members: CellMemberFilter { include: Some(vec![id(1), id(2)]), exclude: vec![] }, ..spec() };
    let shuffled = WindowReduceRemoveSpec {
        members: CellMemberFilter { include: Some(vec![id(2), id(1), id(2), id(1)]), exclude: vec![] },
        ..spec()
    };
    assert_eq!(run(&sorted), run(&shuffled));
}

#[test]
fn a_removal_that_restricts_nothing_is_the_whole_key_and_each_way_of_restricting_is_not() {
    assert_eq!(validate_spec(&spec()), Ok(()));
    assert!(removes_the_whole_key(&spec()));
    for restricted in [
        WindowReduceRemoveSpec { start_ms: Some(0), ..spec() },
        WindowReduceRemoveSpec { end_ms: Some(u64::MAX), ..spec() },
        WindowReduceRemoveSpec { members: CellMemberFilter { include: Some(vec![id(1)]), exclude: vec![] }, ..spec() },
        WindowReduceRemoveSpec { members: CellMemberFilter { include: None, exclude: vec![id(1)] }, ..spec() },
        WindowReduceRemoveSpec { filters: vec![on("slot", FeatureFilterOp::NotEqual, 9)], ..spec() },
    ] {
        assert_eq!(validate_spec(&restricted), Ok(()), "{restricted:?}");
        assert!(!removes_the_whole_key(&restricted), "{restricted:?}");
    }
}

#[test]
fn a_name_the_key_does_not_declare_an_empty_include_set_and_a_backwards_window_are_refused() {
    let unknown = WindowReduceRemoveSpec { filters: vec![on("views", FeatureFilterOp::Equal, 1)], ..spec() };
    let err = Selection::compile(&header(), &unknown).err().expect("refused");
    assert!(err.contains("`views`") && err.contains("slot") && err.contains("shown"), "{err}");

    let empty = WindowReduceRemoveSpec { members: CellMemberFilter { include: Some(vec![]), exclude: vec![] }, ..spec() };
    assert!(validate_spec(&empty).unwrap_err().contains("include set names no member"));

    let backwards = WindowReduceRemoveSpec { start_ms: Some(10), end_ms: Some(9), ..spec() };
    assert!(validate_spec(&backwards).unwrap_err().contains("after it ends"));
    // One stamp apart the other way is a window of two stamps.
    assert_eq!(validate_spec(&WindowReduceRemoveSpec { start_ms: Some(9), end_ms: Some(10), ..spec() }), Ok(()));
}

#[test]
fn predicates_and_member_sets_are_accepted_at_the_limit_and_refused_one_past_it() {
    let predicates = |n: usize| WindowReduceRemoveSpec {
        filters: (0..n).map(|i| on("slot", FeatureFilterOp::NotEqual, i as u64)).collect(),
        ..spec()
    };
    assert_eq!(validate_spec(&predicates(MAX_REMOVE_PREDICATES)), Ok(()));
    assert!(validate_spec(&predicates(MAX_REMOVE_PREDICATES + 1)).unwrap_err().contains("predicates per removal"));
    assert_eq!(MAX_REMOVE_PREDICATES, 8);

    let side = |n: usize| (0..n as u64).map(id).collect::<Vec<_>>();
    for (include, exclude) in [(true, false), (false, true)] {
        let set = |n: usize| WindowReduceRemoveSpec {
            members: CellMemberFilter {
                include: include.then(|| side(n)),
                exclude: if exclude { side(n) } else { vec![] },
            },
            ..spec()
        };
        assert_eq!(validate_spec(&set(1024)), Ok(()), "include={include}");
        assert!(validate_spec(&set(1025)).unwrap_err().contains("1024"), "include={include}");
    }
    // A bytes member longer than any stored member could be is refused rather than never matched.
    let long = WindowReduceRemoveSpec {
        members: CellMemberFilter { include: Some(vec![CellMemberId::Bytes(vec![7; 257])]), exclude: vec![] },
        ..spec()
    };
    assert!(validate_spec(&long).is_err());
}

#[test]
fn a_sample_that_keeps_every_cell_comes_back_with_the_cells_it_had() {
    let none = WindowReduceRemoveSpec { filters: vec![on("slot", FeatureFilterOp::Equal, 9)], ..spec() };
    let outcomes = run(&none);
    assert!(outcomes.iter().all(|o| o.removed == 0));
    let stored = data();
    for (outcome, (stamp, cells)) in outcomes.iter().zip(&stored) {
        assert_eq!((outcome.timestamp_ms, &outcome.kept), (*stamp, cells));
    }
}
