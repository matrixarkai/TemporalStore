// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The lane cap of a cell: a key may declare up to [`MAX_LANES_PER_CELL`] (32) lanes. The cap is a
//! bound, not a layout: a header lists the lanes a key declares and a cell stores the lanes it
//! holds, so a key that declares two lanes costs exactly what it did under a lower cap, and every
//! lane added costs the same.

use super::*;

fn header_with_lanes(count: usize) -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: KeySpec {
            namespace: "ns".to_string(),
            entity: EntityType::U64,
            dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
        },
        labels: vec!["slot".to_string()],
        lanes: (0..count).map(|i| LaneSpec { name: format!("lane{i:02}"), reducer: LaneReducer::Sum }).collect(),
    }
}

fn cell(member: u64, lanes: usize, salt: i64) -> Cell {
    Cell {
        member: Member::U64(member),
        labels: vec![member % 3],
        lanes: (0..lanes).map(|i| salt + i as i64 * 7 - 40).collect(),
    }
}

#[test]
fn thirty_two_lanes_are_accepted_and_thirty_three_are_refused_by_the_header_and_by_a_cell() {
    assert_eq!(MAX_LANES_PER_CELL, 32);
    assert!(header_with_lanes(32).validate().is_ok());
    let over = header_with_lanes(33).validate().unwrap_err();
    assert!(over.contains("lanes per cell 33 is over the limit of 32"), "{over}");
    assert!(header_with_lanes(0).validate().is_err());

    // A value of 32-lane cells encodes and reads back; a 33rd lane on a cell is refused.
    let h = header_with_lanes(32);
    let cells = vec![cell(1, 32, 5), cell(9, 32, -3)];
    let value = encode_value(&h, &cells).expect("32 lanes encode");
    assert_eq!(decode_value(&value).expect("32 lanes decode"), (h.clone(), cells));
    let wide = header_with_lanes(33);
    assert!(encode_value(&wide, &[cell(1, 33, 0)]).is_err());
}

#[test]
fn a_merge_at_sixteen_and_at_thirty_two_lanes_is_the_plain_merge_byte_for_byte() {
    for lanes in [16usize, 32] {
        let h = header_with_lanes(lanes);
        let first = encode_value(&h, &[cell(1, lanes, 3), cell(5, lanes, 4), cell(8, lanes, -9)]).unwrap();
        let second = encode_value(&h, &[cell(5, lanes, 10), cell(2, lanes, 1), cell(5, lanes, 2)]).unwrap();
        // The flat path every write takes.
        let (stored, _, _) = merge_request(None, None, &first).unwrap();
        let (merged, _, counts) = merge_request(Some(&h), Some(&stored), &second).unwrap();
        // The plain statement of the merge.
        let (_, mut cells) = decode_value(&stored).unwrap();
        let (_, incoming) = decode_value(&second).unwrap();
        let plain_counts = merge_into_sample(&h, &mut cells, &incoming).unwrap();
        assert_eq!(merged, encode_value(&h, &cells).unwrap(), "{lanes} lanes");
        assert_eq!((counts.created, counts.merged), (plain_counts.created, plain_counts.merged), "{lanes} lanes");
        // Every lane of a merged member is the sum of what was written: member 5 got 4 + 10 + 2 on lane 0.
        let five = cells.iter().find(|c| c.member == Member::U64(5)).unwrap();
        assert_eq!(five.lanes[0], (4 - 40) + (10 - 40) + (2 - 40), "{lanes} lanes");
        assert_eq!(five.lanes.len(), lanes);
    }
}

#[test]
fn every_lane_costs_the_same_bytes_in_the_header_and_in_a_cell_at_any_count() {
    // The header and a one-cell sample grow by one fixed step per lane, from two lanes to the cap: there
    // is no padding to the cap and no step that depends on it, so a key that uses fewer lanes pays for
    // fewer. The control is that the sizes do grow (a flat series would make the step zero).
    let header_len = |n: usize| header_with_lanes(n).encode().unwrap().len();
    let step = header_len(3) - header_len(2);
    assert!(step > 0, "CONTROL: a lane adds bytes to the header");
    for n in 2..MAX_LANES_PER_CELL {
        assert_eq!(header_len(n + 1) - header_len(n), step, "header step at {n} lanes");
    }
    // One cell whose lanes all hold 1 (one byte each), packed as a sample stores it.
    let cell_len = |n: usize| {
        let ones = Cell { member: Member::U64(7), labels: vec![1], lanes: vec![1; n] };
        encode_sample(&header_with_lanes(n), &[ones]).unwrap().len()
    };
    let cell_step = cell_len(3) - cell_len(2);
    assert!(cell_step > 0, "CONTROL: a lane adds bytes to a cell");
    for n in 2..MAX_LANES_PER_CELL {
        assert_eq!(cell_len(n + 1) - cell_len(n), cell_step, "cell step at {n} lanes");
    }
}
