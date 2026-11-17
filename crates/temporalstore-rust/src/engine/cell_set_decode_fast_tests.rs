// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The decode of a sample reads a cell written as the encoder writes it straight through, and any
//! other cell field by field. The two readings must agree on every sample and on every damage a
//! sample can take: the same cells, or the same refusal in the same words.

use super::*;

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn header(labels: usize, lanes: usize) -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: KeySpec {
            namespace: "ns".to_string(),
            entity: EntityType::U64,
            dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
        },
        labels: (0..labels).map(|i| format!("g{i}")).collect(),
        lanes: (0..lanes)
            .map(|i| LaneSpec { name: format!("l{i}"), reducer: LaneReducer::Sum })
            .collect(),
    }
}

/// `count` cells with ascending members (distances when they are all numbers), `bytes_members`
/// of them bytes, and values that need one, two and several varint bytes, some of them negative.
fn random_cells(state: &mut u64, h: &CellHeader, count: usize, bytes_members: bool) -> Vec<Cell> {
    let mut member = 0u64;
    let mut cells: Vec<Cell> = (0..count)
        .map(|_| {
            member += 1 + xorshift(state) % 300;
            let wide = |state: &mut u64| match xorshift(state) % 4 {
                0 => xorshift(state) % 100,
                1 => xorshift(state) % 20_000,
                2 => xorshift(state) % 5_000_000,
                _ => xorshift(state),
            };
            Cell {
                member: Member::U64(member),
                labels: (0..h.labels.len()).map(|_| wide(state)).collect(),
                lanes: (0..h.lanes.len())
                    .map(|_| {
                        let v = (wide(state) >> 1) as i64;
                        if xorshift(state) % 3 == 0 { -v } else { v }
                    })
                    .collect(),
            }
        })
        .collect();
    if bytes_members {
        for tag in [&b"aa"[..], &b"zz"[..]] {
            cells.push(Cell {
                member: Member::Bytes(tag.to_vec()),
                labels: (0..h.labels.len()).map(|_| xorshift(state) % 50).collect(),
                lanes: (0..h.lanes.len()).map(|_| (xorshift(state) % 50) as i64).collect(),
            });
        }
    }
    cells
}

fn packed_of(h: &CellHeader, cells: &[Cell]) -> Vec<u8> {
    let value = encode_value(h, cells).unwrap();
    split_value(&value).unwrap().1.to_vec()
}

fn same(a: &Result<FlatCells, String>, b: &Result<FlatCells, String>) -> bool {
    match (a, b) {
        (Ok(a), Ok(b)) => {
            a.len() == b.len()
                && (0..a.len()).all(|i| {
                    let (x, y) = (a.cell(i), b.cell(i));
                    x.member == y.member && x.labels == y.labels && x.lanes == y.lanes
                })
        }
        (Err(a), Err(b)) => a == b,
        _ => false,
    }
}

#[test]
fn a_sample_decodes_the_same_through_the_fast_path_and_field_by_field() {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut compared = 0usize;
    for (labels, lanes) in [(0usize, 1usize), (1, 2), (2, 6), (4, 16)] {
        let h = header(labels, lanes);
        for count in [0usize, 1, 5, 64, 300] {
            for bytes_members in [false, true] {
                let cells = random_cells(&mut state, &h, count, bytes_members);
                let packed = packed_of(&h, &cells);
                let fast = FlatCells::decode(&h, &packed, 0);
                let slow = FlatCells::decode_by_fields(&h, &packed, 0);
                assert!(fast.is_ok(), "an encoded sample must decode: {fast:?}");
                assert!(same(&fast, &slow), "{labels} labels {lanes} lanes {count} cells bytes={bytes_members}");
                assert_eq!(fast.as_ref().unwrap().len(), cells.len());
                compared += 1;
            }
        }
    }
    assert!(compared >= 40, "{compared}");
}

#[test]
fn a_sample_written_under_a_narrower_header_reads_as_zero_in_what_it_lacks_either_way() {
    let mut state = 0x1234_5678_9abc_def1u64;
    let narrow = header(1, 2);
    let wide = header(3, 5);
    let cells = random_cells(&mut state, &narrow, 100, false);
    let packed = packed_of(&narrow, &cells);
    let fast = FlatCells::decode(&wide, &packed, 3);
    let slow = FlatCells::decode_by_fields(&wide, &packed, 3);
    assert!(same(&fast, &slow));
    let decoded = fast.unwrap();
    assert_eq!(100, decoded.len());
    for i in 0..decoded.len() {
        let cell = decoded.cell(i);
        assert_eq!(&cell.labels[1..], &[0, 0]);
        assert_eq!(&cell.lanes[2..], &[0, 0, 0]);
    }
}

#[test]
fn a_damaged_sample_is_refused_in_the_same_words_or_read_as_the_same_cells_either_way() {
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let (mut ok, mut refused) = (0usize, 0usize);
    for (labels, lanes) in [(1usize, 2usize), (2, 3)] {
        let h = header(labels, lanes);
        let cells = random_cells(&mut state, &h, 40, true);
        let packed = packed_of(&h, &cells);
        for round in 0..1500 {
            let mut bytes = packed.clone();
            match round % 5 {
                0 => {
                    let at = (xorshift(&mut state) as usize) % bytes.len();
                    bytes[at] ^= 1 << (xorshift(&mut state) % 8);
                }
                1 => bytes.truncate((xorshift(&mut state) as usize) % bytes.len()),
                2 => {
                    let at = (xorshift(&mut state) as usize) % bytes.len();
                    bytes.insert(at, (xorshift(&mut state) % 256) as u8);
                }
                3 => {
                    let at = (xorshift(&mut state) as usize) % bytes.len();
                    bytes[at] = (xorshift(&mut state) % 256) as u8;
                    let more = (xorshift(&mut state) as usize) % bytes.len();
                    bytes[more] = (xorshift(&mut state) % 256) as u8;
                }
                _ => {
                    let from = (xorshift(&mut state) as usize) % bytes.len();
                    let to = from + (xorshift(&mut state) as usize) % (bytes.len() - from);
                    let piece = bytes[from..=to.min(bytes.len() - 1)].to_vec();
                    bytes.extend_from_slice(&piece);
                }
            }
            let fast = FlatCells::decode(&h, &bytes, 0);
            let slow = FlatCells::decode_by_fields(&h, &bytes, 0);
            assert!(same(&fast, &slow), "round {round}: {fast:?} against {slow:?}");
            if fast.is_ok() {
                ok += 1;
            } else {
                refused += 1;
            }
        }
    }
    // The controls: the damage reached both arms, so neither comparison was empty.
    assert!(ok > 50, "{ok} damaged samples still decoded");
    assert!(refused > 500, "{refused} damaged samples were refused");
}

#[test]
fn every_cell_of_a_sample_written_as_the_encoder_writes_it_takes_the_fast_path_at_any_size() {
    let h = header(2, 3);
    let mut state = 0xdead_beef_cafe_f00du64;
    for count in [64usize, 1024, 4096] {
        let cells = random_cells(&mut state, &h, count, false);
        let packed = packed_of(&h, &cells);
        FAST_RECORDS.with(|n| n.set(0));
        let decoded = FlatCells::decode(&h, &packed, 0).unwrap();
        let fast = FAST_RECORDS.with(|n| n.get());
        assert_eq!(count, decoded.len());
        assert_eq!(count as u64, fast, "{count} cells: only {fast} were read through the fast path");
    }
    // The control: a sample whose members are bytes is read field by field, so the counter is
    // not one that counts every decode.
    let cells = random_cells(&mut state, &h, 0, true);
    let packed = packed_of(&h, &cells);
    FAST_RECORDS.with(|n| n.set(0));
    assert_eq!(2, FlatCells::decode(&h, &packed, 0).unwrap().len());
    assert_eq!(0, FAST_RECORDS.with(|n| n.get()));
}
