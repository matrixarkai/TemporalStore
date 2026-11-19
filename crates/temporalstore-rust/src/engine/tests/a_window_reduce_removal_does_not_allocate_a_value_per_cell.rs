// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A cell removal reads each sample in the flat form: its allocations do not grow with the cells a
//! sample holds.
//!
//! It decoded every sample into one value per cell (a label vector and a lane vector each), so a
//! removal over 64 samples of 1,024 cells took 47 ms and its allocations rose with every cell.
//!
//! The guard removes half the cells of 16 samples at 64 and at 1,024 cells a sample and compares the
//! growth in allocations with the growth of THE CONTROL measured in the same test: the per-cell
//! decode of the same stored samples. It is a relative bound, so it needs no absolute figure, and
//! it cannot pass when the instrument sees nothing.

#![allow(clippy::all)]
use super::*;
use super::the_window_reduce_bytes_are_counted_per_operation::{header, open, seed_samples, stored_key, value_of};
use crate::types::{CellMemberFilter, CellMemberId, WindowReduceRemoveSpec};

const SAMPLES: usize = 16;

fn remove_half(engine: &TemporalEngine, key: &str, cells: usize) -> ExecuteResponse {
    engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::WindowReduceRemove {
            key: key.to_string(),
            spec: WindowReduceRemoveSpec {
                start_ms: None,
                end_ms: None,
                members: CellMemberFilter {
                    include: Some((0..(cells as u64 / 2)).map(CellMemberId::U64).collect()),
                    exclude: Vec::new(),
                },
                filters: Vec::new(),
            },
        },
    })
}

/// Allocations of one removal of half the cells.
#[cfg(feature = "alloc-probe")]
fn removal_allocs(cells: usize) -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    let key = stored_key(1);
    seed_samples(&engine, &key, SAMPLES, cells);
    let probe = crate::alloc_probe::Probe::start();
    let response = remove_half(&engine, &key, cells);
    let allocs = probe.stop().allocs;
    assert!(response.status.ok, "{:?}", response.status);
    allocs
}

/// THE CONTROL: the per-cell decode of the same stored samples.
#[cfg(feature = "alloc-probe")]
fn per_cell_decode_allocs(cells: usize) -> u64 {
    let header = header();
    let stored: Vec<Vec<u8>> = (0..SAMPLES as u64).map(|salt| value_of(cells, salt + 1)).collect();
    let probe = crate::alloc_probe::Probe::start();
    for bytes in &stored {
        let (_, packed) = crate::engine::cell_set::split_value(bytes).expect("split");
        let decoded = crate::engine::cell_set::decode_sample(&header, packed).expect("decode");
        std::hint::black_box(&decoded);
    }
    probe.stop().allocs
}

#[cfg(feature = "alloc-probe")]
#[test]
fn a_removal_does_not_allocate_more_as_the_cells_of_a_sample_grow() {
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(canary.stop().allocs > 0, "counting allocator not installed");
    drop(sink);

    let (small, large) = (64usize, 1_024usize);
    let _ = removal_allocs(small);
    let (flat_small, flat_large) = (removal_allocs(small), removal_allocs(large));
    let (control_small, control_large) = (per_cell_decode_allocs(small), per_cell_decode_allocs(large));
    println!("removal allocs: {flat_small} -> {flat_large}; per-cell decode alone: {control_small} -> {control_large}");
    let control_growth = control_large.saturating_sub(control_small);
    assert!(
        control_growth >= (SAMPLES * (large - small)) as u64,
        "the per-cell control grew by {control_growth} allocations for {} more cells, so this instrument cannot see a value per cell",
        SAMPLES * (large - small)
    );
    let growth = flat_large.saturating_sub(flat_small);
    assert!(
        growth * 20 < control_growth,
        "a removal allocated {flat_small} at {small} cells a sample and {flat_large} at {large}; the per-cell decode alone grows by {control_growth}"
    );
}

/// The removal still takes the cells it should: half of each sample goes and the rest stay.
#[test]
fn a_removal_over_flat_samples_removes_exactly_the_named_cells() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    let key = stored_key(1);
    seed_samples(&engine, &key, 4, 64);
    let response = remove_half(&engine, &key, 64);
    assert!(response.status.ok, "{:?}", response.status);
    match response.response {
        CommandResponse::WindowReduceRemoveReceipt { samples_before, samples_after, cells_before, cells_removed } => {
            assert_eq!((samples_before, samples_after), (4, 4));
            assert_eq!(cells_before, 4 * 64);
            assert_eq!(cells_removed, 4 * 32);
        }
        other => panic!("not a removal receipt: {other:?}"),
    }
    // A second identical removal finds nothing to take.
    let again = remove_half(&engine, &key, 64);
    match again.response {
        CommandResponse::WindowReduceRemoveReceipt { cells_removed, .. } => assert_eq!(cells_removed, 0),
        other => panic!("not a removal receipt: {other:?}"),
    }
}
