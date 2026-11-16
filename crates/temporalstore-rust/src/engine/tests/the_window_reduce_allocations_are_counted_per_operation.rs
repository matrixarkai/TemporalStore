// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE OPERATION OF THE CELL MODEL ALLOCATES, COUNTED BY THE COUNTING ALLOCATOR, THROUGH THE
//! REAL `execute` PATH.
//!
//! Each table sweeps ONE dimension at a time (samples, cells per sample, keys) so a cost that
//! scales with a count shows as a slope. Counts are allocation BLOCKS first, bytes second. Run:
//!
//!   cargo test --features alloc-probe --lib the_window_reduce_allocation_tables -- --ignored --nocapture --test-threads=1
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::alloc_probe::Probe;
use crate::engine::cell_set::{
    encode_key, encode_value, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType, EntityValue,
    KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{WindowReduceCapSpec, WindowReduceRankSpec, WindowReduceRollupSpec, WindowReduceScore, WindowReduceWindow, FeatureWritePolicy};

pub(super) fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

pub(super) fn exec(engine: &TemporalEngine, command: Command) -> ExecuteResponse {
    engine.execute(ExecuteRequest { shard_id: 1, command })
}

fn key_spec() -> KeySpec {
    KeySpec {
        namespace: "ns".to_string(),
        entity: EntityType::U64,
        dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
    }
}

pub(super) fn stored_key(entity: u64) -> String {
    encode_key(
        &key_spec(),
        &CellKey { entity: EntityValue::U64(entity), dims: vec![Some(DimValue::U16(3))] },
    )
    .unwrap()
}

pub(super) fn header() -> CellHeader {
    CellHeader {
        version: HEADER_VERSION,
        key: key_spec(),
        labels: vec!["slot".to_string()],
        lanes: vec![
            LaneSpec { name: "shown".to_string(), reducer: LaneReducer::Sum },
            LaneSpec { name: "peak".to_string(), reducer: LaneReducer::Max },
        ],
    }
}

pub(super) fn cells(count: usize, salt: u64) -> Vec<Cell> {
    (0..count as u64)
        .map(|m| Cell {
            member: Member::U64(m),
            labels: vec![m % 7],
            lanes: vec![(m + salt) as i64 % 13, (m * salt) as i64 % 29],
        })
        .collect()
}

/// `samples` samples of `cell_count` cells each under `key`, stamped 100, 200, ...
pub(super) fn seed(engine: &TemporalEngine, key: &str, samples: usize, cell_count: usize) {
    let h = header();
    for i in 0..samples {
        let value = encode_value(&h, &cells(cell_count, i as u64 + 1)).unwrap();
        let response = exec(
            engine,
            Command::FeatureAppend {
                key: key.to_string(),
                points: vec![FeaturePoint { timestamp_ms: 100 * (i as u64 + 1), value }],
            },
        );
        assert!(response.status.ok, "seed refused: {:?}", response.status);
    }
}

pub(super) fn spec() -> WindowReduceRankSpec {
    WindowReduceRankSpec::new(
        WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX },
        WindowReduceScore::Lane { lane: "shown".to_string() },
        10,
    )
}

/// (allocations, bytes) of one run of `op`, after `op` has run once to warm every cache.
pub(super) fn per_op(reps: usize, mut op: impl FnMut()) -> (f64, f64) {
    op();
    let probe = Probe::start();
    for _ in 0..reps {
        op();
    }
    let counts = probe.stop();
    (counts.allocs as f64 / reps as f64, counts.alloc_bytes as f64 / reps as f64)
}

pub(super) fn ranked_read(engine: &TemporalEngine, key: &str) -> Vec<crate::types::RankedCellRow> {
    match exec(engine, Command::WindowReduceRank { key: key.to_string(), spec: spec() }).response {
        CommandResponse::WindowReduceRanking { ranked } => {
            assert!(ranked.refused.is_empty(), "{:?}", ranked);
            ranked.rows
        }
        other => panic!("not a ranking: {other:?}"),
    }
}

pub(super) fn fan_in_read(engine: &TemporalEngine, keys: &[String]) -> Vec<crate::types::RankedCellRow> {
    match exec(engine, Command::WindowReduceFanInRank { keys: keys.to_vec(), spec: spec() }).response {
        CommandResponse::WindowReduceRanking { ranked } => {
            assert!(ranked.refused.is_empty(), "{:?}", ranked);
            ranked.rows
        }
        other => panic!("not a ranking: {other:?}"),
    }
}

/// (allocations, bytes) of one rollup of `samples` samples of `cell_count` cells into ten buckets.
pub(super) fn rollup_once(samples: usize, cell_count: usize) -> (f64, f64) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let key = stored_key(1);
    seed(&engine, &key, samples, cell_count);
    let span = 100 * (samples as u64 + 1);
    let spec = WindowReduceRollupSpec {
        older_than_ms: span,
        bucket_ms: span / 10,
        cap: None::<WindowReduceCapSpec>,
        anchor: Default::default(),
        coarser_tiers: Vec::new(),
        truncate: None,
    };
    let probe = Probe::start();
    let response = exec(&engine, Command::WindowReduceRollup { key, spec });
    let counts = probe.stop();
    assert!(response.status.ok, "{:?}", response.status);
    (counts.allocs as f64, counts.alloc_bytes as f64)
}

/// (allocations, bytes) of one merge-reduce write of `cell_count` cells into a sample of the same members.
fn merge_once(cell_count: usize) -> (f64, f64) {
    const OPS: usize = 20;
    let h = header();
    let key = stored_key(1);
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let members = cells(cell_count, 1);
    let write = |engine: &TemporalEngine, value: Vec<u8>| {
        let response = exec(
            engine,
            Command::FeatureAppendWithPolicy {
                key: key.clone(),
                points: vec![FeaturePoint { timestamp_ms: 100, value }],
                policy: FeatureWritePolicy::MergeReduce,
                retry_token: None,
            },
        );
        assert!(response.status.ok, "{:?}", response.status);
    };
    write(&engine, encode_value(&h, &members).unwrap());
    // The requests are encoded before the probe starts: the encode is the caller's, not the write's.
    let requests: Vec<Vec<u8>> = (0..OPS).map(|_| encode_value(&h, &members).unwrap()).collect();
    let probe = Probe::start();
    for value in requests {
        write(&engine, value);
    }
    let counts = probe.stop();
    (counts.allocs as f64 / OPS as f64, counts.alloc_bytes as f64 / OPS as f64)
}

#[test]
#[ignore = "measurement: run with --features alloc-probe --ignored"]
fn the_window_reduce_allocation_tables() {
    for cell_count in [1usize, 64, 1024] {
        let (a, b) = merge_once(cell_count);
        eprintln!("CELLTABLE merge cells={cell_count} allocs={a:.1} bytes={b:.0}");
    }
    for (samples, cell_count) in [(1usize, 200usize), (16, 200), (256, 200), (16, 50), (16, 800)] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        let key = stored_key(1);
        seed(&engine, &key, samples, cell_count);
        let (a, b) = per_op(5, || {
            assert_eq!(10, ranked_read(&engine, &key).len());
        });
        eprintln!(
            "CELLTABLE rank samples={samples} cells={cell_count} allocs={a:.1} bytes={b:.0} allocs_per_cell_read={:.3}",
            a / (samples * cell_count) as f64
        );
    }
    for (keys, samples, cell_count) in [(8usize, 4usize, 200usize), (8, 16, 200), (8, 16, 50), (2, 16, 200)] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        let names: Vec<String> = (0..keys as u64).map(|e| stored_key(e + 1)).collect();
        for name in &names {
            seed(&engine, name, samples, cell_count);
        }
        let (a, b) = per_op(5, || {
            assert_eq!(10, fan_in_read(&engine, &names).len());
        });
        eprintln!(
            "CELLTABLE fan_in keys={keys} samples_per_key={samples} cells={cell_count} allocs={a:.1} bytes={b:.0} allocs_per_cell_read={:.3}",
            a / (keys * samples * cell_count) as f64
        );
    }
    for (samples, cell_count) in [(100usize, 50usize), (1000, 50), (1000, 200)] {
        let (a, b) = rollup_once(samples, cell_count);
        eprintln!(
            "CELLTABLE rollup samples={samples} cells={cell_count} allocs={a:.0} bytes={b:.0} allocs_per_cell={:.3}",
            a / (samples * cell_count) as f64
        );
    }
    {
        use crate::engine::cell_retry_ring::{RetryReceipt, RetryRing};
        let mut ring = RetryRing::new();
        let receipt = RetryReceipt { samples_written: 1, cells_created: 2, cells_merged: 3 };
        for token in 0..16u64 {
            ring.record(token, receipt).unwrap();
        }
        let probe = Probe::start();
        for token in 0..1000u64 {
            let _ = ring.admit(token);
        }
        let admit = probe.stop();
        let probe = Probe::start();
        for token in 100..1100u64 {
            ring.record(token, receipt).unwrap();
        }
        let record = probe.stop();
        let probe = Probe::start();
        for _ in 0..1000 {
            let _ = ring.encode();
        }
        let encode = probe.stop();
        eprintln!(
            "CELLTABLE retry_ring admit_allocs_per_1000={} record_allocs_per_1000={} encode_allocs_per_1000={}",
            admit.allocs, record.allocs, encode.allocs
        );
    }
}

/// Reads of one key of 16 samples at `cell_count` cells each, through the engine: the
/// allocations and bytes of one read.
fn rank_at(cell_count: usize) -> (f64, f64) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let key = stored_key(1);
    seed(&engine, &key, 16, cell_count);
    per_op(5, || {
        assert_eq!(10, ranked_read(&engine, &key).len());
    })
}

/// A ranked read through the engine decodes every sample it reads, so what it allocates follows
/// the SAMPLES it reads and not the cells in them. Decoding a cell into a `Cell` cost two
/// allocations (its labels and its lanes) for every stored cell, on every read. The flat form holds a sample's cells in
/// a few vectors, so the two cell counts allocate the same number of times.
///
/// The CONTROL is the same two reads in bytes, which must grow with the cell count: a count that
/// is flat because the probe cannot see the cell count is not a pass. The first two asserts keep
/// the probe from being blind; the third is the flatness.
#[test]
#[ignore = "measurement: run with --features alloc-probe --ignored"]
fn a_ranked_read_through_the_engine_does_not_allocate_per_cell() {
    let (few, few_bytes) = rank_at(50);
    let (many, many_bytes) = rank_at(800);
    eprintln!("RANK_FLAT 50 cells: {few} allocs {few_bytes} B; 800 cells: {many} allocs {many_bytes} B");
    assert!(few > 16.0, "the probe is blind: {few} allocations for a read of 16 samples");
    assert!(many_bytes > few_bytes * 4.0, "the probe cannot see the cell count: {few_bytes} -> {many_bytes}");
    assert!(
        (many - few).abs() < 4.0,
        "the allocations of a read moved with the cell count ({few} at 50, {many} at 800): a per-cell allocation is back"
    );
}

/// The same for a read over several keys: 8 keys of 4 samples, at 50 and at 800 cells.
#[test]
#[ignore = "measurement: run with --features alloc-probe --ignored"]
fn a_fan_in_read_through_the_engine_does_not_allocate_per_cell() {
    let at = |cell_count: usize| -> (f64, f64) {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        let names: Vec<String> = (0..8u64).map(|e| stored_key(e + 1)).collect();
        for name in &names {
            seed(&engine, name, 4, cell_count);
        }
        per_op(5, || {
            assert_eq!(10, fan_in_read(&engine, &names).len());
        })
    };
    let (few, few_bytes) = at(50);
    let (many, many_bytes) = at(800);
    eprintln!("FAN_IN_FLAT 50 cells: {few} allocs {few_bytes} B; 800 cells: {many} allocs {many_bytes} B");
    assert!(few > 32.0, "the probe is blind: {few} allocations for a read of 32 samples");
    assert!(many_bytes > few_bytes * 4.0, "the probe cannot see the cell count: {few_bytes} -> {many_bytes}");
    assert!(
        (many - few).abs() < 4.0,
        "the allocations of a read moved with the cell count ({few} at 50, {many} at 800): a per-cell allocation is back"
    );
}

/// A rollup decodes every sample it rolls up, folds them bucket by bucket and writes the rolled
/// samples back, so what it allocates follows the SAMPLES and buckets and not the cells in
/// them. The plain form spent two allocations on every stored cell at the decode alone. 100
/// samples roll into ten buckets here, at 50 and at 400 cells a sample (a per-cell allocation
/// would add 70,000). The CONTROL is the same two rollups in bytes, which must grow with the cell
/// count: a count that is flat because the probe cannot see the cell count is not a pass.
#[test]
#[ignore = "measurement: run with --features alloc-probe --ignored"]
fn a_rollup_through_the_engine_does_not_allocate_per_cell() {
    let (few, few_bytes) = rollup_once(100, 50);
    let (many, many_bytes) = rollup_once(100, 400);
    eprintln!("ROLLUP_FLAT 50 cells: {few} allocs {few_bytes} B; 400 cells: {many} allocs {many_bytes} B");
    assert!(few > 300.0, "the probe is blind: {few} allocations for a rollup of 100 samples");
    assert!(many_bytes > few_bytes * 4.0, "the probe cannot see the cell count: {few_bytes} -> {many_bytes}");
    assert!(
        (many - few).abs() < 100.0,
        "the allocations of a rollup moved with the cell count ({few} at 50, {many} at 400): a per-cell allocation is back"
    );
}

/// Allocations of each step a merge-reduce plan takes, measured on the pure pieces the planner
/// calls, for a typical shape (one dimension, one label, two lanes) and 64 cells. The planner runs
/// twice per write (validation, then execution), so each line is paid twice. A table for choosing
/// what to remove: the whole write is `the_window_reduce_allocation_tables`.
#[test]
#[ignore = "measurement: run with --features alloc-probe --ignored"]
fn the_merge_plan_allocations_by_step() {
    use crate::engine::cell_set::{
        check_header, decode_key, decode_request, merge_point, split_value, split_value_raw, Existing, FlatCells,
    };
    use std::rc::Rc;
    let h = header();
    let key = stored_key(1);
    let cells64 = cells(64, 1);
    let value = encode_value(&h, &cells64).unwrap();
    let request = encode_value(&h, &cells(64, 2)).unwrap();
    let (head_bytes, packed) = split_value_raw(&value).unwrap();
    let head_bytes = head_bytes.to_vec();
    let packed = packed.to_vec();
    let newest = Rc::new(h.clone());
    let mut rows: Vec<(&'static str, f64)> = Vec::new();
    fn step(rows: &mut Vec<(&'static str, f64)>, name: &'static str, mut op: impl FnMut()) {
        op();
        let probe = Probe::start();
        for _ in 0..20 {
            op();
        }
        rows.push((name, probe.stop().allocs as f64 / 20.0));
    }
    step(&mut rows, "split_value_raw", || drop(split_value_raw(&value).unwrap()));
    step(&mut rows, "CellHeader::decode", || drop(CellHeader::decode(&head_bytes).unwrap()));
    step(&mut rows, "split_value (header decode)", || drop(split_value(&value).unwrap()));
    step(&mut rows, "CellHeader::clone", || drop(h.clone()));
    step(&mut rows, "CellHeader::encode", || drop(h.encode().unwrap()));
    step(&mut rows, "check_header equal", || drop(check_header(&h, &h).unwrap()));
    step(&mut rows, "decode_key", || drop(decode_key(&h.key, &key).unwrap()));
    step(&mut rows, "FlatCells::decode stored", || drop(FlatCells::decode(&h, &packed, 64).unwrap()));
    step(&mut rows, "decode_request (header + cells)", || drop(decode_request(&request).unwrap()));
    step(&mut rows, "merge_point stored (whole)", || {
        let existing = Existing::Stored { header: &h, sample: &packed };
        drop(merge_point(Some(&newest), existing, &request).unwrap())
    });
    let merged = {
        let existing = Existing::Stored { header: &h, sample: &packed };
        merge_point(Some(&newest), existing, &request).unwrap().1
    };
    step(&mut rows, "FlatCells::encode_value", || drop(merged.encode_value(&h).unwrap()));
    for (name, allocs) in rows {
        eprintln!("PLANSTEP {name:<36} allocs={allocs:.1}");
    }
}

/// 400 merge-reduce writes of 64 cells into a sample of the same members, for a heap profiler
/// to attribute the allocations of one write to the code that makes them. It asserts nothing.
#[test]
#[ignore = "profiling: run under a heap profiler"]
fn a_merge_write_loop_for_a_heap_profiler() {
    let h = header();
    let key = stored_key(1);
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let members = cells(64, 1);
    let requests: Vec<Vec<u8>> = (0..401).map(|_| encode_value(&h, &members).unwrap()).collect();
    for value in requests {
        let response = exec(
            &engine,
            Command::FeatureAppendWithPolicy {
                key: key.clone(),
                points: vec![FeaturePoint { timestamp_ms: 100, value }],
                policy: FeatureWritePolicy::MergeReduce,
                retry_token: None,
            },
        );
        assert!(response.status.ok, "{:?}", response.status);
    }
}

/// A request value whose header bytes are not the canonical spelling (the version field moved
/// last) and decode to the very same header.
fn respelled_request(h: &CellHeader, members: &[Cell]) -> Vec<u8> {
    use crate::engine::cell_set::split_value_raw;
    let canonical = encode_value(h, members).unwrap();
    let (head, packed) = split_value_raw(&canonical).unwrap();
    let mut head = head.to_vec();
    // The first field is the version, a tag and a one-byte varint.
    head.rotate_left(2);
    let mut out = Vec::new();
    crate::types::encode_bytes_field(&mut out, 1, &head);
    crate::types::encode_bytes_field(&mut out, 2, packed);
    out
}

/// Allocations of one merge write (64 cells, the same members each time) and the sample left
/// stored, when the requests carry the canonical header bytes or a respelling of them.
fn merge_writes(respelled: bool) -> (f64, Vec<u8>) {
    const OPS: usize = 20;
    let h = header();
    let key = stored_key(1);
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let members = cells(64, 1);
    let make = || if respelled { respelled_request(&h, &members) } else { encode_value(&h, &members).unwrap() };
    let write = |value: Vec<u8>| {
        let response = exec(
            &engine,
            Command::FeatureAppendWithPolicy {
                key: key.clone(),
                points: vec![FeaturePoint { timestamp_ms: 100, value }],
                policy: FeatureWritePolicy::MergeReduce,
                retry_token: None,
            },
        );
        assert!(response.status.ok, "{:?}", response.status);
    };
    // The first write creates the key from a canonical request, so the stored header is canonical.
    write(encode_value(&h, &members).unwrap());
    write(make());
    let requests: Vec<Vec<u8>> = (0..OPS).map(|_| make()).collect();
    let probe = Probe::start();
    for value in requests {
        write(value);
    }
    let spent = probe.stop().allocs as f64 / OPS as f64;
    let stored = match exec(
        &engine,
        Command::FeatureQuery { key: key.clone(), start_ms: 0, end_ms: u64::MAX, count: Some(10) },
    )
    .response
    {
        CommandResponse::FeaturePoints { points } => {
            assert_eq!(1, points.len());
            points[0].value.clone()
        }
        other => panic!("not points: {other:?}"),
    };
    (spent, stored)
}

/// A merge-reduce request whose header bytes are the bytes already stored is not decoded and
/// compared again: it declares the stored header exactly. A request that spells the same header
/// differently takes the full path, which decodes it and compares it, and costs more allocations
/// for a header of this shape (every name in it is a string). Both leave the same stored sample,
/// to the byte: that is the oracle that the shortcut changes nothing but the cost.
///
/// There is no absolute ceiling: the guard is the difference between the two, which only the
/// shortcut can make positive (a plant that turns it off makes the two equal).
#[test]
#[ignore = "measurement: run with --features alloc-probe --ignored"]
fn a_merge_request_that_carries_the_stored_header_bytes_is_not_decoded_again() {
    let h = header();
    let members = cells(64, 1);
    assert_ne!(
        encode_value(&h, &members).unwrap(),
        respelled_request(&h, &members),
        "CONTROL: the respelled request must differ in bytes from the canonical one"
    );
    let (fast, fast_stored) = merge_writes(false);
    let (slow, slow_stored) = merge_writes(true);
    eprintln!("HEADER_SHORTCUT same bytes: {fast} allocs; respelled header: {slow} allocs");
    assert_eq!(fast_stored, slow_stored, "the two spellings stored different samples");
    assert!(fast > 20.0, "the probe is blind: {fast} allocations for a merge write");
    assert!(
        slow - fast >= 12.0,
        "a request carrying the stored header bytes ({fast}) costs about as much as one that does not ({slow}): the shortcut is gone"
    );
}
