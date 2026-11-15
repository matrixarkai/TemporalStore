// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE OPERATION OF THE CELL MODEL ALLOCATES, COUNTED BY THE COUNTING ALLOCATOR, THROUGH THE
//! REAL `execute` PATH.
//!
//! Each table sweeps ONE dimension at a time (snapshots, cells per snapshot, keys) so a cost that
//! scales with a count shows as a slope. Counts are allocation BLOCKS first, bytes second. Run:
//!
//!   cargo test --features alloc-probe --lib the_cell_model_allocation_tables -- --ignored --nocapture --test-threads=1
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::alloc_probe::Probe;
use crate::engine::cell_set::{
    encode_key, encode_value, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType, EntityValue,
    KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{CellCapSpec, CellRankSpec, CellRollupSpec, CellScore, CellWindow, FeatureWritePolicy};

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

/// `snapshots` snapshots of `cell_count` cells each under `key`, stamped 100, 200, ...
pub(super) fn seed(engine: &TemporalEngine, key: &str, snapshots: usize, cell_count: usize) {
    let h = header();
    for i in 0..snapshots {
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

pub(super) fn spec() -> CellRankSpec {
    CellRankSpec::new(
        CellWindow::Between { start_ms: 0, end_ms: u64::MAX },
        CellScore::Lane { lane: "shown".to_string() },
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
    match exec(engine, Command::CellRankQuery { key: key.to_string(), spec: spec() }).response {
        CommandResponse::CellRanking { ranked } => {
            assert!(ranked.refused.is_empty(), "{:?}", ranked);
            ranked.rows
        }
        other => panic!("not a ranking: {other:?}"),
    }
}

pub(super) fn fan_in_read(engine: &TemporalEngine, keys: &[String]) -> Vec<crate::types::RankedCellRow> {
    match exec(engine, Command::CellFanInRankQuery { keys: keys.to_vec(), spec: spec() }).response {
        CommandResponse::CellRanking { ranked } => {
            assert!(ranked.refused.is_empty(), "{:?}", ranked);
            ranked.rows
        }
        other => panic!("not a ranking: {other:?}"),
    }
}

/// (allocations, bytes) of one rollup of `snapshots` snapshots of `cell_count` cells into ten buckets.
pub(super) fn rollup_once(snapshots: usize, cell_count: usize) -> (f64, f64) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let key = stored_key(1);
    seed(&engine, &key, snapshots, cell_count);
    let span = 100 * (snapshots as u64 + 1);
    let spec = CellRollupSpec {
        older_than_ms: span,
        bucket_ms: span / 10,
        cap: None::<CellCapSpec>,
        anchor: Default::default(),
        coarser_tiers: Vec::new(),
        truncate: None,
    };
    let probe = Probe::start();
    let response = exec(&engine, Command::CellRollup { key, spec });
    let counts = probe.stop();
    assert!(response.status.ok, "{:?}", response.status);
    (counts.allocs as f64, counts.alloc_bytes as f64)
}

/// (allocations, bytes) of one merge-reduce write of `cell_count` cells into a snapshot of the same members.
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
fn the_cell_model_allocation_tables() {
    for cell_count in [1usize, 64, 1024] {
        let (a, b) = merge_once(cell_count);
        eprintln!("CELLTABLE merge cells={cell_count} allocs={a:.1} bytes={b:.0}");
    }
    for (snapshots, cell_count) in [(1usize, 200usize), (16, 200), (256, 200), (16, 50), (16, 800)] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        let key = stored_key(1);
        seed(&engine, &key, snapshots, cell_count);
        let (a, b) = per_op(5, || {
            assert_eq!(10, ranked_read(&engine, &key).len());
        });
        eprintln!(
            "CELLTABLE rank snapshots={snapshots} cells={cell_count} allocs={a:.1} bytes={b:.0} allocs_per_cell_read={:.3}",
            a / (snapshots * cell_count) as f64
        );
    }
    for (keys, snapshots, cell_count) in [(8usize, 4usize, 200usize), (8, 16, 200), (8, 16, 50), (2, 16, 200)] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        let names: Vec<String> = (0..keys as u64).map(|e| stored_key(e + 1)).collect();
        for name in &names {
            seed(&engine, name, snapshots, cell_count);
        }
        let (a, b) = per_op(5, || {
            assert_eq!(10, fan_in_read(&engine, &names).len());
        });
        eprintln!(
            "CELLTABLE fan_in keys={keys} snapshots_per_key={snapshots} cells={cell_count} allocs={a:.1} bytes={b:.0} allocs_per_cell_read={:.3}",
            a / (keys * snapshots * cell_count) as f64
        );
    }
    for (snapshots, cell_count) in [(100usize, 50usize), (1000, 50), (1000, 200)] {
        let (a, b) = rollup_once(snapshots, cell_count);
        eprintln!(
            "CELLTABLE rollup snapshots={snapshots} cells={cell_count} allocs={a:.0} bytes={b:.0} allocs_per_cell={:.3}",
            a / (snapshots * cell_count) as f64
        );
    }
    {
        use crate::engine::cell_retry_ring::{RetryReceipt, RetryRing};
        let mut ring = RetryRing::new();
        let receipt = RetryReceipt { snapshots_written: 1, cells_created: 2, cells_merged: 3 };
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
