// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE OPERATION OF THE WINDOW REDUCER WRITES AND READS ON DISK, THROUGH THE REAL `execute`
//! PATH, AT THREE SCALES ON EACH AXIS.
//!
//! Durable bytes written are summed over the three stores a write lands in -- the write-ahead log,
//! the index log and the block store -- from their own counters, and beside them the growth of the
//! whole data directory (the sum of file lengths). Bytes read come from the same counters. The
//! logical payload of a write is the length of the request value it carried. The axes are swept
//! one at a time: cells in a request, samples a key retains (and a key at its retention cap), keys
//! in the shard, and the tokened-write ring. A figure that grows with an axis is a cost that is not
//! flat in scale.
//!
//!   cargo test --lib the_window_reduce_amplification_table -- --ignored --nocapture --test-threads=1
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;
use crate::control::{Config, SetConfigRequest};
use crate::engine::cell_set::{
    encode_key, encode_value, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType, EntityValue,
    KeySpec, LaneReducer, LaneSpec, Member, HEADER_VERSION,
};
use crate::types::{
    CellMemberFilter, CellMemberId, FeatureWritePolicy, WindowReduceRankSpec, WindowReduceRemoveSpec,
    WindowReduceRollupSpec, WindowReduceScore, WindowReduceWindow,
};

fn open(dir: &std::path::Path, cap: Option<usize>) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    if let Some(cap) = cap {
        assert!(
            engine
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config { version: 2, feature_max_size: cap, ..Default::default() },
                })
                .ok,
            "the retention cap has to be the one under test"
        );
    }
    engine
}

fn exec(engine: &TemporalEngine, command: Command) -> ExecuteResponse {
    engine.execute(ExecuteRequest { shard_id: 1, command })
}

fn key_spec() -> KeySpec {
    KeySpec {
        namespace: "ns".to_string(),
        entity: EntityType::U64,
        dimensions: vec![DimSpec { name: "channel".to_string(), ty: DimType::U16, required: true }],
    }
}

fn stored_key(entity: u64) -> String {
    encode_key(&key_spec(), &CellKey { entity: EntityValue::U64(entity), dims: vec![Some(DimValue::U16(3))] }).unwrap()
}

fn header() -> CellHeader {
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

fn cells(count: usize, salt: u64) -> Vec<Cell> {
    (0..count as u64)
        .map(|m| Cell {
            member: Member::U64(m),
            labels: vec![m % 7],
            lanes: vec![(m + salt) as i64 % 13 + 1, (m * salt) as i64 % 29 + 1],
        })
        .collect()
}

fn value_of(count: usize, salt: u64) -> Vec<u8> {
    encode_value(&header(), &cells(count, salt)).unwrap()
}

/// Sum of every file length under `root`.
fn tree_bytes(root: &std::path::Path) -> u64 {
    fn walk(path: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(path) else { return };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                walk(&entry.path(), total);
            } else {
                *total += meta.len();
            }
        }
    }
    let mut total = 0;
    walk(root, &mut total);
    total
}

#[derive(Clone, Copy, Default)]
struct Bytes {
    wal_w: u64,
    idx_w: u64,
    blk_w: u64,
    wal_r: u64,
    idx_r: u64,
    blk_r: u64,
    tree: u64,
}

impl Bytes {
    fn now(engine: &TemporalEngine, dir: &std::path::Path) -> Bytes {
        let wal = engine.write_ahead_log_store().stats(1);
        let idx = engine.index_log_store().stats(1);
        let blk = engine.block_store().stats();
        Bytes {
            wal_w: wal.bytes_written,
            idx_w: idx.bytes_written,
            blk_w: blk.bytes_written,
            wal_r: wal.bytes_read,
            idx_r: idx.bytes_read,
            blk_r: blk.bytes_read,
            tree: tree_bytes(dir),
        }
    }
    fn since(self, before: Bytes) -> Bytes {
        Bytes {
            wal_w: self.wal_w.saturating_sub(before.wal_w),
            idx_w: self.idx_w.saturating_sub(before.idx_w),
            blk_w: self.blk_w.saturating_sub(before.blk_w),
            wal_r: self.wal_r.saturating_sub(before.wal_r),
            idx_r: self.idx_r.saturating_sub(before.idx_r),
            blk_r: self.blk_r.saturating_sub(before.blk_r),
            tree: self.tree.saturating_sub(before.tree),
        }
    }
    fn written(self) -> u64 {
        self.wal_w + self.idx_w + self.blk_w
    }
    fn read(self) -> u64 {
        self.wal_r + self.idx_r + self.blk_r
    }
}

/// One line of the table: per operation, the bytes written by each store, their sum, the growth of
/// the directory, the logical payload, and the ratio of the sum to the payload.
fn write_row(name: &str, params: &str, ops: usize, logical: u64, d: Bytes) {
    let per = |x: u64| x as f64 / ops as f64;
    let logical = per(logical).max(1.0);
    eprintln!(
        "AMP {name:<20} {params:<30} payload={:>8.0} wal={:>8.0} index={:>8.0} block={:>8.0} written={:>8.0} dir={:>8.0} ratio={:>7.2}",
        logical,
        per(d.wal_w),
        per(d.idx_w),
        per(d.blk_w),
        per(d.written()),
        per(d.tree),
        per(d.written()) / logical
    );
}

fn read_row(name: &str, params: &str, d: Bytes) {
    eprintln!(
        "AMP {name:<20} {params:<30} read={:>9} (wal={} index={} block={}) written={}",
        d.read(),
        d.wal_r,
        d.idx_r,
        d.blk_r,
        d.written()
    );
}

fn merge(engine: &TemporalEngine, key: &str, timestamp_ms: u64, value: Vec<u8>, token: Option<u64>) {
    let response = exec(
        engine,
        Command::FeatureAppendWithPolicy {
            key: key.to_string(),
            points: vec![FeaturePoint { timestamp_ms, value }],
            policy: FeatureWritePolicy::MergeReduce,
            retry_token: token,
        },
    );
    assert!(response.status.ok, "{:?}", response.status);
}

/// `samples` samples of `cells_per` cells at stamps 100, 200, ... in one append.
fn seed_samples(engine: &TemporalEngine, key: &str, samples: usize, cells_per: usize) {
    let points: Vec<FeaturePoint> = (0..samples as u64)
        .map(|i| FeaturePoint { timestamp_ms: 100 * (i + 1), value: value_of(cells_per, i + 1) })
        .collect();
    let response = exec(engine, Command::FeatureAppend { key: key.to_string(), points });
    assert!(response.status.ok, "{:?}", response.status);
}

fn rank_spec() -> WindowReduceRankSpec {
    WindowReduceRankSpec::new(
        WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX },
        WindowReduceScore::Lane { lane: "shown".to_string() },
        10,
    )
}

/// Cells in one request: 10 merge writes into one existing sample.
fn cells_axis() {
    for cell_count in [1usize, 64, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), None);
        let key = stored_key(1);
        merge(&engine, &key, 100, value_of(cell_count, 1), None);
        let requests: Vec<Vec<u8>> = (0..10u64).map(|i| value_of(cell_count, 2 + i)).collect();
        let logical: u64 = requests.iter().map(|v| v.len() as u64).sum();
        let before = Bytes::now(&engine, dir.path());
        for value in requests {
            merge(&engine, &key, 100, value, None);
        }
        write_row("merge_cells", &format!("cells={cell_count}"), 10, logical, Bytes::now(&engine, dir.path()).since(before));
    }
}

/// Samples a key retains: 10 merges into the newest sample and 10 into new samples, 64 cells each.
fn samples_axis() {
    for samples in [1usize, 64, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), None);
        let key = stored_key(1);
        seed_samples(&engine, &key, samples, 4);
        let newest = 100 * samples as u64;
        let requests: Vec<Vec<u8>> = (0..10u64).map(|i| value_of(64, 50 + i)).collect();
        let logical: u64 = requests.iter().map(|v| v.len() as u64).sum();
        let before = Bytes::now(&engine, dir.path());
        for value in requests.clone() {
            merge(&engine, &key, newest, value, None);
        }
        write_row("merge_newest", &format!("samples={samples}"), 10, logical, Bytes::now(&engine, dir.path()).since(before));
        let before = Bytes::now(&engine, dir.path());
        for (i, value) in requests.into_iter().enumerate() {
            merge(&engine, &key, newest + 100 * (i as u64 + 1), value, None);
        }
        write_row("merge_new_sample", &format!("samples={samples}"), 10, logical, Bytes::now(&engine, dir.path()).since(before));
    }
}

/// A key at its retention cap: every write trims one sample. 40 writes past the cap.
fn cap_axis() {
    for cap in [16usize, 64, 256, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), Some(cap));
        let key = stored_key(1);
        seed_samples(&engine, &key, cap, 4);
        let requests: Vec<Vec<u8>> = (0..40u64).map(|i| value_of(64, 50 + i)).collect();
        let logical: u64 = requests.iter().map(|v| v.len() as u64).sum();
        let before = Bytes::now(&engine, dir.path());
        for (i, value) in requests.into_iter().enumerate() {
            merge(&engine, &key, 100 * (cap as u64 + 1 + i as u64), value, None);
        }
        write_row("merge_at_cap", &format!("cap={cap}"), 40, logical, Bytes::now(&engine, dir.path()).since(before));
    }
}

/// Keys in the shard: each holds one sample; 10 merge writes into existing keys.
fn keys_axis() {
    for keys in [1usize, 100, 1000] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), None);
        for k in 0..keys as u64 {
            merge(&engine, &stored_key(k + 1), 100, value_of(64, 1), None);
        }
        let requests: Vec<Vec<u8>> = (0..10u64).map(|i| value_of(64, 2 + i)).collect();
        let logical: u64 = requests.iter().map(|v| v.len() as u64).sum();
        let before = Bytes::now(&engine, dir.path());
        for (i, value) in requests.into_iter().enumerate() {
            merge(&engine, &stored_key((i % keys) as u64 + 1), 100, value, None);
        }
        write_row("merge_keys", &format!("keys={keys}"), 10, logical, Bytes::now(&engine, dir.path()).since(before));
    }
}

/// The tokened-write ring fills to 16 entries: one write at a time, the bytes of the n-th.
fn ring_axis() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), None);
    let key = stored_key(1);
    merge(&engine, &key, 100, value_of(64, 1), Some(1_000));
    for n in 1..=40u64 {
        let value = value_of(64, 1 + n);
        let logical = value.len() as u64;
        let before = Bytes::now(&engine, dir.path());
        merge(&engine, &key, 100, value, Some(n));
        if [1, 2, 4, 8, 16, 17, 32, 40].contains(&n) {
            write_row("merge_tokened", &format!("write={n}"), 1, logical, Bytes::now(&engine, dir.path()).since(before));
        }
    }
}

/// Reads and maintenance writes over a key of 64 samples of 64 cells, on a cold engine.
fn read_and_maintenance_axis() {
    for samples in [16usize, 64, 256] {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = open(dir.path(), None);
            for k in 0..8u64 {
                seed_samples(&engine, &stored_key(k + 1), samples, 64);
            }
        }
        // Reopened: nothing is cached and the store is read as a restart reads it.
        let engine = open(dir.path(), None);
        let recovery = Bytes::now(&engine, dir.path());
        read_row("recovery", &format!("samples={samples} keys=8"), recovery);
        let key = stored_key(1);
        let before = Bytes::now(&engine, dir.path());
        let ranked = exec(&engine, Command::WindowReduceRank { key: key.clone(), spec: rank_spec() });
        assert!(ranked.status.ok);
        read_row("rank_cold", &format!("samples={samples} cells=64"), Bytes::now(&engine, dir.path()).since(before));
        let before = Bytes::now(&engine, dir.path());
        let ranked = exec(&engine, Command::WindowReduceRank { key: key.clone(), spec: rank_spec() });
        assert!(ranked.status.ok);
        read_row("rank_warm", &format!("samples={samples} cells=64"), Bytes::now(&engine, dir.path()).since(before));
        let keys: Vec<String> = (0..8u64).map(|k| stored_key(k + 1)).collect();
        let before = Bytes::now(&engine, dir.path());
        let fan = exec(&engine, Command::WindowReduceFanInRank { keys, spec: rank_spec() });
        assert!(fan.status.ok);
        read_row("fan_in_8_keys", &format!("samples={samples} cells=64"), Bytes::now(&engine, dir.path()).since(before));

        // Maintenance writes on key 2: a removal of half the cells of every sample, then a rollup.
        let key2 = stored_key(2);
        let before = Bytes::now(&engine, dir.path());
        let removed = exec(
            &engine,
            Command::WindowReduceRemove {
                key: key2.clone(),
                spec: WindowReduceRemoveSpec {
                    start_ms: None,
                    end_ms: None,
                    members: CellMemberFilter {
                        include: Some((0..32u64).map(CellMemberId::U64).collect()),
                        exclude: Vec::new(),
                    },
                    filters: Vec::new(),
                },
            },
        );
        assert!(removed.status.ok, "{:?}", removed.status);
        read_row("remove_half_cells", &format!("samples={samples} cells=64"), Bytes::now(&engine, dir.path()).since(before));
        let span = 100 * (samples as u64 + 1);
        let key3 = stored_key(3);
        let before = Bytes::now(&engine, dir.path());
        let rolled = exec(
            &engine,
            Command::WindowReduceRollup {
                key: key3,
                spec: WindowReduceRollupSpec {
                    older_than_ms: span,
                    bucket_ms: span / 8,
                    cap: None,
                    anchor: Default::default(),
                    coarser_tiers: Vec::new(),
                    truncate: None,
                },
            },
        );
        assert!(rolled.status.ok, "{:?}", rolled.status);
        read_row("rollup_8_buckets", &format!("samples={samples} cells=64"), Bytes::now(&engine, dir.path()).since(before));
    }
}

#[test]
#[ignore = "measurement: run with --ignored --nocapture"]
fn the_window_reduce_amplification_table() {
    cells_axis();
    samples_axis();
    cap_axis();
    keys_axis();
    ring_axis();
    read_and_maintenance_axis();
}

/// Allocations spent by `op`, or 0 when the counting allocator is not installed (the column is
/// then printed as `n/a` by the caller, which asks `allocations_counted()`).
#[cfg(feature = "alloc-probe")]
fn allocations_of(op: impl FnOnce()) -> u64 {
    let probe = crate::alloc_probe::Probe::start();
    op();
    probe.stop().allocs
}

#[cfg(not(feature = "alloc-probe"))]
fn allocations_of(op: impl FnOnce()) -> u64 {
    op();
    0
}

fn allocations_counted() -> bool {
    cfg!(feature = "alloc-probe")
}

/// A sample of `total` cells, built by merge writes of at most 1,024 cells each (the most a
/// request may carry), over disjoint members.
fn grow_sample(engine: &TemporalEngine, key: &str, timestamp_ms: u64, total: usize) {
    let mut start = 0usize;
    while start < total {
        let count = (total - start).min(1024);
        let members: Vec<Cell> = (start as u64..(start + count) as u64)
            .map(|m| Cell { member: Member::U64(m), labels: vec![m % 7], lanes: vec![(m % 13) as i64 + 1, (m % 29) as i64 + 1] })
            .collect();
        merge(engine, key, timestamp_ms, encode_value(&header(), &members).unwrap(), None);
        start += count;
    }
}

/// One cell merged into an existing sample of 64, 1,024 and 4,096 cells: the durable bytes a write
/// of one cell costs, and its allocations, as the sample it lands in grows.
fn one_cell_axis() {
    for cell_count in [64usize, 1024, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), None);
        let key = stored_key(1);
        grow_sample(&engine, &key, 100, cell_count);
        let request = |salt: u64| value_of(1, salt);
        let one = request(2).len() as u64;
        // Ten writes of one cell each, the request encoded before the measurement starts.
        let requests: Vec<Vec<u8>> = (0..10u64).map(|i| request(2 + i)).collect();
        let before = Bytes::now(&engine, dir.path());
        let allocs = allocations_of(|| {
            for value in requests {
                merge(&engine, &key, 100, value, None);
            }
        });
        let d = Bytes::now(&engine, dir.path()).since(before);
        let per = |x: u64| x as f64 / 10.0;
        let allocs = if allocations_counted() { format!("{:.1}", allocs as f64 / 10.0) } else { "n/a".to_string() };
        eprintln!(
            "AMP one_cell_write       sample_cells={cell_count:<5} payload={:>5.0} wal={:>7.0} index={:>6.0} block={:>7.0} written={:>7.0} allocs={allocs}",
            per(one),
            per(d.wal_w),
            per(d.idx_w),
            per(d.blk_w),
            per(d.written()),
        );
    }
}

/// A warm ranked read over 16, 64 and 256 samples of 64 cells: its allocations.
fn rank_allocations_axis() {
    for samples in [16usize, 64, 256] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), None);
        let key = stored_key(1);
        seed_samples(&engine, &key, samples, 64);
        let read = || {
            let response = exec(&engine, Command::WindowReduceRank { key: key.clone(), spec: rank_spec() });
            assert!(response.status.ok);
        };
        read();
        let allocs = allocations_of(|| {
            for _ in 0..5 {
                read();
            }
        });
        let allocs = if allocations_counted() { format!("{:.1}", allocs as f64 / 5.0) } else { "n/a".to_string() };
        eprintln!("AMP rank_allocations     samples={samples:<4} cells=64    allocs={allocs}");
    }
}

#[test]
#[ignore = "measurement: run with --ignored --nocapture (add --features alloc-probe for the allocation columns)"]
fn the_window_reduce_one_cell_write_table() {
    one_cell_axis();
    rank_allocations_axis();
}

// ------------------------------------------------------------------------------------------
// Latency
// ------------------------------------------------------------------------------------------

/// Microseconds of each of `n` runs of `op`, after two untimed runs.
fn micros(n: usize, mut op: impl FnMut(usize)) -> Vec<f64> {
    op(usize::MAX - 1);
    op(usize::MAX);
    (0..n)
        .map(|i| {
            let started = std::time::Instant::now();
            op(i);
            started.elapsed().as_nanos() as f64 / 1_000.0
        })
        .collect()
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let at = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[at.min(sorted.len() - 1)]
}

fn latency_row(name: &str, params: &str, mut runs: Vec<f64>) {
    runs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    eprintln!(
        "LAT {name:<18} {params:<34} n={:<4} p50={:>10.1}us p99={:>10.1}us",
        runs.len(),
        percentile(&runs, 0.5),
        percentile(&runs, 0.99)
    );
}

/// `retained` samples under one key: the newest, at `100 * retained`, holds `cells` cells; the
/// older ones hold 4 each.
fn key_with_samples(engine: &TemporalEngine, key: &str, retained: usize, cells: usize) {
    if retained > 1 {
        seed_samples(engine, key, retained - 1, 4);
    }
    grow_sample(engine, key, 100 * retained as u64, cells);
}

fn latency_merge() {
    for retained in [1usize, 64, 256] {
        for cells in [64usize, 1024, 4096] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path(), None);
            let key = stored_key(1);
            key_with_samples(&engine, &key, retained, cells);
            let newest = 100 * retained as u64;
            let requests: Vec<Vec<u8>> = (0..202u64).map(|i| value_of(1, 2 + i)).collect();
            let runs = micros(200, |i| {
                merge(&engine, &key, newest, requests[i.min(201)].clone(), None);
            });
            latency_row("merge_1_cell", &format!("retained={retained} sample_cells={cells}"), runs);
        }
    }
    for cells in [64usize, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), None);
        let key = stored_key(1);
        grow_sample(&engine, &key, 100, cells);
        let requests: Vec<Vec<u8>> = (0..102u64).map(|i| value_of(cells, 2 + i)).collect();
        let runs = micros(100, |i| merge(&engine, &key, 100, requests[i.min(101)].clone(), None));
        latency_row("merge_all_cells", &format!("retained=1 sample_cells={cells}"), runs);
    }
}

fn latency_reads() {
    for cells in [64usize, 1024, 4096] {
        for samples in [16usize, 64, 256] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path(), None);
            let key = stored_key(1);
            seed_samples(&engine, &key, samples, cells);
            let n = if cells * samples > 200_000 { 20 } else { 100 };
            let runs = micros(n, |_| {
                let response = exec(&engine, Command::WindowReduceRank { key: key.clone(), spec: rank_spec() });
                assert!(response.status.ok);
            });
            latency_row("rank_read", &format!("samples={samples} cells={cells}"), runs);
        }
    }
    for cells in [64usize, 1024] {
        for samples in [16usize, 64] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path(), None);
            let keys: Vec<String> = (0..8u64).map(|k| stored_key(k + 1)).collect();
            for key in &keys {
                seed_samples(&engine, key, samples, cells);
            }
            let runs = micros(50, |_| {
                let response = exec(&engine, Command::WindowReduceFanInRank { keys: keys.clone(), spec: rank_spec() });
                assert!(response.status.ok);
            });
            latency_row("fan_in_8_keys", &format!("samples={samples} cells={cells}"), runs);
            let queries: Vec<crate::types::WindowReduceRankBatchItem> = keys
                .iter()
                .map(|key| crate::types::WindowReduceRankBatchItem { key: key.clone(), spec: rank_spec() })
                .collect();
            let runs = micros(50, |_| {
                let response = exec(&engine, Command::WindowReduceRankBatch { queries: queries.clone() });
                assert!(response.status.ok);
            });
            latency_row("batch_8_reads", &format!("samples={samples} cells={cells}"), runs);
        }
    }
}

/// Maintenance and lifecycle: each run is on a key of its own, so none runs on a key a previous
/// run already changed.
fn latency_maintenance() {
    const KEYS: usize = 20;
    for (samples, cells) in [(64usize, 64usize), (64, 1024), (256, 64)] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), None);
        for k in 0..(3 * KEYS + 6) as u64 {
            seed_samples(&engine, &stored_key(k + 1), samples, cells);
        }
        let span = 100 * (samples as u64 + 1);
        let runs = micros(KEYS, |i| {
            let key = stored_key(i.min(KEYS + 1) as u64 + 1);
            let response = exec(
                &engine,
                Command::WindowReduceRollup {
                    key,
                    spec: WindowReduceRollupSpec {
                        older_than_ms: span,
                        bucket_ms: span / 8,
                        cap: None,
                        anchor: Default::default(),
                        coarser_tiers: Vec::new(),
                        truncate: None,
                    },
                },
            );
            assert!(response.status.ok, "{:?}", response.status);
        });
        latency_row("rollup_8_buckets", &format!("samples={samples} cells={cells}"), runs);
        let runs = micros(KEYS, |i| {
            let key = stored_key((KEYS + 2 + i.min(KEYS + 1)) as u64 + 1);
            let response = exec(
                &engine,
                Command::WindowReduceRemove {
                    key,
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
            );
            assert!(response.status.ok, "{:?}", response.status);
        });
        latency_row("remove_half_cells", &format!("samples={samples} cells={cells}"), runs);
        let runs = micros(KEYS, |i| {
            let key = stored_key((2 * KEYS + 4 + i.min(KEYS + 1)) as u64 + 1);
            let response = exec(&engine, Command::FeatureDelete { key });
            assert!(response.status.ok, "{:?}", response.status);
        });
        latency_row("delete_key", &format!("samples={samples} cells={cells}"), runs);
    }
    for (keys, samples, cells) in [(8usize, 64usize, 64usize), (8, 64, 1024)] {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = open(dir.path(), None);
            for k in 0..keys as u64 {
                seed_samples(&engine, &stored_key(k + 1), samples, cells);
            }
        }
        let runs = micros(5, |_| {
            let reopened = open(dir.path(), None);
            drop(reopened);
        });
        latency_row("load", &format!("keys={keys} samples={samples} cells={cells}"), runs);
    }
}

#[test]
#[ignore = "measurement: run in release with --ignored --nocapture"]
fn the_window_reduce_latency_table() {
    latency_merge();
    latency_reads();
    latency_maintenance();
}

// ------------------------------------------------------------------------------------------
// Throughput
// ------------------------------------------------------------------------------------------

/// Operations per second of `threads` threads each running `op(thread, iteration)` in a loop for
/// `seconds`, on the one engine. `op` returns when the operation has completed.
fn throughput(threads: usize, seconds: f64, op: &(dyn Fn(usize, usize) + Sync)) -> f64 {
    let stop = std::sync::atomic::AtomicBool::new(false);
    let done = std::sync::atomic::AtomicU64::new(0);
    let started = std::time::Instant::now();
    std::thread::scope(|scope| {
        for t in 0..threads {
            let (stop, done) = (&stop, &done);
            scope.spawn(move || {
                let mut i = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    op(t, i);
                    done.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    i += 1;
                }
            });
        }
        std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    done.load(std::sync::atomic::Ordering::Relaxed) as f64 / started.elapsed().as_secs_f64()
}

fn throughput_row(name: &str, params: &str, per_thread_keys: bool, rates: Vec<(usize, f64)>) {
    let cells: Vec<String> = rates.iter().map(|(c, r)| format!("c{c}={r:>9.0}/s")).collect();
    eprintln!(
        "QPS {name:<18} {params:<34} keys={} {}",
        if per_thread_keys { "per-thread" } else { "shared" },
        cells.join(" ")
    );
}

fn throughput_writes() {
    for (cells, retained) in [(64usize, 1usize), (1024, 1), (1024, 64)] {
        for per_thread_keys in [true, false] {
            let mut rates = Vec::new();
            for threads in [1usize, 4, 16] {
                let dir = tempfile::tempdir().unwrap();
                let engine = open(dir.path(), None);
                let keys: Vec<String> = (0..threads as u64).map(|t| stored_key(t + 1)).collect();
                for t in 0..threads {
                    let key = if per_thread_keys { &keys[t] } else { &keys[0] };
                    if per_thread_keys || t == 0 {
                        key_with_samples(&engine, key, retained, cells);
                    }
                }
                let newest = 100 * retained as u64;
                let value = value_of(1, 2);
                let rate = throughput(threads, 2.0, &|t, _| {
                    let key = if per_thread_keys { &keys[t] } else { &keys[0] };
                    merge(&engine, key, newest, value.clone(), None);
                });
                rates.push((threads, rate));
            }
            throughput_row("write_1_cell", &format!("retained={retained} sample_cells={cells}"), per_thread_keys, rates);
        }
    }
}

fn throughput_reads() {
    for (cells, samples) in [(64usize, 16usize), (1024, 16), (64, 256)] {
        for per_thread_keys in [true, false] {
            let mut rates = Vec::new();
            for threads in [1usize, 4, 16] {
                let dir = tempfile::tempdir().unwrap();
                let engine = open(dir.path(), None);
                let keys: Vec<String> = (0..threads as u64).map(|t| stored_key(t + 1)).collect();
                for t in 0..threads {
                    if per_thread_keys || t == 0 {
                        seed_samples(&engine, &keys[if per_thread_keys { t } else { 0 }], samples, cells);
                    }
                }
                let rate = throughput(threads, 2.0, &|t, _| {
                    let key = if per_thread_keys { &keys[t] } else { &keys[0] };
                    let response = exec(&engine, Command::WindowReduceRank { key: key.clone(), spec: rank_spec() });
                    assert!(response.status.ok);
                });
                rates.push((threads, rate));
            }
            throughput_row("rank_read", &format!("samples={samples} cells={cells}"), per_thread_keys, rates);
        }
    }
}

#[test]
#[ignore = "measurement: run in release with --ignored --nocapture"]
fn the_window_reduce_throughput_table() {
    throughput_writes();
    throughput_reads();
}
