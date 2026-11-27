// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SCALAR REPORTER'S SHARD GUARD VISITS WHAT IT REPORTS, NOT THE SHARD.
//!
//! The gauges a scrape or heartbeat reads (`shard_index_lags`, `shard_expiry_backlogs`,
//! `bucket_gauge_sweep`, `shard_stats`, `bucket_index_resident_bytes`) run under the engine-wide
//! shard-table guard, which stalls every writer for as long as it is held. Each now goes through
//! `with_shard` / `with_all_shards_read`, whose closure reports the entries it touched, and the
//! bound asserted here is on ENTRIES (flat at 1k, 8k and 64k keys), never on time.
//!
//! THE CONTROL. `bucket_gauge_sweep` with no cap is the same code with the cap removed; it must visit
//! at least one entry per bucket, which grows with the shard. A counter that reads flat on the
//! uncapped form too is not counting.
//!
//! LOCK ORDER. `shard_stats` used to ask the block store (its mutex) from INSIDE the shard guard.
//! The store reads count themselves when a shard guard is held on the thread; the count must be 0.
//!
//! rust-internal: measures this crate's own report paths, no product behaviour

#![allow(clippy::all)]

use super::*;

use crate::engine::shard_guard;
use std::sync::Arc;

const SCALES: [usize; 3] = [1_000, 8_000, 64_000];
const GAUGE_CAP: usize = 500;

fn engine_with(dir: &std::path::Path, keys: usize) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    for index in 0..keys {
        let outcome = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: format!("key-{index:07}"), value: vec![b'v'; 24] },
        });
        assert!(outcome.status.ok, "{:?}", outcome.response);
    }
    engine
}

#[test]
fn a_scalar_reporter_visits_at_most_its_cap_at_every_scale() {
    let mut rows: Vec<(usize, u64, u64, u64, u64, u64, u64)> = Vec::new();
    for keys in SCALES {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_with(dir.path(), keys);
        // Warm the cached resident-bytes total: a cold read is a one-time walk, and the shape asked
        // about here is the steady scrape.
        let _ = engine.bucket_index_resident_bytes(1);

        let _ = engine.shard_index_lags();
        let lags = shard_guard::last_hold_entries();
        let _ = engine.shard_expiry_backlogs();
        let backlogs = shard_guard::last_hold_entries();

        let sweep = engine.bucket_gauge_sweep(1, None, GAUGE_CAP);
        let gauge = shard_guard::last_hold_entries();
        let blocks_in_swept: u64 = sweep.gauges.iter().map(|g| g.block_count).sum();
        assert!(sweep.gauges.len() <= GAUGE_CAP, "{keys}: the sweep ignored its cap");
        assert_eq!(
            sweep.gauges.len() as u64 + blocks_in_swept,
            gauge,
            "{keys}: entries visited must be the buckets read plus the blocks summed"
        );

        shard_guard::reset_store_reads_under_guard();
        let stats = engine.shard_stats(1).expect("shard loaded");
        let stats_visited = shard_guard::last_hold_entries();
        assert_eq!(
            0,
            shard_guard::store_reads_under_guard(),
            "{keys}: shard_stats asked the block store from inside the shard guard"
        );
        assert!(stats.storage.bucket_index_resident_entries > 0);

        let _ = engine.bucket_index_resident_bytes(1);
        let resident = shard_guard::last_hold_entries();

        // THE CONTROL: the same sweep with the cap off.
        let uncapped = engine.bucket_gauge_sweep(1, None, usize::MAX);
        let uncapped_visited = shard_guard::last_hold_entries();
        assert!(
            uncapped_visited >= uncapped.buckets_total as u64,
            "{keys}: the uncapped control did not visit every bucket ({uncapped_visited} of {})",
            uncapped.buckets_total
        );

        rows.push((keys, lags, backlogs, gauge, stats_visited, resident, uncapped_visited));
    }
    for (keys, lags, backlogs, gauge, stats, resident, uncapped) in &rows {
        println!(
            "keys={keys:>6} lags={lags} backlogs={backlogs} gauge_sweep={gauge} shard_stats={stats} resident_bytes={resident} | control(uncapped sweep)={uncapped}"
        );
    }
    let first = rows[0];
    for row in &rows[1..] {
        assert_eq!(first.1, row.1, "shard_index_lags visits grew with the shard: {rows:?}");
        assert_eq!(first.2, row.2, "shard_expiry_backlogs visits grew with the shard: {rows:?}");
        assert_eq!(first.4, row.4, "shard_stats visits grew with the shard: {rows:?}");
        assert_eq!(first.5, row.5, "bucket_index_resident_bytes visits grew with the shard: {rows:?}");
        assert!(row.3 <= first.3 * 4 + 64, "gauge sweep visits grew with the shard: {rows:?}");
        assert!(row.6 > first.6, "the control did not grow with the shard: {rows:?}");
    }
    assert_eq!(1, first.1, "one shard loaded: lags visit one entry");
    assert_eq!(1, first.2, "one shard loaded: backlogs visit one entry");
}
