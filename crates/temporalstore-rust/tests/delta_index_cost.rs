// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A/B micro-measurement of the per-write served-index cost.
//!
//! Reads the mode from the process env (`MATRIXARK_DELTA_SERVED_INDEX`) rather than setting
//! it, so the harness can run it twice (once OFF, once ON) as two separate processes with no
//! env race. It writes an increasing number of keys and times fixed-size write batches at a
//! growing store size, then reports the per-write wall time. On the default (OFF) path each
//! write re-serializes and atomically rewrites the whole `shard-{id}.index.json`, so the
//! per-write time climbs with the store size (O(store)); on the delta (ON) path the base
//! rewrite is deferred, so it stays flat (O(delta)). Run with `--nocapture` to see the report.

use std::path::PathBuf;
use std::time::Instant;

use temporalstore_rust::{Command, ExecuteRequest, TemporalEngine};

const SHARD_ID: u64 = 1;

#[test]
fn per_write_served_index_cost_report() {
    let mode = if matches!(
        std::env::var("MATRIXARK_DELTA_SERVED_INDEX")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    ) {
        "DELTA(on)"
    } else {
        "BASELINE(off)"
    };

    let mut root = std::env::temp_dir();
    root.push(format!("ts-delta-cost-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let index_dir = root.join("indexes");
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine =
        TemporalEngine::with_local_dirs(4096, root.join("cache"), root.join("pages"), &index_dir);
    engine.load_shard(SHARD_ID);

    let batch = 100usize;
    let checkpoints = [0usize, 500, 1000, 2000, 3000];
    let mut next = 0usize;
    println!("\n=== per-write served-index cost [{mode}] ===");
    for &target in &checkpoints {
        // Grow the store up to `target` entries.
        while next < target {
            let key = format!("key-{next:08}");
            let response = engine.execute(ExecuteRequest {
                shard_id: SHARD_ID,
                command: Command::StringSet {
                    key,
                    value: b"payload-value".to_vec(),
                },
            });
            assert!(response.status.ok);
            next += 1;
        }
        // Time a fixed batch of writes at this store size.
        let start = Instant::now();
        for i in 0..batch {
            let key = format!("probe-{target}-{i:04}");
            let response = engine.execute(ExecuteRequest {
                shard_id: SHARD_ID,
                command: Command::StringSet {
                    key,
                    value: b"payload-value".to_vec(),
                },
            });
            assert!(response.status.ok);
        }
        next += batch;
        let per_write_us = start.elapsed().as_secs_f64() * 1e6 / batch as f64;
        let served_len = engine.export_index_bytes(SHARD_ID).map(|b| b.len()).unwrap_or(0);
        println!(
            "store~{target:>6} entries | per-write {per_write_us:>8.1} us | whole-index size {served_len:>9} bytes",
        );
    }
    println!("=== end [{mode}] ===\n");
    let _ = std::fs::remove_dir_all(&root);
}
