// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE INDEX'S AMPLIFICATION UNDER REWRITE, and whether its log reclaim bounds it.
//!
//! # EVERY INDEX FIGURE SO FAR WAS TAKEN WHERE RECLAIM COULD NOT FIRE
//!
//! The published set reports the index log's storage-write ratio and the index's disk space ratio
//! on a corpus where every key is written once. The index log's reclaim has a TWO-PART gate --
//! `DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD` is 768 KiB, and at least 40% of the log must be
//! reclaimable -- and that corpus produces on the order of 105 KB of log. An order of magnitude
//! short of the first condition, so reclaim could not have fired even if it had been asked, and
//! neither ratio could move.
//!
//! That is the same shape as the payload side, where a write-once corpus made both ratios 1.0x by
//! arithmetic rather than by merit. This rewrites until the log crosses the threshold, reports the
//! set after every round, then runs the cycle and reports what reclaim did.
//!
//! # THE ON-DISK NUMERATOR EXCLUDES THE WAL, DELIBERATELY
//!
//! The engine keeps `wals/` under the directory it is handed for indexes. Summing that directory
//! put 262,144 B of write-ahead log inside an index ratio once already -- 71% of the numerator --
//! so the walk here partitions the WAL out and reports it beside the index's own files rather than
//! folding it in.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_index_log_under_rewrite -- --ignored --nocapture

use crate::engine::reports::{
    StorageManagerCycleRequest, DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD,
};
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 2_000;
const VALUE: usize = 256;
/// Enough rounds to carry the log past 768 KiB: one pass over this corpus writes roughly 105 KB of
/// index log, so eight passes clear the threshold with margin.
const ROUNDS: usize = 8;

fn noisy_value(index: usize, round: usize, len: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add((round as u64).wrapping_mul(0x1234_5678_9ABC_DEF0))
        | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

/// The index's own bytes on disk, and the WAL's, kept apart.
fn index_and_wal_bytes(index_dir: &std::path::Path) -> (u64, u64) {
    fn walk(path: &std::path::Path, prefix: &str, out: &mut Vec<(String, u64)>) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let rel = if prefix.is_empty() { name } else { format!("{prefix}/{name}") };
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => walk(&entry.path(), &rel, out),
                Ok(_) => out.push((rel, entry.metadata().map(|m| m.len()).unwrap_or(0))),
                Err(_) => {}
            }
        }
    }
    let mut files = Vec::new();
    walk(index_dir, "", &mut files);
    let is_wal = |rel: &str| rel == "wals" || rel.starts_with("wals/");
    (
        files.iter().filter(|(r, _)| !is_wal(r)).map(|(_, l)| *l).sum(),
        files.iter().filter(|(r, _)| is_wal(r)).map(|(_, l)| *l).sum(),
    )
}

fn ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 { 0 } else { numerator.saturating_mul(1_000_000) / denominator }
}

fn as_x(value: u64) -> f64 {
    value as f64 / 1_000_000.0
}

#[test]
#[ignore = "rewrites 2,000 keys eight times to cross the log threshold; run by name"]
fn the_index_log_under_rewrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    let index_dir = dir.path().join("indexes");

    let snapshot = |label: String| -> (String, u64, u64, u64, u64) {
        let log = engine.index_log_store.stats(1);
        let image = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            crate::engine::encode_index_bytes(shard).len() as u64
        };
        let (index_bytes, wal_bytes) = index_and_wal_bytes(&index_dir);
        (label, log.bytes_written, image, index_bytes, wal_bytes)
    };

    let mut rows = Vec::new();
    for round in 0..ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("ilog-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
        rows.push(snapshot(format!("after {} pass(es)", round + 1)));
    }

    println!();
    println!(
        "  THE INDEX UNDER REWRITE -- {KEYS} keys x {VALUE} B, {ROUNDS} passes, incompressible"
    );
    println!("  the log reclaim gate: {} B and at least 40% reclaimable",
        DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD);
    println!();
    println!(
        "    {:<20} {:>11} {:>10} {:>12} {:>10} {:>8} {:>8}",
        "", "log written", "image", "index disk", "wal disk", "wr ratio", "sp ratio"
    );
    for (label, log_written, image, index_bytes, wal_bytes) in &rows {
        println!(
            "    {:<20} {:>11} {:>10} {:>12} {:>10} {:>7.3}x {:>7.3}x",
            label,
            log_written,
            image,
            index_bytes,
            wal_bytes,
            as_x(ppm(*log_written, *image)),
            as_x(ppm(*index_bytes, *image)),
        );
    }

    let (_, log_before, image_before, disk_before, _) = rows[rows.len() - 1].clone();
    let crossed = log_before >= DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD;
    println!();
    println!(
        "    log written {log_before} B vs the {} B threshold: {}",
        DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD,
        if crossed { "CROSSED" } else { "NOT crossed" }
    );

    // ---- THE CYCLE, with the index-gc stage on and the production threshold in effect.
    let buckets: Vec<u32> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.bucket_index.bucket_map.keys().copied().collect()
    };
    let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: false,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        selected_dump_buckets: buckets.clone(),
        max_dump_buckets_per_round: buckets.len(),
        min_undumped_wal_records: 1,
        min_undumped_wal_bytes: 1,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        index_gc_index_log_bytes_threshold: DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD,
        ..StorageManagerCycleRequest::default()
    });
    let removed: Vec<String> = report
        .stages
        .iter()
        .filter(|stage| stage.index_log_records_removed > 0)
        .map(|stage| format!("{}={}", stage.stage, stage.index_log_records_removed))
        .collect();
    let (_, log_after, image_after, disk_after, wal_after) = snapshot("after the cycle".into());

    println!();
    println!("  AFTER A CYCLE WITH INDEX GC ON");
    println!(
        "    log written {log_before} -> {log_after} B,  index on disk {disk_before} -> \
         {disk_after} B,  image {image_before} -> {image_after} B,  wal {wal_after} B"
    );
    println!(
        "    stages reporting removed index-log records: {}",
        if removed.is_empty() { "NONE".to_string() } else { removed.join(" ") }
    );
    println!(
        "    index space: {:.3}x -> {:.3}x",
        as_x(ppm(disk_before, image_before)),
        as_x(ppm(disk_after, image_after))
    );
    println!();

    // ---- NON-VACUITY. Every figure is a ratio over the image.
    for (label, log_written, image, index_bytes, _) in &rows {
        assert!(*image > 0, "{label}: the index encoded to nothing, so both ratios are vacuous");
        assert!(*log_written > 0, "{label}: no index log was written, so the fixture never ran");
        assert!(*index_bytes > 0, "{label}: the index occupies nothing on disk");
    }

    // ---- THE WRITE RATIO MUST CLIMB WITH REWRITE. This is the question the write-once corpus
    // ---- could not ask: the log accumulates a record per write, so eight passes over the same
    // ---- keys must cost more log than one.
    let (_, log_first, image_first, _, _) = rows[0].clone();
    assert!(
        ppm(log_before, image_before) > ppm(log_first, image_first),
        "the index log's write ratio did not climb across {ROUNDS} passes: {:.3}x then {:.3}x. The \
         log takes a record per write, so a ratio that does not move is not measuring the log",
        as_x(ppm(log_first, image_first)),
        as_x(ppm(log_before, image_before))
    );

    // ---- AND THE WAL IS NOT IN THE INDEX NUMERATOR, which is how 262,144 B of write-ahead log
    // ---- got into an index ratio once. If the walk stops seeing it, the partition is filtering
    // ---- nothing and the next reader inherits that figure.
    assert!(
        wal_after > 0,
        "the walk found no WAL bytes under the index directory. Either the layout moved and the \
         partition is now filtering nothing, or the walk stopped recursing -- and a breakdown that \
         cannot see the WAL is how it got into this numerator before"
    );

    // ---- WHAT RECLAIM DID IS PRINTED, NOT ASSERTED. Whether the gate fires depends on the 40%
    // ---- reclaimable condition as well as the byte threshold, and what a single round should
    // ---- recover is a policy question. The trajectory above is the finding.
    assert!(
        disk_after <= disk_before.saturating_mul(2),
        "a cycle took the index from {disk_before} B to {disk_after} B on disk, more than doubling \
         it. A dump may write an image beside the log it supersedes, but not that"
    );
}
