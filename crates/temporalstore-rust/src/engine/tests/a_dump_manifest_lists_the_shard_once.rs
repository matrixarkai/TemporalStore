// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A DUMP MANIFEST LISTS THE SHARD'S LIVE BLOCKS ONCE, AND ITS SUMMARIES DESCRIBE THE CAPTURE.
//!
//! The manifest used to list every live block of the live shard for its bucket summaries and then
//! list the captured copy again for the slab set and the lifecycle report: two full walks of one
//! store, the first under the live read guard. Both consumers now read ONE listing of the capture.
//! The summaries must come out the same as the live shard's (a quiet shard captures to itself) and
//! must be taken from the SAME state the image describes, so a write landing after the capture is
//! not in them.
#![allow(clippy::all)]
use super::*;

const SHARD: ShardId = 1;

fn engine_with_keys(dir: &std::path::Path, keys: usize) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("once-{index:04}"),
                value: format!("value-{index}").into_bytes(),
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
    engine
}

#[test]
fn the_summaries_in_a_manifest_are_the_live_shards_summaries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_keys(dir.path(), 64);
    let live = engine.bucket_storage_summaries(SHARD);
    assert!(!live.is_empty());
    let manifest = engine.create_bucket_dump_manifest(SHARD, Vec::new()).unwrap();
    let mut expected = live;
    expected.sort_by_key(|summary| summary.routing_bucket);
    assert_eq!(
        format!("{:?}", manifest.bucket_summaries),
        format!("{expected:?}"),
        "a quiet shard captures to itself, so the two listings agree"
    );
}

#[test]
fn a_manifest_lists_the_live_blocks_once() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_keys(dir.path(), 64);
    let live_blocks: u64 = engine
        .bucket_storage_summaries(SHARD)
        .iter()
        .map(|summary| summary.block_ref_count)
        .sum();
    assert!(live_blocks >= 64, "{live_blocks}");
    // The counter is global: assert on the walk of THIS call, not on a ratio across tests.
    reset_live_block_scan_entries();
    engine.create_bucket_dump_manifest(SHARD, Vec::new()).unwrap();
    let scanned = live_block_scan_entries();
    assert!(
        scanned >= live_blocks,
        "the capture is listed at least once: {scanned} < {live_blocks}"
    );
    assert!(
        scanned < 2 * live_blocks,
        "the live shard was listed again for the summaries: {scanned} entries for {live_blocks} blocks"
    );
}

/// MEASUREMENT (ignored): the manifest call's busy time and listing size at `DUMP_KEYS` keys.
#[test]
#[ignore]
fn what_one_manifest_call_costs_by_key_count() {
    let keys: usize = std::env::var("DUMP_KEYS").ok().and_then(|v| v.parse().ok()).unwrap_or(20_000);
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_with_keys(dir.path(), keys);
    let mut micros = Vec::new();
    for _ in 0..5 {
        reset_live_block_scan_entries();
        let started = std::time::Instant::now();
        engine.create_bucket_dump_manifest(SHARD, Vec::new()).unwrap();
        micros.push(started.elapsed().as_micros());
        println!("[manifest-call] keys={keys} listed_entries={} us={}", live_block_scan_entries(), micros.last().unwrap());
    }
    micros.sort();
    println!("[manifest-call] keys={keys} median_us={}", micros[micros.len() / 2]);
}
