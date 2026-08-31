// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Reload-equality coverage for the upsert index-log delta records
//! (TS_INDEXLOG_UPSERT_DELTAS): the served view reconstructed through base-only fold
//! recovery must equal the pre-reload view at scale.
#![allow(clippy::all)]
use super::*;

/// Forensic probe against a preserved on-disk store (not a regression test): point
/// PROBE_DIR at a store prefix (holding cache/ pages/ indexes/), load shard 1 through
/// the normal lifecycle, and print what the served view holds.
#[test]
#[ignore]
fn evidence_store_probe() {
    let dir = std::env::var("PROBE_DIR").expect("set PROBE_DIR to a store prefix");
    let engine = TemporalEngine::with_local_dirs(
        1 << 30,
        format!("{dir}/cache"),
        format!("{dir}/pages"),
        format!("{dir}/indexes"),
    );
    let started = std::time::Instant::now();
    let response = engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    println!(
        "load: ok={} msg={} elapsed={:?}",
        response.status.ok, response.status.message, started.elapsed()
    );
    let shards = engine.shards.read().unwrap();
    if let Some(shard) = shards.get(&1) {
        let pages: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| bucket.page_index.len())
            .sum();
        let live_pages: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| bucket.page_index.values().filter(|p| !p.deleted).count())
            .sum();
        println!(
            "served: hashes={} strings={} features={} context_nodes={} buckets={} pages={} live_pages={} anchor={:?}",
            shard.hashes.len(),
            shard.strings.len(),
            shard.features.len(),
            shard.context_nodes.len(),
            shard.bucket_index.bucket_map.len(),
            pages,
            live_pages,
            shard.applied_wal_sequence,
        );
    } else {
        println!("served: shard 1 absent");
    }
}
