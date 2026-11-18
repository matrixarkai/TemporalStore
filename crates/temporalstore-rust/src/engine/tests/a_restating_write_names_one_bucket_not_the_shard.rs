// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WRITE THAT RESTATES AN OBJECT'S BLOCKS NAMES THE BLOCKS OF ONE BUCKET, NOT OF THE SHARD.
//!
//! `sync_bucket_index_object_blocks_with_mode` drops an object's superseded blocks bucket by bucket
//! and, for each bucket, names that bucket's blocks first. It named them through an inversion built
//! over EVERY block of EVERY object in the shard -- two hash maps grown to the shard's block count,
//! built and dropped on each restating write. Every context event write restates its node's
//! blocks, so at 2,000 nodes one event write allocated about 606 KB for it (two hash tables of
//! 336 KB and 270 KB, eleven grow steps each).
//!
//! The projection reads `(routing_bucket, handle)` of the blocks of ONE bucket, and an inversion
//! restricted to that bucket holds exactly those positions, so it names the same blocks to the same
//! objects. It is `BlockKeysByPosition::from_parts_for_bucket`, which already existed.
//!
//! WHAT MOVES: the allocation of a restating write stops growing with the shard. WHAT DOES NOT: any
//! name, any block filed or dropped, any byte written.
//!
//! TWO GUARDS.
//!   * `a_bucket_projection_names_what_the_whole_shard_inversion_names` builds a shard of context
//!     nodes, hashes and features and requires, for EVERY bucket, the projection through the bucket
//!     inversion to equal the projection through the whole-shard one, with a count that the buckets
//!     really held names.
//!   * `probe::a_restating_write_does_not_allocate_with_the_shard` (needs `alloc-probe`, `--ignored`)
//!     holds the bytes of one context event write at 100 nodes and at 1,500 nodes within a small
//!     margin of each other, with the whole-shard inversion built in the test as the control that
//!     must grow with the shard.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::engine::state::{block_identities_in, BlockKeysByPosition};
use crate::types::{ContextEvent, ContextNode};
use std::sync::Arc;

fn open(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        1 << 24,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn node(hash: u64) -> Box<ContextNode> {
    Box::new(
        serde_json::from_value(serde_json::json!({
            "node_hash": hash, "canonical_name": "node", "l0": "a short summary of the node"
        }))
        .expect("node"),
    )
}

fn event(id: u64, at: u64) -> Box<ContextEvent> {
    Box::new(
        serde_json::from_value(serde_json::json!({
            "event_id_hash": id, "event_time_ms": at, "text": "an event of some text"
        }))
        .expect("event"),
    )
}

fn write_event(engine: &TemporalEngine, node_hash: u64, id: u64, at: u64) {
    run(
        engine,
        Command::ContextWriteEvent {
            tenant_hash: 7,
            node_hash,
            event: event(id, at),
            first_write_only: false,
            cold_storage: false,
        },
    );
}

fn populate(engine: &TemporalEngine, nodes: u64) {
    for hash in 1..=nodes {
        run(engine, Command::ContextUpsertNode { tenant_hash: 7, node: node(hash) });
        write_event(engine, hash, hash, 1_000 + hash);
        write_event(engine, hash, hash + 1_000_000, 2_000 + hash);
    }
}

#[test]
fn a_bucket_projection_names_what_the_whole_shard_inversion_names() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    populate(&engine, 150);
    for index in 0..80u64 {
        run(
            &engine,
            Command::HashSet {
                key: format!("h-{index}"),
                field: "f".to_string(),
                value: b"v".to_vec(),
            },
        );
        run(
            &engine,
            Command::FeatureAppend {
                key: format!("f-{index}"),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1_000 + index,
                    value: b"1".to_vec(),
                }],
            },
        );
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard");
    let whole = BlockKeysByPosition::from_parts(
        &shard.bucket_index.objects,
        &shard.bucket_index.object_block_lookup,
    );
    let mut buckets_with_names = 0usize;
    let mut names_compared = 0usize;
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        let restricted = BlockKeysByPosition::from_parts_for_bucket(
            &shard.bucket_index.objects,
            &shard.bucket_index.object_block_lookup,
            *routing_bucket,
        );
        let through_whole = block_identities_in(bucket, &whole);
        let through_bucket = block_identities_in(bucket, &restricted);
        assert_eq!(
            through_whole, through_bucket,
            "bucket {routing_bucket} is named differently by the bucket inversion"
        );
        buckets_with_names += usize::from(!through_whole.is_empty());
        names_compared += through_whole.len();
    }
    // THE CONTROL: the shard really held named blocks in many buckets, so equal maps are not
    // equal emptiness.
    assert!(buckets_with_names >= 20, "only {buckets_with_names} buckets named a block");
    assert!(names_compared >= 300, "only {names_compared} names were compared");
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const WRITES: u64 = 60;

    /// Bytes per context event write, over WRITES writes to nodes that already hold events, with
    /// `nodes` nodes in the shard.
    fn bytes_per_write(nodes: u64) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = open(dir.path());
        populate(&engine, nodes);
        let commands: Vec<Command> = (0..WRITES)
            .map(|index| Command::ContextWriteEvent {
                tenant_hash: 7,
                node_hash: 1 + index % nodes,
                event: event(5_000_000 + index, 10_000 + index),
                first_write_only: false,
                cold_storage: false,
            })
            .collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok);
        }
        probe.stop().alloc_bytes as f64 / WRITES as f64
    }

    /// What the whole-shard inversion allocates at `nodes` nodes: the control.
    fn whole_shard_inversion_bytes(nodes: u64) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = open(dir.path());
        populate(&engine, nodes);
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard");
        let probe = Probe::start();
        let inversion = BlockKeysByPosition::from_parts(
            &shard.bucket_index.objects,
            &shard.bucket_index.object_block_lookup,
        );
        let bytes = probe.stop().alloc_bytes as f64;
        drop(inversion);
        bytes
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_restating_write_does_not_allocate_with_the_shard() {
        let canary = Probe::start();
        let sink: Vec<u8> = Vec::with_capacity(8192);
        assert!(
            canary.stop().allocs > 0,
            "the counting allocator is not installed -- rerun with `--features alloc-probe`"
        );
        drop(sink);

        let control_small = whole_shard_inversion_bytes(100);
        let control_large = whole_shard_inversion_bytes(1_500);
        assert!(
            control_large >= control_small * 8.0,
            "the control (the whole-shard inversion) grew only {control_small:.0} -> {control_large:.0} B, \
             so the probe cannot see work that grows with the shard"
        );

        let small = bytes_per_write(100);
        let large = bytes_per_write(1_500);
        eprintln!(
            "  [restating write] {small:.0} B/write at 100 nodes, {large:.0} B/write at 1,500 \
             (control inversion: {control_small:.0} -> {control_large:.0} B)"
        );
        assert!(
            large <= small * 1.5 + 4096.0,
            "a context event write allocated {small:.0} B at 100 nodes and {large:.0} B at 1,500: \
             it is naming the blocks of the whole shard"
        );
    }
}
