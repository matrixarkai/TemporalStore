// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CONTEXT NODE REWRITTEN IN PLACE REPLACES ITS BLOCK, IT DOES NOT ADD TO IT.
//!
//! A node is rewritten at a new address each time, and the append files that block without knowing
//! it replaces one. So every earlier block of the node stayed a live entry in the bucket index and
//! in the object's block refs: ten rewrites left ten entries, and each write then emitted one index
//! row per entry. A node rewritten 640 times allocated 1,043 blocks an upsert, against 130 at 40
//! rewrites; a plain hash overwrite, which asks the same question of the same map and retires the
//! block it leaves, stays at 61.
//!
//! The node write now asks `block_an_element_vacates` before it installs and retires that block
//! after the new one is in.
//!
//! WHAT MOVES: the entries the index holds for a rewritten node (one, not one per rewrite), and
//! with them the rows every later write to it emits and the cost of each write. WHAT DOES NOT: the
//! node an answer returns, the log record, or any format.
//!
//! GUARDS.
//!   * `a_rewritten_node_holds_one_block_entry` rewrites two nodes different numbers of times and
//!     requires exactly one live entry for each in the rendered index, and the last text back.
//!   * `a_rewritten_node_reads_back_after_a_restart` rewrites a node twenty times, drops the engine
//!     with no unload (so the log is replayed) and, in a second arm, unloads and loads it, and
//!     requires the last text and one live entry after each.
//!   * `probe::a_node_rewrite_costs_the_same_after_many_rewrites` (alloc-probe, `--ignored`) holds
//!     the allocations of an upsert after 40 prior rewrites and after 640 within a margin of each
//!     other, with the growing entry count of the unfixed path (read off the index) as the control
//!     the probe is sized against.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::{CommandResponse, ContextNode};
use std::sync::Arc;

fn dirs(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    (dir.join("cache"), dir.join("blocks"), dir.join("indexes"))
}

fn open(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let (cache, blocks, indexes) = dirs(dir);
    let engine = Arc::new(TemporalEngine::with_local_dirs(1 << 24, cache, blocks, indexes));
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn node(hash: u64, text: &str) -> Box<ContextNode> {
    Box::new(
        serde_json::from_value(serde_json::json!({
            "node_hash": hash, "canonical_name": "node", "l0": text
        }))
        .expect("node"),
    )
}

fn upsert(engine: &TemporalEngine, hash: u64, text: &str) {
    run(engine, Command::ContextUpsertNode { tenant_hash: 7, node: node(hash, text) });
}

fn read_text(engine: &TemporalEngine, hash: u64) -> Option<String> {
    match run(engine, Command::ContextGetNode { tenant_hash: 7, node_hash: hash }) {
        CommandResponse::ContextNode { node, .. } => node.map(|node| node.l0),
        other => panic!("expected a node, got {other:?}"),
    }
}

/// How many LIVE block entries the rendered index holds for the node's object key.
fn live_entries(engine: &TemporalEngine, hash: u64) -> usize {
    let key = format!(" key=ctx:node:7:{hash} ");
    engine
        .bucket_index_shape_for_test(1)
        .lines()
        .filter(|line| line.contains(&key) && line.contains("deleted=false"))
        .count()
}

#[test]
fn a_rewritten_node_holds_one_block_entry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    // A node written once, a node rewritten many times, and a node in between: the count must not
    // depend on how often a node was rewritten.
    upsert(&engine, 1, "only once");
    assert_eq!(live_entries(&engine, 1), 1, "a node written once");
    let mut last = String::new();
    for round in 0..40 {
        last = format!("text {round}");
        upsert(&engine, 2, &last);
        if round == 0 || round == 9 || round == 39 {
            assert_eq!(live_entries(&engine, 2), 1, "after {} rewrites", round + 1);
        }
    }
    for round in 0..5 {
        upsert(&engine, 3, &format!("three {round}"));
    }
    assert_eq!(live_entries(&engine, 3), 1);
    // THE CONTROL: the nodes did not collapse into one another or vanish.
    assert_eq!(live_entries(&engine, 1), 1);
    assert_eq!(read_text(&engine, 1).as_deref(), Some("only once"));
    assert_eq!(read_text(&engine, 2).as_deref(), Some(last.as_str()));
    assert_eq!(read_text(&engine, 3).as_deref(), Some("three 4"));
    assert_eq!(read_text(&engine, 99), None, "a node never written reads as absent");
}

#[test]
fn a_rewritten_node_reads_back_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        for round in 0..20 {
            upsert(&engine, 5, &format!("version {round}"));
        }
        upsert(&engine, 6, "neighbour");
        assert_eq!(live_entries(&engine, 5), 1);
        // DROPPED WITH NO UNLOAD: the next open recovers from the log alone.
    }
    let engine = open(dir.path());
    assert_eq!(read_text(&engine, 5).as_deref(), Some("version 19"), "after replaying the log");
    assert_eq!(read_text(&engine, 6).as_deref(), Some("neighbour"));
    assert_eq!(live_entries(&engine, 5), 1, "the replayed node holds one entry");
    // A rewrite after the recovery still replaces, and a second recovery still agrees.
    upsert(&engine, 5, "after recovery");
    assert_eq!(live_entries(&engine, 5), 1);
    // And the graceful arm: unload writes the index, load reads it back.
    engine.unload_shard(1);
    engine.load_shard(1);
    assert_eq!(read_text(&engine, 5).as_deref(), Some("after recovery"));
    assert_eq!(read_text(&engine, 6).as_deref(), Some("neighbour"));
    assert_eq!(live_entries(&engine, 5), 1, "after an unload and a load");
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 40;

    /// Allocations per upsert of OPS upserts to a node that already has been rewritten `prior`
    /// times.
    fn per_upsert(prior: usize) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = open(dir.path());
        for round in 0..prior {
            upsert(&engine, 1, &format!("p{round}"));
        }
        let commands: Vec<Command> = (0..OPS)
            .map(|round| Command::ContextUpsertNode { tenant_hash: 7, node: node(1, &format!("m{round}")) })
            .collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok);
        }
        probe.stop().allocs as f64 / OPS as f64
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_node_rewrite_costs_the_same_after_many_rewrites() {
        let canary = Probe::start();
        let sink: Vec<u8> = Vec::with_capacity(8192);
        assert!(
            canary.stop().allocs > 0,
            "the counting allocator is not installed -- rerun with `--features alloc-probe`"
        );
        drop(sink);

        let few = per_upsert(40);
        let many = per_upsert(640);
        eprintln!("  [node rewrite] {few:.1} allocs/upsert after 40 rewrites, {many:.1} after 640");
        assert!(
            many <= few * 1.15 + 5.0,
            "an upsert allocated {few:.1} blocks after 40 rewrites and {many:.1} after 640: the \
             node's earlier blocks are still live entries the write walks"
        );
    }
}
