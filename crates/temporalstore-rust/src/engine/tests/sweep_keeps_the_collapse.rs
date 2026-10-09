// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER A WHOLE COMPACTION SWEEP LEAVES THE COLLAPSED ENTRIES COLLAPSED.
//!
//! # THE HOLE THIS CLOSES
//!
//! The projection emits one entry per page under the gate, and that was measured. But it was
//! measured by folding first and turning the gate on afterwards, then re-deriving by hand. The
//! case that was NOT covered is the one that matters operationally: the gate on for the **whole
//! sweep**.
//!
//! It matters because `compact_shard_blocks` calls `rebuild_bucket_first_index` TWICE -- once on
//! the partial-failure commit path and once at the end of a successful round -- and that is the
//! fight predicted when this series was planned: compaction folds the pages, then the sweep
//! re-derives the index, so a filing change made anywhere else is undone before the sweep returns.
//!
//! The prediction after reading those two call sites is that nothing re-explodes, because both are
//! bare calls into the same projection the gate switches. **A prediction is not an observation**,
//! and "appears to be resolved" is how a stale premise starts, so this runs the real sweep with the
//! gate set before the first write and counts what survives it.
//!
//! # THE FLOORS, AND WHAT THEY FLOOR
//!
//! On EACH arm, because one arm reaching the sweep while the other does not is itself the
//! broken-arm case and a single shared floor cannot see it. And they floor the **reaching of the
//! path** -- that a fold actually happened and that pages were walked -- never the **value of the
//! result**, because the result under test may legitimately be a small number and a floor on it
//! would forbid the true answer.
//!
//! Each arm gets its own engine, so neither can answer out of the other's state.

#![allow(clippy::all)]
use super::*;

const MEMBERS: usize = 40;
const VALUE_WIDTH: usize = 24;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "sweep".to_string(),
        shard_uri: "local://sweep/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn member_bytes(index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; VALUE_WIDTH];
    let stamp = format!("s-{index:05}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

/// Live pages, live entries, and entries carrying a component, for one set object.
fn pages_and_entries(engine: &TemporalEngine, object_key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut pages = std::collections::BTreeSet::new();
    let mut entries = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set"
                || &*page.object_key != object_key
                || page.deleted
            {
                continue;
            }
            entries += 1;
            pages.insert((
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            ));
        }
    }
    (pages.len(), entries)
}

/// Run a whole sweep and report what survived it.
///
/// THE `gate_on` PARAMETER IS GONE, AND IT HAD ALREADY STOPPED DOING ANYTHING. It selected between
/// `if gate_on { } else { }` -- two EMPTY branches, left behind when the gate itself was retired --
/// so the two rows this test printed were two runs of identical code, and the "gate off" row's
/// expectations (one entry per element, every one naming its element) described a path that no
/// longer existed. A comparison between two runs of the same code is the shape this campaign calls
/// a tautology, and it was also asserting a false half.
fn sweep_once() -> (usize, usize, u64, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    for index in 0..MEMBERS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "sweep/set".to_string(),
                member: member_bytes(index),
            },
        });
        assert!(response.status.ok, "write failed: {response:?}");
    }

    crate::engine::reset_container_batch_counts();
    engine
        .compact_shard_blocks(1)
        .expect("the sweep must succeed");
    let (batches, folded) = crate::engine::container_batch_counts();
    let (pages, entries) = pages_and_entries(&engine, "sweep/set");
    drop(engine);
    (pages, entries, batches, folded)
}

#[test]
fn a_whole_sweep_leaves_the_collapsed_entries_collapsed() {
    println!("\n=== after a WHOLE sweep ===");

    let (pages, entries, batches, folded) = sweep_once();
    println!("  pages {pages}  entries {entries}  batches {batches}  folded {folded}");

    // FLOORS, ON THE REACHING OF THE PATH. `entries == pages` is satisfied by 0 == 0, so a sweep
    // that lost the object entirely would pass the claim below without these.
    assert!(
        batches > 0 && folded > 0,
        "the sweep wrote {batches} batch(es) folding {folded} page(s), so it never reached the \
         fold and the numbers above say nothing about what a sweep leaves behind"
    );
    assert!(
        pages > 0 && entries > 0,
        "{pages} page(s) and {entries} entr(ies) after the sweep, so the object did not survive it"
    );
    // AND THE FOLD ACTUALLY SHARED A PAGE, which is what makes one-entry-a-page a smaller number
    // than one-entry-an-element rather than the same number by coincidence. Without this, a
    // fixture whose members never folded would satisfy `entries == pages` with one of each.
    assert!(
        pages < MEMBERS,
        "the {MEMBERS} members resolve to {pages} page(s) after the sweep, so nothing folded and \
         `entries == pages` below would hold with or without the collapse"
    );

    // THE CLAIM: THE ENTRIES STAY COLLAPSED **THROUGH** THE SWEEP, including its two internal
    // rebuilds. The derivation is the keystone -- a filing change cannot be undone by the
    // re-derivation, because the re-derivation is the same projection.
    assert_eq!(
        pages, entries,
        "the sweep left {entries} entries for {pages} page(s). A sweep calls \
         `rebuild_bucket_first_index` twice, so if the rebuild re-derived one entry per element \
         the collapse would be undone before the sweep returned -- which is exactly the fight this \
         test exists to rule out"
    );

    // THE SECOND HALF OF THIS TEST USED TO BE A `named` COUNTER asserted to zero: no entry still
    // carrying an element name after the sweep, "a component here means a rebuild put one back".
    // `BlockIndex` has no component field, so a rebuild has nothing to put back and the counter
    // could only ever read zero. It is enforced by `state.rs`'s pin instead --
    // `size_of::<BlockIndex>() == 40` with `!= 39 && != 41` and a field sum equal to the width, so
    // re-adding a name fails const-evaluation rather than being counted here.
    println!("  => entries after the sweep: {entries} for {pages} page(s)");
}
