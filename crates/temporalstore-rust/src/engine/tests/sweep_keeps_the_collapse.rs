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
use crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE;

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
fn pages_entries_named(engine: &TemporalEngine, object_key: &str) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut pages = std::collections::BTreeSet::new();
    let mut entries = 0usize;
    let mut named = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set"
                || &*page.object_key != object_key
                || page.deleted
            {
                continue;
            }
            entries += 1;
            if page.component.is_some() {
                named += 1;
            }
            pages.insert((
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            ));
        }
    }
    (pages.len(), entries, named)
}

/// Run a whole sweep with the gate set BEFORE the first write, and report what survived it.
fn sweep_under(gate_on: bool) -> (usize, usize, usize, u64, u64) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    // BOTH DIRECTIONS AS VALUES: an unset variable now selects the GATED path.
    if gate_on {
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, "1");
    } else {
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, "0");
    }
    // The gate is set before `load_shard`, which is itself one of the projection's consumers.
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
    let (pages, entries, named) = pages_entries_named(&engine, "sweep/set");
    std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE);
    drop(engine);
    (pages, entries, named, batches, folded)
}

#[test]
fn a_whole_sweep_leaves_the_collapsed_entries_collapsed() {
    println!("\n=== after a WHOLE sweep, gate set before the first write ===");
    println!(
        "  {:>5}  {:>6}  {:>8}  {:>7}  {:>8}  {:>7}",
        "gate", "pages", "entries", "named", "batches", "folded"
    );

    let mut rows: Vec<(&str, usize, usize, usize, u64, u64)> = Vec::new();
    for (label, gate_on) in [("off", false), ("on", true)] {
        let (pages, entries, named, batches, folded) = sweep_under(gate_on);
        println!(
            "  {label:>5}  {pages:>6}  {entries:>8}  {named:>7}  {batches:>8}  {folded:>7}"
        );
        rows.push((label, pages, entries, named, batches, folded));
    }

    // FLOORS, ON EACH ARM, ON THE REACHING OF THE PATH.
    for (label, pages, entries, _named, batches, folded) in &rows {
        assert!(
            *batches > 0 && *folded > 0,
            "gate {label}: the sweep wrote {batches} batch(es) folding {folded} page(s), so it \
             never reached the fold and the row above says nothing about what a sweep leaves behind"
        );
        assert!(
            *pages > 0 && *entries > 0,
            "gate {label}: {pages} page(s) and {entries} entr(ies) after the sweep, so the object \
             did not survive it at all and the comparison below is between absences"
        );
    }

    let off = &rows[0];
    let on = &rows[1];

    // THE TWO SWEEPS AGREE ON WHICH PAGES ARE LIVE. The gate may change how many entries name a
    // page, never the live set.
    assert_eq!(
        off.1, on.1,
        "the sweeps disagree about how many pages are live ({} off, {} on)",
        off.1, on.1
    );

    // UNGATED: the sweep leaves one entry per element, every one named. This is the default path.
    assert_eq!(
        MEMBERS, off.2,
        "gate off: the sweep left {} entries for {MEMBERS} members. The default path must be \
         unchanged by this series at every step",
        off.2
    );
    assert_eq!(
        MEMBERS, off.3,
        "gate off: {} of {} ungated entries carry a component; all of them must",
        off.3, off.2
    );

    // GATED: the entries stay collapsed THROUGH the sweep, including its two internal rebuilds.
    // This is the claim -- that the derivation is the keystone and a filing change cannot be
    // undone by the re-derivation, because the re-derivation is the same projection.
    assert_eq!(
        on.1, on.2,
        "gate on: the sweep left {} entries for {} page(s). A sweep calls \
         `rebuild_bucket_first_index` twice, so if the rebuild re-derived one entry per element \
         the collapse would be undone before the sweep returned -- which is exactly the fight this \
         step exists to rule out",
        on.2, on.1
    );
    assert_eq!(
        0, on.3,
        "gate on: {} entries still carry a component after the sweep. An entry that names a page \
         needs no element name, so a component here means a rebuild put one back",
        on.3
    );
    println!(
        "  => entries after the sweep: {} off, {} on, for {} page(s)",
        off.2, on.2, on.1
    );
}
