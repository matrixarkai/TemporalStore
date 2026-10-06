// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN OVERWRITE KEEPS THE MEMBER'S ORDINAL UNDER EITHER PROJECTION.
//!
//! # THE PROPERTY, WHICH ALREADY HAS A GUARD FOR THE DEFAULT PATH
//!
//! `container_page_ordinal` assigns a container element its POSITION, and an overwrite of the same
//! element must reuse that position rather than take a new one --
//! `container_page_ordinal::an_overwrite_keeps_the_members_ordinal_rather_than_climbing` pins that
//! for the ungated path. The mechanism is an identity branch: the walk looks for a page already
//! filed under this element's component and, finding one, returns the ordinal that page holds.
//!
//! # WHY THE GATE BREAKS IT, AND WHY NO EXISTING TEST SEES THAT
//!
//! Under one entry per page the entry carries NO component, so
//! `page.component.as_deref() == Some(component)` never matches and the walk falls through to
//! `highest + 1`. Every rewrite of one member then takes a fresh ordinal, which is a fresh
//! `block_id`, which is a fresh ADDRESS -- so a member rewritten five times leaves five pages
//! instead of overwriting one.
//!
//! The existing guard cannot see this: it reads no environment variable, so it runs with the gate
//! off and passes. Inheriting coverage from the ungated suite is an illusion, which is why this arm
//! exists rather than being assumed.
//!
//! # WHERE THE ORDINAL COMES FROM INSTEAD
//!
//! Not from a page read -- `container_page_ordinal` is handed the index and nothing that could
//! fetch a page, and adding a fetch to a write path inside a footprint change is the scope drift
//! this series has already declined once.
//!
//! It comes from the RESIDENT MAP, which is keyed by the element and whose value carries the
//! address: a member that already exists has its own `block_id` right there, and that IS its
//! position. So the gated path asks the map the question the index can no longer answer, and the
//! answer is authoritative rather than derived.

#![allow(clippy::all)]
use super::*;
use crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE;

const REWRITES: usize = 5;

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
        table_name: "ordinal".to_string(),
        shard_uri: "local://ordinal/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

/// Distinct page addresses, and the block ids on them, for one set object.
fn pages_and_ordinals(engine: &TemporalEngine, object_key: &str) -> (usize, Vec<u64>) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut pages = std::collections::BTreeSet::new();
    let mut ordinals = std::collections::BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set"
                || &*page.object_key != object_key
                || page.deleted
            {
                continue;
            }
            pages.insert((
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            ));
            if let Some(block_id) = page.address.block_id() {
                ordinals.insert(block_id);
            }
        }
    }
    (pages.len(), ordinals.into_iter().collect())
}

/// Every live set entry of this object, as (component, slab, offset, length, block_id), so a
/// surprising entry count can be read rather than guessed at.
fn entry_rows(engine: &TemporalEngine, object_key: &str) -> Vec<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut rows = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != object_key {
                continue;
            }
            rows.push(format!(
                "deleted={} component={:?} slab={} off={} len={} block_id={:?}",
                page.deleted,
                page.component.as_deref(),
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
                page.address.block_id()
            ));
        }
    }
    rows.sort();
    rows
}

/// Rewrite ONE member `REWRITES` times under `gate_on`, and report what the index holds after.
fn rewrites_under(gate_on: bool) -> (usize, Vec<u64>, usize, Vec<String>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    // BOTH DIRECTIONS AS VALUES: an unset variable now selects the GATED path.
    if gate_on {
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, "1");
    } else {
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, "0");
    }
    load_on(&engine);

    let member = b"the-one-member".to_vec();
    for _ in 0..REWRITES {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "ord/set".to_string(),
                member: member.clone(),
            },
        });
        assert!(response.status.ok, "write failed: {response:?}");
    }

    let (pages, ordinals) = pages_and_ordinals(&engine, "ord/set");
    // How many members the resident map holds, so a row cannot be read as correct because the
    // object vanished.
    let resident = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard.sets.get("ord/set").map(|m| m.len()).unwrap_or(0)
    };
    let rows_detail = entry_rows(&engine, "ord/set");
    std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE);
    drop(engine);
    (pages, ordinals, resident, rows_detail)
}

#[test]
fn an_overwrite_keeps_the_members_ordinal_under_either_projection() {
    println!("\n=== one member rewritten {REWRITES} times ===");
    println!("  {:>5}  {:>6}  {:>9}  {:>24}", "gate", "pages", "resident", "ordinals");

    let mut rows: Vec<(&str, usize, Vec<u64>, usize)> = Vec::new();
    for (label, gate_on) in [("off", false), ("on", true)] {
        let (pages, ordinals, resident, rows_detail) = rewrites_under(gate_on);
        println!("  {label:>5}  {pages:>6}  {resident:>9}  {ordinals:>24?}");
        for row in &rows_detail {
            println!("          {row}");
        }
        rows.push((label, pages, ordinals, resident));
    }

    // FLOORS, on each arm, on the reaching of the path: the object has to exist and be named.
    for (label, pages, _ordinals, resident) in &rows {
        assert_eq!(
            1, *resident,
            "gate {label}: the resident map holds {resident} member(s) after rewriting ONE member \
             {REWRITES} times, so this row is not measuring an overwrite"
        );
        assert!(
            *pages > 0,
            "gate {label}: no page is filed for the object at all, so the count below compares \
             absences"
        );
    }

    // THE ORDINAL DOES NOT CLIMB, ON EITHER ARM, AND THAT IS WHAT THIS STEP FIXES.
    //
    // An overwrite is the same element in the same position. Under the gate the entry carries no
    // component, so `container_page_ordinal`'s identity branch cannot find the member's own
    // previous page and falls through to `highest + 1` -- measured before the fix as ordinals
    // [0, 1]. The position now comes from `shard.sets[key][member].block_id()`: the map that is
    // keyed by the member, holding the address it currently occupies, which IS its position.
    // Authoritative rather than derived, and no page read.
    for (label, _pages, ordinals, _resident) in &rows {
        assert_eq!(
            vec![0u64],
            *ordinals,
            "gate {label}: one member rewritten {REWRITES} times holds ordinals {ordinals:?}. An \
             overwrite must reuse the member's position; a climbing ordinal is a fresh block id, a \
             fresh address, and therefore a fresh page"
        );
    }

    // THE ENTRY COUNTS, AS THEY ACTUALLY ARE RATHER THAN AS I FIRST ASSUMED.
    //
    // This engine APPENDS a page per write; it does not modify one in place. The ungated path
    // still holds ONE entry because its replacement is scoped BY THE ELEMENT -- the write unnames
    // the element's own previous page. A page-named entry cannot carry that scope, so under the
    // gate the stale entry the last derivation filed is still there beside the one this write
    // filed. Asserted rather than described, because the number surprised me.
    assert_eq!(
        1, rows[0].1,
        "gate off: {} entries where the element-scoped replacement should leave one",
        rows[0].1
    );
    assert!(
        rows[1].1 > 1,
        "gate on: {} entries. The stale entry from the last derivation is expected to still be \
         here -- if it is not, the per-write filer has started unnaming pages and the healing \
         assertion below is testing nothing",
        rows[1].1
    );
}

/// AND A DERIVATION HEALS IT, WHICH IS THE PROPERTY THE SERIES RESTS ON.
///
/// The stale entry is a TRANSIENT, not a defect: it names a page no element carries any more, so
/// the next derivation does not emit it. The projection reads the resident map, which holds only
/// the CURRENT address for each element -- which is exactly why one entry per live page is
/// reachable by the derivation and not by the write.
///
/// Nothing else in the suite checks this, and without it the series rests on a reading rather than
/// a test -- which is what nearly shipped an orphan three steps ago.
#[test]
fn a_derivation_drops_the_entry_no_element_carries_any_more() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, "1");
    load_on(&engine);

    let member = b"the-one-member".to_vec();
    for _ in 0..REWRITES {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "heal/set".to_string(),
                member: member.clone(),
            },
        });
        assert!(response.status.ok, "write failed: {response:?}");
    }

    let before = entry_rows(&engine, "heal/set");
    // FLOOR: there has to be something to heal, or the assertion after the derivation passes over
    // a state that was already clean.
    assert!(
        before.len() > 1,
        "only {} entr(ies) before the derivation, so there is no stale entry for it to drop and \
         this test would pass without exercising the healing",
        before.len()
    );

    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        let (start, end) = shard.routing_range();
        crate::engine::storage_bucket_internals::rebuild_bucket_first_index(1, shard, start, end);
    }

    let after = entry_rows(&engine, "heal/set");
    println!("\n=== a derivation drops what no element carries ===");
    for row in &before {
        println!("  before  {row}");
    }
    for row in &after {
        println!("  after   {row}");
    }

    let resident = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard.sets.get("heal/set").map(|m| m.len()).unwrap_or(0)
    };
    std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE);

    assert_eq!(
        1, resident,
        "the resident map holds {resident} member(s), so the object is not the one-member shape \
         this test is about"
    );
    assert_eq!(
        1,
        after.len(),
        "after the derivation {} entr(ies) remain where one live page should leave one. A stale \
         entry naming a page no element carries is supposed to be dropped by the derivation, \
         because the projection reads the resident map and the resident map holds only the \
         current address",
        after.len()
    );
    assert!(
        after[0].contains("component=None"),
        "the surviving entry is {:?}, which still names an element. Under the gate the derivation \
         files one entry per page and a page needs no element name",
        after[0]
    );
}
