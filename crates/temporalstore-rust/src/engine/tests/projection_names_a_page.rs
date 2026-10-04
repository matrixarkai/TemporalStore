// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE PROJECTION EMITS, GATE OFF AGAINST GATE ON, PER OBJECT.
//!
//! OBSERVED BEFORE IT IS ASSERTED. This module prints the entry count both ways first and asserts
//! only the fixture's floor, because writing "assert one entry" before running it would be fitting
//! the test to a guess -- and if the count comes out as something else, the pressure is then to
//! adjust the expectation rather than to understand it. The assertions come in a second pass, over
//! the numbers this prints.
//!
//! IT OWNS THE GATE VARIABLE WHILE IT RUNS, and so does `one_entry_a_page_gate`. The value is
//! process-global, so the two are safe together only because the verdict for this repository is a
//! single-threaded run; each removes the variable when it is done so the next test sees the shipped
//! default.

#![allow(clippy::all)]
use super::*;
use crate::engine::TS_CONTAINER_ONE_ENTRY_A_PAGE;

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
        table_name: "projection".to_string(),
        shard_uri: "local://projection/1".to_string(),
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
    let stamp = format!("m-{index:05}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

/// Distinct live pages, live entries, and tombstoned entries this set object resolves to.
fn pages_and_entries(engine: &TemporalEngine, object_key: &str) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut pages = std::collections::BTreeSet::new();
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != object_key {
                continue;
            }
            if page.deleted {
                tombstoned += 1;
                continue;
            }
            live += 1;
            pages.insert((
                page.address.block_slab_id(),
                page.address.offset(),
                page.address.length(),
            ));
        }
    }
    (pages.len(), live, tombstoned)
}

/// How many of this object's live entries carry a component at all.
fn entries_with_a_component(engine: &TemporalEngine, object_key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut named = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() == "set"
                && &*page.object_key == object_key
                && !page.deleted
                && page.component.is_some()
            {
                named += 1;
            }
        }
    }
    named
}

/// Every member the set listing answers with.
fn listed_members(engine: &TemporalEngine, object_key: &str) -> Vec<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SetMembers {
            key: object_key.to_string(),
        },
    });
    assert!(response.status.ok, "the listing failed: {response:?}");
    match response.response {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    }
}

/// Re-derive the whole projection, which is what a compaction sweep does twice.
fn rederive(engine: &TemporalEngine) {
    let mut shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get_mut(&1).expect("shard 1 loaded");
    let (start, end) = shard.routing_range();
    crate::engine::storage_bucket_internals::rebuild_bucket_first_index(1, shard, start, end);
}

#[test]
fn what_the_projection_emits_per_object_gate_off_against_gate_on() {
    println!("\n=== entries per OBJECT, set kind, gate off against gate on ===");
    println!(
        "  {:>9}  {:>6}  {:>7}  {:>7}  {:>6}  {:>7}  {:>7}  {:>6}",
        "occupancy", "pages", "off:ent", "off:named", "pages", "on:ent", "on:named", "tombs"
    );

    for occupancy in [1usize, 4, 40] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine);
        let key = format!("obj-{occupancy:03}");
        for index in 0..occupancy {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SetAdd {
                    key: key.clone(),
                    member: member_bytes(index),
                },
            });
            assert!(response.status.ok, "write failed: {response:?}");
        }
        crate::engine::reset_container_batch_counts();
        engine
            .compact_shard_blocks(1)
            .expect("the fold round must succeed");
        let (batches, folded) = crate::engine::container_batch_counts();

        // GATE OFF: today's projection, re-derived so both columns come from the same door.
        std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE);
        rederive(&engine);
        let (off_pages, off_entries, _) = pages_and_entries(&engine, &key);
        let off_named = entries_with_a_component(&engine, &key);

        // GATE ON: the same model map, projected the other way.
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, "1");
        rederive(&engine);
        let (on_pages, on_entries, on_tombs) = pages_and_entries(&engine, &key);
        let on_named = entries_with_a_component(&engine, &key);
        std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE);

        println!(
            "  {occupancy:>9}  {off_pages:>6}  {off_entries:>7}  {off_named:>9}  \
             {on_pages:>6}  {on_entries:>7}  {on_named:>7}  {on_tombs:>6}  \
             (batches {batches}, folded {folded})"
        );

        // AND WHAT A READ ANSWERS UNDER EACH PROJECTION, on A SEPARATE OBJECT PER ARM.
        //
        // `SetMembers` answers through `cached_response`, keyed by the object. Listing the same key
        // under one projection and then the other returns the FIRST answer out of the cache, so
        // the second number is not a reading of the second projection -- it is the first one again.
        // Measured that way the gate-on column read a full listing and meant nothing. Two objects
        // of identical shape, one listed under each projection, is what makes the two numbers
        // independent.
        //
        // The listing is step eight of this series, so at step two it has not been brought along.
        // Printing it is how to find out whether that matters yet rather than guess.
        let off_key = format!("read-off-{occupancy:03}");
        let on_key = format!("read-on-{occupancy:03}");
        for probe_key in [&off_key, &on_key] {
            for index in 0..occupancy {
                let response = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::SetAdd {
                        key: probe_key.clone(),
                        member: member_bytes(index),
                    },
                });
                assert!(response.status.ok, "write failed: {response:?}");
            }
        }
        engine
            .compact_shard_blocks(1)
            .expect("the fold round must succeed");

        std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE);
        rederive(&engine);
        let off_listed = listed_members(&engine, &off_key).len();
        std::env::set_var(TS_CONTAINER_ONE_ENTRY_A_PAGE, "1");
        rederive(&engine);
        let on_listed = listed_members(&engine, &on_key).len();
        std::env::remove_var(TS_CONTAINER_ONE_ENTRY_A_PAGE);
        println!(
            "             listing returns: {off_listed} member(s) gate off, {on_listed} gate on, \
             of {occupancy} written (separate objects, so neither reads the other's cache)"
        );

        // ---- THE FLOOR, before any figure above is read. ----
        assert!(
            off_entries > 0 && on_entries > 0,
            "occupancy {occupancy}: one of the projections emitted nothing ({off_entries} off, \
             {on_entries} on), so the row above is a comparison between unexercised arms"
        );
        assert_eq!(
            1, off_pages,
            "occupancy {occupancy}: the fixture holds {off_pages} pages, so the members did not \
             come to share one and the collapse has nothing to collapse"
        );

        // ---- THE TWO PROJECTIONS AGREE ON WHICH PAGES ARE LIVE. ----
        //
        // They walk the same map with the same filter, so this is the invariant that says the gate
        // changes how many entries NAME a page and nothing about which pages there are. If this
        // ever fails, the gated arm is not a reprojection of the same live set.
        assert_eq!(
            off_pages, on_pages,
            "occupancy {occupancy}: the projections disagree about WHICH pages are live \
             ({off_pages} off, {on_pages} on)"
        );

        // ---- GATE OFF: ONE ENTRY PER ELEMENT, EVERY ONE NAMED. ----
        assert_eq!(
            occupancy, off_entries,
            "occupancy {occupancy}: the ungated projection emitted {off_entries} entries. It must \
             stay exactly one per element -- this is the default path, and it is what every \
             deployment that has not set the gate still runs"
        );
        assert_eq!(
            occupancy, off_named,
            "occupancy {occupancy}: {off_named} of {off_entries} ungated entries carry a \
             component. All of them must: the component is what tells two entries of one object \
             apart when the entry is per-element"
        );

        // ---- GATE ON: ONE ENTRY PER PAGE, NONE NAMED. ----
        //
        // This is the collapse, as a direct count rather than as width arithmetic: at forty
        // elements on one page it is forty entries against one, per OBJECT.
        assert_eq!(
            on_pages, on_entries,
            "occupancy {occupancy}: the gated projection emitted {on_entries} entries for \
             {on_pages} page(s). The page id IS the identity under this gate, so the count must be \
             the page count"
        );
        assert_eq!(
            0, on_named,
            "occupancy {occupancy}: {on_named} gated entries still carry a component. The page \
             already carries each element's key in its payload, so an entry that names a page \
             needs no element name -- a component here means the collapse is only half done"
        );

        // ---- AND THE READ, WHICH IS NOT YET BROUGHT ALONG. ----
        //
        // ASSERTED AS IT IS, NOT AS IT SHOULD END UP, and that is deliberate. A gate buys
        // incremental reviewability by giving up the signal a breaking change normally provides:
        // nothing goes red, because the suite runs with the gate off. So the incomplete state is
        // pinned instead. Step eight brings the listing along, and when it does THIS ASSERTION MUST
        // BE CHANGED -- which is the forced restatement the gate would otherwise have removed.
        //
        // Why it is empty: the listing takes identity from per-element entries, and under the gate
        // there are none to take it from. That is not a defect in the projection; it is the
        // consumer not having caught up, and it is why the gate ships off and must not be turned
        // on before the series finishes.
        assert_eq!(
            occupancy,
            off_listed,
            "occupancy {occupancy}: the ungated listing returned {off_listed} of {occupancy} \
             members. The DEFAULT path must be whole at every step of this series"
        );
        assert_eq!(
            0, on_listed,
            "occupancy {occupancy}: the gated listing returned {on_listed} members where this \
             step expects none. If a later step has brought the listing along, this assertion is \
             the one to change -- deliberately, saying so -- rather than the projection"
        );
    }
}
