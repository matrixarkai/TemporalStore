// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE PROJECTION EMITS PER OBJECT, AND THAT RE-DERIVING TWICE DOES NOT MOVE IT.
//!
//! OBSERVED BEFORE IT IS ASSERTED. This module prints the entry count first and asserts only the
//! fixture's floor, because writing "assert one entry" before running it would be fitting the test
//! to a guess -- and if the count comes out as something else, the pressure is then to adjust the
//! expectation rather than to understand it. The assertions come in a second pass, over the
//! numbers this prints.
//!
//! THE HEADER USED TO READ "GATE OFF AGAINST GATE ON", AND THE GATE IS GONE. Both of this module's
//! columns went through the same `rederive`, so the comparison was between a projection and
//! itself, and the two assertions pinning the "ungated" column to one entry per ELEMENT described
//! a projection that no longer existed. The fixture is kept and asked a question it can still
//! answer wrongly -- whether the derivation is IDEMPOTENT, which matters because a compaction
//! sweep re-derives twice. The paragraph about owning a process-global gate variable went with the
//! gate: this module sets nothing global any more.

#![allow(clippy::all)]
use super::*;

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
fn what_the_projection_emits_per_object_and_that_re_deriving_twice_does_not_move_it() {
    println!("\n=== entries per OBJECT, set kind, across two re-derivations ===");
    println!(
        "  {:>9}  {:>6}  {:>7}  {:>6}  {:>7}  {:>6}",
        "occupancy", "pages", "entries", "pages", "entries", "tombs"
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

        // THE TWO COLUMNS ARE TWO RE-DERIVATIONS NOW, NOT TWO PROJECTIONS.
        //
        // They were labelled "gate off" and "gate on" and the comment above the first one
        // explained how to avoid "comparing the gated projection against itself". With the gate
        // retired that is exactly what the test was doing: both columns call the same `rederive`
        // through the same door, so every "off against on" assertion below compared a number with
        // itself -- and the two that pinned the ungated column to one entry per ELEMENT were
        // asserting a projection that no longer exists.
        //
        // THE FIXTURE IS KEPT AND ITS CLAIM MOVED, because two successive re-derivations is a
        // question worth asking and one this can get wrong: the projection must be IDEMPOTENT. A
        // sweep calls `rebuild_bucket_first_index` twice, so a derivation that emitted a different
        // entry set the second time round would collapse on the first pass and un-collapse on the
        // second. That is a real failure mode, it is what this fixture is already shaped to
        // measure, and it is asserted below.
        rederive(&engine);
        let (first_pages, first_entries, _) = pages_and_entries(&engine, &key);

        rederive(&engine);
        let (second_pages, second_entries, second_tombs) = pages_and_entries(&engine, &key);

        println!(
            "  {occupancy:>9}  {first_pages:>6}  {first_entries:>7}  \
             {second_pages:>6}  {second_entries:>7}  {second_tombs:>6}  \
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

        rederive(&engine);
        let off_listed = listed_members(&engine, &off_key).len();
        rederive(&engine);
        let on_listed = listed_members(&engine, &on_key).len();
        println!(
            "             listing returns: {off_listed} member(s) gate off, {on_listed} gate on, \
             of {occupancy} written (separate objects, so neither reads the other's cache)"
        );
        // FLOOR: the object survived both re-derivations, so the equalities below are not
        // between absences.
        assert!(
            first_pages > 0 && first_entries > 0,
            "occupancy {occupancy}: the first re-derivation left {first_pages} page(s) and \
             {first_entries} entr(ies), so every comparison below holds over nothing"
        );

        // ---- IDEMPOTENCE: THE SECOND RE-DERIVATION MUST NOT MOVE EITHER NUMBER. ----
        //
        // A compaction sweep re-derives twice. A projection that emitted a different entry set on
        // the second pass would collapse and then un-collapse inside one sweep.
        assert_eq!(
            (first_pages, first_entries),
            (second_pages, second_entries),
            "occupancy {occupancy}: re-deriving twice moved the projection -- {first_pages} \
             page(s)/{first_entries} entr(ies) then {second_pages}/{second_entries}. A sweep \
             re-derives twice, so a projection that is not idempotent undoes itself mid-sweep"
        );

        // ---- ONE ENTRY PER PAGE, as a direct count rather than as width arithmetic. ----
        //
        // At forty elements on one page this is forty entries against one, per OBJECT. Compared
        // against the measured page count and not against a literal, so a fixture that folds
        // differently cannot make it vacuous.
        assert_eq!(
            second_pages, second_entries,
            "occupancy {occupancy}: the projection emitted {second_entries} entries for \
             {second_pages} page(s). The page IS the identity, so the count must be the page count"
        );

        // AND THE "NONE NAMED" HALF IS STRUCTURAL NOW. This block also asserted `0 == on_named`
        // from `entries_with_a_component`, whose helper has been deleted with it: `BlockIndex` has
        // no component field, so the count could only read zero. `state.rs`'s pin is what refuses
        // a name coming back -- 40 with `!= 39 && != 41` and a field sum equal to the width.

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
        // THE TWO LISTING ARMS ARE THE SAME ARM NOW, and both are kept only because they read
        // SEPARATE OBJECTS: `SetMembers` answers through `cached_response` keyed by the object, so
        // two keys are what make the two numbers independent readings rather than one cached
        // answer read twice. They used to be labelled ungated and gated; there is no gate, so what
        // they now assert is that two objects of identical shape both list whole. That is still a
        // claim this can fail -- the listing folds pages by `append_position` and a fold that lost
        // a page would shorten it -- but it is one claim measured twice, not two.
        assert_eq!(
            occupancy,
            off_listed,
            "occupancy {occupancy}: the first object's listing returned {off_listed} of \
             {occupancy} members"
        );
        // ---- CHANGED DELIBERATELY AT STEP EIGHT, which is the step the previous note
        // ---- anticipated by name.
        //
        // What stood here pinned ZERO, and it was right for every step before this one: the
        // projection had collapsed the entries to one a page, and nothing had yet taught any
        // reader to take an element's identity out of a PAYLOAD. A gated listing therefore
        // answered empty -- its `None` arm can only be answered by a page that names no element,
        // and every gated page is framed.
        //
        // Step eight brought the listing along. It folds the object's pages with
        // `container_membership::derive_membership`, which orders them by `append_position` and
        // treats a value and a tombstone as the same kind of statement, so the later page wins.
        // That is the direction that resurrected a removed member when it was tried with a
        // hand-rolled loop, and the two things that make it safe are driven elsewhere: the
        // tombstone page is reachable from the index, and the fold owns the precedence.
        //
        // RESTATED, NOT DELETED. Removing it would leave the gated listing with no pin at all,
        // and the property worth holding from here on is the stronger one -- that the gated
        // listing answers exactly what the ungated listing answers.
        assert_eq!(
            occupancy, on_listed,
            "occupancy {occupancy}: the second object's listing returned {on_listed} of \
             {occupancy} members. The listing folds the object's pages by `append_position` to \
             answer this, so a fold that drops a page shortens it"
        );
    }
}
