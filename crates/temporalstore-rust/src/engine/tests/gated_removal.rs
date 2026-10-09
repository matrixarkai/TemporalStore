// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A GATED REMOVAL STAYS REMOVED, THOUGH THE PAGE STILL STATES THE MEMBER.
//!
//! # WHY THIS IS DRIVEN RATHER THAN BUILT
//!
//! Deletion as a property of the element in the payload is already implemented:
//! `CONTAINER_PAGE_MAGIC_V2` writes a `TAG_REMOVED` item per removed element, and
//! `ContainerPageItem::deleted` carries it. The module says why in the place a reader would look --
//! "a removal has to be a WRITTEN ITEM rather than an absence, because a page is never rewritten by
//! the removal path" -- and records the failure it closed: twelve members folded onto one page, one
//! removed, the index naming eleven and the page holding twelve, so a membership derived from pages
//! resurrected the removed member.
//!
//! So the page format needs nothing, the payload carries its own version in its magic rather than in
//! `SHARD_INDEX_FORMAT_VERSION`, and no stamp moves. What reading CANNOT answer is whether a removal
//! survives when the index entry no longer names elements, and that is this test.
//!
//! # THE TRAP THIS IS AIMED AT
//!
//! `mark_bucket_index_block_deleted` finds the entry to tombstone BY COMPONENT. Under one entry per
//! page the entry carries none, so the match cannot select it -- the same selection-versus-meaning
//! shape that produced an orphan at step two and five entries at step five-a. A removal that cannot
//! find its entry, over a page that still states the member as live, is exactly the arrangement a
//! resurrection comes out of.
//!
//! # PRESENCE AND ABSENCE ARE TWO PROPERTIES
//!
//! This campaign has two independent demonstrations that a presence-only guard cannot see a
//! resurrection -- a listing with a working source-proof AND a working negative control still served
//! a removed member. So the arm is three-part and the middle part is the one that matters:
//!
//!   1. remove through the REAL surface, not by reaching into the index;
//!   2. **prove the page still states the member**, so the two sources demonstrably disagree before
//!      the reader is consulted -- otherwise this is an agreement test and can discriminate nothing;
//!   3. assert the reader sides with the AUTHORITATIVE source.
//!
//! Plus a reload, because the durable question is not what a live engine answers but what comes
//! back.

#![allow(clippy::all)]
use super::*;

const MEMBERS: usize = 40;
const VALUE_WIDTH: usize = 24;

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "removal".to_string(),
        shard_uri: "local://removal/1".to_string(),
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
    let stamp = format!("r-{index:05}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < VALUE_WIDTH {
            bytes[slot] = *byte;
        }
    }
    bytes
}

fn write_to(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// THE AUTHORITATIVE SOURCE: the members the durable map holds for this object.
fn resident_members(engine: &TemporalEngine, object_key: &str) -> std::collections::BTreeSet<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    shard
        .sets
        .get(object_key)
        .map(|members| members.iter().map(|(member, _)| member.clone()).collect())
        .unwrap_or_default()
}

/// THE OTHER SOURCE: every element key a live page of this object still states as PRESENT.
///
/// Read off the bytes, so the disagreement below is observed rather than assumed.
fn components_the_pages_state(
    engine: &TemporalEngine,
    object_key: &str,
) -> std::collections::BTreeSet<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut stated = std::collections::BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != object_key {
                continue;
            }
            let Ok(bytes) = engine.block_store.read(&page.address) else {
                continue;
            };
            if let crate::engine::container_pages::ContainerPageDecode::Framed {
                spelling,
                items,
                ..
            } = crate::engine::container_pages::decode_container_page(&bytes)
            {
                for item in items {
                    if item.deleted {
                        continue;
                    }
                    if let Some(component) =
                        crate::engine::container_pages::component_from_element_key(
                            spelling, &item.key,
                        )
                    {
                        stated.insert(component);
                    }
                }
            }
        }
    }
    stated
}

/// Live and tombstoned set entries for this object.
fn entry_counts(engine: &TemporalEngine, object_key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != object_key {
                continue;
            }
            if page.deleted {
                tombstoned += 1;
            } else {
                live += 1;
            }
        }
    }
    (live, tombstoned)
}

#[test]
fn a_gated_removal_stays_removed_across_a_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    let victim = member_bytes(2);
    let victim_component = hex::encode(&victim);

    {
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            &pages,
            &indexes,
        );
        load_on(&engine);

        for index in 0..MEMBERS {
            write_to(
                &engine,
                Command::SetAdd {
                    key: "removal/set".to_string(),
                    member: member_bytes(index),
                },
            );
        }

        // THE FOLD IS WHAT MAKES THE SOURCES ABLE TO DISAGREE. Before it each member owns its own
        // page, so removing one takes its whole page with it and no page is left stating it.
        crate::engine::reset_container_batch_counts();
        engine
            .compact_shard_blocks(1)
            .expect("the fold round must succeed");
        let (batches, folded) = crate::engine::container_batch_counts();
        assert!(
            batches > 0 && folded > 0,
            "the fixture folded nothing: {batches} batch(es), {folded} page(s). Without a shared \
             page the removal cannot leave a page stating the member, and the middle arm below \
             would be vacuous"
        );

        // ---- PART 1: REMOVE THROUGH THE REAL SURFACE. ----
        write_to(
            &engine,
            Command::SetRemove {
                key: "removal/set".to_string(),
                member: victim.clone(),
            },
        );

        let (live, tombstoned) = entry_counts(&engine, "removal/set");
        println!(
            "\n=== a gated removal ===\n  after remove: {live} live entr(ies), {tombstoned} tombstoned"
        );

        // ---- PART 2: THE PAGE STILL STATES THE MEMBER, so the sources disagree. ----
        let stated = components_the_pages_state(&engine, "removal/set");
        assert!(
            stated.contains(&victim_component),
            "no live page states the removed member any more, so the payload and the durable map \
             AGREE and this test cannot tell a reader that honours the removal from one that \
             enumerates the page. A page is never rewritten by the removal path, so after a fold \
             the folded page is supposed to still carry it"
        );
        println!(
            "  the pages still state {} member(s), including the removed one",
            stated.len()
        );

        // ---- PART 3: THE AUTHORITATIVE SOURCE HAS DROPPED IT. ----
        let resident = resident_members(&engine, "removal/set");
        assert!(
            !resident.contains(&victim),
            "the durable map still holds the removed member, so the removal did not take effect \
             at all and the reload below would be testing the wrong thing"
        );
        assert_eq!(
            MEMBERS - 1,
            resident.len(),
            "the durable map holds {} members after removing one of {MEMBERS}",
            resident.len()
        );

        engine.flush_shard_index(1);
    }

    // ---- AND IT SURVIVES A RELOAD, which is the durable question. ----
    //
    // Its own cache directory, so nothing is answered out of a page the first engine left warm.
    let reloaded = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-reloaded"),
        &pages,
        &indexes,
    );
    load_on(&reloaded);

    let resident = resident_members(&reloaded, "removal/set");
    let stated = components_the_pages_state(&reloaded, "removal/set");
    println!(
        "  after reload: durable holds {} member(s); pages state {}",
        resident.len(),
        stated.len()
    );

    // FLOOR: the object has to have come back at all, or "the member is absent" is true for the
    // wrong reason.
    assert!(
        resident.len() > 1,
        "only {} member(s) came back, so the object did not survive the reload and its absence \
         proves nothing about the removal",
        resident.len()
    );
    assert!(
        !resident.contains(&victim),
        "THE REMOVED MEMBER CAME BACK ACROSS A RELOAD. Its page still states it as live and the \
         index entry names a page rather than an element, so a load that rebuilt membership from \
         pages would resurrect it -- which is the failure this whole arrangement is arranged \
         against"
    );
    assert_eq!(
        MEMBERS - 1,
        resident.len(),
        "{} members came back where {} were left after the removal",
        resident.len(),
        MEMBERS - 1
    );
    // AND EVERY OTHER MEMBER DID come back, so the removal took exactly one.
    for index in 0..MEMBERS {
        let member = member_bytes(index);
        if member == victim {
            continue;
        }
        assert!(
            resident.contains(&member),
            "member {index} did not survive the reload, so the removal took more than the one \
             member it was given"
        );
    }
}
