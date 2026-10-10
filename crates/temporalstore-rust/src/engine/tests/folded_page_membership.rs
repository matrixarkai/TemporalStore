// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A FOLDED PAGE IS NOW AUTHORITATIVE FOR MEMBERSHIP, AND THIS IS THE CHECK THAT SAID IT WAS NOT.
//!
//! # WHAT THIS MODULE USED TO ASSERT, AND WHY IT IS INVERTED RATHER THAN DELETED
//!
//! #2027 folds a container's element pages into one at compaction. #2028 then drove the refutation of
//! the stage that was to follow: twelve set members folded to one page, one member removed, the live
//! index naming eleven and the page holding twelve. A removal reached the resident map and the page
//! INDEX and did not reach the PAGE, because a folded page stays live for its other elements and
//! nothing rewrites it. So the page set was a SUPERSET of the membership, and deriving membership
//! from it resurrected the removed member -- exactly the over-complete state #2025 enumerated five
//! readers for, four of them live, arriving from the other side.
//!
//! The assertion that carried that constraint said, in its own failure message, that a red here would
//! mean the constraint had been LIFTED and was a result rather than a break. It has been. The
//! assertion is inverted in place rather than moved to a new module so that the diff is the record:
//! the row counts either side of it are the same measurement, and the verdict flipped.
//!
//! # WHAT LIFTED IT
//!
//! `container_pages`' second shape carries a per-item removal flag, and the removal path appends a
//! page that states the removal instead of only dropping the index entry. So the page set is no longer
//! a superset of the membership; it is a LOG of it, folded by `container_membership::derive_membership`
//! in append order.
//!
//! # WHAT THIS ASSERTS NOW
//!
//! Three things, and the third is the one that would have been easy to leave out:
//!
//!   1. the PAGES name the removed member as REMOVED -- not merely absent from a page, which is a
//!      different and much weaker fact;
//!   2. the membership DERIVED from the pages equals what the RESIDENT MAP holds, element for
//!      element. That side was the page index's element names until the width step took the
//!      element-name field off the entry; `shard.sets` is the durable authority and the served
//!      answer now, and the pair is stronger for it -- the entry name and the page payload were
//!      both written by one append, while the map and the pages are maintained by different paths;
//!   3. and the derivation is COMPLETE -- no page failed to read, none failed to walk, none was
//!      unframed. An incomplete derivation that happened to agree would agree by luck, and #2028's
//!      own fix to `insert_timestamped_secondary_view` is the recorded case of a read failure
//!      producing a silently short derived view that nothing else held a copy of.
//!
//! # DENOMINATORS
//!
//! Every side is counted and asserted non-zero before it is compared. A fixture that folded nothing,
//! or that read no page, would make two sides agree for the wrong reason. The fold's own counters are
//! asserted first, because "one page per container" is also what an object with one element looks
//! like -- and the tombstone's own page is asserted to exist, because a removal that wrote no page
//! would leave the derivation agreeing with the index only because the member was in neither.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// Members written. More than one page's worth would obscure the point; twelve fold into one page,
/// which is the state the refutation was about.
const MEMBERS: usize = 12;

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
        table_name: "folded-page-membership".to_string(),
        shard_uri: "local://folded-page-membership/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "write failed: {response:?}");
}

/// rust-internal: drives the engine's own command surface
#[test]
fn a_folded_pages_membership_now_states_the_member_the_live_index_no_longer_names() {
    // THE FIXTURE'S SERVED SIDE IS THE RESIDENT MAP, NOT THE PAGE INDEX. This comment predicted
    // the red rather than fixing it: "THE FIXTURE COUNTS MEMBERS THE LIVE INDEX NAMES, which is
    // zero once entries name pages instead of elements -- its floor says the removal did not do
    // what this fixture assumes." It is zero, and the fix is to read the side where membership
    // actually lives. See the re-attribution at SIDE 1 below.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let members: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|i| format!("member-{i:04}").into_bytes())
        .collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: "folded".to_string(),
                member: member.clone(),
            },
        );
    }

    // FOLD FIRST. The counters are the instrument, because "one page per container" is also what an
    // idle round over a one-element object reports.
    crate::engine::reset_container_batch_counts();
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let (batches, folded) = crate::engine::container_batch_counts();
    assert!(
        batches > 0 && folded > 0,
        "nothing folded ({batches} batches, {folded} pages), so this fixture is not in the state it \
         claims to be asking about"
    );

    // Both removal counters are floored over the removal below, so a removal that recorded NOTHING
    // in the pages cannot pass as one that did.
    crate::engine::container_pages::reset_unframed_container_removal_count();
    crate::engine::execute_on_shard::reset_tombstone_append_failure_count();

    let victim = members[3].clone();
    let victim_component = hex::encode(&victim);
    write(
        &engine,
        Command::SetRemove {
            key: "folded".to_string(),
            member: victim.clone(),
        },
    );

    assert_eq!(
        0,
        crate::engine::container_pages::unframed_container_removal_count(),
        "the removal could not frame a tombstone page, so the pages say nothing about it"
    );
    assert_eq!(
        0,
        crate::engine::execute_on_shard::tombstone_append_failure_count(),
        "the removal could not append its tombstone page"
    );

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    // SIDE 1: what the engine SERVES as this object's membership -- the resident map.
    //
    // RE-ATTRIBUTED, AND THAT IS THE ONE EDIT THE WIDTH STEP FORCED IN THIS ARM. This side read
    // `bucket_index_component_block_addresses` and recovered each member from the page entry's own
    // element name. `BlockIndex` has no element-name field now, so that walk answers for NO
    // members, and this arm's floor is what said so: "the live index names 0 of 12 members after
    // one removal, so the removal did not do what this fixture assumes". The removal was fine; the
    // SOURCE moved.
    //
    // `shard.sets` is where it moved to. It is durable (`#[serde(default)]`) and it is what
    // `SetMembers` and `SetLen` answer from, so it is the served membership in the same sense the
    // index walk used to be. THE PAIR IS STRONGER THAN IT WAS, not weaker: the entry's element name
    // and the page's payload were both written by the same append, so agreeing told you little
    // about either, whereas the resident map is maintained by the command path and the pages by the
    // append path. Two artefacts that can disagree.
    let served: BTreeSet<Vec<u8>> = shard
        .sets
        .get("folded")
        .map(|members| members.keys().cloned().collect())
        .unwrap_or_default();

    // SIDE 2: every page this object resolves to, TOMBSTONE ENTRIES INCLUDED.
    //
    // NO `page.deleted` SKIP, and that is the one line that differs from the refutation's walk. A
    // tombstone entry is precisely a deleted entry, so a walk that filtered them would read the page
    // set as it was before this change and would resurrect the member -- which is what makes the
    // absence of that filter the subject here rather than an incidental difference.
    let mut addresses: Vec<crate::block_store::ElementEntry> = Vec::new();
    let mut live_entries = 0usize;
    let mut tombstone_entries = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != "set" || &*page.object_key != "folded" {
                continue;
            }
            if page.deleted {
                tombstone_entries += 1;
            } else {
                live_entries += 1;
            }
            addresses.push(page.address.clone());
        }
    }

    assert!(
        tombstone_entries > 0,
        "DENOMINATOR: the removal left no tombstone entry, so there is no page for the derivation to \
         read the removal out of and any agreement below would be an agreement about nothing"
    );
    assert!(live_entries > 0, "DENOMINATOR: no live entries for this object");

    let derived = crate::engine::container_membership::derive_membership(
        "set",
        addresses,
        |address| engine.block_store.read(address).ok(),
    );

    println!("=== after a fold and one removal ===");
    println!(
        "  entries: {live_entries} live, {tombstone_entries} tombstone; \
         {} distinct page(s) read, {} of the first shape",
        derived.pages_read, derived.live_only_pages
    );
    println!(
        "  the resident map holds {} member(s); the pages derive {} live and {} removed",
        served.len(),
        derived.live.len(),
        derived.removed.len()
    );
    println!(
        "  the removed member: held by the resident map = {}, derived live from the pages = {}, \
         NAMED REMOVED by the pages = {}",
        served.contains(&victim),
        derived.live.contains_key(&victim_component),
        derived.removed.contains(&victim_component)
    );
    println!(
        "  derivation failures: {} read, {} undecodable, {} unframed, {} unrenderable",
        derived.read_failures, derived.undecodable, derived.unframed, derived.unrenderable_items
    );

    // DENOMINATORS FIRST. A zero on either side makes every comparison below empty.
    assert!(
        derived.pages_read > 0,
        "DENOMINATOR: no pages read for this object"
    );
    assert_eq!(
        MEMBERS - 1,
        served.len(),
        "the resident map holds {} of {MEMBERS} members after one removal, so the removal did not \
         do what this fixture assumes",
        served.len()
    );
    // NOT VACUOUS, AND IT WAS ON ITS WAY TO BEING SO. Against the index walk this read
    // `!named.contains(&victim)` over a set that had become EMPTY for every kind, so it could only
    // ever pass. Against the resident map the set holds eleven real members and this asks a
    // question that can be answered either way.
    assert!(
        !served.contains(&victim),
        "the resident map still holds the removed member"
    );

    // COMPLETE, AND THEREFORE WORTH COMPARING. Asserted before the agreement rather than after: an
    // incomplete derivation that agreed would be agreeing by luck, and the failure counts say which
    // of four ways it fell short rather than only that it did.
    assert!(
        derived.is_complete(),
        "the derivation is incomplete ({} failure(s)), so its agreement with the map below would \
         be luck: {} read, {} undecodable, {} unframed, {} unrenderable",
        derived.failures(),
        derived.read_failures,
        derived.undecodable,
        derived.unframed,
        derived.unrenderable_items
    );

    // THE INVERTED CONSTRAINT. This is the assertion whose flip is the result.
    assert!(
        derived.removed.contains(&victim_component),
        "THE PAGES DO NOT STATE THE REMOVAL. This is the refutation #2028 recorded, back: a folded \
         page stays live for its other elements and nothing rewrote it, so the removed member is \
         still in it and a page-derived membership resurrects it. Either the removal did not append \
         a tombstone page, or the entry that kept that page reachable was dropped, or the derivation \
         did not reach it."
    );
    assert!(
        !derived.live.contains_key(&victim_component),
        "the pages derive the removed member as LIVE as well as removed, so the tombstone did not \
         outrank the page it supersedes -- which is an ORDERING failure, not a missing tombstone"
    );

    // AND THE TWO SIDES AGREE, ELEMENT FOR ELEMENT. Not by count: #2014 is the recorded case of a
    // length answer and its listing agreeing by coincidence, and a count comparison here would pass
    // if the derivation held a different eleven members.
    let derived_members: BTreeSet<Vec<u8>> = derived
        .live
        .keys()
        .filter_map(|component| hex::decode(component).ok())
        .collect();
    assert_eq!(
        served, derived_members,
        "the membership derived from the pages is not the membership the resident map holds"
    );

    // AND THE DERIVED VALUES ARE THE MEMBERS THEMSELVES, which for a set is what #2017 measured the
    // page already spells. A derivation that recovered the right KEYS and the wrong values would
    // satisfy every assertion above.
    let mismatched: BTreeMap<String, usize> = derived
        .live
        .iter()
        .filter(|(component, value)| hex::decode(component).as_deref() != Ok(value.as_slice()))
        .map(|(component, value)| (component.clone(), value.len()))
        .collect();
    assert!(
        mismatched.is_empty(),
        "{} derived element(s) carry a value that is not the member their component spells: {:?}",
        mismatched.len(),
        mismatched
    );
}
