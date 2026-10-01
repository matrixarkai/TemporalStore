// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE RETAINED TOMBSTONE ENTRY COSTS, AND WHAT IT MUST NOT CHANGE.
//!
//! # WHY THIS MODULE EXISTS SEPARATELY FROM THE PAGE CODEC'S GUARDS
//!
//! `container_tombstone_collection` drives the page FORMAT and the fold: what a removal encodes and how
//! an ordered set of pages folds into a membership. None of that needs an engine. This module drives
//! the other half, which does: a removal now leaves an INDEX ENTRY behind, and that entry is the first
//! traffic `BlockIndex::deleted` has ever carried.
//!
//! Before this change, NOTHING IN PRODUCTION SET THAT FIELD TO TRUE. Of the thirty-nine walks of
//! `block_index` outside the tests, nineteen filter `deleted` in branches that were therefore never
//! exercised and twenty do not filter it at all. So the interesting question is not whether the
//! tombstone is written -- `container_tombstone_collection` settles that -- but whether any reader
//! that now meets one answers differently than it should. Two did, and both are guarded here.
//!
//! # THE SIX CLAIMS
//!
//!   1. no index reader answers from a tombstone entry, over the five readers #2025 enumerated;
//!   2. a removal retains exactly one entry, and the cost is stated as a number;
//!   3. a key's LAST element still files its object id, which a naive reading of the retained entry
//!      would have broken;
//!   4. a removal that matched nothing writes no tombstone entry;
//!   5. a removed member does not make its OBJECT deleted in the runtime report;
//!   6. a zset SCORE CHANGE tombstones the element key it left, which is the arm least likely to be
//!      recognised as a removal at all.
//!
//! Each prints its denominators. A tombstone count of zero and a tombstone count that is right look
//! the same to an assertion that only checks the live side, which is how four of these would pass
//! against a removal that recorded nothing.
//!
//! # THE THIRTY-NINE WALKS, WITH A VERDICT EACH
//!
//! This is the part a reviewer cannot reconstruct, so it is written down rather than summarised. Every
//! walk of `block_index` outside the tests, enumerated mechanically with its enclosing function --
//! not from memory, and with the count asserted, because a verdict over a subset of the walks is not
//! an audit. Three were WRONG and are fixed by this change; three are KNOWN and accepted with the
//! reason; three are unreachable with a tombstone; the rest are correct or harmless.
//!
//! A WORD ON THE TWO PASSES. A first pass classified by asking whether the word `deleted` appeared
//! within ten lines of the walk, and that was wrong twice in opposite directions: at
//! `collect_bucket_index_live_block_entries` the word was a FIELD being populated rather than a
//! filter, and the release refusal turned out to be `BlockDeleted` rather than the model-map
//! disagreement first reported. Every verdict below is from the predicate, not the proximity.
//!
//! ```text
//!  #  site                                                  verdict
//!  1  engine.rs:3783 fold_carried_container_elements        CORRECT  `if !page.deleted` -- a tombstone is not a live page key
//!  2  engine.rs:3907 fold_delta_block_items                 CORRECT  retain by (kind,key,component): a delta restating that
//!                                                                    element takes its tombstone with it
//!  3  engine.rs:3921 fold_delta_block_items                 CORRECT  retain by covered_keys, whole-object
//!  4  engine.rs:4740 mark_bucket_index_object_deleted       CORRECT  a whole-object delete takes tombstones too; the
//!                                                                    CommonDelete arm of the five-way guard asserts zero
//!  5  engine.rs:4931 mark_..._block_deleted_recording       CORRECT  the removal's own retain
//!  6  engine.rs:4979 mark_..._block_deleted_recording       WRONG, FIXED HERE. `any(|page| page.object_id() == id)` would
//!                                                                    be answered by the removed element's OWN tombstone, so a
//!                                                                    key's last element never filed its object id. Now asks
//!                                                                    about LIVE pages. a_keys_last_element_still_files_its_object_id
//!  7  engine.rs:5384 record_exists_exact                    CORRECT  `!page.deleted`; EXISTS is one of #2025's five readers
//!  8  engine.rs:6111 object_manager_stats                   CORRECT  `.filter(|page| !page.deleted)`
//!  9  engine.rs:6236 object_manager_stats                   HARMLESS dirty-bucket count; a fresh tombstone IS dirty and its
//!                                                                    bucket does need dumping
//! 10  bucket_store.rs:105 runtime_report                    CORRECT  counts it as deleted_block_ref_count, which is what it is
//! 11  bucket_store.rs:204 bucket_index_block_address        CORRECT  `!page.deleted`
//! 12  bucket_store.rs:253 ..._component_block_addresses     CORRECT  `!page.deleted`. THE reader every serving path resolves
//!                                                                    through, and the reason index answers do not move
//! 13  lifecycle.rs:2141 synthetic_address_count_for_test    HARMLESS a test helper counting addresses; a tombstone has one
//! 14  lifecycle.rs:2188 rehydrate_wal_resident_blocks       CORRECT  skips it, so a WAL-resident tombstone page is not
//!                                                                    re-registered -- the page is on the block store and the
//!                                                                    derivation reads it from there
//! 15  object_manager.rs:87 runtime_report                   WRONG, FIXED HERE. Counted a tombstone as a LIVE block ref, and
//!                                                                    `object.deleted |= page.deleted` let ONE removed member
//!                                                                    report its whole object deleted.
//!                                                                    a_removed_member_does_not_make_its_object_deleted
//! 16  state.rs:3356 rebuild_object_block_lookup             CORRECT  delegates to insert_object_block_lookup, which returns
//!                                                                    early on a deleted page -- so tombstones stay OUT of the
//!                                                                    fast lookup, which is what keeps index answers unchanged
//! 17  state.rs:3525 contains_object_block_address           CORRECT  `!page.deleted`
//! 18  state.rs:3564 next_block_index_for_object             CORRECT AS-IS, and this one is a judgement. It has no filter and
//!                                                                    needs none: its thirteen call sites are all `feature` and
//!                                                                    `context_*`, the timestamped kinds, which have no
//!                                                                    tombstones. Two high-water derivations over DISJOINT kind
//!                                                                    sets, so filtering one was the right half to filter.
//! 19  state.rs:3638 container_page_ordinal                  WRONG, FIXED HERE. No filter, so a tombstone stopped `max` from
//!                                                                    falling: the ordinal would climb once per element ever
//!                                                                    written and a churning container would walk to
//!                                                                    MAX_ADDRESSABLE_BLOCK_ID and fall off the ceiling
//! 20  sbi.rs:890 storage_topology_snapshot...               CORRECT  sets a delete_marker, which a recorded removal is
//! 21  sbi.rs:1429 rebuild_bucket_block_ownership            KNOWN, ACCEPTED (2 of 2 below)
//! 22  sbi.rs:1675 collect_..._live_block_entries            CORRECT  includes tombstones and CARRIES `deleted` into
//!                                                                    LiveBlockEntry; nine downstream sites read that flag.
//!                                                                    Named `live` and is not a live-only set -- checked
//!                                                                    because the name says otherwise
//! 23  sbi.rs:2109 release_bucket_blocks                     KNOWN, ACCEPTED (1 of 2 below)
//! 24  sbi.rs:2139 release_bucket_blocks                     UNREACHABLE with a tombstone -- #23 refuses first
//! 25  sbi.rs:2151 release_bucket_blocks                     UNREACHABLE -- same
//! 26  sbi.rs:2174 release_bucket_blocks                     UNREACHABLE -- same
//! 27  sbi.rs:3574 upsert_bucket_index_block_inner           CORRECT  keeps object_index while a tombstone holds the id; the
//!                                                                    object does still have pages
//! 28  sbi.rs:3592 upsert_bucket_index_block_inner           CORRECT AND LOAD-BEARING. Its retain matches on component, so a
//!                                                                    RE-ADD clears the tombstone entry. That is the difference
//!                                                                    between one entry per DISTINCT element removed and one per
//!                                                                    REMOVAL, it was not written for this, and nothing else
//!                                                                    would notice if it stopped.
//!                                                                    a_re_add_clears_the_tombstone_entry_so_churn_on_one_element_does_not_accumulate
//! 29  sbi.rs:3598 upsert_bucket_index_block_inner           CORRECT  as #27
//! 30  sbi.rs:3762 sync_..._object_blocks_with_mode          CORRECT  a whole-object restate drops every entry of the key,
//!                                                                    tombstones included; not a per-element container path
//! 31  sbi.rs:4007 update_bucket_layout                      CORRECT  live_object_ids filters `!page.deleted`; an all-tombstone
//!                                                                    bucket clears object_index, which is true of it
//! 32  sbi.rs:4058 refresh_one_bucket_runtime_flags          CORRECT  sibling of #31
//! 33  sbi.rs:4061 refresh_one_bucket_runtime_flags          KNOWN, ACCEPTED (2 of 2 below) -- the same predicate as #21
//! 34  sbi.rs:4076 refresh_one_bucket_runtime_flags          HARMLESS TTL minimum over object keys; a tombstone's key is the
//!                                                                    same key, so the minimum cannot move
//! 35  sbi.rs:4216 clear_published_object_dirty_state        HARMLESS any-dirty; a tombstone is dirty until dumped
//! 36  sbi.rs:5283 validate_bucket_ownership_index...        CORRECT  matches by ADDRESS; a tombstone's address is its own and
//!                                                                    cannot answer for a live entry's
//! 37  storage_lifecycle_methods.rs:1829 lookup              CORRECT  skips it for logical/physical bytes, so a tombstone
//!                                                                    page's bytes are not counted in the bucket's size. A
//!                                                                    small undercount, named rather than left implicit
//! 38  storage_reporting.rs:40 filing_by_object_key          HARMLESS object_key -> bucket; a tombstone has the same key
//! 39  storage_reporting.rs:489 storage_physical_index_...   HARMLESS includes tombstones in a physical report, deduped by
//!                                                                    (key,kind,component,slab,offset)
//! ```
//!
//! # THE TWO KNOWN CONSEQUENCES, NAMED RATHER THAN UNMENTIONED
//!
//! **1. A bucket holding a tombstone is refused release** (#23). `release_bucket_blocks` answers
//! `BucketReleaseRefusal::BlockDeleted` on the first entry carrying `deleted`. That refusal is
//! PRE-EXISTING and deliberate -- it was written for a deleted block and had never fired, because
//! nothing set the flag -- so this change does not introduce it, it commissions it. It is not fixable
//! without changing what release means: the path releases a bucket only when the model maps could
//! rebuild exactly what is resident, and the model maps do not hold removed elements, so a tombstone
//! is by construction something they cannot rebuild. The cost is that such a bucket stays resident
//! until a compaction round rewrites it; the effect is bounded by the tombstone count, which #28
//! bounds by DISTINCT elements removed rather than by removals; and the refusal is counted, so it is
//! visible rather than silent. Releasing across it would lose the removals.
//!
//! **2. A bucket whose every entry is a tombstone is marked `deleted`** (#21, #33). `every_page_deleted
//! = !is_empty() && all(|page| page.deleted)`, and that is arguably just true of such a bucket: it
//! holds no live data. Two things read it. `bucket_store::runtime_report` counts it in
//! `deleted_bucket_count`, which is a report. Eviction eligibility is `in_memory() && !deleted() &&
//! !object_index.is_empty()`, so such a bucket is not evicted and stays resident -- and #31 has
//! already cleared its `object_index`, so the third term excludes it anyway and the `deleted` flag
//! changes nothing about eviction. The DUMP does not consult it, which is the part that matters: the
//! tombstones are serialized and survive a reload. So the observable effect is one report counter.
//!
//! Neither is fixed here. Both are stated in the pull request body as well as here, because an
//! unfixed consequence that is named is a decision and one that is merely not mentioned is not.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, table: &str) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: table.to_string(),
        shard_uri: format!("local://{table}/1"),
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

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "read failed: {response:?}");
    response.response
}

/// (live entries, tombstone entries) for one object, counted off the page index itself.
fn entry_counts(engine: &TemporalEngine, kind: &str, key: &str) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.model_id.as_str() != kind || &*page.object_key != key {
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

/// The components the tombstone entries of one object name.
fn tombstoned_components(engine: &TemporalEngine, kind: &str, key: &str) -> BTreeSet<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut held = BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted && page.model_id.as_str() == kind && &*page.object_key == key {
                if let Some(component) = page.component.as_deref() {
                    held.insert(component.to_string());
                }
            }
        }
    }
    held
}

/// rust-internal: drives SetAdd/SetRemove and the readers #2025 enumerated
#[test]
fn no_index_reader_answers_from_a_tombstone_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, "cte-readers");

    let key = "cte-set";
    let members: Vec<Vec<u8>> = (0..3).map(|m| format!("m{m}").into_bytes()).collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    let victim = members[1].clone();
    let victim_component = hex::encode(&victim);
    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: victim.clone(),
        },
    );

    // DENOMINATOR FIRST: there must actually BE a tombstone entry, or every reader below answers
    // correctly for the trivial reason that there is nothing for it to answer from.
    let (live, tombstoned) = entry_counts(&engine, "set", key);
    println!("=== after removing one of three ===");
    println!("  entries: {live} live, {tombstoned} tombstone");
    assert_eq!(
        1, tombstoned,
        "DENOMINATOR: {tombstoned} tombstone entries, so this module is not testing what it claims"
    );
    assert_eq!(2, live, "{live} live entries after removing one of three");

    // READER 1: the page index reader every serving path resolves through.
    let named: BTreeSet<Vec<u8>> =
        crate::engine::bucket_store::bucket_index_component_block_addresses(
            &engine
                .shards
                .read()
                .expect("engine lock poisoned")
                .get(&1)
                .expect("shard is loaded"),
            "set",
            key,
        )
        .iter()
        .filter_map(|(component, _)| component.as_deref().and_then(|c| hex::decode(c).ok()))
        .collect();
    println!("  index reader names {} member(s)", named.len());
    assert_eq!(2, named.len(), "the index reader names {} of 2", named.len());
    assert!(
        !named.contains(&victim),
        "the index reader names the removed member, so it answered from the tombstone entry"
    );

    // READER 2 and 3: the listing, and the count that IS the listing counted (SMEMBERS / SCARD).
    let listed = match read(
        &engine,
        Command::SetMembers {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("SetMembers answered {other:?}"),
    };
    println!("  SMEMBERS returns {} member(s), SCARD is that counted", listed.len());
    assert_eq!(2, listed.len(), "SMEMBERS returned {}", listed.len());
    assert!(
        !listed.contains(&victim),
        "SMEMBERS listed the removed member"
    );

    // READERS 4 and 5: EXISTS and TTL, which #2025 found answering for a removed element. Driven at
    // the boundary that made them wrong -- removing the LAST member, where the key itself must go.
    for member in [members[0].clone(), members[2].clone()] {
        write(
            &engine,
            Command::SetRemove {
                key: key.to_string(),
                member,
            },
        );
    }
    let (live_after, tombstoned_after) = entry_counts(&engine, "set", key);
    let exists = match read(
        &engine,
        Command::CommonExists {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("CommonExists answered {other:?}"),
    };
    let emptied = match read(
        &engine,
        Command::SetMembers {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("SetMembers answered {other:?}"),
    };
    println!("  after removing all three: {live_after} live, {tombstoned_after} tombstone entries");
    println!("  EXISTS answers {exists}, SMEMBERS returns {} member(s)", emptied.len());
    assert_eq!(
        0, live_after,
        "{live_after} live entries remain after every member was removed"
    );
    assert_eq!(
        3, tombstoned_after,
        "DENOMINATOR: {tombstoned_after} tombstone entries for three removals, so EXISTS below is \
         not being asked in the presence of the thing it must ignore"
    );
    assert_eq!(
        0, exists,
        "EXISTS answered {exists} for a key whose every member was removed -- which is #2025's \
         defect exactly, arriving from the tombstone entry instead of the resident map"
    );
    assert!(
        emptied.is_empty(),
        "SMEMBERS returned {} member(s) for an emptied key",
        emptied.len()
    );
}

/// A REMOVAL RETAINS ONE ENTRY, AND A ROUND THAT DOES NOT REWRITE THE WHOLE CONTAINER KEEPS IT.
///
/// # WHY THE FIXTURE IS OVER THE BATCH CAP, WHICH IT DID NOT USED TO BE
///
/// This test was `a_removal_retains_one_entry_and_nothing_yet_collects_it` over EIGHT members, and
/// both halves of that name were true when it was written: nothing in production called the fold, so
/// no round could collect a tombstone whatever it rewrote. A round now does -- the compaction round
/// censuses the container's entries, asks `container_membership::tombstones_collectable`, and drops
/// the tombstone entries when the rewrite was total -- so an eight-member container is collected and
/// the old claim is false.
///
/// The fixture is therefore over `CONTAINER_BATCH_ELEMENT_CAP`, which makes the round seal TWO
/// batches and the rule decline: a tombstone dropped into batch one is not seen by batch two. That
/// keeps this test on the subject it was built for -- the entry COST, and the CARRY-OVER across
/// `bucket_map.clear()` -- rather than turning it into a second copy of the collection guard.
/// `container_tombstone_wiring::a_total_round_collects_a_containers_tombstones_and_a_split_round_does_not`
/// drives the other verdict, and drives both in ONE round so neither is the gate answering the same
/// way to everything.
///
/// rust-internal: drives SetAdd/SetRemove and counts index entries
#[test]
fn a_removal_retains_one_entry_and_a_split_round_does_not_collect_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, "cte-cost");

    // THE COST IS STATED AS A NUMBER RATHER THAN DESCRIBED. A removal used to FREE an entry and now
    // retains one, so a workload that adds and removes distinct members holds one entry per element
    // ever written rather than one per live element -- until a round rewrites the container whole.
    //
    // OVER THE ELEMENT CAP ON PURPOSE, so the round below splits and the rule declines. Expressed
    // against the constant rather than as a literal, so a change to the cap moves this fixture with it
    // instead of silently making the round single-batch and this test a duplicate of the collecting one.
    const MEMBERS: usize = crate::engine::CONTAINER_BATCH_ELEMENT_CAP + 30;
    const REMOVED: usize = 3;
    let key = "cte-cost-set";
    let members: Vec<Vec<u8>> = (0..MEMBERS)
        .map(|m| format!("member-{m:03}").into_bytes())
        .collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    let (live_before, tombstoned_before) = entry_counts(&engine, "set", key);
    assert_eq!(
        (MEMBERS, 0),
        (live_before, tombstoned_before),
        "the seed produced {live_before} live and {tombstoned_before} tombstone entries"
    );

    for member in members.iter().take(REMOVED) {
        write(
            &engine,
            Command::SetRemove {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    let (live_after, tombstoned_after) = entry_counts(&engine, "set", key);
    println!("=== the cost of a removal, in entries ===");
    println!(
        "  {MEMBERS} members written, {REMOVED} removed: {live_after} live + {tombstoned_after} \
         tombstone = {} entries, where the live membership is {}",
        live_after + tombstoned_after,
        MEMBERS - REMOVED
    );
    assert_eq!(
        MEMBERS - REMOVED,
        live_after,
        "{live_after} live entries for {} live members",
        MEMBERS - REMOVED
    );
    assert_eq!(
        REMOVED, tombstoned_after,
        "{tombstoned_after} tombstone entries for {REMOVED} removals -- one per removal is the cost \
         this change pays, and a different number means removals are not filing one each"
    );
    assert_eq!(
        MEMBERS,
        live_after + tombstoned_after,
        "the total entry count moved from {MEMBERS}, so the cost is not exactly one retained entry \
         per removed element"
    );

    // A COMPACTION ROUND REBUILDS THE INDEX, AND THE TOMBSTONES MUST SURVIVE IT.
    //
    // THE HARD PART, AND IT WAS A REFUTATION BEFORE IT WAS A FIX. `compact_shard_blocks` calls
    // `rebuild_bucket_block_ownership` directly, twice; that function does `bucket_map.clear()` and
    // rebuilds from `collect_model_live_block_entries`, which walks the RESIDENT MODEL MAPS. Those hold
    // only live elements -- a removed member is gone from `shard.sets` by construction -- so a rebuild
    // produced one entry per live element and NONE for a removal, and every round erased the entries
    // that kept the removals readable from the pages. A removal was durable in the pages only until the
    // next round.
    //
    // It is fixed by CARRYING the tombstone entries across the clear rather than re-deriving them,
    // because they cannot be re-derived: nothing outside the index knows a removal happened except the
    // WAL, and a rebuild is not a replay. `tombstones_refiled_count` is the instrument, and it is read
    // here rather than inferred -- "the count did not change" is also what a round that never rebuilt
    // reports.
    //
    // A NOTE ON THE FIRST ATTRIBUTION, kept because it was wrong in an instructive way: it blamed
    // `promote_model_maps_to_bucket_index_authority`, whose counters read ZERO over this round. The
    // mechanism was right and the ENTRY POINT was not, and a fix aimed at the wrapper would have left
    // two direct call sites doing it anyway.
    //
    // The source is still asserted, because it is what makes carrying them necessary rather than
    // merely sufficient: the resident map must hold the live members and NONE of the removed ones.
    //
    // AND THE ROUND BELOW IS A SPLIT ONE, so the collection the round can now perform declines and the
    // carry-over is what this measures. The decline is asserted on its own COUNTER rather than inferred
    // from the tombstones surviving: "they survived" is equally what a container the census never
    // looked at produces, and those are different facts.
    crate::engine::reset_container_batch_counts();
    crate::engine::storage_bucket_internals::reset_container_tombstone_collection_counts();
    crate::engine::storage_bucket_internals::reset_tombstones_refiled_count();
    engine
        .compact_shard_blocks(1)
        .expect("the fold round must succeed");
    let (batches, folded) = crate::engine::container_batch_counts();
    let refiled = crate::engine::storage_bucket_internals::tombstones_refiled_count();
    let (live_folded, tombstoned_folded) = entry_counts(&engine, "set", key);
    let (resident_live, resident_holds_removed) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let set = shard.sets.get(key);
        (
            set.map_or(0, |members| members.len()),
            set.map_or(false, |members| {
                members.iter().any(|(member, _)| {
                    (0..REMOVED).any(|m| *member == format!("member-{m:03}").into_bytes())
                })
            }),
        )
    };
    println!(
        "  after a fold round ({batches} batches, {folded} pages folded): {live_folded} live + \
         {tombstoned_folded} tombstone; {refiled} tombstone(s) carried across the rebuild"
    );
    println!(
        "  the resident map the rebuild reads: {resident_live} member(s), holds a removed one = \
         {resident_holds_removed}"
    );
    let (collected, declined) =
        crate::engine::storage_bucket_internals::container_tombstone_collection_counts();
    println!("  collection: {collected} entries collected, {declined} container(s) declined");
    assert!(
        batches > 0 && folded > 0,
        "DENOMINATOR: the round folded nothing ({batches} batches, {folded} pages), so it says \
         nothing about what a round does to a tombstone"
    );
    // THE FIXTURE'S WHOLE POINT, asserted rather than assumed: the round must have SPLIT. A
    // single-batch round over this container would be collected, and every assertion below would then
    // be measuring the wrong verdict -- which is exactly what happened to this test's previous shape
    // when the collection was wired.
    assert!(
        batches > 1,
        "THE ROUND DID NOT SPLIT ({batches} batch). This fixture is over \
         CONTAINER_BATCH_ELEMENT_CAP so that it does; one batch means the cap moved and this test is \
         now driving the COLLECTING verdict under a name that says otherwise."
    );
    assert_eq!(
        0, collected,
        "A SPLIT ROUND COLLECTED {collected} tombstone entries. The rule's second term is that a \
         single batch is required, because a tombstone dropped into batch one is not seen by batch two."
    );
    assert!(
        declined > 0,
        "NOTHING WAS DECLINED, so the tombstones surviving below is not the gate declining -- it would \
         be a container the census never looked at, which has the same appearance and a different cause"
    );
    assert!(
        refiled > 0,
        "DENOMINATOR: the round carried {refiled} tombstones across a rebuild, so either it did not \
         rebuild at all -- in which case the survival below is not evidence of anything -- or the \
         carry-over is not running"
    );
    assert_eq!(
        REMOVED, tombstoned_folded,
        "A FOLD ROUND ERASED THE TOMBSTONE ENTRIES: {tombstoned_folded} survive of {REMOVED}. The \
         rebuild reads the resident model maps, which cannot express a removal, so the entries have to \
         be CARRIED across `bucket_map.clear()`. Without that a removal stops being recorded in the \
         pages at the next compaction round and a page-derived membership resurrects the element."
    );
    assert_eq!(
        MEMBERS - REMOVED,
        live_folded,
        "the rebuild produced {live_folded} live entries for {} live members",
        MEMBERS - REMOVED
    );
    // WHY CARRYING THEM IS NECESSARY, asserted at the source rather than argued.
    assert_eq!(
        MEMBERS - REMOVED,
        resident_live,
        "the resident map holds {resident_live} members where {} are live",
        MEMBERS - REMOVED
    );
    assert!(
        !resident_holds_removed,
        "THE RESIDENT MAP HOLDS A REMOVED MEMBER. If it did, a rebuild from it could re-derive the \
         removal and carrying the entries over would be unnecessary -- so this assertion is what makes \
         the carry-over the only available mechanism rather than one of two."
    );
}

/// rust-internal: drives SetAdd/SetRemove and reads the bucket tombstone index
#[test]
fn a_keys_last_element_still_files_its_object_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, "cte-lastel");

    let key = "cte-last-set";
    let object_id = crate::engine::stable_block_object_id(1, "set", key);
    let members: Vec<Vec<u8>> = (0..2).map(|m| format!("m{m}").into_bytes()).collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }

    let filed = |engine: &TemporalEngine| -> bool {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard
            .bucket_index
            .bucket_map
            .values()
            .any(|bucket| bucket.deleted_object_index.contains(&object_id))
    };

    assert!(
        !filed(&engine),
        "DENOMINATOR: the object id is filed as deleted before anything was removed"
    );

    // NOT THE LAST ELEMENT. The id must NOT be filed -- a deleted element is not a deleted object,
    // and filing it here would make every survivor read as a deleted block ref.
    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: members[0].clone(),
        },
    );
    let after_first = filed(&engine);
    println!("=== the object tombstone ===");
    println!("  after removing one of two, the object id is filed = {after_first}");
    assert!(
        !after_first,
        "the object id was filed after ONE of two elements was removed"
    );

    // THE LAST ELEMENT. Now it must be filed -- and this is the half a retained tombstone entry would
    // have broken: the filter that decides it asks whether any page still carries the id, and the
    // removed element's own tombstone would have answered yes forever. It asks about LIVE pages.
    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: members[1].clone(),
        },
    );
    let after_last = filed(&engine);
    let (live, tombstoned) = entry_counts(&engine, "set", key);
    println!(
        "  after removing the last, the object id is filed = {after_last} ({live} live, \
         {tombstoned} tombstone entries)"
    );
    assert_eq!(
        0, live,
        "{live} live entries remain, so this is not the last-element case"
    );
    assert_eq!(
        2, tombstoned,
        "DENOMINATOR: {tombstoned} tombstone entries, so the filter below is not being tested in \
         the presence of the entries that would defeat it"
    );
    assert!(
        after_last,
        "THE OBJECT ID WAS NOT FILED AT THE KEY'S LAST ELEMENT. The retained tombstone entries \
         answered the `any(|page| page.object_id() == id)` filter, so the object never reads as \
         deleted -- and `object_manager::runtime_report` asks that index per page."
    );
}

/// rust-internal: drives a SetRemove that matches nothing
#[test]
fn a_removal_that_matched_nothing_writes_no_tombstone_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, "cte-nomatch");

    let key = "cte-nomatch-set";
    write(
        &engine,
        Command::SetAdd {
            key: key.to_string(),
            member: b"present".to_vec(),
        },
    );
    let (live_before, tombstoned_before) = entry_counts(&engine, "set", key);
    assert_eq!(
        (1, 0),
        (live_before, tombstoned_before),
        "the seed produced {live_before} live and {tombstoned_before} tombstone entries"
    );

    // A MEMBER THAT WAS NEVER ADDED. The removal matches no entry, so it removed no element -- and a
    // tombstone filed for it would state a removal that never happened, which a derivation would then
    // apply to an element that may later be added for real.
    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: b"never-added".to_vec(),
        },
    );
    let (live_after, tombstoned_after) = entry_counts(&engine, "set", key);
    println!("=== a removal that matched nothing ===");
    println!("  {live_after} live, {tombstoned_after} tombstone entries");
    assert_eq!(
        1, live_after,
        "the removal of an absent member took a live entry with it"
    );
    assert_eq!(
        0, tombstoned_after,
        "{tombstoned_after} tombstone entries were filed for a removal that matched nothing"
    );

    // AND THE PRESENT MEMBER IS THE CONTROL: removing it DOES file one, so the zero above is a
    // consequence of the miss rather than of tombstones not working in this fixture at all.
    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: b"present".to_vec(),
        },
    );
    let (live_control, tombstoned_control) = entry_counts(&engine, "set", key);
    println!("  CONTROL, removing the member that was there: {live_control} live, {tombstoned_control} tombstone");
    assert_eq!(
        1, tombstoned_control,
        "CONTROL: removing a member that WAS present filed {tombstoned_control} tombstones, so the \
         zero above proves nothing"
    );
}

/// rust-internal: drives SetAdd/SetRemove and reads object_manager::runtime_report
#[test]
fn a_removed_member_does_not_make_its_object_deleted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, "cte-objrep");

    const MEMBERS: usize = 12;
    let key = "cte-report-set";
    let object_id = crate::engine::stable_block_object_id(1, "set", key);
    for m in 0..MEMBERS {
        write(
            &engine,
            Command::SetAdd {
                key: key.to_string(),
                member: format!("member-{m:03}").into_bytes(),
            },
        );
    }
    write(
        &engine,
        Command::SetRemove {
            key: key.to_string(),
            member: b"member-003".to_vec(),
        },
    );

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let report = crate::engine::object_manager::runtime_report(shard);
    let object = report
        .objects
        .iter()
        .find(|object| object.object_id == object_id)
        .expect("the object appears in the runtime report");

    println!("=== the runtime report, after removing one of {MEMBERS} ===");
    println!(
        "  live_block_ref_count {} | object: block_refs {} hot {} cold {} deleted {} | deleted={} \
         residency={}",
        report.live_block_ref_count,
        object.block_ref_count,
        object.hot_block_ref_count,
        object.cold_block_ref_count,
        object.deleted_block_ref_count,
        object.deleted,
        object.residency
    );

    // DENOMINATOR: the tombstone has to be in the index for this report to be about anything.
    let (live, tombstoned) = entry_counts(&engine, "set", key);
    assert_eq!(
        (MEMBERS - 1, 1),
        (live, tombstoned),
        "the fixture holds {live} live and {tombstoned} tombstone entries"
    );

    assert!(
        !object.deleted,
        "ONE REMOVED MEMBER MADE THE WHOLE OBJECT READ AS DELETED. `object.deleted |= page.deleted` \
         said it from a single page, which was right while the only writer of that flag deleted an \
         object's pages together and is wrong the moment one removed member carries it."
    );
    assert_eq!(
        1, object.deleted_block_ref_count,
        "the tombstone is not counted as a deleted block ref ({} of them), which it is",
        object.deleted_block_ref_count
    );
    assert_eq!(
        MEMBERS - 1,
        report.live_block_ref_count,
        "live_block_ref_count is {} where {} entries are live -- the field is named `live` and a \
         tombstone is not one",
        report.live_block_ref_count,
        MEMBERS - 1
    );
    assert_ne!(
        "deleted", object.residency,
        "the object's residency reads deleted while {} of its refs are live",
        MEMBERS - 1
    );
}

/// rust-internal: drives SetAdd/SetRemove/SetAdd on ONE member
#[test]
fn a_re_add_clears_the_tombstone_entry_so_churn_on_one_element_does_not_accumulate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, "cte-readd");

    // THE MECHANISM IS NOT IN THIS CHANGE, AND THAT IS WHY IT NEEDS A GUARD.
    //
    // `upsert_bucket_index_block_inner`'s `retain` matches on `(object_key, model_id, component)`, and
    // a tombstone entry carries the component it removed -- so writing the element back drops the
    // tombstone as a side effect of the upsert's existing behaviour. Nobody wrote that for tombstones;
    // it falls out, and it is the difference between "a removal costs one entry per DISTINCT element
    // ever removed" and "one entry per REMOVAL". Nothing else in the tree would notice if it stopped.
    //
    // It is also why the orphaned tombstone PAGE is harmless: the re-add's page is appended after it,
    // so even reachable it would be outranked by append position.
    let key = "cte-readd-set";
    let member = b"the-member".to_vec();
    const CYCLES: usize = 5;

    // BOTH PATHS, AND THE FIRST VERSION OF THIS GUARD COVERED ONLY ONE.
    //
    // `upsert_bucket_index_block_inner` branches on whether `object_block_lookup` is established. The
    // lookup-enabled arm removes only what the lookup NAMES, and a tombstone entry is never in it --
    // `insert_object_block_lookup` returns early on a deleted page -- so that arm left the tombstone
    // behind and the entry count grew once per removal forever. The other arm is a `retain` matching
    // on the component and takes the tombstone with it.
    //
    // This fixture originally ran with an empty lookup, took the second arm, and PASSED while the
    // first was wrong. It was `a_bucket_holding_one_block_holds_no_node`, on a hash, with the lookup
    // established, that failed. So the state is now forced explicitly and asserted, and the cycles run
    // under each -- because "a guard covering one of two live copies lets the other keep the bug" is
    // exactly what happened.
    for established in [false, true] {
        if established {
            // A LIVE DECOY FIRST, because a rebuild cannot establish a lookup out of nothing:
            // `insert_object_block_lookup` returns early on a deleted page, so rebuilding over a
            // store whose only entry is a tombstone produces an EMPTY lookup and the second half
            // would silently re-run the first half's branch. That is what happened on the first
            // attempt, and the assertion below is what said so.
            write(
                &engine,
                Command::SetAdd {
                    key: "cte-readd-decoy".to_string(),
                    member: b"decoy".to_vec(),
                },
            );
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("shard is loaded");
            shard.bucket_index.rebuild_object_block_lookup();
        }
        let is_established = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            !shard.bucket_index.object_block_lookup.is_empty()
        };
        println!("=== lookup established = {is_established} (wanted {established}) ===");
        if established {
            assert!(
                is_established,
                "the rebuild did not establish the lookup, so the arm this half exists to cover was \
                 not taken and the other arm was tested twice"
            );
        }
        run_cycles(&engine, key, &member, CYCLES, is_established);
    }

    let (live, tombstoned) = entry_counts(&engine, "set", key);
    println!("  after both paths: {live} live, {tombstoned} tombstone entries");
    assert_eq!(
        (0, 1),
        (live, tombstoned),
        "both paths together left {live} live and {tombstoned} tombstone entries"
    );
}

/// The add/remove cycles, run once per branch of the upsert's lookup test.
fn run_cycles(
    engine: &TemporalEngine,
    key: &str,
    member: &[u8],
    cycles: usize,
    is_established: bool,
) {
    let member = member.to_vec();
    for cycle in 0..cycles {
        write(
            engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        );
        let (live_added, tombstoned_added) = entry_counts(engine, "set", key);
        assert_eq!(
            (1, 0),
            (live_added, tombstoned_added),
            "cycle {cycle}: after the add there are {live_added} live and {tombstoned_added} \
             tombstone entries. A tombstone surviving the re-add is the accumulation this asserts \
             against: `upsert_bucket_index_block_inner`'s retain matches on component and must have \
             taken it."
        );

        write(
            engine,
            Command::SetRemove {
                key: key.to_string(),
                member: member.clone(),
            },
        );
        let (live_removed, tombstoned_removed) = entry_counts(engine, "set", key);
        assert_eq!(
            (0, 1),
            (live_removed, tombstoned_removed),
            "cycle {cycle}: after the removal there are {live_removed} live and \
             {tombstoned_removed} tombstone entries"
        );
    }

    let (live, tombstoned) = entry_counts(engine, "set", key);
    println!(
        "  {cycles} add/remove cycles with the lookup established = {is_established}: \
         {live} live, {tombstoned} tombstone"
    );
    assert_eq!(
        (0, 1),
        (live, tombstoned),
        "with the lookup established = {is_established}, {cycles} cycles left {live} live and \
         {tombstoned} tombstone entries. ONE is the whole point: if this grows with the cycle count \
         then churn on a single element leaks an entry per removal, and the cost of this change is \
         unbounded in the number of WRITES rather than bounded by the number of distinct elements."
    );
}

/// rust-internal: drives ZSetAdd twice at different scores
#[test]
fn a_score_change_tombstones_the_element_key_it_left() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, "cte-score");

    // A SCORE CHANGE IS A REMOVAL, and it is the arm least likely to be recognised as one. A zset's
    // element key is the biased score followed by the member, so moving a member's score REMOVES the
    // old element key and writes a new one. Leave the old key untombstoned and a membership derived
    // from the pages holds the member TWICE, at both scores.
    let key = "cte-score-zset";
    let member = b"the-member".to_vec();
    let old_component = crate::engine::execute_on_shard::zset_component(
        crate::engine::execute_on_shard::zset_score_bits(1.0),
        &member,
    );
    let new_component = crate::engine::execute_on_shard::zset_component(
        crate::engine::execute_on_shard::zset_score_bits(2.0),
        &member,
    );
    assert_ne!(
        old_component, new_component,
        "DENOMINATOR: the two scores spell one component, so there is no element key to leave"
    );

    write(
        &engine,
        Command::ZSetAdd {
            key: key.to_string(),
            member: member.clone(),
            score: 1.0,
        },
    );
    let (live_seed, tombstoned_seed) = entry_counts(&engine, "zset", key);
    assert_eq!(
        (1, 0),
        (live_seed, tombstoned_seed),
        "the seed produced {live_seed} live and {tombstoned_seed} tombstone entries"
    );

    write(
        &engine,
        Command::ZSetAdd {
            key: key.to_string(),
            member: member.clone(),
            score: 2.0,
        },
    );

    let (live, tombstoned) = entry_counts(&engine, "zset", key);
    let tombstoned_names = tombstoned_components(&engine, "zset", key);
    println!("=== a zset score change ===");
    println!("  {live} live, {tombstoned} tombstone entries");
    println!("  the element key it left:   {old_component}");
    println!("  the element key it took:   {new_component}");
    println!("  tombstoned components:     {tombstoned_names:?}");

    assert_eq!(
        1, live,
        "{live} live entries after a score change, and one member at one score is one element"
    );
    assert_eq!(
        1, tombstoned,
        "A SCORE CHANGE LEFT {tombstoned} TOMBSTONES, NOT ONE. Zero means the old element key is \
         still live in the pages and a page-derived membership holds this member at BOTH scores."
    );
    assert!(
        tombstoned_names.contains(&old_component),
        "the tombstone does not name the element key the score change LEFT ({old_component}); it \
         names {tombstoned_names:?}"
    );
    assert!(
        !tombstoned_names.contains(&new_component),
        "the tombstone names the element key the score change TOOK, so the pages say the member is \
         gone at the score it now holds"
    );

    // AND THE READERS AGREE: one member, at the new score.
    let listed = match read(
        &engine,
        Command::ZSetCard {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("ZSetCard answered {other:?}"),
    };
    println!("  ZCARD answers {listed}");
    assert_eq!(
        1, listed,
        "ZCARD answers {listed} for one member held at one score"
    );
}
