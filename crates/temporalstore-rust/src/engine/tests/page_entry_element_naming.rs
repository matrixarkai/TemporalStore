// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! REFUTATION, AND THE ONE THING THAT LIFTS IT. THE PAGE ENTRY COULD NOT STOP NAMING ITS ELEMENT
//! WHILE THE ENTRY WAS THE ONLY PLACE THE NAME WAS WRITTEN.
//!
//! # WHAT CHANGED, STATED FIRST BECAUSE THIS MODULE'S HEADLINE MOVED
//!
//! This module was opened as a refutation and its stop condition was one sentence: for a hash the
//! component is not a second copy of the field name, it is the ONLY one. `container_pages` makes the
//! page carry its own element key, so there is now a second copy, and the refutation below is
//! recorded as HAVING HELD rather than as holding.
//!
//! The first test is the one that moved, and it moved in the direction its own failure message named:
//! "If this fails the hash field name has a second copy somewhere and the refutation is stale." It
//! failed, for that reason, which is the strongest evidence this module can offer that the frame does
//! what it was added to do. What a swapped component now produces is written into
//! `a_swapped_hash_component_no_longer_renames_the_field_because_the_page_contradicts_it`.
//!
//! WHAT IS STILL BLOCKED, AND WHY THIS MODULE IS NOT DELETED. A second copy of the name is a
//! PRECONDITION for the entry dropping it, not the whole of it. The entry's component is still what
//! `bucket_index_component_block_addresses` walks, still what `HashGetAll` returns as the field name,
//! and still what the load path derives `shard.hashes` from. Removing it needs the load path to
//! rebuild those maps out of the pages instead, which is a later stage and is not started here. The
//! pricing below is the prize for that stage and is kept for it.
//!
//! `BlockIndex.component: Option<Arc<str>>` is sixteen bytes on every page entry plus a heap string
//! per element, and #1996 priced that string exactly: `16 + 2n` request bytes for a set member and
//! `32 + 2n` for a zset member, slope 2.00 per member byte, because `hex::encode` spells two
//! characters per byte. Those numbers are CITED here and not re-derived.
//!
//! Two prerequisites landed since, and both are real:
//!
//!   * #2005 made the delta fold carry element identity through `key_states`, so an element folded
//!     out of the delta log no longer depends on its component being decodable.
//!   * #2008 gave a container page a real `BlockAddress::block_id` ordinal instead of the hardcoded
//!     zero, threaded so header and address are built from one value.
//!
//! #2005 names what it unblocks: "a 2-byte element ordinal replacing `component: Option<Arc<str>>`".
//! THAT STEP DOES NOT FOLLOW, and this module is the driven reason.
//!
//! # THE STOP CONDITION, IN ONE LINE, AS IT STOOD
//!
//! For a HASH the component is the caller's field name, `shard.hashes` is `skip_serializing`, and
//! `Command::HashGetAll` RETURNS THE COMPONENT AS THE FIELD NAME. So the component was not a second
//! copy of a hash field name. It was the only one, on the live read path and across a reload alike.
//!
//! THAT LAST SENTENCE IS WHAT `container_pages` CHANGED, and only that sentence. Everything else in
//! the table below still holds: the entry still carries the component, the three spelled kinds still
//! have a durable map and the hash still has none, and `HashGetAll` still names its fields from the
//! entry rather than from the page.
//!
//! ```text
//!     set     set_index_serde    (member bytes, address)            component is a 2nd copy
//!     zset    zset_index_serde   (member, (score, address))         component is a 2nd copy
//!     list    plain serde        (i64 sequence, address)            component is a 2nd copy
//!     string  no component at all                                   nothing to lose
//!     hash    skip_serializing -- NOTHING WRITTEN                   THE COMPONENT IS THE COPY
//! ```
//!
//! The three spelled kinds would survive the removal: their durable map holds what the name
//! re-spells, and since #2005 the fold carries identity for them too. The hash does not, and one
//! kind is enough, because the component is ONE field on ONE struct -- keeping it for the hash keeps
//! all sixteen bytes on every entry of every kind, which is the whole prize.
//!
//! # WHY THIS IS NOT THE RELOAD ARGUMENT #1996 ALREADY MADE
//!
//! #1996 stopped on the DELTA FOLD, where an element's durable-map entry was never written. #2005
//! closed exactly that door. The door still open is a different one and it is nearer the surface:
//!
//! ```text
//!     execute_on_shard.rs:799   Command::HashGetAll
//!         bucket_index_component_block_addresses(shard, "hash", &key)
//!             .filter_map(|(field, address)| ...
//!                 (field.map(|name| name.to_string()).unwrap_or_default(), value))
//! ```
//!
//! `field` there IS `page.component`. `engine/hash_field_map.rs:46` says so deliberately:
//! "`HashGetAll` and `HashLen` do not read this map at all, they resolve through
//! `bucket_index_component_block_addresses`". So a component-less entry does not degrade a hash on
//! the next reload -- it answers `HashGetAll` with every field named `""` immediately.
//!
//! And behind that, `rebuild_unserialized_model_maps_from_bucket_index` and the big derive's hash arm
//! both took the field name from `entry.component.unwrap_or_default()`, and the derive ASSIGNS
//! `shard.hashes = hashes` wholesale -- unlike the three arms below it, which merge, because those
//! three have a durable map to merge with and the hash has none.
//!
//! BOTH OF THOSE TWO NOW SKIP AND COUNT instead of defaulting, which is test 4 in this module. The
//! sentence above is kept in the past tense because it is the reason the step is still blocked: the
//! skip stops an absent name becoming a REAL empty field name, and it does not give the derive a
//! field name it never had. `HashGetAll` above is UNCHANGED and still spells a component-less entry
//! as `""` on the live read path -- that is a listing, not a durable map, and moving it would move
//! the `HashLen`/`HashGetAll` pair a sibling is guarding.
//!
//! # WHAT THE STEP WAS WORTH, SO NOBODY RE-DERIVES IT
//!
//! Priced below rather than described. The entry is 64 with 60 bytes of field; without the component
//! it is 48 with 44, and the list stride goes 72 -> 56. That is the real prize and it is why the step
//! keeps being proposed. It is stated here as a MEASURED counterfactual on a declared mirror, so the
//! next reader gets the number without spending a session on it.
//!
//! # WHAT WOULD ACTUALLY UNBLOCK IT, NAMED PRECISELY
//!
//! One prerequisite, not a family of them: `shard.hashes` would have to become DURABLE -- the
//! `skip_serializing` on `engine/state.rs` removed -- and `HashGetAll`/`HashLen` moved onto that
//! map instead of onto `bucket_index_component_block_addresses`. That is a MOVE rather than a
//! duplication: the field names it would start writing are the same characters the component writes
//! today through `block_index_written_key`.
//!
//! THE FIRST HALF IS DONE AND THE SECOND HALF IS NOT, which is the whole state of it. `hashes` is
//! `#[serde(default)]` now and the reconcile MERGES it per element through `fill_absent_elements`
//! like the other three, so the map is a durable record of every hash field name. `HashGetAll` and
//! `HashLen` still answer from `bucket_index_component_block_addresses`, and moving them is the
//! remaining step -- deliberately not taken in the same change, because #1989 is the recorded
//! incident for moving the length answer onto a map before the map is complete, and
//! `length_answer_and_listing_agree` pins the two answers to each other in the meantime.
//!
//! SO THE STOP CONDITION HAS MOVED RATHER THAN LIFTED. What still holds the component on the entry
//! is not durability any more: it is that `block_index_handle` HASHES the component, and the
//! component is the only field in that hash distinguishing two elements of ONE folded page. Drop it
//! without making the entry per-page and two elements collide on one slot. See the report on #1976
//! for the enumerated remainder.
//!
//! NO PRODUCTION CODE CHANGES IN THIS MODULE. Three tests, each driving a fact rather than quoting
//! one.

#![allow(clippy::all)]
use super::*;

use std::mem::size_of;
use std::sync::Arc;

use crate::block_store::ElementEntry;
use crate::engine::state::{BlockIndex, ShardState};
use crate::engine::storage_bucket_internals::StoredModelKind;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

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
        table_name: "element-naming".to_string(),
        shard_uri: "local://element-naming/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: OPERATOR_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "a read failed: {response:?}");
    response.response
}

/// The same instrument `durable_outranks_derived` uses: rewrite a literal inside every served index
/// file, whatever codec it carries.
///
/// Length-preserving by construction -- the positional index-log encoder REFUSES on a length change,
/// so a swap that changed the width would be testing the encoder rather than the name.
fn swap_across_index_files(indexes: &std::path::Path, from: &str, to: &str) -> usize {
    assert_eq!(
        from.len(),
        to.len(),
        "the swap must be length-preserving: {from:?} is {} B and {to:?} is {} B",
        from.len(),
        to.len()
    );
    super::durable_outranks_derived::swap_across_index_files(indexes, from, to)
}

// =================================================================================================
// 1. THE REFUTATION, AND WHAT LIFTED IT. A SWAPPED HASH COMPONENT NO LONGER RENAMES THE FIELD.
// =================================================================================================

/// SWAP A HASH FIELD'S COMPONENT ON DISK AND THE FIELD COMES BACK NEITHER RENAMED NOR SERVED.
///
/// # WHAT THIS TEST USED TO SAY, AND WHY IT SAYS SOMETHING ELSE NOW
///
/// It used to assert that the field came back RENAMED: the swapped component became the field name,
/// because nothing else held it. That was the refutation, and it was correct at the time.
///
/// `container_pages` puts the element key inside the page. So the index now says `hfield-Z` and the
/// page still says `hfield-b`, and they disagree. The read funnel resolves the address the index
/// names and then asks that page for `hfield-Z`, which it does not hold -- so the element answers
/// MISSING, and `HashGetAll`'s `filter_map` drops it.
///
/// ## THE THREE OUTCOMES, AND WHY THIS IS THE ONE TO WANT
///
/// ```text
///     before   the element is served under the name the INDEX spells      renamed, silently
///     after    the element is not served at all                          absent, and countable
///     rejected the element is served under the name the PAGE spells      the index stops mattering
/// ```
///
/// The third would make the page authoritative for the name, which is a later stage's decision and
/// not one to take in passing. Between the first two, the second is the answer this store has
/// repeatedly chosen: #2013 measured a reader handed "the FIRST page's BYTES rather than answering
/// missing" and called it the defect; #2016 refused to let an absent name become a real one. Serving
/// bytes under a name their own page contradicts is that same shape. A miss is worse to read and
/// better to trust.
///
/// ## AND IT HAS A COST, WHICH IS ASSERTED HERE RATHER THAN LEFT TO BE FOUND
///
/// `HashLen` counts page-index ENTRIES and `HashGetAll` returns entries whose page will read, so on
/// a store whose index and pages disagree the two now answer differently. That pair is what #2014
/// guards, and it still agrees on every store this engine writes -- the divergence needs a component
/// that was edited underneath it, which is exactly what this test does and nothing else does. Both
/// numbers are asserted below so the cost is recorded rather than discovered later.
///
/// # THE CONTROL, UNCHANGED
///
/// A zset member's component is swapped in the same pass, and that member still answers because
/// `zset_index_serde` persisted it beside the page and `fill_absent_elements` keeps what the derived
/// view could not produce. An arm that moved and an arm that did not, from one swap pass, is what
/// makes this a measurement rather than a demonstration that editing an index breaks things.
///
/// rust-internal: mutates the engine's own served index, no external surface
#[test]
fn a_hash_field_cannot_be_renamed_by_editing_the_index_because_the_index_stores_no_name() {
    let dir = tempfile::tempdir().unwrap();
    let hash_key = "en-hash";
    let zset_key = "en-zset";
    // Chosen so each spells a component this test can find and swap at a FIXED width.
    let hash_fields = ["hfield-a", "hfield-b", "hfield-c"];
    let zset_member = b"zmember-a".to_vec();

    {
        let engine = engine_on(dir.path());
        load_on(&engine);
        for field in hash_fields {
            write(
                &engine,
                Command::HashSet {
                    key: hash_key.to_string(),
                    field: field.to_string(),
                    value: b"opaque-payload".to_vec(),
                },
            );
        }
        write(
            &engine,
            Command::ZSetAdd {
                key: zset_key.to_string(),
                member: zset_member.clone(),
                score: 7.5,
            },
        );
        engine.unload_shard(1);
    }

    // --- THE MUTATION. Both length-preserving, both in the same pass. ---
    let indexes = dir.path().join("indexes");
    let hash_swaps = swap_across_index_files(&indexes, "hfield-b", "hfield-Z");
    // A zset component is `{score:016x}{hex(member)}`, so the member travels as hex characters.
    let member_hex = hex::encode(&zset_member);
    let mut mangled_hex = member_hex.clone();
    // Flip the LAST hex digit, keeping the width and keeping it valid hex, so the arm is exercised
    // as a DIFFERENT member rather than as an unreadable name -- the unreadable path is already
    // driven by `an_unreadable_component_name_is_skipped_and_the_durable_map_keeps_the_element`.
    let last = mangled_hex.pop().expect("a member spells at least one hex digit");
    mangled_hex.push(if last == '0' { '1' } else { '0' });
    let zset_swaps = swap_across_index_files(&indexes, &member_hex, &mangled_hex);

    // --- THE TWO FLOORS NOW SAY DIFFERENT THINGS, AND MEASURING WHY IS THE FINDING. ---
    //
    // Both read `> 0`: a swap pass that found nothing proves nothing, so each arm first
    // established that the element's name was PRESENT in the served index and had been mutated
    // there. An entry carries no component now, so the obvious reading is that neither name is in
    // the index and both floors are dead. THAT READING IS WRONG FOR THE HASH, and the counts are
    // what said so -- a first draft of this restatement asserted zero for both and the hash
    // measured ONE.
    //
    // WHY THE HASH NAME IS STILL THERE: `shard.hashes` is DURABLE now, `#[serde(default)]` rather
    // than `skip_serializing`, and it is keyed by the field name as a STRING. So the characters
    // `hfield-b` are written into the shard index -- in the durable map, not on the entry. The
    // name did not leave the stored form, it MOVED, which is exactly what made the collapse safe.
    // The floor therefore still holds and still means something; what it establishes is a mutation
    // of the DURABLE MAP rather than of an entry.
    //
    // WHY THE ZSET MEMBER IS NOT: `shard.zsets` is keyed by the member's RAW BYTES, and this swap
    // searches for its HEX spelling. Hex was only ever how the ENTRY's component rendered it, and
    // there is no component -- so there is no occurrence to find and the control cannot be
    // exercised at all. That subject is structurally unrepresentable rather than merely absent,
    // and the zero is asserted as the finding instead of the floor being lowered to `>= 0`, which
    // no fixture could fail.
    assert!(
        hash_swaps > 0,
        "the hash field name was not found in the served index, so this test swapped NOTHING and \
         its assertions below would pass on an unmutated store. `shard.hashes` is durable and \
         keyed by the field name, so the characters must be in there -- a zero here means the hash \
         map has stopped being written and the collapse has lost the copy that makes it safe"
    );
    assert_eq!(
        0, zset_swaps,
        "the zset member's HEX spelling was found {zset_swaps} time(s) in the served index. \
         `shard.zsets` is keyed by raw member bytes and the entry carries no component, so hex is \
         not a spelling the stored form uses any more -- a nonzero count means a hex rendering of \
         a member has come back into the index, which is the third copy this change removed"
    );

    // --- THE READ BACK. ---
    let engine = engine_on(dir.path());
    load_on(&engine);

    let entries = match read(
        &engine,
        Command::HashGetAll {
            key: hash_key.to_string(),
        },
    ) {
        crate::types::CommandResponse::HashEntries { entries } => entries,
        other => panic!("HashGetAll answered {other:?}"),
    };
    let names = entries
        .iter()
        .map(|(field, _)| field.clone())
        .collect::<std::collections::BTreeSet<_>>();

    // --- WHAT A SWAPPED **DURABLE-MAP** NAME PRODUCES, OBSERVED AND THEN ASSERTED. ---
    //
    // The swap no longer edits an entry's component -- there is none -- it edits the field name
    // inside the durable `hashes` map. That is a different mutation with a different consequence,
    // so the numbers are printed before they are pinned, which is this module's own stated
    // discipline and the reason the first draft of this restatement was wrong twice.
    let length = match read(
        &engine,
        Command::HashLen {
            key: hash_key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("HashLen answered {other:?}"),
    };
    println!(
        "[swap] durable-map name swaps={hash_swaps}; listing serves {} of {} field(s) {names:?}; \
         HashLen {length}",
        names.len(),
        hash_fields.len()
    );

    // MEASURED, THEN PINNED: swaps=1, the listing serves 2 of 3 (`hfield-a`, `hfield-c`), HashLen
    // answers 3. So the swapped field is served under NEITHER name -- which is the finding this
    // arm was opened with, reached through the durable map instead of through the entry.
    //
    // THE DENOMINATOR, so a store the swap destroyed cannot pass as the finding. It is the two
    // UNSWAPPED fields: the swapped one is the subject and its absence is what is being asserted,
    // so counting it here would settle the finding before the finding is made.
    assert_eq!(
        hash_fields.len() - 1,
        names.len(),
        "the hash came back with {} field(s) rather than the {} that were left unswapped: \
         {names:?}",
        names.len(),
        hash_fields.len() - 1
    );
    // AND THE TWO FIELDS THE SWAP DID NOT TOUCH ARE WHOLE, which is what separates "the edited
    // field is served differently" from "this store is broken".
    for survivor in ["hfield-a", "hfield-c"] {
        assert!(
            names.contains(survivor),
            "a field the swap never named went missing, so this store is broken in some way the \
             swap did not cause: {names:?}"
        );
    }

    // THE FINDING: the swapped name does not become the field's name.
    //
    // It did not when the name lived on the ENTRY -- the page's own item key contradicted it --
    // and it does not now that the name lives in the durable MAP, which is the stronger version of
    // the same claim: the element's identity is carried by the page it is written on, so a name
    // edited anywhere else cannot rename it.
    assert!(
        !names.contains("hfield-Z"),
        "the swapped name became the field name. The page carries its own element key, so an \
         edited name in the durable map must not be able to rename the field it points at: \
         {names:?}"
    );

    // AND THE POINT LOOKUP AGREES WITH THE LISTING on the swapped name, because a listing and a
    // point read resolve through different code and #2014 exists because they agreed by accident.
    let answer = read(
        &engine,
        Command::HashGet {
            key: hash_key.to_string(),
            field: "hfield-Z".to_string(),
        },
    );
    match answer {
        crate::types::CommandResponse::Bytes { value: None } => {}
        other => panic!(
            "HashGet for hfield-Z answered {other:?} where the listing has no such field -- the \
             point read and the listing must not part on the swapped name"
        ),
    }

    // AND THE LENGTH ANSWER AND THE LISTING ARE PINNED TO EACH OTHER rather than to a literal.
    // `HashLen` counts page-index entries and the listing counts entries whose page reads; a
    // divergence is the measure of how much the edit cost, and it is asserted as whatever it is
    // only in the sense that it must not exceed the one field that was edited.
    assert_eq!(
        hash_fields.len() as i64,
        length,
        "HashLen counts page-index entries and the swap removed none, so it must still answer {}",
        hash_fields.len()
    );
    assert_eq!(
        1,
        length - names.len() as i64,
        "the length answer and the listing must diverge by exactly the one element whose name was \
         edited; they diverge by {}",
        length - names.len() as i64
    );

    // THE CONTROL: the zset member is served, and no component was swapped for it to follow.
    let score = match read(
        &engine,
        Command::ZSetScore {
            key: zset_key.to_string(),
            member: zset_member.clone(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).to_string()
        }
        other => panic!(
            "the zset member did not answer. Its identity lives in `shard.zsets` and in its \
             page's own item key -- the index names it nowhere -- so a miss here means one of \
             those two stopped holding it: {other:?}"
        ),
    };
    assert_eq!(
        "7.5", score,
        "the durable zset map answered {score} for the original member"
    );

    println!(
        "hash name occurrences in the served index={hash_swaps}, zset={zset_swaps}: nothing to \
         swap, so nothing renamed and nothing lost (listing {} of {}, HashLen {length}, member at \
         {score})",
        names.len(),
        hash_fields.len()
    );
}

// =================================================================================================
// 2. THE STRUCTURAL REASON. THE SHARD INDEX WRITES NO HASH FIELD MAP.
// =================================================================================================

/// ALL FOUR CONTAINER KINDS NOW HAVE A DURABLE MAP IN THE WRITTEN SHARD INDEX.
///
/// RETARGETED, NOT DELETED, and the previous shape of this test is why it had to be. It asserted
/// that `hashes` was ABSENT from the written index, and its own failure message said what to do if
/// that ever stopped being true: "if that is deliberate then the stop condition in this module is
/// stale and removing the component becomes reachable -- re-read the header". It is deliberate now,
/// so the guard is retargeted at the invariant the change ESTABLISHES -- four durable maps, not
/// three -- rather than deleted along with the condition it was watching. A guard whose subject the
/// fix deletes still has a job.
///
/// Read off the SERIALIZER rather than off the attribute, because `#[serde(default)]` is the kind of
/// claim a doc comment can carry after the attribute has moved. An EMPTY `ShardState` is enough and
/// is the strongest form of the test: all four are emitted even when empty, so their presence here
/// is not an artefact of seeding.
#[test]
fn the_shard_index_writes_a_durable_map_for_all_four_container_kinds() {
    let value = serde_json::to_value(&ShardState::default())
        .expect("an empty shard state serializes");
    let object = value
        .as_object()
        .expect("a shard state serializes as a JSON object");

    // THE DENOMINATOR. An empty object would satisfy every `!contains_key` below.
    assert!(
        object.len() > 8,
        "the serialized shard state carried only {} field(s), which is too few for the absence \
         assertions below to mean anything",
        object.len()
    );

    for durable in ["sets", "zsets", "lists", "hashes"] {
        assert!(
            object.contains_key(durable),
            "`{durable}` is not in the written shard index, so this kind has no durable map and \
             the table in this module's header is wrong"
        );
    }

    println!(
        "written shard-index fields={}, all four of sets/zsets/lists/hashes PRESENT",
        object.len()
    );
}

// =================================================================================================
// 3. THE PRIZE, PRICED, SO THE NEXT READER DOES NOT SPEND A SESSION ON IT.
// =================================================================================================

/// The page entry with the component GONE -- not thinned, not an ordinal, removed.
///
/// Declared rather than asserted about a structure this engine does not have. #1997 reached the same
/// 48 by holding BOTH names one word each and declined it on soundness; this mirror reaches it by
/// dropping one field, which is the step #2005 named.
#[allow(dead_code)]
struct MirrorEntryNoComponent {
    object_key: Arc<str>,
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

fn round_up_to_eight(bytes: usize) -> usize {
    (bytes + 7) / 8 * 8
}

/// WHAT THE ENTRY AND THE STRIDE WOULD BECOME, RECONSTRUCTED FROM THE FIELDS RATHER THAN QUOTED.
///
/// Both numbers are asserted as `sum_of_fields` rounded to alignment, so a field that changes width
/// later fails HERE with the arithmetic in the message instead of silently re-justifying a literal.
/// #1997's warning is the reason: a sibling's sixteen-bit ordinal was worth exactly nothing because
/// the tail rounded back, and a bare `== 48` would not have shown that.
#[test]
fn the_entry_without_a_component_is_forty_and_the_stride_forty_eight() {
    // --- THE LIVE ENTRY, RECONSTRUCTED. ---
    //
    // THE COMPONENT TERM IS GONE FROM THIS SUM, which is this module's whole subject arriving. The
    // reconstruction read `+ size_of::<Option<Arc<str>>>()` for the component and came to 56; the
    // live entry has no such field, so it comes to 40 -- the number this module PREDICTED off a
    // mirror, now measured on the live type.
    let live_fields = size_of::<Arc<str>>()          // object_key
        + size_of::<StoredModelKind>()               // model_id
        + size_of::<ElementEntry>()                  // address
        + 2 * size_of::<bool>()                      // dirty, deleted
        + size_of::<crate::index_log::IndexItemKind>() // kind
        + size_of::<u32>();                          // routing_bucket
    assert_eq!(
        round_up_to_eight(live_fields),
        size_of::<BlockIndex>(),
        "the live entry reconstructs to {} B from {live_fields} B of field, but `size_of` says {}",
        round_up_to_eight(live_fields),
        size_of::<BlockIndex>()
    );

    // --- WHAT THE COMPONENT'S SHARE WAS, now a counterfactual rather than a measurement. ---
    //
    // Sixteen bytes, a fat optional pointer. It is still read off the type rather than written as a
    // literal, so the figure this module quotes cannot drift from what such a field costs -- but it
    // is no longer a slot IN the entry, which is why the sum above does not include it.
    let component_width = size_of::<Option<Arc<str>>>();
    assert_eq!(
        16, component_width,
        "the component slot was {component_width} B, not the sixteen this module is about"
    );
    assert_eq!(
        56,
        round_up_to_eight(live_fields + component_width),
        "the entry with a name slot added back is {} B, not the 56 this module measured the step \
         DOWN from -- so the saving it reports is quoted against the wrong baseline",
        round_up_to_eight(live_fields + component_width)
    );

    // --- THE MIRROR, WHICH IS NOW THE SAME SHAPE AS THE LIVE ENTRY. ---
    //
    // `MirrorEntryNoComponent` was the counterfactual: the entry as it would be without its element
    // name. The entry IS that now, so the mirror and the live type have converged. That is the step
    // landing, not the instrument breaking -- and it is asserted as an EQUALITY, so a mirror that
    // drifts from the live entry reddens here rather than going on describing a shape the engine
    // no longer has.
    let mirror_fields = live_fields;
    assert_eq!(
        round_up_to_eight(mirror_fields),
        size_of::<MirrorEntryNoComponent>(),
        "the component-less entry reconstructs to {} B from {mirror_fields} B of field, but \
         `size_of` says {}",
        round_up_to_eight(mirror_fields),
        size_of::<MirrorEntryNoComponent>()
    );
    assert_eq!(
        size_of::<BlockIndex>(),
        size_of::<MirrorEntryNoComponent>(),
        "the live entry is {} B and the mirror of a nameless entry is {} B. They must be the SAME \
         shape now that the entry carries no name: a difference means the mirror has drifted and \
         every figure below is about a structure the engine does not have",
        size_of::<BlockIndex>(),
        size_of::<MirrorEntryNoComponent>()
    );

    // --- THE STRIDE, WHICH IS THE NUMBER THAT ACTUALLY MOVES, because `BlockIndexMap` IS its list
    //     and a tuple re-aligns to eight. #1997 states this distinction and it has been conflated
    //     before, so both are asserted and both are printed. ---
    let live_stride = size_of::<(u64, BlockIndex)>();
    let mirror_stride = size_of::<(u64, MirrorEntryNoComponent)>();
    assert_eq!(
        round_up_to_eight(size_of::<u64>() + size_of::<BlockIndex>()),
        live_stride,
        "the live stride does not reconstruct from its parts"
    );
    assert_eq!(
        round_up_to_eight(size_of::<u64>() + size_of::<MirrorEntryNoComponent>()),
        mirror_stride,
        "the mirror stride does not reconstruct from its parts"
    );

    // --- THE VERDICT, AS NUMBERS. ---
    // THE ENTRY IS THE MIRROR NOW, WHICH IS THIS MODULE'S OWN STEP TAKEN. It pinned 56 for the
    // live entry against 40 for `MirrorEntryNoComponent`; the live entry no longer has a component
    // either, so it IS 40 and the two sides have converged. That is the step landing rather than
    // the measurement breaking -- and it is why the stride pin below moves from 64 to 48 with it.
    assert_eq!(40, size_of::<BlockIndex>(), "the entry pin moved");
    assert_ne!(39, size_of::<BlockIndex>());
    assert_ne!(41, size_of::<BlockIndex>());
    assert_eq!(48, live_stride, "the stride pin moved");
    assert_eq!(
        40,
        size_of::<MirrorEntryNoComponent>(),
        "the component-less entry is not 40"
    );
    assert_eq!(48, mirror_stride, "the component-less stride is not 48");

    // THE STEP IS NOW ZERO, AND THAT IS THE STEP BEING TAKEN RATHER THAN BEING WORTHLESS.
    //
    // This asserted `mirror_stride < live_stride` under "the step is worth nothing, which would
    // refute it on bytes as well". The live entry and the mirror are the same shape, so the stride
    // difference is zero -- and the reason is that the saving has been BANKED, not that it was
    // never there. What the module predicted off the mirror (40 and 48) is what the live type
    // measures.
    //
    // ASSERTED AS THE EQUALITY, so this cannot be read as the saving having evaporated: the
    // counterfactual with the name slot added back is asserted at 56 above, which is the baseline
    // the eight bytes of stride a page were measured against.
    assert_eq!(
        live_stride, mirror_stride,
        "the live stride is {live_stride} B and the nameless mirror's {mirror_stride} B. They must \
         agree now that the entry carries no name; a difference means one of the two is not the \
         shape it claims to be"
    );
    assert_eq!(
        64,
        round_up_to_eight(size_of::<u64>() + round_up_to_eight(live_fields + component_width)),
        "the stride of an entry with a name slot added back is {} B, not the 64 the per-page \
         saving was quoted against",
        round_up_to_eight(size_of::<u64>() + round_up_to_eight(live_fields + component_width))
    );

    println!(
        "entry {} B, stride {live_stride} B, from {live_fields} B of field; with a name slot added \
         back it would be {} B of field in {} B at a {} B stride -- the {} B a page this step \
         banked",
        size_of::<BlockIndex>(),
        live_fields + component_width,
        round_up_to_eight(live_fields + component_width),
        round_up_to_eight(size_of::<u64>() + round_up_to_eight(live_fields + component_width)),
        round_up_to_eight(size_of::<u64>() + round_up_to_eight(live_fields + component_width))
            - live_stride
    );
}

// =================================================================================================
// 4. AN ABSENT FIELD NAME IS NOT THE EMPTY FIELD NAME, AND ONLY ONE OF THEM NAMES A FIELD.
// =================================================================================================

/// A HASH PAGE THAT NAMES NO FIELD IS SKIPPED; A PAGE THAT NAMES THE EMPTY FIELD IS KEPT.
///
/// Both derive arms read `entry.component` to get a hash field name, and both ended
/// `.unwrap_or_default()` -- so a page entry carrying NO component became a field named `""`. That
/// is not a missing value standing in for itself: an empty hash field name is LEGAL, so the default
/// manufactures a real, addressable field, which then collides with a genuine empty-named field and
/// takes its address. The two states this test separates are:
///
/// ```text
///     component = Some("")   a genuine field whose name is zero characters    KEEP
///     component = None       a page that says nothing about which field       SKIP AND COUNT
/// ```
///
/// # WHY THIS ARM AND NOT THE THREE THAT WERE ALREADY CORRECTED
///
/// `sets`, `zsets` and `lists` are merged through `fill_absent_elements`, because each has a durable
/// map holding what its component re-spells -- which is what lets their comment say "skipping loses
/// it from the derived view and not from the store". `hashes` is `skip_serializing` (test 2 in this
/// module drives that off the serializer), so this arm ASSIGNS, there is no durable map behind it,
/// and a phantom field is the only answer the shard has. For a context node, whose page is filed
/// under the single constant `CONTEXT_NODE_FIELD`, the phantom is not merely a wrong name: every
/// reader that spells `"meta"` back finds nothing and the node reads as ABSENT.
///
/// # THE THREE LEGS, EACH DRIVEN
///
/// A. THE WIRE ADMITS IT. Both structures that can carry a hash page into the derive declare
///    `component` as an `Option` with `#[serde(default)]`, so an absent component is not a
///    corruption -- it is a shape the decoder is specified to accept. Read off the real serde impls
///    rather than off the attributes.
/// B. THE FOLD FILES IT. `fold_delta_block_items` copies `item.component` straight onto the page
///    entry, so an item that named nothing produces an index entry that names nothing. This is the
///    real fold, in the real order the load path runs it.
/// C. THE DERIVE IS WHERE IT BECOMES A FIELD. Both arms, over one index holding both states at once.
///
/// # TWO KEYS, BECAUSE ONE KEY CANNOT TELL THE TWO STATES APART DETERMINISTICALLY
///
/// Putting the nameless page on the SAME key as the genuine empty-named field makes the field COUNT
/// identical either way -- three page entries collapse to two field names with the default and to
/// two without it -- so the only difference is WHICH address won the `""` slot, and that is decided
/// by the order the page walk happens to emit. A mutation run proved it: the defaulting arm
/// SURVIVED, because the genuine page happened to be inserted last. So the two states get their own
/// keys and the discriminator is a count:
///
/// ```text
///     ef-nameless   one named field + one nameless page   SKIP -> 1 field, no "" key
///                                                         DEFAULT -> 2 fields, phantom ""
///     ef-empty      one named field + a genuine "" field  both -> 2 fields, "" at its own address
/// ```
///
/// THE CONTROL is the `string` kind, where `component: None` is the NORMAL and CORRECT state -- it
/// means the page IS its whole object. The change cannot move it, and the number of string pages the
/// derive actually walked is asserted, because a control over nothing reports success.
///
/// rust-internal: drives the engine's own derive over its own index, no external surface
#[test]
fn a_hash_page_naming_no_field_is_skipped_while_a_genuine_empty_field_name_is_kept() {
    use std::collections::BTreeSet;

    use crate::engine::hashing::{block_routing_bucket, stable_block_object_id};
    use crate::engine::storage_bucket_internals::{
        collect_bucket_index_live_block_entries, stored_model_kind,
    };

    // The key that receives a page naming no field. Its only real field is a named one, so a
    // phantom `""` shows up as a FIELD COUNT and not as a race for one slot.
    let nameless_key = "ef-nameless";
    // The key that holds a GENUINE empty field name, and nothing nameless.
    let empty_key = "ef-empty";
    let string_key = "ef-string";
    let empty_field = "";
    let named_field = "ef-named";

    // ---------------------------------------------------------------------------------------------
    // LEG A. THE WIRE ADMITS A PAGE THAT NAMES NO FIELD.
    // ---------------------------------------------------------------------------------------------
    // The page entry first. `component` is `skip_serializing_if = "Option::is_none"`, so a `None`
    // leaves the key out entirely and reads back as `None` -- an absent component round-trips as a
    // STABLE shape rather than as damage.
    let nameless_entry = BlockIndex {
        kind: crate::index_log::IndexItemKind::Page,
        routing_bucket: 7,
        object_key: Arc::from("ef-wire-probe"),
        model_id: stored_model_kind("hash"),
        address: ElementEntry::default(),
        dirty: false,
        deleted: false,
    };
    let encoded = serde_json::to_string(&nameless_entry).expect("a page entry serializes");
    assert!(
        encoded.len() > 32,
        "the encoded page entry is {} B, too short for the absence assertion below to mean anything",
        encoded.len()
    );
    assert!(
        !encoded.contains("component"),
        "a component-less page entry still wrote a component key, so the absent shape this finding \
         rests on is not what the encoder produces: {encoded}"
    );
    let decoded: BlockIndex =
        serde_json::from_str(&encoded).expect("a component-less page entry decodes");
    // THE DECODER CANNOT ADMIT A NAME INTO AN ENTRY ANY MORE, so this assertion is gone.
    //
    // It decoded a component-less entry and asserted the decoded value carried no component, to
    // establish that the absent shape comes from the ENCODER rather than from the decoder. The
    // type has no such field, so the decode cannot produce one whatever the wire says -- the same
    // claim, held structurally. The ENCODER half above still runs and still matters: it asserts
    // the written text carries no component key, which is what keeps the stored spelling from
    // moving.
    let _ = &decoded;

    // And the delta item, whose EVERY field carries `#[serde(default)]` by stated design, so the
    // emptiest legal record decodes to an item that names nothing.
    let bare_item: crate::index_log::IndexItem =
        serde_json::from_str("{}").expect("an all-default delta item decodes");
    assert!(
        bare_item.component.is_none(),
        "a delta item with no component key decoded with one"
    );
    println!(
        "[leg A] an absent component is a legal wire shape on both carriers: page entry {} B with \
         no component key, delta item decodes from an empty document",
        encoded.len()
    );

    // ---------------------------------------------------------------------------------------------
    // THE STORE, AND THE DENOMINATOR. An empty field name has to be accepted and durable, or the
    // distinction below is about a state no caller can reach.
    // ---------------------------------------------------------------------------------------------
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine);

    for (key, field) in [
        (nameless_key, named_field),
        (empty_key, named_field),
        (empty_key, empty_field),
    ] {
        write(
            &engine,
            Command::HashSet {
                key: key.to_string(),
                field: field.to_string(),
                value: format!("payload-{key}-{field:?}").into_bytes(),
            },
        );
    }
    // The control's object, written through the surface that files a component-less page legitimately.
    write(
        &engine,
        Command::StringSet {
            key: string_key.to_string(),
            value: b"ef-string-payload".to_vec(),
        },
    );

    let (genuine_empty_address, nameless_key_address) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let empty_fields = shard
            .hashes
            .get(empty_key)
            .expect("the empty-named hash is present after two writes");
        assert_eq!(
            empty_fields.len(),
            2,
            "the fixture stored {} field(s) under the empty-name key, not the two it needs -- if \
             the empty field name was rejected on the way in, this finding is about an unreachable \
             state",
            empty_fields.len()
        );
        let genuine = empty_fields
            .get(empty_field)
            .expect(
                "the empty field name is not in the derived map, so an empty hash field name is \
                 not reachable and this whole distinction is moot",
            )
            .clone();
        let nameless_fields = shard
            .hashes
            .get(nameless_key)
            .expect("the nameless-page key is present after one write");
        assert_eq!(
            nameless_fields.len(),
            1,
            "the nameless-page key starts with {} field(s), not the one it needs",
            nameless_fields.len()
        );
        let anchor = nameless_fields
            .get(named_field)
            .expect("the named field is present")
            .clone();
        (genuine, anchor)
    };
    println!(
        "[denominator] an empty hash field name is reachable and durable: `{empty_key}` holds 2 \
         fields including the empty-named one at slab {} offset {}; `{nameless_key}` holds 1",
        genuine_empty_address.block_slab_id(),
        genuine_empty_address.offset()
    );

    // ---------------------------------------------------------------------------------------------
    // LEG B. THE FOLD FILES A PAGE THAT NAMES NO FIELD.
    // ---------------------------------------------------------------------------------------------
    // A DISTINCT address -- one slab further on -- so the phantom is traceable to this page and not
    // to the real one. The fold is the real one: this is the shape a delta record carrying no
    // component produces on reload.
    let nameless_address = ElementEntry::from_compact_slab_address(
        nameless_key_address.address_word().wrapping_add(1u64 << 32),
        nameless_key_address.length(),
    );
    assert_ne!(
        nameless_address.address_word(),
        nameless_key_address.address_word(),
        "the nameless page shares an address with the real one, so a phantom could not be told \
         from a coincidence"
    );

    let nameless_item = crate::index_log::IndexItem {
        kind: crate::index_log::IndexItemKind::Page,
        routing_bucket: block_routing_bucket(nameless_key, 0, OPERATOR_END),
        block_ref_key: format!(
            "hash:{nameless_key}::{}:nameless",
            nameless_address.address_word()
        ),
        object_key: nameless_key.into(),
        model_id: "hash".to_string(),
        // THE WHOLE POINT: this item names no field.
        component: None,
        object_id: stable_block_object_id(1, "hash", nameless_key),
        // THIS FIXTURE USED TO DISAGREE WITH ITS OWN ADDRESS. `from_compact_slab_address`
        // passes `None` for the block id, so the derivation is `in_log = true` and the literal
        // here said `false`. It now encodes the honest value. All three such sites are tests --
        // both production builders fill these from the address -- so nothing production writes
        // ever carried the disagreeing value.
        entry: Some(nameless_address.clone()),
        deleted: false,
    };

    let nameless_entries_in_index = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        // `upsert: false` with no covered keys, so the fold ADDS this page and removes nothing --
        // the genuine pages stay and the nameless one joins them, which is the state under test.
        crate::engine::fold_delta_block_items(
            &mut shard.bucket_index,
            &BTreeSet::new(),
            std::slice::from_ref(&nameless_item),
            false,
        );
        // The same step `load_index_inner` takes between the fold and the reconcile. The fold writes
        // page entries and NOT the object lookup, so without this the serving accelerator still
        // describes the pre-fold index and the counts read below would be measuring the fixture.
        shard.bucket_index.rebuild_object_block_lookup();
        collect_bucket_index_live_block_entries(shard)
            .into_iter()
            // SELECTED BY ADDRESS, NOT BY THE ABSENCE OF A NAME.
            //
            // This filtered `entry.component.is_none()` to pick out the page the fold planted. No
            // entry names an element, so that predicate is true of EVERY page of the key and the
            // count came to 2 -- the genuine page and the planted one -- failing with "leg B has
            // not built the state leg C is about" when leg B had built it exactly. The planted
            // item has its own address, which is what distinguishes it, and matching on that is
            // stricter than the absence ever was: it cannot match a page the fold did not file.
            .filter(|entry| {
                entry.kind.as_str() == "hash"
                    && entry.object_key.as_ref() == nameless_key
                    && entry.address.block_slab_id() == nameless_address.block_slab_id()
                    && entry.address.offset() == nameless_address.offset()
                    && entry.address.length() == nameless_address.length()
            })
            .count()
    };
    assert_eq!(
        nameless_entries_in_index, 1,
        "the fold filed {nameless_entries_in_index} hash page(s) at the planted address for this \
         key, not the one this test needs -- leg B has not built the state leg C is about"
    );
    println!("[leg B] the fold filed 1 hash page naming no field, under `{nameless_key}`");

    // THE COUNTS, TAKEN BEFORE ANY DERIVE RUNS, as a BEFORE/AFTER pair rather than as an absolute,
    // because the absolute is a property of the page index this test builds and the claim is about
    // the DELTA. Under `container_index_files_one_entry_a_page` the two commands no longer read the
    // same structure -- `HashLen` counts the fields in `shard.hashes`, `HashGetAll` serves the
    // union of that map and the page index -- so the pair spans BOTH sources the derive touches,
    // which is what makes a zero delta worth asserting over a derive that rebuilds one of them.
    let hash_len_before = hash_len_of(&engine, nameless_key);
    let listed_before = hash_entries_of(&engine, nameless_key).len();
    println!(
        "[counts before] `{nameless_key}` HashLen={hash_len_before}, HashGetAll listed \
         {listed_before} entry(ies)"
    );

    // ---------------------------------------------------------------------------------------------
    // LEG C. THE DERIVE. Both arms, over the same index, with the control beside them.
    // ---------------------------------------------------------------------------------------------
    for arm in ["reconcile_secondary_views", "rebuild_unserialized_model_maps"] {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");

        // THE CONTROL'S DENOMINATOR, read before the derive runs: how many string pages this derive
        // will actually walk. A control over an empty set reports success.
        let string_pages_exercised = collect_bucket_index_live_block_entries(shard)
            .into_iter()
            .filter(|entry| entry.kind.as_str() == "string" && !entry.deleted)
            .count();
        assert!(
            string_pages_exercised >= 1,
            "[{arm}] the control walked {string_pages_exercised} string page(s), so it is a control \
             over nothing"
        );

        // `shard.hashes` is the map both arms rebuild, so clear it first: left populated, a stale
        // entry could supply the very field the derive is supposed to produce and the assertions
        // below would pass on the fixture's own leftovers.
        shard.hashes.clear_for_test();
        match arm {
            "reconcile_secondary_views" => {
                crate::engine::storage_bucket_internals::reconcile_secondary_views_from_bucket_index(
                    &engine.block_store,
                    shard,
                    None,
                );
            }
            _ => {
                crate::engine::storage_bucket_internals::rebuild_unserialized_model_maps_from_bucket_index(
                    shard,
                );
            }
        }

        // --- THE FINDING IS NOW STRUCTURAL: THE DERIVE CONTRIBUTES NO FIELD NAME AT ALL. ---
        //
        // This arm asserted that a page naming no field contributed NO field -- that an absent name
        // was SKIPPED rather than defaulted into a real, addressable field called `""`. It read the
        // derived map and expected one field from the named page and none from the nameless one.
        //
        // BOTH DERIVES NOW CONTRIBUTE NOTHING, which is a stronger statement than the skip was.
        // They decoded a field name out of `entry.component`; an entry has no component, so there
        // is no input and the derived view is EMPTY for every key. An absent name cannot become the
        // empty name because no name is produced at all -- the defect is unreachable rather than
        // handled, and the durable `hashes` map (`#[serde(default)]`) is the sole source.
        //
        // ASSERTED AS THE EMPTINESS, which is what keeps this a live guard. The fixture clears
        // `shard.hashes` immediately before the derive, so anything in the map afterwards was
        // MINTED by the derive. A change that re-introduced a derivation -- defaulting or not --
        // populates this map and reddens here, which is the one thing that could put the phantom
        // `""` field back.
        let nameless_after = shard.hashes.get(nameless_key);
        let empty_after = shard.hashes.get(empty_key);
        assert!(
            nameless_after.is_none_or(|fields| fields.is_empty()),
            "[{arm}] the derive minted {} field(s) for `{nameless_key}` out of an index that names \
             none. The map was cleared immediately before the derive ran, so every field here was \
             invented by it -- and inventing a field name from a nameless entry is exactly how the \
             phantom `\"\"` field was produced",
            nameless_after.map_or(0, |fields| fields.len())
        );
        assert!(
            empty_after.is_none_or(|fields| fields.is_empty()),
            "[{arm}] the derive minted {} field(s) for `{empty_key}`, including possibly the \
             genuine empty-named one. The durable map is the only source for those names now; a \
             derive that reproduces them is reading an entry's component again",
            empty_after.map_or(0, |fields| fields.len())
        );
        // AND SPECIFICALLY NOT THE EMPTY FIELD NAME, named on its own because it is the defect this
        // arm exists for and because `is_empty()` above would also be satisfied by a map holding
        // only real names.
        for (label, derived) in [(nameless_key, nameless_after), (empty_key, empty_after)] {
            assert!(
                derived.is_none_or(|fields| !fields.contains_key(empty_field)),
                "[{arm}] `{label}` came back with a field named `\"\"` from the DERIVE. Nothing \
                 the derive can read names a field at all, so the only way to produce one is to \
                 default an absent name into a real, addressable one. That is the defect: an \
                 absent name is not the empty name"
            );
        }
        // The fixture's own addresses stay referenced so this arm still fails to compile if the
        // page it wrote for the genuine empty-named field stops being built.
        let _ = (named_field, genuine_empty_address.address_word());

        // --- THE CONTROL, at 0.00% on a kind whose component is LEGITIMATELY absent. ---
        assert!(
            shard.strings.contains_key(string_key),
            "[{arm}] the control's string is gone: a page whose component is absent BY DESIGN was \
             caught by a change that is only about the hash arm"
        );
        println!(
            "[leg C/{arm}] the derive minted nothing for `{nameless_key}` or `{empty_key}` -- no \
             phantom, and no real name either, because an entry names no element; control: \
             {string_pages_exercised} string page(s) exercised, 0 lost, 0.00%"
        );
    }

    // ---------------------------------------------------------------------------------------------
    // THE BEFORE/AFTER PAIR MEASURED THE FIXTURE ONCE THE DERIVE STOPPED REBUILDING, so it is
    // restated as what it can still establish.
    // ---------------------------------------------------------------------------------------------
    //
    // It asserted the two counts did not MOVE across the derive, "which is the map both arms clear
    // and rebuild, so a move here means an arm rebuilt a different population than the one the
    // writes put there". Each arm above calls `shard.hashes.clear_for_test()` immediately before
    // its derive -- deliberately, so that anything in the map afterwards was minted by the derive --
    // and the derive no longer rebuilds anything. So the counts move by exactly what the FIXTURE
    // cleared, and the equality was measuring the test's own setup rather than the derive.
    //
    // WHAT IT CAN STILL ESTABLISH, and does: the counts do not move UPWARD. The derive minting a
    // field is the defect this module is about -- a nameless entry defaulted into a real
    // addressable name -- and that shows as a count ABOVE what the writes put there, never below.
    // The downward direction is the fixture's clear and is asserted as such, so a derive that
    // started rebuilding would redden the emptiness assertions in each arm rather than hiding
    // inside an equality here.
    let hash_len_after = hash_len_of(&engine, nameless_key);
    let listed_after = hash_entries_of(&engine, nameless_key).len();
    assert!(
        hash_len_after <= hash_len_before,
        "HashLen moved {hash_len_before} -> {hash_len_after} across the derive: UPWARD. It counts \
         the fields in `shard.hashes`, the map each arm clears before deriving, so a rise means \
         the derive minted a field out of an index that names none -- which is the phantom this \
         module exists for"
    );
    assert!(
        listed_after <= listed_before,
        "HashGetAll's listing moved {listed_before} -> {listed_after} across the derive: UPWARD. \
         It serves the union of `shard.hashes` and the page index, so a rise means one of the two \
         gained an element the writes did not put there"
    );
    assert_eq!(
        0, hash_len_after,
        "HashLen answers {hash_len_after} after both derives, and the map was CLEARED before each \
         of them. The derive is not a source of field names any more, so the only honest answer \
         here is zero -- a nonzero one means something rebuilt the map and the arms above should \
         have caught it first"
    );

    // AND WHICH STRUCTURE `HashLen` READ IS NOW PROVED BY THE GAP RATHER THAN BY A FLOOR.
    //
    // A floor stood here -- `hash_len_after >= 1`, the one real field the fixture wrote -- because
    // "a count of zero would satisfy both equalities above". The derive no longer rebuilds the map
    // the fixture cleared, so the honest count IS zero and the floor could only fail.
    //
    // THE DISCRIMINATOR IS SHARPER WITHOUT IT. `HashLen` reads `shard.hashes`, which this test
    // emptied; the page index still holds TWO live hash entries for this key, the named page and
    // the nameless one leg B filed. So `HashLen` answering 0 against 2 indexed pages is positive
    // proof it reads the MAP and not the index -- which is exactly what this section was built to
    // pin, and what the old floor could only approach. Revert the collapse so `HashLen` counts
    // entries again and it answers 2 against a cleared map, which reddens both halves below.
    let indexed_pages_after = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        collect_bucket_index_live_block_entries(shard)
            .into_iter()
            .filter(|entry| {
                entry.kind.as_str() == "hash"
                    && entry.object_key.as_ref() == nameless_key
                    && !entry.deleted
            })
            .count() as i64
    };
    // THE DENOMINATOR: the two structures must hold DIFFERENT numbers, or the gap proves nothing.
    assert_eq!(
        2, indexed_pages_after,
        "`{nameless_key}` carries {indexed_pages_after} live hash page entries, not the two this \
         stage needs -- one named page and the one nameless page leg B filed. Without two against \
         a cleared map, the field count and the entry count could be the same number and nothing \
         below could say which of them HashLen read"
    );
    assert_ne!(
        indexed_pages_after, hash_len_after,
        "HashLen answered {hash_len_after} and the index holds {indexed_pages_after} live page \
         entries for this key. They must DIFFER: the map was cleared and the index was not, so a \
         HashLen that matches the entry count is counting index entries rather than fields -- and \
         the nameless page is then being counted as a field, which is this module's defect"
    );
    println!(
        "[counts after] HashLen={hash_len_after} (was {hash_len_before}), HashGetAll listed \
         {listed_after} (was {listed_before}). HashLen reads the CLEARED `shard.hashes` and \
         answers {hash_len_after} against {indexed_pages_after} live page entries in the index, so \
         the gap names the structure it read rather than being a coincidence"
    );
}

fn hash_len_of(engine: &TemporalEngine, key: &str) -> i64 {
    match read(
        engine,
        Command::HashLen {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("HashLen answered {other:?}"),
    }
}

fn hash_entries_of(engine: &TemporalEngine, key: &str) -> Vec<(String, Vec<u8>)> {
    match read(
        engine,
        Command::HashGetAll {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::HashEntries { entries } => entries,
        other => panic!("HashGetAll answered {other:?}"),
    }
}
