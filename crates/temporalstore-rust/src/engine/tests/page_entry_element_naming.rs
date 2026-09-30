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
//! `skip_serializing` on `engine/state.rs:133` removed -- and `HashGetAll`/`HashLen` moved onto that
//! map instead of onto `bucket_index_component_block_addresses`. That is a stored-format change of
//! its own, and it is a MOVE rather than a duplication: the field names it would start writing are
//! the same characters the component writes today through `block_index_written_key`. It was not done
//! here because it is a second format break landing beside a sibling's, and the campaign takes one
//! bump at a time.
//!
//! NO PRODUCTION CODE CHANGES IN THIS MODULE. Three tests, each driving a fact rather than quoting
//! one.

#![allow(clippy::all)]
use super::*;

use std::mem::size_of;
use std::sync::Arc;

use crate::block_store::BlockAddress;
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
fn a_swapped_hash_component_no_longer_renames_the_field_because_the_page_contradicts_it() {
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

    // --- THE VACUITY FLOOR. A swap pass that found nothing proves nothing. ---
    assert!(
        hash_swaps > 0,
        "the hash field component was never found in the served index, so this test swapped \
         NOTHING and its assertions below would pass on an unmutated store"
    );
    assert!(
        zset_swaps > 0,
        "the zset member component was never found in the served index, so the control arm was \
         NOT EXERCISED -- a control that did not run reads exactly like a control that did not move"
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

    // THE DENOMINATOR, so a store that came back empty cannot pass the assertions below. It is the
    // two UNSWAPPED fields: the swapped one is the subject and its absence is the finding, so
    // counting it here would settle the finding before the finding is asserted.
    assert_eq!(
        hash_fields.len() - 1,
        names.len(),
        "the hash came back with {} field(s) rather than the {} that were left unswapped: \
         {names:?}",
        names.len(),
        hash_fields.len() - 1
    );
    for survivor in ["hfield-a", "hfield-c"] {
        assert!(
            names.contains(survivor),
            "a field whose component was NOT swapped went missing, so this store is broken in some \
             way the swap did not cause: {names:?}"
        );
    }

    // THE FINDING: the swapped component names an element its page does not hold, so it is served
    // under NEITHER name.
    assert!(
        !names.contains("hfield-Z"),
        "the swapped component became the field name, which is what this test asserted BEFORE the \
         page carried its own element key -- so the page is no longer contradicting the index and \
         something has stopped selecting by element: {names:?}"
    );
    assert!(
        !names.contains("hfield-b"),
        "the original field name was served under a component that no longer spells it, so the \
         index's name is being ignored rather than checked against the page: {names:?}"
    );

    // AND THE POINT LOOKUP AGREES WITH THE LISTING, both ways round. Asserted because a listing and
    // a point read resolve through different code and #2014 exists because they agreed by accident.
    for absent in ["hfield-b", "hfield-Z"] {
        let answer = read(
            &engine,
            Command::HashGet {
                key: hash_key.to_string(),
                field: absent.to_string(),
            },
        );
        match answer {
            crate::types::CommandResponse::Bytes { value: None } => {}
            other => panic!("HashGet for {absent} answered {other:?} where the listing has neither"),
        }
    }

    // THE COST, RECORDED. `HashLen` counts entries and the listing counts entries whose page reads,
    // so on a store edited underneath the engine the two diverge by exactly the edited element.
    let length = match read(
        &engine,
        Command::HashLen {
            key: hash_key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("HashLen answered {other:?}"),
    };
    assert_eq!(
        hash_fields.len() as i64,
        length,
        "HashLen counts page-index entries, and the swap did not remove one, so it must still \
         answer {}",
        hash_fields.len()
    );
    assert_eq!(
        1,
        length - names.len() as i64,
        "the length answer and the listing must diverge by exactly the one element whose component \
         was edited; they diverge by {}",
        length - names.len() as i64
    );

    // THE CONTROL: the zset member did NOT follow its swapped component.
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
            "the ORIGINAL zset member did not answer after its component was swapped, so the \
             durable map did not outrank the name and the control is not a control: {other:?}"
        ),
    };
    assert_eq!(
        "7.5", score,
        "the durable zset map answered {score} for the original member"
    );

    println!(
        "hash component swaps={hash_swaps} -> field NEITHER renamed NOR served (listing {} of {}, \
         HashLen {length}); zset component swaps={zset_swaps} -> member UNCHANGED at {score}",
        names.len(),
        hash_fields.len()
    );
}

// =================================================================================================
// 2. THE STRUCTURAL REASON. THE SHARD INDEX WRITES NO HASH FIELD MAP.
// =================================================================================================

/// THE THREE SPELLED KINDS HAVE A DURABLE MAP IN THE WRITTEN SHARD INDEX AND THE HASH DOES NOT.
///
/// Read off the SERIALIZER rather than off the attribute, because `#[serde(skip_serializing)]` is
/// the kind of claim a doc comment can carry after the attribute has moved. An EMPTY `ShardState`
/// is enough and is the strongest form of the test: `sets`, `zsets` and `lists` are emitted even
/// when empty, so their presence here is not an artefact of seeding, and `hashes` is absent for the
/// one reason that it is never written at all.
#[test]
fn the_shard_index_writes_no_hash_field_map_and_writes_the_other_three() {
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

    for durable in ["sets", "zsets", "lists"] {
        assert!(
            object.contains_key(durable),
            "`{durable}` is not in the written shard index, so this kind has no durable map and \
             the table in this module's header is wrong"
        );
    }
    assert!(
        !object.contains_key("hashes"),
        "`hashes` IS written to the shard index now. If that is deliberate then the stop condition \
         in this module is stale and removing the component becomes reachable -- re-read the header"
    );

    println!(
        "written shard-index fields={}, sets/zsets/lists present, hashes ABSENT",
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
    address: BlockAddress,
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
fn the_entry_without_a_component_is_forty_eight_and_the_stride_fifty_six() {
    // --- THE LIVE ENTRY, RECONSTRUCTED. ---
    let live_fields = size_of::<Arc<str>>()          // object_key
        + size_of::<StoredModelKind>()               // model_id
        + size_of::<Option<Arc<str>>>()              // component
        + size_of::<BlockAddress>()                  // address
        + 3 * size_of::<bool>();                     // dirty, deleted, log_backed
    assert_eq!(
        round_up_to_eight(live_fields),
        size_of::<BlockIndex>(),
        "the live entry reconstructs to {} B from {live_fields} B of field, but `size_of` says {}",
        round_up_to_eight(live_fields),
        size_of::<BlockIndex>()
    );

    // --- THE COMPONENT'S SHARE OF IT. ---
    let component_width = size_of::<Option<Arc<str>>>();
    assert_eq!(
        16, component_width,
        "the component slot is {component_width} B, not the sixteen this module is about"
    );

    // --- THE MIRROR, THE SAME WAY. ---
    let mirror_fields = live_fields - component_width;
    assert_eq!(
        round_up_to_eight(mirror_fields),
        size_of::<MirrorEntryNoComponent>(),
        "the component-less entry reconstructs to {} B from {mirror_fields} B of field, but \
         `size_of` says {}",
        round_up_to_eight(mirror_fields),
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
    assert_eq!(64, size_of::<BlockIndex>(), "the entry pin moved");
    assert_eq!(72, live_stride, "the stride pin moved");
    assert_eq!(
        48,
        size_of::<MirrorEntryNoComponent>(),
        "the component-less entry is not 48"
    );
    assert_eq!(56, mirror_stride, "the component-less stride is not 56");
    assert!(
        mirror_stride < live_stride,
        "the step is worth nothing, which would refute it on bytes as well"
    );

    println!(
        "entry {} -> {} B ({live_fields} -> {mirror_fields} B of field); \
         stride {live_stride} -> {mirror_stride} B; per-page saving {} B, DECLINED on the hash",
        size_of::<BlockIndex>(),
        size_of::<MirrorEntryNoComponent>(),
        live_stride - mirror_stride
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
        object_key: Arc::from("ef-wire-probe"),
        model_id: stored_model_kind("hash"),
        component: None,
        address: BlockAddress::default(),
        dirty: false,
        deleted: false,
        log_backed: false,
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
    assert!(
        decoded.component.is_none(),
        "a page entry written without a component decoded with one, so the decoder is not what \
         admits this shape -- re-read `BlockIndex::component`"
    );

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
    let nameless_address = BlockAddress::from_compact_slab_address(
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
        object_key: nameless_key.to_string(),
        model_id: "hash".to_string(),
        // THE WHOLE POINT: this item names no field.
        component: None,
        object_id: stable_block_object_id(1, "hash", nameless_key),
        block_id: 0,
        address: Some(nameless_address.clone()),
        size: nameless_address.length(),
        in_log: false,
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
            .filter(|entry| {
                entry.kind.as_str() == "hash"
                    && entry.object_key.as_ref() == nameless_key
                    && entry.component.is_none()
            })
            .count()
    };
    assert_eq!(
        nameless_entries_in_index, 1,
        "the fold filed {nameless_entries_in_index} component-less hash page(s) for this key, not \
         the one this test needs -- leg B has not built the state leg C is about"
    );
    println!("[leg B] the fold filed 1 hash page naming no field, under `{nameless_key}`");

    // THE COUNTS, TAKEN BEFORE ANY DERIVE RUNS. `HashLen` and `HashGetAll` resolve through
    // `bucket_index_component_block_addresses` and NOT through `shard.hashes`, so neither can move
    // when the derive changes -- measured as a BEFORE/AFTER pair rather than asserted as an
    // absolute, because the absolute is a property of the page index this test builds and the claim
    // is about the DELTA. A sibling is landing a guard on exactly this pair, and this change has to
    // be visibly clear of it.
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
        shard.hashes.clear();
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

        // --- THE FINDING. A page that named no field contributed NO field. ---
        let derived_nameless = shard.hashes.get(nameless_key).unwrap_or_else(|| {
            panic!("[{arm}] the derive produced no field map for `{nameless_key}` at all")
        });
        assert!(
            !derived_nameless.contains_key(empty_field),
            "[{arm}] `{nameless_key}` came back with a field named `\"\"`. Nothing ever wrote an \
             empty-named field under this key -- the only page that could have produced it is the \
             one that named NO field, defaulted into a real, addressable field name. That is the \
             defect: an absent name is not the empty name"
        );
        assert_eq!(
            derived_nameless.len(),
            1,
            "[{arm}] `{nameless_key}` derived {} field(s) from one named page and one nameless one. \
             Two means the nameless page became a field of its own; zero means the real one was lost",
            derived_nameless.len()
        );
        assert!(
            derived_nameless.contains_key(named_field),
            "[{arm}] the named field under `{nameless_key}` is gone, so the skip took a field the \
             derive could read"
        );

        // --- THE DISTINCTION. A GENUINE empty field name is untouched, at its own address. ---
        let derived_empty = shard.hashes.get(empty_key).unwrap_or_else(|| {
            panic!("[{arm}] the derive produced no field map for `{empty_key}` at all")
        });
        let empty_address = derived_empty.get(empty_field).unwrap_or_else(|| {
            panic!(
                "[{arm}] the genuine empty-named field is GONE. Skipping a page that names no field \
                 must not take the real empty-named field with it -- that is the distinction this \
                 test exists for"
            )
        });
        assert_eq!(
            empty_address.address_word(),
            genuine_empty_address.address_word(),
            "[{arm}] the genuine empty-named field resolves to {} instead of the page that was \
             written for it ({})",
            empty_address.address_word(),
            genuine_empty_address.address_word()
        );
        assert_eq!(
            derived_empty.len(),
            2,
            "[{arm}] `{empty_key}` derived {} field(s), not the two that were written",
            derived_empty.len()
        );

        // --- THE CONTROL, at 0.00% on a kind whose component is LEGITIMATELY absent. ---
        assert!(
            shard.strings.contains_key(string_key),
            "[{arm}] the control's string is gone: a page whose component is absent BY DESIGN was \
             caught by a change that is only about the hash arm"
        );
        println!(
            "[leg C/{arm}] `{nameless_key}` -> 1 field, no phantom; `{empty_key}` -> 2 fields with \
             the empty-named one at its own address; control: {string_pages_exercised} string \
             page(s) exercised, 0 lost, 0.00%"
        );
    }

    // ---------------------------------------------------------------------------------------------
    // AND NEITHER COUNT MOVED. Both derive arms have now run over this index.
    // ---------------------------------------------------------------------------------------------
    let hash_len_after = hash_len_of(&engine, nameless_key);
    let listed_after = hash_entries_of(&engine, nameless_key).len();
    assert_eq!(
        hash_len_before, hash_len_after,
        "HashLen moved {hash_len_before} -> {hash_len_after} across the derive. It resolves through \
         `bucket_index_component_block_addresses` and never reads `shard.hashes`, so a move here \
         means this change reached further than the derive it is about"
    );
    assert_eq!(
        listed_before, listed_after,
        "HashGetAll's listing moved {listed_before} -> {listed_after} across the derive, which it \
         cannot do by reading the page index alone"
    );
    // THE DENOMINATOR for the pair: a count of zero would satisfy both equalities above.
    assert!(
        hash_len_after >= 2 && listed_after >= 1,
        "the count pair is {hash_len_after}/{listed_after}, too small for the two equalities above \
         to have measured anything"
    );
    println!(
        "[counts after] HashLen={hash_len_after} (was {hash_len_before}), HashGetAll listed \
         {listed_after} (was {listed_before}) -- 0 moved, and neither reads `shard.hashes`. The two \
         differ from each other for the reason a sibling is guarding, which this change does not \
         touch"
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
