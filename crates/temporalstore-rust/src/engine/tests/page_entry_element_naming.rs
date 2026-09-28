// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! REFUTATION. THE PAGE ENTRY CANNOT STOP NAMING ITS ELEMENT, BECAUSE FOR ONE KIND THE NAME IS
//! THE ELEMENT AND THE SERVING PATH READS IT.
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
//! # THE STOP CONDITION, IN ONE LINE
//!
//! For a HASH the component is the caller's field name, `shard.hashes` is `skip_serializing`, and
//! `Command::HashGetAll` RETURNS THE COMPONENT AS THE FIELD NAME. So the component is not a second
//! copy of a hash field name. It is the only one, on the live read path and across a reload alike.
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
//! And behind that, `rebuild_unserialized_model_maps_from_bucket_index`
//! (`storage_bucket_internals.rs:1625`) and the big derive's hash arm (`:4306`) both take the field
//! name from `entry.component.unwrap_or_default()`, and the derive ASSIGNS `shard.hashes = hashes`
//! wholesale -- unlike the three arms below it, which merge, because those three have a durable map
//! to merge with and the hash has none.
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
// 1. THE REFUTATION. A HASH FIELD NAME FOLLOWS ITS COMPONENT; A ZSET MEMBER DOES NOT.
// =================================================================================================

/// SWAP A HASH FIELD'S COMPONENT ON DISK AND THE FIELD COMES BACK RENAMED.
///
/// Nothing outranks it, because there is nothing else that holds it. The CONTROL is in the same
/// store, under the same instrument, with the same kind of mutation: a zset member's component is
/// swapped too, and that member comes back UNCHANGED because `zset_index_serde` persisted it beside
/// the page and `fill_absent_elements` keeps what the derived view could not produce.
///
/// An arm that moved and an arm that did not, from one swap pass, is what makes this a measurement
/// rather than a demonstration that editing an index breaks things.
///
/// rust-internal: mutates the engine's own served index, no external surface
#[test]
fn a_hash_field_name_follows_a_swapped_component_because_nothing_outranks_it() {
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

    // The DENOMINATOR, so a store that came back empty cannot pass the two assertions below.
    assert_eq!(
        hash_fields.len(),
        names.len(),
        "the hash came back with {} field(s) rather than {}: {names:?}",
        names.len(),
        hash_fields.len()
    );

    // THE FINDING: the field is named by whatever the component now spells.
    assert!(
        names.contains("hfield-Z"),
        "the swapped component did not become the field name; HashGetAll answered {names:?}. If \
         this fails the hash field name has a second copy somewhere and the refutation is stale"
    );
    assert!(
        !names.contains("hfield-b"),
        "the original field name survived a component swap, so something outranks the component \
         for a hash and this module's stop condition no longer holds: {names:?}"
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
        "hash component swaps={hash_swaps} -> field RENAMED; \
         zset component swaps={zset_swaps} -> member UNCHANGED at {score}"
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
