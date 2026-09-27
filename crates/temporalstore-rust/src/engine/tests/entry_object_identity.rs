// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHY A PAGE ENTRY CARRIES ITS OBJECT'S KEY BESIDE AN ID DERIVED FROM THAT SAME KEY.
//!
//! `BlockIndex` holds `object_key: Arc<str>` and `address: BlockAddress`, and the address holds an
//! `object_id: u64` computed from the key. That reads like the identity stated twice, and #1974
//! established the derivation -- `stable_block_object_id(shard, kind, key, component)`, computed at
//! about thirty sites. This module is the refutation of removing the key, and it exists because the
//! refutation is NOT the one the shape suggests.
//!
//! THE ID DOES NOT NAME AN OBJECT. It names a TRIPLE: the component is folded into it alongside the
//! kind and the key, so two pages of one object under different components carry DIFFERENT ids and
//! no page of a multi-component object shares an id with any other. `address_footprint` recorded
//! that as a note about a fixture ("a component is its own object rather than another block") and
//! `bucket_fill` recorded it as a property of the page handle. Neither drew the consequence, and the
//! consequence is what decides this:
//!
//!   * A reader that matches a page against a caller's kind AND key AND component could be served
//!     by the id. The caller holds all three and `ShardState` holds the shard, so the id is
//!     computable where the comparison happens. Five readers are of that shape.
//!   * A reader that is COMPONENT-BLIND cannot. `bucket_index_component_block_addresses` is the
//!     sharpest: it is the whole-object read path, it asks for EVERY COMPONENT of one key, and its
//!     caller holds a key and no component list. The ids it would have to match are not derivable
//!     from what it is given -- computing them needs the components, which are the answer it was
//!     called to produce. That is not a reader missing a parameter. It is a reader asking a question
//!     the id cannot express.
//!
//! AND THE STORED SPELLING IS THE KEY'S CHARACTERS. `block_index_written_key` renders the page
//! index's own map key through `block_ref_key_from_parts` as `kind:object_key:component:...`, and
//! that string is what a dump writes and what the replay log shares. `pages_per_bucket` pinned the
//! COMPONENT's place in it -- found by mutation, because dropping a field from that key leaves every
//! key distinct (the addresses differ), so uniqueness, key counts and ordering all still pass. The
//! object key's place in it was pinned by nothing, and a mutation dropping it would have survived
//! for exactly the same reason. It is pinned here.
//!
//! WHAT IS NOT CLAIMED, because it is only a missing parameter. `release_bucket_blocks` compares a
//! resident page against one derived from the model maps through `released_block_identity`, whose
//! own doc excludes `object_id` because "the model map's copy of the same page may not carry it".
//! That exclusion is real but it is not a pin on the KEY: `derive_released_block_identities` already
//! takes the shard, so it could compute the id on the derived side and compare on that. The two
//! sites in that function that DO need the characters are its lookup probes -- `block_refs_for` and
//! `remove_object_block_lookup_entry` -- because `ObjectBlockLookup` is character-keyed, per object,
//! above the component. This distinction is drawn because the campaign has now three times found a
//! reader that "cannot be given what it needs" to merely lack an argument.
//!
//! AND THE FALLBACK SITES PIN NOTHING HERE. Every
//! `unwrap_or_else(|| stable_block_object_id(..))` reads its key from the model-map walk or from a
//! command parameter, never from a page entry -- so `reload_released_bucket` does not read this field
//! at all. Its one mention is a struct-literal WRITE, fed by
//! `collect_model_live_block_entries_in_bucket`: the model maps supply the characters and reload uses
//! them to recompute the id. The "needs no hash fallback to answer" precondition on `BucketNode` is
//! about the ROUTING BUCKET, not about this field.
//!
//! WHAT A HANDLE WOULD COST IS MEASURED ELSEWHERE AND STILL DECLINED.
//! `a_handle_for_the_object_key_pays_only_above_the_measured_break_even` prices a four-byte index
//! into a shared table at one entry per distinct object against sixteen bytes a page, and the
//! break-even is 3.38 pages per object: it PAYS on containers at 100 and LOSES on routed keys at 1
//! and on a mixed store at 1.98. A handle would satisfy every reader here -- keeping the characters
//! reachable is what a handle does -- so this module refutes REMOVING the field, not interning it.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::control::LoadShardRequest;
use crate::engine::hashing::stable_block_object_id;
use crate::engine::state::BlockIndex;
use crate::engine::storage_bucket_internals::StoredModelKind;

/// The range a production shard is loaded on: `TS_SHARD_END_ROUTING_BUCKET=1023` (#1973).
const OPERATOR_END: u32 = 1023;

/// The kind every fixture here uses. Hash is the kind whose objects carry real components, which is
/// the population the whole module is about.
const FIXTURE_KIND: StoredModelKind = StoredModelKind::Hash;

/// Container keys and members. Every denominator below is derived from these rather than restated
/// as a literal at the assertion that checks it.
const CONTAINER_KEYS: usize = 8;
const MEMBERS_PER_KEY: usize = 25;
/// Routed keys for the CONTROL arm, where an object holds exactly one page.
const ROUTED_KEYS: usize = 200;

// =================================================================================================
// FIXTURE. The same shapes `page_entry_handles` seeds, at the size these assertions need rather than
// the size a byte measurement needs.
// =================================================================================================

fn identity_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ))
}

fn load_operator_shard(engine: &TemporalEngine) {
    let response = engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: OPERATOR_END,
        readonly: false,
        table_name: String::new(),
    });
    assert!(
        response.status.ok,
        "shard must load over 0..={OPERATOR_END}: {:?}",
        response.status
    );
}

fn ack_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    assert!(
        !commands.is_empty(),
        "an empty seed would make every row below vacuous"
    );
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// Hash containers only: one key, `MEMBERS_PER_KEY` fields, each field its own page and its own
/// component. The population where the id's granularity and the object's disagree.
fn seed_hash_containers(engine: &TemporalEngine) {
    let mut commands = Vec::with_capacity(CONTAINER_KEYS * MEMBERS_PER_KEY);
    for k in 0..CONTAINER_KEYS {
        for f in 0..MEMBERS_PER_KEY {
            commands.push(Command::HashSet {
                key: format!("h{k}"),
                field: format!("f{f}"),
                value: vec![b'v'; 32],
            });
        }
    }
    ack_batch(engine, commands);
}

/// Keys holding exactly one page each: the CONTROL population, where the mechanism predicts no
/// effect at all.
fn seed_routed_strings(engine: &TemporalEngine) {
    ack_batch(
        engine,
        (0..ROUTED_KEYS)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
}

/// Every live page in shard 1, as the ids filed under each object key. The denominator for every
/// row below.
fn ids_by_object_key(engine: &TemporalEngine) -> BTreeMap<String, Vec<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut by_key: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted {
                continue;
            }
            by_key
                .entry(page.object_key.to_string())
                .or_default()
                .push(page.object_id());
        }
    }
    by_key
}

/// A free-standing entry at a FIXED address, whose object id is the one the engine would compute
/// for it. Taken from `stable_block_object_id` rather than invented, so a comparison between two of
/// these is about the field that differs and the id that follows from it.
fn entry_named(object_key: &str, component: Option<&str>) -> BlockIndex {
    let object_id = stable_block_object_id(1, FIXTURE_KIND.as_str(), object_key, component);
    BlockIndex {
        object_key: Arc::from(object_key),
        model_id: FIXTURE_KIND,
        component: component.map(Arc::from),
        address: BlockAddress::from_parts(9, 4_096, 96, Some(7), Some(object_id)),
        dirty: false,
        deleted: false,
        log_backed: true,
    }
}

// =================================================================================================
// 1. THE ID NAMES A TRIPLE, NOT AN OBJECT.
// =================================================================================================

/// THE ID IS PER (SHARD, KIND, KEY, COMPONENT), SO ONE OBJECT KEY HOLDS AS MANY IDS AS IT HAS PAGES.
///
/// Asserted three ways, and the third is a control on the first two.
///
///   1. FROM THE AUTHORITY. `stable_block_object_id` is called directly over one key and a derived
///      number of components, and the distinct ids counted. The expected count comes from the loop
///      bound rather than from a literal, so this cannot pass by agreeing with a stale number.
///   2. OVER A REAL STORE. A hash container's pages are grouped by object key and the ids counted
///      per key: one key, `MEMBERS_PER_KEY` pages, `MEMBERS_PER_KEY` ids.
///   3. THE CONTROL. Routed string keys hold ONE page each, so the mechanism predicts NO key with
///      more than one id. That arm asserts ZERO and requires nothing non-zero of itself, which is
///      what a control is for: if it ever reported a non-zero share, the grouping above would be
///      counting pages of different objects together.
///
/// rust-internal: reads declarations and a seeded store, no product behaviour
#[test]
fn the_page_id_names_a_triple_so_one_object_key_holds_as_many_ids_as_pages() {
    // --- 1. FROM THE AUTHORITY, WITH THE EXPECTATION DERIVED. ---
    let components: Vec<String> = (0..MEMBERS_PER_KEY).map(|f| format!("f{f}")).collect();
    let folded: BTreeSet<u64> = components
        .iter()
        .map(|component| {
            stable_block_object_id(1, FIXTURE_KIND.as_str(), "h0", Some(component))
        })
        .collect();
    println!(
        "\n=== stable_block_object_id over ONE key and {} components ===",
        components.len()
    );
    println!(
        "  distinct ids: {} over {} components",
        folded.len(),
        components.len()
    );
    assert_eq!(
        components.len(),
        folded.len(),
        "one key and {} distinct components produced {} distinct ids. If the component were NOT \
         folded into the id this would be 1, and every component-blind reader could be served by \
         the id instead of by the key",
        components.len(),
        folded.len()
    );
    let component_less = stable_block_object_id(1, FIXTURE_KIND.as_str(), "h0", None);
    assert!(
        !folded.contains(&component_less),
        "the id for (shard, kind, key, None) collides with one of the {} component ids, so the \
         component is not reaching the hash and this whole module is measuring nothing",
        folded.len()
    );

    // --- 2. OVER A REAL STORE. ---
    let dir = tempfile::tempdir().expect("tempdir");
    let path_length = dir.path().as_os_str().len();
    let engine = identity_engine(dir.path());
    load_operator_shard(&engine);
    seed_hash_containers(&engine);
    let by_key = ids_by_object_key(&engine);

    println!(
        "\n=== {CONTAINER_KEYS} hash containers x {MEMBERS_PER_KEY} fields, operator \
         {OPERATOR_END}, store path {path_length} characters ==="
    );
    println!(
        "  object keys seen: {} (denominator for every row below)",
        by_key.len()
    );
    assert_eq!(
        CONTAINER_KEYS,
        by_key.len(),
        "the fixture was supposed to reach {CONTAINER_KEYS} distinct object keys and reached {}; a \
         store that did not reach the population cannot refute anything about it",
        by_key.len()
    );
    let mut keys_holding_many_ids = 0usize;
    for (object_key, ids) in &by_key {
        let distinct: BTreeSet<u64> = ids.iter().copied().collect();
        println!(
            "  {object_key:<8} pages={:<4} distinct ids={:<4}",
            ids.len(),
            distinct.len()
        );
        assert_eq!(
            MEMBERS_PER_KEY,
            ids.len(),
            "{object_key} holds {} pages, not the {MEMBERS_PER_KEY} the seed wrote -- the container \
             population was not reached",
            ids.len()
        );
        assert_eq!(
            ids.len(),
            distinct.len(),
            "{object_key}'s {} pages share {} ids. Every page of one object is supposed to carry \
             its OWN id, because the component is folded in; if they shared one, the id would name \
             the object and the key beside it would be redundant",
            ids.len(),
            distinct.len()
        );
        if distinct.len() > 1 {
            keys_holding_many_ids += 1;
        }
    }
    assert_eq!(
        by_key.len(),
        keys_holding_many_ids,
        "{keys_holding_many_ids} of {} container keys hold more than one id; all of them are \
         supposed to",
        by_key.len()
    );

    // --- 3. THE CONTROL: a population where the mechanism predicts nothing. ---
    let control_dir = tempfile::tempdir().expect("tempdir");
    assert_eq!(
        path_length,
        control_dir.path().as_os_str().len(),
        "the store path length moved between the arms. No figure in this test is a byte count, but \
         the arms must still be comparable and bytes move at about six a character"
    );
    let control = identity_engine(control_dir.path());
    load_operator_shard(&control);
    seed_routed_strings(&control);
    let control_keys = ids_by_object_key(&control);
    let control_many = control_keys
        .values()
        .filter(|ids| ids.iter().copied().collect::<BTreeSet<u64>>().len() > 1)
        .count();
    let share = 100.0 * control_many as f64 / control_keys.len().max(1) as f64;
    println!(
        "\n=== CONTROL: {ROUTED_KEYS} routed string keys, one page each ===\n  keys={} \
         keys holding more than one id={control_many} ({share:.2}%)",
        control_keys.len()
    );
    assert_eq!(
        ROUTED_KEYS,
        control_keys.len(),
        "the control reached {} object keys, not {ROUTED_KEYS}; a control that did not run is not a \
         control",
        control_keys.len()
    );
    assert_eq!(
        0, control_many,
        "{control_many} routed keys hold more than one id. A routed key is one page, so the \
         component folding has nothing to act on here and this count is supposed to be exactly \
         zero -- if it is not, the grouping above is counting pages of different objects together"
    );
}

// =================================================================================================
// 2. THE STORED SPELLING IS THE KEY'S CHARACTERS.
// =================================================================================================

/// THE WRITTEN KEY OF A PAGE SPELLS ITS OBJECT KEY, SO AN ID CANNOT RENDER IT.
///
/// `pages_per_bucket` pinned the COMPONENT's place in this string after a mutation that dropped it
/// survived every test in the crate -- dropping any field from the written key leaves every key
/// DISTINCT, because the addresses differ, so uniqueness, key counts and ordering all still pass.
/// The object key's place in it was pinned by nothing and the identical mutation would have survived
/// for the identical reason.
///
/// THE POSITION IS DERIVED FROM THE AUTHORITY rather than restated. `block_ref_key_from_parts`
/// builds `kind:object_key:component:...`, so the object key is the field after the kind -- and the
/// precondition that makes that split meaningful, that neither the kind nor the key contains the
/// separator, is ASSERTED rather than assumed.
///
/// rust-internal: reads declarations, no product behaviour
#[test]
fn the_written_key_of_a_page_spells_its_object_key_so_an_id_cannot_render_it() {
    let object_key = "container";
    let page = entry_named(object_key, Some("body"));
    let written = crate::engine::state::block_index_written_key(&page);
    println!("\n=== the written key of one page ===\n  {written}");

    // --- THE PRECONDITION FOR SPLITTING IT, ENFORCED RATHER THAN PRINTED. ---
    assert!(
        !FIXTURE_KIND.as_str().contains(':') && !object_key.contains(':'),
        "the separator appears inside the kind {:?} or the key {object_key:?}, so the field split \
         below does not mean what it says",
        FIXTURE_KIND.as_str()
    );

    // --- THE KEY'S CHARACTERS ARE THE FIELD AFTER THE KIND. ---
    let mut fields = written.split(':');
    assert_eq!(
        Some(FIXTURE_KIND.as_str()),
        fields.next(),
        "the written key does not open with the model spelling: {written:?}"
    );
    assert_eq!(
        Some(object_key),
        fields.next(),
        "the field after the model spelling is not the object key's characters. \
         `block_ref_key_from_parts` renders `kind:object_key:component:...` and this string is the \
         page index's own stored map key, shared with the replay log -- so the entry cannot hand it \
         over without holding those characters: {written:?}"
    );

    // --- AND THE ID IS NOT IN IT, so the characters are not substitutable by the id. ---
    let id = page.object_id();
    assert_ne!(
        0, id,
        "the fixture page carries no object id, so the comparison below is vacuous"
    );
    assert!(
        !written.contains(&id.to_string()),
        "the written key contains the object id {id}, which would make the id a candidate spelling \
         for it: {written:?}"
    );

    // --- TWO PAGES DIFFERING ONLY IN THE KEY RENDER DIFFERENT STORED SPELLINGS. ---
    let other_key = "containee";
    assert_eq!(
        object_key.len(),
        other_key.len(),
        "the two keys differ in length as well as in text, so a difference below could be about the \
         length rather than about the characters"
    );
    let other = entry_named(other_key, Some("body"));
    let other_written = crate::engine::state::block_index_written_key(&other);
    println!("  the same page under {other_key:?}\n  {other_written}");
    assert_ne!(
        written, other_written,
        "two pages at the SAME address under different object keys render the same written key, so \
         the key has stopped reaching the stored spelling"
    );

    // --- AND THE HANDLE MOVES WITH IT, which is what keeps the two from naming different pages. ---
    let handle = crate::engine::state::block_index_handle(&page);
    let other_handle = crate::engine::state::block_index_handle(&other);
    assert_ne!(
        handle, other_handle,
        "two pages differing only in their object key hash to one handle {handle}. \
         `block_index_handle`'s contract is to hash exactly the fields `block_index_written_key` \
         renders, and those handles are written to disk inside the lookup's refs"
    );
}

// =================================================================================================
// 3. THE READER THAT REFUTES IT, DRIVEN.
// =================================================================================================

/// THE WHOLE-OBJECT READ PATH ASKS FOR EVERY COMPONENT OF ONE KEY, AND THE ID CANNOT EXPRESS THAT.
///
/// `bucket_index_component_block_addresses(shard, model_id, object_key)` takes a key and NO
/// component and returns one pair per component. This drives it over a real container and shows that
/// the pages it returns resolve to as many distinct ids as there are pages -- so an id-keyed index
/// could not have answered it: the caller would have to know the components in order to compute the
/// ids, and the components are what it called to find out.
///
/// THE DENOMINATOR IS PRINTED AND THE POPULATION ASSERTED, because this reader driven over an empty
/// store returns an empty vector and reads exactly like a reader that answered.
///
/// rust-internal: drives one index read over a seeded store
#[test]
fn the_whole_object_read_path_asks_for_every_component_of_one_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = identity_engine(dir.path());
    load_operator_shard(&engine);
    seed_hash_containers(&engine);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    println!(
        "\n=== bucket_index_component_block_addresses over {CONTAINER_KEYS} container keys ==="
    );
    let mut answered = 0usize;
    for k in 0..CONTAINER_KEYS {
        let object_key = format!("h{k}");
        let pairs = crate::engine::bucket_store::bucket_index_component_block_addresses(
            shard,
            FIXTURE_KIND.as_str(),
            &object_key,
        );
        let components: BTreeSet<Option<String>> = pairs
            .iter()
            .map(|(component, _)| component.as_ref().map(|name| name.to_string()))
            .collect();
        let ids: BTreeSet<u64> = pairs
            .iter()
            .map(|(component, _)| {
                stable_block_object_id(
                    1,
                    FIXTURE_KIND.as_str(),
                    &object_key,
                    component.as_deref(),
                )
            })
            .collect();
        println!(
            "  {object_key:<6} pairs={:<4} distinct components={:<4} distinct ids={:<4}",
            pairs.len(),
            components.len(),
            ids.len()
        );
        assert_eq!(
            MEMBERS_PER_KEY,
            pairs.len(),
            "{object_key} answered with {} pairs, not the {MEMBERS_PER_KEY} components the seed \
             wrote. An empty or short answer here reads exactly like a reader that resolved",
            pairs.len()
        );
        assert_eq!(
            pairs.len(),
            ids.len(),
            "{object_key}'s {} pages resolve to {} distinct ids. They are supposed to be one id per \
             page -- the whole reason this reader cannot be handed an id instead of the key is that \
             the id set is as large as the answer and is computable only FROM the answer",
            pairs.len(),
            ids.len()
        );
        answered += 1;
    }
    assert_eq!(
        CONTAINER_KEYS, answered,
        "{answered} of {CONTAINER_KEYS} keys were driven through the read path; a loop that \
         examined fewer proves less than it says"
    );
}

// =================================================================================================
// 4. THE ENTRY DID NOT MOVE, and the node's duplicated slot id is declined with the arithmetic.
// =================================================================================================

/// THE ENTRY IS STILL 72 BYTES AND ITS RECONSTRUCTION SAYS SO, and `BucketNode::routing_bucket` is
/// declined on the layout rule rather than on a total.
///
/// This module removes no field, so the width is asserted as UNCHANGED -- and asserted as a
/// reconstruction, because a literal total cannot tell a structure that is full from one that is
/// half padding, and those two want opposite fixes.
///
/// AND THE RECONSTRUCTION IS ORDER-INDEPENDENT, which this test had to be corrected to be. It was
/// first written asserting `object_key` sits at offset 0, on the strength of being declared first.
/// It sits at 48: rustc orders `repr(Rust)` fields by alignment and size, not by declaration, so the
/// three pointer-width fields are a contiguous head in SOME order and the one-byte tail follows.
/// Nothing but `offset_of!` would have said so -- and a hand-written field list with `size_of` of
/// the WRONG type compiles silently, which is why every width below is taken from the field itself
/// through `field_width` rather than from a type named at the call site.
///
/// AND THE NODE'S DUPLICATED SLOT ID IS WORTH ZERO, arithmetically. `BucketMap` is keyed by the
/// routing bucket and `BucketNode` stores it again, but the node's tail is
/// `u32 + BucketLayoutState + BucketFlags` -- six bytes in eight, so TWO are already slack. Dropping
/// the four-byte field leaves two, which rounds to the same eight: the node stays as wide as it is.
/// It crosses the step only if all THREE tail members go, and `flags` is live residency state that
/// release and reload both set while `layout` is read across the dump path.
///
/// rust-internal: measures declarations, no product behaviour
#[test]
fn the_entry_is_unchanged_and_the_nodes_duplicated_slot_id_is_worth_nothing() {
    use crate::engine::state::{
        BlockIndexMap, BucketFlags, BucketLayoutState, BucketNode, BucketTtl, DeletedObjectIndex,
        ObjectIndex,
    };
    use std::mem::{align_of, offset_of, size_of};

    // --- THE ENTRY, RECONSTRUCTED FROM ITS OWN FIELDS, EACH WIDTH TAKEN FROM THE FIELD. ---
    //
    // `field_width` infers `T` from the field it is handed, so a field that stops being an
    // `Arc<str>` changes the number here. A list naming the types at the call site would not.
    fn field_width<T>(_field: &T) -> usize {
        size_of::<T>()
    }
    let sample = entry_named("container", Some("body"));
    let widths = [
        ("object_key", field_width(&sample.object_key), offset_of!(BlockIndex, object_key)),
        ("model_id", field_width(&sample.model_id), offset_of!(BlockIndex, model_id)),
        ("component", field_width(&sample.component), offset_of!(BlockIndex, component)),
        ("address", field_width(&sample.address), offset_of!(BlockIndex, address)),
        ("dirty", field_width(&sample.dirty), offset_of!(BlockIndex, dirty)),
        ("deleted", field_width(&sample.deleted), offset_of!(BlockIndex, deleted)),
        ("log_backed", field_width(&sample.log_backed), offset_of!(BlockIndex, log_backed)),
    ];
    println!("\n=== the page entry, field by field at its REAL offset ===");
    for (name, width, offset) in &widths {
        println!("  {name:<12} {width:>3} B at offset {offset:>3}");
    }
    let field_sum: usize = widths.iter().map(|(_, width, _)| width).sum();
    let padding = size_of::<BlockIndex>() - field_sum;
    println!(
        "  {:>3} B = {field_sum} B of field + {padding} B padding",
        size_of::<BlockIndex>()
    );

    // --- NO FIELD OVERLAPS OR OVERRUNS, which is what makes the sum a reconstruction and not a
    //     coincidence. Order-independent: `repr(Rust)` places fields by alignment, not by
    //     declaration, and this test was corrected after assuming otherwise.
    for (name, width, offset) in &widths {
        assert!(
            offset + width <= size_of::<BlockIndex>(),
            "{name} occupies {offset}..{} of a {} B structure",
            offset + width,
            size_of::<BlockIndex>()
        );
        for (other, other_width, other_offset) in &widths {
            if name == other {
                continue;
            }
            assert!(
                offset + width <= *other_offset || other_offset + other_width <= *offset,
                "{name} at {offset}..{} overlaps {other} at {other_offset}..{}; the field sum is \
                 counting one byte twice",
                offset + width,
                other_offset + other_width
            );
        }
    }
    assert!(
        padding < align_of::<BlockIndex>(),
        "the entry carries {padding} B of padding against an alignment of {}. A whole step of \
         padding is a step the structure could lose without giving anything up, and this module's \
         verdict is that it cannot",
        align_of::<BlockIndex>()
    );

    // --- AND THE KEY IS STILL A FAT POINTER, which is what a handle would be replacing. ---
    assert_eq!(
        size_of::<Arc<str>>(),
        field_width(&sample.object_key),
        "the object key is {} B, not the {} B of a shared-pointer-to-text. Every figure in the \
         handle measurement is about swapping that pointer for a four-byte index",
        field_width(&sample.object_key),
        size_of::<Arc<str>>()
    );
    let without_the_key = field_sum - field_width(&sample.object_key);
    println!(
        "  dropping object_key entirely -> {without_the_key} B of field, rounds to {} B (the entry \
         would lose {} B)",
        without_the_key.div_ceil(align_of::<BlockIndex>()) * align_of::<BlockIndex>(),
        size_of::<BlockIndex>()
            - without_the_key.div_ceil(align_of::<BlockIndex>()) * align_of::<BlockIndex>()
    );

    // --- THE NODE, RECONSTRUCTED, so the tail arithmetic below is about this structure. ---
    let node_eight_aligned = size_of::<BucketTtl>()
        + 3 * size_of::<u64>()
        + size_of::<ObjectIndex>()
        + size_of::<DeletedObjectIndex>()
        + size_of::<BlockIndexMap>();
    let node_tail = size_of::<u32>() + size_of::<BucketLayoutState>() + size_of::<BucketFlags>();
    let node_tail_rounded = node_tail.div_ceil(8) * 8;
    assert_eq!(
        size_of::<BucketNode>(),
        node_eight_aligned + node_tail_rounded,
        "the node is {} B and its fields reconstruct to {} B; the tail arithmetic below would be \
         about a different structure",
        size_of::<BucketNode>(),
        node_eight_aligned + node_tail_rounded
    );
    assert_eq!(
        8,
        align_of::<BucketNode>(),
        "the node is not eight-aligned, so every step argument here is about a different rule"
    );

    // --- AND THE SLOT ID IS FREE TO KEEP. ---
    let without_slot_id = node_tail - size_of::<u32>();
    let without_all_three = 0usize;
    println!(
        "=== the bucket node's tail ===\n  holds {node_tail} B of field in {node_tail_rounded} B \
         ({} B already slack)\n  drop routing_bucket        -> {without_slot_id} B of field, \
         rounds to {} B, node stays {} B\n  drop all three tail fields -> {without_all_three} B of \
         field, rounds to {} B, node loses {} B",
        node_tail_rounded - node_tail,
        without_slot_id.div_ceil(8) * 8,
        size_of::<BucketNode>(),
        without_all_three.div_ceil(8) * 8,
        node_tail_rounded - without_all_three.div_ceil(8) * 8
    );
    assert!(
        node_tail_rounded > node_tail,
        "the node's tail holds {node_tail} B in {node_tail_rounded} B with no slack at all, so \
         dropping a field from it WOULD cross the step and this decline is the wrong shape"
    );
    assert_eq!(
        node_tail_rounded,
        without_slot_id.div_ceil(8) * 8,
        "dropping routing_bucket takes the node's tail from {node_tail_rounded} B to {} B. It is \
         supposed to round to the same step, which is the whole reason the field is kept rather \
         than retired",
        without_slot_id.div_ceil(8) * 8
    );
    assert_eq!(
        8,
        node_tail_rounded - without_all_three.div_ceil(8) * 8,
        "emptying the node's tail entirely saves {} B rather than one eight-byte step, so the \
         combination this declines is not the combination described",
        node_tail_rounded - without_all_three.div_ceil(8) * 8
    );
}
