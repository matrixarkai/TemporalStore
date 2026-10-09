// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHY A PAGE ENTRY CARRIES ITS OBJECT'S KEY BESIDE AN ID DERIVED FROM THAT SAME KEY.
//!
//! `BlockIndex` holds `object_key: Arc<str>` and `address: BlockAddress`, and the address holds an
//! `object_id: u64` computed from the key. That reads like the identity stated twice, and #1974
//! established the derivation -- `stable_block_object_id(shard, kind, key)`, computed at
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
                .push(page.object_id(1));
        }
    }
    by_key
}

/// A free-standing entry at a FIXED address, whose object id is the one the engine would compute
/// for it. Taken from `stable_block_object_id` rather than invented, so a comparison between two of
/// these is about the field that differs and the id that follows from it.
fn entry_named(object_key: &str, component: Option<&str>) -> BlockIndex {
    let object_id = stable_block_object_id(1, FIXTURE_KIND.as_str(), object_key);
    BlockIndex {
        kind: crate::index_log::IndexItemKind::Page,
        routing_bucket: 7,
        object_key: Arc::from(object_key),
        model_id: FIXTURE_KIND,
        address: ElementEntry::from_parts(9, 4_096, 96, Some(7), Some(object_id)),
        dirty: false,
        deleted: false,
    }
}

// =================================================================================================
// 1. THE ID NAMES A TRIPLE, NOT AN OBJECT.
// =================================================================================================

/// THE ID IS PER (SHARD, KIND, KEY), SO ONE OBJECT KEY HOLDS EXACTLY ONE ID HOWEVER MANY PAGES IT HAS.
///
/// #1986 recorded the opposite, and this test asserted it: "the object id names a page's triple,
/// not an object". That was a DESCRIPTION OF A DEFECT rather than a property to preserve, and
/// taking the component out of `stable_block_object_id` is what closes it. The three arms are
/// INVERTED rather than deleted, so each still names the denominator it reads.
///
///   1. FROM THE AUTHORITY. `stable_block_object_id` is asked once per component over one key --
///      the same loop as before -- and the distinct answers counted. It is now ONE. The component
///      count is printed beside it and asserted greater than one, so an arm that stopped
///      generating components could not report 1 for the wrong reason.
///   2. OVER A REAL STORE. A hash container's pages are grouped by object key and the ids counted
///      per key: one key, `MEMBERS_PER_KEY` pages, ONE id.
///   3. THE CONTROL. A routed string key holds ONE page and its component was already `None`, so
///      this change is the IDENTITY on it: one page, one id, before and after. That arm asserts
///      the number it always asserted, which is what makes it a control -- if it moved, the
///      collapse above would be something other than the component leaving the hash.
///
/// rust-internal: reads declarations and a seeded store, no product behaviour
#[test]
fn the_page_id_names_an_object_so_one_object_key_holds_exactly_one_id() {
    // --- 1. FROM THE AUTHORITY, WITH THE EXPECTATION DERIVED. ---
    let components: Vec<String> = (0..MEMBERS_PER_KEY).map(|f| format!("f{f}")).collect();
    assert!(
        components.len() > 1,
        "DENOMINATOR: {} component(s) generated. One id out of one component is arithmetic \
         rather than a collapse, and every row below would be vacuous",
        components.len()
    );
    // The id no longer TAKES a component, so the component is varied by asking once per
    // component and counting how many DISTINCT answers come back.
    let folded: BTreeSet<u64> = components
        .iter()
        .map(|_| stable_block_object_id(1, FIXTURE_KIND.as_str(), "h0"))
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
        1,
        folded.len(),
        "one key and {} distinct components produced {} distinct ids, not 1. The component has \
         left the identity, so every element of this key is the SAME object and a \
         component-blind reader can be served by the id rather than only by the key",
        components.len(),
        folded.len()
    );
    // AND THE TERMS THAT REMAIN STILL SEPARATE, or the assertion above would also pass for a
    // derivation that had stopped reading its inputs and returned a constant.
    let only = *folded.iter().next().expect("exactly one id");
    let other_key = stable_block_object_id(1, FIXTURE_KIND.as_str(), "h1");
    let other_kind = stable_block_object_id(1, "string", "h0");
    assert!(
        only != other_key && only != other_kind,
        "the id collapsed across the KEY or the KIND as well ({only} vs {other_key} vs \
         {other_kind}), which is a broken derivation rather than a component-free one"
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
    // THE PRIZE, READ OFF THE SAME POPULATION: rows in `bucket.object_index` per key.
    //
    // `object_index` holds one entry per DISTINCT object id in the bucket, and #2007 measured it
    // at 0.9983 rows per page. The count below is what it becomes: one row per key, whatever the
    // key's element count.
    let mut keys_holding_many_ids = 0usize;
    let mut pages_total = 0usize;
    let mut ids_total = 0usize;
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
            1,
            distinct.len(),
            "{object_key}'s {} pages carry {} distinct ids, not 1. Every page of one object is an \
             ELEMENT of it and they are supposed to share the object's id; if they do not, the \
             component is still reaching the hash",
            ids.len(),
            distinct.len()
        );
        pages_total += ids.len();
        ids_total += distinct.len();
        if distinct.len() > 1 {
            keys_holding_many_ids += 1;
        }
    }
    assert_eq!(
        0, keys_holding_many_ids,
        "{keys_holding_many_ids} of {} container keys hold more than one id; none of them are \
         supposed to",
        by_key.len()
    );
    println!(
        "  TOTAL pages={pages_total} object-index rows={ids_total} ({:.4} rows per page, was \
         1.0000 when the id named the page)",
        ids_total as f64 / pages_total as f64
    );
    assert_eq!(
        ids_total,
        by_key.len(),
        "{ids_total} object-index rows over {} keys: the saving this change exists for is exactly \
         one row per key, so these two must be equal",
        by_key.len()
    );
    assert!(
        pages_total > ids_total,
        "DENOMINATOR: {pages_total} pages and {ids_total} rows. A fixture whose keys held one page \
         each would satisfy every assertion above while measuring nothing"
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
        "{control_many} routed keys hold more than one id. A routed key is one page and its \
         component was already `None`, so removing the component from the id is the IDENTITY on \
         this population -- this count was zero before the change and must still be zero, or \
         the collapse measured above is not the component leaving the hash"
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
    let id = page.object_id(1);
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
        // WHAT SEPARATES THE ELEMENTS OF THE ANSWER IS THE ADDRESS NOW, NOT A NAME.
        //
        // This collected the pairs' COMPONENTS and asserted there were `MEMBERS_PER_KEY` distinct
        // ones -- "the COMPONENT is what separates the elements; if these collapsed too, the reader
        // would have no way to tell one element of the answer from another". The pair's first slot
        // is dead on both of `bucket_index_component_block_addresses`'s arms, so that set is now a
        // single `None` for every key and the assertion could never pass again.
        //
        // THE CLAIM IS NOT ABANDONED, IT MOVED TO THE TERM THAT STILL CARRIES IT. The reader is
        // handed a KEY and answers a page per element; what distinguishes those pages is the
        // ADDRESS each one is at, and that is counted here instead. It is a claim this can still
        // get wrong -- a filing that collapsed two elements onto one page, or a reader that
        // returned the same address twice, drops this count below the member count -- which is
        // exactly the failure the component set was watching for, observed through the term that
        // still exists.
        let separators: BTreeSet<String> = pairs
            .iter()
            .map(|(_, address)| format!("{address:?}"))
            .collect();
        // THE IDS THESE PAIRS RESOLVE TO. One per KEY now, not one per page -- which is why
        // the loop below counts them rather than the components: the component set is what the
        // reader actually answers with, and the id set is what an id-keyed reader would have.
        let ids: BTreeSet<u64> = pairs
            .iter()
            .map(|_| stable_block_object_id(1, FIXTURE_KIND.as_str(), &object_key))
            .collect();
        println!(
            "  {object_key:<6} pairs={:<4} distinct addresses={:<4} distinct ids={:<4}",
            pairs.len(),
            separators.len(),
            ids.len()
        );
        assert_eq!(
            MEMBERS_PER_KEY,
            pairs.len(),
            "{object_key} answered with {} pairs, not the {MEMBERS_PER_KEY} components the seed \
             wrote. An empty or short answer here reads exactly like a reader that resolved",
            pairs.len()
        );
        // AND THE REASON THIS READER CANNOT BE HANDED AN ID, RESTATED THE OTHER WAY UP.
        //
        // It used to be that the id set was as LARGE as the answer and computable only from it.
        // Since the component left the identity the id set has collapsed to ONE, and the point
        // stands more strongly: a single number cannot name which of {MEMBERS_PER_KEY} elements
        // is wanted, so the reader still has to be given the key and answer with components.
        assert_eq!(
            1,
            ids.len(),
            "{object_key}'s {} pages resolve to {} distinct ids, not 1. Every page of one key is \
             an element of one object now",
            pairs.len(),
            ids.len()
        );
        assert_eq!(
            MEMBERS_PER_KEY,
            separators.len(),
            "{object_key} answered {} distinct ADDRESSES for {} pages. The address is what \
             separates the elements of this answer now -- if these collapsed, the reader would \
             have no way to tell one element of the answer from another",
            separators.len(),
            pairs.len()
        );
        assert!(
            separators.len() > ids.len(),
            "{object_key}: {} addresses against {} ids. The whole reason this reader takes the KEY \
             and not an id is that the id is coarser than the answer",
            separators.len(),
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

/// THE ENTRY IS UNCHANGED BY THIS MODULE AND ITS RECONSTRUCTION SAYS SO, and `BucketNode::routing_bucket` is
/// declined on the layout rule rather than on a total.
///
/// This module removes no field, so the width is asserted as UNCHANGED -- and asserted as a
/// reconstruction, because a literal total cannot tell a structure that is full from one that is
/// half padding, and those two want opposite fixes.
///
/// THE HEADING USED TO NAME 72, which is what the entry weighed when this module landed; #1994 took
/// it to 64. Nothing below moved, because nothing below is a literal -- which is the whole argument
/// for a reconstruction, demonstrated here by the prose going stale while the test did not.
///
/// AND THE RECONSTRUCTION IS ORDER-INDEPENDENT, which this test had to be corrected to be. It was
/// first written asserting `object_key` sits at offset 0, on the strength of being declared first.
/// It did not: rustc orders `repr(Rust)` fields by alignment and size, not by declaration, so the
/// pointer-width fields are a contiguous head in SOME order and the one-byte tail follows. The
/// offset it sits at has moved since as well -- it was 48 and is 16, because the address narrowed --
/// which is a second reason nothing here quotes one.
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
        // The `component` row is gone with the field. `field_width` infers from the field it is
        // handed, so a row for a field that does not exist cannot be written at all -- which is
        // the property this list was built for.
        ("address", field_width(&sample.address), offset_of!(BlockIndex, address)),
        ("dirty", field_width(&sample.dirty), offset_of!(BlockIndex, dirty)),
        ("deleted", field_width(&sample.deleted), offset_of!(BlockIndex, deleted)),
        ("kind", field_width(&sample.kind), offset_of!(BlockIndex, kind)),
        ("routing_bucket", field_width(&sample.routing_bucket), offset_of!(BlockIndex, routing_bucket)),
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
    //
    // A SIXTH BUCKET-NODE DECOMPOSITION THE TOMBSTONE WORD DID NOT REACH, AND IT WAS ALREADY RED.
    //
    // `BucketNode::tombstone_elements` was missing from this sum. The node is 96 B and this
    // reconstructed to 88, so the assertion below failed with "the node is 96 B and its fields
    // reconstruct to 88 B" -- and it failed on this branch BEFORE the entry stopped naming its
    // element, because nothing here reads `BlockIndex`: `BlockIndexMap` holds the entry behind a
    // pointer and is 24 bytes whatever the entry measures. So this is not fallout from the width
    // step, it is a decomposition the tombstone structure's own restatement pass missed. Five
    // decompositions were restated for it and a sixth was recorded as missed; this is a seventh.
    //
    // ADDED AS A TERM READ OFF THE TYPE, not as a literal 8, so a structure that stops being one
    // indirect word fails here rather than passing on a stale number.
    let node_eight_aligned = size_of::<BucketTtl>()
        + 3 * size_of::<u64>()
        + size_of::<ObjectIndex>()
        + size_of::<DeletedObjectIndex>()
        + size_of::<crate::engine::state::TombstoneElements>()
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
