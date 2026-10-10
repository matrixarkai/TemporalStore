// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE CORE INDEX PUTS ON THE WIRE, ENUMERATED BY THE COMPILER AND NOT BY A COUNT.
//!
//! # WHY THIS EXISTS BEFORE THE CHANGE IT GUARDS
//!
//! `Serialize for CoreIndex` is DERIVED today. A proposal to let the page index be serialized with
//! a sibling field in scope -- so that a term which has left the entry can still be written --
//! needs that impl to be HAND-WRITTEN, because serde hands a `Serialize` impl no context and a
//! bucket's serializer therefore cannot see anything but the bucket.
//!
//! A HAND-WRITTEN IMPL THAT OMITS A FIELD COMPILES AND WRITES A NARROWER WIRE FORMAT. That is the
//! worst failure available here: the encode succeeds, the file is accepted, and what is missing
//! only surfaces when something tries to read it back. So the guard is not a nicety to add
//! afterwards -- it is what makes the change safe to make, and it is therefore landed FIRST,
//! against the derive, while the derive is still the thing being described.
//!
//! NOTHING HERE CHANGES THE IMPL, SO NO BYTES MOVE AND NO FORMAT STAMP IS OWED. This file only
//! observes what the current impl emits. Whoever replaces the derive owes the stamp question
//! separately, and owes it on whether the BYTES changed rather than on whether the code did: an
//! impl emitting the identical shape owes nothing, while one that reorders or renames a key does.
//!
//! # THE GUARD IS A DESTRUCTURING, NOT A NUMBER
//!
//! A field count is a literal that goes stale silently. An EXHAUSTIVE DESTRUCTURING does not: a
//! field added to `CoreIndex` makes the pattern below fail to compile, naming the field it does
//! not mention. So the enumeration is maintained by the compiler, and every field is forced to
//! declare in one place whether it reaches the wire. `ShardState` already carries this shape for
//! its own packing pass, with the skipped fields annotated in the pattern -- see the destructuring
//! around `state.rs:5765`.
//!
//! # AND THE WIRE IS OBSERVED, NOT READ OFF THE ATTRIBUTES
//!
//! `#[serde(skip)]` on a field is a claim about the wire; the wire is what the encoder emits. So
//! the key set is taken from a real encode of a populated index, and the five skipped fields are
//! POPULATED BEFORE ENCODING and asserted to come back at their defaults. That population is the
//! control: without it, "the skipped fields come back empty" would pass on an index where they
//! were empty all along, which is a test that cannot fail.
//!
//! SIX FIELDS, AND EXACTLY ONE REACHES THE WIRE. That measurement is the reason the hand-written
//! impl is a small change rather than a thirty-field transcription: the omission hazard applies to
//! one field, and the context it needs -- the object lookup -- is a sibling field of the same
//! struct, so there is nothing to plumb through.

use std::sync::Arc;

use crate::engine::state::{BucketNode, BucketTtl, CoreIndex};

/// A `CoreIndex` with EVERY ONE of its six fields non-default.
///
/// The on-wire field is given two buckets with distinguishable contents so the round trip has
/// something to compare field by field. The five skipped fields are populated for the reason in
/// the module docs: they are the control that makes "skipped" an observation.
fn an_index_with_every_field_populated() -> CoreIndex {
    let mut index = CoreIndex::default();

    let mut first = BucketNode::default();
    first.routing_bucket = 7;
    first.ttl_ms = BucketTtl::from_ms(Some(1_234));
    first.dirty_generation = 42;
    first.set_meta_loaded(true);
    first.set_in_memory(true);

    let mut second = BucketNode::default();
    second.routing_bucket = 9;
    second.ttl_ms = BucketTtl::ABSENT;
    second.dirty_generation = 7;
    second.set_meta_loaded(true);
    second.set_dirty(true);

    index.bucket_map.insert(7, first);
    index.bucket_map.insert(9, second);

    // The five skipped fields, each made non-default so the round trip below can tell "skipped"
    // from "was empty anyway".
    index.block_slab_live.mark_ready();
    let model: Arc<str> = Arc::from("string");
    let object: Arc<str> = Arc::from("an-object");
    index.object_block_lookup.file(
        &model,
        &object,
        crate::engine::state::BlockLookupRef {
            routing_bucket: 7,
            block_ref_key: 99,
        },
    );
    index.kind_pool.insert(Arc::clone(&model));
    index.object_component_block_refs = Some(3);
    index.released_buckets.insert(11);

    index
}

#[test]
fn every_field_of_the_core_index_declares_whether_it_reaches_the_wire() {
    let index = an_index_with_every_field_populated();

    // ===========================================================================================
    // THE GUARD. A field ADDED to `CoreIndex` fails to compile HERE, naming itself, rather than
    // being silently dropped from a hand-written encoder's output. Nothing is bound with `..`,
    // which is the whole mechanism: `..` would make this pattern accept a field set it has never
    // been told about.
    // ===========================================================================================
    let CoreIndex {
        // ON THE WIRE -- the only one. Everything a stored index carries from this struct is
        // inside this field.
        bucket_map,
        // SKIPPED, `#[serde(skip)]`. Derived per-slab live tallies, seeded wherever the index is
        // rebuilt wholesale.
        block_slab_live,
        // SKIPPED, `#[serde(default, skip_serializing)]` -- DECODED IF PRESENT BUT NEVER WRITTEN,
        // which is a different thing from `skip` and is why it is annotated separately. Rebuilt
        // from the bucket map on load.
        object_block_lookup,
        // SKIPPED, `#[serde(skip)]`. An interning pool for the model spellings.
        kind_pool,
        // SKIPPED, `#[serde(skip)]`. A derived total, `None` meaning not established.
        object_component_block_refs,
        // SKIPPED, `#[serde(skip)]`. Which buckets have had their resident blocks released.
        released_buckets,
    } = &index;

    // The populated-ness of all six is asserted, so a future edit that empties the fixture turns
    // the controls below vacuous LOUDLY rather than quietly.
    assert_eq!(bucket_map.len(), 2, "the on-wire field must be populated");
    assert!(block_slab_live.is_ready(), "skipped field must be populated");
    assert!(
        object_block_lookup.get("string", "an-object").is_some(),
        "skipped field must be populated"
    );
    assert!(!kind_pool.is_empty(), "skipped field must be populated");
    assert_eq!(
        *object_component_block_refs,
        Some(3),
        "skipped field must be populated"
    );
    assert!(!released_buckets.is_empty(), "skipped field must be populated");

    // ===========================================================================================
    // THE WIRE, OBSERVED. One key, and it is the bucket map.
    // ===========================================================================================
    let encoded = serde_json::to_value(&index).expect("a core index serializes");
    let object = encoded.as_object().expect("a core index encodes as a map");
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    println!("the core index puts these keys on the wire: {keys:?}");
    assert_eq!(
        keys,
        vec!["bucket_map"],
        "exactly one of the six fields reaches the wire; a change here is a change to the \
         stored shape and owes the format stamp question"
    );
}

#[test]
fn an_encode_then_decode_keeps_every_wire_field_and_drops_every_omitted_one() {
    let before = an_index_with_every_field_populated();

    // A ROUND TRIP AND NOT A BYTE GOLDEN. A golden records whatever today's encoder emits, so it
    // moves with any change to the encoder and cannot say whether the change was lossless. Encode
    // then decode, and compare what came back against what went in.
    let encoded = serde_json::to_string(&before).expect("a core index serializes");
    let after: CoreIndex = serde_json::from_str(&encoded).expect("a core index deserializes");

    // THE ON-WIRE FIELD, COMPARED TERM BY TERM rather than by a length or a debug string.
    assert_eq!(
        after.bucket_map.len(),
        before.bucket_map.len(),
        "the bucket count must survive"
    );
    for (routing_bucket, original) in &before.bucket_map {
        let reloaded = after
            .bucket_map
            .get(routing_bucket)
            .unwrap_or_else(|| panic!("bucket {routing_bucket} must survive the round trip"));
        assert_eq!(reloaded.routing_bucket, original.routing_bucket);
        assert_eq!(reloaded.ttl_ms.ms(), original.ttl_ms.ms());
        assert_eq!(reloaded.dirty_generation, original.dirty_generation);
        assert_eq!(reloaded.dirty(), original.dirty());
        assert_eq!(reloaded.deleted(), original.deleted());
        assert_eq!(reloaded.meta_loaded(), original.meta_loaded());
        assert_eq!(reloaded.loading(), original.loading());
        assert_eq!(reloaded.in_memory(), original.in_memory());
        assert_eq!(reloaded.layout, original.layout);
        assert_eq!(reloaded.block_index.len(), original.block_index.len());
        assert_eq!(
            reloaded.object_index.object_count(),
            original.object_index.object_count()
        );
    }

    // THE SKIPPED FIELDS, AND THIS IS THE HALF THE POPULATION ABOVE EXISTS FOR. Each went in
    // non-default and must come back at its default, which is what "skipped" means on the wire.
    assert!(
        !after.block_slab_live.is_ready(),
        "a skipped derived tally must not survive a round trip"
    );
    assert!(
        after.object_block_lookup.is_empty(),
        "a skip_serializing lookup must not be written, so it must come back empty"
    );
    assert!(
        after.kind_pool.is_empty(),
        "a skipped interning pool must not survive"
    );
    assert_eq!(
        after.object_component_block_refs, None,
        "a skipped derived total must come back unestablished rather than zero"
    );
    assert!(
        after.released_buckets.is_empty(),
        "a skipped release record must not survive"
    );

    // NOT ASSERTED, AND DELIBERATELY: a bucket's two first-dirty sequence numbers. The node's own
    // visitor forces them to zero on every decode -- "Transient by declaration", at its closing
    // construction -- so asserting they survive would assert the opposite of the documented
    // behaviour, and asserting they are zero would restate the decoder to itself.
}

// =================================================================================================
// WHETHER THE ENTRY CAN LOSE ITS IDENTITY WITHOUT THE BYTES MOVING -- SETTLED HERE RATHER THAN
// REASONED ABOUT, BECAUSE THE TIDY VERSION OF THE STORY IS WRONG IN TWO PLACES.
//
// THE STORY UNDER TEST. The stored page index is STRING-KEYED and the key contains the object key,
// so the hope is that the encoder can keep writing the same bytes while sourcing the identity from
// the sibling object lookup, and the decoder can recover the identity from the map key. If that
// held, the entry could shed its field with nothing on disk moving and no format stamp owed.
//
// IT DOES NOT HOLD, AND THE TWO TESTS BELOW ARE WHY.
//
//   1. THE OBJECT KEY IS A MEMBER OF THE ENTRY ON THE WIRE. `kind` and `routing_bucket` carry
//      `#[serde(skip)]`; the object key does not. So removing the field removes a member from
//      every stored entry -- that IS a byte change -- unless the encoder keeps emitting it from
//      somewhere else.
//
//   2. THE MAP KEY IS NOT SELF-DELIMITING, so the decoder cannot be handed the job instead. The
//      stored key is `kind:object:component:slab:offset:length:block_id:generation` joined with
//      bare colons, and an object key MAY CONTAIN A COLON -- a context entity is filed under
//      `{collection_key}:{entity_hash}`, stated twice in `storage_bucket_internals.rs` at `:2754`
//      and `:3195`. The second test constructs two DIFFERENT (object key, component) pairs that
//      produce the IDENTICAL key, which is the proof that no parser can invert it in general.
//      The lookup's own flat key is length-prefixed for exactly this reason; this one is not.
//
// WHAT THAT LEAVES, STATED SO THE NEXT READER DOES NOT RE-DERIVE IT. With `component` fixed at
// `None` -- which every entry's is today -- the trailing five terms are numeric and a RIGHT-ANCHORED
// parse would work. That is a property held at a distance rather than one the format enforces, and
// the collision below is what it is being held against. So "recover the identity from the key" is
// available only while a property the format does not check continues to hold.
//
// SO THE ROUTE THAT KEEPS THE BYTES IDENTICAL IS THE THIRD ONE: the entry sheds the FIELD, the
// encoder keeps emitting the MEMBER from the sibling lookup, and the decoder reads that member into
// a temporary it uses for the handle and the lookup rebuild without storing it. Nothing on disk
// moves and no stamp is owed -- and the cost is that both directions become hand-written, not just
// the encoder.
// =================================================================================================

#[test]
fn a_page_entry_carries_its_object_key_as_a_member_on_the_wire() {
    use crate::engine::state::{BlockIndex, BlockSlabLiveIndex};

    let mut bucket = BucketNode::default();
    bucket.routing_bucket = 3;
    bucket.set_meta_loaded(true);
    let mut live = BlockSlabLiveIndex::default();
    bucket.insert_page(
        BlockIndex {
            object_key: Arc::from("an-object"),
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::from_stored_name(
                "string",
            )
            .expect("`string` is a stored model spelling"),
            address: Default::default(),
            dirty: false,
            deleted: false,
        },
        &mut live,
    );

    let encoded = serde_json::to_value(&bucket).expect("a bucket node serializes");
    let page_index = encoded
        .get("page_index")
        .and_then(serde_json::Value::as_object)
        .expect("a node writes its page index as a map");
    assert_eq!(page_index.len(), 1, "one page was inserted");
    let (stored_key, entry) = page_index.iter().next().expect("one entry");
    let entry = entry
        .as_object()
        .expect("a page entry is written as a map");
    let mut members: Vec<&str> = entry.keys().map(String::as_str).collect();
    members.sort_unstable();
    println!("a page entry writes these members: {members:?}");
    println!("under this stored key: {stored_key}");

    // THE CLAIM. The object key is a MEMBER, so taking the field off the entry takes a member off
    // the wire unless the encoder supplies it from elsewhere. The two skipped terms are absent
    // from this list, which is the control showing the list distinguishes skipped from written.
    assert!(
        members.contains(&"object_key"),
        "the object key is written as a member of the entry: {members:?}"
    );
    assert!(
        !members.contains(&"kind"),
        "`kind` is skipped, so its absence is what makes the assertion above informative"
    );
    assert!(
        !members.contains(&"routing_bucket") && !members.contains(&"routing_slot"),
        "the routing bucket is skipped and injected by the node, so it is absent too"
    );

    // AND THE STORED KEY CARRIES THE CHARACTERS TOO, which is the half the hopeful story rested on.
    assert!(
        stored_key.contains("an-object"),
        "the stored key contains the object key: {stored_key}"
    );
}

#[test]
fn the_stored_page_key_is_not_self_delimiting_so_no_parser_can_invert_it() {
    use crate::index_log::block_ref_key_from_parts;

    // TWO DIFFERENT IDENTITIES, ONE KEY. The separator is a bare colon and both the object key and
    // the component may contain one, so the boundary between them is not recoverable.
    let colon_in_the_object = block_ref_key_from_parts("string", "a:b", None, 0, 0, 0, 0, 0);
    let colon_in_the_component = block_ref_key_from_parts("string", "a", Some("b:"), 0, 0, 0, 0, 0);
    println!("object `a:b`, no component -> {colon_in_the_object}");
    println!("object `a`, component `b:` -> {colon_in_the_component}");
    assert_eq!(
        colon_in_the_object, colon_in_the_component,
        "two different identities must collide for the format to be shown ambiguous"
    );

    // THE CONTROL, and without it the collision above proves nothing about the FORMAT: two
    // identities that differ in a way the format DOES delimit must produce different keys, or the
    // builder would simply be returning a constant.
    let plainly_different = block_ref_key_from_parts("string", "a", None, 0, 0, 0, 0, 0);
    assert_ne!(
        plainly_different, colon_in_the_object,
        "the builder must distinguish identities it can delimit, or the collision is vacuous"
    );

    // AND THE NUMERIC TAIL IS DELIMITED, which is why a right-anchored parse works TODAY while
    // every component is absent. Asserted so that the narrow availability of that parse is
    // recorded as a property being relied on rather than a guarantee.
    let later_generation = block_ref_key_from_parts("string", "a:b", None, 0, 0, 0, 0, 1);
    assert_ne!(
        later_generation, colon_in_the_object,
        "the trailing terms are positional and do distinguish"
    );
}


/// A DELETED PAGE'S IDENTITY IS IN THE BUCKET'S TOMBSTONE ROW, AND THAT ROW COMPOSES THE SAME
/// STORED KEY THE ENTRY DOES.
///
/// # WHY THIS IS THE MISSING HALF OF THE PRE-VERIFICATION
///
/// The encode-side check that the identity held BESIDE a page agrees with the identity held ON it
/// drew on one source, the object lookup, and a tombstone is never in that lookup --
/// `insert_object_block_lookup` returns early on a deleted page. So the check skipped every
/// deleted page, and it was measured skipping them: over the hundred dump tests it compared
/// 1,998,922 pages and NONE of them was deleted, while the encoder was reached by deleted pages
/// under `tombstone` and `container_tombstone`. A swap from check to source cannot be called
/// pre-verified on a class the check never looked at.
///
/// # SO THE SECOND SOURCE IS ASSERTED HERE, DIRECTLY
///
/// This builds one bucket holding a LIVE page and a DELETED one of the same object, files both
/// through the index's own `insert_object_block_lookup` -- so the exclusion of the deleted page is
/// the production rule and not a fixture's choice -- and asserts three things: the lookup names
/// one of the two, the bucket's tombstone row names the other, and the row composes exactly the
/// stored key the encode wrote for it.
///
/// THE FIRST ASSERTION IS THE CONTROL. Without it a tombstone could be in the lookup after all,
/// and the row would be a second copy of an identity already available rather than the only one.
#[test]
fn the_identity_beside_a_deleted_page_is_its_tombstone_row_and_it_composes_the_same_stored_key() {
    use crate::block_store::ElementEntry;
    use crate::engine::state::BlockIndex;
    use crate::engine::storage_bucket_internals::stored_model_kind;

    const SLAB: u64 = 3;
    const LIVE_OFFSET: u64 = 0;
    const DEAD_OFFSET: u64 = 64;
    const LENGTH: u64 = 27;
    const OBJECT: &str = "an-object";
    const KIND: &str = "set";
    const ELEMENT: &str = "m0";

    let mut index = CoreIndex::default();
    let mut node = BucketNode::default();
    node.routing_bucket = 7;
    node.set_meta_loaded(true);
    node.set_in_memory(true);
    index.bucket_map.insert(7, node);

    let live = BlockIndex {
        object_key: Arc::from(OBJECT),
        model_id: stored_model_kind(KIND),
        address: ElementEntry::from_parts(SLAB, LIVE_OFFSET, LENGTH, Some(1), None),
        dirty: false,
        deleted: false,
    };
    let dead = BlockIndex {
        object_key: Arc::from(OBJECT),
        model_id: stored_model_kind(KIND),
        address: ElementEntry::from_parts(SLAB, DEAD_OFFSET, LENGTH, Some(2), None),
        dirty: true,
        deleted: true,
    };

    let (live_handle, dead_handle) = {
        let CoreIndex {
            bucket_map,
            block_slab_live,
            ..
        } = &mut index;
        let node = bucket_map.get_mut(&7).expect("the bucket was just inserted");
        let live_handle = node.insert_page(live.clone(), block_slab_live);
        let dead_handle = node.insert_page(dead.clone(), block_slab_live);
        node.record_tombstone_element(OBJECT, stored_model_kind(KIND), ELEMENT, &dead.address);
        (live_handle, dead_handle)
    };
    assert_ne!(
        live_handle, dead_handle,
        "two pages at different offsets must file under different handles, or this bucket holds one page"
    );

    let object: Arc<str> = Arc::from(OBJECT);
    index.insert_object_block_lookup(7, live_handle, &object, &live);
    index.insert_object_block_lookup(7, dead_handle, &object, &dead);

    // THE CONTROL: the lookup holds the live page and REFUSES the deleted one.
    let refs = index
        .block_refs_for(KIND, OBJECT, None)
        .expect("the live page files a ref");
    assert_eq!(
        1,
        refs.len(),
        "a deleted page must not reach the lookup, or the tombstone row is not the only source"
    );
    assert_eq!(
        live_handle, refs[0].block_ref_key,
        "the one ref must be the LIVE page's"
    );

    // AND THE ROW HOLDS THE DELETED PAGE'S IDENTITY.
    let row_key = {
        let node = index.bucket_map.get(&7).expect("the bucket is still there");
        let row = node
            .tombstone_element_at(&dead.address)
            .expect("the removal filed a row against the deleted page");
        crate::index_log::block_ref_key_from_parts(
            row.model_id.as_str(),
            &row.object_key,
            None,
            dead.address.block_slab_id(),
            dead.address.offset(),
            dead.address.length(),
            dead.address.block_id().unwrap_or_default(),
            dead.address.generation().unwrap_or_default(),
        )
    };

    let encoded = serde_json::to_value(&index).expect("a core index serializes");
    let pages = encoded
        .get("bucket_map")
        .and_then(|buckets| buckets.get("7"))
        .and_then(|node| node.get("page_index"))
        .and_then(|pages| pages.as_object())
        .expect("the bucket writes a page index")
        .clone();

    assert_eq!(
        2,
        pages.len(),
        "the encode must write BOTH pages, or the deleted one is not on the wire at all: {:?}",
        pages.keys().collect::<Vec<_>>()
    );
    assert!(
        pages.contains_key(&row_key),
        "the key the TOMBSTONE ROW composes must be one the encode wrote, or the row is not a \
         source for this page: row composed {row_key}, encode wrote {:?}",
        pages.keys().collect::<Vec<_>>()
    );
}

#[test]
fn the_legacy_spelling_of_the_one_wire_field_still_loads_and_an_unknown_one_does_not() {
    // WHY THIS SITS BESIDE THE GUARD RATHER THAN ONLY IN ANOTHER MODULE.
    //
    // The one wire field carries `alias = "slots"`, and a HAND-WRITTEN `Deserialize` -- which is
    // the change this file exists to make safe -- is precisely the edit that drops a legacy alias
    // with no other symptom: everything still compiles, the round trip above still passes, because
    // a round trip only ever writes the CURRENT spelling and reads it back.
    //
    // `core_index_loads_legacy_bucket_page_field_names` does cover this today. Relying on it means
    // the guard that sits beside the impl does not assert the thing the impl is most likely to
    // break, so the coverage is restated here where the destructuring and the key set already are.
    //
    // THE CONTROL IS THE WHOLE TEST, AND WITHOUT IT THIS ASSERTS NOTHING. The field is
    // `#[serde(default)]`, so an index whose top-level key the decoder does not recognise decodes
    // SUCCESSFULLY, with an empty map and no error. "It decoded" is therefore true of any input at
    // all. The discriminating pair is: the legacy spelling must come back POPULATED, and an
    // unrecognised spelling must come back EMPTY. Only the two together say the alias is doing
    // work.
    //
    // AND THE BUCKETS ARE TAKEN FROM A REAL ENCODE rather than hand-written as JSON, so this test
    // does not carry its own copy of the node's stored shape -- a copy that would go stale the
    // first time the node's own encoder moved, and would then be testing a shape nothing writes.
    let index = an_index_with_every_field_populated();
    let encoded = serde_json::to_value(&index).expect("a core index serializes");
    let mut fields = encoded
        .as_object()
        .expect("a core index encodes as a map")
        .clone();
    let buckets = fields
        .remove("bucket_map")
        .expect("the one wire field is written under its current spelling");
    assert!(
        fields.is_empty(),
        "nothing else is on the wire, so nothing else needs re-spelling here: {:?}",
        fields.keys().collect::<Vec<_>>()
    );

    // THE LEGACY SPELLING. An index written before the field was renamed is keyed `slots`.
    let mut legacy = serde_json::Map::new();
    legacy.insert("slots".to_string(), buckets.clone());
    let from_legacy: CoreIndex = serde_json::from_value(serde_json::Value::Object(legacy))
        .expect("an index keyed with the legacy spelling must still decode");
    assert_eq!(
        from_legacy.bucket_map.len(),
        index.bucket_map.len(),
        "the legacy spelling must still load the bucket map, not quietly default it"
    );

    // THE CONTROL. A spelling the decoder knows nothing about must be IGNORED -- which is what
    // makes the assertion above a statement about the alias rather than about `default`.
    let mut unknown = serde_json::Map::new();
    unknown.insert("slotz".to_string(), buckets);
    let from_unknown: CoreIndex = serde_json::from_value(serde_json::Value::Object(unknown))
        .expect("an unrecognised key is ignored, which is what lets a newer index load into an older engine");
    assert!(
        from_unknown.bucket_map.is_empty(),
        "an unrecognised spelling must leave the map at its default; if this were populated the \
         test above would pass whatever the alias did"
    );
}
