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
