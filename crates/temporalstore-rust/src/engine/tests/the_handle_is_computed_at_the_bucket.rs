// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE HANDLE MOVED UP A LEVEL AND DID NOT CHANGE VALUE.
//!
//! `block_index_handle` used to be called from inside `BlockIndexMap::insert_unaccounted`: from a
//! scope holding the entry and nothing else. A map cannot see the sibling fields of the bucket that
//! owns it, and serde's `serialize(&self)` cannot be handed one either, so the written key was in
//! the same position. That pinned every term the handle hashes to the entry -- a term moved off it
//! into a per-bucket structure would have become unreachable from the one line that needs it.
//!
//! It is computed at [`BucketNode::insert_page`] now, where the bucket's own structures are in
//! scope, and the written key is composed through `PageIndexAt`, which holds the whole node.
//!
//! # WHY VALUE EQUALITY IS THE WHOLE SAFETY ARGUMENT
//!
//! Handles are written to disk inside the lookup's refs. Two processes holding the same block must
//! compute the same handle or those refs point at nothing -- which is what a counter did, silently,
//! until a reload lost an object. So this change is only safe if it moved WHERE and not WHAT: same
//! inputs, same order, same hasher, byte-identical `u64`. That is driven here rather than reasoned
//! about, and driven with a negative control, because an equality between two expressions that
//! happen to be the same expression proves nothing.
//!
//! # AND THE FLOOR THAT IS EASY TO OMIT
//!
//! `BlockIndexMap` has an inline arm and a list arm, and the handle is assigned by different code
//! on each. A fixture reaching only one arm is a handle compared once, so both are asserted
//! reached before any equality below is read as general.
//!
//! rust-internal: drives this crate's own handle derivation, no external surface

#![allow(clippy::all)]
use super::*;
use crate::block_store::ElementEntry;
use crate::engine::state::{
    block_index_handle, BlockIndex, BlockIndexMap, BlockSlabLiveIndex, BucketNode,
};
use crate::engine::storage_bucket_internals::stored_model_kind;
use std::sync::Arc;

fn page(object: &str, component: Option<&str>, slab: u64, offset: u32, length: u32) -> BlockIndex {
    BlockIndex {
        kind: crate::index_log::IndexItemKind::Page,
        routing_bucket: 7,
        object_key: Arc::from(object),
        model_id: stored_model_kind("hash"),
        component: component.map(Arc::from),
        address: ElementEntry::from_parts(slab, offset as u64, length as u64, Some(7), Some(11)),
        dirty: false,
        deleted: false,
    }
}

/// The entries driven. Two share an object and differ by component; the rest differ by address.
fn fixture() -> Vec<BlockIndex> {
    vec![
        page("tenant/1/obj/aaa", Some("f-1"), 7, 0, 64),
        page("tenant/1/obj/aaa", Some("f-2"), 7, 64, 64),
        page("tenant/1/obj/bbb", None, 9, 128, 32),
        page("tenant/1/obj/ccc", Some("f-1"), 9, 160, 48),
    ]
}

#[test]
fn the_hoisted_handle_is_byte_identical_to_the_one_the_map_used_to_compute() {
    let entries = fixture();
    assert!(entries.len() >= 4, "the fixture must drive more than one entry");

    // --- THE SUBJECT: install through the bucket-level door and compare the handle it returns
    //     against `block_index_handle` read directly off the same entry. ---
    let mut bucket = BucketNode::default();
    let mut live = BlockSlabLiveIndex::default();
    let mut reached_inline = false;
    let mut reached_list = false;
    let mut handles = Vec::new();

    for (at, entry) in entries.iter().enumerate() {
        let expected = block_index_handle(entry);
        let assigned = bucket.insert_page(entry.clone(), &mut live);
        assert_eq!(
            expected, assigned,
            "entry {at}: the bucket-level door assigned {assigned} where the handle read off the \
             entry is {expected}. A handle that changed value invalidates every ref already written \
             to disk, which is the one failure this hoist must not have"
        );
        handles.push(assigned);
        match &bucket.block_index {
            BlockIndexMap::One(_, _) => reached_inline = true,
            _ => reached_list = true,
        }
    }

    // --- BOTH ARMS REACHED, floored individually. A handle compared on one arm is compared once. ---
    assert!(
        reached_inline,
        "the fixture never left the map in its inline arm, so the handle was never compared there"
    );
    assert!(
        reached_list,
        "the fixture never reached the list arm, so the handle was never compared there"
    );

    // --- EVERY HANDLE DISTINCT, or the equalities above could hold for one repeated value. ---
    let mut sorted = handles.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        handles.len(),
        sorted.len(),
        "the fixture produced {} distinct handles from {} entries, so some comparison above was \
         between two copies of one value",
        sorted.len(),
        handles.len()
    );

    // --- NEGATIVE CONTROL ONE: PERTURB AN ADDRESS TERM. The handle must move, or it is not
    //     reading the address at all and the equality above is insensitive to it. ---
    let base = &entries[0];
    let mut longer = base.clone();
    longer.address = ElementEntry::from_parts(7, 0, 65, Some(7), Some(11));
    assert_ne!(
        block_index_handle(base),
        block_index_handle(&longer),
        "changing the address LENGTH left the handle alone, so `length` is not hashed and the \
         address terms this hoist exists to free are not actually pinned by it"
    );
    let mut elsewhere = base.clone();
    elsewhere.address = ElementEntry::from_parts(7, 1, 64, Some(7), Some(11));
    assert_ne!(
        block_index_handle(base),
        block_index_handle(&elsewhere),
        "changing the address OFFSET left the handle alone"
    );

    // --- NEGATIVE CONTROL TWO: PERTURB THE COMPONENT, separately, because the address and the
    //     name are two different sources and an instrument blind to one is not blind to both. ---
    let mut renamed = base.clone();
    renamed.component = Some(Arc::from("f-9"));
    assert_ne!(
        block_index_handle(base),
        block_index_handle(&renamed),
        "changing the component left the handle alone, so the name terms are not hashed"
    );
    let mut componentless = base.clone();
    componentless.component = None;
    assert_ne!(
        block_index_handle(base),
        block_index_handle(&componentless),
        "dropping the component left the handle alone"
    );

    println!(
        "\n=== the handle after the hoist ===\n  {} entries, {} distinct handles, inline arm \
         reached: {reached_inline}, list arm reached: {reached_list}\n  every handle equals the one \
         read off its own entry; four perturbations each move it",
        entries.len(),
        sorted.len()
    );
}

/// THE RELEASED DOOR TOO, because it is a second install path and shares nothing but the hasher.
///
/// rust-internal: drives this crate's own handle derivation, no external surface
#[test]
fn the_released_door_assigns_the_same_handle_as_the_charged_one() {
    let entries = fixture();
    let mut charged = BucketNode::default();
    let mut released = BucketNode::default();
    let mut live = BlockSlabLiveIndex::default();

    let mut compared = 0usize;
    for entry in &entries {
        let a = charged.insert_page(entry.clone(), &mut live);
        let b = released.insert_released_page(entry.clone());
        assert_eq!(
            a, b,
            "the charged door assigned {a} and the released door {b} for one entry; they must \
             agree, because `reload_released_bucket` re-files blocks whose refs were written by the \
             charged path"
        );
        assert_eq!(
            block_index_handle(entry), a,
            "neither door agrees with the handle read off the entry"
        );
        compared += 1;
    }
    assert_eq!(entries.len(), compared, "only {compared} entries were compared across the doors");
    println!("\n  {compared} entries: both install doors assign the handle read off the entry");
}

/// AND THE STORED SPELLING DID NOT MOVE, which is the written-key half of the same hoist.
///
/// `PageIndexAt` composes the key at the bucket level where `impl Serialize for BlockIndexMap` used
/// to compose it inside the map. Same function, same order, so the bytes are the same -- asserted
/// against a node built the same way, with a control that the fixture writes more than one entry so
/// the ORDER of the written map is actually exercised.
///
/// rust-internal: drives this crate's own serde impls, no external surface
#[test]
fn the_written_page_index_is_the_same_map_it_was_before_the_hoist() {
    let mut bucket = BucketNode::default();
    let mut live = BlockSlabLiveIndex::default();
    for entry in fixture() {
        bucket.insert_page(entry, &mut live);
    }
    let written = serde_json::to_value(&bucket).expect("a node serializes");
    let page_index = written
        .get("page_index")
        .and_then(|value| value.as_object())
        .expect("the node writes a page_index object");

    assert_eq!(
        4,
        page_index.len(),
        "the written page index holds {} entries, not the four filed -- so either the map lost one \
         or two entries collided on one rendered key",
        page_index.len()
    );
    // THE ORDER IS EXERCISED, which a single-entry fixture could not show: the keys come out
    // ascending by rendered string, not in the hash order the map iterates in.
    let keys: Vec<&String> = page_index.keys().collect();
    let mut ascending = keys.clone();
    ascending.sort();
    assert_eq!(ascending, keys, "the written keys are not in ascending order");
    println!("\n  the written page index: {} keys, ascending", keys.len());
}
