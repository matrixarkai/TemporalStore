// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TWO ELEMENTS OF ONE COMPACTED PAGE MUST NOT SHARE A HANDLE.
//!
//! # THE FAILURE THIS PINS, WHICH HAS HAPPENED
//!
//! `container_pages` batches a container's elements onto one page at compaction, so N elements
//! share one address. Each still gets its own entry in `block_index`, and the map is keyed by
//! `block_index_handle`, which hashes model id, object key, **component**, and five address terms.
//!
//! Siblings on a batched page agree on the model id, the object key and every address term. **The
//! component is the only input that differs.** Remove it and the hash sees identical inputs, the
//! two entries land on one key, and the second `insert` overwrites the first.
//!
//! This campaign has already observed that collapse at the forty-fields-one-page shape: forty hash
//! fields on one page arrived as one entry, thirty-nine lost. `index_log.rs` states the mechanism
//! directly -- the component is *"the only discriminator between two elements of one folded page …
//! dropping it collapses them onto one slot. It cannot go until the entry stops being
//! per-element."*
//!
//! # WHY THIS TEST EXISTS BEFORE THE MIGRATION AND NOT AFTER
//!
//! The entry is being relocated into the model map, where the element's identity becomes the
//! level-2 **key** and the entry stops having to identify itself. At that point the handle's job
//! shrinks to separating siblings at one address, and a two-byte element index does it. That is a
//! correct design, and it is also exactly the shape in which a mistake is invisible: the entry
//! compiles, the map accepts both inserts, and one of them is simply not there afterwards.
//!
//! So the discriminator is pinned **now**, while `component` still provides it, and the pin is
//! written so it keeps holding when the index replaces the component -- it asserts that siblings
//! differ, not that they differ *because of the component*.
//!
//! # THE MIRROR IS PROVED FAITHFUL BEFORE IT IS BELIEVED
//!
//! To show the component is what separates them, the handle has to be recomputed without it --
//! which needs a mirror of `block_index_handle`. A mirror that is not checked against the real
//! function measures itself: #2057's own notes record a mirror whose assertion went red because
//! `repr(Rust)` reordered it. So the mirror here is first required to **reproduce the real handle
//! exactly** while it still includes the component, and only then run with the component dropped.

#![allow(clippy::all)]
use super::*;
use crate::block_store::BlockAddress;
use crate::engine::state::{block_index_handle, BlockIndex};
use crate::engine::storage_bucket_internals::StoredModelKind;

/// A mirror of `block_index_handle`'s inputs, with the component optionally omitted.
///
/// `include_component` is the only difference between the two runs, so the comparison below
/// isolates that one term rather than any other difference between mirror and original.
fn mirrored_handle(page: &BlockIndex, include_component: bool) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    page.model_id.hash(&mut hasher);
    page.object_key.hash(&mut hasher);
    if include_component {
        page.component.as_deref().hash(&mut hasher);
    }
    page.address.block_slab_id().hash(&mut hasher);
    page.address.offset().hash(&mut hasher);
    page.address.length().hash(&mut hasher);
    page.address.block_id().unwrap_or_default().hash(&mut hasher);
    page.address.generation().unwrap_or_default().hash(&mut hasher);
    hasher.finish()
}

/// One batched page: every element shares this address.
fn batched_page_address() -> BlockAddress {
    BlockAddress::from_parts(42, 1_048_576, 4096, Some(7), None)
}

fn sibling(object_key: &str, field: &str, address: BlockAddress) -> BlockIndex {
    BlockIndex {
        object_key: std::sync::Arc::from(object_key),
        model_id: StoredModelKind::Hash,
        component: Some(std::sync::Arc::from(field)),
        address,
        dirty: false,
        deleted: false,
        log_backed: false,
    }
}

/// rust-internal: hashes entries this test builds, no product behaviour
#[test]
fn forty_elements_of_one_compacted_page_have_forty_distinct_handles() {
    const OBJECT: &str = "tenant/7/object/000000123";
    const FIELDS: usize = 40;

    let address = batched_page_address();
    let siblings: Vec<BlockIndex> = (0..FIELDS)
        .map(|index| sibling(OBJECT, &format!("field-{index:02}"), address.clone()))
        .collect();

    // THE SHAPE IS THE SUBJECT, so it is asserted rather than assumed: every element must share
    // one address, or this is not a batched page and the collapse below cannot arise.
    let addresses: std::collections::BTreeSet<String> =
        siblings.iter().map(|page| format!("{:?}", page.address)).collect();
    assert_eq!(
        1,
        addresses.len(),
        "the {FIELDS} elements must share ONE address to be a batched page; they span {}",
        addresses.len(),
    );
    let keys: std::collections::BTreeSet<&str> =
        siblings.iter().map(|page| &*page.object_key).collect();
    assert_eq!(1, keys.len(), "they must also share one object key");

    // THE PIN: forty elements, forty handles.
    let real: std::collections::BTreeSet<u64> =
        siblings.iter().map(block_index_handle).collect();
    println!(
        "[sibling-handles] {FIELDS} elements on one address -> {} distinct handle(s)",
        real.len()
    );
    assert_eq!(
        FIELDS,
        real.len(),
        "{} of {FIELDS} elements of one compacted page share a handle, so an insert overwrites a \
         sibling and the map holds fewer elements than were written. This is the forty-fields \
         collapse: thirty-nine lost.",
        FIELDS - real.len(),
    );

    // THE MIRROR IS PROVED FAITHFUL FIRST. With the component included it must reproduce the real
    // handle for every element; otherwise the run below measures the mirror's own drift.
    for page in &siblings {
        assert_eq!(
            block_index_handle(page),
            mirrored_handle(page, true),
            "the mirror does not reproduce `block_index_handle` for {:?}; it has drifted from the \
             original and the dropped-component run below would prove nothing",
            page.component,
        );
    }
    println!("[sibling-handles] mirror reproduces the real handle for all {FIELDS}");

    // AND NOW THE CONTROL: drop the one term and watch them collapse. This is what removing
    // `component` from the entry would do while the entry still has to identify itself.
    let without: std::collections::BTreeSet<u64> =
        siblings.iter().map(|page| mirrored_handle(page, false)).collect();
    println!(
        "[sibling-handles] with the component dropped -> {} distinct handle(s)",
        without.len()
    );
    assert_eq!(
        1,
        without.len(),
        "dropping the component left {} distinct handles rather than 1. If these elements are \
         separable without it then the component is NOT the discriminator here, and the ordering \
         argument this test exists to support is wrong -- which is worth knowing before a \
         migration, not after.",
        without.len(),
    );

    println!(
        "[sibling-handles] the component is the only input separating siblings on one page: \
         {FIELDS} handles with it, 1 without"
    );
}

/// AND THE PIN IS WRITTEN TO SURVIVE THE MIGRATION, which is the point of stating it this way.
///
/// After the relocation the element's identity is the level-2 key and the handle separates
/// siblings by a two-byte element index instead. This asserts the PROPERTY -- siblings at one
/// address get distinct handles -- rather than the mechanism, so it keeps holding when the
/// mechanism is replaced and fails if the replacement does not actually separate them.
///
/// rust-internal: hashes entries this test builds, no product behaviour
#[test]
fn two_siblings_differing_only_in_their_element_are_separable() {
    let address = batched_page_address();
    let first = sibling("tenant/7/object/1", "alpha", address.clone());
    let second = sibling("tenant/7/object/1", "beta", address);

    assert_eq!(
        format!("{:?}", first.address),
        format!("{:?}", second.address),
        "VACUITY: the two must share an address, or they are not siblings on one page",
    );
    assert_eq!(
        &*first.object_key, &*second.object_key,
        "VACUITY: and one object key",
    );
    assert_ne!(
        first.component, second.component,
        "VACUITY: they must differ in their element, or there is nothing to separate",
    );

    assert_ne!(
        block_index_handle(&first),
        block_index_handle(&second),
        "two elements of one page share a handle. Whatever currently separates siblings -- the \
         component today, an element index after the relocation -- is not doing it, and one of \
         these two will overwrite the other in `block_index`.",
    );
}
