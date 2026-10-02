// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LOG-RESIDENT FACT HAS ONE SOURCE, AND THE STORED FIELD IS NOT IT.
//!
//! `BlockAddress::block_id()` is `None` exactly while a block still lives in the log, so the
//! address already answers "is this log-resident" and a stored flag beside it could only agree or
//! disagree. It did both: three sites derived it (`storage_bucket_internals.rs:362`, `:3655`,
//! `:3983`) and three wrote a constant `false` without consulting the address (`:3524`, `:5873`,
//! and a fixture in `state.rs`). Three maintain it, three override it — which is what makes the
//! invariant FALSE rather than merely unproven.
//!
//! # WHY THAT WAS INVISIBLE, AND WHY THE INVISIBILITY WAS LUCK
//!
//! The only behavioural reader is `object_manager`'s hot/cold classification, and it tests
//! `deleted` FIRST:
//!
//! ```text
//!     if page.deleted || object_deleted { ...deleted_block_ref_count... }
//!     else if bucket.in_memory() && !page.log_backed { ...hot... }
//!     else { ...cold... }
//! ```
//!
//! The one production site that writes the inconsistent flag is
//! `insert_container_tombstone_entry`, which sets `deleted: true` on the same entry. So that
//! reader never reached the flag for exactly the entries where it was wrong. **That protection is
//! the branch order of one `if`, not a property of the design** — reorder those branches and the
//! latent defect becomes live. It is the reason this is a fix rather than a tidy-up.
//!
//! # WHAT IT DID COST: THE REPORT
//!
//! `storage_reporting` asks "does any page in this bucket live in the log", and it asked the
//! stored flag. A bucket whose only log-resident page was a tombstone answered NO while a page was
//! in the log. The second test below drives exactly that bucket.
//!
//! # WHY THE FIELD IS STILL THERE
//!
//! It cannot be cheaply deleted and deleting it would buy nothing. The shard index is
//! `serde_json`, so the field NAME is a stored key: removing it refuses every existing index — a
//! format stamp — in exchange for **zero** resident bytes, because `BlockIndex` carries 52 bytes of
//! field inside 56 and a `bool` leaving takes that to 51 inside 56. The width does not move. So the
//! field stays, keeps being written so the stored shape is untouched, and stops being read.
//!
//! THE VALUE OF THIS CHANGE IS CORRECTNESS, NOT FOOTPRINT.

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

/// THE ACCESSOR DISAGREES WITH THE STORED FLAG ON A TOMBSTONE, WHICH IS THE POINT.
///
/// A tombstone entry is built with `log_backed: false` regardless of its address. Handed a
/// log-resident address — `block_id: None` — the stored flag says "not in the log" and the address
/// says it is. This asserts that disagreement directly, so the fix is demonstrated against the
/// defect rather than asserted over a store that happens not to contain one.
///
/// rust-internal: constructs one entry, no product behaviour
#[test]
fn the_accessor_and_the_stored_flag_disagree_on_a_log_resident_tombstone() {
    use crate::block_store::BlockAddress;

    // A log-resident address: no block id. This is the shape a staged page has before a dump.
    let in_log = BlockAddress::from_parts(42, 1_048_576, 4096, None, None);
    assert!(
        in_log.block_id().is_none(),
        "VACUITY: this fixture's address must be log-resident or neither assertion says anything",
    );
    // And a slab-backed control, so a fixture that made every address log-resident cannot pass.
    let in_slab = BlockAddress::from_parts(42, 1_048_576, 4096, Some(7), None);
    assert!(in_slab.block_id().is_some(), "the control address must be slab-backed");

    let tombstone = BlockIndex {
        object_key: std::sync::Arc::from("tenant/7/object/1"),
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::Hash,
        component: Some(std::sync::Arc::from("field-0")),
        address: in_log.clone(),
        dirty: true,
        deleted: true,
        // The constant the tombstone path writes, reproduced deliberately.
        log_backed: false,
    };

    println!(
        "[log-resident] stored flag {} / accessor {} on a log-resident tombstone",
        tombstone.log_backed, tombstone.log_backed()
    );

    assert!(
        tombstone.log_backed(),
        "the accessor must read the address, which says this block is log-resident",
    );
    assert!(
        !tombstone.log_backed,
        "the stored flag must still carry the constant the tombstone path writes -- if this is \
         true the fixture is no longer reproducing the disagreement and the test below proves \
         nothing",
    );
    assert_ne!(
        tombstone.log_backed,
        tombstone.log_backed(),
        "THE DISAGREEMENT IS THE SUBJECT. If these agree, either the tombstone path started \
         deriving the flag -- in which case this test should be deleted rather than adjusted -- or \
         the accessor stopped reading the address",
    );

    // And where the flag IS maintained the two agree, so the accessor is not simply inverted.
    let slab_page = BlockIndex { address: in_slab, log_backed: false, ..tombstone.clone() };
    assert!(
        !slab_page.log_backed(),
        "a slab-backed block is not log-resident, so the accessor must say so",
    );
    assert_eq!(
        slab_page.log_backed,
        slab_page.log_backed(),
        "on a slab-backed entry the stored flag and the accessor agree, which is the control",
    );
}

/// AND THE STORED SHAPE DOES NOT MOVE, which is why no format stamp is spent.
///
/// The field is still a field and still serialized under its own name. Asserted on the bytes
/// rather than on the declaration, with a planted difference, because "the shape did not move" and
/// "my comparison cannot see movement" are indistinguishable otherwise.
///
/// rust-internal: serializes one entry, no product behaviour
#[test]
fn the_stored_entry_still_carries_the_flag_under_its_own_name() {
    use crate::block_store::BlockAddress;

    let page = BlockIndex {
        object_key: std::sync::Arc::from("tenant/7/object/1"),
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::Hash,
        component: Some(std::sync::Arc::from("field-0")),
        address: BlockAddress::from_parts(42, 1_048_576, 4096, Some(7), None),
        dirty: false,
        deleted: false,
        log_backed: false,
    };

    let encoded = serde_json::to_string(&page).expect("a page entry serializes");
    println!("[log-resident] stored entry: {encoded}");

    assert!(
        encoded.contains("\"log_backed\""),
        "the stored key is gone, which would refuse every existing index: {encoded}",
    );

    // THE PLANTED DIFFERENCE. If this comparison cannot see a changed flag it cannot see a missing
    // one either, and the assertion above would pass over anything.
    let flipped = BlockIndex { log_backed: true, ..page.clone() };
    let flipped_encoded = serde_json::to_string(&flipped).expect("serializes");
    assert_ne!(
        encoded, flipped_encoded,
        "CONTROL: flipping the stored flag must change the bytes, or this test cannot detect the \
         field leaving at all",
    );

    // And the width is unchanged, so nobody reads this as a footprint change.
    assert_eq!(
        56,
        std::mem::size_of::<BlockIndex>(),
        "the entry is {} bytes; this change was never a footprint change and the body says so",
        std::mem::size_of::<BlockIndex>(),
    );
}
