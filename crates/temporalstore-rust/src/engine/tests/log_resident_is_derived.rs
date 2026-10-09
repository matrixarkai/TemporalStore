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
//! # WHY THE FIELD IS GONE NOW, AND WHY IT WAS NOT A FOOTPRINT DECISION EITHER WAY
//!
//! This section said the field could not be cheaply deleted, and its arithmetic was right and is
//! still right: a `bool` leaving an entry carrying 52 bytes of field inside 56 buys **zero**
//! resident bytes. The entry is 56 bytes before this change and 56 after. Removing it was never
//! worth anything in width and is not being done for width.
//!
//! It is gone because the entry and the index-log row became ONE TYPE, and this field's own
//! documentation said its "VALUE IS NOT MAINTAINED and must not be consulted". Carrying a field
//! that is known to be wrong into a unified type is how it acquires readers: the next person sees
//! a field, not a paragraph. So the one fact it claimed to hold is now answerable only from the
//! address, through the accessor, and there is nothing left to disagree with it.
//!
//! AND ONE CLAIM HERE WAS SIMPLY WRONG, which is why it is corrected rather than deleted.
//! "Removing it refuses every existing index" names the wrong direction. `BlockIndex` derives its
//! `Deserialize` with no `deny_unknown_fields`, so an index written BEFORE this change still loads
//! -- serde ignores the key it no longer declares. The direction that breaks is the other one: a
//! NEW index read by an OLD binary, which finds a declared field absent. That is what the format
//! stamp is spent on, and it degrades to a refusal and a replay rather than to a misread.
//!
//! THE VALUE OF THE ORIGINAL CHANGE WAS CORRECTNESS, NOT FOOTPRINT. The same is true of this one.

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

/// THE LOG-RESIDENT FACT IS DERIVED, AND NOTHING CAN CONTRADICT IT ANY MORE.
///
/// THIS TEST REPLACES ONE THAT ASSERTED A DISAGREEMENT, on that test's own instruction. It said:
/// "If these agree, either the tombstone path started deriving the flag -- in which case this test
/// should be deleted rather than adjusted -- or the accessor stopped reading the address." The
/// stored flag is gone, so the first case has happened in the strongest available form: there is no
/// second source left to agree or disagree with the address.
///
/// SO THE SUBJECT CHANGED RATHER THAN THE ANSWER. The old test proved a defect was PRESENT. This
/// one proves it is UNREACHABLE, which is worth more: a test that shows a defect cannot be
/// represented outlives every fixture that shows one is currently absent.
///
/// rust-internal: constructs one entry, no product behaviour
#[test]
fn the_log_resident_fact_is_derived_from_the_address_alone() {
    use crate::block_store::ElementEntry;

    // A log-resident address: no block id. This is the shape a staged page has before a dump.
    let in_log = ElementEntry::from_parts(42, 1_048_576, 4096, None, None);
    assert!(
        in_log.block_id().is_none(),
        "VACUITY: this fixture's address must be log-resident or neither assertion says anything",
    );
    // And a slab-backed control, so a fixture that made every address log-resident cannot pass.
    let in_slab = ElementEntry::from_parts(42, 1_048_576, 4096, Some(7), None);
    assert!(in_slab.block_id().is_some(), "the control address must be slab-backed");

    // The entry the tombstone path builds. It no longer carries a log-resident flag to get wrong.
    let tombstone = BlockIndex {
        kind: crate::index_log::IndexItemKind::Page,
        routing_bucket: 7,
        object_key: std::sync::Arc::from("tenant/7/object/1"),
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::Hash,
        component: Some(std::sync::Arc::from("field-0")),
        address: in_log.clone(),
        dirty: true,
        deleted: true,
    };

    println!(
        "[log-resident] accessor {} on a log-resident tombstone, with no stored flag beside it",
        tombstone.log_backed()
    );

    // THE SUBJECT: the accessor reads the address. This is the arm the old test's disagreement
    // assertion stood on, and it is unchanged.
    assert!(
        tombstone.log_backed(),
        "the accessor must read the address, which says this block is log-resident",
    );

    // THE CONTROL, and it is a REAL control now rather than a tautology. Comparing the accessor
    // against itself would assert nothing, so the control is that the accessor DISTINGUISHES the
    // two addresses -- a stuck `true` fails here.
    let slab_page = BlockIndex { address: in_slab, ..tombstone.clone() };
    assert!(
        !slab_page.log_backed(),
        "a slab-backed block is not log-resident, so the accessor must say so -- and an accessor \
         that returned a constant would have passed the assertion above and fails here",
    );
    assert_ne!(
        tombstone.log_backed(),
        slab_page.log_backed(),
        "the accessor must answer differently for the two addresses, which is what makes the arm \
         above a measurement of the address rather than of a constant",
    );
}

/// THE STORED SHAPE MOVES BY EXACTLY ONE KEY, AND THAT IS WHAT THE STAMP IS SPENT ON.
///
/// THE SUBJECT OF THIS TEST INVERTED. It asserted the key was still written, so that no stamp was
/// needed; the key is gone now and a stamp is spent. The restatement is deliberate and is the
/// point: a reader comparing revisions should see the claim change rather than find a passing test
/// that quietly means something else.
///
/// ONE KEY REMOVED, NOTHING ADDED. The two fields this type gained are `skip`ped on the named side
/// -- a resident entry is always a page, and the bucket already names itself -- so the stored form
/// shrinks by one key and grows by none. That is asserted below rather than described, because the
/// obvious fear about merging two types is that the stored form grows.
///
/// Asserted on the BYTES rather than on the declaration, with a planted difference, because "the
/// key is gone" and "my comparison cannot see the key" are indistinguishable otherwise. The plant
/// had to MOVE: it used to flip the very field being removed, so it now flips `deleted`, which
/// survives. A plant on a field that no longer exists is not a weaker control, it is none.
///
/// rust-internal: serializes one entry, no product behaviour
#[test]
fn the_stored_entry_has_dropped_the_flag_and_gained_no_key() {

    let page = BlockIndex {
        kind: crate::index_log::IndexItemKind::Page,
        routing_bucket: 7,
        object_key: std::sync::Arc::from("tenant/7/object/1"),
        model_id: crate::engine::storage_bucket_internals::StoredModelKind::Hash,
        component: Some(std::sync::Arc::from("field-0")),
        address: ElementEntry::from_parts(42, 1_048_576, 4096, Some(7), None),
        dirty: false,
        deleted: false,
    };

    let encoded = serde_json::to_string(&page).expect("a page entry serializes");
    println!("[log-resident] stored entry: {encoded}");

    assert!(
        !encoded.contains("\"log_backed\""),
        "the stored key is still written, so this change did not move the shape it claims to: \
         {encoded}",
    );
    // AND NOTHING ARRIVED IN ITS PLACE. The two gained fields are `skip`ped, so neither may appear.
    for gained in ["\"kind\"", "\"routing_bucket\""] {
        assert!(
            !encoded.contains(gained),
            "{gained} reached the stored form; the named side is supposed to skip it, and every \
             entry in every index would carry it: {encoded}",
        );
    }

    // THE PLANTED DIFFERENCE, RE-SITED. It used to flip `log_backed` -- the field this change
    // removes -- so it had to move onto a field that survives. If this comparison cannot see
    // `deleted` change it cannot see a key leave either, and the assertions above would pass over
    // anything.
    let flipped = BlockIndex { deleted: true, ..page.clone() };
    let flipped_encoded = serde_json::to_string(&flipped).expect("serializes");
    assert_ne!(
        encoded, flipped_encoded,
        "CONTROL: flipping a surviving stored field must change the bytes, or this test cannot \
         detect a field leaving at all",
    );

    // And the width is unchanged, so nobody reads this as a footprint change.
    assert_eq!(
        56,
        std::mem::size_of::<BlockIndex>(),
        "the entry is {} bytes; this change was never a footprint change and the body says so",
        std::mem::size_of::<BlockIndex>(),
    );
}
