// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER A RECONSTRUCT DROPS THE REFS IT JUST INVALIDATED.
//!
//! # WHY THIS ARM EXISTS: THE CALL IT DEFENDS SHIPPED COMMENTED OUT
//!
//! `install_reconstructed` ends by pruning every ref that names a block the rebuilt index does not
//! hold. That call was disabled by a one-line bisecting scaffold --
//!
//! ```text
//! // BISECT: prune disabled.
//! // self.prune_refs_to_the_pages_the_index_holds();
//! ```
//!
//! -- and the scaffold was COMMITTED, in the same change that moved each object's refs into its
//! slot. So what shipped was merge-and-never-prune, which is none of the three rules that change
//! measured, and the invariant "a ref must name a block the index holds" ran on nothing.
//!
//! **NOTHING CAUGHT IT, AND THAT IS THE POINT OF THIS FILE.** A private method whose only call site
//! is commented out is invisible to the compiler, and disabling a prune keeps too MANY refs, which
//! no assertion was watching. Restoring the call is not a defence -- a comment satisfies any reading
//! that merely looks for the text. What defends it is an arm that FAILS when the call is disabled,
//! which is what this is: re-comment that line and this test goes red.
//!
//! # WHY A STALE REF IS NOT MERELY UNTIDY
//!
//! `BlockKeysByPosition` answers "which object does the block at this position belong to" from these
//! refs. A ref left at a position the index no longer holds can be served as ANOTHER object's
//! identity once something else occupies that position -- so the stored key composed for a block
//! would name the wrong object. The encoder asks the same question from the other side and refuses
//! an index whose block it cannot name.
//!
//! rust-internal: drives this crate's own index installer, no product behaviour

#![allow(clippy::all)]
use super::*;

/// A reconstruct that does not hold the block a carried ref names must drop that ref.
///
/// rust-internal: drives this crate's own index installer, no product behaviour
#[test]
fn a_reconstruct_prunes_the_refs_it_invalidates() {
    use crate::engine::storage_bucket_internals::StoredModelKind;

    // THE CARRIED SIDE: an index holding one object whose row names one block position.
    let mut live = crate::engine::state::CoreIndex::default();
    let object_key: std::sync::Arc<str> = std::sync::Arc::from("pruned-object");
    let filed = crate::ElementEntry::from_parts(1, 64, 4, Some(1), Some(30));
    live.insert_object_block_lookup(7, 4242, &object_key, &block_entry_for(&filed));
    live.adopt_refs_into_slot(StoredModelKind::String, object_key.as_ref());

    let named_before = live
        .object_block_refs("string", object_key.as_ref())
        .map_or(0, crate::engine::state::BlockRefs::len);
    // DENOMINATOR FIRST: a ref that was never there cannot be pruned, and the assertion below
    // would then pass over an index that never held anything.
    assert_eq!(
        1, named_before,
        "the fixture did not leave a ref to invalidate, so this arm would pass for free"
    );

    // THE REBUILT SIDE: a reconstruct that holds NO blocks at all, so the carried ref names a
    // position this index does not have. `objects` is left empty because a reconstruct cannot
    // derive it -- that is the carried field, and `install_reconstructed` asserts as much.
    let rebuilt = crate::engine::state::CoreIndex::default();
    live.install_reconstructed(rebuilt);

    let named_after = live
        .object_block_refs("string", object_key.as_ref())
        .map_or(0, crate::engine::state::BlockRefs::len);

    println!();
    println!("  refs naming a block the index holds: {named_before} before, {named_after} after");
    println!();

    assert_eq!(
        0, named_after,
        "a reconstruct that holds no blocks left {named_after} ref(s) behind, so the prune at the \
         end of `install_reconstructed` did not run. That call shipped once as a commented-out \
         bisecting scaffold -- check it is still a call and not a comment. A ref naming a position \
         the index does not hold can be served as ANOTHER object's identity by \
         `BlockKeysByPosition` once something else occupies that position, and the encoder refuses \
         an index whose block it cannot name."
    );
}

/// A minimal live block entry at the position the ref names.
fn block_entry_for(address: &crate::ElementEntry) -> crate::engine::state::BlockIndex {
    // THROUGH THE CONSTRUCTOR, because the entry no longer has those three as FIELDS: the model
    // spelling and both flags are bits in the word the address lends its holder. The arguments are
    // the same four values in the same order, so this fixture still builds the same entry -- it is
    // only that a struct literal can no longer say so.
    crate::engine::state::BlockIndex::new(
        crate::engine::storage_bucket_internals::StoredModelKind::String,
        address.clone(),
        false,
        false,
    )
}
