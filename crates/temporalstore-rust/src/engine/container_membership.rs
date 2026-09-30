// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CONTAINER'S MEMBERSHIP, DERIVED FROM ITS PAGES.
//!
//! # WHAT #2028 REFUTED, AND WHAT CHANGED UNDER IT
//!
//! #2028 drove the refutation: twelve set members folded to one page, one member removed, and the
//! page still held twelve. A removal reached the resident map and the page INDEX and did not reach
//! the PAGE, because the page stayed live for its other elements and nothing rewrote it. So the page
//! was a SUPERSET of the membership, and a membership derived from it resurrected the removed
//! element -- which is exactly the over-complete state #2025 enumerated five readers for and fixed,
//! arriving from the other side.
//!
//! What changed is that a removal is now a WRITTEN ITEM. `container_pages`' second shape carries a
//! per-item removal flag, and the removal path appends a page stating it. The page set is therefore
//! no longer a superset of the membership; it is a LOG of it, and this module is the fold over that
//! log.
//!
//! # THE ORDER IS THE WHOLE ARGUMENT
//!
//! A tombstone only outranks the item it removes if the reader knows which came later. Three
//! sequences make that unavoidable rather than a nicety:
//!
//! ```text
//!     add m          page P (live m)                 -> m present
//!     add m, del m   page P (live m), page T (gone)  -> m absent
//!     add m, del m,
//!         add m      P (live), T (gone), L (live)    -> m PRESENT, and only the order says so
//! ```
//!
//! Take "a tombstone always wins" and the third sequence loses a member that is there. Take "a live
//! item always wins" and the second keeps one that is not. Neither is a shortcut past the ordering.
//!
//! THE ORDER IS THE APPEND POSITION, `(block_slab_id, offset)`, and it is a fact about the block
//! store rather than a convention this module hopes for. `roll_slab_inner` sets the next slab id to
//! `next_from_current.max(next_from_disk)` where both terms are at least the current id plus one, so
//! slab ids strictly increase and are NEVER REUSED; within a slab `write_offset` only advances. So
//! the pair is a strict total order and it equals the order the pages were written in.
//! `the_fold_orders_pages_by_append_position_and_not_by_the_order_it_was_handed_them` asserts that from the store rather
//! than from this comment.
//!
//! # A READ THAT FAILS IS NOT AN ELEMENT THAT IS ABSENT
//!
//! `insert_timestamped_secondary_view` is the existing template for a page-derived view, and #2028
//! fixed exactly this on it: a failed read used to produce an empty series, and because
//! `context_events` and `context_indexes` are `#[serde(default, skip_serializing)]` the derived view
//! is the ONLY copy -- so an unreadable page became a silently empty answer with no second place to
//! check. Every failure here is counted and NONE is swallowed: [`DerivedMembership::is_complete`]
//! answers false whenever any page could not be read or could not be walked, and a caller that would
//! install a derived membership must refuse on an incomplete one. The counts are public so a guard
//! can floor them at zero over an exercise it drove itself, rather than asserting a total that a
//! swallowed failure would also produce.
//!
//! # WHAT A PAGE OF THE FIRST SHAPE MEANS HERE
//!
//! A `LiveOnly` page cannot say an element was removed. Its silence about an element therefore means
//! NOTHING -- not "present", not "absent" -- and it is counted separately so a store still holding
//! pre-#2028 pages is visible as such rather than being read as authoritative. Its items are still
//! applied as live, which is correct: they were written as live and the shape has no other kind.

use std::collections::{BTreeMap, BTreeSet};

use super::container_pages::{
    component_from_element_key, decode_container_page, ContainerPageDecode, ContainerPageShape,
};
use crate::block_store::BlockAddress;

/// What the pages of one container say its membership is.
#[derive(Debug, Default, Clone)]
pub(super) struct DerivedMembership {
    /// Component to value, for every element the pages say is present.
    pub(super) live: BTreeMap<String, Vec<u8>>,
    /// Components the pages say were REMOVED and nothing later re-added. Kept rather than discarded
    /// because it is what a tombstone-collection decision is made from, and because a guard
    /// distinguishing "the page names it gone" from "no page mentions it" needs both sides.
    pub(super) removed: BTreeSet<String>,
    /// Distinct page addresses walked.
    pub(super) pages_read: usize,
    /// Pages whose bytes could not be read at all.
    pub(super) read_failures: usize,
    /// Pages whose bytes were read and could not be walked as a frame.
    pub(super) undecodable: usize,
    /// Pages that were not framed at all -- a bare value from before `container_pages`. The frame is
    /// what names the element, so such a page contributes NOTHING here and is counted, never guessed
    /// at from the entry that pointed at it.
    pub(super) unframed: usize,
    /// Pages of the first shape, which cannot state a removal.
    pub(super) live_only_pages: usize,
    /// Items whose key its own page's spelling could not render back to a component.
    pub(super) unrenderable_items: usize,
}

impl DerivedMembership {
    /// Whether every page this object resolves to was read and walked.
    ///
    /// AN INCOMPLETE DERIVATION MUST NOT BE INSTALLED. It is not "the membership minus a few" -- a
    /// page that could not be read may have been the one carrying a tombstone, so an incomplete
    /// derivation can be over-complete as easily as under-complete, and there is no direction to
    /// fail safely in. The one safe answer is to decline and let the caller keep what it has.
    pub(super) fn is_complete(&self) -> bool {
        self.read_failures == 0
            && self.undecodable == 0
            && self.unframed == 0
            && self.unrenderable_items == 0
    }

    /// Every failure, as one number, for a message that has to state a denominator.
    pub(super) fn failures(&self) -> usize {
        self.read_failures + self.undecodable + self.unframed + self.unrenderable_items
    }
}

/// The append position of a page, which is the order its items are applied in.
///
/// A TUPLE AND NOT A DERIVED SCALAR. Packing the two into one `u64` is what `BlockAddress` does
/// internally for storage, and doing it again here would invite the reader to wonder whether the
/// offset can overflow into the slab id. The tuple cannot.
pub(super) fn append_position(address: &BlockAddress) -> (u64, u64) {
    (address.block_slab_id(), address.offset())
}

/// Fold a container's pages into the membership they state.
///
/// `pages` is every page this object resolves to, LIVE ENTRIES AND TOMBSTONE ENTRIES ALIKE. The
/// caller supplies them because only it knows how it walked the index; what this function owns is
/// the ordering and the fold, which are the parts that were wrong before there was anything to fold.
///
/// Duplicate addresses are collapsed BEFORE the fold rather than after: a folded page is named by
/// one entry per element it absorbed, so reading it once per entry would apply its items several
/// times. That is harmless for a live item and it is NOT harmless for the page count, which is what
/// a guard measures the fold with -- #2028's own check deduplicated for exactly this reason.
pub(super) fn derive_membership<F>(
    kind: &str,
    pages: impl IntoIterator<Item = BlockAddress>,
    mut read_page: F,
) -> DerivedMembership
where
    F: FnMut(&BlockAddress) -> Option<Vec<u8>>,
{
    let mut derived = DerivedMembership::default();
    if super::container_pages::ElementKeySpelling::for_kind(kind).is_none() {
        // Not a kind whose pages name elements, so there is no membership in them to derive. An
        // empty answer that `is_complete` calls complete, because nothing failed -- the caller asked
        // the wrong question and gets a truthful empty rather than a failure it would have to
        // interpret.
        return derived;
    }

    // ORDER FIRST, THEN READ. The sort is over addresses and costs nothing per page; doing it after
    // reading would mean holding every page's bytes at once.
    let mut ordered: Vec<BlockAddress> = pages.into_iter().collect();
    ordered.sort_by_key(append_position);
    ordered.dedup_by_key(|address| append_position(address));

    // What the most recent page to mention each component said about it. A tombstone and a value are
    // the same kind of statement here -- the later one simply replaces the earlier -- which is why
    // there is one map and not a set beside it.
    let mut latest: BTreeMap<String, Option<Vec<u8>>> = BTreeMap::new();

    for address in &ordered {
        let Some(bytes) = read_page(address) else {
            // COUNTED, NOT SKIPPED. #2028's fix to `insert_timestamped_secondary_view` is the
            // recorded reason: a read failure that produces no entry and no count is a derived view
            // that is silently short, and the derived view is the only copy.
            derived.read_failures += 1;
            continue;
        };
        derived.pages_read += 1;
        match decode_container_page(&bytes) {
            ContainerPageDecode::Framed {
                spelling,
                shape,
                items,
            } => {
                if shape == ContainerPageShape::LiveOnly {
                    derived.live_only_pages += 1;
                }
                for item in items {
                    let Some(component) = component_from_element_key(spelling, &item.key) else {
                        // A key its own page's spelling cannot render is a corrupt page, not an
                        // absent element -- the same reading `component_from_element_key`'s own
                        // doc comment gives it.
                        derived.unrenderable_items += 1;
                        continue;
                    };
                    latest.insert(
                        component,
                        if item.deleted {
                            None
                        } else {
                            Some(item.value)
                        },
                    );
                }
            }
            ContainerPageDecode::NotFramed => {
                derived.unframed += 1;
            }
            ContainerPageDecode::Corrupt(_) => {
                derived.undecodable += 1;
            }
        }
    }

    for (component, value) in latest {
        match value {
            Some(value) => {
                derived.live.insert(component, value);
            }
            None => {
                derived.removed.insert(component);
            }
        }
    }
    derived
}

/// Whether a compaction round may DROP the tombstones it is rewriting.
///
/// # THE RULE
///
/// A tombstone may be dropped only by a round that rewrites the container's ENTIRE page set. If one
/// page of the object survives the round outside the rewrite, the tombstone stays -- whatever that
/// page happens to contain.
///
/// # WHY THE NAIVE RULE RESURRECTS, WHICH IS THE SAME DEFECT IN NEW CLOTHES
///
/// The naive rule is "a fold has rewritten these elements, so the tombstones among them are spent".
/// It is wrong because a fold does NOT see a container's whole page set. Two mechanisms in this tree
/// leave pages behind, and either is enough:
///
///   * `compact_container_pages_batched` seals a batch at `CONTAINER_BATCH_ELEMENT_CAP` elements or
///     at `container_batch_target_bytes()`, so a container larger than one batch is rewritten into
///     SEVERAL pages by one round; and
///   * it skips any element whose address `CompactionRewriteStats::should_relocate` declines, so a
///     round can leave a page in place for reasons that have nothing to do with membership.
///
/// So: member `m` lives in page A, is removed into tombstone T, and a round folds T into a new page
/// B while A is not relocated. Drop the tombstone as it goes into B and the pages are A (live m) and
/// B (silent about m) -- and `derive_membership` puts `m` back. `a_tombstone_dropped_while_an_older_page_survives_resurrects_its_element`
/// drives exactly that sequence and asserts the resurrection, then asserts this rule declines it.
///
/// # WHAT THE COMPARISON DESIGN DOES, WHICH IS THE SAME ANSWER ARRIVED AT DIFFERENTLY
///
/// It decides `remove_tombstone` by asking whether the bucket it picked to compact is the LAST one --
/// its buckets are ordered oldest-last, so the last bucket is the one with nothing older outside it.
/// And its other path dumps the whole model to page 0 and deletes every other page, where the
/// question does not arise because nothing survives to name anything. Both are this rule: a tombstone
/// goes when the rewrite is total.
///
/// # A NOTE ON WHAT THIS TREE'S FOLD CAN CURRENTLY SEE
///
/// `compact_container_pages_batched` takes its elements from the RESIDENT maps (`shard.sets` and the
/// other three), and a removed element is not in them. So the fold does not visit a tombstone at all
/// today: it neither collects one nor risks dropping one, and a tombstone survives every round
/// untouched. That is SAFE and it is not collection -- tombstones accumulate until something walks
/// the index rather than the resident map. This function is the rule that walk will have to obey, and
/// it is written and driven now so that the walk cannot be added without it.
pub(super) fn may_drop_tombstones(
    pages_in_container: usize,
    pages_being_rewritten: usize,
    batches_this_round: usize,
) -> bool {
    // ASSERTED AS A CONJUNCTION OF THREE, and none of the three is implied by the others:
    // rewriting every page is not enough if the round splits them into several pages, because a
    // tombstone dropped into batch 1 is not seen by batch 2; and a single batch is not enough if it
    // does not cover every page. The denominator is checked too -- a container with no pages makes
    // the first two agree at zero, which would let a round over nothing claim a total rewrite.
    pages_in_container > 0
        && pages_being_rewritten == pages_in_container
        && batches_this_round == 1
}
