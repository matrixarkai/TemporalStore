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
//!
//! AND THE CALLER NOW REFUSES ONE, which is what the count was missing. Counting it was never the
//! protection it looked like: `live_only_pages` had THREE occurrences -- the field, its increment,
//! and a `println!` in one test -- so no production decision read it, and a derivation over a v1
//! page returned a SUPERSET of the membership, resurrecting every element a removal had retired,
//! while `is_complete` answered TRUE because nothing had failed. [`derive_trusted_membership`] is the
//! entry point that answers `None` for it, [`DerivedMembership::is_authoritative`] is the predicate,
//! and [`MembershipDerivePath`] counts BOTH arms so a refusal is not mistaken for a container that
//! had no pages.
//!
//! # NOTHING MIGRATES A V1 PAGE, SO THE REFUSAL IS NOT A TRANSIENT STATE
//!
//! There is no upgrade pass. `encode_container_page` is the only writer of either magic and it writes
//! the SECOND unconditionally, so every page written from #2040 onward is v2 and no code rewrites an
//! existing v1 page *because* it is v1. A v1 page is replaced only incidentally, when a compaction
//! round happens to relocate the elements on it and re-encodes them -- and `should_relocate` declines
//! a page already on the target slab and, on a periodic round, every page outside the drain set. So a
//! container can hold a v1 page indefinitely.
//!
//! THE CONSEQUENCE IS A BOUND ON EVERYTHING DOWNSTREAM, and it is stated here rather than implied: a
//! store still holding v1 pages can NEVER have a trusted derivation over the containers that hold
//! them, so page-derived membership is a property a store EARNS by having been fully rewritten, not
//! one it has because the binary is new. `membership_derive_path_counts`' second element is how an
//! operator finds out which kind of store they have.

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

    /// Whether this derivation STATES the membership, rather than merely not having failed.
    ///
    /// # A DIFFERENT QUESTION FROM `is_complete`, AND NOT FOLDED INTO IT
    ///
    /// [`Self::is_complete`] asks about the READ: was every page this object resolves to fetched and
    /// walked. A `LiveOnly` page is fetched perfectly and walked perfectly. What it cannot do is
    /// state a REMOVAL -- `container_pages`' own words for the first shape are "Every item is live;
    /// the shape cannot say otherwise" -- so a derivation that saw one is a SUPERSET of the
    /// membership: every element any page ever named live, including the ones a later removal
    /// retired.
    ///
    /// Folding the two would make a v1 page read as a read FAILURE, which it is not, and would move
    /// what `is_complete`'s four counters mean under the guards that already assert them at zero.
    /// Two predicates, two questions, and a caller that must not act on a superset asks this one.
    ///
    /// # WHAT WAS WRONG BEFORE THIS PREDICATE EXISTED
    ///
    /// `live_only_pages` was incremented and counted and NOTHING DECIDED ANYTHING WITH IT -- its
    /// three occurrences on main were this field, that increment, and one `println!` in a test. So a
    /// derivation over a pre-#2040 page resurrected every element a removal had retired and reported
    /// itself COMPLETE while doing it, because `is_complete` answers true for a page that read fine.
    /// The superset was not a risk the caller accepted; it was one no caller could see.
    pub(super) fn is_authoritative(&self) -> bool {
        self.is_complete() && self.live_only_pages == 0
    }
}

/// WHICH WAY A DERIVATION OVER A CONTAINER'S PAGES WENT.
///
/// # WHY A COUNTED REFUSAL, AND NOT AN `Err` AND NOT A BARE `Option`
///
/// Shaped on [`super::persistence::IndexLoadPath`], which exists for exactly this class of problem
/// and says so: "the refusal is a `return Ok(None)` that the caller cannot tell from an absent
/// index". The same two states collide here. A derivation REFUSED because a v1 page is present and a
/// derivation over a container with NO PAGES both hand the caller nothing, and they are opposites --
/// the first is a container whose tombstones can never be collected and whose membership cannot be
/// trusted, the second is a container with nothing to collect and nothing to distrust. One counter
/// each is what makes them different facts rather than one silence.
///
/// NOT AN `Err`: there is no message a caller can act on and nothing to propagate. The only correct
/// response to any refusal is to keep what the index already said, which is what the caller would
/// have done anyway, so an error would be a `Result` every call site discards.
///
/// NOT A BARE `Option`: that is the mistake above, with the type system's blessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipDerivePath {
    /// Every page was read, walked, and carried the SECOND shape. The membership may be acted on.
    Trusted,
    /// At least one page carried the FIRST shape, which cannot state a removal, so the derivation is
    /// a superset of the membership. See [`DerivedMembership::is_authoritative`].
    RefusedLiveOnlyPage,
    /// A page could not be read, could not be walked as a frame, was not framed at all, or named an
    /// element its own page's spelling could not render back. Refused for the reason
    /// [`DerivedMembership::is_complete`] gives: such a derivation can be over-complete as easily as
    /// under-complete, so there is no direction to fail safely in.
    RefusedIncomplete,
    /// The container resolved to no readable page at all, or to a kind whose pages carry no element
    /// key. NOT A REFUSAL: nothing was derived because there was nothing to derive.
    NoPages,
}

static MEMBERSHIP_DERIVES_TRUSTED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static MEMBERSHIP_DERIVES_REFUSED_LIVE_ONLY: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static MEMBERSHIP_DERIVES_REFUSED_INCOMPLETE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static MEMBERSHIP_DERIVES_NO_PAGES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn note_membership_derive(path: MembershipDerivePath) {
    let counter = match path {
        MembershipDerivePath::Trusted => &MEMBERSHIP_DERIVES_TRUSTED,
        MembershipDerivePath::RefusedLiveOnlyPage => &MEMBERSHIP_DERIVES_REFUSED_LIVE_ONLY,
        MembershipDerivePath::RefusedIncomplete => &MEMBERSHIP_DERIVES_REFUSED_INCOMPLETE,
        MembershipDerivePath::NoPages => &MEMBERSHIP_DERIVES_NO_PAGES,
    };
    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// (trusted, refused-live-only, refused-incomplete, no-pages) since the last reset.
///
/// A TUPLE AND NOT A SUM, for the reason `index_load_path_counts` gives: a guard holding only the
/// total could not tell a container that was trusted from one that was refused, which is the entire
/// distinction this exists to draw.
pub fn membership_derive_path_counts() -> (u64, u64, u64, u64) {
    (
        MEMBERSHIP_DERIVES_TRUSTED.load(std::sync::atomic::Ordering::Relaxed),
        MEMBERSHIP_DERIVES_REFUSED_LIVE_ONLY.load(std::sync::atomic::Ordering::Relaxed),
        MEMBERSHIP_DERIVES_REFUSED_INCOMPLETE.load(std::sync::atomic::Ordering::Relaxed),
        MEMBERSHIP_DERIVES_NO_PAGES.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Forget the counts, so a test measures its own derivations rather than the suite before it.
pub fn reset_membership_derive_path_counts() {
    MEMBERSHIP_DERIVES_TRUSTED.store(0, std::sync::atomic::Ordering::Relaxed);
    MEMBERSHIP_DERIVES_REFUSED_LIVE_ONLY.store(0, std::sync::atomic::Ordering::Relaxed);
    MEMBERSHIP_DERIVES_REFUSED_INCOMPLETE.store(0, std::sync::atomic::Ordering::Relaxed);
    MEMBERSHIP_DERIVES_NO_PAGES.store(0, std::sync::atomic::Ordering::Relaxed);
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

/// Fold a container's pages into the membership they state, and REFUSE one that cannot be trusted.
///
/// # THE PRODUCTION ENTRY POINT, AND WHY IT IS A SECOND FUNCTION
///
/// [`derive_membership`] is the FOLD: it answers whatever the pages said, counts everything that
/// went wrong, and judges nothing. That is the right shape for it -- a guard wants the counts and
/// wants to see a superset for itself, which is how
/// `a_live_only_page_makes_the_derivation_a_superset_and_the_caller_refuses_it` drives this at all.
/// This is the function that DECIDES, and every production caller goes through it so that the
/// decision is in one place rather than re-derived at each site from the raw counters.
///
/// # THE ORDER OF THE ARMS IS PART OF THE CONTRACT
///
/// A derivation can be incomplete AND have seen a v1 page. `RefusedIncomplete` is noted first,
/// because it is the stronger statement: an incomplete derivation is unusable whatever the shapes
/// were, while a complete v1 derivation is a precisely known superset. Exactly one arm is noted per
/// call, so the four counters sum to the number of derivations and a guard can floor them against a
/// denominator it drove itself. `a_derivation_that_is_both_incomplete_and_live_only_counts_once_as_incomplete`
/// pins the choice, so a later reader changing the order has to change a test that says why.
///
/// `NoPages` RETURNS `None` TOO, and that is not the same as a refusal collapsing into it: the two
/// hand back the same value because there is nothing to act on either way, and they are told apart by
/// the COUNTER, which is the whole reason [`MembershipDerivePath`] exists.
pub(super) fn derive_trusted_membership<F>(
    kind: &str,
    pages: impl IntoIterator<Item = BlockAddress>,
    read_page: F,
) -> Option<DerivedMembership>
where
    F: FnMut(&BlockAddress) -> Option<Vec<u8>>,
{
    let derived = derive_membership(kind, pages, read_page);
    if !derived.is_complete() {
        note_membership_derive(MembershipDerivePath::RefusedIncomplete);
        return None;
    }
    if derived.live_only_pages > 0 {
        note_membership_derive(MembershipDerivePath::RefusedLiveOnlyPage);
        return None;
    }
    if derived.pages_read == 0 {
        note_membership_derive(MembershipDerivePath::NoPages);
        return None;
    }
    // ASSERTED RATHER THAN ASSUMED, because this is the one arm that hands back a membership a
    // caller will act on, and the two predicates are maintained separately.
    debug_assert!(derived.is_authoritative());
    note_membership_derive(MembershipDerivePath::Trusted);
    Some(derived)
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
/// # THE WALK THAT OBEYS IT NOW EXISTS, AND WHAT IT HAD TO ADD
///
/// This paragraph used to say the fold could not see a tombstone: `compact_container_pages_batched`
/// takes its elements from the RESIDENT maps (`shard.sets` and the other three) and a removed element
/// is not in them, so a round neither collected a tombstone nor risked dropping one. That is still
/// true of the ELEMENT walk; what changed is that the compaction round now ALSO censuses the
/// container's index entries, which is where the tombstones are, and
/// [`tombstones_collectable`] is the gate it asks. See that function for the two terms this rule does
/// not supply on its own.
///
/// # THE UNIT OF BOTH COUNTS IS A DISTINCT PAGE, AND GETTING THAT WRONG RESURRECTS
///
/// Stated here because the obvious number to hand this function is the wrong one. The compactor's own
/// `pages_folded` counts ELEMENTS whose address it repointed -- it is `destinations.len()`, one per
/// element -- and several elements share one batched page, so it is larger than the page count
/// whenever #2027's folding did anything at all. Pass it as `pages_being_rewritten` and the comparison
/// is an element count against a page count, which can COINCIDE: a container of pages A(x, y, z),
/// B(m live, w live) and T(not m) has three pages, and a round that relocates x, y and z while
/// `should_relocate` declines w reports three -- so the rule says the rewrite was total while B
/// SURVIVES SAYING `m` IS LIVE, and dropping T resurrects `m`. The caller therefore counts DISTINCT
/// SOURCE PAGE POSITIONS, not elements.
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

/// What one container's compaction round did, in the unit [`may_drop_tombstones`] asks for.
///
/// LIVE AND TOMBSTONE PAGES ARE SEPARATE FIELDS rather than one total, because the round rewrites the
/// two for different reasons and only one of them is conditional. Every tombstone page of the
/// container goes if the collection goes -- that is what collecting means -- while a LIVE page goes
/// only if `should_relocate` accepted it and the batch held. Summing them before this type would hide
/// which half fell short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ContainerRoundFacts {
    /// Distinct pages the container's LIVE entries point at, before the round moved anything.
    pub(super) live_pages: usize,
    /// How many of those the round actually read and repointed away from. DISTINCT PAGES, not
    /// elements -- see [`may_drop_tombstones`] for the resurrection the element count causes.
    pub(super) live_pages_rewritten: usize,
    /// Distinct pages the container's TOMBSTONE entries point at.
    pub(super) tombstone_pages: usize,
    /// Batches this round sealed for this container. More than one and the rule declines.
    pub(super) batches: usize,
}

/// Whether this round may drop this container's tombstones -- the FULL gate, not just the rule.
///
/// # THREE TERMS, AND [`may_drop_tombstones`] IS ONLY ONE OF THEM
///
/// The rule answers "was the rewrite total", which is a question about COUNTS. Two things it cannot
/// see have to hold as well, and both of them are about what the pages SAY:
///
///   1. THE DERIVATION MUST BE TRUSTED. `derived` is `None` whenever
///      [`derive_trusted_membership`] refused -- most importantly for a `LiveOnly` page, which cannot
///      state a removal and so makes the fold a SUPERSET. Dropping a tombstone on the strength of a
///      superset is the resurrection this whole stage exists to close: the v1 page still names the
///      element live, nothing else now says otherwise, and the next derivation puts it back. This is
///      the term that makes the collection depend on the refusal rather than merely coexist with it.
///   2. EVERY COMPONENT BEING DROPPED MUST BE ONE THE PAGES AGREE IS GONE. A tombstone entry whose
///      component is NOT in `derived.removed` means some page in the set states that element LIVE
///      later than the tombstone -- a re-add, which `derive_membership`'s third sequence is entirely
///      about. Its tombstone is already spent and dropping it is harmless, but dropping it *because
///      the round was total* while treating the element as removed is not, so the conservative answer
///      is to decline the whole container and let the next round see a simpler state.
///
/// A CONJUNCTION AND NOT A SCORE. None of the three is weighted or defaulted.
///
/// # HOW MUCH OF THE WORK EACH TERM ACTUALLY DOES, STATED HONESTLY
///
/// It would be easy to present all three as equally load-bearing and they are not. IF the denominator
/// is right -- if `facts.live_pages` really is every page the container's live entries point at -- then
/// a total rewrite leaves nothing behind but the round's own fresh v2 batch, holding exactly the
/// resident live elements, and no derivation over it can resurrect anything. On that assumption terms
/// 1 and 2 are DEFENCE IN DEPTH rather than necessity.
///
/// They are kept because that assumption is a property no type enforces. The denominator comes from a
/// census of the INDEX and the rewritten count comes from a walk of the RESIDENT MAPS, and nothing
/// makes those two populations agree -- they are built by different code from different sources. A
/// container holding an element in one and not the other makes the two counts disagree, and the
/// failure is silent in both directions. Term 1 then refuses anything the pages cannot vouch for, and
/// term 2 refuses any component the pages do not actually say is gone. The cost of both is two
/// predicates and a page read per container that has a tombstone; the cost of being wrong is a member
/// coming back.
pub(super) fn tombstones_collectable(
    facts: &ContainerRoundFacts,
    derived: Option<&DerivedMembership>,
    components_being_dropped: &[String],
) -> bool {
    let Some(derived) = derived else {
        return false;
    };
    // BELT AND BRACES, and not redundant: `derive_trusted_membership` is the only producer of a
    // `Some` here today, so this cannot fire -- but it is the invariant the two terms below rest on,
    // and a second producer added later would otherwise slip past both.
    if !derived.is_authoritative() {
        return false;
    }
    if components_being_dropped.is_empty() {
        // Nothing to collect. Declining rather than returning true keeps "this round collected" a
        // statement that something was actually dropped.
        return false;
    }
    if !components_being_dropped
        .iter()
        .all(|component| derived.removed.contains(component))
    {
        return false;
    }
    may_drop_tombstones(
        facts.live_pages + facts.tombstone_pages,
        facts.live_pages_rewritten + facts.tombstone_pages,
        facts.batches,
    )
}
