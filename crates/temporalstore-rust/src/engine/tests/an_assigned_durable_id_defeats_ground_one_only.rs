// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN ASSIGNED DURABLE ID DEFEATS GROUND ONE ONLY, AND GROUND ONE IS THE FIXABLE ONE.
//!
//! [`super::the_entry_cannot_name_its_object_by_slot`] refuses a bucket-relative ordinal on two
//! independent grounds. A later proposal reopens the leg from a different direction -- keep the
//! page index addressed by a `u64`, but make that `u64` an ASSIGNED, PERSISTED id rather than the
//! computed digest `block_index_handle` produces, so that the non-invertibility that kills the
//! digest route does not arise. This module prices that proposal, because it is the best-shaped
//! one the leg has had and the next reader will have it too.
//!
//! IT REACHES TWENTY-FOUR AND THAT PART IS REAL. The map arm already holds `(u64, BlockIndex)`, so
//! swapping a digest key for an assigned key costs no width at all, and the entry lands at
//! twenty-four with zero slack -- measured in [`super::entry_width_at_each_step`]. Every other
//! route to twenty-four that this campaign has priced reached a wider number than it claimed. This
//! one does not.
//!
//! IT STILL DOES NOT CLEAR THE REFUSAL, FOR ONE REASON STATED IN THE REFUSAL ITSELF. The slot
//! module's verdict is at its own line 160: the leg "is refused on Ground 2, and Ground 1 is the
//! reason a partial fix would not rescue it". Ground 1 is that positions do not survive a reload,
//! and the module calls it "a fixable defect" in as many words. Ground 2 is that THE ENTRY IS THE
//! ONLY DURABLE HOME OF THE KEY'S CHARACTERS, and the module's closing sentence on it is that it
//! "is not fixable without a durable ordinal-to-characters map, which is the second index this leg
//! exists to avoid."
//!
//! An assigned id is durable, so it answers Ground 1. It is a `u64`, so it answers nothing about
//! Ground 2. ASSIGNED VERSUS COMPUTED IS ORTHOGONAL TO THE BINDING OBJECTION, and the pins below
//! are what say so rather than the paragraph.
//!
//! # CLAIM 1 -- A DURABLE, RESTART-STABLE OBJECT ID ALREADY EXISTS, AND IT CONSUMES THE CHARACTERS
//!
//! `hashing::stable_block_object_id` at `hashing.rs:175` is FNV-1a over the shard, the kind and
//! the KEY BYTES. So the tree does not lack a stable id; it lacks a way back from one. The
//! signature pin below is the checkable form of that: the id is produced BY consuming the
//! characters, which is precisely why holding the id does not hand them back.
//!
//! # CLAIM 2 -- THE ID HAS TO BE THE MAP KEY, NOT A FIELD, OR THE WIDTH DOES NOT LAND
//!
//! Carried as a field in place of the shared pointer, the field sum is thirty-two and the entry
//! measures thirty-two. The mirror below measures it. So the proposal only reaches twenty-four in
//! the form where the id is the key of the holding map and the entry carries no identity term at
//! all -- which is the form Ground 2 is about.
//!
//! # CLAIM 3 -- "DURABLE BECAUSE THE LOG ITEM CARRIES IT" IS FALSE IN THIS TREE
//!
//! This is the trap most likely to be walked into again, because it is true of the protocol the
//! proposal was drawn from and false here. An index-log row DOES carry `object_key` as text, and
//! the slot module already records that it is written FROM the entry rather than independently of
//! it. The second half is the one that bites a persisted id:
//! `Engine::dump_and_reclaim_index_logs_with_min_reclaimable` at `persistence.rs:744` calls
//! `gc_reflected_before_anchor` at `persistence.rs:755`, whose own comment is "drop every record
//! the durable base already reflects", and the line below it reads "Pages at or below the anchor
//! are in the durable base now".
//!
//! SO THE LOG IS GIVEN BACK AT A DUMP. Anything whose only durable home is a log item has the
//! lifetime of the window between dumps. A persisted id would have to live in the BASE index --
//! and then so would the key-to-id map, which is the second index again.
//!
//! # CLAIM 4 -- THE READER SPLIT, WHICH IS WHERE THE REAL SCOPE IS
//!
//! The slot module's census stands and is the number that matters: fifty-nine production sites,
//! split by WHAT EACH SITE WANTS rather than by what it touches.
//!
//!   * TWENTY-EIGHT are served by an id. Fourteen compare the entry against a caller's string,
//!     six compare it against another in-tree row, two test a caller's set, one is the handle
//!     itself, and five reach the object lookup, which could be re-keyed.
//!   * TWENTY-TWO WANT THE CHARACTERS AND HAVE NO OTHER SOURCE. An assigned id changes none of
//!     them, which is the whole of this module.
//!   * ONE CANNOT BE SERVED AT ALL by any `u64`, and it is named in claim 5.
//!
//! # CLAIM 5 -- THE ONE SITE NO ID REACHES, AND WHY ITS OBVIOUS FIX IS A TAUTOLOGY
//!
//! `storage_bucket_internals.rs:2298` refuses a release when
//! `block_routing_bucket(&block.object_key, start, end) != routing_bucket`. The signature pin
//! below is the checkable claim: that function takes `&str`, so no id of any provenance can be fed
//! to it.
//!
//! AND THE ENTRY'S OWN `routing_bucket` FIELD IS NOT A SUBSTITUTE. The site's comment says it
//! "compares two INDEPENDENT things rather than a block's copy of its bucket against the bucket
//! holding it" -- where the block IS against where its KEY routes. Substituting the field makes
//! both sides the same answer, so the check could no longer fail. That is the shape this campaign
//! has shipped before: a detector replaced by a comparison of a value against itself, two lines
//! from a comment explaining why it must not be.
//!
//! WHICH OF THE TWO ROUTING NUMBERS IS MEANT, because this file warns three times that they
//! differ. Both this site and the reload it defends use `(key, start, end)` over the shard's
//! stamped range -- `shard.routing_range()` is read into the loop at `storage_bucket_internals.rs`
//! just above it, and the released-bucket supplement derives with the same three arguments. The
//! `(key, 0, u32::MAX)` form the warnings are about belongs to the removal path and is not either
//! side of this comparison.
//!
//! # CLAIM 6 -- THE TOTALS, WITH BOTH DENOMINATORS NAMED WHERE THE NUMBERS ARE WRITTEN
//!
//! A width step is not a footprint step, and this proposal is the case where the two have opposite
//! signs. The arithmetic is pinned below rather than stated. At the measured one page per object
//! THE ENTRY AND THE OBJECT ARE ONE TO ONE, so moving the key out of the entry to share it between
//! pages has nothing to share it between -- the saving a re-keying exists to capture is zero by
//! construction at that population, and a container to hold the key costs more than the sixteen
//! bytes the entry frees.
//!
//! PER ENTRY, NODE AGAINST NODE: forty today, twenty-four under this proposal. A real sixteen
//! bytes, and it is the number a node-to-node comparison should use.
//!
//! PER OBJECT, TOTAL AGAINST TOTAL, at one page per object: forty-eight today for the entry and
//! its map key; thirty-two under this proposal, plus at least twenty-four for a persisted
//! key-to-id map holding one shared pointer and one id per object. Fifty-six against forty-eight.
//! EIGHT BYTES WORSE, AND THAT IS A FLOOR -- it counts payload only, with no container overhead
//! for the new map and nothing at all for the id-to-characters direction the twenty-two readers of
//! claim 4 need.
//!
//! THESE TWO RATIOS HAVE DIFFERENT DENOMINATORS AND MUST NOT BE MIXED. The first is per entry
//! against a node layout; the second is per object against this tree's total. A design whose index
//! never holds the key at all pays its identity cost somewhere else entirely, so its total is not
//! comparable to our total by narrowing our node.
//!
//! # WHAT IS AVAILABLE AND UNPROVEN, recorded so it is not mistaken for part of this refusal
//!
//! `DirtyObjectIndex` maintains a BUCKET-keyed side (`bucket_counts`, `bucket_ids`,
//! `drain_buckets`) beside its key-keyed one. The dirty rollup in
//! `refresh_one_bucket_runtime_flags` -- one of the two readers in that function, and one of the
//! hardest cases because it holds only a bucket -- could therefore be answered from the bucket
//! number alone, needing neither the characters nor an id, and more cheaply than the per-page
//! probe it runs today.
//!
//! IT IS NOT ASSERTED HERE AND MUST NOT RIDE ON THIS MODULE. It widens the predicate from "any
//! page's object is dirty" to "any dirty object routes here", and the differing-numbers warning
//! above applies again: the bucket a dirty object is FILED under is not guaranteed to be the
//! bucket HOLDING its pages. It needs its own fail-first test with a control that can refuse.
//! The other reader in that function, the TTL minimum over `expires_at_ms`, has no bucket-keyed
//! counterpart at all -- that map is `BTreeMap<String, u64>` at `state.rs:129`, keyed by the
//! characters.

use std::mem::{align_of, size_of};
use std::sync::Arc;

use crate::block_store::ElementEntry;
use crate::engine::state::BlockIndex;
use crate::engine::storage_bucket_internals::StoredModelKind;

/// CLAIM 1, AS A TYPE. The stable object id is produced by CONSUMING the key bytes.
///
/// A signature pin rather than prose: if this function ever grows a form that takes an id instead
/// of a key, this line stops compiling and the claim above is reopened rather than silently
/// stale. It is the checkable half of "we already have a durable id and it is still not a way back
/// to the characters".
const _: fn(crate::types::ShardId, &str, &str) -> u64 =
    crate::engine::hashing::stable_block_object_id;

/// CLAIM 5, AS A TYPE. The routing bucket is a function of the key BYTES and nothing else.
///
/// This is the pin that bounds the whole route: no `u64` can be fed to it, so the release guard at
/// `storage_bucket_internals.rs:2298` cannot be served by any id, assigned or computed. If a form
/// taking an id is ever added, this line fails and the one unservable site becomes servable.
const _: fn(&str, u32, u32) -> u32 = crate::engine::hashing::block_routing_bucket;

/// CLAIM 2, AS A MEASUREMENT. The id carried as a FIELD lands the entry at thirty-two.
///
/// # EVERY MIRROR IN THIS FILE DECLARED TWO FIELDS THE LIVE ENTRY LOST TWO LEGS AGO
///
/// All four carried `kind: IndexItemKind` and `routing_bucket: u32` -- five bytes between them --
/// and `65c7fc562` removed both from `BlockIndex`. NOT ONE ASSERTION MOVED WHEN THEY WERE
/// CORRECTED, which is exactly why this is written up rather than quietly fixed: the comparison
/// mirror held forty bytes of field against a live entry holding thirty-five, and FORTY AND
/// THIRTY-FIVE BOTH ROUND TO FORTY, so `size_of::<mirror>() == size_of::<BlockIndex>()` went on
/// passing while the mirror described a field set the entry did not have.
///
/// THIS IS THE THIRD RECORDED INSTANCE OF THAT SHAPE AND THE SECOND STILL LIVE.
/// `entry_width_at_each_step`'s module doc records it for its own step-zero mirror and answers it
/// with field OFFSET pins, saying in as many words that its width controls "discriminate at WORD
/// granularity and no finer". `state.rs`'s address-word probe records it for itself. This file
/// read both notes and carried the defect anyway, because nothing here reads an offset.
///
/// SO THE ARITHMETIC IN THIS PARAGRAPH WAS WRONG TOO, and it is the half a width cannot check. It
/// read "one plus four plus eight plus one plus sixteen plus one plus one is thirty-two bytes of
/// field, and thirty-two is a multiple of the alignment, so it lands there exactly". With the two
/// retired terms gone it is eight plus one plus sixteen plus one plus one: TWENTY-SEVEN bytes of
/// field in thirty-two, with five of slack. It lands at thirty-two by ROUNDING, not exactly --
/// which is the same correction `state.rs`'s probe took for the same reason.
///
/// THE VERDICT IS UNCHANGED AND THAT IS NOT LUCK: thirty-two is still not twenty-four, so the
/// proposal still reaches twenty-four only in the form where the id is the KEY OF THE HOLDING MAP
/// and the entry carries no identity term. That form is the one Ground 2 is about, which is why
/// this mirror is the bridge between the width claim and the refusal.
#[allow(dead_code)]
struct TheIdCarriedAsAFieldInstead {
    object_id: u64,
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
}

const _: () = assert!(size_of::<TheIdCarriedAsAFieldInstead>() == 32);
const _: () = assert!(align_of::<TheIdCarriedAsAFieldInstead>() == 8);

/// THE CONTROL FOR THAT MIRROR, and it is the control that was passing over a stale field set.
///
/// The mirror is the live entry with one term swapped, so it is only evidence if the UNSWAPPED
/// mirror measures the live entry. This is the one that held forty bytes of field against the live
/// entry's thirty-five and compared equal anyway, for the reason the claim-2 doc gives: it is
/// asserted at WORD granularity, and the five bytes the two retired fields contributed were
/// absorbed by the rounding.
///
/// IT IS STILL ASSERTED AT WORD GRANULARITY, and this file does not pretend otherwise. The
/// stronger instrument is a field OFFSET pin, and `entry_width_at_each_step` carries it for the
/// same structure; what this file owes is a mirror whose field list is the live one, which is now
/// the case.
#[allow(dead_code)]
struct TheLiveEntryForComparison {
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
}

const _: () = assert!(size_of::<TheLiveEntryForComparison>() == size_of::<BlockIndex>());
const _: () = assert!(align_of::<TheLiveEntryForComparison>() == align_of::<BlockIndex>());

/// THE NEGATIVE CONTROL: a mirror with one word too many must not measure the live entry, or the
/// comparison above is not discriminating at all.
#[allow(dead_code)]
struct TheLiveEntryWithOneWordTooMany {
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
    one_word_too_many: u64,
}

const _: () = assert!(size_of::<TheLiveEntryWithOneWordTooMany>() != size_of::<BlockIndex>());

/// CLAIM 6, AS ARITHMETIC. Each term of the totals, pinned where the total is written.
///
/// The entry with no identity term, for the per-object total below. Twenty-four -- and NINETEEN
/// bytes of field in it, with FIVE of slack, not the zero this doc used to claim.
///
/// BOTH HALVES OF THAT SENTENCE WERE WRONG AND THE WIDTH WAS RIGHT. It read "Twenty-four, with the
/// same zero slack the width module measures", and the width module measures five: its step-one
/// row is "NINETEEN bytes of field in twenty-four, with FIVE bytes of slack", under a heading
/// saying that reading inverts the rule this entry is governed by. This mirror carried the two
/// retired fields on top of that, so it held twenty-four bytes of field in twenty-four and the
/// zero claim was true OF THE MIRROR and of nothing else.
#[allow(dead_code)]
struct TheEntryWithNoIdentityTerm {
    model_id: StoredModelKind,
    address: ElementEntry,
    dirty: bool,
    deleted: bool,
}

const _: () = assert!(size_of::<TheEntryWithNoIdentityTerm>() == 24);

/// AND THIS MIRROR IS NOW THE LIVE ENTRY, WHICH IS ASSERTED RATHER THAN LEFT TO BE NOTICED.
///
/// It was built to measure "the entry with no identity term" against an entry that had one. The
/// entry has none: the identity lives once per object in `CoreIndex::object_block_lookup` and is
/// resolved from the position the page is filed at. So the proposal's ENTRY SIDE and the shipped
/// entry are one field set, and every arithmetic below that subtracts one from the other is zero.
///
/// THE MIRROR IS KEPT RATHER THAN FOLDED, AND ONLY BECAUSE THIS EQUALITY IS WRITTEN. Two
/// declarations of one shape are two things to keep in step -- `entry_width_at_each_step` folded
/// its step-one mirror away for exactly that reason, and `state.rs` has folded two probes. What
/// keeps this one honest is that its collapse into the live type is a CHECKED claim: if either
/// shape moves, this line fails rather than the two quietly drifting apart.
const _: () = assert!(size_of::<TheEntryWithNoIdentityTerm>() == size_of::<BlockIndex>());

/// PER ENTRY, NODE AGAINST NODE -- AND THE TWO COLUMNS HAVE MET.
///
/// The map arm holds the key beside the entry either way, so this is what a page costs resident.
/// It was 48 with the identity on the entry and the proposal's side was 32; the identity left and
/// the live element IS 32. The second assertion of this pair is the equality above.
const _: () = assert!(size_of::<(u64, BlockIndex)>() == 32);
const _: () = assert!(size_of::<(u64, TheEntryWithNoIdentityTerm)>() == 32);

/// PER OBJECT, TOTAL AGAINST TOTAL. What a persisted key-to-id map holds for one object, payload
/// only: one shared pointer to the characters and one id.
const _: () = assert!(size_of::<Arc<str>>() + size_of::<u64>() == 24);

/// AND THE TOTAL ITSELF, RECONSTRUCTED FROM THE TERMS RATHER THAN RESTATED AS A LITERAL.
///
/// Written this way so that a term whose width moves fails HERE instead of leaving a stale literal
/// standing -- and this is the leg where one did move. The `> ` still holds and the difference does
/// not: it was EIGHT and it is TWENTY-FOUR, because the entry side of the comparison got smaller
/// and the map side did not.
///
/// THE PROPOSAL'S ENTRY SIDE IS NOW LEVEL WITH THE SHIPPED ONE, asserted twenty lines above, so
/// its whole remaining cost is the per-object key-to-id map: one shared pointer to the characters
/// and one id. That is the restatement -- the verdict is unchanged and the ARITHMETIC behind it is
/// now entirely the map, where before it was the map minus an entry-side saving.
///
/// SO THE REFUSAL GOT STRONGER RATHER THAN WEAKER, which is worth saying because a number moving
/// usually means the opposite. A proposal that cost eight bytes an object against a forty-byte
/// entry costs twenty-four against a twenty-four-byte one: the saving it was partly paying for has
/// already been taken by this change, and it was taken WITHOUT the map.
const _: () = assert!(
    size_of::<(u64, TheEntryWithNoIdentityTerm)>() + size_of::<Arc<str>>() + size_of::<u64>()
        > size_of::<(u64, BlockIndex)>()
);
const _: () = assert!(
    size_of::<(u64, TheEntryWithNoIdentityTerm)>() + size_of::<Arc<str>>() + size_of::<u64>()
        - size_of::<(u64, BlockIndex)>()
        == 24
);
/// AND THE DIFFERENCE IS THE MAP, EXACTLY, which is the form that cannot go stale: it says the
/// entry sides cancel and names what is left.
const _: () = assert!(
    size_of::<(u64, TheEntryWithNoIdentityTerm)>() + size_of::<Arc<str>>() + size_of::<u64>()
        - size_of::<(u64, BlockIndex)>()
        == size_of::<Arc<str>>() + size_of::<u64>()
);

#[test]
fn an_assigned_durable_id_is_priced_and_the_numbers_are_reconstructed_from_the_terms() {
    // THE IDENTITY OF THE SUBJECT BEFORE ANY CONTENT CHECK.
    assert_eq!(
        size_of::<TheLiveEntryForComparison>(),
        size_of::<BlockIndex>(),
        "the comparison mirror must measure the live entry"
    );

    let today_per_object = size_of::<(u64, BlockIndex)>();
    let proposal_entry = size_of::<(u64, TheEntryWithNoIdentityTerm)>();
    let proposal_map = size_of::<Arc<str>>() + size_of::<u64>();
    let proposal_per_object = proposal_entry + proposal_map;

    println!("per entry, node against node      today {today_per_object:>3} B   proposal {proposal_entry:>3} B");
    println!("  the entry alone                 today {:>3} B   proposal {:>3} B", size_of::<BlockIndex>(), size_of::<TheEntryWithNoIdentityTerm>());
    println!("  the id carried as a FIELD instead     {:>3} B   (so 24 needs it as the map key)", size_of::<TheIdCarriedAsAFieldInstead>());
    println!("per object, TOTAL against TOTAL   today {today_per_object:>3} B   proposal {proposal_per_object:>3} B   (map {proposal_map} B, payload only)");
    println!("  the proposal is worse by              {:>3} B   and that is a FLOOR", proposal_per_object - today_per_object);

    assert_eq!(size_of::<TheIdCarriedAsAFieldInstead>(), 32);
    assert_eq!(size_of::<TheEntryWithNoIdentityTerm>(), 24);
    assert!(
        proposal_per_object > today_per_object,
        "the per-object total is the denominator this proposal loses in"
    );
    // TWENTY-FOUR, NOT EIGHT, AND IT IS THE BASELINE THAT MOVED. The proposal's entry side is
    // now identical to the shipped entry -- the identity has already left -- so what it adds is
    // the per-object key-to-id map and only the map. The const block under claim 6 carries the
    // reasoning and asserts the same difference as `Arc<str>` plus a `u64`.
    assert_eq!(proposal_per_object - today_per_object, 24);
}

// =================================================================================================
// CLAIM 7 -- THE +24 TERM IN CLAIM 6 DOES NOT APPLY, BECAUSE THE CHARACTERS ARE ALREADY OWNED
// ELSEWHERE AND THE PLACE THAT OWNS THEM IS ALREADY WALKED BY PRODUCTION CODE.
//
// CLAIM 6 IS CORRECTED RATHER THAN DELETED, because the arithmetic in it is right and its PREMISE
// was wrong, and the premise is the part a later reader would otherwise re-adopt. It priced a
// persisted key-to-id map as NEW structure: twenty-four bytes an object that the tree would have
// to start paying. It does not have to. The model maps own the characters today, unconditionally,
// because that is how a record is addressed:
//
//   * `ShardState::strings` is `HashMap<ModelKey, ElementEntry>` at `state.rs:150`, and
//     `ModelKey` is `Box<str>` at `state.rs:73` -- the characters, OWNED, one per object.
//   * `RecordedMap` holds `HashMap<String, K::Elements>` at `recorded_map.rs:152`, which is the
//     same thing for the four recorded kinds.
//
// So there is no container to build and no bytes to add. The identity would be READ from structure
// that exists, not moved into structure that does not.
//
// AND IT IS ALREADY REACHED THIS WAY BY PRODUCTION CODE, which is what makes this a cost question
// rather than a design question. `visit_model_live_blocks` offers every live model-map block in the
// shard to a callback as `(kind, object_key, component, address)`, and its own doc says of the key
// that "Every other arm's key is borrowed from the map and costs nothing" -- one arm composes a
// key with `format!` and is the stated exception. `collect_model_live_block_entries` is that walk
// with an always-true filter, and it takes NOTHING BUT A SHARD. The signature pin below is the
// checkable form of that.
//
// SO THE CORRECTED TOTAL, with both denominators named again. Per entry, node against node: forty
// to twenty-four. Per object, TOTAL against total at the measured one page per object: forty-eight
// to thirty-two -- the entry sheds a sixteen-byte shared pointer per page and NOTHING is added, so
// it is a SIXTEEN BYTE SAVING on both denominators rather than eight bytes of regression.
//
// WHAT THE CHARACTERS COST IS UNCHANGED BY ANY OF THIS, and saying so is what keeps the number
// honest. They are stored twice today -- once as the model map's own key and once as the object
// lookup's inner key -- and they would still be stored twice afterwards, because the lookup needs
// its key to BE a key. The entry's field is a POINTER at the lookup's allocation, so what the
// removal sheds is sixteen bytes of pointer per page and not one byte of text.
//
// AND THE SAVING IS CONDITIONAL ON THE READER CENSUS, WHICH IS WHERE THIS NOW LIVES OR DIES. Two
// functions cannot take a model-map source at all, and one of them is on the write path:
//
//   * `block_index_written_key` through `serialize_page_index`, reached as
//     `PageIndexAt<'a>(&'a BucketNode)` inside a DERIVED `Serialize for CoreIndex`. Serde hands a
//     `Serialize` impl no context, so neither the shard nor the model maps can reach that scope.
//     Structural, not a cost.
//   * `refresh_one_bucket_runtime_flags`, which holds only a `&mut BucketNode` and contains FIVE
//     of the character-wanting sites -- four dirty probes and the TTL minimum. It is reached per
//     write through `refresh_pending_bucket_runtime_flags` from `engine.rs:1062` and
//     `stream_batch_methods.rs:567`, and that function exists precisely to refresh only the
//     touched buckets instead of sweeping. A whole-shard model walk per bucket per write is the
//     quadratic outcome, so this is not servable this way AT ALL rather than servable expensively.
// =================================================================================================

/// CLAIM 7, AS A TYPE. `ModelKey` IS `Box<str>` -- the characters, owned, one per object.
///
/// A type-identity pin rather than a width pin: it compiles only while the two are the same type,
/// so a `ModelKey` that stopped being the characters would fail here instead of leaving the claim
/// standing. A width pin alone would not do it, since several unrelated things measure sixteen.
const _: fn(crate::engine::state::ModelKey) -> Box<str> = |key| key;

/// CLAIM 7, AS A TYPE. A model-map-sourced identity collector exists and takes NOTHING but a shard.
///
/// This is what says the identity can be read from existing structure rather than new structure.
/// If it ever needs the bucket index to answer, this line fails and claim 7 is reopened.
const _: fn(&crate::engine::state::ShardState) -> Vec<crate::engine::storage_bucket_internals::LiveBlockEntry> =
    crate::engine::storage_bucket_internals::collect_model_live_block_entries;

/// CLAIM 7, AS ARITHMETIC -- AND IT HAS LANDED, SO IT IS WIDTHS NOW AND NOT A DIFFERENCE.
///
/// It read `size_of::<(u64, BlockIndex)>() - size_of::<(u64, TheEntryWithNoIdentityTerm)>() == 16`:
/// the per-page saving from reading the identity out of existing structure instead of holding it
/// on the entry. That is what this change did. Both sides are now the same type, so the difference
/// is ZERO and an assertion of sixteen over it is false -- while an assertion of zero would be a
/// comparison of a value with itself wearing the name of a saving.
///
/// SO THE CLAIM IS STATED AS THE TWO WIDTHS IT IS ABOUT. The element was 48 and is 32; the sixteen
/// is the shared pointer that left, which is pinned as a term rather than as a subtraction. A
/// reader who wants the saving reads it off the pair.
const _: () = assert!(size_of::<(u64, BlockIndex)>() == 32);
const _: () = assert!(size_of::<Arc<str>>() == 16);

// =================================================================================================
// CLAIM 8 -- THE BUCKET TTL GATES NOTHING OPERATIONAL, SO THE TTL HALF OF THE PER-WRITE REFRESH IS
// NOT ON THE CORRECTNESS PATH.
//
// WHAT IS PINNED AND WHAT IS CITED, said first so neither is mistaken for the other. The accessor
// signature and the width below are PINNED and fail if they move. Everything else in this claim is
// CITED: the consumer list and the expiry path are reachable only through module-private items and
// public report structs, so a test cannot assert membership of either. A citation is not a proof
// and is recorded as a citation.
//
// THE QUESTION THIS ANSWERS. `refresh_one_bucket_runtime_flags` holds only a `&mut BucketNode` and
// contains five of the character-wanting sites -- four dirty probes and one TTL minimum -- on the
// per-write path. For the TTL minimum the first question is not "where does the input come from"
// but WHAT TOLERANCE THE CONSUMER HAS, because an exact minimum needs an ordered structure per
// object while a conservative one needs almost nothing.
//
// THE ANSWER IS NEITHER: IT IS A REPORTED STATISTIC. Every reader of `BucketNode::ttl_ms` is a
// report or the serializer, CITED:
//
//   * `object_manager.rs:83` sets a report field; `:89` and `:154` fold it into one.
//   * `storage_reporting.rs:347` sets a report flag bit; `:368` packs it into a report blob;
//     `:473` copies it into a report struct.
//   * `bucket_store.rs:101` counts it into `ttl_bucket_count`; `:134` sets a report field.
//   * `state.rs:3951` serializes it, and `ttl_ms` is in `BUCKET_NODE_FIELDS` at `state.rs:4008`.
//   * It is CONSTRUCTED in only one place: `storage_bucket_internals.rs:5375` and `:5378`, which
//     is the function above.
//
// AND REAL EXPIRY DOES NOT READ IT. Expiry runs off `expires_at_ms` and its deadline-ordered
// mirror `expiry_by_deadline` -- `engine.rs:4831` to `:4851` for the mutators, and
// `recovery_sweep_compact.rs:996` for the due-key prefix scan. No expiry, eviction or serving
// decision consults a bucket's TTL. CITED.
//
// NO CONSUMER DOCUMENTS IT AS LIVE. `bucket_store.rs:38` declares `pub ttl_ms: Option<u64>` on the
// public report with no claim about freshness, and the object report's field at
// `object_manager.rs:24` likewise. So there is no doc to correct and no promise to honour. CITED.
//
// WHAT THAT BUYS, AND IT IS THE CHEAPEST OF THREE. The per-write refresh can simply DROP the TTL
// term and leave it to the full sweep, which already runs on load at `persistence.rs:375` and
// `:536`. That REMOVES a per-write loop over a bucket's pages rather than re-sourcing it: no new
// structure, no bytes, and no stamp. The value then goes stale between full sweeps, which is a
// reporting-accuracy decision rather than a correctness one -- and whoever implements it owes the
// code a statement of HOW STALE it can get and WHAT refreshes it, because that is the claim a
// later reader will need and will not otherwise find.
//
// THE TWO ALTERNATIVES, PRICED SO THE CHOICE IS NOT REOPENED BLIND. Making it derived-on-read
// would take it off the wire and buy a format stamp for a statistic. A third bucket-keyed expiry
// mirror, maintained at the `set_expiry` chokepoint -- which has the whole shard in scope at
// `engine.rs:4846`, so it can commit to `block_routing_bucket(key, start, end)`, the same number
// that decides where a page lives -- is sound with a CONSERVATIVE minimum precisely because the
// consumer is a report, so it never has to search for the next minimum on a removal. It would cost
// about twelve bytes a bucket against a removal saving sixteen bytes a page at the in-tree p50 of
// thirty-nine pages a bucket (`state.rs:1414`), so roughly two per cent of the saving. Cheap, and
// unnecessary if the term is simply dropped.
// =================================================================================================

/// CLAIM 8, PINNED. The accessor every report consumer reads the bucket TTL through.
///
/// A signature pin on `ms`, which is what makes the claim "every consumer takes an `Option<u64>`
/// out of this and nothing more" checkable at the one place they all pass through. If the accessor
/// ever starts handing back something a consumer could act on differently, this line fails.
const _: fn(crate::engine::state::BucketTtl) -> Option<u64> = crate::engine::state::BucketTtl::ms;

/// CLAIM 8, PINNED. The width, because the cheapest option turns on costing nothing.
///
/// `Option<NonZeroU64>` rides the niche, so the term is one word and dropping its per-write
/// maintenance moves no bytes at all -- which is the half of the argument that a reader would
/// otherwise have to take on trust.
const _: () = assert!(size_of::<crate::engine::state::BucketTtl>() == 8);
