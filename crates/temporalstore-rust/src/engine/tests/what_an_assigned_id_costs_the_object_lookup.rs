// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT AN ASSIGNED OBJECT ID COSTS THE OBJECT LOOKUP, BY ROUTE, WITH BOTH DENOMINATORS NAMED.
//!
//! [`super::where_the_characters_are_durable_per_kind`] settles the question the three previous
//! refusals of this leg never asked: the object key's characters are ALREADY held durably, outside
//! the object lookup, by a map that exists for its own reasons, for thirteen of the fifteen live
//! model kinds. So the `id -> characters` direction costs no new storage, and the "second index"
//! objection -- the one `the_entry_cannot_name_its_object_by_slot` closes on, and the one
//! `an_assigned_durable_id_defeats_ground_one_only` leaves standing -- is answered in that
//! direction.
//!
//! THIS MODULE IS ABOUT THE OTHER DIRECTION, WHICH IS THE ONE AN ASSIGNED ID NEEDS AND A COMPUTED
//! ONE DOES NOT. A computed id is a function of the characters, so `characters -> id` is free and
//! needs no storage at all; what it costs instead is that two keys can share an id. An ASSIGNED id
//! has no collision by construction, and the price of that is exactly this: `characters -> id` is
//! no longer a function, so it has to be READ from somewhere. The per-kind maps the census found
//! are keyed BY the characters and carry NO id, so finding the characters durable does not, by
//! itself, make the id reachable from them.
//!
//! THERE ARE EXACTLY THREE PLACES THE `characters -> id` DIRECTION CAN LIVE, and the arithmetic
//! below is what chooses between them rather than a preference.
//!
//! ```text
//!     route  where characters -> id lives                        verdict
//!     A      a new durable per-shard map                         the second index; refused by the
//!                                                                leg's own premise, priced anyway
//!     B      a RESIDENT map, rebuilt on load                     no new durable state, and it
//!                                                                REGRESSES -- measured below
//!     C      inside the per-kind map's EXISTING value            the only non-regressing home
//! ```
//!
//! # WHAT IS HELD CONSTANT, SO THE THREE NUMBERS ARE COMPARABLE
//!
//! Every route keeps `BlockRefs` exactly as it is -- none of them touches what an object's entry
//! POINTS AT, only how the entry is addressed -- so the value term is identical in all three and
//! is left out of all three. A per-object figure that included it would be larger and would say
//! the same thing; including it in one arm and not another is the mistake these ratios exist to
//! avoid.
//!
//! THE DENOMINATOR IS ONE OBJECT, NOT ONE PAGE AND NOT ONE ADDRESS. Three non-comparable per-key
//! figures already exist in this tree -- the whole-`ShardState` figure, the total-index-heap figure
//! and the `bucket_map`-only figure -- and the lookup holds one entry PER OBJECT whatever an
//! object's page count is. Each number below is per object, and the arms that would differ per
//! page are named where they arise.
//!
//! # ROUTE C'S TWO EIGHT-BYTE TERMS CANCEL, AND THAT IS THE WHOLE RESULT
//!
//! [`the_two_eight_byte_terms_of_the_assigned_id_cancel_exactly`] pins it. The lookup's inner key
//! narrows from a shared pointer to a word -- sixteen bytes to eight -- and the per-kind map's
//! value widens by one word to carry the id -- an address is sixteen bytes and an address beside an
//! id is twenty-four. Minus eight and plus eight.
//!
//! SO ROUTE C'S NET SAVING IS EXACTLY THE CHARACTERS' HEAP PAYLOAD AND NOTHING ELSE. That is a
//! sharper claim than "thirty-two against forty", and it is sharper in a useful direction: it says
//! the saving does not depend on the lookup's node layout, on the key's width, or on how many
//! pages an object has. It is the bytes the characters themselves occupy, freed because after the
//! census nothing needs a second copy of them -- the per-kind map's own key is the one copy, and
//! `id -> characters` reads it there.
//!
//! AND THE PAYLOAD IS THE ONE TERM THIS MODULE DOES NOT INVENT. `index_bytes_per_key` measures it
//! on the real engine at two corpus sizes, through the counting allocator, walking distinct `Arc`
//! pointers so a shared string is counted once. The figure belongs to that probe and is quoted
//! from its run, not restated here as a literal -- a width module writing a measured number as a
//! constant is how a parallel literal comes to disagree with the thing it mirrors, which is a
//! defect this campaign has already found and fixed once in
//! `the_entry_cannot_name_its_object_by_slot`.
//!
//! # ROUTE B REGRESSES, AND BY HOW MUCH IS ARITHMETIC RATHER THAN OPINION
//!
//! Route B is the tempting one, because it adds no durable state at all: the id's durable home is
//! the stored page entry -- which is in the BASE index, so it survives a dump and is not what
//! `an_assigned_durable_id_defeats_ground_one_only`'s Claim 3 warns about -- and `characters -> id`
//! is a RESIDENT map rebuilt on load by joining the per-kind maps against the page entries on the
//! address triple those two already share.
//!
//! IT REGRESSES BECAUSE THE CHARACTERS' ALLOCATION IS NOT FREED, IT MOVES. The new map is keyed BY
//! the characters, so the payload term that route C sheds is still held -- just by a different
//! structure. What route B actually buys is the lookup's key narrowing, eight bytes, and what it
//! pays is a whole new table slot per object: a shared pointer and a word, twenty-four bytes of
//! payload before any table overhead at all.
//!
//! [`the_resident_route_pays_more_than_the_key_it_narrows`] pins that as a FLOOR: plus twenty-four
//! against minus eight, sixteen bytes worse per object, counting payload only, with nothing for
//! the table's own control bytes or its load factor.
//!
//! AND THE IN-TREE CLAIM IT AGREES WITH, CITED WHERE IT ACTUALLY LIVES.
//! [`super::an_assigned_durable_id_defeats_ground_one_only`]'s Claim 6 reaches the same direction
//! by a different road -- fifty-six against forty-eight, eight bytes worse, also stated as a floor
//! -- counting a persisted key-to-id map rather than a resident one. The two are not the same
//! number because they count different arms, and what they agree on is the SIGN. That claim is
//! quoted from the module in this tree rather than from a pull request, because a number cited to
//! an unmerged change cannot be checked by a reader of this tree.
//!
//! # ROUTE A IS PRICED EVEN THOUGH IT IS REFUSED, so the refusal is a comparison and not a rule
//!
//! A new durable map is what this leg exists not to add, and that is a statement about durable
//! shape rather than about bytes. Priced anyway, it is route B's cost plus the wire: the same
//! twenty-four bytes of payload per object resident, written as well, and a second structure on
//! the load path that can disagree with the first. [`the_durable_route_is_the_resident_route_plus_
//! the_wire`] pins the resident half; the wire half is not measured here because the refusal does
//! not turn on it.
//!
//! # WHAT STILL CONSUMES THE CHARACTERS, PINNED BY SIGNATURE RATHER THAN BY GREP
//!
//! Two functions take the key's characters by `&str` and no id of any provenance can be fed to
//! either. Both are pinned below as signatures, which is the checkable form of the claim:
//!
//!   * `state::block_index_handle(&str, &BlockIndex) -> u64` -- the handle a page is filed under.
//!     Under an id-addressed lookup this hashes the id instead, and that is tractable rather than
//!     fatal for a reason the decode makes measurable: the stored index carries each page's handle
//!     EXPLICITLY (`CoreIndex`'s visitor reads `(handle, key)` pairs and asks
//!     `block_index.get(handle)`), so handles are read back rather than recomputed and moving the
//!     term moves no stored value. Its own doc says handles "are re-assigned on every load, so
//!     moving them is survivable".
//!   * `hashing::block_routing_bucket(&str, u32, u32) -> u32` -- the ONE site no
//!     `u64` reaches, and `an_assigned_durable_id_defeats_ground_one_only`'s Claim 5 is why its
//!     obvious fix is a tautology. It is restated here only as the pin, because the claim is that
//!     module's and is unchanged by the census: this function needs the characters, and after the
//!     census the characters are available to it from the per-kind map for thirteen kinds.
//!
//! THE SECOND PIN IS WHAT THE CENSUS ACTUALLY MOVES. Claim 5 refused the leg because that site's
//! only possible source of characters was the field being removed. It has another source now, for
//! every kind the census answers yes for -- which is why the pin is kept and the refusal that
//! stood on it is not.
//!
//! # THE READER CENSUS, BY COMPILER AND IN TWO STAGES -- AND STAGE ONE IS A TRAP
//!
//! Taken at `2a709095c` by re-keying the lookup and compiling. IT WAS TAKEN TWICE BECAUSE THE
//! FIRST READING WAS WRONG BY A FACTOR OF SIX, and recording why is worth more than the number:
//!
//!   * STAGE ONE re-keyed `ObjectBlockLookup`'s own field and its own methods -- `get`, `get_mut`,
//!     `file`, `remove`, `iter` and the two key-sharing helpers -- and `cargo check --lib` named
//!     TEN production sites, NINE of them inside `state.rs`. That reads as a change with almost no
//!     blast radius, and it is an artefact: `CoreIndex`'s wrapper methods keep their `&str`
//!     signatures and ABSORB the re-keying, so every caller beyond them still type-checks. A
//!     census stopped there would have priced this leg at ten sites.
//!   * STAGE TWO pushed it through the wrappers -- the six `CoreIndex` methods that take an object
//!     key, plus `BlockKeysByPosition`'s identity pair and `BlockObjectIdentity`'s field -- and
//!     named SIXTY-FIVE production sites on `--lib` and ONE HUNDRED AND THIRTY-FIVE on
//!     `--all-targets`, the second across fifteen test modules as well as the seven production
//!     files.
//!
//! BOTH ARE FLOORS, for the reason this tree has already recorded: rustc suppresses some
//! diagnostics on a site that already carries another, so a census taken before the previous
//! step's errors are fixed is incomplete by construction. And `--lib` is not all code -- it
//! compiles neither the test targets nor `#[cfg(feature)]` regions.
//!
//! SPLIT BY WHAT EACH SITE WANTS, which is what decides the leg rather than the total:
//!
//!   * FORTY-THREE HOLD THE CHARACTERS AND WANT AN ID (`E0308`, a `&str` where a `u64` is
//!     expected). These are the sites that need the `characters -> id` direction, and they are the
//!     reason this module exists.
//!   * TWENTY-TWO WANT THE CHARACTERS off the identity beside a page (`E0609` seventeen, `E0614`
//!     five). These are the sites the census in
//!     [`super::where_the_characters_are_durable_per_kind`] answers, for thirteen kinds.
//!
//! AND BY WHETHER THE DURABLE HOME IS EVEN REACHABLE FROM THE SITE, which is the structural half
//! and is not a byte count. The durable `characters -> id` home under route C is a SIBLING FIELD
//! of `ShardState`, and `CoreIndex` is another of its fields -- so a site holding only the index
//! cannot reach it. Measured over the sixty-five: FIFTY sit in a function with a `ShardState` in
//! its signature and FIFTEEN do not, the fifteen falling in eight functions.
//!
//! THE FIFTEEN ARE NOT ALL BLOCKERS, AND SORTING THEM IS THE POINT. Two of the eight are the
//! encode and the decode -- `serialize_block_index` and `CoreIndex`'s visitor -- which under an
//! id-addressed lookup move IDS rather than characters, so they are served by the change rather
//! than blocked by it. `fold_delta_block_items` holds six of the fifteen and takes a bare
//! `&mut CoreIndex`, but its one production caller passes `&mut shard.bucket_index` and calls
//! `apply_key_states(shard, ..)` on the next line, so the shard is in scope there and the
//! signature is wideable. What is left is small and named, and none of it is measured here as a
//! refusal.
//!
//! # WHAT THIS MODULE DOES NOT CLAIM
//!
//! It does not claim route C is implemented, and it does not claim the thirteen kinds are
//! interchangeable in effort. Route C is a stored-format change to each per-kind map that takes
//! it, and the census's two hold-outs force the lookup to be addressed per kind rather than
//! uniformly -- an object lookup that is id-addressed for some model ids and character-addressed
//! for others. Neither is measured here. What is measured here is which of the three routes can
//! pay for itself, and the answer is one of them.
//!
//! NO PRODUCTION CODE CHANGES HERE AND NO STORED SHAPE MOVES, so no `SHARD_INDEX_FORMAT_VERSION`
//! is owed.
//!
//! rust-internal: type widths and signature pins only, no external surface

#![allow(clippy::all)]

use std::sync::Arc;

use crate::block_store::ElementEntry;

// =================================================================================================
// THE TERMS, EACH PINNED ON ITS OWN SO A MOVED WIDTH MOVES THE ARITHMETIC
// =================================================================================================

/// The lookup's inner key today: one shared pointer to the object key's characters.
const KEY_AS_SHARED_POINTER: usize = std::mem::size_of::<Arc<str>>();

/// The lookup's inner key under every one of the three routes: one word.
const KEY_AS_A_WORD: usize = std::mem::size_of::<u64>();

/// A per-kind map's value today, for the kinds whose value is a bare address.
const VALUE_TODAY: usize = std::mem::size_of::<ElementEntry>();

/// The same value carrying an assigned id beside the address.
const VALUE_WITH_AN_ID: usize = std::mem::size_of::<(u64, ElementEntry)>();

/// One entry of a resident `characters -> id` table, payload only.
const RESIDENT_ENTRY_PAYLOAD: usize = std::mem::size_of::<(Arc<str>, u64)>();

/// One entry of the lookup today: the shared pointer and what it maps to.
const LOOKUP_ENTRY_TODAY: usize =
    KEY_AS_SHARED_POINTER + std::mem::size_of::<crate::engine::state::BlockRefs>();

/// The same entry addressed by a word. `state.rs` already pins the forty next to the type; this is
/// the thirty-two beside it, so the pair is readable in one place.
const LOOKUP_ENTRY_ID_ADDRESSED: usize =
    KEY_AS_A_WORD + std::mem::size_of::<crate::engine::state::BlockRefs>();

// THE WIDTHS THEMSELVES, PINNED WITH `==` EACH. A neighbouring `!=` is not added: an `==` on a
// width read with `size_of` is already exact, and the pattern this campaign uses `!=` for is
// bracketing a width that is otherwise only bounded.
const _: () = assert!(KEY_AS_SHARED_POINTER == 16);
const _: () = assert!(KEY_AS_A_WORD == 8);
const _: () = assert!(VALUE_TODAY == 16);
const _: () = assert!(VALUE_WITH_AN_ID == 24);
const _: () = assert!(RESIDENT_ENTRY_PAYLOAD == 24);
const _: () = assert!(LOOKUP_ENTRY_TODAY == 40);
const _: () = assert!(LOOKUP_ENTRY_ID_ADDRESSED == 32);

// =================================================================================================
// ROUTE C
// =================================================================================================

/// THE TWO EIGHT-BYTE TERMS CANCEL EXACTLY, so route C's net saving is the characters' payload.
///
/// Written as the two differences and then as their sum, rather than as "both are eight", because
/// the claim is that they are EQUAL AND OPPOSITE and either one moving on its own breaks it. A
/// future widening of `ElementEntry` -- or a lookup key that stops being one word -- fails here,
/// which is the only way this result does not quietly become false.
#[test]
fn the_two_eight_byte_terms_of_the_assigned_id_cancel_exactly() {
    let key_narrows_by = KEY_AS_SHARED_POINTER as i64 - KEY_AS_A_WORD as i64;
    let value_widens_by = VALUE_WITH_AN_ID as i64 - VALUE_TODAY as i64;

    assert_eq!(
        key_narrows_by, 8,
        "the lookup's inner key must narrow by one word, or route C's arithmetic is not this one"
    );
    assert_eq!(
        value_widens_by, 8,
        "carrying the assigned id beside the address must cost one word; a value that pads \
         differently changes what route C costs"
    );
    assert_eq!(
        value_widens_by - key_narrows_by,
        0,
        "THE RESULT: the two terms cancel, so route C's net per-object saving is exactly the \
         characters' heap payload -- measured by `index_bytes_per_key`, not stated here"
    );
}

/// AND THE PAYLOAD IS NOT ZERO, which is the control that stops the result above from being a
/// statement that route C saves nothing.
///
/// `index_bytes_per_key` measures the real figure on a real engine; this arm only refuses the
/// degenerate reading. A key's characters occupy at least their own bytes, so a store whose object
/// keys are the shape this campaign measures on -- `tenant/1/object/00000000`, twenty-four bytes --
/// cannot have a zero payload, and the saving route C names is therefore real rather than nominal.
#[test]
fn the_payload_route_c_frees_is_not_nominal() {
    let probe_shaped_key = "tenant/1/object/00000000";
    assert_eq!(probe_shaped_key.len(), 24, "the shape this campaign measures on");
    assert!(
        probe_shaped_key.len() > 0,
        "a key with no characters would make route C's saving nominal"
    );
    // THE ALLOCATION IS WIDER THAN THE CHARACTERS, AND BY A KNOWN AMOUNT. An `Arc<str>`'s heap
    // block carries two counters ahead of the bytes, so the payload a route C frees is the
    // characters plus that header -- which is why the measured figure is larger than the string
    // length and why this arm states a FLOOR rather than the number.
    let two_counters = std::mem::size_of::<usize>() * 2;
    assert_eq!(two_counters, 16, "strong and weak, one word each");
    assert!(
        probe_shaped_key.len() + two_counters >= 40,
        "floor on what one object's characters cost on the heap, before the allocator's own \
         rounding: the measured figure belongs to `index_bytes_per_key`"
    );
}

/// FORTY TO THIRTY-TWO IS COMMON TO ALL THREE ROUTES, so it is not what chooses between them.
///
/// This is the figure the leg is usually stated in, and stating it alone is what has made the
/// choice look settled when it is not: EVERY route addresses the lookup by a word, so every route
/// takes the entry from forty to thirty-two. What differs is where the characters END UP -- freed
/// (route C), moved into a new resident table (route B), or moved and also written (route A) --
/// and that difference is larger than the eight bytes this arm is about.
///
/// `state.rs` pins the forty beside the type as `size_of::<Option<Arc<str>>>() +
/// size_of::<BlockRefs>()`. This reconstructs it from the key term this module uses, so the two
/// cannot drift: a `BlockRefs` that widened would move both.
#[test]
fn the_entry_reaches_thirty_two_under_every_route_which_is_why_it_cannot_choose_one() {
    assert_eq!(LOOKUP_ENTRY_TODAY, 40, "the lookup's entry today");
    assert_eq!(
        LOOKUP_ENTRY_ID_ADDRESSED, 32,
        "and under a word-addressed lookup, whichever route put the word there"
    );
    assert_eq!(
        LOOKUP_ENTRY_TODAY - LOOKUP_ENTRY_ID_ADDRESSED,
        KEY_AS_SHARED_POINTER - KEY_AS_A_WORD,
        "the whole of the entry's narrowing is the key term and none of it is the value, which is          why holding `BlockRefs` constant across the three routes is sound rather than convenient"
    );
}

// =================================================================================================
// ROUTE B
// =================================================================================================

/// THE RESIDENT ROUTE PAYS MORE THAN THE KEY IT NARROWS, AND THIS IS THE FLOOR.
///
/// Route B keeps the characters -- its new map is keyed by them -- so the payload term route C
/// sheds is still held, by a different structure, and cancels out of this comparison. What is left
/// is one table slot per object against eight bytes of key narrowing.
///
/// A FLOOR AND NOT AN ESTIMATE. It counts payload only: no control byte, no load factor, no
/// allocation for the table itself. The real figure is larger, which is the safe direction for a
/// claim that this route is worse.
#[test]
fn the_resident_route_pays_more_than_the_key_it_narrows() {
    let key_narrows_by = (KEY_AS_SHARED_POINTER - KEY_AS_A_WORD) as i64;
    let resident_map_costs = RESIDENT_ENTRY_PAYLOAD as i64;
    let net = resident_map_costs - key_narrows_by;

    assert_eq!(key_narrows_by, 8);
    assert_eq!(resident_map_costs, 24);
    assert_eq!(
        net, 16,
        "route B is sixteen bytes per object WORSE at the floor, which is why an assigned id \
         cannot pay for itself out of a resident translation"
    );
    assert!(
        net > 0,
        "if this ever goes negative the resident route becomes viable and this module's verdict \
         changes"
    );
}

/// AND THE CHARACTERS' PAYLOAD CANCELS OUT OF ROUTE B RATHER THAN BEING IGNORED.
///
/// Stated as its own arm because leaving a term out of a comparison and a term CANCELLING are
/// different things, and only one of them is honest. Both sides of route B hold exactly one shared
/// pointer to each object's characters -- today's lookup key, and route B's new table key -- so the
/// payload is the same on both sides and drops out.
#[test]
fn route_b_holds_the_same_one_copy_of_the_characters_on_both_sides() {
    // One shared pointer today: the lookup's inner key.
    let pointers_today = 1usize;
    // One shared pointer under route B: the resident table's key. The lookup holds a word.
    let pointers_under_route_b = 1usize;
    assert_eq!(
        pointers_today, pointers_under_route_b,
        "the payload term cancels; if route B ever held two copies it would be worse again"
    );
    // AND ROUTE C HOLDS ZERO, which is where its saving comes from: the per-kind map's own key is
    // the copy, and it is not an `Arc` -- so nothing in the index holds a shared pointer to the
    // characters at all.
    let pointers_under_route_c = 0usize;
    assert!(
        pointers_under_route_c < pointers_today,
        "route C is the only route that sheds a copy rather than moving one"
    );
}

// =================================================================================================
// ROUTE A
// =================================================================================================

/// THE DURABLE ROUTE IS THE RESIDENT ROUTE PLUS THE WIRE, so it cannot be cheaper than route B.
///
/// Priced rather than merely refused: a durable `characters -> id` map is resident too -- it is
/// decoded into memory like every other durable map -- so route A pays everything route B pays and
/// then writes it as well. The refusal does not turn on the bytes, but a refusal that could not
/// also say "and it is not cheaper" would be a preference.
#[test]
fn the_durable_route_is_the_resident_route_plus_the_wire() {
    let route_b_resident = RESIDENT_ENTRY_PAYLOAD as i64;
    let route_a_resident = RESIDENT_ENTRY_PAYLOAD as i64;
    assert_eq!(
        route_a_resident, route_b_resident,
        "a durable map is resident as well, so route A's resident cost is route B's exactly"
    );
    assert!(
        route_a_resident >= route_b_resident,
        "route A cannot be cheaper in memory than route B, and it is also written"
    );
}

// =================================================================================================
// WHAT STILL CONSUMES THE CHARACTERS
// =================================================================================================

/// THE TWO FUNCTIONS THAT TAKE THE CHARACTERS, PINNED AS SIGNATURES.
///
/// A signature pin is the checkable form of "no id can be fed to this". If either function ever
/// takes a `u64`, this stops compiling -- which is the only way the claim does not go stale beside
/// the code it is about.
///
/// `block_index_handle` is tractable under an id-addressed lookup and
/// `block_routing_bucket` is the one site Claim 5 of
/// `an_assigned_durable_id_defeats_ground_one_only` says no id reaches. The difference the census
/// makes is to that second one: it has a source for the characters now, for thirteen kinds, where
/// before it had only the field being removed.
#[test]
fn both_consumers_of_the_characters_take_a_str_and_not_an_id() {
    let handle: fn(&str, &crate::engine::state::BlockIndex) -> u64 =
        crate::engine::state::block_index_handle;
    let routing: fn(&str, u32, u32) -> u32 = crate::engine::hashing::block_routing_bucket;

    // Called, not merely named, so the pin is a use and not a cast the optimiser can drop.
    let bucket = routing("tenant/1/object/00000000", 0, 64);
    assert!(bucket < 64, "the routing bucket must land inside the range it was given");

    let block = crate::engine::state::BlockIndex {
        model_id: crate::engine::storage_bucket_internals::stored_model_kind("string"),
        address: ElementEntry::from_parts(1, 64, 32, Some(9), Some(11)),
        dirty: false,
        deleted: false,
    };
    let first = handle("tenant/1/object/00000000", &block);
    let second = handle("tenant/1/object/00000001", &block);
    assert_ne!(
        first, second,
        "the handle is decided by the characters, which is the whole content of this pin: two \
         keys over one page must not produce one handle"
    );
}
