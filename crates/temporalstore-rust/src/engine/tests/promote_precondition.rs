// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE MODEL-MAP PROMOTION CHECK DECIDES, AND WHAT IT COSTS TO DECIDE IT.
//!
//! `promote_model_maps_to_bucket_index_authority` is the precondition in front of
//! `rebuild_bucket_block_ownership`: it asks whether the bucket index already names every live
//! model-map page, and rebuilds ownership only if it does not. #1822 is what happens when that
//! rebuild runs without one in front of it -- a restart served 0 of 6 hash fields against 6 of 6
//! strings -- and the invariant was written down only in a comment at the OTHER call site.
//!
//! #1888 then measured the check. It materialised a vector of every live block entry in the shard
//! to run one `any()` over it: 4.0 allocations per record the shard holds, 22% of a restore's
//! index fold, 10% of the whole restore, and on both configurations it measured the answer was
//! always "nothing is missing". The vector is now gone. The walk is not -- the walk IS the check.
//!
//! SO TWO SEPARATE THINGS HAVE TO BE TRUE, asserted separately and in this order so a mutant that
//! kills the first does not stop the second from being reached:
//!
//!   1. THE DECISION IS UNCHANGED. Five arms, each with a control:
//!      * no live model-map page at all -> `false`, establishing nothing, asserted on a shard
//!        with a NON-EMPTY index and again on a wholly empty one, where two arms both apply;
//!      * an index that names everything -> `false`, and the index is not moved;
//!      * an EMPTY index under live model pages -> `true` -- the other side of an `||` whose two
//!        halves each answer every case the other covers wrongly;
//!      * an index missing ONE page, in a shard whose other buckets -- the orphan's own included
//!        -- are present and populated, so it is the `any()` deciding and not the empty-map arm
//!        -> `true`, and every page is named afterwards;
//!      * a page routing to a RELEASED bucket -> not missing, against a control that shows the
//!        same fixture answering `true` without the release record.
//!   2. THE CHECK NO LONGER COSTS THE STORE, in allocations, over a denominator of the pages the
//!      walk actually offered it.
//!
//! IS THE POSITIVE ARM REACHABLE AT ALL? It has to be, or this is a precondition nobody can test
//! and "none is ever missing" is a property of the code rather than of two measured corpora.
//! `a_block_only_the_model_maps_know_about_is_found_and_filed` constructs it, out of the same
//! `upsert_bucket_index_block` a live write files through. `a_restore_says_which_arm_it_took`
//! then reports which arm a real restart takes over a store-sized denominator -- the negative
//! one, which is precisely why the vector was pure cost there.
#![allow(clippy::all)]
use super::*;
use crate::engine::state::ShardState;
use crate::engine::storage_bucket_internals::{
    promote_model_map_check_counts, promote_model_maps_to_bucket_index_authority,
    reset_promote_model_map_check_counts, upsert_bucket_index_block,
};

const SHARD: ShardId = 1;
const SPREAD: u32 = 8;

/// THE RANGE THE FIXTURE RUNS ON, AND WHY IT IS NARROW.
///
/// A page's bucket is `block_routing_bucket(object_key, start, end)` now, so nothing about this
/// fixture chooses it -- it is decided by the key and the range. Over the WHOLE keyspace 64 keys land
/// in 64 distinct buckets, and the positive arm below needs the orphan's bucket to hold OTHER pages:
/// its whole point is that an implementation noticing only a wholly absent BUCKET would pass. Eight
/// buckets is what makes that collision certain rather than a one-in-67-million accident.
///
/// Every call into the engine here passes the same range, and `bucket_of` computes over it, so the
/// fixture and the code under test partition the shard the same way.
const END: u32 = SPREAD - 1;

/// A page address shaped like one the block store handed back.
///
/// # THE PAGE ORDINAL IS ZERO, AND ZERO IS THE ONLY VALUE THIS FIXTURE MAY FILE
///
/// Every page here is filed into the string map through `set_string_address`, whose row is an
/// `ObjectSlot`: the inline arm keeps the ordinal's PRESENCE and has no room for its VALUE, so a
/// row filed with ordinal `n != 0` hands back 0. `same_block_address` compares the ordinal and
/// `block_ref_key_from_parts` puts it in the index key, so an address filed through the ROW and the
/// same `ElementEntry` filed into the BUCKET INDEX were a different page to every reader -- which
/// is what made four of the five arms below answer the opposite of what they assert, in four
/// different places, none of which names an address.
///
/// The ordinal names WHICH page of an object an address is, and every page here belongs to a
/// single-page object, so zero is not a convenience: it is the only value the store can keep.
/// `ObjectRecord::from_object_rows` refuses a nonzero one by name as
/// `NonZeroPageOrdinalDoesNotFitInline`, `set_address` coerces it and counts the coercion
/// (`the_write_back_door_and_the_page_ordinal`), and #2322 corrected the identical datum in
/// `model_kind_registry_guards`. So this changes what the fixture says, not what it tests.
///
/// IT IS NOT LEFT TO A LITERAL TO STAY RIGHT. `shard_with_unfiled` reads every row back as it
/// files it, and `the_model_maps_hand_back_the_addresses_the_index_was_filed_with` compares the two
/// columns directly, so a fixture that files an unkeepable ordinal again fails at the filing rather
/// than four assertions away.
///
/// The fifth argument is INERT -- `ElementEntry::try_from_parts` consumes only its presence, and
/// `generation()` is derived from the ordinal -- so the pages here are told apart by their OFFSET,
/// which is distinct per index.
fn address_at(index: u64) -> ElementEntry {
    ElementEntry::from_parts(7, index * 128, 128, Some(0), Some(9_000 + index))
}

fn key_of(index: u64) -> String {
    format!("k-{index:06}")
}

/// WHERE A PAGE OF THIS FIXTURE IS ACTUALLY FILED, and not a number this file picks.
///
/// The fixture used to choose a bucket (`index % SPREAD`) and stamp it onto the address, because the
/// filing site read the bucket off the address. It derives the bucket from the KEY now, so a fixture
/// that chose one would be filing pages into buckets the engine does not put them in -- and the
/// vacuity checks below, which look the orphan's own bucket up in the map, would look in the wrong
/// place. This is the engine's own expression.
fn bucket_of(index: u64) -> u32 {
    crate::engine::hashing::block_routing_bucket(&key_of(index), 0, END)
}

/// A shard holding `count` string pages in the model map, and in the bucket index too -- except
/// for pages routing to `unfiled_bucket`, which are left in the map alone.
///
/// Filed with `upsert_bucket_index_block`, the function a live write files through, so the index
/// half of the fixture is built the way production builds it rather than assembled by hand.
fn shard_with(count: u64, unfiled_group: Option<u64>) -> ShardState {
    shard_with_unfiled(count, unfiled_group).0
}

/// The same, returning the buckets whose pages were left OUT of the index.
///
/// `unfiled_group` selects by `index % SPREAD` rather than by bucket id, because the bucket a page
/// lands in is now `block_routing_bucket(key, ..)` and nothing chooses it. The buckets those pages
/// route to are returned, so the release test can record exactly them.
fn shard_with_unfiled(count: u64, unfiled_group: Option<u64>) -> (ShardState, BTreeSet<u32>) {
    let mut shard = ShardState::default();
    // `upsert_bucket_index_block` reads the shard's own range to decide where to file. An unstamped
    // state answers the whole keyspace, which would put every key in a bucket of its own.
    shard.set_routing_range(0, END);
    let mut unfiled_buckets = BTreeSet::new();
    let mut index = 0u64;
    while index < count {
        let address = address_at(index);
        shard.set_string_address(std::sync::Arc::from(key_of(index).as_str()), &address.clone());
        // READ THE ROW BACK, following #2322. A string row narrows a page ordinal, so a fixture
        // that files an address the row cannot keep presents one address to the model-map walk and
        // a different one to the bucket index below -- and every arm in this file compares the two.
        // Asserting it here fails at the filing instead of four assertions away.
        assert_eq!(
            shard.string_address(&key_of(index)).as_ref(),
            Some(&address),
            "the string map did not hand back the address this fixture filed for `{}`: the row \
             narrowed it, so the walk and the index are being compared on different pages",
            key_of(index),
        );
        if Some(index % u64::from(SPREAD)) == unfiled_group {
            unfiled_buckets.insert(bucket_of(index));
        } else {
            upsert_bucket_index_block(
                &mut shard,
                SHARD,
                "string",
                &key_of(index),
                None,
                address,
                false,
            );
        }
        index += 1;
    }
    (shard, unfiled_buckets)
}

/// WHAT THE MODEL MAPS HOLD versus WHAT A READER OF THEM SEES, SIDE BY SIDE.
///
/// Every arm below turns on ONE question -- is the address the model-map walk hands out the same
/// address the bucket index was filed with -- and when the answer is no they fail in four separate
/// places, none of which names an address. `an_empty_bucket_index_under_live_model_blocks_is_rebuilt_whole`
/// read `left: 0, right: 32`: thirty-two rows in the maps and not one of them named by an index
/// rebuilt from those same rows. That reads like a broken rebuild and was a one-field narrowing.
///
/// So this asks the question directly and prints both columns. A string row is an `ObjectSlot`
/// whose inline arm keeps a page ordinal's PRESENCE and drops its VALUE; `same_block_address`
/// compares the ordinal and `block_ref_key_from_parts` puts it in the index key, so a row filed
/// with a nonzero ordinal is a DIFFERENT page to every reader than the index entry filed from the
/// same `ElementEntry`. The nonzero case is measured beside the fixture's own, rather than argued.
///
/// rust-internal: guards this crate's own fixture against the row width, no product behaviour
#[test]
fn the_model_maps_hand_back_the_addresses_the_index_was_filed_with() {
    const PAGES: u64 = 32;
    let shard = shard_with(PAGES, None);

    let mut held = 0u64;
    let mut round_tripped = 0u64;
    let mut named_by_index = 0u64;
    let mut index = 0u64;
    while index < PAGES {
        let filed = address_at(index);
        if let Some(read_back) = shard.string_address(&key_of(index)) {
            held += 1;
            if read_back == filed {
                round_tripped += 1;
            }
            if shard.bucket_index.contains_object_block_address(
                "string",
                &key_of(index),
                None,
                &read_back,
            ) {
                named_by_index += 1;
            }
        }
        index += 1;
    }

    // THE NARROWING ITSELF, on its own shard, so the two columns are comparable rather than
    // described. The same `ElementEntry` goes into the row and into the index; what comes back out
    // of the row is then looked up in that index.
    let mut narrowing = ShardState::default();
    narrowing.set_routing_range(0, END);
    let unkeepable = ElementEntry::from_parts(7, 0, 128, Some(11), Some(22));
    narrowing.set_string_address(std::sync::Arc::from("probe"), &unkeepable.clone());
    upsert_bucket_index_block(
        &mut narrowing,
        SHARD,
        "string",
        "probe",
        None,
        unkeepable.clone(),
        false,
    );
    let narrowed = narrowing.string_address("probe").expect("the row was filed");
    let index_names_what_was_filed = narrowing
        .bucket_index
        .contains_object_block_address("string", "probe", None, &unkeepable);
    let index_names_what_the_row_returns = narrowing
        .bucket_index
        .contains_object_block_address("string", "probe", None, &narrowed);

    println!(
        "\n  WHAT THE MAPS HOLD vs WHAT A READER OF THEM SEES, at {PAGES} string pages\n\
         \n    model-map rows                                      {held}\n\
           .. handing back the address the fixture filed        {round_tripped}\n\
           .. named by the bucket index at THAT address         {named_by_index}\n\
         \n    ONE ROW, ONE INDEX ENTRY, ONE `ElementEntry`: page ordinal filed -> read back\n\
           {:?} -> {:?}   the fixture's own value\n\
           {:?} -> {:?}   nonzero: the row has room for the PRESENCE, not the VALUE\n\
           index names the address that was FILED         {index_names_what_was_filed}\n\
           index names the address the ROW HANDS BACK     {index_names_what_the_row_returns}\n",
        address_at(0).block_id(),
        shard.string_address(&key_of(0)).and_then(|at| at.block_id()),
        unkeepable.block_id(),
        narrowed.block_id(),
    );

    assert_eq!(held, PAGES, "VACUITY: the fixture filed no string row, so nothing here is compared");
    assert_eq!(
        round_tripped, PAGES,
        "{} of {PAGES} model-map rows hand back an address other than the one the fixture filed, \
         so the walk and the index in every arm below are being compared on different pages",
        PAGES - round_tripped,
    );
    assert_eq!(
        named_by_index, PAGES,
        "the bucket index names {named_by_index} of {PAGES} pages at the address the model map \
         hands out, so the promotion check is offered missing pages that are not missing",
    );

    // THE TRIPWIRE, and the condition for deleting all of this. While the row narrows, the fixture
    // above must file zero. The day the inline arm round-trips a nonzero ordinal -- the owned array
    // lands and the write-back door routes to it -- this fails, and that is the prompt to say so
    // here rather than to leave a literal pinned for a reason that has expired.
    assert_ne!(
        narrowed, unkeepable,
        "the string row now round-trips a nonzero page ordinal. If that is deliberate, this \
         assertion and the zero pinned in `address_at` are what should be deleted, in that change"
    );
    assert!(
        index_names_what_was_filed && !index_names_what_the_row_returns,
        "the row and the index no longer disagree about an unkeepable ordinal (filed: \
         {index_names_what_was_filed}, read back: {index_names_what_the_row_returns}), so the \
         reason the fixture files zero has changed and this file should say what it is now"
    );
}

/// ARM 1 OF 5: no live model-map page at all -> `false`, having established nothing.
///
/// Its own test because the per-execute caller in `engine.rs` reads exactly this answer to decide
/// whether to latch its fast-skip flag -- "promote returns false without establishing anything on
/// an empty shard" is the comment there -- so a check that answered `true` here would latch the
/// skip on a shard it had never actually checked.
///
/// The bucket index is NOT empty, which is what makes this the arm under test: an implementation
/// that answered from the index rather than from the maps would go the other way.
#[test]
fn a_shard_with_no_live_model_block_establishes_nothing() {
    let mut shard = shard_with(8, None);
    shard.strings_mut().clear();
    assert!(
        !shard.bucket_index.bucket_map.is_empty(),
        "VACUITY: with an empty bucket index the empty-map arm would answer this instead"
    );

    reset_promote_model_map_check_counts();
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, END);
    let (checks, rebuilds, pages) = promote_model_map_check_counts();

    assert!(
        !promoted,
        "an empty set of model-map pages establishes nothing, so this must not report a rebuild \
         -- the per-execute fast-skip in engine.rs latches on this answer"
    );
    assert_eq!(
        (checks, rebuilds, pages),
        (1, 0, 0),
        "one check, no rebuild, no page offered"
    );

    // SECOND HALF, after the first and not folded into it: a WHOLLY empty shard, where the
    // empty-bucket-map arm and the no-model-page arm both apply. The no-model-page arm has to
    // win, or the very first command against a fresh shard rebuilds an index derived from
    // nothing and latches the fast-skip on the result.
    let mut fresh = ShardState::default();
    reset_promote_model_map_check_counts();
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut fresh, 0, END);
    let (_, rebuilds, pages) = promote_model_map_check_counts();
    assert!(
        !promoted,
        "an empty shard has no model-map page, and that must be answered before `bucket_map` is \
         asked whether it is empty -- it is"
    );
    assert_eq!((rebuilds, pages), (0, 0), "empty shard: no rebuild, no page offered");
}

/// ARM 3 OF 5, THE OTHER HALF OF THE MISSING-PAGE TEST: an EMPTY bucket index under live model pages.
///
/// Its own arm because it is decided by the other side of an `||`, and either side alone answers
/// every case the other one covers WRONGLY. This is the state a shard reaches when the index was
/// never built -- the recovery arm that hands `ShardState::default()` to the promote and expects
/// the whole index to come out of the model maps -- and an implementation that only ran the
/// per-page `any()` would find every page "missing" and reach the same `true`, which is why the
/// cheaper arm is read first and why removing it has to be visible.
#[test]
fn an_empty_bucket_index_under_live_model_blocks_is_rebuilt_whole() {
    let mut shard = shard_with(32, None);
    shard.bucket_index.bucket_map.clear();
    shard.bucket_index.object_block_lookup.clear();
    assert!(
        !shard.strings().is_empty(),
        "VACUITY: with no model page this would be answered by the no-model-page arm instead"
    );

    reset_promote_model_map_check_counts();
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, END);
    let (checks, rebuilds, pages) = promote_model_map_check_counts();

    assert!(
        promoted,
        "an index naming nothing cannot already name 32 live model-map pages"
    );
    assert_eq!((checks, rebuilds), (1, 1), "one check, and it rebuilt");
    assert_eq!(pages, 32, "VACUITY: the walk must still have been offered every page");
    let mut named = 0u64;
    let mut index = 0u64;
    while index < 32 {
        if shard.bucket_index.contains_object_block_address(
            "string",
            &key_of(index),
            None,
            &address_at(index),
        ) {
            named += 1;
        }
        index += 1;
    }
    assert_eq!(named, 32, "the rebuild must derive the whole index from the model maps");
}

/// ARM 2 OF 5: an index that names every live model page -> `false`, and NOTHING is rebuilt.
///
/// The answer every measured restore gets, and the reason the vector was pure cost.
#[test]
fn an_index_that_names_every_live_block_rebuilds_nothing() {
    let mut shard = shard_with(64, None);
    let before: Vec<u32> = shard.bucket_index.bucket_map.keys().copied().collect();

    reset_promote_model_map_check_counts();
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, END);
    let (checks, rebuilds, pages) = promote_model_map_check_counts();

    assert!(
        !promoted,
        "every one of the 64 model-map pages is filed in the index at the same address, so there \
         is nothing to promote"
    );
    assert_eq!((checks, rebuilds), (1, 0), "one check, no rebuild");
    assert_eq!(
        pages, 64,
        "VACUITY: the check must have been offered all 64 pages -- a walk that saw none reaches \
         the same `false` for the wrong reason"
    );
    let after: Vec<u32> = shard.bucket_index.bucket_map.keys().copied().collect();
    assert_eq!(
        before, after,
        "a check that decided `false` must not have moved the index"
    );
}

/// ARM 4 OF 5, THE POSITIVE ONE: a page only the model maps know about is FOUND, and FILED.
///
/// THE ARM A FIXTURE THAT ONLY EXPRESSES "NOTHING IS MISSING" CANNOT REACH -- and a check whose
/// positive arm is never constructed cannot be told apart from one that was deleted. Deleting it
/// is the #1822 shape exactly: the rebuild behind this check is what repairs the index, so a
/// check that never fires means the repair never runs.
///
/// The orphan is one extra key in `strings` that was never filed, in a shard of 64 filed pages
/// over 8 buckets, routing to a bucket that EXISTS and holds eight other pages. So:
///
///   * `bucket_map` is not empty -- the `is_empty()` arm cannot be what decides;
///   * the orphan's own bucket is present and populated -- an implementation that only noticed a
///     wholly absent bucket would still report nothing missing;
///   * and `strings` is a `HashMap`, so the orphan's position in the walk is not fixed: an
///     implementation that tested only the first or the last page it was offered would fail this
///     most of the time rather than never.
#[test]
fn a_block_only_the_model_maps_know_about_is_found_and_filed() {
    let mut shard = shard_with(64, None);
    let orphan = 64u64;
    let orphan_bucket = bucket_of(orphan);
    let orphan_address = address_at(orphan);
    shard.set_string_address(std::sync::Arc::from(key_of(orphan).as_str()), &orphan_address.clone());

    assert!(
        !shard.bucket_index.bucket_map.is_empty(),
        "VACUITY: the empty-map arm must not be what decides this"
    );
    assert!(
        shard
            .bucket_index
            .bucket_map
            .get(&orphan_bucket)
            .is_some_and(|bucket| !bucket.block_index.is_empty()),
        "VACUITY: the orphan's own bucket must be present and hold other pages, or a check that \
         only noticed missing BUCKETS would pass this"
    );
    assert!(
        !shard.bucket_index.contains_object_block_address(
            "string",
            &key_of(orphan),
            None,
            &orphan_address,
        ),
        "VACUITY: the page must really be absent from the index before the check is asked"
    );

    reset_promote_model_map_check_counts();
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, END);
    let (checks, rebuilds, pages) = promote_model_map_check_counts();

    // "It noticed" is asserted before, and apart from, "the rebuild put the page back". They are
    // two claims, and a change that quietly stopped noticing would otherwise be reported here as
    // a failure to repair.
    assert!(
        promoted,
        "the model maps hold a live page at an address the index does not name, which is the \
         whole question this precondition asks"
    );
    assert_eq!((checks, rebuilds), (1, 1), "one check, and it rebuilt");
    assert_eq!(
        pages, 65,
        "VACUITY: the walk must still have been offered every page, orphan included"
    );
    assert!(
        shard.bucket_index.contains_object_block_address(
            "string",
            &key_of(orphan),
            None,
            &orphan_address,
        ),
        "the rebuild behind the check must leave the index naming the page only the maps had"
    );
    // The CONTROL on the repair: it re-derived the whole shard, not just the orphan.
    let mut named = 0u64;
    let mut index = 0u64;
    while index <= orphan {
        if shard.bucket_index.contains_object_block_address(
            "string",
            &key_of(index),
            None,
            &address_at(index),
        ) {
            named += 1;
        }
        index += 1;
    }
    assert_eq!(
        named,
        orphan + 1,
        "the rebuild must leave every one of the {} pages named",
        orphan + 1
    );
}

/// ARM 5 OF 5: a page routing to a RELEASED bucket is absent ON PURPOSE, and is not missing.
///
/// An ORDERED PAIR against its own control, because the two halves fail in opposite directions
/// and only the pair tells them apart: the SAME fixture without the release record must answer
/// `true` (so it really is presenting unnamed pages), and with it must answer `false` (so the
/// exemption is what is doing the work). A check that had simply stopped noticing anything would
/// pass the second half on its own.
///
/// What goes wrong without the exemption is not a wrong answer but a release that never survives
/// one execute: the first command after a release finds every released page unnamed, rebuilds the
/// whole shard, and undoes it.
#[test]
fn a_block_in_a_released_bucket_is_not_a_missing_block() {
    // ONE GROUP OF KEYS, and the buckets they route to. The fixture used to name a bucket id and
    // stamp it; the bucket is derived from the key now, so the group is selected by key and the
    // buckets it lands in are read back out.
    let released_group = 3u64;

    // CONTROL FIRST, and it is the half that can go vacuous.
    let (mut control, released_buckets) = shard_with_unfiled(64, Some(released_group));
    assert!(
        !released_buckets.is_empty(),
        "the fixture left no page unfiled, so neither half below is presenting a missing page"
    );
    reset_promote_model_map_check_counts();
    let control_promoted =
        promote_model_maps_to_bucket_index_authority(SHARD, &mut control, 0, END);
    let (_, control_rebuilds, control_blocks) = promote_model_map_check_counts();
    assert!(
        control_promoted,
        "CONTROL: with {released_buckets:?}'s pages unfiled and NOT recorded as released, they are \
         missing and the check must notice -- if this fails the fixture is not presenting a missing \
         page at all and the exemption below proves nothing"
    );
    assert_eq!(
        (control_rebuilds, control_blocks),
        (1, 64),
        "control: rebuilt, 64 pages walked"
    );

    // THE ARM. The identical fixture, plus the release record.
    let mut shard = shard_with(64, Some(released_group));
    for released_bucket in &released_buckets {
        shard.bucket_index.released_buckets.insert(*released_bucket);
    }
    reset_promote_model_map_check_counts();
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, END);
    let (_, rebuilds, pages) = promote_model_map_check_counts();
    assert!(
        !promoted,
        "a page routing to a released bucket is absent on purpose and must not count as missing \
         -- counting it makes the first command after a release rebuild the whole shard"
    );
    assert_eq!(
        (rebuilds, pages),
        (0, 64),
        "the arm: no rebuild, and the same 64 pages walked"
    );
}

/// WHAT THE CHECK COSTS, over a denominator of the pages it was offered.
///
/// The claim is that the check is no longer proportional to the store IN ALLOCATIONS. It is still
/// proportional to it in WORK, and has to be: the walk is the check. #1888 measured the old form
/// at 4.0 allocations per record the shard holds, which at these two sizes is 2,000 and 20,000.
///
/// TWO SIZES ten times apart, because one size cannot tell a per-page cost from a fixed one.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_check_does_not_allocate_per_block_it_walks() {
    fn cost(count: u64) -> (u64, u64) {
        let mut shard = shard_with(count, None);
        reset_promote_model_map_check_counts();
        let before = crate::alloc_probe::counted_now().expect("built with `alloc-probe`");
        let promoted =
            promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, END);
        let after = crate::alloc_probe::counted_now().expect("built with `alloc-probe`");
        let (_, rebuilds, pages) = promote_model_map_check_counts();
        assert!(
            !promoted && rebuilds == 0,
            "this arm must take the negative path -- the positive one rebuilds the shard, and \
             what would then be measured is the rebuild"
        );
        assert_eq!(
            pages, count,
            "VACUITY: a check offered no pages is cheap for the wrong reason"
        );
        (after.0 - before.0, pages)
    }

    let (small_allocs, small_pages) = cost(500);
    let (large_allocs, large_pages) = cost(5_000);
    println!(
        "  promotion check: {small_allocs} allocation(s) over {small_pages} pages walked, and \
         {large_allocs} over {large_pages}. The materialising form cost 4.0 per page: {} and {}.",
        small_pages * 4,
        large_pages * 4
    );

    // Halves, ordered, smaller claim first: a mutant that puts the vector back fails the small
    // arm, and the large arm is still reached and still reported.
    assert!(
        small_allocs * 2 < small_pages,
        "the check allocated {small_allocs} time(s) to walk {small_pages} pages; the form this \
         replaced cost 4.0 per page and the walk itself needs none"
    );
    assert!(
        large_allocs * 2 < large_pages,
        "the check allocated {large_allocs} time(s) to walk {large_pages} pages; the form this \
         replaced cost 4.0 per page and the walk itself needs none"
    );
}

/// WHICH ARM A REAL RESTART TAKES, and so what the vector was buying on that path.
///
/// #1888's "none is ever missing" was an observation about two corpora. This makes it a reported
/// quantity of an actual restore instead of a reading of the four call sites: a shard that took
/// writes, dumped a manifest, took more writes and went away, brought back the way a restart
/// brings it back.
///
/// It asserts the CENSUS and prints the verdict. That the checks ran at all, over a store-sized
/// denominator, is the durable claim -- a restore that legitimately starts taking the positive
/// arm is a change in the engine and not a failure here, while a restore that stops running the
/// check at all is the #1822 shape and fails.
#[test]
fn a_restore_says_which_arm_it_took() {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks_dir = dir.path().join("pages");
    let indexes_dir = dir.path().join("indexes");
    let records = 200usize;

    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-a"),
        &blocks_dir,
        &indexes_dir,
    );
    engine.load_shard(SHARD);
    let mut index = 0usize;
    while index < records {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("k-{index:08}"),
                value: vec![b'v'; 64],
            },
        });
        assert!(response.status.ok, "SET {index}: {response:?}");
        if index + 1 == records / 2 {
            engine
                .create_bucket_dump_manifest(SHARD, Vec::new())
                .expect("dump manifest");
        }
        index += 1;
    }
    // Dropped rather than unloaded: an unload materialises the base index, which is the durable
    // checkpoint a crash does not leave behind.
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-b"),
        &blocks_dir,
        &indexes_dir,
    );
    reset_promote_model_map_check_counts();
    restarted.load_shard(SHARD);
    let (checks, rebuilds, walked) = promote_model_map_check_counts();
    println!(
        "  restore of {records} records: {checks} promotion check(s), {rebuilds} of them \
         rebuilt, {walked} model-map page(s) walked"
    );

    // THE DENOMINATOR, and the reason this is not a tautology: the counters describe the restore
    // only if the restore actually brought the records back.
    let mut readable = 0usize;
    let mut probe = 0usize;
    while probe < records {
        if matches!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::StringGet {
                        key: format!("k-{probe:08}"),
                    },
                })
                .response,
            CommandResponse::Bytes { value: Some(_) }
        ) {
            readable += 1;
        }
        probe += 1;
    }
    assert_eq!(
        readable, records,
        "VACUITY: the restart served {readable} of {records} records, so the counters above \
         describe a broken restore"
    );
    assert!(
        checks > 0,
        "the restore ran no promotion check at all -- that is the #1822 shape, an ownership \
         rebuild with nothing in front of it"
    );
    assert!(
        walked >= records as u64,
        "the checks walked {walked} model-map page(s) for a {records}-record store, so they were \
         not looking at the recovered shard"
    );
}
