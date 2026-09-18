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
//! `a_page_only_the_model_maps_know_about_is_found_and_filed` constructs it, out of the same
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

/// A page address shaped like one the block store handed back.
///
/// The object id is EXPLICIT. `rebuild_bucket_block_ownership` reads
/// `address.object_id().unwrap_or_else(|| stable_block_object_id(..))` and files the result, so an
/// address carrying none comes back from a rebuild carrying one -- and `same_block_address`
/// compares object ids exactly. Supplying it keeps "the same page" the same page on both sides of
/// a rebuild, which is what the positive arm has to assert.
fn address_at(index: u64, routing_bucket: u32) -> BlockAddress {
    BlockAddress::from_parts(
        7,
        index * 128,
        128,
        Some(1_000 + index),
        Some(9_000 + index),
        Some(routing_bucket),
        None,
    )
}

fn key_of(index: u64) -> String {
    format!("k-{index:06}")
}

/// A shard holding `count` string pages in the model map, and in the bucket index too -- except
/// for pages routing to `unfiled_bucket`, which are left in the map alone.
///
/// Filed with `upsert_bucket_index_block`, the function a live write files through, so the index
/// half of the fixture is built the way production builds it rather than assembled by hand.
fn shard_with(count: u64, unfiled_bucket: Option<u32>) -> ShardState {
    let mut shard = ShardState::default();
    let mut index = 0u64;
    while index < count {
        let routing_bucket = (index as u32) % SPREAD;
        let address = address_at(index, routing_bucket);
        shard.strings.insert(key_of(index), address.clone());
        if Some(routing_bucket) != unfiled_bucket {
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
    shard
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
fn a_shard_with_no_live_model_page_establishes_nothing() {
    let mut shard = shard_with(8, None);
    shard.strings.clear();
    assert!(
        !shard.bucket_index.bucket_map.is_empty(),
        "VACUITY: with an empty bucket index the empty-map arm would answer this instead"
    );

    reset_promote_model_map_check_counts();
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, u32::MAX);
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
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut fresh, 0, u32::MAX);
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
fn an_empty_bucket_index_under_live_model_pages_is_rebuilt_whole() {
    let mut shard = shard_with(32, None);
    shard.bucket_index.bucket_map.clear();
    shard.bucket_index.object_block_lookup.clear();
    assert!(
        !shard.strings.is_empty(),
        "VACUITY: with no model page this would be answered by the no-model-page arm instead"
    );

    reset_promote_model_map_check_counts();
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, u32::MAX);
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
            &address_at(index, (index as u32) % SPREAD),
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
fn an_index_that_names_every_live_page_rebuilds_nothing() {
    let mut shard = shard_with(64, None);
    let before: Vec<u32> = shard.bucket_index.bucket_map.keys().copied().collect();

    reset_promote_model_map_check_counts();
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, u32::MAX);
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
fn a_page_only_the_model_maps_know_about_is_found_and_filed() {
    let mut shard = shard_with(64, None);
    let orphan = 64u64;
    let orphan_bucket = (orphan as u32) % SPREAD;
    let orphan_address = address_at(orphan, orphan_bucket);
    shard.strings.insert(key_of(orphan), orphan_address.clone());

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
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, u32::MAX);
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
            &address_at(index, (index as u32) % SPREAD),
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
fn a_page_in_a_released_bucket_is_not_a_missing_page() {
    let released_bucket = 3u32;

    // CONTROL FIRST, and it is the half that can go vacuous.
    let mut control = shard_with(64, Some(released_bucket));
    reset_promote_model_map_check_counts();
    let control_promoted =
        promote_model_maps_to_bucket_index_authority(SHARD, &mut control, 0, u32::MAX);
    let (_, control_rebuilds, control_pages) = promote_model_map_check_counts();
    assert!(
        control_promoted,
        "CONTROL: with bucket {released_bucket}'s pages unfiled and NOT recorded as released, \
         they are missing and the check must notice -- if this fails the fixture is not \
         presenting a missing page at all and the exemption below proves nothing"
    );
    assert_eq!(
        (control_rebuilds, control_pages),
        (1, 64),
        "control: rebuilt, 64 pages walked"
    );

    // THE ARM. The identical fixture, plus the release record.
    let mut shard = shard_with(64, Some(released_bucket));
    shard.bucket_index.released_buckets.insert(released_bucket);
    reset_promote_model_map_check_counts();
    let promoted = promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, u32::MAX);
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
fn the_check_does_not_allocate_per_page_it_walks() {
    fn cost(count: u64) -> (u64, u64) {
        let mut shard = shard_with(count, None);
        reset_promote_model_map_check_counts();
        let before = crate::alloc_probe::counted_now().expect("built with `alloc-probe`");
        let promoted =
            promote_model_maps_to_bucket_index_authority(SHARD, &mut shard, 0, u32::MAX);
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
    let pages_dir = dir.path().join("pages");
    let indexes_dir = dir.path().join("indexes");
    let records = 200usize;

    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        dir.path().join("cache-a"),
        &pages_dir,
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
        &pages_dir,
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
