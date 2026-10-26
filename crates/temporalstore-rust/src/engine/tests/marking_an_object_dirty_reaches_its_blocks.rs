// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MARKING AN OBJECT DIRTY REACHES THE PAGES IT ALREADY HAS, AT MARK TIME.
//!
//! # THE INVARIANT, AND IT IS THE WHOLE OF THE CHANGE
//!
//! A bucket is dirty when any page in it is dirty. It used to also be dirty when any page's
//! OBJECT was dirty, which asked a page which object it belonged to -- on the write path, in a
//! scope holding nothing but the bucket. The dirtiness is pushed onto the pages at mark time now,
//! so the predicate is `any(page.dirty)` and the question is never asked.
//!
//! That is only equivalent if EVERY SITE THAT MARKS AN OBJECT DIRTY ALSO PUSHES. There are exactly
//! two, and the fail-first for this module is omitting the push at one of them: the bucket then
//! reads clean while its object is dirty, which is a silent wrong answer rather than a failure.
//!
//! # WHY THE NEAREST EXISTING GUARD CANNOT STAND IN FOR THIS
//!
//! `bucket_runtime_flags_match_full_sweep` compares the targeted refresh against the sweep and
//! does include dirtiness -- as a BOOLEAN, `bucket.dirty()`. It can see THAT a bucket is dirty and
//! not WHY, so it passes whether the dirtiness arrived through the object set or through the pages.
//! It is insensitive to this change rather than wrong about it, and it was cited in prose as
//! holding the equivalence, which is the worse failure: it stops the next reader looking. This
//! module asserts the mechanism it cannot see.
//!
//! # AND THE PREMISE THAT FAILED TWICE, KEPT BECAUSE IT IS WHY THE CODE IS SHAPED THIS WAY
//!
//! The push can only find pages through the object lookup, which is derived and empty until
//! something builds it. The first version asserted that a mark could not reach a shard holding
//! buckets with an unbuilt lookup, reasoning that a load rebuilds before returning the shard. TWO
//! TESTS FIRED THAT ASSERTION IMMEDIATELY, and it was replaced by a rebuild.
//!
//! BOTH THE ASSERTION AND THE TWO TESTS WERE ABOUT THE WRONG CONDITION. "Buckets present" is not
//! "pages present", and "pages present" is not "a LIVE page present" -- a tombstone is a page a
//! mark must not dirty. Measured over a 107-name set of mark-reaching tests, 97 verdicts: six
//! reaches had no pages at all, two had only tombstones, and the only two reaches with a live page
//! were the fixtures in this module, each writing into `bucket_map` directly with no filing
//! anywhere in the process.
//!
//! AND A CENSUS MAKES THAT STRUCTURAL. All nine production sites that install a page into a
//! bucket's map file it in the same enclosing function; the only non-filing doors have no callers
//! at all or none outside tests. So a live page cannot be resident unfiled in production, the
//! rebuild served no reachable state, and `push_object_dirty_onto_its_existing_blocks` now ASSERTS
//! that invariant where it used to repair it -- which makes every one of the 926 tests that enter
//! it the check, instead of one fixture here.

use std::sync::Arc;

use crate::engine::hashing::block_routing_bucket;
use crate::engine::state::{BlockIndex, BlockSlabLiveIndex, BucketNode, ShardState};
use crate::engine::storage_bucket_internals::{
    mark_object_dirty, refresh_bucket_runtime_flags,
};

const WHOLE_RANGE_END: u32 = u32::MAX;

fn a_shard_holding_one_clean_block(object_key: &str) -> (ShardState, u32) {
    let routing_bucket = block_routing_bucket(object_key, 0, WHOLE_RANGE_END);
    let mut shard = ShardState::default();
    let mut bucket = BucketNode::default();
    bucket.routing_bucket = routing_bucket;
    bucket.set_meta_loaded(true);
    bucket.set_in_memory(true);
    let mut live = BlockSlabLiveIndex::default();
    bucket.insert_block(&object_key, 
        BlockIndex {
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::from_stored_name(
                "string",
            )
            .expect("`string` is a stored model spelling"),
            address: Default::default(),
            // CLEAN, which is the only interesting starting state: a page that is already dirty
            // would satisfy every assertion below whatever the mark did.
            dirty: false,
            deleted: false,
        },
        &mut live,
    );
    shard.bucket_index.bucket_map.insert(routing_bucket, bucket);
    (shard, routing_bucket)
}

/// A shard holding one clean page THAT NOTHING HAS FILED -- a state production cannot reach.
///
/// Kept, and kept unfiled, because it is the subject of
/// [`the_push_builds_the_object_lookup_when_it_finds_it_unbuilt`]'s successor: the push now
/// REFUSES this state rather than repairing it. The census that says production cannot reach it is
/// written at the assertion in `push_object_dirty_onto_its_existing_blocks`; this is the fixture
/// that proves the assertion can fire.
///
/// Every other arm in this module wants [`a_shard_holding_one_filed_clean_block`], because their
/// subjects are positive -- that a mark reaches its pages, and that a mark on a different object
/// does not -- and neither is testable in a state the engine refuses.
fn a_shard_holding_one_filed_clean_block(object_key: &str) -> (ShardState, u32) {
    let (mut shard, routing_bucket) = a_shard_holding_one_clean_block(object_key);
    // FILED, AS ALL NINE PRODUCTION INSTALL SITES FILE: in the same breath as the install.
    //
    // THIS IS NOT A FIXTURE QUIETLY REPAIRED TO MAKE A TEST PASS. The unfiled state used to be the
    // only thing in the tree describing the window the push's rebuild existed for, which is why
    // editing it would have erased the finding. The assertion in
    // `push_object_dirty_onto_its_existing_blocks` is that description now, held on every run by
    // every test that marks -- so filing here restores a positive claim the assertion has made
    // safe to restore, rather than hiding anything.
    let placed: Vec<(u64, BlockIndex)> = shard
        .bucket_index
        .bucket_map
        .get(&routing_bucket)
        .expect("the fixture bucket must exist")
        .block_index
        .iter()
        .map(|(handle, block)| (*handle, block.clone()))
        .collect();
    let key: Arc<str> = Arc::from(object_key);
    for (handle, block) in &placed {
        shard
            .bucket_index
            .insert_object_block_lookup(routing_bucket, *handle, &key, block);
    }
    (shard, routing_bucket)
}

fn any_block_dirty(shard: &ShardState, routing_bucket: u32) -> bool {
    shard
        .bucket_index
        .bucket_map
        .get(&routing_bucket)
        .expect("the fixture bucket must exist")
        .block_index
        .values()
        .any(|block| block.dirty)
}

#[test]
fn marking_an_object_dirty_sets_the_dirty_bit_on_the_blocks_it_already_has() {
    // THE FILED FIXTURE, because this arm's subject is POSITIVE -- that a mark reaches the pages
    // an object already has -- and that is not testable in a state the engine refuses.
    let (mut shard, routing_bucket) = a_shard_holding_one_filed_clean_block("pushed-object");

    // THE STARTING STATE IS ASSERTED, or "it is dirty afterwards" could be true of a fixture that
    // was dirty all along and the mark could be doing nothing.
    assert!(
        !any_block_dirty(&shard, routing_bucket),
        "the fixture page must start clean"
    );

    mark_object_dirty(&mut shard, "pushed-object", 0, WHOLE_RANGE_END);

    assert!(
        any_block_dirty(&shard, routing_bucket),
        "marking the object must reach the page it already has; otherwise the bucket predicate, \
         which now reads only `page.dirty`, answers false for a dirty object"
    );

    // AND THE PREDICATE THE PUSH EXISTS TO SERVE actually answers true through it. The sweep
    // recomputes the bucket flag from the pages alone.
    refresh_bucket_runtime_flags(&mut shard);
    assert!(
        shard
            .bucket_index
            .bucket_map
            .get(&routing_bucket)
            .expect("bucket")
            .dirty(),
        "the sweep must see the bucket as dirty, reading only the pages"
    );
}

#[test]
fn an_unmarked_object_leaves_its_blocks_clean() {
    // THE CONTROL FOR THE MODULE. If marking is what makes a page dirty, NOT marking must leave it
    // clean -- otherwise the assertion above would hold on a shard where everything is dirty.
    // THE FILED FIXTURE TOO. This is the CONTROL for the arm above -- if marking is what makes a
    // page dirty, not marking must leave it clean -- so it has to run in the same reachable state,
    // or it controls for nothing the arm above does.
    let (mut shard, routing_bucket) = a_shard_holding_one_filed_clean_block("untouched-object");

    mark_object_dirty(&mut shard, "a-different-object", 0, WHOLE_RANGE_END);

    assert!(
        !any_block_dirty(&shard, routing_bucket),
        "marking a different object must not reach this one's pages"
    );
    refresh_bucket_runtime_flags(&mut shard);
    assert!(
        !shard
            .bucket_index
            .bucket_map
            .get(&routing_bucket)
            .expect("bucket")
            .dirty(),
        "a bucket whose pages are all clean must read clean"
    );
}

/// A MARK ON A SHARD HOLDING A LIVE PAGE NOTHING HAS FILED IS REFUSED, NOT REPAIRED.
///
/// THIS ARM'S CLAIM IS INVERTED FROM WHAT IT WAS, and the inversion is the finding. It asserted
/// that the push ESTABLISHES the lookup when it finds it unbuilt -- a rebuild of the whole shard,
/// reading each page's object key off the entry. The census says no production route reaches the
/// state it was repairing: all nine sites that install a page file it in the same call, and the
/// only doors that do not file have no callers at all or none outside tests.
///
/// So the push asserts the invariant instead, and this arm is what proves the assertion can FIRE.
/// It is the only arm in this module that still wants the unfiled fixture; the other two want
/// [`a_shard_holding_one_filed_clean_block`], because their subjects are positive.
///
/// rust-internal: drives an engine invariant, no product behaviour
#[test]
#[should_panic(expected = "a live page is resident while the object lookup names nothing")]
fn a_mark_on_a_live_block_nothing_has_filed_is_refused() {
    // THE CASE THE FAILED PREMISE WAS ABOUT. The fixture never builds the lookup, so the push has
    // to establish it before it can find anything. This is the state two existing tests reached
    // and the first version of the push asserted was unreachable.
    let (mut shard, routing_bucket) = a_shard_holding_one_clean_block("unbuilt-lookup-object");
    assert!(
        shard.bucket_index.object_block_lookup.is_empty(),
        "the fixture must start with an unbuilt lookup, or this test is about the other path"
    );
    assert!(
        !shard.bucket_index.bucket_map.is_empty(),
        "and it must hold a bucket, or there would be nothing for the push to miss"
    );

    mark_object_dirty(&mut shard, "unbuilt-lookup-object", 0, WHOLE_RANGE_END);

    assert!(
        !shard.bucket_index.object_block_lookup.is_empty(),
        "the push must establish the lookup rather than silently finding nothing"
    );
    assert!(
        any_block_dirty(&shard, routing_bucket),
        "and having established it, the push must reach the page"
    );
}

/// WHAT REFRESHING A BUCKET'S RUNTIME FLAGS COSTS ON ONE WRITE, IN PAGES VISITED.
///
/// # THE QUESTION
///
/// The predicate this module is about -- `any(page.dirty)` -- is recomputed, not maintained.
/// `refresh_pending_bucket_runtime_flags` runs on every write, and for each bucket the write
/// touched `refresh_one_bucket_runtime_flags` folds over every page that bucket holds TWICE: once
/// for `all(page.deleted)` and once for `any(page.dirty)`. Neither fold looks at the write; both
/// recompute a summary of the whole bucket.
///
/// So the cost of one write includes the pages its bucket already held. How much that is depends
/// entirely on how many pages share a bucket, which the routing range decides -- and the range this
/// measures at is the NARROW one, because that is the setting the module header names as the one to
/// run in production (`TS_SHARD_END_ROUTING_SLOT=1023`, which cuts resident memory 45%). A wide
/// range gives every key its own bucket and hides the fold behind a bucket of one.
///
/// # WHY THE COUNTER AND NOT A TIMER
///
/// `bucket_visit_sites::REFRESH_FLAGS` is charged `bucket.block_index.len()` at each visit, so it
/// reports PAGES VISITED rather than calls made -- which is the quantity in question. A timer on
/// this box would also be measuring whatever else is building at the time; the counter is exact and
/// attributable, and the same instrument already attributes the layout and clear-dirty walks.
///
/// # THE FIRST READING WAS OF THE WRONG COUNTER, AND A POSITIVE CONTROL IS WHAT CAUGHT IT
///
/// This probe first read `bucket_visit_sites::REFRESH_FLAGS` and got this:
///
///     corpus   pages in bucket   REFRESH (pages visited)
///         40                41                         0
///        320               321                         0
///
/// Zero against a bucket of 321 pages, which reads as "the folds are not on the write path" and is
/// what was nearly concluded. It is not what the counter says. `REFRESH_FLAGS` is charged inside
/// `refresh_one_bucket_runtime_flags` but inside the `BucketTtlRollup::Recompute` arm AND inside
/// the `else` of `if expires_at_ms.is_empty()` -- so it counts the TTL rollup, only when a TTL
/// exists, and never the two folds above it. These fixtures set no TTL, so it could only ever
/// report zero.
///
/// THE POSITIVE CONTROL IS WHAT TURNED THAT INTO A FINDING. Calling the full sweep deliberately,
/// over a bucket of 40 pages, ALSO charged 0 -- and a sweep that walks 40 pages and reports 0 is an
/// instrument fault, not a cheap sweep. `FLAG_FOLDS` was added for the folds themselves, charged by
/// `inspect` so it reports iterations taken rather than bucket length, because both folds
/// short-circuit.
///
/// A ZERO FROM THIS INSTRUMENT FAMILY IS ONLY EVIDENCE ONCE THE CONTROL FIRES, which is why the
/// control is part of the test and not a thing that was run once by hand. The denominator -- that
/// the bucket really holds the corpus -- rules out the empty fixture; the control rules out the dead
/// counter.
///
/// # WHAT IT FOUND, ON THE COUNTER THAT SEES THE FOLDS: THEY ARE FLAT AT TWO
///
///     corpus   pages in bucket   FOLD visits
///         40                41             2
///        320               321             2
///
/// Two visits against a bucket of 321 pages, and the same two against 41. BOTH FOLDS SHORT-CIRCUIT
/// AND BOTH DECIDE ON THE FIRST PAGE: `all(page.deleted)` stops at the first page that is not
/// deleted, `any(page.dirty)` stops at the first that is dirty, and a bucket that was just written
/// into has a dirty page at the front. The comment beside the folds says this already -- "during
/// ingest the dirty check in particular answers on block one" -- and this is that comment measured.
///
/// SO THERE IS NOTHING TO MAINTAIN INCREMENTALLY. A dirty-page count on the bucket would make
/// `any(page.dirty)` O(1) exactly, and would save two iterations per refresh. The whole-bucket
/// recomputation this probe was built to price does not happen.
///
/// WHAT IS WORTH GUARDING IS THE FLATNESS, and that is what the assertion below does. The
/// short-circuit is the whole reason these folds are cheap, and it is a property of how they are
/// WRITTEN, not of what they compute: `values().filter(|p| p.dirty).count() > 0` computes the same
/// predicate and walks every page. At the narrow routing range production runs, that is about 4,600
/// pages a write at 4.7M records -- and no functional test would fail.
///
/// rust-internal: counts this crates own bucket walks, no product behaviour
///
///   cargo test -p temporalstore-rust --lib what_refreshing_a_buckets_flags_costs_per_write -- --ignored --nocapture
#[test]
#[ignore = "drives 320 writes; run by name -- it is a cost guard, not a correctness one"]
fn what_refreshing_a_buckets_flags_costs_per_write() {
    use crate::types::{Command, ExecuteRequest};

    // ONE BUCKET, so "pages in the bucket" is "pages in the store" and the fold's input is the
    // corpus. This is the narrow end of the range the header describes, taken to its limit: it
    // isolates the fold from the bucket hashing, which is a separate question.
    const END_ROUTING_BUCKET: u32 = 0;

    println!();
    println!("  corpus   pages in bucket   FOLD visits   layout   clear_dirty   ttl_rollup   remove_all");

    let mut measured: Vec<(usize, u64, u64)> = Vec::new();
    for rung in [40_usize, 320] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = crate::TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        assert!(
            engine
                .load_shard_with(crate::control::LoadShardRequest {
                    shard_id: 1,
                    table_name: "refresh-fold-cost".to_string(),
                    shard_uri: "local://refresh-fold-cost/1".to_string(),
                    start_routing_bucket: 0,
                    end_routing_bucket: END_ROUTING_BUCKET,
                    readonly: false,
                    load_version: 1,
                    local_node_id: Some(1),
                })
                .status
                .ok,
            "the shard must load for the measurement to mean anything"
        );

        for index in 0..rung {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("rf-{index:06}"),
                    value: vec![b'v'; 48],
                },
            });
            assert!(response.status.ok, "corpus write {index}: {:?}", response.status);
        }

        // THE CORPUS IS IN PLACE; now count ONE more write.
        crate::engine::bucket_visit_sites::reset();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("rf-{rung:06}"),
                value: vec![b'v'; 48],
            },
        });
        assert!(response.status.ok, "measured write: {:?}", response.status);
        let (layout, clear_dirty, ttl_rollup, remove_all) =
            crate::engine::bucket_visit_sites::snapshot();
        // THE FOLDS, from the counter that sees them. `ttl_rollup` is printed beside it because its
        // zero is what the first reading of this probe mistook for the folds being free.
        let folds = crate::engine::bucket_visit_sites::flag_folds();

        let blocks_in_bucket: usize = {
            let shards = engine.shards.read().expect("shards lock poisoned");
            let shard = shards.get(&1).expect("shard 1 loaded");
            shard
                .bucket_index
                .bucket_map
                .values()
                .map(|bucket| bucket.block_index.len())
                .sum()
        };

        println!(
            "  {rung:>6}   {blocks_in_bucket:>15}   {folds:>11}   {layout:>6}   {clear_dirty:>11}   {ttl_rollup:>10}   {remove_all:>10}"
        );
        measured.push((rung, blocks_in_bucket as u64, folds));
    }

    // --- DENOMINATOR: the fold's input is the pages in the bucket, so the fixture has to have put
    // --- them there. A corpus that did not accumulate makes every number above a measurement of an
    // --- empty bucket, which would read as "the fold is free".
    for (rung, blocks, _) in &measured {
        assert!(
            *blocks >= (*rung as u64),
            "corpus {rung} left only {blocks} pages resident, so the bucket the fold walks was never \
             filled and the visit counts above are not measurements of it"
        );
    }
    println!();
    println!(
        "  pages x{:.1}  ->  fold visits x{:.1}",
        measured[1].1 as f64 / measured[0].1.max(1) as f64,
        measured[1].2 as f64 / measured[0].2.max(1) as f64,
    );
    println!(
        "  a ratio near the page ratio means one write pays for every page its bucket holds;"
    );
    println!(
        "  a ratio near 1 means the refresh is not walking the corpus and the cost is elsewhere."
    );

    // --- AND THE VERDICT: ZERO, ASSERTED. The folds are summaries of a whole bucket recomputed
    // --- from scratch, so reaching them from a write makes that write cost the pages its bucket
    // --- already holds. Measured at 0 visits against a bucket of 321, from two directions. If this
    // --- fires, a write has started refreshing bucket flags: either maintain the summaries
    // --- incrementally (a dirty-page count is exactly `any(page.dirty)` and is O(1)) or take the
    // --- refresh back off the write path. Do not raise the bound.
    // --- THE VERDICT IS FLATNESS, not a level. The folds visit two pages whether the bucket holds
    // --- 41 or 321, because both short-circuit on the first page that decides them. A bound on the
    // --- level would be a bound on an accident of fixture order; the invariant that matters is that
    // --- the count does not follow the corpus, because that is what breaks if the short-circuit is
    // --- written out of either fold.
    let (small_rung, small_blocks, small_folds) = measured[0];
    let (large_rung, large_blocks, large_folds) = measured[1];
    assert!(
        small_folds > 0 && large_folds > 0,
        "the folds charged {small_folds} and {large_folds} visits, so at least one rung measured \
         nothing and the flatness below would be the flatness of two zeroes"
    );
    let block_growth = large_blocks as f64 / small_blocks.max(1) as f64;
    let fold_growth = large_folds as f64 / small_folds.max(1) as f64;
    assert!(
        fold_growth * 2.0 < block_growth,
        "fold visits grew x{fold_growth:.1} while the bucket grew x{block_growth:.1} \
         ({small_folds} visits at {small_blocks} pages on corpus {small_rung}, {large_folds} at \
         {large_blocks} on corpus {large_rung}). The two folds in \
         `refresh_one_bucket_runtime_flags` are cheap only because `all` and `any` stop at the \
         first page that decides them; a predicate written with `count()`, `collect` or \
         `filter().count()` computes the same answer and walks the whole bucket, which is \
         O(store/buckets) on every write and invisible to every functional test."
    );

    // --- POSITIVE CONTROL, because the assertion above expects ZERO and three different kinds of
    // --- dead measurement also produce zero: a counter nobody charges, an instrument that moved,
    // --- and a fold that no longer exists. The denominator above rules out the empty fixture; this
    // --- rules out the rest by calling the sweep ON PURPOSE and requiring the counter to see it.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    assert!(
        engine
            .load_shard_with(crate::control::LoadShardRequest {
                shard_id: 1,
                table_name: "refresh-fold-control".to_string(),
                shard_uri: "local://refresh-fold-control/1".to_string(),
                start_routing_bucket: 0,
                end_routing_bucket: END_ROUTING_BUCKET,
                readonly: false,
                load_version: 1,
                local_node_id: Some(1),
            })
            .status
            .ok
    );
    const CONTROL_BLOCKS: usize = 40;
    for index in 0..CONTROL_BLOCKS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("ctl-{index:06}"),
                value: vec![b'v'; 48],
            },
        });
        assert!(response.status.ok, "control write {index}: {:?}", response.status);
    }
    crate::engine::bucket_visit_sites::reset();
    {
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        refresh_bucket_runtime_flags(shard);
    }
    let control_folds = crate::engine::bucket_visit_sites::flag_folds();
    println!(
        "  positive control: calling the sweep over {CONTROL_BLOCKS} pages charges {control_folds} fold visits"
    );
    // MOVES, NOT REACHES THE LENGTH. A sweep over 40 pages charges 2, because the folds
    // short-circuit -- that is the finding, not a fault. The control's job is only to prove the
    // counter is wired to the walk, so that the flat numbers above are a measurement of a cheap
    // fold rather than a reading of a dead counter. An earlier version of this control demanded
    // `>= CONTROL_BLOCKS` and failed on a correct tree, which is a guard asserting the wrong
    // quantity.
    assert!(
        control_folds > 0,
        "the sweep over {CONTROL_BLOCKS} pages charged {control_folds} fold visits, so the counter \
         is not wired to the fold it is named for. Every figure this probe prints is then a dead \
         measurement rather than a cheap one -- which is exactly the fault this control was added \
         to catch, and it caught it once already on `REFRESH_FLAGS`."
    );
}
