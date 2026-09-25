// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! SCOPING THE ROUND'S SUMMARY WALK TO THE DIRTY SET: WHAT HAS TO BE TRUE FIRST, AND IS NOT.
//!
//! mx#1938 priced the obvious fix for the empty round -- scope `bucket_storage_summaries` to the
//! dirty bucket set -- and declined it for a named reason:
//!
//! > The walk credits each page to `entry.address.routing_bucket()` with a hash fallback over the
//! > SHARD'S routing range, while `upsert_bucket_index_block_inner` files that same page under a
//! > fallback over `0..u32::MAX`, so a page whose address carries no explicit routing bucket can
//! > be filed under one bucket and summarised under another. Measured on this fixture the branch
//! > does not fire -- zero unrouted pages and zero misfiled pages at both sizes -- but "does not
//! > fire on this fixture" is not the same statement as "cannot fire".
//!
//! This file answers that, and the answer has three parts.
//!
//! # 1. THAT ZERO WAS AN IDENTITY, NOT A MEASUREMENT
//!
//! mx#1938 and mx#1934 both build their shard with `engine.load_shard(1)`, which loads it on
//! `start_routing_bucket = 0`, `end_routing_bucket = u32::MAX`. On THAT shard the two fallbacks
//! are not two placements that happened to agree: they are the same function applied to the same
//! three arguments. `block_routing_bucket(key, 0, u32::MAX)` is `block_routing_bucket(key, start,
//! end)` with `start = 0` and `end = u32::MAX` substituted. No page could have been misfiled, and
//! no fixture of that shape can ever report anything but zero.
//!
//! `a_wide_range_fixture_cannot_tell_the_two_placements_apart` asserts exactly that: on the wide
//! shard the misfiled count is 0 AND the two placement functions agree on every key, so the zero
//! is attributable to the identity rather than to the engine. The same assertion on a shard loaded
//! with the production narrow range -- `TS_SHARD_END_ROUTING_SLOT=1023`, the setting that cuts
//! resident memory 45% -- reports the two placements disagreeing on 2,000 of 2,000 keys.
//!
//! # 2. THE PAGE CAN BE CONSTRUCTED, AND HERE IT IS
//!
//! A page reaches the disagreeing state when its address carries no routing bucket. The door is
//! the engine's own decoder, not a test: `BlockAddressWire::routing_bucket` is `Option<u32>` under
//! `#[serde(default)]`, in a struct whose sibling aliases (`routing_slot`, `page_segment_id`,
//! `extent_id`) exist precisely because older on-disk spellings still load, and whose doc comment
//! says so -- "old to new is safe". `the_engines_own_decoder_produces_an_address_with_no_routing_bucket`
//! feeds the decoder an address record with the field absent and gets `None` back.
//!
//! Put pages in that state into a narrow shard and run the reconstruct the WAL-replay tail runs --
//! `rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX)`, which is `lifecycle.rs`'s own call
//! with `lifecycle.rs`'s own arguments -- and every one of them is filed under a bucket the
//! summary walk does not credit it to. Not a hypothetical: a count, at a stated denominator.
//!
//! # 3. WHAT THAT DOES TO A SCOPED WALK, MEASURED IN BOTH DIRECTIONS
//!
//! A scoped walk cannot start from the live page set -- materialising that IS the cost being
//! removed. It has to enter at `bucket_map[dirty bucket]`, which makes the precondition a
//! statement about where a page is FILED, not about where it is summarised:
//!
//! > EVERY LIVE PAGE OF A DIRTY OBJECT MUST BE FILED IN A BUCKET THE DIRTY SET NAMES.
//!
//! The dirty set is keyed by `block_routing_bucket(key, start, end)` at both of the two sites that
//! mark an object dirty. The filing is decided by `address.routing_bucket()` with a `0..u32::MAX`
//! fallback -- at `upsert_bucket_index_block_inner`, and, WHEN THIS FILE WAS WRITTEN, at NINE of
//! the twenty production call sites that rebuild the bucket index. So the precondition failed for
//! every page that reached one of those without a routing bucket of its own, and it failed
//! SILENTLY: the page is still readable, still summarised, still counted. Only a walk that entered
//! through the dirty set would miss it.
//!
//! SIX OF THOSE NINE NOW PASS THE SHARD'S OWN RANGE. THREE ARE CORRECT AS WRITTEN, for two
//! different reasons, and neither reason is visible from the call site alone:
//!
//!   * TWO run `rebuild_bucket_block_ownership` over a DECODED MANIFEST INDEX -- a whole-shard
//!     image written by whatever range the SOURCE shard ran on. That rebuild FILTERS on the range
//!     it is given, so narrowing it there DELETES every page whose source bucket falls outside the
//!     installing shard's range. Driven, not argued: a cross-range restore read a record back as
//!     None.
//!   * ONE is a `#[cfg(test)]` helper on a shard it publishes as `0..u32::MAX`, for which the whole
//!     range IS the shard's range.
//!
//! `the_three_rebuild_call_sites_still_on_the_whole_range_are_each_right_to_be` holds the list, and
//! `engine/tests/bucket_filing_range.rs` measures what the change did, what had to be true before
//! it could be made, and what the filter costs when it is pointed at a foreign image.
//! `upsert_bucket_index_block_inner`'s own hard-coded fallback is untouched and is a separate
//! class: it takes no range argument at all, so no call site can correct it.
//!
//! THERE IS A THIRD PLACEMENT RULE, and it is the one thing here that is already safe. A RELEASED
//! bucket holds no entry in `bucket_map` at all; `collect_live_block_entries` supplements it from
//! the model maps, keeping only pages whose address carries an explicit routing bucket equal to
//! the bucket's own, with no hash fallback at all. A scoped walk opening `bucket_map[b]` for a
//! released `b` would find nothing. What stops that mattering is a precondition on the other
//! side: `release_bucket_blocks` refuses a bucket that is dirty, so a released bucket is never in
//! the dirty set. `assert_no_released_bucket_is_dirty` asserts it at every checkpoint in this
//! file -- it began life as a NOT-EXERCISED assertion and went red on the first run, because a
//! dumping round releases buckets and this fixture reaches eight of them.
//!
//! It does not un-fail either. `upsert_bucket_index_block_inner` stages its computed bucket into
//! the WAL outcome item and the index-log item, and `WalOutcomeItem::resolved_address` and
//! `IndexLogItem::restore_address_repeats` stamp that value back onto the address on every replay.
//! The fallback fires once; the bucket it chose is then explicit for ever.
//!
//! Both halves are counted separately -- a page a scoped walk cannot REACH, and a page it would
//! reach but CREDIT elsewhere -- because they are different defects. And both directions of the
//! set comparison are asserted separately, because only one of them loses data:
//!
//! ```text
//!   a dirty bucket the scoped walk MISSES   -- the page is never dumped. Silent.
//!   a clean bucket the scoped walk SKIPS    -- the saving. Waste avoided.
//! ```
//!
//! # WHAT THIS CHANGES: NOTHING, DELIBERATELY
//!
//! No engine code moves here. The scoped walk lives in this file as a measurement, so the saving
//! can be priced and the precondition can be watched, and `scoping_is_exact_only_while_every_page_carries_the_shards_own_placement`
//! is the guard that says when it becomes safe to move.
//!
//! # WHAT IT SAVES, AND WHAT IT DOES NOT
//!
//! mx#1938 wrote that scoping `bucket_storage_summaries` "would take 2,889,724 entries off the
//! empty round at 100,000 records". That is the WHOLE round's live-page cost, and this walk is one
//! of nine sites in five modules that make it up -- the same nine, by file and line, that the
//! measurement below charges. Measured directly rather than inherited:
//!
//! ```text
//!                             store 20,000   store 100,000    ratio   per record of STORE
//!   the empty ROUND               497,908       2,488,501    4.998x     24.895 -> 24.885
//!   THIS walk within it            79,582         397,696    4.997x      3.979 ->  3.977
//!   share of the round               16.0%           16.0%
//! ```
//!
//! Scoping this walk is SIXTEEN PER CENT of the empty round, not all of it. The 397,696 charged to
//! `storage_reporting.rs:171` reproduces mx#1938's 398,456 for the same site to within 0.2%, on a
//! shard loaded with a different routing range -- so the attribution is the same measurement, and
//! only the claim built on top of it moves.
//!
//! AND THE INTEGRAL AS PUBLISHED IS A THOUSAND TIMES TOO LARGE. mx#1938 gives it as "about
//! 28.9 N^2 / 2k -- 1.445e12 at a million records with a round every 10,000". The constant is
//! right (28.9 there, 24.9 here) and the formula is right, but `N^2 / 2k` at `N = 1e6`, `k = 1e4`
//! is 5e7, so 28.9 of them is **1.445e9**. Asserted below with mx#1938's own constant so a reader
//! quoting the exponent fails here.
//!
//! # THE RESTART CASE
//!
//! mx#1938 flagged that dump SELECTION does not survive a restart, and that this binds anyone
//! driving more of the round from the dirty set. It binds hard. After a restart the dirty set is
//! empty, so a walk scoped to it produces the EMPTY set where the full walk produces every bucket
//! in the store. That is the losing direction at full width: not one bucket missed, all of them.
//!
//! WHY it is empty is worth stating exactly, because mx#1938 attributes it to "the
//! clear-dirty-on-load contract" and that contract is about something else. `load_shard_with`
//! does clear every BUCKET and PAGE dirty flag. It does not clear `shard.dirty_objects`, and
//! nothing does: `dirty_objects.clear()` has ZERO production call sites in this crate -- two
//! tests, and nothing else. The index is empty after a load because it is `#[serde(skip)]` on
//! `ShardState`, so a shard decoded from a served index arrives with `DirtyObjectIndex::default()`
//! and there is no clear to perform. A mutant that empties `clear()` therefore survives every
//! test here, which is reported below rather than dressed up: it cannot reach the subject.
//! `a_round_immediately_after_a_restart_would_see_no_buckets_at_all` drives the restart and
//! asserts it, so the answer to "walk everything or prove it need not" is recorded as WALK
//! EVERYTHING, with the reason measured rather than argued.

use super::*;
use crate::engine::hashing::block_routing_bucket;
use crate::engine::reports::{StorageManagerCycleReport, StorageManagerCycleRequest};
use crate::engine::storage_bucket_internals::{
    collect_live_block_entries, rebuild_bucket_first_index, refresh_bucket_runtime_flags,
};
use crate::engine::storage_reporting::bucket_storage_summaries;
use std::collections::{BTreeMap, BTreeSet};

/// The end bucket a production shard is loaded with. `TS_SHARD_END_ROUTING_SLOT=1023` is the
/// setting that cuts resident memory 45%, and 1023 is the value it sets.
const NARROW_END_BUCKET: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses, which is what mx#1934 and mx#1938 measured on.
const WIDE_END_BUCKET: u32 = u32::MAX;

/// Corpus sizes, the same two mx#1934 and mx#1938 used, so the figures here sit beside theirs.
const SMALL: usize = 20_000;
const LARGE: usize = 100_000;

/// Records written per batch. A write BATCH is one log record however many objects it carries.
const SEED_BATCH: usize = 500;

/// Low enough that the round dumps on every call, which is what drains the dirty set.
const DUMPING_MIN_UNDUMPED_WAL_RECORDS: u64 = 1;

// ---------------------------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------------------------

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

/// A shard loaded on an explicit routing range, which is the whole point: `load_shard` loads on
/// `0..u32::MAX` and that range is what makes the two placements identical.
fn load_on_range(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "walk-scope".to_string(),
        shard_uri: "local://walk-scope/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{end_routing_bucket}: {:?}",
        response.status
    );
}

/// Write `count` records from `from`, returning the keys, so the caller holds its OWN list of what
/// is outstanding. That list is the witness the engine's dirty set is compared against, and it
/// shares no state with `shard.dirty_objects`.
fn seed(engine: &TemporalEngine, from: usize, count: usize) -> Vec<String> {
    let mut written = Vec::with_capacity(count);
    let mut index = from;
    let to = from + count;
    while index < to {
        let end = (index + SEED_BATCH).min(to);
        let mut commands = Vec::new();
        for cursor in index..end {
            let key = format!("k-{cursor:08}");
            commands.push(Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 128],
            });
            written.push(key);
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
    written
}

fn round_request(min_undumped_wal_records: u64) -> StorageManagerCycleRequest {
    StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: true,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records,
        min_undumped_wal_bytes: 96 * 1024 * 1024,
        eviction_memory_pressure_threshold: 1,
        eviction_batch_limit: 4,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        index_gc_index_log_bytes_threshold: 1,
        ..StorageManagerCycleRequest::default()
    }
}

fn one_round(engine: &TemporalEngine) -> StorageManagerCycleReport {
    engine.run_storage_manager_cycle(round_request(DUMPING_MIN_UNDUMPED_WAL_RECORDS))
}

/// The shard's own routing range, read from the authority the engine reads it from.
fn routing_range(engine: &TemporalEngine) -> (u32, u32) {
    engine
        .infos
        .read()
        .expect("info lock poisoned")
        .get(&1)
        .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
        .unwrap_or((0, u32::MAX))
}

// ---------------------------------------------------------------------------------------------
// The old-format door, and the two placements
// ---------------------------------------------------------------------------------------------

/// An address as an index written before the routing-bucket field carried it holds one.
///
/// Built by the engine's OWN serde impl: serialize the address to its wire shape, drop the `rs`
/// key, and hand it back to the decoder. `#[serde(default)]` on `BlockAddressWire::routing_bucket`
/// is what answers, and it is the same code path that reads a served index off disk --
/// `decode_index_bytes` takes plain JSON explicitly, "the served index as an older build wrote it".
///
/// Deliberately NOT `set_routing_bucket(None)`: a test that reaches for the setter proves only
/// that the setter works. This proves the DECODER produces the state.
fn as_an_older_build_wrote_it(address: &BlockAddress) -> BlockAddress {
    let mut wire = serde_json::to_value(address).expect("an address serializes to its wire shape");
    let object = wire
        .as_object_mut()
        .expect("the address wire shape is a JSON object");
    let removed = object.remove("rs");
    assert!(
        removed.is_some(),
        "the address wire shape carried no `rs` key to remove, so this helper is a no-op and \
         every count taken through it is zero for the wrong reason: {object:?}"
    );
    serde_json::from_value(wire).expect("the engine's decoder accepts an address with no `rs`")
}

/// Where the summary walk credits a page: the address first, the SHARD'S range as the fallback.
/// This mirrors `bucket_storage_summaries`' live-page loop exactly.
fn summarised_under(entry: &crate::engine::storage_bucket_internals::LiveBlockEntry, start: u32, end: u32) -> u32 {
    entry
        .address
        .routing_bucket()
        .unwrap_or_else(|| block_routing_bucket(&entry.object_key, start, end))
}

/// Where the dirty set files an object: always the shard's own range, never the address.
/// This mirrors `mark_async_dirty_object` and the synchronous mark in `engine.rs`.
fn dirty_set_would_file(object_key: &str, start: u32, end: u32) -> u32 {
    block_routing_bucket(object_key, start, end)
}

/// The buckets `bucket_map` actually holds pages in -- where the index FILED them.
fn filed_buckets(shard: &crate::engine::state::ShardState) -> BTreeSet<u32> {
    shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_, bucket)| !bucket.block_index.is_empty())
        .map(|(routing_bucket, _)| *routing_bucket)
        .collect()
}

/// The buckets the summary walk credits at least one live page to.
fn summarised_buckets(shard: &crate::engine::state::ShardState, start: u32, end: u32) -> BTreeSet<u32> {
    bucket_storage_summaries(shard, start, end)
        .into_iter()
        .filter(|summary| summary.block_ref_count > 0)
        .map(|summary| summary.routing_bucket)
        .collect()
}

// ---------------------------------------------------------------------------------------------
// The scoped walk, as a MEASUREMENT. No engine code moves.
// ---------------------------------------------------------------------------------------------

/// What `bucket_storage_summaries`' live-page half would produce if it visited only the pages of
/// the dirty buckets, and how many pages it visited to produce it.
///
/// Scoped the only way a scoped walk can be: straight into `bucket_map` at the dirty bucket ids,
/// never materialising the rest of the store. The page is still CREDITED the way the full walk
/// credits it, so the two can differ only in which pages were looked at.
fn scoped_live_page_summaries(
    shard: &crate::engine::state::ShardState,
    start: u32,
    end: u32,
) -> (BTreeMap<u32, u64>, u64) {
    let dirty: Vec<u32> = shard.dirty_objects.bucket_ids().collect();
    let mut credited = BTreeMap::<u32, u64>::new();
    let mut visits = 0u64;
    for routing_bucket in dirty {
        let Some(bucket) = shard.bucket_index.bucket_map.get(&routing_bucket) else {
            continue;
        };
        for page in bucket.block_index.values() {
            visits += 1;
            let credited_to = page
                .address
                .routing_bucket()
                .unwrap_or_else(|| block_routing_bucket(&page.object_key, start, end));
            *credited.entry(credited_to).or_default() += 1;
        }
    }
    (credited, visits)
}

/// What the full walk produces, as the same shape, so the two are comparable element by element.
fn full_live_page_summaries(
    shard: &crate::engine::state::ShardState,
    start: u32,
    end: u32,
) -> BTreeMap<u32, u64> {
    let mut credited = BTreeMap::<u32, u64>::new();
    for summary in bucket_storage_summaries(shard, start, end) {
        if summary.block_ref_count > 0 {
            credited.insert(summary.routing_bucket, summary.block_ref_count);
        }
    }
    credited
}

/// A RELEASED bucket is a THIRD placement rule, and the one thing that keeps it out of the way.
///
/// A released bucket holds no page entries in `bucket_map` at all -- `collect_live_block_entries`
/// supplements it from the model maps, keeping only pages whose address carries an explicit
/// routing bucket equal to the bucket's own, with NO hash fallback. A walk scoped to
/// `bucket_map[dirty bucket]` would therefore find NOTHING for a released bucket.
///
/// What makes that safe is a precondition on the other side: `release_bucket_blocks` refuses any
/// bucket that is dirty. So a released bucket is never in the dirty set and a scoped walk never
/// tries to enter one. That is the invariant, and it is asserted rather than assumed -- this
/// started life as a NOT-EXERCISED assertion and went red on the first run, because a dumping
/// round releases buckets. The count is reported so a release that stopped happening shows up as
/// a denominator that collapsed rather than as a clean pass.
fn assert_no_released_bucket_is_dirty(shard: &crate::engine::state::ShardState) -> usize {
    let released = &shard.bucket_index.released_buckets;
    let dirty_and_released: Vec<u32> = shard
        .dirty_objects
        .bucket_ids()
        .filter(|routing_bucket| released.contains(routing_bucket))
        .collect();
    assert!(
        dirty_and_released.is_empty(),
        "{} buckets are BOTH dirty and released: {:?}. A released bucket holds no entry in \
         `bucket_map`, so a walk scoped to the dirty set opens it and finds nothing -- and \
         `release_bucket_blocks` is supposed to refuse a dirty bucket precisely so that cannot \
         happen. Every scoped-walk figure in this file rests on this.",
        dirty_and_released.len(),
        &dirty_and_released[..dirty_and_released.len().min(5)]
    );
    released.len()
}

// =============================================================================================
// 1. The two placements, and why a wide fixture cannot see the difference
// =============================================================================================

/// THE TWO FALLBACK RANGES ARE TWO DIFFERENT FUNCTIONS, ON EVERY KEY A SHARD HOLDS.
///
/// Stated as arithmetic before any engine runs, because it is arithmetic: `bucket_for_object`
/// reduces the key's hash modulo the bucket COUNT and adds the start, so narrowing the range from
/// 2^32 buckets to 1,024 changes the answer for every key whose hash exceeds 1,024 -- which is
/// every key, at a 64-bit hash.
///
/// rust-internal: arithmetic on the engine's own routing-bucket hash, no product behaviour
#[test]
fn the_two_fallback_ranges_are_different_functions_of_the_same_key() {
    const KEYS: usize = 2_000;
    let keys: Vec<String> = (0..KEYS).map(|index| format!("k-{index:08}")).collect();
    assert_eq!(keys.len(), KEYS, "the fixture built no keys to compare");

    let narrow_disagreements = keys
        .iter()
        .filter(|key| {
            block_routing_bucket(key, 0, NARROW_END_BUCKET)
                != block_routing_bucket(key, 0, WIDE_END_BUCKET)
        })
        .count();
    assert_eq!(
        narrow_disagreements, KEYS,
        "on a shard loaded with the production narrow range, `block_routing_bucket(key, start, \
         end)` and `block_routing_bucket(key, 0, u32::MAX)` place {narrow_disagreements} of \
         {KEYS} keys in different buckets. Anything other than all of them means this fixture's \
         keys are not what a shard holds and every count below is drawn from the wrong population."
    );

    // The control: the same comparison on the range mx#1934 and mx#1938 measured on.
    let wide_disagreements = keys
        .iter()
        .filter(|key| {
            block_routing_bucket(key, 0, WIDE_END_BUCKET)
                != block_routing_bucket(key, 0, WIDE_END_BUCKET)
        })
        .count();
    assert_eq!(
        wide_disagreements, 0,
        "on `0..u32::MAX` the two expressions are the same function of the same arguments, so a \
         disagreement here would mean `block_routing_bucket` is not deterministic"
    );
    println!(
        "  placements disagree on {narrow_disagreements}/{KEYS} keys at end={NARROW_END_BUCKET}, \
         {wide_disagreements}/{KEYS} at end=u32::MAX"
    );
}

/// THE FIXTURE mx#1938 MEASURED ON COULD NOT HAVE REPORTED ANYTHING BUT ZERO.
///
/// Its shard is loaded by `engine.load_shard(1)`, which is `0..u32::MAX`. The two placements are
/// then the same expression, so "zero misfiled pages at both sizes" is an identity. This asserts
/// BOTH halves -- the zero, and the identity that explains it -- so the reading is attributable.
///
/// rust-internal: prices the engine's own bucket placement, no product behaviour
#[test]
fn a_wide_range_fixture_cannot_tell_the_two_placements_apart() {
    const RECORDS: usize = 800;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    engine.load_shard(1);

    let (start, end) = routing_range(&engine);
    assert_eq!(
        (start, end),
        (0, WIDE_END_BUCKET),
        "`load_shard` is supposed to load on the whole range; this test's whole point is that it \
         does, so a change here invalidates the reading rather than the code"
    );

    let keys = seed(&engine, 0, RECORDS);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let _released = assert_no_released_bucket_is_dirty(shard);

    let entries = collect_live_block_entries(shard);
    assert!(
        !entries.is_empty(),
        "the fixture stored no live page, so the misfiled count below is zero because there is \
         nothing to misfile"
    );

    let misfiled = entries
        .iter()
        .filter(|entry| {
            summarised_under(entry, start, end)
                != block_routing_bucket(&entry.object_key, 0, WIDE_END_BUCKET)
        })
        .count();
    assert_eq!(
        misfiled, 0,
        "{misfiled} of {} live pages on the WIDE shard are summarised somewhere other than the \
         `0..u32::MAX` placement",
        entries.len()
    );

    // And now the half that says what that zero is worth.
    let placements_differ = keys
        .iter()
        .filter(|key| {
            block_routing_bucket(key, start, end) != block_routing_bucket(key, 0, WIDE_END_BUCKET)
        })
        .count();
    assert_eq!(
        placements_differ, 0,
        "on this shard the two placements are the same function of the same arguments, so the \
         zero above measures the substitution and not the engine. If this is ever non-zero the \
         zero above has become a real reading and this comment is wrong."
    );
    println!(
        "  WIDE shard: {} live pages, {misfiled} misfiled, and the two placements agree on \
         {}/{} keys -- the zero is an identity",
        entries.len(),
        keys.len() - placements_differ,
        keys.len()
    );
}

// =============================================================================================
// 2. The construction
// =============================================================================================

/// THE DOOR: THE ENGINE'S OWN DECODER PRODUCES AN ADDRESS WITH NO ROUTING BUCKET.
///
/// Two ways in, both the engine's, neither a setter: a hand-written record in the wire shape an
/// older build wrote, and a round trip of a real address with the field dropped.
///
/// rust-internal: reads the engine's own index wire shape, no product behaviour
#[test]
fn the_engines_own_decoder_produces_an_address_with_no_routing_bucket() {
    // As an older build wrote it: the three required fields, and nothing optional.
    let older: BlockAddress = serde_json::from_slice(br#"{"ps":7,"o":128,"l":64}"#)
        .expect("the decoder takes an address record with no optional fields");
    assert_eq!(
        older.routing_bucket(),
        None,
        "`BlockAddressWire::routing_bucket` is `Option<u32>` under `#[serde(default)]`, so a \
         record written before the field existed has to decode to None. It did not."
    );
    assert_eq!(older.block_slab_id, 7, "the rest of the record still decoded");
    assert_eq!(older.length(), 64, "the rest of the record still decoded");

    // And a REAL address, round-tripped through the same shape with the key removed.
    let current = BlockAddress::from_parts(3, 64, 128, Some(1), Some(2), Some(909));
    assert_eq!(
        current.routing_bucket(),
        Some(909),
        "the fixture address has to carry a routing bucket for dropping it to mean anything"
    );
    let older = as_an_older_build_wrote_it(&current);
    assert_eq!(
        older.routing_bucket(),
        None,
        "dropping `rs` from the wire shape has to produce an address with no routing bucket"
    );
    assert_eq!(
        (older.block_slab_id, older.offset, older.length(), older.object_id()),
        (3, 64, 128, Some(2)),
        "dropping `rs` must drop ONLY the routing bucket; anything else changed makes the \
         construction below a different experiment"
    );
}

/// A PAGE FILED UNDER ONE BUCKET AND SUMMARISED UNDER ANOTHER, CONSTRUCTED.
///
/// The reconstruct is the production one, with the production arguments: `lifecycle.rs`'s WAL
/// replay tail and `persistence.rs`'s bulk-ingest flush both call
/// `rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX)` and then persist what it produced.
/// The shard is loaded on the production narrow range. The only thing the fixture supplies is
/// addresses in the state the engine's own decoder produces.
///
/// SECOND ARM, SAME FIXTURE: the same reconstruct with the shard's OWN range, which is the
/// reconciliation. It agrees. So the mismatch is not a property of unrouted pages -- it is a
/// property of the argument those five call sites pass.
///
/// rust-internal: reads the engine's own reconstruct path, no product behaviour
#[test]
fn the_reconstruct_the_replay_tail_runs_files_a_page_where_the_summary_walk_does_not_look() {
    const RECORDS: usize = 600;

    for (label, rebuild_end, expect_disagreement) in [
        ("as production calls it (0..u32::MAX)", WIDE_END_BUCKET, true),
        ("with the shard's own range (0..1023)", NARROW_END_BUCKET, false),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on_range(&engine, NARROW_END_BUCKET);
        seed(&engine, 0, RECORDS);

        let (start, end) = routing_range(&engine);
        assert_eq!(
            (start, end),
            (0, NARROW_END_BUCKET),
            "{label}: the shard did not load on the narrow range, so this measures the identity"
        );

        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1");
        let _released = assert_no_released_bucket_is_dirty(shard);

        let pages_before = collect_live_block_entries(shard).len();
        assert!(
            pages_before > 0,
            "{label}: the fixture stored no page, so there is nothing to file anywhere"
        );

        // Put the model-map addresses into the state an older index decodes into. The bucket index
        // is rebuilt FROM these below, which is what a replay tail and a bulk flush both do.
        let keys: Vec<String> = shard.strings.keys().cloned().collect();
        assert!(
            !keys.is_empty(),
            "{label}: the string model map is empty, so the reconstruct has no source"
        );
        for key in &keys {
            let older = as_an_older_build_wrote_it(shard.strings.get(key).expect("key present"));
            shard.strings.insert(key.clone(), older);
        }

        // THE PRODUCTION RECONSTRUCT, with the argument under test.
        rebuild_bucket_first_index(1, shard, 0, rebuild_end);
        refresh_bucket_runtime_flags(shard);

        let entries = collect_live_block_entries(shard);
        assert_eq!(
            entries.len(),
            pages_before,
            "{label}: the reconstruct changed the page count ({pages_before} -> {}), so the two \
             arms are not comparing the same store",
            entries.len()
        );
        // DENOMINATOR: the addresses are still unrouted after the reconstruct. It files a page
        // under the bucket its ARGUMENTS name and stamps only the object id onto the address, so
        // the fallback is what decided the filing and the fallback is what this measures. A run
        // where the reconstruct had started stamping the bucket would make every count below zero
        // for a reason that has nothing to do with the two ranges.
        let unrouted = entries
            .iter()
            .filter(|entry| entry.address.routing_bucket().is_none())
            .count();
        assert_eq!(
            unrouted,
            entries.len(),
            "{label}: {unrouted} of {} pages came out of the reconstruct without a routing \
             bucket, and all of them should have. `rebuild_bucket_first_index` calls \
             `set_object_id` on the address it files and nothing else, so an address that arrived \
             unrouted stays unrouted and the FILING is where the two ranges part company.",
            entries.len()
        );

        let filed = filed_buckets(shard);
        let summarised = summarised_buckets(shard, start, end);
        let only_filed: Vec<u32> = filed.difference(&summarised).copied().collect();
        let only_summarised: Vec<u32> = summarised.difference(&filed).copied().collect();

        // How many PAGES are credited to a bucket other than the one they are filed in.
        let mut misplaced = 0usize;
        for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
            for page in bucket.block_index.values() {
                let credited = page
                    .address
                    .routing_bucket()
                    .unwrap_or_else(|| block_routing_bucket(&page.object_key, start, end));
                if credited != *routing_bucket {
                    misplaced += 1;
                }
            }
        }
        println!(
            "  {label}: {} pages, {misplaced} filed under a bucket they are not summarised \
             under; only-filed {} buckets, only-summarised {} buckets",
            entries.len(),
            only_filed.len(),
            only_summarised.len()
        );

        if expect_disagreement {
            assert_eq!(
                misplaced,
                entries.len(),
                "the reconstruct the replay tail runs places every unrouted page by \
                 `block_routing_bucket(key, 0, u32::MAX)`, and the summary walk credits it by \
                 `block_routing_bucket(key, 0, {NARROW_END_BUCKET})`. Those are different \
                 functions of the same key (asserted separately), so all {} pages have to be \
                 misplaced and {misplaced} were. mx#1938 measured zero of these because its \
                 shard made the two functions identical.",
                entries.len()
            );
            assert!(
                !only_filed.is_empty(),
                "at least one bucket must hold pages nothing summarises; a dump naming the \
                 bucket the summaries DO name carries none of that bucket's slabs"
            );
        } else {
            assert_eq!(
                misplaced, 0,
                "with the shard's own range passed to the SAME reconstruct, every page is filed \
                 where it is summarised. This is the reconciliation, and it is one argument."
            );
            assert!(
                only_filed.is_empty() && only_summarised.is_empty(),
                "reconciled, the filed and summarised bucket sets must be equal; only-filed \
                 {only_filed:?}, only-summarised {only_summarised:?}"
            );
        }
    }
}

// =============================================================================================
// 3. What that does to a scoped walk
// =============================================================================================

/// THE PRECONDITION A SCOPED WALK NEEDS, ASSERTED ON THE LIVE WRITE PATH AND BROKEN ON THE OTHER.
///
/// > Every live page of a dirty object must be summarised under a bucket the dirty set names.
///
/// Arm one is the live write path on a narrow shard: it holds, with a denominator. Arm two is the
/// same shard after the reconstruct above: it fails for every page. Both are counted rather than
/// asserted as booleans, because "it holds" and "there was nothing to hold it for" read alike.
///
/// rust-internal: reads the engine's own dirty index, no product behaviour
#[test]
fn scoping_is_exact_only_while_every_page_carries_the_shards_own_placement() {
    const RECORDS: usize = 600;

    for (label, break_it) in [("live write path", false), ("after the replay reconstruct", true)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on_range(&engine, NARROW_END_BUCKET);
        let written = seed(&engine, 0, RECORDS);
        let (start, end) = routing_range(&engine);

        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1");
        let _released = assert_no_released_bucket_is_dirty(shard);

        if break_it {
            let keys: Vec<String> = shard.strings.keys().cloned().collect();
            for key in &keys {
                let older = as_an_older_build_wrote_it(shard.strings.get(key).expect("key"));
                shard.strings.insert(key.clone(), older);
            }
            rebuild_bucket_first_index(1, shard, 0, WIDE_END_BUCKET);
            refresh_bucket_runtime_flags(shard);
        }

        // The witness: the fixture's own keys, hashed with the shard's own range. It shares no
        // state with `shard.dirty_objects` -- comparing the dirty set against a re-derivation of
        // itself would pass however wrong both were.
        let witness: BTreeSet<u32> = written
            .iter()
            .map(|key| dirty_set_would_file(key, start, end))
            .collect();
        let engine_dirty: BTreeSet<u32> = shard.dirty_objects.bucket_ids().collect();
        assert!(
            !witness.is_empty(),
            "{label}: the witness names no bucket, so every comparison below is vacuous"
        );
        assert_eq!(
            engine_dirty, witness,
            "{label}: the dirty set and a witness built outside the engine disagree; only-engine \
             {:?}, only-witness {:?}",
            engine_dirty.difference(&witness).take(5).collect::<Vec<_>>(),
            witness.difference(&engine_dirty).take(5).collect::<Vec<_>>(),
        );

        let entries = collect_live_block_entries(shard);
        let dirty_pages = entries
            .iter()
            .filter(|entry| shard.dirty_objects.contains(&entry.object_key))
            .count();
        assert!(
            dirty_pages > 0,
            "{label}: no live page belongs to a dirty object, so the precondition below holds \
             over an empty set"
        );

        // HALF ONE -- CAN THE SCOPED WALK REACH THE PAGE AT ALL? It enters at
        // `bucket_map[dirty bucket]`, so a page of a dirty object FILED anywhere else is a page
        // the scoped walk never looks at. This is the half that loses data.
        let mut unreachable = 0usize;
        let mut miscredited = 0usize;
        for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
            for page in bucket.block_index.values() {
                if !shard.dirty_objects.contains(&page.object_key) {
                    continue;
                }
                if !witness.contains(routing_bucket) {
                    unreachable += 1;
                }
                // HALF TWO -- WOULD IT CREDIT THE PAGE TO THE RIGHT BUCKET? Asserted separately,
                // because a page that is reachable and credited elsewhere is a different defect
                // from one that cannot be reached.
                let credited = page
                    .address
                    .routing_bucket()
                    .unwrap_or_else(|| block_routing_bucket(&page.object_key, start, end));
                if !witness.contains(&credited) {
                    miscredited += 1;
                }
            }
        }
        println!(
            "  {label}: {dirty_pages} pages of dirty objects, {unreachable} filed where a scoped \
             walk never looks, {miscredited} credited to a bucket the dirty set does not name"
        );

        if break_it {
            assert_eq!(
                unreachable, dirty_pages,
                "after the reconstruct every page is FILED by `block_routing_bucket(key, 0, \
                 u32::MAX)` while the dirty set still keys on the shard's own range, so every \
                 page of a dirty object sits in a bucket a walk scoped to the dirty set never \
                 opens. The dump that names the dirty bucket then carries no slab for any of \
                 them. That is the losing direction, and it is all {dirty_pages} pages."
            );
            assert_eq!(
                miscredited, 0,
                "the pages are still unrouted, so the summary walk's own fallback would credit \
                 them to the shard's range and get the right bucket. It is the FILING that moved, \
                 which is why this half is counted separately from the half above."
            );
        } else {
            assert_eq!(
                (unreachable, miscredited),
                (0, 0),
                "on the live write path every address carries `block_routing_bucket(key, start, \
                 end)`, which is what the dirty set keys on, so both halves hold for all \
                 {dirty_pages} pages"
            );
        }
    }
}

/// THE SET THE SCOPED WALK PRODUCES, ELEMENT BY ELEMENT AGAINST THE FULL WALK AND A WITNESS.
///
/// Five points in a write/dump sequence. The two differences are asserted SEPARATELY and with
/// different messages, because only one of them loses data:
///
/// * a DIRTY bucket the full walk has and the scoped walk does not -- the page is never dumped;
/// * a CLEAN bucket the full walk has and the scoped walk does not -- the saving.
///
/// rust-internal: prices the engine's own summary walk, no product behaviour
#[test]
fn the_scoped_walk_equals_the_full_walk_on_every_bucket_the_dirty_set_names() {
    const STORE: usize = 2_000;
    const BACKLOG: usize = 250;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on_range(&engine, NARROW_END_BUCKET);
    let (start, end) = routing_range(&engine);

    let mut outstanding: Vec<String> = Vec::new();
    let mut saved_total = 0u64;
    let mut checkpoints = 0usize;

    fn check(
        engine: &TemporalEngine,
        label: &str,
        outstanding: &[String],
        expect_empty: bool,
        start: u32,
        end: u32,
    ) -> u64 {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        let _released = assert_no_released_bucket_is_dirty(shard);

        let witness: BTreeSet<u32> = outstanding
            .iter()
            .map(|key| dirty_set_would_file(key, start, end))
            .collect();
        let engine_dirty: BTreeSet<u32> = shard.dirty_objects.bucket_ids().collect();
        assert_eq!(
            engine_dirty, witness,
            "{label}: the engine's dirty bucket set and the witness built outside it disagree"
        );
        if expect_empty {
            assert!(
                witness.is_empty(),
                "{label}: this point is supposed to have nothing outstanding and the witness \
                 names {} buckets",
                witness.len()
            );
        }

        let full = full_live_page_summaries(shard, start, end);
        let (scoped, _visits) = scoped_live_page_summaries(shard, start, end);

        // DIRECTION ONE, THE ONE THAT LOSES DATA: a bucket the dirty set names where the two
        // walks do not agree page for page.
        let mut disagreeing: Vec<(u32, u64, u64)> = Vec::new();
        for routing_bucket in &witness {
            let full_count = full.get(routing_bucket).copied().unwrap_or(0);
            let scoped_count = scoped.get(routing_bucket).copied().unwrap_or(0);
            if full_count != scoped_count {
                disagreeing.push((*routing_bucket, full_count, scoped_count));
            }
        }
        assert!(
            disagreeing.is_empty(),
            "{label}: {} of {} buckets the dirty set names are summarised differently by the \
             scoped walk than by the full one (bucket, full, scoped): {:?}. Every one of these \
             is a dump that carries fewer pages than the bucket holds -- silent, and discovered \
             only when something looks for a record that is not there.",
            disagreeing.len(),
            witness.len(),
            &disagreeing[..disagreeing.len().min(5)]
        );

        // DIRECTION TWO, THE ONE THAT ONLY WASTES: buckets the full walk summarises and the
        // scoped walk never looks at. This is the saving, and it is reported, not feared.
        let skipped: Vec<u32> = full
            .keys()
            .filter(|routing_bucket| !scoped.contains_key(routing_bucket))
            .copied()
            .collect();
        let skipped_pages: u64 = skipped.iter().map(|b| full[b]).sum();
        for routing_bucket in &skipped {
            assert!(
                !witness.contains(routing_bucket),
                "{label}: bucket {routing_bucket} is in the dirty witness AND was skipped by the \
                 scoped walk; that is the losing direction and direction one should have caught it"
            );
        }

        // And nothing invented: a bucket the scoped walk credits that the full walk does not.
        let invented: Vec<u32> = scoped
            .keys()
            .filter(|routing_bucket| !full.contains_key(routing_bucket))
            .copied()
            .collect();
        assert!(
            invented.is_empty(),
            "{label}: the scoped walk credited {} buckets the full walk does not summarise at \
             all: {:?}",
            invented.len(),
            &invented[..invented.len().min(5)]
        );

        println!(
            "  {label:<34} witness {:>5}, full {:>5} buckets, scoped {:>5}, skipped {:>5} \
             buckets / {skipped_pages:>6} pages",
            witness.len(),
            full.len(),
            scoped.len(),
            skipped.len()
        );
        skipped_pages
    }

    saved_total += check(&engine, "1: fresh store", &outstanding, true, start, end);
    checkpoints += 1;

    outstanding.extend(seed(&engine, 0, STORE));
    saved_total += check(&engine, "2: written, no round", &outstanding, false, start, end);
    checkpoints += 1;

    let report = one_round(&engine);
    assert!(report.errors.is_empty(), "the settling round errored: {:?}", report.errors);
    let _ = one_round(&engine);
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let drained = shards.get(&1).expect("shard 1").dirty_objects.len();
        assert_eq!(
            drained, 0,
            "the settling rounds left {drained} dirty objects behind, so point 3 is not the \
             empty-backlog point it claims to be"
        );
    }
    outstanding.clear();
    saved_total += check(&engine, "3: after a dumping round", &outstanding, true, start, end);
    checkpoints += 1;

    outstanding.extend(seed(&engine, STORE, BACKLOG));
    saved_total += check(&engine, "4: small backlog, large store", &outstanding, false, start, end);
    checkpoints += 1;

    outstanding.extend(seed(&engine, STORE + BACKLOG, BACKLOG));
    saved_total += check(&engine, "5: backlog accumulated", &outstanding, false, start, end);
    checkpoints += 1;

    assert_eq!(checkpoints, 5, "the sequence was supposed to check five points");
    assert!(
        saved_total > 0,
        "across five points the scoped walk skipped no page at all, so the saving this file \
         exists to price is zero and every figure below is vacuous"
    );
    println!("  pages the scoped walk did not visit, summed over the five points: {saved_total}");
}

/// THE RESTART CASE, DRIVEN. WALK EVERYTHING.
///
/// mx#1938 established that dump selection does not survive a restart and flagged it as the
/// binding constraint on anyone driving more of the round from the dirty set. Driven here: after a
/// restart the dirty set is empty, so a walk scoped to it visits NOTHING while the full walk
/// summarises every bucket in the store. Not one bucket missed -- all of them.
///
/// So the answer to "walk everything after a restart, or prove it need not" is WALK EVERYTHING,
/// and this is the measurement that says so rather than the argument.
///
/// rust-internal: reads the engine's own load contract, no product behaviour
#[test]
fn a_round_immediately_after_a_restart_would_see_no_buckets_at_all() {
    const RECORDS: usize = 1_200;

    let dir = tempfile::tempdir().expect("tempdir");
    let (buckets_before, pages_before) = {
        let engine = engine_on(dir.path());
        load_on_range(&engine, NARROW_END_BUCKET);
        seed(&engine, 0, RECORDS);
        let (start, end) = routing_range(&engine);
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        let full = full_live_page_summaries(shard, start, end);
        let (_scoped, visits) = scoped_live_page_summaries(shard, start, end);
        assert!(
            visits > 0,
            "before the restart the scoped walk must visit something, or the after-figure is not \
             a change"
        );
        (full.len(), full.values().sum::<u64>())
    };
    assert!(
        buckets_before > 0 && pages_before > 0,
        "the fixture stored nothing before the restart ({buckets_before} buckets, \
         {pages_before} pages)"
    );

    // The restart: a new engine over the same directories, loaded on the same range.
    let engine = engine_on(dir.path());
    load_on_range(&engine, NARROW_END_BUCKET);
    let (start, end) = routing_range(&engine);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let _released = assert_no_released_bucket_is_dirty(shard);

    let dirty_after = shard.dirty_objects.len();
    assert_eq!(
        dirty_after, 0,
        "the clear-dirty-on-load contract says a load clears every flag, and the shard came back \
         holding {dirty_after} dirty objects. The rest of this test measures the consequence of \
         that contract and means nothing without it."
    );

    let full = full_live_page_summaries(shard, start, end);
    let (scoped, visits) = scoped_live_page_summaries(shard, start, end);
    assert!(
        !full.is_empty(),
        "the shard came back from the restart holding no live page, so the comparison below is \
         between two empty sets"
    );
    assert!(
        scoped.is_empty() && visits == 0,
        "a walk scoped to an empty dirty set has nothing to visit, and this one credited {} \
         buckets over {visits} visits",
        scoped.len()
    );
    println!(
        "  after the restart: full walk {} buckets / {} pages, scoped walk {} buckets / {visits} \
         visits -- the ENTIRE store is in the losing direction",
        full.len(),
        full.values().sum::<u64>(),
        scoped.len()
    );
}

// =============================================================================================
// 4. What it would save, at two sizes
// =============================================================================================

/// WHAT SCOPING THIS ONE WALK SAVES, COUNTED IN VISITS AT TWO CORPUS SIZES.
///
/// VISITS, not allocations: the scoped walk borrows out of `bucket_map` and materialises nothing,
/// and an allocation-based instrument is blind to exactly that. Counted at the two sizes mx#1934
/// and mx#1938 used so the figures sit beside theirs, on a shard with NOTHING outstanding -- the
/// round this file is named for.
///
/// THE RESIDUAL. The scoped walk is audited with an instrument it does not feed: the process-wide
/// live-page scan counter, charged inside `collect_live_block_entries` in another module. Around
/// a span holding only the scoped walk it reads 0 -- and a 0 cannot be told from an instrument
/// that is not connected, so the same span is then given a real whole-store walk to see, and has
/// to recover it exactly.
///
/// rust-internal: prices the engine's own maintenance walk, no product behaviour
#[test]
fn what_scoping_the_summary_walk_saves_at_two_corpus_sizes() {
    let mut rows: Vec<(usize, u64, u64, usize)> = Vec::new();

    for records in [SMALL, LARGE] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on_range(&engine, NARROW_END_BUCKET);
        seed(&engine, 0, records);
        // Settle, so the round measured has an EMPTY backlog: nothing to dump, nothing to retire.
        let _ = one_round(&engine);
        let _ = one_round(&engine);
        let (start, end) = routing_range(&engine);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        let _released = assert_no_released_bucket_is_dirty(shard);
        let outstanding = shard.dirty_objects.len();
        assert_eq!(
            outstanding, 0,
            "{records} records: the settling rounds left {outstanding} dirty objects, so this is \
             not the empty round it is reported as"
        );

        // ONE FULL WALK, and -- the point of taking it here -- the NAME the engine charges it
        // under. The per-site tally is keyed by `file:line` of the caller, so calling the walk on
        // its own with the tally cleared makes the key SELF-LOCATING: exactly one site is charged
        // and it is this walk's. Nothing below has to hard-code a line number that drifts.
        crate::engine::reset_live_block_scan_entries();
        crate::engine::reset_live_block_scan_sites();
        let full = full_live_page_summaries(shard, start, end);
        let one_walk = crate::engine::live_block_scan_entries();
        let charged = crate::engine::live_block_scan_sites_snapshot();
        assert_eq!(
            charged.len(),
            1,
            "{records} records: one call of `bucket_storage_summaries` charged {} calling sites, \
             so the key below is not this walk's: {:?}",
            charged.len(),
            charged.keys().collect::<Vec<_>>()
        );
        let summary_site = charged.keys().next().expect("exactly one site").clone();
        assert!(
            one_walk > 0,
            "{records} records: the full walk charged nothing, so the instrument is not connected"
        );

        // THE SCOPED WALK, and the residual around it.
        crate::engine::reset_live_block_scan_entries();
        let (scoped, scoped_visits) = scoped_live_page_summaries(shard, start, end);
        let residual = crate::engine::live_block_scan_entries();
        assert_eq!(
            residual, 0,
            "{records} records: the scoped walk charged {residual} entries to the engine's own \
             live-page counter. It reads `bucket_map` directly and should charge nothing."
        );
        // A 0 residual is the one reading indistinguishable from a disconnected instrument, so
        // plant a real whole-store walk inside the same span and recover it exactly.
        //
        // THE PLANT IS `one_walk`, NOT `collect_live_block_entries(...).len()`, and the difference
        // is a measurement in itself. A settled shard has RELEASED buckets, and a released bucket
        // holds no entry in `bucket_map`, so `collect_live_block_entries` walks the index AND then
        // re-walks the model maps to supplement them -- charging about twice the store to return
        // one entry per page. Comparing the charge against the returned length is comparing two
        // different quantities, and the first form of this assertion did exactly that (39,860
        // charged against 20,000 returned). `one_walk` was charged by the SAME call under the
        // SAME shard state a moment ago, so it is the plant this span can recover exactly.
        let _ = full_live_page_summaries(shard, start, end);
        let recovered = crate::engine::live_block_scan_entries();
        assert_eq!(
            recovered, one_walk,
            "{records} records: a whole-store summary walk charging {one_walk} entries was \
             planted inside the span that had just read 0, and the instrument recovered \
             {recovered}. Without this the 0 above says nothing."
        );
        assert!(
            scoped.is_empty() && scoped_visits == 0,
            "{records} records: the backlog is empty, so the scoped walk has no bucket to visit \
             and it visited {scoped_visits}"
        );
        drop(shards);

        // NOW THE ROUND ITSELF, which is what the saving has to be priced against. One round with
        // nothing to do, with the whole-call counter and the per-site tally read around it.
        crate::engine::reset_live_block_scan_entries();
        crate::engine::reset_live_block_scan_sites();
        let report = one_round(&engine);
        let round_entries = crate::engine::live_block_scan_entries();
        let round_sites = crate::engine::live_block_scan_sites_snapshot();
        assert!(
            report.errors.is_empty(),
            "{records} records: the measured round errored, so every figure it reports measures \
             nothing: {:?}",
            report.errors
        );
        assert!(
            round_entries > 0,
            "{records} records: the round materialised no live-page entry at all"
        );
        let summary_charge = round_sites.get(&summary_site).copied().unwrap_or(0);
        assert!(
            summary_charge > 0,
            "{records} records: the round charged NOTHING to {summary_site}, which is the walk \
             this whole file prices. Either the round stopped calling it or the site key moved."
        );
        let calls_per_round = summary_charge as f64 / one_walk as f64;

        println!(
            "  store {records:>7}: round {round_entries:>9} entries; ONE summary walk \
             {one_walk:>8} ({:.2}x the record count -- a settled shard has {_released} released \
             buckets and the supplement re-walks the model maps); the round charges \
             {summary_charge:>9} to {summary_site} ({calls_per_round:.2} walks); scoped walk \
             visits {scoped_visits}; this walk is {:.1}% of the round",
            one_walk as f64 / records as f64,
            100.0 * summary_charge as f64 / round_entries as f64
        );
        for (site, entries) in &round_sites {
            println!("      {entries:>9}  {site}");
        }
        rows.push((records, round_entries, summary_charge, full.len()));
    }

    let (small_records, small_round, small_saved, _) = rows[0];
    let (large_records, large_round, large_saved, _) = rows[1];
    assert!(
        small_saved > 0 && large_saved > 0,
        "the scoped walk saved nothing at one of the two sizes ({small_saved}, {large_saved})"
    );

    let ratio = large_saved as f64 / small_saved as f64;
    let corpus_ratio = large_records as f64 / small_records as f64;
    let per_record_small = small_saved as f64 / small_records as f64;
    let per_record_large = large_saved as f64 / large_records as f64;
    println!();
    println!(
        "  ROUND, nothing pending:   {small_round} -> {large_round} entries \
         ({:.3}x for a {corpus_ratio:.3}x corpus)",
        large_round as f64 / small_round as f64
    );
    println!(
        "  SCOPING THIS ONE WALK:    {small_saved} -> {large_saved} entries ({ratio:.3}x), \
         {:.1}% -> {:.1}% of the round",
        100.0 * small_saved as f64 / small_round as f64,
        100.0 * large_saved as f64 / large_round as f64
    );
    println!(
        "  per record of STORE       {per_record_small:.3} -> {per_record_large:.3}"
    );
    println!(
        "  per record of WORK        UNDEFINED -- the backlog is empty and nothing was retired, \
         asserted above"
    );
    // The integral: a store grown to N with one round every k records pays about
    // `per_record * N^2 / 2k` in this walk alone.
    let integral = per_record_large * 1_000_000.0 * 1_000_000.0 / (2.0 * 10_000.0);
    let round_integral = (large_round as f64 / large_records as f64)
        * 1_000_000.0
        * 1_000_000.0
        / (2.0 * 10_000.0);
    println!(
        "  INTEGRAL at N=1,000,000, one round every 10,000 records: the whole round \
         {round_integral:.4e} entries, of which THIS walk {integral:.4e}"
    );

    // AND A CORRECTION TO THE PUBLISHED FIGURE, because the integral is the number a reader
    // carries away. mx#1938 states the empty round's integral as "about 28.9 N^2 / 2k =
    // 1.445e12 at a million records with a round every 10,000". The CONSTANT reproduces -- 28.9
    // entries per record of store, against 24.9 measured here on a narrow shard -- but the
    // arithmetic does not: N^2 / 2k at N = 1e6 and k = 1e4 is 5e7, so 28.9 of them is 1.445e9.
    // The published figure is a thousand times too large. Asserted rather than remarked, with
    // mx#1938's own constant, so a later reader quoting e12 fails here.
    const PUBLISHED_PER_RECORD: f64 = 28.9;
    let published_integral = PUBLISHED_PER_RECORD * 1_000_000.0 * 1_000_000.0 / (2.0 * 10_000.0);
    println!(
        "  mx#1938's own constant, its own formula: 28.9 * N^2 / 2k at N=1e6, k=1e4 = \
         {published_integral:.4e}, not the 1.445e12 it prints"
    );
    assert!(
        (1.0e9..1.0e10).contains(&published_integral),
        "28.9 * N^2 / 2k at N = 1e6 and k = 1e4 came out {published_integral:.4e}; it is 1.445e9 \
         and if this arithmetic has moved then so has the correction that goes with it"
    );
    assert!(
        (0.5e9..5.0e9).contains(&round_integral),
        "the measured round's integral is {round_integral:.4e}. It sits beside mx#1938's 1.445e9 \
         because the per-record constants are 24.9 and 28.9; a figure outside 0.5e9..5e9 means \
         the two are no longer measuring the same shape and the comparison has to go."
    );

    assert!(
        ratio > 4.0,
        "the saving is supposed to be store-proportional: {small_saved} -> {large_saved} is \
         {ratio:.3}x for a corpus {corpus_ratio:.3}x larger. Below 4x it is not the whole-store \
         term this file claims it is."
    );
    assert!(
        (per_record_large / per_record_small - 1.0).abs() < 0.25,
        "per record of STORE the saving moved {per_record_small:.3} -> {per_record_large:.3}, \
         which is more than the 25% a flat per-record term is allowed to wander"
    );
    // AND THE HONEST HALF: this walk is a MINORITY of the round. mx#1938 wrote that scoping it
    // "would take 2,889,724 entries off the empty round at 100,000 records", which is the whole
    // round's figure. Asserted as a ceiling so a later reader cannot quote the saving as the
    // round's.
    let share = large_saved as f64 / large_round as f64;
    assert!(
        share < 0.5,
        "this walk is {:.1}% of the empty round at {large_records} records. If it ever became a \
         majority the framing above -- that scoping it is a minority of the round's floor -- is \
         no longer true and the prose has to change with the number.",
        100.0 * share
    );
}

// =============================================================================================
// 5. The enumeration, held
// =============================================================================================

/// The text between one call's own parentheses, balanced, so a multi-line call is read whole and
/// the NEXT call's arguments are not read at all.
pub(super) fn call_arguments(lines: &[&str], line_index: usize, needle: &str) -> Option<String> {
    let start_column = lines[line_index].find(needle)? + needle.len();
    let mut depth = 1i32;
    let mut out = String::new();
    let mut column = start_column;
    for line in lines.iter().skip(line_index).take(40) {
        let chars: Vec<char> = line.chars().collect();
        while column < chars.len() {
            match chars[column] {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(out);
                    }
                }
                _ => {}
            }
            out.push(chars[column]);
            column += 1;
        }
        out.push(' ');
        column = 0;
    }
    None
}

/// THE THREE CALL SITES STILL PASSING `0..u32::MAX`, AND WHY EACH IS RIGHT TO.
///
/// Where a page is FILED is decided by whoever rebuilds the bucket index, and each of the three
/// functions that do takes the routing range as an argument. Enumerated with the compiler --
/// the three doors into `BlockAddress`'s private routing-bucket field were renamed and rustc was
/// asked to name every caller, ITERATED to a zero pass because one rename-and-compile pass is not
/// an enumeration: rustc poisons the type of an expression holding an unresolved method and
/// suppresses every later use of that value. Pass one named 81 read sites and 72 assign sites,
/// pass two named 6 more that pass one had suppressed, pass three named none.
///
/// Twenty production call sites rebuild the index. When mx#1942 took that enumeration, ELEVEN
/// passed the shard's own range and NINE passed `0, u32::MAX` -- the recovery, replay,
/// manifest-install and bulk-flush paths, exactly the paths on which an address can arrive without
/// a routing bucket of its own. Eight of those nine now pass the shard's range. The total is still
/// twenty; what moved is the split, 11/9 to 19/1.
///
/// THE THREE THAT REMAIN ARE EACH CORRECT AS WRITTEN, for two different reasons.
///
/// ONE IS NOT PRODUCTION CODE, which is the thing this scan structurally cannot see.
/// `lifecycle.rs`'s `test_publish_recovering_shard` is a `#[cfg(test)]` helper, and this scan
/// excludes test FILES, not `#[cfg(test)]` items inside production files -- so it counted a
/// test-only helper among the nine production sites. That helper publishes its own shard info with
/// `start_routing_bucket: 0, end_routing_bucket: u32::MAX` a few lines below the call, so the whole
/// range IS that shard's own range.
///
/// TWO RUN OVER A DECODED MANIFEST INDEX, and there the whole range is load-bearing.
/// `install_bucket_dump_manifest` and the durable-manifest recovery base in `lifecycle.rs` both
/// call `rebuild_bucket_block_ownership` on the output of `decode_index_bytes` -- a WHOLE-SHARD
/// image carrying explicit routing buckets from whatever range the source shard ran on. That
/// rebuild does not merely PLACE unrouted pages by the range it is handed, it also FILTERS on it,
/// so the installing shard's range silently deletes every page whose source bucket falls outside
/// it. Measured rather than reasoned: with the target's range passed,
/// `storage_merged_dump_load_policy_coordinates_dump_load_replay_and_index_gc` -- source on
/// `load_shard` (0..u32::MAX), restore target on `0..16_383` -- read `merged-a` back as None. A
/// whole-shard image is installed whole or it is truncated; there is no third option.
///
/// So the nine were never one class, and the split is not by file or by callee. It is: does this
/// rebuild run over THIS shard's own live model maps (the shard's range is right) or over a
/// FOREIGN whole-shard image (the whole range is right)?
///
/// HELD AS A LIST, not as a count, because a count agrees with itself after a site moves. If a
/// site here is FIXED, delete it from this list. If one is ADDED, this fails and names it.
///
/// AND THE LIST IS WHAT MAKES THIS NON-VACUOUS. It is compared by equality, not by containment,
/// and it is not empty -- so a matcher that broke and found nothing produces `[]`, which fails
/// against a one-element list rather than passing as "no offending sites". That is the property an
/// empty expected list would have destroyed, which is why the remaining entry is kept in the list
/// rather than the assertion being turned into "must be empty". The floors below cover the other
/// direction.
///
/// THE CONTROL IS A SECOND IMPLEMENTATION. The same enumeration was taken outside the crate, in
/// Python, over the same source, and produced the same 20 / 11 / 9 split with the same seven
/// file-and-callee rows when mx#1942 wrote it.
///
/// rust-internal: reads this crate's own call sites, no product behaviour
#[test]
fn the_three_rebuild_call_sites_still_on_the_whole_range_are_each_right_to_be() {
    use std::path::Path;

    /// Every rebuild this scan can see that passes `0, u32::MAX`, as `file :: callee x count`.
    ///
    /// Keyed by file and callee rather than by line, so a sibling change that moves a line does
    /// not fail this and a change that ADDS or REMOVES one of these calls does.
    ///
    /// Three entries, and the note above says why each is right. Two are the manifest-decode
    /// rebuilds, where the whole range is what keeps a whole-shard image whole; one is
    /// `test_publish_recovering_shard`, a `#[cfg(test)]` helper this file-level scan cannot
    /// distinguish from production code.
    const WHOLE_RANGE_SITES: &[&str] = &[
        "engine/bucket_dump_manifest_methods.rs :: rebuild_bucket_block_ownership x1",
        "engine/lifecycle.rs :: promote_model_maps_to_bucket_index_authority x1",
        "engine/lifecycle.rs :: rebuild_bucket_block_ownership x1",
    ];
    const REBUILDS: [&str; 3] = [
        "rebuild_bucket_block_ownership(",
        "rebuild_bucket_first_index(",
        "promote_model_maps_to_bucket_index_authority(",
    ];

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut pending = vec![root.clone()];
    let mut files_scanned = 0usize;
    let mut lines_scanned = 0usize;
    let mut excluded = 0usize;
    let mut whole_range: Vec<String> = Vec::new();
    let mut shard_range: Vec<String> = Vec::new();

    while let Some(path) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                pending.push(entry_path);
                continue;
            }
            if entry_path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let display = entry_path.display().to_string();
            // A test naming one of these is not production code rebuilding an index. Taken out,
            // and the removals COUNTED, so an exclusion that started matching everything shows up
            // as a denominator that collapsed rather than as a clean pass.
            if display.contains("/tests/") || display.ends_with("tests.rs") {
                excluded += 1;
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&entry_path) else {
                continue;
            };
            files_scanned += 1;
            let lines: Vec<&str> = text.lines().collect();
            lines_scanned += lines.len();
            let relative = display
                .rsplit_once("/src/")
                .map(|(_, tail)| tail.to_string())
                .unwrap_or(display.clone());
            for (index, line) in lines.iter().enumerate() {
                for needle in REBUILDS {
                    if !line.contains(needle) {
                        continue;
                    }
                    // The definition is not a call of itself.
                    if line.contains(&format!("fn {}", &needle[..needle.len() - 1])) {
                        continue;
                    }
                    let Some(arguments) = call_arguments(&lines, index, needle) else {
                        continue;
                    };
                    let site = format!("{relative} :: {}", &needle[..needle.len() - 1]);
                    // BOTH OF THESE COST A RED RUN, and both are recorded because a later reader
                    // will reach for the same two shortcuts.
                    //
                    // A FIXED-LINE WINDOW IS TOO WIDE. `recovery_sweep_compact.rs` rebuilds the
                    // first index on `0, u32::MAX` and then rebuilds ownership on the shard's own
                    // range two lines later, so any window big enough to hold a multi-line call
                    // also holds the NEXT call's arguments and reads the first as a shard-range
                    // site. The arguments are taken by balancing parentheses instead.
                    //
                    // AND `u32::MAX` FIRST IS TOO EAGER. `engine.rs` passes
                    // `info.start_routing_bucket` with `.unwrap_or(u32::MAX)` beside it as the
                    // default for a shard whose info is missing, so its arguments hold BOTH
                    // spellings. A call that names `start_routing_bucket` at all is reading the
                    // shard's range.
                    if arguments.contains("start_routing_bucket") {
                        shard_range.push(site);
                    } else if arguments.contains("u32::MAX") {
                        whole_range.push(site);
                    }
                }
            }
        }
    }

    // VACUITY FLOORS, before any verdict. A scan that stopped finding files, or an exclusion that
    // started matching everything, reads exactly like a tree with no offending site left in it.
    assert!(
        files_scanned > 80,
        "the scan read {files_scanned} production .rs files under {}; below 80 it has stopped \
         reading the crate and every list below is empty for the wrong reason",
        root.display()
    );
    assert!(
        lines_scanned > 100_000,
        "the scan read {lines_scanned} lines; below 100,000 it is not reading this crate"
    );
    assert!(
        excluded > 10,
        "the scan excluded {excluded} test files; this crate has more than ten, so an exclusion \
         matching fewer means the matcher moved"
    );
    let total = whole_range.len() + shard_range.len();
    assert!(
        total >= 15,
        "the scan found {total} rebuild call sites in all; there were 20 when this was written \
         and below 15 the matcher is finding something other than the calls"
    );
    // THE FLOOR THAT MOVED WITH THE FIX. Before it, 11 sites read the shard's range; after it, 19.
    // A floor left at the old number would have gone on passing while the argument test quietly
    // stopped recognising a shard-range call and dropped those sites into NEITHER list -- which
    // would also empty `whole_range` and, with an empty expected list, have read as a clean pass.
    //
    // Set BELOW the current 19 on purpose: retiring a rebuild call site lowers this count BY
    // DESIGN, and a floor that forbids that would fail the next sibling who legitimately removes
    // one. 15 is low enough to allow four removals and high enough that a matcher which stopped
    // recognising the argument -- which would take this to roughly zero -- still fails here.
    assert!(
        shard_range.len() >= 13,
        "only {} rebuild call sites read the shard's own range; there were 17 after six of the \
         nine were reconciled and three were kept. Below 13, either several were retired at once \
         or the `start_routing_bucket` test has stopped recognising them -- and in the second case \
         every list above is wrong for a reason that does not show up as a failure anywhere else.",
        shard_range.len()
    );

    whole_range.sort();
    shard_range.sort();
    println!(
        "  {files_scanned} files / {lines_scanned} lines scanned, {excluded} test files excluded"
    );
    println!("  rebuild call sites: {total} total, {} on the shard's own range, {} on 0..u32::MAX",
        shard_range.len(),
        whole_range.len()
    );

    // Collapse to `file :: callee xN`, so a moved line is not a failure and an added or removed
    // call is.
    let mut counted: BTreeMap<String, usize> = BTreeMap::new();
    for site in &whole_range {
        *counted.entry(site.clone()).or_default() += 1;
    }
    let observed: Vec<String> = counted
        .iter()
        .map(|(site, count)| format!("{site} x{count}"))
        .collect();
    for site in &observed {
        println!("    0..u32::MAX  {site}");
    }

    let mut expected_sorted: Vec<String> =
        WHOLE_RANGE_SITES.iter().map(|s| s.to_string()).collect();
    expected_sorted.sort();
    assert_eq!(
        observed, expected_sorted,
        "the set of rebuild call sites filing pages under `0..u32::MAX` has moved.\n\
         If you FIXED one -- passed the shard's own range, which is what every other caller does \
         -- delete it from WHOLE_RANGE_SITES; that is the change this file exists to make safe, \
         and `the_reconstruct_the_replay_tail_runs_files_a_page_where_the_summary_walk_does_not_look` \
         is the measurement that says it works.\n\
         If you ADDED one, it is a new path on which a page can be filed under a bucket the dirty \
         set does not name, and a walk scoped to the dirty set would silently miss it."
    );
}
