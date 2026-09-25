// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT AN EVICTION ROUND COSTS ONCE THE STORE IS LARGE, and whether a round can keep up with one.
//!
//! Eviction has been measured here before, always at 500 to 4,000 objects. At that size every
//! term in a round is small enough that a term proportional to the STORE and a term proportional
//! to the BATCH are the same handful of microseconds, and the two cannot be told apart. These
//! tests run the same round two orders of magnitude further out and print the ratio beside the
//! corpus ratio, so each term names itself.
//!
//! THE RESULT, at 20,000 and 200,000 objects with the shipped batch limit of 16 victims:
//!
//! ```text
//!                                     20,000        200,000     ratio   corpus ratio 10.00x
//!   victims chosen                        16             16      1.00x   FLAT
//!   sampler scan budget                  320            320      1.00x   FLAT
//!   recency entries cloned                 0              0        --    FLAT (sampled arm)
//!   model-map addresses visited       20,000        200,000     10.00x   GROWS WITH THE STORE
//!   bucket-index entries visited      40,000        400,000     10.00x   GROWS WITH THE STORE
//!   blocks released                        0              0        --    the actuator releases
//!                                                                        nothing at the default
//! ```
//!
//! THE SHAPE. Choosing the victims is bounded and has been since the sampler landed -- the
//! sampler walks `samples * batch_limit * scan_turns` buckets and no more, and clones no recency
//! map. What is not bounded is everything the round does AROUND the choice:
//!
//!   * `release_bucket_blocks` asks the model maps what a reload would rebuild for its victims.
//!     The maps are keyed by object and the routing bucket is a field of the ADDRESS, so the pass
//!     runs `accept` on every live address in the shard to answer a question about at most
//!     `batch_limit` buckets. That walk is not new and the tree already argues it cannot be
//!     answered from an index. What is new is that it now has a counter: the one counter that
//!     watched this path, `BUCKET_SCOPED_MODEL_ENTRIES`, charges only what the walk EMITS, which
//!     is bounded by the victims -- so the audit row for a whole-store term read as a batch-sized
//!     term at every size ever measured.
//!   * the pressure gate reads `bucket_index_resident_bytes` before the round and again after it,
//!     and each read walks every node and every resident page in the bucket index.
//!
//! AND IT IS UNDER THE SHARD-TABLE WRITE GUARD. `apply_storage_eviction` takes `shards.write()`
//! and calls `release_bucket_blocks` inside it, so the whole-store walk is an interval every
//! serving read and write on the shard queues behind.
//!
//! WHAT THE ACTUATOR RELEASES AT THE SHIPPED SETTINGS: nothing. `eviction_dump_before_evict`
//! ships false, a bucket written and not yet dumped is dirty, and a dirty bucket is refused --
//! correctly, because the model maps carry no per-page dirty bit for a reload to restore. So a
//! default-configured round pays the whole-store walk and the two index walks to release zero
//! blocks. Turn the dump on and the same round releases its victims; both arms are measured, and
//! the refusal TERM is named rather than left as a count.
//!
//! WHETHER IT KEEPS UP. A round gives back at most `batch_limit` buckets however large the store
//! is, and costs a number of visits proportional to the store. The cost of one unit of relief is
//! therefore the store divided by the batch, and it is measured here at two sizes so that is a
//! number rather than an argument. `an_eviction_round_cannot_keep_up_with_the_store_it_is_draining`
//! runs real rounds rather than extrapolating from one, and reports how many.

#![allow(clippy::all)]
use super::*;
use crate::engine::storage_bucket_internals::{release_bucket_blocks, BucketReleaseOutcome};
use crate::Command;
#[cfg(feature = "alloc-probe")]
use crate::ExecuteRequest;
use std::collections::{BTreeMap, BTreeSet};

/// The two corpus sizes. A decade apart so a term that is flat and a term that tracks the store
/// separate by a factor of ten rather than by a residual.
/// Read by the `alloc-probe` arms only, which is where a corpus this size is affordable.
#[allow(dead_code)]
const SMALL: usize = 20_000;
#[allow(dead_code)]
const LARGE: usize = 200_000;

/// The sizes the always-on guards use. The claim they pin is the same one; what they give up is
/// the resolution, which is what the `alloc-probe` table above is for.
const GUARD_SMALL: usize = 2_000;
const GUARD_LARGE: usize = 8_000;

/// The shipped batch limit (`DEFAULT_EVICTION_BATCH_LIMIT`).
const BATCH: usize = 16;

fn evict_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine.use_sampled_eviction_for_test();
    engine
}

/// Seed `count` objects, rolling to a fresh block slab after every batch.
///
/// THE SLAB LAYOUT IS THE FIXTURE, AND TWO EARLIER SHAPES OF IT PROVED NOTHING.
///
/// The shipped slab target is 1 GiB, so a corpus of this size writes into ONE slab: every live
/// page a victim holds carries `block_slab_id` 0, and a guard asserting that the victims span
/// slabs compares `{0}` with `{0}` and passes whatever the selection does.
///
/// Rolling ONCE at the midpoint does not fix it, and the reason is the selection rather than the
/// layout. `block_routing_bucket` recency is stamped by every command, read or write, with
/// `now_ms()` (`engine.rs`), and the sampler ranks its pool coldest-first -- so the victims are
/// the EARLIEST-WRITTEN buckets in its scan window, and the earliest-written pages are on the
/// first slab by construction. Measured: a midpoint roll gave sixteen victims all on slab 0.
///
/// A third shape also failed and is worth naming because it looks like the fix. Keys of the form
/// `prefix-{ordinal:09}` hash to routing buckets that cluster: at 8,000 records the eighty LOWEST
/// routing buckets in the store came from ordinals 4,680 to 4,699 -- twenty consecutive ordinals
/// -- so the sampler's window was a twenty-ordinal slice of the write order, and a midpoint roll
/// put all of it on slab 1. Scattering the ordinal across the key space (see [`scale_key`]) fixes
/// the clustering and does NOT fix the span, because recency still decides.
///
/// What works is making the slab non-monotonic in write TIME at the granularity the ranking uses.
/// Rolling after every batch puts consecutive batches on consecutive slabs, so the coldest
/// buckets in any scan window are drawn from the first few batches and therefore from several
/// slabs -- and if a whole run of batches shares one `now_ms()`, the tie breaks on routing bucket,
/// which is scattered across the batches for the same reason. Both readings span.
///
/// Every roll is asked for with a target of one byte, which any slab that has been written to at
/// all is already at -- so each one rolls a slab with RECORDS IN IT, never an empty file.
fn seed_with_a_slab_roll(engine: &TemporalEngine, count: usize) {
    seed_range(engine, 0, count);
}

/// The key for one ordinal, with the ordinal SCATTERED across the key space first.
///
/// MEASURED, and the reason the obvious fixture does not work. With keys of the shape
/// `prefix-{ordinal:09}` the routing buckets of neighbouring ordinals land next to each other:
/// the store's object hash is FNV-1a, a run of ordinals differs only in its last few digits, and
/// at 8,000 records the eighty LOWEST routing buckets in the whole store came from ordinals 4,680
/// to 4,699 -- twenty consecutive ordinals. The sampler ranks its pool by routing bucket when
/// recency ties, so it chose sixteen victims from a twenty-ordinal window; pages are appended in
/// write order, so any slab boundary drawn on the ordinal puts that whole window on ONE slab, and
/// a guard asserting the victims span slabs compared `{1}` with `{1}` and failed -- or, had the
/// roll fallen the other way, passed while proving nothing.
///
/// Multiplying the ordinal by an odd constant first is a bijection on 32 bits, so the keys stay
/// distinct and one per ordinal, and neighbouring ordinals stop being neighbours in routing-bucket
/// space. The key is also a CONSTANT 22 bytes at every ordinal and every corpus size, which
/// matters because allocation counts move with key length and the two corpus arms are compared by
/// allocation.
fn scale_key(ordinal: usize) -> String {
    let scattered = (ordinal as u64).wrapping_mul(2_654_435_761) & 0xffff_ffff;
    format!("evict-large-{scattered:010}")
}

fn seed_range(engine: &TemporalEngine, from: usize, to: usize) {
    const BATCH_SIZE: usize = 500;
    let mut index = from;
    while index < to {
        let end = (index + BATCH_SIZE).min(to);
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            commands.push(Command::StringSet {
                key: scale_key(cursor),
                value: vec![b'v'; 128],
            });
            cursor += 1;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(
            response.status.ok,
            "seed write failed at {index}: {:?}",
            response.status
        );
        let rolled = engine
            .block_store
            .prepare_next_slab_with_target(1)
            .expect("rolling the block slab between seed batches");
        assert!(
            rolled.is_some(),
            "the roll after the batch ending at {end} did not happen, so this batch and the next \
             share a slab and the layout the slab-span guard depends on is not the one described"
        );
        index = end;
    }
}

/// Live pages per object key, read straight off the model maps.
///
/// The COUNT per key, not merely which keys are present. Evicting too much is data loss and it is
/// silent, so the direction guard compares this map element by element against the same map taken
/// before the round: a key that kept its entry but lost a page would pass a presence check.
fn live_pages_by_key(engine: &TemporalEngine) -> BTreeMap<String, usize> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut counts = BTreeMap::new();
    for entry in crate::engine::collect_live_block_entries(shard) {
        *counts.entry(entry.object_key.to_string()).or_insert(0) += 1;
    }
    counts
}

/// Objects each routing bucket still claims, by bucket.
///
/// A release empties a bucket's RESIDENT page list and deliberately keeps its `object_index` --
/// that is what keeps a released bucket countable and distinguishes it from one that genuinely
/// holds nothing. The model maps do not record it, so [`live_pages_by_key`] cannot see it go; a
/// release that dropped it would be silent everywhere else in this file.
fn objects_by_bucket(engine: &TemporalEngine) -> BTreeMap<u32, usize> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.object_index.len()))
        .collect()
}

/// Slab ids the shard's live pages actually sit on, and how many pages sit on each.
fn live_pages_by_slab(engine: &TemporalEngine) -> BTreeMap<u64, usize> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut by_slab = BTreeMap::new();
    for entry in crate::engine::collect_live_block_entries(shard) {
        *by_slab.entry(entry.address.block_slab_id).or_insert(0) += 1;
    }
    by_slab
}

/// The slab ids the named routing buckets hold resident pages on.
fn slabs_spanned_by(engine: &TemporalEngine, buckets: &BTreeSet<u32>) -> BTreeSet<u64> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 loaded");
    let mut slabs = BTreeSet::new();
    for routing_bucket in buckets {
        if let Some(bucket) = shard.bucket_index.bucket_map.get(routing_bucket) {
            for page in bucket.block_index.values() {
                if !page.deleted {
                    slabs.insert(page.address.block_slab_id);
                }
            }
        }
    }
    slabs
}

/// Everything one round of `apply_storage_eviction` cost and did, in counters rather than clocks.
///
/// Every field is read by at least one test in this file, but two of them only by the
/// `alloc-probe` arms, so the struct says so rather than turning a default build's warning list
/// into noise.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct RoundCost {
    records: usize,
    buckets: usize,
    victims: usize,
    /// Model-map addresses the round LOOKED AT. The whole-store term.
    model_visits: u64,
    /// Model-map pages the round MATERIALIZED. The batch-sized term, and the only one that had a
    /// counter before this file.
    model_emitted: u64,
    /// Bucket-index nodes and pages the two resident-bytes reads walked.
    resident_visits: u64,
    /// Whole-store live-page entries materialized anywhere in the round.
    live_scan: u64,
    /// Recency entries the selection copied. Zero on the sampled arm by construction.
    recency_cloned: u64,
    released_buckets: usize,
    released_blocks: usize,
    refused: usize,
    /// Times the release ran its whole-store derivation. At most one per `release_bucket_blocks`
    /// call, and zero when no candidate reached the model-map comparison.
    derivations: u64,
    index_bytes_before: u64,
    index_bytes_after: u64,
    round_nanos: u128,
}

/// Drive ONE round on a seeded engine and collect every counter around it.
///
/// Every counter is reset immediately before the call and read immediately after, which is what
/// makes a process-wide counter mean one round; the suite runs `--test-threads=1`.
fn one_round(engine: &TemporalEngine, records: usize, batch_limit: usize, dump: bool) -> RoundCost {
    let buckets = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        shards
            .get(&1)
            .map(|shard| shard.bucket_index.bucket_map.len())
            .unwrap_or_default()
    };
    crate::engine::reset_model_map_addresses_visited();
    crate::engine::reset_bucket_release_model_derivations();
    crate::engine::reset_bucket_scoped_model_entries();
    crate::engine::reset_bucket_index_resident_bytes_visits();
    crate::engine::reset_live_block_scan_entries();
    crate::engine::storage_lifecycle_methods::EVICTION_RECENCY_ENTRIES_CLONED
        .store(0, std::sync::atomic::Ordering::Relaxed);
    let started = std::time::Instant::now();
    // Threshold 0 so the pressure gate admits and the round actually runs.
    let report = engine.apply_storage_eviction(1, 0, batch_limit, dump, false);
    let round_nanos = started.elapsed().as_nanos();
    let cost = RoundCost {
        records,
        buckets,
        victims: report.selected_victims.len(),
        model_visits: crate::engine::model_map_addresses_visited(),
        model_emitted: crate::engine::bucket_scoped_model_entries(),
        resident_visits: crate::engine::bucket_index_resident_bytes_visits(),
        live_scan: crate::engine::live_block_scan_entries(),
        recency_cloned: crate::engine::storage_lifecycle_methods::EVICTION_RECENCY_ENTRIES_CLONED
            .load(std::sync::atomic::Ordering::Relaxed),
        released_buckets: report.bucket_index_buckets_released,
        released_blocks: report.bucket_index_blocks_released,
        refused: report.bucket_index_release_refused,
        derivations: crate::engine::bucket_release_model_derivations(),
        index_bytes_before: report.bucket_index_bytes_before,
        index_bytes_after: report.bucket_index_bytes_after,
        round_nanos,
    };
    assert!(
        report.pressure_gate_open,
        "{records} records: the round never got past the pressure gate, so nothing was measured: \
         {}",
        report.skipped_reason
    );
    cost
}

fn ratio(small: u64, large: u64) -> f64 {
    if small == 0 {
        0.0
    } else {
        large as f64 / small as f64
    }
}

// -------------------------------------------------------------------------------------------
// FIXTURE PROOF. Runs first and on its own, so a failure here cannot let a claim below stand.
// -------------------------------------------------------------------------------------------

/// THE FIXTURE CAN EXPRESS WHAT THE TESTS BELOW MEASURE.
///
/// Four things have to be true before any number in this file means anything, and each is a
/// separate assertion with its own message:
///
///   1. the store holds MORE THAN ONE routing bucket -- otherwise "the victims" and "the store"
///      are the same object and every ratio is a comparison of a thing with itself;
///   2. the store holds MORE THAN ONE block slab, and both slabs hold RECORDS. A slab count taken
///      from the directory would pass on two empty files;
///   3. the victims a round chooses span MORE THAN ONE slab. A mutant in this area survived
///      elsewhere because every live page sat in slab 0 and the test compared `{0}` against
///      `{0}`; re-seeding into one key range collapses it back, which is why the fixture rolls
///      mid-seed and the halves are disjoint key ranges;
///   4. the round chooses FEWER victims than there are buckets, which is what makes "per victim"
///      and "per record" different denominators at all.
#[test]
fn the_eviction_fixture_holds_several_slabs_and_the_victims_span_them() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = evict_engine(dir.path());
    seed_with_a_slab_roll(&engine, GUARD_LARGE);

    let by_slab = live_pages_by_slab(&engine);
    let buckets = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        shards
            .get(&1)
            .map(|shard| shard.bucket_index.bucket_map.len())
            .unwrap_or_default()
    };
    let report = engine.apply_storage_eviction(1, 0, BATCH, false, false);
    let victims = report
        .selected_victims
        .iter()
        .map(|victim| victim.routing_bucket)
        .collect::<BTreeSet<_>>();
    let victim_slabs = slabs_spanned_by(&engine, &victims);

    println!(
        "  fixture: {GUARD_LARGE} records -> {buckets} buckets, live pages by slab {by_slab:?}, \
         {} victims spanning slabs {victim_slabs:?}",
        victims.len()
    );

    assert!(
        buckets > 1,
        "the fixture produced {buckets} routing bucket(s); with one bucket the store and the \
         batch are the same object and every ratio below compares a thing with itself"
    );
    assert!(
        by_slab.len() > 1,
        "the fixture produced {} block slab(s) holding live pages; a single-slab store cannot \
         show that a selection spans slabs, and the mid-seed roll is there to prevent exactly \
         this: {by_slab:?}",
        by_slab.len()
    );
    for (slab, pages) in &by_slab {
        assert!(
            *pages > 0,
            "slab {slab} holds {pages} live pages; a slab counted from the directory rather than \
             from its records would pass here on an empty file"
        );
    }
    assert!(
        !victims.is_empty(),
        "the round chose no victims, so there is nothing whose slab span could be asserted"
    );
    assert!(
        victim_slabs.len() > 1,
        "the {} victims chosen all sit on slab(s) {victim_slabs:?}; with the victims confined to \
         one slab a guard comparing victim slabs against store slabs would be comparing a set \
         with itself",
        victims.len()
    );
    assert!(
        victims.len() < buckets,
        "the round took {} of {buckets} buckets; with the batch equal to the store there is no \
         difference between a per-victim cost and a per-record cost",
        victims.len()
    );
}

// -------------------------------------------------------------------------------------------
// WHAT THE ACTUATOR RELEASES.
// -------------------------------------------------------------------------------------------

/// WHAT THE RELEASE ACTUALLY RELEASES, AT THE SHIPPED SETTING AND AT THE OTHER ONE.
///
/// An earlier note in this campaign recorded that the eviction round's actuator releases nothing.
/// This is that claim checked rather than inherited, and it is half right: at the shipped setting
/// it releases nothing, and the reason is a single named term rather than an absence.
///
/// `eviction_dump_before_evict` ships FALSE. A bucket that has been written and not dumped is
/// dirty, `release_bucket_blocks` refuses a dirty bucket, and it is right to -- the model maps
/// carry no per-page dirty bit, so a reload could not restore what a release of a dirty bucket
/// dropped. Turn the dump on, the round dumps its victims and clears their dirty state first, and
/// the same actuator releases them.
///
/// Both arms assert the refusal TERM, not just the count: eleven terms share `refused_buckets`,
/// and a round refused for `NotInMemory` and a round refused for `BucketDirty` are different
/// findings that a count cannot separate.
#[test]
fn the_release_actuator_is_refused_at_the_shipped_setting_and_releases_with_the_dump_on() {
    /// The round, plus a second release driven straight at the actuator so the refusal TERM can
    /// be read. `StorageEvictionReport` publishes the refusal COUNT and eleven terms share it.
    fn arm(dump: bool) -> (RoundCost, BucketReleaseOutcome) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = evict_engine(dir.path());
        seed_with_a_slab_roll(&engine, GUARD_SMALL);
        let cost = one_round(&engine, GUARD_SMALL, BATCH, dump);

        let cache_report = engine.storage_cache_inspection_report(1);
        let cache_by_bucket = cache_report
            .bucket_summaries
            .iter()
            .map(|summary| (summary.routing_bucket, summary.clone()))
            .collect::<BTreeMap<_, _>>();
        let candidates = engine
            .sampled_eviction_victims(1, BATCH, &cache_by_bucket)
            .iter()
            .map(|victim| victim.routing_bucket)
            .collect::<Vec<_>>();
        let mut shards = engine.shards.write().expect("shards lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        let outcome = release_bucket_blocks(shard, &candidates);
        assert_eq!(
            outcome.released_buckets.len() + outcome.refused_buckets,
            candidates.len(),
            "dump={dump}: the actuator acted on {} of {} candidates, so the terms below describe \
             a partial run",
            outcome.released_buckets.len() + outcome.refused_buckets,
            candidates.len()
        );
        assert_eq!(
            outcome.refusals.total(),
            outcome.refused_buckets,
            "dump={dump}: the named terms sum to {} against {} refusals, so a refusal was \
             counted without being attributed",
            outcome.refusals.total(),
            outcome.refused_buckets
        );
        (cost, outcome)
    }

    let (shipped, shipped_terms) = arm(false);
    let (dumped, dumped_terms) = arm(true);
    println!(
        "  dump_before_evict=false: {} victims -> {} buckets released, {} blocks, {} refused\n    \
         terms: {:?}\n  \
         dump_before_evict=true : {} victims -> {} buckets released, {} blocks, {} refused\n    \
         terms: {:?}",
        shipped.victims,
        shipped.released_buckets,
        shipped.released_blocks,
        shipped.refused,
        shipped_terms.refusals,
        dumped.victims,
        dumped.released_buckets,
        dumped.released_blocks,
        dumped.refused,
        dumped_terms.refusals,
    );

    // WHICH TERM. The shipped setting refuses on `bucket_dirty` and nothing else -- a round
    // refused for `not_in_memory` would be a different finding with the same count.
    assert_eq!(
        shipped_terms.refusals.bucket_dirty, shipped_terms.refused_buckets,
        "at the shipped setting {} of {} refusals were `bucket_dirty`; the claim recorded here \
         is that an undumped bucket is what the actuator declines, and another term would mean \
         something else is stopping it: {:?}",
        shipped_terms.refusals.bucket_dirty, shipped_terms.refused_buckets, shipped_terms.refusals
    );

    assert!(
        shipped.victims > 0 && dumped.victims > 0,
        "both arms must have chosen victims, or neither measured an actuator: {} and {}",
        shipped.victims,
        dumped.victims
    );
    assert_eq!(
        shipped.released_blocks, 0,
        "the shipped setting released {} blocks; this test exists to record that it releases \
         none, and a number here means the refusal it is built on has changed",
        shipped.released_blocks
    );
    assert_eq!(
        shipped.refused, shipped.victims,
        "at the shipped setting every victim must be refused, got {} refused of {} victims",
        shipped.refused, shipped.victims
    );
    assert!(
        dumped.released_blocks > 0,
        "with the dump on the actuator released {} blocks; if this is zero too then the release \
         path is inert at every setting and the finding is a different one",
        dumped.released_blocks
    );
    assert!(
        dumped.index_bytes_after < dumped.index_bytes_before,
        "with the dump on the resident bucket index did not shrink ({} -> {}), so the release \
         freed nothing the pressure gate can see",
        dumped.index_bytes_before,
        dumped.index_bytes_after
    );
    assert_eq!(
        shipped.index_bytes_after, shipped.index_bytes_before,
        "the shipped setting changed the resident index ({} -> {}) while releasing nothing",
        shipped.index_bytes_before,
        shipped.index_bytes_after
    );
}

// -------------------------------------------------------------------------------------------
// DIRECTION. Evicting too little is memory held; evicting too much is data loss, and silent.
// -------------------------------------------------------------------------------------------

/// A ROUND LOSES NOTHING, CHECKED ELEMENT BY ELEMENT AGAINST A CONTROL.
///
/// The strong form, in both arms. Not "the store is still non-empty" and not "the keys are still
/// there": the live-page COUNT for every key before the round is compared with the count after,
/// key by key, so a key that survived with one of its two pages dropped fails here. The control
/// is the same map taken on the same engine immediately before the round.
///
/// Both settings, because they take different paths through the actuator: one refuses every
/// victim and one releases them. A release drops RESIDENT entries and keeps the model maps
/// whole -- that is what makes it reversible -- so the live-page map must be identical across it.
#[test]
fn an_eviction_round_leaves_every_live_key_and_page_exactly_where_it_was() {
    for dump in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = evict_engine(dir.path());
        seed_with_a_slab_roll(&engine, GUARD_SMALL);

        let before = live_pages_by_key(&engine);
        let buckets_before = objects_by_bucket(&engine);
        assert_eq!(
            before.len(),
            GUARD_SMALL,
            "dump={dump}: the control map holds {} keys for {GUARD_SMALL} seeded records, so it \
             is not describing the store this round runs on",
            before.len()
        );
        let cost = one_round(&engine, GUARD_SMALL, BATCH, dump);
        let after = live_pages_by_key(&engine);

        assert!(
            cost.victims > 0,
            "dump={dump}: the round chose no victims, so it did nothing this could be the \
             control for"
        );
        let lost = before
            .iter()
            .filter(|(key, count)| after.get(*key) != Some(*count))
            .map(|(key, count)| (key.clone(), *count, after.get(key).copied()))
            .take(5)
            .collect::<Vec<_>>();
        assert!(
            lost.is_empty(),
            "dump={dump}: {} of {} keys changed their live-page count across one round -- \
             eviction is supposed to release RESIDENT state and lose nothing. First few \
             (key, before, after): {lost:?}",
            before
                .iter()
                .filter(|(key, count)| after.get(*key) != Some(*count))
                .count(),
            before.len()
        );
        assert_eq!(
            after.len(),
            before.len(),
            "dump={dump}: the store gained or lost keys across the round ({} -> {})",
            before.len(),
            after.len()
        );

        // THE SECOND DIRECTION, which the model maps cannot report. A released bucket keeps its
        // object index; a mutant that cleared it alongside the page list would leave every
        // assertion above green and the store uncountable.
        let buckets_after = objects_by_bucket(&engine);
        let emptied = buckets_before
            .iter()
            .filter(|(routing_bucket, count)| buckets_after.get(*routing_bucket) != Some(*count))
            .map(|(routing_bucket, count)| {
                (
                    *routing_bucket,
                    *count,
                    buckets_after.get(routing_bucket).copied(),
                )
            })
            .take(5)
            .collect::<Vec<_>>();
        assert!(
            emptied.is_empty(),
            "dump={dump}: a bucket's object index changed across the round -- a release drops \
             the resident page list and keeps the object index, so this must be EQUAL and not \
             merely non-zero. First few (bucket, before, after): {emptied:?}"
        );
    }
}

// -------------------------------------------------------------------------------------------
// THE ALWAYS-ON SCALING GUARD. Counters, so it needs no feature and no allocator.
// -------------------------------------------------------------------------------------------

/// ONE ROUND'S CHOOSING IS FLAT AND ONE ROUND'S ACCOUNTING IS NOT, at two sizes four times apart.
///
/// The claim this pins is the shape rather than the constant: the sampler's work does not move
/// with the store and the walks around it do. Deliberately counter-based -- no `alloc-probe`, no
/// clock -- so it runs in the ordinary gate and cannot be turned green by a busy box.
///
/// THE CONTROL is the victim count and the recency clone: if those moved with the store too, the
/// growth below would say nothing about which term grows.
#[test]
fn an_eviction_rounds_bookkeeping_tracks_the_store_while_its_choosing_does_not() {
    fn arm(records: usize, dump: bool) -> RoundCost {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = evict_engine(dir.path());
        seed_with_a_slab_roll(&engine, records);
        one_round(&engine, records, BATCH, dump)
    }

    let small = arm(GUARD_SMALL, false);
    let large = arm(GUARD_LARGE, false);
    // THE MODEL-MAP CLAIM MOVED ARMS, AND IT DID NOT WEAKEN.
    //
    // As written, every assertion below ran at the shipped `dump_before_evict` false, where every
    // candidate is refused on `bucket_dirty`. The release now derives lazily, so at that setting
    // no candidate reaches the model-map comparison and the pass does not happen: the visit rows
    // read a true ZERO rather than the whole store. That is the change, and it is asserted as
    // such below and at two sizes in `a_round_that_refuses_every_candidate_no_longer_walks_the_\
    // model_maps`.
    //
    // What did NOT change is the claim this test was written to record: when a candidate DOES
    // survive its own terms, the pass it then runs is proportional to the STORE and not to the
    // sixteen buckets it is asking about. So that claim -- the identity, the ratio and the
    // per-victim growth -- is asserted here against a RELEASING round instead of being relaxed,
    // and the shipped-setting arm keeps every other row it had. The bucket-index rows stay on the
    // shipped arm because nothing about them moved.
    let small_releasing = arm(GUARD_SMALL, true);
    let large_releasing = arm(GUARD_LARGE, true);
    let corpus = ratio(GUARD_SMALL as u64, GUARD_LARGE as u64);

    println!(
        "\n  ONE EVICTION ROUND, batch limit {BATCH}, dump off\n\
         \n                                  {GUARD_SMALL:>8} {GUARD_LARGE:>10}      ratio  (corpus {corpus:.2}x)\n\
           routing buckets               {:>8} {:>10}   {:>8.2}x\n\
           victims chosen                {:>8} {:>10}   {:>8.2}x\n\
           recency entries cloned        {:>8} {:>10}\n\
           model-map addresses visited   {:>8} {:>10}   {:>8.2}x\n\
           ... on a RELEASING round      {:>8} {:>10}   {:>8.2}x\n\
           model-map pages emitted       {:>8} {:>10}\n\
           bucket-index entries visited  {:>8} {:>10}   {:>8.2}x\n\
           live-page entries scanned     {:>8} {:>10}\n\
           blocks released               {:>8} {:>10}\n",
        small.buckets,
        large.buckets,
        ratio(small.buckets as u64, large.buckets as u64),
        small.victims,
        large.victims,
        ratio(small.victims as u64, large.victims as u64),
        small.recency_cloned,
        large.recency_cloned,
        small.model_visits,
        large.model_visits,
        ratio(small.model_visits, large.model_visits),
        small_releasing.model_visits,
        large_releasing.model_visits,
        ratio(small_releasing.model_visits, large_releasing.model_visits),
        small.model_emitted,
        large.model_emitted,
        small.resident_visits,
        large.resident_visits,
        ratio(small.resident_visits, large.resident_visits),
        small.live_scan,
        large.live_scan,
        small.released_blocks,
        large.released_blocks,
    );

    // VACUITY FLOOR, before any ratio is believed.
    assert!(
        large.buckets > small.buckets,
        "the two corpora produced {} and {} routing buckets; without a bigger store there is no \
         scaling question",
        small.buckets,
        large.buckets
    );
    assert!(
        small_releasing.model_visits > 0 && large_releasing.model_visits > 0,
        "a RELEASING round visited no model-map addresses at either size ({} and {}), which is \
         what this counter reads when it is not installed at all",
        small_releasing.model_visits,
        large_releasing.model_visits
    );
    // THE CHANGE, on the arm that ships. Stated here rather than only in the test that owns it,
    // because this is the table an operator reads and a whole-store row that quietly became zero
    // would otherwise look like the counter dying.
    assert_eq!(
        (small.model_visits, large.model_visits, small.derivations, large.derivations),
        (0, 0, 0, 0),
        "at the shipped `dump_before_evict` false every candidate is refused on a term that reads \
         its own bucket, so the round should reach no derivation at all; it visited {} and {} \
         addresses in {} and {} derivation(s)",
        small.model_visits,
        large.model_visits,
        small.derivations,
        large.derivations
    );

    // THE CONTROL ARM, with its own failure message. The point of the test is that one term moves
    // and another does not; a run where BOTH moved would be measuring the store twice.
    assert_eq!(
        small.victims, large.victims,
        "the victim count moved with the store ({} -> {}), so the growth below cannot be \
         attributed to the bookkeeping rather than to the batch",
        small.victims, large.victims
    );
    assert_eq!(
        (small.recency_cloned, large.recency_cloned),
        (0, 0),
        "the sampled arm copied recency entries ({} and {}); this arm exists so that choosing \
         costs the batch, and a non-zero here means the exhaustive arm ran instead",
        small.recency_cloned,
        large.recency_cloned
    );

    // THE SUBJECT. Both walks are proportional to the store, so both ratios must reach the corpus
    // ratio. A fix that bounds either one turns this red, which is the point: the number is
    // recorded so that it cannot change silently.
    let visits_ratio = ratio(small_releasing.model_visits, large_releasing.model_visits);
    assert!(
        visits_ratio >= corpus * 0.9,
        "model-map visits on a RELEASING round grew {visits_ratio:.2}x over a {corpus:.2}x corpus \
         ({} -> {}); this guard records that the release's pass over the model maps is \
         proportional to the STORE and not to the {BATCH} buckets it is asking about",
        small_releasing.model_visits,
        large_releasing.model_visits
    );
    let resident_ratio = ratio(small.resident_visits, large.resident_visits);
    assert!(
        resident_ratio >= corpus * 0.9,
        "bucket-index visits grew {resident_ratio:.2}x over a {corpus:.2}x corpus ({} -> {}); the \
         pressure gate reads resident bytes before the round and after it, and each read walks \
         the whole index",
        small.resident_visits,
        large.resident_visits
    );

    // THE ATTRIBUTION, AS AN EXACT IDENTITY RATHER THAN A RESIDUAL.
    //
    // A round at the shipped setting makes EXACTLY ONE pass over the model maps -- the release's
    // -- and EXACTLY TWO reads of the bucket index, one on each side of the actuator. Both are
    // asserted against the store's own size rather than against a sum of the probe rows, so an
    // unattributed walk added later cannot hide inside a tolerance: it shows up as a whole extra
    // pass. The allocation table in `what_an_eviction_round_costs_on_a_large_store_in_both_\
    // victim_regimes` cannot do this job -- the pass is borrow-only and allocates NOTHING, which
    // is why every allocation row there is flat while these counters are not.
    for arm in [&small_releasing, &large_releasing] {
        assert_eq!(
            arm.model_visits, arm.records as u64,
            "a RELEASING round over {} records visited {} model-map addresses; one whole-store \
             pass is {} exactly, so anything else means a pass was added or one stopped happening",
            arm.records, arm.model_visits, arm.records
        );
        assert_eq!(
            arm.derivations, 1,
            "a RELEASING round over {} records ran {} whole-store derivation(s); the batch shares \
             exactly one",
            arm.records, arm.derivations
        );
    }
    for arm in [&small, &large] {
        let expected_index_reads = 2 * (arm.buckets as u64 + arm.records as u64);
        assert_eq!(
            arm.resident_visits, expected_index_reads,
            "a round over {} records visited {} bucket-index entries; two reads of an index \
             holding {} nodes and {} pages is {expected_index_reads} exactly",
            arm.records, arm.resident_visits, arm.buckets, arm.records
        );
    }

    // PER VICTIM, which is the number an operator feels: the cost of one unit of relief.
    let small_per_victim = small_releasing.model_visits / small_releasing.victims.max(1) as u64;
    let large_per_victim = large_releasing.model_visits / large_releasing.victims.max(1) as u64;
    println!(
        "  RELEASING round, model-map addresses visited PER VICTIM: {small_per_victim} at \
         {GUARD_SMALL} records, {large_per_victim} at {GUARD_LARGE} -- {:.2}x",
        ratio(small_per_victim, large_per_victim)
    );
    assert!(
        large_per_victim > small_per_victim,
        "the per-victim cost did not grow ({small_per_victim} -> {large_per_victim}); with a \
         fixed batch against a growing store it has to, and a flat reading means the batch moved"
    );
}

/// A ROUND VISITS THE WHOLE STORE EVEN WHEN IT IS ASKED FOR ONE VICTIM.
///
/// The batch-size half of the same claim, and the one that separates "the round is expensive
/// because it takes sixteen buckets" from "the round is expensive because the store is large".
/// One victim and sixteen victims on the SAME corpus cost the same visits.
///
/// RUN ON A RELEASING ROUND, AND WITH A VACUITY FLOOR THAT IT DID NOT HAVE. As written, both arms
/// ran at the shipped `dump_before_evict` false, where the release now reaches no derivation at
/// all -- so both arms read ZERO visits and the equality between them held for the wrong reason.
/// A guard that compares two numbers and is satisfied by both being zero is the shape that let
/// the defect this file is about survive, so the arms moved to the regime where the pass still
/// happens and the floor below asserts it happened. The claim is unchanged and #1930's m8 mutant
/// -- a sampler that ignores its batch limit and takes every bucket -- is still the thing it
/// catches.
#[test]
fn the_whole_store_pass_does_not_shrink_when_the_batch_does() {
    fn arm(batch_limit: usize) -> RoundCost {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = evict_engine(dir.path());
        seed_with_a_slab_roll(&engine, GUARD_SMALL);
        one_round(&engine, GUARD_SMALL, batch_limit, true)
    }

    let one = arm(1);
    let many = arm(BATCH);
    println!(
        "  batch 1: {} victims, {} model-map visits | batch {BATCH}: {} victims, {} visits",
        one.victims, one.model_visits, many.victims, many.model_visits
    );

    // CONTROL: the two arms really did ask for different amounts of work.
    assert!(
        many.victims > one.victims,
        "both arms chose the same number of victims ({} and {}), so this compares nothing",
        one.victims,
        many.victims
    );
    // VACUITY FLOOR: both arms reached the pass at all. Without this the equality below is
    // satisfied by 0 == 0, which is what a round that never derives reads.
    assert_eq!(
        (one.derivations, many.derivations),
        (1, 1),
        "the arms ran {} and {} whole-store derivation(s); this test is about the size of a pass \
         that happens",
        one.derivations,
        many.derivations
    );
    assert_eq!(
        one.model_visits, GUARD_SMALL as u64,
        "the one-victim arm visited {} model-map addresses over a {GUARD_SMALL}-record store; one \
         whole-store pass is {GUARD_SMALL} exactly",
        one.model_visits
    );
    assert_eq!(
        one.model_visits, many.model_visits,
        "a round for {} victim(s) visited {} model-map addresses and a round for {} victims \
         visited {}; if these differ the pass is proportional to the batch after all",
        one.victims, one.model_visits, many.victims, many.model_visits
    );
}

// -------------------------------------------------------------------------------------------
// THE LARGE-STORE TABLE, BOTH REGIMES, AND THE INDEPENDENT RESIDUAL.
// -------------------------------------------------------------------------------------------

/// WHAT ONE ROUND COSTS AT 20,000 AND 200,000 OBJECTS, IN BOTH VICTIM REGIMES.
///
/// TWO REGIMES, because the same defect reports opposite things in them. With a FIXED batch
/// against a growing store the per-round cost grows and the per-victim cost grows with it. With a
/// batch PROPORTIONAL to the store the per-round cost grows and the per-victim cost is flat -- a
/// perfectly healthy-looking reading for the same code. Measuring only the second is how a
/// whole-store term reports as bounded.
///
/// Allocations rather than a clock for the attribution rows: this box sits between load 4 and 30
/// for hours and a wall time taken on it describes the box. The counters are clock-free anyway.
///
/// Only compiled with `alloc-probe`: without the counting allocator every allocation row reads
/// zero, which is indistinguishable from "counted, and the answer is zero". The canary asserts
/// the allocator is installed before any allocation number is believed, and the PLANTED MARKERS
/// below prove the span recovers a known quantity exactly.
#[cfg(feature = "alloc-probe")]
#[test]
fn what_an_eviction_round_costs_on_a_large_store_in_both_victim_regimes() {
    let canary = crate::alloc_probe::Probe::start();
    let sink: Vec<u8> = Vec::with_capacity(8192);
    assert!(
        canary.stop().allocs > 0,
        "counting allocator not installed despite the feature being on; every allocation row \
         below would read zero and look like a bounded path"
    );
    drop(sink);

    /// THE PLANT. A pre-reserved vector so the pushes allocate nothing, then exactly this many
    /// boxes inside the span. If the span cannot recover a quantity it was handed, it cannot be
    /// trusted to report the quantity it was not.
    const PLANTED: u64 = 37;
    let mut markers: Vec<Box<u64>> = Vec::with_capacity(PLANTED as usize);
    let probe = crate::alloc_probe::Probe::start();
    for index in 0..PLANTED {
        markers.push(Box::new(index));
    }
    let planted = probe.stop().allocs;
    assert_eq!(
        planted, PLANTED,
        "the allocation span recovered {planted} of {PLANTED} planted markers; a span that \
         cannot count what it was given cannot be used as the outside number below"
    );
    drop(markers);

    struct Arm {
        cost: RoundCost,
        /// The OUTSIDE number: process-wide allocation calls across the whole round.
        whole_round_allocs: u64,
        /// The attributed rows, each probed on its own afterwards.
        inspect: u64,
        resident: u64,
        select: u64,
        release: u64,
        /// Wall time the shipped-setting release held the shard-table write guard.
        hold_ns: u128,
        /// Wall time one whole-store model-map pass takes under that same guard: the term the
        /// release used to pay before any candidate had been looked at.
        walk_ns: u128,
    }

    fn arm(records: usize, batch_limit: usize) -> Arm {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = evict_engine(dir.path());
        seed_with_a_slab_roll(&engine, records);

        // The round, probed whole. This counter is the process's, not a sum of the rows below.
        let probe = crate::alloc_probe::Probe::start();
        let cost = one_round(&engine, records, batch_limit, false);
        let whole_round_allocs = probe.stop().allocs;

        // The phases, each on its own, on the same engine right after.
        let inspect = crate::alloc_probe::Probe::start();
        let cache_report = engine.storage_cache_inspection_report(1);
        let inspect = inspect.stop().allocs;

        let resident = crate::alloc_probe::Probe::start();
        let _ = engine.bucket_index_resident_bytes(1);
        let resident = resident.stop().allocs;

        let cache_by_bucket = cache_report
            .bucket_summaries
            .iter()
            .map(|summary| (summary.routing_bucket, summary.clone()))
            .collect::<BTreeMap<_, _>>();

        let select = crate::alloc_probe::Probe::start();
        let picked = engine.sampled_eviction_victims(1, batch_limit, &cache_by_bucket);
        let select = select.stop().allocs;

        let candidates = picked
            .iter()
            .map(|victim| victim.routing_bucket)
            .collect::<Vec<_>>();
        let release = crate::alloc_probe::Probe::start();
        // THE SHARD-TABLE WRITE GUARD, ON ITS OWN CLOCK. Started after the guard is in hand and
        // read before it is dropped, so it spans exactly the interval a serving read queues
        // behind. Two numbers, because the question is what the WALK costs in there:
        //
        //   hold_ns  the shipped-setting release itself -- sixteen candidates, each refused on
        //            its own bucket's state, and no derivation;
        //   walk_ns  one bucket-scoped model-map pass, under the same guard, on the same shard,
        //            in the same process. That is the same `visit_model_live_blocks` traversal
        //            the derivation runs, with a routing-bucket `accept`, so it is a PROXY for
        //            the term that used to be inside `hold_ns` and is now not.
        //
        // Both are wall clock on a box that sits between load 4 and 30, so they are the weakest
        // rows in this file and nothing is asserted about them. The counters above are the claim.
        let (hold_ns, walk_ns) = {
            let mut shards = engine.shards.write().expect("shards lock poisoned");
            let Some(shard) = shards.get_mut(&1) else {
                panic!("shard 1 loaded");
            };
            let started = std::time::Instant::now();
            let _ = crate::engine::storage_bucket_internals::release_bucket_blocks(
                shard,
                &candidates,
            );
            let hold_ns = started.elapsed().as_nanos();
            let started = std::time::Instant::now();
            let walked = crate::engine::storage_bucket_internals::
                collect_model_live_block_entries_in_bucket(shard, candidates[0]);
            let walk_ns = started.elapsed().as_nanos();
            assert!(
                !walked.is_empty() || cost.records == 0,
                "the proxy walk emitted nothing, so it did not run the traversal it stands in for"
            );
            (hold_ns, walk_ns)
        };
        let release = release.stop().allocs;

        Arm {
            cost,
            whole_round_allocs,
            inspect,
            resident,
            release,
            select,
            hold_ns,
            walk_ns,
        }
    }

    // REGIME ONE: a fixed batch, which is what ships.
    let fixed_small = arm(SMALL, BATCH);
    let fixed_large = arm(LARGE, BATCH);
    // REGIME TWO: a batch proportional to the store, so the victim count grows with the corpus.
    let prop_small = arm(SMALL, SMALL / 1_000);
    let prop_large = arm(LARGE, LARGE / 1_000);

    let corpus = ratio(SMALL as u64, LARGE as u64);
    for (label, small, large) in [
        ("FIXED batch (shipped)", &fixed_small, &fixed_large),
        ("PROPORTIONAL batch", &prop_small, &prop_large),
    ] {
        let small_named = small.inspect + small.resident + small.select + small.release;
        let large_named = large.inspect + large.resident + large.select + large.release;
        let small_residual = small.whole_round_allocs as i64 - small_named as i64;
        let large_residual = large.whole_round_allocs as i64 - large_named as i64;
        println!(
            "\n  {label}, corpus {corpus:.2}x\n\
             \n                                  {SMALL:>10} {LARGE:>12}      ratio\n\
               routing buckets               {:>10} {:>12}   {:>8.2}x\n\
               victims chosen                {:>10} {:>12}   {:>8.2}x\n\
               model-map addresses visited   {:>10} {:>12}   {:>8.2}x\n\
               model-map pages emitted       {:>10} {:>12}   {:>8.2}x\n\
               bucket-index entries visited  {:>10} {:>12}   {:>8.2}x\n\
               live-page entries scanned     {:>10} {:>12}\n\
               recency entries cloned        {:>10} {:>12}\n\
               blocks released               {:>10} {:>12}\n\
               VISITS PER VICTIM             {:>10} {:>12}   {:>8.2}x\n\
             \n    where the round's allocations go\n\
               cache inspection report       {:>10} {:>12}   {:>8.2}x\n\
               bucket index resident bytes   {:>10} {:>12}   {:>8.2}x\n\
               sampled victim selection      {:>10} {:>12}   {:>8.2}x\n\
               bucket release                {:>10} {:>12}   {:>8.2}x\n\
               whole round (OUTSIDE counter) {:>10} {:>12}   {:>8.2}x\n\
               RESIDUAL = outside - rows     {:>10} {:>12}\n\
               residual per record           {:>10.4} {:>12.4}\n",
            small.cost.buckets,
            large.cost.buckets,
            ratio(small.cost.buckets as u64, large.cost.buckets as u64),
            small.cost.victims,
            large.cost.victims,
            ratio(small.cost.victims as u64, large.cost.victims as u64),
            small.cost.model_visits,
            large.cost.model_visits,
            ratio(small.cost.model_visits, large.cost.model_visits),
            small.cost.model_emitted,
            large.cost.model_emitted,
            ratio(small.cost.model_emitted, large.cost.model_emitted),
            small.cost.resident_visits,
            large.cost.resident_visits,
            ratio(small.cost.resident_visits, large.cost.resident_visits),
            small.cost.live_scan,
            large.cost.live_scan,
            small.cost.recency_cloned,
            large.cost.recency_cloned,
            small.cost.released_blocks,
            large.cost.released_blocks,
            small.cost.model_visits / small.cost.victims.max(1) as u64,
            large.cost.model_visits / large.cost.victims.max(1) as u64,
            ratio(
                small.cost.model_visits / small.cost.victims.max(1) as u64,
                large.cost.model_visits / large.cost.victims.max(1) as u64
            ),
            small.inspect,
            large.inspect,
            ratio(small.inspect, large.inspect),
            small.resident,
            large.resident,
            ratio(small.resident, large.resident),
            small.select,
            large.select,
            ratio(small.select, large.select),
            small.release,
            large.release,
            ratio(small.release, large.release),
            small.whole_round_allocs,
            large.whole_round_allocs,
            ratio(small.whole_round_allocs, large.whole_round_allocs),
            small_residual,
            large_residual,
            small_residual as f64 / SMALL as f64,
            large_residual as f64 / LARGE as f64,
        );

        // THE RESIDUAL, asserted per record at both sizes. An unattributed term that grows with
        // the store shows here as a residual per record that does not fall; a table whose rows
        // add up to the outside number has nothing left over to hide a walk in.
        assert!(
            small.whole_round_allocs > 0 && large.whole_round_allocs > 0,
            "{label}: the outside counter read zero, so the residual below is an identity rather \
             than a reading"
        );
        let residual_per_record_small = small_residual as f64 / SMALL as f64;
        let residual_per_record_large = large_residual as f64 / LARGE as f64;
        assert!(
            residual_per_record_large <= residual_per_record_small.abs().max(0.01) * 4.0,
            "{label}: the unattributed residual per record ROSE from \
             {residual_per_record_small:.4} to {residual_per_record_large:.4}; a phase that grows \
             with the store and is not named in the rows above is what that reads like"
        );
    }

    // THE REGIME CONTRAST, which is the whole reason both are run.
    assert_eq!(
        fixed_small.cost.victims, fixed_large.cost.victims,
        "the fixed-batch regime did not hold its victim count ({} -> {}), so it is not the fixed \
         regime",
        fixed_small.cost.victims, fixed_large.cost.victims
    );
    assert!(
        prop_large.cost.victims > prop_small.cost.victims,
        "the proportional regime did not grow its victim count ({} -> {}), so both regimes are \
         the same measurement",
        prop_small.cost.victims,
        prop_large.cost.victims
    );
    let fixed_per_victim = ratio(
        fixed_small.cost.model_visits / fixed_small.cost.victims.max(1) as u64,
        fixed_large.cost.model_visits / fixed_large.cost.victims.max(1) as u64,
    );
    let prop_per_victim = ratio(
        prop_small.cost.model_visits / prop_small.cost.victims.max(1) as u64,
        prop_large.cost.model_visits / prop_large.cost.victims.max(1) as u64,
    );
    println!(
        "\n  PER-VICTIM COST ACROSS THE DECADE: fixed batch {fixed_per_victim:.2}x, proportional \
         batch {prop_per_victim:.2}x -- the same code, read two ways.\n"
    );
    println!(
        "  THE GUARD HOLD, wall clock and therefore the weakest rows here. A shipped-setting \
         release of {BATCH} candidates held the shard-table write guard for {:.3} ms at {SMALL} \
         records and {:.3} ms at {LARGE}; one whole-store model-map pass under that same guard, \
         on the same shard and in the same process, takes {:.3} ms and {:.3} ms. The second pair \
         is what the first pair used to include.\n",
        fixed_small.hold_ns as f64 / 1e6,
        fixed_large.hold_ns as f64 / 1e6,
        fixed_small.walk_ns as f64 / 1e6,
        fixed_large.walk_ns as f64 / 1e6
    );
    // THE REGIME CONTRAST MOVED TO AN ALWAYS-ON TEST, AND IT DID NOT WEAKEN.
    //
    // As written, this asserted `fixed_per_victim > prop_per_victim` on rounds taken at the
    // shipped `dump_before_evict` false. The release now derives lazily and no candidate at that
    // setting reaches the model-map comparison, so both regimes read ZERO here and the inequality
    // is satisfied by neither side happening. That would be a guard passing for the wrong reason,
    // so the growth claim is asserted where the pass still exists --
    // `the_surviving_release_pass_is_still_whole_store_in_both_batch_regimes`, which runs on
    // RELEASING rounds in BOTH regimes at two sizes and is in the ORDINARY gate rather than
    // behind `alloc-probe`. What stays here is the sharper statement for the setting that ships:
    // NEITHER regime pays the whole-store pass, and that is asserted as an exact zero on all four
    // arms rather than as a comparison between two of them.
    assert_eq!(
        (
            fixed_small.cost.model_visits,
            fixed_large.cost.model_visits,
            prop_small.cost.model_visits,
            prop_large.cost.model_visits
        ),
        (0, 0, 0, 0),
        "at the shipped setting the four arms visited {} / {} / {} / {} model-map addresses; \
         every candidate is refused on its own bucket's state in all four, so none of them should \
         reach a derivation at all",
        fixed_small.cost.model_visits,
        fixed_large.cost.model_visits,
        prop_small.cost.model_visits,
        prop_large.cost.model_visits
    );
    assert_eq!(
        (
            fixed_small.cost.derivations,
            fixed_large.cost.derivations,
            prop_small.cost.derivations,
            prop_large.cost.derivations
        ),
        (0, 0, 0, 0),
        "the four arms ran {} / {} / {} / {} whole-store derivation(s) at the shipped setting",
        fixed_small.cost.derivations,
        fixed_large.cost.derivations,
        prop_small.cost.derivations,
        prop_large.cost.derivations
    );
}

/// CAN A ROUND KEEP UP WITH THE STORE IT IS DRAINING? Measured over real rounds, not extrapolated.
///
/// The question a small store cannot answer. A round gives back at most `batch_limit` buckets
/// however large the store is; what it COSTS is proportional to the store. So the cost of one
/// unit of relief is the store divided by the batch, and draining a store of B buckets takes
/// B/batch rounds each visiting every live address -- the sum, not the count, is what grows.
///
/// WHAT IS MEASURED HERE, with the dump on so the actuator actually releases:
///   * how many blocks a round releases, over `ROUNDS` consecutive rounds;
///   * how many model-map addresses those rounds visited in total;
///   * how many writes fit in the time one round took, measured on the same engine in the same
///     process so the box's load divides out of the comparison.
///
/// HOW MANY ROUNDS: `ROUNDS`, stated rather than implied. That is enough to show the per-round
/// release is bounded and the per-round cost is not; it is NOT enough to establish an asymptote,
/// and this test does not claim one. What it claims is the ratio between what a round gives back
/// and what accrues while it runs, at two sizes.
#[cfg(feature = "alloc-probe")]
#[test]
fn an_eviction_round_cannot_keep_up_with_the_store_it_is_draining() {
    // HOW MANY, stated rather than implied. Each round here runs with the dump on, and a dump
    // manifest embeds a whole-shard index image, so the arm is bounded by what that costs at
    // 200,000 records rather than by what would settle an asymptote. Twelve rounds establishes
    // that the release per round is capped and the cost per round is not; it does not establish
    // a limit, and nothing below claims one.
    const ROUNDS: usize = 12;

    struct Drain {
        records: usize,
        buckets: usize,
        rounds: usize,
        blocks_released: usize,
        model_visits: u64,
        round_nanos: u128,
        write_nanos: u128,
    }

    fn drain(records: usize) -> Drain {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = evict_engine(dir.path());
        seed_with_a_slab_roll(&engine, records);
        let buckets = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            shards
                .get(&1)
                .map(|shard| shard.bucket_index.bucket_map.len())
                .unwrap_or_default()
        };

        let mut blocks_released = 0usize;
        let mut model_visits = 0u64;
        let mut round_nanos = 0u128;
        let mut rounds = 0usize;
        for _ in 0..ROUNDS {
            let cost = one_round(&engine, records, BATCH, true);
            blocks_released += cost.released_blocks;
            model_visits += cost.model_visits;
            round_nanos += cost.round_nanos;
            rounds += 1;
        }

        // WHAT ACCRUES, on the same engine and in the same process, so the box's load is common
        // to both numbers and divides out of the ratio below. One write puts one page into the
        // index, which is the unit a release gives back.
        const WRITES: usize = 200;
        let started = std::time::Instant::now();
        for index in 0..WRITES {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("evict-keepup-{index:09}"),
                    value: vec![b'v'; 128],
                },
            });
            assert!(response.status.ok, "keep-up write failed: {:?}", response.status);
        }
        let write_nanos = started.elapsed().as_nanos() / WRITES as u128;

        Drain {
            records,
            buckets,
            rounds,
            blocks_released,
            model_visits,
            round_nanos,
            write_nanos,
        }
    }

    let small = drain(SMALL);
    let large = drain(LARGE);

    for arm in [&small, &large] {
        let per_round_release = arm.blocks_released as f64 / arm.rounds as f64;
        let per_round_visits = arm.model_visits as f64 / arm.rounds as f64;
        let round_ns = arm.round_nanos as f64 / arm.rounds as f64;
        let writes_per_round = round_ns / arm.write_nanos.max(1) as f64;
        println!(
            "\n  {} records, {} buckets, {} rounds run\n\
               blocks released per round        {per_round_release:>12.2}\n\
               model-map visits per round       {per_round_visits:>12.0}\n\
               visits per block released        {:>12.0}\n\
               one round                        {:>12.0} ns\n\
               one write                        {:>12.0} ns\n\
               writes that fit in one round     {writes_per_round:>12.1}\n\
               net index pages per round        {:>12.1}",
            arm.records,
            arm.buckets,
            arm.rounds,
            if arm.blocks_released == 0 {
                f64::INFINITY
            } else {
                arm.model_visits as f64 / arm.blocks_released as f64
            },
            round_ns,
            arm.write_nanos as f64,
            per_round_release - writes_per_round,
        );
    }

    // FLOORS, before the comparison means anything.
    assert!(
        small.blocks_released > 0 && large.blocks_released > 0,
        "one of the arms released nothing over {ROUNDS} rounds ({} and {}), so there is no rate \
         to compare against the write rate",
        small.blocks_released,
        large.blocks_released
    );
    assert!(
        large.buckets > small.buckets,
        "the two arms hold {} and {} buckets, so this is one store measured twice",
        small.buckets,
        large.buckets
    );

    // THE BOUNDED SIDE. A round's release is capped by the batch at both sizes -- that is the
    // half of the keep-up question that does not move.
    let small_per_round = small.blocks_released as f64 / small.rounds as f64;
    let large_per_round = large.blocks_released as f64 / large.rounds as f64;
    assert!(
        large_per_round <= BATCH as f64,
        "a round released {large_per_round:.2} blocks against a batch limit of {BATCH}; the cap \
         is what makes the relief bounded and the comparison below meaningful"
    );

    // THE UNBOUNDED SIDE, in a unit no clock touches: what a round spends per unit of relief.
    let small_cost = small.model_visits as f64 / small.blocks_released as f64;
    let large_cost = large.model_visits as f64 / large.blocks_released as f64;
    println!(
        "\n  VISITS PER BLOCK RELEASED: {small_cost:.0} at {} records, {large_cost:.0} at {} -- \
         {:.2}x over a {:.2}x corpus. A round gives back at most {BATCH} buckets at either size, \
         so the price of one unit of relief is the store.\n",
        small.records,
        large.records,
        large_cost / small_cost,
        ratio(SMALL as u64, LARGE as u64),
    );
    assert!(
        large_cost > small_cost * 2.0,
        "the cost of releasing one block was {small_cost:.0} visits at {} records and \
         {large_cost:.0} at {}; this guard records that it tracks the store, and a flat reading \
         means the whole-store pass has been bounded",
        small.records,
        large.records
    );
    assert!(
        small_per_round <= BATCH as f64,
        "the small arm released {small_per_round:.2} blocks per round against a batch of {BATCH}, \
         so the two arms are not capped the same way"
    );
}

// -------------------------------------------------------------------------------------------
// THE WHOLE-STORE PASS IS NOW PAID ONLY WHEN A CANDIDATE ASKS FOR IT.
//
// #1930 measured the pass and left it: `release_bucket_blocks` ran `accept` on every live address
// in the shard -- 200,000 at 200,000 records -- to answer a question about at most `batch_limit`
// buckets, and it ran it BEFORE any candidate had been examined. At the shipped
// `eviction_dump_before_evict` false every candidate is then refused on `bucket_dirty`, so the
// derived map was built and discarded whole.
//
// Every term below the derivation reads the CANDIDATE's own state, so the derivation is now
// memoized and reached only by a candidate that survives all of them. What that does NOT do is
// make the surviving pass proportional to the batch, and
// `the_surviving_release_pass_is_still_whole_store_in_both_batch_regimes` pins that so the
// reduction cannot later be read as a fix for the general case.
// -------------------------------------------------------------------------------------------

/// Markers planted into the corpus so the visit counter's denominator can be recovered exactly.
const PLANTED_MARKERS: usize = 37;

/// Live model-map pages the shard holds, counted by a DIFFERENT walk from the one under test.
fn live_pages_total(engine: &TemporalEngine) -> u64 {
    live_pages_by_key(engine).values().map(|n| *n as u64).sum()
}

/// Write `count` extra live string keys under a prefix the corpus does not use.
///
/// One live model-map address each, so the visit counter must read the corpus PLUS these. The
/// prefix is disjoint from [`scale_key`]'s, so no marker can collide with a corpus key and be
/// counted once for two records.
fn plant_markers(engine: &TemporalEngine, count: usize) {
    let commands = (0..count)
        .map(|marker| Command::StringSet {
            key: format!("evict-marker-{marker:09}"),
            value: vec![b'm'; 128],
        })
        .collect::<Vec<_>>();
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands,
    });
    assert!(
        response.status.ok,
        "planting {count} markers failed: {:?}",
        response.status
    );
}

/// THE INSTRUMENT CAN SEE A WALK, AND THE DENOMINATOR IT COUNTS AGAINST IS RECOVERABLE.
///
/// Read this before any of the zeros below are believed. The claim of the two tests that follow
/// is that a counter reads ZERO, and a counter wired to nothing reads zero at every store size
/// and in every configuration -- so the first thing asserted here is the OTHER arm: a round that
/// does reach the derivation charges the counter exactly the number of live model-map pages the
/// shard holds, counted by a separate walk.
///
/// PLANTED MARKERS. The corpus is `GUARD_SMALL` records and then [`PLANTED_MARKERS`] more under a
/// disjoint prefix. The instrument must recover the planted count exactly: live pages minus the
/// corpus is asserted EQUAL to 37, not merely positive, and the visit count is asserted EQUAL to
/// live pages, not within a tolerance. An instrument that under-counted by one map, or that
/// charged what the walk EMITS instead of what it VISITS -- which is the mistake that let this
/// defect live -- fails both.
#[test]
fn the_release_visit_counter_can_see_a_walk_and_recovers_its_planted_markers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = evict_engine(dir.path());
    seed_with_a_slab_roll(&engine, GUARD_SMALL);
    plant_markers(&engine, PLANTED_MARKERS);

    // Taken BEFORE the measured window: this walk charges the same counter.
    let live_pages = live_pages_total(&engine);

    let releasing = one_round(&engine, GUARD_SMALL, BATCH, true);

    println!(
        "  canary: corpus {GUARD_SMALL} + {PLANTED_MARKERS} markers -> {live_pages} live pages; \
         releasing round visited {} addresses, ran {} derivation(s), released {} block(s)",
        releasing.model_visits, releasing.derivations, releasing.released_blocks
    );

    assert_eq!(
        live_pages,
        (GUARD_SMALL + PLANTED_MARKERS) as u64,
        "the fixture holds {live_pages} live model-map pages, not the {} it was asked for; the \
         planted markers are what makes the visit denominator recoverable and they are not all \
         there",
        GUARD_SMALL + PLANTED_MARKERS
    );
    assert_eq!(
        live_pages.saturating_sub(GUARD_SMALL as u64),
        PLANTED_MARKERS as u64,
        "recovered {} planted markers, not {PLANTED_MARKERS}",
        live_pages.saturating_sub(GUARD_SMALL as u64)
    );
    assert_eq!(
        releasing.derivations, 1,
        "a releasing round ran {} whole-store derivation(s); the arm that proves the counter is \
         alive has to be an arm where the walk actually happens",
        releasing.derivations
    );
    assert!(
        releasing.released_blocks > 0,
        "the dump-on round released {} blocks, so this arm did not reach the model-map \
         comparison and cannot serve as the positive control",
        releasing.released_blocks
    );
    assert_eq!(
        releasing.model_visits, live_pages,
        "the round visited {} model-map addresses against {live_pages} live pages. The identity \
         is EXACT and deliberately not a tolerance: one pass over the maps, so a second pass \
         added later shows up as a whole extra store rather than inside a margin",
        releasing.model_visits
    );
}

/// THE CLAIM. A round that refuses every candidate no longer walks the model maps at all.
///
/// Two corpus sizes a factor of four apart, and BOTH ARMS at each size with the control named:
///
///   * REFUSING (`eviction_dump_before_evict` false, which is what ships) -- every candidate is
///     refused on `bucket_dirty`, a term that reads the candidate's own state, so no candidate
///     reaches the model-map comparison. Visits and derivations are asserted EQUAL to zero at
///     both sizes.
///   * RELEASING (dump on) -- the control. Candidates survive, one derivation runs, and it visits
///     exactly the live pages. Asserted EQUAL at each size rather than as a ratio, and asserted
///     to GROW with the corpus, so the refusing arm's zeros cannot be produced by a dead counter.
///
/// The test fails if the two arms agree. That is the assertion that matters: a mutant that stops
/// the counter being charged makes both arms read zero and satisfies the headline claim perfectly.
#[test]
fn a_round_that_refuses_every_candidate_no_longer_walks_the_model_maps() {
    /// One size, both arms, on two engines seeded identically.
    fn at(records: usize) -> (RoundCost, RoundCost, u64) {
        let refusing = {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = evict_engine(dir.path());
            seed_with_a_slab_roll(&engine, records);
            one_round(&engine, records, BATCH, false)
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = evict_engine(dir.path());
        seed_with_a_slab_roll(&engine, records);
        let live_pages = live_pages_total(&engine);
        let releasing = one_round(&engine, records, BATCH, true);
        (refusing, releasing, live_pages)
    }

    let (small_refusing, small_releasing, small_live) = at(GUARD_SMALL);
    let (large_refusing, large_releasing, large_live) = at(GUARD_LARGE);

    println!(
        "\n  corpus {:.2}x ({GUARD_SMALL} -> {GUARD_LARGE} records)\n",
        large_live as f64 / small_live as f64
    );
    println!(
        "{:<38}{:>12}{:>13}{:>12}",
        "", GUARD_SMALL, GUARD_LARGE, "ratio"
    );
    for (label, small, large) in [
        (
            "REFUSING arm: model-map visits",
            small_refusing.model_visits,
            large_refusing.model_visits,
        ),
        (
            "REFUSING arm: derivations",
            small_refusing.derivations,
            large_refusing.derivations,
        ),
        (
            "CONTROL arm: model-map visits",
            small_releasing.model_visits,
            large_releasing.model_visits,
        ),
        (
            "CONTROL arm: derivations",
            small_releasing.derivations,
            large_releasing.derivations,
        ),
        (
            "CONTROL arm: live pages in the shard",
            small_live,
            large_live,
        ),
    ] {
        println!(
            "{label:<38}{small:>12}{large:>13}{:>12.2}x",
            ratio(small, large)
        );
    }

    for (records, cost) in [
        (GUARD_SMALL, &small_refusing),
        (GUARD_LARGE, &large_refusing),
    ] {
        assert!(
            cost.victims > 0,
            "{records} records: the refusing round chose {} victims, so there was nothing to \
             refuse and the zero below is vacuous",
            cost.victims
        );
        assert_eq!(
            cost.refused, cost.victims,
            "{records} records: {} of {} candidates were refused; this arm's claim is that ALL \
             of them are, on a term that needs no model-map walk",
            cost.refused, cost.victims
        );
        assert_eq!(
            cost.released_blocks, 0,
            "{records} records: the shipped-setting round released {} blocks",
            cost.released_blocks
        );
        assert_eq!(
            cost.derivations, 0,
            "{records} records: the refusing round ran {} whole-store derivation(s). Every term \
             that refused reads the candidate's own bucket, so none of them needed one",
            cost.derivations
        );
        assert_eq!(
            cost.model_visits, 0,
            "{records} records: the refusing round visited {} model-map addresses. Before this \
             change that number was the whole store, at every store size, to release nothing",
            cost.model_visits
        );
    }

    for (records, cost, live) in [
        (GUARD_SMALL, &small_releasing, small_live),
        (GUARD_LARGE, &large_releasing, large_live),
    ] {
        assert!(
            cost.released_blocks > 0,
            "{records} records: the CONTROL arm released {} blocks, so it never reached the \
             model-map comparison and cannot control anything",
            cost.released_blocks
        );
        assert_eq!(
            cost.derivations, 1,
            "{records} records: the CONTROL arm ran {} derivation(s), not the one pass the whole \
             batch shares",
            cost.derivations
        );
        assert_eq!(
            cost.model_visits, live,
            "{records} records: the CONTROL arm visited {} addresses against {live} live pages. \
             Exact identity, not a tolerance",
            cost.model_visits
        );
    }

    assert!(
        large_releasing.model_visits > small_releasing.model_visits,
        "the CONTROL arm visited {} addresses on a {GUARD_SMALL}-record store and {} on a \
         {GUARD_LARGE}-record one. If the surviving walk does not grow with the store then the \
         counter is dead, and a dead counter satisfies the REFUSING arm's zeros for free",
        small_releasing.model_visits,
        large_releasing.model_visits
    );
    assert!(
        large_refusing.model_visits < large_releasing.model_visits,
        "both arms report {} visits at {GUARD_LARGE} records, so the dump setting made no \
         difference to the walk and one of these two arms is not measuring what it says",
        large_refusing.model_visits
    );
}

/// THE REFUTATION, GUARDED. The pass that DOES run is still proportional to the store.
///
/// What the change above removes is a pass that nobody was going to read. It does not make
/// releasing N buckets cost N: when a candidate survives its own terms, the derivation still runs
/// `accept` on every live address in the shard, because the model maps are keyed by object, the
/// routing bucket is a field of the ADDRESS, and nothing in the tree indexes the other way --
/// `object_block_lookup` is derived from the very block index being checked, so it answers WITH
/// it rather than ABOUT it.
///
/// BOTH BATCH REGIMES, with the control arm asserted by name, exactly as #1930 framed them:
///
///   * a FIXED batch takes the same sixteen victims however large the store is, so the per-victim
///     cost tracks the corpus;
///   * a PROPORTIONAL batch takes victims in proportion, so the per-victim cost is flat -- the
///     same code, reading as a healthy system.
///
/// Both are asserted, so neither can be chosen by accident, and the test fails if the two regimes
/// report the same per-victim growth.
#[test]
fn the_surviving_release_pass_is_still_whole_store_in_both_batch_regimes() {
    /// Per-victim visits for a RELEASING round, at one size and one batch rule.
    fn per_victim(records: usize, batch_limit: usize) -> (u64, usize, f64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = evict_engine(dir.path());
        seed_with_a_slab_roll(&engine, records);
        let cost = one_round(&engine, records, batch_limit, true);
        assert!(
            cost.victims > 0,
            "{records} records, batch {batch_limit}: no victims, so per-victim is a division by \
             zero rather than a measurement"
        );
        assert_eq!(
            cost.derivations, 1,
            "{records} records, batch {batch_limit}: {} derivation(s); this arm is about the \
             pass that DOES run",
            cost.derivations
        );
        let victims = cost.victims;
        (
            cost.model_visits,
            victims,
            cost.model_visits as f64 / victims as f64,
        )
    }

    /// The proportional rule: one victim per hundred records, so the batch is a fixed share of
    /// the store rather than a constant.
    fn proportional_batch(records: usize) -> usize {
        records / 100
    }

    let corpus = GUARD_LARGE as f64 / GUARD_SMALL as f64;
    let (fixed_small_visits, fixed_small_victims, fixed_small) = per_victim(GUARD_SMALL, BATCH);
    let (fixed_large_visits, fixed_large_victims, fixed_large) = per_victim(GUARD_LARGE, BATCH);
    let (prop_small_visits, prop_small_victims, prop_small) =
        per_victim(GUARD_SMALL, proportional_batch(GUARD_SMALL));
    let (prop_large_visits, prop_large_victims, prop_large) =
        per_victim(GUARD_LARGE, proportional_batch(GUARD_LARGE));

    let fixed_growth = fixed_large / fixed_small;
    let prop_growth = prop_large / prop_small;

    println!("\n  corpus {corpus:.2}x, RELEASING rounds only (dump on)\n");
    println!(
        "{:<28}{:>12}{:>13}{:>12}",
        "", GUARD_SMALL, GUARD_LARGE, "ratio"
    );
    println!(
        "{:<28}{:>12}{:>13}{:>11.2}x",
        "FIXED batch: victims",
        fixed_small_victims,
        fixed_large_victims,
        fixed_large_victims as f64 / fixed_small_victims as f64
    );
    println!(
        "{:<28}{:>12}{:>13}{:>11.2}x",
        "FIXED batch: visits",
        fixed_small_visits,
        fixed_large_visits,
        ratio(fixed_small_visits, fixed_large_visits)
    );
    println!(
        "{:<28}{:>12.0}{:>13.0}{:>11.2}x",
        "FIXED batch: per victim", fixed_small, fixed_large, fixed_growth
    );
    println!(
        "{:<28}{:>12}{:>13}{:>11.2}x",
        "PROPORTIONAL: victims",
        prop_small_victims,
        prop_large_victims,
        prop_large_victims as f64 / prop_small_victims as f64
    );
    println!(
        "{:<28}{:>12}{:>13}{:>11.2}x",
        "PROPORTIONAL: visits",
        prop_small_visits,
        prop_large_visits,
        ratio(prop_small_visits, prop_large_visits)
    );
    println!(
        "{:<28}{:>12.0}{:>13.0}{:>11.2}x",
        "PROPORTIONAL: per victim", prop_small, prop_large, prop_growth
    );

    assert_eq!(
        fixed_small_victims, fixed_large_victims,
        "the FIXED arm's own control: its victim count moved from {fixed_small_victims} to \
         {fixed_large_victims}, so it is not the fixed-batch regime and its per-victim growth \
         means nothing"
    );
    assert!(
        prop_large_victims > prop_small_victims,
        "the PROPORTIONAL arm's own control: its victim count did NOT move ({prop_small_victims} \
         -> {prop_large_victims}), so it is the fixed regime under another name"
    );
    assert!(
        fixed_growth > corpus * 0.8,
        "the surviving pass's per-victim cost grew {fixed_growth:.2}x over a {corpus:.2}x corpus \
         under a fixed batch. This test exists to REFUTE a claim, not to support one: if this \
         number has become flat then the walk was made proportional to the batch and the \
         refutation in this file is stale"
    );
    assert!(
        prop_growth < 1.5,
        "the same pass read {prop_growth:.2}x per victim under a PROPORTIONAL batch; the point \
         of printing both is that the same code reads healthy in one regime and not the other"
    );
    assert!(
        fixed_growth > prop_growth * 2.0,
        "the two regimes reported the same per-victim growth ({fixed_growth:.2}x and \
         {prop_growth:.2}x), so the arms are not distinguishing anything"
    );
}

/// DIRECTION. A mixed batch releases exactly the clean buckets and leaves every other one WHOLE.
///
/// Releasing a bucket that should have been kept is data loss and it is silent; failing to
/// release one is space held. So the set released is compared ELEMENT BY ELEMENT against an
/// oracle computed in this test from the shard's own state before the call, and every bucket the
/// oracle says should survive is asserted to hold a page count EQUAL to what it held before --
/// not merely non-zero, which a bucket that lost one of its two pages would satisfy.
///
/// The fixture is mixed by production code rather than by poking flags: half the candidates are
/// dumped through `create_bucket_dump_manifest` and cleared through
/// `clear_dumped_bucket_dirty_state`, which is exactly what `dump_before_evict` does, and the
/// other half are left dirty. Both halves are asserted non-empty, because a batch that was all
/// one or all the other would pass this test while proving only one direction.
///
/// It is also the other half of the lazy derivation: one candidate here DOES reach the model-map
/// comparison, so the memoized pass is asserted to run exactly once and to release the right set.
#[test]
fn releasing_a_mixed_batch_releases_exactly_the_clean_buckets_and_leaves_the_rest_whole() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = evict_engine(dir.path());
    seed_with_a_slab_roll(&engine, GUARD_SMALL);

    // Sixteen routing buckets that actually hold resident pages, in a stable order.
    let candidates: Vec<u32> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        shard
            .bucket_index
            .bucket_map
            .iter()
            .filter(|(_, bucket)| !bucket.block_index.is_empty())
            .map(|(routing_bucket, _)| *routing_bucket)
            .take(BATCH)
            .collect()
    };
    assert_eq!(
        candidates.len(),
        BATCH,
        "the fixture offered {} candidate buckets with resident pages, not {BATCH}",
        candidates.len()
    );

    // HALF OF THEM MADE CLEAN, the way the dump-on eviction path makes them clean.
    let to_dump = candidates[..BATCH / 2].to_vec();
    let manifest = engine
        .create_bucket_dump_manifest(1, to_dump.clone())
        .expect("dumping half the candidates");
    engine.clear_dumped_bucket_dirty_state(1, &manifest);

    // THE ORACLE, and the before-state, read directly off the shard and independently of what the
    // release is about to do. The terms are the release's own residency and per-block terms.
    let (oracle_released, oracle_dirty, blocks_before, objects_before) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        let mut released = Vec::new();
        let mut dirty = Vec::new();
        let mut blocks = BTreeMap::new();
        let mut objects = BTreeMap::new();
        for routing_bucket in candidates.iter().copied().collect::<BTreeSet<_>>() {
            let bucket = shard
                .bucket_index
                .bucket_map
                .get(&routing_bucket)
                .expect("candidate bucket present");
            blocks.insert(routing_bucket, bucket.block_index.len());
            objects.insert(routing_bucket, bucket.object_index.len());
            let pages_clean = bucket
                .block_index
                .values()
                .all(|page| !page.dirty && !page.deleted);
            if bucket.in_memory()
                && !bucket.loading()
                && !bucket.deleted()
                && !bucket.block_index.is_empty()
                && pages_clean
            {
                if bucket.dirty() {
                    dirty.push(routing_bucket);
                } else {
                    released.push(routing_bucket);
                }
            } else {
                dirty.push(routing_bucket);
            }
        }
        (released, dirty, blocks, objects)
    };

    let pages_before = live_pages_by_key(&engine);

    crate::engine::reset_model_map_addresses_visited();
    crate::engine::reset_bucket_release_model_derivations();
    let outcome = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 loaded");
        release_bucket_blocks(shard, &candidates)
    };
    let derivations = crate::engine::bucket_release_model_derivations();
    let pages_after = live_pages_by_key(&engine);

    println!(
        "  mixed batch: {} candidates, oracle says release {:?} and refuse {} on dirt; the \
         actuator released {:?} in {derivations} derivation(s)",
        candidates.len(),
        oracle_released,
        oracle_dirty.len(),
        outcome.released_buckets
    );

    assert!(
        !oracle_released.is_empty(),
        "the oracle expects no bucket to be released, so the set equality below holds vacuously"
    );
    assert!(
        !oracle_dirty.is_empty(),
        "the oracle expects every candidate to be released, so nothing in this batch tests the \
         direction that keeps data"
    );
    assert_eq!(
        outcome.refusals.lookup_not_local, 0,
        "{} candidates were refused on lookup locality, a term the oracle does not model; the \
         oracle is only exact on a healthy fixture",
        outcome.refusals.lookup_not_local
    );
    assert_eq!(
        outcome.refusals.model_map_disagreement, 0,
        "{} candidates were refused because the model maps disagreed with what was resident; on \
         this fixture that is a fixture fault, not a finding",
        outcome.refusals.model_map_disagreement
    );
    assert_eq!(
        outcome.released_buckets, oracle_released,
        "the actuator released {:?}; the oracle, reading the same terms off the same shard state, \
         says {oracle_released:?}. Compared element by element and in order, because a release of \
         the wrong bucket and a release of the right number of buckets look the same to a count",
        outcome.released_buckets
    );
    assert_eq!(
        outcome.refusals.bucket_dirty,
        oracle_dirty.len(),
        "{} candidates were refused on `bucket_dirty`; the oracle says {}",
        outcome.refusals.bucket_dirty,
        oracle_dirty.len()
    );
    assert_eq!(
        outcome.refused_buckets,
        oracle_dirty.len(),
        "{} candidates were refused in total against {} refused on dirt, so some other term fired \
         and the oracle is not describing this run",
        outcome.refused_buckets,
        oracle_dirty.len()
    );
    assert_eq!(
        derivations, 1,
        "the mixed batch ran {derivations} whole-store derivation(s); one candidate survives its \
         own terms here, so the memoized pass must run exactly once for the whole batch"
    );

    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 loaded");
        for routing_bucket in &oracle_dirty {
            let bucket = shard
                .bucket_index
                .bucket_map
                .get(routing_bucket)
                .expect("refused bucket still present");
            let before = blocks_before[routing_bucket];
            assert_eq!(
                bucket.block_index.len(),
                before,
                "bucket {routing_bucket} was refused but now holds {} resident pages against \
                 {before} before the release. EQUAL, not merely non-zero: a bucket that kept one \
                 of its two pages would pass a presence check",
                bucket.block_index.len()
            );
            assert!(
                bucket.in_memory(),
                "bucket {routing_bucket} was refused but is no longer marked resident"
            );
        }
        for routing_bucket in &oracle_released {
            let bucket = shard
                .bucket_index
                .bucket_map
                .get(routing_bucket)
                .expect("released bucket still routable");
            assert_eq!(
                bucket.block_index.len(),
                0,
                "bucket {routing_bucket} was released but still holds {} resident pages",
                bucket.block_index.len()
            );
            let before = objects_before[routing_bucket];
            assert_eq!(
                bucket.object_index.len(),
                before,
                "bucket {routing_bucket} was released and its object index went from {before} to \
                 {}. A release KEEPS the object index -- it is what distinguishes a released \
                 bucket from one that holds nothing -- and the model maps do not record it, so \
                 nothing else in this file would see it go",
                bucket.object_index.len()
            );
        }
    }

    assert_eq!(
        pages_after.len(),
        pages_before.len(),
        "the shard held {} distinct live keys before the release and {} after",
        pages_before.len(),
        pages_after.len()
    );
    for (key, before) in &pages_before {
        let after = pages_after.get(key).copied().unwrap_or_default();
        assert_eq!(
            after, *before,
            "key {key} had {before} live pages before the release and {after} after"
        );
    }
}
