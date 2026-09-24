// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PAGE LIST OF ONE BUCKET, AS A CONTAINER.
//!
//! `BlockIndexMap::Many` held a `BTreeMap<u64, BlockIndex>` from #654 until #1963. This module is
//! the measurement that replaced it with a flat list, and the guards that make the replacement a
//! container change rather than a behaviour change.
//!
//! WHAT A TREE COSTS HERE. A `BTreeMap` leaf carries eleven value slots whether or not it fills
//! them. At a 104-byte page entry that is 16 + 11x8 + 11x104 = 1,248 bytes of node per eleven
//! pages, and the fill was measured at 63% -- so a page in a tree pays for about one and a half
//! slots. A list pays for its own entry and for whatever capacity slack the growth policy leaves,
//! and the growth policy is something this engine chooses. That is the whole of the argument, and
//! every figure below is a measurement of one side of it.
//!
//! WHAT THE LIST MUST NOT CHANGE, and what each guard here covers:
//!
//!   * ORDER. A `BTreeMap<u64, _>` walks in ascending key order and FIVE production readers take
//!     that order as given -- see `an_unsorted_page_list_would_reorder_every_walk_of_this_index`,
//!     which names them. The list is kept sorted by the same handle, so every walk yields the
//!     identical sequence. This is the central risk of the change and it is answered by the
//!     representation, not by a sort at each reader.
//!   * DUPLICATES. A map deduplicated by construction; a list does not.
//!     `a_second_insert_of_the_same_page_replaces_it_rather_than_adding_beside_it` drives the
//!     rewrite that would otherwise file a page twice, on both arms.
//!   * THE STORED SHAPE. `BlockIndexMap` serializes as the same map of rendered string keys, in
//!     the same string order, that it always did.
//!     `the_stored_page_index_is_byte_for_byte_what_the_tree_wrote` holds that with an injection
//!     control.
//!
//! WHAT THE FIXTURE HAS TO REACH. `TemporalEngine::load_shard` defaults the end routing bucket to
//! `u32::MAX`, and on the whole keyspace every key lands in a bucket of its own BY CONSTRUCTION --
//! no `Many` arm is ever built, so a measurement taken there cannot say anything about the shape
//! this module changes. `docs/runtime_tuning.md` tells an operator to set
//! `TS_SHARD_END_ROUTING_BUCKET=1023`. Both ranges are measured at both corpus sizes, and the
//! narrow arm ASSERTS it reached a multi-page bucket before any figure taken from it is believed.
#![allow(clippy::all)]
use super::*;
use crate::engine::state::{
    bisect_page, block_index_handle, find_page, page_lookup_entries_examined,
    reset_page_lookup_entries_examined, scan_page, BlockIndex, BlockIndexMap, BlockSlabLiveIndex,
    BucketNode,
};
use std::mem::size_of;

/// The shortest list the `Many` arm ever holds. A bucket with one page is held inline and a
/// bucket with none is `Empty`, so no shorter list exists to choose a strategy for.
const SHORTEST_MANY: usize = 2;
use std::collections::BTreeMap;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_BUCKET=1023`.
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses. Every key lands alone here by construction.
const WIDE_END: u32 = u32::MAX;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "flat-page-list".to_string(),
        shard_uri: "local://flat-page-list/1".to_string(),
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

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

fn seed_routed(engine: &TemporalEngine, count: usize) -> Vec<String> {
    let keys: Vec<String> = (0..count).map(|i| format!("flat-{i:06}")).collect();
    run_batch(
        engine,
        keys.iter()
            .map(|key| Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
    keys
}

// =============================================================================================
// 1. PRICE THE LENGTH FIRST: THE DISTRIBUTION, NEVER A MEAN
// =============================================================================================

/// Pages held per routing bucket, as bucket counts keyed by pages held.
#[derive(Debug, Default, Clone)]
struct Lengths {
    counts: BTreeMap<usize, usize>,
}

impl Lengths {
    fn buckets(&self) -> usize {
        self.counts.values().copied().sum()
    }

    fn pages(&self) -> usize {
        self.counts.iter().map(|(held, count)| held * count).sum()
    }

    fn widest(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    /// The length at or below which `fraction` of BUCKETS sit.
    ///
    /// Over buckets and not over pages, because the question a representation asks is "how long is
    /// the list I am about to walk", and every bucket asks it once.
    fn percentile(&self, fraction: f64) -> usize {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0;
        }
        let want = (fraction * buckets as f64).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (held, count) in &self.counts {
            seen += count;
            if seen >= want {
                return *held;
            }
        }
        self.widest()
    }

    /// Reported ALONGSIDE the percentiles and the maximum, never instead of them. A mean of 1.98
    /// in this engine once contained no bucket holding two.
    fn mean(&self) -> f64 {
        if self.buckets() == 0 {
            0.0
        } else {
            self.pages() as f64 / self.buckets() as f64
        }
    }

    fn report(&self, label: &str) {
        println!(
            "  {label}: {} buckets, {} pages | p50 {} p90 {} p99 {} MAX {} | mean {:.4}",
            self.buckets(),
            self.pages(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.widest(),
            self.mean()
        );
    }
}

/// Lengths of the `Many` arm only -- the lists a scan actually walks.
fn many_arm_lengths(engine: &TemporalEngine) -> Lengths {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut lengths = Lengths::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        if matches!(bucket.block_index, BlockIndexMap::Many(_)) {
            *lengths.counts.entry(bucket.block_index.len()).or_default() += 1;
        }
    }
    lengths
}

/// (Empty, One, Many) bucket counts.
fn arms(engine: &TemporalEngine) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut counts = (0usize, 0usize, 0usize);
    for bucket in shard.bucket_index.bucket_map.values() {
        match &bucket.block_index {
            BlockIndexMap::Empty => counts.0 += 1,
            BlockIndexMap::One(_, _) => counts.1 += 1,
            BlockIndexMap::Many(_) => counts.2 += 1,
        }
    }
    counts
}

/// THE ARM HISTOGRAM AND THE PAGES-PER-KEY DISTRIBUTION, AT BOTH RANGES, AT TWO CORPUS SIZES.
///
/// The number a container decision rests on is the LENGTH DISTRIBUTION, and a mean cannot carry
/// it: a scan's cost lives entirely in the tail, and the tail is what a mean hides. So this
/// publishes the arm split, then the percentiles and the MAXIMUM of the `Many` arm, and asserts
/// the fixture reached that arm before any of it is read as a fact about it.
///
/// rust-internal: measures the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds four stores up to 40,000 records each; run by name"]
fn the_page_list_length_distribution_is_reported_as_a_histogram() {
    let mut narrow_reached_many = 0usize;
    let mut widest_seen = 0usize;
    for records in [SMALL, LARGE] {
        for end_routing_bucket in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            let (empty, one, many) = arms(&engine);
            let lengths = many_arm_lengths(&engine);
            let range = if end_routing_bucket == WIDE_END {
                "whole".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            println!(
                "{records} records on {range}: arms Empty {empty} / One {one} / Many {many}"
            );
            lengths.report("Many arm");
            for (held, count) in &lengths.counts {
                if *held <= 4 || held % 8 == 0 || *held == lengths.widest() {
                    println!(
                        "      {held:>4} page(s): {count:>6} buckets ({:>7.3}%)",
                        100.0 * *count as f64 / lengths.buckets().max(1) as f64
                    );
                }
            }
            let indexed: usize = {
                let shards = engine.shards.read().expect("engine lock poisoned");
                let shard = shards.get(&1).expect("shard is loaded");
                shard
                    .bucket_index
                    .bucket_map
                    .values()
                    .map(|bucket| bucket.block_index.len())
                    .sum()
            };
            assert_eq!(
                indexed,
                keys.len(),
                "{records}/{range}: the index holds {indexed} pages for {} written keys, so the \
                 distribution above is not this fixture's",
                keys.len()
            );
            if end_routing_bucket == NARROW_END {
                narrow_reached_many += many;
                widest_seen = widest_seen.max(lengths.widest());
                assert!(
                    many > 0,
                    "{records} records on {range} built no `Many` arm at all; a fixture that \
                     never builds the shape this module changes cannot measure it"
                );
                assert!(
                    lengths.widest() >= 2,
                    "{records} records on {range}: the widest `Many` list holds \
                     {} page(s), which is not a multi-page list",
                    lengths.widest()
                );
            } else {
                assert_eq!(
                    many, 0,
                    "{records} records on the whole keyspace put {many} bucket(s) on the `Many` \
                     arm; at one page a bucket none should be, and if any are then this arm is \
                     not the single-page shape it is being read as"
                );
            }
        }
    }
    assert!(
        narrow_reached_many > 0 && widest_seen >= 2,
        "no multi-page list was built at either corpus size; every figure above would be vacuous"
    );
    println!(
        "  the widest page list seen across both corpus sizes on 0..{NARROW_END} holds \
         {widest_seen} pages"
    );
}

// =============================================================================================
// 2. ORDER -- THE CENTRAL RISK, ANSWERED BY THE REPRESENTATION
// =============================================================================================

fn page(object_key: &str, component: Option<&str>, slab: u64, offset: u64) -> BlockIndex {
    BlockIndex {
        object_key: std::sync::Arc::from(object_key),
        model_id: std::sync::Arc::from("string"),
        component: component.map(std::sync::Arc::from),
        address: crate::block_store::BlockAddress::from_parts(
            slab,
            offset,
            32,
            Some(7),
            Some(11),
            Some(3),
            Some(13),
        ),
        dirty: false,
        deleted: false,
        log_backed: false,
    }
}

fn built_from(pages: &[BlockIndex]) -> BlockIndexMap {
    let mut live = BlockSlabLiveIndex::default();
    let mut index = BlockIndexMap::default();
    for entry in pages {
        index.insert(entry.clone(), &mut live);
    }
    index
}

fn walk(index: &BlockIndexMap) -> Vec<u64> {
    index.iter().map(|(handle, _)| *handle).collect()
}

/// THE ORDER CONTROL.
///
/// A `BTreeMap<u64, _>` walks in ascending key order whatever order it was filled in. FIVE
/// production readers depend on that, and none of them would fail a test if it changed -- each
/// would quietly produce a different answer:
///
///   1. `engine::collect_command_index_items_for` emits one index-log item per page IN THIS
///      ORDER, so the order is what goes into the index log.
///   2. `storage_bucket_internals::collect_live_block_entries` materialises the live page set in
///      this order; the dump manifest, WAL reclaim, compaction and the GC snapshot all read it.
///   3. `storage_bucket_internals`' storage-topology sampler walks it and BREAKS at
///      `MAX_STORAGE_TOPOLOGY_SAMPLES`, so the order decides WHICH pages are sampled.
///   4. `engine::bucket_index_shape_for_test` renders it as the string that a shard built by
///      commands is compared against a shard rebuilt from records -- two different fill orders.
///   5. `bucket_store::bucket_index_block_address`'s whole-scan fallback takes the FIRST match.
///
/// Keeping the list sorted by the handle the tree was keyed by makes all five see the identical
/// sequence. This asserts exactly that, by filling the same pages in three different orders.
///
/// AND IT CARRIES ITS OWN NEGATIVE CONTROL: the same pages appended UNSORTED are shown to give a
/// different walk, so a run in which the sort silently stopped happening cannot pass.
///
/// rust-internal: page index ordering, no product behaviour
#[test]
fn an_unsorted_page_list_would_reorder_every_walk_of_this_index() {
    let pages: Vec<BlockIndex> = (0..24)
        .map(|i| page("bag", Some(&format!("f{i}")), 1, i as u64 * 64))
        .collect();

    let forwards = walk(&built_from(&pages));
    let mut backwards_input = pages.clone();
    backwards_input.reverse();
    let backwards = walk(&built_from(&backwards_input));
    let mut shuffled_input = pages.clone();
    // A fixed permutation, not a random one: this has to fail the same way every run.
    shuffled_input.sort_by_key(|entry| block_index_handle(entry).rotate_left(29));
    let shuffled = walk(&built_from(&shuffled_input));

    assert_eq!(
        forwards.len(),
        pages.len(),
        "the fixture filed {} of {} pages, so the walk below is not over the set it claims",
        forwards.len(),
        pages.len()
    );
    assert_eq!(
        forwards, backwards,
        "filling the same pages in the opposite order produced a different walk"
    );
    assert_eq!(
        forwards, shuffled,
        "filling the same pages in a third order produced a different walk"
    );
    assert!(
        forwards.windows(2).all(|pair| pair[0] < pair[1]),
        "the walk is not strictly ascending by handle, which is the order the tree walked in and \
         the order the five readers named above were written against"
    );

    // NEGATIVE CONTROL: the order this guard exists to rule out must actually differ from the
    // order it asserts, or the assertions above hold trivially.
    let appended: Vec<u64> = backwards_input
        .iter()
        .map(|entry| block_index_handle(entry))
        .collect();
    assert_ne!(
        forwards, appended,
        "appending in fill order gave the same sequence as sorting by handle, so this fixture \
         cannot tell a sorted list from an unsorted one and proves nothing"
    );
}

/// The walk survives a page being REMOVED from the middle and another inserted after it.
///
/// A sorted list inserts in the middle; a guard that only ever appends would not notice a policy
/// that pushed to the back.
///
/// rust-internal: page index ordering, no product behaviour
#[test]
fn the_walk_stays_ascending_across_removals_and_middle_inserts() {
    let mut live = BlockSlabLiveIndex::default();
    let mut index = BlockIndexMap::default();
    let pages: Vec<BlockIndex> = (0..32)
        .map(|i| page("bag", Some(&format!("g{i}")), 2, i as u64 * 64))
        .collect();
    for entry in &pages {
        index.insert(entry.clone(), &mut live);
    }
    let mut removed_any = false;
    for entry in pages.iter().step_by(3) {
        let handle = block_index_handle(entry);
        if index.remove(&handle, &mut live).is_some() {
            removed_any = true;
        }
    }
    assert!(removed_any, "the fixture removed nothing, so nothing was exercised");
    for i in 32..48 {
        index.insert(page("bag", Some(&format!("g{i}")), 2, i as u64 * 64), &mut live);
    }
    let handles = walk(&index);
    assert!(
        handles.len() > 16,
        "only {} pages survived the churn; the list is too short to say anything about order",
        handles.len()
    );
    assert!(
        handles.windows(2).all(|pair| pair[0] < pair[1]),
        "the walk stopped being ascending after removals and middle inserts: {handles:?}"
    );
}

// =============================================================================================
// 3. DUPLICATES -- WHAT A MAP DID BY CONSTRUCTION AND A LIST HAS TO DO ON PURPOSE
// =============================================================================================

/// A map deduplicated by key; a list appends unless told otherwise.
///
/// EVERY INSERT PATH REACHES `insert_unaccounted`: `BlockIndexMap::insert` (the accounted write
/// path, four production call sites), `insert_released` (the one caller that re-files a released
/// bucket's already-counted pages), and the `FromIterator` a load arrives through. So the
/// duplicate decision is made in one place, and it is REPLACE -- which is what the map did, and
/// what a rewrite of the same page needs, because the lookup refs on disk hold the handle and a
/// second entry under it would be a page that can be found twice or lost on the next rewrite.
///
/// Driven on BOTH arms: a duplicate arriving at `One` and a duplicate arriving at `Many`.
///
/// rust-internal: page index insert semantics, no product behaviour
#[test]
fn a_second_insert_of_the_same_page_replaces_it_rather_than_adding_beside_it() {
    let mut live = BlockSlabLiveIndex::default();

    // --- The One arm. ---
    let mut index = BlockIndexMap::default();
    let only = page("solo", None, 5, 128);
    let first = index.insert(only.clone(), &mut live);
    let again = index.insert(only.clone(), &mut live);
    assert_eq!(first, again, "the same page took two different handles");
    assert_eq!(1, index.len(), "a rewrite on the One arm filed {} pages", index.len());
    assert!(
        matches!(index, BlockIndexMap::One(..)),
        "a rewrite on the One arm spilled the bucket onto a list"
    );

    // --- The Many arm, at the shortest list it holds and again at a long one. ---
    for length in [SHORTEST_MANY, 4usize, 24] {
        let mut index = BlockIndexMap::default();
        let pages: Vec<BlockIndex> = (0..length)
            .map(|i| page("bag", Some(&format!("h{i}")), 9, i as u64 * 64))
            .collect();
        for entry in &pages {
            index.insert(entry.clone(), &mut live);
        }
        assert_eq!(length, index.len(), "the fixture filed the wrong number of pages");
        assert!(
            matches!(index, BlockIndexMap::Many(_)),
            "a list of {length} did not reach the Many arm"
        );
        let target = &pages[length / 2];
        let handle = index.insert(target.clone(), &mut live);
        assert_eq!(
            length,
            index.len(),
            "re-filing a page at length {length} grew the list to {}; the list is holding the \
             same page twice",
            index.len()
        );
        assert_eq!(
            block_index_handle(target),
            handle,
            "the rewrite was filed under a handle that is not the page's own"
        );
        assert!(
            walk(&index).windows(2).all(|pair| pair[0] < pair[1]),
            "the list stopped being sorted after a duplicate insert at length {length}"
        );
        // And the page that came back is the one just written, not the one it displaced.
        let stored = index.get(&handle).expect("the rewritten page is findable");
        assert_eq!(
            target.address, stored.address,
            "the duplicate insert left the displaced address in place"
        );
    }
}

/// A rewrite that MOVES the page charges the tally the move, which is what a replace has to do
/// and what an append would get wrong twice over.
///
/// The handle is a hash of identity AND address, so a page whose address moved takes a NEW handle
/// and is a genuinely new entry -- the old one is removed by its own path. What this covers is the
/// other case: the same handle arriving twice, where the map used to discharge the displaced
/// address and charge the new one. `insert` returns the handle and discharges through
/// `BlockSlabLiveIndex`, so the tally is the observable.
///
/// rust-internal: page index accounting, no product behaviour
#[test]
fn replacing_a_page_discharges_the_address_it_displaced() {
    let mut live = BlockSlabLiveIndex::default();
    let mut index = BlockIndexMap::default();
    let pages: Vec<BlockIndex> = (0..6)
        .map(|i| page("bag", Some(&format!("k{i}")), 4, i as u64 * 64))
        .collect();
    for entry in &pages {
        index.insert(entry.clone(), &mut live);
    }
    let before = live.tally(4);
    assert_eq!(
        6, before.block_refs,
        "the fixture charged {} pages to slab 4, not the six it filed",
        before.block_refs
    );
    index.insert(pages[3].clone(), &mut live);
    let after = live.tally(4);
    assert_eq!(
        before, after,
        "re-filing the same page moved the live tally from {before:?} to {after:?}; a replace is \
         supposed to discharge exactly what it displaces"
    );
}

// =============================================================================================
// 4. THE LOOKUP: TWO STRATEGIES ON ONE SORTED LIST, AND WHERE THEY CROSS
// =============================================================================================

fn sorted_list(length: usize) -> Vec<(u64, BlockIndex)> {
    let mut pages: Vec<(u64, BlockIndex)> = (0..length)
        .map(|i| {
            let entry = page("bag", Some(&format!("m{i}")), 3, i as u64 * 64);
            (block_index_handle(&entry), entry)
        })
        .collect();
    pages.sort_by_key(|(handle, _)| *handle);
    pages
}

/// BOTH STRATEGIES ANSWER IDENTICALLY, AT EVERY LENGTH, ON EVERY KEY.
///
/// They are two ways of asking one sorted list the same question, and both return the found
/// position or the INSERT position -- which is what makes the choice between them a performance
/// choice and nothing else. If this ever fails, the measurement that picked one of them was
/// answering a correctness question it had no business answering.
///
/// Covers hits and MISSES: the miss arm is the one that decides where an insert lands, so a
/// strategy that got misses wrong would corrupt the sort rather than return a wrong page.
///
/// rust-internal: page index lookup, no product behaviour
#[test]
fn the_walk_and_the_bisection_answer_identically_at_every_length() {
    let mut checked = 0usize;
    let mut hits = 0usize;
    let mut misses = 0usize;
    for length in 0..=48 {
        let pages = sorted_list(length);
        for (handle, _) in &pages {
            assert_eq!(
                scan_page(&pages, handle),
                bisect_page(&pages, handle),
                "the two strategies disagreed on a present handle at length {length}"
            );
            assert_eq!(
                find_page(&pages, handle),
                scan_page(&pages, handle),
                "the shipped lookup disagreed with the walk on a present handle at length {length}"
            );
            checked += 1;
            hits += 1;
        }
        for absent in [0u64, 1, u64::MAX, u64::MAX / 2, 0x5555_5555_5555_5555] {
            if pages.iter().any(|(handle, _)| *handle == absent) {
                continue;
            }
            assert_eq!(
                scan_page(&pages, &absent),
                bisect_page(&pages, &absent),
                "the two strategies disagreed on an absent handle at length {length}"
            );
            assert_eq!(
                find_page(&pages, &absent),
                bisect_page(&pages, &absent),
                "the shipped lookup disagreed with the bisection on an absent handle at length \
                 {length}"
            );
            checked += 1;
            misses += 1;
        }
    }
    // A selector matching nothing is vacuous, not a pass -- and this one has to have run BOTH
    // arms, not only the cheap one.
    assert!(
        hits > 100 && misses > 100,
        "the comparison ran {hits} hits and {misses} misses; one of the two arms is not being \
         exercised and the agreement it reports is partly empty"
    );
    println!("  the walk and the bisection agreed on {checked} lookups ({hits} hits, {misses} misses)");
}

/// WHERE THE WALK STOPS BEING THE CHEAPER OF THE TWO, COUNTED -- AND WHY THE SHIPPED LOOKUP IS
/// THE BISECTION.
///
/// COUNTS, NOT TIMES. A timing ratio in this campaign read 485x on an idle box and 11x on a busy
/// one off identical code. What the two strategies differ in is the ENTRIES they touch, and that
/// repeats exactly -- so the entry counter inside the two functions is the instrument, and the
/// shipped one is read through the door production calls rather than through a copy.
///
/// THE TABLE RUNS EVERY LENGTH FROM ONE, so the crossover is located rather than asserted from a
/// single point, and it runs well past the measured maximum of 50 so the tail is visible too.
///
/// WHAT IT SETTLES. The bisection becomes the cheaper of the two at a list of THREE, and the
/// `Many` arm exists only from TWO -- where the two tie exactly. There is therefore no length
/// this arm ever holds at which a walk is ahead, and no ceiling worth carrying. At the measured
/// p50 of 39 pages a bucket the walk touches twenty entries against the bisection's five, which
/// is far outside any correction contiguity could make to a count.
///
/// rust-internal: page index lookup cost, no product behaviour
#[test]
fn the_walk_and_the_bisection_cross_over_where_the_measurement_says() {
    println!(
        "  {:>7} {:>16} {:>16}  {}",
        "length", "walk entries/hit", "bisect entries/hit", "cheaper"
    );
    let mut crossover: Option<usize> = None;
    let mut rows = 0usize;
    let mut at_thirty_nine: Option<(f64, f64)> = None;
    for length in [1usize, 2, 3, 4, 6, 8, 12, 16, 24, 32, 39, 50, 64] {
        let pages = sorted_list(length);
        assert_eq!(length, pages.len(), "the fixture built a list of the wrong length");
        let walk_entries = counted(|| {
            for (handle, _) in &pages {
                let _ = std::hint::black_box(scan_page(&pages, handle));
            }
        }) as f64
            / length as f64;
        let bisect_entries = counted(|| {
            for (handle, _) in &pages {
                let _ = std::hint::black_box(bisect_page(&pages, handle));
            }
        }) as f64
            / length as f64;
        println!(
            "  {length:>7} {walk_entries:>16.3} {bisect_entries:>16.3}  {}",
            if walk_entries < bisect_entries {
                "walk"
            } else if bisect_entries < walk_entries {
                "bisect"
            } else {
                "tie"
            }
        );
        if crossover.is_none() && bisect_entries < walk_entries {
            crossover = Some(length);
        }
        if length == 39 {
            at_thirty_nine = Some((walk_entries, bisect_entries));
        }
        rows += 1;
    }
    assert!(rows >= 10, "the table has {rows} rows; too few to locate a crossover");
    let crossover = crossover.expect(
        "the bisection never became the cheaper of the two at any length up to 64; the instrument \
         is not reading what it is being read as reading",
    );
    println!(
        "  the bisection first reads cheaper at {crossover} entries; the shortest list the Many \
         arm holds is {SHORTEST_MANY}"
    );

    // THE SHIPPED CHOICE, AS THE CLAIM IT IS. A walk would be ahead only on lists SHORTER than
    // the crossover, and the shortest list this arm holds is two. So the crossover must sit at or
    // below the first length where the arm could prefer a walk, or there is a range of real
    // lengths the shipped lookup is spending entries on.
    assert!(
        crossover <= SHORTEST_MANY + 1,
        "the counts cross at {crossover}, so lists of {SHORTEST_MANY}..{} are cheaper to walk and \
         the shipped bisection is spending entries on them -- a ceiling is owed",
        crossover - 1
    );

    // AND THE MARGIN AT THE MEASURED p50, which is the number the decision actually rests on.
    let (walk_39, bisect_39) = at_thirty_nine.expect("the table covers the measured p50 of 39");
    println!(
        "  at the measured p50 of 39 pages a bucket: walk {walk_39:.3} entries a hit, bisection \
         {bisect_39:.3} -- {:.2}x",
        walk_39 / bisect_39
    );
    assert!(
        walk_39 > bisect_39 * 3.0,
        "at 39 entries the walk costs {walk_39:.3} against the bisection's {bisect_39:.3}, under \
         three times; below that margin contiguity could plausibly close the difference and the \
         choice would need a timing instrument this campaign does not trust"
    );

    // THE SHIPPED DOOR IS THE BISECTION, asserted by what it CHARGES rather than by reading the
    // source: a door that quietly walked would charge the walk's entries here.
    let long = sorted_list(39);
    let through_the_door = counted(|| {
        for (handle, _) in &long {
            let _ = std::hint::black_box(find_page(&long, handle));
        }
    });
    let through_the_bisection = counted(|| {
        for (handle, _) in &long {
            let _ = std::hint::black_box(bisect_page(&long, handle));
        }
    });
    let through_the_walk = counted(|| {
        for (handle, _) in &long {
            let _ = std::hint::black_box(scan_page(&long, handle));
        }
    });
    assert_eq!(
        through_the_bisection, through_the_door,
        "the shipped lookup charged {through_the_door} entries over 39 pages and the bisection \
         charged {through_the_bisection}; production is not running the strategy this test picked"
    );
    assert_ne!(
        through_the_walk, through_the_door,
        "the walk and the shipped lookup charged the same {through_the_door} entries, so this \
         control cannot tell them apart and its agreement above means nothing"
    );
}

/// Entries the shipped lookup functions examined while `body` ran.
fn counted(body: impl FnOnce()) -> u64 {
    reset_page_lookup_entries_examined();
    body();
    page_lookup_entries_examined()
}

/// THE PLANTED MARKER for the entry counter, recovered exactly.
///
/// A counter that reads zero would make every strategy look free, which is the failure that reads
/// as good news. A walk over a list of N that stops at the LAST entry touches exactly N, and a
/// walk for a handle above every one in the list is exactly that walk.
///
/// rust-internal: measures the harness's own instrument, no product behaviour
#[test]
fn the_entry_counter_recovers_a_planted_walk_exactly() {
    const PLANTED: usize = 37;
    let pages = sorted_list(PLANTED);
    assert_eq!(PLANTED, pages.len(), "the fixture built the wrong list");
    let touched = counted(|| {
        let _ = std::hint::black_box(scan_page(&pages, &u64::MAX));
    });
    assert_eq!(
        PLANTED as u64, touched,
        "a walk of {PLANTED} entries for a handle past all of them charged {touched} entries"
    );
    // And the counter must move at all under the shipped door, or the production path is not the
    // path being counted.
    let through_the_door = counted(|| {
        let _ = std::hint::black_box(find_page(&pages, &u64::MAX));
    });
    assert!(
        through_the_door > 0,
        "the shipped lookup charged nothing; it is not the function being measured"
    );
}

// =============================================================================================
// 5. WHAT THE CONTAINER COSTS, ON ONE INSTRUMENT, BOTH SIDES
// =============================================================================================

/// THE PLANTED MARKER. Recovered exactly, or every byte figure below is noise.
///
/// The failure this guards against is the one that reads as good news: an instrument reporting
/// near zero makes a shape look free.
///
/// rust-internal: measures the harness's own instrument, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn the_span_instrument_used_here_recovers_a_planted_megabyte_exactly() {
    const PLANTED: usize = 1 << 20;
    let (bytes, allocs) = span_counts(|| {
        let marker: Vec<u8> = Vec::with_capacity(PLANTED);
        std::hint::black_box(&marker);
    });
    println!("planted {PLANTED} B, instrument charged {bytes} B in {allocs} call(s)");
    assert_eq!(PLANTED as u64, bytes, "the span instrument charged {bytes} B for {PLANTED} B");
    assert_eq!(1, allocs, "one planted vector charged {allocs} allocations, not one");

    // AND IT MUST SEE A GROWTH STEP, not only a single up-front request -- the list side of every
    // comparison below grows in steps, and an instrument blind to `realloc` would report it free.
    let (grow_bytes, grow_allocs) = span_counts(|| {
        let mut marker: Vec<u8> = Vec::with_capacity(16);
        marker.resize(16, 0u8);
        marker.reserve_exact(PLANTED - 16);
        std::hint::black_box(&marker);
    });
    println!("planted a {PLANTED} B growth, instrument charged {grow_bytes} B in {grow_allocs} call(s)");
    assert!(
        grow_bytes >= (PLANTED - 16) as u64,
        "a growth to {PLANTED} B charged only {grow_bytes} B; the instrument is blind to \
         reallocation and every stepped list below would read as free"
    );
}

/// What a span of work charged the allocator, in bytes and in CALLS.
///
/// A SPAN AND NOT A CLONE, for the side that has capacity slack. `Vec::clone` takes exactly
/// `len` capacity, so a clone of a live list reports the bytes it WOULD cost if it had been
/// built to size -- which is precisely the slack this measurement is about. Charging the build
/// instead counts the capacity actually taken, growth steps included, and charges the tree's
/// nodes the same way.
#[cfg(feature = "alloc-probe")]
fn span_counts(body: impl FnOnce()) -> (u64, u64) {
    let probe = Probe::start();
    body();
    let counts = probe.stop();
    (counts.alloc_bytes, counts.allocs)
}

/// WHAT THE PAGE INDEX OF A REAL STORE COSTS AS A LIST AGAINST WHAT IT COST AS A TREE.
///
/// BYTES AND ALLOCATIONS PER RECORD, AT TWO CORPUS SIZES, ON ONE INSTRUMENT. Both sides are built
/// here, in one process, from the SAME pages the engine filed, at the real length distribution --
/// so this is a before/after rather than a measurement set against an arithmetic. A published
/// decline in this area had its sign inverted by comparing a `size_of` saving against an
/// allocator cost; no `size_of` appears in this comparison.
///
/// AT THE CONFIGURED RANGE. On the default whole keyspace every bucket holds its page inline and
/// there is no container to compare, which is the trap #1959 fell into; the range measured here
/// is the one `docs/runtime_tuning.md` tells an operator to set, and the fixture asserts it
/// reached the `Many` arm before reporting anything.
///
/// UNACCOUNTED INSERTS on the list side: `insert` also charges a per-slab live tally, which
/// allocates and has no counterpart on the tree side. `insert_released` is the same mechanism
/// with the accounting off, so both sides charge the container and nothing else.
///
/// rust-internal: measures container footprint, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "seeds two stores of up to 40,000 records; run by name"]
fn the_page_index_of_a_real_store_costs_less_as_a_list_than_as_a_tree() {
    let mut path_lengths: Vec<usize> = Vec::new();
    println!(
        "  {:>8} {:>8} {:>9} {:>12} {:>12} {:>12} {:>12} {:>9}",
        "records", "buckets", "pages", "tree B/rec", "tree alc/rec", "list B/rec", "list alc/rec",
        "B ratio"
    );
    let mut ratios: Vec<(usize, f64, f64)> = Vec::new();
    for records in [SMALL, LARGE] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);
        let keys = seed_routed(&engine, records);

        // The pages, bucket by bucket, exactly as the engine filed them.
        let by_bucket: Vec<Vec<BlockIndex>> = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            shard
                .bucket_index
                .bucket_map
                .values()
                .map(|bucket| bucket.block_index.values().cloned().collect())
                .filter(|pages: &Vec<BlockIndex>| !pages.is_empty())
                .collect()
        };
        let pages: usize = by_bucket.iter().map(|b| b.len()).sum();
        assert_eq!(
            pages,
            keys.len(),
            "{records}: the index holds {pages} pages for {} written keys, so the per-record \
             divisor below is not the fixture's",
            keys.len()
        );
        let multi = by_bucket.iter().filter(|b| b.len() > 1).count();
        assert!(
            multi > 0,
            "{records}: no bucket held more than one page, so there is no container to compare"
        );

        let mut trees: Vec<BTreeMap<u64, BlockIndex>> = Vec::with_capacity(by_bucket.len());
        let (tree_bytes, tree_allocs) = span_counts(|| {
            for bucket in &by_bucket {
                let mut tree: BTreeMap<u64, BlockIndex> = BTreeMap::new();
                for page in bucket {
                    tree.insert(block_index_handle(page), page.clone());
                }
                trees.push(tree);
            }
        });

        let mut lists: Vec<BlockIndexMap> = Vec::with_capacity(by_bucket.len());
        let (list_bytes, list_allocs) = span_counts(|| {
            for bucket in &by_bucket {
                let mut list = BlockIndexMap::default();
                for page in bucket {
                    list.insert_released(page.clone());
                }
                lists.push(list);
            }
        });

        assert_eq!(
            trees.iter().map(|t| t.len()).sum::<usize>(),
            lists.iter().map(|l| l.len()).sum::<usize>(),
            "{records}: the two containers hold different numbers of pages"
        );
        assert!(
            tree_bytes > 0 && tree_allocs > 0 && list_bytes > 0 && list_allocs > 0,
            "{records}: the instrument charged tree {tree_bytes} B / {tree_allocs} calls and list \
             {list_bytes} B / {list_allocs} calls; a zero reading is the instrument failing, not \
             a container being free"
        );

        let ratio = list_bytes as f64 / tree_bytes as f64;
        let alloc_ratio = list_allocs as f64 / tree_allocs as f64;
        println!(
            "  {records:>8} {:>8} {pages:>9} {:>12.2} {:>12.4} {:>12.2} {:>12.4} {ratio:>9.3}",
            by_bucket.len(),
            tree_bytes as f64 / pages as f64,
            tree_allocs as f64 / pages as f64,
            list_bytes as f64 / pages as f64,
            list_allocs as f64 / pages as f64
        );
        ratios.push((records, ratio, alloc_ratio));
        std::hint::black_box(&trees);
        std::hint::black_box(&lists);
    }

    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|length| *length == first),
        "the store path length moved across arms ({path_lengths:?}); allocation bytes move at \
         about six bytes a character and the byte column would carry it"
    );
    println!("  store path length held at {first} characters across both arms");

    for (records, ratio, alloc_ratio) in &ratios {
        println!(
            "  {records} records: the list charges {:.1}% of the tree's bytes and {:.2}x its \
             allocation calls",
            100.0 * ratio,
            alloc_ratio
        );
    }
    // THE SIGN, not a ratio target. If the list ever charges more bytes than the tree at a real
    // corpus size, the container change does not pay and must be declined with these numbers.
    for (records, ratio, _) in &ratios {
        assert!(
            *ratio < 1.0,
            "at {records} records the list charged {ratio:.3}x the tree's bytes for the same \
             pages; the container change does not pay"
        );
    }
}

/// WHAT THE GROWTH STEP IS WORTH, against `Vec`'s own doubling.
///
/// `Vec` doubles, which at a list of 39 leaves a capacity of 64 -- a quarter of the bytes unused,
/// which is the same slack the tree node left. Stepping by eight caps the waste at seven entries.
/// Both are measured on the same instrument; the shipped policy is asserted to leave less.
///
/// rust-internal: measures container footprint, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn the_page_list_growth_step_is_what_keeps_the_slack_off_the_measurement() {
    println!(
        "  {:>7} {:>14} {:>12} {:>14} {:>12} {:>9}",
        "length", "doubling B", "doubling alc", "stepped B", "stepped alc", "B saved"
    );
    let mut rows = 0usize;
    let mut worst = 0.0f64;
    for length in [3usize, 8, 12, 20, 33, 39, 50, 65] {
        let pages = sorted_list(length);

        // What `Vec`'s own growth leaves: push one at a time, no reserve.
        let mut doubling: Vec<(u64, BlockIndex)> = Vec::new();
        let (doubling_bytes, doubling_allocs) = span_counts(|| {
            for entry in &pages {
                doubling.push(entry.clone());
            }
        });

        let mut stepped = BlockIndexMap::default();
        let (stepped_bytes, stepped_allocs) = span_counts(|| {
            for (_, page) in &pages {
                stepped.insert_released(page.clone());
            }
        });

        let saved = 100.0 * (doubling_bytes as f64 - stepped_bytes as f64) / doubling_bytes as f64;
        worst = if rows == 0 { saved } else { worst.min(saved) };
        // THE PRICE OF THE STEP, BANDED. Stepping trades reallocation calls for capacity slack,
        // and a step small enough reallocates on every insert -- which is the failure this band
        // must EXCLUDE, not merely be near. `Vec`'s doubling is the cheapest any policy can be in
        // calls, so the band is stated as a multiple of it.
        assert!(
            stepped_allocs <= doubling_allocs.saturating_mul(3),
            "at {length} entries the step policy charged {stepped_allocs} allocation calls \
             against doubling's {doubling_allocs}, more than three times; a step that \
             reallocates on nearly every insert is not a growth policy"
        );
        println!(
            "  {length:>7} {doubling_bytes:>14} {doubling_allocs:>12} {stepped_bytes:>14} \
             {stepped_allocs:>12} {saved:>8.1}%"
        );
        std::hint::black_box(&doubling);
        std::hint::black_box(&stepped);
        rows += 1;
    }
    assert!(rows >= 6, "only {rows} lengths measured");
    println!("  the step policy's worst byte saving against doubling is {worst:.1}%");
    // NEVER BEHIND. A policy that saves at the long lengths and LOSES at the short ones is a
    // policy that costs bytes on the corpus most buckets are in: at 4,000 records the measured
    // p50 is 4 pages and the maximum is 8. The first spill taking two steps instead of one made
    // a list of three cost twice what `Vec` would have, and this is what said so.
    assert!(
        worst >= 0.0,
        "at its worst measured length the step policy charged {:.1}% MORE than `Vec`'s own \
         doubling; a growth policy that loses at the short lengths loses on most buckets",
        -worst
    );

    // THE SLACK ITSELF, off the live list rather than a clone: `Vec::clone` takes exactly `len`
    // capacity, so a clone cannot see the thing this policy exists to bound.
    for length in [3usize, 12, 20, 33, 39, 50, 65] {
        let pages = sorted_list(length);
        let mut list = BlockIndexMap::default();
        for (_, page) in &pages {
            list.insert_released(page.clone());
        }
        let slack = match &list {
            BlockIndexMap::Many(entries) => entries.capacity() - entries.len(),
            other => panic!("a list of {length} took the {other:?} arm"),
        };
        // What `Vec`'s doubling would have left at the same length -- the value this band must
        // EXCLUDE, or it is a band around nothing.
        let mut doubling: Vec<(u64, BlockIndex)> = Vec::new();
        for entry in &pages {
            doubling.push(entry.clone());
        }
        let doubling_slack = doubling.capacity() - doubling.len();
        println!(
            "  a live list of {length} carries {slack} unused slot(s); doubling would carry \
             {doubling_slack}"
        );
        assert!(
            slack < 4,
            "a live list of {length} carries {slack} unused slots; the step policy is supposed to \
             cap that below four"
        );
        if length >= 33 {
            assert!(
                doubling_slack >= 4,
                "at {length} entries `Vec`'s own doubling would leave {doubling_slack} unused \
                 slots, which is inside the band this policy is asserted against -- the band does \
                 not exclude the thing it guards against"
            );
        }
    }
}

/// DOES THE `One` ARM STILL EARN ITS PLACE once `Many` is a list?
///
/// NOT RHETORICAL, AND THE ANSWER IS NOT THE ONE THE ARM WAS WRITTEN FOR. `One` exists because a
/// `BTreeMap` holding a single entry cost 1,496 live bytes to carry a 120-byte page. A LIST
/// holding a single entry costs 112. So the comparison the arm won is gone, and what is left is:
///
///   SAVED, per bucket, everywhere: dropping the arm takes `BlockIndexMap` from its `One` width
///   to the 24 bytes of a vector header, and `BucketNode` with it. `BucketMap` is a
///   `BTreeMap<u32, BucketNode>`, whose leaf carries eleven value slots filled or not, so that
///   saving is multiplied by the bucket map before it is banked.
///
///   PAID, per SINGLE-PAGE bucket only: one heap allocation holding one entry.
///
/// So the answer is a crossover in the fraction of buckets holding exactly one page, and both
/// sides are measured here on one instrument. THE BUCKET-MAP SIDE IS PRICED WITH A STAND-IN VALUE
/// OF THE RIGHT WIDTH -- a `BTreeMap`'s node cost depends on nothing else about the value -- and
/// the stand-in's width is asserted against `size_of::<BucketNode>()` so a node that moves under
/// this test breaks it rather than quietly re-pricing it.
///
/// rust-internal: measures container footprint, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn the_one_arm_still_earns_its_place_and_this_is_the_fraction_where_it_would_not() {
    // --- What the arm costs a bucket that holds one page: nothing on the heap. ---
    let pages = sorted_list(1);
    let single = pages[0].1.clone();
    let mut inline = BlockIndexMap::default();
    inline.insert_released(single.clone());
    assert!(
        matches!(inline, BlockIndexMap::One(..)),
        "one page did not take the inline arm, so this comparison is not about that arm"
    );
    let (inline_bytes, inline_allocs) = span_counts(|| {
        let mut held = BlockIndexMap::default();
        held.insert_released(single.clone());
        std::hint::black_box(&held);
    });
    let (spilled_bytes, spilled_allocs) = span_counts(|| {
        let held: Vec<(u64, BlockIndex)> = vec![(block_index_handle(&single), single.clone())];
        std::hint::black_box(&held);
    });
    println!(
        "  one page inline: {inline_bytes} B in {inline_allocs} alloc(s); the same page in a \
         one-element list: {spilled_bytes} B in {spilled_allocs} alloc(s)"
    );
    assert!(
        spilled_allocs > inline_allocs,
        "a one-element list charged {spilled_allocs} allocations against the inline arm's \
         {inline_allocs}; if the list is not the one that allocates, the inline arm has no reason \
         to exist"
    );

    // --- What the arm costs EVERY bucket: the width it adds to the node, through the bucket map.
    const BUCKETS: u32 = 4_096;
    type WideSlot = (u64, [u64; 23]);
    type NarrowSlot = (u64, [u64; 12]);
    let node_width_now = size_of::<BucketNode>();
    let node_width_without = node_width_now - (size_of::<BlockIndexMap>() - size_of::<Vec<(u64, BlockIndex)>>());
    assert_eq!(
        node_width_now,
        size_of::<WideSlot>(),
        "the stand-in for a bucket node is {} B against the node's {node_width_now} B; it is \
         pricing the wrong width",
        size_of::<WideSlot>()
    );
    assert_eq!(
        node_width_without,
        size_of::<NarrowSlot>(),
        "the stand-in for a node without the inline arm is {} B against the {node_width_without} \
         B such a node would be",
        size_of::<NarrowSlot>()
    );

    let mut wide: BTreeMap<u32, WideSlot> = BTreeMap::new();
    let (wide_bytes, _) = span_counts(|| {
        for i in 0..BUCKETS {
            wide.insert(i, (i as u64, [0u64; 23]));
        }
    });
    let mut narrow: BTreeMap<u32, NarrowSlot> = BTreeMap::new();
    let (narrow_bytes, _) = span_counts(|| {
        for i in 0..BUCKETS {
            narrow.insert(i, (i as u64, [0u64; 12]));
        }
    });
    std::hint::black_box((&wide, &narrow));
    assert!(
        wide_bytes > narrow_bytes,
        "a bucket map of {node_width_now}-byte nodes charged {wide_bytes} B and one of \
         {node_width_without}-byte nodes charged {narrow_bytes} B; the narrower node is supposed \
         to be the cheaper one and this instrument cannot see the difference"
    );
    let saved_per_bucket = (wide_bytes - narrow_bytes) as f64 / BUCKETS as f64;
    let paid_per_single = (spilled_bytes.saturating_sub(inline_bytes)) as f64;
    println!(
        "  through the bucket map, a node of {node_width_now} B costs {:.1} B a bucket and one of \
         {node_width_without} B costs {:.1}: dropping the inline arm would save {saved_per_bucket:.1} B \
         on EVERY bucket and pay {paid_per_single:.1} B on every SINGLE-PAGE one",
        wide_bytes as f64 / BUCKETS as f64,
        narrow_bytes as f64 / BUCKETS as f64
    );
    assert!(
        paid_per_single > 0.0,
        "a one-element list charged no more than the inline arm; the crossover below would be a \
         division by a measurement that did not happen"
    );
    let crossover = saved_per_bucket / paid_per_single;
    println!(
        "  SO: dropping the `One` arm pays in BYTES wherever fewer than {:.1}% of buckets hold \
         exactly one page",
        100.0 * crossover
    );
    println!(
        "  MEASURED single-page fractions: 100.0% on the default whole keyspace at both corpus \
         sizes; 2.54% at 4,000 records and 0.00% at 40,000 on 0..{NARROW_END} -- see \
         the_page_list_length_distribution_is_reported_as_a_histogram"
    );

    // THE ANSWER, AS A GUARD RATHER THAN AS PROSE. A fraction above 100% means there is NO
    // occupancy at which the inline arm pays for itself in bytes -- not even the default range's
    // 100% single-page store, which is the case the arm was written for. What keeps it is the
    // allocation column: dropping it buys one heap allocation per single-page bucket, which on
    // the default range is one per record on the write path. That is the whole of the argument
    // for keeping it, and it is an allocation-count argument, not a byte one.
    //
    // If this ever reads BELOW 100%, the arm has started paying for itself again -- a node that
    // narrowed, or a page entry that widened -- and the note above is stale rather than wrong.
    assert!(
        crossover > 1.0,
        "the inline arm now pays for itself in bytes below {:.1}% single-page occupancy, which \
         the default range exceeds; the reasoning recorded here is stale",
        100.0 * crossover
    );
}

// =============================================================================================
// 6. THE STORED SHAPE DID NOT MOVE
// =============================================================================================

/// THE INDEX ON DISK IS BYTE FOR BYTE WHAT THE TREE WROTE.
///
/// `BlockIndexMap` serializes as a map of RENDERED STRING KEYS sorted by that string, and it did
/// before this change too -- the container it walks to build that map is the only thing that
/// moved. So an index written by a build from before #1963 loads here, and one written here loads
/// there, and this asserts it at the byte level rather than at the "it round-trips" level.
///
/// WITH AN INJECTION CONTROL: one page altered must move the bytes, or a byte comparison that
/// always passes is proving nothing.
///
/// rust-internal: page index wire shape, no product behaviour
#[test]
fn the_stored_page_index_is_byte_for_byte_what_the_tree_wrote() {
    let pages = sorted_list(24);

    let mut list = BlockIndexMap::default();
    let mut live = BlockSlabLiveIndex::default();
    for (_, entry) in &pages {
        list.insert(entry.clone(), &mut live);
    }
    let written = serde_json::to_vec(&list).expect("the page index serializes");
    assert!(
        written.len() > 1_000,
        "the fixture wrote {} bytes for 24 pages; too small to be the index",
        written.len()
    );

    // Filled in a different order, it must write the identical bytes -- which is the on-disk face
    // of the ordering guard above.
    let mut backwards = BlockIndexMap::default();
    for (_, entry) in pages.iter().rev() {
        backwards.insert(entry.clone(), &mut live);
    }
    let written_backwards = serde_json::to_vec(&backwards).expect("the page index serializes");
    assert_eq!(
        written, written_backwards,
        "the same pages filled in the opposite order wrote different bytes"
    );

    // And it loads back as the same walk, through the same `From<BTreeMap<String, BlockIndex>>`
    // the stored shape has always been read by.
    let loaded: BlockIndexMap = serde_json::from_slice(&written).expect("the page index loads");
    assert_eq!(
        walk(&list),
        walk(&loaded),
        "an index written and read back walks in a different order than the one that wrote it"
    );
    let rewritten = serde_json::to_vec(&loaded).expect("the loaded index serializes");
    assert_eq!(
        written, rewritten,
        "loading and rewriting the index changed {} bytes",
        written.len()
    );

    // INJECTION CONTROL: a single altered page must move the bytes.
    let mut injected = BlockIndexMap::default();
    for (at, (_, entry)) in pages.iter().enumerate() {
        let mut entry = entry.clone();
        if at == 7 {
            entry.deleted = true;
        }
        injected.insert(entry, &mut live);
    }
    let with_injection = serde_json::to_vec(&injected).expect("the page index serializes");
    assert_ne!(
        written, with_injection,
        "altering one page of twenty-four left the written bytes unchanged; this comparison \
         cannot see a change and its passing above means nothing"
    );
}

// =============================================================================================
// 7. THE READ PATH, MEASURED THROUGH THE ENGINE
// =============================================================================================

/// WHAT A READ EXAMINES, THROUGH THE ENGINE'S OWN RESOLVER, AT THE CONFIGURED RANGE.
///
/// This change trades a tree descent for a walk, and the read path is where it is won or lost.
/// The counter sits inside the two lookup functions production calls, so this is a reading of the
/// path and not of a copy.
///
/// THROUGH `bucket_store::bucket_index_block_address`, which is the resolver
/// `Command::StringGet` reaches through `read_bucket_index_value` once the response cache misses
/// -- driven directly here rather than through `execute`, because a warm cache answers without
/// resolving anything and a measurement of the read path taken on a cache hit is a measurement of
/// the cache.
///
/// THE CONTROL ON THE EXPLANATION: the same resolutions on the WHOLE keyspace, where every bucket
/// holds one page inline and no list lookup happens at all, must charge ZERO. If that arm moves,
/// the counter is counting something other than the list lookup and the narrow figure below
/// cannot be attributed to it.
///
/// rust-internal: measures the engine's own read path, no product behaviour
#[test]
#[ignore = "seeds two stores of 4,000 records; run by name"]
fn a_read_examines_entries_only_where_a_page_list_exists() {
    let mut charged: BTreeMap<u32, (u64, usize)> = BTreeMap::new();
    for end_routing_bucket in [WIDE_END, NARROW_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        let keys = seed_routed(&engine, SMALL);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        reset_page_lookup_entries_examined();
        let mut served = 0usize;
        for key in keys.iter().take(1_000) {
            if crate::engine::bucket_store::bucket_index_block_address(
                shard, "string", key, None,
            )
            .is_some()
            {
                served += 1;
            }
        }
        let examined = page_lookup_entries_examined();
        assert_eq!(1_000, served, "the fixture resolved {served} of 1,000 keys");
        charged.insert(end_routing_bucket, (examined, served));
        println!(
            "  0..{end_routing_bucket}: {examined} list entries examined over {served} \
             resolutions ({:.4} a read)",
            examined as f64 / served as f64
        );
    }

    let (wide_examined, _) = charged[&WIDE_END];
    let (narrow_examined, narrow_served) = charged[&NARROW_END];
    assert_eq!(
        0, wide_examined,
        "on the whole keyspace every bucket holds its page inline and no list is walked, yet the \
         counter charged {wide_examined} entries; it is counting something else"
    );
    assert!(
        narrow_examined > 0,
        "on 0..{NARROW_END} the reads examined no list entries at all, so this measurement has no \
         subject"
    );
    println!(
        "  a read on the configured range walks {:.3} list entries; on the default range it walks \
         none, which is the control on the attribution",
        narrow_examined as f64 / narrow_served as f64
    );
}

// =============================================================================================
// 8. AN INDEPENDENT RESIDUAL
// =============================================================================================

/// EVERY PAGE THE ENGINE WROTE IS IN EXACTLY ONE LIST, COUNTED FROM THE OTHER END.
///
/// Not the sum of the rows above: this walks the bucket index and counts distinct HANDLES, then
/// sets that against the keys the fixture wrote. A list that lost a page to a duplicate, or held
/// one twice, shows here as a non-zero residual with a name.
///
/// WITH A PLANTED MARKER: a page is removed from the index behind the engine's back and the
/// residual must come back at exactly one, so a residual of zero is a measurement rather than an
/// instrument that cannot count.
///
/// rust-internal: measures the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds a store of 4,000 records; run by name"]
fn every_written_page_sits_in_exactly_one_list_and_a_planted_loss_shows_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed_routed(&engine, SMALL);

    let (distinct, total, listed_keys) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let mut handles = std::collections::BTreeSet::new();
        let mut total = 0usize;
        let mut object_keys = std::collections::BTreeSet::new();
        for bucket in shard.bucket_index.bucket_map.values() {
            for (handle, page) in &bucket.block_index {
                handles.insert(*handle);
                object_keys.insert(page.object_key.to_string());
                total += 1;
            }
        }
        (handles.len(), total, object_keys)
    };

    assert_eq!(
        total, distinct,
        "the index walks {total} entries but only {distinct} distinct handles; {} page(s) are \
         filed twice",
        total - distinct
    );
    let missing: Vec<&String> = keys.iter().filter(|key| !listed_keys.contains(*key)).collect();
    assert!(
        missing.is_empty(),
        "{} written key(s) have no page in any list; the first few are {:?}",
        missing.len(),
        missing.iter().take(5).collect::<Vec<_>>()
    );
    println!("  {total} pages in {distinct} distinct handles over {} written keys; residual 0", keys.len());

    // THE PLANTED LOSS. Take one page out from under the index and confirm the count above sees
    // it -- a residual that reads zero because it cannot count is worse than none.
    let planted = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        let mut live = BlockSlabLiveIndex::default();
        let mut taken = None;
        for bucket in shard.bucket_index.bucket_map.values_mut() {
            if let Some((handle, page)) = bucket.block_index.iter().next() {
                let (handle, key) = (*handle, page.object_key.to_string());
                bucket.block_index.remove(&handle, &mut live);
                taken = Some(key);
                break;
            }
        }
        taken.expect("the index held at least one page to plant a loss in")
    };
    let after: usize = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| bucket.block_index.len())
            .sum()
    };
    assert_eq!(
        total - 1,
        after,
        "a planted removal of the page for {planted} moved the count from {total} to {after}; the \
         residual instrument cannot see a lost page and its zero above says nothing"
    );
    println!("  the planted loss of one page for {planted} was recovered exactly");
}
