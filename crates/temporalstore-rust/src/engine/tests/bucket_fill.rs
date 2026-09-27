// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT DECIDES HOW MANY PAGES LAND IN ONE ROUTING BUCKET.
//!
//! #1959 measured the pages a bucket holds and found two populations: a store of routed keys is
//! 99.900% single-page, a store of container keys holds a hundred pages a bucket and has no
//! single-page bucket at all. It read the first as a property of the WORKLOAD -- "keys that route
//! one to a bucket" -- and priced the page index's shapes against it.
//!
//! IT IS NOT A PROPERTY OF THE WORKLOAD. IT IS A PROPERTY OF THE ROUTING RANGE, AND THE RANGE IS A
//! DEPLOYMENT SETTING THIS ENGINE ALREADY SHIPS.
//!
//! A page's bucket is `block_routing_bucket(object_key, start, end)`:
//!
//! ```text
//! start + (FNV-1a-64(object_key) % (end - start + 1))
//! ```
//!
//! The modulus is the RANGE WIDTH. `TemporalEngine::load_shard` and `startup_load_shard_request`
//! both default `end_routing_bucket` to `u32::MAX`, so a shard loaded with the default divides
//! 4.29 billion buckets among a few thousand keys and every key lands alone --
//! `docs/runtime_tuning.md` says so in as many words: "with the full `u32` range every key lands
//! in a slot of its own". #1959's fixture calls `engine.load_shard(1)`, which is that default.
//!
//! The documented production setting is `TS_SHARD_END_ROUTING_BUCKET=1023`, and at 1,024 buckets
//! the SAME routed workload fills them. `the_pages_a_bucket_holds_are_decided_by_the_routing_range`
//! reports both widths at two corpus sizes as histograms with counts.
//!
//! AND THE CONTAINER STORE IS NOT A DIFFERENT ROUTING CALL, A DIFFERENT RANGE OR A DIFFERENT KEY
//! SHAPE. It is the same call on the same range. Routing takes the OBJECT KEY and never sees the
//! component; the page HANDLE is `stable_block_object_id(shard, kind, key, component)` and does.
//! So a hash's hundred fields are a hundred distinct handles under one object key, and one object
//! key is one bucket at any range.
//! `routing_never_sees_the_component_which_is_why_a_containers_pages_share_one_bucket` asserts
//! both halves against the functions themselves.
//!
//! WHAT THIS MODULE DOES NOT CLAIM. It does not propose a code change, and it does not recommend
//! narrowing the range. The lever is a knob that already exists and is already documented. What
//! was missing is the other side of its trade: `docs/runtime_tuning.md` sells the narrow range as
//! "about 45% less resident memory at no cost on disk" and names no cost at all. There are three,
//! and all three are measured here.
//!
//!   1. THE BUCKET MAP ITSELF CAN MOVE THE WRONG WAY. `BlockIndexMap::One` holds its page inline
//!      and allocates nothing; `Many` is a `BTreeMap` whose node is sized for ELEVEN entries
//!      whether or not it fills them. Filling a bucket trades one node per PAGE for one node per
//!      BUCKET plus a map node, so below a fill of about eleven the trade is a loss. Measured: at
//!      3.91 pages a bucket the narrow range costs +25.4% bytes a page AND +425% allocations a
//!      page; at 39.06 pages a bucket bytes fall 36.0% while allocations are still +121%. The two
//!      instruments disagree in SIGN at the large corpus, which is why both are reported.
//!   2. THE DIRTY SET AND THE DUMP UNIT COARSEN BY THE BUCKET'S KEY COUNT. A dump of one bucket
//!      drains one key on the whole keyspace and eight at 4,000 records on 0..1023.
//!   3. NARROWING A POPULATED STORE IS SILENT. A reopened range is consulted only for an address
//!      carrying no bucket of its own, and the live write path stamps one at `append_value`. So
//!      nothing is re-filed: every page stays where the OLD range put it, every record still
//!      reads, and on a narrowed shard all of them sit above the shard's own end -- outside every
//!      per-bucket sweep the engine runs. Widening is safe only because the wide range contains
//!      the narrow one.
#![allow(clippy::all)]
use super::*;
use crate::engine::hashing::{block_routing_bucket, stable_block_object_id};
use crate::engine::storage_bucket_internals::collect_live_block_entries;
use std::collections::{BTreeMap, BTreeSet};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_BUCKET=1023`, the
/// setting `docs/runtime_tuning.md` tells an operator to set before the first ingest.
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses, and `startup_load_shard_request`'s default.
/// #1959's fixture is on this one.
const WIDE_END: u32 = u32::MAX;

/// How many buckets the narrow range has. Not a literal anywhere below: derived from the range so
/// a change to `NARROW_END` moves it.
const NARROW_BUCKETS: usize = (NARROW_END as usize) + 1;

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
        table_name: "bucket-fill".to_string(),
        shard_uri: "local://bucket-fill/1".to_string(),
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

fn ack(response: &crate::types::BatchExecuteResponse) {
    assert!(response.status.ok, "seed must ack: {:?}", response.status);
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        ack(&engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        }));
    }
}

/// ROUTED KEYS: plain strings, one page each. The shape #1958 and #1959 both measured.
fn seed_routed(engine: &TemporalEngine, count: usize) -> Vec<String> {
    let keys: Vec<String> = (0..count).map(|i| format!("fill-{i:06}")).collect();
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

/// CONTAINER KEYS: `keys` hashes of `members` fields each. Every field is its own page.
fn seed_container(engine: &TemporalEngine, keys: usize, members: usize) -> Vec<String> {
    let container_keys: Vec<String> = (0..keys).map(|k| format!("bag-{k:06}")).collect();
    let mut commands = Vec::with_capacity(keys * members);
    for key in &container_keys {
        for f in 0..members {
            commands.push(Command::HashSet {
                key: key.clone(),
                field: format!("f{f}"),
                value: vec![b'v'; 32],
            });
        }
    }
    run_batch(engine, commands);
    container_keys
}

fn read_back(engine: &TemporalEngine, keys: &[String]) -> usize {
    keys.iter()
        .filter(|key| {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: (*key).clone(),
                },
            });
            response.status.ok
                && matches!(
                    &response.response,
                    crate::types::CommandResponse::Bytes { value: Some(_) }
                )
        })
        .count()
}

// ---------------------------------------------------------------------------------------------
// THE DISTRIBUTION, as counts.
// ---------------------------------------------------------------------------------------------

/// Pages held per routing bucket, as bucket COUNTS keyed by the number of pages held.
///
/// A histogram and not a mean: #1959's mean of 1.98 contained not one bucket holding two.
#[derive(Debug, Default, Clone)]
struct PagesPerBucket {
    counts: BTreeMap<usize, usize>,
}

impl PagesPerBucket {
    fn buckets(&self) -> usize {
        self.counts.values().copied().sum()
    }

    fn pages(&self) -> usize {
        self.counts.iter().map(|(held, count)| held * count).sum()
    }

    /// Buckets holding strictly more than one page -- the anti-constant case.
    fn multi_page(&self) -> usize {
        self.counts
            .iter()
            .filter(|(held, _)| **held > 1)
            .map(|(_, count)| *count)
            .sum()
    }

    fn widest(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    fn mean(&self) -> f64 {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0.0;
        }
        self.pages() as f64 / buckets as f64
    }

    fn single_page_fraction(&self) -> f64 {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0.0;
        }
        self.counts.get(&1).copied().unwrap_or_default() as f64 / buckets as f64
    }

    /// Print as counts, collapsing the long tail into ranges so a 40-page-deep histogram stays
    /// readable without ever reporting a mean in place of a count.
    fn report(&self, label: &str) {
        println!(
            "{label}: {} buckets, {} pages, mean {:.4}, single-page {:.5}, widest {}, multi-page {}",
            self.buckets(),
            self.pages(),
            self.mean(),
            self.single_page_fraction(),
            self.widest(),
            self.multi_page()
        );
        for (held, count) in &self.counts {
            if *held <= 8 || held % 8 == 0 || *held == self.widest() {
                println!(
                    "    {held:>4} page(s): {count:>6} buckets ({:>7.3}%)",
                    100.0 * *count as f64 / self.buckets().max(1) as f64
                );
            }
        }
    }
}

fn pages_per_bucket(engine: &TemporalEngine) -> PagesPerBucket {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut hist = PagesPerBucket::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        if bucket.block_index.is_empty() {
            continue;
        }
        *hist.counts.entry(bucket.block_index.len()).or_default() += 1;
    }
    hist
}

/// Every bucket that holds a page, with the OBJECT KEYS it holds, deduplicated and sorted. The
/// element-by-element picture a count cannot give.
fn bucket_key_sets(engine: &TemporalEngine) -> BTreeMap<u32, BTreeSet<String>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut contents: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        let keys: BTreeSet<String> = bucket
            .block_index
            .values()
            .map(|page| page.object_key.to_string())
            .collect();
        if keys.is_empty() {
            continue;
        }
        contents.insert(*routing_bucket, keys);
    }
    contents
}

/// Every bucket with the PAGE HANDLES it holds -- the map's own keys, which are object ids and
/// therefore distinguish a container's components. A key set cannot see a lost component; this
/// can.
fn bucket_handle_sets(engine: &TemporalEngine) -> BTreeMap<u32, BTreeSet<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut contents: BTreeMap<u32, BTreeSet<u64>> = BTreeMap::new();
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        let handles: BTreeSet<u64> = bucket.block_index.iter().map(|(handle, _)| *handle).collect();
        if handles.is_empty() {
            continue;
        }
        contents.insert(*routing_bucket, handles);
    }
    contents
}

fn flatten<T: Ord + Clone>(contents: &BTreeMap<u32, BTreeSet<T>>) -> BTreeSet<T> {
    contents.values().flatten().cloned().collect()
}

/// How many buckets sit in each arm of `BlockIndexMap`: (Empty, One, Many).
///
/// THE ARM IS STILL THE FOOTPRINT, AND WHAT THE `One` ARM COSTS HAS CHANGED. It used to hold its
/// page INLINE and allocate nothing, so `One` was free on the heap and expensive in every node --
/// the width of a whole entry, paid by every bucket in the map whether or not it held one page. The
/// arm now holds a POINTER: eight bytes in the node plus one allocation sized for the entry.
///
/// SO THE COLUMNS STILL SAY WHAT THEY SAID, WITH ONE TERM CHANGED. Filling a bucket still moves it
/// from `One` to `Many` and still buys list capacity -- but it no longer buys the bucket's FIRST
/// allocation, because the single page already had one. `inline_arm_trade.rs` is the measurement.
fn block_index_arms(engine: &TemporalEngine) -> (usize, usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut arms = (0usize, 0usize, 0usize);
    for bucket in shard.bucket_index.bucket_map.values() {
        match &bucket.block_index {
            crate::engine::state::BlockIndexMap::Empty => arms.0 += 1,
            crate::engine::state::BlockIndexMap::One(_, _) => arms.1 += 1,
            crate::engine::state::BlockIndexMap::Many(_) => arms.2 += 1,
        }
    }
    arms
}

fn total<T>(contents: &BTreeMap<u32, BTreeSet<T>>) -> usize {
    contents.values().map(|set| set.len()).sum()
}

// =============================================================================================
// 1. THE RANGE, NOT THE WORKLOAD, DECIDES THE FILL
// =============================================================================================

/// THE HISTOGRAM AT BOTH RANGES, AT TWO CORPUS SIZES.
///
/// #1959 reported "a store of keys that route one to a bucket is 99.900% single-page" and read it
/// as a fact about routed workloads. Its fixture is on `engine.load_shard(1)` -- the whole
/// keyspace -- where every key lands alone BY CONSTRUCTION and no workload can do otherwise. The
/// same seed on the documented production range fills the bucket.
///
/// THE ANTI-CONSTANT ASSERTION. The narrow arm is asserted to reach a bucket holding more than one
/// page at both sizes. A fixture that only ever produced one page per bucket could not tell a
/// correct `BlockIndexMap` from a constant answering `One`.
///
/// THE STORE PATH LENGTH is held constant across arms and asserted equal. It moves allocation
/// bytes at about six bytes a character; bucket and page COUNTS are immune to it, which is why
/// the counts carry the claim here and the allocator figures are taken separately.
///
/// rust-internal: reads the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds four stores up to 40,000 records each; run by name"]
fn the_pages_a_bucket_holds_are_decided_by_the_routing_range() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut observed: BTreeMap<(usize, u32), PagesPerBucket> = BTreeMap::new();

    for records in [SMALL, LARGE] {
        for end_routing_bucket in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            let hist = pages_per_bucket(&engine);
            let width = if end_routing_bucket == WIDE_END {
                "whole keyspace".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            hist.report(&format!("{records} routed keys on {width}"));

            // DENOMINATOR: every key really is in the index. A run that wrote nothing would
            // report a clean single-page histogram of zero buckets.
            assert_eq!(
                hist.pages(),
                keys.len(),
                "{records}/{end_routing_bucket}: the index holds {} pages for {} written keys; \
                 the histogram below is not over the fixture",
                hist.pages(),
                keys.len()
            );
            observed.insert((records, end_routing_bucket), hist);
        }
    }

    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|length| *length == first),
        "the store path length moved across arms ({path_lengths:?}); it shifts allocation bytes \
         at about six bytes a character"
    );
    println!("  store path length held at {first} characters across all arms");

    for records in [SMALL, LARGE] {
        let wide = observed.get(&(records, WIDE_END)).expect("wide arm");
        let narrow = observed.get(&(records, NARROW_END)).expect("narrow arm");

        // THE WIDE ARM REPRODUCES #1959. One key, one bucket, essentially no collision.
        assert!(
            wide.single_page_fraction() > 0.99,
            "on the whole keyspace {records} routed keys gave a single-page fraction of {:.5}; \
             #1959 measured 0.99900 and this arm is meant to reproduce it",
            wide.single_page_fraction()
        );
        assert_eq!(
            wide.buckets(),
            records - wide.multi_page(),
            "on the whole keyspace the bucket count must be the key count less one for each \
             bucket that took a second key"
        );

        // THE NARROW ARM IS BOUNDED BY THE RANGE, NOT BY THE CORPUS. This is the mechanism: the
        // modulus is the range width, so the bucket count cannot exceed it however many keys
        // arrive.
        assert!(
            narrow.buckets() <= NARROW_BUCKETS,
            "a shard on 0..{NARROW_END} reported {} buckets, which is more than the {NARROW_BUCKETS} \
             the range has; the modulus is not the range width",
            narrow.buckets()
        );

        // THE ANTI-CONSTANT ASSERTION.
        assert!(
            narrow.multi_page() > 0,
            "the narrow arm at {records} records reached no bucket holding more than one page, so \
             it cannot tell a correct `BlockIndexMap` from a constant answering `One`"
        );

        // AND THE FILL IS THE FINDING. The same keys, the same workload, the same engine.
        assert!(
            narrow.single_page_fraction() < 0.25,
            "the narrow arm at {records} records is {:.5} single-page; if a production-range shard \
             were still single-page-dominated then #1959's reading would hold at both widths and \
             the range would not be the lever",
            narrow.single_page_fraction()
        );
        println!(
            "  {records} routed keys: whole keyspace {:.4} pages a bucket over {} buckets, \
             0..{NARROW_END} {:.4} over {} -- a factor of {:.1} on the bucket count",
            wide.mean(),
            wide.buckets(),
            narrow.mean(),
            narrow.buckets(),
            wide.buckets() as f64 / narrow.buckets().max(1) as f64
        );
    }

    // FLAT ACROSS A TENFOLD CORPUS on the wide range -- a property of the range -- and TENFOLD on
    // the narrow one, because there the bucket count is pinned and the pages are not.
    let wide_small = observed.get(&(SMALL, WIDE_END)).expect("wide small");
    let wide_large = observed.get(&(LARGE, WIDE_END)).expect("wide large");
    let narrow_small = observed.get(&(SMALL, NARROW_END)).expect("narrow small");
    let narrow_large = observed.get(&(LARGE, NARROW_END)).expect("narrow large");
    assert!(
        (wide_large.mean() - wide_small.mean()).abs() < 0.01,
        "the whole-keyspace mean moved from {:.4} to {:.4} across a tenfold corpus; on that range \
         it is a property of the range and must not",
        wide_small.mean(),
        wide_large.mean()
    );
    assert!(
        narrow_large.mean() > narrow_small.mean() * 5.0,
        "on 0..{NARROW_END} a tenfold corpus moved the pages a bucket holds from {:.4} only to \
         {:.4}; the bucket count is capped by the range, so the fill is what must grow",
        narrow_small.mean(),
        narrow_large.mean()
    );
}

// =============================================================================================
// 2. THE MECHANISM: ROUTING TAKES THE KEY, THE HANDLE TAKES THE COMPONENT
// =============================================================================================

/// WHY A CONTAINER STORE PUTS A HUNDRED PAGES IN ONE BUCKET, read off the two functions.
///
/// #1959 left this as an observation -- "they all route to their container key's one bucket". It
/// is not a different routing call, a different range or a different key shape. It is the SAME
/// call on the SAME range, and the reason is that the two identities are computed from different
/// things:
///
///   * `block_routing_bucket(object_key, start, end)` takes the OBJECT KEY and nothing else;
///   * `stable_block_object_id(shard, kind, key, component)` -- the page's handle, and the key of
///     `BlockIndexMap` -- takes the COMPONENT as well.
///
/// So a hash's hundred fields are a hundred distinct handles under one object key, and one object
/// key is one bucket at ANY range. Narrowing the range cannot split a container; widening it
/// cannot spread one.
///
/// Asserted three ways: against the functions directly, against a seeded container store, and
/// with the routed store as the partner that says the SAME call gives the other answer.
///
/// rust-internal: reads the engine's own placement functions, no product behaviour
#[test]
#[ignore = "seeds a container store; run by name"]
fn routing_never_sees_the_component_which_is_why_a_containers_pages_share_one_bucket() {
    // --- Against the functions themselves, at both widths. ---
    for end_routing_bucket in [WIDE_END, NARROW_END] {
        let key = "bag-000000";
        let home = block_routing_bucket(key, 0, end_routing_bucket);
        let mut handles: BTreeSet<u64> = BTreeSet::new();
        for f in 0..100 {
            // The routing call takes the key. There is no component argument to give it.
            assert_eq!(
                block_routing_bucket(key, 0, end_routing_bucket),
                home,
                "the routing call moved between two invocations on the same key and range"
            );
            handles.insert(stable_block_object_id(1, "hash", key, Some(&format!("f{f}"))));
        }
        assert_eq!(
            handles.len(),
            100,
            "a hundred components gave {} distinct handles on 0..{end_routing_bucket}; if the \
             handle did not take the component, a container's fields would overwrite each other \
             in one `BlockIndexMap` entry and this whole population would not exist",
            handles.len()
        );
        // The partner: the same call on a DIFFERENT key gives a different bucket, so `home` above
        // is not a constant the function returns regardless of input.
        let neighbours: BTreeSet<u32> = (0..64)
            .map(|k| block_routing_bucket(&format!("bag-{k:06}"), 0, end_routing_bucket))
            .collect();
        assert!(
            neighbours.len() > 1,
            "64 distinct keys routed to {} bucket(s) on 0..{end_routing_bucket}; the routing call \
             is answering a constant and every placement figure in this module is vacuous",
            neighbours.len()
        );
    }

    // --- Against a seeded container store, both widths. ---
    const CONTAINERS: usize = 40;
    const MEMBERS: usize = 100;
    for end_routing_bucket in [WIDE_END, NARROW_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        let keys = seed_container(&engine, CONTAINERS, MEMBERS);

        let contents = bucket_key_sets(&engine);
        let handles = bucket_handle_sets(&engine);
        let hist = pages_per_bucket(&engine);
        hist.report(&format!(
            "{CONTAINERS} containers of {MEMBERS} fields on 0..{end_routing_bucket}"
        ));

        assert_eq!(
            total(&handles),
            CONTAINERS * MEMBERS,
            "the index holds {} pages for {} written fields",
            total(&handles),
            CONTAINERS * MEMBERS
        );

        // EVERY CONTAINER'S PAGES ARE WHERE ITS KEY ROUTES, element by element.
        for key in &keys {
            let home = block_routing_bucket(key, 0, end_routing_bucket);
            let held = contents
                .get(&home)
                .unwrap_or_else(|| panic!("no bucket at {home}, where {key} routes"));
            assert!(
                held.contains(key),
                "{key} routes to bucket {home} on 0..{end_routing_bucket} and that bucket does \
                 not hold it; filing and routing disagree, which is the shape that makes a page \
                 invisible to a scoped reader"
            );
        }

        // AND NO CONTAINER IS SPLIT. Each key's pages are in exactly one bucket.
        let mut buckets_per_key: BTreeMap<&String, usize> = BTreeMap::new();
        for held in contents.values() {
            for key in held {
                if let Some(written) = keys.iter().find(|candidate| *candidate == key) {
                    *buckets_per_key.entry(written).or_default() += 1;
                }
            }
        }
        let split: Vec<&&String> = buckets_per_key
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(key, _)| key)
            .collect();
        assert!(
            split.is_empty(),
            "{} container keys had pages in more than one bucket on 0..{end_routing_bucket} \
             ({:?}); a container is one object key and one object key is one bucket at any range",
            split.len(),
            &split[..split.len().min(5)]
        );
        assert_eq!(
            buckets_per_key.len(),
            CONTAINERS,
            "the walk found {} of {CONTAINERS} container keys in the index",
            buckets_per_key.len()
        );

        // NO SINGLE-PAGE BUCKET, at either width. This is what says the range is not the lever
        // for this population -- only the member count is.
        assert_eq!(
            hist.counts.get(&1).copied().unwrap_or_default(),
            0,
            "a container store on 0..{end_routing_bucket} produced a single-page bucket; every \
             field is its own page under one key, so none is possible"
        );
        assert_eq!(
            hist.widest(),
            MEMBERS,
            "the widest bucket on 0..{end_routing_bucket} holds {} pages, not the {MEMBERS} \
             fields a container was written with",
            hist.widest()
        );
    }
}

// =============================================================================================
// 3. THE STRONG FORM: THE SAME PAGES, BUCKET FOR BUCKET, ACROSS THE TWO RANGES
// =============================================================================================

/// THE PER-BUCKET PAGE SET COMPARED ELEMENT BY ELEMENT AGAINST WHAT ROUTING SAYS IT SHOULD BE.
///
/// Coarser buckets are silent in one direction and finer ones in the other: a reader that scopes
/// by bucket may see pages it should not, and a page filed under a bucket nothing summarises is
/// INVISIBLE. #1949 measured an actuator that chose four victims, dropped no objects and dropped
/// four buckets, because filing and summarising disagreed. A count of pages cannot see either.
///
/// So this asserts the set, not the size, three ways at each width:
///
///   1. the union of every bucket's key set equals the full written key set -- nothing lost;
///   2. each bucket's key set equals EXACTLY the keys routing says belong there -- nothing
///      misfiled, in either direction; and
///   3. the page HANDLE sets are identical across the two widths -- the narrow store holds the
///      same pages as the wide one, regrouped and not rewritten.
///
/// rust-internal: reads the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds two stores of 4,000 records; run by name"]
fn the_narrow_range_holds_the_same_pages_the_wide_one_does_bucket_for_bucket() {
    let mut key_sets: BTreeMap<u32, BTreeMap<u32, BTreeSet<String>>> = BTreeMap::new();
    let mut handle_sets: BTreeMap<u32, BTreeSet<u64>> = BTreeMap::new();
    let mut written: Vec<String> = Vec::new();

    for end_routing_bucket in [WIDE_END, NARROW_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        let keys = seed_routed(&engine, SMALL);
        if written.is_empty() {
            written = keys.clone();
        } else {
            assert_eq!(written, keys, "the two arms were seeded with different keys");
        }

        let contents = bucket_key_sets(&engine);
        let expected: BTreeSet<String> = keys.iter().cloned().collect();

        // 1. NOTHING LOST.
        let held = flatten(&contents);
        assert_eq!(
            held,
            expected,
            "on 0..{end_routing_bucket} the index holds {} distinct keys against {} written; \
             {} are missing and {} are not ours",
            held.len(),
            expected.len(),
            expected.difference(&held).count(),
            held.difference(&expected).count()
        );

        // 2. NOTHING MISFILED, IN EITHER DIRECTION. Built from the routing function rather than
        // from the index, so this is a comparison and not the index against itself.
        let mut routed: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
        for key in &keys {
            routed
                .entry(block_routing_bucket(key, 0, end_routing_bucket))
                .or_default()
                .insert(key.clone());
        }
        assert_eq!(
            contents.keys().copied().collect::<BTreeSet<u32>>(),
            routed.keys().copied().collect::<BTreeSet<u32>>(),
            "on 0..{end_routing_bucket} the index occupies {} buckets and routing names {}; a \
             bucket in one and not the other is a page nothing summarises",
            contents.len(),
            routed.len()
        );
        for (routing_bucket, expected_keys) in &routed {
            let filed = contents
                .get(routing_bucket)
                .unwrap_or_else(|| panic!("bucket {routing_bucket} is filed empty"));
            assert_eq!(
                filed, expected_keys,
                "on 0..{end_routing_bucket} bucket {routing_bucket} is filed with {} keys and \
                 routing puts {} there",
                filed.len(),
                expected_keys.len()
            );
        }
        println!(
            "  0..{end_routing_bucket}: {} buckets, {} keys, every bucket's key set equal to \
             what routing names, element by element",
            contents.len(),
            held.len()
        );

        // 3. Keep the handles for the cross-width comparison.
        let handles = flatten(&bucket_handle_sets(&engine));
        handle_sets.insert(end_routing_bucket, handles);
        key_sets.insert(end_routing_bucket, contents);
    }

    // 3. THE SAME PAGES, REGROUPED. The handle is `stable_block_object_id(shard, kind, key,
    // component)` and carries no bucket, so an identical handle set is what says the narrow store
    // holds the same pages and not merely the same number of them.
    let wide = handle_sets.get(&WIDE_END).expect("wide handles");
    let narrow = handle_sets.get(&NARROW_END).expect("narrow handles");
    assert_eq!(
        wide, narrow,
        "the whole-keyspace store holds {} page handles and the 0..{NARROW_END} store holds {}; \
         {} are in one and not the other. The range regroups pages; it must not change which \
         pages exist",
        wide.len(),
        narrow.len(),
        wide.symmetric_difference(narrow).count()
    );

    // AND THE GROUPING REALLY DID CHANGE, or the equality above is vacuous.
    let wide_buckets = key_sets.get(&WIDE_END).expect("wide keys").len();
    let narrow_buckets = key_sets.get(&NARROW_END).expect("narrow keys").len();
    assert!(
        narrow_buckets < wide_buckets,
        "the two widths occupied {narrow_buckets} and {wide_buckets} buckets; if the grouping did \
         not move, the set equality above compares a store with itself and asserts nothing"
    );
    println!(
        "  the same {} page handles, regrouped from {wide_buckets} buckets into {narrow_buckets}",
        wide.len()
    );
}

// =============================================================================================
// 4. THE CROSS-RANGE RESTORE, IN BOTH DIRECTIONS
// =============================================================================================

/// A STORE WRITTEN ON ONE RANGE AND REOPENED ON THE OTHER, BOTH WAYS.
///
/// `docs/runtime_tuning.md` says "Set this before the first ingest" and warns that "changing the
/// range on a populated store remaps keys to different slots" -- a warning with no measurement
/// behind it. This drives it, in both directions, because the two are not symmetrical:
///
///   * WIDENING (0..1023 then the whole keyspace) moves a page from a bucket the shard holds to
///     one it also holds, since the wide range contains the narrow one; and
///   * NARROWING (the whole keyspace then 0..1023) moves a page from a bucket OUTSIDE the new
///     range to one inside it, and `rebuild_bucket_block_ownership` drops a page whose address
///     carries an explicit bucket outside the range rather than re-filing it.
///
/// Both are asserted at three levels, because "the records are readable" would hold even if every
/// page had been orphaned into a bucket nothing will ever open: every record readable, every page
/// present handle for handle, and every occupied bucket inside the new range.
///
/// rust-internal: drives the engine's own restart, no product behaviour
#[test]
#[ignore = "seeds and restarts two stores; run by name"]
fn a_store_reopened_on_the_other_range_keeps_every_page_in_both_directions() {
    const RECORDS: usize = 2_000;
    let mut grouped: BTreeMap<&str, usize> = BTreeMap::new();

    for (from_end, to_end, direction) in [
        (WIDE_END, NARROW_END, "narrowing"),
        (NARROW_END, WIDE_END, "widening"),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let keys: Vec<String>;
        let handles_before: BTreeSet<u64>;
        let buckets_before: usize;

        {
            let engine = engine_on(dir.path());
            load_on(&engine, from_end);
            keys = seed_routed(&engine, RECORDS);
            let before = bucket_handle_sets(&engine);
            handles_before = flatten(&before);
            buckets_before = before.len();
            assert_eq!(
                handles_before.len(),
                RECORDS,
                "{direction}: the store went in holding {} pages for {RECORDS} keys",
                handles_before.len()
            );
            engine.flush_shard_index(1);
        }

        let engine = engine_on(dir.path());
        load_on(&engine, to_end);

        // PART ONE: every record still readable.
        let readable = read_back(&engine, &keys);
        assert_eq!(
            readable,
            RECORDS,
            "{direction} from 0..{from_end} to 0..{to_end}: {readable} of {RECORDS} records \
             survived. A page filed under a bucket nothing summarises is supposed to be invisible \
             to a SCOPED reader and still reachable by the full walk; unreadable is active loss"
        );

        // PART TWO: every page present, handle for handle. A count would pass while a page was
        // swapped for a duplicate of another.
        let after = bucket_handle_sets(&engine);
        let handles_after = flatten(&after);
        assert_eq!(
            handles_after,
            handles_before,
            "{direction} from 0..{from_end} to 0..{to_end}: {} page handles went in and {} came \
             back; {} are in one and not the other",
            handles_before.len(),
            handles_after.len(),
            handles_before.symmetric_difference(&handles_after).count()
        );

        // PART THREE -- THE FINDING, AND IT IS NOT SYMMETRICAL.
        //
        // The two directions are asserted separately because the engine does two different things:
        //
        //   * WIDENING cannot file anything out of range. The wide range CONTAINS the narrow one,
        //     so every explicit bucket a 0..1023 shard stamped is still inside 0..u32::MAX.
        //   * NARROWING leaves EVERY page filed above the new end. The live write path stamps
        //     `block_routing_bucket(key, start, end)` onto the address at `append_value`, so a
        //     store written on the whole keyspace carries an explicit nine-figure bucket on every
        //     page; `rebuild_bucket_first_index` files a page under its address's own bucket and
        //     filters nothing, so the pages come back where the OLD range put them.
        //
        // The pages are readable and present -- parts one and two above say so -- and that is
        // exactly what makes this quiet. They are simply outside every per-bucket sweep the shard
        // runs: the dump's bucket selection, eviction's victim sampling, the reclaim floor and the
        // release pass all enumerate `bucket_map` against the shard's own range.
        let outside: Vec<u32> = after
            .keys()
            .copied()
            .filter(|routing_bucket| *routing_bucket > to_end)
            .collect();
        let pages_outside: usize = outside
            .iter()
            .filter_map(|routing_bucket| after.get(routing_bucket))
            .map(|handles| handles.len())
            .sum();

        let entries = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1");
            collect_live_block_entries(shard).len()
        };
        println!(
            "  {direction} 0..{from_end} -> 0..{to_end}: {readable}/{RECORDS} readable, \
             {entries} live pages, {buckets_before} buckets -> {} buckets, {} buckets holding \
             {pages_outside} pages above the shard's end",
            after.len(),
            outside.len()
        );

        if to_end == WIDE_END {
            assert!(
                outside.is_empty(),
                "{direction} from 0..{from_end} to 0..{to_end}: {} buckets are occupied above the \
                 shard's end. The wide range CONTAINS the narrow one, so nothing a 0..{from_end} \
                 shard stamped can fall outside it; if this fires, the widening direction has \
                 acquired a hazard the narrowing one has and this test is what has to say so",
                outside.len()
            );
        } else {
            // THE MEASURED HAZARD, asserted element by element rather than as "some". If a later
            // change ever teaches the load to RE-FILE a page whose explicit bucket is outside the
            // new range, this assertion goes red -- and that is the signal to drop the "set this
            // before the first ingest" warning from `docs/runtime_tuning.md`, which is the only
            // thing standing between an operator and this state today.
            assert_eq!(
                pages_outside,
                RECORDS,
                "{direction} from 0..{from_end} to 0..{to_end}: {pages_outside} of {RECORDS} pages \
                 came back filed above the shard's end, not all of them. The engine does not \
                 re-file a page whose address carries an explicit bucket, so either every page is \
                 outside or the write path has stopped stamping one -- a partial count means the \
                 two halves of this fixture disagree and neither figure can be read"
            );
            assert_eq!(
                readable, RECORDS,
                "{direction}: {readable} of {RECORDS} records are readable. The point of this arm \
                 is that narrowing a populated store is SILENT -- every record still reads, which \
                 is why the misfiling is not noticed. If reads break too, this is active loss and \
                 the warning in `docs/runtime_tuning.md` is far too mild"
            );
        }

        // AND THE MECHANISM BEHIND BOTH DIRECTIONS, asserted rather than described: REOPENING ON
        // A DIFFERENT RANGE DOES NOT MOVE A PAGE AT ALL.
        //
        // The range is not a filter applied to a stored placement -- it is an argument to the
        // placement function, and only for an address that carries NO bucket of its own. The live
        // write path stamps one at `append_value`, so on a populated store every address already
        // has its answer and the reopened range is never consulted. The bucket count is therefore
        // identical across the restart in BOTH directions, and that single fact explains the
        // asymmetry above: narrowing strands pages above the new end because nothing re-files
        // them, and widening is safe only because the wide range happens to contain the narrow
        // one -- not because anything moved.
        assert_eq!(
            buckets_before,
            after.len(),
            "{direction} from 0..{from_end} to 0..{to_end}: the store occupied {buckets_before} \
             buckets before the restart and {} after. A reopened range is consulted only for an \
             address carrying no explicit bucket, so a populated store's index must come back \
             grouped exactly as it went in. If this moves, the engine has acquired a re-filing \
             pass and the whole 'set this before the first ingest' rule can be revisited",
            after.len()
        );
        grouped.insert(direction, buckets_before);
    }

    // THE NON-VACUITY CHECK FOR THE WHOLE TEST. Every assertion above says the index did NOT move
    // across a restart, which would also hold if the range never decided anything. It does decide
    // -- at WRITE time -- and the proof is that the two arms, seeded identically and differing
    // only in the range they were WRITTEN on, went in grouped differently.
    let written_wide = *grouped.get("narrowing").expect("the narrowing arm was written wide");
    let written_narrow = *grouped.get("widening").expect("the widening arm was written narrow");
    println!(
        "  written on the whole keyspace: {written_wide} buckets; written on 0..{NARROW_END}: \
         {written_narrow} buckets -- the range decides filing at WRITE time and only then"
    );
    assert!(
        written_narrow < written_wide,
        "the arm written on 0..{NARROW_END} occupied {written_narrow} buckets and the arm written \
         on the whole keyspace occupied {written_wide}. If the two are not different then the \
         range decided nothing even at write time, and every 'the index did not move' assertion \
         above is trivially true"
    );
    assert!(
        written_narrow <= NARROW_BUCKETS,
        "the arm written on 0..{NARROW_END} occupied {written_narrow} buckets, more than the \
         {NARROW_BUCKETS} that range has"
    );
}

// =============================================================================================
// 5. WHAT THE FILL IS WORTH, PER PAGE, ON ONE INSTRUMENT
// =============================================================================================

/// What one `clone()` of a value charges the allocator, in bytes and in CALLS.
///
/// Cloning a `BTreeMap` rebuilds it node for node, so this is the map's OWN allocations, owing
/// nothing to `size_of` x count. #1959 found a published decline whose sign was wrong because it
/// set a `size_of` saving against an allocator cost; both sides here are this one instrument.
#[cfg(feature = "alloc-probe")]
fn clone_counts<T: Clone>(value: &T) -> (u64, u64) {
    let probe = Probe::start();
    let copy = value.clone();
    let counts = probe.stop();
    std::hint::black_box(&copy);
    drop(copy);
    (counts.alloc_bytes, counts.allocs)
}

/// THE PLANTED MARKER. Recovered exactly, or every figure below is noise.
///
/// The failure this guards against is the one that reads as good news: an instrument reporting
/// near zero makes a shape look free.
///
/// RUN BY NAME, NOT IN THE SUITE. The counting allocator is PROCESS-WIDE, so under the
/// suite's default parallelism this span also charges whatever other test threads allocate
/// while it is open, and the planted figure comes back inflated. The measured clean-main gate
/// carries the identically-named control in `pages_per_bucket` as a standing failure for
/// exactly that reason; this one is `#[ignore]`d instead of joining it.
///
/// rust-internal: measures the harness's own instrument, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn the_clone_instrument_used_here_recovers_a_planted_megabyte_exactly() {
    const PLANTED: usize = 1 << 20;
    let marker: Vec<u8> = vec![0xA5; PLANTED];
    let (bytes, allocs) = clone_counts(&marker);
    println!("planted {PLANTED} B, instrument charged {bytes} B in {allocs} call(s)");
    assert_eq!(
        PLANTED as u64, bytes,
        "the clone instrument charged {bytes} B for a planted {PLANTED} B; it is not measuring \
         what it is being read as measuring"
    );
    assert_eq!(
        1, allocs,
        "one planted vector charged {allocs} allocations, not one"
    );
}

/// BYTES AND ALLOCATIONS PER PAGE, AT BOTH RANGES, AT TWO CORPUS SIZES.
///
/// PER PAGE and not per bucket, because the point #1959 established is that the node's 200 bytes
/// are expensive only because there is one node per page. Fill the bucket and the node amortises
/// -- BUT NOT FOR FREE, AND NOT ALWAYS FOR A WIN. This test was first written as
/// `narrowing_the_routing_range_lowers_the_bytes_and_the_allocations_a_page_pays` and the
/// measurement refuted its own name at the small corpus, which is why it now names the TRADE
/// instead of a direction: a guard whose name states a false claim is a guard that will one day
/// be read instead of run.
///
/// ONE INSTRUMENT for both sides: the counting allocator over a clone of the real `bucket_map`,
/// as production built it. No `size_of` arithmetic appears in the comparison.
///
/// BOTH FIGURES ARE REPORTED AND NEITHER DIRECTION IS ASSUMED. The allocator rounds a request up
/// to a size class, which can swallow a byte win while leaving the allocation count clean, so the
/// two can disagree in magnitude AND IN SIGN.
///
/// AND THE SIGN IS NOT OBVIOUS HERE, WHICH IS THE POINT OF THE ARM COLUMNS. `BlockIndexMap::One`
/// holds its page INLINE and allocates nothing; `Many` is a `BTreeMap` whose node is sized for
/// eleven entries whether or not it fills them. Filling a bucket moves it from the arm that
/// allocates nothing onto the arm that allocates a node, so a fill too SMALL to amortise that node
/// makes the page index cost MORE per page, not less. The arm distribution is reported at every
/// arm so that a reader can see which way each size went and why, rather than being handed a
/// ratio.
///
/// rust-internal: measures the engine's own bucket map, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "seeds four stores up to 40,000 records each; run by name"]
fn filling_a_bucket_trades_a_node_a_page_for_a_map_node_a_bucket_and_the_fill_decides_the_sign() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut observed: BTreeMap<(usize, u32), Arm> = BTreeMap::new();

    for records in [SMALL, LARGE] {
        for end_routing_bucket in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            let arms = block_index_arms(&engine);

            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1");
            let buckets = shard.bucket_index.bucket_map.len();
            let pages: usize = shard
                .bucket_index
                .bucket_map
                .values()
                .map(|bucket| bucket.block_index.len())
                .sum();
            assert_eq!(
                pages,
                keys.len(),
                "{records}/{end_routing_bucket}: the map holds {pages} pages for {} keys, so the \
                 per-page divisor below is not the fixture's",
                keys.len()
            );
            assert!(buckets > 0, "{records}/{end_routing_bucket}: no buckets to divide by");

            let (bytes, allocs) = clone_counts(&shard.bucket_index.bucket_map);
            assert!(
                bytes > 0 && allocs > 0,
                "{records}/{end_routing_bucket}: the instrument charged {bytes} B in {allocs} \
                 calls for a map of {buckets} buckets; a zero reading is the instrument failing, \
                 not the map being free"
            );
            observed.insert(
                (records, end_routing_bucket),
                Arm {
                    bytes,
                    allocs,
                    buckets,
                    pages,
                    arms,
                },
            );
        }
    }

    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|length| *length == first),
        "the store path length moved across arms ({path_lengths:?}); allocation bytes move at \
         about six bytes a character and the byte column would carry it"
    );
    println!("  store path length held at {first} characters across all arms");
    println!(
        "  {:>8} {:>12} {:>9} {:>8} {:>9} {:>12} {:>20}",
        "records", "range end", "buckets", "pages", "B/page", "allocs/page", "index arms E/One/Many"
    );
    for ((records, end_routing_bucket), row) in &observed {
        println!(
            "  {records:>8} {:>12} {:>9} {:>8} {:>9.1} {:>12.4} {:>7}/{:>5}/{:>6}",
            if *end_routing_bucket == WIDE_END {
                "whole".to_string()
            } else {
                end_routing_bucket.to_string()
            },
            row.buckets,
            row.pages,
            row.bytes_per_page(),
            row.allocs_per_page(),
            row.arms.0,
            row.arms.1,
            row.arms.2
        );
    }

    for records in [SMALL, LARGE] {
        let wide = observed.get(&(records, WIDE_END)).expect("wide arm");
        let narrow = observed.get(&(records, NARROW_END)).expect("narrow arm");

        assert_eq!(
            wide.pages, narrow.pages,
            "the two arms at {records} records hold {} and {} pages; a per-page figure over \
             different page counts is two questions",
            wide.pages, narrow.pages
        );
        assert!(
            narrow.buckets < wide.buckets,
            "at {records} records the narrow arm occupied {} buckets against the wide arm's {}; \
             without fewer buckets there is no amortising to measure",
            narrow.buckets,
            wide.buckets
        );

        // THE ARMS MOVED, or the per-page figures are two readings of the same shape.
        assert_eq!(
            wide.arms.2, 0,
            "on the whole keyspace {} buckets are already on the `Many` arm at {records} records; \
             at one page a bucket none should be, and if any are then the wide arm is not the \
             single-page shape every figure here is compared against",
            wide.arms.2
        );
        assert!(
            narrow.arms.2 > 0,
            "on 0..{NARROW_END} no bucket reached the `Many` arm at {records} records, so the \
             fill did not happen and the per-page figures below compare two single-page stores"
        );

        // NEITHER DIRECTION IS ASSERTED. The fill trades one node per PAGE for one node per
        // BUCKET plus a `BTreeMap` node per filled bucket, and which of those is larger depends
        // on the fill -- so the sign is reported, with the arm counts that explain it.
        println!(
            "  {records} records: allocations a page {:.4} -> {:.4} ({:+.1}%); bytes a page \
             {:.1} -> {:.1} ({:+.1}%); page index arms One {} -> {}, Many {} -> {}",
            wide.allocs_per_page(),
            narrow.allocs_per_page(),
            100.0 * (narrow.allocs_per_page() - wide.allocs_per_page()) / wide.allocs_per_page(),
            wide.bytes_per_page(),
            narrow.bytes_per_page(),
            100.0 * (narrow.bytes_per_page() - wide.bytes_per_page()) / wide.bytes_per_page(),
            wide.arms.1,
            narrow.arms.1,
            wide.arms.2,
            narrow.arms.2
        );
    }

    // WHAT IS ASSERTED IS THE MECHANISM, NOT THE SIGN: a deeper fill amortises the map node
    // better than a shallow one, so whatever the sign at the small corpus, the large corpus must
    // be the better of the two. This is the claim a tuning decision actually rests on, and it is
    // the one direction that cannot be true by accident.
    let small_wide = observed.get(&(SMALL, WIDE_END)).expect("small wide");
    let small_narrow = observed.get(&(SMALL, NARROW_END)).expect("small narrow");
    let large_wide = observed.get(&(LARGE, WIDE_END)).expect("large wide");
    let large_narrow = observed.get(&(LARGE, NARROW_END)).expect("large narrow");
    let small_ratio = small_narrow.allocs_per_page() / small_wide.allocs_per_page();
    let large_ratio = large_narrow.allocs_per_page() / large_wide.allocs_per_page();
    println!(
        "  the fill deepens from {:.2} to {:.2} pages a bucket between the two corpus sizes, and \
         the allocation ratio moves from {small_ratio:.3} to {large_ratio:.3}",
        small_narrow.pages as f64 / small_narrow.buckets as f64,
        large_narrow.pages as f64 / large_narrow.buckets as f64
    );
    assert!(
        large_ratio < small_ratio,
        "the narrow range costs {large_ratio:.3}x the allocations a page at {LARGE} records and \
         {small_ratio:.3}x at {SMALL}. A deeper fill spreads the map node over more pages, so the \
         larger corpus must be the better of the two; if it is not, the node is not what the \
         per-page figure is tracking"
    );
}

/// One arm of the allocator table: what the map charged, over what it was holding.
#[cfg(feature = "alloc-probe")]
struct Arm {
    bytes: u64,
    allocs: u64,
    buckets: usize,
    pages: usize,
    /// `BlockIndexMap` arms as (Empty, One, Many) -- the footprint the byte column cannot explain.
    arms: (usize, usize, usize),
}

#[cfg(feature = "alloc-probe")]
impl Arm {
    fn bytes_per_page(&self) -> f64 {
        self.bytes as f64 / self.pages as f64
    }

    fn allocs_per_page(&self) -> f64 {
        self.allocs as f64 / self.pages as f64
    }
}

// =============================================================================================
// 6. WHAT THE FILL COSTS: THE DIRTY SET AND THE DUMP UNIT COARSEN BY THE BUCKET'S KEY COUNT
// =============================================================================================

/// THE OTHER SIDE OF THE TRADE, MEASURED.
///
/// `docs/runtime_tuning.md` sells the narrow range as "about 45% less resident memory at no cost
/// on disk" and names no cost at all. There is one, and it is not on disk: a routing bucket is the
/// unit of the dirty set and of the dump, and both coarsen by exactly the bucket's key count.
///
///   * `DirtyObjectIndex::drain_buckets` is the drain a dump runs once its manifest is durable. It
///     drops every dirty key of the named buckets. On the whole keyspace that is one key; on
///     0..1023 it is the whole group.
///   * `DirtyKeySet` holds one key inline and promotes to a boxed `BTreeSet` beyond that, so the
///     narrow range moves almost every dirty bucket into the `Many` arm -- the arm whose node is
///     sized for eleven.
///   * `refresh_one_bucket_runtime_flags` sets `bucket.dirty` as an OR over every page, so one
///     dirty key marks the whole group and a dump of that bucket writes every key in it.
///
/// Measured rather than argued: the drain is run and what it drops is counted, at both widths.
///
/// rust-internal: reads the engine's own dirty index, no product behaviour
#[test]
#[ignore = "seeds two stores of 4,000 records; run by name"]
fn filling_a_bucket_coarsens_the_dirty_set_by_exactly_the_keys_it_holds() {
    let mut observed: BTreeMap<u32, (usize, usize, usize, usize, usize)> = BTreeMap::new();

    for end_routing_bucket in [WIDE_END, NARROW_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        let keys = seed_routed(&engine, SMALL);

        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1");

        // DENOMINATOR: a freshly written store has every key dirty. If it did not, the drain
        // below would drop nothing and report a clean zero for the wrong reason.
        let dirty_buckets: Vec<(u32, u64)> = shard.dirty_objects.bucket_counts().collect();
        let dirty_keys: usize = dirty_buckets.iter().map(|(_, count)| *count as usize).sum();
        assert_eq!(
            dirty_keys,
            keys.len(),
            "on 0..{end_routing_bucket} the dirty index holds {dirty_keys} keys after writing {}; \
             the drain below is not over the fixture",
            keys.len()
        );

        let (empty_arm, one_arm, many_arm) = shard.dirty_objects.bucket_arms();
        assert_eq!(
            empty_arm, 0,
            "on 0..{end_routing_bucket} {empty_arm} dirty buckets hold an Empty key set; a set \
             that empties is meant to be dropped from the map by its caller"
        );

        // THE MEASUREMENT: drain ONE bucket -- what a dump of one bucket drops -- and count it.
        // The widest bucket, because that is the amplification an operator actually meets.
        let widest = dirty_buckets
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(routing_bucket, _)| *routing_bucket)
            .expect("a freshly written store has a dirty bucket");
        let dropped = shard.dirty_objects.drain_buckets(&[widest]);
        assert!(
            dropped > 0,
            "on 0..{end_routing_bucket} draining bucket {widest} dropped nothing, so this arm \
             measures the drain not running rather than what it drains"
        );

        println!(
            "  0..{end_routing_bucket}: {} dirty buckets over {dirty_keys} dirty keys; \
             DirtyKeySet arms (Empty {empty_arm}, One {one_arm}, Many {many_arm}); draining the \
             widest bucket dropped {dropped} keys"
        , dirty_buckets.len());
        observed.insert(
            end_routing_bucket,
            (dirty_buckets.len(), dirty_keys, one_arm, many_arm, dropped),
        );
    }

    let (wide_buckets, _, wide_one, wide_many, wide_dropped) =
        *observed.get(&WIDE_END).expect("wide arm");
    let (narrow_buckets, _, narrow_one, narrow_many, narrow_dropped) =
        *observed.get(&NARROW_END).expect("narrow arm");

    // ON THE WHOLE KEYSPACE the dirty set is what #1958 measured: one key a bucket, the inline
    // arm, and a dump of one bucket drops one key.
    assert_eq!(
        wide_dropped, 1,
        "on the whole keyspace a dump of one bucket drained {wide_dropped} keys; #1958 measured \
         40,040 of 40,040 buckets holding exactly one"
    );
    assert_eq!(
        wide_many, 0,
        "on the whole keyspace {wide_many} dirty buckets are in the boxed `Many` arm; at one key \
         a bucket none should be"
    );

    // ON THE PRODUCTION RANGE both move, and the amplification is the bucket's key count.
    assert!(
        narrow_dropped > wide_dropped,
        "on 0..{NARROW_END} a dump of one bucket drained {narrow_dropped} keys against the whole \
         keyspace's {wide_dropped}; if they were equal the fill would be free and this whole \
         section would be unnecessary"
    );
    assert!(
        narrow_many > 0,
        "on 0..{NARROW_END} no dirty bucket reached the boxed `Many` arm of `DirtyKeySet`, so the \
         narrow range did not fill a dirty bucket and the amplification above is not what it says"
    );
    println!(
        "  the cost of the fill: a dump of one bucket drains {wide_dropped} key on the whole \
         keyspace and {narrow_dropped} on 0..{NARROW_END}; `DirtyKeySet` goes from \
         (One {wide_one}, Many {wide_many}) over {wide_buckets} dirty buckets to \
         (One {narrow_one}, Many {narrow_many}) over {narrow_buckets}"
    );
}

/// WHY THE OBJECT-TO-BLOCKS LOOKUP CANNOT BE ANSWERED BY SCANNING ONE BUCKET.
///
/// `ObjectBlockLookup` holds six levels -- two `BTreeMap`s, `ComponentList`, `ComponentBlocks`,
/// `BlockRefs` and `BlockLookupRef` -- and every block entry in a bucket already carries the
/// object key that would let a scan answer the same question. The lookup is also already
/// `skip_serializing` and rebuilt by `rebuild_object_block_lookup` on load, so it is provably a
/// cache of the bucket map and nothing about it is authoritative.
///
/// So the question is not whether the ANSWER is derivable -- a load derives it -- but whether a
/// READ can derive it, one object at a time, and the cache be dropped. That needs the bucket to
/// scan. There is exactly one function that could supply it, `block_routing_bucket(key, start,
/// end)`, and THE RANGE IT TAKES IS NOT A PROPERTY OF THE STORE. It is a deployment setting
/// (`TS_SHARD_END_ROUTING_BUCKET`, documented at 1023, defaulted to `u32::MAX`), and this
/// module's own header records that narrowing a populated store re-files nothing.
///
/// This turns that prose into an assertion. A page filed on one range sits where THAT range put
/// it for the rest of its life, so a bucket recomputed from the key and the range in force names
/// a bucket that does not hold the object -- and on a narrowed shard cannot even be a bucket the
/// shard holds. `BlockLookupRef::routing_bucket` records where a block ACTUALLY is, which is the
/// one thing the key cannot be asked for.
///
/// THE POSITIVE CONTROL IS THE SAME CALL AT THE RANGE THE STORE WAS WRITTEN ON, where it agrees
/// for every key. Without it a computation that was simply broken would read as this finding.
///
/// rust-internal: reads the engine's own placement function, no product behaviour
#[test]
#[ignore = "seeds a routed store; run by name"]
fn an_objects_bucket_cannot_be_recomputed_from_its_key_once_the_range_has_moved() {
    const KEYS: usize = 4_000;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, WIDE_END);
    let keys = seed_routed(&engine, KEYS);

    // Where every object's blocks ACTUALLY sit, read off the bucket map.
    let contents = bucket_key_sets(&engine);
    let mut actual: BTreeMap<String, u32> = BTreeMap::new();
    for (routing_bucket, held) in &contents {
        for key in held {
            actual.insert(key.clone(), *routing_bucket);
        }
    }
    assert_eq!(
        actual.len(),
        KEYS,
        "the fixture placed {} of {KEYS} keys; every figure below divides by this denominator",
        actual.len()
    );

    // --- POSITIVE CONTROL: recomputing at the range the store was WRITTEN on agrees. ---
    let mut agreed_same_range = 0usize;
    for (key, sits_in) in &actual {
        if block_routing_bucket(key, 0, WIDE_END) == *sits_in {
            agreed_same_range += 1;
        }
    }
    assert_eq!(
        agreed_same_range, KEYS,
        "recomputing the bucket at the range the store was written on agreed for only \
         {agreed_same_range} of {KEYS} keys; the placement call is not answering what the bucket \
         map holds, and the disagreement measured below would be that defect rather than the \
         range moving"
    );

    // --- THE FINDING: the operator narrows to the documented production range. ---
    // Nothing is re-filed, so every block stays where the wide range put it while a recomputation
    // answers inside 0..NARROW_END.
    let mut disagreed = 0usize;
    let mut unnameable = 0usize;
    for (key, sits_in) in &actual {
        if block_routing_bucket(key, 0, NARROW_END) != *sits_in {
            disagreed += 1;
        }
        if *sits_in > NARROW_END {
            unnameable += 1;
        }
    }
    println!(
        "  a store written on 0..{WIDE_END} and reopened on 0..{NARROW_END}: a bucket recomputed \
         from the object key disagrees with the bucket the blocks are in for {disagreed} of \
         {KEYS} objects ({:.2}%), and {unnameable} of {KEYS} ({:.2}%) sit above {NARROW_END}, \
         which the narrowed shard cannot name at all",
        disagreed as f64 * 100.0 / KEYS as f64,
        unnameable as f64 * 100.0 / KEYS as f64,
    );
    assert!(
        disagreed * 100 > KEYS * 99,
        "only {disagreed} of {KEYS} objects moved; if a recomputed bucket were right for nearly \
         all of them a read could derive its own bucket and this refutation would not hold"
    );
    assert!(
        unnameable > 0,
        "no object sits above {NARROW_END} on a store written over the whole keyspace, so the \
         narrowed shard could reach every block it holds and the second half of this claim is \
         unfounded"
    );

    // And the stored lookup is right exactly where the recomputation is wrong: its refs carry the
    // bucket as filed rather than as computed.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut checked = 0usize;
    for (_model, object, refs) in shard.bucket_index.object_block_lookup.iter() {
        let sits_in = actual.get(object.as_ref()).copied();
        for block_ref in refs.all_refs() {
            assert_eq!(
                Some(block_ref.routing_bucket),
                sits_in,
                "the lookup files {object} in bucket {} while the bucket map holds it in {sits_in:?}",
                block_ref.routing_bucket
            );
            checked += 1;
        }
    }
    assert_eq!(
        checked, KEYS,
        "checked {checked} stored block refs against {KEYS} objects; a lookup that held none \
         would agree with the bucket map trivially"
    );
}

/// WHAT A DERIVED READ WOULD COST, COUNTED, AT BOTH POPULATIONS.
///
/// Suppose the bucket were somehow known. A bucket's blocks are held as a flat list sorted by
/// HANDLE, and `block_index_handle` hashes the object key, the component and the whole address
/// together -- so the list's order carries no trace of the object key. Finding one object's
/// blocks in it is a walk of the WHOLE list: there is no bisection, because the list is not
/// ordered on what is being looked for, and no early stop, because nothing says how many blocks
/// the object has.
///
/// A ONE-BLOCK OBJECT IS THE CASE THAT REGRESSES, and it must not be averaged with the other.
/// Today its answer is `ComponentList::One` held inline: nothing is probed. Derived, it is the
/// bucket's whole list. This reports the two populations separately, with the histogram of what
/// a derived walk would examine.
///
/// rust-internal: reads the engine's own bucket map, no product behaviour
#[test]
#[ignore = "seeds two stores; run by name"]
fn deriving_an_objects_blocks_from_the_flat_list_walks_the_whole_bucket() {
    #[derive(Default)]
    struct Walk {
        lengths: Vec<usize>,
        components: Vec<usize>,
        scattered: usize,
        objects: usize,
    }

    fn measure(engine: &TemporalEngine) -> Walk {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let mut walk = Walk::default();
        for bucket in shard.bucket_index.bucket_map.values() {
            let entries: Vec<(u64, String)> = bucket
                .block_index
                .iter()
                .map(|(handle, page)| (*handle, page.object_key.to_string()))
                .collect();
            if entries.is_empty() {
                continue;
            }
            let mut per_object: BTreeMap<String, Vec<usize>> = BTreeMap::new();
            for (at, (_handle, key)) in entries.iter().enumerate() {
                per_object.entry(key.clone()).or_default().push(at);
            }
            for positions in per_object.values() {
                walk.objects += 1;
                // A walk cannot stop before the end: it has no count to stop on.
                walk.lengths.push(entries.len());
                walk.components.push(positions.len());
                let first = *positions.first().expect("non-empty");
                let last = *positions.last().expect("non-empty");
                if last - first + 1 != positions.len() {
                    walk.scattered += 1;
                }
            }
        }
        walk
    }

    fn pct(sorted: &[usize], p: f64) -> usize {
        if sorted.is_empty() {
            return 0;
        }
        let at = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[at]
    }

    fn report(label: &str, walk: &Walk) {
        let mut sorted = walk.lengths.clone();
        sorted.sort_unstable();
        let mean = sorted.iter().sum::<usize>() as f64 / sorted.len() as f64;
        let blocks: usize = walk.components.iter().sum();
        let mean_blocks = blocks as f64 / walk.objects as f64;
        // What the shipped shape probes: the `One` arm is inline and probes nothing, the `Many`
        // arm bisects.
        let probes: f64 = walk
            .components
            .iter()
            .map(|n| if *n <= 1 { 0.0 } else { (*n as f64).log2().ceil() })
            .sum::<f64>()
            / walk.objects as f64;
        println!(
            "  {label}: {} objects, blocks/object mean {mean_blocks:.3} (MAX {}) | a derived walk \
             examines p50 {} p90 {} p99 {} MAX {} mean {mean:.4} entries | the shipped lookup \
             probes {probes:.4} | {} of {} objects have their blocks SCATTERED through the list",
            walk.objects,
            walk.components.iter().copied().max().unwrap_or(0),
            pct(&sorted, 0.50),
            pct(&sorted, 0.90),
            pct(&sorted, 0.99),
            sorted.last().copied().unwrap_or(0),
            walk.scattered,
            walk.objects,
        );
    }

    // POPULATION ONE: routed keys on the documented production range -- one block an object.
    let routed = {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);
        let keys = seed_routed(&engine, LARGE);
        assert_eq!(keys.len(), LARGE, "the routed fixture seeded {} keys", keys.len());
        let walk = measure(&engine);
        report("ROUTED, one block an object", &walk);
        walk
    };
    assert!(
        routed.objects > 0,
        "the routed arm measured no objects, so every figure it reports is vacuous"
    );
    assert_eq!(
        routed.components.iter().copied().max(),
        Some(1),
        "the routed arm's widest object holds {:?} blocks; this population is defined by holding \
         one, and if it does not the two populations below are the same population",
        routed.components.iter().copied().max()
    );

    // POPULATION TWO: containers on the same range -- a hundred blocks an object.
    let container = {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, NARROW_END);
        let keys = seed_container(&engine, 40, 100);
        assert_eq!(keys.len(), 40, "the container fixture seeded {} keys", keys.len());
        let walk = measure(&engine);
        report("CONTAINER, a hundred blocks an object", &walk);
        walk
    };
    assert_eq!(
        container.components.iter().copied().max(),
        Some(100),
        "the container arm's widest object holds {:?} blocks rather than 100, so it is not the \
         population it claims to be",
        container.components.iter().copied().max()
    );

    // NEITHER ARM ABOVE CAN ANSWER WHETHER AN OBJECT'S BLOCKS ARE CONTIGUOUS IN THE LIST, and
    // both reported zero scattered. That zero is vacuous, and saying so is the point: 40 containers
    // on 1024 buckets is ONE multi-block object a bucket, and a routed object holds ONE block, so
    // in both arms every object is trivially a contiguous run of the whole or of one. A third arm
    // on a range narrow enough to put SEVERAL containers in one bucket is what exercises it.
    let crowded = {
        const CROWDED_END: u32 = 7;
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, CROWDED_END);
        let keys = seed_container(&engine, 40, 100);
        assert_eq!(keys.len(), 40, "the crowded fixture seeded {} keys", keys.len());
        let walk = measure(&engine);
        report("CROWDED, several containers to a bucket", &walk);
        walk
    };
    assert!(
        crowded.lengths.iter().copied().max().unwrap_or(0) > 100,
        "the crowded arm's widest bucket holds {:?} entries, which is no more than one container's \
         hundred; the range was not narrow enough to put two containers in one bucket and the \
         scattering figure below is as vacuous as the two above",
        crowded.lengths.iter().copied().max()
    );
    println!(
        "  CONTIGUITY, on the one arm that can answer it: {} of {} objects have their blocks \
         SCATTERED through their bucket's list rather than sitting in one run. The list is sorted \
         by HANDLE and the handle hashes the object key, the component and the address together, \
         so the order carries no trace of the object -- a derived walk cannot bisect to a run and \
         cannot stop at the end of one.",
        crowded.scattered, crowded.objects
    );
    assert_eq!(
        crowded.scattered, crowded.objects,
        "{} of {} objects sharing a bucket kept their blocks in one contiguous run; if handle \
         order grouped an object's blocks a derived walk could bound itself and the claim above \
         is wrong",
        crowded.scattered, crowded.objects
    );

    // THE REGRESSION, STATED AS ONE. A one-block object is answered today without a probe.
    let mut routed_sorted = routed.lengths.clone();
    routed_sorted.sort_unstable();
    let p50 = pct(&routed_sorted, 0.50);
    assert!(
        p50 > 1,
        "the routed arm's median bucket holds {p50} entries, so a walk would be free and there is \
         no regression to report; the fixture is not on a range that fills a bucket"
    );
    println!(
        "  THE SERVING REGRESSION, NOT AVERAGED: an object holding ONE block is answered today by \
         an inline arm with ZERO probes. Derived, the same answer is a walk of its bucket's whole \
         list -- p50 {p50}, p99 {}, MAX {} entries, each a string comparison against the object \
         key. There is no length at which the walk is the cheaper of the two.",
        pct(&routed_sorted, 0.99),
        routed_sorted.last().copied().unwrap_or(0),
    );
}

/// A DERIVED ANSWER COMES BACK IN THE WRONG ORDER.
///
/// `ObjectBlockRefs::position` BISECTS `by_component`, and its doc says the order is the one "a
/// caller would expect": components ascending, `None` first, matching `Option`'s own ordering.
/// That order is a precondition of the bisection, not a convenience -- `refs_for` is `position`
/// plus an index.
///
/// A walk of the bucket's flat list yields handle order, and the handle is a hash. So a derived
/// answer would have to SORT its result on every read to be usable by the bisection that reads
/// it, and a derived answer that skipped the sort would silently reorder a container's members
/// while still type-checking. This asserts the two orders differ, so the sort is not optional.
///
/// rust-internal: reads the engine's own bucket map, no product behaviour
#[test]
#[ignore = "seeds a container store; run by name"]
fn a_walk_of_the_flat_list_answers_in_handle_order_not_component_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, NARROW_END);
    let keys = seed_container(&engine, 4, 100);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    let mut compared = 0usize;
    let mut differed = 0usize;
    for key in &keys {
        // The order a walk of the flat list would produce: handle order, filtered to this object.
        let mut walked: Vec<String> = Vec::new();
        for bucket in shard.bucket_index.bucket_map.values() {
            for (_handle, page) in bucket.block_index.iter() {
                if &*page.object_key == key.as_str() {
                    walked.push(page.component.as_deref().unwrap_or("").to_string());
                }
            }
        }
        // The order the lookup holds, which is what `position` bisects.
        let mut held: Vec<String> = Vec::new();
        for kind in crate::engine::storage_model_kinds() {
            if let Some(entry) = shard.bucket_index.object_block_refs(kind, key) {
                for component in entry.by_component.iter() {
                    held.push(component.component.as_deref().unwrap_or("").to_string());
                }
            }
        }
        if held.is_empty() {
            continue;
        }
        assert_eq!(
            walked.len(),
            held.len(),
            "{key}: a walk found {} blocks and the lookup holds {}; the two are not looking at \
             the same object and the order comparison below is meaningless",
            walked.len(),
            held.len()
        );
        let mut sorted = walked.clone();
        sorted.sort();
        assert_eq!(
            sorted, held,
            "{key}: the lookup's order is not the walk's order SORTED, so the derived answer \
             cannot be repaired by sorting and the two shapes disagree on content, not just order"
        );
        compared += 1;
        if walked != held {
            differed += 1;
        }
    }
    assert!(
        compared > 0,
        "compared no objects, so this test asserts nothing about either order"
    );
    println!(
        "  {differed} of {compared} container objects come back from a walk in an order that is \
         NOT the component order the lookup holds and `position` bisects; a derived read must \
         sort, and one that did not would reorder a container's members silently"
    );
    assert_eq!(
        differed, compared,
        "only {differed} of {compared} objects were reordered by the walk; if handle order and \
         component order agreed, the sort would be free and this cost would not exist"
    );
}
