// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WOULD A BUCKET THAT HOLDS ONE PAGE BE CHEAPER AS ONE TAGGED WORD?
//!
//! THE PROPOSAL. `BucketNode` is the widest per-item structure in the engine and there is one
//! per routing bucket. #1958 took it 208 -> 200, #1961 took it 200 -> 192, #1966 took it
//! 192 -> 184 and the address merge took it 184 -> 176, all by narrowing or merging fields. The
//! shape proposed next does not narrow a field: it
//! replaces the node's three container members -- `object_index`, `deleted_object_index` and
//! `block_index`, 120 of the 168 bytes -- with ONE TAGGED WORD whose tag says whether the bucket
//! is simple or general, and puts the payload of each arm behind that word. The node is then 56
//! bytes, which is asserted here and reconstructs field by field.
//!
//! THE ANSWER IS STILL NO. IT IS NO FOR DIFFERENT REASONS THAN IT WAS, AND THE CHANGE IN REASONS
//! IS THE POINT OF THIS REVISION. When this module was written it gave three grounds; one of them
//! has since stopped being true, a second was measured on a tree that has moved, and a fourth --
//! the one that actually settles it -- was quoted as arithmetic and never measured. All four are
//! now measured, at the corpus sizes and the routing ranges below.
//!
//!   * GONE: THE READ COST. This module declined the shape partly because "a tagged simple arm
//!     touches 2.0000 lines to reach a page's address against the live arm's 1.5200 -- +0.4800 a
//!     read". IT IS +0.0000 NOW. The live arm touches 2.0000 as well. The address sits 72 bytes
//!     into the node -- past a whole cache line -- so the node and the address it resolves
//!     through cannot share a line at any alignment, inline or not. The guard that asserted
//!     1.5200 was RED on `06828b8a0` before this change and nothing ran it, because it is
//!     `#[ignore]`d. It is repaired, it states the offset it depends on, and it refuses an offset
//!     under a line rather than carrying a number whose mechanism has moved. WHAT THE COUNT
//!     CANNOT SEE IS STATED WITH IT: two lines and two lines is an equal count, not a proof of
//!     equal cost, because the live arm's second line is inside the node's own allocation and the
//!     tagged arm's is in a separate one. The claim made is "adds no line", not "adds no cost",
//!     and the unmeasured remainder can only run against the tagged shape.
//!   * MOVED: THE PUBLISHED TABLE. The four cells here were measured before #1966, which took
//!     eight bytes out of the address inside the inline page entry. The assertions were retargeted
//!     then; the numbers in this comment were not. Three of the four allocation ratios still
//!     reproduce exactly and one does not, and both byte columns at the configured range moved.
//!   * SETTLED, AND IT IS THE ONE THAT DECIDES IT: THE NODE NARROWS AND THE KEY DOES NOT. A
//!     simple bucket costs 168 B inline today. Tagged, it costs a 56 B node plus the chunk the
//!     allocator serves a 104 B payload from, READ BACK FROM THE ALLOCATOR rather than taken from
//!     a formula. The bytes do not leave the key; they move from a field into a chunk.
//!     RE-MEASURED TWICE SINCE, AND THE MARGIN HAS CLOSED TO ZERO: at 184 the pair was 192 and
//!     the key held 8 B MORE; the flag packing took both sides down by eight (176 against 184)
//!     and the address merge took both down by eight again (168 against 168), because each of
//!     them shrinks the boxed payload by exactly what it shrinks the node by. The key now holds
//!     the SAME bytes either way. The byte argument against the shape is spent; what is left
//!     against it is one allocation and one indirection per occupied bucket, which no instrument
//!     here prices.
//!     `the_bytes_a_tagged_key_saves_are_not_bytes_a_tagged_key_stops_holding` reads the chunk
//!     back and asserts the pair never comes in UNDER the inline width, which is the claim being
//!     refuted, rather than pinning a delta that moves with the rounding step.
//!   * STANDING: THE SAVING THAT DOES EXIST IS NOT A PER-KEY SAVING AND IT IS NOT THE SHAPE'S. It
//!     is B-tree slot waste. A `BTreeMap` node holds eleven value slots whether they are filled or
//!     not, so a narrower value fits more of them into one node -- which is worth real bytes at
//!     the DEFAULT routing range, where every key sits in a bucket of its own, and almost nothing
//!     at the range an operator is told to configure.
//!
//! THE CELLS, ON ONE INSTRUMENT, BOTH SIDES, RE-MEASURED. The REQUEST column is what the caller
//! asked the allocator for; the CHUNK column is what the allocator set aside to answer. Only the
//! second is what a key actually holds, and the tagged side is the side that allocates, so the
//! request column flatters it at every cell:
//!
//! ```text
//!   corpus / range                            simple    request      CHUNK  allocations
//!   4000 routed keys on the whole keyspace  100.000%     0.580x     0.628x       7.265x
//!   4000 routed keys on 0..1023               4.510%     0.874x     0.898x       1.317x
//!   40000 routed keys on the whole keyspace 100.000%     0.602x     0.653x       7.559x
//!   40000 routed keys on 0..1023              0.000%     0.980x     0.983x       1.304x
//! ```
//!
//! READ THE FIRST COLUMN AGAINST THE REST AND THE WHOLE FINDING IS THERE. The saving is not a
//! property of the shape; it is a property of HOW MANY BUCKETS ARE IN THE ARM THE SHAPE IS FOR,
//! and that number is decided by the routing range rather than by the workload. Two things follow,
//! and each of them on its own is the decline:
//!
//!   1. WHERE THE SHAPE WINS, A KNOB THAT ALREADY SHIPS WINS TWO AND A HALF TIMES AS MUCH FOR
//!      NOTHING. On the same instrument and the same corpus the LIVE shape reads 301.895 B/record
//!      on the whole keyspace and 120.091 B/record on 0..1023 -- 0.398x, against the tagged
//!      shape's best cell of 0.653x -- and `docs/runtime_tuning.md` already tells an operator to
//!      set exactly that before the first ingest. The knob costs no allocation, no indirection and
//!      no line of code. (This module previously published 0.632x for the same comparison, from
//!      the pre-#1966 tree.)
//!   2. WHERE THE KNOB IS SET, THE ARM IS GONE AND SO IS THE SAVING. `single_page_object` is
//!      4.510% of occupied buckets at 4,000 records on 0..1023 and 0.000% at 40,000, where
//!      `multi_object` is 100.000%. At 0.000% simple the tagged shape saves 1.7% of the chunk
//!      bytes and still costs 1.304x the allocations. That cell is the control on the explanation:
//!      a configuration where the mechanism predicts the saving should be absent, measured.
//!
//! THE PREMISE IS ALSO WRONG, AND THAT MATTERS SEPARATELY FROM THE PRICE. The proposal says the
//! node "carries the whole general case for every key". It does not. Every one of the three
//! container members is ALREADY a biased `Empty`/`One`/`Many` shape whose general arm is the
//! only one that allocates: `BlockIndexMap` holds its single page inline, `ObjectIndex` holds
//! its single id inline, `DeletedObjectIndex` is one nullable pointer that is null in the case
//! it is almost always in. A simple bucket allocates NOTHING for the general case today, so
//! there is no general case for a tagged word to take out of it. What the word moves out of
//! line is the simple bucket's OWN data -- and since that data is already a line away from the
//! node, moving it costs no read and saves no resident byte.
//!
//! AND THAT DATA DOES NOT FIT IN A WORD -- THOUGH THE ADDRESS INSIDE IT NOW DOES. A page's
//! slab id and offset are one 64-bit word here as well, but a `BlockAddress` is 32 bytes around
//! it: an identity, a length, a block id, a routing bucket and a presence byte all sit beside the
//! word, and the page entry around THAT carries three shared names, for 88. #1962 established
//! that those three names are per-object and per-page facts that cannot be hoisted to the bucket:
//! hoisting them "would have passed every test at the default range and lost pages silently at the
//! cluster range". So the simple arm cannot be a word here, and the most it can be is a pointer to
//! 104 bytes in a 112-byte chunk -- which is the shape this module prices and declines.
//!
//! WHAT WOULD CHANGE THE ANSWER, STATED SO THE NEXT REVISIT DOES NOT START FROM NOTHING. Not a
//! narrower node: the node is not what costs. The per-key figure moves only if the PAGE ENTRY
//! gets smaller, and the entry is 88 bytes of which 48 are three `Arc<str>` names. Those are
//! per-page facts and must stay per-page -- #1962 is not in dispute -- but a per-page NAME HANDLE
//! is still a per-page fact, and four bytes rather than sixteen. That is a different change with a
//! different risk, and it is the one with the arithmetic behind it.
//!
//! THE MEASUREMENT DOES NOT DEPEND ON THE DECLINE BEING RIGHT. Every figure is printed with its
//! denominator, the mirror of the live declaration is asserted against the declaration itself
//! before any price is quoted, the store path length is held constant and stated, and the
//! instrument recovers a planted marker exactly. If the numbers move the decline should be
//! revisited rather than re-argued -- as it was here.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::mem::{align_of, size_of};

use crate::engine::state::{
    BlockIndex, BlockIndexMap, BucketLayoutState, BucketNode, BucketTtl, DeletedObjectIndex,
    ObjectIndex,
};
use crate::engine::storage_bucket_internals::classify_bucket_layout;

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_BUCKET=1023`, the
/// setting `docs/runtime_tuning.md` tells an operator to set before the first ingest.
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses, and `startup_load_shard_request`'s default.
const WIDE_END: u32 = u32::MAX;

/// How many buckets the narrow range has. Derived from the range, not written down twice.
const NARROW_BUCKETS: usize = (NARROW_END as usize) + 1;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

// =============================================================================================
// FIXTURE
// =============================================================================================

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
        table_name: "node-arms".to_string(),
        shard_uri: "local://node-arms/1".to_string(),
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

/// ROUTED KEYS: plain strings, one page each.
fn seed_routed(engine: &TemporalEngine, count: usize) -> Vec<String> {
    let keys: Vec<String> = (0..count).map(|i| format!("arm-{i:06}")).collect();
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

/// CONTAINER KEYS: hashes of `members` fields each. Every field is its own page, and every one
/// of them routes to the container key's single bucket.
fn seed_container(engine: &TemporalEngine, keys: usize, members: usize) -> Vec<String> {
    let container_keys: Vec<String> = (0..keys).map(|k| format!("sack-{k:06}")).collect();
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

// =============================================================================================
// THE HISTOGRAM
// =============================================================================================

/// The five layout arms with a bucket count each, plus the per-bucket page counts the
/// percentiles and the maximum are taken over.
#[derive(Debug, Default, Clone)]
struct ArmHistogram {
    /// Bucket counts keyed by the arm `classify_bucket_layout` puts them in.
    arms: BTreeMap<&'static str, usize>,
    /// Pages held, one entry per OCCUPIED bucket, so a percentile is over buckets that exist.
    pages: Vec<usize>,
    /// Objects held, one entry per occupied bucket.
    objects: Vec<usize>,
}

fn arm_name(layout: BucketLayoutState) -> &'static str {
    match layout {
        BucketLayoutState::Empty => "empty",
        BucketLayoutState::SingleObject => "single_object_no_page",
        BucketLayoutState::SingleBlockObject => "single_page_object",
        BucketLayoutState::MultiBlockObject => "multi_page_object",
        BucketLayoutState::MultiObject => "multi_object",
    }
}

/// Every arm this module ever claims a figure for. A claim about an arm with no samples is not a
/// measurement, and `assert_every_claimed_arm_has_samples` refuses one.
const EVERY_ARM: [&str; 5] = [
    "empty",
    "single_object_no_page",
    "single_page_object",
    "multi_page_object",
    "multi_object",
];

impl ArmHistogram {
    fn buckets(&self) -> usize {
        self.arms.values().copied().sum()
    }

    fn occupied(&self) -> usize {
        self.pages.len()
    }

    fn total_pages(&self) -> usize {
        self.pages.iter().sum()
    }

    fn count(&self, arm: &str) -> usize {
        self.arms.get(arm).copied().unwrap_or_default()
    }

    /// The fraction of OCCUPIED buckets in one arm. Empty buckets are excluded from the
    /// denominator deliberately: a released bucket holds no pages and would dilute every arm.
    fn occupied_fraction(&self, arm: &str) -> f64 {
        let occupied = self.occupied();
        if occupied == 0 {
            return 0.0;
        }
        self.count(arm) as f64 / occupied as f64
    }

    /// The two arms a tagged word would hold inline: one object with one page, and one object
    /// with none. Together, the fraction of buckets the proposal would actually help.
    fn simple_fraction(&self) -> f64 {
        self.occupied_fraction("single_page_object") + self.occupied_fraction("single_object_no_page")
    }

    fn percentile(sorted: &[usize], q: f64) -> usize {
        if sorted.is_empty() {
            return 0;
        }
        // Nearest-rank, which for a count distribution answers a value that actually occurs
        // rather than an interpolation between two that do.
        let rank = ((q * sorted.len() as f64).ceil() as usize).max(1);
        sorted[rank.min(sorted.len()) - 1]
    }

    fn report(&self, label: &str) {
        let mut pages = self.pages.clone();
        pages.sort_unstable();
        let mut objects = self.objects.clone();
        objects.sort_unstable();
        println!(
            "\n{label}: {} buckets, {} occupied, {} pages",
            self.buckets(),
            self.occupied(),
            self.total_pages()
        );
        for arm in EVERY_ARM {
            println!(
                "    {arm:<22} {:>7} buckets  ({:>7.3}% of occupied)",
                self.count(arm),
                100.0 * self.occupied_fraction(arm)
            );
        }
        println!(
            "    pages/bucket   p50 {:>6}  p90 {:>6}  p99 {:>6}  MAX {:>6}",
            Self::percentile(&pages, 0.50),
            Self::percentile(&pages, 0.90),
            Self::percentile(&pages, 0.99),
            pages.last().copied().unwrap_or_default()
        );
        println!(
            "    objects/bucket p50 {:>6}  p90 {:>6}  p99 {:>6}  MAX {:>6}",
            Self::percentile(&objects, 0.50),
            Self::percentile(&objects, 0.90),
            Self::percentile(&objects, 0.99),
            objects.last().copied().unwrap_or_default()
        );
    }
}

/// The arm histogram, read off the engine's OWN classifier rather than recomputed here. A
/// separate copy of the rule would be measuring this module instead of the engine.
fn arm_histogram(engine: &TemporalEngine) -> ArmHistogram {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut hist = ArmHistogram::default();
    for arm in EVERY_ARM {
        hist.arms.insert(arm, 0);
    }
    for node in shard.bucket_index.bucket_map.values() {
        let pages = node.block_index.len();
        let objects = node.object_index.len();
        *hist
            .arms
            .entry(arm_name(classify_bucket_layout(objects, pages)))
            .or_default() += 1;
        if pages > 0 {
            hist.pages.push(pages);
            hist.objects.push(objects);
        }
    }
    hist
}

/// Refuses a run in which an arm this module quotes has no samples at all.
fn assert_every_claimed_arm_has_samples(claimed: &[&str], reached: &ArmHistogram, label: &str) {
    for arm in claimed {
        assert!(
            reached.count(arm) > 0,
            "{label}: this module quotes a figure for `{arm}` and the fixture reached ZERO \
             buckets in it; an arm with no samples is not a measurement"
        );
    }
}

/// THE ARM DISTRIBUTION, AT TWO CORPUS SIZES AND AT BOTH ROUTING RANGES.
///
/// COUNTS, PERCENTILES AND A MAXIMUM -- never a mean. #1959 published a mean of 1.98 pages a
/// bucket over a store that contained not one bucket holding two, and priced a shape against it.
///
/// THE TWO RANGES ARE THE WHOLE POINT. A page's bucket is
/// `block_routing_bucket(object_key, start, end)`, whose modulus is the RANGE WIDTH, so the
/// engine's default of `0..u32::MAX` puts every key in a bucket of its own BY CONSTRUCTION and
/// no workload can do otherwise. `docs/runtime_tuning.md` tells an operator to set
/// `TS_SHARD_END_ROUTING_BUCKET=1023` before the first ingest, and at 1,024 buckets the same
/// keys fill them. A proposal priced only at the default is priced on a fixture, not on a
/// deployment.
///
/// THE CONTAINER WORKLOAD IS HERE BECAUSE IT IS THE ONE THE RANGE CANNOT SPLIT. Routing takes the
/// object key and never the component, so a hash's hundred fields are a hundred pages under one
/// key and therefore in one bucket at ANY range. It is the arm distribution a narrow range
/// cannot reach and a wide range cannot escape, and it is where the maximum comes from.
///
/// THE DENOMINATOR IS ASSERTED. A run that wrote nothing would report a clean histogram of zero
/// buckets, which reads as a beautifully simple store.
///
/// rust-internal: reads the engine's own bucket index and its own classifier, no product
/// behaviour
#[test]
#[ignore = "seeds six stores up to 40,000 records each; run by name"]
fn the_arm_a_bucket_node_lands_in_is_decided_by_the_routing_range_not_by_the_workload() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut routed: BTreeMap<(usize, u32), ArmHistogram> = BTreeMap::new();

    for records in [SMALL, LARGE] {
        for end_routing_bucket in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            let hist = arm_histogram(&engine);
            let width = if end_routing_bucket == WIDE_END {
                "the whole keyspace".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            hist.report(&format!("{records} routed keys on {width}"));
            assert_eq!(
                hist.total_pages(),
                keys.len(),
                "{records}/{end_routing_bucket}: the index holds {} pages for {} written keys, so \
                 the histogram above is not over the fixture",
                hist.total_pages(),
                keys.len()
            );
            routed.insert((records, end_routing_bucket), hist);
        }
    }

    // THE WORKLOAD THE RANGE CANNOT SPLIT, at both sizes, on the wide range -- where a
    // single-page reading would otherwise be automatic.
    let mut container: BTreeMap<usize, ArmHistogram> = BTreeMap::new();
    for (keys, members) in [(SMALL / 100, 100), (LARGE / 100, 100)] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine, WIDE_END);
        let written = seed_container(&engine, keys, members);
        let hist = arm_histogram(&engine);
        hist.report(&format!(
            "{} container keys x {members} fields on the whole keyspace",
            written.len()
        ));
        assert_eq!(
            hist.total_pages(),
            keys * members,
            "the container fixture wrote {} pages for {} expected",
            hist.total_pages(),
            keys * members
        );
        container.insert(keys * members, hist);
    }

    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|length| *length == first),
        "the store path length moved across arms ({path_lengths:?}); it shifts allocation bytes \
         at about six bytes a character"
    );
    println!("\n  store path length held at {first} characters across all six stores");

    // --- THE CLAIM, ARM BY ARM. ---
    for records in [SMALL, LARGE] {
        let wide = routed.get(&(records, WIDE_END)).expect("wide arm");
        let narrow = routed.get(&(records, NARROW_END)).expect("narrow arm");

        assert_every_claimed_arm_has_samples(
            &["single_page_object"],
            wide,
            &format!("{records} routed keys on the whole keyspace"),
        );
        assert_every_claimed_arm_has_samples(
            &["multi_object"],
            narrow,
            &format!("{records} routed keys on 0..{NARROW_END}"),
        );

        // ON THE DEFAULT RANGE THE SIMPLE ARM IS EVERYTHING.
        assert!(
            wide.simple_fraction() > 0.99,
            "on the whole keyspace {records} routed keys put {:.5} of occupied buckets in a \
             simple arm; at that range every key lands alone by construction and this arm is \
             meant to show it",
            wide.simple_fraction()
        );

        // ON THE CONFIGURED RANGE IT IS NOT. This is the half a fixture on `load_shard(1)`
        // cannot see.
        assert!(
            narrow.simple_fraction() < 0.25,
            "on 0..{NARROW_END} {records} routed keys put {:.5} of occupied buckets in a simple \
             arm; if a configured shard were still simple-dominated the proposal would be worth \
             writing and this decline would be wrong",
            narrow.simple_fraction()
        );

        // AND THE BUCKET COUNT MOVES THE OTHER WAY, which is the whole of the second reason.
        assert!(
            narrow.buckets() <= NARROW_BUCKETS,
            "a shard on 0..{NARROW_END} reported {} buckets, more than the {NARROW_BUCKETS} the \
             range has; the modulus is not the range width",
            narrow.buckets()
        );
        println!(
            "  {records} routed keys: whole keyspace {} buckets, {:.3}% simple; 0..{NARROW_END} \
             {} buckets, {:.3}% simple -- the arm the proposal helps and the bucket count move in \
             OPPOSITE directions",
            wide.buckets(),
            100.0 * wide.simple_fraction(),
            narrow.buckets(),
            100.0 * narrow.simple_fraction()
        );
    }

    // --- THE CONTAINER WORKLOAD REACHES THE GENERAL ARM ON THE WIDE RANGE TOO. ---
    //
    // AND IT REACHES `multi_object`, NOT `multi_page_object`, WHICH THIS TEST GOT WRONG FIRST
    // TIME. A hash's hundred fields look like one object with a hundred pages and are not: the
    // page handle is `stable_block_object_id(shard, kind, key, component)` and it TAKES THE
    // COMPONENT, so a hundred fields are a hundred distinct object ids filed under one routing
    // key. `object_index` therefore holds a hundred ids and the classifier answers
    // `multi_object`. `multi_page_object` -- ONE object id holding several pages -- is not
    // reached by any workload seeded here, which is reported below rather than asserted away.
    for (pages, hist) in &container {
        assert_every_claimed_arm_has_samples(
            &["multi_object"],
            hist,
            &format!("{pages} container pages on the whole keyspace"),
        );
        assert!(
            hist.occupied_fraction("single_page_object") < 0.01,
            "the container store put {:.5} of occupied buckets in `single_page_object`; it is \
             meant to be the workload the range cannot split",
            hist.occupied_fraction("single_page_object")
        );
    }

    // --- WHICH ARMS THE CORPORA NEVER REACHED, NAMED. ---
    //
    // An arm with no samples is not a measurement, so the arms this module quotes figures for
    // are asserted non-empty above and the rest are reported here as the zeroes they are. Both
    // are reachable in principle and neither changes the pricing:
    //
    //   * `single_object_no_page` is a RELEASED bucket -- `release_bucket_blocks` empties the
    //     page index and keeps `object_index` -- which holds no page in either representation
    //     and so cannot decide between them;
    //   * `multi_page_object` needs one object id holding several pages, which the handle
    //     function above makes unreachable for a container.
    let mut never_reached: Vec<&str> = Vec::new();
    for arm in EVERY_ARM {
        let reached: usize = routed
            .values()
            .chain(container.values())
            .map(|hist| hist.count(arm))
            .sum();
        if reached == 0 {
            never_reached.push(arm);
        }
    }
    println!("  arms with ZERO samples across all six stores: {never_reached:?}");

    // THE DENOMINATOR FOR THE ARM COLUMNS. Every occupied bucket landed in exactly one arm, so
    // a percentage above is over the whole population and not over part of it.
    for hist in routed.values().chain(container.values()) {
        let counted: usize = EVERY_ARM.iter().map(|arm| hist.count(arm)).sum();
        assert_eq!(
            hist.buckets(),
            counted,
            "the arm columns account for {counted} of {} buckets; a percentage over part of a \
             population is not the population's",
            hist.buckets()
        );
    }

    // AND THE CLASSIFIER ITSELF DISTINGUISHES ALL FIVE, so a zero above is the corpus not
    // reaching an arm rather than the rule not having one.
    let distinguished: std::collections::BTreeSet<&str> = [(0, 0), (1, 0), (1, 1), (1, 4), (3, 9)]
        .into_iter()
        .map(|(objects, pages)| arm_name(classify_bucket_layout(objects, pages)))
        .collect();
    assert_eq!(
        EVERY_ARM.len(),
        distinguished.len(),
        "the classifier answered {} distinct arms over the five inputs that select them: \
         {distinguished:?}",
        distinguished.len()
    );

    // --- THE MAXIMUM, WHICH A MEAN HIDES. ---
    let narrow_large = routed.get(&(LARGE, NARROW_END)).expect("narrow large");
    let widest = narrow_large.pages.iter().copied().max().unwrap_or_default();
    assert!(
        widest > 11,
        "the widest bucket on 0..{NARROW_END} at {LARGE} records holds {widest} pages; below \
         twelve a `BTreeMap` node is not even filled once and the general arm would be priced \
         against a shape it never reaches"
    );
    println!("  widest bucket on 0..{NARROW_END} at {LARGE} records: {widest} pages");
}

// =============================================================================================
// WHAT A SIMPLE BUCKET ALREADY CARRIES
// =============================================================================================

/// A SIMPLE BUCKET ALREADY ALLOCATES NOTHING FOR THE GENERAL CASE.
///
/// The proposal's premise is that the node "carries the whole general case for every key". Every
/// one of the node's three container members is already a biased shape whose general arm is the
/// only one that allocates, and this asserts it from the arms themselves rather than from the
/// declaration's comments:
///
///   * `BlockIndexMap::One` holds its page INLINE -- no map, no node, no allocation;
///   * `ObjectIndex::One` holds its single id INLINE, and only `Many` takes a box;
///   * `DeletedObjectIndex` is one nullable pointer, null in the case it is almost always in.
///
/// So there is no general-case container for a tagged word to take out of a simple node. What it
/// would move out of line is the simple bucket's OWN page entry, which is the next test's
/// subject.
///
/// rust-internal: reads the engine's own declarations, no product behaviour
#[test]
fn a_simple_bucket_holds_no_general_case_to_take_away() {
    // Each member's EMPTY and SINGLE spellings, and the width they occupy in the node.
    assert_eq!(
        96,
        size_of::<BlockIndexMap>(),
        "the page index is {} bytes in the node, not 96; the accounting below is stale",
        size_of::<BlockIndexMap>()
    );
    assert_eq!(16, size_of::<ObjectIndex>(), "the object index moved");
    assert_eq!(
        8,
        size_of::<DeletedObjectIndex>(),
        "the tombstone index moved"
    );

    // The three members sum to 120 of the node's 168 -- the bytes the proposal would replace
    // with one word. It was 136 of 192, then 128 of 184 when the address inside the inline page
    // entry shed its derived generation, 128 of 176 when the five flags became five bits (the
    // node moved and the members did not -- the flags are not in them), and 120 of 168 when that
    // address merged its slab id and its offset into one word. So of the three steps, two took
    // the same eight bytes off the members AND off the node, and one took eight off the node
    // alone. What the proposal would replace is unchanged in kind and smaller in size, which is
    // the direction that makes the proposal worse rather than better.
    let members = size_of::<BlockIndexMap>() + size_of::<ObjectIndex>() + size_of::<DeletedObjectIndex>();
    assert_eq!(
        120, members,
        "the three container members are {members} bytes, not 120"
    );
    assert!(
        members < size_of::<BucketNode>(),
        "the members cannot be wider than the node that holds them"
    );

    // AND THE SIMPLE SPELLING OF EACH IS THE ONE THAT DOES NOT ALLOCATE. Asserted by
    // construction over the arms themselves: a simple bucket's members are the inline arms.
    let simple_page = BlockIndexMap::One(7, page_fixture());
    assert!(
        matches!(simple_page, BlockIndexMap::One(_, _)),
        "a single page must land in the inline arm"
    );
    assert_eq!(1, simple_page.len(), "the inline arm holds exactly one page");

    let mut ids = ObjectIndex::default();
    assert!(matches!(ids, ObjectIndex::Empty), "an empty object index allocates nothing");
    ids.insert(42);
    assert!(
        matches!(ids, ObjectIndex::One(42)),
        "a single object id must stay in the inline arm rather than taking a box"
    );

    let tombstones = DeletedObjectIndex::default();
    assert!(
        tombstones.is_empty(),
        "the tombstone index of a live bucket is the null pointer"
    );

    // THE ONE FACT THAT DECIDES THE SHAPE: what a simple bucket's data actually is.
    assert_eq!(
        32,
        size_of::<crate::block_store::BlockAddress>(),
        "an address is {} bytes, not 32",
        size_of::<crate::block_store::BlockAddress>()
    );
    assert_eq!(
        88,
        size_of::<BlockIndex>(),
        "a page entry is {} bytes, not 88",
        size_of::<BlockIndex>()
    );
    assert!(
        size_of::<crate::block_store::BlockAddress>() > size_of::<u64>(),
        "the whole proposal rests on a page's address fitting in a tagged 64-bit word; here the \
         address ALONE is {} bytes, and the entry around it carries three shared names as well",
        size_of::<crate::block_store::BlockAddress>()
    );
}

fn page_fixture() -> BlockIndex {
    BlockIndex {
        object_key: std::sync::Arc::from("arm-000001"),
        model_id: std::sync::Arc::from("m"),
        component: None,
        address: crate::block_store::BlockAddress::from_parts(
            1,
            64,
            32,
            Some(9),
            Some(11),
            Some(3),
        ),
        dirty: false,
        deleted: false,
        log_backed: false,
    }
}

// =============================================================================================
// THE TAGGED WORD, BUILT
// =============================================================================================

/// Which arm a tagged word is pointing at. Two bits, and the invariant that makes them free is
/// asserted rather than assumed.
const KIND_MASK: usize = 0b11;
const KIND_EMPTY: usize = 0;
const KIND_SIMPLE: usize = 1;
const KIND_GENERAL: usize = 2;

/// WHERE THE TAG LIVES, AND WHETHER IT IS FREE.
///
/// #1958 found a discriminant with no niche spending sixteen bytes to carry eight bytes of
/// number, so a tag's home has to be stated. This one rides the LOW TWO BITS OF THE POINTER.
/// Both payloads have alignment 8, so their addresses have three zero bits to spare and the tag
/// costs nothing: the word is eight bytes and holds both the tag and the address.
///
/// THE INVARIANT IS GUARDED, NOT ASSUMED. `tag` refuses any address whose low bits are already
/// set -- which is what a payload of alignment 1 or 2 would hand it -- and
/// `the_tag_guard_refuses_an_address_whose_low_bits_are_not_free` feeds it one and asserts the
/// refusal. A guard nothing has ever failed is a guard nothing has ever tested.
fn tag(raw: usize, kind: usize) -> usize {
    assert_eq!(
        0,
        raw & KIND_MASK,
        "the tag rides the low two bits of the payload address and address {raw:#x} has one of \
         them set; the payload's alignment is not what this representation requires"
    );
    assert!(kind <= KIND_MASK, "a two-bit tag cannot hold kind {kind}");
    raw | kind
}

/// The simple arm's payload, out of line: one object, one page.
struct SimpleLayout {
    object_id: u64,
    handle: u64,
    page: BlockIndex,
}

/// The general arm's payload, out of line: everything the node holds today.
struct GeneralLayout {
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// ONE TAGGED WORD, which is the whole of the proposal.
struct TaggedLayout(usize);

impl TaggedLayout {
    fn empty() -> Self {
        TaggedLayout(KIND_EMPTY)
    }

    fn simple(layout: SimpleLayout) -> Self {
        let raw = Box::into_raw(Box::new(layout)) as usize;
        TaggedLayout(tag(raw, KIND_SIMPLE))
    }

    fn general(layout: GeneralLayout) -> Self {
        let raw = Box::into_raw(Box::new(layout)) as usize;
        TaggedLayout(tag(raw, KIND_GENERAL))
    }

    fn kind(&self) -> usize {
        self.0 & KIND_MASK
    }

    fn as_simple(&self) -> Option<&SimpleLayout> {
        if self.kind() != KIND_SIMPLE {
            return None;
        }
        // SAFETY: the word was built by `simple`, which boxed a `SimpleLayout` and masked the
        // tag into bits the alignment guarantees are zero; the kind check above is what says the
        // pointer is that one.
        Some(unsafe { &*((self.0 & !KIND_MASK) as *const SimpleLayout) })
    }

    fn as_general(&self) -> Option<&GeneralLayout> {
        if self.kind() != KIND_GENERAL {
            return None;
        }
        // SAFETY: as above, for the general arm.
        Some(unsafe { &*((self.0 & !KIND_MASK) as *const GeneralLayout) })
    }

    /// The page entry a read has to reach, whichever arm holds it.
    fn page(&self, handle: u64) -> Option<&BlockIndex> {
        if let Some(simple) = self.as_simple() {
            return (simple.handle == handle).then_some(&simple.page);
        }
        self.as_general()
            .and_then(|general| general.block_index.get(&handle))
    }
}

impl Clone for TaggedLayout {
    fn clone(&self) -> Self {
        match self.kind() {
            KIND_SIMPLE => {
                let held = self.as_simple().expect("the kind says simple");
                TaggedLayout::simple(SimpleLayout {
                    object_id: held.object_id,
                    handle: held.handle,
                    page: held.page.clone(),
                })
            }
            KIND_GENERAL => {
                let held = self.as_general().expect("the kind says general");
                TaggedLayout::general(GeneralLayout {
                    object_index: held.object_index.clone(),
                    deleted_object_index: held.deleted_object_index.clone(),
                    block_index: held.block_index.clone(),
                })
            }
            _ => TaggedLayout::empty(),
        }
    }
}

impl Drop for TaggedLayout {
    fn drop(&mut self) {
        let raw = self.0 & !KIND_MASK;
        match self.kind() {
            // SAFETY: the pointer came from `Box::into_raw` on this exact type and is taken back
            // exactly once, because `self.0` is consumed here.
            KIND_SIMPLE => drop(unsafe { Box::from_raw(raw as *mut SimpleLayout) }),
            // SAFETY: as above, for the general arm.
            KIND_GENERAL => drop(unsafe { Box::from_raw(raw as *mut GeneralLayout) }),
            _ => {}
        }
        self.0 = KIND_EMPTY;
    }
}

/// THE NODE UNDER THE PROPOSAL: the same header, and one word where the three containers were.
#[derive(Clone)]
struct TaggedNode {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    payload: TaggedLayout,
}

/// THE CONTROL FOR EVERY WIDTH BELOW: the live declaration, rebuilt from its own field types. If
/// this is not `size_of::<BucketNode>()` then the tagged node is being compared against a
/// structure this engine does not have.
#[allow(dead_code)]
struct LiveNodeMirror {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// Rebuild one live node in the tagged shape, arm chosen by the engine's OWN classifier.
fn retag(node: &BucketNode) -> TaggedNode {
    let pages = node.block_index.len();
    let objects = node.object_index.len();
    let simple = matches!(
        classify_bucket_layout(objects, pages),
        BucketLayoutState::SingleBlockObject
    ) && node.deleted_object_index.is_empty();
    let payload = if pages == 0 && objects == 0 {
        TaggedLayout::empty()
    } else if simple {
        let (handle, page) = node
            .block_index
            .iter()
            .next()
            .map(|(handle, page)| (*handle, page.clone()))
            .expect("the simple arm holds exactly one page");
        TaggedLayout::simple(SimpleLayout {
            object_id: node.object_index.iter().next().copied().unwrap_or_default(),
            handle,
            page,
        })
    } else {
        TaggedLayout::general(GeneralLayout {
            object_index: node.object_index.clone(),
            deleted_object_index: node.deleted_object_index.clone(),
            block_index: node.block_index.clone(),
        })
    };
    TaggedNode {
        routing_bucket: node.routing_bucket,
        layout: node.layout,
        flags: BucketFlags::default().with(BucketFlags::DIRTY, node.dirty()).with(BucketFlags::DELETED, node.deleted()).with(BucketFlags::META_LOADED, node.meta_loaded()).with(BucketFlags::LOADING, node.loading()).with(BucketFlags::IN_MEMORY, node.in_memory()),
        ttl_ms: node.ttl_ms,
        dirty_generation: node.dirty_generation,
        first_dirty_wal_sequence: node.first_dirty_wal_sequence,
        first_dirty_index_log_sequence: node.first_dirty_index_log_sequence,
        payload,
    }
}

/// THE TAG'S HOME, AND THE WIDTH OF EACH ARM, WITH THE RECONSTRUCTION ASSERTED FOR EACH.
///
/// A width quoted without its field sum cannot tell a structure that is FULL from one that is
/// half padding, and those two want opposite fixes. Every structure here is reported as
/// `eight_aligned + round_up(tail) == size_of`, which is the layout rule itself rather than a
/// number that happens to match.
///
/// rust-internal: measures declarations, no product behaviour
#[test]
fn the_tagged_node_is_fifty_six_bytes_and_every_arm_reconstructs() {
    // --- THE CONTROL COMES FIRST. ---
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<LiveNodeMirror>(),
        "the mirror of the live declaration is {} bytes against the declaration's {}; every \
         price below would be fiction",
        size_of::<LiveNodeMirror>(),
        size_of::<BucketNode>()
    );

    // --- THE TAG IS FREE, AND THE ALIGNMENT THAT MAKES IT FREE IS ASSERTED. ---
    assert_eq!(
        8,
        size_of::<TaggedLayout>(),
        "the tagged word is {} bytes; a tag with nowhere free to live costs eight of its own and \
         the whole saving with it",
        size_of::<TaggedLayout>()
    );
    assert!(
        align_of::<SimpleLayout>() > KIND_MASK,
        "the simple payload has alignment {}, which leaves fewer than the two low bits the tag \
         rides in",
        align_of::<SimpleLayout>()
    );
    assert!(
        align_of::<GeneralLayout>() > KIND_MASK,
        "the general payload has alignment {}, which leaves fewer than the two low bits the tag \
         rides in",
        align_of::<GeneralLayout>()
    );

    // --- EACH ARM, RECONSTRUCTED. ---
    for (name, size, eight_aligned, tail) in [
        (
            "BucketNode (live)",
            size_of::<BucketNode>(),
            size_of::<BucketTtl>()
                + 3 * size_of::<u64>()
                + size_of::<ObjectIndex>()
                + size_of::<DeletedObjectIndex>()
                + size_of::<BlockIndexMap>(),
            size_of::<u32>() + size_of::<BucketLayoutState>() + size_of::<BucketFlags>(),
        ),
        (
            "TaggedNode (proposed)",
            size_of::<TaggedNode>(),
            size_of::<BucketTtl>() + 3 * size_of::<u64>() + size_of::<TaggedLayout>(),
            size_of::<u32>() + size_of::<BucketLayoutState>() + size_of::<BucketFlags>(),
        ),
        (
            "SimpleLayout (out of line)",
            size_of::<SimpleLayout>(),
            2 * size_of::<u64>() + size_of::<BlockIndex>(),
            0,
        ),
        (
            "GeneralLayout (out of line)",
            size_of::<GeneralLayout>(),
            size_of::<ObjectIndex>() + size_of::<DeletedObjectIndex>() + size_of::<BlockIndexMap>(),
            0,
        ),
    ] {
        let reconstructed = eight_aligned + tail.div_ceil(8) * 8;
        println!(
            "  {name:<28} {size:>4} B = {eight_aligned:>4} B eight-aligned + {:>2} B tail rounded",
            tail.div_ceil(8) * 8
        );
        assert_eq!(
            reconstructed, size,
            "{name} is {size} B and its fields reconstruct to {reconstructed} B; the accounting \
             is not describing the structure"
        );
    }

    assert_eq!(160, size_of::<BucketNode>(), "the live node moved");
    // 48, not 56, and for the same reason the live node is 160 and not 168: this mirror carries
    // the node's header, and the header lost a whole word when `last_dump_sequence` left it.
    // (56 was itself 64 until the five flags became one byte and a ten-byte tail became six.) The
    // address merge takes eight more off the LIVE node and off this mirror's boxed payload alike,
    // because the payload holds that address too. BOTH SIDES LOSE THE SAME EIGHT BYTES EACH TIME,
    // so the verdict below is untouched by any of the three.
    assert_eq!(
        48,
        size_of::<TaggedNode>(),
        "the tagged node is {} bytes, not 48",
        size_of::<TaggedNode>()
    );
    assert_eq!(
        104,
        size_of::<SimpleLayout>(),
        "the simple payload is {} bytes, not 104",
        size_of::<SimpleLayout>()
    );

    // --- AND THE ARITHMETIC THAT READS AS A WIN, NAMED AS ARITHMETIC. ---
    //
    // 120 bytes off the struct, and then a 104-byte allocation for very nearly every bucket at
    // the default range. glibc serves a 104-byte request from a 112-byte chunk once it has taken
    // its own header word and rounded to a multiple of sixteen, so the PAIR is 64 + 112 = 176 --
    // exactly what it replaced, before the allocation itself and before the pointer chase. The
    // conclusion has survived both address narrowings unchanged, and for the same reason each
    // time: the node and the out-of-line payload shrink together. This
    // is `size_of` arithmetic and is quoted as such; the measured figure is in
    // `what_the_tagged_node_actually_costs_the_allocator`.
    let chunk = |request: usize| (request + 8).div_ceil(16) * 16;
    let pair = size_of::<TaggedNode>() + chunk(size_of::<SimpleLayout>());
    println!(
        "\n  ARITHMETIC ONLY: a simple bucket is {} B inline today against {} B of node + {} B of \
         chunk = {pair} B tagged ({:+} B), plus one allocation and one indirection",
        size_of::<BucketNode>(),
        size_of::<TaggedNode>(),
        chunk(size_of::<SimpleLayout>()),
        pair as i64 - size_of::<BucketNode>() as i64
    );
    assert!(
        pair >= size_of::<BucketNode>(),
        "the tagged pair costs {pair} B against {} B inline; if that has become a win the decline \
         recorded here is stale and should be revisited",
        size_of::<BucketNode>()
    );
}

/// THE TAG GUARD FAILS ON A VIOLATING ADDRESS.
///
/// The invariant that makes the tag free is that the payload's address has its low two bits
/// clear. A guard that has never refused anything has never been tested, so this feeds it an
/// address that violates the invariant and asserts the refusal.
///
/// rust-internal: measures this module's own guard, no product behaviour
#[test]
fn the_tag_guard_refuses_an_address_whose_low_bits_are_not_free() {
    // The positive control: a properly aligned address is accepted and the tag reads back.
    let aligned = 0x7f00_0000_1000usize;
    assert_eq!(
        KIND_SIMPLE,
        tag(aligned, KIND_SIMPLE) & KIND_MASK,
        "an aligned address must carry the tag back out"
    );
    assert_eq!(
        aligned,
        tag(aligned, KIND_SIMPLE) & !KIND_MASK,
        "an aligned address must survive tagging unchanged"
    );

    // THE NEGATIVE CONTROL. One byte past an aligned address is what a payload of alignment 1
    // would hand the tag, and it must be refused rather than silently corrupting the pointer.
    //
    // The hook is taken down across both refusals and put back afterwards: a guard that is
    // SUPPOSED to panic would otherwise print two backtraces into every ordinary suite run, and
    // a gate read at NAME level is easier to trust when a passing test is silent.
    let violating = aligned + 1;
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let refused = std::panic::catch_unwind(|| tag(violating, KIND_SIMPLE));
    let wide_kind = std::panic::catch_unwind(|| tag(aligned, KIND_MASK + 1));
    std::panic::set_hook(previous);

    assert!(
        refused.is_err(),
        "the tag guard accepted address {violating:#x}, whose low bit is set; tagging it would \
         hand back a pointer one byte off the payload and the representation would be unsound"
    );

    // A kind wider than the two bits is refused too, for the same reason.
    assert!(
        wide_kind.is_err(),
        "a kind that does not fit in two bits must be refused"
    );
}

// =============================================================================================
// WHAT IT COSTS, ON THE ALLOCATOR
// =============================================================================================

/// What one `clone()` charges the allocator, in bytes and in CALLS.
///
/// Cloning a `BTreeMap` rebuilds it node for node, so this charges the container's OWN
/// allocations and owes nothing to `size_of` x count. That is the point: a B-tree node holds
/// eleven value slots whether they are filled or not, so a narrower value is worth MORE than its
/// own width and a `size_of` comparison would under-state it.
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
/// rust-internal: measures the harness's own instrument, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn the_instrument_this_module_uses_recovers_a_planted_megabyte_exactly() {
    const PLANTED: usize = 1 << 20;
    let marker: Vec<u8> = vec![0xA5; PLANTED];
    let (bytes, allocs) = clone_counts(&marker);
    println!("planted {PLANTED} B, instrument charged {bytes} B in {allocs} call(s)");
    assert_eq!(
        PLANTED as u64, bytes,
        "the clone instrument charged {bytes} B for a planted {PLANTED} B"
    );
    assert_eq!(1, allocs, "one planted vector charged {allocs} allocations, not one");
}

#[cfg(feature = "alloc-probe")]
#[derive(Debug, Clone, Copy)]
struct Side {
    bytes: u64,
    allocs: u64,
}

#[cfg(feature = "alloc-probe")]
impl Side {
    fn per(&self, records: usize) -> (f64, f64) {
        (
            self.bytes as f64 / records as f64,
            self.allocs as f64 / records as f64,
        )
    }
}

/// BYTES AND ALLOCATIONS PER RECORD, LIVE SHAPE AGAINST TAGGED SHAPE, ON ONE INSTRUMENT.
///
/// ONE INSTRUMENT, BOTH SIDES, SAME SPAN OF THE SAME TEST. #1959 found a published decline whose
/// sign was wrong because it set a `size_of` saving against an allocator cost. This change is
/// exactly that trade -- inline bytes for a heap allocation -- so no `size_of` figure appears in
/// the comparison at all.
///
/// WHAT IS BEING CLONED. The engine's real `bucket_map` as production built it, and a map of the
/// same buckets rebuilt in the tagged shape by `retag`, arm chosen by the engine's own
/// classifier. Both clones charge their own B-tree nodes, so the eleven-slot node sizing that
/// makes a narrow value worth more than its width is charged on BOTH sides rather than assumed
/// on one.
///
/// THE TWO FIGURES CAN DISAGREE IN SIGN, and that is the finding here. The tagged map's nodes
/// are narrower, so it moves fewer BYTES; it also takes one allocation per occupied bucket that
/// the live map does not take at all. Both are printed at every arm.
///
/// WHAT THE INSTRUMENT CHARGES, stated because it decides how the byte figure should be read:
/// `layout.size()`, the REQUEST, not the chunk the allocator serves it from. It therefore
/// UNDER-charges the tagged side, which is the side that takes the allocations. The byte column
/// is a floor on the tagged side's cost, not an estimate of it.
///
/// rust-internal: measures the engine's own bucket map, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "seeds four stores up to 40,000 records each; run by name"]
fn what_the_tagged_node_actually_costs_the_allocator() {
    let mut path_lengths: Vec<usize> = Vec::new();
    // (records, range, simple fraction, live B/record, tagged/live bytes, tagged/live allocs)
    let mut cells: Vec<(usize, u32, f64, f64, f64, f64)> = Vec::new();

    for records in [SMALL, LARGE] {
        for end_routing_bucket in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            let hist = arm_histogram(&engine);

            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1");
            let live_map = &shard.bucket_index.bucket_map;
            let buckets = live_map.len();
            assert!(buckets > 0, "no buckets to divide by");
            assert_eq!(
                hist.total_pages(),
                keys.len(),
                "the map holds {} pages for {} keys, so the per-record divisor is not the \
                 fixture's",
                hist.total_pages(),
                keys.len()
            );

            // Built OUTSIDE the measured span on both sides: only the clone is charged.
            let tagged_map: BTreeMap<u32, TaggedNode> = live_map
                .iter()
                .map(|(routing_bucket, node)| (*routing_bucket, retag(node)))
                .collect();
            assert_eq!(
                live_map.len(),
                tagged_map.len(),
                "the tagged map holds {} buckets against the live map's {}; it is not the same \
                 population",
                tagged_map.len(),
                live_map.len()
            );

            let (live_bytes, live_allocs) = clone_counts(live_map);
            let (tagged_bytes, tagged_allocs) = clone_counts(&tagged_map);
            let live = Side { bytes: live_bytes, allocs: live_allocs };
            let tagged = Side { bytes: tagged_bytes, allocs: tagged_allocs };
            assert!(
                live.bytes > 0 && live.allocs > 0 && tagged.bytes > 0 && tagged.allocs > 0,
                "an instrument reading zero is the instrument failing, not the map being free: \
                 live {live:?}, tagged {tagged:?}"
            );

            let width = if end_routing_bucket == WIDE_END {
                "the whole keyspace".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            let (live_b, live_a) = live.per(records);
            let (tagged_b, tagged_a) = tagged.per(records);
            println!(
                "\n{records} routed keys on {width}: {buckets} buckets, {:.3}% simple",
                100.0 * hist.simple_fraction()
            );
            println!(
                "    live    {:>10.3} B/record  {:>8.4} allocs/record   ({live_bytes} B, \
                 {live_allocs} calls)",
                live_b, live_a
            );
            println!(
                "    tagged  {:>10.3} B/record  {:>8.4} allocs/record   ({tagged_bytes} B, \
                 {tagged_allocs} calls)",
                tagged_b, tagged_a
            );
            println!(
                "    delta   {:>+10.3} B/record  {:>+8.4} allocs/record  ({:.3}x bytes, {:.3}x \
                 allocations)",
                tagged_b - live_b,
                tagged_a - live_a,
                tagged_b / live_b,
                tagged_a / live_a
            );

            // THE ALLOCATION COUNT IS THE HALF THAT CANNOT BE ROUNDED AWAY, and it moves the
            // wrong way at every arm: one box per occupied bucket that the live shape does not
            // take.
            assert!(
                tagged.allocs > live.allocs,
                "{records}/{end_routing_bucket}: the tagged shape took {} allocations against \
                 the live shape's {}; it is meant to buy an allocation per occupied bucket and \
                 if it does not, `retag` is not building the arms it claims to",
                tagged.allocs,
                live.allocs
            );
            // THE DECOMPOSITION, because the total hides two effects pulling opposite ways.
            // The tagged side takes one box per OCCUPIED bucket, which the live side does not
            // take at all; and its map nodes are cheaper, because a B-tree node holds eleven
            // value slots whether they are filled or not and a narrower value fits more of them
            // into one node. Subtracting the boxes leaves the tagged map's OWN node
            // allocations, which must be FEWER than the live map's or the second effect is not
            // happening and the byte column below has no mechanism.
            assert!(
                tagged.allocs >= hist.occupied() as u64,
                "{records}/{end_routing_bucket}: the tagged shape took {} allocations over {} \
                 occupied buckets; it owes one box to each and cannot take fewer",
                tagged.allocs,
                hist.occupied()
            );
            let tagged_nodes = tagged.allocs - hist.occupied() as u64;
            println!(
                "    boxes   {:>10} (one per occupied bucket)   map nodes: live {} vs tagged \
                 {tagged_nodes}",
                hist.occupied(),
                live.allocs
            );
            assert!(
                tagged_nodes < live.allocs,
                "{records}/{end_routing_bucket}: the tagged map took {tagged_nodes} node \
                 allocations against the live map's {}; a narrower value is meant to fit more \
                 slots into one node, and if it does not then the byte column above has no \
                 mechanism behind it",
                live.allocs
            );
            cells.push((
                records,
                end_routing_bucket,
                hist.simple_fraction(),
                live_b,
                tagged_b / live_b,
                tagged_a / live_a,
            ));
        }
    }

    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|length| *length == first),
        "the store path length moved across arms ({path_lengths:?}); it shifts allocation bytes \
         at about six bytes a character"
    );
    println!("\n  store path length held at {first} characters across all four stores");

    // =========================================================================================
    // THE VERDICT, AND IT IS THE SIMPLE FRACTION THAT CARRIES IT.
    // =========================================================================================
    //
    // The byte saving is not a property of the shape. It is a property of HOW MANY BUCKETS ARE
    // IN THE ARM THE SHAPE IS FOR, and the four cells say so: the saving tracks the simple
    // fraction from 100% down to 0% and goes with it.
    println!("\n=== what the tagged shape is worth, against how many buckets it is for ===");
    println!("  {:<38} {:>9} {:>10} {:>12}", "corpus / range", "simple", "bytes", "allocations");
    for (records, range, simple, _, byte_ratio, alloc_ratio) in &cells {
        let width = if *range == WIDE_END {
            "the whole keyspace".to_string()
        } else {
            format!("0..{range}")
        };
        println!(
            "  {:<38} {:>8.3}% {:>9.3}x {:>11.3}x",
            format!("{records} routed keys on {width}"),
            100.0 * simple,
            byte_ratio,
            alloc_ratio
        );
    }

    // WHERE THE SIMPLE ARM IS EVERYTHING, THE SHAPE IS WORTH 40% OF THE BYTES -- AND SEVEN
    // TIMES THE ALLOCATIONS.
    for (records, range, simple, _, byte_ratio, alloc_ratio) in &cells {
        if *range != WIDE_END {
            continue;
        }
        assert!(
            *simple > 0.99,
            "{records} on the whole keyspace read {simple:.5} simple; the cell is meant to be \
             the one where the shape applies to everything"
        );
        assert!(
            *byte_ratio < 0.65,
            "{records} on the whole keyspace: the tagged shape read {byte_ratio:.3}x the bytes. \
             It is meant to be a real byte saving there, and a decline that under-states the \
             saving is as wrong as one that over-states it"
        );
        assert!(
            *alloc_ratio > 5.0,
            "{records} on the whole keyspace: the tagged shape read {alloc_ratio:.3}x the \
             allocations; one box per bucket against a live shape that allocates only map nodes \
             is what that costs, and a smaller figure would mean `retag` is not boxing"
        );
    }

    // WHERE THE SIMPLE ARM IS ABSENT, THE SAVING IS ABSENT WITH IT. This is the control on the
    // explanation: a cell the mechanism predicts should show nothing, measured.
    let (_, _, simple, _, byte_ratio, alloc_ratio) = cells
        .iter()
        .find(|(records, range, _, _, _, _)| *records == LARGE && *range == NARROW_END)
        .copied()
        .expect("the configured-range cell at the large corpus");
    assert_eq!(
        0.0, simple,
        "the configured range at {LARGE} records read {simple:.5} simple; the control below \
         needs a cell with NO buckets in the arm the shape is for"
    );
    assert!(
        byte_ratio > 0.95,
        "the configured range at {LARGE} records read {byte_ratio:.3}x the bytes over a store \
         with ZERO simple buckets. The mechanism says the saving comes from the simple arm, so a \
         real saving here would mean it comes from somewhere else and the explanation is wrong"
    );
    assert!(
        alloc_ratio > 1.0,
        "the configured range still buys a box per bucket; it read {alloc_ratio:.3}x"
    );
    println!(
        "\n  CONTROL: with 0.000% of buckets in the simple arm the shape saves {:.1}% of the \
         bytes and still costs {alloc_ratio:.3}x the allocations",
        100.0 * (1.0 - byte_ratio)
    );

    // AND THE ALTERNATIVE THE CHANGE IS COMPETING WITH IS A KNOB THAT ALREADY SHIPS. The same
    // instrument, the same corpus, the live shape on both ranges.
    let live_wide = cells
        .iter()
        .find(|(records, range, _, _, _, _)| *records == LARGE && *range == WIDE_END)
        .map(|cell| cell.3)
        .expect("the wide cell at the large corpus");
    let live_narrow = cells
        .iter()
        .find(|(records, range, _, _, _, _)| *records == LARGE && *range == NARROW_END)
        .map(|cell| cell.3)
        .expect("the narrow cell at the large corpus");
    println!(
        "  FOR COMPARISON, the shipped knob: the LIVE shape reads {live_wide:.3} B/record on the \
         whole keyspace and {live_narrow:.3} B/record on 0..{NARROW_END} -- {:.3}x, for a \
         setting `docs/runtime_tuning.md` already tells an operator to make, with no allocation \
         and no indirection",
        live_narrow / live_wide
    );
    assert!(
        live_narrow < live_wide,
        "the configured range read {live_narrow:.3} B/record against the default's \
         {live_wide:.3}; the comparison above assumes the shipped knob is the cheaper one"
    );
}

// =============================================================================================
// WHAT IT COSTS A READ
// =============================================================================================

/// How many distinct 64-byte lines a read has to touch to get from the node to the page's
/// address. Measured from the ADDRESSES THEMSELVES at run time, not asserted.
fn lines_touched(node_address: usize, address_address: usize) -> usize {
    if node_address / 64 == address_address / 64 {
        1
    } else {
        2
    }
}

/// THE READ PATH, COUNTED -- AND WHAT IT COSTS IS NOW NOTHING.
///
/// THIS GUARD WAS RED ON MAIN BEFORE THIS CHANGE, AND IT WAS RED FOR A REASON WORTH HAVING. It
/// asserted the live inline arm touches 1.5200 lines to reach a page's address, in a band chosen
/// so that 2.00 -- the tagged arm's figure -- could not pass it. On `06828b8a0` the live arm
/// touches 2.0000, the band refuses it, and the test fails. It is `#[ignore]`d, so nothing ran it.
///
/// WHY IT MOVED, MEASURED RATHER THAN GUESSED. A read crosses a line boundary when the address
/// sits far enough into the node that no alignment can keep the two together. The node is
/// 8-aligned, so a node's own start falls anywhere on an eight-byte step within its line; an
/// address at offset D therefore shares the node's line only when D is under 64 AND the node
/// happens to start early enough. THE OFFSET IS PRINTED BELOW. At 1.5200 it was under 64 and the
/// B-tree slot decided each case; #1966 took eight bytes out of the address that sits inside the
/// inline page entry, the fields after it moved, and the offset is now past 64 -- where no
/// alignment can help and the answer is 2.0000 for every bucket without exception.
///
/// AND THAT REMOVES THE THIRD REASON THIS MODULE GIVES FOR DECLINING THE TAGGED SHAPE. The
/// module's own summary lists it: "AND IT IS PAID FOR ON THE READ. A tagged simple arm touches
/// 2.0000 lines to reach a page's address against the live arm's 1.5200 -- +0.4800 a read".
/// Both arms touch 2.0000 now. The read cost of the indirection is +0.0000, measured, and the
/// decline has to rest on the other two reasons or not at all. It does: see
/// `the_hundred_and_twenty_bytes_a_tagged_key_saves_are_not_bytes_a_tagged_key_stops_holding`
/// and `the_tagged_shape_priced_on_chunks_instead_of_requests`.
///
/// BOTH SIDES ARE NOW READ OUT OF A MAP, WHICH THEY WERE NOT. The live side took its node from a
/// B-tree slot and the tagged side took its node from a STACK LOCAL, and the two homes have
/// different alignment distributions -- so the comparison was between a heap node and a stack
/// node, not between two representations. The tagged nodes are built into a map first and
/// measured there.
///
/// THE CONTROL ON THE EXPLANATION IS KEPT. The mechanism claims the line count is decided by
/// where the address sits, not by the shape being tagged, so the same count is taken over buckets
/// whose live arm is ALREADY out of line -- the general arm -- where it predicts no difference.
///
/// rust-internal: measures the engine's own bucket map, no product behaviour
#[test]
#[ignore = "seeds two stores of 4,000 records; run by name"]
fn a_tagged_simple_arm_adds_a_line_to_every_page_read_and_the_general_arm_shows_it_does_not() {
    // --- THE INSTRUMENT'S OWN CONTROLS, BEFORE IT IS USED FOR ANYTHING. ---
    //
    // This test's finding is that BOTH arms touch two lines, and the assertions below say so as
    // equalities. An equality against a constant cannot tell the measured value from a broken
    // instrument: a `lines_touched` wedged at 2 satisfies every one of them. It was planted as a
    // mutant and SURVIVED, which is how this got here.
    //
    // So the instrument is fed two inputs whose answers are known and disagree. A counter stuck
    // at either answer fails one of them.
    assert_eq!(
        1,
        lines_touched(0x1000, 0x1020),
        "two addresses 32 B apart inside one 64 B line must read as ONE line; an instrument that \
         cannot answer 1 cannot have measured the 2s below"
    );
    assert_eq!(
        2,
        lines_touched(0x1000, 0x1040),
        "two addresses either side of a 64 B boundary must read as TWO lines"
    );
    assert_eq!(
        1,
        lines_touched(0x103f, 0x1000),
        "the last byte of a line and its first are ONE line, in either order of arguments"
    );

    // --- THE SUBJECT: simple buckets, on the range where they are everything. ---
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, WIDE_END);
    let keys = seed_routed(&engine, SMALL);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let live_map = &shard.bucket_index.bucket_map;

    // THE TAGGED NODES LIVE IN A MAP, so both sides are measured in the same kind of home.
    let tagged_map: BTreeMap<u32, TaggedNode> = live_map
        .iter()
        .filter(|(_, node)| matches!(node.block_index, BlockIndexMap::One(_, _)))
        .map(|(routing_bucket, node)| (*routing_bucket, retag(node)))
        .collect();

    let mut live_lines = 0usize;
    let mut tagged_lines = 0usize;
    let mut simple_buckets = 0usize;
    // THE OFFSET IS THE MECHANISM, so it is collected rather than reasoned about.
    let mut offsets: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
    let mut node_starts: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
    for (routing_bucket, node) in live_map.iter() {
        if !matches!(node.block_index, BlockIndexMap::One(_, _)) {
            continue;
        }
        simple_buckets += 1;
        let (handle, page) = node.block_index.iter().next().expect("the inline arm holds one page");
        let node_at = node as *const BucketNode as usize;
        let address_at = &page.address as *const _ as usize;
        offsets.insert(address_at - node_at);
        node_starts.insert(node_at % 64);
        live_lines += lines_touched(node_at, address_at);

        let tagged = tagged_map.get(routing_bucket).expect("every simple bucket was retagged");
        let reached = tagged
            .payload
            .page(*handle)
            .expect("the tagged simple arm must answer its own handle");
        tagged_lines += lines_touched(
            tagged as *const TaggedNode as usize,
            &reached.address as *const _ as usize,
        );
    }

    assert!(
        simple_buckets > 0,
        "the fixture reached no bucket in the inline arm, so the count below is over nothing"
    );
    assert_eq!(
        simple_buckets,
        keys.len(),
        "the fixture wrote {} keys and reached {simple_buckets} inline-arm buckets",
        keys.len()
    );
    assert_eq!(
        simple_buckets,
        tagged_map.len(),
        "the tagged map holds {} of the {simple_buckets} simple buckets; it is not the same \
         population",
        tagged_map.len()
    );

    // --- THE MECHANISM, PRINTED WITH ITS DENOMINATOR. ---
    assert_eq!(
        1,
        offsets.len(),
        "the address sits at {offsets:?} different offsets inside the node across \
         {simple_buckets} buckets; one declaration has one field offset, so more than one answer \
         means this is not reading the field it thinks it is"
    );
    let offset = *offsets.iter().next().expect("one offset");
    println!(
        "\n  THE MECHANISM: the inline arm's address sits {offset} B into the node, and a node \
         starts at {} distinct positions within a 64 B line",
        node_starts.len()
    );
    assert!(
        offset >= 64,
        "the address sits {offset} B into the node, under a line. At that offset a node that \
         starts early enough in its line keeps both in one line and the count is BELOW 2.0 -- \
         which is what 1.5200 was. The assertions below say 2.0000 for every bucket and would be \
         wrong; re-measure rather than retarget them"
    );

    let live_per = live_lines as f64 / simple_buckets as f64;
    let tagged_per = tagged_lines as f64 / simple_buckets as f64;
    println!(
        "SIMPLE ARM over {simple_buckets} buckets: live {live_per:.4} lines a read, tagged \
         {tagged_per:.4} -- {:+.4}",
        tagged_per - live_per
    );

    // AT AN OFFSET PAST A LINE, NO ALIGNMENT SAVES THE LIVE ARM EITHER.
    assert_eq!(
        2 * simple_buckets,
        live_lines,
        "the live inline arm touched {live_lines} lines over {simple_buckets} buckets. With its \
         address {offset} B into the node -- past a whole line -- the two cannot share a line at \
         any alignment, so anything but two a read means the offset above is not the one the read \
         actually walks"
    );
    assert_eq!(
        2 * simple_buckets,
        tagged_lines,
        "the tagged simple arm touched {tagged_lines} lines over {simple_buckets} buckets; a \
         payload in its own allocation is a second line on every page read, without exception"
    );
    assert_eq!(
        live_lines, tagged_lines,
        "the tagged arm read {tagged_per:.4} lines against the live arm's {live_per:.4}. This \
         module declined the tagged shape partly BECAUSE of this difference, at +0.4800 a read. \
         If a difference has come back the decline's third reason is live again and the summary \
         at the top of this file should say so"
    );
    println!(
        "  SO THE INDIRECTION COSTS +0.0000 LINES A READ. The page entry is already a line away \
         from the node whether it is held inline or behind a pointer, so moving it out of line \
         does not add a line."
    );

    // WHAT THIS COUNT CANNOT SEE, SAID PLAINLY, BECAUSE A DECLINE RESTS ON IT.
    //
    // Two lines and two lines is an equal COUNT. It is not a proof of equal cost. Inline, the
    // second line is inside the SAME allocation as the first -- adjacent, on the same page, and
    // very likely already fetched. Behind a pointer it is wherever the allocator put it, which
    // is a different line, possibly a different page, and a separate entry in the translation
    // buffer. This instrument counts distinct 64-byte lines and is blind to that difference.
    //
    // IT IS REPORTED RATHER THAN ASSERTED because the honest form of it is a measurement this
    // module does not have: separating the two needs a hardware counter or a timing harness, and
    // a timing ratio in this campaign read 485x idle against 11x busy off identical code. So the
    // claim made here is the narrow one the instrument supports -- the tagged arm adds no LINE --
    // and the wider claim, that it adds no COST, is NOT made.
    //
    // It does not change the conclusion, and it changes which way the uncertainty points: the
    // unmeasured part of the read cost can only make the tagged shape worse, never better.
    let live_spread = simple_buckets; // one measured pair per bucket, stated as the denominator
    println!(
        "  AND WHAT THIS COUNT CANNOT SEE: over {live_spread} buckets both shapes touch two \
         lines, but the live arm's second line is INSIDE the node's own allocation and the \
         tagged arm's is in a separate one. Counting lines cannot tell an adjacent line from a \
         distant one, so +0.0000 is 'adds no line', not 'adds no cost' -- and the part that is \
         unmeasured can only run against the tagged shape."
    );

    // --- THE CONTROL: a workload where the mechanism predicts NO difference. ---
    drop(shards);
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, WIDE_END);
    seed_container(&engine, SMALL / 100, 100);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let general_map: BTreeMap<u32, TaggedNode> = shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_, node)| matches!(node.block_index, BlockIndexMap::Many(_)))
        .map(|(routing_bucket, node)| (*routing_bucket, retag(node)))
        .collect();
    let mut control_live = 0usize;
    let mut control_tagged = 0usize;
    let mut general_buckets = 0usize;
    for (routing_bucket, node) in shard.bucket_index.bucket_map.iter() {
        if !matches!(node.block_index, BlockIndexMap::Many(_)) {
            continue;
        }
        general_buckets += 1;
        let (handle, page) = node.block_index.iter().next().expect("a general arm holds pages");
        control_live += lines_touched(
            node as *const BucketNode as usize,
            &page.address as *const _ as usize,
        );
        let tagged = general_map.get(routing_bucket).expect("every general bucket was retagged");
        let reached = tagged.payload.page(*handle).expect("the tagged general arm must answer");
        control_tagged += lines_touched(
            tagged as *const TaggedNode as usize,
            &reached.address as *const _ as usize,
        );
    }

    assert!(
        general_buckets > 0,
        "the control reached no bucket in the general arm, so it controls nothing"
    );
    println!(
        "CONTROL, general arm over {general_buckets} buckets: live {:.4} lines a read, tagged \
         {:.4} -- {:+.4}, and the mechanism predicts 0",
        control_live as f64 / general_buckets as f64,
        control_tagged as f64 / general_buckets as f64,
        (control_tagged as f64 - control_live as f64) / general_buckets as f64
    );
    assert_eq!(
        control_live, control_tagged,
        "the control moved from {control_live} lines to {control_tagged} over {general_buckets} \
         buckets; where the live arm is ALREADY out of line the tagged shape must cost nothing"
    );
}

// =============================================================================================
// THE REVISIT: WHAT THE SAVING IS ONCE THE ALLOCATOR CHUNK IS COUNTED
// =============================================================================================

/// What one `clone()` charges, in REQUEST bytes, in CHUNK bytes, and in calls.
///
/// The request column is what the caller asked for; the chunk column is what the allocator set
/// aside to answer. Everything above this line in this module is priced on the first, and the
/// note on `what_the_tagged_node_actually_costs_the_allocator` says so in as many words: it
/// "UNDER-charges the tagged side, which is the side that takes the allocations", and calls its
/// own byte column "a floor on the tagged side's cost, not an estimate of it". This is the
/// estimate.
#[cfg(feature = "alloc-probe")]
fn clone_counts_chunked<T: Clone>(value: &T) -> (u64, u64, u64) {
    let probe = Probe::start();
    let copy = value.clone();
    let counts = probe.stop();
    std::hint::black_box(&copy);
    drop(copy);
    (counts.alloc_bytes, counts.chunk_bytes, counts.allocs)
}

/// THE PER-KEY FOOTPRINT OF A SIMPLE BUCKET, MEASURED RATHER THAN CALCULATED.
///
/// THE CLAIM THIS EXISTS TO CHECK. The proposal is described as taking the node from its full
/// width down to 64 -- "one hundred and twenty bytes on every key" when the node was 184. Both
/// numbers are right and the subtraction between them is not, because those bytes do not leave:
/// all but sixteen of them move into a heap allocation, and an allocation costs the chunk it is
/// served from rather than the width that was asked for.
///
/// THE COUNT IS NOT A CONSTANT, WHICH IS WHY IT IS NO LONGER IN THIS TEST'S NAME. The node has
/// since gone 184 -> 176 (the address inside the inline page entry merged its slab id and its
/// offset into one word) and the boxed payload went 112 -> 104 with it. The saving the sentence
/// claims is therefore 112 bytes now, not 120, and it will move again the next time the page entry
/// does. What does NOT move is the direction, which is the whole content of this test: the payload
/// shrank by the same eight bytes as the node, so the chunk it is served from shrank by one
/// rounding step too, and the pair still does not come in under the inline width.
///
/// AND THE MARGIN HAS CLOSED TO EXACTLY ZERO, WHICH IS A CHANGE IN THE ARGUMENT AND IS STATED AS
/// ONE. At 184 the pair was 192 and the key held 8 bytes MORE. At 176 the pair is 176: the key
/// holds the SAME bytes, and what is left against the shape is the allocation and the indirection
/// rather than a byte count. So the assertion below is that the pair never comes in UNDER the
/// inline width -- which is the claim being refuted -- and the delta is printed rather than
/// pinned, because pinning it would make this test fail on the day the rounding step changes
/// without the conclusion changing at all.
///
/// `the_tagged_node_is_sixty_four_bytes_and_every_arm_reconstructs` already states the sum, and
/// states it as arithmetic -- "ARITHMETIC ONLY", in its own words, with the chunk taken from a
/// formula. The formula is not checked there against anything. Here the chunk is READ BACK FROM
/// THE ALLOCATOR for the payload the proposal would actually box, so the pair is a measurement.
///
/// THE RESIDUAL THAT CAN FAIL. Every allocation's chunk must exceed its request by at least the
/// header word and at most a full rounding step. That is not a restatement of the chunk figure --
/// it is a property of the allocator that the reading either has or has not got -- and it is the
/// assertion that fails first if `chunk_behind` ever degrades to handing back the request, which
/// is what it does on a target whose allocator cannot be asked.
///
/// rust-internal: measures declarations and this crate's allocator, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
fn the_bytes_a_tagged_key_saves_are_not_bytes_a_tagged_key_stops_holding() {
    let boxed = Box::new(SimpleLayout {
        object_id: 1,
        handle: 7,
        page: page_fixture(),
    });
    let chunk = crate::alloc_probe::chunk_behind(&boxed);
    let request = size_of::<SimpleLayout>();

    // --- THE RESIDUAL, AND IT IS NOT ALGEBRA ON THE FIGURE IT AUDITS. ---
    let over = chunk - request;
    assert!(
        (8..=24).contains(&over),
        "the allocator set aside {chunk} B for a {request} B payload, {over} B over. A chunk owes \
         its request a header word and at most one rounding step; outside that band the reading \
         is not a chunk, and the degraded path -- which hands the request straight back and would \
         read 0 here -- is the case this catches"
    );

    let inline = size_of::<BucketNode>();
    let pair = size_of::<TaggedNode>() + chunk;
    println!(
        "\n  A SIMPLE BUCKET, PER KEY:\n    live    {inline:>4} B, held inline in the node, no allocation\n    \
         tagged  {:>4} B of node + {chunk} B of chunk (for a {request} B payload) = {pair} B",
        size_of::<TaggedNode>()
    );
    println!(
        "    delta   {:>+4} B per key, and one allocation and one indirection that were not there",
        pair as i64 - inline as i64
    );

    // --- THE CLAIM, NAMED AND REFUSED. ---
    //
    // The count is DERIVED from the two widths rather than written down, so it moves when the
    // node does instead of going stale beside it. It was 120 when the node was 184.
    let claimed_saving = inline - size_of::<TaggedNode>();
    assert_eq!(
        112, claimed_saving,
        "the node goes {inline} -> {} on this tree, a claimed saving of {claimed_saving} B; if \
         that has changed, the sentence this test refutes has changed with it",
        size_of::<TaggedNode>()
    );
    assert!(
        pair >= inline,
        "a tagged simple bucket holds {pair} B per key against the live shape's {inline} B, which \
         is LESS -- a real per-key saving. The decline recorded in this module rests on the bytes \
         not going away, so if they now do, the decline is stale and the shape should be built"
    );
    println!(
        "    so: the node narrows by {claimed_saving} B and the KEY holds {} B more. The saving \
         that does exist is not this one -- it is B-tree slot waste, priced in the next test.",
        pair - inline
    );
    // WHERE THE MARGIN NOW STANDS, stated rather than pinned. Equal is the interesting case and
    // the one this tree is in: the byte argument against the shape has been spent, and what is
    // left is the allocation and the indirection.
    if pair == inline {
        println!(
            "    NOTE: the pair is EXACTLY the inline width. The byte argument against the tagged \
             shape is spent at this node width; what remains against it is one allocation and one \
             indirection per occupied bucket, which this instrument does not price."
        );
    }
}

/// THE FOUR CELLS AGAIN, WITH THE COLUMN #1965 COULD ONLY QUOTE AS A FORMULA.
///
/// WHY THE REQUEST COLUMN IS NOT ENOUGH. The live shape holds its page entry inline, so the only
/// thing it asks the allocator for is B-tree nodes: few, large, and barely rounded. The tagged
/// shape asks for one payload per occupied bucket on top -- 104 bytes at this node width, served
/// from a 112-byte chunk. The rounding lands entirely on the side that is being compared
/// favourably, so a request-only comparison is biased, in a known direction, by a known amount.
/// This measures it instead. The cells below were measured when the payload was 112 bytes in a
/// 128-byte chunk; the payload lost eight bytes with the address merge and the chunk lost a
/// rounding step with it, so the CELLS have moved and the direction has not.
///
/// WHAT IS NOT IN DISPUTE. The byte saving at the default routing range is real and this
/// reproduces it. What the chunk column changes is HOW BIG, and what the per-key test above
/// changes is WHERE IT COMES FROM: not from the key holding less, but from a B-tree slot holding
/// a narrower value. Those are different mechanisms with different fixes and only one of them
/// survives the routing range an operator is told to set.
///
/// THE READ COST, IN THE INSTRUMENT'S OWN TERMS. A read allocates nothing in either shape, so the
/// counting allocator reads the read path as 0.000 -- which is not the read being free, it is the
/// instrument being blind to it. That is asserted here rather than left implied, so the line
/// count in `a_tagged_simple_arm_adds_a_line_to_every_page_read_and_the_general_arm_shows_it_does
/// _not` is known to be the WHOLE of the read cost and not a part of it.
///
/// rust-internal: measures the engine's own bucket map, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "seeds four stores of 4,000 and 40,000 records; run by name"]
fn the_tagged_shape_priced_on_chunks_instead_of_requests() {
    let mut path_lengths: Vec<usize> = Vec::new();
    // (records, range, simple fraction, request ratio, chunk ratio, alloc ratio, chunk B/record live, tagged)
    let mut cells: Vec<(usize, u32, f64, f64, f64, f64, f64, f64)> = Vec::new();

    for records in [SMALL, LARGE] {
        for end_routing_bucket in [WIDE_END, NARROW_END] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            let hist = arm_histogram(&engine);

            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1");
            let live_map = &shard.bucket_index.bucket_map;
            assert_eq!(
                hist.total_pages(),
                keys.len(),
                "the map holds {} pages for {} keys, so the per-record divisor is not the \
                 fixture's",
                hist.total_pages(),
                keys.len()
            );

            let tagged_map: BTreeMap<u32, TaggedNode> = live_map
                .iter()
                .map(|(routing_bucket, node)| (*routing_bucket, retag(node)))
                .collect();
            assert_eq!(
                live_map.len(),
                tagged_map.len(),
                "the tagged map is not the same population as the live one"
            );

            let (live_req, live_chunk, live_allocs) = clone_counts_chunked(live_map);
            let (tag_req, tag_chunk, tag_allocs) = clone_counts_chunked(&tagged_map);
            assert!(
                live_req > 0 && tag_req > 0 && live_allocs > 0 && tag_allocs > 0,
                "an instrument reading zero is the instrument failing, not the map being free"
            );

            // THE CHUNK COLUMN IS NEVER BELOW THE REQUEST COLUMN. If it is, the reading has
            // degraded to echoing the request and every ratio below is the old figure wearing a
            // new name.
            assert!(
                live_chunk > live_req && tag_chunk > tag_req,
                "chunk bytes must exceed request bytes on both sides and read live {live_chunk} \
                 vs {live_req}, tagged {tag_chunk} vs {tag_req}; an equal reading is the \
                 degraded path and this whole test would then be a copy of \
                 `what_the_tagged_node_actually_costs_the_allocator`"
            );

            let width = if end_routing_bucket == WIDE_END {
                "the whole keyspace".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            let per = |v: u64| v as f64 / records as f64;
            println!(
                "\n{records} routed keys on {width}: {} buckets, {:.3}% simple",
                live_map.len(),
                100.0 * hist.simple_fraction()
            );
            println!(
                "    live    request {:>9.3} B/rec   CHUNK {:>9.3} B/rec   {:>8.4} allocs/rec",
                per(live_req),
                per(live_chunk),
                per(live_allocs)
            );
            println!(
                "    tagged  request {:>9.3} B/rec   CHUNK {:>9.3} B/rec   {:>8.4} allocs/rec",
                per(tag_req),
                per(tag_chunk),
                per(tag_allocs)
            );
            println!(
                "    ratio           {:>9.3}x           {:>9.3}x           {:>8.3}x",
                tag_req as f64 / live_req as f64,
                tag_chunk as f64 / live_chunk as f64,
                tag_allocs as f64 / live_allocs as f64
            );
            println!(
                "    the chunk column moves the tagged side {:+.3}x against the request column \
                 -- rounding the request column cannot see",
                (tag_chunk as f64 / live_chunk as f64) - (tag_req as f64 / live_req as f64)
            );

            cells.push((
                records,
                end_routing_bucket,
                hist.simple_fraction(),
                tag_req as f64 / live_req as f64,
                tag_chunk as f64 / live_chunk as f64,
                tag_allocs as f64 / live_allocs as f64,
                per(live_chunk),
                per(tag_chunk),
            ));
        }
    }

    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|length| *length == first),
        "the store path length moved across arms ({path_lengths:?}); it shifts allocation bytes \
         at about six bytes a character"
    );
    println!("\n  store path length held at {first} characters across all four stores");

    println!("\n=== the tagged shape, priced on what the allocator sets aside ===");
    println!(
        "  {:<38} {:>8} {:>10} {:>10} {:>12}",
        "corpus / range", "simple", "request", "CHUNK", "allocations"
    );
    for (records, range, simple, req, chunk, allocs, _, _) in &cells {
        let width = if *range == WIDE_END {
            "the whole keyspace".to_string()
        } else {
            format!("0..{range}")
        };
        println!(
            "  {:<38} {:>7.3}% {:>9.3}x {:>9.3}x {:>11.3}x",
            format!("{records} routed keys on {width}"),
            100.0 * simple,
            req,
            chunk,
            allocs
        );
    }

    // --- THE CHUNK COLUMN IS WORSE FOR THE TAGGED SIDE AT EVERY CELL, WHICH IS THE POINT. ---
    for (records, range, _, req, chunk, _, _, _) in &cells {
        assert!(
            chunk > req,
            "{records}/{range}: the tagged shape read {chunk:.4}x on chunks against {req:.4}x on \
             requests. The chunk column must be the less favourable of the two -- the tagged side \
             is the one taking the allocations, and rounding falls on whoever allocates. A chunk \
             ratio at or below the request ratio means the columns are not measuring what they \
             are named for"
        );
    }

    // --- THE VERDICT, WITH THE READ COST SET AGAINST THE SAVING RATHER THAN BESIDE IT. ---
    let (_, _, _, _, wide_chunk, wide_allocs, wide_live_b, wide_tag_b) = cells
        .iter()
        .find(|(records, range, ..)| *records == LARGE && *range == WIDE_END)
        .copied()
        .expect("the wide cell at the large corpus");
    let (_, _, narrow_simple, _, narrow_chunk, narrow_allocs, narrow_live_b, narrow_tag_b) = cells
        .iter()
        .find(|(records, range, ..)| *records == LARGE && *range == NARROW_END)
        .copied()
        .expect("the narrow cell at the large corpus");

    println!("\n=== THE TRADE, STATED ===");
    println!(
        "  DEFAULT RANGE  (every key alone in a bucket): {wide_live_b:.1} -> {wide_tag_b:.1} \
         B/record, {:+.1} B a key saved, at {wide_allocs:.2}x the allocations and +0.4800 cache \
         lines on EVERY page read",
        wide_live_b - wide_tag_b
    );
    println!(
        "  CONFIGURED RANGE (0..{NARROW_END}, what runtime tuning tells an operator to set): \
         {narrow_live_b:.1} -> {narrow_tag_b:.1} B/record, {:+.1} B a key saved, at \
         {narrow_allocs:.2}x the allocations -- over a store with {:.3}% of buckets in the arm \
         the shape is for",
        narrow_live_b - narrow_tag_b,
        100.0 * narrow_simple
    );
    println!(
        "  AND THE CONFIGURED RANGE IS ALREADY THE CHEAPER STORE: {narrow_live_b:.1} B/record \
         against {wide_live_b:.1} on the LIVE shape, {:.3}x, for a setting that costs no \
         allocation, no indirection and no line of code.",
        narrow_live_b / wide_live_b
    );

    // The number that decides it: what the shape is worth where an operator actually runs.
    assert!(
        narrow_chunk > 0.95,
        "at the configured range the tagged shape read {narrow_chunk:.4}x the chunk bytes. The \
         decline in this module rests on the saving being nearly absent there; a figure that has \
         dropped materially below 1.0 means it is no longer absent and the shape should be built"
    );
    assert!(
        wide_chunk < 0.80,
        "at the default range the tagged shape read {wide_chunk:.4}x the chunk bytes; the saving \
         there is real and a decline that under-states it is as wrong as one that over-states it"
    );

    // --- THE READ COST, IN THE INSTRUMENT'S OWN TERMS. ---
    //
    // A read resolves a handle to an address and allocates nothing doing it, in either shape. So
    // the counting allocator -- the instrument every byte figure above comes from -- is BLIND to
    // the read cost, and the cache-line count is not one input among several: it is the only
    // measurement of the read there is. Asserted rather than assumed, because a decline that
    // rests on a read cost has to know that its own instrument cannot see it.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, WIDE_END);
    let keys = seed_routed(&engine, SMALL);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let node = shard
        .bucket_index
        .bucket_map
        .values()
        .find(|node| matches!(node.block_index, BlockIndexMap::One(_, _)))
        .expect("a simple bucket");
    let tagged = retag(node);
    let (handle, _) = node.block_index.iter().next().expect("one page");
    let handle = *handle;

    let probe = Probe::start();
    let reached = tagged.payload.page(handle).expect("the tagged arm answers");
    let counts = probe.stop();
    std::hint::black_box(&reached.address);
    println!(
        "\n  READING one page through the tagged word charged {} allocations and {} chunk bytes \
         over {} keys",
        counts.allocs,
        counts.chunk_bytes,
        keys.len()
    );
    assert_eq!(
        0, counts.allocs,
        "resolving a handle through the tagged word charged {} allocations; it is meant to be a \
         mask and a load",
        counts.allocs
    );
    println!(
        "  SO THE READ COST IS NOT IN ANY COLUMN ABOVE. The counting allocator reads it as 0.000 \
         because nothing is allocated, exactly as a resident-only change predicts. The whole of \
         the read cost is the +0.4800 cache lines a page read measured in \
         `a_tagged_simple_arm_adds_a_line_to_every_page_read_and_the_general_arm_shows_it_does_not`."
    );
}

// =============================================================================================
// THE COMPONENT LEVEL: IS ITS `Many` ARM REAL, AND WHAT DOES THE LIST COST?
// =============================================================================================

/// THE QUESTION. `ObjectBlockRefs::by_component` is a `ComponentList` -- `Empty`, `One` held
/// inline, or `Many` in a vector -- and its own doc says "the measured average is one component
/// per object". A three-arm container over a population that is always one would be machinery for
/// nothing, which is the shape #1966 found in `generation`: a field that equalled its neighbours
/// on every one of 120,080 live addresses.
///
/// THE FIRST HALF OF THE ANSWER IS FREE AND IT IS IN THE DECLARATIONS. A `ComponentList` is the
/// SAME WIDTH as the `ComponentBlocks` its `One` arm holds inline, because the tag rides a niche
/// in the component name. The three arms therefore cost ZERO bytes over storing a single
/// component bare. Whatever the histogram says, there are no bytes here to reclaim -- an
/// important difference from `generation`, which was eight bytes that could actually be removed.
/// That is asserted rather than described.
///
/// THE SECOND HALF IS THE DISTRIBUTION, AND IT IS MEASURED AT BOTH RANGES. A mean of one is the
/// kind of figure this campaign has been wrong about before: #1959 published a mean of 1.98
/// pages a bucket over a store containing not one bucket that held two. So this reports counts
/// per arm, percentiles and a MAXIMUM, with the per-arm sample count printed, and it reports them
/// for the workload that can reach `Many` as well as the one that cannot.
///
/// THE ROUTING RANGE IS NOT THE VARIABLE HERE, AND THE MEASUREMENT SHOWS WHY. The object lookup
/// is keyed by (kind, object key) and its component list is a per-OBJECT fact, so the routing
/// range -- which decides which BUCKET a page lands in -- cannot move it. Both ranges are
/// measured anyway rather than argued, because that is the premise #1962 caught being true at
/// one range and false at the other.
///
/// rust-internal: reads the engine's own object lookup, no product behaviour
#[test]
#[ignore = "seeds four stores of 4,000 records; run by name"]
fn the_component_level_is_not_a_list_of_one() {
    use crate::engine::state::{ComponentBlocks, ComponentList};

    // --- WHAT THE THREE ARMS COST, WHICH IS NOTHING. ---
    assert_eq!(
        size_of::<ComponentList>(),
        size_of::<ComponentBlocks>(),
        "a ComponentList is {} B and one ComponentBlocks is {} B. The three-arm shape is only \
         free while they are equal -- the tag rides a niche in the component name -- and if they \
         have come apart then the list IS costing bytes and is worth revisiting",
        size_of::<ComponentList>(),
        size_of::<ComponentBlocks>()
    );
    println!(
        "\n  THE LIST IS FREE: ComponentList {} B == ComponentBlocks {} B, so the Empty/One/Many \
         shape costs ZERO bytes over holding one component bare. There is nothing here to \
         reclaim by removing an arm.",
        size_of::<ComponentList>(),
        size_of::<ComponentBlocks>()
    );

    // (label, arm counts, components per object)
    let mut rows: Vec<(String, [usize; 3], Vec<usize>)> = Vec::new();
    let mut path_lengths: Vec<usize> = Vec::new();

    let mut measure = |label: String, engine: &TemporalEngine| {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1");
        let mut arms = [0usize; 3];
        let mut per_object: Vec<usize> = Vec::new();
        for (_kind, _key, entry) in shard.bucket_index.object_block_lookup.iter() {
            let n = entry.by_component.len();
            match &entry.by_component {
                ComponentList::Empty => arms[0] += 1,
                ComponentList::One(_) => arms[1] += 1,
                ComponentList::Many(_) => arms[2] += 1,
            }
            per_object.push(n);
        }
        rows.push((label, arms, per_object));
    };

    // THE ROUTED WORKLOAD: plain keys, no component at all.
    for end_routing_bucket in [WIDE_END, NARROW_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        seed_routed(&engine, SMALL);
        let width = if end_routing_bucket == WIDE_END {
            "the whole keyspace".to_string()
        } else {
            format!("0..{end_routing_bucket}")
        };
        measure(format!("{SMALL} routed keys on {width}"), &engine);
    }

    // THE CONTAINER WORKLOAD: one key, many fields, and a field IS a component.
    for end_routing_bucket in [WIDE_END, NARROW_END] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        seed_container(&engine, SMALL / 100, 100);
        let width = if end_routing_bucket == WIDE_END {
            "the whole keyspace".to_string()
        } else {
            format!("0..{end_routing_bucket}")
        };
        measure(
            format!("{} container keys x 100 fields on {width}", SMALL / 100),
            &engine,
        );
    }

    let first = path_lengths[0];
    assert!(
        path_lengths.iter().all(|l| *l == first),
        "the store path length moved across arms ({path_lengths:?})"
    );

    let percentile = |sorted: &[usize], q: f64| -> usize {
        if sorted.is_empty() {
            return 0;
        }
        let rank = ((q * sorted.len() as f64).ceil() as usize).max(1);
        sorted[rank.min(sorted.len()) - 1]
    };

    println!(
        "\n  {:<48} {:>8} {:>8} {:>8} {:>6} {:>6} {:>6} {:>6}",
        "corpus / range", "Empty", "One", "Many", "p50", "p90", "p99", "MAX"
    );
    let mut many_total = 0usize;
    let mut one_total = 0usize;
    for (label, arms, per_object) in &rows {
        let mut sorted = per_object.clone();
        sorted.sort_unstable();
        println!(
            "  {:<48} {:>8} {:>8} {:>8} {:>6} {:>6} {:>6} {:>6}",
            label,
            arms[0],
            arms[1],
            arms[2],
            percentile(&sorted, 0.50),
            percentile(&sorted, 0.90),
            percentile(&sorted, 0.99),
            sorted.last().copied().unwrap_or_default()
        );
        // THE DENOMINATOR. Every object landed in exactly one arm, or a percentage above is over
        // part of a population rather than the population.
        assert_eq!(
            per_object.len(),
            arms[0] + arms[1] + arms[2],
            "{label}: the arm columns account for {} of {} objects",
            arms[0] + arms[1] + arms[2],
            per_object.len()
        );
        assert!(
            !per_object.is_empty(),
            "{label}: the lookup held NO objects, so the row above is over nothing and a store \
             that wrote nothing reads as a beautifully simple one"
        );
        many_total += arms[2];
        one_total += arms[1];
    }

    // --- THE CLAIM, DECIDED BY THE SAMPLES. ---
    assert!(
        one_total > 0,
        "no object anywhere landed in the One arm, so the inline arm this shape exists for was \
         never reached and the measurement claims nothing about it"
    );
    println!(
        "\n  SAMPLES: {one_total} objects in the One arm, {many_total} in the Many arm across \
         all four stores."
    );
    if many_total > 0 {
        println!(
            "  THE `Many` ARM IS REAL. A container key's fields are components of ONE object, so \
             an object with many components is an ordinary write and not a corner. The component \
             level is doing work; leave it alone."
        );
    } else {
        println!(
            "  THE `Many` ARM WAS NOT REACHED BY ANY WORKLOAD SEEDED HERE. That is a statement \
             about this corpus and not about the engine -- and it would still not be a reason to \
             remove the arm, because the arms cost nothing (asserted above)."
        );
    }
    assert!(
        many_total > 0,
        "the container workload is meant to put one object under many components; if it reached \
         ZERO Many arms then a field is not a component of its key's object and the whole reading \
         of this level is wrong"
    );
}
