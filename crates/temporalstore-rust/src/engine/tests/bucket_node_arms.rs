// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WOULD A BUCKET THAT HOLDS ONE PAGE BE CHEAPER AS ONE TAGGED WORD?
//!
//! THE PROPOSAL. `BucketNode` is the widest per-item structure in the engine and there is one
//! per routing bucket. #1958 took it 208 -> 200 and #1961 took it 200 -> 192, both by narrowing
//! fields. The shape proposed next does not narrow a field: it replaces the node's three
//! container members -- `object_index`, `deleted_object_index` and `block_index`, 128 of the 184
//! bytes -- with ONE TAGGED WORD whose tag says whether the bucket is simple or general, and
//! puts the payload of each arm behind that word. A bucket holding one page would then carry a
//! 64-byte node and one allocation instead of a 184-byte node and none.
//!
//! THREE THINGS HAD TO BE MEASURED BEFORE IT COULD BE WRITTEN, and all three are here.
//!
//!   1. HOW OFTEN IS A BUCKET SIMPLE. `classify_bucket_layout` already answers it, and its
//!      answer is reported here as a histogram with per-arm counts, percentiles and a maximum --
//!      at two corpus sizes, at the DEFAULT routing range AND at the range
//!      `docs/runtime_tuning.md` tells an operator to set. #1962 found a premise that was true
//!      at the default and false at the configured range; #1959 reported a mean of 1.98 pages a
//!      bucket over a store containing not one bucket that held two.
//!   2. WHAT THE WORD WOULD COST, ON THE ALLOCATOR AND NOT ON `size_of`. Both shapes are cloned
//!      under the counting allocator in the same span of the same test, so the B-tree node that
//!      holds them is charged on both sides. A published decline in this area had its sign
//!      INVERTED by setting a `size_of` saving against an allocator cost, and this change is
//!      exactly that trade: inline bytes for a heap allocation.
//!   3. WHAT IT WOULD COST A READ. An out-of-line arm puts the address in a different allocation
//!      from the node, so reaching it touches a second line. That is measured here from the
//!      addresses themselves, not asserted, and it is measured on a workload where the
//!      mechanism predicts NO difference as the control.
//!
//! THE ANSWER IS NO, AND IT IS NOT "BECAUSE IT SAVES NOTHING". IT SAVES 40% OF THE INDEX'S
//! BYTES IN ONE CONFIGURATION AND 1.2% IN THE OTHER, AND THE CONFIGURATION THAT WINS IS THE ONE
//! NOBODY IS TOLD TO RUN. The four measured cells, on one instrument, both sides:
//!
//! ```text
//!   corpus / range                            simple      bytes  allocations
//!   4000 routed keys on the whole keyspace  100.000%     0.581x       7.265x
//!   4000 routed keys on 0..1023               4.510%     0.936x       1.317x
//!   40000 routed keys on the whole keyspace 100.000%     0.604x       7.559x
//!   40000 routed keys on 0..1023              0.000%     0.988x       1.123x
//! ```
//!
//! READ THE FIRST COLUMN AGAINST THE SECOND AND THE WHOLE FINDING IS THERE. The saving is not a
//! property of the shape; it is a property of HOW MANY BUCKETS ARE IN THE ARM THE SHAPE IS FOR,
//! and that number is decided by the routing range rather than by the workload. Three things
//! follow, and each of them on its own is the decline:
//!
//!   1. WHERE THE SHAPE WINS, A KNOB THAT ALREADY SHIPS WINS ALMOST AS MUCH FOR NOTHING. On the
//!      same instrument and the same corpus the LIVE shape reads 314.596 B/record on the whole
//!      keyspace and 198.856 B/record on 0..1023 -- 0.632x, against the tagged shape's 0.604x --
//!      and `docs/runtime_tuning.md` already tells an operator to set exactly that before the
//!      first ingest. The knob costs no allocation, no indirection and no line of code.
//!   2. WHERE THE KNOB IS SET, THE ARM IS GONE AND SO IS THE SAVING. `single_page_object` is
//!      4.510% of occupied buckets at 4,000 records on 0..1023 and 0.000% at 40,000, where
//!      `multi_object` is 100.000%. At 0.000% simple the tagged shape saves 1.2% of the bytes
//!      and still costs 1.123x the allocations. That cell is the control on the explanation: a
//!      configuration where the mechanism predicts the saving should be absent, measured.
//!   3. AND IT IS PAID FOR ON THE READ. A tagged simple arm touches 2.0000 lines to reach a
//!      page's address against the live arm's 1.5200 -- +0.4800 a read, on every page read, at
//!      the range where the saving exists at all.
//!
//! THE PREMISE IS ALSO WRONG, AND THAT MATTERS SEPARATELY FROM THE PRICE. The proposal says the
//! node "carries the whole general case for every key". It does not. Every one of the three
//! container members is ALREADY a biased `Empty`/`One`/`Many` shape whose general arm is the
//! only one that allocates: `BlockIndexMap` holds its single page inline, `ObjectIndex` holds
//! its single id inline, `DeletedObjectIndex` is one nullable pointer that is null in the case
//! it is almost always in. A simple bucket allocates NOTHING for the general case today, so
//! there is no general case for a tagged word to take out of it. What the word moves out of
//! line is the simple bucket's OWN data, and that is why the cost lands on the read.
//!
//! AND THAT DATA DOES NOT FIT IN A WORD. The design being compared against holds a page's
//! address in 64 bits. Here a `BlockAddress` is 48 bytes of its own -- two slab coordinates, two
//! identities, a length, a block id, a routing bucket and a presence byte -- and the page entry
//! around it carries three shared names as well, for 104. #1962 established that those three
//! names are per-object and per-page facts that cannot be hoisted to the bucket: hoisting them
//! "would have passed every test at the default range and lost pages silently at the cluster
//! range". So the simple arm cannot be a word here, and the most it can be is a pointer to 120
//! bytes -- which is the shape this module prices and declines.
//!
//! THE MEASUREMENT DOES NOT DEPEND ON THE DECLINE BEING RIGHT. Every figure is printed with its
//! denominator, the mirror of the live declaration is asserted against the declaration itself
//! before any price is quoted, and the instrument recovers a planted marker exactly. If the
//! numbers move the decline should be revisited rather than re-argued.
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
        104,
        size_of::<BlockIndexMap>(),
        "the page index is {} bytes in the node, not 104; the accounting below is stale",
        size_of::<BlockIndexMap>()
    );
    assert_eq!(16, size_of::<ObjectIndex>(), "the object index moved");
    assert_eq!(
        8,
        size_of::<DeletedObjectIndex>(),
        "the tombstone index moved"
    );

    // The three members sum to 128 of the node's 184 -- the bytes the proposal would replace
    // with one word. It was 136 of 192 until the address inside the inline page entry shed
    // its derived generation; the members and the node each lost the same eight bytes, so
    // what the proposal would replace is unchanged in kind and eight smaller in size.
    let members = size_of::<BlockIndexMap>() + size_of::<ObjectIndex>() + size_of::<DeletedObjectIndex>();
    assert_eq!(
        128, members,
        "the three container members are {members} bytes, not 128"
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
        40,
        size_of::<crate::block_store::BlockAddress>(),
        "an address is {} bytes, not 40",
        size_of::<crate::block_store::BlockAddress>()
    );
    assert_eq!(
        96,
        size_of::<BlockIndex>(),
        "a page entry is {} bytes, not 96",
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
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    last_dump_sequence: u64,
    payload: TaggedLayout,
}

/// THE CONTROL FOR EVERY WIDTH BELOW: the live declaration, rebuilt from its own field types. If
/// this is not `size_of::<BucketNode>()` then the tagged node is being compared against a
/// structure this engine does not have.
#[allow(dead_code)]
struct LiveNodeMirror {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    last_dump_sequence: u64,
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
        dirty: node.dirty,
        deleted: node.deleted,
        meta_loaded: node.meta_loaded,
        loading: node.loading,
        in_memory: node.in_memory,
        ttl_ms: node.ttl_ms,
        dirty_generation: node.dirty_generation,
        first_dirty_wal_sequence: node.first_dirty_wal_sequence,
        first_dirty_index_log_sequence: node.first_dirty_index_log_sequence,
        last_dump_sequence: node.last_dump_sequence,
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
fn the_tagged_node_is_sixty_four_bytes_and_every_arm_reconstructs() {
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
                + 4 * size_of::<u64>()
                + size_of::<ObjectIndex>()
                + size_of::<DeletedObjectIndex>()
                + size_of::<BlockIndexMap>(),
            size_of::<u32>() + size_of::<BucketLayoutState>() + 5,
        ),
        (
            "TaggedNode (proposed)",
            size_of::<TaggedNode>(),
            size_of::<BucketTtl>() + 4 * size_of::<u64>() + size_of::<TaggedLayout>(),
            size_of::<u32>() + size_of::<BucketLayoutState>() + 5,
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

    assert_eq!(184, size_of::<BucketNode>(), "the live node moved");
    assert_eq!(
        64,
        size_of::<TaggedNode>(),
        "the tagged node is {} bytes, not 64",
        size_of::<TaggedNode>()
    );
    assert_eq!(
        112,
        size_of::<SimpleLayout>(),
        "the simple payload is {} bytes, not 112",
        size_of::<SimpleLayout>()
    );

    // --- AND THE ARITHMETIC THAT READS AS A WIN, NAMED AS ARITHMETIC. ---
    //
    // 128 bytes off the struct, and then a 120-byte allocation for very nearly every bucket at
    // the default range. glibc serves a 120-byte request from a 128-byte chunk once it has taken
    // its own header word and rounded to a multiple of sixteen, so the PAIR is 64 + 128 = 192 --
    // exactly what it replaced, before the allocation itself and before the pointer chase. This
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

/// THE READ PATH, COUNTED -- AND THE CONTROL ON THE EXPLANATION.
///
/// #1961 made reads FASTER and said so, because the default assumption for a representation
/// change is a trade. So the read side is measured here rather than assumed, and it is measured
/// as a COUNT: a timing ratio in this campaign read 485x idle against 11x busy off identical
/// code, while counts repeat to three significant figures.
///
/// WHAT IS COUNTED. The distinct 64-byte lines between the node and the `BlockAddress` a read
/// resolves through, taken from the two addresses at run time. Inline, the address lives in the
/// node; behind a tagged word it lives in a separate allocation and can only share a line by
/// coincidence.
///
/// THE CONTROL ON THE EXPLANATION. The mechanism claims the extra line comes from the arm being
/// OUT OF LINE, not from the shape being tagged. So the same count is taken over buckets whose
/// live arm is ALREADY out of line -- the general arm, where `BlockIndexMap::Many` holds its
/// pages in a B-tree node of its own -- and there the mechanism predicts NO difference. It is
/// measured, and the prediction is asserted.
///
/// rust-internal: measures the engine's own bucket map, no product behaviour
#[test]
#[ignore = "seeds two stores; run by name"]
fn a_tagged_simple_arm_adds_a_line_to_every_page_read_and_the_general_arm_shows_it_does_not() {
    // --- THE SUBJECT: simple buckets, on the range where they are everything. ---
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, WIDE_END);
    let keys = seed_routed(&engine, SMALL);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let live_map = &shard.bucket_index.bucket_map;

    let mut live_lines = 0usize;
    let mut tagged_lines = 0usize;
    let mut simple_buckets = 0usize;
    let mut held = Vec::new();
    for node in live_map.values() {
        if !matches!(node.block_index, BlockIndexMap::One(_, _)) {
            continue;
        }
        simple_buckets += 1;
        let (handle, page) = node.block_index.iter().next().expect("the inline arm holds one page");
        live_lines += lines_touched(node as *const BucketNode as usize, &page.address as *const _ as usize);

        let tagged = retag(node);
        let reached = tagged
            .payload
            .page(*handle)
            .expect("the tagged simple arm must answer its own handle");
        tagged_lines += lines_touched(
            &tagged as *const TaggedNode as usize,
            &reached.address as *const _ as usize,
        );
        held.push(tagged);
    }
    std::hint::black_box(&held);

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
    let live_per = live_lines as f64 / simple_buckets as f64;
    let tagged_per = tagged_lines as f64 / simple_buckets as f64;
    println!(
        "\nSIMPLE ARM over {simple_buckets} buckets: live {live_per:.4} lines a read, tagged \
         {tagged_per:.4} -- {:+.4}",
        tagged_per - live_per
    );
    // THE LIVE ARM IS NOT 1.0, AND THE FIRST RUN OF THIS TEST SAID SO. It was written asserting
    // one line, on the reasoning that the address is IN the node; it measured 1.52. The node is
    // 192 bytes and the address sits 96 bytes into it, so a node whose own start is not
    // line-aligned puts the two in different lines, and the B-tree slot a node lands in decides
    // that. The assertion now carries the measured value rather than the assumed one.
    //
    // THE BAND EXCLUDES THE VALUE IT GUARDS AGAINST. Its upper end is below 2.00, which is what
    // the tagged arm costs, so a live arm that had quietly become an out-of-line one could not
    // pass this.
    assert!(
        (1.40..1.70).contains(&live_per),
        "the live inline arm touched {live_per:.4} lines a read over {simple_buckets} buckets; \
         measured at 1.5200, and a band that admitted 2.00 could not tell an inline arm from an \
         out-of-line one"
    );
    assert_eq!(
        2 * simple_buckets,
        tagged_lines,
        "the tagged simple arm touched {tagged_lines} lines over {simple_buckets} buckets; a \
         payload in its own allocation is a second line on every page read, without exception"
    );
    assert!(
        tagged_per > live_per,
        "the tagged simple arm read {tagged_per:.4} lines against the live arm's {live_per:.4}; \
         if the indirection has become free the decline recorded here is stale"
    );

    // --- THE CONTROL: a workload where the mechanism predicts NO difference. ---
    drop(shards);
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, WIDE_END);
    seed_container(&engine, SMALL / 100, 100);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    let mut control_live = 0usize;
    let mut control_tagged = 0usize;
    let mut general_buckets = 0usize;
    let mut held = Vec::new();
    for node in shard.bucket_index.bucket_map.values() {
        if !matches!(node.block_index, BlockIndexMap::Many(_)) {
            continue;
        }
        general_buckets += 1;
        let (handle, page) = node.block_index.iter().next().expect("a general arm holds pages");
        control_live += lines_touched(
            node as *const BucketNode as usize,
            &page.address as *const _ as usize,
        );
        let tagged = retag(node);
        let reached = tagged.payload.page(*handle).expect("the tagged general arm must answer");
        control_tagged += lines_touched(
            &tagged as *const TaggedNode as usize,
            &reached.address as *const _ as usize,
        );
        held.push(tagged);
    }
    std::hint::black_box(&held);

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
         buckets; where the live arm is ALREADY out of line the tagged shape must cost nothing, \
         and if it does not then the extra line on the simple arm is not coming from the \
         indirection this module says it comes from"
    );
}
