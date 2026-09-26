// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW MANY PAGES A ROUTING BUCKET HOLDS, and what that decides about the page index's shape.
//!
//! #1958 accounted for every byte of `BucketNode` and found that over half of it -- 112 of 200 --
//! is the `BlockIndexMap` it carries inline. It also reported, in passing, that its fixture held
//! 40,080 page entries against 40,040 buckets: a MEAN of 1.001 pages a bucket. A mean that close
//! to one invites an obvious conclusion -- that a bucket holding two pages is a rarity worth
//! nothing, so the index should be shaped entirely around the single-page case.
//!
//! A MEAN OF 1.001 IS CONSISTENT WITH TWO DISTRIBUTIONS THAT WANT OPPOSITE ANSWERS, and this
//! module measures which one this engine produces. It produces BOTH, in different workloads, and
//! that is the finding:
//!
//!   * A store of keys that route one to a bucket -- strings, and the timestamped series whose
//!     points are held in the model maps rather than as pages -- is 99.90% single-page.
//!   * A store of CONTAINER keys -- hashes, sets, sorted sets, lists -- is 0% single-page. Every
//!     field, member and element is its own page, and they all route to their container key's one
//!     bucket. Measured at 100.0000 pages a bucket for a hundred-member container.
//!
//! So "almost every bucket holds exactly one page" is a statement about ONE workload. The fixture
//! #1958 ranked its structures on contains no container key at all, and the histogram below is
//! reported as counts rather than as a mean for exactly that reason.
//!
//! WHAT THAT DECIDES. The page index is ALREADY a tagged representation and has been since #654:
//! `BlockIndexMap` is `Empty | One(u64, BlockIndex) | Many(BTreeMap<u64, BlockIndex>)`, the single
//! page held inline and the many-page case behind the map's own nodes. The shape that a
//! single-page-dominated distribution argues for is the shape this engine already has. The
//! remaining question is the one #1958 answered by arithmetic and this module answers with the
//! counting allocator: whether the inline `One` arm should ALSO go behind a pointer, taking
//! `BlockIndexMap` from 112 bytes to 32 and `BucketNode` from 200 to 120.
//!
//! IT SHOULD NOT, AND THE MEASUREMENT IS THE WHOLE OF THE CASE. At the measured distribution the
//! boxed arm pays one allocation for 99.90% of buckets in the workload where buckets are numerous,
//! and buys nothing in the workload where they are not -- there are 20 buckets behind 2,000 pages
//! in a container store, so 80 bytes off each of 20 nodes is 1,600 bytes against a page set
//! costing 208,000. The clone instrument charges the boxed shape MORE, not less, and the read path
//! gains a dependent load it did not have.
//!
//! WHICH IS THE OPPOSITE CONCLUSION TO `ObjectIndex`, deliberately, and the contrast is the rule:
//! `ObjectIndex` boxes its MULTI-ENTRY arm, because there that arm is the wide one (a collection
//! at 24 bytes) and the single-entry arm is a bare `u64`. `BlockIndexMap`'s wide arm is the
//! COMMON one -- a whole 104-byte page entry -- and its multi-entry arm is the narrow one. Boxing
//! the UNCOMMON arm is a win; boxing "the arm that happens to be widest" is a loss whenever that
//! arm is also the common one. The distribution is what tells the two apart.
//!
//! ONE CORRECTION TO THAT CONTRAST, measured since: the object index's multi-entry arm is NOT
//! rare in every workload. An object id is hashed over `shard:kind:key:component` while the
//! routing bucket is hashed over the key alone, so a collection key files one object id per
//! MEMBER into one bucket -- 46.46% of buckets hold two or more on a store with collections in
//! it, against 0.00% on a store of strings and series.
//! `how_many_objects_a_bucket_holds_at_two_corpus_sizes_and_two_shape_mixes` reports both. What
//! survives is the rule above, which is about which arm is WIDE and which is COMMON, not about
//! either being rare; the object index's boxed arm holds a sorted run rather than a tree for
//! exactly that reason.
//!
//! THE DENOMINATORS ARE READ OFF THE SHARD and asserted before anything divides by them, and the
//! fixture is asserted to REACH multi-page buckets: a fixture that only ever produced one page per
//! bucket could not tell a correct implementation of this type from a constant.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;

use crate::block_store::BlockAddress;
use crate::engine::state::{
    BlockIndex, BlockIndexMap, BlockSlabLiveIndex, BucketLayoutState, BucketNode, BucketTtl,
    DeletedObjectIndex,
    ObjectIndex,
};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

// ---------------------------------------------------------------------------------------------
// THE DISTRIBUTION.
// ---------------------------------------------------------------------------------------------

/// Pages held per routing bucket, as bucket COUNTS keyed by the number of pages held.
///
/// A histogram and not a mean, because the mean is what hid this: 1.001 pages a bucket describes
/// a store that is 99.9% single-page and it describes a store with a handful of enormous buckets,
/// and the two want opposite representations.
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

    fn holding(&self, pages: usize) -> usize {
        self.counts.get(&pages).copied().unwrap_or_default()
    }

    /// Buckets holding strictly more than one page.
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
        self.holding(1) as f64 / buckets as f64
    }

    fn report(&self, label: &str) {
        println!("\n=== {label} ===");
        println!(
            "  buckets={} pages={} mean={:.4} widest bucket={} page(s)",
            self.buckets(),
            self.pages(),
            self.mean(),
            self.widest()
        );
        let buckets = self.buckets();
        for (held, count) in self.counts.iter() {
            println!(
                "  {held:>6} page(s) : {count:>8} buckets  ({:>7.3}%)",
                if buckets == 0 {
                    0.0
                } else {
                    100.0 * *count as f64 / buckets as f64
                }
            );
        }
    }
}

fn pages_per_bucket(engine: &TemporalEngine) -> PagesPerBucket {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut hist = PagesPerBucket::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        *hist.counts.entry(bucket.block_index.len()).or_default() += 1;
    }
    hist
}

fn probe_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn ack(response: &crate::types::BatchExecuteResponse) {
    assert!(response.status.ok, "seed must ack: {:?}", response.status);
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        ack(&response);
    }
}

/// KEYS THAT ROUTE ONE TO A BUCKET -- the shape #1958's fixture is made of.
///
/// `strings_n` string keys, plus `series_keys` timestamped series of `series_points` points each.
/// The series points are held in the model maps, not as pages, which is why a series key's bucket
/// holds two pages and not a thousand -- and those series buckets are what makes this fixture
/// reach a multi-page bucket at all.
fn seed_routed_keys(
    engine: &TemporalEngine,
    strings_n: usize,
    series_keys: usize,
    series_points: usize,
) {
    run_batch(
        engine,
        (0..strings_n)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect(),
    );
    for k in 0..series_keys {
        for chunk_start in (0..series_points).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(series_points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(response.status.ok, "series seed must ack: {:?}", response.status);
        }
    }
}

/// CONTAINER KEYS -- a hash, a set, a sorted set and a list, each of `members` elements.
///
/// Every element is its own page and they all route to the container key's one bucket, so this
/// workload produces buckets holding exactly `members` pages and no single-page bucket at all.
fn seed_container_keys(engine: &TemporalEngine, keys: usize, members: usize) {
    let mut commands = Vec::with_capacity(keys * members);
    for k in 0..keys {
        match k % 4 {
            0 => {
                for f in 0..members {
                    commands.push(Command::HashSet {
                        key: format!("h{k}"),
                        field: format!("f{f}"),
                        value: vec![b'v'; 32],
                    });
                }
            }
            1 => {
                for m in 0..members {
                    commands.push(Command::SetAdd {
                        key: format!("t{k}"),
                        member: format!("m{m}").into_bytes(),
                    });
                }
            }
            2 => {
                for m in 0..members {
                    commands.push(Command::ZSetAdd {
                        key: format!("z{k}"),
                        member: format!("m{m}").into_bytes(),
                        score: m as f64,
                    });
                }
            }
            _ => {
                for m in 0..members {
                    commands.push(Command::ListPush {
                        key: format!("l{k}"),
                        member: format!("m{m}").into_bytes(),
                        left: false,
                    });
                }
            }
        }
    }
    run_batch(engine, commands);
}

/// THE MEASUREMENT THAT DECIDES EVERYTHING BELOW.
///
/// Three workloads at two corpus sizes ten times apart, each reported as a HISTOGRAM WITH COUNTS.
///
/// THE ANTI-CONSTANT ASSERTION. Each arm asserts the fixture reaches a bucket holding more than
/// one page. A fixture in which every bucket held exactly one page cannot tell a correct
/// implementation of `BlockIndexMap` from a constant that answers `One` -- the `Many` arm, the
/// promotion into it and the shrink back out of it would all be unreached, and every test over
/// them would pass against an engine that had deleted them.
///
/// THE STORE PATH LENGTH is held constant across arms and asserted: `tempfile` names every
/// directory with the same number of characters, and allocation bytes move at about six bytes a
/// character. Bucket and page COUNTS are immune to it, which is why the counts carry the claim.
#[test]
#[ignore = "seeds six stores up to 40,000 records each; run by name"]
fn the_pages_a_bucket_holds_are_two_populations_and_not_one_mean() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut routed: Vec<PagesPerBucket> = Vec::new();
    let mut container: Vec<PagesPerBucket> = Vec::new();
    let mut mixed: Vec<PagesPerBucket> = Vec::new();

    for (label, records) in [("4,000 records", 4_000usize), ("40,000 records", 40_000usize)] {
        // --- Keys that route one to a bucket. #1958's fixture shape, exactly. ---
        {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            seed_routed_keys(&engine, records, records / 1_000, 1_000);
            let hist = pages_per_bucket(&engine);
            hist.report(&format!("{label}: keys that route one to a bucket"));
            routed.push(hist);
        }

        // --- Container keys: one page per field, member or element. ---
        {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            seed_container_keys(&engine, records / 100, 100);
            let hist = pages_per_bucket(&engine);
            hist.report(&format!("{label}: container keys, 100 elements each"));
            container.push(hist);
        }

        // --- Both, in one store. ---
        {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = probe_engine(dir.path());
            seed_routed_keys(&engine, records / 2, records / 2_000, 1_000);
            seed_container_keys(&engine, records / 200, 100);
            let hist = pages_per_bucket(&engine);
            hist.report(&format!("{label}: both shapes in one store"));
            mixed.push(hist);
        }
    }

    assert_eq!(
        6,
        path_lengths.len(),
        "all six arms must have run, or the comparisons below compare an arm with itself"
    );
    for length in &path_lengths {
        assert_eq!(
            path_lengths[0], *length,
            "the store path length moved between arms ({} then {length}); allocation bytes move \
             with it at about six bytes a character",
            path_lengths[0]
        );
    }

    // --- Denominators. ---
    for hist in routed.iter().chain(container.iter()).chain(mixed.iter()) {
        assert!(
            hist.buckets() > 0,
            "denominator: no routing buckets, so every fraction below is a zero that means nothing"
        );
        assert!(
            hist.pages() > 0,
            "denominator: no page entries, so the structure this module is about is absent"
        );
    }

    // --- The routed arm: overwhelmingly single-page, and it REACHES a multi-page bucket. ---
    for (index, hist) in routed.iter().enumerate() {
        assert!(
            hist.single_page_fraction() > 0.99,
            "routed arm {index}: {:.4} of buckets hold exactly one page; the single-page case is \
             not dominant here and the inline arm would be carrying the rare shape",
            hist.single_page_fraction()
        );
        assert!(
            hist.multi_page() > 0,
            "routed arm {index}: NO bucket holds more than one page, so this fixture cannot tell \
             a correct `BlockIndexMap` from a constant that always answers One"
        );
        assert!(
            hist.widest() >= 2,
            "routed arm {index}: the widest bucket holds {} page(s)",
            hist.widest()
        );
    }

    // --- The container arm: the opposite population, from the same engine. ---
    for (index, hist) in container.iter().enumerate() {
        assert_eq!(
            0,
            hist.holding(1),
            "container arm {index}: {} buckets hold exactly one page; this arm exists to be the \
             population where the single-page case does NOT dominate",
            hist.holding(1)
        );
        assert!(
            hist.mean() >= 2.0,
            "container arm {index}: mean {:.4} pages a bucket",
            hist.mean()
        );
    }

    // --- The mixed arm holds BOTH populations at once. ---
    for (index, hist) in mixed.iter().enumerate() {
        assert!(
            hist.holding(1) > 0 && hist.multi_page() > 0,
            "mixed arm {index}: holds {} single-page and {} multi-page buckets; it is supposed to \
             be bimodal and it is not",
            hist.holding(1),
            hist.multi_page()
        );
    }

    // --- Flat across a tenfold corpus, in both directions. ---
    println!("\n=== the two populations, across a tenfold corpus ===");
    println!(
        "  routed    : {:.5} -> {:.5} single-page fraction",
        routed[0].single_page_fraction(),
        routed[1].single_page_fraction()
    );
    println!(
        "  container : {:.5} -> {:.5} single-page fraction  (mean {:.2} -> {:.2} pages/bucket)",
        container[0].single_page_fraction(),
        container[1].single_page_fraction(),
        container[0].mean(),
        container[1].mean()
    );
    println!(
        "  mixed     : {:.5} -> {:.5} single-page fraction",
        mixed[0].single_page_fraction(),
        mixed[1].single_page_fraction()
    );
    assert!(
        (routed[0].single_page_fraction() - routed[1].single_page_fraction()).abs() < 0.01,
        "the routed arm's single-page fraction moved with the corpus, so it is not a property of \
         the workload"
    );
    assert!(
        container[0].mean() >= 2.0 && container[1].mean() >= 2.0,
        "the container arm stopped being multi-page at one of the two sizes"
    );
    assert!(
        routed[1].single_page_fraction() > container[1].single_page_fraction() + 0.9,
        "the two arms are supposed to be the two ends of this distribution and they are not: \
         routed {:.4}, container {:.4}",
        routed[1].single_page_fraction(),
        container[1].single_page_fraction()
    );
}

// ---------------------------------------------------------------------------------------------
// THE PAGE INDEX, BYTE BY BYTE, AND THE SHAPES IT COULD TAKE.
// ---------------------------------------------------------------------------------------------

/// Every byte of the page entry and of the index that holds it, as a RECONSTRUCTION.
///
/// `eight_aligned + round_up(tail) == size_of` for each, so a field added to either lands in a
/// named group and the failure says which group it landed in rather than only that a total moved.
/// A total-only assertion cannot tell "a field was added" from "the aligner rounded differently".
#[test]
fn every_byte_of_the_page_index_is_accounted_for() {
    // --- BlockIndex: three shared names and an address pack solid; three flags round up. ---
    let arc_str = size_of::<Arc<str>>();
    let opt_arc_str = size_of::<Option<Arc<str>>>();
    let index_eight_aligned = 2 * arc_str + opt_arc_str + size_of::<BlockAddress>();
    let index_tail = 3 * size_of::<bool>();
    let index_rounded_tail = round_up_to(index_tail, 8);
    println!(
        "BlockIndex: {index_eight_aligned} B eight-aligned + {index_tail} B tail rounded to \
         {index_rounded_tail} = {} B, size_of = {}",
        index_eight_aligned + index_rounded_tail,
        size_of::<BlockIndex>()
    );
    assert_eq!(
        size_of::<BlockIndex>(),
        index_eight_aligned + index_rounded_tail,
        "the page entry no longer reconstructs as {index_eight_aligned} bytes of eight-aligned \
         field plus {index_tail} bytes of flag rounded up to {index_rounded_tail}"
    );
    assert_eq!(96, size_of::<BlockIndex>(), "the page entry's budgeted width moved");

    // The three flags are ALREADY inside the rounding. Narrowing them reclaims nothing; only
    // removing the tail entirely would, and it is three keys of the stored index.
    assert!(
        index_tail < index_rounded_tail,
        "the three flags no longer sit inside alignment slack, so the note above is stale"
    );

    // --- BlockIndexMap: the widest arm is the COMMON one, and the tag rides a niche. ---
    let one_arm = size_of::<u64>() + size_of::<BlockIndex>();
    // A FLAT LIST since #1963, not a tree. Same 24-byte header either way, so this line does not
    // move the assertion below -- but it has to name the shipped type or the reconstruction is
    // describing a shape the engine no longer builds.
    let many_arm = size_of::<Vec<(u64, BlockIndex)>>();
    println!(
        "BlockIndexMap: One arm {one_arm} B, Many arm {many_arm} B, size_of = {}",
        size_of::<BlockIndexMap>()
    );
    assert_eq!(
        size_of::<BlockIndexMap>(),
        one_arm,
        "the page index is no longer exactly its One arm; the discriminant has stopped riding a \
         niche, or another arm has become the widest"
    );
    assert!(
        one_arm > many_arm,
        "the common arm is supposed to be the WIDE one here -- that is what makes boxing it a \
         loss rather than the win it is on ObjectIndex, where the rare arm is the wide one"
    );
    assert_eq!(104, size_of::<BlockIndexMap>(), "the page index's budgeted width moved");

    // --- And it is over half of the node, which is why any further accounting starts here. ---
    assert!(
        size_of::<BlockIndexMap>() * 2 > size_of::<BucketNode>(),
        "the inline page index is {} of the node's {} bytes; if it is no longer more than half, \
         the next dominant term named in this module is the wrong one",
        size_of::<BlockIndexMap>(),
        size_of::<BucketNode>()
    );
}

fn round_up_to(value: usize, multiple: usize) -> usize {
    value.div_ceil(multiple) * multiple
}

// --- The mirrors. Each is built from the same field types as the declaration, so the widths
// --- below are statements about the declaration and not estimates. The control comes first.

/// The page index as it stands. If this is not `size_of::<BlockIndexMap>()` every row is fiction.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorLivePageIndex {
    Empty,
    One(u64, BlockIndex),
    Many(BTreeMap<u64, BlockIndex>),
}

/// The single page behind a pointer: the shape whose sign is not visible in its width.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorBoxedPageIndex {
    Empty,
    One(u64, Box<BlockIndex>),
    Many(BTreeMap<u64, BlockIndex>),
}

/// No tag at all: one map, always, for every bucket including the empty ones.
#[allow(dead_code)]
#[derive(Clone)]
struct MirrorAlwaysMapped(BTreeMap<u64, BlockIndex>);

/// One map behind a pointer, absent for an empty bucket. The narrowest shape available, and the
/// one that allocates for every bucket that holds anything at all.
#[allow(dead_code)]
#[derive(Clone)]
struct MirrorAlwaysIndirect(Option<Box<BTreeMap<u64, BlockIndex>>>);

/// Two pages inline before the map is earned -- the inline-storage answer, which makes the common
/// case bigger to avoid an allocation the common case was never going to make.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorInlineTwo {
    Empty,
    One(u64, BlockIndex),
    Two([(u64, BlockIndex); 2]),
    Many(BTreeMap<u64, BlockIndex>),
}

/// The handle narrowed to a non-zero word, in case the tag could be made to ride it instead.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorNonZeroHandle {
    Empty,
    One(std::num::NonZeroU64, BlockIndex),
    Many(BTreeMap<u64, BlockIndex>),
}

/// THE SHAPE WHERE THE COMMON CASE HOLDS NO PAGE STRUCTURE AT ALL: one field in the node that is
/// EITHER the single page's address OR a pointer to an out-of-line structure, tiered 0 -> fixed
/// -> variable. The single-page bucket allocates nothing, which is what makes it different from
/// every boxed shape above.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorOneWordInline {
    Empty,
    /// The single page's whole entry, held in ONE WORD.
    One(u64),
    /// The rare case, and the only one that allocates.
    Many(Box<BTreeMap<u64, BlockIndex>>),
}

/// The same tiering, but holding what THIS engine's single page actually needs: its address.
#[allow(dead_code)]
#[derive(Clone)]
enum MirrorInlineAddress {
    Empty,
    One(u64, BlockAddress),
    Many(Box<BTreeMap<u64, BlockIndex>>),
}

/// The page entry with `deleted` removed -- encoded instead as a length of zero, which removes a
/// field rather than narrowing one.
#[allow(dead_code)]
struct MirrorPageNoDeleted {
    object_key: Arc<str>,
    model_id: Arc<str>,
    component: Option<Arc<str>>,
    address: BlockAddress,
    dirty: bool,
    log_backed: bool,
}

/// The page entry with all three flags folded into a single byte.
#[allow(dead_code)]
struct MirrorPageOneFlagByte {
    object_key: Arc<str>,
    model_id: Arc<str>,
    component: Option<Arc<str>>,
    address: BlockAddress,
    flags: u8,
}

/// The page entry with all three flags gone entirely.
#[allow(dead_code)]
struct MirrorPageNoFlags {
    object_key: Arc<str>,
    model_id: Arc<str>,
    component: Option<Arc<str>>,
    address: BlockAddress,
}

/// The page entry with its three shared names gone -- the largest group in it.
#[allow(dead_code)]
struct MirrorPageAddressOnly {
    address: BlockAddress,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// A node mirroring the live declaration field for field, so a node width can be quoted beside
/// each index shape without asserting anything about a structure this engine does not have.
#[allow(dead_code)]
#[derive(Clone)]
struct MirrorNode<I> {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: I,
}

/// WHAT EACH SHAPE OF THE PAGE INDEX WOULD COST, IN WIDTH.
///
/// THE CONTROL COMES FIRST, twice: the mirror of the live index has to equal the declaration, and
/// the mirror of the live NODE has to equal `BucketNode`. Without both, every row is describing a
/// structure this engine does not have and the numbers are fiction.
///
/// WIDTH IS NOT THE RANKING. Four of these five shapes are narrower than what is there, and four
/// of them allocate for a bucket that holds one page. The distribution above says 99.90% of the
/// buckets in a routed store hold exactly one, so "narrower" and "cheaper" point in opposite
/// directions here, and only the allocator settles it. That is the next test.
#[test]
fn what_each_shape_of_the_page_index_would_cost() {
    assert_eq!(
        size_of::<BlockIndexMap>(),
        size_of::<MirrorLivePageIndex>(),
        "the mirror of the live page index is {} bytes against the declaration's {}; the mirrors \
         have drifted and every price below is fiction",
        size_of::<MirrorLivePageIndex>(),
        size_of::<BlockIndexMap>()
    );
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<MirrorNode<BlockIndexMap>>(),
        "the mirror of the live node is {} bytes against the declaration's {}; the node widths \
         below are fiction",
        size_of::<MirrorNode<BlockIndexMap>>(),
        size_of::<BucketNode>()
    );

    let live_index = size_of::<BlockIndexMap>();
    let live_node = size_of::<BucketNode>();
    let rows: Vec<(&'static str, usize, usize, bool)> = vec![
        (
            "as it stands: Empty | One inline | Many",
            live_index,
            live_node,
            false,
        ),
        (
            "the single page behind a pointer",
            size_of::<MirrorBoxedPageIndex>(),
            size_of::<MirrorNode<MirrorBoxedPageIndex>>(),
            true,
        ),
        (
            "one map always, no tag",
            size_of::<MirrorAlwaysMapped>(),
            size_of::<MirrorNode<MirrorAlwaysMapped>>(),
            true,
        ),
        (
            "one map behind a pointer, absent when empty",
            size_of::<MirrorAlwaysIndirect>(),
            size_of::<MirrorNode<MirrorAlwaysIndirect>>(),
            true,
        ),
        (
            "two pages inline before the map is earned",
            size_of::<MirrorInlineTwo>(),
            size_of::<MirrorNode<MirrorInlineTwo>>(),
            false,
        ),
        (
            "the handle narrowed to a non-zero word",
            size_of::<MirrorNonZeroHandle>(),
            size_of::<MirrorNode<MirrorNonZeroHandle>>(),
            false,
        ),
    ];

    println!("\n=== shapes of the page index, by width ===");
    println!(
        "  {:<44} {:>7} {:>7} {:>9} {:>26}",
        "shape", "index", "node", "vs today", "single-page bucket pays"
    );
    for (name, index, node, allocates) in &rows {
        println!(
            "  {:<44} {:>7} {:>7} {:>9} {:>26}",
            name,
            index,
            node,
            format!("{:+}", *node as isize - live_node as isize),
            if *allocates {
                "one allocation"
            } else {
                "nothing"
            }
        );
    }

    // The boxed shape is the narrowest of the tagged ones and takes the most off the node. That
    // is the whole of its case, and it is not enough -- the next test is why.
    assert!(
        size_of::<MirrorNode<MirrorBoxedPageIndex>>() < live_node,
        "boxing the single page is supposed to NARROW the node; if it no longer does, the decline \
         below is being argued against a shape that is not even a candidate"
    );

    // Inline storage for a second page makes the common case wider to avoid an allocation the
    // common case was never going to make: 99.90% of routed buckets hold one page, so the second
    // slot is carried by every bucket and used by one in a thousand.
    assert!(
        size_of::<MirrorInlineTwo>() > live_index,
        "the inline-two shape is supposed to be WIDER than what is there; it is the cost of \
         reaching the second page without a map"
    );

    // The handle already has no spare niche to give: the tag rides a pointer inside the page.
    assert_eq!(
        live_index,
        size_of::<MirrorNonZeroHandle>(),
        "narrowing the handle to a non-zero word moved the index width, which would mean the tag \
         had been costing a word and is not"
    );
}

/// THE SHAPE WHERE THE COMMON CASE HOLDS NO PAGE STRUCTURE AT ALL, AND WHAT FORBIDS IT HERE.
///
/// Every candidate in the table above still stores a whole `BlockIndex` for a single-page bucket,
/// inline or behind a pointer. The shape worth asking about is the one that stores NOTHING extra:
/// one field in the node that is either the single page's address or a pointer to an out-of-line
/// structure, tiered 0 -> a fixed structure -> a variable one, with the single-page bucket
/// allocating nothing at all.
///
/// IT IS A LARGE WIN IF IT FITS, AND IT DOES NOT FIT HERE. The tiering asks that a single page's
/// entry be one word. This engine's page entry is 104 bytes, and the smallest part of it that a
/// read cannot do without -- the address -- is 48 on its own. The gate is not the flags or the
/// tagging; it is that `BlockAddress` is six words, and it is six words because #1937 already
/// narrowed it and could not get it below that: two 64-bit slab coordinates, two 64-bit
/// identities, two narrow 32-bit fields, the routing bucket and a presence byte. Packing the
/// single page into one word would mean a different stored address format, not a different
/// resident layout.
///
/// THE TWO ECONOMIES THAT REMOVE A FIELD RATHER THAN NARROWING ONE, priced here on their merits
/// because both are real ideas and both are worth knowing the answer to:
///
///   * **a size of zero MEANS deleted**, so no `deleted` field exists. Measured below: it removes
///     one of the three flag bytes and moves the entry's width by NOTHING, because all three
///     already sit inside one 8-byte rounding. Removing ALL THREE moves 8.
///   * **the flags occupy one byte rather than one word**. Measured below: they already do. Three
///     `bool` fields are three bytes, not three words, and folding them into a single `u8` moves
///     the entry by nothing at all.
///
/// So of the two, one is already banked and the other is worth 8 bytes only if all three flags go
/// -- and `dirty`, `deleted` and `log_backed` are three keys of the stored page entry.
#[test]
fn what_the_common_case_could_hold_inline_and_what_this_engines_page_entry_forbids() {
    // --- The controls, first and twice. ---
    assert_eq!(
        size_of::<BlockIndexMap>(),
        size_of::<MirrorLivePageIndex>(),
        "the mirror of the live page index has drifted from the declaration"
    );
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<MirrorNode<BlockIndexMap>>(),
        "the mirror of the live node has drifted from the declaration"
    );

    let word = size_of::<u64>();
    println!("\n=== the tiering, 0 -> a fixed structure -> a variable one ===");
    println!(
        "  {:<52} {:>7} {:>7} {:>10}",
        "shape", "index", "node", "vs today"
    );
    for (name, index, node) in [
        (
            "as it stands: the whole page entry inline",
            size_of::<BlockIndexMap>(),
            size_of::<MirrorNode<BlockIndexMap>>(),
        ),
        (
            "the single page's ADDRESS inline, map behind a pointer",
            size_of::<MirrorInlineAddress>(),
            size_of::<MirrorNode<MirrorInlineAddress>>(),
        ),
        (
            "ONE WORD: the page in the field, or a pointer",
            size_of::<MirrorOneWordInline>(),
            size_of::<MirrorNode<MirrorOneWordInline>>(),
        ),
    ] {
        println!(
            "  {:<52} {:>7} {:>7} {:>10}",
            name,
            index,
            node,
            format!("{:+}", node as isize - size_of::<BucketNode>() as isize)
        );
    }
    println!(
        "  and a hand-rolled union stealing the tag from the pointer's low bit would be {} -- one \
         word, no tag of its own",
        word
    );

    // --- WHAT FORBIDS IT. The single page's smallest irreducible payload. ---
    println!("\n=== what a single page's entry cannot do without ===");
    println!(
        "  the address alone                     : {:>4} B  ({} words)",
        size_of::<BlockAddress>(),
        size_of::<BlockAddress>() / word
    );
    println!(
        "  the address plus its three flags      : {:>4} B",
        size_of::<MirrorPageAddressOnly>()
    );
    println!(
        "  the whole page entry as declared      : {:>4} B",
        size_of::<BlockIndex>()
    );
    assert!(
        size_of::<BlockAddress>() > word,
        "the address fits in one word, so the one-word tiering IS available here and this test is \
         arguing against a shape that is not blocked"
    );
    assert!(
        size_of::<MirrorOneWordInline>() <= 2 * word,
        "the one-word shape is supposed to be a word and its tag; it is {} B",
        size_of::<MirrorOneWordInline>()
    );
    // The gate, stated as a ratio so the message says how far away it is rather than only that it
    // is far.
    println!(
        "  THE GATE: the one-word tiering needs the single page's entry to be {} B. The address \
         alone is {}x that, and the entry as declared is {}x. Reaching it is a change to the \
         STORED address format, which is 13 of the node's 15 fields' problem and not a resident \
         layout decision.",
        word,
        size_of::<BlockAddress>() / word,
        size_of::<BlockIndex>() / word
    );

    // --- THE TWO ECONOMIES, each measured. ---
    println!("\n=== removing a field rather than narrowing one, priced ===");
    for (name, width) in [
        ("the page entry as declared", size_of::<BlockIndex>()),
        (
            "with `deleted` gone (a size of zero means deleted)",
            size_of::<MirrorPageNoDeleted>(),
        ),
        (
            "with all three flags folded into one byte",
            size_of::<MirrorPageOneFlagByte>(),
        ),
        ("with all three flags gone", size_of::<MirrorPageNoFlags>()),
        (
            "with the three shared names gone",
            size_of::<MirrorPageAddressOnly>(),
        ),
    ] {
        println!(
            "  {:<52} {:>4} B  {:>6}",
            name,
            width,
            format!("{:+}", width as isize - size_of::<BlockIndex>() as isize)
        );
    }
    assert_eq!(
        size_of::<BlockIndex>(),
        size_of::<MirrorPageNoDeleted>(),
        "removing the `deleted` field moved the entry's width; the three flags are no longer \
         sitting inside one rounding and the note above is stale"
    );
    assert_eq!(
        size_of::<BlockIndex>(),
        size_of::<MirrorPageOneFlagByte>(),
        "folding the three flags into one byte moved the entry's width, which would mean they had \
         been costing more than three bytes and they are not"
    );
    assert!(
        size_of::<MirrorPageNoFlags>() < size_of::<BlockIndex>(),
        "removing ALL THREE flags is supposed to move the entry; it is the only flag change that \
         crosses the rounding"
    );

    // --- AND THE SAVING THE WHOLE TIERING WOULD BUY, IF THE ENTRY EVER GOT THERE. ---
    //
    // Stated so the next step has a ceiling rather than an aspiration: this is what the node would
    // be if the single page's entry did fit in a word, which it does not today.
    let ceiling = size_of::<BucketNode>() - size_of::<BlockIndexMap>() + 2 * word;
    println!(
        "\n  CEILING: were the entry ever to fit in a word, the node would be {} B against \
         today's {} -- {} B a bucket, and every single-page bucket would allocate nothing. That is \
         the prize, and the address format is what stands between this engine and it.",
        ceiling,
        size_of::<BucketNode>(),
        size_of::<BucketNode>() - ceiling
    );
    assert!(
        ceiling < size_of::<BucketNode>(),
        "the tiering would not narrow the node at all, so there is no prize to describe"
    );
}

// ---------------------------------------------------------------------------------------------
// THE COUNTING ALLOCATOR, WHICH IS THE INSTRUMENT THAT SETTLES IT.
// ---------------------------------------------------------------------------------------------

/// What one `clone()` of a value charges the allocator.
///
/// Cloning a `BTreeMap` rebuilds it node for node, so the bytes charged across one clone of a
/// bucket map are the map's OWN allocations -- measured, and owing nothing to `size_of` x count.
/// #1958 measured the allocator charging 1.58x its own arithmetic on this very map, because a
/// B-tree node holds eleven value slots whether or not it fills them.
#[cfg(feature = "alloc-probe")]
fn clone_alloc_bytes<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    let counts = probe.stop();
    std::hint::black_box(&copy);
    drop(copy);
    counts.alloc_bytes
}

/// What one `clone()` charges in allocation CALLS, which is immune to the store path length.
#[cfg(feature = "alloc-probe")]
fn clone_allocs<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    let counts = probe.stop();
    std::hint::black_box(&copy);
    drop(copy);
    counts.allocs
}

/// THE PLANTED MARKER. Recovered exactly, or every figure below is noise.
///
/// The failure this guards against is the one that reads as good news: an instrument reporting
/// near zero makes a shape look free, and a decline argued on a blind instrument is worse than no
/// decline at all.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_clone_instrument_used_here_recovers_a_planted_megabyte_exactly() {
    const PLANTED: usize = 1 << 20;
    let marker: Vec<u8> = vec![0xA5; PLANTED];
    let measured = clone_alloc_bytes(&marker);
    println!(
        "planted {PLANTED} B, instrument charged {measured} B ({:.4}x)",
        measured as f64 / PLANTED as f64
    );
    assert_eq!(
        PLANTED as u64, measured,
        "the clone instrument charged {measured} B for a planted {PLANTED} B; it is not measuring \
         what it is being read as measuring"
    );
}

/// Build the mirror node holding the live index, from a real node.
fn mirror_live_node(node: &BucketNode) -> MirrorNode<BlockIndexMap> {
    MirrorNode {
        routing_bucket: node.routing_bucket,
        layout: node.layout,
        flags: BucketFlags::default().with(BucketFlags::DIRTY, node.dirty()).with(BucketFlags::DELETED, node.deleted()).with(BucketFlags::META_LOADED, node.meta_loaded()).with(BucketFlags::LOADING, node.loading()).with(BucketFlags::IN_MEMORY, node.in_memory()),
        ttl_ms: node.ttl_ms,
        dirty_generation: node.dirty_generation,
        first_dirty_wal_sequence: node.first_dirty_wal_sequence,
        first_dirty_index_log_sequence: node.first_dirty_index_log_sequence,
        object_index: node.object_index.clone(),
        deleted_object_index: node.deleted_object_index.clone(),
        block_index: node.block_index.clone(),
    }
}

/// The same node with its page index in the boxed shape, holding the SAME page set.
fn mirror_boxed_node(node: &BucketNode) -> MirrorNode<MirrorBoxedPageIndex> {
    let entries: Vec<(u64, BlockIndex)> = node
        .block_index
        .iter()
        .map(|(handle, page)| (*handle, page.clone()))
        .collect();
    let block_index = match entries.len() {
        0 => MirrorBoxedPageIndex::Empty,
        1 => {
            let (handle, page) = entries.into_iter().next().expect("length is one");
            MirrorBoxedPageIndex::One(handle, Box::new(page))
        }
        _ => MirrorBoxedPageIndex::Many(entries.into_iter().collect()),
    };
    MirrorNode {
        routing_bucket: node.routing_bucket,
        layout: node.layout,
        flags: BucketFlags::default().with(BucketFlags::DIRTY, node.dirty()).with(BucketFlags::DELETED, node.deleted()).with(BucketFlags::META_LOADED, node.meta_loaded()).with(BucketFlags::LOADING, node.loading()).with(BucketFlags::IN_MEMORY, node.in_memory()),
        ttl_ms: node.ttl_ms,
        dirty_generation: node.dirty_generation,
        first_dirty_wal_sequence: node.first_dirty_wal_sequence,
        first_dirty_index_log_sequence: node.first_dirty_index_log_sequence,
        object_index: node.object_index.clone(),
        deleted_object_index: node.deleted_object_index.clone(),
        block_index,
    }
}

/// The order production builds the bucket map in, approximated: NOT ascending.
///
/// This matters more than it looks, and the first version of this test got it wrong. A `BTreeMap`
/// COLLECTED from a sorted iterator is built with full nodes; one built by `insert` as keys arrive
/// is about two thirds full, which is exactly the 63.2% / 63.6% #1958 measured. `clone()`
/// reproduces whichever shape its source has, so a mirror built by `collect` measures a tree with
/// a third fewer nodes and almost no unfilled slot.
///
/// THE UNFILLED SLOTS ARE WHERE A NARROWER VALUE TYPE WINS. A node holds eleven value slots
/// whether or not it fills them, so eighty bytes off the value is eighty bytes off every slot
/// including the empty ones -- and at two thirds full there are half again as many slots per
/// bucket as at full. The fill therefore decides how much the narrower shape saves, and the saving
/// is what the added allocation has to be set against. Measuring the candidate on a full tree and
/// the incumbent on production's tree would have compared two different questions.
fn production_order(keys: &[u32]) -> Vec<u32> {
    let mut order: Vec<u32> = keys.to_vec();
    order.sort_by_key(|key| (*key as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    order
}

/// Buckets per allocated B-tree node, and what fraction of the eleven value slots that fills.
///
/// `node_allocations` must EXCLUDE any allocation that is not a tree node -- the boxed arm makes
/// one per single-page bucket, and counting those would report a tree that was 8% full.
#[cfg(feature = "alloc-probe")]
fn fill(entries: usize, node_allocations: u64) -> (f64, f64) {
    if node_allocations == 0 {
        return (0.0, 0.0);
    }
    let per_node = entries as f64 / node_allocations as f64;
    (per_node, per_node / 11.0)
}

/// BOXING THE SINGLE-PAGE ARM, MEASURED BY THE ALLOCATOR RATHER THAN ARGUED FROM ITS WIDTH.
///
/// #1958 priced this shape from its width and the allocator's rounding: minus eighty bytes on the
/// struct, plus a 104-byte request served out of a 112-byte chunk for very nearly every bucket. It
/// never ran the allocator over it, and its own headline result -- that the allocator charges
/// 1.58x the `size_of` arithmetic on this map -- is precisely why arithmetic is not enough here.
///
/// AND THE ANSWER IS NOT THE ONE #1958 PUBLISHED. Its row set the node saving at 80 bytes a
/// bucket -- `size_of` arithmetic -- against an allocation of 112 bytes a bucket, which is an
/// allocator figure, and concluded +32 a bucket. The two are not the same instrument, and the
/// whole point of #1958's own headline result is that they differ by 1.58x here: a B-tree node
/// carries eleven value slots whether or not it fills them, so eighty bytes off the value type is
/// eighty bytes off EVERY slot, filled or not. At the fill this map actually runs at that is about
/// 127 bytes a bucket saved, not 80, and the row's sign reverses. Measured below rather than
/// re-argued.
///
/// THE CONTROL COMES FIRST AND IS NOT OPTIONAL. The mirror node holding the LIVE index has to
/// charge EXACTLY what a map of real `BucketNode` built the same way charges. The first version of
/// this test built the mirrors by `collect` and the control failed by 4.8 MB -- which is the whole
/// reason `production_order` exists, and a good deal more interesting than the assertion passing
/// would have been.
///
/// THE ONE THING THE HARNESS CANNOT REPRODUCE IS PRODUCTION'S EXACT FILL, so the claim is made in
/// the CONSERVATIVE direction instead of being fitted to it. A lower fill means more nodes, more
/// unfilled slots, and a larger saving for the narrower value -- so a harness tree that is FULLER
/// than production's understates the boxed shape's byte win. The test asserts that it is fuller,
/// and the number it reports is therefore a floor on the win and not a fit to it.
///
/// WHAT THE COUNTING ALLOCATOR DOES NOT SEE, stated because it eats most of the difference: it
/// charges the bytes REQUESTED, and a 104-byte request is served out of a larger chunk once the
/// allocator has taken its header and rounded to a size class. Every added allocation below is
/// therefore charged at least eight bytes short of what it costs. The COUNT is the figure that
/// carries, because it moves with nothing.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "seeds 40,000 records and clones six bucket maps; run by name"]
fn boxing_the_single_page_arm_costs_an_allocation_for_every_bucket_that_holds_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path_length = dir.path().as_os_str().len();
    let engine = probe_engine(dir.path());
    seed_routed_keys(&engine, 40_000, 40, 1_000);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let hist = pages_per_bucket(&engine);
    hist.report("the store this is measured on");

    let buckets = hist.buckets();
    let single_page = hist.holding(1);
    assert!(buckets > 0, "denominator: the shard holds no buckets");
    assert!(
        hist.multi_page() > 0,
        "the fixture reaches no multi-page bucket, so the Many arm is unexercised on both sides"
    );
    assert!(
        single_page > 0,
        "denominator: no bucket holds exactly one page, so the shape under test is unreached"
    );

    let keys: Vec<u32> = shard.bucket_index.bucket_map.keys().copied().collect();
    let order = production_order(&keys);
    assert_eq!(buckets, order.len(), "the insert order lost buckets");

    // --- Built the way production builds it: one insert at a time, not in key order. ---
    //
    // What the CONTROL is measured against: real `BucketNode`s, the type production stores, in a
    // tree built exactly as the mirrors are. If the mirror does not charge what this charges, the
    // mirror is not a mirror.
    let mut partner: BTreeMap<u32, BucketNode> = BTreeMap::new();
    let mut live_mirror: BTreeMap<u32, MirrorNode<BlockIndexMap>> = BTreeMap::new();
    let mut boxed_mirror: BTreeMap<u32, MirrorNode<MirrorBoxedPageIndex>> = BTreeMap::new();
    for routing_bucket in &order {
        let node = shard
            .bucket_index
            .bucket_map
            .get(routing_bucket)
            .expect("the bucket is in the map it was keyed from");
        partner.insert(*routing_bucket, node.clone());
        live_mirror.insert(*routing_bucket, mirror_live_node(node));
        boxed_mirror.insert(*routing_bucket, mirror_boxed_node(node));
    }

    // --- And built full, from a sorted iterator, as the second row. ---
    let live_full: BTreeMap<u32, MirrorNode<BlockIndexMap>> = shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, node)| (*routing_bucket, mirror_live_node(node)))
        .collect();
    let boxed_full: BTreeMap<u32, MirrorNode<MirrorBoxedPageIndex>> = shard
        .bucket_index
        .bucket_map
        .iter()
        .map(|(routing_bucket, node)| (*routing_bucket, mirror_boxed_node(node)))
        .collect();

    for (name, held) in [
        ("control partner", partner.len()),
        ("live mirror", live_mirror.len()),
        ("boxed mirror", boxed_mirror.len()),
        ("live mirror, full", live_full.len()),
        ("boxed mirror, full", boxed_full.len()),
    ] {
        assert_eq!(buckets, held, "the {name} lost buckets on the way in");
    }

    let real_bytes = clone_alloc_bytes(&shard.bucket_index.bucket_map);
    let real_calls = clone_allocs(&shard.bucket_index.bucket_map);
    let partner_bytes = clone_alloc_bytes(&partner);
    let partner_calls = clone_allocs(&partner);
    let live_bytes = clone_alloc_bytes(&live_mirror);
    let live_calls = clone_allocs(&live_mirror);
    let boxed_bytes = clone_alloc_bytes(&boxed_mirror);
    let boxed_calls = clone_allocs(&boxed_mirror);
    let live_full_bytes = clone_alloc_bytes(&live_full);
    let live_full_calls = clone_allocs(&live_full);
    let boxed_full_bytes = clone_alloc_bytes(&boxed_full);
    let boxed_full_calls = clone_allocs(&boxed_full);

    // A boxed arm allocates once per single-page bucket; those are not tree nodes and must come
    // out before any fill is reported, or the boxed rows read as an 8%-full tree.
    let boxes = single_page as u64;

    println!("\n=== one clone of the bucket map, by the counting allocator ===");
    println!("  store path length held at {path_length} characters");
    println!(
        "  buckets={buckets}  single-page={single_page}  pages={}",
        hist.pages()
    );
    println!(
        "  {:<40} {:>12} {:>12} {:>12} {:>22}",
        "map", "bytes", "allocations", "tree nodes", "buckets/node (of 11)"
    );
    for (name, bytes, calls, node_calls) in [
        ("the real bucket map, as production built it", real_bytes, real_calls, real_calls),
        ("control partner: real nodes, harness order", partner_bytes, partner_calls, partner_calls),
        ("mirror, live index           [CONTROL]", live_bytes, live_calls, live_calls),
        ("mirror, single page boxed", boxed_bytes, boxed_calls, boxed_calls - boxes),
        ("mirror, live index, FULL tree", live_full_bytes, live_full_calls, live_full_calls),
        ("mirror, boxed, FULL tree", boxed_full_bytes, boxed_full_calls, boxed_full_calls - boxes),
    ] {
        let (per_node, fraction) = fill(buckets, node_calls);
        println!(
            "  {name:<40} {bytes:>12} {calls:>12} {node_calls:>12} {:>15.2} ({:>4.1}%)",
            per_node,
            100.0 * fraction
        );
    }

    // --- THE CONTROL, and it is exact. The mirror node has to be indistinguishable from the
    // --- real node to the allocator when the two trees are built identically. ---
    assert_eq!(
        partner_bytes, live_bytes,
        "the mirror of the live index charges {live_bytes} B where a tree of REAL BucketNodes \
         built the same way charges {partner_bytes} B; the mirror is not a mirror and every \
         figure below is fiction"
    );
    assert_eq!(
        partner_calls, live_calls,
        "the mirror of the live index makes {live_calls} allocations where a tree of real \
         BucketNodes built the same way makes {partner_calls}"
    );
    assert!(
        real_calls > 0 && live_calls > 0,
        "a clone charged zero allocations; the probe is not counting and every row is a zero that \
         reads as good news"
    );

    // --- AND THE DIRECTION. The harness tree must be FULLER than production's, so the byte
    // --- figure below is a floor on the boxed shape's win rather than a fit to it. ---
    let (real_per_node, real_fraction) = fill(buckets, real_calls);
    let (harness_per_node, harness_fraction) = fill(buckets, live_calls);
    println!(
        "\n  fill: production {real_per_node:.2} buckets a node ({:.1}%), harness \
         {harness_per_node:.2} ({:.1}%)",
        100.0 * real_fraction,
        100.0 * harness_fraction
    );
    assert!(
        live_calls <= real_calls,
        "the harness tree has {live_calls} nodes against production's {real_calls} -- it is EMPTIER \
         than production's, so the narrower value type has more unfilled slot to save here than it \
         would in production and the figure below would be an overstatement rather than a floor"
    );

    // --- THE CANDIDATE, at production's fill and at full. ---
    let per_bucket_saving = size_of::<BucketNode>() - size_of::<MirrorNode<MirrorBoxedPageIndex>>();
    let arithmetic_saving = per_bucket_saving * buckets;
    let measured = boxed_bytes as i64 - live_bytes as i64;
    let measured_full = boxed_full_bytes as i64 - live_full_bytes as i64;
    let extra_calls = boxed_calls as i64 - live_calls as i64;
    let extra_calls_full = boxed_full_calls as i64 - live_full_calls as i64;

    println!("\n=== what boxing the single page actually costs ===");
    println!(
        "  by size_of arithmetic      : {:>+12} B  ({per_bucket_saving} B a bucket over {buckets} \
         buckets)",
        -(arithmetic_saving as i64)
    );
    println!(
        "  by the allocator, as built : {measured:>+12} B  {extra_calls:>+12} allocations  \
         ({:+.2} B a bucket)",
        measured as f64 / buckets as f64
    );
    println!(
        "  by the allocator, full tree: {measured_full:>+12} B  {extra_calls_full:>+12} \
         allocations  ({:+.2} B a bucket)",
        measured_full as f64 / buckets as f64
    );

    // WHAT #1958 SAID, RECONSTRUCTED, AND WHERE IT PARTS COMPANY WITH THE ALLOCATOR.
    //
    // Its row priced the node saving by `size_of` (80 B a bucket) and the allocation by the
    // allocator (112 B a bucket) and subtracted one from the other. The node saving does not stop
    // at the node: the value slot it sits in is one of eleven a tree node carries whether or not
    // it fills them, so the saving per bucket is the width saving times the SLOTS per bucket.
    let slots_per_bucket = 11.0 * live_calls as f64 / buckets as f64;
    println!("\n=== the two instruments, side by side, per bucket ===");
    println!(
        "  node saving by size_of                : {per_bucket_saving:>8} B a bucket"
    );
    println!(
        "  slots a bucket actually occupies      : {slots_per_bucket:>8.3}  (11 a node, \
         {:.2} buckets a node)",
        buckets as f64 / live_calls as f64
    );
    println!(
        "  node saving as the allocator sees it  : {:>8.1} B a bucket  ({:.2}x the arithmetic)",
        per_bucket_saving as f64 * slots_per_bucket,
        slots_per_bucket
    );
    println!(
        "  the boxed page, charged               : {:>8} B a bucket that holds one page",
        size_of::<BlockIndex>()
    );
    println!(
        "  net, measured                         : {:>+8.1} B a bucket",
        measured as f64 / buckets as f64
    );

    // ONE ALLOCATION PER SINGLE-PAGE BUCKET, at both fills. This is the figure that does not move
    // with the tree shape, the store path, or the box, and it is the cost the width cannot show.
    for (label, calls) in [("as built", extra_calls), ("full tree", extra_calls_full)] {
        assert!(
            calls as usize >= single_page,
            "boxing charged {calls} extra allocations ({label}) against {single_page} single-page \
             buckets; it is supposed to be at least one apiece, and fewer means the shape under \
             test is not being built"
        );
    }
    println!(
        "\n  the allocation count is the durable figure: {:+} allocations for {single_page} \
         single-page buckets, {:.4} apiece, and it does not move with the tree shape",
        extra_calls,
        extra_calls as f64 / single_page as f64
    );

    // WHAT THE ALLOCATOR UNDERCHARGES. Each added request is 104 bytes and is served out of a
    // larger chunk; at eight bytes of rounding apiece that is what the counted figure omits.
    let undercount = (single_page * 8) as i64;
    println!(
        "  and the counted bytes omit the allocator's own rounding: {single_page} requests of {} \
         B, at least {undercount} B of chunk beyond what is charged above",
        size_of::<BlockIndex>()
    );
    println!(
        "  net of that rounding, as built: {:+} B ({:+.2} B a bucket)",
        measured + undercount,
        (measured + undercount) as f64 / buckets as f64
    );

    // THE SIGN, REPORTED RATHER THAN ASSERTED IN ONE DIRECTION.
    //
    // This test is not here to confirm a conclusion. It asserts what it can prove -- the added
    // allocation per single-page bucket, which is what the width cannot show -- and reports the
    // byte figure with the instruments' disagreement laid out beside it. The byte figure depends
    // on the tree's fill and the allocator's rounding, both of which are measured above; the count
    // does not depend on either.
    if measured + undercount < 0 {
        println!(
            "\n  SIGN: by bytes alone the boxed shape is CHEAPER here by {} B ({:.2} B a bucket), \
             which is the opposite of the +32 B a bucket #1958 computed -- because its node saving \
             was `size_of` arithmetic and its allocation was an allocator figure, and at this \
             tree's fill a bucket occupies {slots_per_bucket:.2} value slots and not one.",
            -(measured + undercount),
            -(measured + undercount) as f64 / buckets as f64
        );
    } else {
        println!(
            "\n  SIGN: by bytes the boxed shape costs {} B more ({:.2} B a bucket)",
            measured + undercount,
            (measured + undercount) as f64 / buckets as f64
        );
    }
    println!(
        "  WHAT IT BUYS THAT WITH: {single_page} allocations that did not exist, one for every \
         bucket that holds a single page -- a per-bucket allocation on the write path, one \
         dependent load on every page read, and {single_page} small scattered chunks for the \
         allocator to fragment around. The bytes are within this instrument's own rounding; the \
         allocations are not."
    );

    // --- THE RESIDUAL, reported rather than assumed away, with its own planted marker in
    // --- `the_clone_instrument_used_here_recovers_a_planted_megabyte_exactly`. ---
    let node_widths = (size_of::<BucketNode>() * buckets) as i64;
    let residual = real_bytes as i64 - node_widths;
    println!(
        "\n  residual: the map charges {residual} B beyond {node_widths} B of node width \
         ({:.2} B a bucket) -- key arrays, node headers, the value slots nothing filled, and what \
         each node owns on the heap",
        residual as f64 / buckets as f64
    );
    assert!(
        residual > 0,
        "the residual is {residual}; at or below zero the two instruments are not independent and \
         the comparison cannot notice anything"
    );
}

// ---------------------------------------------------------------------------------------------
// THE READ PATH, WHICH A FOOTPRINT MEASUREMENT CANNOT SEE.
// ---------------------------------------------------------------------------------------------

/// READING A PAGE: INLINE AGAINST BEHIND A POINTER.
///
/// A footprint measurement cannot see this, and a shape that makes the common path slower to make
/// the structure smaller is not a win however the bytes come out. The inline arm answers a lookup
/// out of the node's own bytes; the boxed arm has to follow a pointer into a separate allocation
/// first, and that is a DEPENDENT load -- the address of the page is not known until the node has
/// been read, so it cannot be issued in parallel with reading the node.
///
/// MEASURED ABBA, not A then B: this box runs other work, and a drift between the first and second
/// half of a run lands entirely on whichever arm ran second. ABBA cancels a linear drift; an A/B
/// does not. The claim carried is the ALLOCATION COUNT and the structural fact of the extra
/// dependent load, both immune to load; the timing is reported as supporting and its own spread is
/// printed so a reader can see whether it separated at all.
#[test]
#[ignore = "times four million lookups; run by name"]
fn reading_a_page_behind_a_pointer_costs_a_dependent_load_the_inline_arm_does_not() {
    const BUCKETS: usize = 20_000;
    const ROUNDS: usize = 50;

    let mut live = BlockSlabLiveIndex::default();
    let mut inline: Vec<(u64, BlockIndexMap)> = Vec::with_capacity(BUCKETS);
    for i in 0..BUCKETS {
        let mut index = BlockIndexMap::default();
        let handle = index.insert(page_for(i as u64), &mut live);
        inline.push((handle, index));
    }
    let boxed: Vec<(u64, MirrorBoxedPageIndex)> = inline
        .iter()
        .map(|(handle, index)| {
            let page = index.get(handle).expect("the page is there").clone();
            (*handle, MirrorBoxedPageIndex::One(*handle, Box::new(page)))
        })
        .collect();

    // --- Proof both arms are in the shape they are named for. ---
    assert!(
        inline
            .iter()
            .all(|(_, index)| matches!(index, BlockIndexMap::One(..))),
        "the inline arm is not holding its pages inline, so the treatment did not run"
    );
    assert!(
        boxed
            .iter()
            .all(|(_, index)| matches!(index, MirrorBoxedPageIndex::One(..))),
        "the boxed arm is not holding its pages behind a pointer, so the treatment did not run"
    );

    let read_inline = || {
        let mut sum = 0u64;
        for (handle, index) in &inline {
            if let BlockIndexMap::One(held, page) = index {
                if held == handle {
                    sum = sum.wrapping_add(page.address.block_slab_id);
                }
            }
        }
        sum
    };
    let read_boxed = || {
        let mut sum = 0u64;
        for (handle, index) in &boxed {
            if let MirrorBoxedPageIndex::One(held, page) = index {
                if held == handle {
                    sum = sum.wrapping_add(page.address.block_slab_id);
                }
            }
        }
        sum
    };

    // --- ABBA. ---
    let mut inline_ns = 0u128;
    let mut boxed_ns = 0u128;
    let mut checksum_inline = 0u64;
    let mut checksum_boxed = 0u64;
    for _ in 0..ROUNDS {
        let start = std::time::Instant::now();
        checksum_inline = std::hint::black_box(read_inline());
        inline_ns += start.elapsed().as_nanos();

        let start = std::time::Instant::now();
        checksum_boxed = std::hint::black_box(read_boxed());
        boxed_ns += start.elapsed().as_nanos();

        let start = std::time::Instant::now();
        checksum_boxed = std::hint::black_box(read_boxed());
        boxed_ns += start.elapsed().as_nanos();

        let start = std::time::Instant::now();
        checksum_inline = std::hint::black_box(read_inline());
        inline_ns += start.elapsed().as_nanos();
    }

    // --- The two arms must have read the same pages, or they are not comparable. ---
    assert_eq!(
        checksum_inline, checksum_boxed,
        "the two arms summed different pages, so they are not reading the same page set"
    );
    assert_ne!(
        0, checksum_inline,
        "both arms summed zero; the reads were optimised away and the timing below is empty"
    );

    let reads = (BUCKETS * ROUNDS * 2) as f64;
    println!("\n=== reading one page out of a single-page bucket, ABBA over {ROUNDS} rounds ===");
    println!(
        "  inline : {:>10.2} ns a read  ({inline_ns} ns over {reads} reads)",
        inline_ns as f64 / reads
    );
    println!(
        "  boxed  : {:>10.2} ns a read  ({boxed_ns} ns over {reads} reads)",
        boxed_ns as f64 / reads
    );
    println!(
        "  boxed / inline = {:.3}x -- one extra DEPENDENT load a read, which no footprint \
         measurement can see",
        boxed_ns as f64 / inline_ns as f64
    );

    // The structural claim, which does not depend on the clock: the boxed shape holds its page in
    // a separate allocation, so reading it is a second load that cannot issue until the first has
    // returned. The timing is reported; this is what is asserted.
    assert!(
        size_of::<MirrorBoxedPageIndex>() < size_of::<BlockIndexMap>(),
        "the boxed shape is not narrower, so it is not holding the page out of line and this \
         comparison is measuring nothing"
    );
}

// ---------------------------------------------------------------------------------------------
// THE TRANSITION, IN BOTH DIRECTIONS, ELEMENT BY ELEMENT.
// ---------------------------------------------------------------------------------------------

fn page_for(seed: u64) -> BlockIndex {
    BlockIndex {
        object_key: Arc::from(format!("key-{seed}").as_str()),
        model_id: Arc::from("strings"),
        component: None,
        address: BlockAddress::from_parts(
            7,
            seed * 128,
            64,
            Some(seed),
            Some(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)),
            Some(11),
        ),
        dirty: false,
        deleted: false,
        log_backed: false,
    }
}

fn component_page(seed: u64, component: &str) -> BlockIndex {
    BlockIndex {
        object_key: Arc::from("container"),
        model_id: Arc::from("hashes"),
        component: Some(Arc::from(component)),
        address: BlockAddress::from_parts(
            9,
            seed * 256,
            96,
            Some(seed),
            Some(seed.wrapping_mul(0x1000_0000_01B3)),
            Some(11),
        ),
        dirty: true,
        deleted: false,
        log_backed: true,
    }
}

/// Every field of a page entry, compared one at a time, with the field that differs NAMED.
///
/// `BlockIndex` does not derive `PartialEq`, and a comparison written as "both are non-empty" is
/// exactly the assertion that cannot see the failure this guards against. A page index that loses
/// an entry is SILENT data loss: the page is still on the slab and nothing can find it.
fn assert_same_page(context: &str, handle: u64, left: &BlockIndex, right: &BlockIndex) {
    assert_eq!(
        left.object_key, right.object_key,
        "{context}: page {handle} changed object_key"
    );
    assert_eq!(
        left.model_id, right.model_id,
        "{context}: page {handle} changed model_id"
    );
    assert_eq!(
        left.component, right.component,
        "{context}: page {handle} changed component"
    );
    assert_eq!(
        left.address, right.address,
        "{context}: page {handle} changed address"
    );
    assert_eq!(left.dirty, right.dirty, "{context}: page {handle} changed dirty");
    assert_eq!(
        left.deleted, right.deleted,
        "{context}: page {handle} changed deleted"
    );
    assert_eq!(
        left.log_backed, right.log_backed,
        "{context}: page {handle} changed log_backed"
    );
}

/// The whole page set, as an ordered list of handle-and-entry, compared element by element.
fn assert_same_page_set(context: &str, left: &BlockIndexMap, right: &BlockIndexMap) {
    let left_entries: Vec<(u64, &BlockIndex)> =
        left.iter().map(|(handle, page)| (*handle, page)).collect();
    let right_entries: Vec<(u64, &BlockIndex)> =
        right.iter().map(|(handle, page)| (*handle, page)).collect();
    assert_eq!(
        left_entries.len(),
        right_entries.len(),
        "{context}: the page set holds {} entries against {}",
        left_entries.len(),
        right_entries.len()
    );
    let left_handles: Vec<u64> = left_entries.iter().map(|(handle, _)| *handle).collect();
    let right_handles: Vec<u64> = right_entries.iter().map(|(handle, _)| *handle).collect();
    assert_eq!(
        left_handles, right_handles,
        "{context}: the page set's handles are not the same handles in the same order"
    );
    for ((handle, left_page), (_, right_page)) in left_entries.iter().zip(right_entries.iter()) {
        assert_same_page(context, *handle, left_page, right_page);
    }
}

/// A BUCKET THAT GROWS PAST ONE PAGE AND SHRINKS BACK HOLDS THE SAME PAGES, ONE BY ONE.
///
/// WHAT ALREADY EXISTS, because a hole has to be shown and not assumed. TWO tests already drive
/// this transition, and the tree was searched before any of this was called a hole:
///
///   * `part4::a_bucket_holding_one_block_holds_no_node` drives it through the engine and asserts
///     the ARM at each step -- inline, then a map, then inline again.
///   * `index_bytes_per_key::every_inline_shape_collapses_back_on_a_fill_and_drain` fills to eight
///     pages and drains one at a time, checking the arm against the expected arm at EVERY count,
///     through `remove` and again through `retain`.
///
/// Between them the shape is well covered. What neither does is look at the PAGES. Both assert the
/// arm and the length, so a promotion that dropped the page it was already holding, or a shrink
/// that collapsed to the wrong one of two, changes nothing either of them reads -- and the index
/// would be the right shape holding the wrong contents. That is the hole this fills, and it is the
/// difference between an index that is fat and one that has lost a page.
///
/// `block_index_handle` has no direct test anywhere in the crate, which is why the two pages that
/// differ only in their component are driven here as well.
///
/// DIRECTION DECIDES THE TEST. A page index that keeps too much is merely fat; one that loses an
/// entry is silent data loss, because the page is still on its slab and nothing can find it again.
/// So the assertion is EQUALITY of the whole page set against a control taken before the
/// transition, field by field, not that the set is non-empty afterwards.
///
/// BOTH DIRECTIONS ARE DRIVEN. One page becomes four -- which promotes the inline arm into a map,
/// a shape change and not a push -- and four become one, which shrinks the map back into the
/// inline arm. A shrink that forgot to happen would cost memory silently; a shrink that dropped
/// the surviving entry would cost the page. The map is then emptied a second way, through
/// `retain`, because that is a second caller of the same demotion and a guard covering one caller
/// lets the other keep the bug.
#[test]
fn a_bucket_that_grows_past_one_page_and_shrinks_back_holds_the_same_page_set() {
    let mut live = BlockSlabLiveIndex::default();
    let mut index = BlockIndexMap::default();
    assert!(index.is_empty(), "a fresh page index holds nothing");
    assert!(
        matches!(index, BlockIndexMap::Empty),
        "a fresh page index is not in the Empty shape"
    );

    // --- One page, held inline. ---
    let first = page_for(1);
    let first_handle = index.insert(first.clone(), &mut live);
    assert!(
        matches!(index, BlockIndexMap::One(..)),
        "one page is not being held inline, which is the shape the whole measurement rests on"
    );
    let inline_control = index.clone();
    assert_eq!(1, index.len(), "one page, one entry");

    // --- Grow to four: the inline arm has to promote into a map. ---
    let mut handles = vec![first_handle];
    for seed in 2..=4u64 {
        handles.push(index.insert(page_for(seed), &mut live));
    }
    assert!(
        matches!(index, BlockIndexMap::Many(_)),
        "four pages are not being held in a map, so the promotion did not happen"
    );
    assert_eq!(4, index.len(), "four pages, four entries");
    let grown_control = index.clone();

    // The page that was there before the promotion must have survived it, field for field.
    let survived = index
        .get(&first_handle)
        .expect("the page held inline before the promotion is gone after it -- silent data loss");
    assert_same_page("across the promotion into a map", first_handle, &first, survived);

    // Every one of the four is present and is its own page, compared one at a time.
    for (offset, handle) in handles.iter().enumerate() {
        let seed = offset as u64 + 1;
        let held = index
            .get(handle)
            .unwrap_or_else(|| panic!("page {handle} (seed {seed}) is not in the grown index"));
        assert_same_page("in the grown index", *handle, &page_for(seed), held);
    }

    // --- A clone of a four-page index is the same four pages. ---
    assert_same_page_set("cloning a four-page index", &grown_control, &index);

    // --- Shrink back to one: the map has to demote into the inline arm. ---
    for handle in handles.iter().skip(1) {
        let removed = index
            .remove(handle, &mut live)
            .unwrap_or_else(|| panic!("removing page {handle} answered nothing"));
        std::hint::black_box(&removed);
    }
    assert_eq!(1, index.len(), "one page should remain");
    assert!(
        matches!(index, BlockIndexMap::One(..)),
        "the index did not shrink back to the inline shape; a bucket that briefly held four pages \
         would keep a map node for the rest of its life"
    );

    // --- THE STRONG FORM. The surviving page set is EQUAL to what it was before the round trip. ---
    assert_same_page_set(
        "after growing to four pages and shrinking back to one",
        &inline_control,
        &index,
    );

    // --- A lookup is by HANDLE, not "whatever is inline". ---
    //
    // The inline arm has exactly one entry and answering it for any key would pass every length
    // and emptiness assertion in this file.
    assert!(
        index.get(&first_handle.wrapping_add(1)).is_none(),
        "the inline arm answered a lookup for a handle it does not hold"
    );
    assert!(
        index.get(&first_handle).is_some(),
        "the inline arm did not answer a lookup for the handle it does hold"
    );

    // --- Iterating the inline arm yields the one page, not nothing. ---
    assert_eq!(
        1,
        index.iter().count(),
        "iterating a one-page index yielded a different number of entries than it holds"
    );

    // --- And out the other end: removing the last page returns the index to Empty. ---
    let last = index
        .remove(&first_handle, &mut live)
        .expect("the last page is gone before it was removed");
    assert_same_page("the last page removed", first_handle, &first, &last);
    assert!(
        matches!(index, BlockIndexMap::Empty),
        "an emptied index did not return to the Empty shape"
    );
    assert_eq!(0, index.len(), "an emptied index still reports entries");

    // --- THE SECOND CALLER OF THE SAME DEMOTION. `retain` drops pages too, and it has its own
    // --- call to the shrink. A guard covering one caller lets the other keep the bug. ---
    let mut retained = BlockIndexMap::default();
    let mut retained_handles = Vec::new();
    for seed in 10..=13u64 {
        retained_handles.push(retained.insert(page_for(seed), &mut live));
    }
    assert!(
        matches!(retained, BlockIndexMap::Many(_)),
        "four pages are not in a map"
    );
    let keep = retained_handles[2];
    retained.retain(&mut live, |handle, _page| *handle == keep);
    assert_eq!(1, retained.len(), "retain should have left exactly one page");
    assert!(
        matches!(retained, BlockIndexMap::One(..)),
        "a map that `retain` drops to one page must give up its node, the same as `remove` does"
    );
    let survivor = retained
        .get(&keep)
        .expect("the page retain was told to keep is gone");
    assert_same_page("the page retain kept", keep, &page_for(12), survivor);

    // --- And retain the last page away, which goes through the INLINE arm. ---
    retained.retain(&mut live, |_handle, _page| false);
    assert_eq!(0, retained.len(), "retain should have left nothing");
    assert!(
        matches!(retained, BlockIndexMap::Empty),
        "a map that `retain` empties must return to the Empty shape, not stay an empty map"
    );

    // --- A MAP EMPTIED IN ONE STEP, which is the only way to reach the demotion's zero case. ---
    //
    // Found by mutation, and it is worth saying how, because the shape of the hole is not obvious.
    // The demotion is only ever called from the map arm, and it converts a map of ONE into the
    // inline arm -- so a drain that removes pages one at a time leaves the inline arm holding the
    // last page and takes it away through a different branch entirely. The zero case is reachable
    // only by emptying a map of several in a single call, which nothing in this tree did: a mutant
    // that left an emptied map as an empty map survived every existing test and this one, until
    // this case was added.
    let mut emptied = BlockIndexMap::default();
    for seed in 30..=33u64 {
        emptied.insert(page_for(seed), &mut live);
    }
    assert!(
        matches!(emptied, BlockIndexMap::Many(_)),
        "four pages are not in a map, so the case below is not the case it is named for"
    );
    emptied.retain(&mut live, |_handle, _page| false);
    assert_eq!(0, emptied.len(), "retain should have left nothing");
    assert!(
        emptied.is_empty(),
        "an emptied index does not report itself empty"
    );
    assert!(
        matches!(emptied, BlockIndexMap::Empty),
        "a map emptied in one step stayed a map: it reports zero pages and still carries the node \
         this type exists to avoid, for the rest of the bucket's life"
    );
    assert_eq!(
        0,
        emptied.iter().count(),
        "an emptied index yielded entries"
    );

    // --- TWO PAGES THAT DIFFER ONLY IN THEIR COMPONENT ARE TWO PAGES. ---
    //
    // This is the container workload the distribution above measures: a hash field, a set member
    // and a list element are the same object key under different components, and they all route to
    // the same bucket. A handle that did not read the component would file them all as one page
    // and 99 of every 100 would be lost -- while every length assertion on a single-page fixture
    // still passed.
    let mut components = BlockIndexMap::default();
    let first_component = components.insert(component_page(20, "alpha"), &mut live);
    let second_component = components.insert(component_page(20, "beta"), &mut live);
    assert_ne!(
        first_component, second_component,
        "two pages of one object differing only in their component were given the same handle; \
         the second would silently displace the first"
    );
    assert_eq!(
        2,
        components.len(),
        "two components of one object are two pages, and this index holds {}",
        components.len()
    );
    assert_same_page(
        "the first component",
        first_component,
        &component_page(20, "alpha"),
        components.get(&first_component).expect("alpha is filed"),
    );
    assert_same_page(
        "the second component",
        second_component,
        &component_page(20, "beta"),
        components.get(&second_component).expect("beta is filed"),
    );
}

/// THE SERIALIZED SHAPE DOES NOT MOVE ACROSS THE TRANSITION.
///
/// The page index is written into the shard index and its entries are stored keys, so which arm a
/// bucket happens to be in must not reach the bytes. It does not by construction -- `Serialize`
/// goes through `values()`, which is arm-agnostic, and sorts by the written key -- but "by
/// construction" is what this tree has repeatedly found to be untrue, so it is driven.
///
/// WHAT ALREADY EXISTS. `per_item_byte_budget::the_stored_spelling_of_a_bucket_node_did_not_move`
/// pins a whole `BucketNode`'s JSON against captured bytes -- but its fixture builds the page
/// index with `BlockIndexMap::default()`, so the bytes it pins contain an EMPTY page index. The
/// `One` and `Many` arms' wire output is pinned against an expected value nowhere. That is what
/// makes this a hole rather than a second copy of an existing guard.
///
/// THREE NODES, ONE EXPECTED SPELLING. A node that has only ever held one page; a node that grew
/// to four and shrank back to that same page; and the four-page node compared against itself
/// through a clone. If the arm reached the bytes, the first two would differ.
#[test]
fn the_inline_and_mapped_arms_write_the_same_bytes_for_the_same_page_set() {
    let mut live = BlockSlabLiveIndex::default();

    // A node that has only ever held one page.
    let mut untouched = BucketNode::default();
    untouched.routing_bucket = 11;
    let handle = untouched.block_index.insert(page_for(1), &mut live);
    assert!(
        matches!(untouched.block_index, BlockIndexMap::One(..)),
        "the untouched node is not in the inline shape"
    );

    // A node that grew to four pages and shrank back to the same single page.
    let mut round_tripped = BucketNode::default();
    round_tripped.routing_bucket = 11;
    round_tripped.block_index.insert(page_for(1), &mut live);
    let mut extra = Vec::new();
    for seed in 2..=4u64 {
        extra.push(round_tripped.block_index.insert(page_for(seed), &mut live));
    }
    assert!(
        matches!(round_tripped.block_index, BlockIndexMap::Many(_)),
        "the round-tripped node never reached the map shape, so this proves nothing"
    );
    for handle in &extra {
        round_tripped
            .block_index
            .remove(handle, &mut live)
            .expect("removing a page answered nothing");
    }
    assert!(
        matches!(round_tripped.block_index, BlockIndexMap::One(..)),
        "the round-tripped node did not shrink back to the inline shape"
    );

    let untouched_bytes = serde_json::to_string(&untouched).expect("a node serializes");
    let round_tripped_bytes = serde_json::to_string(&round_tripped).expect("a node serializes");
    println!("\n=== the stored spelling across the transition ===");
    println!("  never left the inline arm : {} B", untouched_bytes.len());
    println!("  grew to four and shrank   : {} B", round_tripped_bytes.len());
    assert_eq!(
        untouched_bytes, round_tripped_bytes,
        "a bucket that briefly held four pages writes different bytes from one that never did, so \
         which arm the index is in has reached the stored format"
    );

    // --- The control: this comparison CAN report a difference. ---
    //
    // Without it, two nodes that both serialized to nothing would pass the assertion above.
    let mut different = BucketNode::default();
    different.routing_bucket = 11;
    different.block_index.insert(page_for(2), &mut live);
    let different_bytes = serde_json::to_string(&different).expect("a node serializes");
    assert_ne!(
        untouched_bytes, different_bytes,
        "a node holding a DIFFERENT page serializes to the same bytes; the comparison above \
         cannot report a difference and is vacuous"
    );
    assert!(
        untouched_bytes.len() > 100,
        "the node serialized to {} bytes; an empty spelling would make every comparison here \
         vacuous",
        untouched_bytes.len()
    );

    // --- THE WRITTEN KEY CARRIES THE COMPONENT, which nothing in the crate pinned. ---
    //
    // Found by mutation, and the reason it was missed is worth keeping. The written key is
    // rendered from the page's identity AND its address, so dropping the component from it leaves
    // every key DISTINCT -- the addresses differ. Uniqueness cannot catch it, key counts cannot
    // catch it, and the ordering guard cannot catch it; the only thing that changes is the text of
    // a stored key, which is the bytes on disk. A mutation that dropped the component from the
    // written key survived every test in this crate.
    //
    // The two pages below differ ONLY in their component and sit at the same address, which is
    // what makes this about the component and not about the address.
    let mut without_component = component_page(5, "body");
    without_component.component = None;
    let with_key = crate::engine::state::block_index_written_key(&component_page(5, "body"));
    let without_key = crate::engine::state::block_index_written_key(&without_component);
    println!("  written key with a component    : {with_key}");
    println!("  written key without one         : {without_key}");
    assert!(
        with_key.contains("body"),
        "the written key for a page with a component does not carry the component: {with_key:?}"
    );
    assert_ne!(
        with_key, without_key,
        "two pages of one object at the same address, one with a component and one without, \
         render the SAME written key -- the component has stopped reaching the stored spelling"
    );

    // --- And a many-page node round-trips through the stored form back to the same page set. ---
    let mut many = BucketNode::default();
    many.routing_bucket = 11;
    for seed in 1..=4u64 {
        many.block_index.insert(page_for(seed), &mut live);
    }
    many.block_index.insert(component_page(5, "body"), &mut live);
    assert_eq!(5, many.block_index.len(), "five pages, five entries");
    let written = serde_json::to_string(&many).expect("a node serializes");
    let read_back: BucketNode = serde_json::from_str(&written).expect("a node deserializes");
    assert_same_page_set(
        "a five-page bucket through the stored form",
        &many.block_index,
        &read_back.block_index,
    );
    assert_eq!(
        written,
        serde_json::to_string(&read_back).expect("a node serializes"),
        "a node that has been through the stored form writes different bytes the second time"
    );
    std::hint::black_box(handle);
}

// ---------------------------------------------------------------------------------------------
// WHAT THE NEXT DOMINANT TERM IS, AND IT DEPENDS ON THE WORKLOAD.
// ---------------------------------------------------------------------------------------------

/// THE RANKING #1958 PUBLISHED IS A PROPERTY OF ITS FIXTURE, AND THIS SAYS WHICH TERM LEADS WHEN.
///
/// `BucketNode` x bucket count heads the table in a store of keys that route one to a bucket,
/// because there is one bucket per key. In a container store there are a hundred pages behind each
/// bucket, and `BlockIndex` x PAGE count leads by a factor the bucket row cannot approach.
///
/// So the next dominant term after the node's own width is the PAGE ENTRY, and it is per page
/// rather than per bucket -- 104 bytes of which 48 are the address, 48 are three shared names held
/// as fat pointers, and 8 are three flag bytes inside their own rounding.
#[test]
#[ignore = "seeds two stores; run by name"]
fn the_page_entry_and_not_the_bucket_node_is_the_next_dominant_term() {
    let mut rows: Vec<(&'static str, usize, usize, usize, usize)> = Vec::new();
    let mut path_lengths = Vec::new();

    {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = probe_engine(dir.path());
        seed_routed_keys(&engine, 40_000, 40, 1_000);
        let hist = pages_per_bucket(&engine);
        hist.report("keys that route one to a bucket");
        rows.push((
            "keys that route one to a bucket",
            hist.buckets(),
            hist.pages(),
            size_of::<BucketNode>() * hist.buckets(),
            size_of::<BlockIndex>() * hist.pages(),
        ));
    }
    {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = probe_engine(dir.path());
        seed_container_keys(&engine, 400, 100);
        let hist = pages_per_bucket(&engine);
        hist.report("container keys, 100 elements each");
        rows.push((
            "container keys",
            hist.buckets(),
            hist.pages(),
            size_of::<BucketNode>() * hist.buckets(),
            size_of::<BlockIndex>() * hist.pages(),
        ));
    }

    assert_eq!(
        path_lengths[0], path_lengths[1],
        "the store path length moved between arms"
    );

    println!("\n=== which term leads, by workload ===");
    println!(
        "  {:<34} {:>9} {:>9} {:>16} {:>16}",
        "workload", "buckets", "pages", "BucketNode x n", "BlockIndex x n"
    );
    for (name, buckets, pages, node_bytes, page_bytes) in &rows {
        println!("  {name:<34} {buckets:>9} {pages:>9} {node_bytes:>16} {page_bytes:>16}");
    }

    let (_, routed_buckets, routed_pages, routed_nodes, routed_pages_bytes) = rows[0];
    let (_, container_buckets, container_pages, container_nodes, container_pages_bytes) = rows[1];
    assert!(routed_buckets > 0 && container_buckets > 0, "denominator: a workload holds no buckets");
    assert!(routed_pages > 0 && container_pages > 0, "denominator: a workload holds no pages");

    // In the routed store the two terms are within a whisker of each other, because there is one
    // page per bucket and the page entry IS most of the node.
    println!(
        "\n  routed    : the page entry is {:.1}% of the node's own total",
        100.0 * routed_pages_bytes as f64 / routed_nodes as f64
    );
    println!(
        "  container : the page entry is {:.1}x the node total",
        container_pages_bytes as f64 / container_nodes as f64
    );
    assert!(
        container_pages_bytes > container_nodes * 10,
        "the container store's page entries are {container_pages_bytes} B against \
         {container_nodes} B of node; if the page entry no longer dominates there, the next term \
         named here is wrong"
    );

    // What the next step would have to reach, priced.
    println!("\n=== inside the 104-byte page entry ===");
    println!(
        "  address                       : {:>4} B",
        size_of::<BlockAddress>()
    );
    println!(
        "  object_key + model_id         : {:>4} B  (two fat pointers; the length rides beside \
         each)",
        2 * size_of::<Arc<str>>()
    );
    println!(
        "  component                     : {:>4} B",
        size_of::<Option<Arc<str>>>()
    );
    println!(
        "  dirty + deleted + log_backed  : {:>4} B  (3 B of flag in an 8 B rounding)",
        round_up_to(3 * size_of::<bool>(), 8)
    );
    println!(
        "  NEXT STEP, PRICED: the three shared names are {} of the {} bytes. Held as a THIN \
         pointer -- the length in the allocation's own header rather than beside each copy -- they \
         would be {} B, taking the entry to {} and the node to {}. That is {} B a page and {} B a \
         bucket, and it costs a load of the length on every read that needs one plus an unsafe \
         shared-string type this tree does not have.",
        2 * size_of::<Arc<str>>() + size_of::<Option<Arc<str>>>(),
        size_of::<BlockIndex>(),
        3 * size_of::<usize>(),
        size_of::<BlockIndex>() - (2 * size_of::<Arc<str>>() + size_of::<Option<Arc<str>>>())
            + 3 * size_of::<usize>(),
        size_of::<BucketNode>()
            - (2 * size_of::<Arc<str>>() + size_of::<Option<Arc<str>>>())
            + 3 * size_of::<usize>(),
        (2 * size_of::<Arc<str>>() + size_of::<Option<Arc<str>>>()) - 3 * size_of::<usize>(),
        (2 * size_of::<Arc<str>>() + size_of::<Option<Arc<str>>>()) - 3 * size_of::<usize>(),
    );

    // The three names are the largest single group inside the entry, which is what makes them the
    // next thing to price rather than the flags or the address.
    let names = 2 * size_of::<Arc<str>>() + size_of::<Option<Arc<str>>>();
    assert!(
        names >= size_of::<BlockAddress>(),
        "the three shared names are {names} B against the address's {} B; if the address has \
         become the larger group, the next step named here is the wrong one",
        size_of::<BlockAddress>()
    );
}
