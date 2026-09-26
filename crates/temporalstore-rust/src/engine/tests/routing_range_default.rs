// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CHOOSING THE SHIPPED ROUTING-RANGE DEFAULT FROM A MEASUREMENT, AND WHAT IT WOULD COST.
//!
//! The shipped default is the whole 32-bit keyspace. `docs/runtime_tuning.md` then tells an
//! operator to set 1,024 buckets before the first ingest, so the shipped default is not the
//! configuration the documentation says to run. #1960 established that the fill is a property of
//! the RANGE and not of the workload, and priced 1,024 against the whole keyspace. This module
//! asks the next question -- WHICH range should ship -- and it does not assume the answer is the
//! one the documentation happens to show.
//!
//! # WHAT 1023 IS, AND WHAT IT IS NOT
//!
//! 1023 is an example in a document. It is not a measured optimum, and nothing in the tree ever
//! swept the range to find one. #1960 compared two points; two points cannot locate a crossover.
//! So the range is swept here over four candidates at two corpus sizes, and every column an
//! operator would need to choose with is reported at every point.
//!
//! # THE CROSSOVER THE SWEEP IS LOOKING FOR
//!
//! `BlockIndexMap::One` holds its page inline and allocates nothing; `Many` is a flat sorted
//! `Vec`. Filling a bucket trades one bucket node per PAGE for one bucket node plus one vector
//! per BUCKET, so there is a fill below which the trade is a loss. That crossover is a property
//! of PAGES PER BUCKET, and pages per bucket is
//!
//! ```text
//!     records x pages-per-record / bucket-count
//! ```
//!
//! -- which contains the RECORD COUNT. A fixed bucket count therefore lands at a different fill
//! for every corpus, and the sweep is run at two corpus sizes precisely so that a candidate which
//! only looks good at one of them cannot pass as an optimum.
//! `the_fill_a_candidate_range_produces_moves_with_the_corpus_not_only_with_the_range` is the
//! assertion that this is so, and it is the finding this module exists to establish.
//!
//! # NEVER A MEAN
//!
//! #1959 published a mean of 1.98 pages a bucket for a store containing NOT ONE bucket holding
//! two. Every distribution here is reported as a histogram with p50, p90, p99 and MAX over a
//! stated denominator, and the mean is printed only beside them.
//!
//! # ONE INSTRUMENT, BOTH COLUMNS
//!
//! `bucket_fill.rs` reports `alloc_bytes` -- what the caller ASKED the allocator for. #1967 added
//! `ALLOC_CHUNK_BYTES`, which reads `malloc_usable_size` and charges what the allocator actually
//! handed over. The two differ by the rounding, and the rounding is exactly what a change of
//! container shape moves, so every byte figure here is reported in BOTH columns.
//!
//! # THE READ PATH IS COUNTED, NOT TIMED
//!
//! `PAGE_LOOKUP_ENTRIES_EXAMINED` counts the entries the shipped bisection touches, incremented
//! inside `find_page`, the one door every `Many`-arm lookup goes through. A deeper fill means a
//! longer list means more probes, and that is the read-path price of the fill. It is a count, so
//! it does not move with the load on the box.
//!
//! # THE CONTROL ON THE EXPLANATION
//!
//! The mechanism claimed is that the RANGE WIDTH is the modulus, so narrowing it groups keys.
//! `narrowing_the_range_changes_nothing_for_a_store_whose_pages_share_one_object_key` is the
//! workload where that mechanism predicts NO effect: routing takes the object key and never the
//! component, so a single key's many component pages sit in one bucket at every range. If the
//! sweep's effect showed up there too, the explanation would be wrong.

#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The candidate ranges swept, as END buckets. 1023 is the one `docs/runtime_tuning.md` shows;
/// the other three bracket it by a factor of four each way so a crossover between them is visible
/// rather than inferred.
const CANDIDATES: [u32; 4] = [255, 1023, 4095, 65535];

/// The shipped default, and the control every candidate is measured against.
const WIDE_END: u32 = u32::MAX;

const SMALL: usize = 4_000;
const LARGE: usize = 40_000;

/// Every arm the sweep runs: the four candidates plus the shipped default.
fn swept_ranges() -> Vec<u32> {
    let mut ranges: Vec<u32> = CANDIDATES.to_vec();
    ranges.push(WIDE_END);
    ranges
}

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
        table_name: "routing-range-default".to_string(),
        shard_uri: "local://routing-range-default/1".to_string(),
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

/// ROUTED KEYS: plain strings, one page each -- the shape every figure below is over.
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
// THE DISTRIBUTION: a histogram with percentiles and a MAX, over a stated denominator.
// ---------------------------------------------------------------------------------------------

/// Pages held per routing bucket, as bucket COUNTS keyed by pages held.
///
/// NEVER REPORTED AS A MEAN ALONE. #1959's mean of 1.98 pages a bucket described a store holding
/// no bucket with two pages in it. The percentiles below are taken over the BUCKET population --
/// `p50` is the pages held by the median occupied bucket -- and the denominator is printed with
/// every row so that a percentile over four buckets cannot read as one over four thousand.
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

    fn mean(&self) -> f64 {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0.0;
        }
        self.pages() as f64 / buckets as f64
    }

    fn max(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    fn min(&self) -> usize {
        self.counts.keys().copied().next().unwrap_or_default()
    }

    /// The pages held by the bucket at `fraction` of the way through the bucket population,
    /// ordered by pages held. A count off the histogram, not an interpolation: every value it can
    /// return is a page count some bucket actually holds.
    fn percentile(&self, fraction: f64) -> usize {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0;
        }
        let target = ((buckets as f64) * fraction).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (held, count) in &self.counts {
            seen += count;
            if seen >= target {
                return *held;
            }
        }
        self.max()
    }

    fn buckets_holding_more_than_one(&self) -> usize {
        self.counts
            .iter()
            .filter(|(held, _)| **held > 1)
            .map(|(_, count)| *count)
            .sum()
    }

    /// Every row, no collapsing. `bucket_fill.rs`'s reporter prints a row only when the page count
    /// is at most eight, a multiple of eight, or the widest -- which at a fill of 39 hides most of
    /// the distribution and makes the printed rows fail to sum to the denominator. A histogram
    /// whose rows do not add up is not one, so this prints every row and ASSERTS the sum.
    fn report(&self, label: &str) {
        let buckets = self.buckets();
        println!(
            "  {label}: {buckets} occupied buckets over {} pages | mean {:.4} min {} p50 {} \
             p90 {} p99 {} MAX {} | buckets holding >1 page: {} of {buckets}",
            self.pages(),
            self.mean(),
            self.min(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.max(),
            self.buckets_holding_more_than_one(),
        );
        let printed: usize = self.counts.values().copied().sum();
        assert_eq!(
            printed, buckets,
            "{label}: the histogram's rows sum to {printed} buckets over a denominator of \
             {buckets}"
        );
        for (held, count) in &self.counts {
            println!(
                "        {held:>6} page(s): {count:>6} buckets ({:>7.3}% of {buckets})",
                100.0 * *count as f64 / buckets.max(1) as f64
            );
        }
    }

    /// The same line without the rows, for the sweep table.
    fn line(&self, label: &str) {
        println!(
            "  {label:<34} buckets {:>6} pages {:>6} | mean {:>8.3} p50 {:>5} p90 {:>5} \
             p99 {:>5} MAX {:>5}",
            self.buckets(),
            self.pages(),
            self.mean(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.percentile(0.99),
            self.max(),
        );
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

/// How many buckets sit in each arm of `BlockIndexMap`: (Empty, One, Many).
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

/// The store path length every arm is held at, asserted equal across arms because allocation
/// bytes move at about six bytes a character.
fn store_path_length(dir: &std::path::Path) -> usize {
    dir.to_string_lossy().len()
}

// =============================================================================================
// 1. THE SWEEP: PAGES PER BUCKET AT EVERY CANDIDATE, AT TWO CORPUS SIZES
// =============================================================================================

/// THE RANGE SWEEP AS A HISTOGRAM WITH PERCENTILES AND A MAX, AT TWO CORPUS SIZES.
///
/// The question a default has to answer is "how full is a bucket", and the answer is not a
/// property of the range alone. `records x pages-per-record / bucket-count` contains the record
/// count, so the same candidate range lands at a different fill for every corpus -- and a
/// candidate chosen at one corpus size is chosen for that size only. This reports the whole
/// distribution at each of the five arms at each of the two sizes so that the dependence is
/// visible rather than argued.
///
/// PER-ARM SAMPLE COUNTS AND THE DENOMINATOR are printed on every row. An arm with no bucket in
/// it is not a measurement, and the `Many`-arm count says whether the fill happened at all.
///
/// THE STORE PATH LENGTH is asserted equal across arms: it moves allocation bytes at about six
/// bytes a character, and while page and bucket COUNTS are immune to it, the allocator table in
/// section 3 is not and shares this fixture's shape.
///
/// rust-internal: reads the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds ten stores up to 40,000 records each; run by name"]
fn the_pages_a_bucket_holds_at_every_candidate_range_as_percentiles_and_max() {
    let mut observed: BTreeMap<(usize, u32), PagesPerBucket> = BTreeMap::new();
    let mut path_lengths: BTreeSet<usize> = BTreeSet::new();

    for records in [SMALL, LARGE] {
        println!("=== {records} routed records ===");
        for end_routing_bucket in swept_ranges() {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.insert(store_path_length(dir.path()));
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            assert_eq!(
                records,
                read_back(&engine, &keys),
                "on 0..{end_routing_bucket} at {records} records the fixture cannot read its own \
                 store back, so the distribution below is over a store that does not work"
            );
            let hist = pages_per_bucket(&engine);
            let arms = block_index_arms(&engine);
            assert_eq!(
                records,
                hist.pages(),
                "on 0..{end_routing_bucket} at {records} records the index holds {} pages for \
                 {records} one-page records; the denominator is not what it is read as",
                hist.pages()
            );
            let label = if end_routing_bucket == WIDE_END {
                "0..u32::MAX (shipped default)".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            hist.line(&format!("{label} [arms E{} O{} M{}]", arms.0, arms.1, arms.2));
            observed.insert((records, end_routing_bucket), hist);
        }
    }

    assert_eq!(
        1,
        path_lengths.len(),
        "the arms ran at {} different store path lengths ({:?}); allocation bytes move at about \
         six bytes a character, so arms at different path lengths are not comparable",
        path_lengths.len(),
        path_lengths
    );
    println!(
        "  store path length held at {} characters across all {} arms",
        path_lengths.iter().next().copied().unwrap_or_default(),
        observed.len()
    );

    // THE FULL HISTOGRAM AT THE DOCUMENTED CANDIDATE, at both sizes, rows and all.
    for records in [SMALL, LARGE] {
        observed
            .get(&(records, 1023))
            .expect("the documented candidate ran")
            .report(&format!("0..1023 at {records} records"));
    }

    // THE SHIPPED DEFAULT IS ONE PAGE A BUCKET AT BOTH SIZES, by construction and at every
    // percentile. Asserted because it is the premise of the whole campaign.
    for records in [SMALL, LARGE] {
        let wide = observed.get(&(records, WIDE_END)).expect("the wide arm ran");
        assert_eq!(
            1,
            wide.max(),
            "on the shipped default at {records} records the widest bucket holds {} pages. Every \
             key lands alone by construction at a modulus of 4.29 billion, so a bucket holding \
             two means the fixture is not the shape every figure here is compared against",
            wide.max()
        );
        assert_eq!(
            0,
            wide.buckets_holding_more_than_one(),
            "on the shipped default at {records} records {} buckets hold more than one page",
            wide.buckets_holding_more_than_one()
        );
    }

    // EVERY CANDIDATE REACHED THE ARM IT IS BEING READ AS MEASURING. A candidate whose buckets are
    // all still single-page measures the shipped default under a different name.
    for records in [SMALL, LARGE] {
        for candidate in CANDIDATES {
            let hist = observed
                .get(&(records, candidate))
                .expect("every candidate ran");
            if (records as f64) / ((candidate as f64) + 1.0) < 1.0 {
                // Fewer records than buckets: a fill is not expected and its absence is not a
                // fixture defect. Printed rather than asserted, and named as the reason the
                // candidate is a poor one at this size.
                println!(
                    "  NOTE 0..{candidate} at {records} records: {} of {} buckets hold more than \
                     one page -- at {:.3} records a bucket this candidate is the shipped default \
                     in all but name",
                    hist.buckets_holding_more_than_one(),
                    hist.buckets(),
                    (records as f64) / ((candidate as f64) + 1.0),
                );
                continue;
            }
            assert!(
                hist.buckets_holding_more_than_one() > 0,
                "0..{candidate} at {records} records produced no bucket holding more than one \
                 page, so this arm did not fill and every figure taken from it describes the \
                 single-page shape instead"
            );
        }
    }
}

/// THE FILL A CANDIDATE PRODUCES MOVES WITH THE CORPUS, WHICH IS WHY NO FIXED DEFAULT IS RIGHT.
///
/// THIS IS THE FINDING. The crossover the container shapes have is a property of PAGES PER
/// BUCKET, and pages per bucket is `records x pages-per-record / bucket-count`. A shipped default
/// fixes the denominator and nothing else, so the fill it lands at is set by the corpus -- which
/// the engine does not know when it loads a shard. The same candidate is therefore below the
/// crossover at one corpus size and above it at another, and this asserts that it genuinely is
/// rather than leaving it as arithmetic.
///
/// Asserted as a RATIO between the two corpus sizes at each candidate, which is the form that
/// cannot be true by accident: ten times the records over the same bucket count must be about ten
/// times the fill.
///
/// rust-internal: reads the engine's own bucket index, no product behaviour
#[test]
#[ignore = "seeds eight stores up to 40,000 records each; run by name"]
fn the_fill_a_candidate_range_produces_moves_with_the_corpus_not_only_with_the_range() {
    let mut fills: BTreeMap<(usize, u32), (f64, usize)> = BTreeMap::new();

    for records in [SMALL, LARGE] {
        for candidate in CANDIDATES {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = engine_on(dir.path());
            load_on(&engine, candidate);
            let keys = seed_routed(&engine, records);
            assert_eq!(records, read_back(&engine, &keys), "the arm cannot read itself back");
            let hist = pages_per_bucket(&engine);
            fills.insert((records, candidate), (hist.mean(), hist.buckets()));
            println!(
                "  0..{candidate} at {records} records: p50 {} pages, MAX {} pages over {} \
                 occupied buckets of {} in the range",
                hist.percentile(0.50),
                hist.max(),
                hist.buckets(),
                (candidate as usize) + 1
            );
        }
    }

    // THE MEAN REPORTED ABOVE IS OVER OCCUPIED BUCKETS, and that matters here. Below saturation a
    // hash leaves buckets empty, so ten times the records lands in more DISTINCT buckets as well
    // as more pages each, and the occupied-bucket mean moves by LESS than the corpus ratio. The
    // strict prediction therefore holds only where both arms are saturated -- every bucket in the
    // range occupied -- and where they are not, the occupancy is printed and the weaker bound is
    // used. Measured: 0..4095 goes 1.452 -> 9.766 (6.72x, not 10x) precisely because its small arm
    // occupies 2,754 of 4,096 buckets.
    let corpus_ratio = LARGE as f64 / SMALL as f64;
    for candidate in CANDIDATES {
        let range_buckets = (candidate as usize) + 1;
        let (small, small_occupied) = *fills.get(&(SMALL, candidate)).expect("small arm");
        let (large, large_occupied) = *fills.get(&(LARGE, candidate)).expect("large arm");
        let ratio = large / small;
        let saturated = small_occupied == range_buckets && large_occupied == range_buckets;
        println!(
            "  0..{candidate}: fill {small:.3} -> {large:.3} pages a bucket as the corpus goes \
             {SMALL} -> {LARGE} ({ratio:.2}x against a corpus ratio of {corpus_ratio:.2}x); \
             occupancy {small_occupied}/{range_buckets} -> {large_occupied}/{range_buckets}, \
             saturated at both sizes: {saturated}"
        );
        if saturated {
            assert!(
                ratio > corpus_ratio * 0.95,
                "0..{candidate} is saturated at both sizes ({small_occupied} and \
                 {large_occupied} of {range_buckets} buckets occupied), so the fill is exactly \
                 records over buckets and ten times the records must be ten times the fill. It \
                 moved {ratio:.2}x against {corpus_ratio:.2}x"
            );
        } else if large_occupied == range_buckets {
            // Saturated at the large corpus only. The small arm's occupied-bucket mean is over a
            // subset, which understates the ratio, so the strict prediction cannot be asserted --
            // but the fill must still move substantially with the corpus.
            assert!(
                ratio > 2.0,
                "0..{candidate} moved its fill only {ratio:.2}x for ten times the records while \
                 reaching saturation ({large_occupied} of {range_buckets}) at the large corpus. \
                 Below saturation the occupied-bucket mean understates the ratio -- occupancy \
                 {small_occupied} -> {large_occupied} -- but it must still move, or the fill does \
                 not depend on the corpus at all"
            );
        } else {
            // SATURATED AT NEITHER SIZE. This candidate has more buckets than the corpus has
            // records, so it is not filling buckets at all: it is the shipped default under
            // another name, and the honest thing to assert about it is exactly that. An occupancy
            // ratio here is not a fill measurement and must not be read as one.
            assert!(
                large < 11.0,
                "0..{candidate} is saturated at neither corpus size (occupancy {small_occupied} \
                 -> {large_occupied} of {range_buckets}) yet reached a fill of {large:.3} pages a \
                 bucket. A candidate with more buckets than the corpus has records is supposed to \
                 stay near one page a bucket; if it does not, the classification this arm rests \
                 on is wrong"
            );
            println!(
                "        0..{candidate} is saturated at NEITHER size: {range_buckets} buckets for \
                 at most {LARGE} records, so it never fills and its fill ratio of {ratio:.2}x is \
                 occupancy growth, not a fill. It is the shipped default under another name."
            );
        }
    }

    // AND THE CONSEQUENCE, STATED AS THE ASSERTION A DEFAULT DECISION RESTS ON: at the two sizes
    // measured, no single candidate sits in the same regime. The crossover between `One` and a
    // filled `Many` is around eleven pages; a candidate below it at 4,000 records is above it at
    // 40,000 and the reverse.
    let straddlers: Vec<u32> = CANDIDATES
        .into_iter()
        .filter(|candidate| {
            let (small, _) = *fills.get(&(SMALL, *candidate)).expect("small arm");
            let (large, _) = *fills.get(&(LARGE, *candidate)).expect("large arm");
            small < 11.0 && large >= 11.0
        })
        .collect();
    println!(
        "  candidates whose fill straddles the eleven-page crossover between {SMALL} and {LARGE} \
         records: {straddlers:?} of {:?}",
        CANDIDATES
    );
    assert!(
        !straddlers.is_empty(),
        "no candidate's fill crosses the eleven-page mark between {SMALL} and {LARGE} records. \
         The claim this module makes is that a fixed bucket count cannot hold a fill across corpus \
         sizes; if no candidate straddles, that claim is not established by this fixture and the \
         recommendation must not lean on it"
    );
}

// =============================================================================================
// 2. THE READ PATH, COUNTED
// =============================================================================================

/// WHAT MORE PAGES A BUCKET COSTS THE READ PATH, AS ENTRIES EXAMINED PER LOOKUP.
///
/// COUNTED, NOT TIMED. A timing-ratio instrument in this campaign read 485x idle against 11x busy
/// off identical code. `PAGE_LOOKUP_ENTRIES_EXAMINED` is incremented inside `find_page` -- the one
/// door every `Many`-arm lookup goes through -- so this counts the entries the shipped bisection
/// actually touched on the production path.
///
/// The `One` arm is not a list and examines nothing, so the shipped default reads ZERO entries a
/// lookup and every candidate reads more. That is the price, and it is bounded: a bisection over a
/// list of `n` touches about `log2(n)` entries, so ten times the fill is between three and four
/// more probes rather than ten times the work. Reported per lookup at every candidate so the
/// bound is a measurement and not an appeal to the asymptotics.
///
/// rust-internal: reads the engine's own lookup counter, no product behaviour
#[test]
#[ignore = "seeds ten stores up to 40,000 records each; run by name"]
fn the_read_path_entries_examined_a_lookup_grow_with_the_fill_a_candidate_produces() {
    let mut observed: BTreeMap<(usize, u32), (f64, usize, usize)> = BTreeMap::new();

    for records in [SMALL, LARGE] {
        for end_routing_bucket in swept_ranges() {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            let hist = pages_per_bucket(&engine);

            crate::engine::state::reset_page_lookup_entries_examined();
            let readable = read_back(&engine, &keys);
            let examined = crate::engine::state::page_lookup_entries_examined();
            assert_eq!(
                records, readable,
                "on 0..{end_routing_bucket} at {records} records only {readable} records read \
                 back, so the entry count below is over a partial read"
            );

            let per_lookup = examined as f64 / records as f64;
            let label = if end_routing_bucket == WIDE_END {
                "0..u32::MAX (shipped default)".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            println!(
                "  {records} records {label:<30} p50 fill {:>5} MAX {:>5} | {examined:>8} \
                 entries examined over {records} lookups = {per_lookup:.4} a lookup",
                hist.percentile(0.50),
                hist.max()
            );
            observed.insert(
                (records, end_routing_bucket),
                (per_lookup, hist.percentile(0.50), hist.max()),
            );
        }
    }

    // THE SHIPPED DEFAULT EXAMINES NOTHING, because every bucket is on the inline `One` arm and
    // `find_page` is never reached. This is the denominator of the whole section and it is zero,
    // so the cost is reported as an absolute count a lookup rather than as a ratio -- a ratio
    // against zero is not a number.
    for records in [SMALL, LARGE] {
        let (wide_per_lookup, _, wide_max) =
            *observed.get(&(records, WIDE_END)).expect("the wide arm ran");
        assert_eq!(
            1, wide_max,
            "the shipped default at {records} records holds {wide_max} pages in its widest \
             bucket, so it is not the single-page shape this section's zero is attributed to"
        );
        assert_eq!(
            0.0, wide_per_lookup,
            "the shipped default at {records} records examined {wide_per_lookup} entries a \
             lookup. The `One` arm holds its page inline and never reaches `find_page`, so a \
             non-zero here means the counter is reading a path other than the one named"
        );
    }

    // AND EVERY CANDIDATE PAYS, MONOTONICALLY IN THE FILL. Narrower range, deeper fill, more
    // probes. Asserted at the large corpus, where every candidate has filled.
    let mut previous: Option<(u32, f64)> = None;
    for candidate in CANDIDATES {
        let (per_lookup, p50, max) = *observed.get(&(LARGE, candidate)).expect("candidate ran");
        if let Some((wider_earlier, wider_cost)) = previous {
            // CANDIDATES ascends, so each successive candidate is WIDER and must be cheaper.
            assert!(
                per_lookup <= wider_cost,
                "0..{candidate} examined {per_lookup:.4} entries a lookup against \
                 0..{wider_earlier}'s {wider_cost:.4}, yet 0..{candidate} is the WIDER range and \
                 therefore the shallower fill. The read-path cost is supposed to follow the list \
                 length; if it does not, this section is not measuring the walk"
            );
        }
        println!(
            "  0..{candidate} at {LARGE} records: p50 fill {p50}, MAX {max}, \
             {per_lookup:.4} entries examined a lookup"
        );
        previous = Some((candidate, per_lookup));
    }

    let (narrowest, _, _) = *observed.get(&(LARGE, CANDIDATES[0])).expect("narrowest");
    let (widest_candidate, _, _) = *observed
        .get(&(LARGE, CANDIDATES[CANDIDATES.len() - 1]))
        .expect("widest candidate");
    println!(
        "  the read-path price of the fill at {LARGE} records: 0.0000 entries a lookup on the \
         shipped default, {widest_candidate:.4} at 0..{}, {narrowest:.4} at 0..{} -- a bisection, \
         so a 256-fold narrowing is eight more probes and not 256 times the work",
        CANDIDATES[CANDIDATES.len() - 1],
        CANDIDATES[0]
    );
}

// =============================================================================================
// 3. BOTH BYTE COLUMNS, AND THE ALLOCATION COUNT
// =============================================================================================

/// THE PLANTED MARKER FOR BOTH COLUMNS. Recovered exactly, or every figure in section 3 is noise.
///
/// The plant is a size the allocator ROUNDS -- 100 bytes, served from a 112-byte chunk -- because
/// that is the only kind of plant that can separate the two failures the two columns have:
///
///   * the request column recovers `COPIES x 100` EXACTLY, so an instrument that is not running
///     cannot report a comfortable zero; and
///   * the chunk column recovers `COPIES x documented_glibc_chunk(100)` EXACTLY, which is MORE.
///     A chunk column that merely copied the request column -- the pre-#1967 bias every byte
///     comparison in this campaign carried -- fails here rather than flattering the out-of-line
///     shapes this module measures.
///
/// A LARGE PLANT IS DELIBERATELY NOT USED. A megabyte request is served through `mmap` with its
/// own page rounding rather than from a heap chunk, so its chunk reading is larger than
/// `documented_glibc_chunk` predicts and an exact assertion over it would be an assertion about
/// the mapping granularity instead of about the instrument. Measured at 1 MiB: request 1,048,576 B
/// against chunk 1,052,664 B.
///
/// rust-internal: measures the harness's own instrument, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "the counting allocator is process-wide; run by name"]
fn both_byte_columns_recover_a_planted_size_and_the_chunk_column_is_not_the_request_column() {
    const ROUNDED: usize = 100;
    const COPIES: usize = 4_096;
    let expected_chunk = crate::alloc_probe::documented_glibc_chunk(ROUNDED);
    assert_ne!(
        ROUNDED, expected_chunk,
        "the planted size must be one the allocator rounds, or this control cannot tell a chunk \
         reading from a request reading"
    );

    // THE OUTER VECTOR IS RESERVED BEFORE THE PROBE STARTS. Reserving inside the span charges its
    // own `COPIES x 24` bytes to the plant and the request column then reads 507,904 for a planted
    // 409,600 -- an instrument that looks broken when it is the fixture that is.
    let mut held: Vec<Vec<u8>> = Vec::with_capacity(COPIES);
    let probe = Probe::start();
    for _ in 0..COPIES {
        held.push(vec![0xA5u8; ROUNDED]);
    }
    let counts = probe.stop();
    std::hint::black_box(&held);
    println!(
        "  planted {COPIES} x {ROUNDED} B: request column {} B (expected {}), chunk column {} B \
         (expected {}), {} call(s)",
        counts.alloc_bytes,
        (COPIES * ROUNDED) as u64,
        counts.chunk_bytes,
        (COPIES * expected_chunk) as u64,
        counts.allocs
    );
    assert_eq!(
        COPIES as u64, counts.allocs,
        "{COPIES} planted vectors charged {} allocations; anything else means the span caught \
         work that is not the plant",
        counts.allocs
    );
    assert_eq!(
        (COPIES * ROUNDED) as u64,
        counts.alloc_bytes,
        "the request column charged {} B for {COPIES} planted {ROUNDED}-byte requests",
        counts.alloc_bytes
    );
    assert_eq!(
        (COPIES * expected_chunk) as u64,
        counts.chunk_bytes,
        "the chunk column charged {} B for {COPIES} planted {ROUNDED}-byte requests, which glibc \
         serves from {expected_chunk}-byte chunks. If this reads {} -- the request total -- then \
         the chunk column is the request column under another name and every byte comparison in \
         this module carries the bias #1967 corrected",
        counts.chunk_bytes,
        (COPIES * ROUNDED) as u64
    );
    assert!(
        counts.chunk_bytes > counts.alloc_bytes,
        "the chunk column charged {} B and the request column {} B; the chunk is never smaller \
         and for a rounded size never equal",
        counts.chunk_bytes,
        counts.alloc_bytes
    );
    drop(held);
}

/// What the bucket map charges the allocator, in both columns and in calls.
#[cfg(feature = "alloc-probe")]
struct Arm {
    request_bytes: u64,
    chunk_bytes: u64,
    allocs: u64,
    buckets: usize,
    pages: usize,
    records: usize,
    arms: (usize, usize, usize),
    fill_p50: usize,
    fill_max: usize,
}

#[cfg(feature = "alloc-probe")]
impl Arm {
    fn request_per_record(&self) -> f64 {
        self.request_bytes as f64 / self.records as f64
    }
    fn chunk_per_record(&self) -> f64 {
        self.chunk_bytes as f64 / self.records as f64
    }
    fn allocs_per_record(&self) -> f64 {
        self.allocs as f64 / self.records as f64
    }
}

/// BOTH BYTE COLUMNS AND THE ALLOCATION COUNT, PER RECORD, AT EVERY CANDIDATE, AT TWO SIZES.
///
/// PER RECORD and not per page, so the column is the one an operator sizing a box reads. The page
/// count is asserted equal to the record count in the fixture, so the two differ only by a name.
///
/// BOTH COLUMNS, BECAUSE THEY CAN DISAGREE IN SIGN. `alloc_bytes` charges `layout.size()`;
/// `chunk_bytes` charges `malloc_usable_size`. A container change moves the rounding, so a saving
/// the request column reports can be entirely rounding the chunk column never gave back. Every
/// comparison in this campaign before #1967 read the request column only.
///
/// ONE INSTRUMENT FOR BOTH SIDES: the counting allocator over a clone of the real `bucket_map` as
/// production built it. No `size_of` arithmetic appears in the comparison.
///
/// NEITHER DIRECTION IS ASSERTED. The arm counts are printed beside the bytes because they are
/// what explains the sign, and the sign is the thing being measured.
///
/// rust-internal: measures the engine's own bucket map, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "seeds ten stores up to 40,000 records each; run by name"]
fn both_byte_columns_and_the_allocations_a_record_at_every_candidate_range() {
    let mut observed: BTreeMap<(usize, u32), Arm> = BTreeMap::new();
    let mut path_lengths: BTreeSet<usize> = BTreeSet::new();

    for records in [SMALL, LARGE] {
        for end_routing_bucket in swept_ranges() {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.insert(store_path_length(dir.path()));
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, records);
            assert_eq!(records, read_back(&engine, &keys), "the arm cannot read itself back");
            let hist = pages_per_bucket(&engine);
            let arms = block_index_arms(&engine);

            let (request_bytes, chunk_bytes, allocs) = {
                let shards = engine.shards.read().expect("engine lock poisoned");
                let shard = shards.get(&1).expect("shard 1");
                let probe = Probe::start();
                let copy = shard.bucket_index.bucket_map.clone();
                let counts = probe.stop();
                std::hint::black_box(&copy);
                drop(copy);
                (counts.alloc_bytes, counts.chunk_bytes, counts.allocs)
            };
            assert!(
                request_bytes > 0 && allocs > 0,
                "on 0..{end_routing_bucket} at {records} records the clone charged \
                 {request_bytes} B in {allocs} calls. A zero here is the instrument not running, \
                 which reads as the shape being free"
            );

            observed.insert(
                (records, end_routing_bucket),
                Arm {
                    request_bytes,
                    chunk_bytes,
                    allocs,
                    buckets: hist.buckets(),
                    pages: hist.pages(),
                    records,
                    arms,
                    fill_p50: hist.percentile(0.50),
                    fill_max: hist.max(),
                },
            );
        }
    }

    assert_eq!(
        1,
        path_lengths.len(),
        "the arms ran at {} different store path lengths ({:?}); bytes move at about six bytes a \
         character",
        path_lengths.len(),
        path_lengths
    );
    println!(
        "  store path length held at {} characters across all {} arms",
        path_lengths.iter().next().copied().unwrap_or_default(),
        observed.len()
    );

    for records in [SMALL, LARGE] {
        println!("=== {records} routed records: the bucket map, per record ===");
        let wide = observed.get(&(records, WIDE_END)).expect("wide arm");
        for end_routing_bucket in swept_ranges() {
            let arm = observed.get(&(records, end_routing_bucket)).expect("arm ran");
            assert_eq!(
                arm.pages, arm.records,
                "on 0..{end_routing_bucket} at {records} records the index holds {} pages for \
                 {records} records; a per-record figure over a different page count is two \
                 questions",
                arm.pages
            );
            let label = if end_routing_bucket == WIDE_END {
                "0..u32::MAX (shipped)".to_string()
            } else {
                format!("0..{end_routing_bucket}")
            };
            println!(
                "  {label:<24} fill p50 {:>4} MAX {:>4} | REQUEST {:>8.1} B/rec ({:+7.1}%) \
                 CHUNK {:>8.1} B/rec ({:+7.1}%) | allocs {:>7.4}/rec ({:+8.1}%) | \
                 buckets {:>6} arms E{} O{} M{}",
                arm.fill_p50,
                arm.fill_max,
                arm.request_per_record(),
                100.0 * (arm.request_per_record() - wide.request_per_record())
                    / wide.request_per_record(),
                arm.chunk_per_record(),
                100.0 * (arm.chunk_per_record() - wide.chunk_per_record())
                    / wide.chunk_per_record(),
                arm.allocs_per_record(),
                100.0 * (arm.allocs_per_record() - wide.allocs_per_record())
                    / wide.allocs_per_record(),
                arm.buckets,
                arm.arms.0,
                arm.arms.1,
                arm.arms.2,
            );
        }
    }

    // THE ARMS MOVED, or every figure above is two readings of one shape.
    for records in [SMALL, LARGE] {
        let wide = observed.get(&(records, WIDE_END)).expect("wide arm");
        assert_eq!(
            0, wide.arms.2,
            "on the shipped default at {records} records {} buckets are already on the `Many` \
             arm; at one page a bucket none should be",
            wide.arms.2
        );
        let narrowest = observed.get(&(records, CANDIDATES[0])).expect("narrowest arm");
        assert!(
            narrowest.arms.2 > 0,
            "0..{} at {records} records put no bucket on the `Many` arm, so no candidate filled \
             and the table above compares single-page stores",
            CANDIDATES[0]
        );
    }

    // THE TWO COLUMNS ARE NOT THE SAME COLUMN, on the real measurement rather than on a plant.
    // If they were, the chunk column would be decorative and this module's claim to have settled
    // the sign would be empty.
    let mut columns_differ = 0usize;
    for ((records, end_routing_bucket), arm) in &observed {
        if arm.chunk_bytes != arm.request_bytes {
            columns_differ += 1;
        } else {
            println!(
                "  NOTE 0..{end_routing_bucket} at {records} records: the two columns agree \
                 exactly at {} B",
                arm.chunk_bytes
            );
        }
    }
    assert!(
        columns_differ > 0,
        "the request and chunk columns agreed exactly on all {} arms. On a real bucket map they \
         differ by the allocator's rounding, so agreement everywhere means the chunk column is \
         not reading `malloc_usable_size` and this table carries the pre-#1967 bias",
        observed.len()
    );
    println!(
        "  the two byte columns differ on {columns_differ} of {} arms",
        observed.len()
    );
}

// =============================================================================================
// 4. WHAT MORE PAGES A BUCKET COSTS THE WRITE PATH AND THE RELEASE UNIT
// =============================================================================================

/// THE DUMP UNIT, THE DIRTY DRAIN AND THE RELEASE UNIT ALL COARSEN BY THE BUCKET'S FILL.
///
/// A routing bucket is the unit of three separate things, and every one of them gets coarser as
/// the bucket fills:
///
///   * `DirtyObjectIndex::drain_buckets` is what a dump runs once its manifest is durable, and it
///     drops every dirty key of the named bucket -- so a dump of one bucket writes the bucket's
///     whole key group;
///   * `refresh_one_bucket_runtime_flags` ORs `bucket.dirty` over every page, so one dirty key
///     marks the group; and
///   * `release_bucket_blocks` frees a whole bucket's pages at once, so the bucket is also the
///     granularity at which memory can be given back.
///
/// Measured at every candidate: the keys a one-bucket drain drops, and the pages a one-bucket
/// release frees. Both are counts of what the engine's own functions did, not derivations from the
/// fill.
///
/// rust-internal: reads the engine's own dirty index and release pass, no product behaviour
#[test]
#[ignore = "seeds ten stores of 40,000 records each; run by name"]
fn the_dump_drain_and_the_release_unit_coarsen_by_the_pages_a_bucket_holds() {
    let mut observed: BTreeMap<u32, (usize, usize, usize, usize)> = BTreeMap::new();

    for end_routing_bucket in swept_ranges() {
        // TWO STORES PER RANGE, because the two units want opposite states: the dirty drain is
        // only measurable while the store IS dirty, and `release_bucket_blocks` REFUSES a dirty
        // bucket on `bucket_dirty`. Measuring both on one store would flatten one of them to zero
        // at every range, which reads as the unit being range-independent.
        let (dirty_buckets_len, dirty_keys, drained, fill_p50, fill_max) = {
            let dir = tempfile::tempdir().expect("tempdir");
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let keys = seed_routed(&engine, LARGE);
            let hist = pages_per_bucket(&engine);
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("shard 1");

            // DENOMINATOR: a freshly written store has every key dirty. Without this the drain
            // could drop nothing and report a clean zero for the wrong reason.
            let dirty_buckets: Vec<(u32, u64)> = shard.dirty_objects.bucket_counts().collect();
            let dirty_keys: usize = dirty_buckets.iter().map(|(_, count)| *count as usize).sum();
            assert_eq!(
                dirty_keys,
                keys.len(),
                "on 0..{end_routing_bucket} the dirty index holds {dirty_keys} keys after writing \
                 {}",
                keys.len()
            );

            // THE DUMP DRAIN, on the WIDEST dirty bucket -- the amplification an operator meets.
            let widest_dirty = dirty_buckets
                .iter()
                .max_by_key(|(_, count)| *count)
                .map(|(routing_bucket, _)| *routing_bucket)
                .expect("a freshly written store has a dirty bucket");
            let drained = shard.dirty_objects.drain_buckets(&[widest_dirty]);
            assert!(
                drained > 0,
                "on 0..{end_routing_bucket} draining bucket {widest_dirty} dropped nothing, so \
                 this arm measures the drain not running"
            );
            (
                dirty_buckets.len(),
                dirty_keys,
                drained,
                hist.percentile(0.50),
                hist.max(),
            )
        };

        // THE RELEASE UNIT, on a store RELOADED so the pass is not refused.
        //
        // `release_bucket_blocks` refuses a dirty bucket, and it is right to: the model maps carry
        // no per-page dirty bit, so a reload could not restore what a release of a dirty bucket
        // dropped. A freshly written store is entirely dirty, so without clearing it every release
        // here is refused and frees zero pages -- identically at every range, which would read as
        // the release unit being range-independent.
        //
        // NEITHER `flush_shard_index` NOR `dump_index_catalog` CLEARS IT. Both were tried and both
        // left one `bucket_dirty` refusal: `refresh_one_bucket_runtime_flags` sets the flag as
        // `bucket.dirty() | any_page_dirty`, an OR that never clears. What does clear it is the
        // clear-dirty-on-load contract in `persistence.rs` -- a reloaded page comes back
        // `dirty = false` -- so the store is written, flushed, dropped and reopened.
        let pages_released = {
            let dir = tempfile::tempdir().expect("tempdir");
            {
                let engine = engine_on(dir.path());
                load_on(&engine, end_routing_bucket);
                let _ = seed_routed(&engine, LARGE);
                engine.flush_shard_index(1);
                engine.unload_shard(1);
            }
            let engine = engine_on(dir.path());
            load_on(&engine, end_routing_bucket);
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("shard 1");
            let widest_bucket = shard
                .bucket_index
                .bucket_map
                .iter()
                .max_by_key(|(_, bucket)| bucket.block_index.len())
                .map(|(routing_bucket, _)| *routing_bucket)
                .expect("a written store has a bucket");
            let held = shard
                .bucket_index
                .bucket_map
                .get(&widest_bucket)
                .map(|bucket| bucket.block_index.len())
                .unwrap_or_default();
            let released = crate::engine::storage_bucket_internals::release_bucket_blocks(
                shard,
                &[widest_bucket],
            );
            assert_eq!(
                0, released.refused_buckets,
                "on 0..{end_routing_bucket} the release of bucket {widest_bucket} (holding \
                 {held} pages) was refused {} time(s): {:?}. A refused release frees zero pages \
                 at every range, which would read as the release unit not moving with the fill",
                released.refused_buckets, released.refusals
            );
            released.released_blocks
        };

        let label = if end_routing_bucket == WIDE_END {
            "0..u32::MAX (shipped)".to_string()
        } else {
            format!("0..{end_routing_bucket}")
        };
        println!(
            "  {label:<24} fill p50 {fill_p50:>4} MAX {fill_max:>4} | {dirty_buckets_len} dirty \
             buckets over {dirty_keys} dirty keys | a one-bucket dump drains {drained} keys | a \
             one-bucket release frees {pages_released} pages",
        );
        observed.insert(
            end_routing_bucket,
            (drained, pages_released, dirty_buckets_len, fill_max),
        );
    }

    // THE SHIPPED DEFAULT IS THE FINEST UNIT THERE IS: one key, one page.
    let (wide_drained, wide_released, _, wide_max) =
        *observed.get(&WIDE_END).expect("the wide arm ran");
    assert_eq!(
        1, wide_max,
        "the shipped default's widest bucket holds {wide_max} pages, so it is not the one-page \
         shape the rest of this test compares against"
    );
    assert_eq!(
        1, wide_drained,
        "on the shipped default a dump of one bucket drained {wide_drained} keys, not one"
    );
    assert_eq!(
        1, wide_released,
        "on the shipped default a release of one bucket freed {wide_released} pages, not one"
    );

    // AND EVERY CANDIDATE COARSENS BOTH, monotonically: the narrower the range the deeper the
    // fill and the coarser the unit.
    let mut previous: Option<(u32, usize, usize)> = None;
    for candidate in CANDIDATES {
        let (drained, released, _, _) = *observed.get(&candidate).expect("candidate ran");
        assert!(
            drained > wide_drained && released > wide_released,
            "0..{candidate} drains {drained} keys and releases {released} pages a bucket against \
             the shipped default's {wide_drained} and {wide_released}. If a candidate did not \
             coarsen either unit then it did not fill a bucket and this whole section is vacuous"
        );
        if let Some((wider, wider_drained, wider_released)) = previous {
            assert!(
                drained <= wider_drained && released <= wider_released,
                "0..{candidate} drains {drained} keys and releases {released} pages against \
                 0..{wider}'s {wider_drained} and {wider_released}, yet 0..{candidate} is the \
                 WIDER range and therefore the shallower fill"
            );
        }
        previous = Some((candidate, drained, released));
    }

    let (narrow_drained, narrow_released, _, _) =
        *observed.get(&CANDIDATES[0]).expect("narrowest ran");
    println!(
        "  the write-path and release price of the fill at {LARGE} records: a one-bucket dump \
         drains {wide_drained} key and a release frees {wide_released} page on the shipped \
         default, against {narrow_drained} keys and {narrow_released} pages at 0..{} -- both \
         units coarsen by exactly the fill",
        CANDIDATES[0]
    );
}

// =============================================================================================
// 5. THE CONTROL ON THE EXPLANATION
// =============================================================================================

/// THE WORKLOAD WHERE THE MECHANISM PREDICTS NO EFFECT, AND THERE IS NONE.
///
/// Everything above is attributed to one mechanism: the range width is the MODULUS, so narrowing
/// it maps more object keys onto one bucket. That explanation makes a falsifiable prediction in
/// the other direction -- a store whose pages all share ONE OBJECT KEY cannot be regrouped by any
/// range, because `block_routing_bucket` takes the object key and never the component while the
/// page handle `stable_block_object_id(shard, kind, key, component)` takes both. One key is one
/// bucket at every range.
///
/// So this seeds a single container key with many component pages and sweeps the same five ranges
/// over it. If the byte and fill figures moved here too, the mechanism named above would not be
/// the mechanism and the sweep would be measuring something else.
///
/// rust-internal: reads the engine's own placement functions, no product behaviour
#[test]
#[ignore = "seeds five container stores; run by name"]
fn narrowing_the_range_changes_nothing_for_a_store_whose_pages_share_one_object_key() {
    const MEMBERS: usize = 2_000;
    let mut observed: BTreeMap<u32, (usize, usize, usize)> = BTreeMap::new();

    for end_routing_bucket in swept_ranges() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        load_on(&engine, end_routing_bucket);
        run_batch(
            &engine,
            (0..MEMBERS)
                .map(|f| Command::HashSet {
                    key: "one-container".to_string(),
                    field: format!("f{f}"),
                    value: vec![b'v'; 32],
                })
                .collect(),
        );
        let hist = pages_per_bucket(&engine);
        let arms = block_index_arms(&engine);
        let label = if end_routing_bucket == WIDE_END {
            "0..u32::MAX (shipped)".to_string()
        } else {
            format!("0..{end_routing_bucket}")
        };
        println!(
            "  {label:<24} {} occupied buckets, {} pages, p50 {} MAX {} | arms E{} O{} M{}",
            hist.buckets(),
            hist.pages(),
            hist.percentile(0.50),
            hist.max(),
            arms.0,
            arms.1,
            arms.2
        );
        observed.insert(end_routing_bucket, (hist.buckets(), hist.pages(), hist.max()));
    }

    // NON-VACUITY FIRST: the fixture has to have written pages, or "no effect" is the effect of
    // an empty store.
    let (_, wide_pages, wide_max) = *observed.get(&WIDE_END).expect("the wide arm ran");
    assert!(
        wide_pages >= MEMBERS,
        "the container arm wrote {wide_pages} pages for {MEMBERS} components; an absent effect \
         over an empty store is not a control"
    );
    assert!(
        wide_max > 1,
        "the container arm's widest bucket holds {wide_max} page, so the components did not share \
         a bucket even on the shipped default and this control is not the shape it names"
    );

    // AND THE PREDICTION: identical at every range, element for element.
    for end_routing_bucket in swept_ranges() {
        let (buckets, pages, max) = *observed.get(&end_routing_bucket).expect("arm ran");
        assert_eq!(
            (1, wide_pages, wide_max),
            (buckets, pages, max),
            "on 0..{end_routing_bucket} a store of one object key occupied {buckets} buckets \
             holding {pages} pages with a widest of {max}, against the shipped default's 1 \
             bucket, {wide_pages} pages, widest {wide_max}. Routing takes the object key and \
             never the component, so the range cannot split one key -- if it did here, the \
             mechanism every figure in this module is attributed to is not the mechanism"
        );
    }
    println!(
        "  the control holds: one object key is one bucket holding {wide_pages} pages at all {} \
         ranges, so the sweep's effect is the modulus over OBJECT KEYS and nothing else",
        swept_ranges().len()
    );
}

// =============================================================================================
// 6. THE MIGRATION -- DRIVEN, NOT ARGUED
// =============================================================================================

use crate::engine::routing_range_stamp::{
    read_routing_range_stamp, routing_range_stamp_path, LEGACY_END_ROUTING_BUCKET,
    LEGACY_START_ROUTING_BUCKET,
};

/// The index directory `engine_on` hands the engine, and therefore where the stamp lives.
fn index_dir_of(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("indexes")
}

/// Load, returning the status rather than asserting it -- the refusal arms need to read it.
fn try_load_on(engine: &TemporalEngine, end_routing_bucket: u32) -> crate::types::Status {
    engine
        .load_shard_with(crate::control::LoadShardRequest {
            shard_id: 1,
            table_name: "routing-range-default".to_string(),
            shard_uri: "local://routing-range-default/1".to_string(),
            start_routing_bucket: 0,
            end_routing_bucket,
            readonly: false,
            load_version: 1,
            local_node_id: Some(1),
        })
        .status
}

/// Every bucket that holds a page, with the page HANDLES it holds. A key set cannot see a lost
/// component; this can.
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

/// A STORE BUILT ON THE OLD DEFAULT IS REFUSED UNDER THE NEW ONE, LOUDLY AND BEFORE THE DECODE.
///
/// THIS IS THE ASSERTION THE DEFAULT MOVE RESTS ON. Measured on the build before the stamp existed:
/// a store of 2,000 routed keys written on the whole keyspace and reopened on `0..1023` came back
/// with 2,000 of 2,000 pages filed in buckets ABOVE the shard's own end -- every record readable,
/// and every page outside the dump's bucket selection, eviction's victim sampling, the reclaim floor
/// and the release pass. Nothing failed, nothing logged, and a per-bucket sweep simply never saw the
/// data again.
///
/// So the refusal is asserted at four levels, because "it returned an error" would hold for the
/// wrong error:
///
///   1. the load is NOT ok;
///   2. the code is exactly `routing_range_mismatch` -- a different code is a different finding;
///   3. the message names BOTH ranges and the stamp file, because an operator who cannot see which
///      two ranges disagree cannot act on it; and
///   4. the shard is NOT in the served map afterwards -- a refusal that still installed the shard
///      would be a warning wearing an error's clothes.
///
/// rust-internal: drives the engine's own load path, no product behaviour
#[test]
fn a_store_built_on_the_old_default_is_refused_under_the_new_default() {
    const RECORDS: usize = 200;
    let dir = tempfile::tempdir().expect("tempdir");

    // PHASE ONE: build it on the old default, the range every store on disk today was built on.
    let keys = {
        let engine = engine_on(dir.path());
        load_on(&engine, LEGACY_END_ROUTING_BUCKET);
        let keys = seed_routed(&engine, RECORDS);
        assert_eq!(
            RECORDS,
            read_back(&engine, &keys),
            "the write arm cannot read its own store, so the refusal below would be over a store \
             that never worked"
        );
        engine.flush_shard_index(1);
        engine.unload_shard(1);
        keys
    };

    // THE FIXTURE'S OWN PRECONDITION: the store really was stamped with the old range. Without
    // this the refusal below could be firing for a missing stamp instead of a disagreeing one.
    let stamped = read_routing_range_stamp(&index_dir_of(dir.path()), 1)
        .expect("phase one stamped the store it created");
    assert_eq!(
        (LEGACY_START_ROUTING_BUCKET, LEGACY_END_ROUTING_BUCKET),
        (stamped.start_routing_bucket, stamped.end_routing_bucket),
        "phase one recorded {stamped:?}, not the old default it was loaded on"
    );

    // PHASE TWO: open it on the new default.
    let engine = engine_on(dir.path());
    let status = try_load_on(&engine, crate::DEFAULT_END_ROUTING_BUCKET);
    println!(
        "  built on 0..{LEGACY_END_ROUTING_BUCKET}, opened on \
         0..{}: ok={} code={} message={}",
        crate::DEFAULT_END_ROUTING_BUCKET,
        status.ok,
        status.code,
        status.message
    );

    assert!(
        !status.ok,
        "a store built on 0..{LEGACY_END_ROUTING_BUCKET} loaded successfully on \
         0..{}. Its pages are filed under nine-figure buckets the shard does not hold; the load \
         has to refuse, because nothing re-files them and every record still reads",
        crate::DEFAULT_END_ROUTING_BUCKET
    );
    assert_eq!(
        "routing_range_mismatch", status.code,
        "the load was refused with code `{}`, not `routing_range_mismatch`. A refusal for another \
         reason is a different finding and this test would be passing on it",
        status.code
    );
    for expected in [
        LEGACY_END_ROUTING_BUCKET.to_string(),
        crate::DEFAULT_END_ROUTING_BUCKET.to_string(),
        "routing-range.json".to_string(),
    ] {
        assert!(
            status.message.contains(&expected),
            "the refusal message does not contain `{expected}`: {}. An operator who cannot see \
             which two ranges disagree, and where the built one is recorded, cannot act on it",
            status.message
        );
    }
    assert!(
        !engine
            .shards
            .read()
            .expect("engine lock poisoned")
            .contains_key(&1),
        "the load was refused and the shard is in the served map anyway, so the refusal is a \
         warning rather than a refusal"
    );

    // AND THE STORE IS STILL LOADABLE ON ITS OWN RANGE -- the refusal is not a brick.
    let status = try_load_on(&engine, LEGACY_END_ROUTING_BUCKET);
    assert!(
        status.ok,
        "the same store could not be reopened on the range it was built on: {status:?}. A refusal \
         that leaves no way back is worse than the silence it replaced"
    );
    assert_eq!(
        RECORDS,
        read_back(&engine, &keys),
        "reopened on its own range, the store served fewer than {RECORDS} records"
    );
}

/// A STORE BUILT ON THE NEW DEFAULT ROUND-TRIPS, WHOLE.
///
/// The partner of the refusal above, and the arm that says the new default is a working
/// configuration rather than one the refusal merely protects. Asserted at three levels: every
/// record readable, every page present HANDLE FOR HANDLE, and every occupied bucket INSIDE the
/// range -- the third because "the records are readable" held even in the silent-stranding state
/// this change exists to remove.
///
/// rust-internal: drives the engine's own restart, no product behaviour
#[test]
fn a_store_built_on_the_new_default_round_trips_whole() {
    const RECORDS: usize = 200;
    let dir = tempfile::tempdir().expect("tempdir");
    let end = crate::DEFAULT_END_ROUTING_BUCKET;

    let (keys, handles_before, buckets_before) = {
        let engine = engine_on(dir.path());
        load_on(&engine, end);
        let keys = seed_routed(&engine, RECORDS);
        assert_eq!(RECORDS, read_back(&engine, &keys), "the write arm cannot read itself back");
        let before = bucket_handle_sets(&engine);
        let handles: BTreeSet<u64> = before.values().flatten().copied().collect();
        assert_eq!(
            RECORDS,
            handles.len(),
            "the store went in holding {} pages for {RECORDS} keys",
            handles.len()
        );
        engine.flush_shard_index(1);
        engine.unload_shard(1);
        (keys, handles, before.len())
    };

    let engine = engine_on(dir.path());
    let status = try_load_on(&engine, end);
    assert!(status.ok, "a store built on 0..{end} could not be reopened on 0..{end}: {status:?}");

    let readable = read_back(&engine, &keys);
    let after = bucket_handle_sets(&engine);
    let handles_after: BTreeSet<u64> = after.values().flatten().copied().collect();
    let outside: Vec<u32> = after.keys().copied().filter(|bucket| *bucket > end).collect();
    println!(
        "  built and reopened on 0..{end}: {readable}/{RECORDS} readable, {} page handles, \
         {buckets_before} buckets -> {} buckets, {} occupied above the shard's end",
        handles_after.len(),
        after.len(),
        outside.len()
    );

    assert_eq!(RECORDS, readable, "{readable} of {RECORDS} records came back");
    assert_eq!(
        handles_before, handles_after,
        "{} page handles went in and {} came back; {} are in one and not the other",
        handles_before.len(),
        handles_after.len(),
        handles_before.symmetric_difference(&handles_after).count()
    );
    assert!(
        outside.is_empty(),
        "{} buckets are occupied above the shard's end on a store built and read on the same \
         range: {outside:?}. Those pages are outside every per-bucket sweep the shard runs",
        outside.len()
    );
    assert!(
        after.len() <= (end as usize) + 1,
        "the store occupies {} buckets, more than the {} the range has",
        after.len(),
        (end as usize) + 1
    );
}

/// A STORE THAT PREDATES THE STAMP IS HONOURED ON THE RANGE IT WAS BUILT ON, NOT REFUSED.
///
/// The third case, and the one that keeps the default move from stopping every existing deployment
/// from starting. A store already on disk today carries no stamp, and its range is not unknown: the
/// whole keyspace was the ONLY default a store could have been built on. So the requested range is
/// OVERRIDDEN with the range the store was built under, the stamp is written so the next load does
/// not have to infer it again, and the override is logged.
///
/// THE PRE-STAMP STORE IS SIMULATED BY DELETING THE STAMP, and the deletion is ASSERTED to have
/// removed a file that was there. A fixture that deleted nothing would be testing the ordinary
/// matching-stamp path under this test's name.
///
/// THIS IS ALSO THE NEGATIVE CONTROL ON THE REFUSAL. The arm above shows the gate firing; this arm
/// shows it NOT firing on an input where it must not, over the same store, the same ranges and the
/// same engine. Without it, a gate that refused every load at all would pass the arm above.
///
/// rust-internal: drives the engine's own load path, no product behaviour
#[test]
fn a_store_that_predates_the_stamp_is_honoured_on_the_range_it_was_built_on() {
    const RECORDS: usize = 200;
    let dir = tempfile::tempdir().expect("tempdir");

    let keys = {
        let engine = engine_on(dir.path());
        load_on(&engine, LEGACY_END_ROUTING_BUCKET);
        let keys = seed_routed(&engine, RECORDS);
        assert_eq!(RECORDS, read_back(&engine, &keys), "the write arm cannot read itself back");
        engine.flush_shard_index(1);
        engine.unload_shard(1);
        keys
    };

    // MAKE IT A PRE-STAMP STORE, and assert the deletion removed something.
    let stamp = routing_range_stamp_path(&index_dir_of(dir.path()), 1);
    assert!(
        stamp.exists(),
        "there is no stamp at {} to delete, so this fixture is not producing a pre-stamp store \
         and every assertion below is about a different case",
        stamp.display()
    );
    std::fs::remove_file(&stamp).expect("the stamp is removable");
    assert!(
        read_routing_range_stamp(&index_dir_of(dir.path()), 1).is_none(),
        "the stamp is still readable after being removed"
    );

    // OPEN IT ASKING FOR THE NEW DEFAULT. It must LOAD, on the OLD range.
    let engine = engine_on(dir.path());
    let status = try_load_on(&engine, crate::DEFAULT_END_ROUTING_BUCKET);
    assert!(
        status.ok,
        "a store with pages on disk and no stamp was refused when asked for \
         0..{}: {status:?}. Refusing here would stop every store written before the stamp existed \
         from ever loading again",
        crate::DEFAULT_END_ROUTING_BUCKET
    );

    let carried = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        shards.get(&1).expect("shard 1 is loaded").routing_range()
    };
    let readable = read_back(&engine, &keys);
    let after = bucket_handle_sets(&engine);
    let outside: Vec<u32> = after
        .keys()
        .copied()
        .filter(|bucket| *bucket > carried.1)
        .collect();
    println!(
        "  pre-stamp store asked for 0..{}: loaded carrying {carried:?}, {readable}/{RECORDS} \
         readable, {} buckets, {} occupied above the carried end",
        crate::DEFAULT_END_ROUTING_BUCKET,
        after.len(),
        outside.len()
    );

    // THE RANGE IT WAS BUILT ON WON, not the one it was asked for.
    assert_eq!(
        (LEGACY_START_ROUTING_BUCKET, LEGACY_END_ROUTING_BUCKET),
        carried,
        "the pre-stamp store came up carrying {carried:?}. It was asked for 0..{}, and honouring \
         that would file every subsequent write in a bucket group the existing pages are not in",
        crate::DEFAULT_END_ROUTING_BUCKET
    );
    assert_eq!(RECORDS, readable, "{readable} of {RECORDS} records came back");
    assert!(
        outside.is_empty(),
        "{} buckets are occupied above the end the shard came up carrying: {outside:?}. The whole \
         point of honouring the built range is that no page is out of range",
        outside.len()
    );

    // AND THE INFERENCE WAS RECORDED, so the next load does not have to make it again.
    let stamped = read_routing_range_stamp(&index_dir_of(dir.path()), 1)
        .expect("the adopted range was recorded");
    assert_eq!(
        (LEGACY_START_ROUTING_BUCKET, LEGACY_END_ROUTING_BUCKET),
        (stamped.start_routing_bucket, stamped.end_routing_bucket),
        "the adoption recorded {stamped:?} rather than the range it adopted"
    );
}

/// A FRESH STORE IS STAMPED WITH THE RANGE IT IS CREATED ON, WHATEVER THAT RANGE IS.
///
/// The fourth case, and the one that makes the other three decidable. Driven at three ranges, one
/// of which is the new default and one of which is the old, so the stamp cannot be a constant.
///
/// rust-internal: drives the engine's own load path, no product behaviour
#[test]
fn a_fresh_store_is_stamped_with_the_range_it_is_created_on() {
    for end_routing_bucket in [
        crate::DEFAULT_END_ROUTING_BUCKET,
        255,
        LEGACY_END_ROUTING_BUCKET,
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(
            read_routing_range_stamp(&index_dir_of(dir.path()), 1).is_none(),
            "a fresh temporary directory already carries a stamp, so this arm is not over a new \
             store"
        );
        let engine = engine_on(dir.path());
        let status = try_load_on(&engine, end_routing_bucket);
        assert!(status.ok, "a new store could not be created on 0..{end_routing_bucket}: {status:?}");
        let stamped = read_routing_range_stamp(&index_dir_of(dir.path()), 1)
            .unwrap_or_else(|| panic!("a new store on 0..{end_routing_bucket} was not stamped"));
        println!("  new store on 0..{end_routing_bucket} stamped {stamped:?}");
        assert_eq!(
            (0, end_routing_bucket),
            (stamped.start_routing_bucket, stamped.end_routing_bucket),
            "a new store created on 0..{end_routing_bucket} was stamped {stamped:?}"
        );
    }
}

/// THE CONVENIENCE LOAD IS DELIBERATELY NOT THE PRODUCTION DEFAULT, AND THAT IS HELD HERE.
///
/// `Engine::load_shard` keeps the whole keyspace while `startup_load_shard_request` now defaults to
/// [`crate::DEFAULT_END_ROUTING_BUCKET`]. The divergence is intended -- several hundred fixtures
/// call the convenience and the range decides which arm of `BlockIndexMap` every one of them
/// exercises -- but an intended divergence and a forgotten one look identical in the source, so it
/// is asserted with the reason attached.
///
/// #1959 measured "almost every bucket holds exactly one page" off a fixture on the convenience
/// load and read it as a property of the workload. At the whole keyspace the modulus is 4.29
/// billion and no workload can do otherwise, which is why this is worth a guard rather than a
/// comment.
///
/// rust-internal: reads the engine's own defaults, no product behaviour
#[test]
fn the_convenience_load_is_deliberately_not_the_production_default() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    engine.load_shard(1);
    let convenience = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        shards.get(&1).expect("shard 1 is loaded").routing_range()
    };
    println!(
        "  Engine::load_shard gives {convenience:?}; the production default end is {}",
        crate::DEFAULT_END_ROUTING_BUCKET
    );
    assert_eq!(
        (LEGACY_START_ROUTING_BUCKET, LEGACY_END_ROUTING_BUCKET),
        convenience,
        "`Engine::load_shard` now gives {convenience:?}. If it has been moved onto the production \
         default, every fixture that calls it has changed which `BlockIndexMap` arm it exercises \
         and each one needs examining rather than retargeting -- and this guard's own doc comment, \
         and `load_shard`'s, are now wrong"
    );
    assert_ne!(
        LEGACY_END_ROUTING_BUCKET,
        crate::DEFAULT_END_ROUTING_BUCKET,
        "the production default is still the whole keyspace, so the divergence this guard records \
         does not exist and the sweep in this module changed nothing"
    );
}

/// THE STAMP DECIDES ON THE RANGE AND NOTHING ELSE -- the unit table for the four cases.
///
/// The three tests above drive the engine end to end, which is the right level for a behaviour
/// change but a coarse one for a four-case decision: each of them exercises one case, and a case
/// that stopped being reachable would simply stop being tested. This drives
/// `decide_routing_range` directly over all four, including the one the engine tests cannot
/// easily produce -- a stamp present, disagreeing, over a store with NO on-disk state -- and
/// asserts the ORDER of the cases: a disagreeing stamp refuses whether or not there are pages.
///
/// rust-internal: drives the engine's own decision function, no product behaviour
#[test]
fn the_routing_range_decision_covers_its_four_cases_in_the_right_order() {
    use crate::engine::routing_range_stamp::{
        decide_routing_range, store_has_on_disk_state, write_routing_range_stamp,
        RoutingRangeDecision, RoutingRangeStamp,
    };

    // CASE 4: nothing on disk at all -- a new store takes the range it is asked for.
    let dir = tempfile::tempdir().expect("tempdir");
    let index_dir = dir.path().join("indexes");
    std::fs::create_dir_all(&index_dir).expect("index dir");
    assert!(
        !store_has_on_disk_state(&index_dir, 1),
        "an empty index directory reports on-disk state, so every case below is misclassified"
    );
    assert_eq!(
        RoutingRangeDecision::Load {
            start_routing_bucket: 0,
            end_routing_bucket: 1023,
            write_stamp: true,
            adopted_legacy: false,
        },
        decide_routing_range(&index_dir, 1, 0, 1023),
        "a new store was not given the range it asked for"
    );

    // CASE 3: on-disk state, no stamp -- the built range is honoured and the request overridden.
    std::fs::write(index_dir.join("shard-1.index.json"), b"{}").expect("write a base index");
    assert!(
        store_has_on_disk_state(&index_dir, 1),
        "a base index file on disk is not reported as on-disk state"
    );
    assert_eq!(
        RoutingRangeDecision::Load {
            start_routing_bucket: LEGACY_START_ROUTING_BUCKET,
            end_routing_bucket: LEGACY_END_ROUTING_BUCKET,
            write_stamp: true,
            adopted_legacy: true,
        },
        decide_routing_range(&index_dir, 1, 0, 1023),
        "a pre-stamp store with pages was not honoured on the range it was built on"
    );
    // And asking for the legacy range is the SAME decision without the loud override, because
    // nothing was overridden.
    assert_eq!(
        RoutingRangeDecision::Load {
            start_routing_bucket: LEGACY_START_ROUTING_BUCKET,
            end_routing_bucket: LEGACY_END_ROUTING_BUCKET,
            write_stamp: true,
            adopted_legacy: false,
        },
        decide_routing_range(&index_dir, 1, LEGACY_START_ROUTING_BUCKET, LEGACY_END_ROUTING_BUCKET),
        "a pre-stamp store asked for the range it was built on reported an override anyway; \
         `adopted_legacy` is what the caller logs, and logging it here would cry wolf on every \
         ordinary start"
    );

    // CASE 1: a stamp that agrees -- load, write nothing.
    write_routing_range_stamp(
        &index_dir,
        1,
        RoutingRangeStamp {
            start_routing_bucket: 0,
            end_routing_bucket: 1023,
        },
    )
    .expect("stamp is writable");
    assert_eq!(
        RoutingRangeDecision::Load {
            start_routing_bucket: 0,
            end_routing_bucket: 1023,
            write_stamp: false,
            adopted_legacy: false,
        },
        decide_routing_range(&index_dir, 1, 0, 1023),
        "an agreeing stamp did not load cleanly"
    );

    // CASE 2: a stamp that disagrees -- REFUSE. Both directions, because narrowing and widening
    // are not the same hazard and neither is safe: widening leaves pages inside the new range but
    // routes every new write to a different bucket than a re-read would compute from the key.
    for (requested_start, requested_end) in [(0, 255), (0, LEGACY_END_ROUTING_BUCKET), (1, 1023)] {
        match decide_routing_range(&index_dir, 1, requested_start, requested_end) {
            RoutingRangeDecision::Refuse { message } => {
                assert!(
                    message.contains("1023") && message.contains(&requested_end.to_string()),
                    "the refusal for {requested_start}..{requested_end} names neither the stamped \
                     range nor the requested one: {message}"
                );
            }
            other => panic!(
                "a stamp of 0..1023 did not refuse a request for \
                 {requested_start}..{requested_end}: {other:?}"
            ),
        }
    }

    // AND A DISAGREEING STAMP REFUSES EVEN WITH NO PAGES ON DISK -- the order of the cases, which
    // the engine tests above cannot show. If the on-disk-state check ran first, an empty store
    // would silently adopt whatever it was asked for and lose its claim on the range.
    std::fs::remove_file(index_dir.join("shard-1.index.json")).expect("removable");
    assert!(
        !store_has_on_disk_state(&index_dir, 1),
        "the base index is still reported after being removed"
    );
    assert!(
        matches!(
            decide_routing_range(&index_dir, 1, 0, 255),
            RoutingRangeDecision::Refuse { .. }
        ),
        "with no pages on disk a disagreeing stamp stopped refusing, so the stamp check does not \
         run before the on-disk-state check"
    );
}
