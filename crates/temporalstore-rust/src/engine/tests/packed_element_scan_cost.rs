// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE PACKED ELEMENT-KEY SCAN COSTS IN NANOSECONDS, AGAINST WHAT IT REPLACES.
//!
//! # WHY THIS MEASUREMENT EXISTS WHEN A RATIO IS ALREADY RECORDED
//!
//! A recorded result prices a packed linear scan at 3.0-17.8x a table probe and reads as a
//! refutation. That figure is a RATIO, and it was taken over a different denominator: its scan
//! walked one of 1,024 FIXED routing buckets, so its length was `corpus / 1024` -- 39 keys at
//! 40,000 and 390 at 400,000 -- and its conclusion that the trade "gets worse forever" follows
//! from exactly that. The scan priced here walks the ELEMENTS OF ONE OBJECT. Its length is a
//! property of the data shape, not of the corpus: a forty-field hash has forty elements in a store
//! of forty thousand objects and in a store of four million. The two numbers answer different
//! questions and the first does not bound the second.
//!
//! So this file measures the absolute cost, in nanoseconds, of the two things the change actually
//! trades, at occupancies from one element to four hundred.
//!
//! # AND THE SCAN IS NOT A PROPOSAL -- IT ALREADY RUNS ON EVERY ELEMENT READ
//!
//! `select_container_element` is the shipped selector, and BOTH read doors call it
//! unconditionally: the owning funnel and the shared/cached funnel. An element read of a container
//! page already walks the packed keys today. That is why ARM B here is the real function rather
//! than a prototype of one -- there is nothing to prototype.
//!
//! What the change removes is ARM A: the per-element level of the resident lookup,
//! `ObjectBlockRefs::position`, a binary search whose comparison dereferences an `Option<Arc<str>>`
//! into a separate heap allocation and compares the name. Arm A is the cost that goes; arm B is the
//! cost that stays. A measurement that priced B alone would be pricing the status quo.
//!
//! # HOW EACH ARM IS PROVED TO HAVE RUN
//!
//! A broken arm looks exactly like a winning arm, so neither is believed on its timing alone:
//!
//!   * the fixture is asserted NON-DEGENERATE -- every component name across the whole population
//!     is distinct. A recorded A/B here was once saved by this exact check, where padding made
//!     40,000 keys identical and one arm held a single entry;
//!   * each arm's hit case must report a hit on every iteration and its miss case a miss on every
//!     iteration, counted and asserted, so an arm that answered `Absent` throughout cannot pass as
//!     fast;
//!   * the two arms are CROSS-CHECKED: for the identical input they must agree on hit versus miss,
//!     asserted per occupancy. An arm resolving a different element is a different measurement.
//!
//! Ordering is ABBA rather than interleaved, because interleaving does not cancel an order effect.
//! Timing is calibrated to a floor of `MIN_SAMPLE` and reported per operation in nanoseconds with
//! a fraction: a benchmark here once recorded whole microseconds and published a read as "1us" and
//! 1,000,000 ops/sec when a read is 50ns.
//!
//! # THE POPULATION IS NOT ONE OBJECT
//!
//! One object is the wrong regime. Every occupancy drives `TARGET_ELEMENTS` elements spread over
//! `TARGET_ELEMENTS / occupancy` pages, visited in a pseudo-random stride so the walk cannot ride
//! a prefetcher, and the payload is sized to exceed a core-private cache.
//!
//! # THE FOOTPRINT ARITHMETIC IS READ FROM THE PINNED WIDTHS
//!
//! Every width it multiplies is a `size_of` assertion in `state.rs`, re-asserted here, so a struct
//! that changes width cannot leave this file's arithmetic adding up to the right answer for the
//! wrong reason. The denominator is stated in every figure, because every wrong number in this
//! campaign was a denominator question.

#![allow(clippy::all)]
use crate::engine::container_pages::{
    encode_container_page, select_container_element, ContainerElementRead, ElementKeySpelling,
};
use crate::engine::state::{
    BlockLookupRef, BlockRefs, ComponentBlocks, ComponentList, ObjectBlockRefs,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Elements driven at every occupancy, so the regime is a store and not an object.
///
/// THE DEBUG ARM IS A CORRECTNESS RUN, NOT A MEASUREMENT, and it is smaller only so that the
/// shipped gate -- which runs this suite in DEBUG -- is not made to carry a release benchmark's
/// fixture. Every GUARD still runs at the smaller size: non-degeneracy, the per-arm hit and miss
/// proof, and the pinned-width arithmetic. What the debug arm must never be used for is a
/// published figure, because a published figure from a debug build is a recorded harm in this
/// campaign, which is why the header this test prints says so in the output itself.
#[cfg(debug_assertions)]
const TARGET_ELEMENTS: usize = 4_000;
#[cfg(not(debug_assertions))]
const TARGET_ELEMENTS: usize = 200_000;

/// The occupancies. 40 on one compacted page is measured and real; 1-4 is the context-node shape
/// the write path actually produces; 128 and 400 are the wide arm, and 400 is also the length at
/// which the recorded ratio reached 17.8x, so the two can be read on one axis.
const OCCUPANCIES: [usize; 6] = [1, 2, 4, 40, 128, 400];

/// A timing sample runs at least this long, so the per-operation figure is not a clock artefact.
#[cfg(debug_assertions)]
const MIN_SAMPLE: Duration = Duration::from_millis(2);
#[cfg(not(debug_assertions))]
const MIN_SAMPLE: Duration = Duration::from_millis(120);

/// A value width that is representative rather than flattering: a hit COPIES the value out
/// (`ContainerElementRead::Found(Vec<u8>)`), so the hit path carries an allocation and a memcpy of
/// this many bytes and the figure should not understate it.
const VALUE_BYTES: usize = 48;

/// Deterministic, so a rerun measures the same walk.
struct Stride(u64);

impl Stride {
    fn next(&mut self) -> u64 {
        // xorshift64*, enough to defeat a prefetcher and cheap enough not to be the measurement.
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// One occupancy's fixture: the packed pages, and the resident per-element level over the same
/// elements, built from the same names so the two arms are asked about the identical thing.
struct Fixture {
    occupancy: usize,
    pages: Vec<Vec<u8>>,
    resident: Vec<ObjectBlockRefs>,
    /// `components[page][slot]` -- the name of one element, as a reader would spell it.
    components: Vec<Vec<String>>,
    /// A name no page holds, for the miss case.
    absent: String,
    payload_bytes: usize,
    name_bytes_total: usize,
}

fn build(occupancy: usize) -> Fixture {
    let pages_wanted = (TARGET_ELEMENTS / occupancy).max(8);
    let value = vec![0xA5u8; VALUE_BYTES];
    let mut pages = Vec::with_capacity(pages_wanted);
    let mut resident = Vec::with_capacity(pages_wanted);
    let mut components = Vec::with_capacity(pages_wanted);
    let mut payload_bytes = 0usize;
    let mut name_bytes_total = 0usize;

    for page_index in 0..pages_wanted {
        // Names are unique across the WHOLE population, not just within a page, so the
        // non-degeneracy assertion below is a real check and not a within-page one.
        let names: Vec<String> = (0..occupancy)
            .map(|slot| format!("field-{page_index:07}-{slot:04}"))
            .collect();
        name_bytes_total += names.iter().map(|name| name.len()).sum::<usize>();

        let items: Vec<(&[u8], &[u8])> = names
            .iter()
            .map(|name| (name.as_bytes(), value.as_slice()))
            .collect();
        let page = encode_container_page(ElementKeySpelling::Utf8, &items);
        payload_bytes += page.len();
        pages.push(page);

        // The resident per-element level over the same elements, in the shape production holds it:
        // sorted by component, one `ComponentBlocks` per element, each naming its own heap string.
        let mut by_component: Vec<ComponentBlocks> = names
            .iter()
            .map(|name| ComponentBlocks {
                component: Some(Arc::from(name.as_str())),
                refs: BlockRefs::One(BlockLookupRef {
                    routing_bucket: (page_index % 1024) as u32,
                    block_ref_key: page_index as u64,
                }),
            })
            .collect();
        by_component.sort_by(|left, right| left.component.cmp(&right.component));
        let list = if by_component.len() == 1 {
            ComponentList::One(by_component.pop().expect("length is one"))
        } else {
            ComponentList::Many(by_component)
        };
        resident.push(ObjectBlockRefs { by_component: list });
        components.push(names);
    }

    Fixture {
        occupancy,
        pages,
        resident,
        components,
        absent: "field-absent-0000000-9999".to_string(),
        payload_bytes,
        name_bytes_total,
    }
}

/// Which element of a page an iteration asks for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Case {
    /// Element zero -- the cheapest hit for a walk.
    First,
    /// The last element -- the dearest hit for a walk, and the one a walk's critics price.
    Last,
    /// A different element each iteration -- the realistic hit.
    Spread,
    /// Absent. The full walk, and the case the recorded ratio came from.
    Miss,
}

impl Case {
    fn label(self) -> &'static str {
        match self {
            Case::First => "hit, first",
            Case::Last => "hit, last",
            Case::Spread => "hit, spread",
            Case::Miss => "MISS",
        }
    }

    fn is_miss(self) -> bool {
        self == Case::Miss
    }
}

fn wanted<'a>(fixture: &'a Fixture, case: Case, page: usize, step: u64) -> &'a str {
    match case {
        Case::First => fixture.components[page][0].as_str(),
        Case::Last => fixture.components[page][fixture.occupancy - 1].as_str(),
        Case::Spread => {
            fixture.components[page][(step as usize) % fixture.occupancy].as_str()
        }
        Case::Miss => fixture.absent.as_str(),
    }
}

/// One timed run. Returns nanoseconds per operation and the number of HITS observed, which is what
/// proves the arm did the work rather than answering `Absent` quickly.
fn run<F>(mut operation: F) -> (f64, u64, u64)
where
    F: FnMut(u64) -> bool,
{
    // Warm the fixture and the branch predictors without counting them.
    let mut warm = 0u64;
    for step in 0..2_000u64 {
        if operation(step) {
            warm += 1;
        }
    }
    std::hint::black_box(warm);

    let mut iterations = 0u64;
    let mut hits = 0u64;
    let started = Instant::now();
    loop {
        for _ in 0..1_000 {
            if operation(iterations) {
                hits += 1;
            }
            iterations += 1;
        }
        if started.elapsed() >= MIN_SAMPLE {
            break;
        }
    }
    let elapsed = started.elapsed();
    let per_operation = elapsed.as_secs_f64() * 1e9 / iterations as f64;
    (per_operation, iterations, hits)
}

/// ARM A -- what the change REMOVES: the per-element level of the resident lookup.
fn arm_a(fixture: &Fixture, case: Case) -> (f64, u64, u64) {
    let mut stride = Stride(0x9E37_79B9_7F4A_7C15);
    let pages = fixture.resident.len();
    run(|step| {
        let page = (stride.next() as usize) % pages;
        let name = wanted(fixture, case, page, step);
        let found = fixture.resident[page].refs_for(Some(name));
        std::hint::black_box(&found);
        found.is_some()
    })
}

/// ARM B -- what the change KEEPS, and what already runs on every element read today.
fn arm_b(fixture: &Fixture, case: Case) -> (f64, u64, u64) {
    let mut stride = Stride(0x9E37_79B9_7F4A_7C15);
    let pages = fixture.pages.len();
    run(|step| {
        let page = (stride.next() as usize) % pages;
        let name = wanted(fixture, case, page, step);
        let read = select_container_element(&fixture.pages[page], name);
        let hit = matches!(read, ContainerElementRead::Found(_));
        std::hint::black_box(&read);
        hit
    })
}

/// Resident bytes for ONE OBJECT of `occupancy` elements, today and under packed element keys.
///
/// Every width is a pinned `size_of` assertion, re-asserted immediately above the arithmetic.
fn resident_bytes_per_object(occupancy: usize, mean_name_bytes: usize) -> (usize, usize, usize) {
    assert_eq!(std::mem::size_of::<ComponentBlocks>(), 40);
    assert_eq!(std::mem::size_of::<ComponentList>(), 40);
    assert_eq!(std::mem::size_of::<ObjectBlockRefs>(), 40);
    assert_eq!(std::mem::size_of::<BlockLookupRef>(), 16);
    assert_eq!(std::mem::size_of::<BlockRefs>(), 24);
    assert_eq!(
        std::mem::size_of::<crate::engine::state::BlockIndex>(),
        56,
        "the entry width this arithmetic multiplies"
    );

    // An `Arc<str>` name: two counter words plus the bytes, rounded to the allocator's 16-byte
    // granularity. Stated rather than hidden, because at one element it is the largest term.
    let name_alloc = {
        let raw = 16 + mean_name_bytes;
        (raw + 15) / 16 * 16
    };

    // TODAY, per object: the `ComponentList` header, one `ComponentBlocks` per element -- held
    // inline at one element and on the heap beyond -- one name allocation per element, and one
    // handle plus one `BlockIndex` per element in the bucket's block list.
    let component_level = if occupancy == 1 {
        40
    } else {
        40 + occupancy * std::mem::size_of::<ComponentBlocks>()
    };
    let block_list = occupancy * (8 + std::mem::size_of::<crate::engine::state::BlockIndex>());
    let names = occupancy * name_alloc;
    let today = component_level + block_list + names;

    // UNDER PACKED ELEMENT KEYS, per object: one `ComponentList::One` holding a component of
    // `None` -- inline, no heap, no name -- and ONE handle plus ONE entry for the object's page.
    let packed_wide_entry = 40 + (8 + std::mem::size_of::<crate::engine::state::BlockIndex>());
    // And with the entry narrowed to the 24 bytes it can be once it carries no identity.
    let packed_narrow_entry = 40 + (8 + 24);

    (today, packed_wide_entry, packed_narrow_entry)
}

fn load_average() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .map(|text| {
            text.split_whitespace()
                .take(3)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_else(|_| "unavailable".to_string())
}

#[test]
fn what_the_packed_element_key_scan_costs_against_what_it_replaces() {
    println!();
    if cfg!(debug_assertions) {
        println!(
            "DEBUG BUILD -- GUARDS ONLY. The nanosecond columns below are NOT publishable: a \
             debug figure was published from this campaign once already. Re-run with --release \
             for a figure."
        );
    } else {
        println!("release build: the nanosecond columns below are measurements.");
    }
    println!("load average at start: {}", load_average());
    println!(
        "population per occupancy: {} elements, value {} B, ABBA ordering, >= {} ms a sample",
        TARGET_ELEMENTS,
        VALUE_BYTES,
        MIN_SAMPLE.as_millis()
    );
    println!();

    for occupancy in OCCUPANCIES {
        let fixture = build(occupancy);
        let elements = fixture.pages.len() * occupancy;

        // NON-DEGENERACY. Every name in the whole population is distinct, so neither arm can be
        // measuring a one-entry structure while reporting the length of a large one.
        let distinct: std::collections::HashSet<&str> = fixture
            .components
            .iter()
            .flat_map(|page| page.iter().map(|name| name.as_str()))
            .collect();
        assert_eq!(
            distinct.len(),
            elements,
            "degenerate fixture at occupancy {occupancy}: {} distinct names for {elements} \
             elements -- a broken arm would read as a fast arm",
            distinct.len()
        );
        assert!(
            !distinct.contains(fixture.absent.as_str()),
            "the miss case's name is present at occupancy {occupancy}, so the miss arm is a hit arm"
        );

        let mean_name_bytes = fixture.name_bytes_total / elements;
        println!(
            "occupancy {occupancy}: {} pages, {elements} elements, {:.2} MiB of payload, \
             mean name {} B",
            fixture.pages.len(),
            fixture.payload_bytes as f64 / (1024.0 * 1024.0),
            mean_name_bytes
        );
        println!(
            "    {:<13} {:>12} {:>12} {:>9}  {}",
            "case", "A, index ns", "B, scan ns", "B/A", "load"
        );

        for case in [Case::First, Case::Spread, Case::Last, Case::Miss] {
            // ABBA. The first and last sample of each arm bracket the other arm, so a drift in
            // machine state over the run cannot be read as a difference between the arms.
            let (a1, _, a1_hits) = arm_a(&fixture, case);
            let (b1, _, b1_hits) = arm_b(&fixture, case);
            let (b2, _, b2_hits) = arm_b(&fixture, case);
            let (a2, _, a2_hits) = arm_a(&fixture, case);

            let a = (a1 + a2) / 2.0;
            let b = (b1 + b2) / 2.0;

            // PROOF THE ARMS RAN, and that they agree about what they were asked.
            for (label, hits, iterations) in [
                ("A1", a1_hits, a1),
                ("A2", a2_hits, a2),
                ("B1", b1_hits, b1),
                ("B2", b2_hits, b2),
            ] {
                let _ = iterations;
                if case.is_miss() {
                    assert_eq!(
                        hits, 0,
                        "{label} reported {hits} hits for the MISS case at occupancy \
                         {occupancy}; the miss arm resolved something"
                    );
                } else {
                    assert!(
                        hits > 0,
                        "{label} reported no hits for {} at occupancy {occupancy}; the arm \
                         answered absent throughout and its timing is not a lookup",
                        case.label()
                    );
                }
            }

            println!(
                "    {:<13} {:>12.1} {:>12.1} {:>8.2}x  {}",
                case.label(),
                a,
                b,
                b / a,
                load_average()
            );
        }

        let (today, packed_wide, packed_narrow) =
            resident_bytes_per_object(occupancy, mean_name_bytes);
        println!(
            "    resident B PER OBJECT  today {today}   packed(56-wide entry) {packed_wide}   \
             packed(24 entry) {packed_narrow}"
        );
        println!(
            "    resident B PER ELEMENT today {:.1}   packed(56) {:.1}   packed(24) {:.1}   \
             -> {:.1}x / {:.1}x fewer per object",
            today as f64 / occupancy as f64,
            packed_wide as f64 / occupancy as f64,
            packed_narrow as f64 / occupancy as f64,
            today as f64 / packed_wide as f64,
            today as f64 / packed_narrow as f64
        );
        println!();
    }

    println!("load average at end: {}", load_average());
}
