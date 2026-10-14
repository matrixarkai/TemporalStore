// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A SERIES REPRESENTATION COSTS PER KEY, READ FROM THE ALLOCATOR AND NOT FROM THE STRUCTURE.
//!
//! # THE DECISION THIS IS FOR
//!
//! Eight resident maps hold `object key -> (timestamp -> page address)` as
//! `HashMap<String, BTreeMap<u64, ElementEntry>>`, and the campaign to move them off the per-key
//! B-tree has to choose what to move them TO. Two decisions hide inside that one: the CONTAINER
//! (tree against flat array) and, if an array, its GROWTH POLICY (amortised doubling against exact
//! capacity). Folding them into a single comparison hides the larger of the two, so they are
//! measured as separate arms here.
//!
//! This module takes no position on which maps should move, and converts none of them. It prices
//! the candidate shapes so that the choice is made on a reading rather than on arithmetic.
//!
//! # WHY IT IS MEASURED AT THE ALLOCATOR AND NOT FROM `size_of`
//!
//! `size_of` sees the owning field and nothing else, and an allocation count is not a count of
//! BYTES. An allocator serves a request out of a chunk at least as large and usually larger, so a
//! shape that moves bytes out of line pays a header word and the rounding on every allocation, and
//! neither `size_of` nor a call count can see any of the three. The largest single effect in the
//! table below -- a `Vec` whose first allocation holds four elements when one was asked for -- is
//! invisible to both.
//!
//! # WHAT THE INSTRUMENT IS, STATED SO IT IS NOT "CORRECTED" LATER
//!
//! `ALLOC_CHUNK_BYTES` is `malloc_usable_size(ptr) + size_of::<usize>()`. Those are not two
//! candidate instruments to choose between: the chunk column ALREADY adds the header word that a
//! bare usable-size reading omits, so "chunk bytes" and "usable size plus the header" are the same
//! number here and the eight bytes per live allocation are inside it. `ALLOC_BYTES`, the request
//! column, is read beside it as a control, because the difference between the two IS what an
//! out-of-line shape pays and an inline one does not -- and an equality between them would mean
//! the chunk column is not reading the allocator at all.
//!
//! # WHY ELEVEN OCCUPANCIES AND NOT ONE
//!
//! The curves have DIFFERENT STEPS, so any single occupancy describes one regime and misdescribes
//! the others. A `BTreeMap` leaf holds up to eleven pairs before it splits; a doubling `Vec` steps
//! at every power of two. So 9, 10 and 11 are in the set because that is where the tree leaf is
//! nearly full and the doubling array has just overshot -- the one band where a growing array can
//! LOSE outright -- and 65 and 129 sit just past a doubling. A set that skipped them would report
//! a uniform win that is not uniform.
//!
//! # THE PER-ROW ABSOLUTE IS NOT A PROPERTY OF THE REPRESENTATION
//!
//! Bytes per row is a function of the OCCUPANCY DISTRIBUTION as much as of the shape: the same
//! total number of points, redistributed, moves it substantially. So what this table publishes is
//! the RATIO between shapes AT A STATED OCCUPANCY, and every absolute is labelled with the
//! occupancy it was read at. A single per-row figure quoted without its occupancy is the
//! denominator error this table exists to prevent, and three figures that were circulating in
//! earlier prose do not reproduce here at all.

use crate::block_store::ElementEntry;
use std::collections::BTreeMap;

/// A legal address. `ElementEntry` has no public constructor and `from_parts` refuses a block id
/// wider than `u16::MAX` rather than truncating, so the id is masked as the other address fixtures
/// in this crate mask it.
fn address(i: u64) -> ElementEntry {
    ElementEntry::from_parts(1, i * 64, 64, Some(i & u64::from(u16::MAX)), Some(i))
}

/// The pair a timestamped series stores: one instant and one page address.
type Point = (u64, ElementEntry);

/// The occupancies, and why each is here. 9/10/11 bracket the B-tree leaf split and are where a
/// doubling array can lose; 65 and 129 sit just past a doubling; 1 is the regime with the most
/// keys; 1000 is the steady state.
#[cfg(feature = "alloc-probe")]
const OCCUPANCIES: [usize; 11] = [1, 2, 5, 9, 10, 11, 12, 65, 100, 129, 1000];

/// Which arm a column holds. Named rather than indexed so a row cannot be read off by position.
#[cfg(feature = "alloc-probe")]
const ARMS: [&str; 5] = [
    "tree_insert",
    "tree_collect",
    "array_doubling",
    "array_exact",
    "array_hybrid",
];

/// WHAT ONE KEY SERIES COSTS IN FIVE SHAPES -- chunk bytes AND allocation count, at eleven
/// occupancies, read from the allocator.
///
/// # THE DENOMINATOR IS ONE KEY
///
/// Every figure is for a SINGLE object key series, measured on the series object itself with no
/// `HashMap` in the span. That is the per-key cost a representation change moves. The inline width
/// of each shape is reported separately and added in, because a shape that is cheaper on the heap
/// can be wider in the map value and the total is what a shard pays.
///
/// rust-internal: measures this crate own resident structures, no product behaviour
#[cfg(feature = "alloc-probe")]
#[test]
fn what_one_keys_series_costs_in_five_shapes_at_the_allocator() {
    // NON-VACUITY FIRST, because every row after this is a difference of counter readings and a
    // counter that never moves reports a table of zeros that looks like a free change.
    let probe = crate::alloc_probe::Probe::start();
    let canary: Vec<u64> = (0..64).collect();
    let moved = probe.stop();
    std::hint::black_box(&canary);
    assert!(
        moved.allocs >= 1 && moved.chunk_bytes >= 1,
        "the counting allocator is not installed -- every row below would read zero, and a zero \
         here means the opposite of a free change (allocs {} chunk_bytes {})",
        moved.allocs,
        moved.chunk_bytes
    );
    // AND THE CHUNK COLUMN MUST READ STRICTLY ABOVE THE REQUEST COLUMN for this canary, or it is
    // not reading the allocator and every out-of-line arm below is being charged at request size,
    // which systematically flatters exactly the arms this table exists to separate.
    assert!(
        moved.chunk_bytes > moved.alloc_bytes,
        "the chunk column must read ABOVE the request column -- equality means it is not reading \
         malloc_usable_size and the instrument is not what it says (chunk {} request {})",
        moved.chunk_bytes,
        moved.alloc_bytes
    );

    // THE INLINE WIDTHS, WHICH ARE THE HALF THE HEAP TABLE CANNOT SEE. `Box<[T]>` is a FAT
    // pointer: exact capacity costs eight more bytes in the map value than a boxed `Vec` does,
    // and buys back more than that on the heap. Both halves are reported so neither can be quoted
    // alone.
    let inline_tree = std::mem::size_of::<BTreeMap<u64, ElementEntry>>();
    let inline_doubling = std::mem::size_of::<Option<Box<Vec<Point>>>>();
    let inline_exact = std::mem::size_of::<Option<Box<[Point]>>>();
    println!(
        "instrument: ALLOC_CHUNK_BYTES = malloc_usable_size + {} B header; canary {} B requested, \
         {} B set aside in {} allocation(s)",
        std::mem::size_of::<usize>(),
        moved.alloc_bytes,
        moved.chunk_bytes,
        moved.allocs
    );
    println!("point = (u64, ElementEntry) = {} B", std::mem::size_of::<Point>());
    println!(
        "inline width in the map value: tree {inline_tree} B, boxed-Vec array {inline_doubling} B, \
         exact owned slice {inline_exact} B"
    );
    println!("arms: {ARMS:?}\n");

    println!(
        "{:>6} | {:>11} {:>4} | {:>12} {:>4} | {:>14} {:>4} | {:>11} {:>4} | {:>12} {:>4}",
        "points",
        "tree_ins_B",
        "n",
        "tree_coll_B",
        "n",
        "arr_double_B",
        "n",
        "arr_exact_B",
        "n",
        "arr_hybrid_B",
        "n"
    );

    let mut rows: Vec<(usize, [u64; 5], [u64; 5])> = Vec::new();

    for n in OCCUPANCIES {
        // The fixture is built OUTSIDE every span: constructing it allocates, and that allocation
        // belongs to no arm.
        let points: Vec<Point> = (0..n as u64).map(|at| (at, address(at))).collect();

        // ARM 1: the B-tree as a live append-mostly series fills it, and as `serde` fills it on a
        // decode -- one ascending insert at a time.
        let probe = crate::alloc_probe::Probe::start();
        let mut tree_ins: BTreeMap<u64, ElementEntry> = BTreeMap::new();
        for (at, entry) in &points {
            tree_ins.insert(*at, entry.clone());
        }
        let tree_ins_counts = probe.stop();

        // ARM 2: the same B-tree built by `collect`. This arm is here because an earlier version
        // of this module ASSUMED `collect` packs the leaves and is therefore the tree at its
        // cheapest. THE MEASUREMENT REFUTES THAT at every occupancy in this table -- it costs more
        // than the ascending inserts, not less -- so the arm is kept precisely to carry the
        // correction, and the cheapest tree arm is computed below rather than assumed.
        let probe = crate::alloc_probe::Probe::start();
        let tree_coll: BTreeMap<u64, ElementEntry> = points.iter().cloned().collect();
        let tree_coll_counts = probe.stop();

        // ARM 3: AN AMORTISED-DOUBLING OWNED ARRAY -- `Option<Box<Vec<Point>>>`, filled by
        // ascending appends. This is the payload shape of the owned series type on the open
        // conversion branch, measured as the SHAPE rather than through that type, so this table
        // stands on `main` and can inform that branch before it merges rather than after.
        let probe = crate::alloc_probe::Probe::start();
        let mut doubling: Option<Box<Vec<Point>>> = None;
        for point in &points {
            doubling
                .get_or_insert_with(|| Box::new(Vec::new()))
                .push(point.clone());
        }
        let doubling_counts = probe.stop();

        // ARM 4: THE FLOOR -- an exact-capacity owned slice, `Option<Box<[Point]>>`. `to_vec`
        // allocates exactly `len`, so `into_boxed_slice` does not reallocate and the reading is
        // the resident cost of a buffer holding n points and nothing spare. This is a FOOTPRINT
        // floor and not an append path: the churn an exact-growth policy would pay is arm 5.
        let probe = crate::alloc_probe::Probe::start();
        let exact: Option<Box<[Point]>> = Some(points.as_slice().to_vec().into_boxed_slice());
        let exact_counts = probe.stop();

        // ARM 5: THE HYBRID ALREADY PRESENT IN THIS TREE. `HashFieldMap` is a sorted owned array with
        // `EXACT_GROWTH_BELOW: usize = 8` -- exact capacity below eight entries, amortised above --
        // so the policy is measured here rather than proposed. Its allocation column is where the
        // footprint-for-churn trade is paid, and the band just above the threshold is where it
        // stops helping.
        let probe = crate::alloc_probe::Probe::start();
        let mut hybrid_buf: Vec<Point> = Vec::new();
        for point in &points {
            if hybrid_buf.len() < 8 {
                hybrid_buf.reserve_exact(1);
            }
            hybrid_buf.push(point.clone());
        }
        let hybrid: Option<Box<Vec<Point>>> = Some(Box::new(hybrid_buf));
        let hybrid_counts = probe.stop();

        // Held live across every stop so nothing above was freed inside a span.
        std::hint::black_box((&tree_ins, &tree_coll, &doubling, &exact, &hybrid));

        // DENOMINATORS, CHECKED PER ARM. An arm holding a different number of points would make
        // every ratio in this table a comparison of two different things.
        assert_eq!(n, tree_ins.len(), "denominator: {n} points, tree_insert");
        assert_eq!(n, tree_coll.len(), "denominator: {n} points, tree_collect");
        assert_eq!(
            n,
            doubling.as_ref().map_or(0, |v| v.len()),
            "denominator: {n} points, array_doubling"
        );
        assert_eq!(
            n,
            exact.as_ref().map_or(0, |s| s.len()),
            "denominator: {n} points, array_exact"
        );
        assert_eq!(
            n,
            hybrid.as_ref().map_or(0, |v| v.len()),
            "denominator: {n} points, array_hybrid"
        );

        let bytes = [
            tree_ins_counts.chunk_bytes,
            tree_coll_counts.chunk_bytes,
            doubling_counts.chunk_bytes,
            exact_counts.chunk_bytes,
            hybrid_counts.chunk_bytes,
        ];
        let allocs = [
            tree_ins_counts.allocs,
            tree_coll_counts.allocs,
            doubling_counts.allocs,
            exact_counts.allocs,
            hybrid_counts.allocs,
        ];
        println!(
            "{:>6} | {:>11} {:>4} | {:>12} {:>4} | {:>14} {:>4} | {:>11} {:>4} | {:>12} {:>4}",
            n, bytes[0], allocs[0], bytes[1], allocs[1], bytes[2], allocs[2], bytes[3], allocs[3],
            bytes[4], allocs[4]
        );
        rows.push((n, bytes, allocs));
    }

    // -------------------------------------------------------------------------------------------
    // AGAINST THE CHEAPEST TREE ARM, COMPUTED AND NOT ASSUMED, with the inline width added in so
    // the fat-pointer cost of exact capacity is charged to it.
    // -------------------------------------------------------------------------------------------
    println!(
        "\n{:>6} | {:>9} | {:>12} {:>12} | {:>11} {:>11}",
        "points", "tree_min_B", "total_dbl_B", "total_exa_B", "tree/dbl", "tree/exact"
    );
    for (n, bytes, _) in &rows {
        let tree_min = bytes[0].min(bytes[1]) + inline_tree as u64;
        let total_dbl = bytes[2] + inline_doubling as u64;
        let total_exa = bytes[3] + inline_exact as u64;
        println!(
            "{:>6} | {:>9} | {:>12} {:>12} | {:>10.2}x {:>10.2}x",
            n,
            tree_min,
            total_dbl,
            total_exa,
            tree_min as f64 / total_dbl as f64,
            tree_min as f64 / total_exa as f64
        );
    }

    // WHERE EACH ARRAY ARM FAILS TO BEAT THE CHEAPEST TREE ARM. Reported per arm and per
    // occupancy rather than collapsed, because the doubling arm has such a band and the exact arm
    // is claimed not to.
    let loses = |arm: usize| -> Vec<usize> {
        rows.iter()
            .filter(|(_, bytes, _)| {
                let tree_min = bytes[0].min(bytes[1]) + inline_tree as u64;
                let inline = if arm == 3 { inline_exact } else { inline_doubling };
                bytes[arm] + inline as u64 >= tree_min
            })
            .map(|(n, _, _)| *n)
            .collect()
    };
    let doubling_loses = loses(2);
    let exact_loses = loses(3);
    let hybrid_loses = loses(4);
    println!("\npoints where array_doubling does NOT beat the cheapest tree arm: {doubling_loses:?}");
    println!("points where array_exact    does NOT beat the cheapest tree arm: {exact_loses:?}");
    println!("points where array_hybrid   does NOT beat the cheapest tree arm: {hybrid_loses:?}");

    // -------------------------------------------------------------------------------------------
    // WHAT IS ASSERTED. Only what is deterministic: these are counter differences over
    // single-threaded construction of fixed shapes, so they do not move with machine load.
    // -------------------------------------------------------------------------------------------

    // 1. EXACT CAPACITY BEATS THE CHEAPEST TREE ARM AT EVERY OCCUPANCY, inline width included.
    //    This is the claim that decides the representation, so it is asserted at every row.
    assert!(
        exact_loses.is_empty(),
        "exact capacity must beat the cheapest tree arm at every occupancy, inline width \
         included -- it does not at {exact_loses:?}"
    );

    // 2. AND IT NEVER COSTS MORE ON THE HEAP THAN THE DOUBLING SHAPE.
    for (n, bytes, _) in &rows {
        assert!(
            bytes[3] <= bytes[2],
            "at {n} points exact capacity must not cost more heap than doubling \
             (exact {}, doubling {})",
            bytes[3],
            bytes[2]
        );
    }

    // 3. THE DOUBLING SHAPE HAS A BAND WHERE IT LOSES AND EXACT CAPACITY DOES NOT. That asymmetry
    //    is the whole argument for the growth policy, so a run where the doubling arm never lost
    //    would mean this table has stopped covering the band and must not quietly pass.
    assert!(
        !doubling_loses.is_empty(),
        "the doubling arm is expected to lose to a nearly-full tree leaf somewhere in 9..=11 -- \
         an empty set here means the occupancy set no longer brackets the split and the \
         comparison has gone vacuous"
    );

    // 4. THE GROWTH POLICY IS WORTH SOMETHING AT ONE POINT PER KEY, the regime with the most keys
    //    and where a doubling `Vec` overshoots hardest.
    let (_, first_bytes, first_allocs) = rows[0];
    assert!(
        first_bytes[3] < first_bytes[2],
        "at one point per key exact capacity must be strictly cheaper than doubling -- equality \
         would mean the capacity-four first allocation does not exist (exact {}, doubling {})",
        first_bytes[3],
        first_bytes[2]
    );
    println!(
        "\nAT ONE POINT PER KEY: cheapest tree {} B heap in {} alloc(s); doubling array {} B in \
         {}; exact array {} B in {}. The GROWTH POLICY alone accounts for {} B of the {} B \
         between the tree and the exact array.",
        first_bytes[0].min(first_bytes[1]),
        if first_bytes[0] <= first_bytes[1] { first_allocs[0] } else { first_allocs[1] },
        first_bytes[2],
        first_allocs[2],
        first_bytes[3],
        first_allocs[3],
        first_bytes[2] - first_bytes[3],
        first_bytes[0].min(first_bytes[1]) - first_bytes[3]
    );

    // 5. THE WIN NARROWS WITH OCCUPANCY BUT DOES NOT CLOSE. Earlier prose had the tree
    //    converging onto the array at high occupancy; it does not. Asserted as a floor on the
    //    steady-state ratio so that a claim of convergence cannot be made from this table.
    let last = rows[rows.len() - 1];
    let tree_min_last = last.1[0].min(last.1[1]) + inline_tree as u64;
    let exact_last = last.1[3] + inline_exact as u64;
    let ratio_last = tree_min_last as f64 / exact_last as f64;
    let tree_min_first = first_bytes[0].min(first_bytes[1]) + inline_tree as u64;
    let exact_first = first_bytes[3] + inline_exact as u64;
    let ratio_first = tree_min_first as f64 / exact_first as f64;
    println!(
        "ratio against the cheapest tree arm: {ratio_first:.2}x at 1 point per key, \
         {ratio_last:.2}x at {} -- NARROWING, and the two do NOT meet",
        last.0
    );
    assert!(
        ratio_first > ratio_last,
        "the win must concentrate at low occupancy ({ratio_first:.2}x at 1 against \
         {ratio_last:.2}x at {})",
        last.0
    );
    assert!(
        ratio_last > 1.5,
        "and the two must NOT converge -- a steady-state ratio of {ratio_last:.2}x at {} \
         points is the correction to earlier prose that had these two meeting, and a reading at \
         or below 1.5x would retire that correction rather than confirm it",
        last.0
    );
}
