// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A NEW CONTAINER ELEMENT SPENDS LOOKING AT ITS SIBLINGS, AS A COUNT OF SIBLINGS.
//!
//! # WHY THIS IS COUNTED AND NOT TIMED
//!
//! `container_block_ordinal` gives a new element the position `max(block_id) + 1` over the
//! object's live elements, and `resident_highest_block_id` computes that `max` by **walking every
//! element of the object**. So inserting the Nth element of a container is O(N), and building one
//! of N elements is O(N^2) -- which is a claim about WORK, and the only evidence for it was
//! inspection plus a wall clock: 2,000 writes in 8.88 s against 10,000 not finishing in seven
//! minutes. That curve is consistent with this scan and also with the per-op fsync beside it,
//! priced at ~3.3 ms, and a clock cannot separate them.
//!
//! Siblings examined can. It is the quantity the O(N^2) claim is actually about, it is immune to
//! what else the write path pays for, and it is stable from run to run.
//!
//! # WHERE THE DEFECT CAME FROM, SINCE IT IS NOT VISIBLE FROM INSIDE
//!
//! It came out of pricing a single HSET by the bytes it lands on disk -- ~2,419 B per op for an
//! 8-byte payload, which is an envelope question -- and this is the second of the two mechanisms
//! that search turned up. The reason no arm covered it is that no workload here builds a container
//! wide enough: a hash of **20 fields** is the shape the benchmarks use, and at 20 fields a
//! quadratic term is invisible.
//!
//! # WHAT IS ASSERTED
//!
//! The SHAPE, at three element counts, with the quadratic read off the ratio between them rather
//! than from a single number: doubling the elements must roughly quadruple the siblings examined.
//! A linear implementation holds the ratio near 2.0, so the two are told apart by the same arm.

#![allow(clippy::all)]
use super::*;

/// Siblings examined while building one container of `elements` members, and the ordinal the last
/// member was given.
fn build_one_container(kind: &str, elements: usize) -> (u64, u64) {
    use crate::types::{Command, ExecuteRequest};

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = std::sync::Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    ));
    engine.load_shard(1);

    // ZEROED AFTER THE LOAD, so the shard's own setup cannot be charged to the build.
    crate::engine::state::SIBLING_SCAN_CALLS.store(0, std::sync::atomic::Ordering::Relaxed);
    crate::engine::state::SIBLING_SCAN_ELEMENTS.store(0, std::sync::atomic::Ordering::Relaxed);

    for i in 0..elements {
        let command = match kind {
            "hash" => Command::HashSet {
                key: "c".to_string(),
                field: format!("f{i:06}"),
                value: vec![b'v'; 8],
            },
            "set" => Command::SetAdd {
                key: "c".to_string(),
                member: format!("m{i:06}").into_bytes(),
            },
            "zset" => Command::ZSetAdd {
                key: "c".to_string(),
                member: format!("m{i:06}").into_bytes(),
                score: i as f64,
            },
            "list" => Command::ListPush {
                key: "c".to_string(),
                member: format!("m{i:06}").into_bytes(),
                left: false,
            },
            other => panic!("unknown kind {other}"),
        };
        let response = engine.execute(ExecuteRequest { shard_id: 1, command });
        assert!(response.status.ok, "{kind} write {i}: {:?}", response.status);
    }

    let calls = crate::engine::state::SIBLING_SCAN_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let examined =
        crate::engine::state::SIBLING_SCAN_ELEMENTS.load(std::sync::atomic::Ordering::Relaxed);
    (calls, examined)
}

/// HOW THE SIBLING SCAN GROWS, AND WHICH KIND IS ACTUALLY SLOW. THEY ARE NOT THE SAME ANSWER.
///
/// # TWO READINGS, AND THE SECOND ONE CORRECTS THE FIRST'S INTERPRETATION
///
/// The scan is **exactly quadratic**: building a container of N elements examines `N*(N-1)/2`
/// siblings, identically for all four kinds, because each new element asks for `max(block_id)`
/// over every element already there. That is pinned below as an equality rather than a ratio, so
/// it names the formula and not a trend.
///
/// But the scan is **cheap**. 32,640 pointer examinations for a 256-element container is
/// microseconds, and the four kinds do the SAME scan work while their per-write wall time differs
/// by 18x. So the quadratic cannot be what makes a container build slow at these sizes, and the
/// hypothesis it was recorded under -- "the fsync-per-op cost is linear and cannot explain that
/// curve; this scan can" -- is **half right**: the curve is real, the cost is not where it was
/// attributed. Pinning both readings together is what keeps the two apart.
///
/// Where the quadratic DOES bite is the ceiling. At `MAX_ADDRESSABLE_BLOCK_ID` the same formula is
/// ~2.1 billion examinations for one object, so this is a scaling defect rather than a present
/// cost, and worth fixing as one.
///
/// # WHY THIS WAS NOT ALREADY MEASURED
///
/// It came out of pricing a single HSET by the bytes it lands on disk, and the workload that would
/// expose it does not exist in any benchmark here: a hash of **20 fields** is the shape they use,
/// and at 20 fields a quadratic term is invisible.
///
/// # THE SIZES ARE CHOSEN BY WHAT THE WRITE PATH CAN AFFORD
///
/// A doubling is a doubling at any scale, so the formula is read at 64/128/256. At 250/500/1,000
/// this arm did not finish 1,750 writes in twenty minutes, which is itself the second reading
/// below.
///
/// rust-internal: measures this crate's own write path, no product behaviour
#[test]
fn what_a_new_container_element_scans() {
    const SMALL: usize = 64;
    const MID: usize = 128;
    const LARGE: usize = 256;

    /// What the scan costs if every new element walks every sibling already present: the sum of
    /// 0..n-1. Written as the closed form so the assertion names the shape it is testing.
    fn every_sibling_every_time(elements: usize) -> u64 {
        (elements as u64) * (elements as u64 - 1) / 2
    }

    println!();
    let mut rows: Vec<(&str, f64, u64, u64)> = Vec::new();

    for kind in ["hash", "set", "zset", "list"] {
        let started = std::time::Instant::now();
        let (_, small) = build_one_container(kind, SMALL);
        let (mid_calls, mid) = build_one_container(kind, MID);
        let (large_calls, large) = build_one_container(kind, LARGE);
        let per_write =
            started.elapsed().as_secs_f64() * 1000.0 / (SMALL + MID + LARGE) as f64;

        println!(
            "  {kind:<6} siblings {:>7} / {:>7} / {:>7}   expected {:>7} / {:>7} / {:>7}   \
             {per_write:>6.1} ms per write",
            small,
            mid,
            large,
            every_sibling_every_time(SMALL),
            every_sibling_every_time(MID),
            every_sibling_every_time(LARGE),
        );

        // --- NON-VACUITY: a kind whose writes never reach the allocator says nothing either way.
        assert!(
            large_calls > 0 && mid_calls > 0,
            "{kind}: the ordinal allocator was never reached, so this row measures nothing"
        );
        // --- READING ONE, AS AN EQUALITY. Every new element walks every sibling, so the total is
        // --- the sum of 0..n-1 exactly. An allocator that stops scanning fails HERE, by name,
        // --- which is the point: this is the arm a fix has to come and change.
        for (elements, examined) in [(SMALL, small), (MID, mid), (LARGE, large)] {
            assert_eq!(
                every_sibling_every_time(elements),
                examined,
                "{kind}: building {elements} elements examined {examined} siblings against the \
                 {} that `max(block_id)` over every sibling costs. If this is LOWER the allocator \
                 stopped scanning, which is the fix -- say so here and delete this clause. If it \
                 is HIGHER something scans more than once per insert.",
                every_sibling_every_time(elements)
            );
        }
        rows.push((kind, per_write, mid, large));
    }

    // --- READING TWO: the four kinds do the SAME scan work and do not cost the same to write, so
    // --- the scan is not what the time is going on.
    let slowest = rows.iter().cloned().fold(rows[0], |a, b| if b.1 > a.1 { b } else { a });
    let fastest = rows.iter().cloned().fold(rows[0], |a, b| if b.1 < a.1 { b } else { a });
    println!();
    println!(
        "  the same scan work costs {:.1} ms per write on {} and {:.1} ms on {} -- {:.1}x apart",
        slowest.1,
        slowest.0,
        fastest.1,
        fastest.0,
        slowest.1 / fastest.1.max(f64::MIN_POSITIVE)
    );
    println!();

    // Every kind examined the same number of siblings, which is the premise that makes the time
    // spread attributable to something OTHER than the scan.
    let reference = rows[0].3;
    for (kind, _, _, large) in &rows {
        assert_eq!(
            reference, *large,
            "{kind} examined {large} siblings against {reference} for {}: the kinds must do the \
             same scan work for the time spread below to say anything about what else they do",
            rows[0].0
        );
    }
    assert!(
        slowest.1 / fastest.1.max(f64::MIN_POSITIVE) > 2.0,
        "the four kinds cost within 2x of each other per write ({:.1} ms on {} against {:.1} on \
         {}), so this arm no longer shows the scan and the time to be separate questions -- which \
         is the whole of its second reading",
        slowest.1,
        slowest.0,
        fastest.1,
        fastest.0
    );
}
