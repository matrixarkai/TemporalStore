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
//! # WHAT IS ASSERTED, AND WHY IT IS AN EQUALITY RATHER THAN A RATIO
//!
//! Siblings examined, at three element counts, against `N*(N-1)/2` EXACTLY -- no tolerance, no
//! trend. A ratio would have read 4.0x per doubling and said the same thing less precisely, and a
//! ratio has to be given a band, which is a number nobody can defend.
//!
//! And, separately, that **per-write cost does not scale with that count**. Siblings quadruple
//! from the middle size to the largest, so a write paying for the scan would roughly double. That
//! is the claim this arm exists to make -- the scan is quadratic AND cheap -- and it is the half
//! that a fix elsewhere cannot invalidate.

#![allow(clippy::all)]
use super::*;

/// How many times the sibling scan ran, how many siblings it examined, and what a write cost --
/// building one container of `elements` members.
///
/// The clock starts AFTER the shard is loaded and the counters are zeroed, so the engine's own
/// setup is not charged to a write. It covers the writes and nothing else.
fn build_one_container(kind: &str, elements: usize) -> (u64, u64, f64) {
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

    let started = std::time::Instant::now();
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

    let per_write = started.elapsed().as_secs_f64() * 1000.0 / elements as f64;
    let calls = crate::engine::state::SIBLING_SCAN_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let examined =
        crate::engine::state::SIBLING_SCAN_ELEMENTS.load(std::sync::atomic::Ordering::Relaxed);
    (calls, examined, per_write)
}

/// HOW THE SIBLING SCAN GROWS, AND WHETHER ANY WRITE ACTUALLY PAYS FOR IT.
///
/// # TWO READINGS, AND ONLY ONE OF THEM IS A CLAIM ABOUT TODAY
///
/// The scan is **exactly quadratic**: building a container of N elements examines `N*(N-1)/2`
/// siblings, identically for all four kinds, because each new element asks for `max(block_id)`
/// over every element already there. Pinned below as an equality, so it names the formula rather
/// than a trend.
///
/// And the scan is **cheap**: 32,640 pointer examinations for a 256-element container is
/// microseconds. The way to assert that WITHOUT asserting anything about which kind is slow today
/// is to read the growth of per-write cost against the growth of the scan. Siblings QUADRUPLE from
/// `MID` to `LARGE`; a write that paid for the scan would roughly DOUBLE. So a per-write cost that
/// stays flat across that doubling is the scan being irrelevant to it, measured.
///
/// Where the quadratic DOES bite is the ceiling. At `MAX_ADDRESSABLE_BLOCK_ID` the same formula is
/// ~2.1 billion examinations for one object, so this is a scaling defect rather than a present
/// cost, and worth fixing as one.
///
/// # WHY THE COST SPREAD BETWEEN KINDS IS PRINTED AND NOT ASSERTED
///
/// This arm first asserted it: `hash` measured **102.3 ms per write against zset's 2.7**, 37.9x
/// apart at identical scan work, which is a clean proof that the scan is not the cost. It was also
/// the wrong thing to assert. That spread is a DEFECT -- `upsert_bucket_index_block_inner` answered
/// a per-write question by building two hash maps over the whole shard's object lookup, at
/// `O(all blocks in the shard)` per write -- and `a_container_write_asks_its_own_slot` closes it,
/// taking a hash field write from `182.65 ms` to `9.94` at 800 fields. The moment it landed, an
/// assertion that the kinds differ by more than 2x would FAIL, on an arm that was correct when it
/// merged.
///
/// So the spread is reported, and what is asserted is the invariant it was evidence FOR. `hash` is
/// excluded from the growth clause for the same reason and named in the exclusion: while it carries
/// a second per-write term that grows with the container, its curve cannot say anything about this
/// one. Add it to `PAY_ONLY_FOR_THE_SCAN` once that term is gone and the clause gets stronger for
/// free.
///
/// # THE SIZES ARE CHOSEN BY WHAT THE WRITE PATH CAN AFFORD
///
/// A doubling is a doubling at any scale, so the formula is read at 64/128/256. At 250/500/1,000
/// this arm did not finish 1,750 writes in twenty minutes -- which was the cost spread above,
/// before it was understood.
///
/// rust-internal: measures this crate's own write path, no product behaviour
#[test]
fn what_a_new_container_element_scans() {
    const SMALL: usize = 64;
    const MID: usize = 128;
    const LARGE: usize = 256;

    /// The kinds whose write path has no term other than this scan that grows with the container,
    /// so their per-write curve is attributable to it. See the note above on why `hash` is absent.
    const PAY_ONLY_FOR_THE_SCAN: &[&str] = &["set", "zset", "list"];

    /// Siblings examined if every new element walks every sibling already present: the sum of
    /// 0..n-1. Written as the closed form so the assertion names the shape it is testing.
    fn every_sibling_every_time(elements: usize) -> u64 {
        (elements as u64) * (elements as u64 - 1) / 2
    }

    println!();
    // kind, ms/write at MID, ms/write at LARGE, siblings at LARGE
    let mut rows: Vec<(&str, f64, f64, u64)> = Vec::new();

    for kind in ["hash", "set", "zset", "list"] {
        let (_, small, small_ms) = build_one_container(kind, SMALL);
        let (mid_calls, mid, mid_ms) = build_one_container(kind, MID);
        let (large_calls, large, large_ms) = build_one_container(kind, LARGE);

        println!(
            "  {kind:<6} siblings {:>7} / {:>7} / {:>7}   expected {:>7} / {:>7} / {:>7}",
            small,
            mid,
            large,
            every_sibling_every_time(SMALL),
            every_sibling_every_time(MID),
            every_sibling_every_time(LARGE),
        );
        println!(
            "  {:<6} ms/write {small_ms:>7.2} / {mid_ms:>7.2} / {large_ms:>7.2}   \
             scan x4 over that doubling, cost x{:.2}",
            "",
            large_ms / mid_ms.max(f64::MIN_POSITIVE),
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
        rows.push((kind, mid_ms, large_ms, large));
    }

    // --- THE PREMISE OF EVERYTHING BELOW: the kinds do the SAME scan work, so a difference in
    // --- what they cost is a difference in something else.
    let reference = rows[0].3;
    for (kind, _, _, large) in &rows {
        assert_eq!(
            reference, *large,
            "{kind} examined {large} siblings against {reference} for {}: the kinds must do the \
             same scan work before any statement about their costs says anything",
            rows[0].0
        );
    }

    // --- REPORTED, NOT ASSERTED. See the note above: this spread is a defect being closed
    // --- elsewhere, so pinning it would make this arm fail on someone else's fix.
    let slowest = rows.iter().cloned().fold(rows[0], |a, b| if b.2 > a.2 { b } else { a });
    let fastest = rows.iter().cloned().fold(rows[0], |a, b| if b.2 < a.2 { b } else { a });
    println!();
    println!(
        "  at {LARGE} elements the same scan work costs {:.2} ms per write on {} and {:.2} ms on \
         {} -- {:.1}x apart",
        slowest.2,
        slowest.0,
        fastest.2,
        fastest.0,
        slowest.2 / fastest.2.max(f64::MIN_POSITIVE)
    );

    // --- READING TWO, AS THE INVARIANT RATHER THAN THE EVIDENCE. The scan QUADRUPLES from MID to
    // --- LARGE. A write that paid for it would roughly double; these stay flat, which is the
    // --- measured form of "the scan is quadratic and cheap".
    const COST_GROWTH_A_SCAN_WOULD_FORCE: f64 = 2.0;
    const FLAT_ENOUGH: f64 = 1.8;
    let mut checked = 0usize;
    for (kind, mid_ms, large_ms, _) in &rows {
        if !PAY_ONLY_FOR_THE_SCAN.contains(kind) {
            continue;
        }
        checked += 1;
        let growth = large_ms / mid_ms.max(f64::MIN_POSITIVE);
        assert!(
            growth < FLAT_ENOUGH,
            "{kind}: siblings examined went x4 from {MID} to {LARGE} elements and per-write cost \
             went x{growth:.2} ({mid_ms:.2} -> {large_ms:.2} ms), at or past the x\
             {COST_GROWTH_A_SCAN_WOULD_FORCE:.1} a write PAYING for that scan would show. Either \
             the scan stopped being cheap, or something else on this kind's write path now grows \
             with the container -- and the first thing to check is which, because this arm's whole \
             point is that a quadratic scan and a slow write are separate questions."
        );
    }
    // A clause that filters its own population is disarmed by shrinking it.
    assert_eq!(
        PAY_ONLY_FOR_THE_SCAN.len(),
        checked,
        "the growth clause ran on {checked} kinds against the {} it names: a kind that stops being \
         measured here stops being checked here, silently",
        PAY_ONLY_FOR_THE_SCAN.len()
    );
    println!();
}
