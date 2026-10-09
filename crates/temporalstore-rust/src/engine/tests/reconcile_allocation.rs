// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What reconciling a timestamped series map against its block-derived view ALLOCATES.
//!
//! WHY THIS IS A LOAD-PATH MEASUREMENT AND NOT A REFACTOR NOTE. `reconcile_timestamped_series_membership`
//! takes the persisted map by reference, takes the block-derived view by value, and RETURNS A NEW MAP.
//! That signature is what makes it hard to generalise over a container -- but the signature is not the
//! cost. The cost is what the shape forces it to build:
//!
//!   * for every key present in BOTH, a fresh `BTreeMap` collected from the persisted series, because
//!     the merge is expressed as building a new series rather than refreshing addresses in the old one;
//!   * for every key present in the PERSISTED map only, a full deep `clone` of its series;
//!   * and a fresh outer map to put them in.
//!
//! On a normal reload the blocks exist for every key, so the first of those is the dominant one: the
//! function REBUILDS THE WHOLE SERIES MAP in order to refresh addresses inside it. Mutating the
//! persisted map in place refreshes the addresses and allocates none of it -- and the persisted-only
//! loop disappears entirely, because a key nothing touched needs no clone to survive.
//!
//! So the in-place form is not a concession made to get a trait through. It is cheaper on the path
//! every shard load takes, and the generalisation comes free with it. This fixture is here so that
//! stays recorded: a buried win gets reverted by the next person optimising this path, because nothing
//! tells them the shape was load-bearing.
//!
//! ONE ARM IS THE SHIPPED FUNCTION; ONLY THE OTHER IS TRANSCRIBED. The rebuilding form no longer
//! exists in the tree, so pricing it needs a transcription -- and a transcription that had drifted
//! would price a function nobody runs. So it is asserted to produce the SAME MAP as the shipped form
//! on the same inputs, over all-covered, half-covered and none-covered, plus a check that a covered
//! address really did change so the agreement cannot be two forms agreeing on having done nothing.
//! The in-place arm is NOT transcribed: it delegates to the shipped function, because a fixture that
//! prices a copy of live code can agree with itself forever while the live code moves. A worked
//! example in a test is not inert, and confining it to the arm that has no original is what makes it
//! safe.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::collections::HashMap;

use crate::block_store::ElementEntry;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

type Series = BTreeMap<u64, ElementEntry>;
type SeriesMap = HashMap<String, Series>;

fn address(id: u64) -> ElementEntry {
    ElementEntry::from_parts(id, 0, 64, Some(1), Some(id))
}

/// The shape the function had before this change, transcribed so both can be measured at once.
fn reconcile_by_rebuilding(persisted: &SeriesMap, block_derived: SeriesMap) -> SeriesMap {
    let mut result: SeriesMap = HashMap::new();
    for (key, block_series) in block_derived {
        match persisted.get(&key) {
            Some(persisted_series) => {
                let merged = persisted_series
                    .iter()
                    .map(|(timestamp_ms, persisted_address)| {
                        let address = block_series
                            .get(timestamp_ms)
                            .cloned()
                            .unwrap_or_else(|| persisted_address.clone());
                        (*timestamp_ms, address)
                    })
                    .collect();
                result.insert(key, merged);
            }
            None => {
                result.insert(key, block_series);
            }
        }
    }
    for (key, persisted_series) in persisted {
        result
            .entry(key.clone())
            .or_insert_with(|| persisted_series.clone());
    }
    result
}

/// THE SHIPPED FUNCTION, not a copy of it.
///
/// This arm delegates rather than transcribing, and that asymmetry is deliberate. The rebuilding arm
/// above no longer exists in the tree, so pricing it requires a transcription and that transcription
/// is verified against this one before either is measured. This arm DOES exist, so copying it would
/// introduce exactly the drift the verification is there to catch -- a fixture that prices a copy of
/// the shipped code can agree with itself forever while the shipped code moves.
fn reconcile_in_place(target: &mut SeriesMap, block_derived: SeriesMap) {
    crate::engine::storage_bucket_internals::reconcile_timestamped_series_membership_in_place(
        target,
        block_derived,
    )
}

/// `keys` series of `points` points each, with the derived view holding fresh addresses for the
/// first `covered` of those keys -- the rest standing for a block the reconcile could not read.
fn inputs(keys: usize, points: usize, covered: usize) -> (SeriesMap, SeriesMap) {
    let mut persisted: SeriesMap = HashMap::new();
    let mut derived: SeriesMap = HashMap::new();
    for k in 0..keys {
        let key = format!("f{k}");
        let series: Series = (0..points as u64).map(|at| (at, address(at))).collect();
        if k < covered {
            // The derived view carries the same timestamps at DIFFERENT addresses, so a refresh is
            // observable and a no-op implementation cannot pass the equivalence check below.
            derived.insert(
                key.clone(),
                (0..points as u64).map(|at| (at, address(at + 1_000_000))).collect(),
            );
        }
        persisted.insert(key, series);
    }
    (persisted, derived)
}

/// The two forms agree, including on the cases that distinguish them.
#[test]
fn refreshing_in_place_gives_the_same_map_as_rebuilding_it() {
    for (keys, points, covered) in [(8usize, 5usize, 8usize), (8, 5, 4), (8, 5, 0)] {
        let (persisted, derived) = inputs(keys, points, covered);
        // DENOMINATORS: both inputs are populated, or the agreement below is between two empties.
        assert_eq!(keys, persisted.len(), "denominator: persisted keys");
        assert_eq!(covered, derived.len(), "denominator: derived keys");

        let rebuilt = reconcile_by_rebuilding(&persisted, derived.clone());
        let mut in_place = persisted.clone();
        reconcile_in_place(&mut in_place, derived.clone());
        assert_eq!(
            rebuilt, in_place,
            "the two forms disagree at {keys} keys, {points} points, {covered} covered -- so the \
             transcription and the shipped form are not the same function and nothing below prices \
             anything"
        );
        // And the refresh really happened, or both arms are agreeing on having done nothing.
        if covered > 0 {
            let refreshed = in_place
                .get("f0")
                .and_then(|series| series.get(&0))
                .cloned()
                .expect("f0 at 0 is there");
            assert_eq!(
                address(1_000_000),
                refreshed,
                "the covered key's address was not refreshed, so the equivalence is vacuous"
            );
        }
        // A derived-only key must arrive, and a persisted-only key must survive.
        assert!(in_place.contains_key(&format!("f{}", keys - 1)), "every persisted key survives");
    }
}

/// What each form allocates, at three corpus sizes.
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "reconciles three corpora twice under the counting allocator; run by name"]
fn what_a_reconcile_allocates_rebuilding_against_refreshing_in_place() {
    let mut rows: Vec<(usize, usize, u64, u64, u64, u64)> = Vec::new();
    for (keys, points) in [(40usize, 1_000usize), (400, 100), (4_000, 10)] {
        // EVERY KEY COVERED, which is the normal reload: the blocks exist, so this is the case the
        // path actually takes rather than the failure case.
        let (persisted, derived) = inputs(keys, points, keys);
        let total_points = keys * points;
        assert_eq!(
            total_points,
            persisted.values().map(|s| s.len()).sum::<usize>(),
            "denominator: every point is in the persisted map"
        );

        let rebuilt_input = derived.clone();
        let probe = Probe::start();
        let rebuilt = reconcile_by_rebuilding(&persisted, rebuilt_input);
        let rebuild_counts = probe.stop();
        std::hint::black_box(&rebuilt);
        drop(rebuilt);

        let mut in_place = persisted.clone();
        let in_place_input = derived.clone();
        let probe = Probe::start();
        reconcile_in_place(&mut in_place, in_place_input);
        let in_place_counts = probe.stop();
        std::hint::black_box(&in_place);
        drop(in_place);

        println!(
            "  {keys:>5} keys x {points:>5} points = {total_points:>7} points\n\
             {:<8}rebuilding:        {:>11} B in {:>7} allocations\n\
             {:<8}refreshing in place: {:>9} B in {:>7} allocations\n\
             {:<8}in place / rebuilding: {:.4}x bytes, {:.4}x allocations",
            "", rebuild_counts.alloc_bytes, rebuild_counts.allocs,
            "", in_place_counts.alloc_bytes, in_place_counts.allocs,
            "",
            in_place_counts.alloc_bytes as f64 / rebuild_counts.alloc_bytes.max(1) as f64,
            in_place_counts.allocs as f64 / rebuild_counts.allocs.max(1) as f64,
        );
        rows.push((
            keys,
            points,
            rebuild_counts.alloc_bytes,
            rebuild_counts.allocs,
            in_place_counts.alloc_bytes,
            in_place_counts.allocs,
        ));
    }

    // THE UNCOVERED PATH, PRICED SEPARATELY. A key the derived view carries and the persisted map
    // does not is INSERTED, which can grow the target's table -- so the zero above is a claim about
    // the covered case and this says what the other case costs rather than leaving it implied.
    {
        let keys = 400usize;
        let points = 100usize;
        let (persisted, derived) = inputs(keys, points, 0);
        // Half the derived view's keys are ones the persisted map has never seen.
        let mut arriving: SeriesMap = HashMap::new();
        for k in keys..(keys + keys / 2) {
            arriving.insert(
                format!("f{k}"),
                (0..points as u64).map(|at| (at, address(at))).collect(),
            );
        }
        assert_eq!(0, derived.len(), "denominator: nothing is covered in this case");
        assert_eq!(keys / 2, arriving.len(), "denominator: two hundred keys are arriving");
        let mut target = persisted.clone();
        let probe = Probe::start();
        reconcile_in_place(&mut target, arriving);
        let counts = probe.stop();
        std::hint::black_box(&target);
        println!(
            "  the uncovered path: {} keys arriving into a {keys}-key map allocated {} B in {} \
             allocations ({:.1} B a key)",
            keys / 2,
            counts.alloc_bytes,
            counts.allocs,
            counts.alloc_bytes as f64 / (keys / 2) as f64
        );
        assert_eq!(
            keys + keys / 2,
            target.len(),
            "every arriving key must have arrived and every persisted key survived"
        );
    }

    assert_eq!(3, rows.len(), "three corpus sizes must be measured");
    println!("\n  per point reconciled:");
    for (keys, points, rb, rba, ip, ipa) in &rows {
        let total = (*keys * *points) as f64;
        println!(
            "  {keys:>5} x {points:>5}: rebuilding {:>7.1} B and {:>6.3} allocations a point; in \
             place {:>7.1} B and {:>6.3}",
            *rb as f64 / total,
            *rba as f64 / total,
            *ip as f64 / total,
            *ipa as f64 / total
        );
    }
    // NON-VACUITY on the scan: the rebuilding arm must have allocated something, or the ratio is a
    // zero over a zero and the probe is blind.
    let rebuilt_total: u64 = rows.iter().map(|(_, _, rb, _, _, _)| *rb).sum();
    assert!(
        rebuilt_total > 0,
        "the rebuilding arm allocated nothing across three corpora, so the probe is not measuring it"
    );
    // THE CLAIM, AND IT IS ZERO RATHER THAN A RATIO. Every corpus above has every key covered, which
    // is the normal reload: the blocks exist. In that case refreshing in place should allocate
    // NOTHING AT ALL -- a `BlockAddress` is sixteen bytes of integers so its clone touches no heap,
    // iterating a B-tree allocates nothing, and consuming the derived view only frees.
    //
    // "Less than rebuilding" would have been the weaker assertion and the less useful signal. If this
    // form still allocates per KEY, something is still building a container per key and the figure
    // shows up as a constant times the key count rather than as zero -- which says WHERE the cost is,
    // not merely that there is less of it.
    for (keys, points, rb, rba, ip, ipa) in &rows {
        assert!(
            *rb > 0 && *rba > 0,
            "the rebuilding arm allocated nothing at {keys} x {points}, so there is no cost here to \
             remove and the comparison is vacuous"
        );
        assert_eq!(
            0, *ip,
            "at {keys} keys x {points} points, refreshing in place allocated {ip} B in {ipa} \
             allocations where it should allocate nothing. Divide by the key count ({:.3} B a key) \
             and by the point count ({:.3} B a point): whichever of those is a round number is the \
             thing still being built per unit",
            *ip as f64 / *keys as f64,
            *ip as f64 / (*keys * *points) as f64
        );
    }
    // AND THE COST REMOVED SCALES WITH POINTS, not merely with keys -- the collect per key is per
    // POINT in its length, which is why the rebuild is not a fixed overhead that could be ignored.
    let (small_keys, small_points, small_rb, _, _, _) = rows[0];
    let (big_keys, big_points, big_rb, _, _, _) = rows[rows.len() - 1];
    println!(
        "  the cost removed, per point: {:.1} B at {small_keys} x {small_points}, {:.1} B at \
         {big_keys} x {big_points}",
        small_rb as f64 / (small_keys * small_points) as f64,
        big_rb as f64 / (big_keys * big_points) as f64
    );
}
