// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A LIVE-USER-BYTES DENOMINATOR WOULD COST, AND WHETHER IT CAN BE HAD FOR NOTHING.
//!
//! # THE ONE RATIO IN THE SET THAT IS NOT PUBLISHED
//!
//! The write ratios this store publishes all divide STORED bytes by STORED bytes, which is what
//! makes them unit-matched and compressibility-invariant. The ratio that is missing is
//! logical-bytes-written over LIVE USER BYTES -- how many times over the workload has written the
//! uncompressed data it is keeping. It is absent because nothing holds a live user-byte total: an
//! object record carries `page_size`, which is assigned from `address.length()` and is therefore
//! the STORED length, and `block_slab_live`'s tally sums the same quantity.
//!
//! There are three ways to get the denominator, and they differ by an order of magnitude in cost:
//!
//!   a. AN EXACT SIZE ON THE RECORD. A `u32` of uncompressed length. `ObjectRecord` is 15 bytes of
//!      field in 16, so a `u32` takes it to 19 of field and, at `align_of` 8, to 24 bytes wide --
//!      eight bytes a record, which this arm weighs through the table that holds it rather than
//!      quoting the struct delta.
//!   b. A ONE-BYTE SIZE CLASS IN THE TAIL SLACK. The record pins its own slack --
//!      `size_of::<ObjectRecord>() - 15 == 1` -- so one byte is already paid for and a `u8` code
//!      changes no width at all. It is approximate, and this arm measures by how much.
//!   c. DIVIDING THE STORED TALLY BY THE STORE-WIDE COMPRESSION RATIO. Free, needs no field, and
//!      wrong exactly when the live set compresses differently from the history. This arm is built
//!      to make that visible rather than to assume it.
//!
//! # SO THE CORPUS MIXES COMPRESSIBILITY ON PURPOSE
//!
//! Four cohorts: two sizes crossed with incompressible and highly-compressible values. A uniform
//! corpus cannot tell (c) apart from (a) -- with one compressibility the store-wide ratio IS the
//! live set's ratio and the estimate is exact by construction. The arm asserts the cohorts really
//! did store differently before it reports any error, because an estimate that looks perfect on a
//! fixture that could not challenge it is the fixture trap this campaign keeps finding.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib what_a_live_user_bytes_denominator_would_cost -- --nocapture

use crate::engine::TemporalEngine;
use crate::types::{Command, ExecuteRequest};

/// Keys per cohort.
const PER_COHORT: usize = 400;

/// A one-byte size class: 4-bit exponent, 4-bit mantissa, round to nearest.
///
/// Code 0 means zero bytes. Otherwise `value = (16 + m) << e` for `e` in 0..16 and `m` in 0..16,
/// which spans 16 B to 1,015,808 B in steps of one sixteenth -- a worst-case 3.1% per object, and
/// less than that in a sum because the roundings are in both directions. Values above the top class
/// saturate, which is stated rather than hidden: this is a measurement of whether the shape is
/// worth building, and the production encoding would widen the exponent if the corpus needed it.
fn size_class(len: u64) -> u8 {
    if len == 0 {
        return 0;
    }
    let mut best_code: u8 = 1;
    let mut best_error = u64::MAX;
    for code in 1u16..=255 {
        let e = (code - 1) >> 4;
        let m = (code - 1) & 15;
        let value = (16u64 + m as u64) << e;
        let error = value.abs_diff(len);
        if error < best_error {
            best_error = error;
            best_code = code as u8;
        }
    }
    best_code
}

fn declass(code: u8) -> u64 {
    if code == 0 {
        return 0;
    }
    let e = (code as u16 - 1) >> 4;
    let m = (code as u16 - 1) & 15;
    (16u64 + m as u64) << e
}

/// Incompressible bytes: no short run, no small period.
fn noisy(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

/// Highly compressible bytes: one repeated byte.
fn flat(len: usize) -> Vec<u8> {
    vec![b'v'; len]
}

#[test]
fn what_a_live_user_bytes_denominator_would_cost() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // (label, value length, compressible) -- two sizes crossed with two compressibilities.
    // SIZES CHOSEN NOT TO BE EXACTLY REPRESENTABLE. The size class encodes `(16 + m) << e`, so 64
    // and 1024 land exactly on a class and the encoding reports 0.000% error -- which is true of
    // those lengths and says nothing about the method. These four all fall between classes.
    let cohorts: [(&str, usize, bool); 4] = [
        ("small incompressible", 100, false),
        ("large incompressible", 1500, false),
        ("small compressible", 700, true),
        ("large compressible", 3000, true),
    ];

    let mut exact_user_bytes: u64 = 0;
    let mut classed_user_bytes: u64 = 0;
    let mut stored_before = 0u64;
    let mut per_cohort_stored: Vec<(&str, u64, u64)> = Vec::new();

    for (label, len, compressible) in cohorts {
        let before = engine.block_store.stats().bytes_written;
        for index in 0..PER_COHORT {
            let value = if compressible {
                flat(len)
            } else {
                noisy((index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ len as u64, len)
            };
            exact_user_bytes += value.len() as u64;
            classed_user_bytes += declass(size_class(value.len() as u64));
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key: format!("{label}-{index:06}"), value },
            });
            assert!(response.status.ok, "{label} write {index}: {:?}", response.status);
        }
        let after = engine.block_store.stats().bytes_written;
        per_cohort_stored.push((label, len as u64 * PER_COHORT as u64, after - before));
        stored_before = after;
    }
    let _ = stored_before;

    // --- NOW SKEW THE LIVE SET AWAY FROM THE HISTORY, which is the only state that can separate a
    // --- store-wide estimate from a per-object size. The first version of this arm wrote each key
    // --- once, so the live set WAS the whole history: the store-wide ratio was the live set's
    // --- ratio by construction and the estimate came out exact at 0.000%. Its own assertion caught
    // --- that and refused to conclude anything.
    //
    // Overwriting the two compressible cohorts with INCOMPRESSIBLE bytes of the same length leaves
    // the history full of cheap writes while the live set is entirely expensive ones, so a ratio
    // taken over the history overstates what the live set compresses to.
    for (label, len, compressible) in cohorts {
        if !compressible {
            continue;
        }
        for index in 0..PER_COHORT {
            let value = noisy(
                (index as u64).wrapping_mul(0xD1B5_4A32_D192_ED03) ^ (len as u64 + 7),
                len,
            );
            // The live user bytes do not change: same key, same length, different bytes. The
            // totals accumulated above still describe the live set exactly.
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet { key: format!("{label}-{index:06}"), value },
            });
            assert!(response.status.ok, "{label} overwrite {index}: {:?}", response.status);
        }
    }

    let published = engine.shard_stats(1).expect("a loaded shard reports stats").storage.clone();
    let store = engine.block_store.stats();
    let live_stored = published.live_block_bytes;

    // --- THE COHORTS MUST HAVE STORED DIFFERENTLY, or (c) cannot be shown wrong and every error
    // --- below is a property of the fixture rather than of the method.
    println!();
    println!("  WHAT EACH COHORT ASKED FOR AND WHAT IT STORED");
    println!("    {:<24} {:>12} {:>12} {:>9}", "cohort", "user B", "stored B", "ratio");
    for (label, user, stored) in &per_cohort_stored {
        println!(
            "    {:<24} {:>12} {:>12} {:>8.3}x",
            label,
            user,
            stored,
            *stored as f64 / *user as f64
        );
    }
    let incompressible_ratio = per_cohort_stored[1].2 as f64 / per_cohort_stored[1].1 as f64;
    let compressible_ratio = per_cohort_stored[3].2 as f64 / per_cohort_stored[3].1 as f64;
    assert!(
        incompressible_ratio > compressible_ratio * 2.0,
        "the two large cohorts stored at {incompressible_ratio:.3}x and {compressible_ratio:.3}x of \
         their user bytes. They must differ materially, or this corpus cannot tell a store-wide \
         compression estimate apart from an exact per-object size and the comparison below is \
         vacuous."
    );

    // --- THE THREE DENOMINATORS.
    let store_wide = if store.bytes_written == 0 {
        0.0
    } else {
        store.logical_bytes_written as f64 / store.bytes_written as f64
    };
    let estimated_user_bytes = (live_stored as f64 * store_wide) as u64;

    println!();
    println!("  THE LIVE-USER-BYTES DENOMINATOR, THREE WAYS");
    println!("    exact (a sum of the values written)      {exact_user_bytes:>12} B");
    println!(
        "    one-byte size class                      {classed_user_bytes:>12} B   error {:>+8.3}%",
        100.0 * (classed_user_bytes as f64 - exact_user_bytes as f64) / exact_user_bytes as f64
    );
    println!(
        "    stored tally / store-wide compression    {estimated_user_bytes:>12} B   error {:>+8.3}%",
        100.0 * (estimated_user_bytes as f64 - exact_user_bytes as f64) / exact_user_bytes as f64
    );
    println!(
        "      (live stored {live_stored} B, store-wide logical/physical {store_wide:.4})"
    );

    let class_error =
        ((classed_user_bytes as f64 - exact_user_bytes as f64) / exact_user_bytes as f64).abs();
    let estimate_error =
        ((estimated_user_bytes as f64 - exact_user_bytes as f64) / exact_user_bytes as f64).abs();

    println!();
    println!(
        "  (history logical {} B over {} live user B -- the live set is written {:.2}x over)",
        store.logical_bytes_written,
        exact_user_bytes,
        store.logical_bytes_written as f64 / exact_user_bytes as f64
    );
    println!();
    println!("  WHAT THE MISSING RATIO WOULD READ");
    for (name, denominator) in [
        ("exact", exact_user_bytes),
        ("size class", classed_user_bytes),
        ("estimate", estimated_user_bytes),
    ] {
        let ppm = if denominator == 0 {
            0
        } else {
            store.logical_bytes_written.saturating_mul(1_000_000) / denominator
        };
        println!(
            "    logical_write over {name:<12} {:>9} ppm  ({:.3}x)",
            ppm,
            ppm as f64 / 1_000_000.0
        );
    }
    println!();

    assert!(exact_user_bytes > 0, "nothing was written, so every figure above is vacuous");
    assert!(live_stored > 0, "nothing is live, so the estimate has no denominator");

    // --- THE SIZE CLASS IS THE CHEAP OPTION AND IT IS ALSO THE ACCURATE ONE. This is the finding:
    // --- one byte already paid for in the record's tail slack beats a free store-wide estimate by
    // --- a wide margin on a corpus whose compressibility is not uniform.
    assert!(
        class_error < estimate_error,
        "the one-byte size class was {:.3}% out and the store-wide estimate {:.3}%. The class was \
         expected to be the more accurate of the two free options -- if the estimate wins, this \
         corpus is not mixed enough to separate them and the conclusion does not follow.",
        100.0 * class_error,
        100.0 * estimate_error
    );
    // --- AND IT IS ACCURATE IN ABSOLUTE TERMS, not merely better than a bad alternative. 4% is
    // --- the bound the 4-bit mantissa implies with rounding in both directions; a failure here
    // --- means the encoding is not behaving as its own arithmetic says.
    assert!(
        class_error < 0.04,
        "the one-byte size class was {:.3}% out, above the 4% its 4-bit mantissa implies",
        100.0 * class_error
    );
}
