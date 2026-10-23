// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! EVERY PUBLISHED AMPLIFICATION RATIO IS RE-DERIVABLE FROM PUBLISHED COUNTERS.
//!
//! # WHY THIS IS THE PROPERTY AND NOT THE FIELD COUNT
//!
//! The shard status publishes four amplification ratios, and until the denominators landed beside
//! them a consumer could re-derive only one. `logical_bytes_written` -- the denominator of the
//! storage-write ratio -- appeared in `control.rs` exactly once, inside a doc comment explaining
//! that ratio, and never as a field. The lifetime ratio's denominator, the live blocks the index
//! still points at, was not published either.
//!
//! That matters because every amplification figure corrected in this area was wrong in a way no
//! range check catches, and each was caught by having the raw counters to hand:
//!
//!   * a payload write ratio that was really the compression factor, its halves in different units;
//!   * a cache figure that multiplied the cache's own overhead by that same factor;
//!   * an index disk ratio that divided the image BY ITSELF -- which passes a range check, a
//!     non-zero check and a self-consistency identity alike.
//!
//! A consumer holding ratios without counters can find none of those. One that can recompute every
//! ratio from published fields can. This arm makes that recomputation a gate.
//!
//! # EXACT EQUALITY, AND DENOMINATORS FIRST
//!
//! The comparisons are exact: these are integer ppm figures produced by the same ratio helper the
//! engine uses, so a mismatch means the published counter is not the one the ratio divided by --
//! exactly the defect being guarded, and exactly what a tolerance would admit.
//!
//! Each denominator is required non-zero before its identity is checked, because the engine
//! reports 0 ppm on a zero denominator by convention and `0 == 0` would hold vacuously. The arm
//! prints which ratios it exercised, so a pass cannot mean it checked nothing.
//!
//! rust-internal: measures this crate's own published counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib every_published_ratio_is_rederivable -- --ignored --nocapture

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 400;
const VALUE: usize = 256;

/// The same arithmetic the engine's own `ppm_ratio` performs, including its zero-denominator
/// convention. Restated here rather than borrowed, so the arm compares the published figure
/// against an INDEPENDENT computation instead of against the function that produced it.
fn ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    numerator.saturating_mul(1_000_000) / denominator
}

#[test]
#[ignore = "seeds a store and reads its published status; run by name"]
fn every_published_ratio_is_rederivable_from_published_counters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("rederive-{index:06}"),
                value: vec![b'v'; VALUE],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }

    let published = engine
        .shard_stats(1)
        .expect("a loaded shard reports stats")
        .storage;

    println!();
    println!("  PUBLISHED COUNTERS");
    println!("    bytes_written          {:>12}", published.bytes_written);
    println!("    logical_bytes_written  {:>12}", published.logical_bytes_written);
    println!("    live_block_bytes       {:>12}", published.live_block_bytes);
    println!("    storage_slab_total     {:>12}", published.storage_slab_total_bytes);
    println!("    storage_slab_used      {:>12}", published.storage_slab_used_bytes);
    println!("    non_purged_physical    {:>12}", published.non_purged_physical_bytes);
    println!("    pending_discard_bytes  {:>12}", published.pending_discard_bytes);
    println!();
    println!("  PUBLISHED RATIOS, each beside the same figure recomputed from the counters above");

    // (name, published ppm, numerator, denominator)
    let identities: [(&str, u64, u64, u64); 5] = [
        (
            "storage_write_amplification_ppm",
            published.storage_write_amplification_ppm,
            published.bytes_written,
            published.logical_bytes_written,
        ),
        (
            "lifetime_write_amplification_ppm",
            published.lifetime_write_amplification_ppm,
            published.bytes_written,
            published.live_block_bytes,
        ),
        (
            "space_amplification_ppm",
            published.space_amplification_ppm,
            published.storage_slab_total_bytes,
            published.storage_slab_used_bytes,
        ),
        // THE FOURTH: the space figure whose terms can actually fall. Both of its halves are
        // published, which is the whole point of it being here -- the pinned one above divides
        // terms that cannot see a purge or an overwrite, and a consumer could not tell that from
        // the ppm figure alone.
        (
            "block_space_amplification_ppm",
            published.block_space_amplification_ppm,
            published.non_purged_physical_bytes,
            published.live_block_bytes,
        ),
        // THE FIFTH, AND THE ONLY IDENTITY HERE WHOSE NUMERATOR ADDS. It is the reason the retired
        // bytes are published as a counter beside the ratio instead of only inside it: without
        // `pending_discard_bytes` on the status, this figure would be the one ratio a consumer
        // could not check, which is precisely what the published denominators exist to prevent.
        //
        // IT IS NOT A SECOND SPELLING OF THE FOURTH. That one divides non-purged file bytes by the
        // live tally and is reload-independent; this one divides the live set plus what the index
        // has retired and not had collected, moves at the instant of the overwrite, and is derived
        // state that reads zero after a reload. They agree in the steady state and diverge after a
        // restart, which makes each the other's cross-check.
        (
            "recorded_space_amplification_ppm",
            published.recorded_space_amplification_ppm,
            published
                .live_block_bytes
                .saturating_add(published.pending_discard_bytes),
            published.live_block_bytes,
        ),
    ];

    let mut exercised = 0_usize;
    for (name, reported, numerator, denominator) in identities {
        let recomputed = ppm(numerator, denominator);
        println!(
            "    {name:<34} {reported:>9} published, {recomputed:>9} recomputed  \
             ({numerator} / {denominator})"
        );
        // DENOMINATOR FIRST. The engine reports 0 ppm when a denominator is unavailable, so an
        // identity over a zero denominator would hold at 0 == 0 while checking nothing.
        if denominator == 0 {
            println!(
                "      .. NOT exercised: the denominator is 0, so this identity would hold \
                 vacuously"
            );
            continue;
        }
        assert_eq!(
            reported, recomputed,
            "{name} publishes {reported} ppm, but recomputing it from the counters this status \
             also publishes -- {numerator} over {denominator} -- gives {recomputed}. Either the \
             ratio divides by something it does not publish, or a published counter is not the one \
             it divides by. Both make the figure unfalsifiable at the consumer, which is the whole \
             reason the denominators were published"
        );
        exercised += 1;
    }

    println!();
    println!("    {exercised} of {} identities exercised", identities.len());
    println!();

    // A PASS MUST NOT MEAN "every denominator was zero". At least the storage-write pair is
    // reachable on any corpus that wrote anything, so requiring two of four leaves room for the
    // live tally to be underived without letting a vacuous run report success -- and TWO of the
    // four now divide by it, so an underived tally costs two identities rather than one.
    //
    // AND THE FLOOR IS DELIBERATELY NOT RAISED TO THREE. Three would require the live tally to be
    // DERIVED for this arm to pass, which is a different claim from the one this floor makes: the
    // floor exists so that a run where every denominator happened to be zero cannot report success.
    // Turning it into a readiness assertion would make this arm fail for a reason it does not name,
    // and readiness belongs in an arm that says `is_ready` in its message. Two identities being
    // skipped together is the cost, and the line above is where a reader is told to expect it.
    assert!(
        exercised >= 2,
        "only {exercised} of {} identities had a non-zero denominator, so this arm checked almost \
         nothing. A store that has just written {KEYS} keys should be able to exercise at least the \
         storage-write and space pairs",
        identities.len()
    );

    // AND THE LEGACY FIELD IS STILL THE ONE IT ALWAYS WAS. `logical_write_amplification_ppm` is
    // frozen and misnamed: it carries the storage-write quantity. Pinned here so that publishing
    // the denominators cannot be read as having fixed it -- it has not, and the name is still
    // wrong.
    assert_eq!(
        published.logical_write_amplification_ppm, published.storage_write_amplification_ppm,
        "the frozen `logical_write_amplification_ppm` no longer equals the storage-write ratio it \
         has always carried. If it was deliberately re-pointed, this arm should say so rather than \
         keep asserting the old equality"
    );
}
