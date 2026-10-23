// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE STALE RECORD'S TWO PARTITIONS, CHECKED FROM THE PUBLISHED STATUS.
//!
//! # WHY NOT THE ARM THAT ALREADY CHECKS THEM
//!
//! `the_overwrite_records_what_it_retired` asserts both identities over `SlabStaleSummary`, inside
//! the engine, where every term is in hand. This asserts them over `ShardCanonicalStorageStats` —
//! what a consumer receives — and until the eight unpublished figures were carried onto it, neither
//! identity was checkable there, because each was missing a term.
//!
//! The two arms fail for different reasons, which is the point of having both: the inner one if the
//! record's arithmetic breaks, this one if the status stops carrying a term that arithmetic needs.
//! **An assignment that drops a field passes the inner arm.**
//!
//! # THE TWO PARTITIONS
//!
//! ```text
//! pending_discard_bytes + stale_bytes_discarded          == stale_bytes_recorded
//! by_overwrite + by_removal + by_sweep                   == stale_bytes_recorded
//! ```
//!
//! Integers on both sides, so these are exact — no rounding, no tolerance. A partition constrains
//! itself in a way a ratio cannot, which is why the published figures are shaped this way.
//!
//! # AND A SUM THAT ACCOUNTS CAN STILL BE ATTRIBUTED WRONGLY
//!
//! Three terms summing to the right total, with the weight in the wrong term, is a passing sum and a
//! wrong answer. So this also asserts that the cause the fixture CREATES is the cause the status
//! attributes: a pure-overwrite workload must put every retired byte in `by_overwrite` and none in
//! the other two. That is the shape of a cause-label error measured once at 100% wrong — a pure
//! overwrite corpus reporting its bytes under removal — and a sum alone would not have caught it.
//!
//! rust-internal: measures this crate's own published counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_published_stale_record_accounts_for_itself -- --ignored --nocapture

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 1_000;
const VALUE: usize = 256;
const ROUNDS: usize = 3;

fn noisy_value(index: usize, round: usize, len: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add((round as u64).wrapping_mul(0x1234_5678_9ABC_DEF0))
        | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

fn ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    ((numerator as u128 * 1_000_000u128) / denominator as u128) as u64
}

#[test]
#[ignore = "rewrites 1,000 keys three times; run by name"]
fn the_published_stale_record_accounts_for_itself() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // A PURE OVERWRITE WORKLOAD, deliberately: no deletes and no expiry, so every retired byte has
    // exactly one possible cause and a misattribution cannot hide behind a mixed fixture.
    for round in 0..=ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("accounted-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
    }

    let s = engine
        .shard_stats(1)
        .expect("a loaded shard reports stats")
        .storage;

    println!();
    println!("  THE PUBLISHED STALE RECORD -- {KEYS} keys x {VALUE} B, {ROUNDS} rewrites, overwrite only");
    println!();
    println!("    recorded                       {:>12}", s.stale_bytes_recorded);
    println!("      pending                      {:>12}", s.pending_discard_bytes);
    println!("      discarded                    {:>12}", s.stale_bytes_discarded);
    println!("      ---- sum                     {:>12}", s.pending_discard_bytes + s.stale_bytes_discarded);
    println!();
    println!("      by overwrite                 {:>12}", s.stale_bytes_by_overwrite);
    println!("      by removal                   {:>12}", s.stale_bytes_by_removal);
    println!("      by sweep                     {:>12}", s.stale_bytes_by_sweep);
    println!(
        "      ---- sum                     {:>12}",
        s.stale_bytes_by_overwrite + s.stale_bytes_by_removal + s.stale_bytes_by_sweep
    );
    println!();
    println!("    pending block refs             {:>12}", s.pending_discard_block_refs);
    println!("    merged ranges                  {:>12}", s.merged_discard_range_count);
    println!("    compaction candidate ranges    {:>12}", s.compaction_candidate_range_count);
    println!("    fragmented ranges              {:>12}", s.fragmented_range_count);
    println!("    DROPPED ranges (placement)     {:>12}", s.dropped_discard_range_count);
    println!("    slabs tracked / pruned         {:>12} / {}", s.stale_slabs_tracked, s.stale_slabs_pruned);
    println!("    discard backlog                {:>12} ppm", s.discard_backlog_ppm);
    println!();

    // ---- NON-VACUITY. Every assertion below holds at zero.
    assert!(
        s.stale_bytes_recorded > 0,
        "nothing was recorded as retired after {ROUNDS} rewrites of {KEYS} keys, so both partitions \
         below hold at 0 == 0 and this arm checks nothing"
    );

    // ---- PARTITION ONE: pending and discarded account for everything recorded.
    assert_eq!(
        s.pending_discard_bytes + s.stale_bytes_discarded,
        s.stale_bytes_recorded,
        "pending {} + discarded {} = {}, against {} recorded. These are a partition of the recorded \
         bytes, so they must account for all of them exactly -- integers on both sides, no \
         rounding. A shortfall means the status is carrying one of the terms from somewhere other \
         than the record this total came from",
        s.pending_discard_bytes,
        s.stale_bytes_discarded,
        s.pending_discard_bytes + s.stale_bytes_discarded,
        s.stale_bytes_recorded
    );

    // ---- PARTITION TWO: the three causes account for everything recorded.
    let by_cause = s.stale_bytes_by_overwrite + s.stale_bytes_by_removal + s.stale_bytes_by_sweep;
    assert_eq!(
        by_cause, s.stale_bytes_recorded,
        "the causes sum to {by_cause} against {} recorded -- overwrite {}, removal {}, sweep {}. \
         Every retired byte has exactly one cause, so these must account for all of them; a \
         shortfall means a fourth cause exists and is not published, and an excess means one byte \
         is counted under two",
        s.stale_bytes_recorded,
        s.stale_bytes_by_overwrite,
        s.stale_bytes_by_removal,
        s.stale_bytes_by_sweep
    );

    // ---- AND THE ATTRIBUTION IS RIGHT, not merely complete. A sum can account perfectly with the
    // ---- weight in the wrong term; this fixture only ever overwrites, so there is exactly one
    // ---- correct answer. Measured once at 100% wrong in this area -- a pure-overwrite corpus
    // ---- reporting its bytes under removal -- and the sum above would have passed that.
    assert_eq!(
        s.stale_bytes_by_overwrite, s.stale_bytes_recorded,
        "a workload that only overwrites attributed {} of {} retired bytes to overwrite. There are \
         no deletes and no expiry here, so every byte has one possible cause and anything in \
         removal ({}) or sweep ({}) is a cause label taken from the mechanism rather than the call \
         site",
        s.stale_bytes_by_overwrite,
        s.stale_bytes_recorded,
        s.stale_bytes_by_removal,
        s.stale_bytes_by_sweep
    );
    assert_eq!(s.stale_bytes_by_removal, 0, "no deletes were issued");
    assert_eq!(s.stale_bytes_by_sweep, 0, "no expiry or sweep ran");

    // ---- THE BACKLOG RATIO IS THE PARTITION'S OWN PPM, recomputed rather than trusted.
    assert_eq!(
        s.discard_backlog_ppm,
        ppm(s.pending_discard_bytes, s.stale_bytes_recorded),
        "the status publishes {} ppm of backlog where {} pending over {} recorded is {} ppm. The \
         ratio must be exactly its two published terms or a consumer cannot check it",
        s.discard_backlog_ppm,
        s.pending_discard_bytes,
        s.stale_bytes_recorded,
        ppm(s.pending_discard_bytes, s.stale_bytes_recorded)
    );

    // ---- THE PLACEMENT FIGURES ARE EXACT ON THIS FIXTURE, asserted rather than hoped. Non-zero
    // ---- here would mean the per-slab cap refused ranges, and every WHERE figure beside it --
    // ---- compaction candidates, fragmented ranges -- would be a lower bound instead of a count.
    assert_eq!(
        s.dropped_discard_range_count, 0,
        "the per-slab range cap refused {} ranges on this corpus, so the placement figures beside \
         it are lower bounds rather than counts. Either the fixture outgrew the cap -- in which \
         case it is no longer measuring what this arm says -- or the cap shrank",
        s.dropped_discard_range_count
    );

    // ---- AND THE RECORD IS ACTUALLY TRACKING SLABS, or the placement figures are zero for a
    // ---- reason that has nothing to do with placement.
    assert!(
        s.stale_slabs_tracked > 0,
        "the record tracks no slabs while reporting {} retired bytes",
        s.stale_bytes_recorded
    );
}
