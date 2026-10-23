// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LIVE AND STALE SHARES ARE A PARTITION OF THE BYTES ON DISK, and must behave like one.
//!
//! # WHY A PARTITION IS A STRONGER PUBLISHED SHAPE THAN A RATIO
//!
//! `live_bytes_ppm` and `stale_bytes_ppm` are computed over the same whole —
//! `non_purged_physical_bytes` — so they **sum to 1,000,000** within integer floor rounding. That
//! identity is checkable from the published numbers alone.
//!
//! None of the four amplification ratios on the same struct has that property. Each divides one
//! quantity by another with nothing constraining the answer, which is why
//! `logical_write_amplification_ppm` could carry a name that did not describe its own arithmetic for
//! as long as it did: no identity over the set would have failed, and
//! `the_published_amplification_ratios_are_the_ratio_they_name` pins self-consistency rather than
//! agreement with any definition.
//!
//! # TWO ASSERTIONS, BECAUSE EACH CATCHES WHAT THE OTHER CANNOT
//!
//!   * **The parts sum.** A term computed over the wrong whole fails here immediately.
//!   * **The stale share rises under rewrite, while the live set does not grow.** A figure can sum
//!     correctly and still be constant for the wrong reason — which is exactly how the
//!     slab-granular `space_amplification_ppm` passed every check it had while pinned at 1.000x
//!     over five rewrites of every key. Rewriting the same keys is the only instrument that
//!     separates "nothing is stale" from "stale is not being counted", and asserting BOTH halves is
//!     what makes either one evidence.
//!
//! # THE FLOOR IS EXACTLY ZERO, WHICH THIS ARM FOUND AND ITS OWN DOC GOT WRONG FIRST
//!
//! This section predicted a small non-zero stale share on the first round, because the live term is
//! block-payload bytes and the whole is file bytes, so framing would fall on the stale side.
//! **Measured: exactly 0.00%**, with the parts summing to 1,000,000.
//!
//! The terms agree because `live_block_bytes` sums `BlockAddress::length` — what was STORED, after
//! compression and including the block envelope — and `non_purged_physical_bytes` is the file size
//! of those same bytes. Both sides are stored bytes.
//!
//! Framing is real against a different denominator: 536,000 B on disk for 512,000 B of USER bytes
//! (`the_payload_on_all_three_media`), which is stored bytes over PRE-COMPRESSION bytes — the
//! mixed-unit division these fields exist to avoid. The equality is now asserted below, so if a
//! framing term ever does appear on one side it fails here instead of looking like a documented
//! floor.
//!
//! rust-internal: measures this crate's own published counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_live_and_stale_shares_are_a_partition -- --ignored --nocapture

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 1_000;
const VALUE: usize = 256;
const ROUNDS: usize = 4;

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

fn as_pct(ppm: u64) -> f64 {
    ppm as f64 / 10_000.0
}

#[test]
#[ignore = "rewrites 1,000 keys four times; run by name"]
fn the_live_and_stale_shares_are_a_partition() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // (label, live_ppm, stale_ppm, live bytes, whole bytes)
    let mut rows: Vec<(String, u64, u64, u64, u64)> = Vec::new();
    for round in 0..=ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("partition-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
        let published = engine
            .shard_stats(1)
            .expect("a loaded shard reports stats")
            .storage;
        rows.push((
            format!("after {} write(s)", round + 1),
            published.live_bytes_ppm,
            published.stale_bytes_ppm,
            published.live_block_bytes,
            published.non_purged_physical_bytes,
        ));
    }

    println!();
    println!("  THE STORED IMAGE AS PARTS OF A WHOLE -- {KEYS} keys x {VALUE} B, incompressible");
    println!();
    println!(
        "    {:<20} {:>11} {:>11} {:>10} {:>10} {:>12}",
        "", "live B", "whole B", "live %", "stale %", "sum (ppm)"
    );
    for (label, live_ppm, stale_ppm, live, whole) in &rows {
        println!(
            "    {:<20} {:>11} {:>11} {:>9.2}% {:>9.2}% {:>12}",
            label,
            live,
            whole,
            as_pct(*live_ppm),
            as_pct(*stale_ppm),
            live_ppm + stale_ppm
        );
    }
    println!();

    // ---- NON-VACUITY FIRST. Every term can be zero for its own reason.
    for (label, live_ppm, _, live, whole) in &rows {
        assert!(*whole > 0, "{label}: nothing is on disk, so the partition is over nothing");
        assert!(
            *live > 0,
            "{label}: the live tally is zero, so the partition reports everything stale and the \
             rise asserted below would be meaningless"
        );
        assert!(*live_ppm > 0, "{label}: the live share is 0 ppm while {live} B are live");
    }

    // ---- THE PARTS SUM TO 1,000,000. Floor division can cost one ppm, so the tolerance is 1 and
    // ---- not a fraction -- a tolerance wide enough to absorb a real error would defeat the whole
    // ---- reason this shape was chosen over another ratio.
    for (label, live_ppm, stale_ppm, live, whole) in &rows {
        let sum = live_ppm + stale_ppm;
        assert!(
            sum.abs_diff(1_000_000) <= 1,
            "{label}: the live and stale shares are {live_ppm} + {stale_ppm} = {sum} ppm, not \
             1,000,000. They are published as a PARTITION of {whole} B with {live} B live, so they \
             must be over the same whole -- a sum off by more than floor rounding means one of them \
             divides by something else"
        );
    }

    // ---- A FRESH STORE IS ENTIRELY LIVE, pinned as an equality rather than left as the
    // ---- "floor is above zero" hedge this arm's doc used to carry. Both terms are stored bytes,
    // ---- so the first round must read 0 ppm stale; if a framing term ever lands on one side only,
    // ---- this fails and names it instead of the discrepancy passing as a documented floor.
    let (_, round_one_live_ppm, round_one_stale_ppm, round_one_live, round_one_whole) =
        rows[0].clone();
    assert_eq!(
        round_one_live, round_one_whole,
        "the first round reports {round_one_live} B live against {round_one_whole} B on disk. \
         Nothing has been overwritten yet, so every stored byte is still pointed at and the two \
         must be equal -- a difference is either framing counted on one side only, or a block the \
         index lost"
    );
    assert_eq!(
        round_one_stale_ppm, 0,
        "the first round reports {round_one_stale_ppm} ppm stale on a store where nothing has been \
         overwritten. Both terms are stored bytes, so this must be exactly zero"
    );
    assert_eq!(
        round_one_live_ppm, 1_000_000,
        "the first round reports {round_one_live_ppm} ppm live where the whole image is live"
    );

    // ---- AND THE STALE SHARE RISES, which is what makes a correct sum evidence rather than
    // ---- arithmetic. Four rewrites of every key leave four fifths of the bytes on disk dead.
    let (_, first_live_ppm, first_stale_ppm, _, _) = rows[0].clone();
    let (_, last_live_ppm, last_stale_ppm, _, _) = rows[rows.len() - 1].clone();
    println!(
        "    live {:.2}% -> {:.2}%     stale {:.2}% -> {:.2}%",
        as_pct(first_live_ppm),
        as_pct(last_live_ppm),
        as_pct(first_stale_ppm),
        as_pct(last_stale_ppm)
    );
    println!(
        "    both terms are STORED bytes -- BlockAddress::length against the file size of those"
    );
    println!(
        "    same bytes -- so a fresh store reads exactly 0% stale. Framing shows up against USER"
    );
    println!("    bytes (1.047x on disk), not between two stored-byte terms.");
    println!();
    assert!(
        last_stale_ppm > first_stale_ppm.saturating_add(500_000),
        "the stale share went {first_stale_ppm} -> {last_stale_ppm} ppm across {ROUNDS} rewrites \
         of every key. Each round leaves the previous round's blocks on disk and unreferenced, so \
         by the last round most of the image must be stale; a share that does not move means the \
         live term is tracking the file rather than the index -- which is precisely what \
         `space_amplification_ppm` does, and why it reads 1.000x forever"
    );

    // ---- AND THE LIVE SET DID NOT GROW, or the rise above is just more data arriving.
    let live_bytes: Vec<u64> = rows.iter().map(|row| row.3).collect();
    let smallest = live_bytes.iter().copied().min().unwrap_or(0);
    let largest = live_bytes.iter().copied().max().unwrap_or(0);
    assert!(
        largest <= smallest.saturating_mul(2),
        "the live byte figure ranged {smallest}..{largest} across the rewrites. The same {KEYS} \
         keys are overwritten every round, so the live set must stay about constant -- if it grew, \
         the stale share rose because more data arrived and this arm is measuring the workload \
         rather than the accounting"
    );
}
