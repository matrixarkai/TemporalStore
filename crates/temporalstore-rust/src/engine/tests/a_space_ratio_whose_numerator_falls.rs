// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SPACE RATIO THAT WOULD MOVE, measured from counters that exist today.
//!
//! # THE PUBLISHED ONE IS BLIND AT BOTH ENDS
//!
//! `space_amplification_ppm` is `total_known_physical_bytes / live_physical_bytes`, and each half
//! fails to reflect reclamation:
//!
//!   * the DENOMINATOR is slab-granular. `summarize_slabs` adds a slab's whole `physical_bytes`
//!     when its state is `Active` or `Sealed`, so a slab counts fully live however many of its
//!     blocks are dead. Measured: it tracked `bytes_written` to the byte across five rewrites of
//!     every key, holding the ratio at 1.000x where the block-granular truth was 5.000x.
//!   * the NUMERATOR is accumulated BEFORE the state match, so a `Purged` slab whose files are
//!     deleted still contributes its full size. Measured: a purge freed 1,232,400 B and
//!     `reclaimable` fell to 0, while this figure did not move.
//!
//! Both are monotonically increasing. The field name is honest -- "total **known**" is a
//! known-slabs figure, not disk occupancy -- so what is wrong is reading it as a space numerator.
//!
//! # THE CORRECT PAIR NEEDS NO NEW ACCOUNTING
//!
//! ```text
//! numerator   = total_known_physical_bytes - purged_physical_bytes   // files still on disk
//! denominator = the maintained block-granular live tally             // blocks the index points at
//! ```
//!
//! The numerator falls when a purge deletes files; the denominator falls when an overwrite retires
//! a block's index entry. Both move in the direction the ratio claims to measure, and both are
//! already computed -- `purged_physical_bytes` on the slab summary, the tally on the shard.
//!
//! # ITS FLOOR IS ABOVE 1.0x, AND THAT IS A PROPERTY RATHER THAN A DEFECT
//!
//! The numerator is FILE bytes and the denominator is BLOCK PAYLOAD bytes, so the ratio includes
//! slab headers, block envelopes and record framing the denominator excludes. A reader expecting
//! 1.0x at zero garbage will misread it. This is the same structure the ratio is modelled on --
//! bytes on disk per byte of live data -- so it is stated rather than corrected.
//!
//! # IT IS PUBLISHED NOW, AND THIS ARM CHECKS THE FIELD
//!
//! This used to say that publishing meant a field on `ShardCanonicalStorageStats`, an assignment in
//! `persistence.rs` and another identity in the re-derivability arm, with another lane mid-flight in
//! all three. That lane landed, and the three pieces exist: `block_space_amplification_ppm`, its
//! numerator `non_purged_physical_bytes`, and the fourth identity in
//! `every_published_ratio_is_rederivable`.
//!
//! So the final section no longer measures a proposal — it **restates both terms independently** (a
//! local fold over the tally, a local subtraction over the slab summary) and asserts the published
//! field equals them. An arm that computes a figure the engine also publishes and never compares
//! the two is exactly how a ratio carried one name while computing another for as long as it did.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib a_space_ratio_whose_numerator_falls -- --ignored --nocapture

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

fn ppm(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 { 0 } else { numerator.saturating_mul(1_000_000) / denominator }
}

fn as_x(value: u64) -> f64 {
    value as f64 / 1_000_000.0
}

#[test]
#[ignore = "rewrites 1,000 keys four times; run by name"]
fn a_space_ratio_whose_numerator_falls() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // (label, published numerator, published denominator, proposed numerator, proposed denominator)
    let read = |label: String| -> (String, u64, u64, u64, u64) {
        let slabs = engine.block_store.slab_summary();
        let live_blocks = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            if shard.bucket_index.block_slab_live.is_ready() {
                shard
                    .bucket_index
                    .block_slab_live
                    .iter()
                    .fold(0_u64, |total, (_, tally)| total.saturating_add(tally.bytes))
            } else {
                0
            }
        };
        (
            label,
            slabs.total_known_physical_bytes,
            slabs.live_physical_bytes,
            slabs
                .total_known_physical_bytes
                .saturating_sub(slabs.purged_physical_bytes),
            live_blocks,
        )
    };

    let mut rows = Vec::new();
    for round in 0..=ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("space-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
        rows.push(read(format!("after {} write(s)", round + 1)));
    }

    println!();
    println!("  TWO SPACE RATIOS OVER ONE STORE -- {KEYS} keys x {VALUE} B, incompressible");
    println!();
    println!(
        "    {:<20} {:>11} {:>11} {:>8} | {:>11} {:>11} {:>8}",
        "", "total known", "live slab", "PUBLISHED", "non-purged", "live blocks", "PROPOSED"
    );
    for (label, total, live_slab, non_purged, live_blocks) in &rows {
        println!(
            "    {:<20} {:>11} {:>11} {:>7.3}x | {:>11} {:>11} {:>7.3}x",
            label,
            total,
            live_slab,
            as_x(ppm(*total, *live_slab)),
            non_purged,
            live_blocks,
            as_x(ppm(*non_purged, *live_blocks)),
        );
    }
    println!();

    let (_, first_total, first_live_slab, first_np, first_lb) = rows[0].clone();
    let (_, last_total, last_live_slab, last_np, last_lb) = rows[rows.len() - 1].clone();
    println!(
        "    published: {:.3}x -> {:.3}x     proposed: {:.3}x -> {:.3}x",
        as_x(ppm(first_total, first_live_slab)),
        as_x(ppm(last_total, last_live_slab)),
        as_x(ppm(first_np, first_lb)),
        as_x(ppm(last_np, last_lb))
    );
    println!(
        "    the proposed numerator is FILE bytes and its denominator BLOCK PAYLOAD bytes, so its"
    );
    println!("    floor is above 1.0x even at zero garbage -- framing is in one half only.");
    println!();

    // ---- NON-VACUITY. Both ratios are over counters that can be zero.
    for (label, total, live_slab, non_purged, live_blocks) in &rows {
        assert!(*total > 0, "{label}: no slab bytes at all, so neither ratio means anything");
        assert!(*live_slab > 0, "{label}: the published denominator is zero");
        assert!(
            *live_blocks > 0,
            "{label}: the live tally is zero, so the proposed denominator is unavailable and this \
             arm cannot compare the two"
        );
        assert!(*non_purged > 0, "{label}: the proposed numerator is zero");
    }

    // ---- THE PUBLISHED RATIO IS PINNED, which is the defect and is asserted so that re-pointing
    // ---- its denominator makes this arm fail and say what moved.
    assert!(
        (as_x(ppm(last_total, last_live_slab)) - 1.0).abs() < 0.01,
        "the published space ratio read {:.3}x after {} rewrites. It has been pinned at 1.0x \
         because a live slab contributes its whole file -- if it now moves, the denominator \
         changed and this comparison needs restating",
        as_x(ppm(last_total, last_live_slab)),
        ROUNDS
    );

    // ---- AND THE PROPOSED ONE MOVES, which is what makes the first assertion a defect rather
    // ---- than a property of the workload. The live set never changed size; only its storage did.
    assert!(
        ppm(last_np, last_lb) > ppm(first_np, first_lb).saturating_mul(2),
        "the proposed ratio went {:.3}x -> {:.3}x across {} rewrites. Its denominator counts only \
         the blocks the index still points at, so dead blocks must push it up; if it does not \
         move, the tally is tracking the file rather than the index",
        as_x(ppm(first_np, first_lb)),
        as_x(ppm(last_np, last_lb)),
        ROUNDS
    );

    // ---- THE PUBLISHED FIELD IS THE FIGURE THIS ARM DEFINES. Both terms are restated from the
    // ---- engine's own structures above and compared to what the status reports, so a drift
    // ---- between the definition and the assignment fails here rather than being published.
    let published = engine
        .shard_stats(1)
        .expect("a loaded shard reports stats")
        .storage;
    println!(
        "    published block_space_amplification_ppm = {} ppm ({:.3}x), over {} / {}",
        published.block_space_amplification_ppm,
        as_x(published.block_space_amplification_ppm),
        published.non_purged_physical_bytes,
        published.live_block_bytes
    );
    println!();
    assert_eq!(
        published.non_purged_physical_bytes, last_np,
        "the status publishes {} non-purged bytes where this arm computes {last_np} from the slab \
         summary it reads directly. The numerator is `total_known - purged`; a disagreement means \
         the assignment is over different terms than the definition",
        published.non_purged_physical_bytes
    );
    assert_eq!(
        published.live_block_bytes, last_lb,
        "the status publishes {} live block bytes where this arm folds {last_lb} out of the \
         index's own tally. A disagreement means the published denominator is not the tally",
        published.live_block_bytes
    );
    assert_eq!(
        published.block_space_amplification_ppm,
        ppm(last_np, last_lb),
        "the status publishes {} ppm where {last_np} over {last_lb} is {} ppm. The ratio must be \
         exactly its two published terms, or a consumer cannot falsify it -- which is the reason \
         both terms are fields at all",
        published.block_space_amplification_ppm,
        ppm(last_np, last_lb)
    );
    assert!(
        published.block_space_amplification_ppm > published.space_amplification_ppm,
        "the two space ratios read {} and {} ppm after {ROUNDS} rewrites of every key. The pinned \
         one cannot see a dead block inside a live slab, so the block-granular figure must be \
         strictly higher here -- if they agree, the new field is dividing slab-granular terms too",
        published.block_space_amplification_ppm,
        published.space_amplification_ppm
    );

    // ---- AND THE LIVE SET REALLY DID NOT GROW, or the rise above is just more data.
    let live_figures: Vec<u64> = rows.iter().map(|row| row.4).collect();
    let smallest = live_figures.iter().copied().min().unwrap_or(0);
    let largest = live_figures.iter().copied().max().unwrap_or(0);
    assert!(
        largest <= smallest.saturating_mul(2),
        "the live tally ranged {smallest}..{largest} B while the same {KEYS} keys were rewritten at \
         a fixed value size. A live figure that moves with bytes WRITTEN rather than bytes live is \
         the defect this arm is about"
    );
}
