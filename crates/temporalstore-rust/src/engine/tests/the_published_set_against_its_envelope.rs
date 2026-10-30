// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE PUBLISHED AMPLIFICATION SET AGAINST THE ENVELOPE IT IS HELD TO.
//!
//! The store publishes a set of amplification ratios. The envelope those ratios are read against
//! is a space ceiling of **5.0x**, a write trigger of **3.0x**, and a floor of **1.0x** under every
//! one of them. This arm drives one store through the three states those bounds describe -- written
//! once, rewritten, then consolidated -- and reports every published ratio beside the bound that
//! applies to it, so "we are inside the envelope" is a table rather than a claim.
//!
//! # THE FLOOR IS NOT A PROPERTY OF THE STORE, IT IS A PROPERTY OF THE WORKLOAD
//!
//! A 1.0x floor assumes a store writes at least as many bytes as it is given, and
//! `storage_write_amplification_ppm` is physical-over-logical. This campaign had been quoting
//! **0.137x** for it and calling the floor inapplicable. That figure is real and it is also
//! FIXTURE-DEPENDENT: it was measured on a run of one repeated byte, which compresses to almost
//! nothing.
//!
//! Measured here on both: the codec ratio is **below** the floor on compressible values and
//! **above** it on incompressible ones, where framing slightly exceeds the payload. So the
//! honest statement is that this ratio STRADDLES the floor depending on what is written, and a
//! gate at 1.0x would fire on one workload and pass on the other. This arm asserts the straddle,
//! which is a claim about the ratio; asserting either single value would be a claim about a
//! fixture.
//!
//! The arm's first version asserted the ratio was below 1.0x and FAILED at 1.017x, which is how
//! the fixture dependence was found.
//!
//! # WHICH BOUND APPLIES TO WHICH RATIO IS NOT OBVIOUS, SO IT IS SPELLED OUT
//!
//! Four of the published ratios are write ratios and they do not share a bound:
//!
//!   * `storage_write_amplification_ppm` -- the CODEC. No bound; it is below 1.0x by design.
//!   * `lifetime_write_amplification_ppm` -- a lifetime numerator over a current denominator, so
//!     it rises with uptime and NO fixed bound can apply. Its own field note says so.
//!   * `write_amplification_since_compaction_ppm` -- the windowed one, and the only write ratio a
//!     3.0x trigger can be read against, because a compaction brings it down.
//!   * `user_write_amplification_ppm` -- user bytes on BOTH sides, which makes it easy to read
//!     and does NOT give it a bound. Its numerator is lifetime and only its denominator is
//!     current, so like the lifetime ratio above it rises with uptime, and a consolidation
//!     RAISES it: bytes written, no user data added. A 3.0x reading means the workload has
//!     written its live data three times over in the caller's own units SINCE THE SHARD LOADED
//!     -- useful to read, impossible to gate. The first version of this arm labelled it
//!     `<= 3.0x` in the table below and then printed 4.000x three lines later; that is how the
//!     mislabel was caught, and the assertion near the end of the arm is what keeps it off.
//!
//! And two are space ratios: `space_amplification_ppm`, which is slab-granular and pinned near
//! 1.0x, and `block_space_amplification_ppm`, which is the one that moves.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_published_set_against_its_envelope -- --nocapture

use crate::engine::TemporalEngine;
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 300;
/// Exactly representable as a size class -- `(16 + 6) << 5` -- so the user-byte figures are exact.
const VALUE_LEN: usize = 704;

/// The ceiling the space ratios are read against, in ppm.
const SPACE_CEILING_PPM: u64 = 5_000_000;
/// The trigger the windowed write ratio is read against, in ppm.
const WRITE_TRIGGER_PPM: u64 = 3_000_000;
/// The floor the set is nominally held to, in ppm. Recorded because one ratio is BELOW it by design.
const NOMINAL_FLOOR_PPM: u64 = 1_000_000;

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

fn write_round(engine: &TemporalEngine, round: usize) {
    for index in 0..KEYS {
        let value = noisy(
            (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ round as u64,
            VALUE_LEN,
        );
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: format!("env-{index:06}"), value },
        });
        assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
    }
}

fn report(label: &str, s: &crate::control::ShardCanonicalStorageStats) {
    let x = |ppm: u64| ppm as f64 / 1_000_000.0;
    println!("  === {label} ===");
    println!(
        "    {:<44} {:>10} {:>9}  {}",
        "ratio", "ppm", "x", "bound"
    );
    for (name, ppm, bound) in [
        ("storage_write_amplification_ppm", s.storage_write_amplification_ppm, "none (codec; below 1.0x by design)"),
        ("lifetime_write_amplification_ppm", s.lifetime_write_amplification_ppm, "none (rises with uptime)"),
        ("write_amplification_since_compaction_ppm", s.write_amplification_since_compaction_ppm, "<= 3.0x trigger"),
        ("user_write_amplification_ppm", s.user_write_amplification_ppm, "none (lifetime over current; rises with uptime)"),
        ("space_amplification_ppm", s.space_amplification_ppm, "<= 5.0x (slab-granular, pinned)"),
        ("block_space_amplification_ppm", s.block_space_amplification_ppm, "<= 5.0x (this one moves)"),
    ] {
        println!("    {:<44} {:>10} {:>8.3}x  {}", name, ppm, x(ppm), bound);
    }
    println!(
        "    terms: logical_written={} live_block={} live_user={} covered={}",
        s.logical_bytes_written, s.live_block_bytes, s.live_user_bytes, s.live_objects_with_logical_size
    );
    println!();
}

#[test]
fn the_published_set_against_its_envelope() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    println!();

    // ---------- 1. WRITTEN ONCE. Every ratio is at its floor, or below it.
    write_round(&engine, 0);
    let once = engine.shard_stats(1).expect("stats").storage.clone();
    report("written once", &once);

    assert_eq!(
        KEYS as u64, once.live_objects_with_logical_size,
        "coverage must be complete before any figure here is read as describing the live set"
    );
    assert_eq!(
        NOMINAL_FLOOR_PPM, once.user_write_amplification_ppm,
        "written once, the user-byte ratio must be exactly 1.0x and read {} ppm",
        once.user_write_amplification_ppm
    );
    // ---------- THE FLOOR STRADDLE, measured on a SECOND store whose values compress.
    //
    // The store above writes incompressible bytes and its codec ratio is ABOVE the floor:
    // framing slightly exceeds the payload. A store of compressible values is BELOW it. Neither
    // number alone says anything about the ratio -- together they say the floor is decided by the
    // workload, so it cannot be a gate.
    let flat_dir = tempfile::tempdir().expect("tempdir");
    let flat = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        flat_dir.path().join("cache"),
        flat_dir.path().join("pages"),
        flat_dir.path().join("indexes"),
    );
    flat.load_shard(1);
    for index in 0..KEYS {
        let response = flat.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("env-{index:06}"),
                value: vec![b'v'; VALUE_LEN],
            },
        });
        assert!(response.status.ok, "compressible write {index}: {:?}", response.status);
    }
    let flat_stats = flat.shard_stats(1).expect("stats").storage.clone();
    println!();
    println!("  THE 1.0x FLOOR IS DECIDED BY THE WORKLOAD, NOT THE STORE");
    println!(
        "    codec ratio {:.3}x on incompressible values, {:.3}x on compressible ones",
        once.storage_write_amplification_ppm as f64 / 1_000_000.0,
        flat_stats.storage_write_amplification_ppm as f64 / 1_000_000.0
    );
    println!();
    assert!(
        flat_stats.storage_write_amplification_ppm < NOMINAL_FLOOR_PPM,
        "on compressible values the codec ratio must fall BELOW the 1.0x floor and read {} ppm. If it does not, the compression path stopped working, which is a bigger finding than the bound.",
        flat_stats.storage_write_amplification_ppm
    );
    assert!(
        once.storage_write_amplification_ppm > flat_stats.storage_write_amplification_ppm,
        "the codec ratio read {} ppm on incompressible values and {} on compressible ones. The incompressible store must read HIGHER, or these two fixtures do not differ in the way this straddle rests on.",
        once.storage_write_amplification_ppm,
        flat_stats.storage_write_amplification_ppm
    );

    // ---------- 2. REWRITTEN. The write ratios climb; the space ratios follow the garbage.
    for round in 1..3 {
        write_round(&engine, round);
    }
    let rewritten = engine.shard_stats(1).expect("stats").storage.clone();
    report("rewritten three times", &rewritten);

    assert_eq!(
        3 * NOMINAL_FLOOR_PPM, rewritten.user_write_amplification_ppm,
        "three passes over the same keys must read exactly 3.0x in user bytes and read {} ppm",
        rewritten.user_write_amplification_ppm
    );
    // AT THE TRIGGER, NOT PAST IT -- which is what a trigger is for: this is the state that should
    // cause a consolidation, and the next phase performs one.
    assert!(
        rewritten.write_amplification_since_compaction_ppm >= WRITE_TRIGGER_PPM,
        "after three rewrites with no compaction the windowed write ratio should have reached the \
         {WRITE_TRIGGER_PPM} ppm trigger and read {} ppm -- if it has not, the trigger would never \
         fire on this workload and the bound is unreachable rather than satisfied",
        rewritten.write_amplification_since_compaction_ppm
    );
    // AND THE SPACE RATIO THAT CAN SEE GARBAGE IS INSIDE ITS CEILING.
    assert!(
        rewritten.block_space_amplification_ppm <= SPACE_CEILING_PPM,
        "the block-granular space ratio read {} ppm, outside the {SPACE_CEILING_PPM} ppm ceiling, \
         after only three rewrites",
        rewritten.block_space_amplification_ppm
    );

    // ---------- 3. CONSOLIDATED. The windowed ratio comes back inside the trigger; the lifetime
    // ---------- one does not, because nothing resets it.
    let plan = engine.storage_lifecycle_plan(crate::StorageLifecycleRequest {
        shard_id: 1,
        ..crate::StorageLifecycleRequest::default()
    });
    let drain = crate::engine::compaction_drain_block_slab_ids(&plan.reclaim_candidates);
    assert!(
        !drain.is_empty(),
        "no slab is drainable, so this phase cannot consolidate and the recovery below would be \
         asserted over a no-op"
    );
    let compaction = engine
        .compact_shard_blocks_draining(1, drain)
        .expect("a compaction round over a loaded shard");
    assert!(
        compaction.rewritten_block_refs > 0,
        "the compaction relocated nothing, so no window opened"
    );
    let consolidated = engine.shard_stats(1).expect("stats").storage.clone();
    report("after one consolidation", &consolidated);

    assert!(
        consolidated.write_amplification_since_compaction_ppm < WRITE_TRIGGER_PPM,
        "after a consolidation the windowed write ratio must be back inside the \
         {WRITE_TRIGGER_PPM} ppm trigger and read {} ppm. A trigger that does not clear when the \
         thing it asks for is done would fire for ever.",
        consolidated.write_amplification_since_compaction_ppm
    );
    assert!(
        consolidated.lifetime_write_amplification_ppm
            >= rewritten.lifetime_write_amplification_ppm,
        "the lifetime write ratio fell across a consolidation ({} -> {}). It must not: nothing \
         resets its numerator and a compaction only adds to it. The two ratios are different \
         quantities and this is what keeps them apart.",
        rewritten.lifetime_write_amplification_ppm,
        consolidated.lifetime_write_amplification_ppm
    );

    // AND THE USER-BYTE RATIO IS LIFETIME TOO, which is why the table above gives it no bound.
    //
    // It is tempting to read a user-byte ratio against the 3.0x trigger: both sides are in the
    // caller's own units, so 3.0x reads as -- the workload rewrote its live data three times.
    // It still cannot be gated. The numerator is lifetime and only the denominator is current,
    // so a consolidation RAISES it: the compaction writes bytes and adds no user data. Asserting
    // that rise is what keeps a fixed bound off it, and what would fail if it ever became
    // windowed and the table's -- no bound -- stopped being true.
    assert!(
        consolidated.user_write_amplification_ppm > rewritten.user_write_amplification_ppm,
        "the user-byte write ratio did not rise across a consolidation ({} -> {}). It must: the \
         compaction wrote bytes and added no user data, so a lifetime numerator over a current \
         denominator can only climb. If it ever falls here the ratio has become windowed -- and \
         the bound column above must stop saying it has none, because a 3.0x trigger would then \
         be readable against it.",
        rewritten.user_write_amplification_ppm,
        consolidated.user_write_amplification_ppm
    );

    // ---------- THE SUMMARY, so a reader sees the envelope in one place.
    println!("  WHERE THE SET SITS AGAINST THE ENVELOPE");
    println!(
        "    space ceiling 5.0x      block_space {:.3}x  (slab-granular reads {:.3}x)",
        consolidated.block_space_amplification_ppm as f64 / 1_000_000.0,
        consolidated.space_amplification_ppm as f64 / 1_000_000.0
    );
    println!(
        "    write trigger 3.0x      windowed    {:.3}x  (reached {:.3}x before consolidating)",
        consolidated.write_amplification_since_compaction_ppm as f64 / 1_000_000.0,
        rewritten.write_amplification_since_compaction_ppm as f64 / 1_000_000.0
    );
    println!(
        "    user bytes (no bound)   user_write  {:.3}x at three rewrites, {:.3}x after compacting",
        rewritten.user_write_amplification_ppm as f64 / 1_000_000.0,
        consolidated.user_write_amplification_ppm as f64 / 1_000_000.0
    );
    println!(
        "    the 1.0x floor is workload-decided: codec {:.3}x here, {:.3}x on compressible values",
        consolidated.storage_write_amplification_ppm as f64 / 1_000_000.0,
        flat_stats.storage_write_amplification_ppm as f64 / 1_000_000.0
    );
    println!();
}
