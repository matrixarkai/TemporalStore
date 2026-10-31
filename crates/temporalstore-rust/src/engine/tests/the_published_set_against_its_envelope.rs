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
        dir.path().join("blocks"),
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
        flat_dir.path().join("blocks"),
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

/// AMPLIFICATION COUNTED IN ITEMS MOVES, WHICH IS THE WHOLE REASON IT EXISTS.
///
/// # THE FIGURE IT SITS BESIDE CANNOT MOVE
///
/// `space_amplification_ppm` divides bytes by bytes and is pinned near 1.000x: its denominator is
/// slab-granular and blind at both ends, so a store at a true 5.0x publishes as 1.000x
/// (`the_space_ratio_denominator_is_slab_granular_and_blind`). Putting a fixed bound on that field
/// would be a closed gate -- the same shape as a budget whose unit exceeds the budget, and the same
/// reason `block_gc_min_slab_garbage_basis_points` is inert today.
///
/// `item_space_amplification_ppm` asks the same question of quantities that cannot be blind: item
/// records in the durable form over live objects. A count does not round to a slab.
///
/// # WHAT IS ASSERTED, IN THIS ORDER
///
///   1. **NON-VACUITY FIRST.** It must EXCEED 1.0 after overwrites. If a new figure cannot leave
///      1.0 it has inherited the defect it was built to escape, and everything after this
///      assertion would be measuring nothing. This is the control, so it comes first.
///   2. It must return to EXACTLY 1.0 after a dump. Not approximately: a reset that left the
///      figure above 1.0 would put a fixed bound out of reach downward and so make it unusable as
///      a trigger, which is precisely what separates a windowed ratio from a lifetime one.
///   3. The DENOMINATOR must be distinct live objects, not written records. If it counted records
///      the ratio would be 1.0 by construction and every reading above would be a tautology --
///      the one-field-two-quantities trap, so it is checked against the key count rather than
///      trusted.
///
/// rust-internal: measures this crate's own published counters, no product behaviour
///
///   cargo test -p temporalstore-rust --lib amplification_counted_in_items_moves_and_a_dump_resets_it -- --exact --nocapture
#[test]
fn amplification_counted_in_items_moves_and_a_dump_resets_it() {
    const KEYS: usize = 200;
    const VALUE_LEN: usize = 704;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let write_round = |round: usize| {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("item-{index:06}"),
                    value: vec![b'v'; VALUE_LEN],
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
    };
    let figure = || -> (u64, u64) {
        let storage = engine.shard_stats(1).expect("stats").storage.clone();
        (storage.item_space_amplification_ppm, storage.live_objects_with_logical_size)
    };

    // ---------- WRITTEN ONCE: one record an object, so the durable form IS the live set.
    write_round(0);
    let (once_ppm, once_live) = figure();
    println!();
    println!("  written once          {:>9.3}x over {once_live} live objects", once_ppm as f64 / 1e6);
    assert_eq!(
        KEYS as u64, once_live,
        "denominator: {once_live} live objects for {KEYS} distinct keys written once"
    );
    assert_eq!(
        1_000_000, once_ppm,
        "written once the item ratio must be exactly 1.0 and read {once_ppm} ppm -- one record an \
         object is the definition of no amplification"
    );

    // ---------- OVERWRITTEN: the superseded records are still in the durable form.
    write_round(1);
    let (twice_ppm, twice_live) = figure();
    println!("  overwritten once      {:>9.3}x over {twice_live} live objects", twice_ppm as f64 / 1e6);

    // 3. THE DENOMINATOR IS DISTINCT OBJECTS, NOT RECORDS. Checked before the ratio is read as
    //    meaning anything: if it had moved with the writes, the ratio would be 1.0 by construction.
    assert_eq!(
        KEYS as u64, twice_live,
        "the denominator moved to {twice_live} after overwriting the SAME {KEYS} keys. It must \
         count distinct live objects, not written records -- if it counts records this ratio is \
         1.0 by construction and every figure here is a tautology."
    );
    // 1. NON-VACUITY. The reason this field exists is that the byte figure beside it cannot do this.
    assert!(
        twice_ppm > 1_000_000,
        "the item ratio read {twice_ppm} ppm after every key was overwritten, so it did NOT leave \
         1.0. That is the defect this field was added to escape -- a figure that cannot rise \
         cannot carry a trigger, and the rest of this arm would be measuring nothing. Check that \
         `durable_items` is incremented on the overwrite arm of `set_string_logical_size` and not \
         only on the insert arm."
    );
    assert_eq!(
        2_000_000, twice_ppm,
        "two records an object should read exactly 2.0x and read {twice_ppm} ppm"
    );

    // ---------- DUMPED: a dump re-serializes the live set, so the superseded records are gone.
    let candidates: Vec<u32> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.bucket_index.bucket_map.keys().copied().collect()
    };
    assert!(!candidates.is_empty(), "no bucket to dump, so the reset below would be a no-op");
    let lifecycle = engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: candidates.clone(),
        max_dump_buckets_per_round: candidates.len(),
        ..Default::default()
    });
    std::hint::black_box(&lifecycle);
    let (dumped_ppm, dumped_live) = figure();
    println!("  after a dump          {:>9.3}x over {dumped_live} live objects", dumped_ppm as f64 / 1e6);
    println!();

    // 2. EXACTLY 1.0, because a trigger needs a reachable floor.
    assert_eq!(
        KEYS as u64, dumped_live,
        "the dump changed the live object count to {dumped_live}; it rewrites records, it does not \
         delete objects"
    );
    assert_eq!(
        1_000_000, dumped_ppm,
        "after a dump the item ratio must be EXACTLY 1.0 and read {dumped_ppm} ppm. A dump \
         re-serializes the live set, so no superseded record survives it. A reset that left this \
         above 1.0 would put a fixed bound out of reach downward, which is what makes a lifetime \
         ratio ungateable."
    );
    assert!(
        dumped_ppm < twice_ppm,
        "the dump did not lower the ratio ({twice_ppm} -> {dumped_ppm} ppm), so nothing reset and \
         the figure would climb with uptime rather than tracking the store"
    );
}

/// THE 2.0x ITEM CAP FIRES, DECLINES WHEN UNSET, AND CLEARS ITSELF.
///
/// # WHAT IS BEING FOLLOWED
///
/// The design being matched caps amplification by CONSTRUCTION rather than publishing a ratio: its
/// full-dump decision ORs in `total_item >= live_items * 2`, and a full dump re-serializes from the
/// live set, so the figure resets to 1.0 and cannot stay above the cap. 2_000_000 ppm is that 2.0x.
///
/// Our dump selection had no amplification predicate at all -- it picks DIRTY buckets, ordered by
/// `last_dump_sequence`, capped per round -- so this is the predicate, not a re-tuning of one.
///
/// # WHY ALL FOUR CHECKS, AND WHY IN THIS ORDER
///
/// "It fired" on its own is satisfied by a gate that always fires, so a one-sided check would pass
/// on a predicate that ignores its threshold entirely. The order below is the order in which a
/// mistake would otherwise hide:
///
///   1. **DECLINES when the knob is 0**, at a ratio that would otherwise trip it. This is the
///      control: it separates a working threshold from one that is never consulted.
///   2. **FIRES at the threshold**, and selects EVERY bucket rather than the dirty ones -- the
///      superseded records are spread across buckets that are not dirty now, so a dirty-only dump
///      would leave the ratio where it was.
///   3. **The ratio returns to 1.0** once that dump is applied, which is what makes a fixed cap a
///      bound rather than a line the figure sits above for ever.
///   4. **It then DECLINES again**, unprompted. That is the difference between a trigger and a
///      latch, and a latch would force a full dump every round for the rest of the shard's life.
///
/// rust-internal: drives this crate's own lifecycle plan, no product surface
///
///   cargo test -p temporalstore-rust --lib the_item_cap_fires_declines_and_clears_itself -- --exact --nocapture
#[test]
fn the_item_cap_fires_declines_and_clears_itself() {
    const KEYS: usize = 200;
    const VALUE_LEN: usize = 704;
    /// 2.0x, the value the design being followed uses.
    const CAP_PPM: u64 = 2_000_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let write_round = |round: usize| {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("cap-{index:06}"),
                    value: vec![b'v'; VALUE_LEN],
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
    };
    let ratio_ppm = || -> u64 {
        engine
            .shard_stats(1)
            .expect("stats")
            .storage
            .item_space_amplification_ppm
    };
    let plan_with = |ppm: u64| {
        engine.storage_lifecycle_plan(crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            item_amplification_full_dump_ppm: ppm,
            ..Default::default()
        })
    };

    write_round(0);
    write_round(1);
    let ratio = ratio_ppm();
    println!();
    println!("  after one overwrite round, item ratio {:.3}x", ratio as f64 / 1e6);
    assert_eq!(
        CAP_PPM, ratio,
        "the fixture must sit exactly AT the cap for the two sides below to be about the threshold \
         rather than about the corpus; it read {ratio} ppm"
    );

    // ---------- 1. THE CONTROL: unset means it does not fire, at this very ratio.
    let off = plan_with(0);
    println!(
        "  knob 0           forced={} selected={} dirty={}",
        off.full_dump_forced_by_item_amplification,
        off.selected_dump_buckets.len(),
        off.dirty_buckets.len()
    );
    assert!(
        !off.full_dump_forced_by_item_amplification,
        "the cap fired with the knob at 0, at a ratio of {ratio} ppm. Then the threshold is not \
         being consulted and the 'fires' check below would pass on a gate that always fires."
    );

    // ---------- 2. IT FIRES, AND TAKES EVERY BUCKET.
    let on = plan_with(CAP_PPM);
    println!(
        "  knob {CAP_PPM}   forced={} selected={} dirty={}",
        on.full_dump_forced_by_item_amplification,
        on.selected_dump_buckets.len(),
        on.dirty_buckets.len()
    );
    assert!(
        on.full_dump_forced_by_item_amplification,
        "the cap did NOT fire at {ratio} ppm against a {CAP_PPM} ppm threshold"
    );
    assert!(
        on.selected_dump_buckets.len() >= off.selected_dump_buckets.len(),
        "the forced dump selected {} buckets against {} for the dirty-only plan. A full dump must \
         cover at least what the dirty plan would: the superseded records this retires sit in \
         buckets that are not dirty now, so selecting fewer would leave the ratio where it was.",
        on.selected_dump_buckets.len(),
        off.selected_dump_buckets.len()
    );
    assert!(
        !on.selected_dump_buckets.is_empty(),
        "the cap fired and selected no bucket, so the dump it forces is a no-op"
    );

    // ---------- 3. APPLYING IT RETURNS THE RATIO TO 1.0.
    let report = engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
        shard_id: 1,
        item_amplification_full_dump_ppm: CAP_PPM,
        ..Default::default()
    });
    std::hint::black_box(&report);
    let after = ratio_ppm();
    println!("  after the forced dump, item ratio {:.3}x", after as f64 / 1e6);
    assert_eq!(
        1_000_000, after,
        "after the forced full dump the ratio must be exactly 1.0 and read {after} ppm. A cap that \
         does not bring the figure back under itself is not a bound, it is a line the figure sits \
         above permanently."
    );

    // ---------- 4. AND IT DECLINES AGAIN ON ITS OWN: a trigger, not a latch.
    let cleared = plan_with(CAP_PPM);
    println!(
        "  knob {CAP_PPM} again   forced={}",
        cleared.full_dump_forced_by_item_amplification
    );
    println!();
    assert!(
        !cleared.full_dump_forced_by_item_amplification,
        "the cap still fires after the dump brought the ratio to {after} ppm, below its own \
         {CAP_PPM} ppm threshold. That is a latch rather than a trigger, and it would force a full \
         dump of the whole shard on every round for the rest of its life."
    );

    // ---------- 5. AND IT COVERS BUCKETS THAT ARE NOT DIRTY, which is the property that makes
    // ---------- it a FULL dump rather than a differently-triggered dirty one.
    //
    // Everything above ran on a fixture where EVERY bucket was dirty, so `selected` and `dirty`
    // were the same set and a cap implemented as "the dirty dump, triggered by a ratio" would
    // have passed every check. This phase separates them: the dump above left nothing dirty, so
    // rewriting only HALF the keys leaves half the buckets clean while the ratio climbs on the
    // whole shard. A dirty-only dump would then retire the superseded records of half the shard
    // and leave the ratio above the cap for ever.
    let half = KEYS / 2;
    for round in 0..2 {
        for index in 0..half {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("cap-{index:06}"),
                    value: vec![b'v'; VALUE_LEN],
                },
            });
            assert!(response.status.ok, "half round {round} write {index}: {:?}", response.status);
        }
    }
    let partial = ratio_ppm();
    let dirty_only = plan_with(0);
    let forced = plan_with(CAP_PPM);
    println!();
    println!(
        "  half the keys rewritten: ratio {:.3}x, dirty {} buckets, forced selects {}",
        partial as f64 / 1e6,
        dirty_only.selected_dump_buckets.len(),
        forced.selected_dump_buckets.len()
    );
    println!();
    assert!(
        partial >= CAP_PPM,
        "rewriting half the keys twice left the ratio at {partial} ppm, below the {CAP_PPM} ppm \
         cap, so the comparison below is not about a firing cap. The fixture needs more rounds."
    );
    assert!(
        forced.full_dump_forced_by_item_amplification,
        "the cap did not fire at {partial} ppm"
    );
    assert!(
        dirty_only.selected_dump_buckets.len() < forced.selected_dump_buckets.len(),
        "the forced dump selected {} buckets and the dirty-only plan selected {} -- they must \
         DIFFER here, or this phase is still comparing a fixture where every bucket is dirty and \
         the cap could be implemented as a dirty dump with a ratio trigger and pass anyway.",
         forced.selected_dump_buckets.len(),
         dirty_only.selected_dump_buckets.len()
    );
    assert!(
        !dirty_only.full_dump_forced_by_item_amplification,
        "the knob is 0 in the dirty-only plan and the cap fired anyway"
    );
}
