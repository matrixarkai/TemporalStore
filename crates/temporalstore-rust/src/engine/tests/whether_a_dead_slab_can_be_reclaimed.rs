// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER A WHOLLY DEAD SLAB CAN BE RECLAIMED, and what the footprint does when it is.
//!
//! # EVERY EARLIER ARM MEASURED A RATIO THAT COULD NOT MOVE
//!
//! `the_amplification_set_under_rewrite` showed the published space ratio pinned at 1.000x while
//! the block-granular figure reached 5.000x, and showed five reclaim rounds each adding one copy
//! of the live set. Neither says whether the footprint can come DOWN, for two reasons:
//!
//!   * every key landed in one slab, so overwriting them all left that slab holding the live
//!     copies -- no slab could become wholly dead, and a collector with nothing to collect is not
//!     evidence either way;
//!   * the purge phase was never driven. `purge_delayed_destroy` is a field of
//!     `StorageLifecycleRequest`, NOT of `StorageManagerCycleRequest`, so a fixture driving only
//!     the cycle runs the compaction phase alone however many rounds it runs.
//!
//! # SO THIS KILLS ONE WHOLE SLAB
//!
//! Write until several slabs are sealed, find the oldest, overwrite only the keys whose blocks
//! live in it, and leave every other slab untouched. That slab is then entirely dead, which is the
//! only shape where a purge has something to do.
//!
//! HOW MUCH TO WRITE IS DISCOVERED, not hardcoded against a capacity constant: the arm writes
//! batches until the summary reports sealed slabs. A fixture built on a guessed threshold that is
//! never reached runs green and measures nothing.
//!
//! # AND THE AGE GATE IS OVERRIDDEN DELIBERATELY
//!
//! `DELAYED_DESTROY_MIN_AGE_MS` is one hour, so no test can outwait the quarantine. The final purge
//! uses `purge_delayed_destroy_slabs_older_than(0)` -- an explicit override of the production
//! delay, which existing block-store tests also use. What it proves is narrower than a production
//! purge: that the slab was ELIGIBLE and the destroy path works, not that the delay would elapse.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib whether_a_dead_slab_can_be_reclaimed -- --ignored --nocapture

use crate::engine::reports::{StorageLifecycleRequest, StorageManagerCycleRequest};
use crate::types::{Command, ExecuteRequest};

const VALUE: usize = 4_096;
/// Keys per slab. Small, because the slab boundary is forced by a roll rather than reached by
/// volume -- 20,000 keys at 4 KiB wrote 82 MB and sealed nothing.
const BATCH: usize = 300;
/// Slabs to fill and seal before the one that dies. Two sealed plus one active is the minimum
/// shape: a slab that can die while others live.
const SEALED_SLABS: usize = 2;

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

#[test]
#[ignore = "seals several slabs then kills one; run by name"]
fn whether_a_dead_slab_can_be_reclaimed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    // ---- FILL AND SEAL BY ROLLING, because volume does not seal a slab. A batch, a roll, a
    // ---- batch, a roll, then a final batch into the slab that stays active.
    let mut written = 0_usize;
    for slab in 0..=SEALED_SLABS {
        for index in written..written + BATCH {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("slab-{index:06}"),
                    value: noisy_value(index, 0, VALUE),
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
        written += BATCH;
        if slab < SEALED_SLABS {
            // THE DELIBERATE OVERRIDE. Production seals when its own policy decides to; forcing it
            // is the only way to reach the state under test. What this proves is therefore
            // narrower than a production roll: that the destroy path works on an ELIGIBLE slab,
            // not that the policy would produce one at this size.
            let roll = engine.block_store.roll_slab().expect("the slab rolls");
            println!("  rolled after {written} keys: {roll:?}");
        }
    }
    let summary = engine.block_store.slab_summary();
    println!();
    println!(
        "  seeded {written} keys x {VALUE} B: {} active, {} sealed slab(s), {} B on disk",
        summary.active_slabs, summary.sealed_slabs, summary.total_known_physical_bytes
    );
    assert!(
        summary.sealed_slabs >= SEALED_SLABS as u64,
        "{} slab(s) sealed after {SEALED_SLABS} rolls. The fixture needs a slab that can die while \
         others live, and the rolls are what provide it",
        summary.sealed_slabs
    );

    // ---- FIND THE OLDEST SLAB AND THE KEYS LIVING IN IT.
    let fractions = engine
        .block_store
        .slab_live_fractions()
        .expect("the slab live fractions read");
    let victim = fractions
        .iter()
        .map(|f| f.block_slab_id)
        .min()
        .expect("at least one slab exists");
    let doomed: Vec<String> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard
            .strings()
            .iter()
            .filter(|(_, slot)| slot.block_slab_id() == victim)
            .map(|(key, _)| key.to_string())
            .collect()
    };
    println!(
        "  slab {victim} is the oldest and holds {} of the {written} keys",
        doomed.len()
    );
    assert!(
        !doomed.is_empty(),
        "no key resolves to slab {victim}, so overwriting 'its' keys would kill nothing and the \
         arm would measure a purge with no candidate"
    );
    assert!(
        doomed.len() < written,
        "every one of the {written} keys resolves to slab {victim}, so there is only one slab in \
         play after all and no slab can die while others live"
    );

    // ---- OVERWRITE ONLY THOSE KEYS, so that slab becomes entirely dead.
    for (round, key) in doomed.iter().enumerate() {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.clone(),
                value: noisy_value(round, 1, VALUE),
            },
        });
        assert!(response.status.ok, "overwrite of {key}: {:?}", response.status);
    }

    let describe = |label: &str| -> (u64, u64, u64) {
        let summary = engine.block_store.slab_summary();
        let fractions = engine
            .block_store
            .slab_live_fractions()
            .unwrap_or_default();
        let victim_row = fractions.iter().find(|f| f.block_slab_id == victim);
        println!();
        println!("  {label}");
        println!(
            "    total {} B, live(slab-granular) {} B, reclaimable {} B, {} slab(s) known",
            summary.total_known_physical_bytes,
            summary.live_physical_bytes,
            summary.reclaimable_physical_bytes,
            fractions.len()
        );
        match victim_row {
            Some(row) => println!(
                "    slab {victim}: physical {} B, logical(ever) {} B, live {} B, garbage {} bp",
                row.physical_bytes, row.logical_bytes, row.live_bytes, row.garbage_basis_points
            ),
            None => println!("    slab {victim}: GONE from the fractions listing"),
        }
        (
            summary.total_known_physical_bytes,
            summary.reclaimable_physical_bytes,
            victim_row.map(|r| r.physical_bytes).unwrap_or(0),
        )
    };

    let (total_after_kill, _, victim_after_kill) = describe("AFTER KILLING SLAB'S KEYS");

    // ---- PHASE ONE: COMPACTION, which is what quarantines a dead slab.
    let buckets: Vec<u32> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.bucket_index.bucket_map.keys().copied().collect()
    };
    let cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: false,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        selected_dump_buckets: buckets.clone(),
        max_dump_buckets_per_round: buckets.len(),
        min_undumped_wal_records: 1,
        min_undumped_wal_bytes: 1,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        ..StorageManagerCycleRequest::default()
    });
    std::hint::black_box(&cycle);
    let (_, reclaimable_after_compact, _) = describe("AFTER A COMPACTION ROUND");

    // ---- PHASE TWO: THE PURGE, on the request type that owns it.
    let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: buckets.clone(),
        max_dump_buckets_per_round: buckets.len(),
        purge_delayed_destroy: true,
        ..Default::default()
    });
    let purge_metrics: Vec<String> = lifecycle
        .storage_lifecycle_metrics
        .iter()
        .filter(|(name, value)| name.contains("purge") && **value > 0)
        .map(|(name, value)| format!("{name}={value}"))
        .collect();
    println!();
    println!(
        "  purge requested through the lifecycle path; non-zero purge metrics: {}",
        if purge_metrics.is_empty() { "NONE".to_string() } else { purge_metrics.join(" ") }
    );
    let (total_after_purge, _, _) = describe("AFTER THE PRODUCTION PURGE");

    // ---- AND THE AGE OVERRIDE, because the quarantine is an hour and no test outwaits it.
    let forced = engine
        .block_store
        .purge_delayed_destroy_slabs_older_than(0)
        .expect("the age-zero purge runs");
    println!();
    println!(
        "  AGE-ZERO PURGE (a deliberate override of the {} ms production delay): {:?}",
        crate::block_store::DELAYED_DESTROY_MIN_AGE_MS,
        forced
    );
    let (total_after_forced, _, victim_after_forced) = describe("AFTER THE AGE-ZERO PURGE");

    println!();
    println!("  ===== DID THE FOOTPRINT COME DOWN?");
    println!("    after killing the slab's keys  {total_after_kill} B");
    println!("    after the production purge     {total_after_purge} B");
    println!("    after the age-zero purge       {total_after_forced} B");
    println!(
        "    the dead slab itself: {victim_after_kill} B -> {victim_after_forced} B"
    );
    println!();

    // ---- NON-VACUITY: the fixture has to have produced a quarantined slab, or the purge had no
    // ---- candidate and neither outcome below means anything.
    assert!(
        reclaimable_after_compact > 0 || total_after_forced < total_after_kill,
        "after compaction nothing was reclaimable and the purges freed nothing, so no slab ever \
         became a purge candidate. The fixture did not kill a slab -- either the overwrite did not \
         displace its blocks, or the compaction did not quarantine it, and until one of those is \
         true this arm cannot say whether reclaim works"
    );

    // ---- THE FOOTPRINT MUST NOT GROW ACROSS THE PURGES. What it comes down BY is printed rather
    // ---- than asserted: that is a policy question. Growing across a purge is not.
    assert!(
        total_after_forced <= total_after_purge,
        "the age-zero purge took the footprint from {total_after_purge} B to {total_after_forced} \
         B. A purge removes files; it cannot add them"
    );
}
