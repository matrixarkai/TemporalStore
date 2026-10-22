// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE AMPLIFICATION SET UNDER REWRITE, and what a reclaim cycle gives back.
//!
//! # EVERY FIGURE BEFORE THIS ONE WAS 1.0x BY CONSTRUCTION
//!
//! Every amplification arm in this area writes each key exactly once. On a write-once corpus
//! nothing is stale, so bytes-ever-written equals bytes-live and the rewrite ratio is 1.0x, and
//! physical equals live so the space ratio is 1.0x too. Those are not findings about good
//! behaviour, they are arithmetic about the fixture. The ceilings these ratios exist to be held
//! against -- 5.0x on space, 3.0x on write -- can only be approached by a workload that produces
//! DEAD bytes, and none had been run.
//!
//! This one overwrites the same keys repeatedly, reports the set after each round, then runs a real
//! reclaim cycle and reports it again. It answers three things the write-once corpus cannot:
//!
//!   1. does the rewrite ratio RISE with rewrite pressure, or is it inert?
//!   2. does space amplification climb as dead blocks accumulate, and how far?
//!   3. does a reclaim cycle bring it back, and back to what?
//!
//! # EVERY RATIO DIVIDES STORED BYTES BY STORED BYTES
//!
//! There are two quantities in this store called logical. `logical_bytes_written` counts the user's
//! bytes as offered, BEFORE compression; a block's length -- what the live tally sums -- is what was
//! stored, AFTER it. A ratio with one on each side reports the compression factor multiplied by
//! whatever it claimed to measure. So the rewrite and space ratios here are stored-over-stored, and
//! the compression ratio is reported separately as the one quantity whose subject IS that mismatch.
//!
//! # THE VALUES ARE INCOMPRESSIBLE, AND THAT IS NOT INCIDENTAL
//!
//! `vec![b'v'; n]` compresses to almost nothing, which makes stored bytes framing rather than
//! content: on that fixture `live_physical_bytes` reads the same 70,000 B at value 256, 1,024 and
//! 4,096 alike, so a stored-byte denominator stops tracking the data. Every ratio here stands on
//! stored bytes, so the fixture must produce bytes that survive the write path.
//!
//! rust-internal: measures this crate's own counters, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib the_amplification_set_under_rewrite -- --ignored --nocapture

use crate::engine::reports::StorageManagerCycleRequest;
use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 2_000;
const VALUE: usize = 256;
const ROUNDS: usize = 4;
/// SEVERAL RECLAIM ROUNDS, NOT ONE. A compaction rewrites live data before releasing the copy it
/// replaces, so a single round can legitimately end holding more than it began with. Whether
/// rounds CONVERGE is the question, and one round cannot answer it.
const RECLAIM_ROUNDS: usize = 5;

/// Values that cannot compress, so a stored-byte denominator means something.
///
/// An inlined xorshift64 keyed by key index and round, so each round writes genuinely different
/// bytes -- a rewrite with the SAME bytes is a workload a store is allowed to recognise and skip,
/// which would make this arm measure deduplication instead of rewrite pressure.
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

struct Snapshot {
    label: String,
    stored_written: u64,
    logical_written: u64,
    /// SLAB-GRANULAR, and this is the one the published space ratio divides by.
    ///
    /// `summarize_slabs` adds a slab's whole `physical_bytes` to it when the slab is `Active` or
    /// `Sealed`, so a slab counts fully live however many of its blocks are dead.
    live_slab_granular: u64,
    /// BLOCK-GRANULAR, from the maintained `BlockSlabLiveIndex` tally.
    ///
    /// `SlabLiveTally::bytes` sums `BlockAddress::length` over the blocks the index still points
    /// at, and a block leaves the tally when its index entry does -- so an overwrite decrements
    /// it. `None` when the tally has not been derived, because a provisional zero must not be
    /// divided by.
    live_block_granular: Option<u64>,
    physical: u64,
    reclaimable: u64,
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 { 0.0 } else { numerator as f64 / denominator as f64 }
}

impl Snapshot {
    /// Rewrite pressure as the published denominator computes it.
    fn rewrite_slab(&self) -> f64 {
        ratio(self.stored_written, self.live_slab_granular)
    }
    /// Rewrite pressure against the blocks the index still points at.
    fn rewrite_block(&self) -> f64 {
        ratio(self.stored_written, self.live_block_granular.unwrap_or(0))
    }
    /// Space as the published denominator computes it.
    fn space_slab(&self) -> f64 {
        ratio(self.physical, self.live_slab_granular)
    }
    /// Space against the blocks the index still points at.
    fn space_block(&self) -> f64 {
        ratio(self.physical, self.live_block_granular.unwrap_or(0))
    }
    /// Stored bytes per user byte offered: COMPRESSION, the one ratio that mixes the units on
    /// purpose because that relationship is its subject.
    fn compression(&self) -> f64 {
        if self.logical_written == 0 {
            0.0
        } else {
            self.stored_written as f64 / self.logical_written as f64
        }
    }
}

fn snapshot(engine: &crate::TemporalEngine, label: impl Into<String>) -> Snapshot {
    let store = engine.block_store.stats();
    let slabs = engine.block_store.slab_summary();
    // THE BLOCK-GRANULAR FIGURE, read from the shard's own maintained tally rather than
    // recomputed here, so this arm and the GC's candidate selection are reading one number.
    let live_block_granular = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        if shard.bucket_index.block_slab_live.is_ready() {
            Some(
                shard
                    .bucket_index
                    .block_slab_live
                    .iter()
                    .fold(0_u64, |total, (_, tally)| total.saturating_add(tally.bytes)),
            )
        } else {
            None
        }
    };
    Snapshot {
        label: label.into(),
        stored_written: store.bytes_written,
        logical_written: store.logical_bytes_written,
        live_slab_granular: slabs.live_physical_bytes,
        live_block_granular,
        physical: slabs.total_known_physical_bytes,
        reclaimable: slabs.reclaimable_physical_bytes,
    }
}

#[test]
#[ignore = "writes 2,000 keys five times over and runs a reclaim cycle; run by name"]
fn the_amplification_set_under_rewrite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let mut shots = Vec::new();
    for round in 0..=ROUNDS {
        for index in 0..KEYS {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("amp-{index:06}"),
                    value: noisy_value(index, round, VALUE),
                },
            });
            assert!(response.status.ok, "round {round} write {index}: {:?}", response.status);
        }
        shots.push(snapshot(&engine, if round == 0 {
            "after 1 write".to_string()
        } else {
            format!("after {} writes", round + 1)
        }));
    }

    // THE RECLAIM CYCLE, through the production round rather than by reaching into the store. Every
    // bucket is named because reclaim wants a dump manifest at each bucket's current generation,
    // and a bucket that went clean without being dumped at that generation has none.
    for round in 1..=RECLAIM_ROUNDS {
        // THE BUCKET SET IS RE-READ EACH ROUND. Reclaim wants a dump manifest matching each
        // bucket's CURRENT generation, and a round that compacts changes generations -- a set
        // captured once goes stale and later rounds would ask for manifests that do not exist.
        let buckets: Vec<u32> = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            shard.bucket_index.bucket_map.keys().copied().collect()
        };
        assert!(
            !buckets.is_empty(),
            "round {round}: no bucket to dump, so the reclaim round has nothing to act on"
        );
        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
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
        std::hint::black_box(&report);

        // THE PURGE IS A SEPARATE PHASE ON A SEPARATE REQUEST TYPE. `purge_delayed_destroy` is a
        // field of `StorageLifecycleRequest`, not of the cycle request, so a fixture that drives
        // only the cycle runs the compaction phase alone and can never free a quarantined slab --
        // however many rounds it runs. Driven here so that "nothing was freed" is a result rather
        // than an omission.
        let purge = engine.apply_storage_lifecycle(
            crate::engine::reports::StorageLifecycleRequest {
                shard_id: 1,
                selected_dump_buckets: buckets.clone(),
                max_dump_buckets_per_round: buckets.len(),
                purge_delayed_destroy: true,
                ..Default::default()
            },
        );
        let purge_metrics: Vec<String> = purge
            .storage_lifecycle_metrics
            .iter()
            .filter(|(name, value)| name.contains("purge") && **value > 0)
            .map(|(name, value)| format!("{name}={value}"))
            .collect();
        println!(
            "    round {round}: purge requested, non-zero purge metrics: {}",
            if purge_metrics.is_empty() {
                "NONE".to_string()
            } else {
                purge_metrics.join(" ")
            }
        );

        shots.push(snapshot(&engine, format!("after reclaim round {round}")));
    }

    println!();
    println!("  THE AMPLIFICATION SET UNDER REWRITE -- {KEYS} keys x {VALUE} B, incompressible");
    println!();
    println!(
        "    {:<22} {:>10} {:>10} {:>10} {:>10} {:>7} {:>7} {:>7} {:>7}",
        "",
        "stored wr",
        "live SLAB",
        "live BLOCK",
        "physical",
        "rw slab",
        "rw blk",
        "sp slab",
        "sp blk"
    );
    for shot in &shots {
        println!(
            "    {:<22} {:>10} {:>10} {:>10} {:>10} {:>6.3}x {:>6.3}x {:>6.3}x {:>6.3}x",
            shot.label,
            shot.stored_written,
            shot.live_slab_granular,
            shot.live_block_granular
                .map(|bytes| bytes.to_string())
                .unwrap_or_else(|| "not ready".to_string()),
            shot.physical,
            shot.rewrite_slab(),
            shot.rewrite_block(),
            shot.space_slab(),
            shot.space_block(),
        );
    }
    println!();
    println!(
        "    reclaimable stayed {} throughout; compression {:.3}x",
        shots.iter().map(|s| s.reclaimable).max().unwrap_or(0),
        shots[0].compression()
    );
    println!();

    let first = &shots[0];
    // The last write snapshot is the one before the first reclaim round.
    let last_write = &shots[ROUNDS];
    let after_reclaim = &shots[shots.len() - 1];
    println!(
        "    SLAB-granular (published):  rewrite {:.3}x -> {:.3}x,  space {:.3}x -> {:.3}x",
        first.rewrite_slab(),
        last_write.rewrite_slab(),
        first.space_slab(),
        last_write.space_slab()
    );
    println!(
        "    BLOCK-granular (the tally): rewrite {:.3}x -> {:.3}x,  space {:.3}x -> {:.3}x",
        first.rewrite_block(),
        last_write.rewrite_block(),
        first.space_block(),
        last_write.space_block()
    );
    println!(
        "    after {RECLAIM_ROUNDS} reclaim rounds: space {:.3}x slab, {:.3}x block",
        after_reclaim.space_slab(),
        after_reclaim.space_block()
    );
    println!("    the ceilings these ratios are held against: 5.0x space, 3.0x write");
    println!();
    println!(
        "    block-granular space across the reclaim rounds: {}",
        shots[ROUNDS..]
            .iter()
            .map(|s| format!("{:.3}x", s.space_block()))
            .collect::<Vec<_>>()
            .join(" -> ")
    );
    println!(
        "    and the footprint on disk: {}",
        shots[ROUNDS..]
            .iter()
            .map(|s| s.physical.to_string())
            .collect::<Vec<_>>()
            .join(" -> ")
    );
    println!();
    // WHY IT DOES NOT COME DOWN HERE, STATED RATHER THAN LEFT TO BE INFERRED. The purge stage is
    // requested above, so this is not a missing flag; what refuses is the quarantine age. A rising
    // number with no explanation beside it is how a fixture artefact gets reported as a leak.
    println!(
        "    THE QUARANTINE AGE IS {} ms ({} minutes), and these rounds run in seconds -- so no",
        crate::block_store::DELAYED_DESTROY_MIN_AGE_MS,
        crate::block_store::DELAYED_DESTROY_MIN_AGE_MS / 60_000
    );
    println!(
        "    slab quarantined by this workload is old enough to purge, with or without the flag."
    );
    println!(
        "    Each round therefore compacts the live set into a new slab and leaves the old one"
    );
    println!(
        "    quarantined. That is the two-phase design running with its second phase age-gated,"
    );
    println!("    NOT a collector that refuses to release. What the rounds show is the FIRST");
    println!("    phase repeated, and convergence is not observable within a test's lifetime.");
    println!();
    println!(
        "    SO THE FINDING THAT SURVIVES: space amplification cannot fall within the quarantine"
    );
    println!(
        "    age of the rewrite that caused it -- and the published ratio cannot see any of it"
    );
    println!("    regardless, because its denominator is slab-granular.");
    println!();

    // ---- NON-VACUITY FIRST. Every figure above is a ratio, and a zero denominator satisfies any
    // ---- bound while measuring nothing.
    for shot in &shots {
        assert!(
            shot.live_slab_granular > 0,
            "{}: the slab-granular figure is zero, so the published ratios are vacuous here",
            shot.label
        );
        assert!(
            shot.stored_written > 0,
            "{}: nothing was written, so the fixture never ran",
            shot.label
        );
    }

    // ---- THE FIXTURE HAS TO ACTUALLY REWRITE. If the store recognised the repeated keys and
    // ---- skipped the writes, every ratio below would be the write-once figure under a new name.
    assert!(
        last_write.stored_written > first.stored_written,
        "writing every key {} times stored {} B against {} B after one pass, so the rewrites did \
         not reach the store and this arm is measuring a write-once corpus",
        ROUNDS + 1,
        last_write.stored_written,
        first.stored_written
    );

    // ---- THE PUBLISHED DENOMINATOR IS BLIND, AND THAT IS THE FINDING. `summarize_slabs` adds a
    // ---- slab's whole `physical_bytes` to `live_physical_bytes` when the slab is `Active` or
    // ---- `Sealed`, so the denominator grows in step with the numerator and both ratios sit at
    // ---- 1.0x however much of the slab is dead. Asserted rather than described, so that changing
    // ---- the denominator to a block-granular one makes this arm FAIL and say what moved.
    assert!(
        (last_write.space_slab() - 1.0).abs() < 0.01,
        "the slab-granular space ratio read {:.3}x after {} rewrites of every key. It has been \
         pinned at 1.0x because a live slab contributes its whole file size -- if it now moves, \
         the denominator changed and this arm's finding needs restating",
        last_write.space_slab(),
        ROUNDS + 1
    );

    // ---- AND THE BLOCK-GRANULAR ONE MOVES, which is what makes the first assertion a defect
    // ---- rather than a property of the workload. The tally decrements when an index entry is
    // ---- replaced, so an overwrite retires the block it displaced.
    let block_live = last_write
        .live_block_granular
        .expect("the maintained tally was never derived, so the comparison cannot be made");
    assert!(
        block_live < last_write.live_slab_granular,
        "the block-granular tally reports {} B live and the slab-granular figure {} B after {} \
          rewrites of every key. If they agree, either nothing was actually overwritten or the \
          tally is not retiring displaced blocks -- and then there is no second denominator to \
          compare against",
        block_live,
        last_write.live_slab_granular,
        ROUNDS + 1
    );
    assert!(
        last_write.space_block() > first.space_block() * 1.5,
        "space against the live tally went {:.3}x -> {:.3}x across {} rewrites. A denominator \
         that counts only live blocks must make the ratio climb as dead ones pile up; if it does \
         not, the tally is tracking the file rather than the index",
        first.space_block(),
        last_write.space_block(),
        ROUNDS
    );

    // ---- AND THE TALLY DOES NOT GROW WITH THE REWRITES, which is the other half of the same
    // ---- point: the live set never changed size, only its storage did. Every snapshot holds the
    // ---- same keys at the same value size, so a tally that tracked bytes written rather than
    // ---- bytes live would show this growing -- which is exactly what the slab figure does.
    let live_figures: Vec<u64> = shots.iter().filter_map(|s| s.live_block_granular).collect();
    assert_eq!(
        live_figures.len(),
        shots.len(),
        "the maintained tally was not derived at every snapshot, so the comparison has gaps"
    );
    let smallest = live_figures.iter().copied().min().unwrap_or(0);
    let largest = live_figures.iter().copied().max().unwrap_or(0);
    assert!(
        largest <= smallest.saturating_mul(2),
        "the live tally ranged {smallest}..{largest} B while the live SET never changed -- the \
         same {KEYS} keys at {VALUE} B throughout. A live figure that moves with bytes WRITTEN \
         rather than bytes live is the defect this arm is about, and the tally was supposed to be \
         the half that gets it right"
    );

    // ---- WHAT RECLAIM CONVERGES TO IS PRINTED, NOT ASSERTED, and the reason is the age gate
    // ---- rather than a judgement about the collector. The purge stage IS requested; the
    // ---- quarantine age refuses it, so every round here runs the compaction phase alone and
    // ---- each adds one copy of the live set. That is expected, so what gets asserted is the
    // ---- shape of it -- a bounded cost per round -- and not a target it was never going to hit.
    let per_round = (after_reclaim.physical.saturating_sub(last_write.physical)) as f64
        / RECLAIM_ROUNDS as f64;
    let live_copy = last_write.live_block_granular.unwrap_or(0) as f64;
    println!(
        "    each round added {per_round:.0} B against a live set of {live_copy:.0} B ({:.2} copies)",
        if live_copy == 0.0 { 0.0 } else { per_round / live_copy }
    );
    assert!(
        live_copy > 0.0,
        "the live tally is zero at the last write, so there is nothing to measure a round's cost \
         against"
    );
    assert!(
        per_round <= live_copy * 1.5,
        "each reclaim round added {per_round:.0} B against a live set of {live_copy:.0} B. A round \
         that runs the compaction phase alone should cost about ONE copy of the live set; much \
         more than that is not the two-phase design being age-gated, it is a round doing \
         something else"
    );
}
