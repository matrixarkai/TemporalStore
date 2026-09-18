// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one storage-manager round costs at two corpus sizes.
//!
//! THE RESULT. A round's cost is not flat in the store: it walks the whole live-page set a
//! CONSTANT NUMBER OF TIMES, and that constant is the same at every corpus size measured. Counted
//! rather than timed, because the box these run on is shared and a stopwatch there measures the
//! other tenants:
//!
//! ```text
//!   records   live-page entries materialised by ONE round   ratio
//!     8,000                                       112,000   14.00x
//!    80,000                                     1,120,000   14.00x
//! ```
//!
//! Exactly 14.00 at both, and the per-site split is identical at both sizes -- eight call sites,
//! summing to 14:
//!
//! ```text
//!   storage_reporting.rs:171  bucket_storage_summaries            5.00x
//!   recovery_sweep_compact.rs:334                                 2.00x
//!   storage_lifecycle_methods.rs:837 sampling snapshots           2.00x
//!   compaction.rs:61          compaction_utility_report           1.00x
//!   storage_bucket_internals.rs:3623 validate_bucket_ownership    1.00x
//!   storage_reporting.rs:23   object lifecycle report             1.00x
//!   storage_reporting.rs:801  bucket generation fingerprints      1.00x
//!   storage_reporting.rs:825  collect_live_block_addresses        1.00x
//! ```
//!
//! So the round is proportional to the store with a constant of 14, and no stage budget bounds
//! it: the budgets in this round bound the DUMP (buckets per round), the EXPIRE sweep (buckets per
//! round) and the readability probe (512 pages), none of which is the walk above. The wall clock
//! corroborates -- a steady round took 1,352-1,625 ms at 8,000 and 8,117-9,657 ms at 80,000, at
//! box load 4.7-6.3 -- but the count is the claim, because it came out to the same three
//! significant figures on every run.
//!
//! AN IDLE SHARD DOES NOT ESCAPE IT. The plan's own whole-shard walk is skipped when no object is
//! dirty, which is true and is what makes `bucket_summaries` an `Option`. It does not make the
//! ROUND cheap: measured on a shard whose dirty set had drained to zero and which had taken no
//! write since, the round still materialised 15.00x the record count, because the other walks
//! never consult the dirty set. `an_idle_round_still_walks_the_live_page_set` is that number.
//!
//! WHAT THESE TESTS ARE FOR. The ratio is asserted at TWO sizes, so the two failures read
//! differently: a ratio that changed at both sizes means a walk was added or removed, and a ratio
//! that differs BETWEEN the sizes means something in the round stopped being proportional to the
//! store and started being proportional to something else. The second is the one worth a page.
#![allow(clippy::all)]
use super::*;
use crate::engine::reports::StorageManagerCycleRequest;

/// Live-page entries one steady round materialises, per record in the store.
///
/// Measured, at 4,000 / 8,000 / 80,000 records, on a shard whose dump is delayed -- which is the
/// ordinary state of a shard written to in batches, see
/// `the_dump_threshold_counts_log_records_not_dirty_objects`.
const WALKS_PER_ROUND: u64 = 14;

/// The same, once the dirty set has drained. HIGHER, not lower: a shard that has taken a dump has
/// a manifest, and validating it walks the live set again.
const WALKS_PER_IDLE_ROUND: u64 = 15;

fn round_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn seed_in_batches(engine: &TemporalEngine, shard_id: ShardId, count: usize, batch: usize) {
    let mut index = 0usize;
    while index < count {
        let end = (index + batch).min(count);
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            commands.push(Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: vec![b'v'; 128],
            });
            cursor += 1;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
}

/// The round the periodic driver runs, with the data node's own thresholds.
fn round_request() -> StorageManagerCycleRequest {
    StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: false,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 1_000,
        min_undumped_wal_bytes: 96 * 1024 * 1024,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        ..StorageManagerCycleRequest::default()
    }
}

/// Live-page entries one round materialises on this engine, and the round's own report.
fn walk_volume_of_one_round(
    engine: &TemporalEngine,
) -> (u64, crate::engine::reports::StorageManagerCycleReport) {
    crate::engine::reset_live_block_scan_entries();
    let report = engine.run_storage_manager_cycle(round_request());
    (crate::engine::live_block_scan_entries(), report)
}

/// One STEADY round's walk volume per record, at `records` records written in `batch`-sized
/// batches. The first round is the warm-up; the second is the one measured.
fn steady_walks_per_record(records: usize, batch: usize) -> u64 {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed_in_batches(&engine, 1, records, batch);
    let _ = walk_volume_of_one_round(&engine);
    let (entries, report) = walk_volume_of_one_round(&engine);

    // THE DENOMINATOR. A round that walked nothing satisfies any ceiling, and so does one that
    // errored out before its first walk.
    assert!(
        report.errors.is_empty(),
        "{records} records: the round errored, so its walk volume measures nothing: {:?}",
        report.errors
    );
    assert!(
        entries > 0,
        "{records} records: the round materialised no live-page entries at all, so this \
         measures nothing"
    );
    assert_eq!(
        entries % records as u64,
        0,
        "{records} records: {entries} entries is not a whole multiple of the record count, so \
         the walk volume is not a count of whole-store passes and the ratio below would round \
         a real difference away"
    );
    println!(
        "  {records:>6} records in batches of {batch:<4} -> ONE round materialised {entries:>9} \
         live-page entries = {}x the store",
        entries / records as u64
    );
    entries / records as u64
}

/// ONE ROUND COSTS A CONSTANT NUMBER OF WHOLE-STORE WALKS, at both corpus sizes.
///
/// Two sizes an order of magnitude apart. Asserting the RATIO rather than a time is what makes
/// this survive a busy box, and asserting it at two sizes is what separates "someone added a
/// walk" (both move together) from "something stopped being proportional to the store" (they
/// diverge).
#[test]
fn a_round_walks_the_live_page_set_a_constant_number_of_times() {
    const SMALL: usize = 2_000;
    const LARGE: usize = 20_000;

    let small = steady_walks_per_record(SMALL, 1_000);
    let large = steady_walks_per_record(LARGE, 1_000);

    assert_eq!(
        small, large,
        "a round cost {small} whole-store walks at {SMALL} records and {large} at {LARGE}. The \
         two must agree: the round's walk volume is supposed to be a fixed number of passes over \
         the store, so a size-dependent count means some pass is now proportional to something \
         other than the live-page set"
    );
    assert_eq!(
        small, WALKS_PER_ROUND,
        "a round now costs {small} whole-store walks per record, not the measured \
         {WALKS_PER_ROUND}. Each one materialises every live page in the shard and clones two \
         strings per entry, so a walk added here is paid on every periodic round for ever. \
         `live_block_scan_sites_snapshot` names the call sites"
    );
}

/// AN IDLE SHARD STILL WALKS THE WHOLE LIVE-PAGE SET.
///
/// The plan's own walk is skipped when no object is dirty. The round's is not. This drains the
/// dirty set first -- asserted, so the test cannot pass by never reaching the idle state -- then
/// measures a round that has nothing whatever to do.
#[test]
fn an_idle_round_still_walks_the_live_page_set() {
    const RECORDS: usize = 2_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    // One log record per write, so the dump threshold is crossed and the dirty set can drain.
    seed_in_batches(&engine, 1, RECORDS, 1);

    let dirty_before = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        shards
            .get(&1)
            .map(|shard| shard.dirty_objects.len())
            .unwrap_or(0)
    };
    assert_eq!(
        dirty_before, RECORDS,
        "the fixture did not land: {dirty_before} dirty objects, expected {RECORDS}"
    );

    let (_, dumping) = walk_volume_of_one_round(&engine);
    assert!(
        !dumping.plan.dump_delayed,
        "the dump was delayed, so the dirty set cannot drain and the round below is not idle"
    );

    let dirty_after = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        shards
            .get(&1)
            .map(|shard| shard.dirty_objects.len())
            .unwrap_or(0)
    };
    assert_eq!(
        dirty_after, 0,
        "the dirty set did not drain ({dirty_after} left), so the round measured below is not \
         the idle one this test is about"
    );

    // Now a round with an empty dirty set, no write since, and nothing to dump.
    let (entries, idle) = walk_volume_of_one_round(&engine);
    assert!(idle.errors.is_empty(), "idle round errored: {:?}", idle.errors);
    assert!(
        idle.plan.bucket_summaries.is_none(),
        "the plan took its whole-shard walk, so this is not measuring the case where that walk \
         was skipped"
    );
    let walks = entries / RECORDS as u64;
    println!(
        "  IDLE round over {RECORDS} records, dirty set empty, nothing to dump: {entries} \
         live-page entries = {walks}x the store"
    );
    assert_eq!(
        walks, WALKS_PER_IDLE_ROUND,
        "an idle round now costs {walks} whole-store walks per record, not the measured \
         {WALKS_PER_IDLE_ROUND}. An idle shard pays this every cycle for as long as it is loaded"
    );
}

/// THE DUMP THRESHOLD COUNTS LOG RECORDS; THE WORK IT DEFERS IS THE DIRTY-OBJECT SET.
///
/// `min_undumped_wal_records` is compared against undumped LOG records, and a batch is one log
/// record however many objects it carries. So the same corpus can sit either side of the same
/// threshold depending only on how the writer grouped it -- and on the far side the dirty set
/// never drains, which means the round above pays its whole-store walks for ever and produces
/// nothing.
#[test]
fn the_dump_threshold_counts_log_records_not_dirty_objects() {
    const RECORDS: usize = 2_000;
    const THRESHOLD: u64 = 1_000;

    fn one_arm(records: usize, batch: usize) -> (u64, bool, usize) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = round_engine(dir.path());
        seed_in_batches(&engine, 1, records, batch);
        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            min_undumped_wal_records: THRESHOLD,
            ..round_request()
        });
        let dirty = {
            let shards = engine.shards.read().expect("shards lock poisoned");
            shards
                .get(&1)
                .map(|shard| shard.dirty_objects.len())
                .unwrap_or(0)
        };
        println!(
            "  {records} objects in batches of {batch:<5} -> undumped log records {:>5}, \
             dump_delayed {:<5}, dirty objects left {dirty:>5}",
            report.plan.undumped_wal_records, report.plan.dump_delayed
        );
        (report.plan.undumped_wal_records, report.plan.dump_delayed, dirty)
    }

    // Same objects, same threshold. Only the grouping differs.
    let (batched_records, batched_delayed, batched_dirty) = one_arm(RECORDS, 500);
    let (single_records, single_delayed, single_dirty) = one_arm(RECORDS, 1);

    assert_eq!(
        single_records, RECORDS as u64,
        "one write per batch must produce one log record per object, or the two arms below are \
         not the same corpus"
    );
    assert_eq!(
        batched_records,
        (RECORDS / 500) as u64,
        "a batch must be ONE log record however many objects it carries -- that is the whole \
         mechanism this test is about"
    );

    assert!(
        batched_delayed,
        "the batched arm crossed the threshold, so it does not show the divergence"
    );
    assert!(
        !single_delayed,
        "the unbatched arm did not cross the threshold, so there is nothing to contrast"
    );
    assert_eq!(
        batched_dirty, RECORDS,
        "the batched arm should be holding every one of its {RECORDS} objects dirty behind a \
         threshold that can only see {batched_records} log records"
    );
    assert_eq!(
        single_dirty, 0,
        "the unbatched arm should have drained its dirty set in one round"
    );
}

/// THE PREPARE STAGE ROLLS A SLAB, AND A ROLL TAKES THE BLOCK-STORE MUTEX ACROSS A WHOLE-MANIFEST
/// REWRITE.
///
/// `prepare_next_slab_with_target` locks the block store and calls `roll_slab_inner` while
/// holding it, and that rewrites the entire slab manifest. So whatever a roll costs on a store
/// with many slabs, a round that triggers one pays it with the store mutex held, and every append
/// and read on the store waits behind it.
///
/// Two arms on ONE fixture, because "prepare rolled nothing" is satisfied just as well by a
/// prepare that never ran: the control asks for a target the active slab is nowhere near and must
/// produce NO roll, and the treatment asks for one it has passed and must produce exactly one.
#[test]
fn the_prepare_stage_can_roll_a_slab_and_take_the_block_store_mutex() {
    const RECORDS: usize = 500;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed_in_batches(&engine, 1, RECORDS, 100);

    let offset = engine.block_store.active_slab_write_offset();
    assert!(
        offset > 0,
        "the fixture wrote nothing to the active slab, so neither arm below means anything"
    );

    fn prepare_arm(engine: &TemporalEngine, target: u64) -> (Option<u64>, usize) {
        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            enable_prepare: true,
            prepare_slab_target_bytes: target,
            ..StorageManagerCycleRequest::default()
        });
        let prepared = report
            .stages
            .iter()
            .find(|stage| stage.stage == "prepare")
            .expect("the round always reports a prepare stage")
            .prepared_block_slab_id;
        (
            prepared,
            engine.block_store.slab_ids().unwrap_or_default().len(),
        )
    }

    // CONTROL: a target the active slab has not reached. No roll.
    let slabs_before = engine.block_store.slab_ids().unwrap_or_default().len();
    let (control_rolled, control_slabs) = prepare_arm(&engine, offset.saturating_mul(1_000));
    assert_eq!(
        control_rolled, None,
        "prepare rolled a slab at a target the active slab is {offset} bytes short of, so the \
         treatment arm below proves nothing about the target"
    );
    assert_eq!(
        control_slabs, slabs_before,
        "the control arm changed the slab count {slabs_before} -> {control_slabs}"
    );

    // TREATMENT: a target the active slab has passed. One roll.
    let (treatment_rolled, treatment_slabs) = prepare_arm(&engine, 1);
    assert!(
        treatment_rolled.is_some(),
        "prepare did not roll at a target of 1 byte against a {offset}-byte active slab, so the \
         round cannot reach the roll path at all and this test has stopped describing it"
    );
    assert_eq!(
        treatment_slabs,
        control_slabs + 1,
        "the roll did not add a slab: {control_slabs} -> {treatment_slabs}"
    );
    println!(
        "  active slab at {offset} bytes: target {}x above it rolled nothing, target 1 rolled \
         slab {:?} ({control_slabs} -> {treatment_slabs} slabs)",
        1_000, treatment_rolled
    );
}

/// THE ROUND'S BLOCK READS ARE FLAT IN CORPUS SIZE, AND EVERY ONE OF THEM IS UNDER A GUARD.
///
/// Two separate facts, asserted separately.
///
/// FLAT: the round's readability probe is bounded per round, so the number of pages it reads off
/// the block store does not grow with the store. Measured 512 at 8,000 records and 512 at 80,000.
/// The assertion is that the two sizes AGREE, not that they equal 512 -- retuning the bound is a
/// decision someone is allowed to make, and a test that fought it would be guarding the number
/// rather than the property.
///
/// UNDER A GUARD: all of them. `block_reads_under_guard` equals `block_reads_total`, which means
/// every page this round reads is read while the thread holds a shard-table guard, so those reads
/// block every WRITE on the shard for their duration. That is tolerable only because the count is
/// bounded -- which is why the two halves belong in one test: neither is reassuring alone. The
/// total is the denominator, and a round that read nothing would satisfy "none under a guard"
/// just as well as one that had moved its reads out from under it.
#[test]
fn the_rounds_block_reads_are_flat_in_corpus_size_and_all_under_a_guard() {
    const SMALL: usize = 2_000;
    const LARGE: usize = 20_000;

    fn reads_of_one_steady_round(records: usize) -> (u64, u64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = round_engine(dir.path());
        seed_in_batches(&engine, 1, records, 1_000);
        let _ = engine.run_storage_manager_cycle(round_request());
        crate::engine::reset_maintenance_block_read_counts();
        let report = engine.run_storage_manager_cycle(round_request());
        assert!(
            report.errors.is_empty(),
            "{records} records: the round errored, so its read counts measure nothing: {:?}",
            report.errors
        );
        let counts = crate::engine::maintenance_block_read_counts();
        println!(
            "  {records:>6} records -> ONE round read {} pages, {} of them under a shard guard",
            counts.block_reads_total, counts.block_reads_under_guard
        );
        (counts.block_reads_under_guard, counts.block_reads_total)
    }

    let (small_guarded, small_total) = reads_of_one_steady_round(SMALL);
    let (large_guarded, large_total) = reads_of_one_steady_round(LARGE);

    // THE DENOMINATOR, before either claim below.
    assert!(
        small_total > 0 && large_total > 0,
        "the round read no pages at either size ({small_total}, {large_total}), so neither the \
         flatness claim nor the guard claim below has anything to be about"
    );

    assert_eq!(
        small_total, large_total,
        "one round read {small_total} pages at {SMALL} records and {large_total} at {LARGE}. The \
         round's page reads are bounded per round, so a size-dependent count means a whole-store \
         read has come back into the round"
    );

    assert_eq!(
        small_guarded, small_total,
        "at {SMALL} records {small_guarded} of {small_total} page reads happened under a shard \
         guard. The two are expected to be EQUAL -- this test records WHERE the round's reads \
         happen, so a change that moved some of them out from under the guard should update this \
         deliberately rather than leave the round described wrongly"
    );
    assert_eq!(
        large_guarded, large_total,
        "at {LARGE} records {large_guarded} of {large_total} page reads happened under a shard \
         guard"
    );
}
