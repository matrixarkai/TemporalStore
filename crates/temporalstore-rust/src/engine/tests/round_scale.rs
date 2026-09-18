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
//!     2,000                                        24,000   12.00x
//!    20,000                                       240,000   12.00x
//! ```
//!
//! Exactly 12.00 at both, and the per-site split is identical at both sizes -- six call sites,
//! summing to 12:
//!
//! ```text
//!   storage_reporting.rs:171  bucket_storage_summaries            5.00x
//!   recovery_sweep_compact.rs:334 object lifecycle snapshot       2.00x
//!   storage_lifecycle_methods.rs:873 sampling snapshots           2.00x
//!   compaction.rs:61          compaction_utility_report           1.00x
//!   storage_reporting.rs:801  bucket generation fingerprints      1.00x
//!   recovery_sweep_compact.rs:461 the recovery report             1.00x
//! ```
//!
//! IT WAS FOURTEEN. Three of those passes were made by ONE function,
//! `storage_recovery_report_without_boundary_sampled`, under ONE shard-table read guard: once for
//! the addresses it probes, once inside `validate_shard_block_ownership`, once inside
//! `storage_object_lifecycle_report`. The guard is held across all three, so the second and third
//! could only rebuild what the first already held. They are now one walk, and the separate
//! `Vec<BlockAddress>` that used to be kept alive beside the entries is not built at all.
//!
//! WHAT COULD NOT BE SHARED, and why this is a hoist under one guard rather than a round-scoped
//! memo. Traced in order, the round's remaining passes are separated by mutation of the very
//! state they read: `apply_storage_lifecycle` writes a dump manifest and then calls
//! `clear_dumped_bucket_dirty_state` between its first walk and its second, and on a dumping
//! round the shard's dirty-bucket flags were measured moving 0 -> 2,000 -> 0 WITHIN one round.
//! `bucket_storage_summaries` reports `dirty_object_count`, so a snapshot taken before that
//! clear and served after it would report dirty buckets that are no longer dirty. The five
//! `storage_reporting.rs:171` passes are five different answers, not one answer fetched five
//! times.
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
//! write since, the round still materialised 13.00x the record count, because the other walks
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
const WALKS_PER_ROUND: u64 = 12;

/// The same, once the dirty set has drained. HIGHER, not lower: a shard that has taken a dump has
/// a manifest, and validating it walks the live set again.
const WALKS_PER_IDLE_ROUND: u64 = 13;

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

/// THE RECOVERY REPORT DERIVES THREE ANSWERS FROM ONE WALK OF THE LIVE-PAGE SET.
///
/// `storage_recovery_report_without_boundary_sampled` needs the same live-page set three times:
/// for the addresses its readability probe walks, for the bucket-ownership validation, and for
/// the object-lifecycle report. It used to walk for each, three whole-store passes of the
/// fourteen one round made. It holds ONE shard-table read guard across all three, so they cannot
/// disagree and the second and third could only rebuild the first.
///
/// TWO CLAIMS, asserted in this order.
///
/// SAFETY FIRST, because it is the one that is not checked anywhere else. The three consumers
/// must still describe the same page set, with a denominator: a report over an empty shard would
/// satisfy any agreement. A mutant that feeds one consumer a different set fails here.
///
/// COST SECOND. The call materialises the live-page set ONCE. Asserted as a count rather than a
/// time, and as a count of SITES as well as entries -- `1.00x` is also what a call that walked
/// once and then answered two of the three questions wrongly would report.
#[test]
fn the_recovery_report_derives_three_answers_from_one_walk() {
    const RECORDS: usize = 2_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    // SEEDED ACROSS MORE THAN ONE SLAB, deliberately. With every live page in slab 0 the
    // slab-id agreement below holds whatever the id is derived from -- a mutant that replaced
    // `entry.address.block_slab_id` with the constant 0 passed. Rolling a slab half way through
    // the seed gives the two sides something to disagree about.
    seed_in_batches(&engine, 1, RECORDS / 2, 1_000);
    let rolled = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        prepare_slab_target_bytes: 1,
        ..StorageManagerCycleRequest::default()
    });
    assert!(
        rolled
            .stages
            .iter()
            .any(|stage| stage.stage == "prepare" && stage.prepared_block_slab_id.is_some()),
        "the fixture did not roll a slab, so every live page is still in one slab and the \
         slab-id agreement below cannot fail"
    );
    // DISJOINT keys, matching `seed_in_batches`' own format. Re-seeding from zero would rewrite
    // the first half onto the slab just rolled, the old pages would go stale, and every LIVE
    // page would be in one slab again -- which is the state this fixture exists to avoid.
    let mut commands = Vec::new();
    let mut cursor = RECORDS / 2;
    while cursor < RECORDS {
        commands.push(Command::StringSet {
            key: format!("k-{cursor:08}"),
            value: vec![b'v'; 128],
        });
        cursor += 1;
    }
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands,
    });
    assert!(
        response.status.ok,
        "the second-half seed failed: {:?}",
        response.status
    );

    crate::engine::reset_live_block_scan_entries();
    crate::engine::reset_live_block_scan_sites();
    let report = engine.storage_recovery_report_without_boundary(1);
    let entries = crate::engine::live_block_scan_entries();
    let sites = crate::engine::live_block_scan_sites_snapshot();

    // THE DENOMINATOR, before either claim.
    assert!(
        report.total_block_refs > 0,
        "the report found no live pages at all, so neither claim below is about anything"
    );

    // ---- SAFETY: the three consumers describe one page set ----------------
    //
    // `total_page_refs` comes from the walk itself; `live_page_refs` is counted by the
    // object-lifecycle report, which used to do its own walk. They are the same pages.
    assert_eq!(
        report.total_block_refs as u64, report.object_lifecycle.live_block_refs,
        "the readability probe walked {} live pages and the object-lifecycle report counted {}. \
         Both describe the live-page set of one shard under one read guard, so a difference \
         means one of them is being derived from something other than the pages the other saw",
        report.total_block_refs, report.object_lifecycle.live_block_refs
    );

    // The per-slab live tallies are built in the same loop as the probe, and the slab id list is
    // derived from the same entries. Every slab that holds a live page must appear in both.
    let slabs_from_live_reports = report
        .block_slab_live_reports
        .iter()
        .filter(|slab| slab.live_block_refs > 0)
        .map(|slab| slab.block_slab_id)
        .collect::<std::collections::BTreeSet<_>>();
    let slabs_from_id_list = report
        .live_block_slab_ids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        slabs_from_id_list.len() > 1,
        "the live pages sit in {} slab(s). With one slab the agreement below holds however the \
         slab id is derived, which is exactly how a constant-id defect passed this test once",
        slabs_from_id_list.len()
    );
    assert_eq!(
        slabs_from_live_reports, slabs_from_id_list,
        "the slabs with live pages and the reported live-slab id list disagree"
    );

    // The ownership validation's findings are carried on the lifecycle report. Both now come
    // from the one walk, so the count the report carries must be the count it was given.
    assert_eq!(
        report.object_lifecycle.owner_mismatch_block_refs,
        report.owner_mismatch_block_refs.len() as u64,
        "the lifecycle report carries {} owner mismatches but the validation returned {}",
        report.object_lifecycle.owner_mismatch_block_refs,
        report.owner_mismatch_block_refs.len()
    );

    // ---- COST: one walk, from one site ------------------------------------
    println!(
        "  storage_recovery_report_without_boundary over {RECORDS} records: {entries} live-page \
         entries from {} site(s) = {}x the store",
        sites.len(),
        entries / RECORDS as u64
    );
    for (site, walked) in &sites {
        println!("    {walked:>8}  {site}");
    }
    assert_eq!(
        sites.len(),
        1,
        "the recovery report walked the live-page set from {} call sites, not 1: {sites:?}. All \
         three of its consumers are under one shard-table read guard, so a second site means a \
         walk has come back",
        sites.len()
    );
    assert_eq!(
        entries,
        report.total_block_refs as u64,
        "the recovery report materialised {entries} live-page entries for a shard holding {} \
         live pages. It needs the live-page set three times and takes one walk for all three, so \
         these are the same number",
        report.total_block_refs
    );
}

/// WHAT ONE WALK COSTS PER LIVE PAGE, and that folding three into one REMOVED that cost rather
/// than moving it somewhere else in the same function.
///
/// THE OLD CLAIM WAS STALE. "Each walk clones two strings per entry" was true of an earlier
/// `LiveBlockEntry`; the three text fields are `Arc<str>` now, so a walk copies three pointers
/// and bumps three refcounts per page and copies no text at all. Pinned as a size below, because
/// that is the thing that would change if a field went back to `String` -- 24 bytes and a heap
/// copy per page, per walk, on every periodic round.
///
/// THE HOIST DID NOT MOVE THE COST. The old function kept a `Vec<BlockAddress>` for its probe
/// loop AND built a fresh `Vec<LiveBlockEntry>` inside the ownership validation that followed it,
/// both alive under the same guard. The new one keeps the entries only, and reads each address
/// out of the entry it is already holding. So the peak is lower by a whole address vector, not
/// merely rearranged -- and the entries vector it does keep is one it used to build anyway.
///
/// Counted with `size_of`, so this needs no counting allocator and cannot be moved by box load.
#[test]
fn a_live_page_entry_carries_pointers_not_text_and_the_hoist_lowered_the_peak() {
    let entry_bytes = std::mem::size_of::<LiveBlockEntry>();
    let address_bytes = std::mem::size_of::<crate::block_store::BlockAddress>();
    let arc_str_bytes = std::mem::size_of::<std::sync::Arc<str>>();
    let string_bytes = std::mem::size_of::<String>();

    println!("  LiveBlockEntry {entry_bytes} B, BlockAddress {address_bytes} B");
    println!("  Arc<str> {arc_str_bytes} B against String {string_bytes} B");

    // HALF ONE: the text fields are shared pointers, not owned text. Asserted FIRST because it
    // is the claim the old note got wrong, and it is what makes a walk cheap per page.
    assert!(
        arc_str_bytes < string_bytes,
        "an Arc<str> is {arc_str_bytes} B and a String {string_bytes} B; this test's whole \
         premise is that the entry's text fields are shared rather than owned"
    );
    // object_key + kind + component are the three text fields; the rest is the address and
    // three flags. If a text field became owned this would grow by at least 8 B.
    let text_field_bytes = 3 * arc_str_bytes;
    assert!(
        entry_bytes <= text_field_bytes + address_bytes + 8,
        "a live-page entry is {entry_bytes} B against {text_field_bytes} B of shared text \
         pointers, {address_bytes} B of address and a few flags. Bigger than that means a field \
         is carrying owned text, which is a heap copy per live page PER WALK on every periodic \
         round"
    );

    // HALF TWO: the peak this function holds per live page fell. Ordered after the half above so
    // a mutant that makes entries owned is reported as what it is rather than as a peak change.
    let old_peak_per_page = entry_bytes + address_bytes;
    let new_peak_per_page = entry_bytes;
    println!(
        "  recovery report peak per live page: {old_peak_per_page} B -> {new_peak_per_page} B, \
         {} B/page of address vector no longer built beside the entries",
        old_peak_per_page - new_peak_per_page
    );
    assert!(
        new_peak_per_page < old_peak_per_page,
        "the hoist holds {new_peak_per_page} B per live page where the three-walk version held \
         {old_peak_per_page} B. If these are equal the address vector is still being built and \
         the cost was moved rather than removed"
    );
}
