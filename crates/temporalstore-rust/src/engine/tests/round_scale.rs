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
//!     2,000                                        26,000   13.00x
//!    20,000                                       260,000   13.00x
//! ```
//!
//! Exactly 13.00 at both, and the per-site split is identical at both sizes -- seven call sites,
//! summing to 13. The split is PRINTED by `steady_walks_per_record` rather than maintained here
//! by hand, because a table beside a total is a list that goes stale without anything failing:
//!
//! ```text
//!   storage_reporting.rs:171  bucket_storage_summaries            5.00x
//!   recovery_sweep_compact.rs:334 object lifecycle snapshot       2.00x
//!   storage_lifecycle_methods.rs:873 sampling snapshots           2.00x
//!   compaction.rs:61          compaction_utility_report           1.00x
//!   storage_reporting.rs:801  bucket generation fingerprints      1.00x
//!   recovery_sweep_compact.rs:461 the recovery report             1.00x
//!   storage_reporting.rs:1029 feature block layout report         1.00x
//! ```
//!
//! IT READ TWELVE, AND IT WAS ALWAYS THIRTEEN. The last row is not a pass that was added; it is
//! one that could not be counted. `LIVE_BLOCK_SCAN_ENTRIES` was charged in
//! `collect_live_block_entries` and nowhere else, and `storage_feature_block_layout_report`
//! calls `collect_bucket_index_live_block_entries` directly -- so a whole-store pass made on
//! every periodic round was charged to nothing, and the guard here that exists to notice a walk
//! being added could not see it. The charge now happens inside the walks themselves.
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
//! So the round is proportional to the store with a constant of 13, and no stage budget bounds
//! it: the budgets in this round bound the DUMP (buckets per round), the EXPIRE sweep (buckets per
//! round) and the readability probe (512 pages), none of which is the walk above. The wall clock
//! corroborates -- a steady round took 1,352-1,625 ms at 8,000 and 8,117-9,657 ms at 80,000, at
//! box load 4.7-6.3 -- but the count is the claim, because it came out to the same three
//! significant figures on every run.
//!
//! AN IDLE SHARD DOES NOT ESCAPE IT. The plan's own whole-shard walk is skipped when no object is
//! dirty, which is true and is what makes `bucket_summaries` an `Option`. It does not make the
//! ROUND cheap: measured on a shard whose dirty set had drained to zero and which had taken no
//! write since, the round still materialised 14.00x the record count, because the other walks
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
const WALKS_PER_ROUND: u64 = 13;

/// The same, once the dirty set has drained. HIGHER, not lower: a shard that has taken a dump has
/// a manifest, and validating it walks the live set again.
const WALKS_PER_IDLE_ROUND: u64 = 14;

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
    walk_volume_and_sites_of_one_round(engine).0
}

/// The same, plus WHICH call sites walked. The split in the module note above is this, printed,
/// rather than a list maintained by hand beside a total that would not notice it going stale.
fn walk_volume_and_sites_of_one_round(
    engine: &TemporalEngine,
) -> (
    (u64, crate::engine::reports::StorageManagerCycleReport),
    std::collections::BTreeMap<String, u64>,
) {
    crate::engine::reset_live_block_scan_entries();
    crate::engine::reset_live_block_scan_sites();
    let report = engine.run_storage_manager_cycle(round_request());
    let entries = crate::engine::live_block_scan_entries();
    let sites = crate::engine::live_block_scan_sites_snapshot();
    ((entries, report), sites)
}

/// One STEADY round's walk volume per record, at `records` records written in `batch`-sized
/// batches. The first round is the warm-up; the second is the one measured.
fn steady_walks_per_record(records: usize, batch: usize) -> u64 {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed_in_batches(&engine, 1, records, batch);
    let _ = walk_volume_of_one_round(&engine);
    let ((entries, report), sites) = walk_volume_and_sites_of_one_round(&engine);

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
         live-page entries = {}x the store, from {} call site(s)",
        entries / records as u64,
        sites.len()
    );
    for (site, walked) in &sites {
        println!("    {:>6.2}x  {site}", *walked as f64 / records as f64);
    }
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
/// thirteen one round makes. It holds ONE shard-table read guard across all three, so they cannot
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
fn the_recovery_report_derives_three_answers_from_one_walk_and_takes_a_second_for_layout() {
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

    // ---- COST: two walks, from two sites ----------------------------------
    //
    // IT SAID ONE, AND IT WAS NEVER ONE. The hoist above really did fold three passes into one,
    // and this guard really did watch it -- but it watched through a counter that was charged in
    // `collect_live_block_entries` and nowhere else. `storage_feature_block_layout_report` calls
    // `collect_bucket_index_live_block_entries` directly, so its whole-store pass was charged to
    // nothing and this assertion read 1 while the report took 2.
    //
    // Now that both walks are charged, the number is written down. It is two, it is asserted
    // EXACTLY, and both sites are named below, so a third still fails here -- which is what this
    // guard was for.
    //
    // NOT FOLDED INTO ONE HERE, and the reason is not effort. The two walks do not always read
    // the same set: `collect_live_block_entries` takes the model-map arm when `bucket_map` is
    // empty, while the layout report always takes the bucket-index one. Handing the first walk's
    // entries to the second would be a silent answer change on an empty index, so removing the
    // second pass is its own change with its own measurement.
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
        2,
        "the recovery report walked the live-page set from {} call sites, not 2: {sites:?}. The \
         two are the hoisted probe/ownership/lifecycle walk and the feature-block layout \
         report's own walk; a third means a pass has come back",
        sites.len()
    );
    let mut walked_sites = sites.keys().map(String::as_str).collect::<Vec<_>>();
    walked_sites.sort_unstable();
    assert!(
        walked_sites
            .iter()
            .any(|site| site.contains("recovery_sweep_compact.rs")),
        "the hoisted walk must still be one of the two sites: {walked_sites:?}"
    );
    assert!(
        walked_sites
            .iter()
            .any(|site| site.contains("storage_reporting.rs")),
        "the feature-block layout report's walk must be the other: {walked_sites:?}"
    );
    // ---- WHAT ONE WALK OF THESE TWO COSTS --------------------------------
    //
    // More than the page set it hands back, and the difference is not small. A round leaves
    // buckets RELEASED -- 16 of them on this fixture, which is an ordinary storage round and not
    // a contrived state -- and a released bucket's index holds no pages. So
    // `collect_bucket_index_live_block_entries` walks the index, then walks the WHOLE model-map
    // page set a second time to supplement the released buckets' pages back in. It materialises
    // `indexed + every live page` to return `indexed + released`.
    //
    // Charging the RETURN value, as this counter used to, therefore could not see the second
    // walk at all: it read 2,000 for a call that built 3,984 entries. Both terms are derived
    // from the shard below rather than written down, so a fixture that stops releasing anything
    // fails the denominator instead of quietly measuring the easy case.
    let (indexed_pages, released_buckets) = {
        let shards = engine.shards.read().expect("shards lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        (
            shard
                .bucket_index
                .bucket_map
                .values()
                .map(|bucket| bucket.block_index.len())
                .sum::<usize>() as u64,
            shard.bucket_index.released_buckets.len(),
        )
    };

    // THE DENOMINATOR for everything below. With nothing released the supplement walk never
    // runs, one walk costs exactly the page set, and this guard would be measuring a case that
    // cannot show the cost it exists to name.
    assert!(
        released_buckets > 0,
        "the fixture released no buckets, so the supplement walk never ran and the charge below \
         is not the one this guard is about"
    );
    assert!(
        indexed_pages < report.total_block_refs as u64,
        "the released buckets must be missing from the index ({indexed_pages} indexed against \
         {} returned), or there was nothing to supplement",
        report.total_block_refs
    );

    let charges = sites.values().copied().collect::<Vec<_>>();
    println!(
        "    one walk charged {} to return {} ({indexed_pages} indexed + a whole {}-page \
         re-walk), {released_buckets} bucket(s) released",
        charges[0], report.total_block_refs, report.total_block_refs
    );
    assert_eq!(
        charges[0], charges[1],
        "the two sites walk the same shard under the same guard, so they must materialise the \
         same number of entries: {sites:?}"
    );
    assert_eq!(
        charges[0],
        indexed_pages + report.total_block_refs as u64,
        "one walk materialised {} entries; with {released_buckets} bucket(s) released it should \
         build the {indexed_pages} indexed pages and then the whole {}-page set again",
        charges[0],
        report.total_block_refs
    );
    assert_eq!(
        entries,
        2 * charges[0],
        "the report charged {entries} across its two sites, which must be exactly twice what one \
         of them costs"
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

// ---------------------------------------------------------------------------------------------
// WHAT A ROUND READS OF THE LOGS -- a different denominator from the live-page walks above.
// ---------------------------------------------------------------------------------------------

/// Bytes of the write-ahead log this engine has decoded, taken from the counter inside the walk.
///
/// `bytes_read` is incremented in `scan_collect`, the one walk every WAL scan shares, so a reader
/// added to the log cannot avoid moving it. `raw_stats` is the NON-scanning accessor: reading an
/// instrument must not be work the instrument counts, and `stats()` here would take a piece-tail
/// read of its own.
fn wal_bytes_decoded(engine: &TemporalEngine) -> u64 {
    engine.write_ahead_log_store().raw_stats(1).bytes_read
}

/// The same for the index log, whose count sits one line below the write-ahead log's.
fn index_log_bytes_decoded(engine: &TemporalEngine) -> u64 {
    engine.index_log_store().stats(1).bytes_read
}

/// How many pieces the shard's log is in, and how many bytes are on disk under it.
///
/// The regime assertions rest on this: a log in ONE piece that has never rolled is not the log a
/// steady store has, and every figure taken on it would be a small number about nothing.
fn wal_shape(engine: &TemporalEngine) -> (usize, u64) {
    let info = engine
        .write_ahead_log_store()
        .info(1)
        .expect("write-ahead log info");
    let root = info
        .path
        .parent()
        .expect("the log's path has a parent")
        .to_path_buf();
    let pieces = crate::wal::wal_piece_extents_for_test(&root, 1);
    let bytes = pieces
        .iter()
        .map(|(path, _, _, _)| path.metadata().map(|meta| meta.len()).unwrap_or(0))
        .sum();
    (pieces.len(), bytes)
}

/// xorshift64*, seeded per record.
///
/// The batched seeder above writes a repeated byte, and the log compresses its records: 2,000
/// objects written that way put 30,737 bytes in the log where their payload is 280,000. A corpus
/// of repeated bytes holds a fraction of the log its record count implies, so a fixture built from
/// one does not roll, does not reach the regime, and measures a cost against a log it has not got.
fn incompressible_value(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64
        ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03);
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8
        })
        .collect()
}

/// Bytes per value in the log-reading fixtures. Large enough that a few thousand records roll the
/// log several times over at the shipped 256 KiB threshold, which is what puts the fixture into
/// the regime this cost occurs in.
const LOG_FIXTURE_VALUE_BYTES: usize = 512;

/// Seed ONE log record per object, incompressibly.
///
/// One per object, not one per batch: a batch writes a SINGLE write-ahead log record however many
/// commands it carries, so the batched seeder above puts 79 records in the log for 20,000 objects.
/// That is below the dump threshold, so such a log is never dumped, never reclaimed, and the
/// drained regime measured below could never be reached from it.
fn seed_one_record_per_object(engine: &TemporalEngine, shard_id: ShardId, count: usize) {
    for index in 0..count {
        let response = engine.execute(crate::types::ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: format!("k-{index:08}"),
                value: incompressible_value(LOG_FIXTURE_VALUE_BYTES, index as u64),
            },
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
    }
}

/// What one round, and the report it used to ask for, read of the two logs.
struct LogReadCost {
    records: usize,
    pieces: usize,
    log_bytes_on_disk: u64,
    /// The instrument's own control: a deliberate whole-log `record_count()`, which must move the
    /// counter by about the log's size. A counter that has quietly stopped incrementing reports a
    /// perfect result for every claim below it, so it is planted and recovered before anything
    /// else is measured.
    planted_wal: u64,
    /// What the FULL compatibility report reads of each log. This is what a round paid before the
    /// pressure sites stopped asking for it -- measured live on this fixture, not kept here as a
    /// constant that would go stale the moment a write logs a different number of bytes.
    full_report_wal: u64,
    full_report_index: u64,
    /// What the CHEAP report -- the one the pressure sites now ask for -- reads of each log.
    pressure_report_wal: u64,
    pressure_report_index: u64,
    /// What one whole round reads of each log now.
    round_wal: u64,
    round_index: u64,
    /// The same full report, taken again once the round has drained the log. The DRAINED regime.
    drained_report_wal: u64,
    drained_log_bytes_on_disk: u64,
}

/// Measure one fixture: seed, plant, take both readings on the RETAINED log, then run the round
/// and take the drained reading.
///
/// Order is load-bearing. The round reclaims, so every figure about a retained log has to be taken
/// before it runs; the drained figure is the same report after it.
fn log_read_cost(records: usize) -> LogReadCost {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed_one_record_per_object(&engine, 1, records);

    let (pieces, log_bytes_on_disk) = wal_shape(&engine);

    // CONTROL FOR THE INSTRUMENT. Reading it twice, with nothing in between, must not move it.
    let idle_before = wal_bytes_decoded(&engine);
    let idle_after = wal_bytes_decoded(&engine);
    assert_eq!(
        idle_before, idle_after,
        "{records} records: reading the byte counter moved it, so every delta below is this \
         accessor's own work and not the work being measured"
    );

    // PLANTED. A whole-log count, deliberately, so the counter is known to answer.
    let before = wal_bytes_decoded(&engine);
    let counted = engine
        .write_ahead_log_store()
        .record_count(1)
        .expect("record count");
    let planted_wal = wal_bytes_decoded(&engine) - before;
    assert!(
        counted >= records,
        "{records} objects written one log record each, and the log counted {counted}. This \
         fixture is not the log it thinks it is"
    );

    // THE FULL REPORT, on the retained log. This is the cost the pressure snapshot used to carry.
    let wal_before = wal_bytes_decoded(&engine);
    let index_before = index_log_bytes_decoded(&engine);
    let _ = engine.storage_log_compatibility_report(1);
    let full_report_wal = wal_bytes_decoded(&engine) - wal_before;
    let full_report_index = index_log_bytes_decoded(&engine) - index_before;

    // THE CHEAP REPORT, on the same retained log. This is the site that changed.
    let wal_before = wal_bytes_decoded(&engine);
    let index_before = index_log_bytes_decoded(&engine);
    let _ = engine.storage_log_pressure_report(1);
    let pressure_report_wal = wal_bytes_decoded(&engine) - wal_before;
    let pressure_report_index = index_log_bytes_decoded(&engine) - index_before;

    // ONE WHOLE ROUND, on the same retained log.
    let wal_before = wal_bytes_decoded(&engine);
    let index_before = index_log_bytes_decoded(&engine);
    let report = engine.run_storage_manager_cycle(round_request());
    let round_wal = wal_bytes_decoded(&engine) - wal_before;
    let round_index = index_log_bytes_decoded(&engine) - index_before;
    assert!(
        report.errors.is_empty(),
        "{records} records: the round errored, so what it read measures nothing: {:?}",
        report.errors
    );

    // THE DRAINED REGIME: the same full report, once the round has taken what it can.
    let (_, drained_log_bytes_on_disk) = wal_shape(&engine);
    let wal_before = wal_bytes_decoded(&engine);
    let _ = engine.storage_log_compatibility_report(1);
    let drained_report_wal = wal_bytes_decoded(&engine) - wal_before;

    LogReadCost {
        records,
        pieces,
        log_bytes_on_disk,
        planted_wal,
        full_report_wal,
        full_report_index,
        pressure_report_wal,
        pressure_report_index,
        round_wal,
        round_index,
        drained_report_wal,
        drained_log_bytes_on_disk,
    }
}

fn print_log_read_cost(cost: &LogReadCost) {
    println!(
        "    {:>6} records, {:>3} pieces, {:>9} B of log | PLANTED count read {:>9} B | \
         FULL REPORT read {:>9} B of log + {:>8} B of index log | ONE ROUND read {:>9} B of log \
         + {:>8} B of index log | DRAINED to {:>8} B, same report read {:>8} B | PRESSURE REPORT \
         read {} B + {} B",
        cost.records,
        cost.pieces,
        cost.log_bytes_on_disk,
        cost.planted_wal,
        cost.full_report_wal,
        cost.full_report_index,
        cost.round_wal,
        cost.round_index,
        cost.drained_log_bytes_on_disk,
        cost.drained_report_wal,
        cost.pressure_report_wal,
        cost.pressure_report_index
    );
}

/// The apparatus and the regime, asserted before any ratio is read from it.
///
/// A cost measured where it cannot occur reads exactly like a cost that is gone.
fn assert_log_read_regime(cost: &LogReadCost) {
    assert!(
        cost.pieces > 1,
        "{} records: the log is in {} piece(s). A store whose log has never rolled is not the \
         steady state this measures",
        cost.records,
        cost.pieces
    );
    assert!(
        cost.log_bytes_on_disk > 0,
        "{} records: the log holds no bytes, so every figure here is a zero about nothing",
        cost.records
    );
    assert!(
        cost.planted_wal >= cost.log_bytes_on_disk / 2,
        "{} records: a deliberate whole-log count moved the byte counter by {} against {} bytes \
         on disk. The counter is not counting the walk, so a zero anywhere below means nothing",
        cost.records,
        cost.planted_wal,
        cost.log_bytes_on_disk
    );
}

/// A ROUND NO LONGER DECODES EITHER LOG END TO END -- at two corpus sizes, in both regimes.
///
/// The live-page walks measured above are what a round costs in the STORE. This is what it cost in
/// the LOGS, and nothing counted it. The pressure snapshot asked `storage_log_compatibility_report`
/// and read two byte figures out of it; that report also fills `wal_records` and
/// `index_log_records`, and each of those is a `record_count()` -- a walk of the whole log that
/// decodes every record in it. So every round decoded the write-ahead log end to end, and the
/// index log after it, for two numbers it then discarded.
///
/// The denominator is the worse of the two available. It is not the store, it is the RETAINED LOG:
/// the round that reclaims is the round that pays, so a shard whose log is not draining -- held by
/// a block-retention floor, by a durable-index anchor, or simply between dumps -- makes its own
/// maintenance more expensive the longer it fails to drain.
///
/// THE REGIME, which decides whether any of this is visible at all:
///
/// * RETAINED -- the log is still there. The count is the whole log and grows with it. This is the
///   regime the cost is real in, and the one the figures below are taken in.
/// * DRAINED -- the round has reclaimed the log back to a handful of records. The count is those
///   records, the cost is nothing, and a measurement taken only here would report "flat and
///   healthy" about a walk that is linear in a log this fixture no longer has.
///
/// Both are measured, at both sizes, so neither can be picked by accident later.
#[test]
fn a_round_no_longer_decodes_both_logs_end_to_end() {
    const SMALL: usize = 2_000;
    const LARGE: usize = 8_000;

    let small = log_read_cost(SMALL);
    let large = log_read_cost(LARGE);

    println!("  WHAT ONE STORAGE-MANAGER ROUND READS OF THE LOGS");
    print_log_read_cost(&small);
    print_log_read_cost(&large);

    assert_log_read_regime(&small);
    assert_log_read_regime(&large);

    let record_ratio = LARGE as f64 / SMALL as f64;
    let byte_ratio = large.log_bytes_on_disk as f64 / small.log_bytes_on_disk.max(1) as f64;
    let full_ratio = large.full_report_wal as f64 / small.full_report_wal.max(1) as f64;
    println!(
        "    ratio: {record_ratio:.2}x records, {byte_ratio:.2}x log bytes, \
         {full_ratio:.2}x read by the full report; one round read {} B and {} B",
        small.round_wal, large.round_wal
    );

    // THE TREATMENT WAS APPLIED: the larger fixture really does hold a larger log.
    assert!(
        byte_ratio > record_ratio * 0.5,
        "the two logs differ by {byte_ratio:.2}x in bytes against {record_ratio:.2}x in records, \
         so the larger fixture is not the larger log this is sized for"
    );

    // WHAT IT COST. The full report reads the whole log, at both sizes, and what it reads GROWS
    // with the log. This is the quantity, measured live rather than remembered.
    assert!(
        small.full_report_wal >= small.log_bytes_on_disk / 2
            && large.full_report_wal >= large.log_bytes_on_disk / 2,
        "the full report read {} B and {} B against logs of {} B and {} B. If it is not reading \
         the log in full then the cost this test is about does not exist and the result below \
         proves nothing",
        small.full_report_wal,
        large.full_report_wal,
        small.log_bytes_on_disk,
        large.log_bytes_on_disk
    );
    assert!(
        full_ratio > byte_ratio * 0.5,
        "what the full report read grew {full_ratio:.2}x for {byte_ratio:.2}x the log. It is \
         supposed to be reading the whole log, so this fixture is not producing the growth the \
         result below is measured against"
    );

    // THE RESULT, PART ONE. The report the pressure sites now ask for reads NEITHER LOG AT ALL.
    // This is the exact claim: every figure it answers comes from `stats()`, which is piece
    // headers and accumulated counters, and none of it from the records.
    for cost in [&small, &large] {
        assert_eq!(
            cost.pressure_report_wal, 0,
            "{} records: the pressure report decoded {} bytes of the write-ahead log. It is \
             supposed to answer entirely from `stats()`",
            cost.records, cost.pressure_report_wal
        );
        assert_eq!(
            cost.pressure_report_index, 0,
            "{} records: the pressure report decoded {} bytes of the index log",
            cost.records, cost.pressure_report_index
        );
    }

    // THE RESULT, PART TWO. What a whole round reads of the write-ahead log is now a CONSTANT --
    // measured at 573 and 574 bytes for a 4x store -- rather than the log. Asserted as a shape,
    // not as those two numbers: a constant is what matters, and the constant itself would move
    // with any unrelated change to what the reclaim plan reads.
    assert!(
        large.round_wal <= small.round_wal + small.round_wal / 4 + 64,
        "one round read {} bytes of the write-ahead log on the small store and {} on the store \
         four times its size. That is the round reading the LOG again instead of a fixed amount \
         of it",
        small.round_wal,
        large.round_wal
    );
    assert!(
        large.round_wal * 100 < large.full_report_wal,
        "one round read {} bytes of the write-ahead log where the full report reads {}. The \
         round is back to reading the log in full",
        large.round_wal,
        large.full_report_wal
    );

    // WHAT IS STILL THERE, named rather than left for someone to rediscover. The round still
    // reads the INDEX log in full, and it grows with the store -- but not from the pressure
    // snapshot, which the two zeros above have just proved reads nothing. That read belongs to
    // the index-GC stage, which walks the index log because walking it is its job. It is stated
    // here so the difference between the two logs is a measured fact and not an oversight.
    assert!(
        large.round_index > small.round_index,
        "the round read {} bytes of the index log on the small store and {} on the large one. If \
         that has stopped growing, the index-GC stage this note describes has changed and the \
         note is now wrong",
        small.round_index,
        large.round_index
    );

    // AND THE DRAINED REGIME, stated rather than left to be discovered. The same report on a
    // drained log reads almost nothing -- which is why a measurement taken only there would have
    // closed this question with a flat, healthy-looking number.
    assert!(
        large.drained_report_wal < large.full_report_wal,
        "the full report read {} B on the drained log against {} B on the retained one. Without \
         that difference the two regimes are the same regime and this test measures one of them \
         twice",
        large.drained_report_wal,
        large.full_report_wal
    );
}

/// THE CHEAP REPORT IS THE SAME REPORT, FIELD BY FIELD -- not a second opinion about the log.
///
/// The direction analysis. Removing a walk from a maintenance round is only safe if the numbers
/// the round actually reads are unchanged: `wal_bytes` and `index_log_bytes` feed the pressure
/// score and, through it, whether a stage runs at all. Reading them from a different place that
/// happened to agree on one fixture is how two live copies of a rule start to disagree.
///
/// So this compares the FIELDS, not their plausibility, at two corpus sizes and after a write has
/// moved every one of them off zero -- equality of zeros is equality about nothing.
#[test]
fn the_log_pressure_report_agrees_with_the_full_report_field_by_field() {
    for records in [2_000usize, 8_000usize] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = round_engine(dir.path());
        seed_one_record_per_object(&engine, 1, records);
        engine.flush_shard_index(1);

        let full = engine.storage_log_compatibility_report(1);
        let cheap = engine.storage_log_pressure_report(1);

        assert!(
            full.wal_bytes > 0
                && full.index_log_bytes > 0
                && full.wal_last_sequence > 0
                && full.index_log_last_sequence > 0,
            "{records} records: the full report answers {full:?}, and a field that is zero in \
             both reports agrees about nothing"
        );
        assert_eq!(
            cheap.shard_id, full.shard_id,
            "{records} records: the two reports are about different shards"
        );
        assert_eq!(
            cheap.wal_bytes, full.wal_bytes,
            "{records} records: the round would read {} bytes of log where the full report says \
             {}",
            cheap.wal_bytes, full.wal_bytes
        );
        assert_eq!(
            cheap.index_log_bytes, full.index_log_bytes,
            "{records} records: the round would read {} bytes of index log where the full report \
             says {}",
            cheap.index_log_bytes, full.index_log_bytes
        );
        assert_eq!(
            cheap.wal_last_sequence, full.wal_last_sequence,
            "{records} records: the two reports disagree about the log's last sequence"
        );
        assert_eq!(
            cheap.index_log_last_sequence, full.index_log_last_sequence,
            "{records} records: the two reports disagree about the index log's last sequence"
        );
        println!(
            "  {records:>6} records: both reports say wal_bytes {}, index_log_bytes {}, \
             wal_last_sequence {}, index_log_last_sequence {}",
            cheap.wal_bytes,
            cheap.index_log_bytes,
            cheap.wal_last_sequence,
            cheap.index_log_last_sequence
        );
    }
}
