// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CAN A ROUND TELL A SMALL BACKLOG FROM A LARGE ONE? IT ALREADY CAN, EXACTLY. ITS COST NEVER ASKS.
//!
//! mx#1934 measured one maintenance round at 20,000 and 100,000 records and named a mechanism for
//! what it found: "after 2,000 writes to a 102,000-record shard, the round still reports 102,000
//! dirty buckets -- it cannot tell a small amount of pending work from a large one."
//!
//! READ `round_scale.rs` BESIDE THIS FILE. IT GOT THERE FIRST, AND THIS FILE SAYS SO.
//! `the_dump_threshold_counts_log_records_not_dirty_objects` already established the mechanism
//! mx#1934 misread -- a write BATCH is one log record however many objects it carries, so a
//! batched corpus sits on the delayed side of `min_undumped_wal_records` and "the dirty set never
//! drains" -- and `an_idle_round_still_walks_the_live_page_set` already established that a round
//! with an empty dirty set walks the store anyway. Neither finding is new here. What this file
//! adds is set equality against a witness from outside the engine, the restart argument, and both
//! of those established results carried up to mx#1934's own fixture and sizes.
//!
//! REPRODUCED FIRST, BIT FOR BIT. Every figure mx#1934 published reproduces on this tree:
//! 377,824 -> 1,897,824 live-page entries for the all-pending round (5.023x), 609,052 ->
//! 2,849,052 for the fixed-pending round (4.678x), and 304.526 -> 1,424.526 entries per record of
//! work retired. Nothing below disputes a number it reported.
//!
//! WHAT IS DISPUTED IS THE MECHANISM, AND THE ANSWER WAS ALREADY IN THE TREE. The 102,000 dirty
//! buckets were REAL. In that fixture the round never dumps, so nothing is ever retired and every
//! bucket in the store genuinely is dirty -- the round was reporting the truth, not failing to
//! distinguish anything. The fixture seeds in batches of 500, so 20,000 records make FORTY log
//! sequences; `min_undumped_wal_records` is 1,000, which is also the production default; 40 <
//! 1,000 delays the dump on every round for ever, `selected_dump_buckets` is empty every round,
//! `clear_dumped_bucket_dirty_state` is never reached, and the dirty set is drained ZERO times in
//! the life of the store. Asserted at mx#1934's own size by
//! `the_measured_fixture_never_dumps_so_the_buckets_it_calls_dirty_really_are_dirty`, with a
//! control arm that lowers only that one threshold and watches the same store drain to nothing.
//!
//! THE TRACKING IS ALREADY THERE AND -- THIS PART IS NEW -- IT IS EXACT AS A SET.
//! `shard.dirty_objects` is keyed by routing bucket at the write itself, drained by the dump that
//! captured the bucket, and held back for any bucket whose generation moved since the capture.
//! Everything measured before this file compared COUNTS. Counts agree for two sets that are both
//! wrong by the same amount, and the failure that matters here is one bucket, so the assertion has
//! to be on the SET. Element by element against a witness built OUTSIDE the engine -- the
//! fixture's own list of keys written since the last dump, hashed with `block_routing_bucket` --
//! the two sets are equal at five points in a write sequence, with no bucket in either one alone,
//! and the two differences are asserted separately because only one of them loses data. No cost
//! moves onto the write path because there is nothing new to maintain: mx#1709 already put the
//! bucket id into the dirty index at `mark_async_dirty_object`.
//!
//! SO WHAT IS THE DEFECT? THE ROUND'S COST NEVER CONSULTS ANY OF IT. With the dump firing and the
//! dirty set drained to EMPTY -- no pending work of any kind, nothing to dump, nothing to retire
//! -- a round still walks the whole live page set. This is `an_idle_round_still_walks_the_live_page_set`
//! carried to the two sizes above the 100,000-record line, where it can be scaled rather than
//! stated, and set beside a fixed backlog so the two terms can be separated:
//!
//! ```text
//!   BACKLOG      store 20,000   store 100,000    ratio   per record of STORE   slabs
//!   empty             569,724       2,889,724   5.072x     28.486 -> 28.897        5
//!   2,000 records     755,036       3,555,036   4.708x     34.320 -> 34.853        8
//!   marginal cost
//!   of the SAME
//!   2,000 backlog     185,312         665,312   3.590x   92.656 -> 332.656 PER
//!                                                          RECORD OF WORK
//! ```
//!
//! A round with NOTHING TO DO costs 2,889,724 live-page entries at 100,000 records and 569,724 at
//! 20,000: 5.072x for a store 5x larger, and 81% of what the 2,000-record round costs. That figure
//! divided by the work retired is not large, it is UNDEFINED -- the work retired is zero. Per
//! record of WORK retired the backlog's own marginal cost goes 92.656 -> 332.656, 3.590x, because
//! the dump that retires 2,000 buckets re-walks the whole store to do it. Both terms scale with
//! the store and neither asks the dirty set anything.
//!
//! THE MARGINAL ROW CARRIES A SLAB TERM AND THE OTHER TWO DO NOT. Writing the backlog rolls slabs,
//! so the loaded arms run on eight slabs and the empty arms on five. The ratio WITHIN each row is
//! measured at a constant slab count -- asserted, arm against arm -- and is what the verdicts rest
//! on; the marginal row subtracts an eight-slab round from a five-slab one and is reported for its
//! ratio rather than for its absolute size.
//!
//! WHERE THE FLOOR GOES, NAMED. Attributed by calling site on the empty round at 100,000 records,
//! which is the round with no work in it at all:
//!
//! ```text
//!   storage_reporting.rs:171            398,456   whole-store summary, 4 per round
//!   storage_reporting.rs:126            299,100
//!   storage_reporting.rs:801            299,100
//!   recovery_sweep_compact.rs:424       298,844
//!   storage_lifecycle_methods.rs:975    298,844
//!   compaction.rs:61                    199,360
//!   recovery_sweep_compact.rs:1430      199,228
//!   recovery_sweep_compact.rs:551       199,228
//!   storage_reporting.rs:1029           199,228
//! ```
//!
//! Nine sites in five modules, each walking a store that has nothing pending. `round_scale.rs`
//! names seven of them at 2,000 and 20,000 records and holds their per-record split under a guard;
//! this is the same split at 100,000 with nothing pending, and it is reported rather than asserted
//! here so the two files do not both own one list. That is where the work is, and it is NOT dirty
//! tracking -- `bucket_storage_summaries` alone has eighteen callers,
//! several of which publish whole-store report figures that a scoped walk would silently narrow.
//! Nothing here is changed, for the reason the campaign's own rule gives: a round that skips a
//! bucket that was dirty is silent until someone looks for a record that is not there.
//!
//! WHAT WAS PRICED AND DECLINED. Scoping `bucket_storage_summaries` to the dirty bucket set is the
//! obvious change and would take 2,889,724 entries off the empty round at 100,000 records. It is
//! declined here because it is not provably exact. The walk credits each page to
//! `entry.address.routing_bucket()` with a hash fallback over the SHARD'S range, while
//! `upsert_bucket_index_block_inner` files that same page under a fallback over `0..u32::MAX`, so
//! a page whose address carries no explicit routing bucket can be filed under one bucket and
//! summarised under another. A walk scoped to the dirty buckets would then miss it, which is the
//! losing direction. Measured on this fixture the branch does not fire -- zero unrouted pages and
//! zero misfiled pages at both sizes -- but "does not fire on this fixture" is not the same
//! statement as "cannot fire", and the shortfall is the whole argument.
//!
//! SUM THE INTEGRAL. The floor is the term that matters, because it is paid whether or not
//! anything happened. A store grown to N records with one round every `k` records pays about
//! `28.9 N^2 / 2k` live-page entries in floor alone -- 1.445e12 at a million records with a round
//! every 10,000 -- and every one of those rounds truthfully reports zero dirty buckets.
//!
//! CRASH AND RESTART. The dirty set is in memory only and is EMPTY after a restart: 2,000 dirty
//! objects become 0. That is safe for data and is asserted to be -- every sampled record is still
//! readable after the restart, after a round that dumped nothing, and after a second restart --
//! because durability is anchored on the persisted bucket index and the log, not on the dirty set.
//! `load_shard_with` clears every bucket and page dirty flag on load and
//! `refresh_bucket_runtime_flags` recomputes `bucket.dirty` from the empty set, which is the
//! clear-dirty-on-load contract. What a restart DOES take away is dump SELECTION: the round after
//! a restart selects no bucket to dump even though the log still counts those writes as undumped.
//! Measured and asserted, because it is the constraint on anyone who would drive more of the round
//! from this set.
#![allow(clippy::all)]
use super::*;
use crate::engine::reports::{StorageManagerCycleReport, StorageManagerCycleRequest};
use std::collections::BTreeSet;

/// The two corpus sizes, five times apart -- the same two mx#1934 used, so the tables can be read
/// beside each other.
const SMALL: usize = 20_000;
const LARGE: usize = 100_000;

/// Records written immediately before the round in the fixed-backlog arm, at BOTH sizes.
const FIXED_PENDING: usize = 2_000;

/// Records per write batch. This is the number that decides the whole fixture artefact below: the
/// log threshold counts SEQUENCES, and a batch is one sequence.
const SEED_BATCH: usize = 500;

/// `default_storage_manager_min_undumped_wal_records()`, which is also what mx#1934's fixture
/// asked for. Held here as a constant so the arithmetic against `SEED_BATCH` is visible.
const PUBLISHED_MIN_UNDUMPED_WAL_RECORDS: u64 = 1_000;

/// The threshold the control arm uses instead, so the dump actually fires. The ONLY setting that
/// differs between the two arms.
const DUMPING_MIN_UNDUMPED_WAL_RECORDS: u64 = 1;

/// Stages this fixture never gives work to apply. Their zeros are NOT measurements of a cheap
/// stage, and are asserted to be zero so a later fixture that gives them work fails here rather
/// than turning an absent measurement into a verdict.
const UNEXERCISED_STAGES: &[&str] = &["reclaim_page", "index_gc"];

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

/// Write `count` records from `from` and return the keys written, so the caller holds its own list
/// of what is outstanding -- the witness the engine's dirty set is compared against.
///
/// One record in ten carries a deadline so the expire sweep has something to remove, and the data
/// slab is rolled four times on the way so the store is more than one slab. Both match mx#1934's
/// fixture, so the reproduction arm can be compared against its published table.
fn seed(engine: &TemporalEngine, from: usize, count: usize) -> Vec<String> {
    let mut written = Vec::with_capacity(count);
    let mut index = from;
    let to = from + count;
    let roll_every = (count / 4).max(1);
    let mut since_roll = 0usize;
    while index < to {
        let end = (index + SEED_BATCH).min(to);
        let mut commands = Vec::new();
        for cursor in index..end {
            let key = if cursor % 10 == 0 {
                format!("t-{cursor:08}")
            } else {
                format!("k-{cursor:08}")
            };
            if cursor % 10 == 0 {
                commands.push(Command::StringSetEx {
                    key: key.clone(),
                    value: vec![b'v'; 128],
                    ttl_ms: 1,
                });
            } else {
                commands.push(Command::StringSet {
                    key: key.clone(),
                    value: vec![b'v'; 128],
                });
            }
            written.push(key);
            }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        since_roll += end - index;
        index = end;
        if since_roll >= roll_every && index < to {
            since_roll = 0;
            let before = engine.block_store.slab_ids().unwrap_or_default().len();
            engine.block_store.roll_slab().expect("roll a data slab");
            let after = engine.block_store.slab_ids().unwrap_or_default().len();
            assert!(
                after > before,
                "the fixture asked for a slab roll and got none ({before} -> {after}), so the \
                 store is one slab and compaction and page reclaim have no choice to make"
            );
        }
    }
    written
}

/// The round the periodic driver runs, with every stage on, parameterised on the one threshold the
/// two arms differ by.
fn round_request(min_undumped_wal_records: u64) -> StorageManagerCycleRequest {
    StorageManagerCycleRequest {
        shard_id: 1,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: true,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records,
        min_undumped_wal_bytes: 96 * 1024 * 1024,
        eviction_memory_pressure_threshold: 1,
        eviction_batch_limit: 4,
        max_expire_hot_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        max_expire_cold_buckets_per_round:
            crate::engine::reports::DEFAULT_MAX_EXPIRE_COLD_BUCKETS_PER_ROUND,
        index_gc_max_entries_per_round:
            crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND,
        index_gc_index_log_bytes_threshold: 1,
        ..StorageManagerCycleRequest::default()
    }
}

/// How many dirty objects the shard is holding, and in how many buckets.
fn dirty_state(engine: &TemporalEngine) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1");
    (
        shard.dirty_objects.len(),
        shard.dirty_objects.bucket_ids().count(),
    )
}

/// The buckets the engine says are dirty, read from the dirty index itself.
fn dirty_bucket_set(engine: &TemporalEngine) -> BTreeSet<u32> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards
        .get(&1)
        .expect("shard 1")
        .dirty_objects
        .bucket_ids()
        .collect()
}

/// The shard's own routing range, which is what the witness below has to hash with.
fn routing_range(engine: &TemporalEngine) -> (u32, u32) {
    engine
        .infos
        .read()
        .expect("info lock poisoned")
        .get(&1)
        .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
        .unwrap_or((0, u32::MAX))
}

/// Total objects the dirty index has ever had drained out of it, process-wide.
fn drained_total() -> u64 {
    crate::engine::storage_lifecycle_methods::DIRTY_DRAIN_VISITS
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// One round, with the walk instrument read once around the WHOLE call.
///
/// The total comes from a counter the stage rows do not feed: the rows accumulate in their own
/// atomics and are cleared at every stage boundary, while this is read once around the call.
/// Subtracting one from the other is therefore a reading rather than an identity, which is the
/// only reason the residual test below means anything.
struct RoundCost {
    report: StorageManagerCycleReport,
    whole_call_entries: u64,
    store_records: usize,
    backlog: usize,
    retired: usize,
    slabs: usize,
    path_len: usize,
}

impl RoundCost {
    fn stage(&self, name: &str) -> &crate::engine::reports::StorageManagerStageReport {
        self.report
            .stages
            .iter()
            .find(|stage| stage.stage == name)
            .unwrap_or_else(|| {
                panic!(
                    "the round reported no stage named {name}; it reported {:?}",
                    self.report
                        .stages
                        .iter()
                        .map(|stage| stage.stage.as_str())
                        .collect::<Vec<_>>()
                )
            })
    }

    fn stage_row_entries(&self) -> u64 {
        self.report
            .stages
            .iter()
            .map(|stage| stage.walk.live_block_entries)
            .sum()
    }

    fn per_record_of_store(&self) -> f64 {
        self.whole_call_entries as f64 / self.store_records as f64
    }
}

fn one_round(engine: &TemporalEngine, min_undumped_wal_records: u64) -> (StorageManagerCycleReport, u64) {
    crate::engine::reset_live_block_scan_entries();
    let report = engine.run_storage_manager_cycle(round_request(min_undumped_wal_records));
    (report, crate::engine::live_block_scan_entries())
}

/// Build a store of `records`, settle it with two rounds that DO dump, then measure one round with
/// `backlog` records written immediately before it.
///
/// Settling first is what separates this measurement from mx#1934's: after two dumping rounds the
/// dirty set is empty and every subsequent round is measured against a backlog the fixture chose
/// rather than against every record the store has ever held.
fn measure(records: usize, backlog: usize) -> RoundCost {
    let dir = tempfile::tempdir().expect("tempdir");
    let path_len = dir.path().as_os_str().len();
    let engine = round_engine(dir.path());
    seed(&engine, 0, records);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let _settle_one = one_round(&engine, DUMPING_MIN_UNDUMPED_WAL_RECORDS);
    let _settle_two = one_round(&engine, DUMPING_MIN_UNDUMPED_WAL_RECORDS);
    let (settled_objects, _) = dirty_state(&engine);
    assert_eq!(
        settled_objects, 0,
        "{records} records: the settling rounds left {settled_objects} dirty objects behind, so \
         the backlog this arm goes on to measure is not the one it wrote"
    );
    if backlog > 0 {
        seed(&engine, records, backlog);
    }
    std::thread::sleep(std::time::Duration::from_millis(30));
    let (before_objects, before_buckets) = dirty_state(&engine);
    assert_eq!(
        before_objects, backlog,
        "{records} records: the fixture asked for a backlog of {backlog} and the shard is holding \
         {before_objects} dirty objects. Every per-record-of-work figure below divides by the \
         wrong number if this is not exact"
    );
    let drained_before = drained_total();
    let (report, whole_call_entries) = one_round(&engine, DUMPING_MIN_UNDUMPED_WAL_RECORDS);
    let retired = (drained_total() - drained_before) as usize;
    assert!(
        report.errors.is_empty(),
        "{records} records / {backlog} backlog: the round errored, so every figure it reports \
         measures nothing: {:?}",
        report.errors
    );
    // FLOOR THE INSTRUMENT. A counter that counted nothing reads exactly like a measurement of
    // zero, and every row below would be the first wearing the second's clothes.
    assert!(
        whole_call_entries > 0,
        "{records} records / {backlog} backlog: the round materialised no live-page entry at all"
    );
    let slabs = report.plan.live_block_slab_ids.len();
    println!(
        "  store {records:>7} backlog {backlog:>5}: entries {whole_call_entries:>9}, \
         dirty buckets before {before_buckets:>6}, retired {retired:>6}, slabs {slabs}"
    );
    RoundCost {
        report,
        whole_call_entries,
        store_records: records + backlog,
        backlog,
        retired,
        slabs,
        path_len,
    }
}

// ---------------------------------------------------------------------------------------------
// 1. The mechanism mx#1934 named, corrected.
// ---------------------------------------------------------------------------------------------

/// THE 102,000 DIRTY BUCKETS WERE REAL.
///
/// mx#1934 read the round's own report -- 102,000 dirty buckets after 2,000 writes to a
/// 102,000-record shard -- as the round failing to tell a small backlog from a large one. It was
/// the round telling the truth. In that fixture the dump never fires, so nothing is ever retired
/// and every bucket really is dirty.
///
/// The arithmetic is entirely in two constants. The log threshold counts SEQUENCES and a write
/// batch is one sequence, so 20,000 records seeded 500 at a time make 40 sequences against a
/// threshold of 1,000. `records_say_wait` is therefore true on every round for ever,
/// `selected_dump_buckets` is empty, `apply_storage_lifecycle` builds no manifest, and
/// `clear_dumped_bucket_dirty_state` -- the only thing that drains the dirty set on this path --
/// is never reached.
///
/// THE CONTROL ARM IS THE SAME STORE WITH ONE THRESHOLD LOWERED. Nothing else differs: same seed,
/// same rounds, same stages. It drains to nothing, which is what says the tracking was never
/// broken and the fixture was never dumping.
#[test]
fn the_measured_fixture_never_dumps_so_the_buckets_it_calls_dirty_really_are_dirty() {
    const RECORDS: usize = SMALL;
    // The artefact, stated as arithmetic before it is measured: this is why the dump never fires.
    let sequences_for_the_seed = (RECORDS / SEED_BATCH) as u64;
    assert!(
        sequences_for_the_seed < PUBLISHED_MIN_UNDUMPED_WAL_RECORDS,
        "this test exists because {RECORDS} records seeded {SEED_BATCH} at a time make \
         {sequences_for_the_seed} log sequences against a threshold of \
         {PUBLISHED_MIN_UNDUMPED_WAL_RECORDS}. If that is no longer true the fixture now dumps \
         and the arm below is measuring something else"
    );

    // ARM A: the published settings.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed(&engine, 0, RECORDS);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let drained_before = drained_total();
    let mut last = None;
    for _ in 0..3 {
        let (report, _) = one_round(&engine, PUBLISHED_MIN_UNDUMPED_WAL_RECORDS);
        assert!(
            report.plan.selected_dump_buckets.is_empty(),
            "the published settings selected {} buckets to dump. This whole test rests on the \
             dump being delayed on every round",
            report.plan.selected_dump_buckets.len()
        );
        assert!(
            report.pressure_signals.undumped_wal_records < PUBLISHED_MIN_UNDUMPED_WAL_RECORDS,
            "undumped log records reached {} against a threshold of \
             {PUBLISHED_MIN_UNDUMPED_WAL_RECORDS}, so the dump is no longer delayed",
            report.pressure_signals.undumped_wal_records
        );
        last = Some(report);
    }
    let published = last.expect("three rounds ran");
    let drained_published = drained_total() - drained_before;
    let (published_objects, published_buckets) = dirty_state(&engine);
    println!(
        "  PUBLISHED SETTINGS: after 3 rounds, dirty objects {published_objects}, dirty buckets \
         {published_buckets}, drained {drained_published}, round reports \
         {} dirty buckets"
    , published.pressure_signals.dirty_bucket_count);

    assert_eq!(
        drained_published, 0,
        "the dirty set was drained {drained_published} times under the published settings. The \
         claim being corrected is that it is NEVER drained there"
    );
    assert_eq!(
        published_objects, RECORDS,
        "every one of the {RECORDS} seeded records should still be dirty under the published \
         settings, and {published_objects} are"
    );
    assert_eq!(
        published.pressure_signals.dirty_bucket_count, published_buckets,
        "the round reported {} dirty buckets and the shard is holding {published_buckets}. The \
         round's report is the thing being defended here: it has to equal what the shard holds",
        published.pressure_signals.dirty_bucket_count
    );

    // ARM B, THE CONTROL: the same store, one threshold lowered, nothing else changed.
    let control_dir = tempfile::tempdir().expect("tempdir");
    let control = round_engine(control_dir.path());
    seed(&control, 0, RECORDS);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let control_drained_before = drained_total();
    let (control_report, _) = one_round(&control, DUMPING_MIN_UNDUMPED_WAL_RECORDS);
    let control_drained = drained_total() - control_drained_before;
    let (control_objects, control_buckets) = dirty_state(&control);
    println!(
        "  CONTROL ARM (threshold {DUMPING_MIN_UNDUMPED_WAL_RECORDS}): selected \
         {} buckets, drained {control_drained}, dirty objects now {control_objects}, dirty \
         buckets now {control_buckets}",
        control_report.plan.selected_dump_buckets.len()
    );
    assert_eq!(
        control_drained, RECORDS as u64,
        "the control arm drained {control_drained} objects where the store holds {RECORDS}. If \
         the control does not retire the whole store, it is not a control for an arm that \
         retires none of it"
    );
    assert_eq!(
        control_objects, 0,
        "the control arm left {control_objects} dirty objects behind. The point of the arm is \
         that the SAME store, with only the dump threshold moved, retires everything"
    );
}

// ---------------------------------------------------------------------------------------------
// 2. The strong form: the set, element by element, against a witness from outside the engine.
// ---------------------------------------------------------------------------------------------

/// THE SET, NOT THE COUNT, AT FIVE POINTS IN A WRITE SEQUENCE.
///
/// A round that processes a clean bucket is wasteful. A round that MISSES a dirty one leaves data
/// unreflected, and that is silent until someone looks for a record that is not there. So the
/// assertion here is set equality element by element, and both differences are named separately:
/// a bucket the engine calls dirty that the witness does not is waste, and a bucket the witness
/// calls dirty that the engine does not is the one that matters.
///
/// THE WITNESS IS BUILT OUTSIDE THE ENGINE. It is the fixture's own list of keys written since the
/// last dump, hashed through `block_routing_bucket` with the shard's own routing range. It shares
/// no state with `shard.dirty_objects`: the engine's set is maintained at the write and drained at
/// the dump, the witness is accumulated by the caller and cleared when the caller sees the dump
/// retire everything. Comparing the engine's set against a re-derivation of the engine's set would
/// be an identity and would pass however wrong both were.
#[test]
fn the_set_of_buckets_the_round_acts_on_equals_a_witness_built_outside_the_engine() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    let (start, end) = routing_range(&engine);

    // Keys written and not yet retired by a dump. The caller's own book, kept by the caller.
    let mut outstanding: Vec<String> = Vec::new();
    let mut checkpoints = 0usize;

    let mut check = |engine: &TemporalEngine, outstanding: &[String], label: &str| {
        let witness = outstanding
            .iter()
            .map(|key| crate::engine::hashing::block_routing_bucket(key, start, end))
            .collect::<BTreeSet<u32>>();
        let engine_set = dirty_bucket_set(engine);
        let only_engine = engine_set.difference(&witness).copied().collect::<Vec<_>>();
        let only_witness = witness.difference(&engine_set).copied().collect::<Vec<_>>();
        println!(
            "  {label:34} outstanding keys {:>6}, witness buckets {:>6}, engine buckets {:>6}, \
             engine-only {:>4}, witness-only {:>4}",
            outstanding.len(),
            witness.len(),
            engine_set.len(),
            only_engine.len(),
            only_witness.len()
        );
        assert!(
            only_witness.is_empty(),
            "{label}: {} bucket(s) hold a record written since the last dump that the round does \
             NOT call dirty, first four {:?}. This is the direction that loses data: the round \
             will not dump them and nothing will say so",
            only_witness.len(),
            only_witness.iter().take(4).collect::<Vec<_>>()
        );
        assert!(
            only_engine.is_empty(),
            "{label}: the round calls {} bucket(s) dirty that hold nothing written since the last \
             dump, first four {:?}. Not a loss, but the tracking is then not exact and no figure \
             derived from it is either",
            only_engine.len(),
            only_engine.iter().take(4).collect::<Vec<_>>()
        );
        checkpoints += 1;
    };

    // POINT 1: a fresh store, nothing written. Both sets empty -- asserted, because two empty sets
    // are equal for free and the points after it are what give this one meaning.
    check(&engine, &outstanding, "1: fresh store");

    // POINT 2: a first write burst, nothing dumped yet.
    outstanding.extend(seed(&engine, 0, 4_000));
    check(&engine, &outstanding, "2: 4,000 written, no round");

    // POINT 3: after a round that dumps. The dump retires everything it captured, so the witness
    // is cleared to match -- and the assertion is that the engine cleared exactly the same set.
    std::thread::sleep(std::time::Duration::from_millis(30));
    let drained_before = drained_total();
    let (report, _) = one_round(&engine, DUMPING_MIN_UNDUMPED_WAL_RECORDS);
    let retired = drained_total() - drained_before;
    assert_eq!(
        retired as usize,
        outstanding.len(),
        "the round retired {retired} objects where the witness was holding {} outstanding. The \
         witness can only be cleared below if the dump captured all of it",
        outstanding.len()
    );
    assert!(
        !report.plan.selected_dump_buckets.is_empty(),
        "the round selected no bucket to dump, so this point tests nothing"
    );
    outstanding.clear();
    check(&engine, &outstanding, "3: after a dumping round");

    // POINT 4: a SMALL backlog against a store that is now much larger than it. This is the point
    // the whole question is about.
    outstanding.extend(seed(&engine, 4_000, 250));
    check(&engine, &outstanding, "4: 250 against a 4,000 store");
    assert!(
        dirty_bucket_set(&engine).len() < 4_000,
        "the round calls {} buckets dirty for a backlog of 250 against a store of 4,000. The \
         round IS able to tell a small backlog from a large one, and this is the assertion that \
         says so",
        dirty_bucket_set(&engine).len()
    );

    // POINT 5: more writes on top of the small backlog, still undumped. The two bursts have to
    // accumulate into one set rather than the second replacing the first.
    outstanding.extend(seed(&engine, 4_250, 250));
    check(&engine, &outstanding, "5: 500 accumulated, no round");

    assert_eq!(
        checkpoints, 5,
        "the sequence was supposed to compare the two sets at five points and compared \
         {checkpoints}"
    );
}

// ---------------------------------------------------------------------------------------------
// 3. Both regimes, both sizes, with the control arm asserted by name.
// ---------------------------------------------------------------------------------------------

/// A ROUND WITH AN EMPTY BACKLOG STILL COSTS THE WHOLE STORE.
///
/// Two backlogs -- NOTHING pending, and a fixed 2,000 records pending -- at two store sizes five
/// times apart, with the dump firing so the backlog in each arm is the one the fixture asked for.
///
/// The empty-backlog arm is the finding. It retires zero records and walks 2,889,724 live-page
/// entries at 100,000 records. Per record of work retired that is not a large number, it is
/// undefined; per record of STORE it is 28.9 and it is the same 28.9 at a fifth of the size.
///
/// THE CONTROL ARM, BY NAME: the empty arm asserts it retired NOTHING (so its cost cannot be
/// attributed to work), and the fixed arm asserts its backlog did NOT grow with the store (2,000
/// at both sizes) while its cost did. A fixture that stopped distinguishing the two arms would
/// land them on the same figure, which is asserted against too.
#[test]
fn a_round_with_an_empty_backlog_still_costs_the_whole_store() {
    let empty_small = measure(SMALL, 0);
    let empty_large = measure(LARGE, 0);
    let loaded_small = measure(SMALL, FIXED_PENDING);
    let loaded_large = measure(LARGE, FIXED_PENDING);

    // HOLD THE STORE PATH LENGTH CONSTANT ACROSS ARMS. Allocation bytes move at 6.0 bytes per path
    // character, and although this measurement counts walk entries rather than bytes, an arm built
    // under a different path is not the same fixture.
    let lengths = [
        empty_small.path_len,
        empty_large.path_len,
        loaded_small.path_len,
        loaded_large.path_len,
    ];
    assert!(
        lengths.iter().all(|length| *length == lengths[0]),
        "the four arms were built under store paths of different lengths {lengths:?}, so they are \
         not the same fixture at two sizes"
    );
    println!("  store path length held at {} bytes in all four arms", lengths[0]);

    // The slab count is the second term this fixture does not hold still across store sizes, so it
    // is held still here and said out loud.
    assert_eq!(
        empty_small.slabs, empty_large.slabs,
        "the empty arms ran on {} and {} slabs, so their ratio carries a slab term as well as a \
         store term",
        empty_small.slabs, empty_large.slabs
    );
    assert_eq!(
        loaded_small.slabs, loaded_large.slabs,
        "the loaded arms ran on {} and {} slabs",
        loaded_small.slabs, loaded_large.slabs
    );

    println!();
    println!("  BACKLOG           store {SMALL:>7}   store {LARGE:>7}    ratio   /rec store");
    for (label, small, large) in [
        ("empty", &empty_small, &empty_large),
        ("2,000 records", &loaded_small, &loaded_large),
    ] {
        println!(
            "  {label:16} {:>13} {:>15}   {:>6.3}x   {:.3} -> {:.3}",
            small.whole_call_entries,
            large.whole_call_entries,
            large.whole_call_entries as f64 / small.whole_call_entries as f64,
            small.per_record_of_store(),
            large.per_record_of_store(),
        );
    }
    let marginal_small = loaded_small.whole_call_entries - empty_small.whole_call_entries;
    let marginal_large = loaded_large.whole_call_entries - empty_large.whole_call_entries;
    println!(
        "  {:16} {marginal_small:>13} {marginal_large:>15}   {:>6.3}x   {:.3} -> {:.3} per record \
         of WORK",
        "marginal",
        marginal_large as f64 / marginal_small as f64,
        marginal_small as f64 / FIXED_PENDING as f64,
        marginal_large as f64 / FIXED_PENDING as f64,
    );
    println!();

    // THE CONTROL ARM, NAMED: the empty arms retired nothing at all.
    for (label, arm) in [("small", &empty_small), ("large", &empty_large)] {
        assert_eq!(
            arm.backlog, 0,
            "the {label} empty arm was built with a backlog of {}, which is not empty",
            arm.backlog
        );
        assert_eq!(
            arm.retired, 0,
            "the {label} empty arm retired {} records. The whole claim is that this round had \
             NOTHING to do, and a round that retired something did have something to do",
            arm.retired
        );
        assert_eq!(
            arm.report.pressure_signals.dirty_bucket_count, 0,
            "the {label} empty arm's round reported {} dirty buckets. The round is supposed to be \
             reporting an empty backlog truthfully here",
            arm.report.pressure_signals.dirty_bucket_count
        );
    }

    // THE OTHER CONTROL ARM: the loaded arms' backlog did NOT grow with the store.
    assert_eq!(
        loaded_small.backlog, loaded_large.backlog,
        "the loaded arms carried backlogs of {} and {}, so their cost ratio is measuring the \
         backlog and not the store",
        loaded_small.backlog, loaded_large.backlog
    );
    for (label, arm) in [("small", &loaded_small), ("large", &loaded_large)] {
        assert_eq!(
            arm.retired, FIXED_PENDING,
            "the {label} loaded arm retired {} of the {FIXED_PENDING} records it wrote, so its \
             per-record-of-work figure divides by the wrong number",
            arm.retired
        );
    }

    // A FIXTURE THAT STOPPED DISTINGUISHING THE ARMS WOULD LAND THEM ON THE SAME FIGURE.
    assert_ne!(
        empty_large.whole_call_entries, loaded_large.whole_call_entries,
        "the empty and loaded arms cost the same {} entries at {LARGE} records. Two arms that \
         differ by 2,000 written records are not supposed to be indistinguishable",
        empty_large.whole_call_entries
    );

    // THE FINDING. An empty backlog costs a multiple of the store, and the multiple is the store's.
    let empty_ratio =
        empty_large.whole_call_entries as f64 / empty_small.whole_call_entries as f64;
    let store_ratio = empty_large.store_records as f64 / empty_small.store_records as f64;
    assert!(
        empty_ratio > store_ratio * 0.9,
        "a round with an EMPTY backlog cost {empty_ratio:.3}x more on a store {store_ratio:.3}x \
         larger. If that ratio has fallen well below the store's, the floor has stopped tracking \
         the store and this file's finding no longer holds"
    );
    assert!(
        empty_large.whole_call_entries * 100 > loaded_large.whole_call_entries * 70,
        "the empty-backlog round is now {} entries against the loaded round's {}, which is less \
         than 70% of it. The finding is that a round with nothing to do costs most of what a round \
         with work costs",
        empty_large.whole_call_entries,
        loaded_large.whole_call_entries
    );

    // PER RECORD OF STORE IS THE DENOMINATOR THAT HIDES THIS. Reported, and asserted flat, so the
    // table cannot be read as if the healthy-looking figure were the whole story.
    let flat = empty_large.per_record_of_store() / empty_small.per_record_of_store();
    assert!(
        (0.9..1.1).contains(&flat),
        "per record of STORE moved {flat:.3}x between the two empty arms. It is supposed to be \
         flat -- that is exactly what makes it the wrong denominator"
    );

    // LABEL THE STAGES THIS FIXTURE NEVER EXERCISED, and assert they did nothing, so a later
    // fixture that gives them work fails here rather than turning an absent measurement into a
    // verdict.
    for arm in [&empty_small, &empty_large, &loaded_small, &loaded_large] {
        for name in UNEXERCISED_STAGES {
            let stage = arm.stage(name);
            assert_eq!(
                stage.walk.live_block_entries, 0,
                "{name} is labelled NOT EXERCISED in this file and walked {} entries in the \
                 store-{}/backlog-{} arm. Its zero has become a measurement and the label is now \
                 a lie",
                stage.walk.live_block_entries, arm.store_records, arm.backlog
            );
        }
    }
    for name in UNEXERCISED_STAGES {
        println!("  {name}: NOT EXERCISED, its zero is not a measurement");
    }
}

// ---------------------------------------------------------------------------------------------
// 4. Crash and restart.
// ---------------------------------------------------------------------------------------------

/// THE DIRTY SET IS IN MEMORY ONLY AND IS EMPTY AFTER A RESTART. THAT IS SAFE, AND HERE IS WHY.
///
/// A dirty set that empties on restart is either safe -- everything is rebuilt -- or a silent loss,
/// and which one it is cannot be argued from the fact that it empties. It is safe here, because
/// durability is anchored on the persisted bucket index and the log rather than on the dirty set:
/// `load_shard_with` clears every bucket and page dirty flag on load and
/// `refresh_bucket_runtime_flags` recomputes `bucket.dirty` from the (empty) set, which is the
/// clear-dirty-on-load contract. Asserted by reading the records back, after the restart, after a
/// round that dumped nothing, and after a SECOND restart -- because a fix that preserves data for
/// a consumer that discards it one line later is a shape this campaign has already found.
///
/// WHAT A RESTART DOES TAKE AWAY is dump SELECTION. The round after a restart selects no bucket to
/// dump even though the log still counts those writes as undumped, because selection is driven off
/// the dirty set and the dirty set is empty. No record is lost by that -- asserted below -- but it
/// is the constraint on anyone who would drive MORE of the round from this set, and it is asserted
/// rather than described so that a change to it fails here.
#[test]
fn a_restart_empties_the_dirty_set_and_no_record_is_lost() {
    const RECORDS: usize = 2_000;
    const SAMPLE: usize = 200;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed(&engine, 0, RECORDS);

    let readable = |engine: &TemporalEngine| -> usize {
        (0..SAMPLE)
            .filter(|cursor| {
                // Only the non-deadline keys: one record in ten is written with a 1 ms deadline
                // and is entitled to be gone.
                cursor % 10 != 0
            })
            .filter(|cursor| {
                let response = engine.batch_execute(crate::types::BatchExecuteRequest {
                    shard_id: 1,
                    commands: vec![Command::StringGet {
                        key: format!("k-{cursor:08}"),
                    }],
                });
                response.status.ok
                    && response.responses.iter().any(|entry| {
                        matches!(
                            &entry.response,
                            crate::types::CommandResponse::Bytes { value: Some(_) }
                        )
                    })
            })
            .count()
    };
    let expected = (0..SAMPLE).filter(|cursor| cursor % 10 != 0).count();

    let (before_objects, before_buckets) = dirty_state(&engine);
    let before_readable = readable(&engine);
    assert_eq!(
        before_objects, RECORDS,
        "the fixture wrote {RECORDS} records and the shard is holding {before_objects} dirty. \
         Nothing below means anything if the set was not full to begin with"
    );
    assert_eq!(
        before_readable, expected,
        "only {before_readable} of {expected} sampled records were readable BEFORE the restart, \
         so the restart cannot be blamed for anything missing after it"
    );
    println!("  BEFORE restart: dirty objects {before_objects}, dirty buckets {before_buckets}, readable {before_readable}/{expected}");

    engine.unload_shard(1);
    engine.load_shard(1);

    let (after_objects, after_buckets) = dirty_state(&engine);
    let after_readable = readable(&engine);
    println!("  AFTER  restart: dirty objects {after_objects}, dirty buckets {after_buckets}, readable {after_readable}/{expected}");
    assert_eq!(
        after_objects, 0,
        "the dirty set held {after_objects} objects after a restart. This test exists to record \
         that it holds NONE -- it is not persisted -- and every argument below is about that"
    );
    assert_eq!(
        after_readable, expected,
        "the dirty set emptied on restart and only {after_readable} of {expected} sampled records \
         came back. That is the silent-loss branch and it is supposed to be unreachable: the \
         records are anchored on the persisted bucket index, not on the dirty set"
    );

    // THE ROUND AFTER A RESTART SELECTS NOTHING TO DUMP. Recorded as a measurement, because it is
    // the constraint on driving more of the round from this set.
    let drained_before = drained_total();
    let (report, _) = one_round(&engine, DUMPING_MIN_UNDUMPED_WAL_RECORDS);
    let retired = drained_total() - drained_before;
    println!(
        "  round after restart: dirty buckets {}, selected {}, undumped log records {}, retired {retired}",
        report.plan.dirty_buckets.len(),
        report.plan.selected_dump_buckets.len(),
        report.pressure_signals.undumped_wal_records,
    );
    assert!(
        report.plan.selected_dump_buckets.is_empty(),
        "the round after a restart selected {} buckets to dump. If that has become possible, the \
         dirty set is surviving a restart from somewhere and the argument in this file's note \
         needs re-deriving rather than adjusting",
        report.plan.selected_dump_buckets.len()
    );
    assert_eq!(
        retired, 0,
        "the round after a restart retired {retired} objects out of a set that is empty"
    );

    // AND THE RECORDS ARE STILL THERE, ACROSS A SECOND RESTART. The dump that did not happen was
    // not needed.
    engine.unload_shard(1);
    engine.load_shard(1);
    let twice_readable = readable(&engine);
    println!("  AFTER  2nd restart: readable {twice_readable}/{expected}");
    assert_eq!(
        twice_readable, expected,
        "only {twice_readable} of {expected} sampled records survived a second restart after a \
         round that dumped nothing. That is the loss this test is here to rule out"
    );
}

// ---------------------------------------------------------------------------------------------
// 5. The residual, from outside, with a planted marker.
// ---------------------------------------------------------------------------------------------

/// AN INDEPENDENT RESIDUAL, AND A PLANT TO PROVE IT COULD HAVE SEEN ONE.
///
/// The figures above are read from a process-wide walk counter taken once around the whole call.
/// The stage rows are separate atomics cleared at every stage boundary. The difference is
/// therefore a reading and not an identity -- but on this fixture it reads ZERO, and a zero is the
/// one reading that cannot be told from an instrument that is not connected.
///
/// So the span is widened to start BEFORE the round, where a real whole-store quantity can be put
/// inside it that no stage row can see: a `bucket_storage_summaries` call made just before the
/// round materialises the entire live page set, is charged to the process-wide counter, and is
/// charged to no stage because the stage accumulators are cleared when the round starts. Two
/// rounds, identical but for that plant, and the residual has to move by exactly the plant.
#[test]
fn the_residual_outside_the_round_stage_rows_recovers_a_planted_whole_store_walk() {
    const RECORDS: usize = 8_000;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed(&engine, 0, RECORDS);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let _settle = one_round(&engine, DUMPING_MIN_UNDUMPED_WAL_RECORDS);

    // (residual entries, planted entries, entries the stage rows claimed)
    fn walk_residual(engine: &TemporalEngine, plant: bool) -> (u64, u64, u64) {
        crate::engine::reset_live_block_scan_entries();
        let planted = if plant {
            let before = crate::engine::live_block_scan_entries();
            // The WALK is what is being planted, so the walk is what is measured: taking the
            // length of the returned vector instead would leave the plant unproven in exactly the
            // case where the instrument is not connected.
            let _summaries = engine.bucket_storage_summaries(1);
            crate::engine::live_block_scan_entries() - before
        } else {
            0
        };
        let report = engine.run_storage_manager_cycle(round_request(DUMPING_MIN_UNDUMPED_WAL_RECORDS));
        let span = crate::engine::live_block_scan_entries();
        let charged = report
            .stages
            .iter()
            .map(|stage| stage.walk.live_block_entries)
            .sum::<u64>();
        (span.saturating_sub(charged), planted, charged)
    }

    let (control_residual, _, control_charged) = walk_residual(&engine, false);
    let (planted_residual, planted, treatment_charged) = walk_residual(&engine, true);

    println!("  CONTROL round: stage rows charged {control_charged} entries, residual {control_residual}");
    println!("  PLANTED round: stage rows charged {treatment_charged} entries, residual {planted_residual}; plant was {planted}");

    // THE PLANT IS A WHOLE-STORE QUANTITY, not a token.
    assert!(
        planted as usize > RECORDS / 2,
        "the plant walked {planted} entries on an {RECORDS}-record store. It is supposed to be \
         the whole live page set; something this small means the engine walked something else and \
         the recovery below is about the wrong quantity"
    );
    assert_eq!(
        control_residual, 0,
        "the CONTROL round left {control_residual} entries unattributed to any stage. A nonzero \
         control residual means a stage span no longer covers the work charged inside it, and \
         every stage row in this file is then reading across a boundary that has drifted"
    );
    assert_eq!(
        planted_residual,
        control_residual + planted,
        "the residual did not recover the planted {planted} entries exactly: it moved from \
         {control_residual} to {planted_residual}. A residual that does not move by exactly what \
         is put in front of it is not measuring what is outside the stage rows"
    );
}
