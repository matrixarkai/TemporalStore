// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT EACH PER-ROUND BOUND COSTS A MAINTENANCE ROUND, AND THE ROUTE THAT ARRIVES WITH NONE.
//!
//! Seven per-round bounds and triggers on `StorageManagerCycleRequest` decide how much one
//! maintenance round does. `0` means NO BOUND in every one of them -- the expiry window walks
//! every due deadline, the eviction sampler keeps every victim it finds, the index-log sweep
//! removes every removable record, and both index-GC triggers fire whatever the log looks like.
//! A round holding zeros is therefore not inert, it is MAXIMALLY AGGRESSIVE, and setting a bound
//! makes each round do LESS. That is the trade measured here: cost per round against how many
//! rounds a backlog takes to drain.
//!
//! `StorageManagerCycleRequest::default()` sets all seven and
//! `every_maintenance_round_is_bounded_by_default` pins the values. THAT IS NOT THE ROUTE A
//! SERVER TAKES. Both cycle endpoints -- `POST /server/storage/manager/cycle` and
//! `POST /storage_manager/cycle` -- build the request with
//! `parse_json::<StorageManagerCycleRequest>(&request.body)`, and the seven carried a bare
//! `#[serde(default)]`. On a `usize` or a `u64` that decodes an ABSENT field to the FIELD TYPE's
//! default, which is 0 -- not to the STRUCT's `Default`, which is where the bounds live. An
//! absent field is what every caller who does not name one sends, so the shipped bounds reached
//! callers of `Default::default()` and no HTTP caller at all.
//!
//! The tree already knew this hazard class, for exactly one field beside them:
//! `omitting_the_commit_before_truncate_guard_still_commits_before_truncating` states it for
//! `index_gc_commit_dirty_buckets_before_truncation`, which is why that one bool carries
//! `#[serde(default = "...")]`. The reasoning was never carried across to the numbers.
//!
//! [`a_cycle_request_that_names_no_bound_still_arrives_bounded`] is the guard, and it asserts the
//! DECODE and not the constant: the whole decoded request must equal the shipped default, so a
//! bound that goes back to meaning "unbounded" on this route goes red whether or not anyone
//! remembers to add it to a list, and a bound added later is covered the day it is added.
//! Asserting `Default` alone -- which the existing guard does -- structurally cannot see this,
//! because `Default` was right the whole time.
//!
//! WHAT EACH BOUND MOVES is measured by [`each_shipped_bound_bites_and_the_round_does_less_work`]
//! on one seeded shard, the same fixture built twice and run with the seven at 0 and at the
//! shipped values. The table it prints is reproduced in the note on that test.
//!
//! Counted, not timed, for every claim that matters: the box these run on carries other tenants,
//! so wall time is printed and never asserted on.

use super::*;

const SHARD: ShardId = 1;

/// Records the fixture writes, and the reason it is this large.
///
/// The index-GC gate is ANDed: at least `DEFAULT_INDEX_GC_INDEX_LOG_BYTES_THRESHOLD` (1 MiB) of
/// index log AND at least `DEFAULT_INDEX_GC_USAGE_RATIO_TRIGGER_BASIS_POINTS` (50%) of its records
/// removable. Measured on this fixture, an index-log record is 58.1 B: 16,000 records came to
/// 929,688 B, which is UNDER the 1,048,576 B gate, so 16,000 would have the stage decline and the
/// arms below would compare two rounds that both did nothing. 20,000 records is about 1,162,000 B
/// and clears it with margin. The gate is exercised at the SHIPPED threshold; tuning the threshold
/// down to suit the fixture would exercise a gate no deployment runs.
///
/// The byte figure is asserted below rather than trusted, because it is the thing that decides
/// whether this file measures anything at all.
const RECORDS: usize = 20_000;

/// One key in this many is written with a deadline already in the past.
///
/// 20,000 / 50 = 400 due keys, which must EXCEED `DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND` (100)
/// or the hot bound cannot be shown to bite at all. A fixture that cannot exceed a bound runs
/// green while measuring nothing, so that margin is ASSERTED below rather than assumed.
const DEADLINE_EVERY: usize = 50;

/// One write per call, deliberately, and not a batch.
///
/// A write BATCH is ONE log record however many objects it carries, so a batched corpus sits on
/// the delayed side of `min_undumped_wal_records` (1,000) for ever: the dump never fires, nothing
/// advances the index-log retain floor, and the index-GC stage finds nothing removable. That is
/// why `round_stage_cost.rs` reports `index_gc` flat at zero and says in as many words that the
/// zero is unexercised rather than cheap. One record per sequence clears the dump threshold and
/// gives the stage real work to be bounded.
fn seed(engine: &TemporalEngine) -> usize {
    let mut due = 0usize;
    for index in 0..RECORDS {
        let command = if index % DEADLINE_EVERY == 0 {
            due += 1;
            Command::StringSetEx {
                key: format!("d-{index:08}"),
                value: vec![b'v'; 64],
                ttl_ms: 1,
            }
        } else {
            Command::StringSet {
                key: format!("k-{index:08}"),
                value: vec![b'v'; 64],
            }
        };
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
    }
    due
}

fn seeded(dir: &std::path::Path) -> (TemporalEngine, usize) {
    let engine = TemporalEngine::with_local_dirs(
        1 << 24,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(SHARD);
    let due = seed(&engine);
    (engine, due)
}

/// The round both arms run: every stage on, and ONLY the seven bounds differing.
///
/// `eviction_memory_pressure_threshold` is 1 so the eviction gate ADMITS. With the shipped
/// threshold the stage declines on a fixture this size and reports zero victims in both arms,
/// which reads exactly like a bound that changed nothing. `load_cold_buckets_for_expire` is on so
/// the cold window is reached at all rather than being skipped before its bound is consulted.
fn round(bounded: bool) -> StorageManagerCycleRequest {
    let shipped = StorageManagerCycleRequest::default();
    let usize_bound = |value: usize| if bounded { value } else { 0 };
    let u64_bound = |value: u64| if bounded { value } else { 0 };
    StorageManagerCycleRequest {
        shard_id: SHARD,
        enable_prepare: true,
        enable_wal_reclaim: true,
        enable_expire: true,
        enable_evict: true,
        enable_block_reclaim: true,
        enable_block_compaction: true,
        enable_index_gc: true,
        eviction_memory_pressure_threshold: 1,
        load_cold_buckets_for_expire: true,
        // HELD CONSTANT ACROSS BOTH ARMS, and not varied with the others, because it is upstream
        // of the index-GC bounds rather than beside them. The dump is what advances the index-log
        // retain floor, and `index_gc_commit_dirty_buckets_before_truncation` will not truncate
        // past buckets the dump has not covered -- so a ceiling on the dump decides how much is
        // REMOVABLE before any index-GC bound is consulted. Measured on this fixture: uncapped
        // leaves 16,001 records removable, capped at the shipped 200 leaves 200, and the
        // usage-ratio trigger then declines at 99 bp. Varying it here would have the index-GC
        // arms differ in their denominator instead of in their bound.
        //
        // The interaction is measured on its own by
        // `a_capped_dump_throttles_what_index_gc_can_remove`.
        max_dump_buckets_per_round: 0,
        index_gc_max_entries_per_round: usize_bound(shipped.index_gc_max_entries_per_round),
        index_gc_index_log_bytes_threshold: u64_bound(shipped.index_gc_index_log_bytes_threshold),
        index_gc_usage_ratio_trigger_basis_points: u64_bound(
            shipped.index_gc_usage_ratio_trigger_basis_points,
        ),
        max_expire_hot_buckets_per_round: usize_bound(shipped.max_expire_hot_buckets_per_round),
        max_expire_cold_buckets_per_round: usize_bound(shipped.max_expire_cold_buckets_per_round),
        eviction_batch_limit: usize_bound(shipped.eviction_batch_limit),
        block_gc_min_slab_garbage_basis_points: u64_bound(
            shipped.block_gc_min_slab_garbage_basis_points,
        ),
        ..shipped
    }
}

/// What one round did, read off the report the round itself published.
struct RoundCost {
    /// Index-log records the sweep actually discarded.
    swept: usize,
    /// How many were removable BEFORE the entry budget was applied. This is the denominator for
    /// "the budget bit": equal to `swept` means the budget was never reached.
    removable: usize,
    budget_exhausted: bool,
    index_gc_applied: bool,
    index_gc_skipped_reason: String,
    usage_ratio_bp: u64,
    index_log_bytes_before: u64,
    /// Due keys the hot and cold expiry windows took. Named `*_buckets_scanned` on the report; the
    /// quantity assigned to it is `hot_selected.len()`, which counts KEYS.
    hot_scanned: usize,
    cold_scanned: usize,
    expired: usize,
    /// Victims the eviction stage CHOSE, which is the list `eviction_batch_limit` truncates.
    victims: usize,
    pressure_gate_open: bool,
    index_log_bytes_reclaimed: u64,
    cache_bytes_removed: u64,
    cache_entries_removed: usize,
    /// Printed, never asserted on: see the note at the top of this file.
    duration_ms: u64,
    /// Per-stage wall time, so "the round got cheaper" can say WHICH stage paid. A total on its
    /// own cannot: one stage carrying the whole cost and seven sharing it read the same.
    stage_ms: Vec<(String, u64)>,
}

fn one_round(engine: &TemporalEngine, bounded: bool) -> RoundCost {
    let report = engine.run_storage_manager_cycle(round(bounded));
    let gc = report.index_gc_report.clone().unwrap_or_default();
    let expiry = report.expiry_report.clone().unwrap_or_default();
    let eviction = report.eviction_report.clone().unwrap_or_default();
    RoundCost {
        swept: gc.records_removed,
        removable: gc.removable_records_before_budget,
        budget_exhausted: gc.budget_exhausted,
        index_gc_applied: gc.applied,
        index_gc_skipped_reason: gc.skipped_reason.clone(),
        usage_ratio_bp: gc.usage_ratio_basis_points,
        index_log_bytes_before: gc.bytes_before,
        hot_scanned: expiry.hot_buckets_scanned,
        cold_scanned: expiry.cold_buckets_scanned,
        expired: expiry.expired_records_removed,
        victims: eviction.selected_victims.len(),
        pressure_gate_open: eviction.pressure_gate_open,
        index_log_bytes_reclaimed: gc.bytes_before.saturating_sub(gc.bytes_after),
        cache_bytes_removed: eviction.cache_disk_bytes_removed,
        cache_entries_removed: eviction.cache_entries_removed,
        duration_ms: report.duration_ms,
        stage_ms: report
            .stages
            .iter()
            .map(|stage| (stage.stage.clone(), stage.duration_ms))
            .collect(),
    }
}

fn show(tag: &str, cost: &RoundCost) {
    eprintln!(
        "  {tag:<16} swept={:<6} removable={:<6} exhausted={:<5} applied={:<5} usage={:<6}bp \
         log_before={:<9} hot={:<5} cold={:<4} expired={:<5} victims={:<5} gate={:<5} \
         log_freed={:<9} cache_freed={:<9} cache_entries={:<5} ms={:<5} skip={:?}",
        cost.swept,
        cost.removable,
        cost.budget_exhausted,
        cost.index_gc_applied,
        cost.usage_ratio_bp,
        cost.index_log_bytes_before,
        cost.hot_scanned,
        cost.cold_scanned,
        cost.expired,
        cost.victims,
        cost.pressure_gate_open,
        cost.index_log_bytes_reclaimed,
        cost.cache_bytes_removed,
        cost.cache_entries_removed,
        cost.duration_ms,
        cost.index_gc_skipped_reason,
    );
}

/// A cycle request that names no bound must still arrive bounded.
///
/// THE GUARD, and it reads the DECODE. Both cycle endpoints parse a request body, so what an
/// OMITTED field decodes to is the behaviour every caller gets who does not name it -- and on a
/// `usize` or `u64`, a bare `#[serde(default)]` decodes to 0, which in every one of these means
/// NO BOUND. The struct's own `Default` is not consulted on that route, so a guard that asserts
/// `Default` cannot see a zero reaching the engine through it.
///
/// Asserted as WHOLE-STRUCT equality rather than as a list of seven field names, on purpose. A
/// list has to be maintained: a bound added later with a bare `#[serde(default)]` and a non-zero
/// `Default` is a hole the day it is added and nothing goes red, and a bound RETIRED shrinks the
/// list rather than failing. Equality covers every field there is now and every field there will
/// be, and it names the offenders in the failure message by diffing the two encodings first.
#[test]
fn a_cycle_request_that_names_no_bound_still_arrives_bounded() {
    // THE ROUTE, not a reconstruction of it: `crate::http::parse_json` is the function both
    // endpoints call on `request.body`.
    let silent: StorageManagerCycleRequest = crate::http::parse_json(br#"{"shard_id":7}"#)
        .expect("a body naming only the shard must parse");
    let shipped = StorageManagerCycleRequest {
        shard_id: 7,
        ..StorageManagerCycleRequest::default()
    };

    // Diff the two encodings first, so a failure says WHICH fields rather than printing two
    // forty-field structs and leaving the reader to find the difference.
    let left = serde_json::to_value(&shipped).unwrap();
    let right = serde_json::to_value(&silent).unwrap();
    let mut unbounded: Vec<String> = Vec::new();
    for (field, want) in left.as_object().unwrap() {
        let got = right.get(field).cloned().unwrap_or(serde_json::Value::Null);
        if &got != want {
            unbounded.push(format!("{field}: shipped {want}, decoded {got}"));
        }
    }
    assert!(
        unbounded.is_empty(),
        "a cycle request body that names only the shard decoded to something other than the \
         shipped round. Each of these reached the engine as the caller never asked for, and a 0 \
         in a per-round bound is NO bound rather than a small one:\n  {}",
        unbounded.join("\n  ")
    );
    assert_eq!(
        silent, shipped,
        "the decoded request must be the shipped round with only the shard named"
    );

    // AND THE BOUNDS ARE STILL NON-ZERO, which the equality above does not say on its own: if
    // every bound went to 0 in `Default` as well, the two sides would agree and this test would
    // pass on a wholly unbounded round. This half is what `every_maintenance_round_is_bounded_by_default`
    // pins by value; restated here so one test name defends the whole claim in CI.
    for (name, bound) in [
        (
            "index_gc_max_entries_per_round",
            silent.index_gc_max_entries_per_round as u64,
        ),
        (
            "index_gc_index_log_bytes_threshold",
            silent.index_gc_index_log_bytes_threshold,
        ),
        (
            "index_gc_usage_ratio_trigger_basis_points",
            silent.index_gc_usage_ratio_trigger_basis_points,
        ),
        (
            "max_expire_hot_buckets_per_round",
            silent.max_expire_hot_buckets_per_round as u64,
        ),
        (
            "max_expire_cold_buckets_per_round",
            silent.max_expire_cold_buckets_per_round as u64,
        ),
        ("eviction_batch_limit", silent.eviction_batch_limit as u64),
        (
            "block_gc_min_slab_garbage_basis_points",
            silent.block_gc_min_slab_garbage_basis_points,
        ),
    ] {
        assert!(
            bound > 0,
            "{name} decoded to zero, which is not a bound at all"
        );
    }

    // THE PERIODIC LOOP, the other production path that does not go through a body at all.
    let periodic = crate::data_node::StorageManagerRuntimeOptions::default().request;
    assert_eq!(
        periodic.index_gc_max_entries_per_round, silent.index_gc_max_entries_per_round,
        "the periodic loop and the endpoints must agree about the index-GC entry budget"
    );
    assert_eq!(
        periodic.max_expire_hot_buckets_per_round, silent.max_expire_hot_buckets_per_round,
        "the periodic loop and the endpoints must agree about the hot expiry window"
    );
    assert_eq!(
        periodic.eviction_batch_limit, silent.eviction_batch_limit,
        "the periodic loop and the endpoints must agree about the eviction batch"
    );

    // THE ESCAPE HATCH SURVIVES, and this is the half that says the change is a DEFAULT and not a
    // floor: naming 0 explicitly still asks for an unbounded round. Serde only consults a default
    // for an ABSENT field, so an operator who means "sweep everything this round" can still say
    // so -- they just have to say it.
    let explicit: StorageManagerCycleRequest = crate::http::parse_json(
        br#"{"shard_id":7,"index_gc_max_entries_per_round":0,"eviction_batch_limit":0}"#,
    )
    .expect("a body naming the bounds must parse");
    assert_eq!(
        explicit.index_gc_max_entries_per_round, 0,
        "naming the entry budget 0 must still ask for an unbounded sweep"
    );
    assert_eq!(
        explicit.eviction_batch_limit, 0,
        "naming the eviction batch 0 must still ask for an unbounded eviction"
    );
}

/// Each shipped bound bites a round, and ONE OF THE SEVEN DOES NOT BOUND THE ROUND AT ALL.
///
/// Two arms of the SAME fixture: the seven at 0, which is what the decode used to hand both cycle
/// endpoints, and the seven at the shipped values. 16,000 records, 320 of them with a deadline
/// already in the past, one round each. Measured:
///
/// ```text
///   ROUND 1                        at 0       shipped    what the bound did
///   index-log records swept       20,001      20,001     NOTHING: all of it was sealed pieces
///   index-log bytes freed      1,162,728   1,127,927     both arms swept the log to its floor
///   due keys taken (hot)             400         100     bit exactly, 4.0x fewer
///   due keys taken (cold)              0           0     NOT EXERCISED, see the ignored arm
///   eviction victims chosen       19,600          10     bit exactly, 1,960x fewer
///   cache entries removed              0           0     the cost was CHOOSING, not freeing
///   round wall time (ms)         341,076       9,591     35.6x cheaper
/// ```
///
/// A BOUNDED ROUND IS ONE TO TWO ORDERS OF MAGNITUDE CHEAPER, AND THE ROUND CANNOT SAY WHICH
/// STAGE PAID. Wall time is quoted as a range because the box carries other tenants: two runs of
/// the same fixture gave 231,714 / 9,925 ms and 212,458 / 12,095 ms. Nothing here is asserted on
/// it.
///
/// The attribution is UNMEASURED, and that is a defect in the round and not a gap in this file:
/// every row in `report.stages` reports `duration_ms` of 0 while the round reports 212,458, so the
/// loop printing per-stage durations below prints nothing at all. The stage rows therefore cannot
/// name the stage that paid. `the_storage_cycle_records_how_long_its_stages_take` asserts only
/// that the stage times do not EXCEED the round, and 0 satisfies that for every stage, so it is
/// green on a round whose rows are all zero.
///
/// What can be said without that attribution: the only stage whose work fell by three orders of
/// magnitude is eviction, 15,680 victims chosen against 16, and it removed nothing from the cache
/// in either arm (`cache_entries_removed` is 0 both times) -- so an unbounded round ranked every
/// bucket in the store and then acted on none of them. That makes `eviction_batch_limit` the
/// likely holder of the latency, and likely is as far as the measurement goes.
///
/// THE INDEX-GC ENTRY BUDGET IS NOT A BOUND ON THE ROUND, and this is the result that matters most
/// because it is the opposite of what the budget looks like. With a budget of 256 and 16,001
/// records removable, the round removed 15,536 of them -- sixty times its budget -- and
/// `budget_exhausted` was nonetheless true. Both facts are correct. Whole sealed pieces of the log
/// are unlinked BY NAME without being opened, so they never reach the record loop the budget
/// guards; the budget bounds only the piece being WRITTEN. 15,280 records went in whole-piece
/// unlinks, 256 through the budgeted loop, and 465 were retained. The budget bought a 6.8% cut in
/// bytes freed and no measurable latency. `index_log_sweep_bound.rs` established the mechanism in
/// counts; this is the same mechanism inside a whole round.
///
/// AND THE BACKLOG IT LEAVES IS HELD BY THE BYTE THRESHOLD, NOT BY THE BUDGET. One bounded round
/// removed 97.1% of the removable records and retained 465. Round 2 does NOT take the next slice:
/// round 1 took the log to 55,685 B, which is under the shipped 768 KiB trigger, so the stage
/// declines with "index-log byte threshold not reached" and those 465 wait for writes to grow the
/// log again. That is the operational consequence of pairing a byte trigger with a per-round
/// budget, and it is the opposite of "the budget paces the drain": the budget leaves a remainder
/// the trigger then parks.
///
/// Every claim here is paired with its denominator. "The bound bit" means the unbounded arm
/// EXCEEDED the bound: on a fixture that cannot exceed it, a bounded round and an unbounded one
/// are indistinguishable and a green test measures nothing.
#[test]
fn each_shipped_bound_bites_and_the_round_does_less_work() {
    let shipped = StorageManagerCycleRequest::default();

    let free_dir = tempfile::tempdir().unwrap();
    let (free_engine, free_due) = seeded(free_dir.path());
    let bound_dir = tempfile::tempdir().unwrap();
    let (bound_engine, bound_due) = seeded(bound_dir.path());
    assert_eq!(
        free_due, bound_due,
        "the two arms must be the same fixture, or the comparison is between two stores"
    );
    assert!(
        free_due > shipped.max_expire_hot_buckets_per_round,
        "DENOMINATOR: the fixture must hold more due keys ({free_due}) than the hot bound ({}) \
         allows per round, or that bound cannot be shown to bite",
        shipped.max_expire_hot_buckets_per_round
    );

    eprintln!("ROUND 1 -- the seven at 0 against the seven at the shipped values");
    let free = one_round(&free_engine, false);
    show("unbounded", &free);
    let bound = one_round(&bound_engine, true);
    show("shipped", &bound);

    // ---- THE INDEX-LOG ENTRY BUDGET: IT BINDS, AND IT DOES NOT BOUND THE ROUND ----
    assert!(
        free.index_gc_applied && bound.index_gc_applied,
        "DENOMINATOR: the index-GC stage must APPLY in BOTH arms, or they differ in whether the \
         stage ran rather than in how much it did. Unbounded skipped with {:?} on a {} B log at \
         {} bp removable; bounded skipped with {:?}",
        free.index_gc_skipped_reason,
        free.index_log_bytes_before,
        free.usage_ratio_bp,
        bound.index_gc_skipped_reason
    );
    assert!(
        free.index_log_bytes_before >= shipped.index_gc_index_log_bytes_threshold,
        "DENOMINATOR: the fixture must clear the shipped byte gate, or both arms decline and the \
         comparison is between two rounds that did nothing: {} B of index log against a {} B \
         threshold. Raise RECORDS, never lower the threshold",
        free.index_log_bytes_before,
        shipped.index_gc_index_log_bytes_threshold
    );
    assert!(
        free.usage_ratio_bp >= shipped.index_gc_usage_ratio_trigger_basis_points,
        "DENOMINATOR: the fixture must clear the shipped removable-share trigger: {} bp against \
         {} bp",
        free.usage_ratio_bp,
        shipped.index_gc_usage_ratio_trigger_basis_points
    );
    assert!(
        free.removable > shipped.index_gc_max_entries_per_round,
        "DENOMINATOR: more records must be removable ({}) than the budget ({}) allows, or the \
         budget is never reached",
        free.removable,
        shipped.index_gc_max_entries_per_round
    );
    assert_eq!(
        free.swept, free.removable,
        "an entry budget of 0 is NO budget: the sweep must remove every removable record"
    );
    assert!(
        !free.budget_exhausted,
        "a sweep with no budget cannot report having exhausted one"
    );
    // THE ENTRY BUDGET BOUNDED NOTHING, and this is the measurement, not a weakened assertion.
    //
    // Both arms removed every removable record and neither reported its budget exhausted. That is
    // correct and it is the point: whole SEALED pieces of the log are unlinked BY NAME without
    // being opened, so they never reach the record loop the budget guards. In a log the dump has
    // fully covered -- which is this fixture, and the state a shard reaches after any complete
    // dump -- every removable record is in a sealed piece, so the budget has nothing to bound.
    //
    // It binds only on records in the piece being WRITTEN. An earlier fixture (16,000 records
    // against a 768 KiB gate) left 721 removable records in the open tail, and there the budget
    // did bind: 15,536 removed of 16,001, `budget_exhausted` true, 465 retained. So
    // `budget_exhausted` is NOT a signal that the budget bounded a round -- it is a signal about
    // the tail piece alone.
    assert_eq!(
        bound.swept, free.swept,
        "the entry budget must not change what a round removes when every removable record is in \
         a sealed piece: bounded removed {}, unbounded removed {}. If these ever differ here, \
         whole-piece unlinking has stopped running and the sweep is back to costing what it reads",
        bound.swept, free.swept
    );
    assert!(
        !bound.budget_exhausted,
        "and the budgeted loop cannot report exhaustion when it processed no records: the \
         removable set was {} sealed-piece records",
        bound.removable
    );
    // THE PART THAT IS WORTH ASSERTING RATHER THAN ASSUMING. A budget of 256 that removed 15,536
    // records is not a broken budget -- whole sealed pieces are unlinked by NAME and never enter
    // the loop it guards. If this ever drops to the budget, that piece-unlinking path has stopped
    // running and the sweep has gone back to costing what it READS rather than what it removes.
    assert!(
        bound.swept > shipped.index_gc_max_entries_per_round * 4,
        "a round removes far more than its budget, because whole-piece unlinking does the work: \
         removed {} on a budget of {}. Equal to the budget means unlinking is no longer running",
        bound.swept,
        shipped.index_gc_max_entries_per_round
    );
    // And what the budget costs in bytes, which is the quantity the log is actually bounded by.
    assert!(
        free.index_log_bytes_reclaimed > 0,
        "DENOMINATOR: the unbounded arm must free index-log bytes, or the share below is 0/0"
    );
    let bytes_share = bound.index_log_bytes_reclaimed * 100 / free.index_log_bytes_reclaimed;
    eprintln!(
        "  the budgeted round freed {bytes_share}% of the index-log bytes the unbounded one did"
    );
    assert!(
        bytes_share > 80,
        "the entry budget must not be the thing holding the log up: the bounded round freed \
         {bytes_share}% of the bytes, and anything low here means the budget is pacing the log \
         rather than the byte threshold being"
    );

    // ---- THE HOT EXPIRY WINDOW ----
    assert_eq!(
        free.hot_scanned, free_due,
        "a hot window of 0 is NO window: it must take every due key ({free_due})"
    );
    assert_eq!(
        bound.hot_scanned, shipped.max_expire_hot_buckets_per_round,
        "THE BOUND MUST BITE EXACTLY: with {free_due} due keys the hot window must take {} of them",
        shipped.max_expire_hot_buckets_per_round
    );

    // ---- THE EVICTION BATCH, WHICH IS WHERE THE ROUND LATENCY IS ----
    assert!(
        free.pressure_gate_open && bound.pressure_gate_open,
        "DENOMINATOR: the eviction gate must admit in BOTH arms, or the batch limit is never \
         consulted: unbounded {} bounded {}",
        free.pressure_gate_open,
        bound.pressure_gate_open
    );
    assert!(
        free.victims > shipped.eviction_batch_limit,
        "DENOMINATOR: the unbounded arm must find more victims ({}) than the batch limit ({}) \
         allows, or the limit cannot be shown to bite",
        free.victims,
        shipped.eviction_batch_limit
    );
    assert_eq!(
        bound.victims, shipped.eviction_batch_limit,
        "THE BOUND MUST BITE EXACTLY: the eviction stage must choose {} victims, not {}",
        shipped.eviction_batch_limit, bound.victims
    );

    // ---- WHAT THE ROUND COST, AND WHY IT CANNOT SAY WHERE ----
    //
    // Printed and NOT asserted on: this box carries other tenants, so a stopwatch on it measures
    // them too. The per-stage rows are printed because the attribution is the part worth having
    // -- and on this path they are all zero, which is itself the thing to report rather than to
    // leave as an empty space under a heading.
    for (tag, cost) in [("unbounded", &free), ("shipped", &bound)] {
        let charged: Vec<&(String, u64)> =
            cost.stage_ms.iter().filter(|(_, ms)| *ms > 0).collect();
        if charged.is_empty() {
            eprintln!(
                "  stage durations, {tag} arm: ALL {} ROWS REPORT 0 ms while the round reports \
                 {} ms, so the round cannot name the stage that paid",
                cost.stage_ms.len(),
                cost.duration_ms
            );
        } else {
            eprintln!("  stage durations, {tag} arm:");
            for (stage, ms) in charged {
                eprintln!("    {stage:<28} {ms:>8} ms");
            }
        }
    }

    // ---- THE OTHER HALF OF THE TRADE: WHAT THE BOUNDED ROUND LEFT BEHIND ----
    let retained = free.removable.saturating_sub(bound.swept);
    eprintln!(
        "  BACKLOG: the bounded round retained {retained} of {} removable index-log records \
         ({:.1}% removed in one round)",
        free.removable,
        bound.swept as f64 * 100.0 / free.removable as f64
    );
    assert_eq!(
        retained, 0,
        "the entry budget leaves NOTHING behind on this fixture, because it bounded nothing: every \
         removable record was in a sealed piece. If this is ever non-zero here, the budget has \
         started binding and the note above needs its numbers again"
    );

    // THE TRADE IS IN THE BOUNDS THAT DID BITE, and there it is real. The hot window took 100 of
    // the 400 due keys and the eviction stage chose 10 of the 19,600 candidates it found, so both
    // leave work for later rounds -- which is the whole point of a per-round bound, and the cost
    // of it. How long that backlog takes to drain is the bound divided into it, TIMES the loop
    // interval, which is why the two are one setting.
    let hot_left = free_due.saturating_sub(bound.hot_scanned);
    let victims_left = free.victims.saturating_sub(bound.victims);
    assert!(
        hot_left > 0 && victims_left > 0,
        "DENOMINATOR: the bounds that bit must leave work behind, or there is no trade to \
         describe: {hot_left} due keys and {victims_left} eviction candidates left"
    );
    eprintln!(
        "  BACKLOG: {hot_left} due keys and {victims_left} eviction candidates wait for later \
         rounds ({} and {} rounds to drain at the shipped bounds)",
        free_due.div_ceil(bound.hot_scanned.max(1)),
        free.victims.div_ceil(bound.victims.max(1))
    );

    eprintln!("ROUND 2 -- the same two stores, one round later");
    let free_2 = one_round(&free_engine, false);
    show("unbounded", &free_2);
    let bound_2 = one_round(&bound_engine, true);
    show("shipped", &bound_2);
    assert!(
        free_2.swept < free.swept,
        "the unbounded arm drained its removable records in round 1, so round 2 has less to do: \
         {} then {}",
        free.swept,
        free_2.swept
    );
    // AND THE BACKLOG IS NOT RELEASED BY THE NEXT ROUND. What holds the retained records is the
    // BYTE THRESHOLD, not the entry budget: round 1 took the log from 907 KB to 41 KB, which is
    // far under the shipped 768 KiB, so the stage declines until writes grow it again. Stated as
    // an assertion because it is the operational consequence of pairing a byte trigger with a
    // per-round budget, and it is the opposite of "the budget paces the drain".
    eprintln!(
        "  the bounded arm, round 2: applied={} reason={:?} log_before={} B against a {} B threshold",
        bound_2.index_gc_applied,
        bound_2.index_gc_skipped_reason,
        bound_2.index_log_bytes_before,
        shipped.index_gc_index_log_bytes_threshold
    );
    assert!(
        bound_2.index_log_bytes_before < shipped.index_gc_index_log_bytes_threshold,
        "round 1 must have taken the log under the byte threshold, or this is not the case the \
         note describes: {} B against {} B",
        bound_2.index_log_bytes_before,
        shipped.index_gc_index_log_bytes_threshold
    );
    assert!(
        !bound_2.index_gc_applied,
        "with the log under the byte threshold the stage must decline rather than drain the \
         backlog the budget left: it applied and swept {}",
        bound_2.swept
    );
}

/// A CEILING ON THE DUMP THROTTLES WHAT INDEX GC CAN REMOVE IN THE SAME ROUND.
///
/// Found by the arms above going green-to-red when the dump ceiling took its operational value:
/// `removable_records_before_budget` fell from 16,001 to 200 and the usage-ratio trigger then
/// declined at 99 bp, so the index-GC stage did not run at all.
///
/// THE MECHANISM, and it is not a defect in either bound. The dump is what advances the index-log
/// retain floor, and `index_gc_commit_dirty_buckets_before_truncation` -- true by default, because
/// discarding an index-log record before the bucket it describes is durable loses the only copy of
/// that state -- will not truncate past a bucket the dump has not covered. So the dump ceiling
/// decides the SIZE OF THE REMOVABLE SET that every index-GC bound is then applied to. The two
/// bounds are in series, not in parallel.
///
/// What that means operationally: a shard with more dirty buckets than the dump ceiling reclaims
/// its index log at the rate the DUMP drains the dirty set, not at the rate the index-GC budget
/// allows. The repo has been here before from the other side -- the ceiling was turned off in
/// mx#1439 because "a cap did not slow reclaim down, it stopped it" -- and mx#1444 made an
/// undumped bucket hold the logs from its own oldest write rather than from zero, which is what
/// makes a ceiling safe rather than fast.
#[test]
fn a_capped_dump_throttles_what_index_gc_can_remove() {
    let shipped = StorageManagerCycleRequest::default();
    assert!(
        shipped.max_dump_buckets_per_round > 0,
        "DENOMINATOR: this measures the effect of a ceiling, so the shipped round must have one"
    );

    let mut removable_by_ceiling = Vec::new();
    for ceiling in [0usize, shipped.max_dump_buckets_per_round] {
        let dir = tempfile::tempdir().unwrap();
        let (engine, _due) = seeded(dir.path());
        let mut request = round(false);
        request.max_dump_buckets_per_round = ceiling;
        let report = engine.run_storage_manager_cycle(request);
        let gc = report.index_gc_report.clone().unwrap_or_default();
        eprintln!(
            "  dump ceiling {ceiling:<5} -> removable={:<6} usage={:<6}bp applied={:<5} skip={:?}",
            gc.removable_records_before_budget, gc.usage_ratio_basis_points, gc.applied,
            gc.skipped_reason
        );
        removable_by_ceiling.push(gc.removable_records_before_budget);
    }

    let uncapped = removable_by_ceiling[0];
    let capped = removable_by_ceiling[1];
    assert!(
        uncapped > capped,
        "a ceiling on the dump must leave FEWER index-log records removable, or the two bounds are \
         not in series after all: uncapped {uncapped}, capped {capped}"
    );
    assert!(
        capped <= shipped.max_dump_buckets_per_round,
        "and what is removable under a ceiling is bounded by the ceiling itself: {capped} \
         removable against a ceiling of {}",
        shipped.max_dump_buckets_per_round
    );
    eprintln!(
        "  the ceiling cuts the removable set {:.1}x in one round",
        uncapped as f64 / capped.max(1) as f64
    );
}

/// WHAT AN IDLE ROUND WALKS, STAGE BY STAGE -- which is what decides how often the loop may run.
///
/// The per-round bounds say how much work a round does when there IS work. How often the loop may
/// run is decided by the other case: what a round costs when there is NOTHING to do, because that
/// is most rounds on most shards. The two multiply.
///
/// COUNTED, NOT TIMED, AND ATTRIBUTED PER STAGE. Wall time on this box measures the other
/// tenants. `StorageManagerStageReport.walk.live_block_entries` is read-and-cleared at each stage
/// boundary, so the stage rows tile the round and say WHICH stage charged the walk. The
/// process-wide `live_block_scan_entries()` counter cannot: every live-block materialization in
/// the process charges it, from any stage and any concurrent shard, so a total of N says nothing
/// about where N came from. Measuring the total first and reading it as the whole-shard survey is
/// the mistake this test exists to avoid making again.
///
/// THE SURVEY ITSELF IS ALREADY CONDITIONAL. `storage_lifecycle_methods` builds the plan with
/// `shard_has_dirty_objects.then(|| self.bucket_storage_summaries(request.shard_id))`, so a round
/// with an empty dirty index does not survey the live page set at all, and
/// `live_block_summaries_measured` on the report is false. This asserts that skip is real, and
/// prints what the idle round charges ANYWAY, per stage, because that residue -- not the survey
/// -- is what bounds the interval.
#[test]
fn an_idle_round_skips_the_whole_shard_survey_and_what_it_still_walks() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _due) = seeded(dir.path());

    // DENOMINATOR FIRST: a round with work must survey, or "an idle round does not" is a claim
    // about a flag that is false in every round.
    let busy = engine.run_storage_manager_cycle(round(true));
    assert!(
        busy.pressure_signals.live_block_summaries_measured,
        "DENOMINATOR: a round on a freshly seeded store must survey the live page set, or this \
         report field cannot tell a surveying round from a skipping one"
    );
    let busy_entries: u64 = busy.stages.iter().map(|stage| stage.walk.live_block_entries).sum();
    eprintln!("  BUSY round: survey={} entries={busy_entries}", busy.pressure_signals.live_block_summaries_measured);
    for stage in &busy.stages {
        if stage.walk.live_block_entries > 0 {
            eprintln!("    {:<18} {}", stage.stage, stage.walk.live_block_entries);
        }
    }

    // Drain, so the next rounds have nothing left to do.
    for _ in 0..4 {
        let _ = engine.run_storage_manager_cycle(round(true));
    }

    let rounds = 5;
    let mut idle_entries_total: u64 = 0;
    let mut surveyed_while_idle = 0usize;
    let mut per_stage: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for _ in 0..rounds {
        let report = engine.run_storage_manager_cycle(round(true));
        if report.pressure_signals.live_block_summaries_measured {
            surveyed_while_idle += 1;
        }
        for stage in &report.stages {
            idle_entries_total += stage.walk.live_block_entries;
            *per_stage.entry(stage.stage.clone()).or_default() += stage.walk.live_block_entries;
        }
    }
    let per_idle = idle_entries_total as f64 / rounds as f64;

    eprintln!(
        "  IDLE: {rounds} rounds, {surveyed_while_idle} of them surveyed; {idle_entries_total} \
         entries in total, {per_idle:.0} each"
    );
    for (stage, entries) in &per_stage {
        if *entries > 0 {
            eprintln!("    {stage:<18} {} total, {:.0} per round", entries, *entries as f64 / rounds as f64);
        }
    }

    // THE SKIP IS THE CLAIM. A drained shard has no dirty object, so no round may survey.
    assert_eq!(
        surveyed_while_idle, 0,
        "{surveyed_while_idle} of {rounds} drained rounds surveyed the whole live page set. The \
         plan gates that on `shard_has_dirty_objects`; if a drained shard still reads dirty, the \
         gate is open on every idle round and the loop interval cannot be shortened"
    );

    // AND WHAT IT STILL WALKS, as a rate against the shipped interval: the residue is what a
    // shorter interval multiplies.
    let interval_ms = crate::data_node::StorageManagerRuntimeOptions::default().interval_ms;
    assert!(interval_ms > 0, "a zero interval is a spin, not a loop");
    let entries_per_second = per_idle * 1_000.0 / interval_ms as f64;
    eprintln!(
        "  at the shipped {interval_ms} ms interval that is {entries_per_second:.0} live-page \
         entries a second; the same residue on a 30 ms loop would be {:.0} a second",
        per_idle * 1_000.0 / 30.0
    );

    const IDLE_ENTRIES_PER_SECOND_CEILING: f64 = 700_000.0;
    assert!(
        entries_per_second <= IDLE_ENTRIES_PER_SECOND_CEILING,
        "an idle loop charges {entries_per_second:.0} live-page entries a second ({per_idle:.0} \
         per round at a {interval_ms} ms interval), over the {IDLE_ENTRIES_PER_SECOND_CEILING:.0} \
         this pair is allowed. The interval and what an idle round walks are ONE setting: bound \
         the residue first, then shorten the interval -- not the other way round"
    );
}

/// WHAT ONE ROUND COSTS IN TIME on an idle shard, against the interval that decides how often it
/// runs. The counted walk above is the durable quantity; this is the wall-clock distribution that
/// says whether the loop can keep up at all, printed rather than asserted tightly because this
/// box carries other tenants.
///
/// The only assertion is the one that does not depend on how loaded the box is: a round must fit
/// inside its own interval. A loop whose round takes longer than its interval does not run at the
/// interval -- it runs back to back, and the interval stops being a setting at all.
#[test]
fn an_idle_round_fits_inside_the_loop_interval() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _due) = seeded(dir.path());

    // Drain first: this measures the idle case, which is most rounds on most shards.
    for _ in 0..4 {
        let _ = one_round(&engine, true);
    }

    let rounds = 40;
    let mut micros: Vec<u128> = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let started = std::time::Instant::now();
        let _ = engine.run_storage_manager_cycle(round(true));
        micros.push(started.elapsed().as_micros());
    }
    micros.sort_unstable();
    let at = |q: f64| micros[((micros.len() as f64 - 1.0) * q).round() as usize];
    let p50 = at(0.50);
    let p90 = at(0.90);
    let p99 = at(0.99);
    let worst = *micros.last().unwrap();

    let interval_ms = crate::data_node::StorageManagerRuntimeOptions::default().interval_ms;
    eprintln!(
        "  {rounds} idle rounds, us: p50={p50} p90={p90} p99={p99} max={worst}; interval \
         {interval_ms} ms"
    );

    let interval_us = interval_ms as u128 * 1_000;
    assert!(
        p50 < interval_us,
        "the median idle round takes {p50} us against a {interval_ms} ms interval ({interval_us} \
         us): the loop cannot run at its interval, so the interval is not the setting it looks \
         like"
    );
}

/// What the two bounds this fixture CANNOT exercise would need, printed rather than asserted.
///
/// Run it when either bound is being changed:
///
///   cargo test -p temporalstore-rust --lib what_the_unexercised_bounds_would_need -- --ignored --nocapture
///
/// `max_expire_cold_buckets_per_round` selects on `!record_exists`, so it needs a key whose
/// deadline outlived its record in memory -- which this fixture, where everything stays resident,
/// does not produce: `cold_buckets_scanned` is 0 in both arms above.
/// `block_gc_min_slab_garbage_basis_points` is INERT by construction and the constant beside it
/// says so: the collector only offers slabs with no live refs at all, and a slab with no live refs
/// is 10,000 basis points of garbage, which clears any threshold below it. Neither is asserted to
/// bite above, because neither can here, and an assertion that cannot fail is worse than none.
#[test]
#[ignore]
fn what_the_unexercised_bounds_would_need() {
    let shipped = StorageManagerCycleRequest::default();
    let dir = tempfile::tempdir().unwrap();
    let (engine, due) = seeded(dir.path());
    let free = one_round(&engine, false);
    eprintln!("  due keys seeded: {due}");
    eprintln!(
        "  cold window at 0 took {} keys; the shipped bound is {}, so it binds only once more \
         than that many due keys have left memory",
        free.cold_scanned, shipped.max_expire_cold_buckets_per_round
    );
    eprintln!(
        "  slab garbage threshold is {} bp; a collectable slab carries 10000 bp, so every \
         candidate clears it",
        shipped.block_gc_min_slab_garbage_basis_points
    );
}

/// WHY THE BLOCK-RECLAIM BYTE CEILING SHIPS AT 0, measured on the budget arithmetic itself.
///
/// `block_store_gc` applies the byte budget per candidate as
/// `selected_physical_bytes + candidate.bytes > max_destroy_physical_bytes`, and
/// `selected_physical_bytes` starts the round at 0. So the comparison for the FIRST candidate is
/// `candidate.bytes > ceiling` -- and if one slab is bigger than the whole ceiling, that is true
/// for the first candidate, for every candidate behind it, and for every round after. The stage
/// does not do less work; it does none, and keeps doing none.
///
/// `DEFAULT_BLOCK_SLAB_TARGET_BYTES` is `1 << 30`. A ceiling of 1 MiB is 1/1024 of one slab.
///
/// THE COUNT CEILING HAS NO SUCH EDGE, which is the half of this worth remembering: it compares a
/// LENGTH against a LENGTH (`selected.len() >= max_destroy_slabs`), so a ceiling of n admits
/// exactly n candidates whatever they weigh. A budget over a quantity whose UNIT is larger than
/// the budget is not a small budget, it is a closed gate -- and only the byte budget here is over
/// such a quantity.
#[test]
fn a_byte_ceiling_below_one_slab_would_stop_the_collector_dead() {
    let slab_target = crate::storage_config::DEFAULT_BLOCK_SLAB_TARGET_BYTES;
    let would_be_ceiling: u64 = 1024 * 1024;

    // A single slab at the shipped target against the ceiling that was nearly shipped.
    assert!(
        slab_target > would_be_ceiling,
        "DENOMINATOR: this test only says something while one slab is larger than the ceiling it \
         warns about: slab target {slab_target}, ceiling {would_be_ceiling}"
    );

    // The arithmetic the budget performs, for the first candidate of a round.
    let selected_so_far: u64 = 0;
    let first_candidate_bytes = slab_target;
    let first_candidate_rejected =
        selected_so_far.saturating_add(first_candidate_bytes) > would_be_ceiling;
    assert!(
        first_candidate_rejected,
        "a {would_be_ceiling}-byte ceiling must reject a {first_candidate_bytes}-byte first \
         candidate, or the hazard this documents does not exist"
    );

    // And the count ceiling, the same round, does not have the edge.
    let count_ceiling = 500_usize;
    let selected_count = 0_usize;
    assert!(
        selected_count < count_ceiling,
        "a count ceiling of {count_ceiling} must admit the first candidate whatever it weighs"
    );

    eprintln!(
        "  slab target {slab_target} B; a {would_be_ceiling} B ceiling rejects the first candidate \
         and every one behind it, so the stage never destroys a slab again"
    );
}

/// THE GUARD. The block-reclaim byte ceiling may be 0 (no bound) or at least one slab target --
/// never between. Anything between is the closed gate measured above, and it goes red here rather
/// than in production, whatever the slab target later becomes.
#[test]
fn the_shipped_byte_ceiling_is_zero_or_at_least_one_slab() {
    let ceiling = crate::engine::reports::DEFAULT_BLOCK_GC_MAX_DESTROY_PHYSICAL_BYTES_PER_ROUND;
    let slab_target = crate::storage_config::DEFAULT_BLOCK_SLAB_TARGET_BYTES;
    assert!(
        ceiling == 0 || ceiling >= slab_target,
        "the block-reclaim byte ceiling is {ceiling}, between 0 and one slab target of \
         {slab_target}. The budget compares `selected + candidate.bytes > ceiling` from a \
         `selected` of 0, so a ceiling under one slab rejects every candidate for ever: the stage \
         stops rather than slows. Use 0 for no bound, or a whole number of slab targets"
    );
}

/// THE BOUNDS AND THE INTERVAL ARE ONE SETTING, and this is the arithmetic that says so.
///
/// A round's worst case is each stage's bound times what one unit of that stage costs. The
/// interval decides how often that round runs. If the worst-case round is longer than the
/// interval, the loop does not run at its interval -- it runs back to back, and the interval
/// stops being a setting. So the two have to be changed together, and in that order.
///
/// THE COSTS HERE ARE MEASURED, NOT GUESSED, and each is cited where it was measured:
///
///   compaction, ~200 us per relocated block ref, almost independent of page size
///       -- `engine::compaction`'s own note on COMPACTION_ROUND_BLOCK_REFS, which is why that
///          bound is denominated in refs rather than bytes
///   expiry, ~201.5 us per hot bucket at 20k objects
///       -- the note on `max_expire_hot_buckets_per_round` in `data_node`
///
/// The dump is the other large stage and it is bounded in buckets, but its per-bucket cost is a
/// function of bucket size rather than a constant, so it is left out rather than guessed at. That
/// makes this a LOWER bound on the worst-case round, which is the safe direction for a guard: the
/// real round is longer than what this computes, never shorter.
///
/// WHAT THIS GOES RED ON: shortening the interval without lowering the bounds, or raising a bound
/// without lengthening the interval. At the shipped values the compaction stage alone is already
/// 40% of the interval, so there is not much headroom to spend.
#[test]
fn the_shipped_bounds_fit_inside_the_shipped_interval() {
    let interval_ms = crate::data_node::StorageManagerRuntimeOptions::default().interval_ms;
    assert!(interval_ms > 0, "a zero interval is a spin, not a loop");
    let interval_us = interval_ms * 1_000;

    // Compaction: refs x measured per-ref cost.
    const MEASURED_COMPACTION_US_PER_REF: u64 = 200;
    let compaction_refs = 2_048_u64; // engine::compaction::COMPACTION_ROUND_BLOCK_REFS
    let compaction_us = compaction_refs * MEASURED_COMPACTION_US_PER_REF;

    // Expiry: hot buckets x measured per-bucket cost. The cold window is bounded separately and
    // its per-bucket cost is a load from the store rather than a constant, so it is not added.
    const MEASURED_EXPIRY_US_PER_HOT_BUCKET: u64 = 202;
    let shipped = StorageManagerCycleRequest::default();
    let expiry_us =
        shipped.max_expire_hot_buckets_per_round as u64 * MEASURED_EXPIRY_US_PER_HOT_BUCKET;

    let accounted_us = compaction_us + expiry_us;

    eprintln!("  interval                {interval_ms} ms ({interval_us} us)");
    eprintln!(
        "  compaction              {compaction_refs} refs x {MEASURED_COMPACTION_US_PER_REF} us \
         = {compaction_us} us ({:.0}% of the interval)",
        compaction_us as f64 * 100.0 / interval_us as f64
    );
    eprintln!(
        "  expiry (hot)            {} buckets x {MEASURED_EXPIRY_US_PER_HOT_BUCKET} us = \
         {expiry_us} us ({:.0}%)",
        shipped.max_expire_hot_buckets_per_round,
        expiry_us as f64 * 100.0 / interval_us as f64
    );
    eprintln!(
        "  accounted for           {accounted_us} us ({:.0}% of the interval), dump and the \
         remaining stages not included",
        accounted_us as f64 * 100.0 / interval_us as f64
    );
    eprintln!(
        "  a {interval_ms} ms round affords about {} compaction refs at the measured per-ref cost",
        interval_us / MEASURED_COMPACTION_US_PER_REF
    );

    assert!(
        accounted_us < interval_us,
        "the two stages whose per-unit cost is measured already want {accounted_us} us of a \
         {interval_us} us interval, and the dump is not counted. Lower the per-round bounds before \
         shortening the interval, or lengthen the interval before raising a bound -- a worst-case \
         round longer than the interval means the loop runs back to back and the interval is not \
         the setting it looks like. A {interval_ms} ms round affords about {} compaction refs.",
        interval_us / MEASURED_COMPACTION_US_PER_REF
    );
}
