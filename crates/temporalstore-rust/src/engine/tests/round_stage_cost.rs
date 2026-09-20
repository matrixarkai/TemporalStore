// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What ONE maintenance round costs, STAGE BY STAGE, on a large store.
//!
//! The round is what runs continuously: prepare, reclaim the log, expire, evict, reclaim pages,
//! collect the index log, compact, reap metrics. Its stages have each been measured before, at
//! small sizes. The question here is the one only a large store answers -- as the store grows,
//! does one round cost what the STORE is, or what the round has to DO? -- and it is asked of each
//! stage separately, because a total hides the element: one correctly bounded stage holds the sum
//! down while three beside it are not.
//!
//! THE ANSWER IS THE STORE. Counted, not timed; this box is shared and a stopwatch on it measures
//! the other tenants. Two sizes five times apart, both above the ~100,000-record line below which
//! every whole-store cost this campaign has found was invisible, on a four-slab store:
//!
//! ```text
//!   ALL RECORDS PENDING -- every record rewritten before the round
//!   stage                      20,000       100,000    ratio   per record   verdict
//!   plan                       40,000       200,000    5.000x    2.000      GROWING
//!   prepare                         0             0       --     0.000      FLAT
//!   reclaim_wal                99,360       499,360    5.026x    4.968      GROWING
//!   expire                          0             0       --     0.000      FLAT, bounded
//!   evict                           0             0       --     0.000      FLAT, bounded
//!   reclaim_page                    0             0       --     0.000      FLAT, not exercised
//!   index_gc                        0             0       --     0.000      FLAT, not exercised
//!   merged_dump_load_policy   119,232       599,232    5.026x    5.962      GROWING
//!   compact                   119,232       599,232    5.026x    5.962      GROWING
//!   reap_metrics                    0             0       --     0.000      FLAT
//!   ROUND                     377,824     1,897,824    5.023x   18.891      GROWING
//! ```
//!
//! Four of the ten stages walk the whole live page set, each a fixed number of times per round --
//! 2, 5, 6 and 6, summing to 19 -- and the per-record figure moves 18.891 -> 18.978 across a store
//! five times larger, which is 0.5%. Six stages are flat at zero. Two of those six are flat
//! because they are BOUNDED per cycle, one is flat because it does no walking, and TWO ARE NOT
//! MEASURED AT ALL: `reclaim_page` and `index_gc` found no work to apply in this fixture, and
//! their zeros are therefore unmeasured rather than measured. Said here rather than left to be
//! read as cheap stages, which is exactly what an unexercised stage looks like.
//!
//! The cheaper bucket-index walk tells the same story and only `compact` makes it: 79,488 ->
//! 399,488 entries, 5.026x, 3.974 -> 3.995 per record.
//!
//! WHAT IT COSTS TO DO ALMOST NOTHING. The second regime holds the pending work FIXED -- 2,000
//! fresh records whatever the store holds -- which is what a shard mostly does. Per record of
//! STORE the round costs the same as ever; per record of WORK it costs linearly more:
//!
//! ```text
//!   2,000 RECORDS PENDING, eight-slab store
//!                             store 22,000   store 102,000   ratio
//!   plan                            43,744         203,744   4.657x
//!   reclaim_wal                    152,200         712,200   4.680x
//!   merged_dump_load_policy        260,904       1,220,904   4.680x
//!   compact                        152,204         712,204   4.680x
//!   ROUND                          609,052       2,849,052   4.678x
//!   per record of STORE             27.684          27.932   1.009x   FLAT
//!   per record of WORK RETIRED     304.526        1424.526   4.678x   LINEAR IN THE STORE
//! ```
//!
//! The store grew 4.636x and the cost of retiring one record grew 4.678x with it. That is the
//! answer to the question this file exists for, in one line: A ROUND IS NOT PAID FOR BY THE WORK
//! IN FRONT OF IT. The two regimes are NOT directly comparable stage by stage -- the eight-slab
//! store walks more per record than the four-slab one, and slab count is a second term this
//! fixture does not hold still across regimes -- but the scaling WITHIN each regime is measured at
//! a constant slab count and is what the verdicts above rest on.
//!
//! SUM THE INTEGRAL. A per-round cost that is a CONSTANT MULTIPLE of the store is not a flat cost.
//! The round fires on a timer while the store grows under it, so the multiple applies to a subject
//! the round itself watches grow. Building a store to N records with one round per `k` records
//! written costs `19 * k * (N/k)(N/k + 1)/2` live-page entries, which is about `19 N^2 / 2k`. At
//! N = 1,000,000 and a round every 10,000 records that is 9.5e11 entries to build the store once
//! -- quadratic in N, while every individual round reports a perfectly steady 18.9x.
//!
//! IS ANY STAGE BOUNDED PER CYCLE? Three are, by name: `expire` at its hot and cold per-round
//! bucket bounds (128 and 8), `evict` at `eviction_batch_limit`, and `index_gc` at
//! `index_gc_max_entries_per_round` (256). `every_maintenance_round_is_bounded_by_default` already
//! asserts those are set and non-zero, and this measurement confirms they hold: expire takes
//! exactly 128 buckets and evict exactly its batch at BOTH sizes. What they bound is what each
//! stage CHANGES. None of them bounds what it READS, and the four stages in the table above --
//! `plan`, `reclaim_wal`, `merged_dump_load_policy`, `compact` -- have no per-cycle bound of
//! either kind: each does all the work it finds. One bound does live inside
//! `merged_dump_load_policy`, `RECOVERY_READABLE_PROBE_PER_ROUND` at 512 pages, and it is why that
//! stage reads only 512 pages while still walking the index six times.
//!
//! NOTHING SHEDS OR THROTTLES. No stage returns "I stopped short, come back sooner"; the three
//! bounded ones carry a cursor to the next round and say nothing else, and the four unbounded ones
//! have nothing to say. Reported because it was asked, including the part that is "nothing".
#![allow(clippy::all)]
use super::*;
use crate::engine::reports::{
    StageWalkCharges, StorageManagerCycleReport, StorageManagerCycleRequest,
};

/// The two corpus sizes, five times apart.
const SMALL: usize = 20_000;
const LARGE: usize = 100_000;

/// Records written before the round in the fixed-pending regime, at BOTH sizes. The knob of this
/// whole measurement: how much work is waiting, relative to the store.
const FIXED_PENDING: usize = 2_000;

/// Whole-store walks each stage makes per round with every record pending. Named individually, so
/// a failure says WHICH stage moved rather than that a total did.
const WALKS_PER_STAGE: &[(&str, u64)] = &[
    ("plan", 2),
    ("prepare", 0),
    ("reclaim_wal", 5),
    ("expire", 0),
    ("evict", 0),
    ("reclaim_page", 0),
    ("index_gc", 0),
    ("merged_dump_load_policy", 6),
    ("compact", 6),
    ("reap_metrics", 0),
];

/// The stages whose zero above is a BOUND doing its job, not an idle stage. Asserted to have done
/// work by `every_stage_this_round_measures_had_work_to_do`.
const BOUNDED_STAGES: &[&str] = &["expire", "evict"];

/// The stages whose zero above is NOT MEASURED: this fixture never gave them work to apply. Named
/// here so the table above cannot be read as saying they are cheap.
const UNEXERCISED_STAGES: &[&str] = &["reclaim_page", "index_gc"];

/// The stages with no per-cycle bound, which walk the whole store every round.
const UNBOUNDED_WALKING_STAGES: &[&str] =
    &["plan", "reclaim_wal", "merged_dump_load_policy", "compact"];

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

/// Roll the active data slab, so the store is more than one slab and page reclaim and compaction
/// have a choice to make.
///
/// The block store's own roll rather than a prepare-only round: a round surveys the whole shard on
/// the way past, and at 100,000 records a fixture built out of rounds costs four times what the
/// measurement does.
fn roll_slab(engine: &TemporalEngine) {
    let before = engine.block_store.slab_ids().unwrap_or_default().len();
    engine.block_store.roll_slab().expect("roll a data slab");
    let after = engine.block_store.slab_ids().unwrap_or_default().len();
    assert!(
        after > before,
        "the fixture asked for a slab roll and got none ({before} -> {after}), so the store is one \
         slab and compaction and page reclaim have no choice to make"
    );
}

/// Write `count` records from `from`, one in ten with a deadline so the expire sweep has something
/// to remove, rolling the data slab four times on the way so the store is not one slab.
fn seed(engine: &TemporalEngine, from: usize, count: usize) {
    let mut index = from;
    let to = from + count;
    let roll_every = (count / 4).max(1);
    let mut since_roll = 0usize;
    while index < to {
        let end = (index + 500).min(to);
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            if cursor % 10 == 0 {
                commands.push(Command::StringSetEx {
                    key: format!("t-{cursor:08}"),
                    value: vec![b'v'; 128],
                    ttl_ms: 1,
                });
            } else {
                commands.push(Command::StringSet {
                    key: format!("k-{cursor:08}"),
                    value: vec![b'v'; 128],
                });
            }
            cursor += 1;
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
            roll_slab(engine);
        }
    }
}

/// The round the periodic driver runs, with every stage on.
///
/// `enable_evict` is ON here where the older round measurements left it off. A stage switched off
/// reports a cost of zero that reads exactly like a cheap stage, and the question asked in this
/// file is what the WHOLE round costs.
fn round_request() -> StorageManagerCycleRequest {
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
        min_undumped_wal_records: 1_000,
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

/// One round, with the process-wide instruments read once around the whole call.
///
/// These totals come from counters THE STAGE ROWS DO NOT FEED. The rows accumulate in their own
/// atomics and are cleared at every stage boundary; these are separate counters read once around
/// the call. Subtracting one from the other is therefore a reading and not an identity, which is
/// the only reason the residual below means anything.
struct RoundCost {
    report: StorageManagerCycleReport,
    whole_call: StageWalkCharges,
    store_records: usize,
    pending: usize,
    slabs: usize,
    path_len: usize,
}

fn one_round(engine: &TemporalEngine) -> (StorageManagerCycleReport, StageWalkCharges) {
    crate::engine::reset_live_block_scan_entries();
    crate::engine::reset_bucket_block_index_visits();
    crate::engine::reset_index_encode_counts();
    let report = engine.run_storage_manager_cycle(round_request());
    let encodes = crate::engine::index_encode_counts();
    (
        report,
        StageWalkCharges {
            live_block_entries: crate::engine::live_block_scan_entries(),
            bucket_block_index_visits: crate::engine::bucket_block_index_visits(),
            index_encodes: encodes.encodes_total,
            index_encode_bytes: encodes.encode_bytes_total,
        },
    )
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

    fn entries(&self, name: &str) -> u64 {
        self.stage(name).walk.live_block_entries
    }

    fn total_entries(&self) -> u64 {
        self.report
            .stages
            .iter()
            .map(|stage| stage.walk.live_block_entries)
            .sum()
    }

    fn visits(&self, name: &str) -> u64 {
        self.stage(name).walk.bucket_block_index_visits
    }
}

/// A round measured on a freshly built store, with `pending` records written immediately before it
/// so the round has work waiting.
///
/// The FIRST round is the warm-up and the SECOND is the one reported: a first round on a store
/// that has never been dumped does a different amount of work from every round after it, and the
/// steady round is the one that runs forever.
fn measure(records: usize, pending: usize) -> RoundCost {
    let dir = tempfile::tempdir().expect("tempdir");
    let path_len = dir.path().as_os_str().len();
    let engine = round_engine(dir.path());
    seed(&engine, 0, records);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let _warmup = one_round(&engine);
    let store_records = if pending >= records {
        seed(&engine, 0, records);
        records
    } else {
        seed(&engine, records, pending);
        records + pending
    };
    std::thread::sleep(std::time::Duration::from_millis(30));
    let (report, whole_call) = one_round(&engine);
    assert!(
        report.errors.is_empty(),
        "{records} records / {pending} pending: the round errored, so every figure it reports \
         measures nothing: {:?}",
        report.errors
    );
    // FLOOR THE INSTRUMENT. A counter that counted nothing reads exactly like a measurement of
    // zero, and the whole table below would be the first of those wearing the second's clothes.
    assert!(
        whole_call.live_block_entries > 0,
        "{records} records / {pending} pending: the round materialised no live-page entry at all"
    );
    let slabs = report.plan.live_block_slab_ids.len();
    RoundCost {
        report,
        whole_call,
        store_records,
        pending,
        slabs,
        path_len,
    }
}

/// The four measurements this file rests on, taken once each.
///
/// Shared rather than repeated: the 100,000-record arms take about two minutes apiece, and four
/// tests each building their own would spend eight minutes rebuilding stores that are identical by
/// construction.
fn cost(records: usize, pending: usize) -> &'static RoundCost {
    use std::sync::OnceLock;
    static ALL_SMALL: OnceLock<RoundCost> = OnceLock::new();
    static ALL_LARGE: OnceLock<RoundCost> = OnceLock::new();
    static FIXED_SMALL: OnceLock<RoundCost> = OnceLock::new();
    static FIXED_LARGE: OnceLock<RoundCost> = OnceLock::new();
    match (records, pending >= records) {
        (SMALL, true) => ALL_SMALL.get_or_init(|| measure(SMALL, SMALL)),
        (LARGE, true) => ALL_LARGE.get_or_init(|| measure(LARGE, LARGE)),
        (SMALL, false) => FIXED_SMALL.get_or_init(|| measure(SMALL, FIXED_PENDING)),
        (LARGE, false) => FIXED_LARGE.get_or_init(|| measure(LARGE, FIXED_PENDING)),
        _ => panic!("no measurement is taken for {records} records / {pending} pending"),
    }
}

/// The store path length is held CONSTANT across arms and stated, because a path's length moves
/// allocation counts and has masqueraded as a real difference on this tree before.
fn assert_paths_comparable(small: &RoundCost, large: &RoundCost) {
    assert_eq!(
        small.path_len, large.path_len,
        "the two arms were measured under store paths of different length ({} vs {}); path length \
         moves allocation counts and would sit inside the ratio as if it were a corpus effect",
        small.path_len, large.path_len
    );
    println!("  store path length held at {} bytes in both arms", small.path_len);
}

fn print_table(tag: &str, small: &RoundCost, large: &RoundCost) {
    println!(
        "  {tag}: store {} -> {} records, {} -> {} slabs",
        small.store_records, large.store_records, small.slabs, large.slabs
    );
    println!(
        "  {:<26} {:>12} {:>12} {:>9} {:>10} {:>10}  verdict",
        "stage", "small", "large", "ratio", "/rec small", "/rec large"
    );
    for (name, _) in WALKS_PER_STAGE {
        let s = small.entries(name);
        let l = large.entries(name);
        let verdict = if s == 0 && l == 0 {
            if UNEXERCISED_STAGES.contains(name) {
                "FLAT, not exercised"
            } else if BOUNDED_STAGES.contains(name) {
                "FLAT, bounded"
            } else {
                "FLAT"
            }
        } else {
            "GROWING"
        };
        println!(
            "  {:<26} {:>12} {:>12} {:>9} {:>10.3} {:>10.3}  {verdict}",
            name,
            s,
            l,
            if s == 0 {
                "--".to_string()
            } else {
                format!("{:.3}x", l as f64 / s as f64)
            },
            s as f64 / small.store_records as f64,
            l as f64 / large.store_records as f64,
        );
    }
    println!(
        "  {:<26} {:>12} {:>12} {:>8.3}x {:>10.3} {:>10.3}  ROUND",
        "TOTAL",
        small.total_entries(),
        large.total_entries(),
        large.total_entries() as f64 / small.total_entries() as f64,
        small.total_entries() as f64 / small.store_records as f64,
        large.total_entries() as f64 / large.store_records as f64,
    );
}

#[test]
fn a_round_costs_the_whole_store_stage_by_stage() {
    let small = cost(SMALL, SMALL);
    let large = cost(LARGE, LARGE);
    assert_paths_comparable(small, large);
    print_table("ALL RECORDS PENDING", small, large);

    // THE CONTROL FOR THIS ARM. Its knob is that the pending work GROWS with the store. If it did
    // not, the two rows are one measurement taken twice and every ratio below is an artefact.
    assert_eq!(
        small.stage("reclaim_wal").candidate_count * (LARGE / SMALL),
        large.stage("reclaim_wal").candidate_count,
        "ALL-PENDING control: the pending work did not grow with the store -- {} undumped records \
         at {SMALL} and {} at {LARGE}, where growing with the store means {}. This arm exists to \
         differ from the fixed-pending arm by exactly this, and without it the two arms are the \
         same measurement",
        small.stage("reclaim_wal").candidate_count,
        large.stage("reclaim_wal").candidate_count,
        small.stage("reclaim_wal").candidate_count * (LARGE / SMALL),
    );
    assert_eq!(
        small.slabs, large.slabs,
        "the two arms hold {} and {} slabs. Slab count is a second term in what the round walks, \
         so a ratio taken across different slab counts is not a corpus ratio",
        small.slabs, large.slabs,
    );

    // EVERY STAGE, NAMED, AT BOTH SIZES. Not a total: a total lets one bounded stage hold down
    // three unbounded ones, which is the shape this campaign keeps finding.
    for (name, walks) in WALKS_PER_STAGE {
        let expected_small = walks * SMALL as u64;
        let expected_large = walks * LARGE as u64;
        // The whole-store passes are exact multiples of the store; the two stages that walk the
        // index carry a fixed overhead of a few hundred entries on top, so the per-record figure
        // is compared rather than the raw count.
        let small_per_record = small.entries(name) as f64 / small.store_records as f64;
        let large_per_record = large.entries(name) as f64 / large.store_records as f64;
        assert!(
            (small_per_record - *walks as f64).abs() < 0.1,
            "stage {name} walked {small_per_record:.3} times the store at {SMALL} records, not \
             {walks}. A stage that changed its walk count is the finding here, not a number to \
             update ({} entries, expected about {expected_small})",
            small.entries(name),
        );
        assert!(
            (large_per_record - *walks as f64).abs() < 0.1,
            "stage {name} walked {large_per_record:.3} times the store at {LARGE} records against \
             {small_per_record:.3} at {SMALL}. Differing BETWEEN the sizes means this stage \
             stopped being proportional to the store and started being proportional to something \
             else, which is the failure worth a page ({} entries, expected about {expected_large})",
            large.entries(name),
        );
    }

    // The cheaper bucket-index walk, separately, because a stage can be flat in one and growing in
    // the other and an aggregate of the two would show neither.
    let compact_visits_small = small.visits("compact") as f64 / small.store_records as f64;
    let compact_visits_large = large.visits("compact") as f64 / large.store_records as f64;
    println!(
        "  compact bucket-index visits: {:.3} -> {:.3} per record",
        compact_visits_small, compact_visits_large
    );
    assert!(
        compact_visits_small > 3.0 && (compact_visits_small - compact_visits_large).abs() < 0.1,
        "the compact stage's bucket-index walk moved from {compact_visits_small:.3} to \
         {compact_visits_large:.3} entries per record across a store five times larger"
    );

    // THE WALK RESIDUAL, at both sizes. The rows are accumulated in their own atomics and cleared
    // at every stage boundary; this total is a different counter read once around the whole call,
    // so the difference is a reading rather than an identity. Zero means no whole-store walk was
    // made outside a stage; the planted-encode test beside this one is what says a nonzero one
    // would have been seen.
    for (label, measured) in [("small", small), ("large", large)] {
        let rows = measured.total_entries();
        let seen = measured.whole_call.live_block_entries;
        println!(
            "  {label}: whole call walked {seen} entries, stage rows claim {rows}, residual {}",
            seen as i64 - rows as i64
        );
        assert!(
            seen >= rows,
            "the stage rows claim {rows} live-page entries at the {label} size and the \
             independent counter saw {seen}. Rows exceeding it would mean the round walked on \
             another thread, which this measurement is not built to read"
        );
        assert_eq!(
            seen, rows,
            "{} live-page entries were walked inside the round and charged to no stage at the \
             {label} size. That is a whole-store cost with no stage to its name: give it a stage, \
             or record here which part of the round makes it",
            seen - rows,
        );
    }

    let per_record: u64 = WALKS_PER_STAGE.iter().map(|(_, walks)| walks).sum();
    let round_per_record = large.total_entries() as f64 / large.store_records as f64;
    assert!(
        (round_per_record - per_record as f64).abs() < 0.2,
        "one round walked the live page set {round_per_record:.3} times at {LARGE} records, not \
         the {per_record} its stages sum to"
    );
}

#[test]
fn a_round_with_a_fixed_amount_of_work_waiting_still_costs_the_whole_store() {
    let small = cost(SMALL, FIXED_PENDING);
    let large = cost(LARGE, FIXED_PENDING);
    assert_paths_comparable(small, large);
    print_table("FIXED PENDING WORK", small, large);

    // THE CONTROL FOR THIS ARM, in two halves. The first is its premise: the work waiting is the
    // same at both sizes.
    assert_eq!(
        small.pending, large.pending,
        "FIXED-PENDING control: the arms wrote {} and {} records before their rounds. Held fixed \
         is the entire difference between this arm and the all-pending one; without it this IS \
         the all-pending arm and the comparison below is between a measurement and itself",
        small.pending, large.pending,
    );
    // The second half is the one with teeth, because the premise above is true by construction
    // and a construction can be wrong. The two regimes must be DIFFERENT MEASUREMENTS: a fixture
    // that quietly rewrote the whole store here would land on the all-pending arm's figure, and
    // every conclusion drawn from the pair would be drawn from one arm measured twice.
    let all_pending_per_store = cost(LARGE, LARGE).total_entries() as f64 / LARGE as f64;
    let this_per_store = large.total_entries() as f64 / large.store_records as f64;
    assert!(
        this_per_store - all_pending_per_store > 5.0,
        "FIXED-PENDING control: this arm cost {this_per_store:.3} passes over the store and the \
         all-pending arm cost {all_pending_per_store:.3}. Two regimes that land on the same figure \
         are one regime, and the fixture stopped distinguishing them"
    );

    // THE ROUND CANNOT SEE HOW MUCH WORK IS WAITING, which is the mechanism behind everything
    // below: after {FIXED_PENDING} fresh records the round still reports the WHOLE store dirty.
    assert_eq!(
        large.stage("reclaim_wal").dirty_bucket_count,
        large.store_records,
        "the round reported {} dirty buckets on a {}-record store after only {FIXED_PENDING} \
         records were written. If that ever stops being the whole store, the round has gained a \
         way to tell a small amount of pending work from a large one",
        large.stage("reclaim_wal").dirty_bucket_count,
        large.store_records,
    );
    assert_eq!(
        small.slabs, large.slabs,
        "the two fixed-pending arms hold {} and {} slabs, so the ratio below is not a corpus ratio",
        small.slabs, large.slabs,
    );

    // PER RECORD OF STORE: flat. The round costs what the store is.
    let per_store_small = small.total_entries() as f64 / small.store_records as f64;
    let per_store_large = large.total_entries() as f64 / large.store_records as f64;
    assert!(
        (per_store_small - per_store_large).abs() < 0.5,
        "with the work held fixed the round cost {per_store_small:.3} passes over the store at {} \
         records and {per_store_large:.3} at {}. A round whose cost tracked the WORK would fall as \
         the store grew under an unchanged workload; one that tracks the STORE holds steady, and \
         holding steady is what is being asserted",
        small.store_records,
        large.store_records,
    );

    // PER RECORD OF WORK: linear in the store. This is the sentence the two regimes exist to let
    // us say, and the number an operator actually pays.
    let per_work_small = small.total_entries() as f64 / small.pending as f64;
    let per_work_large = large.total_entries() as f64 / large.pending as f64;
    let store_growth = large.store_records as f64 / small.store_records as f64;
    let work_growth = per_work_large / per_work_small;
    println!(
        "  per record of WORK retired: {per_work_small:.3} -> {per_work_large:.3} ({work_growth:.3}x) \
         while the store grew {store_growth:.3}x"
    );
    assert!(
        (work_growth - store_growth).abs() < 0.3,
        "retiring one record cost {per_work_small:.3} live-page entries on a {}-record store and \
         {per_work_large:.3} on a {}-record one: {work_growth:.3}x against a store {store_growth:.3}x \
         larger. Those two tracking each other is the finding -- the price of doing a fixed amount \
         of maintenance rises linearly with everything the shard is holding",
        small.store_records,
        large.store_records,
    );
}

/// Which stages are bounded per cycle, and which simply do all the work they find.
///
/// Named individually, because the useful answer is not how many but WHICH. Note what the bounds
/// bound: `every_maintenance_round_is_bounded_by_default` asserts these are set and non-zero, and
/// they are -- they bound what a stage CHANGES. Nothing bounds what it READS.
#[test]
fn three_round_stages_are_bounded_per_cycle_and_four_walk_the_whole_store() {
    let small = cost(SMALL, SMALL);
    let large = cost(LARGE, LARGE);

    for name in BOUNDED_STAGES {
        let taken = small.stage(name).candidate_count;
        let grown = large.stage(name).candidate_count;
        assert!(
            taken > 0,
            "stage {name} is named here as bounded, but it took no candidate at all at {SMALL} \
             records, so this proves nothing about a bound"
        );
        assert_eq!(
            taken, grown,
            "stage {name} took {taken} candidates at {SMALL} records and {grown} at {LARGE}. A \
             bound that moves with the store is not a bound"
        );
        println!("  {name}: bounded at {taken} candidates at BOTH sizes");
    }
    assert_eq!(
        small.stage("expire").candidate_count,
        crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
        "expire swept {} buckets rather than the {} its per-round bound allows. A stage that did \
         not REACH its bound has a cost that is not the bound's, and calling it bounded on that \
         evidence is unproven",
        small.stage("expire").candidate_count,
        crate::engine::reports::DEFAULT_MAX_EXPIRE_HOT_BUCKETS_PER_ROUND,
    );
    assert!(
        crate::engine::reports::DEFAULT_INDEX_GC_MAX_ENTRIES_PER_ROUND > 0,
        "index_gc's per-round entry bound is zero, which this request's own documentation says \
         means NO bound; the stage would then be unbounded and this test's name would be wrong"
    );

    for name in UNBOUNDED_WALKING_STAGES {
        let s = small.entries(name);
        let l = large.entries(name);
        assert!(s > 0, "stage {name} walked nothing at {SMALL} records");
        let growth = l as f64 / s as f64;
        assert!(
            (growth - (LARGE / SMALL) as f64).abs() < 0.1,
            "stage {name} is named here as having no per-cycle bound: it walked {s} entries at \
             {SMALL} records and {l} at {LARGE}, a growth of {growth:.3}x where an unbounded stage \
             grows {}x. If this stage has GAINED a bound, that is good news this test should be \
             rewritten to record rather than relaxed to accommodate",
            LARGE / SMALL,
        );
        println!("  {name}: unbounded, {growth:.3}x for a {}x store", LARGE / SMALL);
    }
}

/// A stage with nothing to do reports a cost of zero that reads exactly like a cheap stage.
///
/// Six of the ten rows in the table are zero. This is what separates the ones that are bounded
/// stages having DONE their work from the ones this fixture never gave any.
#[test]
fn every_stage_this_round_measures_had_work_to_do() {
    let measured = cost(SMALL, SMALL);

    assert!(
        measured.slabs > 1,
        "the store is {} slab(s), so compaction and page reclaim have no choice to make and their \
         figures measure the fixture rather than the round",
        measured.slabs
    );
    assert!(
        measured.store_records >= SMALL,
        "the measured store holds {} records, fewer than the {SMALL} written",
        measured.store_records
    );

    let expire = measured.stage("expire");
    assert!(
        expire.expired_records_removed > 0,
        "the expire stage removed no record, so its zero is a stage that was idle rather than a \
         stage that is bounded"
    );
    assert!(
        measured.stage("evict").candidate_count > 0,
        "the evict stage selected no victim, so its zero is an idle stage"
    );
    assert!(
        measured.stage("prepare").metrics_bucket_count > 0,
        "the prepare stage saw no routing bucket at all"
    );
    assert!(
        measured.stage("compact").blocks_compacted > 0,
        "the compact stage compacted nothing, so the six whole-store passes it is charged were \
         made for no work and the fixture never gave it any"
    );
    assert!(
        measured.stage("reclaim_wal").candidate_count > 0,
        "the reclaim_wal stage had no undumped record waiting, so this is the idle-shard round \
         and not the steady one the table describes"
    );
    assert!(
        measured.stage("reap_metrics").metrics_bucket_count > 0,
        "the reap_metrics stage reported metrics for no bucket, which is the unmeasured zero its \
         own reason string warns about"
    );
    assert!(
        measured.stage("plan").walk.live_block_entries > 0,
        "the plan stage walked nothing, so it did not survey the shard and every figure derived \
         from its summaries below is an unmeasured zero"
    );

    // THE STAGE SPANS TILE THE ROUND. Each is closed where its work ends and the next begins, and
    // the round's own duration runs from before the first to after the last -- so the rows can sum
    // to less than the round (each is truncated to whole milliseconds) and never to more.
    //
    // A sum that EXCEEDS the round is one interval charged to two stages, which is exactly the
    // defect this file was written around: the stage rows are not built in the order the stages
    // run, and a span closed at a ROW rather than at its WORK gives four stages' cost to a fifth.
    // The walk counters cannot see that -- the stages it moved between walk nothing -- so this is
    // the assertion that holds the boundaries in place.
    let stage_sum: u64 = measured
        .report
        .stages
        .iter()
        .map(|stage| stage.duration_ms)
        .sum();
    println!(
        "  stage durations sum to {stage_sum} ms of a round that took {} ms",
        measured.report.duration_ms
    );
    assert!(
        stage_sum <= measured.report.duration_ms,
        "the stage durations sum to {stage_sum} ms on a round that took {} ms. They tile the \
         round, so their sum cannot exceed it: a sum that does means one interval is charged to \
         two stages, and the round's own breakdown -- which is what an operator reads to decide \
         which stage to bound -- is naming the wrong one",
        measured.report.duration_ms,
    );

    // AND WHAT WAS NOT EXERCISED, said out loud. These two stages found nothing to apply in this
    // fixture, so their zeros in the table are NOT MEASUREMENTS. If a later fixture change gives
    // them work, this assertion fails and the table's verdict for them has to be re-read -- which
    // is the right failure, because a stage that starts doing work starts having a cost.
    for name in UNEXERCISED_STAGES {
        let stage = measured.stage(name);
        assert!(
            !stage.applied,
            "stage {name} is documented in this file as NOT EXERCISED -- its zero in the table is \
             an absent measurement rather than a cheap stage -- but it applied work in this run. \
             Re-measure it and give it a verdict of its own"
        );
        println!("  {name}: NOT EXERCISED, its zero is not a measurement");
    }
}

/// What a round does that no stage row accounts for, read with an instrument that is neither a
/// stage row nor a sum of stage rows.
///
/// WHY THIS IS NOT A WALK COUNTER. A round can encode the ENTIRE served index -- its own tail does
/// exactly that once the undumped log has crossed a threshold, after the last stage row is built.
/// That cost is the whole store, and it materialises no live-page entry and visits no bucket
/// index, so BOTH walk counters read zero across it: a walk-based residual reports a whole-store
/// cost as nothing at all. `index_encode_counts` is charged inside `serialize_index`, lives in
/// another module as a thread-local tally, and no stage row feeds it.
///
/// WHY THE SPAN IS WIDER THAN THE ROUND. Measured across the round alone this residual reads ZERO
/// on this fixture, and a zero is the one reading that cannot be told from an identity -- the
/// stage rows would look exactly like slices of the counter they are being subtracted from. So the
/// span is moved to start BEFORE the round, where a real whole-store quantity is placed inside it:
/// the stage accumulators are cleared when a round starts, so an encode made before the round is
/// seen by the instrument and by no stage row, which is what an unattributed cost IS. Two rounds
/// are run, identical but for that planted encode, and the difference between their residuals has
/// to be the planted encode to the byte.
#[test]
fn the_residual_outside_the_round_stage_rows_recovers_a_planted_whole_store_encode() {
    const RECORDS: usize = 8_000;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = round_engine(dir.path());
    seed(&engine, 0, RECORDS);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let _warmup = one_round(&engine);

    /// One round's encode residual: what the independent instrument saw, minus what the stage rows
    /// claim. Never the sum of the rows it audits.
    fn encode_residual(engine: &TemporalEngine, plant: bool) -> (u64, u64, u64, u64) {
        crate::engine::reset_index_encode_counts();
        let planted = if plant {
            let before = crate::engine::index_encode_counts().encode_bytes_total;
            // The catalog dump encodes the served index FIRST and only then makes the folded
            // anchor durable, so what it returns says whether the fold landed rather than whether
            // the encode happened. The ENCODE is what is being planted, so the encode is what is
            // measured and required below: taking the boolean for it would leave the plant
            // unproven in exactly the case where the plant is zero.
            let _folded = engine.dump_index_catalog(1);
            crate::engine::index_encode_counts().encode_bytes_total - before
        } else {
            0
        };
        crate::engine::reset_live_block_scan_entries();
        crate::engine::reset_bucket_block_index_visits();
        let report = engine.run_storage_manager_cycle(round_request());
        assert!(report.errors.is_empty(), "the round errored: {:?}", report.errors);
        let seen = crate::engine::index_encode_counts();
        let charged_bytes: u64 = report
            .stages
            .iter()
            .map(|stage| stage.walk.index_encode_bytes)
            .sum();
        let charged_encodes: u64 =
            report.stages.iter().map(|stage| stage.walk.index_encodes).sum();
        // FLOOR THE INSTRUMENT. A residual read from a counter that counted nothing is not a zero,
        // it is an absent measurement, and the two are indistinguishable without this line.
        assert!(
            seen.encode_bytes_total > 0,
            "the encode instrument read zero bytes across the whole span, so its residual \
             measures nothing"
        );
        assert!(
            seen.encode_bytes_total >= charged_bytes,
            "the stage rows claim {charged_bytes} encoded bytes and the independent instrument \
             saw {}. Rows exceeding the instrument would mean the round encoded on another \
             thread, which this measurement is not built to read",
            seen.encode_bytes_total,
        );
        (
            seen.encode_bytes_total - charged_bytes,
            seen.encodes_total - charged_encodes,
            planted,
            charged_bytes,
        )
    }

    // A steady store, so the two rounds below differ only by the plant.
    seed(&engine, 0, RECORDS);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let (control_bytes, control_encodes, _, control_charged) = encode_residual(&engine, false);
    seed(&engine, 0, RECORDS);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let (planted_span_bytes, planted_span_encodes, planted, treatment_charged) =
        encode_residual(&engine, true);

    println!(
        "  CONTROL round: stage rows charged {control_charged} encoded bytes, residual \
         {control_bytes} bytes / {control_encodes} encode(s)"
    );
    println!(
        "  PLANTED round: stage rows charged {treatment_charged} encoded bytes, residual \
         {planted_span_bytes} bytes / {planted_span_encodes} encode(s); plant was {planted} bytes"
    );

    // THE PLANT IS A WHOLE-STORE QUANTITY, not a token. It is the served index of an
    // 8,000-record shard, which is what the round's own tail writes when its threshold is crossed.
    assert!(
        planted > 100_000,
        "the plant was {planted} bytes on an {RECORDS}-record store. It is supposed to be the \
         whole served index; something this small means the engine wrote something else and the \
         recovery below is about the wrong quantity"
    );

    // EXACT RECOVERY. The residual has to move by the plant and by nothing else.
    assert_eq!(
        planted_span_bytes,
        control_bytes + planted,
        "the residual did not recover the planted {planted} bytes exactly: it moved from \
         {control_bytes} to {planted_span_bytes}. A residual that does not move by exactly what is \
         placed inside it is arithmetic between two views of one number rather than an \
         independent reading -- and if the two rounds differ in their OWN unattributed encode \
         this fails here, loudly, which is the right failure"
    );
    assert_eq!(
        planted_span_encodes,
        control_encodes + 1,
        "the residual recovered {} planted encodes rather than exactly one",
        planted_span_encodes - control_encodes,
    );

    // WHAT THE ROUND ITSELF LEAVES UNATTRIBUTED, reported rather than asserted away. Zero here
    // means every served-index encode this round made was charged to a stage -- and the plant
    // above is what says a nonzero one would have been seen.
    println!(
        "  the round's OWN unattributed encode on this fixture: {control_bytes} bytes / \
         {control_encodes} encode(s)"
    );
    assert_eq!(
        control_bytes, 0,
        "this round left {control_bytes} bytes of served-index encode outside every stage row. \
         That is a whole-store cost with no stage to its name, which is worth reading rather than \
         absorbing: give it a stage, or record here which part of the round makes it"
    );
}
