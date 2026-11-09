// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the periodic maintenance loop reports about its own stages, and what a quiet round of it
//! costs. `run_storage_manager_once` is the function the scheduler runs unattended; the on-demand
//! cycle is a different function with a different report.
#![allow(clippy::all)]
use super::*;

const END_BUCKET: u32 = 1023;

const LOOP_STAGES: [&str; 9] = [
    "prepare",
    "reclaim_wal",
    "expire",
    "evict",
    "reclaim_memory",
    "reclaim_page",
    "reclaim_index",
    "compact_pages",
    "reap_metrics",
];

fn seeded_runtime(dir: &std::path::Path, keys: usize) -> DataNodeRuntime {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "loop-report".to_string(),
        shard_uri: "local://loop-report/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: END_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(response.status.ok, "load shard 1: {:?}", response.status);
    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("loop-{index:06}"),
                value: vec![b'v'; 96],
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 64,
            max_background_queue_depth: 8,
        },
    )
}

/// Every stage the loop reaches has a row, in order; a row says whether its stage ran; the rows
/// are intervals of the round's own clock so they cannot add up to more than the round; and the
/// walk charge reaches the row of the stage that did the walking.
#[test]
fn the_loop_report_prices_every_stage_it_reaches() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 300);

    let options = StorageManagerOptions {
        // One stage off, so the skip path has to produce a row too.
        enable_block_compaction: false,
        ..StorageManagerOptions::default()
    };
    let report = runtime.run_storage_manager_once(1, options);

    let names = report
        .stage_spans
        .iter()
        .map(|span| span.stage.as_str())
        .collect::<Vec<_>>();
    let mut expected = vec!["pressure_snapshot"];
    expected.extend(LOOP_STAGES);
    assert_eq!(names, expected, "one row per stage reached, in the order reached");

    for span in report.stage_spans.iter().skip(1) {
        assert_eq!(
            span.executed,
            report.executed_stages.contains(&span.stage),
            "row `{}` disagrees with executed_stages {:?}",
            span.stage,
            report.executed_stages
        );
    }
    let compact = report
        .stage_spans
        .iter()
        .find(|span| span.stage == "compact_pages")
        .unwrap();
    assert!(!compact.executed, "the disabled stage must read as skipped");
    assert!(
        report.executed_stages.len() + report.skipped_stages.len() >= LOOP_STAGES.len(),
        "every loop stage reports either way"
    );

    let summed = report
        .stage_spans
        .iter()
        .map(|span| span.duration_us)
        .sum::<u64>();
    assert!(
        summed <= report.duration_us,
        "the rows are intervals of the round's clock: {summed} us of rows in a {} us round",
        report.duration_us
    );

    // NON-VACUITY. A fixture written and not dumped is dirty, so the plan walks it; the counters
    // are process-wide and a neighbouring test can only add to them, so a positive floor is stable.
    let walked = report
        .stage_spans
        .iter()
        .map(|span| span.walk.live_block_entries)
        .sum::<u64>();
    assert!(
        walked > 0,
        "a dirty shard's round walked nothing, so the rows are not reading the walk counters"
    );

    // ADDITIVE: an older serialised report still reads, and the new fields come back empty.
    let mut json = serde_json::to_value(&report).unwrap();
    let object = json.as_object_mut().unwrap();
    assert!(object.remove("stage_spans").is_some());
    assert!(object.remove("duration_us").is_some());
    let old: StorageManagerLoopReport = serde_json::from_value(json).unwrap();
    assert!(old.stage_spans.is_empty());
    assert_eq!(old.duration_us, 0);
    assert_eq!(old.executed_stages, report.executed_stages);
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    sorted[((sorted.len() * pct).div_ceil(100)).clamp(1, sorted.len()) - 1]
}

fn measure(label: &str, runtime: &DataNodeRuntime, options: &StorageManagerOptions, rounds: usize) {
    let mut per_stage: std::collections::BTreeMap<String, (Vec<u64>, Vec<u64>, usize)> =
        Default::default();
    let mut totals = Vec::new();
    let mut order = Vec::new();
    for _ in 0..rounds {
        let report = runtime.run_storage_manager_once(1, options.clone());
        totals.push(report.duration_us);
        for span in &report.stage_spans {
            if !order.contains(&span.stage) {
                order.push(span.stage.clone());
            }
            let row = per_stage.entry(span.stage.clone()).or_default();
            row.0.push(span.duration_us);
            row.1.push(span.walk.live_block_entries);
            row.2 += usize::from(span.executed);
        }
    }
    totals.sort_unstable();
    eprintln!(
        "== {label}: {rounds} rounds, round us min={} p50={} p80={} max={}",
        totals[0],
        percentile(&totals, 50),
        percentile(&totals, 80),
        totals[totals.len() - 1]
    );
    for stage in &order {
        let (mut us, walk, ran) = per_stage[stage].clone();
        us.sort_unstable();
        let walk_min = walk.iter().min().unwrap();
        let walk_max = walk.iter().max().unwrap();
        eprintln!(
            "   {stage:<18} ran {ran:>2}/{rounds}  walk_entries {walk_min}..{walk_max}  us p50={} p80={} max={}",
            percentile(&us, 50),
            percentile(&us, 80),
            us[us.len() - 1]
        );
    }
}

fn state(runtime: &DataNodeRuntime, options: &StorageManagerOptions) -> String {
    let (pressure, plan) = runtime.storage_manager_pressure_snapshot(1, options);
    format!(
        "undumped_wal_records={} dirty_buckets={} dump_delayed={} selected={}",
        pressure.undumped_wal_records,
        pressure.dirty_bucket_count,
        plan.dump_delayed,
        plan.selected_dump_buckets.len()
    )
}

/// Measurement harness, not a gate: `--ignored --nocapture`, one test thread, `LOOP_KEYS=<n>`.
#[test]
#[ignore]
fn what_a_quiet_loop_round_pays_stage_by_stage() {
    let keys = std::env::var("LOOP_KEYS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1000usize);
    let shipped = StorageManagerOptions::default();
    // Releases the dump unconditionally; everything else shipped.
    let releasing = StorageManagerOptions {
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        ..StorageManagerOptions::default()
    };
    eprintln!(
        "N={keys} keys, shipped: bytes_gate={} records_gate={} max_dump_buckets_per_round={}",
        shipped.min_undumped_wal_bytes,
        shipped.min_undumped_wal_records,
        shipped.max_dump_buckets_per_round
    );

    // ---- A: dirty but below the shipped gate: what a server's written shard looks like. ----
    let dir_a = tempfile::tempdir().unwrap();
    let a = seeded_runtime(dir_a.path(), keys);
    eprintln!("A pre-state (shipped): {}", state(&a, &shipped));
    for round in 1..=6 {
        let report = a.run_storage_manager_once(1, shipped.clone());
        let walked: u64 = report.stage_spans.iter().map(|s| s.walk.live_block_entries).sum();
        let encodes: u64 = report.stage_spans.iter().map(|s| s.walk.index_encodes).sum();
        eprintln!(
            "A shipped round {round}: {} us, walk_entries={walked}, index_encodes={encodes}, \
             dump_manifest_in_reclaim_wal={}, in-round plan: undumped={} dirty={} | after: {}",
            report.duration_us,
            report
                .lifecycle_report
                .as_ref()
                .and_then(|r| r.dump_manifest.as_ref())
                .map(|m| m.bucket_ids.len())
                .unwrap_or(0),
            report.pressure.undumped_wal_records,
            report.pressure.dirty_bucket_count,
            state(&a, &shipped)
        );
        for span in &report.stage_spans {
            eprintln!(
                "      {:<18} ran={} {:>8} us walk={} encodes={}",
                span.stage, span.executed, span.duration_us, span.walk.live_block_entries,
                span.walk.index_encodes
            );
        }
    }
    measure("A dirty-but-delayed, shipped", &a, &shipped, 25);
    eprintln!("A after measuring: {}", state(&a, &shipped));

    // ---- B: drain with the releasing gate, printing the state after EVERY round. ----
    let dir_b = tempfile::tempdir().unwrap();
    let b = seeded_runtime(dir_b.path(), keys);
    eprintln!("B pre-state (releasing): {}", state(&b, &releasing));
    for round in 1..=12 {
        let report = b.run_storage_manager_once(1, releasing.clone());
        eprintln!(
            "B drain round {round}: executed={:?} dumped_in_round={} | after: {}",
            report.executed_stages,
            report
                .lifecycle_report
                .as_ref()
                .and_then(|r| r.dump_manifest.as_ref())
                .map(|m| m.bucket_ids.len())
                .unwrap_or(0),
            state(&b, &releasing)
        );
    }
    eprintln!("B state under SHIPPED gate: {}", state(&b, &shipped));
    for _ in 0..5 {
        b.run_storage_manager_once(1, shipped.clone());
    }
    measure("B after drain, shipped", &b, &shipped, 25);
    eprintln!("B after measuring: {}", state(&b, &shipped));
}

/// What the WAL-reclaim stage's walk charge was in one round: nonzero means it asked the question.
fn wal_stage_walk(report: &StorageManagerLoopReport) -> u64 {
    report
        .stage_spans
        .iter()
        .find(|span| span.stage == "reclaim_wal")
        .expect("every round reports the reclaim_wal stage")
        .walk
        .live_block_entries
}

/// A SHARD THAT NOBODY WRITES TO STOPS ASKING THE WAL-RECLAIM QUESTION, AND A WRITE MAKES IT ASK
/// AGAIN.
///
/// The question is "could a reclaim do anything", and answering it builds a plan that walks every
/// live block. A quiet shard used to pay that walk on every round for an answer that could not have
/// changed. The round that finds nothing to reclaim now remembers what it read in O(1) and the next
/// one skips the question while none of it has moved.
///
/// BOTH HALVES ARE ASSERTED. The skip alone would pass a stage that had stopped working, so the
/// second half writes to the shard and requires the very next round to walk again. The walk charge
/// is the observable: it is nonzero exactly when the plan was built.
#[test]
fn a_quiet_shard_stops_asking_the_wal_reclaim_question_and_a_write_makes_it_ask_again() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 200);
    // THE DUMP THRESHOLDS ARE RAISED, AND THAT IS WHAT MAKES THE WRITE HALF MEAN ANYTHING. At the
    // shipped defaults one write makes the shard dirty enough to cross `dirty_bucket_pressure`, and
    // the stage then runs for THAT reason on a path that never consults the memory at all. The
    // post-write walk was satisfied by the other trigger, so planting a memory that never
    // invalidates still passed. With the thresholds out of reach, the only thing that can make the
    // stage ask again after a write is the memory's own key having moved.
    let options = StorageManagerOptions {
        dirty_bucket_pressure: 1_000_000,
        min_undumped_wal_records: 1_000_000,
        ..StorageManagerOptions::default()
    };

    // Settle: the first rounds do the shard's real work (dump, reclaim). Bound the wait so a stage
    // that never settles fails here instead of hanging the suite.
    let mut settled_after = None;
    for round in 1..=12 {
        let report = runtime.run_storage_manager_once(1, options.clone());
        if wal_stage_walk(&report) == 0 {
            settled_after = Some(round);
            break;
        }
    }
    let settled_after =
        settled_after.expect("the WAL-reclaim stage never stopped walking on an idle shard");

    // QUIET: several further rounds, every one of them skipping the question. A stage that asked
    // once more per round would be caught here, and one that had never asked would be caught below.
    for round in 0..5 {
        let report = runtime.run_storage_manager_once(1, options.clone());
        assert_eq!(
            wal_stage_walk(&report),
            0,
            "quiet round {round} (settled after {settled_after}) walked the shard again although \
             nothing had changed"
        );
    }

    // A WRITE changes what the stage reads, so the very next round must ask again.
    let response = runtime.inner.engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "loop-after-the-quiet".to_string(),
            value: vec![b'w'; 96],
        },
    });
    assert!(response.status.ok, "write after the quiet rounds: {:?}", response.status);
    let report = runtime.run_storage_manager_once(1, options.clone());
    assert!(
        wal_stage_walk(&report) > 0,
        "a write after the quiet rounds did not make the WAL-reclaim stage ask again, so it is \
         skipping on a stale answer"
    );

    // QUIET AGAIN, then change only what the CALLER passes. A reader that appears registers no
    // write, so nothing on the shard moves; the retention inputs are the only part of the memory's
    // key that can notice, and a stage that answered from the old key would keep skipping.
    let mut quiet_again = false;
    for _ in 0..12 {
        let report = runtime.run_storage_manager_once(1, options.clone());
        if wal_stage_walk(&report) == 0 {
            quiet_again = true;
            break;
        }
    }
    assert!(quiet_again, "the stage never went quiet again after the write");
    let with_a_reader = StorageManagerOptions {
        follower_replay_cursors: vec![crate::BucketDumpFollowerReplayCursor {
            follower_id: "a-reader-that-just-appeared".to_string(),
            shard_id: 1,
            wal_sequence: 1,
            index_log_sequence: 1,
        }],
        ..options.clone()
    };
    let report = runtime.run_storage_manager_once(1, with_a_reader);
    assert!(
        wal_stage_walk(&report) > 0,
        "a new retention input did not make the WAL-reclaim stage ask again, so the memory's key \
         does not include what the caller passes"
    );
}

/// What the index-reclaim stage's walk charge was in one round: nonzero means it did its work.
fn index_stage_walk(report: &StorageManagerLoopReport) -> u64 {
    report
        .stage_spans
        .iter()
        .find(|span| span.stage == "reclaim_index")
        .expect("every round reports the reclaim_index stage")
        .walk
        .live_block_entries
}

/// A SHARD WITH NOTHING FOR THE INDEX STAGE TO DO STOPS PAYING FOR IT, AND A DIRTY ONE DOES NOT.
///
/// The stage dumps the dirty set, prunes manifests, purges delayed destroys and only then asks
/// whether the index log is big enough to collect; on a shard where none of that has work, every
/// round paid a walk of every live block to find that out. The stage now answers from the round's
/// own plan, and still counts as having run, so the stage list keeps saying maintenance is alive.
///
/// BOTH HALVES ARE ASSERTED, and the second is the one that proves the first is a gate and not a
/// stage that stopped working: a write makes the shard dirty, and the very next round must do the
/// stage's real work, which is the dump, which walks. The stage must also stay in the executed
/// list on every quiet round.
#[test]
fn a_shard_with_nothing_for_the_index_stage_stops_paying_for_it_and_a_dirty_one_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 200);
    let options = StorageManagerOptions::default();

    // Settle: the first round dumps the whole dirty set through this stage. Bound the wait so a
    // stage that never settles fails here and does not hang the suite.
    let mut settled = false;
    for _ in 0..12 {
        let report = runtime.run_storage_manager_once(1, options.clone());
        if index_stage_walk(&report) == 0 {
            settled = true;
            break;
        }
    }
    assert!(settled, "the index stage never stopped walking on an idle shard");

    for round in 0..5 {
        let report = runtime.run_storage_manager_once(1, options.clone());
        assert_eq!(
            index_stage_walk(&report),
            0,
            "quiet round {round}: the index stage walked the shard although it had no work"
        );
        assert!(
            report.executed_stages.iter().any(|stage| stage == "reclaim_index"),
            "quiet round {round}: the index stage left the stage list, which an operator reads \
             as maintenance having died: {:?}",
            report.executed_stages
        );
    }

    // A WRITE makes the shard dirty, so the next round has real work for the stage.
    let response = runtime.inner.engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "loop-after-the-quiet-index".to_string(),
            value: vec![b'w'; 96],
        },
    });
    assert!(response.status.ok, "write after the quiet rounds: {:?}", response.status);
    let report = runtime.run_storage_manager_once(1, options.clone());
    assert!(
        index_stage_walk(&report) > 0,
        "a dirty shard's round did not do the index stage's work, so the gate is skipping a stage \
         that had something to dump"
    );
}



/// The span a profiler collects over: ONE periodic round of a shard that has nothing to do, so the
/// instructions it reports are the unattended loop's idle cost. `#[inline(never)]` so the symbol
/// exists for `valgrind --toggle-collect`.
#[inline(never)]
fn quiet_round_span(
    runtime: &DataNodeRuntime,
    options: &StorageManagerOptions,
) -> StorageManagerLoopReport {
    runtime.run_storage_manager_once(1, options.clone())
}

/// Drain a seeded shard with a gate that releases the dump, then settle it under the shipped
/// options, so the round the profiler sees is a round with nothing to do.
fn drained_runtime(dir: &std::path::Path, keys: usize) -> DataNodeRuntime {
    let releasing = StorageManagerOptions {
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        ..StorageManagerOptions::default()
    };
    let runtime = seeded_runtime(dir, keys);
    for _ in 0..12 {
        runtime.run_storage_manager_once(1, releasing.clone());
    }
    let shipped = StorageManagerOptions::default();
    for _ in 0..3 {
        runtime.run_storage_manager_once(1, shipped.clone());
    }
    runtime
}

/// Profiler driver, not a gate: `LOOP_KEYS=<n>`, run under
/// `valgrind --tool=callgrind --toggle-collect='*quiet_round_span*'`.
#[test]
#[ignore]
fn a_quiet_round_for_the_profiler() {
    let keys = std::env::var("LOOP_KEYS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1000usize);
    let dir = tempfile::tempdir().unwrap();
    let runtime = drained_runtime(dir.path(), keys);
    let shipped = StorageManagerOptions::default();
    crate::engine::reset_storage_plan_build_counts();
    crate::engine::reset_manifest_digest_memo_counts();
    let report = quiet_round_span(&runtime, &shipped);
    let (lifecycle_plans, wal_plans) = crate::engine::storage_plan_build_counts();
    let (digest_hits, digest_computed) = crate::engine::manifest_digest_memo_counts();
    eprintln!(
        "QUIET_PLANS keys={keys} lifecycle_plans={lifecycle_plans} wal_reclaim_plans={wal_plans}          manifest_digest_hits={digest_hits} manifest_digests_computed={digest_computed}          manifests_retained={}",
        runtime.engine().list_bucket_dump_manifests(1).len()
    );
    eprintln!(
        "QUIET_ROUND keys={keys} state=[{}] executed={:?}",
        state(&runtime, &shipped),
        report.executed_stages
    );
    for span in &report.stage_spans {
        eprintln!(
            "QUIET_STAGE keys={keys} {:<18} ran={} walk_entries={} encodes={}",
            span.stage, span.executed, span.walk.live_block_entries, span.walk.index_encodes
        );
    }
}

/// THE REAP PUBLISHES PER-BUCKET GAUGES IN BOUNDED, RESUMABLE SLICES, AND THE SLICES ADD UP.
///
/// Three claims, each of which can fail on its own. A slice never holds more buckets than the option
/// allows, so a round's cost does not grow with the shard. The slices, taken in the order the rounds
/// ran, are EXACTLY the whole-shard sweep -- no bucket twice, none skipped, in routing order -- so
/// resuming from the cursor loses nothing. And once the last bucket is read the next round starts
/// again from the first, so the gauges stay current instead of stopping at the end.
///
/// The fixture must hold more buckets than two slices, or the sweep never has to resume and the
/// second claim is satisfied by a sweep that is really one slice. That is asserted, not assumed.
#[test]
fn the_metrics_reap_sweeps_every_bucket_in_bounded_slices_and_wraps() {
    const SLICE: usize = 16;
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 200);
    let off = StorageManagerOptions {
        reap_metrics_max_buckets_per_round: 0,
        ..StorageManagerOptions::default()
    };
    let sliced = StorageManagerOptions {
        reap_metrics_max_buckets_per_round: SLICE,
        ..StorageManagerOptions::default()
    };

    // Settle first, with the gauges off, so maintenance has finished changing the shard before the
    // whole-shard sweep that the slices are compared against is taken.
    for _ in 0..3 {
        runtime.run_storage_manager_once(1, off.clone());
    }
    let truth = runtime.inner.engine.bucket_gauge_sweep(1, None, usize::MAX);
    assert!(
        truth.gauges.len() > SLICE * 2,
        "the fixture holds {} buckets, which is not more than two slices of {SLICE}; the sweep \
         would never have to resume",
        truth.gauges.len()
    );
    assert_eq!(truth.buckets_total, truth.gauges.len());
    assert_eq!(truth.next_after, None, "a whole-shard sweep reaches the end");
    assert_eq!(
        truth.gauges.iter().map(|gauge| gauge.object_count).sum::<u64>(),
        200,
        "every seeded key is one object in exactly one bucket"
    );

    let mut seen = Vec::new();
    let mut rounds = 0usize;
    let mut wrapped = false;
    while rounds < 200 {
        rounds += 1;
        let report = runtime.run_storage_manager_once(1, sliced.clone());
        let reap = report.metrics_reap.as_ref().expect("the reap ran");
        assert!(
            reap.bucket_gauges.len() <= SLICE,
            "round {rounds} published {} buckets against a slice of {SLICE}",
            reap.bucket_gauges.len()
        );
        assert_eq!(reap.buckets_total, truth.buckets_total);
        seen.extend(reap.bucket_gauges.iter().cloned());
        if reap.bucket_sweep_next_after.is_none() {
            wrapped = true;
            break;
        }
    }
    assert!(wrapped, "{rounds} rounds never reached the last bucket");
    assert!(rounds > 1, "a single slice cannot show that the sweep resumes");
    assert_eq!(
        seen, truth.gauges,
        "the slices, in the order the rounds ran, must be exactly the whole-shard sweep"
    );

    // Wrapped: the next round starts again from the first bucket.
    let again = runtime.run_storage_manager_once(1, sliced.clone());
    assert_eq!(
        again
            .metrics_reap
            .as_ref()
            .and_then(|reap| reap.bucket_gauges.first()),
        truth.gauges.first(),
        "after the last bucket the sweep must start over from the first"
    );

    // Zero turns the per-bucket gauges off and leaves the rest of the reap running.
    let report = runtime.run_storage_manager_once(1, off);
    let reap = report.metrics_reap.as_ref().expect("the reap still ran");
    assert!(reap.bucket_gauges.is_empty());
    assert_eq!(reap.buckets_total, 0);
}

/// A WARM CACHE SURVIVES A ROUND UNLESS AN OPERATOR SET A BOUND, AND A BOUND STILL INVALIDATES IT.
///
/// The `reclaim_memory` stage drops every cached block of the shard. Its gate used to open at one
/// cached byte, so a shard that had served a single read lost its cache on the next round and every
/// round after, and readers paid cold reads each time. The expire and evict stages are what relieve
/// memory pressure; this stage is a blanket invalidation, so by default it waits for a bound.
///
/// BOTH ARMS ARE ASSERTED. The default arm alone would pass a stage that had stopped working, so the
/// second arm sets a one-byte bound and requires the same warm cache to be dropped.
#[test]
fn a_warm_cache_survives_a_default_round_and_an_explicit_bound_still_drops_it() {
    fn warm(engine: &TemporalEngine) {
        for index in 0..50 {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("loop-{index:06}"),
                },
            });
            assert!(response.status.ok, "read {index}: {:?}", response.status);
        }
    }
    fn cached(runtime: &DataNodeRuntime, options: &StorageManagerOptions) -> u64 {
        runtime
            .storage_manager_pressure_snapshot(1, options)
            .0
            .cache_memory_bytes
    }

    // DEFAULT: the cache stays.
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 200);
    let shipped = StorageManagerOptions::default();
    let engine = runtime.engine();
    warm(&engine);
    let before = cached(&runtime, &shipped);
    assert!(
        before > 0,
        "the fixture never warmed the cache, so the arms below prove nothing"
    );
    let report = runtime.run_storage_manager_once(1, shipped.clone());
    assert!(
        !report.executed_stages.iter().any(|stage| stage == "reclaim_memory"),
        "a default round ran the blanket invalidation: {:?}",
        report.executed_stages
    );
    assert!(
        cached(&runtime, &shipped) > 0,
        "a default round dropped a warm cache ({before} bytes before)"
    );

    // CONTROL: an explicit one-byte bound still invalidates.
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 200);
    let bounded = StorageManagerOptions {
        cache_memory_bytes_pressure: 1,
        ..StorageManagerOptions::default()
    };
    let engine = runtime.engine();
    warm(&engine);
    assert!(cached(&runtime, &bounded) > 0, "the control fixture never warmed");
    let report = runtime.run_storage_manager_once(1, bounded.clone());
    assert!(
        report.executed_stages.iter().any(|stage| stage == "reclaim_memory"),
        "an explicit bound did not run the stage: {:?}",
        report.executed_stages
    );
    assert_eq!(
        cached(&runtime, &bounded),
        0,
        "an explicit bound ran the stage but the cache is still warm"
    );
}

/// Measurement harness, not a gate: `--ignored --nocapture`, one test thread, `LOOP_KEYS=<n>`,
/// `LOOP_WRITES=<writes per round>`. A shard that takes writes between rounds is dirty at every
/// round, so this prices what a round pays then, stage by stage and by the call site that walked.
#[test]
#[ignore]
fn what_a_dirty_loop_round_walks_by_call_site() {
    let keys = std::env::var("LOOP_KEYS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1500usize);
    let writes = std::env::var("LOOP_WRITES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20usize);
    let rounds = 8usize;
    // `LOOP_OLD_CACHE_GATE=1` restores the one-byte cache bounds the shipped defaults used to carry.
    let shipped = if std::env::var("LOOP_OLD_CACHE_GATE").is_ok() {
        StorageManagerOptions {
            cache_memory_bytes_pressure: 1,
            cache_disk_bytes_pressure: 1,
            ..StorageManagerOptions::default()
        }
    } else {
        StorageManagerOptions::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), keys);
    // Warm the cache the way a served shard would be, so the stage's gate has something to see.
    {
        let engine = runtime.engine();
        for index in 0..keys.min(200) {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("loop-{index:06}"),
                },
            });
        }
    }
    for _ in 0..6 {
        runtime.run_storage_manager_once(1, shipped.clone());
    }
    let engine = runtime.engine();
    for round in 0..rounds {
        for write in 0..writes {
            let index = (round * writes + write) % keys;
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("loop-{index:06}"),
                    value: vec![b'w'; 96],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
        crate::engine::reset_live_block_scan_sites();
        let report = runtime.run_storage_manager_once(1, shipped.clone());
        let walked: u64 = report.stage_spans.iter().map(|s| s.walk.live_block_entries).sum();
        eprintln!(
            "D round {round}: {} us, walk_entries={walked} (= {:.1} whole-shard walks of {keys})",
            report.duration_us,
            walked as f64 / keys as f64
        );
        for span in &report.stage_spans {
            eprintln!(
                "      {:<18} ran={} {:>8} us walk={}",
                span.stage, span.executed, span.duration_us, span.walk.live_block_entries
            );
        }
        for (site, entries) in crate::engine::live_block_scan_sites_snapshot() {
            eprintln!("      site {site:<60} {entries:>8} entries ({:.1}x)", entries as f64 / keys as f64);
        }
    }
}

/// A DIRTY ROUND READS THE LIVE BLOCKS OF THE SHARD NO MORE THAN `DIRTY_ROUND_WALK_BOUND` TIMES.
///
/// A shard that takes writes between rounds is dirty at every round, and the round then builds its
/// plans from a listing of every live block. The WAL reclaim plan needs the bucket summaries and the
/// live digests; those come off ONE listing, so each plan build lists the shard once instead of twice
/// and the round totals 20 listings of the shard. Counted in live-block entries visited, not timed, because a stopwatch on a shared box
/// measures the other tenants.
///
/// THE BOUND IS EXACT, so a plan that lists the shard once more fails it. The bound is on the TOTAL
/// of the round and not on one stage: dropping one stage's walks has moved them to a later stage
/// before. The control is that the walk is large enough to mean something and that the round did
/// dump (a round that did nothing would pass a bound trivially).
const DIRTY_ROUND_WALK_BOUND: u64 = 20;

#[test]
fn a_dirty_round_lists_the_shards_live_blocks_no_more_than_the_bound() {
    let keys = 300usize;
    let writes = 20usize;
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), keys);
    let options = StorageManagerOptions::default();
    for _ in 0..6 {
        runtime.run_storage_manager_once(1, options.clone());
    }
    let engine = runtime.engine();
    let mut checked = 0;
    for round in 0..4usize {
        for write in 0..writes {
            let index = (round * writes + write) % keys;
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("loop-{index:06}"),
                    value: vec![b'w'; 96],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
        let report = runtime.run_storage_manager_once(1, options.clone());
        let walked: u64 = report.stage_spans.iter().map(|s| s.walk.live_block_entries).sum();
        eprintln!("dirty round {round}: walk_entries={walked} ({:.1} listings)", walked as f64 / keys as f64);
        // The first dirty round also settles the writes of the warm-up; the bound is for the rest.
        if round == 0 {
            continue;
        }
        assert!(walked >= keys as u64 * 10, "the round listed only {walked} entries");
        assert!(
            walked <= DIRTY_ROUND_WALK_BOUND * keys as u64,
            "a dirty round listed {walked} entries = {:.1} listings of {keys} keys, bound {DIRTY_ROUND_WALK_BOUND}",
            walked as f64 / keys as f64
        );
        checked += 1;
    }
    assert_eq!(checked, 3);
}
