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
    //
    // THE SEEDED WRITES ARE RELEASED WITH A ONE-BYTE GATE. A dirty shard below the byte gate is
    // deliberately left undumped, so settling under the shipped gate would never finish: the seed
    // is a few kilobytes of log. The quiet rounds below then run under `options`, where nothing
    // is dirty and the memory alone decides.
    let settling = StorageManagerOptions {
        min_undumped_wal_records: 1,
        min_undumped_wal_bytes: 1,
        ..options.clone()
    };
    let mut settled_after = None;
    for round in 1..=12 {
        let report = runtime.run_storage_manager_once(1, settling.clone());
        // Settled means IDLE and not asking: a round that skips the stage because the shard is
        // still dirty below the byte gate walks nothing either, and is not the memory at work.
        if wal_stage_walk(&report) == 0 && report.pressure.dirty_bucket_count == 0 {
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

    // A WRITE makes the shard dirty, and a dirty shard below the byte gate is not idle: the stage
    // does not ask the question at all (no walk), because the log bytes decide, not the memory.
    let response = runtime.inner.engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "loop-after-the-quiet".to_string(),
            value: vec![b'w'; 96],
        },
    });
    assert!(response.status.ok, "write after the quiet rounds: {:?}", response.status);
    let report = runtime.run_storage_manager_once(1, options.clone());
    assert_eq!(
        wal_stage_walk(&report),
        0,
        "a dirty shard below the byte gate asked the WAL-reclaim question"
    );

    // Release the write with a one-byte gate until the shard is clean again, then QUIET AGAIN.
    let releasing = StorageManagerOptions {
        min_undumped_wal_records: 1,
        min_undumped_wal_bytes: 1,
        ..options.clone()
    };
    for _ in 0..12 {
        runtime.run_storage_manager_once(1, releasing.clone());
    }
    let mut quiet_again = false;
    for _ in 0..12 {
        let report = runtime.run_storage_manager_once(1, options.clone());
        if wal_stage_walk(&report) == 0 && report.pressure.dirty_bucket_count == 0 {
            quiet_again = true;
            break;
        }
    }
    assert!(quiet_again, "the stage never went quiet again after the write");
    // Change only what the CALLER passes. A reader that appears registers no write, so nothing on
    // the shard moves; the retention inputs are the only part of the memory's key that can notice,
    // and a stage that answered from the old key would keep skipping.
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
    // The stage dumps through the shipped byte gate, so this fixture crosses it; below the gate a
    // dirty shard is left undumped on purpose and keeps walking, which is a different property.
    let options = StorageManagerOptions {
        min_undumped_wal_bytes: 1,
        ..StorageManagerOptions::default()
    };

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

/// A DIRTY SHARD THE BYTE GATE HOLDS BACK STOPS PAYING FOR THE INDEX STAGE TOO.
///
/// With the shipped gate and a log under the collector's floor, the stage has no dump to make and
/// no log to collect, so a round must not walk the shard to find that out. The shard is dirty, so
/// the older "nothing dirty" shortcut does not apply; the gate decision is what lets the round skip.
/// The stage must still count as having run.
///
/// Planted fault: reverting the stage's idle test to "nothing dirty and nothing undumped" makes the
/// first assertion fail, because the dirty shard then walks on every round.
#[test]
fn a_dirty_shard_below_the_byte_gate_does_not_walk_for_the_index_stage() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 200);
    let options = StorageManagerOptions::default();
    for round in 0..4 {
        let report = runtime.run_storage_manager_once(1, options.clone());
        assert!(
            report.pressure.dirty_bucket_count > 0,
            "round {round}: the fixture must keep the shard dirty under the shipped gate"
        );
        assert_eq!(
            index_stage_walk(&report),
            0,
            "round {round}: a dirty shard below the gate and the floor walked for the index stage"
        );
        assert!(
            report.executed_stages.iter().any(|stage| stage == "reclaim_index"),
            "round {round}: the index stage left the stage list: {:?}",
            report.executed_stages
        );
    }
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

/// THE CACHE-BOUND STAGE DUMPS NOTHING.
///
/// `reclaim_memory` invalidates cached blocks, which are copies; the data is in the log and the
/// resident index whether or not a bucket was dumped. It used to run a whole lifecycle request that
/// also dumped up to `max_dump_buckets_per_round` dirty buckets, so a stage named for memory spent
/// the dump stage's I/O. Every other stage is switched off here, so the only thing that could
/// shrink the dirty set is this one, and the count has to be the same on both sides while the cache
/// is emptied.
#[test]
fn the_cache_bound_stage_drops_the_cache_and_dumps_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 400);
    let engine = runtime.engine();
    for index in 0..50 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("loop-{index:06}"),
            },
        });
        assert!(response.status.ok);
    }
    let only_this_stage = StorageManagerOptions {
        enable_prepare: false,
        enable_wal_reclaim: false,
        enable_expire: false,
        enable_block_gc: false,
        enable_block_compaction: false,
        enable_index_gc: false,
        enable_metrics_reap: false,
        enable_evict: false,
        cache_memory_bytes_pressure: 1,
        ..StorageManagerOptions::default()
    };
    let (before, _) = runtime.storage_manager_pressure_snapshot(1, &only_this_stage);
    assert!(before.dirty_bucket_count > 0, "the fixture has no dirty bucket, so a dump would show nothing");
    assert!(before.cache_memory_bytes > 0, "the fixture never warmed the cache");
    let report = runtime.run_storage_manager_once(1, only_this_stage.clone());
    assert!(
        report.executed_stages.iter().any(|stage| stage == "reclaim_memory"),
        "the stage did not run: {:?}",
        report.executed_stages
    );
    let (after, _) = runtime.storage_manager_pressure_snapshot(1, &only_this_stage);
    assert_eq!(after.cache_memory_bytes, 0, "the cache was not dropped");
    assert_eq!(
        after.dirty_bucket_count, before.dirty_bucket_count,
        "the cache-bound stage dumped dirty buckets"
    );
}

/// The runtime's expired-key count is what the loop's expire stage removed, summed over rounds.
///
/// The metrics endpoint publishes `expired_records_removed` from the runtime stats, so this is the
/// number an operator reads to tell whether expiry keeps up. Three keys with a one-millisecond
/// deadline are made due and one round runs: the count must read exactly three, and a second round
/// with nothing due must leave it at three.
#[test]
fn the_runtime_counts_the_keys_the_expire_stage_removed() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 10);
    let engine = runtime.engine();
    for index in 0..3 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetEx {
                key: format!("due-{index}"),
                value: vec![b'v'; 16],
                ttl_ms: 1,
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(runtime.stats().expired_records_removed, 0);
    let first = runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    assert_eq!(first.expired_records_removed, 3);
    assert_eq!(runtime.stats().expired_records_removed, 3);
    runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    assert_eq!(
        runtime.stats().expired_records_removed,
        3,
        "a round with nothing due moved the expired-key count"
    );
}

/// The overdue count is the backlog a bounded round leaves behind.
///
/// Three keys are made due and the round may remove one hot key, so one is removed, two remain
/// overdue, and the count reads two; the next two rounds drain it to zero. A gauge that read zero
/// whatever the backlog would report expiry as keeping up on a server where it is not.
#[test]
fn the_overdue_count_is_what_a_bounded_round_leaves_due() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 10);
    let engine = runtime.engine();
    for index in 0..3 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetEx {
                key: format!("due-{index}"),
                value: vec![b'v'; 16],
                ttl_ms: 1,
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(engine.expiry_overdue_keys(), 3);
    let one_per_round = StorageManagerOptions {
        max_expire_hot_buckets_per_round: 1,
        ..StorageManagerOptions::default()
    };
    runtime.run_storage_manager_once(1, one_per_round.clone());
    assert_eq!(engine.expiry_overdue_keys(), 2, "one removed, two still overdue");
    runtime.run_storage_manager_once(1, one_per_round.clone());
    runtime.run_storage_manager_once(1, one_per_round);
    assert_eq!(engine.expiry_overdue_keys(), 0);
}

/// A shard's round expires that shard's keys and no other shard's.
///
/// The expire stage used to sweep every loaded shard from each shard's round, so with two shards a
/// round for shard 1 removed shard 2's due key and reported it as its own. Shard 2 holds the only
/// due key here; shard 1's round must remove nothing and shard 2's own round must remove it.
#[test]
fn a_shards_round_expires_only_that_shards_keys() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 10);
    let engine = runtime.engine();
    engine.load_shard(2);
    let response = engine.execute(ExecuteRequest {
        shard_id: 2,
        command: Command::StringSetEx {
            key: "other-shard-due".to_string(),
            value: vec![b'v'; 16],
            ttl_ms: 1,
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let options = StorageManagerOptions::default();
    let own = runtime.run_storage_manager_once(1, options.clone());
    assert_eq!(
        own.expired_records_removed, 0,
        "shard 1's round removed a key that belongs to shard 2"
    );
    let other = runtime.run_storage_manager_once(2, options);
    assert_eq!(
        other.expired_records_removed, 1,
        "shard 2's own round did not remove its due key"
    );
}

/// The evict and expire counts are each stage's own run count, once per round.
///
/// `storage_manager_evict_count` read the reclaim-memory run count, so it stayed at zero on a
/// server where eviction ran every round, and `storage_manager_expire_count` added a second counter
/// that the same round also bumped, so it read two per round.
#[test]
fn the_expire_and_evict_counts_are_each_stages_own_runs() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 200);
    let options = StorageManagerOptions::default();
    for _ in 0..3 {
        let report = runtime.run_storage_manager_once(1, options.clone());
        assert!(report.executed_stages.iter().any(|stage| stage == "expire"));
        assert!(report.executed_stages.iter().any(|stage| stage == "evict"));
        assert!(!report.executed_stages.iter().any(|stage| stage == "reclaim_memory"));
    }
    let metrics = data_node_storage_lifecycle_metrics(&runtime.stats());
    assert_eq!(metrics["storage_manager_expire_count"], 3);
    assert_eq!(metrics["storage_manager_evict_count"], 3);

    // The evict count moves only on evict: a round with eviction off and the cache bound set runs
    // reclaim_memory and must leave it where it was.
    // Eviction has two gates: `enable_evict` and the per-shard index bound, which opens the stage
    // on its own. Both are off here, or the evict stage runs and its count legitimately moves.
    let reclaim_only = StorageManagerOptions {
        enable_evict: false,
        index_resident_limit_bytes: 0,
        cache_memory_bytes_pressure: 1,
        ..StorageManagerOptions::default()
    };
    let engine = runtime.engine();
    // Warm the cache the way the sibling test does, or the one-byte bound has nothing to open on.
    for index in 0..50 {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("loop-{index:06}"),
            },
        });
        assert!(response.status.ok);
    }
    let report = runtime.run_storage_manager_once(1, reclaim_only);
    assert!(
        report.executed_stages.iter().any(|stage| stage == "reclaim_memory"),
        "the reclaim_memory arm did not run: {:?}",
        report.executed_stages
    );
    let after = data_node_storage_lifecycle_metrics(&runtime.stats());
    assert_eq!(
        after["storage_manager_evict_count"], 3,
        "a reclaim_memory round moved the evict count"
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
    // `LOOP_DUMP=1` crosses the byte gate so every round dumps, which prices the whole-index encode.
    let shipped = if std::env::var("LOOP_DUMP").is_ok() {
        StorageManagerOptions {
            dirty_bucket_pressure: 1,
            min_undumped_wal_bytes: 1,
            ..shipped
        }
    } else {
        shipped
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
                "      {:<18} ran={} {:>8} us walk={} encodes={}",
                span.stage,
                span.executed,
                span.duration_us,
                span.walk.live_block_entries,
                span.walk.index_encodes
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
    // THE GATE IS CROSSED, because the control below is that the round dumped. Under the shipped
    // byte gate a shard this small is left undumped and the stages skip their walks, so a round that
    // listed nothing would fail the floor for the right reason and say nothing about the bound.
    let options = StorageManagerOptions {
        dirty_bucket_pressure: 1,
        min_undumped_wal_bytes: 1,
        ..StorageManagerOptions::default()
    };
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

/// A DIRTY ROUND UNDER THE BYTE GATE LISTS NOTHING FOR THE INDEX STAGE.
///
/// The other arm of `a_dirty_round_lists_the_shards_live_blocks_no_more_than_the_bound`: the same
/// fixture with the shipped gate, where the round has no dump to make. In every round where the
/// index log is below its floor and the plan names no prune, roll-forward or destroy, the index
/// stage's own span must list zero live blocks. The round total stays under the dumping arm's 18
/// listings in every round.
///
/// Planted fault: reverting the index stage's idle test to "nothing dirty and nothing undumped"
/// makes the stage list the shard and fails the zero assertion.
#[test]
fn a_dirty_round_under_the_byte_gate_lists_less_than_a_dumping_round_does() {
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
        // The index log is read before the round: once it reaches the floor the stage collects it by
        // design, and only below the floor is "lists nothing" the claim.
        let log_below_floor = engine.index_log_store().log_len_bytes(1)
            < StorageManagerCycleRequest::default().index_gc_index_log_bytes_threshold;
        let report = runtime.run_storage_manager_once(1, options.clone());
        let walked: u64 = report.stage_spans.iter().map(|s| s.walk.live_block_entries).sum();
        // The stage also has work when the round's own plan names a manifest to prune, an install to
        // roll forward or a slab to destroy; those rounds are the stage doing its job, not a walk
        // the gate should have saved.
        let plan = &report.lifecycle_plan;
        let other_work = plan.reasons.iter().any(|reason| {
            reason == "slot_dump_manifest_prune" || reason == "slot_dump_install_roll_forward_check"
        }) || !plan.delayed_destroy_block_slab_ids.is_empty();
        eprintln!(
            "gated dirty round {round}: walk_entries={walked} ({:.1} listings), index stage walk={}, \
             log below floor={log_below_floor}, other work={other_work}, reasons={:?}",
            walked as f64 / keys as f64,
            index_stage_walk(&report),
            plan.reasons
        );
        assert!(
            report.pressure.dirty_bucket_count > 0,
            "round {round}: the fixture must keep the shard dirty under the shipped gate"
        );
        if log_below_floor && !other_work {
            assert_eq!(
                index_stage_walk(&report),
                0,
                "round {round}: the index stage listed live blocks although the gate holds its dump back"
            );
            checked += 1;
        }
        // Other stages may still list, so the round total is only held under the dumping arm's
        // measured level (18 listings), not under a floor of its own.
        assert!(
            walked < keys as u64 * 18,
            "round {round}: a dirty round the gate holds back listed {walked} entries, at the dumping \
             round's level of {}",
            keys * 18
        );
    }
    assert!(checked >= 1, "no round had an idle index stage under the gate, so nothing was checked");
}

/// The stage rows say where in the round each stage began, on the round's own clock: the first row
/// begins at zero, each row begins where the one before it ended, and the last one ends inside the
/// round. That is what lets a writer's acknowledgements, timestamped on any other clock, be placed
/// inside the stage that was running, so a stage's effect on writers can be read from one run.
/// With a writer attached, every write is charged to exactly one of the stage windows or to the
/// idle baseline, so no write is dropped and none is counted twice.
#[test]
fn a_writers_waits_are_attributed_to_the_stage_window_it_began_in() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), 300);
    let engine = std::sync::Arc::new(runtime.engine());

    let (stall, report) = crate::engine::writer_stall_probe::measure_writer_stall_with(
        &engine,
        1,
        || runtime.run_storage_manager_once(1, StorageManagerOptions::default()),
    );

    let spans = &report.stage_spans;
    assert!(!spans.is_empty(), "the round reported no stage rows");
    assert_eq!(spans[0].start_offset_us, 0, "the first stage row does not begin at the round's start");
    for pair in spans.windows(2) {
        let end = pair[0].start_offset_us + pair[0].duration_us;
        // The next row begins where this one ended, to within the microsecond truncation of two
        // separate conversions.
        assert!(
            pair[1].start_offset_us.abs_diff(end) <= 2,
            "row {} begins at {} but row {} ended at {end}",
            pair[1].stage,
            pair[1].start_offset_us,
            pair[0].stage
        );
    }
    let last = spans.last().unwrap();
    assert!(
        last.start_offset_us + last.duration_us <= report.duration_us + 2,
        "the last stage row ends after the round did"
    );

    let windows: Vec<(String, u64, u64)> = spans
        .iter()
        .map(|span| (span.stage.clone(), span.start_offset_us, span.duration_us))
        .collect();
    let by_window = stall.window_stalls(&windows);
    let idle = stall.idle_baseline();
    let charged: usize = by_window.iter().map(|w| w.writes).sum::<usize>() + idle.writes;
    // A write that began in the few microseconds between the probe calling the round and the round
    // taking its first timestamp, or between the last row and the end of the call, belongs to no
    // window and to no idle count; there can only be a handful of them.
    assert!(
        charged <= stall.samples.len() && stall.samples.len() - charged <= 4,
        "{} writes were issued but {charged} were charged to a window or to the idle baseline",
        stall.samples.len()
    );
    assert!(idle.writes > 0, "the writer issued nothing outside the round to read the windows against");
    println!("PROBE round_us={} idle={idle:?}", stall.round_us);
    for window in &by_window {
        println!("PROBE {window:?}");
    }
}

/// Runs the gated fixture for ten rounds, writing only in the first `write_rounds` of them, and
/// returns per round whether the plan listed a quarantined slab and what the index stage walked.
///
/// The writing rounds are what put a slab in quarantine; the quiet rounds after them are the shape a
/// running server spends nearly all its time in, with a quarantine that is neither growing nor old
/// enough to purge. `options_for` picks the options of each round.
fn quarantine_rounds(
    write_rounds: usize,
    options_for: impl Fn(usize) -> StorageManagerOptions,
) -> Vec<(bool, u64)> {
    let keys = 300usize;
    let writes = 20usize;
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), keys);
    for _ in 0..6 {
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
    }
    let engine = runtime.engine();
    let mut rounds = Vec::new();
    for round in 0..10usize {
        if round < write_rounds {
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
        }
        let report = runtime.run_storage_manager_once(1, options_for(round));
        let quarantined = !report.lifecycle_plan.delayed_destroy_block_slab_ids.is_empty();
        eprintln!(
            "quarantine round {round}: quarantined={quarantined} index stage walk={} reasons={:?}",
            index_stage_walk(&report),
            report.lifecycle_plan.reasons
        );
        rounds.push((quarantined, index_stage_walk(&report)));
    }
    rounds
}

/// A QUARANTINE THAT HAS NOT AGED OUT AND HAS BEEN CHECKED DOES NOT MAKE THE INDEX STAGE WALK.
///
/// The plan lists every slab in quarantine, and a quarantine is never empty once overwrites have
/// collected a slab. The purge acts on none of it before the grace has passed, so a quarantine that
/// nothing has joined since a purge round checked it against the live set is not work for the stage.
/// After four writing rounds the shard goes quiet with its quarantine intact; the last three rounds
/// must list a quarantined slab (the denominator) and leave the index stage at zero walk. The first
/// quiet rounds may still pay one full round for the arrival that preceded them.
///
/// Planted fault: dropping `|| index_quarantine_waits` from the stage's idle test, or the
/// `checked == arrivals` half from the store's answer, puts the walk back (1500 entries on this
/// fixture).
#[test]
fn a_checked_quarantine_that_has_not_aged_out_does_not_make_the_index_stage_walk() {
    let rounds = quarantine_rounds(4, |_| StorageManagerOptions::default());
    for round in 7..10usize {
        let (quarantined, walk) = rounds[round];
        assert!(quarantined, "round {round} must still list a quarantined slab: {rounds:?}");
        assert_eq!(walk, 0, "round {round}: a young checked quarantine made the stage walk: {rounds:?}");
    }
}

/// A YOUNG QUARANTINE COSTS THE INDEX STAGE NO WALK EVEN WHILE WRITES KEEP ADDING TO IT.
///
/// With writes every round the page stage sets a new slab aside each time, so the quarantine is
/// never "checked since the last arrival" and the stage cannot skip. It still needs only the purge:
/// the dump, the prune, the roll-forward and the log collector are idle, and the purge reads the
/// maintained tally while nothing can age out. Every round whose plan lists a quarantined slab must
/// leave the index stage at zero walk, and at least two must exist.
///
/// Planted fault: making `index_gc_purge_only` false sends these rounds down the full path and the
/// stage walks the shard again (1500 entries on this fixture).
#[test]
fn a_young_quarantine_fed_by_steady_writes_costs_the_index_stage_no_walk() {
    let rounds = quarantine_rounds(10, |_| StorageManagerOptions::default());
    let quarantined: Vec<_> = rounds.iter().filter(|(quarantined, _)| *quarantined).collect();
    assert!(
        quarantined.len() >= 2,
        "the fixture must leave a slab in quarantine for at least two rounds, got {rounds:?}"
    );
    for (_, walk) in &quarantined {
        assert_eq!(*walk, 0, "a young quarantine made the stage walk the shard: {rounds:?}");
    }
}

/// THE TALLY NAMES THE SLABS THE WALK DOES, on the shapes this fixture can make: overwrites that
/// retire slabs, with rounds of compaction and collection between them.
///
/// The tally is only handed to a purge that cannot destroy, so a gap would delay a restore and not
/// lose a slab; this is what says the gap is not there for strings. It does not cover the other
/// models, which is why the walked set stays the one a purge that can destroy is given.
///
/// Planted fault: leaving the tally's compaction-pinned slabs out of the set makes the two differ
/// while a compaction round is unpublished.
#[test]
fn the_tally_names_the_slabs_the_walk_does_across_overwrites_and_collection() {
    let keys = 300usize;
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), keys);
    let engine = runtime.engine();
    for round in 0..8usize {
        for write in 0..40usize {
            let index = (round * 40 + write) % keys;
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("loop-{index:06}"),
                    value: vec![b'w'; 96],
                },
            });
            assert!(response.status.ok, "write {index}: {:?}", response.status);
        }
        runtime.run_storage_manager_once(1, StorageManagerOptions::default());
        let walked = engine.live_block_slab_ids_all_shards();
        let from_tally = engine
            .live_block_slab_ids_all_shards_from_tally()
            .expect("the tally is derived after loop rounds");
        assert!(!walked.is_empty(), "round {round}: the fixture holds no live slab");
        assert_eq!(from_tally, walked, "round {round}: the tally and the walk name different slabs");
    }
}
