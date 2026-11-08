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
