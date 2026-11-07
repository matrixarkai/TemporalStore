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
