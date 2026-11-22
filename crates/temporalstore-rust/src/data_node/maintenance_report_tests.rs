// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::{Command, CommandResponse, ExecuteRequest};

fn seeded(dir: &std::path::Path) -> DataNodeRuntime {
    seeded_keys(dir, 32)
}

fn seeded_keys(dir: &std::path::Path, keys: usize) -> DataNodeRuntime {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("key-{i}"),
                value: vec![i as u8; 96],
            },
        });
        assert!(response.status.ok);
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

fn options() -> StorageManagerOptions {
    StorageManagerOptions {
        min_undumped_wal_records: 1,
        max_dump_buckets_per_round: 4,
        enable_memory_reclaim: false,
        enable_expire: false,
        enable_block_gc: false,
        enable_block_compaction: false,
        enable_evict: false,
        enable_metrics_reap: false,
        ..Default::default()
    }
}

#[test]
fn timer_execution_preserves_actions_and_counters_without_exposing_partial_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path());
    let report = runtime.run_storage_manager_once_with_report_details(1, options(), false);
    assert!(report.status.ok, "{:?}", report.status);
    assert!(
        report
            .executed_stages
            .iter()
            .any(|stage| stage == "reclaim_wal")
    );
    assert!(report.lifecycle_report.is_none());
    let stats = runtime.stats();
    assert!(stats.dump_runs > 0);
    assert!(stats.storage_lifecycle_runs > 0);
    assert_eq!(stats.storage_manager_runs, 1);
    for i in 0..32 {
        let response = runtime.engine().execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("key-{i}"),
            },
        });
        assert!(response.status.ok);
        assert!(
            matches!(response.response, CommandResponse::Bytes { value: Some(value) } if value == vec![i as u8; 96])
        );
    }
}

#[test]
fn timer_pressure_plans_preserve_work_without_bucket_reports() {
    for threshold in [1, u64::MAX] {
        let dir = tempfile::tempdir().unwrap();
        let runtime = seeded(dir.path());
        let options = StorageManagerOptions {
            min_undumped_wal_records: threshold,
            min_undumped_wal_bytes: u64::MAX,
            ..options()
        };
        crate::engine::reset_live_block_scan_entries();
        let (mut detailed_pressure, mut detailed_plan) =
            runtime.storage_manager_pressure_snapshot(1, &options);
        let detailed_work = crate::engine::live_block_scan_entries();
        crate::engine::reset_live_block_scan_entries();
        let (pressure, plan) =
            runtime.storage_manager_pressure_snapshot_with_report_details(1, &options, false);
        let execution_work = crate::engine::live_block_scan_entries();

        assert!(detailed_pressure.live_block_summaries_measured);
        assert!(detailed_pressure.expired_bucket_object_scan_debt > 0);
        assert!(!pressure.live_block_summaries_measured);
        assert_eq!(pressure.expired_bucket_object_scan_debt, 0);
        assert!(detailed_plan.bucket_summaries.is_some());
        assert!(plan.bucket_summaries.is_none());
        detailed_pressure.live_block_summaries_measured = false;
        detailed_pressure.expired_bucket_object_scan_debt = 0;
        detailed_plan.bucket_summaries = None;
        assert_eq!(pressure, detailed_pressure);
        assert_eq!(plan, detailed_plan);
        assert!(detailed_work > execution_work,
            "detailed={detailed_work} execution={execution_work}");
    }
}

#[test]
fn explicit_manager_and_lifecycle_requests_still_return_full_diagnostics() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path());
    let report = runtime.run_storage_manager_once(1, options());
    assert!(report.status.ok);
    let lifecycle = report
        .lifecycle_report
        .expect("explicit manager diagnostics");
    assert!(
        !lifecycle
            .storage_index_snapshot
            .page_index_entry_samples
            .is_empty()
    );
    let response = runtime.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        ..Default::default()
    });
    assert!(response.status.ok);
    assert!(
        !response
            .report
            .storage_index_snapshot
            .page_index_entry_samples
            .is_empty()
    );
}

#[test]
fn background_compaction_task_preserves_counters_and_error_status() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path());
    let drain = runtime.engine().live_block_slab_ids(1);
    assert!(!drain.is_empty());
    let status = task_execution::run_compaction_background_draining(
        &runtime.inner,
        1,
        drain.into_iter().collect(),
    );
    assert!(status.ok, "{status:?}");
    assert_eq!(runtime.stats().compaction_runs, 1);
    let status =
        task_execution::run_compaction_background_draining(&runtime.inner, 99, BTreeSet::new());
    assert!(!status.ok);
    assert_eq!(status.code, "shard_not_loaded");
    assert_eq!(runtime.stats().compaction_runs, 2);
}

/// THE DRAINING STAGE ALWAYS RUNS THE SLICED PASS. A sliced pass pauses between slices, so a boundary
/// callback that never fires is a pass that was not sliced. The whole round it replaced held the
/// shard write guard for the whole stage; the control is the whole-round entry point itself, which
/// must NOT fire the callback, so the instrument can tell the two apart.
#[test]
fn the_draining_stage_always_runs_the_sliced_pass() {
    let boundaries = std::rc::Rc::new(std::cell::Cell::new(0usize));
    let count = |boundaries: &std::rc::Rc<std::cell::Cell<usize>>| {
        let boundaries = std::rc::Rc::clone(boundaries);
        crate::engine::set_slice_boundary_for_test(Some(Box::new(move |_| {
            boundaries.set(boundaries.get() + 1);
        })));
    };

    // More keys than one slice holds, or the pass is a single slice and never pauses.
    let keys = crate::engine::COMPACTION_SLICE_KEYS * 3;
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_keys(dir.path(), keys);
    let drain: BTreeSet<u64> = runtime.engine().live_block_slab_ids(1).into_iter().collect();
    count(&boundaries);
    let status = task_execution::run_compaction_background_draining(&runtime.inner, 1, drain);
    assert!(status.ok, "{status:?}");
    assert!(boundaries.get() > 0, "the draining stage did not pause between slices: it ran a whole round");

    // Control: the whole-round entry point does not pause, so a zero above would not have meant
    // 'the stage ran one'.
    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded_keys(dir.path(), keys);
    let drain: BTreeSet<u64> = runtime.engine().live_block_slab_ids(1).into_iter().collect();
    boundaries.set(0);
    runtime
        .engine()
        .compact_shard_blocks_draining_background(1, drain)
        .unwrap();
    crate::engine::set_slice_boundary_for_test(None);
    assert_eq!(boundaries.get(), 0, "the whole-round control fired the boundary callback");
}

/// A compaction step returns when its budget is spent, not when the pass is over: with room for one
/// slice it moves one slice and reports `More`, the pass stays in flight, and stepping on finishes
/// it with every key still readable. A step that ignored its budget would finish the pass in one
/// call and report `Done` the first time.
#[test]
fn a_compaction_step_returns_when_its_budget_is_spent_and_the_pass_resumes() {
    use super::compaction_stage::{CompactionCursor, CompactionStage};
    use super::round_budget::{BoundedStage, StageBudget, StageContext, StageProgress};

    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path());
    runtime.engine().block_store().roll_slab().unwrap();
    for i in 0..16 {
        assert!(
            runtime
                .engine()
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("key-{i}"),
                        value: vec![42; 96]
                    },
                })
                .status
                .ok
        );
    }
    let options = StorageManagerOptions {
        enable_block_compaction: true,
        stale_block_slab_pressure: 1,
        reclaimable_physical_bytes_pressure: 1,
        ..options()
    };
    let context = StageContext { options: &options, plan: None };
    let mut stage = CompactionStage::new(&runtime);
    stage.keys_per_slice = 2;
    let mut cursor = CompactionCursor::default();

    // Room for one slice: one slice is moved and the pass is left in flight.
    let mut budget = StageBudget::new(std::time::Duration::from_secs(60), 1);
    let first = stage.step(1, &mut cursor, &mut budget, &context);
    assert_eq!(first.progress, StageProgress::More, "the step ran past its budget");
    assert_eq!(first.work_done, 2, "a step with room for one slice moved more or less than one");
    assert!(first.status.ok, "{:?}", first.status);
    assert!(
        runtime.engine().compaction_pass_in_flight(1),
        "a step that returned More must leave the pass in flight"
    );

    // Stepping on finishes the pass, in more than one further step.
    let mut steps = 1;
    loop {
        let mut budget = StageBudget::new(std::time::Duration::from_secs(60), 1);
        let step = stage.step(1, &mut cursor, &mut budget, &context);
        assert!(step.status.ok, "{:?}", step.status);
        steps += 1;
        if step.progress != StageProgress::More {
            assert_eq!(step.progress, StageProgress::Done);
            break;
        }
        assert!(steps < 100, "the pass never finished");
    }
    assert!(steps > 2, "the pass was not spread over steps: {steps}");
    assert!(!runtime.engine().compaction_pass_in_flight(1));
    for i in 0..32 {
        let response = runtime.engine().execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: format!("key-{i}") },
        });
        let expected = if i < 16 { vec![42; 96] } else { vec![i as u8; 96] };
        assert!(
            matches!(response.response, CommandResponse::Bytes { value: Some(value) } if value == expected),
            "key-{i} lost its value across the stepped pass"
        );
    }

    // Nothing to compact: a step with compaction off is Idle and starts no pass.
    let off = StorageManagerOptions { enable_block_compaction: false, ..options.clone() };
    let mut idle_cursor = CompactionCursor::default();
    let mut budget = StageBudget::new(std::time::Duration::from_secs(60), 200);
    let idle = stage.step(1, &mut idle_cursor, &mut budget, &StageContext { options: &off, plan: None });
    assert_eq!(idle.progress, StageProgress::Idle);
}

/// A tick-mode compaction step is exported under the same stage name a whole round uses, so the
/// scrape shows its busy time and longest step. A step that left the timing table alone ran
/// compaction and exported nothing for it.
#[test]
fn a_tick_compaction_step_is_timed_under_the_compaction_stage_name() {
    use super::round_budget::ShardRoundState;

    let dir = tempfile::tempdir().unwrap();
    let runtime = seeded(dir.path());
    runtime.engine().block_store().roll_slab().unwrap();
    for i in 0..16 {
        assert!(
            runtime
                .engine()
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet { key: format!("key-{i}"), value: vec![42; 96] },
                })
                .status
                .ok
        );
    }
    let options = StorageManagerOptions {
        enable_block_compaction: true,
        stale_block_slab_pressure: 1,
        reclaimable_physical_bytes_pressure: 1,
        ..options()
    };
    assert!(
        runtime.stats().storage_manager_stage_timing.get("compact_pages").is_none(),
        "nothing has run yet"
    );
    let period = std::time::Duration::from_secs(30);
    let mut state = ShardRoundState::new(std::time::Instant::now(), period);
    let report = runtime.run_compaction_step(1, &options, &mut state, period);
    assert!(report.status.ok, "{:?}", report.status);
    assert!(
        report.executed_stages.iter().any(|stage| stage == "compact_pages"),
        "the step did not run compaction: {report:?}"
    );
    let timing = runtime
        .stats()
        .storage_manager_stage_timing
        .get("compact_pages")
        .cloned()
        .expect("a tick compaction step must be timed under the stage name the scrape exports");
    assert_eq!(timing.rounds_executed, 1);
    assert!(timing.total_us > 0 && timing.max_us == timing.total_us, "{timing:?}");
}

#[test]
fn timer_manager_compacts_without_a_partial_response_while_explicit_calls_keep_details() {
    for detailed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let runtime = seeded(dir.path());
        runtime.engine().block_store().roll_slab().unwrap();
        for i in 0..16 {
            assert!(
                runtime
                    .engine()
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: format!("key-{i}"),
                            value: vec![42; 96]
                        },
                    })
                    .status
                    .ok
            );
        }
        let report = runtime.run_storage_manager_once_with_report_details(
            1,
            StorageManagerOptions {
                enable_block_compaction: true,
                stale_block_slab_pressure: 1,
                reclaimable_physical_bytes_pressure: 1,
                ..options()
            },
            detailed,
        );
        assert!(report.status.ok, "{:?}", report.status);
        assert!(
            report
                .executed_stages
                .iter()
                .any(|stage| stage == "compact_pages"),
            "{report:?}"
        );
        assert_eq!(runtime.stats().compaction_runs, 1);
        assert_eq!(runtime.stats().storage_manager_compact_runs, 1);
        if detailed {
            let compaction = report
                .compaction_report
                .expect("explicit compaction report");
            assert!(compaction.rewritten_object_blocks > 0);
            assert!(!compaction.model_layouts.is_empty());
        } else {
            assert!(report.compaction_report.is_none());
        }
    }
}
