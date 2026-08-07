//! Storage-manager runtime scheduling/reporting helpers, extracted from data_node.rs.

use super::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub(super) fn storage_manager_runtime_initial_report(
    options: &StorageManagerRuntimeOptions,
) -> StorageManagerRuntimeReport {
    StorageManagerRuntimeReport {
        running: true,
        paused: false,
        stopped: false,
        interval_ms: options.interval_ms,
        jitter_percent: options.jitter_percent,
        current_backoff_ms: options.initial_backoff_ms,
        last_delay_ms: options.interval_ms,
        phase_prepare_enabled: options.request.enable_prepare,
        phase_wal_reclaim_enabled: options.request.enable_oplog_reclaim,
        phase_expire_enabled: options.request.enable_expire,
        phase_evict_enabled: options.request.enable_evict,
        phase_page_gc_enabled: options.request.enable_page_reclaim,
        phase_compaction_enabled: options.request.enable_page_compaction,
        phase_index_gc_enabled: options.request.enable_index_gc,
        bounded_max_dump_buckets_per_round: options.request.max_dump_buckets_per_round,
        configured_follower_cursor_count: options.request.follower_replay_cursors.len(),
        configured_raft_snapshot_ref_count: options.request.raft_snapshot_refs.len(),
        configured_page_gc_raft_install_floor_slab_id: options
            .request
            .page_gc_raft_install_floor_slab_id,
        ..StorageManagerRuntimeReport::default()
    }
}

pub(super) fn wait_for_storage_manager_cycle_completion(
    runtime: &DataNodeRuntime,
    job_id: u64,
    timeout_ms: u64,
) -> Option<StorageManagerCycleReport> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(1));
    loop {
        if let Some(status) = runtime.job_status(job_id) {
            if status.finished_at_ms.is_some() {
                if let Some(DataNodeTaskOutput::StorageManager(response)) = status.output {
                    return Some(response.report);
                }
                return None;
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

pub(super) fn apply_storage_manager_cycle_to_runtime_report(
    report: &mut StorageManagerRuntimeReport,
    cycle: StorageManagerCycleReport,
) {
    let mut selected_buckets = BTreeSet::new();
    let mut skipped_reasons = BTreeSet::new();
    let mut phase_blockers = BTreeSet::new();
    let mut bytes_reclaimed = 0u64;
    let mut pressure_before = 0u64;
    let mut pressure_after = 0u64;
    let mut wal_floor_sequence = 0u64;
    let mut index_log_floor_sequence = 0u64;
    let mut retention_blockers = 0usize;
    for stage in &cycle.stages {
        selected_buckets.extend(stage.selected_buckets.iter().copied());
        if stage.skipped && !stage.reason.is_empty() {
            skipped_reasons.insert(stage.reason.clone());
        }
        if !stage.skipped_reason.is_empty() {
            skipped_reasons.insert(stage.skipped_reason.clone());
        }
        if stage.retention_blockers > 0 {
            phase_blockers.insert(format!("{}:retention_blockers", stage.stage));
        }
        for error in &stage.errors {
            phase_blockers.insert(error.clone());
        }
        bytes_reclaimed = bytes_reclaimed
            .saturating_add(stage.bytes_reclaimed)
            .saturating_add(stage.page_bytes_reclaimed)
            .saturating_add(stage.cache_disk_bytes_removed)
            .saturating_add(stage.before_bytes.saturating_sub(stage.after_bytes));
        pressure_before = pressure_before.max(stage.pressure_before);
        pressure_after = pressure_after.max(stage.pressure_after);
        wal_floor_sequence = wal_floor_sequence.max(stage.wal_floor_sequence);
        index_log_floor_sequence = index_log_floor_sequence.max(stage.index_log_floor_sequence);
        retention_blockers = retention_blockers.max(stage.retention_blockers);
    }
    report.last_pressure_snapshot = Some(StorageManagerPressureSnapshot {
        shard_id: cycle.shard_id,
        dirty_bucket_count: cycle.pressure_snapshot.dirty_bucket_count,
        selected_dirty_bucket_count: cycle.plan.selected_dump_buckets.len(),
        undumped_oplog_records: cycle.pressure_snapshot.undumped_wal_records,
        wal_bytes: cycle.pressure_snapshot.wal_bytes,
        index_log_bytes: cycle.pressure_snapshot.index_log_bytes,
        stale_page_slab_count: cycle.plan.stale_page_slab_ids.len(),
        reclaim_candidate_count: cycle.plan.reclaim_candidates.len(),
        reclaimable_physical_bytes: cycle.plan.reclaimable_physical_bytes,
        page_slab_stale_density_basis_points: cycle
            .pressure_snapshot
            .page_slab_stale_density_basis_points,
        cache_memory_bytes: cycle.pressure_snapshot.memory_cache_bytes,
        cache_disk_bytes: cycle.pressure_snapshot.disk_cache_bytes,
        memory_cache_pressure_score: cycle.pressure_snapshot.memory_cache_pressure_score,
        expired_bucket_object_scan_debt: cycle.pressure_snapshot.expired_bucket_object_scan_debt,
        delayed_destroy_slab_count: cycle.pressure_snapshot.delayed_destroy_slab_count,
        delayed_destroy_bytes: cycle.pressure_snapshot.delayed_destroy_bytes,
        follower_cursor_retention_blockers: cycle
            .pressure_snapshot
            .follower_cursor_retention_blockers,
        raft_snapshot_retention_blockers: cycle.pressure_snapshot.raft_snapshot_retention_blockers,
        compaction_debt_model_count: cycle.pressure_snapshot.compaction_debt_model_count,
        compaction_debt_score: cycle.pressure_snapshot.compaction_debt_score,
        total_pressure_score: cycle.pressure_snapshot.total_pressure_score,
        background_queue_depth: 0,
        foreground_queue_depth: 0,
    });
    report.last_phase_reports = cycle.stages.clone();
    report.last_selected_buckets = selected_buckets.into_iter().collect();
    report.last_skipped_reasons = skipped_reasons.into_iter().collect();
    report.last_bytes_reclaimed = bytes_reclaimed;
    report.last_pressure_before = pressure_before.max(cycle.pressure_snapshot.total_pressure_score);
    report.last_pressure_after = pressure_after;
    report.last_wal_floor_sequence = wal_floor_sequence;
    report.last_index_log_floor_sequence = index_log_floor_sequence;
    report.last_retention_blockers = retention_blockers;
    report.last_phase_blockers = phase_blockers.into_iter().collect();
    report.last_completed_cycle = Some(cycle);
}

pub(super) fn storage_manager_runtime_delay_ms(
    options: &StorageManagerRuntimeOptions,
    round: u64,
    current_backoff_ms: u64,
) -> u64 {
    let base = options.interval_ms.max(1).max(current_backoff_ms);
    let jitter_bound = base.saturating_mul(options.jitter_percent.min(100) as u64) / 100;
    if jitter_bound == 0 {
        return base;
    }
    let seed = options
        .request
        .shard_id
        .wrapping_mul(1_099_511_628_211)
        .wrapping_add(round.wrapping_mul(1_469_598_103_934_665_603));
    base.saturating_add(seed % jitter_bound.saturating_add(1))
}

pub(super) fn storage_manager_runtime_next_backoff_ms(
    current_backoff_ms: u64,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
) -> u64 {
    let floor = initial_backoff_ms.max(1);
    let ceiling = max_backoff_ms.max(floor);
    if current_backoff_ms < floor {
        floor
    } else {
        current_backoff_ms.saturating_mul(2).min(ceiling)
    }
}

pub(super) fn sleep_until_storage_manager_runtime_round(stop: &AtomicBool, delay_ms: u64) -> bool {
    let deadline = Instant::now() + Duration::from_millis(delay_ms.max(1));
    while Instant::now() < deadline {
        if stop.load(Ordering::Relaxed) {
            return true;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(remaining.min(Duration::from_millis(10)));
    }
    stop.load(Ordering::Relaxed)
}
