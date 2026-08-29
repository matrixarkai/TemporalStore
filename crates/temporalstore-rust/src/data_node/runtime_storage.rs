// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! DataNodeRuntime storage-manager scheduling/lifecycle methods, extracted from data_node.rs.

use super::*;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

impl DataNodeRuntime {
    pub fn storage_lifecycle_plan(&self, request: StorageLifecycleRequest) -> StorageLifecyclePlan {
        self.inner.engine.storage_lifecycle_plan(request)
    }

    pub fn storage_manager_pressure_snapshot(
        &self,
        shard_id: ShardId,
        options: &StorageManagerOptions,
    ) -> (StorageManagerPressureSnapshot, StorageLifecyclePlan) {
        let plan = self
            .inner
            .engine
            .storage_lifecycle_plan(StorageLifecycleRequest {
                shard_id,
                selected_dump_buckets: Vec::new(),
                max_dump_buckets_per_round: options.max_dump_buckets_per_round,
                min_undumped_wal_records: options.min_undumped_wal_records,
                purge_delayed_destroy: options.enable_page_gc,
                prune_bucket_dump_manifests: options.enable_index_gc,
                roll_forward_bucket_dump_installs: options.enable_index_gc,
                follower_replay_cursors: Vec::new(),
                page_gc_shared_store_cursors: Vec::new(),
                page_gc_raft_snapshot_refs: Vec::new(),
                page_gc_checkpoint_floor_slab_id: None,
                page_gc_raft_install_floor_slab_id: None,
                page_gc_delayed_destroy_grace_ms: 0,
                invalidate_cache: false,
                warm_cache: false,
            });
        let cache = self.inner.engine.cache().stats();
        let log_pressure = self.inner.engine.storage_log_compatibility_report(shard_id);
        let queue = self
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned");
        (
            StorageManagerPressureSnapshot {
                shard_id,
                dirty_bucket_count: plan.dirty_buckets.len(),
                selected_dirty_bucket_count: plan.selected_dump_buckets.len(),
                undumped_wal_records: plan.undumped_wal_records,
                wal_bytes: plan.undumped_wal_records,
                index_log_bytes: log_pressure.index_log_bytes,
                stale_page_slab_count: plan.stale_page_slab_ids.len(),
                reclaim_candidate_count: plan.reclaim_candidates.len(),
                reclaimable_physical_bytes: plan.reclaimable_physical_bytes,
                page_slab_stale_density_basis_points: 0,
                cache_memory_bytes: cache.memory_bytes,
                cache_disk_bytes: cache.disk_bytes,
                memory_cache_pressure_score: cache
                    .memory_bytes
                    .saturating_add(cache.disk_bytes)
                    .saturating_add(cache.async_writeback_queue_bytes)
                    .saturating_add(cache.async_writeback_queue_depth),
                expired_bucket_object_scan_debt: plan.bucket_summaries.len(),
                delayed_destroy_slab_count: plan.delayed_destroy_page_slab_ids.len(),
                delayed_destroy_bytes: plan.reclaimable_physical_bytes,
                follower_cursor_retention_blockers: 0,
                raft_snapshot_retention_blockers: 0,
                compaction_debt_model_count: usize::from(!plan.stale_page_slab_ids.is_empty()),
                compaction_debt_score: plan.reclaimable_physical_bytes,
                total_pressure_score: plan
                    .dirty_buckets
                    .len()
                    .saturating_add(plan.selected_dump_buckets.len())
                    as u64
                    + plan.undumped_wal_records
                    + log_pressure.index_log_bytes
                    + plan.reclaimable_physical_bytes
                    + cache.memory_bytes
                    + cache.disk_bytes,
                background_queue_depth: queue.background_queued_total,
                foreground_queue_depth: queue
                    .queued_total
                    .saturating_sub(queue.background_queued_total),
            },
            plan,
        )
    }

    pub fn run_storage_manager_once(
        &self,
        shard_id: ShardId,
        options: StorageManagerOptions,
    ) -> StorageManagerLoopReport {
        let (pressure, lifecycle_plan) = self.storage_manager_pressure_snapshot(shard_id, &options);
        let mut executed_stages = Vec::new();
        let mut skipped_stages = Vec::new();
        let mut pressure_decisions = Vec::new();
        let mut lifecycle_report = None;
        let mut compaction_report = None;
        let mut gc_report = None;
        let mut expired_records_removed = 0;
        let mut status = Status::ok();

        if options.enable_prepare {
            executed_stages.push("prepare".to_string());
            // Pre-allocate the next data slab so a client append never has to roll inline.
            //
            // Rolling costs several fsyncs plus a slab-directory scan (see
            // LocalBlockStore::prepare_next_slab). Left to the write path it lands on one
            // unlucky write as a latency outlier unrelated to that write's size. Doing it
            // here -- the stage that already exists and is already named "prepare" -- is what
            // this design does with PrepareNewZone in the same position of
            // its background cycle.
            //
            // A no-op while the active slab has room, so this costs a lock and a comparison
            // on the overwhelming majority of cycles. A failure is recorded and does not stop
            // the cycle: the inline roll in `append` remains the fallback, so the only
            // consequence is that the next append pays for the roll, exactly as it does today.
            match self.inner.engine.block_store().prepare_next_slab() {
                Ok(Some(roll)) => {
                    tracing::debug!(
                        previous_page_slab_id = roll.previous_page_slab_id,
                        new_page_slab_id = roll.new_page_slab_id,
                        "storage manager pre-allocated the next data slab"
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "storage manager could not pre-allocate the next data slab;                          the next append will roll inline"
                    );
                }
            }
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_prepare_runs += 1;
        } else {
            skipped_stages.push("prepare_disabled".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "prepare",
            options.enable_prepare,
            true,
            options.enable_prepare,
            vec![
                storage_manager_pressure_signal(
                    "dirty_slot_count",
                    pressure.dirty_bucket_count as u64,
                    options.dirty_bucket_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "stale_page_segment_count",
                    pressure.stale_page_slab_count as u64,
                    options.stale_page_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "background_queue_depth",
                    pressure.background_queue_depth as u64,
                    1,
                ),
            ],
            vec!["continuous_prepare_plan_refresh".to_string()],
            (!options.enable_prepare).then(|| "prepare_disabled".to_string()),
        ));

        let dump_pressure = pressure.dirty_bucket_count >= options.dirty_bucket_pressure.max(1)
            || pressure.undumped_wal_records >= options.min_undumped_wal_records.max(1);
        let cache_pressure = pressure.cache_memory_bytes
            >= options.cache_memory_bytes_pressure.max(1)
            || pressure.cache_disk_bytes >= options.cache_disk_bytes_pressure.max(1);
        let stale_page_pressure = pressure.stale_page_slab_count
            >= options.stale_page_slab_pressure.max(1)
            || pressure.reclaim_candidate_count >= options.stale_page_slab_pressure.max(1)
            || pressure.reclaimable_physical_bytes
                >= options.reclaimable_physical_bytes_pressure.max(1);

        if options.enable_wal_reclaim && dump_pressure {
            let response = self.apply_storage_lifecycle(StorageLifecycleRequest {
                shard_id,
                selected_dump_buckets: Vec::new(),
                max_dump_buckets_per_round: options.max_dump_buckets_per_round,
                min_undumped_wal_records: options.min_undumped_wal_records,
                purge_delayed_destroy: false,
                prune_bucket_dump_manifests: false,
                roll_forward_bucket_dump_installs: false,
                follower_replay_cursors: Vec::new(),
                page_gc_shared_store_cursors: Vec::new(),
                page_gc_raft_snapshot_refs: Vec::new(),
                page_gc_checkpoint_floor_slab_id: None,
                page_gc_raft_install_floor_slab_id: None,
                page_gc_delayed_destroy_grace_ms: 0,
                invalidate_cache: false,
                warm_cache: false,
            });
            if let Some(manifest) = response.report.dump_manifest.as_ref() {
                clear_dirty_shard_buckets(
                    &self.inner.dirty,
                    &self.inner.engine,
                    shard_id,
                    &manifest.bucket_ids,
                );
                let mut stats = self
                    .inner
                    .stats
                    .lock()
                    .expect("runtime stats lock poisoned");
                stats.dump_runs += 1;
                stats.storage_manager_reclaim_wal_runs += 1;
            }
            lifecycle_report = Some(response.report);
            executed_stages.push("reclaim_wal".to_string());
        } else if !options.enable_wal_reclaim {
            skipped_stages.push("reclaim_wal_disabled".to_string());
        } else {
            skipped_stages.push("reclaim_wal_no_pressure".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "reclaim_wal",
            options.enable_wal_reclaim,
            dump_pressure,
            options.enable_wal_reclaim && dump_pressure,
            vec![
                storage_manager_pressure_signal(
                    "dirty_slot_count",
                    pressure.dirty_bucket_count as u64,
                    options.dirty_bucket_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "undumped_wal_records",
                    pressure.undumped_wal_records,
                    options.min_undumped_wal_records.max(1),
                ),
            ],
            storage_manager_trigger_reasons(&[
                (
                    pressure.dirty_bucket_count >= options.dirty_bucket_pressure.max(1),
                    "dirty_slot_pressure",
                ),
                (
                    pressure.undumped_wal_records >= options.min_undumped_wal_records.max(1),
                    "undumped_wal_pressure",
                ),
            ]),
            storage_manager_skip_reason(
                options.enable_wal_reclaim,
                dump_pressure,
                "reclaim_wal",
            ),
        ));

        if options.enable_memory_reclaim && cache_pressure {
            let response = self.apply_storage_lifecycle(StorageLifecycleRequest {
                shard_id,
                selected_dump_buckets: Vec::new(),
                max_dump_buckets_per_round: 0,
                min_undumped_wal_records: 0,
                purge_delayed_destroy: false,
                prune_bucket_dump_manifests: false,
                roll_forward_bucket_dump_installs: false,
                follower_replay_cursors: Vec::new(),
                page_gc_shared_store_cursors: Vec::new(),
                page_gc_raft_snapshot_refs: Vec::new(),
                page_gc_checkpoint_floor_slab_id: None,
                page_gc_raft_install_floor_slab_id: None,
                page_gc_delayed_destroy_grace_ms: 0,
                invalidate_cache: true,
                warm_cache: false,
            });
            let mut stats = self
                .inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned");
            stats.storage_manager_reclaim_memory_runs += 1;
            lifecycle_report = Some(response.report);
            executed_stages.push("reclaim_memory".to_string());
        } else if !options.enable_memory_reclaim {
            skipped_stages.push("reclaim_memory_disabled".to_string());
        } else {
            skipped_stages.push("reclaim_memory_no_pressure".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "reclaim_memory",
            options.enable_memory_reclaim,
            cache_pressure,
            options.enable_memory_reclaim && cache_pressure,
            vec![
                storage_manager_pressure_signal(
                    "cache_memory_bytes",
                    pressure.cache_memory_bytes,
                    options.cache_memory_bytes_pressure.max(1),
                ),
                storage_manager_pressure_signal(
                    "cache_disk_bytes",
                    pressure.cache_disk_bytes,
                    options.cache_disk_bytes_pressure.max(1),
                ),
            ],
            storage_manager_trigger_reasons(&[
                (
                    pressure.cache_memory_bytes >= options.cache_memory_bytes_pressure.max(1),
                    "cache_memory_pressure",
                ),
                (
                    pressure.cache_disk_bytes >= options.cache_disk_bytes_pressure.max(1),
                    "cache_disk_pressure",
                ),
            ]),
            storage_manager_skip_reason(
                options.enable_memory_reclaim,
                cache_pressure,
                "reclaim_memory",
            ),
        ));

        if options.enable_expire {
            expired_records_removed = self.sweep_expired_records();
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_expire_runs += 1;
            executed_stages.push("expire".to_string());
        } else {
            skipped_stages.push("expire_disabled".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "expire",
            options.enable_expire,
            expired_records_removed > 0,
            options.enable_expire,
            vec![storage_manager_pressure_signal(
                "expired_records_removed",
                expired_records_removed as u64,
                1,
            )],
            if expired_records_removed > 0 {
                vec!["expired_records_present".to_string()]
            } else {
                vec!["continuous_expiry_sweep".to_string()]
            },
            (!options.enable_expire).then(|| "expire_disabled".to_string()),
        ));

        if options.enable_page_gc && stale_page_pressure {
            let retain_page_slabs_from_id = lifecycle_plan
                .stale_page_slab_ids
                .iter()
                .min()
                .map(|slab_id| slab_id.saturating_add(1));
            let response = run_gc_inner(
                &self.inner,
                GcRequest {
                    shard_id,
                    retain_wal_from_sequence: lifecycle_report
                        .as_ref()
                        .and_then(|report| report.dump_manifest.as_ref())
                        .map(|manifest| manifest.wal_sequence),
                    retain_index_log_from_sequence: lifecycle_report
                        .as_ref()
                        .and_then(|report| report.dump_manifest.as_ref())
                        .map(|manifest| manifest.index_log_sequence),
                    retain_page_slabs_from_id,
                },
            );
            if !response.status.ok {
                status = response.status.clone();
            }
            gc_report = Some(response);
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_reclaim_page_runs += 1;
            executed_stages.push("reclaim_page".to_string());
        } else if !options.enable_page_gc {
            skipped_stages.push("reclaim_page_disabled".to_string());
        } else {
            skipped_stages.push("reclaim_page_no_pressure".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "reclaim_page",
            options.enable_page_gc,
            stale_page_pressure,
            options.enable_page_gc && stale_page_pressure,
            vec![
                storage_manager_pressure_signal(
                    "stale_page_segment_count",
                    pressure.stale_page_slab_count as u64,
                    options.stale_page_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "reclaim_candidate_count",
                    pressure.reclaim_candidate_count as u64,
                    options.stale_page_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "reclaimable_physical_bytes",
                    pressure.reclaimable_physical_bytes,
                    options.reclaimable_physical_bytes_pressure.max(1),
                ),
            ],
            storage_manager_trigger_reasons(&[
                (
                    pressure.stale_page_slab_count >= options.stale_page_slab_pressure.max(1),
                    "stale_page_segment_pressure",
                ),
                (
                    pressure.reclaim_candidate_count >= options.stale_page_slab_pressure.max(1),
                    "reclaim_candidate_pressure",
                ),
                (
                    pressure.reclaimable_physical_bytes
                        >= options.reclaimable_physical_bytes_pressure.max(1),
                    "reclaimable_physical_bytes_pressure",
                ),
            ]),
            storage_manager_skip_reason(
                options.enable_page_gc,
                stale_page_pressure,
                "reclaim_page",
            ),
        ));

        if options.enable_page_compaction && stale_page_pressure {
            let response = run_compaction_inner(&self.inner, CompactionRequest { shard_id });
            if !response.status.ok {
                status = response.status.clone();
            }
            compaction_report = Some(response);
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_compact_runs += 1;
            executed_stages.push("compact_pages".to_string());
        } else if !options.enable_page_compaction {
            skipped_stages.push("compact_pages_disabled".to_string());
        } else {
            skipped_stages.push("compact_pages_no_pressure".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "compact_pages",
            options.enable_page_compaction,
            stale_page_pressure,
            options.enable_page_compaction && stale_page_pressure,
            vec![
                storage_manager_pressure_signal(
                    "stale_page_segment_count",
                    pressure.stale_page_slab_count as u64,
                    options.stale_page_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "reclaim_candidate_count",
                    pressure.reclaim_candidate_count as u64,
                    options.stale_page_slab_pressure.max(1) as u64,
                ),
                storage_manager_pressure_signal(
                    "reclaimable_physical_bytes",
                    pressure.reclaimable_physical_bytes,
                    options.reclaimable_physical_bytes_pressure.max(1),
                ),
            ],
            storage_manager_trigger_reasons(&[
                (
                    pressure.stale_page_slab_count >= options.stale_page_slab_pressure.max(1),
                    "stale_page_segment_pressure",
                ),
                (
                    pressure.reclaim_candidate_count >= options.stale_page_slab_pressure.max(1),
                    "reclaim_candidate_pressure",
                ),
                (
                    pressure.reclaimable_physical_bytes
                        >= options.reclaimable_physical_bytes_pressure.max(1),
                    "reclaimable_physical_bytes_pressure",
                ),
            ]),
            storage_manager_skip_reason(
                options.enable_page_compaction,
                stale_page_pressure,
                "compact_pages",
            ),
        ));

        let index_gc_pressure = lifecycle_plan.reasons.iter().any(|reason| {
            reason == "slot_dump_manifest_prune" || reason == "slot_dump_install_roll_forward_check"
        });
        if options.enable_index_gc {
            let response = self.apply_storage_lifecycle(StorageLifecycleRequest {
                shard_id,
                selected_dump_buckets: Vec::new(),
                max_dump_buckets_per_round: 0,
                min_undumped_wal_records: 0,
                purge_delayed_destroy: true,
                prune_bucket_dump_manifests: true,
                roll_forward_bucket_dump_installs: true,
                follower_replay_cursors: Vec::new(),
                page_gc_shared_store_cursors: Vec::new(),
                page_gc_raft_snapshot_refs: Vec::new(),
                page_gc_checkpoint_floor_slab_id: None,
                page_gc_raft_install_floor_slab_id: None,
                page_gc_delayed_destroy_grace_ms: 0,
                invalidate_cache: false,
                warm_cache: false,
            });
            lifecycle_report = Some(response.report);
            self.inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .storage_manager_index_gc_runs += 1;
            executed_stages.push("reclaim_index".to_string());
        } else {
            skipped_stages.push("reclaim_index_disabled".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "reclaim_index",
            options.enable_index_gc,
            index_gc_pressure,
            options.enable_index_gc,
            vec![
                storage_manager_pressure_signal(
                    "manifest_prune_reasons",
                    lifecycle_plan
                        .reasons
                        .iter()
                        .filter(|reason| reason.as_str() == "slot_dump_manifest_prune")
                        .count() as u64,
                    1,
                ),
                storage_manager_pressure_signal(
                    "install_roll_forward_reasons",
                    lifecycle_plan
                        .reasons
                        .iter()
                        .filter(|reason| reason.as_str() == "slot_dump_install_roll_forward_check")
                        .count() as u64,
                    1,
                ),
            ],
            if index_gc_pressure {
                lifecycle_plan
                    .reasons
                    .iter()
                    .filter(|reason| {
                        reason.as_str() == "slot_dump_manifest_prune"
                            || reason.as_str() == "slot_dump_install_roll_forward_check"
                    })
                    .cloned()
                    .collect()
            } else {
                vec!["continuous_index_gc_safety_check".to_string()]
            },
            (!options.enable_index_gc).then(|| "reclaim_index_disabled".to_string()),
        ));

        // Gather what the stage has always claimed to gather. Deliberately a snapshot and not
        // a reset: `durability_metrics` documents its counters as process-wide and monotonic,
        // and tests reset them to isolate a measurement window -- a background cycle clearing
        // them would pull the floor out from under anyone diffing two points in time.
        let metrics_reap = options.enable_metrics_reap.then(|| {
            let durability_barriers: std::collections::BTreeMap<String, u64> =
                crate::durability_metrics::snapshot()
                    .into_iter()
                    .map(|(site, count)| (site.to_string(), count))
                    .collect();
            StorageMetricsReapReport {
                durability_barriers_total: durability_barriers.values().copied().sum(),
                durability_barriers,
                wal: self.inner.engine.write_ahead_log_store().stats(shard_id),
            }
        });
        if metrics_reap.is_some() {
            executed_stages.push("reap_metrics".to_string());
        } else {
            skipped_stages.push("reap_metrics_disabled".to_string());
        }
        pressure_decisions.push(storage_manager_pressure_decision(
            "reap_metrics",
            options.enable_metrics_reap,
            true,
            options.enable_metrics_reap,
            vec![
                storage_manager_pressure_signal(
                    "foreground_queue_depth",
                    pressure.foreground_queue_depth as u64,
                    1,
                ),
                storage_manager_pressure_signal(
                    "background_queue_depth",
                    pressure.background_queue_depth as u64,
                    1,
                ),
            ],
            vec!["continuous_metrics_reap".to_string()],
            (!options.enable_metrics_reap).then(|| "reap_metrics_disabled".to_string()),
        ));

        self.inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .storage_manager_loops += 1;

        StorageManagerLoopReport {
            shard_id,
            pressure,
            executed_stages,
            skipped_stages,
            pressure_decisions,
            lifecycle_plan,
            lifecycle_report,
            expired_records_removed,
            metrics_reap,
            compaction_report,
            gc_report,
            status,
        }
    }

    pub fn start_storage_manager_scheduler(
        &self,
        interval: Duration,
        shard_id: ShardId,
        options: StorageManagerOptions,
    ) -> StorageLifecycleScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                runtime.run_storage_manager_once(shard_id, options.clone());
            }
        });
        StorageLifecycleScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn storage_production_readiness_report(
        &self,
        shard_id: ShardId,
    ) -> StorageProductionReadinessReport {
        self.inner
            .engine
            .storage_production_readiness_report(shard_id)
    }

    pub fn storage_production_readiness_report_with_policy(
        &self,
        shard_id: ShardId,
        policy: StorageProductionReadinessPolicy,
    ) -> StorageProductionReadinessReport {
        self.inner
            .engine
            .storage_production_readiness_report_with_policy(shard_id, policy)
    }

    pub fn apply_storage_lifecycle(
        &self,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleResponse {
        let report = self.inner.engine.apply_storage_lifecycle(request);
        self.inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .storage_lifecycle_runs += 1;
        StorageLifecycleResponse {
            status: Status::ok(),
            report,
        }
    }

    pub fn start_dirty_dump_scheduler(
        &self,
        interval: Duration,
        controller: RequestController,
    ) -> DirtyDumpScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                runtime.schedule_dirty_shard_dumps(controller);
            }
        });
        DirtyDumpScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn start_storage_lifecycle_scheduler(
        &self,
        interval: Duration,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                runtime.apply_storage_lifecycle(request.clone());
            }
        });
        StorageLifecycleScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn start_storage_manager_cycle_scheduler(
        &self,
        interval: Duration,
        request: StorageManagerCycleRequest,
        controller: RequestController,
    ) -> StorageManagerScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                let should_submit = !runtime
                    .inner
                    .queue
                    .lock()
                    .expect("runtime queue lock poisoned")
                    .has_pending_storage_manager(request.shard_id);
                if should_submit {
                    runtime.submit_storage_manager_cycle(request.clone(), controller);
                }
            }
        });
        StorageManagerScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn start_storage_manager_runtime(
        &self,
        options: StorageManagerRuntimeOptions,
    ) -> StorageManagerRuntime {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let paused = Arc::new(AtomicBool::new(false));
        let report = Arc::new(Mutex::new(storage_manager_runtime_initial_report(&options)));
        let thread_stop = Arc::clone(&stop);
        let thread_paused = Arc::clone(&paused);
        let thread_report = Arc::clone(&report);
        let handle = thread::spawn(move || {
            let mut round = 0u64;
            let mut current_backoff_ms = options.initial_backoff_ms;
            // Cycles whose completion outlived the short wait below. Their reports are collected
            // on a later tick instead of being thrown away -- see the comment at the wait.
            //
            // A LIST, not a single slot. With one slot the report was lost every time: the
            // top-of-tick poll saw the cycle still running, the cycle finished later in that same
            // tick, `has_pending` then went false so a new cycle was submitted, and submitting
            // overwrote the slot before anything polled the finished job again. Every cycle was
            // abandoned exactly one tick before it completed, so `last_completed_cycle` stayed
            // None forever and every derived figure -- bytes reclaimed, pressure after, WAL and
            // index-log floors -- stayed at zero while the manager was doing real work.
            //
            // Bounded by `has_pending_storage_manager`, which allows only one cycle in flight per
            // shard, so this holds a couple of entries at most; the cap is a backstop, not a
            // budget.
            let mut uncollected_cycle_jobs: Vec<u64> = Vec::new();
            const MAX_UNCOLLECTED_CYCLE_JOBS: usize = 64;
            loop {
                let delay_ms =
                    storage_manager_runtime_delay_ms(&options, round, current_backoff_ms);
                {
                    let mut report = thread_report
                        .lock()
                        .expect("storage manager runtime report lock poisoned");
                    report.last_delay_ms = delay_ms;
                    report.current_backoff_ms = current_backoff_ms;
                }
                if sleep_until_storage_manager_runtime_round(&thread_stop, delay_ms) {
                    break;
                }
                if thread_stop.load(Ordering::Relaxed) {
                    break;
                }
                round = round.saturating_add(1);
                if thread_paused.load(Ordering::Relaxed) {
                    let mut report = thread_report
                        .lock()
                        .expect("storage manager runtime report lock poisoned");
                    report.rounds_skipped_paused = report.rounds_skipped_paused.saturating_add(1);
                    report.paused = true;
                    continue;
                }
                {
                    let mut report = thread_report
                        .lock()
                        .expect("storage manager runtime report lock poisoned");
                    report.paused = false;
                    report.rounds_attempted = report.rounds_attempted.saturating_add(1);
                }
                // Collect a cycle that finished after we stopped waiting for it. Without this
                // the runtime report only ever reflected cycles that completed inside the very
                // short budget below -- which are precisely the cycles that found nothing to
                // do. Any cycle that actually dumped buckets or reclaimed WAL outlived the
                // budget and had its report dropped, so the reported dirty-bucket count,
                // selected buckets and reclaim floors sat at zero while the manager was busy.
                if !uncollected_cycle_jobs.is_empty() {
                    let mut still_running = Vec::with_capacity(uncollected_cycle_jobs.len());
                    for job_id in std::mem::take(&mut uncollected_cycle_jobs) {
                        match runtime.job_status(job_id) {
                            Some(status) if status.finished_at_ms.is_some() => {
                                // Finished: collect the report if it carried one, and stop
                                // tracking either way so a job that finished without a usable
                                // output cannot pin a slot forever.
                                if let Some(DataNodeTaskOutput::StorageManager(response)) =
                                    status.output
                                {
                                    let mut report = thread_report
                                        .lock()
                                        .expect("storage manager runtime report lock poisoned");
                                    apply_storage_manager_cycle_to_runtime_report(
                                        &mut report,
                                        response.report,
                                    );
                                }
                            }
                            Some(_) => still_running.push(job_id),
                            // Gone from the jobs map (pruned): nothing left to collect.
                            None => {}
                        }
                    }
                    uncollected_cycle_jobs = still_running;
                }
                let should_submit = !runtime
                    .inner
                    .queue
                    .lock()
                    .expect("runtime queue lock poisoned")
                    .has_pending_storage_manager(options.request.shard_id);
                if !should_submit {
                    let mut report = thread_report
                        .lock()
                        .expect("storage manager runtime report lock poisoned");
                    report.rounds_skipped_pending = report.rounds_skipped_pending.saturating_add(1);
                    continue;
                }
                let submitted = runtime
                    .submit_storage_manager_cycle(options.request.clone(), options.controller);
                let mut report = thread_report
                    .lock()
                    .expect("storage manager runtime report lock poisoned");
                report.last_job_id = Some(submitted.job_id);
                report.last_status = Some(submitted.status.clone());
                if submitted.status.ok {
                    report.rounds_submitted = report.rounds_submitted.saturating_add(1);
                    current_backoff_ms = options.initial_backoff_ms;
                } else {
                    report.submit_failures = report.submit_failures.saturating_add(1);
                    current_backoff_ms = storage_manager_runtime_next_backoff_ms(
                        current_backoff_ms,
                        options.initial_backoff_ms,
                        options.max_backoff_ms,
                    );
                }
                drop(report);
                if submitted.status.ok {
                    let wait_budget_ms = options
                        .controller
                        .timeout_ms
                        .max(50)
                        .min(delay_ms.saturating_mul(10).max(50));
                    // The budget is deliberately tied to the loop interval, not to
                    // `controller.timeout_ms`, so pause/stop stay responsive. That means a
                    // working cycle routinely finishes AFTER we stop waiting -- remember it and
                    // collect it at the top of a later tick rather than losing its report.
                    if let Some(cycle) = wait_for_storage_manager_cycle_completion(
                        &runtime,
                        submitted.job_id,
                        wait_budget_ms,
                    ) {
                        let mut report = thread_report
                            .lock()
                            .expect("storage manager runtime report lock poisoned");
                        apply_storage_manager_cycle_to_runtime_report(&mut report, cycle);
                    } else {
                        if uncollected_cycle_jobs.len() >= MAX_UNCOLLECTED_CYCLE_JOBS {
                            uncollected_cycle_jobs.remove(0);
                        }
                        uncollected_cycle_jobs.push(submitted.job_id);
                    }
                }
            }
            let mut report = thread_report
                .lock()
                .expect("storage manager runtime report lock poisoned");
            report.running = false;
            report.stopped = true;
        });
        StorageManagerRuntime {
            stop,
            paused,
            report,
            handle: Some(handle),
        }
    }

    pub fn sweep_expired_records(&self) -> usize {
        let reports = self.inner.engine.sweep_all_expired_records();
        let removed = reports
            .iter()
            .map(|report| report.expired_records_removed)
            .sum::<usize>();
        let mut stats = self
            .inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned");
        stats.expiry_sweeps += 1;
        stats.expired_records_removed += removed as u64;
        removed
    }

    pub fn start_expiry_sweep_scheduler(&self, interval: Duration) -> ExpirySweepScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                runtime.sweep_expired_records();
            }
        });
        ExpirySweepScheduler {
            stop,
            handle: Some(handle),
        }
    }
}
