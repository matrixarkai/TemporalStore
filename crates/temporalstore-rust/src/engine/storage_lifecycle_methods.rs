//! Storage lifecycle / WAL-reclaim / eviction methods for TemporalEngine, split from engine.rs.
use super::*;

impl TemporalEngine {
    pub fn storage_lifecycle_plan(&self, request: StorageLifecycleRequest) -> StorageLifecyclePlan {
        let slot_summaries = self.slot_storage_summaries(request.shard_id);
        let dirty_slots = slot_summaries
            .iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .map(|summary| summary.routing_slot)
            .collect::<Vec<_>>();
        let latest_dump_oplog_sequence =
            latest_slot_dump_manifest_at(&self.index_dir, request.shard_id)
                .map(|manifest| manifest.oplog_sequence)
                .unwrap_or_default();
        let current_oplog_sequence = self.wal_store.stats(request.shard_id).last_sequence;
        let undumped_oplog_records =
            current_oplog_sequence.saturating_sub(latest_dump_oplog_sequence);
        let explicit_slots = !request.selected_dump_slots.is_empty();
        let dump_delayed = !explicit_slots
            && request.min_undumped_oplog_records > 0
            && undumped_oplog_records < request.min_undumped_oplog_records;
        let mut selected_dump_slots = if explicit_slots {
            request.selected_dump_slots.clone()
        } else if dump_delayed {
            Vec::new()
        } else {
            dirty_slots.clone()
        };
        if request.max_dump_slots_per_round > 0
            && selected_dump_slots.len() > request.max_dump_slots_per_round
        {
            selected_dump_slots.truncate(request.max_dump_slots_per_round);
        }
        let live_page_segment_ids = self.live_page_segment_ids(request.shard_id);
        let live_page_segment_set = live_page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let stale_page_segment_ids = self
            .page_store
            .segment_ids()
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !live_page_segment_set.contains(id))
            .collect::<Vec<_>>();
        let recovery = self.storage_recovery_report_without_boundary(request.shard_id);
        let stale_page_segment_set = stale_page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut reclaim_candidates =
            storage_reclaim_candidates_from_recovery(&recovery, &stale_page_segment_set);
        let delayed_destroy_reports = self
            .page_store
            .delayed_destroy_segment_reports()
            .unwrap_or_default();
        reclaim_candidates.extend(delayed_destroy_reports.iter().map(|report| {
            StorageReclaimCandidate {
                page_segment_id: report.page_segment_id,
                physical_bytes: report.physical_bytes,
                live_physical_bytes: 0,
                stale_physical_bytes: report.physical_bytes,
                reclaim_score: report.physical_bytes.saturating_mul(2),
                reason: "delayed_destroy".to_string(),
                ..StorageReclaimCandidate::default()
            }
        }));
        reclaim_candidates.sort_by(|left, right| {
            right
                .reclaim_score
                .cmp(&left.reclaim_score)
                .then_with(|| right.stale_physical_bytes.cmp(&left.stale_physical_bytes))
                .then_with(|| left.page_segment_id.cmp(&right.page_segment_id))
        });
        let mut reasons = Vec::new();
        if !selected_dump_slots.is_empty() {
            reasons.push("dirty_slot_dump".to_string());
        } else if dump_delayed && !dirty_slots.is_empty() {
            reasons.push("dirty_slot_dump_delayed".to_string());
        }
        if !stale_page_segment_ids.is_empty() {
            reasons.push("stale_page_segment_gc".to_string());
        }
        if !reclaim_candidates.is_empty() {
            reasons.push("ranked_reclaim_candidates".to_string());
        }
        if request.purge_delayed_destroy && !delayed_destroy_reports.is_empty() {
            reasons.push("delayed_destroy_purge".to_string());
        }
        let page_gc_dependency_plan = self.storage_page_gc_dependency_plan(
            request.shard_id,
            reclaim_candidates
                .iter()
                .map(|candidate| candidate.page_segment_id),
            request.page_gc_shared_store_cursors.clone(),
            request.page_gc_raft_snapshot_refs.clone(),
            request.page_gc_checkpoint_floor_segment_id,
            request.page_gc_raft_install_floor_segment_id,
            request.page_gc_delayed_destroy_grace_ms,
        );
        if !page_gc_dependency_plan
            .candidate_page_segment_ids
            .is_empty()
            && !page_gc_dependency_plan.safe_to_reclaim
        {
            reasons.push("page_gc_dependency_blocked".to_string());
        }
        let manifest_prune_plan = self.slot_dump_manifest_prune_plan_with_follower_cursors(
            request.shard_id,
            request.follower_replay_cursors.clone(),
        );
        if !manifest_prune_plan.prunable_manifest_ids.is_empty()
            || !manifest_prune_plan.prunable_marker_manifest_ids.is_empty()
        {
            reasons.push("slot_dump_manifest_prune".to_string());
        }
        if !self
            .interrupted_slot_dump_installs(request.shard_id)
            .is_empty()
        {
            reasons.push("slot_dump_install_roll_forward_check".to_string());
        }
        if request.invalidate_cache {
            reasons.push("cache_invalidation".to_string());
        }
        StorageLifecyclePlan {
            shard_id: request.shard_id,
            dirty_slots,
            selected_dump_slots,
            undumped_oplog_records,
            dump_delayed,
            slot_summaries,
            live_page_segment_ids,
            stale_page_segment_ids,
            reclaim_candidates,
            delayed_destroy_page_segment_ids: delayed_destroy_reports
                .iter()
                .map(|report| report.page_segment_id)
                .collect(),
            reclaimable_physical_bytes: delayed_destroy_reports
                .iter()
                .map(|report| report.physical_bytes)
                .sum(),
            reasons,
        }
    }

    pub fn storage_page_gc_dependency_plan(
        &self,
        shard_id: ShardId,
        candidate_page_segment_ids: impl IntoIterator<Item = u64>,
        shared_store_cursors: impl IntoIterator<Item = StoragePageGcReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = SlotDumpRaftSnapshotRef>,
        checkpoint_snapshot_floor: Option<u64>,
        raft_snapshot_install_floor: Option<u64>,
        delayed_destroy_grace_ms: u64,
    ) -> StoragePageGcDependencyPlan {
        let mut candidate_page_segment_ids = candidate_page_segment_ids
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        candidate_page_segment_ids.sort_unstable();
        let candidate_set = candidate_page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let live_page_segment_ids = self.live_page_segment_ids(shard_id);
        let live_set = live_page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let manifests = self.list_slot_dump_manifests(shard_id);
        let mut manifest_page_segment_ids = manifests
            .iter()
            .flat_map(|manifest| manifest.page_segment_ids.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        manifest_page_segment_ids.sort_unstable();
        let manifest_set = manifest_page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let shared_store_cursors = shared_store_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        let delayed_destroy_reports = self
            .page_store
            .delayed_destroy_segment_reports()
            .unwrap_or_default();
        let delayed_destroy_modified = delayed_destroy_reports
            .iter()
            .map(|report| (report.page_segment_id, report.modified_unix_ms))
            .collect::<BTreeMap<_, _>>();
        let now = now_ms();
        let mut dependency_blocks = Vec::new();
        for page_segment_id in &candidate_page_segment_ids {
            if live_set.contains(page_segment_id) {
                dependency_blocks.push(StoragePageGcDependencyBlock {
                    page_segment_id: *page_segment_id,
                    dependency: "live_page_ref".to_string(),
                    owner_id: format!("shard:{shard_id}"),
                    reason: "indexed live page references still point at this page segment"
                        .to_string(),
                    ..StoragePageGcDependencyBlock::default()
                });
            }
            if manifest_set.contains(page_segment_id) {
                let owner_id = manifests
                    .iter()
                    .filter(|manifest| manifest.page_segment_ids.contains(page_segment_id))
                    .map(|manifest| manifest.manifest_id.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                dependency_blocks.push(StoragePageGcDependencyBlock {
                    page_segment_id: *page_segment_id,
                    dependency: "slot_dump_manifest".to_string(),
                    owner_id,
                    reason: "slot dump manifest still names this page segment".to_string(),
                    ..StoragePageGcDependencyBlock::default()
                });
            }
            for cursor in shared_store_cursors
                .iter()
                .filter(|cursor| cursor.shard_id == shard_id)
            {
                if *page_segment_id >= cursor.retain_from_page_segment_id {
                    dependency_blocks.push(StoragePageGcDependencyBlock {
                        page_segment_id: *page_segment_id,
                        dependency: "shared_store_replay_cursor".to_string(),
                        owner_id: cursor.cursor_id.clone(),
                        retain_from_page_segment_id: Some(cursor.retain_from_page_segment_id),
                        reason: if cursor.reason.is_empty() {
                            "shared-store replay cursor has not advanced past this page segment"
                                .to_string()
                        } else {
                            cursor.reason.clone()
                        },
                        ..StoragePageGcDependencyBlock::default()
                    });
                }
            }
            for snapshot in raft_snapshot_refs
                .iter()
                .filter(|snapshot| snapshot.shard_id == shard_id)
            {
                if *page_segment_id >= snapshot.index_log_sequence {
                    dependency_blocks.push(StoragePageGcDependencyBlock {
                        page_segment_id: *page_segment_id,
                        dependency: "raft_snapshot_ref".to_string(),
                        owner_id: snapshot.snapshot_id.clone(),
                        retain_from_page_segment_id: Some(snapshot.index_log_sequence),
                        reason: "Raft snapshot reference has not released this page segment floor"
                            .to_string(),
                        ..StoragePageGcDependencyBlock::default()
                    });
                }
            }
            if checkpoint_snapshot_floor
                .map(|floor| *page_segment_id >= floor)
                .unwrap_or(false)
            {
                dependency_blocks.push(StoragePageGcDependencyBlock {
                    page_segment_id: *page_segment_id,
                    dependency: "checkpoint_snapshot_floor".to_string(),
                    owner_id: format!("checkpoint:{shard_id}"),
                    retain_from_page_segment_id: checkpoint_snapshot_floor,
                    reason: "checkpoint/snapshot floor still retains this page segment".to_string(),
                    ..StoragePageGcDependencyBlock::default()
                });
            }
            if raft_snapshot_install_floor
                .map(|floor| *page_segment_id >= floor)
                .unwrap_or(false)
            {
                dependency_blocks.push(StoragePageGcDependencyBlock {
                    page_segment_id: *page_segment_id,
                    dependency: "raft_snapshot_install_floor".to_string(),
                    owner_id: format!("raft-install:{shard_id}"),
                    retain_from_page_segment_id: raft_snapshot_install_floor,
                    reason: "Raft snapshot install floor still retains this page segment"
                        .to_string(),
                    ..StoragePageGcDependencyBlock::default()
                });
            }
            if delayed_destroy_grace_ms > 0 {
                if let Some(modified_unix_ms) = delayed_destroy_modified
                    .get(page_segment_id)
                    .and_then(|modified| *modified)
                {
                    let retain_until = modified_unix_ms.saturating_add(delayed_destroy_grace_ms);
                    if now < retain_until {
                        dependency_blocks.push(StoragePageGcDependencyBlock {
                            page_segment_id: *page_segment_id,
                            dependency: "delayed_destroy_grace_period".to_string(),
                            owner_id: format!("delayed-destroy:{page_segment_id}"),
                            retain_until_unix_ms: Some(retain_until),
                            reason:
                                "delayed-destroy grace period has not elapsed for this page segment"
                                    .to_string(),
                            ..StoragePageGcDependencyBlock::default()
                        });
                    }
                }
            }
        }
        let blocked_page_segment_ids = dependency_blocks
            .iter()
            .map(|block| block.page_segment_id)
            .collect::<BTreeSet<_>>();
        let dependency_count = |dependency: &str| {
            dependency_blocks
                .iter()
                .filter(|block| block.dependency == dependency)
                .count()
        };
        let live_ref_block_count = dependency_count("live_page_ref");
        let slot_dump_manifest_block_count = dependency_count("slot_dump_manifest");
        let shared_store_cursor_block_count = dependency_count("shared_store_replay_cursor");
        let raft_snapshot_ref_block_count = dependency_count("raft_snapshot_ref");
        let checkpoint_snapshot_floor_block_count = dependency_count("checkpoint_snapshot_floor");
        let raft_snapshot_install_floor_block_count =
            dependency_count("raft_snapshot_install_floor");
        let delayed_destroy_grace_block_count = dependency_count("delayed_destroy_grace_period");
        let reclaimable_page_segment_ids = candidate_page_segment_ids
            .iter()
            .copied()
            .filter(|id| !blocked_page_segment_ids.contains(id))
            .collect::<Vec<_>>();
        let blocked_page_segment_ids = blocked_page_segment_ids.into_iter().collect::<Vec<_>>();
        let mut blocker_reasons = dependency_blocks
            .iter()
            .map(|block| block.dependency.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if candidate_set.is_empty() {
            blocker_reasons.clear();
        }
        StoragePageGcDependencyPlan {
            shard_id,
            safe_to_reclaim: !candidate_set.is_empty() && dependency_blocks.is_empty(),
            candidate_page_segment_ids,
            reclaimable_page_segment_ids,
            blocked_page_segment_ids,
            live_page_segment_ids,
            manifest_page_segment_ids,
            shared_store_cursor_count: shared_store_cursors
                .iter()
                .filter(|cursor| cursor.shard_id == shard_id)
                .count(),
            checkpoint_snapshot_floor,
            raft_snapshot_install_floor,
            delayed_destroy_grace_ms,
            live_ref_block_count,
            slot_dump_manifest_block_count,
            shared_store_cursor_block_count,
            raft_snapshot_ref_block_count,
            checkpoint_snapshot_floor_block_count,
            raft_snapshot_install_floor_block_count,
            delayed_destroy_grace_block_count,
            dependency_blocks,
            blocker_reasons,
        }
    }

    pub fn apply_storage_lifecycle(
        &self,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleReport {
        let plan = self.storage_lifecycle_plan(request.clone());
        let dump_manifest = if plan.selected_dump_slots.is_empty() {
            None
        } else {
            self.create_slot_dump_manifest(request.shard_id, plan.selected_dump_slots.clone())
                .ok()
        };
        let (cache_entries_removed, cache_disk_bytes_removed) = if request.invalidate_cache {
            self.cache
                .invalidate_shard(request.shard_id)
                .map(|report| (report.memory_entries_removed, report.disk_bytes_removed))
                .unwrap_or_default()
        } else {
            (0, 0)
        };
        let cache_warmup = if request.warm_cache {
            self.storage_cache_warmup_report(request.shard_id, plan.selected_dump_slots.clone())
        } else {
            StorageCacheWarmupReport {
                shard_id: request.shard_id,
                selected_slots: plan.selected_dump_slots.clone(),
                ..StorageCacheWarmupReport::default()
            }
        };
        let cache_warmup_page_refs = cache_warmup.warmed_page_refs;
        let purge_report = if request.purge_delayed_destroy {
            self.page_store
                .purge_delayed_destroy_segments_with_report()
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let manifest_prune_plan = self.slot_dump_manifest_prune_plan_with_follower_cursors(
            request.shard_id,
            request.follower_replay_cursors.clone(),
        );
        let manifest_prune_report = request.prune_slot_dump_manifests.then(|| {
            self.apply_slot_dump_manifest_prune_with_follower_cursors(
                request.shard_id,
                request.follower_replay_cursors.clone(),
            )
        });
        let install_roll_forward_reports = if request.roll_forward_slot_dump_installs {
            self.roll_forward_slot_dump_installs(request.shard_id)
        } else {
            self.slot_dump_install_roll_forward_reports(request.shard_id)
        };
        let object_lifecycle = self
            .storage_recovery_report_without_boundary(request.shard_id)
            .object_lifecycle;
        let mut report = StorageLifecycleReport {
            shard_id: request.shard_id,
            public_storage_contract: Default::default(),
            public_storage_feature_shapes: Default::default(),
            effective_storage_tuning: effective_storage_tuning_from_env(),
            storage_lifecycle_metrics: default_storage_lifecycle_metrics(),
            storage_write_contract: default_storage_write_contract_empty(),
            storage_read_contract: default_storage_read_contract_empty(),
            storage_cold_scan_contract: default_storage_cold_scan_contract_empty(),
            storage_manager_contract: default_storage_manager_contract_empty(),
            storage_index_contract: default_storage_index_contract_empty(),
            storage_cache_contract: default_storage_cache_contract_empty(),
            storage_reclaim_contract: default_storage_reclaim_contract_empty(),
            storage_safety_snapshot: Default::default(),
            storage_watermark_snapshot: Default::default(),
            storage_gc_snapshot: Default::default(),
            storage_index_snapshot: Default::default(),
            storage_topology_snapshot: Default::default(),
            storage_write_sequence: default_storage_write_sequence(),
            storage_read_sequence: default_storage_read_sequence(),
            storage_cold_scan_sequence: default_storage_cold_scan_sequence(),
            storage_lifecycle_phases: default_storage_lifecycle_phases(),
            storage_cache_layers: default_storage_cache_layers(),
            storage_cache_semantics: default_storage_cache_semantics(),
            storage_reclaim_semantics: default_storage_reclaim_semantics(),
            storage_reclaim_scope: Default::default(),
            plan,
            dump_manifest,
            cache_entries_removed,
            cache_disk_bytes_removed,
            cache_warmup_page_refs,
            cache_warmup,
            delayed_destroy_purged_segments: purge_report.purged_page_segment_ids,
            delayed_destroy_purged_bytes: purge_report.purged_physical_bytes,
            manifest_prune_plan,
            manifest_prune_report,
            install_roll_forward_reports,
            object_lifecycle,
        };
        report.refresh_public_lifecycle_metrics();
        if let Some(shard) = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
        {
            report.storage_index_snapshot = storage_index_snapshot_with_samples(
                request.shard_id,
                shard,
                report.storage_index_snapshot,
            );
            report.storage_watermark_snapshot = storage_watermark_snapshot_with_samples(
                request.shard_id,
                shard,
                report.storage_watermark_snapshot,
            );
            report.storage_gc_snapshot = storage_gc_snapshot_with_samples(
                request.shard_id,
                shard,
                report.storage_gc_snapshot,
            );
            report.storage_topology_snapshot = storage_topology_snapshot_with_samples(
                request.shard_id,
                shard,
                report.storage_topology_snapshot,
            );
        }
        report
    }

    pub fn storage_wal_reclaim_plan(
        &self,
        shard_id: ShardId,
        follower_replay_cursors: impl IntoIterator<Item = SlotDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = SlotDumpRaftSnapshotRef>,
    ) -> StorageWalReclaimPlan {
        let follower_replay_cursors = follower_replay_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        let current_oplog_sequence = self.write_ahead_log_store().stats(shard_id).last_sequence;
        let current_index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let slot_summaries = self.slot_storage_summaries(shard_id);
        let current_slot_fingerprints = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&shard_id)
            .map(slot_generation_fingerprints_by_slot)
            .unwrap_or_default();
        let manifests = self.list_slot_dump_manifests(shard_id);
        let mut missing_slot_generations = Vec::new();
        let mut retained_manifest_ids = BTreeSet::<String>::new();
        let mut durable_oplog_frontier = u64::MAX;
        let mut durable_index_log_frontier = u64::MAX;
        let mut covered_slot_count = 0usize;

        for summary in &slot_summaries {
            let matching_manifest = manifests.iter().rev().find(|manifest| {
                let Ok(manifest_state) =
                    serde_json::from_slice::<ShardState>(&manifest.index_bytes)
                else {
                    return false;
                };
                let manifest_slot_fingerprints =
                    slot_generation_fingerprints_by_slot(&manifest_state);
                manifest.slot_summaries.iter().any(|manifest_summary| {
                    slot_dump_summary_matches_current_generation(
                        manifest_summary,
                        summary,
                        &manifest_slot_fingerprints,
                        &current_slot_fingerprints,
                    )
                })
            });
            let Some(manifest) = matching_manifest else {
                missing_slot_generations.push(summary.routing_slot);
                continue;
            };
            retained_manifest_ids.insert(manifest.manifest_id.clone());
            covered_slot_count = covered_slot_count.saturating_add(1);
            durable_oplog_frontier = durable_oplog_frontier.min(manifest.oplog_sequence);
            durable_index_log_frontier =
                durable_index_log_frontier.min(manifest.index_log_sequence);
        }

        let mut blocker_reasons = Vec::new();
        if slot_summaries.is_empty() {
            blocker_reasons.push("no_slot_generations_to_anchor_reclaim".to_string());
            durable_oplog_frontier = 0;
            durable_index_log_frontier = 0;
        }
        if !missing_slot_generations.is_empty() {
            blocker_reasons.push("slot_generation_without_durable_dump".to_string());
        }

        if durable_oplog_frontier == u64::MAX {
            durable_oplog_frontier = 0;
        }
        if durable_index_log_frontier == u64::MAX {
            durable_index_log_frontier = 0;
        }
        let mut follower_cursor_block_count = 0usize;
        for cursor in follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
        {
            if cursor.oplog_sequence < durable_oplog_frontier
                || cursor.index_log_sequence < durable_index_log_frontier
            {
                follower_cursor_block_count = follower_cursor_block_count.saturating_add(1);
                blocker_reasons.push(format!(
                    "follower_cursor_retains_logs:{}",
                    cursor.follower_id
                ));
            }
        }

        let mut raft_snapshot_block_count = 0usize;
        for snapshot in raft_snapshot_refs
            .iter()
            .filter(|snapshot| snapshot.shard_id == shard_id)
        {
            if snapshot.oplog_sequence < durable_oplog_frontier
                || snapshot.index_log_sequence < durable_index_log_frontier
            {
                raft_snapshot_block_count = raft_snapshot_block_count.saturating_add(1);
                blocker_reasons.push(format!(
                    "raft_snapshot_retains_logs:{}",
                    snapshot.snapshot_id
                ));
            }
        }
        let safe_to_reclaim = missing_slot_generations.is_empty()
            && covered_slot_count == slot_summaries.len()
            && durable_oplog_frontier > 0
            && durable_index_log_frontier > 0
            && follower_cursor_block_count == 0
            && raft_snapshot_block_count == 0;
        let retain_from_oplog_sequence = if safe_to_reclaim {
            durable_oplog_frontier.saturating_add(1)
        } else {
            0
        };
        let retain_from_index_log_sequence = if safe_to_reclaim {
            durable_index_log_frontier.saturating_add(1)
        } else {
            0
        };

        StorageWalReclaimPlan {
            shard_id,
            safe_to_reclaim,
            durable_slot_generation_frontier_oplog_sequence: durable_oplog_frontier,
            durable_slot_generation_frontier_index_log_sequence: durable_index_log_frontier,
            retain_from_oplog_sequence,
            retain_from_index_log_sequence,
            current_oplog_sequence,
            current_index_log_sequence,
            covered_slot_count,
            uncovered_slot_count: missing_slot_generations.len(),
            follower_cursor_block_count,
            raft_snapshot_block_count,
            missing_slot_generations,
            retained_manifest_ids: retained_manifest_ids.into_iter().collect(),
            blocker_reasons,
        }
    }

    pub fn apply_storage_wal_reclaim(
        &self,
        plan: StorageWalReclaimPlan,
    ) -> StorageWalReclaimReport {
        if !plan.safe_to_reclaim {
            return StorageWalReclaimReport {
                plan,
                applied: false,
                ..StorageWalReclaimReport::default()
            };
        }
        let oplog_gc = self
            .write_ahead_log_store()
            .gc_before_sequence(plan.shard_id, plan.retain_from_oplog_sequence)
            .ok();
        StorageWalReclaimReport {
            applied: oplog_gc.is_some(),
            oplog_records_removed: oplog_gc
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            oplog_bytes_before: oplog_gc
                .as_ref()
                .map(|report| report.bytes_before)
                .unwrap_or_default(),
            oplog_bytes_after: oplog_gc
                .as_ref()
                .map(|report| report.bytes_after)
                .unwrap_or_default(),
            index_log_records_removed: 0,
            index_log_bytes_before: self.index_log_store.stats(plan.shard_id).bytes_written,
            index_log_bytes_after: self.index_log_store.stats(plan.shard_id).bytes_written,
            plan,
        }
    }

    pub(super) fn storage_index_gc_report(
        &self,
        plan: &StorageLifecyclePlan,
        wal_plan: &StorageWalReclaimPlan,
        lifecycle_report: Option<&StorageLifecycleReport>,
        request: &StorageManagerCycleRequest,
    ) -> StorageIndexGcReport {
        let records = self
            .index_log_store
            .scan(request.shard_id, 0, u64::MAX, u64::MAX)
            .unwrap_or_default();
        let records_before = records.len();
        let bytes_before = records
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>();
        let removable_records_before_budget = records
            .iter()
            .filter_map(|(_, bytes)| {
                serde_json::from_slice::<crate::index_log::IndexLogRecord>(bytes).ok()
            })
            .filter(|record| record.sequence < wal_plan.retain_from_index_log_sequence)
            .count();
        let usage_ratio_basis_points = if records_before == 0 {
            0
        } else {
            (removable_records_before_budget as u64).saturating_mul(10_000) / records_before as u64
        };
        let threshold_triggered = request.index_gc_index_log_bytes_threshold == 0
            || bytes_before >= request.index_gc_index_log_bytes_threshold;
        let usage_ratio_triggered = request.index_gc_usage_ratio_trigger_basis_points == 0
            || usage_ratio_basis_points >= request.index_gc_usage_ratio_trigger_basis_points;
        let dirty_slots_committed_before_truncate = plan.selected_dump_slots.is_empty()
            || lifecycle_report
                .and_then(|report| report.dump_manifest.as_ref())
                .map(|manifest| !manifest.slot_ids.is_empty())
                .unwrap_or(false);
        let safe_to_truncate = wal_plan.safe_to_reclaim
            && removable_records_before_budget > 0
            && (!request.index_gc_commit_dirty_slots_before_truncation
                || dirty_slots_committed_before_truncate);
        let should_apply = request.enable_index_gc
            && !request.dry_run
            && safe_to_truncate
            && threshold_triggered
            && usage_ratio_triggered;
        let gc = should_apply
            .then(|| {
                self.index_log_store
                    .gc_before_sequence_limited(
                        request.shard_id,
                        wal_plan.retain_from_index_log_sequence,
                        request.index_gc_max_entries_per_round,
                    )
                    .ok()
            })
            .flatten();
        let bytes_after = gc
            .as_ref()
            .map(|report| report.bytes_after)
            .unwrap_or(bytes_before);
        let records_after = gc
            .as_ref()
            .map(|report| report.records_after)
            .unwrap_or(records_before);
        let skipped_reason = if !request.enable_index_gc {
            "index GC disabled"
        } else if request.dry_run {
            "dry_run"
        } else if !wal_plan.safe_to_reclaim {
            "durable WAL/index frontier not safe"
        } else if removable_records_before_budget == 0 {
            "no reclaimable index-log entries"
        } else if request.index_gc_commit_dirty_slots_before_truncation
            && !dirty_slots_committed_before_truncate
        {
            "dirty slots not committed before truncation"
        } else if !threshold_triggered {
            "index-log byte threshold not reached"
        } else if !usage_ratio_triggered {
            "index-log usage ratio trigger not reached"
        } else if gc.is_none() {
            "index-log truncation failed"
        } else {
            ""
        }
        .to_string();
        StorageIndexGcReport {
            shard_id: request.shard_id,
            enabled: request.enable_index_gc,
            applied: gc
                .as_ref()
                .map(|report| report.records_removed > 0)
                .unwrap_or(false),
            dirty_slots_committed_before_truncate,
            bytes_threshold: request.index_gc_index_log_bytes_threshold,
            usage_ratio_trigger_basis_points: request.index_gc_usage_ratio_trigger_basis_points,
            usage_ratio_basis_points,
            max_entries_per_round: request.index_gc_max_entries_per_round,
            retain_from_index_log_sequence: wal_plan.retain_from_index_log_sequence,
            records_before,
            records_after,
            records_removed: gc
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            removable_records_before_budget,
            budget_exhausted: gc
                .as_ref()
                .map(|report| report.budget_exhausted)
                .unwrap_or(false),
            bytes_before,
            bytes_after,
            threshold_triggered,
            usage_ratio_triggered,
            safe_to_truncate,
            skipped_reason,
        }
    }

    pub fn apply_storage_eviction(
        &self,
        shard_id: ShardId,
        memory_pressure_threshold: u64,
        batch_limit: usize,
        dump_before_evict: bool,
        delete_drop: bool,
    ) -> StorageEvictionReport {
        let before_cache = self.storage_cache_inspection_report(shard_id);
        let pressure_before = before_cache
            .stats
            .memory_bytes
            .saturating_add(before_cache.stats.disk_bytes)
            .saturating_add(before_cache.stats.async_writeback_queue_bytes)
            .saturating_add(before_cache.stats.async_writeback_queue_depth);
        if pressure_before < memory_pressure_threshold {
            return StorageEvictionReport {
                shard_id,
                mode: if delete_drop {
                    "delete_drop"
                } else {
                    "evict_cache"
                }
                .to_string(),
                pressure_before,
                pressure_after: pressure_before,
                memory_pressure_threshold,
                batch_limit,
                dump_before_evict,
                skipped_reason: "memory_pressure_below_threshold".to_string(),
                ..StorageEvictionReport::default()
            };
        }
        let cache_by_slot = before_cache
            .slot_summaries
            .iter()
            .map(|summary| (summary.routing_slot, summary.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut victims = self
            .slot_storage_summaries(shard_id)
            .into_iter()
            .map(|summary| {
                let cache = cache_by_slot.get(&summary.routing_slot);
                let cache_memory_bytes = cache.map(|cache| cache.memory_bytes).unwrap_or_default();
                let cache_disk_bytes = cache.map(|cache| cache.disk_bytes).unwrap_or_default();
                StorageEvictionVictim {
                    routing_slot: summary.routing_slot,
                    object_count: summary.object_count,
                    logical_bytes: summary.logical_bytes,
                    physical_bytes: summary.physical_bytes,
                    cache_memory_bytes,
                    cache_disk_bytes,
                    dirty_object_count: summary.dirty_object_count,
                    weight: cache_memory_bytes
                        .saturating_mul(4)
                        .saturating_add(cache_disk_bytes.saturating_mul(2))
                        .saturating_add(summary.physical_bytes)
                        .saturating_add(summary.dirty_object_count.saturating_mul(1024)),
                }
            })
            .filter(|victim| victim.weight > 0)
            .collect::<Vec<_>>();
        victims.sort_by(|left, right| {
            right
                .weight
                .cmp(&left.weight)
                .then_with(|| left.routing_slot.cmp(&right.routing_slot))
        });
        if batch_limit > 0 && victims.len() > batch_limit {
            victims.truncate(batch_limit);
        }
        let mut dump_manifest_ids = Vec::new();
        if dump_before_evict {
            let dirty_slots = victims
                .iter()
                .filter(|victim| victim.dirty_object_count > 0)
                .map(|victim| victim.routing_slot)
                .collect::<Vec<_>>();
            if !dirty_slots.is_empty() {
                if let Ok(manifest) = self.create_slot_dump_manifest(shard_id, dirty_slots) {
                    dump_manifest_ids.push(manifest.manifest_id);
                }
            }
        }
        let mut cache_entries_removed = 0usize;
        let mut cache_disk_bytes_removed = 0u64;
        for victim in &victims {
            if let Ok(report) = self.cache.invalidate_slot(shard_id, victim.routing_slot) {
                cache_entries_removed =
                    cache_entries_removed.saturating_add(report.memory_entries_removed);
                cache_disk_bytes_removed =
                    cache_disk_bytes_removed.saturating_add(report.disk_bytes_removed);
            }
        }
        let mut dropped_object_count = 0usize;
        if delete_drop && !victims.is_empty() {
            let victim_slots = victims
                .iter()
                .map(|victim| victim.routing_slot)
                .collect::<BTreeSet<_>>();
            let mut shards = self.shards.write().expect("shards lock poisoned");
            if let Some(shard) = shards.get_mut(&shard_id) {
                let object_keys = collect_live_page_entries(shard)
                    .into_iter()
                    .filter_map(|entry| {
                        let slot = entry
                            .address
                            .routing_slot
                            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
                        victim_slots.contains(&slot).then_some(entry.object_key)
                    })
                    .collect::<BTreeSet<_>>();
                for key in object_keys {
                    if delete_record(shard, &key) {
                        dropped_object_count = dropped_object_count.saturating_add(1);
                        invalidate_record_all(&self.cache, shard_id, &key);
                    }
                }
                if dropped_object_count > 0 {
                    if let Ok(index_bytes) = serde_json::to_vec_pretty(shard) {
                        let _ = self.persist_index_bytes(shard_id, &index_bytes);
                        let _ = self.index_log_store.append_json(shard_id, &index_bytes);
                    }
                }
            }
        }
        let after_cache = self.storage_cache_inspection_report(shard_id);
        let pressure_after = after_cache
            .stats
            .memory_bytes
            .saturating_add(after_cache.stats.disk_bytes)
            .saturating_add(after_cache.stats.async_writeback_queue_bytes)
            .saturating_add(after_cache.stats.async_writeback_queue_depth);
        StorageEvictionReport {
            shard_id,
            mode: if delete_drop {
                "delete_drop"
            } else {
                "evict_cache"
            }
            .to_string(),
            pressure_before,
            pressure_after,
            memory_pressure_threshold,
            pressure_gate_open: true,
            batch_limit,
            dump_before_evict,
            dump_manifest_ids,
            selected_victims: victims,
            cache_entries_removed,
            cache_disk_bytes_removed,
            dropped_object_count,
            cooldown: pressure_after >= pressure_before,
            skipped_reason: String::new(),
        }
    }
}
