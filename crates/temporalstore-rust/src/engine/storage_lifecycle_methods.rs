// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage lifecycle / WAL-reclaim / eviction methods for TemporalEngine, split from engine.rs.
use super::*;

/// How many dirty-object keys the dump drain has looked at, across every call.
///
/// The drain's cost is not visible from outside -- it is a closure inside a `retain` -- and
/// deriving it as |dirty objects| x |buckets| is arithmetic about the code rather than a
/// measurement of it. This counts what actually happens, which is the only version that keeps
/// being true after someone changes the loop.
pub(crate) static DIRTY_DRAIN_VISITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

impl TemporalEngine {
    pub fn storage_lifecycle_plan(&self, request: StorageLifecycleRequest) -> StorageLifecyclePlan {
        let bucket_summaries = self.bucket_storage_summaries(request.shard_id);
        // Select the least-recently-dumped (most overdue) dirty buckets first, matching the
        // WAL-reclaim routine's oldest-first-dirty ordering (dirty buckets are consumed
        // in non-decreasing first-dirty-log-id order).
        // bucket_summaries arrives in ascending routing_bucket order (a BTreeMap), so truncating to
        // max_dump_buckets_per_round always dropped the same high-id buckets -- a bucket dirtied
        // once could be starved forever by low-id buckets re-dirtied every round, never
        // checkpointed and pinning the WAL reclaim floor. Ordering by last_dump_sequence (the WAL
        // sequence at the bucket's last dump; 0 = never dumped) ascending makes an overdue bucket
        // rise to the top and guarantees every dirty bucket is eventually selected; routing_bucket
        // is a stable tiebreaker.
        let mut dirty_bucket_summaries = bucket_summaries
            .iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .collect::<Vec<_>>();
        dirty_bucket_summaries.sort_by(|left, right| {
            left.last_dump_sequence
                .cmp(&right.last_dump_sequence)
                .then_with(|| left.routing_bucket.cmp(&right.routing_bucket))
        });
        let dirty_buckets = dirty_bucket_summaries
            .iter()
            .map(|summary| summary.routing_bucket)
            .collect::<Vec<_>>();
        let latest_dump_wal_sequence =
            latest_bucket_dump_manifest_at(&self.index_dir, request.shard_id)
                .map(|manifest| manifest.wal_sequence)
                .unwrap_or_default();
        let current_wal_sequence = self.wal_store.stats(request.shard_id).last_sequence;
        let undumped_wal_records =
            current_wal_sequence.saturating_sub(latest_dump_wal_sequence);
        let explicit_buckets = !request.selected_dump_buckets.is_empty();
        let dump_delayed = !explicit_buckets
            && request.min_undumped_wal_records > 0
            && undumped_wal_records < request.min_undumped_wal_records;
        let mut selected_dump_buckets = if explicit_buckets {
            request.selected_dump_buckets.clone()
        } else if dump_delayed {
            Vec::new()
        } else {
            dirty_buckets.clone()
        };
        if request.max_dump_buckets_per_round > 0
            && selected_dump_buckets.len() > request.max_dump_buckets_per_round
        {
            selected_dump_buckets.truncate(request.max_dump_buckets_per_round);
        }
        let live_page_slab_ids = self.live_page_slab_ids(request.shard_id);
        let live_page_slab_set = live_page_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let stale_page_slab_ids = self
            .page_store
            .slab_ids()
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !live_page_slab_set.contains(id))
            .collect::<Vec<_>>();
        let recovery = self.storage_recovery_report_without_boundary(request.shard_id);
        let stale_page_slab_set = stale_page_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut reclaim_candidates =
            storage_reclaim_candidates_from_recovery(&recovery, &stale_page_slab_set);
        let delayed_destroy_reports = self
            .page_store
            .delayed_destroy_slab_reports()
            .unwrap_or_default();
        reclaim_candidates.extend(delayed_destroy_reports.iter().map(|report| {
            StorageReclaimCandidate {
                page_slab_id: report.page_slab_id,
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
                .then_with(|| left.page_slab_id.cmp(&right.page_slab_id))
        });
        let mut reasons = Vec::new();
        if !selected_dump_buckets.is_empty() {
            reasons.push("dirty_slot_dump".to_string());
        } else if dump_delayed && !dirty_buckets.is_empty() {
            reasons.push("dirty_slot_dump_delayed".to_string());
        }
        if !stale_page_slab_ids.is_empty() {
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
                .map(|candidate| candidate.page_slab_id),
            request.page_gc_shared_store_cursors.clone(),
            request.page_gc_raft_snapshot_refs.clone(),
            request.page_gc_checkpoint_floor_slab_id,
            request.page_gc_raft_install_floor_slab_id,
            request.page_gc_delayed_destroy_grace_ms,
        );
        if !page_gc_dependency_plan
            .candidate_page_slab_ids
            .is_empty()
            && !page_gc_dependency_plan.safe_to_reclaim
        {
            reasons.push("page_gc_dependency_blocked".to_string());
        }
        let manifest_prune_plan = self.bucket_dump_manifest_prune_plan_with_follower_cursors(
            request.shard_id,
            request.follower_replay_cursors.clone(),
        );
        if !manifest_prune_plan.prunable_manifest_ids.is_empty()
            || !manifest_prune_plan.prunable_marker_manifest_ids.is_empty()
        {
            reasons.push("slot_dump_manifest_prune".to_string());
        }
        if !self
            .interrupted_bucket_dump_installs(request.shard_id)
            .is_empty()
        {
            reasons.push("slot_dump_install_roll_forward_check".to_string());
        }
        if request.invalidate_cache {
            reasons.push("cache_invalidation".to_string());
        }
        StorageLifecyclePlan {
            shard_id: request.shard_id,
            dirty_buckets,
            selected_dump_buckets,
            undumped_wal_records,
            dump_delayed,
            bucket_summaries,
            live_page_slab_ids,
            stale_page_slab_ids,
            reclaim_candidates,
            delayed_destroy_page_slab_ids: delayed_destroy_reports
                .iter()
                .map(|report| report.page_slab_id)
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
        candidate_page_slab_ids: impl IntoIterator<Item = u64>,
        shared_store_cursors: impl IntoIterator<Item = StoragePageGcReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = BucketDumpRaftSnapshotRef>,
        checkpoint_snapshot_floor: Option<u64>,
        raft_snapshot_install_floor: Option<u64>,
        delayed_destroy_grace_ms: u64,
    ) -> StoragePageGcDependencyPlan {
        let mut candidate_page_slab_ids = candidate_page_slab_ids
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        candidate_page_slab_ids.sort_unstable();
        let candidate_set = candidate_page_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let live_page_slab_ids = self.live_page_slab_ids(shard_id);
        let live_set = live_page_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let manifests = self.list_bucket_dump_manifests(shard_id);
        let mut manifest_page_slab_ids = manifests
            .iter()
            .flat_map(|manifest| manifest.page_slab_ids.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        manifest_page_slab_ids.sort_unstable();
        let manifest_set = manifest_page_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let shared_store_cursors = shared_store_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        let delayed_destroy_reports = self
            .page_store
            .delayed_destroy_slab_reports()
            .unwrap_or_default();
        let delayed_destroy_modified = delayed_destroy_reports
            .iter()
            .map(|report| (report.page_slab_id, report.modified_unix_ms))
            .collect::<BTreeMap<_, _>>();
        let now = now_ms();
        let mut dependency_blocks = Vec::new();
        for page_slab_id in &candidate_page_slab_ids {
            if live_set.contains(page_slab_id) {
                dependency_blocks.push(StoragePageGcDependencyBlock {
                    page_slab_id: *page_slab_id,
                    dependency: "live_page_ref".to_string(),
                    owner_id: format!("shard:{shard_id}"),
                    reason: "indexed live page references still point at this page segment"
                        .to_string(),
                    ..StoragePageGcDependencyBlock::default()
                });
            }
            if manifest_set.contains(page_slab_id) {
                let owner_id = manifests
                    .iter()
                    .filter(|manifest| manifest.page_slab_ids.contains(page_slab_id))
                    .map(|manifest| manifest.manifest_id.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                dependency_blocks.push(StoragePageGcDependencyBlock {
                    page_slab_id: *page_slab_id,
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
                if *page_slab_id >= cursor.retain_from_page_slab_id {
                    dependency_blocks.push(StoragePageGcDependencyBlock {
                        page_slab_id: *page_slab_id,
                        dependency: "shared_store_replay_cursor".to_string(),
                        owner_id: cursor.cursor_id.clone(),
                        retain_from_page_slab_id: Some(cursor.retain_from_page_slab_id),
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
                if *page_slab_id >= snapshot.index_log_sequence {
                    dependency_blocks.push(StoragePageGcDependencyBlock {
                        page_slab_id: *page_slab_id,
                        dependency: "raft_snapshot_ref".to_string(),
                        owner_id: snapshot.snapshot_id.clone(),
                        retain_from_page_slab_id: Some(snapshot.index_log_sequence),
                        reason: "Raft snapshot reference has not released this page segment floor"
                            .to_string(),
                        ..StoragePageGcDependencyBlock::default()
                    });
                }
            }
            if checkpoint_snapshot_floor
                .map(|floor| *page_slab_id >= floor)
                .unwrap_or(false)
            {
                dependency_blocks.push(StoragePageGcDependencyBlock {
                    page_slab_id: *page_slab_id,
                    dependency: "checkpoint_snapshot_floor".to_string(),
                    owner_id: format!("checkpoint:{shard_id}"),
                    retain_from_page_slab_id: checkpoint_snapshot_floor,
                    reason: "checkpoint/snapshot floor still retains this page segment".to_string(),
                    ..StoragePageGcDependencyBlock::default()
                });
            }
            if raft_snapshot_install_floor
                .map(|floor| *page_slab_id >= floor)
                .unwrap_or(false)
            {
                dependency_blocks.push(StoragePageGcDependencyBlock {
                    page_slab_id: *page_slab_id,
                    dependency: "raft_snapshot_install_floor".to_string(),
                    owner_id: format!("raft-install:{shard_id}"),
                    retain_from_page_slab_id: raft_snapshot_install_floor,
                    reason: "Raft snapshot install floor still retains this page segment"
                        .to_string(),
                    ..StoragePageGcDependencyBlock::default()
                });
            }
            if delayed_destroy_grace_ms > 0 {
                if let Some(modified_unix_ms) = delayed_destroy_modified
                    .get(page_slab_id)
                    .and_then(|modified| *modified)
                {
                    let retain_until = modified_unix_ms.saturating_add(delayed_destroy_grace_ms);
                    if now < retain_until {
                        dependency_blocks.push(StoragePageGcDependencyBlock {
                            page_slab_id: *page_slab_id,
                            dependency: "delayed_destroy_grace_period".to_string(),
                            owner_id: format!("delayed-destroy:{page_slab_id}"),
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
        let blocked_page_slab_ids = dependency_blocks
            .iter()
            .map(|block| block.page_slab_id)
            .collect::<BTreeSet<_>>();
        let dependency_count = |dependency: &str| {
            dependency_blocks
                .iter()
                .filter(|block| block.dependency == dependency)
                .count()
        };
        let live_ref_block_count = dependency_count("live_page_ref");
        let bucket_dump_manifest_block_count = dependency_count("slot_dump_manifest");
        let shared_store_cursor_block_count = dependency_count("shared_store_replay_cursor");
        let raft_snapshot_ref_block_count = dependency_count("raft_snapshot_ref");
        let checkpoint_snapshot_floor_block_count = dependency_count("checkpoint_snapshot_floor");
        let raft_snapshot_install_floor_block_count =
            dependency_count("raft_snapshot_install_floor");
        let delayed_destroy_grace_block_count = dependency_count("delayed_destroy_grace_period");
        let reclaimable_page_slab_ids = candidate_page_slab_ids
            .iter()
            .copied()
            .filter(|id| !blocked_page_slab_ids.contains(id))
            .collect::<Vec<_>>();
        let blocked_page_slab_ids = blocked_page_slab_ids.into_iter().collect::<Vec<_>>();
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
            candidate_page_slab_ids,
            reclaimable_page_slab_ids,
            blocked_page_slab_ids,
            live_page_slab_ids,
            manifest_page_slab_ids,
            shared_store_cursor_count: shared_store_cursors
                .iter()
                .filter(|cursor| cursor.shard_id == shard_id)
                .count(),
            checkpoint_snapshot_floor,
            raft_snapshot_install_floor,
            delayed_destroy_grace_ms,
            live_ref_block_count,
            bucket_dump_manifest_block_count,
            shared_store_cursor_block_count,
            raft_snapshot_ref_block_count,
            checkpoint_snapshot_floor_block_count,
            raft_snapshot_install_floor_block_count,
            delayed_destroy_grace_block_count,
            dependency_blocks,
            blocker_reasons,
        }
    }

    /// Clear the dirty state of buckets just captured by `manifest` (a dumped
    /// bucket has its dirty flag cleared), so the storage cycle does not
    /// re-select and re-dump them every round. A bucket re-dirtied since the manifest
    /// snapshot (its current derived generation no longer equals the captured one) is
    /// left dirty, so reclaim never advances past an undumped write. The bucket's
    /// generation is held at the captured (derived) value and its live pages are
    /// untouched, so bucket_dump_summary_matches_current_generation keeps matching and
    /// WAL/index reclaim gating is unchanged.
    pub(super) fn clear_dumped_bucket_dirty_state(
        &self,
        shard_id: ShardId,
        manifest: &BucketDumpManifest,
    ) {
        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return;
        };
        // Per-bucket derived generation the manifest captured (base + dirty count) --
        // exactly what the reclaim fingerprint compares.
        let captured: std::collections::HashMap<u32, u64> = manifest
            .bucket_summaries
            .iter()
            .map(|summary| (summary.routing_bucket, summary.dirty_generation))
            .collect();
        // Current derived generation BEFORE mutation: detects writes that landed after
        // the dump snapshot (those buckets must stay dirty for the next dump).
        let current: std::collections::HashMap<u32, u64> =
            bucket_storage_summaries(shard, start_routing_bucket, end_routing_bucket)
                .into_iter()
                .map(|summary| (summary.routing_bucket, summary.dirty_generation))
                .collect();
        // Buckets this dump actually clears. Collected first so the dirty set is walked ONCE.
        //
        // The retain used to sit inside this loop, so every qualifying bucket walked every dirty
        // object and re-hashed its key to recompute a routing bucket -- a bucket the caller
        // already knew, and one that `mark_async_dirty_object` had computed on the line above the
        // insert and thrown away. The work was |dirty objects| x |buckets|, to remove at most
        // |dirty objects| entries: measured at 4 040 000 closure calls to clear 4 000 objects
        // across 1 010 buckets, a 1010x amplification.
        //
        // Nothing in the per-bucket body depends on the dirty set having been cleared, and
        // `current` was captured before any mutation, so hoisting the walk out is the same answer
        // in one pass.
        let mut cleared_buckets: std::collections::HashSet<u32> = std::collections::HashSet::new();
        for bucket_id in manifest.bucket_ids.iter().copied() {
            let Some(&captured_generation) = captured.get(&bucket_id) else {
                continue;
            };
            if current.get(&bucket_id).copied().unwrap_or_default() != captured_generation {
                continue;
            }
            cleared_buckets.insert(bucket_id);
            if let Some(bucket) = shard.bucket_index.bucket_map.get_mut(&bucket_id) {
                // Hold the generation at the captured (derived) value so the reclaim
                // fingerprint still matches once the dirty objects are cleared.
                bucket.dirty_generation = bucket.dirty_generation.max(captured_generation);
                // Record the dumped-log sequence (informational; not part of the fingerprint).
                bucket.last_dump_sequence = bucket.last_dump_sequence.max(manifest.wal_sequence);
                bucket.dirty = false;
                for page in bucket.page_index.values_mut() {
                    page.dirty = false;
                }
            }
        }
        if !cleared_buckets.is_empty() {
            shard.dirty_objects.retain(|key| {
                DIRTY_DRAIN_VISITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                !cleared_buckets.contains(&page_routing_bucket(
                    key,
                    start_routing_bucket,
                    end_routing_bucket,
                ))
            });
        }
    }

    pub fn apply_storage_lifecycle(
        &self,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleReport {
        let plan = self.storage_lifecycle_plan(request.clone());
        let dump_manifest = if plan.selected_dump_buckets.is_empty() {
            None
        } else {
            self.create_bucket_dump_manifest(request.shard_id, plan.selected_dump_buckets.clone())
                .ok()
        };
        if let Some(manifest) = &dump_manifest {
            // Once a bucket is dumped its dirty flag is cleared, so the storage cycle
            // stops re-selecting and re-dumping it every round. Dumped state is anchored
            // by the WAL watermark (last_dump_sequence / manifest.wal_sequence, the
            // dumped-log sequence), not by the dirty flag.
            self.clear_dumped_bucket_dirty_state(request.shard_id, manifest);
        }
        let (cache_entries_removed, cache_disk_bytes_removed) = if request.invalidate_cache {
            self.cache
                .invalidate_shard(request.shard_id)
                .map(|report| (report.memory_entries_removed, report.disk_bytes_removed))
                .unwrap_or_default()
        } else {
            (0, 0)
        };
        let cache_warmup = if request.warm_cache {
            self.storage_cache_warmup_report(request.shard_id, plan.selected_dump_buckets.clone())
        } else {
            StorageCacheWarmupReport {
                shard_id: request.shard_id,
                selected_buckets: plan.selected_dump_buckets.clone(),
                ..StorageCacheWarmupReport::default()
            }
        };
        let cache_warmup_page_refs = cache_warmup.warmed_page_refs;
        let purge_report = if request.purge_delayed_destroy {
            self.page_store
                .purge_delayed_destroy_slabs_with_report()
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let manifest_prune_plan = self.bucket_dump_manifest_prune_plan_with_follower_cursors(
            request.shard_id,
            request.follower_replay_cursors.clone(),
        );
        // Roll forward interrupted installs BEFORE pruning: prune removes obsolete
        // install markers, which would otherwise drop an interrupted install before it
        // can be recovered (leaving install_roll_forward_reports empty even though an
        // interrupted install was present).
        let install_roll_forward_reports = if request.roll_forward_bucket_dump_installs {
            self.roll_forward_bucket_dump_installs(request.shard_id)
        } else {
            self.bucket_dump_install_roll_forward_reports(request.shard_id)
        };
        let manifest_prune_report = request.prune_bucket_dump_manifests.then(|| {
            self.apply_bucket_dump_manifest_prune_with_follower_cursors(
                request.shard_id,
                request.follower_replay_cursors.clone(),
            )
        });
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
            delayed_destroy_purged_slabs: purge_report.purged_page_slab_ids,
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
        follower_replay_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = BucketDumpRaftSnapshotRef>,
    ) -> StorageWalReclaimPlan {
        let follower_replay_cursors = follower_replay_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        let current_wal_sequence = self.write_ahead_log_store().stats(shard_id).last_sequence;
        let current_index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let bucket_summaries = self.bucket_storage_summaries(shard_id);
        let current_bucket_fingerprints = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&shard_id)
            .map(bucket_generation_fingerprints_by_bucket)
            .unwrap_or_default();
        let manifests = self.list_bucket_dump_manifests(shard_id);
        let mut missing_bucket_generations = Vec::new();
        let mut retained_manifest_ids = BTreeSet::<String>::new();
        let mut durable_wal_frontier = u64::MAX;
        let mut durable_index_log_frontier = u64::MAX;
        let mut covered_bucket_count = 0usize;

        // Decode each manifest's index ONCE, not once per bucket.
        //
        // This sat inside the loop below, so every bucket re-deserialized every manifest's
        // WHOLE shard index out of JSON and rebuilt the generation fingerprints for every
        // bucket in it -- identical work, repeated once per bucket. The manifests are read
        // before the loop and do not change while the plan is computed, so one pass produces
        // the same answer every visit would have.
        //
        // The cost was quadratic in the corpus: a real (non-dry-run) WAL reclaim took 6.5s at
        // 1k records, 23s at 2k and 100s at 4k -- x4 per doubling -- and a 40k shard did not
        // finish in ten minutes with a core pegged at 100%. Reclaim is the only thing that
        // removes WAL and index-log bytes, so a shard large enough to need it was a shard on
        // which it could not run.
        let manifest_fingerprints = manifests
            .iter()
            .map(|manifest| {
                crate::engine::decode_index_bytes(&manifest.index_bytes)
                    .ok()
                    .map(|manifest_state| {
                        bucket_generation_fingerprints_by_bucket(&manifest_state)
                    })
            })
            .collect::<Vec<_>>();

        for summary in &bucket_summaries {
            let matching_manifest = manifests
                .iter()
                .enumerate()
                .rev()
                .find(|(manifest_index, manifest)| {
                    // A manifest whose index will not decode matched nothing before and
                    // matches nothing now.
                    let Some(manifest_bucket_fingerprints) =
                        manifest_fingerprints[*manifest_index].as_ref()
                    else {
                        return false;
                    };
                    manifest.bucket_summaries.iter().any(|manifest_summary| {
                        bucket_dump_summary_matches_current_generation(
                            manifest_summary,
                            summary,
                            manifest_bucket_fingerprints,
                            &current_bucket_fingerprints,
                        )
                    })
                })
                .map(|(_, manifest)| manifest);
            let Some(manifest) = matching_manifest else {
                missing_bucket_generations.push(summary.routing_bucket);
                continue;
            };
            retained_manifest_ids.insert(manifest.manifest_id.clone());
            covered_bucket_count = covered_bucket_count.saturating_add(1);
            durable_wal_frontier = durable_wal_frontier.min(manifest.wal_sequence);
            durable_index_log_frontier =
                durable_index_log_frontier.min(manifest.index_log_sequence);
        }

        let mut blocker_reasons = Vec::new();
        if bucket_summaries.is_empty() {
            blocker_reasons.push("no_slot_generations_to_anchor_reclaim".to_string());
            durable_wal_frontier = 0;
            durable_index_log_frontier = 0;
        }
        if !missing_bucket_generations.is_empty() {
            blocker_reasons.push("slot_generation_without_durable_dump".to_string());
        }

        if durable_wal_frontier == u64::MAX {
            durable_wal_frontier = 0;
        }
        if durable_index_log_frontier == u64::MAX {
            durable_index_log_frontier = 0;
        }
        let mut follower_cursor_block_count = 0usize;
        for cursor in follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
        {
            if cursor.wal_sequence < durable_wal_frontier
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
            if snapshot.wal_sequence < durable_wal_frontier
                || snapshot.index_log_sequence < durable_index_log_frontier
            {
                raft_snapshot_block_count = raft_snapshot_block_count.saturating_add(1);
                blocker_reasons.push(format!(
                    "raft_snapshot_retains_logs:{}",
                    snapshot.snapshot_id
                ));
            }
        }
        // Two different questions were being answered by one boolean.
        //
        // WHETHER the frontier can be trusted: every live generation needs a durable dump behind
        // it, or the lowest manifest sequence does not describe what is actually on disk. These
        // stay absolute -- there is no safe partial answer to a frontier that is wrong.
        let generations_durable = missing_bucket_generations.is_empty()
            && covered_bucket_count == bucket_summaries.len()
            && durable_wal_frontier > 0
            && durable_index_log_frontier > 0;

        // HOW FAR it may be followed: a retention cursor marks what some reader has still to
        // consume. Everything at or below the SLOWEST cursor is behind every reader and can go
        // whether or not that cursor ever advances. Refusing at the cursor instead of clamping to
        // it meant one lagging follower pinned the entire log for as long as it lagged, and the
        // log grew without bound underneath it.
        //
        // The floor is a minimum over followers AND snapshot refs together: they are separate
        // lists but the same question, and taking them apart would let one advance past the other
        // and drop a log the slower one still needs.
        let cursor_wal_floor = follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
            .map(|cursor| cursor.wal_sequence)
            .chain(
                raft_snapshot_refs
                    .iter()
                    .filter(|snapshot| snapshot.shard_id == shard_id)
                    .map(|snapshot| snapshot.wal_sequence),
            )
            .min();
        let cursor_index_log_floor = follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
            .map(|cursor| cursor.index_log_sequence)
            .chain(
                raft_snapshot_refs
                    .iter()
                    .filter(|snapshot| snapshot.shard_id == shard_id)
                    .map(|snapshot| snapshot.index_log_sequence),
            )
            .min();

        // Never above the durable frontier, and never above the slowest cursor. With no cursors at
        // all the frontier stands unchanged, which is what it did before.
        let effective_wal_frontier = cursor_wal_floor
            .map_or(durable_wal_frontier, |floor| durable_wal_frontier.min(floor));
        let effective_index_log_frontier = cursor_index_log_floor.map_or(
            durable_index_log_frontier,
            |floor| durable_index_log_frontier.min(floor),
        );

        // A clamp to zero reclaims nothing, which is the right answer for a cursor that has never
        // advanced -- the win here is exactly the span a reader has already consumed, and for a
        // permanently stuck follower that span is empty.
        let safe_to_reclaim =
            generations_durable && effective_wal_frontier > 0 && effective_index_log_frontier > 0;
        let retain_from_wal_sequence = if safe_to_reclaim {
            effective_wal_frontier.saturating_add(1)
        } else {
            0
        };
        let retain_from_index_log_sequence = if safe_to_reclaim {
            effective_index_log_frontier.saturating_add(1)
        } else {
            0
        };

        StorageWalReclaimPlan {
            shard_id,
            safe_to_reclaim,
            durable_bucket_generation_frontier_wal_sequence: durable_wal_frontier,
            durable_bucket_generation_frontier_index_log_sequence: durable_index_log_frontier,
            retain_from_wal_sequence,
            retain_from_index_log_sequence,
            current_wal_sequence,
            current_index_log_sequence,
            covered_bucket_count,
            uncovered_bucket_count: missing_bucket_generations.len(),
            follower_cursor_block_count,
            raft_snapshot_block_count,
            missing_bucket_generations,
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
        // The plan reached `safe_to_reclaim` by finding a durable bucket-dump manifest for
        // every live generation and taking the LOWEST wal sequence among them, so the durable
        // index reflects everything at or below that frontier.
        //
        // This stays the DURABLE frontier, not the cursor-clamped one. The anchor is an upper
        // bound on what may be dropped; `retain_from_wal_sequence` may sit below it because a
        // retention cursor clamped it, and dropping less than the anchor permits is safe. Passing
        // the clamped value here would prove less durability than has actually been established
        // and would be the wrong number for a different reason.
        let durable_index = crate::wal::DurableIndexAnchor::proven_durable_through(
            plan.shard_id,
            plan.durable_bucket_generation_frontier_wal_sequence,
        );
        let wal_gc = self
            .write_ahead_log_store()
            .gc_before_sequence(plan.shard_id, plan.retain_from_wal_sequence, &durable_index)
            .ok();
        StorageWalReclaimReport {
            applied: wal_gc.is_some(),
            wal_records_removed: wal_gc
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            wal_bytes_before: wal_gc
                .as_ref()
                .map(|report| report.bytes_before)
                .unwrap_or_default(),
            wal_bytes_after: wal_gc
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
                // Decode the integrity framing (accepts legacy unframed records too) before
                // reading the sequence; a corrupt line is simply not counted here (this is a
                // GC-pressure metric, not the recovery path).
                let payload = crate::log_framing::decode_line(bytes).ok()?;
                // Through the index log's own decoder, not serde_json directly. A record's
                // payload is not necessarily JSON, and reading it as though it were fails
                // quietly here -- `.ok()` drops it, the record is not counted, and GC reports
                // "no reclaimable index-log entries" while the log grows. A second decoder is
                // exactly what the served index's first attempt at a binary format died of.
                crate::index_log::decode_index_payload::<crate::index_log::IndexLogRecord>(payload)
                    .ok()
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
        let dirty_buckets_committed_before_truncate = plan.selected_dump_buckets.is_empty()
            || lifecycle_report
                .and_then(|report| report.dump_manifest.as_ref())
                .map(|manifest| !manifest.bucket_ids.is_empty())
                .unwrap_or(false);
        let safe_to_truncate = wal_plan.safe_to_reclaim
            && removable_records_before_budget > 0
            && (!request.index_gc_commit_dirty_buckets_before_truncation
                || dirty_buckets_committed_before_truncate);
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
        } else if request.index_gc_commit_dirty_buckets_before_truncation
            && !dirty_buckets_committed_before_truncate
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
            dirty_buckets_committed_before_truncate,
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

    /// Pick victims with a bounded sampled scan instead of enumerating every bucket.
    ///
    /// Reads recency and eligibility straight off the bucket index -- the same signals the full
    /// scan derives, but without materializing every live page -- and computes byte totals only
    /// for the buckets actually chosen, which is at most `batch_limit` of them.
    fn sampled_eviction_victims(
        &self,
        shard_id: ShardId,
        batch_limit: usize,
        cache_by_bucket: &BTreeMap<u32, crate::StorageCacheBucketSummary>,
    ) -> Vec<StorageEvictionVictim> {
        use super::eviction_sampler::{select_victims, BucketSample, BucketSource, ScanResult};

        /// Bucket-index-backed source. Scanning ranges over the ordered map from the cursor, so
        /// a pass touches only the window it is budgeted for.
        struct IndexSource<'a> {
            buckets: &'a super::state::BucketMap,
            recency: &'a std::collections::HashMap<u32, u64>,
        }

        impl<'a> IndexSource<'a> {
            fn sample(&self, routing_bucket: u32, bucket: &super::state::BucketNode) -> BucketSample {
                BucketSample {
                    routing_bucket,
                    // Evicting a bucket only frees something if it is resident and still holds
                    // live objects, which is the same eligibility the full scan applies via its
                    // weight filter.
                    eligible: bucket.in_memory
                        && !bucket.deleted
                        && !bucket.object_index.is_empty(),
                    last_used_ms: self.recency.get(&routing_bucket).copied().unwrap_or(0),
                }
            }
        }

        impl<'a> BucketSource for IndexSource<'a> {
            fn bucket_count(&self) -> usize {
                self.buckets.len()
            }

            fn scan(
                &self,
                cursor: Option<u32>,
                budget: usize,
                visit: &mut dyn FnMut(&BucketSample) -> bool,
            ) -> ScanResult {
                if self.buckets.is_empty() || budget == 0 {
                    return ScanResult::default();
                }
                let mut scanned = 0usize;
                let mut wrapped = false;
                let mut next_cursor = None;
                let mut keep_going = true;

                let start = cursor.unwrap_or(0);
                // Two ranges rather than one, so the scan wraps past the end of the bucket space
                // back to the beginning without materializing the map.
                for pass in 0..2 {
                    let iter: Box<dyn Iterator<Item = (&u32, &super::state::BucketNode)>> =
                        if pass == 0 {
                            Box::new(self.buckets.range(start..))
                        } else {
                            wrapped = true;
                            Box::new(self.buckets.range(..start))
                        };
                    for (routing_bucket, bucket) in iter {
                        if scanned >= budget || !keep_going || scanned >= self.buckets.len() {
                            next_cursor = Some(*routing_bucket);
                            break;
                        }
                        let sample = self.sample(*routing_bucket, bucket);
                        scanned += 1;
                        keep_going = visit(&sample);
                        next_cursor = Some(routing_bucket.saturating_add(1));
                    }
                    if scanned >= budget || !keep_going || scanned >= self.buckets.len() {
                        break;
                    }
                }

                ScanResult {
                    scanned,
                    // Covering the whole store restarts from the top next pass.
                    next_cursor: if scanned >= self.buckets.len() {
                        None
                    } else {
                        next_cursor
                    },
                    wrapped,
                }
            }

            fn lookup(&self, routing_bucket: u32) -> Option<BucketSample> {
                self.buckets
                    .get(&routing_bucket)
                    .map(|bucket| self.sample(routing_bucket, bucket))
            }
        }

        let config = super::evict_sampler_config();
        let mut shards = self.shards.write().expect("shards lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return Vec::new();
        };
        // Take the sampler state out so the scan can borrow the bucket index immutably.
        let mut sampler = std::mem::take(&mut shard.evict_sampler);
        let selected = {
            let source = IndexSource {
                buckets: &shard.bucket_index.bucket_map,
                recency: &shard.bucket_recency,
            };
            select_victims(&mut sampler, config, batch_limit, source).victims
        };
        shard.evict_sampler = sampler;

        // Byte totals for the chosen buckets only. Bounded by batch_limit, not by store size.
        selected
            .into_iter()
            .filter_map(|routing_bucket| {
                let bucket = shard.bucket_index.bucket_map.get(&routing_bucket)?;
                let mut logical_bytes = 0u64;
                let mut physical_bytes = 0u64;
                let mut dirty_object_count = 0u64;
                for page in bucket.page_index.values() {
                    if page.deleted {
                        continue;
                    }
                    logical_bytes = logical_bytes.saturating_add(page.address.length);
                    physical_bytes = physical_bytes.saturating_add(page.address.length);
                    if page.dirty {
                        dirty_object_count = dirty_object_count.saturating_add(1);
                    }
                }
                let cache = cache_by_bucket.get(&routing_bucket);
                let cache_memory_bytes = cache.map(|cache| cache.memory_bytes).unwrap_or_default();
                let cache_disk_bytes = cache.map(|cache| cache.disk_bytes).unwrap_or_default();
                Some(StorageEvictionVictim {
                    routing_bucket,
                    object_count: bucket.object_index.len() as u64,
                    logical_bytes,
                    physical_bytes,
                    cache_memory_bytes,
                    cache_disk_bytes,
                    dirty_object_count,
                    weight: cache_memory_bytes
                        .saturating_mul(4)
                        .saturating_add(cache_disk_bytes.saturating_mul(2))
                        .saturating_add(physical_bytes)
                        .saturating_add(dirty_object_count.saturating_mul(1024)),
                    last_touched_ms: shard
                        .bucket_recency
                        .get(&routing_bucket)
                        .copied()
                        .unwrap_or(0),
                })
            })
            .collect()
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
        let cache_by_bucket = before_cache
            .bucket_summaries
            .iter()
            .map(|summary| (summary.routing_bucket, summary.clone()))
            .collect::<BTreeMap<_, _>>();
        let recency_by_bucket = {
            let shards = self.shards.read().expect("engine lock poisoned");
            shards
                .get(&shard_id)
                .map(|shard| shard.bucket_recency.clone())
                .unwrap_or_default()
        };
        let victims = if self
            .evict_sampled_lru
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.sampled_eviction_victims(shard_id, batch_limit, &cache_by_bucket)
        } else {
            let mut victims = self
                .bucket_storage_summaries(shard_id)
                .into_iter()
                .map(|summary| {
                    let cache = cache_by_bucket.get(&summary.routing_bucket);
                    let cache_memory_bytes =
                        cache.map(|cache| cache.memory_bytes).unwrap_or_default();
                    let cache_disk_bytes = cache.map(|cache| cache.disk_bytes).unwrap_or_default();
                    StorageEvictionVictim {
                        routing_bucket: summary.routing_bucket,
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
                        last_touched_ms: recency_by_bucket
                            .get(&summary.routing_bucket)
                            .copied()
                            .unwrap_or(0),
                    }
                })
                .filter(|victim| victim.weight > 0)
                .collect::<Vec<_>>();
            // The LRU policy sorts candidates by last-used time, then evicts
            // least-recently-used buckets first. Never-touched buckets (last_touched_ms ==
            // 0) are coldest and go first; ties fall back to the heavier bucket, then the
            // lower routing_bucket for determinism.
            victims.sort_by(|left, right| {
                left.last_touched_ms
                    .cmp(&right.last_touched_ms)
                    .then_with(|| right.weight.cmp(&left.weight))
                    .then_with(|| left.routing_bucket.cmp(&right.routing_bucket))
            });
            if batch_limit > 0 && victims.len() > batch_limit {
                victims.truncate(batch_limit);
            }
            victims
        };
        let mut dump_manifest_ids = Vec::new();
        if dump_before_evict {
            let dirty_buckets = victims
                .iter()
                .filter(|victim| victim.dirty_object_count > 0)
                .map(|victim| victim.routing_bucket)
                .collect::<Vec<_>>();
            if !dirty_buckets.is_empty() {
                if let Ok(manifest) = self.create_bucket_dump_manifest(shard_id, dirty_buckets) {
                    dump_manifest_ids.push(manifest.manifest_id);
                }
            }
        }
        let mut cache_entries_removed = 0usize;
        let mut cache_disk_bytes_removed = 0u64;
        for victim in &victims {
            if let Ok(report) = self.cache.invalidate_slot(shard_id, victim.routing_bucket) {
                cache_entries_removed =
                    cache_entries_removed.saturating_add(report.memory_entries_removed);
                cache_disk_bytes_removed =
                    cache_disk_bytes_removed.saturating_add(report.disk_bytes_removed);
            }
        }
        let mut dropped_object_count = 0usize;
        if delete_drop && !victims.is_empty() {
            let victim_buckets = victims
                .iter()
                .map(|victim| victim.routing_bucket)
                .collect::<BTreeSet<_>>();
            // Encoding the served index and writing it out are the expensive part of this
            // flush -- measured at 42 ms of encode alone for a 2,000-key shard, plus two file
            // writes -- and all of it used to happen while this write lock was held, so every
            // read and write on the shard queued behind it. The lock is only needed for the
            // mutations; a stamped CLONE (9 ms) carries the exact state out, and the encode and
            // the writes happen after the guard is dropped.
            let mut pending_index_flush = None;
            let mut shards = self.shards.write().expect("shards lock poisoned");
            if let Some(shard) = shards.get_mut(&shard_id) {
                let object_keys = collect_live_page_entries(shard)
                    .into_iter()
                    .filter_map(|entry| {
                        let bucket = entry
                            .address
                            .routing_bucket()
                            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
                        victim_buckets.contains(&bucket).then_some(entry.object_key)
                    })
                    .collect::<BTreeSet<_>>();
                let mut deleted_keys = Vec::new();
                for key in object_keys {
                    if delete_record(shard, &key) {
                        dropped_object_count = dropped_object_count.saturating_add(1);
                        invalidate_record_all(&self.cache, shard_id, &key);
                        deleted_keys.push(key);
                    }
                }
                if dropped_object_count > 0 {
                    // A delete_drop eviction is a LOGICAL delete, so it must follow the same
                    // durability discipline as the expiry sweep (recovery_sweep_compact.rs), which
                    // is the codebase's other logged deletion. Emitting no WAL tombstone here left
                    // the deletion (a) invisible to followers (never replicated), and (b) under
                    // MATRIXARK_BULK_INGEST -- where persist_index_bytes is a no-op -- neither
                    // persisted NOR recoverable from the WAL, so the key resurrected on reload when
                    // replay reapplied the earlier SET. Emit a CommonDelete tombstone per dropped
                    // key (buffered/unfsynced, mirroring the expiry sweep) and anchor
                    // applied_wal_sequence past them so replay observes the deletion instead of the
                    // stale write. (eviction never deletes -- it only dumps and drops from cache -- so
                    // there is no analog; this aligns the Rust-only delete_drop path with the
                    // engine's own tombstone discipline.)
                    if !replaying_wal() {
                        for key in &deleted_keys {
                            let command = Command::CommonDelete { key: key.clone().to_string() };
                            let appended =
                                self.wal_store
                                    .append_with_sync(shard_id, command.clone(), false);
                            // Same reasoning as the expiry sweep: a drop that deletes is a
                            // deletion, and it has to reach every log a successor may replay.
                            if appended.is_ok() {
                                self.mirror_maintenance_write(shard_id, &command);
                            }
                        }
                        shard.applied_wal_sequence =
                            Some(self.wal_store.stats(shard_id).last_sequence);
                    }
                    // Stamp here so the clone carries the current on-disk shape, then hand
                    // the snapshot out; the encode and both writes happen below, unlocked.
                    shard.index_format_version = super::SHARD_INDEX_FORMAT_VERSION;
                    pending_index_flush = Some(shard.clone());
                }
            }
            drop(shards);
            if let Some(snapshot) = pending_index_flush {
                let index_bytes = super::serialize_index(&snapshot);
                let _ = self.persist_index_bytes(shard_id, &index_bytes);
                let _ = self.index_log_store.append_index_bytes(shard_id, &index_bytes);
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
