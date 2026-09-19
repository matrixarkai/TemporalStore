// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage readiness / compatibility / cache-warmup / recovery-boundary reports for TemporalEngine, split from engine.rs.
use super::*;

impl TemporalEngine {
    pub fn run_storage_manager_loop(
        &self,
        mut request: StorageManagerLoopRequest,
    ) -> StorageManagerLoopReport {
        request.lifecycle.shard_id = request.shard_id;
        let lifecycle = if request.apply {
            self.apply_storage_lifecycle(request.lifecycle.clone())
        } else {
            let plan = self.storage_lifecycle_plan(request.lifecycle.clone());
            StorageLifecycleReport {
                shard_id: request.shard_id,
                plan,
                object_lifecycle: self.storage_object_lifecycle_snapshot(request.shard_id),
                ..StorageLifecycleReport::default()
            }
        };

        let mut phases = Vec::new();
        phases.push(StorageManagerLoopPhaseReport {
            phase: "prepare".to_string(),
            attempted: true,
            applied: true,
            evidence: vec![
                "built storage lifecycle plan from dirty slots, live/stale segments, delayed destroy inventory, and manifest/index-log state".to_string(),
            ],
            blockers: Vec::new(),
        });

        phases.push(StorageManagerLoopPhaseReport {
            phase: "reclaim".to_string(),
            attempted: true,
            applied: request.apply
                && (!lifecycle.delayed_destroy_purged_slabs.is_empty()
                    || !lifecycle.plan.reclaim_candidates.is_empty()),
            evidence: vec![
                "ranked reclaim candidates by stale bytes, live density, delayed-destroy pressure, and utility score".to_string(),
            ],
            blockers: Vec::new(),
        });

        phases.push(StorageManagerLoopPhaseReport {
            phase: "evict".to_string(),
            attempted: request.lifecycle.invalidate_cache,
            applied: lifecycle.cache_entries_removed > 0 || lifecycle.cache_disk_bytes_removed > 0,
            evidence: vec![
                "cache invalidation phase uses shard-scoped cache eviction and byte accounting"
                    .to_string(),
            ],
            blockers: Vec::new(),
        });

        let expiry_sweep = if request.expire_records {
            self.sweep_expired_records(request.shard_id)
                .unwrap_or_else(|_| ShardExpirySweepReport {
                    shard_id: request.shard_id,
                    expired_records_removed: 0,
                    ..ShardExpirySweepReport::default()
                })
        } else {
            ShardExpirySweepReport {
                shard_id: request.shard_id,
                expired_records_removed: 0,
                ..ShardExpirySweepReport::default()
            }
        };
        phases.push(StorageManagerLoopPhaseReport {
            phase: "expire".to_string(),
            attempted: request.expire_records,
            applied: expiry_sweep.expired_records_removed > 0,
            evidence: vec![
                "expiry phase sweeps loaded shard TTL metadata and persists removals through index-log".to_string(),
            ],
            blockers: Vec::new(),
        });

        let compaction = if request.compact_blocks {
            match self.compact_shard_blocks(request.shard_id) {
                Ok(report) => Some(report),
                Err(err) => {
                    phases.push(StorageManagerLoopPhaseReport {
                        phase: "compact".to_string(),
                        attempted: true,
                        applied: false,
                        evidence: vec![
                            "compaction phase attempted live-page rewrite and model-layout/tombstone validation".to_string(),
                        ],
                        blockers: vec![err.message],
                    });
                    None
                }
            }
        } else {
            None
        };
        if let Some(report) = &compaction {
            phases.push(StorageManagerLoopPhaseReport {
                phase: "compact".to_string(),
                attempted: true,
                applied: report.model_layout_compaction_ready,
                evidence: report.model_layout_compaction_evidence.clone(),
                blockers: report.model_layout_compaction_blockers.clone(),
            });
        } else if !request.compact_blocks {
            phases.push(StorageManagerLoopPhaseReport {
                phase: "compact".to_string(),
                attempted: false,
                applied: false,
                evidence: vec![
                    "compaction phase can call compact_shard_pages when enabled".to_string()
                ],
                blockers: Vec::new(),
            });
        }

        phases.push(StorageManagerLoopPhaseReport {
            phase: "index_gc".to_string(),
            attempted: request.lifecycle.prune_bucket_dump_manifests
                || request.lifecycle.roll_forward_bucket_dump_installs,
            applied: lifecycle.manifest_prune_report.is_some()
                || !lifecycle.install_roll_forward_reports.is_empty(),
            evidence: vec![
                "index-GC phase prunes slot dump manifests and rolls forward interrupted installs using follower cursor retention".to_string(),
            ],
            blockers: Vec::new(),
        });

        let blockers = phases
            .iter()
            .flat_map(|phase| {
                phase
                    .blockers
                    .iter()
                    .map(|blocker| format!("{}: {blocker}", phase.phase))
            })
            .collect::<Vec<_>>();
        let attempted = phases.iter().filter(|phase| phase.attempted).count();
        let loop_ready = blockers.is_empty()
            && attempted >= 5
            && phases
                .iter()
                .any(|phase| phase.phase == "prepare" && phase.applied)
            && phases.iter().any(|phase| phase.phase == "reclaim")
            && phases.iter().any(|phase| phase.phase == "evict")
            && phases.iter().any(|phase| phase.phase == "expire")
            && phases
                .iter()
                .any(|phase| phase.phase == "compact" && phase.attempted)
            && phases.iter().any(|phase| phase.phase == "index_gc");
        StorageManagerLoopReport {
            shard_id: request.shard_id,
            loop_ready,
            phases,
            lifecycle,
            expiry_sweep,
            compaction,
            evidence: vec![
                "StorageManager loop executes prepare/reclaim/evict/expire/compact/index-GC phases through existing durable storage paths".to_string(),
                "loop report keeps per-phase evidence and blockers so readiness fails closed".to_string(),
            ],
            blockers,
        }
    }

    pub fn storage_production_readiness_report(
        &self,
        shard_id: ShardId,
    ) -> StorageProductionReadinessReport {
        self.storage_production_readiness_report_with_policy(
            shard_id,
            StorageProductionReadinessPolicy::default(),
        )
    }

    pub fn storage_production_readiness_report_with_policy(
        &self,
        shard_id: ShardId,
        policy: StorageProductionReadinessPolicy,
    ) -> StorageProductionReadinessReport {
        let boundary = self.storage_recovery_boundary_report(shard_id);
        let recovery = self.storage_recovery_report_without_boundary(shard_id);
        let slab_integrity = storage_slab_integrity_report(shard_id, &recovery, &boundary);
        let plan = self.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id,
            selected_dump_buckets: Vec::new(),
            max_dump_buckets_per_round: 0,
            min_undumped_wal_records: 0,
            min_undumped_wal_bytes: 0,
            purge_delayed_destroy: false,
            purge_delayed_destroy_slab_ids: None,
            prune_bucket_dump_manifests: false,
            roll_forward_bucket_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            block_gc_shared_store_cursors: Vec::new(),
            block_gc_raft_snapshot_refs: Vec::new(),
            block_gc_checkpoint_floor_slab_id: None,
            block_gc_raft_install_floor_slab_id: None,
            block_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        });
        let stats = self
            .loaded_shard_stats()
            .into_iter()
            .find(|stats| stats.shard_id == shard_id);
        let cache = stats
            .as_ref()
            .map(|stats| stats.cache.clone())
            .unwrap_or_else(|| self.cache.stats());
        let block_store = stats
            .as_ref()
            .map(|stats| stats.block_store_compat.clone())
            .unwrap_or_else(|| self.block_store.stats());
        let log_compatibility = self.storage_log_compatibility_report(shard_id);
        let block_format_compatibility = self.storage_block_format_compatibility_report(shard_id);
        let bucket_dump_manifest_count = self.list_bucket_dump_manifests(shard_id).len();
        let interrupted_bucket_dump_install_count = boundary.interrupted_bucket_dump_installs.len();
        let undumped_wal_records = boundary
            .latest_safe_wal_sequence
            .saturating_sub(boundary.latest_dump_wal_sequence);
        let mut blockers = Vec::new();
        if !boundary.stale_index_block_refs.is_empty() {
            blockers.push("stale_index_page_refs".to_string());
        }
        if !boundary.corrupt_block_slab_ids.is_empty() {
            blockers.push("corrupt_page_segments".to_string());
        }
        if boundary.unreadable_block_bytes > 0 || !recovery.all_live_blocks_readable {
            blockers.push("unreadable_live_page_refs".to_string());
        }
        if !boundary.owner_mismatch_block_refs.is_empty() {
            blockers.push("owner_mismatch_page_refs".to_string());
        }
        if boundary.object_lifecycle.missing_owner_block_refs > 0 {
            blockers.push("missing_owner_page_refs".to_string());
        }
        if boundary.object_lifecycle.reused_object_id_conflicts > 0 {
            blockers.push("reused_object_id_conflicts".to_string());
        }
        if interrupted_bucket_dump_install_count > 0 {
            blockers.push("interrupted_slot_dump_installs".to_string());
        }
        if !boundary.manifest_chain_issues.is_empty() {
            blockers.push("broken_slot_dump_manifest_chain".to_string());
        }
        if !slab_integrity.integrity_ok
            && !blockers
                .iter()
                .any(|blocker| blocker == "storage_segment_integrity_failed")
        {
            blockers.push("storage_segment_integrity_failed".to_string());
        }
        if recovery.feature_block_layout.has_errors() {
            blockers.push("feature_page_layout_mismatch".to_string());
        }

        let mut warnings = Vec::new();
        if !plan.dirty_buckets.is_empty() {
            warnings.push("dirty_slots_pending_dump".to_string());
        }
        if !plan.stale_block_slab_ids.is_empty() {
            warnings.push("stale_page_segments_pending_gc".to_string());
        }
        if !boundary.orphan_block_slab_ids.is_empty() {
            warnings.push("orphan_page_segments_pending_gc".to_string());
        }
        if bucket_dump_manifest_count == 0 && recovery.total_block_refs > 0 {
            warnings.push("no_slot_dump_manifest_for_live_pages".to_string());
        }
        if policy
            .max_dirty_buckets
            .map(|limit| plan.dirty_buckets.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("dirty_slots_exceed_policy".to_string());
        }
        if policy
            .max_stale_block_slabs
            .map(|limit| plan.stale_block_slab_ids.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("stale_page_segments_exceed_policy".to_string());
        }
        if policy
            .max_orphan_block_slabs
            .map(|limit| boundary.orphan_block_slab_ids.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("orphan_page_segments_exceed_policy".to_string());
        }
        if policy
            .max_undumped_wal_records
            .map(|limit| undumped_wal_records > limit)
            .unwrap_or(false)
        {
            blockers.push("undumped_wal_records_exceed_policy".to_string());
        }
        if policy.require_bucket_dump_manifest
            && bucket_dump_manifest_count == 0
            && recovery.total_block_refs > 0
        {
            blockers.push("slot_dump_manifest_required".to_string());
        }
        if policy.block_on_warnings && !warnings.is_empty() {
            blockers.push("warnings_exceed_policy".to_string());
        }

        StorageProductionReadinessReport {
            shard_id,
            policy,
            production_ready: blockers.is_empty(),
            blockers,
            warnings,
            dirty_bucket_count: plan.dirty_buckets.len(),
            stale_block_slab_count: plan.stale_block_slab_ids.len(),
            orphan_block_slab_count: boundary.orphan_block_slab_ids.len(),
            undumped_wal_records,
            corrupt_block_slab_count: boundary.corrupt_block_slab_ids.len(),
            unreadable_block_ref_count: recovery.unreadable_block_refs.len(),
            owner_mismatch_block_ref_count: boundary.owner_mismatch_block_refs.len(),
            missing_owner_block_ref_count: boundary.object_lifecycle.missing_owner_block_refs,
            reused_object_id_conflict_count: boundary.object_lifecycle.reused_object_id_conflicts,
            interrupted_bucket_dump_install_count,
            prepared_bucket_dump_install_count: boundary.prepared_bucket_dump_install_count,
            installed_bucket_dump_install_count: boundary.installed_bucket_dump_install_count,
            unknown_bucket_dump_install_count: boundary.unknown_bucket_dump_install_count,
            bucket_dump_manifest_count,
            cache_memory_bytes: cache.memory_bytes,
            cache_disk_bytes: cache.disk_bytes,
            block_store_bytes_written: block_store.bytes_written,
            boundary,
            object_lifecycle: recovery.object_lifecycle,
            slab_integrity,
            log_compatibility,
            block_format_compatibility,
            feature_block_layout_mismatch_count: recovery.feature_block_layout.mismatch_count(),
            corrupt_feature_block_count: recovery
                .feature_block_layout
                .corrupt_packed_feature_blocks
                .len(),
            feature_block_layout: recovery.feature_block_layout,
        }
    }

    /// The log figures the maintenance round reads, with no walk of either log.
    ///
    /// `stats()` answers both of these from the piece headers and the accumulated counters, which
    /// is work proportional to the number of PIECES. `storage_log_compatibility_report` answers
    /// them too, and counts the records on the way -- which is work proportional to the log's
    /// BYTES, decoded. The pressure sites want only what is here, so this is what they ask for.
    ///
    /// Taken from the same `stats()` calls as the full report, so the two cannot drift about what
    /// the log holds.
    pub fn storage_log_pressure_report(&self, shard_id: ShardId) -> StorageLogPressureReport {
        let wal_stats = self.wal_store.stats(shard_id);
        let index_log_stats = self.index_log_store.stats(shard_id);
        StorageLogPressureReport {
            shard_id,
            wal_last_sequence: wal_stats.last_sequence,
            index_log_last_sequence: index_log_stats.last_sequence,
            wal_bytes: wal_stats.bytes_written,
            index_log_bytes: index_log_stats.bytes_written,
        }
    }

    pub fn storage_log_compatibility_report(
        &self,
        shard_id: ShardId,
    ) -> StorageLogCompatibilityReport {
        let wal_stats = self.wal_store.stats(shard_id);
        let index_log_stats = self.index_log_store.stats(shard_id);
        // Counted, not collected -- the twin of the pair in the recovery report. This one is
        // reached through `storage_manager_pressure_snapshot`, so it is on the SAME maintenance
        // round as that pair: fixing only the other would have left every round still reading
        // both logs in full, from a second site.
        let wal_records = self.wal_store.record_count(shard_id).unwrap_or_default();
        let index_log_records = self.index_log_store.record_count(shard_id).unwrap_or_default();
        StorageLogCompatibilityReport {
            shard_id,
            wal_format: "rust-jsonl-command-v1".to_string(),
            index_log_format: "rust-jsonl-shard-index-v1".to_string(),
            compatibility_mode: "rust_native_migration_only".to_string(),
            migration_required: true,
            native_reader_supported: false,
            native_writer_supported: false,
            golden_conversion_required: true,
            rust_native_replay_safe: true,
            native_binary_compatible: false,
            wal_last_sequence: wal_stats.last_sequence,
            index_log_last_sequence: index_log_stats.last_sequence,
            wal_records,
            index_log_records,
            wal_bytes: wal_stats.bytes_written,
            index_log_bytes: index_log_stats.bytes_written,
            compatibility_gaps: vec![
                "compatibility mode is migration-only; direct mixed-format binary log serving is not supported"
                    .to_string(),
                "binary/protobuf wal reader and writer are not implemented".to_string(),
                "binary/protobuf index-log reader and writer are not implemented".to_string(),
                "golden log conversion/replay suite is required before migration".to_string(),
            ],
        }
    }

    pub fn storage_block_format_compatibility_report(
        &self,
        shard_id: ShardId,
    ) -> StorageBlockFormatCompatibilityReport {
        let stats = self.block_store.stats();
        let summary = self.block_store.slab_summary();
        StorageBlockFormatCompatibilityReport {
            shard_id,
            block_format: "rust-page-envelope-v6".to_string(),
            rust_envelope_version: 6,
            compatibility_mode: "rust_envelope_migration_only".to_string(),
            migration_required: true,
            native_block_header_reader_supported: false,
            native_block_header_writer_supported: false,
            golden_conversion_required: true,
            rust_native_read_safe: true,
            native_block_header_compatible: false,
            checksum_protected: true,
            object_ids_embedded: true,
            routing_buckets_embedded: true,
            compression_supported: true,
            active_slabs: summary.active_slabs,
            sealed_slabs: summary.sealed_slabs,
            delayed_destroy_slabs: summary.delayed_destroy_slabs,
            live_physical_bytes: summary.live_physical_bytes,
            reclaimable_physical_bytes: summary.reclaimable_physical_bytes,
            block_store_writes: stats.writes,
            block_store_bytes_written: stats.bytes_written,
            logical_bytes_written: stats.logical_bytes_written,
            compressed_records_written: stats.compressed_records_written,
            compatibility_gaps: vec![
                "compatibility mode is migration-only; direct mixed mixed-format page-header serving is not supported"
                    .to_string(),
                "binary protobuf page header reader and writer are not implemented".to_string(),
                "slot/page layout and page-id allocation are not byte-compatible".to_string(),
                "golden page conversion/replay suite is required before migration".to_string(),
            ],
        }
    }

    pub fn warm_cache_from_block_index(
        &self,
        shard_id: ShardId,
        selected_buckets: impl IntoIterator<Item = u32>,
    ) -> usize {
        self.storage_cache_warmup_report(shard_id, selected_buckets)
            .warmed_block_refs
    }

    pub fn storage_cache_warmup_report(
        &self,
        shard_id: ShardId,
        selected_buckets: impl IntoIterator<Item = u32>,
    ) -> StorageCacheWarmupReport {
        let selected_buckets = selected_buckets.into_iter().collect::<BTreeSet<_>>();
        let mut report = StorageCacheWarmupReport {
            shard_id,
            selected_buckets: selected_buckets.iter().copied().collect(),
            ..StorageCacheWarmupReport::default()
        };
        // THE ADDRESSES ARE CHOSEN UNDER THE GUARD; THE READS HAPPEN AFTER IT DROPS.
        //
        // This stage used to hold the shard-table READ guard across the whole loop below -- one
        // block-store read and one cache insert PER LIVE PAGE, with no per-round budget of any
        // kind. A read guard admits other readers, so this looks cheap and is not: it excludes
        // every WRITER on the shard for as long as the I/O takes, which on a warm of the whole
        // shard is every page in the store. The rest of the maintenance round is among the
        // writers it blocks.
        //
        // Nothing in the loop needs the shard. `collect_live_block_entries` already MATERIALIZES
        // the whole set into a Vec, and after that the body touches only `entry.address` and
        // `entry.object_key`, both owned by the Vec; `routing_bucket_for_key` reads `infos`, a
        // different lock. So the guard was being held for the producer's sake and paid for by
        // the consumer.
        //
        // WHAT A STALE ADDRESS MAKES THIS DO. Between the walk and the read a writer can delete
        // the page this entry names. The consequence is bounded in both directions: a read that
        // no longer resolves returns Err and is counted as `failed_page_refs`, which the loop
        // already handles, and a page that IS read populates the cache under
        // `CacheKey::page_with_slot` -- keyed by slab id, offset and length, i.e. by the physical
        // location whose bytes do not change while the slab is live. The entry is a cached copy
        // of bytes that are still on disk, not a claim that the page is live, and nothing reads
        // the cache to decide what is live. This is a REPORT and a cache fill; it decides no
        // reclaim, so a stale answer costs at most one wasted cache slot.
        let mut plan: Vec<(CacheKey, BlockAddress)> = Vec::new();
        let held_across_io = {
            let shards = self.shards_read_marked();
            let Some(shard) = shards.get(&shard_id) else {
                return report;
            };
            for entry in collect_live_block_entries(shard) {
                let routing_bucket = entry
                    .address
                    .routing_bucket()
                    .unwrap_or_else(|| self.routing_bucket_for_key(shard_id, &entry.object_key));
                if !selected_buckets.is_empty() && !selected_buckets.contains(&routing_bucket) {
                    report.skipped_block_refs = report.skipped_block_refs.saturating_add(1);
                    continue;
                }
                report.considered_block_refs = report.considered_block_refs.saturating_add(1);
                let key = CacheKey::page_with_slot(
                    shard_id,
                    entry.address.block_slab_id,
                    entry.address.offset,
                    entry.address.length,
                    entry.address.routing_bucket(),
                );
                plan.push((key, entry.address));
            }
            // The control arm of `the_cache_warmup_reads_blocks_after_the_shard_guard_drops` keeps
            // the guard held across the reads, so the guard has a positive control to compare
            // against rather than an assertion that nothing can fail.
            self.warm_cache_under_shard_guard
                .load(std::sync::atomic::Ordering::Relaxed)
                .then_some(shards)
        };
        for (key, address) in plan {
            if self.cache.peek_tier(&key).is_some() {
                report.already_cached_block_refs = report.already_cached_block_refs.saturating_add(1);
                report.warmed_block_refs = report.warmed_block_refs.saturating_add(1);
            } else if let Ok(bytes) = self.read_block_counted(&address) {
                report.page_store_reads = report.page_store_reads.saturating_add(1);
                report.block_store_reads = report.block_store_reads.saturating_add(1);
                let byte_len = bytes.len() as u64;
                match self.cache.put(key, bytes) {
                    Ok(()) => {
                        report.warmed_block_refs = report.warmed_block_refs.saturating_add(1);
                        report.warmed_bytes = report.warmed_bytes.saturating_add(byte_len);
                    }
                    Err(_) => {
                        report.failed_block_refs = report.failed_block_refs.saturating_add(1);
                    }
                }
            } else {
                report.failed_block_refs = report.failed_block_refs.saturating_add(1);
            }
        }
        drop(held_across_io);
        report
    }

    pub fn storage_cache_inspection_report(
        &self,
        shard_id: ShardId,
    ) -> StorageCacheInspectionReport {
        let entries = self.cache.entries_for_shard(shard_id);
        let mut bucket_summaries = BTreeMap::<u32, StorageCacheBucketSummary>::new();
        for entry in &entries {
            let Some(routing_bucket) = cache_entry_routing_bucket(entry) else {
                continue;
            };
            let summary = bucket_summaries
                .entry(routing_bucket)
                .or_insert(StorageCacheBucketSummary {
                    routing_bucket,
                    ..StorageCacheBucketSummary::default()
                });
            summary.entry_count = summary.entry_count.saturating_add(1);
            summary.memory_bytes = summary.memory_bytes.saturating_add(entry.memory_bytes);
            summary.disk_bytes = summary.disk_bytes.saturating_add(entry.disk_bytes);
            if entry.pinned {
                summary.pinned_entries = summary.pinned_entries.saturating_add(1);
                summary.pinned_bytes = summary.pinned_bytes.saturating_add(entry.memory_bytes);
            }
        }
        StorageCacheInspectionReport {
            shard_id,
            stats: self.cache.stats(),
            entries,
            bucket_summaries: bucket_summaries.into_values().collect(),
        }
    }

    pub fn invalidate_storage_cache_bucket(
        &self,
        request: StorageCacheInvalidateBucketRequest,
    ) -> Result<CacheGcReport, Status> {
        self.cache
            .invalidate_slot(request.shard_id, request.routing_bucket)
            .map_err(|err| Status::error("cache_slot_invalidation_failed", err.to_string()))
    }

    /// The boundary report, checking EVERY live page for readability.
    ///
    /// What the diagnostic endpoint and the harnesses want: the whole store looked at. The
    /// maintenance cycle calls the sampled form instead -- see
    /// `storage_recovery_report_without_boundary_sampled` for why.
    pub fn storage_recovery_boundary_report(
        &self,
        shard_id: ShardId,
    ) -> StorageRecoveryBoundaryReport {
        self.storage_recovery_boundary_report_sampled(shard_id, 0)
    }

    /// The same report, reading at most `readable_probe_limit` live pages. 0 means no bound.
    pub fn storage_recovery_boundary_report_sampled(
        &self,
        shard_id: ShardId,
        readable_probe_limit: usize,
    ) -> StorageRecoveryBoundaryReport {
        let manifests =
            list_bucket_dump_manifests_shared_at(&self.index_dir, shard_id).unwrap_or_default();
        let latest_dump_wal_sequence = manifests
            .iter()
            .map(|manifest| manifest.wal_sequence)
            .max()
            .unwrap_or_default();
        let latest_dump_index_log_sequence = manifests
            .iter()
            .map(|manifest| manifest.index_log_sequence)
            .max()
            .unwrap_or_default();
        let latest_safe_wal_sequence = self.wal_store.stats(shard_id).last_sequence;
        let latest_safe_index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let live_block_slab_ids = self
            .live_block_slab_ids(shard_id)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let all_slab_ids = self
            .block_store
            .slab_ids()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let orphan_block_slab_ids = all_slab_ids
            .difference(&live_block_slab_ids)
            .copied()
            .collect::<Vec<_>>();
        let latest_dump_buckets = manifests
            .last()
            .map(|manifest| manifest.bucket_ids.iter().copied().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let missing_dump_bucket_ids = self
            .bucket_storage_summaries(shard_id)
            .into_iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .map(|summary| summary.routing_bucket)
            .filter(|bucket| !latest_dump_buckets.contains(bucket))
            .collect::<Vec<_>>();
        let interrupted_bucket_dump_installs = self.interrupted_bucket_dump_installs(shard_id);
        let (
            prepared_bucket_dump_install_count,
            installed_bucket_dump_install_count,
            unknown_bucket_dump_install_count,
        ) = bucket_dump_install_phase_counts(&interrupted_bucket_dump_installs);
        let manifest_chain_issues = bucket_dump_manifest_chain_issues(&manifests);
        let recovery =
            self.storage_recovery_report_without_boundary_sampled(shard_id, readable_probe_limit);
        let corrupt_block_slab_ids = recovery
            .block_slab_reports
            .iter()
            .filter(|report| report.has_corruption)
            .map(|report| report.block_slab_id)
            .collect::<Vec<_>>();
        let unreadable_block_bytes = recovery
            .unreadable_block_refs
            .iter()
            .map(|error| error.length)
            .sum();
        let object_lifecycle = recovery.object_lifecycle.clone();
        StorageRecoveryBoundaryReport {
            shard_id,
            latest_safe_wal_sequence,
            latest_safe_index_log_sequence,
            latest_dump_wal_sequence,
            latest_dump_index_log_sequence,
            selected_replay_wal_sequence: latest_dump_wal_sequence
                .min(latest_safe_wal_sequence),
            selected_replay_index_log_sequence: latest_dump_index_log_sequence
                .min(latest_safe_index_log_sequence),
            orphan_block_slab_ids,
            missing_dump_bucket_ids,
            stale_index_block_refs: recovery.unreadable_block_refs,
            interrupted_bucket_dump_installs,
            prepared_bucket_dump_install_count,
            installed_bucket_dump_install_count,
            unknown_bucket_dump_install_count,
            manifest_chain_issues,
            owner_mismatch_block_refs: recovery.owner_mismatch_block_refs,
            missing_owner_block_refs: recovery.missing_owner_block_refs,
            object_lifecycle,
            corrupt_block_slab_ids,
            unreadable_block_bytes,
        }
    }
}
