// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Index install/recovery + expiry sweep + page compaction methods for TemporalEngine, split from engine.rs.
use super::*;

impl TemporalEngine {
    pub fn install_index_bytes(
        &self,
        shard_id: ShardId,
        bytes: &[u8],
    ) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.index_dir)?;
        fs::write(self.index_path(shard_id), bytes)
    }

    pub fn storage_recovery_report(&self, shard_id: ShardId) -> StorageRecoveryReport {
        let mut report = self.storage_recovery_report_without_boundary(shard_id);
        report.boundary = self.storage_recovery_boundary_report(shard_id);
        report.slab_integrity =
            storage_slab_integrity_report(shard_id, &report, &report.boundary);
        report
    }

    pub(super) fn storage_recovery_report_without_boundary(&self, shard_id: ShardId) -> StorageRecoveryReport {
        // Durable served-index size. The base is materialized only at compaction, so a fresh
        // crash-recovered shard has no base file yet -- the durable served index is the base
        // folded with the index-log deltas, whose reconstructed size we report (via the
        // served-index funnel) so recovery diagnostics reflect real durable state.
        let base_index_bytes = self
            .index_path(shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let index_bytes = if base_index_bytes == 0 {
            self.load_served_index_bytes(shard_id)
                .map(|bytes| bytes.len() as u64)
                .unwrap_or_default()
        } else {
            base_index_bytes
        };
        let wal_records = self
            .wal_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let index_log_records = self
            .index_log_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let active_page_slab_ids = self.page_store.slab_ids().unwrap_or_default();
        let zone_descriptors = self.page_store.zone_descriptors();
        let zone_summary = self.page_store.zone_summary();
        let page_slab_reports = self.page_store.slab_reports().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        let addresses = shards
            .get(&shard_id)
            .map(collect_live_page_addresses)
            .unwrap_or_default();
        let total_page_refs = addresses.len();
        let mut readable_page_refs = 0usize;
        let mut unreadable_page_refs = Vec::new();
        let mut owner_mismatch_page_refs = Vec::new();
        let mut missing_owner_page_refs = 0usize;
        let mut object_lifecycle = StorageObjectLifecycleReport::default();
        let mut feature_page_layout = StorageFeaturePageLayoutReport::default();
        let mut page_slab_live_reports = page_slab_reports
            .iter()
            .map(|report| {
                (
                    report.page_slab_id,
                    StorageRecoverySlabLiveReport {
                        page_slab_id: report.page_slab_id,
                        physical_bytes: report.physical_bytes,
                        logical_bytes: report.logical_bytes,
                        page_count: report.page_count,
                        ..StorageRecoverySlabLiveReport::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut live_object_ids = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut live_routing_buckets = BTreeMap::<u64, BTreeSet<u32>>::new();
        for address in &addresses {
            let slab_report = page_slab_live_reports
                .entry(address.page_slab_id)
                .or_insert(StorageRecoverySlabLiveReport {
                    page_slab_id: address.page_slab_id,
                    ..StorageRecoverySlabLiveReport::default()
                });
            slab_report.live_page_refs = slab_report.live_page_refs.saturating_add(1);
            slab_report.live_physical_bytes = slab_report
                .live_physical_bytes
                .saturating_add(address.length);
            if let Some(object_id) = address.object_id() {
                let objects = live_object_ids.entry(address.page_slab_id).or_default();
                objects.insert(object_id);
                slab_report.live_object_count = objects.len() as u64;
            }
            if let Some(routing_bucket) = address.routing_bucket() {
                let buckets = live_routing_buckets
                    .entry(address.page_slab_id)
                    .or_default();
                buckets.insert(routing_bucket);
                slab_report.live_routing_bucket_count = buckets.len() as u64;
            }
            match self.page_store.read(address) {
                Ok(bytes) => {
                    readable_page_refs += 1;
                    slab_report.readable_live_page_refs =
                        slab_report.readable_live_page_refs.saturating_add(1);
                    slab_report.live_logical_bytes = slab_report
                        .live_logical_bytes
                        .saturating_add(bytes.len() as u64);
                }
                Err(err) => {
                    slab_report.unreadable_live_page_refs =
                        slab_report.unreadable_live_page_refs.saturating_add(1);
                    unreadable_page_refs.push(StorageRecoveryPageError {
                        page_slab_id: address.page_slab_id,
                        offset: address.offset,
                        length: address.length,
                        error: err.to_string(),
                    });
                }
            }
        }
        if let Some(shard) = shards.get(&shard_id) {
            let ownership = self.validate_shard_page_ownership(shard_id, shard);
            owner_mismatch_page_refs = ownership.mismatches;
            missing_owner_page_refs = ownership.missing_owner_page_refs;
            object_lifecycle = storage_object_lifecycle_report(shard_id, shard);
            object_lifecycle.owner_mismatch_page_refs = owner_mismatch_page_refs.len() as u64;
            object_lifecycle.missing_owner_page_refs = missing_owner_page_refs as u64;
            feature_page_layout = storage_feature_page_layout_report(&self.page_store, shard);
        }
        let page_slab_live_reports = page_slab_live_reports
            .into_values()
            .map(|mut report| {
                report.stale_page_estimate =
                    report.page_count.saturating_sub(report.live_page_refs);
                report.live_ref_density_basis_points = if report.page_count == 0 {
                    0
                } else {
                    report.live_page_refs.saturating_mul(10_000) / report.page_count
                };
                report
            })
            .collect::<Vec<_>>();
        object_lifecycle.stale_object_ids = page_slab_live_reports
            .iter()
            .map(|report| report.stale_page_estimate)
            .sum();
        let mut live_page_slab_ids = addresses
            .iter()
            .map(|address| address.page_slab_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        live_page_slab_ids.sort_unstable();
        StorageRecoveryReport {
            shard_id,
            index_bytes,
            index_write_atomic: true,
            wal_records,
            index_log_records,
            active_page_slab_ids,
            live_page_slab_ids,
            zone_descriptors,
            zone_summary,
            page_slab_reports,
            page_slab_live_reports,
            total_page_refs,
            readable_page_refs,
            unreadable_page_refs,
            owner_mismatch_page_refs,
            missing_owner_page_refs,
            object_lifecycle,
            all_live_pages_readable: total_page_refs == readable_page_refs,
            boundary: StorageRecoveryBoundaryReport::default(),
            slab_integrity: StorageSlabIntegrityReport::default(),
            feature_page_layout,
        }
    }

    pub fn live_page_slab_ids(&self, shard_id: ShardId) -> Vec<u64> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let mut ids = shards
            .get(&shard_id)
            .map(collect_live_page_slab_ids)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// Union of live page-slab ids across EVERY shard currently loaded into this engine.
    ///
    /// One engine owns a single `page_store` shared by all shards it hosts, and the current
    /// append cursor + slab counter are global, so two shards' pages can land in the same slab.
    /// Any slab referenced by *any* loaded shard is live and must not be reclaimed. A single
    /// shard's live set is therefore an unsafe basis for GC: a slab live only in shard B looks
    /// stale to shard A's cycle and would be deleted, silently destroying B's committed pages.
    /// Reclaim must be driven by this union so a slab referenced by any shard is retained.
    ///
    /// For a single loaded shard this equals `live_page_slab_ids(that_shard)`, so single-shard
    /// callers are unaffected.
    pub fn live_page_slab_ids_all_shards(&self) -> Vec<u64> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let mut ids = shards
            .values()
            .flat_map(collect_live_page_slab_ids)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

/// How far a round may walk to fill its window.
///
/// The window is the useful work; the budget stops a long run of keys in the other category from
/// turning a bounded round back into a walk of everything. Zero limits mean no limit, and then
/// there is nothing to bound.
fn expiry_scan_budget(limit: usize) -> usize {
    if limit == 0 {
        return 0;
    }
    limit.saturating_mul(8).max(64)
}

    pub fn sweep_expired_records(
        &self,
        shard_id: ShardId,
    ) -> Result<ShardExpirySweepReport, Status> {
        self.sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id,
            load_cold_buckets: true,
            ..ShardExpirySweepRequest::default()
        })
    }

    pub fn sweep_expired_records_with_request(
        &self,
        request: ShardExpirySweepRequest,
    ) -> Result<ShardExpirySweepReport, Status> {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        let now = now_ms();
        // Read each window from where its cursor left off. Asking about every deadline to find
        // a window of sixteen made a round cost the size of the whole set, on every cycle.
        let hot_limit = request.max_hot_buckets_per_round;
        let cold_limit = request.max_cold_buckets_per_round;
        let scan_budget = Self::expiry_scan_budget(hot_limit.max(cold_limit));
        let (hot_selected, next_hot_cursor) = crate::engine::expiry_window(
            &shard.expires_at_ms,
            request.hot_cursor.as_deref(),
            hot_limit,
            scan_budget,
            |key| record_exists(shard, key),
        );
        let (cold_selected, next_cold_cursor) = crate::engine::expiry_window(
            &shard.expires_at_ms,
            request.cold_cursor.as_deref(),
            cold_limit,
            scan_budget,
            |key| !record_exists(shard, key),
        );
        let mut expired_records_removed = 0;
        let mut skipped_records = 0usize;
        let mut loaded_for_expire = 0usize;
        let mut expired_keys: Vec<String> = Vec::new();
        for (key, expires_at) in hot_selected.iter() {
            if *expires_at <= now {
                if delete_record(shard, key) {
                    invalidate_record_all(&self.cache, request.shard_id, key);
                    expired_records_removed += 1;
                    expired_keys.push(key.clone());
                }
            } else {
                skipped_records = skipped_records.saturating_add(1);
            }
        }
        for (key, expires_at) in cold_selected.iter() {
            if *expires_at <= now {
                if request.load_cold_buckets {
                    loaded_for_expire = loaded_for_expire.saturating_add(1);
                    if delete_record(shard, key) {
                        invalidate_record_all(&self.cache, request.shard_id, key);
                        expired_records_removed += 1;
                        expired_keys.push(key.clone());
                    } else {
                        shard.expires_at_ms.remove(key);
                    }
                } else {
                    skipped_records = skipped_records.saturating_add(1);
                }
            } else {
                skipped_records = skipped_records.saturating_add(1);
            }
        }
        if expired_records_removed > 0 {
            // Expiry IS a logged,
            // replicated delete. Emit a WAL tombstone per expired key -- buffered and
            // unfsynced, mirroring the fire-and-forget commit -- so followers and WAL
            // replay observe the deletion instead of relying on each node running its own
            // sweep with its own clock/enable_expire. Then anchor the served snapshot past
            // the tombstones so a restart does not resurrect the key by replaying the
            // earlier SET/EXPIRE records.
            if !replaying_wal() {
                for key in &expired_keys {
                    let command = Command::CommonDelete { key: key.clone() };
                    let appended = self
                        .wal_store
                        .append_with_sync(request.shard_id, command.clone(), false);
                    // An expiry is a real deletion, so it has to reach every log that a
                    // successor might replay -- not only this node's.
                    if appended.is_ok() {
                        self.mirror_maintenance_write(request.shard_id, &command);
                    }
                }
                shard.applied_wal_sequence =
                    Some(self.wal_store.stats(request.shard_id).last_sequence);
            }
            let index_bytes = Ok::<_, serde_json::Error>(super::serialize_index_stamped(shard))
                .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
            self.persist_index_bytes(request.shard_id, &index_bytes)
                .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
            let _ = self
                .index_log_store
                .append_index_bytes(request.shard_id, &index_bytes);
        }
        Ok(ShardExpirySweepReport {
            shard_id: request.shard_id,
            expired_records_removed,
            hot_buckets_scanned: hot_selected.len(),
            cold_buckets_scanned: cold_selected.len(),
            scanned_records: hot_selected.len().saturating_add(cold_selected.len()),
            skipped_records,
            loaded_for_expire,
            next_hot_cursor,
            next_cold_cursor,
            round_limit: hot_limit.saturating_add(cold_limit),
            load_on_expire_only_when_needed: true,
        })
    }

    pub fn sweep_all_expired_records(&self) -> Vec<ShardExpirySweepReport> {
        self.loaded_shard_ids()
            .into_iter()
            .filter_map(|shard_id| self.sweep_expired_records(shard_id).ok())
            .collect()
    }

    pub(super) fn validate_shard_page_ownership(
        &self,
        shard_id: ShardId,
        shard: &ShardState,
    ) -> StoragePageOwnershipValidation {
        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        validate_bucket_ownership_index(shard_id, shard, start_routing_bucket, end_routing_bucket)
    }

    pub fn compact_shard_pages(&self, shard_id: ShardId) -> Result<ShardCompactionReport, Status> {
        let (start_routing_bucket, end_routing_bucket) = self
            .infos
            .read()
            .expect("shard info lock poisoned")
            .get(&shard_id)
            .map(|info| (info.start_routing_bucket, info.end_routing_bucket))
            .unwrap_or((0, u32::MAX));
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        let ownership = self.validate_shard_page_ownership(shard_id, shard);
        if !ownership.mismatches.is_empty() {
            return Err(Status::error(
                "page_compaction_owner_mismatch",
                format!(
                    "refusing compaction because {} live page refs disagree with object/page/slot ownership",
                    ownership.mismatches.len()
                ),
            ));
        }
        let before_slabs = collect_live_page_slab_ids(shard);
        let before = compaction_utility_report(&self.page_store, shard);
        let tombstoned_object_ids_before =
            storage_object_lifecycle_report(shard_id, shard).tombstoned_object_ids;
        let model_layouts_before = compaction_model_layout_reports(&self.page_store, shard);
        let object_manager_before =
            object_manager_runtime_report(shard_id, shard, start_routing_bucket, end_routing_bucket);
        let bucket_layout_transition_count_before = object_manager_before.layout_transition_count;
        let roll = self
            .page_store
            .roll_slab()
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let mut rewrite_stats = CompactionRewriteStats::default();

        // Relocate every model's live pages onto the freshly rolled slab. A mid-way failure
        // (append ENOSPC / an unreadable torn page) is caught below so we can durably commit the
        // consistent partial state instead of leaving the volatile index half-advanced but
        // unpersisted -- see the `if let Err(err)` handler after this block for why.
        let relocation_result: Result<(), Status> = (|| {
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            "string",
            shard.strings.values_mut(),
            &mut rewrite_stats,
        )?;
        for fields in shard.hashes.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "hash",
                fields.values_mut(),
                &mut rewrite_stats,
            )?;
        }
        for members in shard.zsets.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "zset",
                members.values_mut().map(|entry| &mut entry.1),
                &mut rewrite_stats,
            )?;
        }
        for elements in shard.lists.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "list",
                elements.values_mut(),
                &mut rewrite_stats,
            )?;
        }
        for members in shard.sets.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "set",
                members.values_mut(),
                &mut rewrite_stats,
            )?;
        }
        for series in shard.features.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "feature",
                series,
                &mut rewrite_stats,
            )?;
        }
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            "control_state",
            shard.control_state_pages.values_mut(),
            &mut rewrite_stats,
        )?;
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            "context_node",
            shard.context_nodes.values_mut(),
            &mut rewrite_stats,
        )?;
        for series in shard.context_events.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_event",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_indexes.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_index",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_audits.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_audit",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_children.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_child",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_summaries.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_summary",
                series,
                &mut rewrite_stats,
            )?;
        }
        for series in shard.context_compressions.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                "context_compression",
                series,
                &mut rewrite_stats,
            )?;
        }
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            "context_entity",
            shard
                .context_entities
                .values_mut()
                .flat_map(|series| series.values_mut()),
            &mut rewrite_stats,
        )?;
            Ok(())
        })();
        if let Err(err) = relocation_result {
            // A relocation failed partway. The in-memory index is now a CONSISTENT partial
            // snapshot -- pages already moved point at the fresh durable slab, the rest still point
            // at their old slabs -- but it has DIVERGED from the on-disk index, which still
            // references the now-vacated old slabs. Returning here without persisting (the old
            // behavior) let the independent next-cycle reclaim trust this volatile index, see a
            // fully-vacated old slab as stale, quarantine+purge it, and a later reload of the STALE
            // on-disk index would then dangle at the deleted slab -> silent durable data loss.
            // avoids the desync structurally (the compactor leaves the index unchanged on
            // failure and commits the rewrite atomically). We instead
            // durably commit the consistent partial: rebuild the secondary views so the serialized
            // index is internally consistent, fsync the relocated bytes so the index never names a
            // non-durable page, then persist -- leaving volatile == durable so reclaim is safe --
            // and propagate the original error so the caller knows compaction did not fully
            // complete (a later run retries the not-yet-moved pages).
            rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX);
            refresh_bucket_runtime_flags(shard);
            rebuild_bucket_page_ownership(shard_id, shard, start_routing_bucket, end_routing_bucket);
            self.page_store.sync_durable().map_err(|barrier| {
                Status::error(
                    "page_compaction_failed",
                    format!(
                        "durability barrier failed while committing a partial compaction: {barrier}"
                    ),
                )
            })?;
            let partial_index_bytes = Ok::<_, serde_json::Error>(super::serialize_index_stamped(shard))
                .map_err(|serialize| Status::error("page_compaction_failed", serialize.to_string()))?;
            self.persist_index_bytes(shard_id, &partial_index_bytes)
                .map_err(|persist| Status::error("page_compaction_failed", persist.to_string()))?;
            let _ = self.index_log_store.append_index_bytes(shard_id, &partial_index_bytes);
            return Err(err);
        }

        rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX);
        refresh_bucket_runtime_flags(shard);
        let after_slabs = collect_live_page_slab_ids(shard);
        let after = compaction_utility_report(&self.page_store, shard);
        rebuild_bucket_page_ownership(shard_id, shard, start_routing_bucket, end_routing_bucket);
        let tombstoned_object_ids_after =
            storage_object_lifecycle_report(shard_id, shard).tombstoned_object_ids;
        let object_manager_after =
            object_manager_runtime_report(shard_id, shard, start_routing_bucket, end_routing_bucket);
        let bucket_layout_transition_count_after = object_manager_after.layout_transition_count;
        let bucket_layout_states_after = object_manager_after.layout_states;
        let stale_page_slab_ids = before_slabs
            .difference(&after_slabs)
            .copied()
            .collect::<Vec<_>>();
        let reclaimable_stale_page_slab_count = stale_page_slab_ids.len();
        let model_policy_family_count = before.model_policies.len();
        let tombstone_policy_model_count = before
            .model_policies
            .iter()
            .filter(|policy| policy.tombstone_compaction_triggered)
            .count();
        let stale_density_policy_model_count = before
            .model_policies
            .iter()
            .filter(|policy| policy.stale_density_triggered)
            .count();
        let layout_aware_policy_model_count = before
            .model_policies
            .iter()
            .filter(|policy| policy.layout_aware_rewrite_required)
            .count();
        // Durability barrier BEFORE publishing the base index that names the relocated pages.
        // Under deferred-fsync modes (bulk / page_wal_single_barrier -> append.rs
        // defer_data_sync) compaction relocates pages fsync-deferred, so the moved
        // bytes may still be in the page cache. Persisting a base index that references them
        // and crashing before the next barrier would leave dangling references at an un-synced
        // slab (compaction does not advance applied_wal_sequence, so WAL replay does not
        // re-derive them) = permanent silent loss. The partial-failure path above already
        // syncs here; the success path must too. Unconditional, matching that path.
        self.page_store.sync_durable().map_err(|barrier| {
            Status::error(
                "page_compaction_failed",
                format!(
                    "durability barrier failed before publishing the compacted base index: {barrier}"
                ),
            )
        })?;
        let index_bytes = Ok::<_, serde_json::Error>(super::serialize_index_stamped(shard))
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        self.persist_index_bytes(shard_id, &index_bytes)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = self.index_log_store.append_index_bytes(shard_id, &index_bytes);
        let rewritten_object_pages = rewrite_stats.rewritten_page_refs;
        let bucket_layout_transition_count =
            bucket_layout_transition_count_after.saturating_sub(bucket_layout_transition_count_before);
        let has_model_layouts = !model_layouts_before.is_empty();
        let preserves_tombstones = tombstoned_object_ids_after >= tombstoned_object_ids_before;
        let improves_density =
            before.live_ref_density_basis_points <= after.live_ref_density_basis_points;
        let has_layout_transitions = bucket_layout_transition_count > 0
            || bucket_layout_states_after
                .iter()
                .any(|state| state.object_count > 0);
        let mut model_layout_compaction_blockers = Vec::new();
        if rewritten_object_pages == 0 {
            model_layout_compaction_blockers.push("no live page refs were rewritten".to_string());
        }
        if !has_model_layouts {
            model_layout_compaction_blockers.push("model layout report is empty".to_string());
        }
        if !preserves_tombstones {
            model_layout_compaction_blockers
                .push("tombstone object count decreased during compaction".to_string());
        }
        if !improves_density {
            model_layout_compaction_blockers
                .push("live-ref density did not improve or remain stable".to_string());
        }
        if !has_layout_transitions {
            model_layout_compaction_blockers
                .push("slot layout transition evidence is missing".to_string());
        }
        Ok(ShardCompactionReport {
            shard_id,
            model_layout_compaction_ready: model_layout_compaction_blockers.is_empty(),
            model_layout_compaction_evidence: vec![
                "compaction rewrites live refs by model layout".to_string(),
                "packed timestamped model layouts preserve shared page refs".to_string(),
                "tombstone object ids are preserved across compaction".to_string(),
                "stale page density is removed from the compacted live set".to_string(),
                "slot layout transition counts and states are reported after compaction"
                    .to_string(),
                "per-model policies expose tombstone density, stale-page density, object-page packing, and cold-page rewrite eligibility".to_string(),
                "stale segments left behind by moved indexes are reported as reclaimable".to_string(),
            ],
            model_layout_compaction_blockers,
            previous_page_slab_id: roll.previous_page_slab_id,
            compacted_page_slab_id: roll.new_page_slab_id,
            rewritten_page_refs: rewrite_stats.rewritten_page_refs,
            cold_page_rewrite_refs: rewrite_stats.cold_page_rewrite_refs,
            object_page_pack_group_count: before
                .model_policies
                .iter()
                .map(|policy| policy.object_page_pack_group_count as usize)
                .sum(),
            stale_page_slab_ids,
            reclaimable_stale_page_slab_count,
            model_policy_family_count,
            tombstone_policy_model_count,
            stale_density_policy_model_count,
            layout_aware_policy_model_count,
            model_rewrite_policies: rewrite_stats.into_reports(&before),
            rewritten_object_pages,
            bucket_layout_transition_count,
            bucket_layout_states_after,
            tombstoned_object_ids_before,
            tombstoned_object_ids_after,
            model_layouts: model_layouts_before,
            before,
            after,
        })
    }
}
