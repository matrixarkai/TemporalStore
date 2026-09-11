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

    /// The reclaim candidates, built once from the narrow view and once from the full report.
    ///
    /// Same selection function, two sources for the tally it reads. The narrow view leaves the
    /// read-dependent fields at zero, so this is the check that the planner never looked at
    /// them.
    #[cfg(test)]
    pub(crate) fn reclaim_candidates_two_ways_for_test(
        &self,
        shard_id: ShardId,
    ) -> (Vec<StorageReclaimCandidate>, Vec<StorageReclaimCandidate>) {
        let live = self
            .live_block_slab_ids(shard_id)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let stale = self
            .page_store
            .slab_ids()
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !live.contains(id))
            .collect::<BTreeSet<_>>();
        let narrow = storage_reclaim_candidates_from_slab_reports(
            &self.storage_reclaim_slab_reports(shard_id),
            &stale,
        );
        let full = storage_reclaim_candidates_from_slab_reports(
            &self.storage_recovery_report(shard_id).block_slab_live_reports,
            &stale,
        );
        (narrow, full)
    }

    /// Each dirty bucket's first undumped write sequence, for the test that pins it.
    #[cfg(test)]
    pub(crate) fn first_dirty_sequences_for_test(&self, shard_id: ShardId) -> Vec<(u32, u64)> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return Vec::new();
        };
        let mut out = shard
            .bucket_index
            .bucket_map
            .iter()
            .map(|(routing_bucket, bucket)| (*routing_bucket, bucket.first_dirty_wal_sequence))
            .collect::<Vec<_>>();
        out.sort();
        out
    }

    /// The per-slab live/stale tally the reclaim planner reads, without the whole-store scan.
    ///
    /// `storage_reclaim_candidates_from_slab_reports` consumes seven fields off each of these:
    /// the slab id, its physical bytes and page count, its live page refs and live physical
    /// bytes, and the two figures derived from those. Not one of them needs the page itself --
    /// a page's physical size is in the address that names it. The recovery report supplied
    /// them anyway, and supplied them by reading every live page off the block store to fill
    /// in `live_logical_bytes`, which the planner never reads.
    ///
    /// The fields that DO require the read -- `live_logical_bytes`, `readable_live_page_refs`,
    /// `unreadable_live_page_refs` -- are left at zero here, so this is not a drop-in for the
    /// report: it is the planner's view, and `the_reclaim_planner_sees_the_same_candidates`
    /// pins it to the answer the report produced.
    pub(super) fn storage_reclaim_slab_reports(
        &self,
        shard_id: ShardId,
    ) -> Vec<StorageRecoverySlabLiveReport> {
        // Counted by header walk, not `slab_reports()`. That function calls
        // `decode_page_record` on every record in every slab -- a CRC32C verify and a
        // decompress each -- which is a full integrity pass over the whole store, and the
        // selection below reads two fields out of it: the slab's size and its block count.
        // Measured at 32,000 records: 12.95 ms against 0.37 ms, 35x, with identical counts.
        //
        // `logical_bytes` is left at zero here for the same reason as the read-dependent
        // fields: `storage_reclaim_candidates_from_slab_reports` does not read it, and
        // `the_reclaim_planner_sees_the_same_candidates` is what holds that true.
        let block_slab_counts = self.page_store.slab_block_counts().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        let addresses = shards
            .get(&shard_id)
            .map(collect_live_page_addresses)
            .unwrap_or_default();
        let mut reports = block_slab_counts
            .iter()
            .map(|(block_slab_id, physical_bytes, block_count)| {
                (
                    *block_slab_id,
                    StorageRecoverySlabLiveReport {
                        block_slab_id: *block_slab_id,
                        physical_bytes: *physical_bytes,
                        page_count: *block_count,
                        ..StorageRecoverySlabLiveReport::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut live_object_ids = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut live_routing_buckets = BTreeMap::<u64, BTreeSet<u32>>::new();
        for address in &addresses {
            let slab_report = reports.entry(address.block_slab_id).or_insert(
                StorageRecoverySlabLiveReport {
                    block_slab_id: address.block_slab_id,
                    ..StorageRecoverySlabLiveReport::default()
                },
            );
            slab_report.live_page_refs = slab_report.live_page_refs.saturating_add(1);
            slab_report.live_physical_bytes = slab_report
                .live_physical_bytes
                .saturating_add(address.length);
            if let Some(object_id) = address.object_id() {
                let objects = live_object_ids.entry(address.block_slab_id).or_default();
                objects.insert(object_id);
                slab_report.live_object_count = objects.len() as u64;
            }
            if let Some(routing_bucket) = address.routing_bucket() {
                let buckets = live_routing_buckets
                    .entry(address.block_slab_id)
                    .or_default();
                buckets.insert(routing_bucket);
                slab_report.live_routing_bucket_count = buckets.len() as u64;
            }
        }
        reports
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
            .collect()
    }

    /// The object-lifecycle view on its own, without the whole-store scan around it.
    ///
    /// The recovery report produces this field as a by-product of reading EVERY live page off
    /// disk, which is how it counts the readable ones. Two callers on the maintenance path want
    /// nothing else from that report, so they were paying a full-store read to get it -- on a
    /// loop that runs every thirty seconds, for the life of the process.
    ///
    /// Nothing here reads a page. Every count comes from the shard's own maps, the ownership
    /// validation and the slab reports, which is all the field was ever made of:
    ///
    /// | at 32,000 live pages | the report | this |
    /// |---|---|---|
    /// | wall time | ~840 ms | ~45 ms |
    /// | pages read from the block store | 32,000 | 0 |
    ///
    /// `object_lifecycle_snapshot_matches_the_recovery_report` holds the two to the same answer,
    /// so a change to either that separates them fails there rather than in a shipped round.
    /// The snapshot, and the live page count, for the tests that hold it to the report.
    #[cfg(test)]
    pub(crate) fn storage_object_lifecycle_snapshot_for_test(
        &self,
        shard_id: ShardId,
    ) -> StorageObjectLifecycleReport {
        self.storage_object_lifecycle_snapshot(shard_id)
    }

    #[cfg(test)]
    pub(crate) fn live_page_count_for_test(&self, shard_id: ShardId) -> usize {
        let shards = self.shards.read().expect("engine lock poisoned");
        shards
            .get(&shard_id)
            .map(|shard| collect_live_page_addresses(shard).len())
            .unwrap_or_default()
    }

    pub(super) fn storage_object_lifecycle_snapshot(
        &self,
        shard_id: ShardId,
    ) -> StorageObjectLifecycleReport {
        // Header walk, not a full decode of every record -- see `storage_reclaim_slab_reports`.
        // Only the per-slab block count is read below.
        let block_slab_counts = self.page_store.slab_block_counts().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return StorageObjectLifecycleReport::default();
        };
        let ownership = self.validate_shard_page_ownership(shard_id, shard);
        let mut report = storage_object_lifecycle_report(shard_id, shard);
        report.owner_mismatch_page_refs = ownership.mismatches.len() as u64;
        report.missing_owner_page_refs = ownership.missing_owner_page_refs as u64;
        // stale_object_ids is the per-slab shortfall of live refs against the slab's own page
        // count, summed. An address naming a slab the store has no report for contributes
        // nothing (the report path gives it page_count 0, so its shortfall saturates to 0).
        let mut live_page_refs_by_slab = BTreeMap::<u64, u64>::new();
        for address in collect_live_page_addresses(shard) {
            *live_page_refs_by_slab
                .entry(address.block_slab_id)
                .or_default() += 1;
        }
        report.stale_object_ids = block_slab_counts
            .iter()
            .map(|(block_slab_id, _physical_bytes, block_count)| {
                block_count.saturating_sub(
                    live_page_refs_by_slab
                        .get(block_slab_id)
                        .copied()
                        .unwrap_or_default(),
                )
            })
            .sum();
        report
    }

    pub(super) fn storage_recovery_report_without_boundary(&self, shard_id: ShardId) -> StorageRecoveryReport {
        self.storage_recovery_report_without_boundary_sampled(shard_id, 0)
    }

    /// The same report, reading at most `readable_probe_limit` live pages this call.
    ///
    /// The readability check is the only part of this report that reads a page, and it reads
    /// EVERY live one: measured at 32,000 records it is 575 ms and 32,000 reads, about a fifth
    /// of a maintenance round, growing with the store.
    ///
    /// The maintenance cycle wants this report for `manifest_chain_issues`, the two dump
    /// sequences and the two replay sequences -- none of which reads a page. It never consults
    /// `unreadable_page_refs` or `unreadable_page_bytes`, but it does CARRY them in the report it
    /// returns, so simply not filling them in would be a silent lie to whoever reads that report.
    /// Sampling is the honest version: corruption is still found, over rounds rather than all in
    /// one, and `readable_probe_limit` says how much of the store this particular call looked at.
    ///
    /// 0 means no bound, matching every other round bound here. The diagnostic endpoint and the
    /// harnesses keep passing 0 and so keep scanning everything.
    pub(super) fn storage_recovery_report_without_boundary_sampled(
        &self,
        shard_id: ShardId,
        readable_probe_limit: usize,
    ) -> StorageRecoveryReport {
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
        // Counted, not collected. Both of these used to read their whole log into a vector
        // and take its length -- on the plan path of every maintenance round.
        let wal_records = self.wal_store.record_count(shard_id).unwrap_or_default();
        let index_log_records = self.index_log_store.record_count(shard_id).unwrap_or_default();
        let active_block_slab_ids = self.page_store.slab_ids().unwrap_or_default();
        let slab_descriptors = self.page_store.slab_descriptors();
        let slab_summary = self.page_store.slab_summary();
        let block_slab_reports = self.page_store.slab_reports().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        let addresses = shards
            .get(&shard_id)
            .map(collect_live_page_addresses)
            .unwrap_or_default();
        let total_page_refs = addresses.len();
        let mut readable_page_refs = 0usize;
        let mut probed_page_refs = 0usize;
        let mut unreadable_page_refs = Vec::new();
        let mut owner_mismatch_page_refs = Vec::new();
        let mut missing_owner_page_refs = 0usize;
        let mut object_lifecycle = StorageObjectLifecycleReport::default();
        let mut feature_page_layout = StorageFeaturePageLayoutReport::default();
        let mut block_slab_live_reports = block_slab_reports
            .iter()
            .map(|report| {
                (
                    report.block_slab_id,
                    StorageRecoverySlabLiveReport {
                        block_slab_id: report.block_slab_id,
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
            let slab_report = block_slab_live_reports
                .entry(address.block_slab_id)
                .or_insert(StorageRecoverySlabLiveReport {
                    block_slab_id: address.block_slab_id,
                    ..StorageRecoverySlabLiveReport::default()
                });
            slab_report.live_page_refs = slab_report.live_page_refs.saturating_add(1);
            slab_report.live_physical_bytes = slab_report
                .live_physical_bytes
                .saturating_add(address.length);
            if let Some(object_id) = address.object_id() {
                let objects = live_object_ids.entry(address.block_slab_id).or_default();
                objects.insert(object_id);
                slab_report.live_object_count = objects.len() as u64;
            }
            if let Some(routing_bucket) = address.routing_bucket() {
                let buckets = live_routing_buckets
                    .entry(address.block_slab_id)
                    .or_default();
                buckets.insert(routing_bucket);
                slab_report.live_routing_bucket_count = buckets.len() as u64;
            }
            // Past the sample budget this call stops READING, and keeps everything above that
            // does not need a read -- the per-slab live tallies are what the reclaim planner and
            // the object-lifecycle report are built from, and they must stay complete.
            if readable_probe_limit > 0 && probed_page_refs >= readable_probe_limit {
                continue;
            }
            probed_page_refs += 1;
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
                        block_slab_id: address.block_slab_id,
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
        let block_slab_live_reports = block_slab_live_reports
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
        object_lifecycle.stale_object_ids = block_slab_live_reports
            .iter()
            .map(|report| report.stale_page_estimate)
            .sum();
        let mut live_block_slab_ids = addresses
            .iter()
            .map(|address| address.block_slab_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        live_block_slab_ids.sort_unstable();
        StorageRecoveryReport {
            shard_id,
            index_bytes,
            index_write_atomic: true,
            wal_records,
            index_log_records,
            active_block_slab_ids,
            live_block_slab_ids,
            slab_descriptors,
            slab_summary,
            block_slab_reports,
            block_slab_live_reports,
            total_page_refs,
            readable_page_refs,
            unreadable_page_refs,
            owner_mismatch_page_refs,
            missing_owner_page_refs,
            object_lifecycle,
            // Against what was PROBED, not against every live page. With a sample budget the
            // two differ, and reading it as "every page is readable" when only some were tried
            // is exactly the false assurance this field exists to avoid.
            all_live_pages_readable: probed_page_refs == readable_page_refs,
            boundary: StorageRecoveryBoundaryReport::default(),
            slab_integrity: StorageSlabIntegrityReport::default(),
            feature_page_layout,
        }
    }

    pub fn live_block_slab_ids(&self, shard_id: ShardId) -> Vec<u64> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let mut ids = shards
            .get(&shard_id)
            .map(collect_live_block_slab_ids)
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
    /// For a single loaded shard this equals `live_block_slab_ids(that_shard)`, so single-shard
    /// callers are unaffected.
    pub fn live_block_slab_ids_all_shards(&self) -> Vec<u64> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let mut ids = shards
            .values()
            .flat_map(collect_live_block_slab_ids)
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
        self.compact_shard_pages_with_budgets(
            shard_id,
            COMPACTION_ROUND_BYTES,
            COMPACTION_ROUND_PAGE_REFS,
        )
    }

    /// Compact, relocating at most `budget_bytes` of pages this round.
    ///
    /// The budget is a parameter and not only a constant so a test can force MANY rounds over a
    /// handful of pages. A boundary that only appears once a store passes 256 MiB is a boundary
    /// no test would reach, and the rules that make a bounded round correct -- a round resumes
    /// onto the slab it was filling rather than rolling again, and rounds together still move
    /// every page -- all live at that boundary.
    /// Compact with a byte budget only, leaving the ref count unbounded.
    ///
    /// This is what the byte-budget probe measures and what every existing caller wants: adding
    /// a ref bound here would silently change what those measurements mean.
    pub(crate) fn compact_shard_pages_with_budget(
        &self,
        shard_id: ShardId,
        budget_bytes: u64,
    ) -> Result<ShardCompactionReport, Status> {
        self.compact_shard_pages_with_budgets(shard_id, budget_bytes, usize::MAX)
    }

    pub(crate) fn compact_shard_pages_with_budgets(
        &self,
        shard_id: ShardId,
        budget_bytes: u64,
        budget_page_refs: usize,
    ) -> Result<ShardCompactionReport, Status> {
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
        let before_slabs = collect_live_block_slab_ids(shard);
        let before = compaction_utility_report(&self.page_store, shard);
        let delete_marked_object_ids_before =
            storage_object_lifecycle_report(shard_id, shard).delete_marked_object_ids;
        let model_layouts_before = compaction_model_layout_reports(&self.page_store, shard);
        let object_manager_before =
            object_manager_runtime_report(shard_id, shard, start_routing_bucket, end_routing_bucket);
        let bucket_layout_transition_count_before = object_manager_before.layout_transition_count;
        // Start a round, or continue the one a budget cut short.
        //
        // A round rolls a fresh slab and relocates live pages onto it. Rolling AGAIN while a
        // round is unfinished would re-move everything the last round moved -- the pages it just
        // relocated would no longer be on the newest slab -- so a bounded round would shuffle
        // rather than progress. Continuing to fill the same slab is what makes each round move
        // pages that have not moved yet.
        let resumed = self
            .compaction_rounds
            .read()
            .expect("compaction round lock poisoned")
            .get(&shard_id)
            .copied();
        let (previous_block_slab_id, target_block_slab_id) = match resumed {
            Some(round) => round,
            None => {
                let roll = self
                    .page_store
                    .roll_slab()
                    .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
                (roll.previous_block_slab_id, roll.new_block_slab_id)
            }
        };
        let mut rewrite_stats =
            CompactionRewriteStats::for_round(target_block_slab_id, budget_bytes, budget_page_refs);

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
            // A compactor that leaves the index untouched on failure and commits the rewrite
            // atomically avoids the desync structurally. We instead
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

        // A round that spent its budget stays open, so the next one fills the same slab instead
        // of rolling a new one and re-moving what this one moved. A round that relocated
        // everything closes, and the next starts fresh.
        {
            let mut rounds = self
                .compaction_rounds
                .write()
                .expect("compaction round lock poisoned");
            if rewrite_stats.left_work_behind() {
                rounds.insert(shard_id, (previous_block_slab_id, target_block_slab_id));
            } else {
                rounds.remove(&shard_id);
            }
        }
        rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX);
        refresh_bucket_runtime_flags(shard);
        let after_slabs = collect_live_block_slab_ids(shard);
        let after = compaction_utility_report(&self.page_store, shard);
        rebuild_bucket_page_ownership(shard_id, shard, start_routing_bucket, end_routing_bucket);
        let delete_marked_object_ids_after =
            storage_object_lifecycle_report(shard_id, shard).delete_marked_object_ids;
        let object_manager_after =
            object_manager_runtime_report(shard_id, shard, start_routing_bucket, end_routing_bucket);
        let bucket_layout_transition_count_after = object_manager_after.layout_transition_count;
        let bucket_layout_states_after = object_manager_after.layout_states;
        let stale_block_slab_ids = before_slabs
            .difference(&after_slabs)
            .copied()
            .collect::<Vec<_>>();
        let reclaimable_stale_block_slab_count = stale_block_slab_ids.len();
        let model_policy_family_count = before.model_policies.len();
        let delete_marker_policy_model_count = before
            .model_policies
            .iter()
            .filter(|policy| policy.delete_marker_compaction_triggered)
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
        let preserves_delete_markers = delete_marked_object_ids_after >= delete_marked_object_ids_before;
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
        if !preserves_delete_markers {
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
            previous_block_slab_id,
            compacted_block_slab_id: target_block_slab_id,
            pages_left_by_budget: rewrite_stats.skipped_by_budget,
            bytes_left_by_budget: rewrite_stats.skipped_by_budget_bytes,
            rewritten_page_refs: rewrite_stats.rewritten_page_refs,
            cold_page_rewrite_refs: rewrite_stats.cold_page_rewrite_refs,
            object_page_pack_group_count: before
                .model_policies
                .iter()
                .map(|policy| policy.object_page_pack_group_count as usize)
                .sum(),
            stale_block_slab_ids,
            reclaimable_stale_block_slab_count,
            model_policy_family_count,
            delete_marker_policy_model_count,
            stale_density_policy_model_count,
            layout_aware_policy_model_count,
            model_rewrite_policies: rewrite_stats.into_reports(&before),
            rewritten_object_pages,
            bucket_layout_transition_count,
            bucket_layout_states_after,
            delete_marked_object_ids_before,
            delete_marked_object_ids_after,
            model_layouts: model_layouts_before,
            before,
            after,
        })
    }
}
