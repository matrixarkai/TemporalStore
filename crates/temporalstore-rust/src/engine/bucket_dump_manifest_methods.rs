// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Bucket-dump manifest lifecycle methods for TemporalEngine, split from engine.rs.
use super::*;

impl TemporalEngine {
    pub fn create_bucket_dump_manifest(
        &self,
        shard_id: ShardId,
        selected_buckets: impl IntoIterator<Item = u32>,
    ) -> Result<BucketDumpManifest, Status> {
        // Make the pages + WAL the manifest is about to reference durable before we
        // capture their slab ids / wal_sequence. In live mode each append already
        // fsyncs, but under MATRIXARK_BULK_INGEST appends defer fsync, so a dump firing
        // mid-bulk would otherwise name slabs whose bytes are not yet on disk
        // (the dump path always commits blocks durably before updating the index).
        // The durability barrier MUST succeed before we capture slab ids / wal_sequence and
        // advance the dumped-log frontier -- the commit step refuses to touch
        // the index if the page/wal commit failed. Swallowing these errors let a dump whose
        // pages never reached disk (e.g. a sync_data EIO/ENOSPC under bulk mode) still clear
        // the bucket dirty and let WAL reclaim truncate the records backing those pages ->
        // silent data loss on crash. Propagate: an Err here skips clear_dumped_bucket_dirty_state
        // (apply_storage_lifecycle consumes the manifest via .ok()), so the bucket stays dirty
        // and the reclaim frontier does not advance.
        self.page_store.sync_durable().map_err(|err| {
            Status::error(
                "slot_dump_failed",
                format!("page durability barrier failed before dump capture: {err}"),
            )
        })?;
        self.wal_store.flush(shard_id).map_err(|err| {
            Status::error(
                "slot_dump_failed",
                format!("wal flush failed before dump capture: {err}"),
            )
        })?;
        let selected_buckets = selected_buckets.into_iter().collect::<BTreeSet<_>>();
        let summaries = self.bucket_storage_summaries(shard_id);
        if summaries.is_empty()
            && !self
                .shards
                .read()
                .expect("engine lock poisoned")
                .contains_key(&shard_id)
        {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        }
        let mut bucket_summaries = summaries
            .into_iter()
            .filter(|summary| {
                selected_buckets.is_empty() || selected_buckets.contains(&summary.routing_bucket)
            })
            .collect::<Vec<_>>();
        bucket_summaries.sort_by_key(|summary| summary.routing_bucket);
        let mut page_slab_ids = bucket_summaries
            .iter()
            .flat_map(|summary| summary.page_slab_ids.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        page_slab_ids.sort_unstable();
        let index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let index_bytes = self
            .export_index_bytes(shard_id)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        let index_sha256 = sha256_hex_bytes(&index_bytes);
        let dump_index_state = crate::engine::decode_index_bytes(&index_bytes)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        // The manifest's replay watermark MUST equal the WAL sequence the EMBEDDED index
        // actually reflects -- NOT the live WAL tail. Under MATRIXARK_BULK_INGEST the on-disk
        // index (what export_index_bytes returns) stays frozen at the last flush_shard_index
        // anchor while the WAL tail races ahead (per-command index persist is a no-op in bulk
        // mode). Taking the live tail here made install set replay_watermark PAST records the
        // embedded index never captured, so on reload WAL replay skipped them and reclaim
        // truncated them -> silent data loss. couples the two by construction: Load replays
        // from the dumped-log id stored INSIDE the dumped index, so the
        // replay cursor always equals the state the index captured. Derive the watermark from the
        // embedded index's own anchor so reload replays exactly the WAL suffix it lacks. (None
        // only for a shard never written/flushed -> empty index -> fall back to the tail, 0.)
        let wal_sequence = dump_index_state
            .applied_wal_sequence
            .unwrap_or_else(|| self.wal_store.stats(shard_id).last_sequence);
        let created_unix_ms = now_ms();
        let manifest_id = format!("{shard_id}-{index_log_sequence}-{created_unix_ms}");
        let parent_manifest_id = latest_bucket_dump_manifest_at(&self.index_dir, shard_id)
            .map(|manifest| manifest.manifest_id);
        let object_lifecycle = storage_object_lifecycle_report_for_buckets(
            shard_id,
            &dump_index_state,
            &selected_buckets,
            |key| self.routing_bucket_for_key(shard_id, key),
        );
        let mut manifest = BucketDumpManifest {
            version: 3,
            shard_id,
            manifest_id,
            manifest_kind: "slot_dump".to_string(),
            dump_generation_id: String::new(),
            source_manifest_ids: Vec::new(),
            source_manifest_coverage: Vec::new(),
            parent_manifest_id,
            load_version_handoff: None,
            created_unix_ms,
            bucket_ids: bucket_summaries
                .iter()
                .map(|summary| summary.routing_bucket)
                .collect(),
            page_slab_ids,
            wal_sequence,
            index_log_sequence,
            live_page_refs: bucket_summaries
                .iter()
                .map(|summary| summary.page_ref_count)
                .sum(),
            logical_bytes: bucket_summaries
                .iter()
                .map(|summary| summary.logical_bytes)
                .sum(),
            physical_bytes: bucket_summaries
                .iter()
                .map(|summary| summary.physical_bytes)
                .sum(),
            bucket_summaries,
            object_lifecycle,
            index_bytes,
            index_sha256,
            checksum: String::new(),
        };
        manifest.dump_generation_id = bucket_dump_generation_id(&manifest);
        manifest.checksum = bucket_dump_manifest_checksum(&manifest)?;
        self.persist_bucket_dump_manifest(&manifest)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        // The index-log is bounded by the storage-manager's consumer-aware index GC (see
        // `storage_wal_reclaim_plan` / `run_storage_manager_cycle`), which retains from the
        // durable dump frontier -- the min anchor across durable manifests, held back by any
        // lagging follower replay cursor or raft snapshot. That is the correct layer for GC
        // (this manifest-create path has no visibility into those consumers), and
        // `install_latest_manifest_if_newer_on_load` bridges the on-disk base to the manifest
        // anchor on reload, so fold(base + retained deltas) stays complete.
        Ok(manifest)
    }

    pub fn create_merged_bucket_dump_manifest(
        &self,
        shard_id: ShardId,
        selected_buckets: impl IntoIterator<Item = u32>,
        source_manifest_ids: impl IntoIterator<Item = String>,
        next_load_version: Option<u64>,
    ) -> Result<BucketDumpManifest, Status> {
        let selected_buckets = selected_buckets.into_iter().collect::<BTreeSet<_>>();
        let mut source_manifest_ids = source_manifest_ids.into_iter().collect::<Vec<_>>();
        source_manifest_ids.sort();
        source_manifest_ids.dedup();
        if source_manifest_ids.is_empty() {
            return Err(Status::error(
                "merged_slot_dump_missing_sources",
                "merged slot dump requires at least one source manifest",
            ));
        }
        let (source_manifest_count, missing_source_manifest_ids, source_manifest_bucket_ids) =
            self.bucket_dump_source_manifest_coverage(shard_id, &source_manifest_ids);
        if !missing_source_manifest_ids.is_empty() {
            return Err(Status::error(
                "merged_slot_dump_missing_sources",
                format!(
                    "merged slot dump source manifests are missing or invalid: {:?}",
                    missing_source_manifest_ids
                ),
            ));
        }
        let source_bucket_set = source_manifest_bucket_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let target_buckets = self
            .bucket_storage_summaries(shard_id)
            .into_iter()
            .filter(|summary| {
                selected_buckets.is_empty() || selected_buckets.contains(&summary.routing_bucket)
            })
            .map(|summary| summary.routing_bucket)
            .collect::<Vec<_>>();
        let missing_coverage = target_buckets
            .iter()
            .copied()
            .filter(|bucket| !source_bucket_set.contains(bucket))
            .collect::<Vec<_>>();
        if !missing_coverage.is_empty() {
            return Err(Status::error(
                "merged_slot_dump_source_coverage",
                format!(
                    "merged slot dump sources do not cover target slots: {:?}",
                    missing_coverage
                ),
            ));
        }
        let mut manifest = self.create_bucket_dump_manifest(shard_id, target_buckets)?;
        manifest.manifest_kind = "merged_slot_dump".to_string();
        // Capture each validated source's bucket coverage into the manifest so a
        // target engine that does not hold the source files can preflight coverage.
        manifest.source_manifest_coverage = source_manifest_ids
            .iter()
            .filter_map(|manifest_id| {
                bucket_dump_manifest_at(&self.index_dir, shard_id, manifest_id)
                    .ok()
                    .flatten()
                    .map(|source| BucketDumpSourceCoverage {
                        manifest_id: manifest_id.clone(),
                        bucket_ids: source.bucket_ids,
                    })
            })
            .collect();
        manifest.source_manifest_ids = source_manifest_ids;
        if let Some(next_load_version) = next_load_version {
            let previous_load_version = self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&shard_id)
                .map(|info| info.load_version)
                .unwrap_or_default();
            manifest.load_version_handoff = Some(BucketDumpLoadVersionHandoff {
                previous_load_version,
                next_load_version,
                applied: false,
            });
        }
        manifest.checksum = bucket_dump_manifest_checksum(&manifest)?;
        let (_, missing_source_manifest_ids, _, missing_bucket_ids) =
            self.bucket_dump_merged_source_preflight(&manifest);
        if !missing_source_manifest_ids.is_empty() || !missing_bucket_ids.is_empty() {
            return Err(Status::error(
                "merged_slot_dump_source_preflight",
                format!(
                    "merged source preflight failed sources={:?} missing_slots={:?}",
                    missing_source_manifest_ids, missing_bucket_ids
                ),
            ));
        }
        debug_assert!(source_manifest_count >= 1);
        self.persist_bucket_dump_manifest(&manifest)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        Ok(manifest)
    }

    pub fn list_bucket_dump_manifests(&self, shard_id: ShardId) -> Vec<BucketDumpManifest> {
        list_bucket_dump_manifests_at(&self.index_dir, shard_id).unwrap_or_default()
    }

    /// Land a manifest in this node's index dir durably, without installing it.
    ///
    /// This is deliberately NOT [`install_bucket_dump_manifest`](Self::install_bucket_dump_manifest):
    /// installing rewrites shard state from the manifest, which is what a restore-onto-this-shard
    /// wants. This one only records that the dump EXISTS, for a node inheriting a shard whose
    /// data is already in shared storage -- the lineage arrives with the data, and whether to
    /// install any of it stays a separate decision.
    pub fn store_bucket_dump_manifest(
        &self,
        manifest: &BucketDumpManifest,
    ) -> Result<(), std::io::Error> {
        self.persist_bucket_dump_manifest(manifest)
    }

    pub fn interrupted_bucket_dump_installs(&self, shard_id: ShardId) -> Vec<BucketDumpInstallMarker> {
        interrupted_bucket_dump_installs_at(&self.index_dir, shard_id).unwrap_or_default()
    }

    pub fn bucket_dump_manifest_prune_plan(&self, shard_id: ShardId) -> BucketDumpManifestPrunePlan {
        self.bucket_dump_manifest_prune_plan_with_follower_cursors(shard_id, Vec::new())
    }

    pub fn bucket_dump_manifest_prune_plan_with_follower_cursors(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
    ) -> BucketDumpManifestPrunePlan {
        self.bucket_dump_manifest_prune_plan_with_retention_refs(
            shard_id,
            follower_cursors,
            Vec::new(),
        )
    }

    pub fn bucket_dump_manifest_prune_plan_with_retention_refs(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = BucketDumpRaftSnapshotRef>,
    ) -> BucketDumpManifestPrunePlan {
        let follower_cursors = follower_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        bucket_dump_manifest_prune_plan_at(
            &self.index_dir,
            shard_id,
            &follower_cursors,
            &raft_snapshot_refs,
        )
        .unwrap_or_else(|err| BucketDumpManifestPrunePlan {
            shard_id,
            reasons: vec![format!("slot_dump_prune_plan_failed:{err}")],
            ..BucketDumpManifestPrunePlan::default()
        })
    }

    pub fn bucket_dump_install_roll_forward_reports(
        &self,
        shard_id: ShardId,
    ) -> Vec<BucketDumpInstallRollForwardReport> {
        self.interrupted_bucket_dump_installs(shard_id)
            .into_iter()
            .map(|marker| self.bucket_dump_install_roll_forward_report(&marker))
            .collect()
    }

    pub fn roll_forward_bucket_dump_installs(
        &self,
        shard_id: ShardId,
    ) -> Vec<BucketDumpInstallRollForwardReport> {
        self.interrupted_bucket_dump_installs(shard_id)
            .into_iter()
            .map(|marker| {
                let mut report = self.bucket_dump_install_roll_forward_report(&marker);
                if report.can_retry_install {
                    match bucket_dump_manifest_at(
                        &self.index_dir,
                        marker.shard_id,
                        &marker.manifest_id,
                    )
                    .ok()
                    .flatten()
                    .map(|manifest| {
                        if manifest.manifest_kind == "merged_slot_dump" {
                            let merged_report = self.install_merged_bucket_dump_manifest(&manifest);
                            if merged_report.installed {
                                Ok(())
                            } else {
                                Err(Status::error(
                                    merged_report.status_code,
                                    "merged slot dump roll-forward failed",
                                ))
                            }
                        } else {
                            self.install_bucket_dump_manifest(&manifest)
                        }
                    }) {
                        Some(Ok(())) => {
                            report.completed_install = true;
                            report.completed_commit = true;
                            report.obsolete_marker_files_removed =
                                remove_obsolete_bucket_dump_install_markers(
                                    &self.index_dir,
                                    marker.shard_id,
                                    &marker.manifest_id,
                                )
                                .unwrap_or_default();
                            report.reason = "install_retried".to_string();
                        }
                        Some(Err(status)) => {
                            report.can_retry_install = false;
                            report.reason = format!("install_retry_failed:{}", status.code);
                        }
                        None => {
                            report.can_retry_install = false;
                            report.reason = "missing_manifest".to_string();
                        }
                    }
                } else if report.can_roll_forward {
                    match self.persist_bucket_dump_install_marker_by_fields(
                        marker.shard_id,
                        &marker.manifest_id,
                        "commit",
                        marker.wal_sequence,
                        marker.index_log_sequence,
                    ) {
                        Ok(()) => {
                            report.completed_commit = true;
                            report.obsolete_marker_files_removed =
                                remove_obsolete_bucket_dump_install_markers(
                                    &self.index_dir,
                                    marker.shard_id,
                                    &marker.manifest_id,
                                )
                                .unwrap_or_default();
                            report.reason = "commit_marker_written".to_string();
                        }
                        Err(err) => {
                            report.can_roll_forward = false;
                            report.reason = format!("commit_marker_failed:{err}");
                        }
                    }
                }
                report
            })
            .collect()
    }

    pub fn apply_bucket_dump_manifest_prune(&self, shard_id: ShardId) -> BucketDumpManifestPruneReport {
        self.apply_bucket_dump_manifest_prune_with_follower_cursors(shard_id, Vec::new())
    }

    pub fn apply_bucket_dump_manifest_prune_with_follower_cursors(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
    ) -> BucketDumpManifestPruneReport {
        self.apply_bucket_dump_manifest_prune_with_retention_refs(
            shard_id,
            follower_cursors,
            Vec::new(),
        )
    }

    pub fn apply_bucket_dump_manifest_prune_with_retention_refs(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = BucketDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = BucketDumpRaftSnapshotRef>,
    ) -> BucketDumpManifestPruneReport {
        let plan = self.bucket_dump_manifest_prune_plan_with_retention_refs(
            shard_id,
            follower_cursors,
            raft_snapshot_refs,
        );
        let mut removed_manifest_ids = Vec::new();
        for manifest_id in &plan.prunable_manifest_ids {
            let path = bucket_dump_manifest_path(&self.index_dir, shard_id, manifest_id);
            if fs::remove_file(path).is_ok() {
                removed_manifest_ids.push(manifest_id.clone());
            }
        }
        // Pruning an ancestor leaves whatever survives pointing at a manifest that is no
        // longer there, which `bucket_dump_manifest_chain_issues` would report as
        // `missing_parent_manifest` -- indistinguishable from real corruption. Detach the
        // survivors whose parent we just removed, so a dangling link keeps meaning exactly one
        // thing. Nothing reconstructs through the chain (each manifest embeds a complete
        // index), so dropping the link loses no recovery capability.
        if !removed_manifest_ids.is_empty() {
            let removed = removed_manifest_ids.iter().cloned().collect::<BTreeSet<_>>();
            if let Ok(surviving) = list_bucket_dump_manifests_at(&self.index_dir, shard_id) {
                for mut manifest in surviving {
                    let parent_pruned = manifest
                        .parent_manifest_id
                        .as_ref()
                        .is_some_and(|parent| removed.contains(parent));
                    if !parent_pruned {
                        continue;
                    }
                    manifest.parent_manifest_id = None;
                    manifest.dump_generation_id = bucket_dump_generation_id(&manifest);
                    if let Ok(checksum) = bucket_dump_manifest_checksum(&manifest) {
                        manifest.checksum = checksum;
                        let _ = self.persist_bucket_dump_manifest(&manifest);
                    }
                }
            }
        }
        let mut removed_marker_files = 0usize;
        if let Ok(marker_files) = bucket_dump_install_marker_files_at(&self.index_dir, shard_id) {
            let prunable_marker_manifest_ids = plan
                .prunable_marker_manifest_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            for (marker, path) in marker_files {
                if prunable_marker_manifest_ids.contains(&marker.manifest_id)
                    && fs::remove_file(path).is_ok()
                {
                    removed_marker_files = removed_marker_files.saturating_add(1);
                }
            }
        }
        BucketDumpManifestPruneReport {
            shard_id,
            plan,
            removed_manifest_ids,
            removed_marker_files,
        }
    }

    pub(super) fn bucket_dump_install_roll_forward_report(
        &self,
        marker: &BucketDumpInstallMarker,
    ) -> BucketDumpInstallRollForwardReport {
        if marker.phase != "install" && marker.phase != "prepare" {
            return BucketDumpInstallRollForwardReport {
                shard_id: marker.shard_id,
                manifest_id: marker.manifest_id.clone(),
                interrupted_phase: marker.phase.clone(),
                can_roll_forward: false,
                completed_commit: false,
                completed_install: false,
                can_retry_install: false,
                obsolete_marker_files_removed: 0,
                reason: "unknown_interrupted_phase".to_string(),
            };
        }
        let Some(manifest) =
            bucket_dump_manifest_at(&self.index_dir, marker.shard_id, &marker.manifest_id)
                .ok()
                .flatten()
        else {
            return BucketDumpInstallRollForwardReport {
                shard_id: marker.shard_id,
                manifest_id: marker.manifest_id.clone(),
                interrupted_phase: marker.phase.clone(),
                can_roll_forward: false,
                completed_commit: false,
                completed_install: false,
                can_retry_install: false,
                obsolete_marker_files_removed: 0,
                reason: "missing_manifest".to_string(),
            };
        };
        let reason = match self.validate_bucket_dump_manifest(&manifest) {
            Ok(()) if marker.phase == "install" => "commit_ready".to_string(),
            Ok(()) => "install_retry_ready".to_string(),
            Err(status) => format!("manifest_invalid:{}", status.code),
        };
        BucketDumpInstallRollForwardReport {
            shard_id: marker.shard_id,
            manifest_id: marker.manifest_id.clone(),
            interrupted_phase: marker.phase.clone(),
            can_roll_forward: reason == "commit_ready",
            can_retry_install: reason == "install_retry_ready",
            completed_commit: false,
            completed_install: false,
            obsolete_marker_files_removed: 0,
            reason,
        }
    }

    pub fn validate_bucket_dump_manifest(&self, manifest: &BucketDumpManifest) -> Result<(), Status> {
        let expected = bucket_dump_manifest_checksum(manifest)
            .map_err(|_| Status::error("slot_dump_corrupt", "slot dump manifest is corrupt"))?;
        if manifest.checksum != expected {
            return Err(Status::error(
                "slot_dump_checksum_mismatch",
                "slot dump manifest checksum mismatch",
            ));
        }
        if manifest.version >= 2 && manifest.dump_generation_id.is_empty() {
            return Err(Status::error(
                "slot_dump_missing_generation",
                "slot dump manifest is missing dump generation id",
            ));
        }
        if manifest.index_bytes.is_empty() {
            return Err(Status::error(
                "slot_dump_partial_manifest",
                "slot dump manifest is missing index bytes",
            ));
        }
        let actual_index_sha256 = sha256_hex_bytes(&manifest.index_bytes);
        if manifest.index_sha256 != actual_index_sha256 {
            return Err(Status::error(
                "slot_dump_index_checksum_mismatch",
                "slot dump manifest index checksum mismatch",
            ));
        }
        let existing_slabs = self
            .page_store
            .slab_ids()
            .map_err(|err| Status::error("slot_dump_invalid", err.to_string()))?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let missing = manifest
            .page_slab_ids
            .iter()
            .copied()
            .filter(|id| !existing_slabs.contains(id))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(Status::error(
                "slot_dump_missing_page_segments",
                format!("slot dump references missing page segments: {missing:?}"),
            ));
        }
        let mut restored = crate::engine::decode_index_bytes(&manifest.index_bytes)
            .map_err(|err| Status::error("slot_dump_invalid_index", err.to_string()))?;
        // `hashes` is `skip_serializing`, so a freshly deserialized index never carries it.
        // Without this the model-maps lifecycle below sees no hash objects while the
        // bucket-index derivation does, and every shard holding a hash key is rejected.
        // Only the unserialized maps are rebuilt: the serialized ones must stay exactly as
        // decoded, or the cross-check would repair the very tampering it exists to catch.
        rebuild_unserialized_model_maps_from_bucket_index(&mut restored);
        let restored = restored;
        let manifest_buckets = manifest.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
        if manifest_buckets.len() != manifest.bucket_ids.len()
            || manifest.bucket_ids != manifest_buckets.iter().copied().collect::<Vec<_>>()
        {
            return Err(Status::error(
                "slot_dump_slot_ids_not_canonical",
                "slot dump manifest slot ids must be sorted and unique",
            ));
        }
        let canonical_page_slab_ids = manifest
            .page_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if manifest.page_slab_ids != canonical_page_slab_ids {
            return Err(Status::error(
                "slot_dump_page_segment_ids_not_canonical",
                "slot dump manifest page segment ids must be sorted and unique",
            ));
        }
        let live_page_entries = collect_live_page_entries(&restored)
            .into_iter()
            .filter(|entry| {
                let routing_bucket = entry.address.routing_bucket.unwrap_or_else(|| {
                    self.routing_bucket_for_key(manifest.shard_id, &entry.object_key)
                });
                manifest_buckets.is_empty() || manifest_buckets.contains(&routing_bucket)
            })
            .collect::<Vec<_>>();
        if live_page_entries.len() as u64 != manifest.live_page_refs {
            return Err(Status::error(
                "slot_dump_live_ref_mismatch",
                format!(
                    "slot dump expected {} live page refs but index has {}",
                    manifest.live_page_refs,
                    live_page_entries.len()
                ),
            ));
        }
        let expected_bucket_summaries =
            bucket_dump_manifest_comparable_summaries(&restored, &manifest_buckets);
        let actual_bucket_summaries = comparable_bucket_dump_summaries(manifest.bucket_summaries.clone());
        if actual_bucket_summaries != expected_bucket_summaries {
            return Err(Status::error(
                "slot_dump_slot_summary_mismatch",
                format!(
                    "slot dump slot summaries do not match restored index page ownership: manifest={actual_bucket_summaries:?} restored={expected_bucket_summaries:?}"
                ),
            ));
        }
        let expected_logical_bytes = expected_bucket_summaries
            .iter()
            .map(|summary| summary.logical_bytes)
            .sum::<u64>();
        let expected_physical_bytes = expected_bucket_summaries
            .iter()
            .map(|summary| summary.physical_bytes)
            .sum::<u64>();
        if manifest.logical_bytes != expected_logical_bytes
            || manifest.physical_bytes != expected_physical_bytes
        {
            return Err(Status::error(
                "slot_dump_byte_accounting_mismatch",
                format!(
                    "slot dump byte totals logical={} physical={} do not match restored index logical={} physical={}",
                    manifest.logical_bytes,
                    manifest.physical_bytes,
                    expected_logical_bytes,
                    expected_physical_bytes
                ),
            ));
        }
        if manifest.version >= 3 {
            let expected_object_lifecycle = storage_object_lifecycle_report_for_buckets(
                manifest.shard_id,
                &restored,
                &manifest_buckets,
                |key| self.routing_bucket_for_key(manifest.shard_id, key),
            );
            // Also derive the lifecycle from the serialized model maps. For a
            // consistent index the two agree; a manifest whose model maps disagree
            // with its bucket index (e.g. a tampered object_id that the bucket-index
            // derivation alone would miss) is rejected.
            let model_object_lifecycle = storage_object_lifecycle_report_for_buckets_from_model_maps(
                manifest.shard_id,
                &restored,
                &manifest_buckets,
                |key| self.routing_bucket_for_key(manifest.shard_id, key),
            );
            if manifest.object_lifecycle != expected_object_lifecycle
                || manifest.object_lifecycle != model_object_lifecycle
            {
                return Err(Status::error(
                    "slot_dump_object_lifecycle_mismatch",
                    "slot dump object lifecycle metadata does not match restored index",
                ));
            }
        }
        let referenced_page_slab_ids = live_page_entries
            .iter()
            .map(|entry| entry.address.page_slab_id)
            .collect::<BTreeSet<_>>();
        let manifest_page_slab_ids = manifest
            .page_slab_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if referenced_page_slab_ids != manifest_page_slab_ids {
            return Err(Status::error(
                "slot_dump_page_segment_mismatch",
                format!(
                    "slot dump page segment ids {manifest_page_slab_ids:?} do not match live refs {referenced_page_slab_ids:?}"
                ),
            ));
        }
        if !manifest.dump_generation_id.is_empty()
            && manifest.dump_generation_id != bucket_dump_generation_id(manifest)
        {
            return Err(Status::error(
                "slot_dump_generation_mismatch",
                "slot dump manifest generation id does not match its sequence, slots, pages, and index checksum",
            ));
        }
        let mut unreadable_page_refs = 0usize;
        let mut unreadable_page_bytes = 0u64;
        for entry in live_page_entries {
            if self.page_store.read(&entry.address).is_err() {
                unreadable_page_refs = unreadable_page_refs.saturating_add(1);
                unreadable_page_bytes = unreadable_page_bytes.saturating_add(entry.address.length);
            }
        }
        if unreadable_page_refs > 0 {
            return Err(Status::error(
                "slot_dump_unreadable_page_refs",
                format!(
                    "slot dump has {unreadable_page_refs} unreadable page refs covering {unreadable_page_bytes} bytes"
                ),
            ));
        }
        Ok(())
    }

    pub fn bucket_dump_install_preflight_report(
        &self,
        manifest: &BucketDumpManifest,
    ) -> BucketDumpInstallPreflightReport {
        let current_wal_sequence = self.wal_store.stats(manifest.shard_id).last_sequence;
        let current_index_log_sequence =
            self.index_log_store.stats(manifest.shard_id).last_sequence;
        let existing_slabs = self
            .page_store
            .slab_ids()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let missing_page_slab_ids = manifest
            .page_slab_ids
            .iter()
            .copied()
            .filter(|id| !existing_slabs.contains(id))
            .collect::<Vec<_>>();
        let corrupt_page_slab_ids = self
            .page_store
            .slab_reports()
            .unwrap_or_default()
            .into_iter()
            .filter(|report| {
                report.has_corruption && manifest.page_slab_ids.contains(&report.page_slab_id)
            })
            .map(|report| report.page_slab_id)
            .collect::<Vec<_>>();
        let stale_manifest = current_index_log_sequence > manifest.index_log_sequence;
        let mut blockers = Vec::new();
        if stale_manifest {
            blockers.push("stale_manifest_sequence".to_string());
        }
        if !missing_page_slab_ids.is_empty() {
            blockers.push("missing_page_segments".to_string());
        }
        if !corrupt_page_slab_ids.is_empty() {
            blockers.push("corrupt_page_segments".to_string());
        }

        let mut unreadable_page_ref_count = 0usize;
        let mut unreadable_page_bytes = 0u64;
        let mut restored_index = None;
        if !manifest.index_bytes.is_empty() && missing_page_slab_ids.is_empty() {
            if let Ok(restored) = crate::engine::decode_index_bytes(&manifest.index_bytes) {
                let manifest_buckets = manifest.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
                for entry in collect_live_page_entries(&restored) {
                    let routing_bucket = entry.address.routing_bucket.unwrap_or_else(|| {
                        self.routing_bucket_for_key(manifest.shard_id, &entry.object_key)
                    });
                    if manifest_buckets.is_empty() || manifest_buckets.contains(&routing_bucket) {
                        if self.page_store.read(&entry.address).is_err() {
                            unreadable_page_ref_count = unreadable_page_ref_count.saturating_add(1);
                            unreadable_page_bytes =
                                unreadable_page_bytes.saturating_add(entry.address.length);
                        }
                    }
                }
                restored_index = Some(restored);
            } else {
                blockers.push("invalid_manifest_index".to_string());
            }
        }
        let (stale_object_conflicts, mut stale_page_conflicts) = restored_index
            .as_ref()
            .map(|restored| self.bucket_dump_stale_conflict_report(manifest, restored))
            .unwrap_or_default();
        let (
            source_manifest_count,
            missing_source_manifest_ids,
            source_manifest_bucket_ids,
            source_bucket_coverage_missing_bucket_ids,
        ) = self.bucket_dump_merged_source_preflight(manifest);
        if !missing_source_manifest_ids.is_empty() {
            blockers.push("missing_source_manifests".to_string());
        }
        if !source_bucket_coverage_missing_bucket_ids.is_empty() {
            blockers.push("source_manifest_slot_coverage".to_string());
        }
        if stale_manifest && stale_page_conflicts.is_empty() {
            stale_page_conflicts.push(format!(
                "index_log_sequence:{}->{}",
                manifest.index_log_sequence, current_index_log_sequence
            ));
        }
        if unreadable_page_ref_count > 0 {
            blockers.push("unreadable_page_refs".to_string());
        }
        if let Some(handoff) = &manifest.load_version_handoff {
            let current_load_version = self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&manifest.shard_id)
                .map(|info| info.load_version)
                .unwrap_or_default();
            if current_load_version != handoff.previous_load_version {
                blockers.push("load_version_handoff_mismatch".to_string());
            }
        }
        if !stale_object_conflicts.is_empty() {
            blockers.push("stale_object_conflicts".to_string());
        }
        if !stale_page_conflicts.is_empty() {
            blockers.push("stale_page_conflicts".to_string());
        }
        blockers.sort();
        blockers.dedup();

        BucketDumpInstallPreflightReport {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            install_safe: blockers.is_empty(),
            blockers,
            current_wal_sequence,
            current_index_log_sequence,
            manifest_wal_sequence: manifest.wal_sequence,
            manifest_index_log_sequence: manifest.index_log_sequence,
            missing_page_slab_ids,
            corrupt_page_slab_ids,
            unreadable_page_ref_count,
            unreadable_page_bytes,
            stale_manifest,
            stale_object_conflict_count: stale_object_conflicts.len(),
            stale_page_conflict_count: stale_page_conflicts.len(),
            stale_object_conflicts,
            stale_page_conflicts,
            source_manifest_count,
            missing_source_manifest_ids,
            source_manifest_bucket_ids,
            source_bucket_coverage_missing_bucket_ids,
        }
    }

    pub(super) fn bucket_dump_merged_source_preflight(
        &self,
        manifest: &BucketDumpManifest,
    ) -> (usize, Vec<String>, Vec<u32>, Vec<u32>) {
        if manifest.manifest_kind != "merged_slot_dump" && manifest.source_manifest_ids.is_empty() {
            return (0, Vec::new(), Vec::new(), Vec::new());
        }
        let (source_manifest_count, missing_source_manifest_ids, source_manifest_bucket_ids) =
            if !manifest.source_manifest_coverage.is_empty() {
                // Self-describing manifest: use the embedded per-source coverage so a
                // target engine without the source files on disk can still validate.
                let present = manifest
                    .source_manifest_coverage
                    .iter()
                    .map(|coverage| coverage.manifest_id.clone())
                    .collect::<BTreeSet<_>>();
                let missing = manifest
                    .source_manifest_ids
                    .iter()
                    .filter(|manifest_id| !present.contains(*manifest_id))
                    .cloned()
                    .collect::<Vec<_>>();
                let bucket_ids = manifest
                    .source_manifest_coverage
                    .iter()
                    .flat_map(|coverage| coverage.bucket_ids.iter().copied())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                (manifest.source_manifest_coverage.len(), missing, bucket_ids)
            } else {
                self.bucket_dump_source_manifest_coverage(
                    manifest.shard_id,
                    &manifest.source_manifest_ids,
                )
            };
        let source_buckets = source_manifest_bucket_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let missing_bucket_ids = manifest
            .bucket_ids
            .iter()
            .copied()
            .filter(|bucket| !source_buckets.contains(bucket))
            .collect::<Vec<_>>();
        (
            source_manifest_count,
            missing_source_manifest_ids,
            source_manifest_bucket_ids,
            missing_bucket_ids,
        )
    }

    pub(super) fn bucket_dump_source_manifest_coverage(
        &self,
        shard_id: ShardId,
        source_manifest_ids: &[String],
    ) -> (usize, Vec<String>, Vec<u32>) {
        let mut source_manifest_count = 0usize;
        let mut missing_source_manifest_ids = Vec::new();
        let mut source_manifest_bucket_ids = BTreeSet::new();
        for manifest_id in source_manifest_ids {
            match bucket_dump_manifest_at(&self.index_dir, shard_id, manifest_id) {
                Ok(Some(source))
                    if source.shard_id == shard_id
                        && bucket_dump_manifest_checksum(&source)
                            .map(|checksum| checksum == source.checksum)
                            .unwrap_or(false) =>
                {
                    source_manifest_count = source_manifest_count.saturating_add(1);
                    source_manifest_bucket_ids.extend(source.bucket_ids);
                }
                _ => missing_source_manifest_ids.push(manifest_id.clone()),
            }
        }
        (
            source_manifest_count,
            missing_source_manifest_ids,
            source_manifest_bucket_ids.into_iter().collect(),
        )
    }

    pub(super) fn bucket_dump_stale_conflict_report(
        &self,
        manifest: &BucketDumpManifest,
        restored: &ShardState,
    ) -> (Vec<String>, Vec<String>) {
        let manifest_buckets = manifest.bucket_ids.iter().copied().collect::<BTreeSet<_>>();
        let manifest_entries =
            bucket_dump_entries_by_key(manifest.shard_id, restored, &manifest_buckets, |key| {
                self.routing_bucket_for_key(manifest.shard_id, key)
            });
        let current_entries = {
            let shards = self.shards.read().expect("engine lock poisoned");
            let Some(current) = shards.get(&manifest.shard_id) else {
                return (Vec::new(), Vec::new());
            };
            bucket_dump_entries_by_key(manifest.shard_id, current, &manifest_buckets, |key| {
                self.routing_bucket_for_key(manifest.shard_id, key)
            })
        };
        // bucket_dump_entries_by_key keys pages by "kind:object_key:component:page_id",
        // so a value update (which allocates a new page_id) shows up as a removed page
        // plus a new page for the same object. Classify at the object level (strip the
        // trailing :page_id): a page whose object still exists on the other side is a
        // page conflict; only an object present on exactly one side is an object
        // conflict.
        let object_level = |key: &str| -> String {
            key.rsplit_once(':')
                .map(|(prefix, _)| prefix.to_string())
                .unwrap_or_else(|| key.to_string())
        };
        let manifest_object_keys = manifest_entries
            .keys()
            .map(|key| object_level(key))
            .collect::<BTreeSet<_>>();
        let current_object_keys = current_entries
            .keys()
            .map(|key| object_level(key))
            .collect::<BTreeSet<_>>();
        let mut stale_object_conflicts = Vec::new();
        let mut stale_page_conflicts = Vec::new();
        for key in manifest_entries.keys().chain(current_entries.keys()) {
            let object_key = object_level(key);
            match (manifest_entries.get(key), current_entries.get(key)) {
                (Some(manifest_address), Some(current_address)) => {
                    if manifest_address != current_address {
                        stale_page_conflicts.push(object_key);
                    }
                }
                (Some(_), None) => {
                    if current_object_keys.contains(&object_key) {
                        stale_page_conflicts.push(object_key);
                    }
                }
                (None, Some(_)) => {
                    if manifest_object_keys.contains(&object_key) {
                        stale_page_conflicts.push(object_key);
                    } else {
                        stale_object_conflicts.push(object_key);
                    }
                }
                (None, None) => {}
            }
        }
        stale_object_conflicts.sort();
        stale_object_conflicts.dedup();
        stale_page_conflicts.sort();
        stale_page_conflicts.dedup();
        (stale_object_conflicts, stale_page_conflicts)
    }

    pub fn bucket_dump_fault_matrix_report(&self, shard_id: ShardId) -> BucketDumpFaultMatrixReport {
        let manifest = match self.create_bucket_dump_manifest(shard_id, Vec::new()) {
            Ok(manifest) => manifest,
            Err(status) => {
                let scenario = BucketDumpFaultScenarioReport {
                    scenario: "create_manifest".to_string(),
                    passed: false,
                    expected_code: "ok".to_string(),
                    actual_code: status.code,
                    blockers: Vec::new(),
                    install_safe: false,
                };
                return BucketDumpFaultMatrixReport {
                    shard_id,
                    manifest_id: String::new(),
                    production_ready_slice: false,
                    scenario_count: 1,
                    passed_count: 0,
                    failed_scenarios: vec![scenario.clone()],
                    scenarios: vec![scenario],
                };
            }
        };

        let mut scenarios = Vec::new();

        let mut checksum_mismatch = manifest.clone();
        checksum_mismatch.logical_bytes = checksum_mismatch.logical_bytes.saturating_add(1);
        let checksum_code = self
            .validate_bucket_dump_manifest(&checksum_mismatch)
            .err()
            .map(|status| status.code)
            .unwrap_or_else(|| "ok".to_string());
        scenarios.push(bucket_dump_fault_scenario(
            "checksum_mismatch",
            "slot_dump_checksum_mismatch",
            checksum_code,
            Vec::new(),
            false,
        ));

        let mut partial = manifest.clone();
        partial.index_bytes.clear();
        partial.checksum =
            bucket_dump_manifest_checksum(&partial).unwrap_or_else(|err| err.code.clone());
        let partial_code = self
            .install_bucket_dump_manifest(&partial)
            .err()
            .map(|status| status.code)
            .unwrap_or_else(|| "ok".to_string());
        scenarios.push(bucket_dump_fault_scenario(
            "partial_manifest",
            "slot_dump_partial_manifest",
            partial_code,
            Vec::new(),
            false,
        ));

        let mut missing = manifest.clone();
        let missing_slab_id = missing
            .page_slab_ids
            .iter()
            .copied()
            .max()
            .unwrap_or_default()
            .saturating_add(1_000_000);
        missing.page_slab_ids.push(missing_slab_id);
        missing.page_slab_ids.sort_unstable();
        missing.checksum =
            bucket_dump_manifest_checksum(&missing).unwrap_or_else(|err| err.code.clone());
        let missing_preflight = self.bucket_dump_install_preflight_report(&missing);
        let missing_code = self
            .validate_bucket_dump_manifest(&missing)
            .err()
            .map(|status| status.code)
            .unwrap_or_else(|| "ok".to_string());
        scenarios.push(bucket_dump_fault_scenario(
            "missing_page_segment",
            "slot_dump_missing_page_segments",
            missing_code,
            missing_preflight.blockers,
            missing_preflight.install_safe,
        ));

        let mut stale_code = "not_run".to_string();
        let mut stale_blockers = Vec::new();
        let mut stale_install_safe = false;
        let stale_write = self.execute(ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: "__slot_dump_fault_matrix_stale__".to_string(),
                value: b"newer".to_vec(),
            },
        });
        if stale_write.status.ok {
            let stale_preflight = self.bucket_dump_install_preflight_report(&manifest);
            stale_install_safe = stale_preflight.install_safe;
            stale_blockers = stale_preflight.blockers;
            stale_code = self
                .install_bucket_dump_manifest(&manifest)
                .err()
                .map(|status| status.code)
                .unwrap_or_else(|| "ok".to_string());
        }
        scenarios.push(bucket_dump_fault_scenario(
            "stale_manifest",
            "slot_dump_stale_manifest",
            stale_code,
            stale_blockers,
            stale_install_safe,
        ));

        let restart_code;
        let mut restart_blockers = Vec::new();
        let mut restart_install_safe = false;
        match self.create_bucket_dump_manifest(shard_id, Vec::new()) {
            Ok(restart_manifest) => {
                match self.persist_bucket_dump_install_marker_by_fields(
                    restart_manifest.shard_id,
                    &restart_manifest.manifest_id,
                    "prepare",
                    restart_manifest.wal_sequence,
                    restart_manifest.index_log_sequence,
                ) {
                    Ok(()) => {
                        let before = self.bucket_dump_install_roll_forward_reports(shard_id);
                        let applied = self.roll_forward_bucket_dump_installs(shard_id);
                        let interrupted_after = self.interrupted_bucket_dump_installs(shard_id);
                        restart_install_safe = before.iter().any(|report| report.can_retry_install);
                        if !restart_install_safe {
                            restart_blockers.push("roll_forward_not_retryable".to_string());
                        }
                        if !applied
                            .iter()
                            .any(|report| report.completed_install && report.completed_commit)
                        {
                            restart_blockers.push("roll_forward_not_completed".to_string());
                        }
                        if !interrupted_after.is_empty() {
                            restart_blockers.push("interrupted_markers_remaining".to_string());
                        }
                        restart_code = if restart_blockers.is_empty() {
                            "slot_dump_restart_roll_forward".to_string()
                        } else {
                            "slot_dump_restart_roll_forward_failed".to_string()
                        };
                    }
                    Err(err) => restart_code = format!("slot_dump_marker_write_failed:{err}"),
                }
            }
            Err(status) => restart_code = status.code,
        }
        scenarios.push(bucket_dump_fault_scenario(
            "restart_during_install_roll_forward",
            "slot_dump_restart_roll_forward",
            restart_code,
            restart_blockers,
            restart_install_safe,
        ));

        let mut corrupt_code = "not_run".to_string();
        let mut corrupt_blockers = Vec::new();
        let mut corrupt_install_safe = false;
        if let Some(slab_id) = manifest.page_slab_ids.first().copied() {
            match self.page_store.read_slab(slab_id) {
                Ok(mut slab) if !slab.is_empty() => {
                    if let Some(last) = slab.last_mut() {
                        *last ^= 0xff;
                    }
                    match self.page_store.install_slab(slab_id, &slab) {
                        Ok(()) => {
                            let corrupt_preflight =
                                self.bucket_dump_install_preflight_report(&manifest);
                            corrupt_install_safe = corrupt_preflight.install_safe;
                            corrupt_blockers = corrupt_preflight.blockers;
                            corrupt_code = if corrupt_blockers
                                .iter()
                                .any(|blocker| blocker == "corrupt_page_segments")
                            {
                                "corrupt_page_segments".to_string()
                            } else {
                                self.validate_bucket_dump_manifest(&manifest)
                                    .err()
                                    .map(|status| status.code)
                                    .unwrap_or_else(|| "ok".to_string())
                            };
                        }
                        Err(err) => corrupt_code = format!("install_segment_failed:{err}"),
                    }
                }
                Ok(_) => corrupt_code = "empty_segment".to_string(),
                Err(err) => corrupt_code = format!("read_segment_failed:{err}"),
            }
        }
        scenarios.push(bucket_dump_fault_scenario(
            "corrupt_page_segment",
            "corrupt_page_segments",
            corrupt_code,
            corrupt_blockers,
            corrupt_install_safe,
        ));

        let passed_count = scenarios.iter().filter(|scenario| scenario.passed).count();
        let failed_scenarios = scenarios
            .iter()
            .filter(|scenario| !scenario.passed)
            .cloned()
            .collect::<Vec<_>>();
        BucketDumpFaultMatrixReport {
            shard_id,
            manifest_id: manifest.manifest_id,
            production_ready_slice: failed_scenarios.is_empty(),
            scenario_count: scenarios.len(),
            passed_count,
            failed_scenarios,
            scenarios,
        }
    }

    pub fn install_bucket_dump_manifest(&self, manifest: &BucketDumpManifest) -> Result<(), Status> {
        self.validate_bucket_dump_manifest(manifest)?;
        let preflight = self.bucket_dump_install_preflight_report(manifest);
        if !preflight.install_safe {
            if preflight.stale_manifest {
                return Err(Status::error(
                    "slot_dump_stale_manifest",
                    format!(
                        "manifest index sequence {} is older than current {}",
                        manifest.index_log_sequence, preflight.current_index_log_sequence
                    ),
                ));
            }
            if preflight.unreadable_page_ref_count > 0 {
                return Err(Status::error(
                    "slot_dump_unreadable_page_refs",
                    format!(
                        "slot dump has {} unreadable page refs covering {} bytes",
                        preflight.unreadable_page_ref_count, preflight.unreadable_page_bytes
                    ),
                ));
            }
            return Err(Status::error(
                "slot_dump_install_preflight_failed",
                format!(
                    "slot dump install preflight blockers: {:?}",
                    preflight.blockers
                ),
            ));
        }
        self.validate_bucket_dump_generation_for_install(manifest)?;
        let current_index_sequence = self.index_log_store.stats(manifest.shard_id).last_sequence;
        if current_index_sequence > manifest.index_log_sequence {
            return Err(Status::error(
                "slot_dump_stale_manifest",
                format!(
                    "manifest index sequence {} is older than current {}",
                    manifest.index_log_sequence, current_index_sequence
                ),
            ));
        }
        let mut restored = crate::engine::decode_index_bytes(&manifest.index_bytes)
            .map_err(|err| Status::error("slot_dump_invalid_index", err.to_string()))?;
        // Rebuild the unserialized model maps from the durable bucket index BEFORE rebuilding
        // ownership. `rebuild_bucket_page_ownership` clears the bucket map and repopulates it
        // from the model maps, so running it against a state whose `hashes` map never survived
        // serialization would drop every hash page from the restored index -- and that index is
        // what gets persisted durably below.
        rebuild_unserialized_model_maps_from_bucket_index(&mut restored);
        rebuild_bucket_page_ownership(manifest.shard_id, &mut restored, 0, u32::MAX);
        let restored_index_bytes = serialize_index(&restored);
        self.persist_bucket_dump_install_marker(manifest, "prepare")
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        // Durable (bulk-gate-bypassing) write: under bulk-ingest mode the ordinary
        // persist_index_bytes is a no-op, which would leave the stale pre-manifest index on
        // disk while the advanced replay watermark suppresses replay of the records this
        // manifest embeds -- silently losing them. Recovery install must always materialize.
        self.persist_index_bytes_durable(manifest.shard_id, &restored_index_bytes)
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        self.persist_bucket_dump_install_marker(manifest, "install")
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        if self
            .shards
            .read()
            .expect("engine lock poisoned")
            .contains_key(&manifest.shard_id)
        {
            self.shards
                .write()
                .expect("engine lock poisoned")
                .insert(manifest.shard_id, restored);
        }
        self.persist_bucket_dump_manifest(manifest)
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        self.persist_bucket_dump_install_marker(manifest, "commit")
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        Ok(())
    }

    pub fn install_merged_bucket_dump_manifest(
        &self,
        manifest: &BucketDumpManifest,
    ) -> BucketDumpMergedInstallReport {
        let preflight = self.bucket_dump_install_preflight_report(manifest);
        let rollback_marker_written = self
            .persist_bucket_dump_install_marker(manifest, "rollback")
            .is_ok();
        if !preflight.install_safe {
            return BucketDumpMergedInstallReport {
                shard_id: manifest.shard_id,
                manifest_id: manifest.manifest_id.clone(),
                source_manifest_ids: manifest.source_manifest_ids.clone(),
                bucket_ids: manifest.bucket_ids.clone(),
                source_manifest_count: preflight.source_manifest_count,
                missing_source_manifest_ids: preflight.missing_source_manifest_ids.clone(),
                source_bucket_coverage_missing_bucket_ids: preflight
                    .source_bucket_coverage_missing_bucket_ids
                    .clone(),
                stale_object_conflict_count: preflight.stale_object_conflict_count,
                stale_page_conflict_count: preflight.stale_page_conflict_count,
                preflight,
                rollback_marker_written,
                prepare_marker_written: false,
                install_marker_written: false,
                commit_marker_written: false,
                load_version_handoff: manifest.load_version_handoff.clone(),
                installed: false,
                status_code: "slot_dump_install_preflight_failed".to_string(),
            };
        }
        match self.install_bucket_dump_manifest(manifest) {
            Ok(()) => {
                let mut load_version_handoff = manifest.load_version_handoff.clone();
                if let Some(handoff) = load_version_handoff.as_mut() {
                    if let Some(info) = self
                        .infos
                        .write()
                        .expect("info lock poisoned")
                        .get_mut(&manifest.shard_id)
                    {
                        info.load_version = handoff.next_load_version;
                        handoff.applied = true;
                    }
                }
                BucketDumpMergedInstallReport {
                    shard_id: manifest.shard_id,
                    manifest_id: manifest.manifest_id.clone(),
                    source_manifest_ids: manifest.source_manifest_ids.clone(),
                    bucket_ids: manifest.bucket_ids.clone(),
                    source_manifest_count: preflight.source_manifest_count,
                    missing_source_manifest_ids: preflight.missing_source_manifest_ids.clone(),
                    source_bucket_coverage_missing_bucket_ids: preflight
                        .source_bucket_coverage_missing_bucket_ids
                        .clone(),
                    stale_object_conflict_count: preflight.stale_object_conflict_count,
                    stale_page_conflict_count: preflight.stale_page_conflict_count,
                    preflight,
                    rollback_marker_written,
                    prepare_marker_written: true,
                    install_marker_written: true,
                    commit_marker_written: true,
                    load_version_handoff,
                    installed: true,
                    status_code: "ok".to_string(),
                }
            }
            Err(status) => BucketDumpMergedInstallReport {
                shard_id: manifest.shard_id,
                manifest_id: manifest.manifest_id.clone(),
                source_manifest_ids: manifest.source_manifest_ids.clone(),
                bucket_ids: manifest.bucket_ids.clone(),
                source_manifest_count: preflight.source_manifest_count,
                missing_source_manifest_ids: preflight.missing_source_manifest_ids.clone(),
                source_bucket_coverage_missing_bucket_ids: preflight
                    .source_bucket_coverage_missing_bucket_ids
                    .clone(),
                stale_object_conflict_count: preflight.stale_object_conflict_count,
                stale_page_conflict_count: preflight.stale_page_conflict_count,
                preflight,
                rollback_marker_written,
                prepare_marker_written: false,
                install_marker_written: false,
                commit_marker_written: false,
                load_version_handoff: manifest.load_version_handoff.clone(),
                installed: false,
                status_code: status.code,
            },
        }
    }
}
