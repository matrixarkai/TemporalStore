//! Index/manifest persistence + load/flush helper methods for TemporalEngine, split from engine.rs.
use super::*;

impl TemporalEngine {
    pub(super) fn index_path(&self, shard_id: ShardId) -> PathBuf {
        self.index_dir.join(format!("shard-{shard_id}.index.json"))
    }

    pub(super) fn persist_slot_dump_manifest(
        &self,
        manifest: &SlotDumpManifest,
    ) -> Result<(), std::io::Error> {
        let path =
            slot_dump_manifest_path(&self.index_dir, manifest.shard_id, &manifest.manifest_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        fs::write(path, bytes)
    }

    pub(super) fn persist_slot_dump_install_marker(
        &self,
        manifest: &SlotDumpManifest,
        phase: &str,
    ) -> Result<(), std::io::Error> {
        self.persist_slot_dump_install_marker_by_fields(
            manifest.shard_id,
            &manifest.manifest_id,
            phase,
            manifest.oplog_sequence,
            manifest.index_log_sequence,
        )
    }

    pub(super) fn persist_slot_dump_install_marker_by_fields(
        &self,
        shard_id: ShardId,
        manifest_id: &str,
        phase: &str,
        oplog_sequence: u64,
        index_log_sequence: u64,
    ) -> Result<(), std::io::Error> {
        write_slot_dump_install_marker(
            &self.index_dir,
            &SlotDumpInstallMarker {
                shard_id,
                manifest_id: manifest_id.to_string(),
                phase: phase.to_string(),
                oplog_sequence,
                index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
    }

    pub(super) fn validate_slot_dump_generation_for_install(
        &self,
        manifest: &SlotDumpManifest,
    ) -> Result<(), Status> {
        if manifest.dump_generation_id.is_empty() {
            return Ok(());
        }
        let requested_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
        let source_manifest_ids = manifest
            .source_manifest_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for existing in self.list_slot_dump_manifests(manifest.shard_id) {
            if existing.manifest_id == manifest.manifest_id
                || source_manifest_ids.contains(&existing.manifest_id)
                || existing.dump_generation_id.is_empty()
                || existing.dump_generation_id == manifest.dump_generation_id
            {
                continue;
            }
            let existing_slots = existing.slot_ids.iter().copied().collect::<BTreeSet<_>>();
            let overlaps = requested_slots.is_empty()
                || existing_slots.is_empty()
                || !requested_slots.is_disjoint(&existing_slots);
            if overlaps
                && existing.index_log_sequence >= manifest.index_log_sequence
                && existing.oplog_sequence >= manifest.oplog_sequence
            {
                return Err(Status::error(
                    "slot_dump_generation_conflict",
                    format!(
                        "manifest generation {} conflicts with installed generation {} for overlapping slots",
                        manifest.dump_generation_id, existing.dump_generation_id
                    ),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn load_index(&self, shard_id: ShardId) -> Option<ShardState> {
        let bytes = fs::read(self.index_path(shard_id)).ok()?;
        let mut shard = serde_json::from_slice::<ShardState>(&bytes).ok()?;
        reconcile_secondary_views_from_slot_index(&self.page_store, &mut shard);
        refresh_slot_runtime_flags(&mut shard);
        Some(shard)
    }

    /// Persist the in-memory shard index to disk once (used by bulk backfill
    /// after driving many extract_context calls under MATRIXARK_BULK_INGEST=1,
    /// which skips per-record persistence). Also refreshes the index-log tail.
    pub fn flush_shard_index(&self, shard_id: ShardId) {
        // Make the chunk's deferred bulk writes durable before publishing the
        // served index: fsync page segments + extent manifest, then the WAL.
        let _ = self.page_store.sync_durable();
        let _ = self.wal_store.flush(shard_id);
        let index_bytes = {
            // Reconstruct everything the per-command bulk path deferred: promote
            // model-map pages into slot_index, rebuild the secondary views, refresh
            // runtime flags, then serialize once. Needs a write lock.
            let mut shards = self.shards.write().expect("engine lock poisoned");
            match shards.get_mut(&shard_id) {
                Some(shard) => {
                    if promote_model_maps_to_slot_index_authority(shard_id, shard, 0, u32::MAX)
                    {
                        reconcile_secondary_views_from_slot_index(&self.page_store, shard);
                    }
                    rebuild_slot_first_index(shard_id, shard, 0, u32::MAX);
                    refresh_slot_runtime_flags(shard);
                    serialize_index(shard)
                }
                None => return,
            }
        };
        // Write the served shard index directly, bypassing the bulk-mode gate.
        let _ = fs::create_dir_all(&self.index_dir);
        let _ = atomic_write_bytes(&self.index_path(shard_id), &index_bytes);
    }

    pub(super) fn persist_index_bytes(&self, shard_id: ShardId, bytes: &[u8]) -> Result<(), std::io::Error> {
        // Bulk backfill defers the served-index rewrite to flush_shard_index()
        // (turns O(n^2) per-record persistence into one write per batch).
        if bulk_ingest_mode() {
            return Ok(());
        }
        fs::create_dir_all(&self.index_dir)?;
        atomic_write_bytes(&self.index_path(shard_id), bytes)
    }

    pub(super) fn validate_load_version(&self, shard_id: ShardId, load_version: u64) -> Result<(), Status> {
        let infos = self.infos.read().expect("info lock poisoned");
        let Some(info) = infos.get(&shard_id) else {
            return Err(Status::error(
                "shard_not_loaded",
                "shard is not loaded on this server",
            ));
        };
        if !info.loaded {
            return Err(Status::error(
                "shard_not_loaded",
                "shard is not loaded on this server",
            ));
        }
        if info.load_version != load_version {
            return Err(Status::error(
                "load_version_mismatch",
                format!(
                    "request load_version {} does not match loaded version {}",
                    load_version, info.load_version
                ),
            ));
        }
        Ok(())
    }

    pub(super) fn shard_stats(&self, shard_id: ShardId) -> Option<ShardStats> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        shards.get(&shard_id).map(|state| {
            let page_store = self.page_store.stats();
            let page_store_zones = self.page_store.zone_summary();
            let string_records = state.strings.len();
            let hash_records = state.hashes.len();
            let set_records = state.sets.len();
            let feature_records = state.features.len();
            let sequence_records = state.sequences.len();
            let ips_records = state.ips.len();
            let risk_records = state.risk.len() + state.risk_changes.len();
            let loaded = info.as_ref().map(|info| info.loaded).unwrap_or(true);
            let readonly = info.as_ref().map(|info| info.readonly).unwrap_or(false);
            let load_version = info
                .as_ref()
                .map(|info| info.load_version)
                .unwrap_or_default();
            let table_name = info
                .as_ref()
                .map(|info| info.table_name.clone())
                .unwrap_or_default();
            let shard_uri = info
                .as_ref()
                .map(|info| info.shard_uri.clone())
                .unwrap_or_default();
            let start_routing_slot = info
                .as_ref()
                .map(|info| info.start_routing_slot)
                .unwrap_or_default();
            let end_routing_slot = info
                .as_ref()
                .map(|info| info.end_routing_slot)
                .unwrap_or(u32::MAX);
            let object_manager = object_manager_stats(state, start_routing_slot, end_routing_slot);
            let secondary_view_total_records = string_records
                + hash_records
                + set_records
                + feature_records
                + sequence_records
                + ips_records
                + risk_records;
            let total_records = if state.slot_index.slot_map.is_empty() {
                secondary_view_total_records
            } else {
                object_manager.object_count
            };
            let shard_stat_info = ShardStatInfo {
                shard_id,
                loaded,
                readonly,
                load_version,
                table_name,
                shard_uri,
                start_routing_slot,
                end_routing_slot,
                total_records,
                storage_bytes: page_store.bytes_written,
                object_manager: object_manager.clone(),
            };
            let storage = crate::control::ShardCanonicalStorageStats {
                page_index_entries: object_manager.page_ref_count as u64,
                block_index_entries: page_store.writes,
                object_index_entries: object_manager.object_count as u64,
                slot_entries: object_manager.routing_slot_count as u64,
                storage_zone_count: page_store_zones
                    .active_bands
                    .saturating_add(page_store_zones.sealed_bands)
                    .saturating_add(page_store_zones.delayed_destroy_bands)
                    .saturating_add(page_store_zones.purged_bands),
                active_storage_zones: page_store_zones.active_bands,
                sealed_storage_zones: page_store_zones.sealed_bands,
                stream_segment_count: page_store_zones
                    .active_bands
                    .saturating_add(page_store_zones.sealed_bands)
                    .saturating_add(page_store_zones.delayed_destroy_bands)
                    .saturating_add(page_store_zones.purged_bands),
                storage_zone_total_bytes: page_store_zones.total_known_physical_bytes,
                storage_zone_used_bytes: page_store_zones.live_physical_bytes,
                storage_zone_stale_bytes: page_store_zones.reclaimable_physical_bytes,
                page_reads: page_store.reads,
                page_writes: page_store.writes,
                block_reads: page_store.reads,
                block_writes: page_store.writes,
                bytes_read: page_store.bytes_read,
                bytes_written: page_store.bytes_written,
                append_watermark: page_store.writes,
                compaction_watermark: page_store_zones.reclaimable_physical_bytes,
            };
            ShardStats {
                shard_id,
                loaded,
                readonly,
                load_version,
                total_records,
                string_records,
                hash_records,
                set_records,
                feature_records,
                sequence_records,
                ips_records,
                risk_records,
                storage_bytes: page_store.bytes_written,
                object_manager,
                shard_stat_info,
                storage,
                cache: self.cache.stats(),
                page_store: page_store.clone(),
                page_store_zones: page_store_zones.clone(),
                block_store: page_store,
                block_store_bands: page_store_zones,
                write_ahead_log: self.wal_store.stats(shard_id),
            }
        })
    }
}
