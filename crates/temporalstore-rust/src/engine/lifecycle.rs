// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

impl Default for TemporalEngine {
    fn default() -> Self {
        Self::with_cache_and_block_store(MultiLayerCache::default(), LocalBlockStore::default())
    }
}

impl TemporalEngine {
    pub fn new(cache: MultiLayerCache) -> Self {
        Self::with_cache_and_block_store(cache, LocalBlockStore::default())
    }

    pub fn with_cache_and_block_store(cache: MultiLayerCache, block_store: LocalBlockStore) -> Self {
        Self::with_cache_block_store_and_index_dir(cache, block_store, unique_temp_path("indexes"))
    }

    pub fn with_cache_block_store_and_index_dir(
        cache: MultiLayerCache,
        block_store: LocalBlockStore,
        index_dir: impl Into<PathBuf>,
    ) -> Self {
        let index_dir = index_dir.into();
        let wal_store = LocalWriteAheadLogStore::new(index_dir.join("wals"));
        let index_log_store = LocalIndexLogStore::new(index_dir.join("indexlogs"));
        Self {
            shards: Arc::default(),
            cache,
            page_store: block_store,
            wal_store,
            index_log_store,
            index_dir,
            configs: Arc::default(),
            infos: Arc::default(),
            admissions: Arc::default(),
        }
    }

    pub fn cache(&self) -> MultiLayerCache {
        self.cache.clone()
    }

    pub fn block_store(&self) -> LocalBlockStore {
        self.page_store.clone()
    }

    #[deprecated(
        since = "0.1.0",
        note = "use block_store; page naming remains only for legacy compatibility"
    )]
    pub fn page_store(&self) -> LocalBlockStore {
        self.block_store()
    }

    pub fn write_ahead_log_store(&self) -> LocalWriteAheadLogStore {
        self.wal_store.clone()
    }

    #[deprecated(
        since = "0.1.0",
        note = "use write_ahead_log_store; wal naming remains only for legacy compatibility"
    )]
    pub fn wal_store(&self) -> LocalWriteAheadLogStore {
        self.write_ahead_log_store()
    }

    pub fn index_log_store(&self) -> LocalIndexLogStore {
        self.index_log_store.clone()
    }

    pub(crate) fn ingestion_dir(&self) -> PathBuf {
        self.index_dir.join("ingestion")
    }

    pub fn with_local_dirs(
        memory_capacity_bytes: usize,
        cache_dir: impl Into<PathBuf>,
        block_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
    ) -> Self {
        Self::with_local_dirs_and_block_store_options(
            memory_capacity_bytes,
            cache_dir,
            block_store_dir,
            index_dir,
            BlockStoreOptions::default(),
        )
    }

    pub fn with_local_dirs_and_block_store_options(
        memory_capacity_bytes: usize,
        cache_dir: impl Into<PathBuf>,
        block_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
        block_store_options: BlockStoreOptions,
    ) -> Self {
        Self::with_cache_block_store_and_index_dir(
            MultiLayerCache::new(memory_capacity_bytes, cache_dir),
            LocalBlockStore::with_options(block_store_dir, block_store_options),
            index_dir,
        )
    }

    pub fn load_shard(&self, shard_id: ShardId) {
        let request = LoadShardRequest {
            shard_id,
            load_version: 0,
            local_node_id: None,
            shard_uri: String::new(),
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
            readonly: false,
            table_name: String::new(),
        };
        let _ = self.load_shard_with(request);
    }

    pub fn load_shard_with(&self, request: LoadShardRequest) -> LoadShardResponse {
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
        {
            return LoadShardResponse {
                status: Status::error("already_exists", "shard already exists"),
            };
        }
        let (loaded, replay_watermark) = if wal_single_barrier() {
            // SINGLE-BARRIER RECOVERY TRUST (base-only). The data-page + delta fdatasyncs and
            // the per-write base rewrite are all deferred, so neither the served-index delta nor
            // the un-synced base rewrite can be trusted -- they may reference pages that were
            // never fsync'd. Recover ONLY from durable checkpoints:
            //  * the base index snapshot, materialized durably (fsync) at the last dump/flush --
            //    that path fsyncs every page BEFORE advancing its watermark, so every page
            //    at/below the base watermark is on disk; and
            //  * the latest durable dump manifest, if newer than the base file.
            // Then replay the WAL tail from that durable watermark, re-deriving every page written
            // after it (a lost un-synced page is rebuilt, never left dangling) and, via the
            // config-log, re-applying config-driven eviction at the exact frontier. The delta is
            // deliberately NOT folded (load_index_base_only), so each tail record is applied
            // EXACTLY ONCE -- no double-apply of non-idempotent commands (counters, appends).
            let base_only =
                self.load_index_base_only(request.shard_id, eager_cache_warm_on_load());
            let base_watermark = base_only
                .as_ref()
                .and_then(|state| state.applied_wal_sequence)
                .unwrap_or(0);
            match latest_bucket_dump_manifest_at(&self.index_dir, request.shard_id) {
                Some(manifest) if manifest.wal_sequence > base_watermark => {
                    // A durable dump is newer than the base file (base not materialized at that
                    // dump). Use the manifest's embedded durable index as the recovery base. Read
                    // it directly (not install_bucket_dump_manifest) so the stale-manifest guard --
                    // which refuses to install a manifest older than the delta-advanced index-log
                    // sequence -- cannot block trusting the durable checkpoint over the un-synced
                    // delta.
                    match serde_json::from_slice::<ShardState>(&manifest.index_bytes) {
                        Ok(mut restored) => {
                            rebuild_bucket_page_ownership(
                                request.shard_id,
                                &mut restored,
                                0,
                                u32::MAX,
                            );
                            (Some(restored), manifest.wal_sequence)
                        }
                        Err(_) => (base_only, base_watermark),
                    }
                }
                _ => (base_only, base_watermark),
            }
        } else {
            // If the latest durable dump manifest is newer than the served index, install
            // it as the load base first (recovers data already dumped into a manifest and
            // then WAL-GC'd): the dumped index is restored first, then
            // startup load replays the WAL on top.
            let installed_manifest_watermark =
                match self.install_latest_manifest_if_newer_on_load(request.shard_id) {
                    Ok(watermark) => watermark,
                    // An index-load failure is fatal. A newer durable manifest
                    // that will not install means the served snapshot + (possibly reclaimed)
                    // WAL cannot be trusted to hold the records it covers -- refuse the load.
                    Err(status) => return LoadShardResponse { status },
                };
            let loaded = self.load_index(request.shard_id, eager_cache_warm_on_load());
            // WAL replay watermark, from the dumped-log id read on startup load:
            // installed manifest -> its wal_sequence; no index
            // file -> 0 (fresh/async-only shard, replay whole retained WAL); anchored index
            // -> its anchor; unanchored (pre-field) index -> current last_sequence (replay
            // nothing, safe upgrade).
            let replay_watermark = match installed_manifest_watermark {
                Some(manifest_watermark) => manifest_watermark,
                None => match &loaded {
                    None => 0,
                    Some(state) => state
                        .applied_wal_sequence
                        .unwrap_or_else(|| self.wal_store.stats(request.shard_id).last_sequence),
                },
            };
            (loaded, replay_watermark)
        };
        let mut state = loaded.unwrap_or_default();
        promote_model_maps_to_bucket_index_authority(
            request.shard_id,
            &mut state,
            request.start_routing_bucket,
            request.end_routing_bucket,
        );
        // Publish the info row WITH recovering:true BEFORE inserting into `shards`. A
        // concurrent execute() acquires shards.write() first, so if it observes the shard
        // present it is guaranteed (happens-before via the shards lock) to also observe
        // recovering:true and reject the write. Inserting into `shards` first would open a
        // window where the shard is visible but the info row is absent -> the gate defaults
        // to false -> a concurrent write interleaves with replay (the double-apply this gate
        // exists to prevent).
        self.infos.write().expect("info lock poisoned").insert(
            request.shard_id,
            ShardInfo {
                shard_id: request.shard_id,
                loaded: true,
                table_name: request.table_name,
                shard_uri: request.shard_uri,
                start_routing_bucket: request.start_routing_bucket,
                end_routing_bucket: request.end_routing_bucket,
                readonly: request.readonly,
                load_version: request.load_version,
                local_node_id: request.local_node_id,
                membership_version: 0,
                replica_membership_version: 0,
                membership_valid: true,
                replica_node_ids: Vec::new(),
                leader_node_id: None,
                // Serving is gated off until replay below completes.
                recovering: true,
            },
        );
        self.shards
            .write()
            .expect("engine lock poisoned")
            .insert(request.shard_id, state);
        self.configs
            .write()
            .expect("config lock poisoned")
            .entry(request.shard_id)
            .or_default();
        // Single-barrier mode: config is not carried in the served-index checkpoint, so restore
        // the last durably-logged config before replay. This covers the no-replay path (a clean
        // dump with nothing to tail-replay) and post-restart client writes; the replay loop below
        // overrides it with the WAL-sequence-ordered config while re-driving historical records,
        // then restores the latest again. No-op (and byte-for-byte unchanged) off the flag.
        if wal_single_barrier() {
            if let Some(entry) = self.config_log_entries(request.shard_id).into_iter().last() {
                self.configs
                    .write()
                    .expect("config lock poisoned")
                    .insert(request.shard_id, entry.config);
            }
        }
        self.admissions
            .write()
            .expect("admission lock poisoned")
            .entry(AdmissionScope::Shard(request.shard_id))
            .or_default();
        // Replay any WAL records not yet reflected in the loaded index, rebuilding
        // in-memory state the way startup load replays the wal. Without
        // this an async_storage write (WAL entry recorded, page/index deferred to the
        // dump) is silently lost on restart if the crash beats the dump.
        if let Err(status) = self.replay_wal_into_shard(request.shard_id, replay_watermark) {
            // ReplayWal returns DataLoss on a WAL hole and aborts Load. Unwind the
            // partially-loaded shard and refuse the load rather than serve truncated
            // state -- a not-loaded shard is recoverable/re-routable; silent truncation
            // is not.
            self.shards
                .write()
                .expect("engine lock poisoned")
                .remove(&request.shard_id);
            self.infos
                .write()
                .expect("info lock poisoned")
                .remove(&request.shard_id);
            self.configs
                .write()
                .expect("config lock poisoned")
                .remove(&request.shard_id);
            self.admissions
                .write()
                .expect("admission lock poisoned")
                .remove(&AdmissionScope::Shard(request.shard_id));
            return LoadShardResponse { status };
        }
        // Replay succeeded and the shard is consistent: open it for serving. While
        // `recovering` was set, client commands were rejected with a retryable status so no
        // write could interleave with replay and regress the WAL anchor.
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&request.shard_id)
        {
            info.recovering = false;
        }
        // Disk->memory promotion on a normal restart is folded directly into
        // load_index()/reconcile above (gated by eager_cache_warm_on_load()): the pages
        // reconcile reads to rebuild the secondary views are promoted into the cache
        // tier in the same pass, so we avoid a second warm pass re-reading every page
        // under the mutex-serialized block store. No-op on a fresh/empty shard.
        LoadShardResponse {
            status: Status::ok(),
        }
    }

    /// If the latest durable bucket-dump manifest is newer than the served index,
    /// install it (validate + preflight + restore embedded index) and return its
    /// wal_sequence as the WAL replay watermark. `Ok(None)` when nothing newer exists.
    /// `Err` when a newer manifest IS present but will not install: treats
    /// an index-load failure as fatal, because once the served snapshot
    /// lags the manifest the intervening WAL records may already be reclaimed, so a
    /// silent fall-back to the stale snapshot would drop them. The caller refuses the
    /// load instead.
    pub(super) fn install_latest_manifest_if_newer_on_load(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<u64>, Status> {
        let Some(manifest) = latest_bucket_dump_manifest_at(&self.index_dir, shard_id) else {
            return Ok(None);
        };
        let served_anchor = self
            .load_index(shard_id, false)
            .and_then(|state| state.applied_wal_sequence)
            .unwrap_or(0);
        if manifest.wal_sequence <= served_anchor {
            return Ok(None);
        }
        match self.install_bucket_dump_manifest(&manifest) {
            Ok(()) => Ok(Some(manifest.wal_sequence)),
            Err(status) => Err(status),
        }
    }

    /// Replay WAL records with sequence greater than `watermark`, re-driving each
    /// through execute (which rebuilds the bucket index + model maps) WITHOUT
    /// re-appending to the WAL or re-persisting the index per record, then anchor and
    /// persist the reconstructed index once. Matches the WAL replay path,
    /// including its strict sequence-continuity check.
    pub(super) fn replay_wal_into_shard(
        &self,
        shard_id: ShardId,
        watermark: u64,
    ) -> Result<(), Status> {
        let records = match self.wal_store.scan(shard_id, 0, u64::MAX, u64::MAX) {
            Ok(records) => records,
            Err(_) => return Ok(()),
        };
        let mut pending: Vec<WriteAheadLogRecord> = records
            .into_iter()
            .filter_map(|(_, line)| serde_json::from_slice::<WriteAheadLogRecord>(&line).ok())
            .filter(|record| record.sequence > watermark)
            .collect();
        if pending.is_empty() {
            return Ok(());
        }
        pending.sort_by_key(|record| record.sequence);

        // Single-barrier mode: replay config-driven eviction (feature_max_size trims) with the
        // config that was effective at each record's WAL frontier. An entry stamped `after_seq`
        // is effective for records with sequence > after_seq, so before executing a record we
        // apply every not-yet-applied config entry with `after_seq < record.sequence`. This
        // re-derives the exact historical trim -- no resurrection (default config would skip the
        // trim) and no over-trim/loss (the latest config applied to an older record). Empty (and
        // inert) off the flag.
        let config_log: Vec<ConfigLogEntry> = if wal_single_barrier() {
            self.config_log_entries(shard_id)
        } else {
            Vec::new()
        };
        let mut config_cursor = 0usize;

        let _guard = WalReplayGuard::enter();
        let mut expected = watermark.saturating_add(1);
        let mut replayed_through = watermark;
        for record in pending {
            // Strict sequence continuity, matching the WAL replay, which
            // returns DataLoss and aborts Load on a hole in the retained WAL. A gap means
            // a WAL record was lost (partial-GC crash / corruption); refuse the load
            // rather than silently serve a truncated prefix.
            if record.sequence != expected {
                return Err(Status::error(
                    "wal_replay_sequence_gap",
                    format!(
                        "WAL replay hole during recovery: expected sequence {expected}, found {}",
                        record.sequence
                    ),
                ));
            }
            while config_cursor < config_log.len()
                && config_log[config_cursor].after_seq < record.sequence
            {
                self.configs
                    .write()
                    .expect("config lock poisoned")
                    .insert(shard_id, config_log[config_cursor].config.clone());
                config_cursor += 1;
            }
            // Resolve TTL deadlines / event times against the LEADER's timestamp
            // captured when this record was written, not the (later) restart clock, so
            // recovery reconstructs the identical absolute deadlines the leader logged
            // (resolve-then-log) instead of extending every recently-SETEX'd key.
            set_replay_clock_ms(record.metadata.as_ref().map(|meta| meta.timestamp_ms));
            let response = self.execute(ExecuteRequest {
                shard_id,
                command: record.command,
            });
            if !response.status.ok {
                return Err(Status::error(
                    "wal_replay_failed",
                    format!("WAL replay command at sequence {} failed", record.sequence),
                ));
            }
            replayed_through = record.sequence;
            expected = expected.saturating_add(1);
        }
        // Restore the latest config for post-recovery client writes: any config entries stamped
        // at/after the last replayed sequence (future-effective changes) were intentionally not
        // applied above.
        while config_cursor < config_log.len() {
            self.configs
                .write()
                .expect("config lock poisoned")
                .insert(shard_id, config_log[config_cursor].config.clone());
            config_cursor += 1;
        }

        if replayed_through > watermark {
            // Only the uncaptured tail beyond the reconstructed delta anchor reaches here
            // (e.g. async writes, which do not append index-log deltas). The delta path's
            // sync writes were already folded at their original addresses in load_index, so
            // this reconstruct handles just the replayed tail.
            let index_bytes = {
                let mut shards = self.shards.write().expect("engine lock poisoned");
                match shards.get_mut(&shard_id) {
                    Some(shard) => {
                        // The per-command model-map -> bucket-index promotion and
                        // first-index rebuild were deferred for every replayed command
                        // (defer_bucket_index_reconstruct() is true under the WalReplayGuard),
                        // which is what turns an O(n^2) reload into O(n). Fold every
                        // replayed record into the bucket index ONCE here, mirroring
                        // flush_shard_index()'s reconstruct-once pass, so serving reads see
                        // the replayed records and the persisted index reflects them.
                        if promote_model_maps_to_bucket_index_authority(
                            shard_id,
                            shard,
                            0,
                            u32::MAX,
                        ) {
                            reconcile_secondary_views_from_bucket_index(
                                &self.page_store,
                                shard,
                                None,
                            );
                        }
                        rebuild_bucket_first_index(shard_id, shard, 0, u32::MAX);
                        refresh_bucket_runtime_flags(shard);
                        shard.applied_wal_sequence = Some(replayed_through);
                        Some(serialize_index(shard))
                    }
                    None => None,
                }
            };
            // Single-barrier mode: DO NOT persist the reconstructed base index here. The pages
            // this replay just re-derived are not yet fsync'd (the data-page barrier is deferred),
            // so writing a base index anchored at `replayed_through` would again advance the
            // durable base watermark past the durable page frontier -- a second crash would then
            // trust it and drop the un-synced replayed tail. The base index is advanced ONLY by
            // the durable dump/flush path (which fsyncs pages first); until then every reload
            // re-derives the tail from the WAL (the documented base-only re-derivation tradeoff).
            if let Some(index_bytes) = index_bytes {
                if !wal_single_barrier() {
                    let _ = self.persist_index_bytes(shard_id, &index_bytes);
                }
            }
        }
        Ok(())
    }

    pub fn unload_shard(&self, shard_id: ShardId) {
        let _ = self.unload_shard_with(UnloadShardRequest { shard_id });
    }

    pub fn unload_shard_with(&self, request: UnloadShardRequest) -> UnloadShardResponse {
        if !self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
        {
            return UnloadShardResponse {
                status: Status::error("shard_not_found", "shard is not loaded"),
            };
        }
        // Delta path: the per-write base rewrite is deferred, so materialize the current
        // in-memory index to disk before the shard leaves memory. This keeps a later cold
        // load (and any consumer that reads shard-{id}.index.json directly) on a current
        // base. No-op on the default path, where the base is already current per write.
        if delta_served_index_enabled() {
            let index_bytes = self
                .shards
                .read()
                .expect("engine lock poisoned")
                .get(&request.shard_id)
                .map(serialize_index);
            if let Some(index_bytes) = index_bytes {
                let _ = self.persist_index_bytes_durable(request.shard_id, &index_bytes);
            }
        }
        self.shards
            .write()
            .expect("engine lock poisoned")
            .remove(&request.shard_id);
        self.infos
            .write()
            .expect("info lock poisoned")
            .remove(&request.shard_id);
        self.configs
            .write()
            .expect("config lock poisoned")
            .remove(&request.shard_id);
        self.admissions
            .write()
            .expect("admission lock poisoned")
            .remove(&AdmissionScope::Shard(request.shard_id));
        UnloadShardResponse {
            status: Status::ok(),
        }
    }

    pub fn reload_shard_with(&self, request: LoadShardRequest) -> LoadShardResponse {
        let existing = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .cloned();
        let Some(existing) = existing else {
            return self.load_shard_with(request);
        };
        if request.load_version < existing.load_version {
            return LoadShardResponse {
                status: Status::error(
                    "stale_load_version",
                    format!(
                        "reload version {} is older than loaded version {}",
                        request.load_version, existing.load_version
                    ),
                ),
            };
        }
        self.infos.write().expect("info lock poisoned").insert(
            request.shard_id,
            ShardInfo {
                shard_id: request.shard_id,
                loaded: true,
                table_name: request.table_name,
                shard_uri: request.shard_uri,
                start_routing_bucket: request.start_routing_bucket,
                end_routing_bucket: request.end_routing_bucket,
                readonly: request.readonly,
                load_version: request.load_version,
                local_node_id: request.local_node_id,
                membership_version: existing.membership_version,
                replica_membership_version: existing.replica_membership_version,
                membership_valid: existing.membership_valid,
                replica_node_ids: existing.replica_node_ids,
                leader_node_id: existing.leader_node_id,
                // Reload updates metadata only; it does not replay the WAL, so it serves
                // immediately.
                recovering: false,
            },
        );
        LoadShardResponse {
            status: Status::ok(),
        }
    }

    /// Test-only: run the publish phase of `load_shard_with` (install newer manifest, load
    /// the served-index base, publish the shard as `recovering: true`) but STOP before WAL
    /// replay, leaving the shard parked in the recovery window. Returns the WAL replay
    /// watermark to hand to `test_finish_recovery`. Reuses the exact real recovery helpers so
    /// the observed gate behaviour matches production; it only splits the single synchronous
    /// `load_shard_with` into two steps a single-threaded test can observe between.
    #[cfg(test)]
    pub(crate) fn test_publish_recovering_shard(&self, shard_id: ShardId) -> u64 {
        let installed_manifest_watermark = self
            .install_latest_manifest_if_newer_on_load(shard_id)
            .expect("manifest install should succeed in test");
        let loaded = self.load_index(shard_id, eager_cache_warm_on_load());
        let replay_watermark = match installed_manifest_watermark {
            Some(manifest_watermark) => manifest_watermark,
            None => match &loaded {
                None => 0,
                Some(state) => state
                    .applied_wal_sequence
                    .unwrap_or_else(|| self.wal_store.stats(shard_id).last_sequence),
            },
        };
        let mut state = loaded.unwrap_or_default();
        promote_model_maps_to_bucket_index_authority(shard_id, &mut state, 0, u32::MAX);
        self.infos.write().expect("info lock poisoned").insert(
            shard_id,
            ShardInfo {
                shard_id,
                loaded: true,
                table_name: String::new(),
                shard_uri: String::new(),
                start_routing_bucket: 0,
                end_routing_bucket: u32::MAX,
                readonly: false,
                load_version: 0,
                local_node_id: None,
                membership_version: 0,
                replica_membership_version: 0,
                membership_valid: true,
                replica_node_ids: Vec::new(),
                leader_node_id: None,
                recovering: true,
            },
        );
        self.shards
            .write()
            .expect("engine lock poisoned")
            .insert(shard_id, state);
        self.configs
            .write()
            .expect("config lock poisoned")
            .entry(shard_id)
            .or_default();
        self.admissions
            .write()
            .expect("admission lock poisoned")
            .entry(AdmissionScope::Shard(shard_id))
            .or_default();
        replay_watermark
    }

    /// Test-only: finish the recovery started by `test_publish_recovering_shard` by running
    /// the real WAL replay and then clearing the `recovering` gate, exactly as
    /// `load_shard_with`'s tail does.
    #[cfg(test)]
    pub(crate) fn test_finish_recovery(&self, shard_id: ShardId, watermark: u64) {
        self.replay_wal_into_shard(shard_id, watermark)
            .expect("wal replay should succeed in test");
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&shard_id)
        {
            info.recovering = false;
        }
    }
}
