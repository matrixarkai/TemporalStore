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
        note = "use write_ahead_log_store; oplog naming remains only for legacy compatibility"
    )]
    pub fn oplog_store(&self) -> LocalWriteAheadLogStore {
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
        // If the latest durable dump manifest is newer than the served index, install
        // it as the load base first (recovers data already dumped into a manifest and
        // then WAL-GC'd). C++ analog: index_->Load() restores the dumped index before
        // ObjectManager::Load() replays the oplog on top.
        let installed_manifest_watermark =
            match self.install_latest_manifest_if_newer_on_load(request.shard_id) {
                Ok(watermark) => watermark,
                // C++ parity: index_->Load() failure is fatal. A newer durable manifest
                // that will not install means the served snapshot + (possibly reclaimed)
                // WAL cannot be trusted to hold the records it covers -- refuse the load.
                Err(status) => return LoadShardResponse { status },
            };
        let loaded = self.load_index(request.shard_id);
        // WAL replay watermark, mirroring C++ ObjectManager::Load() reading
        // index_->GetDumpedLogId(): installed manifest -> its oplog_sequence; no index
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
        self.admissions
            .write()
            .expect("admission lock poisoned")
            .entry(AdmissionScope::Shard(request.shard_id))
            .or_default();
        // Replay any WAL records not yet reflected in the loaded index, rebuilding
        // in-memory state the way C++ ObjectManager::Load() replays the oplog. Without
        // this an async_storage write (WAL entry recorded, page/index deferred to the
        // dump) is silently lost on restart if the crash beats the dump.
        if let Err(status) = self.replay_wal_into_shard(request.shard_id, replay_watermark) {
            // C++ ReplayOplog returns DataLoss on a WAL hole and aborts Load. Unwind the
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
        // Eager disk->memory promotion on a normal restart. load_index() above already
        // read every live page from the page store to rebuild the secondary views, so
        // those bytes are hot in the OS page cache -- but the engine's in-memory cache
        // tier is left cold, and every early retrieval after a restart otherwise pays a
        // cold-cache page read. Promote the live pages into the cache tier once here so
        // the shard comes back not just ready but warm. No-op on a fresh/empty shard
        // (nothing live to warm), so it never slows a cold ingestion start. Opt out with
        // MATRIXARK_EAGER_CACHE_WARM_ON_LOAD=0.
        if eager_cache_warm_on_load() {
            let _ = self.warm_cache_from_page_index(request.shard_id, std::iter::empty::<u32>());
        }
        LoadShardResponse {
            status: Status::ok(),
        }
    }

    /// If the latest durable bucket-dump manifest is newer than the served index,
    /// install it (validate + preflight + restore embedded index) and return its
    /// oplog_sequence as the WAL replay watermark. `Ok(None)` when nothing newer exists.
    /// `Err` when a newer manifest IS present but will not install: C++ treats
    /// index_->Load() failure as fatal (partition.cc), because once the served snapshot
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
            .load_index(shard_id)
            .and_then(|state| state.applied_wal_sequence)
            .unwrap_or(0);
        if manifest.oplog_sequence <= served_anchor {
            return Ok(None);
        }
        match self.install_bucket_dump_manifest(&manifest) {
            Ok(()) => Ok(Some(manifest.oplog_sequence)),
            Err(status) => Err(status),
        }
    }

    /// Replay WAL records with sequence greater than `watermark`, re-driving each
    /// through execute (which rebuilds the bucket index + model maps) WITHOUT
    /// re-appending to the WAL or re-persisting the index per record, then anchor and
    /// persist the reconstructed index once. Mirrors C++ ObjectManager::ReplayOplog,
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

        let _guard = WalReplayGuard::enter();
        let mut expected = watermark.saturating_add(1);
        let mut replayed_through = watermark;
        for record in pending {
            // Strict sequence continuity, matching C++ ObjectManager::ReplayOplog, which
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
            // Resolve TTL deadlines / event times against the LEADER's timestamp
            // captured when this record was written, not the (later) restart clock, so
            // recovery reconstructs the identical absolute deadlines the leader logged
            // (C++ resolve-then-log) instead of extending every recently-SETEX'd key.
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

        if replayed_through > watermark {
            let index_bytes = {
                let mut shards = self.shards.write().expect("engine lock poisoned");
                match shards.get_mut(&shard_id) {
                    Some(shard) => {
                        shard.applied_wal_sequence = Some(replayed_through);
                        Some(serialize_index(shard))
                    }
                    None => None,
                }
            };
            if let Some(index_bytes) = index_bytes {
                let _ = self.persist_index_bytes(shard_id, &index_bytes);
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
}
