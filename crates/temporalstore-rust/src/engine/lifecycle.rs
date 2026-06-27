use super::*;

impl Default for TemporalEngine {
    fn default() -> Self {
        Self::with_cache_and_page_store(MultiLayerCache::default(), LocalPageStore::default())
    }
}

impl TemporalEngine {
    pub fn new(cache: MultiLayerCache) -> Self {
        Self::with_cache_and_page_store(cache, LocalPageStore::default())
    }

    pub fn with_cache_and_page_store(cache: MultiLayerCache, page_store: LocalPageStore) -> Self {
        Self::with_cache_page_store_and_index_dir(cache, page_store, unique_temp_path("indexes"))
    }

    pub fn with_cache_page_store_and_index_dir(
        cache: MultiLayerCache,
        page_store: LocalPageStore,
        index_dir: impl Into<PathBuf>,
    ) -> Self {
        let index_dir = index_dir.into();
        let wal_store = LocalWriteAheadLogStore::new(index_dir.join("wals"));
        let index_log_store = LocalIndexLogStore::new(index_dir.join("indexlogs"));
        Self {
            shards: Arc::default(),
            cache,
            page_store,
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

    pub fn page_store(&self) -> LocalPageStore {
        self.page_store.clone()
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
        page_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
    ) -> Self {
        Self::with_local_dirs_and_page_store_options(
            memory_capacity_bytes,
            cache_dir,
            page_store_dir,
            index_dir,
            PageStoreOptions::default(),
        )
    }

    pub fn with_local_dirs_and_page_store_options(
        memory_capacity_bytes: usize,
        cache_dir: impl Into<PathBuf>,
        page_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
        page_store_options: PageStoreOptions,
    ) -> Self {
        Self::with_cache_page_store_and_index_dir(
            MultiLayerCache::new(memory_capacity_bytes, cache_dir),
            LocalPageStore::with_options(page_store_dir, page_store_options),
            index_dir,
        )
    }

    pub fn load_shard(&self, shard_id: ShardId) {
        let request = LoadShardRequest {
            shard_id,
            load_version: 0,
            local_node_id: None,
            shard_uri: String::new(),
            start_routing_slot: 0,
            end_routing_slot: u32::MAX,
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
        let mut state = self.load_index(request.shard_id).unwrap_or_default();
        rebuild_slot_page_ownership(
            request.shard_id,
            &mut state,
            request.start_routing_slot,
            request.end_routing_slot,
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
        self.infos.write().expect("info lock poisoned").insert(
            request.shard_id,
            ShardInfo {
                shard_id: request.shard_id,
                loaded: true,
                table_name: request.table_name,
                shard_uri: request.shard_uri,
                start_routing_slot: request.start_routing_slot,
                end_routing_slot: request.end_routing_slot,
                readonly: request.readonly,
                load_version: request.load_version,
                local_node_id: request.local_node_id,
                membership_version: 0,
                replica_membership_version: 0,
                membership_valid: true,
                replica_node_ids: Vec::new(),
                leader_node_id: None,
            },
        );
        LoadShardResponse {
            status: Status::ok(),
        }
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
                start_routing_slot: request.start_routing_slot,
                end_routing_slot: request.end_routing_slot,
                readonly: request.readonly,
                load_version: request.load_version,
                local_node_id: request.local_node_id,
                membership_version: existing.membership_version,
                replica_membership_version: existing.replica_membership_version,
                membership_valid: existing.membership_valid,
                replica_node_ids: existing.replica_node_ids,
                leader_node_id: existing.leader_node_id,
            },
        );
        LoadShardResponse {
            status: Status::ok(),
        }
    }
}
